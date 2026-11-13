# Databricks notebook source
# MAGIC %md
# MAGIC # Wyndham hands-on lab: admin setup
# MAGIC
# MAGIC **Run by a workspace admin ONCE before the workshop.** Attendees do NOT
# MAGIC run this notebook; they use `01_lab_companion`, which this notebook copies into each
# MAGIC attendee's home folder.
# MAGIC
# MAGIC What it does, in order:
# MAGIC 1. Preflight: confirms the runner is an admin and that the `de-lab-users` group exists at the account level
# MAGIC 2. Sets the group's entitlements to **Workspace access + Databricks SQL access** only (no cluster creation)
# MAGIC 3. Creates one lab catalog, binds it to **this workspace only**, and adds the shared `genie_lab` schema and `raw` volume
# MAGIC 4. Generates **synthetic** hotel and stay files (no guests, no PII) and holds back one day for the live incremental demo
# MAGIC 5. Builds a reference `gold_hotel_daily` table so anyone whose pipeline breaks can still do metric views and Genie
# MAGIC 6. Creates one sandbox schema per attendee and grants least-privilege access
# MAGIC 7. Grants the group `CAN USE` on the existing Starter SQL warehouse (no new warehouse)
# MAGIC 8. Copies the companion notebook into each attendee's home folder
# MAGIC
# MAGIC Everything is idempotent: rerun it after adding people to the group.
# MAGIC
# MAGIC **Runner needs:** workspace admin, plus `CREATE CATALOG` on the metastore (or metastore admin).
# MAGIC **Compute:** serverless notebook. **Runtime:** about 2 minutes.

# COMMAND ----------

dbutils.widgets.text("catalog", "de_lab", "Lab catalog")
dbutils.widgets.text("group", "de-lab-users", "Attendee group (account-level)")
dbutils.widgets.text("managed_location", "", "Catalog managed location (optional)")
dbutils.widgets.text("warehouse_name", "", "SQL warehouse (blank = Starter)")
dbutils.widgets.text("companion_path", "", "Path to 01_lab_companion (blank = same folder)")
dbutils.widgets.text("end_date", "2026-11-17", "Last stay date (held back for live demo)")
dbutils.widgets.dropdown("peer_sharing", "yes", ["yes", "no"], "Allow attendees to share with each other")

# COMMAND ----------

import base64, csv, json, os, random, re
from datetime import date, timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam, workspace as ws

w = WorkspaceClient()

CATALOG          = dbutils.widgets.get("catalog").strip()
GROUP            = dbutils.widgets.get("group").strip()
MANAGED_LOCATION = dbutils.widgets.get("managed_location").strip()
WAREHOUSE_NAME   = dbutils.widgets.get("warehouse_name").strip()
COMPANION_PATH   = dbutils.widgets.get("companion_path").strip()
END_DATE         = date.fromisoformat(dbutils.widgets.get("end_date").strip())
START_DATE       = date(2026, 10, 1)
PEER_SHARING     = dbutils.widgets.get("peer_sharing") == "yes"

LAB_SCHEMA = "genie_lab"
VOLUME     = "raw"
RAW        = f"/Volumes/{CATALOG}/{LAB_SCHEMA}/{VOLUME}"

def lab_schema_for(user_name):
    """jane.doe@wyndham.com -> lab_jane_doe. Must match the function in 01_lab_companion."""
    local = user_name.split("@")[0].lower()
    return "lab_" + re.sub(r"[^a-z0-9]+", "_", local).strip("_")

print(f"Catalog:    {CATALOG}")
print(f"Group:      {GROUP}")
print(f"Raw volume: {RAW}")
print(f"Stay dates: {START_DATE} to {END_DATE} ({END_DATE} is held back)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Preflight
# MAGIC Unity Catalog grants only work on **account-level** groups. If `de-lab-users` was created as a
# MAGIC workspace-local group, recreate it in the account console (or sync it from your IdP) and assign it
# MAGIC to this workspace.

# COMMAND ----------

me = w.current_user.me()
assert any(g.display == "admins" for g in (me.groups or [])), \
    f"{me.user_name} is not a workspace admin. Ask a workspace admin to run this notebook."

matches = list(w.groups.list(filter=f'displayName eq "{GROUP}"'))
assert matches, (f"Group `{GROUP}` is not visible in this workspace. Create it at the account level "
                 f"(or sync it from your IdP), add attendees, and assign it to this workspace.")
grp = w.groups.get(matches[0].id)

resource_type = getattr(grp.meta, "resource_type", None) if grp.meta else None
assert resource_type != "WorkspaceGroup", \
    f"`{GROUP}` is a workspace-local group. Unity Catalog grants need an account-level group."

users, skipped = [], []
for m in grp.members or []:
    ref = m.ref or ""
    if ref.startswith("Groups/") or ref.startswith("ServicePrincipals/"):
        skipped.append(m.display)
        continue
    try:
        users.append(w.users.get(m.value).user_name)
    except Exception:
        skipped.append(m.display)
users = sorted(set(users))

schemas = {u: lab_schema_for(u) for u in users}
dupes = {s for s in schemas.values() if list(schemas.values()).count(s) > 1}
assert not dupes, f"Two attendees map to the same schema name: {dupes}. Rename one schema by hand."

print(f"✓ Runner {me.user_name} is a workspace admin")
print(f"✓ `{GROUP}` is an account-level group with {len(users)} user(s)")
for u in users:
    print(f"    {u:45s} -> {CATALOG}.{schemas[u]}")
if skipped:
    print(f"  ! Skipped nested groups / service principals (add people directly): {skipped}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Entitlements
# MAGIC Attendees need **Workspace access** (notebooks, pipelines, jobs) and **Databricks SQL access**
# MAGIC (SQL editor, metric views, Genie). Nothing else. Serverless pipelines, jobs, and notebooks do
# MAGIC not need the cluster-creation entitlement, so it is not granted.

# COMMAND ----------

have = {e.value for e in (grp.entitlements or [])}
missing = {"workspace-access", "databricks-sql-access"} - have
if missing:
    w.groups.patch(
        grp.id,
        schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
        operations=[iam.Patch(op=iam.PatchOp.ADD, path="entitlements",
                              value=[{"value": e} for e in sorted(missing)])],
    )
print(f"✓ Entitlements on `{GROUP}`: workspace-access, databricks-sql-access"
      + (f" (added {sorted(missing)})" if missing else " (already set)"))
if "allow-cluster-create" in have:
    print(f"  ! `{GROUP}` has allow-cluster-create. The lab doesn't need it; consider removing it.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Lab catalog, shared schema, raw volume
# MAGIC The catalog is **bound to this workspace only**, so it is not reachable from prod or any other
# MAGIC workspace on the metastore. If the metastore has no root storage (or policy requires per-catalog
# MAGIC storage), set the `managed_location` widget to a path under an existing external location.

# COMMAND ----------

loc = f" MANAGED LOCATION '{MANAGED_LOCATION}'" if MANAGED_LOCATION else ""
spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`{loc} "
          f"COMMENT 'Wyndham hands-on lab. Synthetic data only. Dropped after the workshop.'")

ws_id = w.get_workspace_id()
try:
    w.api_client.do("PATCH", f"/api/2.1/unity-catalog/bindings/catalog/{CATALOG}",
                    body={"add": [{"workspace_id": ws_id, "binding_type": "BINDING_TYPE_READ_WRITE"}]})
    w.api_client.do("PATCH", f"/api/2.1/unity-catalog/catalogs/{CATALOG}",
                    body={"isolation_mode": "ISOLATED"})
    print(f"✓ Catalog {CATALOG} bound to workspace {ws_id} only")
except Exception as e:
    print(f"  ! Could not set the workspace binding automatically: {e}\n"
          f"    Do it in Catalog Explorer > {CATALOG} > Workspaces: uncheck 'All workspaces have access'.")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{LAB_SCHEMA}` "
          f"COMMENT 'Shared, read-only synthetic sample data for the lab'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{LAB_SCHEMA}`.`{VOLUME}` "
          f"COMMENT 'Raw landing files: hotels/ and stays/'")
for sub in ("hotels", "stays", "_holdback"):
    os.makedirs(f"{RAW}/{sub}", exist_ok=True)
print(f"✓ Schema {CATALOG}.{LAB_SCHEMA} and volume {RAW}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Synthetic sample data
# MAGIC 48 hotels across four regions, and one JSON-lines file per night with one row per room night
# MAGIC sold (about 4,000 rows a night). Every stay field lands as a **string** on purpose; fixing
# MAGIC types is the silver step. Seeded data quality issues for the silver expectations:
# MAGIC - about 0.4% of rows with `room_rate` of `0.00` or a negative value (`valid_rate`)
# MAGIC - about 0.2% of rows with a null `hotel_id` (`has_hotel`)
# MAGIC
# MAGIC Shapes are tuned so the slide story holds: Northeast leads on RevPAR, Friday and Saturday
# MAGIC nights run hotter. The last night (`end_date`) is written to `_holdback/` so it can be
# MAGIC dropped in live during slide 11 (see `02_admin_day_of`).

# COMMAND ----------

random.seed(2026)

REGIONS = {  # occupancy base, rate multiplier (tuned to land near the values on slides 21 and 25)
    "Northeast": (0.705, 0.99),
    "Southeast": (0.740, 0.92),
    "West":      (0.655, 0.88),
    "Central":   (0.680, 0.72),
}
BRANDS = [  # brand, segment, base ADR, room count range
    ("Super 8",        "Economy",        72,  (55, 90)),
    ("Days Inn",       "Economy",        78,  (60, 110)),
    ("Microtel",       "Economy",        82,  (60, 90)),
    ("Baymont",        "Midscale",       96,  (70, 110)),
    ("La Quinta",      "Midscale",       108, (90, 140)),
    ("Ramada",         "Midscale",       112, (100, 160)),
    ("Wingate",        "Upper Midscale", 128, (85, 120)),
    ("Wyndham Garden", "Upper Midscale", 138, (110, 170)),
    ("Wyndham",        "Upscale",        172, (140, 220)),
]
CHANNELS = ["Direct Web", "Mobile App", "OTA", "GDS", "Call Center"]
CHANNEL_WEIGHTS = [0.30, 0.15, 0.30, 0.10, 0.15]
CHANNEL_RATE = {"Direct Web": 1.00, "Mobile App": 0.97, "OTA": 0.90, "GDS": 1.08, "Call Center": 1.02}

hotels = []
for r_idx, region in enumerate(REGIONS):
    for i in range(12):
        brand, segment, adr, (lo, hi) = random.choice(BRANDS)
        hotels.append({"hotel_id": f"WH{1000 + r_idx * 100 + i}", "brand": brand, "segment": segment,
                       "region": region, "rooms": random.randint(lo, hi), "_adr": adr})

with open(f"{RAW}/hotels/hotels.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["hotel_id", "brand", "segment", "region", "rooms"])
    for h in hotels:
        writer.writerow([h["hotel_id"], h["brand"], h["segment"], h["region"], h["rooms"]])

n_rows, n_bad_rate, n_null_hotel = 0, 0, 0
d = START_DATE
while d <= END_DATE:
    weekend = d.weekday() in (4, 5)  # Friday and Saturday nights
    lines = []
    for h in hotels:
        occ_base, rate_mult = REGIONS[h["region"]]
        occ = min(0.98, max(0.30, occ_base + (0.12 if weekend else 0.0) + random.gauss(0, 0.05)))
        nightly = h["_adr"] * rate_mult * (1.08 if weekend else 1.0) * random.uniform(0.95, 1.05)
        for n in range(round(h["rooms"] * occ)):
            channel = random.choices(CHANNELS, CHANNEL_WEIGHTS)[0]
            rec = {"stay_id": f"ST-{d:%Y%m%d}-{h['hotel_id']}-{n:03d}",
                   "hotel_id": h["hotel_id"],
                   "stay_date": d.isoformat(),
                   "room_rate": f"{nightly * CHANNEL_RATE[channel] * random.uniform(0.9, 1.1):.2f}",
                   "channel": channel}
            roll = random.random()
            if roll < 0.003:
                rec["room_rate"], n_bad_rate = "0.00", n_bad_rate + 1
            elif roll < 0.004:
                rec["room_rate"], n_bad_rate = f"-{random.randint(1, 99)}.00", n_bad_rate + 1
            if random.random() < 0.002:
                rec["hotel_id"], n_null_hotel = None, n_null_hotel + 1
            lines.append(json.dumps(rec))
    folder = "_holdback" if d == END_DATE else "stays"
    with open(f"{RAW}/{folder}/stays_{d:%Y_%m_%d}.json", "w") as f:
        f.write("\n".join(lines) + "\n")
    n_rows += len(lines)
    d += timedelta(days=1)

n_files = len(os.listdir(f"{RAW}/stays"))
print(f"✓ hotels/hotels.csv ({len(hotels)} hotels)")
print(f"✓ stays/ {n_files} daily files, {n_rows:,} rows total "
      f"({n_bad_rate} bad rates, {n_null_hotel} null hotel_ids)")
print(f"✓ _holdback/stays_{END_DATE:%Y_%m_%d}.json held back for the live demo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Reference gold table
# MAGIC Same logic and grain as the attendee pipeline (`gold_hotel_daily`, one row per hotel per night).
# MAGIC Attendees whose pipeline isn't finished can point their metric view at this table and keep going.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE `{CATALOG}`.`{LAB_SCHEMA}`.gold_hotel_daily
COMMENT 'Reference copy of the module 1 gold table. Use it if your own pipeline is not finished.'
AS
WITH s AS (
  SELECT hotel_id, CAST(stay_date AS DATE) AS stay_date, CAST(room_rate AS DECIMAL(10, 2)) AS room_rate
  FROM read_files('{RAW}/stays/', format => 'json')
),
h AS (
  SELECT * FROM read_files('{RAW}/hotels/', format => 'csv', header => true)
)
SELECT s.hotel_id, h.brand, h.segment, h.region, s.stay_date,
       h.rooms          AS rooms_available,
       COUNT(*)         AS rooms_sold,
       SUM(s.room_rate) AS room_revenue
FROM s JOIN h ON s.hotel_id = h.hotel_id
WHERE s.room_rate > 0 AND s.hotel_id IS NOT NULL
GROUP BY ALL
""")
print(f"✓ {CATALOG}.{LAB_SCHEMA}.gold_hotel_daily "
      f"({spark.table(f'`{CATALOG}`.`{LAB_SCHEMA}`.gold_hotel_daily').count():,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Grants
# MAGIC | Securable | Grantee | Privileges | Why |
# MAGIC |---|---|---|---|
# MAGIC | catalog `de_lab` | group | `USE CATALOG` | Reach anything in the catalog |
# MAGIC | schema `genie_lab` | group | `USE SCHEMA` | See the shared sample data |
# MAGIC | volume `genie_lab.raw` | group | `READ VOLUME` | `read_files` in the pipeline, file arrival trigger |
# MAGIC | table `genie_lab.gold_hotel_daily` | group | `SELECT` | Catch-up table for modules 3 and 4 |
# MAGIC | schema `lab_<user>` | that user only | `USE SCHEMA`, `CREATE TABLE`, `CREATE MATERIALIZED VIEW` | Streaming tables, materialized views, metric view |
# MAGIC | every `lab_<user>` schema | group (if `peer_sharing`) | `USE SCHEMA` | Lets the "share it" step work. Peers still see nothing without a `SELECT` grant from the owner |
# MAGIC
# MAGIC Attendees own the tables, views, pipelines, and jobs they create, so no `SELECT`/`MODIFY` on their
# MAGIC schema is needed. Nobody gets `CREATE SCHEMA`, `CREATE CATALOG`, `MODIFY` on shared data, external
# MAGIC locations, or any other catalog.

# COMMAND ----------

G = f"`{GROUP}`"
for stmt in [
    f"GRANT USE CATALOG ON CATALOG `{CATALOG}` TO {G}",
    f"GRANT USE SCHEMA ON SCHEMA `{CATALOG}`.`{LAB_SCHEMA}` TO {G}",
    f"GRANT READ VOLUME ON VOLUME `{CATALOG}`.`{LAB_SCHEMA}`.`{VOLUME}` TO {G}",
    f"GRANT SELECT ON TABLE `{CATALOG}`.`{LAB_SCHEMA}`.gold_hotel_daily TO {G}",
]:
    spark.sql(stmt)
print(f"✓ Group grants applied to {G}")

for u, s in schemas.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{s}` COMMENT 'Lab sandbox for {u}'")
    spark.sql(f"GRANT USE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW "
              f"ON SCHEMA `{CATALOG}`.`{s}` TO `{u}`")
    if PEER_SHARING:
        spark.sql(f"GRANT USE SCHEMA ON SCHEMA `{CATALOG}`.`{s}` TO {G}")
print(f"✓ {len(schemas)} sandbox schema(s) created and granted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Lab SQL warehouse (existing Starter warehouse)
# MAGIC The lab reuses the workspace's existing Starter warehouse. Nothing about its configuration is
# MAGIC changed; the only change is adding `CAN USE` for the group. Leave the `warehouse_name` widget
# MAGIC blank to pick the warehouse whose name contains "Starter", or set it to an exact name.
# MAGIC
# MAGIC Genie needs a **Pro or serverless** warehouse, and metric views need the **Current** channel
# MAGIC (Databricks Runtime 17.3+). The cell warns if the warehouse doesn't meet either.

# COMMAND ----------

all_wh = list(w.warehouses.list())
if WAREHOUSE_NAME:
    found = [x for x in all_wh if x.name == WAREHOUSE_NAME]
else:
    found = [x for x in all_wh if "starter" in (x.name or "").lower()]
assert len(found) == 1, (f"Expected exactly one matching warehouse, found {[x.name for x in found]}. "
                         f"Set the warehouse_name widget to the exact name.")
wh = found[0]
WAREHOUSE_NAME, wh_id = wh.name, wh.id

w.permissions.update("warehouses", wh_id, access_control_list=[
    iam.AccessControlRequest(group_name=GROUP, permission_level=iam.PermissionLevel.CAN_USE)])
print(f"✓ {G} has CAN USE on {WAREHOUSE_NAME} ({wh_id})")

wh_type = getattr(wh.warehouse_type, "value", wh.warehouse_type)
channel = getattr(getattr(wh.channel, "name", None), "value", getattr(wh.channel, "name", None))
if not wh.enable_serverless_compute and wh_type != "PRO":
    print(f"  ! {WAREHOUSE_NAME} is a Classic warehouse. Genie needs Pro or serverless.")
if channel == "CHANNEL_NAME_PREVIEW":
    print(f"  ! {WAREHOUSE_NAME} is on the Preview channel. Metric views work, but Current is the safer choice for a live lab.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Copy the companion notebook to each attendee
# MAGIC Lands in `/Users/<attendee>/de-lab/01_lab_companion`, so attendee code lives in their own folder
# MAGIC and survives the post-workshop teardown. Existing copies are left alone, so rerunning this
# MAGIC never overwrites someone's work.

# COMMAND ----------

if not COMPANION_PATH:
    this_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    COMPANION_PATH = f"{os.path.dirname(this_path)}/01_lab_companion"

src = base64.b64decode(w.workspace.export(COMPANION_PATH, format=ws.ExportFormat.SOURCE).content).decode()
src = src.replace('LAB_CATALOG_DEFAULT = "de_lab"', f'LAB_CATALOG_DEFAULT = "{CATALOG}"')
src = src.replace('LAB_WAREHOUSE = "Serverless Starter Warehouse"', f'LAB_WAREHOUSE = "{WAREHOUSE_NAME}"')
payload = base64.b64encode(src.encode()).decode()

for u in users:
    folder = f"/Users/{u}/de-lab"
    try:
        w.workspace.mkdirs(folder)
        w.workspace.import_(path=f"{folder}/01_lab_companion", content=payload,
                            format=ws.ImportFormat.SOURCE, language=ws.Language.PYTHON, overwrite=False)
        print(f"  ✓ {folder}/01_lab_companion")
    except Exception as e:
        msg = str(e)
        print(f"  - {folder}/01_lab_companion " +
              ("already exists, left as is" if "exists" in msg.lower() else f"FAILED: {msg[:120]}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS ON CATALOG `{CATALOG}`"))
display(spark.sql(f"SHOW GRANTS ON VOLUME `{CATALOG}`.`{LAB_SCHEMA}`.`{VOLUME}`"))
display(spark.sql(f"SHOW SCHEMAS IN `{CATALOG}`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup complete
# MAGIC
# MAGIC **Dry run:** sign in as one test attendee (or have one attendee try) and run the first two
# MAGIC sections of `01_lab_companion`. If those pass, the whole lab will work.
# MAGIC
# MAGIC **Added someone late?** Add them to `de-lab-users` and rerun this notebook.
# MAGIC
# MAGIC **During the lab (slide 11)** and **after the lab (teardown):** use `02_admin_day_of`.
