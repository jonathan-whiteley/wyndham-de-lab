# Databricks notebook source
# MAGIC %md
# MAGIC # Wyndham hands-on lab: day-of and teardown
# MAGIC
# MAGIC **Admin only.** Pick an action in the `action` widget, then run all. With the default `none`,
# MAGIC nothing changes.
# MAGIC
# MAGIC | Action | When | What it does |
# MAGIC |---|---|---|
# MAGIC | `release_holdback` | During slide 11 | Moves the held-back night into `stays/`. Every attendee's file-arrival trigger fires and their pipeline reads only the new file |
# MAGIC | `teardown` | After the workshop | Deletes lab jobs and pipelines, trashes Genie agents built on the lab catalog, drops the lab catalog |
# MAGIC
# MAGIC Teardown does **not** touch the Starter warehouse. It keeps attendee notebooks and source files
# MAGIC in their home folders, and keeps the `de-lab-users` group. Removing the group (account console or
# MAGIC IdP) also removes its `CAN USE` on the warehouse.

# COMMAND ----------

dbutils.widgets.text("catalog", "de_lab", "Lab catalog")
dbutils.widgets.dropdown("action", "none", ["none", "release_holdback", "teardown"], "Action")

# COMMAND ----------

import json, os
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
CATALOG = dbutils.widgets.get("catalog").strip()
ACTION  = dbutils.widgets.get("action")
RAW     = f"/Volumes/{CATALOG}/genie_lab/raw"
print(f"Action: {ACTION}  |  catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Release the held-back night (slide 11)

# COMMAND ----------

if ACTION == "release_holdback":
    held = sorted(os.listdir(f"{RAW}/_holdback"))
    if not held:
        print("Nothing held back. Already released?")
    for f in held:
        dbutils.fs.mv(f"{RAW}/_holdback/{f}", f"{RAW}/stays/{f}")
        print(f"✓ Released {f} into stays/. Triggers fire within about a minute.")
else:
    print("Skipped (action is not release_holdback)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teardown (after the workshop)
# MAGIC Order matters: jobs first (they reference pipelines), then pipelines, then Genie agents, then
# MAGIC the catalog. Only objects tied to the lab catalog are removed.

# COMMAND ----------

if ACTION == "teardown":
    # 1. Pipelines that publish to the lab catalog
    lab_pipelines = {}
    for p in w.pipelines.list_pipelines():
        spec = w.pipelines.get(p.pipeline_id).spec
        if spec and spec.catalog == CATALOG:
            lab_pipelines[p.pipeline_id] = p.name

    # 2. Jobs that run one of those pipelines
    for j in w.jobs.list(expand_tasks=True):
        tasks = (j.settings.tasks or []) if j.settings else []
        if any(t.pipeline_task and t.pipeline_task.pipeline_id in lab_pipelines for t in tasks):
            w.jobs.delete(j.job_id)
            print(f"  ✓ Deleted job {j.settings.name} ({j.job_id})")

    for pid, name in lab_pipelines.items():
        w.pipelines.delete(pid)
        print(f"  ✓ Deleted pipeline {name} ({pid})")

    # 3. Genie agents whose data comes from the lab catalog (best effort; other agents are untouched)
    try:
        token = None
        while True:
            resp = w.api_client.do("GET", "/api/2.0/genie/spaces",
                                   query={"page_token": token} if token else None)
            for s in resp.get("spaces", []):
                detail = w.api_client.do("GET", f"/api/2.0/genie/spaces/{s['space_id']}",
                                         query={"include_serialized_space": "true"})
                serialized = detail.get("serialized_space") or json.dumps(detail)
                if f"{CATALOG}." in serialized:
                    w.api_client.do("DELETE", f"/api/2.0/genie/spaces/{s['space_id']}")
                    print(f"  ✓ Trashed Genie agent {s.get('title')} ({s['space_id']})")
            token = resp.get("next_page_token")
            if not token:
                break
    except Exception as e:
        print(f"  ! Genie cleanup skipped ({str(e)[:120]}). Trash the 'Hotel performance' agents from the Genie page.")

    # 4. Catalog: every attendee schema, table, metric view, and the raw volume
    spark.sql(f"DROP CATALOG IF EXISTS `{CATALOG}` CASCADE")
    print(f"  ✓ Dropped catalog {CATALOG}")
else:
    print("Skipped (action is not teardown)")
