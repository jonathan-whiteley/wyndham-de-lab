# Databricks notebook source
# MAGIC %md
# MAGIC # From raw data to a Genie agent: lab companion
# MAGIC
# MAGIC Follow along with the slides. This notebook finds your sandbox schema, hands you the code for
# MAGIC each step with your names already filled in, and checks your work as you go.
# MAGIC
# MAGIC | Module | You build | Where |
# MAGIC |---|---|---|
# MAGIC | 0 · Setup | Confirm access to the sample data | This notebook |
# MAGIC | 1 · Data engineering 101 | `stays_pipeline`, `stays_refresh` job, bundle | Jobs & Pipelines (code below) |
# MAGIC | 2 · Semantic layer | See why RevPAR needs one definition | This notebook |
# MAGIC | 3 · Metric views | `hotel_kpis` | Catalog Explorer or this notebook |
# MAGIC | 4 · Genie | **Hotel performance** agent | Genie (config below) |
# MAGIC
# MAGIC **Compute:** attach to **Serverless**. All data is synthetic (no guests, no PII).

# COMMAND ----------

LAB_CATALOG_DEFAULT = "de_lab"
LAB_WAREHOUSE = "Serverless Starter Warehouse"
dbutils.widgets.text("catalog", LAB_CATALOG_DEFAULT, "Lab catalog")

# COMMAND ----------

import os, re

CATALOG = dbutils.widgets.get("catalog").strip()
ME = spark.sql("SELECT current_user()").first()[0]

def lab_schema_for(user_name):
    """jane.doe@wyndham.com -> lab_jane_doe. Must match the function in 00_admin_setup."""
    local = user_name.split("@")[0].lower()
    return "lab_" + re.sub(r"[^a-z0-9]+", "_", local).strip("_")

MY_SCHEMA = lab_schema_for(ME)
RAW = f"/Volumes/{CATALOG}/genie_lab/raw"
FQ = f"`{CATALOG}`.`{MY_SCHEMA}`"

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{MY_SCHEMA}`")

print(f"You are:        {ME}")
print(f"Lab catalog:    {CATALOG}")
print(f"Your schema:    {CATALOG}.{MY_SCHEMA}   <- everything you build goes here")
print(f"Sample data:    {RAW}")
print(f"SQL warehouse:  {LAB_WAREHOUSE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 0 · Setup
# MAGIC If either cell below fails with a permissions error, raise a hand. It means setup missed you.

# COMMAND ----------

display(dbutils.fs.ls(f"{RAW}/hotels/"))
display(dbutils.fs.ls(f"{RAW}/stays/"))

# COMMAND ----------

# MAGIC %md
# MAGIC Peek at the raw files. Notice every `stays` column is a **string**, including `stay_date` and
# MAGIC `room_rate`. Fixing that is the silver step.

# COMMAND ----------

display(spark.sql(f"SELECT * FROM read_files('{RAW}/hotels/', format => 'csv', header => true) LIMIT 10"))

# COMMAND ----------

stays_raw = spark.sql(f"SELECT * FROM read_files('{RAW}/stays/', format => 'json')")
stays_raw.printSchema()
display(stays_raw.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 1 · Data engineering 101
# MAGIC
# MAGIC ### Steps 1-3: create the pipeline (slides 8-10)
# MAGIC 1. **+ New → ETL pipeline**, name it `stays_pipeline`
# MAGIC 2. Default catalog: the lab catalog. Default schema: **your schema** (printed above)
# MAGIC 3. Language **SQL**, start from an **empty file**
# MAGIC 4. In `transformations/`, create `stays.sql` and `gold.sql` and paste the code printed below.
# MAGIC    The catalog is already filled in.

# COMMAND ----------

STAYS_SQL = f"""-- transformations/stays.sql

-- Bronze: picks up only new files on each run
CREATE OR REFRESH STREAMING TABLE bronze_stays
AS SELECT * FROM STREAM read_files(
  '{RAW}/stays/', format => 'json');

-- Silver: typed columns plus quality rules
CREATE OR REFRESH STREAMING TABLE silver_stays (
  CONSTRAINT valid_rate EXPECT (room_rate > 0) ON VIOLATION DROP ROW,
  CONSTRAINT has_hotel  EXPECT (hotel_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT stay_id, hotel_id, channel,
  CAST(stay_date AS DATE)          AS stay_date,
  CAST(room_rate AS DECIMAL(10, 2)) AS room_rate
FROM STREAM bronze_stays;
"""

GOLD_SQL = f"""-- transformations/gold.sql

-- Hotel list: small, rebuilt on each run
CREATE OR REFRESH MATERIALIZED VIEW hotels
AS SELECT * FROM read_files(
  '{RAW}/hotels/',
  format => 'csv', header => true);

-- Gold: one row per hotel per night
CREATE OR REFRESH MATERIALIZED VIEW gold_hotel_daily
AS SELECT
  s.hotel_id, h.brand, h.segment, h.region, s.stay_date,
  h.rooms           AS rooms_available,
  COUNT(*)          AS rooms_sold,
  SUM(s.room_rate)  AS room_revenue
FROM silver_stays s
JOIN hotels h ON s.hotel_id = h.hotel_id
GROUP BY ALL;
"""

print(STAYS_SQL)
print("-" * 70)
print(GOLD_SQL)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: run it and check the results (slide 11)
# MAGIC Click **Run pipeline**. When all four boxes are green, run the next two cells.
# MAGIC `bronze - silver` is the number of rows your expectations dropped.

# COMMAND ----------

display(spark.sql(f"""
SELECT
  (SELECT COUNT(*) FROM {FQ}.bronze_stays)     AS bronze_rows,
  (SELECT COUNT(*) FROM {FQ}.silver_stays)     AS silver_rows,
  (SELECT COUNT(*) FROM {FQ}.bronze_stays) -
  (SELECT COUNT(*) FROM {FQ}.silver_stays)     AS dropped_by_expectations,
  (SELECT COUNT(*) FROM {FQ}.gold_hotel_daily) AS gold_rows
"""))

# COMMAND ----------

# Expectation results per pipeline update, straight from the pipeline event log
try:
    display(spark.sql(f"""
    SELECT e.timestamp, x.name AS expectation, x.passed_records, x.failed_records
    FROM event_log(TABLE({FQ}.silver_stays)) e
    LATERAL VIEW explode(from_json(
      e.details:flow_progress.data_quality.expectations,
      'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) t AS x
    WHERE e.event_type = 'flow_progress'
      AND e.details:flow_progress.data_quality IS NOT NULL
    ORDER BY e.timestamp DESC
    """))
except Exception as e:
    print(f"Event log not available yet ({str(e)[:100]}). Check the pipeline UI instead: click silver_stays.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: schedule it with a job (slide 13)
# MAGIC The next cell writes `freshness_check.sql` into your `de-lab` folder. It fails the job if the
# MAGIC newest stay in silver is more than 3 days old.
# MAGIC
# MAGIC 1. **Create → Job**, name it `stays_refresh`
# MAGIC 2. Task 1: type **Pipeline**, pick `stays_pipeline`
# MAGIC 3. Task 2: type **SQL**, **File**, pick `freshness_check.sql`, the lab SQL warehouse (printed at the top), depends on task 1
# MAGIC 4. **Add trigger → File arrival**, location: the stays path printed below
# MAGIC 5. **Job notifications → On failure**: your email

# COMMAND ----------

FRESHNESS_SQL = f"""-- Fails the job if the newest stay is more than 3 days old
SELECT
  CASE
    WHEN MAX(stay_date) >= date_sub(current_date(), 3)
      THEN concat('Fresh through ', CAST(MAX(stay_date) AS STRING))
    ELSE raise_error(concat('Stays data is stale. Latest stay_date: ', CAST(MAX(stay_date) AS STRING)))
  END AS freshness
FROM {CATALOG}.{MY_SCHEMA}.silver_stays;
"""

lab_folder = f"/Workspace/Users/{ME}/de-lab"
try:
    os.makedirs(lab_folder, exist_ok=True)
    with open(f"{lab_folder}/freshness_check.sql", "w") as f:
        f.write(FRESHNESS_SQL)
    print(f"✓ Wrote {lab_folder}/freshness_check.sql")
except Exception as e:
    print(f"Couldn't write the file ({e}). Create freshness_check.sql in your folder and paste:\n")
    print(FRESHNESS_SQL)

print(f"\nFile arrival trigger location:  {RAW}/stays/")
try:
    display(spark.sql(FRESHNESS_SQL.rstrip().rstrip(";")))
except Exception as e:
    print(f"\nFreshness check didn't pass yet: {str(e)[:150]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: ship it as code with a bundle (slide 14)
# MAGIC The instructor drives this step. Copy the pipeline and job IDs from their URLs if you want to try
# MAGIC it later in a Git folder (**Create → Bundle**) or from the web terminal:
# MAGIC ```bash
# MAGIC databricks bundle init default-minimal
# MAGIC databricks bundle generate pipeline --existing-pipeline-id <id> --key stays_pipeline --bind
# MAGIC databricks bundle generate job      --existing-job-id <id>      --key stays_refresh  --bind
# MAGIC databricks bundle validate
# MAGIC databricks bundle deploy -t dev
# MAGIC databricks bundle run stays_refresh -t dev
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 2 · Why a semantic layer matters (slide 16)
# MAGIC One night, every hotel. Both numbers come from valid SQL. Only one is RevPAR.

# COMMAND ----------

GOLD = f"{CATALOG}.{MY_SCHEMA}.gold_hotel_daily"
if not spark.catalog.tableExists(GOLD):
    GOLD = f"{CATALOG}.genie_lab.gold_hotel_daily"
    print(f"Your gold table isn't there yet, so this uses the reference copy: {GOLD}\n")

display(spark.sql(f"""
WITH by_hotel AS (
  SELECT hotel_id,
         SUM(room_revenue)                          AS room_revenue,
         SUM(rooms_available)                       AS rooms_available,
         SUM(room_revenue) / SUM(rooms_available)   AS hotel_revpar
  FROM {GOLD}
  WHERE stay_date = '2026-10-15'
  GROUP BY hotel_id
)
SELECT ROUND(AVG(hotel_revpar), 2)                          AS wrong_average_of_hotel_revpars,
       ROUND(SUM(room_revenue) / SUM(rooms_available), 2)   AS right_total_revenue_over_total_rooms
FROM by_hotel
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 3 · Metric views (slides 19-21)
# MAGIC Create `hotel_kpis` in **Catalog Explorer** (open `gold_hotel_daily`, **Create → Metric view**)
# MAGIC and paste the YAML printed below. Stuck, or prefer SQL? Run the cell after it instead; it creates
# MAGIC the same metric view.
# MAGIC
# MAGIC This YAML adds `Segment` and `Hotel ID` to the dimensions on slide 20, so the "top 5 hotels"
# MAGIC example in Genie works.

# COMMAND ----------

MV_YAML = f"""version: 1.1
source: {GOLD}
comment: "Hotel performance KPIs"
dimensions:
  - name: Stay Date
    expr: stay_date
  - name: Brand
    expr: brand
  - name: Segment
    expr: segment
  - name: Region
    expr: region
  - name: Hotel ID
    expr: hotel_id
measures:
  - name: RevPAR
    expr: SUM(room_revenue) / SUM(rooms_available)
    synonyms: ["revenue per available room"]
  - name: ADR
    expr: SUM(room_revenue) / SUM(rooms_sold)
    synonyms: ["average daily rate"]
  - name: Occupancy
    expr: SUM(rooms_sold) / SUM(rooms_available)
    synonyms: ["occupancy rate", "occ"]
"""
print(MV_YAML)

# COMMAND ----------

# SQL path: creates (or replaces) the metric view from the YAML above
spark.sql(f"""
CREATE OR REPLACE VIEW {FQ}.hotel_kpis
WITH METRICS
LANGUAGE YAML
AS $$
{MV_YAML}
$$
""")
print(f"✓ {CATALOG}.{MY_SCHEMA}.hotel_kpis")

# COMMAND ----------

# MAGIC %md
# MAGIC Query it with `MEASURE()`. Then drop `Region` from the query: you get portfolio RevPAR, still
# MAGIC total revenue over total rooms. (In the SQL editor, pick the lab SQL warehouse and set the catalog
# MAGIC and schema dropdowns to yours so `hotel_kpis` resolves.)

# COMMAND ----------

display(spark.sql(f"""
SELECT `Region`,
       MEASURE(`RevPAR`)    AS revpar,
       MEASURE(`ADR`)       AS adr,
       MEASURE(`Occupancy`) AS occupancy
FROM {FQ}.hotel_kpis
WHERE `Stay Date` >= '2026-10-01'
GROUP BY ALL
ORDER BY revpar DESC
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT MEASURE(`RevPAR`) AS portfolio_revpar,
       MEASURE(`ADR`)    AS portfolio_adr,
       MEASURE(`Occupancy`) AS portfolio_occupancy
FROM {FQ}.hotel_kpis
WHERE `Stay Date` >= '2026-10-01'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 4 · Build your Genie agent (slides 23-26)
# MAGIC 1. **Genie → + New**, add **only** `hotel_kpis` as data, and pick the lab SQL warehouse
# MAGIC 2. Title **Hotel performance**, then paste the instructions, sample questions, and example SQL printed below
# MAGIC 3. Ask a question, open **Show code**, and confirm it uses `MEASURE(`RevPAR`)`
# MAGIC 4. Add the benchmark questions, run them, and compare to the answer key further down

# COMMAND ----------

print(f"""TITLE
Hotel performance

DESCRIPTION
RevPAR, ADR, and occupancy by hotel, brand, segment, and region (synthetic lab data).

GENERAL INSTRUCTIONS
Use hotel_kpis for RevPAR, ADR, and occupancy.
"Last month" means the last full calendar month.

SAMPLE QUESTIONS
What was RevPAR last month by region?
Which brand had the best occupancy in October?
How is ADR trending week over week?

EXAMPLE SQL (title: Top 5 hotels by RevPAR)
SELECT `Hotel ID`, MEASURE(`RevPAR`) AS revpar
FROM {CATALOG}.{MY_SCHEMA}.hotel_kpis
GROUP BY ALL
ORDER BY revpar DESC
LIMIT 5

BENCHMARK QUESTIONS
RevPAR by region last month
Top 5 hotels by ADR in October
Occupancy trend by week
ADR on weekend nights in October
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Benchmark answer key
# MAGIC If the weekend question misses, check **Show code**: hotel weekend nights are Friday and Saturday,
# MAGIC not Saturday and Sunday. Add the instruction *Weekend nights are Friday and Saturday* and rerun.

# COMMAND ----------

LAST_MONTH = "`Stay Date` >= add_months(date_trunc('MONTH', current_date()), -1) AND `Stay Date` < date_trunc('MONTH', current_date())"
OCTOBER = "`Stay Date` BETWEEN '2026-10-01' AND '2026-10-31'"

print("1. RevPAR by region last month")
display(spark.sql(f"SELECT `Region`, ROUND(MEASURE(`RevPAR`), 2) AS revpar FROM {FQ}.hotel_kpis WHERE {LAST_MONTH} GROUP BY ALL ORDER BY revpar DESC"))

print("2. Top 5 hotels by ADR in October")
display(spark.sql(f"SELECT `Hotel ID`, ROUND(MEASURE(`ADR`), 2) AS adr FROM {FQ}.hotel_kpis WHERE {OCTOBER} GROUP BY ALL ORDER BY adr DESC LIMIT 5"))

print("3. Occupancy trend by week")
display(spark.sql(f"SELECT date_trunc('WEEK', `Stay Date`) AS week, ROUND(MEASURE(`Occupancy`), 3) AS occupancy FROM {FQ}.hotel_kpis GROUP BY ALL ORDER BY week"))

print("4. ADR on weekend (Friday and Saturday) nights in October")
display(spark.sql(f"SELECT ROUND(MEASURE(`ADR`), 2) AS weekend_adr FROM {FQ}.hotel_kpis WHERE {OCTOBER} AND dayofweek(`Stay Date`) IN (6, 7)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Share it (slide 26)
# MAGIC Your teammate needs three things: access to the agent (**Share → Can run**), `SELECT` on
# MAGIC `hotel_kpis`, and the warehouse (everyone in the lab already has it). Put a neighbor's email
# MAGIC below and run the cell to grant the `SELECT`.

# COMMAND ----------

NEIGHBOR = ""  # e.g. "jane.doe@wyndham.com"

if NEIGHBOR:
    spark.sql(f"GRANT SELECT ON TABLE {FQ}.hotel_kpis TO `{NEIGHBOR}`")
    print(f"✓ {NEIGHBOR} can query {CATALOG}.{MY_SCHEMA}.hotel_kpis. Now share the Genie agent with them.")
else:
    print("Set NEIGHBOR to a teammate's email and rerun.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wrap-up
# MAGIC - **Removed after today:** the lab catalog (your schema, tables, metric view), your pipeline,
# MAGIC   job, and Genie agent
# MAGIC - **Kept:** this notebook, `freshness_check.sql`, and any bundle or pipeline source files in your
# MAGIC   workspace folder
# MAGIC - **Next:** the same pattern on your real gold tables, with metric views and Genie on top
