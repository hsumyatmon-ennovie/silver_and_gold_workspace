# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6fa25cdd-36f9-4f2e-9817-c1f4d946d4d9",
# META       "default_lakehouse_name": "Gold_Production_Lakehouse",
# META       "default_lakehouse_workspace_id": "d74457b3-045c-445d-82c6-9a2e4b9f1436",
# META       "known_lakehouses": [
# META         {
# META           "id": "6fa25cdd-36f9-4f2e-9817-c1f4d946d4d9"
# META         },
# META         {
# META           "id": "ad99fdfa-85b1-4480-9f7f-2640bfd65f24"
# META         },
# META         {
# META           "id": "3a130b81-98ec-4fd4-a404-95edc1f0ef1e"
# META         },
# META         {
# META           "id": "e248ea90-8431-4df2-9f29-87866bf9dd5a"
# META         },
# META         {
# META           "id": "ff4d6787-a716-43b6-baaf-972b7426ffa5"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead",  "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead",     "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite",    "LEGACY")

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")


# Just cause sales data have dates older than 1900s


from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Production Casting Status

# MARKDOWN ********************

# ## All Silver

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_production_casting_status
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH
# MAGIC /* ============================================================
# MAGIC    1) Sources
# MAGIC    ============================================================ */
# MAGIC /* ---- item variant: pick ONE code per item to avoid row explosion ---- */
# MAGIC item_variant_1 AS (
# MAGIC     SELECT
# MAGIC         V.`Item No.` AS item_no,
# MAGIC         V.`Code`     AS item_size,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY V.`Item No.`
# MAGIC             ORDER BY V.`Code` ASC
# MAGIC         ) AS rn
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Variant` V
# MAGIC ),
# MAGIC po_src AS (
# MAGIC     SELECT
# MAGIC         I.`Status`               AS prod_order_status,
# MAGIC         I.`No.`                  AS prod_order_no,
# MAGIC         I.`Description`          AS prod_order_description,
# MAGIC         I.`Source No.`           AS FG_item_no,
# MAGIC         I.`Routing No.`          AS item_routing_no,
# MAGIC         I.`Due Date`             AS prod_order_due_date,
# MAGIC         I.`Finished Date`        AS prod_order_finished_date,
# MAGIC         I.`Quantity`             AS prod_order_quantity,
# MAGIC         I.`Cost Amount`          AS prod_order_cost_amount,
# MAGIC         I.`No. Series`           AS prod_order_no_series,
# MAGIC         I.`Starting Date-Time`   AS prod_order_starting_date_time,
# MAGIC         I.`Ending Date-Time`     AS prod_order_ending_date_time,
# MAGIC         I.`Sales Order No.`      AS sales_order_no,
# MAGIC         I.`Sales Order Line No.` AS sales_order_line_no,
# MAGIC         I.`Prod. Order Type.`    AS prod_order_type,
# MAGIC         I.`Remark`               AS remark,
# MAGIC         I.`For Item`             AS ref_item,
# MAGIC         I.`For Prod.Order No.`   AS ref_prod_order,
# MAGIC         I.`SystemCreatedAt`      AS created_on,
# MAGIC         I.`SystemModifiedAt`     AS modified_on,
# MAGIC         V.item_size              AS item_size
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Production Order` I
# MAGIC     LEFT JOIN item_variant_1 V
# MAGIC         ON I.`Source No.` = V.item_no
# MAGIC        AND V.rn = 1
# MAGIC ),
# MAGIC 
# MAGIC pl_src AS (
# MAGIC     SELECT
# MAGIC         `Status`                      AS prod_order_status,
# MAGIC         `Prod. Order No.`             AS prod_order_no,
# MAGIC         `Line No.`                    AS prod_order_line_no,
# MAGIC         `Item No.`                    AS item_no,
# MAGIC         `Description`                 AS item_description,
# MAGIC         `Location Code`               AS item_location,
# MAGIC         `Shortcut Dimension 2 Code`   AS item_material,
# MAGIC         `Quantity`                    AS prod_line_quantity,
# MAGIC         `Finished Quantity`           AS prod_line_finished_quantity,
# MAGIC         `Remaining Quantity`          AS prod_line_remaining_quantity,
# MAGIC         `Due Date`                    AS prod_line_due_date,
# MAGIC         `Production BOM No.`          AS bom_no,
# MAGIC         `Routing No.`                 AS item_routing_no,
# MAGIC         `Inventory Posting Group`     AS inventory_posting_group,
# MAGIC         `Routing Reference No.`       AS item_routing_line,
# MAGIC         `Unit Cost`                   AS prod_line_unit_cost,
# MAGIC         `Cost Amount`                 AS prod_line_cost_amount,
# MAGIC         `Unit of Measure Code`        AS item_uom,
# MAGIC         `Starting Date-Time`          AS prod_line_start_date,
# MAGIC         `Ending Date-Time`            AS prod_line_end_date,
# MAGIC         `Sales Order No.`             AS sales_order_no,
# MAGIC         `Sales Order Line No.`        AS sales_order_line_no,
# MAGIC         `Production BOM Version Code` AS bom_version,
# MAGIC         `Routing Version Code`        AS item_routing_version,
# MAGIC         `SystemCreatedAt`             AS created_on,
# MAGIC         `SystemModifiedAt`            AS modified_on
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Prod Order Line`
# MAGIC ),
# MAGIC 
# MAGIC it_src AS (
# MAGIC     SELECT
# MAGIC         I.`No.`                 AS item_no,
# MAGIC         I.`Item Category Code`  AS item_category_code,
# MAGIC         I.`Last Date Modified`  AS modified_on
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item` I
# MAGIC ),
# MAGIC 
# MAGIC /* engine-anchored due dates: latest engine_run per (prod_order_no, prod_order_line_no) */
# MAGIC pod_src AS (
# MAGIC     SELECT prod_order_no, prod_order_line_no, prod_order_due_date AS planned_prod_order_due_date
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             prod_order_no,
# MAGIC             prod_order_line_no,
# MAGIC             prod_order_due_date,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY prod_order_no, prod_order_line_no
# MAGIC                 ORDER BY engine_run_ts DESC
# MAGIC             ) AS rn
# MAGIC         FROM Gold_Production_Lakehouse.prod.planning_operation_due
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC s_src AS (
# MAGIC     SELECT
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no,
# MAGIC         created_on,
# MAGIC         trim(current_location_code) AS current_location_code,
# MAGIC         trim(machine_center_no)     AS machine_center_no,
# MAGIC         type_name,
# MAGIC         open,
# MAGIC         prod_order_status AS pos_status,
# MAGIC         modified_on       AS s_modified_on
# MAGIC     FROM Silver_Production_Lakehouse.prod.silver_prod_order_status
# MAGIC ),
# MAGIC 
# MAGIC cp_src AS (
# MAGIC     SELECT
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no,
# MAGIC         item_no            AS itemCST,
# MAGIC         casting_prod_order,
# MAGIC         casting_qty_to_tree,
# MAGIC         casting_qty_passed,
# MAGIC         casting_qty_reject,
# MAGIC         modified_on        AS cp_modified_on
# MAGIC     FROM Silver_Production_Lakehouse.prod.silver_casting_parts
# MAGIC ),
# MAGIC 
# MAGIC ct_src AS (
# MAGIC     SELECT
# MAGIC         casting_prod_order,
# MAGIC         casting_tree_no,
# MAGIC         casting_status,
# MAGIC         modified_on        AS ct_modified_on
# MAGIC     FROM Silver_Production_Lakehouse.prod.silver_casting_tree
# MAGIC ),
# MAGIC 
# MAGIC /* ============================================================
# MAGIC    2) prod_order = po + pl
# MAGIC    ============================================================ */
# MAGIC prod_order AS (
# MAGIC     SELECT
# MAGIC         po.sales_order_no,
# MAGIC         pl.sales_order_line_no,
# MAGIC 
# MAGIC         po.prod_order_no,
# MAGIC         pl.prod_order_line_no,
# MAGIC 
# MAGIC         po.FG_item_no,
# MAGIC         po.item_routing_no,
# MAGIC         po.item_size,  -- ✅ carry forward item_size
# MAGIC         po.prod_order_starting_date_time,
# MAGIC         po.prod_order_ending_date_time,
# MAGIC         coalesce(pod.planned_prod_order_due_date, po.prod_order_due_date) AS prod_order_due_date,
# MAGIC         weekofyear(coalesce(pod.planned_prod_order_due_date, po.prod_order_due_date)) AS commit_week,
# MAGIC         pl.prod_line_due_date,
# MAGIC         po.prod_order_finished_date,
# MAGIC         po.prod_order_quantity,
# MAGIC         po.prod_order_status,
# MAGIC         po.ref_prod_order,
# MAGIC         po.ref_item,
# MAGIC 
# MAGIC         pl.prod_line_start_date,
# MAGIC         pl.prod_line_end_date,
# MAGIC         pl.prod_line_quantity,
# MAGIC         pl.prod_line_finished_quantity,
# MAGIC         pl.prod_line_remaining_quantity,
# MAGIC         pl.item_location,
# MAGIC         pl.item_no AS prod_item_line,
# MAGIC 
# MAGIC         concat(po.sales_order_no, pl.sales_order_line_no) AS SOL,
# MAGIC         concat(po.prod_order_no, cast(pl.prod_order_line_no AS string)) AS POL,
# MAGIC 
# MAGIC         po.modified_on AS po_modified_on,
# MAGIC         pl.modified_on AS pl_modified_on
# MAGIC     FROM po_src po
# MAGIC     LEFT JOIN pl_src pl
# MAGIC         ON po.prod_order_no = pl.prod_order_no
# MAGIC     LEFT JOIN pod_src pod
# MAGIC         ON po.prod_order_no = pod.prod_order_no
# MAGIC        AND pl.prod_order_line_no = pod.prod_order_line_no
# MAGIC ),
# MAGIC 
# MAGIC /* ============================================================
# MAGIC    3) LatestStatus (dedupe + filters)
# MAGIC    ============================================================ */
# MAGIC latest_status AS (
# MAGIC     SELECT
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no,
# MAGIC         created_on,
# MAGIC         type_name,
# MAGIC         open,
# MAGIC         pos_status,
# MAGIC         s_modified_on,
# MAGIC         -- CASE
# MAGIC         --     WHEN current_location_code IS NULL OR length(current_location_code) = 0
# MAGIC         --         THEN machine_center_no
# MAGIC         --     WHEN upper(substr(current_location_code, 1, 4)) = 'CELL'
# MAGIC         --         THEN machine_center_no
# MAGIC         --     WHEN current_location_code = 'CASTING ROOM'
# MAGIC         --         THEN machine_center_no
# MAGIC         --     ELSE current_location_code
# MAGIC         -- END AS Prod_Status
# MAGIC         CASE
# MAGIC     WHEN current_location_code IS NULL OR length(current_location_code) = 0
# MAGIC         THEN machine_center_no
# MAGIC     WHEN upper(substr(current_location_code, 1, 4)) = 'CELL'
# MAGIC         THEN machine_center_no
# MAGIC     WHEN upper(current_location_code) LIKE '%ROOM%'
# MAGIC         THEN machine_center_no
# MAGIC     WHEN current_location_code = 'CISP101'
# MAGIC         THEN machine_center_no
# MAGIC     ELSE current_location_code
# MAGIC END AS Prod_Status
# MAGIC     FROM s_src
# MAGIC ),
# MAGIC 
# MAGIC dedup_status AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             ls.*,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY
# MAGIC                     ls.prod_order_no,
# MAGIC                     ls.prod_order_line_no,
# MAGIC                     ls.type_name,
# MAGIC                     ls.Prod_Status
# MAGIC                 ORDER BY
# MAGIC                     ls.created_on DESC
# MAGIC             ) AS rn
# MAGIC         FROM latest_status ls
# MAGIC     ) x
# MAGIC     WHERE x.rn = 1
# MAGIC ),
# MAGIC 
# MAGIC dedup_status_f AS (
# MAGIC     SELECT
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no,
# MAGIC         Prod_Status,
# MAGIC         s_modified_on
# MAGIC     FROM dedup_status
# MAGIC     WHERE
# MAGIC         upper(trim(type_name)) = 'IN LOCATION IN'
# MAGIC         AND upper(trim(open)) = 'YES'
# MAGIC         AND upper(trim(pos_status)) = 'RELEASED'
# MAGIC ),
# MAGIC 
# MAGIC /* ============================================================
# MAGIC    4) Join prod_order + latest status
# MAGIC    ============================================================ */
# MAGIC prod_with_latest AS (
# MAGIC     SELECT
# MAGIC         po.*,
# MAGIC         ls.Prod_Status,
# MAGIC         ls.s_modified_on
# MAGIC     FROM prod_order po
# MAGIC     LEFT JOIN dedup_status_f ls
# MAGIC         ON po.prod_order_no = ls.prod_order_no
# MAGIC        AND po.prod_order_line_no = ls.prod_order_line_no
# MAGIC ),
# MAGIC 
# MAGIC /* ============================================================
# MAGIC    5) Join item + casting parts + casting tree
# MAGIC    ============================================================ */
# MAGIC prod_joined AS (
# MAGIC     SELECT
# MAGIC         pl.*,
# MAGIC 
# MAGIC         it.item_category_code AS itemFG_Category,
# MAGIC 
# MAGIC         cp.itemCST,
# MAGIC         cp.casting_prod_order,
# MAGIC         cp.casting_qty_to_tree,
# MAGIC         cp.casting_qty_passed,
# MAGIC         cp.casting_qty_reject,
# MAGIC 
# MAGIC         ct.casting_tree_no,
# MAGIC         ct.casting_status,
# MAGIC 
# MAGIC         cp.cp_modified_on,
# MAGIC         ct.ct_modified_on,
# MAGIC         it.modified_on AS it_modified_on
# MAGIC     FROM prod_with_latest pl
# MAGIC     LEFT JOIN it_src it
# MAGIC         ON pl.prod_item_line = it.item_no
# MAGIC     LEFT JOIN cp_src cp
# MAGIC         ON pl.prod_order_no = cp.prod_order_no
# MAGIC        AND pl.prod_order_line_no = cp.prod_order_line_no
# MAGIC     LEFT JOIN ct_src ct
# MAGIC         ON cp.casting_prod_order = ct.casting_prod_order
# MAGIC ),
# MAGIC 
# MAGIC 
# MAGIC /* ============================================================
# MAGIC    5.1) Filters
# MAGIC    ============================================================ */
# MAGIC prod_filtered AS (
# MAGIC     SELECT *
# MAGIC     FROM prod_joined
# MAGIC     WHERE
# MAGIC         (prod_order_status IS NULL OR prod_order_status IN ('Released','Finished'))
# MAGIC         AND (itemFG_Category IS NULL OR itemFG_Category IN ('FG','CASTING','SEMI-FG'))
# MAGIC ),
# MAGIC 
# MAGIC /* ============================================================
# MAGIC    5.2) Status derivation
# MAGIC    ============================================================ */
# MAGIC prod_final AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CASE
# MAGIC             WHEN casting_status IS NOT NULL THEN upper(casting_status)
# MAGIC             WHEN Prod_Status IS NOT NULL   THEN upper(Prod_Status)
# MAGIC             WHEN itemFG_Category = 'CASTING' THEN 'WAX'
# MAGIC             ELSE 'RELEASED'
# MAGIC         END AS Status
# MAGIC     FROM prod_filtered
# MAGIC ),
# MAGIC 
# MAGIC /* ============================================================
# MAGIC    6) Dedupe by KEYS keep newest
# MAGIC    KEYS = (prod_order_no, prod_order_line_no, casting_prod_order, casting_tree_no)
# MAGIC    ============================================================ */
# MAGIC staged AS (
# MAGIC     SELECT
# MAGIC         sales_order_no, sales_order_line_no, prod_order_no, prod_order_line_no,
# MAGIC         FG_item_no, item_routing_no, prod_order_starting_date_time, prod_order_ending_date_time,
# MAGIC         prod_order_due_date, commit_week, prod_line_due_date, prod_order_finished_date,
# MAGIC         prod_order_quantity, prod_order_status, ref_prod_order, ref_item,
# MAGIC         prod_line_start_date, prod_line_end_date, prod_line_quantity, prod_line_finished_quantity,
# MAGIC         prod_line_remaining_quantity, item_location, prod_item_line, SOL, POL,
# MAGIC 
# MAGIC         Prod_Status,
# MAGIC         itemFG_Category,
# MAGIC         item_size,
# MAGIC 
# MAGIC         itemCST,
# MAGIC         casting_prod_order, casting_qty_to_tree, casting_qty_passed, casting_qty_reject,
# MAGIC         casting_tree_no, casting_status, Status,
# MAGIC 
# MAGIC         greatest(
# MAGIC             cast(po_modified_on AS timestamp),
# MAGIC             cast(pl_modified_on AS timestamp),
# MAGIC             cast(s_modified_on  AS timestamp),
# MAGIC             cast(cp_modified_on AS timestamp),
# MAGIC             cast(ct_modified_on AS timestamp),
# MAGIC             cast(it_modified_on AS timestamp)
# MAGIC         ) AS _modified_any
# MAGIC     FROM prod_final
# MAGIC ),
# MAGIC 
# MAGIC staged_dedup AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             s.*,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY
# MAGIC                     s.prod_order_no,
# MAGIC                     s.prod_order_line_no,
# MAGIC                     s.casting_prod_order,
# MAGIC                     s.casting_tree_no
# MAGIC                 ORDER BY
# MAGIC                     s._modified_any DESC,
# MAGIC                     xxhash64(
# MAGIC                         concat(
# MAGIC                             coalesce(cast(s.prod_order_no AS string), ''),
# MAGIC                             '|', coalesce(cast(s.prod_order_line_no AS string), ''),
# MAGIC                             '|', coalesce(cast(s.casting_prod_order AS string), ''),
# MAGIC                             '|', coalesce(cast(s.casting_tree_no AS string), ''),
# MAGIC                             '|', coalesce(cast(s.item_size AS string), ''),
# MAGIC                             '|', coalesce(cast(s.Status AS string), '')
# MAGIC                         )
# MAGIC                     ) DESC
# MAGIC             ) AS rn
# MAGIC         FROM staged s
# MAGIC     ) x
# MAGIC     WHERE x.rn = 1
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     sales_order_no, sales_order_line_no, prod_order_no, prod_order_line_no,
# MAGIC     FG_item_no, item_routing_no, prod_order_starting_date_time, prod_order_ending_date_time,
# MAGIC     prod_order_due_date, commit_week, prod_line_due_date, prod_order_finished_date,
# MAGIC     prod_order_quantity, prod_order_status, ref_prod_order, ref_item,
# MAGIC     prod_line_start_date, prod_line_end_date, prod_line_quantity, prod_line_finished_quantity,
# MAGIC     prod_line_remaining_quantity, item_location, prod_item_line, SOL, POL,
# MAGIC     CONCAT(sales_order_no, FG_item_no) AS SOI,
# MAGIC     Prod_Status,
# MAGIC     itemFG_Category,
# MAGIC     item_size,
# MAGIC 
# MAGIC     itemCST,
# MAGIC     casting_prod_order, casting_qty_to_tree, casting_qty_passed, casting_qty_reject,
# MAGIC     casting_tree_no, casting_status, Status,
# MAGIC 
# MAGIC     _modified_any
# MAGIC FROM staged_dedup
# MAGIC ;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Casting Output

# MARKDOWN ********************

# ## All Silver

# CELL ********************

# ==============================================================
# casting_output — Materialized Delta Table (incremental MERGE)
# ==============================================================

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# ------------------ CONFIG ------------------
SRC_PARTS = "Silver_Production_Lakehouse.prod.silver_casting_parts"
SRC_TREE  = "Silver_Production_Lakehouse.prod.silver_casting_tree"
TARGET    = "Gold_Production_Lakehouse.prod.gold_casting_output"

MODCOL = "_modified_any"

# Business keys = your GROUP BY columns (must match 1:1)
KEYS = [
    "created_on",
    "prod_order_no",
    "prod_order_line_no",
    "casting_prod_order",
    "item_no",
    "casting_tree_no",
    "casting_status",
]

# ------------------ HELPERS ------------------
def table_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except:
        return False

def create_like(name: str, df):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {name.rsplit('.',1)[0]}")
    df.limit(0).write.format("delta").mode("overwrite").saveAsTable(name)

def maintain(name: str, zcols=None):
    try:
        if zcols:
            spark.sql(f"OPTIMIZE {name} ZORDER BY ({', '.join([f'`{c}`' for c in zcols])})")
        else:
            spark.sql(f"OPTIMIZE {name}")
    except Exception as e:
        print(f"OPTIMIZE notice: {e}")
        spark.sql(f"OPTIMIZE {name}")
    spark.sql(f"VACUUM {name} RETAIN 168 HOURS")

# ------------------ 1) LOAD & JOIN (SQL-parity) ------------------
p = spark.table(SRC_PARTS).alias("p").select(
    "prod_order_no",
    "prod_order_line_no",
    "casting_prod_order",
    "item_no",
    "casting_qty_to_tree",
)

t = spark.table(SRC_TREE).alias("t").select(
    "created_on",
    "casting_prod_order",
    "casting_tree_no",
    "casting_status",
)

# LEFT JOIN + WHERE t.casting_status='Complete' ==> effectively INNER JOIN
joined = (
    p.join(t, on=(F.col("p.casting_prod_order") == F.col("t.casting_prod_order")), how="inner")
     .filter(F.col("t.casting_status") == F.lit("Complete"))
)

# ------------------ 2) AGGREGATION ------------------
agg = (
    joined.groupBy(
        F.col("t.created_on").alias("created_on"),
        F.col("p.prod_order_no").alias("prod_order_no"),
        F.col("p.prod_order_line_no").alias("prod_order_line_no"),
        F.col("p.casting_prod_order").alias("casting_prod_order"),
        F.col("p.item_no").alias("item_no"),
        F.col("t.casting_tree_no").alias("casting_tree_no"),
        F.col("t.casting_status").alias("casting_status"),
    )
    .agg(
        F.sum(
            F.when(F.substring(F.col("t.casting_tree_no"), 1, 3) != F.lit("SUB"),
                   F.col("p.casting_qty_to_tree")).otherwise(F.lit(0))
        ).alias("CountUniquePart_NotSUB"),
        F.sum(
            F.when(F.substring(F.col("t.casting_tree_no"), 1, 3) == F.lit("SUB"),
                   F.col("p.casting_qty_to_tree")).otherwise(F.lit(0))
        ).alias("CountUniquePart_SUB"),
    )
    .withColumn(MODCOL, F.current_timestamp())
)

# ------------------ 3) INCREMENTAL UPSERT (MERGE) ------------------
if not table_exists(TARGET):
    create_like(TARGET, agg)
    print(f"Created table: {TARGET}")

target_dt = DeltaTable.forName(spark, TARGET)

merge_cond = " AND ".join([f"t.`{k}` = s.`{k}`" for k in KEYS])

(
    target_dt.alias("t")
    .merge(agg.alias("s"), merge_cond)
    .whenMatchedUpdate(set={
        "CountUniquePart_NotSUB": "s.CountUniquePart_NotSUB",
        "CountUniquePart_SUB": "s.CountUniquePart_SUB",
        MODCOL: f"s.{MODCOL}",
    })
    .whenNotMatchedInsert(values={
        "created_on": "s.created_on",
        "prod_order_no": "s.prod_order_no",
        "prod_order_line_no": "s.prod_order_line_no",
        "casting_prod_order": "s.casting_prod_order",
        "item_no": "s.item_no",
        "casting_tree_no": "s.casting_tree_no",
        "casting_status": "s.casting_status",
        "CountUniquePart_NotSUB": "s.CountUniquePart_NotSUB",
        "CountUniquePart_SUB": "s.CountUniquePart_SUB",
        MODCOL: f"s.{MODCOL}",
    })
    .execute()
)

# optional housekeeping
maintain(TARGET, zcols=KEYS)

print(f"Incremental upsert complete → {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Casting Timeline

# MARKDOWN ********************

# ## All Silver

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# ---------- Config ----------
CATALOG = None  # e.g. "ENG-Bronze" if needed; set to None if your silver DB is in the current catalog
SILVER_DB = "Silver_Production_Lakehouse"
SILVER_SCHEMA = "prod"

LOG_TBL   = f"{SILVER_DB}.{SILVER_SCHEMA}.silver_casting_log"
PARTS_TBL = f"{SILVER_DB}.{SILVER_SCHEMA}.silver_casting_parts"
TREE_TBL  = f"{SILVER_DB}.{SILVER_SCHEMA}.silver_casting_tree"

TGT_TBL_NAME = "gold_casting_timeline"
TGT_FULL = f"Gold_Production_Lakehouse.prod.gold_casting_timeline"

# Helper to backtick a possibly hyphenated catalog.table
def fq(tbl):
    # Accept qualified names like "db.schema.table" or with catalog prefix.
    # If you actually have a catalog (with hyphen), set CATALOG="ENG-Bronze"
    if CATALOG:
        return f"`{CATALOG}`.{tbl}"
    return tbl

# ---------- Utilities ----------
def table_exists(fullname: str) -> bool:
    try:
        _ = spark.table(fullname).limit(1).count()
        return True
    except AnalysisException:
        return False

def first_existing(colnames, df):
    """Return the first column from colnames that exists in df, else None."""
    existing = set(df.columns)
    for c in colnames:
        if c in existing:
            return F.col(c)
    return None

def change_ts_expr(df):
    """
    Build a 'change timestamp' expression from typical columns:
    modified_on, updated_on, last_modified, created_on, created_at, _ingest_ts
    """
    candidates = ["modified_on", "updated_on", "last_modified", "created_on", "created_at", "_ingest_ts"]
    cols = [first_existing([c], df) for c in candidates]
    cols = [c for c in cols if c is not None]
    if not cols:
        # last resort: current_timestamp() so we don't break (but this will force full reloads)
        return F.current_timestamp()
    return F.coalesce(*cols)

# ---------- Target watermark (max source ts we've processed) ----------
def get_target_max_source_ts():
    if not table_exists(TGT_FULL):
        return None
    df = spark.table(TGT_FULL)
    if "source_max_ts" in df.columns:
        row = df.agg(F.max("source_max_ts").alias("mx")).collect()[0]
        return row["mx"]
    return None

target_watermark = get_target_max_source_ts()

# ---------- Load sources ----------
log_df   = spark.table(fq(LOG_TBL))
parts_df = spark.table(fq(PARTS_TBL))
tree_df  = spark.table(fq(TREE_TBL))

# Build change_ts per source (used both for watermark and for recompute set)
log_change_ts   = change_ts_expr(log_df).alias("change_ts")
parts_change_ts = change_ts_expr(parts_df).alias("change_ts")
tree_change_ts  = change_ts_expr(tree_df).alias("change_ts")

# ---------- Determine affected orders (incremental set) ----------
def changed_orders(df, key_col, ts_expr, watermark):
    d = df.select(F.col(key_col).alias("casting_prod_order"), ts_expr)
    if watermark is not None:
        d = d.filter(F.col("change_ts") > F.lit(watermark))
    return d.select("casting_prod_order").distinct()

if target_watermark is None:
    # First run: all orders from any source
    candidate_orders = (
        log_df.select("casting_prod_order").distinct()
        .unionByName(parts_df.select("casting_prod_order").distinct(), allowMissingColumns=True)
        .unionByName(tree_df.select("casting_prod_order").distinct(), allowMissingColumns=True)
        .distinct()
    )
else:
    cand_log   = changed_orders(log_df,   "casting_prod_order", log_change_ts,   target_watermark)
    cand_parts = changed_orders(parts_df, "casting_prod_order", parts_change_ts, target_watermark)
    cand_tree  = changed_orders(tree_df,  "casting_prod_order", tree_change_ts,  target_watermark)
    candidate_orders = cand_log.unionByName(cand_parts).unionByName(cand_tree).distinct()

# Short-circuit if nothing changed
if candidate_orders.limit(1).count() == 0:
    print("No changes since last run — target is up to date.")
else:
    candidate_orders.createOrReplaceTempView("affected_orders")

    # ---------- Step 1: step_date (Bangkok time, first time per normalized step) ----------
    # Normalize statuses and map to canonical step keys (typo/case tolerant for WAX step)
    norm_log = (
        log_df.alias("l")
        .join(F.broadcast(candidate_orders), on="casting_prod_order", how="inner")
        .withColumn("status_norm", F.trim(F.lower(F.col("l.casting_status"))))
        .withColumn(
            "status_key",
            F.when(
                # tolerate "casting/carrsting" and flexible whitespace
                F.col("status_norm").rlike(r"^start\s+new\s+ca[rs]*ting\s+tree$"), F.lit("WAX_TREE")
            ).when(F.col("status_norm") == F.lit("cutting transfer"),        F.lit("CASTING_FINISHED"))
             .when(F.col("status_norm") == F.lit("casting output"),          F.lit("CUTTING_FINISHED"))
             .when(F.col("status_norm") == F.lit("post casting parts"),      F.lit("WAREHOUSE_OUTPUT"))
             .otherwise(F.lit(None))
        )
        .withColumn(
            "created_on_bkk_date",
            F.to_date(F.from_utc_timestamp(F.col("l.created_on"), "Asia/Bangkok"))
        )
    )

    # Earliest date per (order, step)
    step_date_df = (
        norm_log
        .filter(F.col("status_key").isNotNull())
        .groupBy("casting_prod_order", "status_key")
        .agg(F.min("created_on_bkk_date").alias("step_date"))
    )

    # ---------- Step 2: pivot to columns ----------
    pivot_time_df = (
        step_date_df.groupBy("casting_prod_order").agg(
            F.max(F.when(F.col("status_key") == F.lit("WAX_TREE"),           F.col("step_date"))).alias("WAX_TREE"),
            F.max(F.when(F.col("status_key") == F.lit("CASTING_FINISHED"),   F.col("step_date"))).alias("CASTING_FINISHED"),
            F.max(F.when(F.col("status_key") == F.lit("CUTTING_FINISHED"),   F.col("step_date"))).alias("CUTTING_FINISHED"),
            F.max(F.when(F.col("status_key") == F.lit("WAREHOUSE_OUTPUT"),   F.col("step_date"))).alias("WAREHOUSE_OUTPUT"),
        )
    )

    # ---------- Step 2b: derive WAX_TREE error + display ----------
    # Did we ever see a WAX-like status for each order?
    wax_seen_df = (
        norm_log
        .withColumn("is_wax_like", F.col("status_norm").rlike(r"^start\s+new\s+ca[rs]*ting\s+tree$"))
        .groupBy("casting_prod_order")
        .agg(F.max(F.col("is_wax_like").cast("int")).alias("has_wax_like"))
        .select("casting_prod_order", (F.col("has_wax_like") == 1).alias("has_wax_like"))
    )

    # For orders where the status exists but date is still null, point to missing/invalid created_on
    wax_with_created_on_info = (
        norm_log
        .filter(F.col("status_norm").rlike(r"^start\s+new\s+ca[rs]*ting\s+tree$"))
        .groupBy("casting_prod_order")
        .agg(
            F.max(F.col("l.created_on").isNull().cast("int")).alias("any_created_on_null"),
            F.count("*").alias("wax_rows")
        )
        .withColumn("any_created_on_null", F.col("any_created_on_null") == 1)
    )

    pivot_time_df = (
        pivot_time_df
        .join(wax_seen_df, on="casting_prod_order", how="left")
        .join(wax_with_created_on_info, on="casting_prod_order", how="left")
        .withColumn(
            "WAX_TREE_ERROR",
            F.when(F.col("WAX_TREE").isNotNull(), F.lit(None).cast("string"))
             .when(F.coalesce(F.col("has_wax_like"), F.lit(False)) == F.lit(False),
                   F.lit('No matching "Start New Casting Tree" status (check spelling/typos).'))
             .when(F.col("any_created_on_null") == F.lit(True),
                   F.lit('Missing/NULL created_on for WAX step (cannot compute Bangkok date).'))
             .otherwise(F.lit("WAX step present but date could not be derived."))
        )
        .withColumn(
            "WAX_TREE_DISPLAY",
            F.when(F.col("WAX_TREE").isNotNull(), F.date_format(F.col("WAX_TREE"), "yyyy-MM-dd"))
             .otherwise(F.col("WAX_TREE_ERROR"))
        )
        .drop("has_wax_like", "any_created_on_null", "wax_rows")
    )

    # ---------- Step 3: qty per order ----------
    # Note: MIN(tree_no) for the single "tree_no" field in the final result
    qty_df = (
        parts_df.alias("p")
        .join(F.broadcast(candidate_orders), on="casting_prod_order", how="inner")
        .join(tree_df.alias("t"), on="casting_prod_order", how="left")
        .groupBy("p.casting_prod_order")
        .agg(
            F.min("p.prod_order_no").alias("prod_order_no"),
            F.min("p.prod_order_line_no").alias("prod_order_line_no"),
            F.min("p.item_no").alias("item_no"),
            F.min("t.casting_tree_no").alias("tree_no"),

            F.sum("p.casting_qty_to_tree").alias("qty_to_tree"),
            F.sum("p.casting_qty_passed").alias("qty_good"),
            F.sum(F.col("p.casting_qty_to_tree") - F.col("p.casting_qty_passed")).alias("qty_reject"),

            F.sum(F.when(F.substring(F.col("t.casting_tree_no"), 1, 3) != F.lit("SUB"), F.col("p.casting_qty_to_tree")).otherwise(F.lit(0))).alias("qty_to_tree_NotSUB"),
            F.sum(F.when(F.substring(F.col("t.casting_tree_no"), 1, 3) == F.lit("SUB"), F.col("p.casting_qty_to_tree")).otherwise(F.lit(0))).alias("qty_to_tree_SUB"),

            F.countDistinct(F.when(F.substring(F.col("t.casting_tree_no"), 1, 3) != F.lit("SUB"), F.col("t.casting_tree_no"))).alias("TreeNo_NotSUB"),
            F.countDistinct(F.when(F.substring(F.col("t.casting_tree_no"), 1, 3) == F.lit("SUB"), F.col("t.casting_tree_no"))).alias("TreeNo_SUB"),
        )
    )

    # ---------- Step 4: final projection ----------
    final_df = (
        qty_df.alias("q")
        .join(pivot_time_df.alias("pt"), on="casting_prod_order", how="left")
        .select(
            F.col("q.casting_prod_order"),
            F.col("q.tree_no"),

            # raw dates
            F.col("pt.WAX_TREE"),
            F.col("pt.CASTING_FINISHED"),
            F.col("pt.CUTTING_FINISHED"),
            F.col("pt.WAREHOUSE_OUTPUT"),

            # For month grouping & chronological sorting
            F.date_format(F.col("pt.WAX_TREE"), "yyyy-MM").alias("Month_Of_WAX_TREE"),

            F.col("q.TreeNo_NotSUB"),
            F.col("q.TreeNo_SUB"),
            F.col("q.qty_to_tree"),
            F.col("q.qty_good"),
            F.col("q.qty_reject"),
            F.col("q.qty_to_tree_NotSUB"),
            F.col("q.qty_to_tree_SUB"),

             # new: error & display for WAX
            F.col("pt.WAX_TREE_ERROR"),
            F.col("pt.WAX_TREE_DISPLAY"),

        )
    )

    # ---------- Watermark (max source ts observed this run) ----------
    log_mx   = log_df.select(log_change_ts).agg(F.max("change_ts").alias("mx")).collect()[0]["mx"]
    parts_mx = parts_df.select(parts_change_ts).agg(F.max("change_ts").alias("mx")).collect()[0]["mx"]
    tree_mx  = tree_df.select(tree_change_ts).agg(F.max("change_ts").alias("mx")).collect()[0]["mx"]
    mx_list = [d for d in [log_mx, parts_mx, tree_mx] if d is not None]

    if mx_list:
        overall_mx = max(mx_list)
        final_df = final_df.withColumn("source_max_ts", F.lit(overall_mx))
    else:
        # If we couldn't find any source timestamps, stamp now to avoid NULLs
        final_df = final_df.withColumn("source_max_ts", F.current_timestamp())

    # ---------- Create or MERGE ----------
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB}")
    spark.sql(f"USE {SILVER_DB}")

    if not table_exists(TGT_FULL):
        (final_df
           .write
           .format("delta")
           .mode("overwrite")
           .option("overwriteSchema", "true")
           .saveAsTable(TGT_FULL)
        )
        print(f"Created table {TGT_FULL}")
    else:
        # SCD-1 upsert on casting_prod_order
        final_df.createOrReplaceTempView("casting_timeline_src")
        spark.sql(f"""
            MERGE INTO {TGT_FULL} AS t
            USING casting_timeline_src AS s
            ON t.casting_prod_order = s.casting_prod_order
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"Merged into {TGT_FULL}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Casting Machine Cycle Time

# MARKDOWN ********************

# ## All Silver

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# ===================== Config =====================
SILVER_DB = "Silver_Production_Lakehouse"
SILVER_SCHEMA = "prod"
LOG_TBL  = f"{SILVER_DB}.{SILVER_SCHEMA}.silver_casting_log"
TREE_TBL = f"{SILVER_DB}.{SILVER_SCHEMA}.silver_casting_tree"

GOLD_DB = "Gold_Production_Lakehouse"
GOLD_SCHEMA = "prod"
TGT_TABLE = "gold_casting_matching_cycle_time"
TGT_FULL  = f"{GOLD_DB}.{GOLD_SCHEMA}.{TGT_TABLE}"

# If status column differs (e.g., "module_name"), change this:
STATUS_COL = "casting_status"

# ===================== Helpers =====================
def table_exists(name: str) -> bool:
    try:
        spark.table(name).limit(1).count()
        return True
    except AnalysisException:
        return False

def get_target_watermark():
    """Use max(source_max_ts) from the target as watermark for incremental runs."""
    if not table_exists(TGT_FULL):
        return None
    df = spark.table(TGT_FULL)
    if "source_max_ts" not in df.columns:
        return None
    return df.agg(F.max("source_max_ts").alias("mx")).collect()[0]["mx"]

def first_existing(df, names):
    for n in names:
        if n in df.columns:
            return F.col(n)
    return None

def change_ts(df):
    """Prefer modified_on; fallback to updated_on/created_on/_ingest_ts."""
    expr = first_existing(df, ["modified_on","updated_on","last_modified","created_on","created_at","_ingest_ts"])
    return expr if expr is not None else F.current_timestamp()

def minutes_between(a, b):
    # returns minutes as double
    return (F.unix_timestamp(b) - F.unix_timestamp(a)) / F.lit(60.0)

# ===================== Load sources =====================
log_df  = spark.table(LOG_TBL)
tree_df = spark.table(TREE_TBL)

# sanity checks
need_log_cols  = ["casting_prod_order", STATUS_COL, "created_on"]
miss_log = [c for c in need_log_cols if c not in log_df.columns]
if miss_log:
    raise ValueError(f"silver_casting_log is missing required columns: {miss_log}")

need_tree_cols = ["casting_prod_order", "casting_tree_no"]
miss_tree = [c for c in need_tree_cols if c not in tree_df.columns]
if miss_tree:
    raise ValueError(f"silver_casting_tree is missing required columns: {miss_tree}")

# ===================== Incremental set (orders + trees) =====================
wm = get_target_watermark()
log_chg  = change_ts(log_df).alias("change_ts")
tree_chg = change_ts(tree_df).alias("change_ts")

# From log: we only know order; join to trees to get (order, tree)
affected_from_log = (
    log_df.select("casting_prod_order", log_chg)
          .filter(F.col("change_ts") > F.lit(wm)) if wm is not None
    else log_df.select("casting_prod_order")
).select("casting_prod_order").distinct()

affected_pairs_from_log = (
    affected_from_log.alias("o")
    .join(tree_df.select("casting_prod_order","casting_tree_no").distinct(),
          on="casting_prod_order", how="inner")
    .select("casting_prod_order","casting_tree_no").distinct()
)

# From tree directly
affected_pairs_from_tree = (
    tree_df.select("casting_prod_order","casting_tree_no", tree_chg)
           .filter(F.col("change_ts") > F.lit(wm)) if wm is not None
    else tree_df.select("casting_prod_order","casting_tree_no")
).select("casting_prod_order","casting_tree_no").distinct()

# Union
affected_pairs = affected_pairs_from_log.unionByName(affected_pairs_from_tree).distinct()

# First run → all current pairs
if wm is None:
    affected_pairs = tree_df.select("casting_prod_order","casting_tree_no").distinct()

if affected_pairs.limit(1).count() == 0:
    print("No changes since last run — target is up to date.")
else:
    # ===================== Step 1: first local time per (order, tree, status) =====================
    # created_on assumed UTC → convert to Asia/Bangkok
    step_time_df = (
        log_df.alias("l")
        .join(tree_df.alias("t"), on="casting_prod_order", how="inner")
        .join(F.broadcast(affected_pairs).alias("a"),
              on=["casting_prod_order","casting_tree_no"], how="inner")
        .groupBy("l.casting_prod_order",
                 "t.casting_tree_no",
                 F.col(f"l.{STATUS_COL}").alias("casting_status"))
        .agg(F.min(F.from_utc_timestamp(F.col("l.created_on"), "Asia/Bangkok")).alias("step_time_local"))
    )

    # ===================== Step 2: pivot statuses to columns per (order, tree) =====================
    pt = (
        step_time_df.groupBy("casting_prod_order","casting_tree_no").agg(
            F.max(F.when(F.col("casting_status") == F.lit("Start New Casting Tree"), F.col("step_time_local"))).alias("t_START_NEW"),
            F.max(F.when(F.col("casting_status") == F.lit("Casting Transfer"),       F.col("step_time_local"))).alias("t_CASTING_TRANSFER"),
            F.max(F.when(F.col("casting_status") == F.lit("Cutting Transfer"),       F.col("step_time_local"))).alias("t_CUTTING_TRANSFER"),
            F.max(F.when(F.col("casting_status") == F.lit("Casting Output"),         F.col("step_time_local"))).alias("t_CASTING_OUTPUT"),
            F.max(F.when(F.col("casting_status") == F.lit("POST CASTING PARTS"),     F.col("step_time_local"))).alias("t_POST_CASTING_PARTS"),
        )
    )

    # ===================== Step 3: long-form 4 phases with durations =====================
    # 1) Start New → Casting Transfer
    p1 = (
        pt.select(
            "casting_prod_order", "casting_tree_no",
            F.lit("StartNew_to_CastingTransfer").alias("phase"),
            F.col("t_START_NEW").alias("start_time"),
            F.col("t_CASTING_TRANSFER").alias("end_time"),
            F.when(F.col("t_START_NEW").isNotNull() & F.col("t_CASTING_TRANSFER").isNotNull(),
                   minutes_between(F.col("t_START_NEW"), F.col("t_CASTING_TRANSFER")) / 60.0
            ).alias("cycle_hours"),
            F.lit(1).alias("phase_order")
        )
    )

    # 2) Casting Transfer → Cutting Transfer
    p2 = (
        pt.select(
            "casting_prod_order", "casting_tree_no",
            F.lit("CastingTransfer_to_CuttingTransfer").alias("phase"),
            F.col("t_CASTING_TRANSFER").alias("start_time"),
            F.col("t_CUTTING_TRANSFER").alias("end_time"),
            F.when(F.col("t_CASTING_TRANSFER").isNotNull() & F.col("t_CUTTING_TRANSFER").isNotNull(),
                   minutes_between(F.col("t_CASTING_TRANSFER"), F.col("t_CUTTING_TRANSFER")) / 60.0
            ).alias("cycle_hours"),
            F.lit(2).alias("phase_order")
        )
    )

    # 3) Cutting Transfer → Casting Output
    p3 = (
        pt.select(
            "casting_prod_order", "casting_tree_no",
            F.lit("CuttingTransfer_to_CastingOutput").alias("phase"),
            F.col("t_CUTTING_TRANSFER").alias("start_time"),
            F.col("t_CASTING_OUTPUT").alias("end_time"),
            F.when(F.col("t_CUTTING_TRANSFER").isNotNull() & F.col("t_CASTING_OUTPUT").isNotNull(),
                   minutes_between(F.col("t_CUTTING_TRANSFER"), F.col("t_CASTING_OUTPUT")) / 60.0
            ).alias("cycle_hours"),
            F.lit(3).alias("phase_order")
        )
    )

    # 4) Casting Output → POST CASTING PARTS
    p4 = (
        pt.select(
            "casting_prod_order", "casting_tree_no",
            F.lit("CastingOutput_to_PostCastingParts").alias("phase"),
            F.col("t_CASTING_OUTPUT").alias("start_time"),
            F.col("t_POST_CASTING_PARTS").alias("end_time"),
            F.when(F.col("t_CASTING_OUTPUT").isNotNull() & F.col("t_POST_CASTING_PARTS").isNotNull(),
                   minutes_between(F.col("t_CASTING_OUTPUT"), F.col("t_POST_CASTING_PARTS")) / 60.0
            ).alias("cycle_hours"),
            F.lit(4).alias("phase_order")
        )
    )

    long_df = p1.unionByName(p2).unionByName(p3).unionByName(p4)
    long_df = long_df.filter(F.col("cycle_hours").isNotNull())

    # extra metrics + monthly grouping (as in your SQL)
    long_df = (
        long_df
        .withColumn("cycle_minutes", (F.unix_timestamp("end_time") - F.unix_timestamp("start_time")))
        .withColumn("cycle_whole_hours", F.floor(F.col("cycle_minutes") / 60))
        .withColumn("cycle_days", F.floor(F.col("cycle_minutes") / 1440))
        .withColumn("start_date", F.to_date("start_time"))
        .withColumn("start_month_yyyy_mm", F.date_format(F.col("start_time"), "yyyy-MM"))
        .orderBy("casting_prod_order", "casting_tree_no", "phase_order", "start_time")
    )

    # ===================== Watermark for next run =====================
    log_mx  = log_df.select(change_ts(log_df).alias("ts")).agg(F.max("ts").alias("mx")).collect()[0]["mx"]
    tree_mx = tree_df.select(change_ts(tree_df).alias("ts")).agg(F.max("ts").alias("mx")).collect()[0]["mx"]
    src_max_ts = max([d for d in [log_mx, tree_mx] if d is not None]) if any([log_mx, tree_mx]) else None
    if src_max_ts is None:
        src_max_ts = F.current_timestamp()
    long_df = long_df.withColumn("source_max_ts", F.lit(src_max_ts))

    # ===================== Create or MERGE (SCD-1) =====================
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}")
    spark.sql(f"USE {GOLD_DB}")

    if not table_exists(TGT_FULL):
        (long_df
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            # .partitionBy("start_month_yyyy_mm")  # optional if the table grows big
            .saveAsTable(TGT_FULL)
        )
        print(f"Created table {TGT_FULL}")
    else:
        long_df.createOrReplaceTempView("gold_cycle_long_src")
        spark.sql(f"""
            MERGE INTO {TGT_FULL} AS t
            USING gold_cycle_long_src AS s
            ON  t.casting_prod_order = s.casting_prod_order
            AND t.casting_tree_no    = s.casting_tree_no
            AND t.phase              = s.phase
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"Merged into {TGT_FULL}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Casting Header Line

# MARKDOWN ********************

# ## All Silver

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# ------------------------------------------------------------------
# 1. Config
# ------------------------------------------------------------------
silver_parts_table = "Silver_Production_Lakehouse.prod.silver_casting_parts"
silver_tree_table  = "Silver_Production_Lakehouse.prod.silver_casting_tree"
gold_table_name    = "Gold_Production_Lakehouse.prod.gold_casting_header_line"

# ------------------------------------------------------------------
# 2. Build the source dataframe (same logic as your view)
# ------------------------------------------------------------------
def build_source_df():
    p = spark.table(silver_parts_table).alias("p")
    t = spark.table(silver_tree_table).alias("t")

    return (
        p.join(
            t,
            on=F.col("p.casting_prod_order") == F.col("t.casting_prod_order"),
            how="left"
        )
        .select(
            F.col("p.created_on"),
            F.col("p.modified_on"),
            F.col("p.prod_order_no"),
            F.col("p.prod_order_line_no"),
            F.col("t.casting_tree_no"),
            F.col("p.casting_prod_order"),
            F.col("p.item_no"),
            F.col("p.casting_qty_to_tree"),
            F.col("p.casting_qty_passed"),
            F.col("p.casting_qty_passed_weight"),
            F.col("p.casting_qty_reject"),
            F.col("p.casting_qty_reject_weight"),
            F.col("p.casting_to_warehouse"),
            F.col("p.casting_warehouse_lot"),
            F.col("t.casting_status"),
            F.col("t.casting_output_status"),
            # Natural key
            F.concat(F.col("p.prod_order_no"), F.col("p.prod_order_line_no")).alias("pol")
        )
    )

# ------------------------------------------------------------------
# 3. Full replace load
# ------------------------------------------------------------------
source_full_df = build_source_df()

(
    source_full_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_table_name)
)

print(f"Full replaced gold table: {gold_table_name}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Casting Repair Status

# MARKDOWN ********************

# ## Gold Casting Header Line + Silver

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead",  "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead",     "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite",    "LEGACY")

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

# Just cause sales data have dates older than 1900s

# ---------------------------------------------------------
# 1) CONFIG (REMAPPED SOURCES)
#    silver_prod_order_header  -> Silver_BC_Lakehouse.bc.`Production Order`
#    silver_prod_order_line    -> Silver_BC_Lakehouse.bc.`Prod Order Line`
# ---------------------------------------------------------
silver_hdr_tbl  = "Silver_BC_Lakehouse.bc.`Production Order`"
silver_line_tbl = "Silver_BC_Lakehouse.bc.`Prod Order Line`"

gold_casting_header_tbl = "Gold_Production_Lakehouse.prod.gold_casting_header_line"
gold_repair_status_tbl  = "Gold_Production_Lakehouse.prod.gold_casting_repair_status"

# Natural key definition (still useful if you want dedupe determinism)
MERGE_KEYS = ["wre_prod_no", "wre_line_no"]  # one row per WRE order + line


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def keep_latest_per_keys(df, keys, order_cols):
    w = Window.partitionBy(*[F.col(k) for k in keys]).orderBy(*order_cols)
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


# ---------------------------------------------------------
# 2) BUILD SOURCE DF (same logical output as your original SQL/job)
#    but reading from BC mirror tables and mapping columns
# ---------------------------------------------------------
def build_source_df():
    # ---- Header mapping (Production Order)
    main = (
        spark.table(silver_hdr_tbl)
        .select(
            F.col("`No.`").alias("prod_order_no"),
            F.col("`For Prod.Order No.`").alias("ref_prod_order"),
            F.col("`SystemCreatedAt`").alias("created_on"),
            F.col("`SystemModifiedAt`").alias("modified_on"),
        )
        .alias("main")
    )

    # ---- Line mapping (Prod Order Line)
    ml = (
        spark.table(silver_line_tbl)
        .select(
            F.col("`Prod. Order No.`").alias("prod_order_no"),
            F.col("`Line No.`").alias("prod_order_line_no"),
            F.col("`Item No.`").alias("item_no"),
            F.col("`Location Code`").alias("item_location"),
            F.col("`Quantity`").alias("prod_line_quantity"),
            F.col("`Finished Quantity`").alias("prod_line_finished_quantity"),
            F.col("`Remaining Quantity`").alias("prod_line_remaining_quantity"),
            F.col("`Due Date`").alias("prod_line_due_date"),
            F.col("`Starting Date-Time`").alias("prod_line_start_date"),
            F.col("`Ending Date-Time`").alias("prod_line_end_date"),
            F.col("`SystemCreatedAt`").alias("created_on"),
            F.col("`SystemModifiedAt`").alias("modified_on"),
        )
        .alias("ml")
    )

    # WRE header/line come from the same BC tables
    wre = main.alias("wre")
    wl  = ml.alias("wl")

    c = spark.table(gold_casting_header_tbl).alias("c")

    df = (
        main
        .join(ml, F.col("ml.prod_order_no") == F.col("main.prod_order_no"), "inner")
        .join(
            wre,
            (F.col("wre.ref_prod_order") == F.col("main.prod_order_no")) &
            (F.col("wre.prod_order_no").like("WRE%")),
            "inner",
        )
        .join(
            wl,
            (F.col("wl.prod_order_no") == F.col("wre.prod_order_no")) &
            (F.col("wl.item_no") == F.col("ml.item_no")),
            "inner",
        )
        .join(
            c,
            (F.col("c.prod_order_no") == F.col("wre.prod_order_no")) &
            (F.col("c.prod_order_line_no") == F.col("wl.prod_order_line_no")),
            "left",
        )
        .select(
            # Main side
            F.col("main.created_on").alias("main_created_on"),
            F.col("main.prod_order_no").alias("main_prod_no"),
            F.col("ml.prod_order_line_no").alias("main_line_no"),
            F.col("ml.item_no").alias("main_item_no"),
            F.col("ml.item_location").alias("main_item_location"),
            F.col("ml.prod_line_quantity").alias("main_qty"),
            F.col("ml.prod_line_finished_quantity").alias("main_finished_qty"),
            F.col("ml.prod_line_remaining_quantity").alias("main_remaining_qty"),
            F.col("ml.prod_line_start_date").alias("main_start_date"),
            F.col("ml.prod_line_end_date").alias("main_end_date"),
            F.col("ml.prod_line_due_date").alias("main_due_date"),

            # WRE side
            F.col("wre.created_on").alias("wre_created_on"),
            F.col("wre.prod_order_no").alias("wre_prod_no"),
            F.col("wl.prod_order_line_no").alias("wre_line_no"),
            F.col("wl.item_no").alias("wre_item_no"),
            F.col("wl.item_location").alias("wre_item_location"),
            F.col("wl.prod_line_quantity").alias("wre_qty"),
            F.col("wl.prod_line_finished_quantity").alias("wre_finished_qty"),
            F.col("wl.prod_line_remaining_quantity").alias("wre_remaining_qty"),
            F.col("wl.prod_line_start_date").alias("wre_start_date"),
            F.col("wl.prod_line_end_date").alias("wre_end_date"),
            F.col("wl.prod_line_due_date").alias("wre_due_date"),

            # Casting gold status (left join)
            F.col("c.casting_prod_order").alias("casting_prod_order"),
            F.col("c.casting_tree_no").alias("casting_tree_no"),
            F.col("c.casting_status").alias("casting_status"),
            F.col("c.casting_qty_passed").alias("casting_qty_passed"),
            F.col("c.casting_qty_reject").alias("casting_qty_reject"),
        )
    )

    return df


# ---------------------------------------------------------
# 3) FULL SOURCE + optional dedupe (recommended)
#    If your joins can create duplicates per (wre_prod_no, wre_line_no),
#    keep only one deterministic row per key.
# ---------------------------------------------------------
source_full_df = build_source_df()

# Optional but recommended: enforce 1 row per key
source_full_df = keep_latest_per_keys(
    source_full_df,
    MERGE_KEYS,
    [
        F.col("wre_created_on").desc_nulls_last(),
        # deterministic tie-breaker
        F.sha2(
            F.concat_ws(
                "§",
                *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in source_full_df.columns]
            ),
            256
        ).desc()
    ]
)

if source_full_df.rdd.isEmpty():
    print("Source produced 0 rows; gold table will not be overwritten.")
else:
    (
        source_full_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(gold_repair_status_tbl)
    )
    print(f"Full replaced: {gold_repair_status_tbl}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #

# MARKDOWN ********************

# # Casting Loss

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC 
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_casting_metal_loss
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'false',
# MAGIC     'delta.autoOptimize.autoCompact'   = 'false'
# MAGIC )
# MAGIC AS
# MAGIC SELECT
# MAGIC     t.`created_on`,
# MAGIC     t.`modified_on`,
# MAGIC     t.`casting_prod_order`,
# MAGIC     t.`casting_tree_no`,
# MAGIC     t.`casting_status`,
# MAGIC     t.`output_item`,
# MAGIC 
# MAGIC     MAX(i.`Metal Category Code`)        AS metal_category_code,
# MAGIC     MAX(m.`metal_name`)                  AS metal_name,
# MAGIC     MAX(m.`wax_to_metal_ration`)         AS wax_to_metal_ration,
# MAGIC     MAX(m.`add_to_base_weight`)          AS add_to_base_weight,
# MAGIC     MAX(m.`tolerance`)                   AS tolerance_limit,
# MAGIC 
# MAGIC     MAX(t.`casting_net_wax_weight`)      AS casting_net_wax_weight,
# MAGIC     MAX(t.`output_item_weight`)          AS output_item_weight,
# MAGIC     MAX(t.`output_item_spure_weight`)    AS output_item_spure_weight,
# MAGIC     MAX(t.`output_item_dust_weight`)     AS output_item_dust_weight,
# MAGIC     MAX(t.`output_item_actual`)          AS output_item_actual,
# MAGIC 
# MAGIC     SUM(p.`casting_qty_passed_weight`)   AS casting_qty_passed_weight,
# MAGIC 
# MAGIC     SUM(p.`casting_qty_passed_weight`)
# MAGIC         + MAX(t.`output_item_weight`)
# MAGIC         + MAX(t.`output_item_spure_weight`)
# MAGIC         + MAX(t.`output_item_dust_weight`)              AS actual_casting_weight,
# MAGIC 
# MAGIC     MAX(t.`casting_net_wax_weight`) * MAX(m.`wax_to_metal_ration`)
# MAGIC         + MAX(m.`add_to_base_weight`)                   AS expected_casting_weight,
# MAGIC 
# MAGIC     ABS(
# MAGIC         (SUM(p.`casting_qty_passed_weight`)
# MAGIC             + MAX(t.`output_item_weight`)
# MAGIC             + MAX(t.`output_item_spure_weight`)
# MAGIC             + MAX(t.`output_item_dust_weight`))
# MAGIC         - (MAX(t.`casting_net_wax_weight`) * MAX(m.`wax_to_metal_ration`)
# MAGIC             + MAX(m.`add_to_base_weight`))
# MAGIC     )                                                   AS tolerance,
# MAGIC 
# MAGIC     (MAX(t.`casting_net_wax_weight`) * MAX(m.`wax_to_metal_ration`)
# MAGIC         + MAX(m.`add_to_base_weight`))
# MAGIC     - (SUM(p.`casting_qty_passed_weight`)
# MAGIC         + MAX(t.`output_item_weight`)
# MAGIC         + MAX(t.`output_item_spure_weight`)
# MAGIC         + MAX(t.`output_item_dust_weight`))             AS metal_loss_gram,
# MAGIC 
# MAGIC     -- ⭐ FIX: เก็บเป็น proportion (0.0014) ไม่ใช่ percent (0.14)
# MAGIC     -- ให้ Power BI / Excel format cell เป็น "0.00%" จะแสดง "0.14%" ถูก
# MAGIC     CASE 
# MAGIC         WHEN (MAX(t.`casting_net_wax_weight`) * MAX(m.`wax_to_metal_ration`)
# MAGIC                 + MAX(m.`add_to_base_weight`)) = 0 THEN NULL
# MAGIC         ELSE
# MAGIC             ((MAX(t.`casting_net_wax_weight`) * MAX(m.`wax_to_metal_ration`)
# MAGIC                 + MAX(m.`add_to_base_weight`))
# MAGIC             - (SUM(p.`casting_qty_passed_weight`)
# MAGIC                 + MAX(t.`output_item_weight`)
# MAGIC                 + MAX(t.`output_item_spure_weight`)
# MAGIC                 + MAX(t.`output_item_dust_weight`)))
# MAGIC             / (MAX(t.`casting_net_wax_weight`) * MAX(m.`wax_to_metal_ration`)
# MAGIC                 + MAX(m.`add_to_base_weight`))
# MAGIC     END                                                 AS metal_loss_pct,
# MAGIC 
# MAGIC     CASE 
# MAGIC         WHEN MAX(m.`wax_to_metal_ration`) IS NULL THEN 'NO_MASTER'
# MAGIC         WHEN (SUM(p.`casting_qty_passed_weight`)
# MAGIC                 + MAX(t.`output_item_weight`)
# MAGIC                 + MAX(t.`output_item_spure_weight`)
# MAGIC                 + MAX(t.`output_item_dust_weight`))
# MAGIC              > (MAX(t.`casting_net_wax_weight`) * MAX(m.`wax_to_metal_ration`)
# MAGIC                 + MAX(m.`add_to_base_weight`)) THEN 'OVER'
# MAGIC         WHEN (SUM(p.`casting_qty_passed_weight`)
# MAGIC                 + MAX(t.`output_item_weight`)
# MAGIC                 + MAX(t.`output_item_spure_weight`)
# MAGIC                 + MAX(t.`output_item_dust_weight`))
# MAGIC              < (MAX(t.`casting_net_wax_weight`) * MAX(m.`wax_to_metal_ration`)
# MAGIC                 + MAX(m.`add_to_base_weight`)) THEN 'LOSS'
# MAGIC         ELSE 'EXACT'
# MAGIC     END                                                 AS loss_direction,
# MAGIC 
# MAGIC     CASE 
# MAGIC         WHEN MAX(m.`wax_to_metal_ration`) IS NULL THEN 'NO_RATIO_MASTER'
# MAGIC         WHEN ABS(
# MAGIC             (SUM(p.`casting_qty_passed_weight`)
# MAGIC                 + MAX(t.`output_item_weight`)
# MAGIC                 + MAX(t.`output_item_spure_weight`)
# MAGIC                 + MAX(t.`output_item_dust_weight`))
# MAGIC             - (MAX(t.`casting_net_wax_weight`) * MAX(m.`wax_to_metal_ration`)
# MAGIC                 + MAX(m.`add_to_base_weight`))
# MAGIC         ) <= MAX(m.`tolerance`) THEN 'PASS'
# MAGIC         ELSE 'OUT_OF_TOLERANCE'
# MAGIC     END                                                 AS tolerance_status,
# MAGIC 
# MAGIC     CURRENT_TIMESTAMP()                                 AS gold_loaded_at
# MAGIC 
# MAGIC FROM Silver_Production_Lakehouse.prod.silver_casting_tree AS t
# MAGIC JOIN Silver_Production_Lakehouse.prod.silver_casting_parts AS p
# MAGIC     ON t.`casting_prod_order` = p.`casting_prod_order`
# MAGIC JOIN Silver_BC_Lakehouse.bc.Item AS i
# MAGIC     ON t.`output_item` = i.`No.`
# MAGIC LEFT JOIN Silver_Inventory_Lakehouse.inv.silver_metal_ratio_master AS m
# MAGIC     ON m.`metal_name` = i.`Metal Category Code`
# MAGIC 
# MAGIC GROUP BY
# MAGIC     t.`created_on`,
# MAGIC     t.`modified_on`,
# MAGIC     t.`casting_prod_order`,
# MAGIC     t.`casting_tree_no`,
# MAGIC     t.`casting_status`,
# MAGIC     t.`output_item`;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
