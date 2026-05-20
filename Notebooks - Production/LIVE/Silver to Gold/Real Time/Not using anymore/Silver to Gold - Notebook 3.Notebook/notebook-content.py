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
# META           "id": "e248ea90-8431-4df2-9f29-87866bf9dd5a"
# META         },
# META         {
# META           "id": "869b263b-1a86-424b-bd97-94bd586442b2"
# META         },
# META         {
# META           "id": "ff4d6787-a716-43b6-baaf-972b7426ffa5"
# META         },
# META         {
# META           "id": "3ea0efcd-03d5-44f1-8e70-99f52a5c2a22"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Casting Output

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
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# # Casting Cycle Time (Not Using)

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# ========= Config =========
SILVER_DB = "Silver_Production_Lakehouse"
SILVER_SCHEMA = "prod"
LOG_TBL = f"{SILVER_DB}.{SILVER_SCHEMA}.silver_casting_log"

TGT_TABLE = "gold_casting_cycletime"
TGT_FULL = f"Gold_Production_Lakehouse.prod.{TGT_TABLE}"

# If your event name column is different (e.g., module_name), set this:
STATUS_COL = "casting_status"   # or "module_name"

# ========= Helpers =========
def table_exists(name: str) -> bool:
    try:
        spark.table(name).limit(1).count()
        return True
    except AnalysisException:
        return False

def get_target_watermark():
    """Use max(source_max_ts) from the target as the watermark for incremental loads."""
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

# ========= Load sources =========
log_df = spark.table(LOG_TBL)

# Build change timestamp & watermark
log_change_ts = change_ts(log_df).alias("change_ts")
watermark = get_target_watermark()

# Determine affected casting orders (incremental set)
cand = log_df.select("casting_prod_order", log_change_ts)
if watermark is not None:
    cand = cand.filter(F.col("change_ts") > F.lit(watermark))
affected_orders = cand.select("casting_prod_order").distinct()

if affected_orders.limit(1).count() == 0:
    print("No changes since last run — target is up to date.")
else:
    # ========= Step 1: first-time (local) per step =========
    # Interpret created_on as UTC and convert to Bangkok local time
    if "created_on" not in log_df.columns:
        raise ValueError("Source is missing 'created_on' column.")

    step_time_df = (
        log_df.alias("l")
        .join(F.broadcast(affected_orders), on="casting_prod_order", how="inner")
        .groupBy("l.casting_prod_order", F.col(f"l.{STATUS_COL}").alias("casting_status"))
        .agg(
            F.min(
                F.from_utc_timestamp(F.col("l.created_on"), "Asia/Bangkok")
            ).alias("step_time_local")
        )
    )

    # ========= Step 2: pivot statuses to columns =========
    # Map the four statuses exactly like your SQL
    pivot_df = (
        step_time_df.groupBy("casting_prod_order").agg(
            F.max(F.when(F.col("casting_status") == F.lit("Start New Casting Tree"), F.col("step_time_local"))).alias("t_WAX_TREE"),
            F.max(F.when(F.col("casting_status") == F.lit("Cutting Transfer"),       F.col("step_time_local"))).alias("t_CASTING_FINISHED"),
            F.max(F.when(F.col("casting_status") == F.lit("Casting Output"),         F.col("step_time_local"))).alias("t_CUTTING_FINISHED"),
            F.max(F.when(F.col("casting_status") == F.lit("POST CASTING PARTS"),     F.col("step_time_local"))).alias("t_WAREHOUSE_OUTPUT"),
        )
    )

    # ========= Step 3: duration calculations =========
    def minutes_between(a, b):
        return (F.unix_timestamp(b) - F.unix_timestamp(a)) / F.lit(60.0)

    # End time priority: warehouse > cutting > casting
    end_time = F.coalesce(
        F.col("t_WAREHOUSE_OUTPUT"),
        F.col("t_CUTTING_FINISHED"),
        F.col("t_CASTING_FINISHED")
    ).alias("end_time")

    result_df = (
        pivot_df
        .select(
            "casting_prod_order",
            F.col("t_WAX_TREE").alias("start_time"),
            end_time
        )
        .join(pivot_df, on="casting_prod_order", how="inner")
        .select(
            F.col("casting_prod_order"),
            F.col("t_WAX_TREE").alias("start_time"),
            F.coalesce(F.col("t_WAREHOUSE_OUTPUT"), F.col("t_CUTTING_FINISHED"), F.col("t_CASTING_FINISHED")).alias("end_time"),

            # Hours segments (nullable if either endpoint is null)
            F.when(F.col("t_WAX_TREE").isNotNull() & F.col("t_CASTING_FINISHED").isNotNull(),
                   minutes_between(F.col("t_WAX_TREE"), F.col("t_CASTING_FINISHED")) / 60.0
            ).alias("cycle_wax_to_casting_hours"),

            F.when(F.col("t_CASTING_FINISHED").isNotNull() & F.col("t_CUTTING_FINISHED").isNotNull(),
                   minutes_between(F.col("t_CASTING_FINISHED"), F.col("t_CUTTING_FINISHED")) / 60.0
            ).alias("cycle_casting_to_cutting_hours"),

            F.when(F.col("t_CUTTING_FINISHED").isNotNull() & F.col("t_WAREHOUSE_OUTPUT").isNotNull(),
                   minutes_between(F.col("t_CUTTING_FINISHED"), F.col("t_WAREHOUSE_OUTPUT")) / 60.0
            ).alias("cycle_cutting_to_wh_hours"),

            # Total hours & days from start to prioritized end
            F.when(
                F.col("t_WAX_TREE").isNotNull() &
                F.coalesce(F.col("t_WAREHOUSE_OUTPUT"), F.col("t_CUTTING_FINISHED"), F.col("t_CASTING_FINISHED")).isNotNull(),
                minutes_between(F.col("t_WAX_TREE"),
                                F.coalesce(F.col("t_WAREHOUSE_OUTPUT"), F.col("t_CUTTING_FINISHED"), F.col("t_CASTING_FINISHED"))) / 60.0
            ).alias("cycle_total_hours"),

            F.when(
                F.col("t_WAX_TREE").isNotNull() &
                F.coalesce(F.col("t_WAREHOUSE_OUTPUT"), F.col("t_CUTTING_FINISHED"), F.col("t_CASTING_FINISHED")).isNotNull(),
                minutes_between(F.col("t_WAX_TREE"),
                                F.coalesce(F.col("t_WAREHOUSE_OUTPUT"), F.col("t_CUTTING_FINISHED"), F.col("t_CASTING_FINISHED"))) / 1440.0
            ).alias("cycle_total_days"),
        )
    )

    # ========= Watermark column for the target =========
    # Max change ts we’ve seen in source (global); good enough for incremental watermarking
    src_max_ts = log_df.select(log_change_ts).agg(F.max("change_ts").alias("mx")).collect()[0]["mx"]
    if src_max_ts is None:
        src_max_ts = F.current_timestamp()
    result_df = result_df.withColumn("source_max_ts", F.lit(src_max_ts))

    # ========= Create or MERGE (SCD-1) =========
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB}")
    spark.sql(f"USE {SILVER_DB}")

    if not table_exists(TGT_FULL):
        (result_df
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(TGT_FULL)
        )
        print(f"Created table {TGT_FULL}")
    else:
        result_df.createOrReplaceTempView("cycle_src")
        spark.sql(f"""
            MERGE INTO {TGT_FULL} AS t
            USING cycle_src AS s
            ON t.casting_prod_order = s.casting_prod_order
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"Merged into {TGT_FULL}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # Casting Machine Cycle Time

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

# # Wax Output

# CELL ********************

# ==============================================================
# gold_wax_output — Materialized Delta Table (team outsource logic)
# ==============================================================

from pyspark.sql import functions as F, Window

# ------------------ CONFIG ------------------
SRC_STATUS = "Gold_Production_Lakehouse.prod.gold_waxing_and_casting_status"
SRC_SO     = "Gold_Production_Lakehouse.prod.gold_sales_order"
TARGET     = "Gold_Production_Lakehouse.prod.gold_wax_output"

MODCOL = "_modified_any"
KEYS   = ["prod_order_no","prod_order_line_no","machine_center_no"]

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

# ------------------ 1) BASE (SQL-parity) ------------------
base = (
    spark.table(SRC_STATUS).alias("e")
    .join(
        spark.table(SRC_SO).alias("s"),
        F.col("e.sales_order_no") == F.col("s.SalesorderNo"),
        "left"
    )
    .select(
        F.col("e.created_on"),
        F.to_date("e.created_on").alias("work_date"),

        # UTC+7 derivatives
        (F.col("e.created_on") + F.expr("INTERVAL 7 HOURS")).alias("created_on_plus7_dt"),
        F.to_date(F.col("e.created_on") + F.expr("INTERVAL 7 HOURS")).alias("work_date_plus7"),
        F.date_format(F.col("e.created_on") + F.expr("INTERVAL 7 HOURS"), "HH:mm:ss").alias("created_time_plus7"),

        F.col("e.team"),
        F.col("e.prod_order_no"),
        F.col("e.prod_order_line_no"),

        # TRY_CAST(REPLACE(TRIM(CAST(operation_no AS varchar)),'.','') AS int)
        F.regexp_replace(F.trim(F.col("e.operation_no").cast("string")), r"\.", "").cast("int").alias("op_seq_clean"),

        F.abs(F.col("e.quantity")).alias("qty"),

        F.col("s.CusAbbr"),
        F.col("s.so_abbr"),
        F.col("e.machine_center_no"),

        # mc_norm = UPPER(TRIM(REPLACE(machine_center_no,'-',' '))) with collapsed spaces
        F.upper(
            F.regexp_replace(
                F.trim(F.regexp_replace(F.col("e.machine_center_no"), "-", " ")),
                r"\s+",
                " "
            )
        ).alias("mc_norm"),

        F.lower(F.trim(F.col("e.type_name"))).alias("type_name_norm"),

        # user_id + normalized
        F.col("e.user_id").alias("user_id"),
        F.lower(F.trim(F.col("e.user_id"))).alias("user_id_norm")
    )
    .filter(F.col("type_name_norm").isin("from employee", "from employee accept", "employee"))
)

# ------------------ 2) RANKED (inference + qty per center) ------------------
cond_inj = F.col("mc_norm").like("%WAX%INJ%")
cond_fil = F.col("mc_norm").like("%WAX%FIL%")
cond_set = F.col("mc_norm").like("%WAX%SET%")

ranked = (
    base
    .withColumn(
        "op_seq_inferred",
        F.when(cond_inj, F.lit(1001))
         .when(cond_fil, F.lit(1002))
         .when(cond_set, F.lit(1003))
         .otherwise(F.lit(None).cast("int"))
    )
    .withColumn("qty_wax_injection", F.when(cond_inj, F.col("qty")).otherwise(F.lit(0)))
    .withColumn("qty_wax_filing",    F.when(cond_fil, F.col("qty")).otherwise(F.lit(0)))
    .withColumn("qty_wax_setting",   F.when(cond_set, F.col("qty")).otherwise(F.lit(0)))
)

# ------------------ 3) SCORED (priority + row_number) ------------------
coalesced = F.coalesce(F.col("op_seq_clean"), F.col("op_seq_inferred"))
priority = (
    F.when((coalesced == F.lit(1001)) & cond_inj, F.lit(0))
     .when((coalesced == F.lit(1002)) & cond_fil, F.lit(0))
     .when((coalesced == F.lit(1003)) & cond_set, F.lit(0))
     .otherwise(F.lit(1))
)

scored = (
    ranked
    .withColumn("op_seq_final", F.coalesce(F.col("op_seq_clean"), F.col("op_seq_inferred"), F.lit(-1)))
    .withColumn("priority", priority)
    .withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("prod_order_no", "prod_order_line_no", "op_seq_final")
                  .orderBy(F.col("priority").asc(), F.col("created_on").asc_nulls_last())
        )
    )
)

# ------------------ 4) FINAL (rn=1, team outsource logic) ------------------
wax_output = (
    scored.filter(F.col("rn") == 1)
          .select(
              # CONVERT(time, created_on_plus7_dt)
              F.date_format(F.col("created_on_plus7_dt"), "HH:mm:ss").alias("created_on_plus7"),
              F.col("work_date_plus7"),
              F.col("created_time_plus7"),

              # CASE WHEN user_id_norm='outsource@ennovie.com' THEN 'Outsource' ELSE CONVERT(varchar(10), team)
              F.when(F.col("user_id_norm") == F.lit("outsource@ennovie.com"), F.lit("O"))
               .otherwise(F.col("team").cast("string")).alias("team"),

              F.col("prod_order_no"),
              F.col("prod_order_line_no"),
              F.col("op_seq_final").alias("op_seq"),
              F.col("machine_center_no"),
              F.col("CusAbbr"),
              F.col("so_abbr"),
              F.col("qty_wax_injection"),
              F.col("qty_wax_filing"),
              F.col("qty_wax_setting"),
          )
          .withColumn(MODCOL, F.current_timestamp())
          .orderBy(F.col("op_seq").asc())
)

# ------------------ 5) CREATE OR REPLACE TABLE ------------------
if not table_exists(TARGET):
    create_like(TARGET, wax_output)
    print(f"Created table: {TARGET}")

wax_output.write.format("delta").mode("overwrite").saveAsTable(TARGET)
maintain(TARGET, zcols=KEYS)

print(f"Table refreshed successfully → {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Wax Cycle Time Compared with Standard

# CELL ********************

# ==============================================================
# gold_wax_cycle_time_compare — Materialized Delta Table (SQL-parity)
# ==============================================================

from pyspark.sql import functions as F, Window

# ------------------ CONFIG ------------------
SRC_STATUS = "Gold_Production_Lakehouse.prod.gold_waxing_and_casting_status"
SRC_ROUTE  = "Silver_Production_Lakehouse.prod.silver_prod_routing_line"
TARGET     = "Gold_Production_Lakehouse.prod.gold_wax_cycle_time_compare"

# Keep source spellings as-is (FILINIG per your data sample)
CENTERS = ["WAX INJECT","WAX FILINIG","WAX SETTING"]
OUTSOURCE_EMAIL = "outsource@ennovie.com"
MODCOL = "_modified_any"

# Z-ORDER keys (no work_date now; we aggregate to last/first dates)
KEYS   = ["prod_order_no","prod_order_line_no","team","machine_center_no","work_type"]

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

# ------------------ 1) LOAD ------------------
base = (
    spark.table(SRC_STATUS)
         .filter(F.col("machine_center_no").isin(CENTERS))
         .select(
             "prod_order_no","prod_order_line_no","created_on","type_name",
             "team","machine_center_no","employee_no","out_qty","user_id","antenna_id"
         )
)

# ------------------ 2) NORMALIZE ------------------
normalized = (
    base.select(
        "prod_order_no","prod_order_line_no","machine_center_no","team",
        "employee_no","created_on","user_id","out_qty","antenna_id",
        F.when(F.col("type_name")=="To employee","Start")
         .when(F.col("type_name")=="From employee","End")
         .alias("event_type")
    ).filter(F.col("event_type").isNotNull())
)

# ------------------ 3) SPLIT STARTS / ENDS ------------------
starts = (
    normalized.filter(F.col("event_type")=="Start")
              .select(
                  "prod_order_no","prod_order_line_no","machine_center_no",
                  "team","employee_no","user_id","antenna_id",
                  F.col("created_on").alias("start_time")
              )
)

ends = (
    normalized.filter(F.col("event_type")=="End")
              .select(
                  "prod_order_no","prod_order_line_no","machine_center_no","antenna_id",
                  F.col("created_on").alias("end_time"),
                  F.coalesce(F.col("out_qty").cast("double"),F.lit(1.0)).alias("qty_out")
              )
)

# ------------------ 4) CLOSEST-END-AFTER-START PAIRING ------------------
w_start_idx = Window.partitionBy("prod_order_no","prod_order_line_no","machine_center_no") \
                    .orderBy(F.col("start_time").asc_nulls_last())

starts_idxed = starts.withColumn("start_id", F.row_number().over(w_start_idx))

candidates = (
    starts_idxed.alias("s")
    .join(
        ends.alias("e"),
        (
            (F.col("s.prod_order_no")==F.col("e.prod_order_no")) &
            (F.col("s.prod_order_line_no")==F.col("e.prod_order_line_no")) &
            (F.col("s.machine_center_no")==F.col("e.machine_center_no")) &
            (F.col("e.end_time") > F.col("s.start_time"))
        ),
        "inner"
    )
    .withColumn(
        "duration_sec",
        (F.col("e.end_time").cast("timestamp").cast("long") - 
         F.col("s.start_time").cast("timestamp").cast("long")).cast("long")
    )
)

w_best = Window.partitionBy(
    "s.prod_order_no","s.prod_order_line_no","s.machine_center_no","s.start_id"
).orderBy(F.col("duration_sec").asc())

paired = (
    candidates
      .select(
          F.col("s.prod_order_no").alias("prod_order_no"),
          F.col("s.prod_order_line_no").alias("prod_order_line_no"),
          F.col("s.machine_center_no").alias("machine_center_no"),
          F.col("s.team").alias("team"),
          F.col("s.employee_no").alias("employee_no"),
          F.col("s.user_id").alias("user_id_start"),
          F.col("s.antenna_id").alias("antenna_id"),
          F.col("s.start_time").alias("start_time"),
          F.col("e.end_time").alias("end_time"),
          F.col("e.qty_out").alias("qty_out"),
          F.col("duration_sec").alias("duration_sec")
      )
      .withColumn("work_date", F.to_date("end_time"))
      .withColumn(
          "work_type",
          F.when(F.lower(F.coalesce(F.col("user_id_start"),F.lit("")))==F.lit(OUTSOURCE_EMAIL), "Outsource")
           .otherwise("In-house")
      )
)

# ------------------ 5) AGGREGATE TO MATCH SQL VIEW ------------------
actual_agg = (
    paired.groupBy("prod_order_no","prod_order_line_no","team","machine_center_no","work_type")
          .agg(
              F.max("work_date").alias("last_exit_date"),
              F.min("work_date").alias("first_entry_date"),
              F.count(F.lit(1)).alias("records"),
              F.sum("duration_sec").alias("sum_sec"),
              F.sum("qty_out").alias("total_qty")
          )
          .withColumn(
              "AvgPerPiece",
              F.when(
                  F.col("total_qty").isNull() | (F.col("total_qty") == F.lit(0.0)),
                  F.lit(None).cast("double")
              ).otherwise( (F.col("sum_sec")/F.lit(60.0)) / F.col("total_qty") )
          )
)

# ------------------ 6) STANDARD TIME (minutes per piece) ------------------
routing_std = (
    spark.table(SRC_ROUTE)
         .select(
             F.col("prod_order_no"),
             F.col("prod_order_line_no"),
             F.col("routing_no").alias("machine_center_no"),
             F.col("run_time").cast("double").alias("run_time_min")
         )
         .groupBy("prod_order_no","prod_order_line_no","machine_center_no")
         .agg(F.sum("run_time_min").alias("standard_min_per_piece"))
)

# ------------------ 7) JOIN + FINAL SHAPE ------------------
compare = (
    actual_agg.alias("a")
              .join(
                  routing_std.alias("r"),
                  [
                      F.col("r.prod_order_no")==F.col("a.prod_order_no"),
                      F.col("r.prod_order_line_no")==F.col("a.prod_order_line_no"),
                      F.col("r.machine_center_no")==F.col("a.machine_center_no")
                  ],
                  "left"
              )
              .select(
                  F.col("a.prod_order_no"),
                  F.col("a.prod_order_line_no"),
                  F.col("a.team"),
                  F.col("a.machine_center_no"),
                  F.col("a.last_exit_date"),
                  F.col("a.first_entry_date"),
                  F.col("a.work_type"),
                  F.col("a.total_qty"),
                  F.col("a.AvgPerPiece").alias("Actual_Min_Per_Piece"),
                  F.col("r.standard_min_per_piece").alias("Standard_Min_Per_Piece"),
                  (F.col("a.AvgPerPiece") - F.col("r.standard_min_per_piece")).alias("Diff_Min_Per_Piece"),
                  F.when(
                      (F.col("r.standard_min_per_piece")>0) & F.col("a.AvgPerPiece").isNotNull(),
                      (F.col("r.standard_min_per_piece") / F.col("a.AvgPerPiece"))*100.0
                  ).alias("Efficiency_Percent"),
                  F.lit("min/piece").alias("Unit"),
                  F.current_timestamp().alias(MODCOL)
              )
)

# ------------------ 8) CREATE OR REPLACE TABLE ------------------
if not table_exists(TARGET):
    create_like(TARGET, compare)
    print(f"Created table: {TARGET}")

compare.write.format("delta").mode("overwrite").saveAsTable(TARGET)
maintain(TARGET, zcols=KEYS)

print(f"Table refreshed successfully → {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Wax Status

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.utils import AnalysisException

# ===================== Config =====================

GOLD_DB   = "Gold_Production_Lakehouse"
SILVER_DB = "Silver_Production_Lakehouse"
SCHEMA    = "prod"
GPO_TBL    = f"{GOLD_DB}.{SCHEMA}.gold_production_order"
STATUS_TBL = f"{SILVER_DB}.{SCHEMA}.silver_prod_order_status"
TGT_FULL   = f"{GOLD_DB}.{SCHEMA}.gold_wax_status"

# ===================== Helpers =====================

def table_exists(name: str) -> bool:
    try:
        spark.table(name).limit(1).count()
        return True
    except AnalysisException:
        return False
def first_existing_name(df, names):
    for n in names:
        if n in df.columns:
            return n
    return None
def change_ts(df):
    candidates = [
        "_modified_any", "modified_on", "updated_on", "last_modified",
        "created_on", "created_at", "_ingest_ts",
        "prod_line_end_date", "prod_line_start_date"
    ]
    name = first_existing_name(df, candidates)
    return F.col(name) if name else F.current_timestamp()
# midpoint timestamp
def midpoint_ts(start_col, end_col):
    return F.when(
        start_col.isNotNull() & end_col.isNotNull(),
        F.to_timestamp(
            F.from_unixtime(
                (F.unix_timestamp(end_col) + F.unix_timestamp(start_col)) / 2
            )
        )
    )

# ===================== Load Sources =====================
gpo_df    = spark.table(GPO_TBL)
status_df = spark.table(STATUS_TBL)

# ===================== Latest GPO =====================
gpo_cols = [
    "prod_order_no","prod_order_line_no","prod_item_line",
    "prod_order_status","item_location",
    "prod_line_start_date","prod_line_end_date",
    "prod_line_quantity","prod_line_remaining_quantity",
    "_modified_any"
]
gpo_keep = [c for c in gpo_cols if c in gpo_df.columns]
order_expr = F.coalesce(F.to_timestamp("_modified_any"), F.current_timestamp()).desc()
w_gpo = Window.partitionBy("prod_order_no","prod_order_line_no").orderBy(order_expr)
latest_gpo = (
    gpo_df
    .select(*gpo_keep)
    .withColumn("rn", F.row_number().over(w_gpo))
    .filter(F.col("rn") == 1)
    .drop("rn")

    # === BUSINESS FILTERS ===

    .filter(F.col("prod_order_status") == "Released")
    .filter(F.col("item_location") == "CST_CUT")
    .filter(F.col("prod_item_line").startswith("C"))
)
# ===================== pos_open =====================
ts_open = F.coalesce(F.to_timestamp("modified_on"), F.to_timestamp("created_on")).alias("ts_open")
w_open = Window.partitionBy(
    "prod_order_no","prod_order_line_no"
).orderBy(
    F.col("ts_open").desc(),
    F.to_timestamp("created_on").desc()
)
pos_open = (
    status_df
    .filter((F.col("open") == "Yes") & (F.col("current_location_code") != "WAX"))
    .withColumn("ts_open", ts_open)
    .withColumn("rn", F.row_number().over(w_open))
    .filter(F.col("rn") == 1)
    .drop("rn","ts_open")
    .select(
        "prod_order_no","prod_order_line_no",
        "created_on","modified_on",
        "type_name","open","current_location_code"
    )
)

# ===================== pos_any =====================

ts_any = F.coalesce(F.to_timestamp("modified_on"), F.to_timestamp("created_on")).alias("ts_any")
w_any = Window.partitionBy(
    "prod_order_no","prod_order_line_no"
).orderBy(
    F.col("ts_any").desc(),
    F.to_timestamp("created_on").desc()
)
pos_any = (
    status_df
    .withColumn("ts_any", ts_any)
    .withColumn("rn", F.row_number().over(w_any))
    .filter(F.col("rn") == 1)
    .drop("rn","ts_any")
    .select(
        "prod_order_no","prod_order_line_no",
        F.col("type_name").alias("type_name_any")
    )
)
# ===================== Join & Compute =====================
base = (
    latest_gpo.alias("gpo")
    .join(pos_open.alias("pos_open"),
          ["prod_order_no","prod_order_line_no"], "left")
    .join(pos_any.alias("pos_any"),
          ["prod_order_no","prod_order_line_no"], "left")
)

line_wax_due_date = midpoint_ts(
    F.col("gpo.prod_line_start_date"),
    F.col("gpo.prod_line_end_date")
)
now_ts = F.current_timestamp()
job_status = (
    F.when(F.col("pos_any.type_name_any") == "From employee", "finished")
     .when(F.col("pos_any.type_name_any") == "To employee", "in process")
     .when(
         F.col("gpo.prod_line_start_date").isNotNull() &
         F.col("gpo.prod_line_end_date").isNotNull() &
         (now_ts > line_wax_due_date),
         "overdue"
     )
     .otherwise("remaining")
)
result_df = (
    base.select(
        F.col("gpo.prod_order_no"),
        F.col("gpo.prod_order_line_no"),
        F.col("gpo.prod_item_line"),
        F.col("gpo.prod_line_start_date"),
        F.col("gpo.prod_line_end_date"),
        line_wax_due_date.alias("line_wax_due_date"),
        F.col("gpo.prod_line_quantity").alias("quantity"),
        F.col("gpo.prod_line_remaining_quantity").alias("remaining_quantity"),
        F.col("pos_open.created_on"),
        F.col("pos_open.modified_on"),
        F.col("pos_open.type_name"),
        F.col("pos_open.open"),
        F.col("pos_open.current_location_code"),
        job_status.alias("job_status")
    )
)
# ===================== Watermark Column =====================
src_max_ts = max(
    [
        gpo_df.select(F.max(F.to_timestamp(change_ts(gpo_df)))).first()[0],
        status_df.select(F.max(F.to_timestamp(change_ts(status_df)))).first()[0]
    ]
)
result_df = result_df.withColumn(
    "source_max_ts",
    F.lit(src_max_ts).cast("timestamp")
)
# ===================== FULL REPLACE WRITE =====================
spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}")
spark.sql(f"USE {GOLD_DB}")
(
    result_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TGT_FULL)
)
print(f"FULL REPLACE completed for {TGT_FULL}")
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Plating Output

# CELL ********************

# Databricks / PySpark
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import LongType, StringType
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

# ============================= CONFIG =============================
TARGET_TABLE = "Gold_Production_Lakehouse.prod.gold_plating_output"

# If the target already contains duplicated keys, set this True ONCE to clean it.
CLEAN_TARGET_DUPES_ONCE = False  # flip to True if MERGE keeps failing and tgt has dupes

# Choose the "latest" rule for tie-breaking (used for source and target de-dupe)
LATEST_ORDER = [
    F.col("created_on").desc_nulls_last(),
    F.col("modified_on").desc_nulls_last(),
]

# MERGE keys (must be unique in both source and target)
MERGE_KEYS = [
    "prod_order_no",
    "prod_order_line_no",
    "item_no",
    "type_name",
    "CorrectCurrentLocation",
]

# ============================= HELPERS =============================
def table_exists(name: str) -> bool:
    try:
        spark.table(name)
        return True
    except AnalysisException:
        return False

def keep_one(df, keys, order_cols):
    """Keep exactly one row per key group using the provided order."""
    w = Window.partitionBy(*[F.col(k) for k in keys]).orderBy(*order_cols)
    return df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

def null_if_blank(c):
    return F.when(F.length(F.trim(c)) == 0, F.lit(None)).otherwise(F.trim(c))

def normalize_merge_key_cols(df):
    """
    Trim string-like MERGE key columns and ensure prod_order_line_no is STRING.
    Adjust as needed if your domain requires UPPER() normalization.
    """
    out = df
    # ensure types / trim
    if "prod_order_line_no" in out.columns:
        out = out.withColumn("prod_order_line_no", F.trim(F.col("prod_order_line_no").cast(StringType())))
    for c in ["prod_order_no", "item_no", "type_name", "CorrectCurrentLocation"]:
        if c in out.columns:
            out = out.withColumn(c, F.trim(F.col(c)))
    return out

def count_dupes(df, keys):
    return df.groupBy(*keys).count().filter(F.col("count") > 1)

# ============================= 1) SOURCES =============================
hdr_src = spark.table("Silver_Production_Lakehouse.prod.silver_prod_order_header").alias("h")
so_src  = spark.table("Gold_Production_Lakehouse.prod.gold_sales_order").alias("so")
st_src  = spark.table("Silver_Production_Lakehouse.prod.silver_prod_order_status").alias("s")
it_src  = spark.table("Silver_Inventory_Lakehouse.inv.silver_item").alias("it")

# ---- De-dupe potential 1->N sources BEFORE joins
# Use better order columns if you have them (e.g., last_updated desc)
hdr_src_1 = keep_one(hdr_src, ["prod_order_no"], [F.col("prod_order_no")])
so_src_1  = keep_one(so_src,  ["SalesorderNo"],  [F.col("SalesorderNo")])
it_src_1  = keep_one(it_src,  ["item_no"],       [F.col("item_no")])

# ============================= 2) Hdr: header + sales =============================
hdr = (
    hdr_src_1.alias("h")
    .join(so_src_1.alias("so"), F.col("so.SalesorderNo") == F.col("h.sales_order_no"), "inner")
    .select(
        F.col("h.prod_order_no"),
        F.col("h.prod_order_status"),
        F.col("h.sales_order_no"),
        F.col("so.CusNo"),
        F.col("so.CusAbbr"),
    )
)
# If you need Released only:
# hdr = hdr.filter(F.col("h.prod_order_status") == "Released")

# ============================= 3) StatusLatest =============================
cur_loc  = F.col("s.current_location_code")
mach_ctr = F.col("s.machine_center_no")

is_cell_or_null = (
    null_if_blank(cur_loc).isNull() |
    (F.upper(F.substring(F.trim(cur_loc), 1, 4)) == F.lit("CELL"))
)

CorrectCurrentLocation = F.when(
    is_cell_or_null,
    F.coalesce(null_if_blank(mach_ctr), F.trim(cur_loc))
).otherwise(F.trim(cur_loc))

status_enriched = (
    st_src
    .withColumn("prod_order_line_no_str", F.col("s.prod_order_line_no").cast(StringType()))
    .withColumn("out_qty", (F.lit(-1) * F.col("s.quantity")).cast(LongType()))
    .withColumn("pol", F.concat(F.col("s.prod_order_no"), F.col("s.prod_order_line_no").cast(StringType())))
    .withColumn("created_on_time", F.col("s.created_on"))
    .withColumn("CorrectCurrentLocation", CorrectCurrentLocation)
)

w_latest = (
    Window.partitionBy(
        F.col("s.prod_order_no"),
        F.col("s.prod_order_line_no"),
        F.col("s.item_no"),
        F.col("CorrectCurrentLocation"),
        F.col("s.type_name"),
    ).orderBy(
        F.col("s.created_on").desc_nulls_last(),
        F.col("s.modified_on").desc_nulls_last()
    )
)

status_latest = (
    status_enriched
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .select(
        F.col("s.created_on"),
        F.col("s.modified_on"),
        F.col("s.prod_order_no"),
        F.col("prod_order_line_no_str").alias("prod_order_line_no"),
        F.col("s.type_name"),
        F.col("s.prod_order_status"),
        F.col("s.open").alias("open"),
        F.col("s.sales_order_no"),
        F.col("s.current_location_code"),
        F.col("s.past_location_code"),
        F.col("s.employee_no"),
        F.col("s.user_id"),
        F.col("s.quantity"),
        F.col("s.remaining_quantity"),
        F.col("s.item_no"),
        F.col("s.machine_center_no"),
        F.col("out_qty"),
        F.col("pol"),
        F.col("created_on_time"),
        F.col("CorrectCurrentLocation"),
    )
)

# ============================= 4) FINAL SELECT =============================
final_df0 = (
    status_latest.alias("s")
    .join(hdr.alias("h"), F.col("s.prod_order_no") == F.col("h.prod_order_no"), "inner")
    .join(it_src_1.alias("it"), F.col("it.item_no") == F.col("s.item_no"), "left")
    .select(
        F.col("s.created_on"),
        F.col("s.modified_on"),
        F.col("h.prod_order_no"),
        F.col("s.prod_order_line_no"),
        F.col("s.type_name"),                 # expose type_name
        F.col("s.prod_order_status"),
        F.col("s.open"),
        F.col("h.sales_order_no"),
        F.col("s.current_location_code"),
        F.col("s.past_location_code"),
        F.col("s.employee_no"),
        F.col("s.user_id"),
        F.col("s.quantity"),
        F.col("s.remaining_quantity"),
        F.col("s.item_no"),
        F.col("s.machine_center_no"),
        F.col("s.out_qty"),
        F.col("s.pol"),
        F.col("s.created_on_time"),
        F.col("s.CorrectCurrentLocation"),
        F.col("h.CusNo"),
        F.col("h.CusAbbr"),
        F.substring(F.col("s.item_no"), 1, 1).alias("item_type"),
        F.col("it.item_category"),
        F.col("it.prod_type"),
    )
)

# Enforce one row per MERGE key in the SOURCE (defensive)
final_df = keep_one(
    normalize_merge_key_cols(final_df0),
    MERGE_KEYS,
    LATEST_ORDER
)

# ============================= 5) DIAGNOSTICS (optional, safe) =============================
# Check whether source or target has dupes on the MERGE keys.
src_dupes_cnt = count_dupes(final_df, MERGE_KEYS).count()
print(f"[Diag] SOURCE duplicate key groups: {src_dupes_cnt}")

tgt_dupes_cnt = 0
if table_exists(TARGET_TABLE):
    tgt_dupes_cnt = count_dupes(normalize_merge_key_cols(spark.table(TARGET_TABLE)), MERGE_KEYS).count()
    print(f"[Diag] TARGET duplicate key groups: {tgt_dupes_cnt}")

# ============================= 6) (OPTIONAL) CLEAN TARGET =============================
# If the target already contains duplicates, MERGE may still fail even with a clean source.
if table_exists(TARGET_TABLE) and CLEAN_TARGET_DUPES_ONCE and tgt_dupes_cnt > 0:
    print("[Action] Cleaning target duplicates once...")
    tgt_df = normalize_merge_key_cols(spark.table(TARGET_TABLE))
    tgt_dedup = keep_one(tgt_df, MERGE_KEYS, [F.col("modified_on").desc_nulls_last(), F.col("created_on").desc_nulls_last()])
    assert count_dupes(tgt_dedup, MERGE_KEYS).count() == 0, "Target still has duplicates after dedupe!"
    (
        tgt_dedup.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )
    print("[Action] Target cleaned.")

# ============================= 7) UPSERT (CREATE or MERGE) =============================
if not table_exists(TARGET_TABLE):
    (
        final_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TARGET_TABLE)
    )
    print(f"[Create] Wrote initial snapshot to {TARGET_TABLE}")
else:
    # Make sure the MERGE keys exist and are comparable types between source & target.
    tgt = DeltaTable.forName(spark, TARGET_TABLE)
    merge_cond = """
        t.prod_order_no = s.prod_order_no AND
        t.prod_order_line_no = s.prod_order_line_no AND
        t.item_no = s.item_no AND
        t.type_name = s.type_name AND
        t.CorrectCurrentLocation = s.CorrectCurrentLocation
    """
    (
        tgt.alias("t")
        .merge(final_df.alias("s"), merge_cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"[Merge] Upserted into {TARGET_TABLE}")

# ============================= 8) (OPTIONAL) OPTIMIZE =============================
# spark.sql(f"OPTIMIZE {TARGET_TABLE} ZORDER BY (prod_order_no, item_no, CorrectCurrentLocation)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

target_table = "Gold_Production_Lakehouse.prod.gold_plating_output"

# Read current contents
df = spark.table(target_table)

# Define the business key for uniqueness
key_cols = [
    "prod_order_no",
    "prod_order_line_no",
    "item_no",
    "type_name",
    "CorrectCurrentLocation",
]

# Rank rows within each key by most recent timestamps
w = Window.partitionBy(*[F.col(c) for c in key_cols]) \
          .orderBy(
              F.col("created_on").desc_nulls_last(),
              F.col("modified_on").desc_nulls_last()
          )

deduped = (
    df.withColumn("rn", F.row_number().over(w))
      .filter(F.col("rn") == 1)
      .drop("rn")
)

# Atomically overwrite the table with the deduped data
(deduped.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table))

# Optional: speed up common predicates
# spark.sql(f"OPTIMIZE {target_table} ZORDER BY (prod_order_no, item_no, CorrectCurrentLocation)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Output Time Compare

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, date

spark = SparkSession.builder.getOrCreate()

# =========================
# Sources & Target
# =========================
S_SRC = "Silver_Production_Lakehouse.prod.silver_prod_order_status"
L_SRC = "Gold_Production_Lakehouse.prod.gold_production_asgn_cell"
R_SRC = "Silver_Production_Lakehouse.prod.silver_prod_routing_line"
TARGET = "Gold_Production_Lakehouse.prod.gold_output_time_compare"

# =========================
# Widgets / Parameters
# =========================
def get_widget(name: str, default: str) -> str:
    try:
        import dbutils  # type: ignore
        return dbutils.widgets.get(name)  # type: ignore
    except Exception:
        return default

try:
    dbutils.widgets.text("full_reload", "false")  # type: ignore
    dbutils.widgets.text("since", "")             # yyyy-MM-dd  # type: ignore
    dbutils.widgets.text("until", "")             # yyyy-MM-dd  # type: ignore
except Exception:
    pass

full_reload = get_widget("full_reload", "false").strip().lower() == "true"
since_param = get_widget("since", "").strip()
until_param = get_widget("until", "").strip()

def parse_date_or_none(s: str):
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()

def table_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False

def run_sql_safely(sql_text: str):
    try:
        spark.sql(sql_text)
    except Exception as e:
        print(f"Skipped optional SQL '{sql_text}': {e}")

# =========================
# Derive incremental window
# =========================
since_date = parse_date_or_none(since_param)
until_date = parse_date_or_none(until_param) or date.today()

if not full_reload and since_date is None and table_exists(TARGET):
    tgt = spark.table(TARGET)
    pick = "change_ts" if "change_ts" in tgt.columns else ("created_on" if "created_on" in tgt.columns else None)
    if pick:
        mx = tgt.agg(F.max(pick).alias("mx")).collect()[0]["mx"]
        if mx is not None:
            since_date = mx.date() if hasattr(mx, "date") else mx

if full_reload or since_date is None:
    since_date = date(1900, 1, 1)

print(f"Incremental window: {since_date} -> {until_date}")

# =========================
# Load sources
# =========================
s_raw = spark.table(S_SRC).alias("s")
l = spark.table(L_SRC).alias("l")
r = spark.table(R_SRC).alias("r")

# Normalize timestamps and build change_ts
s = (
    s_raw
    .withColumn("created_on_ts",  F.to_timestamp(F.col("s.created_on")))
    .withColumn("modified_on_ts", F.to_timestamp(F.col("s.modified_on")))
    .withColumn("change_ts", F.greatest(F.col("created_on_ts"), F.col("modified_on_ts")))
)

# Filter by window on change_ts (date)
s_win = s.filter(
    (F.to_date("change_ts") >= F.lit(since_date)) &
    (F.to_date("change_ts") <= F.lit(until_date))
)

# Filter type_name IN (...)
s_win = s_win.filter(F.col("s.type_name").isin("In location in", "To employee"))

# =========================
# Joins
# =========================
joined = (
    s_win.alias("s")
      .join(
          l,
          (F.col("s.prod_order_no") == F.col("l.prod_order_no")) &
          (F.col("s.prod_order_line_no") == F.col("l.prod_order_line_no")),
          "left"
      )
      .join(
          r,
          (F.col("s.prod_order_no") == F.col("r.prod_order_no")) &
          (F.col("s.prod_order_line_no") == F.col("r.prod_order_line_no")) &
          (F.col("s.operation_no") == F.col("r.operation_no")),
          "left"
      )
)

# =========================
# Transformations (CROSS APPLY + CASE)
# =========================
# +7 hours (Bangkok)
created_on_bkk  = (F.col("s.created_on_ts")  + F.expr("INTERVAL 7 HOURS")).alias("created_on")
modified_on_bkk = (F.col("s.modified_on_ts") + F.expr("INTERVAL 7 HOURS")).alias("modified_on")

# Diff hours / minutes
sec_diff = (F.col("s.modified_on_ts").cast("long") - F.col("s.created_on_ts").cast("long"))
diff_hours   = F.floor(sec_diff / 3600.0).cast("long").alias("diff_hours")
diff_minutes = F.floor(sec_diff / 60.0).cast("long").alias("diff_minutes")

# standard_time & standard_time_total
standard_time        = F.col("r.run_time").alias("standard_time")
standard_time_total  = (F.col("r.run_time") * F.abs(F.col("s.quantity"))).alias("standard_time_total")

# CorrectCurrentLocation CASE
clc_trim = F.trim(F.col("s.current_location_code"))
mcn_trim = F.trim(F.col("s.machine_center_no"))
clc_null = F.when(clc_trim.isNull() | (F.length(clc_trim) == 0), F.lit(None)).otherwise(clc_trim)
starts_with_CELL = (F.upper(clc_trim).substr(1, 4) == F.lit("CELL"))
CorrectCurrentLocation = (
    F.when(clc_null.isNull(), F.coalesce(F.when(F.length(mcn_trim) == 0, None).otherwise(mcn_trim), clc_trim))
     .when(starts_with_CELL, F.coalesce(F.when(F.length(mcn_trim) == 0, None).otherwise(mcn_trim), clc_trim))
     .otherwise(clc_trim)
     .alias("CorrectCurrentLocation")
)

# Final projection (matching your SQL aliases)
final_df = (
    joined.select(
        created_on_bkk,
        modified_on_bkk,
        standard_time,
        standard_time_total,
        diff_hours,
        diff_minutes,
        F.col("s.antenna_id").alias("antenna_id"),
        F.col("s.rfid_transaction_name").alias("rfid_transaction_name"),
        F.col("s.user_id").alias("user_id"),
        F.col("l.prod_line").alias("prod_line"),
        F.col("l.cell_line").alias("cell_line"),
        F.col("s.prod_order_status").alias("prod_order_status"),
        F.col("s.prod_order_no").alias("prod_order_no"),
        F.col("s.prod_order_line_no").alias("prod_order_line_no"),
        F.col("s.type_name").alias("type_name"),
        F.col("s.open").alias("open"),
        F.col("s.operation_no").alias("operation_no"),
        F.col("s.item_no").alias("item_no"),
        F.col("s.quantity").alias("quantity"),
        F.col("s.remaining_quantity").alias("remaining_quantity"),
        F.col("s.sales_order_no").alias("sales_order_no"),
        F.col("s.current_location_code").alias("current_location_code"),
        F.col("s.past_location_code").alias("past_location_code"),
        F.col("s.machine_center_no").alias("machine_center_no"),
        F.col("s.employee_no").alias("employee_no"),
        CorrectCurrentLocation,
        F.col("s.change_ts").alias("change_ts"),  # keep for watermark/debug
    )
)

# =========================
# Merge key (stable, collision-proof)
# =========================
# Use full-precision timestamps (no truncation) to avoid key collisions within the same second.
final_df = final_df.withColumn(
    "row_id",
    F.md5(F.concat_ws(
        "||",
        F.coalesce(F.col("prod_order_no"), F.lit("")),
        F.coalesce(F.col("prod_order_line_no").cast("string"), F.lit("")),
        F.coalesce(F.col("operation_no").cast("string"), F.lit("")),
        F.coalesce(F.col("item_no"), F.lit("")),
        F.coalesce(F.col("employee_no"), F.lit("")),
        F.coalesce(F.col("antenna_id").cast("string"), F.lit("")),
        F.coalesce(F.col("type_name"), F.lit("")),
        F.coalesce(F.col("created_on").cast("string"), F.lit("")),
        F.coalesce(F.col("modified_on").cast("string"), F.lit(""))
    ))
)

# =========================
# De-duplicate source before MERGE (1 row per row_id)
# =========================
# Deterministic winner: latest modified_on, then latest created_on,
# then lexicographically stable tiebreakers.
w = Window.partitionBy("row_id").orderBy(
    F.col("modified_on").desc_nulls_last(),
    F.col("created_on").desc_nulls_last(),
    F.col("rfid_transaction_name").asc_nulls_last(),
    F.col("antenna_id").asc_nulls_last()
)
final_df = (
    final_df
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# =========================
# Create or MERGE into target
# =========================
if not table_exists(TARGET):
    print(f"Target {TARGET} not found. Creating...")
    (final_df
        .repartition(F.to_date("created_on"))
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET))
    # Optional: Databricks perf
    run_sql_safely(f"OPTIMIZE {TARGET}")
else:
    print(f"Merging incremental data into {TARGET}...")
    tgt = DeltaTable.forName(spark, TARGET)
    (tgt.alias("tgt")
        .merge(final_df.alias("src"), "src.row_id <=> tgt.row_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    run_sql_safely(
        f"OPTIMIZE {TARGET} WHERE change_ts >= DATE '{since_date}' AND change_ts <= DATE '{until_date}'"
    )

print("✅ Incremental load complete for Gold_Production_Lakehouse.prod.gold_output_time_compare")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold RFID Transaction

# CELL ********************

# ================================================================
# Job: Gold_Production_Lakehouse.prod.gold_rfid_transaction (incremental)
# ================================================================

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime, date

# ---------- Sources ----------
S_SRC = "Silver_Production_Lakehouse.prod.silver_prod_order_status"  # S
C_SRC = "Silver_Production_Lakehouse.prod.silver_cell_list"          # C
M_SRC = "Silver_Commons_Lakehouse.cmn.silver_machine_center"         # M

# ---------- Target ----------
TARGET = "Gold_Production_Lakehouse.prod.gold_rfid_transaction"

# ---------- Widgets / params ----------
def get_widget(name: str, default: str) -> str:
    try:
        import dbutils  # type: ignore
        return dbutils.widgets.get(name)  # type: ignore
    except Exception:
        return default

try:
    dbutils.widgets.text("full_reload", "false")  # type: ignore
    dbutils.widgets.text("since", "")             # yyyy-MM-dd  # type: ignore
    dbutils.widgets.text("until", "")             # yyyy-MM-dd  # type: ignore
except Exception:
    pass

full_reload = get_widget("full_reload", "false").strip().lower() == "true"
since_param = get_widget("since", "").strip()
until_param = get_widget("until", "").strip()

def parse_date_or_none(s: str):
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()

def table_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False

def pick_watermark_from_target(table_name: str):
    try:
        df = spark.table(table_name)
    except Exception:
        return None
    if "change_ts" in df.columns:
        return df.agg(F.max(F.to_date("change_ts"))).collect()[0][0]
    elif "created_date" in df.columns:
        return df.agg(F.max("created_date")).collect()[0][0]
    return None

since_date = parse_date_or_none(since_param)
until_date = parse_date_or_none(until_param) or date.today()

if not full_reload and since_date is None and table_exists(TARGET):
    wm = pick_watermark_from_target(TARGET)
    if wm is not None:
        since_date = wm

if full_reload or since_date is None:
    since_date = date(1900, 1, 1)

print(f"Incremental window (date(change_ts)): {since_date} -> {until_date}")

# ---------- Load sources ----------
S = spark.table(S_SRC).alias("S")
C = spark.table(C_SRC).alias("C")
M = spark.table(M_SRC).alias("M")

# ---------- Base casting + watermark ----------
S_cast = (
    S.withColumn("created_on_ts",  F.to_timestamp("created_on"))
     .withColumn("modified_on_ts", F.to_timestamp("modified_on"))
     .withColumn("created_date",   F.to_date("created_on_ts"))
     .withColumn("change_ts",      F.greatest("created_on_ts", "modified_on_ts"))
)

S_win = S_cast.filter(
    (F.to_date("change_ts") >= F.lit(since_date)) &
    (F.to_date("change_ts") <= F.lit(until_date))
)

# ---------- Joins ----------
C_slim = C.select(
    F.col("email_address").alias("c_email"),
    "prod_line","cell_line","sub_department"
)
M_slim = M.select("machine_center_no","machine_employee_mapping")

joined = (
    S_win.alias("S")
        .join(C_slim.alias("C"), F.col("S.user_id") == F.col("C.c_email"), "left")
        .join(M_slim.alias("M"), F.col("S.machine_center_no") == F.col("M.machine_center_no"), "left")
)

# ---------- Derived fields (PySpark-safe) ----------
rfid_trim = F.trim(F.col("S.rfid_transaction_name"))
has_rfid  = (rfid_trim.isNotNull()) & (rfid_trim != "")

is_scanned_int = F.when(has_rfid, F.lit(1)).otherwise(F.lit(0))
scan_status    = F.when(has_rfid, F.lit("Scanned")).otherwise(F.lit("Not scanned"))

move_direction = (
    F.when(F.col("S.type_name").like("%Out location%"),  F.lit("OUT_LOC"))
     .when(F.col("S.type_name").like("%In location%"),   F.lit("IN_LOC"))
     .when(F.col("S.type_name").like("%To employee%"),   F.lit("TO_EMP"))
     .when(F.col("S.type_name").like("%From employee%"), F.lit("FROM_EMP"))
     .otherwise(F.lit("OTHER"))
)

month_start  = F.to_date(F.date_trunc("month", F.col("S.created_on_ts")))
weekday_name = F.date_format(F.col("S.created_on_ts"), "EEEE")
created_hour = F.hour(F.col("S.created_on_ts"))
operation_no_int = F.col("S.operation_no").cast("int")

# ---------- Final select ----------
final_df = (
    joined.select(
        # original
        F.col("S.created_date").alias("created_date"),
        F.col("S.created_on_ts").alias("created_on"),
        F.col("S.modified_on_ts").alias("modified_on"),
        F.col("S.prod_order_no").alias("prod_order_no"),
        F.col("S.prod_order_line_no").alias("prod_order_line_no"),
        F.col("S.prod_order_status").alias("prod_order_status"),
        F.col("S.type_name").alias("type_name"),
        F.col("S.operation_no").alias("operation_no"),
        F.col("S.user_id").alias("user_id"),
        F.col("S.rfid_transaction_name").alias("rfid_transaction_name"),
        F.col("S.machine_center_no").alias("machine_center_no"),
        F.col("S.employee_no").alias("employee_no"),
        F.col("S.antenna_id").alias("antenna_id"),
        F.col("S.item_no").alias("item_no"),
        F.col("S.quantity").alias("quantity"),
        F.col("S.remaining_quantity").alias("remaining_quantity"),
        F.col("S.sales_order_no").alias("sales_order_no"),
        F.col("C.prod_line").alias("prod_line"),
        F.col("C.cell_line").alias("cell_line"),
        F.col("C.sub_department").alias("sub_department"),
        F.col("M.machine_employee_mapping").alias("m_group"),

        # derived
        is_scanned_int.alias("is_rfid_scanned"),   # 0/1
        scan_status.alias("scan_status"),
        move_direction.alias("move_direction"),
        month_start.alias("month_start"),
        weekday_name.alias("weekday_name"),
        created_hour.alias("created_hour"),
        operation_no_int.alias("operation_no_int"),

        # watermark
        F.col("S.change_ts").alias("change_ts")
    )
)

# ---------- MERGE key ----------
row_id_cols = [
    "prod_order_no","prod_order_line_no","operation_no",
    "user_id","machine_center_no","employee_no","antenna_id",
    "item_no","type_name","created_on"
]
final_df = final_df.withColumn(
    "row_id",
    F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in row_id_cols]), 256)
)

# ---------- Write / Merge ----------
def merge_or_create(target, df):
    if not table_exists(target):
        print(f"Creating {target} ...")
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema","true")
              .saveAsTable(target)
        )
        return
    print(f"Merging into {target} ...")
    tgt = DeltaTable.forName(spark, target)
    (
        tgt.alias("tgt")
           .merge(df.alias("src"), "tgt.row_id <=> src.row_id")
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute()
    )

merge_or_create(TARGET, final_df)
print(f"✅ Done: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Remaining Production

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.utils import AnalysisException

# ===================== Config =====================
GOLD_DB   = "Gold_Production_Lakehouse"
SILVER_DB = "Silver_Production_Lakehouse"
SCHEMA    = "prod"

GPO_TBL     = f"{GOLD_DB}.{SCHEMA}.gold_production_order"
STATUS_TBL  = f"{SILVER_DB}.{SCHEMA}.silver_prod_order_status"

TGT_FULL    = f"{GOLD_DB}.{SCHEMA}.gold_remaining_production"

# ===================== Helpers =====================
def table_exists(name: str) -> bool:
    try:
        spark.table(name).limit(1).count()
        return True
    except AnalysisException:
        return False

def get_watermark():
    """Return max(source_max_ts) from target (as Python datetime) or None."""
    if not table_exists(TGT_FULL):
        return None
    df = spark.table(TGT_FULL)
    if "source_max_ts" not in df.columns:
        return None
    return df.select(F.max(F.to_timestamp("source_max_ts")).alias("mx")).collect()[0]["mx"]

def first_existing_name(df, names):
    for n in names:
        if n in df.columns:
            return n
    return None

def change_ts(df):
    """
    Choose a reasonable change-timestamp column.
    We’ll cast when used, so strings/dates are fine.
    """
    candidates = [
        "_modified_any", "modified_on", "updated_on", "last_modified",
        "created_on", "created_at", "_ingest_ts",
        "prod_line_end_date", "prod_line_start_date", "prod_line_due_date"
    ]
    name = first_existing_name(df, candidates)
    return F.col(name) if name else F.current_timestamp()

# Datetime midpoint (full timestamp) between two cols, else NULL
def midpoint_ts(col_start, col_end):
    # seconds average: (unix(end) + unix(start))/2 → timestamp
    return F.when(
        col_start.isNotNull() & col_end.isNotNull(),
        F.to_timestamp(F.from_unixtime((F.unix_timestamp(col_end) + F.unix_timestamp(col_start)) / 2.0))
    )

# ===================== Load sources =====================
gpo_df    = spark.table(GPO_TBL)
status_df = spark.table(STATUS_TBL)

# ===================== Incremental keyset =====================
wm = get_watermark()

def affected_pairs_from(df, order_col="prod_order_no", line_col="prod_order_line_no"):
    ts = change_ts(df).cast("timestamp").alias("ts")
    base = df.select(
        F.col(order_col).alias("prod_order_no"),
        F.col(line_col).alias("prod_order_line_no"),
        ts
    )
    if wm is not None:
        base = base.filter(F.col("ts") > F.to_timestamp(F.lit(wm)))
    return base.select("prod_order_no", "prod_order_line_no").distinct()

aff_gpo  = affected_pairs_from(gpo_df, "prod_order_no", "prod_order_line_no")
aff_stat = affected_pairs_from(status_df, "prod_order_no", "prod_order_line_no")

affected_keys = aff_gpo.unionByName(aff_stat).distinct()
if wm is None:
    affected_keys = gpo_df.select("prod_order_no","prod_order_line_no").distinct()

if affected_keys.limit(1).count() == 0:
    print("No changes since last run — target is up to date.")
else:
    # ===================== Latest GPO per (order,line) =====================
    # Only keep columns we need
    gpo_cols = [
        "prod_order_no","prod_order_line_no","prod_item_line",
        "prod_order_status","item_location",
        "prod_line_start_date","prod_line_end_date",
        "prod_line_quantity","prod_line_remaining_quantity",
        "_modified_any"
    ]
    gpo_keep = [c for c in gpo_cols if c in gpo_df.columns]

    gpo_filtered = (
        gpo_df.select(*gpo_keep)
              .join(affected_keys, on=["prod_order_no","prod_order_line_no"], how="inner")
    )

    # Rank by _modified_any desc (fallback to current_timestamp if missing/NULL)
    order_expr = F.coalesce(F.to_timestamp(F.col("_modified_any")), F.current_timestamp()).desc()
    w_gpo = Window.partitionBy("prod_order_no","prod_order_line_no").orderBy(order_expr)

    latest_gpo = (
        gpo_filtered
        .withColumn("rn", F.row_number().over(w_gpo))
        .filter(F.col("rn")==1)
        .drop("rn")
    )

    # Apply final filters like your SQL
    latest_gpo = (
        latest_gpo
        .filter(F.col("prod_order_status") == F.lit("Released"))
        .filter(F.col("item_location") == F.lit("FIN-GOODS"))
        # .filter(F.col("prod_item_line").startswith("C"))
    )

    # ===================== pos_open (latest open != WAX) =====================
    ts_open = F.coalesce(F.to_timestamp("modified_on"), F.to_timestamp("created_on")).alias("ts_open")
    w_open = Window.partitionBy("prod_order_no","prod_order_line_no") \
                   .orderBy(F.col("ts_open").desc(), F.to_timestamp("created_on").desc())

    pos_open_pre = (
        status_df
        .where((F.col("open") == F.lit("Yes")))
        .join(affected_keys, on=["prod_order_no","prod_order_line_no"], how="inner")
        .withColumn("ts_open", ts_open)
        .withColumn("rn", F.row_number().over(w_open))
        .filter(F.col("rn")==1)
        .drop("rn","ts_open")
        .select(
            "prod_order_no","prod_order_line_no",
            "created_on","modified_on","type_name","open","current_location_code"
        )
    )

    # ===================== pos_any (latest regardless) =====================
    ts_any = F.coalesce(F.to_timestamp("modified_on"), F.to_timestamp("created_on")).alias("ts_any")
    w_any = Window.partitionBy("prod_order_no","prod_order_line_no") \
                  .orderBy(F.col("ts_any").desc(), F.to_timestamp("created_on").desc())

    pos_any_pre = (
        status_df
        .join(affected_keys, on=["prod_order_no","prod_order_line_no"], how="inner")
        .withColumn("ts_any", ts_any)
        .withColumn("rn", F.row_number().over(w_any))
        .filter(F.col("rn")==1)
        .drop("rn","ts_any")
        .select("prod_order_no","prod_order_line_no","type_name")
        .withColumnRenamed("type_name","type_name_any")
    )

    # ===================== Join & compute =====================
    base = (
        latest_gpo.alias("gpo")
        .join(pos_open_pre.alias("pos_open"), on=["prod_order_no","prod_order_line_no"], how="left")
        .join(pos_any_pre.alias("pos_any"),   on=["prod_order_no","prod_order_line_no"], how="left")
    )

    # midpoint between start and end
    line_wax_due_date = midpoint_ts(F.col("gpo.prod_line_start_date"), F.col("gpo.prod_line_end_date"))

    # job_status logic
    now_ts = F.current_timestamp()
    job_status = (
        F.when(F.col("pos_any.type_name_any") == F.lit("From employee"), F.lit("finished"))
         .when(F.col("pos_any.type_name_any") == F.lit("To employee"),   F.lit("in process"))
         .when(
             F.col("gpo.prod_line_start_date").isNotNull() &
             F.col("gpo.prod_line_end_date").isNotNull() &
             (now_ts > line_wax_due_date),
             F.lit("overdue")
         )
         .otherwise(F.lit("remaining"))
    )

    prod_line_end_week = F.weekofyear(
            F.to_date(F.col("gpo.prod_line_end_date"))
        ).alias("prod_line_end_week")


    result_df = (
        base.select(
            F.col("gpo.prod_order_no"),
            F.col("gpo.prod_order_line_no"),
            F.col("gpo.prod_item_line"),
            F.col("gpo.prod_line_start_date"),
            F.col("gpo.prod_line_end_date"),
            line_wax_due_date.alias("line_wax_due_date"),
            F.weekofyear(F.to_date(F.col("gpo.prod_line_end_date"))).alias("prod_line_end_week"),

            F.col("gpo.prod_line_quantity").alias("quantity"),
            F.col("gpo.prod_line_remaining_quantity").alias("remaining_quantity"),

            F.col("pos_open.created_on"),
            F.col("pos_open.modified_on"),
            F.col("pos_open.type_name"),
            F.col("pos_open.open"),
            F.col("pos_open.current_location_code"),

            job_status.alias("job_status")
        )
    )


    # ===================== Watermark for next run =====================
    def max_ts(df):
        return df.select(F.max(F.to_timestamp(change_ts(df))).alias("mx")).collect()[0]["mx"]

    mx_list = []
    for src in [gpo_df, status_df]:
        try:
            mx_list.append(max_ts(src))
        except Exception:
            pass

    src_max_ts = max([d for d in mx_list if d is not None]) if any(mx_list) else None
    if src_max_ts is None:
        src_max_ts = spark.range(1).select(F.current_timestamp().alias("ts")).collect()[0]["ts"]

    result_df = result_df.withColumn("source_max_ts", F.lit(src_max_ts).cast("timestamp"))

    # ===================== Create or MERGE (SCD-1) =====================
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}")
    spark.sql(f"USE {GOLD_DB}")

    if not table_exists(TGT_FULL):
        (result_df
          .write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(TGT_FULL))
        print(f"Created table {TGT_FULL}")
    else:
        result_df.createOrReplaceTempView("wax_status_src")
        spark.sql(f"""
            MERGE INTO {TGT_FULL} AS t
            USING wax_status_src AS s
            ON  t.prod_order_no      = s.prod_order_no
            AND t.prod_order_line_no = s.prod_order_line_no
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

# # Loading Capacity

# CELL ********************

from pyspark.sql import functions as F

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
SOURCE_TABLE = "Silver_Production_Lakehouse.prod.silver_loading_capacity"
TARGET_TABLE = "Gold_Production_Lakehouse.prod.gold_loading_capacity" 
# -------------------------------------------------------------------
# Helper: add 7 hours and cast to date
# -------------------------------------------------------------------
def add_7h_to_date(col_name: str):
    """
    Equivalent to:
    CAST(DATEADD(HOUR, 7, CAST(col AS datetime)) AS date)
    """
    return F.to_date(
        F.to_timestamp(F.col(col_name)) + F.expr("INTERVAL 7 HOURS")
    )

# -------------------------------------------------------------------
# Load source
# -------------------------------------------------------------------
src = spark.table(SOURCE_TABLE)

# -------------------------------------------------------------------
# Transform (matches your CREATE OR ALTER VIEW [dbo].[loading])
# -------------------------------------------------------------------
df = (
    src.select(
        add_7h_to_date("created_on").alias("created_on_th"),
        add_7h_to_date("modified_on").alias("modified_on_th"),

        F.col("customer_no"),
        F.col("customer_name"),
        F.col("sales_order_no"),
        F.col("item_no"),

        add_7h_to_date("sales_order_requested_date").alias("sales_order_requested_date_th"),

        F.col("cell_routing"),

        add_7h_to_date("start_date").alias("start_date_th"),
        add_7h_to_date("end_date").alias("end_date_th"),

        F.col("location_code"),
        F.col("prod_order_status"),
        F.col("prod_order_no"),
        F.col("prod_order_line_no"),

        add_7h_to_date("prod_order_due_date").alias("prod_order_due_date_th"),

        # DATEPART(ISO_WEEK, DATEADD(HOUR,7, prod_order_due_date))
        F.weekofyear(
            F.to_timestamp("prod_order_due_date") + F.expr("INTERVAL 7 HOURS")
        ).alias("prod_order_due_week_new"),

        F.col("prod_line_quantity"),
        F.col("prod_line_finished_quantity"),
        F.col("prod_line_remaining_quantity"),
        F.col("type_name"),
        F.col("operation_no"),
        F.col("routing_no"),
        F.col("operation_position"),
        F.col("run_time"),
        F.col("actual_capacity_used"),
        F.col("require_capacity"),
        F.col("remaining_required_capacity"),
        F.col("watermark_ts"),
    )
)

# -------------------------------------------------------------------
# Write to gold (overwrite-style refresh)
# -------------------------------------------------------------------
(
    df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(TARGET_TABLE)
)

print("gold_loading_capacity refreshed successfully.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# # Compare Actual vs Planned

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
SOURCE_FIRM   = "Silver_Production_Lakehouse.prod.silver_firm_plan_log"
SOURCE_HEADER = "Silver_Production_Lakehouse.prod.silver_prod_order_header"

TARGET_TABLE  = "Gold_Production_Lakehouse.prod.gold_compare_plan_vs_actual"

# -------------------------------------------------------------------
# Load sources (full refresh to keep cumulative correct)
# -------------------------------------------------------------------
firm_df   = spark.table(SOURCE_FIRM)
header_df = spark.table(SOURCE_HEADER)

# Ensure date types
firm_df   = firm_df.withColumn("finish_date", F.to_date("finish_date"))
header_df = header_df.withColumn("prod_order_due_date", F.to_date("prod_order_due_date"))

# -------------------------------------------------------------------
# Join exactly as in your SQL
# -------------------------------------------------------------------
joined_df = (
    firm_df.alias("F")
    .join(
        header_df.alias("H"),
        (F.col("F.prod_order_no") == F.col("H.prod_order_no")) &
        (F.col("F.item_no")       == F.col("H.FG_item_no")),
        "inner"
    )
)

# -------------------------------------------------------------------
# Base SELECT (matches your SQL SELECT DISTINCT)
# -------------------------------------------------------------------
base_df = (
    joined_df
    .select(
        F.col("F.assigned_cell").alias("assigned_cell"),
        F.col("F.customer_name").alias("customer_name"),
        F.col("F.finish_week").alias("finish_week"),
        F.col("F.requested_week").alias("requested_week"),
        F.col("F.item_no").alias("item_no"),
        F.col("F.material").alias("material"),
        F.col("F.prod_order_no").alias("prod_order_no"),
        F.col("F.prod_order_line_no").alias("prod_order_line_no"),
        F.col("F.sales_order_no").alias("sales_order_no"),
        F.col("F.quantity").alias("quantity"),
        F.col("F.remark").alias("remark"),
        F.col("F.finish_date").alias("finish_date"),
        F.col("H.prod_order_due_date").alias("cur_due")
    )
    .distinct()
)

# -------------------------------------------------------------------
# Cumulative quantities:
#   - from earliest requested_week to this requested_week (per cell)
#   - from earliest finish_week    to this finish_week    (per cell)
# -------------------------------------------------------------------
w_req_cum = (
    Window
    .partitionBy("assigned_cell")
    .orderBy("requested_week")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

w_fin_cum = (
    Window
    .partitionBy("assigned_cell")
    .orderBy("finish_week")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

result_df = (
    base_df
    .withColumn(
        "acc_qty_requested_week",
        F.sum("quantity").over(w_req_cum)
    )
    .withColumn(
        "acc_qty_finish_week",
        F.sum("quantity").over(w_fin_cum)
    )
)

# -------------------------------------------------------------------
# Write gold table (full overwrite to keep cumulative consistent)
# -------------------------------------------------------------------
(
    result_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_TABLE)
)

print("Full refresh of gold_compare_plan_vs_actual completed.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Production Routing Group

# CELL ********************

# ================================================================
# Job: Gold_Production_Lakehouse.prod.gold_production_routing_group (incremental)
#
# Mirrors SQL view [dbo].[prod_routing_group]:
#
# WITH src AS (
#   SELECT
#       t.created_on, t.modified_on, t.prod_order_no, t.prod_order_status,
#       t.prod_order_line_no, t.item_no, t.operation_no, t.operation_type,
#       t.routing_no, t.run_time,
#       operation_group = CASE WHEN operation_no IS NULL THEN NULL
#                              ELSE TRY_CONVERT(int, LEFT(operation_no, CHARINDEX('.', operation_no + '.') - 1))
#                         END
#   FROM silver_prod_routing_line t
# )
# , calc AS (
#   SELECT
#     src.*,
#     routing_no_machine_center = CASE WHEN operation_type='Machine Center' THEN routing_no ELSE NULL END,
#     routing_no_work_center    = MAX(CASE WHEN operation_type='Work Center' THEN routing_no END)
#                                   OVER (PARTITION BY prod_order_no, prod_order_line_no, operation_group)
#   FROM src
# )
# SELECT *,
#        CONCAT(prod_order_no, prod_order_line_no, operation_group) AS rol
# FROM calc
# WHERE operation_type='Machine Center'
#    OR (operation_group=9 AND operation_no='009')
#
# Incremental strategy:
#   - change_ts = greatest(created_on, modified_on) as timestamp
#   - watermark on date(change_ts)
#   - MERGE on row_id (hash of prod_order_no/line/operation_no/routing_no/operation_type)
# ================================================================

from pyspark.sql import functions as F
from pyspark.sql import Window as W
from delta.tables import DeltaTable
from datetime import datetime, date

# ---------- Source ----------
T_SRC = "Silver_Production_Lakehouse.prod.silver_prod_routing_line"

# ---------- Target ----------
TARGET = "Gold_Production_Lakehouse.prod.gold_production_routing_group"

# ---------- Widgets / params ----------
def get_widget(name: str, default: str) -> str:
    try:
        import dbutils  # type: ignore
        return dbutils.widgets.get(name)  # type: ignore
    except Exception:
        return default

try:
    dbutils.widgets.text("full_reload", "false")  # type: ignore
    dbutils.widgets.text("since", "")             # yyyy-MM-dd  # type: ignore
    dbutils.widgets.text("until", "")             # yyyy-MM-dd  # type: ignore
except Exception:
    pass

full_reload = get_widget("full_reload", "false").strip().lower() == "true"
since_param = get_widget("since", "").strip()
until_param = get_widget("until", "").strip()

def parse_date_or_none(s: str):
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()

def table_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False

def pick_watermark_from_target(table_name: str):
    """
    Use max(to_date(change_ts)) from target as watermark.
    """
    try:
        df = spark.table(table_name)
    except Exception:
        return None
    if "change_ts" in df.columns:
        return df.agg(F.max(F.to_date("change_ts")).alias("mx")).collect()[0]["mx"]
    return None

since_date = parse_date_or_none(since_param)
until_date = parse_date_or_none(until_param) or date.today()

if not full_reload and since_date is None and table_exists(TARGET):
    wm = pick_watermark_from_target(TARGET)
    if wm is not None:
        since_date = wm

if full_reload or since_date is None:
    since_date = date(1900, 1, 1)

print(f"Incremental window (date(change_ts)): {since_date} -> {until_date}")

# ---------- Load source ----------
t_raw = spark.table(T_SRC).alias("t_raw")

# ==========================================================
# 1) Add change_ts + filter incremental window
# ==========================================================
t_ts = (
    t_raw
    .withColumn("created_on_ts",  F.to_timestamp("created_on"))
    .withColumn("modified_on_ts", F.to_timestamp("modified_on"))
    .withColumn("change_ts",      F.greatest("created_on_ts", "modified_on_ts"))
)

t_win = (
    t_ts
    .filter(
        (F.to_date("change_ts") >= F.lit(since_date)) &
        (F.to_date("change_ts") <= F.lit(until_date))
    )
)

# ==========================================================
# 2) src CTE: add operation_group
#    operation_group = int(part before '.'); e.g. '030.01' -> 30
# ==========================================================
src = (
    t_win
    .select(
        "created_on",
        "modified_on",
        "prod_order_no",
        "prod_order_status",
        "prod_order_line_no",
        "item_no",
        "operation_no",
        "operation_type",
        "routing_no",
        "run_time",
        "change_ts"
    )
    .withColumn(
        "operation_group",
        F.when(F.col("operation_no").isNull(), F.lit(None).cast("int"))
         .otherwise(
             F.substring_index("operation_no", ".", 1).cast("int")
         )
    )
)

# ==========================================================
# 3) calc CTE: routing_no_machine_center + routing_no_work_center (window MAX)
# ==========================================================
w_wc = W.partitionBy("prod_order_no", "prod_order_line_no", "operation_group")

calc = (
    src
    .withColumn(
        "routing_no_machine_center",
        F.when(F.col("operation_type") == F.lit("Machine Center"), F.col("routing_no"))
         .otherwise(F.lit(None).cast(src.schema["routing_no"].dataType))
    )
    .withColumn(
        "routing_no_work_center",
        F.max(
            F.when(F.col("operation_type") == F.lit("Work Center"), F.col("routing_no"))
        ).over(w_wc)
    )
)

# ==========================================================
# 4) Final filter + add rol (as in view)
#     rol = CONCAT(prod_order_no, prod_order_line_no, operation_group)
# ==========================================================
final_df = (
    calc
    .filter(
        (F.col("operation_type") == F.lit("Machine Center")) |
        ((F.col("operation_group") == F.lit(9)) & (F.col("operation_no") == F.lit("009")))
    )
    .withColumn(
        "rol",
        F.concat(
            F.coalesce(F.col("prod_order_no").cast("string"), F.lit("")),
            F.coalesce(F.col("prod_order_line_no").cast("string"), F.lit("")),
            F.coalesce(F.col("operation_group").cast("string"), F.lit(""))
        )
    )
)

# ==========================================================
# 5) Add row_id for MERGE (deterministic key) + dedup
# ==========================================================
row_id_cols = [
    "prod_order_no",
    "prod_order_line_no",
    "operation_no",
    "routing_no",
    "operation_type"
]

final_with_id = final_df.withColumn(
    "row_id",
    F.sha2(
        F.concat_ws(
            "||",
            *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in row_id_cols]
        ),
        256
    )
)

# ensure we keep the latest change_ts per row_id
w_dedup = W.partitionBy("row_id").orderBy(F.col("change_ts").desc_nulls_last())

final_dedup = (
    final_with_id
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# ---------- Write / Merge ----------
def merge_or_create(target, df):
    if not table_exists(target):
        print(f"Creating {target} ...")
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .saveAsTable(target)
        )
        return
    print(f"Merging into {target} ...")
    tgt = DeltaTable.forName(spark, target)
    (
        tgt.alias("tgt")
           .merge(df.alias("src"), "tgt.row_id <=> src.row_id")
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute()
    )

merge_or_create(TARGET, final_dedup)
print(f"✅ Done: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Standard vs Actual Time

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1) Build the incremental dataframe (same as you already do)
c_df = spark.table("Gold_Production_Lakehouse.prod.gold_production_status_cycle_time").alias("C")
r_df = spark.table("Gold_Production_Lakehouse.prod.gold_production_routing_group").alias("R")

stg_df = (
    c_df.join(
        r_df,
        (F.col("C.prod_order_no") == F.col("R.prod_order_no")) &
        (F.col("C.prod_order_line_no") == F.col("R.prod_order_line_no")) &
        (F.col("C.op_major") == F.col("R.operation_group")),
        "inner"
    )
    .select(
        "C.prod_order_no",
        "C.prod_order_line_no",
        "C.op_major",
        "C.prod_line",
        "C.cell_line",
        "C.operation",
        "C.routing_no_machine_center",
        "C.in_created",
        "C.out_created",
        "C.to_created",
        "C.from_created",
        "C.dead_to_created",
        "C.station_time",
        "C.operation_time",
        "C.dead_time",
        "C.quantity",
        "C.routing_no_work_center",
        "C.item_no",
        "R.run_time"
    )
)

# 2) Enforce uniqueness on MERGE keys
w = Window.partitionBy(
    "prod_order_no",
    "prod_order_line_no",
    "op_major",
    "operation"
).orderBy(F.col("in_created").desc_nulls_last())

stg_dedup = (
    stg_df
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)   # keep just one row per key
    .drop("rn")
)

# 3) Create target table (run once; your existing code is fine)
spark.sql("""
CREATE TABLE IF NOT EXISTS Gold_Production_Lakehouse.prod.gold_standard_vs_actual_time
USING DELTA
AS SELECT * FROM (
    SELECT *
    FROM (
        SELECT
            cast(null as string) as prod_order_no,
            cast(null as string) as prod_order_line_no,
            cast(null as string) as op_major,
            cast(null as string) as prod_line,
            cast(null as string) as cell_line,
            cast(null as string) as operation,
            cast(null as string) as routing_no_machine_center,
            cast(null as timestamp) as in_created,
            cast(null as timestamp) as out_created,
            cast(null as timestamp) as to_created,
            cast(null as timestamp) as from_created,
            cast(null as timestamp) as dead_to_created,
            cast(null as double) as station_time,
            cast(null as double) as operation_time,
            cast(null as double) as dead_time,
            cast(null as double) as quantity,
            cast(null as string) as routing_no_work_center,
            cast(null as string) as item_no,
            cast(null as double) as run_time
        LIMIT 0
    )
)
""")

# 4) Load staging table with the de-duplicated data
(
    stg_dedup.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "false")
    .saveAsTable("Gold_Production_Lakehouse.prod.gold_standard_vs_actual_time_staging")
)

# 5) MERGE – should no longer throw multiple source row errors
spark.sql("""
MERGE INTO Gold_Production_Lakehouse.prod.gold_standard_vs_actual_time AS tgt
USING Gold_Production_Lakehouse.prod.gold_standard_vs_actual_time_staging AS src
ON  tgt.prod_order_no      = src.prod_order_no
AND tgt.prod_order_line_no = src.prod_order_line_no
AND tgt.op_major           = src.op_major
AND tgt.operation          = src.operation
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Prod Planned Vs Actual QTY (TEMP)

# CELL ********************

# ================================================================
# Job: Gold_Production_Lakehouse.prod.gold_prod_planned_vs_actual_qty  (incremental)
# Mirrors your final SQL (robust casting, no FIN-GOODS filters)
# - planned_qty: SUM(header.prod_order_quantity) per TRY_CONVERT(date, ending_date)
# - actual_output_qty: SUM(io.entry_type_item_quantity) per TRY_CONVERT(date, posting/created)
# - variance_qty = planned_qty - actual_output_qty
# - cum_* = cumulative totals by date
# ================================================================

from pyspark.sql import functions as F, Window as W
from delta.tables import DeltaTable
from datetime import datetime, date

# ---------- Sources ----------
POH_SRC = "Silver_Production_Lakehouse.prod.silver_prod_order_header"
IO_SRC  = "Gold_Production_Lakehouse.prod.gold_inv_output"
TARGET  = "Gold_Production_Lakehouse.prod.gold_prod_planned_vs_actual_qty"

# ---------- Parameters ----------
def get_widget(name: str, default: str) -> str:
    try:
        import dbutils  # type: ignore
        return dbutils.widgets.get(name)  # type: ignore
    except Exception:
        return default

try:
    dbutils.widgets.text("full_reload", "false")
    dbutils.widgets.text("since", "")
    dbutils.widgets.text("until", "")
except Exception:
    pass

full_reload = get_widget("full_reload", "false").strip().lower() == "true"
since_param = get_widget("since", "").strip()
until_param = get_widget("until", "").strip()

def parse_date_or_none(s: str):
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()

def table_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False

def pick_watermark_from_target(table_name: str):
    try:
        df = spark.table(table_name)
    except Exception:
        return None
    if "date" in df.columns:
        return df.agg(F.max("date").alias("mx")).collect()[0]["mx"]
    return None

since_date = parse_date_or_none(since_param)
until_date = parse_date_or_none(until_param) or date.today()

if not full_reload and since_date is None and table_exists(TARGET):
    wm = pick_watermark_from_target(TARGET)
    if wm is not None:
        since_date = wm

if full_reload or since_date is None:
    since_date = date(1900, 1, 1)

print(f"Incremental window (date): {since_date} -> {until_date}")

# ---------- Load sources ----------
poh = spark.table(POH_SRC).alias("poh")
io  = spark.table(IO_SRC).alias("io")

# ---------- PlannedDaily ----------
planned_daily = (
    poh
    .withColumn("date", F.to_date("prod_order_ending_date_time"))
    .groupBy("date")
    .agg(F.sum(F.col("prod_order_quantity")).cast("double").alias("planned_qty"))
)

# ---------- ActualDaily ----------
actual_daily = (
    poh.join(io, F.col("io.order_no") == F.col("poh.prod_order_no"), "inner")
       .withColumn("date", F.to_date(F.col("io.posting_date")))
       .groupBy("date")
       .agg(F.sum(F.col("io.total_qty")).cast("double").alias("actual_output_qty"))
)


# ---------- Combine ----------
combined = (
    planned_daily.alias("p")
    .join(actual_daily.alias("a"), F.col("p.date") == F.col("a.date"), "full")
    .select(
        F.coalesce(F.col("p.date"), F.col("a.date")).alias("date"),
        F.coalesce(F.col("p.planned_qty"), F.lit(0.0)).alias("planned_qty"),
        F.coalesce(F.col("a.actual_output_qty"), F.lit(0.0)).alias("actual_output_qty"),
    )
)

# ---------- Variance + cumulative ----------
w = W.orderBy(F.col("date")).rowsBetween(W.unboundedPreceding, W.currentRow)
final_df_all = (
    combined
    .withColumn("variance_qty", F.col("planned_qty") - F.col("actual_output_qty"))
    .withColumn("cum_planned_qty", F.sum("planned_qty").over(w))
    .withColumn("cum_actual_qty", F.sum("actual_output_qty").over(w))
    .withColumn("cum_variance_qty", F.sum(F.col("planned_qty") - F.col("actual_output_qty")).over(w))
)

# ---------- Incremental filter ----------
final_df = final_df_all.filter(
    (F.col("date") >= F.lit(since_date)) & (F.col("date") <= F.lit(until_date))
)

# ---------- Write / Merge ----------
def merge_or_create(target, df):
    if not table_exists(target):
        print(f"Creating {target} ...")
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .saveAsTable(target)
        )
        return

    print(f"Merging into {target} ...")
    tgt = DeltaTable.forName(spark, target)
    (
        tgt.alias("tgt")
           .merge(df.alias("src"), "tgt.date <=> src.date")
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute()
    )

merge_or_create(TARGET, final_df)
print(f"✅ Done: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Casting Status

# CELL ********************

# Databricks / PySpark
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

# ---------- 1) Source tables ----------
gpo_src = spark.table("Gold_Production_Lakehouse.prod.gold_production_order")
cp_src  = spark.table("Silver_Production_Lakehouse.prod.silver_casting_parts").alias("cp")
ct_src  = spark.table("Silver_Production_Lakehouse.prod.silver_casting_tree").alias("ct")

# ---------- 2) Latest GPO per (prod_order_no, prod_order_line_no) ----------
w_gpo = Window.partitionBy("prod_order_no", "prod_order_line_no").orderBy(F.col("_modified_any").desc())

gpo_latest = (
    gpo_src
    .withColumn("rn", F.row_number().over(w_gpo))
    .filter(F.col("rn") == 1)
    .filter(F.col("prod_order_status") == F.lit("Released"))
    .filter(F.col("item_location")   == F.lit("CST_CUT"))
    .filter(F.col("prod_item_line").like("C%"))
)

# ---------- 3) Latest Casting per (prod_order_no, prod_order_line_no) ----------
# Join parts -> tree; stamp ONLY from casting_tree (ct.*) per your note
cp_ct_joined = (
    cp_src.join(ct_src, F.col("cp.casting_prod_order") == F.col("ct.casting_prod_order"), "left")
          .withColumn("stamp", F.coalesce(F.col("ct.modified_on"), F.col("ct.created_on")))
)

w_cast = (
    Window.partitionBy(F.col("cp.prod_order_no"), F.col("cp.prod_order_line_no"))
          .orderBy(F.col("stamp").desc(), F.col("cp.casting_prod_order").desc())
)

c_latest = (
    cp_ct_joined
    .withColumn("rn", F.row_number().over(w_cast))
    .filter(F.col("rn") == 1)
    .select(
        F.col("cp.prod_order_no").alias("prod_order_no"),
        F.col("cp.prod_order_line_no").alias("prod_order_line_no"),
        F.col("cp.casting_prod_order").alias("casting_prod_order"),
        F.col("ct.casting_tree_no").alias("casting_tree_no"),
        F.col("ct.casting_status").alias("casting_status"),
        F.col("stamp")
    )
)

# ---------- 4) Combine & compute derived columns ----------
joined = (
    gpo_latest.alias("gpo")
    .join(c_latest.alias("c"), ["prod_order_no", "prod_order_line_no"], how="left")
)

status_clean = F.upper(F.trim(F.col("c.casting_status")))
is_complete  = status_clean == F.lit("COMPLETE")
today        = F.current_date()

job_status = (
    F.when(is_complete, "finished")
     .when((~is_complete) & F.col("gpo.prod_line_due_date").isNotNull() & (F.col("gpo.prod_line_due_date") < today), "overdue")
     .when(
         (~is_complete) &
         (F.col("gpo.prod_line_due_date").isNull() | (F.col("gpo.prod_line_due_date") >= today)) &
         (
             F.col("gpo.prod_line_start_date").isNotNull() |
             (F.trim(F.col("c.casting_status")).isNotNull() & (status_clean != F.lit("NOT START")))
         ),
         "in process"
     )
     .otherwise("remaining")
)

result_df = (
    joined.select(
        F.col("gpo.prod_order_no"),
        F.col("gpo.prod_order_line_no"),
        F.col("gpo.prod_item_line"),
        F.col("gpo.prod_line_start_date"),
        F.col("gpo.prod_line_end_date"),
        F.col("gpo.prod_line_due_date").alias("line_casting_due_date"),
        F.col("gpo.prod_line_quantity").alias("quantity"),
        F.col("gpo.prod_line_remaining_quantity").alias("remaining_quantity"),
        F.col("c.casting_prod_order"),
        F.col("c.casting_tree_no"),
        F.col("c.casting_status"),
        job_status.alias("job_status")
    )
)

# ---------- 5) Incremental upsert into Delta table ----------
target_table = "Gold_Production_Lakehouse.prod.gold_casting_status"

def table_exists(name: str) -> bool:
    try:
        spark.table(name)
        return True
    except AnalysisException:
        return False

if not table_exists(target_table):
    result_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
else:
    DeltaTable.forName(spark, target_table) \
        .alias("t") \
        .merge(
            result_df.alias("s"),
            "t.prod_order_no = s.prod_order_no AND t.prod_order_line_no = s.prod_order_line_no"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

# Optional maintenance
# spark.sql(f"OPTIMIZE {target_table} ZORDER BY (line_casting_due_date, job_status)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Prod Cycle Time

# CELL ********************

# ================================================================
# Job: Gold_Production_Lakehouse.prod.gold_prod_cycle_time  (safe merge)
# Mirrors v_gold_production_status_cycle_time_distinct
#   - DISTINCT join between:
#       gold_prod_status_cycle_time_header (H)
#       gold_production_status_cycle_time (S)
# Fix:
#   - Use a truly unique MERGE key (row_id built from ALL output cols)
#   - dropDuplicates(["row_id"]) before MERGE to prevent multi-match
# ================================================================

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# ---------- Sources ----------
H_SRC = "Gold_Production_Lakehouse.prod.gold_prod_status_cycle_time_header"
S_SRC = "Gold_Production_Lakehouse.prod.gold_production_status_cycle_time"

# ---------- Target ----------
TARGET = "Gold_Production_Lakehouse.prod.gold_prod_cycle_time"

# ---------- Utils ----------
def table_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False

def merge_or_create(target, df):
    if not table_exists(target):
        print(f"Creating {target} ...")
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .saveAsTable(target)
        )
        return
    print(f"Merging into {target} ...")
    tgt = DeltaTable.forName(spark, target)
    (
        tgt.alias("tgt")
           .merge(df.alias("src"), "tgt.row_id <=> src.row_id")
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute()
    )

# ---------- Load ----------
H = spark.table(H_SRC).alias("H")
S = spark.table(S_SRC).alias("S")

# ---------- Join + Distinct (mirror the view) ----------
joined = (
    H.join(
        S,
        (F.col("H.prod_order_no") == F.col("S.prod_order_no")) &
        (F.col("H.prod_order_line_no") == F.col("S.prod_order_line_no")),
        "inner"
    )
    .select(
        F.col("H.customer_name").alias("customer_name"),
        F.col("H.sales_order_no").alias("sales_order_no"),
        F.col("H.sales_order_line_no").alias("sales_order_line_no"),
        F.col("H.FG_item_no").alias("FG_item_no"),
        F.col("H.customer_no").alias("customer_no"),
        F.col("S.prod_order_no").alias("prod_order_no"),
        F.col("S.prod_order_line_no").alias("prod_order_line_no"),
        F.col("S.op_major").alias("op_major"),
        F.col("S.prod_line").alias("prod_line"),
        F.col("S.cell_line").alias("cell_line"),
        F.col("S.operation").alias("operation"),
        F.col("S.operation time").alias("operation_time"),
        F.col("S.Dead Time").alias("dead_time"),
        F.col("S.item_no").alias("item_no"),
        F.col("S.quantity").alias("quantity"),
        F.col("S.in_created").alias("in_created"),
        F.col("S.out_created").alias("out_created"),
        F.col("S.to_created").alias("to_created"),
        F.col("S.from_created").alias("from_created"),
        F.col("S.dead_to_created").alias("dead_to_created"),
        F.col("S.station time").alias("station_time"),
    )
    .distinct()
)

# ---------- Build a truly unique MERGE key ----------
row_id_cols = [
    "customer_name","sales_order_no","sales_order_line_no","FG_item_no","customer_no",
    "prod_order_no","prod_order_line_no","op_major","prod_line","cell_line","operation",
    "operation_time","dead_time","item_no","quantity",
    "in_created","out_created","to_created","from_created","dead_to_created","station_time"
]

joined = joined.withColumn(
    "row_id",
    F.sha2(
        F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in row_id_cols]),
        256
    )
)

# Extra safety: remove any accidental duplicate row_ids before MERGE
src = joined.dropDuplicates(["row_id"])

# ---------- Write / Merge ----------
merge_or_create(TARGET, src)
print(f"✅ Done: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
