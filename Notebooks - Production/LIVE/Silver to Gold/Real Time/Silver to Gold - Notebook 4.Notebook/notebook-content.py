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
# META         },
# META         {
# META           "id": "1d620310-5acc-4534-93f9-f52f082a1887"
# META         }
# META       ]
# META     },
# META     "mirrored_db": {
# META       "known_mirrored_dbs": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Production Order

# CELL ********************


spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead",  "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead",     "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite",    "LEGACY")

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## All Silver 

# CELL ********************

# ==============================================================
# GOLD: prod.gold_production_order — FULL RELOAD (BC mirror-safe)
# - Source remap to BC mirror (no [col] syntax; use backticks)
# - Hard schema overwrite (overwriteSchema=true)
# - Source hard-dedupe on KEYS (keeps latest by _modified_any)
# - FIXED: Correct overdue / due logic
# ==============================================================

spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead",  "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead",     "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite",    "LEGACY")

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql import functions as F, Window

# -------- CONFIG (REMAPPED SOURCES) --------
S_ORDER = "Silver_BC_Lakehouse.bc.`Production Order`"
S_LINE  = "Silver_BC_Lakehouse.bc.`Prod Order Line`"
PLANNING_FORWARD_SCHEDULE = "Gold_Production_Lakehouse.prod.planning_forward_schedule"
TARGET  = "prod.gold_production_order"

KEYS   = ["prod_order_no", "prod_order_line_no"]
MODCOL = "_modified_any"

# -------- helpers --------
def table_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False

def maintain(name: str, zcols=None, vacuum_hours: int = 168):
    try:
        if zcols:
            cols = ", ".join([f"`{c}`" for c in zcols])
            spark.sql(f"ANALYZE TABLE {name} COMPUTE STATISTICS FOR COLUMNS {cols}")
            spark.sql(f"OPTIMIZE {name} ZORDER BY ({cols})")
        else:
            spark.sql(f"OPTIMIZE {name}")
    except Exception as e:
        print(f"OPTIMIZE notice: {e}")
    spark.sql(f"VACUUM {name} RETAIN {vacuum_hours} HOURS")

def keep_latest_per_keys(df, keys, order_cols):
    w = Window.partitionBy(*[F.col(k) for k in keys]).orderBy(*order_cols)
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

# -------- 1) read BC mirror --------
po_raw = spark.table(S_ORDER)
pl_raw = spark.table(S_LINE)

print(f"po={po_raw.count():,} pl={pl_raw.count():,}")

# -------- 2) map columns --------
po = (
    po_raw.select(
        F.col("`No.`").alias("prod_order_no"),
        F.col("`Status`").alias("prod_order_status"),
        F.col("`Source No.`").alias("FG_item_no"),
        F.col("`Routing No.`").alias("item_routing_no"),
        F.col("`Quantity`").alias("prod_order_quantity"),
        F.col("`Starting Date-Time`").alias("prod_order_starting_date_time"),
        F.col("`Ending Date-Time`").alias("prod_order_ending_date_time"),
        F.col("`Finished Date`").alias("prod_order_finished_date"),
        F.col("`Due Date`").alias("prod_order_due_date"),
        F.col("`Sales Order No.`").alias("sales_order_no"),
        F.col("`Sales Order Line No.`").alias("sales_order_line_no"),
        F.col("`For Prod.Order No.`").alias("ref_prod_order"),
        F.col("`For Item`").alias("ref_item"),
        F.col("`SystemModifiedAt`").alias("po_modified_on"),
        F.col("`SystemCreatedAt`").alias("po_created_on"),
    )
)

pl = (
    pl_raw.select(
        F.col("`Prod. Order No.`").alias("prod_order_no"),
        F.col("`Line No.`").alias("prod_order_line_no"),
        F.col("`Item No.`").alias("prod_item_line"),
        F.col("`Location Code`").alias("item_location"),
        F.col("`Due Date`").alias("prod_line_due_date"),
        F.col("`Starting Date-Time`").alias("prod_line_start_date"),
        F.col("`Ending Date-Time`").alias("prod_line_end_date"),
        F.col("`Quantity`").alias("prod_line_quantity"),
        F.col("`Finished Quantity`").alias("prod_line_finished_quantity"),
        F.col("`Remaining Quantity`").alias("prod_line_remaining_quantity"),
        F.col("`Description`").alias("description"),
        F.col("`SystemModifiedAt`").alias("pl_modified_on"),
        F.col("`SystemCreatedAt`").alias("pl_created_on"),
    )
)

# -------- 3) join --------
joined = (
    po.alias("po")
      .join(pl.alias("pl"), "prod_order_no", "left")
)

# -------- 3.5) attach planned due date from planning_forward_schedule --------
# Replaces the BC-derived prod_order_due_date with MAX(scheduled_end_date)
# from the latest engine_run per (prod_order_no, prod_order_line_no).
# Falls back to the BC Due Date when planning has no matching row.
_pfs_w = Window.partitionBy("prod_order_no", "prod_order_line_no").orderBy(F.col("engine_run_ts").desc())
pfs = (
    spark.table(PLANNING_FORWARD_SCHEDULE)
    .select("prod_order_no", "prod_order_line_no", "scheduled_end_date", "engine_run_ts")
    .withColumn("_rr", F.dense_rank().over(_pfs_w))
    .filter(F.col("_rr") == 1)
    .groupBy("prod_order_no", "prod_order_line_no")
    .agg(F.max("scheduled_end_date").alias("planned_prod_order_due_date"))
)

joined = (
    joined
    .join(pfs, ["prod_order_no", "prod_order_line_no"], "left")
    .withColumn(
        "prod_order_due_date",
        F.coalesce(F.col("planned_prod_order_due_date"), F.col("prod_order_due_date")),
    )
    .drop("planned_prod_order_due_date")
)

# -------- 4) date logic FIXED --------
due_diff_past   = F.datediff(F.current_date(), F.col("prod_order_due_date"))      # >0 = overdue
due_diff_future = F.datediff(F.col("prod_order_due_date"), F.current_date())      # >0 = days remaining

joined = (
    joined.select(
        "prod_order_status",
        "prod_order_no",
        F.col("prod_order_line_no").cast("string"),

        "sales_order_no",
        "sales_order_line_no",
        "FG_item_no",
        F.substring_index("FG_item_no", "-", 1).alias("FG_item_group"),

        "prod_item_line",
        "item_routing_no",
        "prod_order_quantity",

        "prod_order_starting_date_time",
        "prod_order_ending_date_time",
        "prod_order_finished_date",
        "prod_order_due_date",
        F.weekofyear("prod_order_due_date").alias("commit_week"),

        "ref_prod_order",
        "ref_item",

        "prod_line_due_date",
        "prod_line_start_date",
        "prod_line_end_date",
        "prod_line_quantity",
        "prod_line_finished_quantity",
        "prod_line_remaining_quantity",
        "item_location",
        "description",

        F.concat(F.col("sales_order_no").cast("string"),
                 F.col("sales_order_line_no").cast("string")).alias("SOL"),

        F.concat(F.col("prod_order_no").cast("string"),
                 F.col("prod_order_line_no").cast("string")).alias("POL"),

        # -------- FIXED Due In --------
        F.when(F.col("prod_order_due_date").isNull(), F.lit(None))
         .when(due_diff_past > 0,
               F.concat(F.lit("Overdue "),
                        due_diff_past.cast("string"),
                        F.lit("d")))
         .otherwise(
               F.concat(F.lit("Due "),
                        due_diff_future.cast("string"),
                        F.lit("d"))
         ).alias("due_in"),

        # -------- FIXED Due Status --------
        F.when(F.col("prod_order_due_date").isNull(), F.lit(None))
         .when(due_diff_past > 0, F.lit("Overdue"))
         .when(due_diff_future <= 3, F.lit("At risk"))
         .otherwise(F.lit("On time")).alias("due_status"),

        "po_modified_on",
        "pl_modified_on",
        "po_created_on",
        "pl_created_on"
    )
)

# -------- 5) unified modified watermark --------
joined = joined.withColumn(
    MODCOL,
    F.greatest(
        F.coalesce(F.col("po_modified_on").cast("timestamp"),
                   F.col("po_created_on").cast("timestamp")),
        F.coalesce(F.col("pl_modified_on").cast("timestamp"),
                   F.col("pl_created_on").cast("timestamp")),
    )
)

final_projection = joined.drop(
    "po_modified_on","pl_modified_on",
    "po_created_on","pl_created_on"
)

# -------- 6) dedupe --------
final_dedup = keep_latest_per_keys(
    final_projection,
    KEYS,
    [
        F.col(MODCOL).desc_nulls_last(),
        F.sha2(
            F.concat_ws("§", *[
                F.coalesce(F.col(c).cast("string"), F.lit(""))
                for c in final_projection.columns
            ]),
            256
        ).desc()
    ]
)

print("Rows after dedupe:", final_dedup.count())

# -------- 7) full reload --------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET.rsplit('.',1)[0]}")

(
    final_dedup
      .write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(TARGET)
)

print(f"Done FULL RELOAD overwrite into {TARGET}.")

# -------- 8) maintenance --------
maintain(TARGET, zcols=KEYS + [MODCOL])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Sales Order

# MARKDOWN ********************

# ## All Silver

# CELL ********************

# ==============================================================
# GOLD: prod.gold_sales_order — FULL RELOAD (BC mirror sources)
# ==============================================================

from pyspark.sql import functions as F
from delta.tables import DeltaTable

spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead",  "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead",     "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite",    "LEGACY")

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite","LEGACY")

# ---------- CONFIG (BC mirror sources) ----------
SH = "Silver_BC_Lakehouse.bc.`Sales Header`"   # header (BC)
SL = "Silver_BC_Lakehouse.bc.`Sales Line`"     # line   (BC)
CU = "Silver_BC_Lakehouse.bc.`Customer`"       # customer (BC)

TARGET = "prod.gold_sales_order"
MODCOL = "_modified_any"

# ---------- helpers ----------
def table_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False

def create_with_schema(name: str, df):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {name.rsplit('.',1)[0]}")
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(name))
    spark.sql(f"""
      ALTER TABLE {name}
      SET TBLPROPERTIES (
        delta.autoOptimize.optimizeWrite = true,
        delta.autoOptimize.autoCompact = true
      )
    """)

# ---------- 1) Read BC sources + map columns ----------
sh_src = (
    spark.table(SH)
    .selectExpr(
        "`No.`                                as sales_order_no",
        "`Sell-to Customer No.`               as customer_no",
        "`Sell-to Customer Name`              as customer_name",
        "`Status`                             as sales_order_status",
        "`Requested Delivery Date`            as sales_order_requested_date",
        "`Promised Delivery Date`             as sales_order_promised_date",
        "`External Document No.`              as sales_order_external_document",
        "`Document Date`                      as sales_order_document_date",
        "`Released Date`                      as sales_order_released_date",
        "`Your Reference`                     as sales_order_cs_reference",
        "`Salesperson Code`                   as cs_team",
        "`SystemModifiedAt`                   as sh_modified_on"
    )
)

sl_src = (
    spark.table(SL)
    .selectExpr(
        "`Document No.`                       as sales_order_no",
        "`Document Type`                      as document_type",
        "`Line No.`                           as sales_order_line_no",
        "`No.`                                as item_no",
        "`Description`                        as item_description",
        "`Quantity`                           as item_quantity",
        "`Shipment Date`                      as sales_order_shipment_date",
        "`Shortcut Dimension 2 Code`          as item_material",
        "`Item Reference No.`                 as item_reference",
        "`Qty. to Ship`                       as item_quantity_to_ship",
        "`Quantity Shipped`                   as item_quantity_shipped",
        "`Qty. to Invoice`                    as item_quantity_to_invoice",
        "`Quantity Invoiced`                  as item_quantity_invoiced",
        "`Outstanding Quantity`               as item_outstanding",
        "`SystemModifiedAt`                   as sl_modified_on"
    )
)

cu_src = (
    spark.table(CU)
    .selectExpr(
        "`No.`                                as customer_no",
        "`Name`                               as customer_name_master",
        "`DSVC Branch ID`                     as customer_abbreviation",
        "`SystemModifiedAt`                   as cu_modified_on"
    )
)

sh = sh_src.alias("sh")
sl = sl_src.alias("sl")
cu = cu_src.alias("c")

# ---------- 2) Join + filter ----------
status_ok = F.col("sh.sales_order_status").isin(
    "Open", "Released", "Pending Approval", "Pending Prepayment", "Closed", "Hold"
)

joined = (
    sh.join(sl, F.col("sh.sales_order_no") == F.col("sl.sales_order_no"), "left")
      .join(cu, F.col("c.customer_no") == F.col("sh.customer_no"), "left")
      .where(status_ok)
)

# ---------- 3) Unified modified (still useful as a field) ----------
wm_expr = F.greatest(
    F.to_timestamp(F.col("sl.sl_modified_on")),
    F.to_timestamp(F.col("sh.sh_modified_on")),
    F.to_timestamp(F.col("c.cu_modified_on"))
)

wm_expr = F.coalesce(
    wm_expr,
    F.to_timestamp(F.col("sh.sales_order_released_date")),
    F.to_timestamp(F.col("sh.sales_order_requested_date")),
    F.current_timestamp()
)

# ---------- 4) Projection to gold schema ----------
sales = (
    joined.select(
        # header / customer
        F.col("sh.sales_order_no").alias("SalesorderNo"),
        F.col("sh.customer_no").alias("CusNo"),
        F.col("sh.customer_name").alias("CusName"),
        F.col("c.customer_abbreviation").alias("CusAbbr"),
        F.col("sh.sales_order_status").alias("StatusSO"),
        F.col("sh.sales_order_requested_date").alias("ReqDate"),
        F.col("sh.sales_order_promised_date").alias("PmDate"),
        F.col("sh.sales_order_cs_reference").alias("CSNoted"),
        F.col("sh.cs_team").alias("CSteam"),
        F.col("sh.sales_order_external_document").alias("sales_order_external_document"),
        F.col("sh.sales_order_released_date").alias("sales_order_released_date"),
        F.col("sh.sales_order_document_date").alias("sales_order_document_date"),
        F.weekofyear(F.col("sh.sales_order_requested_date")).alias("requested_week"),

        # line
        F.col("sl.document_type").alias("document_type"),
        F.col("sl.sales_order_line_no").alias("SalesLineNo"),
        F.col("sl.item_no").alias("ItemFG"),
        F.col("sl.item_description").alias("item_description"),
        F.col("sl.item_quantity").alias("Total_QTY"),
        F.col("sl.sales_order_shipment_date").alias("LineShipmentDate"),
        F.col("sl.item_material").alias("TypeofFG"),
        F.col("sl.item_material").alias("line_item_material"),
        F.col("sl.item_reference").alias("item_reference"),
        F.col("sl.item_quantity_to_ship").alias("item_quantity_to_ship"),
        F.col("sl.item_quantity_shipped").alias("item_quantity_shipped"),
        F.col("sl.item_quantity_to_invoice").alias("item_quantity_to_invoice"),
        F.col("sl.item_quantity_invoiced").alias("item_quantity_invoiced"),
        F.col("sl.item_outstanding").alias("OutstandingQty"),

        # derived
        F.concat(F.col("sh.sales_order_no").cast("string"),
                 F.col("sl.item_no").cast("string")).alias("SOI"),
        F.concat(F.col("sh.sales_order_no").cast("string"),
                 F.col("sl.sales_order_line_no").cast("string")).alias("SOL"),
        F.concat(F.expr("left(sh.sales_order_no, 2)"),
                 F.expr("right(sh.sales_order_no, 4)")).alias("so_abbr"),
        F.expr("left(sh.sales_order_no, 2)").alias("so_type"),

        # unified modified
        F.to_timestamp(wm_expr).alias(MODCOL)
    )
    .dropDuplicates()
)

# ---------- 5) System fields + row hash ----------
content_cols = sales.columns
final_df = (
    sales
      .withColumn("updated_at", F.col(MODCOL))
      .withColumn("load_ts", F.current_timestamp())
      .withColumn("source_system", F.lit("bc_sales"))
      .withColumn(
          "row_hash",
          F.sha2(F.concat_ws("§", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in content_cols]), 256)
      )
)

# ---------- 6) FULL RELOAD write (overwrite table) ----------
create_with_schema(TARGET, final_df)

print(f"✅ Full reload done: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Prod Repair

# MARKDOWN ********************

# ## All Silver

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# ---------------------------------------
# Config
# ---------------------------------------
TARGET_TABLE = "Gold_Production_Lakehouse.prod.gold_prod_repair"  
P_TABLE = "Silver_Production_Lakehouse.prod.silver_production_tracking_repairprophoto"
R_TABLE = "Silver_Production_Lakehouse.prod.silver_prod_order_repair"
O_TABLE = "Gold_Production_Lakehouse.prod.gold_production_order"
S_TABLE = "Gold_Production_Lakehouse.prod.gold_sales_order"

# ---------------------------------------
# Helper: check if table exists
# ---------------------------------------
def table_exists(table_name: str) -> bool:
    try:
        return spark._jsparkSession.catalog().tableExists(table_name)
    except Exception:
        try:
            spark.table(table_name)
            return True
        except AnalysisException:
            return False

# ---------------------------------------
# Helper: get last loaded timestamp
# ---------------------------------------
def get_last_loaded_ts(table_name: str, ts_col: str = "record_ts"):
    try:
        df = spark.table(table_name)
        row = df.agg(F.max(ts_col).alias("max_ts")).collect()[0]
        return row["max_ts"]
    except AnalysisException:
        # table does not exist
        return None

# ---------------------------------------
# Build source dataframe (PySpark equivalent of your SQL)
# ---------------------------------------
P = spark.table(P_TABLE).alias("P")
R = spark.table(R_TABLE).alias("R")
O = spark.table(O_TABLE).alias("O")
S = spark.table(S_TABLE).alias("S")

src_df = (
    P.join(
        R,
        (
            (P.main_production_order_no == R.prod_order_no) &
            (P.main_production_order_line_no == R.prod_order_line_no) &
            (P.repair_production_order_no == R.repair_prod_order_no)
        ),
        "inner"
    )
    .join(
        O,
        (
            (P.main_production_order_no == O.prod_order_no) &
            (P.main_production_order_line_no == O.prod_order_line_no)
        ),
        "inner"
    )
    .join(
        S,
        O.sales_order_no == S.SalesorderNo,
        "inner"
    )
    .select(
        # P columns
        P.created_on,
        P.created_by_name,
        P.modified_on,
        P.modified_by_name,
        P.main_production_order_no,
        P.main_production_order_line_no,
        P.repair_production_order_no,
        P.defect_type,
        P.photo_url,

        # R columns (cast date/time to timestamp where needed)
        F.to_timestamp(R.created_on).alias("r_created_on"),
        F.to_timestamp(R.modified_on).alias("r_modified_on"),
        R.created_by.alias("r_created_by"),
        R.prod_order_no,
        R.prod_order_line_no,
        R.repair_prod_order_no,
        R.repair_quantity,
        R.repair_posting_status,
        R.defect_type.alias("r_defect_type"),
        R.work_center_no,

        # O columns
        O.sales_order_no.alias("SO"),
        O.FG_item_no.alias("FG_item_no"),
        F.split(O.FG_item_no, "-")[0].alias("FG_group"),
        O.prod_item_line,
        O.prod_line_quantity,

        # S columns
        S.CusNo,
        S.CusName,
        S.CusAbbr,

        # technical column: latest of both modified dates (same type!)
        F.greatest(
            F.to_timestamp(P.modified_on),
            F.to_timestamp(R.modified_on)
        ).alias("record_ts")
    )
    .distinct()
)

# Optional: inspect schema
# src_df.printSchema()

# ---------------------------------------
# Incremental filtering
# ---------------------------------------
last_loaded_ts = get_last_loaded_ts(TARGET_TABLE, "record_ts") if table_exists(TARGET_TABLE) else None
print("Last loaded timestamp:", last_loaded_ts)

if last_loaded_ts is not None:
    inc_df = src_df.filter(F.col("record_ts") > F.lit(last_loaded_ts))
else:
    # First run or table does not exist: full load
    inc_df = src_df

print("Incremental row count:", inc_df.count())

# If there is nothing new, stop here
if inc_df.rdd.isEmpty():
    print("No new/updated records to load.")
else:
    from delta.tables import DeltaTable

    if not table_exists(TARGET_TABLE) or last_loaded_ts is None:
        # ---------------------------------------
        # Initial full load
        # ---------------------------------------
        print(f"Initial load into {TARGET_TABLE}")
        (
            inc_df
            .write
            .mode("overwrite")
            .format("delta")
            .saveAsTable(TARGET_TABLE)
        )
    else:
        # ---------------------------------------
        # Incremental MERGE (upsert)
        # ---------------------------------------
        print(f"Incremental MERGE into {TARGET_TABLE}")
        delta_target = DeltaTable.forName(spark, TARGET_TABLE)

        # Define business key for upsert (adjust if needed)
        merge_condition = """
          t.main_production_order_no      = s.main_production_order_no AND
          t.main_production_order_line_no = s.main_production_order_line_no AND
          t.repair_production_order_no    = s.repair_production_order_no AND
          t.defect_type                   = s.defect_type
        """

        (
            delta_target.alias("t")
            .merge(
                inc_df.alias("s"),
                merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    print("Load completed.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Casting Status

# MARKDOWN ********************

# ## Silver + Gold Production Order

# CELL ********************

# Databricks / PySpark
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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
# Join parts -> tree; stamp ONLY from casting_tree (ct.*)
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
     .when((~is_complete) & F.col("gpo.prod_line_end_date").isNotNull() & (F.col("gpo.prod_line_end_date") < today), "overdue")
     .when(
         (~is_complete) &
         (F.col("gpo.prod_line_end_date").isNull() | (F.col("gpo.prod_line_end_date") >= today)) &
         (
             F.col("gpo.prod_line_start_date").isNotNull() |
             (F.trim(F.col("c.casting_status")).isNotNull() & (status_clean != F.lit("NOT START")))
         ),
         "in process"
     )
     .when(F.col("gpo.prod_line_end_date").isNotNull() & (F.col("gpo.prod_line_end_date") < today), "overdue")
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

# ---------- 5) FULL REPLACE write to Delta table ----------
target_table = "Gold_Production_Lakehouse.prod.gold_casting_status"

(
    result_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    # If your environment uses Unity Catalog managed tables, this is usually not needed:
    # .option("path", "<optional_path_if_external_table>")
    .saveAsTable(target_table)
)

# Optional maintenance
# spark.sql(f"OPTIMIZE {target_table} ZORDER BY (line_casting_due_date, job_status)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Remaining Production

# MARKDOWN ********************

# ## Silver + Gold Production Order

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

# # Wax Status

# MARKDOWN ********************

# ## Silver + Gold Production Order

# CELL ********************

# MAGIC %%sql
# MAGIC -- Spark SQL (Databricks-style)
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_wax_status AS
# MAGIC WITH LatestGPO AS (
# MAGIC   SELECT
# MAGIC     gpo.*,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY gpo.prod_order_no, gpo.prod_order_line_no
# MAGIC       ORDER BY gpo._modified_any DESC
# MAGIC     ) AS rn
# MAGIC   FROM Gold_Production_Lakehouse.prod.gold_production_order AS gpo
# MAGIC   WHERE gpo.prod_order_status = 'Released'
# MAGIC     AND gpo.item_location     = 'CST_CUT'
# MAGIC     AND gpo.prod_item_line LIKE 'C%'
# MAGIC ),
# MAGIC GPO AS (
# MAGIC   SELECT *
# MAGIC   FROM LatestGPO
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC PosOpenRN AS (
# MAGIC   SELECT
# MAGIC     p.*,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY p.prod_order_no, p.prod_order_line_no
# MAGIC       ORDER BY COALESCE(p.modified_on, p.created_on) DESC,
# MAGIC                p.created_on DESC
# MAGIC     ) AS rn_open
# MAGIC   FROM Silver_Production_Lakehouse.prod.silver_prod_order_status AS p
# MAGIC   WHERE p.`open` = 'Yes'
# MAGIC     AND COALESCE(p.current_location_code, '') <> 'WAX'
# MAGIC ),
# MAGIC pos_open AS (
# MAGIC   SELECT
# MAGIC     p.prod_order_no          AS pos_open_prod_order_no,
# MAGIC     p.prod_order_line_no     AS pos_open_prod_order_line_no,
# MAGIC     p.created_on             AS pos_open_created_on,
# MAGIC     p.modified_on            AS pos_open_modified_on,
# MAGIC     p.type_name              AS pos_open_type_name,
# MAGIC     p.`open`                 AS pos_open_open,
# MAGIC     p.current_location_code  AS pos_open_current_location_code
# MAGIC   FROM PosOpenRN AS p
# MAGIC   WHERE p.rn_open = 1
# MAGIC ),
# MAGIC 
# MAGIC PosAnyRN AS (
# MAGIC   SELECT
# MAGIC     p.*,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY p.prod_order_no, p.prod_order_line_no
# MAGIC       ORDER BY COALESCE(p.modified_on, p.created_on) DESC,
# MAGIC                p.created_on DESC
# MAGIC     ) AS rn_any
# MAGIC   FROM Silver_Production_Lakehouse.prod.silver_prod_order_status AS p
# MAGIC ),
# MAGIC pos_any AS (
# MAGIC   SELECT
# MAGIC     p.prod_order_no       AS pos_any_prod_order_no,
# MAGIC     p.prod_order_line_no  AS pos_any_prod_order_line_no,
# MAGIC     p.type_name           AS pos_any_type_name,
# MAGIC     p.created_on          AS pos_any_created_on,
# MAGIC     p.modified_on         AS pos_any_modified_on
# MAGIC   FROM PosAnyRN AS p
# MAGIC   WHERE p.rn_any = 1
# MAGIC ),
# MAGIC 
# MAGIC Casting AS (
# MAGIC   -- มี record ในตารางนี้ = finished
# MAGIC   SELECT DISTINCT
# MAGIC     chl.prod_order_no,
# MAGIC     chl.prod_order_line_no
# MAGIC   FROM Gold_Production_Lakehouse.prod.gold_casting_header_line AS chl
# MAGIC ),
# MAGIC 
# MAGIC -- NEW: Deduplicate parts_per_mold per part_item (MAX, ignore NULL)
# MAGIC mold_parts AS (
# MAGIC   SELECT
# MAGIC     part_item,
# MAGIC     MAX(parts_per_mold) AS parts_per_mold
# MAGIC   FROM Gold_Production_Lakehouse.prod.gold_mold_item_parts
# MAGIC   WHERE parts_per_mold IS NOT NULL
# MAGIC   GROUP BY part_item
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC   gpo.prod_order_no      AS prod_order_no,
# MAGIC   gpo.prod_order_line_no AS prod_order_line_no,
# MAGIC   gpo.prod_item_line     AS prod_item_line,
# MAGIC   gpo.prod_line_start_date AS prod_line_start_date,
# MAGIC   gpo.prod_line_end_date   AS prod_line_end_date,
# MAGIC 
# MAGIC   CASE
# MAGIC     WHEN gpo.prod_line_end_date IS NOT NULL
# MAGIC       THEN CAST(date_add(CAST(gpo.prod_line_end_date AS DATE), -3) AS TIMESTAMP)
# MAGIC     ELSE CAST(NULL AS TIMESTAMP)
# MAGIC   END AS line_wax_due_date,
# MAGIC 
# MAGIC   gpo.prod_line_quantity           AS quantity,
# MAGIC   gpo.prod_line_remaining_quantity AS remaining_quantity,
# MAGIC 
# MAGIC   -- pos_open fields (latest open, not WAX)
# MAGIC   po.pos_open_created_on            AS created_on,
# MAGIC   po.pos_open_modified_on           AS modified_on,
# MAGIC   po.pos_open_type_name             AS type_name,
# MAGIC   po.pos_open_open                  AS `open`,
# MAGIC   po.pos_open_current_location_code AS current_location_code,
# MAGIC 
# MAGIC   CASE
# MAGIC     -- NEW RULE (highest priority): ถ้ามีใน casting = finished
# MAGIC     WHEN c.prod_order_no IS NOT NULL THEN 'finished'
# MAGIC 
# MAGIC     -- backup rule เดิม
# MAGIC     WHEN pa.pos_any_type_name = 'From employee' THEN 'finished'
# MAGIC     WHEN pa.pos_any_type_name = 'To employee'   THEN 'in process'
# MAGIC 
# MAGIC     -- UPDATED RULE: overdue only AFTER wax due date (end_date - 3 days)
# MAGIC     WHEN gpo.prod_line_end_date IS NOT NULL
# MAGIC      AND current_date() > date_add(CAST(gpo.prod_line_end_date AS DATE), -3)
# MAGIC       THEN 'overdue'
# MAGIC 
# MAGIC     ELSE 'remaining'
# MAGIC   END AS job_status,
# MAGIC 
# MAGIC   -- NEW: mold info
# MAGIC   mp.parts_per_mold,
# MAGIC   CASE
# MAGIC     WHEN mp.parts_per_mold IS NOT NULL
# MAGIC       THEN CEIL(gpo.prod_line_remaining_quantity / CAST(mp.parts_per_mold AS DECIMAL(18,6)))
# MAGIC     ELSE gpo.prod_line_remaining_quantity
# MAGIC   END AS mold_shots_needed
# MAGIC 
# MAGIC FROM GPO AS gpo
# MAGIC LEFT JOIN pos_open AS po
# MAGIC   ON  po.pos_open_prod_order_no      = gpo.prod_order_no
# MAGIC   AND po.pos_open_prod_order_line_no = gpo.prod_order_line_no
# MAGIC LEFT JOIN pos_any AS pa
# MAGIC   ON  pa.pos_any_prod_order_no      = gpo.prod_order_no
# MAGIC   AND pa.pos_any_prod_order_line_no = gpo.prod_order_line_no
# MAGIC LEFT JOIN Casting AS c
# MAGIC   ON  c.prod_order_no      = gpo.prod_order_no
# MAGIC   AND c.prod_order_line_no = gpo.prod_order_line_no
# MAGIC LEFT JOIN mold_parts AS mp
# MAGIC   ON  mp.part_item = gpo.prod_item_line
# MAGIC ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold RFID Transaction

# MARKDOWN ********************

# ## All Silver - need to fix

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_rfid_transaction
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC WITH mc_raw AS (
# MAGIC     SELECT *
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Machine Center`
# MAGIC ),
# MAGIC 
# MAGIC M_slim AS (
# MAGIC     SELECT
# MAGIC         CAST(`No.` AS STRING) AS machine_center_no,
# MAGIC         CAST(`Machine Employee Mapping` AS STRING) AS machine_employee_mapping
# MAGIC     FROM mc_raw
# MAGIC ),
# MAGIC 
# MAGIC M_dedup AS (
# MAGIC     SELECT
# MAGIC         machine_center_no,
# MAGIC         machine_employee_mapping
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC                ROW_NUMBER() OVER (
# MAGIC                    PARTITION BY machine_center_no
# MAGIC                    ORDER BY machine_center_no
# MAGIC                ) AS rn
# MAGIC         FROM M_slim
# MAGIC     ) x
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC -- ✅ CHANGED: Use Quantity from Prod Order Line instead of Input Quantity from Routing Line
# MAGIC prod_line_qty AS (
# MAGIC     SELECT
# MAGIC         `Prod. Order No.` AS prod_order_no,
# MAGIC         `Line No.` AS prod_order_line_no,
# MAGIC         CAST(`Quantity` AS DOUBLE) AS line_quantity
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `Prod. Order No.`, `Line No.`
# MAGIC                 ORDER BY SystemModifiedAt DESC
# MAGIC             ) AS rn
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Prod Order Line`
# MAGIC     ) WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC routing_calc AS (
# MAGIC     SELECT
# MAGIC         rl.`Prod. Order No.` AS prod_order_no,
# MAGIC         rl.`Routing Reference No.` AS prod_order_line_no,
# MAGIC         rl.`Operation No.` AS operation_no,
# MAGIC 
# MAGIC         -- Quantity from Prod Order Line (not Input Quantity from Routing Line)
# MAGIC         COALESCE(plq.line_quantity, CAST(rl.`Input Quantity` AS DOUBLE)) AS input_quantity,
# MAGIC         CAST(rl.`Run Time` AS DOUBLE) AS run_time,
# MAGIC         COALESCE(plq.line_quantity, CAST(rl.`Input Quantity` AS DOUBLE)) * CAST(rl.`Run Time` AS DOUBLE) AS total_run_time
# MAGIC 
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Prod Order Routing Line` rl
# MAGIC     LEFT JOIN prod_line_qty plq
# MAGIC         ON rl.`Prod. Order No.` = plq.prod_order_no
# MAGIC         AND rl.`Routing Reference No.` = plq.prod_order_line_no
# MAGIC     WHERE rl.`Type` = 'Machine Center'
# MAGIC ),
# MAGIC 
# MAGIC S_cast AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         TO_TIMESTAMP(created_on) AS created_on_ts,
# MAGIC         TO_TIMESTAMP(modified_on) AS modified_on_ts,
# MAGIC         TO_DATE(TO_TIMESTAMP(created_on)) AS created_date,
# MAGIC         GREATEST(
# MAGIC             TO_TIMESTAMP(created_on),
# MAGIC             TO_TIMESTAMP(modified_on)
# MAGIC         ) AS change_ts
# MAGIC     FROM Silver_Production_Lakehouse.prod.silver_prod_order_status
# MAGIC ),
# MAGIC 
# MAGIC C_slim AS (
# MAGIC     SELECT
# MAGIC         email_address AS c_email,
# MAGIC         prod_line,
# MAGIC         cell_line,
# MAGIC         sub_department
# MAGIC     FROM Silver_Production_Lakehouse.prod.silver_cell_list
# MAGIC ),
# MAGIC 
# MAGIC joined AS (
# MAGIC     SELECT
# MAGIC         S2.created_date,
# MAGIC         S2.created_on_ts,
# MAGIC         S2.modified_on_ts,
# MAGIC         S2.prod_order_no,
# MAGIC         S2.prod_order_line_no,
# MAGIC         S2.prod_order_status,
# MAGIC         S2.type_name,
# MAGIC         S2.operation_no,
# MAGIC         S2.user_id,
# MAGIC         S2.rfid_transaction_name,
# MAGIC         S2.machine_center_no,
# MAGIC         S2.employee_no,
# MAGIC         S2.antenna_id,
# MAGIC         S2.item_no,
# MAGIC         S2.quantity,
# MAGIC         S2.remaining_quantity,
# MAGIC         S2.sales_order_no,
# MAGIC 
# MAGIC         C2.prod_line,
# MAGIC         C2.cell_line,
# MAGIC         C2.sub_department,
# MAGIC 
# MAGIC         M.machine_employee_mapping,
# MAGIC 
# MAGIC         R.input_quantity,
# MAGIC         R.run_time,
# MAGIC         R.total_run_time,
# MAGIC 
# MAGIC         S2.change_ts
# MAGIC 
# MAGIC     FROM S_cast S2
# MAGIC     LEFT JOIN C_slim C2
# MAGIC         ON S2.user_id = C2.c_email
# MAGIC     LEFT JOIN M_dedup M
# MAGIC         ON S2.machine_center_no = M.machine_center_no
# MAGIC     LEFT JOIN routing_calc R
# MAGIC         ON S2.prod_order_no = R.prod_order_no
# MAGIC         AND S2.prod_order_line_no = R.prod_order_line_no
# MAGIC         AND CAST(S2.operation_no AS STRING) = CAST(R.operation_no AS STRING)
# MAGIC ),
# MAGIC 
# MAGIC final_df AS (
# MAGIC     SELECT
# MAGIC         created_date,
# MAGIC         created_on_ts AS created_on,
# MAGIC         modified_on_ts AS modified_on,
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no,
# MAGIC         prod_order_status,
# MAGIC         type_name,
# MAGIC         operation_no,
# MAGIC         user_id,
# MAGIC         rfid_transaction_name,
# MAGIC         machine_center_no,
# MAGIC         employee_no,
# MAGIC         antenna_id,
# MAGIC         item_no,
# MAGIC         quantity,
# MAGIC         remaining_quantity,
# MAGIC         sales_order_no,
# MAGIC 
# MAGIC         prod_line,
# MAGIC         cell_line,
# MAGIC         sub_department,
# MAGIC         machine_employee_mapping AS m_group,
# MAGIC 
# MAGIC         input_quantity,
# MAGIC         run_time,
# MAGIC         total_run_time,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN TRIM(rfid_transaction_name) IS NOT NULL
# MAGIC              AND TRIM(rfid_transaction_name) <> ''
# MAGIC             THEN 1 ELSE 0
# MAGIC         END AS is_rfid_scanned,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN TRIM(rfid_transaction_name) IS NOT NULL
# MAGIC              AND TRIM(rfid_transaction_name) <> ''
# MAGIC             THEN 'Scanned'
# MAGIC             ELSE 'Not scanned'
# MAGIC         END AS scan_status,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN type_name LIKE '%Out location%' THEN 'OUT_LOC'
# MAGIC             WHEN type_name LIKE '%In location%' THEN 'IN_LOC'
# MAGIC             WHEN type_name LIKE '%To employee%' THEN 'TO_EMP'
# MAGIC             WHEN type_name LIKE '%From employee%' THEN 'FROM_EMP'
# MAGIC             ELSE 'OTHER'
# MAGIC         END AS move_direction,
# MAGIC 
# MAGIC         TO_DATE(DATE_TRUNC('month', created_on)) AS month_start,
# MAGIC         DATE_FORMAT(created_on, 'EEEE') AS weekday_name,
# MAGIC         HOUR(created_on) AS created_hour,
# MAGIC         CAST(operation_no AS INT) AS operation_no_int,
# MAGIC 
# MAGIC         change_ts
# MAGIC     FROM joined
# MAGIC ),
# MAGIC 
# MAGIC with_row_id AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         SHA2(
# MAGIC             CONCAT_WS(
# MAGIC                 '||',
# MAGIC                 COALESCE(CAST(prod_order_no AS STRING), ''),
# MAGIC                 COALESCE(CAST(prod_order_line_no AS STRING), ''),
# MAGIC                 COALESCE(CAST(operation_no AS STRING), ''),
# MAGIC                 COALESCE(CAST(user_id AS STRING), ''),
# MAGIC                 COALESCE(CAST(machine_center_no AS STRING), ''),
# MAGIC                 COALESCE(CAST(employee_no AS STRING), ''),
# MAGIC                 COALESCE(CAST(antenna_id AS STRING), ''),
# MAGIC                 COALESCE(CAST(item_no AS STRING), ''),
# MAGIC                 COALESCE(CAST(type_name AS STRING), ''),
# MAGIC                 COALESCE(CAST(created_on AS STRING), '')
# MAGIC             ),
# MAGIC             256
# MAGIC         ) AS row_id
# MAGIC     FROM final_df
# MAGIC ),
# MAGIC 
# MAGIC dedup AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC                ROW_NUMBER() OVER (
# MAGIC                    PARTITION BY row_id
# MAGIC                    ORDER BY
# MAGIC                        CAST(change_ts AS TIMESTAMP) DESC NULLS LAST,
# MAGIC                        CAST(modified_on AS TIMESTAMP) DESC NULLS LAST
# MAGIC                ) AS rn
# MAGIC         FROM with_row_id
# MAGIC     ) x
# MAGIC     WHERE rn = 1
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     created_date,
# MAGIC     created_on,
# MAGIC     modified_on,
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     prod_order_status,
# MAGIC     type_name,
# MAGIC     operation_no,
# MAGIC     user_id,
# MAGIC     rfid_transaction_name,
# MAGIC     machine_center_no,
# MAGIC     employee_no,
# MAGIC     antenna_id,
# MAGIC     item_no,
# MAGIC     quantity,
# MAGIC     remaining_quantity,
# MAGIC     sales_order_no,
# MAGIC     prod_line,
# MAGIC     cell_line,
# MAGIC     sub_department,
# MAGIC     m_group,
# MAGIC 
# MAGIC     input_quantity,
# MAGIC     run_time,
# MAGIC     total_run_time,
# MAGIC 
# MAGIC     is_rfid_scanned,
# MAGIC     scan_status,
# MAGIC     move_direction,
# MAGIC     month_start,
# MAGIC     weekday_name,
# MAGIC     created_hour,
# MAGIC     operation_no_int,
# MAGIC     change_ts,
# MAGIC     row_id
# MAGIC 
# MAGIC FROM dedup;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# # Gold Production Routing Group

# MARKDOWN ********************

# ## All Silver

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_production_routing_group
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC WITH t_raw AS (
# MAGIC     SELECT
# MAGIC         `Routing No.`           AS item_no,
# MAGIC         `Routing Reference No.` AS prod_order_line_no,
# MAGIC         `Operation No.`         AS operation_no,
# MAGIC         `Type`                  AS operation_type,
# MAGIC         `No.`                   AS routing_no,
# MAGIC         `Run Time`              AS run_time,
# MAGIC         `Status`                AS prod_order_status,
# MAGIC         `Prod. Order No.`       AS prod_order_no,
# MAGIC         `SystemCreatedAt`       AS created_on,
# MAGIC         `SystemModifiedAt`      AS modified_on
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Prod Order Routing Line`
# MAGIC ),
# MAGIC 
# MAGIC t_ts AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         TO_TIMESTAMP(created_on)  AS created_on_ts,
# MAGIC         TO_TIMESTAMP(modified_on) AS modified_on_ts,
# MAGIC         GREATEST(
# MAGIC             TO_TIMESTAMP(created_on),
# MAGIC             TO_TIMESTAMP(modified_on)
# MAGIC         ) AS change_ts
# MAGIC     FROM t_raw
# MAGIC ),
# MAGIC 
# MAGIC src AS (
# MAGIC     SELECT
# MAGIC         created_on,
# MAGIC         modified_on,
# MAGIC         prod_order_no,
# MAGIC         prod_order_status,
# MAGIC         prod_order_line_no,
# MAGIC         item_no,
# MAGIC         operation_no,
# MAGIC         operation_type,
# MAGIC         routing_no,
# MAGIC         run_time,
# MAGIC         change_ts,
# MAGIC         CASE
# MAGIC             WHEN operation_no IS NULL THEN NULL
# MAGIC             ELSE CAST(SPLIT(operation_no, '\\.')[0] AS INT)
# MAGIC         END AS operation_group
# MAGIC     FROM t_ts
# MAGIC ),
# MAGIC 
# MAGIC calc AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CASE
# MAGIC             WHEN operation_type = 'Machine Center' THEN routing_no
# MAGIC             ELSE NULL
# MAGIC         END AS routing_no_machine_center,
# MAGIC         MAX(
# MAGIC             CASE
# MAGIC                 WHEN operation_type = 'Work Center' THEN routing_no
# MAGIC                 ELSE NULL
# MAGIC             END
# MAGIC         ) OVER (
# MAGIC             PARTITION BY prod_order_no, prod_order_line_no, operation_group
# MAGIC         ) AS routing_no_work_center
# MAGIC     FROM src
# MAGIC ),
# MAGIC 
# MAGIC final_df AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CONCAT(
# MAGIC             COALESCE(CAST(prod_order_no AS STRING), ''),
# MAGIC             COALESCE(CAST(prod_order_line_no AS STRING), ''),
# MAGIC             COALESCE(CAST(operation_group AS STRING), '')
# MAGIC         ) AS rol
# MAGIC     FROM calc
# MAGIC     WHERE operation_type = 'Machine Center'
# MAGIC        OR (operation_group = 9 AND operation_no = '009')
# MAGIC ),
# MAGIC 
# MAGIC final_with_id AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         SHA2(
# MAGIC             CONCAT_WS(
# MAGIC                 '||',
# MAGIC                 COALESCE(CAST(prod_order_no AS STRING), ''),
# MAGIC                 COALESCE(CAST(prod_order_line_no AS STRING), ''),
# MAGIC                 COALESCE(CAST(operation_no AS STRING), ''),
# MAGIC                 COALESCE(CAST(routing_no AS STRING), ''),
# MAGIC                 COALESCE(CAST(operation_type AS STRING), '')
# MAGIC             ),
# MAGIC             256
# MAGIC         ) AS row_id
# MAGIC     FROM final_df
# MAGIC ),
# MAGIC 
# MAGIC final_dedup AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY row_id
# MAGIC                 ORDER BY change_ts DESC NULLS LAST
# MAGIC             ) AS rn
# MAGIC         FROM final_with_id
# MAGIC     ) x
# MAGIC     WHERE rn = 1
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     item_no,
# MAGIC     prod_order_line_no,
# MAGIC     operation_no,
# MAGIC     operation_type,
# MAGIC     routing_no,
# MAGIC     run_time,
# MAGIC     prod_order_status,
# MAGIC     prod_order_no,
# MAGIC     TO_TIMESTAMP(created_on)  AS created_on,
# MAGIC     TO_TIMESTAMP(modified_on) AS modified_on,
# MAGIC     change_ts,
# MAGIC     operation_group,
# MAGIC     routing_no_machine_center,
# MAGIC     routing_no_work_center,
# MAGIC     rol,
# MAGIC     row_id
# MAGIC FROM final_dedup;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Standard vs Actual Time

# MARKDOWN ********************

# ## Gold Production Status Cycle Time + Gold Production Routing Group

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_standard_vs_actual_time
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC WITH stg_df AS (
# MAGIC     SELECT
# MAGIC         C.prod_order_no,
# MAGIC         C.prod_order_line_no,
# MAGIC         C.op_major,
# MAGIC         C.prod_line,
# MAGIC         C.cell_line,
# MAGIC         C.operation,
# MAGIC         C.routing_no_machine_center,
# MAGIC         C.in_created,
# MAGIC         C.out_created,
# MAGIC         C.to_created,
# MAGIC         C.from_created,
# MAGIC         C.dead_to_created,
# MAGIC         C.station_time,
# MAGIC         C.operation_time,
# MAGIC         C.dead_time,
# MAGIC         C.quantity,
# MAGIC         C.routing_no_work_center,
# MAGIC         C.item_no,
# MAGIC         R.run_time
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_production_status_cycle_time C
# MAGIC     INNER JOIN Gold_Production_Lakehouse.prod.gold_production_routing_group R
# MAGIC         ON C.prod_order_no = R.prod_order_no
# MAGIC        AND C.prod_order_line_no = R.prod_order_line_no
# MAGIC        AND C.op_major = R.operation_group
# MAGIC ),
# MAGIC 
# MAGIC stg_dedup AS (
# MAGIC     SELECT
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no,
# MAGIC         op_major,
# MAGIC         prod_line,
# MAGIC         cell_line,
# MAGIC         operation,
# MAGIC         routing_no_machine_center,
# MAGIC         in_created,
# MAGIC         out_created,
# MAGIC         to_created,
# MAGIC         from_created,
# MAGIC         dead_to_created,
# MAGIC         station_time,
# MAGIC         operation_time,
# MAGIC         dead_time,
# MAGIC         quantity,
# MAGIC         routing_no_work_center,
# MAGIC         item_no,
# MAGIC         run_time
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY
# MAGIC                     prod_order_no,
# MAGIC                     prod_order_line_no,
# MAGIC                     op_major,
# MAGIC                     operation
# MAGIC                 ORDER BY in_created DESC NULLS LAST
# MAGIC             ) AS rn
# MAGIC         FROM stg_df
# MAGIC     ) x
# MAGIC     WHERE rn = 1
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     op_major,
# MAGIC     prod_line,
# MAGIC     cell_line,
# MAGIC     operation,
# MAGIC     routing_no_machine_center,
# MAGIC     in_created,
# MAGIC     out_created,
# MAGIC     to_created,
# MAGIC     from_created,
# MAGIC     dead_to_created,
# MAGIC     station_time,
# MAGIC     operation_time,
# MAGIC     dead_time,
# MAGIC     quantity,
# MAGIC     routing_no_work_center,
# MAGIC     item_no,
# MAGIC     run_time
# MAGIC FROM stg_dedup;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Sales Order Sum

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_sales_order_sum
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     CusAbbr,
# MAGIC     ItemFG,
# MAGIC     so_abbr,
# MAGIC     SOI,
# MAGIC     StatusSO,
# MAGIC     so_type,
# MAGIC     requested_week,
# MAGIC     SUM(OutstandingQty) AS OutstandingQty,
# MAGIC     SUM(Total_QTY)      AS Total_QTY
# MAGIC FROM Gold_Production_Lakehouse.prod.gold_sales_order
# MAGIC GROUP BY
# MAGIC     CusAbbr,
# MAGIC     ItemFG,
# MAGIC     so_abbr,
# MAGIC     SOI,
# MAGIC     so_type,
# MAGIC     requested_week,
# MAGIC     StatusSO;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- ================================================================
# MAGIC -- Job: Gold_Production_Lakehouse.prod.gold_production_routing_group
# MAGIC -- Spark SQL version
# MAGIC -- ================================================================
# MAGIC 
# MAGIC -- ------------------------------------------------
# MAGIC -- 1) Source remap
# MAGIC -- ------------------------------------------------
# MAGIC CREATE OR REPLACE TEMP VIEW t_raw AS
# MAGIC SELECT
# MAGIC     `Routing No.`            AS item_no,
# MAGIC     `Routing Reference No.`  AS prod_order_line_no,
# MAGIC     `Operation No.`          AS operation_no,
# MAGIC     `Next Operation No.`     AS next_operation_no,
# MAGIC     `Previous Operation No.` AS previous_operation_no,
# MAGIC     `Type`                   AS operation_type,
# MAGIC     `No.`                    AS routing_no,
# MAGIC     `Run Time`               AS run_time,
# MAGIC     `Status`                 AS prod_order_status,
# MAGIC     `Prod. Order No.`        AS prod_order_no,
# MAGIC     `Routing Link Code`      AS routing_link_code,
# MAGIC     `Routing Status`         AS routing_status,
# MAGIC     `Starting Date-Time`     AS starting_date_time,
# MAGIC     `Ending Date-Time`       AS ending_date_time,
# MAGIC     `Location Code`          AS location_code,
# MAGIC     `SystemCreatedAt`        AS created_on,
# MAGIC     `SystemModifiedAt`       AS modified_on
# MAGIC FROM Silver_BC_Lakehouse.bc.`Prod Order Routing Line`;
# MAGIC 
# MAGIC -- ------------------------------------------------
# MAGIC -- 2) Add timestamps
# MAGIC -- ------------------------------------------------
# MAGIC CREATE OR REPLACE TEMP VIEW t_ts AS
# MAGIC SELECT
# MAGIC     *,
# MAGIC     to_timestamp(created_on)  AS created_on_ts,
# MAGIC     to_timestamp(modified_on) AS modified_on_ts,
# MAGIC     greatest(to_timestamp(created_on), to_timestamp(modified_on)) AS change_ts
# MAGIC FROM t_raw;
# MAGIC 
# MAGIC -- ------------------------------------------------
# MAGIC -- 3) Main transform
# MAGIC --    หมายเหตุ:
# MAGIC --    ถ้าจะ filter incremental ให้ใส่เงื่อนไขเพิ่มใน WHERE ของ src
# MAGIC --    เช่น:
# MAGIC --    WHERE to_date(change_ts) BETWEEN DATE('2025-01-01') AND DATE('2025-12-31')
# MAGIC -- ------------------------------------------------
# MAGIC CREATE OR REPLACE TEMP VIEW final_dedup AS
# MAGIC WITH src AS (
# MAGIC     SELECT
# MAGIC         created_on,
# MAGIC         modified_on,
# MAGIC         prod_order_no,
# MAGIC         prod_order_status,
# MAGIC         prod_order_line_no,
# MAGIC         item_no,
# MAGIC         operation_no,
# MAGIC         operation_type,
# MAGIC         routing_no,
# MAGIC         run_time,
# MAGIC         change_ts,
# MAGIC         CASE
# MAGIC             WHEN operation_no IS NULL THEN NULL
# MAGIC             ELSE CAST(split(operation_no, '\\.')[0] AS INT)
# MAGIC         END AS operation_group
# MAGIC     FROM t_ts
# MAGIC ),
# MAGIC calc AS (
# MAGIC     SELECT
# MAGIC         src.*,
# MAGIC         CASE
# MAGIC             WHEN operation_type = 'Machine Center' THEN routing_no
# MAGIC             ELSE NULL
# MAGIC         END AS routing_no_machine_center,
# MAGIC         MAX(
# MAGIC             CASE
# MAGIC                 WHEN operation_type = 'Work Center' THEN routing_no
# MAGIC                 ELSE NULL
# MAGIC             END
# MAGIC         ) OVER (
# MAGIC             PARTITION BY prod_order_no, prod_order_line_no, operation_group
# MAGIC         ) AS routing_no_work_center
# MAGIC     FROM src
# MAGIC ),
# MAGIC final_df AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CONCAT(
# MAGIC             COALESCE(CAST(prod_order_no AS STRING), ''),
# MAGIC             COALESCE(CAST(prod_order_line_no AS STRING), ''),
# MAGIC             COALESCE(CAST(operation_group AS STRING), '')
# MAGIC         ) AS rol
# MAGIC     FROM calc
# MAGIC     WHERE operation_type = 'Machine Center'
# MAGIC        OR (operation_group = 9 AND operation_no = '009')
# MAGIC ),
# MAGIC final_with_id AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         sha2(
# MAGIC             concat_ws(
# MAGIC                 '||',
# MAGIC                 COALESCE(CAST(prod_order_no AS STRING), ''),
# MAGIC                 COALESCE(CAST(prod_order_line_no AS STRING), ''),
# MAGIC                 COALESCE(CAST(operation_no AS STRING), ''),
# MAGIC                 COALESCE(CAST(routing_no AS STRING), ''),
# MAGIC                 COALESCE(CAST(operation_type AS STRING), '')
# MAGIC             ),
# MAGIC             256
# MAGIC         ) AS row_id
# MAGIC     FROM final_df
# MAGIC ),
# MAGIC ranked AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY row_id
# MAGIC             ORDER BY change_ts DESC NULLS LAST
# MAGIC         ) AS rn
# MAGIC     FROM final_with_id
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM ranked
# MAGIC WHERE rn = 1;
# MAGIC 
# MAGIC -- ------------------------------------------------
# MAGIC -- 4) MERGE into target
# MAGIC -- ------------------------------------------------
# MAGIC MERGE INTO Gold_Production_Lakehouse.prod.gold_production_routing_group AS tgt
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         created_on,
# MAGIC         modified_on,
# MAGIC         prod_order_no,
# MAGIC         prod_order_status,
# MAGIC         prod_order_line_no,
# MAGIC         item_no,
# MAGIC         operation_no,
# MAGIC         operation_type,
# MAGIC         routing_no,
# MAGIC         run_time,
# MAGIC         change_ts,
# MAGIC         operation_group,
# MAGIC         routing_no_machine_center,
# MAGIC         routing_no_work_center,
# MAGIC         rol,
# MAGIC         row_id
# MAGIC     FROM final_dedup
# MAGIC ) AS src
# MAGIC ON tgt.row_id <=> src.row_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
