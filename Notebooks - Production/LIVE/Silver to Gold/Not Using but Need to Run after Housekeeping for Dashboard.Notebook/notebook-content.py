# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3a130b81-98ec-4fd4-a404-95edc1f0ef1e",
# META       "default_lakehouse_name": "Silver_Inventory_Lakehouse",
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
# META           "id": "3a130b81-98ec-4fd4-a404-95edc1f0ef1e"
# META         },
# META         {
# META           "id": "1d620310-5acc-4534-93f9-f52f082a1887"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Inv Output

# CELL ********************

# ==========================================================
# Job: Gold_Inventory_Lakehouse.prod.gold_inv_output  (incremental)
# Mirrors your SQL + adds P.prod_order_quantity
# ==========================================================

from pyspark.sql import functions as F, Window as W
from delta.tables import DeltaTable
from datetime import datetime, date

# ---------- Namespaces ----------
CATALOG = "ENG-Silver-and-Gold"
DB      = "Gold_Production_Lakehouse"
SCHEMA  = "prod"
TARGET  = f"{DB}.{SCHEMA}.gold_inv_output"

# ---------- Sources ----------
IL_SRC  = "Silver_Inventory_Lakehouse.inv.silver_item_ledger"                     # I
POH_SRC = "Silver_Production_Lakehouse.prod.silver_prod_order_header"            # P
SOH_SRC = "Silver_Customer_Exp_Lakehouse.cx.silver_sales_header"                 # S (sales header)

# OLD:
# RL_SRC  = "Silver_Production_Lakehouse.prod.silver_prod_routing_line"          # R
# NEW: mirror Prod Order Routing Line
RL_SRC  = "Silver_BC_Lakehouse.bc.`Prod Order Routing Line`"                     # R (mirror)

ST_SRC  = "Silver_Production_Lakehouse.prod.silver_prod_order_status"            # ST
CL_SRC  = "Silver_Production_Lakehouse.prod.silver_cell_list"                    # CL

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
    if "posting_date" in df.columns:
        return df.agg(F.max(F.to_date("posting_date")).alias("mx")).collect()[0]["mx"]
    return None

def ensure_namespace(catalog: str, database: str, schema: str):
    spark.sql(f"USE CATALOG `{catalog}`")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{database}`")
    spark.sql(f"CREATE SCHEMA  IF NOT EXISTS `{database}`.`{schema}`")


since_date = parse_date_or_none(since_param)
until_date = parse_date_or_none(until_param) or date.today()

if not full_reload and since_date is None and table_exists(TARGET):
    wm = pick_watermark_from_target(TARGET)
    if wm is not None:
        since_date = wm
if full_reload or since_date is None:
    since_date = date(1900, 1, 1)

print(f"Incremental window (date from Item Ledger): {since_date} -> {until_date}")

# ---------- Load sources ----------
I  = spark.table(IL_SRC).alias("I")
P  = spark.table(POH_SRC).alias("P")
SH = spark.table(SOH_SRC).alias("SH")

# Routing from BC mirror, mapped to old schema:
#   [Prod. Order No.]       -> prod_order_no
#   [Routing Reference No.] -> prod_order_line_no
#   [Operation No.]         -> operation_no
#   [Run Time]              -> run_time
R_raw = spark.table(RL_SRC)
R = (
    R_raw
        .withColumnRenamed("Prod. Order No.", "prod_order_no")
        .withColumnRenamed("Routing Reference No.", "prod_order_line_no")
        .withColumnRenamed("Operation No.", "operation_no")
        .withColumnRenamed("Run Time", "run_time")
).alias("R")

ST = spark.table(ST_SRC).alias("ST")
CL = spark.table(CL_SRC).alias("CL")

# ---------- output_fg (FIN-GOODS outputs) ----------
il_date = F.to_date(F.coalesce(F.col("I.posting_date"), F.col("I.created_on")))
output_fg = (
    I.filter((F.col("I.entry_type") == F.lit("Output")) &
             (F.col("I.entry_type_item_location") == F.lit("FIN-GOODS")) &
             il_date.isNotNull() &
             (il_date >= F.lit(since_date)) &
             (il_date <= F.lit(until_date)))
     .join(P, F.col("I.document_no") == F.col("P.prod_order_no"), "left")
     .join(SH, F.col("P.sales_order_no") == F.col("SH.sales_order_no"), "left")
     .selectExpr(
         "SH.sales_order_requested_date",
         "I.posting_date",
         "SH.customer_no",
         "SH.customer_name",
         "P.sales_order_no",
         "P.prod_order_quantity",   # <--- added here
         "I.document_no",
         "I.order_no",
         "I.order_lineno",
         "I.item_no",
         "I.item_description",
         "I.entry_type_item_quantity"
     )
     .distinct()
     .alias("O")
)

# ---------- stdtime (routing run_time with status & cell line) ----------
stdtime = (
    R.join(
           ST,
           (F.col("R.prod_order_no") == F.col("ST.prod_order_no")) &
           (F.col("R.prod_order_line_no") == F.col("ST.prod_order_line_no")) &
           (F.col("R.operation_no") == F.col("ST.operation_no")),
           "left"
     )
     .join(CL, F.col("ST.user_id") == F.col("CL.email_address"), "left")
     .filter(F.col("ST.type_name").isin("In location in", "To employee"))
     .select(
         F.col("R.prod_order_no").alias("prod_order_no"),
         F.col("R.prod_order_line_no").alias("prod_order_line_no"),
         F.col("ST.machine_center_no").alias("machine_center_no"),
         F.col("CL.cell_line").alias("cell_line"),
         F.col("CL.prod_line").alias("prod_line"),
         F.when(F.col("ST.user_id") == F.lit("outsource@ennovie.com"), F.lit(0.0))
          .otherwise(F.coalesce(F.col("R.run_time").cast("double"), F.lit(0.0))).alias("run_time_adj")
     )
     .groupBy("prod_order_no", "prod_order_line_no", "machine_center_no", "cell_line", "prod_line")
     .agg(F.sum("run_time_adj").alias("total_run_time"))
     .alias("S")
)

# ---------- Final aggregation ----------
final_df = (
    output_fg.alias("O")
    .join(
        stdtime.alias("S"),
        (F.col("O.document_no") == F.col("S.prod_order_no")) &
        (F.col("O.order_lineno") == F.col("S.prod_order_line_no")),
        "left"
    )
    .groupBy(
        F.col("S.cell_line"),
        F.col("S.prod_line"),
        F.col("S.machine_center_no"),
        F.col("O.document_no"),
        F.col("O.order_no"),
        F.col("O.order_lineno"),
        F.col("O.item_no"),
        F.col("O.item_description"),
        F.col("O.sales_order_requested_date"),
        F.col("O.posting_date"),
        F.col("O.customer_no"),
        F.col("O.customer_name"),
        F.col("O.sales_order_no"),
        F.col("O.prod_order_quantity"),
        F.coalesce(F.col("S.total_run_time"), F.lit(0.0)).alias("grp_total_run_time")
    )
    .agg(F.sum("O.entry_type_item_quantity").alias("total_qty"))
    .select(
        F.col("S.cell_line").alias("cell_line"),
        F.col("S.prod_line").alias("prod_line"),
        F.col("S.machine_center_no").alias("machine_center_no"),
        F.col("O.document_no").alias("document_no"),
        F.col("O.order_no").alias("order_no"),
        F.col("O.order_lineno").alias("order_lineno"),
        F.col("O.item_no").alias("item_no"),
        F.col("O.item_description").alias("item_description"),
        F.col("O.prod_order_quantity").alias("prod_order_quantity"),
        F.col("total_qty").cast("double").alias("total_qty"),
        F.col("O.sales_order_requested_date").alias("sales_order_requested_date"),
        F.col("O.posting_date").alias("posting_date"),
        F.col("O.customer_no").alias("customer_no"),
        F.col("O.customer_name").alias("customer_name"),
        F.col("O.sales_order_no").alias("sales_order_no"),
        F.col("grp_total_run_time").alias("total_run_time"),
        (F.col("total_qty") * F.col("grp_total_run_time")).cast("double").alias("total_runtime_qty"),
    )
)

# ---------- Stabilize & MERGE ----------
row_id_cols = [
    "document_no","order_lineno","machine_center_no","item_no","posting_date"
]
final_df = (
    final_df
    .groupBy(*row_id_cols,
             "cell_line","prod_line","order_no","item_description",
             "sales_order_requested_date","customer_no","customer_name",
             "sales_order_no","prod_order_quantity")
    .agg(
        F.sum("total_qty").alias("total_qty"),
        F.max("total_run_time").alias("total_run_time")
    )
    .withColumn(
        "total_runtime_qty",
        (F.col("total_qty") * F.coalesce(F.col("total_run_time"), F.lit(0.0))).cast("double")
    )
)

final_df = final_df.withColumn(
    "row_id",
    F.sha2(
        F.concat_ws(
            "||",
            *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in row_id_cols]
        ),
        256
    )
)

# ---------- Write ----------
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

# # Prod Planned vs Actual Qty (Temp)

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
