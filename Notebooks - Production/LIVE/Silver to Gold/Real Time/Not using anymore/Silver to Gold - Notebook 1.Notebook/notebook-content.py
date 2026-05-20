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
# META           "id": "e248ea90-8431-4df2-9f29-87866bf9dd5a"
# META         },
# META         {
# META           "id": "ad99fdfa-85b1-4480-9f7f-2640bfd65f24"
# META         },
# META         {
# META           "id": "6fa25cdd-36f9-4f2e-9817-c1f4d946d4d9"
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

# # Production Order

# CELL ********************

# ==============================================================
# GOLD: gold_production_order — Incremental Loader from Silver
# ==============================================================

from pyspark.sql import functions as F, Window

# -------- CONFIG --------
S_ORDER = "Silver_Production_Lakehouse.prod.silver_prod_order_header"
S_LINE  = "Silver_Production_Lakehouse.prod.silver_prod_order_line"
TARGET  = "prod.gold_production_order"

KEYS   = ["prod_order_no", "prod_order_line_no"]
MODCOL = "_modified_any"
LOOKBACK_MIN = 90  # overlap for incremental window


# -------- helpers --------
def table_exists(name: str):
    try:
        return spark.catalog.tableExists(name)
    except:
        return False

def get_last_ts(name: str, col: str):
    if not table_exists(name):
        return None
    r = spark.table(name).select(F.max(F.col(col))).first()
    return r[0] if r else None

def create_like(name: str, df):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {name.rsplit('.',1)[0]}")
    df.limit(0).write.format("delta").mode("overwrite").saveAsTable(name)

def maintain(name: str, zcols=None, vacuum_hours=168):
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


# -------- 1) read Silver --------
po = spark.table(S_ORDER)
pl = spark.table(S_LINE)

print(f"po={po.count():,} pl={pl.count():,}")

# -------- 2) join + compute fields --------
joined = (
    po.alias("po")
      .join(pl.alias("pl"), F.col("po.prod_order_no")==F.col("pl.prod_order_no"), "left")
      .select(
          F.col("po.prod_order_status"),
          F.col("po.prod_order_no"),
          F.col("pl.prod_order_line_no"),
          F.col("po.sales_order_no"),
          F.col("po.sales_order_line_no"),
          F.col("po.FG_item_no"),
          F.col("pl.item_no").alias("prod_item_line"),
          F.col("po.item_routing_no"),
          F.col("po.prod_order_quantity"),
          F.col("po.prod_order_starting_date_time"),
          F.col("po.prod_order_ending_date_time"),
          F.col("po.prod_order_finished_date"),
          F.col("po.prod_order_due_date"),
          F.weekofyear(F.col("po.prod_order_due_date")).alias("commit_week"),
          F.col("po.ref_prod_order"),
          F.col("po.ref_item"),
          F.col("pl.prod_line_due_date"),
          F.col("pl.prod_line_start_date"),
          F.col("pl.prod_line_end_date"),
          F.col("pl.prod_line_quantity"),
          F.col("pl.prod_line_finished_quantity"),
          F.col("pl.prod_line_remaining_quantity"),
          F.col("pl.item_location"),
          F.concat(F.col("po.sales_order_no").cast("string"),
                   F.col("po.sales_order_line_no").cast("string")).alias("SOL"),
          F.concat(F.col("po.prod_order_no").cast("string"),
                   F.col("pl.prod_order_line_no").cast("string")).alias("POL"),
          # Due In (days diff)
          F.when(F.col("po.prod_order_due_date").isNull(), F.lit(None))
           .when(F.datediff(F.current_date(), F.col("po.prod_order_due_date")) < 0,
                 F.concat(F.lit("Overdue "),
                          F.abs(F.datediff(F.current_date(), F.col("po.prod_order_due_date"))).cast("string"),
                          F.lit("d")))
           .otherwise(F.concat(F.lit("Due "),
                               F.datediff(F.col("po.prod_order_due_date"), F.current_date()).cast("string"),
                               F.lit("d"))).alias("due_in"),
          # Due Status
          F.when(F.col("po.prod_order_due_date").isNull(), F.lit(None))
           .when(F.datediff(F.current_date(), F.col("po.prod_order_due_date")) < 0, F.lit("Overdue"))
           .when(F.datediff(F.current_date(), F.col("po.prod_order_due_date")) <= 3, F.lit("At risk"))
           .otherwise(F.lit("On time")).alias("due_status"),
          # watermark sources
          F.col("po.modified_on").alias("po_modified_on"),
          F.col("pl.modified_on").alias("pl_modified_on")
      )
)

# unified modified
joined = joined.withColumn(MODCOL, F.greatest(F.col("po_modified_on"), F.col("pl_modified_on")))

# -------- 3) incremental filter --------
last_ts = get_last_ts(TARGET, MODCOL)
staged = joined
if last_ts is not None:
    staged = staged.filter(F.col(MODCOL) >= (F.lit(last_ts).cast("timestamp") - F.expr(f"INTERVAL {LOOKBACK_MIN} MINUTES")))
print("Rows staged:", staged.count())

# -------- 4) create if missing --------
if not table_exists(TARGET):
    create_like(TARGET, joined.drop("po_modified_on","pl_modified_on"))
    print(f"Created {TARGET}")

# -------- 5) dedupe before MERGE --------
w_keys = Window.partitionBy(*KEYS).orderBy(F.col(MODCOL).desc_nulls_last())
staged_dedup = (
    staged.withColumn("_rn", F.row_number().over(w_keys))
          .filter("_rn = 1")
          .drop("_rn","po_modified_on","pl_modified_on")
)

# -------- 6) merge --------
tgt = spark.table(TARGET)
target_cols = tgt.columns

on_expr  = " AND ".join([f"(t.{k} <=> s.{k})" for k in KEYS])
set_expr = ", ".join([f"t.{c}=s.{c}" for c in target_cols if c not in set(KEYS)])

tmp = "stg_gold_production_order"
staged_dedup.createOrReplaceTempView(tmp)

spark.sql(f"""
    MERGE INTO {TARGET} t
    USING {tmp} s
    ON {on_expr}
    WHEN MATCHED THEN UPDATE SET {set_expr}
    WHEN NOT MATCHED THEN INSERT ({", ".join(target_cols)})
    VALUES ({", ".join([f"s.{c}" for c in target_cols])})
""")

# -------- 7) maintenance --------
maintain(TARGET, zcols=KEYS + [MODCOL])

# # -------- 8) verify --------
# spark.sql(f"SELECT COUNT(*) AS rows_in_gold FROM {TARGET}").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Sales Order

# CELL ********************

# ==============================================================
# GOLD: prod.gold_sales_order — Incremental Loader (no-space cols)
# ==============================================================

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

# ---------- CONFIG ----------
SH = "Silver_Customer_Exp_Lakehouse.cx.silver_sales_header"   # header
SL = "Silver_Customer_Exp_Lakehouse.cx.silver_sales_line"     # line
CU = "Silver_Customer_Exp_Lakehouse.cx.silver_customer"       # customers

TARGET = "prod.gold_sales_order"
KEYS   = ["SalesorderNo", "SalesLineNo", "StatusSO"]          # table grain
MODCOL = "_modified_any"                                       # unified modified ts
LOOKBACK_MIN = 90                                              # minutes overlap

# ---------- helpers ----------
def table_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False

def get_last_ts(name: str, col: str):
    if not table_exists(name):
        return None
    row = spark.table(name).select(F.max(F.col(col)).alias("wm")).first()
    return row["wm"] if row else None

def create_with_schema(name: str, df):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {name.rsplit('.',1)[0]}")
    (df.limit(0)
       .write
       .format("delta")
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

def build_watermark_expr(sh_src, sl_src, cu_src):
    """
    Return a Column expression: greatest(existing TIMESTAMP candidates) OR None.
    Only add columns that truly exist on the source schemas, cast to timestamp.
    """
    candidates = []

    def maybe_add(alias, raw):
        exists = (
            (alias == "sl" and raw in sl_src.columns) or
            (alias == "sh" and raw in sh_src.columns) or
            (alias == "c"  and raw in cu_src.columns)
        )
        if exists:
            candidates.append(F.to_timestamp(F.col(f"{alias}.{raw}")))

    # line first (most granular updates)
    for raw in ["SinkModifiedOn", "modified_on", "modifiedon", "_commit_timestamp", "load_ts", "created_on"]:
        maybe_add("sl", raw)

    # header next
    for raw in ["SinkModifiedOn", "modified_on", "modifiedon", "_commit_timestamp", "load_ts", "created_on"]:
        maybe_add("sh", raw)

    # customer last
    for raw in ["SinkModifiedOn", "modified_on", "modifiedon", "_commit_timestamp", "load_ts", "created_on"]:
        maybe_add("c", raw)

    if candidates:
        return F.greatest(*candidates)
    return None

# ---------- 1) read silver sources ----------
sh_src = spark.table(SH)  # keep unaliased for schema checks
sl_src = spark.table(SL)
cu_src = spark.table(CU)

sh = sh_src.alias("sh")
sl = sl_src.alias("sl")
cu = cu_src.alias("c")

# ---------- 2) join + filters ----------
status_ok = F.col("sh.sales_order_status").isin(
    "Open", "Released", "Pending Approval", "Pending Prepayment", "Closed"
)
type_ok = F.col("sh.sales_order_type").isin("Order")

joined = (
    sh.join(sl, F.col("sh.sales_order_no") == F.col("sl.sales_order_no"), how="left")
      .join(cu, F.col("c.customer_no") == F.col("sh.customer_no"),         how="left")
      .where(status_ok & type_ok)
)

# ---------- 3) robust watermark (TIMESTAMP-only into greatest) ----------
wm_expr = build_watermark_expr(sh_src, sl_src, cu_src)
# business-date fallback
wm_expr = F.coalesce(
    wm_expr if wm_expr is not None else F.lit(None).cast("timestamp"),
    F.to_timestamp(F.col("sh.sales_order_released_date")),
    F.to_timestamp(F.col("sh.sales_order_requested_date")),
    F.current_timestamp()
)

# ---------- 4) project (includes the new columns you requested) ----------
sales = joined.select(
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
    F.col("sl.sales_order_line_no").alias("SalesLineNo"),
    F.col("sl.item_no").alias("ItemFG"),
    F.col("sl.item_description").alias("item_description"),
    F.col("sl.item_quantity").alias("Total_QTY"),
    F.col("sl.sales_order_shipment_date").alias("LineShipmentDate"),
    F.col("sl.item_material").alias("TypeofFG"),             # original type-of-FG
    F.col("sl.item_material").alias("line_item_material"),   # explicit alias you asked for
    F.col("sl.item_reference").alias("item_reference"),
    F.col("sl.item_quantity_to_ship").alias("item_quantity_to_ship"),
    F.col("sl.item_quantity_shipped").alias("item_quantity_shipped"),
    F.col("sl.item_quantity_to_invoice").alias("item_quantity_to_invoice"),
    F.col("sl.item_quantity_invoiced").alias("item_quantity_invoiced"),
    F.col("sl.item_outstanding").alias("OutstandingQty"),

    # derived keys/labels
    F.concat(
        F.col("sh.sales_order_no").cast("string"),
        F.col("sl.sales_order_line_no").cast("string")
    ).alias("SOL"),
    F.concat(
        F.expr("left(sh.sales_order_no, 2)"),
        F.expr("right(sh.sales_order_no, 4)")
    ).alias("so_abbr"),
    F.expr("left(sh.sales_order_no, 2)").alias("so_type"),

    # unified modified
    F.to_timestamp(wm_expr).alias(MODCOL)
).dropDuplicates()

# ---------- 5) incremental window on MODCOL ----------
last_ts = get_last_ts(TARGET, MODCOL)
staged = sales
if last_ts is not None:
    staged = staged.filter(
        F.col(MODCOL) >= (F.lit(last_ts).cast("timestamp") - F.expr(f"INTERVAL {LOOKBACK_MIN} MINUTES"))
    )

# ---------- 6) create target if missing ----------
if not table_exists(TARGET):
    boot = (staged
            .withColumn("updated_at", F.col(MODCOL))
            .withColumn("load_ts", F.current_timestamp())
            .withColumn("source_system", F.lit("silver_sales"))
            .withColumn("row_hash", F.lit(None).cast("string")))
    create_with_schema(TARGET, boot)

# ---------- 7) dedupe by KEYS ----------
w_keys = Window.partitionBy(*KEYS).orderBy(
    F.col(MODCOL).desc_nulls_last(),
    F.col("ReqDate").desc_nulls_last(),
    F.col("sales_order_released_date").desc_nulls_last(),
    # deterministic tie-breaker
    F.sha2(F.concat_ws("§",
        F.coalesce(F.col("SOL").cast("string"), F.lit("")),
        F.coalesce(F.col("ItemFG").cast("string"), F.lit(""))
    ), 256).desc()
)
staged_dedup = (
    staged
      .withColumn("_rn", F.row_number().over(w_keys))
      .filter(F.col("_rn") == 1)
      .drop("_rn")
)

# ---------- 8) add system fields + row hash ----------
content_cols = [c for c in staged_dedup.columns]  # includes MODCOL
staged_h = (
    staged_dedup
      .withColumn("updated_at", F.col(MODCOL))
      .withColumn("load_ts", F.current_timestamp())
      .withColumn("source_system", F.lit("silver_sales"))
      .withColumn(
          "row_hash",
          F.sha2(F.concat_ws("§", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in content_cols]), 256)
      )
)

# ---------- 9) MERGE (Delta) ----------
if staged_h.rdd.isEmpty():
    print("No staged rows after lookback/dedupe. ✅")
else:
    tgt = DeltaTable.forName(spark, TARGET)
    on_clause = " AND ".join([f"t.{k} <=> s.{k}" for k in KEYS])

    (
        tgt.alias("t")
           .merge(staged_h.alias("s"), on_clause)
           .whenMatchedUpdate(condition="t.row_hash <> s.row_hash", set={
               # header/customer
               "CusNo":                         "s.CusNo",
               "CusName":                       "s.CusName",
               "CusAbbr":                       "s.CusAbbr",
               "StatusSO":                      "s.StatusSO",
               "ReqDate":                       "s.ReqDate",
               "PmDate":                        "s.PmDate",
               "CSNoted":                       "s.CSNoted",
               "CSteam":                        "s.CSteam",
               "sales_order_external_document": "s.sales_order_external_document",
               "sales_order_released_date":     "s.sales_order_released_date",
               "sales_order_document_date":     "s.sales_order_document_date",
               "requested_week":                "s.requested_week",

               # line
               "SalesLineNo":                   "s.SalesLineNo",
               "ItemFG":                        "s.ItemFG",
               "item_description":              "s.item_description",
               "Total_QTY":                     "s.Total_QTY",
               "LineShipmentDate":              "s.LineShipmentDate",
               "TypeofFG":                      "s.TypeofFG",
               "line_item_material":            "s.line_item_material",
               "item_reference":                "s.item_reference",
               "item_quantity_to_ship":         "s.item_quantity_to_ship",
               "item_quantity_shipped":         "s.item_quantity_shipped",
               "item_quantity_to_invoice":      "s.item_quantity_to_invoice",
               "item_quantity_invoiced":        "s.item_quantity_invoiced",
               "OutstandingQty":                "s.OutstandingQty",

               # derived / system
               "SOL":                           "s.SOL",
               "so_abbr":                       "s.so_abbr",
               "so_type":                       "s.so_type",
               f"{MODCOL}":                     f"s.{MODCOL}",
               "updated_at":                    "s.updated_at",
               "load_ts":                       "s.load_ts",
               "source_system":                 "s.source_system",
               "row_hash":                      "s.row_hash",
           })
           .whenNotMatchedInsert(values={
               "SalesorderNo":                  "s.SalesorderNo",
               "SalesLineNo":                   "s.SalesLineNo",
               "CusNo":                         "s.CusNo",
               "CusName":                       "s.CusName",
               "CusAbbr":                       "s.CusAbbr",
               "StatusSO":                      "s.StatusSO",
               "ReqDate":                       "s.ReqDate",
               "PmDate":                        "s.PmDate",
               "CSNoted":                       "s.CSNoted",
               "CSteam":                        "s.CSteam",
               "sales_order_external_document": "s.sales_order_external_document",
               "sales_order_released_date":     "s.sales_order_released_date",
               "sales_order_document_date":     "s.sales_order_document_date",
               "requested_week":                "s.requested_week",
               "ItemFG":                        "s.ItemFG",
               "item_description":              "s.item_description",
               "Total_QTY":                     "s.Total_QTY",
               "LineShipmentDate":              "s.LineShipmentDate",
               "TypeofFG":                      "s.TypeofFG",
               "line_item_material":            "s.line_item_material",
               "item_reference":                "s.item_reference",
               "item_quantity_to_ship":         "s.item_quantity_to_ship",
               "item_quantity_shipped":         "s.item_quantity_shipped",
               "item_quantity_to_invoice":      "s.item_quantity_to_invoice",
               "item_quantity_invoiced":        "s.item_quantity_invoiced",
               "OutstandingQty":                "s.OutstandingQty",
               "SOL":                           "s.SOL",
               "so_abbr":                       "s.so_abbr",
               "so_type":                       "s.so_type",
               f"{MODCOL}":                     f"s.{MODCOL}",
               "updated_at":                    "s.updated_at",
               "load_ts":                       "s.load_ts",
               "source_system":                 "s.source_system",
               "row_hash":                      "s.row_hash",
           })
           .execute()
    )

# ---------- (optional) Maintenance ----------
# spark.sql(f"OPTIMIZE {TARGET} ZORDER BY (SalesorderNo, SalesLineNo)")
# spark.sql(f"VACUUM {TARGET} RETAIN 168 HOURS")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Waxing and Casting Status

# CELL ********************

# ==============================================================
# GOLD: gold_wax_and_casting_status
# ==============================================================

from pyspark.sql import functions as F, Window

# ---------- CONFIG ----------
S_SRC  = "Silver_Production_Lakehouse.prod.silver_prod_order_status"
S_TEAM = "Gold_Production_Lakehouse.prod.gold_emp_team_wax"
TARGET = "Gold_Production_Lakehouse.prod.gold_waxing_and_casting_status"

# Business key (must be unique in TARGET and in each batch)
KEYS         = ["prod_order_no","item_no","CorrectCurrentLocation","type_name"]
MODCOL       = "_modified_any"
LOOKBACK_MIN = 90  # minutes overlap for safety

# ---------- HELPERS ----------
def table_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except:
        return False

def get_last_ts(name: str, col: str):
    if not table_exists(name):
        return None
    r = spark.table(name).select(F.max(F.col(col))).first()
    return r[0] if r else None

def create_like(name: str, df):
    schema_qual = ".".join(name.split(".")[:-1])
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_qual}")
    df.limit(0).write.format("delta").mode("overwrite").saveAsTable(name)

def maintain(name: str, zcols=None, vacuum_hours: int = 168):
    try:
        if zcols:
            cols_csv = ", ".join([f"`{c}`" for c in zcols])
            spark.sql(f"ANALYZE TABLE {name} COMPUTE STATISTICS FOR COLUMNS {cols_csv}")
            spark.sql(f"OPTIMIZE {name} ZORDER BY ({cols_csv})")
        else:
            spark.sql(f"OPTIMIZE {name}")
    except Exception as e:
        print(f"OPTIMIZE/ZORDER notice: {e}")
    spark.sql(f"VACUUM {name} RETAIN {vacuum_hours} HOURS")

def hash_row(cols):
    return F.sha2(F.concat_ws("§", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]), 256)

def count_dupe_keys(df, keys):
    return (df.groupBy(*[F.col(k) for k in keys]).count().filter(F.col("count") > 1).count())

def dedupe_on_keys(df, keys, order_cols):
    # order_cols: list of Columns, highest priority first
    w = Window.partitionBy(*keys).orderBy(*order_cols)
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1)
              .drop("_rn"))

def dedupe_target_in_place(target_table: str, keys: list, preferred_order_cols=None):
    if not table_exists(target_table):
        return
    tgt = spark.table(target_table)
    if preferred_order_cols:
        ords = preferred_order_cols
    else:
        ords = [F.col(MODCOL).desc_nulls_last()] if MODCOL in tgt.columns else \
               [F.col("load_ts").desc_nulls_last()] if "load_ts" in tgt.columns else \
               [hash_row(tgt.columns).desc()]
    w = Window.partitionBy(*keys).orderBy(*ords)
    before = tgt.count()
    ded = tgt.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    after = ded.count()
    if after < before:
        print(f"[TARGET CLEANUP] {target_table}: removed {before-after:,} duplicate rows")
        (ded.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema","true")
            .saveAsTable(target_table))

# ---------- 1) LOAD SOURCES ----------
src  = spark.table(S_SRC)
team = spark.table(S_TEAM)
print(f"source={src.count():,}  team={team.count():,}")

# ---------- 2) BUILD DATAWITHCALC ----------
def trim_or_null(col: str):
    c = F.col(col)
    return F.when(c.isNull() | (F.length(F.trim(c)) == 0), F.lit(None)).otherwise(F.trim(c))

cur_loc = trim_or_null("current_location_code")
mach_no = trim_or_null("machine_center_no")
is_cell = F.upper(F.substring(F.coalesce(cur_loc, F.lit("")), 1, 4)) == F.lit("CELL")

data_calc = (
    src.select(
        "created_on","modified_on","prod_order_no","prod_order_line_no","type_name","prod_order_status",
        "operation_no","open","sales_order_no","current_location_code","past_location_code",
        "employee_no","antenna_id","rfid_transaction_name","user_id","quantity","remaining_quantity",
        "item_no","machine_center_no"
    )
    .withColumn("prod_order_line_no", F.col("prod_order_line_no").cast("string"))
    .withColumn("CorrectCurrentLocation", 
                F.when(cur_loc.isNull() | is_cell, F.coalesce(mach_no, cur_loc)).otherwise(cur_loc))
    .withColumn("wKey", 
                F.concat_ws("", F.coalesce(mach_no, F.lit("")),
                               F.coalesce(F.trim(F.col("past_location_code")), F.lit("")),
                               F.coalesce(F.col("antenna_id").cast("string"), F.lit(""))))
    .withColumn("out_qty", (-1 * F.col("quantity")).cast("bigint"))
    .withColumn("pol", F.concat_ws("", F.col("prod_order_no"), F.col("prod_order_line_no")))
    .withColumn("created_on_time", F.col("created_on"))
)

# ---------- 3) PRE-JOIN DEDUPE ----------
pre_order = [
    F.col("modified_on").cast("timestamp").desc_nulls_last(),
    F.col("created_on").cast("timestamp").desc_nulls_last()
]
dedup_pre = dedupe_on_keys(data_calc, KEYS, pre_order)

# ---------- 4) JOIN TEAM ----------
joined = (
    dedup_pre.alias("p")
    .join(team.alias("w"), F.col("p.employee_no") == F.col("w.Employee_Code"), "left")
    .select(
        "p.created_on","p.modified_on","p.prod_order_no","p.prod_order_line_no","p.type_name",
        "p.operation_no","p.prod_order_status","p.open","p.sales_order_no","p.current_location_code",
        "p.past_location_code","p.employee_no","p.antenna_id","p.rfid_transaction_name","p.user_id",
        "p.quantity","p.remaining_quantity","p.item_no","p.machine_center_no","p.out_qty","p.pol",
        "p.created_on_time","w.team","p.CorrectCurrentLocation"
    )
)

# ---------- 5) POST-JOIN FILTER ----------
filtered = (
    joined.filter(
        (F.col("item_no").like("C0%")) &
        (F.col("prod_order_status") == "Released")
        # If you need exact match with the original view, also add:
        # & (F.col("type_name") == "In location in")
        # & (F.col("open") == "Yes")
    )
)

# ---------- 6) WATERMARK ----------
filtered = filtered.withColumn(
    MODCOL, 
    F.coalesce(F.col("modified_on").cast("timestamp"),
               F.col("created_on").cast("timestamp"),
               F.current_timestamp())
)

# ---------- 7) INCREMENTAL FILTER ----------
last_ts = get_last_ts(TARGET, MODCOL)
staged_all = filtered
if last_ts is not None:
    staged_all = staged_all.filter(
        F.col(MODCOL) >= (F.lit(last_ts).cast("timestamp") - F.expr(f"INTERVAL {LOOKBACK_MIN} MINUTES"))
    )

print(f"rows staged (pre-final-dedupe): {staged_all.count():,}")

# ---------- 8) FINAL DEDUPE BEFORE MERGE (HARD STOP) ----------
final_order = [
    F.col(MODCOL).desc_nulls_last(),
    F.col("modified_on").cast("timestamp").desc_nulls_last(),
    F.col("created_on").cast("timestamp").desc_nulls_last(),
    # deterministic tie-breaker:
    hash_row(staged_all.columns).desc()
]
staged = dedupe_on_keys(staged_all, KEYS, final_order)
print(f"rows staged (final-deduped): {staged.count():,}")

# ---------- 9) CREATE TARGET IF MISSING ----------
target_cols = staged.columns
if not table_exists(TARGET):
    create_like(TARGET, staged.select(*target_cols))
    print(f"Created {TARGET}")

# Optional: ensure TARGET itself has unique keys (run once; safe to keep here)
tgt_dupes = count_dupe_keys(spark.table(TARGET), KEYS)
if tgt_dupes > 0:
    print(f"[WARN] Target has {tgt_dupes:,} duplicate key groups — cleaning once.")
    dedupe_target_in_place(
        TARGET,
        KEYS,
        preferred_order_cols=[
            F.col(MODCOL).desc_nulls_last(),
            F.col("modified_on").cast("timestamp").desc_nulls_last(),
            F.col("created_on").cast("timestamp").desc_nulls_last()
        ]
    )

# ---------- 10) HASH COMPARE ----------
tgt = spark.table(TARGET)
compare_cols = [c for c in staged.columns if c not in KEYS]
staged_h = staged.withColumn("_row_hash", hash_row(compare_cols))
t_hash = hash_row(compare_cols)
target_h = tgt.select(*KEYS, t_hash.alias("_row_hash_t"))

to_apply = (
    staged_h.join(target_h, on=KEYS, how="left")
            .filter(F.col("_row_hash_t").isNull() | (F.col("_row_hash") != F.col("_row_hash_t")))
            .select(*[c for c in staged.columns])
)

# Defensive: ensure to_apply is unique on KEYS (just in case)
to_apply = dedupe_on_keys(
    to_apply,
    KEYS,
    [F.col(MODCOL).desc_nulls_last(), hash_row(to_apply.columns).desc()]
)

print(f"rows to MERGE: {to_apply.count():,}")

# ---------- 11) MERGE ----------
if to_apply.limit(1).count() == 0:
    print("No changes to apply.")
else:
    tmp = "stg_gold_production_status_casting"
    to_apply.createOrReplaceTempView(tmp)

    on_expr  = " AND ".join([f"(t.{k} <=> s.{k})" for k in KEYS])
    set_expr = ", ".join([f"t.{c}=s.{c}" for c in target_cols if c not in set(KEYS)])

    spark.sql(f"""
        MERGE INTO {TARGET} t
        USING {tmp} s
        ON {on_expr}
        WHEN MATCHED THEN UPDATE SET {set_expr}
        WHEN NOT MATCHED THEN INSERT ({", ".join(target_cols)})
        VALUES ({", ".join([f"s.{c}" for c in target_cols])})
    """)

    maintain(TARGET, zcols=KEYS + [MODCOL])

print(f"Incremental load complete for {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Casting Summary

# CELL ********************

# ==============================================================
# GOLD: casting_summary — Incremental Loader (robust watermark)
# ==============================================================

from pyspark.sql import functions as F, Window

# --------- CONFIG: source tables ----------
G_PROD = "Gold_Production_Lakehouse.prod.gold_production_order"
G_STAT = "Gold_Production_Lakehouse.prod.gold_waxing_and_casting_status"
S_CAST_PARTS = "Silver_Production_Lakehouse.prod.silver_casting_parts"
S_CAST_TREE  = "Silver_Production_Lakehouse.prod.silver_casting_tree"
S_ITEM       = "Silver_Inventory_Lakehouse.inv.silver_item"
G_SO         = "Gold_Production_Lakehouse.prod.gold_sales_order"

TARGET       = "Gold_Production_Lakehouse.prod.gold_casting_summary"

# Row key (final grain after rn=1)
KEYS         = ["prod_order_no", "prod_item_line"]

# Watermark
MODCOL       = "_modified_any"
LOOKBACK_MIN = 90  # minutes

# --------- helpers ----------
def table_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except:
        return False

def get_last_ts(name: str, col: str):
    if not table_exists(name):
        return None
    r = spark.table(name).select(F.max(F.col(col))).first()
    return r[0] if r else None

def create_like(name: str, df):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {name.rsplit('.',1)[0]}")
    df.limit(0).write.format("delta").mode("overwrite").saveAsTable(name)

def maintain(name: str, zcols=None, vacuum_hours: int = 168):
    try:
        if zcols:
            cols_csv = ", ".join([f"`{c}`" for c in zcols])
            spark.sql(f"ANALYZE TABLE {name} COMPUTE STATISTICS FOR COLUMNS {cols_csv}")
            spark.sql(f"OPTIMIZE {name} ZORDER BY ({cols_csv})")
        else:
            spark.sql(f"OPTIMIZE {name}")
    except Exception as e:
        print(f"OPTIMIZE/ZORDER notice: {e}")
        spark.sql(f"OPTIMIZE {name}")
    spark.sql(f"VACUUM {name} RETAIN {vacuum_hours} HOURS")

def count_dupes(df, keys):
    return (df.groupBy(*[F.col(k) for k in keys]).count()
              .filter(F.col("count") > 1).count())

def add_safe_watermark(df, out_col: str, candidates: list[str]):
    """Create a watermark column from available candidates.
       - 0 found  -> current_timestamp()
       - 1 found  -> that one
       - 2+ found -> greatest()
    """
    present = [c for c in candidates if c in df.columns]
    if len(present) == 0:
        return df.withColumn(out_col, F.current_timestamp())
    if len(present) == 1:
        return df.withColumn(out_col, F.col(present[0]))
    return df.withColumn(out_col, F.greatest(*[F.col(c) for c in present]))

# --------- 1) load sources ----------
pl = spark.table(G_PROD)        # gold production order/line
gs = spark.table(G_STAT)        # gold status
cp = spark.table(S_CAST_PARTS)  # silver casting parts
ct = spark.table(S_CAST_TREE)   # silver casting tree
it = spark.table(S_ITEM)        # silver items
so = spark.table(G_SO)          # gold sales order

print(f"pl={pl.count():,} gs={gs.count():,} cp={cp.count():,} ct={ct.count():,} it={it.count():,} so={so.count():,}")

# --------- 2) ProdLineData ----------
w_fg   = Window.partitionBy("prod_order_no")
w_cast = Window.partitionBy("prod_order_no", "prod_item_line")

prod_line = (
    pl.select(
        "prod_line_due_date","prod_line_start_date","prod_line_end_date",
        "prod_order_no","prod_order_line_no",
        "FG_item_no","prod_item_line","item_location",
        "prod_line_quantity","prod_line_finished_quantity","prod_line_remaining_quantity",
        "sales_order_no","ref_prod_order","prod_order_status"
    )
    # MATCH SQL: restrict to header statuses Released/Firm Planned
    .where(F.col("prod_order_status").isin("Released", "Firm Planned"))
    .withColumn(
        "sales_order",
        F.coalesce(
            F.col("sales_order_no"),
            F.first("sales_order_no", ignorenulls=True).over(Window.partitionBy("ref_prod_order"))
        )
    )
    .withColumn(
        "fg_start_date",
        F.to_date(
            F.max(F.when(F.col("prod_order_line_no")==F.lit(10000), F.col("prod_line_start_date"))).over(w_fg)
        )
    )
    .withColumn(
        "fg_due_date",
        F.max(F.when(F.col("prod_order_line_no")==F.lit(10000), F.col("prod_line_due_date"))).over(w_fg)
    )
    .withColumn(
        "casting_due_date",
        F.max(F.when(F.col("item_location")==F.lit("CST_CUT"), F.col("prod_line_due_date"))).over(w_cast)
    )
    .withColumn(
        "casting_start_date",
        F.to_date(
            F.max(F.when(F.col("item_location")==F.lit("CST_CUT"), F.col("prod_line_start_date"))).over(w_cast)
        )
    )
)

# --------- 3) EmployeeData (Gold status) ----------
emp = (
    gs
    .where(
        (F.col("type_name") == F.lit("In location in")) &
        (F.col("open") == F.lit("Yes"))
    )
    .select(
        "created_on","modified_on","prod_order_no","prod_order_line_no","type_name",
        "prod_order_status","open","sales_order_no","current_location_code",
        "past_location_code","employee_no","user_id","quantity","remaining_quantity",
        "item_no","machine_center_no","out_qty","pol","created_on_time",
        "CorrectCurrentLocation","team"
    )
)


# --------- 4) CastingData (Silver parts + tree) ----------
cast = (
    cp.alias("cp")
      .join(ct.alias("ct"), F.col("cp.casting_prod_order")==F.col("ct.casting_prod_order"), "left")
      .select(
          F.col("cp.prod_order_no"),
          F.col("cp.prod_order_line_no"),
          F.col("cp.casting_prod_order"),
          F.col("ct.casting_tree_no"),
          F.col("cp.item_no").alias("itemCST"),
          F.col("cp.casting_qty_to_tree"),
          F.col("cp.casting_qty_passed"),
          (F.col("cp.casting_qty_to_tree") - F.col("cp.casting_qty_passed")).alias("casting_qty_reject"),
          F.col("ct.casting_status"),
          F.concat(F.col("cp.prod_order_no"), F.col("cp.prod_order_line_no").cast("string")).alias("pol")
      )
)

# --------- 5) ItemData ----------
item = it.select("item_no","item_description","item_metal_category","item_category")

# --------- 6) Salesorder slim ----------
# (lakehouse has CusNo/CusAbbr; keep mapping as in your prior version)
sales = so.select(
    F.col("SalesorderNo").alias("salesorder_no"),
    F.col("CusNo").alias("CustomerNo"),
    F.col("CusAbbr").alias("CustomerAbbr")
)

# --------- 7) FinalBase ----------
final_base = (
    prod_line.alias("p")
      .join(emp.alias("e"),
            (F.col("p.prod_order_no")==F.col("e.prod_order_no")) &
            (F.col("p.prod_order_line_no")==F.col("e.prod_order_line_no")) &
            (F.col("p.prod_item_line")==F.col("e.item_no")),
            "left")
      .join(cast.alias("c"),
            (F.col("p.prod_order_no")==F.col("c.prod_order_no")) &
            (F.col("p.prod_order_line_no")==F.col("c.prod_order_line_no")),
            "left")
      .join(item.alias("i"), F.col("i.item_no")==F.col("p.prod_item_line"), "left")
      .join(sales.alias("s"),
            F.coalesce(F.col("p.sales_order"), F.col("e.sales_order_no"))==F.col("s.salesorder_no"),
            "left")
    
      .select(
          F.col("p.prod_order_no"),
          F.col("p.prod_order_line_no"),
          F.col("p.FG_item_no"),
          F.col("p.prod_item_line"),
          F.col("p.item_location"),
          F.col("p.prod_line_quantity"),
          F.col("p.prod_line_finished_quantity"),
          F.col("p.prod_line_remaining_quantity"),
          F.col("p.prod_line_due_date"),
          F.col("p.prod_line_start_date"),
          F.col("p.prod_line_end_date"),
          F.col("p.sales_order").alias("sales_order"),
          F.col("p.fg_start_date"),
          F.col("p.fg_due_date"),
          F.col("p.casting_start_date"),
          F.col("p.casting_due_date"),

          # prefer header status, fallback to event/status
          F.coalesce(F.col("p.prod_order_status"), F.col("e.prod_order_status")).alias("prod_order_status"),

          F.col("e.created_on"),
          F.col("e.modified_on"),
          F.col("e.type_name"),
          F.col("e.user_id"),
          F.col("e.open"),
          F.col("e.sales_order_no").alias("emp_sales_order_no"),
          F.col("e.quantity"),
          F.col("e.remaining_quantity"),
          F.col("e.current_location_code"),
          F.col("e.CorrectCurrentLocation"),
          F.col("e.machine_center_no"),
          F.col("e.team"),
          F.col("c.casting_prod_order"),
          F.col("c.casting_tree_no"),
          F.col("c.itemCST"),
          F.col("c.casting_qty_to_tree"),
          F.col("c.casting_qty_passed"),
          F.col("c.casting_qty_reject"),
          F.col("c.casting_status"),
          F.col("i.item_description"),
          F.col("i.item_metal_category"),
          F.col("i.item_category"),
          F.col("s.CustomerNo"),
          F.col("s.CustomerAbbr"),

          F.when(F.col("i.item_metal_category")=="SILVER 925","SILVER")
           .when(F.col("i.item_metal_category").isin("14KW","14KY","14KR","18KR","18KW","18KY","9KW","9KY"),"GOLD")
           .otherwise(F.col("i.item_metal_category")).alias("metal_category"),

          # NEW_STATUS per SQL intent: casting_status if non-empty, else machine_center_no if non-empty, else 'Not Start'
          F.when(F.col("c.casting_status").isNotNull() & (F.trim(F.col("c.casting_status")) != ""), F.trim(F.col("c.casting_status")))
           .when(F.col("e.machine_center_no").isNotNull() & (F.trim(F.col("e.machine_center_no")) != ""), F.trim(F.col("e.machine_center_no")))
           .otherwise(F.lit("Not Start")).alias("new_status"),

          F.coalesce(F.col("p.sales_order"), F.col("e.sales_order_no")).alias("new_so"),

          F.when(F.col("c.casting_qty_to_tree").isNotNull() & (F.col("c.casting_qty_to_tree") != 0), F.col("c.casting_qty_to_tree"))
           .otherwise(F.col("p.prod_line_quantity")).alias("new_qty"),

          F.row_number().over(Window.partitionBy("p.prod_order_no","p.prod_item_line")
                              .orderBy(F.col("p.prod_order_line_no").desc())
                         ).alias("rn")
      )
)

# --------- 8) Final SELECT + window aggregates ----------
w_agg_item = Window.partitionBy("prod_order_no","prod_item_line")

result = (
    final_base
      .filter(F.col("rn")==1)
      .filter(F.col("prod_item_line").like("C%"))
      .filter(F.col("prod_line_remaining_quantity") > 0)
      .withColumn("status_uc", F.upper(F.trim(F.col("new_status"))))  # normalize for comparisons
      .select(
          "prod_order_no","prod_order_line_no","FG_item_no","prod_item_line","item_location",
          "fg_start_date","fg_due_date","casting_start_date","casting_due_date","prod_order_status",
          "current_location_code","CorrectCurrentLocation","machine_center_no","team",
          "casting_prod_order",
          F.trim(F.regexp_replace(F.col("casting_tree_no"), "TREE No\\.", "")).alias("tree_no"),
          "itemCST","casting_qty_to_tree","casting_qty_passed","casting_qty_reject","casting_status",
          "CustomerNo","CustomerAbbr","metal_category","new_status",
          F.expr("concat(substring(new_so,1,2), right(new_so,4))").alias("so_abbr"),
          "new_qty",
          F.sum("new_qty").over(w_agg_item).alias("total_qty"),

          # MATCH SQL exactly: count In_WH only when NEW_STATUS == 'COMPLETE' (case-insensitive)
          F.sum(
              F.when(F.col("status_uc") == F.lit("COMPLETE"), F.coalesce(F.col("casting_qty_to_tree"), F.lit(0))).otherwise(F.lit(0))
          ).over(w_agg_item).alias("in_wh"),

          # MATCH SQL exactly: RemainingQty = TotalQty - In_WH (same partition), NULL if zero
          F.when(
              (F.sum("new_qty").over(w_agg_item) -
               F.sum(F.when(F.col("status_uc") == "COMPLETE", F.coalesce(F.col("casting_qty_to_tree"), F.lit(0))).otherwise(F.lit(0))).over(w_agg_item)) == 0,
              F.lit(None)
           ).otherwise(
              (F.sum("new_qty").over(w_agg_item) -
               F.sum(F.when(F.col("status_uc") == "COMPLETE", F.coalesce(F.col("casting_qty_to_tree"), F.lit(0))).otherwise(F.lit(0))).over(w_agg_item))
           ).alias("remaining_qty"),

          F.concat(F.col("prod_order_no"), F.col("prod_item_line")).alias("poi"),

          # carry potential mod columns if they exist
          F.col("modified_on").alias("_mod_emp")
      )
)

# --------- 9) SAFE watermark ----------
result = add_safe_watermark(result, MODCOL, candidates=["_mod_emp"])

# --------- 10) incremental staging ----------
last_ts = get_last_ts(TARGET, MODCOL)
staged = result
if last_ts is not None:
    staged = staged.filter(F.col(MODCOL) >= (F.lit(last_ts).cast("timestamp") - F.expr(f"INTERVAL {LOOKBACK_MIN} MINUTES")))
print("Rows staged:", staged.count())

# --------- 11) create target if missing ----------
if not table_exists(TARGET):
    create_like(TARGET, result.drop("_mod_emp"))
    print(f"Created {TARGET}")

# --------- 12) dedupe on KEYS before MERGE ----------
w_keys = Window.partitionBy(*KEYS).orderBy(F.col(MODCOL).desc_nulls_last())
staged_dedup = (staged
    .withColumn("_rn", F.row_number().over(w_keys))
    .filter("_rn = 1")
    .drop("_rn","_mod_emp")
)

dupes = count_dupes(staged_dedup, KEYS)
if dupes > 0:
    raise RuntimeError(f"Staged data still has {dupes} duplicate key groups")

# --------- 13) MERGE (upsert) ----------
tgt = spark.table(TARGET)
target_cols = tgt.columns

tmp = "stg_gold_casting_summary"
staged_dedup.select(*target_cols).createOrReplaceTempView(tmp)

on_expr  = " AND ".join([f"(t.{k} <=> s.{k})" for k in KEYS])
set_expr = ", ".join([f"{c}=s.{c}" for c in target_cols if c not in set(KEYS)])

spark.sql(f"""
    MERGE INTO {TARGET} t
    USING {tmp} s
    ON {on_expr}
    WHEN MATCHED THEN UPDATE SET {set_expr}
    WHEN NOT MATCHED THEN INSERT ({", ".join(target_cols)})
    VALUES ({", ".join([f"s.{c}" for c in target_cols])})
""")

# --------- 14) maintenance ----------
maintain(TARGET, zcols=KEYS + [MODCOL])

# --------- 15) sanity check ----------
# spark.sql(f"SELECT COUNT(*) AS rows_in_gold FROM {TARGET}").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Inv Output (Not Using)

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
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# # Prod Repair

# CELL ********************

# ==========================================================
# Job: Gold_Production_Lakehouse.prod.gold_prod_repair  (incremental)
#
# Mirrors SQL:
#   WITH pair_strict AS (...)
#   pair_strict_summary AS (...)
#   final SELECT including:
#     - repair_reason (substring between '-' and '∙')
#     - created_by, work_center_no from silver_prod_order_repair
#
# Notes:
#   - No SUM for main_line_qty / wre_total_qty (keep exact value)
#   - Incremental on date(change_ts)
# ==========================================================

from pyspark.sql import functions as F
from pyspark.sql import Window as W
from delta.tables import DeltaTable
from datetime import date, datetime

# ---------- Sources ----------
MH_SRC   = "Silver_Production_Lakehouse.prod.silver_prod_order_header"   # main header
ML_SRC   = "Silver_Production_Lakehouse.prod.silver_prod_order_line"     # main line
WH_SRC   = "Silver_Production_Lakehouse.prod.silver_prod_order_header"   # WRE header
WL_SRC   = "Silver_Production_Lakehouse.prod.silver_prod_order_line"     # WRE line
RPR_SRC  = "Silver_Production_Lakehouse.prod.silver_prod_order_repair"   # repair

# ---------- Target ----------
TARGET = "Gold_Production_Lakehouse.prod.gold_prod_repair"

# ---------- Widgets / Params ----------
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
    """Use max(date(change_ts)) from target as watermark."""
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

# ---------- Load sources ----------
mh_raw  = spark.table(MH_SRC)
ml_raw  = spark.table(ML_SRC)
wh_raw  = spark.table(WH_SRC)
wl_raw  = spark.table(WL_SRC)
rpr_raw = spark.table(RPR_SRC)

# ==========================================================
# 1) change_ts per (main order, main line)
# ==========================================================

# main line modified_on per (order, line)
ml_ts = (
    ml_raw
    .groupBy("prod_order_no", "prod_order_line_no")
    .agg(F.max("modified_on").alias("ml_mod"))
)

# WRE headers only
wh_wre = wh_raw.filter(F.col("prod_order_no").like("WRE%")).alias("wh")

# WH.modified_on per main order
wh_ts = (
    wh_wre
    .groupBy(F.col("ref_prod_order").alias("m_prod"))
    .agg(F.max("modified_on").alias("wh_mod"))
)

# WL.modified_on per WRE order
wl_ts = (
    wl_raw
    .groupBy(F.col("prod_order_no").alias("wre_prod"))
    .agg(F.max("modified_on").alias("wl_mod"))
)

# Map WL → main (via WH.ref_prod_order)
wl_map = (
    wh_wre
    .select(
        F.col("prod_order_no").alias("wre_prod"),
        F.col("ref_prod_order").alias("m_prod")
    )
    .distinct()
    .join(wl_ts, on="wre_prod", how="inner")
    .groupBy("m_prod")
    .agg(F.max("wl_mod").alias("wl_mod"))
)

# Combine WH and WL per main order
w_mod_per_main = (
    wh_ts.alias("A")
    .join(wl_map.alias("B"), on="m_prod", how="full")
    .select(
        F.coalesce(F.col("A.m_prod"), F.col("B.m_prod")).alias("m_prod"),
        F.greatest(F.col("A.wh_mod"), F.col("B.wl_mod")).alias("w_mod")
    )
)

# change_ts per (main order, line)
change_ts_line = (
    ml_ts.alias("mlt")
    .join(
        w_mod_per_main.alias("wm"),
        F.col("mlt.prod_order_no") == F.col("wm.m_prod"),
        "left"
    )
    .select(
        F.col("mlt.prod_order_no").alias("prod_order_no"),
        F.col("mlt.prod_order_line_no").alias("prod_order_line_no"),
        F.greatest(F.col("mlt.ml_mod"), F.col("wm.w_mod")).alias("change_ts")
    )
)

# Scope for incremental
scope = (
    change_ts_line
    .filter(
        (F.to_date("change_ts") >= F.lit(since_date)) &
        (F.to_date("change_ts") <= F.lit(until_date))
    )
    .select(
        F.col("prod_order_no").alias("s_prod_order_no"),
        F.col("prod_order_line_no").alias("s_prod_order_line_no")
    )
    .distinct()
)

# ==========================================================
# 2) pair_strict (main ↔ WRE by item, exclude M-, FIN-GOODS)
# ==========================================================

ml = (
    ml_raw.alias("ml")
    .join(
        scope.alias("sc"),
        (F.col("ml.prod_order_no") == F.col("sc.s_prod_order_no")) &
        (F.col("ml.prod_order_line_no") == F.col("sc.s_prod_order_line_no")),
        "inner"
    )
)

mh = mh_raw.alias("mh")
wh = wh_wre.alias("wh")
wl = wl_raw.alias("wl")

pair_strict = (
    ml
    .join(mh, F.col("mh.prod_order_no") == F.col("ml.prod_order_no"), "inner")
    .join(wh, F.col("wh.ref_prod_order") == F.col("ml.prod_order_no"), "inner")
    .join(wl, F.col("wl.prod_order_no") == F.col("wh.prod_order_no"), "inner")
    .where(
        (F.col("ml.item_no") == F.col("wl.item_no")) &
        (~F.col("ml.item_no").like("M-%")) &
        (~F.col("wl.item_no").like("M-%")) &
        (F.col("ml.item_location") == F.lit("FIN-GOODS"))
    )
    .select(
        # main
        F.col("ml.prod_order_no").alias("main_prod"),
        F.col("ml.prod_order_line_no").alias("main_line_no"),
        F.col("ml.item_no").alias("main_item_no"),
        F.col("ml.item_location").alias("main_item_location"),
        F.col("ml.prod_line_quantity").alias("main_line_qty"),
        F.col("mh.prod_order_status").alias("main_status"),
        F.col("mh.created_on").alias("main_created_on"),
        F.col("wh.remark").alias("wre_remark"),

        # WRE
        F.col("wl.prod_order_no").alias("wre_prod"),
        F.col("wl.prod_order_line_no").alias("wre_line_no"),
        F.col("wl.item_no").alias("wre_item_no"),
        F.col("wl.prod_line_quantity").alias("wre_line_qty"),
        F.col("wh.prod_order_status").alias("wre_status")
    )
)

# ==========================================================
# 3) pair_strict_summary (keep exact main_line_qty / wre_total_qty, no SUM)
# ==========================================================

pair_strict_summary = (
    pair_strict
    .groupBy(
        "main_prod",
        "main_line_no",
        "main_item_no"
    )
    .agg(
        F.min("main_item_location").alias("main_item_location"),
        F.min("main_created_on").alias("main_created_on"),
        F.min("main_status").alias("main_status"),
        # keep the “exact” values (no SUM)
        F.min("main_line_qty").alias("main_line_qty"),
        F.min("wre_line_qty").alias("wre_total_qty")
    )
)

# ==========================================================
# 4) Join ps + summary + repair; add repair_reason
# ==========================================================

# silver_prod_order_repair slim
repair = (
    rpr_raw
    .select(
        F.col("prod_order_no").alias("r_prod_order_no"),
        "created_by",
        "work_center_no"
    )
)

# SQL-style repair_reason expression (between '-' and '∙')
repair_reason_expr = """
CASE
  WHEN instr(wre_remark, '-') > 0
       AND instr(wre_remark, '∙') > instr(wre_remark, '-') + 1
  THEN trim(
         substring(
           wre_remark,
           instr(wre_remark, '-') + 1,
           instr(wre_remark, '∙') - instr(wre_remark, '-') - 1
         )
       )
  ELSE NULL
END
"""

base_final = (
    pair_strict.alias("ps")
    .join(
        pair_strict_summary.alias("s"),
        (F.col("s.main_prod")    == F.col("ps.main_prod")) &
        (F.col("s.main_line_no") == F.col("ps.main_line_no")) &
        (F.col("s.main_item_no") == F.col("ps.main_item_no")),
        "inner"
    )
    .join(
        repair.alias("r"),
        F.col("ps.main_prod") == F.col("r.r_prod_order_no"),
        "left"
    )
    .select(
        F.col("ps.main_prod").alias("main_prod"),
        F.col("ps.main_line_no").alias("main_line_no"),
        F.col("ps.main_item_no").alias("main_item_no"),
        F.col("ps.main_item_location").alias("main_item_location"),
        F.col("ps.main_created_on").alias("main_created_on"),
        F.col("s.main_line_qty").alias("main_line_qty"),
        F.expr(repair_reason_expr).alias("repair_reason"),
        F.col("ps.wre_remark").alias("wre_remark"),
        F.col("s.wre_total_qty").alias("wre_total_qty"),
        F.when(
            F.col("s.main_line_qty") != 0,
            (F.col("s.wre_total_qty") / F.col("s.main_line_qty")) * F.lit(100.0)
        )
        .otherwise(F.lit(None))
        .cast("decimal(18,2)")
        .alias("wre_pct_line"),
        F.col("ps.wre_prod").alias("wre_prod"),
        F.col("ps.wre_line_no").alias("wre_line_no"),
        F.col("ps.wre_item_no").alias("wre_item_no"),
        F.col("ps.wre_line_qty").alias("wre_line_qty"),
        F.col("ps.main_status").alias("main_status"),
        F.col("ps.wre_status").alias("wre_status"),
        F.col("r.created_by").alias("created_by"),
        F.col("r.work_center_no").alias("work_center_no")
    )
)

# ==========================================================
# 5) Attach change_ts, create row_id, dedup for MERGE
# ==========================================================

final_with_ts = (
    base_final.alias("f")
    .join(
        change_ts_line.alias("ct"),
        (F.col("f.main_prod") == F.col("ct.prod_order_no")) &
        (F.col("f.main_line_no") == F.col("ct.prod_order_line_no")),
        "left"
    )
    .select("f.*", F.col("ct.change_ts").alias("change_ts"))
)

# MERGE key (1 row per main+line+item+WRE)
row_id_cols = [
    "main_prod",
    "main_line_no",
    "main_item_no",
    "wre_prod",
    "wre_line_no",
    "wre_item_no"
]

final_with_ts = final_with_ts.withColumn(
    "row_id",
    F.sha2(
        F.concat_ws("||", *[
            F.coalesce(F.col(c).cast("string"), F.lit(""))
            for c in row_id_cols
        ]),
        256
    )
)

# Deduplicate by row_id (avoid multiple source rows per target row)
dedup_w = W.partitionBy("row_id").orderBy(
    F.col("change_ts").desc_nulls_last(),
    F.col("main_created_on").desc_nulls_last()
)

final_df = (
    final_with_ts
    .withColumn("rn", F.row_number().over(dedup_w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# ---------- Write (create or merge) ----------
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

merge_or_create(TARGET, final_df)
print(f"✅ Done: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Prod Component Ready

# CELL ********************

# Spark (Delta) incremental load -> gold_consumed_status
# Implements your SQL:
#   LineStatus CTE:
#     - Prod Order Line LEFT JOIN Prod Order Component on (Prod. Order No., Line No.)
#     - filter components where Item No. NOT LIKE 'M-%'/'PL-%'/'RM-%'
#     - LineStatus derived from finished qty vs qty, picked qty sum, remaining qty flags
#   OrderAgg CTE:
#     - aggregate line statuses per Production Order
#   Final:
#     - Production Order LEFT JOIN OrderAgg
#     - OverallStatus logic per your CASE
#
# Increment strategy:
#   - watermark on Production Order SystemModifiedAt (fallback SystemCreatedAt)
#   - rebuild impacted production orders (by No.) so line/component aggregates stay correct
# MERGE key: prodOrderNo

from pyspark.sql import functions as F
from pyspark.sql.window import Window

TARGET = "Gold_Production_Lakehouse.prod.gold_prod_component_ready"

PO_SRC = "Silver_BC_Lakehouse.bc.`Production Order`"
PL_SRC = "Silver_BC_Lakehouse.bc.`Prod Order Line`"
PC_SRC = "Silver_BC_Lakehouse.bc.`Prod Order Component`"

# 1) Create target table if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    Status STRING,
    prodOrderNo STRING,
    Due_Date DATE,
    Sales_Order_No STRING,
    new_so STRING,
    typeSO STRING,
    Source_No STRING,
    Starting_Date DATE,
    Ending_Date DATE,
    OverallStatus STRING,

    -- tracking
    PO_SystemCreatedAt TIMESTAMP,
    PO_SystemModifiedAt TIMESTAMP
)
USING DELTA
""")

# 2) Watermark from target
wm = (
    spark.table(TARGET)
    .select(F.max(F.col("PO_SystemModifiedAt")).alias("wm"))
    .collect()[0]["wm"]
)
if wm is None:
    wm = "1900-01-01 00:00:00"

# 3) Read sources
po = spark.table(PO_SRC).alias("po")
pl = spark.table(PL_SRC).alias("pl")
pc = spark.table(PC_SRC).alias("pc")

# 4) Incremental impacted production orders (by PO header changes)
po_inc = (
    po.withColumn("wm_ts", F.coalesce(F.col("`SystemModifiedAt`"), F.col("`SystemCreatedAt`")))
      .filter(F.col("wm_ts") > F.to_timestamp(F.lit(str(wm))))
      .select(F.col("`No.`").alias("prodOrderNo"))
      .distinct()
      .alias("po_inc")
)

# If first run (no watermark), rebuild everything
first_run = (str(wm) == "1900-01-01 00:00:00")
if first_run:
    impacted_po = po.select(F.col("`No.`").alias("prodOrderNo")).distinct().alias("imp")
else:
    impacted_po = po_inc.alias("imp")

# 5) Rebuild relevant lines/components for impacted orders
pl_rb = (
    pl.join(impacted_po, on=(F.col("`Prod. Order No.`") == F.col("imp.prodOrderNo")), how="inner")
      .alias("pl_rb")
)

pc_rb = (
    pc.join(impacted_po, on=(F.col("`Prod. Order No.`") == F.col("imp.prodOrderNo")), how="inner")
      .alias("pc_rb")
)

# Filter components (NOT LIKE M- / PL- / RM-)
pc_f = (
    pc_rb.filter(~F.col("`Item No.`").like("M-%"))
         .filter(~F.col("`Item No.`").like("PL-%"))
         .filter(~F.col("`Item No.`").like("RM-%"))
         .alias("pc_f")
)

# 6) LineStatus CTE (grouped by prod order + line + item + qty fields)
d38 = lambda c: F.col(c).cast("decimal(38,10)")

joined = (
    pl_rb.join(
        pc_f,
        on=(
            (F.col("pl_rb.`Prod. Order No.`") == F.col("pc_f.`Prod. Order No.`")) &
            (F.col("pl_rb.`Line No.`") == F.col("pc_f.`Prod. Order Line No.`"))
        ),
        how="left"
    )
)

# Aggregates used in CASE
sum_picked = F.sum(F.coalesce(d38("pc_f.`Qty. Picked`"), F.lit(0).cast("decimal(38,10)")))
sum_has_remaining = F.sum(
    F.when(F.coalesce(d38("pc_f.`Remaining Quantity`"), F.lit(0).cast("decimal(38,10)")) > F.lit(0.1), F.lit(1)).otherwise(F.lit(0))
)

line_status_df = (
    joined.groupBy(
        F.col("pl_rb.`Prod. Order No.`").alias("prodOrderNo"),
        F.col("pl_rb.`Line No.`").alias("prodOrderLineNo"),
        F.col("pl_rb.`Item No.`").alias("pl_item_no"),
        d38("pl_rb.`Finished Quantity`").alias("FinishedQty"),
        d38("pl_rb.`Quantity`").alias("Qty"),
    )
    .agg(
        sum_picked.alias("sumPicked"),
        sum_has_remaining.alias("hasRemainingCnt")
    )
    .withColumn(
        "LineStatus",
        F.when(
            (F.col("prodOrderLineNo") == F.lit(10000)) &
            (F.coalesce(F.col("FinishedQty"), F.lit(0).cast("decimal(38,10)")) == F.coalesce(F.col("Qty"), F.lit(0).cast("decimal(38,10)"))),
            F.lit("Finished")
        )
        .when(
            (F.coalesce(F.col("FinishedQty"), F.lit(0).cast("decimal(38,10)")) == F.coalesce(F.col("Qty"), F.lit(0).cast("decimal(38,10)"))),
            F.lit("Finished")
        )
        .when(
            F.col("sumPicked") == F.lit(0).cast("decimal(38,10)"),
            F.lit("Not Consumed")
        )
        .when(
            F.col("hasRemainingCnt") > F.lit(0),
            F.lit("Partial Consumed")
        )
        .otherwise(F.lit("Consumed"))
    )
    .select("prodOrderNo", "prodOrderLineNo", "LineStatus")
    .alias("ls")
)

# 7) OrderAgg CTE
order_agg = (
    po.join(impacted_po, on=(F.col("`No.`") == F.col("imp.prodOrderNo")), how="inner")
      .select(F.col("`No.`").alias("prodOrderNo"))
      .distinct()
      .join(line_status_df, on="prodOrderNo", how="left")
      .groupBy("prodOrderNo")
      .agg(
          F.sum(F.when((F.col("prodOrderLineNo") == 10000) & (F.col("LineStatus") == "Finished"), F.lit(1)).otherwise(F.lit(0))).alias("hasMainFinished"),
          F.sum(F.when(F.col("LineStatus") == "Partial Consumed", F.lit(1)).otherwise(F.lit(0))).alias("anyPartial"),
          F.sum(F.when(F.col("LineStatus") == "Not Consumed", F.lit(1)).otherwise(F.lit(0))).alias("notConsumedLines"),
          F.count(F.col("LineStatus")).alias("lineCount")
      )
      .alias("oa")
)

# 8) Final select
po_imp = (
    po.join(impacted_po, on=(F.col("`No.`") == F.col("imp.prodOrderNo")), how="inner")
      .alias("po_imp")
)

final_df = (
    po_imp.join(order_agg, on=(F.col("po_imp.`No.`") == F.col("oa.prodOrderNo")), how="left")
    .withColumn("Starting_Date", F.to_date(F.col("po_imp.`Starting Date-Time`")))
    .withColumn("Ending_Date", F.to_date(F.col("po_imp.`Ending Date-Time`")))
    .withColumn(
        "new_so",
        F.concat(
            F.substring(F.col("po_imp.`Sales Order No.`"), 1, 2),
            F.substring(F.col("po_imp.`Sales Order No.`"), -4, 4)
        )
    )
    .withColumn("typeSO", F.substring(F.col("po_imp.`Sales Order No.`"), 1, 2))
    .withColumn(
        "OverallStatus",
        F.when(F.coalesce(F.col("oa.lineCount"), F.lit(0)) == F.lit(0), F.col("po_imp.`Status`"))
         .when(F.coalesce(F.col("oa.hasMainFinished"), F.lit(0)) > F.lit(0), F.lit("Finished"))
         .when(F.coalesce(F.col("oa.anyPartial"), F.lit(0)) > F.lit(0), F.lit("Partial Consumed"))
         .when(F.coalesce(F.col("oa.notConsumedLines"), F.lit(0)) == F.coalesce(F.col("oa.lineCount"), F.lit(0)), F.lit("Not Consumed"))
         .otherwise(F.lit("Finished"))
    )
    .select(
        F.col("po_imp.`Status`").alias("Status"),
        F.col("po_imp.`No.`").alias("prodOrderNo"),
        F.to_date(F.col("po_imp.`Due Date`")).alias("Due_Date"),
        F.col("po_imp.`Sales Order No.`").alias("Sales_Order_No"),
        F.col("new_so"),
        F.col("typeSO"),
        F.col("po_imp.`Source No.`").alias("Source_No"),
        F.col("Starting_Date"),
        F.col("Ending_Date"),
        F.col("OverallStatus"),
        F.col("po_imp.`SystemCreatedAt`").alias("PO_SystemCreatedAt"),
        F.col("po_imp.`SystemModifiedAt`").alias("PO_SystemModifiedAt"),
    )
)

final_df.createOrReplaceTempView("gold_consumed_status_inc")

# 9) MERGE
spark.sql(f"""
MERGE INTO {TARGET} AS t
USING gold_consumed_status_inc AS s
ON  t.prodOrderNo = s.prodOrderNo
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Total Time Outsource Time

# CELL ********************

# ==========================================================
# Job: gold_prod_total_time_outsource_time  (incremental)
# Mirrors your SQL view [prod].[v_prod_time_summary]
# Sources:
#   - Silver_Production_Lakehouse.prod.silver_prod_routing_line  (r)
#   - Silver_Production_Lakehouse.prod.silver_prod_order_status  (s)
#   - Gold_Production_Lakehouse.prod.gold_production_asgn_cell   (c)
# Target (Delta table):
#   - Gold_Production_Lakehouse.prod.gold_prod_total_time_outsource_time
#
# Incremental watermark:
#   per (prod_order_no, prod_order_line_no) -> change_ts =
#   greatest(max(r.modified_on), max(s.modified_on))
# Plus: expose latest s.created_on as created_on in final output
# ==========================================================

from pyspark.sql import functions as F, Window as W
from delta.tables import DeltaTable
from datetime import date, datetime

# ---------- Sources ----------
ROUTING_SRC = "Silver_Production_Lakehouse.prod.silver_prod_routing_line"   # r
STATUS_SRC  = "Silver_Production_Lakehouse.prod.silver_prod_order_status"   # s
CELL_SRC    = "Gold_Production_Lakehouse.prod.gold_production_asgn_cell"    # c

# ---------- Target ----------
TARGET = "Gold_Production_Lakehouse.prod.gold_prod_total_time_outsource_time"

# ---------- Params / widgets ----------
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

print(f"Incremental window (change_ts date): {since_date} -> {until_date}")

# ---------- Load sources ----------
r_raw = spark.table(ROUTING_SRC).alias("r")
s_raw = spark.table(STATUS_SRC).alias("s")
c_raw = spark.table(CELL_SRC).alias("c")

# ---------- Build per-(order,line) change_ts ----------
# Ensure both r_mod and s_mod are TIMESTAMP so greatest() works
r_ts = (
    r_raw
        .groupBy("prod_order_no", "prod_order_line_no")
        .agg(F.max(F.to_timestamp("modified_on")).alias("r_mod"))
)

s_ts = (
    s_raw
        .groupBy("prod_order_no", "prod_order_line_no")
        .agg(F.max(F.to_timestamp("modified_on")).alias("s_mod"))
)

ts_joined = (
    r_ts.join(s_ts, ["prod_order_no", "prod_order_line_no"], "outer")
        .withColumn(
            "change_ts",
            F.greatest(F.col("r_mod"), F.col("s_mod"))  # both TIMESTAMP
        )
        .select("prod_order_no", "prod_order_line_no", "change_ts")
)

# ---------- Incremental filter ----------
ts_windowed = ts_joined.filter(
    (F.to_date("change_ts") >= F.lit(since_date)) &
    (F.to_date("change_ts") <= F.lit(until_date))
)

scope = ts_windowed.select("prod_order_no", "prod_order_line_no").distinct()

r = r_raw.join(scope, ["prod_order_no", "prod_order_line_no"], "inner")
s = s_raw.join(scope, ["prod_order_no", "prod_order_line_no"], "inner")
c = c_raw.join(scope, ["prod_order_no", "prod_order_line_no"], "inner")

# ---------- r_total: total run_time per (order,line) ----------
r_total = (
    r
        .select(
            "prod_order_no",
            "prod_order_line_no",
            F.coalesce(F.col("run_time"), F.lit(0.0)).alias("run_time"),
        )
        .groupBy("prod_order_no", "prod_order_line_no")
        .agg(F.sum("run_time").alias("total_run_time"))
)

# ---------- s_user_ops: distinct (order,line,machine_center_no) for outsource user ----------
s_user_ops = (
    s
        .filter(F.col("user_id") == F.lit("outsource@ennovie.com"))
        .select("prod_order_no", "prod_order_line_no", "machine_center_no")
        .dropDuplicates()
)

# ---------- r_outsource: sum run_time where routing_no == machine_center_no ----------
routing_col = "routing_no" if "routing_no" in r.columns else "operation_no"

r_for_out = (
    r
        .select(
            "prod_order_no",
            "prod_order_line_no",
            routing_col,
            F.coalesce(F.col("run_time"), F.lit(0.0)).alias("run_time"),
        )
)

r_outsource = (
    r_for_out.alias("r")
        .join(
            s_user_ops.alias("u"),
            on=[
                F.col("r.prod_order_no") == F.col("u.prod_order_no"),
                F.col("r.prod_order_line_no") == F.col("u.prod_order_line_no"),
                F.trim(F.col("r." + routing_col)) == F.trim(F.col("u.machine_center_no")),
            ],
            how="inner",
        )
        .groupBy(
            F.col("r.prod_order_no").alias("prod_order_no"),
            F.col("r.prod_order_line_no").alias("prod_order_line_no"),
        )
        .agg(F.sum("run_time").alias("outsource_run_time"))
)

# ---------- item_map: MAX(item_no) per (order,line) ----------
item_map = (
    s
        .groupBy("prod_order_no", "prod_order_line_no")
        .agg(F.max("item_no").alias("item_no"))
)

# ---------- latest status created_on per (order,line) ----------
# Cast to timestamp in case created_on is string
s_created = (
    s
        .groupBy("prod_order_no", "prod_order_line_no")
        .agg(F.max(F.to_timestamp("created_on")).alias("created_on"))
)

# ---------- Assemble final ----------
final_df = (
    r_total.alias("rt")
        .join(
            r_outsource.alias("ro"),
            on=["prod_order_no", "prod_order_line_no"],
            how="left",
        )
        .join(
            c.select(
                "prod_order_no",
                "prod_order_line_no",
                "cell_line",
                "prod_line",
                "item_no",
            ).alias("c"),
            on=["prod_order_no", "prod_order_line_no"],
            how="left",
        )
        .join(
            item_map.alias("im"),
            on=["prod_order_no", "prod_order_line_no"],
            how="left",
        )
        .select(
            F.col("rt.prod_order_no").alias("prod_order_no"),
            F.col("rt.prod_order_line_no").alias("prod_order_line_no"),
            F.coalesce(F.col("im.item_no"), F.col("c.item_no")).alias("item_no"),
            F.col("c.cell_line").alias("cell_line"),
            F.col("c.prod_line").alias("prod_line"),
            F.col("rt.total_run_time").alias("total_run_time"),
            F.coalesce(F.col("ro.outsource_run_time"), F.lit(0.0)).alias(
                "outsource_run_time"
            ),
            F.when(F.col("rt.total_run_time") == F.lit(0.0), F.lit(0.0))
            .otherwise(
                (
                    F.coalesce(F.col("ro.outsource_run_time"), F.lit(0.0))
                    / F.col("rt.total_run_time")
                )
                * F.lit(100.0)
            )
            .alias("outsource_pct"),
        )
        # attach change_ts and created_on
        .join(ts_windowed, ["prod_order_no", "prod_order_line_no"], "left")
        .join(s_created, ["prod_order_no", "prod_order_line_no"], "left")  # adds created_on
)

# ---------- Deterministic row_id for MERGE (order+line) ----------
final_df = final_df.withColumn(
    "row_id",
    F.sha2(
        F.concat_ws(
            "||",
            F.coalesce(F.col("prod_order_no").cast("string"), F.lit("")),
            F.coalesce(F.col("prod_order_line_no").cast("string"), F.lit("")),
        ),
        256,
    ),
)

# ---------- Write (create or merge) ----------
def merge_or_create(target, df):
    if not table_exists(target):
        print(f"Creating {target} ...")
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target)
        )
        return
    print(f"Merging into {target} ...")
    tgt = DeltaTable.forName(spark, target)
    (
        tgt.alias("tgt")
        .merge(df.alias("src"), "src.row_id <=> tgt.row_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

merge_or_create(TARGET, final_df)
print(f"Done: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Actual Time by Employees

# CELL ********************

# ==========================================================
# Job: Gold_Production_Lakehouse.prod.gold_prod_actual_time_by_employees
# Strictly mirrors SQL view dbo.v_ProdOrderRunTimes_actual
# Incremental by date(change_ts)
# ==========================================================

from pyspark.sql import functions as F, Window as W
from delta.tables import DeltaTable
from datetime import datetime, date

# -------- Sources ----------
S_SRC  = "Silver_Production_Lakehouse.prod.silver_prod_order_status"
R_SRC  = "Silver_Production_Lakehouse.prod.silver_prod_routing_line"
CL_SRC = "Silver_Production_Lakehouse.prod.silver_cell_list"

# -------- Target ----------
TARGET = "Gold_Production_Lakehouse.prod.gold_prod_actual_time_by_employees"

# -------- Params ----------
def get_widget(name: str, default: str) -> str:
    try:
        import dbutils  # type: ignore
        return dbutils.widgets.get(name)  # type: ignore
    except Exception:
        return default

try:
    dbutils.widgets.text("full_reload", "false")  # type: ignore
    dbutils.widgets.text("since", "")             # yyyy-MM-dd
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

# -------- Load sources ----------
s_raw  = spark.table(S_SRC).alias("s")
r_raw  = spark.table(R_SRC).alias("r")
cl_raw = spark.table(CL_SRC).alias("cl")

# -------- Incremental scope ----------
# Ensure modified_on is TIMESTAMP on both sides so greatest() works

s_ts = (
    s_raw
    .select(
        "prod_order_no",
        "prod_order_line_no",
        "machine_center_no",
        F.to_timestamp("modified_on").alias("modified_on_ts")
    )
    .groupBy("prod_order_no", "prod_order_line_no", "machine_center_no")
    .agg(F.max("modified_on_ts").alias("s_mod"))
)

r_ts = (
    r_raw
    .select(
        F.col("prod_order_no").alias("r_order"),
        F.col("prod_order_line_no").alias("r_line"),
        F.col("routing_no").alias("r_mc"),
        F.to_timestamp("modified_on").alias("r_mod")
    )
    .groupBy("r_order", "r_line", "r_mc")
    .agg(F.max("r_mod").alias("r_mod"))
)

ts_joined = (
    s_ts.alias("ss")
    .join(
        r_ts.alias("rr"),
        on=[
            F.col("ss.prod_order_no")      == F.col("rr.r_order"),
            F.col("ss.prod_order_line_no") == F.col("rr.r_line"),
            F.col("ss.machine_center_no")  == F.col("rr.r_mc"),
        ],
        how="full"
    )
    .select(
        F.coalesce(F.col("ss.prod_order_no"),      F.col("rr.r_order")).alias("prod_order_no"),
        F.coalesce(F.col("ss.prod_order_line_no"), F.col("rr.r_line")).alias("prod_order_line_no"),
        F.coalesce(F.col("ss.machine_center_no"),  F.col("rr.r_mc")).alias("machine_center_no"),
        F.col("ss.s_mod"),
        F.col("rr.r_mod")
    )
    .withColumn("change_ts", F.greatest(F.col("s_mod"), F.col("r_mod")))  # both TIMESTAMP
)

ts_windowed = ts_joined.filter(
    (F.to_date("change_ts") >= F.lit(since_date)) &
    (F.to_date("change_ts") <= F.lit(until_date))
)

scope = ts_windowed.select("prod_order_no", "prod_order_line_no", "machine_center_no").distinct()

s_scoped = (
    s_raw.alias("s")
    .join(
        scope.alias("sc"),
        on=[
            F.col("s.prod_order_no")      == F.col("sc.prod_order_no"),
            F.col("s.prod_order_line_no") == F.col("sc.prod_order_line_no"),
            F.col("s.machine_center_no")  == F.col("sc.machine_center_no"),
        ],
        how="inner"
    )
    .select("s.*")
)

r_scoped = (
    r_raw.alias("r")
    .join(
        scope.alias("sc"),
        on=[
            F.col("r.prod_order_no")      == F.col("sc.prod_order_no"),
            F.col("r.prod_order_line_no") == F.col("sc.prod_order_line_no"),
            F.col("r.routing_no")         == F.col("sc.machine_center_no"),
        ],
        how="inner"
    )
    .select("r.*")
)

# -------- r_dedup ----------
r_dedup = (
    r_scoped
    .groupBy(
        F.col("prod_order_no").alias("r_order"),
        F.col("prod_order_line_no").alias("r_line"),
        F.col("routing_no").alias("r_mc"),
    )
    .agg(F.max("run_time").alias("run_time"))
    .select(
        F.col("r_order").alias("prod_order_no"),
        F.col("r_line").alias("prod_order_line_no"),
        F.col("r_mc").alias("machine_center_no"),
        "run_time"
    )
)

# -------- s_from (dedup + exclude outsource) ----------
s_from_all = (
    s_scoped
    .filter(
        (F.col("type_name") == F.lit("From employee")) &
        (F.col("user_id") != F.lit("outsource@ennovie.com"))
    )
    .withColumn("created_on_ts",  F.to_timestamp("created_on"))
    .withColumn("modified_on_ts", F.to_timestamp("modified_on"))
)

w_rn = (
    W.partitionBy(
        "prod_order_no", "prod_order_line_no", "machine_center_no",
        "operation_no", "item_no", "user_id"
    )
    .orderBy(F.col("modified_on_ts").desc_nulls_last(),
             F.col("created_on_ts").desc_nulls_last())
)

s_from = (
    s_from_all
    .withColumn("rn", F.row_number().over(w_rn))
    .filter(F.col("rn") == 1)
    .select(
        "created_on", "modified_on", "created_on_ts", "modified_on_ts",
        "prod_order_no", "prod_order_line_no", "machine_center_no",
        "operation_no", "item_no", "user_id", "quantity", "remaining_quantity",
        "sales_order_no", "current_location_code", "past_location_code", "employee_no"
    )
)

# -------- To employee actual minutes ----------
to_filter = (
    s_scoped
    .filter(F.col("type_name") == F.lit("To employee"))
    .withColumn("to_created_on",  F.to_timestamp("created_on"))
    .withColumn("to_modified_on", F.to_timestamp("modified_on"))
)

to_minutes = F.when(
    F.col("to_modified_on").isNull() | F.col("to_created_on").isNull(), F.lit(0.0)
).when(
    F.col("to_modified_on") < F.col("to_created_on"), F.lit(0.0)
).otherwise(
    (F.col("to_modified_on").cast("long") - F.col("to_created_on").cast("long")) / 60.0
)

to_employee_intervals = (
    to_filter
    .select("prod_order_no", "prod_order_line_no", "machine_center_no", "to_created_on", "to_modified_on")
    .withColumn("actual_minutes", F.greatest(to_minutes, F.lit(0.0)))
)

to_employee_agg = (
    to_employee_intervals
    .groupBy("prod_order_no", "prod_order_line_no", "machine_center_no")
    .agg(F.sum("actual_minutes").alias("actual_run_time_min"))
)

# -------- Final SELECT ----------
final_df = (
    s_from.alias("s")
    .join(
        cl_raw.select(
            F.col("email_address").alias("email_address"),
            "cell_line", "prod_line", "sub_department"
        ).alias("l"),
        F.col("s.user_id") == F.col("l.email_address"),
        "left"
    )
    .join(
        r_dedup.alias("r"),
        on=[
            F.col("s.prod_order_no")      == F.col("r.prod_order_no"),
            F.col("s.prod_order_line_no") == F.col("r.prod_order_line_no"),
            F.col("s.machine_center_no")  == F.col("r.machine_center_no"),
        ],
        how="left"
    )
    .join(
        to_employee_agg.alias("t"),
        on=[
            F.col("s.prod_order_no")      == F.col("t.prod_order_no"),
            F.col("s.prod_order_line_no") == F.col("t.prod_order_line_no"),
            F.col("s.machine_center_no")  == F.col("t.machine_center_no"),
        ],
        how="left"
    )
    .select(
        F.col("s.created_on_ts").alias("created_on"),
        F.col("s.modified_on_ts").alias("modified_on"),
        F.col("s.prod_order_no"),
        F.col("s.prod_order_line_no"),
        F.col("s.machine_center_no"),
        F.col("s.operation_no"),
        F.col("s.item_no"),
        F.col("s.user_id"),
        F.col("s.quantity"),
        F.col("s.remaining_quantity"),
        F.col("s.sales_order_no"),
        F.col("s.current_location_code"),
        F.col("s.past_location_code"),
        F.col("s.employee_no"),
        F.col("l.cell_line"),
        F.col("l.prod_line"),
        F.col("l.sub_department"),
        F.col("r.run_time").alias("plan_run_time"),
        (
            F.abs(F.col("s.quantity")).cast("double")
            * F.coalesce(F.col("r.run_time").cast("double"), F.lit(0.0))
        ).alias("total_plan_runtime"),
        F.coalesce(F.col("t.actual_run_time_min"), F.lit(0.0)).alias("actual_run_time_min"),
        F.greatest(F.col("s.modified_on_ts"), F.col("s.created_on_ts")).alias("change_ts")
    )
)

# -------- Row id for merge ----------
row_id_cols = [
    "prod_order_no", "prod_order_line_no", "machine_center_no",
    "operation_no", "item_no", "user_id", "created_on", "modified_on"
]
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

# -------- Merge or Create ----------
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
        .merge(df.alias("src"), "src.row_id <=> tgt.row_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

merge_or_create(TARGET, final_df)
print(f"Done: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Production Order Output

# CELL ********************

# ==========================================================
# Job: Gold_Production_Lakehouse.prod.gold_prod_order_output  (incremental)
# Mirrors the provided SQL:
#   SELECT DISTINCT ...
#   FROM Item Ledger (I)
#     LEFT JOIN gold_production_asgn_cell (C)  ON I.document_no = C.prod_order_no
#     LEFT JOIN prod_order_header (P)          ON I.document_no = P.prod_order_no
#     LEFT JOIN sales_header (S)               ON P.sales_order_no = S.sales_order_no
#   WHERE I.entry_type = 'Output' AND I.entry_type_item_location = 'FIN-GOODS'
# ==========================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, date

# ---------- Target ----------
TARGET = "Gold_Production_Lakehouse.prod.gold_prod_order_output"

# ---------- Sources ----------
IL_SRC  = "Silver_Inventory_Lakehouse.inv.silver_item_ledger"                  # I
GAC_SRC = "Gold_Production_Lakehouse.prod.gold_production_asgn_cell"           # C
POH_SRC = "Silver_Production_Lakehouse.prod.silver_prod_order_header"          # P
SOH_SRC = "Silver_Customer_Exp_Lakehouse.cx.silver_sales_header"               # S

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
    # Use posting_date as the watermark if available
    if "posting_date" in df.columns:
        return df.agg(F.max(F.to_date("posting_date")).alias("mx")).collect()[0]["mx"]
    return None

since_date = parse_date_or_none(since_param)
until_date = parse_date_or_none(until_param) or date.today()

if not full_reload and since_date is None and table_exists(TARGET):
    wm = pick_watermark_from_target(TARGET)
    if wm is not None:
        since_date = wm
if full_reload or since_date is None:
    since_date = date(1900, 1, 1)

print(f"Incremental window (by Item Ledger date): {since_date} -> {until_date}")

# ---------- Load ----------
I   = spark.table(IL_SRC).alias("I")
C   = spark.table(GAC_SRC).alias("C")
P   = spark.table(POH_SRC).alias("P")
SH  = spark.table(SOH_SRC).alias("S")

il_dt = F.to_date(F.coalesce(F.col("I.posting_date"), F.col("I.created_on")))

# ---------- Build exactly like the SQL (with DISTINCT) ----------
df = (
    I.filter(
        (F.col("I.entry_type") == F.lit("Output")) &
        (F.col("I.entry_type_item_location") == F.lit("FIN-GOODS")) &
        il_dt.isNotNull() &
        (il_dt >= F.lit(since_date)) &
        (il_dt <= F.lit(until_date))
    )
    .join(C,  F.col("I.document_no") == F.col("C.prod_order_no"), "left")
    .join(P,  F.col("I.document_no") == F.col("P.prod_order_no"), "left")
    .join(SH, F.col("P.sales_order_no") == F.col("S.sales_order_no"), "left")
    .select(
        F.col("S.sales_order_requested_date").alias("sales_order_requested_date"),
        F.col("I.posting_date").alias("posting_date"),
        F.col("S.customer_no").alias("customer_no"),
        F.col("S.customer_name").alias("customer_name"),
        F.col("P.sales_order_no").alias("sales_order_no"),
        F.col("I.document_no").alias("document_no"),
        F.col("I.order_no").alias("order_no"),
        F.col("I.order_lineno").alias("order_lineno"),
        F.col("I.item_no").alias("item_no"),
        F.col("I.item_description").alias("item_description"),
        F.col("I.entry_type_item_quantity").alias("entry_type_item_quantity"),
        F.col("C.cell_line").alias("cell_line"),
        F.col("C.prod_line").alias("prod_line"),
    )
    .distinct()
)

# ---------- Deterministic row_id for MERGE ----------
row_id_cols = [
    "document_no", "order_lineno", "item_no", "posting_date"
]

df = df.withColumn(
    "row_id",
    F.sha2(
        F.concat_ws(
            "||",
            *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in row_id_cols]
        ),
        256
    )
)

# ---------- Dedupe on row_id (avoid multiple source rows per target row) ----------
# Keep a single representative row per row_id
w = Window.partitionBy("row_id").orderBy(
    F.col("document_no"),
    F.col("order_no"),
    F.col("order_lineno"),
    F.col("item_no")
)

df = (
    df.withColumn("rn", F.row_number().over(w))
      .filter(F.col("rn") == 1)
      .drop("rn")
)

# ---------- Write (create or merge) ----------
def merge_or_create(target, df_):
    if not table_exists(target):
        print(f"Creating {target} ...")
        (
            df_.write
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
           .merge(df_.alias("src"), "tgt.row_id <=> src.row_id")
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute()
    )

merge_or_create(TARGET, df)
print(f"✅ Done: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# # Casting Header Line

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ------------------------------------------------------------------
# 1. Config
# ------------------------------------------------------------------
# Adjust to match your environment
silver_parts_table = "Silver_Production_Lakehouse.prod.silver_casting_parts"
silver_tree_table  = "Silver_Production_Lakehouse.prod.silver_casting_tree"

gold_table_name    = "Gold_Production_Lakehouse.prod.gold_casting_header_line"
# or simply: gold_table_name = "gold_casting_header_line"


# ------------------------------------------------------------------
# 2. Build the source dataframe (same logic as your view)
# ------------------------------------------------------------------
def build_source_df():
    p = spark.table(silver_parts_table).alias("p")
    t = spark.table(silver_tree_table).alias("t")

    df = (
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
            # Natural key you chose: prod order + line
            F.concat(F.col("p.prod_order_no"), F.col("p.prod_order_line_no")).alias("pol")
        )
    )
    return df


# ------------------------------------------------------------------
# 3. Find last loaded modified_on from the GOLD table (if it exists)
# ------------------------------------------------------------------
try:
    # Check if gold table exists by trying to read it
    gold_df = spark.table(gold_table_name)
    gold_exists = True
    max_modified_loaded = gold_df.agg(F.max("modified_on")).collect()[0][0]
except AnalysisException:
    # First run: table does not exist
    gold_exists = False
    max_modified_loaded = None


# ------------------------------------------------------------------
# 4. Get incremental rows from source
# ------------------------------------------------------------------
source_full_df = build_source_df()

if max_modified_loaded is not None:
    # only new/updated records
    source_inc_df = source_full_df.filter(
        F.col("modified_on") > F.lit(max_modified_loaded)
    )
else:
    # initial load
    source_inc_df = source_full_df

# ------------------------------------------------------------------
# 4b. Deduplicate source so MERGE has at most 1 row per pol
# ------------------------------------------------------------------
# Keep the latest record per pol by modified_on
w = Window.partitionBy("pol").orderBy(F.col("modified_on").desc())

source_inc_dedup = (
    source_inc_df
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# If there's nothing new, short-circuit
if source_inc_dedup.rdd.isEmpty():
    print("No new or updated records to load.")
else:
    # ------------------------------------------------------------------
    # 5. Merge into GOLD table
    # ------------------------------------------------------------------
    if not gold_exists:
        # First time: create the table
        (
            source_inc_dedup
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(gold_table_name)
        )
        print(f"Created and loaded gold table: {gold_table_name}")
    else:
        # Incremental upsert (update existing rows, insert new ones)
        delta_gold = DeltaTable.forName(spark, gold_table_name)

        (
            delta_gold.alias("t")
            .merge(
                source_inc_dedup.alias("s"),
                "t.pol = s.pol"      # natural key; ensure GOLD also has 1 row per pol
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(f"Incrementally updated gold table: {gold_table_name}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Casting Repair Status

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

# ---------------------------------------------------------
# 1. Config
# ---------------------------------------------------------
silver_hdr_tbl  = "Silver_Production_Lakehouse.prod.silver_prod_order_header"
silver_line_tbl = "Silver_Production_Lakehouse.prod.silver_prod_order_line"

gold_casting_header_tbl = "Gold_Production_Lakehouse.prod.gold_casting_header_line"
gold_repair_status_tbl  = "Gold_Production_Lakehouse.prod.gold_casting_repair_status"


# ---------------------------------------------------------
# 2. Build source dataframe (same logic as your SQL)
# ---------------------------------------------------------
def build_source_df():
    main = spark.table(silver_hdr_tbl).alias("main")
    ml   = spark.table(silver_line_tbl).alias("ml")
    wre  = spark.table(silver_hdr_tbl).alias("wre")
    wl   = spark.table(silver_line_tbl).alias("wl")
    c    = spark.table(gold_casting_header_tbl).alias("c")

    df = (
        main
        .join(ml, ml.prod_order_no == main.prod_order_no, "inner")
        .join(
            wre,
            (wre.ref_prod_order == main.prod_order_no) &
            (wre.prod_order_no.like("WRE%")),
            "inner"
        )
        .join(
            wl,
            (wl.prod_order_no == wre.prod_order_no) &
            (wl.item_no == ml.item_no),
            "inner"
        )
        .join(
            c,
            (c.prod_order_no == wre.prod_order_no) &
            (c.prod_order_line_no == wl.prod_order_line_no),
            "left"
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

            # Casting gold status
            F.col("c.casting_prod_order"),
            F.col("c.casting_tree_no"),
            F.col("c.casting_status"),
            F.col("c.casting_qty_passed"),
            F.col("c.casting_qty_reject")
        )
    )

    return df


# ---------------------------------------------------------
# 3. Detect last loaded watermark (using wre_created_on)
# ---------------------------------------------------------
try:
    gold_df = spark.table(gold_repair_status_tbl)
    gold_exists = True
    max_wre_created_loaded = gold_df.agg(F.max("wre_created_on")).collect()[0][0]
except AnalysisException:
    gold_exists = False
    max_wre_created_loaded = None


# ---------------------------------------------------------
# 4. Build incremental source
# ---------------------------------------------------------
source_full_df = build_source_df()

if max_wre_created_loaded is not None:
    source_inc_df = source_full_df.filter(
        F.col("wre_created_on") > F.lit(max_wre_created_loaded)
    )
else:
    # initial full load
    source_inc_df = source_full_df

if source_inc_df.rdd.isEmpty():
    print("No new or updated WRE records to load into gold_casting_repair_status.")
else:
    # -----------------------------------------------------
    # 5. Create or MERGE into gold_casting_repair_status
    # -----------------------------------------------------
    if not gold_exists:
        (
            source_inc_df
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(gold_repair_status_tbl)
        )
        print(f"Created and loaded: {gold_repair_status_tbl}")
    else:
        delta_gold = DeltaTable.forName(spark, gold_repair_status_tbl)

        (
            delta_gold.alias("t")
            .merge(
                source_inc_df.alias("s"),
                # natural key (one row per WRE order + line)
                "t.wre_prod_no = s.wre_prod_no AND t.wre_line_no = s.wre_line_no"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"Incrementally updated: {gold_repair_status_tbl}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
