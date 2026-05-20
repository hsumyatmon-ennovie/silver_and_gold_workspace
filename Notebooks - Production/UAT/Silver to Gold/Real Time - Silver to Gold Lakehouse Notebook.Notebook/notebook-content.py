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
# META           "id": "ff4d6787-a716-43b6-baaf-972b7426ffa5"
# META         },
# META         {
# META           "id": "ad99fdfa-85b1-4480-9f7f-2640bfd65f24"
# META         },
# META         {
# META           "id": "3a130b81-98ec-4fd4-a404-95edc1f0ef1e"
# META         },
# META         {
# META           "id": "869b263b-1a86-424b-bd97-94bd586442b2"
# META         },
# META         {
# META           "id": "e248ea90-8431-4df2-9f29-87866bf9dd5a"
# META         },
# META         {
# META           "id": "3ea0efcd-03d5-44f1-8e70-99f52a5c2a22"
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

# # One Time Run

# CELL ********************

from pyspark.sql import functions as F

# ---- 1) Load sources (Lakehouse) ----
emp_df = spark.table("Silver_Production_Lakehouse.prod.silver_employee_rfid_mapping")
wax_df = spark.table("Silver_Production_Lakehouse.prod.silver_wax_team")

# ---- 2) Build mkey like SQL Server CONCAT (NULL -> empty string) ----
# SQL Server CONCAT treats NULLs as '', so we use concat_ws('', ...) to mirror that.
emp_mapped = (
    emp_df.select(
        "created_on",
        "modified_on",
        "employee_rfid_mapping",
        "employee_id",
        "first_name",
        "last_name",
        "antenna_id",
        "cell_no",
        "machine_center_no",
        "reader_id",
    )
    .withColumn("mkey", F.concat_ws("", F.col("reader_id"), F.col("antenna_id")))
)

wax_mapped = (
    wax_df.select(
        "team",
        "reader_id",
        "antenna_id",
    )
    .withColumn("mkey", F.concat_ws("", F.col("reader_id"), F.col("antenna_id")))
)

# ---- 3) Left join on mkey ----
joined = (
    emp_mapped.alias("a")
    .join(wax_mapped.alias("b"), on="mkey", how="left")
    .select(
        "a.created_on",
        "a.modified_on",
        "a.employee_rfid_mapping",
        "a.employee_id",
        "a.first_name",
        "a.last_name",
        "a.antenna_id",
        "a.cell_no",
        "a.machine_center_no",
        "a.reader_id",
        "a.mkey",
        "b.team",
    )
)

# ---- 4) Save full table (materialized) ----
# Change the target name if you prefer a different schema/table.
joined.write.format("delta").mode("overwrite").saveAsTable("prod.gold_emp_wax_team_full")

# ---- 5) Also save the 'WAX ROOM' filtered table (as requested earlier) ----
joined.filter(F.col("cell_no") == "WAX ROOM") \
      .write.format("delta").mode("overwrite").saveAsTable("prod.gold_emp_wax_team")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # Production Status

# CELL ********************

# ==============================================================
# GOLD INCREMENTAL LOADER (with diagnostics)
# Source: Silver_Production_Lakehouse.prod.silver_prod_order_status
# Target: prod.gold_production_status (in this Gold lakehouse)
# ==============================================================

from pyspark.sql import functions as F, Window, types as T

SILVER = "Silver_Production_Lakehouse.prod.silver_prod_order_status"
TARGET = "prod.gold_production_status"
KEYS   = ["prod_order_no","item_no","CorrectCurrentLocation","type_name"]
MODCOL = "modified_on"
LOOKBACK_MIN = 60   # overlap on incremental

def table_exists(name):
    try: return spark.catalog.tableExists(name)
    except: return False

def get_last_ts(name, col):
    if not table_exists(name): return None
    r = spark.table(name).select(F.max(F.col(col))).first()
    return r[0] if r else None

def create_like(name, df):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {name.rsplit('.',1)[0]}")
    # create empty delta table with the same schema
    df.limit(0).write.format("delta").mode("overwrite").saveAsTable(name)

def maintain(name, zcols=None, vacuum_hours=168):
    z = ""
    if zcols:
        keep = [c for c in zcols if c in spark.table(name).columns]
        if keep: z = " ZORDER BY (" + ", ".join([f"`{c}`" for c in keep]) + ")"
    spark.sql(f"OPTIMIZE {name}{z}")
    spark.sql(f"VACUUM {name} RETAIN {vacuum_hours} HOURS")

# ---------- 1) READ ----------
src = spark.table(SILVER)
print(f"Silver rows: {src.count():,}")

# ---------- 2) TRANSFORM (replicate your view logic) ----------
# Select + compute columns
df = (src.select(
        "created_on","modified_on","prod_order_no", "operation_no",
        F.col("prod_order_line_no").cast("string").alias("prod_order_line_no"),
        "type_name","prod_order_status","open","sales_order_no",
        "current_location_code","past_location_code","employee_no","user_id",
        "quantity","remaining_quantity","item_no","machine_center_no",
     )
     .withColumn("out_qty", (-1*F.col("quantity")).cast("bigint"))
     .withColumn("pol", F.concat_ws("", F.col("prod_order_no"), F.col("prod_order_line_no")))
     .withColumn("created_on_time", F.col("created_on"))
)

# CorrectCurrentLocation (trim + rules)
df = df.withColumn("current_location_code", F.trim(F.col("current_location_code"))) \
       .withColumn("machine_center_no", F.trim(F.col("machine_center_no")))

df = df.withColumn(
    "CorrectCurrentLocation",
    F.when( (F.col("current_location_code").isNull()) | (F.length(F.col("current_location_code")) == 0),
            F.coalesce(F.when(F.length(F.col("machine_center_no")) > 0, F.col("machine_center_no")),
                       F.col("current_location_code"))
    ).when(F.upper(F.substring(F.col("current_location_code"), 1, 4)) == F.lit("CELL"),
            F.coalesce(F.when(F.length(F.col("machine_center_no")) > 0, F.col("machine_center_no")),
                       F.col("current_location_code"))
    ).otherwise(F.col("current_location_code"))
)

print(f"After calc: {df.count():,}")

# DEDUPE: latest by created_on (nulls last)
w = Window.partitionBy("prod_order_no","item_no","CorrectCurrentLocation","type_name") \
          .orderBy(F.col("created_on").desc_nulls_last())
df_dedup = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn")==1).drop("rn")

print(f"After dedupe partition/window: {df_dedup.count():,}")

# Normalize for robust filtering (trim/uppercase exacts)
df_norm = (df_dedup
    .withColumn("type_name_norm", F.upper(F.trim(F.col("type_name"))))
    .withColumn("open_norm", F.upper(F.trim(F.col("open"))))
    .withColumn("status_norm", F.upper(F.trim(F.col("prod_order_status"))))
)

final_src = (df_norm
    .filter( (F.col("type_name_norm") == F.lit("IN LOCATION IN")) &
             (F.col("open_norm")      == F.lit("YES")) &
             (F.col("status_norm")    == F.lit("RELEASED")) )
    .select(
        "created_on","modified_on","prod_order_no","prod_order_line_no",  "operation_no",
        "type_name","prod_order_status","open","sales_order_no",
        "current_location_code","past_location_code","employee_no","user_id",
        "quantity","remaining_quantity","item_no","machine_center_no",
        "out_qty","pol","created_on_time","CorrectCurrentLocation"
    )
)

print(f"After filters (In location in / Yes / Released): {final_src.count():,}")

# ---------- 3) INCREMENTAL WINDOW ----------
last_ts = get_last_ts(TARGET, MODCOL)
print(f"Target exists? {table_exists(TARGET)}; last {MODCOL} = {last_ts}")

staged = final_src
if last_ts is not None:
    staged = staged.filter(F.col(MODCOL) >= (F.lit(last_ts).cast("timestamp") - F.expr(f"INTERVAL {LOOKBACK_MIN} MINUTES")))
print(f"Staged rows (after incremental window): {staged.count():,}")

# ---------- 4) CREATE TARGET IF MISSING ----------
if not table_exists(TARGET):
    create_like(TARGET, final_src)
    print(f"Created empty Delta table: {TARGET}")

# ---------- 5) CHANGE DETECTION (fix first-run issue) ----------
# IMPORTANT: if target is empty, _row_hash_t will be NULL; we must treat NULL as "different".
tgt = spark.table(TARGET)
target_cols = tgt.columns

# Left side hash
compare_cols = [c for c in final_src.columns if c not in KEYS]
s_hash = F.sha2(F.concat_ws("§", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in compare_cols]), 256)
staged_h = staged.withColumn("_row_hash", s_hash)

# Right side hash (may be empty)
t_compare_cols = [c for c in target_cols if c in compare_cols]
t_hash = F.sha2(F.concat_ws("§", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in t_compare_cols]), 256)
target_h = tgt.select(*KEYS, t_hash.alias("_row_hash_t"))

# Fix: include rows where _row_hash_t IS NULL (new keys) OR hashes differ
to_apply = (
    staged_h.join(target_h, on=KEYS, how="left")
            .filter( F.col("_row_hash_t").isNull() | (F.col("_row_hash") != F.col("_row_hash_t")) )
            .select(*[c for c in staged_h.columns if c in target_cols])
)

print(f"Rows to MERGE (new or changed): {to_apply.count():,}")

if to_apply.limit(1).count() == 0:
    print("[Gold] No changes to apply.")
else:
    tmp = "stg_gold_production_status"
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

    print("[Gold] MERGE complete.")
    # Optional: speed up queries on keys + modified_on

# # Final sanity check
# spark.sql(f"SELECT COUNT(*) AS rows_in_gold FROM {TARGET}").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Production Casting Status

# CELL ********************

# ==============================================================
# GOLD: prod.gold_production_casting_status — Incremental Loader
# (with hard source-dedupe and optional target cleanup)
# ==============================================================

from pyspark.sql import functions as F, Window

# ------- CONFIG (edit if needed) -------
PO = "Silver_Production_Lakehouse.prod.silver_prod_order_header"
PL = "Silver_Production_Lakehouse.prod.silver_prod_order_line"
S  = "Silver_Production_Lakehouse.prod.silver_prod_order_status"
CP = "Silver_Production_Lakehouse.prod.silver_casting_parts"
CT = "Silver_Production_Lakehouse.prod.silver_casting_tree"
IT = "Silver_Inventory_Lakehouse.inv.silver_item"

TARGET  = "prod.gold_production_casting_status"

# Business key (must be 1:1 at the target grain)
KEYS    = ["prod_order_no", "prod_order_line_no", "casting_prod_order", "casting_tree_no"]

# Unified modified col for incremental and tie-breaking
MODCOL  = "_modified_any"
LOOKBACK_MIN = 90  # minutes overlap window

# ------- helpers -------
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
    if zcols:
        cols_csv = ", ".join([f"`{c}`" for c in zcols])
        try:
            spark.sql(f"ANALYZE TABLE {name} COMPUTE STATISTICS FOR COLUMNS {cols_csv}")
            spark.sql(f"OPTIMIZE {name} ZORDER BY ({cols_csv})")
        except Exception as e:
            print(f"Z-Order notice: {e}")
            spark.sql(f"OPTIMIZE {name}")
    else:
        spark.sql(f"OPTIMIZE {name}")
    spark.sql(f"VACUUM {name} RETAIN {vacuum_hours} HOURS")

def count_dupes(df, keys):
    return (df.groupBy(*[F.col(k) for k in keys])
              .count().filter(F.col("count") > 1).count())

def show_dupes(df, keys, n=20):
    d = (df.groupBy(*[F.col(k) for k in keys])
           .count().filter(F.col("count") > 1)
           .orderBy(F.col("count").desc()))
    c = d.count()
    print(f">>> duplicate key groups: {c}")
    if c:
        d.show(n, truncate=False)

def dedupe_target_in_place(target_table: str, keys: list, freshness_col: str = None):
    """Run only once if your target already contains duplicate keys."""
    if not table_exists(target_table):
        return
    df = spark.table(target_table)
    ords = []
    if freshness_col and freshness_col in df.columns:
        ords.append(F.col(freshness_col).cast("timestamp").desc_nulls_last())
    if "load_ts" in df.columns:
        ords.append(F.col("load_ts").cast("timestamp").desc_nulls_last())
    # deterministic tie-breaker
    ords.append(F.sha2(F.concat_ws("§", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in df.columns]), 256).desc())
    w = Window.partitionBy(*keys).orderBy(*ords)
    deduped = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn")==1).drop("_rn")
    before, after = df.count(), deduped.count()
    if after < before:
        print(f"[TARGET CLEANUP] {target_table}: removed {before-after:,} duplicate rows")
        (deduped.write
               .format("delta")
               .mode("overwrite")
               .option("overwriteSchema","true")
               .saveAsTable(target_table))
        try:
            spark.sql(f"OPTIMIZE {target_table}")
        except Exception as e:
            print(f"OPTIMIZE notice: {e}")

# ------- 1) read silver sources -------
po = spark.table(PO)
pl = spark.table(PL)
s  = spark.table(S)
cp = spark.table(CP)
ct = spark.table(CT)
it = spark.table(IT)

print(f"po={po.count():,}  pl={pl.count():,}  s={s.count():,}  cp={cp.count():,}  ct={ct.count():,}  it={it.count():,}")

# ------- 2) prod_order CTE (po + pl) -------
prod_order = (
    po.alias("po")
      .join(pl.alias("pl"), F.col("po.prod_order_no")==F.col("pl.prod_order_no"), how="left")
      .select(
          F.col("po.sales_order_no").alias("sales_order_no"),
          F.col("po.sales_order_line_no").alias("sales_order_line_no"),
          F.col("po.prod_order_no").alias("prod_order_no"),
          F.col("pl.prod_order_line_no").alias("prod_order_line_no"),
          F.col("po.FG_item_no").alias("FG_item_no"),
          F.col("po.item_routing_no").alias("item_routing_no"),
          F.col("po.prod_order_starting_date_time").alias("prod_order_starting_date_time"),
          F.col("po.prod_order_ending_date_time").alias("prod_order_ending_date_time"),
          F.col("po.prod_order_due_date").alias("prod_order_due_date"),
          F.weekofyear("po.prod_order_due_date").alias("commit_week"),
          F.col("pl.prod_line_due_date").alias("prod_line_due_date"),
          F.col("po.prod_order_finished_date").alias("prod_order_finished_date"),
          F.col("po.prod_order_quantity").alias("prod_order_quantity"),
          F.col("po.prod_order_status").alias("prod_order_status"),
          F.col("po.ref_prod_order").alias("ref_prod_order"),
          F.col("po.ref_item").alias("ref_item"),
          F.col("pl.prod_line_start_date").alias("prod_line_start_date"),
          F.col("pl.prod_line_end_date").alias("prod_line_end_date"),
          F.col("pl.prod_line_quantity").alias("prod_line_quantity"),
          F.col("pl.prod_line_finished_quantity").alias("prod_line_finished_quantity"),
          F.col("pl.prod_line_remaining_quantity").alias("prod_line_remaining_quantity"),
          F.col("pl.item_location").alias("item_location"),
          F.col("pl.item_no").alias("prod_item_line"),
          F.concat_ws("", F.col("po.sales_order_no"), F.col("po.sales_order_line_no")).alias("SOL"),
          F.concat_ws("", F.col("po.prod_order_no"), F.col("pl.prod_order_line_no").cast("string")).alias("POL"),
          # watermarks
          F.col("po.modified_on").alias("po_modified_on"),
          F.col("pl.modified_on").alias("pl_modified_on"),
      )
)

# ------- 3) LatestStatus (silver_production_status) dedup + filters -------
latest_status = (
    s.select(
        F.col("prod_order_no"),
        F.col("prod_order_line_no"),
        F.col("created_on"),
        F.trim(F.col("current_location_code")).alias("current_location_code"),
        F.trim(F.col("machine_center_no")).alias("machine_center_no"),
        F.col("type_name"),
        F.col("open"),
        F.col("prod_order_status").alias("pos_status"),
        F.col("modified_on").alias("s_modified_on"),
    )
    .withColumn(
        "Prod_Status",
        F.when(F.col("current_location_code").isNull() | (F.length("current_location_code")==0),
               F.col("machine_center_no"))
         .when(F.upper(F.substring(F.col("current_location_code"),1,4))=="CELL", F.col("machine_center_no"))
         .otherwise(F.col("current_location_code"))
    )
)

w_stat = Window.partitionBy("prod_order_no","prod_order_line_no","type_name","Prod_Status") \
               .orderBy(F.col("created_on").desc_nulls_last())

dedup_status = (latest_status
    .withColumn("_rn", F.row_number().over(w_stat))
    .filter(F.col("_rn")==1)
    .drop("_rn")
)

dedup_status_f = (
    dedup_status
      .withColumn("type_name_norm", F.upper(F.trim(F.col("type_name"))))
      .withColumn("open_norm",      F.upper(F.trim(F.col("open"))))
      .withColumn("status_norm",    F.upper(F.trim(F.col("pos_status"))))
      .filter( (F.col("type_name_norm")=="IN LOCATION IN") &
               (F.col("open_norm")=="YES") &
               (F.col("status_norm")=="RELEASED") )
      .select(
          "prod_order_no","prod_order_line_no",
          F.col("Prod_Status").alias("Prod_Status"),
          "s_modified_on"
      )
)

# ------- 4) Join prod_order + latest status (explicit list to avoid ambiguity) -------
prod_with_latest = (
    prod_order.alias("po")
      .join(
          dedup_status_f.alias("ls"),
          on=[F.col("po.prod_order_no")==F.col("ls.prod_order_no"),
              F.col("po.prod_order_line_no")==F.col("ls.prod_order_line_no")],
          how="left"
      )
      .select(
          "po.sales_order_no","po.sales_order_line_no","po.prod_order_no","po.prod_order_line_no",
          "po.FG_item_no","po.item_routing_no","po.prod_order_starting_date_time","po.prod_order_ending_date_time",
          "po.prod_order_due_date","po.commit_week","po.prod_line_due_date","po.prod_order_finished_date",
          "po.prod_order_quantity","po.prod_order_status","po.ref_prod_order","po.ref_item",
          "po.prod_line_start_date","po.prod_line_end_date","po.prod_line_quantity",
          "po.prod_line_finished_quantity","po.prod_line_remaining_quantity",
          "po.item_location","po.prod_item_line","po.SOL","po.POL",
          "po.po_modified_on","po.pl_modified_on",
          "ls.Prod_Status","ls.s_modified_on"
      )
)

# ------- 5) Join inventory + casting parts + casting tree -------
prod_joined = (
    prod_with_latest.alias("pl")
      .join(it.alias("it"), F.col("pl.prod_item_line")==F.col("it.item_no"), how="left")
      .join(cp.alias("cp"),
            on=[F.col("pl.prod_order_no")==F.col("cp.prod_order_no"),
                F.col("pl.prod_order_line_no")==F.col("cp.prod_order_line_no")],
            how="left")
      .join(ct.alias("ct"),
            on=[F.col("cp.casting_prod_order")==F.col("ct.casting_prod_order")],
            how="left")
      .select(
          "pl.*",
          F.col("it.item_category").alias("itemFG_Category"),
          F.col("cp.item_no").alias("itemCST"),
          F.col("cp.casting_prod_order").alias("casting_prod_order"),
          F.col("cp.casting_qty_to_tree").alias("casting_qty_to_tree"),
          F.col("cp.casting_qty_passed").alias("casting_qty_passed"),
          F.col("cp.casting_qty_reject").alias("casting_qty_reject"),
          F.col("ct.casting_tree_no").alias("casting_tree_no"),
          F.col("ct.casting_status").alias("casting_status"),
          # more watermarks
          F.col("cp.modified_on").alias("cp_modified_on"),
          F.col("ct.modified_on").alias("ct_modified_on"),
          F.col("it.modified_on").alias("it_modified_on"),
      )
)

# Filters like your SQL view
prod_filtered = (
    prod_joined
      .filter(
          (F.col("prod_order_status").isNull() | F.col("prod_order_status").isin("Released","Finished")) &
          (F.col("itemFG_Category").isNull() | F.col("itemFG_Category").isin("FG","CASTING","SEMI-FG"))
      )
)

# Final Status (match your CASE logic)
prod_final = (
    prod_filtered
      .withColumn(
          "Status",
          F.when(F.col("casting_status").isNotNull(), F.col("casting_status"))
           .when(F.col("Prod_Status").isNotNull(), F.col("Prod_Status"))
           .when(F.col("itemFG_Category")=="CASTING", F.lit("WAX"))
           .otherwise(F.lit("RELEASED"))
      )
)

# Final columns + MODCOL
final_cols = [
    "sales_order_no","sales_order_line_no","prod_order_no","prod_order_line_no",
    "FG_item_no","item_routing_no","prod_order_starting_date_time","prod_order_ending_date_time",
    "prod_order_due_date","commit_week","prod_line_due_date","prod_order_finished_date",
    "prod_order_quantity","prod_order_status","ref_prod_order","ref_item",
    "prod_line_start_date","prod_line_end_date","prod_line_quantity","prod_line_finished_quantity",
    "prod_line_remaining_quantity","item_location","prod_item_line","SOL","POL",
    "Prod_Status","itemFG_Category","itemCST","casting_prod_order","casting_qty_to_tree",
    "casting_qty_passed","casting_qty_reject","casting_tree_no","casting_status","Status"
]

result_all = (
    prod_final
      .withColumn(
          MODCOL,
          F.greatest(
              F.col("po_modified_on"),
              F.col("pl_modified_on"),
              F.col("s_modified_on"),
              F.col("cp_modified_on"),
              F.col("ct_modified_on"),
              F.col("it_modified_on")
          )
      )
      .select(*final_cols, MODCOL)
      .dropDuplicates()
)

print("Rows before incremental window:", result_all.count())

# ------- 6) Incremental window -------
last_ts = get_last_ts(TARGET, MODCOL)
staged = result_all
if last_ts is not None:
    staged = staged.filter(
        F.col(MODCOL) >= (F.lit(last_ts).cast("timestamp") - F.expr(f"INTERVAL {LOOKBACK_MIN} MINUTES"))
    )
print("Rows staged:", staged.count())

# ------- 7) Create target if missing -------
if not table_exists(TARGET):
    create_like(TARGET, result_all)
    print(f"Created {TARGET}")

# ------- 8) SOURCE DEDUPE BY KEYS (mandatory to avoid MERGE error) -------
# Fill any missing key columns as NULLs of proper type
for k in KEYS:
    if k not in staged.columns:
        staged = staged.withColumn(k, F.lit(None).cast("string"))

# Keep the most recent per KEYS (order by MODCOL then deterministic hash)
order_cols = [
    F.col(MODCOL).cast("timestamp").desc_nulls_last(),
    F.sha2(F.concat_ws("§", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in staged.columns]), 256).desc()
]
w_keys = Window.partitionBy(*KEYS).orderBy(*order_cols)

staged_dedup = (staged
    .withColumn("_rn", F.row_number().over(w_keys))
    .filter(F.col("_rn")==1)
    .drop("_rn")
)

print("Source dup groups BEFORE:", count_dupes(staged, KEYS))
print("Source dup groups AFTER :", count_dupes(staged_dedup, KEYS))

# ------- 9) Optional: one-time target cleanup if it already has duplicates -------
# dedupe_target_in_place(TARGET, KEYS, freshness_col=MODCOL)

# ------- 10) Hash compare + MERGE -------
tgt = spark.table(TARGET)
target_cols = tgt.columns

compare_cols = [c for c in staged_dedup.columns if c not in KEYS]
s_hash = F.sha2(F.concat_ws("§", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in compare_cols]), 256)
staged_h = staged_dedup.withColumn("_row_hash", s_hash)

t_hash = F.sha2(F.concat_ws("§", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in target_cols if c in compare_cols]), 256)
target_h = tgt.select(*KEYS, t_hash.alias("_row_hash_t"))

to_apply = (
    staged_h.join(target_h, on=KEYS, how="left")
            .filter(F.col("_row_hash_t").isNull() | (F.col("_row_hash") != F.col("_row_hash_t")))
            .select(*[c for c in staged_h.columns if c in target_cols])
)

print("Rows to MERGE (after hash-compare):", to_apply.count())

if to_apply.limit(1).count() == 0:
    print("No changes to apply.")
else:
    tmp = "stg_gold_production_casting_status"
    to_apply.createOrReplaceTempView(tmp)

    # Use COALESCE(…, '__NULL__') equality to avoid null-safe multi-match on <=> with NULLs
    def on_eq(k): 
        return f"coalesce(t.{k}, '__NULL__') = coalesce(s.{k}, '__NULL__')"
    on_expr  = " AND ".join([on_eq(k) for k in KEYS])

    set_expr = ", ".join([f"t.{c}=s.{c}" for c in target_cols if c not in set(KEYS)])

    spark.sql(f"""
        MERGE INTO {TARGET} t
        USING {tmp} s
        ON {on_expr}
        WHEN MATCHED THEN UPDATE SET {set_expr}
        WHEN NOT MATCHED THEN INSERT ({", ".join(target_cols)})
        VALUES ({", ".join([f"s.{c}" for c in target_cols])})
    """)


print("Done.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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

# # Shipment Plan Status

# CELL ********************

# ==============================================================
# GOLD: gold_shipment_plan_status — Incremental Loader (fixed)
# ==============================================================

from pyspark.sql import functions as F, Window

# ---------- CONFIG: source tables ----------
S_SH   = "Silver_Customer_Exp_Lakehouse.cx.silver_sales_header"
S_SL   = "Silver_Customer_Exp_Lakehouse.cx.silver_sales_line"
S_CU   = "Silver_Customer_Exp_Lakehouse.cx.silver_customer"

S_PO   = "Silver_Production_Lakehouse.prod.silver_prod_order_header"
S_PL   = "Silver_Production_Lakehouse.prod.silver_prod_order_line"
S_RL   = "Silver_Production_Lakehouse.prod.silver_prod_routing_line"
S_CELL = "Silver_Production_Lakehouse.prod.silver_cell_list"
S_PS   = "Silver_Production_Lakehouse.prod.silver_prod_order_status"
S_CP   = "Silver_Production_Lakehouse.prod.silver_casting_parts"
S_CT   = "Silver_Production_Lakehouse.prod.silver_casting_tree"
S_IT   = "Silver_Inventory_Lakehouse.inv.silver_item"

TARGET = "Gold_Production_Lakehouse.prod.gold_shipment_plan_status"

# ---------- Merge keys & watermark ----------
KEYS         = ["salesorder_no","sales_line_no"]   # business grain
MODCOL       = "_modified_any"
LOOKBACK_MIN = 90  # minutes overlap

# ---------- helpers ----------
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

def hash_row(cols):
    return F.sha2(F.concat_ws("§", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]), 256)

def show_dupe_keys(df, keys, limit=10):
    dupes = (df.groupBy(*[F.col(k) for k in keys]).count()
               .filter(F.col("count") > 1)
               .orderBy(F.col("count").desc()))
    c = dupes.count()
    print(f"Duplicate key groups in staged source: {c}")
    if c:
        dupes.show(limit, truncate=False)

# Optional one-time target cleanup if you suspect duplicates in TARGET
def dedupe_target_in_place(target_table: str, keys: list, freshness_col: str = None):
    tgt_df = spark.table(target_table)
    if freshness_col and freshness_col in tgt_df.columns:
        ords = [F.col(freshness_col).cast("timestamp").desc_nulls_last()]
    else:
        ords = [F.col("load_ts").desc_nulls_last()] if "load_ts" in tgt_df.columns else \
               [F.sha2(F.concat_ws("§", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tgt_df.columns]), 256).desc()]
    w = Window.partitionBy(*keys).orderBy(*ords)
    deduped = (tgt_df.withColumn("_rn", F.row_number().over(w))
                      .filter(F.col("_rn") == 1)
                      .drop("_rn"))
    before = tgt_df.count(); after = deduped.count()
    if after < before:
        print(f"[TARGET CLEANUP] {target_table}: removed {before-after:,} duplicate rows")
        (deduped.write
           .format("delta")
           .mode("overwrite")
           .option("overwriteSchema","true")
           .saveAsTable(target_table))
        try:
            spark.sql(f"OPTIMIZE {target_table}")
        except Exception as e:
            print(f"OPTIMIZE notice: {e}")

# ---------- 1) load sources ----------
sh   = spark.table(S_SH)
sl   = spark.table(S_SL)
cu   = spark.table(S_CU)

po   = spark.table(S_PO)
pl   = spark.table(S_PL)
rl   = spark.table(S_RL)
cell = spark.table(S_CELL)
ps   = spark.table(S_PS)
cp   = spark.table(S_CP)
ct   = spark.table(S_CT)
it   = spark.table(S_IT)

print(f"sh={sh.count():,}  sl={sl.count():,}  cu={cu.count():,}  po={po.count():,}  pl={pl.count():,}  rl={rl.count():,}  ps={ps.count():,}  cp={cp.count():,}  ct={ct.count():,}  it={it.count():,}")

# ---------- 2) Sales CTE ----------
sales = (
    sh.alias("sh")
      .join(sl.alias("sl"), F.col("sh.sales_order_no")==F.col("sl.sales_order_no"), "left")
      .join(cu.alias("c"),  F.col("c.customer_no")==F.col("sh.customer_no"), "left")
      .where(
          (F.col("sh.sales_order_status").isin("Open","Released","Pending Approval","Pending Prepayment","Closed")) &
          (F.col("sh.sales_order_type")=="Order")
      )
      .select(
          F.col("sh.sales_order_no").alias("salesorder_no"),
          F.col("sh.customer_no").alias("cus_no"),
          F.col("sh.customer_name").alias("cus_name"),
          F.col("c.customer_abbreviation").alias("cus_abbr"),
          F.col("sh.sales_order_status").alias("status_so"),
          F.col("sh.sales_order_requested_date").alias("req_date"),
          F.col("sh.sales_order_promised_date").alias("pm_date"),
          F.col("sh.sales_order_cs_reference").alias("cs_noted"),
          F.col("sh.cs_team").alias("cs_team"),
          F.col("sh.sales_order_external_document").alias("cs_document"),
          F.col("sh.sales_order_released_date").alias("sales_order_released_date"),
          F.weekofyear(F.col("sh.sales_order_requested_date")).alias("shipment_week"),
          F.col("sl.sales_order_line_no").alias("sales_line_no"),
          F.col("sl.item_no").alias("fg"),
          F.col("sl.item_description").alias("fg_description"),
          F.col("sl.item_quantity").alias("total_qty"),
          F.col("sl.sales_order_shipment_date").alias("line_shipment_date"),
          F.col("sl.item_material").alias("fg_type"),
          (F.coalesce(F.col("sl.item_quantity"),F.lit(0.0)) - F.coalesce(F.col("sl.item_quantity_shipped"),F.lit(0.0))).cast("decimal(18,4)").alias("outstanding_qty"),
          # possible watermarks
          F.col("sh.modified_on").cast("timestamp").alias("_mod_sh"),
          F.col("sl.modified_on").cast("timestamp").alias("_mod_sl"),
          F.col("c.modified_on").cast("timestamp").alias("_mod_cu"),
      )
)

# ---------- 3) ProdOrder CTE (order + line) ----------
prod_order = (
    po.alias("po")
      .join(pl.alias("pl"), F.col("po.prod_order_no")==F.col("pl.prod_order_no"), "left")
      .select(
          F.col("po.sales_order_no"),
          F.col("po.sales_order_line_no"),
          F.col("po.prod_order_no"),
          F.col("pl.prod_order_line_no").alias("prod_order_line_no"),
          F.col("po.FG_item_no"),
          F.col("po.item_routing_no"),
          F.col("po.prod_order_starting_date_time"),
          F.col("po.prod_order_ending_date_time"),
          F.col("po.prod_order_due_date"),
          F.weekofyear(F.col("po.prod_order_due_date")).alias("commit_week"),
          F.col("pl.prod_line_due_date"),
          F.col("po.prod_order_finished_date"),
          F.col("po.prod_order_quantity"),
          F.col("po.prod_order_status"),
          F.col("po.ref_prod_order"),
          F.col("po.ref_item"),
          F.col("pl.prod_line_start_date"),
          F.col("pl.prod_line_end_date"),
          F.col("pl.prod_line_quantity"),
          F.col("pl.prod_line_finished_quantity"),
          F.col("pl.prod_line_remaining_quantity"),
          F.col("pl.item_location"),
          F.col("pl.item_no").alias("prod_item_line"),
          # watermarks
          F.col("po.modified_on").cast("timestamp").alias("_mod_po"),
          F.col("pl.modified_on").cast("timestamp").alias("_mod_pl"),
      )
)

# ---------- 4) Routing (pick first CELL per PO + line; push CELL108 last) ----------
rl_cell = (
    rl.filter(F.col("routing_no").startswith("CELL") & (F.col("prod_order_line_no")==F.lit(10000)))
      .select("prod_order_no","item_no","prod_order_line_no","routing_no")
      .withColumn("is_cell108", F.when(F.col("routing_no")=="CELL108", F.lit(1)).otherwise(F.lit(0)))
      .withColumn("cell_num", F.regexp_extract(F.col("routing_no"), r"^CELL(\d+)$", 1).cast("int"))
)

w_route = Window.partitionBy("prod_order_no","prod_order_line_no") \
                .orderBy(F.col("is_cell108").asc(), F.col("cell_num").asc_nulls_last(), F.col("routing_no").asc())

routing_first = (
    rl_cell.withColumn("rn", F.row_number().over(w_route))
           .filter(F.col("rn")==1)
           .select(
               F.col("prod_order_no"),
               F.col("item_no"),
               F.col("prod_order_line_no"),
               F.col("routing_no").alias("first_cell_no")
           )
)

pr = (
    prod_order.alias("po")
      .join(routing_first.alias("r"),
            (F.col("po.prod_order_no")==F.col("r.prod_order_no")) &
            (F.col("po.prod_order_line_no")==F.col("r.prod_order_line_no")),
            "left")
      .join(cell.alias("c"), F.col("r.first_cell_no")==F.col("c.cell_line"), "left")
      .select(
          F.col("po.*"),
          F.col("r.first_cell_no"),
          F.col("c.cell_line"),
          F.col("c.prod_line"),
      )
)

# ---------- 5) LatestStatus from silver_production_status ----------
latest_status_base = (
    ps.select(
        "prod_order_no",
        F.col("prod_order_line_no").cast("int").alias("prod_order_line_no_int"),
        "created_on",
        "current_location_code",
        "machine_center_no",
        "type_name",
        "open",
        "prod_order_status",
        F.col("modified_on").cast("timestamp").alias("_mod_ps")
    )
    .where(
        (F.col("type_name")=="In location in") &
        (F.col("open")=="Yes") &
        (F.col("prod_order_status")=="Released")
    )
)

w_stat = Window.partitionBy("prod_order_no","prod_order_line_no_int","current_location_code","machine_center_no") \
               .orderBy(F.col("created_on").desc())

latest_status = (
    latest_status_base
      .withColumn("rn", F.row_number().over(w_stat))
      .filter(F.col("rn")==1)
      .select(
          "prod_order_no",
          "prod_order_line_no_int",
          F.col("created_on").alias("status_created_on_utc"),
          F.col("current_location_code").alias("current_location_code_latest"),
          "machine_center_no",
          "_mod_ps"
      )
)

# ---------- 6) ProdWithLatest ----------
pwl = (
    pr.alias("pr")
      .join(latest_status.alias("ls"),
            (F.col("ls.prod_order_no")==F.col("pr.prod_order_no")) &
            (F.col("ls.prod_order_line_no_int")==F.col("pr.prod_order_line_no")),
            "left")
      .select(
          F.col("pr.*"),
          F.col("ls.status_created_on_utc"),
          F.col("ls.current_location_code_latest"),
          F.col("ls.machine_center_no"),
          F.col("ls._mod_ps")
      )
)

# ---------- 7) Casting + Item ----------
casting = (
    cp.alias("cp")
      .join(ct.alias("ct"), F.col("cp.casting_prod_order")==F.col("ct.casting_prod_order"), "left")
      .select(
          F.col("cp.prod_order_no"),
          F.col("cp.prod_order_line_no").alias("prod_order_line_no"),
          F.col("cp.item_no").alias("item_cas"),
          F.col("cp.casting_prod_order"),
          F.col("cp.casting_qty_to_tree").alias("cas_qty"),
          F.col("cp.casting_qty_passed").alias("cas_qty_passed"),
          F.col("cp.casting_qty_reject").alias("cas_qty_reject"),
          F.col("ct.casting_tree_no").alias("cas_tree"),
          F.col("ct.casting_status").alias("cas_status"),
          F.col("cp.modified_on").cast("timestamp").alias("_mod_cp"),
          F.col("ct.modified_on").cast("timestamp").alias("_mod_ct"),
      )
)

items = it.select(
    F.col("item_no"),
    F.col("item_category").alias("itemfg_category")
)

production = (
    pwl.alias("p")
      .join(items.alias("it"), F.col("it.item_no")==F.col("p.ref_item"), "left")
      .join(casting.alias("ca"),
            (F.col("p.prod_order_no")==F.col("ca.prod_order_no")) &
            (F.col("p.prod_order_line_no")==F.col("ca.prod_order_line_no")),
            "left")
      .where(
          (F.col("p.prod_order_status").isNull() | F.col("p.prod_order_status").isin("Released","Finished")) &
          (F.col("it.itemfg_category").isNull() | F.col("it.itemfg_category").isin("FG","CASTING","SEMI-FG"))
      )
      .select(
          F.col("p.*"),
          F.col("it.itemfg_category"),
          F.col("ca.item_cas"),
          F.col("ca.casting_prod_order"),
          F.col("ca.cas_qty"),
          F.col("ca.cas_qty_passed"),
          F.col("ca.cas_qty_reject"),
          F.col("ca.cas_tree"),
          F.col("ca.cas_status"),
          F.col("ca._mod_cp"),
          F.col("ca._mod_ct"),
      )
)

# ---------- 8) Final SELECT (snake_case, no spaces) ----------
w_fg = Window.partitionBy("prod_order_no")
w_cast_due = Window.partitionBy("prod_order_no","ref_item")

result = (
    sales.alias("s")
      .join(production.alias("p"),
            (F.col("s.salesorder_no")==F.col("p.sales_order_no")) &
            (F.col("s.sales_line_no")==F.col("p.sales_order_line_no")),
            "left")
      .select(
          # requested / commit weeks
          F.col("s.shipment_week").alias("requested_week"),
          F.col("p.commit_week").alias("commit_week"),
          # due_in / due_status
          F.when(F.col("p.prod_order_due_date").isNull(), F.lit(None))
           .when(F.datediff(F.current_date(), F.col("p.prod_order_due_date")) > 0,
                 F.concat(F.lit("Overdue "), F.abs(F.datediff(F.current_date(), F.col("p.prod_order_due_date"))), F.lit("d")))
           .otherwise(F.concat(F.lit("Due "), F.datediff(F.col("p.prod_order_due_date"), F.current_date()), F.lit("d"))).alias("due_in"),
          F.when(F.col("p.prod_order_due_date").isNull(), F.lit(None))
           .when(F.datediff(F.current_date(), F.col("p.prod_order_due_date")) > 0, F.lit("Overdue"))
           .when(F.datediff(F.col("p.prod_order_due_date"), F.current_date()) <= 3, F.lit("At risk"))
           .otherwise(F.lit("On time")).alias("due_status"),

          # lines / cells
          F.col("p.prod_line").alias("line"),
          F.col("p.first_cell_no").alias("cell"),

          # sales basics
          F.col("s.cus_abbr").alias("customer"),
          F.col("s.status_so").alias("status_so"),
          F.col("s.salesorder_no").alias("salesorder_no"),
          F.expr("concat(substring(salesorder_no,1,2), right(salesorder_no,4))").alias("so_abbr"),
          F.col("s.sales_line_no").alias("sales_line_no"),
          F.col("s.fg").alias("fg"),
          F.col("s.fg_description").alias("fg_description"),
          F.col("s.cs_team").alias("cs_team"),
          F.col("s.cs_noted").alias("cs_noted"),
          F.col("s.cs_document").alias("cs_document"),
          F.col("s.fg_type").alias("fg_type"),
          F.col("s.total_qty").alias("total_qty"),
          F.col("s.outstanding_qty").alias("outstanding_qty"),

          # production
          F.col("p.prod_order_no").alias("prod"),
          F.col("p.prod_order_due_date").alias("prod_due_date"),
          F.max(F.when(F.col("p.prod_order_line_no")==F.lit(10000), F.col("p.prod_order_starting_date_time"))).over(w_fg).alias("fg_start_date"),
          F.max(F.when(F.col("p.item_location")==F.lit("CST_CUT"), F.col("p.prod_line_due_date"))).over(w_cast_due).alias("casting_due_date"),
          F.col("p.prod_order_line_no").alias("prod_line"),
          F.col("p.prod_item_line").alias("item"),
          F.col("p.item_cas").alias("item_cas"),

          # prod status normalize
          F.when(F.trim(F.col("p.current_location_code_latest"))=="", F.col("p.machine_center_no"))
           .when(F.upper(F.substring(F.col("p.current_location_code_latest"),1,4))=="CELL", F.col("p.machine_center_no"))
           .otherwise(F.col("p.current_location_code_latest")).alias("prod_status"),

          # qtys
          F.col("p.prod_order_quantity").alias("prod_qty"),
          F.col("p.prod_line_remaining_quantity").alias("prod_remaining_qty"),
          F.col("p.prod_line_finished_quantity").alias("prod_finished_qty"),

          # casting side
          F.col("p.casting_prod_order").alias("prod_cas"),
          F.col("p.cas_tree").alias("cas_tree"),
          F.col("p.cas_qty").alias("cas_qty"),
          F.col("p.cas_status").alias("cas_status"),

          # SOL
          F.concat(F.col("s.salesorder_no"), F.col("s.sales_line_no").cast("string")).alias("sol"),

          # forward possible modification columns for watermark
          F.col("_mod_sh"),F.col("_mod_sl"),F.col("_mod_cu"),
          F.col("_mod_po"),F.col("_mod_pl"),F.col("_mod_ps"),
          F.col("_mod_cp"),F.col("_mod_ct"),
      )
)

# ---------- 9) Watermark column (cast to timestamp, handle 1+ candidates) ----------
candidates = [c for c in ["_mod_sh","_mod_sl","_mod_cu","_mod_po","_mod_pl","_mod_ps","_mod_cp","_mod_ct"] if c in result.columns]
if len(candidates) == 0:
    result = result.withColumn(MODCOL, F.current_timestamp())
elif len(candidates) == 1:
    result = result.withColumn(MODCOL, F.col(candidates[0]).cast("timestamp"))
else:
    result = result.withColumn(MODCOL, F.greatest(*[F.col(c).cast("timestamp") for c in candidates]))

# ---------- 10) Incremental stage ----------
last_ts = get_last_ts(TARGET, MODCOL)
staged = result
if last_ts is not None:
    staged = staged.filter(F.col(MODCOL) >= (F.lit(last_ts).cast("timestamp") - F.expr(f"INTERVAL {LOOKBACK_MIN} MINUTES")))

print("Rows staged:", staged.count())

# ---------- 11) Create target once ----------
if not table_exists(TARGET):
    create_like(TARGET, result.drop(*candidates))
    print(f"Created {TARGET}")

# Align to target columns
tgt = spark.table(TARGET)
target_cols = tgt.columns
staged_use = staged.select(*[c for c in staged.columns if c in target_cols])

# ---------- 12) Hash compare to minimize updates ----------
compare_cols = [c for c in staged_use.columns if c not in KEYS]
staged_h = staged_use.withColumn("_row_hash", hash_row(compare_cols))

t_hash = hash_row([c for c in target_cols if c in compare_cols])
target_h = tgt.select(*KEYS, t_hash.alias("_row_hash_t"))

to_apply = (
    staged_h.join(target_h, on=KEYS, how="left")
            .filter(F.col("_row_hash_t").isNull() | (F.col("_row_hash") != F.col("_row_hash_t")))
            .select(*[c for c in staged_use.columns])  # drop hash cols
)

print("Rows to MERGE (pre-dedupe):", to_apply.count())

# ---------- 13) SOURCE DEDUPE before MERGE (fixes multiple-source-match) ----------
show_dupe_keys(to_apply, KEYS)

# keep newest per key by MODCOL, tie-break by deterministic hash
order_cols = [
    F.col(MODCOL).desc_nulls_last(),
    F.sha2(F.concat_ws("§", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in to_apply.columns]), 256).desc()
]
w_keys = Window.partitionBy(*KEYS).orderBy(*order_cols)

to_merge = (to_apply
    .withColumn("_rn", F.row_number().over(w_keys))
    .filter(F.col("_rn")==1)
    .drop("_rn")
)

print("Rows to MERGE (deduped):", to_merge.count())

# ---------- 14) MERGE ----------
if to_merge.limit(1).count() == 0:
    print("No changes to apply.")
else:
    tmp = "stg_gold_shipment_plan_status"
    to_merge.createOrReplaceTempView(tmp)

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

# ---------- (Optional) One-time target cleanup ----------
# dedupe_target_in_place(TARGET, KEYS, freshness_col=MODCOL)

# ---------- sanity ----------
# spark.sql(f"SELECT COUNT(*) AS rows_in_gold FROM {TARGET}").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # Sales Summary

# CELL ********************

# ==============================================================
# GOLD: gold_sales_summary — Incremental Loader (clean version)
# ==============================================================

from pyspark.sql import functions as F, Window

# --------- CONFIG ----------
S_POL = "Silver_Production_Lakehouse.prod.silver_prod_order_line"     # silver prod lines
G_SPS = "Gold_Production_Lakehouse.prod.gold_shipment_plan_status"    # gold shipment plan status

TARGET = "Gold_Production_Lakehouse.prod.gold_sales_summary"

# business grain for MERGE: exactly one row per (SO Abbr x Prod x Customer)
KEYS         = ["so_abbr", "prod", "customer"]   # tighten/loosen as needed
MODCOL       = "_modified_any"
LOOKBACK_MIN = 90

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
    db = name.rsplit(".", 1)[0]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {db}")
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

def hash_row(cols):
    return F.sha2(F.concat_ws("§", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]), 256)

def show_dupe_keys(df, keys, limit=10):
    dupes = (df.groupBy(*[F.col(k) for k in keys]).count()
               .filter(F.col("count") > 1)
               .orderBy(F.col("count").desc()))
    c = dupes.count()
    print(f"Duplicate key groups in source: {c}")
    if c:
        dupes.show(limit, truncate=False)

# (Optional, run manually once if you suspect target already has dupes)
def dedupe_target_in_place(target_table: str, keys: list, freshness_col: str = None):
    tgt_df = spark.table(target_table)
    if freshness_col and freshness_col in tgt_df.columns:
        ords = [F.col(freshness_col).cast("timestamp").desc_nulls_last()]
    else:
        ords = [F.col("load_ts").desc_nulls_last()] if "load_ts" in tgt_df.columns else \
               [F.sha2(F.concat_ws("§", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tgt_df.columns]), 256).desc()]
    w = Window.partitionBy(*keys).orderBy(*ords)
    deduped = (tgt_df.withColumn("_rn", F.row_number().over(w))
                      .filter(F.col("_rn") == 1)
                      .drop("_rn"))
    before = tgt_df.count()
    after  = deduped.count()
    if after < before:
        print(f"[TARGET CLEANUP] {target_table}: removed {before-after:,} duplicate rows")
        (deduped.write
           .format("delta")
           .mode("overwrite")
           .option("overwriteSchema", "true")
           .saveAsTable(target_table))
        try:
            spark.sql(f"OPTIMIZE {target_table}")
        except Exception as e:
            print(f"OPTIMIZE notice: {e}")

# --------- 1) Load sources ----------
pol = spark.table(S_POL)  # expect: prod_order_no, item_no, item_uom, item_location, prod_line_quantity, prod_line_finished_quantity, modified_on
gps = spark.table(G_SPS)  # expect snake_case columns: so_abbr, customer, due_in, due_status, prod, _modified_any, ...

print(f"pol={pol.count():,}  gps={gps.count():,}")

# --------- 2) ps CTE (aggregate production lines by prod_order_no) ----------
ps = (
    pol
      .filter(~F.col("item_no").startswith("M"))
      .filter(F.col("item_uom") == F.lit("PCS"))
      .select(
          "prod_order_no", "item_no", "item_uom",
          F.lower(F.col("item_location")).alias("_loc"),
          F.col("prod_line_quantity").cast("double").alias("_qty"),
          F.col("prod_line_finished_quantity").cast("double").alias("_qty_fin"),
          F.col("modified_on").cast("timestamp").alias("_mod_pol")
      )
      .groupBy("prod_order_no","item_no","item_uom")
      .agg(
          F.sum(F.when(F.col("_loc").isin("casting","cst_cut"), F.coalesce(F.col("_qty"), F.lit(0.0))).otherwise(0.0)).alias("total_casting_qty"),
          F.sum(F.when(F.col("_loc").isin("casting","cst_cut"), F.coalesce(F.col("_qty_fin"), F.lit(0.0))).otherwise(0.0)).alias("total_casting_finished_qty"),
          F.sum(F.when(F.col("_loc")=="fin-goods", F.coalesce(F.col("_qty"), F.lit(0.0))).otherwise(0.0)).alias("total_production_qty"),
          F.sum(F.when(F.col("_loc")=="fin-goods", F.coalesce(F.col("_qty_fin"), F.lit(0.0))).otherwise(0.0)).alias("total_production_finished_qty"),
          F.sum(F.when(~F.col("_loc").isin("casting","fin-goods","cst_cut"), F.coalesce(F.col("_qty"), F.lit(0.0))).otherwise(0.0)).alias("total_semi_qty"),
          F.sum(F.when(~F.col("_loc").isin("casting","fin-goods","cst_cut"), F.coalesce(F.col("_qty_fin"), F.lit(0.0))).otherwise(0.0)).alias("total_semi_finished_qty"),
          F.max("_mod_pol").alias("_mod_ps")  # timestamp
      )
)

# --------- 3) so_prod_counts (distinct prods per SO abbr) ----------
so_prod_counts = (
    gps.where(F.col("prod").isNotNull())
       .groupBy("so_abbr")
       .agg(
           F.countDistinct("prod").alias("total_prod_orders"),
           F.max(F.col("_modified_any").cast("timestamp")).alias("_mod_gps_counts")
       )
)

# --------- 4) Join gps + ps + counts ----------
base = (
    gps.select(
            "so_abbr","customer","due_in","due_status","prod",
            F.col("_modified_any").cast("timestamp").alias("_mod_gps_row")
        )
       .join(ps.select(
                F.col("prod_order_no").alias("prod"),
                "total_casting_qty","total_casting_finished_qty",
                "total_production_qty","total_production_finished_qty",
                "total_semi_qty","total_semi_finished_qty",
                "_mod_ps"
            ),
            on="prod", how="left")
       .join(so_prod_counts, on="so_abbr", how="left")
)

# --------- 5) Percentages ----------
def pct(numer, denom):
    return (
        F.when(F.col(denom).isNull() | (F.col(denom) == 0), F.lit(0.0))
         .otherwise( (F.col(numer).cast("double") / F.col(denom).cast("double")) * 100.0 )
    )

casting_pct    = pct("total_casting_finished_qty",    "total_casting_qty").alias("casting")
production_pct = pct("total_production_finished_qty", "total_production_qty").alias("production")
semi_pct       = pct("total_semi_finished_qty",       "total_semi_qty").alias("semi")

completion = (
    (pct("total_casting_finished_qty","total_casting_qty") +
     pct("total_production_finished_qty","total_production_qty") +
     pct("total_semi_finished_qty","total_semi_qty")) / F.lit(3.0)
).alias("completion")

result = (
    base.select(
        "so_abbr","customer","due_in","due_status","prod","total_prod_orders",
        casting_pct, production_pct, semi_pct, completion,
        "_mod_gps_row","_mod_ps","_mod_gps_counts"
    )
    .withColumn("casting",    F.round(F.col("casting"),    2).cast("decimal(10,2)"))
    .withColumn("production", F.round(F.col("production"), 2).cast("decimal(10,2)"))
    .withColumn("semi",       F.round(F.col("semi"),       2).cast("decimal(10,2)"))
    .withColumn("completion", F.round(F.col("completion"), 2).cast("decimal(10,2)"))
)

# --------- 6) Watermark ----------
present = [c for c in ["_mod_gps_row","_mod_ps","_mod_gps_counts"] if c in result.columns]
if len(present) == 0:
    staged_all = result.withColumn(MODCOL, F.current_timestamp())
elif len(present) == 1:
    staged_all = result.withColumn(MODCOL, F.col(present[0]).cast("timestamp"))
else:
    staged_all = result.withColumn(MODCOL, F.greatest(*[F.col(c).cast("timestamp") for c in present]))

# --------- 7) Incremental stage ----------
last_ts = get_last_ts(TARGET, MODCOL)
staged = staged_all
if last_ts is not None:
    staged = staged.filter(F.col(MODCOL) >= (F.lit(last_ts).cast("timestamp") - F.expr(f"INTERVAL {LOOKBACK_MIN} MINUTES")))
print("Rows staged:", staged.count())

# --------- 8) Create target if missing ----------
if not table_exists(TARGET):
    create_like(
        TARGET,
        staged.select(
            "so_abbr","customer","due_in","due_status","prod","total_prod_orders",
            "casting","production","semi","completion", MODCOL
        )
    )
    print(f"Created {TARGET}")

# Align to target columns
tgt = spark.table(TARGET)
target_cols = tgt.columns
staged_use = staged.select(*[c for c in staged.columns if c in target_cols])

# --------- 9) Hash compare to minimize updates ----------
compare_cols = [c for c in staged_use.columns if c not in KEYS]
staged_h = staged_use.withColumn("_row_hash", hash_row(compare_cols))

t_hash = hash_row([c for c in target_cols if c in compare_cols])
target_h = tgt.select(*KEYS, t_hash.alias("_row_hash_t"))

to_apply = (
    staged_h.join(target_h, on=KEYS, how="left")
            .filter(F.col("_row_hash_t").isNull() | (F.col("_row_hash") != F.col("_row_hash_t")))
            .select(*[c for c in staged_use.columns])  # drop hashes
)

print("Rows to MERGE (pre-dedupe):", to_apply.count())

# --------- 10) Source dedupe before MERGE ----------
show_dupe_keys(to_apply, KEYS)

# keep newest per key (by MODCOL), tie-break by deterministic hash
order_cols = [F.col(MODCOL).desc_nulls_last(),
              F.sha2(F.concat_ws("§", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in to_apply.columns]), 256).desc()]

w_keys = Window.partitionBy(*KEYS).orderBy(*order_cols)
to_merge = (to_apply
    .withColumn("_rn", F.row_number().over(w_keys))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

print("Rows to MERGE (deduped):", to_merge.count())

# --------- 11) MERGE ----------
if to_merge.limit(1).count() == 0:
    print("No changes to apply.")
else:
    tmp = "stg_gold_sales_summary"
    to_merge.createOrReplaceTempView(tmp)

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

# --------- 12) sanity ----------
spark.sql(f"SELECT COUNT(*) AS rows_in_gold FROM {TARGET}").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # Production Assign Cell

# CELL ********************

# ==============================================================
# GOLD: gold_production_asgn_cell — Incremental Loader (fixed)
# ==============================================================

from pyspark.sql import functions as F, Window

# --------- CONFIG (adjust names if needed) ----------
S_ROUTING = "Silver_Production_Lakehouse.prod.silver_prod_routing_line"
S_CELLS   = "Silver_Production_Lakehouse.prod.silver_cell_list"

TARGET    = "Gold_Production_Lakehouse.prod.gold_production_asgn_cell"

KEYS         = ["prod_order_no", "prod_order_line_no"]   # grain
MODCOL       = "_modified_any"
LOOKBACK_MIN = 90

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
        spark.sql(f"OPTIMIZE {name}")
    spark.sql(f"VACUUM {name} RETAIN {vacuum_hours} HOURS")

def hash_row(cols):
    return F.sha2(F.concat_ws("§", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]), 256)

# --------- 1) Load sources ----------
rl = spark.table(S_ROUTING)
cl = spark.table(S_CELLS)

print(f"routing_lines={rl.count():,}  cell_list={cl.count():,}")

# --------- 2) Filter and rank routing rows ----------
# Expect columns: prod_order_no, prod_order_line_no, routing_no, modified_on (string/timestamp)
rl_f = (
    rl.filter(F.col("prod_order_line_no") == 10000)
      .filter(F.col("routing_no").startswith("CELL") | F.col("routing_no").startswith("OUTSOURCE"))
      .select(
          "prod_order_no","item_no","prod_order_line_no","routing_no",
          F.col("modified_on").alias("_mod_rl_raw")
      )
)

# numeric suffix: CELL123 -> 123
suffix_str = F.regexp_extract(F.col("routing_no"), r"^CELL(\d+)$", 1)
suffix_int = F.when(F.length(suffix_str) > 0, suffix_str.cast("int"))

w_rank = Window.partitionBy("prod_order_no","prod_order_line_no").orderBy(
    F.when(F.col("routing_no") == "CELL108", F.lit(1)).otherwise(F.lit(0)).asc(),
    F.when(suffix_int.isNull(), F.lit(1)).otherwise(F.lit(0)).asc(),
    suffix_int.asc_nulls_last(),
    F.col("routing_no").asc()
)

ranked = (
    rl_f.withColumn("_suffix_int", suffix_int)
        .withColumn("_rn", F.row_number().over(w_rank))
)

chosen = (
    ranked.filter(F.col("_rn") == 1)
          .select(
              "prod_order_no","item_no","prod_order_line_no",
              F.col("routing_no").alias("cell_line"),
              F.col("_mod_rl_raw")
          )
)

# --------- 3) Join to cell list ----------
cells = cl.select(
    F.col("cell_line").alias("cell_line_join"),
    F.col("prod_line"),
    # if no modified_on in cell list, produce a null timestamp
    (F.col("modified_on").cast("timestamp") if "modified_on" in cl.columns else F.lit(None).cast("timestamp")).alias("_mod_cl_ts")
)

joined = (
    chosen.join(cells, chosen.cell_line == cells.cell_line_join, "left")
          .select(
              "prod_order_no","item_no","prod_order_line_no","cell_line","prod_line",
              "_mod_rl_raw","_mod_cl_ts"
          )
)

# --------- 4) Watermark column (cast to timestamp first) ----------
mod_rl_ts = F.col("_mod_rl_raw").cast("timestamp")  # works for both string/timestamp inputs
present_ts = [mod_rl_ts, F.col("_mod_cl_ts")]

# build MODCOL safely: if both null, fallback current_timestamp
joined = joined.withColumn(
    MODCOL,
    F.when(
        F.greatest(*[F.lit(0), F.lit(0)]) == F.lit(0),  # dummy to satisfy syntax; replaced below
        F.current_timestamp()
    )
)

# replace with proper logic since greatest() needs homogeneous types and >=1 arg
if len(present_ts) == 2:
    staged_all = joined.drop(MODCOL).withColumn(MODCOL, F.greatest(present_ts[0], present_ts[1]))
else:
    # should not happen here, but keep guard
    staged_all = joined.drop(MODCOL).withColumn(MODCOL, mod_rl_ts)

# --------- 5) Incremental stage ----------
last_ts = get_last_ts(TARGET, MODCOL)
staged = staged_all
if last_ts is not None:
    staged = staged.filter(F.col(MODCOL) >= (F.lit(last_ts).cast("timestamp") - F.expr(f"INTERVAL {LOOKBACK_MIN} MINUTES")))

print("Rows staged:", staged.count())

# --------- 6) Create target if missing ----------
target_cols_order = ["prod_order_no","item_no","prod_order_line_no","cell_line","prod_line", MODCOL]
if not table_exists(TARGET):
    create_like(TARGET, staged.select(*target_cols_order))
    print(f"Created {TARGET}")

tgt = spark.table(TARGET)
target_cols = tgt.columns
staged_use = staged.select(*[c for c in target_cols if c in staged.columns])

# --------- 7) Hash compare ----------
compare_cols = [c for c in staged_use.columns if c not in KEYS]
staged_h = staged_use.withColumn("_row_hash", hash_row(compare_cols))

t_hash = hash_row([c for c in target_cols if c in compare_cols])
target_h = tgt.select(*KEYS, t_hash.alias("_row_hash_t"))

to_apply = (
    staged_h.join(target_h, on=KEYS, how="left")
            .filter(F.col("_row_hash_t").isNull() | (F.col("_row_hash") != F.col("_row_hash_t")))
            .select(*[c for c in staged_use.columns])
)

print("Rows to MERGE:", to_apply.count())

# --------- 8) MERGE ----------
if to_apply.limit(1).count() == 0:
    print("No changes to apply.")
else:
    tmp = "stg_gold_production_asgn_cell"
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


# --------- 9) sanity ----------
# spark.sql(f"SELECT COUNT(*) AS rows_in_gold FROM {TARGET}").show()


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
S_TEAM = "Gold_Production_Lakehouse.prod.gold_emp_wax_team"
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
    .join(team.alias("w"), F.col("p.employee_no") == F.col("w.employee_id"), "left")
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

# ❗ fixed: stray space removed in the catalog path
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
    .withColumn(
        "sales_order",
        F.coalesce(F.col("sales_order_no"),
                   F.first("sales_order_no", ignorenulls=True).over(
                       Window.partitionBy("ref_prod_order")
                   ))
    )
    .withColumn(
        "fg_start_date",
        F.to_date(F.max(F.when(F.col("prod_order_line_no")==F.lit(10000), F.col("prod_line_start_date")))
                    .over(w_fg))
    )
    .withColumn(
        "fg_due_date",
        F.max(F.when(F.col("prod_order_line_no")==F.lit(10000), F.col("prod_line_due_date")))
         .over(w_fg)
    )
    .withColumn(
        "casting_due_date",
        F.max(F.when(F.col("item_location")==F.lit("CST_CUT"), F.col("prod_line_due_date")))
         .over(w_cast)
    )
    .withColumn(
        "casting_start_date",
        F.to_date(F.max(F.when(F.col("item_location")==F.lit("CST_CUT"), F.col("prod_line_start_date")))
                    .over(w_cast))
    )
    # Optional filter if you only want certain header statuses:
    # .where(F.col("prod_order_status").isin("Released","Firm Planned"))
)

# --------- 3) EmployeeData (Gold status) ----------
emp = gs.select(
    "created_on","modified_on","prod_order_no","prod_order_line_no","type_name",
    "prod_order_status","open","sales_order_no","current_location_code",
    "past_location_code","employee_no","user_id","quantity","remaining_quantity",
    "item_no","machine_center_no","out_qty","pol","created_on_time","CorrectCurrentLocation", "team"
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

          # ❗ FIX: prefer order status from production order, fallback to events/status table
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

          # Keep raw casting_status, but provide a user-facing status with default
          F.when(
                F.col("c.casting_status").isNotNull() & (F.trim(F.col("c.casting_status")) != ""),
                F.trim(F.col("c.casting_status"))
            ).when(
                F.col("e.machine_center_no").isNotNull() & (F.trim(F.col("e.machine_center_no")) != ""),
                F.trim(F.col("e.machine_center_no"))
            ).otherwise(
                F.lit("Not Start")
            ).alias("new_status"),

          F.coalesce(F.col("p.sales_order"), F.col("e.sales_order_no")).alias("new_so"),

          F.when(F.col("c.casting_qty_to_tree").isNotNull() & (F.col("c.casting_qty_to_tree") != 0),
                 F.col("c.casting_qty_to_tree")
           ).otherwise(F.col("p.prod_line_quantity")).alias("new_qty"),

          F.row_number().over(Window.partitionBy("p.prod_order_no","p.prod_item_line")
                              .orderBy(F.col("p.prod_order_line_no").desc())
                         ).alias("rn")
      )
)

# --------- 8) Final SELECT + window aggregates ----------
w_agg_item  = Window.partitionBy("prod_order_no","prod_item_line")
w_agg_order = Window.partitionBy("prod_order_no")

result = (
    final_base
      .filter(F.col("rn")==1)
      .filter(F.col("prod_item_line").like("C%"))
      .filter(F.col("prod_line_remaining_quantity") > 0)
      # normalize status for consistent comparisons
      .withColumn("status_lc", F.lower(F.trim(F.col("new_status"))))
      .select(
          "prod_order_no","prod_order_line_no","FG_item_no","prod_item_line","item_location",
          "fg_start_date","fg_due_date","casting_start_date","casting_due_date", "prod_order_status",
          "current_location_code","CorrectCurrentLocation","machine_center_no","team", "casting_prod_order",
          F.trim(F.regexp_replace(F.col("casting_tree_no"), "TREE No\\.", "")).alias("tree_no"),
          "itemCST","casting_qty_to_tree","casting_qty_passed","casting_qty_reject","casting_status",
          "CustomerNo","CustomerAbbr","metal_category","new_status",
          F.expr("concat(substring(new_so,1,2), right(new_so,4))").alias("so_abbr"),
          "new_qty",
          F.sum("new_qty").over(w_agg_item).alias("total_qty"),

          # use normalized status for Lakehouse/in_wh calc
          F.sum(
              F.when(F.col("status_lc").isin("finished","complete"), F.col("casting_qty_to_tree")).otherwise(F.lit(0))
          ).over(w_agg_item).alias("in_wh"),

          # remaining qty: consistent status checks in both branches
          F.when(
              (F.sum("new_qty").over(w_agg_item) -
               F.sum(F.when(F.col("status_lc").isin("finished","complete"), F.col("casting_qty_to_tree")).otherwise(F.lit(0)))
                .over(w_agg_item)) == 0,
              F.lit(None)
           ).otherwise(
              (F.sum("new_qty").over(w_agg_item) -
               F.sum(F.when(F.col("status_lc").isin("finished","complete"), F.col("casting_qty_to_tree")).otherwise(F.lit(0)))
                .over(w_agg_order))
           ).alias("remaining_qty"),

          F.concat(F.col("prod_order_no"), F.col("prod_item_line")).alias("poi"),

          # carry potential mod columns if they exist
          F.col("modified_on").alias("_mod_emp")
      )
)

# --------- 9) SAFE watermark (no greatest() error) ----------
# Add more candidates here if you later carry *_modified_on from other sources.
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

# # Plating

# CELL ********************

# Databricks / PySpark 3.4+
from pyspark.sql import functions as F, Window as W

# ============================================================
# Config
# ============================================================
CAT_GOLD = "Gold_Production_Lakehouse.prod"
CAT_SILV = "Silver_Production_Lakehouse.prod"
CAT_CMN  = "Silver_Commons_Lakehouse.cmn"

SRC_PROD_ORDER     = f"{CAT_GOLD}.gold_production_order"
SRC_STATUS_GOLD    = f"{CAT_GOLD}.gold_production_status"
SRC_STATUS_SILVER  = f"{CAT_SILV}.silver_prod_order_status"   # <-- use created_on from HERE
SRC_ROUTING_LINE   = f"{CAT_SILV}.silver_prod_routing_line"
SRC_STEP_MAP       = f"{CAT_CMN}.silver_prod_step_casting_production"

TARGET_TABLE       = "prod.gold_plating_eta_per_line"
WATERMARK_TABLE    = "prod._watermark_plating_eta"

DEFAULT_LOOKBACK_DAYS    = 30
CUMU_TO_PLATING_MAX_DAYS = 10.0
SESSION_TZ               = "Asia/Bangkok"

# If your timestamp columns are stored in UTC, keep True. If already local (BKK), set False.
IS_SILVER_CREATED_ON_UTC = True
IS_GOLD_CREATED_ON_UTC   = True

# ============================================================
# Session settings
# ============================================================
spark.conf.set("spark.sql.session.timeZone", SESSION_TZ)

# ============================================================
# Helpers
# ============================================================
def uptrim(col):
    return F.upper(F.trim(col))

def to_seqno(col):
    # normalize: '010.01' or '10,01' -> 10.01
    return F.regexp_replace(F.trim(col), ",", ".").cast("double")

def add_days_fraction(base_ts_col, days_col):
    # base_ts_col: timestamp; days_col: numeric
    return F.to_timestamp(
        F.from_unixtime(
            F.unix_timestamp(base_ts_col) + (days_col.cast("double") * F.lit(86400.0))
        )
    )

# Fixed durations — the ONLY source of step duration
duration_map = F.create_map(
    [F.lit(k) for kv in [
        ('FIL',2.0),('HT',1.0),('TUM',1.0),('LAS',1.0),('SET',1.0),
        ('POL',2.0),('SHI',1.0),('PLT',1.0),('GLU',2.0),('QC',1.0),
        ('QA',1.0),('C INS',1.0),('PCK',1.0)
    ] for k in kv]
)

allowed_abbr = ['FIL','HT','TUM','LAS','SET','POL','SHI','PLT','GLU','QC','QA','C INS','PCK']
allowed_df = spark.createDataFrame([(a,) for a in allowed_abbr], ["abbr"])

# ============================================================
# Watermark read/init
# ============================================================
if spark._jsparkSession.catalog().tableExists(WATERMARK_TABLE):
    wm_ts = spark.table(WATERMARK_TABLE).select("last_processed_ts").head()[0]
else:
    wm_ts = spark.range(1).select((F.current_timestamp() - F.expr(f"INTERVAL {DEFAULT_LOOKBACK_DAYS} DAYS")).alias("last_processed_ts")).head()[0]
    (spark.createDataFrame([(wm_ts,)], ["last_processed_ts"])
         .write.mode("overwrite").format("delta").saveAsTable(WATERMARK_TABLE))

# ============================================================
# Load sources
# ============================================================
prod_order = spark.table(SRC_PROD_ORDER).select(
    "prod_order_no", "prod_order_line_no", "prod_item_line",
    "prod_line_quantity", "prod_order_status", "prod_line_start_date"
)

# GOLD status (structure for matching ops & locations)
status_gold_raw = spark.table(SRC_STATUS_GOLD).select(
    "prod_order_no", "prod_order_line_no", "operation_no",
    F.col("CorrectCurrentLocation").alias("StatusRoutingNo"),
    F.col("created_on").alias("created_on_gold")
)

status_gold = (status_gold_raw
    .withColumn(
        "created_on_gold_local",
        F.from_utc_timestamp("created_on_gold", SESSION_TZ) if IS_GOLD_CREATED_ON_UTC else F.col("created_on_gold")
    )
)

# SILVER status (we only trust/use created_on here)
status_silver_raw = spark.table(SRC_STATUS_SILVER).select(
    "prod_order_no", "prod_order_line_no", "operation_no",
    F.col("created_on").alias("created_on_silver")
)

status_silver = (status_silver_raw
    .withColumn(
        "created_on_silver_local",
        F.from_utc_timestamp("created_on_silver", SESSION_TZ) if IS_SILVER_CREATED_ON_UTC else F.col("created_on_silver")
    )
)

routing_line = spark.table(SRC_ROUTING_LINE).select(
    "prod_order_no", "prod_order_line_no", "item_no", "operation_no",
    "routing_no", "operation_type"
)

# step_map
step_map_norm = spark.table(SRC_STEP_MAP).select(
    uptrim("current_operation").alias("MapKey"),
    uptrim("operation_description").alias("DescKey"),
    uptrim("operation_group").alias("OpGroup"),
    uptrim("operation_abb").alias("OpAbb"),
    # OffsetDays NOT used for durations
    F.col("Pro Due Date Offset_Day_").cast("decimal(10,2)").alias("OffsetDays"),
    F.col("current_operation").alias("CurrentOperationRaw")
)

# ============================================================
# Incremental filter (use SILVER created_on for "what changed")
# ============================================================
changed_orders = (status_silver
    .where(F.col("created_on_silver_local") >= F.lit(wm_ts))
    .select("prod_order_no").distinct()
)

changed_orders2 = (prod_order
    .where(F.col("prod_line_start_date") >= (F.lit(wm_ts).cast("timestamp") - F.expr("INTERVAL 7 DAYS")))
    .select("prod_order_no").distinct()
)

changed = changed_orders.unionByName(changed_orders2).distinct()

# ============================================================
# Short-circuit if nothing to do
# ============================================================
if changed.isEmpty():
    print("No changes since watermark; nothing to do.")
    now_ts = spark.range(1).select(F.current_timestamp().alias("last_processed_ts"))
    now_ts.write.mode("overwrite").format("delta").saveAsTable(WATERMARK_TABLE)
else:
    # Reduce sources to changed orders
    prod_order_chg = prod_order.join(changed, "prod_order_no")
    status_gold_chg   = status_gold.join(changed, "prod_order_no")
    status_silver_chg = status_silver.join(changed, "prod_order_no")
    routing_chg    = routing_line.join(changed, "prod_order_no")

    # ========================================================
    # 0) Orders/Line/Item & Start Dates
    # ========================================================
    line0 = (prod_order_chg
        .where(F.col("prod_order_status") == F.lit("Released"))
        # .where(F.col("prod_order_no") == F.lit("WRO250900963"))
        .where(~F.col("prod_order_no").like("C%"))  # drop C* orders
        .withColumn("StartDatePlan", F.to_date("prod_line_start_date"))  # session TZ
        .withColumn(
            "rn",
            F.row_number().over(
                W.partitionBy("prod_order_no", "prod_order_line_no", "prod_item_line")
                 .orderBy(F.col("prod_line_start_date").desc())
            )
        )
    )

    # Actual start: earliest SILVER created_on per order line (fallback to GOLD if no silver)
    silver_first = (status_silver_chg
        .where(F.col("operation_no").isNotNull() & (F.length(F.trim("operation_no")) > 0))
        .groupBy("prod_order_no","prod_order_line_no")
        .agg(F.min("created_on_silver_local").alias("StartTSActual_silver"))
    )

    gold_first = (status_gold_chg
        .where(F.col("operation_no").isNotNull() & (F.length(F.trim("operation_no")) > 0))
        .groupBy("prod_order_no","prod_order_line_no")
        .agg(F.min("created_on_gold_local").alias("StartTSActual_gold"))
    )

    start_from_status = (silver_first
        .join(gold_first, ["prod_order_no","prod_order_line_no"], "full")
        .withColumn("StartTSActual",
            F.coalesce(F.col("StartTSActual_silver"), F.col("StartTSActual_gold"))
        )
        .select(
            "prod_order_no","prod_order_line_no",
            "StartTSActual",
            F.to_date("StartTSActual").alias("StartDateActual")
        )
    )

    line = (line0.where(F.col("rn")==1)
        .join(start_from_status, ["prod_order_no","prod_order_line_no"], "left")
        .select("prod_order_no","prod_order_line_no","prod_item_line","prod_line_quantity",
                "StartDatePlan","StartTSActual","StartDateActual")
    )

    # ========================================================
    # 1) Routing → normalize/map
    # ========================================================
    mc_name = step_map_norm.select(
        uptrim("CurrentOperationRaw").alias("mc_key"),
        F.col("CurrentOperationRaw").alias("mc_name_raw")
    ).distinct()

    routing_raw = (routing_chg
        .join(line.select("prod_order_no","prod_order_line_no").distinct(), ["prod_order_no","prod_order_line_no"])
        .where(uptrim("operation_type") == F.lit("MACHINE CENTER"))
        .select(
            "prod_order_no","prod_order_line_no","item_no",
            to_seqno(F.col("operation_no")).alias("SeqNo"),
            "routing_no"
        )
        .join(mc_name, uptrim("routing_no") == mc_name.mc_key, "left")
        .select(
            "prod_order_no","prod_order_line_no","item_no","SeqNo",
            F.coalesce(F.col("mc_name_raw"), F.col("routing_no")).alias("OpName"),
            F.col("routing_no").alias("RoutingCode")
        )
    )

    routing_norm = routing_raw.withColumn("OpNameU", uptrim("OpName"))

    # Map by exact current_operation then fallback contains(description)
    map_exact = step_map_norm.select("MapKey","OpGroup","OpAbb").withColumnRenamed("MapKey","k")
    rn_exact = (routing_norm
        .join(map_exact, routing_norm.OpNameU == map_exact.k, "left")
        .select(routing_norm["*"],
                map_exact["OpGroup"].alias("MappedGroup_exact"),
                map_exact["OpAbb"].alias("MappedAbbrev_exact"))
    )

    map_fallback = (step_map_norm
        .where(F.length("DescKey")>0)
        .select("DescKey","OpGroup","OpAbb").withColumnRenamed("DescKey","d")
    )

    rn_fb = (rn_exact
        .join(map_fallback, rn_exact.OpNameU.isNotNull() & map_fallback.d.contains(rn_exact.OpNameU), "left")
        .select(rn_exact["*"],
                map_fallback["OpGroup"].alias("MappedGroup_fb"),
                map_fallback["OpAbb"].alias("MappedAbbrev_fb"))
    )

    routing_map = (rn_fb
        .withColumn("MappedGroup", F.coalesce("MappedGroup_exact","MappedGroup_fb"))
        .withColumn("MappedAbbrev", F.coalesce("MappedAbbrev_exact","MappedAbbrev_fb"))
        .select("prod_order_no","prod_order_line_no","item_no","SeqNo","RoutingCode","OpName","OpNameU","MappedGroup","MappedAbbrev")
    )

    routing_allowed = (routing_map
        .join(allowed_df, uptrim("MappedAbbrev")==allowed_df.abbr, "inner")
        .drop("abbr")
    )

    # ========================================================
    # 2) Duration + cumulative (ONLY duration_map; cap 10)
    # ========================================================
    routing_dur = (routing_allowed
        .withColumn(
            "DurationDays",
            F.round(
                F.coalesce(F.element_at(duration_map, uptrim("MappedAbbrev")), F.lit(0.0)), 2
            ).cast("decimal(10,2)")
        )
    )

    routing_cumu = (routing_dur
        .withColumn(
            "CumuDaysToThisStep_raw",
            F.sum("DurationDays").over(
                W.partitionBy("prod_order_no","prod_order_line_no","item_no")
                 .orderBy("SeqNo")
                 .rowsBetween(W.unboundedPreceding, W.currentRow)
            )
        )
        .withColumn(
            "CumuDaysToThisStep",
            F.least(F.col("CumuDaysToThisStep_raw").cast("double"), F.lit(CUMU_TO_PLATING_MAX_DAYS))
        )
        .drop("CumuDaysToThisStep_raw")
    )

    # Dedup per SeqNo
    routing_cumu_dedup = (routing_cumu
        .withColumn(
            "rn_seq",
            F.row_number().over(
                W.partitionBy("prod_order_no","prod_order_line_no","item_no","SeqNo")
                 .orderBy(F.col("OpName").desc())
            )
        )
        .where(F.col("rn_seq")==1)
        .drop("rn_seq")
    )

    # ========================================================
    # 3) First plating step per item
    # ========================================================
    plating_candidates = (routing_cumu_dedup
        .where( (uptrim("MappedGroup")==F.lit("PLATING")) |
                (uptrim("MappedAbbrev")==F.lit("PLT")) |
                (F.col("OpNameU").contains("PLAT")) )
        .withColumn("rn_plating",
            F.row_number().over(
                W.partitionBy("prod_order_no","prod_order_line_no","item_no").orderBy("SeqNo")
            )
        )
    )

    plating_target = (plating_candidates
        .where(F.col("rn_plating")==1)
        .select(
            "prod_order_no","prod_order_line_no","item_no",
            F.col("CumuDaysToThisStep").alias("CumuToPlatingDays"),
            F.col("SeqNo").alias("PlatingSeqNo")
        )
    )

    line_with_plating = (line
        .join(plating_target.select("prod_order_no","prod_order_line_no","item_no").withColumnRenamed("item_no","prod_item_line"),
              ["prod_order_no","prod_order_line_no","prod_item_line"], "inner")
    )

    # ========================================================
    # 4) Latest status & current step matching
    #     - rows/labels from GOLD
    #     - timestamps from SILVER (fallback to GOLD)
    # ========================================================
    # Latest SILVER timestamp per (order,line,op)
    latest_silver = (status_silver_chg
        .groupBy("prod_order_no","prod_order_line_no","operation_no")
        .agg(F.max("created_on_silver_local").alias("LastSeenAt_silver"))
    )

    # Latest GOLD timestamp per (order,line,op)  (fallback if no silver)
    latest_gold = (status_gold_chg
        .groupBy("prod_order_no","prod_order_line_no","operation_no")
        .agg(F.max("created_on_gold_local").alias("LastSeenAt_gold"))
    )

    latest_ts = (latest_silver
        .join(latest_gold, ["prod_order_no","prod_order_line_no","operation_no"], "full")
        .withColumn("LastSeenAt", F.coalesce(F.col("LastSeenAt_silver"), F.col("LastSeenAt_gold")))
        .select("prod_order_no","prod_order_line_no","operation_no","LastSeenAt")
    )

    # GOLD rows for operation + location label
    gold_ops = status_gold_chg.select(
        "prod_order_no","prod_order_line_no","operation_no","StatusRoutingNo"
    ).distinct()

    latest_status = (gold_ops
        .join(latest_ts, ["prod_order_no","prod_order_line_no","operation_no"], "left")
        .where(F.col("LastSeenAt").isNotNull())  # ensure we have a timestamp
    )

    # Matching hierarchy against routing
    numeric_match = (routing_cumu_dedup.alias("rc")
        .join(latest_status.alias("ls"),
              (F.col("rc.prod_order_no")==F.col("ls.prod_order_no")) &
              (F.col("rc.prod_order_line_no")==F.col("ls.prod_order_line_no")) &
              (F.col("rc.SeqNo") == to_seqno(F.col("ls.operation_no"))),
              "inner")
        .select(
            "rc.prod_order_no","rc.prod_order_line_no","rc.item_no","rc.SeqNo","rc.OpName",
            "rc.MappedGroup","rc.MappedAbbrev","rc.CumuDaysToThisStep",
            "ls.LastSeenAt",
            F.lit("NUM").alias("MatchType"),
            F.lit(3).alias("MatchRank")
        )
    )

    name_match_exact = (routing_cumu_dedup.alias("rc")
        .join(latest_status.alias("ls"),
              (F.col("rc.prod_order_no")==F.col("ls.prod_order_no")) &
              (F.col("rc.prod_order_line_no")==F.col("ls.prod_order_line_no")) &
              (
                (uptrim(F.col("ls.StatusRoutingNo")) == F.col("rc.OpNameU")) |
                (uptrim(F.col("ls.StatusRoutingNo")) == uptrim(F.col("rc.RoutingCode")))
              ),
              "inner")
        .select(
            "rc.prod_order_no","rc.prod_order_line_no","rc.item_no","rc.SeqNo","rc.OpName",
            "rc.MappedGroup","rc.MappedAbbrev","rc.CumuDaysToThisStep",
            "ls.LastSeenAt",
            F.lit("NAME_EQ").alias("MatchType"),
            F.lit(2).alias("MatchRank")
        )
    )

    name_match_partial = (routing_cumu_dedup.alias("rc")
        .join(latest_status.alias("ls"),
              (F.col("rc.prod_order_no")==F.col("ls.prod_order_no")) &
              (F.col("rc.prod_order_line_no")==F.col("ls.prod_order_line_no")) &
              (
                F.col("rc.OpNameU").contains(uptrim(F.col("ls.StatusRoutingNo"))) |
                uptrim(F.col("ls.StatusRoutingNo")).contains(F.col("rc.OpNameU"))
              ),
              "inner")
        .select(
            "rc.prod_order_no","rc.prod_order_line_no","rc.item_no","rc.SeqNo","rc.OpName",
            "rc.MappedGroup","rc.MappedAbbrev","rc.CumuDaysToThisStep",
            "ls.LastSeenAt",
            F.lit("NAME_LIKE").alias("MatchType"),
            F.lit(1).alias("MatchRank")
        )
    )

    u = numeric_match.unionByName(name_match_exact).unionByName(name_match_partial)

    current_step = (u
        .withColumn(
            "rn",
            F.row_number().over(
                W.partitionBy("prod_order_no","prod_order_line_no","item_no")
                 .orderBy(F.col("MatchRank").desc(), F.col("LastSeenAt").desc(), F.col("SeqNo").desc())
            )
        )
        .where(F.col("rn")==1)
        .select(
            "prod_order_no","prod_order_line_no","item_no","SeqNo","OpName",
            "MappedGroup","MappedAbbrev","CumuDaysToThisStep","LastSeenAt","MatchType"
        )
    )

    next_step = (
        routing_cumu_dedup
        .withColumn("NextSeqNo", F.lead("SeqNo").over(
            W.partitionBy("prod_order_no","prod_order_line_no","item_no").orderBy("SeqNo")
        ))
        .withColumn("NextOpName", F.lead("OpName").over(
            W.partitionBy("prod_order_no","prod_order_line_no","item_no").orderBy("SeqNo")
        ))
        .withColumn("NextGroup", F.lead("MappedGroup").over(
            W.partitionBy("prod_order_no","prod_order_line_no","item_no").orderBy("SeqNo")
        ))
        .withColumn("NextAbbrev", F.lead("MappedAbbrev").over(           
            W.partitionBy("prod_order_no","prod_order_line_no","item_no").orderBy("SeqNo")
        ))
        .select(
            "prod_order_no","prod_order_line_no","item_no","SeqNo",
            "NextSeqNo","NextOpName","NextGroup","NextAbbrev"             
        )
    )


    next_from_current = (
        current_step.alias("cp")
        .join(next_step.alias("ns"),
            (F.col("cp.prod_order_no")==F.col("ns.prod_order_no")) &
            (F.col("cp.prod_order_line_no")==F.col("ns.prod_order_line_no")) &
            (F.col("cp.item_no")==F.col("ns.item_no")) &
            (F.col("cp.SeqNo")==F.col("ns.SeqNo")), "left")
        .select(
            F.col("cp.prod_order_no"),
            F.col("cp.prod_order_line_no"),
            F.col("cp.item_no"),
            F.col("ns.NextSeqNo"),
            F.col("ns.NextOpName"),
            F.col("ns.NextGroup"),
            F.col("ns.NextAbbrev")                                       
        )
    )


    # ========================================================
    # 5) Sums around plating
    # ========================================================
    sum_to_plating = plating_target.select(
        "prod_order_no","prod_order_line_no","item_no",
        F.col("CumuToPlatingDays"),
        F.col("PlatingSeqNo")
    )

    cumu_before_now = (routing_cumu_dedup.alias("rc")
        .join(current_step.alias("cp"),
              (F.col("rc.prod_order_no")==F.col("cp.prod_order_no")) &
              (F.col("rc.prod_order_line_no")==F.col("cp.prod_order_line_no")) &
              (F.col("rc.item_no")==F.col("cp.item_no")), "inner")
        .where(F.col("rc.SeqNo") < F.col("cp.SeqNo"))
        .groupBy("cp.prod_order_no","cp.prod_order_line_no","cp.item_no")
        .agg(F.max("rc.CumuDaysToThisStep").alias("CumuBeforeDays"))
        .withColumn("CumuBeforeDays", F.coalesce(F.col("CumuBeforeDays"), F.lit(0.0)))
    )

    # ========================================================
    # 6) Per-item result + ETA
    # ========================================================
    lwp = (line_with_plating
        .withColumnRenamed("prod_item_line","item_no")
    )

    result_per_item = (lwp.alias("lwp")
        .join(sum_to_plating.alias("stp"),
              ["prod_order_no","prod_order_line_no","item_no"], "inner")
        .join(current_step.alias("cp"),
              ["prod_order_no","prod_order_line_no","item_no"], "left")
        .join(next_from_current.alias("nfc"),
              ["prod_order_no","prod_order_line_no","item_no"], "left")
        .join(cumu_before_now.alias("cbn"),
              ["prod_order_no","prod_order_line_no","item_no"], "left")

        .withColumn(
            "PlannedPlating_FromOrderStart",
            F.when(F.col("lwp.StartDatePlan").isNotNull(),
                   add_days_fraction(F.to_timestamp("lwp.StartDatePlan"),
                                     F.col("stp.CumuToPlatingDays"))).otherwise(None)
        )
        .withColumn(
            "PlannedPlating_FromActualStart",
            F.when(F.col("lwp.StartTSActual").isNotNull(),
                   add_days_fraction(F.col("lwp.StartTSActual"),
                                     F.col("stp.CumuToPlatingDays"))).otherwise(None)
        )

        .withColumn(
            "RemainToPlatingDays_fromNow",
            F.when(F.col("cp.SeqNo").isNotNull(),
                   F.greatest(
                       F.lit(0.0),
                       F.col("stp.CumuToPlatingDays").cast("double")
                       - F.coalesce(F.col("cbn.CumuBeforeDays").cast("double"), F.lit(0.0))
                   )
            ).otherwise(None)
        )

        .withColumn(
            "BaseTS",
            F.when(F.col("cp.SeqNo").isNotNull(),
                   F.greatest(F.col("cp.LastSeenAt"), F.current_timestamp())).otherwise(None)
        )

        .withColumn(
            "ETA_FromCurrentStatus",
            F.when(
                F.col("cp.SeqNo").isNotNull() & F.col("RemainToPlatingDays_fromNow").isNotNull(),
                add_days_fraction(F.col("BaseTS"), F.col("RemainToPlatingDays_fromNow"))
            ).otherwise(None)
        )

        .withColumn(
            "PlatingStatus",
            F.when(F.col("cp.MappedAbbrev").isNull(), F.lit("Not Start"))
             .when(uptrim("cp.MappedAbbrev").isin('FIL','HT','TUM','LAS','SET','POL','SHI'), F.lit("Waiting to Plating"))
             .when(uptrim("cp.MappedAbbrev") == 'PLT', F.lit("In Plating"))
             .when(uptrim("cp.MappedAbbrev").isin('GLU','QC','QA','C INS','PCK'), F.lit("Out of Plating"))
             .otherwise(F.lit("Not in the list"))
        )

        .select(
            F.col("lwp.prod_order_no"),
            F.col("lwp.prod_order_line_no"),
            F.col("lwp.prod_line_quantity"),
            F.concat_ws("-", F.col("lwp.prod_order_no"), F.col("lwp.prod_order_line_no").cast("string")).alias("pol"),
            F.col("lwp.item_no"),

            F.col("lwp.StartDatePlan"),
            F.col("lwp.StartTSActual"),
            F.col("lwp.StartDateActual"),

            F.col("stp.CumuToPlatingDays"),

            F.col("PlannedPlating_FromOrderStart"),
            F.col("PlannedPlating_FromActualStart"),

            F.col("cp.SeqNo").alias("CurrentSeqNo"),
            F.col("cp.OpName").alias("CurrentOpName"),
            F.col("cp.LastSeenAt").alias("StatusLastSeenAt"),
            F.col("cp.MappedAbbrev").alias("CurrentAbbrev"),

            F.col("nfc.NextSeqNo"),
            F.col("nfc.NextOpName"),
            F.col("nfc.NextAbbrev"),

            F.col("RemainToPlatingDays_fromNow"),
            F.col("ETA_FromCurrentStatus"),
            F.col("PlatingStatus")
        )
    )

    # ========================================================
    # 7) Pick representative item per (order, line)
    # ========================================================
    result_per_line = (result_per_item
        .withColumn("PlannedPlatingForRanking",
            F.coalesce(F.col("PlannedPlating_FromActualStart"),
                       F.col("PlannedPlating_FromOrderStart"))
        )
        .withColumn("rn",
            F.row_number().over(
                W.partitionBy("prod_order_no","prod_order_line_no")
                 .orderBy(F.col("PlannedPlatingForRanking").asc())
            )
        )
        .where(F.col("rn")==1)
        .drop("rn","PlannedPlatingForRanking")
    )

    # Preview (optional)
    result_per_item.distinct().show()
    result_per_line.distinct().show()

    # ========================================================
    # MERGE into Delta (incremental upsert) — enable when ready
    # ========================================================
    from delta.tables import DeltaTable
    if spark._jsparkSession.catalog().tableExists(TARGET_TABLE):
        tgt = DeltaTable.forName(spark, TARGET_TABLE)
        (tgt.alias("t")
            .merge(result_per_line.alias("s"),
                   "t.prod_order_no = s.prod_order_no AND t.prod_order_line_no = s.prod_order_line_no")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (result_per_line.write.mode("overwrite").format("delta").saveAsTable(TARGET_TABLE))

    # Update watermark to "now"
    (spark.range(1)
       .select(F.current_timestamp().alias("last_processed_ts"))
       .write.mode("overwrite").format("delta").saveAsTable(WATERMARK_TABLE))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
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
              F.when(F.col("user_id_norm") == F.lit("outsource@ennovie.com"), F.lit("Outsource"))
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

# # Wax Cycle Time

# CELL ********************

# ==============================================================
# gold_wax_cycle_time_all — Incremental (Delta-safe)
# ==============================================================

from pyspark.sql import functions as F, Window

# ------------------ CONFIG ------------------
SRC = "Gold_Production_Lakehouse.prod.gold_waxing_and_casting_status"

TARGET = "Gold_Production_Lakehouse.prod.gold_wax_cycle_time_all"  # physical Delta table

# Summary grain (must match GROUP BY)
KEYS   = [
    "prod_order_no","prod_order_line_no","team","machine_center_no",
    "work_date","work_type","employee_no"
]
MODCOL = "_modified_any"     # watermark column we’ll compute
LOOKBACK_MIN = 90            # incremental lookback (minutes)

CENTERS = ["WAX INJECT","WAX FILINIG","WAX SETTING"]

# ------------------ helpers ------------------
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
            keep = [c for c in zcols if c in spark.table(name).columns]
            if keep:
                spark.sql(f"OPTIMIZE {name} ZORDER BY ({', '.join([f'`{c}`' for c in keep])})")
            else:
                spark.sql(f"OPTIMIZE {name}")
        else:
            spark.sql(f"OPTIMIZE {name}")
    except Exception as e:
        print(f"OPTIMIZE notice: {e}")
        spark.sql(f"OPTIMIZE {name}")
    spark.sql(f"VACUUM {name} RETAIN {vacuum_hours} HOURS")

def count_dupes(df, keys):
    return (
        df.groupBy(*[F.col(k) for k in keys]).count()
          .filter(F.col("count") > 1).count()
    )

def add_safe_watermark(df, out_col: str, candidates: list[str]):
    present = [c for c in df.columns if c in candidates]
    if not present:
        return df.withColumn(out_col, F.current_timestamp())
    if len(present) == 1:
        return df.withColumn(out_col, F.col(present[0]).cast("timestamp"))
    return df.withColumn(out_col, F.greatest(*[F.col(c).cast("timestamp") for c in present]))

# ------------------ 1) load & base filter ------------------
s = (spark.table(SRC)
        .filter(F.col("machine_center_no").isin(*CENTERS))
        .select(
            "prod_order_no","prod_order_line_no","created_on","type_name","team",
            "machine_center_no","employee_no","out_qty","user_id",
            F.col("_modified_any").alias("_mod_src")   # ok if null
        ))

# ------------------ 2) normalized events ------------------
normalized = (
    s.select(
        "prod_order_no","prod_order_line_no","machine_center_no","team",
        "employee_no","user_id","created_on","out_qty","type_name"
    )
    .withColumn(
        "event_type",
        F.when(F.col("type_name") == "To employee",   F.lit("Start"))
         .when(F.col("type_name") == "From employee", F.lit("End"))
    )
)

# ------------------ 3) pair Start/End by rn ------------------
w_pair = Window.partitionBy(
    "prod_order_no","prod_order_line_no","machine_center_no","team","employee_no"
).orderBy(F.col("created_on").asc_nulls_last())

starts = (normalized.filter(F.col("event_type")=="Start")
          .withColumn("rn", F.row_number().over(w_pair))
          .select(
              "prod_order_no","prod_order_line_no","machine_center_no","team",
              "employee_no","user_id",
              F.col("created_on").alias("start_time"),
              "rn"
          ))

ends = (normalized.filter(F.col("event_type")=="End")
        .withColumn("rn", F.row_number().over(w_pair))
        .select(
            "prod_order_no","prod_order_line_no","machine_center_no","team",
            "employee_no","user_id",
            F.col("created_on").alias("end_time"),
            F.coalesce(F.col("out_qty").cast("double"), F.lit(1.0)).alias("qty_out"),
            "rn"
        ))

paired = (
    starts.alias("s").join(
        ends.alias("e"),
        on=[
            F.col("s.prod_order_no")==F.col("e.prod_order_no"),
            F.col("s.prod_order_line_no")==F.col("e.prod_order_line_no"),
            F.col("s.machine_center_no")==F.col("e.machine_center_no"),
            F.coalesce(F.col("s.team"), F.lit("")) == F.coalesce(F.col("e.team"), F.lit("")),
            F.coalesce(F.col("s.employee_no"), F.lit("")) == F.coalesce(F.col("e.employee_no"), F.lit("")),
            F.col("s.rn")==F.col("e.rn")
        ],
        how="inner"
    )
    .select(
        F.col("s.prod_order_no"),
        F.col("s.prod_order_line_no"),
        F.col("s.machine_center_no"),
        F.col("s.team"),
        F.col("s.employee_no"),
        F.col("s.user_id").alias("user_id_start"),
        F.col("e.user_id").alias("user_id_end"),
        F.col("s.start_time"),
        F.col("e.end_time"),
        F.col("e.qty_out")
    )
    .withColumn(
        "duration_sec",
        (F.col("end_time").cast("timestamp").cast("long") - F.col("start_time").cast("timestamp").cast("long")).cast("long")
    )
    .withColumn("work_date", F.to_date(F.col("end_time")))
    .withColumn(
        "work_type",
        F.when(F.lower(F.coalesce(F.col("user_id_start"), F.lit(""))) == F.lit("outsource@ennovie.com"), "Outsource")
         .otherwise("In-house")
    )
)

# sensible bounds: >0 and <= 8 hours
paired_clean = paired.filter((F.col("duration_sec") > 0) & (F.col("duration_sec") <= 8*3600))

# ------------------ 4) aggregate to final grain ------------------
grp = (paired_clean.groupBy(
            "prod_order_no","prod_order_line_no","team","machine_center_no","work_date","work_type","employee_no"
       )
       .agg(
           F.count(F.lit(1)).alias("records"),
           F.sum(F.col("duration_sec")).alias("sum_sec"),
           F.sum(F.col("qty_out")).alias("total_qty")
       ))

final = (
    grp.withColumn(
            "AvgPerPiece",
            F.when(F.col("total_qty") == F.lit(0), F.lit(None).cast("double"))
             .otherwise((F.col("sum_sec")/60.0) / F.col("total_qty"))
        )
       .withColumn("Unit", F.lit("min/piece"))
)

# ------------------ 5) watermark (FIXED: aggregate end_time with proper groupBy) ------------------
wm_src = (paired_clean
          .groupBy("prod_order_no","prod_order_line_no","team","machine_center_no","work_date","work_type","employee_no")
          .agg(F.max("end_time").alias("_mod_end")))

final_wm = add_safe_watermark(
    final.join(wm_src, on=KEYS, how="left"),
    MODCOL,
    candidates=["_mod_end"]
).drop("_mod_end")

# ------------------ 6) incremental stage ------------------
last_ts = get_last_ts(TARGET, MODCOL)
staged = final_wm
if last_ts is not None:
    staged = staged.filter(
        F.col(MODCOL) >= (F.lit(last_ts).cast("timestamp") - F.expr(f"INTERVAL {LOOKBACK_MIN} MINUTES"))
    )

print(f"rows staged: {staged.count():,}")

# ------------------ 7) create target if missing ------------------
if not table_exists(TARGET):
    create_like(TARGET, staged.select(*KEYS, "records","sum_sec","total_qty","AvgPerPiece","Unit", MODCOL))
    print(f"Created {TARGET}")

# ------------------ 8) dedupe by keys with latest watermark ------------------
w_keys = Window.partitionBy(*KEYS).orderBy(F.col(MODCOL).desc_nulls_last())
staged_unique = staged.withColumn("_rn", F.row_number().over(w_keys)).filter("_rn=1").drop("_rn")

dupes = count_dupes(staged_unique, KEYS)
if dupes > 0:
    raise RuntimeError(f"Duplicate key groups remain for keys={KEYS}: {dupes}")

# ------------------ 9) MERGE upsert ------------------
tgt = spark.table(TARGET)
target_cols = tgt.columns

tmp = "stg_gold_wax_cycle_time_all"
staged_unique.select(*target_cols).createOrReplaceTempView(tmp)

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

# ------------------ 10) maintenance ------------------
maintain(TARGET, zcols=KEYS + [MODCOL])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
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
             "team","machine_center_no","employee_no","out_qty","user_id"
         )
)


# ------------------ 2) NORMALIZE ------------------
normalized = (
    base.select(
        "prod_order_no","prod_order_line_no","machine_center_no","team","employee_no","created_on","user_id","out_qty",
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
                  "team","employee_no","user_id",
                  F.col("created_on").alias("start_time")
              )
)

ends = (
    normalized.filter(F.col("event_type")=="End")
              .select(
                  "prod_order_no","prod_order_line_no","machine_center_no",
                  F.col("created_on").alias("end_time"),
                  F.coalesce(F.col("out_qty").cast("double"),F.lit(1.0)).alias("qty_out")
              )
)

# ------------------ 4) CLOSEST-END-AFTER-START PAIRING (no rn/team/employee join) ------------------
# give each Start a stable id within its partition
w_start_idx = Window.partitionBy("prod_order_no","prod_order_line_no","machine_center_no") \
                    .orderBy(F.col("start_time").asc_nulls_last())

starts_idxed = starts.withColumn("start_id", F.row_number().over(w_start_idx))

# candidate pairs: same order/line/center AND end_time > start_time
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


# pick the nearest End per Start (minimal positive duration)
w_best = Window.partitionBy(
    "s.prod_order_no","s.prod_order_line_no","s.machine_center_no","s.start_id"
).orderBy(F.col("duration_sec").asc())

paired = (
    candidates
    #   .withColumn("rn_best", F.row_number().over(w_best))
    #   .filter(F.col("rn_best")==1)
    #   .filter((F.col("duration_sec")>0) & (F.col("duration_sec")<=8*3600))  # <= 8h cap
      .select(
          F.col("s.prod_order_no").alias("prod_order_no"),
          F.col("s.prod_order_line_no").alias("prod_order_line_no"),
          F.col("s.machine_center_no").alias("machine_center_no"),
          F.col("s.team").alias("team"),
          F.col("s.employee_no").alias("employee_no"),
          F.col("s.user_id").alias("user_id_start"),
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
                  F.lit("min/piece").alias("Unit")
              )
              .withColumn(MODCOL, F.current_timestamp())
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

# # Monthly Remaining Qty

# CELL ********************

# ==============================================================
# gold_monthly_remaining_qty — UNION status + casting_summary
# incremental, watermark-safe, Delta MERGE (with POL)
# ==============================================================

from pyspark.sql import functions as F, Window

# ------------------ CONFIG ------------------
SRC_STATUS  = "Gold_Production_Lakehouse.prod.gold_waxing_and_casting_status"
SRC_SUMMARY = "Gold_Production_Lakehouse.prod.gold_casting_summary"
SRC_CASTP   = "Silver_Production_Lakehouse.prod.silver_casting_parts"

TARGET_TABLE = "Gold_Production_Lakehouse.prod.gold_monthly_remaining_qty"

# One row per (order,line,dimension)
KEYS         = ["prod_order_no","prod_order_line_no","dim_type","dim_value"]
MODCOL       = "_modified_any"
LOOKBACK_MIN = 90

# Toggle this to mirror your commented month filter in SQL
APPLY_MONTH_FILTER = False  # set True to restrict to current month

# ------------------ HELPERS ------------------
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
            keep = [c for c in zcols if c in spark.table(name).columns]
            if keep:
                spark.sql(f"OPTIMIZE {name} ZORDER BY ({', '.join([f'`{c}`' for c in keep])})")
            else:
                spark.sql(f"OPTIMIZE {name}")
        else:
            spark.sql(f"OPTIMIZE {name}")
    except Exception as e:
        print(f"OPTIMIZE notice: {e}")
        spark.sql(f"OPTIMIZE {name}")
    spark.sql(f"VACUUM {name} RETAIN {vacuum_hours} HOURS")

def count_dupes(df, keys):
    return (df.groupBy(*[F.col(k) for k in keys]).count().filter("count > 1").count())

def add_safe_watermark(df, out_col: str, candidates: list[str]):
    present = [c for c in candidates if c in df.columns]
    if not present:
        return df.withColumn(out_col, F.current_timestamp())
    if len(present) == 1:
        return df.withColumn(out_col, F.col(present[0]).cast("timestamp"))
    return df.withColumn(out_col, F.greatest(*[F.col(c).cast("timestamp") for c in present]))

# ------------------ 1) LOAD SOURCES ------------------
status  = spark.table(SRC_STATUS)
summary = spark.table(SRC_SUMMARY)
castp   = spark.table(SRC_CASTP).select("prod_order_no","prod_order_line_no").dropna(how="any").dropDuplicates()

print(f"status={status.count():,}  summary={summary.count():,}  cast_parts={castp.count():,}")

# ------------------ 2) BRANCH A: STATUS ------------------
# due_date = COALESCE(NULLIF(created_on,'1900-01-01'), created_on)
sentinel = F.to_timestamp(F.lit("1900-01-01 00:00:00"))
due_date_a = F.when(F.col("created_on").cast("timestamp") == sentinel, F.lit(None)) \
              .otherwise(F.col("created_on").cast("timestamp"))

A = (
    status
      .filter(F.col("open") == "Yes")
      .filter(F.col("prod_order_status") == "Released")
      .filter(F.col("item_no").like("C0%"))
      .select(
          F.col("team"),
          F.lit(None).cast("string").alias("customer_no"),
          F.lit(None).cast("string").alias("customer_abbr"),
          F.col("item_no").alias("item_no"),
          F.when(F.length(F.trim(F.col("machine_center_no"))) == 0, F.lit(None))
           .otherwise(F.trim(F.col("machine_center_no"))).alias("stage_name"),
          due_date_a.alias("due_date"),
          F.col("remaining_quantity").cast("decimal(38,4)").alias("remaining_qty"),
          F.col("prod_order_no"),
          F.col("prod_order_line_no"),
          F.col("_modified_any").alias("_mod_status")
      )
)

# ------------------ 3) BRANCH B: CASTING SUMMARY ------------------
# due_date = COALESCE(fg_due_date, casting_due_date, fg_start_date, casting_start_date)
due_date_b = F.coalesce(
    F.col("fg_due_date").cast("timestamp"),
    F.col("casting_due_date").cast("timestamp"),
    F.col("fg_start_date").cast("timestamp"),
    F.col("casting_start_date").cast("timestamp")
)

B = (
    summary
      .filter(F.col("prod_order_status") == "Released")
      .filter(F.col("item_location") == "CST_CUT")
      # NOTE: your SQL had FG_item_no filter commented out; we follow that (no filter here).
      .select(
          F.col("team"),
          F.col("CustomerNo").alias("customer_no"),
          F.col("CustomerAbbr").alias("customer_abbr"),
          F.coalesce(F.when(F.length(F.trim(F.col("FG_item_no"))) > 0, F.trim(F.col("FG_item_no"))),
                     F.col("itemCST")).alias("item_no"),
          F.when(F.length(F.trim(F.col("machine_center_no"))) == 0, F.lit(None))
           .otherwise(F.trim(F.col("machine_center_no"))).alias("stage_name"),
          due_date_b.alias("due_date"),
          F.col("remaining_qty").cast("decimal(38,4)").alias("remaining_qty"),
          F.col("prod_order_no"),
          F.col("prod_order_line_no")
          # no source modcol on summary; watermark fallback will use due_date
      )
)

# ------------------ 4) UNION ALL ------------------
unified = A.unionByName(B, allowMissingColumns=True) \
           .withColumn("POL", F.concat(F.col("prod_order_no").cast("string"),
                                       F.col("prod_order_line_no").cast("string")))

# ------------------ 5) ANTI-JOIN done_orders ------------------
unified = (
    unified.alias("u")
           .join(castp.alias("d"),
                 on=[F.col("u.prod_order_no")==F.col("d.prod_order_no"),
                     F.col("u.prod_order_line_no")==F.col("d.prod_order_line_no")],
                 how="left_anti")
)

# ------------------ 6) OPTIONAL MONTH FILTER ------------------
if APPLY_MONTH_FILTER:
    month_start = F.trunc(F.current_timestamp(), "month")
    next_month  = F.add_months(month_start, 1)
    unified = unified.filter((F.col("due_date") >= month_start) & (F.col("due_date") < next_month))

# ------------------ 7) EXPLODE DIMENSIONS (CROSS APPLY VALUES) ------------------
dim_array = F.array(
    F.struct(F.lit("team").alias("dim_type"),
             F.when(F.length(F.trim(F.col("team"))) == 0, F.lit(None))
              .otherwise(F.trim(F.col("team"))).alias("dim_value")),
    F.struct(F.lit("customer").alias("dim_type"),
             F.when(F.length(F.trim(F.col("customer_abbr"))) > 0, F.trim(F.col("customer_abbr")))
              .otherwise(F.when(F.length(F.trim(F.col("customer_no"))) > 0, F.trim(F.col("customer_no")))
                        .otherwise(F.lit(None))).alias("dim_value")),
    F.struct(F.lit("item").alias("dim_type"),
             F.when(F.length(F.trim(F.col("item_no"))) == 0, F.lit(None))
              .otherwise(F.trim(F.col("item_no"))).alias("dim_value")),
    F.struct(F.lit("stage").alias("dim_type"),
             F.when(F.length(F.trim(F.col("stage_name"))) == 0, F.lit(None))
              .otherwise(F.trim(F.col("stage_name"))).alias("dim_value"))
)

exploded = (
    unified
      .withColumn("dim", F.explode(dim_array))
      .select(
          F.col("dim.dim_type").alias("dim_type"),
          F.col("dim.dim_value").alias("dim_value"),
          "team","customer_no","customer_abbr","item_no","stage_name",
          "due_date","remaining_qty","prod_order_no","prod_order_line_no","POL",
          F.col("_mod_status")
      )
      .filter(F.col("dim_value").isNotNull())
)

# ------------------ 8) WATERMARK ------------------
result = add_safe_watermark(exploded, MODCOL, ["_mod_status","due_date"])

# ------------------ 9) INCREMENTAL STAGE ------------------
last_ts = get_last_ts(TARGET_TABLE, MODCOL)
staged = result
if last_ts is not None:
    staged = staged.filter(
        F.col(MODCOL) >= (F.lit(last_ts).cast("timestamp") - F.expr(f"INTERVAL {LOOKBACK_MIN} MINUTES"))
    )

print(f"rows staged: {staged.count():,}")

# ------------------ 10) CREATE TARGET IF MISSING ------------------
if not table_exists(TARGET_TABLE):
    create_like(
        TARGET_TABLE,
        staged.select(
            "dim_type","dim_value","team","customer_no","customer_abbr","item_no","stage_name",
            "due_date","remaining_qty","prod_order_no","prod_order_line_no","POL", MODCOL
        )
    )
    print(f"Created {TARGET_TABLE}")

# ------------------ 11) ENFORCE UNIQUENESS ON MERGE KEYS ------------------
w_keys = Window.partitionBy(*KEYS).orderBy(F.col(MODCOL).desc_nulls_last())
staged_unique = (
    staged.withColumn("_rn", F.row_number().over(w_keys))
          .filter(F.col("_rn") == 1)
          .drop("_rn","_mod_status")
)

dupes = count_dupes(staged_unique, KEYS)
if dupes > 0:
    raise RuntimeError(f"Staged data still has {dupes} duplicate key groups on {KEYS}")

# ------------------ 12) MERGE UPSERT ------------------
tgt = spark.table(TARGET_TABLE)
target_cols = tgt.columns

tmp = "stg_monthly_remaining_qty_union"
staged_unique.select(*target_cols).createOrReplaceTempView(tmp)

on_expr  = " AND ".join([f"(t.{k} <=> s.{k})" for k in KEYS])
set_expr = ", ".join([f"t.{c}=s.{c}" for c in target_cols if c not in set(KEYS)])

spark.sql(f"""
    MERGE INTO {TARGET_TABLE} t
    USING {tmp} s
    ON {on_expr}
    WHEN MATCHED THEN UPDATE SET {set_expr}
    WHEN NOT MATCHED THEN INSERT ({", ".join(target_cols)})
    VALUES ({", ".join([f"s.{c}" for c in target_cols])})
""")

# ------------------ 13) MAINTENANCE ------------------
maintain(TARGET_TABLE, zcols=KEYS + [MODCOL])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # Customer Performance

# CELL ********************

# ==============================================================
# v_customer_performance_monthly — Incremental Delta loader + view (with POL)
# ==============================================================

from pyspark.sql import functions as F, Window

# ---------- sources ----------
SRC_SUMMARY = "Gold_Production_Lakehouse.prod.gold_casting_summary"

# ---------- targets ----------
TARGET_TABLE = "Gold_Production_Lakehouse.prod.gold_customer_performance"   # physical table

# Grouping grain (matches your SQL GROUP BY)
KEYS   = ["CustomerNo","CustomerAbbr","prod_order_no","prod_order_line_no"]
MODCOL = "_modified_any"
LOOKBACK_MIN = 90  # minutes overlap for incremental safety
APPLY_MONTH_FILTER = False  # respect monthly window

# ---------- helpers ----------
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
            keep = [c for c in zcols if c in spark.table(name).columns]
            if keep:
                cols_csv = ", ".join([f"`{c}`" for c in keep])
                spark.sql(f"OPTIMIZE {name} ZORDER BY ({cols_csv})")
            else:
                spark.sql(f"OPTIMIZE {name}")
        else:
            spark.sql(f"OPTIMIZE {name}")
    except Exception as e:
        print(f"OPTIMIZE notice: {e}")
        spark.sql(f"OPTIMIZE {name}")
    spark.sql(f"VACUUM {name} RETAIN {vacuum_hours} HOURS")

def count_dupes(df, keys):
    return (
        df.groupBy(*[F.col(k) for k in keys]).count()
          .filter(F.col("count") > 1).count()
    )

def add_safe_watermark(df, out_col: str, candidates: list[str]):
    present = [c for c in candidates if c in df.columns]
    if not present:
        return df.withColumn(out_col, F.current_timestamp())
    if len(present) == 1:
        return df.withColumn(out_col, F.col(present[0]).cast("timestamp"))
    return df.withColumn(out_col, F.greatest(*[F.col(c).cast("timestamp") for c in present]))

# ---------- 1) month_bounds ----------
month_start = F.trunc(F.current_timestamp(), "month")
next_month  = F.add_months(month_start, 1)

# ---------- 2) load source ----------
c = spark.table(SRC_SUMMARY)

# in_month (Completed only) + anchor_date
in_month = (
    c.filter(F.col("new_status") == "Complete")
     .select(
         "prod_order_no","prod_order_line_no",
         F.col("CustomerNo"), F.col("CustomerAbbr"), F.col("team"),
         "FG_item_no","itemCST",
         F.col("casting_qty_to_tree").cast("decimal(38,4)").alias("casting_qty_to_tree"),
         F.col("casting_qty_passed").cast("decimal(38,4)").alias("casting_qty_passed"),
         F.col("casting_qty_reject").cast("decimal(38,4)").alias("casting_qty_reject"),
         F.col("remaining_qty").cast("decimal(38,4)").alias("remaining_qty"),
         "casting_start_date","fg_start_date","fg_due_date","casting_due_date",
         F.coalesce("fg_due_date","casting_due_date","fg_start_date","casting_start_date").alias("anchor_date"),
         F.col("_modified_any").alias("_mod_src")
     )
)

if APPLY_MONTH_FILTER:
    in_month = in_month.filter((F.col("anchor_date") >= month_start) & (F.col("anchor_date") < next_month))

# ---------- 3) speedified ----------
speedified = (
    in_month.select(
        "prod_order_no","prod_order_line_no",
        "casting_start_date","fg_start_date","fg_due_date","casting_due_date",
        "CustomerNo","CustomerAbbr",
        (F.datediff("fg_start_date","casting_start_date") + F.lit(1)).alias("days_cast_to_fgstart"),
        (F.datediff("fg_due_date","casting_start_date")  + F.lit(1)).alias("days_cast_to_due"),
        (F.datediff("fg_due_date","fg_start_date")       + F.lit(1)).alias("days_fgstart_to_due"),
        F.col("casting_qty_to_tree").alias("qty_in"),
        F.col("casting_qty_passed").alias("qty_passed"),
        F.col("casting_qty_reject").alias("qty_reject"),
        F.col("_mod_src")
    )
)

# ---------- 4) aggregate (fixed) ----------
grp = speedified.groupBy(KEYS)

denom = F.max(F.datediff(F.col("casting_due_date"), F.col("casting_start_date")) + F.lit(1))
denom_safe = F.when(denom == 0, F.lit(None)).otherwise(denom)  # works on older Spark too

result = grp.agg(
    F.max("casting_due_date").alias("casting_due_date"),  # or .alias("latest_casting_due_date")
    F.sum("qty_passed").alias("total_output_qty"),
    F.sum("qty_in").alias("total_input_qty"),
    F.sum("qty_reject").alias("total_reject_qty"),
    (F.sum("qty_passed") / denom_safe).cast("decimal(10,4)").alias("throughput_per_day"),
    F.avg(F.col("days_cast_to_fgstart").cast("double")).cast("decimal(10,2)").alias("avg_days_cast_to_fgstart"),
    F.avg(F.col("days_cast_to_due").cast("double")).cast("decimal(10,2)").alias("avg_days_cast_to_due"),
    F.avg(F.col("days_fgstart_to_due").cast("double")).cast("decimal(10,2)").alias("avg_days_fgstart_to_due")
)


# ---------- 5) add POL (concat of the keys) ----------
result = result.withColumn("pol", F.concat(F.col("prod_order_no"), F.col("prod_order_line_no").cast("string")))

# ---------- 6) watermark ----------
result = add_safe_watermark(
    result,
    MODCOL,
    candidates=["_mod_src","fg_due_date","casting_due_date","fg_start_date","casting_start_date"]
)

# ---------- 7) incremental stage ----------
last_ts = get_last_ts(TARGET_TABLE, MODCOL)
staged = result
if last_ts is not None:
    staged = staged.filter(
        F.col(MODCOL) >= (F.lit(last_ts).cast("timestamp") - F.expr(f"INTERVAL {LOOKBACK_MIN} MINUTES"))
    )
print(f"rows staged: {staged.count():,}")

# ---------- 8) create target if missing (include POL) ----------
if not table_exists(TARGET_TABLE):
    create_like(
        TARGET_TABLE,
        staged.select(
            *KEYS,
            "total_output_qty","total_input_qty","total_reject_qty",
            "throughput_per_day", "casting_due_date",
            "avg_days_cast_to_fgstart","avg_days_cast_to_due","avg_days_fgstart_to_due",
            "pol",
            MODCOL
        )
    )
    print(f"Created {TARGET_TABLE}")

# ---------- 9) enforce uniqueness ----------
w_keys = Window.partitionBy(*KEYS).orderBy(F.col(MODCOL).desc_nulls_last())
staged_unique = (
    staged.withColumn("_rn", F.row_number().over(w_keys))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
)
dupes = count_dupes(staged_unique, KEYS)
if dupes > 0:
    raise RuntimeError(f"Staged data still has {dupes} duplicate key groups on {KEYS}")

# ---------- 10) MERGE upsert ----------
tgt = spark.table(TARGET_TABLE)
target_cols = tgt.columns

tmp = "stg_customer_performance_monthly"
staged_unique.select(*target_cols).createOrReplaceTempView(tmp)

on_expr  = " AND ".join([f"(t.{k} <=> s.{k})" for k in KEYS])
set_expr = ", ".join([f"t.{c}=s.{c}" for c in target_cols if c not in set(KEYS)])

spark.sql(f"""
    MERGE INTO {TARGET_TABLE} t
    USING {tmp} s
    ON {on_expr}
    WHEN MATCHED THEN UPDATE SET {set_expr}
    WHEN NOT MATCHED THEN INSERT ({", ".join(target_cols)})
    VALUES ({", ".join([f"s.{c}" for c in target_cols])})
""")

# ---------- 11) maintain ----------
maintain(TARGET_TABLE, zcols=KEYS + [MODCOL])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
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
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
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

    # ---------- Step 1: step_date (Bangkok time, first time per step) ----------
    # created_on assumed UTC; convert to Asia/Bangkok then to_date
    step_date_df = (
        log_df.alias("l")
        .join(F.broadcast(candidate_orders), on="casting_prod_order", how="inner")
        .groupBy("l.casting_prod_order", "l.casting_status")
        .agg(F.min(F.to_date(F.from_utc_timestamp(F.col("l.created_on"), "Asia/Bangkok"))).alias("step_date"))
    )

    # ---------- Step 2: pivot to columns ----------
    pivot_time_df = (
        step_date_df.groupBy("casting_prod_order").agg(
            F.max(F.when(F.col("casting_status") == F.lit("Start New Casting Tree"), F.col("step_date"))).alias("WAX_TREE"),
            F.max(F.when(F.col("casting_status") == F.lit("Cutting Transfer"),       F.col("step_date"))).alias("CASTING_FINISHED"),
            F.max(F.when(F.col("casting_status") == F.lit("Casting Output"),         F.col("step_date"))).alias("CUTTING_FINISHED"),
            F.max(F.when(F.col("casting_status") == F.lit("POST CASTING PARTS"),       F.col("step_date"))).alias("WAREHOUSE_OUTPUT"),

        )
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

            # Add formatted versions for display (e.g., "Sep 30")
            F.col("pt.WAX_TREE"), F.col("pt.CASTING_FINISHED"), F.col("pt.CUTTING_FINISHED"), F.col("pt.WAREHOUSE_OUTPUT"),

            # For month grouping & chronological sorting
            F.date_format(F.col("pt.WAX_TREE"), "yyyy-MM").alias("Month_Of_WAX_TREE"),


            F.col("q.TreeNo_NotSUB"),
            F.col("q.TreeNo_SUB"),
            F.col("q.qty_to_tree"),
            F.col("q.qty_good"),
            F.col("q.qty_reject"),
            F.col("q.qty_to_tree_NotSUB"),
            F.col("q.qty_to_tree_SUB"),
        )
    )

    # Track the latest source ts we saw *among affected rows* for the watermark column
    src_max_ts = F.greatest(
        F.lit(0).cast("timestamp"),
        change_ts_expr(log_df),
        change_ts_expr(parts_df),
        change_ts_expr(tree_df)
    )

    # Because src_max_ts is not tied per-row, compute a scalar first:
    log_mx   = log_df.select(log_change_ts).agg(F.max("change_ts").alias("mx")).collect()[0]["mx"]
    parts_mx = parts_df.select(parts_change_ts).agg(F.max("change_ts").alias("mx")).collect()[0]["mx"]
    tree_mx  = tree_df.select(tree_change_ts).agg(F.max("change_ts").alias("mx")).collect()[0]["mx"]
    overall_mx = max([d for d in [log_mx, parts_mx, tree_mx] if d is not None]) if any([log_mx, parts_mx, tree_mx]) else None

    if overall_mx is None:
        overall_mx = F.current_timestamp()

    final_df = final_df.withColumn("source_max_ts", F.lit(overall_mx))

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
        # Make a temp view for MERGE
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

# # Casting Cycle Time

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
# META   "frozen": false,
# META   "editable": true
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

# # Print Production Order

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import DeltaTable

SRC = "Gold_Production_Lakehouse.prod.gold_print_production_order_casting"
TGT_DB = "prod"
TGT_TABLE = "gold_print_production_order"
FULL_TGT = f"{TGT_DB}.{TGT_TABLE}"

spark.sql(f"CREATE TABLE IF NOT EXISTS {TGT_DB}")

# --- 1) Build source projection (your SELECT ... WHERE ... plus poi) ---
src = (
    spark.table(SRC)
    .filter(F.col("prod_order_status") == F.lit("Released"))
    .select(
        "prod_order_status",
        "prod_order_no",
        "prod_order_line_no",
        "prod_order_start_date",
        "prod_order_due_date",
        "sales_order_no",
        "item_no",
        "prod_order_print",
    )
    .withColumn("poi", F.concat_ws("", F.col("prod_order_no"), F.col("item_no")))
)

# --- 2) Add a stable row hash to detect content changes (for smart updates) ---
hash_cols = [
    "prod_order_status","prod_order_no","prod_order_line_no","prod_order_start_date",
    "prod_order_due_date","sales_order_no","item_no","prod_order_print","poi"
]
src = src.withColumn("row_hash", F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in hash_cols]), 256))

# --- 3) Try to find a reasonable watermark column from the source ---
candidate_watermarks = ["sink_modified_on", "modified_on", "modifiedon", "_commit_timestamp", "ingest_ts", "load_ts"]
available_wm = next((c for c in candidate_watermarks if c in spark.table(SRC).columns), None)

if available_wm:
    src = src.withColumn("updated_at", F.to_timestamp(F.col(available_wm)))
else:
    # fallback: use hash time (not a real watermark, just to satisfy schema)
    src = src.withColumn("updated_at", F.current_timestamp())

# lineage
src = src.withColumn("load_ts", F.current_timestamp()).withColumn("source_system", F.lit("gold_warehouse"))

# --- 4) If target doesn't exist, create it (CTAS equivalent) ---
if not spark.catalog.tableExists(FULL_TGT):
    (
        src
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(FULL_TGT)
    )
    spark.sql(f"""
      ALTER TABLE {FULL_TGT}
      SET TBLPROPERTIES (
        delta.autoOptimize.optimizeWrite = true,
        delta.autoOptimize.autoCompact = true
      )
    """)
else:
    # --- 5) Incremental filter (only if we actually have a usable watermark) ---
    if available_wm:
        max_wm = spark.table(FULL_TGT).agg(F.max("updated_at").alias("wm")).collect()[0]["wm"]
        if max_wm:
            src = src.filter(F.col("updated_at") > F.lit(max_wm))

    # Short-circuit if nothing to do
    if src.rdd.isEmpty():
        print("No new or updated rows to process. ✅")
    else:
        # --- 6) Merge on poi; only UPDATE when row_hash changed ---
        tgt = DeltaTable.forName(spark, FULL_TGT)

        (
            tgt.alias("t")
            .merge(
                src.alias("s"),
                "t.poi = s.poi"
            )
            .whenMatchedUpdate(condition="t.row_hash <> s.row_hash", set={
                "prod_order_status":       "s.prod_order_status",
                "prod_order_no":           "s.prod_order_no",
                "prod_order_line_no":       "s.prod_order_line_no",
                "prod_order_start_date":   "s.prod_order_start_date",
                "prod_order_due_date":     "s.prod_order_due_date",
                "sale_sorder_no":          "s.sales_order_no",
                "item_no":                "s.item_no",
                "prod_order_print":        "s.prod_order_print",
                "row_hash":               "s.row_hash",
                "updated_at":             "s.updated_at",
                "load_ts":                "s.load_ts",
                "source_system":          "s.source_system"
            })
            .whenNotMatchedInsert(values={
                "prod_order_status":       "s.prod_order_status",
                "prod_order_no":           "s.prod_order_no",
                "prod_order_line_no":       "s.prod_order_line_no",
                "prod_order_start_date":   "s.prod_order_start_date",
                "prod_order_due_date":     "s.prod_order_due_date",
                "sales_order_no":          "s.sales_order_no",
                "item_no":                "s.item_no",
                "prod_order_print":        "s.prod_order_print",
                "poi":                    "s.poi",
                "row_hash":               "s.row_hash",
                "updated_at":             "s.updated_at",
                "load_ts":                "s.load_ts",
                "source_system":          "s.source_system"
            })
            .execute()
        )

# --- 7) Optional housekeeping ---
# spark.sql(f"OPTIMIZE {FULL_TGT} ZORDER BY (poi)")
# spark.sql(f"VACUUM {FULL_TGT} RETAIN 168 HOURS")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # Summary Production Status

# CELL ********************

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

# ------------------ CONFIG ------------------
PO = "Silver_Production_Lakehouse.prod.silver_prod_order_header"
IT = "Silver_Inventory_Lakehouse.inv.silver_item"
SO = "Gold_Production_Lakehouse.prod.gold_sales_order"
PL = "Silver_Production_Lakehouse.prod.silver_prod_order_line"
GS = "Gold_Production_Lakehouse.prod.gold_production_status"  # for NOT EXISTS

TGT = "Gold_Production_Lakehouse.prod.gold_summary_production_status"

# merge grain (unique key)
KEYS = ["prod_order_no", "FG_item_no", "sales_order_line_no"]

# unified modified ts + lookback
MODCOL = "_modified_any"
LOOKBACK_MIN = 90

# ------------------ helpers ------------------
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
       .write.format("delta").mode("overwrite")
       .option("overwriteSchema","true")
       .saveAsTable(name))
    spark.sql(f"""
      ALTER TABLE {name}
      SET TBLPROPERTIES (
        delta.autoOptimize.optimizeWrite = true,
        delta.autoOptimize.autoCompact = true
      )
    """)

def wm_candidates(df, alias, *cols):
    # add only existing, cast to timestamp
    out = []
    for c in cols:
        if c in df.columns:
            out.append(F.to_timestamp(F.col(f"{alias}.{c}")))
    return out

# ------------------ READ ------------------
po_src = spark.table(PO)   # keep unaliased for schema checks
it_src = spark.table(IT)
so_src = spark.table(SO)
pl_src = spark.table(PL)
gs_src = spark.table(GS)

po = po_src.alias("po")
it = it_src.alias("it")
so = so_src.alias("so")
pl = pl_src.alias("pl")
gs = gs_src.alias("s")

# ------------------ BUILD (joins + filters) ------------------
# SO prefixes
prefix_ok = (
    F.col("po.sales_order_no").startswith("RO") |
    F.col("po.sales_order_no").startswith("RE") |
    F.col("po.sales_order_no").startswith("SL") |
    F.col("po.sales_order_no").startswith("SP")
)

# NOT EXISTS => left anti against gold_production_status with given predicates
anti_cond = (
    (F.col("s.prod_order_no") == F.col("po.prod_order_no")) &
    (F.col("s.type_name") == F.lit("In location in")) &
    (F.col("s.CorrectCurrentLocation") == F.lit("PACKING ROOM")) &
    (F.col("s.open") == F.lit("Yes"))
)

joined = (
    po.join(it, F.col("po.FG_item_no") == F.col("it.item_no"), "left")
      .join(so, (F.col("po.sales_order_no") == F.col("so.SalesorderNo")) &
                (F.col("po.sales_order_line_no") == F.col("so.SalesLineNo")), "left")
      .join(pl, (F.col("po.prod_order_no") == F.col("pl.prod_order_no")) &
                (F.col("po.FG_item_no") == F.col("pl.item_no")), "left")
      .join(gs, anti_cond, "left_anti")
      .where( (F.col("it.item_category") == F.lit("FG")) & prefix_ok )
)

# ------------------ Watermark (prefer po/pl/so modified cols) ------------------
wm_exprs = []
wm_exprs += wm_candidates(po_src, "po", "SinkModifiedOn","modified_on","modifiedon","_commit_timestamp","load_ts","created_on")
wm_exprs += wm_candidates(pl_src, "pl", "SinkModifiedOn","modified_on","modifiedon","_commit_timestamp","load_ts","created_on")
# gold_sales_order likely has _modified_any or updated_at
if "_modified_any" in so_src.columns:
    wm_exprs.append(F.to_timestamp(F.col("so._modified_any")))
elif "updated_at" in so_src.columns:
    wm_exprs.append(F.to_timestamp(F.col("so.updated_at")))

if wm_exprs:
    wm_expr = F.greatest(*wm_exprs)
else:
    # fallback to business timestamps
    wm_expr = F.coalesce(
        F.to_timestamp(F.col("po.prod_order_ending_date_time")),
        F.to_timestamp(F.col("po.prod_order_finished_date")),
        F.current_timestamp()
    )

# ------------------ Projection (your SQL) ------------------
src = joined.select(
    F.col("po.prod_order_due_date").alias("prod_order_due_date"),
    F.col("po.prod_order_status").alias("prod_order_status"),
    F.col("po.prod_order_no").alias("prod_order_no"),
    F.col("po.prod_order_description").alias("prod_order_description"),
    F.col("po.FG_item_no").alias("FG_item_no"),
    F.col("po.item_routing_no").alias("item_routing_no"),
    F.col("po.prod_order_quantity").alias("prod_order_quantity"),
    F.col("po.prod_order_type").alias("prod_order_type"),
    F.col("po.sales_order_no").alias("sales_order_no"),
    F.col("po.sales_order_line_no").alias("sales_order_line_no"),
    F.col("po.prod_order_location").alias("prod_order_location"),
    F.col("po.prod_order_starting_date_time").alias("prod_order_starting_date_time"),
    F.col("po.prod_order_ending_date_time").alias("prod_order_ending_date_time"),
    F.col("po.prod_order_finished_date").alias("prod_order_finished_date"),

    F.col("pl.prod_line_quantity").alias("prod_line_quantity"),
    F.col("pl.prod_line_finished_quantity").alias("prod_line_finished_quantity"),
    F.col("pl.prod_line_remaining_quantity").alias("prod_line_remaining_quantity"),

    F.col("so.CusNo").alias("CusNo"),
    F.col("so.CusName").alias("CusName"),
    F.col("so.CusAbbr").alias("CusAbbr"),
    F.col("so.so_abbr").alias("so_abbr"),
    F.col("so.so_type").alias("so_type"),
    F.col("so.TypeofFG").alias("TypeofFG"),
    F.col("so.Total_QTY").alias("Total_QTY"),
    F.col("so.OutstandingQty").alias("OutstandingQty"),
    F.col("so.item_quantity_to_ship").alias("QtytoShip"),
    F.col("so.item_quantity_shipped").alias("QtyShipped"),

    F.to_timestamp(wm_expr).alias(MODCOL)
).dropDuplicates()

# ------------------ Incremental window ------------------
last_ts = get_last_ts(TGT, MODCOL)
staged = src
if last_ts is not None:
    staged = staged.filter(
        F.col(MODCOL) >= (F.lit(last_ts).cast("timestamp") - F.expr(f"INTERVAL {LOOKBACK_MIN} MINUTES"))
    )

# ------------------ Prepare target ------------------
if not table_exists(TGT):
    boot = (staged
            .withColumn("updated_at", F.col(MODCOL))
            .withColumn("load_ts", F.current_timestamp())
            .withColumn("source_system", F.lit("gold_builder"))
            .withColumn("row_hash", F.lit(None).cast("string")))
    create_with_schema(TGT, boot)

# ------------------ Dedupe by KEYS ------------------
w = Window.partitionBy(*KEYS).orderBy(
    F.col(MODCOL).desc_nulls_last(),
    F.col("prod_order_finished_date").desc_nulls_last(),
    F.col("prod_order_ending_date_time").desc_nulls_last(),
    # deterministic tie-breaker
    F.sha2(F.concat_ws("§",
        F.coalesce(F.col("sales_order_no").cast("string"), F.lit("")),
        F.coalesce(F.col("FG_item_no").cast("string"), F.lit(""))
    ), 256).desc()
)
staged1 = (staged
           .withColumn("_rn", F.row_number().over(w))
           .filter(F.col("_rn") == 1)
           .drop("_rn"))

# ------------------ System cols + hash ------------------
content_cols = [c for c in staged1.columns]  # includes MODCOL
staged_h = (
    staged1
      .withColumn("updated_at", F.col(MODCOL))
      .withColumn("load_ts", F.current_timestamp())
      .withColumn("source_system", F.lit("gold_builder"))
      .withColumn("row_hash",
          F.sha2(F.concat_ws("§", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in content_cols]), 256)
      )
)

# ------------------ MERGE (Delta) ------------------
if staged_h.rdd.isEmpty():
    print("No rows to merge for gold_summary_production_status. ✅")
else:
    tgt = DeltaTable.forName(spark, TGT)
    on_clause = " AND ".join([f"t.{k} <=> s.{k}" for k in KEYS])

    (
        tgt.alias("t")
           .merge(staged_h.alias("s"), on_clause)
           .whenMatchedUpdate(condition="t.row_hash <> s.row_hash", set={
               "prod_order_due_date":             "s.prod_order_due_date",
               "prod_order_status":               "s.prod_order_status",
               "prod_order_description":          "s.prod_order_description",
               "item_routing_no":                 "s.item_routing_no",
               "prod_order_quantity":             "s.prod_order_quantity",
               "prod_order_type":                 "s.prod_order_type",
               "sales_order_no":                  "s.sales_order_no",
               "sales_order_line_no":             "s.sales_order_line_no",
               "prod_order_location":             "s.prod_order_location",
               "prod_order_starting_date_time":   "s.prod_order_starting_date_time",
               "prod_order_ending_date_time":     "s.prod_order_ending_date_time",
               "prod_order_finished_date":        "s.prod_order_finished_date",
               "prod_line_quantity":              "s.prod_line_quantity",
               "prod_line_finished_quantity":     "s.prod_line_finished_quantity",
               "prod_line_remaining_quantity":    "s.prod_line_remaining_quantity",
               "CusNo":                           "s.CusNo",
               "CusName":                         "s.CusName",
               "CusAbbr":                         "s.CusAbbr",
               "so_abbr":                         "s.so_abbr",
               "so_type":                         "s.so_type",
               "TypeofFG":                        "s.TypeofFG",
               "Total_QTY":                       "s.Total_QTY",
               "OutstandingQty":                  "s.OutstandingQty",
               "QtytoShip":                       "s.QtytoShip",
               "QtyShipped":                      "s.QtyShipped",
               f"{MODCOL}":                       f"s.{MODCOL}",
               "updated_at":                      "s.updated_at",
               "load_ts":                         "s.load_ts",
               "source_system":                   "s.source_system",
               "row_hash":                        "s.row_hash",
           })
           .whenNotMatchedInsert(values={
               "prod_order_no":                   "s.prod_order_no",
               "FG_item_no":                      "s.FG_item_no",
               "sales_order_line_no":             "s.sales_order_line_no",
               "prod_order_due_date":             "s.prod_order_due_date",
               "prod_order_status":               "s.prod_order_status",
               "prod_order_description":          "s.prod_order_description",
               "item_routing_no":                 "s.item_routing_no",
               "prod_order_quantity":             "s.prod_order_quantity",
               "prod_order_type":                 "s.prod_order_type",
               "sales_order_no":                  "s.sales_order_no",
               "prod_order_location":             "s.prod_order_location",
               "prod_order_starting_date_time":   "s.prod_order_starting_date_time",
               "prod_order_ending_date_time":     "s.prod_order_ending_date_time",
               "prod_order_finished_date":        "s.prod_order_finished_date",
               "prod_line_quantity":              "s.prod_line_quantity",
               "prod_line_finished_quantity":     "s.prod_line_finished_quantity",
               "prod_line_remaining_quantity":    "s.prod_line_remaining_quantity",
               "CusNo":                           "s.CusNo",
               "CusName":                         "s.CusName",
               "CusAbbr":                         "s.CusAbbr",
               "so_abbr":                         "s.so_abbr",
               "so_type":                         "s.so_type",
               "TypeofFG":                        "s.TypeofFG",
               "Total_QTY":                       "s.Total_QTY",
               "OutstandingQty":                  "s.OutstandingQty",
               "QtytoShip":                       "s.QtytoShip",
               "QtyShipped":                      "s.QtyShipped",
               f"{MODCOL}":                       f"s.{MODCOL}",
               "updated_at":                      "s.updated_at",
               "load_ts":                         "s.load_ts",
               "source_system":                   "s.source_system",
               "row_hash":                        "s.row_hash",
           })
           .execute()
    )

# optional maintenance
# spark.sql(f"OPTIMIZE {TGT} ZORDER BY (prod_order_no, sales_order_line_no)")
# spark.sql(f"VACUUM {TGT} RETAIN 168 HOURS")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Plating Cycle Time

# CELL ********************

# Databricks / PySpark 3.4+
from pyspark.sql import functions as F, types as T, Window as W
from datetime import datetime, time, timedelta, date

# =============================
# Config
# =============================
CAT_SILV = "Silver_Production_Lakehouse.prod"

SILVER_STATUS_TABLE  = f"{CAT_SILV}.silver_prod_order_status"
SILVER_ROUTING_TABLE = f"{CAT_SILV}.silver_prod_routing_line"

# Exact type_name strings in the status table
TYPE_IN  = "In location in"
TYPE_OUT = "Out location"

# Only keep this physical room
LOCATION_KEEP = "PLATING ROOM"     # filter target

# Output tables
TARGET_CYCLE_TABLE   = "prod.silver_plating_room_cycle_time_hours"
TARGET_ROLLUP_TABLE  = "prod.silver_plating_room_cycle_time_hours_rollup"  # optional rollup

# Time & calendar
SESSION_TZ = "Asia/Bangkok"
TIMESTAMPS_ARE_UTC = True      # True if silver.created_on is stored as UTC
WORKDAY_START = "08:00"        # 24h hh:mm
WORKDAY_END   = "17:00"
WORKDAYS = {1,2,3,4,5}         # 1=Mon ... 7=Sun (Mon-Fri)
HOLIDAYS = set()               # e.g., {date(2025,1,1), date(2025,4,13)}

# =============================
# Session TZ
# =============================
spark.conf.set("spark.sql.session.timeZone", SESSION_TZ)

# =============================
# Load silver status events
# =============================
st = spark.table(SILVER_STATUS_TABLE).select(
    "prod_order_no", "prod_order_line_no", "item_no",
    "type_name", "created_on",
    "current_location_code", "past_location_code",
    "operation_no"
)

# Normalize timestamps to session TZ if source is UTC
if TIMESTAMPS_ARE_UTC:
    st = st.withColumn("event_ts_local", F.from_utc_timestamp(F.col("created_on"), SESSION_TZ))
else:
    st = st.withColumn("event_ts_local", F.col("created_on"))

# Keep only the two event types we care about
st = st.filter(F.col("type_name").isin(TYPE_IN, TYPE_OUT))

# Build IN events (location = current_location_code at the moment of IN)
ev_in = (
    st.filter(F.col("type_name") == F.lit(TYPE_IN))
      .select(
          "prod_order_no","prod_order_line_no","item_no",
          F.col("current_location_code").alias("location_raw"),
          F.col("operation_no").alias("op_no_at_in"),
          F.col("event_ts_local").alias("in_ts")
      )
)

# Build OUT events (location you are leaving = past_location_code)
ev_out = (
    st.filter(F.col("type_name") == F.lit(TYPE_OUT))
      .select(
          "prod_order_no","prod_order_line_no","item_no",
          F.col("past_location_code").alias("location_raw"),
          F.col("operation_no").alias("op_no_at_out"),
          F.col("event_ts_local").alias("out_ts")
      )
)

# Normalize location names (trim + uppercase) to join reliably
def normalize_location(col):
    # Remove leading/trailing spaces and uppercase
    return F.upper(F.trim(F.col(col)))

ev_in  = ev_in.withColumn("location",  normalize_location("location_raw")).drop("location_raw")
ev_out = ev_out.withColumn("location", normalize_location("location_raw")).drop("location_raw")

# =============================
# Pair IN with earliest subsequent OUT for the same (order,line,item,location)
# =============================
paired = (
    ev_in.alias("i")
    .join(
        ev_out.alias("o"),
        on=[
            F.col("i.prod_order_no")      == F.col("o.prod_order_no"),
            F.col("i.prod_order_line_no") == F.col("o.prod_order_line_no"),
            F.col("i.item_no")            == F.col("o.item_no"),
            F.col("i.location")           == F.col("o.location"),
            F.col("o.out_ts")             >= F.col("i.in_ts")
        ],
        how="left"
    )
    .withColumn(
        "rn",
        F.row_number().over(
            W.partitionBy("i.prod_order_no","i.prod_order_line_no","i.item_no","i.location","i.in_ts")
             .orderBy(F.col("o.out_ts").asc_nulls_last())
        )
    )
    .filter(F.col("rn")==1)
    .select(
        F.col("i.prod_order_no").alias("prod_order_no"),
        F.col("i.prod_order_line_no").alias("prod_order_line_no"),
        F.col("i.item_no").alias("item_no"),
        F.col("i.location").alias("location"),
        F.col("i.in_ts").alias("in_ts"),
        F.col("o.out_ts").alias("out_ts"),
        F.col("i.op_no_at_in").alias("op_no_at_in"),
        F.col("o.op_no_at_out").alias("op_no_at_out")
    )
)

# =============================
# Filter to PLATING ROOM only (normalized)
# =============================
paired = paired.where(F.col("location") == F.lit(LOCATION_KEEP.upper()))

# =============================
# Enrich with routing metadata (optional but useful)
# =============================
rt = spark.table(SILVER_ROUTING_TABLE).select(
    "prod_order_no","prod_order_line_no","item_no",
    F.col("location_code").alias("routing_location_code"),
    "operation_no","operation_type","routing_no","routing_link_code",
    "previous_operation_no","next_operation_no","run_time",
    "starting_date_time","ending_date_time"
).withColumn("routing_location_code", F.upper(F.trim(F.col("routing_location_code"))))

paired_enriched = (
    paired.alias("p")
    .join(
        rt.alias("r"),
        on=[
            F.col("p.prod_order_no")      == F.col("r.prod_order_no"),
            F.col("p.prod_order_line_no") == F.col("r.prod_order_line_no"),
            F.col("p.item_no")            == F.col("r.item_no"),
            F.col("p.location")           == F.col("r.routing_location_code")
        ],
        how="left"
    )
    # If multiple routing rows per location, pick the one whose operation_no is closest to the IN's operation_no
    .withColumn(
        "op_distance",
        F.when(
            F.col("p.op_no_at_in").isNotNull() & F.col("r.operation_no").isNotNull(),
            F.abs(F.col("r.operation_no").cast("double") - F.col("p.op_no_at_in").cast("double"))
        ).otherwise(F.lit(None))
    )
    .withColumn(
        "rnk",
        F.row_number().over(
            W.partitionBy("p.prod_order_no","p.prod_order_line_no","p.item_no","p.location","p.in_ts","p.out_ts")
             .orderBy(F.col("op_distance").asc_nulls_last(), F.col("r.operation_no").asc_nulls_last())
        )
    )
    .filter(F.col("rnk")==1)
    .select(
        "p.*",
        F.col("r.operation_no").alias("routing_operation_no"),
        "r.operation_type","r.routing_no","r.routing_link_code",
        "r.previous_operation_no","r.next_operation_no","r.run_time",
        "r.starting_date_time","r.ending_date_time"
    )
)

# =============================
# Working-hours diff UDF
# =============================
def _parse_hhmm(s: str) -> time:
    hh, mm = s.split(":")
    return time(int(hh), int(mm))

BUS_START = _parse_hhmm(WORKDAY_START)
BUS_END   = _parse_hhmm(WORKDAY_END)
HOLIDAYS_SERIAL = {d.isoformat() for d in HOLIDAYS}

@F.udf("double")
def working_hours_between(start_ts: datetime, end_ts: datetime) -> float:
    if start_ts is None or end_ts is None:
        return None
    if end_ts <= start_ts:
        return 0.0

    holidays = {datetime.fromisoformat(x).date() for x in HOLIDAYS_SERIAL}
    total = timedelta(0)
    cur = start_ts
    end = end_ts

    while cur.date() <= end.date():
        d = cur.date()
        weekday = d.isoweekday()  # 1..7 (Mon..Sun)
        if weekday in WORKDAYS and d not in holidays:
            day_start = datetime.combine(d, BUS_START, tzinfo=cur.tzinfo)
            day_end   = datetime.combine(d, BUS_END, tzinfo=cur.tzinfo)

            win_start = max(day_start, start_ts)
            win_end   = min(day_end, end_ts)

            if win_end > win_start:
                total += (win_end - win_start)

        cur = datetime.combine(d + timedelta(days=1), time(0,0), tzinfo=start_ts.tzinfo)

    return total.total_seconds() / 3600.0  # hours

# =============================
# Compute cycle time (working hours)
# =============================
cycle = (
    paired_enriched
    .withColumn("cycle_hours_working", working_hours_between(F.col("in_ts"), F.col("out_ts")))
    .withColumn("is_closed", F.col("out_ts").isNotNull())
    .select(
        "prod_order_no","prod_order_line_no","item_no","location",
        "in_ts","out_ts","is_closed","cycle_hours_working",
        "op_no_at_in","op_no_at_out",
        "routing_operation_no","operation_type","routing_no","routing_link_code",
        "previous_operation_no","next_operation_no","run_time",
        "starting_date_time","ending_date_time"
    )
)

# =============================
# Optional rollup (per order/line in PLATING ROOM)
# =============================
cycle_rollup = (
    cycle.groupBy("prod_order_no","prod_order_line_no")
         .agg(
             F.count(F.when(F.col("is_closed"), True)).alias("num_closed"),
             F.avg("cycle_hours_working").alias("avg_cycle_hours"),
             F.sum("cycle_hours_working").alias("sum_cycle_hours"),
             F.min("cycle_hours_working").alias("min_cycle_hours"),
             F.max("cycle_hours_working").alias("max_cycle_hours")
         )
)

# =============================
# Write tables
# =============================
# (cycle.write
#       .mode("overwrite")
#       .format("delta")
#       .saveAsTable(TARGET_CYCLE_TABLE))

# (cycle_rollup.write
#       .mode("overwrite")
#       .format("delta")
#       .saveAsTable(TARGET_ROLLUP_TABLE))

# # Quick sanity peek
cycle.show(20, truncate=False)
cycle_rollup.show(20, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Plating Status

# CELL ********************

# Databricks / PySpark 3.4+
from pyspark.sql import functions as F, Window as W

# ============================================================
# Config
# ============================================================
CAT_GOLD     = "Gold_Production_Lakehouse.prod"
CAT_SILV     = "Silver_Production_Lakehouse.prod"
CAT_CMN      = "Silver_Commons_Lakehouse.cmn"
CAT_SILV_INV = "Silver_Inventory_Lakehouse.inv"  # item dim lives here

SRC_PROD_ORDER      = f"{CAT_GOLD}.gold_production_order"
SRC_STATUS_GOLD     = f"{CAT_GOLD}.gold_production_status"
SRC_STATUS_SILVER   = f"{CAT_SILV}.silver_prod_order_status"    # <-- use created_on from HERE
SRC_ROUTING_LINE    = f"{CAT_SILV}.silver_prod_routing_line"
SRC_STEP_MAP        = f"{CAT_CMN}.silver_prod_step_casting_production"

# NEW: route SalesOrder via Silver header
SRC_PROD_HDR        = f"{CAT_SILV}.silver_prod_order_header"

# sales & item sources
SRC_SALES_ORDER     = f"{CAT_GOLD}.gold_sales_order"
SRC_ITEM            = f"{CAT_SILV_INV}.silver_item"             # <-- inventory lakehouse

TARGET_TABLE        = "prod.gold_plating_status"
WATERMARK_TABLE     = "wm._watermark_plating_status"

DEFAULT_LOOKBACK_DAYS    = 30
CUMU_TO_PLATING_MAX_DAYS = 10.0
SESSION_TZ               = "Asia/Bangkok"

# If your timestamp columns are stored in UTC, keep True. If already local (BKK), set False.
IS_SILVER_CREATED_ON_UTC = True
IS_GOLD_CREATED_ON_UTC   = True

# ============================================================
# Session settings
# ============================================================
spark.conf.set("spark.sql.session.timeZone", SESSION_TZ)

# ============================================================
# Helpers
# ============================================================
def uptrim(col):
    return F.upper(F.trim(col))

def to_seqno(col):
    # normalize: '010.01' or '10,01' -> 10.01
    return F.regexp_replace(F.trim(col), ",", ".").cast("double")

def add_days_fraction(base_ts_col, days_col):
    # base_ts_col: timestamp; days_col: numeric
    return F.to_timestamp(
        F.from_unixtime(
            F.unix_timestamp(base_ts_col) + (days_col.cast("double") * F.lit(86400.0))
        )
    )

# Fixed durations — the ONLY source of step duration
duration_map = F.create_map(
    [F.lit(k) for kv in [
        ('FIL',2.0),('HT',1.0),('TUM',1.0),('LAS',1.0),('SET',1.0),
        ('POL',2.0),('SHI',1.0),('PLT',1.0),('GLU',2.0),('QC',1.0),
        ('QA',1.0),('C INS',1.0),('PCK',1.0)
    ] for k in kv]
)

allowed_abbr = ['FIL','HT','TUM','LAS','SET','POL','SHI','PLT','GLU','QC','QA','C INS','PCK']
allowed_df = spark.createDataFrame([(a,) for a in allowed_abbr], ["abbr"])

# ============================================================
# Watermark read/init
# ============================================================
if spark._jsparkSession.catalog().tableExists(WATERMARK_TABLE):
    wm_ts = spark.table(WATERMARK_TABLE).select("last_processed_ts").head()[0]
else:
    wm_ts = spark.range(1).select((F.current_timestamp() - F.expr(f"INTERVAL {DEFAULT_LOOKBACK_DAYS} DAYS")).alias("last_processed_ts")).head()[0]
    (spark.createDataFrame([(wm_ts,)], ["last_processed_ts"])
         .write.mode("overwrite").format("delta").saveAsTable(WATERMARK_TABLE))

# ============================================================
# Load sources
# ============================================================
# include sales_order_no + FG_item_no for enrichment joins (still useful as fallback/keys)
prod_order = spark.table(SRC_PROD_ORDER).select(
    "prod_order_no", "prod_order_line_no", "prod_item_line",
    "prod_line_quantity", "prod_order_status", "prod_line_start_date",
    "sales_order_no", "FG_item_no"
)

# GOLD status (structure for matching ops & locations)
status_gold_raw = spark.table(SRC_STATUS_GOLD).select(
    "prod_order_no", "prod_order_line_no", "operation_no",
    F.col("CorrectCurrentLocation").alias("StatusRoutingNo"),
    F.col("created_on").alias("created_on_gold")
)

status_gold = (status_gold_raw
    .withColumn(
        "created_on_gold_local",
        F.from_utc_timestamp("created_on_gold", SESSION_TZ) if IS_GOLD_CREATED_ON_UTC else F.col("created_on_gold")
    )
)

# SILVER status (we only trust/use created_on here)
status_silver_raw = spark.table(SRC_STATUS_SILVER).select(
    "prod_order_no", "prod_order_line_no", "operation_no",
    F.col("created_on").alias("created_on_silver")
)

status_silver = (status_silver_raw
    .withColumn(
        "created_on_silver_local",
        F.from_utc_timestamp("created_on_silver", SESSION_TZ) if IS_SILVER_CREATED_ON_UTC else F.col("created_on_silver")
    )
)

routing_line = spark.table(SRC_ROUTING_LINE).select(
    "prod_order_no", "prod_order_line_no", "item_no", "operation_no",
    "routing_no", "operation_type"
)

# step_map
step_map_norm = spark.table(SRC_STEP_MAP).select(
    uptrim("current_operation").alias("MapKey"),
    uptrim("operation_description").alias("DescKey"),
    uptrim("operation_group").alias("OpGroup"),
    uptrim("operation_abb").alias("OpAbb"),
    F.col("Pro Due Date Offset_Day_").cast("decimal(10,2)").alias("OffsetDays"),
    F.col("current_operation").alias("CurrentOperationRaw")
)

# ============================================================
# Incremental filter (use SILVER created_on for "what changed")
# ============================================================
changed_orders = (status_silver
    .where(F.col("created_on_silver_local") >= F.lit(wm_ts))
    .select("prod_order_no").distinct()
)

changed_orders2 = (prod_order
    .where(F.col("prod_line_start_date") >= (F.lit(wm_ts).cast("timestamp") - F.expr("INTERVAL 7 DAYS")))
    .select("prod_order_no").distinct()
)

changed = changed_orders.unionByName(changed_orders2).distinct()

# ============================================================
# Short-circuit if nothing to do
# ============================================================
if changed.isEmpty():
    print("No changes since watermark; nothing to do.")
    now_ts = spark.range(1).select(F.current_timestamp().alias("last_processed_ts"))
    now_ts.write.mode("overwrite").format("delta").saveAsTable(WATERMARK_TABLE)

else:
    # Reduce sources to changed orders
    prod_order_chg    = prod_order.join(changed, "prod_order_no")
    status_gold_chg   = status_gold.join(changed, "prod_order_no")
    status_silver_chg = status_silver.join(changed, "prod_order_no")
    routing_chg       = routing_line.join(changed, "prod_order_no")

    # ========================================================
    # 0) Orders/Line/Item & Start Dates
    # ========================================================
    line0 = (prod_order_chg
        .where(F.col("prod_order_status") == F.lit("Released"))
        .where(~F.col("prod_order_no").like("C%"))  # drop C* orders
        .withColumn("StartDatePlan", F.to_date("prod_line_start_date"))  # session TZ
        .withColumn(
            "rn",
            F.row_number().over(
                W.partitionBy("prod_order_no", "prod_order_line_no", "prod_item_line")
                 .orderBy(F.col("prod_line_start_date").desc())
            )
        )
    )

    # Actual start: earliest SILVER created_on per order line (fallback to GOLD if no silver)
    silver_first = (status_silver_chg
        .where(F.col("operation_no").isNotNull() & (F.length(F.trim("operation_no")) > 0))
        .groupBy("prod_order_no","prod_order_line_no")
        .agg(F.min("created_on_silver_local").alias("StartTSActual_silver"))
    )

    gold_first = (status_gold_chg
        .where(F.col("operation_no").isNotNull() & (F.length(F.trim("operation_no")) > 0))
        .groupBy("prod_order_no","prod_order_line_no")
        .agg(F.min("created_on_gold_local").alias("StartTSActual_gold"))
    )

    start_from_status = (silver_first
        .join(gold_first, ["prod_order_no","prod_order_line_no"], "full")
        .withColumn("StartTSActual", F.coalesce("StartTSActual_silver","StartTSActual_gold"))
        .select(
            "prod_order_no","prod_order_line_no",
            "StartTSActual",
            F.to_date("StartTSActual").alias("StartDateActual")
        )
    )

    line = (line0.where(F.col("rn")==1)
        .join(start_from_status, ["prod_order_no","prod_order_line_no"], "left")
        .select("prod_order_no","prod_order_line_no","prod_item_line","prod_line_quantity",
                "StartDatePlan","StartTSActual","StartDateActual")
    )

    # ========================================================
    # 1) Routing → normalize/map
    # ========================================================
    mc_name = step_map_norm.select(
        uptrim("CurrentOperationRaw").alias("mc_key"),
        F.col("CurrentOperationRaw").alias("mc_name_raw")
    ).distinct()

    routing_raw = (routing_chg
        .join(line.select("prod_order_no","prod_order_line_no").distinct(), ["prod_order_no","prod_order_line_no"])
        .where(uptrim("operation_type") == F.lit("MACHINE CENTER"))
        .select(
            "prod_order_no","prod_order_line_no","item_no",
            to_seqno(F.col("operation_no")).alias("SeqNo"),
            "routing_no"
        )
        .join(mc_name, uptrim("routing_no") == mc_name.mc_key, "left")
        .select(
            "prod_order_no","prod_order_line_no","item_no","SeqNo",
            F.coalesce(F.col("mc_name_raw"), F.col("routing_no")).alias("OpName"),
            F.col("routing_no").alias("RoutingCode")
        )
    )

    routing_norm = routing_raw.withColumn("OpNameU", uptrim("OpName"))

    # Map by exact current_operation then fallback contains(description)
    map_exact = step_map_norm.select("MapKey","OpGroup","OpAbb").withColumnRenamed("MapKey","k")
    rn_exact = (routing_norm
        .join(map_exact, routing_norm.OpNameU == map_exact.k, "left")
        .select(
            routing_norm["*"],
            map_exact["OpGroup"].alias("MappedGroup_exact"),
            map_exact["OpAbb"].alias("MappedAbbrev_exact")
        )
    )

    map_fallback = (step_map_norm
        .where(F.length("DescKey")>0)
        .select("DescKey","OpGroup","OpAbb").withColumnRenamed("DescKey","d")
    )

    rn_fb = (rn_exact
        .join(map_fallback, rn_exact.OpNameU.isNotNull() & map_fallback.d.contains(rn_exact.OpNameU), "left")
        .select(
            rn_exact["*"],
            map_fallback["OpGroup"].alias("MappedGroup_fb"),
            map_fallback["OpAbb"].alias("MappedAbbrev_fb")
        )
    )

    routing_map = (rn_fb
        .withColumn("MappedGroup", F.coalesce("MappedGroup_exact","MappedGroup_fb"))
        .withColumn("MappedAbbrev", F.coalesce("MappedAbbrev_exact","MappedAbbrev_fb"))
        .select("prod_order_no","prod_order_line_no","item_no","SeqNo","RoutingCode","OpName","OpNameU","MappedGroup","MappedAbbrev")
    )

    routing_allowed = (routing_map
        .join(allowed_df, uptrim("MappedAbbrev")==allowed_df.abbr, "inner")
        .drop("abbr")
    )

    # ========================================================
    # 2) Duration + cumulative (ONLY duration_map; cap 10)
    # ========================================================
    routing_dur = (routing_allowed
        .withColumn(
            "DurationDays",
            F.round(F.coalesce(F.element_at(duration_map, uptrim("MappedAbbrev")), F.lit(0.0)), 2)
            .cast("decimal(10,2)")
        )
    )

    routing_cumu = (routing_dur
        .withColumn(
            "CumuDaysToThisStep_raw",
            F.sum("DurationDays").over(
                W.partitionBy("prod_order_no","prod_order_line_no","item_no")
                 .orderBy("SeqNo")
                 .rowsBetween(W.unboundedPreceding, W.currentRow)
            )
        )
        .withColumn(
            "CumuDaysToThisStep",
            F.least(F.col("CumuDaysToThisStep_raw").cast("double"), F.lit(CUMU_TO_PLATING_MAX_DAYS))
        )
        .drop("CumuDaysToThisStep_raw")
    )

    # Dedup per SeqNo
    routing_cumu_dedup = (routing_cumu
        .withColumn(
            "rn_seq",
            F.row_number().over(
                W.partitionBy("prod_order_no","prod_order_line_no","item_no","SeqNo")
                 .orderBy(F.col("OpName").desc())
            )
        )
        .where(F.col("rn_seq")==1)
        .drop("rn_seq")
    )

    # ========================================================
    # 3) First plating step per item
    # ========================================================
    plating_candidates = (routing_cumu_dedup
        .where( (uptrim("MappedGroup")==F.lit("PLATING")) |
                (uptrim("MappedAbbrev")==F.lit("PLT")) |
                (F.col("OpNameU").contains("PLAT")) )
        .withColumn("rn_plating",
            F.row_number().over(
                W.partitionBy("prod_order_no","prod_order_line_no","item_no").orderBy("SeqNo")
            )
        )
    )

    plating_target = (plating_candidates
        .where(F.col("rn_plating")==1)
        .select(
            "prod_order_no","prod_order_line_no","item_no",
            F.col("CumuDaysToThisStep").alias("CumuToPlatingDays"),
            F.col("SeqNo").alias("PlatingSeqNo")
        )
    )

    line_with_plating = (line
        .join(plating_target.select("prod_order_no","prod_order_line_no","item_no").withColumnRenamed("item_no","prod_item_line"),
              ["prod_order_no","prod_order_line_no","prod_item_line"], "inner")
    )

    # ========================================================
    # 4) Latest status & current step matching
    # ========================================================
    latest_silver = (status_silver_chg
        .groupBy("prod_order_no","prod_order_line_no","operation_no")
        .agg(F.max("created_on_silver_local").alias("LastSeenAt_silver"))
    )

    latest_gold = (status_gold_chg
        .groupBy("prod_order_no","prod_order_line_no","operation_no")
        .agg(F.max("created_on_gold_local").alias("LastSeenAt_gold"))
    )

    latest_ts = (latest_silver
        .join(latest_gold, ["prod_order_no","prod_order_line_no","operation_no"], "full")
        .withColumn("LastSeenAt", F.coalesce("LastSeenAt_silver","LastSeenAt_gold"))
        .select("prod_order_no","prod_order_line_no","operation_no","LastSeenAt")
    )

    gold_ops = status_gold_chg.select(
        "prod_order_no","prod_order_line_no","operation_no","StatusRoutingNo"
    ).distinct()

    latest_status = (gold_ops
        .join(latest_ts, ["prod_order_no","prod_order_line_no","operation_no"], "left")
        .where(F.col("LastSeenAt").isNotNull())
    )

    numeric_match = (routing_cumu_dedup.alias("rc")
        .join(latest_status.alias("ls"),
              (F.col("rc.prod_order_no")==F.col("ls.prod_order_no")) &
              (F.col("rc.prod_order_line_no")==F.col("ls.prod_order_line_no")) &
              (F.col("rc.SeqNo") == to_seqno(F.col("ls.operation_no"))),
              "inner")
        .select(
            "rc.prod_order_no","rc.prod_order_line_no","rc.item_no","rc.SeqNo","rc.OpName",
            "rc.MappedGroup","rc.MappedAbbrev","rc.CumuDaysToThisStep",
            "ls.LastSeenAt",
            F.lit("NUM").alias("MatchType"),
            F.lit(3).alias("MatchRank")
        )
    )

    name_match_exact = (routing_cumu_dedup.alias("rc")
        .join(latest_status.alias("ls"),
              (F.col("rc.prod_order_no")==F.col("ls.prod_order_no")) &
              (F.col("rc.prod_order_line_no")==F.col("ls.prod_order_line_no")) &
              (
                (uptrim(F.col("ls.StatusRoutingNo")) == F.col("rc.OpNameU")) |
                (uptrim(F.col("ls.StatusRoutingNo")) == uptrim(F.col("rc.RoutingCode")))
              ),
              "inner")
        .select(
            "rc.prod_order_no","rc.prod_order_line_no","rc.item_no","rc.SeqNo","rc.OpName",
            "rc.MappedGroup","rc.MappedAbbrev","rc.CumuDaysToThisStep",
            "ls.LastSeenAt",
            F.lit("NAME_EQ").alias("MatchType"),
            F.lit(2).alias("MatchRank")
        )
    )

    name_match_partial = (routing_cumu_dedup.alias("rc")
        .join(latest_status.alias("ls"),
              (F.col("rc.prod_order_no")==F.col("ls.prod_order_no")) &
              (F.col("rc.prod_order_line_no")==F.col("ls.prod_order_line_no")) &
              (
                F.col("rc.OpNameU").contains(uptrim(F.col("ls.StatusRoutingNo"))) |
                uptrim(F.col("ls.StatusRoutingNo")).contains(F.col("rc.OpNameU"))
              ),
              "inner")
        .select(
            "rc.prod_order_no","rc.prod_order_line_no","rc.item_no","rc.SeqNo","rc.OpName",
            "rc.MappedGroup","rc.MappedAbbrev","rc.CumuDaysToThisStep",
            "ls.LastSeenAt",
            F.lit("NAME_LIKE").alias("MatchType"),
            F.lit(1).alias("MatchRank")
        )
    )

    u = numeric_match.unionByName(name_match_exact).unionByName(name_match_partial)

    current_step = (u
        .withColumn(
            "rn",
            F.row_number().over(
                W.partitionBy("prod_order_no","prod_order_line_no","item_no")
                 .orderBy(F.col("MatchRank").desc(), F.col("LastSeenAt").desc(), F.col("SeqNo").desc())
            )
        )
        .where(F.col("rn")==1)
        .select(
            "prod_order_no","prod_order_line_no","item_no","SeqNo","OpName",
            "MappedGroup","MappedAbbrev","CumuDaysToThisStep","LastSeenAt","MatchType"
        )
    )

    next_step = (
        routing_cumu_dedup
        .withColumn("NextSeqNo", F.lead("SeqNo").over(
            W.partitionBy("prod_order_no","prod_order_line_no","item_no").orderBy("SeqNo")
        ))
        .withColumn("NextOpName", F.lead("OpName").over(
            W.partitionBy("prod_order_no","prod_order_line_no","item_no").orderBy("SeqNo")
        ))
        .withColumn("NextGroup", F.lead("MappedGroup").over(
            W.partitionBy("prod_order_no","prod_order_line_no","item_no").orderBy("SeqNo")
        ))
        .withColumn("NextAbbrev", F.lead("MappedAbbrev").over(           
            W.partitionBy("prod_order_no","prod_order_line_no","item_no").orderBy("SeqNo")
        ))
        .select(
            "prod_order_no","prod_order_line_no","item_no","SeqNo",
            "NextSeqNo","NextOpName","NextGroup","NextAbbrev"             
        )
    )

    next_from_current = (
        current_step.alias("cp")
        .join(next_step.alias("ns"),
            (F.col("cp.prod_order_no")==F.col("ns.prod_order_no")) &
            (F.col("cp.prod_order_line_no")==F.col("ns.prod_order_line_no")) &
            (F.col("cp.item_no")==F.col("ns.item_no")) &
            (F.col("cp.SeqNo")==F.col("ns.SeqNo")), "left")
        .select(
            F.col("cp.prod_order_no"),
            F.col("cp.prod_order_line_no"),
            F.col("cp.item_no"),
            F.col("ns.NextSeqNo"),
            F.col("ns.NextOpName"),
            F.col("ns.NextGroup"),
            F.col("ns.NextAbbrev")                                       
        )
    )

    # ========================================================
    # 5) Sums around plating
    # ========================================================
    sum_to_plating = plating_target.select(
        "prod_order_no","prod_order_line_no","item_no",
        F.col("CumuToPlatingDays"),
        F.col("PlatingSeqNo")
    )

    cumu_before_now = (routing_cumu_dedup.alias("rc")
        .join(current_step.alias("cp"),
              (F.col("rc.prod_order_no")==F.col("cp.prod_order_no")) &
              (F.col("rc.prod_order_line_no")==F.col("cp.prod_order_line_no")) &
              (F.col("rc.item_no")==F.col("cp.item_no")), "inner")
        .where(F.col("rc.SeqNo") < F.col("cp.SeqNo"))
        .groupBy("cp.prod_order_no","cp.prod_order_line_no","cp.item_no")
        .agg(F.max("rc.CumuDaysToThisStep").alias("CumuBeforeDays"))
        .withColumn("CumuBeforeDays", F.coalesce(F.col("CumuBeforeDays"), F.lit(0.0)))
    )

    # ========================================================
    # 6) Per-item result + ETA
    # ========================================================
    lwp = (line_with_plating
        .withColumnRenamed("prod_item_line","item_no")
    )

    result_per_item = (lwp.alias("lwp")
        .join(sum_to_plating.alias("stp"),
              ["prod_order_no","prod_order_line_no","item_no"], "inner")
        .join(current_step.alias("cp"),
              ["prod_order_no","prod_order_line_no","item_no"], "left")
        .join(next_from_current.alias("nfc"),
              ["prod_order_no","prod_order_line_no","item_no"], "left")
        .join(cumu_before_now.alias("cbn"),
              ["prod_order_no","prod_order_line_no","item_no"], "left")

        .withColumn(
            "PlannedPlating_FromOrderStart",
            F.when(F.col("lwp.StartDatePlan").isNotNull(),
                   add_days_fraction(F.to_timestamp("lwp.StartDatePlan"),
                                     F.col("stp.CumuToPlatingDays"))).otherwise(None)
        )
        .withColumn(
            "PlannedPlating_FromActualStart",
            F.when(F.col("lwp.StartTSActual").isNotNull(),
                   add_days_fraction(F.col("lwp.StartTSActual"),
                                     F.col("stp.CumuToPlatingDays"))).otherwise(None)
        )

        .withColumn(
            "RemainToPlatingDays_fromNow",
            F.when(F.col("cp.SeqNo").isNotNull(),
                   F.greatest(
                       F.lit(0.0),
                       F.col("stp.CumuToPlatingDays").cast("double")
                       - F.coalesce(F.col("cbn.CumuBeforeDays").cast("double"), F.lit(0.0))
                   )
            ).otherwise(None)
        )

        .withColumn(
            "BaseTS",
            F.when(F.col("cp.SeqNo").isNotNull(),
                   F.greatest(F.col("cp.LastSeenAt"), F.current_timestamp())).otherwise(None)
        )

        .withColumn(
            "ETA_FromCurrentStatus",
            F.when(
                F.col("cp.SeqNo").isNotNull() & F.col("RemainToPlatingDays_fromNow").isNotNull(),
                add_days_fraction(F.col("BaseTS"), F.col("RemainToPlatingDays_fromNow"))
            ).otherwise(None)
        )

        .withColumn(
            "PlatingStatus",
            F.when(F.col("cp.MappedAbbrev").isNull(), F.lit("Not Start"))
             .when(uptrim("cp.MappedAbbrev").isin('FIL','HT','TUM','LAS','SET','POL','SHI'), F.lit("Waiting to Plating"))
             .when(uptrim("cp.MappedAbbrev") == 'PLT', F.lit("In Plating"))
             .when(uptrim("cp.MappedAbbrev").isin('GLU','QC','QA','C INS','PCK'), F.lit("Out of Plating"))
             .otherwise(F.lit("Not Start"))
        )

        .select(
            F.col("lwp.prod_order_no"),
            F.col("lwp.prod_order_line_no"),
            F.col("lwp.prod_line_quantity"),
            F.concat_ws("-", F.col("lwp.prod_order_no"), F.col("lwp.prod_order_line_no").cast("string")).alias("pol"),
            F.col("lwp.item_no"),

            F.col("lwp.StartDatePlan"),
            F.col("lwp.StartTSActual"),
            F.col("lwp.StartDateActual"),

            F.col("stp.CumuToPlatingDays"),

            F.col("PlannedPlating_FromOrderStart"),
            F.col("PlannedPlating_FromActualStart"),

            F.col("cp.SeqNo").alias("CurrentSeqNo"),
            F.col("cp.OpName").alias("CurrentOpName"),
            F.col("cp.LastSeenAt").alias("StatusLastSeenAt"),
            F.col("cp.MappedAbbrev").alias("CurrentAbbrev"),

            F.col("nfc.NextSeqNo"),
            F.col("nfc.NextOpName"),
            F.col("nfc.NextAbbrev"),

            F.col("RemainToPlatingDays_fromNow"),
            F.col("ETA_FromCurrentStatus"),
            F.col("PlatingStatus")
        )
    )

    # ========================================================
    # 7) Pick representative item per (order, line)
    # ========================================================
    result_per_line = (result_per_item
        .withColumn("PlannedPlatingForRanking",
            F.coalesce(F.col("PlannedPlating_FromActualStart"),
                       F.col("PlannedPlating_FromOrderStart"))
        )
        .withColumn("rn",
            F.row_number().over(
                W.partitionBy("prod_order_no","prod_order_line_no")
                 .orderBy(F.col("PlannedPlatingForRanking").asc())
            )
        )
        .where(F.col("rn")==1)
        .drop("rn","PlannedPlatingForRanking")
    )

    # ========================================================
    # 7.1) Enrich via SILVER header -> GOLD sales_order, and ITEM dim
    #      (adds SalesorderNo, CusNo, CusName, CusAbbr, SOAbbr, item_metal_category, item_category)
    # ========================================================
    # SILVER header gives authoritative mapping from prod_order_no -> sales_order_no
    prod_hdr = (spark.table(SRC_PROD_HDR).select(
        "prod_order_no",
        uptrim("sales_order_no").alias("sales_order_no_u_hdr")
    ).dropDuplicates(["prod_order_no"]))

    # Fallback to GOLD prod_order.sales_order_no if header missing/blank
    prod_keys = (prod_order.select(
        "prod_order_no","prod_order_line_no",
        uptrim("sales_order_no").alias("sales_order_no_u_gold"),
        uptrim("FG_item_no").alias("FG_item_no_u")
    ))

    prod_keys_joined = (prod_keys
        .join(prod_hdr, on="prod_order_no", how="left")
        .withColumn(
            "sales_order_no_u",
            F.coalesce(F.col("sales_order_no_u_hdr"), F.col("sales_order_no_u_gold"))
        )
        .select("prod_order_no","prod_order_line_no","sales_order_no_u","FG_item_no_u")
    )

    # GOLD sales order — pick needed attributes
    sales_so = (spark.table(SRC_SALES_ORDER).select(
        uptrim("SalesorderNo").alias("SalesorderNo_u"),
        F.col("SalesorderNo").alias("SalesorderNo"),
        F.col("CusNo").alias("CusNo"),
        F.col("CusName").alias("CusName"),
        F.col("CusAbbr").alias("CusAbbr"),
        F.col("so_abbr").alias("SOAbbr")
    ))

    # Inventory item dim
    items_dim = (spark.table(SRC_ITEM).select(
        uptrim("item_no").alias("item_no_u"),
        F.col("item_metal_category").alias("item_metal_category"),
        F.col("item_category").alias("item_category")
    ))

    # Join prod -> sales (through header mapping) and prod -> item
    prod_so = (prod_keys_joined
        .join(sales_so, on=(F.col("sales_order_no_u") == F.col("SalesorderNo_u")), how="left")
        .select(
            "prod_order_no","prod_order_line_no",
            "SalesorderNo","CusNo","CusName","CusAbbr","SOAbbr","FG_item_no_u"
        )
    )

    prod_so_item = (prod_so
        .join(items_dim, on=(F.col("FG_item_no_u") == F.col("item_no_u")), how="left")
        .select(
            "prod_order_no","prod_order_line_no",
            "SalesorderNo","CusNo","CusName","CusAbbr","SOAbbr",
            "item_metal_category","item_category"
        )
        .dropDuplicates(["prod_order_no","prod_order_line_no"])
    )

    result_enriched = (result_per_line.alias("r")
        .join(prod_so_item.alias("x"), on=["prod_order_no","prod_order_line_no"], how="left")
        .select(
            "r.*",
            "x.SalesorderNo",
            "x.CusNo",
            "x.CusName",
            "x.CusAbbr",
            "x.SOAbbr",
            "x.item_metal_category",
            "x.item_category"
        )
    )

    # ========================================================
    # MERGE into Delta (incremental upsert)
    # ========================================================
    from delta.tables import DeltaTable
    if spark._jsparkSession.catalog().tableExists(TARGET_TABLE):
        tgt = DeltaTable.forName(spark, TARGET_TABLE)
        (tgt.alias("t")
            .merge(result_enriched.alias("s"),
                   "t.prod_order_no = s.prod_order_no AND t.prod_order_line_no = s.prod_order_line_no")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (result_enriched.write.mode("overwrite").format("delta").saveAsTable(TARGET_TABLE))

    # Update watermark to "now"
    (spark.range(1)
       .select(F.current_timestamp().alias("last_processed_ts"))
       .write.mode("overwrite").format("delta").saveAsTable(WATERMARK_TABLE))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
