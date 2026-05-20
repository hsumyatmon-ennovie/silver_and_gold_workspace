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
# META           "id": "e248ea90-8431-4df2-9f29-87866bf9dd5a"
# META         },
# META         {
# META           "id": "ad99fdfa-85b1-4480-9f7f-2640bfd65f24"
# META         },
# META         {
# META           "id": "869b263b-1a86-424b-bd97-94bd586442b2"
# META         },
# META         {
# META           "id": "ff4d6787-a716-43b6-baaf-972b7426ffa5"
# META         },
# META         {
# META           "id": "3ea0efcd-03d5-44f1-8e70-99f52a5c2a22"
# META         },
# META         {
# META           "id": "3a130b81-98ec-4fd4-a404-95edc1f0ef1e"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # One Time Run

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
 
# Initialize Spark session
spark = SparkSession.builder.appName("BronzeWaxTeam").getOrCreate()
 
# Define schema
schema = StructType([
    StructField("team", IntegerType(), True),
    StructField("reader_id", StringType(), True),
    StructField("antenna_id", IntegerType(), True)
])
 
# Define data
data = [
    (1, "wax_room_1", 1),
    (1, "wax_room_1", 2),
    (2, "wax_room_1", 3),
    (2, "wax_room_1", 4),
    (2, "wax_room_1", 5),
    (2, "wax_room_1", 6),
    (2, "wax_room_1", 7),
    (2, "wax_room_1", 8),
    (3, "wax_room_1", 9),
    (3, "wax_room_1", 10),
    (3, "wax_room_1", 11),
    (3, "wax_room_1", 12),
    (3, "wax_room_1", 13),
    (3, "wax_room_1", 14),
    (4, "wax_room_2", 1),
    (4, "wax_room_2", 2),
    (4, "wax_room_2", 3),
    (4, "wax_room_2", 4),
    (4, "wax_room_2", 5),
    (4, "wax_room_2", 6),
    (5, "wax_room_2", 7),
    (5, "wax_room_2", 8),
    (5, "wax_room_2", 9),
    (5, "wax_room_2", 10),
    (5, "wax_room_2", 11),
    (5, "wax_room_2", 12)
]
 
# Create DataFrame
df = spark.createDataFrame(data, schema=schema)
 
# Write to Delta table (bronze layer)
df.write.format("delta").mode("overwrite").saveAsTable("Silver_Production_Lakehouse.prod.silver_wax_team")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

from pyspark.sql import functions as F

# --- Sources (match your T-SQL) ---
e = spark.table("Silver_Commons_Lakehouse.cmn.silver_employee_rfid_mapping").alias("e")
t = spark.table("Silver_Production_Lakehouse.prod.silver_wax_team").alias("t")

# ---------- a_split ----------
# OUTER APPLY STRING_SPLIT(e.antenna_id, ',') and TRY_CONVERT(int, TRIM(value))
# - explode_outer keeps rows even if antenna_id is null (like OUTER APPLY)
a_split = (
    e.select(
        "e.created_on",
        "e.modified_on",
        "e.Employee_Code",
        "e.First_Name_Eng",
        "e.Last_Name_Eng",
        "e.sub_department_Eng",
        "e.machine_center_no",
        "e.reader_id",
        # piece from splitting antenna_id on commas (treat antenna_id as string)
        F.explode_outer(F.split(F.col("e.antenna_id").cast("string"), ",")).alias("piece")
    )
    .withColumn("piece_trim", F.trim(F.col("piece")))
    .withColumn(
        "antenna_num",
        F.when(F.col("piece_trim").rlike(r"^[+-]?\d+$"), F.col("piece_trim").cast("int"))
         .otherwise(F.lit(None))
    )
)

# ---------- a_norm ----------
# WHERE antenna_num IS NOT NULL; antenna_id = CAST(antenna_num AS varchar(10))
# mkey = CASE WHEN antenna_num IS NOT NULL THEN reader_id + antenna_num_str ELSE NULL END
a_norm = (
    a_split
    .filter(F.col("antenna_num").isNotNull())
    .withColumn("antenna_id", F.col("antenna_num").cast("string"))
    .withColumn(
        "mkey",
        F.when(F.col("antenna_num").isNotNull(),
               F.concat_ws("", F.col("reader_id"), F.col("antenna_num").cast("string")))
         .otherwise(F.lit(None))
    )
    .select(
        "created_on",
        "modified_on",
        "Employee_Code",
        "First_Name_Eng",
        "Last_Name_Eng",
        "sub_department_Eng",
        "machine_center_no",
        "reader_id",
        "antenna_id",   # numeric-only as string
        "mkey"
    )
)

# ---------- b_norm ----------
# antenna_id := CAST(TRY_CONVERT(int, LTRIM(RTRIM(t.antenna_id))) AS varchar(10))
# mkey := CONCAT(reader_id, antenna_id_numeric_str)  -- CONCAT(null, x) -> '' in SQL Server
# Use concat_ws('', ...) to mirror CONCAT’s NULL->'' behavior.
b_norm = (
    t.select("t.team", "t.reader_id", "t.antenna_id")
     .withColumn("ant_trim", F.trim(F.col("t.antenna_id").cast("string")))
     .withColumn(
         "antenna_num",
         F.when(F.col("ant_trim").rlike(r"^[+-]?\d+$"), F.col("ant_trim").cast("int"))
          .otherwise(F.lit(None))
     )
     .withColumn("antenna_id", F.col("antenna_num").cast("string"))
     .withColumn("mkey", F.concat_ws("", F.col("t.reader_id"), F.col("antenna_id")))
     .select(
         F.col("team"),
         F.col("t.reader_id").alias("reader_id"),
         F.col("antenna_id"),
         F.col("mkey")
     )
)

# ---------- Final SELECT with LEFT JOIN on mkey ----------
result = (
    a_norm.alias("a")
          .join(b_norm.alias("b"), on="mkey", how="left")
          .select(
              F.col("a.created_on").alias("created_on"),
              F.col("a.modified_on").alias("modified_on"),
              F.col("a.Employee_Code").alias("Employee_Code"),
              F.col("a.First_Name_Eng").alias("First_Name_Eng"),
              F.col("a.Last_Name_Eng").alias("Last_Name_Eng"),
              F.col("a.antenna_id").alias("antenna_id"),            # numeric-only (e.g., '7' from '7,8')
              F.col("a.sub_department_Eng").alias("sub_department_Eng"),
              F.col("a.machine_center_no").alias("machine_center_no"),
              F.col("a.reader_id").alias("reader_id"),
              F.col("a.mkey").alias("mkey"),
              F.col("b.team").alias("team"),
          )
)

# Instead of this (Databricks-only)
# result.display()

# Use standard PySpark methods:
result.show(truncate=False)             # Print preview to stdout
result.printSchema()                    # Optional: show structure

# ---- Save the results ----
result.write.format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .saveAsTable("Gold_Production_Lakehouse.prod.gold_emp_team_full")

# Optional filtered variant (like your WHERE clause)
result.filter(F.col("sub_department_Eng") == "WAX ROOM") \
      .write.format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .saveAsTable("Gold_Production_Lakehouse.prod.gold_emp_team_wax")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
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

# # Production Casting Status

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
    return (
        df.groupBy(*[F.col(k) for k in keys])
          .count()
          .filter(F.col("count") > 1)
          .count()
    )

def show_dupes(df, keys, n=20):
    d = (
        df.groupBy(*[F.col(k) for k in keys])
          .count()
          .filter(F.col("count") > 1)
          .orderBy(F.col("count").desc())
    )
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
    ords.append(
        F.sha2(
            F.concat_ws("§", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in df.columns]),
            256
        ).desc()
    )
    w = Window.partitionBy(*keys).orderBy(*ords)
    deduped = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn")==1).drop("_rn")
    before, after = df.count(), deduped.count()
    if after < before:
        print(f"[TARGET CLEANUP] {target_table}: removed {before-after:,} duplicate rows")
        (
            deduped.write
                  .format("delta")
                  .mode("overwrite")
                  .option("overwriteSchema","true")
                  .saveAsTable(target_table)
        )
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
# sales_order_line_no comes from pl.sales_order_line_no
prod_order = (
    po.alias("po")
      .join(pl.alias("pl"), F.col("po.prod_order_no")==F.col("pl.prod_order_no"), how="left")
      .select(
          F.col("po.sales_order_no").alias("sales_order_no"),
          F.col("pl.sales_order_line_no").alias("sales_order_line_no"),

          F.col("po.prod_order_no").alias("prod_order_no"),
          F.col("pl.prod_order_line_no").alias("prod_order_line_no"),
          F.col("po.FG_item_no").alias("FG_item_no"),
          F.col("po.item_routing_no").alias("item_routing_no"),
          F.col("po.prod_order_starting_date_time").alias("prod_order_starting_date_time"),
          F.col("po.prod_order_ending_date_time").alias("prod_order_ending_date_time"),
          F.col("po.prod_order_due_date").alias("prod_order_due_date"),
          F.weekofyear(F.col("po.prod_order_due_date")).alias("commit_week"),
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

          F.concat_ws("", F.col("po.sales_order_no"), F.col("pl.sales_order_line_no")).alias("SOL"),
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
        F.when(
            F.col("current_location_code").isNull() | (F.length("current_location_code")==0),
            F.col("machine_center_no")
        )
        .when(F.upper(F.substring(F.col("current_location_code"),1,4))=="CELL", F.col("machine_center_no"))
        .otherwise(F.col("current_location_code"))
    )
)

w_stat = (
    Window
      .partitionBy("prod_order_no","prod_order_line_no","type_name","Prod_Status")
      .orderBy(F.col("created_on").desc_nulls_last())
)

dedup_status = (
    latest_status
      .withColumn("_rn", F.row_number().over(w_stat))
      .filter(F.col("_rn")==1)
      .drop("_rn")
)

dedup_status_f = (
    dedup_status
      .withColumn("type_name_norm", F.upper(F.trim(F.col("type_name"))))
      .withColumn("open_norm",      F.upper(F.trim(F.col("open"))))
      .withColumn("status_norm",    F.upper(F.trim(F.col("pos_status"))))
      .filter(
          (F.col("type_name_norm")=="IN LOCATION IN") &
          (F.col("open_norm")=="YES") &
          (F.col("status_norm")=="RELEASED")
      )
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
          on=[
              F.col("po.prod_order_no")==F.col("ls.prod_order_no"),
              F.col("po.prod_order_line_no")==F.col("ls.prod_order_line_no")
          ],
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
      .join(
          cp.alias("cp"),
          on=[
              F.col("pl.prod_order_no")==F.col("cp.prod_order_no"),
              F.col("pl.prod_order_line_no")==F.col("cp.prod_order_line_no")
          ],
          how="left"
      )
      .join(
          ct.alias("ct"),
          on=[F.col("cp.casting_prod_order")==F.col("ct.casting_prod_order")],
          how="left"
      )
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
          F.when(F.col("casting_status").isNotNull(), F.upper(F.col("casting_status")))
           .when(F.col("Prod_Status").isNotNull(), F.upper(F.col("Prod_Status")))
           .when(F.col("itemFG_Category") == "CASTING", F.lit("WAX"))
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
    F.sha2(
        F.concat_ws("§", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in staged.columns]),
        256
    ).desc()
]
w_keys = Window.partitionBy(*KEYS).orderBy(*order_cols)

staged_dedup = (
    staged
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
s_hash = F.sha2(
    F.concat_ws("§", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in compare_cols]),
    256
)
staged_h = staged_dedup.withColumn("_row_hash", s_hash)

t_hash = F.sha2(
    F.concat_ws("§", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in target_cols if c in compare_cols]),
    256
)
target_h = tgt.select(*KEYS, t_hash.alias("_row_hash_t"))

to_apply = (
    staged_h
      .join(target_h, on=KEYS, how="left")
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

# # Plating Status

# CELL ********************

# Databricks / PySpark 3.4+
from pyspark.sql import functions as F, Window as W
from delta.tables import DeltaTable

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

# route SalesOrder via Silver header
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
        ('QA',1.0),('C INS',1.0),('PCK',1.0),('NOT S',0.0),('WH',0.0)
    ] for k in kv]
)

allowed_abbr = ['FIL','HT','TUM','LAS','SET','POL','SHI','PLT','GLU','QC','QA','C INS','PCK','NOT S','WH']
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
    "prod_line_quantity", "prod_order_status", "prod_line_start_date",
    "sales_order_no",
    "FG_item_no",           # keep if needed elsewhere
    "FG_item_no"            # NEW: include FG_item_no
)

# GOLD status (structure for matching ops & locations)
status_gold_raw = spark.table(SRC_STATUS_GOLD).select(
    "prod_order_no", "prod_order_line_no", "operation_no",
    F.col("CorrectCurrentLocation").alias("StatusRoutingNo"),
    F.col("created_on").alias("created_on_gold")
)
status_gold = status_gold_raw.withColumn(
    "created_on_gold_local",
    F.from_utc_timestamp("created_on_gold", SESSION_TZ) if IS_GOLD_CREATED_ON_UTC else F.col("created_on_gold")
)

# SILVER status (we only trust/use created_on here)
status_silver_raw = spark.table(SRC_STATUS_SILVER).select(
    "prod_order_no", "prod_order_line_no", "operation_no",
    F.col("created_on").alias("created_on_silver")
)
status_silver = status_silver_raw.withColumn(
    "created_on_silver_local",
    F.from_utc_timestamp("created_on_silver", SESSION_TZ) if IS_SILVER_CREATED_ON_UTC else F.col("created_on_silver")
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
        .where(~F.col("prod_order_no").like("C%"))
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
        .select(
            "prod_order_no","prod_order_line_no","prod_item_line","prod_line_quantity",
            "StartDatePlan","StartTSActual","StartDateActual",
            "FG_item_no"   # carry FG_item_no forward
        )
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

    # status-side mapping (includes RELEASED/WH2F)
    status_map_exact = step_map_norm.select(
        uptrim("MapKey").alias("k"),
        F.col("OpGroup").alias("S_OpGroup"),
        F.col("OpAbb").alias("S_OpAbb")
    )
    status_map_fb = step_map_norm.select(
        uptrim("DescKey").alias("d"),
        F.col("OpGroup").alias("S_OpGroup_fb"),
        F.col("OpAbb").alias("S_OpAbb_fb")
    ).where(F.length("d") > 0)

    latest_status_mapped = (latest_status
        .withColumn("StatusRoutingNoU", uptrim("StatusRoutingNo"))
        .join(status_map_exact, F.col("StatusRoutingNoU") == F.col("k"), "left")
        .join(status_map_fb,
              (F.col("S_OpAbb").isNull()) & status_map_fb.d.contains(F.col("StatusRoutingNoU")),
              "left")
        .withColumn("S_MappedAbbrev", F.coalesce("S_OpAbb", "S_OpAbb_fb"))
        .withColumn("S_MappedGroup",  F.coalesce("S_OpGroup", "S_OpGroup_fb"))
        .drop("k","d","S_OpAbb","S_OpAbb_fb","S_OpGroup","S_OpGroup_fb")
    )

    latest_status_allowed = (latest_status_mapped
        .join(allowed_df, uptrim("S_MappedAbbrev") == allowed_df.abbr, "inner")
        .drop("abbr")
    )

    # routing-based matches
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

    # status fallback (covers RELEASED/WH2F when not in routing)
    items_scope = routing_cumu_dedup.select(
        "prod_order_no","prod_order_line_no","item_no"
    ).distinct()

    covered_items = current_step.select(
        "prod_order_no","prod_order_line_no","item_no"
    ).distinct()

    uncovered_items = items_scope.join(covered_items, ["prod_order_no","prod_order_line_no","item_no"], "left_anti")

    fallback_current = (uncovered_items.alias("it")
        .join(latest_status_allowed.alias("ls"),
              ["prod_order_no","prod_order_line_no"], "inner")
        .select(
            F.col("it.prod_order_no"),
            F.col("it.prod_order_line_no"),
            F.col("it.item_no"),
            F.lit(-1.0).alias("SeqNo"),                   # synthetic pre-routing step
            F.col("ls.StatusRoutingNo").alias("OpName"),
            F.col("ls.S_MappedGroup").alias("MappedGroup"),
            F.col("ls.S_MappedAbbrev").alias("MappedAbbrev"),
            F.lit(0.0).alias("CumuDaysToThisStep"),       # before routing starts
            F.col("ls.LastSeenAt").alias("LastSeenAt"),
            F.lit("STATUS_FALLBACK").alias("MatchType")
        )
    )

    current_step_all = current_step.unionByName(fallback_current)

    # Next step via LEAD
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
        current_step_all.alias("cp")
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

    # For synthetic (-1) steps, set "next" to the FIRST routing step
    first_step = (routing_cumu_dedup
        .groupBy("prod_order_no","prod_order_line_no","item_no")
        .agg(F.min("SeqNo").alias("FirstSeqNo"))
    )
    first_step_details = (routing_cumu_dedup.alias("rc")
        .join(first_step.alias("fs"),
              (F.col("rc.prod_order_no")==F.col("fs.prod_order_no")) &
              (F.col("rc.prod_order_line_no")==F.col("fs.prod_order_line_no")) &
              (F.col("rc.item_no")==F.col("fs.item_no")) &
              (F.col("rc.SeqNo")==F.col("fs.FirstSeqNo")), "inner")
        .select(
            F.col("rc.prod_order_no"),
            F.col("rc.prod_order_line_no"),
            F.col("rc.item_no"),
            F.col("rc.SeqNo").alias("NextSeqNo_min"),
            F.col("rc.OpName").alias("NextOpName_min"),
            F.col("rc.MappedGroup").alias("NextGroup_min"),
            F.col("rc.MappedAbbrev").alias("NextAbbrev_min")
        )
    )

    next_from_current_fixed = (
        next_from_current.alias("nf")
        .join(current_step_all.alias("cp"),  # to know synthetic rows
              ["prod_order_no","prod_order_line_no","item_no"], "right")
        .join(first_step_details.alias("fsd"),
              ["prod_order_no","prod_order_line_no","item_no"], "left")
        .select(
            "cp.prod_order_no","cp.prod_order_line_no","cp.item_no",
            F.when(F.col("cp.SeqNo")==F.lit(-1.0), F.col("fsd.NextSeqNo_min")).otherwise(F.col("nf.NextSeqNo")).alias("NextSeqNo"),
            F.when(F.col("cp.SeqNo")==F.lit(-1.0), F.col("fsd.NextOpName_min")).otherwise(F.col("nf.NextOpName")).alias("NextOpName"),
            F.when(F.col("cp.SeqNo")==F.lit(-1.0), F.col("fsd.NextGroup_min")).otherwise(F.col("nf.NextGroup")).alias("NextGroup"),
            F.when(F.col("cp.SeqNo")==F.lit(-1.0), F.col("fsd.NextAbbrev_min")).otherwise(F.col("nf.NextAbbrev")).alias("NextAbbrev")
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
        .join(current_step_all.alias("cp"),
              (F.col("rc.prod_order_no")==F.col("cp.prod_order_no")) &
              (F.col("rc.prod_order_line_no")==F.col("cp.prod_order_line_no")) &
              (F.col("rc.item_no")==F.col("cp.item_no")), "inner")
        .where(F.col("rc.SeqNo") < F.col("cp.SeqNo"))
        .groupBy("cp.prod_order_no","cp.prod_order_line_no","cp.item_no")
        .agg(F.max("rc.CumuDaysToThisStep").alias("CumuBeforeDays"))
        .withColumn("CumuBeforeDays", F.coalesce(F.col("CumuBeforeDays"), F.lit(0.0)))
    )

    # ========================================================
    # 6) Per-item result + ETA  (item_type + FG_item_no here)
    # ========================================================
    lwp = (line_with_plating.withColumnRenamed("prod_item_line","item_no"))

    result_per_item = (lwp.alias("lwp")
        .join(sum_to_plating.alias("stp"),
              ["prod_order_no","prod_order_line_no","item_no"], "inner")
        .join(current_step_all.alias("cp"),
              ["prod_order_no","prod_order_line_no","item_no"], "left")
        .join(next_from_current_fixed.alias("nfc"),
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
             .when(uptrim("cp.MappedAbbrev").isin('NOT S','WH','FIL','HT','TUM','LAS','SET','POL','SHI'), F.lit("Waiting to Plating"))
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
            # NEW: item_type (first character of item_no, uppercased)
            F.when(F.col("lwp.item_no").isNotNull(),
                   F.upper(F.substring(F.trim(F.col("lwp.item_no")), 1, 1))
            ).alias("item_type"),
            # NEW: FG_item_no from GOLD production order (carried to line -> lwp)
            F.col("lwp.FG_item_no").alias("FG_item_no"),

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
    # 7.1) Enrich via SILVER header -> GOLD sales_order,
    #      and ITEM dim using prod_item_line ↔ item_no
    # ========================================================
    prod_hdr = (spark.table(SRC_PROD_HDR).select(
        "prod_order_no",
        uptrim("sales_order_no").alias("sales_order_no_u_hdr")
    ).dropDuplicates(["prod_order_no"]))

    # Use prod_item_line_u for item join
    prod_keys = (prod_order.select(
        "prod_order_no","prod_order_line_no",
        uptrim("sales_order_no").alias("sales_order_no_u_gold"),
        uptrim("prod_item_line").alias("prod_item_line_u")
    ))

    prod_keys_joined = (prod_keys
        .join(prod_hdr, on="prod_order_no", how="left")
        .withColumn("sales_order_no_u", F.coalesce(F.col("sales_order_no_u_hdr"), F.col("sales_order_no_u_gold")))
        .select("prod_order_no","prod_order_line_no","sales_order_no_u","prod_item_line_u")
    )

    sales_so = (spark.table(SRC_SALES_ORDER).select(
        uptrim("SalesorderNo").alias("SalesorderNo_u"),
        F.col("SalesorderNo").alias("SalesorderNo"),
        F.col("CusNo").alias("CusNo"),
        F.col("CusName").alias("CusName"),
        F.col("CusAbbr").alias("CusAbbr"),
        F.col("so_abbr").alias("SOAbbr")
    ))

    items_dim = (spark.table(SRC_ITEM).select(
        uptrim("item_no").alias("item_no_u"),
        F.col("item_metal_category").alias("item_metal_category"),
        F.col("item_category_code").alias("item_category")
    ))

    prod_so = (prod_keys_joined
        .join(sales_so, on=(F.col("sales_order_no_u") == F.col("SalesorderNo_u")), how="left")
        .select(
            "prod_order_no","prod_order_line_no",
            "SalesorderNo","CusNo","CusName","CusAbbr","SOAbbr",
            "prod_item_line_u"
        )
    )

    # Join prod_item_line_u ↔ item_no_u
    prod_so_item = (prod_so
        .join(items_dim, on=(F.col("prod_item_line_u") == F.col("item_no_u")), how="left")
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

# MARKDOWN ********************

# # Production Status Cycle Time

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------
silver_status_table = "Silver_Production_Lakehouse.prod.silver_prod_order_status"
cell_asgn_table    = "Silver_Production_Lakehouse.prod.silver_cell_list"
routing_table      = "Silver_Production_Lakehouse.prod.silver_prod_routing_line"
target_table       = "Gold_Production_Lakehouse.prod.gold_production_status_cycle_time"

# ----------------------------------------------------------------------
# HELPERS
# ----------------------------------------------------------------------
def table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)

def get_last_loaded_ts(table_name: str):
    """
    Use the latest of in_created/out_created/to_created/from_created/dead_to_created
    as watermark for incremental load (all in BKK time).
    """
    if not table_exists(table_name):
        return None

    df = spark.table(table_name)
    if df.rdd.isEmpty():
        return None

    ts_col = F.greatest(
        F.col("in_created"),
        F.col("out_created"),
        F.col("to_created"),
        F.col("from_created"),
        F.col("dead_to_created")
    )

    row = df.select(F.max(ts_col).alias("max_ts")).collect()[0]
    return row["max_ts"]

# ----------------------------------------------------------------------
# 0) INCREMENTAL WATERMARK (BKK timezone, same as created_on_bkk)
# ----------------------------------------------------------------------
last_loaded_ts = get_last_loaded_ts(target_table)

# ----------------------------------------------------------------------
# 1) BASE DATA: join + filter + shift created_on -> BKK (+7h, truncate seconds)
# ----------------------------------------------------------------------
s = spark.table(silver_status_table)
l = spark.table(cell_asgn_table)

# If cell list is relatively small, broadcast to speed up join
l = F.broadcast(l)

base_data = (
    s.join(
        l,
        on=(s.user_id == l.email_address),
        how="left"
    )
    .where(
        (s.type_name.isin("In location in", "Out location", "To employee", "From employee")) &
        (F.trim(s.prod_order_status) == F.lit("Released"))
    )
    .select(
        s.prod_order_no,
        s.prod_order_line_no,
        s.type_name,
        s.operation_no,
        s.item_no,
        s.quantity,
        s.machine_center_no,
        l.prod_line,
        l.cell_line,
        # UTC -> BKK (+7h), truncate to seconds
        F.date_trunc("second", s.created_on + F.expr("INTERVAL 7 HOURS")).alias("created_on_bkk")
    )
)

# INCREMENTAL FILTER (if we have previous data)
if last_loaded_ts is not None:
    base_data = base_data.filter(F.col("created_on_bkk") > F.lit(last_loaded_ts))

# If nothing new, short-circuit
if base_data.rdd.isEmpty():
    print(f"✅ No new data to load. Target already up to date: {target_table}")
else:
    # ------------------------------------------------------------------
    # 2) op_major: leading numeric chunk
    # ------------------------------------------------------------------
    wr = base_data.withColumn(
        "op_major",
        F.regexp_extract(F.col("operation_no"), r"^(\d+)", 1).cast("int")
    )

    # ------------------------------------------------------------------
    # 2.1) routing_map (restricted to relevant orders)
    # ------------------------------------------------------------------
    r = spark.table(routing_table)

    # Only keep routing rows for orders/lines present in the status data
    order_keys = wr.select("prod_order_no", "prod_order_line_no").distinct()
    r_filtered = r.join(
        F.broadcast(order_keys),
        on=["prod_order_no", "prod_order_line_no"],
        how="inner"
    )

    r_with_group = r_filtered.withColumn(
        "op_major",
        F.regexp_extract(F.col("operation_no"), r"^(\d+)", 1).cast("int")
    )

    w_routing = Window.partitionBy(
        "prod_order_no", "prod_order_line_no", "op_major"
    )

    r_calc = (
        r_with_group
        .withColumn(
            "routing_no_work_center",
            F.max(
                F.when(F.col("operation_type") == "Work Center", F.col("routing_no"))
            ).over(w_routing)
        )
    )

    r_group = (
        r_calc
        .filter(
            (F.col("operation_type") == "Machine Center") |
            ((F.col("op_major") == 9) & (F.col("operation_no") == F.lit("009")))
        )
    )

    routing_map = (
        r_group
        .groupBy("prod_order_no", "prod_order_line_no", "op_major")
        .agg(
            F.max(
                F.when(F.col("operation_type") == "Machine Center", F.col("routing_no"))
            ).alias("routing_no_machine_center"),
            F.max("routing_no_work_center").alias("routing_no_work_center")
        )
    )

    # ------------------------------------------------------------------
    # 2.2) join routing_map
    # ------------------------------------------------------------------
    wr_with_routing = wr.join(
        routing_map,
        on=["prod_order_no", "prod_order_line_no", "op_major"],
        how="left"
    )

    # ------------------------------------------------------------------
    # 3) wr_ext: next event times per type within (prod_order_no, line, op_major)
    # ------------------------------------------------------------------
    w_event = (
        Window
        .partitionBy("prod_order_no", "prod_order_line_no", "op_major")
        .orderBy("created_on_bkk")
        .rowsBetween(0, Window.unboundedFollowing)
    )

    wr_ext = (
        wr_with_routing
        .withColumn(
            "end_out_created",
            F.min(F.when(F.col("type_name") == "Out location", F.col("created_on_bkk"))).over(w_event)
        )
        .withColumn(
            "end_from_created",
            F.min(F.when(F.col("type_name") == "From employee", F.col("created_on_bkk"))).over(w_event)
        )
        .withColumn(
            "end_to_created",
            F.min(F.when(F.col("type_name") == "To employee", F.col("created_on_bkk"))).over(w_event)
        )
    )

    # ------------------------------------------------------------------
    # 4) Pair intervals by metric
    # ------------------------------------------------------------------
    pair_in_out = (
        wr_ext
        .filter((F.col("type_name") == "In location in") & F.col("end_out_created").isNotNull())
        .select(
            "prod_order_no", "prod_order_line_no", "op_major",
            "prod_line", "cell_line", "machine_center_no",
            "routing_no_machine_center", "routing_no_work_center",
            "item_no",
            "quantity",
            F.col("created_on_bkk"),
            F.col("end_out_created").alias("end_created")
        )
    )

    pair_to_from = (
        wr_ext
        .filter((F.col("type_name") == "To employee") & F.col("end_from_created").isNotNull())
        .select(
            "prod_order_no", "prod_order_line_no", "op_major",
            "prod_line", "cell_line", "machine_center_no",
            "routing_no_machine_center", "routing_no_work_center",
            "item_no",
            "quantity",
            F.col("created_on_bkk"),
            F.col("end_from_created").alias("end_created")
        )
    )

    pair_in_to = (
        wr_ext
        .filter((F.col("type_name") == "In location in") & F.col("end_to_created").isNotNull())
        .select(
            "prod_order_no", "prod_order_line_no", "op_major",
            "prod_line", "cell_line", "machine_center_no",
            "routing_no_machine_center", "routing_no_work_center",
            "item_no",
            "quantity",
            F.col("created_on_bkk"),
            F.col("end_to_created").alias("end_created")
        )
    )

    # ------------------------------------------------------------------
    # 5) Build intervals (t_start → t_end) with metric
    # ------------------------------------------------------------------
    intervals = (
        pair_in_out.select(
            "prod_order_no", "prod_order_line_no", "op_major",
            "prod_line", "cell_line", "machine_center_no",
            "routing_no_machine_center", "routing_no_work_center",
            "item_no",
            "quantity",
            F.lit("station").alias("metric"),
            F.col("created_on_bkk").alias("t_start"),
            F.col("end_created").alias("t_end")
        )
        .unionByName(
            pair_to_from.select(
                "prod_order_no", "prod_order_line_no", "op_major",
                "prod_line", "cell_line", "machine_center_no",
                "routing_no_machine_center", "routing_no_work_center",
                "item_no",
                "quantity",
                F.lit("operation").alias("metric"),
                F.col("created_on_bkk").alias("t_start"),
                F.col("end_created").alias("t_end")
            )
        )
        .unionByName(
            pair_in_to.select(
                "prod_order_no", "prod_order_line_no", "op_major",
                "prod_line", "cell_line", "machine_center_no",
                "routing_no_machine_center", "routing_no_work_center",
                "item_no",
                "quantity",
                F.lit("dead").alias("metric"),
                F.col("created_on_bkk").alias("t_start"),
                F.col("end_created").alias("t_end")
            )
        )
    ).filter(F.col("t_start").isNotNull() & F.col("t_end").isNotNull())

    intervals = intervals.cache()

    # ------------------------------------------------------------------
    # 6) metric_base
    # ------------------------------------------------------------------
    metric_base = (
        intervals
        .groupBy(
            "prod_order_no", "prod_order_line_no", "op_major",
            "prod_line", "cell_line", "machine_center_no",
            "routing_no_machine_center", "routing_no_work_center", "metric"
        )
        .agg(
            F.min("t_start").alias("t_start"),
            F.max("t_end").alias("t_end"),
            F.max("item_no").alias("item_no"),
            F.max("quantity").alias("quantity")
        )
    )

    # ------------------------------------------------------------------
    # 7) Expand by day for working-slot calculation
    # ------------------------------------------------------------------
    expand_days = (
        intervals
        .withColumn(
            "d",
            F.explode(F.sequence(F.to_date("t_start"), F.to_date("t_end")))
        )
    )

    # ------------------------------------------------------------------
    # 8) Working slots (Mon–Fri 08:00–12:00 & 13:00–18:20; Sat 08:00–12:00 & 13:00–17:00)
    # ------------------------------------------------------------------
    # dayofweek(): 1=Sun, 2=Mon, ..., 7=Sat
    slots = (
        expand_days
        .withColumn("dow", F.dayofweek("d"))
        .filter(F.col("dow").between(2, 7))   # Mon–Sat
        .withColumn("base_ts", F.col("d").cast("timestamp"))
        .withColumn("am_start", F.col("base_ts") + F.expr("INTERVAL 8 HOURS"))
        .withColumn("am_end",   F.col("base_ts") + F.expr("INTERVAL 12 HOURS"))
        .withColumn("pm_start", F.col("base_ts") + F.expr("INTERVAL 13 HOURS"))
        .withColumn(
            "pm_end",
            F.when(
                F.col("dow").between(2, 6),  # Mon–Fri
                F.col("base_ts") + F.expr("INTERVAL 18 HOURS") + F.expr("INTERVAL 20 MINUTES")
            ).otherwise(F.col("base_ts") + F.expr("INTERVAL 17 HOURS"))  # Sat
        )
    )

    # ------------------------------------------------------------------
    # 9) Clip to working minutes per day/slot (AM + PM)
    # ------------------------------------------------------------------
    clip = (
        slots
        # AM
        .withColumn(
            "am_start_eff",
            F.when(
                (F.to_date("t_start") == F.col("d")) & (F.col("t_start") > F.col("am_start")),
                F.col("t_start")
            ).otherwise(F.col("am_start"))
        )
        .withColumn(
            "am_end_eff",
            F.when(
                (F.to_date("t_end") == F.col("d")) & (F.col("t_end") < F.col("am_end")),
                F.col("t_end")
            ).otherwise(F.col("am_end"))
        )
        .withColumn(
            "am_min",
            F.when(
                F.col("am_end_eff") > F.col("am_start_eff"),
                (F.col("am_end_eff").cast("long") - F.col("am_start_eff").cast("long")) / 60
            ).otherwise(F.lit(0))
        )
        # PM
        .withColumn(
            "pm_start_eff",
            F.when(
                (F.to_date("t_start") == F.col("d")) & (F.col("t_start") > F.col("pm_start")),
                F.col("t_start")
            ).otherwise(F.col("pm_start"))
        )
        .withColumn(
            "pm_end_eff",
            F.when(
                (F.to_date("t_end") == F.col("d")) & (F.col("t_end") < F.col("pm_end")),
                F.col("t_end")
            ).otherwise(F.col("pm_end"))
        )
        .withColumn(
            "pm_min",
            F.when(
                F.col("pm_end_eff") > F.col("pm_start_eff"),
                (F.col("pm_end_eff").cast("long") - F.col("pm_start_eff").cast("long")) / 60
            ).otherwise(F.lit(0))
        )
        .select(
            "prod_order_no", "prod_order_line_no", "op_major",
            "prod_line", "cell_line", "machine_center_no",
            "routing_no_machine_center", "routing_no_work_center",
            "metric", "t_start", "t_end", "d",
            F.col("am_min").cast("int").alias("am_min"),
            F.col("pm_min").cast("int").alias("pm_min")
        )
    )

    # ------------------------------------------------------------------
    # 10) metric_work: aggregate working minutes where there are slots
    # ------------------------------------------------------------------
    metric_work = (
        clip
        .groupBy(
            "prod_order_no", "prod_order_line_no", "op_major",
            "prod_line", "cell_line", "machine_center_no",
            "routing_no_machine_center", "routing_no_work_center", "metric"
        )
        .agg(F.sum(F.col("am_min") + F.col("pm_min")).alias("work_min"))
    )

    # ------------------------------------------------------------------
    # 11) metric_sum: join base intervals with working minutes
    # ------------------------------------------------------------------
    metric_sum = (
        metric_base.alias("b")
        .join(
            metric_work.alias("w"),
            on=[
                "prod_order_no", "prod_order_line_no", "op_major",
                "prod_line", "cell_line", "machine_center_no",
                "routing_no_machine_center", "routing_no_work_center", "metric"
            ],
            how="left"
        )
        .select(
            "b.prod_order_no", "b.prod_order_line_no", "b.op_major",
            "b.prod_line", "b.cell_line", "b.machine_center_no",
            "b.routing_no_machine_center", "b.routing_no_work_center",
            "b.metric", "b.t_start", "b.t_end",
            "b.item_no",
            "b.quantity",
            F.coalesce(F.col("work_min"), F.lit(0)).alias("work_min")
        )
    )

    # ------------------------------------------------------------------
    # 12) FINAL RESULT + in_date (for partitioning / Power BI)
    # ------------------------------------------------------------------
    result_df = (
        metric_sum
        .groupBy("prod_order_no", "prod_order_line_no", "op_major")
        .agg(
            F.max("prod_line").alias("prod_line"),
            F.max("cell_line").alias("cell_line"),
            F.max("machine_center_no").alias("operation"),
            F.max("routing_no_machine_center").alias("routing_no_machine_center"),
            F.max("routing_no_work_center").alias("routing_no_work_center"),
            F.max("item_no").alias("item_no"),
            F.max("quantity").alias("quantity"),
            F.max(F.when(F.col("metric") == "station",   F.col("t_start"))).alias("in_created"),
            F.max(F.when(F.col("metric") == "station",   F.col("t_end"))).alias("out_created"),
            F.max(F.when(F.col("metric") == "operation", F.col("t_start"))).alias("to_created"),
            F.max(F.when(F.col("metric") == "operation", F.col("t_end"))).alias("from_created"),
            F.max(F.when(F.col("metric") == "dead",      F.col("t_end"))).alias("dead_to_created"),
            F.sum(F.when(F.col("metric") == "station",   F.col("work_min")).otherwise(F.lit(0))).alias("station_time"),
            F.sum(F.when(F.col("metric") == "operation", F.col("work_min")).otherwise(F.lit(0))).alias("operation_time"),
            F.sum(F.when(F.col("metric") == "dead",      F.col("work_min")).otherwise(F.lit(0))).alias("dead_time")
        )
        .withColumn(
            "in_date",
            F.coalesce(
                F.to_date("in_created"),
                F.to_date("out_created"),
                F.to_date("to_created"),
                F.to_date("from_created"),
                F.to_date("dead_to_created")
            )
        )
    )

    # ------------------------------------------------------------------
    # 13) WRITE: PARTITIONED + INCREMENTAL MERGE + OPTIMIZE
    # ------------------------------------------------------------------
    if not table_exists(target_table):
        # Initial full load: create partitioned Delta table
        (
            result_df
            .repartition("in_date")            # avoids too many small files per partition
            .write
            .format("delta")
            .partitionBy("in_date")            # <<< key for Power BI performance
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )
        print(f"✅ Initial full load completed (partitioned by in_date): {target_table}")
    else:
        # Incremental MERGE into existing partitioned Delta table
        delta_tgt = DeltaTable.forName(spark, target_table)

        (
            delta_tgt.alias("t")
            .merge(
                result_df.alias("s"),
                """
                t.prod_order_no      = s.prod_order_no AND
                t.prod_order_line_no = s.prod_order_line_no AND
                t.op_major           = s.op_major
                """
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"✅ Incremental merge completed: {target_table}")

   # ------------------------------------------------------------------
    # 14) PHYSICAL OPTIMIZATION FOR POWER BI
    # ------------------------------------------------------------------
    # Run every batch (or at least nightly) – speeds up PBI reads a lot
    # NOTE: ZORDER cannot include partition columns (in_date), so we only
    #       ZORDER on non-partitioned dimensions that PBI filters/joins on.
    try:
        spark.sql(f"""
            OPTIMIZE {target_table}
            ZORDER BY (prod_line, cell_line, operation)
        """)
        print(f"✅ OPTIMIZE + ZORDER completed for: {target_table}")
    except Exception as e:
        # Optional: don't fail the whole job if OPTIMIZE isn't supported in env
        print(f"⚠️ OPTIMIZE/ZORDER skipped or failed for {target_table}: {e}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Production Status Cycle Time Header (Not running)


# CELL ********************

# Build: Gold_Production_Lakehouse.prod.gold_prod_status_cycle_time_header   (incremental)
# Fixes MERGE multi-match by: (a) source de-dupe on a natural key, (b) optional target clean, (c) MERGE on natural key.

from pyspark.sql import functions as F
from pyspark.sql import Window as W
from delta.tables import DeltaTable
from datetime import datetime, date
from pyspark.sql.utils import AnalysisException

# ---------- Config ----------
H_SRC  = "Silver_Production_Lakehouse.prod.silver_prod_order_header"   # H
S_SRC  = "Silver_Production_Lakehouse.prod.silver_prod_order_status"   # S
SO_SRC = "Silver_Customer_Exp_Lakehouse.cx.silver_sales_header"        # SO
M_SRC  = "Silver_Commons_Lakehouse.cmn.silver_machine_center"          # M

TARGET = "Gold_Production_Lakehouse.prod.gold_prod_status_cycle_time_header"

# One-time toggle if target already contains dupes on MERGE key
CLEAN_TARGET_DUPES_ONCE = False

# Natural MERGE key (no timestamps)
MERGE_KEYS = [
    "prod_order_no",
    "prod_order_line_no",
    "item_no",
    "operation_no",
    "type_name",
    "user_id",
    "open",
    "employee_no",
    "antenna_id",
]

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
        spark.table(name)
        return True
    except AnalysisException:
        return False

def pick_watermark_from_target(table_name: str):
    try:
        df = spark.table(table_name)
    except Exception:
        return None
    if "change_ts" in df.columns:
        return df.agg(F.max(F.to_date("change_ts")).alias("mx")).collect()[0]["mx"]
    return None

def trim_cols(df, cols):
    out = df
    for c in cols:
        if c in out.columns:
            out = out.withColumn(c, F.trim(F.col(c)))
    return out

def count_dupes(df, keys):
    return (
        df.groupBy(*[F.col(k) for k in keys])
          .count()
          .filter(F.col("count") > 1)
    )

since_date = parse_date_or_none(since_param)
until_date = parse_date_or_none(until_param) or date.today()

if not full_reload and since_date is None and table_exists(TARGET):
    wm = pick_watermark_from_target(TARGET)
    if wm is not None:
        since_date = wm

if full_reload or since_date is None:
    since_date = date(1900, 1, 1)

print(f"Incremental window: {since_date} -> {until_date}")

# ---------- Load sources ----------
H  = spark.table(H_SRC).alias("H")
SO = spark.table(SO_SRC).alias("SO")
M  = spark.table(M_SRC).alias("M")

# Status with timestamps (alias as S)
S_cast = (
    spark.table(S_SRC)
        .withColumn("created_on_ts",  F.to_timestamp(F.col("created_on")))
        .withColumn("modified_on_ts", F.to_timestamp(F.col("modified_on")))
        .alias("S")
)

# ---------- Build SourceWithFinal (your logic) ----------
trim_mc   = F.trim(F.col("S.machine_center_no"))
trim_curr = F.trim(F.col("S.current_location_code"))
mc_final  = F.when((trim_mc.isNotNull()) & (trim_mc != ""), trim_mc).otherwise(trim_curr)

SWF = (
    H.join(S_cast, F.col("H.prod_order_no") == F.col("S.prod_order_no"), "inner")
     .join(SO, F.col("H.sales_order_no") == F.col("SO.sales_order_no"), "inner")
     .filter(F.col("H.prod_order_status") == F.lit("Released"))
     .select(
         F.col("H.sales_order_no").alias("sales_order_no"),
         F.col("H.sales_order_line_no").alias("sales_order_line_no"),
         F.col("H.ref_item").alias("ref_item"),
         F.col("H.FG_item_no").alias("FG_item_no"),
         F.col("S.created_on_ts").alias("created_on"),
         F.col("S.modified_on_ts").alias("modified_on"),
         F.col("S.prod_order_no").alias("prod_order_no"),
         F.col("S.prod_order_line_no").alias("prod_order_line_no"),
         F.col("S.item_no").alias("item_no"),
         F.col("S.operation_no").alias("operation_no"),
         F.col("S.current_location_code").alias("current_location_code"),
         F.col("S.past_location_code").alias("past_location_code"),
         F.col("S.employee_no").alias("employee_no"),
         F.col("S.type_name").alias("type_name"),
         F.col("S.quantity").alias("quantity"),
         F.col("S.remaining_quantity").alias("remaining_quantity"),
         F.col("S.prod_order_status").alias("prod_order_status"),
         F.col("S.open").alias("open"),
         F.col("S.antenna_id").alias("antenna_id"),
         F.col("S.user_id").alias("user_id"),
         mc_final.alias("machine_center_no_final"),
         F.col("SO.customer_no").alias("customer_no"),
         F.col("SO.customer_name").alias("customer_name"),
     )
)

# Watermark column (for incremental)
SWF = SWF.withColumn("change_ts", F.greatest(F.col("created_on"), F.col("modified_on")))

# Incremental window on date(change_ts)
SWF_win = SWF.filter(
    (F.to_date("change_ts") >= F.lit(since_date)) &
    (F.to_date("change_ts") <= F.lit(until_date))
)

# ---------- Final SELECT with join to machine center ----------
final_df0 = (
    SWF_win.alias("SWF")
      .join(
          spark.table(M_SRC)
               .select(
                   F.trim(F.col("machine_center_no")).alias("mc_no_join"),
                   F.col("department_group")
               ),
          on=F.trim(F.col("SWF.machine_center_no_final")) == F.col("mc_no_join"),
          how="left"
      )
      .select(
          F.col("SWF.created_on"),
          F.col("SWF.modified_on"),
          F.col("SWF.prod_order_no"),
          F.col("SWF.prod_order_line_no"),
          F.col("SWF.item_no"),
          F.col("department_group").alias("Type"),
          F.col("SWF.operation_no"),
          F.col("SWF.machine_center_no_final"),
          F.col("SWF.employee_no"),
          F.col("SWF.type_name"),
          F.abs(F.col("SWF.quantity")).alias("quantity"),
          F.col("SWF.remaining_quantity"),
          F.col("SWF.prod_order_status"),
          F.col("SWF.open"),
          F.col("SWF.sales_order_no"),
          F.col("SWF.sales_order_line_no"),
          F.col("SWF.FG_item_no"),
          F.col("SWF.antenna_id"),
          F.col("SWF.user_id"),
          F.col("SWF.customer_no"),
          F.col("SWF.customer_name"),
          F.col("SWF.change_ts"),
      )
)

# ---------- 1) Normalize strings on MERGE keys ----------
final_df0 = trim_cols(final_df0, [
    "prod_order_no","item_no","type_name","user_id",
    "employee_no","antenna_id"
])

# ---------- 2) SOURCE de-dupe on natural MERGE key ----------
w_src = W.partitionBy(*MERGE_KEYS).orderBy(
    F.col("modified_on").desc_nulls_last(),
    F.col("created_on").desc_nulls_last()
)
final_df = (
    final_df0
    .withColumn("rn_src", F.row_number().over(w_src))
    .filter(F.col("rn_src") == 1)
    .drop("rn_src")
)

# ---------- Diagnostics ----------
src_dupes = count_dupes(final_df0, MERGE_KEYS).count()
print(f"[Diag] SOURCE duplicate key groups before dedupe: {src_dupes}")
src_dupes_after = count_dupes(final_df, MERGE_KEYS).count()
print(f"[Diag] SOURCE duplicate key groups after  dedupe: {src_dupes_after}")

# ---------- 3) (Optional) clean TARGET if it already has dupes ----------
if table_exists(TARGET):
    tgt_df = spark.table(TARGET)
    tgt_df = trim_cols(tgt_df, [
        "prod_order_no","item_no","type_name","user_id",
        "employee_no","antenna_id"
    ])
    tgt_dupes = count_dupes(tgt_df, MERGE_KEYS).count()
    print(f"[Diag] TARGET duplicate key groups (before): {tgt_dupes}")

    if CLEAN_TARGET_DUPES_ONCE and tgt_dupes > 0:
        print("[Action] Cleaning target duplicates once...")
        w_tgt = W.partitionBy(*MERGE_KEYS).orderBy(
            F.col("modified_on").desc_nulls_last(),
            F.col("created_on").desc_nulls_last()
        )
        tgt_clean = (
            tgt_df
            .withColumn("rn_tgt", F.row_number().over(w_tgt))
            .filter(F.col("rn_tgt") == 1)
            .drop("rn_tgt")
        )
        assert count_dupes(tgt_clean, MERGE_KEYS).count() == 0, "Target still has duplicates after dedupe!"
        (tgt_clean.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema","true")
            .saveAsTable(TARGET))
        print("[Action] Target cleaned.")

# ---------- 4) MERGE on the explicit natural key ----------
def merge_or_create(target, df):
    if not table_exists(target):
        print(f"Creating {target} ...")
        (df.write
           .format("delta")
           .mode("overwrite")
           .option("overwriteSchema","true")
           .saveAsTable(target))
        return

    print(f"Merging into {target} ...")
    t = DeltaTable.forName(spark, target)
    cond = " AND ".join([f"t.{k} <=> s.{k}" for k in MERGE_KEYS])

    (t.alias("t")
      .merge(df.alias("s"), cond)
      .whenMatchedUpdateAll()
      .whenNotMatchedInsertAll()
      .execute())

merge_or_create(TARGET, final_df)
print("✅ Done:", TARGET)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # Production All Status

# CELL ********************

# ==============================================================
# GOLD INCREMENTAL LOADER (with diagnostics)
# Source: Silver_Production_Lakehouse.prod.silver_prod_order_status
# Target: prod.gold_production_status (in this Gold lakehouse)
# ==============================================================

from pyspark.sql import functions as F, Window, types as T

SILVER = "Silver_Production_Lakehouse.prod.silver_prod_order_status"
TARGET = "prod.gold_production_all_status"
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
    .filter((F.col("status_norm")    == F.lit("RELEASED")) )
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
