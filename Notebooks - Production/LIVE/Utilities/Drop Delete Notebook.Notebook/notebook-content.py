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
# META           "id": "ad99fdfa-85b1-4480-9f7f-2640bfd65f24"
# META         },
# META         {
# META           "id": "e248ea90-8431-4df2-9f29-87866bf9dd5a"
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
# META           "id": "3a130b81-98ec-4fd4-a404-95edc1f0ef1e"
# META         },
# META         {
# META           "id": "25adf76d-df18-41c8-97e8-55789149bd80"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ==============================================
# MULTI-LAKEHOUSE DEDUPE (RUNS EVERY 3 HOURS @ :00)
# ==============================================

from pyspark.sql import functions as F, Window
from datetime import datetime

# ---------------------------
# CONFIGURE HERE
# ---------------------------

# Lakehouses attached to this notebook (add/remove as needed)
LAKEHOUSES = [
    # "Silver_Production_Lakehouse",
    # "Gold_Production_Lakehouse",
    "Silver_Customer_Exp_Lakehouse",
   
]

# Tables to dedupe in each lakehouse
# keys = partition columns for row_number()
# order = column used to pick the "latest" within each partition
DEDUPE_SPECS = [
    # prod schema
    {
        "schema": "prod",
        "table":  "gold_prod_cycle_time",
        "keys":   ["prod_order_no","prod_order_line_no", "operation"],
        "order":  "in_created",
    },
    {
        "schema": "prod",
        "table":  "gold_inv_output",
        "keys":   ["document_no", "order_no", "order_lineno", "item_no", "created_on", "item_lot"],
        "order":  "created_on",
    },
    {
        "schema": "prod",
        "table":  "silver_employee_rfid_mapping",
        "keys":   ["employee_id"],
        "order":  "modified_on",
    },
    {
        "schema": "prod",
        "table":  "silver_prod_order_status",
        "keys":   ["prod_order_no","prod_order_line_no","type_name","current_location_code","machine_center_no","past_location_code"],
        "order":  "created_on",
    },
    {
        "schema": "prod",
        "table":  "silver_prod_order_header",
        "keys":   ["prod_order_no"],
        "order":  "modified_on",
    },
    {
        "schema": "prod",
        "table":  "silver_prod_order_line",
        "keys":   ["prod_order_no","prod_order_line_no"],
        "order":  "modified_on",
    },
    {
        "schema": "prod",
        "table":  "silver_prod_routing_line",
        "keys":   ["prod_order_no","prod_order_line_no","previous_operation_no","next_operation_no","operation_no"],
        "order":  "modified_on",
    },
    {
        "schema": "prod",
        "table":  "silver_casting_parts",
        "keys":   ["prod_order_no","prod_order_line_no","casting_prod_order"],
        "order":  "modified_on",
    },
    {
        "schema": "prod",
        "table":  "silver_casting_tree",
        "keys":   ["casting_prod_order","casting_tree_no"],
        "order":  "modified_on",
    },
        {
        "schema": "prod",
        "table":  "silver_production_bom",
        "keys":   ["bom_no", "bom_version", "item_no"],
        "order":  "modified_on",
    },

    # cx / sales schema
    {
        "schema": "cx",
        "table":  "silver_sales_header",
        "keys":   ["sales_order_no"],
        "order":  "modified_on",
    },
    {
        "schema": "cx",
        "table":  "silver_sales_line",
        "keys":   ["sales_order_no","sales_order_line_no"],
        "order":  "modified_on",
    },
    {
        "schema": "cx",
        "table":  "silver_customer",
        "keys":   ["customer_no"],
        "order":  "created_on",
    },

    # master schema (uncomment if you want to dedupe item master too)
    # {
    #     "schema": "master",
    #     "table":  "silver_item_master",
    #     "keys":   ["item_no"],
    #     "order":  "modified_on",
    # },
]

# Maintenance knobs
DO_OPTIMIZE = True
DO_VACUUM = True
VACUUM_HOURS = 168

# Manual override: set to True to run even if we're not on a 3-hour boundary
FORCE_DEDUPE = True

# ---------------------------
# HELPERS
# ---------------------------

def table_exists(qualified_table: str) -> bool:
    """Return True if <lakehouse>.<schema>.<table> exists."""
    try:
        return spark.catalog.tableExists(qualified_table)
    except Exception:
        return False

def should_run_dedupe() -> bool:
    """Return True when the current hour is multiple of 3 and minute is 0 (or forced)."""
    if FORCE_DEDUPE:
        print("[dedupe] FORCE_DEDUPE=True; running regardless of time window.")
        return True
    now = datetime.now()
    hour, minute = now.hour, now.minute
    if hour % 3 == 0 and minute == 0:
        print(f"[dedupe] {now:%Y-%m-%d %H:%M} — window open (hour % 3 == 0 and minute == 0).")
        return True
    print(f"[dedupe] {now:%Y-%m-%d %H:%M} — window closed (next at hour multiple of 3, minute 00).")
    return False

def dedupe_overwrite(full_table: str, partition_cols: list, order_col: str) -> dict:
    """
    Deduplicate a Delta table in place.
    Keeps only the latest record per partition (based on order_col DESC).
    Returns a result dict for summary.
    """
    result = {
        "table": full_table,
        "before": 0,
        "after": 0,
        "deleted": 0,
        "pct_deleted": 0.0,
        "status": "skipped",
        "error": None,
    }

    print(f"\n[dedupe] Table: {full_table}")
    print(f"[dedupe] Partition keys: {partition_cols}")
    print(f"[dedupe] Order by: {order_col}")

    if not table_exists(full_table):
        print(f"[dedupe] Table not found; skipping: {full_table}")
        return result

    df = spark.table(full_table)
    total_before = df.count()
    result["before"] = total_before

    if total_before == 0:
        print("[dedupe] Empty table; skipping.")
        result["status"] = "empty"
        return result

    # Guard: order column must exist
    if order_col not in df.columns:
        print(f"[dedupe] Column '{order_col}' not found; skipping.")
        result["status"] = "order_col_missing"
        return result

    # Window + row_number to keep latest per partition
    w = Window.partitionBy(*partition_cols).orderBy(F.col(order_col).desc())
    out = df.withColumn("_rn", F.row_number().over(w)) \
            .filter(F.col("_rn") == 1) \
            .drop("_rn")

    total_after = out.count()
    deleted = total_before - total_after
    pct = (deleted / total_before * 100) if total_before else 0.0

    # Write back
    (out.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table))

    # Maintenance
    if DO_OPTIMIZE:
        spark.sql(f"OPTIMIZE {full_table}")
    if DO_VACUUM:
        spark.sql(f"VACUUM {full_table} RETAIN {VACUUM_HOURS} HOURS")

    # Logs
    print(f"[dedupe] Removed duplicates: {deleted:,} ({pct:.2f}%)")
    print(f"[dedupe] Rows after: {total_after:,}")
    if DO_OPTIMIZE:
        print(f"[dedupe] OPTIMIZE done.")
    if DO_VACUUM:
        print(f"[dedupe] VACUUM retain {VACUUM_HOURS}h done.")

    # Result
    result.update({
        "after": total_after,
        "deleted": deleted,
        "pct_deleted": round(pct, 2),
        "status": "ok",
    })
    return result

# ---------------------------
# RUN (time-gated)
# ---------------------------

RUN_RESULTS = []

if should_run_dedupe():
    for lakehouse in LAKEHOUSES:
        print(f"\n=== DEDUPE for lakehouse: {lakehouse} ===")
        for spec in DEDUPE_SPECS:
            full_table = f"{lakehouse}.{spec['schema']}.{spec['table']}"
            try:
                res = dedupe_overwrite(full_table, spec["keys"], spec["order"])
                RUN_RESULTS.append(res)
            except Exception as e:
                msg = str(e)
                print(f"[dedupe] ERROR on {full_table}: {msg}")
                RUN_RESULTS.append({
                    "table": full_table,
                    "before": None,
                    "after": None,
                    "deleted": None,
                    "pct_deleted": None,
                    "status": "error",
                    "error": msg,
                })
else:
    print("\n[dedupe] Not a dedupe window. Set FORCE_DEDUPE=True to override.")

# ---------------------------
# SUMMARY
# ---------------------------

print("\n=== DEDUPE SUMMARY ===")
if not RUN_RESULTS:
    print("(No dedupe run this time.)")
else:
    for r in RUN_RESULTS:
        print(
            f"{r['table']}: status={r['status']}"
            + ("" if r.get("error") is None else f", error={r['error']}")
            + ("" if r['status'] != "ok" else f", before={r['before']:,}, after={r['after']:,}, "
               f"deleted={r['deleted']:,} ({r['pct_deleted']}%)")
        )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, time

# Define cutoff time
cutoff = time(19, 0)   # 7:00 PM

# Get current time
now = datetime.now().time()

if now < cutoff:
    print(f"Current time {now.strftime('%H:%M:%S')} is before 7 PM. Stopping session...")
    # Option 1: For Databricks
    # dbutils.notebook.exit("Stopped because it's before 7 PM")

    # Option 2: For general PySpark
    spark.stop()
    
    # Optionally halt execution
    raise SystemExit("Notebook stopped automatically.")
else:
    print("It's after 7 PM — continuing execution.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================
# CONFIG — EDIT THESE
# =========================
# Path to the cleanup (source) table in OneLake
CLEANUP_PATH = (
    "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/"
    "25adf76d-df18-41c8-97e8-55789149bd80/Tables/cr535_procleanupautojoblogs"
)

# Key column names
SOURCE_KEY_COL = "cr535_productionorderno"  # in the cleanup table
TARGET_KEY_COL = "prod_order_no"            # in all target tables

# Target lakehouse root paths (point each to its Tables directory)
LAKEHOUSE_PATHS = {
    "Silver_Production_Lakehouse": "abfss://d74457b3-045c-445d-82c6-9a2e4b9f1436@onelake.dfs.fabric.microsoft.com/ad99fdfa-85b1-4480-9f7f-2640bfd65f24/Tables/prod",
    "Gold_Production_Lakehouse": "abfss://d74457b3-045c-445d-82c6-9a2e4b9f1436@onelake.dfs.fabric.microsoft.com/6fa25cdd-36f9-4f2e-9817-c1f4d946d4d9/Tables/prod",
    "Silver_Commons_Lakehouse": "abfss://d74457b3-045c-445d-82c6-9a2e4b9f1436@onelake.dfs.fabric.microsoft.com/e248ea90-8431-4df2-9f29-87866bf9dd5a/Tables/cmn",
    "Silver_Customer_Exp_Lakehouse": "abfss://d74457b3-045c-445d-82c6-9a2e4b9f1436@onelake.dfs.fabric.microsoft.com/ff4d6787-a716-43b6-baaf-972b7426ffa5/Tables/cx",
    "Silver_Finance_Lakehouse": "abfss://d74457b3-045c-445d-82c6-9a2e4b9f1436@onelake.dfs.fabric.microsoft.com/869b263b-1a86-424b-bd97-94bd586442b2/Tables/fa",
    "Silver_Inventory_Lakehouse": "abfss://d74457b3-045c-445d-82c6-9a2e4b9f1436@onelake.dfs.fabric.microsoft.com/3a130b81-98ec-4fd4-a404-95edc1f0ef1e/Tables/inv",
}

DRY_RUN = False  # change to False to actually delete

# =========================
# MAIN SCRIPT
# =========================
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# ---- Load cleanup list ----
cleanup_df_raw = spark.read.format("delta").load(CLEANUP_PATH)
cleanup_df = (
    cleanup_df_raw
    .select(F.trim(F.col(SOURCE_KEY_COL).cast("string")).alias(SOURCE_KEY_COL))
    .dropna()
    .dropDuplicates()
    .cache()
)
cleanup_count = cleanup_df.count()
if cleanup_count == 0:
    raise ValueError("Cleanup table has no keys.")
print(f"[INFO] Loaded {cleanup_count} cleanup keys.")

# ---- Helper: list Delta tables under a Lakehouse Tables folder ----
def list_delta_tables(tables_root: str):
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        uri = spark._jvm.java.net.URI(tables_root)
        path = spark._jvm.org.apache.hadoop.fs.Path(uri.getPath())
        statuses = fs.listStatus(path)
        tbls = []
        for st in statuses:
            if st.isDirectory():
                tbl_name = st.getPath().getName()
                delta_log = spark._jvm.org.apache.hadoop.fs.Path(uri.getPath() + "/" + tbl_name + "/_delta_log")
                if fs.exists(delta_log):
                    tbls.append((tbl_name, tables_root.rstrip("/") + "/" + tbl_name))
        return tbls
    except Exception as e:
        print(f"[WARN] could not list tables under {tables_root}: {e}")
        return []

# ---- Helper: check if a table has the target column ----
def has_target_col(table_path: str, target_col: str):
    try:
        cols = [f.name.lower() for f in spark.read.format("delta").load(table_path).schema.fields]
        return target_col.lower() in cols
    except Exception:
        return False

# ---- Helper: perform delete ----
def delete_rows(table_path: str, key_col: str):
    dt = DeltaTable.forPath(spark, table_path)
    df_keys = cleanup_df.select(F.col(SOURCE_KEY_COL).alias("key"))
    # left anti join to keep non-matches
    existing = spark.read.format("delta").load(table_path)
    keep_df = existing.join(df_keys, F.trim(F.col(key_col).cast("string")) == F.col("key"), "left_anti")
    before = existing.count()
    keep_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path)
    after = keep_df.count()
    return before - after

# ---- Process each Lakehouse ----
results = []
for name, tables_root in LAKEHOUSE_PATHS.items():
    print(f"\n[INFO] scanning lakehouse {name}")
    tables = list_delta_tables(tables_root)
    if not tables:
        print(f"[INFO] no Delta tables found in {tables_root}")
        continue

    for tbl_name, tbl_path in tables:
        if not has_target_col(tbl_path, TARGET_KEY_COL):
            results.append((name, tbl_name, 0, "no_target_col"))
            continue

        df = spark.read.format("delta").load(tbl_path)
        joined = df.join(cleanup_df, F.trim(F.col(TARGET_KEY_COL).cast("string")) == F.col(SOURCE_KEY_COL), "inner")
        count = joined.count()

        if count == 0:
            results.append((name, tbl_name, 0, "none"))
            continue

        if DRY_RUN:
            print(f"[DRY] {name}.{tbl_name}: {count} rows would be deleted.")
            results.append((name, tbl_name, count, "would_delete"))
        else:
            deleted = delete_rows(tbl_path, TARGET_KEY_COL)
            print(f"[DONE] {name}.{tbl_name}: deleted {deleted} rows.")
            results.append((name, tbl_name, deleted, "deleted"))

# ---- Show summary ----
summary_df = spark.createDataFrame(results, "lakehouse STRING, table STRING, affected LONG, action STRING")
summary_df.orderBy(F.col("affected").desc()).show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================
# CONFIG — EDIT THESE
# =========================
# Source-of-truth table (allowed orders)
SOURCE_ALLOWED_PATH = (
    "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/"
    "25adf76d-df18-41c8-97e8-55789149bd80/Tables/cr535_productionorder"
)
SOURCE_ALLOWED_COL = "cr535_no"   # key column in the source table (allowed values)

# Target key column name (in all Silver/Gold tables)
TARGET_KEY_COL = "prod_order_no"

# Target lakehouse root paths (point each to its Tables directory)
LAKEHOUSE_PATHS = {
    "Silver_Production_Lakehouse": "abfss://d74457b3-045c-445d-82c6-9a2e4b9f1436@onelake.dfs.fabric.microsoft.com/ad99fdfa-85b1-4480-9f7f-2640bfd65f24/Tables/prod",
    "Gold_Production_Lakehouse": "abfss://d74457b3-045c-445d-82c6-9a2e4b9f1436@onelake.dfs.fabric.microsoft.com/6fa25cdd-36f9-4f2e-9817-c1f4d946d4d9/Tables/prod",
    "Silver_Commons_Lakehouse": "abfss://d74457b3-045c-445d-82c6-9a2e4b9f1436@onelake.dfs.fabric.microsoft.com/e248ea90-8431-4df2-9f29-87866bf9dd5a/Tables/cmn",
    "Silver_Customer_Exp_Lakehouse": "abfss://d74457b3-045c-445d-82c6-9a2e4b9f1436@onelake.dfs.fabric.microsoft.com/ff4d6787-a716-43b6-baaf-972b7426ffa5/Tables/cx",
    "Silver_Finance_Lakehouse": "abfss://d74457b3-045c-445d-82c6-9a2e4b9f1436@onelake.dfs.fabric.microsoft.com/869b263b-1a86-424b-bd97-94bd586442b2/Tables/fa",
    "Silver_Inventory_Lakehouse": "abfss://d74457b3-045c-445d-82c6-9a2e4b9f1436@onelake.dfs.fabric.microsoft.com/3a130b81-98ec-4fd4-a404-95edc1f0ef1e/Tables/inv",
}

# Dry-run shows counts + distinct keys to be deleted; set False to actually rewrite tables
DRY_RUN = False

# (Optional) Write the distinct deleted keys per table to OneLake for auditing
EXPORT_DELETING_KEYS = False
EXPORT_BASE_PATH = None  # e.g., "abfss://.../<someLakehouseId>/Files/cleanup_audit/prod_orders_not_in_source"

# Allow/Deny filters by table name (folder name under /Tables)
ALLOW_TABLES = []  # e.g., ["fact_production"]
DENY_TABLES  = ["gold_print_production_order_casting"]  # e.g., ["supa_sensitive_table"]
# =========================


from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

def should_process_table(tbl: str) -> bool:
    if ALLOW_TABLES:
        return tbl in ALLOW_TABLES
    if DENY_TABLES and tbl in DENY_TABLES:
        return False
    return True

def normalize_key(df, colname: str, alias: str):
    return (
        df.select(F.trim(F.col(colname).cast("string")).alias(alias))
          .where(F.col(alias).isNotNull())
    )

# ---- Load allowed set from SOURCE (cr535_productionorder / cr535_no) ----
src_df_raw = spark.read.format("delta").load(SOURCE_ALLOWED_PATH)
allowed_df = (
    normalize_key(src_df_raw, SOURCE_ALLOWED_COL, "key")
    .dropDuplicates()
    .cache()
)
allowed_cnt = allowed_df.count()
if allowed_cnt == 0:
    raise ValueError("Source-of-truth table has 0 allowed keys. Fill it first.")
print(f"[INFO] Loaded {allowed_cnt} allowed prod orders from {SOURCE_ALLOWED_PATH}")

# ---- Helpers to enumerate Delta tables path-first ----
def list_delta_tables(tables_root: str):
    """
    Finds immediate child folders that are Delta tables (contain _delta_log).
    Returns: list[(table_name, table_path)]
    """
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        uri = spark._jvm.java.net.URI(tables_root)
        path = spark._jvm.org.apache.hadoop.fs.Path(uri.getPath())
        if not fs.exists(path):
            print(f"[WARN] {tables_root} does not exist.")
            return []
        statuses = fs.listStatus(path)
        out = []
        for st in statuses:
            if st.isDirectory():
                name = st.getPath().getName()
                tpath = tables_root.rstrip("/") + "/" + name
                delta_log = spark._jvm.org.apache.hadoop.fs.Path(uri.getPath() + "/" + name + "/_delta_log")
                if fs.exists(delta_log):
                    out.append((name, tpath))
        return out
    except Exception as e:
        print(f"[WARN] could not list {tables_root}: {e}")
        return []

def has_col(table_path: str, colname: str) -> bool:
    try:
        cols = [f.name.lower() for f in spark.read.format("delta").load(table_path).schema.fields]
        return colname.lower() in cols
    except Exception:
        return False

# ---- Deletion logic (opposite direction): keep only rows whose prod_order_no IS IN allowed set ----
def compute_rows_to_delete(table_path: str, key_col: str):
    """
    Returns (rows_to_delete_df, distinct_keys_df)
    rows_to_delete_df: all rows whose key NOT IN allowed (i.e., will be deleted)
    distinct_keys_df: distinct bad keys to be removed
    """
    df = spark.read.format("delta").load(table_path)
    df_norm = df.withColumn("_key_norm", F.trim(F.col(key_col).cast("string")))
    # left_anti -> rows NOT matching allowed set => these are to be deleted
    rows_to_delete = df_norm.join(allowed_df, df_norm["_key_norm"] == allowed_df["key"], "left_anti")
    bad_keys = rows_to_delete.select(F.col("_key_norm").alias("prod_order_no")).where(F.col("prod_order_no").isNotNull()).dropDuplicates()
    return rows_to_delete, bad_keys

def rewrite_keep_only_allowed(table_path: str, key_col: str) -> tuple[int, int]:
    """
    Overwrite table with only rows whose key is in allowed set (left_semi).
    Returns: (deleted_rows_count, kept_rows_count)
    """
    df = spark.read.format("delta").load(table_path)
    before = df.count()
    df_norm = df.withColumn("_key_norm", F.trim(F.col(key_col).cast("string")))
    keep_df = df_norm.join(allowed_df, df_norm["_key_norm"] == allowed_df["key"], "left_semi").drop("_key_norm")
    kept = keep_df.count()
    deleted = before - kept
    # overwrite in place
    keep_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path)
    return deleted, kept

# ---- (Optional) export distinct keys to OneLake for audit ----
def export_distinct_bad_keys(lh_name: str, tbl_name: str, keys_df, base_path: str):
    try:
        out_dir = base_path.rstrip("/") + f"/{lh_name}/{tbl_name}"
        keys_df.write.format("delta").mode("overwrite").save(out_dir)
        print(f"[AUDIT] wrote distinct bad keys -> {out_dir}")
    except Exception as e:
        print(f"[WARN] failed to export keys for {lh_name}.{tbl_name}: {e}")

# ---- Main loop ----
results = []

for lh_name, tables_root in LAKEHOUSE_PATHS.items():
    print(f"\n[INFO] scanning lakehouse: {lh_name}")
    tbls = list_delta_tables(tables_root)
    if not tbls:
        print(f"[INFO] no Delta tables found in {tables_root}")
        continue

    for tbl_name, tbl_path in tbls:
        if not should_process_table(tbl_name):
            results.append((lh_name, tbl_name, 0, 0, "skipped"))
            continue

        if not has_col(tbl_path, TARGET_KEY_COL):
            results.append((lh_name, tbl_name, 0, 0, "no_target_col"))
            continue

        # What will be deleted? (NOT IN source list)
        rows_to_delete_df, bad_keys_df = compute_rows_to_delete(tbl_path, TARGET_KEY_COL)
        delete_row_count = rows_to_delete_df.count()
        delete_key_count = bad_keys_df.count()

        if delete_row_count == 0:
            results.append((lh_name, tbl_name, 0, 0, "none"))
            continue

        # Show a quick peek of the distinct bad keys
        sample_keys = [r["prod_order_no"] for r in bad_keys_df.limit(20).collect()]
        print(f"[DRY] {lh_name}.{tbl_name}: {delete_row_count} rows would be deleted "
              f"({delete_key_count} distinct prod_order_no). Sample: {sample_keys}")

        # Export distinct keys to OneLake if asked
        if EXPORT_DELETING_KEYS and EXPORT_BASE_PATH:
            export_distinct_bad_keys(lh_name, tbl_name, bad_keys_df, EXPORT_BASE_PATH)

        if DRY_RUN:
            results.append((lh_name, tbl_name, delete_row_count, delete_key_count, "would_delete"))
        else:
            try:
                deleted, kept = rewrite_keep_only_allowed(tbl_path, TARGET_KEY_COL)
                print(f"[DONE] {lh_name}.{tbl_name}: deleted {deleted}, kept {kept}")
                results.append((lh_name, tbl_name, deleted, delete_key_count, "deleted"))
            except Exception as e:
                print(f"[ERR] {lh_name}.{tbl_name}: delete failed: {e}")
                results.append((lh_name, tbl_name, delete_row_count, delete_key_count, f"delete_failed: {str(e)[:160]}"))

# ---- Summary ----
summary_df = spark.createDataFrame(
    results, "lakehouse STRING, table STRING, rows_deleted LONG, distinct_keys_deleted LONG, action STRING"
)
summary_df.orderBy(F.col("rows_deleted").desc(), "lakehouse", "table").show(200, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# =========================
# CONFIG — EDIT THESE
# =========================
# Source-of-truth (orders) and (order lines)
ORDERS_PATH = (
    "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/"
    "25adf76d-df18-41c8-97e8-55789149bd80/Tables/cr535_productionorder"
)
ORDERS_COL = "cr535_no"  # order key in ORDERS_PATH

ORDERLINES_PATH = (
    "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/"
    "25adf76d-df18-41c8-97e8-55789149bd80/Tables/cr535_prodorderline"
)
ORDERLINES_ORDER_COL = "cr535_prodorderno"
ORDERLINES_LINE_COL  = "cr535_lineno"

# Target column names (in your Silver/Gold tables)
TARGET_ORDER_COL = "prod_order_no"
TARGET_LINE_CANDIDATES = ["prod_order_line_no", "prodorder_lineno", "prod_order_lineno"]

# Lakehouse /Tables roots (fill these with actual ABFSS from Fabric UI → Files → Copy path)
LAKEHOUSE_PATHS = {
    "Silver_Production_Lakehouse": "abfss://d74457b3-045c-445d-82c6-9a2e4b9f1436@onelake.dfs.fabric.microsoft.com/ad99fdfa-85b1-4480-9f7f-2640bfd65f24/Tables/prod",
    "Gold_Production_Lakehouse": "abfss://d74457b3-045c-445d-82c6-9a2e4b9f1436@onelake.dfs.fabric.microsoft.com/6fa25cdd-36f9-4f2e-9817-c1f4d946d4d9/Tables/prod",
    "Silver_Commons_Lakehouse": "abfss://d74457b3-045c-445d-82c6-9a2e4b9f1436@onelake.dfs.fabric.microsoft.com/e248ea90-8431-4df2-9f29-87866bf9dd5a/Tables/cmn",
    "Silver_Customer_Exp_Lakehouse": "abfss://d74457b3-045c-445d-82c6-9a2e4b9f1436@onelake.dfs.fabric.microsoft.com/ff4d6787-a716-43b6-baaf-972b7426ffa5/Tables/cx",
    "Silver_Finance_Lakehouse": "abfss://d74457b3-045c-445d-82c6-9a2e4b9f1436@onelake.dfs.fabric.microsoft.com/869b263b-1a86-424b-bd97-94bd586442b2/Tables/fa",
    "Silver_Inventory_Lakehouse": "abfss://d74457b3-045c-445d-82c6-9a2e4b9f1436@onelake.dfs.fabric.microsoft.com/3a130b81-98ec-4fd4-a404-95edc1f0ef1e/Tables/inv",
}

# Modes & audit
DRY_RUN = False  # set False to actually rewrite the tables
EXPORT_AUDIT = False
AUDIT_BASE = None  # e.g., "abfss://.../<someLakehouseId>/Files/cleanup_audit/opposite_rule"

# Optional allow/deny lists (table folder names under /Tables)
ALLOW_TABLES = []  # e.g., ["fact_production"]
DENY_TABLES  = ["gold_print_production_order_casting" ]  # e.g., ["do_not_touch"]
# =========================

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# ---------- helpers ----------
def normalize_str(col):
    return F.trim(F.col(col).cast("string"))

def should_process_table(tbl: str) -> bool:
    if ALLOW_TABLES:
        return tbl in ALLOW_TABLES
    if DENY_TABLES and tbl in DENY_TABLES:
        return False
    return True

def list_delta_tables(tables_root: str):
    """List immediate child folders that are Delta tables (contain _delta_log)."""
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        uri = spark._jvm.java.net.URI(tables_root)
        path = spark._jvm.org.apache.hadoop.fs.Path(uri.getPath())
        if not fs.exists(path):
            print(f"[WARN] {tables_root} does not exist.")
            return []
        statuses = fs.listStatus(path)
        out = []
        for st in statuses:
            if st.isDirectory():
                name = st.getPath().getName()
                delta_log = spark._jvm.org.apache.hadoop.fs.Path(uri.getPath() + "/" + name + "/_delta_log")
                if fs.exists(delta_log):
                    out.append((name, tables_root.rstrip("/") + "/" + name))
        return out
    except Exception as e:
        print(f"[WARN] could not list {tables_root}: {e}")
        return []

def schema_has_col(table_path: str, colname: str) -> bool:
    try:
        cols = [f.name.lower() for f in spark.read.format("delta").load(table_path).schema.fields]
        return colname.lower() in cols
    except Exception:
        return False

def find_line_col(table_path: str, candidates: list[str]) -> str | None:
    try:
        names = [f.name for f in spark.read.format("delta").load(table_path).schema.fields]
        lower_map = {n.lower(): n for n in names}
        for c in candidates:
            if c.lower() in lower_map:
                return lower_map[c.lower()]
        return None
    except Exception:
        return None

def export_df(df, base, lh, tbl, name):
    out = base.rstrip("/") + f"/{lh}/{tbl}/{name}"
    df.write.format("delta").mode("overwrite").save(out)
    print(f"[AUDIT] wrote -> {out}")

# ---------- load allowed keys ----------
orders_raw = spark.read.format("delta").load(ORDERS_PATH)
allowed_orders = (
    orders_raw.select(normalize_str(ORDERS_COL).alias("order_key"))
              .where(F.col("order_key").isNotNull())
              .dropDuplicates()
              .cache()
)
orders_cnt = allowed_orders.count()
if orders_cnt == 0:
    raise ValueError("Source orders table has 0 rows.")
print(f"[INFO] Allowed orders: {orders_cnt}")

orderlines_raw = spark.read.format("delta").load(ORDERLINES_PATH)
allowed_pairs = (
    orderlines_raw.select(
        normalize_str(ORDERLINES_ORDER_COL).alias("order_key"),
        normalize_str(ORDERLINES_LINE_COL).alias("line_key"),
    )
    .where(F.col("order_key").isNotNull() & F.col("line_key").isNotNull())
    .dropDuplicates()
    .cache()
)
pairs_cnt = allowed_pairs.count()
print(f"[INFO] Allowed (order,line) pairs: {pairs_cnt}")

# ---------- main loop ----------
results = []
for lh_name, root in LAKEHOUSE_PATHS.items():
    print(f"\n[INFO] scanning lakehouse: {lh_name}")
    tables = list_delta_tables(root)
    if not tables:
        print(f"[INFO] no Delta tables under {root}")
        continue

    for tbl_name, tbl_path in tables:
        if not should_process_table(tbl_name):
            results.append((lh_name, tbl_name, 0, 0, 0, "skipped"))
            continue

        # must have order col
        if not schema_has_col(tbl_path, TARGET_ORDER_COL):
            results.append((lh_name, tbl_name, 0, 0, 0, "no_order_col"))
            continue

        # detect line col (if present)
        line_col = find_line_col(tbl_path, TARGET_LINE_CANDIDATES)

        df = spark.read.format("delta").load(tbl_path)
        df = df.withColumn("_ord", normalize_str(TARGET_ORDER_COL))

        # stage 1: keep only rows whose order is in allowed_orders
        keep_stage1 = df.join(allowed_orders, df["_ord"] == allowed_orders["order_key"], "left_semi")

        # rows removed by stage1
        deleted_stage1 = df.join(allowed_orders, df["_ord"] == allowed_orders["order_key"], "left_anti")
        deleted_stage1_cnt = deleted_stage1.count()
        deleted_stage1_orders = (
            deleted_stage1.select(F.col("_ord").alias("prod_order_no"))
                          .where(F.col("prod_order_no").isNotNull())
                          .dropDuplicates()
        )
        deleted_stage1_orders_cnt = deleted_stage1_orders.count()

        # stage 2 (only if line_col exists): keep only rows whose (order,line) pair exists
        if line_col:
            keep_stage1 = keep_stage1.withColumn("_line", normalize_str(line_col))
            keep_final = keep_stage1.join(
                allowed_pairs,
                (keep_stage1["_ord"] == allowed_pairs["order_key"]) &
                (keep_stage1["_line"] == allowed_pairs["line_key"]),
                "left_semi"
            )

            # rows removed by stage2
            removed_by_stage2 = keep_stage1.join(
                allowed_pairs,
                (keep_stage1["_ord"] == allowed_pairs["order_key"]) &
                (keep_stage1["_line"] == allowed_pairs["line_key"]),
                "left_anti"
            )
            deleted_stage2_cnt = removed_by_stage2.count()
            deleted_stage2_pairs = (
                removed_by_stage2
                    .select(
                        F.col("_ord").alias("prod_order_no"),
                        F.col("_line").alias("prod_order_line_no_detected")
                    )
                    .where(F.col("prod_order_no").isNotNull() & F.col("prod_order_line_no_detected").isNotNull())
                    .dropDuplicates()
            )
            deleted_stage2_pairs_cnt = deleted_stage2_pairs.count()

            # final to write (if DRY_RUN = False)
            final_df = keep_final.drop("_ord", "_line")
        else:
            # no line column → stage1 result is final
            deleted_stage2_cnt = 0
            deleted_stage2_pairs_cnt = 0
            deleted_stage2_pairs = spark.createDataFrame([], "prod_order_no string, prod_order_line_no_detected string")
            final_df = keep_stage1.drop("_ord")

        # audit logging
        print(f"[DRY] {lh_name}.{tbl_name} "
              f"| stage1 drop rows(not in orders): {deleted_stage1_cnt} "
              f"| stage2 drop rows(pair-missing): {deleted_stage2_cnt} "
              f"| total drop: {deleted_stage1_cnt + deleted_stage2_cnt}")

        # (optional) export distinct deleted keys
        if EXPORT_AUDIT and AUDIT_BASE:
            if deleted_stage1_orders_cnt > 0:
                export_df(deleted_stage1_orders, AUDIT_BASE, lh_name, tbl_name, "deleted_distinct_orders")
            if deleted_stage2_pairs_cnt > 0:
                export_df(deleted_stage2_pairs, AUDIT_BASE, lh_name, tbl_name, "deleted_distinct_pairs")

        if DRY_RUN:
            results.append((
                lh_name, tbl_name,
                deleted_stage1_cnt, deleted_stage2_cnt, deleted_stage1_cnt + deleted_stage2_cnt,
                "would_delete" if (deleted_stage1_cnt + deleted_stage2_cnt) > 0 else "none"
            ))
        else:
            before = df.count()
            final_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(tbl_path)
            after = final_df.count()
            actually_deleted = before - after
            print(f"[DONE] {lh_name}.{tbl_name}: deleted {actually_deleted}, kept {after}")
            results.append((
                lh_name, tbl_name,
                deleted_stage1_cnt, deleted_stage2_cnt, actually_deleted, "deleted"
            ))

# summary
summary_schema = "lakehouse string, table string, dropped_by_stage1 long, dropped_by_stage2 long, total_dropped long, action string"
summary_df = spark.createDataFrame(results, summary_schema)
summary_df.orderBy(F.col("total_dropped").desc(), "lakehouse", "table").show(200, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
