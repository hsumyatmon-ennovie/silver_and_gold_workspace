# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "785307fd-af78-4359-969a-51c937ec834b",
# META       "default_lakehouse_name": "dataverse_ennoviedev_cds2_workspace_unq85a0b4fa330ef111afc0000d3a80b",
# META       "default_lakehouse_workspace_id": "17f91c34-23a9-4685-b7f6-2246823b5572",
# META       "known_lakehouses": [
# META         {
# META           "id": "ad99fdfa-85b1-4480-9f7f-2640bfd65f24"
# META         },
# META         {
# META           "id": "785307fd-af78-4359-969a-51c937ec834b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from datetime import date, datetime
from pyspark.sql import functions as F, types as T, Window as W

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================
# HELPERS
# ============================
def parse_ts(col):
    return F.coalesce(
        F.to_timestamp(col),
        F.to_timestamp(col, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss"),
    )

def parse_date(col):
    return F.coalesce(
        F.to_date(col),
        F.to_date(col, "yyyy-MM-dd"),
        F.to_date(col, "dd/MM/yyyy"),
        F.to_date(col, "MM/dd/yyyy"),
    )

def has_cols(df, cols):
    s = set(df.columns)
    return all(c in s for c in cols)

def month_windows(start_ts: datetime, end_ts: datetime, step_months=1):
    """Yield (start, end) month windows [inclusive, exclusive)"""
    y, m = start_ts.year, start_ts.month
    while True:
        win_start = datetime(y, m, 1)
        # next month
        nm = m + step_months
        ny = y + (nm - 1) // 12
        nm = 1 + (nm - 1) % 12
        win_end = datetime(ny, nm, 1)
        if win_start >= end_ts:
            break
        yield (win_start, min(win_end, end_ts))
        y, m = ny, nm

def safe_count(df, label):
    try:
        c = df.count()
        print(f"{label}: {c:,}")
        return c
    except Exception as e:
        print(f"{label}: count failed -> {e}")
        return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================
# LOAD WATERMARK
# ============================
def load_watermark():
    try:
        wm_df = spark.read.format("delta").load(WATERMARK_TBL)
        last_processed_ts = wm_df.agg(F.max("last_processed_ts")).first()[0]
        print(f"Watermark found: {last_processed_ts}")

        return last_processed_ts

    except Exception:
        last_processed_ts = None
        print("No watermark table yet; will backfill.")

        return last_processed_ts

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================
# READ BRONZE ONCE (schema only) + detect optional cols
# ============================
def read_bronze_schema():
    bronze_all = spark.read.format("delta").load(BRONZE_TABLE).selectExpr("*")  # avoid re-read in loop for schema
    # optional columns typical in Dataverse landings
    guid_candidates = [c for c in bronze_all.columns if c.endswith("id")]  # heuristics (e.g., cr535_employeeoutputid)
    has_version = "versionnumber" in bronze_all.columns

    print("Detected GUID columns:", guid_candidates)
    print("Has versionnumber   :", has_version)

    return guid_candidates, has_version, bronze_all

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================
# BUILD THE LOAD (BACKFILL OR INCREMENTAL)
# ============================
def fill_data(bronze_all, last_processed_ts):
    bronze_all = bronze_all.repartition(TARGET_PARTS)  # gently parallelize; adjust to your CU/cluster size

    # Get all column names
    cols = [c.lower() for c in bronze_all.columns]

    # Choose the correct column name
    time_col = (
        "SinkModifiedOn" if "sinkmodifiedon" in cols 
        else "modifiedon" if "modifiedon" in cols 
        else "modified_on"
    )

    if last_processed_ts:
        # Incremental: ONLY rows strictly newer than watermark
        bronze_slice = bronze_all.filter(F.col(time_col) > F.lit(last_processed_ts).cast("timestamp"))
        print("Mode: INCREMENTAL")
        safe_count(bronze_slice, "bronze.incremental")
        df = bronze_slice
    else:
        # First time: FULL backfill in monthly chunks (CU-friendly)
        print("Mode: FIRST-RUN BACKFILL (monthly chunks)")
        start_ts = datetime.fromisoformat(BACKFILL_FROM)
        end_ts   = datetime.utcnow()

        acc = None

        for i, (win_start, win_end) in enumerate(month_windows(start_ts, end_ts, CHUNK_MONTHS), start=1):
            print(f"Backfill chunk {i}: {win_start:%Y-%m-%d} → {win_end:%Y-%m-%d}")
            
            slice_i = bronze_all.filter(
                (F.col(time_col) >= F.lit(win_start)) & (F.col(time_col) < F.lit(win_end))
            )
            
            safe_count(slice_i, f"bronze.chunk_{i}")
            df_i = slice_i

            if acc is None:
                acc = df_i
            else:
                acc = acc.unionByName(df_i, allowMissingColumns=True)


        df = acc if acc is not None else spark.createDataFrame([], schema=transform(bronze_all.limit(0)).schema)

    # Final sanity
    rows_loaded = safe_count(df, "silver.batch_ready")

    return rows_loaded, df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================
# WRITE SINGLE PARQUET FOR WAREHOUSE COPY
# ============================
def write_data_into_parquet(rows_loaded, df):
    final_dir   = f"{STAGING_ROOT}/runs/"
    tmp_dir     = f"{final_dir}_tmp_write"
    final_name  = "batch.parquet"
    target_path = final_dir + final_name

    if rows_loaded is None or rows_loaded == 0:
        print("No rows to write; skipping file + watermark.")
    else:
        # Fabric FS utils (fallback to dbutils)
        try:
            from notebookutils import mssparkutils as fs
            ls      = lambda p: fs.fs.ls(p)
            rm      = lambda p, r=True: fs.fs.rm(p, r)
            mv      = lambda s, d, o=True: fs.fs.mv(s, d, o)
            mkdirs  = lambda p: fs.fs.mkdirs(p)
        except Exception:
            ls      = lambda p: dbutils.fs.ls(p)
            rm      = lambda p, r=True: dbutils.fs.rm(p, r)
            mv      = lambda s, d, o=True: dbutils.fs.mv(s, d)  # pre-delete dest if needed
            mkdirs  = lambda p: dbutils.fs.mkdirs(p)

        # Clean final dir (ensures exactly one file is visible to COPY)
        try: rm(final_dir, True)
        except: pass
        mkdirs(final_dir)

        # Write to temp dir
        writer = df.coalesce(1) if SINGLE_FILE else df
        (writer.write
            .mode("overwrite")
            .format("parquet")
            .save(tmp_dir))

        # Move the single part to final
        parts = [f.path for f in ls(tmp_dir) if f.name.endswith(".parquet")]
        if SINGLE_FILE and len(parts) != 1:
            raise RuntimeError(f"Expected exactly 1 parquet in tmp dir, found {len(parts)}")
        src = parts[0] if SINGLE_FILE else tmp_dir

        # delete existing target if needed (dbutils fallback)
        try: rm(target_path, False)
        except: pass

        mv(src, target_path, True)
        # remove temp folder (only when SINGLE_FILE=True)
        if SINGLE_FILE: 
            rm(tmp_dir, True)

        print(f"Wrote {'single ' if SINGLE_FILE else ''}parquet: {target_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================
# ADVANCE WATERMARK (ONLY AFTER SUCCESS)
# ============================
def update_watermark(bronze_all, df):
    # Get all column names
    cols = [c.lower() for c in bronze_all.columns]

    # Choose the correct column name
    time_col = (
        "SinkModifiedOn" if "sinkmodifiedon" in cols 
        else "modifiedon" if "modifiedon" in cols 
        else "modified_on"
    )
    print(time_col)
    max_mod = df.agg(F.max(time_col)).first()[0]
    if max_mod:
        (spark.createDataFrame([(max_mod,)], "last_processed_ts timestamp")
             .write.mode("overwrite").format("delta").save(WATERMARK_TBL))
        print(f"Watermark advanced to: {max_mod}")
    else:
        print("Batch had no modifiedon; watermark not updated.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def run():

    last_processed_ts =load_watermark()
    print(last_processed_ts)

    guid_candidates, has_version, bronze_all = read_bronze_schema()

    rows_loaded, df = fill_data(bronze_all, last_processed_ts)

    df = df.dropDuplicates()

    # Get all column names
    cols = [c.lower() for c in bronze_all.columns]

    # Choose the correct column name
    time_col = (
        "SinkModifiedOn" if "sinkmodifiedon" in cols 
        else "modifiedon" if "modifiedon" in cols 
        else "modified_on"
    )
    max_mod = df.agg(F.max(time_col)).first()[0]

    print(rows_loaded)
    print(time_col)
    print("Max_mod: ", max_mod)
    print("Last_process_ts: ", last_processed_ts)

    if rows_loaded != 0:
        if last_processed_ts == None or max_mod > last_processed_ts:

            write_data_into_parquet(rows_loaded, df)

            update_watermark(bronze_all, df)
        else:
            print("This batch is already in Silver")
    else:
        print("No new rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Run

# CELL ********************

BRONZE_PATH    = "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennoviedev_cds2_workspace_unq85a0b4fa330ef111afc0000d3a80b.Lakehouse/Tables/"
BRONZE_TABLE   = BRONZE_PATH + "cr535_dsvcproductionorderstatus"

STAGING_PATH   = "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennoviedev_cds2_workspace_unq85a0b4fa330ef111afc0000d3a80b.Lakehouse/Files/silver_staging/"   # Where we drop the single Parquet for Warehouse COPY
STAGING_ROOT   = STAGING_PATH +  "silver_sync/" + "silver_production_status_all"

WATERMARK_PATH  = "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennoviedev_cds2_workspace_unq85a0b4fa330ef111afc0000d3a80b.Lakehouse/Tables/metadata/"     # Delta table with column: last_processed_ts TIMESTAMP
WATERMARK_TBL  = WATERMARK_PATH + "silver_wm_prod_production_status_all"
BACKFILL_FROM  = "2019-01-01"                               # earliest modifiedon to consider (safety)
CHUNK_MONTHS   = 1                                          # backfill chunk size (months) — balances CUs vs speed
TARGET_PARTS   = 16                                         # default partitions for transforms (tune per cluster)
SINGLE_FILE    = True                                       # keep True to output a single Parquet like your flow

run()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
