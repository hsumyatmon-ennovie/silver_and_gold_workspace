# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b94fc278-235b-488b-a7da-0a783bf50307",
# META       "default_lakehouse_name": "Gold_Finance_Lakehouse",
# META       "default_lakehouse_workspace_id": "d74457b3-045c-445d-82c6-9a2e4b9f1436",
# META       "known_lakehouses": [
# META         {
# META           "id": "b94fc278-235b-488b-a7da-0a783bf50307"
# META         },
# META         {
# META           "id": "1d620310-5acc-4534-93f9-f52f082a1887"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# =============================================================================
# nb_bc_schema_dictionary_topup.py
# =============================================================================
# Purpose: Capture schema for tables MISSING from the main dictionary CSV.
#          Designed to be more robust on large tables that may have timed out
#          or rate-limited during the original run.
#
# Differences from main dictionary script:
#   - Uses LIMIT (without ORDER BY RAND) — much faster on large tables
#   - Longer sleep between tables (1.5s vs 0.3s)
#   - Retries 1 time on failure with 5s backoff
#   - Reads existing dictionary CSV to find missing tables automatically
#
# Output appended to: bc_schema_dictionary_<date>.csv (same path as main script)
# =============================================================================

from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import pandas as pd
import os
import time

spark = SparkSession.builder.getOrCreate()

TODAY = date.today().isoformat()
OUT_DIR = '/lakehouse/default/Files/finance_reports'
SILVER_LH = 'Silver_BC_Lakehouse'
SCHEMA = 'bc'
SAMPLE_SIZE = 3
SLEEP_BETWEEN_TABLES = 1.5
EXISTING_DICT_PATH = f'{OUT_DIR}/bc_schema_dictionary_{TODAY}.csv'

print("BC Schema Dictionary — Top-up for missing tables")
print("=" * 70)


# -----------------------------------------------------------------------------
# Step 1 — Compare existing dictionary against actual table list
# -----------------------------------------------------------------------------
print("\n[Step 1] Identifying missing tables...")

# All tables in schema
all_tables = spark.sql(f"SHOW TABLES IN `{SILVER_LH}`.{SCHEMA}").toPandas()
all_table_names = set(all_tables['tableName'].tolist())
print(f"  Total tables in schema: {len(all_table_names):,}")

# Tables already captured
if os.path.exists(EXISTING_DICT_PATH):
    existing = pd.read_csv(EXISTING_DICT_PATH)
    captured = set(existing['table_name'].unique())
    print(f"  Tables already in dictionary: {len(captured):,}")
else:
    captured = set()
    print(f"  No existing dictionary — capturing all tables")

missing = sorted(all_table_names - captured)
print(f"  Tables to capture: {len(missing):,}")
if missing:
    print(f"  First 10 missing: {missing[:10]}")


# -----------------------------------------------------------------------------
# Step 2 — Capture each missing table (robust mode)
# -----------------------------------------------------------------------------
if not missing:
    print("\n✓ Dictionary is complete — no missing tables to capture.")
    raise SystemExit(0)

print(f"\n[Step 2] Capturing {len(missing)} missing tables...")
print(f"  Sample query: SELECT * FROM ... LIMIT {SAMPLE_SIZE} (no ORDER BY RAND)")
print(f"  Sleep between: {SLEEP_BETWEEN_TABLES}s")
print(f"  Max retries:   1 per table\n")

new_rows = []
errors = []
start_time = time.time()


def capture_table(tbl, attempt=1):
    """Capture one table's schema + sample. Returns (rows, error) tuple."""
    table_qualified = f"`{SILVER_LH}`.{SCHEMA}.`{tbl}`"
    try:
        # Schema via Spark DataFrame API (no extra query)
        df = spark.read.table(f"{SILVER_LH}.{SCHEMA}.`{tbl}`")
        nullability = {f.name: f.nullable for f in df.schema.fields}
        dtypes = {f.name: f.dataType.simpleString() for f in df.schema.fields}
        col_names = [f.name for f in df.schema.fields]

        # Row count
        row_count = spark.sql(f"SELECT COUNT(*) AS n FROM {table_qualified}").collect()[0]['n']

        # Sample — use plain LIMIT, no ORDER BY RAND (much faster on large tables)
        if row_count == 0:
            sample_pdf = pd.DataFrame(columns=col_names)
        else:
            sample_pdf = spark.sql(
                f"SELECT * FROM {table_qualified} LIMIT {SAMPLE_SIZE}"
            ).toPandas()

        rows = []
        for pos, col in enumerate(col_names, 1):
            if col in sample_pdf.columns and len(sample_pdf) > 0:
                samples = sample_pdf[col].tolist()
                while len(samples) < SAMPLE_SIZE:
                    samples.append(None)
                samples = [
                    (str(s)[:200] + '...') if s is not None and len(str(s)) > 200
                    else s
                    for s in samples[:SAMPLE_SIZE]
                ]
            else:
                samples = [None] * SAMPLE_SIZE

            rows.append({
                'table_name': tbl,
                'column_position': pos,
                'column_name': col,
                'data_type': dtypes.get(col, 'unknown'),
                'nullable': nullability.get(col, None),
                'row_count': row_count,
                'sample_1': samples[0],
                'sample_2': samples[1],
                'sample_3': samples[2],
            })
        return rows, None

    except Exception as e:
        return None, f"{type(e).__name__}: {str(e)[:300]}"


for idx, tbl in enumerate(missing, 1):
    progress = f"[{idx:3d}/{len(missing)}]"

    rows, err = capture_table(tbl, attempt=1)

    if err is not None:
        # Retry once with backoff
        print(f"{progress} {tbl:<50} ⚠ failed, retrying in 5s...")
        time.sleep(5)
        rows, err = capture_table(tbl, attempt=2)

    if rows is not None:
        new_rows.extend(rows)
        row_count = rows[0]['row_count'] if rows else 0
        n_cols = len(rows)
        print(f"{progress} {tbl:<50} {row_count:>12,} rows × {n_cols:>3} cols")
    else:
        errors.append({'table': tbl, 'error': err})
        print(f"{progress} {tbl:<50} ✗ {err[:80]}")

    time.sleep(SLEEP_BETWEEN_TABLES)

elapsed = time.time() - start_time
print(f"\n✓ Captured {len({r['table_name'] for r in new_rows})} tables in {elapsed/60:.1f} min")
print(f"  New schema rows: {len(new_rows):,}")
print(f"  Failed:          {len(errors):,}")


# -----------------------------------------------------------------------------
# Step 3 — Merge with existing dictionary and write
# -----------------------------------------------------------------------------
print(f"\n[Step 3] Writing merged dictionary...")

new_df = pd.DataFrame(new_rows)

if os.path.exists(EXISTING_DICT_PATH):
    existing_df = pd.read_csv(EXISTING_DICT_PATH)
    combined = pd.concat([existing_df, new_df], ignore_index=True)
    # Dedupe in case of overlap (keep last = newer capture)
    combined = combined.drop_duplicates(
        subset=['table_name', 'column_position'], keep='last'
    )
else:
    combined = new_df

combined = combined.sort_values(['table_name', 'column_position']).reset_index(drop=True)
combined.to_csv(EXISTING_DICT_PATH, index=False)
print(f"✓ Saved (merged): {EXISTING_DICT_PATH}")
print(f"  Total rows: {len(combined):,}")
print(f"  Total tables: {combined['table_name'].nunique():,}")

if errors:
    err_path = f'{OUT_DIR}/bc_schema_dictionary_topup_errors_{TODAY}.csv'
    pd.DataFrame(errors).to_csv(err_path, index=False)
    print(f"⚠ Errors log: {err_path}")
    print(f"  Failed tables:")
    for e in errors:
        print(f"    - {e['table']}")


# -----------------------------------------------------------------------------
# Step 4 — Verify targets present
# -----------------------------------------------------------------------------
print(f"\n[Step 4] Verification")
print("-" * 70)

targets = ['Item', 'Production Order', 'Prod Order Component', 'GL Account']
final_tables = set(combined['table_name'].unique())
for t in targets:
    status = '✓' if t in final_tables else '✗'
    n_cols = len(combined[combined['table_name'] == t]) if t in final_tables else 0
    print(f"  {status} {t:<40} {n_cols:>3} cols")

print("\n✓ Top-up complete.")

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
