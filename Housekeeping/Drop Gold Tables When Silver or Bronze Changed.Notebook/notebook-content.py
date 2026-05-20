# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ad99fdfa-85b1-4480-9f7f-2640bfd65f24",
# META       "default_lakehouse_name": "Silver_Production_Lakehouse",
# META       "default_lakehouse_workspace_id": "d74457b3-045c-445d-82c6-9a2e4b9f1436",
# META       "known_lakehouses": [
# META         {
# META           "id": "ad99fdfa-85b1-4480-9f7f-2640bfd65f24"
# META         },
# META         {
# META           "id": "6fa25cdd-36f9-4f2e-9817-c1f4d946d4d9"
# META         },
# META         {
# META           "id": "3a130b81-98ec-4fd4-a404-95edc1f0ef1e"
# META         },
# META         {
# META           "id": "81bc6bea-77b8-46fe-9189-dcfc3cd43d2f"
# META         },
# META         {
# META           "id": "e248ea90-8431-4df2-9f29-87866bf9dd5a"
# META         },
# META         {
# META           "id": "869b263b-1a86-424b-bd97-94bd586442b2"
# META         },
# META         {
# META           "id": "ff4d6787-a716-43b6-baaf-972b7426ffa5"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ==============================================
# MULTI-LAKEHOUSE TARGETED TABLE DROP
# ==============================================

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ---------------------------
# CONFIGURE HERE
# ---------------------------

# Lakehouses attached to this notebook
LAKEHOUSES = [
    # "Silver_Production_Lakehouse",
    "Gold_Production_Lakehouse",
    # "Silver_Customer_Exp_Lakehouse",
    # "Silver_Commons_Lakehouse",
    # "Silver_Inventory_Lakehouse",
]

# Tables you want to drop (per schema)
# This list is like your DEDUPE_SPECS: just add/remove rows
DROP_SPECS = [
    # examples – replace with your real tables:
    {"schema": "prod", "table": "gold_prod_cycle_time"},
    {"schema": "prod", "table": "gold_inv_output"},
    {"schema": "prod", "table": "silver_employee_rfid_mapping"},
    {"schema": "cx",   "table": "silver_sales_header"},
    {"schema": "cx",   "table": "silver_sales_line"},
    # add more here...
]

# Safety switch: start with True to see what *would* be dropped
DRY_RUN = True

# ---------------------------
# HELPERS
# ---------------------------

def table_exists(qualified_table: str) -> bool:
    """Return True if <lakehouse>.<schema>.<table> exists."""
    try:
        return spark.catalog.tableExists(qualified_table)
    except Exception:
        return False


def drop_table_if_exists(full_table: str) -> dict:
    """
    Drop a single table if it exists.
    Returns a small dict for summary.
    """
    result = {
        "table": full_table,
        "status": "skipped",
        "error": None,
    }

    if not table_exists(full_table):
        print(f"  [drop] Table not found; skipping: {full_table}")
        result["status"] = "not_found"
        return result

    if DRY_RUN:
        print(f"  [dry-run] Would drop: {full_table}")
        result["status"] = "dry_run"
        return result

    try:
        spark.sql(f"DROP TABLE IF EXISTS {full_table}")
        print(f"  [drop] Dropped: {full_table}")
        result["status"] = "dropped"
    except Exception as e:
        msg = str(e)
        print(f"  [drop] ERROR on {full_table}: {msg}")
        result["status"] = "error"
        result["error"] = msg

    return result


# ---------------------------
# RUN
# ---------------------------

RUN_RESULTS = []

print(f"DRY_RUN = {DRY_RUN}")
print(f"Lakehouses : {LAKEHOUSES}")
print("Drop specs :", [f"{s['schema']}.{s['table']}" for s in DROP_SPECS])

for lakehouse in LAKEHOUSES:
    print(f"\n=== DROP for lakehouse: {lakehouse} ===")
    for spec in DROP_SPECS:
        full_table = f"{lakehouse}.{spec['schema']}.{spec['table']}"
        res = drop_table_if_exists(full_table)
        RUN_RESULTS.append(res)

# ---------------------------
# SUMMARY
# ---------------------------

print("\n=== DROP SUMMARY ===")
if not RUN_RESULTS:
    print("(No tables processed.)")
else:
    for r in RUN_RESULTS:
        if r["status"] == "error":
            print(f"{r['table']}: status={r['status']}, error={r['error']}")
        else:
            print(f"{r['table']}: status={r['status']}")

if DRY_RUN:
    print("\nDRY_RUN is True → nothing was actually dropped.")
    print("If this is correct, set DRY_RUN = False and run again.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
