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
# META           "id": "76781d83-17f8-4270-a81d-6759d1ee9a9d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ==============================================
# MULTI-LAKEHOUSE TABLE DROP (DESTRUCTIVE!)
# ==============================================

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ---------------------------
# CONFIGURE HERE
# ---------------------------

# Lakehouses attached to this notebook (same style as your dedupe script)
LAKEHOUSES = [
    # "Silver_Production_Lakehouse",
    "Gold_Production_Lakehouse",
    "Gold_Inventory_Lakehouse",
    "Gold_Customer_Exp_Lakehouse",
    "Gold_Finance_Lakehouse",
    "Gold_Product_Dev_Lakehouse",
    # "Silver_Product_Dev_Lakehouse",
    # "Silver_Finance_Lakehouse",
    # "Silver_Customer_Exp_Lakehouse",
    # "Silver_Commons_Lakehouse",
    # "Silver_Inventory_Lakehouse",
]

# Schemas inside each lakehouse where you want to drop ALL tables
TARGET_SCHEMAS = [
    "dbo",
    "prod",
    "wm",
    "cmn",
    "cx",
    "inv",
    # add/remove as needed
]

# Safety switch: start with True to see what *would* be dropped
DRY_RUN = False

# ---------------------------
# HELPERS
# ---------------------------

def list_tables_in_namespace(lakehouse: str, schema: str):
    """Return rows from SHOW TABLES IN <lakehouse>.<schema>, or [] if not accessible."""
    try:
        return spark.sql(f"SHOW TABLES IN `{lakehouse}`.`{schema}`").collect()
    except Exception as e:
        print(f"  [drop] Cannot list tables in {lakehouse}.{schema}: {e}")
        return []


def drop_table(full_name: str):
    """Drop a table by fully qualified name."""
    try:
        spark.sql(f"DROP TABLE IF EXISTS {full_name}")
        print(f"    [drop] Dropped: {full_name}")
    except Exception as e:
        print(f"    [drop] ERROR dropping {full_name}: {e}")


# ---------------------------
# RUN
# ---------------------------

total_found = 0
total_dropped = 0

print(f"DRY_RUN = {DRY_RUN}")
print(f"Lakehouses: {LAKEHOUSES}")
print(f"Schemas   : {TARGET_SCHEMAS}")

for lakehouse in LAKEHOUSES:
    print(f"\n=== Lakehouse: {lakehouse} ===")

    for schema in TARGET_SCHEMAS:
        print(f"\n  --- Schema: {schema} ---")

        tables = list_tables_in_namespace(lakehouse, schema)

        if not tables:
            print("    [drop] No tables found or schema not accessible.")
            continue

        for t in tables:
            table_name = t.tableName
            full_name = f"`{lakehouse}`.`{schema}`.`{table_name}`"
            total_found += 1

            if DRY_RUN:
                print(f"    [dry-run] Would drop: {full_name}")
            else:
                drop_table(full_name)
                total_dropped += 1

print("\n===================================")
print(f"Total tables found : {total_found}")
print(f"Total tables dropped (if DRY_RUN=False): {total_dropped}")
print("===================================\n")

if DRY_RUN:
    print("DRY_RUN is True → nothing was actually dropped.")
    print("If the list looks correct, set DRY_RUN = False and run again.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
