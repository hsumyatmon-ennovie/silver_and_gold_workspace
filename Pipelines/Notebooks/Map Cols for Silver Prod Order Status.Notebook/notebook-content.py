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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, round as spark_round
from pyspark.sql.types import NumericType

# ---------------------------------------------------
# Helper: apply a single enum map to one column
# ---------------------------------------------------
def apply_enum_map(df: DataFrame, column: str, mapping: dict) -> DataFrame:
    """
    Maps enum values to readable text for Spark DataFrames.
    - If column does not exist, returns df unchanged.
    - Output column is always StringType to avoid type mismatches.
    """
    if column not in df.columns:
        return df
    
    expr = None
    for key, value in mapping.items():
        cond = (col(column) == key)
        if expr is None:
            expr = when(cond, lit(value))  # mapped to string literal
        else:
            expr = expr.when(cond, lit(value))
    
    # Fallback: keep original value but as string
    expr = expr.otherwise(col(column).cast("string"))
    
    return df.withColumn(column, expr)

# ---------------------------------------------------
# Helper: apply all enum maps
# ---------------------------------------------------
def apply_enum_maps(df: DataFrame) -> DataFrame:
    # For boolean columns like "open" (and likely "item_block")
    OPEN_BOOL_MAP = {
        False: "No",
        True: "Yes",
    }

    TYPE_NAME_MAP = {
        184930000: "In location in",
        184930001: "Out location",
        184930002: "To employee",
        184930003: "From employee",
    }

    PROD_ORDER_STATUS_MAP = {
        184930000: "Simulated",
        184930001: "Planned",
        184930002: "Firm Planned",
        184930003: "Released",
        184930004: "Finished",
    }

    

    out = df

    # Columns we know you have in this table
    out = apply_enum_map(out, "open", OPEN_BOOL_MAP)
    out = apply_enum_map(out, "type_name", TYPE_NAME_MAP)
    out = apply_enum_map(out, "prod_order_status", PROD_ORDER_STATUS_MAP)
    return out

# ---------------------------------------------------
# Helper: cast all numeric columns to decimal(18,2)
# ---------------------------------------------------
def cast_numeric_to_decimal(df: DataFrame, precision: int = 18, scale: int = 2) -> DataFrame:
    decimal_type = f"decimal({precision},{scale})"
    out = df
    for field in df.schema.fields:
        if isinstance(field.dataType, NumericType):
            out = out.withColumn(
                field.name,
                spark_round(col(field.name), scale).cast(decimal_type)
            )
    return out

# ---------------------------------------------------
# 1️⃣ Load from staging (SELECT *)
# ---------------------------------------------------
df_raw = spark.sql("""
    SELECT *
    FROM Silver_Production_Lakehouse.staging.silver_prod_order_status_staging
""")

# Optional: inspect schema
# df_raw.printSchema()

# ---------------------------------------------------
# 2️⃣ Apply enum mappings
# ---------------------------------------------------
df_mapped = apply_enum_maps(df_raw)

# ---------------------------------------------------
# 3️⃣ Cast all numeric columns to decimal(18,2)
# ---------------------------------------------------
df_final = df_mapped

# Optional: inspect result
# df_final.printSchema()
# display(df_final)

# ---------------------------------------------------
# 4️⃣ Full reload Silver table
# ---------------------------------------------------
(
    df_final.write
        .format("delta")
        .mode("overwrite")                 # FULL RELOAD
        .option("overwriteSchema", "true")
        .saveAsTable(
            "Silver_Production_Lakehouse.prod.silver_prod_order_status"
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
