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
# META           "id": "1d620310-5acc-4534-93f9-f52f082a1887"
# META         },
# META         {
# META           "id": "3ea0efcd-03d5-44f1-8e70-99f52a5c2a22"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Handle ancient dates/timestamps when READING Parquet
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")

# Handle ancient dates/timestamps when WRITING Parquet
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_print_production_order_casting AS
WITH dedup AS (
    SELECT  DISTINCT *
    FROM `ENG-Bronze`.Append_Bronze_Lakehouse.dbo.`prod.bronze_print_production_order_casting`
),
ranked AS (
    SELECT
        d.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                prod_order_no,
                prod_order_line_no,
                item_no,
                sales_order_no
            ORDER BY
                prod_order_due_date DESC,
                prod_order_print DESC
        ) AS rn
    FROM dedup d
)
SELECT *
FROM ranked
WHERE rn = 1
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_print_production_order_casting AS
WITH dedup AS (
    SELECT DISTINCT *
    FROM Silver_Production_Lakehouse.prod.silver_print_production_order_casting
    WHERE prod_order_status = 'Released'
),
ranked AS (
    SELECT
        d.*,
        CONCAT(prod_order_no, item_no) AS poi,
        ROW_NUMBER() OVER (
            PARTITION BY
                prod_order_no,
                prod_order_line_no,
                item_no
            ORDER BY
                prod_order_due_date DESC
        ) AS rn_latest
    FROM dedup d
)
SELECT *
FROM ranked
WHERE rn_latest = 1
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
