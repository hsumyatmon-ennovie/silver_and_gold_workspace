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
# META           "id": "1d620310-5acc-4534-93f9-f52f082a1887"
# META         }
# META       ]
# META     },
# META     "mirrored_db": {
# META       "known_mirrored_dbs": []
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

# # Silver Wax Team

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
 

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
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Emp Team / Emp Wax Team

# MARKDOWN ********************

# ## All Silver

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
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Emp Work Time

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_emp_work_time
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH
# MAGIC T_win AS (
# MAGIC   SELECT *
# MAGIC   FROM Gold_Production_Lakehouse.prod.gold_employee_time_fixed
# MAGIC ),
# MAGIC 
# MAGIC M AS (
# MAGIC   SELECT
# MAGIC     cast(`No.` as string) AS machine_center_no,
# MAGIC     cast(`Machine Employee Mapping` as string) AS machine_employee_mapping
# MAGIC   FROM Silver_BC_Lakehouse.bc.`Machine Center`
# MAGIC ),
# MAGIC 
# MAGIC Emp_Summary AS (
# MAGIC   SELECT
# MAGIC     D.Division                              AS Division,
# MAGIC     D.Department                            AS Department,
# MAGIC     D.sub_department                        AS sub_department,
# MAGIC     D.Position                              AS Position,
# MAGIC     D.Employee_Code                         AS Employee_Code,
# MAGIC     D.First_Name_Eng                        AS First_Name_Eng,
# MAGIC     D.`First Name Thai`                     AS First_Name_Thai,
# MAGIC     R.antenna_id                            AS antenna_id,
# MAGIC     R.machine_center_no                     AS machine_center_no,
# MAGIC     W.team                                  AS team,
# MAGIC     T.Work_Day                              AS Work_Day,
# MAGIC     M.machine_employee_mapping              AS machine_map,
# MAGIC 
# MAGIC     SUM(DISTINCT late_time_in_minutes)                                            AS late_time_in_minutes,
# MAGIC     SUM(DISTINCT before_time_out_minutes)                                         AS before_time_out_minutes,
# MAGIC     SUM(DISTINCT CAST(coalesce(T.Normal_Work_Minutes, 0) AS DOUBLE))              AS Normal_Work_Minutes,
# MAGIC     SUM(DISTINCT CAST(coalesce(T.OT_Work_Minutes, 0) AS DOUBLE))                  AS OT_Work_Minutes,
# MAGIC     SUM(DISTINCT CAST(coalesce(T.Total_Work_Minutes, 0) AS DOUBLE))               AS Total_Work_Minutes,
# MAGIC     SUM(DISTINCT CAST(coalesce(T.Absent_Minutes, 0) AS DOUBLE))                   AS Absent_Minutes
# MAGIC 
# MAGIC   FROM Silver_Commons_Lakehouse.cmn.silver_employee_data_full_replace D
# MAGIC   INNER JOIN Silver_Commons_Lakehouse.cmn.silver_employee_rfid_mapping R
# MAGIC     ON D.Employee_Code = R.Employee_Code
# MAGIC   INNER JOIN T_win T
# MAGIC     ON D.Employee_Code = T.Employee_Code
# MAGIC   INNER JOIN Gold_Production_Lakehouse.prod.gold_emp_team_full W
# MAGIC     ON D.Employee_Code = W.Employee_Code
# MAGIC   LEFT JOIN M
# MAGIC     ON cast(R.machine_center_no as string) = M.machine_center_no
# MAGIC 
# MAGIC   GROUP BY
# MAGIC     D.Division,
# MAGIC     D.Department,
# MAGIC     D.sub_department,
# MAGIC     D.Position,
# MAGIC     D.Employee_Code,
# MAGIC     D.First_Name_Eng,
# MAGIC     D.`First Name Thai`,
# MAGIC     R.antenna_id,
# MAGIC     R.machine_center_no,
# MAGIC     W.team,
# MAGIC     T.Work_Day,
# MAGIC     M.machine_employee_mapping
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC   Division,
# MAGIC   Department,
# MAGIC   sub_department,
# MAGIC   Position,
# MAGIC   Employee_Code,
# MAGIC   First_Name_Eng,
# MAGIC   First_Name_Thai,
# MAGIC   antenna_id,
# MAGIC   machine_center_no,
# MAGIC   team,
# MAGIC   Work_Day,
# MAGIC   machine_map,
# MAGIC 
# MAGIC   SUM(late_time_in_minutes)         AS late_time_in_minutes,
# MAGIC   SUM(before_time_out_minutes)      AS before_time_out_minutes,
# MAGIC   SUM(Normal_Work_Minutes)          AS Normal_Work_Minutes,
# MAGIC   SUM(OT_Work_Minutes)              AS OT_Work_Minutes,
# MAGIC   SUM(Total_Work_Minutes)           AS Total_Work_Minutes,
# MAGIC   SUM(Absent_Minutes)               AS Absent_Minutes,
# MAGIC 
# MAGIC   COUNT(DISTINCT Employee_Code)     AS Total_emp
# MAGIC FROM Emp_Summary
# MAGIC GROUP BY
# MAGIC   Division,
# MAGIC   Department,
# MAGIC   sub_department,
# MAGIC   Position,
# MAGIC   Employee_Code,
# MAGIC   First_Name_Eng,
# MAGIC   First_Name_Thai,
# MAGIC   antenna_id,
# MAGIC   machine_center_no,
# MAGIC   team,
# MAGIC   Work_Day,
# MAGIC   machine_map
# MAGIC ;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold From Emp Time

# MARKDOWN ********************

# ## Silver + Gold Employee Time Group

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_from_emp_time
# MAGIC AS
# MAGIC WITH base_raw AS (
# MAGIC     SELECT
# MAGIC         CAST(created_on AS DATE) AS created_date,
# MAGIC         created_on,
# MAGIC         modified_on,
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no,
# MAGIC         prod_order_status,
# MAGIC         type_name,
# MAGIC         operation_no,
# MAGIC         user_id,
# MAGIC         machine_center_no,
# MAGIC         employee_no,
# MAGIC         antenna_id,
# MAGIC         item_no,
# MAGIC         quantity,
# MAGIC         remaining_quantity,
# MAGIC         sales_order_no
# MAGIC     FROM `Silver_Production_Lakehouse`.`prod`.`silver_prod_order_status`
# MAGIC     WHERE type_name = 'From employee'
# MAGIC ),
# MAGIC 
# MAGIC /* Keep ONLY latest modified_on row per natural key */
# MAGIC base AS (
# MAGIC     SELECT
# MAGIC         created_date,
# MAGIC         created_on,
# MAGIC         modified_on,
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no,
# MAGIC         prod_order_status,
# MAGIC         type_name,
# MAGIC         operation_no,
# MAGIC         user_id,
# MAGIC         machine_center_no,
# MAGIC         employee_no,
# MAGIC         antenna_id,
# MAGIC         item_no,
# MAGIC         quantity,
# MAGIC         remaining_quantity,
# MAGIC         sales_order_no
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY
# MAGIC                     created_date,
# MAGIC                     prod_order_no,
# MAGIC                     prod_order_line_no,
# MAGIC                     operation_no,
# MAGIC                     user_id,
# MAGIC                     machine_center_no,
# MAGIC                     employee_no,
# MAGIC                     antenna_id
# MAGIC                 ORDER BY
# MAGIC                     CASE WHEN modified_on IS NULL THEN 0 ELSE 1 END DESC,
# MAGIC                     modified_on DESC,
# MAGIC                     created_on DESC
# MAGIC             ) AS rn
# MAGIC         FROM base_raw
# MAGIC     ) x
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC R AS (
# MAGIC     SELECT
# MAGIC         `Prod. Order No.`       AS prod_order_no,
# MAGIC         `Routing Reference No.` AS prod_order_line_no,
# MAGIC         `Operation No.`         AS operation_no,
# MAGIC         CAST(`Run Time` AS DOUBLE) AS run_time
# MAGIC     FROM `Silver_BC_Lakehouse`.`bc`.`Prod Order Routing Line`
# MAGIC ),
# MAGIC 
# MAGIC M AS (
# MAGIC     SELECT
# MAGIC         `No.` AS machine_center_no,
# MAGIC         `Machine Employee Mapping` AS machine_employee_mapping
# MAGIC     FROM `Silver_BC_Lakehouse`.`bc`.`Machine Center`
# MAGIC ),
# MAGIC 
# MAGIC /* mapping from RFID using CELL mailbox rule */
# MAGIC emp_map AS (
# MAGIC     SELECT DISTINCT
# MAGIC         CAST(Work_Day AS DATE) AS work_day,
# MAGIC         UPPER(TRIM(CAST(sub_department AS STRING))) AS sub_department,
# MAGIC         REPLACE(UPPER(TRIM(CAST(sub_department AS STRING))), ' ', '') AS cell_line,
# MAGIC         CONCAT(LOWER(REPLACE(UPPER(TRIM(CAST(sub_department AS STRING))), ' ', '')), '@ennovie.com') AS join_email,
# MAGIC         CASE WHEN prod_line   IS NULL THEN NULL ELSE TRIM(CAST(prod_line   AS STRING)) END AS prod_line,
# MAGIC         CASE WHEN antenna_id  IS NULL THEN NULL ELSE TRIM(CAST(antenna_id  AS STRING)) END AS antenna_id
# MAGIC     FROM `Gold_Production_Lakehouse`.`prod`.`gold_employee_time_group_rfid`
# MAGIC     WHERE sub_department IS NOT NULL
# MAGIC       AND UPPER(TRIM(CAST(sub_department AS STRING))) LIKE 'CELL %'
# MAGIC ),
# MAGIC 
# MAGIC /* Employee time rows (normalized keys) */
# MAGIC E AS (
# MAGIC     SELECT
# MAGIC         CAST(Work_Day AS DATE) AS e_work_day,
# MAGIC         UPPER(TRIM(CAST(sub_department AS STRING))) AS e_sub_department,
# MAGIC         CASE WHEN antenna_id IS NULL THEN NULL ELSE TRIM(CAST(antenna_id AS STRING)) END AS e_antenna_id,
# MAGIC         CAST(total_workhour AS DOUBLE) AS total_workhour,
# MAGIC         TRIM(CAST(employee_code AS STRING)) AS employee_code,
# MAGIC         TRIM(CAST(first_name_thai AS STRING)) AS first_name_thai
# MAGIC     FROM `Gold_Production_Lakehouse`.`prod`.`gold_employee_time_group_rfid`
# MAGIC )
# MAGIC 
# MAGIC SELECT DISTINCT
# MAGIC     S.created_date,
# MAGIC     S.created_on,
# MAGIC     S.modified_on,
# MAGIC     S.prod_order_no,
# MAGIC     S.prod_order_line_no,
# MAGIC     S.operation_no,
# MAGIC     R.run_time,
# MAGIC 
# MAGIC     S.user_id,
# MAGIC 
# MAGIC     EM.prod_line,
# MAGIC     EM.cell_line,
# MAGIC     EM.sub_department,
# MAGIC 
# MAGIC     S.machine_center_no,
# MAGIC     M.machine_employee_mapping AS m_group,
# MAGIC 
# MAGIC     S.employee_no,
# MAGIC     S.antenna_id,
# MAGIC     S.item_no,
# MAGIC     S.sales_order_no,
# MAGIC 
# MAGIC     ABS(S.quantity) AS out_qty,
# MAGIC     CAST(ABS(S.quantity) AS DOUBLE) * COALESCE(CAST(R.run_time AS DOUBLE), 0.0) AS total_runtime_qty,
# MAGIC 
# MAGIC     E.total_workhour,
# MAGIC     E.employee_code,
# MAGIC     E.first_name_thai,
# MAGIC 
# MAGIC     GREATEST(S.created_on, S.modified_on) AS change_ts
# MAGIC 
# MAGIC FROM base AS S
# MAGIC 
# MAGIC LEFT JOIN R
# MAGIC     ON  S.prod_order_no      = R.prod_order_no
# MAGIC     AND S.prod_order_line_no = R.prod_order_line_no
# MAGIC     AND S.operation_no       = R.operation_no
# MAGIC 
# MAGIC LEFT JOIN emp_map EM
# MAGIC     ON  S.created_date = EM.work_day
# MAGIC     AND LOWER(TRIM(CAST(S.user_id AS STRING))) = EM.join_email
# MAGIC 
# MAGIC LEFT JOIN M
# MAGIC     ON S.machine_center_no = M.machine_center_no
# MAGIC 
# MAGIC LEFT JOIN E
# MAGIC     ON  S.created_date    = E.e_work_day
# MAGIC     AND EM.sub_department = E.e_sub_department
# MAGIC     AND (
# MAGIC          TRIM(CAST(S.antenna_id AS STRING)) = E.e_antenna_id
# MAGIC          OR (S.antenna_id IS NULL AND E.e_antenna_id IS NULL)
# MAGIC     );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # From Emp Time Summary

# MARKDOWN ********************

# ## Silver + Gold Employee Time Group

# CELL ********************

# MAGIC %%sql
# MAGIC -- ==========================================================
# MAGIC -- FULL FIXED Spark SQL (Databricks / Delta)
# MAGIC -- Rule:
# MAGIC --   If sub_department = 'CELL 102' => join_email = 'cell102@ennovie.com'
# MAGIC -- Implementation:
# MAGIC --   join_email = lower(regexp_replace(sub_department, '\\s+', '')) || '@ennovie.com'
# MAGIC --
# MAGIC -- Also:
# MAGIC --   sub_department keeps "CELL 102" (with space)
# MAGIC --   cell_line derived as "CELL102" (no space)
# MAGIC -- ==========================================================
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_from_emp_time_summary
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH
# MAGIC base AS (
# MAGIC   SELECT
# MAGIC     CAST(created_on AS DATE) AS created_date,
# MAGIC     created_on,
# MAGIC     modified_on,
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     operation_no,
# MAGIC     user_id,
# MAGIC     machine_center_no,
# MAGIC     antenna_id,
# MAGIC     item_no,
# MAGIC     quantity,
# MAGIC     sales_order_no
# MAGIC   FROM Silver_Production_Lakehouse.prod.silver_prod_order_status
# MAGIC   WHERE type_name = 'From employee'
# MAGIC ),
# MAGIC 
# MAGIC r_slim AS (
# MAGIC   SELECT
# MAGIC     CAST(`Prod. Order No.`       AS STRING) AS r_prod_order_no,
# MAGIC     CAST(`Routing Reference No.` AS STRING) AS r_prod_order_line_no,
# MAGIC     CAST(`Operation No.`         AS STRING) AS r_operation_no,
# MAGIC     CAST(`Run Time`              AS DOUBLE) AS run_time
# MAGIC   FROM Silver_BC_Lakehouse.bc.`Prod Order Routing Line`
# MAGIC ),
# MAGIC 
# MAGIC m_slim AS (
# MAGIC   SELECT
# MAGIC     CAST(`No.` AS STRING) AS machine_center_no,
# MAGIC     CAST(`Machine Employee Mapping` AS STRING) AS machine_employee_mapping
# MAGIC   FROM Silver_BC_Lakehouse.bc.`Machine Center`
# MAGIC ),
# MAGIC 
# MAGIC -- ----------------------------------------------------------
# MAGIC -- emp_map from RFID:
# MAGIC --  sub_department = "CELL 102" (keeps space)
# MAGIC --  cell_line      = "CELL102" (remove spaces)
# MAGIC --  join_email     = "cell102@ennovie.com" (derived from sub_department)
# MAGIC -- NOTE: We do NOT depend on RFID.email_address for matching base.user_id.
# MAGIC -- ----------------------------------------------------------
# MAGIC emp_map AS (
# MAGIC   SELECT DISTINCT
# MAGIC     CAST(Work_Day AS DATE) AS created_date,
# MAGIC 
# MAGIC     CASE
# MAGIC       WHEN sub_department IS NULL THEN NULL
# MAGIC       ELSE upper(trim(CAST(sub_department AS STRING)))
# MAGIC     END AS sub_department,
# MAGIC 
# MAGIC     CASE
# MAGIC       WHEN sub_department IS NULL THEN NULL
# MAGIC       ELSE upper(regexp_replace(trim(CAST(sub_department AS STRING)), '\\s+', ''))
# MAGIC     END AS cell_line,
# MAGIC 
# MAGIC     CASE
# MAGIC       WHEN sub_department IS NULL THEN NULL
# MAGIC       ELSE concat(
# MAGIC              lower(regexp_replace(trim(CAST(sub_department AS STRING)), '\\s+', '')),
# MAGIC              '@ennovie.com'
# MAGIC            )
# MAGIC     END AS join_email,
# MAGIC 
# MAGIC     CASE WHEN prod_line IS NULL THEN NULL ELSE trim(CAST(prod_line AS STRING)) END AS prod_line,
# MAGIC     CASE WHEN antenna_id IS NULL THEN NULL ELSE trim(CAST(antenna_id AS STRING)) END AS antenna_id
# MAGIC   FROM Gold_Production_Lakehouse.prod.gold_employee_time_group_rfid
# MAGIC   WHERE sub_department IS NOT NULL
# MAGIC     AND upper(trim(CAST(sub_department AS STRING))) LIKE 'CELL %'
# MAGIC ),
# MAGIC 
# MAGIC joined AS (
# MAGIC   SELECT
# MAGIC     b.created_date,
# MAGIC     b.created_on,
# MAGIC     b.modified_on,
# MAGIC     b.prod_order_no,
# MAGIC     b.prod_order_line_no,
# MAGIC     b.operation_no,
# MAGIC     b.user_id,
# MAGIC     b.machine_center_no,
# MAGIC     b.antenna_id,
# MAGIC     b.item_no,
# MAGIC     b.sales_order_no,
# MAGIC 
# MAGIC     em.prod_line,
# MAGIC     em.cell_line,
# MAGIC     em.sub_department,
# MAGIC 
# MAGIC     CASE
# MAGIC       WHEN m.machine_employee_mapping IS NULL THEN NULL
# MAGIC       ELSE trim(CAST(m.machine_employee_mapping AS STRING))
# MAGIC     END AS m_group,
# MAGIC 
# MAGIC     r.run_time,
# MAGIC     b.quantity
# MAGIC   FROM base b
# MAGIC   LEFT JOIN r_slim r
# MAGIC     ON  b.prod_order_no      = r.r_prod_order_no
# MAGIC     AND b.prod_order_line_no = r.r_prod_order_line_no
# MAGIC     AND b.operation_no       = r.r_operation_no
# MAGIC   LEFT JOIN emp_map em
# MAGIC     ON  b.created_date = em.created_date
# MAGIC     AND lower(trim(CAST(b.user_id AS STRING))) = em.join_email
# MAGIC     -- antenna_id join OPTIONAL; often causes misses. Keep commented unless you trust it.
# MAGIC     -- AND trim(CAST(b.antenna_id AS STRING)) <=> em.antenna_id
# MAGIC   LEFT JOIN m_slim m
# MAGIC     ON b.machine_center_no = m.machine_center_no
# MAGIC ),
# MAGIC 
# MAGIC joined_norm AS (
# MAGIC   SELECT
# MAGIC       created_date,
# MAGIC       created_on,
# MAGIC       modified_on,
# MAGIC       prod_order_no,
# MAGIC       prod_order_line_no,
# MAGIC       operation_no,
# MAGIC       user_id,
# MAGIC       machine_center_no,
# MAGIC       antenna_id,
# MAGIC       item_no,
# MAGIC       sales_order_no,
# MAGIC       prod_line,
# MAGIC       cell_line,
# MAGIC       sub_department,
# MAGIC       m_group,
# MAGIC       run_time,
# MAGIC       quantity
# MAGIC   FROM (
# MAGIC       SELECT
# MAGIC           created_date,
# MAGIC           created_on,
# MAGIC           modified_on,
# MAGIC           prod_order_no,
# MAGIC           prod_order_line_no,
# MAGIC           operation_no,
# MAGIC           user_id,
# MAGIC           machine_center_no,
# MAGIC           CASE WHEN antenna_id IS NULL THEN NULL ELSE LTRIM(RTRIM(CAST(antenna_id AS VARCHAR(50)))) END AS antenna_id,
# MAGIC           item_no,
# MAGIC           sales_order_no,
# MAGIC           CASE WHEN prod_line IS NULL THEN NULL ELSE LTRIM(RTRIM(CAST(prod_line AS VARCHAR(50)))) END AS prod_line,
# MAGIC           CASE WHEN cell_line IS NULL THEN NULL ELSE LTRIM(RTRIM(CAST(cell_line AS VARCHAR(50)))) END AS cell_line,
# MAGIC           sub_department,
# MAGIC           m_group,
# MAGIC           run_time,
# MAGIC           quantity,
# MAGIC           ROW_NUMBER() OVER (
# MAGIC             PARTITION BY
# MAGIC               created_date,
# MAGIC               prod_order_no,
# MAGIC               prod_order_line_no,
# MAGIC               operation_no,
# MAGIC               user_id,
# MAGIC               machine_center_no,
# MAGIC               CASE WHEN antenna_id IS NULL THEN NULL ELSE LTRIM(RTRIM(CAST(antenna_id AS VARCHAR(50)))) END
# MAGIC             ORDER BY
# MAGIC               modified_on DESC,
# MAGIC               created_on DESC
# MAGIC           ) AS rn
# MAGIC       FROM joined
# MAGIC       -- (optional debug filters go here if you want)
# MAGIC       -- WHERE prod_order_no = 'WRO251204518'
# MAGIC       --   AND prod_order_line_no = '10000'
# MAGIC   ) x
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC 
# MAGIC -- aggregate at order-line-operation with SUM(ABS(quantity))
# MAGIC op_agg AS (
# MAGIC   SELECT
# MAGIC     created_date,
# MAGIC     prod_line,
# MAGIC     cell_line,
# MAGIC     sub_department,
# MAGIC     m_group,
# MAGIC     antenna_id,
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     operation_no,
# MAGIC 
# MAGIC     SUM(ABS(CAST(quantity AS DOUBLE)))                            AS out_qty,
# MAGIC     SUM(ABS(CAST(quantity AS DOUBLE)) * COALESCE(run_time, 0.0D)) AS total_runtime_qty,
# MAGIC     MIN(created_on)                                               AS created_on_min,
# MAGIC     MAX(modified_on)                                              AS modified_on_max
# MAGIC   FROM joined_norm
# MAGIC   WHERE prod_line IS NOT NULL
# MAGIC   GROUP BY
# MAGIC     created_date, prod_line, cell_line, sub_department, m_group, antenna_id,
# MAGIC     prod_order_no, prod_order_line_no, operation_no
# MAGIC ),
# MAGIC 
# MAGIC -- roll up all operations -> 1 row per day/line/cell/subdept/antenna/m_group
# MAGIC order_agg AS (
# MAGIC   SELECT
# MAGIC     created_date,
# MAGIC     prod_line,
# MAGIC     cell_line,
# MAGIC     sub_department,
# MAGIC     m_group,
# MAGIC     antenna_id,
# MAGIC 
# MAGIC     SUM(out_qty)           AS out_qty,
# MAGIC     SUM(total_runtime_qty) AS total_runtime_qty,
# MAGIC     MIN(created_on_min)    AS created_on_min,
# MAGIC     MAX(modified_on_max)   AS modified_on_max
# MAGIC   FROM op_agg
# MAGIC   GROUP BY
# MAGIC     created_date, prod_line, cell_line, sub_department, m_group, antenna_id
# MAGIC ),
# MAGIC 
# MAGIC -- employee hours + names (from RFID)
# MAGIC emp_distinct AS (
# MAGIC   SELECT DISTINCT
# MAGIC     CAST(Work_Day AS DATE) AS created_date,
# MAGIC 
# MAGIC     CASE
# MAGIC       WHEN sub_department IS NULL THEN NULL
# MAGIC       ELSE upper(trim(CAST(sub_department AS STRING)))
# MAGIC     END AS sub_department,
# MAGIC 
# MAGIC     CASE
# MAGIC       WHEN antenna_id IS NULL THEN NULL
# MAGIC       ELSE trim(CAST(antenna_id AS STRING))
# MAGIC     END AS antenna_id,
# MAGIC 
# MAGIC     trim(CAST(employee_code AS STRING))    AS employee_code,
# MAGIC     CAST(total_workhour AS DOUBLE)         AS total_workhour,
# MAGIC     trim(CAST(first_name_thai AS STRING))  AS first_name_thai
# MAGIC   FROM Gold_Production_Lakehouse.prod.gold_employee_time_group_rfid
# MAGIC ),
# MAGIC 
# MAGIC emp_agg AS (
# MAGIC   SELECT
# MAGIC     created_date,
# MAGIC     sub_department,
# MAGIC     antenna_id,
# MAGIC     SUM(total_workhour) AS total_workhour,
# MAGIC     concat_ws(',', sort_array(collect_set(employee_code)))   AS employee_code_list,
# MAGIC     concat_ws(',', sort_array(collect_set(first_name_thai))) AS first_name_thai_list
# MAGIC   FROM emp_distinct
# MAGIC   GROUP BY created_date, sub_department, antenna_id
# MAGIC ),
# MAGIC 
# MAGIC base_keys AS (
# MAGIC   SELECT DISTINCT
# MAGIC     created_date, prod_line, cell_line, sub_department, m_group, antenna_id
# MAGIC   FROM order_agg
# MAGIC ),
# MAGIC 
# MAGIC final_df AS (
# MAGIC   SELECT
# MAGIC     k.created_date,
# MAGIC     oa.created_on_min  AS created_on,
# MAGIC     oa.modified_on_max AS modified_on,
# MAGIC     k.prod_line,
# MAGIC     k.cell_line,
# MAGIC     k.sub_department,
# MAGIC     k.m_group,
# MAGIC     k.antenna_id,
# MAGIC 
# MAGIC     COALESCE(oa.out_qty, 0.0D)           AS out_qty,
# MAGIC     COALESCE(oa.total_runtime_qty, 0.0D) AS total_runtime_qty,
# MAGIC 
# MAGIC     ea.total_workhour,
# MAGIC     ea.employee_code_list   AS employee_code,
# MAGIC     ea.first_name_thai_list AS first_name_thai,
# MAGIC 
# MAGIC     greatest(oa.created_on_min, oa.modified_on_max) AS change_ts
# MAGIC   FROM base_keys k
# MAGIC   LEFT JOIN order_agg oa
# MAGIC     ON  k.created_date   = oa.created_date
# MAGIC     AND k.sub_department <=> oa.sub_department
# MAGIC     AND k.antenna_id     <=> oa.antenna_id
# MAGIC     AND k.prod_line      <=> oa.prod_line
# MAGIC     AND k.cell_line      <=> oa.cell_line
# MAGIC     AND k.m_group        <=> oa.m_group
# MAGIC   LEFT JOIN emp_agg ea
# MAGIC     ON  k.created_date   = ea.created_date
# MAGIC     AND k.sub_department <=> ea.sub_department
# MAGIC     AND k.antenna_id     <=> ea.antenna_id
# MAGIC ),
# MAGIC 
# MAGIC -- keep the grain (do NOT collapse cell_line/sub_department into lists)
# MAGIC merged_final AS (
# MAGIC   SELECT
# MAGIC     created_date,
# MAGIC     sub_department,
# MAGIC     antenna_id,
# MAGIC     prod_line,
# MAGIC     cell_line,
# MAGIC 
# MAGIC     SUM(out_qty)           AS out_qty,
# MAGIC     SUM(total_runtime_qty) AS total_runtime_qty,
# MAGIC 
# MAGIC     MAX(total_workhour) AS total_workhour,
# MAGIC 
# MAGIC     concat_ws(',', sort_array(collect_set(employee_code)))   AS employee_code,
# MAGIC     concat_ws(',', sort_array(collect_set(first_name_thai))) AS first_name_thai,
# MAGIC 
# MAGIC     MAX(m_group) AS m_group,
# MAGIC 
# MAGIC     MIN(created_on)  AS created_on,
# MAGIC     MAX(modified_on) AS modified_on,
# MAGIC     MAX(change_ts)   AS change_ts
# MAGIC   FROM final_df
# MAGIC   GROUP BY
# MAGIC     created_date,
# MAGIC     sub_department,
# MAGIC     antenna_id,
# MAGIC     prod_line,
# MAGIC     cell_line
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC   created_date,
# MAGIC   sub_department,
# MAGIC   antenna_id,
# MAGIC   out_qty,
# MAGIC   total_runtime_qty,
# MAGIC   total_workhour,
# MAGIC   employee_code,
# MAGIC   first_name_thai,
# MAGIC   prod_line,
# MAGIC   cell_line,
# MAGIC   m_group,
# MAGIC   created_on,
# MAGIC   modified_on,
# MAGIC   change_ts
# MAGIC FROM merged_final
# MAGIC WHERE prod_line IS NOT NULL;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Output Time

# MARKDOWN ********************

# ## Gold Output Time Compare + Gold Emp Work Time

# CELL ********************

from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from datetime import datetime, date

spark = SparkSession.builder.getOrCreate()

# =========================
# Sources & Target
# =========================
G_SRC = "Gold_Production_Lakehouse.prod.gold_output_time_compare"   # movement table (g)
E_SRC_DEFAULT = "Gold_Production_Lakehouse.prod.gold_emp_work_time"
TARGET = "Gold_Production_Lakehouse.prod.gold_output_time"          # final table to create/merge

# =========================
# Widgets / Parameters
# =========================
def get_widget(name: str, default: str) -> str:
    try:
        import dbutils  # type: ignore
        return dbutils.widgets.get(name)  # type: ignore
    except Exception:
        return default

try:
    dbutils.widgets.text("e_src", E_SRC_DEFAULT)  # type: ignore
    dbutils.widgets.text("full_reload", "false")  # type: ignore
    dbutils.widgets.text("since", "")             # yyyy-MM-dd  # type: ignore
    dbutils.widgets.text("until", "")             # yyyy-MM-dd  # type: ignore
except Exception:
    pass

E_SRC = get_widget("e_src", E_SRC_DEFAULT).strip()
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
    if "Work_Day" in df.columns:
        mx = df.agg(F.max("Work_Day").alias("mx")).collect()[0]["mx"]
        return mx
    return None

since_date = parse_date_or_none(since_param)
until_date = parse_date_or_none(until_param) or date.today()

if not full_reload and since_date is None and table_exists(TARGET):
    wm = pick_watermark_from_target(TARGET)
    if wm is not None:
        since_date = wm

if full_reload or since_date is None:
    since_date = date(1900, 1, 1)

print(f"Incremental window: {since_date} -> {until_date}")
print(f"Emp summary source (e): {E_SRC}")

# =========================
# Load Sources
# =========================
g_all = spark.table(G_SRC).alias("g")
e_all = spark.table(E_SRC).alias("e")

# created_date = date(g.created_on) like CONVERT(date, g.created_on)
g_all = g_all.withColumn("created_date", F.to_date(F.col("g.created_on")))

# =========================
# last_move CTE: latest per partition ordered by change_ts desc
# =========================
w = Window.partitionBy(
    "g.employee_no",
    "g.CorrectCurrentLocation",
    "g.open",
    "g.item_no",
    "g.prod_order_no",
    "g.prod_order_line_no",
    "g.type_name",
).orderBy(F.col("g.change_ts").desc())

g_ranked = g_all.withColumn("rn", F.row_number().over(w))
last_move = g_ranked.filter(F.col("rn") == 1).alias("g")

# =========================
# Apply Work_Day window using the employee summary (e)
# =========================
e_win = e_all.filter(
    (F.col("e.Work_Day") >= F.lit(since_date)) &
    (F.col("e.Work_Day") <= F.lit(until_date))
)

# Handle Thai name column (space vs underscore) for output
thai_col = None
if "First_Name_Thai" in e_win.columns:
    thai_col = "First_Name_Thai"
elif "First Name Thai" in e_win.columns:
    thai_col = "First Name Thai"

# =========================
# Join per your SQL
# =========================
cond = (
    (F.col("g.employee_no") == F.col("e.Employee_Code")) &
    (F.col("g.created_date") == F.col("e.Work_Day")) &
    (
        (F.col("g.past_location_code") == F.col("e.sub_department")) |
        (F.col("g.antenna_id") == F.col("e.antenna_id"))
    )
)

joined = last_move.join(e_win.alias("e"), cond, "inner")

# =========================
# Final SELECT (aliases + Thai name)
# =========================
cols = [
    F.col("e.sub_department").alias("sub_department"),
    F.col("e.Employee_Code").alias("Employee_Code"),
    F.col("e.Total_emp").alias("Total_emp"),
    F.col("e.Absent_Minutes").alias("Total_Leave_Days"),
    F.col("e.Absent_Minutes").alias("Total_Leave_Hours"),
    F.col("e.late_time_in_minutes").alias("late_time_in_minutes"),
    F.col("e.before_time_out_minutes").alias("before_time_out_minutes"),
    F.col("e.Total_Work_Minutes").alias("Count_Day_Include"),
    F.col("e.Absent_Minutes").alias("Count_Day_Absent"),
]
if thai_col:
    cols.append(F.col(f"e.`{thai_col}`").alias("First_Name_Thai"))
cols += [
    F.col("e.antenna_id").alias("emp_antenna_id"),
    F.col("e.machine_center_no").alias("emp_machine_center_no"),
    F.col("e.team").alias("team"),
    F.col("e.Work_Day").alias("Work_Day"),
    F.col("e.OT_Work_Minutes").alias("OT"),

    F.col("g.current_location_code").alias("current_location_code"),
    F.col("g.past_location_code").alias("past_location_code"),
    F.col("g.machine_center_no").alias("move_machine_center_no"),
    F.col("g.employee_no").alias("employee_no"),
    F.col("g.CorrectCurrentLocation").alias("CorrectCurrentLocation"),
    F.col("g.open").alias("open"),
    F.col("g.operation_no").alias("operation_no"),
    F.col("g.item_no").alias("item_no"),
    F.col("g.quantity").alias("quantity"),
    F.col("g.remaining_quantity").alias("remaining_quantity"),
    F.col("g.sales_order_no").alias("sales_order_no"),
    F.col("g.prod_line").alias("prod_line"),
    F.col("g.cell_line").alias("cell_line"),
    F.col("g.prod_order_status").alias("prod_order_status"),
    F.col("g.prod_order_no").alias("prod_order_no"),
    F.col("g.prod_order_line_no").alias("prod_order_line_no"),
    F.col("g.type_name").alias("type_name"),
    F.col("g.standard_time").alias("standard_time"),
    F.col("g.standard_time_total").alias("standard_time_total"),
    F.col("g.diff_hours").alias("diff_hours"),
    F.col("g.diff_minutes").alias("diff_minutes"),
    F.col("g.antenna_id").alias("move_antenna_id"),
    F.col("g.rfid_transaction_name").alias("rfid_transaction_name"),
    F.col("g.user_id").alias("user_id"),
    F.col("g.created_on").alias("created_on"),
    F.col("g.modified_on").alias("modified_on"),
    F.col("g.created_date").alias("created_date"),
    F.col("g.rn").alias("rn"),
]

final_df = joined.select(*cols)

# =========================
# Merge key (stable hash over day + emp + move/order keys)
# =========================
final_df = final_df.withColumn(
    "row_id",
    F.md5(
        F.concat_ws(
            "||",
            F.col("Work_Day").cast("string"),
            F.col("Employee_Code"),
            F.coalesce(F.col("prod_order_no"), F.lit("")),
            F.coalesce(F.col("prod_order_line_no").cast("string"), F.lit("")),
            F.coalesce(F.col("operation_no").cast("string"), F.lit("")),
            F.coalesce(F.col("item_no"), F.lit("")),
            F.coalesce(F.col("move_antenna_id").cast("string"), F.lit("")),
            F.coalesce(F.col("CorrectCurrentLocation"), F.lit("")),
            F.coalesce(F.col("open").cast("string"), F.lit("")),
        )
    )
)

# ========= 🔧 NEW: ensure 1 source row per row_id before MERGE =========
dedup_w = Window.partitionBy("row_id").orderBy(
    F.col("modified_on").desc_nulls_last(),
    F.col("created_on").desc_nulls_last()
)

final_df = (
    final_df
    .withColumn("src_rn", F.row_number().over(dedup_w))
    .filter(F.col("src_rn") == 1)
    .drop("src_rn")
)

# =========================
# Create or MERGE incrementally
# =========================
if not table_exists(TARGET):
    print(f"Target {TARGET} not found. Creating...")
    (
        final_df
        .repartition(F.col("Work_Day"))
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET)
    )
else:
    print(f"Merging incremental data into {TARGET}...")
    tgt = DeltaTable.forName(spark, TARGET)
    cond = "src.row_id <=> tgt.row_id"
    (
        tgt.alias("tgt")
        .merge(final_df.alias("src"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

print("Incremental load complete for Gold_Production_Lakehouse.prod.gold_output_time")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Plating Status

# MARKDOWN ********************

# ## Silver + Gold

# CELL ********************

# Databricks / PySpark 3.4+
from pyspark.sql import functions as F, Window as W
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException  # safer Delta handling

# ============================================================
# Config
# ============================================================
CAT_GOLD     = "Gold_Production_Lakehouse.prod"
CAT_SILV     = "Silver_Production_Lakehouse.prod"
CAT_CMN      = "Silver_Commons_Lakehouse.cmn"

SRC_PROD_ORDER      = f"{CAT_GOLD}.gold_production_order"
SRC_STATUS_GOLD     = f"{CAT_GOLD}.gold_production_status"
SRC_STATUS_SILVER   = f"{CAT_SILV}.silver_prod_order_status"    # use created_on from HERE
SRC_ROUTING_LINE    = f"Silver_BC_Lakehouse.bc.`Prod Order Routing Line`"
SRC_STEP_MAP        = f"{CAT_CMN}.silver_prod_step_casting_production"

# ------------------------------------------------------------
# CHANGE: use BC mirror Production Order instead of silver header
# ------------------------------------------------------------
SRC_PROD_HDR        = f"Silver_BC_Lakehouse.bc.`Production Order`"

# sales & item sources
SRC_SALES_ORDER     = f"{CAT_GOLD}.gold_sales_order"
SRC_ITEM            = f"Silver_BC_Lakehouse.bc.`Item`"          # <<< CHANGE: use BC mirror Item

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

def ensure_col(df, colname, expr):
    """If colname missing in df, add it as expr."""
    return df if colname in df.columns else df.withColumn(colname, expr)

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
    wm_ts = spark.range(1).select(
        (F.current_timestamp() - F.expr(f"INTERVAL {DEFAULT_LOOKBACK_DAYS} DAYS"))
        .alias("last_processed_ts")
    ).head()[0]
    (spark.createDataFrame([(wm_ts,)], ["last_processed_ts"])
         .write.mode("overwrite").format("delta").saveAsTable(WATERMARK_TABLE))

# ============================================================
# Load sources
# ============================================================
prod_order = spark.table(SRC_PROD_ORDER).select(
    "prod_order_no", "prod_order_line_no", "prod_item_line",
    "prod_line_quantity", "prod_order_status", "prod_line_start_date",
    "sales_order_no",
    "prod_line_due_date",     # final output
    "description",            # final output
    "FG_item_no"
)

# GOLD status (structure for matching ops & locations)
status_gold_raw = spark.table(SRC_STATUS_GOLD).select(
    "prod_order_no", "prod_order_line_no", "operation_no",
    F.col("CorrectCurrentLocation").alias("StatusRoutingNo"),
    F.col("created_on").alias("created_on_gold")
)
status_gold = status_gold_raw.withColumn(
    "created_on_gold_local",
    F.from_utc_timestamp("created_on_gold", SESSION_TZ)
    if IS_GOLD_CREATED_ON_UTC else F.col("created_on_gold")
)

# SILVER status (we only trust/use created_on here)
# IMPORTANT FIX:
# - Ensure type_name/open exist (some environments don't have them)
# - Fix boolean expression precedence by parenthesizing each predicate
status_silver_raw0 = spark.table(SRC_STATUS_SILVER)

status_silver_raw0 = ensure_col(status_silver_raw0, "type_name", F.lit(None).cast("string"))
status_silver_raw0 = ensure_col(status_silver_raw0, "open",      F.lit(None).cast("string"))
status_silver_raw0 = ensure_col(status_silver_raw0, "machine_center_no",      F.lit(None).cast("string"))
status_silver_raw0 = ensure_col(status_silver_raw0, "current_location_code",  F.lit(None).cast("string"))

status_silver_raw = status_silver_raw0.select(
    "prod_order_no", "prod_order_line_no", "operation_no",
    F.col("created_on").alias("created_on_silver"),
    F.col("machine_center_no").alias("machine_center_no"),
    F.col("current_location_code").alias("current_location_no"),
    F.col("type_name").alias("type_name"),
    F.col("open").alias("open")
)

status_silver = status_silver_raw.withColumn(
    "created_on_silver_local",
    F.from_utc_timestamp("created_on_silver", SESSION_TZ)
    if IS_SILVER_CREATED_ON_UTC else F.col("created_on_silver")
)

# ---------- Routing line: support BOTH Silver table and BC mirror ----------
routing_src_df = spark.table(SRC_ROUTING_LINE)

if "Prod. Order No." in routing_src_df.columns:
    # BC mirror: [Prod Order Routing Line]
    routing_line = (
        routing_src_df.selectExpr(
            "`Prod. Order No.`       as prod_order_no",
            "`Routing Reference No.` as prod_order_line_no",
            "`Routing No.`           as item_no",
            "`Operation No.`         as operation_no",
            "`No.`                   as routing_no",
            "`Type`                  as operation_type"
        )
    )
else:
    # Old Silver table schema
    routing_line = routing_src_df.select(
        "prod_order_no",
        "prod_order_line_no",
        "item_no",
        "operation_no",
        "routing_no",
        "operation_type"
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
    (spark.range(1)
        .select(F.current_timestamp().alias("last_processed_ts"))
        .write.mode("overwrite").format("delta").saveAsTable(WATERMARK_TABLE)
    )

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
    base_silver_pred = (
        (F.col("operation_no").isNotNull()) &
        (F.length(F.trim(F.col("operation_no"))) > 0)
    )

    silver_extra_pred = (
        (F.col("type_name") == F.lit("In Location In")) &
        (F.col("open") == F.lit("Yes"))
    )

    silver_first = (status_silver_chg
        .where(base_silver_pred & (silver_extra_pred if ("type_name" in status_silver_chg.columns and "open" in status_silver_chg.columns) else F.lit(True)))
        .groupBy("prod_order_no","prod_order_line_no")
        .agg(F.min("created_on_silver_local").alias("StartTSActual_silver"))
    )

    gold_first = (status_gold_chg
        .where(
            (F.col("operation_no").isNotNull()) &
            (F.length(F.trim(F.col("operation_no"))) > 0)
        )
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

    # carry prod_line_due_date + description forward
    line = (line0.where(F.col("rn")==1)
        .join(start_from_status, ["prod_order_no","prod_order_line_no"], "left")
        .select(
            "prod_order_no","prod_order_line_no","prod_item_line","prod_line_quantity",
            "StartDatePlan","StartTSActual","StartDateActual",
            "prod_line_due_date",
            "description",
            "FG_item_no"
        )
    )

    # ========================================================
    # NEW: Actual Plating Start Date from SILVER status
    # ========================================================
    plating_start = (
        status_silver_chg
            .where(
                (uptrim("machine_center_no") == F.lit("PLATING")) &
                (uptrim("current_location_no") == F.lit("PLATING ROOM"))
            )
            .groupBy("prod_order_no", "prod_order_line_no")
            .agg(F.min("created_on_silver_local").alias("ActualPlatingStartTS"))
            .withColumn("ActualPlatingStartDate", F.to_date("ActualPlatingStartTS"))
    )

    # ========================================================
    # 1) Routing → normalize/map
    # ========================================================
    mc_name = step_map_norm.select(
        uptrim("CurrentOperationRaw").alias("mc_key"),
        F.col("CurrentOperationRaw").alias("mc_name_raw")
    ).distinct()

    routing_raw = (routing_chg
        .join(line.select("prod_order_no","prod_order_line_no").distinct(),
              ["prod_order_no","prod_order_line_no"])
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
        .join(map_fallback,
              rn_exact.OpNameU.isNotNull() & map_fallback.d.contains(rn_exact.OpNameU),
              "left")
        .select(
            rn_exact["*"],
            map_fallback["OpGroup"].alias("MappedGroup_fb"),
            map_fallback["OpAbb"].alias("MappedAbbrev_fb")
        )
    )

    routing_map = (rn_fb
        .withColumn("MappedGroup", F.coalesce("MappedGroup_exact","MappedGroup_fb"))
        .withColumn("MappedAbbrev", F.coalesce("MappedAbbrev_exact","MappedAbbrev_fb"))
        .select(
            "prod_order_no","prod_order_line_no","item_no",
            "SeqNo","RoutingCode","OpName","OpNameU","MappedGroup","MappedAbbrev"
        )
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
                F.coalesce(F.element_at(duration_map, uptrim("MappedAbbrev")), F.lit(0.0)),
                2
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
            F.least(F.col("CumuDaysToThisStep_raw").cast("double"),
                    F.lit(CUMU_TO_PLATING_MAX_DAYS))
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
                W.partitionBy("prod_order_no","prod_order_line_no","item_no")
                 .orderBy("SeqNo")
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
        .join(
            plating_target
                .select("prod_order_no","prod_order_line_no","item_no")
                .withColumnRenamed("item_no","prod_item_line"),
            ["prod_order_no","prod_order_line_no","prod_item_line"],
            "inner"
        )
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
        .join(latest_ts,
              ["prod_order_no","prod_order_line_no","operation_no"],
              "left")
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
              (F.col("S_OpAbb").isNull()) &
              status_map_fb.d.contains(F.col("StatusRoutingNoU")),
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
                 .orderBy(F.col("MatchRank").desc(),
                          F.col("LastSeenAt").desc(),
                          F.col("SeqNo").desc())
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

    uncovered_items = items_scope.join(
        covered_items,
        ["prod_order_no","prod_order_line_no","item_no"],
        "left_anti"
    )

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
            W.partitionBy("prod_order_no","prod_order_line_no","item_no")
             .orderBy("SeqNo")
        ))
        .withColumn("NextOpName", F.lead("OpName").over(
            W.partitionBy("prod_order_no","prod_order_line_no","item_no")
             .orderBy("SeqNo")
        ))
        .withColumn("NextGroup", F.lead("MappedGroup").over(
            W.partitionBy("prod_order_no","prod_order_line_no","item_no")
             .orderBy("SeqNo")
        ))
        .withColumn("NextAbbrev", F.lead("MappedAbbrev").over(
            W.partitionBy("prod_order_no","prod_order_line_no","item_no")
             .orderBy("SeqNo")
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
        .join(current_step_all.alias("cp"),
              ["prod_order_no","prod_order_line_no","item_no"], "right")
        .join(first_step_details.alias("fsd"),
              ["prod_order_no","prod_order_line_no","item_no"], "left")
        .select(
            "cp.prod_order_no","cp.prod_order_line_no","cp.item_no",
            F.when(F.col("cp.SeqNo")==F.lit(-1.0),
                   F.col("fsd.NextSeqNo_min")).otherwise(F.col("nf.NextSeqNo")).alias("NextSeqNo"),
            F.when(F.col("cp.SeqNo")==F.lit(-1.0),
                   F.col("fsd.NextOpName_min")).otherwise(F.col("nf.NextOpName")).alias("NextOpName"),
            F.when(F.col("cp.SeqNo")==F.lit(-1.0),
                   F.col("fsd.NextGroup_min")).otherwise(F.col("nf.NextGroup")).alias("NextGroup"),
            F.when(F.col("cp.SeqNo")==F.lit(-1.0),
                   F.col("fsd.NextAbbrev_min")).otherwise(F.col("nf.NextAbbrev")).alias("NextAbbrev")
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
        .withColumn("CumuBeforeDays",
                    F.coalesce(F.col("CumuBeforeDays"), F.lit(0.0)))
    )

    # ========================================================
    # 6) Per-item result + ETA  (item_type + FG_item_no + due_date + description here)
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
                                     F.col("stp.CumuToPlatingDays")))
        )
        .withColumn(
            "PlannedPlating_FromActualStart",
            F.when(F.col("lwp.StartTSActual").isNotNull(),
                   add_days_fraction(F.col("lwp.StartTSActual"),
                                     F.col("stp.CumuToPlatingDays")))
        )

        .withColumn(
            "RemainToPlatingDays_fromNow",
            F.when(F.col("cp.SeqNo").isNotNull(),
                   F.greatest(
                       F.lit(0.0),
                       F.col("stp.CumuToPlatingDays").cast("double") -
                       F.coalesce(F.col("cbn.CumuBeforeDays").cast("double"),
                                  F.lit(0.0))
                   )
            )
        )

        .withColumn(
            "BaseTS",
            F.when(F.col("cp.SeqNo").isNotNull(),
                   F.greatest(F.col("cp.LastSeenAt"), F.current_timestamp()))
        )

        .withColumn(
            "ETA_FromCurrentStatus",
            F.when(
                (F.col("cp.SeqNo").isNotNull()) &
                (F.col("RemainToPlatingDays_fromNow").isNotNull()),
                add_days_fraction(F.col("BaseTS"),
                                  F.col("RemainToPlatingDays_fromNow"))
            )
        )

        .withColumn(
            "PlatingStatus",
            F.when(F.col("cp.MappedAbbrev").isNull(), F.lit("Not Start Production"))
             .when(uptrim("cp.MappedAbbrev").isin('NOT S','WH','FIL','HT','TUM','LAS','SET','POL','SHI'),
                   F.lit("Waiting to Plating"))
             .when(uptrim("cp.MappedAbbrev") == F.lit('PLT'), F.lit("In Plating"))
             .when(uptrim("cp.MappedAbbrev").isin('GLU','QC','QA','C INS','PCK'),
                   F.lit("Out of Plating"))
             .otherwise(F.lit("Not Start Production"))
        )

        .select(
            F.col("lwp.prod_order_no"),
            F.col("lwp.prod_order_line_no"),
            F.col("lwp.prod_line_quantity"),
            F.concat_ws("-",
                        F.col("lwp.prod_order_no"),
                        F.col("lwp.prod_order_line_no").cast("string")).alias("pol"),
            F.col("lwp.item_no"),
            F.when(F.col("lwp.item_no").isNotNull(),
                   F.upper(F.substring(F.trim(F.col("lwp.item_no")), 1, 1))
            ).alias("item_type"),
            F.col("lwp.FG_item_no").alias("FG_item_no"),

            F.col("lwp.prod_line_due_date").alias("prod_line_due_date"),
            F.col("lwp.description").alias("description"),

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
        .withColumn(
            "rn",
            F.row_number().over(
                W.partitionBy("prod_order_no","prod_order_line_no")
                 .orderBy(F.col("PlannedPlatingForRanking").asc())
            )
        )
        .where(F.col("rn")==1)
        .drop("rn","PlannedPlatingForRanking")
        .join(plating_start, ["prod_order_no","prod_order_line_no"], "left")
    )

    # ========================================================
    # 7.x) Override PlatingStatus when plating is started & current location is WH
    # ========================================================
    result_per_line = (
        result_per_line
        .withColumn(
            "PlatingStatus",
            F.when(
                (F.col("ActualPlatingStartDate").isNotNull()) &
                (uptrim("CurrentAbbrev") == F.lit("WH")),
                F.lit("Out of Plating")
            ).otherwise(F.col("PlatingStatus"))
        )
    )

    # ========================================================
    # 7.1) Enrich via BC Production Order -> GOLD sales_order,
    #      and ITEM dim using prod_item_line ↔ item_no
    # ========================================================

    # --------------------------------------------------------
    # read from BC Production Order (No. + Sales Order No.)
    # --------------------------------------------------------
    prod_hdr = (
        spark.table(SRC_PROD_HDR)
          .selectExpr(
              "`No.` as prod_order_no",
              "`Sales Order No.` as sales_order_no_hdr"
          )
          .withColumn("sales_order_no_u_hdr", uptrim(F.col("sales_order_no_hdr")))
          .drop("sales_order_no_hdr")
          .dropDuplicates(["prod_order_no"])
    )

    prod_keys = (prod_order.select(
        "prod_order_no","prod_order_line_no",
        uptrim("sales_order_no").alias("sales_order_no_u_gold"),
        uptrim("prod_item_line").alias("prod_item_line_u")
    ))

    prod_keys_joined = (prod_keys
        .join(prod_hdr, on="prod_order_no", how="left")
        .withColumn("sales_order_no_u",
                    F.coalesce(F.col("sales_order_no_u_hdr"),
                               F.col("sales_order_no_u_gold")))
        .select("prod_order_no","prod_order_line_no",
                "sales_order_no_u","prod_item_line_u")
    )

    sales_so = (spark.table(SRC_SALES_ORDER).select(
        uptrim("SalesorderNo").alias("SalesorderNo_u"),
        F.col("SalesorderNo").alias("SalesorderNo"),
        F.col("CusNo").alias("CusNo"),
        F.col("CusName").alias("CusName"),
        F.col("CusAbbr").alias("CusAbbr"),
        F.col("so_abbr").alias("SOAbbr")
    ))

    # --------------------------------------------------------
    # CHANGE: item dim from BC mirror [Item]
    # --------------------------------------------------------
    items_dim = (
        spark.table(SRC_ITEM)
          .selectExpr(
              "`No.` as item_no",
              "`Metal Category Code` as item_metal_category",
              "`Item Category Code` as item_category"
          )
          .withColumn("item_no_u", uptrim(F.col("item_no")))
          .select("item_no_u", "item_metal_category", "item_category")
    )

    prod_so = (prod_keys_joined
        .join(sales_so,
              F.col("sales_order_no_u") == F.col("SalesorderNo_u"),
              "left")
        .select(
            "prod_order_no","prod_order_line_no",
            "SalesorderNo","CusNo","CusName","CusAbbr","SOAbbr",
            "prod_item_line_u"
        )
    )

    prod_so_item = (prod_so
        .join(items_dim,
              F.col("prod_item_line_u") == F.col("item_no_u"),
              "left")
        .select(
            "prod_order_no","prod_order_line_no",
            "SalesorderNo","CusNo","CusName","CusAbbr","SOAbbr",
            "item_metal_category","item_category"
        )
        .dropDuplicates(["prod_order_no","prod_order_line_no"])
    )

    result_enriched = (result_per_line.alias("r")
        .join(prod_so_item.alias("x"),
              ["prod_order_no","prod_order_line_no"], "left")
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
    # MERGE into Delta (incremental upsert) — robust to bad path
    # ========================================================
    try:
        tgt = DeltaTable.forName(spark, TARGET_TABLE)
        (tgt.alias("t")
            .merge(result_enriched.alias("s"),
                   "t.prod_order_no = s.prod_order_no "
                   "AND t.prod_order_line_no = s.prod_order_line_no")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    except AnalysisException:
        (result_enriched.write
            .mode("overwrite")
            .format("delta")
            .saveAsTable(TARGET_TABLE)
        )

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

# # Summary Sales Order vs Production Order

# MARKDOWN ********************

# ## All Gold sales order + prod order

# CELL ********************

from pyspark.sql import functions as F

# ------------------------------------------------------------------------------
# 1) Read source tables
# ------------------------------------------------------------------------------

so = spark.table("Gold_Production_Lakehouse.prod.gold_sales_order")
po = spark.table("Gold_Production_Lakehouse.prod.gold_production_order")

# ------------------------------------------------------------------------------
# 2) Build PO_HEADER (header-level production order summary)
# ------------------------------------------------------------------------------

po_header = (
    po.groupBy(
        "prod_order_no",
        "sales_order_no",
        "sales_order_line_no",
        "FG_item_no",
        "prod_order_status",
    )
    .agg(F.max("prod_order_quantity").alias("prod_order_quantity"))
)

# ------------------------------------------------------------------------------
# 3) Build the summary dataset (updated logic)
# ------------------------------------------------------------------------------

joined = (
    so.alias("so")
    .join(
        po_header.alias("po"),
        (F.col("po.sales_order_no") == F.col("so.SalesorderNo"))
        & (F.col("po.sales_order_line_no") == F.col("so.SalesLineNo"))
        & (F.col("po.FG_item_no") == F.col("so.ItemFG")),
        how="left",
    )
    .where(F.col("so.StatusSO") == F.lit("Released"))
)

group_cols = [
    "so.SalesorderNo",
    "so.SalesLineNo",
    "so.ReqDate",
    "so.CusAbbr",
    "so.so_abbr",
    "so.so_type",
    "so.ItemFG",
    "so.item_description",
    "so.Total_QTY",
    "so.StatusSO",
]

summary_df = (
    joined.groupBy(*group_cols)
    .agg(
        # Total number of production orders tied to this SO/Line/FG
        F.count("po.prod_order_no").alias("ProdOrderCount"),

        # Total production qty (Header level)
        F.coalesce(F.sum("po.prod_order_quantity"), F.lit(0)).alias("TotalProdQty"),

        # Firm Planned status
        F.sum(F.when(F.col("po.prod_order_status") == "Firm Planned", 1).otherwise(0)).alias(
            "FirmPlanned_Count"
        ),
        F.coalesce(
            F.sum(
                F.when(
                    F.col("po.prod_order_status") == "Firm Planned",
                    F.col("po.prod_order_quantity"),
                ).otherwise(0)
            ),
            F.lit(0),
        ).alias("FirmPlanned_Qty"),

        # Released status
        F.sum(F.when(F.col("po.prod_order_status") == "Released", 1).otherwise(0)).alias(
            "Released_Count"
        ),
        F.coalesce(
            F.sum(
                F.when(
                    F.col("po.prod_order_status") == "Released",
                    F.col("po.prod_order_quantity"),
                ).otherwise(0)
            ),
            F.lit(0),
        ).alias("Released_Qty"),

        # Finished status
        F.sum(F.when(F.col("po.prod_order_status") == "Finished", 1).otherwise(0)).alias(
            "Finished_Count"
        ),
        F.coalesce(
            F.sum(
                F.when(
                    F.col("po.prod_order_status") == "Finished",
                    F.col("po.prod_order_quantity"),
                ).otherwise(0)
            ),
            F.lit(0),
        ).alias("Finished_Qty"),
    )
)

# Rename some columns to match your SQL naming + add derived cols
summary_df = (
    summary_df.withColumnRenamed("Total_QTY", "SalesOrderQty")
    .withColumnRenamed("StatusSO", "SalesOrderStatus")
    .withColumn("QtyDiff", F.col("SalesOrderQty") - F.col("TotalProdQty"))
    .withColumn(
        "SummaryStatus",
        F.when(F.col("TotalProdQty") == F.col("SalesOrderQty"), F.lit("Completed"))
         .otherwise(F.lit("Not Completed"))
    )
)

# Reorder/select columns explicitly
summary_df = summary_df.select(
    F.col("SalesorderNo"),
    F.col("SalesLineNo"),
    F.col("ReqDate"),
    F.col("CusAbbr"),
    F.col("so_abbr"),
    F.col("so_type"),
    F.col("ItemFG"),
    F.col("item_description"),
    F.col("SalesOrderQty"),
    F.col("SalesOrderStatus"),
    F.col("ProdOrderCount"),
    F.col("TotalProdQty"),
    F.col("FirmPlanned_Count"),
    F.col("FirmPlanned_Qty"),
    F.col("Released_Count"),
    F.col("Released_Qty"),
    F.col("Finished_Count"),
    F.col("Finished_Qty"),
    F.col("QtyDiff"),
    F.col("SummaryStatus"),
)

# ------------------------------------------------------------------------------
# 4) FULL REPLACE load into Delta table (always overwrite)
# ------------------------------------------------------------------------------

target_table = "Gold_Production_Lakehouse.prod.gold_summary_sales_order_vs_production_order"

(
    summary_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold From Emp Time Summary Group

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_from_emp_time_summary_group
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH
# MAGIC base AS (
# MAGIC   SELECT
# MAGIC     CAST(created_on AS DATE) AS created_date,
# MAGIC     created_on,
# MAGIC     modified_on,
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     operation_no,
# MAGIC     user_id,
# MAGIC     machine_center_no,
# MAGIC     antenna_id,
# MAGIC     item_no,
# MAGIC     quantity,
# MAGIC     sales_order_no
# MAGIC   FROM Silver_Production_Lakehouse.prod.silver_prod_order_status
# MAGIC   WHERE type_name = 'From employee'
# MAGIC ),
# MAGIC 
# MAGIC r_slim AS (
# MAGIC   SELECT
# MAGIC     CAST(`Prod. Order No.`       AS STRING) AS r_prod_order_no,
# MAGIC     CAST(`Routing Reference No.` AS STRING) AS r_prod_order_line_no,
# MAGIC     CAST(`Operation No.`         AS STRING) AS r_operation_no,
# MAGIC     CAST(`Run Time`              AS DOUBLE) AS run_time
# MAGIC   FROM Silver_BC_Lakehouse.bc.`Prod Order Routing Line`
# MAGIC ),
# MAGIC 
# MAGIC c_slim AS (
# MAGIC   SELECT
# MAGIC     email_address,
# MAGIC     prod_line,
# MAGIC     cell_line,
# MAGIC     sub_department
# MAGIC   FROM Silver_Production_Lakehouse.prod.silver_cell_list
# MAGIC   WHERE prod_line IS NOT NULL
# MAGIC ),
# MAGIC 
# MAGIC m_slim AS (
# MAGIC   SELECT
# MAGIC     CAST(`No.` AS STRING) AS machine_center_no,
# MAGIC     CAST(`Machine Employee Mapping` AS STRING) AS machine_employee_mapping
# MAGIC   FROM Silver_BC_Lakehouse.bc.`Machine Center`
# MAGIC ),
# MAGIC 
# MAGIC joined AS (
# MAGIC   SELECT
# MAGIC     b.created_date,
# MAGIC     b.created_on,
# MAGIC     b.modified_on,
# MAGIC     b.prod_order_no,
# MAGIC     b.prod_order_line_no,
# MAGIC     b.operation_no,
# MAGIC     b.user_id,
# MAGIC     b.machine_center_no,
# MAGIC     b.antenna_id,
# MAGIC     b.item_no,
# MAGIC     b.sales_order_no,
# MAGIC 
# MAGIC     c.prod_line,
# MAGIC     c.cell_line,
# MAGIC 
# MAGIC     CASE
# MAGIC       WHEN c.sub_department IS NULL THEN NULL
# MAGIC       ELSE upper(trim(CAST(c.sub_department AS STRING)))
# MAGIC     END AS sub_department,
# MAGIC 
# MAGIC     CASE
# MAGIC       WHEN m.machine_employee_mapping IS NULL THEN NULL
# MAGIC       ELSE trim(CAST(m.machine_employee_mapping AS STRING))
# MAGIC     END AS m_group,
# MAGIC 
# MAGIC     r.run_time,
# MAGIC     b.quantity
# MAGIC   FROM base b
# MAGIC   LEFT JOIN r_slim r
# MAGIC     ON  b.prod_order_no      = r.r_prod_order_no
# MAGIC     AND b.prod_order_line_no = r.r_prod_order_line_no
# MAGIC     AND b.operation_no       = r.r_operation_no
# MAGIC   LEFT JOIN c_slim c
# MAGIC     ON b.user_id = c.email_address
# MAGIC   LEFT JOIN m_slim m
# MAGIC     ON b.machine_center_no = m.machine_center_no
# MAGIC ),
# MAGIC 
# MAGIC joined_norm AS (
# MAGIC   SELECT
# MAGIC     created_date,
# MAGIC     created_on,
# MAGIC     modified_on,
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     operation_no,
# MAGIC     user_id,
# MAGIC     machine_center_no,
# MAGIC     CASE WHEN antenna_id IS NULL THEN NULL ELSE trim(CAST(antenna_id AS STRING)) END AS antenna_id,
# MAGIC     item_no,
# MAGIC     sales_order_no,
# MAGIC     CASE WHEN prod_line IS NULL THEN NULL ELSE trim(CAST(prod_line AS STRING)) END AS prod_line,
# MAGIC     CASE WHEN cell_line IS NULL THEN NULL ELSE trim(CAST(cell_line AS STRING)) END AS cell_line,
# MAGIC     sub_department,
# MAGIC     m_group,
# MAGIC     run_time,
# MAGIC     quantity
# MAGIC   FROM joined
# MAGIC ),
# MAGIC 
# MAGIC -- ✅ aggregate at order-line-operation with SUM(ABS(quantity))
# MAGIC op_agg AS (
# MAGIC   SELECT
# MAGIC     created_date,
# MAGIC     prod_line,
# MAGIC     cell_line,
# MAGIC     sub_department,
# MAGIC     m_group,
# MAGIC     antenna_id,
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     operation_no,
# MAGIC 
# MAGIC     SUM(ABS(CAST(quantity AS DOUBLE)))                                   AS out_qty,
# MAGIC     SUM(ABS(CAST(quantity AS DOUBLE)) * COALESCE(run_time, 0.0D))        AS total_runtime_qty,
# MAGIC     MIN(created_on)                                                      AS created_on_min,
# MAGIC     MAX(modified_on)                                                     AS modified_on_max
# MAGIC   FROM joined_norm
# MAGIC   WHERE prod_line IS NOT NULL
# MAGIC   GROUP BY
# MAGIC     created_date, prod_line, cell_line, sub_department, m_group, antenna_id,
# MAGIC     prod_order_no, prod_order_line_no, operation_no
# MAGIC ),
# MAGIC 
# MAGIC -- ✅ roll up all operations -> 1 row per day/line/antenna
# MAGIC order_agg AS (
# MAGIC   SELECT
# MAGIC     created_date,
# MAGIC     prod_line,
# MAGIC     cell_line,
# MAGIC     sub_department,
# MAGIC     m_group,
# MAGIC     antenna_id,
# MAGIC 
# MAGIC     SUM(out_qty)           AS out_qty,
# MAGIC     SUM(total_runtime_qty) AS total_runtime_qty,
# MAGIC     MIN(created_on_min)    AS created_on_min,
# MAGIC     MAX(modified_on_max)   AS modified_on_max
# MAGIC   FROM op_agg
# MAGIC   GROUP BY
# MAGIC     created_date, prod_line, cell_line, sub_department, m_group, antenna_id
# MAGIC ),
# MAGIC 
# MAGIC emp_distinct AS (
# MAGIC   SELECT DISTINCT
# MAGIC     CAST(Work_Day AS DATE) AS created_date,
# MAGIC     CASE
# MAGIC       WHEN sub_department IS NULL THEN NULL
# MAGIC       ELSE upper(trim(CAST(sub_department AS STRING)))
# MAGIC     END AS sub_department,
# MAGIC     CASE
# MAGIC       WHEN antenna_id IS NULL THEN NULL
# MAGIC       ELSE trim(CAST(antenna_id AS STRING))
# MAGIC     END AS antenna_id,
# MAGIC     trim(CAST(employee_code AS STRING))    AS employee_code,
# MAGIC     CAST(total_workhour AS DOUBLE)         AS total_workhour,
# MAGIC     trim(CAST(first_name_thai AS STRING))  AS first_name_thai
# MAGIC   FROM Gold_Production_Lakehouse.prod.gold_employee_time_group_rfid
# MAGIC ),
# MAGIC 
# MAGIC emp_agg AS (
# MAGIC   SELECT
# MAGIC     created_date,
# MAGIC     sub_department,
# MAGIC     antenna_id,
# MAGIC     SUM(total_workhour) AS total_workhour,
# MAGIC     concat_ws(',', sort_array(collect_set(employee_code)))   AS employee_code_list,
# MAGIC     concat_ws(',', sort_array(collect_set(first_name_thai))) AS first_name_thai_list
# MAGIC   FROM emp_distinct
# MAGIC   GROUP BY created_date, sub_department, antenna_id
# MAGIC ),
# MAGIC 
# MAGIC base_keys AS (
# MAGIC   SELECT DISTINCT
# MAGIC     created_date, prod_line, cell_line, sub_department, m_group, antenna_id
# MAGIC   FROM order_agg
# MAGIC ),
# MAGIC 
# MAGIC final_df AS (
# MAGIC   SELECT
# MAGIC     k.created_date,
# MAGIC     oa.created_on_min  AS created_on,
# MAGIC     oa.modified_on_max AS modified_on,
# MAGIC     k.prod_line,
# MAGIC     k.cell_line,
# MAGIC     k.sub_department,
# MAGIC     k.m_group,
# MAGIC     k.antenna_id,
# MAGIC 
# MAGIC     COALESCE(oa.out_qty, 0.0D)           AS out_qty,
# MAGIC     COALESCE(oa.total_runtime_qty, 0.0D) AS total_runtime_qty,
# MAGIC 
# MAGIC     ea.total_workhour,
# MAGIC     ea.employee_code_list   AS employee_code,
# MAGIC     ea.first_name_thai_list AS first_name_thai,
# MAGIC 
# MAGIC     greatest(oa.created_on_min, oa.modified_on_max) AS change_ts
# MAGIC   FROM base_keys k
# MAGIC   LEFT JOIN order_agg oa
# MAGIC     ON  k.created_date  = oa.created_date
# MAGIC     AND k.sub_department <=> oa.sub_department
# MAGIC     AND k.antenna_id     <=> oa.antenna_id
# MAGIC     AND k.prod_line      <=> oa.prod_line
# MAGIC     AND k.cell_line      <=> oa.cell_line
# MAGIC     AND k.m_group        <=> oa.m_group
# MAGIC   LEFT JOIN emp_agg ea
# MAGIC     ON  k.created_date   = ea.created_date
# MAGIC     AND k.sub_department <=> ea.sub_department
# MAGIC     AND k.antenna_id     <=> ea.antenna_id
# MAGIC ),
# MAGIC 
# MAGIC 
# MAGIC merged_final AS (
# MAGIC   SELECT
# MAGIC     created_date,
# MAGIC     sub_department,
# MAGIC     antenna_id,
# MAGIC     prod_line,
# MAGIC     cell_line,
# MAGIC 
# MAGIC     SUM(out_qty) AS out_qty,
# MAGIC     SUM(total_runtime_qty) AS total_runtime_qty,
# MAGIC 
# MAGIC     MAX(total_workhour) AS total_workhour,
# MAGIC 
# MAGIC     MAX(m_group) AS m_group,
# MAGIC 
# MAGIC     MIN(created_on) AS created_on,
# MAGIC     MAX(modified_on) AS modified_on,
# MAGIC     MAX(change_ts) AS change_ts
# MAGIC   FROM final_df
# MAGIC   GROUP BY
# MAGIC     created_date,
# MAGIC     sub_department,
# MAGIC     antenna_id,
# MAGIC     prod_line,
# MAGIC     cell_line
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC   created_date,
# MAGIC   sub_department,
# MAGIC   antenna_id,
# MAGIC   out_qty,
# MAGIC   total_runtime_qty,
# MAGIC   total_workhour,
# MAGIC   prod_line,
# MAGIC   cell_line,
# MAGIC   m_group,
# MAGIC   created_on,
# MAGIC   modified_on,
# MAGIC   change_ts
# MAGIC FROM merged_final
# MAGIC WHERE prod_line IS NOT NULL;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
