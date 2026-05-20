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
# META           "id": "76781d83-17f8-4270-a81d-6759d1ee9a9d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Gold Delta Attendance 

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import Window

# Source tables
time_df = spark.table("Gold_Commons_Lakehouse.cmn.gold_employee_time_all")
emp_df  = spark.table("Gold_Production_Lakehouse.prod.gold_employee_data_daily")

# Last 30 calendar days from today (inclusive)
thirty_days_ago = F.date_sub(F.current_date(), 30)
today = F.current_date()

# 1) Daily present count per department (time table)
emp_daily = (
    time_df
    .filter(
        (F.col("Work_Day") >= thirty_days_ago) &
        (F.col("Work_Day") <= today) &              # ensure inclusive upper bound
        (F.col("Department_Eng").like("PROD  LINE %"))
    )
    .groupBy("Work_Day", "Department_Eng")
    .agg(F.countDistinct("Employee_Code").alias("present_count"))
)

# 2) Total employees per department (master, only active)
emp_master = (
    emp_df
    .filter(
        (F.col("end_date").isNull()) &
        (F.col("department").like("PROD  LINE  %"))
    )
    .groupBy("department")
    .agg(F.countDistinct("employee_code").alias("total_employees"))
)

# 3) Join + daily attendance rate
attendance = (
    emp_daily.alias("d")
    .join(
        emp_master.alias("m"),
        F.col("d.Department_Eng") == F.col("m.department"),
        "inner"
    )
    .select(
        F.col("d.Work_Day").alias("work_day"),
        F.col("d.Department_Eng").alias("department"),
        F.col("present_count"),
        F.col("total_employees"),
        (F.col("present_count") / F.col("total_employees")).alias("attendance_rate")  # 0–1
    )
)

# 4) 30-day average attendance per department (over filtered 30-day set)
dept_window = Window.partitionBy("department")

attendance_with_30day_avg = (
    attendance
    .withColumn(
        "avg_attendance_rate_30d",
        F.avg("attendance_rate").over(dept_window)
    )
)

# 5) Write as full-replace Delta table
(
    attendance_with_30day_avg
    .write
    .format("delta")
    .mode("overwrite")                      # full replace
    .option("overwriteSchema", "true")      # update schema if needed
    .saveAsTable("Gold_Production_Lakehouse.delta.gold_delta_attendance")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Delta Prod Line Crew

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.delta.gold_delta_prod_line_crew
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC WITH emp_daily AS (
# MAGIC     SELECT
# MAGIC         Work_Day,
# MAGIC         Position_Eng                                                         AS role,
# MAGIC         Department_Eng                                                       AS department,
# MAGIC         COUNT(DISTINCT Employee_Code)                                        AS present_count,
# MAGIC         COUNT(DISTINCT CASE WHEN late_time_in_minutes > 0
# MAGIC                             THEN Employee_Code END)                          AS late_count,
# MAGIC         COALESCE(SUM(OT_Work_Minutes), 0)                                   AS total_ot_hours
# MAGIC     FROM Gold_Commons_Lakehouse.cmn.gold_employee_time_all
# MAGIC     WHERE Work_Day          = CURRENT_DATE()
# MAGIC       AND Department_Eng LIKE 'PROD  LINE %'
# MAGIC     GROUP BY Work_Day, Position_Eng, Department_Eng
# MAGIC ),
# MAGIC 
# MAGIC emp_master AS (
# MAGIC     SELECT
# MAGIC         department,
# MAGIC         position                                                             AS role,
# MAGIC         COUNT(DISTINCT employee_code)                                        AS total_employees
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_employee_data_daily
# MAGIC     WHERE end_date   IS NULL
# MAGIC       AND department LIKE 'PROD  LINE %'
# MAGIC     GROUP BY department, position
# MAGIC ),
# MAGIC 
# MAGIC who_late AS (
# MAGIC     SELECT
# MAGIC         Work_Day,
# MAGIC         Position_Eng                                                         AS role,
# MAGIC         Department_Eng                                                       AS department,
# MAGIC         CONCAT_WS(', ',
# MAGIC             SORT_ARRAY(COLLECT_SET(
# MAGIC                 CONCAT(First_Name_Eng, ' (', Employee_Code, ')')
# MAGIC             ))
# MAGIC         )                                                                    AS who_late
# MAGIC     FROM Gold_Commons_Lakehouse.cmn.gold_employee_time_all
# MAGIC     WHERE Work_Day             = CURRENT_DATE()
# MAGIC       AND Department_Eng    LIKE 'PROD  LINE %'
# MAGIC       AND COALESCE(late_time_in_minutes, 0) > 0
# MAGIC     GROUP BY Work_Day, Position_Eng, Department_Eng
# MAGIC ),
# MAGIC 
# MAGIC present_emp AS (
# MAGIC     SELECT DISTINCT
# MAGIC         Work_Day,
# MAGIC         Department_Eng   AS department,
# MAGIC         Position_Eng     AS role,
# MAGIC         Employee_Code    AS employee_code
# MAGIC     FROM Gold_Commons_Lakehouse.cmn.gold_employee_time_all
# MAGIC     WHERE Work_Day       = CURRENT_DATE()
# MAGIC       AND Department_Eng LIKE 'PROD  LINE %'
# MAGIC ),
# MAGIC 
# MAGIC active_emp AS (
# MAGIC     SELECT
# MAGIC         department,
# MAGIC         position         AS role,
# MAGIC         employee_code,
# MAGIC         First_Name_Eng
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_employee_data_daily
# MAGIC     WHERE end_date   IS NULL
# MAGIC       AND department LIKE 'PROD  LINE %'
# MAGIC ),
# MAGIC 
# MAGIC who_absent AS (
# MAGIC     SELECT
# MAGIC         a.department,
# MAGIC         a.role,
# MAGIC         CONCAT_WS(', ',
# MAGIC             SORT_ARRAY(COLLECT_SET(
# MAGIC                 CONCAT(a.First_Name_Eng, ' (', a.employee_code, ')')
# MAGIC             ))
# MAGIC         )                                                                    AS who_absent
# MAGIC     FROM active_emp a
# MAGIC     WHERE NOT EXISTS (
# MAGIC         SELECT 1
# MAGIC         FROM present_emp p
# MAGIC         WHERE p.department    = a.department
# MAGIC           AND p.role          = a.role
# MAGIC           AND p.employee_code = a.employee_code
# MAGIC     )
# MAGIC     GROUP BY a.department, a.role
# MAGIC ),
# MAGIC 
# MAGIC daily_master AS (
# MAGIC     SELECT
# MAGIC         d.Work_Day                                                          AS work_day,
# MAGIC         d.role,
# MAGIC         GREATEST(m.total_employees, d.present_count)                       AS total,
# MAGIC         d.present_count                                                     AS present,
# MAGIC         d.late_count                                                        AS late,
# MAGIC         GREATEST((m.total_employees - d.present_count), 0)                 AS absent,
# MAGIC         d.total_ot_hours                                                    AS ot_hours,
# MAGIC         d.department
# MAGIC     FROM      emp_daily  d
# MAGIC     INNER JOIN emp_master m
# MAGIC         ON  d.role       = m.role
# MAGIC         AND d.department = m.department
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     dm.work_day,
# MAGIC     dm.role,
# MAGIC     dm.total,
# MAGIC     dm.present,
# MAGIC     dm.late,
# MAGIC     dm.absent,
# MAGIC     dm.ot_hours,
# MAGIC     dm.department,
# MAGIC     COALESCE(wl.who_late,   '')                                             AS who_late,
# MAGIC     COALESCE(wa.who_absent, '')                                             AS who_absent
# MAGIC FROM       daily_master dm
# MAGIC LEFT JOIN  who_late     wl
# MAGIC     ON  dm.work_day   = wl.Work_Day
# MAGIC     AND dm.role       = wl.role
# MAGIC     AND dm.department = wl.department
# MAGIC LEFT JOIN  who_absent   wa
# MAGIC     ON  dm.role       = wa.role
# MAGIC     AND dm.department = wa.department

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Delta Line Utilization

# CELL ********************

from pyspark.sql import functions as F

# ------------------------------------------------------------
# 1) Normalize Production (actual productive minutes)
# ------------------------------------------------------------
df_prod = spark.table("Gold_Production_Lakehouse.prod.gold_standard_vs_actual_time")

df_prod_norm = (
    df_prod
    .select(
        F.to_date(F.col("in_created")).alias("work_date"),
        F.regexp_extract(F.col("prod_line"), r"(\d+)", 1).cast("int").alias("line_num"),
        F.col("routing_no_work_center"),
        F.col("operation_time").cast("double").alias("operation_time"),
    )
)

df_prod_norm.createOrReplaceTempView("v_prod_norm")

# ------------------------------------------------------------
# 2) Normalize Employee Time (FIX: Day_Remark + minutes column)
#    Your table has Day_Remark and Normal_Work_Minutes.
# ------------------------------------------------------------
df_emp = spark.table("Gold_Commons_Lakehouse.cmn.gold_employee_time_all")

df_emp_norm = (
    df_emp
    .select(
        F.to_date(F.col("Work_Day")).alias("work_date"),
        F.regexp_extract(F.col("Department_Eng"), r"(\d+)", 1).cast("int").alias("line_num"),
        F.col("Division_Eng"),
        F.col("Overtime_Type"),
        F.col("Normal_Work_Minutes").cast("double").alias("C_DayInc_D2"),  # capacity minutes source
        F.col("Department_Eng"),
        F.col("sub_department_Eng"),
        F.col("Day_Remark").alias("Day_Memo_reason"),  # alias so downstream logic stays same
        F.col("Employee_Code"),
    )
)

df_emp_norm.createOrReplaceTempView("v_emp_norm")

# ------------------------------------------------------------
# 3) Date window: [current_date-7, current_date-1]
# ------------------------------------------------------------
yesterday_col = F.date_sub(F.current_date(), 1)
week_start_col = F.date_sub(yesterday_col, 30)

# ------------------------------------------------------------
# 4) Productive minutes per day+line (CELL only)
# ------------------------------------------------------------
df_productive = (
    df_prod_norm
    .filter(
        (F.col("work_date").between(week_start_col, yesterday_col)) &
        (F.col("routing_no_work_center").like("%CELL%"))
    )
    .groupBy("work_date", "line_num")
    .agg(F.sum("operation_time").cast("double").alias("productive_minutes"))
)

# ------------------------------------------------------------
# 5) Capacity + headcount per day+line (PRODUCTION, Normal OT=N, CELL, not holiday)
# ------------------------------------------------------------
df_emp_filtered = (
    df_emp_norm
    .filter(
        (F.col("work_date").between(week_start_col, yesterday_col)) &
        (F.col("Division_Eng") == "PRODUCTION") &
        (F.col("Overtime_Type") == "N") &
        (F.col("C_DayInc_D2").isNotNull()) &
        (F.col("Department_Eng").contains("PROD  LINE  ")) &
        (F.col("sub_department_Eng").contains("CELL")) &
        (
            F.col("Day_Memo_reason").isNull() |
            (~F.col("Day_Memo_reason").contains("วันหยุด"))
        )
    )
)

df_hours = (
    df_emp_filtered
    .groupBy("work_date", "line_num")
    .agg(
        F.sum("C_DayInc_D2").cast("double").alias("total_work_min_standard_weekday"),
        F.countDistinct("Employee_Code").alias("headcount"),
        F.first("Department_Eng", ignorenulls=True).alias("prod_line"),
    )
)

# ------------------------------------------------------------
# 6) Join + utilization
# ------------------------------------------------------------
df_line_utilization = (
    df_productive.alias("p")
    .join(df_hours.alias("h"), on=["work_date", "line_num"], how="inner")
    .select(
        F.col("work_date"),
        F.col("h.prod_line"),
        F.col("line_num"),
        F.col("h.headcount"),
        F.col("p.productive_minutes"),
        F.col("h.total_work_min_standard_weekday"),
        F.col("h.total_work_min_standard_weekday").alias("capacity_minutes"),
        F.when(
            F.col("h.total_work_min_standard_weekday") != 0,
            F.col("p.productive_minutes") * F.lit(100.0) / F.col("h.total_work_min_standard_weekday")
        ).alias("line_utilization")
    )
)

df_line_utilization.createOrReplaceTempView("v_line_utilization")

# (optional) show result
display(df_line_utilization.orderBy("work_date", "line_num"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Delta Cell and Department

# CELL ********************

from pyspark.sql import functions as F

# 1) Read source employee table
src_df = spark.table("Gold_Production_Lakehouse.prod.gold_employee_data_daily")

# 2) Filter active employees
active_emp_df = (
    src_df
    .where(
        F.col("department").isNotNull()
        & F.col("sub_department").isNotNull()
        & F.col("end_date").isNull()
    )
)

# 3) Add clean sub_department and compute headcount per dept/sub_dept
dept_map_df = (
    active_emp_df
    .withColumn(
        "sub_department_clean",
        F.regexp_replace(F.col("sub_department"), " ", "")
    )
    .groupBy("department", "sub_department", "sub_department_clean")
    # if you have an employee_id column:
    .agg(F.countDistinct("employee_code").alias("headcount"))
    # or, if no unique id column, fall back to row count:
    # .agg(F.count("*").alias("headcount"))
    .orderBy("department", "sub_department")
)

# 4) FULL REPLACE target mapping table
target_table = "Gold_Production_Lakehouse.delta.gold_delta_cell_and_department"

(
    dept_map_df
    .write
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

# # Gold Delta Repair Rate

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from functools import reduce

repair_df    = spark.table("Gold_Production_Lakehouse.prod.gold_prod_repair")
output_df    = spark.table("Gold_Production_Lakehouse.prod.gold_prod_order_output")
cell_dept_df = spark.table("Gold_Production_Lakehouse.delta.gold_delta_cell_and_department")

today = F.current_date()

periods = {
    "all_time":  (F.lit(True),                                                          F.lit(True)),
    "this_week": (F.col("posting_date") >= F.date_trunc("week", today),                 F.to_date(F.col("created_on")) >= F.date_trunc("week", today)),
    "last_week": ((F.col("posting_date") >= F.date_sub(F.date_trunc("week", today), 7)) & (F.col("posting_date") < F.date_trunc("week", today)),
                  (F.to_date(F.col("created_on")) >= F.date_sub(F.date_trunc("week", today), 7)) & (F.to_date(F.col("created_on")) < F.date_trunc("week", today))),
    "yesterday": (F.col("posting_date") == F.date_sub(today, 1),                        F.to_date(F.col("created_on")) == F.date_sub(today, 1)),
}

def build_period_df(period_name, output_filter, repair_filter):

    # repair_agg now filtered to the same period via created_on
    repair_agg_df = (
        repair_df
        .where(repair_filter)                                        # <-- period filter
        .groupBy("work_center_no")
        .agg(
            F.concat_ws(", ", F.sort_array(F.collect_set("defect_type"))).alias("repair_reasons"),
            F.sum("repair_quantity").alias("total_repair_qty")
        )
    )

    base_df = (
        output_df.alias("o")
        .where(output_filter)
        .join(repair_agg_df.alias("r"),
              F.col("o.cell_line") == F.col("r.work_center_no"), "left")
        .join(cell_dept_df.alias("c"),
              F.col("c.sub_department_clean") == F.col("o.cell_line"), "left")
        .where(F.col("c.department").isNotNull())
        .groupBy(
            F.col("o.cell_line").alias("cell_line"),
            F.col("c.department").alias("department")
        )
        .agg(
            F.sum(F.col("o.entry_type_item_quantity")).alias("total_prod_qty"),
            F.coalesce(F.first(F.col("r.total_repair_qty"), ignorenulls=True), F.lit(0)).alias("total_repair_qty"),
            F.first(F.col("r.repair_reasons"), ignorenulls=True).alias("repair_reasons")
        )
    )

    w_dept = Window.partitionBy("department")

    return (
        base_df
        .withColumn("dept_total_repair_qty", F.sum("total_repair_qty").over(w_dept))
        .withColumn("dept_total_prod_qty",   F.sum("total_prod_qty").over(w_dept))
        .withColumn(
            "repair_rate_cell_line",
            F.when(F.col("total_prod_qty") == 0, F.lit(None)).otherwise(
                F.col("total_repair_qty").cast("decimal(10,2)") /
                F.col("total_prod_qty").cast("decimal(10,2)")
            )
        )
        .withColumn(
            "repair_rate_department",
            F.when(F.col("dept_total_prod_qty") == 0, F.lit(None)).otherwise(
                F.col("dept_total_repair_qty").cast("decimal(10,2)") /
                F.col("dept_total_prod_qty").cast("decimal(10,2)")
            )
        )
        .select(
            F.lit(period_name).alias("period"),
            "cell_line",
            "department",
            "total_prod_qty",
            "repair_reasons",
            "total_repair_qty",
            "repair_rate_cell_line",
            "repair_rate_department"
        )
    )

# Each period now passes TWO filters: one for output_df, one for repair_df
period_dfs = [build_period_df(name, out_f, rep_f) for name, (out_f, rep_f) in periods.items()]
result_df = reduce(lambda a, b: a.unionByName(b), period_dfs)

(
    result_df.write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable("Gold_Production_Lakehouse.delta.gold_delta_repair_rate")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Delta Employee Efficiency Rate

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.delta.gold_delta_employee_efficiency_rate AS
# MAGIC 
# MAGIC WITH base AS (
# MAGIC     SELECT
# MAGIC         created_date,
# MAGIC         total_runtime_qty,
# MAGIC         total_workhour,
# MAGIC         employee_code,
# MAGIC         prod_line,
# MAGIC         cell_line
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_from_emp_time_summary
# MAGIC     WHERE created_date >= date_sub(current_date(), 30)
# MAGIC ),
# MAGIC 
# MAGIC split_employees AS (
# MAGIC     SELECT
# MAGIC         b.created_date,
# MAGIC         b.total_runtime_qty,
# MAGIC         b.total_workhour,
# MAGIC         trim(emp) AS employee_code,
# MAGIC         b.prod_line,
# MAGIC         b.cell_line
# MAGIC     FROM base b
# MAGIC     LATERAL VIEW explode(split(b.employee_code, ',')) t AS emp
# MAGIC ),
# MAGIC 
# MAGIC latest_employee AS (
# MAGIC     SELECT *
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_employee_data_daily
# MAGIC     WHERE load_ts = (
# MAGIC         SELECT MAX(load_ts)
# MAGIC         FROM Gold_Production_Lakehouse.prod.gold_employee_data_daily
# MAGIC     )
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     se.created_date,
# MAGIC     se.employee_code,
# MAGIC     e.first_name_thai,
# MAGIC     e.first_name_eng,
# MAGIC     e.position,
# MAGIC     e.department,
# MAGIC     se.prod_line,
# MAGIC     se.cell_line,
# MAGIC 
# MAGIC     SUM(se.total_runtime_qty) AS total_runtime_min,
# MAGIC     SUM(se.total_workhour) AS total_work_min,
# MAGIC 
# MAGIC     ROUND(
# MAGIC         CASE
# MAGIC             WHEN SUM(se.total_workhour) = 0 THEN NULL
# MAGIC             ELSE SUM(se.total_runtime_qty) * 100.0 / SUM(se.total_workhour)
# MAGIC         END,
# MAGIC         2
# MAGIC     ) AS efficiency_pct
# MAGIC 
# MAGIC FROM split_employees se
# MAGIC LEFT JOIN latest_employee e
# MAGIC     ON e.employee_code = se.employee_code
# MAGIC WHERE e.first_name_thai IS NOT NULL
# MAGIC GROUP BY
# MAGIC     se.created_date,
# MAGIC     se.employee_code,
# MAGIC     e.first_name_thai,
# MAGIC     e.first_name_eng,
# MAGIC     e.position,
# MAGIC     e.department,
# MAGIC     se.prod_line,
# MAGIC     se.cell_line
# MAGIC ORDER BY
# MAGIC     se.created_date DESC,
# MAGIC     efficiency_pct DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Delta Work Cell Performance

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 0) Drop target table if exists
spark.sql("""
DROP TABLE IF EXISTS Gold_Production_Lakehouse.delta.gold_delta_efficiency_rate
""")

# 1) Read source tables
emp_time_df = spark.table("Gold_Production_Lakehouse.prod.gold_from_emp_time_summary")
cell_dept_df = spark.table("Gold_Production_Lakehouse.delta.gold_delta_cell_and_department")

# 2) Join on trimmed sub_department
joined_df = (
    emp_time_df.alias("et")
    .join(
        cell_dept_df.alias("cd"),
        F.trim(F.col("cd.sub_department")) == F.trim(F.col("et.sub_department")),
        "left"
    )
)

# 3) Filter rows (7-day window ending yesterday)
today_col = F.current_date()
yesterday_col = F.date_sub(today_col, 1)
week_start_col = F.date_sub(yesterday_col, 30)

filtered_df = (
    joined_df
    .withColumn("created_date_d", F.to_date(F.col("et.created_date")))
    .where(F.col("et.total_workhour").isNotNull())
    .where(F.col("created_date_d").between(week_start_col, yesterday_col))
    .where(F.col("cd.department").like("PROD  LINE  %"))
)

# 4) Base aggregation
base_df = (
    filtered_df
    .groupBy(
        F.col("created_date_d").alias("created_date"),
        F.col("cd.department").alias("department"),
        F.col("cd.sub_department").alias("sub_department"),
        F.col("et.m_group").alias("machine_center_group"),
    )
    .agg(
        F.sum("et.out_qty").alias("total_out_qty"),
        F.sum("et.total_runtime_qty").alias("total_runtime_min"),   # ACTUAL minutes
        F.sum("et.total_workhour").alias("total_workhour_min"),     # STD/PLANNED minutes
        F.concat_ws(", ", F.sort_array(F.collect_set("et.employee_code"))).alias("employee_codes"),
        F.concat_ws(", ", F.sort_array(F.collect_set("et.first_name_thai"))).alias("employee_names"),
    )
)

# 5) Window for department-level totals per day
dept_day_w = Window.partitionBy("created_date", "department")

# 6) Add efficiency columns (Performance vs Standard):
# efficiency_pct = STD / ACTUAL * 100  -> if ACTUAL is lower than STD, efficiency > 100
result_df = (
    base_df
    .withColumn(
        "efficiency_pct",
        F.when(F.col("total_runtime_min").isNull() | (F.col("total_runtime_min") == 0), F.lit(None).cast("double"))
         .otherwise(F.col("total_workhour_min") * 100.0 / F.col("total_runtime_min"))
    )
    .withColumn("dept_total_runtime_min", F.sum("total_runtime_min").over(dept_day_w))
    .withColumn("dept_total_workhour_min", F.sum("total_workhour_min").over(dept_day_w))
    .withColumn(
        "department_efficiency_pct",
        F.when(F.col("dept_total_runtime_min").isNull() | (F.col("dept_total_runtime_min") == 0), F.lit(None).cast("double"))
         .otherwise(F.col("dept_total_workhour_min") * 100.0 / F.col("dept_total_runtime_min"))
    )
    .drop("dept_total_runtime_min", "dept_total_workhour_min")
)

# 7) Create table fresh
result_df.write.mode("overwrite").saveAsTable(
    "Gold_Production_Lakehouse.delta.gold_delta_efficiency_rate"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

spark.sql("""
CREATE OR REPLACE TABLE Gold_Production_Lakehouse.delta.gold_delta_work_cell_performance
USING DELTA
AS

SELECT
    created_date,
    prod_line,
    cell_line,
    position,

    COUNT(DISTINCT employee_code)        AS employee_count,
    SUM(total_runtime_min)               AS total_runtime_min,
    SUM(total_work_min)                  AS total_work_min,

    ROUND(
        CASE
            WHEN SUM(total_work_min) = 0 THEN NULL
            ELSE SUM(total_runtime_min) * 100.0 / SUM(total_work_min)
        END, 2
    ) AS efficiency_pct

FROM Gold_Production_Lakehouse.delta.gold_delta_employee_efficiency_rate

WHERE created_date = date_sub(current_date(), 1)

GROUP BY
    created_date,
    prod_line,
    cell_line,
    position

ORDER BY
    created_date,
    prod_line,
    cell_line,
    efficiency_pct DESC;
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# # Gold Delta Output Qty

# CELL ********************

from pyspark.sql import functions as F

# --- Ensure schema exists ---
spark.sql("CREATE SCHEMA IF NOT EXISTS Gold_Production_Lakehouse.delta")

# --- Date window: last 14 days (excluding today), matching your SQL ---
start_date = F.date_sub(F.current_date(), 14)   # >= today-14
end_date   = F.current_date()                  # < today

# --- Source tables ---
s = spark.table("Gold_Production_Lakehouse.prod.gold_production_all_status").alias("s")
c = spark.table("Gold_Production_Lakehouse.prod.gold_production_asgn_cell").alias("c")

# --- Build result (equivalent to your SQL) ---
df_output_qty = (
    s.join(c, F.col("c.prod_order_no") == F.col("s.prod_order_no"), "left")
     .where(
         (F.col("s.type_name") == F.lit("Out location")) &
         (F.col("s.CorrectCurrentLocation") == F.lit("QA ROOM")) &
         (F.col("s.created_on") >= start_date) &
         (F.col("s.created_on") < end_date) &
         (F.col("c.prod_line").like("LINE%"))
     )
     .groupBy(
         F.to_date(F.col("s.created_on")).alias("output_date"),
        #  F.regexp_replace(F.col("c.cell_line"), "CELL", "CELL ").alias("sub_department"),
         F.concat(F.lit("PROD  "), F.col("c.prod_line")).alias("department")
     )
     .agg(F.sum(F.col("s.out_qty")).alias("total_output_quantity"))
     .orderBy(F.col("output_date").desc())
)

# --- Fully replace Delta table ---
(
    df_output_qty
      .write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable("Gold_Production_Lakehouse.delta.gold_delta_output_qty")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Delta Due Status

# CELL ********************

from pyspark.sql import functions as F

# --- Ensure schema exists ---
spark.sql("CREATE SCHEMA IF NOT EXISTS Gold_Production_Lakehouse.delta")

# --- Date window: Mon..Fri of the current week (end is exclusive Saturday) ---
mon = F.date_trunc("week", F.current_date()).cast("date")  # Monday
sat_excl = F.date_add(mon, 5)                              # Saturday (exclusive end)

# --- Source tables ---
po = spark.table("Gold_Production_Lakehouse.prod.gold_production_order").alias("po")
so = spark.table("Gold_Production_Lakehouse.prod.gold_sales_order").alias("so")
ac = spark.table("Gold_Production_Lakehouse.prod.gold_production_asgn_cell").alias("ac")

# --- Build result (equivalent to your SQL) ---
df_due_status = (
    po.join(so, F.col("so.SOL") == F.col("po.SOL"), "left")
      .join(
          ac,
          (F.col("ac.prod_order_no") == F.col("po.prod_order_no")) &
          (F.col("ac.prod_order_line_no") == F.col("po.prod_order_line_no")),
          "left"
      )
      .where(
          (F.col("so.StatusSO") == F.lit("Released")) &
          (F.col("po.prod_order_due_date") >= mon) &
          (F.col("po.prod_order_due_date") < sat_excl) &
          (F.col("ac.prod_line").isNotNull())
      )
      .groupBy(
          F.concat(F.lit("PROD  "), F.col("ac.prod_line")).alias("department"),
          F.col("po.due_status").alias("due_status")
      )
      .agg(F.count(F.lit(1)).alias("due_status_count"))
      .orderBy(F.col("due_status_count").desc())
)

# --- Fully replace Delta table ---
(
    df_due_status
      .write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable("Gold_Production_Lakehouse.delta.gold_delta_due_status")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Delta Due In

# CELL ********************

from pyspark.sql import functions as F

# --- Ensure schema exists ---
spark.sql("CREATE SCHEMA IF NOT EXISTS Gold_Production_Lakehouse.delta")

# --- Mon..Fri of current week (Mon start; end exclusive = Saturday) ---
mon = F.date_trunc("week", F.current_date()).cast("date")  # Monday
sat_excl = F.date_add(mon, 5)                              # Saturday (exclusive)

# --- Source tables ---
po = spark.table("Gold_Production_Lakehouse.prod.gold_production_order").alias("po")
so = spark.table("Gold_Production_Lakehouse.prod.gold_sales_order").alias("so")
ac = spark.table("Gold_Production_Lakehouse.prod.gold_production_asgn_cell").alias("ac")

# --- Build result (equivalent to your SQL) ---
df_due_in = (
    po.join(so, F.col("so.SOL") == F.col("po.SOL"), "left")
      .join(
          ac,
          (F.col("ac.prod_order_no") == F.col("po.prod_order_no")) &
          (F.col("ac.prod_order_line_no") == F.col("po.prod_order_line_no")),
          "left"
      )
      .where(
          (F.col("so.StatusSO") == F.lit("Released")) &
          (F.col("po.prod_order_due_date") >= mon) &
          (F.col("po.prod_order_due_date") < sat_excl) &
          (F.col("ac.prod_line").isNotNull())
      )
      .groupBy(
          F.concat(F.lit("PROD  "), F.col("ac.prod_line")).alias("department"),
          F.col("po.due_in").alias("due_in")
      )
      .agg(
          F.count(F.lit(1)).alias("due_in_count"),
          F.sum(F.col("po.prod_line_remaining_quantity")).alias("total_remaining_qty")
      )
      .orderBy(F.col("due_in_count").desc())
)

# --- Fully replace Delta table ---
(
    df_due_in
      .write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable("Gold_Production_Lakehouse.delta.gold_delta_due_in")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Delta Absent

# CELL ********************

from pyspark.sql import functions as F, Window

# ----------------------------
# Setup
# ----------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS Gold_Production_Lakehouse.delta")

year_start = F.make_date(F.year(F.current_date()), F.lit(1), F.lit(1))
year_end   = F.make_date(F.year(F.current_date()) + F.lit(1), F.lit(1), F.lit(1))  # exclusive

et = spark.table("Gold_Commons_Lakehouse.cmn.gold_employee_time_all").alias("et")
ed = spark.table("Gold_Production_Lakehouse.prod.gold_employee_data_daily").alias("ed")

active_ed = (
    ed.where(F.col("end_date").isNull())
      .select(F.col("employee_code").alias("active_employee_code"))
      .alias("aed")
)

# ----------------------------
# Latest department per employee (ONLY ONE dept column used)
# ----------------------------
latest_dept_src = (
    et.select(
        F.col("Employee_Code").alias("employee_code"),
        F.col("Department_Eng").alias("latest_department"),
        F.col("Work_Day").alias("dept_work_day")
    )
    .where(F.col("latest_department").isNotNull())
    .alias("lds")
)

w_latest = Window.partitionBy("employee_code").orderBy(F.col("dept_work_day").desc())
latest_dept = (
    latest_dept_src
    .withColumn("rn", F.row_number().over(w_latest))
    .where(F.col("rn") == 1)
    .select("employee_code", "latest_department")
    .alias("ld")
)

# ----------------------------
# Memo cleaning + exclusions
# ----------------------------
excluded_reasons = ["ลาออกระหว่างงวด", "RH", "ครึ่งหลังไม่ได้รูดบัตร", "ยังไม่เริ่มงาน", "ครึ่งแรกไม่ได้รูดบัตร", "VL"]
memo_for_exclude = F.trim(F.regexp_replace(F.coalesce(F.col("et.Day_Memo_reason_EN"), F.lit("")), r"\s+", " "))

memo_raw = F.coalesce(F.col("et.Day_Memo_reason_EN"), F.lit(""))
clean_memo = F.trim(
    F.regexp_replace(
        F.regexp_replace(memo_raw, r'[\t\n\r/\\\-\_\.\,;:\(\)\[\]\{\}\'"`]', " "),
        r"\s+",
        " "
    )
)
norm_memo = F.upper(clean_memo)
for _ in range(5):
    norm_memo = F.regexp_replace(norm_memo, r"([A-Z])\s+([A-Z])", r"$1$2")  # "U P" -> "UP"

# leave minutes per your latest rule (as provided)
leave_minutes_expr = (
    F.coalesce(F.col("et.late_time_in_minutes"), F.lit(0)) +
    F.coalesce(F.col("et.Total_Leave_Hours"), F.lit(0)) +
    F.coalesce(F.col("et.Total_Leave_Days"), F.lit(0))
)

# ----------------------------
# Base (year window) + join latest department (single dept)
# ----------------------------
base = (
    et.select(
        F.col("Employee_Code").alias("employee_code"),
        F.concat_ws(" ", F.col("First_Name_Eng"), F.col("Last_Name_Eng")).alias("employee_name"),
        F.col("Work_Day"),
        leave_minutes_expr.alias("leave_minutes"),
        norm_memo.alias("clean_memo")
    )
    .where(
        (F.col("Work_Day") >= year_start) &
        (F.col("Work_Day") < year_end) &
        (~memo_for_exclude.isin(*excluded_reasons))
    )
    .join(latest_dept, "employee_code", "left")
    .where(
        (F.col("latest_department").like("PROD %")) &
        (F.col("latest_department") != F.lit("PROD LINE S"))
    )
    .alias("b")
)

# ----------------------------
# Leave summary (ABSENT: most leave first)
# ----------------------------
leave_summary = (
    base.groupBy("employee_code", "latest_department")
        .agg(
            F.max("employee_name").alias("employee_name"),
            F.sum("leave_minutes").alias("total_leave_minutes")
        )
        .join(active_ed, F.col("employee_code") == F.col("active_employee_code"), "inner")
        .drop("active_employee_code")
        .withColumn("total_leave_days", F.col("total_leave_minutes") / F.lit(560.0))
        .alias("ls")
)

w_abs = Window.partitionBy("latest_department").orderBy(F.col("total_leave_days").desc(), F.col("employee_code"))
ranked_absent = leave_summary.withColumn("leave_rank", F.row_number().over(w_abs)).alias("r")

# ----------------------------
# Reasons summary (only where leave_minutes > 0)
# ----------------------------
reason_counts = (
    base.join(ranked_absent, F.col("b.employee_code") == F.col("r.employee_code"), "inner")
        .where(
            (F.col("b.leave_minutes") > 0) &
            (F.col("b.clean_memo").isNotNull()) &
            (F.length(F.trim(F.col("b.clean_memo"))) > 0)
        )
        .groupBy(
            F.col("b.employee_code").alias("rc_employee_code"),
            F.col("b.clean_memo").alias("clean_memo")
        )
        .agg(F.count(F.lit(1)).alias("reason_count"))
)

reasons_summary = (
    reason_counts.groupBy("rc_employee_code", "clean_memo")
        .agg(F.sum("reason_count").alias("reason_count"))
        .groupBy("rc_employee_code")
        .agg(
            F.concat_ws(
                ", ",
                F.sort_array(
                    F.collect_list(F.concat(F.col("clean_memo"), F.lit(" x "), F.col("reason_count")))
                )
            ).alias("leave_reasons_summary")
        )
        .alias("rs")
)

# ----------------------------
# Final output (ONLY department column; NO department_eng)
# ----------------------------
final_absent = (
    ranked_absent.join(reasons_summary, F.col("r.employee_code") == F.col("rs.rc_employee_code"), "left")
        .select(
            F.col("r.latest_department").alias("department"),
            F.col("r.leave_rank"),
            F.col("r.employee_code"),
            F.col("r.employee_name"),
            F.col("r.total_leave_minutes"),
            F.col("r.total_leave_days"),
            F.coalesce(F.col("rs.leave_reasons_summary"), F.lit("")).alias("leave_reasons_summary"),
        )
        .orderBy(F.col("department"), F.col("leave_rank"))
)

# ----------------------------
# DROP + RECREATE to remove old schema columns like department_eng
# ----------------------------
spark.sql("DROP TABLE IF EXISTS Gold_Production_Lakehouse.delta.gold_delta_absent")

(final_absent.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("Gold_Production_Lakehouse.delta.gold_delta_absent")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Delta Present

# CELL ********************

from pyspark.sql import functions as F, Window

spark.sql("CREATE SCHEMA IF NOT EXISTS Gold_Production_Lakehouse.delta")

year_start = F.make_date(F.year(F.current_date()), F.lit(1), F.lit(1))
year_end   = F.make_date(F.year(F.current_date()) + F.lit(1), F.lit(1), F.lit(1))

et = spark.table("Gold_Commons_Lakehouse.cmn.gold_employee_time_all").alias("et")
ed = spark.table("Gold_Production_Lakehouse.prod.gold_employee_data_daily").alias("ed")

active_ed = (
    ed.where(F.col("end_date").isNull())
      .select(F.col("employee_code").alias("active_employee_code"))
      .alias("aed")
)

# ----------------------------
# Latest department per employee (single dept used)
# ----------------------------
latest_dept_src = (
    et.select(
        F.col("Employee_Code").alias("employee_code"),
        F.col("Department_Eng").alias("latest_department"),
        F.col("Work_Day").alias("dept_work_day")
    )
    .where(F.col("latest_department").isNotNull())
    .alias("lds")
)

w_latest_dept = Window.partitionBy("employee_code").orderBy(F.col("dept_work_day").desc())

latest_dept = (
    latest_dept_src
    .withColumn("rn", F.row_number().over(w_latest_dept))
    .where(F.col("rn") == 1)
    .select("employee_code", "latest_department")
    .alias("ld")
)

# ----------------------------
# Memo cleaning + exclusions
# ----------------------------
excluded_reasons = ["ลาออกระหว่างงวด", "RH", "ครึ่งหลังไม่ได้รูดบัตร", "ยังไม่เริ่มงาน", "ครึ่งแรกไม่ได้รูดบัตร", "VL"]
memo_for_exclude = F.trim(F.regexp_replace(F.coalesce(F.col("et.Day_Memo_reason_EN"), F.lit("")), r"\s+", " "))

memo_raw = F.coalesce(F.col("et.Day_Memo_reason_EN"), F.lit(""))
clean_memo = F.trim(
    F.regexp_replace(
        F.regexp_replace(memo_raw, r'[\t\n\r/\\\-\_\.\,;:\(\)\[\]\{\}\'"`]', " "),
        r"\s+",
        " "
    )
)
norm_memo = F.upper(clean_memo)
for _ in range(5):
    norm_memo = F.regexp_replace(norm_memo, r"([A-Z])\s+([A-Z])", r"$1$2")

leave_minutes_expr = (
    F.coalesce(F.col("et.late_time_in_minutes"), F.lit(0)) +
    F.coalesce(F.col("et.Total_Leave_Hours"), F.lit(0)) +
    F.coalesce(F.col("et.Total_Leave_Days"), F.lit(0))
)

# ----------------------------
# Base (year window) + latest dept join
# ----------------------------
base = (
    et.select(
        F.col("Employee_Code").alias("employee_code"),
        F.concat_ws(" ", F.col("First_Name_Eng"), F.col("Last_Name_Eng")).alias("employee_name"),
        F.col("Work_Day"),
        leave_minutes_expr.alias("leave_minutes"),
        norm_memo.alias("clean_memo")
    )
    .where(
        (F.col("Work_Day") >= year_start) &
        (F.col("Work_Day") < year_end) &
        (~memo_for_exclude.isin(*excluded_reasons))
    )
    .join(latest_dept, "employee_code", "left")
    .where(
        (F.col("latest_department").like("PROD %")) &
        (F.col("latest_department") != F.lit("PROD LINE S"))
    )
    .alias("b")
)

leave_summary = (
    base.groupBy("employee_code", "latest_department")
        .agg(
            F.max("employee_name").alias("employee_name"),
            F.sum("leave_minutes").alias("total_leave_minutes")
        )
        .join(active_ed, F.col("employee_code") == F.col("active_employee_code"), "inner")
        .drop("active_employee_code")
        .withColumn("total_leave_days", F.col("total_leave_minutes") / F.lit(560.0))
        .alias("ls")
)

# Rank ALL (least leave days first)
w_pre = Window.partitionBy("latest_department").orderBy(F.col("total_leave_days").asc(), F.col("employee_code"))
ranked_present = leave_summary.withColumn("leave_rank", F.row_number().over(w_pre)).alias("r")

reason_counts = (
    base.join(ranked_present, F.col("b.employee_code") == F.col("r.employee_code"), "inner")
        .where(
            (F.col("b.leave_minutes") > 0) &
            (F.col("b.clean_memo").isNotNull()) &
            (F.length(F.trim(F.col("b.clean_memo"))) > 0)
        )
        .groupBy(
            F.col("b.employee_code").alias("rc_employee_code"),
            F.col("b.clean_memo").alias("clean_memo")
        )
        .agg(F.count(F.lit(1)).alias("reason_count"))
)

reasons_summary = (
    reason_counts.groupBy("rc_employee_code", "clean_memo")
        .agg(F.sum("reason_count").alias("reason_count"))
        .groupBy("rc_employee_code")
        .agg(
            F.concat_ws(
                ", ",
                F.sort_array(
                    F.collect_list(F.concat(F.col("clean_memo"), F.lit(" x "), F.col("reason_count")))
                )
            ).alias("leave_reasons_summary")
        )
        .alias("rs")
)

final_present = (
    ranked_present.join(reasons_summary, F.col("r.employee_code") == F.col("rs.rc_employee_code"), "left")
        .select(
            F.col("r.latest_department").alias("department"),
            F.col("r.leave_rank"),
            F.col("r.employee_code"),
            F.col("r.employee_name"),
            F.col("r.total_leave_minutes"),
            F.col("r.total_leave_days"),
            F.coalesce(F.col("rs.leave_reasons_summary"), F.lit("")).alias("leave_reasons_summary"),
        )
        .orderBy(F.col("department"), F.col("leave_rank"))
)

# ----------------------------
# DROP + RECREATE to remove old schema columns (e.g., department_eng)
# ----------------------------
spark.sql("DROP TABLE IF EXISTS Gold_Production_Lakehouse.delta.gold_delta_present")

(final_present.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("Gold_Production_Lakehouse.delta.gold_delta_present")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Delta OT

# CELL ********************

from pyspark.sql import functions as F, Window

spark.sql("CREATE SCHEMA IF NOT EXISTS Gold_Production_Lakehouse.delta")

year_start = F.make_date(F.year(F.current_date()), F.lit(1), F.lit(1))
year_end   = F.make_date(F.year(F.current_date()) + F.lit(1), F.lit(1), F.lit(1))

et = spark.table("Gold_Commons_Lakehouse.cmn.gold_employee_time_all").alias("et")
ed = spark.table("Gold_Production_Lakehouse.prod.gold_employee_data_daily").alias("ed")

active_ed = (
    ed.where(F.col("end_date").isNull())
      .select(F.col("employee_code").alias("active_employee_code"))
      .alias("aed")
)

# ----------------------------
# Latest department per employee (single dept used)
# ----------------------------
latest_dept_src = (
    et.select(
        F.col("Employee_Code").alias("employee_code"),
        F.col("Department_Eng").alias("latest_department"),
        F.col("Work_Day").alias("dept_work_day")
    )
    .where(F.col("latest_department").isNotNull())
    .alias("lds")
)

w_latest = Window.partitionBy("employee_code").orderBy(F.col("dept_work_day").desc())

latest_dept = (
    latest_dept_src
    .withColumn("rn", F.row_number().over(w_latest))
    .where(F.col("rn") == 1)
    .select("employee_code", "latest_department")
    .alias("ld")
)

# 560 minutes/day = 9.333333 hours/day
DAY_HOURS = 560.0 / 60.0

ot_hours_expr = (
    F.coalesce(F.col("et.OT_Work_Minutes"), F.lit(0))
)

base = (
    et.select(
        F.col("Employee_Code").alias("employee_code"),
        F.concat_ws(" ", F.col("First_Name_Eng"), F.col("Last_Name_Eng")).alias("employee_name"),
        F.col("Work_Day"),
        ot_hours_expr.alias("ot_hours")
    )
    .where((F.col("Work_Day") >= year_start) & (F.col("Work_Day") < year_end))
    .join(latest_dept, "employee_code", "left")
    .where(
        (F.col("latest_department").like("PROD %")) &
        (F.col("latest_department") != F.lit("PROD LINE S"))
    )
    .alias("b")
)

ot_summary = (
    base.groupBy("latest_department", "employee_code")
        .agg(
            F.max("employee_name").alias("employee_name"),
            F.sum("ot_hours").alias("total_ot_hours")
        )
        .where(F.col("total_ot_hours") > 0)
        .join(active_ed, F.col("employee_code") == F.col("active_employee_code"), "inner")
        .drop("active_employee_code")
        .withColumn("total_ot_days", F.col("total_ot_hours") / F.lit(DAY_HOURS))
        .alias("os")
)

w_ot = Window.partitionBy("latest_department").orderBy(F.col("total_ot_days").desc(), F.col("employee_code"))

final_ot = (
    ot_summary
    .withColumn("ot_rank", F.row_number().over(w_ot))
    .select(
        F.col("latest_department").alias("department"),
        "ot_rank",
        "employee_code",
        "employee_name",
        "total_ot_hours",
        "total_ot_days"
    )
    .orderBy(F.col("department"), F.col("ot_rank"))
)

# ----------------------------
# DROP + RECREATE to remove old schema columns (if any)
# ----------------------------
spark.sql("DROP TABLE IF EXISTS Gold_Production_Lakehouse.delta.gold_delta_ot")

(final_ot.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("Gold_Production_Lakehouse.delta.gold_delta_ot")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Delta Metal Loss

# CELL ********************

from pyspark.sql import functions as F

# --- Week window (Mon–Fri) based on today's date ---
today = F.current_date()

# Monday of current week
week_start = F.date_sub(today, F.dayofweek(today) - 2)
# Friday of current week
week_end = F.date_add(week_start, 4)

# --- Source table ---
ml = spark.table("Gold_Inventory_Lakehouse.inv.gold_metal_loss_by_line").alias("ml")

df_metal_loss = (
    ml.withColumn("date", F.to_date(F.col("Date")))
      .where((F.col("date") >= week_start) & (F.col("date") <= week_end))
      .groupBy(
          F.col("date").alias("date"),
          F.col("Line").alias("department"),
          F.regexp_replace(F.col("Cell"), "CELL", "CELL ").alias("cell"),
          F.col("loss_after_scrap_std_flag").alias("loss_after_scrap_std_flag")
      )
      .agg(F.count(F.lit(1)).alias("flag_count"))
      .orderBy("date", "department", "cell", "loss_after_scrap_std_flag")
)

(
    df_metal_loss.write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable("Gold_Production_Lakehouse.delta.gold_delta_metal_loss")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold  DELTA Production Detail

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

# CELL ********************

# MAGIC %%sql
# MAGIC -- Spark SQL version (Databricks / Spark)
# MAGIC -- Creates a table from your final T-SQL logic.
# MAGIC -- Change the target table name below if needed.
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.delta.gold_delta_production_detail
# MAGIC -- ============================================================
# MAGIC -- Spark SQL (Databricks) version
# MAGIC -- Creates a Spark table with 1 row per:
# MAGIC --   prod_order_no + prod_order_line_no + item_no
# MAGIC --
# MAGIC -- Notes vs T-SQL:
# MAGIC -- - TOP -> not used (use window row_number)
# MAGIC -- - OUTER APPLY -> replaced with ranked LEFT JOIN
# MAGIC -- - GETDATE() -> current_date()
# MAGIC -- - STRING_AGG -> concat_ws(', ', collect_list(...))
# MAGIC -- - TRIM -> trim()
# MAGIC -- ============================================================
# MAGIC 
# MAGIC AS
# MAGIC WITH a_base AS (
# MAGIC   SELECT
# MAGIC     created_on,
# MAGIC     modified_on,
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     machine_center_no,
# MAGIC     operation_no,
# MAGIC     item_no,
# MAGIC     quantity,
# MAGIC     remaining_quantity,
# MAGIC     sales_order_no,
# MAGIC     current_location_code,
# MAGIC     past_location_code,
# MAGIC     trim(employee_no) AS employee_no,     -- trim only, keep leading zeros
# MAGIC     cell_line,
# MAGIC     prod_line,
# MAGIC     sub_department,
# MAGIC     actual_run_time_min
# MAGIC   FROM Gold_Production_Lakehouse.prod.gold_prod_actual_time_by_employees
# MAGIC   WHERE prod_line LIKE 'LINE%'
# MAGIC ),
# MAGIC 
# MAGIC d_pol AS (
# MAGIC   SELECT
# MAGIC     prod_order_status,
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     sales_order_no,
# MAGIC     sales_order_line_no,
# MAGIC     FG_item_no,
# MAGIC     FG_item_group,
# MAGIC     prod_item_line,
# MAGIC     item_routing_no,
# MAGIC     prod_order_quantity,
# MAGIC     prod_order_starting_date_time,
# MAGIC     prod_order_ending_date_time,
# MAGIC     prod_order_finished_date,
# MAGIC     prod_order_due_date,
# MAGIC     commit_week,
# MAGIC     ref_prod_order,
# MAGIC     ref_item,
# MAGIC     prod_line_due_date,
# MAGIC     prod_line_start_date,
# MAGIC     prod_line_end_date,
# MAGIC     prod_line_quantity,
# MAGIC     prod_line_finished_quantity,
# MAGIC     prod_line_remaining_quantity,
# MAGIC     item_location,
# MAGIC     description,
# MAGIC     SOL,
# MAGIC     POL,
# MAGIC     due_in,
# MAGIC     due_status
# MAGIC   FROM Gold_Production_Lakehouse.prod.gold_production_order
# MAGIC ),
# MAGIC 
# MAGIC b_latest AS (
# MAGIC   SELECT *
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       b.*,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         PARTITION BY b.prod_order_no, b.prod_order_line_no, b.item_no
# MAGIC         ORDER BY b.modified_on DESC, b.created_on DESC
# MAGIC       ) AS rn
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_production_status b
# MAGIC   ) x
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC -- 1 row per prod_order_no + prod_order_line_no + item_no
# MAGIC activity_agg AS (
# MAGIC   SELECT
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     item_no,
# MAGIC     max(cell_line) AS cell_line,
# MAGIC     max(prod_line) AS prod_line,
# MAGIC     max(sub_department) AS sub_department,
# MAGIC     sum(coalesce(actual_run_time_min, 0)) AS actual_time_min,
# MAGIC     max(modified_on) AS last_activity_on_any
# MAGIC   FROM a_base
# MAGIC   GROUP BY prod_order_no, prod_order_line_no, item_no
# MAGIC ),
# MAGIC 
# MAGIC -- unique employees per prod line/item (NO DISTINCT: group by)
# MAGIC emp_dedup AS (
# MAGIC   SELECT
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     item_no,
# MAGIC     employee_no
# MAGIC   FROM a_base
# MAGIC   WHERE employee_no IS NOT NULL AND employee_no <> ''
# MAGIC   GROUP BY prod_order_no, prod_order_line_no, item_no, employee_no
# MAGIC ),
# MAGIC 
# MAGIC -- best name per employee_no
# MAGIC emp_name AS (
# MAGIC   SELECT
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     item_no,
# MAGIC     employee_no,
# MAGIC     employee_name
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       ed.prod_order_no,
# MAGIC       ed.prod_order_line_no,
# MAGIC       ed.item_no,
# MAGIC       ed.employee_no,
# MAGIC       coalesce(
# MAGIC         nullif(trim(concat(coalesce(e.first_name_eng, ''), ' ', coalesce(e.last_name_eng, ''))), ''),
# MAGIC         'Unknown'
# MAGIC       ) AS employee_name,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         PARTITION BY ed.prod_order_no, ed.prod_order_line_no, ed.item_no, ed.employee_no
# MAGIC         ORDER BY
# MAGIC           CASE WHEN e.first_name_eng IS NULL AND e.last_name_eng IS NULL THEN 1 ELSE 0 END,
# MAGIC           e.load_ts DESC
# MAGIC       ) AS rn
# MAGIC     FROM emp_dedup ed
# MAGIC     LEFT JOIN Gold_Production_Lakehouse.prod.gold_employee_data_daily e
# MAGIC       ON e.employee_code = ed.employee_no
# MAGIC       OR e.antenna_id   = ed.employee_no
# MAGIC   ) z
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC who_worked AS (
# MAGIC   SELECT
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     item_no,
# MAGIC     concat_ws(', ', collect_list(concat(employee_name, ' (', employee_no, ')'))) AS all_worked_by
# MAGIC   FROM emp_name
# MAGIC   GROUP BY prod_order_no, prod_order_line_no, item_no
# MAGIC ),
# MAGIC 
# MAGIC last_activity AS (
# MAGIC   SELECT
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     item_no,
# MAGIC     employee_no,
# MAGIC     modified_on
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       a.*,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         PARTITION BY a.prod_order_no, a.prod_order_line_no, a.item_no
# MAGIC         ORDER BY a.modified_on DESC, a.created_on DESC
# MAGIC       ) AS rn
# MAGIC     FROM a_base a
# MAGIC     WHERE a.employee_no IS NOT NULL AND a.employee_no <> ''
# MAGIC   ) x
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC last_worked_by AS (
# MAGIC   SELECT
# MAGIC     la.prod_order_no,
# MAGIC     la.prod_order_line_no,
# MAGIC     la.item_no,
# MAGIC     concat(coalesce(en.employee_name, 'Unknown'), ' (', la.employee_no, ')') AS last_worked_by,
# MAGIC     la.modified_on AS last_worked_on
# MAGIC   FROM last_activity la
# MAGIC   LEFT JOIN emp_name en
# MAGIC     ON  la.prod_order_no = en.prod_order_no
# MAGIC     AND la.prod_order_line_no = en.prod_order_line_no
# MAGIC     AND la.item_no = en.item_no
# MAGIC     AND la.employee_no = en.employee_no
# MAGIC ),
# MAGIC 
# MAGIC -- efficiency from summary table (joined by prod_line only)
# MAGIC efficiency_line AS (
# MAGIC   SELECT
# MAGIC     prod_line,
# MAGIC     sum(coalesce(total_runtime_qty, 0)) AS total_runtime_qty,
# MAGIC     sum(coalesce(total_workhour, 0))    AS total_workhour
# MAGIC   FROM Gold_Production_Lakehouse.prod.gold_from_emp_time_summary
# MAGIC   GROUP BY prod_line
# MAGIC ),
# MAGIC 
# MAGIC -- metal loss: latest per (Line, Cell, FG)
# MAGIC metal_loss_latest AS (
# MAGIC   SELECT *
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       m.`Date`,
# MAGIC       m.Line,
# MAGIC       m.Cell,
# MAGIC       m.Name,
# MAGIC       m.FG,
# MAGIC       m.Prod,
# MAGIC       m.Metal,
# MAGIC       m.total_consumption,
# MAGIC       m.weight_after_FL,
# MAGIC       m.Scrap,
# MAGIC       m.loss_scrap_g,
# MAGIC       m.loss_scrap_pct,
# MAGIC       m.loss_scrap_std_flag,
# MAGIC       m.Dust,
# MAGIC       m.loss_after_dust_g,
# MAGIC       m.loss_after_dust_pct,
# MAGIC       m.loss_after_scrap_std_flag,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         PARTITION BY m.Line, m.Cell, m.FG
# MAGIC         ORDER BY m.`Date` DESC
# MAGIC       ) AS rn
# MAGIC     FROM Gold_Inventory_Lakehouse.inv.gold_metal_loss_by_line m
# MAGIC   ) x
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC -- Sales Order: pick latest row per SalesorderNo (since you used TOP 1 ORDER BY updated_at)
# MAGIC so_ranked AS (
# MAGIC   SELECT *
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       so.*,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         PARTITION BY so.SalesorderNo
# MAGIC         ORDER BY so.updated_at DESC
# MAGIC       ) AS rn
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_sales_order so
# MAGIC     WHERE coalesce(so.OutstandingQty, 0) > 0
# MAGIC       AND so.StatusSO <> 'Closed'
# MAGIC   ) x
# MAGIC   WHERE rn = 1
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC   -- Sales / Customer
# MAGIC   so.CusNo,
# MAGIC   so.CusName,
# MAGIC   so.CusAbbr,
# MAGIC   so.StatusSO,
# MAGIC   so.ReqDate,
# MAGIC   so.PmDate,
# MAGIC   so.requested_week,
# MAGIC   so.SalesorderNo,
# MAGIC   so.SalesLineNo,
# MAGIC   so.ItemFG,
# MAGIC   so.item_description,
# MAGIC   so.Total_QTY,
# MAGIC   so.OutstandingQty,
# MAGIC   so.SOL AS so_SOL,
# MAGIC   so.so_abbr,
# MAGIC   so.so_type,
# MAGIC   so.SOI,
# MAGIC 
# MAGIC   -- Production order
# MAGIC   d.prod_order_status,
# MAGIC   d.prod_order_no,
# MAGIC   d.prod_order_line_no,
# MAGIC   aa.item_no,
# MAGIC   d.FG_item_no,
# MAGIC   d.FG_item_group,
# MAGIC   d.prod_item_line,
# MAGIC   d.item_routing_no,
# MAGIC   d.prod_order_quantity,
# MAGIC   d.prod_order_starting_date_time,
# MAGIC   d.prod_order_ending_date_time,
# MAGIC   d.prod_order_finished_date,
# MAGIC   d.prod_order_due_date,
# MAGIC   d.commit_week,
# MAGIC   d.ref_prod_order,
# MAGIC   d.ref_item,
# MAGIC 
# MAGIC   d.prod_line_start_date,
# MAGIC   d.prod_line_due_date,
# MAGIC   d.prod_line_end_date,
# MAGIC   d.prod_line_quantity,
# MAGIC   d.prod_line_finished_quantity,
# MAGIC   d.prod_line_remaining_quantity,
# MAGIC 
# MAGIC   d.SOL,
# MAGIC   d.POL,
# MAGIC   d.due_in,
# MAGIC   d.due_status,
# MAGIC 
# MAGIC   d.description AS prod_description,
# MAGIC 
# MAGIC   -- Cell / line
# MAGIC   aa.cell_line,
# MAGIC   aa.prod_line,
# MAGIC   aa.sub_department,
# MAGIC 
# MAGIC   -- Stage / location
# MAGIC   coalesce(b.CorrectCurrentLocation, b.current_location_code, d.item_location) AS current_stage_location,
# MAGIC 
# MAGIC   -- Who
# MAGIC   ww.all_worked_by,
# MAGIC   lw.last_worked_by,
# MAGIC   lw.last_worked_on,
# MAGIC 
# MAGIC   -- Efficiency (ONE column)
# MAGIC   round(
# MAGIC     100.0 * cast(el.total_runtime_qty as double) / nullif(cast(el.total_workhour as double), 0.0),
# MAGIC     2
# MAGIC   ) AS efficiency_pct,
# MAGIC 
# MAGIC   -- Progress %
# MAGIC   round(
# MAGIC     100.0 * cast(d.prod_line_finished_quantity as double) / nullif(cast(d.prod_line_quantity as double), 0.0),
# MAGIC     2
# MAGIC   ) AS progress_pct,
# MAGIC 
# MAGIC   -- Timing status
# MAGIC   CASE
# MAGIC   -- ✅ Force completed if production order is Finished
# MAGIC       WHEN upper(d.prod_order_status) = 'FINISHED' THEN 'COMPLETED'
# MAGIC       WHEN d.prod_line_due_date IS NULL THEN 'NO_DUE_DATE'
# MAGIC       WHEN coalesce(d.prod_line_quantity, 0) > 0
# MAGIC           AND coalesce(d.prod_line_finished_quantity, 0) >= coalesce(d.prod_line_quantity, 0)
# MAGIC           THEN 'COMPLETED'
# MAGIC       WHEN d.prod_line_due_date < current_date() THEN 'OVERDUE'
# MAGIC       WHEN d.prod_line_due_date <= date_add(current_date(), 3) THEN 'DUE_SOON'
# MAGIC       ELSE 'ON_TRACK'
# MAGIC   END AS timing_status,
# MAGIC 
# MAGIC   -- Metal loss columns + status
# MAGIC   ml.`Date` AS metal_loss_date,
# MAGIC   ml.Prod   AS metal_loss_prod,
# MAGIC   ml.Metal  AS metal_loss_metal,
# MAGIC   ml.total_consumption,
# MAGIC   ml.weight_after_FL,
# MAGIC   ml.Scrap,
# MAGIC   ml.loss_scrap_g,
# MAGIC   ml.loss_scrap_pct,
# MAGIC   ml.loss_scrap_std_flag,
# MAGIC   ml.Dust,
# MAGIC   ml.loss_after_dust_g,
# MAGIC   ml.loss_after_dust_pct,
# MAGIC   ml.loss_after_scrap_std_flag,
# MAGIC   CASE
# MAGIC     WHEN ml.loss_after_scrap_std_flag IS NULL THEN 'NO_METAL_LOSS_DATA'
# MAGIC     WHEN upper(ml.loss_after_scrap_std_flag) IN ('OVER','ABOVE','HIGH') THEN 'OVER_STANDARD'
# MAGIC     WHEN upper(ml.loss_after_scrap_std_flag) IN ('UNDER','BELOW','LOW') THEN 'UNDER_STANDARD'
# MAGIC     ELSE cast(ml.loss_after_scrap_std_flag as string)
# MAGIC   END AS metal_loss_status
# MAGIC 
# MAGIC FROM activity_agg aa
# MAGIC JOIN d_pol d
# MAGIC   ON aa.prod_order_no = d.prod_order_no
# MAGIC  AND aa.prod_order_line_no = d.prod_order_line_no
# MAGIC  AND aa.item_no = d.FG_item_no         -- keep your join; change if your aa.item_no matches different field
# MAGIC 
# MAGIC LEFT JOIN b_latest b
# MAGIC   ON aa.prod_order_no = b.prod_order_no
# MAGIC  AND aa.prod_order_line_no = b.prod_order_line_no
# MAGIC  AND aa.item_no = b.item_no
# MAGIC 
# MAGIC LEFT JOIN who_worked ww
# MAGIC   ON aa.prod_order_no = ww.prod_order_no
# MAGIC  AND aa.prod_order_line_no = ww.prod_order_line_no
# MAGIC  AND aa.item_no = ww.item_no
# MAGIC 
# MAGIC LEFT JOIN last_worked_by lw
# MAGIC   ON aa.prod_order_no = lw.prod_order_no
# MAGIC  AND aa.prod_order_line_no = lw.prod_order_line_no
# MAGIC  AND aa.item_no = lw.item_no
# MAGIC 
# MAGIC LEFT JOIN efficiency_line el
# MAGIC   ON el.prod_line = aa.prod_line
# MAGIC 
# MAGIC LEFT JOIN metal_loss_latest ml
# MAGIC   ON ml.Line = aa.prod_line
# MAGIC  AND ml.Cell = aa.cell_line
# MAGIC  AND ml.FG   = d.FG_item_no
# MAGIC  AND aa.prod_order_no = ml.Prod
# MAGIC 
# MAGIC LEFT JOIN so_ranked so
# MAGIC   ON so.SalesorderNo = d.sales_order_no
# MAGIC 
# MAGIC WHERE so.SalesorderNo IS NOT NULL
# MAGIC ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Delta Employee Efficiency

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.delta.gold_delta_employee_efficiency AS
# MAGIC 
# MAGIC WITH employee_dim AS (
# MAGIC     SELECT
# MAGIC         a.Employee_Code AS employee_no,
# MAGIC         MAX(a.First_Name_Eng) AS first_name,
# MAGIC         MAX(a.Last_Name_Eng)  AS last_name
# MAGIC     FROM Gold_Commons_Lakehouse.cmn.gold_employee_time_all a
# MAGIC     INNER JOIN Gold_Production_Lakehouse.prod.gold_employee_data_daily b
# MAGIC         ON a.Employee_Code = b.employee_code
# MAGIC     WHERE a.Employee_Code IS NOT NULL
# MAGIC       AND b.end_date IS NULL
# MAGIC     GROUP BY a.Employee_Code
# MAGIC ),
# MAGIC 
# MAGIC routing_dim AS (
# MAGIC     SELECT
# MAGIC         `Routing No.`     AS item_no,
# MAGIC         `Operation No.`   AS operation_no,
# MAGIC         `Work Center No.` AS machine_center_no,
# MAGIC         `Run Time`        AS bc_std_run_time
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `Routing No.`, `Operation No.`, `Work Center No.`
# MAGIC                 ORDER BY `SystemModifiedAt` DESC
# MAGIC             ) AS rn
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Routing Line`
# MAGIC         WHERE `Routing No.`   IS NOT NULL
# MAGIC           AND `Operation No.` IS NOT NULL
# MAGIC     ) ranked
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC prod_30d AS (
# MAGIC     SELECT
# MAGIC         employee_no,
# MAGIC         prod_line,
# MAGIC         cell_line,
# MAGIC         machine_center_no,
# MAGIC         item_no,
# MAGIC         operation_no,
# MAGIC 
# MAGIC         COUNT(*)           AS records_count,
# MAGIC         SUM(ABS(quantity)) AS total_qty,
# MAGIC 
# MAGIC         AVG(plan_run_time) AS avg_plan_run_time_per_pcs,
# MAGIC 
# MAGIC         AVG(
# MAGIC             CASE
# MAGIC                 WHEN ABS(COALESCE(quantity, 0)) = 0 THEN NULL
# MAGIC                 ELSE actual_run_time_min * 1.0 / ABS(quantity)
# MAGIC             END
# MAGIC         ) AS actual_run_time,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN SUM(actual_run_time_min) = 0 THEN NULL
# MAGIC             ELSE (SUM(total_plan_runtime) * 100.0) / SUM(actual_run_time_min)
# MAGIC         END AS actual_efficiency
# MAGIC 
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_prod_actual_time_by_employees
# MAGIC     WHERE created_on >= current_timestamp() - INTERVAL 30 DAYS
# MAGIC       AND employee_no       IS NOT NULL
# MAGIC       AND item_no           IS NOT NULL
# MAGIC       AND operation_no      IS NOT NULL
# MAGIC       AND machine_center_no IS NOT NULL
# MAGIC       AND prod_line LIKE 'LINE%'
# MAGIC     GROUP BY
# MAGIC         employee_no,
# MAGIC         prod_line,
# MAGIC         cell_line,
# MAGIC         machine_center_no,
# MAGIC         item_no,
# MAGIC         operation_no
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     p.employee_no,
# MAGIC     CONCAT(e.first_name, ' ', e.last_name) AS employee_name,
# MAGIC 
# MAGIC     p.prod_line,
# MAGIC     p.cell_line,
# MAGIC     p.machine_center_no,
# MAGIC     p.item_no,
# MAGIC     p.operation_no,
# MAGIC 
# MAGIC     p.records_count,
# MAGIC     p.total_qty,
# MAGIC 
# MAGIC     r.bc_std_run_time,
# MAGIC     p.avg_plan_run_time_per_pcs,
# MAGIC 
# MAGIC     p.actual_run_time,
# MAGIC     p.actual_efficiency,
# MAGIC 
# MAGIC     AVG(p.actual_run_time) OVER (
# MAGIC         PARTITION BY p.item_no, p.operation_no, p.machine_center_no
# MAGIC     ) AS peer_run_time,
# MAGIC 
# MAGIC     AVG(p.actual_efficiency) OVER (
# MAGIC         PARTITION BY p.item_no, p.operation_no, p.machine_center_no
# MAGIC     ) AS peer_efficiency,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN p.avg_plan_run_time_per_pcs > p.actual_run_time THEN 'PLAN_HIGHER'
# MAGIC         WHEN p.avg_plan_run_time_per_pcs < p.actual_run_time THEN 'ACTUAL_HIGHER'
# MAGIC         ELSE 'EQUAL'
# MAGIC     END AS plan_vs_actual_flag
# MAGIC 
# MAGIC FROM prod_30d p
# MAGIC LEFT JOIN employee_dim e ON p.employee_no = e.employee_no
# MAGIC LEFT JOIN routing_dim r
# MAGIC     ON  p.item_no           = r.item_no
# MAGIC     AND p.operation_no      = r.operation_no
# MAGIC     AND p.machine_center_no = r.machine_center_no
# MAGIC 
# MAGIC ORDER BY
# MAGIC     p.employee_no,
# MAGIC     p.prod_line,
# MAGIC     p.cell_line,
# MAGIC     p.machine_center_no,
# MAGIC     p.item_no,
# MAGIC     p.operation_no;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Delta Late Absent Trend

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.delta.gold_delta_late_absent_trend_employee
# MAGIC 
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     a.Work_Day          AS work_date,
# MAGIC     a.Employee_Code,
# MAGIC     a.sub_department_Eng AS cell_line,
# MAGIC     a.Department_Eng     AS prod_line,
# MAGIC 
# MAGIC     CASE WHEN COALESCE(a.late_time_in_minutes, 0) > 0 THEN 1 ELSE 0 END AS is_late,
# MAGIC     COALESCE(a.late_time_in_minutes, 0)                                  AS late_min,
# MAGIC 
# MAGIC     CASE WHEN COALESCE(a.Count_Day_Absent, 0) > 0 THEN 1 ELSE 0 END     AS is_absent,
# MAGIC     COALESCE(a.Absent_Minutes, 0)                                        AS absent_min
# MAGIC 
# MAGIC   FROM Gold_Commons_Lakehouse.cmn.gold_employee_time_all a
# MAGIC   INNER JOIN Gold_Production_Lakehouse.prod.gold_employee_data_daily b
# MAGIC     ON a.Employee_Code = b.employee_code 
# MAGIC   WHERE a.Work_Day >= DATE_SUB(CURRENT_DATE(), 30)
# MAGIC     AND a.Work_Day IS NOT NULL
# MAGIC     AND a.Department_Eng LIKE 'PROD  LINE  %'  -- ← verify spacing
# MAGIC     AND b.end_date IS NULL
# MAGIC ),
# MAGIC 
# MAGIC daily AS (
# MAGIC   SELECT
# MAGIC     work_date,
# MAGIC     Employee_Code,
# MAGIC     -- Use LAST cell_line/prod_line per day in case of duplicates
# MAGIC     MAX(cell_line)  AS cell_line,
# MAGIC     MAX(prod_line)  AS prod_line,
# MAGIC 
# MAGIC     MAX(is_late)    AS is_late,       -- 1 if late that day
# MAGIC     MAX(late_min)   AS late_min,      -- minutes late that day
# MAGIC 
# MAGIC     MAX(is_absent)  AS is_absent,
# MAGIC     MAX(absent_min) AS absent_min
# MAGIC 
# MAGIC   FROM base
# MAGIC   GROUP BY work_date, Employee_Code
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC   work_date,
# MAGIC   Employee_Code,
# MAGIC   cell_line,
# MAGIC   prod_line,
# MAGIC 
# MAGIC   is_late,
# MAGIC   late_min                    AS late_minutes,
# MAGIC 
# MAGIC   is_absent,
# MAGIC   absent_min                  AS absent_minutes,
# MAGIC 
# MAGIC   -- 7-day rolling: % of last 7 working records that were late/absent
# MAGIC   ROUND(
# MAGIC     AVG(is_late * 1.0) OVER (
# MAGIC       PARTITION BY Employee_Code
# MAGIC       ORDER BY work_date
# MAGIC       ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
# MAGIC     ) * 100, 2)               AS late_rate_7d_ma_pct,
# MAGIC 
# MAGIC   ROUND(
# MAGIC     AVG(is_absent * 1.0) OVER (
# MAGIC       PARTITION BY Employee_Code
# MAGIC       ORDER BY work_date
# MAGIC       ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
# MAGIC     ) * 100, 2)               AS absent_rate_7d_ma_pct,
# MAGIC 
# MAGIC   -- How many actual records back the 7d window covers
# MAGIC   COUNT(*) OVER (
# MAGIC     PARTITION BY Employee_Code
# MAGIC     ORDER BY work_date
# MAGIC     ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
# MAGIC   )                           AS days_in_7d_window,
# MAGIC 
# MAGIC   -- Cumulative totals in the 30-day window
# MAGIC   SUM(is_late) OVER (
# MAGIC     PARTITION BY Employee_Code ORDER BY work_date
# MAGIC     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC   )                           AS cumulative_late_days,
# MAGIC 
# MAGIC   SUM(is_absent) OVER (
# MAGIC     PARTITION BY Employee_Code ORDER BY work_date
# MAGIC     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC   )                           AS cumulative_absent_days
# MAGIC 
# MAGIC FROM daily
# MAGIC ORDER BY Employee_Code, work_date;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Repair By Type

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.delta.gold_repair_by_type
# MAGIC SELECT period, cell_line, defect_type, defect_count, total_prod_line_qty
# MAGIC FROM (
# MAGIC 
# MAGIC     -- all_time
# MAGIC     SELECT
# MAGIC         'all_time' AS period,
# MAGIC         work_center_no AS cell_line,
# MAGIC         defect_type,
# MAGIC         COUNT(*) AS defect_count,
# MAGIC         SUM(COALESCE(prod_line_quantity, 0)) AS total_prod_line_qty
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_prod_repair
# MAGIC     GROUP BY work_center_no, defect_type
# MAGIC 
# MAGIC     UNION ALL
# MAGIC 
# MAGIC     -- this_week
# MAGIC     SELECT
# MAGIC         'this_week' AS period,
# MAGIC         work_center_no AS cell_line,
# MAGIC         defect_type,
# MAGIC         COUNT(*) AS defect_count,
# MAGIC         SUM(COALESCE(prod_line_quantity, 0)) AS total_prod_line_qty
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_prod_repair
# MAGIC     WHERE TO_DATE(created_on) >= DATE_TRUNC('week', CURRENT_DATE())
# MAGIC     GROUP BY work_center_no, defect_type
# MAGIC 
# MAGIC     UNION ALL
# MAGIC 
# MAGIC     -- last_week
# MAGIC     SELECT
# MAGIC         'last_week' AS period,
# MAGIC         work_center_no AS cell_line,
# MAGIC         defect_type,
# MAGIC         COUNT(*) AS defect_count,
# MAGIC         SUM(COALESCE(prod_line_quantity, 0)) AS total_prod_line_qty
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_prod_repair
# MAGIC     WHERE TO_DATE(created_on) >= DATE_SUB(DATE_TRUNC('week', CURRENT_DATE()), 7)
# MAGIC       AND TO_DATE(created_on) <  DATE_TRUNC('week', CURRENT_DATE())
# MAGIC     GROUP BY work_center_no, defect_type
# MAGIC 
# MAGIC     UNION ALL
# MAGIC 
# MAGIC     -- yesterday
# MAGIC     SELECT
# MAGIC         'yesterday' AS period,
# MAGIC         work_center_no AS cell_line,
# MAGIC         defect_type,
# MAGIC         COUNT(*) AS defect_count,
# MAGIC         SUM(COALESCE(prod_line_quantity, 0)) AS total_prod_line_qty
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_prod_repair
# MAGIC     WHERE TO_DATE(created_on) = DATE_SUB(CURRENT_DATE(), 1)
# MAGIC     GROUP BY work_center_no, defect_type
# MAGIC 
# MAGIC )
# MAGIC ORDER BY cell_line, period, defect_count DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Repair By Item

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.delta.gold_repair_by_item
# MAGIC SELECT period, cell_line, FG_group, fg_group_count, total_prod_line_qty
# MAGIC FROM (
# MAGIC 
# MAGIC     -- all_time
# MAGIC     SELECT
# MAGIC         'all_time' AS period,
# MAGIC         work_center_no AS cell_line,
# MAGIC         FG_group,
# MAGIC         COUNT(*) AS fg_group_count,
# MAGIC         SUM(COALESCE(prod_line_quantity, 0)) AS total_prod_line_qty
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_prod_repair
# MAGIC     GROUP BY work_center_no, FG_group
# MAGIC 
# MAGIC     UNION ALL
# MAGIC 
# MAGIC     -- this_week
# MAGIC     SELECT
# MAGIC         'this_week' AS period,
# MAGIC         work_center_no AS cell_line,
# MAGIC         FG_group,
# MAGIC         COUNT(*) AS fg_group_count,
# MAGIC         SUM(COALESCE(prod_line_quantity, 0)) AS total_prod_line_qty
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_prod_repair
# MAGIC     WHERE TO_DATE(created_on) >= DATE_TRUNC('week', CURRENT_DATE())
# MAGIC     GROUP BY work_center_no, FG_group
# MAGIC 
# MAGIC     UNION ALL
# MAGIC 
# MAGIC     -- last_week
# MAGIC     SELECT
# MAGIC         'last_week' AS period,
# MAGIC         work_center_no AS cell_line,
# MAGIC         FG_group,
# MAGIC         COUNT(*) AS fg_group_count,
# MAGIC         SUM(COALESCE(prod_line_quantity, 0)) AS total_prod_line_qty
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_prod_repair
# MAGIC     WHERE TO_DATE(created_on) >= DATE_SUB(DATE_TRUNC('week', CURRENT_DATE()), 7)
# MAGIC       AND TO_DATE(created_on) <  DATE_TRUNC('week', CURRENT_DATE())
# MAGIC     GROUP BY work_center_no, FG_group
# MAGIC 
# MAGIC     UNION ALL
# MAGIC 
# MAGIC     -- yesterday
# MAGIC     SELECT
# MAGIC         'yesterday' AS period,
# MAGIC         work_center_no AS cell_line,
# MAGIC         FG_group,
# MAGIC         COUNT(*) AS fg_group_count,
# MAGIC         SUM(COALESCE(prod_line_quantity, 0)) AS total_prod_line_qty
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_prod_repair
# MAGIC     WHERE TO_DATE(created_on) = DATE_SUB(CURRENT_DATE(), 1)
# MAGIC     GROUP BY work_center_no, FG_group
# MAGIC 
# MAGIC )
# MAGIC ORDER BY cell_line, period, fg_group_count DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Repair Trend by Defect Type

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.delta.gold_repair_trend_by_defect_type
# MAGIC SELECT
# MAGIC   to_date(COALESCE(r_created_on, created_on)) AS repair_date,
# MAGIC   work_center_no AS cell_line,
# MAGIC   defect_type,
# MAGIC   COUNT(*) AS repair_count,
# MAGIC   SUM(COALESCE(prod_line_quantity, 0)) AS total_prod_line_qty
# MAGIC FROM Gold_Production_Lakehouse.prod.gold_prod_repair
# MAGIC WHERE to_date(COALESCE(r_created_on, created_on)) >= date_sub(current_date(), 30)
# MAGIC GROUP BY
# MAGIC   to_date(COALESCE(r_created_on, created_on)),
# MAGIC   work_center_no,
# MAGIC   defect_type
# MAGIC ORDER BY
# MAGIC   repair_date,
# MAGIC   cell_line,
# MAGIC   repair_count DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Repair Trend By Item

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.delta.gold_repair_trend_by_item
# MAGIC SELECT
# MAGIC   to_date(COALESCE(r_created_on, created_on)) AS repair_date,
# MAGIC   work_center_no AS cell_line,
# MAGIC   FG_group,
# MAGIC   COUNT(*) AS repair_count,
# MAGIC   SUM(COALESCE(prod_line_quantity, 0)) AS total_prod_line_qty
# MAGIC FROM Gold_Production_Lakehouse.prod.gold_prod_repair
# MAGIC WHERE to_date(COALESCE(r_created_on, created_on)) >= date_sub(current_date(), 30)
# MAGIC GROUP BY
# MAGIC   to_date(COALESCE(r_created_on, created_on)),
# MAGIC   work_center_no,
# MAGIC   FG_group
# MAGIC ORDER BY
# MAGIC   repair_date,
# MAGIC   cell_line,
# MAGIC   repair_count DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Delta Current Employees

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.delta.gold_delta_current_employees
# MAGIC SELECT employee_code, position, sub_department as cell_line, department as prod_line
# MAGIC FROM Gold_Production_Lakehouse.prod.gold_employee_data_daily
# MAGIC WHERE end_date IS NULL
# MAGIC and department IN ('PROD  LINE  1', 'PROD  LINE  2', 'PROD  LINE  3', 'PROD  LINE  4', 'PROD  LINE  5' )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Delta Employee Pairs

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.delta.gold_delta_employee_pairs AS
# MAGIC WITH first_table AS (
# MAGIC     SELECT DISTINCT employee_code
# MAGIC     FROM Gold_Production_Lakehouse.delta.gold_delta_from_emp_time_summary
# MAGIC     WHERE to_date(created_date) >= date_sub(current_date(), 30)
# MAGIC       AND employee_code LIKE '%,%'
# MAGIC ),
# MAGIC 
# MAGIC split_codes AS (
# MAGIC     SELECT
# MAGIC         employee_code,
# MAGIC         trim(element_at(split(employee_code, ','), 1)) AS emp_1,
# MAGIC         trim(element_at(split(employee_code, ','), 2)) AS emp_2,
# MAGIC         trim(element_at(split(employee_code, ','), 3)) AS emp_3,
# MAGIC         trim(element_at(split(employee_code, ','), 4)) AS emp_4
# MAGIC     FROM first_table
# MAGIC ),
# MAGIC 
# MAGIC actual_employee AS (
# MAGIC     SELECT DISTINCT trim(employee_no) AS employee_no
# MAGIC     FROM Gold_Production_Lakehouse.delta.gold_delta_actual_time_by_employees
# MAGIC     WHERE to_date(created_on) >= date_sub(current_date(), 30)
# MAGIC ),
# MAGIC 
# MAGIC active_employee AS (
# MAGIC     SELECT DISTINCT trim(employee_code) AS employee_code
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_employee_data_daily
# MAGIC     WHERE end_date IS NULL
# MAGIC ),
# MAGIC 
# MAGIC validated AS (
# MAGIC     SELECT
# MAGIC         s.employee_code AS employee_code_with_comma,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN a1.employee_no IS NOT NULL AND e1.employee_code IS NOT NULL THEN s.emp_1
# MAGIC         END AS valid_emp_1,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN a2.employee_no IS NOT NULL AND e2.employee_code IS NOT NULL THEN s.emp_2
# MAGIC         END AS valid_emp_2,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN a3.employee_no IS NOT NULL AND e3.employee_code IS NOT NULL THEN s.emp_3
# MAGIC         END AS valid_emp_3,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN a4.employee_no IS NOT NULL AND e4.employee_code IS NOT NULL THEN s.emp_4
# MAGIC         END AS valid_emp_4
# MAGIC 
# MAGIC     FROM split_codes s
# MAGIC 
# MAGIC     LEFT JOIN actual_employee a1
# MAGIC         ON s.emp_1 = a1.employee_no
# MAGIC     LEFT JOIN actual_employee a2
# MAGIC         ON s.emp_2 = a2.employee_no
# MAGIC     LEFT JOIN actual_employee a3
# MAGIC         ON s.emp_3 = a3.employee_no
# MAGIC     LEFT JOIN actual_employee a4
# MAGIC         ON s.emp_4 = a4.employee_no
# MAGIC 
# MAGIC     LEFT JOIN active_employee e1
# MAGIC         ON s.emp_1 = e1.employee_code
# MAGIC     LEFT JOIN active_employee e2
# MAGIC         ON s.emp_2 = e2.employee_code
# MAGIC     LEFT JOIN active_employee e3
# MAGIC         ON s.emp_3 = e3.employee_code
# MAGIC     LEFT JOIN active_employee e4
# MAGIC         ON s.emp_4 = e4.employee_code
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     employee_code_with_comma,
# MAGIC     valid_list[0] AS col_2,
# MAGIC     valid_list[1] AS col_3,
# MAGIC     valid_list[2] AS col_4,
# MAGIC     valid_list[3] AS col_5
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         employee_code_with_comma,
# MAGIC         filter(
# MAGIC             array(valid_emp_1, valid_emp_2, valid_emp_3, valid_emp_4),
# MAGIC             x -> x IS NOT NULL
# MAGIC         ) AS valid_list
# MAGIC     FROM validated
# MAGIC ) t
# MAGIC WHERE size(valid_list) > 0

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Delta Metal Loss Card

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.delta.gold_delta_metal_loss_card
# MAGIC SELECT
# MAGIC     Line,
# MAGIC 
# MAGIC     -- Yesterday
# MAGIC     COUNT(CASE WHEN TO_DATE(Date) = DATE_SUB(CURRENT_DATE(), 1) AND loss_after_scrap_std_flag = 'UNDER' THEN 1 END) AS yesterday_under,
# MAGIC     COUNT(CASE WHEN TO_DATE(Date) = DATE_SUB(CURRENT_DATE(), 1) AND loss_after_scrap_std_flag = 'OVER'  THEN 1 END) AS yesterday_over,
# MAGIC 
# MAGIC     -- This Week (Monday → today)
# MAGIC     COUNT(CASE WHEN TO_DATE(Date) >= DATE_SUB(CURRENT_DATE(), DAYOFWEEK(CURRENT_DATE()) - 2) AND loss_after_scrap_std_flag = 'UNDER' THEN 1 END) AS this_week_under,
# MAGIC     COUNT(CASE WHEN TO_DATE(Date) >= DATE_SUB(CURRENT_DATE(), DAYOFWEEK(CURRENT_DATE()) - 2) AND loss_after_scrap_std_flag = 'OVER'  THEN 1 END) AS this_week_over,
# MAGIC 
# MAGIC     -- Last Week (Mon → Sun)
# MAGIC     COUNT(CASE WHEN TO_DATE(Date) >= DATE_SUB(CURRENT_DATE(), DAYOFWEEK(CURRENT_DATE()) + 5)
# MAGIC                AND TO_DATE(Date) <  DATE_SUB(CURRENT_DATE(), DAYOFWEEK(CURRENT_DATE()) - 2)
# MAGIC                AND loss_after_scrap_std_flag = 'UNDER' THEN 1 END) AS last_week_under,
# MAGIC     COUNT(CASE WHEN TO_DATE(Date) >= DATE_SUB(CURRENT_DATE(), DAYOFWEEK(CURRENT_DATE()) + 5)
# MAGIC                AND TO_DATE(Date) <  DATE_SUB(CURRENT_DATE(), DAYOFWEEK(CURRENT_DATE()) - 2)
# MAGIC                AND loss_after_scrap_std_flag = 'OVER'  THEN 1 END) AS last_week_over,
# MAGIC 
# MAGIC     -- All Time
# MAGIC     COUNT(CASE WHEN loss_after_scrap_std_flag = 'UNDER' THEN 1 END) AS alltime_under,
# MAGIC     COUNT(CASE WHEN loss_after_scrap_std_flag = 'OVER'  THEN 1 END) AS alltime_over
# MAGIC 
# MAGIC FROM Gold_Inventory_Lakehouse.inv.gold_metal_loss_by_line
# MAGIC GROUP BY Line
# MAGIC ORDER BY Line;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Metal Loss By Line Employee

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.delta.gold_delta_metal_loss_by_line_employee
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC WITH agg AS (
# MAGIC   SELECT
# MAGIC     MAX(ml_last_created_on) AS Date,
# MAGIC     ml_prod_line,
# MAGIC     ml_cell_line,
# MAGIC     ml_employee_no,                                       -- ✅ ADD THIS
# MAGIC     ml_employee_first_name_thai,
# MAGIC     ml_fg_item_no,
# MAGIC     ml_item_no,
# MAGIC     ml_prod_order_no,
# MAGIC     ml_material_type,
# MAGIC     SUM(consumption_casting_gr)  AS total_casting,       -- ★ v2.1: casting แยก
# MAGIC     SUM(consumption_finding_gr)  AS total_finding,       -- ★ v2.1: finding แยก
# MAGIC     SUM(consumption_quantity)    AS total_consumption,    -- รวม (เหมือนเดิม)
# MAGIC     MAX(ml_weight)               AS weight_after_FL,
# MAGIC     MAX(ml_sprue_weight)         AS Scrap,
# MAGIC     MAX(ml_dust_weight)          AS Dust
# MAGIC   FROM Gold_Inventory_Lakehouse.inv.gold_metal_loss_summary
# MAGIC   GROUP BY
# MAGIC     ml_prod_line,
# MAGIC     ml_cell_line,
# MAGIC     ml_employee_no,                                       -- ✅ ADD THIS
# MAGIC     ml_employee_first_name_thai,
# MAGIC     ml_fg_item_no,
# MAGIC     ml_item_no,
# MAGIC     ml_prod_order_no,
# MAGIC     ml_material_type
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC   Date,
# MAGIC   ml_prod_line AS Line,
# MAGIC   ml_cell_line AS Cell,
# MAGIC   ml_employee_no as employee_no,                                       -- ✅ ADD THIS
# MAGIC   ml_employee_first_name_thai AS Name,
# MAGIC   ml_fg_item_no AS FG,
# MAGIC   ml_item_no AS item_no,
# MAGIC   ml_prod_order_no AS Prod,
# MAGIC   ml_material_type AS Metal,
# MAGIC 
# MAGIC   total_casting,                                         -- ★ (0a) Casting Consumption
# MAGIC   total_finding,                                         -- ★ (0b) Finding Consumption
# MAGIC   total_consumption,                                     -- (1) Total Consumption (casting + finding)
# MAGIC   weight_after_FL,                                       -- (2) FL Weight
# MAGIC   Scrap,                                                 -- (3) FL Scrap
# MAGIC   Dust,                                                  -- (4) FL Dust
# MAGIC 
# MAGIC   (weight_after_FL + Scrap + Dust) AS total_return,      -- (5) Total Return
# MAGIC 
# MAGIC   (weight_after_FL * 0.02) AS FL_allowance,              -- (7) +2% FL Weight
# MAGIC 
# MAGIC   (weight_after_FL + Scrap + Dust) + (weight_after_FL * 0.02)
# MAGIC       AS total_with_allowance,                           -- (8) Total + Allowance
# MAGIC 
# MAGIC   total_consumption - ((weight_after_FL + Scrap + Dust) + (weight_after_FL * 0.02))
# MAGIC       AS loss_g,                                         -- (9) Loss (g)
# MAGIC 
# MAGIC   (total_consumption - ((weight_after_FL + Scrap + Dust) + (weight_after_FL * 0.02)))
# MAGIC       / NULLIF((weight_after_FL + Scrap + Dust), 0) AS loss_pct,        -- (10) Loss %
# MAGIC 
# MAGIC   CASE
# MAGIC     WHEN (total_consumption - ((weight_after_FL + Scrap + Dust) + (weight_after_FL * 0.02)))
# MAGIC          / NULLIF((weight_after_FL + Scrap + Dust), 0) > 0.02
# MAGIC     THEN 'Missing'
# MAGIC     ELSE 'OK'
# MAGIC   END AS loss_std_flag
# MAGIC 
# MAGIC FROM agg;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Delta Metal Loss By Employee

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.delta.gold_delta_metal_loss_by_employee_trend AS
# MAGIC SELECT
# MAGIC     TO_DATE(Date)                                                                   AS day,
# MAGIC     Line,
# MAGIC     employee_no,
# MAGIC     Name,
# MAGIC 
# MAGIC     -- Counts
# MAGIC     COUNT(CASE WHEN loss_std_flag = 'UNDER' THEN 1 END)                AS under_count,
# MAGIC     COUNT(CASE WHEN loss_std_flag = 'OVER'  THEN 1 END)                AS over_count,
# MAGIC 
# MAGIC     -- Total loss_after_dust_g
# MAGIC     SUM(CASE WHEN loss_std_flag = 'UNDER' THEN loss_g END)  AS under_loss_g,
# MAGIC     SUM(CASE WHEN loss_std_flag = 'OVER'  THEN loss_g END)  AS over_loss_g,
# MAGIC 
# MAGIC     -- Average loss_after_dust_g
# MAGIC     ROUND(AVG(CASE WHEN loss_std_flag = 'UNDER' THEN loss_g END), 4) AS under_avg_loss_g,
# MAGIC     ROUND(AVG(CASE WHEN loss_std_flag = 'OVER'  THEN loss_g END), 4) AS over_avg_loss_g
# MAGIC 
# MAGIC FROM Gold_Production_Lakehouse.delta.gold_delta_metal_loss_by_line_employee
# MAGIC WHERE Line LIKE 'LINE %'
# MAGIC   AND TO_DATE(Date) >= DATE_SUB(CURRENT_DATE(), 30)
# MAGIC 
# MAGIC GROUP BY TO_DATE(Date), Line, employee_no, Name
# MAGIC ORDER BY Line, employee_no, day;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Delta Operation Sequence

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.delta.gold_delta_operation_sequence AS 
# MAGIC SELECT
# MAGIC     `No.` as operation,
# MAGIC     `Operation Group` as operation_group,
# MAGIC     `Operation Sequence` as operation_sequence
# MAGIC FROM Silver_BC_Lakehouse.bc.`Machine Center`
# MAGIC WHERE Blocked = 0
# MAGIC   AND `Department Group` = "PRODUCTION"
# MAGIC ORDER BY `Operation Sequence`

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Delta Metal Loss by Cell

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.delta.gold_delta_metal_loss_by_cell_trend
# MAGIC AS
# MAGIC SELECT
# MAGIC     TO_DATE(Date)                                                              AS day,
# MAGIC     Line,
# MAGIC     Cell,
# MAGIC 
# MAGIC     -- Counts
# MAGIC     COUNT(CASE WHEN loss_std_flag = 'UNDER' THEN 1 END)           AS under_count,
# MAGIC     COUNT(CASE WHEN loss_std_flag = 'OVER'  THEN 1 END)           AS over_count,
# MAGIC 
# MAGIC     -- Total loss_after_dust_g
# MAGIC     SUM(CASE WHEN loss_std_flag = 'UNDER' THEN loss_g END) AS under_loss_g,
# MAGIC     SUM(CASE WHEN loss_std_flag = 'OVER'  THEN loss_g END) AS over_loss_g,
# MAGIC 
# MAGIC     -- Average loss_after_dust_g
# MAGIC     ROUND(AVG(CASE WHEN loss_std_flag = 'UNDER' THEN loss_g END), 4) AS under_avg_loss_g,
# MAGIC     ROUND(AVG(CASE WHEN loss_std_flag = 'OVER'  THEN loss_g END), 4) AS over_avg_loss_g
# MAGIC 
# MAGIC FROM Gold_Production_Lakehouse.delta.gold_delta_metal_loss_by_line_employee
# MAGIC WHERE Line LIKE 'LINE %'
# MAGIC   AND TO_DATE(Date) >= DATE_SUB(CURRENT_DATE(), 30)
# MAGIC 
# MAGIC GROUP BY TO_DATE(Date), Line, Cell
# MAGIC ORDER BY Line, Cell, day;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
