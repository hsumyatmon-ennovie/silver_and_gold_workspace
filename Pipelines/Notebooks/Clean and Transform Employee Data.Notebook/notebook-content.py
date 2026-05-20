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
# META           "id": "09b7e61e-3d0d-48e6-a161-4f7076e12826"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Clean Silver Data

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
    "Silver_Commons_Lakehouse",
    "Gold_Production_Lakehouse",
   
]

# Tables to dedupe in each lakehouse
# keys = partition columns for row_number()
# order = column used to pick the "latest" within each partition
DEDUPE_SPECS = [
    # prod schema
    {
        "schema": "cmn",
        "table":  "silver_employee_data_full_replace",
        "keys":   ["Employee_Code"],
        "order":  "Age",
    },
    {
        "schema": "cmn",
        "table":  "silver_employee_time_full_replace",
        "keys":   ["Work_Day", "Employee_Code"],
        "order":  "actual_date_time_out",
    },
    {
        "schema": "cmn",
        "table":  "silver_employee_time",
        "keys":   ["Work_Day", "Employee_Code"],
        "order":  "actual_date_time_out",
    },
    {
        "schema": "prod",
        "table":  "gold_employee_data_daily",
        "keys":   ["employee_code"],
        "order":  "age",
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

# MARKDOWN ********************

# # Gold Employeee Time Fixed

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_employee_time_fixed AS
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     t.*,
# MAGIC     CAST(t.Work_Day AS DATE) AS WorkDate
# MAGIC   FROM Silver_Commons_Lakehouse.cmn.silver_employee_time t
# MAGIC ),
# MAGIC 
# MAGIC -- 1 row per employee per day
# MAGIC daily AS (
# MAGIC   SELECT
# MAGIC     MAX(Company_Name_Eng) AS Company_Name_Eng,
# MAGIC     MAX(Division_Eng) AS Division_Eng,
# MAGIC     MAX(Department_Eng) AS Department_Eng,
# MAGIC     MAX(Position_Eng) AS Position_Eng,
# MAGIC     MAX(sub_department_Eng) AS sub_department_Eng,
# MAGIC     MAX(level_employee_Eng) AS level_employee_Eng,
# MAGIC     MAX(Approval_Level_Thai) AS Approval_Level_Thai,
# MAGIC 
# MAGIC     Employee_Code,
# MAGIC 
# MAGIC     MAX(First_Name_Thai) AS First_Name_Thai,
# MAGIC     MAX(First_Name_Eng) AS First_Name_Eng,
# MAGIC     MAX(Last_Name_Thai) AS Last_Name_Thai,
# MAGIC     MAX(Last_Name_Eng) AS Last_Name_Eng,
# MAGIC     MAX(Identity_ID) AS Identity_ID,
# MAGIC 
# MAGIC     MAX(Shift_Name) AS Shift_Name,
# MAGIC     MAX(ShiftCode) AS ShiftCode,
# MAGIC     MAX(Standard_Time_In) AS Standard_Time_In,
# MAGIC     MAX(Standard_Time_Out) AS Standard_Time_Out,
# MAGIC 
# MAGIC     MAX(Work_Day) AS Work_Day,
# MAGIC     WorkDate,
# MAGIC 
# MAGIC     MIN(actual_date_time_in) AS actual_date_time_in,
# MAGIC     MAX(actual_date_time_out) AS actual_date_time_out,
# MAGIC 
# MAGIC     MAX(late_time_in_minutes) AS late_time_in_minutes,
# MAGIC     MAX(before_time_out_minutes) AS before_time_out_minutes,
# MAGIC 
# MAGIC     MAX(Count_Day_Include) AS Count_Day_Include,
# MAGIC     MAX(Count_Day_Absent) AS Count_Day_Absent,
# MAGIC     MAX(Total_Leave_Days) AS Total_Leave_Days,
# MAGIC     MAX(Total_Leave_Hours) AS Total_Leave_Hours,
# MAGIC 
# MAGIC     MAX(Break_Start) AS Break_Start,
# MAGIC     MAX(Break_End) AS Break_End,
# MAGIC 
# MAGIC     MAX(Overtime_Type) AS Overtime_Type,
# MAGIC     MAX(OT_Hour_1) AS OT_Hour_1,
# MAGIC     MAX(OT_Hour_2) AS OT_Hour_2,
# MAGIC     MAX(OT_Hour_3) AS OT_Hour_3,
# MAGIC     MAX(OT_Hour_4) AS OT_Hour_4,
# MAGIC     MAX(OT_Hour_5) AS OT_Hour_5,
# MAGIC     MAX(OT_Hour_6) AS OT_Hour_6,
# MAGIC 
# MAGIC     MAX(Seat_No) AS Seat_No
# MAGIC 
# MAGIC   FROM base
# MAGIC   GROUP BY Employee_Code, WorkDate
# MAGIC ),
# MAGIC 
# MAGIC remark_src AS (
# MAGIC   SELECT
# MAGIC     Employee_Code,
# MAGIC     CAST(Work_Day AS DATE) AS Work_Day,
# MAGIC     MAX(Day_Remark) AS Day_Remark
# MAGIC   FROM Silver_Commons_Lakehouse.cmn.silver_employee_time_full_replace
# MAGIC   GROUP BY
# MAGIC     Employee_Code,
# MAGIC     CAST(Work_Day AS DATE)
# MAGIC ),
# MAGIC 
# MAGIC wd AS (
# MAGIC   SELECT
# MAGIC     d.*,
# MAGIC     r.Day_Remark,
# MAGIC     UPPER(date_format(d.WorkDate, 'EEEE')) AS DayName
# MAGIC   FROM daily d
# MAGIC   LEFT JOIN remark_src r
# MAGIC     ON d.Employee_Code = r.Employee_Code
# MAGIC    AND CAST(d.Work_Day AS DATE) = r.Work_Day
# MAGIC ),
# MAGIC 
# MAGIC calc AS (
# MAGIC   SELECT
# MAGIC     wd.*,
# MAGIC 
# MAGIC     timestampadd(HOUR, 8, CAST(WorkDate AS TIMESTAMP)) AS Start0800,
# MAGIC     timestampadd(MINUTE, 20,
# MAGIC       timestampadd(HOUR, 18, CAST(WorkDate AS TIMESTAMP))
# MAGIC     ) AS End1820,
# MAGIC     timestampadd(MINUTE, 40,
# MAGIC       timestampadd(HOUR, 18, CAST(WorkDate AS TIMESTAMP))
# MAGIC     ) AS OTStart1840,
# MAGIC     timestampadd(HOUR, 17, CAST(WorkDate AS TIMESTAMP)) AS End1700
# MAGIC   FROM wd
# MAGIC ),
# MAGIC 
# MAGIC final_calc AS (
# MAGIC   SELECT
# MAGIC     calc.*,
# MAGIC 
# MAGIC     CASE
# MAGIC       WHEN DayName IN ('SATURDAY','SUNDAY') THEN 0
# MAGIC       ELSE
# MAGIC         CASE
# MAGIC           WHEN actual_date_time_in IS NULL
# MAGIC             AND actual_date_time_out IS NULL THEN 0
# MAGIC           WHEN actual_date_time_in IS NOT NULL
# MAGIC             AND actual_date_time_out IS NULL THEN 280
# MAGIC           ELSE
# MAGIC             CASE
# MAGIC               WHEN (
# MAGIC                 timestampdiff(
# MAGIC                   MINUTE,
# MAGIC                   CASE
# MAGIC                     WHEN actual_date_time_in < Start0800 THEN Start0800
# MAGIC                     WHEN actual_date_time_in > End1820 THEN End1820
# MAGIC                     ELSE actual_date_time_in
# MAGIC                   END,
# MAGIC                   CASE
# MAGIC                     WHEN actual_date_time_out > End1820 THEN End1820
# MAGIC                     WHEN actual_date_time_out < Start0800 THEN Start0800
# MAGIC                     ELSE actual_date_time_out
# MAGIC                   END
# MAGIC                 ) - 60
# MAGIC               ) > 0
# MAGIC               THEN
# MAGIC                 LEAST(
# MAGIC                   560,
# MAGIC                   timestampdiff(
# MAGIC                     MINUTE,
# MAGIC                     CASE
# MAGIC                       WHEN actual_date_time_in < Start0800 THEN Start0800
# MAGIC                       WHEN actual_date_time_in > End1820 THEN End1820
# MAGIC                       ELSE actual_date_time_in
# MAGIC                     END,
# MAGIC                     CASE
# MAGIC                       WHEN actual_date_time_out > End1820 THEN End1820
# MAGIC                       WHEN actual_date_time_out < Start0800 THEN Start0800
# MAGIC                       ELSE actual_date_time_out
# MAGIC                     END
# MAGIC                   ) - 60
# MAGIC                 )
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC         END
# MAGIC     END AS Normal_Work_Minutes,
# MAGIC 
# MAGIC     CASE
# MAGIC       WHEN DayName IN ('SATURDAY','SUNDAY') THEN
# MAGIC         CASE
# MAGIC           WHEN actual_date_time_in IS NULL
# MAGIC             AND actual_date_time_out IS NULL THEN 0
# MAGIC           WHEN actual_date_time_in IS NOT NULL
# MAGIC             AND actual_date_time_out IS NULL THEN 280
# MAGIC           ELSE
# MAGIC             CASE
# MAGIC               WHEN (
# MAGIC                 timestampdiff(
# MAGIC                   MINUTE,
# MAGIC                   CASE
# MAGIC                     WHEN actual_date_time_in < Start0800 THEN Start0800
# MAGIC                     WHEN actual_date_time_in > End1700 THEN End1700
# MAGIC                     ELSE actual_date_time_in
# MAGIC                   END,
# MAGIC                   CASE
# MAGIC                     WHEN actual_date_time_out > End1700 THEN End1700
# MAGIC                     WHEN actual_date_time_out < Start0800 THEN Start0800
# MAGIC                     ELSE actual_date_time_out
# MAGIC                   END
# MAGIC                 ) - 60
# MAGIC               ) >= 30
# MAGIC               THEN (
# MAGIC                 timestampdiff(
# MAGIC                   MINUTE,
# MAGIC                   CASE
# MAGIC                     WHEN actual_date_time_in < Start0800 THEN Start0800
# MAGIC                     WHEN actual_date_time_in > End1700 THEN End1700
# MAGIC                     ELSE actual_date_time_in
# MAGIC                   END,
# MAGIC                   CASE
# MAGIC                     WHEN actual_date_time_out > End1700 THEN End1700
# MAGIC                     WHEN actual_date_time_out < Start0800 THEN Start0800
# MAGIC                     ELSE actual_date_time_out
# MAGIC                   END
# MAGIC                 ) - 60
# MAGIC               )
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC         END
# MAGIC       ELSE
# MAGIC         CASE
# MAGIC           WHEN actual_date_time_out IS NULL THEN 0
# MAGIC           ELSE
# MAGIC             CASE
# MAGIC               WHEN timestampdiff(MINUTE, OTStart1840, actual_date_time_out) >= 30
# MAGIC               THEN timestampdiff(MINUTE, OTStart1840, actual_date_time_out)
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC         END
# MAGIC     END AS OT_Work_Minutes
# MAGIC   FROM calc
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC   t.*,
# MAGIC   (Normal_Work_Minutes + OT_Work_Minutes) AS Total_Work_Minutes,
# MAGIC   CASE
# MAGIC     WHEN DayName IN ('SATURDAY','SUNDAY') THEN 0
# MAGIC     ELSE GREATEST(560 - Normal_Work_Minutes, 0)
# MAGIC   END AS Absent_Minutes
# MAGIC FROM final_calc t;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Employee Time

# CELL ********************

# ==============================================================
# GOLD: Gold_Production_Lakehouse.prod.gold_employee_time — Incremental
#  - Source changed to: Gold_Production_Lakehouse.prod.gold_employee_time_fixed
#  - Filters: Division_Eng='PRODUCTION' AND Department_Eng present (not null/blank/'-')
#  - Leave_Weekday (MON..SUN) + Leave_Weekday_Order (1..7)
#  - Keep time-only outputs for Standard/Actual (HH:mm:ss) (best effort parsing)
# ==============================================================

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

# -------------------------
# TARGET
# -------------------------
TGT_DB     = "Gold_Production_Lakehouse"
TGT_SCHEMA = "prod"
TGT_TABLE  = "gold_employee_time"
TGT_FQN    = f"{TGT_DB}.`{TGT_SCHEMA}`.`{TGT_TABLE}`"

# -------------------------
# SOURCE (UPDATED)
# -------------------------
SRC = "Gold_Production_Lakehouse.prod.gold_employee_time_fixed"

# -------------------------
# INCREMENTAL SETTINGS
# -------------------------
MODCOL       = "_modified_any"
LOOKBACK_MIN = 90
KEYS         = ["Employee_Code", "Work_Day"]

# -------------------------
# HELPERS
# -------------------------
def table_exists_fqn(fqn: str) -> bool:
    try:
        return spark.catalog.tableExists(fqn)
    except Exception:
        return False

def get_last_ts_fqn(fqn: str, col: str):
    if not table_exists_fqn(fqn):
        return None
    row = spark.table(fqn).select(F.max(F.col(col)).alias("wm")).first()
    return row["wm"] if row else None

def create_table_if_missing(fqn: str, df):
    (df.limit(0)
       .write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema","true")
       .saveAsTable(fqn))
    spark.sql(f"""
      ALTER TABLE {fqn}
      SET TBLPROPERTIES (
        delta.autoOptimize.optimizeWrite = true,
        delta.autoOptimize.autoCompact   = true
      )
    """)

def build_watermark_expr(src_df):
    cands = []
    for raw in ["SinkModifiedOn","modified_on","modifiedon","_commit_timestamp","load_ts","created_on"]:
        if raw in src_df.columns:
            cands.append(F.to_timestamp(F.col(raw)))
    return F.greatest(*cands) if cands else F.current_timestamp()

def hhmmss(colname: str):
    """
    Best-effort time-only formatting:
    - If it's a timestamp/datetime string -> parse + HH:mm:ss
    - If already HH:mm:ss -> will still pass through (to_timestamp may null depending on engine),
      so we coalesce with a regex-extracted HH:mm:ss if present.
    """
    ts = F.to_timestamp(F.col(colname))
    fmt = F.date_format(ts, "HH:mm:ss")
    # fallback if already contains HH:mm:ss
    fallback = F.regexp_extract(F.col(colname).cast("string"), r"(\d{2}:\d{2}:\d{2})", 1)
    return F.when(fmt.isNotNull(), fmt).otherwise(F.when(F.length(fallback) > 0, fallback).otherwise(F.lit(None)))

# -------------------------
# 1) LOAD + BASE FILTERS
# -------------------------
s = spark.table(SRC)

div_ok = (F.col("Division_Eng") == "PRODUCTION") & (F.trim(F.col("Division_Eng")) != "-")
dept_ok = (
    F.col("Department_Eng").isNotNull() &
    (F.length(F.trim(F.col("Department_Eng"))) > 0) &
    (F.trim(F.col("Department_Eng")) != "-")
)

src = s.where(div_ok & dept_ok)

# -------------------------
# 2) DERIVATIONS (time-only + memo + weekday)
# -------------------------
typed = (
    src
    # published time-only strings (HH:mm:ss)
    .withColumn("Standard_Time_In_fix",   hhmmss("Standard_Time_In"))
    .withColumn("Standard_Time_Out_fix",  hhmmss("Standard_Time_Out"))
    .withColumn("actual_time_in_fix",     hhmmss("actual_date_time_in"))
    .withColumn("actual_time_out_fix",    hhmmss("actual_date_time_out"))
    # memo helpers (keep your existing logic)
    .withColumn("Day_Remark_Trim", F.trim(F.col("Day_Remark")))
    .withColumn("Day_Memo_reason", F.regexp_extract(F.col("Day_Remark_Trim"), r'^([^\s(]+)', 1))
)

# Map reason -> EN (same mapping you had)
rb = F.col("Day_Memo_reason")
reason_en = (
    typed.withColumn(
        "Day_Memo_reason_EN",
        F.when(rb.isin("ลากิจไม่รับค่างจ้างหักเงิน","ลาคลอดไม่รับค่าจ้างหักเงิน","ลากิจไม่รับค่างจ้าง","ลาป่วยไม่รับค่าจ้างหักเงิน"), "U/P")
         .when(rb == "วันหยุดปกติ", "RH")
         .when(rb == "วันหยุดประจำปี", "AH")
         .when(rb == "ลาคลอด", "ML")
         .when(rb == "ลากิจ", "BL")
         .when(rb == "ลาพักร้อน", "VL")
         .when(rb == "ลาป่วย", "SL")
         .when(rb == "สาย", "L")
         .when(rb.isin("ออกก่อนเกินคิดหักขาดงาน","ขาดงาน","LateเกินคิดหักAbsent from Work","LเกินคิดหักABS"), "ABS")
         .otherwise(rb)
    )
)

# Leave_Weekday + Order (ISO Mon=1..Sun=7) based on Work_Day
work_day_date = F.to_date(F.col("Work_Day"))
weekday_iso = F.when(F.dayofweek(work_day_date) == 1, 7).otherwise(F.dayofweek(work_day_date) - 1)

reason_en = (
    reason_en
    .withColumn(
        "Leave_Weekday",
        F.when(weekday_iso == 1, "MON")
         .when(weekday_iso == 2, "TUE")
         .when(weekday_iso == 3, "WED")
         .when(weekday_iso == 4, "THU")
         .when(weekday_iso == 5, "FRI")
         .when(weekday_iso == 6, "SAT")
         .when(weekday_iso == 7, "SUN")
    )
    .withColumn("Leave_Weekday_Order", weekday_iso.cast("int"))
)

# -------------------------
# 3) WATERMARK
# -------------------------
wm_expr = build_watermark_expr(s)

# -------------------------
# 4) FINAL PROJECTION (UPDATED to "show these cols")
# -------------------------
final_src = (
    reason_en.select(
        "Company_Name_Eng",
        "Division_Eng",
        "Department_Eng",
        "Position_Eng",
        "sub_department_Eng",
        "level_employee_Eng",
        "Approval_Level_Thai",
        "Employee_Code",
        "First_Name_Thai",
        "First_Name_Eng",
        "Last_Name_Thai",
        "Last_Name_Eng",
        "Identity_ID",
        "Shift_Name",
        "ShiftCode",

        # time-only outputs (HH:mm:ss)
        F.col("Standard_Time_In_fix").alias("Standard_Time_In"),
        F.col("Standard_Time_Out_fix").alias("Standard_Time_Out"),

        "Work_Day",
        F.col("actual_time_in_fix").alias("actual_date_time_in"),
        F.col("actual_time_out_fix").alias("actual_date_time_out"),

        "late_time_in_minutes",
        "before_time_out_minutes",
        "Count_Day_Include",
        "Count_Day_Absent",
        "Total_Leave_Days",
        "Total_Leave_Hours",
        "Break_Start",
        "Break_End",
        "Overtime_Type",
        "OT_Hour_1",
        "OT_Hour_2",
        "OT_Hour_3",
        "OT_Hour_4",
        "OT_Hour_5",
        "OT_Hour_6",
        "Day_Remark",
        "Seat_No",

        # extra cols from gold_employee_time_fixed
        # "WorkDate",
        # "DayName",
        # "Start0800",
        # "End1820",
        # "OTStart1840",
        # "End1700",
        "Normal_Work_Minutes",
        "OT_Work_Minutes",
        "Total_Work_Minutes",
        "Absent_Minutes",

        # your derived weekday cols
        "Leave_Weekday",
        "Leave_Weekday_Order",

        # memo cols (optional but useful)
        "Day_Memo_reason",
        "Day_Memo_reason_EN",

        # watermark
        F.to_timestamp(wm_expr).alias(MODCOL),
    )
    .dropDuplicates()
)

# -------------------------
# 5) INCREMENTAL WINDOW
# -------------------------
last_ts = get_last_ts_fqn(TGT_FQN, MODCOL)
staged = final_src if last_ts is None else final_src.filter(
    F.col(MODCOL) >= (F.lit(last_ts).cast("timestamp") - F.expr(f"INTERVAL {LOOKBACK_MIN} MINUTES"))
)

# create table if missing (empty schema bootstrap)
if not table_exists_fqn(TGT_FQN):
    boot = (staged
            .withColumn("updated_at", F.col(MODCOL))
            .withColumn("load_ts", F.current_timestamp())
            .withColumn("source_system", F.lit("gold_employee_time_fixed"))
            .withColumn("row_hash", F.lit(None).cast("string")))
    create_table_if_missing(TGT_FQN, boot)

# -------------------------
# 6) DEDUPE BY KEYS
# -------------------------
w = Window.partitionBy(*KEYS).orderBy(F.col(MODCOL).desc_nulls_last())
staged1 = (staged
           .withColumn("_rn", F.row_number().over(w))
           .filter(F.col("_rn") == 1)
           .drop("_rn"))

# -------------------------
# 7) SYSTEM COLS + HASH
# -------------------------
content_cols = [c for c in staged1.columns]  # includes MODCOL
staged_h = (
    staged1
      .withColumn("updated_at", F.col(MODCOL))
      .withColumn("load_ts", F.current_timestamp())
      .withColumn("source_system", F.lit("gold_employee_time_fixed"))
      .withColumn(
          "row_hash",
          F.sha2(F.concat_ws("§", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in content_cols]), 256)
      )
)

# -------------------------
# 8) MERGE
# -------------------------
if staged_h.rdd.isEmpty():
    print("No employee-time rows to merge. ✅")
else:
    tgt = DeltaTable.forName(spark, TGT_FQN)
    on_clause = " AND ".join([f"t.{k} <=> s.{k}" for k in KEYS])

    # columns to update/insert (exclude keys handled explicitly + system cols handled below)
    business_cols = [
        "Company_Name_Eng","Division_Eng","Department_Eng","Position_Eng","sub_department_Eng",
        "level_employee_Eng","Approval_Level_Thai","First_Name_Thai","First_Name_Eng","Last_Name_Thai",
        "Last_Name_Eng","Identity_ID","Shift_Name","ShiftCode","Standard_Time_In","Standard_Time_Out",
        "actual_date_time_in","actual_date_time_out","late_time_in_minutes","before_time_out_minutes",
        "Count_Day_Include","Count_Day_Absent","Total_Leave_Days","Total_Leave_Hours","Break_Start","Break_End",
        "Overtime_Type","OT_Hour_1","OT_Hour_2","OT_Hour_3","OT_Hour_4","OT_Hour_5","OT_Hour_6",
        "Day_Remark","Seat_No",
        # "WorkDate","DayName","Start0800","End1820","OTStart1840","End1700",
        "Normal_Work_Minutes","OT_Work_Minutes","Total_Work_Minutes","Absent_Minutes",
        "Leave_Weekday","Leave_Weekday_Order","Day_Memo_reason","Day_Memo_reason_EN",
        MODCOL
    ]

    set_map = {c: f"s.{c}" for c in business_cols}
    set_map.update({
        "updated_at": "s.updated_at",
        "load_ts": "s.load_ts",
        "source_system": "s.source_system",
        "row_hash": "s.row_hash",
    })

    insert_map = {k: f"s.{k}" for k in (KEYS + business_cols)}
    insert_map.update({
        "updated_at": "s.updated_at",
        "load_ts": "s.load_ts",
        "source_system": "s.source_system",
        "row_hash": "s.row_hash",
    })

    (
        tgt.alias("t")
           .merge(staged_h.alias("s"), on_clause)
           .whenMatchedUpdate(condition="t.row_hash <> s.row_hash", set=set_map)
           .whenNotMatchedInsert(values=insert_map)
           .execute()
    )

# Optional maintenance
# spark.sql(f"OPTIMIZE {TGT_FQN} ZORDER BY (Employee_Code, Work_Day)")
# spark.sql(f"VACUUM {TGT_FQN} RETAIN 168 HOURS")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Employee Time All

# CELL ********************

# ==============================================================
# GOLD: Gold_Production_Lakehouse.prod.gold_employee_time — Incremental
#  - Leave_Weekday (MON..SUN) + Leave_Weekday_Order (1..7)
#  - Keep time-only outputs for Standard/Actual (HH:mm:ss) (best effort parsing)
# ==============================================================

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

# -------------------------
# TARGET
# -------------------------
TGT_DB     = "Gold_Commons_Lakehouse"
TGT_SCHEMA = "cmn"
TGT_TABLE  = "gold_employee_time_all"
TGT_FQN    = f"{TGT_DB}.`{TGT_SCHEMA}`.`{TGT_TABLE}`"

# -------------------------
# SOURCE (UPDATED)
# -------------------------
SRC = "Gold_Production_Lakehouse.prod.gold_employee_time_fixed"

# -------------------------
# INCREMENTAL SETTINGS
# -------------------------
MODCOL       = "_modified_any"
LOOKBACK_MIN = 90
KEYS         = ["Employee_Code", "Work_Day"]

# -------------------------
# HELPERS
# -------------------------
def table_exists_fqn(fqn: str) -> bool:
    try:
        return spark.catalog.tableExists(fqn)
    except Exception:
        return False

def get_last_ts_fqn(fqn: str, col: str):
    if not table_exists_fqn(fqn):
        return None
    row = spark.table(fqn).select(F.max(F.col(col)).alias("wm")).first()
    return row["wm"] if row else None

def create_table_if_missing(fqn: str, df):
    (df.limit(0)
       .write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema","true")
       .saveAsTable(fqn))
    spark.sql(f"""
      ALTER TABLE {fqn}
      SET TBLPROPERTIES (
        delta.autoOptimize.optimizeWrite = true,
        delta.autoOptimize.autoCompact   = true
      )
    """)

def build_watermark_expr(src_df):
    cands = []
    for raw in ["SinkModifiedOn","modified_on","modifiedon","_commit_timestamp","load_ts","created_on"]:
        if raw in src_df.columns:
            cands.append(F.to_timestamp(F.col(raw)))
    return F.greatest(*cands) if cands else F.current_timestamp()

def hhmmss(colname: str):
    """
    Best-effort time-only formatting:
    - If it's a timestamp/datetime string -> parse + HH:mm:ss
    - If already HH:mm:ss -> will still pass through (to_timestamp may null depending on engine),
      so we coalesce with a regex-extracted HH:mm:ss if present.
    """
    ts = F.to_timestamp(F.col(colname))
    fmt = F.date_format(ts, "HH:mm:ss")
    # fallback if already contains HH:mm:ss
    fallback = F.regexp_extract(F.col(colname).cast("string"), r"(\d{2}:\d{2}:\d{2})", 1)
    return F.when(fmt.isNotNull(), fmt).otherwise(F.when(F.length(fallback) > 0, fallback).otherwise(F.lit(None)))

# -------------------------
# 1) LOAD + BASE FILTERS
# -------------------------
s = spark.table(SRC)

src = s

# -------------------------
# 2) DERIVATIONS (time-only + memo + weekday)
# -------------------------
typed = (
    src
    # published time-only strings (HH:mm:ss)
    .withColumn("Standard_Time_In_fix",   hhmmss("Standard_Time_In"))
    .withColumn("Standard_Time_Out_fix",  hhmmss("Standard_Time_Out"))
    .withColumn("actual_time_in_fix",     hhmmss("actual_date_time_in"))
    .withColumn("actual_time_out_fix",    hhmmss("actual_date_time_out"))
    # memo helpers (keep your existing logic)
    .withColumn("Day_Remark_Trim", F.trim(F.col("Day_Remark")))
    .withColumn("Day_Memo_reason", F.regexp_extract(F.col("Day_Remark_Trim"), r'^([^\s(]+)', 1))
)

# Map reason -> EN (same mapping you had)
rb = F.col("Day_Memo_reason")
reason_en = (
    typed.withColumn(
        "Day_Memo_reason_EN",
        F.when(rb.isin("ลากิจไม่รับค่างจ้างหักเงิน","ลาคลอดไม่รับค่าจ้างหักเงิน","ลากิจไม่รับค่างจ้าง","ลาป่วยไม่รับค่าจ้างหักเงิน"), "U/P")
         .when(rb == "วันหยุดปกติ", "RH")
         .when(rb == "วันหยุดประจำปี", "AH")
         .when(rb == "ลาคลอด", "ML")
         .when(rb == "ลากิจ", "BL")
         .when(rb == "ลาพักร้อน", "VL")
         .when(rb == "ลาป่วย", "SL")
         .when(rb == "สาย", "L")
         .when(rb.isin("ออกก่อนเกินคิดหักขาดงาน","ขาดงาน","LateเกินคิดหักAbsent from Work","LเกินคิดหักABS"), "ABS")
         .otherwise(rb)
    )
)

# Leave_Weekday + Order (ISO Mon=1..Sun=7) based on Work_Day
work_day_date = F.to_date(F.col("Work_Day"))
weekday_iso = F.when(F.dayofweek(work_day_date) == 1, 7).otherwise(F.dayofweek(work_day_date) - 1)

reason_en = (
    reason_en
    .withColumn(
        "Leave_Weekday",
        F.when(weekday_iso == 1, "MON")
         .when(weekday_iso == 2, "TUE")
         .when(weekday_iso == 3, "WED")
         .when(weekday_iso == 4, "THU")
         .when(weekday_iso == 5, "FRI")
         .when(weekday_iso == 6, "SAT")
         .when(weekday_iso == 7, "SUN")
    )
    .withColumn("Leave_Weekday_Order", weekday_iso.cast("int"))
)

# -------------------------
# 3) WATERMARK
# -------------------------
wm_expr = build_watermark_expr(s)

# -------------------------
# 4) FINAL PROJECTION (UPDATED to "show these cols")
# -------------------------
final_src = (
    reason_en.select(
        "Company_Name_Eng",
        "Division_Eng",
        "Department_Eng",
        "Position_Eng",
        "sub_department_Eng",
        "level_employee_Eng",
        "Approval_Level_Thai",
        "Employee_Code",
        "First_Name_Thai",
        "First_Name_Eng",
        "Last_Name_Thai",
        "Last_Name_Eng",
        "Identity_ID",
        "Shift_Name",
        "ShiftCode",

        # time-only outputs (HH:mm:ss)
        F.col("Standard_Time_In_fix").alias("Standard_Time_In"),
        F.col("Standard_Time_Out_fix").alias("Standard_Time_Out"),

        "Work_Day",
        F.col("actual_time_in_fix").alias("actual_date_time_in"),
        F.col("actual_time_out_fix").alias("actual_date_time_out"),

        "late_time_in_minutes",
        "before_time_out_minutes",
        "Count_Day_Include",
        "Count_Day_Absent",
        "Total_Leave_Days",
        "Total_Leave_Hours",
        "Break_Start",
        "Break_End",
        "Overtime_Type",
        "OT_Hour_1",
        "OT_Hour_2",
        "OT_Hour_3",
        "OT_Hour_4",
        "OT_Hour_5",
        "OT_Hour_6",
        "Day_Remark",
        "Seat_No",

        # extra cols from gold_employee_time_fixed
        # "WorkDate",
        # "DayName",
        # "Start0800",
        # "End1820",
        # "OTStart1840",
        # "End1700",
        "Normal_Work_Minutes",
        "OT_Work_Minutes",
        "Total_Work_Minutes",
        "Absent_Minutes",

        # your derived weekday cols
        "Leave_Weekday",
        "Leave_Weekday_Order",

        # memo cols (optional but useful)
        "Day_Memo_reason",
        "Day_Memo_reason_EN",

        # watermark
        F.to_timestamp(wm_expr).alias(MODCOL),
    )
    .dropDuplicates()
)

# -------------------------
# 5) INCREMENTAL WINDOW
# -------------------------
last_ts = get_last_ts_fqn(TGT_FQN, MODCOL)
staged = final_src if last_ts is None else final_src.filter(
    F.col(MODCOL) >= (F.lit(last_ts).cast("timestamp") - F.expr(f"INTERVAL {LOOKBACK_MIN} MINUTES"))
)

# create table if missing (empty schema bootstrap)
if not table_exists_fqn(TGT_FQN):
    boot = (staged
            .withColumn("updated_at", F.col(MODCOL))
            .withColumn("load_ts", F.current_timestamp())
            .withColumn("source_system", F.lit("gold_employee_time_fixed"))
            .withColumn("row_hash", F.lit(None).cast("string")))
    create_table_if_missing(TGT_FQN, boot)

# -------------------------
# 6) DEDUPE BY KEYS
# -------------------------
w = Window.partitionBy(*KEYS).orderBy(F.col(MODCOL).desc_nulls_last())
staged1 = (staged
           .withColumn("_rn", F.row_number().over(w))
           .filter(F.col("_rn") == 1)
           .drop("_rn"))

# -------------------------
# 7) SYSTEM COLS + HASH
# -------------------------
content_cols = [c for c in staged1.columns]  # includes MODCOL
staged_h = (
    staged1
      .withColumn("updated_at", F.col(MODCOL))
      .withColumn("load_ts", F.current_timestamp())
      .withColumn("source_system", F.lit("gold_employee_time_fixed"))
      .withColumn(
          "row_hash",
          F.sha2(F.concat_ws("§", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in content_cols]), 256)
      )
)

# -------------------------
# 8) MERGE
# -------------------------
if staged_h.rdd.isEmpty():
    print("No employee-time rows to merge. ✅")
else:
    tgt = DeltaTable.forName(spark, TGT_FQN)
    on_clause = " AND ".join([f"t.{k} <=> s.{k}" for k in KEYS])

    # columns to update/insert (exclude keys handled explicitly + system cols handled below)
    business_cols = [
        "Company_Name_Eng","Division_Eng","Department_Eng","Position_Eng","sub_department_Eng",
        "level_employee_Eng","Approval_Level_Thai","First_Name_Thai","First_Name_Eng","Last_Name_Thai",
        "Last_Name_Eng","Identity_ID","Shift_Name","ShiftCode","Standard_Time_In","Standard_Time_Out",
        "actual_date_time_in","actual_date_time_out","late_time_in_minutes","before_time_out_minutes",
        "Count_Day_Include","Count_Day_Absent","Total_Leave_Days","Total_Leave_Hours","Break_Start","Break_End",
        "Overtime_Type","OT_Hour_1","OT_Hour_2","OT_Hour_3","OT_Hour_4","OT_Hour_5","OT_Hour_6",
        "Day_Remark","Seat_No",
        # "WorkDate","DayName","Start0800","End1820","OTStart1840","End1700",
        "Normal_Work_Minutes","OT_Work_Minutes","Total_Work_Minutes","Absent_Minutes",
        "Leave_Weekday","Leave_Weekday_Order","Day_Memo_reason","Day_Memo_reason_EN",
        MODCOL
    ]

    set_map = {c: f"s.{c}" for c in business_cols}
    set_map.update({
        "updated_at": "s.updated_at",
        "load_ts": "s.load_ts",
        "source_system": "s.source_system",
        "row_hash": "s.row_hash",
    })

    insert_map = {k: f"s.{k}" for k in (KEYS + business_cols)}
    insert_map.update({
        "updated_at": "s.updated_at",
        "load_ts": "s.load_ts",
        "source_system": "s.source_system",
        "row_hash": "s.row_hash",
    })

    (
        tgt.alias("t")
           .merge(staged_h.alias("s"), on_clause)
           .whenMatchedUpdate(condition="t.row_hash <> s.row_hash", set=set_map)
           .whenNotMatchedInsert(values=insert_map)
           .execute()
    )

# Optional maintenance
# spark.sql(f"OPTIMIZE {TGT_FQN} ZORDER BY (Employee_Code, Work_Day)")
# spark.sql(f"VACUUM {TGT_FQN} RETAIN 168 HOURS")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Employee Data Daily

# CELL ********************

from pyspark.sql import functions as F

# --------- CONFIG ---------
SRC = "Silver_Commons_Lakehouse.cmn.silver_employee_data_full_replace"
TGT = "Gold_Production_Lakehouse.prod.gold_employee_data_daily"   # <- target table (overwrite daily)

# ensure schema exists
# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TGT.rsplit('.',1)[0]}")

# --------- EXTRACT ---------
df = spark.table(SRC)

# --------- TRANSFORM (mirror your SELECT) ---------
out = (
    df.filter(F.col("Division") == F.lit("PRODUCTION"))
      .select(
        F.col("Employee_Code").alias("employee_code"),
        F.col("Employee_Type").alias("employee_type"),
        F.col("Title_Eng").alias("title_eng"),
        F.col("First_Name_Eng").alias("first_name_eng"),
        F.col("`First Name Thai`").alias("first_name_thai"),      # column name with space
        F.col("Last_Name_Eng").alias("last_name_eng"),
        F.col("Marital_Status").alias("marital_status"),
        F.col("AntennaID").alias("antenna_id"),
        F.col("Position").alias("position"),
        F.col("sub_department").alias("sub_department"),
        F.col("level_employee").alias("level_employee"),
        F.col("Approval_Level").alias("approval_level"),
        F.col("Seat_No").alias("seat_no"),
        F.col("`Machine Center`").alias("machine_center"),        # column name with space
        F.col("Join_Date").alias("join_date"),
        F.col("Probation_Date").alias("probation_date"),
        F.col("Pass_Date").alias("pass_date"),
        F.col("End_Date").alias("end_date"),
        F.col("Division").alias("division"),
        F.col("Department").alias("department"),
        F.when(F.col("Gender") == F.lit("หญิง"), F.lit("Female"))
         .when(F.col("Gender") == F.lit("ชาย"),  F.lit("Male"))
         .otherwise(F.col("Gender")).alias("gender"),
        F.col("Birth_Date").alias("birth_date"),
        F.col("Age").alias("age"),
        F.col("Company_Name_Eng").alias("company_name_eng"),
        F.col("Contract_Start_Date").alias("contract_start_date"),
        F.col("Contract_End_Date").alias("contract_end_date"),
        F.concat(F.col("Employee_Code"), F.col("First_Name_Eng")).alias("id_name"),
        F.lit("1").alias("qty"),
        # optional lineage
        F.current_timestamp().alias("load_ts"),
        F.lit("silver_employee_data_full_replace").alias("source_system")
      )
)

# --------- LOAD (FULL OVERWRITE) ---------
(
    out.write
       .format("delta")
       .mode("overwrite")                 # <— overwrite contents every run
       .option("overwriteSchema", "true") # <— evolve schema if needed
       .saveAsTable(TGT)
)

# (optional) small maintenance; harmless if not supported in your workspace
# spark.sql(f"OPTIMIZE {TGT}")
# spark.sql(f"VACUUM {TGT} RETAIN 168 HOURS")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Employee Daily Data All

# CELL ********************

from pyspark.sql import functions as F

# --------- CONFIG ---------
SRC = "Silver_Commons_Lakehouse.cmn.silver_employee_data_full_replace"
TGT = "Gold_Commons_Lakehouse.cmn.gold_employee_data_daily_all"   # <- target table (overwrite daily)

# ensure schema exists
# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TGT.rsplit('.',1)[0]}")

# --------- EXTRACT ---------
df = spark.table(SRC)

# --------- TRANSFORM (mirror your SELECT) ---------
out = (
    df.select(
        F.col("Employee_Code").alias("employee_code"),
        F.col("Employee_Type").alias("employee_type"),
        F.col("Title_Eng").alias("title_eng"),
        F.col("First_Name_Eng").alias("first_name_eng"),
        F.col("`First Name Thai`").alias("first_name_thai"),      # column name with space
        F.col("Last_Name_Eng").alias("last_name_eng"),
        F.col("Marital_Status").alias("marital_status"),
        F.col("AntennaID").alias("antenna_id"),
        F.col("Position").alias("position"),
        F.col("sub_department").alias("sub_department"),
        F.col("level_employee").alias("level_employee"),
        F.col("Approval_Level").alias("approval_level"),
        F.col("Seat_No").alias("seat_no"),
        F.col("`Machine Center`").alias("machine_center"),        # column name with space
        F.col("Join_Date").alias("join_date"),
        F.col("Probation_Date").alias("probation_date"),
        F.col("Pass_Date").alias("pass_date"),
        F.col("End_Date").alias("end_date"),
        F.col("Division").alias("division"),
        F.col("Department").alias("department"),
        F.when(F.col("Gender") == F.lit("หญิง"), F.lit("Female"))
         .when(F.col("Gender") == F.lit("ชาย"),  F.lit("Male"))
         .otherwise(F.col("Gender")).alias("gender"),
        F.col("Birth_Date").alias("birth_date"),
        F.col("Age").alias("age"),
        F.col("Company_Name_Eng").alias("company_name_eng"),
        F.col("Contract_Start_Date").alias("contract_start_date"),
        F.col("Contract_End_Date").alias("contract_end_date"),
        F.concat(F.col("Employee_Code"), F.col("First_Name_Eng")).alias("id_name"),
        F.lit("1").alias("qty"),
        # optional lineage
        F.current_timestamp().alias("load_ts"),
        F.lit("silver_employee_data_full_replace").alias("source_system")
      )
)

# --------- LOAD (FULL OVERWRITE) ---------
(
    out.write
       .format("delta")
       .mode("overwrite")                 # <— overwrite contents every run
       .option("overwriteSchema", "true") # <— evolve schema if needed
       .saveAsTable(TGT)
)

# (optional) small maintenance; harmless if not supported in your workspace
# spark.sql(f"OPTIMIZE {TGT}")
# spark.sql(f"VACUUM {TGT} RETAIN 168 HOURS")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Employee Time Group

# CELL ********************

# MAGIC %%sql
# MAGIC -- ================================================================
# MAGIC -- FULL RELOAD (Spark SQL / Delta)
# MAGIC -- Target: Gold_Production_Lakehouse.prod.gold_employee_time_group
# MAGIC -- Sources:
# MAGIC --   D -> Silver_Commons_Lakehouse.cmn.silver_employee_data_full_replace
# MAGIC --   T -> Gold_Production_Lakehouse.prod.gold_employee_time_fixed
# MAGIC -- Notes:
# MAGIC --   - Use *_Eng columns from T (because D doesn't have them)
# MAGIC --   - Preserve sub_department history by grouping on normalized sub_department_Eng
# MAGIC --   - total_workhour = SUM(Total_Work_Minutes) (treated as "hour" per request)
# MAGIC -- ================================================================
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_employee_time_group
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH
# MAGIC T0 AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     TO_DATE(Work_Day) AS Work_Day_d
# MAGIC   FROM Gold_Production_Lakehouse.prod.gold_employee_time_fixed
# MAGIC ),
# MAGIC base AS (
# MAGIC   SELECT
# MAGIC     -- date
# MAGIC     T0.Work_Day_d AS Work_Day,
# MAGIC 
# MAGIC     -- take ENG hierarchy from T0 (your dataset shows these columns are in T)
# MAGIC     REGEXP_REPLACE(TRIM(T0.Department_Eng), '\\s+', ' ') AS department,
# MAGIC     REGEXP_REPLACE(TRIM(T0.Position_Eng),   '\\s+', ' ') AS position,
# MAGIC 
# MAGIC     -- IMPORTANT: keep history + normalize + blank->NULL
# MAGIC     NULLIF(REGEXP_REPLACE(TRIM(T0.sub_department_Eng), '\\s+', ' '), '') AS sub_department,
# MAGIC 
# MAGIC     -- employee identity
# MAGIC     REGEXP_REPLACE(TRIM(T0.Employee_Code), '\\s+', ' ') AS employee_code,
# MAGIC     REGEXP_REPLACE(TRIM(T0.First_Name_Thai), '\\s+', ' ') AS first_name_thai,
# MAGIC 
# MAGIC     -- take these from D (per your original python logic)
# MAGIC     REGEXP_REPLACE(TRIM(D.AntennaID), '\\s+', ' ') AS antenna_id,
# MAGIC     REGEXP_REPLACE(TRIM(D.`Machine Center`), '\\s+', ' ') AS machine_center,
# MAGIC 
# MAGIC     -- measure
# MAGIC     CAST(COALESCE(T0.Total_Work_Minutes, 0) AS DOUBLE) AS workhour
# MAGIC   FROM Silver_Commons_Lakehouse.cmn.silver_employee_data_full_replace D
# MAGIC   INNER JOIN T0
# MAGIC     ON D.Employee_Code = T0.Employee_Code
# MAGIC ),
# MAGIC agg AS (
# MAGIC   SELECT
# MAGIC     Work_Day,
# MAGIC     department,
# MAGIC     position,
# MAGIC     sub_department,
# MAGIC     employee_code,
# MAGIC     first_name_thai,
# MAGIC     antenna_id,
# MAGIC     machine_center,
# MAGIC     SUM(workhour) AS total_workhour
# MAGIC   FROM base
# MAGIC   GROUP BY
# MAGIC     Work_Day,
# MAGIC     department,
# MAGIC     position,
# MAGIC     sub_department,
# MAGIC     employee_code,
# MAGIC     first_name_thai,
# MAGIC     antenna_id,
# MAGIC     machine_center
# MAGIC )
# MAGIC SELECT
# MAGIC   Work_Day,
# MAGIC   department,
# MAGIC   position,
# MAGIC   sub_department,
# MAGIC   employee_code,
# MAGIC   first_name_thai,
# MAGIC   antenna_id,
# MAGIC   machine_center,
# MAGIC   CAST(total_workhour AS DOUBLE) AS total_workhour,
# MAGIC 
# MAGIC   -- deterministic row_id (same pattern you used)
# MAGIC   SHA2(
# MAGIC     CONCAT_WS(
# MAGIC       '||',
# MAGIC       COALESCE(CAST(Work_Day AS STRING), ''),
# MAGIC       COALESCE(department, ''),
# MAGIC       COALESCE(position, ''),
# MAGIC       COALESCE(CAST(sub_department AS STRING), ''),
# MAGIC       COALESCE(employee_code, ''),
# MAGIC       COALESCE(first_name_thai, ''),
# MAGIC       COALESCE(antenna_id, ''),
# MAGIC       COALESCE(machine_center, '')
# MAGIC     ),
# MAGIC     256
# MAGIC   ) AS row_id
# MAGIC FROM agg;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Remaining Work Week

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
SOURCE_EMP  = "Silver_Commons_Lakehouse.cmn.silver_employee_data_full_replace"
SOURCE_TIME = "Gold_Production_Lakehouse.prod.gold_employee_time_fixed"   # UPDATED

TARGET_TABLE = "Gold_Production_Lakehouse.prod.gold_remaining_work_week"

# ------------------------------------------------------------------------------
# Helper: get max Work_Date for incremental loading
# ------------------------------------------------------------------------------
def get_max_work_date(table_name: str):
    try:
        df = spark.table(table_name)
        row = df.agg(F.max("Work_Date").alias("max_date")).collect()[0]
        return row["max_date"]
    except Exception:
        return None

max_work_date = get_max_work_date(TARGET_TABLE)

# ------------------------------------------------------------------------------
# Load source data
# ------------------------------------------------------------------------------
emp_df  = spark.table(SOURCE_EMP)

# UPDATED: Work_Date from Work_Day (already present)
time_df = (
    spark.table(SOURCE_TIME)
         .withColumn("Work_Date", F.to_date("Work_Day"))
)

# Incremental filter: only new dates
if max_work_date is not None:
    time_df = time_df.filter(F.col("Work_Date") > F.lit(max_work_date))

if time_df.rdd.isEmpty():
    print("No new Work_Date rows to process.")

# ------------------------------------------------------------------------------
# WorkCalc CTE (now uses pre-calculated minute columns from fixed table)
# ------------------------------------------------------------------------------
workcalc_df = (
    emp_df.alias("D")
    .join(
        time_df.alias("T"),
        F.col("D.Employee_Code") == F.col("T.Employee_Code"),
        "inner"
    )
    .where(
        (F.col("D.sub_department").like("CELL%")) &
        (~F.col("D.Position").like("%MANAGER%")) &
        (~F.col("D.Position").like("%ADMIN%")) &
        (~F.col("D.Position").like("%OFFICER%")) &
        (F.col("D.Division") == F.lit("PRODUCTION"))
    )
    .select(
        F.col("D.Employee_Code"),
        F.col("D.Division").alias("Division"),
        F.col("D.Department"),
        F.col("D.sub_department").alias("Cell"),
        F.col("D.Position"),
        F.col("T.Work_Date"),

        # UPDATED: use existing calculated cols directly
        F.coalesce(F.col("T.Total_Work_Minutes"),  F.lit(0)).cast("double").alias("Actual_Work_Mins_Row"),
        F.coalesce(F.col("T.Absent_Minutes"),      F.lit(0)).cast("double").alias("Leave_Mins_Row"),
        F.coalesce(F.col("T.OT_Work_Minutes"),     F.lit(0)).cast("double").alias("OT_Mins_Row"),
    )
    .filter(
        (F.col("D.end_date").isNull()) &
        (F.col("D.department").like("PROD  LINE  %"))
    )
)

# ------------------------------------------------------------------------------
# DailyAgg CTE
# ------------------------------------------------------------------------------
FULL_DAY_MINS_CONST = 560

dailyagg_df = (
    workcalc_df
    .groupBy("Division", "Department", "Cell", "Position", "Work_Date")
    .agg(
        F.countDistinct("Employee_Code").alias("Headcount"),

        F.lit(FULL_DAY_MINS_CONST).alias("Full_Day_Mins"),

        # UPDATED: already computed
        F.sum("Actual_Work_Mins_Row").alias("Actual_Work_Mins"),

        # UPDATED: plan still needs standard*headcount - leave mins
        (
            F.lit(FULL_DAY_MINS_CONST) * F.countDistinct("Employee_Code")
            - F.sum("Leave_Mins_Row")
        ).alias("Planned_Work_Mins"),

        (
            F.lit(FULL_DAY_MINS_CONST) * F.countDistinct("Employee_Code")
        ).alias("Total_Standard_Base_Mins"),

        # UPDATED: from fixed fields
        F.sum("Leave_Mins_Row").alias("Total_Leave_Mins"),
        F.sum("OT_Mins_Row").alias("Total_OT_Mins"),

        F.weekofyear("Work_Date").alias("ISO_Week"),
        F.year("Work_Date").alias("ISO_Year")
    )
)

# ------------------------------------------------------------------------------
# Final SELECT + CASE logic for total_workhour
# ------------------------------------------------------------------------------
result_df = (
    dailyagg_df
    .withColumn(
        "total_workhour",
        F.when(
            F.col("Work_Date") <= F.current_date(),
            F.col("Actual_Work_Mins")
        ).otherwise(
            F.col("Planned_Work_Mins")
        )
    )
    .select(
        "Division",
        "Department",
        "Cell",
        "Position",
        "Work_Date",
        "Headcount",
        "total_workhour",
        "Full_Day_Mins",
        "Actual_Work_Mins",
        "Planned_Work_Mins",
        "Total_Standard_Base_Mins",
        "Total_Leave_Mins",
        "Total_OT_Mins",
        "ISO_Week",
        "ISO_Year"
    )
)

# ------------------------------------------------------------------------------
# Upsert into Delta table
# ------------------------------------------------------------------------------
if spark.catalog.tableExists(TARGET_TABLE):
    try:
        delta_target = DeltaTable.forName(spark, TARGET_TABLE)

        (
            delta_target.alias("t")
            .merge(
                result_df.alias("s"),
                """
                t.Division   = s.Division   AND
                t.Department = s.Department AND
                t.Cell       = s.Cell       AND
                t.Position   = s.Position   AND
                t.Work_Date  = s.Work_Date
                """
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    except AnalysisException as e:
        print(f"{TARGET_TABLE} exists but is not Delta. Recreating. Error: {e}")
        (
            result_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(TARGET_TABLE)
        )
else:
    (
        result_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TARGET_TABLE)
    )

print("Incremental load to Gold_Production_Lakehouse.prod.gold_remaining_work_week completed.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# # Clean Gold Data

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
    "Gold_Production_Lakehouse",
   
]

# Tables to dedupe in each lakehouse
# keys = partition columns for row_number()
# order = column used to pick the "latest" within each partition
DEDUPE_SPECS = [
    # prod schema
    {
        "schema": "prod",
        "table":  "gold_employee_data_daily",
        "keys":   ["employee_code"],
        "order":  "age",
    },
    {
        "schema": "prod",
        "table":  "gold_employee_time",
        "keys":   ["Employee_Code"],
        "order":  "Work_Day",
    },
    {
        "schema": "prod",
        "table":  "gold_emp_work_time",
        "keys":   ["Division", "Department", "sub_department", "Position", "Employee_Code", "First_Name_Eng", "First_Name_Thai", "antenna_id", "machine_center_no", "team", "Work_Day", "machine_map",],
        "order":  "Work_Day",
    },
    {
        "schema": "prod",
        "table":  "gold_employee_time_group",
        "keys":   ["employee_code"],
        "order":  "Work_Day",
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
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # Dataverse Employee Data

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Silver_Commons_Lakehouse.cmn.spark_employee_data
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     -- Dataverse Primary Key (GUID – REQUIRED)
# MAGIC     uuid() AS employeesdataid,
# MAGIC 
# MAGIC     -- Business / Natural Keys
# MAGIC     Employee_Code AS employeeid,
# MAGIC 
# MAGIC 
# MAGIC     -- Identifiers
# MAGIC     AntennaID AS antennaid,
# MAGIC 
# MAGIC     -- Name (English)
# MAGIC     First_Name_Eng AS firstname,
# MAGIC     Last_Name_Eng AS lastname,
# MAGIC     Title_Eng AS titleeng,
# MAGIC 
# MAGIC     -- Name (Thai)
# MAGIC     `First Name Thai` AS firstnamethai,
# MAGIC     NULL AS lastnamethai,
# MAGIC 
# MAGIC     -- Full Name (Derived)
# MAGIC     CONCAT(First_Name_Eng, ' ', Last_Name_Eng) AS eng,
# MAGIC     `First Name Thai` AS thai,
# MAGIC 
# MAGIC     -- Employment Details
# MAGIC     Employee_Type AS employeetype,
# MAGIC     Join_Date AS joindate,
# MAGIC     End_Date AS enddate,
# MAGIC     Probation_Date AS probationdate,
# MAGIC     Contract_Start_Date AS contractstartdate,
# MAGIC     Contract_End_Date AS contractenddate,
# MAGIC     Age AS age,
# MAGIC 
# MAGIC     -- Department / Org
# MAGIC     Department AS departmenteng,
# MAGIC     Department AS departmentthai,
# MAGIC     sub_department AS celleng,
# MAGIC     sub_department AS cellthai,
# MAGIC     Department AS linethai,
# MAGIC 
# MAGIC     -- Position / Level
# MAGIC     Position AS positioneng,
# MAGIC     Position AS positionthai,
# MAGIC     level_employee AS leveleng,
# MAGIC     level_employee AS levelthai,
# MAGIC 
# MAGIC     -- Others
# MAGIC     Seat_No AS seatno,
# MAGIC     `Machine Center` AS machinecenter
# MAGIC 
# MAGIC FROM Silver_Commons_Lakehouse.cmn.silver_employee_data_full_replace;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Employee Time Group RFID

# CELL ********************

# ============================================================
# FULL REFRESH: employee time group + RFID mapping + cell list
# ============================================================

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT_TBL = "Gold_Production_Lakehouse.prod.gold_employee_time_group_rfid"

src_sql = """
SELECT
    e.Work_Day,
    e.department,
    e.position,
    e.sub_department,
    e.employee_code,
    e.first_name_thai,
    e.machine_center,
    e.total_workhour,
    e.row_id,
    r.antenna_id,
    r.machine_center_no,

    c.email_address,
    c.cell_line,
    c.prod_line

FROM Gold_Production_Lakehouse.prod.gold_employee_time_group e
JOIN Silver_Commons_Lakehouse.cmn.silver_employee_rfid_mapping r
    ON e.employee_code = r.Employee_Code

LEFT JOIN Silver_Production_Lakehouse.prod.silver_cell_list c
    ON r.sub_department_Eng = c.sub_department
"""

df_src = spark.sql(src_sql)

(
    df_src.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TGT_TBL)
)

print(f"FULL REFRESH completed into: {TGT_TBL}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Employee Time BI

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_employee_time_bi
# MAGIC AS
# MAGIC WITH base AS (
# MAGIC     SELECT
# MAGIC         t.Employee_Code,
# MAGIC         t.Work_Day,
# MAGIC         t.Day_Memo_reason,
# MAGIC         t.Day_Memo_reason_EN,
# MAGIC 
# MAGIC         -- Org / Employee info
# MAGIC         t.Company_Name_Eng,
# MAGIC         t.Division_Eng,
# MAGIC         t.Department_Eng,
# MAGIC         t.Position_Eng,
# MAGIC         t.sub_department_Eng,
# MAGIC         t.level_employee_Eng,
# MAGIC         t.First_Name_Eng,
# MAGIC         t.Last_Name_Eng,
# MAGIC         t.First_Name_Thai,
# MAGIC         t.Last_Name_Thai,
# MAGIC         t.Shift_Name,
# MAGIC         t.ShiftCode,
# MAGIC         t.Seat_No,
# MAGIC 
# MAGIC         -- Shift times
# MAGIC         t.Standard_Time_In,
# MAGIC         t.Standard_Time_Out,
# MAGIC         t.Break_Start,
# MAGIC         t.Break_End,
# MAGIC 
# MAGIC         -- Calendar
# MAGIC         t.Leave_Weekday,
# MAGIC         t.Leave_Weekday_Order,
# MAGIC 
# MAGIC         -- Raw source values (all in MINUTES)
# MAGIC         COALESCE(t.Count_Day_Include, 0)       AS Shift_Minutes_Raw,
# MAGIC         COALESCE(t.Total_Leave_Hours, 0)       AS Leave_Minutes_Raw,
# MAGIC         COALESCE(t.Total_Leave_Days, 0)        AS Leave_Days_Raw,
# MAGIC         COALESCE(t.Count_Day_Absent, 0)        AS Absent_Days_Raw,
# MAGIC         COALESCE(t.late_time_in_minutes, 0)    AS Late_Minutes_Raw,
# MAGIC         COALESCE(t.before_time_out_minutes, 0) AS Early_Out_Minutes_Raw,
# MAGIC         COALESCE(t.Normal_Work_Minutes, 0)     AS Normal_Work_Minutes_Raw,
# MAGIC         COALESCE(t.OT_Work_Minutes, 0)         AS OT_Work_Minutes_Raw,
# MAGIC         COALESCE(t.Total_Work_Minutes, 0)      AS Total_Work_Minutes_Raw,
# MAGIC         COALESCE(t.Absent_Minutes, 0)          AS Absent_Minutes_Raw,
# MAGIC 
# MAGIC         -- OT breakdown (all stored as minutes)
# MAGIC         COALESCE(t.OT_Hour_1, 0) AS OT1,
# MAGIC         COALESCE(t.OT_Hour_2, 0) AS OT2,
# MAGIC         COALESCE(t.OT_Hour_3, 0) AS OT3,
# MAGIC         COALESCE(t.OT_Hour_4, 0) AS OT4,
# MAGIC         COALESCE(t.OT_Hour_5, 0) AS OT5,
# MAGIC         COALESCE(t.OT_Hour_6, 0) AS OT6,
# MAGIC         t.Overtime_Type,
# MAGIC 
# MAGIC         -- Scan existence
# MAGIC         t.actual_date_time_in,
# MAGIC         t.actual_date_time_out
# MAGIC 
# MAGIC     FROM Gold_Commons_Lakehouse.cmn.gold_employee_time_all t
# MAGIC     WHERE (
# MAGIC         t.Day_Memo_reason NOT IN ('ยังไม่เริ่มงาน', 'ไม่พบการตั้งกะงาน')
# MAGIC         OR t.Day_Memo_reason IS NULL
# MAGIC     )
# MAGIC ),
# MAGIC 
# MAGIC shift_standard AS (
# MAGIC     SELECT 560 AS Standard_Shift_Minutes
# MAGIC ),
# MAGIC 
# MAGIC calc AS (
# MAGIC     SELECT
# MAGIC         b.*,
# MAGIC 
# MAGIC         -- 1. FLAGS
# MAGIC         -- Spark dayofweek(): Sun=1, Mon=2, ... Sat=7
# MAGIC         CASE
# MAGIC             WHEN dayofweek(b.Work_Day) BETWEEN 2 AND 6 THEN 1
# MAGIC             ELSE 0
# MAGIC         END AS Is_Weekday,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN b.ShiftCode IS NOT NULL AND b.ShiftCode <> '' THEN 1
# MAGIC             ELSE 0
# MAGIC         END AS Is_Normal_Shift,
# MAGIC 
# MAGIC         -- Is_Holiday: detect from Day_Memo_reason_EN = 'RH' (Regular Holiday)
# MAGIC         CASE
# MAGIC             WHEN b.Day_Memo_reason_EN IN ('RH', 'AH') THEN 1
# MAGIC             ELSE 0
# MAGIC         END AS Is_Holiday,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN b.Day_Memo_reason_EN = 'VL' AND b.Leave_Minutes_Raw > 0 THEN 1
# MAGIC             ELSE 0
# MAGIC         END AS Is_Vacation,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN dayofweek(b.Work_Day) BETWEEN 2 AND 6
# MAGIC              AND (b.ShiftCode IS NULL OR b.ShiftCode = '') THEN 1
# MAGIC             ELSE 0
# MAGIC         END AS Is_No_Shift_Setup,
# MAGIC 
# MAGIC         0 AS Is_Resigned_In_Period,
# MAGIC 
# MAGIC         -- 2. ATTENDANCE
# MAGIC         CASE
# MAGIC             WHEN b.actual_date_time_in IS NOT NULL THEN 1
# MAGIC             ELSE 0
# MAGIC         END AS Attendance_Day_Flag_Calc,
# MAGIC 
# MAGIC         -- 3. LEAVE CLASSIFICATION
# MAGIC 
# MAGIC         -- Vacation Leave (VL only)
# MAGIC         CASE
# MAGIC             WHEN b.Day_Memo_reason_EN = 'VL' AND b.Leave_Minutes_Raw > 0
# MAGIC                 THEN b.Leave_Minutes_Raw
# MAGIC             WHEN b.Day_Memo_reason_EN = 'VL'
# MAGIC              AND b.Leave_Minutes_Raw = 0
# MAGIC              AND b.actual_date_time_in IS NULL
# MAGIC                 THEN ss.Standard_Shift_Minutes
# MAGIC             ELSE 0
# MAGIC         END AS VL_Minutes,
# MAGIC 
# MAGIC         -- Non-Vacation Leave (SL, ML, BL, U/P)
# MAGIC         CASE
# MAGIC             WHEN b.Day_Memo_reason_EN IN ('SL','ML','BL','U/P')
# MAGIC              AND b.Leave_Minutes_Raw > 0
# MAGIC                 THEN b.Leave_Minutes_Raw
# MAGIC             WHEN b.Day_Memo_reason_EN IN ('SL','ML','BL','U/P')
# MAGIC              AND b.Leave_Minutes_Raw = 0
# MAGIC              AND b.actual_date_time_in IS NULL
# MAGIC                 THEN ss.Standard_Shift_Minutes
# MAGIC             ELSE 0
# MAGIC         END AS NVL_Minutes,
# MAGIC 
# MAGIC         -- 4. OT BREAKDOWN
# MAGIC         -- FIX v4.1: OT_Hour_1..6 unreliable
# MAGIC         -- NEW LOGIC:
# MAGIC         --   Normal_Work > 0 -> OT Regular
# MAGIC         --   Normal_Work = 0 -> OT Holiday
# MAGIC         CASE
# MAGIC             WHEN b.Normal_Work_Minutes_Raw > 0 THEN b.OT_Work_Minutes_Raw
# MAGIC             ELSE 0
# MAGIC         END AS OT_Regular_Minutes,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN b.Normal_Work_Minutes_Raw = 0 THEN b.OT_Work_Minutes_Raw
# MAGIC             ELSE 0
# MAGIC         END AS OT_Holiday_Minutes,
# MAGIC 
# MAGIC         b.OT_Work_Minutes_Raw AS OT_Total_Minutes
# MAGIC 
# MAGIC     FROM base b
# MAGIC     CROSS JOIN shift_standard ss
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     -- EMPLOYEE & ORG
# MAGIC     c.Employee_Code,
# MAGIC     c.Work_Day,
# MAGIC     c.Day_Memo_reason,
# MAGIC     c.Day_Memo_reason_EN,
# MAGIC     c.Company_Name_Eng,
# MAGIC     c.Division_Eng,
# MAGIC     c.Department_Eng,
# MAGIC     c.Position_Eng,
# MAGIC     c.sub_department_Eng,
# MAGIC     c.level_employee_Eng,
# MAGIC     c.First_Name_Eng,
# MAGIC     c.Last_Name_Eng,
# MAGIC     c.First_Name_Thai,
# MAGIC     c.Last_Name_Thai,
# MAGIC     c.Shift_Name,
# MAGIC     c.ShiftCode,
# MAGIC     c.Seat_No,
# MAGIC     c.Leave_Weekday,
# MAGIC     c.Leave_Weekday_Order,
# MAGIC 
# MAGIC     -- FLAGS
# MAGIC     c.Is_Weekday,
# MAGIC     c.Is_Normal_Shift,
# MAGIC     c.Is_Holiday,
# MAGIC     c.Is_Vacation,
# MAGIC     c.Is_No_Shift_Setup,
# MAGIC     c.Is_Resigned_In_Period,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN c.Is_Weekday = 1
# MAGIC          AND c.Is_Normal_Shift = 1
# MAGIC          AND c.Is_Holiday = 0
# MAGIC          AND c.Is_Resigned_In_Period = 0
# MAGIC         THEN 1 ELSE 0
# MAGIC     END AS Shift_Day_Flag,
# MAGIC 
# MAGIC     -- SHIFT
# MAGIC     c.Shift_Minutes_Raw AS Shift_Minutes,
# MAGIC     CAST(c.Shift_Minutes_Raw / 60.0 AS DECIMAL(10,2)) AS Shift_Hours,
# MAGIC 
# MAGIC     -- ATTENDANCE
# MAGIC     c.Attendance_Day_Flag_Calc AS Attendance_Day_Flag,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN c.Shift_Minutes_Raw > 0
# MAGIC         THEN LEAST(
# MAGIC             CAST(c.Attendance_Day_Flag_Calc * 100.0 AS DECIMAL(10,2)),
# MAGIC             CAST(100.00 AS DECIMAL(10,2))
# MAGIC         )
# MAGIC         ELSE CAST(0.00 AS DECIMAL(10,2))
# MAGIC     END AS Attendance_Rate_Pct,
# MAGIC 
# MAGIC     -- LATE & EARLY OUT
# MAGIC     c.Late_Minutes_Raw AS Total_Late_Minutes,
# MAGIC     CAST(c.Late_Minutes_Raw / 60.0 AS DECIMAL(10,2)) AS Total_Late_Hours,
# MAGIC 
# MAGIC     c.Early_Out_Minutes_Raw AS Total_Early_Out_Minutes,
# MAGIC     CAST(c.Early_Out_Minutes_Raw / 60.0 AS DECIMAL(10,2)) AS Total_Early_Out_Hours,
# MAGIC 
# MAGIC     -- LEAVE
# MAGIC     c.VL_Minutes AS Total_Vacation_Leave_Minutes,
# MAGIC     CAST(c.VL_Minutes / 60.0 AS DECIMAL(10,2)) AS Total_Vacation_Leave_Hours,
# MAGIC 
# MAGIC     c.NVL_Minutes AS Total_NonVacation_Leave_Minutes,
# MAGIC     CAST(c.NVL_Minutes / 60.0 AS DECIMAL(10,2)) AS Total_NonVacation_Leave_Hours,
# MAGIC 
# MAGIC     (c.VL_Minutes + c.NVL_Minutes) AS Total_Leave_Minutes,
# MAGIC     CAST((c.VL_Minutes + c.NVL_Minutes) / 60.0 AS DECIMAL(10,2)) AS Total_Leave_Hours_Calc,
# MAGIC 
# MAGIC     -- WORK HOURS
# MAGIC     CASE
# MAGIC         WHEN c.Is_Weekday = 1
# MAGIC          AND c.Is_Normal_Shift = 1
# MAGIC          AND c.Is_Holiday = 0
# MAGIC          AND c.Is_Resigned_In_Period = 0
# MAGIC         THEN c.Shift_Minutes_Raw
# MAGIC         ELSE 0
# MAGIC     END AS Total_Work_Minutes_Standard_Weekday,
# MAGIC 
# MAGIC     -- Actual: normal work minutes from source (capped at shift)
# MAGIC     CAST(LEAST(c.Normal_Work_Minutes_Raw, c.Shift_Minutes_Raw) AS BIGINT) AS Total_Work_Minutes_Actual_Weekday,
# MAGIC 
# MAGIC     -- Net: same as Actual/source logic
# MAGIC     CAST(c.Normal_Work_Minutes_Raw AS BIGINT) AS Total_Work_Minutes_Net,
# MAGIC 
# MAGIC     -- Net work hours
# MAGIC     CAST(c.Normal_Work_Minutes_Raw / 60.0 AS DECIMAL(10,2)) AS Work_Hours_Net,
# MAGIC 
# MAGIC     -- OVERTIME
# MAGIC     CAST(c.OT_Regular_Minutes / 60.0 AS DECIMAL(10,2)) AS OT_Regular_Hours,
# MAGIC     CAST(c.OT_Holiday_Minutes / 60.0 AS DECIMAL(10,2)) AS OT_Holiday_Hours,
# MAGIC     CAST(c.OT_Total_Minutes / 60.0 AS DECIMAL(10,2)) AS Total_OT_Hours,
# MAGIC 
# MAGIC     -- ATTENDANCE HOURS
# MAGIC     CAST((c.Normal_Work_Minutes_Raw + c.OT_Total_Minutes) / 60.0 AS DECIMAL(10,2)) AS Attendance_Hours,
# MAGIC 
# MAGIC     -- UTILIZATION %
# MAGIC     CASE
# MAGIC         WHEN c.Shift_Minutes_Raw > 0
# MAGIC          AND c.Is_Weekday = 1
# MAGIC          AND c.Is_Normal_Shift = 1
# MAGIC          AND c.Is_Holiday = 0
# MAGIC         THEN CAST(
# MAGIC             LEAST(
# MAGIC                 c.Normal_Work_Minutes_Raw * 100.0 / c.Shift_Minutes_Raw,
# MAGIC                 100.00
# MAGIC             ) AS DECIMAL(10,2)
# MAGIC         )
# MAGIC         ELSE CAST(0.00 AS DECIMAL(10,2))
# MAGIC     END AS Actual_Work_VS_Standard_Pct,
# MAGIC 
# MAGIC     -- SOURCE REFERENCE
# MAGIC     c.Absent_Minutes_Raw AS Source_Absent_Minutes,
# MAGIC     c.Absent_Days_Raw AS Source_Count_Day_Absent,
# MAGIC     c.Leave_Days_Raw AS Source_Total_Leave_Days,
# MAGIC     c.Leave_Minutes_Raw AS Source_Total_Leave_Hours_Raw,
# MAGIC     c.Overtime_Type AS Source_Overtime_Type
# MAGIC 
# MAGIC FROM calc c;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Employee Time BI Summary

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_employee_time_bi_summary
# MAGIC AS
# MAGIC SELECT
# MAGIC     -- ════════════════════════════════════════════════════════════
# MAGIC     -- ข้อมูลพนักงาน
# MAGIC     -- ════════════════════════════════════════════════════════════
# MAGIC     d.Employee_Code,
# MAGIC     MAX(d.First_Name_Thai)                                    AS First_Name_Thai,
# MAGIC     MAX(d.Last_Name_Thai)                                     AS Last_Name_Thai,
# MAGIC     MAX(d.First_Name_Eng)                                     AS First_Name_Eng,
# MAGIC     MAX(d.Last_Name_Eng)                                      AS Last_Name_Eng,
# MAGIC     MAX(d.Company_Name_Eng)                                   AS Company_Name_Eng,
# MAGIC     MAX(d.Division_Eng)                                       AS Division_Eng,
# MAGIC     MAX(d.Department_Eng)                                     AS Department_Eng,
# MAGIC     MAX(d.sub_department_Eng)                                 AS sub_department_Eng,
# MAGIC     MAX(d.Position_Eng)                                       AS Position_Eng,
# MAGIC     MAX(d.level_employee_Eng)                                 AS level_employee_Eng,
# MAGIC     MAX(d.Seat_No)                                            AS Seat_No,
# MAGIC 
# MAGIC     -- ════════════════════════════════════════════════════════════
# MAGIC     -- SHIFT — เวลาทำงานที่บริษัทกำหนด (ไม่รวม AH/RH)
# MAGIC     -- ════════════════════════════════════════════════════════════
# MAGIC     SUM(d.Shift_Day_Flag)                                     AS SH_D,
# MAGIC 
# MAGIC     CAST(SUM(CASE WHEN d.Shift_Day_Flag = 1
# MAGIC                   THEN d.Shift_Hours ELSE 0 END)
# MAGIC          AS DECIMAL(10,2))                                    AS SH_H,
# MAGIC 
# MAGIC     -- ════════════════════════════════════════════════════════════
# MAGIC     -- ATTENDANCE — การมาทำงานจริง (เฉพาะวันมีกะ)
# MAGIC     -- SHRM: Attendance Rate = Days Present / Scheduled Days
# MAGIC     -- ════════════════════════════════════════════════════════════
# MAGIC     SUM(CASE WHEN d.Shift_Day_Flag = 1
# MAGIC               THEN d.Attendance_Day_Flag ELSE 0 END)          AS AT_D,
# MAGIC 
# MAGIC     CAST(SUM(CASE WHEN d.Shift_Day_Flag = 1
# MAGIC                   THEN d.Work_Hours_Net ELSE 0 END)
# MAGIC          AS DECIMAL(10,2))                                    AS AT_H,
# MAGIC 
# MAGIC     CAST(CASE WHEN SUM(d.Shift_Day_Flag) > 0
# MAGIC               THEN SUM(CASE WHEN d.Shift_Day_Flag = 1
# MAGIC                             THEN d.Attendance_Day_Flag ELSE 0 END) * 100.0
# MAGIC                    / SUM(d.Shift_Day_Flag)
# MAGIC               ELSE 0
# MAGIC          END AS DECIMAL(10,2))                                AS AT_PCT,
# MAGIC 
# MAGIC     -- ════════════════════════════════════════════════════════════
# MAGIC     -- HOLIDAY WORK — วันหยุดที่มาทำงาน (แยกจาก Att)
# MAGIC     -- ════════════════════════════════════════════════════════════
# MAGIC     SUM(CASE WHEN d.Shift_Day_Flag = 0
# MAGIC               THEN d.Attendance_Day_Flag ELSE 0 END)          AS HW_D,
# MAGIC 
# MAGIC     -- ════════════════════════════════════════════════════════════
# MAGIC     -- OVERTIME — แยกตาม พ.ร.บ. มาตรา 61 (1.5×), 63 (3×)
# MAGIC     -- ════════════════════════════════════════════════════════════
# MAGIC     CAST(SUM(d.Total_OT_Hours) AS DECIMAL(10,2))              AS OT_H,
# MAGIC 
# MAGIC     CAST(CASE WHEN SUM(CASE WHEN d.Shift_Day_Flag = 1
# MAGIC                              THEN d.Shift_Hours ELSE 0 END) > 0
# MAGIC               THEN SUM(d.Total_OT_Hours) * 100.0
# MAGIC                    / SUM(CASE WHEN d.Shift_Day_Flag = 1
# MAGIC                               THEN d.Shift_Hours ELSE 0 END)
# MAGIC               ELSE 0
# MAGIC          END AS DECIMAL(10,2))                                AS OT_PCT,
# MAGIC 
# MAGIC     CAST(SUM(d.OT_Regular_Hours) AS DECIMAL(10,2))            AS OTR_H,
# MAGIC 
# MAGIC     CAST(SUM(d.OT_Holiday_Hours) AS DECIMAL(10,2))            AS OTH_H,
# MAGIC 
# MAGIC     -- ════════════════════════════════════════════════════════════
# MAGIC     -- LATE — การมาสาย (แสดงแยก ไม่หักจาก Att)
# MAGIC     -- ILO: ไม่หักซ้ำ
# MAGIC     -- ════════════════════════════════════════════════════════════
# MAGIC     SUM(CASE WHEN d.Total_Late_Minutes > 0
# MAGIC               THEN 1 ELSE 0 END)                              AS LT_C,
# MAGIC 
# MAGIC     SUM(d.Total_Late_Minutes)                                 AS LT_M,
# MAGIC 
# MAGIC     CAST(SUM(d.Total_Late_Minutes) / 60.0
# MAGIC          AS DECIMAL(10,2))                                    AS LT_H,
# MAGIC 
# MAGIC     -- ════════════════════════════════════════════════════════════
# MAGIC     -- ABSENT — ขาดงาน (ไม่มา + ไม่ได้ลา)
# MAGIC     -- SHRM: Absenteeism = unexcused absence only
# MAGIC     -- ════════════════════════════════════════════════════════════
# MAGIC     SUM(CASE WHEN d.Shift_Day_Flag = 1
# MAGIC               AND d.Attendance_Day_Flag = 0
# MAGIC               AND (d.Day_Memo_reason_EN IN ('ABS')
# MAGIC                    OR d.Day_Memo_reason_EN IS NULL
# MAGIC                    OR d.Day_Memo_reason_EN = '')
# MAGIC               THEN 1 ELSE 0 END)                              AS AB_D,
# MAGIC 
# MAGIC     CAST(SUM(CASE WHEN d.Shift_Day_Flag = 1
# MAGIC                    AND d.Attendance_Day_Flag = 0
# MAGIC                    AND (d.Day_Memo_reason_EN IN ('ABS')
# MAGIC                         OR d.Day_Memo_reason_EN IS NULL
# MAGIC                         OR d.Day_Memo_reason_EN = '')
# MAGIC                   THEN d.Source_Absent_Minutes ELSE 0 END)
# MAGIC          / 60.0 AS DECIMAL(10,2))                             AS AB_H,
# MAGIC 
# MAGIC     CAST(CASE WHEN SUM(d.Shift_Day_Flag) > 0
# MAGIC               THEN SUM(CASE WHEN d.Shift_Day_Flag = 1
# MAGIC                              AND d.Attendance_Day_Flag = 0
# MAGIC                              AND (d.Day_Memo_reason_EN IN ('ABS')
# MAGIC                                   OR d.Day_Memo_reason_EN IS NULL
# MAGIC                                   OR d.Day_Memo_reason_EN = '')
# MAGIC                              THEN 1 ELSE 0 END) * 100.0
# MAGIC                    / SUM(d.Shift_Day_Flag)
# MAGIC               ELSE 0
# MAGIC          END AS DECIMAL(10,2))                                AS AB_PCT,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN SUM(d.Shift_Day_Flag) = 0 THEN '🟢'
# MAGIC         WHEN SUM(CASE WHEN d.Shift_Day_Flag = 1
# MAGIC                        AND d.Attendance_Day_Flag = 0
# MAGIC                        AND (d.Day_Memo_reason_EN IN ('ABS')
# MAGIC                             OR d.Day_Memo_reason_EN IS NULL
# MAGIC                             OR d.Day_Memo_reason_EN = '')
# MAGIC                        THEN 1 ELSE 0 END) * 1.0
# MAGIC              / SUM(d.Shift_Day_Flag) > 0.04 THEN '🔴'
# MAGIC         WHEN SUM(CASE WHEN d.Shift_Day_Flag = 1
# MAGIC                        AND d.Attendance_Day_Flag = 0
# MAGIC                        AND (d.Day_Memo_reason_EN IN ('ABS')
# MAGIC                             OR d.Day_Memo_reason_EN IS NULL
# MAGIC                             OR d.Day_Memo_reason_EN = '')
# MAGIC                        THEN 1 ELSE 0 END) * 1.0
# MAGIC              / SUM(d.Shift_Day_Flag) >= 0.03 THEN '🟠'
# MAGIC         ELSE '🟢'
# MAGIC     END                                                       AS ABL,
# MAGIC 
# MAGIC     -- ════════════════════════════════════════════════════════════
# MAGIC     -- LEAVE — การลา (อนุมัติแล้ว ≠ ขาดงาน)
# MAGIC     -- พ.ร.บ. มาตรา 32, 34, 41
# MAGIC     -- ════════════════════════════════════════════════════════════
# MAGIC     SUM(CASE WHEN d.Day_Memo_reason_EN IN ('VL','SL','ML','BL','U/P')
# MAGIC               AND d.Attendance_Day_Flag = 0
# MAGIC               AND d.Shift_Day_Flag = 1
# MAGIC               THEN 1 ELSE 0 END)                              AS LV_D,
# MAGIC 
# MAGIC     CAST(SUM(CASE WHEN d.Day_Memo_reason_EN IN ('VL','SL','ML','BL','U/P')
# MAGIC                   THEN d.Total_Leave_Hours_Calc ELSE 0 END)
# MAGIC          AS DECIMAL(10,2))                                    AS LV_H,
# MAGIC 
# MAGIC     CAST(CASE WHEN SUM(d.Shift_Day_Flag) > 0
# MAGIC               THEN SUM(CASE WHEN d.Day_Memo_reason_EN IN ('VL','SL','ML','BL','U/P')
# MAGIC                              AND d.Attendance_Day_Flag = 0
# MAGIC                              AND d.Shift_Day_Flag = 1
# MAGIC                              THEN 1 ELSE 0 END) * 100.0
# MAGIC                    / SUM(d.Shift_Day_Flag)
# MAGIC               ELSE 0
# MAGIC          END AS DECIMAL(10,2))                                AS LV_PCT,
# MAGIC 
# MAGIC     -- แยกตามประเภทลา (เฉพาะวันมีกะ)
# MAGIC     SUM(CASE WHEN d.Day_Memo_reason_EN = 'VL' AND d.Attendance_Day_Flag = 0 AND d.Shift_Day_Flag = 1 THEN 1 ELSE 0 END) AS LV_VL_D,
# MAGIC     SUM(CASE WHEN d.Day_Memo_reason_EN = 'SL' AND d.Attendance_Day_Flag = 0 AND d.Shift_Day_Flag = 1 THEN 1 ELSE 0 END) AS LV_SL_D,
# MAGIC     SUM(CASE WHEN d.Day_Memo_reason_EN = 'ML' AND d.Attendance_Day_Flag = 0 AND d.Shift_Day_Flag = 1 THEN 1 ELSE 0 END) AS LV_ML_D,
# MAGIC     SUM(CASE WHEN d.Day_Memo_reason_EN = 'BL' AND d.Attendance_Day_Flag = 0 AND d.Shift_Day_Flag = 1 THEN 1 ELSE 0 END) AS LV_BL_D,
# MAGIC     SUM(CASE WHEN d.Day_Memo_reason_EN = 'U/P' AND d.Attendance_Day_Flag = 0 AND d.Shift_Day_Flag = 1 THEN 1 ELSE 0 END) AS LV_UP_D,
# MAGIC 
# MAGIC     -- ════════════════════════════════════════════════════════════
# MAGIC     -- COVERED % — มีเหตุผลรองรับกี่%
# MAGIC     -- 100% = ทุกวันที่ไม่มา มีใบลาครบ ไม่มีขาดงาน
# MAGIC     -- สูงสุด 100%
# MAGIC     -- ════════════════════════════════════════════════════════════
# MAGIC     CAST(LEAST(
# MAGIC         CASE WHEN SUM(d.Shift_Day_Flag) > 0
# MAGIC               THEN (SUM(CASE WHEN d.Shift_Day_Flag = 1
# MAGIC                               THEN d.Attendance_Day_Flag ELSE 0 END)
# MAGIC                     + SUM(CASE WHEN d.Day_Memo_reason_EN IN ('VL','SL','ML','BL','U/P')
# MAGIC                                 AND d.Attendance_Day_Flag = 0
# MAGIC                                 AND d.Shift_Day_Flag = 1
# MAGIC                                 THEN 1 ELSE 0 END)) * 100.0
# MAGIC                    / SUM(d.Shift_Day_Flag)
# MAGIC               ELSE 0
# MAGIC          END,
# MAGIC          100.00
# MAGIC     ) AS DECIMAL(10,2))                                       AS CV_PCT,
# MAGIC 
# MAGIC     -- ════════════════════════════════════════════════════════════
# MAGIC     -- RISK — ระดับความเสี่ยง 🟣🔴🟠🟢
# MAGIC     -- SHRM 3% + ILO 25%
# MAGIC     -- นับ: ABS + L + SL + ML + BL + U/P
# MAGIC     -- ไม่นับ: VL (ลาพักร้อนเป็นสิทธิ์)
# MAGIC     -- ════════════════════════════════════════════════════════════
# MAGIC     CASE
# MAGIC         WHEN SUM(d.Shift_Day_Flag) = 0 THEN '🟢'
# MAGIC         WHEN (SUM(CASE WHEN d.Day_Memo_reason_EN IN ('SL','ML','BL','U/P','ABS','L')
# MAGIC                        THEN 1 ELSE 0 END) * 1.0
# MAGIC               / SUM(d.Shift_Day_Flag)) > 0.03
# MAGIC              AND
# MAGIC              (CASE WHEN SUM(CASE WHEN d.Shift_Day_Flag = 1
# MAGIC                               THEN d.Shift_Hours ELSE 0 END) > 0
# MAGIC                    THEN SUM(d.Total_OT_Hours)
# MAGIC                         / SUM(CASE WHEN d.Shift_Day_Flag = 1
# MAGIC                                    THEN d.Shift_Hours ELSE 0 END)
# MAGIC                    ELSE 0 END) > 0.25
# MAGIC              THEN '🟣'
# MAGIC         WHEN (SUM(CASE WHEN d.Day_Memo_reason_EN IN ('SL','ML','BL','U/P','ABS','L')
# MAGIC                        THEN 1 ELSE 0 END) * 1.0
# MAGIC               / SUM(d.Shift_Day_Flag)) > 0.03
# MAGIC              THEN '🔴'
# MAGIC         WHEN (CASE WHEN SUM(CASE WHEN d.Shift_Day_Flag = 1
# MAGIC                               THEN d.Shift_Hours ELSE 0 END) > 0
# MAGIC                    THEN SUM(d.Total_OT_Hours)
# MAGIC                         / SUM(CASE WHEN d.Shift_Day_Flag = 1
# MAGIC                                    THEN d.Shift_Hours ELSE 0 END)
# MAGIC                    ELSE 0 END) > 0.25
# MAGIC              THEN '🟠'
# MAGIC         ELSE '🟢'
# MAGIC     END                                                       AS RSK
# MAGIC 
# MAGIC FROM Gold_Production_Lakehouse.prod.gold_employee_time_bi d
# MAGIC GROUP BY d.Employee_Code;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
