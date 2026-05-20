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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Fabric Notebook: Ennovie Company Health Scanner
# =================================================
# 360° outlier detection across all Gold-layer tables.
# Produces structured findings + narrative coaching insights.
#
# Output table: Gold_Production_Lakehouse.delta.ml_company_health_scan
#
# Domains scanned:
#   1. Efficiency & Production
#   2. Metal Loss & Waste
#   3. Attendance & Workforce
#   4. Repair & Quality
#   5. Planning & Delivery
#
# Run cadence: Daily (via Fabric pipeline or scheduled notebook)
# Author: DPA Team (Ennovie)
# =================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# ============================================================
# CONFIG
# ============================================================
LAKEHOUSE   = "Gold_Production_Lakehouse"
SCHEMA      = "delta"
IQR_K       = 1.5
ZSCORE_K    = 3.0
RUN_TS      = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
RUN_DATE    = datetime.now().strftime("%Y-%m-%d")

# ============================================================
# HELPERS
# ============================================================

def tbl(name):
    return spark.sql(f"SELECT * FROM {LAKEHOUSE}.{SCHEMA}.{name}")


def safe_count(df, condition):
    """Return count of rows matching condition, 0 if empty."""
    try:
        return df.filter(condition).count()
    except:
        return 0


def pct(part, whole):
    """Safe percentage calculation."""
    return round(part / whole * 100, 1) if whole > 0 else 0


def top_samples(df, condition, cols, n=5):
    """Get top N sample values as list of dicts."""
    try:
        rows = df.filter(condition).select(*cols).limit(n).collect()
        return [r.asDict() for r in rows]
    except:
        return []


# ============================================================
# RESULT COLLECTOR
# ============================================================
# Each finding is a dict with a standard shape.
# At the end we convert to a Spark DF and persist.

findings = []

def add_finding(domain, table_name, finding_type, severity,
                what, so_what, do_what, metric_name=None,
                outlier_count=0, total_count=0, samples=None):
    """
    Add a finding to the collection.
    
    Args:
        domain:       'EFFICIENCY', 'METAL_LOSS', 'ATTENDANCE', 'QUALITY', 'PLANNING'
        table_name:   source table
        finding_type: 'STATISTICAL', 'BUSINESS_RULE', 'DATA_QUALITY', 'CROSS_DOMAIN'
        severity:     'CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'INFO'
        what:         Plain-English description of what the data shows
        so_what:      Why this matters for Ennovie operations
        do_what:      Specific action recommendation
        metric_name:  Column or metric involved
        outlier_count: Number of flagged records
        total_count:  Total records checked
        samples:      Sample values (list of dicts or strings, max 5)
    """
    findings.append({
        "run_date":       RUN_DATE,
        "run_ts":         RUN_TS,
        "domain":         domain,
        "table_name":     table_name,
        "finding_type":   finding_type,
        "severity":       severity,
        "what":           str(what)[:1000],
        "so_what":        str(so_what)[:1000],
        "do_what":        str(do_what)[:1000],
        "metric_name":    str(metric_name or "")[:200],
        "outlier_count":  int(outlier_count),
        "total_count":    int(total_count),
        "pct_affected":   round(outlier_count / max(total_count, 1) * 100, 2),
        "samples":        str(samples or [])[:2000],
    })


def add_stat_finding(domain, table_name, col_name, df, severity_override=None):
    """
    Run IQR + z-score on a column, add findings if outliers detected.
    """
    try:
        non_null = df.filter(F.col(col_name).isNotNull())
        total = non_null.count()
        if total < 10:
            return  # not enough data for meaningful stats

        stats = non_null.select(
            F.expr(f"percentile_approx(`{col_name}`, 0.25)").alias("q1"),
            F.expr(f"percentile_approx(`{col_name}`, 0.75)").alias("q3"),
            F.mean(col_name).alias("mu"),
            F.stddev(col_name).alias("sigma"),
            F.min(col_name).alias("col_min"),
            F.max(col_name).alias("col_max"),
        ).first()

        q1, q3 = stats["q1"], stats["q3"]
        mu, sigma = stats["mu"], stats["sigma"]

        if q1 is not None and q3 is not None:
            iqr = q3 - q1
            lower, upper = q1 - IQR_K * iqr, q3 + IQR_K * iqr
            outlier_cnt = non_null.filter(
                (F.col(col_name) < lower) | (F.col(col_name) > upper)
            ).count()

            if outlier_cnt > 0:
                pct_aff = pct(outlier_cnt, total)
                samples = top_samples(
                    non_null,
                    (F.col(col_name) < lower) | (F.col(col_name) > upper),
                    [col_name], 5
                )
                sev = severity_override or (
                    "HIGH" if pct_aff > 10
                    else "MEDIUM" if pct_aff > 3
                    else "LOW"
                )
                add_finding(
                    domain, table_name, "STATISTICAL", sev,
                    what=f"{outlier_cnt} values in `{col_name}` fall outside the normal range "
                         f"(IQR bounds: {lower:.2f} to {upper:.2f}). "
                         f"Range in data: {stats['col_min']} to {stats['col_max']}.",
                    so_what=f"{pct_aff}% of records are statistical outliers. "
                            f"These could be data entry errors, system glitches, or genuinely exceptional events worth investigating.",
                    do_what=f"Review the {outlier_cnt} flagged records in `{table_name}`.`{col_name}`. "
                            f"Check if they're legitimate edge cases or data issues that need correction at source (BC/RFID).",
                    metric_name=col_name,
                    outlier_count=outlier_cnt,
                    total_count=total,
                    samples=samples,
                )

        # Z-score check (complementary — catches different shape outliers)
        if sigma and sigma > 0:
            z_cnt = non_null.filter(
                F.abs((F.col(col_name) - F.lit(mu)) / F.lit(sigma)) > ZSCORE_K
            ).count()
            if z_cnt > 0 and z_cnt != outlier_cnt:  # only add if different from IQR
                add_finding(
                    domain, table_name, "STATISTICAL",
                    severity_override or ("MEDIUM" if z_cnt > 5 else "LOW"),
                    what=f"{z_cnt} values in `{col_name}` are more than {ZSCORE_K} standard deviations from the mean ({mu:.2f}).",
                    so_what="Extreme values that may skew averages and KPIs shown in dashboards.",
                    do_what=f"Cross-check these extreme values. If legitimate, consider using median instead of mean in reporting for `{col_name}`.",
                    metric_name=col_name,
                    outlier_count=z_cnt,
                    total_count=total,
                )
    except Exception as e:
        add_finding(
            domain, table_name, "DATA_QUALITY", "LOW",
            what=f"Could not run statistical check on `{col_name}`: {str(e)[:200]}",
            so_what="Column may have unexpected data types or all-null values.",
            do_what=f"Verify `{col_name}` data type and completeness in the pipeline.",
            metric_name=col_name,
        )


# ============================================================
# DOMAIN 1: EFFICIENCY & PRODUCTION
# ============================================================

def scan_actual_time_by_employees():
    T = "gold_delta_actual_time_by_employees"
    df = tbl(T)
    total = df.count()
    if total == 0:
        return

    D = "EFFICIENCY"

    # --- Statistical scans ---
    for c in ["quantity", "remaining_quantity", "plan_run_time", "total_plan_runtime", "actual_run_time_min"]:
        add_stat_finding(D, T, c, df)

    # --- BR: Zero or negative runtime ---
    n = safe_count(df, F.col("actual_run_time_min") <= 0)
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "HIGH",
            what=f"{n} records ({pct(n,total)}%) have zero or negative actual runtime.",
            so_what="These records distort efficiency calculations. An employee appears infinitely productive if runtime = 0 but quantity > 0. This also breaks the per-piece time metrics in the employee efficiency dashboard.",
            do_what="Filter these from efficiency calculations immediately. Then trace root cause: likely RFID scan-out without scan-in, or BC posting with missing time capture. Check if specific machine centers or employees are repeat offenders.",
            metric_name="actual_run_time_min", outlier_count=n, total_count=total,
            samples=top_samples(df, F.col("actual_run_time_min") <= 0, ["employee_no", "prod_order_no", "actual_run_time_min", "quantity"]),
        )

    # --- BR: Extremely long runtime (> 8 hrs single record) ---
    n = safe_count(df, F.col("actual_run_time_min") > 480)
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "MEDIUM",
            what=f"{n} records show more than 8 hours of runtime on a single operation.",
            so_what="Either the employee forgot to scan out (RFID stuck), or a complex multi-day job wasn't broken into daily entries. This inflates the cell line's reported work hours and drags down efficiency percentages.",
            do_what="Set up an automated alert in Power Automate: if any RFID scan pair exceeds 8 hours, notify the line supervisor to verify. Consider adding an auto-close rule at shift end.",
            metric_name="actual_run_time_min", outlier_count=n, total_count=total,
            samples=top_samples(df, F.col("actual_run_time_min") > 480, ["employee_no", "machine_center_no", "actual_run_time_min", "prod_order_no"]),
        )

    # --- BR: Missing plan runtime (routing gap) ---
    n = safe_count(df, (F.col("plan_run_time") == 0) & (F.col("actual_run_time_min") > 0))
    if n > 0:
        items = top_samples(df, (F.col("plan_run_time") == 0) & (F.col("actual_run_time_min") > 0),
                            ["item_no", "machine_center_no", "operation_no"])
        add_finding(D, T, "BUSINESS_RULE", "HIGH",
            what=f"{n} records have actual runtime but zero planned runtime — the BC routing standard is missing.",
            so_what="Without a plan runtime, you cannot measure efficiency for these operations. They show up as 'N/A' or 'infinity' in dashboards, and they're invisible to the employee efficiency scoring model. This is a silent blind spot.",
            do_what="Export the distinct item_no + machine_center_no + operation_no combinations and send to the production engineering team to set up routing standards in BC. Prioritize by volume — fix the top 10 item/operation combos first.",
            metric_name="plan_run_time", outlier_count=n, total_count=total,
            samples=items,
        )

    # --- BR: Negative quantity (BC adjustment) ---
    n = safe_count(df, F.col("quantity") < 0)
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "LOW",
            what=f"{n} records have negative quantity — these are BC negative output adjustments.",
            so_what="Expected behavior from BC reversals, but if the count is growing month-over-month, it signals increasing production errors that require correction. Each reversal means someone posted wrong initially.",
            do_what="Track the monthly trend of negative postings. If increasing, investigate which operators or item types generate the most reversals. Consider adding a confirmation step in the production posting flow.",
            metric_name="quantity", outlier_count=n, total_count=total,
        )

    # --- BR: Timestamp anomaly ---
    n = safe_count(df, F.col("created_on") > F.col("modified_on"))
    if n > 0:
        add_finding(D, T, "DATA_QUALITY", "MEDIUM",
            what=f"{n} records have created_on timestamp later than modified_on.",
            so_what="This shouldn't happen — it suggests a timezone conversion issue or a BC sync timing bug in the Fabric pipeline.",
            do_what="Check the Fabric mirroring pipeline for timezone handling. BC stores timestamps in UTC but the RFID system may post in ICT (UTC+7). Align both to UTC before comparison.",
            metric_name="created_on vs modified_on", outlier_count=n, total_count=total,
        )

    # --- Null checks ---
    for c in ["prod_order_no", "employee_no", "item_no", "machine_center_no"]:
        n = safe_count(df, F.col(c).isNull())
        if n > 0:
            add_finding(D, T, "DATA_QUALITY", "HIGH" if c in ["prod_order_no", "employee_no"] else "MEDIUM",
                what=f"{n} records have NULL `{c}` — a required field.",
                so_what=f"Records without `{c}` cannot be joined to other tables, making them orphans in reporting. They're counted in totals but invisible in drill-downs, causing number mismatches between summary and detail views.",
                do_what=f"Add a NOT NULL validation in the Silver→Gold pipeline for `{c}`. Route failing records to a quarantine table for manual review.",
                metric_name=c, outlier_count=n, total_count=total,
            )


def scan_employee_efficiency():
    T = "gold_delta_employee_efficiency"
    df = tbl(T)
    total = df.count()
    if total == 0:
        return

    D = "EFFICIENCY"

    for c in ["total_qty", "actual_run_time", "actual_efficiency", "peer_run_time", "peer_efficiency", "bc_std_run_time"]:
        add_stat_finding(D, T, c, df)

    # --- BR: Extreme efficiency > 300% ---
    n = safe_count(df, F.col("actual_efficiency") > 300)
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "HIGH",
            what=f"{n} employee-operation combos show over 300% efficiency.",
            so_what="Nobody is 3× faster than the standard — this usually means the BC routing standard run time is too generous (set for a slower process that's been improved) or the RFID captured fewer minutes than actually worked. These inflate the leaderboard and make normal performers look bad.",
            do_what="Pull the distinct item_no + operation_no for these records and review the BC routing standards. They likely need updating. Also check if these employees share scan badges (buddy punching).",
            metric_name="actual_efficiency", outlier_count=n, total_count=total,
            samples=top_samples(df, F.col("actual_efficiency") > 300, ["employee_no", "item_no", "machine_center_no", "actual_efficiency", "bc_std_run_time"]),
        )

    # --- BR: Zero/negative efficiency ---
    n = safe_count(df, F.col("actual_efficiency") <= 0)
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "HIGH",
            what=f"{n} records show zero or negative efficiency.",
            so_what="These employees appear non-productive in dashboards even though they may have worked. Likely caused by missing runtime data or a division error in the Gold layer calculation.",
            do_what="Check if `actual_run_time` is 0 or NULL for these records. If so, the RFID time capture failed. Add a fallback: if actual runtime is missing, use the shift's standard work hours as a proxy.",
            metric_name="actual_efficiency", outlier_count=n, total_count=total,
        )

    # --- BR: Ghost production (0 runtime, positive qty) ---
    n = safe_count(df, (F.col("actual_run_time") == 0) & (F.col("total_qty") > 0))
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "CRITICAL",
            what=f"{n} records show output quantity but ZERO actual runtime — 'ghost production'.",
            so_what="This is a data integrity red flag. Either the RFID system didn't capture the work session, or output was posted manually without a corresponding time entry. These records make efficiency metrics unreliable.",
            do_what="Implement a pipeline validation: every output posting MUST have a matching time entry. Flag mismatches in a daily data quality report sent to line supervisors via Teams.",
            metric_name="actual_run_time", outlier_count=n, total_count=total,
            samples=top_samples(df, (F.col("actual_run_time") == 0) & (F.col("total_qty") > 0), ["employee_no", "item_no", "total_qty", "prod_line"]),
        )

    # --- BR: Missing BC standard ---
    n = safe_count(df, F.col("bc_std_run_time") == 0)
    if n > 0:
        items = [r["item_no"] for r in df.filter(F.col("bc_std_run_time") == 0).select("item_no").distinct().limit(15).collect()]
        add_finding(D, T, "BUSINESS_RULE", "HIGH",
            what=f"{n} records reference items with no BC routing standard time.",
            so_what="Without standards, efficiency is meaningless for these items. They represent a measurement gap — you're tracking time but can't evaluate performance.",
            do_what=f"These items need routing setup in BC: {items[:10]}. Prioritize by production volume. As a quick fix, use the median actual time from the last 30 days as a temporary standard.",
            metric_name="bc_std_run_time", outlier_count=n, total_count=total,
            samples=items[:5],
        )

    # --- BR: Peer vs actual divergence ---
    n = safe_count(df, F.abs(F.col("actual_efficiency") - F.col("peer_efficiency")) > 200)
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "MEDIUM",
            what=f"{n} records show a gap of more than 200 percentage points between an employee's efficiency and their peer average.",
            so_what="Large divergence means either this employee is extraordinary (unlikely at > 200pt gap) or the data is wrong. It also makes the peer benchmark unreliable for everyone in that group.",
            do_what="Investigate the top 5 divergent cases. Check if the employee is on a different routing version or if their RFID data has timing errors.",
            metric_name="actual_efficiency vs peer_efficiency", outlier_count=n, total_count=total,
        )


def scan_efficiency_rate():
    T = "gold_delta_efficiency_rate"
    df = tbl(T)
    total = df.count()
    if total == 0:
        return

    D = "EFFICIENCY"
    for c in ["total_out_qty", "total_runtime_min", "total_workhour_min", "efficiency_pct", "department_efficiency_pct"]:
        add_stat_finding(D, T, c, df)

    for c in ["efficiency_pct", "department_efficiency_pct"]:
        n = safe_count(df, (F.col(c) > 200) | (F.col(c) < 0))
        if n > 0:
            add_finding(D, T, "BUSINESS_RULE", "HIGH",
                what=f"{n} sub-department/date combos have `{c}` outside the 0–200% range.",
                so_what="These inflate or deflate the department-level KPIs shown on the TV dashboards. Supervisors may be making staffing decisions based on skewed numbers.",
                do_what=f"Add a cap/floor in the Gold layer: clamp `{c}` to 0–200% for display, but keep raw values in a separate column for investigation.",
                metric_name=c, outlier_count=n, total_count=total,
            )

    # Zero output but runtime > 0
    n = safe_count(df, (F.col("total_out_qty") == 0) & (F.col("total_runtime_min") > 0))
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "MEDIUM",
            what=f"{n} sub-department/date combos logged runtime but zero output.",
            so_what="This is wasted capacity — people clocked time but no finished pieces were recorded. Could be setup time, rework, or a posting delay where output gets recorded the next day.",
            do_what="Check if these are consistently the same sub-departments. If so, the routing may include non-output steps (e.g., setup, cleaning) that need to be separated from productive time.",
            metric_name="total_out_qty", outlier_count=n, total_count=total,
        )


def scan_work_cell_performance():
    T = "gold_delta_work_cell_performance"
    df = tbl(T)
    total = df.count()
    if total == 0:
        return

    D = "EFFICIENCY"
    for c in ["efficiency_pct", "employee_count", "total_runtime_min", "total_work_min"]:
        add_stat_finding(D, T, c, df)

    n = safe_count(df, (F.col("efficiency_pct") > 200) | (F.col("efficiency_pct") < 0))
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "HIGH",
            what=f"{n} cell/date combos have efficiency outside 0–200%.",
            so_what="Cell performance is displayed on the factory TV dashboards. Extreme values mislead supervisors and undermine trust in the data system.",
            do_what="Investigate the underlying employee-level data for these cells. Usually one employee with bad RFID data skews the whole cell.",
            metric_name="efficiency_pct", outlier_count=n, total_count=total,
        )

    n = safe_count(df, F.col("employee_count") == 0)
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "HIGH",
            what=f"{n} cell performance records show 0 employees but have runtime data.",
            so_what="Ghost cell lines — data exists for a cell that supposedly has no workers. This usually happens when employees transfer between cells mid-day and the headcount snapshot misses them.",
            do_what="Check the employee assignment logic in `gold_delta_current_employees`. Consider using a time-weighted headcount instead of a point-in-time snapshot.",
            metric_name="employee_count", outlier_count=n, total_count=total,
        )

    n = safe_count(df, (F.col("total_work_min") == 0) & (F.col("total_runtime_min") > 0))
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "HIGH",
            what=f"{n} cells have productive runtime but zero available work minutes.",
            so_what="The denominator is zero, so efficiency is undefined. This usually means attendance data didn't load for that day, making the cell's efficiency uncalculable.",
            do_what="Add a dependency check: the Gold layer efficiency pipeline should verify that attendance data exists before computing. If missing, flag the cell as 'DATA PENDING' instead of showing a misleading number.",
            metric_name="total_work_min", outlier_count=n, total_count=total,
        )


def scan_line_utilization():
    T = "gold_delta_line_utilization"
    df = tbl(T)
    total = df.count()
    if total == 0:
        return

    D = "EFFICIENCY"
    for c in ["headcount", "productive_minutes", "capacity_minutes", "line_utilization"]:
        add_stat_finding(D, T, c, df)

    n = safe_count(df, (F.col("line_utilization") > 100) | (F.col("line_utilization") < 0))
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "HIGH",
            what=f"{n} line/day combos have utilization outside 0–100%.",
            so_what="Over 100% means more productive time was logged than available capacity — likely OT hours not included in the capacity calculation.",
            do_what="Review the capacity formula: `capacity_minutes` should include approved OT. If it only uses standard weekday hours, add `ot_hours × 60` from the crew table.",
            metric_name="line_utilization", outlier_count=n, total_count=total,
        )

    n = safe_count(df, (F.col("headcount") == 0) & (F.col("productive_minutes") > 0))
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "HIGH",
            what=f"{n} lines show production but zero headcount.",
            so_what="Same root cause as work cell performance ghost cells — HR/attendance data not synced.",
            do_what="Cross-reference with `gold_delta_prod_line_crew` for the same date. If crew data exists but headcount shows 0, the join key is wrong.",
            metric_name="headcount", outlier_count=n, total_count=total,
        )


def scan_output_qty():
    T = "gold_delta_output_qty"
    df = tbl(T)
    total = df.count()
    if total == 0:
        return

    D = "EFFICIENCY"
    add_stat_finding(D, T, "total_output_quantity", df)

    n = safe_count(df, (F.col("total_output_quantity") == 0) & (F.dayofweek(F.col("output_date")).isin([2,3,4,5,6])))
    if n > 0:
        samples = top_samples(df,
            (F.col("total_output_quantity") == 0) & (F.dayofweek(F.col("output_date")).isin([2,3,4,5,6])),
            ["output_date", "department"])
        add_finding(D, T, "BUSINESS_RULE", "MEDIUM",
            what=f"{n} weekday records show zero output.",
            so_what="Either the factory was shut (holiday/event) or output postings were delayed. If delayed, the following day will show an unusually high spike — creating a sawtooth pattern that confuses trend analysis.",
            do_what="Cross-reference with Thai public holidays and Ennovie's factory calendar. Tag known holidays in the data so dashboards can filter them out of trend calculations.",
            metric_name="total_output_quantity", outlier_count=n, total_count=total,
            samples=samples,
        )

    n = safe_count(df, F.col("total_output_quantity") < 0)
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "HIGH",
            what=f"{n} records have negative output — BC reversal entries.",
            so_what="Negative output days pull down weekly/monthly totals. If not understood, management may think production dropped when it was just a correction.",
            do_what="Add a 'gross output' vs 'net output' split in reporting. Show reversals separately so the true production volume is visible.",
            metric_name="total_output_quantity", outlier_count=n, total_count=total,
        )


# ============================================================
# DOMAIN 2: METAL LOSS & WASTE
# ============================================================

def scan_metal_loss():
    T = "gold_delta_metal_loss"
    df = tbl(T)
    if df.count() == 0:
        return

    D = "METAL_LOSS"
    add_stat_finding(D, T, "flag_count", df)

    total_flags = df.agg(F.sum("flag_count")).first()[0] or 0
    over_flags  = df.filter(F.col("loss_after_scrap_std_flag") == "OVER").agg(F.sum("flag_count")).first()[0] or 0

    if total_flags > 0 and (over_flags / total_flags) > 0.5:
        add_finding(D, T, "BUSINESS_RULE", "HIGH",
            what=f"{pct(over_flags, total_flags)}% of all metal loss flags are OVER standard ({over_flags} of {total_flags}).",
            so_what="More than half of all metal loss events exceed the standard — this means the standards themselves may be too tight, or there's a systemic process issue. Either way, precious metal is leaving the factory faster than planned, directly hitting cost of goods.",
            do_what="Two-pronged approach: (1) Review the loss standards with production engineering — were they set for ideal conditions? (2) Identify which cells and employees drive the most OVER flags and run a process audit. Start with the top 3 cells.",
            metric_name="loss_after_scrap_std_flag", outlier_count=int(over_flags), total_count=int(total_flags),
        )


def scan_metal_loss_by_employee():
    T = "gold_delta_metal_loss_by_employee"
    df = tbl(T)
    total = df.count()
    if total == 0:
        return

    D = "METAL_LOSS"
    for c in ["total_consumption_g", "total_loss_g", "actual_total_loss_pct",
              "scrap_variance_pct", "dust_variance_pct", "total_loss_variance_pct"]:
        add_stat_finding(D, T, c, df)

    # Extreme loss > 20%
    n = safe_count(df, F.col("actual_total_loss_pct") > 20)
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "CRITICAL",
            what=f"{n} employees have total metal loss above 20%.",
            so_what="In jewelry manufacturing, losing > 20% of precious metal is extremely costly. At current gold prices, even small percentage increases translate to significant ฿ losses. This could indicate technique issues, equipment problems, or measurement errors.",
            do_what="Immediately review these employees with their supervisors. Check: (1) Are they on complex items that naturally have higher loss? (2) Is their equipment (polishing wheels, filing stations) worn? (3) Are they properly trained in scrap collection? Priority: address the top 3 by total_loss_g first.",
            metric_name="actual_total_loss_pct", outlier_count=n, total_count=total,
            samples=top_samples(df, F.col("actual_total_loss_pct") > 20, ["employee_code", "first_name_eng", "department", "actual_total_loss_pct", "total_loss_g"]),
        )

    # Negative loss (metal gain)
    n = safe_count(df, F.col("total_loss_g") < 0)
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "HIGH",
            what=f"{n} employees show negative metal loss — they supposedly gained metal.",
            so_what="Metal doesn't appear from nowhere. This is a data integrity issue, likely caused by incorrect FL (finishing loss) allowance calculations or mismatched consumption vs output weight records.",
            do_what="Check the FL allowance formula for these employees' items. Also verify that the consumption posting in BC matches the actual metal issued. The finding/casting weights may be swapped.",
            metric_name="total_loss_g", outlier_count=n, total_count=total,
        )

    # Chronic scrap over-standard
    n = safe_count(df, (F.col("working_days") > 5) & (F.col("scrap_over_std_days") > F.col("working_days") * 0.8))
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "HIGH",
            what=f"{n} employees exceeded the scrap standard on more than 80% of their working days.",
            so_what="These aren't one-off bad days — this is a chronic pattern. Either the standard is unrealistic for their assigned items, or they need retraining. Chronic over-standard employees should not be on high-value items.",
            do_what="Create a 'metal loss watchlist' in the daily briefing. For employees on this list > 2 consecutive weeks: (1) reassign to lower-value items, (2) pair with a mentor, (3) check equipment.",
            metric_name="scrap_over_std_days", outlier_count=n, total_count=total,
            samples=top_samples(df, (F.col("working_days") > 5) & (F.col("scrap_over_std_days") > F.col("working_days") * 0.8),
                                ["employee_code", "first_name_eng", "scrap_over_std_days", "working_days"]),
        )

    # Chronic dust over-standard
    n = safe_count(df, (F.col("working_days") > 5) & (F.col("dust_over_std_days") > F.col("working_days") * 0.8))
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "HIGH",
            what=f"{n} employees exceeded the dust standard on more than 80% of their working days.",
            so_what="Dust loss is harder to recover than scrap. Chronic dust over-standard suggests equipment issues (ventilation, collection trays) or technique problems (filing angle, polishing speed).",
            do_what="Inspect the dust collection equipment at these employees' stations. Often a clogged filter or mispositioned collection tray is the culprit. Cheaper to fix equipment than to lose gold.",
            metric_name="dust_over_std_days", outlier_count=n, total_count=total,
        )


def scan_metal_loss_by_line_employee():
    T = "gold_delta_metal_loss_by_line_employee"
    df = tbl(T)
    total = df.count()
    if total == 0:
        return

    D = "METAL_LOSS"
    for c in ["total_consumption", "weight_after_FL", "Scrap", "Dust", "loss_g", "loss_pct"]:
        add_stat_finding(D, T, c, df)

    n = safe_count(df, F.col("loss_pct") > 15)
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "CRITICAL",
            what=f"{n} daily records show metal loss above 15% in a single day.",
            so_what="A single-day loss above 15% is a severe event — it could be a dropped piece, a melting accident, or a weighing error. At current precious metal prices, even one incident at this level could cost thousands of baht.",
            do_what="These should trigger same-day alerts. Set up a Power Automate flow: when the daily metal loss pipeline runs, if any record exceeds 15%, immediately notify the line supervisor and quality manager via Teams.",
            metric_name="loss_pct", outlier_count=n, total_count=total,
            samples=top_samples(df, F.col("loss_pct") > 15, ["Name", "Line", "Metal", "loss_pct", "loss_g", "Date"]),
        )

    for c in ["Scrap", "Dust"]:
        n = safe_count(df, F.col(c) < 0)
        if n > 0:
            add_finding(D, T, "DATA_QUALITY", "HIGH",
                what=f"{n} records have negative `{c}` — physically impossible.",
                so_what=f"Negative {c.lower()} means the calculation is wrong somewhere. Likely the weight_after_FL is greater than total_consumption, or the finding/casting components are double-counted.",
                do_what=f"Review the {c.lower()} calculation formula in the Gold layer pipeline. Check the order of operations: consumption - weight_after_FL - findings - castings - returns.",
                metric_name=c, outlier_count=n, total_count=total,
            )


def scan_metal_loss_by_cell_trend():
    T = "gold_delta_metal_loss_by_cell_trend"
    df = tbl(T)
    if df.count() == 0:
        return

    D = "METAL_LOSS"
    for c in ["under_count", "over_count", "under_loss_g", "over_loss_g"]:
        add_stat_finding(D, T, c, df)

    # Cells with chronic over > 3× under
    agg = df.groupBy("Cell").agg(
        F.sum("over_count").alias("t_over"),
        F.sum("under_count").alias("t_under"),
    ).filter(F.col("t_over") > F.col("t_under") * 3)
    n = agg.count()
    if n > 0:
        cells = [r["Cell"] for r in agg.limit(10).collect()]
        add_finding(D, T, "BUSINESS_RULE", "HIGH",
            what=f"{n} cells have over-standard counts more than 3× their under-standard counts.",
            so_what="These cells are systematically losing more metal than the standard allows. It's not random variation — it's a process problem at the cell level.",
            do_what=f"Focus improvement efforts on these cells: {cells[:5]}. Conduct a 5-why analysis for each: equipment? training? item complexity? material quality?",
            metric_name="Cell", outlier_count=n, total_count=0,
            samples=cells[:5],
        )


def scan_metal_loss_card():
    T = "gold_delta_metal_loss_card"
    df = tbl(T)
    if df.count() == 0:
        return

    D = "METAL_LOSS"
    n = safe_count(df, F.col("yesterday_over") > F.col("yesterday_under") * 2)
    if n > 0:
        lines = top_samples(df, F.col("yesterday_over") > F.col("yesterday_under") * 2,
                            ["Line", "yesterday_over", "yesterday_under"])
        add_finding(D, T, "BUSINESS_RULE", "MEDIUM",
            what=f"{n} production lines had yesterday's OVER flags more than double their UNDER flags.",
            so_what="Yesterday was a bad day for metal loss on these lines. Worth checking if something specific happened — new batch of material, substitute employee, equipment change.",
            do_what="Include these lines in today's morning standup discussion. Check if the pattern continues today — if so, escalate.",
            metric_name="yesterday_over vs yesterday_under", outlier_count=n, total_count=df.count(),
            samples=lines,
        )


# ============================================================
# DOMAIN 3: ATTENDANCE & WORKFORCE
# ============================================================

def scan_attendance():
    T = "gold_delta_attendance"
    df = tbl(T)
    total = df.count()
    if total == 0:
        return

    D = "ATTENDANCE"
    for c in ["present_count", "total_employees", "attendance_rate", "avg_attendance_rate_30d"]:
        add_stat_finding(D, T, c, df)

    n = safe_count(df, F.col("attendance_rate") > 100)
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "CRITICAL",
            what=f"{n} department/day combos show attendance rate above 100%.",
            so_what="More people present than the total headcount — impossible. This means the headcount denominator is stale or the presence count includes temporary workers/trainees not in the employee master.",
            do_what="Sync the `total_employees` denominator with BC's current active employee list daily, not monthly. Add temp workers to the count if they're scanning RFID.",
            metric_name="attendance_rate", outlier_count=n, total_count=total,
            samples=top_samples(df, F.col("attendance_rate") > 100, ["work_day", "department", "present_count", "total_employees", "attendance_rate"]),
        )

    n = safe_count(df, F.col("attendance_rate") < 50)
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "HIGH",
            what=f"{n} department/day combos show attendance below 50%.",
            so_what="Less than half the workforce showed up. If this isn't a holiday, it's a major operational disruption — production targets will be missed, and the remaining workers are likely overloaded.",
            do_what="Cross-reference with the Thai holiday calendar. If not a holiday, investigate: mass leave event? transportation issue? Are these concentrated in one department (indicating a morale problem)?",
            metric_name="attendance_rate", outlier_count=n, total_count=total,
            samples=top_samples(df, F.col("attendance_rate") < 50, ["work_day", "department", "attendance_rate"]),
        )

    n = safe_count(df, F.col("present_count") > F.col("total_employees"))
    if n > 0:
        add_finding(D, T, "DATA_QUALITY", "CRITICAL",
            what=f"{n} records have more people present than total employees.",
            so_what="Same root cause as attendance > 100%. Every downstream metric that uses headcount as a denominator is affected.",
            do_what="Priority fix: update the headcount source. Use `gold_delta_cell_and_department.headcount` and ensure it refreshes daily.",
            metric_name="present_count vs total_employees", outlier_count=n, total_count=total,
        )


def scan_late_absent_trend():
    T = "gold_delta_late_absent_trend_employee"
    df = tbl(T)
    total = df.count()
    if total == 0:
        return

    D = "ATTENDANCE"
    for c in ["late_rate_7d_ma_pct", "absent_rate_7d_ma_pct", "late_minutes", "absent_minutes",
              "cumulative_late_days", "cumulative_absent_days"]:
        add_stat_finding(D, T, c, df)

    # Rate out of range
    for c, label in [("late_rate_7d_ma_pct", "late"), ("absent_rate_7d_ma_pct", "absent")]:
        n = safe_count(df, (F.col(c) > 100) | (F.col(c) < 0))
        if n > 0:
            add_finding(D, T, "BUSINESS_RULE", "HIGH",
                what=f"{n} records have 7-day moving average {label} rate outside 0–100%.",
                so_what="The moving average calculation has a bug — rates can't exceed 100%. This invalidates the trend analysis for these employees.",
                do_what=f"Check the `{c}` calculation in the Gold pipeline. Likely the denominator (days in window) is wrong — could be 0 or the window includes future dates.",
                metric_name=c, outlier_count=n, total_count=total,
            )

    # Late flag contradiction
    n = safe_count(df, (F.col("is_late") == 1) & (F.col("late_minutes") == 0))
    if n > 0:
        add_finding(D, T, "DATA_QUALITY", "MEDIUM",
            what=f"{n} records are flagged as late but show 0 late minutes.",
            so_what="Contradictory data — the flag says late but the minutes say on time. One of them is wrong, and downstream reports using either field will disagree.",
            do_what="Align the definitions: `is_late` should be derived from `late_minutes > 0`, not set independently. Fix in the Silver layer transformation.",
            metric_name="is_late vs late_minutes", outlier_count=n, total_count=total,
        )

    # Extreme lateness (> 8 hrs = basically absent)
    n = safe_count(df, F.col("late_minutes") > 480)
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "MEDIUM",
            what=f"{n} records show lateness exceeding 8 hours — these should probably be classified as absent instead.",
            so_what="Classifying an 8+ hour late arrival as 'late' rather than 'absent' understates the absence rate and inflates late counts.",
            do_what="Add a business rule in the pipeline: if late_minutes > shift_duration (typically 480 min), reclassify as absent. This brings attendance metrics closer to reality.",
            metric_name="late_minutes", outlier_count=n, total_count=total,
        )

    # Cumulative values decreasing (window function bug)
    w = Window.partitionBy("Employee_Code").orderBy("work_date")
    df_lag = df.withColumn("prev_cum_late", F.lag("cumulative_late_days").over(w))
    n = safe_count(df_lag, F.col("prev_cum_late").isNotNull() & (F.col("cumulative_late_days") < F.col("prev_cum_late")))
    if n > 0:
        add_finding(D, T, "DATA_QUALITY", "HIGH",
            what=f"{n} records show cumulative late days DECREASING — cumulative values should only increase.",
            so_what="This is a confirmed bug in the Gold layer window function. The cumulative column is unreliable and any leaderboard or scoring system built on it will produce wrong rankings.",
            do_what="Fix the window function: ensure it uses `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` and that the partition is correctly set to employee + year (or reset annually). Re-run the pipeline after fixing.",
            metric_name="cumulative_late_days", outlier_count=n, total_count=total,
        )


def scan_prod_line_crew():
    T = "gold_delta_prod_line_crew"
    df = tbl(T)
    total = df.count()
    if total == 0:
        return

    D = "ATTENDANCE"
    for c in ["total", "present", "late", "absent", "ot_hours"]:
        add_stat_finding(D, T, c, df)

    n = safe_count(df, (F.col("present") + F.col("absent")) > F.col("total"))
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "CRITICAL",
            what=f"{n} crew records have present + absent > total headcount — double counting.",
            so_what="The math doesn't add up. People are being counted in both present and absent, or the total doesn't include all categories. This breaks every attendance metric downstream.",
            do_what="Review the crew aggregation logic. `present` + `absent` + `other` should equal `total`. Check if 'late' employees are being counted in both 'present' and 'late', inflating the sum.",
            metric_name="present + absent vs total", outlier_count=n, total_count=total,
        )

    for c in ["total", "present", "late", "absent", "ot_hours"]:
        n = safe_count(df, F.col(c) < 0)
        if n > 0:
            add_finding(D, T, "DATA_QUALITY", "HIGH",
                what=f"{n} crew records have negative `{c}`.",
                so_what="Negative headcounts or hours are physically impossible. This is a data error in the Silver→Gold pipeline.",
                do_what=f"Add `GREATEST({c}, 0)` in the Gold layer to prevent negative values. Investigate the source: likely a sign error in a CASE WHEN or a bad join producing negative aggregates.",
                metric_name=c, outlier_count=n, total_count=total,
            )

    # Excessive OT
    n = safe_count(df, (F.col("total") > 0) & (F.col("ot_hours") > F.col("total") * 4))
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "MEDIUM",
            what=f"{n} crew records show average OT exceeding 4 hours per person.",
            so_what="While occasional OT is normal, averaging > 4 hours per person suggests understaffing or poor planning. Extended OT leads to fatigue, which increases metal loss and defect rates — connecting this to quality issues.",
            do_what="Cross-reference these dates/roles with output data: is the extra OT actually producing proportionally more output? If not, it's unproductive OT. Consider hiring temporary staff for peak periods.",
            metric_name="ot_hours", outlier_count=n, total_count=total,
        )


def scan_absent():
    T = "gold_delta_absent"
    df = tbl(T)
    if df.count() == 0:
        return

    D = "ATTENDANCE"
    for c in ["total_leave_minutes", "total_leave_days"]:
        add_stat_finding(D, T, c, df)

    n = safe_count(df, F.col("total_leave_days") > 30)
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "MEDIUM",
            what=f"{n} employees have accumulated more than 30 leave days.",
            so_what="Extended absences impact cell line continuity. Other workers have to cover, which can increase their error rates and metal loss.",
            do_what="Flag these employees to HR for review. If medical leave, plan for a temporary replacement. If voluntary, check if there's a pattern (specific department, time of year).",
            metric_name="total_leave_days", outlier_count=n, total_count=df.count(),
            samples=top_samples(df, F.col("total_leave_days") > 30, ["employee_code", "employee_name", "department", "total_leave_days"]),
        )

    n = safe_count(df, F.col("total_leave_minutes") < 0)
    if n > 0:
        add_finding(D, T, "DATA_QUALITY", "HIGH",
            what=f"{n} employees show negative leave minutes.",
            so_what="Data error — negative leave is impossible.",
            do_what="Check the aggregation logic. Likely a sign error when converting leave types or a double-subtraction in the pipeline.",
            metric_name="total_leave_minutes", outlier_count=n, total_count=df.count(),
        )


def scan_ot():
    T = "gold_delta_ot"
    df = tbl(T)
    if df.count() == 0:
        return

    D = "ATTENDANCE"
    for c in ["total_ot_hours", "total_ot_days"]:
        add_stat_finding(D, T, c, df)

    n = safe_count(df, F.col("total_ot_hours") > 80)
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "HIGH",
            what=f"{n} employees have logged more than 80 OT hours.",
            so_what="Thai labor law limits OT to 36 hours/week. 80+ total hours may indicate compliance risk depending on the period. Beyond legal risk, excessive OT correlates with higher defect rates and metal loss in manufacturing.",
            do_what="Review the time period for this data. If it's monthly, 80 hours may be borderline. If weekly, it's a clear violation. Send an automated alert when any employee crosses the legal threshold.",
            metric_name="total_ot_hours", outlier_count=n, total_count=df.count(),
            samples=top_samples(df, F.col("total_ot_hours") > 80, ["employee_code", "employee_name", "department", "total_ot_hours"]),
        )

    n = safe_count(df, F.col("total_ot_hours") < 0)
    if n > 0:
        add_finding(D, T, "DATA_QUALITY", "HIGH",
            what=f"{n} employees show negative OT hours.",
            so_what="Negative OT is a data error.",
            do_what="Fix in pipeline: apply `GREATEST(total_ot_hours, 0)` and investigate the source of negative values.",
            metric_name="total_ot_hours", outlier_count=n, total_count=df.count(),
        )


# ============================================================
# DOMAIN 4: REPAIR & QUALITY
# ============================================================

def scan_repair_rate():
    T = "gold_delta_repair_rate"
    df = tbl(T)
    total = df.count()
    if total == 0:
        return

    D = "QUALITY"
    for c in ["total_prod_qty", "total_repair_qty", "repair_rate_cell_line", "repair_rate_department"]:
        add_stat_finding(D, T, c, df)

    for c in ["repair_rate_cell_line", "repair_rate_department"]:
        n = safe_count(df, F.col(c) > 50)
        if n > 0:
            add_finding(D, T, "BUSINESS_RULE", "CRITICAL",
                what=f"{n} records show repair rate above 50% in `{c}`.",
                so_what="More than half of production going to repair is a quality crisis. Each repair cycle adds labour cost, delays delivery, and increases metal loss from re-handling. This is the single most impactful area to fix.",
                do_what="Trigger an immediate quality review for these cells/departments. Pull the `gold_repair_by_type` data to identify the top defect types, and cross-reference with `gold_repair_by_item` to find if specific item designs are the root cause.",
                metric_name=c, outlier_count=n, total_count=total,
                samples=top_samples(df, F.col(c) > 50, ["cell_line", "department", c, "repair_reasons", "period"]),
            )

    n = safe_count(df, F.col("total_repair_qty") > F.col("total_prod_qty"))
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "HIGH",
            what=f"{n} records have more repairs than production — pieces repaired multiple times.",
            so_what="Multi-repair pieces are a cost multiplier: each pass through repair adds labor, occupies a workstation, and delays other orders. They also indicate the initial repair didn't solve the root defect.",
            do_what="Identify which pieces required multiple repairs using the production detail table. Consider a 'scrap threshold' — if a piece needs 3+ repairs, it may be cheaper to scrap and restart.",
            metric_name="total_repair_qty", outlier_count=n, total_count=total,
        )


def scan_repair_by_type():
    T = "gold_repair_by_type"
    df = tbl(T)
    if df.count() == 0:
        return

    D = "QUALITY"
    add_stat_finding(D, T, "defect_count", df)

    # Find dominant defect types
    total_defects = df.agg(F.sum("defect_count")).first()[0] or 0
    if total_defects > 0:
        top_defect = df.groupBy("defect_type").agg(
            F.sum("defect_count").alias("cnt")
        ).orderBy(F.desc("cnt")).first()
        if top_defect and (top_defect["cnt"] / total_defects) > 0.3:
            add_finding(D, T, "BUSINESS_RULE", "HIGH",
                what=f"The defect type '{top_defect['defect_type']}' accounts for {pct(top_defect['cnt'], total_defects)}% of all repairs ({top_defect['cnt']} of {total_defects}).",
                so_what="A single defect type dominating repairs means there's a systemic root cause. Fixing this one issue could reduce repair volume by a third or more.",
                do_what=f"Run a dedicated root cause analysis on '{top_defect['defect_type']}' defects. Check: which operations produce them? Which items? Which employees? Use the trend data to see if it's getting better or worse.",
                metric_name="defect_type", outlier_count=int(top_defect["cnt"]), total_count=int(total_defects),
            )


def scan_repair_by_item():
    T = "gold_repair_by_item"
    df = tbl(T)
    if df.count() == 0:
        return

    D = "QUALITY"
    add_stat_finding(D, T, "fg_group_count", df)

    # Item groups with disproportionate repairs
    total_repairs = df.agg(F.sum("fg_group_count")).first()[0] or 0
    if total_repairs > 0:
        top_item = df.groupBy("FG_group").agg(
            F.sum("fg_group_count").alias("cnt")
        ).orderBy(F.desc("cnt")).first()
        if top_item and (top_item["cnt"] / total_repairs) > 0.25:
            add_finding(D, T, "BUSINESS_RULE", "HIGH",
                what=f"Item group '{top_item['FG_group']}' generates {pct(top_item['cnt'], total_repairs)}% of all repairs.",
                so_what="This item design or family has an inherent quality issue. Every unit produced has a high chance of needing repair, making it unprofitable at current quality levels.",
                do_what=f"Review the BOM and routing for '{top_item['FG_group']}'. Check if the design has tight tolerances that are hard to achieve in production. Consider a design revision with the brand customer.",
                metric_name="FG_group", outlier_count=int(top_item["cnt"]), total_count=int(total_repairs),
            )


def scan_repair_trends():
    for T in ["gold_repair_trend_by_defect_type", "gold_repair_trend_by_item"]:
        df = tbl(T)
        if df.count() == 0:
            continue

        D = "QUALITY"
        add_stat_finding(D, T, "repair_count", df)


# ============================================================
# DOMAIN 5: PLANNING & DELIVERY
# ============================================================

def scan_production_detail():
    T = "gold_delta_production_detail"
    df = tbl(T)
    total = df.count()
    if total == 0:
        return

    D = "PLANNING"
    for c in ["Total_QTY", "OutstandingQty", "prod_order_quantity",
              "prod_line_remaining_quantity", "efficiency_pct", "progress_pct"]:
        add_stat_finding(D, T, c, df)

    # Progress out of range
    n = safe_count(df, (F.col("progress_pct") > 100) | (F.col("progress_pct") < 0))
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "HIGH",
            what=f"{n} production orders show progress outside 0–100%.",
            so_what="Progress > 100% means more pieces were completed than ordered (over-production) or the calculation is wrong. Progress < 0% is a data error. Both mislead the planning team.",
            do_what="Cap progress at 0–100% in the display layer. Investigate over-production cases — they tie up capacity and materials for unauthorized work.",
            metric_name="progress_pct", outlier_count=n, total_count=total,
        )

    # Extreme efficiency
    n = safe_count(df, F.col("efficiency_pct") > 500)
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "HIGH",
            what=f"{n} production orders show efficiency above 500%.",
            so_what="5× the standard speed is not realistic. These are data errors that inflate overall efficiency metrics.",
            do_what="Check if these orders have very short routing standards. The plan time may not have been updated when the process was improved.",
            metric_name="efficiency_pct", outlier_count=n, total_count=total,
        )

    # Due before start
    n = safe_count(df, F.col("prod_order_due_date") < F.to_date(F.col("prod_order_starting_date_time")))
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "MEDIUM",
            what=f"{n} production orders have a due date before their start date.",
            so_what="These orders were impossible to complete on time from the moment they were created. This indicates planning issues — either manual date entry errors in BC or unrealistic customer commit dates.",
            do_what="Add a BC validation (AL extension) that prevents due_date < start_date on production order creation. For existing records, review with the planning team.",
            metric_name="prod_order_due_date", outlier_count=n, total_count=total,
        )

    # Negative remaining quantity
    n = safe_count(df, F.col("prod_line_remaining_quantity") < 0)
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "MEDIUM",
            what=f"{n} production lines have negative remaining quantity — over-produced.",
            so_what="Over-production wastes materials (especially precious metals) and ties up capacity. In make-to-order manufacturing, producing more than ordered has no value.",
            do_what="Flag over-produced orders to production managers. Check if the customer accepted the extras or if the metal needs to be reclaimed.",
            metric_name="prod_line_remaining_quantity", outlier_count=n, total_count=total,
        )

    # Scrap loss > 20% on orders
    n = safe_count(df, F.col("loss_scrap_pct") > 20)
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "HIGH",
            what=f"{n} production orders have scrap loss exceeding 20%.",
            so_what="Order-level scrap above 20% significantly impacts the gross margin on that order. The customer price was likely quoted assuming standard loss rates.",
            do_what="Cross-reference with the customer and item to find patterns. Some brands or item types may consistently have high loss — adjust future quoting accordingly.",
            metric_name="loss_scrap_pct", outlier_count=n, total_count=total,
        )

    # Null checks
    for c in ["prod_order_no", "SalesorderNo", "ItemFG", "prod_line"]:
        n = safe_count(df, F.col(c).isNull())
        if n > 0:
            add_finding(D, T, "DATA_QUALITY", "HIGH" if c == "prod_order_no" else "MEDIUM",
                what=f"{n} production detail records have NULL `{c}`.",
                so_what=f"NULL `{c}` breaks drill-down capability in reports.",
                do_what=f"Add NOT NULL validation in the pipeline for `{c}`.",
                metric_name=c, outlier_count=n, total_count=total,
            )


def scan_due_in():
    T = "gold_delta_due_in"
    df = tbl(T)
    if df.count() == 0:
        return

    D = "PLANNING"
    for c in ["due_in_count", "total_remaining_qty"]:
        add_stat_finding(D, T, c, df)

    n = safe_count(df, F.col("total_remaining_qty") < 0)
    if n > 0:
        add_finding(D, T, "BUSINESS_RULE", "MEDIUM",
            what=f"{n} due-in records show negative remaining quantity.",
            so_what="Negative remaining means more was delivered than ordered. Over-delivery in precious metals manufacturing is expensive.",
            do_what="Review with planning: were these approved over-deliveries or posting errors?",
            metric_name="total_remaining_qty", outlier_count=n, total_count=df.count(),
        )


def scan_due_status():
    T = "gold_delta_due_status"
    df = tbl(T)
    if df.count() == 0:
        return

    D = "PLANNING"
    total_orders = df.agg(F.sum("due_status_count")).first()[0] or 0
    overdue = df.filter(F.col("due_status").ilike("%overdue%")).agg(F.sum("due_status_count")).first()[0] or 0
    if total_orders > 0 and overdue > 0:
        add_finding(D, T, "BUSINESS_RULE",
            "CRITICAL" if pct(overdue, total_orders) > 30 else "HIGH" if pct(overdue, total_orders) > 15 else "MEDIUM",
            what=f"{overdue} of {total_orders} orders ({pct(overdue, total_orders)}%) are overdue.",
            so_what="Overdue orders mean customer commitments are being missed. In make-to-order manufacturing, delivery reliability directly affects brand relationships and future order volume.",
            do_what="Prioritize overdue orders in the daily planning meeting. For chronic late delivery (> 20%), analyze the bottleneck operations using the production detail runtime data to find where orders get stuck.",
            metric_name="due_status", outlier_count=int(overdue), total_count=int(total_orders),
        )


# ============================================================
# DOMAIN 6: CROSS-DOMAIN INSIGHTS
# ============================================================

def scan_cross_domain():
    """
    Cross-domain checks that connect findings across tables.
    These produce the most actionable coaching insights.
    """

    # --- INSIGHT 1: Low attendance → Low efficiency correlation ---
    try:
        att = tbl("gold_delta_attendance").filter(F.col("attendance_rate") < 70) \
            .select("work_day", "department").distinct()
        eff = tbl("gold_delta_efficiency_rate") \
            .withColumnRenamed("created_date", "work_day") \
            .select("work_day", "department", "efficiency_pct")

        joined = att.join(eff, ["work_day", "department"], "inner")
        n = joined.count()
        if n > 0:
            avg_eff = joined.agg(F.avg("efficiency_pct")).first()[0]
            if avg_eff is not None and avg_eff < 60:
                add_finding("CROSS_DOMAIN", "attendance + efficiency_rate", "CROSS_DOMAIN", "HIGH",
                    what=f"On {n} department/day combos where attendance dropped below 70%, average efficiency was only {avg_eff:.1f}%.",
                    so_what="Low attendance doesn't just mean fewer hands — it disproportionately impacts efficiency. Remaining workers handle unfamiliar positions, work without their usual partners, and rush to cover gaps. The efficiency drop is often larger than the headcount drop.",
                    do_what="Build a real-time 'understaffed alert': when morning attendance scan shows < 70% for any department, automatically notify the production planner to re-sequence orders (prioritize simpler items that fewer people can handle). Consider a cross-training matrix so workers can flex between cells.",
                    metric_name="attendance_rate → efficiency_pct",
                    outlier_count=n, total_count=0,
                )
    except Exception:
        pass

    # --- INSIGHT 2: High OT departments → High repair rate ---
    try:
        crew = tbl("gold_delta_prod_line_crew")
        high_ot = crew.filter((F.col("total") > 0) & (F.col("ot_hours") > F.col("total") * 3)) \
            .select("department").distinct()

        repair = tbl("gold_delta_repair_rate").filter(F.col("period") == "this_week")
        joined = high_ot.join(repair, "department", "inner")
        n = joined.count()
        if n > 0:
            avg_repair = joined.agg(F.avg("repair_rate_department")).first()[0]
            if avg_repair is not None and avg_repair > 15:
                add_finding("CROSS_DOMAIN", "prod_line_crew + repair_rate", "CROSS_DOMAIN", "HIGH",
                    what=f"{n} departments with high OT (> 3hrs/person) this week also have above-average repair rates ({avg_repair:.1f}%).",
                    so_what="Fatigue from excessive overtime directly causes more defects. Workers are less precise with filing, soldering, and polishing when tired. The 'savings' from OT are eaten by repair costs.",
                    do_what="For departments showing this pattern, reduce OT and add a second shift instead. Track the repair rate before and after the change to quantify the impact.",
                    metric_name="ot_hours → repair_rate",
                    outlier_count=n, total_count=0,
                )
    except Exception:
        pass

    # --- INSIGHT 3: Metal loss + Employee efficiency ---
    try:
        ml = tbl("gold_delta_metal_loss_by_employee") \
            .filter(F.col("actual_total_loss_pct") > 10) \
            .select("employee_code", "actual_total_loss_pct")

        eff = tbl("gold_delta_employee_efficiency_rate") \
            .select(F.col("employee_code"), F.col("efficiency_pct"))

        joined = ml.join(eff, "employee_code", "inner")
        n = joined.count()
        if n > 0:
            # Find employees who are fast BUT lossy
            fast_lossy = joined.filter((F.col("efficiency_pct") > 100) & (F.col("actual_total_loss_pct") > 10))
            fn = fast_lossy.count()
            if fn > 0:
                add_finding("CROSS_DOMAIN", "metal_loss_by_employee + employee_efficiency_rate", "CROSS_DOMAIN", "HIGH",
                    what=f"{fn} employees are fast (efficiency > 100%) but have high metal loss (> 10%).",
                    so_what="Speed at the cost of waste. These employees look good on the efficiency leaderboard but are costing the company in precious metal loss. The net value of their output may be lower than a slower, more careful worker.",
                    do_what="Redesign the scoring formula: efficiency should be penalized by metal loss. Proposed: `net_score = efficiency_pct × (1 - excess_loss_pct)`. Share individual metal loss data with these employees — most don't realize their loss rate is high.",
                    metric_name="efficiency_pct + actual_total_loss_pct",
                    outlier_count=fn, total_count=n,
                    samples=top_samples(fast_lossy, F.lit(True), ["employee_code", "efficiency_pct", "actual_total_loss_pct"]),
                )
    except Exception:
        pass


# ============================================================
# RUN ALL SCANS
# ============================================================

print(f"🏭 Ennovie Company Health Scanner — {RUN_TS}")
print("=" * 60)

scanners = [
    # Domain 1: Efficiency & Production
    ("Actual Time by Employees",    scan_actual_time_by_employees),
    ("Employee Efficiency",         scan_employee_efficiency),
    ("Employee Efficiency Rate",    scan_efficiency_rate),  # reusing function
    ("Efficiency Rate",             scan_efficiency_rate),
    ("Work Cell Performance",       scan_work_cell_performance),
    ("Line Utilization",            scan_line_utilization),
    ("Output Quantity",             scan_output_qty),
    # Domain 2: Metal Loss
    ("Metal Loss Summary",          scan_metal_loss),
    ("Metal Loss by Employee",      scan_metal_loss_by_employee),
    ("Metal Loss by Line/Employee", scan_metal_loss_by_line_employee),
    ("Metal Loss by Cell Trend",    scan_metal_loss_by_cell_trend),
    ("Metal Loss Card",             scan_metal_loss_card),
    # Domain 3: Attendance
    ("Attendance",                  scan_attendance),
    ("Late/Absent Trend",           scan_late_absent_trend),
    ("Prod Line Crew",              scan_prod_line_crew),
    ("Absent",                      scan_absent),
    ("OT",                          scan_ot),
    # Domain 4: Quality
    ("Repair Rate",                 scan_repair_rate),
    ("Repair by Type",              scan_repair_by_type),
    ("Repair by Item",              scan_repair_by_item),
    ("Repair Trends",               scan_repair_trends),
    # Domain 5: Planning
    ("Production Detail",           scan_production_detail),
    ("Due In",                      scan_due_in),
    ("Due Status",                  scan_due_status),
    # Domain 6: Cross-Domain
    ("Cross-Domain Insights",       scan_cross_domain),
]

for label, func in scanners:
    try:
        print(f"  Scanning: {label}...")
        func()
        print(f"  ✅ {label} complete")
    except Exception as e:
        print(f"  ❌ {label} failed: {str(e)[:100]}")
        add_finding("ERROR", label, "ERROR", "LOW",
            what=f"Scanner failed: {str(e)[:300]}",
            so_what="This table could not be scanned — findings may be incomplete.",
            do_what="Check if the table exists and is populated. Review the error message.",
        )

print(f"\n{'=' * 60}")
print(f"📊 Total findings: {len(findings)}")

# ============================================================
# WRITE RESULTS
# ============================================================

result_schema = StructType([
    StructField("run_date",       StringType()),
    StructField("run_ts",         StringType()),
    StructField("domain",         StringType()),
    StructField("table_name",     StringType()),
    StructField("finding_type",   StringType()),
    StructField("severity",       StringType()),
    StructField("what",           StringType()),
    StructField("so_what",        StringType()),
    StructField("do_what",        StringType()),
    StructField("metric_name",    StringType()),
    StructField("outlier_count",  IntegerType()),
    StructField("total_count",    IntegerType()),
    StructField("pct_affected",   DoubleType()),
    StructField("samples",        StringType()),
])

if findings:
    from pyspark.sql import Row
    rows = [Row(**f) for f in findings]
    result_df = spark.createDataFrame(rows, schema=result_schema)
    
    # Write to lakehouse (append mode — keep history)
    result_df.write.mode("append").saveAsTable(
        f"{LAKEHOUSE}.{SCHEMA}.ml_company_health_scan"
    )
    
    print(f"\n✅ Written {len(findings)} findings to {LAKEHOUSE}.{SCHEMA}.ml_company_health_scan")
    
    # --- Print summary by severity ---
    print("\n📋 SEVERITY SUMMARY:")
    for sev in ["CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO"]:
        cnt = sum(1 for f in findings if f["severity"] == sev)
        if cnt > 0:
            emoji = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢", "INFO": "ℹ️"}.get(sev, "")
            print(f"  {emoji} {sev}: {cnt} findings")

    print("\n📋 DOMAIN SUMMARY:")
    for dom in ["EFFICIENCY", "METAL_LOSS", "ATTENDANCE", "QUALITY", "PLANNING", "CROSS_DOMAIN"]:
        cnt = sum(1 for f in findings if f["domain"] == dom)
        if cnt > 0:
            print(f"  {dom}: {cnt} findings")

else:
    print("\n✅ No outliers detected — all clear!")

print(f"\n🏁 Scan complete at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Gold_Production_Lakehouse.delta.ml_company_health_scan LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
