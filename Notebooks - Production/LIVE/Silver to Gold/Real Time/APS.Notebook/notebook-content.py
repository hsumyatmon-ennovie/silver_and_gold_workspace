# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9c785c00-bff3-4379-a2f9-c17fd9df2974",
# META       "default_lakehouse_name": "Gold_Auto_Planning_Lakehouse",
# META       "default_lakehouse_workspace_id": "7194ead2-ba84-4757-8651-fd656958487b",
# META       "known_lakehouses": [
# META         {
# META           "id": "9c785c00-bff3-4379-a2f9-c17fd9df2974"
# META         },
# META         {
# META           "id": "1d620310-5acc-4534-93f9-f52f082a1887"
# META         },
# META         {
# META           "id": "6fa25cdd-36f9-4f2e-9817-c1f4d946d4d9"
# META         },
# META         {
# META           "id": "ad99fdfa-85b1-4480-9f7f-2640bfd65f24"
# META         },
# META         {
# META           "id": "e248ea90-8431-4df2-9f29-87866bf9dd5a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Notebook: nb_gold_planning_pro_baseline
# Purpose: Capacity-leveled BACKWARD distribution of sibling PROs into a FROZEN planned_finish_date,
#          snapshotted to gold_plan_pro_baseline. This is the stable "plan" the Production Remaining
#          page buckets on — it does NOT move when the forward engine re-runs.
# Layer:   Staging/Silver -> Gold
# Schedule: on-demand (run at PLAN-PUBLISH; do NOT run on the forward engine's continuous cadence)
# Dependencies: staging.planning_forward_schedule, silver_holiday_calendar,
#               (optional) gold_scheduling_cell_capacity
#
# WHY THIS EXISTS
#   scheduled_end_date in planning_forward_schedule is the forward/left-shifted date -> it clusters
#   siblings and overruns due dates, and it MOVES every engine run. No existing column distributes
#   sibling PROs across capacity-leveled dates <= customer due. This notebook builds that distribution
#   ONCE and freezes it. See the FREEZE section for the no-move guarantee.

# ============================================================
# CONFIGURATION  +  PARAMETERS
# ============================================================
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (StructType, StructField, StringType, DateType,
                               IntegerType, DoubleType, BooleanType, TimestampType)
from delta.tables import DeltaTable
from datetime import datetime, date, timedelta
from collections import defaultdict
import logging

spark = SparkSession.builder.getOrCreate()
logger = logging.getLogger(__name__)

# ---- Lakehouse / schema locations (set to your environment) ----
# ---- Table names ----
# LESSON FROM THE RUN: in this Fabric catalog, a 2-part name `X.table` resolves X as a SCHEMA in
# the lakehouse the notebook is ATTACHED to (that's why `staging.planning_forward_schedule` works).
# A LAKEHOUSE name as the qualifier does NOT cross lakehouses. So:
#   - source is referenced by its schema (`staging`)
#   - outputs are UNQUALIFIED -> they land in the attached lakehouse's default schema.
#     Attach this notebook to the lakehouse where you want the baseline (e.g. Gold_Auto_Planning).
#     If you use schema-per-layer, qualify instead, e.g. TARGET_TABLE = "gold.gold_plan_pro_baseline".
#   - the holiday calendar lives in another lakehouse (Silver_Commons); HOLIDAY_CANDIDATES tries
#     a 3-part then 2-part reference, else weekends-only (non-fatal).
SRC_FORWARD    = "staging.planning_forward_schedule"   # confirmed reachable
# Holiday calendar lives in Silver_Commons_Lakehouse (workspace ENG-Silver-and-Gold), schema `cmn`.
# Cross-workspace/lakehouse refs here need the 4-part form `workspace`.lakehouse.schema.table.
# Candidates are tried in order; date column is auto-detected.
HOLIDAY_CANDIDATES = [
    "`ENG-Silver-and-Gold`.Silver_Commons_Lakehouse.cmn.silver_holiday_calendar",  # 4-part (CONFIRMED working)
    "Silver_Commons_Lakehouse.cmn.silver_holiday_calendar",   # 3-part fallback (if attached as 3-part)
    "cmn.silver_holiday_calendar",                            # 2-part fallback
    # ABFS path also works without the catalog, e.g.:
    # "abfss://ENG-Silver-and-Gold@onelake.dfs.fabric.microsoft.com/Silver_Commons_Lakehouse.Lakehouse/Tables/cmn/silver_holiday_calendar",
]
CAP_TABLE      = "gold_scheduling_cell_capacity"       # unqualified -> attached lakehouse
TARGET_TABLE   = "gold_plan_pro_baseline"              # unqualified -> attached lakehouse
QUALITY_TABLE  = "meta_plan_quality_log"               # best-effort logging (never fatal)
ERROR_TABLE    = "meta_plan_error_log"                 # best-effort logging (never fatal)
NB_NAME        = "nb_gold_planning_pro_baseline"

def _safe_append(df, table):
    """Append for auxiliary logs only. Logging must never crash the actual baseline build."""
    try:
        df.write.format("delta").mode("append").saveAsTable(table)
    except Exception as e:
        logger.warning(f"meta log write skipped ({table}): {e}")

# ---- Planning behaviour ----
FORCE_REPLAN   = True   # False = FREEZE (plan only NEW PROs, never move existing). True = recompute all.
PRIMARY_LINE   = 10000   # BC primary FG line
OVERFLOW_MODE  = "after_due"   # "after_due" = late PROs land on first free day AFTER due (flagged).
                               # "stack_on_due" = pile late PROs on the due date itself (flagged).

# ---- Capacity model  (THIS is the v1 placeholder — wire to gold_scheduling_cell_capacity) ----
CAPACITY_BASIS          = "pcs"   # "pcs" -> load = planned_qty ; "minutes" -> load = run_time_min
DEFAULT_CELL_CAP_PER_DAY = 500     # fallback capacity per cell per working day (in CAPACITY_BASIS units)
USE_CAPACITY_TABLE      = False   # set True once the column mapping below is confirmed

TODAY = date.today()   # Bangkok server date; swap for an explicit param if you run in UTC


# ============================================================
# LOAD
# ============================================================
def load_pro_demand():
    """One row per PRO (primary line). Carries demand identity, home cell, due, and capacity load.

    assigned_home_cell uses the SAME 4-tier COALESCE the portal uses, so baseline keys line up
    exactly with what Production Remaining displays.
    """
    df = spark.sql(f"""
        SELECT
            prod_order_no,
            prod_order_line_no,
            MAX(NULLIF(TRIM(sales_order_no), ''))                       AS sales_order_no,
            MAX(NULLIF(TRIM(item_no), ''))                              AS item_no,
            MAX(NULLIF(TRIM(customer_abbr), ''))                        AS customer_abbr,
            MAX(NULLIF(TRIM(customer_name), ''))                        AS customer_name,
            -- prod line (PROD LINE preference over room aliases)
            COALESCE(
                MAX(CASE WHEN UPPER(CAST(assigned_prod_line AS string)) LIKE 'PROD%LINE%'
                         THEN assigned_prod_line END),
                MAX(assigned_prod_line)
            )                                                           AS assigned_prod_line,
            -- home cell (4-tier, matches portal)
            COALESCE(
                MAX(CASE WHEN UPPER(CAST(assigned_home_cell AS string)) LIKE 'CELL%'
                         THEN assigned_home_cell END),
                MAX(assigned_home_cell),
                MAX(CASE WHEN UPPER(CAST(assigned_worker AS string)) LIKE 'CELL[0-9]%'
                          AND UPPER(CAST(assigned_cell_line AS string)) LIKE 'CELL%'
                         THEN assigned_cell_line END),
                MAX(CASE WHEN UPPER(CAST(assigned_cell_line AS string)) LIKE 'CELL%'
                         THEN assigned_cell_line END),
                MAX(assigned_cell_line)
            )                                                           AS assigned_home_cell,
            MAX(CAST(customer_due_date AS date))                        AS customer_due_date,
            MAX(CAST(scheduled_end_date AS date))                       AS sched_end_date,
            -- capacity load candidates
            MAX(CAST(prod_order_quantity AS double))                    AS planned_qty,
            MAX(CAST(prod_line_remaining_qty AS double))                AS remaining_qty,
            -- run_time_min is NOT a column here; derive an approx PRO workload from BC routing.
            -- total minutes ~= setup(all ops) + qty * run_per_piece(all ops). VERIFY vs your
            -- canonical total-run-time field before trusting the "minutes" capacity basis.
            (COALESCE(SUM(CAST(bc_setup_time AS double)), 0)
             + COALESCE(MAX(CAST(prod_order_quantity AS double)), 0)
               * COALESCE(SUM(CAST(bc_run_time_per_piece AS double)), 0)) AS run_time_min
        FROM {SRC_FORWARD}
        WHERE COALESCE(prod_line_remaining_qty, 0) > 0
          AND TRY_CAST(prod_order_line_no AS int) = {PRIMARY_LINE}
          AND UPPER(TRIM(CAST(assigned_prod_line AS string))) NOT IN ('PACKING ROOM', 'QA ROOM')
        GROUP BY prod_order_no, prod_order_line_no
    """)
    return df


def _read_any(ref):
    """Read a Delta source by catalog name OR OneLake ABFS path. spark.sql (not spark.table) is used
    for catalog names so multipart / backtick-quoted refs like `ws`.lh.schema.table resolve reliably."""
    if ref.startswith("abfss://"):
        return spark.read.format("delta").load(ref)
    return spark.sql(f"SELECT * FROM {ref}")

def load_holidays():
    """DISTINCT holiday dates -> Python set. Tries each candidate (3-part name, 2-part name, or
    ABFS path) and auto-detects the date column, so it survives schema/column-name/attachment
    differences. Table has duplicate rows per date, so DISTINCT. Non-fatal: weekends-only on failure.
    """
    for ref in HOLIDAY_CANDIDATES:
        try:
            df = _read_any(ref)
            cols = df.dtypes                         # [(name, type), ...]
            # prefer a date-typed column whose name has 'date'; else any date/timestamp column;
            # else any column whose name has 'date' (cast it). 'date' (not 'holiday') avoids
            # false positives like ID_Holiday / Holiday_Note; it uniquely hits Holiday_Date.
            date_col = next((n for n, t in cols if t == "date" and "date" in n.lower()), None) \
                    or next((n for n, t in cols if t in ("date", "timestamp")), None) \
                    or next((n for n, t in cols if "date" in n.lower()), None)
            if date_col is None:
                continue
            rows = (df.select(F.to_date(F.col(date_col)).alias("d"))
                      .where(F.to_date(F.col(date_col)).isNotNull())
                      .distinct().collect())
            logger.info(f"holidays loaded from {ref} via `{date_col}` ({len(rows)} dates)")
            return {r["d"] for r in rows if r["d"] is not None}
        except Exception:
            continue
    logger.warning("holiday calendar not found in any candidate; weekends only")
    return set()


def load_cell_capacity():
    """cell -> capacity-per-working-day, in CAPACITY_BASIS units.

    TODO (wire to real data): confirm column names + unit of gold_scheduling_cell_capacity.
    If it is in minutes/day, set CAPACITY_BASIS='minutes'. Replace the column names below.
    Until confirmed, USE_CAPACITY_TABLE stays False and every cell uses DEFAULT_CELL_CAP_PER_DAY.
    """
    if not USE_CAPACITY_TABLE:
        return {}
    rows = spark.sql(f"""
        SELECT cell_id            AS cell,        -- TODO: real column
               daily_capacity     AS cap          -- TODO: real column + confirm pcs vs minutes
        FROM {CAP_TABLE}
    """).collect()
    return {str(r["cell"]).replace(" ", "").upper(): float(r["cap"]) for r in rows}


def load_existing_baseline():
    """Already-frozen plan (for capacity seeding + to know which PROs are new)."""
    if FORCE_REPLAN or not DeltaTable.isDeltaTable(spark, TARGET_TABLE):
        return {}, set()
    rows = spark.sql(f"""
        SELECT prod_order_no, assigned_home_cell, planned_finish_date, planned_qty, run_time_min
        FROM {TARGET_TABLE}
    """).collect()
    seeded = defaultdict(lambda: defaultdict(float))   # cell -> {date: load}
    frozen_ids = set()
    for r in rows:
        frozen_ids.add(r["prod_order_no"])
        cell = norm_cell(r["assigned_home_cell"])
        load = (r["run_time_min"] if CAPACITY_BASIS == "minutes" else r["planned_qty"]) or 0.0
        if r["planned_finish_date"] is not None:
            seeded[cell][r["planned_finish_date"]] += float(load)
    return seeded, frozen_ids


# ============================================================
# CALENDAR HELPERS  (working day = Mon-Fri, not a holiday)
# ============================================================
def norm_cell(c):
    return str(c).replace(" ", "").upper() if c is not None else "(NO CELL)"

def is_workday(d, holidays):
    return d.weekday() < 5 and d not in holidays

def workday_on_or_before(d, holidays):
    g = 0
    while not is_workday(d, holidays):
        d -= timedelta(days=1); g += 1
        if g > 2000: break
    return d

def workday_on_or_after(d, holidays):
    g = 0
    while not is_workday(d, holidays):
        d += timedelta(days=1); g += 1
        if g > 2000: break
    return d

def prev_workday(d, holidays):
    d -= timedelta(days=1)
    return workday_on_or_before(d, holidays)

def next_workday(d, holidays):
    d += timedelta(days=1)
    return workday_on_or_after(d, holidays)


# ============================================================
# TRANSFORM  —  the leveling algorithm
# ============================================================
def cap_for(cell, cap_map):
    return cap_map.get(cell, DEFAULT_CELL_CAP_PER_DAY)

def level_one_cell(pros, cell, cap_map, holidays, seed_day_load):
    """Backward (JIT) fill for one cell. pros sorted by (due asc, pro_no asc).
    Returns list of result dicts. day_load is seeded with already-frozen PROs so new PROs
    do not double-book days the frozen plan already occupies.
    """
    cap = cap_for(cell, cap_map)
    day_load = defaultdict(float, seed_day_load)   # copy of frozen seed for this cell
    out = []
    for p in pros:
        load = p["load"]
        due  = p["due_for_fill"]
        placed, is_late = None, False

        # --- backward fill: latest workday <= due that still has room, clamped >= TODAY ---
        d = workday_on_or_before(due, holidays)
        while d >= TODAY:
            if day_load[d] + load <= cap:
                day_load[d] += load
                placed = d
                break
            d = prev_workday(d, holidays)

        # --- overflow: cannot fit on/ before due (cell full back to today, or due < today) ---
        if placed is None:
            if OVERFLOW_MODE == "stack_on_due":
                placed = workday_on_or_before(max(due, TODAY), holidays)
            else:  # "after_due": first free workday after due (and >= today)
                d = workday_on_or_after(max(next_workday(due, holidays), TODAY), holidays)
                while day_load[d] + load > cap:
                    d = next_workday(d, holidays)
                placed = d
            day_load[placed] += load

        if p["has_due"] and placed > p["customer_due_date"]:
            is_late = True

        out.append({
            **p["carry"],
            "planned_finish_date": placed,
            "is_late": is_late,
            "days_late": (placed - p["customer_due_date"]).days if (is_late and p["has_due"]) else 0,
        })
    return out


def run_leveling(pdf, holidays, cap_map, seeded):
    """pdf: pandas DataFrame of PROs-to-plan. Groups by cell, levels each, returns list of dicts."""
    FAR_FUTURE = TODAY + timedelta(days=365 * 3)
    # bucket PROs by cell
    by_cell = defaultdict(list)
    for row in pdf.itertuples(index=False):
        cell = norm_cell(row.assigned_home_cell)
        load = (row.run_time_min if CAPACITY_BASIS == "minutes" else row.planned_qty)
        load = float(load) if load and load > 0 else 1.0
        has_due = row.customer_due_date is not None
        due_for_fill = row.customer_due_date or row.sched_end_date or FAR_FUTURE
        by_cell[cell].append({
            "load": load,
            "has_due": has_due,
            "customer_due_date": row.customer_due_date,
            "due_for_fill": due_for_fill,
            "carry": {
                "prod_order_no": row.prod_order_no,
                "prod_order_line_no": int(row.prod_order_line_no) if row.prod_order_line_no else PRIMARY_LINE,
                "sales_order_no": row.sales_order_no,
                "item_no": row.item_no,
                "customer_abbr": row.customer_abbr,
                "customer_name": row.customer_name,
                "assigned_prod_line": row.assigned_prod_line,
                "assigned_home_cell": row.assigned_home_cell,
                "customer_due_date": row.customer_due_date,
                "planned_qty": float(row.planned_qty) if row.planned_qty else None,
                "run_time_min": float(row.run_time_min) if row.run_time_min else None,
            },
        })

    results = []
    for cell, pros in by_cell.items():
        # most-urgent (earliest due) placed first -> it claims the scarce late slots near its due
        pros.sort(key=lambda x: (x["due_for_fill"], x["carry"]["prod_order_no"]))
        results.extend(level_one_cell(pros, cell, cap_map, holidays, seeded.get(cell, {})))
    return results


# ============================================================
# WRITE  +  FREEZE GUARANTEE
# ============================================================
RESULT_SCHEMA = StructType([
    StructField("prod_order_no",       StringType()),
    StructField("prod_order_line_no",  IntegerType()),
    StructField("sales_order_no",      StringType()),
    StructField("item_no",             StringType()),
    StructField("customer_abbr",       StringType()),
    StructField("customer_name",       StringType()),
    StructField("assigned_prod_line",  StringType()),
    StructField("assigned_home_cell",  StringType()),
    StructField("customer_due_date",   DateType()),
    StructField("planned_qty",         DoubleType()),
    StructField("run_time_min",        DoubleType()),
    StructField("planned_finish_date", DateType()),
    StructField("is_late",             BooleanType()),
    StructField("days_late",           IntegerType()),
    StructField("capacity_basis",      StringType()),
    StructField("plan_run_ts",         TimestampType()),
])

def to_spark(results):
    now = datetime.utcnow()
    rows = [(
        r["prod_order_no"], r["prod_order_line_no"], r["sales_order_no"], r["item_no"],
        r["customer_abbr"], r["customer_name"], r["assigned_prod_line"], r["assigned_home_cell"],
        r["customer_due_date"], r["planned_qty"], r["run_time_min"],
        r["planned_finish_date"], r["is_late"], r["days_late"], CAPACITY_BASIS, now,
    ) for r in results]
    return spark.createDataFrame(rows, schema=RESULT_SCHEMA)

def write_baseline(new_df):
    """FREEZE: when the table exists and FORCE_REPLAN is False, INSERT new PROs only and NEVER
    update an existing row. That is the no-move guarantee: once a PRO has a planned_finish_date,
    re-runs leave it exactly where it is. FORCE_REPLAN overwrites everything for an explicit re-plan.
    """
    exists = DeltaTable.isDeltaTable(spark, TARGET_TABLE)
    if exists and not FORCE_REPLAN:
        dt = DeltaTable.forName(spark, TARGET_TABLE)
        (dt.alias("t")
           .merge(new_df.alias("s"), "t.prod_order_no = s.prod_order_no")
           .whenNotMatchedInsertAll()      # insert NEW PROs; matched rows are deliberately left frozen
           .execute())
    else:
        (new_df.write.format("delta").mode("overwrite")
               .option("overwriteSchema", "true")
               .saveAsTable(TARGET_TABLE))


# ============================================================
# QUALITY CHECKS  +  LOGGING
# ============================================================
def quality_and_log(new_df, planned_count):
    checks = []
    n = new_df.count()
    checks.append(("rows_planned", n, "PASS" if n >= 0 else "FAIL"))
    null_pk = new_df.filter(F.col("prod_order_no").isNull()).count()
    checks.append(("null_prod_order_no", null_pk, "PASS" if null_pk == 0 else "FAIL"))
    dup_pk = new_df.groupBy("prod_order_no").count().filter(F.col("count") > 1).count()
    checks.append(("dup_prod_order_no", dup_pk, "PASS" if dup_pk == 0 else "FAIL"))
    # feasibility signal: PROs that could not fit before their customer due date
    late = new_df.filter(F.col("is_late") == True).count()           # noqa: E712
    checks.append(("infeasible_late_pros", late, "WARN" if late > 0 else "PASS"))

    log_df = spark.createDataFrame(
        [(NB_NAME, c, str(v), s, datetime.utcnow().isoformat()) for c, v, s in checks],
        schema=StructType([
            StructField("notebook_name", StringType()), StructField("check_name", StringType()),
            StructField("check_value", StringType()),  StructField("status", StringType()),
            StructField("run_timestamp", StringType()),
        ]))
    _safe_append(log_df, QUALITY_TABLE)
    logger.info(f"planned={planned_count} late/infeasible={late}")
    if any(s == "FAIL" for _, _, s in checks):
        raise Exception(f"Quality FAIL: {[c for c in checks if c[2]=='FAIL']}")
    return late


# ============================================================
# MAIN
# ============================================================
try:
    logger.info(f"{NB_NAME} start  FORCE_REPLAN={FORCE_REPLAN}  basis={CAPACITY_BASIS}  "
                f"default_cap={DEFAULT_CELL_CAP_PER_DAY}  use_cap_table={USE_CAPACITY_TABLE}")

    demand_df = load_pro_demand()
    holidays  = load_holidays()
    cap_map   = load_cell_capacity()
    seeded, frozen_ids = load_existing_baseline()

    # plan only NEW PROs unless replanning (this is what keeps the frozen ones from moving)
    to_plan_df = demand_df if FORCE_REPLAN else demand_df.filter(~F.col("prod_order_no").isin(list(frozen_ids)) if frozen_ids else F.lit(True))
    pdf = to_plan_df.toPandas()
    logger.info(f"PROs to plan this run: {len(pdf)} (already frozen: {len(frozen_ids)})")

    if len(pdf) == 0:
        logger.info("nothing new to plan; baseline unchanged")
    else:
        results = run_leveling(pdf, holidays, cap_map, seeded)
        new_df  = to_spark(results)
        late    = quality_and_log(new_df, len(results))
        write_baseline(new_df)
        logger.info(f"{NB_NAME} done; wrote {len(results)} rows; {late} flagged infeasible (late)")

except Exception as e:
    logger.error(f"{NB_NAME} failed: {e}")
    err = spark.createDataFrame(
        [(NB_NAME, str(e), datetime.utcnow().isoformat())],
        schema=StructType([StructField("notebook_name", StringType()),
                           StructField("error_message", StringType()),
                           StructField("error_timestamp", StringType())]))
    _safe_append(err, ERROR_TABLE)
    raise


# ============================================================
# PAGE-SIDE CHANGE  —  productionForwardLoad  (Fabric SQL endpoint, T-SQL)
# ============================================================
# The page currently buckets on the LIVE forward date (it moves):
#     MAX(CONVERT(varchar(10), TRY_CONVERT(date, scheduled_end_date), 23)) AS end_date
#
# Wrap the existing aggregate as a CTE `agg`, rename its date to sched_end_date, then read the
# FROZEN date from the baseline. Keep the OUTPUT column named `end_date` so the frontend bucketing
# (day/week/month) is unchanged — only its SOURCE swaps from live -> frozen:
#
#   WITH agg AS (
#       /* ... existing productionForwardLoad SELECT, but emit sched_end_date instead of end_date ... */
#   )
#   SELECT
#       agg.*,                                  -- (remove agg's own end_date column)
#       CONVERT(varchar(10),
#               COALESCE(b.planned_finish_date, agg.sched_end_date), 23) AS end_date,
#       b.is_late, b.days_late                  -- optional: show plan-adherence on the page
#   FROM agg
#   LEFT JOIN GoldPlanning.gold_plan_pro_baseline b
#          ON b.prod_order_no = agg.prod_order_no;
#
# COALESCE fallback: a brand-new PRO not yet picked up by this notebook still shows on its live
# date until the next plan-publish run freezes it. Qty stays prod_line_remaining_qty (live) — it
# correctly drops as work completes; only the column (date) is frozen.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Notebook: nb_gold_planning_pro_baseline
# Purpose: Capacity-leveled BACKWARD distribution of sibling PROs into a FROZEN planned_finish_date,
#          snapshotted to gold_plan_pro_baseline. This is the stable "plan" the Production Remaining
#          page buckets on — it does NOT move when the forward engine re-runs.
# Layer:   Staging/Silver -> Gold
# Schedule: on-demand (run at PLAN-PUBLISH; do NOT run on the forward engine's continuous cadence)
# Dependencies: staging.planning_forward_schedule, silver_holiday_calendar,
#               (optional) gold_scheduling_cell_capacity
#
# WHY THIS EXISTS
#   scheduled_end_date in planning_forward_schedule is the forward/left-shifted date -> it clusters
#   siblings and overruns due dates, and it MOVES every engine run. No existing column distributes
#   sibling PROs across capacity-leveled dates <= customer due. This notebook builds that distribution
#   ONCE and freezes it. See the FREEZE section for the no-move guarantee.

# ============================================================
# CONFIGURATION  +  PARAMETERS
# ============================================================
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (StructType, StructField, StringType, DateType,
                               IntegerType, DoubleType, BooleanType, TimestampType)
from delta.tables import DeltaTable
from datetime import datetime, date, timedelta
from collections import defaultdict
import logging

spark = SparkSession.builder.getOrCreate()
logger = logging.getLogger(__name__)

# ---- Lakehouse / schema locations (set to your environment) ----
# ---- Table names ----
# LESSON FROM THE RUN: in this Fabric catalog, a 2-part name `X.table` resolves X as a SCHEMA in
# the lakehouse the notebook is ATTACHED to (that's why `staging.planning_forward_schedule` works).
# A LAKEHOUSE name as the qualifier does NOT cross lakehouses. So:
#   - source is referenced by its schema (`staging`)
#   - outputs are UNQUALIFIED -> they land in the attached lakehouse's default schema.
#     Attach this notebook to the lakehouse where you want the baseline (e.g. Gold_Auto_Planning).
#     If you use schema-per-layer, qualify instead, e.g. TARGET_TABLE = "gold.gold_plan_pro_baseline".
#   - the holiday calendar lives in another lakehouse (Silver_Commons); HOLIDAY_CANDIDATES tries
#     a 3-part then 2-part reference, else weekends-only (non-fatal).
SRC_FORWARD    = "staging.planning_forward_schedule"   # confirmed reachable
# Holiday calendar lives in Silver_Commons_Lakehouse (workspace ENG-Silver-and-Gold), schema `cmn`.
# Cross-workspace/lakehouse refs here need the 4-part form `workspace`.lakehouse.schema.table.
# Candidates are tried in order; date column is auto-detected.
HOLIDAY_CANDIDATES = [
    "`ENG-Silver-and-Gold`.Silver_Commons_Lakehouse.cmn.silver_holiday_calendar",  # 4-part (CONFIRMED working)
    "Silver_Commons_Lakehouse.cmn.silver_holiday_calendar",   # 3-part fallback (if attached as 3-part)
    "cmn.silver_holiday_calendar",                            # 2-part fallback
    # ABFS path also works without the catalog, e.g.:
    # "abfss://ENG-Silver-and-Gold@onelake.dfs.fabric.microsoft.com/Silver_Commons_Lakehouse.Lakehouse/Tables/cmn/silver_holiday_calendar",
]
CAP_TABLE      = "gold_scheduling_cell_capacity"       # unqualified -> attached lakehouse
TARGET_TABLE   = "gold_plan_pro_baseline"              # unqualified -> attached lakehouse
QUALITY_TABLE  = "meta_plan_quality_log"               # best-effort logging (never fatal)
ERROR_TABLE    = "meta_plan_error_log"                 # best-effort logging (never fatal)
NB_NAME        = "nb_gold_planning_pro_baseline"

def _safe_append(df, table):
    """Append for auxiliary logs only. Logging must never crash the actual baseline build."""
    try:
        df.write.format("delta").mode("append").saveAsTable(table)
    except Exception as e:
        logger.warning(f"meta log write skipped ({table}): {e}")

# ---- Planning behaviour ----
FORCE_REPLAN   = False   # False = FREEZE (plan only NEW PROs, never move existing). True = recompute all.
PRIMARY_LINE   = 10000   # BC primary FG line
OVERFLOW_MODE  = "after_due"   # "after_due" = late PROs land on first free day AFTER due (flagged).
                               # "stack_on_due" = pile late PROs on the due date itself (flagged).

# ---- Capacity model: MINUTES basis. capacity(cell) = headcount(cell) x MIN_PER_WORKER_PER_DAY ----
CAPACITY_BASIS           = "minutes"  # load = run_time_min (setup + qty*run/pc); capacity in minutes/day
MIN_PER_WORKER_PER_DAY   = 560        # effective working minutes per worker per day (1 shift)
DEFAULT_CELL_CAP_PER_DAY = 2800       # MINUTES/day fallback for a cell with NO detectable headcount (~5*560)
USE_CAPACITY_TABLE       = False      # (unused) capacity is derived from headcount, not from a table

TODAY = date.today()   # Bangkok server date; swap for an explicit param if you run in UTC


# ============================================================
# LOAD
# ============================================================
def load_pro_demand():
    """One row per PRO (primary line). Carries demand identity, home cell, due, and capacity load.

    assigned_home_cell uses the SAME 4-tier COALESCE the portal uses, so baseline keys line up
    exactly with what Production Remaining displays.
    """
    df = spark.sql(f"""
        SELECT
            prod_order_no,
            prod_order_line_no,
            MAX(NULLIF(TRIM(sales_order_no), ''))                       AS sales_order_no,
            MAX(NULLIF(TRIM(item_no), ''))                              AS item_no,
            MAX(NULLIF(TRIM(customer_abbr), ''))                        AS customer_abbr,
            MAX(NULLIF(TRIM(customer_name), ''))                        AS customer_name,
            -- prod line (PROD LINE preference over room aliases)
            COALESCE(
                MAX(CASE WHEN UPPER(CAST(assigned_prod_line AS string)) LIKE 'PROD%LINE%'
                         THEN assigned_prod_line END),
                MAX(assigned_prod_line)
            )                                                           AS assigned_prod_line,
            -- home cell (4-tier, matches portal)
            COALESCE(
                MAX(CASE WHEN UPPER(CAST(assigned_home_cell AS string)) LIKE 'CELL%'
                         THEN assigned_home_cell END),
                MAX(assigned_home_cell),
                MAX(CASE WHEN UPPER(CAST(assigned_worker AS string)) LIKE 'CELL[0-9]%'
                          AND UPPER(CAST(assigned_cell_line AS string)) LIKE 'CELL%'
                         THEN assigned_cell_line END),
                MAX(CASE WHEN UPPER(CAST(assigned_cell_line AS string)) LIKE 'CELL%'
                         THEN assigned_cell_line END),
                MAX(assigned_cell_line)
            )                                                           AS assigned_home_cell,
            MAX(CAST(customer_due_date AS date))                        AS customer_due_date,
            MAX(CAST(scheduled_end_date AS date))                       AS sched_end_date,
            -- capacity load candidates
            MAX(CAST(prod_order_quantity AS double))                    AS planned_qty,
            MAX(CAST(prod_line_remaining_qty AS double))                AS remaining_qty,
            -- run_time_min is NOT a column here; derive an approx PRO workload from BC routing.
            -- total minutes ~= setup(all ops) + qty * run_per_piece(all ops). VERIFY vs your
            -- canonical total-run-time field before trusting the "minutes" capacity basis.
            (COALESCE(SUM(CAST(bc_setup_time AS double)), 0)
             + COALESCE(MAX(CAST(prod_order_quantity AS double)), 0)
               * COALESCE(SUM(CAST(bc_run_time_per_piece AS double)), 0)) AS run_time_min
        FROM {SRC_FORWARD}
        WHERE COALESCE(prod_line_remaining_qty, 0) > 0
          AND TRY_CAST(prod_order_line_no AS int) = {PRIMARY_LINE}
          AND UPPER(TRIM(CAST(assigned_prod_line AS string))) NOT IN ('PACKING ROOM', 'QA ROOM')
        GROUP BY prod_order_no, prod_order_line_no
    """)
    return df


def _read_any(ref):
    """Read a Delta source by catalog name OR OneLake ABFS path. spark.sql (not spark.table) is used
    for catalog names so multipart / backtick-quoted refs like `ws`.lh.schema.table resolve reliably."""
    if ref.startswith("abfss://"):
        return spark.read.format("delta").load(ref)
    return spark.sql(f"SELECT * FROM {ref}")

def load_holidays():
    """DISTINCT holiday dates -> Python set. Tries each candidate (3-part name, 2-part name, or
    ABFS path) and auto-detects the date column, so it survives schema/column-name/attachment
    differences. Table has duplicate rows per date, so DISTINCT. Non-fatal: weekends-only on failure.
    """
    for ref in HOLIDAY_CANDIDATES:
        try:
            df = _read_any(ref)
            cols = df.dtypes                         # [(name, type), ...]
            # prefer a date-typed column whose name has 'date'; else any date/timestamp column;
            # else any column whose name has 'date' (cast it). 'date' (not 'holiday') avoids
            # false positives like ID_Holiday / Holiday_Note; it uniquely hits Holiday_Date.
            date_col = next((n for n, t in cols if t == "date" and "date" in n.lower()), None) \
                    or next((n for n, t in cols if t in ("date", "timestamp")), None) \
                    or next((n for n, t in cols if "date" in n.lower()), None)
            if date_col is None:
                continue
            rows = (df.select(F.to_date(F.col(date_col)).alias("d"))
                      .where(F.to_date(F.col(date_col)).isNotNull())
                      .distinct().collect())
            logger.info(f"holidays loaded from {ref} via `{date_col}` ({len(rows)} dates)")
            return {r["d"] for r in rows if r["d"] is not None}
        except Exception:
            continue
    logger.warning("holiday calendar not found in any candidate; weekends only")
    return set()


def load_cell_capacity():
    """cell -> capacity per working day (MINUTES). capacity = headcount(cell) x MIN_PER_WORKER_PER_DAY.

    headcount = DISTINCT real workers per cell from the source schedule, where a "real worker" is a
    non-blank assigned_worker that is NOT a CELL-code. The cell key uses the SAME 5-tier COALESCE as
    load_pro_demand, then norm_cell(), so caps line up exactly with the cells the leveler plans.
    Cells with no detectable worker fall back to DEFAULT_CELL_CAP_PER_DAY via cap_for().
    """
    if CAPACITY_BASIS != "minutes":
        return {}                      # pcs basis has no headcount model -> DEFAULT for every cell
    rows = spark.sql(f"""
        WITH pro AS (
          SELECT prod_order_no,
            COALESCE(
                MAX(CASE WHEN UPPER(CAST(assigned_home_cell AS string)) LIKE 'CELL%'
                         THEN assigned_home_cell END),
                MAX(assigned_home_cell),
                MAX(CASE WHEN UPPER(CAST(assigned_worker AS string)) LIKE 'CELL[0-9]%'
                          AND UPPER(CAST(assigned_cell_line AS string)) LIKE 'CELL%'
                         THEN assigned_cell_line END),
                MAX(CASE WHEN UPPER(CAST(assigned_cell_line AS string)) LIKE 'CELL%'
                         THEN assigned_cell_line END),
                MAX(assigned_cell_line)
            )                                                          AS cell,
            MAX(CASE WHEN UPPER(CAST(assigned_worker AS string)) NOT LIKE 'CELL%'
                     THEN NULLIF(TRIM(assigned_worker), '') END)       AS worker
          FROM {SRC_FORWARD}
          WHERE COALESCE(prod_line_remaining_qty, 0) > 0
            AND TRY_CAST(prod_order_line_no AS int) = {PRIMARY_LINE}
            AND UPPER(TRIM(CAST(assigned_prod_line AS string))) NOT IN ('PACKING ROOM', 'QA ROOM')
          GROUP BY prod_order_no
        )
        SELECT cell, COUNT(DISTINCT worker) AS workers
        FROM pro
        WHERE cell IS NOT NULL
        GROUP BY cell
    """).collect()
    cap = {}
    for r in rows:
        hc = int(r["workers"] or 0)
        if hc > 0:
            cap[norm_cell(r["cell"])] = hc * float(MIN_PER_WORKER_PER_DAY)
    sample = dict(sorted(cap.items())[:4])
    logger.info(f"capacity(min) for {len(cap)} cells @ {MIN_PER_WORKER_PER_DAY} min/worker; e.g. {sample}")
    return cap


def load_existing_baseline():
    """Already-frozen plan (for capacity seeding + to know which PROs are new)."""
    if FORCE_REPLAN or not DeltaTable.isDeltaTable(spark, TARGET_TABLE):
        return {}, set()
    rows = spark.sql(f"""
        SELECT prod_order_no, assigned_home_cell, planned_finish_date, planned_qty, run_time_min
        FROM {TARGET_TABLE}
    """).collect()
    seeded = defaultdict(lambda: defaultdict(float))   # cell -> {date: load}
    frozen_ids = set()
    for r in rows:
        frozen_ids.add(r["prod_order_no"])
        cell = norm_cell(r["assigned_home_cell"])
        load = (r["run_time_min"] if CAPACITY_BASIS == "minutes" else r["planned_qty"]) or 0.0
        if r["planned_finish_date"] is not None:
            seeded[cell][r["planned_finish_date"]] += float(load)
    return seeded, frozen_ids


# ============================================================
# CALENDAR HELPERS  (working day = Mon-Fri, not a holiday)
# ============================================================
def norm_cell(c):
    return str(c).replace(" ", "").upper() if c is not None else "(NO CELL)"

def is_workday(d, holidays):
    return d.weekday() < 5 and d not in holidays

def workday_on_or_before(d, holidays):
    g = 0
    while not is_workday(d, holidays):
        d -= timedelta(days=1); g += 1
        if g > 2000: break
    return d

def workday_on_or_after(d, holidays):
    g = 0
    while not is_workday(d, holidays):
        d += timedelta(days=1); g += 1
        if g > 2000: break
    return d

def prev_workday(d, holidays):
    d -= timedelta(days=1)
    return workday_on_or_before(d, holidays)

def next_workday(d, holidays):
    d += timedelta(days=1)
    return workday_on_or_after(d, holidays)


# ============================================================
# TRANSFORM  —  the leveling algorithm
# ============================================================
def cap_for(cell, cap_map):
    return cap_map.get(cell, DEFAULT_CELL_CAP_PER_DAY)

def level_one_cell(pros, cell, cap_map, holidays, seed_day_load):
    """Backward (JIT) fill for one cell. pros sorted by (due asc, pro_no asc).
    Returns list of result dicts. day_load is seeded with already-frozen PROs so new PROs
    do not double-book days the frozen plan already occupies.
    """
    cap = cap_for(cell, cap_map)
    day_load = defaultdict(float, seed_day_load)   # copy of frozen seed for this cell
    out = []
    for p in pros:
        load = p["load"]
        due  = p["due_for_fill"]
        placed, is_late = None, False

        # --- backward fill: latest workday <= due that still has room, clamped >= TODAY ---
        d = workday_on_or_before(due, holidays)
        while d >= TODAY:
            if day_load[d] + load <= cap:
                day_load[d] += load
                placed = d
                break
            d = prev_workday(d, holidays)

        # --- overflow: cannot fit on/ before due (cell full back to today, or due < today) ---
        if placed is None:
            if OVERFLOW_MODE == "stack_on_due":
                placed = workday_on_or_before(max(due, TODAY), holidays)
            else:  # "after_due": first free workday after due (and >= today)
                d = workday_on_or_after(max(next_workday(due, holidays), TODAY), holidays)
                while day_load[d] + load > cap:
                    d = next_workday(d, holidays)
                placed = d
            day_load[placed] += load

        if p["has_due"] and placed > p["customer_due_date"]:
            is_late = True

        out.append({
            **p["carry"],
            "planned_finish_date": placed,
            "is_late": is_late,
            "days_late": (placed - p["customer_due_date"]).days if (is_late and p["has_due"]) else 0,
        })
    return out


def run_leveling(pdf, holidays, cap_map, seeded):
    """pdf: pandas DataFrame of PROs-to-plan. Groups by cell, levels each, returns list of dicts."""
    FAR_FUTURE = TODAY + timedelta(days=365 * 3)
    # bucket PROs by cell
    by_cell = defaultdict(list)
    for row in pdf.itertuples(index=False):
        cell = norm_cell(row.assigned_home_cell)
        load = (row.run_time_min if CAPACITY_BASIS == "minutes" else row.planned_qty)
        load = float(load) if load and load > 0 else 1.0
        has_due = row.customer_due_date is not None
        due_for_fill = row.customer_due_date or row.sched_end_date or FAR_FUTURE
        by_cell[cell].append({
            "load": load,
            "has_due": has_due,
            "customer_due_date": row.customer_due_date,
            "due_for_fill": due_for_fill,
            "carry": {
                "prod_order_no": row.prod_order_no,
                "prod_order_line_no": int(row.prod_order_line_no) if row.prod_order_line_no else PRIMARY_LINE,
                "sales_order_no": row.sales_order_no,
                "item_no": row.item_no,
                "customer_abbr": row.customer_abbr,
                "customer_name": row.customer_name,
                "assigned_prod_line": row.assigned_prod_line,
                "assigned_home_cell": row.assigned_home_cell,
                "customer_due_date": row.customer_due_date,
                "planned_qty": float(row.planned_qty) if row.planned_qty else None,
                "run_time_min": float(row.run_time_min) if row.run_time_min else None,
            },
        })

    results = []
    for cell, pros in by_cell.items():
        # most-urgent (earliest due) placed first -> it claims the scarce late slots near its due
        pros.sort(key=lambda x: (x["due_for_fill"], x["carry"]["prod_order_no"]))
        results.extend(level_one_cell(pros, cell, cap_map, holidays, seeded.get(cell, {})))
    return results


# ============================================================
# WRITE  +  FREEZE GUARANTEE
# ============================================================
RESULT_SCHEMA = StructType([
    StructField("prod_order_no",       StringType()),
    StructField("prod_order_line_no",  IntegerType()),
    StructField("sales_order_no",      StringType()),
    StructField("item_no",             StringType()),
    StructField("customer_abbr",       StringType()),
    StructField("customer_name",       StringType()),
    StructField("assigned_prod_line",  StringType()),
    StructField("assigned_home_cell",  StringType()),
    StructField("customer_due_date",   DateType()),
    StructField("planned_qty",         DoubleType()),
    StructField("run_time_min",        DoubleType()),
    StructField("planned_finish_date", DateType()),
    StructField("is_late",             BooleanType()),
    StructField("days_late",           IntegerType()),
    StructField("capacity_basis",      StringType()),
    StructField("plan_run_ts",         TimestampType()),
])

def to_spark(results):
    now = datetime.utcnow()
    rows = [(
        r["prod_order_no"], r["prod_order_line_no"], r["sales_order_no"], r["item_no"],
        r["customer_abbr"], r["customer_name"], r["assigned_prod_line"], r["assigned_home_cell"],
        r["customer_due_date"], r["planned_qty"], r["run_time_min"],
        r["planned_finish_date"], r["is_late"], r["days_late"], CAPACITY_BASIS, now,
    ) for r in results]
    return spark.createDataFrame(rows, schema=RESULT_SCHEMA)

def write_baseline(new_df):
    """FREEZE: when the table exists and FORCE_REPLAN is False, INSERT new PROs only and NEVER
    update an existing row. That is the no-move guarantee: once a PRO has a planned_finish_date,
    re-runs leave it exactly where it is. FORCE_REPLAN overwrites everything for an explicit re-plan.
    """
    exists = DeltaTable.isDeltaTable(spark, TARGET_TABLE)
    if exists and not FORCE_REPLAN:
        dt = DeltaTable.forName(spark, TARGET_TABLE)
        (dt.alias("t")
           .merge(new_df.alias("s"), "t.prod_order_no = s.prod_order_no")
           .whenNotMatchedInsertAll()      # insert NEW PROs; matched rows are deliberately left frozen
           .execute())
    else:
        (new_df.write.format("delta").mode("overwrite")
               .option("overwriteSchema", "true")
               .saveAsTable(TARGET_TABLE))


# ============================================================
# QUALITY CHECKS  +  LOGGING
# ============================================================
def quality_and_log(new_df, planned_count):
    checks = []
    n = new_df.count()
    checks.append(("rows_planned", n, "PASS" if n >= 0 else "FAIL"))
    null_pk = new_df.filter(F.col("prod_order_no").isNull()).count()
    checks.append(("null_prod_order_no", null_pk, "PASS" if null_pk == 0 else "FAIL"))
    dup_pk = new_df.groupBy("prod_order_no").count().filter(F.col("count") > 1).count()
    checks.append(("dup_prod_order_no", dup_pk, "PASS" if dup_pk == 0 else "FAIL"))
    # feasibility signal: PROs that could not fit before their customer due date
    late = new_df.filter(F.col("is_late") == True).count()           # noqa: E712
    checks.append(("infeasible_late_pros", late, "WARN" if late > 0 else "PASS"))

    log_df = spark.createDataFrame(
        [(NB_NAME, c, str(v), s, datetime.utcnow().isoformat()) for c, v, s in checks],
        schema=StructType([
            StructField("notebook_name", StringType()), StructField("check_name", StringType()),
            StructField("check_value", StringType()),  StructField("status", StringType()),
            StructField("run_timestamp", StringType()),
        ]))
    _safe_append(log_df, QUALITY_TABLE)
    logger.info(f"planned={planned_count} late/infeasible={late}")
    if any(s == "FAIL" for _, _, s in checks):
        raise Exception(f"Quality FAIL: {[c for c in checks if c[2]=='FAIL']}")
    return late


# ============================================================
# MAIN
# ============================================================
try:
    logger.info(f"{NB_NAME} start  FORCE_REPLAN={FORCE_REPLAN}  basis={CAPACITY_BASIS}  "
                f"min_per_worker={MIN_PER_WORKER_PER_DAY}  default_cap={DEFAULT_CELL_CAP_PER_DAY}")

    demand_df = load_pro_demand()
    holidays  = load_holidays()
    cap_map   = load_cell_capacity()
    seeded, frozen_ids = load_existing_baseline()

    # plan only NEW PROs unless replanning (this is what keeps the frozen ones from moving)
    to_plan_df = demand_df if FORCE_REPLAN else demand_df.filter(~F.col("prod_order_no").isin(list(frozen_ids)) if frozen_ids else F.lit(True))
    pdf = to_plan_df.toPandas()
    logger.info(f"PROs to plan this run: {len(pdf)} (already frozen: {len(frozen_ids)})")

    if len(pdf) == 0:
        logger.info("nothing new to plan; baseline unchanged")
    else:
        results = run_leveling(pdf, holidays, cap_map, seeded)
        new_df  = to_spark(results)
        late    = quality_and_log(new_df, len(results))
        write_baseline(new_df)
        logger.info(f"{NB_NAME} done; wrote {len(results)} rows; {late} flagged infeasible (late)")

except Exception as e:
    logger.error(f"{NB_NAME} failed: {e}")
    err = spark.createDataFrame(
        [(NB_NAME, str(e), datetime.utcnow().isoformat())],
        schema=StructType([StructField("notebook_name", StringType()),
                           StructField("error_message", StringType()),
                           StructField("error_timestamp", StringType())]))
    _safe_append(err, ERROR_TABLE)
    raise


# ============================================================
# PAGE-SIDE CHANGE  —  productionForwardLoad  (Fabric SQL endpoint, T-SQL)
# ============================================================
# The page currently buckets on the LIVE forward date (it moves):
#     MAX(CONVERT(varchar(10), TRY_CONVERT(date, scheduled_end_date), 23)) AS end_date
#
# Wrap the existing aggregate as a CTE `agg`, rename its date to sched_end_date, then read the
# FROZEN date from the baseline. Keep the OUTPUT column named `end_date` so the frontend bucketing
# (day/week/month) is unchanged — only its SOURCE swaps from live -> frozen:
#
#   WITH agg AS (
#       /* ... existing productionForwardLoad SELECT, but emit sched_end_date instead of end_date ... */
#   )
#   SELECT
#       agg.*,                                  -- (remove agg's own end_date column)
#       CONVERT(varchar(10),
#               COALESCE(b.planned_finish_date, agg.sched_end_date), 23) AS end_date,
#       b.is_late, b.days_late                  -- optional: show plan-adherence on the page
#   FROM agg
#   LEFT JOIN GoldPlanning.gold_plan_pro_baseline b
#          ON b.prod_order_no = agg.prod_order_no;
#
# COALESCE fallback: a brand-new PRO not yet picked up by this notebook still shows on its live
# date until the next plan-publish run freezes it. Qty stays prod_line_remaining_qty (live) — it
# correctly drops as work completes; only the column (date) is frozen.

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
