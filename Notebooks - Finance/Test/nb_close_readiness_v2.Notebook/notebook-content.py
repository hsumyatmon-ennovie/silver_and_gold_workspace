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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================================
# nb_close_readiness_v3.py
#
# Ennovie Inventory & COGS — Close Readiness Verification Report
#
# Purpose:
#   Verify that the four key balance-sheet positions are correct as of a
#   given date:
#     - 10391  Inventory (Interim)
#     - 20380  Invt. Accrual Acc. (Interim)
#     - 10392  COGS (Interim)
#     - 10410  WIP
#
# Tests run:
#   T1 — GL vs sub-ledger reconciliation per account
#   T2 — Pair match: 10391 + 20380 should net to zero
#   T3 — Aging hygiene per account
#   WIP classification: Legitimate / Stuck / Abandoned suspect / Cleared
#   10391 precious-metals flag: RMPS/RMPG/RMPB/RMPO should be near-zero
#   IAS 8 year-of-origin scoping (NEW in v3): allocates findings to
#     pre-audit-year / audit-year / post-audit-year buckets for KPMG.
#
# Output:
#   HTML report written to the Lakehouse Files area + inline preview.
#
# Run modes:
#   - Interactive (notebook): set as_of_date = None to mean "today"
#   - Pipeline: override as_of_date and audit_year via Fabric pipeline params
#
# ---------------------------------------------------------------------------
# Cumulative patch notes (applied throughout):
#
# v1.1 — Decimal/float typing:
#   BC mirrored Amount/Cost columns and SUM(DECIMAL) come back as
#   decimal.Decimal in pandas. Mixing with float() triggers
#       TypeError: unsupported operand type(s) for -: 'float' and 'Decimal'.
#   Fix: CAST(... AS DOUBLE) in every SQL aggregate + to_float() at every
#   scalar boundary + .astype(float) on DataFrame numeric columns.
#
# v1.2 — Spark SQL ORDER BY aliasing:
#   Spark's analyzer rejects re-using an aggregate function in ORDER BY
#   once it has been aliased in SELECT. Use ORDER BY <alias> instead of
#   ORDER BY <repeated aggregate>. Applied to unmatched_q + precious_q.
#
# v1.3 — WIP sign convention:
#   BC posts Value Entries with signed cost_actual / cost_expected.
#   Consumption arrives negative (credit from raw mat → WIP); Output
#   arrives positive (debit out of WIP → FG). Correct WIP residual per
#   PRO = -(cost_actual + cost_expected) uniformly over all Production VEs.
#   No CASE on ile_entry_type. Matches the sign of net_amount in
#   gold_bc_gl_entry_clean for account 10410. Applied to sub_10410 and
#   wip_class_q.
#
# v3.0 — IAS 8 year-of-origin analysis added (new Cell 13):
#   View A: GL balance by account × posting year.
#   View B: stuck Finished PRO variance by year-of-finish.
#   View C: pair imbalance (10391 + 20380) by year.
#   Allocation summary: pre-audit-year / audit-year / post-audit-year
#   buckets with proposed IAS 8 treatment.
#   HTML report gains section 07.
# ============================================================================


# %% CELL 1 — PARAMETERS ------------------------------------------------------
# Override these via Fabric pipeline parameters or edit inline.

as_of_date              = None     # "YYYY-MM-DD"  None = today
audit_year              = 2025     # year KPMG is auditing — drives IAS 8 buckets
output_dir              = "/lakehouse/default/Files/finance_reports/close_readiness"
materiality_thb_interim = 500      # tolerance for 10391, 20380, 10392 recon
materiality_thb_wip     = 5000     # WIP cycles much more value; looser
materiality_thb_pair    = 100      # 10391 + 20380 pair should be near-exact
abandoned_pro_days      = 120      # idle PROs older than this = suspect
typical_cycle_days      = 112      # 16 weeks; expected upper bound of normal


# %% CELL 2 — IMPORTS & SESSION ----------------------------------------------
from datetime import datetime, date, timedelta
from decimal import Decimal
from pyspark.sql import SparkSession
import pandas as pd
import os
import html as html_lib

spark = SparkSession.builder.getOrCreate()

# Resolve date parameter
if as_of_date is None:
    as_of_date = date.today().isoformat()
generated_at = datetime.now().isoformat(timespec="seconds")
current_year = int(as_of_date[:4])

print(f"Close readiness check as of: {as_of_date}")
print(f"Audit year (per KPMG scope): {audit_year}")
print(f"Generated at:                {generated_at}")


# %% CELL 3 — SELF-DISCOVERY (validate gold tables + columns exist) ----------
# Defensive check — fail fast with a clear message if the lakehouse schema
# has drifted from what this notebook expects.

required = {
    "fa.gold_bc_gl_entry_clean":     ["gl_account_no", "posting_date", "net_amount", "document_no"],
    "fa.gold_bc_interim_lines_base": ["ile_entry_type", "cost_expected", "cost_actual",
                                       "is_adjustment", "posting_date", "ve_order_type",
                                       "ve_order_no", "inv_posting_group"],
    "fa.gold_pbi_wip_detail":        ["prod_order_no", "prod_status", "posting_date",
                                       "cost_actual"],
}

missing = {}
for tbl, cols in required.items():
    try:
        actual_cols = [f.name for f in spark.table(tbl).schema.fields]
        gap = [c for c in cols if c not in actual_cols]
        if gap:
            missing[tbl] = gap
    except Exception as e:
        missing[tbl] = f"TABLE NOT FOUND: {e}"

if missing:
    print("⚠ Schema discovery flagged the following gaps:")
    for tbl, gap in missing.items():
        print(f"  {tbl}: {gap}")
    print("Notebook will continue but some cells may fail.")
else:
    print("✓ All required gold tables and columns present.")


# %% CELL 4 — HELPER FUNCTIONS -----------------------------------------------

def run_sql(query):
    """Execute SQL against the Lakehouse and return a pandas DataFrame."""
    return spark.sql(query).toPandas()


def to_float(x):
    """
    Coerce any numeric scalar (Decimal, int, float, np.float, None, NaN)
    to native Python float. Returns 0.0 for None/NaN so downstream arithmetic
    never blows up. Use this at every Spark→Python scalar boundary.
    """
    if x is None:
        return 0.0
    if isinstance(x, Decimal):
        return float(x)
    try:
        if pd.isna(x):
            return 0.0
    except (TypeError, ValueError):
        pass
    return float(x)


def coerce_decimals(df, cols):
    """In-place: cast given columns to float64 if they're Decimal-typed."""
    if df is None or len(df) == 0:
        return df
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(float)
    return df


def fmt_thb(amount):
    """Format a THB amount with thousands separators."""
    if amount is None or pd.isna(amount):
        return "—"
    return f"฿{float(amount):,.2f}"


def fmt_int(n):
    if n is None or pd.isna(n):
        return "—"
    return f"{int(n):,}"


def verdict(value, threshold):
    """Return tuple (symbol, color) for a difference vs threshold."""
    if value is None or pd.isna(value):
        return ("?", "gray")
    abs_val = abs(float(value))
    if abs_val < threshold:
        return ("✓", "green")
    elif abs_val < threshold * 3:
        return ("⚠", "amber")
    else:
        return ("✗", "red")


def bucket_for(year):
    """Map a calendar year to an IAS 8 treatment bucket."""
    if year is None or pd.isna(year):
        return None
    y = int(year)
    if y < audit_year:
        return "pre_audit_year"
    elif y == audit_year:
        return "audit_year"
    else:
        return "post_audit_year"


# %% CELL 5 — ADJUST COST RECENCY (THE GATE QUESTION) ------------------------
# If this comes back stale, every other test is meaningless until the batch
# is fixed and re-run.

recency_q = f"""
SELECT
    CAST(MAX(posting_date) AS DATE)                       AS last_adjustment_date,
    DATEDIFF(day, MAX(posting_date), CAST('{as_of_date}' AS DATE)) AS days_since,
    COUNT(*)                                              AS adjustment_ves_last_30d,
    CAST(SUM(ABS(cost_actual)) AS DOUBLE)                 AS abs_value_moved_thb
FROM fa.gold_bc_interim_lines_base
WHERE is_adjustment = 1
  AND posting_date BETWEEN DATEADD(day, -30, CAST('{as_of_date}' AS DATE))
                       AND CAST('{as_of_date}' AS DATE)
"""
recency_row = run_sql(recency_q).iloc[0]
recency = {
    "last_adjustment_date":      recency_row["last_adjustment_date"],
    "days_since":                None if pd.isna(recency_row["days_since"]) else int(recency_row["days_since"]),
    "adjustment_ves_last_30d":   int(recency_row["adjustment_ves_last_30d"] or 0),
    "abs_value_moved_thb":       to_float(recency_row["abs_value_moved_thb"]),
}

if recency["days_since"] is None:
    recency_status = ("✗", "red", "No adjustment activity in last 30 days — batch may be disabled")
elif recency["days_since"] <= 1:
    recency_status = ("✓", "green", "Fresh — batch ran in last 24 hours")
elif recency["days_since"] == 2:
    recency_status = ("⚠", "amber", "Borderline — likely a weekend; verify Job Queue Log")
else:
    recency_status = ("✗", "red", f"STALE — {recency['days_since']} days since last run; fix before trusting results")

print(f"Last adjustment: {recency['last_adjustment_date']}")
print(f"Status: {recency_status[0]} {recency_status[2]}")


# %% CELL 6 — GL BALANCES FOR THE FOUR ACCOUNTS ------------------------------

gl_q = f"""
SELECT
    gl_account_no,
    COUNT(*)                            AS entry_count,
    CAST(SUM(net_amount) AS DOUBLE)     AS gl_balance,
    MIN(posting_date)                   AS oldest_entry,
    MAX(posting_date)                   AS newest_entry
FROM fa.gold_bc_gl_entry_clean
WHERE gl_account_no IN ('10391','20380','10392','10410')
  AND posting_date <= CAST('{as_of_date}' AS DATE)
GROUP BY gl_account_no
"""
gl = run_sql(gl_q)
coerce_decimals(gl, ["gl_balance"])
gl = gl.set_index("gl_account_no")

def gl_bal(acct):
    return to_float(gl.loc[acct, "gl_balance"]) if acct in gl.index else 0.0

print(gl)


# %% CELL 7 — TEST 1: GL ↔ SUB-LEDGER RECONCILIATION -------------------------
# For each account, compute the balance the sub-ledger says it SHOULD be.

# 10391 Inventory (Interim) — unsettled DR from Purchase VEs
sub_10391 = to_float(run_sql(f"""
SELECT CAST(COALESCE(SUM(cost_expected), 0) AS DOUBLE) AS bal
FROM fa.gold_bc_interim_lines_base
WHERE ile_entry_type = 'Purchase'
  AND posting_date <= CAST('{as_of_date}' AS DATE)
""").iloc[0]["bal"])

# 20380 Accrual (Interim) — mirror of 10391, opposite sign
sub_20380 = -sub_10391

# 10392 COGS (Interim) — unsettled DR from Sale VEs
sub_10392 = to_float(run_sql(f"""
SELECT CAST(COALESCE(SUM(cost_expected), 0) AS DOUBLE) AS bal
FROM fa.gold_bc_interim_lines_base
WHERE ile_entry_type = 'Sale'
  AND posting_date <= CAST('{as_of_date}' AS DATE)
""").iloc[0]["bal"])

# 10410 WIP — uniform sign convention (v1.3 patch).
sub_10410 = to_float(run_sql(f"""
SELECT CAST(-COALESCE(SUM(cost_actual + cost_expected), 0) AS DOUBLE) AS bal
FROM fa.gold_bc_interim_lines_base
WHERE ve_order_type = 'Production'
  AND posting_date <= CAST('{as_of_date}' AS DATE)
""").iloc[0]["bal"])

recon = pd.DataFrame([
    {"acct": "10391", "name": "Inventory (Interim)",  "gl": gl_bal("10391"), "expected": sub_10391, "tol": materiality_thb_interim},
    {"acct": "20380", "name": "Accrual (Interim)",    "gl": gl_bal("20380"), "expected": sub_20380, "tol": materiality_thb_interim},
    {"acct": "10392", "name": "COGS (Interim)",       "gl": gl_bal("10392"), "expected": sub_10392, "tol": materiality_thb_interim},
    {"acct": "10410", "name": "WIP",                  "gl": gl_bal("10410"), "expected": sub_10410, "tol": materiality_thb_wip},
])
recon["gl"]       = recon["gl"].astype(float)
recon["expected"] = recon["expected"].astype(float)
recon["diff"]     = recon["gl"] - recon["expected"]
recon["verdict"]  = recon.apply(lambda r: verdict(r["diff"], r["tol"]), axis=1)
recon["symbol"]   = recon["verdict"].apply(lambda v: v[0])
recon["color"]    = recon["verdict"].apply(lambda v: v[1])

print(recon[["acct", "name", "gl", "expected", "diff", "symbol"]])


# %% CELL 8 — TEST 2: PAIR MATCH (10391 ↔ 20380) -----------------------------
# Every receipt-side journal entry hits 10391 DR and 20380 CR in equal amount.
# Across all time, the two accounts must sum to zero. Drift = unmatched legs.

pair_q = f"""
SELECT
    CAST(SUM(CASE WHEN gl_account_no = '10391' THEN net_amount ELSE 0 END) AS DOUBLE) AS sum_10391,
    CAST(SUM(CASE WHEN gl_account_no = '20380' THEN net_amount ELSE 0 END) AS DOUBLE) AS sum_20380,
    CAST(SUM(net_amount) AS DOUBLE) AS pair_net
FROM fa.gold_bc_gl_entry_clean
WHERE gl_account_no IN ('10391','20380')
  AND posting_date <= CAST('{as_of_date}' AS DATE)
"""
pair_row = run_sql(pair_q).iloc[0]
pair = {
    "sum_10391": to_float(pair_row["sum_10391"]),
    "sum_20380": to_float(pair_row["sum_20380"]),
    "pair_net":  to_float(pair_row["pair_net"]),
}
pair_v = verdict(pair["pair_net"], materiality_thb_pair)

# Drill-down: which documents have unmatched legs?
# ORDER BY the alias (v1.2 patch), not the repeated aggregate.
unmatched_q = f"""
SELECT
    document_no,
    CAST(SUM(net_amount) AS DOUBLE) AS doc_net,
    COUNT(*)                        AS legs
FROM fa.gold_bc_gl_entry_clean
WHERE gl_account_no IN ('10391','20380')
  AND posting_date <= CAST('{as_of_date}' AS DATE)
GROUP BY document_no
HAVING ABS(SUM(net_amount)) > 0.01
ORDER BY ABS(doc_net) DESC
"""
unmatched = run_sql(unmatched_q)
if len(unmatched):
    unmatched["doc_net"] = unmatched["doc_net"].astype(float)
    unmatched["legs"]    = unmatched["legs"].astype(int)

print(f"Pair net: {fmt_thb(pair['pair_net'])} — {pair_v[0]}")
print(f"Unmatched documents: {len(unmatched)}")


# %% CELL 9 — TEST 3: AGING HYGIENE ------------------------------------------

aging_q = f"""
WITH banded AS (
    SELECT
        gl_account_no,
        CASE
            WHEN DATEDIFF(day, posting_date, CAST('{as_of_date}' AS DATE)) <= 30 THEN '0-30 days'
            WHEN DATEDIFF(day, posting_date, CAST('{as_of_date}' AS DATE)) <= 60 THEN '31-60 days'
            WHEN DATEDIFF(day, posting_date, CAST('{as_of_date}' AS DATE)) <= 90 THEN '61-90 days'
            WHEN DATEDIFF(day, posting_date, CAST('{as_of_date}' AS DATE)) <= 180 THEN '91-180 days'
            ELSE '180+ days'
        END AS age_band,
        net_amount
    FROM fa.gold_bc_gl_entry_clean
    WHERE gl_account_no IN ('10391','20380','10392','10410')
      AND posting_date <= CAST('{as_of_date}' AS DATE)
)
SELECT
    gl_account_no,
    age_band,
    COUNT(*)                         AS entries,
    CAST(SUM(net_amount) AS DOUBLE)  AS balance_thb
FROM banded
GROUP BY gl_account_no, age_band
ORDER BY gl_account_no, age_band
"""
aging = run_sql(aging_q)
if len(aging):
    aging["balance_thb"] = aging["balance_thb"].astype(float)
    aging["entries"]     = aging["entries"].astype(int)
print(aging)


# %% CELL 10 — WIP CLASSIFICATION (uniform sign, v1.3) -----------------------

wip_class_q = f"""
WITH pro_wip AS (
    SELECT
        ve_order_no AS prod_order_no,
        MAX(prod_status_x) AS prod_status,
        MIN(posting_date)  AS opened,
        MAX(posting_date)  AS last_activity,
        DATEDIFF(day, MAX(posting_date), CAST('{as_of_date}' AS DATE)) AS days_idle,
        CAST(-SUM(cost_actual + cost_expected) AS DOUBLE) AS wip_residual
    FROM (
        SELECT
            ilb.*,
            COALESCE(wip.prod_status, 'Unknown') AS prod_status_x
        FROM fa.gold_bc_interim_lines_base ilb
        LEFT JOIN (
            SELECT DISTINCT prod_order_no, prod_status
            FROM fa.gold_pbi_wip_detail
        ) wip ON ilb.ve_order_no = wip.prod_order_no
        WHERE ilb.ve_order_type = 'Production'
          AND ilb.posting_date <= CAST('{as_of_date}' AS DATE)
    ) joined
    GROUP BY ve_order_no
)
SELECT
    CASE
        WHEN ABS(wip_residual) < 1.00
            THEN 'A · Cleared (zero residual)'
        WHEN prod_status = 'Finished'
            THEN 'C · Stuck — Finished but non-zero (variance gap)'
        WHEN prod_status IN ('Released','In Progress','Firm Planned') AND days_idle > {abandoned_pro_days}
            THEN 'D · Abandoned suspect (no activity >120d)'
        WHEN prod_status IN ('Released','In Progress','Firm Planned')
            THEN 'B · Legitimate (in flight)'
        ELSE 'E · Other / unknown status'
    END AS classification,
    COUNT(*)                              AS pros,
    CAST(SUM(wip_residual) AS DOUBLE)     AS total_thb,
    MIN(opened)                           AS oldest_opened,
    CAST(MAX(days_idle) AS DOUBLE)        AS max_idle_days,
    CAST(AVG(days_idle) AS DOUBLE)        AS avg_idle_days
FROM pro_wip
GROUP BY
    CASE
        WHEN ABS(wip_residual) < 1.00
            THEN 'A · Cleared (zero residual)'
        WHEN prod_status = 'Finished'
            THEN 'C · Stuck — Finished but non-zero (variance gap)'
        WHEN prod_status IN ('Released','In Progress','Firm Planned') AND days_idle > {abandoned_pro_days}
            THEN 'D · Abandoned suspect (no activity >120d)'
        WHEN prod_status IN ('Released','In Progress','Firm Planned')
            THEN 'B · Legitimate (in flight)'
        ELSE 'E · Other / unknown status'
    END
ORDER BY classification
"""
wip_class = run_sql(wip_class_q)
if len(wip_class):
    wip_class["pros"]          = wip_class["pros"].astype(int)
    wip_class["total_thb"]     = wip_class["total_thb"].astype(float)
    wip_class["max_idle_days"] = wip_class["max_idle_days"].astype(float)
    wip_class["avg_idle_days"] = wip_class["avg_idle_days"].astype(float)
print(wip_class)


# %% CELL 11 — 10391 PRECIOUS-METAL FLAG -------------------------------------
# ORDER BY the alias (v1.2 patch).

precious_q = f"""
SELECT
    inv_posting_group,
    COUNT(*)                              AS open_receipts,
    CAST(SUM(cost_expected) AS DOUBLE)    AS interim_balance_thb,
    MAX(posting_date)                     AS most_recent
FROM fa.gold_bc_interim_lines_base
WHERE ile_entry_type = 'Purchase'
  AND ABS(cost_expected) > 0.01
  AND posting_date <= CAST('{as_of_date}' AS DATE)
GROUP BY inv_posting_group
ORDER BY ABS(interim_balance_thb) DESC
"""
precious = run_sql(precious_q)
if len(precious):
    precious["open_receipts"]       = precious["open_receipts"].astype(int)
    precious["interim_balance_thb"] = precious["interim_balance_thb"].astype(float)
PRECIOUS_GROUPS = {"RMPS", "RMPG", "RMPB", "RMPO"}
precious["is_precious"] = precious["inv_posting_group"].isin(PRECIOUS_GROUPS)
precious["interpretation"] = precious["is_precious"].apply(
    lambda x: "⚠ Precious — should be COD, near-zero" if x else "✓ Non-precious — balance expected"
)
print(precious)


# %% CELL 12 — VERDICT ROLL-UP -----------------------------------------------

verdict_rows = []
verdict_rows.append({
    "check":  "Gate · Adjust Cost recency",
    "status": recency_status[0],
    "color":  recency_status[1],
    "detail": f"Last run {recency['last_adjustment_date']} ({recency['days_since']} days ago)",
})
for _, r in recon.iterrows():
    verdict_rows.append({
        "check":  f"T1 · {r['acct']} {r['name']} — GL vs sub-ledger",
        "status": r["symbol"],
        "color":  r["color"],
        "detail": f"Difference {fmt_thb(r['diff'])} (tol ±{fmt_thb(r['tol'])})",
    })
verdict_rows.append({
    "check":  "T2 · Pair match 10391 ↔ 20380",
    "status": pair_v[0],
    "color":  pair_v[1],
    "detail": f"Net {fmt_thb(pair['pair_net'])} across {len(unmatched)} unmatched documents",
})

for acct in ["10391", "20380", "10392", "10410"]:
    aging_subset = aging[aging["gl_account_no"] == acct]
    if len(aging_subset) == 0:
        continue
    bad_bands = ["180+ days"] if acct == "10410" else ["61-90 days", "91-180 days", "180+ days"]
    bad_balance = float(aging_subset[aging_subset["age_band"].isin(bad_bands)]["balance_thb"].sum())
    tol = materiality_thb_wip if acct == "10410" else materiality_thb_interim
    v = verdict(bad_balance, tol)
    verdict_rows.append({
        "check":  f"T3 · {acct} aging hygiene",
        "status": v[0],
        "color":  v[1],
        "detail": f"Stuck balance in old buckets: {fmt_thb(bad_balance)}",
    })

verdicts = pd.DataFrame(verdict_rows)

if (verdicts["status"] == "✗").any():
    overall_v = ("✗", "red", "Do not close — material issues detected")
elif (verdicts["status"] == "⚠").any():
    overall_v = ("⚠", "amber", "Investigate before closing")
elif (verdicts["status"] == "?").any():
    overall_v = ("?", "gray", "Inconclusive — check missing data")
else:
    overall_v = ("✓", "green", "Verified correct — clear to close")

print("\n=== OVERALL VERDICT ===")
print(f"{overall_v[0]} {overall_v[2]}")


# %% CELL 13 — YEAR-OF-ORIGIN ANALYSIS (FOR IAS 8 / KPMG SCOPING) ------------
# Under TFRS / IAS 8, material errors must be allocated to the period they
# arose in, not absorbed into a single current-period adjustment. This cell
# produces three views and a bucketed allocation summary.
#
# All queries follow v1.1/v1.2/v1.3 patterns:
#   - CAST(... AS DOUBLE) on aggregates (avoid Decimal)
#   - ORDER BY alias (avoid Spark analyzer issue)
#   - Uniform -(cost_actual + cost_expected) for WIP residual

# View A: GL balance by account by year
yoa_q = f"""
SELECT
    gl_account_no,
    YEAR(posting_date) AS year,
    COUNT(*) AS entries,
    CAST(SUM(CASE WHEN net_amount > 0 THEN net_amount ELSE 0 END) AS DOUBLE) AS dr_amount,
    CAST(SUM(CASE WHEN net_amount < 0 THEN net_amount ELSE 0 END) AS DOUBLE) AS cr_amount,
    CAST(SUM(net_amount) AS DOUBLE) AS net_amount
FROM fa.gold_bc_gl_entry_clean
WHERE gl_account_no IN ('10391','20380','10392','10410')
  AND posting_date <= CAST('{as_of_date}' AS DATE)
GROUP BY gl_account_no, YEAR(posting_date)
ORDER BY gl_account_no, year
"""
yoa = run_sql(yoa_q)
if len(yoa):
    yoa["entries"]    = yoa["entries"].astype(int)
    yoa["dr_amount"]  = yoa["dr_amount"].astype(float)
    yoa["cr_amount"]  = yoa["cr_amount"].astype(float)
    yoa["net_amount"] = yoa["net_amount"].astype(float)

# View B: Stuck Finished PRO variance by year of last activity
stuck_by_year_q = f"""
WITH pro_summary AS (
    SELECT
        ve_order_no AS prod_order_no,
        MAX(prod_status_x) AS prod_status,
        YEAR(MAX(posting_date)) AS year_of_finish,
        CAST(-SUM(cost_actual + cost_expected) AS DOUBLE) AS wip_residual
    FROM (
        SELECT ilb.*, COALESCE(wip.prod_status, 'Unknown') AS prod_status_x
        FROM fa.gold_bc_interim_lines_base ilb
        LEFT JOIN (
            SELECT DISTINCT prod_order_no, prod_status
            FROM fa.gold_pbi_wip_detail
        ) wip ON ilb.ve_order_no = wip.prod_order_no
        WHERE ilb.ve_order_type = 'Production'
          AND ilb.posting_date <= CAST('{as_of_date}' AS DATE)
    ) joined
    GROUP BY ve_order_no
)
SELECT
    year_of_finish AS year,
    COUNT(*) AS pros_finished,
    CAST(SUM(CASE WHEN wip_residual > 0 THEN wip_residual ELSE 0 END) AS DOUBLE) AS unfavorable_thb,
    CAST(SUM(CASE WHEN wip_residual < 0 THEN wip_residual ELSE 0 END) AS DOUBLE) AS favorable_thb,
    CAST(SUM(wip_residual) AS DOUBLE) AS net_thb,
    CAST(SUM(ABS(wip_residual)) AS DOUBLE) AS gross_thb
FROM pro_summary
WHERE prod_status = 'Finished'
  AND ABS(wip_residual) > 1.00
GROUP BY year_of_finish
ORDER BY year_of_finish
"""
stuck_by_year = run_sql(stuck_by_year_q)
if len(stuck_by_year):
    stuck_by_year["pros_finished"]   = stuck_by_year["pros_finished"].astype(int)
    stuck_by_year["unfavorable_thb"] = stuck_by_year["unfavorable_thb"].astype(float)
    stuck_by_year["favorable_thb"]   = stuck_by_year["favorable_thb"].astype(float)
    stuck_by_year["net_thb"]         = stuck_by_year["net_thb"].astype(float)
    stuck_by_year["gross_thb"]       = stuck_by_year["gross_thb"].astype(float)

# View C: Pair-imbalance (10391/20380) by year
pair_by_year_q = f"""
SELECT
    YEAR(posting_date) AS year,
    CAST(SUM(CASE WHEN gl_account_no = '10391' THEN net_amount ELSE 0 END) AS DOUBLE) AS sum_10391,
    CAST(SUM(CASE WHEN gl_account_no = '20380' THEN net_amount ELSE 0 END) AS DOUBLE) AS sum_20380,
    CAST(SUM(net_amount) AS DOUBLE) AS pair_net,
    COUNT(DISTINCT document_no) AS docs
FROM fa.gold_bc_gl_entry_clean
WHERE gl_account_no IN ('10391','20380')
  AND posting_date <= CAST('{as_of_date}' AS DATE)
GROUP BY YEAR(posting_date)
ORDER BY year
"""
pair_by_year = run_sql(pair_by_year_q)
if len(pair_by_year):
    pair_by_year["sum_10391"] = pair_by_year["sum_10391"].astype(float)
    pair_by_year["sum_20380"] = pair_by_year["sum_20380"].astype(float)
    pair_by_year["pair_net"]  = pair_by_year["pair_net"].astype(float)
    pair_by_year["docs"]      = pair_by_year["docs"].astype(int)

# Allocation summary: IAS 8 treatment buckets
ias8_summary = {
    "pre_audit_year":  {"label": f"Pre-{audit_year} (adjust opening RE)",
                         "wip_net": 0.0, "wip_gross": 0.0, "pros": 0,
                         "pair_net": 0.0, "interim_net": 0.0},
    "audit_year":      {"label": f"{audit_year} (restate audit-year statements)",
                         "wip_net": 0.0, "wip_gross": 0.0, "pros": 0,
                         "pair_net": 0.0, "interim_net": 0.0},
    "post_audit_year": {"label": f"Post-{audit_year} (current-period correction)",
                         "wip_net": 0.0, "wip_gross": 0.0, "pros": 0,
                         "pair_net": 0.0, "interim_net": 0.0},
}

for _, r in stuck_by_year.iterrows():
    b = bucket_for(r["year"])
    if b is None:
        continue
    ias8_summary[b]["wip_net"]   += float(r["net_thb"])
    ias8_summary[b]["wip_gross"] += float(r["gross_thb"])
    ias8_summary[b]["pros"]      += int(r["pros_finished"])

for _, r in pair_by_year.iterrows():
    b = bucket_for(r["year"])
    if b is None:
        continue
    ias8_summary[b]["pair_net"] += float(r["pair_net"])

# "Other interim" = GL on 10391, 10392, 20380 by year (10410 covered in WIP bucket).
for _, r in yoa.iterrows():
    if r["gl_account_no"] == "10410":
        continue
    b = bucket_for(r["year"])
    if b is None:
        continue
    ias8_summary[b]["interim_net"] += float(r["net_amount"])

print("\n=== IAS 8 ALLOCATION SUMMARY ===")
for k, v in ias8_summary.items():
    print(f"  {v['label']}:")
    print(f"    WIP stuck variance (net):     {fmt_thb(v['wip_net'])}  across {v['pros']:,} PROs")
    print(f"    WIP stuck variance (gross):   {fmt_thb(v['wip_gross'])}")
    print(f"    10391/20380 pair imbalance:   {fmt_thb(v['pair_net'])}")
    print(f"    Other interim accounts (net): {fmt_thb(v['interim_net'])}")


# %% CELL 14 — BUILD HTML REPORT ---------------------------------------------

def render_status_pill(symbol, color):
    color_map = {
        "green":  ("#1B5E20", "#E8F5E9", "#c8e6c9"),
        "amber":  ("#92400E", "#FFFBEB", "#FDE68A"),
        "red":    ("#DC2626", "#FEF2F2", "#FECACA"),
        "gray":   ("#6B7A6A", "#F1EFE8", "#D3D1C7"),
    }
    fg, bg, brd = color_map.get(color, color_map["gray"])
    return f'<span style="background:{bg}; color:{fg}; border:1px solid {brd}; padding:3px 10px; border-radius:12px; font-family:IBM Plex Mono,monospace; font-size:12px; font-weight:600;">{symbol}</span>'


def build_html_report():
    overall_color_hex = {"green":"#1B5E20","amber":"#F59E0B","red":"#DC2626","gray":"#6B7A6A"}[overall_v[1]]

    # ─── Recon rows ───
    recon_rows = ""
    for _, r in recon.iterrows():
        recon_rows += f"""
        <tr>
            <td class="mono">{r['acct']}</td>
            <td>{r['name']}</td>
            <td class="mono right">{fmt_thb(r['gl'])}</td>
            <td class="mono right">{fmt_thb(r['expected'])}</td>
            <td class="mono right" style="color:{'#1B5E20' if abs(r['diff'])<r['tol'] else '#DC2626'};">{fmt_thb(r['diff'])}</td>
            <td class="mono">±{fmt_thb(r['tol'])}</td>
            <td>{render_status_pill(r['symbol'], r['color'])}</td>
        </tr>"""

    # ─── Aging table ───
    aging_pivot = aging.pivot_table(
        index="gl_account_no", columns="age_band",
        values="balance_thb", aggfunc="sum", fill_value=0,
    )
    age_cols = ["0-30 days","31-60 days","61-90 days","91-180 days","180+ days"]
    age_cols = [c for c in age_cols if c in aging_pivot.columns]
    aging_rows = ""
    for acct in aging_pivot.index:
        row = f'<tr><td class="mono">{acct}</td>'
        for c in age_cols:
            v = float(aging_pivot.loc[acct, c]) if c in aging_pivot.columns else 0.0
            extra_style = ""
            if c in ("61-90 days","91-180 days","180+ days") and abs(v) > 0.01:
                extra_style = ' style="color:#DC2626; font-weight:600;"'
            row += f'<td class="right mono"{extra_style}>{fmt_thb(v) if abs(v)>0.01 else "—"}</td>'
        row += "</tr>"
        aging_rows += row
    age_header = "".join(f"<th>{c}</th>" for c in age_cols)

    # ─── WIP classification ───
    wip_rows = ""
    wip_total = float(wip_class["total_thb"].sum()) if len(wip_class) else 0.0
    for _, r in wip_class.iterrows():
        pct = (float(r["total_thb"])/wip_total * 100) if wip_total else 0.0
        is_good = r["classification"].startswith(("A","B"))
        bg = "#FAFBF8" if is_good else "#FEF2F2"
        wip_rows += f"""
        <tr style="background:{bg};">
            <td><strong>{r['classification']}</strong></td>
            <td class="right mono">{fmt_int(r['pros'])}</td>
            <td class="right mono">{fmt_thb(r['total_thb'])}</td>
            <td class="right mono">{pct:.1f}%</td>
            <td class="right mono">{fmt_int(r['avg_idle_days'])}d</td>
            <td class="right mono">{fmt_int(r['max_idle_days'])}d</td>
        </tr>"""

    # ─── Precious metal table ───
    precious_rows = ""
    for _, r in precious.iterrows():
        is_precious = r["inv_posting_group"] in PRECIOUS_GROUPS
        flag = "⚠" if is_precious and abs(float(r["interim_balance_thb"])) > materiality_thb_interim else ""
        bg = "#FEF2F2" if is_precious and abs(float(r["interim_balance_thb"])) > materiality_thb_interim else ""
        precious_rows += f"""
        <tr style="background:{bg};">
            <td class="mono"><strong>{r['inv_posting_group']}</strong> {flag}</td>
            <td class="right mono">{fmt_int(r['open_receipts'])}</td>
            <td class="right mono">{fmt_thb(r['interim_balance_thb'])}</td>
            <td>{r['most_recent']}</td>
            <td>{r['interpretation']}</td>
        </tr>"""

    # ─── Verdict roll-up ───
    verdict_rows_html = ""
    for _, v in verdicts.iterrows():
        verdict_rows_html += f"""
        <tr>
            <td style="font-size:13px;">{v['check']}</td>
            <td>{render_status_pill(v['status'], v['color'])}</td>
            <td style="font-size:12px; color:#6B7A6A;">{v['detail']}</td>
        </tr>"""

    # ─── Unmatched docs ───
    unmatched_section = ""
    if len(unmatched) > 0:
        unmatched_rows = ""
        for _, u in unmatched.head(20).iterrows():
            unmatched_rows += f"""
            <tr>
                <td class="mono">{html_lib.escape(str(u['document_no']))}</td>
                <td class="right mono" style="color:#DC2626;">{fmt_thb(u['doc_net'])}</td>
                <td class="right mono">{fmt_int(u['legs'])}</td>
            </tr>"""
        unmatched_section = f"""
        <div class="card">
            <div class="card-header">
                <span class="card-title">Pair drill-down · {len(unmatched)} documents with unmatched legs</span>
                <span class="card-badge" style="background:#FEF2F2; color:#DC2626; border:1px solid #FECACA;">TOP 20</span>
            </div>
            <table>
                <tr><th>Document no</th><th class="right">Doc net (THB)</th><th class="right">Legs</th></tr>
                {unmatched_rows}
            </table>
        </div>"""

    # ─── NEW: Year-of-origin section ───
    # GL balance by account by year
    yoa_pivot = yoa.pivot_table(index="gl_account_no", columns="year",
                                 values="net_amount", aggfunc="sum", fill_value=0)
    years = sorted([y for y in yoa_pivot.columns if not pd.isna(y)])
    yoa_header = "".join(f"<th class='right'>{int(y)}</th>" for y in years)
    yoa_rows = ""
    for acct in yoa_pivot.index:
        row = f'<tr><td class="mono"><strong>{acct}</strong></td>'
        for y in years:
            v = float(yoa_pivot.loc[acct, y])
            is_prior = int(y) < audit_year
            is_audit = int(y) == audit_year
            style = ""
            if is_prior and abs(v) > materiality_thb_interim:
                style = ' style="color:#92400E; background:#FFFBEB;"'
            elif is_audit and abs(v) > materiality_thb_interim:
                style = ' style="color:#DC2626; background:#FEF2F2;"'
            row += f'<td class="right mono"{style}>{fmt_thb(v) if abs(v)>0.01 else "—"}</td>'
        row += "</tr>"
        yoa_rows += row

    # Stuck WIP by year of finish
    stuck_year_rows = ""
    for _, r in stuck_by_year.iterrows():
        y = int(r["year"]) if not pd.isna(r["year"]) else 0
        bucket_lbl = "Pre-audit-year (prior period)" if y < audit_year else ("Audit year" if y == audit_year else "Current period")
        bucket_color = "#FFFBEB" if y < audit_year else ("#FEF2F2" if y == audit_year else "#E8F5E9")
        stuck_year_rows += f"""
        <tr style="background:{bucket_color};">
            <td class="mono"><strong>{y}</strong></td>
            <td class="right mono">{fmt_int(r['pros_finished'])}</td>
            <td class="right mono" style="color:#DC2626;">{fmt_thb(r['unfavorable_thb'])}</td>
            <td class="right mono" style="color:#1B5E20;">{fmt_thb(r['favorable_thb'])}</td>
            <td class="right mono"><strong>{fmt_thb(r['net_thb'])}</strong></td>
            <td class="right mono">{fmt_thb(r['gross_thb'])}</td>
            <td style="font-size:11px;">{bucket_lbl}</td>
        </tr>"""

    # IAS 8 allocation summary
    ias8_rows = ""
    bucket_colors = {
        "pre_audit_year":  ("#FFFBEB", "#92400E", "Likely prior-period adjustment (adjust opening RE)"),
        "audit_year":      ("#FEF2F2", "#DC2626", "Subject to restatement in audit-year financial statements"),
        "post_audit_year": ("#E8F5E9", "#1B5E20", f"Current-period correction (booked in {current_year})"),
    }
    for key, b in ias8_summary.items():
        bg, fg, treatment = bucket_colors[key]
        total = b["wip_net"] + b["pair_net"] + b["interim_net"]
        ias8_rows += f"""
        <tr style="background:{bg};">
            <td style="color:{fg};"><strong>{b['label']}</strong></td>
            <td class="right mono">{fmt_thb(b['wip_net'])}</td>
            <td class="right mono">{fmt_int(b['pros'])}</td>
            <td class="right mono">{fmt_thb(b['pair_net'])}</td>
            <td class="right mono">{fmt_thb(b['interim_net'])}</td>
            <td class="right mono"><strong>{fmt_thb(total)}</strong></td>
            <td style="font-size:11px; color:{fg};">{treatment}</td>
        </tr>"""

    # Pair-imbalance by year
    pair_year_rows = ""
    for _, r in pair_by_year.iterrows():
        y = int(r["year"]) if not pd.isna(r["year"]) else 0
        is_pre = y < audit_year
        is_audit = y == audit_year
        style = ""
        if is_pre and abs(r["pair_net"]) > materiality_thb_pair:
            style = ' style="background:#FFFBEB;"'
        elif is_audit and abs(r["pair_net"]) > materiality_thb_pair:
            style = ' style="background:#FEF2F2;"'
        pair_year_rows += f"""
        <tr{style}>
            <td class="mono"><strong>{y}</strong></td>
            <td class="right mono">{fmt_thb(r['sum_10391'])}</td>
            <td class="right mono">{fmt_thb(r['sum_20380'])}</td>
            <td class="right mono"><strong>{fmt_thb(r['pair_net'])}</strong></td>
            <td class="right mono">{fmt_int(r['docs'])}</td>
        </tr>"""

    html_doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Ennovie Close Readiness — {as_of_date}</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@300;400;500;600;700&family=Playfair+Display:wght@700;900&display=swap" rel="stylesheet">
<style>
:root {{
    --green-dark:#1B5E20; --gold:#C8A951; --gold-light:#F5E6C0;
    --bg:#F4F5F0; --bg-dark:#0F1A0D; --card:#FFFFFF; --border:#E0E5D8;
    --text:#1A2118; --muted:#6B7A6A;
    --mono:'IBM Plex Mono',monospace; --sans:'IBM Plex Sans',sans-serif; --display:'Playfair Display',serif;
}}
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family:var(--sans); background:var(--bg); color:var(--text); line-height:1.6; font-size:14px; }}
.masthead {{ background:var(--bg-dark); color:white; padding:36px 60px 30px; position:relative; overflow:hidden; }}
.masthead::before {{ content:''; position:absolute; inset:0; background:repeating-linear-gradient(-55deg,transparent,transparent 40px,rgba(44,90,39,0.12) 40px,rgba(44,90,39,0.12) 41px); }}
.masthead-inner {{ position:relative; z-index:1; max-width:1100px; margin:0 auto; }}
.badge {{ display:inline-flex; align-items:center; gap:8px; background:rgba(200,169,81,0.15); border:1px solid rgba(200,169,81,0.35); color:var(--gold); padding:4px 12px; border-radius:4px; font-family:var(--mono); font-size:11px; letter-spacing:1.5px; text-transform:uppercase; margin-bottom:18px; }}
.masthead h1 {{ font-family:var(--display); font-size:34px; font-weight:900; line-height:1.1; margin-bottom:8px; background:linear-gradient(135deg,#fff 60%,var(--gold)); -webkit-background-clip:text; -webkit-text-fill-color:transparent; }}
.masthead-sub {{ color:rgba(255,255,255,0.55); font-size:14px; max-width:720px; margin-bottom:20px; }}
.meta-row {{ display:flex; gap:18px; flex-wrap:wrap; }}
.meta-pill {{ font-family:var(--mono); font-size:11px; color:rgba(255,255,255,0.6); background:rgba(255,255,255,0.06); border:1px solid rgba(255,255,255,0.1); padding:5px 12px; border-radius:3px; }}
.meta-pill span {{ color:white; }}
.main {{ max-width:1100px; margin:0 auto; padding:32px 60px; }}
.verdict-banner {{ background:white; border-left:8px solid {overall_color_hex}; padding:20px 28px; border-radius:0 10px 10px 0; box-shadow:0 1px 4px rgba(0,0,0,0.04); margin-bottom:28px; }}
.verdict-banner h2 {{ font-family:var(--display); font-size:22px; color:{overall_color_hex}; margin-bottom:4px; }}
.verdict-banner p {{ color:var(--muted); font-size:13px; }}
.section-header {{ display:flex; align-items:flex-start; gap:18px; margin:36px 0 18px; }}
.section-number {{ font-family:var(--mono); font-size:11px; font-weight:600; color:var(--gold); background:var(--gold-light); border:1px solid var(--gold); padding:6px 10px; border-radius:4px; margin-top:4px; }}
.section-title h2 {{ font-family:var(--display); font-size:22px; font-weight:700; color:var(--green-dark); }}
.section-title p {{ color:var(--muted); font-size:13px; margin-top:2px; }}
.card {{ background:var(--card); border:1px solid var(--border); border-radius:10px; padding:22px 26px; box-shadow:0 1px 4px rgba(0,0,0,0.04); margin-bottom:16px; }}
.card-header {{ display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; padding-bottom:12px; border-bottom:1px solid var(--border); }}
.card-title {{ font-weight:600; font-size:14px; color:var(--green-dark); }}
.card-badge {{ font-family:var(--mono); font-size:10px; font-weight:600; padding:3px 9px; border-radius:3px; letter-spacing:0.5px; background:#E8F5E9; color:#1B5E20; border:1px solid #c8e6c9; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th {{ background:#E8F5E9; color:var(--green-dark); font-weight:600; font-size:11px; letter-spacing:0.5px; text-transform:uppercase; padding:9px 14px; text-align:left; border-bottom:2px solid #c8e6c9; }}
td {{ padding:9px 14px; border-bottom:1px solid var(--border); vertical-align:top; }}
tr:last-child td {{ border-bottom:none; }}
.right {{ text-align:right; }}
.mono {{ font-family:var(--mono); font-size:12px; }}
.footer {{ background:var(--bg-dark); color:rgba(255,255,255,0.35); text-align:center; padding:22px; font-family:var(--mono); font-size:11px; }}
.callout {{ border-left:4px solid var(--gold); background:var(--gold-light); padding:12px 16px; border-radius:0 6px 6px 0; margin:12px 0; font-size:13px; color:#5a4000; }}
.callout-amber {{ border-left-color:#F59E0B; background:#FFFBEB; color:#92400E; }}
.callout-red {{ border-left-color:#DC2626; background:#FEF2F2; color:#7f1d1d; }}
.signature {{ background:white; border:1px dashed var(--border); border-radius:8px; padding:18px 24px; margin-top:24px; font-size:13px; color:var(--muted); }}
.signature .sig-line {{ display:flex; gap:32px; margin-top:14px; }}
.signature .sig-block {{ flex:1; border-bottom:1px solid #888; padding-bottom:4px; }}
.signature .sig-label {{ font-size:11px; color:var(--muted); margin-top:4px; font-family:var(--mono); text-transform:uppercase; letter-spacing:0.5px; }}
</style>
</head>
<body>

<div class="masthead">
    <div class="masthead-inner">
        <div class="badge">CLOSE READINESS · INVENTORY &amp; COGS VERIFICATION</div>
        <h1>WIP &amp; Interim Verification</h1>
        <p class="masthead-sub">Three-test reconciliation, pair-match, aging hygiene, and IAS 8 year-of-origin scoping on the four key balance-sheet positions.</p>
        <div class="meta-row">
            <div class="meta-pill">As of <span>{as_of_date}</span></div>
            <div class="meta-pill">Audit year <span>{audit_year}</span></div>
            <div class="meta-pill">Generated <span>{generated_at}</span></div>
            <div class="meta-pill">Source <span>Fabric · fa schema</span></div>
        </div>
    </div>
</div>

<div class="main">

<div class="verdict-banner">
    <h2>{overall_v[0]} &nbsp; Overall verdict: {overall_v[2]}</h2>
    <p>Materiality thresholds: ±{fmt_thb(materiality_thb_interim)} on interim accounts, ±{fmt_thb(materiality_thb_wip)} on WIP, ±{fmt_thb(materiality_thb_pair)} on the 10391/20380 pair.</p>
</div>

<div class="section-header">
    <div class="section-number">01</div>
    <div class="section-title">
        <h2>Verdict roll-up</h2>
        <p>One row per test.</p>
    </div>
</div>
<div class="card">
    <table>
        <tr><th>Check</th><th>Status</th><th>Detail</th></tr>
        {verdict_rows_html}
    </table>
</div>

<div class="section-header">
    <div class="section-number">02</div>
    <div class="section-title">
        <h2>Test 1 · GL vs sub-ledger reconciliation</h2>
    </div>
</div>
<div class="card">
    <table>
        <tr><th>Account</th><th>Name</th><th class="right">GL balance</th><th class="right">Sub-ledger expected</th><th class="right">Difference</th><th>Tolerance</th><th>Status</th></tr>
        {recon_rows}
    </table>
</div>

<div class="section-header">
    <div class="section-number">03</div>
    <div class="section-title">
        <h2>Test 2 · Pair match (10391 ↔ 20380)</h2>
    </div>
</div>
<div class="card">
    <table>
        <tr><th>Metric</th><th class="right">Value (THB)</th></tr>
        <tr><td>Sum of 10391 (Inv Interim)</td><td class="right mono">{fmt_thb(pair['sum_10391'])}</td></tr>
        <tr><td>Sum of 20380 (Accrual Interim)</td><td class="right mono">{fmt_thb(pair['sum_20380'])}</td></tr>
        <tr><td><strong>Pair net</strong></td><td class="right mono" style="color:{'#1B5E20' if abs(pair['pair_net'])<materiality_thb_pair else '#DC2626'};"><strong>{fmt_thb(pair['pair_net'])}</strong></td></tr>
        <tr><td>Documents with unmatched legs</td><td class="right mono">{len(unmatched)}</td></tr>
    </table>
</div>
{unmatched_section}

<div class="section-header">
    <div class="section-number">04</div>
    <div class="section-title">
        <h2>Test 3 · Aging hygiene</h2>
    </div>
</div>
<div class="card">
    <table>
        <tr><th>Account</th>{age_header}</tr>
        {aging_rows}
    </table>
</div>

<div class="section-header">
    <div class="section-number">05</div>
    <div class="section-title">
        <h2>WIP classification</h2>
    </div>
</div>
<div class="card">
    <table>
        <tr><th>Classification</th><th class="right">PROs</th><th class="right">Total THB</th><th class="right">% of WIP</th><th class="right">Avg idle</th><th class="right">Max idle</th></tr>
        {wip_rows}
    </table>
</div>

<div class="section-header">
    <div class="section-number">06</div>
    <div class="section-title">
        <h2>10391 by posting group — precious-metal flag</h2>
    </div>
</div>
<div class="card">
    <table>
        <tr><th>Posting group</th><th class="right">Open receipts</th><th class="right">Interim balance</th><th>Most recent</th><th>Interpretation</th></tr>
        {precious_rows}
    </table>
</div>

<div class="section-header">
    <div class="section-number">07</div>
    <div class="section-title">
        <h2>Year-of-origin analysis · IAS 8 / TFRS scoping</h2>
        <p>For the KPMG conversation: how should the identified errors be allocated across periods?</p>
    </div>
</div>

<div class="card">
    <div class="card-header">
        <span class="card-title">IAS 8 allocation summary — proposed treatment by bucket</span>
        <span class="card-badge" style="background:#FFFBEB; color:#92400E; border:1px solid #FDE68A;">FOR AUDITOR REVIEW</span>
    </div>
    <table>
        <tr>
            <th>Bucket</th>
            <th class="right">WIP variance (net)</th>
            <th class="right">PROs</th>
            <th class="right">Pair imbalance</th>
            <th class="right">Other interim</th>
            <th class="right">Combined</th>
            <th>Proposed treatment</th>
        </tr>
        {ias8_rows}
    </table>
    <div class="callout callout-amber" style="margin-top:14px;">
        <strong>Reading this:</strong> Under TFRS / IAS 8, material errors are allocated to the period they arose, not absorbed as a single current-period adjustment. The amber bucket (pre-{audit_year}) would adjust opening retained earnings of the earliest comparative period presented. The red bucket ({audit_year}) would restate the audit-year financial statements. The green bucket (post-{audit_year}) is a normal current-period correction.
    </div>
    <div class="callout callout-red">
        <strong>Important — not financial advice:</strong> The bucket assignments above are based on posting dates and PRO completion years, which are reasonable proxies for "when the error arose" but not authoritative. The final treatment must be agreed with KPMG. Materiality thresholds, comparative period requirements, and treatment of the underlying control deficiency are all auditor-judgement calls.
    </div>
</div>

<div class="card">
    <div class="card-header">
        <span class="card-title">GL balance breakdown by year-of-origin (4 accounts × all years)</span>
        <span class="card-badge">VIEW A</span>
    </div>
    <table>
        <tr><th>Account</th>{yoa_header}</tr>
        {yoa_rows}
    </table>
    <p style="font-size:12px; color:#6B7A6A; margin-top:10px;">
        Amber-shaded cells: pre-{audit_year} balance above materiality (prior-period exposure).
        Red-shaded cells: {audit_year} balance above materiality (audit-year exposure).
    </p>
</div>

<div class="card">
    <div class="card-header">
        <span class="card-title">Stuck Finished PRO variance by year of finish</span>
        <span class="card-badge">VIEW B</span>
    </div>
    <table>
        <tr>
            <th>Year</th>
            <th class="right">PROs finished</th>
            <th class="right">Unfavorable</th>
            <th class="right">Favorable</th>
            <th class="right">Net</th>
            <th class="right">Gross (abs)</th>
            <th>Period bucket</th>
        </tr>
        {stuck_year_rows}
    </table>
    <p style="font-size:12px; color:#6B7A6A; margin-top:10px;">
        Each row: PROs marked Finished in that year whose WIP residual didn't clear (Material Variance setup gap).
        Unfavorable variance = COGS should have been higher; favorable = COGS should have been lower.
    </p>
</div>

<div class="card">
    <div class="card-header">
        <span class="card-title">Pair imbalance by year (10391 + 20380)</span>
        <span class="card-badge">VIEW C</span>
    </div>
    <table>
        <tr><th>Year</th><th class="right">Sum 10391</th><th class="right">Sum 20380</th><th class="right">Net</th><th class="right">Docs</th></tr>
        {pair_year_rows}
    </table>
    <p style="font-size:12px; color:#6B7A6A; margin-top:10px;">
        Independent of WIP variance: this is the unmatched-pair issue, by year of origin.
        Likely driven by opening-balance migration journals and any large reclass entries.
    </p>
</div>

<div class="signature">
    <strong>Controller / Finance / KPMG sign-off:</strong>
    <div class="sig-line">
        <div class="sig-block"><div class="sig-label">Controller signature</div></div>
        <div class="sig-block"><div class="sig-label">CFO signature</div></div>
        <div class="sig-block"><div class="sig-label">Auditor acknowledgement</div></div>
    </div>
    <p style="margin-top:14px; font-size:12px;">
        Signatures confirm the verification report has been reviewed, findings have been investigated,
        and proposed IAS 8 allocation is agreed prior to booking adjustments.
    </p>
</div>

</div>

<div class="footer">
    Ennovie Close Readiness Report · Generated by nb_close_readiness_v3 · {generated_at}
    &nbsp;·&nbsp; Source: Fabric Lakehouse fa schema &nbsp;·&nbsp; As of {as_of_date} &nbsp;·&nbsp; Audit year {audit_year}
</div>

</body>
</html>"""
    return html_doc


html_output = build_html_report()


# %% CELL 15 — WRITE HTML TO LAKEHOUSE + INLINE PREVIEW ----------------------

os.makedirs(output_dir, exist_ok=True)
filename = f"close_readiness_v3_{as_of_date}_{datetime.now().strftime('%H%M%S')}.html"
full_path = os.path.join(output_dir, filename)

with open(full_path, "w", encoding="utf-8") as f:
    f.write(html_output)

print(f"✓ Report written to: {full_path}")
print(f"✓ File size: {os.path.getsize(full_path):,} bytes")
print(f"✓ Overall verdict: {overall_v[0]} {overall_v[2]}")

try:
    displayHTML(html_output)
except NameError:
    print("(displayHTML not available — open the file from Lakehouse Files area)")


# %% CELL 16 — OPTIONAL: PERSIST RESULTS TO A GOLD TABLE ---------------------
# Uncomment when fa.gold_cf_close_readiness_log exists. Appends one row per
# run for month-over-month tracking and to feed downstream BI signals.

# from pyspark.sql import Row
# history_row = Row(
#     as_of_date           = as_of_date,
#     audit_year           = audit_year,
#     generated_at         = generated_at,
#     overall_status       = overall_v[0],
#     overall_message      = overall_v[2],
#     gl_10391             = float(gl_bal("10391")),
#     gl_20380             = float(gl_bal("20380")),
#     gl_10392             = float(gl_bal("10392")),
#     gl_10410             = float(gl_bal("10410")),
#     diff_10391           = float(recon.loc[recon["acct"]=="10391","diff"].iloc[0]),
#     diff_10392           = float(recon.loc[recon["acct"]=="10392","diff"].iloc[0]),
#     diff_10410           = float(recon.loc[recon["acct"]=="10410","diff"].iloc[0]),
#     pair_net             = float(pair["pair_net"]),
#     unmatched_docs       = int(len(unmatched)),
#     wip_legitimate_thb   = float(wip_class[wip_class["classification"].str.startswith("B")]["total_thb"].sum()),
#     wip_stuck_thb        = float(wip_class[wip_class["classification"].str.startswith("C")]["total_thb"].sum()),
#     wip_abandoned_thb    = float(wip_class[wip_class["classification"].str.startswith("D")]["total_thb"].sum()),
#     pre_audit_wip_net    = ias8_summary["pre_audit_year"]["wip_net"],
#     audit_year_wip_net   = ias8_summary["audit_year"]["wip_net"],
#     post_audit_wip_net   = ias8_summary["post_audit_year"]["wip_net"],
#     pre_audit_pair_net   = ias8_summary["pre_audit_year"]["pair_net"],
#     audit_year_pair_net  = ias8_summary["audit_year"]["pair_net"],
#     post_audit_pair_net  = ias8_summary["post_audit_year"]["pair_net"],
#     report_path          = full_path,
# )
# spark.createDataFrame([history_row]) \
#      .write.format("delta").mode("append") \
#      .saveAsTable("fa.gold_cf_close_readiness_log")
# print("✓ History row appended to fa.gold_cf_close_readiness_log")

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
