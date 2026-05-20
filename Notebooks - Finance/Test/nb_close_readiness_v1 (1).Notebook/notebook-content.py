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
# nb_close_readiness_v1.py
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
# Three tests are run, plus a WIP classification and a precious-metals flag:
#   T1 — GL vs sub-ledger reconciliation per account
#   T2 — Pair match: 10391 + 20380 should net to zero
#   T3 — Aging hygiene per account
#   WIP classification: Legitimate / Stuck / Abandoned suspect / Cleared
#   10391 precious-metals flag: RMPS/RMPG/RMPB/RMPO should be near-zero
#
# Output:
#   HTML report written to the Lakehouse Files area + inline preview.
#
# Run modes:
#   - Interactive (notebook): set as_of_date = None to mean "today"
#   - Pipeline:  override as_of_date via Fabric pipeline parameter for
#                month-end close runs (e.g., as_of_date = "2026-03-31")
#
# Numeric typing notes (v1.1 patch):
#   BC mirrored Amount/Cost columns and SUM(DECIMAL) aggregations come back
#   from Spark as Python decimal.Decimal in pandas. Mixing those with native
#   Python float (e.g. from float() casts) triggers
#       TypeError: unsupported operand type(s) for -: 'float' and 'Decimal'
#   in pandas arithmetic. Fix: CAST(... AS DOUBLE) inside every SQL aggregate
#   and float() at every scalar extraction boundary, so all downstream math
#   stays in float64.
#
# Spark SQL aliasing note (v1.2 patch):
#   Spark's analyzer does not always re-resolve an aggregate function in
#   ORDER BY once it has been aliased in SELECT. The safe pattern is to
#   ORDER BY the alias (or by ABS(alias)), not by repeating the aggregate
#   expression. Applied to unmatched_q and precious_q.
#
# WIP sign convention note (v1.3 patch):
#   BC posts Value Entries with signed cost_actual / cost_expected — i.e.
#   Consumption already arrives negative (credit from raw mat → WIP) and
#   Output arrives positive (debit out of WIP → FG). The previous CASE
#   that flipped Output produced a double-negation and reconciled WIP
#   against an inverted sub-ledger.
#
#   Correct WIP residual per PRO = -(cost_actual + cost_expected) uniformly
#   over all Production VEs. This matches the sign convention of net_amount
#   in gold_bc_gl_entry_clean for account 10410, so T1 difference reflects
#   only real variance (Material/Purchase Variance gap), not a sign flip.
#   Applied to sub_10410 (Cell 7) and wip_class_q (Cell 10).
# ============================================================================


# %% CELL 1 — PARAMETERS ------------------------------------------------------
# Override these via Fabric pipeline parameters or edit inline.

as_of_date              = None     # "YYYY-MM-DD"  None = today
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

print(f"Close readiness check as of: {as_of_date}")
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
# Force float64 on the balance column so downstream math is safe
gl["gl_balance"] = gl["gl_balance"].astype(float)
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

# 10410 WIP — uniform sign convention.
# BC posts Consumption VEs with negative (cost_actual + cost_expected) and
# Output VEs with positive (cost_actual + cost_expected). The GL net_amount
# on 10410 follows the opposite convention (DR-positive for the WIP account),
# so the sub-ledger expected balance is the NEGATED sum of all signed VE costs.
# No CASE on ile_entry_type — the sign is already in the data.
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
# Belt-and-braces: ensure both columns are float64 before subtracting
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
# Use HAVING + ORDER BY on the alias (not the aggregate) — Spark analyzer
# rejects re-using an aggregate function once aliased; ORDER BY ABS(doc_net)
# is the canonical fix.
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
# A reconciled balance can still contain stuck entries. Aging tells you whether
# the balance reflects recent activity (correct) or stuck old entries (wrong).

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


# %% CELL 10 — WIP CLASSIFICATION (the "as of today" core test) --------------
# Each PRO falls into one of four buckets. "Correct WIP today" = Legitimate.
# WIP residual uses the same uniform sign convention as Cell 7's sub_10410:
# -(cost_actual + cost_expected) over all Production VEs, no CASE on
# ile_entry_type — the sign is already embedded in the source data.

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
# Per Ennovie's cash-on-delivery model for silver, gold, brass, GPC:
# RMPS / RMPG / RMPB / RMPO posting groups should show near-zero in 10391.
# Anything sitting there is an unusual non-COD precious purchase worth flagging.
# ORDER BY the alias (interim_balance_thb), not the raw aggregate.

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

# Aging verdict per account
for acct in ["10391", "20380", "10392", "10410"]:
    aging_subset = aging[aging["gl_account_no"] == acct]
    if len(aging_subset) == 0:
        continue
    # Sum of "bad" age bands
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

# Overall verdict
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


# %% CELL 13 — BUILD HTML REPORT ---------------------------------------------
# Matches the visual style of the Ennovie Finance Dashboard reference doc.

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
    # KPI cards (4 accounts + overall)
    overall_color_hex = {"green":"#1B5E20","amber":"#F59E0B","red":"#DC2626","gray":"#6B7A6A"}[overall_v[1]]

    # Reconciliation table rows
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

    # Aging table
    aging_pivot = aging.pivot_table(
        index="gl_account_no",
        columns="age_band",
        values="balance_thb",
        aggfunc="sum",
        fill_value=0,
    )
    age_cols = ["0-30 days", "31-60 days", "61-90 days", "91-180 days", "180+ days"]
    age_cols = [c for c in age_cols if c in aging_pivot.columns]
    aging_rows = ""
    for acct in aging_pivot.index:
        row = f'<tr><td class="mono">{acct}</td>'
        for c in age_cols:
            v = float(aging_pivot.loc[acct, c]) if c in aging_pivot.columns else 0.0
            cls = "right mono"
            extra_style = ""
            if c in ("61-90 days","91-180 days","180+ days") and abs(v) > 0.01:
                extra_style = ' style="color:#DC2626; font-weight:600;"'
            row += f'<td class="{cls}"{extra_style}>{fmt_thb(v) if abs(v)>0.01 else "—"}</td>'
        row += "</tr>"
        aging_rows += row
    age_header = "".join(f"<th>{c}</th>" for c in age_cols)

    # WIP classification table
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

    # Precious metal flag rows
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

    # Verdict roll-up
    verdict_rows_html = ""
    for _, v in verdicts.iterrows():
        verdict_rows_html += f"""
        <tr>
            <td style="font-size:13px;">{v['check']}</td>
            <td>{render_status_pill(v['status'], v['color'])}</td>
            <td style="font-size:12px; color:#6B7A6A;">{v['detail']}</td>
        </tr>"""

    # Unmatched documents (if any)
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
        <p class="masthead-sub">Three-test reconciliation, pair-match, and aging hygiene on the four key balance-sheet positions — plus WIP classification for production-in-flight integrity.</p>
        <div class="meta-row">
            <div class="meta-pill">As of <span>{as_of_date}</span></div>
            <div class="meta-pill">Generated <span>{generated_at}</span></div>
            <div class="meta-pill">Tests run <span>{len(verdicts)}</span></div>
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
        <p>One row per test. Click through to the relevant section below for detail.</p>
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
        <p>The formal test — every account should match the cost sub-ledger within tolerance.</p>
    </div>
</div>
<div class="card">
    <table>
        <tr>
            <th>Account</th><th>Name</th>
            <th class="right">GL balance</th>
            <th class="right">Sub-ledger expected</th>
            <th class="right">Difference</th>
            <th>Tolerance</th>
            <th>Status</th>
        </tr>
        {recon_rows}
    </table>
</div>

<div class="section-header">
    <div class="section-number">03</div>
    <div class="section-title">
        <h2>Test 2 · Pair match (10391 ↔ 20380)</h2>
        <p>Every receipt creates a DR on 10391 and a CR on 20380 of equal amount. Together they must sum to zero across all time.</p>
    </div>
</div>
<div class="card">
    <table>
        <tr><th>Metric</th><th class="right">Value (THB)</th></tr>
        <tr><td>Sum of 10391 (Inv Interim)</td><td class="right mono">{fmt_thb(pair['sum_10391'])}</td></tr>
        <tr><td>Sum of 20380 (Accrual Interim)</td><td class="right mono">{fmt_thb(pair['sum_20380'])}</td></tr>
        <tr><td><strong>Pair net</strong></td><td class="right mono" style="color:{'#1B5E20' if abs(pair['pair_net'])<materiality_thb_pair else '#DC2626'}; font-weight:600;">{fmt_thb(pair['pair_net'])}</td></tr>
        <tr><td>Documents with unmatched legs</td><td class="right mono">{len(unmatched)}</td></tr>
    </table>
</div>
{unmatched_section}

<div class="section-header">
    <div class="section-number">04</div>
    <div class="section-title">
        <h2>Test 3 · Aging hygiene</h2>
        <p>Balance correctness is also about <em>when</em> the entries are from. Old entries (>60 days for interim, >180 days for WIP) are suspect.</p>
    </div>
</div>
<div class="card">
    <table>
        <tr><th>Account</th>{age_header}</tr>
        {aging_rows}
    </table>
    <div class="callout" style="margin-top:14px;">
        <strong>Reading guide:</strong> For 10391, 20380, 10392 — anything beyond 31–60 days is unusual and suggests stuck cost (likely the Material/Purchase Variance setup gap). For 10410 WIP — 91–180 days is acceptable given Ennovie's 10–16 week production cycle; 180+ days signals abandoned or stuck PROs.
    </div>
</div>

<div class="section-header">
    <div class="section-number">05</div>
    <div class="section-title">
        <h2>WIP classification</h2>
        <p>Production orders bucketed by whether their WIP residual is legitimate, stuck, or suspect.</p>
    </div>
</div>
<div class="card">
    <table>
        <tr>
            <th>Classification</th>
            <th class="right">PROs</th>
            <th class="right">Total THB</th>
            <th class="right">% of WIP</th>
            <th class="right">Avg idle</th>
            <th class="right">Max idle</th>
        </tr>
        {wip_rows}
    </table>
    <div class="callout" style="margin-top:14px;">
        <strong>Correct WIP today = the Legitimate bucket (B).</strong> Stuck (C) reflects the Material Variance setup gap. Abandoned suspect (D) needs operational review — may be scrap, lost pieces, or PROs that should be closed manually.
    </div>
</div>

<div class="section-header">
    <div class="section-number">06</div>
    <div class="section-title">
        <h2>10391 by posting group — precious-metal flag</h2>
        <p>Precious metals are cash-on-delivery, so RMPS/RMPG/RMPB/RMPO should be near-zero. Anything here is an unusual non-COD precious purchase.</p>
    </div>
</div>
<div class="card">
    <table>
        <tr>
            <th>Posting group</th>
            <th class="right">Open receipts</th>
            <th class="right">Interim balance</th>
            <th>Most recent</th>
            <th>Interpretation</th>
        </tr>
        {precious_rows}
    </table>
</div>

<div class="signature">
    <strong>Controller / Finance sign-off:</strong>
    <div class="sig-line">
        <div class="sig-block">
            <div class="sig-label">Signature</div>
        </div>
        <div class="sig-block">
            <div class="sig-label">Date</div>
        </div>
    </div>
    <p style="margin-top:14px; font-size:12px;">By signing, the reviewer confirms the verification report has been read, the verdict accepted, and any ⚠ or ✗ findings investigated to satisfaction before posting period-end close entries.</p>
</div>

</div>

<div class="footer">
    Ennovie Close Readiness Report · Generated by nb_close_readiness_v1 · {generated_at}
    &nbsp;·&nbsp; Source: Fabric Lakehouse fa schema &nbsp;·&nbsp; As of {as_of_date}
</div>

</body>
</html>"""
    return html_doc


html_output = build_html_report()


# %% CELL 14 — WRITE HTML TO LAKEHOUSE + INLINE PREVIEW ----------------------

os.makedirs(output_dir, exist_ok=True)
filename = f"close_readiness_{as_of_date}_{datetime.now().strftime('%H%M%S')}.html"
full_path = os.path.join(output_dir, filename)

with open(full_path, "w", encoding="utf-8") as f:
    f.write(html_output)

print(f"✓ Report written to: {full_path}")
print(f"✓ File size: {os.path.getsize(full_path):,} bytes")
print(f"✓ Overall verdict: {overall_v[0]} {overall_v[2]}")

# Inline preview when running interactively
try:
    displayHTML(html_output)
except NameError:
    print("(displayHTML not available — open the file from Lakehouse Files area)")


# %% CELL 15 — OPTIONAL: PERSIST RESULTS TO A GOLD TABLE ---------------------
# Comment out if you don't want to track history. When uncommented, this
# appends one row per run to a gold_cf_close_readiness_log table for
# month-over-month comparison and audit trail.

# from pyspark.sql import Row
# history_row = Row(
#     as_of_date         = as_of_date,
#     generated_at       = generated_at,
#     overall_status     = overall_v[0],
#     overall_message    = overall_v[2],
#     gl_10391           = float(gl_bal("10391")),
#     gl_20380           = float(gl_bal("20380")),
#     gl_10392           = float(gl_bal("10392")),
#     gl_10410           = float(gl_bal("10410")),
#     diff_10391         = float(recon.loc[recon["acct"]=="10391","diff"].iloc[0]),
#     diff_10392         = float(recon.loc[recon["acct"]=="10392","diff"].iloc[0]),
#     diff_10410         = float(recon.loc[recon["acct"]=="10410","diff"].iloc[0]),
#     pair_net           = float(pair["pair_net"]),
#     unmatched_docs     = int(len(unmatched)),
#     wip_legitimate_thb = float(wip_class[wip_class["classification"].str.startswith("B")]["total_thb"].sum()),
#     wip_stuck_thb      = float(wip_class[wip_class["classification"].str.startswith("C")]["total_thb"].sum()),
#     wip_abandoned_thb  = float(wip_class[wip_class["classification"].str.startswith("D")]["total_thb"].sum()),
#     report_path        = full_path,
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
