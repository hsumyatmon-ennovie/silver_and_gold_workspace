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
# META         },
# META         {
# META           "id": "1d620310-5acc-4534-93f9-f52f082a1887"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================================
# nb_pro_variance_v1.py
#
# Ennovie — Per-PRO Material Variance Analysis
#
# Purpose:
#   For every Finished Production Order, compute the variance between the
#   PLANNED material cost (from Prod Order Component, locked at PRO refresh)
#   and the ACTUAL material consumption cost (from Value Entries / FIFO).
#
#   This is the OPERATIONAL variance — the thing BC's Material Variance Account
#   is supposed to settle. By computing it here independently, we get:
#     1. A per-PRO variance ledger usable as a control narrative for KPMG
#     2. A reconciliation against the BC stuck WIP residual
#     3. Operational visibility into which PROs / brands / items drive variance
#
# Source data:
#   - Silver_BC_Lakehouse.bc.`Prod Order Component`  (planned cost per PRO+Item)
#   - fa.gold_bc_interim_lines_base                  (actual consumption VEs)
#   - fa.gold_pbi_wip_detail                         (PRO status + finished date)
#
# Output:
#   - HTML report → /lakehouse/default/Files/finance_reports/pro_variance/
#   - Optional Delta table → fa.gold_pro_variance
#
# Materials-only FIFO note:
#   Ennovie treats labor and overhead as period costs (not capitalized to
#   inventory). This notebook uses DIRECT cost fields (Direct Unit Cost,
#   Direct Cost Amount) rather than total Unit Cost / Cost Amount, which
#   would include indirect cost loading.
# ============================================================================


# %% CELL 1 — PARAMETERS ------------------------------------------------------

as_of_date              = None         # "YYYY-MM-DD"; None = today
year_from               = 2023         # earliest year to analyze (BC go-live: Jul 2023)
output_dir              = "/lakehouse/default/Files/finance_reports/pro_variance"
silver_lakehouse        = "Silver_BC_Lakehouse"   # name of the BC mirror lakehouse
materiality_thb_per_pro = 1000         # per-PRO variance over this is "material"
top_n_outliers          = 25           # how many extreme PROs to show in the report
persist_to_gold         = False        # set True to write fa.gold_pro_variance


# %% CELL 2 — IMPORTS & SESSION ----------------------------------------------
from datetime import datetime, date
from decimal import Decimal
from pyspark.sql import SparkSession
import pandas as pd
import os
import html as html_lib

spark = SparkSession.builder.getOrCreate()

if as_of_date is None:
    as_of_date = date.today().isoformat()
generated_at = datetime.now().isoformat(timespec="seconds")
audit_year = int(as_of_date[:4]) - 1   # KPMG audits prior year

print(f"PRO variance analysis as of: {as_of_date}")
print(f"Year range:                  {year_from} → {as_of_date[:4]}")
print(f"Audit year:                  {audit_year}")
print(f"Generated:                   {generated_at}")


# %% CELL 3 — SELF-DISCOVERY -------------------------------------------------
# Verify required tables and confirm Prod Order Component coverage of Finished PROs

print("\n── Schema discovery ──")
try:
    poc_df = spark.sql(f"""
        SELECT COUNT(*) AS n, COUNT(DISTINCT `Prod. Order No.`) AS pros,
               MIN(`Direct Cost Amount`) AS min_dca, MAX(`Direct Cost Amount`) AS max_dca,
               COUNT(DISTINCT `Status`) AS n_statuses
        FROM `{silver_lakehouse}`.bc.`Prod Order Component`
    """).toPandas().iloc[0]
    print(f"✓ Prod Order Component: {int(poc_df['n']):,} rows, "
          f"{int(poc_df['pros']):,} distinct PROs, {int(poc_df['n_statuses'])} statuses")
except Exception as e:
    print(f"✗ Cannot access Prod Order Component: {e}")
    print(f"  Verify the silver lakehouse name (currently '{silver_lakehouse}') "
          f"and that it's attached to this notebook.")
    raise

# Check what statuses are present (Released, Finished, etc.)
status_dist = spark.sql(f"""
    SELECT `Status`, COUNT(DISTINCT `Prod. Order No.`) AS pros
    FROM `{silver_lakehouse}`.bc.`Prod Order Component`
    GROUP BY `Status`
    ORDER BY pros DESC
""").toPandas()
print("\nStatus distribution in Prod Order Component:")
print(status_dist.to_string(index=False))

# Check coverage: how many Finished PROs in WIP detail have matching POC rows?
coverage = spark.sql(f"""
    WITH finished_pros AS (
        SELECT DISTINCT prod_order_no FROM fa.gold_pbi_wip_detail WHERE prod_status = 'Finished'
    ),
    poc_pros AS (
        SELECT DISTINCT `Prod. Order No.` AS prod_order_no
        FROM `{silver_lakehouse}`.bc.`Prod Order Component`
    )
    SELECT
        (SELECT COUNT(*) FROM finished_pros) AS total_finished,
        (SELECT COUNT(*) FROM finished_pros f
           INNER JOIN poc_pros p ON p.prod_order_no = f.prod_order_no) AS finished_with_poc
""").toPandas().iloc[0]
finished_with_poc = int(coverage["finished_with_poc"])
total_finished = int(coverage["total_finished"])
coverage_pct = finished_with_poc / total_finished * 100 if total_finished else 0
print(f"\nFinished PRO coverage: {finished_with_poc:,} / {total_finished:,} "
      f"({coverage_pct:.1f}%) have matching Prod Order Component rows")
if coverage_pct < 90:
    print("⚠  Coverage <90% — old Finished PROs may have been CLOSED (moved to "
          "Posted Prod Order Component, T5409). Variance for those PROs not computable here.")


# %% CELL 4 — HELPERS ---------------------------------------------------------

def run_sql(query):
    """Execute SQL; auto-cast Decimal columns to float."""
    df = spark.sql(query).toPandas()
    for col in df.columns:
        if df[col].dtype == object:
            nn = df[col].dropna()
            if len(nn) > 0 and isinstance(nn.iloc[0], Decimal):
                df[col] = df[col].astype(float)
    return df

def fmt_thb(x):
    if x is None or pd.isna(x):
        return "—"
    return f"฿{x:,.2f}"

def fmt_int(n):
    if n is None or pd.isna(n):
        return "—"
    return f"{int(n):,}"

def fmt_pct(x):
    if x is None or pd.isna(x):
        return "—"
    return f"{x:.1%}"


# %% CELL 5 — PULL PLANNED COSTS ---------------------------------------------
# One row per (PRO, Item) with planned direct cost (locked at PRO refresh)

planned_q = f"""
SELECT
    `Prod. Order No.`              AS prod_order_no,
    `Item No.`                     AS item_no,
    MAX(`Shortcut Dimension 1 Code`) AS brand,
    MAX(`Status`)                  AS pro_status,
    SUM(`Quantity`)                AS planned_qty,
    SUM(`Direct Cost Amount`)      AS planned_direct_cost,
    SUM(`Cost Amount`)             AS planned_total_cost,
    AVG(NULLIF(`Direct Unit Cost`, 0)) AS planned_direct_unit_cost
FROM `{silver_lakehouse}`.bc.`Prod Order Component`
GROUP BY `Prod. Order No.`, `Item No.`
"""
planned = run_sql(planned_q)
print(f"\n✓ Planned: {len(planned):,} (PRO, Item) component rows")


# %% CELL 6 — PULL ACTUAL CONSUMPTION ----------------------------------------
# Sum of Value Entry consumption per (PRO, Item)
# Consumption VEs carry NEGATIVE cost_actual (relief from raw material's value);
# we negate to express the POSITIVE amount that flowed INTO WIP for that component

actual_q = f"""
SELECT
    ve_order_no                          AS prod_order_no,
    item_no,
    -SUM(cost_actual + cost_expected)    AS actual_cost,
    MIN(posting_date)                    AS first_consumption,
    MAX(posting_date)                    AS last_consumption,
    COUNT(*)                             AS ve_count
FROM fa.gold_bc_interim_lines_base
WHERE ve_order_type  = 'Production'
  AND ile_entry_type = 'Consumption'
  AND posting_date <= CAST('{as_of_date}' AS DATE)
  AND YEAR(posting_date) >= {year_from}
GROUP BY ve_order_no, item_no
"""
actual = run_sql(actual_q)
print(f"✓ Actual:  {len(actual):,} (PRO, Item) consumption summaries")


# %% CELL 7 — COMPUTE PER-COMPONENT VARIANCE ---------------------------------
# Full outer join: components with plan but no actual = under-consumption
# Components with actual but no plan = off-BOM consumption (potential finding)

# Use SQL via a temp view for the join (Spark handles large joins better than pandas)
spark.createDataFrame(planned).createOrReplaceTempView("planned")
spark.createDataFrame(actual).createOrReplaceTempView("actual")

variance_q = """
SELECT
    COALESCE(p.prod_order_no, a.prod_order_no) AS prod_order_no,
    COALESCE(p.item_no,       a.item_no)       AS item_no,
    p.brand,
    p.pro_status,
    p.planned_qty,
    p.planned_direct_cost,
    p.planned_direct_unit_cost,
    a.actual_cost,
    a.first_consumption,
    a.last_consumption,
    COALESCE(a.actual_cost, 0) - COALESCE(p.planned_direct_cost, 0) AS variance_thb,
    CASE
        WHEN p.prod_order_no IS NULL THEN 'Off-BOM consumption'
        WHEN a.prod_order_no IS NULL THEN 'Planned but not consumed'
        ELSE 'Both'
    END AS coverage_flag
FROM planned p
FULL OUTER JOIN actual a
    ON a.prod_order_no = p.prod_order_no
   AND a.item_no       = p.item_no
"""
component_variance = run_sql(variance_q)

print(f"✓ Per-component variance: {len(component_variance):,} rows")
print("\nCoverage flag breakdown:")
print(component_variance["coverage_flag"].value_counts())


# %% CELL 8 — ROLL UP TO PER-PRO TOTALS --------------------------------------
# One row per PRO with total planned, total actual, variance, plus joins to
# PRO status / finished date from wip_detail

# Get PRO status and finished date from the wip detail
pro_meta_q = f"""
WITH pro_dates AS (
    SELECT
        prod_order_no,
        MAX(prod_status)   AS prod_status,
        MAX(finished_date) AS finished_date,
        MIN(posting_date)  AS opened_date,
        MAX(posting_date)  AS last_activity
    FROM fa.gold_pbi_wip_detail
    GROUP BY prod_order_no
)
SELECT * FROM pro_dates
"""
pro_meta = run_sql(pro_meta_q)
spark.createDataFrame(pro_meta).createOrReplaceTempView("pro_meta")

# Now roll up component_variance to per-PRO
spark.createDataFrame(component_variance).createOrReplaceTempView("comp_var")

pro_variance_q = """
SELECT
    cv.prod_order_no,
    MAX(cv.brand)                           AS brand,
    MAX(m.prod_status)                      AS prod_status,
    MAX(m.finished_date)                    AS finished_date,
    MAX(m.opened_date)                      AS opened_date,
    MAX(m.last_activity)                    AS last_activity,
    YEAR(MAX(m.finished_date))              AS year_of_finish,
    COUNT(*)                                AS component_lines,
    SUM(COALESCE(cv.planned_direct_cost,0)) AS planned_total,
    SUM(COALESCE(cv.actual_cost,0))         AS actual_total,
    SUM(cv.variance_thb)                    AS variance_total,
    SUM(CASE WHEN cv.variance_thb > 0 THEN cv.variance_thb ELSE 0 END) AS unfav_variance,
    SUM(CASE WHEN cv.variance_thb < 0 THEN cv.variance_thb ELSE 0 END) AS fav_variance,
    SUM(ABS(cv.variance_thb))               AS gross_variance
FROM comp_var cv
LEFT JOIN pro_meta m ON m.prod_order_no = cv.prod_order_no
GROUP BY cv.prod_order_no
"""
pro_variance = run_sql(pro_variance_q)
pro_variance["variance_pct"] = pro_variance.apply(
    lambda r: r["variance_total"] / r["planned_total"] if r["planned_total"] not in (0, None) else None,
    axis=1
)
print(f"\n✓ Per-PRO totals: {len(pro_variance):,} PROs")
print(f"  Total planned (all years):  {fmt_thb(pro_variance['planned_total'].sum())}")
print(f"  Total actual  (all years):  {fmt_thb(pro_variance['actual_total'].sum())}")
print(f"  Net variance  (all years):  {fmt_thb(pro_variance['variance_total'].sum())}")
print(f"  Gross variance (all years): {fmt_thb(pro_variance['gross_variance'].sum())}")


# %% CELL 9 — YEAR-OF-FINISH VIEW (KPMG) -------------------------------------

finished_only = pro_variance[pro_variance["prod_status"] == "Finished"].copy()

year_view = finished_only.groupby("year_of_finish").agg(
    pros=("prod_order_no", "count"),
    planned_total=("planned_total", "sum"),
    actual_total=("actual_total", "sum"),
    unfav=("unfav_variance", "sum"),
    fav=("fav_variance", "sum"),
    net=("variance_total", "sum"),
    gross=("gross_variance", "sum"),
).reset_index()
year_view = year_view[year_view["year_of_finish"].notna()]
year_view["year_of_finish"] = year_view["year_of_finish"].astype(int)
year_view = year_view.sort_values("year_of_finish")
year_view["bucket"] = year_view["year_of_finish"].apply(
    lambda y: f"Pre-{audit_year} (prior period)" if y < audit_year
              else (f"{audit_year} (audit year)" if y == audit_year
                    else f"Post-{audit_year} (current period)")
)
print("\n=== VARIANCE BY YEAR OF FINISH ===")
print(year_view.to_string(index=False))


# %% CELL 10 — BY-BRAND VIEW -------------------------------------------------

brand_view = finished_only.groupby("brand").agg(
    pros=("prod_order_no", "count"),
    planned_total=("planned_total", "sum"),
    actual_total=("actual_total", "sum"),
    net=("variance_total", "sum"),
    gross=("gross_variance", "sum"),
).reset_index().sort_values("gross", ascending=False)
brand_view["variance_pct"] = brand_view.apply(
    lambda r: r["net"] / r["planned_total"] if r["planned_total"] else None, axis=1
)
print(f"\n=== VARIANCE BY BRAND (top 20 by gross) ===")
print(brand_view.head(20).to_string(index=False))


# %% CELL 11 — TOP OUTLIERS --------------------------------------------------

# Top unfavorable (actual > planned)
top_unfav = finished_only.nlargest(top_n_outliers, "variance_total")[
    ["prod_order_no", "brand", "year_of_finish", "planned_total",
     "actual_total", "variance_total", "variance_pct"]
].copy()

# Top favorable (actual < planned)
top_fav = finished_only.nsmallest(top_n_outliers, "variance_total")[
    ["prod_order_no", "brand", "year_of_finish", "planned_total",
     "actual_total", "variance_total", "variance_pct"]
].copy()

# Off-BOM (consumption with no plan)
off_bom = component_variance[component_variance["coverage_flag"] == "Off-BOM consumption"].copy()
off_bom_sum = off_bom["actual_cost"].sum() if len(off_bom) else 0
print(f"\n=== OUTLIERS ===")
print(f"Top unfavorable PRO: {fmt_thb(top_unfav.iloc[0]['variance_total']) if len(top_unfav) else '—'}")
print(f"Top favorable PRO:   {fmt_thb(top_fav.iloc[0]['variance_total']) if len(top_fav) else '—'}")
print(f"Off-BOM consumption (no planned BOM, actual usage): "
      f"{len(off_bom):,} components, {fmt_thb(off_bom_sum)}")


# %% CELL 12 — RECONCILE TO BC STUCK RESIDUAL --------------------------------
# Compare the operational variance computed here to BC's stuck WIP residual
# (output cost - consumption cost on Finished PROs).
# These should reconcile: if output posts at standard cost = planned_direct_cost,
# then BC stuck residual ≈ planned_total - actual_total = -variance_total

bc_stuck_q = f"""
WITH pro_wip AS (
    SELECT
        ve_order_no AS prod_order_no,
        MAX(prod_status_x) AS prod_status,
        -SUM(cost_actual + cost_expected) AS bc_wip_residual
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
    prod_status,
    COUNT(*)                AS pros,
    SUM(bc_wip_residual)    AS bc_residual_total
FROM pro_wip
WHERE prod_status = 'Finished'
  AND ABS(bc_wip_residual) > 1.00
GROUP BY prod_status
"""
bc_stuck = run_sql(bc_stuck_q)
bc_stuck_total = bc_stuck["bc_residual_total"].sum() if len(bc_stuck) else 0

operational_variance_total = finished_only["variance_total"].sum()
recon_diff = operational_variance_total + bc_stuck_total   # should be near zero if formulas align
print(f"\n=== RECONCILIATION: Operational variance vs BC stuck residual ===")
print(f"Operational variance (this notebook): {fmt_thb(operational_variance_total)}")
print(f"BC stuck WIP residual (Finished PRO): {fmt_thb(bc_stuck_total)}")
print(f"Reconciliation gap (should be ~0):    {fmt_thb(recon_diff)}")
if abs(recon_diff) > abs(operational_variance_total) * 0.1:
    print("⚠  Gap exceeds 10% of operational variance — investigate.")
else:
    print("✓  Operational variance reconciles to BC stuck residual within 10%.")


# %% CELL 13 — BUILD HTML REPORT ---------------------------------------------

def render_pill(symbol, color):
    cmap = {
        "green":("#1B5E20","#E8F5E9","#c8e6c9"),
        "amber":("#92400E","#FFFBEB","#FDE68A"),
        "red":  ("#DC2626","#FEF2F2","#FECACA"),
        "gray": ("#6B7A6A","#F1EFE8","#D3D1C7"),
    }
    fg, bg, brd = cmap.get(color, cmap["gray"])
    return f'<span style="background:{bg}; color:{fg}; border:1px solid {brd}; padding:3px 10px; border-radius:12px; font-family:IBM Plex Mono,monospace; font-size:12px; font-weight:600;">{symbol}</span>'


def build_html():
    # KPIs
    tot_planned   = finished_only["planned_total"].sum()
    tot_actual    = finished_only["actual_total"].sum()
    tot_variance  = finished_only["variance_total"].sum()
    tot_unfav     = finished_only["unfav_variance"].sum()
    tot_fav       = finished_only["fav_variance"].sum()
    tot_pros      = len(finished_only)
    avg_var_pct   = (tot_variance / tot_planned) if tot_planned else 0
    favorable = tot_variance < 0
    headline_color = "#1B5E20" if favorable else "#DC2626"
    headline_label = "FAVORABLE — actual < planned" if favorable else "UNFAVORABLE — actual > planned"

    # ─── Year-of-finish rows ───
    year_rows = ""
    for _, r in year_view.iterrows():
        y = int(r["year_of_finish"])
        if y < audit_year:
            bg = "#FFFBEB"
        elif y == audit_year:
            bg = "#FEF2F2"
        else:
            bg = "#E8F5E9"
        year_rows += f"""
        <tr style="background:{bg};">
            <td class="mono"><strong>{y}</strong></td>
            <td class="right mono">{fmt_int(r['pros'])}</td>
            <td class="right mono">{fmt_thb(r['planned_total'])}</td>
            <td class="right mono">{fmt_thb(r['actual_total'])}</td>
            <td class="right mono" style="color:#DC2626;">{fmt_thb(r['unfav'])}</td>
            <td class="right mono" style="color:#1B5E20;">{fmt_thb(r['fav'])}</td>
            <td class="right mono"><strong>{fmt_thb(r['net'])}</strong></td>
            <td class="right mono">{fmt_thb(r['gross'])}</td>
            <td style="font-size:11px;">{r['bucket']}</td>
        </tr>"""

    # ─── Brand rows (top 20) ───
    brand_rows = ""
    for _, r in brand_view.head(20).iterrows():
        b = r["brand"] if r["brand"] else "(no dim1)"
        brand_rows += f"""
        <tr>
            <td class="mono"><strong>{html_lib.escape(str(b))}</strong></td>
            <td class="right mono">{fmt_int(r['pros'])}</td>
            <td class="right mono">{fmt_thb(r['planned_total'])}</td>
            <td class="right mono">{fmt_thb(r['actual_total'])}</td>
            <td class="right mono"><strong>{fmt_thb(r['net'])}</strong></td>
            <td class="right mono">{fmt_pct(r['variance_pct'])}</td>
            <td class="right mono">{fmt_thb(r['gross'])}</td>
        </tr>"""

    # ─── Top unfavorable rows ───
    unfav_rows = ""
    for _, r in top_unfav.iterrows():
        unfav_rows += f"""
        <tr>
            <td class="mono">{html_lib.escape(str(r['prod_order_no']))}</td>
            <td class="mono">{html_lib.escape(str(r['brand'] or '—'))}</td>
            <td class="right mono">{fmt_int(r['year_of_finish'])}</td>
            <td class="right mono">{fmt_thb(r['planned_total'])}</td>
            <td class="right mono">{fmt_thb(r['actual_total'])}</td>
            <td class="right mono" style="color:#DC2626;"><strong>{fmt_thb(r['variance_total'])}</strong></td>
            <td class="right mono">{fmt_pct(r['variance_pct'])}</td>
        </tr>"""

    # ─── Top favorable rows ───
    fav_rows = ""
    for _, r in top_fav.iterrows():
        fav_rows += f"""
        <tr>
            <td class="mono">{html_lib.escape(str(r['prod_order_no']))}</td>
            <td class="mono">{html_lib.escape(str(r['brand'] or '—'))}</td>
            <td class="right mono">{fmt_int(r['year_of_finish'])}</td>
            <td class="right mono">{fmt_thb(r['planned_total'])}</td>
            <td class="right mono">{fmt_thb(r['actual_total'])}</td>
            <td class="right mono" style="color:#1B5E20;"><strong>{fmt_thb(r['variance_total'])}</strong></td>
            <td class="right mono">{fmt_pct(r['variance_pct'])}</td>
        </tr>"""

    # ─── Reconciliation card ───
    recon_color = "green" if abs(recon_diff) < abs(operational_variance_total) * 0.1 else "amber"
    recon_msg = ("✓ Variance reconciles to BC stuck residual within 10%."
                 if recon_color == "green"
                 else "⚠ Variance does NOT fully reconcile — investigate gap.")

    html_doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Ennovie PRO Variance — {as_of_date}</title>
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
.kpi-grid {{ display:grid; grid-template-columns:repeat(4,1fr); gap:14px; margin-bottom:24px; }}
.kpi {{ background:white; border:1px solid var(--border); border-top:3px solid var(--green-dark); border-radius:8px; padding:16px 18px; }}
.kpi-label {{ font-size:11px; color:var(--muted); text-transform:uppercase; letter-spacing:0.5px; margin-bottom:6px; }}
.kpi-val {{ font-family:var(--display); font-size:22px; font-weight:700; color:var(--green-dark); }}
.kpi-sub {{ font-size:11px; color:var(--muted); margin-top:4px; font-family:var(--mono); }}
.headline-banner {{ background:white; border-left:8px solid {headline_color}; padding:20px 28px; border-radius:0 10px 10px 0; box-shadow:0 1px 4px rgba(0,0,0,0.04); margin-bottom:28px; }}
.headline-banner h2 {{ font-family:var(--display); font-size:22px; color:{headline_color}; margin-bottom:4px; }}
.headline-banner p {{ color:var(--muted); font-size:13px; }}
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
.callout-green {{ border-left-color:#1B5E20; background:#E8F5E9; color:#1a3a14; }}
</style>
</head>
<body>

<div class="masthead">
    <div class="masthead-inner">
        <div class="badge">PRO VARIANCE LEDGER · OPERATIONAL CONTROL</div>
        <h1>Per-PRO Material Variance</h1>
        <p class="masthead-sub">Independent variance computation from BOM-planned cost vs. Value Entry-actual consumption — the operational control narrative for the variance audit conversation.</p>
        <div class="meta-row">
            <div class="meta-pill">As of <span>{as_of_date}</span></div>
            <div class="meta-pill">Audit year <span>{audit_year}</span></div>
            <div class="meta-pill">PROs analyzed <span>{fmt_int(tot_pros)}</span></div>
            <div class="meta-pill">Generated <span>{generated_at}</span></div>
        </div>
    </div>
</div>

<div class="main">

<div class="headline-banner">
    <h2>Net variance: {fmt_thb(tot_variance)} ({headline_label})</h2>
    <p>Across {fmt_int(tot_pros)} Finished PROs from {year_from} onwards. Unfavorable component (actual > planned): {fmt_thb(tot_unfav)}. Favorable component (actual < planned): {fmt_thb(tot_fav)}. Variance as % of planned cost: {fmt_pct(avg_var_pct)}.</p>
</div>

<div class="kpi-grid">
    <div class="kpi">
        <div class="kpi-label">Planned cost (BOM)</div>
        <div class="kpi-val">{fmt_thb(tot_planned)}</div>
        <div class="kpi-sub">Direct cost × planned qty, summed</div>
    </div>
    <div class="kpi">
        <div class="kpi-label">Actual cost (FIFO)</div>
        <div class="kpi-val">{fmt_thb(tot_actual)}</div>
        <div class="kpi-sub">Consumption VEs, summed</div>
    </div>
    <div class="kpi">
        <div class="kpi-label">Net variance</div>
        <div class="kpi-val" style="color:{headline_color};">{fmt_thb(tot_variance)}</div>
        <div class="kpi-sub">{fmt_pct(avg_var_pct)} of planned</div>
    </div>
    <div class="kpi">
        <div class="kpi-label">Gross variance</div>
        <div class="kpi-val">{fmt_thb(finished_only['gross_variance'].sum())}</div>
        <div class="kpi-sub">Sum of |variance| per PRO</div>
    </div>
</div>

<div class="section-header">
    <div class="section-number">01</div>
    <div class="section-title">
        <h2>Reconciliation · operational variance vs BC stuck residual</h2>
        <p>This notebook's variance number should align with the BC stuck WIP residual from the close readiness report.</p>
    </div>
</div>
<div class="card">
    <table>
        <tr><th>Metric</th><th class="right">Value (THB)</th></tr>
        <tr><td>Operational variance (this notebook)</td><td class="right mono">{fmt_thb(operational_variance_total)}</td></tr>
        <tr><td>BC stuck WIP residual on Finished PROs</td><td class="right mono">{fmt_thb(bc_stuck_total)}</td></tr>
        <tr><td><strong>Reconciliation gap</strong> (should be ~0)</td><td class="right mono"><strong>{fmt_thb(recon_diff)}</strong> {render_pill('✓' if recon_color=='green' else '⚠', recon_color)}</td></tr>
    </table>
    <div class="callout callout-green" style="margin-top:14px;">
        <strong>{recon_msg}</strong> The reconciliation tests whether independent computation (planned BOM cost vs actual consumption) yields the same number as BC's GL-level stuck residual (output at standard cost vs consumption at actual). If aligned, the audit narrative is consistent: "operationally we know the variance, and it matches what GL says is stuck."
    </div>
</div>

<div class="section-header">
    <div class="section-number">02</div>
    <div class="section-title">
        <h2>Variance by year of finish · IAS 8 view</h2>
        <p>Per-PRO variance bucketed by the year the PRO was finished. This is what should have flowed to COGS in each year.</p>
    </div>
</div>
<div class="card">
    <table>
        <tr>
            <th>Year</th>
            <th class="right">PROs</th>
            <th class="right">Planned</th>
            <th class="right">Actual</th>
            <th class="right">Unfavorable</th>
            <th class="right">Favorable</th>
            <th class="right">Net</th>
            <th class="right">Gross</th>
            <th>Period bucket</th>
        </tr>
        {year_rows}
    </table>
</div>

<div class="section-header">
    <div class="section-number">03</div>
    <div class="section-title">
        <h2>Variance by brand customer · operational view</h2>
        <p>Top 20 brands by gross variance volume.</p>
    </div>
</div>
<div class="card">
    <table>
        <tr>
            <th>Brand (dim1)</th>
            <th class="right">PROs</th>
            <th class="right">Planned</th>
            <th class="right">Actual</th>
            <th class="right">Net variance</th>
            <th class="right">Variance %</th>
            <th class="right">Gross variance</th>
        </tr>
        {brand_rows}
    </table>
</div>

<div class="section-header">
    <div class="section-number">04</div>
    <div class="section-title">
        <h2>Top {top_n_outliers} unfavorable PROs · for operational investigation</h2>
        <p>PROs where actual cost most exceeded planned. These are where to look for yield issues, over-consumption, or pricing errors.</p>
    </div>
</div>
<div class="card">
    <table>
        <tr>
            <th>PRO No.</th>
            <th>Brand</th>
            <th class="right">Year</th>
            <th class="right">Planned</th>
            <th class="right">Actual</th>
            <th class="right">Variance (unfav)</th>
            <th class="right">%</th>
        </tr>
        {unfav_rows}
    </table>
</div>

<div class="section-header">
    <div class="section-number">05</div>
    <div class="section-title">
        <h2>Top {top_n_outliers} favorable PROs · for spot-check</h2>
        <p>PROs where actual cost came in well below planned. Worth spot-checking — sometimes large favorable variance signals a different issue (e.g., missing consumption posting).</p>
    </div>
</div>
<div class="card">
    <table>
        <tr>
            <th>PRO No.</th>
            <th>Brand</th>
            <th class="right">Year</th>
            <th class="right">Planned</th>
            <th class="right">Actual</th>
            <th class="right">Variance (fav)</th>
            <th class="right">%</th>
        </tr>
        {fav_rows}
    </table>
</div>

<div class="section-header">
    <div class="section-number">06</div>
    <div class="section-title">
        <h2>Off-BOM consumption · components used without a plan</h2>
        <p>Materials consumed against a PRO that didn't appear on the Production Order Component list.</p>
    </div>
</div>
<div class="card">
    <p style="font-size:14px;">
        <strong>{fmt_int(len(off_bom))} component lines</strong> with off-BOM consumption,
        totaling <strong>{fmt_thb(off_bom_sum)}</strong>.
    </p>
    <div class="callout">
        Off-BOM consumption can be legitimate (substitution materials, post-Finished adjustment journals) or indicate a control gap (consumption posted to the wrong PRO, BOM not refreshed after substitution, etc.). If the total is small relative to total variance, it's noise; if material, it's worth investigating which items and which PROs.
    </div>
</div>

</div>

<div class="footer">
    Ennovie PRO Variance Ledger · Generated by nb_pro_variance_v1 · {generated_at}
    &nbsp;·&nbsp; Source: Silver_BC_Lakehouse.bc.Prod Order Component + fa.gold_bc_interim_lines_base
    &nbsp;·&nbsp; PROs analyzed: {fmt_int(tot_pros)}
</div>

</body>
</html>"""
    return html_doc


html_output = build_html()


# %% CELL 14 — WRITE HTML + DISPLAY ------------------------------------------

os.makedirs(output_dir, exist_ok=True)
fname = f"pro_variance_v1_{as_of_date}_{datetime.now().strftime('%H%M%S')}.html"
fpath = os.path.join(output_dir, fname)
with open(fpath, "w", encoding="utf-8") as f:
    f.write(html_output)

print(f"\n✓ Report written to: {fpath}")
print(f"✓ File size: {os.path.getsize(fpath):,} bytes")

try:
    displayHTML(html_output)
except NameError:
    print("(displayHTML not available — open file from Lakehouse Files area)")


# %% CELL 15 — OPTIONAL: PERSIST TO GOLD TABLE -------------------------------
# Toggle persist_to_gold = True in Cell 1 to materialize. Writes one row per
# PRO into fa.gold_pro_variance for use in PBI dashboards and the AI cashflow
# intelligence report. Drops + recreates so it's always current as-of run time.

if persist_to_gold:
    out_df = pro_variance.copy()
    out_df["as_of_date"] = as_of_date
    out_df["computed_at"] = generated_at
    spark_df = spark.createDataFrame(out_df)
    spark_df.write.format("delta").mode("overwrite").saveAsTable("fa.gold_pro_variance")
    print(f"✓ Persisted {len(out_df):,} PROs to fa.gold_pro_variance")
else:
    print("(persist_to_gold=False — not writing Delta table; toggle in Cell 1 when ready)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
