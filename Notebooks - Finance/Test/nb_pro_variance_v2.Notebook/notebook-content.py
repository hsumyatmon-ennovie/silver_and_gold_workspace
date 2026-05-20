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
# nb_pro_variance_v2.py
#
# Ennovie — Per-PRO Material Variance Analysis (v2)
#
# v2 changes from v1:
#   - Outlier filtering: excludes PROs with planned cost > 10M THB
#     (the July 2024 inflated-cost data quality issue identified in v1)
#   - Min-planned floor: excludes PROs with planned < 100 THB (no real plan)
#   - Brand bridging through fa.gold_bc_gl_entry_clean (dim1) since
#     Prod Order Component's Shortcut Dimension 1 Code is mostly unpopulated
#   - New "Data Quality Findings" section surfacing excluded PROs separately
#   - Reconciliation now computed on CLEAN data only — should approximate
#     the BC stuck residual (~฿29M) much more closely
#
# The DQ findings are NOT discarded — they're presented as a separate
# operational/audit issue worth investigating. Two patterns expected:
#   1. Inflated planned cost (e.g., WRO240703XXX series) — likely UOM
#      conversion or revaluation event in July 2024
#   2. Missing BOM (e.g., WSP* series) — planned ≈ 0, actual > 10K
#      consumption against PROs without proper BOM setup
# ============================================================================


# %% CELL 1 — PARAMETERS ------------------------------------------------------

as_of_date                     = None         # "YYYY-MM-DD"; None = today
year_from                      = 2023
output_dir                     = "/lakehouse/default/Files/finance_reports/pro_variance"
silver_lakehouse               = "Silver_BC_Lakehouse"

# Variance materiality
materiality_thb_per_pro        = 1000
top_n_outliers                 = 25

# DATA QUALITY filters
outlier_threshold_thb_per_pro  = 10_000_000   # PROs above this are excluded (inflated)
min_planned_for_variance       = 100           # PROs below this have no meaningful plan
suspicious_ratio               = 100           # actual/planned ratio above this is suspicious

# Persistence
persist_to_gold                = False


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
audit_year = int(as_of_date[:4]) - 1

print(f"PRO variance analysis (v2) as of: {as_of_date}")
print(f"Audit year:                       {audit_year}")
print(f"Outlier threshold (per PRO):      ฿{outlier_threshold_thb_per_pro:,}")
print(f"Min planned for variance:         ฿{min_planned_for_variance:,}")


# %% CELL 3 — SELF-DISCOVERY -------------------------------------------------
print("\n── Schema discovery ──")
try:
    poc_count = spark.sql(f"""
        SELECT COUNT(*) AS n FROM `{silver_lakehouse}`.bc.`Prod Order Component`
    """).toPandas().iloc[0]["n"]
    print(f"✓ Prod Order Component accessible: {int(poc_count):,} rows")
except Exception as e:
    print(f"✗ Cannot access Prod Order Component: {e}")
    raise


# %% CELL 4 — HELPERS ---------------------------------------------------------

def run_sql(query):
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

planned_q = f"""
SELECT
    `Prod. Order No.`                     AS prod_order_no,
    `Item No.`                            AS item_no,
    MAX(`Status`)                         AS pro_status_poc,
    SUM(`Quantity`)                       AS planned_qty,
    SUM(`Direct Cost Amount`)             AS planned_direct_cost,
    SUM(`Cost Amount`)                    AS planned_total_cost,
    AVG(NULLIF(`Direct Unit Cost`, 0))    AS planned_direct_unit_cost
FROM `{silver_lakehouse}`.bc.`Prod Order Component`
GROUP BY `Prod. Order No.`, `Item No.`
"""
planned = run_sql(planned_q)
print(f"\n✓ Planned: {len(planned):,} (PRO, Item) component rows")


# %% CELL 6 — PULL ACTUAL CONSUMPTION ----------------------------------------

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


# %% CELL 7 — BRAND BRIDGE via gold_bc_gl_entry_clean ------------------------
# Prod Order Component's Shortcut Dimension 1 Code is mostly NULL or has
# non-brand values (OFFICE-XX, PROD-XX). The real brand dim is on GL entries
# tagged with the PRO. Pick the dominant dim1 per PRO.

brand_bridge_q = f"""
WITH pro_brand_counts AS (
    SELECT
        prod_order_no,
        dim1,
        COUNT(*) AS hits
    FROM fa.gold_bc_gl_entry_clean
    WHERE prod_order_no IS NOT NULL
      AND prod_order_no <> ''
      AND dim1 IS NOT NULL
      AND dim1 <> ''
    GROUP BY prod_order_no, dim1
),
ranked AS (
    SELECT
        prod_order_no, dim1, hits,
        ROW_NUMBER() OVER (PARTITION BY prod_order_no ORDER BY hits DESC) AS rn
    FROM pro_brand_counts
)
SELECT prod_order_no, dim1 AS brand, hits
FROM ranked
WHERE rn = 1
"""
brand_bridge = run_sql(brand_bridge_q)
print(f"✓ Brand bridge: {len(brand_bridge):,} PROs have a dim1 in GL")

# Check distribution of brand values
brand_dist = brand_bridge["brand"].value_counts().head(20)
print(f"\nTop 20 dim1 values bridged from GL:")
print(brand_dist.to_string())


# %% CELL 8 — COMPONENT VARIANCE ---------------------------------------------

spark.createDataFrame(planned).createOrReplaceTempView("planned")
spark.createDataFrame(actual).createOrReplaceTempView("actual")

variance_q = """
SELECT
    COALESCE(p.prod_order_no, a.prod_order_no) AS prod_order_no,
    COALESCE(p.item_no,       a.item_no)       AS item_no,
    p.pro_status_poc,
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
print(f"\n✓ Per-component variance: {len(component_variance):,} rows")


# %% CELL 9 — PER-PRO TOTALS + BRAND JOIN ------------------------------------

pro_meta_q = f"""
SELECT
    prod_order_no,
    MAX(prod_status)   AS prod_status,
    MAX(finished_date) AS finished_date,
    MIN(posting_date)  AS opened_date,
    MAX(posting_date)  AS last_activity
FROM fa.gold_pbi_wip_detail
GROUP BY prod_order_no
"""
pro_meta = run_sql(pro_meta_q)
spark.createDataFrame(pro_meta).createOrReplaceTempView("pro_meta")
spark.createDataFrame(component_variance).createOrReplaceTempView("comp_var")
spark.createDataFrame(brand_bridge).createOrReplaceTempView("brand_bridge")

pro_variance_q = """
SELECT
    cv.prod_order_no,
    b.brand,
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
    SUM(ABS(cv.variance_thb))               AS gross_variance,
    SUM(CASE WHEN cv.coverage_flag = 'Off-BOM consumption' THEN 1 ELSE 0 END) AS off_bom_lines,
    SUM(CASE WHEN cv.coverage_flag = 'Planned but not consumed' THEN 1 ELSE 0 END) AS plan_only_lines
FROM comp_var cv
LEFT JOIN pro_meta m ON m.prod_order_no = cv.prod_order_no
LEFT JOIN brand_bridge b ON b.prod_order_no = cv.prod_order_no
GROUP BY cv.prod_order_no, b.brand
"""
pro_variance = run_sql(pro_variance_q)
pro_variance["variance_pct"] = pro_variance.apply(
    lambda r: r["variance_total"] / r["planned_total"] if r["planned_total"] not in (0, None) else None,
    axis=1
)
print(f"\n✓ Per-PRO totals: {len(pro_variance):,} PROs")


# %% CELL 10 — CATEGORIZE PROs (data quality split) --------------------------
# Three buckets:
#   VALID   — planned and actual both reasonable; included in variance calc
#   DQ-A    — inflated planned (> outlier_threshold)
#   DQ-B    — missing BOM (planned < min, actual > 10K)
#   DQ-C    — no actual consumption (planned > min, actual = 0)
#   DQ-D    — no plan, no actual (empty PROs)

def categorize(row):
    p = row["planned_total"] or 0
    a = row["actual_total"]  or 0
    if p > outlier_threshold_thb_per_pro:
        return "DQ-A · Inflated planned cost"
    if p < min_planned_for_variance and a > 10_000:
        return "DQ-B · Missing BOM (no plan, large actual)"
    if p >= min_planned_for_variance and a == 0:
        return "DQ-C · No consumption posted"
    if p < min_planned_for_variance and a < 10_000:
        return "DQ-D · Empty / not started"
    return "VALID"

pro_variance["category"] = pro_variance.apply(categorize, axis=1)

cat_summary = pro_variance.groupby("category").agg(
    pros=("prod_order_no", "count"),
    planned_total=("planned_total", "sum"),
    actual_total=("actual_total", "sum"),
    variance_total=("variance_total", "sum"),
).reset_index().sort_values("pros", ascending=False)

print("\n=== PRO CATEGORIZATION ===")
print(cat_summary.to_string(index=False))

valid_pros = pro_variance[pro_variance["category"] == "VALID"].copy()
print(f"\nVALID PROs (used for variance analysis): {len(valid_pros):,}")
print(f"Excluded PROs (data quality issues):     {len(pro_variance) - len(valid_pros):,}")


# %% CELL 11 — FINISHED-ONLY VARIANCE (the clean operational view) -----------

finished_valid = valid_pros[valid_pros["prod_status"] == "Finished"].copy()
print(f"\n=== CLEAN VARIANCE (VALID + Finished only) ===")
print(f"PROs:           {len(finished_valid):,}")
print(f"Planned total:  {fmt_thb(finished_valid['planned_total'].sum())}")
print(f"Actual total:   {fmt_thb(finished_valid['actual_total'].sum())}")
print(f"Net variance:   {fmt_thb(finished_valid['variance_total'].sum())}")
print(f"Gross variance: {fmt_thb(finished_valid['gross_variance'].sum())}")


# %% CELL 12 — YEAR-OF-FINISH VIEW (on clean data) ---------------------------

year_view = finished_valid.groupby("year_of_finish").agg(
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
print("\n=== YEAR-OF-FINISH (VALID only) ===")
print(year_view.to_string(index=False))


# %% CELL 13 — BRAND VIEW (on clean data, bridged) ---------------------------

brand_view = finished_valid.groupby("brand", dropna=False).agg(
    pros=("prod_order_no", "count"),
    planned_total=("planned_total", "sum"),
    actual_total=("actual_total", "sum"),
    net=("variance_total", "sum"),
    gross=("gross_variance", "sum"),
).reset_index().sort_values("gross", ascending=False)
brand_view["variance_pct"] = brand_view.apply(
    lambda r: r["net"] / r["planned_total"] if r["planned_total"] else None, axis=1
)
print(f"\n=== BY BRAND (VALID only, bridged via GL) — top 20 ===")
print(brand_view.head(20).to_string(index=False))


# %% CELL 14 — OUTLIERS (on clean data) --------------------------------------

top_unfav = finished_valid.nlargest(top_n_outliers, "variance_total")[
    ["prod_order_no", "brand", "year_of_finish", "planned_total",
     "actual_total", "variance_total", "variance_pct"]
].copy()
top_fav = finished_valid.nsmallest(top_n_outliers, "variance_total")[
    ["prod_order_no", "brand", "year_of_finish", "planned_total",
     "actual_total", "variance_total", "variance_pct"]
].copy()


# %% CELL 15 — DATA QUALITY FINDINGS ----------------------------------------

# DQ-A: top 25 inflated PROs (largest planned)
dq_inflated = pro_variance[pro_variance["category"] == "DQ-A · Inflated planned cost"]
dq_inflated_top = dq_inflated.nlargest(25, "planned_total")[
    ["prod_order_no", "year_of_finish", "planned_total", "actual_total", "variance_total"]
].copy()

# DQ-B: top 25 missing-BOM PROs (largest actual)
dq_no_bom = pro_variance[pro_variance["category"] == "DQ-B · Missing BOM (no plan, large actual)"]
dq_no_bom_top = dq_no_bom.nlargest(25, "actual_total")[
    ["prod_order_no", "prod_status", "year_of_finish", "planned_total", "actual_total"]
].copy()

# Aggregates per category
dq_inflated_total_planned = dq_inflated["planned_total"].sum() if len(dq_inflated) else 0
dq_inflated_total_actual  = dq_inflated["actual_total"].sum()  if len(dq_inflated) else 0
dq_no_bom_total_actual    = dq_no_bom["actual_total"].sum()    if len(dq_no_bom) else 0

print("\n=== DATA QUALITY FINDINGS ===")
print(f"DQ-A (inflated planned): {len(dq_inflated):,} PROs, planned={fmt_thb(dq_inflated_total_planned)}, actual={fmt_thb(dq_inflated_total_actual)}")
print(f"DQ-B (missing BOM):      {len(dq_no_bom):,} PROs, actual={fmt_thb(dq_no_bom_total_actual)}")


# %% CELL 16 — RECONCILIATION TO BC STUCK RESIDUAL ---------------------------

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
    SUM(CASE WHEN prod_status = 'Finished' AND ABS(bc_wip_residual) > 1.00
             THEN bc_wip_residual ELSE 0 END) AS bc_stuck_finished,
    SUM(CASE WHEN prod_status = 'Finished'
             THEN bc_wip_residual ELSE 0 END) AS bc_total_finished
FROM pro_wip
"""
bc_stuck_row = run_sql(bc_stuck_q).iloc[0]
bc_stuck_finished_total = bc_stuck_row["bc_stuck_finished"] or 0
bc_total_finished = bc_stuck_row["bc_total_finished"] or 0

clean_variance_total = finished_valid["variance_total"].sum()
# operational variance = -(BC residual) sign relationship explained in v1
recon_diff = clean_variance_total + bc_stuck_finished_total
recon_pct = abs(recon_diff) / max(abs(bc_stuck_finished_total), 1) * 100

print(f"\n=== RECONCILIATION (clean data) ===")
print(f"Clean operational variance:          {fmt_thb(clean_variance_total)}")
print(f"BC stuck WIP residual (Finished):    {fmt_thb(bc_stuck_finished_total)}")
print(f"Sum (should be ~0):                  {fmt_thb(recon_diff)}")
print(f"Gap as % of BC residual:             {recon_pct:.1f}%")

if recon_pct < 20:
    recon_status = ("✓", "green", f"Reconciles within {recon_pct:.0f}% — variance trustworthy")
elif recon_pct < 50:
    recon_status = ("⚠", "amber", f"Reconciles within {recon_pct:.0f}% — review recommended")
else:
    recon_status = ("✗", "red", f"Gap is {recon_pct:.0f}% of BC residual — further investigation needed")


# %% CELL 17 — BUILD HTML ----------------------------------------------------

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
    tot_planned   = finished_valid["planned_total"].sum()
    tot_actual    = finished_valid["actual_total"].sum()
    tot_variance  = finished_valid["variance_total"].sum()
    tot_unfav     = finished_valid["unfav_variance"].sum()
    tot_fav       = finished_valid["fav_variance"].sum()
    tot_pros      = len(finished_valid)
    avg_var_pct   = (tot_variance / tot_planned) if tot_planned else 0
    favorable = tot_variance < 0
    headline_color = "#1B5E20" if favorable else "#DC2626"
    headline_label = "FAVORABLE — actual < planned" if favorable else "UNFAVORABLE — actual > planned"

    # Category summary rows
    cat_rows = ""
    cat_colors = {
        "VALID":                                       ("#E8F5E9", "#1B5E20"),
        "DQ-A · Inflated planned cost":                ("#FEF2F2", "#DC2626"),
        "DQ-B · Missing BOM (no plan, large actual)":  ("#FFFBEB", "#92400E"),
        "DQ-C · No consumption posted":                ("#F1EFE8", "#6B7A6A"),
        "DQ-D · Empty / not started":                  ("#F1EFE8", "#6B7A6A"),
    }
    for _, r in cat_summary.iterrows():
        bg, fg = cat_colors.get(r["category"], ("#FFFFFF", "#1A2118"))
        cat_rows += f"""
        <tr style="background:{bg};">
            <td style="color:{fg};"><strong>{r['category']}</strong></td>
            <td class="right mono">{fmt_int(r['pros'])}</td>
            <td class="right mono">{fmt_thb(r['planned_total'])}</td>
            <td class="right mono">{fmt_thb(r['actual_total'])}</td>
            <td class="right mono">{fmt_thb(r['variance_total'])}</td>
        </tr>"""

    # Year-of-finish rows
    year_rows = ""
    for _, r in year_view.iterrows():
        y = int(r["year_of_finish"])
        bg = "#FFFBEB" if y < audit_year else ("#FEF2F2" if y == audit_year else "#E8F5E9")
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

    # Brand rows (top 20)
    brand_rows = ""
    for _, r in brand_view.head(20).iterrows():
        b = r["brand"] if pd.notna(r["brand"]) and r["brand"] else "(no dim1)"
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

    # Top unfavorable
    unfav_rows = ""
    for _, r in top_unfav.iterrows():
        unfav_rows += f"""
        <tr>
            <td class="mono">{html_lib.escape(str(r['prod_order_no']))}</td>
            <td class="mono">{html_lib.escape(str(r['brand'] if pd.notna(r['brand']) else '—'))}</td>
            <td class="right mono">{fmt_int(r['year_of_finish'])}</td>
            <td class="right mono">{fmt_thb(r['planned_total'])}</td>
            <td class="right mono">{fmt_thb(r['actual_total'])}</td>
            <td class="right mono" style="color:#DC2626;"><strong>{fmt_thb(r['variance_total'])}</strong></td>
            <td class="right mono">{fmt_pct(r['variance_pct'])}</td>
        </tr>"""

    # Top favorable
    fav_rows = ""
    for _, r in top_fav.iterrows():
        fav_rows += f"""
        <tr>
            <td class="mono">{html_lib.escape(str(r['prod_order_no']))}</td>
            <td class="mono">{html_lib.escape(str(r['brand'] if pd.notna(r['brand']) else '—'))}</td>
            <td class="right mono">{fmt_int(r['year_of_finish'])}</td>
            <td class="right mono">{fmt_thb(r['planned_total'])}</td>
            <td class="right mono">{fmt_thb(r['actual_total'])}</td>
            <td class="right mono" style="color:#1B5E20;"><strong>{fmt_thb(r['variance_total'])}</strong></td>
            <td class="right mono">{fmt_pct(r['variance_pct'])}</td>
        </tr>"""

    # DQ-A rows
    dq_inflated_rows = ""
    for _, r in dq_inflated_top.iterrows():
        dq_inflated_rows += f"""
        <tr>
            <td class="mono">{html_lib.escape(str(r['prod_order_no']))}</td>
            <td class="right mono">{fmt_int(r['year_of_finish'])}</td>
            <td class="right mono" style="color:#DC2626;">{fmt_thb(r['planned_total'])}</td>
            <td class="right mono">{fmt_thb(r['actual_total'])}</td>
            <td class="right mono">{fmt_thb(r['variance_total'])}</td>
        </tr>"""

    # DQ-B rows
    dq_no_bom_rows = ""
    for _, r in dq_no_bom_top.iterrows():
        dq_no_bom_rows += f"""
        <tr>
            <td class="mono">{html_lib.escape(str(r['prod_order_no']))}</td>
            <td class="mono">{html_lib.escape(str(r['prod_status'] if pd.notna(r['prod_status']) else '—'))}</td>
            <td class="right mono">{fmt_int(r['year_of_finish'])}</td>
            <td class="right mono">{fmt_thb(r['planned_total'])}</td>
            <td class="right mono" style="color:#92400E;"><strong>{fmt_thb(r['actual_total'])}</strong></td>
        </tr>"""

    html_doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Ennovie PRO Variance v2 — {as_of_date}</title>
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
.card-badge-red {{ background:#FEF2F2; color:#DC2626; border:1px solid #FECACA; }}
.card-badge-amber {{ background:#FFFBEB; color:#92400E; border:1px solid #FDE68A; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th {{ background:#E8F5E9; color:var(--green-dark); font-weight:600; font-size:11px; letter-spacing:0.5px; text-transform:uppercase; padding:9px 14px; text-align:left; border-bottom:2px solid #c8e6c9; }}
td {{ padding:9px 14px; border-bottom:1px solid var(--border); vertical-align:top; }}
tr:last-child td {{ border-bottom:none; }}
.right {{ text-align:right; }}
.mono {{ font-family:var(--mono); font-size:12px; }}
.footer {{ background:var(--bg-dark); color:rgba(255,255,255,0.35); text-align:center; padding:22px; font-family:var(--mono); font-size:11px; }}
.callout {{ border-left:4px solid var(--gold); background:var(--gold-light); padding:12px 16px; border-radius:0 6px 6px 0; margin:12px 0; font-size:13px; color:#5a4000; }}
.callout-red {{ border-left-color:#DC2626; background:#FEF2F2; color:#7f1d1d; }}
.callout-amber {{ border-left-color:#F59E0B; background:#FFFBEB; color:#92400E; }}
.callout-green {{ border-left-color:#1B5E20; background:#E8F5E9; color:#1a3a14; }}
</style>
</head>
<body>

<div class="masthead">
    <div class="masthead-inner">
        <div class="badge">PRO VARIANCE LEDGER · v2 · WITH OUTLIER FILTER</div>
        <h1>Per-PRO Material Variance</h1>
        <p class="masthead-sub">Outlier-filtered variance computation with reconciliation to BC stuck residual and data quality findings surfaced separately.</p>
        <div class="meta-row">
            <div class="meta-pill">As of <span>{as_of_date}</span></div>
            <div class="meta-pill">Audit year <span>{audit_year}</span></div>
            <div class="meta-pill">Outlier filter <span>>฿{outlier_threshold_thb_per_pro/1_000_000:,.0f}M</span></div>
            <div class="meta-pill">Valid finished PROs <span>{fmt_int(tot_pros)}</span></div>
            <div class="meta-pill">Generated <span>{generated_at}</span></div>
        </div>
    </div>
</div>

<div class="main">

<div class="headline-banner">
    <h2>Clean net variance: {fmt_thb(tot_variance)} ({headline_label})</h2>
    <p>Across {fmt_int(tot_pros)} VALID Finished PROs (excluding {fmt_int(len(pro_variance) - len(valid_pros))} PROs with data quality issues — see section 02). Unfavorable: {fmt_thb(tot_unfav)}. Favorable: {fmt_thb(tot_fav)}. Variance as % of planned: {fmt_pct(avg_var_pct)}.</p>
</div>

<div class="kpi-grid">
    <div class="kpi">
        <div class="kpi-label">Clean planned cost</div>
        <div class="kpi-val">{fmt_thb(tot_planned)}</div>
        <div class="kpi-sub">VALID Finished PROs only</div>
    </div>
    <div class="kpi">
        <div class="kpi-label">Clean actual cost</div>
        <div class="kpi-val">{fmt_thb(tot_actual)}</div>
        <div class="kpi-sub">Consumption VEs, summed</div>
    </div>
    <div class="kpi">
        <div class="kpi-label">Clean net variance</div>
        <div class="kpi-val" style="color:{headline_color};">{fmt_thb(tot_variance)}</div>
        <div class="kpi-sub">{fmt_pct(avg_var_pct)} of planned</div>
    </div>
    <div class="kpi">
        <div class="kpi-label">BC stuck residual</div>
        <div class="kpi-val">{fmt_thb(bc_stuck_finished_total)}</div>
        <div class="kpi-sub">Reference target</div>
    </div>
</div>

<div class="section-header">
    <div class="section-number">01</div>
    <div class="section-title">
        <h2>Reconciliation · clean variance vs BC stuck residual</h2>
        <p>After excluding data quality outliers, the variance should approximate the BC GL-level stuck residual.</p>
    </div>
</div>
<div class="card">
    <table>
        <tr><th>Metric</th><th class="right">Value (THB)</th></tr>
        <tr><td>Clean operational variance (VALID PROs)</td><td class="right mono">{fmt_thb(clean_variance_total)}</td></tr>
        <tr><td>BC stuck WIP residual on Finished PROs</td><td class="right mono">{fmt_thb(bc_stuck_finished_total)}</td></tr>
        <tr><td><strong>Reconciliation gap</strong></td><td class="right mono"><strong>{fmt_thb(recon_diff)}</strong> &nbsp; {render_pill(recon_status[0], recon_status[1])}</td></tr>
        <tr><td>Gap as % of BC residual</td><td class="right mono">{recon_pct:.1f}%</td></tr>
    </table>
    <div class="callout callout-{recon_status[1]}" style="margin-top:14px;">
        <strong>{recon_status[2]}</strong>
    </div>
</div>

<div class="section-header">
    <div class="section-number">02</div>
    <div class="section-title">
        <h2>PRO categorization · data quality split</h2>
        <p>How many PROs fall into each bucket and how much value each bucket represents.</p>
    </div>
</div>
<div class="card">
    <table>
        <tr>
            <th>Category</th>
            <th class="right">PROs</th>
            <th class="right">Planned total</th>
            <th class="right">Actual total</th>
            <th class="right">Variance total</th>
        </tr>
        {cat_rows}
    </table>
    <div class="callout">
        <strong>Reading the categories:</strong> VALID = used for variance analysis (sections 03-06). DQ-A = inflated planned cost likely from a data event in July 2024. DQ-B = consumption posted to a PRO with no/minimal BOM. DQ-C = planned PRO that never consumed material. DQ-D = empty PROs (no plan, no actual). Sections 07-08 drill into DQ-A and DQ-B.
    </div>
</div>

<div class="section-header">
    <div class="section-number">03</div>
    <div class="section-title">
        <h2>Clean variance by year of finish · IAS 8 view</h2>
        <p>For the KPMG conversation — variance bucketed by year, outliers excluded.</p>
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
    <div class="section-number">04</div>
    <div class="section-title">
        <h2>Variance by brand customer · bridged via GL</h2>
        <p>Top 20 brands by gross variance. Brand sourced from gold_bc_gl_entry_clean[dim1] since Prod Order Component's dim1 is mostly unpopulated.</p>
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
    <div class="section-number">05</div>
    <div class="section-title">
        <h2>Top {top_n_outliers} unfavorable PROs · operational findings</h2>
    </div>
</div>
<div class="card">
    <table>
        <tr>
            <th>PRO No.</th><th>Brand</th><th class="right">Year</th>
            <th class="right">Planned</th><th class="right">Actual</th>
            <th class="right">Variance</th><th class="right">%</th>
        </tr>
        {unfav_rows}
    </table>
</div>

<div class="section-header">
    <div class="section-number">06</div>
    <div class="section-title">
        <h2>Top {top_n_outliers} favorable PROs · spot-check</h2>
    </div>
</div>
<div class="card">
    <table>
        <tr>
            <th>PRO No.</th><th>Brand</th><th class="right">Year</th>
            <th class="right">Planned</th><th class="right">Actual</th>
            <th class="right">Variance</th><th class="right">%</th>
        </tr>
        {fav_rows}
    </table>
</div>

<div class="section-header">
    <div class="section-number">07</div>
    <div class="section-title">
        <h2>Data quality finding A · inflated planned cost</h2>
        <p>PROs with planned cost above ฿{outlier_threshold_thb_per_pro/1_000_000:,.0f}M. Almost certainly data corruption, not real production cost.</p>
    </div>
</div>
<div class="card">
    <div class="card-header">
        <span class="card-title">{fmt_int(len(dq_inflated))} PROs · total planned {fmt_thb(dq_inflated_total_planned)} vs actual {fmt_thb(dq_inflated_total_actual)}</span>
        <span class="card-badge card-badge-red">EXCLUDED FROM VARIANCE</span>
    </div>
    <table>
        <tr>
            <th>PRO No.</th><th class="right">Year</th>
            <th class="right">Planned (inflated)</th>
            <th class="right">Actual</th><th class="right">Variance</th>
        </tr>
        {dq_inflated_rows}
    </table>
    <div class="callout callout-red" style="margin-top:14px;">
        <strong>Investigation priority:</strong> The pattern (WRO/WEG 2024-07-XX series, planned cost in billions, actual consumption negligible) matches the CAS2407-XXXX entries from the close-readiness pair imbalance — both showing up in July 2024. Likely a one-time data event: UOM conversion bug, inventory revaluation that touched Prod Order Component, or a custom extension/integration that wrote incorrect values for a window in July 2024. Worth asking the BC partner or DPA: "What changed in July 2024 affecting production order component costs and accounts 10391/20380?"
    </div>
</div>

<div class="section-header">
    <div class="section-number">08</div>
    <div class="section-title">
        <h2>Data quality finding B · missing BOM</h2>
        <p>PROs with planned cost &lt; ฿{min_planned_for_variance:,} but actual consumption &gt; ฿10,000.</p>
    </div>
</div>
<div class="card">
    <div class="card-header">
        <span class="card-title">{fmt_int(len(dq_no_bom))} PROs · total actual consumption {fmt_thb(dq_no_bom_total_actual)}</span>
        <span class="card-badge card-badge-amber">EXCLUDED FROM VARIANCE</span>
    </div>
    <table>
        <tr>
            <th>PRO No.</th><th>Status</th><th class="right">Year</th>
            <th class="right">Planned</th><th class="right">Actual</th>
        </tr>
        {dq_no_bom_rows}
    </table>
    <div class="callout callout-amber" style="margin-top:14px;">
        <strong>Process control finding:</strong> These PROs have consumption posted against them but little or no planned material on the Prod Order Component. Either (a) BOM was never refreshed on the PRO before starting, (b) consumption was posted against the wrong PRO, or (c) these are special-purpose PROs (samples, repairs, rework) that don't follow the standard BOM flow. Worth checking whether they have a common PRO prefix (e.g., WSP* for samples/work-shop) — if so, the process is intentional but should be documented as a non-standard category.
    </div>
</div>

</div>

<div class="footer">
    Ennovie PRO Variance Ledger · Generated by nb_pro_variance_v2 · {generated_at}
    &nbsp;·&nbsp; Outlier filter: >฿{outlier_threshold_thb_per_pro:,} per PRO
    &nbsp;·&nbsp; VALID Finished PROs: {fmt_int(tot_pros)}
</div>

</body>
</html>"""
    return html_doc


html_output = build_html()


# %% CELL 18 — WRITE + DISPLAY -----------------------------------------------

os.makedirs(output_dir, exist_ok=True)
fname = f"pro_variance_v2_{as_of_date}_{datetime.now().strftime('%H%M%S')}.html"
fpath = os.path.join(output_dir, fname)
with open(fpath, "w", encoding="utf-8") as f:
    f.write(html_output)

print(f"\n✓ Report written to: {fpath}")
print(f"✓ File size: {os.path.getsize(fpath):,} bytes")
print(f"✓ Recon status: {recon_status[0]} {recon_status[2]}")

try:
    displayHTML(html_output)
except NameError:
    print("(displayHTML not available — open file from Lakehouse Files area)")


# %% CELL 19 — OPTIONAL PERSIST ----------------------------------------------

if persist_to_gold:
    out_df = valid_pros.copy()
    out_df["as_of_date"] = as_of_date
    out_df["computed_at"] = generated_at
    out_df["outlier_threshold"] = outlier_threshold_thb_per_pro
    spark.createDataFrame(out_df).write.format("delta").mode("overwrite").saveAsTable("fa.gold_pro_variance")
    print(f"✓ Persisted {len(out_df):,} VALID PROs to fa.gold_pro_variance")
else:
    print("(persist_to_gold=False — not writing Delta table)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
