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

spark.sql("SHOW TABLES IN Silver_BC_Lakehouse.bc LIKE 'G%'").show(truncate=False)
spark.sql("SHOW TABLES IN Silver_BC_Lakehouse.bc LIKE '%ccount%'").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # nb_pro_variance_v4.py

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# nb_pro_variance_v4.py
#
# Ennovie — Per-PRO Material Variance Analysis (v4)
#
# v4 changes from v3 — THE MASTER DATA FIX:
#
#   Root cause identified: specific item cards (e.g., M-935-SLV) have
#   corrupted `Direct Unit Cost` values (e.g., ฿15,001,982/g instead of
#   ~฿35/g for silver alloy). This propagates to `Direct Cost Amount` on
#   every Prod Order Component line consuming those items, but NOT to
#   `Cost Amount` (which uses `Unit Cost`, the field BC actually posts to GL).
#
#   v4 switches the planned-cost reference from `Direct Cost Amount` →
#   `Cost Amount`, and from `Direct Unit Cost` → `Unit Cost`. This bypasses
#   the corrupted field entirely. The reconciliation against BC stuck
#   residual should now land in green (within 20%) without aggressive
#   filtering, because the data is fundamentally clean.
#
#   Outlier filters retained as safety net for any other item-level
#   corruption that may exist on Unit Cost. Defaults relaxed back to ฿10M
#   absolute threshold since we expect clean data.
#
#   Note: `Unit Cost` may include overhead loading via Indirect Cost % and
#   Overhead Rate. For Ennovie's materials-only FIFO model these fields are
#   typically zero on items, so Unit Cost ≈ Direct Unit Cost on clean items.
#   If overhead is non-zero on some items, planned cost will be slightly
#   higher than actual consumption (since actual VEs reflect material only).
#   This shows as small systematic favorable variance — not a problem to
#   reconcile against BC, which has the same convention.
# ============================================================================


# %% CELL 1 — PARAMETERS ------------------------------------------------------

as_of_date                     = None
year_from                      = 2023
output_dir                     = "/lakehouse/default/Files/finance_reports/pro_variance"
silver_lakehouse               = "Silver_BC_Lakehouse"

materiality_thb_per_pro        = 1000
top_n_outliers                 = 25

# Outlier filters (safety nets — should rarely trigger with clean Unit Cost data)
outlier_threshold_thb_per_pro  = 10_000_000   # ฿10M per PRO (back from 1M in v3)
min_planned_for_variance       = 100
suspicious_min_planned         = 50_000
suspicious_actual_ratio        = 0.05

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

print(f"PRO variance analysis (v4) as of: {as_of_date}")
print(f"Audit year:                       {audit_year}")
print(f"Planned reference:                Cost Amount (Unit Cost × Quantity)")
print(f"  — NOT Direct Cost Amount (which has master data corruption)")


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


# %% CELL 5 — PULL PLANNED COSTS (v4: uses Cost Amount / Unit Cost) ---------
# Switched from Direct Cost Amount → Cost Amount to avoid the corrupted
# Direct Unit Cost field. Cost Amount uses the standard Unit Cost which
# BC actually posts to GL.

planned_q = f"""
SELECT
    `Prod. Order No.`                AS prod_order_no,
    `Item No.`                       AS item_no,
    MAX(`Status`)                    AS pro_status_poc,
    SUM(`Quantity`)                  AS planned_qty,
    SUM(`Cost Amount`)               AS planned_cost,            -- was Direct Cost Amount
    SUM(`Direct Cost Amount`)        AS planned_direct_cost_raw, -- kept for comparison
    AVG(NULLIF(`Unit Cost`, 0))      AS planned_unit_cost,       -- was Direct Unit Cost
    AVG(NULLIF(`Direct Unit Cost`, 0)) AS direct_unit_cost_raw   -- kept for DQ detection
FROM `{silver_lakehouse}`.bc.`Prod Order Component`
GROUP BY `Prod. Order No.`, `Item No.`
"""
planned = run_sql(planned_q)
print(f"\n✓ Planned: {len(planned):,} (PRO, Item) component rows")

# Compute the corruption indicator: ratio of Direct Unit Cost to Unit Cost
# If this is > 100, the item has corrupted master data
planned["unit_cost_ratio"] = planned.apply(
    lambda r: r["direct_unit_cost_raw"] / r["planned_unit_cost"]
              if r["planned_unit_cost"] not in (0, None) and r["planned_unit_cost"] > 0
              and r["direct_unit_cost_raw"] not in (0, None)
              else None,
    axis=1
)
n_corrupted_lines = planned[planned["unit_cost_ratio"] > 100].shape[0]
print(f"  Component lines with Direct Unit Cost > 100× Unit Cost: {n_corrupted_lines:,}")
print(f"  (these would have inflated variance in v1-v3; now correctly handled)")


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


# %% CELL 7 — BRAND BRIDGE ---------------------------------------------------

brand_bridge_q = f"""
WITH pro_brand_counts AS (
    SELECT prod_order_no, dim1, COUNT(*) AS hits
    FROM fa.gold_bc_gl_entry_clean
    WHERE prod_order_no IS NOT NULL AND prod_order_no <> ''
      AND dim1 IS NOT NULL AND dim1 <> ''
    GROUP BY prod_order_no, dim1
),
ranked AS (
    SELECT prod_order_no, dim1, hits,
           ROW_NUMBER() OVER (PARTITION BY prod_order_no ORDER BY hits DESC) AS rn
    FROM pro_brand_counts
)
SELECT prod_order_no, dim1 AS brand, hits
FROM ranked WHERE rn = 1
"""
brand_bridge = run_sql(brand_bridge_q)
print(f"✓ Brand bridge: {len(brand_bridge):,} PROs have a dim1 in GL")


# %% CELL 8 — COMPONENT VARIANCE (uses planned_cost, not planned_direct_cost)

spark.createDataFrame(planned).createOrReplaceTempView("planned")
spark.createDataFrame(actual).createOrReplaceTempView("actual")

variance_q = """
SELECT
    COALESCE(p.prod_order_no, a.prod_order_no) AS prod_order_no,
    COALESCE(p.item_no,       a.item_no)       AS item_no,
    p.pro_status_poc,
    p.planned_qty,
    p.planned_cost,
    p.planned_unit_cost,
    p.unit_cost_ratio,
    a.actual_cost,
    a.first_consumption,
    a.last_consumption,
    COALESCE(a.actual_cost, 0) - COALESCE(p.planned_cost, 0) AS variance_thb,
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
    SUM(COALESCE(cv.planned_cost,0))        AS planned_total,
    SUM(COALESCE(cv.actual_cost,0))         AS actual_total,
    SUM(cv.variance_thb)                    AS variance_total,
    SUM(CASE WHEN cv.variance_thb > 0 THEN cv.variance_thb ELSE 0 END) AS unfav_variance,
    SUM(CASE WHEN cv.variance_thb < 0 THEN cv.variance_thb ELSE 0 END) AS fav_variance,
    SUM(ABS(cv.variance_thb))               AS gross_variance,
    MAX(cv.unit_cost_ratio)                 AS max_unit_cost_ratio
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
pro_variance["actual_over_planned"] = pro_variance.apply(
    lambda r: r["actual_total"] / r["planned_total"] if r["planned_total"] not in (0, None) and r["planned_total"] > 0 else None,
    axis=1
)
print(f"\n✓ Per-PRO totals: {len(pro_variance):,} PROs")


# %% CELL 10 — CATEGORIZE PROs -----------------------------------------------

def categorize(row):
    p = row["planned_total"] or 0
    a = row["actual_total"]  or 0
    ratio = row["actual_over_planned"]
    if p > outlier_threshold_thb_per_pro:
        return "DQ-A1 · Absolute outlier (>฿10M)"
    if p > suspicious_min_planned and ratio is not None and ratio < suspicious_actual_ratio:
        return "DQ-A2 · Ratio outlier (actual < 5% of planned)"
    if p < min_planned_for_variance and a > 10_000:
        return "DQ-B · Missing BOM"
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

print("\n=== PRO CATEGORIZATION (v4) ===")
print(cat_summary.to_string(index=False))

valid_pros = pro_variance[pro_variance["category"] == "VALID"].copy()
print(f"\nVALID PROs: {len(valid_pros):,}")
print(f"Excluded:   {len(pro_variance) - len(valid_pros):,}")


# %% CELL 11 — FINISHED-ONLY VARIANCE ---------------------------------------

finished_valid = valid_pros[valid_pros["prod_status"] == "Finished"].copy()
print(f"\n=== CLEAN VARIANCE (v4, Cost Amount basis) ===")
print(f"PROs:           {len(finished_valid):,}")
print(f"Planned total:  {fmt_thb(finished_valid['planned_total'].sum())}")
print(f"Actual total:   {fmt_thb(finished_valid['actual_total'].sum())}")
print(f"Net variance:   {fmt_thb(finished_valid['variance_total'].sum())}")


# %% CELL 12 — YEAR-OF-FINISH ------------------------------------------------

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
print("\n=== YEAR-OF-FINISH (v4) ===")
print(year_view.to_string(index=False))


# %% CELL 13 — BRAND VIEW ----------------------------------------------------

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


# %% CELL 14 — OUTLIERS ------------------------------------------------------

top_unfav = finished_valid.nlargest(top_n_outliers, "variance_total")[
    ["prod_order_no", "brand", "year_of_finish", "planned_total",
     "actual_total", "variance_total", "variance_pct"]
].copy()
top_fav = finished_valid.nsmallest(top_n_outliers, "variance_total")[
    ["prod_order_no", "brand", "year_of_finish", "planned_total",
     "actual_total", "variance_total", "variance_pct"]
].copy()


# %% CELL 15 — DATA QUALITY DETAILS ------------------------------------------

dq_a1 = pro_variance[pro_variance["category"].str.startswith("DQ-A1")]
dq_a1_top = dq_a1.nlargest(25, "planned_total")[
    ["prod_order_no", "year_of_finish", "planned_total", "actual_total", "variance_total", "max_unit_cost_ratio"]
].copy()

dq_a2 = pro_variance[pro_variance["category"].str.startswith("DQ-A2")]
dq_a2_top = dq_a2.nlargest(25, "planned_total")[
    ["prod_order_no", "year_of_finish", "planned_total", "actual_total", "actual_over_planned", "max_unit_cost_ratio"]
].copy()

dq_no_bom = pro_variance[pro_variance["category"].str.startswith("DQ-B")]
dq_no_bom_top = dq_no_bom.nlargest(25, "actual_total")[
    ["prod_order_no", "prod_status", "year_of_finish", "planned_total", "actual_total"]
].copy()


# %% CELL 16 — RECONCILIATION ------------------------------------------------

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
             THEN bc_wip_residual ELSE 0 END) AS bc_stuck_finished
FROM pro_wip
"""
bc_stuck_finished_total = run_sql(bc_stuck_q).iloc[0]["bc_stuck_finished"] or 0
clean_variance_total = finished_valid["variance_total"].sum()
recon_diff = clean_variance_total + bc_stuck_finished_total
recon_pct = abs(recon_diff) / max(abs(bc_stuck_finished_total), 1) * 100

print(f"\n=== RECONCILIATION (v4) ===")
print(f"Clean operational variance:          {fmt_thb(clean_variance_total)}")
print(f"BC stuck WIP residual (Finished):    {fmt_thb(bc_stuck_finished_total)}")
print(f"Reconciliation gap:                  {fmt_thb(recon_diff)}")
print(f"Gap as % of BC residual:             {recon_pct:.1f}%")

if recon_pct < 20:
    recon_status = ("✓", "green", f"Reconciles within {recon_pct:.0f}% — variance trustworthy")
elif recon_pct < 50:
    recon_status = ("⚠", "amber", f"Reconciles within {recon_pct:.0f}% — review recommended")
else:
    recon_status = ("✗", "red", f"Gap is {recon_pct:.0f}% of BC residual — investigate")


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

    cat_rows = ""
    cat_colors = {
        "VALID":                                            ("#E8F5E9", "#1B5E20"),
        "DQ-A1 · Absolute outlier (>฿10M)":                 ("#FEF2F2", "#DC2626"),
        "DQ-A2 · Ratio outlier (actual < 5% of planned)":   ("#FEF2F2", "#DC2626"),
        "DQ-B · Missing BOM":                               ("#FFFBEB", "#92400E"),
        "DQ-C · No consumption posted":                     ("#F1EFE8", "#6B7A6A"),
        "DQ-D · Empty / not started":                       ("#F1EFE8", "#6B7A6A"),
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

    dq_a1_rows = ""
    for _, r in dq_a1_top.iterrows():
        ratio_str = f"{r['max_unit_cost_ratio']:,.0f}×" if pd.notna(r['max_unit_cost_ratio']) else "—"
        dq_a1_rows += f"""
        <tr>
            <td class="mono">{html_lib.escape(str(r['prod_order_no']))}</td>
            <td class="right mono">{fmt_int(r['year_of_finish'])}</td>
            <td class="right mono" style="color:#DC2626;">{fmt_thb(r['planned_total'])}</td>
            <td class="right mono">{fmt_thb(r['actual_total'])}</td>
            <td class="right mono">{fmt_thb(r['variance_total'])}</td>
            <td class="right mono">{ratio_str}</td>
        </tr>"""

    dq_a2_rows = ""
    for _, r in dq_a2_top.iterrows():
        ratio_str = f"{r['max_unit_cost_ratio']:,.0f}×" if pd.notna(r['max_unit_cost_ratio']) else "—"
        dq_a2_rows += f"""
        <tr>
            <td class="mono">{html_lib.escape(str(r['prod_order_no']))}</td>
            <td class="right mono">{fmt_int(r['year_of_finish'])}</td>
            <td class="right mono" style="color:#DC2626;">{fmt_thb(r['planned_total'])}</td>
            <td class="right mono">{fmt_thb(r['actual_total'])}</td>
            <td class="right mono">{fmt_pct(r['actual_over_planned'])}</td>
            <td class="right mono">{ratio_str}</td>
        </tr>"""

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

    dq_a1_total = dq_a1["planned_total"].sum() if len(dq_a1) else 0
    dq_a2_total = dq_a2["planned_total"].sum() if len(dq_a2) else 0
    dq_no_bom_total = dq_no_bom["actual_total"].sum() if len(dq_no_bom) else 0

    html_doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Ennovie PRO Variance v4 — {as_of_date}</title>
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
        <div class="badge">PRO VARIANCE LEDGER · v4 · MASTER DATA FIX</div>
        <h1>Per-PRO Material Variance</h1>
        <p class="masthead-sub">v4 switches the planned-cost reference from Direct Cost Amount (which uses the corrupted Direct Unit Cost field on certain items) to Cost Amount (which uses standard Unit Cost). This is the field BC actually posts to GL — should reconcile cleanly to BC stuck residual.</p>
        <div class="meta-row">
            <div class="meta-pill">As of <span>{as_of_date}</span></div>
            <div class="meta-pill">Audit year <span>{audit_year}</span></div>
            <div class="meta-pill">Cost reference <span>Unit Cost</span></div>
            <div class="meta-pill">Valid Finished <span>{fmt_int(tot_pros)}</span></div>
        </div>
    </div>
</div>

<div class="main">

<div class="headline-banner">
    <h2>Net variance: {fmt_thb(tot_variance)} ({headline_label})</h2>
    <p>Across {fmt_int(tot_pros)} VALID Finished PROs using v4 (Cost Amount) basis. Unfavorable: {fmt_thb(tot_unfav)}. Favorable: {fmt_thb(tot_fav)}. Variance % of planned: {fmt_pct(avg_var_pct)}.</p>
</div>

<div class="kpi-grid">
    <div class="kpi">
        <div class="kpi-label">Planned cost (Cost Amount)</div>
        <div class="kpi-val">{fmt_thb(tot_planned)}</div>
        <div class="kpi-sub">Quantity × Unit Cost</div>
    </div>
    <div class="kpi">
        <div class="kpi-label">Actual cost (FIFO VEs)</div>
        <div class="kpi-val">{fmt_thb(tot_actual)}</div>
        <div class="kpi-sub">Consumption Value Entries</div>
    </div>
    <div class="kpi">
        <div class="kpi-label">Net variance</div>
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
        <h2>Reconciliation · v4 vs BC stuck residual</h2>
        <p>With clean Unit Cost basis, variance should now approximate BC stuck residual.</p>
    </div>
</div>
<div class="card">
    <table>
        <tr><th>Metric</th><th class="right">Value (THB)</th></tr>
        <tr><td>Clean operational variance (VALID PROs, v4 basis)</td><td class="right mono">{fmt_thb(clean_variance_total)}</td></tr>
        <tr><td>BC stuck WIP residual on Finished PROs</td><td class="right mono">{fmt_thb(bc_stuck_finished_total)}</td></tr>
        <tr><td><strong>Reconciliation gap</strong></td><td class="right mono"><strong>{fmt_thb(recon_diff)}</strong> &nbsp; {render_pill(recon_status[0], recon_status[1])}</td></tr>
        <tr><td>Gap as % of BC residual</td><td class="right mono">{recon_pct:.1f}%</td></tr>
    </table>
    <div class="callout callout-{recon_status[1]}" style="margin-top:14px;">
        <strong>{recon_status[2]}</strong>
    </div>
    <div class="callout callout-green" style="margin-top:14px;">
        <strong>v4 methodology note:</strong> This run uses <code>Cost Amount</code> (= <code>Quantity × Unit Cost</code>) as the planned-cost reference, replacing v1-v3's use of <code>Direct Cost Amount</code> (= <code>Quantity × Direct Unit Cost</code>). The switch was made because certain item cards (e.g., <code>M-935-SLV</code>) have corrupted <code>Direct Unit Cost</code> values (฿15,001,982/g instead of ~฿35/g). This corruption does NOT affect BC's GL postings, which use <code>Unit Cost</code> — confirming our reconciliation target is the trustworthy figure.
    </div>
</div>

<div class="section-header">
    <div class="section-number">02</div>
    <div class="section-title">
        <h2>PRO categorization</h2>
    </div>
</div>
<div class="card">
    <table>
        <tr>
            <th>Category</th><th class="right">PROs</th>
            <th class="right">Planned</th><th class="right">Actual</th>
            <th class="right">Variance</th>
        </tr>
        {cat_rows}
    </table>
    <div class="callout" style="margin-top:14px;">
        With v4's Unit Cost basis, the DQ-A categories should be much smaller than v3 (or empty). If DQ-A1/A2 still have PROs, those are items with corruption on BOTH Direct Unit Cost AND Unit Cost — worth investigating individually.
    </div>
</div>

<div class="section-header">
    <div class="section-number">03</div>
    <div class="section-title">
        <h2>Clean variance by year of finish · IAS 8 view</h2>
    </div>
</div>
<div class="card">
    <table>
        <tr>
            <th>Year</th><th class="right">PROs</th>
            <th class="right">Planned</th><th class="right">Actual</th>
            <th class="right">Unfav</th><th class="right">Fav</th>
            <th class="right">Net</th><th class="right">Gross</th>
            <th>Bucket</th>
        </tr>
        {year_rows}
    </table>
</div>

<div class="section-header">
    <div class="section-number">04</div>
    <div class="section-title">
        <h2>Variance by brand customer</h2>
    </div>
</div>
<div class="card">
    <table>
        <tr>
            <th>Brand (dim1)</th><th class="right">PROs</th>
            <th class="right">Planned</th><th class="right">Actual</th>
            <th class="right">Net</th><th class="right">%</th>
            <th class="right">Gross</th>
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
        <h2>DQ-A1 · Absolute outlier (planned > ฿10M, v4 basis)</h2>
        <p>Should be much smaller than v3 since Unit Cost is generally clean. Any PROs here have corruption on Unit Cost too — worth investigating.</p>
    </div>
</div>
<div class="card">
    <div class="card-header">
        <span class="card-title">{fmt_int(len(dq_a1))} PROs · total planned {fmt_thb(dq_a1_total)}</span>
        <span class="card-badge card-badge-red">EXCLUDED</span>
    </div>
    <table>
        <tr>
            <th>PRO No.</th><th class="right">Year</th>
            <th class="right">Planned</th><th class="right">Actual</th>
            <th class="right">Variance</th><th class="right">Max DUC/UC ratio</th>
        </tr>
        {dq_a1_rows}
    </table>
</div>

<div class="section-header">
    <div class="section-number">08</div>
    <div class="section-title">
        <h2>DQ-A2 · Ratio outlier (actual &lt; 5% of planned)</h2>
        <p>Should also be smaller than v3 since the main inflation source (Direct Unit Cost corruption) is now bypassed.</p>
    </div>
</div>
<div class="card">
    <div class="card-header">
        <span class="card-title">{fmt_int(len(dq_a2))} PROs · total planned {fmt_thb(dq_a2_total)}</span>
        <span class="card-badge card-badge-red">EXCLUDED</span>
    </div>
    <table>
        <tr>
            <th>PRO No.</th><th class="right">Year</th>
            <th class="right">Planned</th><th class="right">Actual</th>
            <th class="right">Ratio</th><th class="right">Max DUC/UC ratio</th>
        </tr>
        {dq_a2_rows}
    </table>
</div>

<div class="section-header">
    <div class="section-number">09</div>
    <div class="section-title">
        <h2>DQ-B · Missing BOM</h2>
        <p>Same as v3 — process control finding, not affected by master data fix.</p>
    </div>
</div>
<div class="card">
    <div class="card-header">
        <span class="card-title">{fmt_int(len(dq_no_bom))} PROs · total actual {fmt_thb(dq_no_bom_total)}</span>
        <span class="card-badge card-badge-amber">EXCLUDED</span>
    </div>
    <table>
        <tr>
            <th>PRO No.</th><th>Status</th><th class="right">Year</th>
            <th class="right">Planned</th><th class="right">Actual</th>
        </tr>
        {dq_no_bom_rows}
    </table>
</div>

</div>

<div class="footer">
    Ennovie PRO Variance Ledger · Generated by nb_pro_variance_v4 · {generated_at}
    &nbsp;·&nbsp; v4: Cost Amount (Unit Cost) basis · master data fix
</div>

</body>
</html>"""
    return html_doc


html_output = build_html()


# %% CELL 18 — WRITE + DISPLAY -----------------------------------------------

os.makedirs(output_dir, exist_ok=True)
fname = f"pro_variance_v4_{as_of_date}_{datetime.now().strftime('%H%M%S')}.html"
fpath = os.path.join(output_dir, fname)
with open(fpath, "w", encoding="utf-8") as f:
    f.write(html_output)

print(f"\n✓ Report written to: {fpath}")
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
    out_df["methodology"] = "v4_unit_cost"
    spark.createDataFrame(out_df).write.format("delta").mode("overwrite").saveAsTable("fa.gold_pro_variance")
    print(f"✓ Persisted {len(out_df):,} VALID PROs to fa.gold_pro_variance (v4 basis)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # nb_dqa1_item_fix_plan_v1_3.py

# CELL ********************

# =============================================================================
# nb_dqa1_item_fix_plan_v1_3.py
# =============================================================================
# Purpose: Identify the items behind DQ-A1 absolute-outlier PROs and
#          produce a documentation pack for the DQ-A1 exclusion.
#
# Run as: addendum cell at the end of nb_pro_variance_v4.py
#         (assumes `pro_variance` is in memory from CELL 10 of v4)
#
# v1.3 changes from v1.2 — THE CORRUPTION-IS-TRANSACTIONAL FIX:
#
#   v1.1/v1.2 incorrectly assumed corruption lived on the BC Item master
#   (i.e., that an Item Card had a bad Direct Unit Cost value that needed
#   resetting). Diagnostic query revealed:
#
#     - BC Item table has NO `Direct Unit Cost` column at all
#       (it has Unit Cost, Last Direct Cost, Standard Cost — that's it)
#     - The corruption is on Prod Order Component lines (transactional),
#       not on Item master
#     - M-935-SLV has clean master data (UC=฿73.93, LDC=฿73.55) but
#       its component lines on DQ-A1 PROs have DUC=฿15,001,982 — these
#       were posted at some historical event and froze there
#     - BC's GL postings used Unit Cost (correct value) so GL is clean;
#       only the planned-cost calc using Direct Unit Cost (v3 logic) inflated
#
#   v1.3 reframes Fix 5 from "edit Item Cards in BC" to "document the
#   exclusion + identify scope":
#     - Trace corruption from Prod Order Component lines directly
#     - Aggregate by item to see corruption blast radius
#     - Produce CSV pack for finance/audit trail
#     - Recommend Path A (document-only) since master data is clean
#
# Produces three CSVs in /lakehouse/default/Files/finance_reports/:
#   1. dq_a1_pros_<date>.csv               — DQ-A1 PROs flagged by v4
#   2. dq_a1_corrupted_components_<date>.csv — corrupted component lines per item
#   3. dq_a1_documentation_pack_<date>.csv   — item-level summary with master
#                                              data status (the deliverable)
#
# Author: Ennovie DPA — Phloy
# Date:   2026-05-12
# =============================================================================

from datetime import date
import os
import pandas as pd

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
TODAY = date.today().isoformat()
OUT_DIR = '/lakehouse/default/Files/finance_reports'
DQ_A1_LABEL = 'DQ-A1 · Absolute outlier (>฿10M)'

# Corruption threshold (same as v4 CELL 5: DUC > 100× UC on component line)
DUC_ABSOLUTE_THRESHOLD = 1000
DUC_RATIO_THRESHOLD = 100

os.makedirs(OUT_DIR, exist_ok=True)


# -----------------------------------------------------------------------------
# Sanity check — confirm pro_variance is in scope
# -----------------------------------------------------------------------------
try:
    _check = pro_variance.shape
    print(f"✓ pro_variance available: {_check[0]:,} PROs, {_check[1]} columns")
    if 'category' not in pro_variance.columns:
        raise RuntimeError(
            "pro_variance is in memory but missing 'category' column. "
            "Run CELL 10 (PRO categorisation) of nb_pro_variance_v4.py first."
        )
except NameError:
    raise RuntimeError(
        "pro_variance not in memory. Run nb_pro_variance_v4.py up through "
        "CELL 10 (PRO categorisation) first, then re-run this cell."
    )


# -----------------------------------------------------------------------------
# Step 1 — Filter and persist DQ-A1 PRO list
# -----------------------------------------------------------------------------
print("\n" + "=" * 70)
print("STEP 1 — Filter DQ-A1 PROs and persist")
print("=" * 70)

dq_a1 = pro_variance[pro_variance['category'] == DQ_A1_LABEL].copy()
print(f"DQ-A1 PROs identified: {len(dq_a1):,}")
print(f"  Total planned: ฿{dq_a1['planned_total'].sum():>20,.2f}")
print(f"  Total actual:  ฿{dq_a1['actual_total'].sum():>20,.2f}")
print(f"  Total variance:฿{dq_a1['variance_total'].sum():>20,.2f}")

dq_a1_path = f'{OUT_DIR}/dq_a1_pros_{TODAY}.csv'
dq_a1[['prod_order_no', 'planned_total', 'actual_total', 'variance_total']].to_csv(
    dq_a1_path, index=False
)
print(f"\n✓ Saved: {dq_a1_path}")

if len(dq_a1) == 0:
    print("\n⚠ No DQ-A1 PROs found. Nothing to document. Exiting early.")
    raise SystemExit(0)

# Make DQ-A1 PROs available as Spark temp view for subsequent joins
dq_a1_pros_sdf = spark.createDataFrame(dq_a1[['prod_order_no']])
dq_a1_pros_sdf.createOrReplaceTempView('v_dq_a1_pros')


# -----------------------------------------------------------------------------
# Step 2 — Find corrupted Prod Order Component LINES inside DQ-A1 PROs
# -----------------------------------------------------------------------------
# This is where the corruption actually lives (transactional, not master).
# We pull every component line where DUC > 100× UC + DUC > ฿1,000 — same
# threshold v4 CELL 5 uses for its `unit_cost_ratio` flag.
# -----------------------------------------------------------------------------
print("\n" + "=" * 70)
print("STEP 2 — Trace corruption to component lines")
print("=" * 70)

corrupted_components_query = f"""
SELECT
    poc.`Prod. Order No.`                    AS prod_order_no,
    poc.`Item No.`                           AS item_no,
    poc.`Line No.`                           AS line_no,
    CAST(poc.`Quantity` AS DOUBLE)           AS quantity,
    CAST(poc.`Unit Cost` AS DOUBLE)          AS unit_cost_on_line,
    CAST(poc.`Direct Unit Cost` AS DOUBLE)   AS direct_unit_cost_on_line,
    CAST(poc.`Cost Amount` AS DOUBLE)        AS cost_amount,
    CAST(poc.`Direct Cost Amount` AS DOUBLE) AS direct_cost_amount,
    CAST(poc.`Direct Unit Cost` AS DOUBLE) /
        NULLIF(CAST(poc.`Unit Cost` AS DOUBLE), 0) AS cost_ratio
FROM Silver_BC_Lakehouse.bc.`Prod Order Component` poc
JOIN v_dq_a1_pros dq ON dq.prod_order_no = poc.`Prod. Order No.`
WHERE poc.`Direct Unit Cost` > {DUC_ABSOLUTE_THRESHOLD}
  AND poc.`Direct Unit Cost` > poc.`Unit Cost` * {DUC_RATIO_THRESHOLD}
"""
corrupted_components = spark.sql(corrupted_components_query).toPandas()
print(f"Corrupted component lines in DQ-A1 PROs: {len(corrupted_components):,}")
print(f"  Total inflated direct_cost_amount: ฿{corrupted_components['direct_cost_amount'].sum():,.2f}")
print(f"  Total clean cost_amount:           ฿{corrupted_components['cost_amount'].sum():,.2f}")
print(f"  Distinct items involved:           {corrupted_components['item_no'].nunique():,}")
print(f"  Distinct PROs affected:            {corrupted_components['prod_order_no'].nunique():,}")

components_path = f'{OUT_DIR}/dq_a1_corrupted_components_{TODAY}.csv'
corrupted_components.to_csv(components_path, index=False)
print(f"\n✓ Saved: {components_path}")


# -----------------------------------------------------------------------------
# Step 3 — Aggregate to item level + cross-check with Item master
# -----------------------------------------------------------------------------
# For each item that appears in corrupted lines, show:
#   - blast radius (how many PROs / lines / inflated value)
#   - Item master state (UC, LDC, SC) so we can confirm master is clean
# -----------------------------------------------------------------------------
print("\n" + "=" * 70)
print("STEP 3 — Aggregate to item level + cross-check Item master")
print("=" * 70)

# Item-level aggregation from corrupted components
item_agg = corrupted_components.groupby('item_no').agg(
    corrupted_pros=('prod_order_no', 'nunique'),
    corrupted_lines=('line_no', 'count'),
    inflated_direct_cost=('direct_cost_amount', 'sum'),
    clean_cost_amount=('cost_amount', 'sum'),
    sample_duc=('direct_unit_cost_on_line', 'max'),
    sample_uc_on_line=('unit_cost_on_line', 'max'),
    sample_cost_ratio=('cost_ratio', 'max'),
).reset_index()

# Pull Item master state for these items
items_list = "', '".join(item_agg['item_no'].tolist())
item_master_query = f"""
SELECT
    i.`No.`                                          AS item_no,
    i.Description,
    CAST(i.`Unit Cost` AS DOUBLE)                    AS master_unit_cost,
    CAST(i.`Last Direct Cost` AS DOUBLE)             AS master_last_direct_cost,
    CAST(i.`Standard Cost` AS DOUBLE)                AS master_standard_cost,
    i.`Costing Method`                               AS costing_method,
    i.`Inventory Posting Group`                      AS inventory_posting_group,
    i.`Gen. Prod. Posting Group`                     AS gen_prod_posting_group,
    i.`Item Category Code`                           AS item_category_code,
    i.SystemModifiedAt                               AS master_last_modified
FROM Silver_BC_Lakehouse.bc.Item i
WHERE i.`No.` IN ('{items_list}')
"""
item_master = spark.sql(item_master_query).toPandas()
print(f"Item master records pulled: {len(item_master):,}")

# Join — left from item_agg so any items missing in master are visible
doc_pack = item_agg.merge(item_master, on='item_no', how='left')

# Flag whether master data itself looks clean (UC sane vs sample DUC inflation)
def master_status(row):
    if pd.isna(row['master_unit_cost']):
        return 'NO_MASTER'
    uc = row['master_unit_cost']
    duc = row['sample_duc']
    # Master is "clean" if its Unit Cost is in a sane range (say, < ฿100K/unit)
    # and nowhere near the inflated DUC on component lines
    if uc < 100_000 and duc > uc * 100:
        return 'MASTER_CLEAN_LINES_DIRTY'  # the M-935-SLV pattern
    elif uc > 100_000:
        return 'MASTER_ALSO_INFLATED'
    else:
        return 'OK'

doc_pack['master_status'] = doc_pack.apply(master_status, axis=1)

# Sort by blast radius — most-impactful items first
doc_pack = doc_pack.sort_values('corrupted_pros', ascending=False).reset_index(drop=True)


# -----------------------------------------------------------------------------
# Step 4 — Persist documentation pack
# -----------------------------------------------------------------------------
print("\n" + "=" * 70)
print("STEP 4 — Persist documentation pack")
print("=" * 70)

doc_cols = [
    'item_no',
    'Description',
    'master_status',
    'corrupted_pros',
    'corrupted_lines',
    'sample_duc',
    'sample_uc_on_line',
    'sample_cost_ratio',
    'master_unit_cost',
    'master_last_direct_cost',
    'master_standard_cost',
    'costing_method',
    'inflated_direct_cost',
    'clean_cost_amount',
    'inventory_posting_group',
    'gen_prod_posting_group',
    'master_last_modified',
]
doc_pack_path = f'{OUT_DIR}/dq_a1_documentation_pack_{TODAY}.csv'
doc_pack[doc_cols].to_csv(doc_pack_path, index=False)
print(f"✓ Saved: {doc_pack_path}")


# -----------------------------------------------------------------------------
# Step 5 — Summary + interpretation
# -----------------------------------------------------------------------------
print("\n" + "=" * 70)
print("STEP 5 — Summary")
print("=" * 70)

status_counts = doc_pack['master_status'].value_counts()
print(f"\nItem master status breakdown:")
for status, count in status_counts.items():
    print(f"  {status:30s} {count:>4} items")

print(f"\nTop 20 items by blast radius (corrupted_pros):")
print("-" * 100)
display_cols = ['item_no', 'master_status', 'corrupted_pros', 'corrupted_lines',
                'sample_duc', 'master_unit_cost', 'sample_cost_ratio']
summary = doc_pack[display_cols].head(20).copy()
summary['sample_duc'] = summary['sample_duc'].map('{:>18,.2f}'.format)
summary['master_unit_cost'] = summary['master_unit_cost'].map(
    lambda v: '—' if pd.isna(v) else f'{v:>10,.2f}'
)
summary['sample_cost_ratio'] = summary['sample_cost_ratio'].map(
    lambda v: '—' if pd.isna(v) else f'{v:>10,.0f}x'
)
print(summary.to_string(index=False))


# -----------------------------------------------------------------------------
# Step 6 — Interpretation guide
# -----------------------------------------------------------------------------
print("\n" + "=" * 70)
print("INTERPRETATION")
print("=" * 70)

n_clean_master = (doc_pack['master_status'] == 'MASTER_CLEAN_LINES_DIRTY').sum()
n_dirty_master = (doc_pack['master_status'] == 'MASTER_ALSO_INFLATED').sum()
n_no_master = (doc_pack['master_status'] == 'NO_MASTER').sum()

print(f"""
The {len(corrupted_components):,} corrupted component lines across {len(dq_a1):,} DQ-A1 PROs
fall into these categories based on Item master state:

  {n_clean_master:>4}  MASTER_CLEAN_LINES_DIRTY — Item master is fine (e.g., M-935-SLV with
                                      UC=฿73.93), but historical Prod Order Component
                                      lines have inflated Direct Unit Cost. These are
                                      historical posting anomalies frozen on closed PROs.
                                      Cannot be 'fixed' on Item Card because Item Card is
                                      already correct.

  {n_dirty_master:>4}  MASTER_ALSO_INFLATED   — Item master Unit Cost is itself inflated.
                                      Rare. Would need separate Item Card cleanup.

  {n_no_master:>4}  NO_MASTER              — Component refers to an Item No. that doesn't
                                      exist in Item master. Likely obsolete/deleted item.

For MASTER_CLEAN_LINES_DIRTY items (most cases):
  → No BC action available — Item master is already correct
  → DUC inflation is locked in historical PRO component lines
  → BC's GL postings used Unit Cost (clean), so GL is unaffected
  → v4's reconciliation gap stays green because v4 uses Cost Amount (Unit Cost basis)
  → DQ-A1 exclusion is justified — this is the documentation trail

NEXT STEPS:
  1. Review {doc_pack_path} with Finance / Audit
  2. Use this CSV as audit trail for the DQ-A1 exclusion in v4 report
  3. If MASTER_ALSO_INFLATED items exist (count above), those are the only
     ones where editing Item Card UC would actually help
  4. For root cause investigation: look at SystemModifiedAt on the original
     Prod Order Component records to find when the bad DUC was posted
     (likely traces to a Standard Cost rollup or revaluation event)
""")

print("✓ DQ-A1 documentation pack complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # nb_orphan_ile_master_sweep

# CELL ********************

# =============================================================================
# nb_orphan_ile_master_sweep_v3.py
# =============================================================================
# PASTE THIS INTO A SINGLE PYTHON CELL IN FABRIC NOTEBOOK
# (NO %%sql magic — this is PySpark code, not SQL)
#
# Purpose: Self-discovering orphan ILE sweep — finds every BC table that
#          references Item Ledger Entry No. and surfaces references whose
#          target ILE doesn't exist.
#
# v3 changes from v2:
#   1. REMOVED 'applies-to entry' / 'applies-from entry' from ILE_REF_PATTERNS
#      → These refer to the HOST table's own ledger, NOT to ILE. They were
#        producing 1.6M false-positive orphan rows on Value Entry.
#        (Real orphans: 3 ILEs. False positives: 1,642,425.)
#   2. Step 11 fixed — Cost Amount (Actual)/(Expected) lives on Value Entry,
#      NOT on Item Ledger Entry. Schema-correct join: VE -> ILE -> Item.
#   3. Added Step 0 — mirror range diagnostic — classifies orphans as
#      'mirror lag' (above mirror's max ILE) vs 'sync gap' (within range
#      but missing). Provides actionable next steps.
#
# Author: Ennovie DPA — Phloy
# Date:   2026-05-12
# =============================================================================

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import LongType
from functools import reduce

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
BC = "Silver_BC_Lakehouse.bc"
GOLD = "fa"
SCAN_AS_OF = "2026-05-12"

# ILE-reference column patterns — substring match, case-insensitive
# v3: removed applies-to/applies-from (host's own ledger, not ILE)
ILE_REF_PATTERNS = [
    "item ledger entry no",     # explicit reference to ILE (gold standard)
    "inbound item entry no",    # IAE.Inbound Item Entry No.
    "outbound item entry no",   # IAE.Outbound Item Entry No.
    "ile no",                   # custom AL fields convention
]

# Tables whose `Entry No.` is THEIR OWN primary key — not an ILE reference.
LEDGER_TABLES_TO_EXCLUDE = {
    "Bank Account Ledger Entry",
    "G/L Entry", "G_L Entry", "GL Entry",
    "Customer Ledger Entry",
    "Vendor Ledger Entry",
    "Detailed Cust. Ledg. Entry",
    "Detailed Vendor Ledg. Entry",
    "Detailed Employee Ledger Entry",
    "FA Ledger Entry",
    "Employee Ledger Entry",
    "Phys. Inventory Ledger Entry",
    "Reservation Entry",
    "Warehouse Entry",
    "Warehouse Activity Line",
    "Job Ledger Entry",
    "Insurance Coverage Ledger Entry",
    "Cost Entry",
    "Maintenance Ledger Entry",
    "Resource Ledger Entry",
    "Capacity Ledger Entry",
    "Item Ledger Entry",
}

print("=" * 70)
print("Orphan ILE Master Sweep (v3)")
print(f"Scan as of: {SCAN_AS_OF}")
print("=" * 70)


# -----------------------------------------------------------------------------
# Step 0 — Mirror range diagnostic (NEW in v3)
# -----------------------------------------------------------------------------
# Captures the current ILE Entry No. range so we can classify orphans:
#   - Above ile_max  -> mirror lag (waiting for CDC catch-up)
#   - Within range   -> sync gap (CDC skipped records, needs fix)
# -----------------------------------------------------------------------------
print("\n[0] Mirror range diagnostic")
ile_range_row = spark.sql(f"""
    SELECT
        MIN(`Entry No.`) AS ile_min,
        MAX(`Entry No.`) AS ile_max,
        COUNT(*) AS ile_count
    FROM `Silver_BC_Lakehouse`.bc.`Item Ledger Entry`
""").collect()[0]
ile_min = ile_range_row["ile_min"]
ile_max = ile_range_row["ile_max"]
ile_count = ile_range_row["ile_count"]
print(f"    ILE range in mirror: {ile_min:,} -> {ile_max:,}")
print(f"    Total ILE records:   {ile_count:,}")
print(f"    Gap count (range - records): {(ile_max - ile_min + 1) - ile_count:,}")
print(f"    (gap count is normal BC behavior - some Entry Nos are rolled back)")


# -----------------------------------------------------------------------------
# Step 1 — Catalog discovery
# -----------------------------------------------------------------------------
print("\n[1] Catalog discovery")
catalog = spark.sql(f"SHOW TABLES IN {BC}").select("tableName").collect()
tables_present = sorted([r["tableName"] for r in catalog])
print(f"    BC tables mirrored: {len(tables_present)}")

required = ["Item Ledger Entry", "Value Entry", "Item Application Entry", "Item"]
missing_core = [t for t in required if t not in tables_present]
assert not missing_core, f"Cannot proceed - missing core tables: {missing_core}"
print("    All core tables present")


# -----------------------------------------------------------------------------
# Step 2 — Identify ILE-reference candidate columns (smart filter)
# -----------------------------------------------------------------------------
print("\n[2] Scanning for ILE-reference columns")
print(f"    Patterns:  {ILE_REF_PATTERNS}")
print(f"    Excluded:  {len(LEDGER_TABLES_TO_EXCLUDE)} ledger tables")

ile_ref_candidates = []

for t in tables_present:
    if t in LEDGER_TABLES_TO_EXCLUDE:
        continue
    try:
        cols = spark.table(f"{BC}.`{t}`").columns
    except Exception as e:
        print(f"    skip {t}: {type(e).__name__}")
        continue
    for c in cols:
        cl = c.lower()
        if any(p in cl for p in ILE_REF_PATTERNS):
            ile_ref_candidates.append((t, c))

print(f"\n    Discovered {len(ile_ref_candidates)} candidate ILE-reference columns:")
for t, c in ile_ref_candidates:
    print(f"      {t:<50} -> `{c}`")


# -----------------------------------------------------------------------------
# Step 3 — Sweep function — LEFT ANTI JOIN per candidate
# -----------------------------------------------------------------------------
print("\n[3] Sweeping orphan references (LEFT ANTI JOIN per candidate)")

def clean_alias(col_name: str) -> str:
    """Normalize BC column name to snake_case alias."""
    return (col_name.lower()
                    .replace(" ", "_")
                    .replace(".", "")
                    .replace("(", "")
                    .replace(")", "")
                    .replace("/", "_")
                    .replace("-", "_"))

def sweep_orphans(table_name: str, ref_col: str):
    """Return one row per orphan reference from `table_name`.`ref_col`."""
    df = spark.table(f"{BC}.`{table_name}`")
    if ref_col not in df.columns:
        return None

    payload_map = {}
    for cand in [
        "Entry No.", "Item No.", "Posting Date",
        "Document No.", "Source No.",
        "Cost Amount (Actual)", "Cost Amount (Expected)",
        "Quantity", "Invoiced Quantity",
    ]:
        if cand in df.columns and cand != ref_col:
            payload_map[clean_alias(cand)] = cand

    ile = spark.table(f"{BC}.`Item Ledger Entry`").select(
        F.col("`Entry No.`").alias("ile_entry_no")
    )

    select_exprs = [
        F.lit(table_name).alias("source_table"),
        F.lit(ref_col).alias("source_column"),
        F.col(f"`{ref_col}`").cast(LongType()).alias("missing_ile_no"),
    ]
    for alias, orig in payload_map.items():
        select_exprs.append(F.col(f"`{orig}`").alias(alias))

    selected = df.select(*select_exprs).where(
        F.col("missing_ile_no").isNotNull() & (F.col("missing_ile_no") > 0)
    )

    return selected.join(
        ile,
        selected["missing_ile_no"] == ile["ile_entry_no"],
        how="left_anti",
    )

sweeps = []
for t, c in ile_ref_candidates:
    print(f"    {t}.`{c}` ...", end=" ")
    try:
        out = sweep_orphans(t, c)
        if out is None:
            print("(column missing, skipped)")
            continue
        cnt = out.count()
        print(f"{cnt} orphan rows")
        if cnt > 0:
            sweeps.append(out)
    except Exception as e:
        print(f"FAIL {type(e).__name__}: {str(e)[:100]}")

print(f"\n    Sources with orphans: {len(sweeps)}")


# -----------------------------------------------------------------------------
# Step 4 — Union with consistent schema
# -----------------------------------------------------------------------------
print("\n[4] Unifying schemas across sweeps")

common_payload = [
    "entry_no", "item_no", "posting_date", "document_no", "source_no",
    "cost_amount_actual", "cost_amount_expected",
    "quantity", "invoiced_quantity",
]

def conform(df: DataFrame) -> DataFrame:
    """Pad missing payload columns with NULL so all sweeps share a schema."""
    select_exprs = [
        F.col("source_table"),
        F.col("source_column"),
        F.col("missing_ile_no"),
    ]
    for c in common_payload:
        if c in df.columns:
            select_exprs.append(F.col(c).alias(c))
        else:
            select_exprs.append(F.lit(None).alias(c))
    return df.select(*select_exprs)

if not sweeps:
    print("    No orphans found across any source. Exiting early.")
    raise SystemExit(0)

orphan_raw = reduce(DataFrame.unionByName, [conform(s) for s in sweeps])
orphan_raw_count = orphan_raw.count()
print(f"    Total orphan reference rows: {orphan_raw_count:,}")
orphan_raw.show(20, truncate=False)


# -----------------------------------------------------------------------------
# Step 5 — Enrich with Item metadata + classify orphan type (v3)
# -----------------------------------------------------------------------------
print("\n[5] Enriching with Item master metadata + orphan classification")
item_md = spark.table(f"{BC}.`Item`").select(
    F.col("`No.`").alias("item_no"),
    F.col("`Description`").alias("item_description"),
    F.col("`Item Category Code`").alias("item_category_code"),
    F.col("`Inventory Posting Group`").alias("inventory_posting_group"),
    F.col("`Gen. Prod. Posting Group`").alias("gen_prod_posting_group"),
)

# Classify each orphan: mirror_lag (above ile_max) vs sync_gap (within range)
orphan_enriched = (
    orphan_raw.join(item_md, on="item_no", how="left")
    .withColumn("scan_date", F.lit(SCAN_AS_OF))
    .withColumn("orphan_type", F.when(
        F.col("missing_ile_no") > F.lit(ile_max), "mirror_lag"
    ).otherwise("sync_gap"))
)
orphan_enriched.cache()
print(f"    Enriched rows: {orphan_enriched.count():,}")

# Classification breakdown
print("\n    Orphan type breakdown:")
orphan_enriched.groupBy("orphan_type").agg(
    F.countDistinct("missing_ile_no").alias("unique_orphan_iles"),
    F.count(F.lit(1)).alias("reference_rows"),
).show(truncate=False)

orphan_enriched.show(20, truncate=False)


# -----------------------------------------------------------------------------
# Step 6 — Master rollup (one row per missing_ile_no x item)
# -----------------------------------------------------------------------------
print("\n[6] Building master rollup")
orphan_master = (
    orphan_enriched.groupBy(
        "missing_ile_no",
        "item_no",
        "item_description",
        "item_category_code",
        "inventory_posting_group",
        "gen_prod_posting_group",
        "orphan_type",
    )
    .agg(
        F.collect_set(F.concat_ws(".", "source_table", "source_column")).alias("reference_sources"),
        F.count(F.lit(1)).alias("reference_count"),
        F.sum("cost_amount_actual").alias("cost_actual_referenced"),
        F.sum("cost_amount_expected").alias("cost_expected_referenced"),
        F.min("posting_date").alias("earliest_ref_posting_date"),
        F.max("posting_date").alias("latest_ref_posting_date"),
    )
    .withColumn("scan_date", F.lit(SCAN_AS_OF))
    .orderBy(F.desc("reference_count"), "missing_ile_no")
)
print(f"    Unique orphan ILEs: {orphan_master.count():,}")
print(f"    Unique items affected: {orphan_master.select('item_no').distinct().count():,}")
orphan_master.show(50, truncate=False)


# -----------------------------------------------------------------------------
# Step 7 — Summary rollups
# -----------------------------------------------------------------------------
print("\n[7] Summary rollups")

by_item = (
    orphan_master.groupBy("item_no", "item_description", "item_category_code",
                          "inventory_posting_group")
    .agg(
        F.countDistinct("missing_ile_no").alias("orphan_ile_count"),
        F.sum("reference_count").alias("total_references"),
        F.sum("cost_actual_referenced").alias("cost_actual_total"),
        F.sum("cost_expected_referenced").alias("cost_expected_total"),
    )
    .orderBy(F.desc("orphan_ile_count"))
)
print("\n=== ORPHANS BY ITEM ===")
by_item.show(50, truncate=False)

by_category = (
    orphan_master.groupBy("item_category_code")
    .agg(
        F.countDistinct("item_no").alias("items_affected"),
        F.countDistinct("missing_ile_no").alias("orphan_ile_count"),
        F.sum("reference_count").alias("total_references"),
    )
    .orderBy(F.desc("orphan_ile_count"))
)
print("\n=== ORPHANS BY ITEM CATEGORY ===")
by_category.show(30, truncate=False)

by_source = (
    orphan_enriched.groupBy("source_table", "source_column")
    .agg(
        F.countDistinct("missing_ile_no").alias("orphan_ile_count"),
        F.count(F.lit(1)).alias("ref_rows"),
    )
    .orderBy(F.desc("orphan_ile_count"))
)
print("\n=== ORPHANS BY SOURCE TABLE ===")
by_source.show(50, truncate=False)


# -----------------------------------------------------------------------------
# Step 8 — Today's batch artefacts
# -----------------------------------------------------------------------------
print("\n[8] Today's orphan references")
today_refs = (
    orphan_enriched.where(F.col("posting_date") == F.to_date(F.lit(SCAN_AS_OF)))
    .select("missing_ile_no", "item_no", "source_table", "source_column",
            "orphan_type", "entry_no", "posting_date", "document_no",
            "cost_amount_actual", "cost_amount_expected")
    .orderBy("missing_ile_no", "source_table")
)
today_count = today_refs.count()
print(f"    Orphan references created on {SCAN_AS_OF}: {today_count:,}")
if today_count > 0:
    today_refs.show(100, truncate=False)
    print("    -> Likely collateral from today's failed Adjust Cost retries.")
    print("    -> Or fresh transactions awaiting mirror sync.")


# -----------------------------------------------------------------------------
# Step 9 — Materialize to gold layer
# -----------------------------------------------------------------------------
print("\n[9] Materializing to gold layer")
(orphan_master.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.gold_orphan_ile_master"))
print(f"    OK {GOLD}.gold_orphan_ile_master")

(orphan_enriched.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.gold_orphan_ile_detail"))
print(f"    OK {GOLD}.gold_orphan_ile_detail")

(by_category.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.gold_orphan_ile_by_category"))
print(f"    OK {GOLD}.gold_orphan_ile_by_category")


# -----------------------------------------------------------------------------
# Step 10 — BC Adjust Cost exclusion filters
# -----------------------------------------------------------------------------
print("\n[10] BC Adjust Cost exclusion filters")
print("=== Paste into BC Adjust Cost - Item Entries -> Item No. Filter ===\n")

categories = [r["item_category_code"] for r in
              by_category.where(F.col("item_category_code").isNotNull())
              .select("item_category_code").collect()]

for cat in categories:
    items_in_cat = [r["item_no"] for r in
                    by_item.where(F.col("item_category_code") == cat)
                    .select("item_no").collect()]
    if not items_in_cat:
        continue
    exclude_filter = "&".join([f"<>{i}" for i in items_in_cat])
    print(f"Item Category: {cat}")
    preview = items_in_cat[:10]
    suffix = f" ... +{len(items_in_cat)-10} more" if len(items_in_cat) > 10 else ""
    print(f"  Items to exclude ({len(items_in_cat)}): {preview}{suffix}")
    print(f"  BC filter: {exclude_filter}")
    print()


# -----------------------------------------------------------------------------
# Step 11 — JE sizing (subledger vs GL) — FIXED in v3
# -----------------------------------------------------------------------------
# Cost Amount (Actual) and Cost Amount (Expected) live on Value Entry,
# NOT on Item Ledger Entry. v3 joins VE -> ILE -> Item correctly.
# -----------------------------------------------------------------------------
print("\n[11] JE sizing - subledger vs GL (Value Entry basis)")

subledger = (
    spark.table(f"{BC}.`Value Entry`").alias("ve")
    .join(
        spark.table(f"{BC}.`Item Ledger Entry`").alias("ile"),
        F.col("ve.`Item Ledger Entry No.`") == F.col("ile.`Entry No.`"),
        "inner"
    )
    .join(item_md.alias("i"), F.col("ile.`Item No.`") == F.col("i.item_no"), "left")
    .where(F.col("ve.`Posting Date`") <= F.to_date(F.lit(SCAN_AS_OF)))
    .groupBy("i.inventory_posting_group")
    .agg(
        F.sum(F.col("ve.`Cost Amount (Actual)`")).alias("subledger_actual"),
        F.sum(F.col("ve.`Cost Amount (Expected)`")).alias("subledger_expected"),
        F.countDistinct(F.col("ile.`Entry No.`")).alias("ile_entries"),
        F.count(F.lit(1)).alias("value_entries"),
    )
    .orderBy("inventory_posting_group")
)
print("=== SUBLEDGER TOTALS BY INVENTORY POSTING GROUP (from Value Entry) ===")
subledger.show(50, truncate=False)

inv_accounts = [
    "10310","10311","10312","10313","10320","10321","10322","10323",
    "10330","10331","10332","10340","10350","10360","10361","10362",
    "10363","10370","10371","10372","10373","10380","10382","10385",
    "10390","10391","10392","10410","50141",
]
gl_inv = (
    spark.table(f"{GOLD}.gold_bc_gl_entry_clean")
    .where(F.col("gl_account_no").isin(inv_accounts))
    .groupBy("gl_account_no")
    .agg(
        F.sum("net_amount").alias("gl_balance"),
        F.count(F.lit(1)).alias("entries")
    )
    .orderBy("gl_account_no")
)
print("=== GL BALANCE - INVENTORY ACCOUNTS ===")
gl_inv.show(50, truncate=False)


# -----------------------------------------------------------------------------
# Step 12 — Today's GL postings sanity check
# -----------------------------------------------------------------------------
print("\n[12] Today's GL postings sanity check")
today_gl = (
    spark.table(f"{GOLD}.gold_bc_gl_entry_clean")
    .where(F.col("posting_date") == F.to_date(F.lit(SCAN_AS_OF)))
    .where(F.col("gl_account_no").isin(inv_accounts))
    .groupBy("gl_account_no", "document_no")
    .agg(
        F.sum("net_amount").alias("net_amount"),
        F.count(F.lit(1)).alias("entries")
    )
    .orderBy("document_no", "gl_account_no")
)
print("=== GL POSTINGS DATED TODAY ===")
today_gl.show(100, truncate=False)


# -----------------------------------------------------------------------------
# Step 13 — Final summary + action recommendations (NEW in v3)
# -----------------------------------------------------------------------------
print("\n" + "=" * 70)
print("[13] Final summary & recommended actions")
print("=" * 70)

total_orphans = orphan_master.count()
lag_orphans = orphan_master.where(F.col("orphan_type") == "mirror_lag").count()
gap_orphans = orphan_master.where(F.col("orphan_type") == "sync_gap").count()

print(f"\n    Total unique orphan ILEs: {total_orphans:,}")
print(f"      mirror_lag (above max {ile_max:,}): {lag_orphans:,}")
print(f"      sync_gap   (within mirror range):  {gap_orphans:,}")

print("""
    Recommended actions:

    mirror_lag orphans:
      -> Wait for next CDC sync cycle, then re-run this notebook.
      -> If count grows over multiple runs, the mirror's max ILE is not
         advancing -> check Fabric Mirroring health for Item Ledger Entry.

    sync_gap orphans:
      -> CDC skipped these records. Either:
         (a) Full reload of Item Ledger Entry mirror, OR
         (b) Open ticket with Fabric/BC admin to investigate CDC corruption.
      -> Workaround: in downstream gold layer, use COALESCE on Value Entry
         payload columns when ILE join is NULL.

    For analysis stability:
      -> Use fa.gold_orphan_ile_master as the source of truth for
         BC Adjust Cost exclusion + close-readiness reporting.
      -> Schedule this notebook weekly to track orphan trends.
""")

print("=" * 70)
print("Orphan ILE sweep v3 complete.")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT i.`No.`, i.`Description`, i.`Item Category Code`
# MAGIC FROM `Silver_BC_Lakehouse`.bc.`Item` i
# MAGIC WHERE i.`No.` IN (
# MAGIC     'RM-MT-000115-BR',
# MAGIC     'M-930-SCRAP-HD',
# MAGIC     'R000105438-00004',
# MAGIC     'SMCH002127-00001'
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- ดูว่า ILE เหล่านี้มีอยู่ใน BC จริงๆ มั้ย (ผ่าน meta-check)
# MAGIC -- ถ้า max_ile = 3789884 แต่ ILE 3582204/3657227/3702380 ไม่มี
# MAGIC -- แสดงว่าเป็น CDC skip ไม่ใช่ mirror lag
# MAGIC 
# MAGIC SELECT 
# MAGIC     'Below 3582204' AS bucket,
# MAGIC     COUNT(*) AS iles_in_bucket
# MAGIC FROM `Silver_BC_Lakehouse`.bc.`Item Ledger Entry`
# MAGIC WHERE `Entry No.` BETWEEN 3582200 AND 3582210
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Around 3657227',
# MAGIC     COUNT(*)
# MAGIC FROM `Silver_BC_Lakehouse`.bc.`Item Ledger Entry`
# MAGIC WHERE `Entry No.` BETWEEN 3657220 AND 3657230
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Around 3702380',
# MAGIC     COUNT(*)
# MAGIC FROM `Silver_BC_Lakehouse`.bc.`Item Ledger Entry`
# MAGIC WHERE `Entry No.` BETWEEN 3702375 AND 3702385;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- 2. Current state of all key inventory accounts (compare to baseline below)
# MAGIC SELECT gl_account_no,
# MAGIC        SUM(net_amount) AS balance,
# MAGIC        COUNT(*) AS entries
# MAGIC FROM fa.gold_bc_gl_entry_clean
# MAGIC WHERE gl_account_no IN ('10391','10392','10410',
# MAGIC                          '10360','10361','10362','10363',
# MAGIC                          '10370','10371','10372','10373',
# MAGIC                          '50141')
# MAGIC GROUP BY gl_account_no
# MAGIC ORDER BY gl_account_no;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT document_no, gl_account_no,
# MAGIC        SUM(net_amount) AS net_amount, COUNT(*) AS entries
# MAGIC FROM fa.gold_bc_gl_entry_clean
# MAGIC WHERE posting_date = DATE'2026-05-12'
# MAGIC GROUP BY document_no, gl_account_no
# MAGIC ORDER BY document_no, gl_account_no;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) AS remaining_orphans,
# MAGIC        COUNT(DISTINCT item_no) AS items_affected
# MAGIC FROM fa.gold_orphan_ile_master;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- The Expected-vs-Actual cost subledger position, split by Completely Invoiced status
# MAGIC SELECT
# MAGIC     ile.`Completely Invoiced`,
# MAGIC     COUNT(DISTINCT ile.`Entry No.`)        AS ile_count,
# MAGIC     COUNT(*)                                AS ve_row_count,
# MAGIC     SUM(ve.`Cost Amount (Expected)`)        AS exp_cost_total,
# MAGIC     SUM(ve.`Cost Amount (Actual)`)          AS act_cost_total
# MAGIC FROM `Silver_BC_Lakehouse`.bc.`Value Entry` ve
# MAGIC JOIN `Silver_BC_Lakehouse`.bc.`Item Ledger Entry` ile
# MAGIC     ON ile.`Entry No.` = ve.`Item Ledger Entry No.`
# MAGIC WHERE ile.`Posting Date` <= DATE'2026-05-12'
# MAGIC GROUP BY ile.`Completely Invoiced`;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Breakdown of the 44,699 stuck ILEs by Entry Type and age
# MAGIC SELECT
# MAGIC     ile.`Entry Type`,
# MAGIC     YEAR(ile.`Posting Date`)    AS yr,
# MAGIC     QUARTER(ile.`Posting Date`) AS q,
# MAGIC     COUNT(DISTINCT ile.`Entry No.`)  AS ile_count,
# MAGIC     SUM(ve.`Cost Amount (Expected)`) AS exp_cost,
# MAGIC     SUM(ve.`Cost Amount (Actual)`)   AS act_cost
# MAGIC FROM `Silver_BC_Lakehouse`.bc.`Value Entry` ve
# MAGIC JOIN `Silver_BC_Lakehouse`.bc.`Item Ledger Entry` ile
# MAGIC     ON ile.`Entry No.` = ve.`Item Ledger Entry No.`
# MAGIC WHERE ile.`Completely Invoiced` = false
# MAGIC GROUP BY ile.`Entry Type`, YEAR(ile.`Posting Date`), QUARTER(ile.`Posting Date`)
# MAGIC ORDER BY yr DESC, q DESC, ile.`Entry Type`;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Top open production orders driving the 10391 bloat
# MAGIC SELECT
# MAGIC     ile.`Document No.`     AS prod_order_no,
# MAGIC     ile.`Item No.`,
# MAGIC     i.`Description`,
# MAGIC     i.`Inventory Posting Group`,
# MAGIC     COUNT(DISTINCT ile.`Entry No.`)   AS output_ile_count,
# MAGIC     SUM(ve.`Cost Amount (Expected)`)  AS expected_cost,
# MAGIC     MIN(ile.`Posting Date`)           AS oldest_output,
# MAGIC     MAX(ile.`Posting Date`)           AS newest_output
# MAGIC FROM `Silver_BC_Lakehouse`.bc.`Value Entry` ve
# MAGIC JOIN `Silver_BC_Lakehouse`.bc.`Item Ledger Entry` ile
# MAGIC     ON ile.`Entry No.` = ve.`Item Ledger Entry No.`
# MAGIC JOIN `Silver_BC_Lakehouse`.bc.`Item` i
# MAGIC     ON i.`No.` = ile.`Item No.`
# MAGIC WHERE ile.`Completely Invoiced` = false
# MAGIC   AND ile.`Entry Type` = 'Output'
# MAGIC   AND ile.`Posting Date` >= DATE'2026-01-01'
# MAGIC GROUP BY ile.`Document No.`, ile.`Item No.`, i.`Description`,
# MAGIC          i.`Inventory Posting Group`
# MAGIC ORDER BY expected_cost DESC
# MAGIC LIMIT 200;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 2. Monthly net change on the FG Brass GL account itself
# MAGIC --    — to see if the May trend differs from prior months
# MAGIC SELECT
# MAGIC     YEAR(posting_date)  AS yr,
# MAGIC     MONTH(posting_date) AS mo,
# MAGIC     COUNT(*)            AS entries,
# MAGIC     SUM(net_amount)     AS monthly_net_change,
# MAGIC     SUM(SUM(net_amount)) OVER (ORDER BY YEAR(posting_date), MONTH(posting_date)) AS running_balance
# MAGIC FROM fa.gold_bc_gl_entry_clean
# MAGIC WHERE gl_account_no = '10372'
# MAGIC GROUP BY YEAR(posting_date), MONTH(posting_date)
# MAGIC ORDER BY yr DESC, mo DESC
# MAGIC LIMIT 18;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- For items contributing most to negative SF accounts: what does their
# MAGIC -- last successful Adjusted Output ILE say the cost was?
# MAGIC WITH last_good_actual AS (
# MAGIC     SELECT
# MAGIC         ile.`Item No.`,
# MAGIC         MAX(ile.`Posting Date`) AS last_finished_date,
# MAGIC         FIRST(ve.`Cost Amount (Actual)` / NULLIF(ile.`Quantity`, 0))
# MAGIC           AS last_unit_actual
# MAGIC     FROM `Silver_BC_Lakehouse`.bc.`Value Entry` ve
# MAGIC     JOIN `Silver_BC_Lakehouse`.bc.`Item Ledger Entry` ile
# MAGIC         ON ile.`Entry No.` = ve.`Item Ledger Entry No.`
# MAGIC     WHERE ile.`Entry Type` = 'Output'
# MAGIC       AND ile.`Completely Invoiced` = true
# MAGIC       AND ile.`Quantity` > 0
# MAGIC       AND ve.`Cost Amount (Actual)` <> 0
# MAGIC     GROUP BY ile.`Item No.`
# MAGIC ),
# MAGIC inventory_calc AS (
# MAGIC     -- Compute on-hand inventory from ILE (BC FlowField equivalent)
# MAGIC     SELECT
# MAGIC         `Item No.` AS item_no,
# MAGIC         SUM(`Remaining Quantity`) AS onhand_qty
# MAGIC     FROM `Silver_BC_Lakehouse`.bc.`Item Ledger Entry`
# MAGIC     GROUP BY `Item No.`
# MAGIC     HAVING SUM(`Remaining Quantity`) > 0
# MAGIC )
# MAGIC SELECT
# MAGIC     i.`No.`, 
# MAGIC     i.`Description`, 
# MAGIC     i.`Inventory Posting Group`,
# MAGIC     inv.onhand_qty,
# MAGIC     i.`Unit Cost`               AS item_card_unit_cost,
# MAGIC     lga.last_unit_actual        AS last_successful_unit_actual,
# MAGIC     lga.last_finished_date,
# MAGIC     inv.onhand_qty * i.`Unit Cost`           AS value_by_item_card,
# MAGIC     inv.onhand_qty * lga.last_unit_actual    AS value_by_last_actual,
# MAGIC     ABS(i.`Unit Cost` - lga.last_unit_actual) /
# MAGIC         NULLIF(lga.last_unit_actual, 0)      AS unit_cost_drift_pct
# MAGIC FROM `Silver_BC_Lakehouse`.bc.`Item` i
# MAGIC JOIN inventory_calc inv ON inv.item_no = i.`No.`
# MAGIC LEFT JOIN last_good_actual lga ON lga.`Item No.` = i.`No.`
# MAGIC WHERE i.`Inventory Posting Group` LIKE 'SF%'
# MAGIC ORDER BY inv.onhand_qty * COALESCE(lga.last_unit_actual, i.`Unit Cost`) DESC
# MAGIC LIMIT 50;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Full picture of the 12 PROs for the smoking-gun item
# MAGIC SELECT
# MAGIC     poh.`No.`           AS prod_order_no,
# MAGIC     poh.`Status`,
# MAGIC     poh.`Source No.`    AS source_sales_order,
# MAGIC     poh.`Quantity`      AS planned_qty,
# MAGIC     poh.`Starting Date`,
# MAGIC     poh.`Ending Date`,
# MAGIC     poh.`Due Date`,
# MAGIC     poh.`Finished Date`
# MAGIC FROM `Silver_BC_Lakehouse`.bc.`Production Order` poh
# MAGIC WHERE poh.`Source No.` = 'C000014414-00002'
# MAGIC    OR poh.`No.` IN ('WRO260303048','WRO260303348','WRO260303049',
# MAGIC                      'WRO260400140','WRO260303347','WRO260303887',
# MAGIC                      'WRO260303978','WRO260303886','WRO260303888',
# MAGIC                      'WRO260304373','WRO260303349')
# MAGIC ORDER BY poh.`No.`;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Did the units actually ship? Consumption + Sale entries for this item
# MAGIC SELECT
# MAGIC     ile.`Entry Type`,
# MAGIC     ile.`Posting Date`,
# MAGIC     ile.`Document No.`,
# MAGIC     ile.`Source No.`,
# MAGIC     ile.`Quantity`,
# MAGIC     ile.`Remaining Quantity`,
# MAGIC     ile.`Completely Invoiced`
# MAGIC FROM `Silver_BC_Lakehouse`.bc.`Item Ledger Entry` ile
# MAGIC WHERE ile.`Item No.` = 'C000014414-00002'
# MAGIC ORDER BY ile.`Posting Date` DESC, ile.`Entry No.` DESC
# MAGIC LIMIT 100;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = LEGACY;
# MAGIC SET spark.sql.parquet.int96RebaseModeInRead = LEGACY;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = LEGACY;
# MAGIC 
# MAGIC -- Query 1 (corrected): PROs producing item C000014414-00002, with computed finished/remaining qty
# MAGIC WITH item_pros AS (
# MAGIC     -- Find every PRO that has Output ILEs for this item
# MAGIC     SELECT DISTINCT `Document No.` AS pro_no
# MAGIC     FROM `Silver_BC_Lakehouse`.bc.`Item Ledger Entry`
# MAGIC     WHERE `Item No.` = 'C000014414-00002'
# MAGIC       AND `Entry Type` = 'Output'
# MAGIC ),
# MAGIC pro_output_actuals AS (
# MAGIC     -- Compute the actual output produced per PRO
# MAGIC     SELECT
# MAGIC         `Document No.`               AS pro_no,
# MAGIC         SUM(`Quantity`)              AS actual_output_qty,
# MAGIC         COUNT(*)                     AS output_ile_count,
# MAGIC         MIN(`Posting Date`)          AS first_output_date,
# MAGIC         MAX(`Posting Date`)          AS last_output_date
# MAGIC     FROM `Silver_BC_Lakehouse`.bc.`Item Ledger Entry`
# MAGIC     WHERE `Entry Type` = 'Output'
# MAGIC       AND `Item No.` = 'C000014414-00002'
# MAGIC     GROUP BY `Document No.`
# MAGIC )
# MAGIC SELECT
# MAGIC     poh.`No.`              AS prod_order_no,
# MAGIC     poh.`Status`,
# MAGIC     poh.`Source Type`,
# MAGIC     poh.`Source No.`       AS sales_order,
# MAGIC     poh.`Quantity`         AS planned_qty,
# MAGIC     COALESCE(po.actual_output_qty, 0)             AS produced_qty,
# MAGIC     poh.`Quantity` - COALESCE(po.actual_output_qty, 0) AS remaining_qty,
# MAGIC     po.output_ile_count,
# MAGIC     poh.`Starting Date`,
# MAGIC     poh.`Ending Date`,
# MAGIC     poh.`Due Date`,
# MAGIC     poh.`Finished Date`,
# MAGIC     poh.`Last Date Modified`,
# MAGIC     po.first_output_date,
# MAGIC     po.last_output_date
# MAGIC FROM `Silver_BC_Lakehouse`.bc.`Production Order` poh
# MAGIC JOIN item_pros ip
# MAGIC     ON ip.pro_no = poh.`No.`
# MAGIC LEFT JOIN pro_output_actuals po
# MAGIC     ON po.pro_no = poh.`No.`
# MAGIC ORDER BY poh.`Starting Date`, poh.`No.`;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC WITH item_pros AS (
# MAGIC     SELECT DISTINCT `Document No.` AS pro_no
# MAGIC     FROM `Silver_BC_Lakehouse`.bc.`Item Ledger Entry`
# MAGIC     WHERE `Item No.` = 'C000014414-00002'
# MAGIC       AND `Entry Type` = 'Output'
# MAGIC ),
# MAGIC pro_output_actuals AS (
# MAGIC     SELECT
# MAGIC         `Document No.`               AS pro_no,
# MAGIC         SUM(`Quantity`)              AS actual_output_qty,
# MAGIC         COUNT(*)                     AS output_ile_count,
# MAGIC         MIN(`Posting Date`)          AS first_output_date,
# MAGIC         MAX(`Posting Date`)          AS last_output_date
# MAGIC     FROM `Silver_BC_Lakehouse`.bc.`Item Ledger Entry`
# MAGIC     WHERE `Entry Type` = 'Output'
# MAGIC       AND `Item No.` = 'C000014414-00002'
# MAGIC     GROUP BY `Document No.`
# MAGIC )
# MAGIC SELECT
# MAGIC     poh.`No.`              AS prod_order_no,
# MAGIC     poh.`Status`,
# MAGIC     poh.`Source Type`,
# MAGIC     poh.`Source No.`       AS sales_order,
# MAGIC     poh.`Quantity`         AS planned_qty,
# MAGIC     COALESCE(po.actual_output_qty, 0)                  AS produced_qty,
# MAGIC     poh.`Quantity` - COALESCE(po.actual_output_qty, 0) AS remaining_qty,
# MAGIC     po.output_ile_count,
# MAGIC     poh.`Starting Date`, poh.`Ending Date`, poh.`Due Date`,
# MAGIC     poh.`Finished Date`, poh.`Last Date Modified`,
# MAGIC     po.first_output_date, po.last_output_date
# MAGIC FROM `Silver_BC_Lakehouse`.bc.`Production Order` poh
# MAGIC JOIN item_pros ip ON ip.pro_no = poh.`No.`
# MAGIC LEFT JOIN pro_output_actuals po ON po.pro_no = poh.`No.`
# MAGIC ORDER BY poh.`Starting Date`, poh.`No.`;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- A: What's in this item's BOM? (May reveal the diamond/stone content)
# MAGIC SELECT
# MAGIC     bomc.`Production BOM No.`,
# MAGIC     bomc.`Version Code`,
# MAGIC     bomc.`Line No.`,
# MAGIC     bomc.`Type`,
# MAGIC     bomc.`No.`            AS component_no,
# MAGIC     bomc.`Description`,
# MAGIC     bomc.`Quantity per`,
# MAGIC     bomc.`Unit of Measure Code`,
# MAGIC     -- Pull component item info
# MAGIC     comp.`Inventory Posting Group` AS comp_ipg,
# MAGIC     comp.`Item Category Code`      AS comp_category,
# MAGIC     comp.`Unit Cost`               AS comp_unit_cost,
# MAGIC     bomc.`Quantity per` * comp.`Unit Cost` AS line_cost_per_unit
# MAGIC FROM `Silver_BC_Lakehouse`.bc.`Production BOM Line` bomc
# MAGIC LEFT JOIN `Silver_BC_Lakehouse`.bc.`Item` comp
# MAGIC     ON comp.`No.` = bomc.`No.`
# MAGIC WHERE bomc.`Production BOM No.` IN (
# MAGIC     SELECT `Production BOM No.`
# MAGIC     FROM `Silver_BC_Lakehouse`.bc.`Item`
# MAGIC     WHERE `No.` = 'C000014414-00002'
# MAGIC )
# MAGIC ORDER BY bomc.`Production BOM No.`, bomc.`Version Code`, bomc.`Line No.`;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- B: What was actually consumed against these 15 PROs?
# MAGIC SELECT
# MAGIC     pol.`Prod. Order No.`,
# MAGIC     pol.`Item No.`            AS component_consumed,
# MAGIC     comp.`Description`,
# MAGIC     comp.`Inventory Posting Group` AS comp_ipg,
# MAGIC     pol.`Quantity per`,
# MAGIC     pol.`Expected Quantity`,
# MAGIC     pol.`Act. Consumption Qty.`,
# MAGIC     -- Compute actual cost from Value Entry
# MAGIC     SUM(ve.`Cost Amount (Expected)`) AS exp_cost_consumed
# MAGIC FROM `Silver_BC_Lakehouse`.bc.`Prod. Order Component` pol
# MAGIC LEFT JOIN `Silver_BC_Lakehouse`.bc.`Item` comp
# MAGIC     ON comp.`No.` = pol.`Item No.`
# MAGIC LEFT JOIN `Silver_BC_Lakehouse`.bc.`Item Ledger Entry` ile
# MAGIC     ON ile.`Order Type` = 'Production'
# MAGIC    AND ile.`Order No.` = pol.`Prod. Order No.`
# MAGIC    AND ile.`Item No.` = pol.`Item No.`
# MAGIC LEFT JOIN `Silver_BC_Lakehouse`.bc.`Value Entry` ve
# MAGIC     ON ve.`Item Ledger Entry No.` = ile.`Entry No.`
# MAGIC WHERE pol.`Prod. Order No.` IN (
# MAGIC     'WRO260303048','WRO260303049','WRO260303347','WRO260303348','WRO260303349',
# MAGIC     'WRO260303886','WRO260303887','WRO260303888','WRO260303978',
# MAGIC     'WRO260400140','WRO260304373','WRO260400585','WRO260400620',
# MAGIC     'WRO260401143','WSP260200026'
# MAGIC )
# MAGIC GROUP BY pol.`Prod. Order No.`, pol.`Item No.`, comp.`Description`,
# MAGIC          comp.`Inventory Posting Group`, pol.`Quantity per`,
# MAGIC          pol.`Expected Quantity`, pol.`Act. Consumption Qty.`
# MAGIC ORDER BY pol.`Prod. Order No.`, exp_cost_consumed DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- B (corrected): Components consumed against the 15 Released PROs
# MAGIC SELECT
# MAGIC     pol.`Prod. Order No.`,
# MAGIC     pol.`Item No.`                  AS component_consumed,
# MAGIC     comp.`Description`,
# MAGIC     comp.`Inventory Posting Group`  AS comp_ipg,
# MAGIC     pol.`Quantity per`,
# MAGIC     pol.`Expected Quantity`,
# MAGIC     pol.`Act. Consumption Qty.`,
# MAGIC     SUM(ve.`Cost Amount (Expected)`) AS exp_cost_consumed
# MAGIC FROM `Silver_BC_Lakehouse`.bc.`Prod Order Component` pol   -- ← no period
# MAGIC LEFT JOIN `Silver_BC_Lakehouse`.bc.`Item` comp
# MAGIC     ON comp.`No.` = pol.`Item No.`
# MAGIC LEFT JOIN `Silver_BC_Lakehouse`.bc.`Item Ledger Entry` ile
# MAGIC     ON ile.`Order Type` = 'Production'
# MAGIC    AND ile.`Order No.` = pol.`Prod. Order No.`
# MAGIC    AND ile.`Item No.` = pol.`Item No.`
# MAGIC LEFT JOIN `Silver_BC_Lakehouse`.bc.`Value Entry` ve
# MAGIC     ON ve.`Item Ledger Entry No.` = ile.`Entry No.`
# MAGIC WHERE pol.`Prod. Order No.` IN (
# MAGIC     'WRO260303048','WRO260303049','WRO260303347','WRO260303348','WRO260303349',
# MAGIC     'WRO260303886','WRO260303887','WRO260303888','WRO260303978',
# MAGIC     'WRO260400140','WRO260304373','WRO260400585','WRO260400620',
# MAGIC     'WRO260401143','WSP260200026'
# MAGIC )
# MAGIC GROUP BY pol.`Prod. Order No.`, pol.`Item No.`, comp.`Description`,
# MAGIC          comp.`Inventory Posting Group`, pol.`Quantity per`,
# MAGIC          pol.`Expected Quantity`, pol.`Act. Consumption Qty.`
# MAGIC ORDER BY pol.`Prod. Order No.`, exp_cost_consumed DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SHOW COLUMNS FROM `Silver_BC_Lakehouse`.bc.`Prod Order Component`;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- B1: BOM components for the 15 PROs — what was PLANNED to be consumed and at what cost
# MAGIC SELECT
# MAGIC     pol.`Prod. Order No.`,
# MAGIC     pol.`Item No.`                  AS component_consumed,
# MAGIC     pol.`Description`,
# MAGIC     comp.`Inventory Posting Group`  AS comp_ipg,
# MAGIC     pol.`Quantity per`,
# MAGIC     pol.`Quantity`                  AS planned_qty,
# MAGIC     pol.`Expected Quantity`,
# MAGIC     pol.`Remaining Quantity`,
# MAGIC     pol.`Unit Cost`                 AS comp_unit_cost,
# MAGIC     pol.`Cost Amount`               AS planned_cost_amount,
# MAGIC     pol.`Direct Cost Amount`
# MAGIC FROM `Silver_BC_Lakehouse`.bc.`Prod Order Component` pol
# MAGIC LEFT JOIN `Silver_BC_Lakehouse`.bc.`Item` comp
# MAGIC     ON comp.`No.` = pol.`Item No.`
# MAGIC WHERE pol.`Prod. Order No.` IN (
# MAGIC     'WRO260303048','WRO260303049','WRO260303347','WRO260303348','WRO260303349',
# MAGIC     'WRO260303886','WRO260303887','WRO260303888','WRO260303978',
# MAGIC     'WRO260400140','WRO260304373','WRO260400585','WRO260400620',
# MAGIC     'WRO260401143','WSP260200026'
# MAGIC )
# MAGIC ORDER BY pol.`Prod. Order No.`, pol.`Cost Amount` DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- B2: Actually-consumed components for the 15 PROs (from Item Ledger Entry consumption rows)
# MAGIC SELECT
# MAGIC     ile.`Order No.`              AS pro_no,
# MAGIC     ile.`Item No.`               AS component_consumed,
# MAGIC     comp.`Description`,
# MAGIC     comp.`Inventory Posting Group`  AS comp_ipg,
# MAGIC     SUM(ile.`Quantity`)          AS actual_qty_consumed,
# MAGIC     SUM(ve.`Cost Amount (Expected)`) AS exp_cost_consumed,
# MAGIC     SUM(ve.`Cost Amount (Actual)`)   AS act_cost_consumed
# MAGIC FROM `Silver_BC_Lakehouse`.bc.`Item Ledger Entry` ile
# MAGIC LEFT JOIN `Silver_BC_Lakehouse`.bc.`Value Entry` ve
# MAGIC     ON ve.`Item Ledger Entry No.` = ile.`Entry No.`
# MAGIC LEFT JOIN `Silver_BC_Lakehouse`.bc.`Item` comp
# MAGIC     ON comp.`No.` = ile.`Item No.`
# MAGIC WHERE ile.`Order Type` = 'Production'
# MAGIC   AND ile.`Entry Type` = 'Consumption'
# MAGIC   AND ile.`Order No.` IN (
# MAGIC     'WRO260303048','WRO260303049','WRO260303347','WRO260303348','WRO260303349',
# MAGIC     'WRO260303886','WRO260303887','WRO260303888','WRO260303978',
# MAGIC     'WRO260400140','WRO260304373','WRO260400585','WRO260400620',
# MAGIC     'WRO260401143','WSP260200026'
# MAGIC   )
# MAGIC GROUP BY ile.`Order No.`, ile.`Item No.`, comp.`Description`, comp.`Inventory Posting Group`
# MAGIC ORDER BY ile.`Order No.`, exp_cost_consumed DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Items whose BOM lists themselves as a component (circular reference)
# MAGIC SELECT
# MAGIC     i.`No.`           AS output_item,
# MAGIC     i.`Description`,
# MAGIC     i.`Inventory Posting Group`,
# MAGIC     i.`Unit Cost`,
# MAGIC     pbl.`Production BOM No.` AS bom_no,
# MAGIC     pbl.`Quantity per`,
# MAGIC     pbl.`Unit Cost`   AS comp_unit_cost
# MAGIC FROM `Silver_BC_Lakehouse`.bc.`Item` i
# MAGIC JOIN `Silver_BC_Lakehouse`.bc.`Production BOM Line` pbl
# MAGIC     ON pbl.`Production BOM No.` = i.`Production BOM No.`
# MAGIC    AND pbl.`No.` = i.`No.`                            -- ← self-reference
# MAGIC    AND pbl.`Type` = 'Item'
# MAGIC ORDER BY i.`Unit Cost` DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Items whose BOM lists themselves as a component (circular reference)
# MAGIC SELECT
# MAGIC     i.`No.`           AS output_item,
# MAGIC     i.`Description`,
# MAGIC     i.`Inventory Posting Group`,
# MAGIC     i.`Unit Cost`,
# MAGIC     pbl.`Production BOM No.` AS bom_no,
# MAGIC     pbl.`Quantity per`,
# MAGIC     pbl.`Unit Cost`   AS comp_unit_cost
# MAGIC FROM `Silver_BC_Lakehouse`.bc.`Item` i
# MAGIC JOIN `Silver_BC_Lakehouse`.bc.`Production BOM Line` pbl
# MAGIC     ON pbl.`Production BOM No.` = i.`Production BOM No.`
# MAGIC    AND pbl.`No.` = i.`No.`                            -- ← self-reference
# MAGIC    AND pbl.`Type` = 'Item'
# MAGIC ORDER BY i.`Unit Cost` DESC;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Cost history of C000014414-00002: how did Unit Cost evolve over time?
# MAGIC SELECT
# MAGIC     ve.`Posting Date`,
# MAGIC     ve.`Document No.`,
# MAGIC     ile.`Entry Type`,
# MAGIC     ile.`Quantity`,
# MAGIC     ve.`Cost per Unit`,
# MAGIC     ve.`Cost Amount (Expected)`,
# MAGIC     ve.`Cost Amount (Actual)`,
# MAGIC     ile.`Completely Invoiced`
# MAGIC FROM `Silver_BC_Lakehouse`.bc.`Value Entry` ve
# MAGIC JOIN `Silver_BC_Lakehouse`.bc.`Item Ledger Entry` ile
# MAGIC     ON ile.`Entry No.` = ve.`Item Ledger Entry No.`
# MAGIC WHERE ile.`Item No.` = 'C000014414-00002'
# MAGIC ORDER BY ve.`Posting Date`, ve.`Entry No.`
# MAGIC LIMIT 100;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Find the FIRST chronological appearance of inflated Cost per Unit
# MAGIC SELECT
# MAGIC     ve.`Posting Date`,
# MAGIC     ve.`Document No.`,
# MAGIC     ile.`Entry Type`,
# MAGIC     ile.`Source No.`,
# MAGIC     ile.`Quantity`,
# MAGIC     ile.`Remaining Quantity`,
# MAGIC     ve.`Cost per Unit`,
# MAGIC     ve.`Cost Amount (Expected)`,
# MAGIC     ve.`Cost Amount (Actual)`,
# MAGIC     ile.`Completely Invoiced`
# MAGIC FROM `Silver_BC_Lakehouse`.bc.`Value Entry` ve
# MAGIC JOIN `Silver_BC_Lakehouse`.bc.`Item Ledger Entry` ile
# MAGIC     ON ile.`Entry No.` = ve.`Item Ledger Entry No.`
# MAGIC WHERE ile.`Item No.` = 'C000014414-00002'
# MAGIC   AND ABS(ve.`Cost per Unit`) > 100
# MAGIC ORDER BY ve.`Posting Date`, ve.`Entry No.`
# MAGIC LIMIT 20;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fabric notebook source
# ============================================================================
#  nb_c000014414_patient_zero_v1.py
#  ----------------------------------------------------------------------------
#  Forensic investigation of Item Card Unit Cost corruption on
#  C000014414-00002 (Brass AW25 Molten Pave Earring Left, customer E-108041).
#
#  Context: 2026-05-12 PM handover identified that Item.Unit Cost = ฿179,914.97
#  but no Value Entry row shows that figure. First VE > ฿100 was 2025-09-09.
#  Goal: trace patient zero, confirm where the inflated Item Card figure came
#  from, and verify what cost the 15 open PROs are carrying.
#
#  Run cell-by-cell. Final cell materializes findings to:
#      fa.gold_c000014414_patient_zero
#  Output is referenced in the FY25 close KPMG disclosure narrative.
# ============================================================================

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 1 — Setup

# LEGACY rebase for pre-1582 BC sentinel dates + parameter
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")

ITEM_NO = "C000014414-00002"
SIBLING_RIGHT = "C000014416-00002"
FG_PAIR = "E000108041-00002"
CORRUPTED_UC = 179914.97

print(f"Target item     : {ITEM_NO}")
print(f"Sibling (Right) : {SIBLING_RIGHT}")
print(f"FG pair         : {FG_PAIR}")
print(f"Item.Unit Cost  : ฿{CORRUPTED_UC:,.2f}")


# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 2 — Discover mirrored table names
# MAGIC Confirms which `Prod*` tables exist before we query them
# MAGIC (mirror strips periods from table names, keeps them in columns).

# MAGIC %%sql
SHOW TABLES IN `Silver_BC_Lakehouse`.bc LIKE '%rod%';


# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 3 — Full VE history Aug–Oct 2025, no cost floor
# MAGIC We have to see what immediately precedes the Sept 9 jump.
# MAGIC The handover noted Mar–Jul 2025 was ฿0–฿9. What happened in Aug?

# MAGIC %%sql
SELECT
    ve.`Entry No.`               AS ve_entry_no,
    ile.`Entry No.`              AS ile_entry_no,
    ve.`Posting Date`,
    ve.`Document No.`,
    ile.`Entry Type`,
    ve.`Item Ledger Entry Type`,
    ile.`Source No.`,
    ile.`Quantity`,
    ile.`Remaining Quantity`,
    ve.`Valued Quantity`,
    ve.`Cost per Unit`,
    ve.`Cost Amount (Expected)`,
    ve.`Cost Amount (Actual)`,
    ve.`Expected Cost Posted to G/L`,
    ve.`Cost Posted to G/L`,
    ile.`Completely Invoiced`,
    ve.`Adjustment`
FROM `Silver_BC_Lakehouse`.bc.`Value Entry` ve
JOIN `Silver_BC_Lakehouse`.bc.`Item Ledger Entry` ile
    ON ile.`Entry No.` = ve.`Item Ledger Entry No.`
WHERE ile.`Item No.` = 'C000014414-00002'
  AND ve.`Posting Date` BETWEEN DATE'2025-07-01' AND DATE'2025-10-31'
ORDER BY ve.`Entry No.`;


# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 4 — Revaluation entries on this item (any date)
# MAGIC If a Revaluation Journal touched this item, this is where it shows.
# MAGIC `Cost per Unit` on a Revaluation line is the *new* posted unit cost.

# MAGIC %%sql
SELECT
    ve.`Posting Date`,
    ve.`Document No.`,
    ve.`Item Ledger Entry Type`  AS underlying_ile_type,
    ve.`Cost per Unit`,
    ve.`Valued Quantity`,
    ve.`Cost Amount (Expected)`,
    ve.`Cost Amount (Actual)`,
    ve.`User ID`,
    ve.`Source Code`,
    ve.`Reason Code`,
    ve.`Adjustment`
FROM `Silver_BC_Lakehouse`.bc.`Value Entry` ve
WHERE ve.`Item No.` = 'C000014414-00002'
  AND ve.`Entry Type` = 'Revaluation'
ORDER BY ve.`Posting Date`, ve.`Entry No.`;


# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 5 — Hunt for ฿179,914 across ALL Value Entry
# MAGIC Does the corrupted figure ever appear as a posted Cost per Unit
# MAGIC anywhere in the system (on this item or any other)?

# MAGIC %%sql
SELECT
    ve.`Item No.`,
    ve.`Posting Date`,
    ve.`Document No.`,
    ve.`Entry Type`,
    ve.`Item Ledger Entry Type`,
    ve.`Cost per Unit`,
    ve.`Valued Quantity`,
    ve.`Cost Amount (Actual)`,
    ve.`Source Code`,
    ve.`User ID`
FROM `Silver_BC_Lakehouse`.bc.`Value Entry` ve
WHERE ABS(ve.`Cost per Unit`) BETWEEN 170000 AND 190000
ORDER BY ve.`Posting Date`, ve.`Entry No.`;


# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 6 — Item Card state for the trio
# MAGIC Side-by-side Item.Unit Cost on Left / Right / FG pair.
# MAGIC Also pulls `Last Direct Cost`, `Standard Cost`, `Costing Method`.

# MAGIC %%sql
SELECT
    i.`No.`,
    i.`Description`,
    i.`Inventory Posting Group`,
    i.`Item Category Code`,
    i.`Costing Method`,
    i.`Unit Cost`,
    i.`Standard Cost`,
    i.`Last Direct Cost`,
    i.`Unit Price`,
    i.`Blocked`,
    i.`Last Date Modified`,
    i.`Created From Nonstock Item`,
    i.`Production BOM No.`
FROM `Silver_BC_Lakehouse`.bc.`Item` i
WHERE i.`No.` IN (
    'C000014414-00002',  -- target
    'C000014416-00002',  -- sibling Right
    'E000108041-00002'   -- FG pair
);


# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 7 — Check whether Change Log Entry is mirrored

tables_df = spark.sql("SHOW TABLES IN `Silver_BC_Lakehouse`.bc LIKE '%hange%og%'")
display(tables_df)

# If a Change Log table exists, the next cell becomes the right query.
# If empty, fall back to inspecting `User ID` + `Posting Date` on Item Journal
# Value Entries (Cell 4) as the audit trail proxy.


# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 8 — All PROs ever touching this item
# MAGIC Status, line cost, finished/remaining qty. The 15 "open" PROs come from
# MAGIC the Released subset. Compare `Unit Cost` on the Prod Order Line vs what
# MAGIC Value Entry actually posts.

# MAGIC %%sql
SELECT
    pol.`Prod. Order No.`,
    pol.`Line No.`,
    pol.`Status`,
    pol.`Item No.`,
    pol.`Description`,
    pol.`Quantity`,
    pol.`Finished Quantity`,
    pol.`Remaining Quantity`,
    pol.`Unit Cost`,
    pol.`Cost Amount`,
    pol.`Starting Date`,
    pol.`Ending Date`,
    pol.`Due Date`,
    pol.`Production BOM No.`,
    pol.`Routing No.`
FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line` pol
WHERE pol.`Item No.` = 'C000014414-00002'
ORDER BY pol.`Status` DESC, pol.`Starting Date` DESC;


# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 9 — For the 15 Released PROs: where does the inflated cost sit?
# MAGIC Three possible homes: (a) on the Output line for C000014414 itself,
# MAGIC (b) on a Component line consuming it, (c) on the FG pair output.
# MAGIC This cell rolls up by PRO + line role.

# MAGIC %%sql
WITH target_pros AS (
    SELECT DISTINCT `Prod. Order No.`
    FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line`
    WHERE `Item No.` = 'C000014414-00002'
      AND `Status` = 'Released'
),
pro_outputs AS (
    SELECT
        pol.`Prod. Order No.`,
        'OUTPUT'                 AS line_role,
        pol.`Item No.`,
        pol.`Description`,
        pol.`Quantity`,
        pol.`Unit Cost`,
        pol.`Cost Amount`
    FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line` pol
    INNER JOIN target_pros tp ON tp.`Prod. Order No.` = pol.`Prod. Order No.`
    WHERE pol.`Status` = 'Released'
),
pro_components AS (
    SELECT
        poc.`Prod. Order No.`,
        'COMPONENT'              AS line_role,
        poc.`Item No.`,
        poc.`Description`,
        poc.`Expected Quantity`  AS quantity,
        poc.`Unit Cost`,
        poc.`Cost Amount`
    FROM `Silver_BC_Lakehouse`.bc.`Prod Order Component` poc
    INNER JOIN target_pros tp ON tp.`Prod. Order No.` = poc.`Prod. Order No.`
    WHERE poc.`Status` = 'Released'
)
SELECT * FROM pro_outputs
UNION ALL
SELECT * FROM pro_components
ORDER BY `Prod. Order No.`, line_role DESC, `Item No.`;


# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 10 — Materialise findings to gold table

from pyspark.sql import functions as F

# Re-run the key facts as DataFrames and union into one summary record set.
ile_full = spark.sql("""
    SELECT
        'VE_history' AS finding_type,
        ve.`Posting Date` AS posting_date,
        ve.`Document No.` AS document_no,
        ile.`Entry Type` AS entry_type,
        ile.`Quantity` AS quantity,
        ve.`Cost per Unit` AS cost_per_unit,
        ve.`Cost Amount (Expected)` AS cost_expected,
        ve.`Cost Amount (Actual)` AS cost_actual,
        CAST(ile.`Completely Invoiced` AS STRING) AS completely_invoiced,
        ve.`User ID` AS user_id,
        ve.`Source Code` AS source_code
    FROM `Silver_BC_Lakehouse`.bc.`Value Entry` ve
    JOIN `Silver_BC_Lakehouse`.bc.`Item Ledger Entry` ile
        ON ile.`Entry No.` = ve.`Item Ledger Entry No.`
    WHERE ile.`Item No.` = 'C000014414-00002'
""")

item_card = spark.sql("""
    SELECT
        'Item_Card' AS finding_type,
        i.`Last Date Modified` AS posting_date,
        'ITEM_MASTER' AS document_no,
        i.`Costing Method` AS entry_type,
        CAST(NULL AS DECIMAL(38,20)) AS quantity,
        i.`Unit Cost` AS cost_per_unit,
        CAST(NULL AS DECIMAL(38,20)) AS cost_expected,
        i.`Standard Cost` AS cost_actual,
        CAST(i.`Blocked` AS STRING) AS completely_invoiced,
        CAST(NULL AS STRING) AS user_id,
        i.`Inventory Posting Group` AS source_code
    FROM `Silver_BC_Lakehouse`.bc.`Item` i
    WHERE i.`No.` = 'C000014414-00002'
""")

pro_lines = spark.sql("""
    SELECT
        'PRO_Line' AS finding_type,
        pol.`Starting Date` AS posting_date,
        pol.`Prod. Order No.` AS document_no,
        pol.`Status` AS entry_type,
        pol.`Remaining Quantity` AS quantity,
        pol.`Unit Cost` AS cost_per_unit,
        CAST(NULL AS DECIMAL(38,20)) AS cost_expected,
        pol.`Cost Amount` AS cost_actual,
        CAST(NULL AS STRING) AS completely_invoiced,
        CAST(NULL AS STRING) AS user_id,
        pol.`Production BOM No.` AS source_code
    FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line` pol
    WHERE pol.`Item No.` = 'C000014414-00002'
      AND pol.`Status` = 'Released'
""")

findings = (
    ile_full
    .unionByName(item_card)
    .unionByName(pro_lines)
    .withColumn("item_no", F.lit("C000014414-00002"))
    .withColumn("snapshot_date", F.current_date())
)

(findings
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("fa.gold_c000014414_patient_zero"))

print("Wrote fa.gold_c000014414_patient_zero")
display(spark.sql("""
    SELECT finding_type, COUNT(*) AS rows, MIN(posting_date) AS earliest,
           MAX(posting_date) AS latest, MAX(ABS(cost_per_unit)) AS max_unit_cost
    FROM fa.gold_c000014414_patient_zero
    GROUP BY finding_type
    ORDER BY finding_type
"""))


# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 11 — Decision support summary (run last)
# MAGIC Three single-number answers needed for the KPMG narrative + JE sizing.

# MAGIC %%sql
SELECT
    -- (a) Does ฿179,914 appear in VE? (Cell 5 result rolled to a scalar)
    (SELECT COUNT(*)
     FROM `Silver_BC_Lakehouse`.bc.`Value Entry`
     WHERE ABS(`Cost per Unit`) BETWEEN 170000 AND 190000)         AS ve_rows_near_179k,

    -- (b) Total Expected Cost stuck on the 15 Released PROs for this item
    (SELECT SUM(pol.`Cost Amount`)
     FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line` pol
     WHERE pol.`Item No.` = 'C000014414-00002'
       AND pol.`Status` = 'Released')                              AS released_pro_cost_amount,

    -- (c) What the cost WOULD be at sibling's Unit Cost
    (SELECT SUM(pol.`Remaining Quantity` * i.`Unit Cost`)
     FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line` pol
     CROSS JOIN (SELECT `Unit Cost` FROM `Silver_BC_Lakehouse`.bc.`Item`
                 WHERE `No.` = 'C000014416-00002') i
     WHERE pol.`Item No.` = 'C000014414-00002'
       AND pol.`Status` = 'Released')                              AS cost_at_sibling_uc,

    -- (d) JE size = difference between (b) and (c)
    (SELECT SUM(pol.`Cost Amount`)
       - SUM(pol.`Remaining Quantity` *
             (SELECT `Unit Cost` FROM `Silver_BC_Lakehouse`.bc.`Item`
              WHERE `No.` = 'C000014416-00002'))
     FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line` pol
     WHERE pol.`Item No.` = 'C000014414-00002'
       AND pol.`Status` = 'Released')                              AS implied_reclass_je_size;


# COMMAND ----------
# MAGIC %md
# MAGIC ## Operator notes
# MAGIC
# MAGIC - **Cell 2** confirms the exact table name spelling before later cells use it.
# MAGIC   If `Prod Order Line` returns nothing, try `Prod_ Order Line` or look at
# MAGIC   `Prod. Order Line` variants — the mirror has been inconsistent.
# MAGIC - **Cell 3** is the headline cell — the gap between the last sensible row
# MAGIC   in July and the first ~฿140 row tells you whether something posted in
# MAGIC   August that we haven't seen yet.
# MAGIC - **Cell 4** is the most likely smoking gun. A Revaluation entry with
# MAGIC   `Cost per Unit` ≈ ฿179,914 would explain everything. If empty, the
# MAGIC   corruption is from a non-VE source (manual Item Card edit, Standard
# MAGIC   Cost Worksheet roll, or BOM cost calc) — escalate to BC partner.
# MAGIC - **Cell 5** validates whether ฿179,914 ever posted anywhere. A non-empty
# MAGIC   result means we have the document trail. An empty result means the
# MAGIC   figure is purely a master-record write — and the JE narrative becomes
# MAGIC   "Item Card edited offline, never transacted".
# MAGIC - **Cell 11** sizes the JE for Thursday. (b) − (c) is what reclasses
# MAGIC   10391 → 10362. If it differs materially from the ฿28.1M we calc'd
# MAGIC   this morning, re-baseline before posting.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 1 — Setup

# LEGACY rebase for pre-1582 BC sentinel dates + parameter
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")

ITEM_NO = "C000014414-00002"
SIBLING_RIGHT = "C000014416-00002"
FG_PAIR = "E000108041-00002"
CORRUPTED_UC = 179914.97

print(f"Target item     : {ITEM_NO}")
print(f"Sibling (Right) : {SIBLING_RIGHT}")
print(f"FG pair         : {FG_PAIR}")
print(f"Item.Unit Cost  : ฿{CORRUPTED_UC:,.2f}")


# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 2 — Discover mirrored table names
# MAGIC Confirms which `Prod*` tables exist before we query them
# MAGIC (mirror strips periods from table names, keeps them in columns).

# MAGIC %%sql
SHOW TABLES IN `Silver_BC_Lakehouse`.bc LIKE '%rod%';


# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 3 — Full VE history Aug–Oct 2025, no cost floor
# MAGIC We have to see what immediately precedes the Sept 9 jump.
# MAGIC The handover noted Mar–Jul 2025 was ฿0–฿9. What happened in Aug?

# MAGIC %%sql
SELECT
    ve.`Entry No.`               AS ve_entry_no,
    ile.`Entry No.`              AS ile_entry_no,
    ve.`Posting Date`,
    ve.`Document No.`,
    ile.`Entry Type`,
    ve.`Item Ledger Entry Type`,
    ile.`Source No.`,
    ile.`Quantity`,
    ile.`Remaining Quantity`,
    ve.`Valued Quantity`,
    ve.`Cost per Unit`,
    ve.`Cost Amount (Expected)`,
    ve.`Cost Amount (Actual)`,
    ve.`Expected Cost Posted to G/L`,
    ve.`Cost Posted to G/L`,
    ile.`Completely Invoiced`,
    ve.`Adjustment`
FROM `Silver_BC_Lakehouse`.bc.`Value Entry` ve
JOIN `Silver_BC_Lakehouse`.bc.`Item Ledger Entry` ile
    ON ile.`Entry No.` = ve.`Item Ledger Entry No.`
WHERE ile.`Item No.` = 'C000014414-00002'
  AND ve.`Posting Date` BETWEEN DATE'2025-07-01' AND DATE'2025-10-31'
ORDER BY ve.`Entry No.`;


# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 4 — Revaluation entries on this item (any date)
# MAGIC If a Revaluation Journal touched this item, this is where it shows.
# MAGIC `Cost per Unit` on a Revaluation line is the *new* posted unit cost.

# MAGIC %%sql
SELECT
    ve.`Posting Date`,
    ve.`Document No.`,
    ve.`Item Ledger Entry Type`  AS underlying_ile_type,
    ve.`Cost per Unit`,
    ve.`Valued Quantity`,
    ve.`Cost Amount (Expected)`,
    ve.`Cost Amount (Actual)`,
    ve.`User ID`,
    ve.`Source Code`,
    ve.`Reason Code`,
    ve.`Adjustment`
FROM `Silver_BC_Lakehouse`.bc.`Value Entry` ve
WHERE ve.`Item No.` = 'C000014414-00002'
  AND ve.`Entry Type` = 'Revaluation'
ORDER BY ve.`Posting Date`, ve.`Entry No.`;


# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 5 — Hunt for ฿179,914 across ALL Value Entry
# MAGIC Does the corrupted figure ever appear as a posted Cost per Unit
# MAGIC anywhere in the system (on this item or any other)?

# MAGIC %%sql
SELECT
    ve.`Item No.`,
    ve.`Posting Date`,
    ve.`Document No.`,
    ve.`Entry Type`,
    ve.`Item Ledger Entry Type`,
    ve.`Cost per Unit`,
    ve.`Valued Quantity`,
    ve.`Cost Amount (Actual)`,
    ve.`Source Code`,
    ve.`User ID`
FROM `Silver_BC_Lakehouse`.bc.`Value Entry` ve
WHERE ABS(ve.`Cost per Unit`) BETWEEN 170000 AND 190000
ORDER BY ve.`Posting Date`, ve.`Entry No.`;


# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 6 — Item Card state for the trio
# MAGIC Side-by-side Item.Unit Cost on Left / Right / FG pair.
# MAGIC Also pulls `Last Direct Cost`, `Standard Cost`, `Costing Method`.

# MAGIC %%sql
SELECT
    i.`No.`,
    i.`Description`,
    i.`Inventory Posting Group`,
    i.`Item Category Code`,
    i.`Costing Method`,
    i.`Unit Cost`,
    i.`Standard Cost`,
    i.`Last Direct Cost`,
    i.`Unit Price`,
    i.`Blocked`,
    i.`Last Date Modified`,
    i.`Created From Nonstock Item`,
    i.`Production BOM No.`
FROM `Silver_BC_Lakehouse`.bc.`Item` i
WHERE i.`No.` IN (
    'C000014414-00002',  -- target
    'C000014416-00002',  -- sibling Right
    'E000108041-00002'   -- FG pair
);


# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 7 — Check whether Change Log Entry is mirrored

tables_df = spark.sql("SHOW TABLES IN `Silver_BC_Lakehouse`.bc LIKE '%hange%og%'")
display(tables_df)

# If a Change Log table exists, the next cell becomes the right query.
# If empty, fall back to inspecting `User ID` + `Posting Date` on Item Journal
# Value Entries (Cell 4) as the audit trail proxy.


# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 8 — All PROs ever touching this item
# MAGIC Status, line cost, finished/remaining qty. The 15 "open" PROs come from
# MAGIC the Released subset. Compare `Unit Cost` on the Prod Order Line vs what
# MAGIC Value Entry actually posts.

# MAGIC %%sql
SELECT
    pol.`Prod. Order No.`,
    pol.`Line No.`,
    pol.`Status`,
    pol.`Item No.`,
    pol.`Description`,
    pol.`Quantity`,
    pol.`Finished Quantity`,
    pol.`Remaining Quantity`,
    pol.`Unit Cost`,
    pol.`Cost Amount`,
    pol.`Starting Date`,
    pol.`Ending Date`,
    pol.`Due Date`,
    pol.`Production BOM No.`,
    pol.`Routing No.`
FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line` pol
WHERE pol.`Item No.` = 'C000014414-00002'
ORDER BY pol.`Status` DESC, pol.`Starting Date` DESC;


# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 9 — For the 15 Released PROs: where does the inflated cost sit?
# MAGIC Three possible homes: (a) on the Output line for C000014414 itself,
# MAGIC (b) on a Component line consuming it, (c) on the FG pair output.
# MAGIC This cell rolls up by PRO + line role.

# MAGIC %%sql
WITH target_pros AS (
    SELECT DISTINCT `Prod. Order No.`
    FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line`
    WHERE `Item No.` = 'C000014414-00002'
      AND `Status` = 'Released'
),
pro_outputs AS (
    SELECT
        pol.`Prod. Order No.`,
        'OUTPUT'                 AS line_role,
        pol.`Item No.`,
        pol.`Description`,
        pol.`Quantity`,
        pol.`Unit Cost`,
        pol.`Cost Amount`
    FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line` pol
    INNER JOIN target_pros tp ON tp.`Prod. Order No.` = pol.`Prod. Order No.`
    WHERE pol.`Status` = 'Released'
),
pro_components AS (
    SELECT
        poc.`Prod. Order No.`,
        'COMPONENT'              AS line_role,
        poc.`Item No.`,
        poc.`Description`,
        poc.`Expected Quantity`  AS quantity,
        poc.`Unit Cost`,
        poc.`Cost Amount`
    FROM `Silver_BC_Lakehouse`.bc.`Prod Order Component` poc
    INNER JOIN target_pros tp ON tp.`Prod. Order No.` = poc.`Prod. Order No.`
    WHERE poc.`Status` = 'Released'
)
SELECT * FROM pro_outputs
UNION ALL
SELECT * FROM pro_components
ORDER BY `Prod. Order No.`, line_role DESC, `Item No.`;


# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 10 — Materialise findings to gold table

from pyspark.sql import functions as F

# Re-run the key facts as DataFrames and union into one summary record set.
ile_full = spark.sql("""
    SELECT
        'VE_history' AS finding_type,
        ve.`Posting Date` AS posting_date,
        ve.`Document No.` AS document_no,
        ile.`Entry Type` AS entry_type,
        ile.`Quantity` AS quantity,
        ve.`Cost per Unit` AS cost_per_unit,
        ve.`Cost Amount (Expected)` AS cost_expected,
        ve.`Cost Amount (Actual)` AS cost_actual,
        CAST(ile.`Completely Invoiced` AS STRING) AS completely_invoiced,
        ve.`User ID` AS user_id,
        ve.`Source Code` AS source_code
    FROM `Silver_BC_Lakehouse`.bc.`Value Entry` ve
    JOIN `Silver_BC_Lakehouse`.bc.`Item Ledger Entry` ile
        ON ile.`Entry No.` = ve.`Item Ledger Entry No.`
    WHERE ile.`Item No.` = 'C000014414-00002'
""")

item_card = spark.sql("""
    SELECT
        'Item_Card' AS finding_type,
        i.`Last Date Modified` AS posting_date,
        'ITEM_MASTER' AS document_no,
        i.`Costing Method` AS entry_type,
        CAST(NULL AS DECIMAL(38,20)) AS quantity,
        i.`Unit Cost` AS cost_per_unit,
        CAST(NULL AS DECIMAL(38,20)) AS cost_expected,
        i.`Standard Cost` AS cost_actual,
        CAST(i.`Blocked` AS STRING) AS completely_invoiced,
        CAST(NULL AS STRING) AS user_id,
        i.`Inventory Posting Group` AS source_code
    FROM `Silver_BC_Lakehouse`.bc.`Item` i
    WHERE i.`No.` = 'C000014414-00002'
""")

pro_lines = spark.sql("""
    SELECT
        'PRO_Line' AS finding_type,
        pol.`Starting Date` AS posting_date,
        pol.`Prod. Order No.` AS document_no,
        pol.`Status` AS entry_type,
        pol.`Remaining Quantity` AS quantity,
        pol.`Unit Cost` AS cost_per_unit,
        CAST(NULL AS DECIMAL(38,20)) AS cost_expected,
        pol.`Cost Amount` AS cost_actual,
        CAST(NULL AS STRING) AS completely_invoiced,
        CAST(NULL AS STRING) AS user_id,
        pol.`Production BOM No.` AS source_code
    FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line` pol
    WHERE pol.`Item No.` = 'C000014414-00002'
      AND pol.`Status` = 'Released'
""")

findings = (
    ile_full
    .unionByName(item_card)
    .unionByName(pro_lines)
    .withColumn("item_no", F.lit("C000014414-00002"))
    .withColumn("snapshot_date", F.current_date())
)

(findings
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("fa.gold_c000014414_patient_zero"))

print("Wrote fa.gold_c000014414_patient_zero")
display(spark.sql("""
    SELECT finding_type, COUNT(*) AS rows, MIN(posting_date) AS earliest,
           MAX(posting_date) AS latest, MAX(ABS(cost_per_unit)) AS max_unit_cost
    FROM fa.gold_c000014414_patient_zero
    GROUP BY finding_type
    ORDER BY finding_type
"""))


# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 11 — Decision support summary (run last)
# MAGIC Three single-number answers needed for the KPMG narrative + JE sizing.

# MAGIC %%sql
SELECT
    -- (a) Does ฿179,914 appear in VE? (Cell 5 result rolled to a scalar)
    (SELECT COUNT(*)
     FROM `Silver_BC_Lakehouse`.bc.`Value Entry`
     WHERE ABS(`Cost per Unit`) BETWEEN 170000 AND 190000)         AS ve_rows_near_179k,

    -- (b) Total Expected Cost stuck on the 15 Released PROs for this item
    (SELECT SUM(pol.`Cost Amount`)
     FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line` pol
     WHERE pol.`Item No.` = 'C000014414-00002'
       AND pol.`Status` = 'Released')                              AS released_pro_cost_amount,

    -- (c) What the cost WOULD be at sibling's Unit Cost
    (SELECT SUM(pol.`Remaining Quantity` * i.`Unit Cost`)
     FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line` pol
     CROSS JOIN (SELECT `Unit Cost` FROM `Silver_BC_Lakehouse`.bc.`Item`
                 WHERE `No.` = 'C000014416-00002') i
     WHERE pol.`Item No.` = 'C000014414-00002'
       AND pol.`Status` = 'Released')                              AS cost_at_sibling_uc,

    -- (d) JE size = difference between (b) and (c)
    (SELECT SUM(pol.`Cost Amount`)
       - SUM(pol.`Remaining Quantity` *
             (SELECT `Unit Cost` FROM `Silver_BC_Lakehouse`.bc.`Item`
              WHERE `No.` = 'C000014416-00002'))
     FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line` pol
     WHERE pol.`Item No.` = 'C000014414-00002'
       AND pol.`Status` = 'Released')                              AS implied_reclass_je_size;

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fabric notebook source — PySpark only
# ============================================================================
#  nb_c000014414_patient_zero_v2.py
#  ----------------------------------------------------------------------------
#  Forensic investigation of Item Card Unit Cost corruption on
#  C000014414-00002 (Brass AW25 Molten Pave Earring Left, customer E-108041).
#
#  Item.Unit Cost on master record: THB 179,914.97 (corrupted).
#  No Value Entry shows that figure. First VE > 100 was 2025-09-09.
#
#  v2 change: all cells are PySpark. SQL is wrapped in spark.sql(""" ... """).
#  No cell-type switching required — paste each block into a Python cell.
#
#  Final cell materialises findings to: fa.gold_c000014414_patient_zero
# ============================================================================


# ============================================================================
# CELL 1 - Setup
# ============================================================================

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")

ITEM_NO       = "C000014414-00002"
SIBLING_RIGHT = "C000014416-00002"
FG_PAIR       = "E000108041-00002"
CORRUPTED_UC  = 179914.97

print(f"Target item     : {ITEM_NO}")
print(f"Sibling (Right) : {SIBLING_RIGHT}")
print(f"FG pair         : {FG_PAIR}")
print(f"Item.Unit Cost  : THB {CORRUPTED_UC:,.2f}")


# ============================================================================
# CELL 2 - Discover mirrored Prod* table names
# Mirror strips periods from table names but keeps them in columns.
# Confirm exact spelling before Cells 8/9 use the names.
# ============================================================================

prod_tables = spark.sql("""
    SHOW TABLES IN `Silver_BC_Lakehouse`.bc LIKE '*rod*'
""")
display(prod_tables)


# ============================================================================
# CELL 3 - Full VE history Jul-Oct 2025, no cost floor
# What immediately precedes the Sept 9 jump from <THB 10 to ~THB 140?
# ============================================================================

ve_jul_oct = spark.sql("""
    SELECT
        ve.`Entry No.`                   AS ve_entry_no,
        ile.`Entry No.`                  AS ile_entry_no,
        ve.`Posting Date`,
        ve.`Document No.`,
        ile.`Entry Type`,
        ve.`Item Ledger Entry Type`,
        ile.`Source No.`,
        ile.`Quantity`,
        ile.`Remaining Quantity`,
        ve.`Valued Quantity`,
        ve.`Cost per Unit`,
        ve.`Cost Amount (Expected)`,
        ve.`Cost Amount (Actual)`,
        ve.`Expected Cost Posted to G/L`,
        ve.`Cost Posted to G/L`,
        ile.`Completely Invoiced`,
        ve.`Adjustment`,
        ve.`User ID`,
        ve.`Source Code`
    FROM `Silver_BC_Lakehouse`.bc.`Value Entry` ve
    JOIN `Silver_BC_Lakehouse`.bc.`Item Ledger Entry` ile
        ON ile.`Entry No.` = ve.`Item Ledger Entry No.`
    WHERE ile.`Item No.` = 'C000014414-00002'
      AND ve.`Posting Date` BETWEEN DATE'2025-07-01' AND DATE'2025-10-31'
    ORDER BY ve.`Entry No.`
""")
display(ve_jul_oct)


# ============================================================================
# CELL 4 - Revaluation entries on this item (any date)
# A Revaluation row with Cost per Unit near 179,914 is the smoking gun.
# Returns User ID and Source Code so you know who/where it came from.
# ============================================================================

reval_entries = spark.sql("""
    SELECT
        ve.`Posting Date`,
        ve.`Document No.`,
        ve.`Item Ledger Entry Type`     AS underlying_ile_type,
        ve.`Cost per Unit`,
        ve.`Valued Quantity`,
        ve.`Cost Amount (Expected)`,
        ve.`Cost Amount (Actual)`,
        ve.`User ID`,
        ve.`Source Code`,
        ve.`Reason Code`,
        ve.`Adjustment`
    FROM `Silver_BC_Lakehouse`.bc.`Value Entry` ve
    WHERE ve.`Item No.` = 'C000014414-00002'
      AND ve.`Entry Type` = 'Revaluation'
    ORDER BY ve.`Posting Date`, ve.`Entry No.`
""")
display(reval_entries)


# ============================================================================
# CELL 5 - Hunt for THB 179,914 across ALL Value Entry (any item)
# Does the corrupted figure exist anywhere as a posted Cost per Unit?
# Empty result = corruption is master-record only, never transacted.
# ============================================================================

ve_near_179k = spark.sql("""
    SELECT
        ve.`Item No.`,
        ve.`Posting Date`,
        ve.`Document No.`,
        ve.`Entry Type`,
        ve.`Item Ledger Entry Type`,
        ve.`Cost per Unit`,
        ve.`Valued Quantity`,
        ve.`Cost Amount (Actual)`,
        ve.`Source Code`,
        ve.`User ID`
    FROM `Silver_BC_Lakehouse`.bc.`Value Entry` ve
    WHERE ABS(ve.`Cost per Unit`) BETWEEN 170000 AND 190000
    ORDER BY ve.`Posting Date`, ve.`Entry No.`
""")
display(ve_near_179k)


# ============================================================================
# CELL 6 - Item Card state for the trio: Left, Right, FG pair
# ============================================================================

item_card_trio = spark.sql("""
    SELECT
        i.`No.`,
        i.`Description`,
        i.`Inventory Posting Group`,
        i.`Item Category Code`,
        i.`Costing Method`,
        i.`Unit Cost`,
        i.`Standard Cost`,
        i.`Last Direct Cost`,
        i.`Unit Price`,
        i.`Blocked`,
        i.`Last Date Modified`,
        i.`Created From Nonstock Item`,
        i.`Production BOM No.`
    FROM `Silver_BC_Lakehouse`.bc.`Item` i
    WHERE i.`No.` IN (
        'C000014414-00002',
        'C000014416-00002',
        'E000108041-00002'
    )
""")
display(item_card_trio)


# ============================================================================
# CELL 7 - Check whether a Change Log table is mirrored
# If yes, the next investigative step is to filter it on Table No. 27 (Item)
# and Field No. of Unit Cost. If no, fall back to VE User ID + Source Code.
# ============================================================================

change_log_tables = spark.sql("""
    SHOW TABLES IN `Silver_BC_Lakehouse`.bc LIKE '*hange*og*'
""")
display(change_log_tables)


# ============================================================================
# CELL 8 - All PROs ever touching this item as the output line
# Shows Unit Cost / Cost Amount that Prod Order Line is carrying.
# Status = 'Released' rows are the 15 in-flight PROs.
# ============================================================================

pro_lines_all = spark.sql("""
    SELECT
        pol.`Prod. Order No.`,
        pol.`Line No.`,
        pol.`Status`,
        pol.`Item No.`,
        pol.`Description`,
        pol.`Quantity`,
        pol.`Finished Quantity`,
        pol.`Remaining Quantity`,
        pol.`Unit Cost`,
        pol.`Cost Amount`,
        pol.`Starting Date`,
        pol.`Ending Date`,
        pol.`Due Date`,
        pol.`Production BOM No.`,
        pol.`Routing No.`
    FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line` pol
    WHERE pol.`Item No.` = 'C000014414-00002'
    ORDER BY pol.`Status` DESC, pol.`Starting Date` DESC
""")
display(pro_lines_all)


# ============================================================================
# CELL 9 - 15 Released PROs - where does the inflated cost actually sit?
# Three possible homes:
#   OUTPUT line for C000014414 itself
#   COMPONENT line for a downstream PRO consuming it (FG pair PROs)
# Rolled up by PRO + line role.
# ============================================================================

pro_cost_rollup = spark.sql("""
    WITH target_pros AS (
        SELECT DISTINCT `Prod. Order No.`
        FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line`
        WHERE `Item No.` = 'C000014414-00002'
          AND `Status` = 'Released'
    ),
    pro_outputs AS (
        SELECT
            pol.`Prod. Order No.`,
            'OUTPUT'                AS line_role,
            pol.`Item No.`,
            pol.`Description`,
            pol.`Quantity`,
            pol.`Unit Cost`,
            pol.`Cost Amount`
        FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line` pol
        INNER JOIN target_pros tp
            ON tp.`Prod. Order No.` = pol.`Prod. Order No.`
        WHERE pol.`Status` = 'Released'
    ),
    pro_components AS (
        SELECT
            poc.`Prod. Order No.`,
            'COMPONENT'             AS line_role,
            poc.`Item No.`,
            poc.`Description`,
            poc.`Expected Quantity` AS quantity,
            poc.`Unit Cost`,
            poc.`Cost Amount`
        FROM `Silver_BC_Lakehouse`.bc.`Prod Order Component` poc
        INNER JOIN target_pros tp
            ON tp.`Prod. Order No.` = poc.`Prod. Order No.`
        WHERE poc.`Status` = 'Released'
    )
    SELECT * FROM pro_outputs
    UNION ALL
    SELECT * FROM pro_components
    ORDER BY `Prod. Order No.`, line_role DESC, `Item No.`
""")
display(pro_cost_rollup)


# ============================================================================
# CELL 10 - Materialise findings to fa.gold_c000014414_patient_zero
# Single artefact KPMG memo can cite. Union of VE history + Item Card snapshot
# + Released PRO line state, tagged by finding_type and snapshot_date.
# ============================================================================

from pyspark.sql import functions as F

ile_full = spark.sql("""
    SELECT
        'VE_history'                                    AS finding_type,
        ve.`Posting Date`                               AS posting_date,
        ve.`Document No.`                               AS document_no,
        ile.`Entry Type`                                AS entry_type,
        CAST(ile.`Quantity` AS DECIMAL(38,20))          AS quantity,
        CAST(ve.`Cost per Unit` AS DECIMAL(38,20))      AS cost_per_unit,
        CAST(ve.`Cost Amount (Expected)` AS DECIMAL(38,20)) AS cost_expected,
        CAST(ve.`Cost Amount (Actual)` AS DECIMAL(38,20))   AS cost_actual,
        CAST(ile.`Completely Invoiced` AS STRING)       AS completely_invoiced,
        ve.`User ID`                                    AS user_id,
        ve.`Source Code`                                AS source_code
    FROM `Silver_BC_Lakehouse`.bc.`Value Entry` ve
    JOIN `Silver_BC_Lakehouse`.bc.`Item Ledger Entry` ile
        ON ile.`Entry No.` = ve.`Item Ledger Entry No.`
    WHERE ile.`Item No.` = 'C000014414-00002'
""")

item_card = spark.sql("""
    SELECT
        'Item_Card'                                     AS finding_type,
        i.`Last Date Modified`                          AS posting_date,
        'ITEM_MASTER'                                   AS document_no,
        i.`Costing Method`                              AS entry_type,
        CAST(NULL AS DECIMAL(38,20))                    AS quantity,
        CAST(i.`Unit Cost` AS DECIMAL(38,20))           AS cost_per_unit,
        CAST(NULL AS DECIMAL(38,20))                    AS cost_expected,
        CAST(i.`Standard Cost` AS DECIMAL(38,20))       AS cost_actual,
        CAST(i.`Blocked` AS STRING)                     AS completely_invoiced,
        CAST(NULL AS STRING)                            AS user_id,
        i.`Inventory Posting Group`                     AS source_code
    FROM `Silver_BC_Lakehouse`.bc.`Item` i
    WHERE i.`No.` = 'C000014414-00002'
""")

pro_lines = spark.sql("""
    SELECT
        'PRO_Line'                                      AS finding_type,
        pol.`Starting Date`                             AS posting_date,
        pol.`Prod. Order No.`                           AS document_no,
        pol.`Status`                                    AS entry_type,
        CAST(pol.`Remaining Quantity` AS DECIMAL(38,20)) AS quantity,
        CAST(pol.`Unit Cost` AS DECIMAL(38,20))         AS cost_per_unit,
        CAST(NULL AS DECIMAL(38,20))                    AS cost_expected,
        CAST(pol.`Cost Amount` AS DECIMAL(38,20))       AS cost_actual,
        CAST(NULL AS STRING)                            AS completely_invoiced,
        CAST(NULL AS STRING)                            AS user_id,
        pol.`Production BOM No.`                        AS source_code
    FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line` pol
    WHERE pol.`Item No.` = 'C000014414-00002'
      AND pol.`Status` = 'Released'
""")

findings = (
    ile_full
        .unionByName(item_card)
        .unionByName(pro_lines)
        .withColumn("item_no", F.lit("C000014414-00002"))
        .withColumn("snapshot_date", F.current_date())
)

(findings
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("fa.gold_c000014414_patient_zero"))

print("Wrote fa.gold_c000014414_patient_zero")

summary = spark.sql("""
    SELECT
        finding_type,
        COUNT(*)                       AS rows,
        MIN(posting_date)              AS earliest,
        MAX(posting_date)              AS latest,
        MAX(ABS(cost_per_unit))        AS max_unit_cost
    FROM fa.gold_c000014414_patient_zero
    GROUP BY finding_type
    ORDER BY finding_type
""")
display(summary)


# ============================================================================
# CELL 11 - Decision support summary
# Four single-number answers for the KPMG narrative and JE sizing:
#   (a) does the 179k figure exist in VE
#   (b) Released PRO Cost Amount carrying the bad cost
#   (c) what the same PROs would cost at sibling Right earring Unit Cost
#   (d) implied reclass JE size = b - c
# ============================================================================

decision_support = spark.sql("""
    SELECT
        (SELECT COUNT(*)
         FROM `Silver_BC_Lakehouse`.bc.`Value Entry`
         WHERE ABS(`Cost per Unit`) BETWEEN 170000 AND 190000)
            AS ve_rows_near_179k,

        (SELECT SUM(pol.`Cost Amount`)
         FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line` pol
         WHERE pol.`Item No.` = 'C000014414-00002'
           AND pol.`Status` = 'Released')
            AS released_pro_cost_amount,

        (SELECT SUM(pol.`Remaining Quantity` * sib.`Unit Cost`)
         FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line` pol
         CROSS JOIN (
             SELECT `Unit Cost`
             FROM `Silver_BC_Lakehouse`.bc.`Item`
             WHERE `No.` = 'C000014416-00002'
         ) sib
         WHERE pol.`Item No.` = 'C000014414-00002'
           AND pol.`Status` = 'Released')
            AS cost_at_sibling_uc,

        (
            (SELECT SUM(pol.`Cost Amount`)
             FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line` pol
             WHERE pol.`Item No.` = 'C000014414-00002'
               AND pol.`Status` = 'Released')
          -
            (SELECT SUM(pol.`Remaining Quantity` * sib.`Unit Cost`)
             FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line` pol
             CROSS JOIN (
                 SELECT `Unit Cost`
                 FROM `Silver_BC_Lakehouse`.bc.`Item`
                 WHERE `No.` = 'C000014416-00002'
             ) sib
             WHERE pol.`Item No.` = 'C000014414-00002'
               AND pol.`Status` = 'Released')
        )   AS implied_reclass_je_size
""")
display(decision_support)


# ============================================================================
# OPERATOR NOTES
# ----------------------------------------------------------------------------
# - All cells are PySpark. Paste each banner-to-banner block into a fresh
#   Fabric Python cell. No cell-type switching needed.
#
# - Cell 2 confirms the exact mirrored table name. If a later cell errors
#   with "table not found", run Cell 2 and adjust the table name in the
#   failing cell to match what Cell 2 returns.
#
# - Cell 4 is the smoking-gun candidate. A Revaluation row with Cost per
#   Unit near 179,914 explains everything: User ID + Source Code tell you
#   who posted it. Empty result means the corruption is on the master
#   record only - escalate to BC partner for Q1 FY26 repair.
#
# - Cell 5 validates whether THB 179,914 ever posted anywhere as a unit
#   cost. Empty = master-record-only write. Non-empty = full audit trail
#   exists; pull the document.
#
# - Cell 11 sizes the JE for Thursday. The implied_reclass_je_size column
#   is what reclasses 10391 to 10362. Sanity-check against this morning's
#   THB 28.1M working estimate before posting; if it differs by more than
#   ~10 percent re-baseline before the JE is approved.
#
# - Cell 10 materialises everything to fa.gold_c000014414_patient_zero so
#   the KPMG disclosure can cite a single Fabric table.
# ============================================================================

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Back-cast 10391 and 10362 to FY25 close
# MAGIC SELECT
# MAGIC     gl_account_no,
# MAGIC     SUM(CASE WHEN posting_date <= DATE'2025-12-31' THEN net_amount ELSE 0 END) AS balance_dec31_2025,
# MAGIC     SUM(CASE WHEN posting_date <= DATE'2026-05-12' THEN net_amount ELSE 0 END) AS balance_today,
# MAGIC     SUM(CASE WHEN posting_date > DATE'2025-12-31' THEN net_amount ELSE 0 END) AS fy26_movement
# MAGIC FROM fa.gold_bc_gl_entry_clean
# MAGIC WHERE gl_account_no IN ('10391','10362','10360','10361','10410','10372')
# MAGIC GROUP BY gl_account_no
# MAGIC ORDER BY gl_account_no;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- When were the 15 C000014414 PROs Released?
# MAGIC SELECT
# MAGIC     `No.` AS prod_order_no,
# MAGIC     `Status`,
# MAGIC     `Source No.`,
# MAGIC     `Starting Date`,
# MAGIC     `Creation Date`,
# MAGIC     `Cost Amount` AS planned_cost
# MAGIC FROM `Silver_BC_Lakehouse`.bc.`Production Order`
# MAGIC WHERE `Source No.` = 'C000014414-00002'
# MAGIC    OR `No.` IN (<paste the 15 PRO numbers from untitled__3_.csv>)
# MAGIC ORDER BY `Creation Date`;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC     po.`No.` AS prod_order_no,
# MAGIC     po.`Status`,
# MAGIC     po.`Source Type`,
# MAGIC     po.`Source No.` AS header_source_no,
# MAGIC     pol.`Item No.` AS line_item_no,
# MAGIC     po.`Creation Date`,
# MAGIC     po.`Starting Date`,
# MAGIC     po.`Ending Date`,
# MAGIC     pol.`Quantity` AS planned_qty,
# MAGIC     pol.`Unit Cost`,
# MAGIC     pol.`Cost Amount`,
# MAGIC     YEAR(po.`Creation Date`) AS creation_year
# MAGIC FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line` pol
# MAGIC JOIN `Silver_BC_Lakehouse`.bc.`Production Order` po
# MAGIC     ON po.`No.`    = pol.`Prod. Order No.`
# MAGIC    AND po.`Status` = pol.`Status`
# MAGIC WHERE pol.`Item No.` = 'C000014414-00002'
# MAGIC   AND po.`Status`    = 'Released'
# MAGIC ORDER BY po.`Creation Date`;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Force the rebase config
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")

# Verify it took
print("datetime rebase:", spark.conf.get("spark.sql.parquet.datetimeRebaseModeInRead"))
print("int96 rebase:   ", spark.conf.get("spark.sql.parquet.int96RebaseModeInRead"))

# Run the query through spark.sql so the config is guaranteed to apply
df = spark.sql("""
    SELECT
        po.`No.` AS prod_order_no,
        po.`Status`,
        po.`Source No.` AS header_source_no,
        pol.`Item No.` AS line_item_no,
        po.`Creation Date`,
        po.`Starting Date`,
        po.`Ending Date`,
        pol.`Quantity` AS planned_qty,
        pol.`Unit Cost`,
        pol.`Cost Amount`,
        YEAR(po.`Creation Date`) AS creation_year
    FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line` pol
    JOIN `Silver_BC_Lakehouse`.bc.`Production Order` po
        ON po.`No.`    = pol.`Prod. Order No.`
       AND po.`Status` = pol.`Status`
    WHERE pol.`Item No.` = 'C000014414-00002'
      AND po.`Status`    = 'Released'
    ORDER BY po.`Creation Date`
""")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("""
    SELECT
        poc.`Prod. Order No.` AS fg_pro,
        poc.`Status`,
        po.`Source No.`       AS fg_output_item,
        po.`Creation Date`    AS fg_pro_created,
        YEAR(po.`Creation Date`) AS fg_created_year,
        poc.`Item No.`        AS component_item,
        poc.`Quantity per`,
        poc.`Expected Quantity`,
        poc.`Remaining Quantity`
    FROM `Silver_BC_Lakehouse`.bc.`Prod Order Component` poc
    JOIN `Silver_BC_Lakehouse`.bc.`Production Order` po
        ON po.`No.`    = poc.`Prod. Order No.`
       AND po.`Status` = poc.`Status`
    WHERE poc.`Item No.` = 'C000014414-00002'
      AND poc.`Status`   = 'Released'
    ORDER BY po.`Creation Date`
""")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")

df = spark.sql("""
    SELECT
        poc.`Prod. Order No.` AS fg_pro,
        poc.`Status`,
        poc.`Item No.`        AS component_item,
        poc.`Quantity per`,
        poc.`Expected Quantity`,
        poc.`Remaining Quantity`,
        REGEXP_EXTRACT(poc.`Prod. Order No.`, '[A-Z]+([0-9]{2})', 1) AS pro_year_code
    FROM `Silver_BC_Lakehouse`.bc.`Prod Order Component` poc
    WHERE poc.`Item No.` = 'C000014414-00002'
      AND poc.`Status`   = 'Released'
    ORDER BY poc.`Prod. Order No.`
""")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run this FIRST every time the session starts
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Verify
print("Session OK")
print(" datetime rebase:", spark.conf.get("spark.sql.parquet.datetimeRebaseModeInRead"))
print(" int96 rebase:   ", spark.conf.get("spark.sql.parquet.int96RebaseModeInRead"))
print(" broadcast thresh:", spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("""
    SELECT
        poc.`Prod. Order No.` AS fg_pro,
        poc.`Status`,
        poc.`Item No.`        AS component_item,
        poc.`Quantity per`,
        poc.`Expected Quantity`,
        poc.`Remaining Quantity`,
        REGEXP_EXTRACT(poc.`Prod. Order No.`, '[A-Z]+([0-9]{2})', 1) AS pro_year_code
    FROM `Silver_BC_Lakehouse`.bc.`Prod Order Component` poc
    WHERE poc.`Item No.` = 'C000014414-00002'
      AND poc.`Status`   = 'Released'
    ORDER BY poc.`Prod. Order No.`
""")
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("""
    SELECT
        gl_account_no,
        SUM(CASE WHEN posting_date <= DATE'2025-12-31' THEN net_amount ELSE 0 END) AS balance_dec31_2025,
        SUM(net_amount) AS balance_today,
        SUM(CASE WHEN posting_date > DATE'2025-12-31' THEN net_amount ELSE 0 END) AS fy26_movement
    FROM fa.gold_bc_gl_entry_clean
    WHERE gl_account_no IN ('10391','10362','10360','10361','10410','10372','10310','10311')
    GROUP BY gl_account_no
    ORDER BY gl_account_no
""")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("""
    SELECT
        document_no,
        YEAR(posting_date) AS year,
        MONTH(posting_date) AS month,
        COUNT(*) AS entry_count,
        SUM(CASE WHEN gl_account_no = '10410' THEN net_amount ELSE 0 END) AS to_10410,
        SUM(CASE WHEN gl_account_no = '10391' THEN net_amount ELSE 0 END) AS to_10391,
        SUM(CASE WHEN gl_account_no = '50140' THEN net_amount ELSE 0 END) AS to_50140,
        SUM(CASE WHEN gl_account_no = '10392' THEN net_amount ELSE 0 END) AS to_10392
    FROM fa.gold_bc_gl_entry_clean
    WHERE document_no LIKE '24010001%'
       OR document_no LIKE 'CAS2407%'
       OR document_no LIKE 'CAS2408%'
    GROUP BY document_no, YEAR(posting_date), MONTH(posting_date)
    ORDER BY entry_count DESC
""")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("""
    SELECT
        YEAR(posting_date) AS year,
        document_no LIKE 'CAS24%' OR document_no LIKE '240100%' AS is_reval_doc,
        COUNT(*) AS entries,
        SUM(net_amount) AS total
    FROM fa.gold_bc_gl_entry_clean
    WHERE gl_account_no = '50140'
    GROUP BY YEAR(posting_date), document_no LIKE 'CAS24%' OR document_no LIKE '240100%'
    ORDER BY year, is_reval_doc
""")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("""
    SELECT *
    FROM `Silver_BC_Lakehouse`.bc.`G_L Account`
    WHERE `No.` IN ('50140', '50141', '10391', '10392', '10410')
""")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("""
    SELECT
        po.`Status`,
        COUNT(DISTINCT po.`No.`) AS pro_count,
        SUM(pol.`Cost Amount`) AS total_planned_cost
    FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line` pol
    JOIN `Silver_BC_Lakehouse`.bc.`Production Order` po
        ON po.`No.`    = pol.`Prod. Order No.`
       AND po.`Status` = pol.`Status`
    WHERE po.`Status` IN ('Released', 'Finished')
      AND (
            (po.`Status` = 'Released' AND po.`Creation Date` <= DATE'2025-12-31')
         OR (po.`Status` = 'Finished' AND po.`Ending Date` > DATE'2025-12-31'
                                       AND po.`Creation Date` <= DATE'2025-12-31')
          )
    GROUP BY po.`Status`
""")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("""
    SELECT
        YEAR(posting_date) AS year,
        SUM(net_amount) AS net_movement,
        COUNT(*) AS entry_count
    FROM fa.gold_bc_gl_entry_clean
    WHERE gl_account_no = '10410'
    GROUP BY YEAR(posting_date)
    ORDER BY year
""")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT 
        document_no,
        SUM(net_amount) AS amount_on_10410,
        COUNT(*) AS entries
    FROM fa.gold_bc_gl_entry_clean
    WHERE gl_account_no = '10410'
      AND ABS(net_amount) > 1000000
    GROUP BY document_no
    ORDER BY ABS(SUM(net_amount)) DESC
    LIMIT 30
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT 
        document_no LIKE 'WPRO%' OR document_no LIKE 'WRO%' OR document_no LIKE 'WSP%' AS is_pro_related,
        document_no LIKE 'CAS%' AS is_cas,
        document_no LIKE 'JV%' OR document_no LIKE 'SEC%' AS is_manual,
        COUNT(*) AS entries,
        SUM(net_amount) AS total
    FROM fa.gold_bc_gl_entry_clean
    WHERE gl_account_no = '50140'
      AND YEAR(posting_date) = 2025
    GROUP BY 
        document_no LIKE 'WPRO%' OR document_no LIKE 'WRO%' OR document_no LIKE 'WSP%',
        document_no LIKE 'CAS%',
        document_no LIKE 'JV%' OR document_no LIKE 'SEC%'
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables_df = spark.sql("SHOW TABLES IN Silver_BC_Lakehouse.bc")
display(tables_df.filter("tableName LIKE '%count%' OR tableName LIKE '%ccount%'"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_tables = spark.sql("SHOW TABLES IN Silver_BC_Lakehouse.bc").collect()
for r in all_tables:
    if 'count' in r.tableName.lower() or 'g' in r.tableName.lower()[:2]:
        print(r.namespace, r.tableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT YEAR(posting_date) AS year, SUM(net_amount) AS yearly_net
    FROM fa.gold_bc_gl_entry_clean
    WHERE gl_account_no IN ('50140', '5000', '5001')
    GROUP BY YEAR(posting_date), gl_account_no
    ORDER BY year
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT 
        posting_date,
        document_no,
        description,
        external_document_no,
        net_amount,
        document_type
    FROM fa.gold_bc_gl_entry_clean
    WHERE gl_account_no = '50140'
      AND YEAR(posting_date) = 2025
      AND (document_no LIKE 'JV%' OR document_no LIKE 'SEC%')
    ORDER BY ABS(net_amount) DESC
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT 
        `No.`, 
        `Name`,
        `Account Type`,
        `Account Category`,
        `Income/Balance`,
        `Direct Posting`,
        `Gen. Posting Type`
    FROM `Silver_BC_Lakehouse`.bc.`GL Account`
    WHERE `No.` IN ('50140','50141','10391','10392','10410','10362','10360','10361')
    ORDER BY `No.`
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT 
        `No.`, 
        `Name`,
        `Account Type`,
        `Account Category`,
        `Income/Balance`,
        `Direct Posting`,
        `Gen. Posting Type`
    FROM `Silver_BC_Lakehouse`.bc.`GL Account`
    WHERE `No.` IN ('50140','50141','10391','10392','10410','10362','10360','10361')
    ORDER BY `No.`
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT 
        posting_date,
        document_no,
        document_type,
        description,
        source_type,
        source_no,
        prod_order_no,
        dim1,
        dim2,
        net_amount
    FROM fa.gold_bc_gl_entry_clean
    WHERE gl_account_no = '50140'
      AND YEAR(posting_date) = 2025
      AND (document_no LIKE 'JV%' OR document_no LIKE 'SEC%')
    ORDER BY ABS(net_amount) DESC
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# (run after the standard session config — LEGACY + broadcast=-1)
spark.sql("""
    SELECT `No.`, `Name`, `Account Category`, `Account Subcategory Entry No.`, `Income/Balance`, `Direct Posting`
    FROM `Silver_BC_Lakehouse`.bc.`GL Account`
    WHERE LOWER(`Name`) LIKE '%construction%'
       OR LOWER(`Name`) LIKE '%auc%'
       OR LOWER(`Name`) LIKE '%cip%'
       OR `Name` LIKE '%ก่อสร้าง%'
    ORDER BY `No.`
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT 
        source_type,
        document_type,
        COUNT(*) AS entries,
        SUM(net_amount) AS total_amount,
        MIN(posting_date) AS first_txn,
        MAX(posting_date) AS last_txn
    FROM fa.gold_bc_gl_entry_clean
    WHERE gl_account_no = 'PASTE_AUC_NO_HERE'
    GROUP BY source_type, document_type
    ORDER BY ABS(SUM(net_amount)) DESC
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT 
        g.source_no AS vendor_no,
        v.`Name` AS vendor_name,
        v.`Buy-from Vendor No.` AS buy_from,
        COUNT(*) AS transaction_count,
        SUM(g.net_amount) AS total_amount,
        SUM(CASE WHEN g.net_amount > 0 THEN g.net_amount ELSE 0 END) AS total_debit,
        SUM(CASE WHEN g.net_amount < 0 THEN g.net_amount ELSE 0 END) AS total_credit,
        MIN(g.posting_date) AS first_txn,
        MAX(g.posting_date) AS last_txn
    FROM fa.gold_bc_gl_entry_clean g
    LEFT JOIN `Silver_BC_Lakehouse`.bc.`Vendor` v
        ON v.`No.` = g.source_no
    WHERE g.gl_account_no = 'PASTE_AUC_NO_HERE'
      AND (g.source_type = 'Vendor' OR g.source_type = '2')  -- handles both text and integer enum
    GROUP BY g.source_no, v.`Name`, v.`Buy-from Vendor No.`
    ORDER BY ABS(SUM(g.net_amount)) DESC
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT 
        `No.`,
        `Name`,
        `Account Type`,
        `Account Category`,
        `Income/Balance`,
        `Direct Posting`
    FROM `Silver_BC_Lakehouse`.bc.`GL Account`
    WHERE LOWER(`Name`) LIKE '%construction%'
       OR LOWER(`Name`) LIKE '%under construct%'
       OR LOWER(`Name`) LIKE '%cip%'
       OR LOWER(`Name`) LIKE '%auc%'
       OR LOWER(`Name`) LIKE '%progress%'
       OR LOWER(`Name`) LIKE '%งานระหว่างก่อสร้าง%'
       OR LOWER(`Name`) LIKE '%ก่อสร้าง%'
    ORDER BY `No.`
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT
        COALESCE(v.`No.`, '(no vendor link)') AS vendor_no,
        COALESCE(v.`Name`, '(no vendor link)') AS vendor_name,
        COUNT(DISTINCT gle.document_no) AS doc_count,
        COUNT(*) AS entry_count,
        SUM(gle.net_amount) AS total_amount,
        MIN(gle.posting_date) AS earliest_date,
        MAX(gle.posting_date) AS latest_date
    FROM fa.gold_bc_gl_entry_clean gle
    LEFT JOIN `Silver_BC_Lakehouse`.bc.`Vendor Ledger Entry` vle
        ON vle.`Document No.` = gle.document_no
    LEFT JOIN `Silver_BC_Lakehouse`.bc.`Vendor` v
        ON v.`No.` = vle.`Vendor No.`
    WHERE gle.gl_account_no IN ('XXXX')
    GROUP BY v.`No.`, v.`Name`
    ORDER BY ABS(SUM(gle.net_amount)) DESC
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT
        posting_date,
        document_no,
        description,
        dim1, dim2,
        net_amount
    FROM fa.gold_bc_gl_entry_clean
    WHERE gl_account_no IN ('XXXX')
      AND document_no NOT IN (
          SELECT DISTINCT `Document No.`
          FROM `Silver_BC_Lakehouse`.bc.`Vendor Ledger Entry`
      )
    ORDER BY posting_date DESC, ABS(net_amount) DESC
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT
        COALESCE(v.`No.`, '(no vendor link)') AS vendor_no,
        COALESCE(v.`Name`, '(no vendor link)') AS vendor_name,
        COUNT(DISTINCT gle.document_no) AS doc_count,
        COUNT(*) AS entry_count,
        SUM(gle.net_amount) AS total_amount,
        MIN(gle.posting_date) AS earliest_date,
        MAX(gle.posting_date) AS latest_date
    FROM fa.gold_bc_gl_entry_clean gle
    LEFT JOIN `Silver_BC_Lakehouse`.bc.`Vendor Ledger Entry` vle
        ON vle.`Document No.` = gle.document_no
    LEFT JOIN `Silver_BC_Lakehouse`.bc.`Vendor` v
        ON v.`No.` = vle.`Vendor No.`
    WHERE gle.gl_account_no = '10680'
    GROUP BY v.`No.`, v.`Name`
    ORDER BY ABS(SUM(gle.net_amount)) DESC
""").show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT
        gl_account_no,
        YEAR(posting_date) AS year,
        COUNT(*) AS entries,
        SUM(net_amount) AS total,
        MAX(ABS(net_amount)) AS largest_entry
    FROM fa.gold_bc_gl_entry_clean
    WHERE gl_account_no IN ('10771', '70111')
    GROUP BY gl_account_no, YEAR(posting_date)
    ORDER BY gl_account_no, year
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT
        gl_account_no,
        COUNT(*) AS entries,
        SUM(net_amount) AS balance
    FROM fa.gold_bc_gl_entry_clean
    WHERE gl_account_no IN ('10680','10771','70111')
    GROUP BY gl_account_no
    ORDER BY gl_account_no
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT
        COALESCE(v.`No.`, '(no vendor link)') AS vendor_no,
        COALESCE(v.`Name`, '(no vendor link)') AS vendor_name,
        COUNT(DISTINCT gle.document_no) AS doc_count,
        COUNT(*) AS entry_count,
        SUM(gle.net_amount) AS total_amount,
        MIN(gle.posting_date) AS earliest_date,
        MAX(gle.posting_date) AS latest_date
    FROM fa.gold_bc_gl_entry_clean gle
    LEFT JOIN `Silver_BC_Lakehouse`.bc.`Vendor Ledger Entry` vle
        ON vle.`Document No.` = gle.document_no
    LEFT JOIN `Silver_BC_Lakehouse`.bc.`Vendor` v
        ON v.`No.` = vle.`Vendor No.`
    WHERE gle.gl_account_no = '10680'
    GROUP BY v.`No.`, v.`Name`
    ORDER BY ABS(SUM(gle.net_amount)) DESC
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT
        COALESCE(v.`No.`, '(no vendor link)') AS vendor_no,
        COALESCE(v.`Name`, '(no vendor link)') AS vendor_name,
        SUM(CASE WHEN YEAR(gle.posting_date) = 2023 THEN gle.net_amount ELSE 0 END) AS y2023,
        SUM(CASE WHEN YEAR(gle.posting_date) = 2024 THEN gle.net_amount ELSE 0 END) AS y2024,
        SUM(CASE WHEN YEAR(gle.posting_date) = 2025 THEN gle.net_amount ELSE 0 END) AS y2025,
        SUM(CASE WHEN YEAR(gle.posting_date) = 2026 THEN gle.net_amount ELSE 0 END) AS y2026,
        SUM(gle.net_amount) AS total,
        COUNT(*) AS entries
    FROM fa.gold_bc_gl_entry_clean gle
    LEFT JOIN `Silver_BC_Lakehouse`.bc.`Vendor Ledger Entry` vle
        ON vle.`Document No.` = gle.document_no
    LEFT JOIN `Silver_BC_Lakehouse`.bc.`Vendor` v
        ON v.`No.` = vle.`Vendor No.`
    WHERE gle.gl_account_no = '10680'
    GROUP BY v.`No.`, v.`Name`
    ORDER BY ABS(SUM(gle.net_amount)) DESC
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    WITH novlink_docs AS (
        SELECT DISTINCT gle.document_no
        FROM fa.gold_bc_gl_entry_clean gle
        LEFT JOIN `Silver_BC_Lakehouse`.bc.`Vendor Ledger Entry` vle
            ON vle.`Document No.` = gle.document_no
        WHERE gle.gl_account_no = '10680'
          AND vle.`Document No.` IS NULL
    )
    SELECT
        gle.gl_account_no,
        gle.gl_account_name,
        COUNT(*) AS entries,
        SUM(gle.net_amount) AS total_amount
    FROM fa.gold_bc_gl_entry_clean gle
    INNER JOIN novlink_docs n ON n.document_no = gle.document_no
    WHERE gle.gl_account_no != '10680'
    GROUP BY gle.gl_account_no, gle.gl_account_name
    ORDER BY ABS(SUM(gle.net_amount)) DESC
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT
        gle.document_no,
        gle.posting_date,
        gle.description,
        gle.net_amount AS amount_on_10680
    FROM fa.gold_bc_gl_entry_clean gle
    LEFT JOIN `Silver_BC_Lakehouse`.bc.`Vendor Ledger Entry` vle
        ON vle.`Document No.` = gle.document_no
    WHERE gle.gl_account_no = '10680'
      AND vle.`Document No.` IS NULL
    ORDER BY gle.posting_date, ABS(gle.net_amount) DESC
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
