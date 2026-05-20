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
# nb_anomaly_investigation_v1.py
#
# Ennovie — Anomaly Drill-down Investigations
#
# Three independent investigations into anomalies surfaced by close readiness
# and PRO variance work:
#
#   1. 2026 pair imbalance (+฿47M YTD, actively growing)
#      Goal: identify the posting source generating one-sided 10391/20380 entries
#
#   2. 2023-2024 gross volume inflation (495B WIP, 250B Inv Interim)
#      Goal: identify the document/series and the underlying inflation mechanism
#      Starting points: doc 24010001 (-฿64.8B), CAS2407-XXXX series, WRO240703XXX PROs
#
#   3. Negative Semi-Finished balances (-฿28M on 10362 brass)
#      Goal: identify CR-only postings that created the impossible negative balance
#
# Each cell is independent — run individually as needed.
# Each query saves a CSV to /lakehouse/default/Files/finance_reports/investigations
# so you can open in Excel for ad-hoc filtering.
# ============================================================================


# %% CELL 1 — PARAMETERS + IMPORTS -------------------------------------------
from datetime import datetime, date
from decimal import Decimal
from pyspark.sql import SparkSession
import pandas as pd
import os

spark = SparkSession.builder.getOrCreate()

silver_lakehouse = "Silver_BC_Lakehouse"
output_dir       = "/lakehouse/default/Files/finance_reports/investigations"
as_of_date       = date.today().isoformat()

# Account constants
ACC_INV_INTERIM = "10391"   # Inventory Interim
ACC_ACCRUAL     = "20380"   # Accrual Interim (pair with 10391)
ACC_COGS_INTRM  = "10392"   # COGS Interim
ACC_WIP         = "10410"   # WIP
ACC_SF_BRASS    = "10362"   # Semi-finished brass (negative balance)
ACC_SF_SILVER   = "10360"   # Semi-finished silver (also negative)
ACC_SF_GOLD     = "10361"   # Semi-finished gold (also negative)

os.makedirs(output_dir, exist_ok=True)
print(f"Investigations as of {as_of_date}")
print(f"Output dir: {output_dir}")


# %% CELL 2 — HELPERS --------------------------------------------------------
def run_sql(query):
    df = spark.sql(query).toPandas()
    for col in df.columns:
        if df[col].dtype == object:
            nn = df[col].dropna()
            if len(nn) > 0 and isinstance(nn.iloc[0], Decimal):
                df[col] = df[col].astype(float)
    return df


def save_csv(df, name):
    path = f"{output_dir}/{name}_{as_of_date}.csv"
    df.to_csv(path, index=False)
    return path


def fmt_thb(x):
    if pd.isna(x):
        return "—"
    return f"฿{x:,.2f}"


# %% CELL 3 — DISCOVER AVAILABLE COLUMNS -------------------------------------
# Confirm what's in gold_bc_gl_entry_clean before querying — some BC fields
# (source_code, user_id, document_type) may or may not be exposed in gold.

print("Columns in fa.gold_bc_gl_entry_clean:")
gl_cols = sorted(spark.table("fa.gold_bc_gl_entry_clean").columns)
for c in gl_cols:
    print(f"  - {c}")

# Note which optional columns are present (these would let us drill deeper)
print("\nOptional columns:")
for c in ["source_code", "source_type", "source_no", "user_id",
          "document_type", "description", "gen_bus_posting_group",
          "gen_prod_posting_group", "reason_code", "ile_quantity"]:
    print(f"  {'✓' if c in gl_cols else '✗'} {c}")


# ════════════════════════════════════════════════════════════════════════════
# INVESTIGATION 1 — 2026 PAIR IMBALANCE ON 10391/20380
# ════════════════════════════════════════════════════════════════════════════


# %% CELL 4 — Q1A: Top 100 unmatched 2026 documents -------------------------
# The biggest unmatched pair documents to investigate first. Each represents
# a posting event where 10391 DR and 20380 CR didn't balance.

q1a = f"""
SELECT
    document_no,
    MIN(posting_date) AS posting_date,
    SUM(CASE WHEN gl_account_no = '{ACC_INV_INTERIM}' THEN net_amount ELSE 0 END) AS amount_10391,
    SUM(CASE WHEN gl_account_no = '{ACC_ACCRUAL}'    THEN net_amount ELSE 0 END) AS amount_20380,
    SUM(net_amount) AS pair_net,
    SUM(CASE WHEN gl_account_no = '{ACC_INV_INTERIM}' THEN 1 ELSE 0 END) AS legs_10391,
    SUM(CASE WHEN gl_account_no = '{ACC_ACCRUAL}'    THEN 1 ELSE 0 END) AS legs_20380
FROM fa.gold_bc_gl_entry_clean
WHERE gl_account_no IN ('{ACC_INV_INTERIM}', '{ACC_ACCRUAL}')
  AND YEAR(posting_date) = 2026
GROUP BY document_no
HAVING ABS(SUM(net_amount)) > 0.01
ORDER BY ABS(pair_net) DESC
LIMIT 100
"""
df_q1a = run_sql(q1a)
print(f"\n=== Q1A: {len(df_q1a)} unmatched 2026 docs ===")
print(f"Top 10:")
print(df_q1a.head(10).to_string())
save_csv(df_q1a, "1a_2026_unmatched_pair_docs")


# %% CELL 5 — Q1B: 2026 imbalance grouped by document prefix ----------------
# Document number prefixes tell you the posting routine. Common BC prefixes:
#   POI / POSI = Purchase Invoice
#   POR / GR   = Purchase Receipt / Goods Receipt
#   PCR        = Purchase Credit Memo
#   SIN        = Sales Invoice
#   JNL / GJ   = General Journal
#   ITJ        = Item Journal
#   CAS        = Casting (Ennovie-specific)
#   WRO        = Work Order
#   WSP        = Wax / Sample (Ennovie-specific)

q1b = f"""
SELECT
    SUBSTRING(document_no, 1, 5) AS doc_prefix,
    COUNT(DISTINCT document_no) AS distinct_docs,
    SUM(CASE WHEN gl_account_no = '{ACC_INV_INTERIM}' THEN net_amount ELSE 0 END) AS amount_10391,
    SUM(CASE WHEN gl_account_no = '{ACC_ACCRUAL}'    THEN net_amount ELSE 0 END) AS amount_20380,
    SUM(net_amount) AS pair_net,
    SUM(ABS(net_amount)) AS gross_activity
FROM fa.gold_bc_gl_entry_clean
WHERE gl_account_no IN ('{ACC_INV_INTERIM}', '{ACC_ACCRUAL}')
  AND YEAR(posting_date) = 2026
GROUP BY SUBSTRING(document_no, 1, 5)
HAVING ABS(SUM(net_amount)) > 100
ORDER BY ABS(pair_net) DESC
"""
df_q1b = run_sql(q1b)
print(f"\n=== Q1B: 2026 pair imbalance by document prefix ===")
print(df_q1b.to_string())
save_csv(df_q1b, "1b_2026_pair_by_prefix")
print("\n>>> If one prefix dominates, that's the posting routine to investigate.")


# %% CELL 6 — Q1C: One-sided documents (hit ONLY 10391 OR ONLY 20380) -------
# Cleanest signal: documents that should have hit both accounts but only hit one.
# These are unambiguous broken-pair posting events.

q1c = f"""
SELECT
    document_no,
    MIN(posting_date) AS posting_date,
    SUM(CASE WHEN gl_account_no = '{ACC_INV_INTERIM}' THEN net_amount ELSE 0 END) AS amount_10391,
    SUM(CASE WHEN gl_account_no = '{ACC_ACCRUAL}'    THEN net_amount ELSE 0 END) AS amount_20380,
    CASE
        WHEN SUM(CASE WHEN gl_account_no = '{ACC_INV_INTERIM}' THEN 1 ELSE 0 END) = 0
            THEN 'Only 20380 (missing 10391)'
        WHEN SUM(CASE WHEN gl_account_no = '{ACC_ACCRUAL}'    THEN 1 ELSE 0 END) = 0
            THEN 'Only 10391 (missing 20380)'
    END AS missing_side
FROM fa.gold_bc_gl_entry_clean
WHERE gl_account_no IN ('{ACC_INV_INTERIM}', '{ACC_ACCRUAL}')
  AND YEAR(posting_date) = 2026
GROUP BY document_no
HAVING (SUM(CASE WHEN gl_account_no = '{ACC_INV_INTERIM}' THEN 1 ELSE 0 END) = 0
     OR SUM(CASE WHEN gl_account_no = '{ACC_ACCRUAL}'    THEN 1 ELSE 0 END) = 0)
ORDER BY ABS(amount_10391 + amount_20380) DESC
LIMIT 100
"""
df_q1c = run_sql(q1c)
print(f"\n=== Q1C: {len(df_q1c)} one-sided 2026 docs ===")
print(df_q1c.head(20).to_string())
save_csv(df_q1c, "1c_2026_onesided_docs")


# %% CELL 7 — Q1D: Monthly pattern — when did 2026 imbalance start? ---------
# 2025 was clean (pair net ≈ ฿1.9M). 2026 has +฿47M. Which months?

q1d = f"""
SELECT
    YEAR(posting_date) AS year,
    MONTH(posting_date) AS month,
    SUM(CASE WHEN gl_account_no = '{ACC_INV_INTERIM}' THEN net_amount ELSE 0 END) AS amount_10391,
    SUM(CASE WHEN gl_account_no = '{ACC_ACCRUAL}'    THEN net_amount ELSE 0 END) AS amount_20380,
    SUM(net_amount) AS pair_net,
    COUNT(DISTINCT document_no) AS docs
FROM fa.gold_bc_gl_entry_clean
WHERE gl_account_no IN ('{ACC_INV_INTERIM}', '{ACC_ACCRUAL}')
  AND YEAR(posting_date) IN (2025, 2026)
GROUP BY YEAR(posting_date), MONTH(posting_date)
ORDER BY year, month
"""
df_q1d = run_sql(q1d)
print(f"\n=== Q1D: Monthly pair pattern 2025–2026 ===")
print(df_q1d.to_string())
save_csv(df_q1d, "1d_pair_monthly_2025_2026")
print("\n>>> Look for the FIRST month where pair_net jumps materially — that's when the issue started.")


# ════════════════════════════════════════════════════════════════════════════
# INVESTIGATION 2 — 2023-2024 INFLATION SOURCE
# ════════════════════════════════════════════════════════════════════════════


# %% CELL 8 — Q2A: Full dissection of document 24010001 (-฿64.8B net) -------
# The largest unmatched document. Shows every GL leg, every account it hit.

q2a = """
SELECT
    posting_date,
    gl_account_no,
    net_amount,
    document_no,
    dim1, dim2,
    prod_order_no
FROM fa.gold_bc_gl_entry_clean
WHERE document_no = '24010001'
ORDER BY net_amount DESC
"""
df_q2a = run_sql(q2a)
print(f"\n=== Q2A: Document 24010001 has {len(df_q2a)} GL legs ===")
print(df_q2a.to_string())
save_csv(df_q2a, "2a_doc_24010001_dissect")

# What accounts did it hit, and net per account?
if len(df_q2a) > 0:
    print("\nSummary by account:")
    agg = df_q2a.groupby('gl_account_no').agg(
        net_amount=('net_amount', 'sum'),
        legs=('net_amount', 'count'),
        max_leg=('net_amount', 'max'),
        min_leg=('net_amount', 'min')
    ).sort_values('net_amount', key=lambda s: s.abs(), ascending=False)
    print(agg.to_string())


# %% CELL 9 — Q2B: CAS2407 series — what accounts did they hit? -------------
# 30+ casting documents from July 2024, billions per document. Find pattern.

q2b = """
SELECT
    document_no,
    gl_account_no,
    SUM(net_amount) AS total_net,
    COUNT(*) AS legs,
    MIN(posting_date) AS first_date,
    MAX(posting_date) AS last_date
FROM fa.gold_bc_gl_entry_clean
WHERE document_no LIKE 'CAS2407-%'
GROUP BY document_no, gl_account_no
ORDER BY document_no, ABS(total_net) DESC
"""
df_q2b = run_sql(q2b)
print(f"\n=== Q2B: CAS2407 series ===")
print(f"  {df_q2b['document_no'].nunique()} documents hitting {df_q2b['gl_account_no'].nunique()} distinct GL accounts")
print(f"\nFirst few document-account combinations:")
print(df_q2b.head(20).to_string())
save_csv(df_q2b, "2b_cas2407_account_breakdown")

# Account-level aggregate
print("\nAccount-level totals for the CAS2407 series:")
print(df_q2b.groupby('gl_account_no').agg(
    total_net=('total_net', 'sum'),
    docs_hitting=('document_no', 'nunique'),
).sort_values('total_net', key=lambda s: s.abs(), ascending=False).head(15).to_string())


# %% CELL 10 — Q2C: Inspect WRO240304038  components from BC Silver ---------
# This PRO had ฿38B planned cost in our variance v1. Look at the underlying
# quantity / unit cost / UOM fields. Will reveal whether the inflation is in
# Quantity (UOM conversion issue) or Unit Cost (pricing issue).

q2c = f"""
SELECT
    `Item No.`,
    `Description`,
    `Quantity`,
    `Expected Quantity`,
    `Unit of Measure Code`,
    `Qty. per Unit of Measure`,
    `Quantity per`,
    `Unit Cost`,
    `Direct Unit Cost`,
    `Cost Amount`,
    `Direct Cost Amount`,
    `Length`, `Width`, `Weight`, `Depth`,
    `Status`,
    `Line No.`
FROM `{silver_lakehouse}`.bc.`Prod Order Component`
WHERE `Prod. Order No.` = 'WRO240304038 '
ORDER BY `Line No.`
"""
df_q2c = run_sql(q2c)
print(f"\n=== Q2C: WRO240304038  components ({len(df_q2c)} lines) ===")
print(df_q2c.to_string())
save_csv(df_q2c, "2c_WRO240304038 _components")
print("\n>>> Look at the most expensive line(s). If `Direct Cost Amount` >> `Quantity × Direct Unit Cost`,")
print("    there's a calculation/posting error. If `Quantity` itself is absurd (e.g., a piece weighing")
print("    1,000,000 g), that's a UOM conversion bug at PRO refresh.")


# %% CELL 11 — Q2D: All 2023-2024 mega documents on 10391/10410 ------------
# Cast wider net beyond 24010001 and CAS2407-XXXX. Any other large anomalies?

q2d = f"""
SELECT
    document_no,
    MIN(posting_date) AS posting_date,
    SUM(CASE WHEN gl_account_no = '{ACC_INV_INTERIM}' THEN net_amount ELSE 0 END) AS amount_10391,
    SUM(CASE WHEN gl_account_no = '{ACC_WIP}'        THEN net_amount ELSE 0 END) AS amount_10410,
    SUM(ABS(net_amount)) AS gross_activity,
    COUNT(*) AS legs
FROM fa.gold_bc_gl_entry_clean
WHERE gl_account_no IN ('{ACC_INV_INTERIM}', '{ACC_WIP}')
  AND YEAR(posting_date) IN (2023, 2024)
GROUP BY document_no
HAVING SUM(ABS(net_amount)) > 100000000   -- > 100M gross
ORDER BY gross_activity DESC
LIMIT 100
"""
df_q2d = run_sql(q2d)
print(f"\n=== Q2D: {len(df_q2d)} mega-documents in 2023-2024 (>฿100M gross) ===")
print(df_q2d.head(30).to_string())
save_csv(df_q2d, "2d_2023_2024_mega_documents")

# Prefix breakdown
if len(df_q2d) > 0:
    df_q2d['prefix'] = df_q2d['document_no'].str[:6]
    print("\nMega-doc prefixes:")
    prefix_agg = df_q2d.groupby('prefix').agg(
        docs=('document_no', 'count'),
        gross_total=('gross_activity', 'sum'),
    ).sort_values('gross_total', ascending=False)
    print(prefix_agg.to_string())


# ════════════════════════════════════════════════════════════════════════════
# INVESTIGATION 3 — NEGATIVE SEMI-FINISHED BALANCES
# ════════════════════════════════════════════════════════════════════════════


# %% CELL 12 — Q3A: Yearly DR/CR on all three negative SF accounts ---------
# Shows when each negative balance accumulated.

q3a = f"""
SELECT
    gl_account_no,
    CASE gl_account_no
        WHEN '{ACC_SF_SILVER}' THEN 'SF Silver'
        WHEN '{ACC_SF_GOLD}'   THEN 'SF Gold'
        WHEN '{ACC_SF_BRASS}'  THEN 'SF Brass'
    END AS name,
    YEAR(posting_date) AS year,
    SUM(CASE WHEN net_amount > 0 THEN net_amount ELSE 0 END) AS dr_total,
    SUM(CASE WHEN net_amount < 0 THEN net_amount ELSE 0 END) AS cr_total,
    SUM(net_amount) AS net,
    COUNT(*) AS entries
FROM fa.gold_bc_gl_entry_clean
WHERE gl_account_no IN ('{ACC_SF_SILVER}', '{ACC_SF_GOLD}', '{ACC_SF_BRASS}')
GROUP BY gl_account_no, YEAR(posting_date)
ORDER BY gl_account_no, year
"""
df_q3a = run_sql(q3a)
print(f"\n=== Q3A: Semi-finished accounts yearly DR/CR ===")
print(df_q3a.to_string())
save_csv(df_q3a, "3a_negative_sf_yearly")
print("\n>>> Look for years where CR substantially exceeded DR — those built the negative balance.")


# %% CELL 13 — Q3B: Top 50 CR-dominant documents on 10362 (brass) ----------
# These are documents that REMOVED value from semi-finished brass without
# matching DR additions — the source of the impossible -฿28M balance.

q3b = f"""
SELECT
    document_no,
    MIN(posting_date) AS posting_date,
    SUM(net_amount) AS amount_10362,
    COUNT(*) AS legs,
    MAX(prod_order_no) AS prod_order_no
FROM fa.gold_bc_gl_entry_clean
WHERE gl_account_no = '{ACC_SF_BRASS}'
GROUP BY document_no
HAVING SUM(net_amount) < -10000
ORDER BY amount_10362 ASC
LIMIT 50
"""
df_q3b = run_sql(q3b)
print(f"\n=== Q3B: Top 50 CR-heavy documents on 10362 (SF Brass) ===")
print(df_q3b.head(20).to_string())
save_csv(df_q3b, "3b_10362_top_cr_docs")


# %% CELL 14 — Q3C: What accounts pair with 10362 entries? -----------------
# When 10362 CR happens, what gets DR'd? This reveals the posting routine.
# If most pairs go to FG accounts → output postings (legitimate but maybe
# over-output). If pairs go to 50140 Inventory Adjmt → adjustment journals
# clearing residuals. If pairs go to other SF accounts → reclass entries.

q3c = f"""
WITH brass_docs AS (
    SELECT DISTINCT document_no
    FROM fa.gold_bc_gl_entry_clean
    WHERE gl_account_no = '{ACC_SF_BRASS}'
      AND net_amount < 0    -- CR side only
)
SELECT
    gle.gl_account_no AS paired_account,
    COUNT(DISTINCT gle.document_no) AS docs,
    SUM(gle.net_amount) AS total_paired_amount
FROM fa.gold_bc_gl_entry_clean gle
INNER JOIN brass_docs bd ON bd.document_no = gle.document_no
WHERE gle.gl_account_no != '{ACC_SF_BRASS}'
GROUP BY gle.gl_account_no
ORDER BY ABS(SUM(gle.net_amount)) DESC
LIMIT 30
"""
df_q3c = run_sql(q3c)
print(f"\n=== Q3C: Accounts paired with 10362 CR postings ===")
print(df_q3c.to_string())
save_csv(df_q3c, "3c_10362_paired_accounts")
print("\n>>> If 10372 (FG Brass) dominates the DR pair total, the issue is over-output:")
print("    more cost moved to FG than was ever consumed into SF. If 50140 (Inv Adjmt) shows up,")
print("    Adjust Cost was clearing residuals via the wrong account.")


# %% CELL 15 — SUMMARY OF INVESTIGATIONS -----------------------------------

print("\n" + "="*78)
print("INVESTIGATION SUMMARY")
print("="*78)
print(f"\nAll CSV outputs in: {output_dir}")
print(f"\nInvestigation 1 — 2026 PAIR IMBALANCE")
print(f"  Q1A: Top 100 unmatched 2026 docs → 1a_2026_unmatched_pair_docs_{as_of_date}.csv")
print(f"  Q1B: By document prefix              → 1b_2026_pair_by_prefix_{as_of_date}.csv")
print(f"  Q1C: One-sided 2026 docs             → 1c_2026_onesided_docs_{as_of_date}.csv")
print(f"  Q1D: Monthly pattern 2025-2026       → 1d_pair_monthly_2025_2026_{as_of_date}.csv")
print(f"\nInvestigation 2 — 2023-2024 INFLATION SOURCE")
print(f"  Q2A: Document 24010001 dissection    → 2a_doc_24010001_dissect_{as_of_date}.csv")
print(f"  Q2B: CAS2407 series account map      → 2b_cas2407_account_breakdown_{as_of_date}.csv")
print(f"  Q2C: WRO240304038  components         → 2c_WRO240304038 _components_{as_of_date}.csv")
print(f"  Q2D: 2023-2024 mega documents        → 2d_2023_2024_mega_documents_{as_of_date}.csv")
print(f"\nInvestigation 3 — NEGATIVE SEMI-FINISHED")
print(f"  Q3A: Yearly DR/CR on 10360/61/62     → 3a_negative_sf_yearly_{as_of_date}.csv")
print(f"  Q3B: Top CR docs on 10362            → 3b_10362_top_cr_docs_{as_of_date}.csv")
print(f"  Q3C: 10362 paired accounts           → 3c_10362_paired_accounts_{as_of_date}.csv")
print("\n" + "="*78)
print("\nNext steps after running:")
print("  1. Look at Q1B prefix breakdown — if one prefix dominates 2026 imbalance,")
print("     pull a sample document from that prefix in BC and inspect the source")
print("     code, user, and posting routine.")
print("  2. Look at Q2A — see what accounts doc 24010001 hit. If it's a migration")
print("     journal (large numbers across many accounts), that confirms BC go-live")
print("     opening balance posting.")
print("  3. Look at Q2C — find which component line in WRO240304038  has the")
print("     inflated cost. The mechanism (UOM vs unit cost vs quantity) will be")
print("     obvious from comparing Quantity × Direct Unit Cost = Direct Cost Amount.")
print("  4. Look at Q3C — which DR account pairs with 10362 CR entries. That's the")
print("     posting routine responsible for the negative balance.")

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
