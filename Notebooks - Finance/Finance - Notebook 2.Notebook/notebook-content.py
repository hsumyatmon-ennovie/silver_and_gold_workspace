# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1d620310-5acc-4534-93f9-f52f082a1887",
# META       "default_lakehouse_name": "Silver_BC_Lakehouse",
# META       "default_lakehouse_workspace_id": "d74457b3-045c-445d-82c6-9a2e4b9f1436",
# META       "known_lakehouses": [
# META         {
# META           "id": "1d620310-5acc-4534-93f9-f52f082a1887"
# META         },
# META         {
# META           "id": "b94fc278-235b-488b-a7da-0a783bf50307"
# META         }
# META       ]
# META     }
# META   }
# META }

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

# MARKDOWN ********************

# # Gold GL Entry Clean

# CELL ********************

# ============================================================
# FULL REFRESH: Clean G/L Entry (vw_bc_gl_entry_clean -> gold table)
# Source: Silver_BC_Lakehouse.bc.`GL Entry`
#
# Target table (edit if you want):
#   Gold_Finance_Lakehouse.fa.gold_bc_gl_entry_clean
# ============================================================

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT_TBL = "Gold_Finance_Lakehouse.fa.gold_bc_gl_entry_clean"

src_sql = """
SELECT
    ge.`BC Company`                  AS bc_company,
    ge.`Entry No.`                   AS gl_entry_no,
    CAST(ge.`Posting Date` AS date)  AS posting_date,
    ge.`G/L Account No.`             AS gl_account_no,
    ga.`Name`                        AS gl_account_name,
    CONCAT(ge.`G/L Account No.`, ' - ', ga.`Name`) AS gl_account_no_name,
    CAST(
        COALESCE(ge.`Amount`, ge.`Debit Amount` - ge.`Credit Amount`)
        AS DECIMAL(38,2)
    ) AS net_amount,
    ge.`Document Type`               AS document_type,
    ge.`Document No.`                AS document_no,
    ge.`Description`                 AS description,
    ge.`Source Code`                 AS source_code,
    ge.`Source Type`                 AS source_type,
    ge.`Source No.`                  AS source_no,
    ge.`Global Dimension 1 Code`     AS dim1,
    ge.`Global Dimension 2 Code`     AS dim2,
    ge.`Dimension Set ID`            AS dim_set_id,
    ge.`Prod. Order No.`             AS prod_order_no
FROM Silver_BC_Lakehouse.bc.`GL Entry` ge
LEFT JOIN Silver_BC_Lakehouse.bc.`GL Account` ga
    ON ge.`BC Company` = ga.`BC Company`
   AND ge.`G/L Account No.` = ga.`No.`
WHERE COALESCE(ge.`Reversed`, false) = false;

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

# # Gold BC Interim Lines Base

# CELL ********************

# ============================================================
# FULL REFRESH: Interim Lines Base (vw_bc_interim_lines_base -> gold table)
# ============================================================

from pyspark.sql import functions as F

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT_TBL = "Gold_Finance_Lakehouse.fa.gold_bc_interim_lines_base"

src_sql = """
WITH ve AS (
  SELECT
    ve.`BC Company`                         AS bc_company,
    ve.`Entry No.`                          AS ve_entry_no,
    CAST(ve.`Posting Date` AS date)         AS posting_date,
    ve.`Item No.`                           AS item_no,
    ve.`Location Code`                      AS location_code,
    ve.`Inventory Posting Group`            AS inv_posting_group,
    ve.`Item Ledger Entry No.`              AS ile_entry_no,
    ve.`Document Type`                      AS ve_document_type,
    ve.`Document No.`                       AS ve_document_no,
    ve.`Order Type`                         AS ve_order_type,
    ve.`Order No.`                          AS ve_order_no,
    ve.`Source Code`                        AS source_code,
    ve.`Source Type`                        AS source_type,
    ve.`Source No.`                         AS source_no,
    ve.`Dimension Set ID`                   AS dim_set_id,
    ve.`Global Dimension 1 Code`            AS dim1,
    ve.`Global Dimension 2 Code`            AS dim2,

    CAST(COALESCE(ve.`Cost Amount (Expected)`,0) AS DECIMAL(38,2)) AS cost_expected,
    CAST(COALESCE(ve.`Cost Amount (Actual)`,0)   AS DECIMAL(38,2)) AS cost_actual,

    CAST(
      COALESCE(ve.`Cost Amount (Expected)`,0) - COALESCE(ve.`Cost Amount (Actual)`,0)
      AS DECIMAL(38,2)
    ) AS interim_delta,

    COALESCE(ve.`Cost Posted to G/L`,0)           AS cost_posted_to_gl,
    COALESCE(ve.`Expected Cost Posted to G/L`,0)  AS exp_cost_posted_to_gl,

    COALESCE(ve.`Adjustment`, false)              AS is_adjustment
  FROM Silver_BC_Lakehouse.bc.`Value Entry` ve
),
ile AS (
  SELECT
    ile.`BC Company`                        AS bc_company,
    ile.`Entry No.`                         AS ile_entry_no,
    ile.`Document Type`                     AS ile_document_type,
    ile.`Document No.`                      AS ile_document_no,
    ile.`Entry Type`                        AS ile_entry_type,
    COALESCE(ile.`Completely Invoiced`,false) AS completely_invoiced,
    COALESCE(ile.`Open`,false)              AS ile_open
  FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` ile
),
ips AS (
  SELECT
    `BC Company`                  AS bc_company,
    `Location Code`               AS location_code,
    `Invt. Posting Group Code`    AS inv_posting_group,
    `Inventory Account (Interim)` AS inventory_interim_account,
    `WIP Account`                 AS wip_account
  FROM Silver_BC_Lakehouse.bc.`Inventory Posting Setup`
)
SELECT
  v.*,
  i.ile_document_type,
  i.ile_document_no,
  i.ile_entry_type,
  i.completely_invoiced,
  i.ile_open,
  p.inventory_interim_account,
  p.wip_account,

  CASE
    WHEN p.inventory_interim_account IS NULL THEN NULL
    WHEN i.ile_document_type IN ('Sales Shipment','Shipment') THEN '10392'
    ELSE p.inventory_interim_account
  END AS interim_gl_account_no

FROM ve v
LEFT JOIN ile i
  ON v.bc_company = i.bc_company
 AND v.ile_entry_no = i.ile_entry_no
LEFT JOIN ips p
  ON v.bc_company = p.bc_company
 AND v.location_code = p.location_code
 AND v.inv_posting_group = p.inv_posting_group
WHERE v.interim_delta <> 0
"""

df_src = spark.sql(src_sql)

# -------------------------------------------------
# DIM) GL Account lookup (Name, No - Name)
# -------------------------------------------------
gl_acct_dim = (
    spark.sql("""
        SELECT
            `BC Company` AS bc_company,
            `No.`        AS gl_account_no,
            `Name`       AS gl_account_name
        FROM Silver_BC_Lakehouse.bc.`GL Account`
    """)
    .dropDuplicates(["bc_company", "gl_account_no"])
    .withColumn("gl_account_no_name", F.concat(F.col("gl_account_no"), F.lit(" - "), F.col("gl_account_name")))
)

# -------------------------------------------------
# Add interim GL account name columns
# -------------------------------------------------
df_out = (
    df_src
    .join(
        gl_acct_dim.select(
            F.col("bc_company"),
            F.col("gl_account_no").alias("interim_gl_account_no"),
            F.col("gl_account_name").alias("interim_gl_account_name"),
            F.col("gl_account_no_name").alias("interim_gl_account_no_name"),
        ),
        on=["bc_company", "interim_gl_account_no"],
        how="left",
    )
)

(
    df_out.write
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

# # Gold Recon Inventory Interim WIP Summary

# CELL ********************

from pyspark.sql import functions as F

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

GL_TBL      = "Gold_Finance_Lakehouse.fa.gold_bc_gl_entry_clean"
INTERIM_TBL = "Gold_Finance_Lakehouse.fa.gold_bc_interim_lines_base"

OUT_SUMMARY        = "Gold_Finance_Lakehouse.fa.gold_recon_inventory_interim_wip_summary"
OUT_INTERIM_DETAIL = "Gold_Finance_Lakehouse.fa.gold_recon_inventory_interim_wip_interim_detail"
OUT_WIP_DETAIL     = "Gold_Finance_Lakehouse.fa.gold_recon_inventory_interim_wip_wip_detail"


def run_recon_inventory_interim_wip(include_details: bool = True):
    # ✅ As-of date = TODAY
    asof_col = F.current_date()

    # -------------------------------------------------
    # A) GL balances (10391 / 10392 / 10410)
    # -------------------------------------------------
    gl = (
        spark.table(GL_TBL)
        .filter(F.col("posting_date") <= asof_col)
        .filter(F.col("gl_account_no").isin("10391", "10392", "10410"))
        .groupBy("gl_account_no")
        .agg(F.sum("net_amount").alias("gl_balance"))
    )

    # -------------------------------------------------
    # B) Interim subledger (10391 / 10392)
    # -------------------------------------------------
    interim_lines = (
        spark.table(INTERIM_TBL)
        .filter(F.col("posting_date") <= asof_col)
        .filter(F.col("interim_gl_account_no").isin("10391", "10392"))
    )

    subledger_interim = (
        interim_lines
        .groupBy(F.col("interim_gl_account_no").alias("gl_account_no"))
        .agg(
            F.sum("interim_delta").alias("subledger_balance"),
            F.first("interim_gl_account_name", ignorenulls=True).alias("interim_gl_account_name"),
            F.first("interim_gl_account_no_name", ignorenulls=True).alias("interim_gl_account_no_name"),
        )
    )

    acct_dim = spark.createDataFrame([("10391",), ("10392",)], ["gl_account_no"])

    interim_per_acct = (
        acct_dim
        .join(gl, "gl_account_no", "left")
        .join(subledger_interim, "gl_account_no", "left")
        .select(
            F.lit("Interim Recon (Per Account + Pool)").alias("result_set"),
            F.col("gl_account_no"),
            F.col("interim_gl_account_name"),
            F.col("interim_gl_account_no_name"),
            F.coalesce(F.col("gl_balance"), F.lit(0)).alias("gl_balance"),
            F.coalesce(F.col("subledger_balance"), F.lit(0)).alias("subledger_balance"),
            (F.coalesce(F.col("gl_balance"), F.lit(0))
             - F.coalesce(F.col("subledger_balance"), F.lit(0))).alias("difference"),
            asof_col.alias("as_of_date"),
        )
    )

    interim_pool = (
        interim_per_acct
        .agg(
            F.lit("Interim Recon (Per Account + Pool)").alias("result_set"),
            F.lit("10391+10392").alias("gl_account_no"),
            F.lit("Pool (10391+10392)").alias("interim_gl_account_name"),
            F.lit("10391+10392 - Pool (10391+10392)").alias("interim_gl_account_no_name"),
            F.sum("gl_balance").alias("gl_balance"),
            F.sum("subledger_balance").alias("subledger_balance"),
            F.sum("difference").alias("difference"),
        )
        .withColumn("as_of_date", asof_col)
    )

    # -------------------------------------------------
    # C) WIP subledger (10410)
    # -------------------------------------------------
    wip_subledger = spark.sql("""
        SELECT
            SUM(CAST(COALESCE(ve.`Cost Amount (Actual)`,0) AS DECIMAL(38,2))) AS subledger_wip
        FROM Silver_BC_Lakehouse.bc.`Value Entry` ve
        JOIN Silver_BC_Lakehouse.bc.`Production Order` po
          ON ve.`BC Company` = po.`BC Company`
         AND ve.`Order No.`  = po.`No.`
        WHERE CAST(ve.`Posting Date` AS date) <= current_date()
          AND (CAST(po.`Finished Date` AS date) IS NULL
               OR CAST(po.`Finished Date` AS date) > current_date())
          AND COALESCE(ve.`Cost Amount (Actual)`,0) <> 0
    """)

    wip_gl = gl.filter(F.col("gl_account_no") == "10410")

    wip_recon = (
        wip_gl.crossJoin(wip_subledger)
        .select(
            F.lit("WIP Recon").alias("result_set"),
            F.lit("10410").alias("gl_account_no"),
            # keeping schema consistent; if you want name for 10410 too, tell me and I’ll add it
            F.lit(None).cast("string").alias("interim_gl_account_name"),
            F.lit(None).cast("string").alias("interim_gl_account_no_name"),
            F.coalesce(F.col("gl_balance"), F.lit(0)).alias("gl_balance"),
            F.coalesce(F.col("subledger_wip"), F.lit(0)).alias("subledger_balance"),
            (F.coalesce(F.col("gl_balance"), F.lit(0))
             - F.coalesce(F.col("subledger_wip"), F.lit(0))).alias("difference"),
            asof_col.alias("as_of_date"),
        )
    )

    # -------------------------------------------------
    # OUTPUT 1: SUMMARY
    # -------------------------------------------------
    summary = interim_per_acct.unionByName(interim_pool).unionByName(wip_recon)

    (
        summary.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(OUT_SUMMARY)
    )

    print(f"FULL REFRESH completed into: {OUT_SUMMARY}")

    # -------------------------------------------------
    # OPTIONAL DETAILS
    # -------------------------------------------------
    if include_details:
        interim_lines.createOrReplaceTempView("interim_lines_asof")

        interim_detail = spark.sql("""
            SELECT
                'Interim Detail' AS result_set,
                posting_date,
                interim_gl_account_no,
                interim_gl_account_name,
                interim_gl_account_no_name,
                item_no,
                location_code,
                inv_posting_group,
                ile_document_type,
                ile_document_no,
                ve_order_type,
                ve_order_no,
                cost_expected,
                cost_actual,
                interim_delta,
                DATEDIFF(current_date(), posting_date) AS aging_days,
                CASE
                    WHEN DATEDIFF(current_date(), posting_date) <= 7  THEN '0–7'
                    WHEN DATEDIFF(current_date(), posting_date) <= 30 THEN '8–30'
                    WHEN DATEDIFF(current_date(), posting_date) <= 60 THEN '31–60'
                    ELSE '60+'
                END AS aging_band,
                source_code,
                source_type,
                source_no,
                dim1,
                dim2,
                dim_set_id,
                current_date() AS as_of_date
            FROM interim_lines_asof
        """)

        interim_detail.write.mode("overwrite").saveAsTable(OUT_INTERIM_DETAIL)

        wip_detail = spark.sql("""
            SELECT
                'WIP Detail' AS result_set,
                CAST(ve.`Posting Date` AS date) AS posting_date,
                ve.`Order No.`  AS prod_order_no,
                po.`Status`     AS prod_status,
                CAST(po.`Finished Date` AS date) AS finished_date,
                ve.`Item No.`   AS item_no,
                ve.`Location Code` AS location_code,
                ve.`Inventory Posting Group` AS inv_posting_group,
                CAST(COALESCE(ve.`Cost Amount (Actual)`,0) AS DECIMAL(38,2)) AS cost_actual,
                ve.`Document Type` AS ve_document_type,
                ve.`Document No.`  AS ve_document_no,
                ve.`Source Code`   AS source_code,
                current_date() AS as_of_date
            FROM Silver_BC_Lakehouse.bc.`Value Entry` ve
            JOIN Silver_BC_Lakehouse.bc.`Production Order` po
              ON ve.`BC Company` = po.`BC Company`
             AND ve.`Order No.`  = po.`No.`
            WHERE CAST(ve.`Posting Date` AS date) <= current_date()
              AND (CAST(po.`Finished Date` AS date) IS NULL
                   OR CAST(po.`Finished Date` AS date) > current_date())
              AND COALESCE(ve.`Cost Amount (Actual)`,0) <> 0
        """)

        wip_detail.write.mode("overwrite").saveAsTable(OUT_WIP_DETAIL)


# 🚀 Run (no parameters needed)
run_recon_inventory_interim_wip(include_details=True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold PBI Interim Detail

# CELL ********************

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TARGET = "Gold_Finance_Lakehouse.fa.gold_pbi_interim_detail"
SRC    = "Gold_Finance_Lakehouse.fa.gold_bc_interim_lines_base"

src_sql = f"""
SELECT
    l.bc_company,
    l.posting_date,

    -- ✅ Aging Days Column
    datediff(current_date(), l.posting_date) AS aging,

    -- ✅ Band Bucket Column
    CASE
        WHEN datediff(current_date(), l.posting_date) BETWEEN 0 AND 7  THEN '0-7'
        WHEN datediff(current_date(), l.posting_date) BETWEEN 8 AND 30 THEN '8-30'
        WHEN datediff(current_date(), l.posting_date) BETWEEN 31 AND 60 THEN '31-60'
        WHEN datediff(current_date(), l.posting_date) BETWEEN 61 AND 90 THEN '61-90'
        ELSE '90+'
    END AS band,

    CASE
        WHEN l.ile_document_type IN ('Purchase Receipt','Receipt') THEN 'GRN'
        WHEN l.ile_document_type IN ('Sales Shipment','Shipment')  THEN 'SHIPMENT'
        WHEN l.ile_entry_type    IN ('Output')                     THEN 'OUTPUT'
        WHEN COALESCE(l.is_adjustment, false) = true               THEN 'ADJUSTMENT'
        WHEN l.ve_document_type LIKE '%Shipment%'                  THEN 'SHIPMENT'
        WHEN l.ve_document_type LIKE '%Receipt%'                   THEN 'GRN'
        WHEN l.ve_document_type LIKE '%Output%'                    THEN 'OUTPUT'
        WHEN l.ve_document_type LIKE '%Journal%'                   THEN 'ADJUSTMENT'
        ELSE 'OTHER'
    END AS doc_short,

    CASE
        WHEN l.ile_document_type IN ('Purchase Receipt','Receipt') THEN 1
        WHEN l.ile_document_type IN ('Sales Shipment','Shipment')  THEN 2
        WHEN l.ile_entry_type    IN ('Output')                     THEN 3
        WHEN COALESCE(l.is_adjustment, false) = true               THEN 4
        ELSE 9
    END AS doc_sort,

    COALESCE(NULLIF(l.ile_document_no,''), NULLIF(l.ve_document_no,'')) AS document_no_final,

    l.interim_gl_account_no,
    l.interim_gl_account_name,
    l.interim_gl_account_no_name,

    l.item_no,
    l.location_code,
    l.inv_posting_group,

    l.ile_document_type,
    l.ile_document_no,
    l.ile_entry_type,

    l.ve_document_type,
    l.ve_document_no,
    l.ve_order_type,
    l.ve_order_no,

    CASE
        WHEN l.ve_order_type IN ('Production','Prod. Order','Production Order') THEN l.ve_order_no
        ELSE NULL
    END AS order_no_display,

    l.cost_expected,
    l.cost_actual,
    l.interim_delta,

    l.cost_posted_to_gl,
    l.exp_cost_posted_to_gl,
    l.is_adjustment,
    l.completely_invoiced,
    l.ile_open,

    l.inventory_interim_account,
    l.wip_account,

    l.source_code,
    l.source_type,
    l.source_no,
    l.dim1,
    l.dim2,
    l.dim_set_id,

    CASE
        WHEN l.inventory_interim_account IS NULL THEN 'Posting setup missing'
        WHEN l.ile_document_type IN ('Purchase Receipt','Receipt')
             AND COALESCE(l.completely_invoiced, false) = false THEN 'GRN not invoiced'
        WHEN l.ile_document_type IN ('Sales Shipment','Shipment')
             AND COALESCE(l.completely_invoiced, false) = false THEN 'Shipment not invoiced'
        WHEN l.ile_entry_type IN ('Output')
             AND COALESCE(l.ile_open, false) = true THEN 'Output not finished'
        WHEN (COALESCE(l.cost_posted_to_gl, 0) = 0 OR COALESCE(l.exp_cost_posted_to_gl, 0) = 0) THEN 'Not posted to G/L'
        WHEN COALESCE(l.is_adjustment, false) = false THEN 'Cost not adjusted'
        ELSE 'Other'
    END AS root_cause

FROM {SRC} l
WHERE l.interim_gl_account_no IN ('10391','10392')
"""

df = spark.sql(src_sql)

(
    df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(TARGET)
)

print(f"FULL REFRESH completed: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold PBI WIP Detail

# CELL ********************

from pyspark.sql import functions as F

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TARGET = "Gold_Finance_Lakehouse.fa.gold_pbi_wip_detail"

src_sql = """
SELECT
    ve.`BC Company`                         AS bc_company,
    CAST(ve.`Posting Date` AS DATE)         AS posting_date,
    ve.`Order No.`                          AS prod_order_no,
    po.`Status`                             AS prod_status,
    CAST(po.`Finished Date` AS DATE)        AS finished_date,
    ve.`Item No.`                           AS item_no,
    ve.`Location Code`                      AS location_code,
    ve.`Inventory Posting Group`            AS inv_posting_group,
    CAST(COALESCE(ve.`Cost Amount (Actual)`,0) AS DECIMAL(38,2)) AS cost_actual,
    ve.`Document Type`                      AS ve_document_type,
    ve.`Document No.`                       AS ve_document_no,
    ve.`Source Code`                        AS source_code
FROM Silver_BC_Lakehouse.bc.`Value Entry` ve
JOIN Silver_BC_Lakehouse.bc.`Production Order` po
  ON ve.`BC Company` = po.`BC Company`
 AND ve.`Order No.`  = po.`No.`
WHERE COALESCE(ve.`Cost Amount (Actual)`,0) <> 0
"""

df = spark.sql(src_sql)

(
    df.write
      .format("delta")
      .mode("overwrite")          # 🔥 FULL REPLACE
      .option("overwriteSchema", "true")
      .saveAsTable(TARGET)
)

print(f"FULL REFRESH completed: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# ============================================================
# PARAMETERS
# ============================================================
DOC_NO = "GRN2511-0018"
ITEM_NO = "CO-BRU-000308"

# Optional: if you know company, set it to reduce noise
# BC_COMPANY = "YOUR_COMPANY"
BC_COMPANY = None

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# ============================================================
# 0) Load base tables (raw) and filter for the case
# ============================================================

ve_raw = spark.table("Silver_BC_Lakehouse.bc.`Value Entry`")
ile_raw = spark.table("Silver_BC_Lakehouse.bc.`Item Ledger Entry`")
ips_raw = spark.table("Silver_BC_Lakehouse.bc.`Inventory Posting Setup`")
gl_raw  = spark.table("Silver_BC_Lakehouse.bc.`GL Account`")

ve_case = (
    ve_raw
    .filter(F.col("`Document No.`") == DOC_NO)
    .filter(F.col("`Item No.`") == ITEM_NO)
)

if BC_COMPANY:
    ve_case = ve_case.filter(F.col("`BC Company`") == BC_COMPANY)

print("=== Value Entry rows for the case ===")
print("count =", ve_case.count())
display(
    ve_case.select(
        F.col("`BC Company`").alias("bc_company"),
        F.col("`Entry No.`").alias("ve_entry_no"),
        F.col("`Posting Date`").alias("posting_date"),
        F.col("`Document Type`").alias("ve_document_type"),
        F.col("`Document No.`").alias("ve_document_no"),
        F.col("`Item No.`").alias("item_no"),
        F.col("`Location Code`").alias("location_code"),
        F.col("`Inventory Posting Group`").alias("inv_posting_group"),
        F.col("`Item Ledger Entry No.`").alias("ile_entry_no"),
        F.col("`Cost Amount (Expected)`").alias("cost_expected_raw"),
        F.col("`Cost Amount (Actual)`").alias("cost_actual_raw"),
        F.col("`Cost Posted to G/L`").alias("cost_posted_to_gl_raw"),
        F.col("`Expected Cost Posted to G/L`").alias("exp_cost_posted_to_gl_raw"),
        F.col("`Adjustment`").alias("is_adjustment_raw"),
        F.col("`Source Code`").alias("source_code"),
        F.col("`Order Type`").alias("order_type"),
        F.col("`Order No.`").alias("order_no"),
    )
    .orderBy("ve_entry_no")
)

# ============================================================
# 1) Recompute interim_delta exactly like your view does
#    (and show which rows survive WHERE interim_delta <> 0)
# ============================================================

ve_case_calc = (
    ve_case
    .select(
        F.col("`BC Company`").alias("bc_company"),
        F.col("`Entry No.`").alias("ve_entry_no"),
        F.to_date(F.col("`Posting Date`")).alias("posting_date"),
        F.col("`Item No.`").alias("item_no"),
        F.col("`Location Code`").alias("location_code"),
        F.col("`Inventory Posting Group`").alias("inv_posting_group"),
        F.col("`Item Ledger Entry No.`").alias("ile_entry_no"),
        F.col("`Document Type`").alias("ve_document_type"),
        F.col("`Document No.`").alias("ve_document_no"),
        F.col("`Order Type`").alias("ve_order_type"),
        F.col("`Order No.`").alias("ve_order_no"),
        F.col("`Source Code`").alias("source_code"),
        F.col("`Source Type`").alias("source_type"),
        F.col("`Source No.`").alias("source_no"),
        F.col("`Dimension Set ID`").alias("dim_set_id"),
        F.col("`Global Dimension 1 Code`").alias("dim1"),
        F.col("`Global Dimension 2 Code`").alias("dim2"),
        F.coalesce(F.col("`Cost Amount (Expected)`"), F.lit(0)).cast("decimal(38,2)").alias("cost_expected"),
        F.coalesce(F.col("`Cost Amount (Actual)`"),   F.lit(0)).cast("decimal(38,2)").alias("cost_actual"),
        (
            F.coalesce(F.col("`Cost Amount (Expected)`"), F.lit(0)) -
            F.coalesce(F.col("`Cost Amount (Actual)`"),   F.lit(0))
        ).cast("decimal(38,2)").alias("interim_delta"),
        F.coalesce(F.col("`Cost Posted to G/L`"),          F.lit(0)).alias("cost_posted_to_gl"),
        F.coalesce(F.col("`Expected Cost Posted to G/L`"), F.lit(0)).alias("exp_cost_posted_to_gl"),
        F.coalesce(F.col("`Adjustment`"), F.lit(False)).alias("is_adjustment"),
    )
)

print("=== Value Entry rows after interim_delta calc ===")
display(
    ve_case_calc.select(
        "bc_company","ve_entry_no","posting_date","ve_document_type","ve_document_no",
        "item_no","location_code","inv_posting_group","ile_entry_no",
        "cost_expected","cost_actual","interim_delta",
        "cost_posted_to_gl","exp_cost_posted_to_gl","is_adjustment"
    ).orderBy("ve_entry_no")
)

ve_case_nonzero = ve_case_calc.filter(F.col("interim_delta") != 0)

print("=== Value Entry rows that pass WHERE interim_delta <> 0 ===")
print("count =", ve_case_nonzero.count())
display(ve_case_nonzero.orderBy("ve_entry_no"))

# If THIS count is already 2, then the 2 lines are coming from Value Entry itself.
# Still continue below to confirm joins do not multiply.

# ============================================================
# 2) Check uniqueness of join keys in Item Ledger Entry (ILE)
#    Your join: v.bc_company = i.bc_company AND v.ile_entry_no = i.ile_entry_no
# ============================================================

ile_case = (
    ile_raw
    .join(
        ve_case_nonzero.select("bc_company","ile_entry_no").dropDuplicates(),
        on=[
            ile_raw["`BC Company`"] == F.col("bc_company"),
            ile_raw["`Entry No.`"] == F.col("ile_entry_no"),
        ],
        how="inner"
    )
)

print("=== Matching ILE rows for the ile_entry_no(s) from VE ===")
print("count =", ile_case.count())
display(
    ile_case.select(
        F.col("`BC Company`").alias("bc_company"),
        F.col("`Entry No.`").alias("ile_entry_no"),
        F.col("`Document Type`").alias("ile_document_type"),
        F.col("`Document No.`").alias("ile_document_no"),
        F.col("`Entry Type`").alias("ile_entry_type"),
        F.col("`Completely Invoiced`").alias("completely_invoiced"),
        F.col("`Open`").alias("ile_open")
    ).orderBy("ile_entry_no")
)

# Uniqueness check: should be 1 row per (bc_company, ile_entry_no)
ile_dupes = (
    ile_case
    .groupBy(F.col("`BC Company`").alias("bc_company"), F.col("`Entry No.`").alias("ile_entry_no"))
    .count()
    .filter(F.col("count") > 1)
)

print("=== ILE duplicate key check (should be empty) ===")
print("dup groups =", ile_dupes.count())
display(ile_dupes)

# ============================================================
# 3) Check uniqueness of Inventory Posting Setup (IPS) join keys
#    Your join: (bc_company, location_code, inv_posting_group)
#    If IPS has duplicates for that key, it WILL multiply your rows.
# ============================================================

ips_case_keys = ve_case_nonzero.select("bc_company", "location_code", "inv_posting_group").dropDuplicates()

ips_case = (
    ips_raw
    .join(
        ips_case_keys,
        on=[
            ips_raw["`BC Company`"] == F.col("bc_company"),
            ips_raw["`Location Code`"] == F.col("location_code"),
            ips_raw["`Invt. Posting Group Code`"] == F.col("inv_posting_group"),
        ],
        how="inner"
    )
)

print("=== Matching IPS rows for the VE key(s) ===")
print("count =", ips_case.count())
display(
    ips_case.select(
        F.col("`BC Company`").alias("bc_company"),
        F.col("`Location Code`").alias("location_code"),
        F.col("`Invt. Posting Group Code`").alias("inv_posting_group"),
        F.col("`Inventory Account (Interim)`").alias("inventory_interim_account"),
        F.col("`WIP Account`").alias("wip_account"),
    )
)

ips_dupes = (
    ips_case
    .groupBy(
        F.col("`BC Company`").alias("bc_company"),
        F.col("`Location Code`").alias("location_code"),
        F.col("`Invt. Posting Group Code`").alias("inv_posting_group"),
    )
    .count()
    .filter(F.col("count") > 1)
)

print("=== IPS duplicate key check (if non-empty, this explains row multiplication) ===")
print("dup groups =", ips_dupes.count())
display(ips_dupes)

# ============================================================
# 4) Rebuild the EXACT joins for ONLY the case and see where rows double
# ============================================================

# Recreate ile projection used in your SQL
ile_proj = (
    ile_raw.select(
        F.col("`BC Company`").alias("bc_company"),
        F.col("`Entry No.`").alias("ile_entry_no"),
        F.col("`Document Type`").alias("ile_document_type"),
        F.col("`Document No.`").alias("ile_document_no"),
        F.col("`Entry Type`").alias("ile_entry_type"),
        F.coalesce(F.col("`Completely Invoiced`"), F.lit(False)).alias("completely_invoiced"),
        F.coalesce(F.col("`Open`"), F.lit(False)).alias("ile_open"),
    )
)

# Recreate ips projection used in your SQL
ips_proj = (
    ips_raw.select(
        F.col("`BC Company`").alias("bc_company"),
        F.col("`Location Code`").alias("location_code"),
        F.col("`Invt. Posting Group Code`").alias("inv_posting_group"),
        F.col("`Inventory Account (Interim)`").alias("inventory_interim_account"),
        F.col("`WIP Account`").alias("wip_account"),
    )
)

# Join step-by-step and count rows at each step
step0 = ve_case_nonzero.cache()
print("step0 (VE nonzero) rows =", step0.count())

step1 = (
    step0
    .join(ile_proj, on=["bc_company", "ile_entry_no"], how="left")
    .cache()
)
print("step1 (after ILE join) rows =", step1.count())

step2 = (
    step1
    .join(ips_proj, on=["bc_company", "location_code", "inv_posting_group"], how="left")
    .cache()
)
print("step2 (after IPS join) rows =", step2.count())

# Compute interim_gl_account_no exactly like your SQL
step3 = (
    step2
    .withColumn(
        "interim_gl_account_no",
        F.when(F.col("inventory_interim_account").isNull(), F.lit(None).cast("string"))
         .when(F.col("ile_document_type").isin("Sales Shipment", "Shipment"), F.lit("10392"))
         .otherwise(F.col("inventory_interim_account"))
    )
    .cache()
)
print("step3 (after interim_gl_account_no calc) rows =", step3.count())

print("=== Inspect final rows before GL dim join ===")
display(
    step3.select(
        "bc_company","ve_entry_no","ve_document_no","item_no","ile_entry_no",
        "location_code","inv_posting_group","interim_delta",
        "ile_document_type","ile_document_no","ile_entry_type",
        "inventory_interim_account","wip_account","interim_gl_account_no"
    ).orderBy("ve_entry_no")
)

# ============================================================
# 5) GL dim join duplication check
# ============================================================

gl_acct_dim = (
    gl_raw.select(
        F.col("`BC Company`").alias("bc_company"),
        F.col("`No.`").alias("gl_account_no"),
        F.col("`Name`").alias("gl_account_name"),
    )
    .dropDuplicates(["bc_company", "gl_account_no"])
    .withColumn("gl_account_no_name", F.concat(F.col("gl_account_no"), F.lit(" - "), F.col("gl_account_name")))
)

# ensure GL dim is unique per key
gl_dupes = (
    gl_acct_dim.groupBy("bc_company","gl_account_no").count().filter(F.col("count") > 1)
)
print("=== GL dim duplicate key check (should be empty due to dropDuplicates) ===")
print("dup groups =", gl_dupes.count())
display(gl_dupes)

final_case = (
    step3
    .join(
        gl_acct_dim.select(
            F.col("bc_company"),
            F.col("gl_account_no").alias("interim_gl_account_no"),
            F.col("gl_account_name").alias("interim_gl_account_name"),
            F.col("gl_account_no_name").alias("interim_gl_account_no_name"),
        ),
        on=["bc_company", "interim_gl_account_no"],
        how="left",
    )
)

print("final_case rows =", final_case.count())
display(
    final_case.select(
        "bc_company","ve_entry_no","ve_document_no","item_no","ile_entry_no",
        "interim_delta",
        "inventory_interim_account","interim_gl_account_no",
        "interim_gl_account_name","interim_gl_account_no_name"
    ).orderBy("ve_entry_no")
)

# ============================================================
# 6) QUICK DIAGNOSIS HELPERS (what usually causes 2 lines)
# ============================================================

print("=== Group VE by Document+Item to see distinct VE entry nos and ILE entry nos ===")
display(
    ve_case_nonzero
    .groupBy("bc_company","ve_document_no","item_no")
    .agg(
        F.count("*").alias("ve_rows_nonzero"),
        F.countDistinct("ve_entry_no").alias("distinct_ve_entry_no"),
        F.countDistinct("ile_entry_no").alias("distinct_ile_entry_no"),
        F.collect_set("ve_entry_no").alias("ve_entry_nos"),
        F.collect_set("ile_entry_no").alias("ile_entry_nos"),
        F.sum("interim_delta").alias("sum_interim_delta")
    )
)

print("=== Check IPS multiplicative risk: how many IPS rows match the VE keys? ===")
display(
    step1
    .groupBy("bc_company","location_code","inv_posting_group")
    .agg(
        F.count("*").alias("rows_before_ips_join"),
    )
)

display(
    ips_proj
    .join(
        ips_case_keys,
        on=["bc_company","location_code","inv_posting_group"],
        how="inner"
    )
    .groupBy("bc_company","location_code","inv_posting_group")
    .count()
    .orderBy(F.desc("count"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- ============================================================
# MAGIC -- Q1: Most recent adjustment activity, last 30 days
# MAGIC -- NOTE: is_adjustment is NULL in source → use inferred logic
# MAGIC --       Adjustment VE = cost_actual moved AND cost_expected reversed
# MAGIC -- ============================================================
# MAGIC SELECT
# MAGIC     CAST(MAX(posting_date) AS DATE)                              AS last_adjustment_date,
# MAGIC     DATEDIFF(day, MAX(posting_date), CAST(GETDATE() AS DATE))    AS days_since,
# MAGIC     COUNT(*)                                                     AS adjustment_ves_last_30d,
# MAGIC     SUM(ABS(cost_actual))                                        AS abs_value_moved_thb
# MAGIC FROM Gold_Finance_Lakehouse.fa.gold_bc_interim_lines_base
# MAGIC WHERE posting_date >= DATEADD(day, -30, CAST(GETDATE() AS DATE))
# MAGIC   AND ABS(cost_actual)   > 0.01
# MAGIC   AND ABS(cost_expected) > 0.01;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
