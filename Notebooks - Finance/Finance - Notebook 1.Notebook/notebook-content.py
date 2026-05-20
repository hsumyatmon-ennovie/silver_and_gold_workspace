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

# # Gold QGL40

# CELL ********************

from pyspark.sql import functions as F

TARGET = "Gold_Finance_Lakehouse.fa.gold_QGL40"
SRC = "Silver_BC_Lakehouse.bc.`GL Entry`"

# 1) Create target table if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    Posting_Date DATE,
    Document_No STRING,
    G_L_Account_No STRING,
    Description STRING,
    Amount DECIMAL(38,10),
    Source_No STRING,

    -- tracking
    SystemCreatedAt TIMESTAMP,
    SystemModifiedAt TIMESTAMP
)
USING DELTA
""")

# 2) Read source + business filter (FULL REPLACE)
src = (
    spark.table(SRC)
    .filter(
        (F.col("`G/L Account No.`").like("4%")) |
        (F.col("`G/L Account No.`").like("2%"))
    )
)

d38 = lambda c: F.col(c).cast("decimal(38,10)")

# 3) Select with backticks
gold_full = src.select(
    F.col("`Posting Date`").cast("date").alias("Posting_Date"),
    F.col("`Document No.`").alias("Document_No"),
    F.col("`G/L Account No.`").alias("G_L_Account_No"),
    F.col("`Description`").alias("Description"),
    d38("`Amount`").alias("Amount"),
    F.col("`Source No.`").alias("Source_No"),

    # tracking
    F.col("`SystemCreatedAt`").alias("SystemCreatedAt"),
    F.col("`SystemModifiedAt`").alias("SystemModifiedAt"),
)

# 4) FULL REPLACE write (overwrite table data)
(
    gold_full.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET)
)

print(f"FULL REPLACE completed into: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold GL No

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Finance_Lakehouse.fa.gold_Gl_no
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH gl_p AS (
# MAGIC   SELECT
# MAGIC     CAST(`G/L Account No.` AS STRING)                         AS gl_acc,
# MAGIC     `Document No.`                                            AS gl_doc,
# MAGIC     `Source No.`                                              AS gl_src,
# MAGIC     CAST(`Amount` AS DECIMAL(38,10))                          AS gl_amt,
# MAGIC     TO_DATE(`Posting Date`)                                   AS gl_post_date,
# MAGIC     CAST(`Global Dimension 2 Code` AS STRING)                 AS Global_Dimension_2_Code,
# MAGIC     CAST(`Gen. Prod. Posting Group` AS STRING)                AS Gen_Prod_Posting_Group
# MAGIC   FROM Silver_BC_Lakehouse.bc.`GL Entry`
# MAGIC   WHERE `G/L Account No.` LIKE '4%' OR `G/L Account No.` LIKE '2%'
# MAGIC ),
# MAGIC c_p AS (
# MAGIC   SELECT
# MAGIC     `No.`   AS cus_no,
# MAGIC     `Name`  AS cus_name
# MAGIC   FROM Silver_BC_Lakehouse.bc.`Customer`
# MAGIC ),
# MAGIC gl_c AS (
# MAGIC   SELECT
# MAGIC     g.*,
# MAGIC     c.cus_name
# MAGIC   FROM gl_p g
# MAGIC   LEFT JOIN c_p c
# MAGIC     ON g.gl_src = c.cus_no
# MAGIC ),
# MAGIC ar_p AS (
# MAGIC   SELECT
# MAGIC     TO_DATE(posting_date)                    AS ar_date,
# MAGIC     customer_name                            AS ar_customer_name,
# MAGIC     CAST(AR_amount AS DECIMAL(38,10))        AS AR_amount
# MAGIC   FROM Silver_Commons_Lakehouse.cmn.silver_Old_AR_SAP
# MAGIC ),
# MAGIC j AS (
# MAGIC   SELECT
# MAGIC     gc.gl_post_date,
# MAGIC     ap.ar_date,
# MAGIC     gc.gl_acc,
# MAGIC     gc.gl_doc,
# MAGIC     gc.gl_src,
# MAGIC     gc.gl_amt,
# MAGIC     gc.cus_name,
# MAGIC     gc.Global_Dimension_2_Code,
# MAGIC     gc.Gen_Prod_Posting_Group,
# MAGIC     ap.ar_customer_name,
# MAGIC     ap.AR_amount
# MAGIC   FROM gl_c gc
# MAGIC   FULL OUTER JOIN ar_p ap
# MAGIC     ON gc.gl_post_date = ap.ar_date
# MAGIC ),
# MAGIC -- ขั้นตอนเดิม: aggregate GL level
# MAGIC gl_agg AS (
# MAGIC   SELECT
# MAGIC     COALESCE(gl_post_date, ar_date)   AS GL_Date,
# MAGIC     gl_acc                            AS G_L_Account_No,
# MAGIC     gl_doc                            AS Document_No,
# MAGIC     gl_src                            AS Source_No,
# MAGIC     Global_Dimension_2_Code,
# MAGIC     Gen_Prod_Posting_Group,
# MAGIC  
# MAGIC     CAST(SUM(
# MAGIC       CASE
# MAGIC         WHEN gl_acc IN (
# MAGIC           '40110','40120','40130','40140',
# MAGIC           '40210','40220','40230','40240',
# MAGIC           '40310','40320','40330','40340',
# MAGIC           '40410','20352',
# MAGIC           '40510','40520',
# MAGIC           '40540'
# MAGIC         )
# MAGIC         THEN -gl_amt
# MAGIC         ELSE CAST(0 AS DECIMAL(38,10))
# MAGIC       END
# MAGIC     ) AS DECIMAL(38,10))              AS TotalGLAmount,
# MAGIC  
# MAGIC     CASE
# MAGIC       WHEN SIZE(COLLECT_SET(
# MAGIC         CASE
# MAGIC           WHEN gl_src IS NOT NULL
# MAGIC            AND LENGTH(TRIM(gl_src)) > 0
# MAGIC            AND cus_name IS NOT NULL
# MAGIC           THEN cus_name
# MAGIC           ELSE ar_customer_name
# MAGIC         END
# MAGIC       )) > 0
# MAGIC       THEN CONCAT_WS(', ', ARRAY_SORT(COLLECT_SET(
# MAGIC         CASE
# MAGIC           WHEN gl_src IS NOT NULL
# MAGIC            AND LENGTH(TRIM(gl_src)) > 0
# MAGIC            AND cus_name IS NOT NULL
# MAGIC           THEN cus_name
# MAGIC           ELSE ar_customer_name
# MAGIC         END
# MAGIC       )))
# MAGIC       ELSE 'N/A'
# MAGIC     END                               AS Customers,
# MAGIC  
# MAGIC     CAST(SUM(COALESCE(AR_amount, CAST(0 AS DECIMAL(38,10)))) AS DECIMAL(38,10)) AS TotaloldAmount
# MAGIC  
# MAGIC   FROM j
# MAGIC   GROUP BY
# MAGIC     COALESCE(gl_post_date, ar_date),
# MAGIC     gl_acc,
# MAGIC     gl_doc,
# MAGIC     gl_src,
# MAGIC     Global_Dimension_2_Code,
# MAGIC     Gen_Prod_Posting_Group
# MAGIC )
# MAGIC -- ขั้นตอนใหม่: JOIN กับ posted sales inv แล้ว SUM Quantity
# MAGIC SELECT
# MAGIC   g.G_L_Account_No,
# MAGIC   g.Document_No,
# MAGIC   g.Source_No,
# MAGIC   g.GL_Date,
# MAGIC   g.Global_Dimension_2_Code,
# MAGIC   g.Gen_Prod_Posting_Group,
# MAGIC   g.TotalGLAmount,
# MAGIC   g.Customers,
# MAGIC   g.TotaloldAmount,
# MAGIC   COALESCE(SUM(i.Quantity), 0) AS Quantity
# MAGIC  
# MAGIC FROM gl_agg g
# MAGIC LEFT JOIN Gold_Customer_Exp_Lakehouse.cx.gold_posted_sales_inv_join_so i
# MAGIC   ON g.Document_No = i.invNo
# MAGIC   AND g.Gen_Prod_Posting_Group = i.Gen_Prod_Posting_Group
# MAGIC  
# MAGIC WHERE g.Gen_Prod_Posting_Group <> 'GL'
# MAGIC  
# MAGIC GROUP BY
# MAGIC   g.G_L_Account_No,
# MAGIC   g.Document_No,
# MAGIC   g.Source_No,
# MAGIC   g.GL_Date,
# MAGIC   g.Global_Dimension_2_Code,
# MAGIC   g.Gen_Prod_Posting_Group,
# MAGIC   g.TotalGLAmount,
# MAGIC   g.Customers,
# MAGIC   g.TotaloldAmount
# MAGIC ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold QGL40 Merged

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TARGET = "Gold_Finance_Lakehouse.fa.gold_qgl40_merged"
QGL_TBL = "Gold_Finance_Lakehouse.fa.gold_qgl40"
CUS_TBL = "Silver_BC_Lakehouse.bc.`Customer`"
SAP_TBL = "Silver_Commons_Lakehouse.cmn.silver_Old_AR_SAP"

# Load tables
qgl = spark.table(QGL_TBL)
customer = spark.table(CUS_TBL)
sap = spark.table(SAP_TBL)

# ---- First dataset (GL) ----
df_gl = (
    qgl.alias("qgl")
    .join(
        customer.alias("cus"),
        F.upper(F.col("cus.`No.`")) == F.upper(F.col("qgl.Source_No")),
        "left"
    )
    .select(
        F.col("qgl.Posting_Date").cast("date").alias("Posting_Date"),
        F.col("qgl.Document_No").cast("string").alias("Document_No"),
        F.col("qgl.G_L_Account_No").cast("string").alias("G_L_Account_No"),
        F.col("qgl.Description").cast("string").alias("Description"),
        F.col("cus.`Name`").cast("string").alias("customer_name"),
        F.col("qgl.Amount").cast(DecimalType(38, 10)).alias("Amount"),
        F.lit(None).cast(DecimalType(18, 2)).alias("AR_quantity"),
        F.col("qgl.Source_No").cast("string").alias("Source_No"),
        F.col("qgl.SystemCreatedAt").cast("timestamp").alias("SystemCreatedAt"),
        F.col("qgl.SystemModifiedAt").cast("timestamp").alias("SystemModifiedAt"),
    )
)

# ---- Second dataset (SAP AR) ----
df_sap = (
    sap.alias("sap")
    .join(
        customer.alias("cus"),
        F.upper(F.col("cus.`Name`")) == F.upper(F.col("sap.customer_name")),
        "left"
    )
    .select(
        F.to_date(F.col("sap.posting_date_LY")).alias("Posting_Date"),
        F.lit(None).cast("string").alias("Document_No"),
        F.lit(None).cast("string").alias("G_L_Account_No"),
        F.lit(None).cast("string").alias("Description"),
        F.col("sap.customer_name").cast("string").alias("customer_name"),
        F.col("sap.AR_amount").cast(DecimalType(38, 10)).alias("Amount"),
        F.col("sap.AR_quantity").cast(DecimalType(18, 2)).alias("AR_quantity"),
        F.col("cus.`No.`").cast("string").alias("Source_No"),
        F.lit(None).cast("timestamp").alias("SystemCreatedAt"),
        F.lit(None).cast("timestamp").alias("SystemModifiedAt"),
    )
)

# ---- Union (full replace output) ----
df_merged = df_gl.unionByName(df_sap)

# Optional: de-dupe exact duplicates
# df_merged = df_merged.dropDuplicates()

# ---- FULL REPLACE write ----
(
    df_merged.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET)
)

print(f"FULL REPLACE completed into: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold prod_order_ready_to_finish

# CELL ********************

# MAGIC %%sql
# MAGIC -- Notebook: nb_gold_prod_order_ready_to_finish
# MAGIC -- Purpose:  Replicate BC Page 60412 "Prod Orders Ready to Finish" as Gold table
# MAGIC -- Layer:    Silver_BC -> Gold
# MAGIC -- Schedule: Daily (after Silver refresh)
# MAGIC -- Dependencies: Silver_BC_Lakehouse tables: Production Order, Prod Order Line,
# MAGIC --               Item Ledger Entry, Value Entry, Sales Header, Sales Line, Item, Customer
# MAGIC -- Notes:    - Cost from Value Entry (ILE FlowFields not mirrored)
# MAGIC --           - Qty from ILE only (avoid VE row multiplication)
# MAGIC --           - Consumption cost = first Prod Order Line only
# MAGIC --           - Output value/qty = Line 10000 only (per AL logic)
# MAGIC --           - Tolerance = 10% from Manufacturing Setup
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- SET LAKEHOUSE CONTEXT (run in Fabric notebook)
# MAGIC -- ============================================================
# MAGIC -- USE Gold_Production;
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Finance_Lakehouse.fa.gold_prod_order_ready_to_finish
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CTE 1: First Line No. per Production Order
# MAGIC -- ============================================================
# MAGIC first_line AS (
# MAGIC     SELECT
# MAGIC         `Prod. Order No.`,
# MAGIC         MIN(`Line No.`) AS first_line_no
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Prod Order Line`
# MAGIC     WHERE `Status` = 'Released'
# MAGIC     GROUP BY `Prod. Order No.`
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CTE 2: First line detail (Std Unit Cost, Remaining Qty, Due Date)
# MAGIC -- ============================================================
# MAGIC first_line_detail AS (
# MAGIC     SELECT
# MAGIC         pol.`Prod. Order No.`,
# MAGIC         pol.`Line No.`,
# MAGIC         pol.`Item No.`              AS line_item_no,
# MAGIC         pol.`Unit Cost`             AS std_unit_cost,
# MAGIC         pol.`Remaining Quantity`    AS remaining_qty,
# MAGIC         pol.`Due Date` AS due_date,
# MAGIC         pol.`Sales Order No.`       AS pol_sales_order_no,
# MAGIC         pol.`Sales Order Line No.`  AS pol_sales_order_line_no
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Prod Order Line` pol
# MAGIC     INNER JOIN first_line fl
# MAGIC         ON  pol.`Prod. Order No.` = fl.`Prod. Order No.`
# MAGIC         AND pol.`Line No.`        = fl.first_line_no
# MAGIC     WHERE pol.`Status` = 'Released'
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CTE 3: Consumption Cost (first line only) via Value Entry
# MAGIC -- ============================================================
# MAGIC ile_consumption AS (
# MAGIC     SELECT
# MAGIC         ile.`Order No.`,
# MAGIC         ABS(SUM(
# MAGIC             CASE
# MAGIC                 WHEN ve.`Cost Amount (Actual)` <> 0 THEN ve.`Cost Amount (Actual)`
# MAGIC                 ELSE ve.`Cost Amount (Expected)`
# MAGIC             END
# MAGIC         )) AS consumption_cost
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` ile
# MAGIC     INNER JOIN first_line fl
# MAGIC         ON  ile.`Order No.`      = fl.`Prod. Order No.`
# MAGIC         AND ile.`Order Line No.` = fl.first_line_no
# MAGIC     INNER JOIN Silver_BC_Lakehouse.bc.`Value Entry` ve
# MAGIC         ON  ve.`Item Ledger Entry No.` = ile.`Entry No.`
# MAGIC     WHERE ile.`Order Type`  = 'Production'
# MAGIC       AND ile.`Entry Type`  = 'Consumption'
# MAGIC       AND ve.`Entry Type`   = 'Direct Cost'
# MAGIC     GROUP BY ile.`Order No.`
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CTE 4a: Output Qty & Date (Line 10000 only) -- from ILE only
# MAGIC -- ============================================================
# MAGIC ile_output_qty AS (
# MAGIC     SELECT
# MAGIC         ile.`Order No.`,
# MAGIC         ABS(SUM(ile.`Quantity`))    AS output_qty,
# MAGIC         MAX(ile.`Posting Date`)     AS output_date
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` ile
# MAGIC     WHERE ile.`Order Type`     = 'Production'
# MAGIC       AND ile.`Entry Type`     = 'Output'
# MAGIC       AND ile.`Order Line No.` = 10000
# MAGIC     GROUP BY ile.`Order No.`
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CTE 4b: Output Value (Line 10000 only) -- from VE
# MAGIC -- ============================================================
# MAGIC ile_output_val AS (
# MAGIC     SELECT
# MAGIC         ile.`Order No.`,
# MAGIC         ABS(SUM(
# MAGIC             CASE
# MAGIC                 WHEN ve.`Cost Amount (Actual)` <> 0 THEN ve.`Cost Amount (Actual)`
# MAGIC                 ELSE ve.`Cost Amount (Expected)`
# MAGIC             END
# MAGIC         )) AS output_value
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` ile
# MAGIC     INNER JOIN Silver_BC_Lakehouse.bc.`Value Entry` ve
# MAGIC         ON  ve.`Item Ledger Entry No.` = ile.`Entry No.`
# MAGIC     WHERE ile.`Order Type`     = 'Production'
# MAGIC       AND ile.`Entry Type`     = 'Output'
# MAGIC       AND ile.`Order Line No.` = 10000
# MAGIC       AND ve.`Entry Type`      = 'Direct Cost'
# MAGIC     GROUP BY ile.`Order No.`
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CTE 5: Consumption Lines (distinct Order Line No. per Order)
# MAGIC -- ============================================================
# MAGIC cons_lines AS (
# MAGIC     SELECT
# MAGIC         `Order No.`,
# MAGIC         CONCAT_WS(' | ', COLLECT_LIST(CAST(`Order Line No.` AS STRING))) AS cons_lines
# MAGIC     FROM (
# MAGIC         SELECT DISTINCT `Order No.`, `Order Line No.`
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry`
# MAGIC         WHERE `Order Type` = 'Production'
# MAGIC           AND `Entry Type` = 'Consumption'
# MAGIC         ORDER BY `Order No.`, `Order Line No.`
# MAGIC     ) sub
# MAGIC     GROUP BY `Order No.`
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CTE 6: Tolerance Check (Component actual > expected x 1.10)
# MAGIC -- ============================================================
# MAGIC comp_tolerance AS (
# MAGIC     SELECT
# MAGIC         poc.`Prod. Order No.`,
# MAGIC         MAX(CASE
# MAGIC             WHEN COALESCE(ca.actual_qty, 0)
# MAGIC                  > poc.`Expected Quantity` * 1.10
# MAGIC             THEN 1 ELSE 0
# MAGIC         END) AS cons_vs_exp_over_tol
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Prod Order Component` poc
# MAGIC     LEFT JOIN (
# MAGIC         SELECT
# MAGIC             `Order No.`, `Order Line No.`, `Item No.`,
# MAGIC             ABS(SUM(`Quantity`)) AS actual_qty
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry`
# MAGIC         WHERE `Order Type` = 'Production'
# MAGIC           AND `Entry Type` = 'Consumption'
# MAGIC         GROUP BY `Order No.`, `Order Line No.`, `Item No.`
# MAGIC     ) ca
# MAGIC         ON  poc.`Prod. Order No.`      = ca.`Order No.`
# MAGIC         AND poc.`Prod. Order Line No.` = ca.`Order Line No.`
# MAGIC         AND poc.`Item No.`             = ca.`Item No.`
# MAGIC     WHERE poc.`Status` = 'Released'
# MAGIC     GROUP BY poc.`Prod. Order No.`
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CTE 7: Sales Price LCY (Unit Price / Currency Factor)
# MAGIC -- ============================================================
# MAGIC sales_price AS (
# MAGIC     SELECT
# MAGIC         sl.`Document No.`    AS sales_order_no,
# MAGIC         sl.`Line No.`        AS sales_line_no,
# MAGIC         sl.`No.`             AS item_no,
# MAGIC         CASE
# MAGIC             WHEN COALESCE(sh.`Currency Code`, '') = ''
# MAGIC                  OR COALESCE(sh.`Currency Factor`, 0) = 0
# MAGIC             THEN sl.`Unit Price`
# MAGIC             ELSE ROUND(sl.`Unit Price` / sh.`Currency Factor`, 5)
# MAGIC         END AS sales_price_lcy_val
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Sales Line` sl
# MAGIC     INNER JOIN Silver_BC_Lakehouse.bc.`Sales Header` sh
# MAGIC         ON  sl.`Document No.`   = sh.`No.`
# MAGIC         AND sh.`Document Type`  = 'Order'
# MAGIC     WHERE sl.`Document Type` = 'Order'
# MAGIC       AND sl.`Type`          = 'Item'
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CTE 8: Customer Name
# MAGIC -- ============================================================
# MAGIC customer_info AS (
# MAGIC     SELECT
# MAGIC         sh.`No.`     AS sales_order_no,
# MAGIC         c.`Name`     AS customer_name
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Sales Header` sh
# MAGIC     INNER JOIN Silver_BC_Lakehouse.bc.`Customer` c
# MAGIC         ON sh.`Sell-to Customer No.` = c.`No.`
# MAGIC     WHERE sh.`Document Type` = 'Order'
# MAGIC )
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- MAIN SELECT
# MAGIC -- ============================================================
# MAGIC SELECT
# MAGIC     -- ZONE 1: STATUS
# MAGIC     CASE
# MAGIC         WHEN COALESCE(oq.output_qty, 0) > 0
# MAGIC          AND COALESCE(fld.remaining_qty, 0) = 0
# MAGIC          AND COALESCE(c.consumption_cost, 0) > 0
# MAGIC          AND COALESCE(ct.cons_vs_exp_over_tol, 0) = 0
# MAGIC         THEN TRUE ELSE FALSE
# MAGIC     END                                              AS ready_to_finish_flag,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN COALESCE(ct.cons_vs_exp_over_tol, 0) = 1
# MAGIC         THEN TRUE ELSE FALSE
# MAGIC     END                                              AS cons_vs_exp_over_tol_flag,
# MAGIC 
# MAGIC     -- ZONE 2: IDENTITY
# MAGIC     po.`No.`                                         AS prod_order_no,
# MAGIC     po.`Source No.`                                   AS item_no,
# MAGIC     po.`Description` AS description,
# MAGIC     po.`Sales Order No.`                              AS sales_order_no,
# MAGIC     ci.customer_name,
# MAGIC     fld.due_date,
# MAGIC     oq.output_date,
# MAGIC 
# MAGIC     -- ZONE 3: COST
# MAGIC     COALESCE(fld.std_unit_cost, 0)                 AS std_unit_cost,
# MAGIC     COALESCE(c.consumption_cost, 0)                 AS actual_cost_cons,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN COALESCE(oq.output_qty, 0) <> 0
# MAGIC         THEN ROUND(COALESCE(c.consumption_cost, 0) / oq.output_qty, 5)
# MAGIC         ELSE 0
# MAGIC     END                                               AS actual_unit_cost,
# MAGIC 
# MAGIC     COALESCE(oq.output_qty, 0)                      AS output_qty,
# MAGIC     COALESCE(ov.output_value, 0)                    AS output_value,
# MAGIC     COALESCE(fld.remaining_qty, 0)                  AS remaining_qty,
# MAGIC 
# MAGIC     ROUND(
# MAGIC         ABS(COALESCE(fld.remaining_qty, 0))
# MAGIC         * COALESCE(NULLIF(itm.`Last Direct Cost`, 0), itm.`Standard Cost`, 0)
# MAGIC     , 2)                                              AS remaining_value,
# MAGIC 
# MAGIC     -- ZONE 4: COST VARIANCE
# MAGIC     CASE
# MAGIC         WHEN COALESCE(oq.output_qty, 0) <> 0
# MAGIC         THEN ROUND(
# MAGIC                 (COALESCE(c.consumption_cost, 0) / oq.output_qty)
# MAGIC                 - COALESCE(fld.std_unit_cost, 0), 5)
# MAGIC         ELSE 0
# MAGIC     END                                               AS std_vs_actual_var,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN COALESCE(fld.std_unit_cost, 0) <> 0
# MAGIC          AND COALESCE(oq.output_qty, 0) <> 0
# MAGIC         THEN ROUND(
# MAGIC                 ((COALESCE(c.consumption_cost, 0) / oq.output_qty)
# MAGIC                  - fld.std_unit_cost)
# MAGIC                 / fld.std_unit_cost * 100, 2)
# MAGIC         ELSE 0
# MAGIC     END                                               AS std_vs_actual_var_pct,
# MAGIC 
# MAGIC     ROUND(COALESCE(ov.output_value, 0) - COALESCE(c.consumption_cost, 0), 2)
# MAGIC                                                       AS out_minus_cons,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN COALESCE(c.consumption_cost, 0) <> 0
# MAGIC         THEN ROUND(
# MAGIC                 (COALESCE(ov.output_value, 0) - c.consumption_cost)
# MAGIC                 / c.consumption_cost * 100, 2)
# MAGIC         ELSE 0
# MAGIC     END                                               AS out_vs_cons_pct,
# MAGIC 
# MAGIC     -- ZONE 5: SALES PRICE & PROFITABILITY
# MAGIC     COALESCE(sp.sales_price_lcy_val, 0)                 AS sales_price_lcy,
# MAGIC 
# MAGIC     ROUND(COALESCE(sp.sales_price_lcy_val, 0) - COALESCE(fld.std_unit_cost, 0), 2)
# MAGIC                                                       AS gp_vs_std_cost,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN COALESCE(sp.sales_price_lcy_val, 0) <> 0
# MAGIC         THEN ROUND(
# MAGIC                 (sp.sales_price_lcy_val - COALESCE(fld.std_unit_cost, 0))
# MAGIC                 / sp.sales_price_lcy_val * 100, 2)
# MAGIC         ELSE 0
# MAGIC     END                                               AS gp_vs_std_pct,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN COALESCE(oq.output_qty, 0) <> 0
# MAGIC         THEN ROUND(
# MAGIC                 COALESCE(sp.sales_price_lcy_val, 0)
# MAGIC                 - (COALESCE(c.consumption_cost, 0) / oq.output_qty), 2)
# MAGIC         ELSE ROUND(COALESCE(sp.sales_price_lcy_val, 0), 2)
# MAGIC     END                                               AS gp_vs_actual_cost,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN COALESCE(sp.sales_price_lcy_val, 0) <> 0
# MAGIC          AND COALESCE(oq.output_qty, 0) <> 0
# MAGIC         THEN ROUND(
# MAGIC                 (sp.sales_price_lcy_val
# MAGIC                  - (COALESCE(c.consumption_cost, 0) / oq.output_qty))
# MAGIC                 / sp.sales_price_lcy_val * 100, 2)
# MAGIC         ELSE 0
# MAGIC     END                                               AS gp_vs_actual_pct,
# MAGIC 
# MAGIC     -- ZONE 6: REMAINING
# MAGIC     cl.cons_lines,
# MAGIC 
# MAGIC     -- METADATA
# MAGIC     current_timestamp()                               AS _load_timestamp
# MAGIC 
# MAGIC FROM Silver_BC_Lakehouse.bc.`Production Order` po
# MAGIC 
# MAGIC LEFT JOIN first_line_detail fld
# MAGIC     ON po.`No.` = fld.`Prod. Order No.`
# MAGIC 
# MAGIC LEFT JOIN ile_consumption c
# MAGIC     ON po.`No.` = c.`Order No.`
# MAGIC 
# MAGIC LEFT JOIN ile_output_qty oq
# MAGIC     ON po.`No.` = oq.`Order No.`
# MAGIC 
# MAGIC LEFT JOIN ile_output_val ov
# MAGIC     ON po.`No.` = ov.`Order No.`
# MAGIC 
# MAGIC LEFT JOIN cons_lines cl
# MAGIC     ON po.`No.` = cl.`Order No.`
# MAGIC 
# MAGIC LEFT JOIN comp_tolerance ct
# MAGIC     ON po.`No.` = ct.`Prod. Order No.`
# MAGIC 
# MAGIC LEFT JOIN sales_price sp
# MAGIC     ON  fld.pol_sales_order_no      = sp.sales_order_no
# MAGIC     AND fld.pol_sales_order_line_no = sp.sales_line_no
# MAGIC 
# MAGIC LEFT JOIN customer_info ci
# MAGIC     ON po.`Sales Order No.` = ci.sales_order_no
# MAGIC 
# MAGIC LEFT JOIN Silver_BC_Lakehouse.bc.`Item` itm
# MAGIC     ON po.`Source No.` = itm.`No.`
# MAGIC 
# MAGIC WHERE po.`Status` = 'Released'
# MAGIC ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # Gold prod_order_component_variance

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE  Gold_Finance_Lakehouse.fa.gold_prod_order_component_variance
# MAGIC 
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC WITH
# MAGIC bom AS (
# MAGIC     SELECT
# MAGIC         poc.`Prod. Order No.`              AS prod_order_no,
# MAGIC         poc.`Prod. Order Line No.`         AS po_line_no,
# MAGIC         poc.`Line No.`                     AS comp_line_no,
# MAGIC         poc.`Item No.`                     AS component_item_no,
# MAGIC         poc.`Description`                  AS component_description,
# MAGIC         poc.`Unit of Measure Code`         AS uom,
# MAGIC         poc.`Expected Quantity`            AS bom_qty,
# MAGIC         poc.`Unit Cost`                    AS bom_unit_cost,
# MAGIC         (poc.`Expected Quantity` * poc.`Unit Cost`) AS bom_total_cost
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Prod Order Component` poc
# MAGIC ),
# MAGIC 
# MAGIC actual AS (
# MAGIC     SELECT
# MAGIC         ile.`Order No.`             AS prod_order_no,
# MAGIC         ile.`Order Line No.`        AS po_line_no,
# MAGIC         ile.`Item No.`              AS component_item_no,
# MAGIC         SUM(ABS(ile.`Quantity`))    AS actual_qty,
# MAGIC         SUM(ABS(ve.`Cost Amount (Actual)`)) AS actual_cost
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` ile
# MAGIC     INNER JOIN Silver_BC_Lakehouse.bc.`Value Entry` ve
# MAGIC         ON ile.`Entry No.` = ve.`Item Ledger Entry No.`
# MAGIC     WHERE ile.`Entry Type` = 'Consumption'
# MAGIC     GROUP BY ile.`Order No.`, ile.`Order Line No.`, ile.`Item No.`
# MAGIC ),
# MAGIC 
# MAGIC -- PO → SO + FG item mapping (from profitability table)
# MAGIC po_so AS (
# MAGIC     SELECT DISTINCT
# MAGIC         p.prod_order_no,
# MAGIC         p.sales_order_no,
# MAGIC         p.item_no AS fg_item_no
# MAGIC     FROM Gold_Finance_Lakehouse.fa.gold_prod_order_profitability p
# MAGIC     WHERE p.sales_order_no IS NOT NULL
# MAGIC       AND p.sales_order_no <> ''
# MAGIC ),
# MAGIC 
# MAGIC -- Gold/Silver fix price from Sales Header directly
# MAGIC so_metal_price AS (
# MAGIC     SELECT
# MAGIC         sh.`No.`                    AS sales_order_no,
# MAGIC         sh.`Order Date`             AS so_order_date,
# MAGIC         sh.`Gold Per Ounce`         AS gold_per_ounce_usd,
# MAGIC         sh.`Silver Per Ounce`       AS silver_per_ounce_usd,
# MAGIC         sh.`Currency Code`          AS so_currency_code,
# MAGIC         sh.`Currency Factor`        AS so_currency_factor,
# MAGIC         sh.`Sell-to Customer No.`   AS so_customer_no,
# MAGIC         sh.`Sell-to Customer Name`  AS so_customer_name
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Sales Header` sh
# MAGIC     WHERE sh.`Document Type` = 'Order'
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     b.prod_order_no,
# MAGIC     b.po_line_no,
# MAGIC     b.comp_line_no,
# MAGIC     b.component_item_no,
# MAGIC     b.component_description,
# MAGIC     b.uom,
# MAGIC 
# MAGIC     -- ── SO + FG link (for JOIN to gold_invoice_summary) ──────
# MAGIC     ps.sales_order_no,
# MAGIC     ps.fg_item_no,
# MAGIC 
# MAGIC     -- ── BOM ──────────────────────────────────────────────────
# MAGIC     b.bom_qty,
# MAGIC     b.bom_unit_cost,
# MAGIC     b.bom_total_cost,
# MAGIC 
# MAGIC     -- ── Actual (FIFO from Value Entry) ───────────────────────
# MAGIC     COALESCE(a.actual_qty, 0)                    AS actual_qty,
# MAGIC     COALESCE(a.actual_cost, 0)                   AS actual_cost,
# MAGIC     CASE WHEN COALESCE(a.actual_qty, 0) <> 0
# MAGIC          THEN COALESCE(a.actual_cost, 0) / a.actual_qty
# MAGIC          ELSE 0
# MAGIC     END                                          AS actual_unit_cost,
# MAGIC 
# MAGIC     -- ── Variance ─────────────────────────────────────────────
# MAGIC     (COALESCE(a.actual_qty, 0) - b.bom_qty)      AS qty_var,
# MAGIC     CASE WHEN b.bom_qty <> 0
# MAGIC          THEN ((COALESCE(a.actual_qty, 0) - b.bom_qty) / b.bom_qty) * 100
# MAGIC          ELSE 0
# MAGIC     END                                          AS qty_var_pct,
# MAGIC     (COALESCE(a.actual_cost, 0) - b.bom_total_cost) AS cost_var,
# MAGIC     CASE WHEN b.bom_total_cost <> 0
# MAGIC          THEN ((COALESCE(a.actual_cost, 0) - b.bom_total_cost) / b.bom_total_cost) * 100
# MAGIC          ELSE 0
# MAGIC     END                                          AS cost_var_pct,
# MAGIC     CASE WHEN b.bom_unit_cost <> 0
# MAGIC          THEN ((CASE WHEN COALESCE(a.actual_qty, 0) <> 0
# MAGIC                      THEN COALESCE(a.actual_cost, 0) / a.actual_qty
# MAGIC                      ELSE 0 END
# MAGIC                - b.bom_unit_cost) / b.bom_unit_cost) * 100
# MAGIC          ELSE 0
# MAGIC     END                                          AS unit_cost_var_pct,
# MAGIC 
# MAGIC     -- ── Item enrichment ───────────────────────────────────────
# MAGIC     i.`Item Category Code`                       AS item_category_code,
# MAGIC 
# MAGIC     -- component_type_group = SUB-CATEGORY in RM Analysis
# MAGIC     CASE
# MAGIC         WHEN i.`Item Category Code` = 'CASTING'       THEN 'CASTING'
# MAGIC         WHEN i.`Item Category Code` = 'FINDINGS'      THEN 'FINDINGS'
# MAGIC         WHEN i.`Item Category Code` = 'SEMI-FG'       THEN 'SEMI-FG'
# MAGIC         WHEN i.`Item Category Code` = 'FG'            THEN 'FG'
# MAGIC         WHEN i.`Item Category Code` IN ('MIXED METAL', 'PURE METAL', 'ALLOY', 'SOLDER')
# MAGIC             THEN 'RAW_METAL'
# MAGIC         WHEN i.`Item Category Code` IN ('DIAMONDS LAB', 'DIAMOND NAT')
# MAGIC             THEN 'DIAMOND'
# MAGIC         WHEN i.`Item Category Code` IN ('GEMSTONES', 'PEARLS', 'SYNT STONE')
# MAGIC             THEN 'STONE'
# MAGIC         WHEN i.`Item Category Code` = 'PLATING SOLUTION'
# MAGIC             THEN 'PLATING'
# MAGIC         WHEN i.`Item Category Code` IN ('LEATHER', 'BEAD')
# MAGIC             THEN 'OTHER_MATERIAL'
# MAGIC         WHEN i.`Item Category Code` IN ('CONSUMABLES', 'TOOLS', 'EQUIPMENT', 'STATIONARY', 'PACKAGING')
# MAGIC             THEN 'INDIRECT'
# MAGIC         ELSE COALESCE(i.`Item Category Code`, 'UNKNOWN')
# MAGIC     END                                          AS component_type_group,
# MAGIC 
# MAGIC     -- component_type_parent = CATEGORY PARENT in RM Analysis
# MAGIC     CASE
# MAGIC         WHEN i.`Item Category Code` IN ('MIXED METAL', 'PURE METAL', 'ALLOY', 'SOLDER')
# MAGIC             THEN 'Precious Metals'
# MAGIC         WHEN i.`Item Category Code` IN ('DIAMONDS LAB', 'DIAMOND NAT')
# MAGIC             THEN 'Diamonds'
# MAGIC         WHEN i.`Item Category Code` IN ('GEMSTONES', 'PEARLS', 'SYNT STONE')
# MAGIC             THEN 'Stones & Pearls'
# MAGIC         WHEN i.`Item Category Code` = 'FINDINGS'
# MAGIC             THEN 'Findings'
# MAGIC         WHEN i.`Item Category Code` = 'CASTING'
# MAGIC             THEN 'Casting Parts'
# MAGIC         WHEN i.`Item Category Code` = 'PLATING SOLUTION'
# MAGIC             THEN 'Plating'
# MAGIC         WHEN i.`Item Category Code` IN ('CONSUMABLES', 'TOOLS', 'EQUIPMENT', 'STATIONARY', 'PACKAGING')
# MAGIC             THEN 'Consumables'
# MAGIC         ELSE 'Other'
# MAGIC     END                                          AS component_type_parent,
# MAGIC 
# MAGIC     i.`Metal Category Code`                      AS metal_category_code,
# MAGIC     i.`Product Type`                             AS product_type,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN i.`Metal Category Code` IN (
# MAGIC             'SILVER 925', '14KW', '14KY', '14KR',
# MAGIC             '18KW', '18KY', '18KR', '9KW', '9KY'
# MAGIC         ) THEN true
# MAGIC         WHEN i.`Item Category Code` IN ('PURE METAL', 'ALLOY', 'MIXED METAL', 'SOLDER')
# MAGIC             AND i.`Metal Category Code` IS NOT NULL
# MAGIC             AND i.`Metal Category Code` <> ''
# MAGIC         THEN true
# MAGIC         ELSE false
# MAGIC     END                                          AS is_precious_metal,
# MAGIC 
# MAGIC     -- ── Sales Order metal fix price (USD/ozt) ─────────────────
# MAGIC     smp.so_order_date,
# MAGIC     smp.gold_per_ounce_usd,
# MAGIC     smp.silver_per_ounce_usd,
# MAGIC     smp.so_currency_code,
# MAGIC     smp.so_currency_factor,
# MAGIC     smp.so_customer_no,
# MAGIC     smp.so_customer_name,
# MAGIC 
# MAGIC     -- ── Derived: metal price THB/gram ─────────────────────────
# MAGIC     -- Formula: (USD_per_ozt / 31.1035) / currency_factor
# MAGIC     -- currency_factor = FCY per 1 THB, so THB = USD / factor
# MAGIC     CASE WHEN smp.gold_per_ounce_usd > 0 AND smp.so_currency_factor > 0
# MAGIC          THEN (smp.gold_per_ounce_usd / 31.1035) / smp.so_currency_factor
# MAGIC          ELSE NULL
# MAGIC     END                                          AS gold_thb_per_gram,
# MAGIC 
# MAGIC     CASE WHEN smp.silver_per_ounce_usd > 0 AND smp.so_currency_factor > 0
# MAGIC          THEN (smp.silver_per_ounce_usd / 31.1035) / smp.so_currency_factor
# MAGIC          ELSE NULL
# MAGIC     END                                          AS silver_thb_per_gram
# MAGIC 
# MAGIC FROM bom b
# MAGIC 
# MAGIC LEFT JOIN actual a
# MAGIC     ON  b.prod_order_no     = a.prod_order_no
# MAGIC     AND b.po_line_no        = a.po_line_no
# MAGIC     AND b.component_item_no = a.component_item_no
# MAGIC 
# MAGIC LEFT JOIN Silver_BC_Lakehouse.bc.`Item` i
# MAGIC     ON b.component_item_no = i.`No.`
# MAGIC 
# MAGIC LEFT JOIN po_so ps
# MAGIC     ON b.prod_order_no = ps.prod_order_no
# MAGIC 
# MAGIC LEFT JOIN so_metal_price smp
# MAGIC     ON ps.sales_order_no = smp.sales_order_no
# MAGIC ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # gold prod order profitability

# CELL ********************

# MAGIC %%sql
# MAGIC -- Cell 1: gold_prod_order_profitability (Detail per PO - Released + Finished)
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Finance_Lakehouse.fa.gold_prod_order_profitability
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH
# MAGIC 
# MAGIC first_line AS (
# MAGIC     SELECT
# MAGIC         `Prod. Order No.`,
# MAGIC         `Status`,
# MAGIC         MIN(`Line No.`) AS first_line_no
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Prod Order Line`
# MAGIC     WHERE `Status` IN ('Released', 'Finished')
# MAGIC     GROUP BY `Prod. Order No.`, `Status`
# MAGIC ),
# MAGIC 
# MAGIC first_line_detail AS (
# MAGIC     SELECT
# MAGIC         pol.`Prod. Order No.`,
# MAGIC         pol.`Status`                    AS po_status,
# MAGIC         pol.`Unit Cost`                 AS std_unit_cost,
# MAGIC         pol.`Remaining Quantity`        AS remaining_qty,
# MAGIC         pol.`Due Date`                  AS due_date,
# MAGIC         pol.`Sales Order No.`           AS pol_sales_order_no,
# MAGIC         pol.`Sales Order Line No.`      AS pol_sales_order_line_no
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Prod Order Line` pol
# MAGIC     INNER JOIN first_line fl
# MAGIC         ON  pol.`Prod. Order No.` = fl.`Prod. Order No.`
# MAGIC         AND pol.`Status`          = fl.`Status`
# MAGIC         AND pol.`Line No.`        = fl.first_line_no
# MAGIC ),
# MAGIC 
# MAGIC -- Consumption Cost (first line only) via Value Entry
# MAGIC ile_consumption AS (
# MAGIC     SELECT
# MAGIC         ile.`Order No.`,
# MAGIC         ABS(SUM(
# MAGIC             CASE
# MAGIC                 WHEN ve.`Cost Amount (Actual)` <> 0 THEN ve.`Cost Amount (Actual)`
# MAGIC                 ELSE ve.`Cost Amount (Expected)`
# MAGIC             END
# MAGIC         )) AS consumption_cost
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` ile
# MAGIC     INNER JOIN first_line fl
# MAGIC         ON  ile.`Order No.`      = fl.`Prod. Order No.`
# MAGIC         AND ile.`Order Line No.` = fl.first_line_no
# MAGIC     INNER JOIN Silver_BC_Lakehouse.bc.`Value Entry` ve
# MAGIC         ON  ve.`Item Ledger Entry No.` = ile.`Entry No.`
# MAGIC     WHERE ile.`Order Type`  = 'Production'
# MAGIC       AND ile.`Entry Type`  = 'Consumption'
# MAGIC       AND ve.`Entry Type`   = 'Direct Cost'
# MAGIC     GROUP BY ile.`Order No.`
# MAGIC ),
# MAGIC 
# MAGIC -- Output Qty (Line 10000 only) from ILE
# MAGIC ile_output_qty AS (
# MAGIC     SELECT
# MAGIC         ile.`Order No.`,
# MAGIC         ABS(SUM(ile.`Quantity`))    AS output_qty,
# MAGIC         MAX(ile.`Posting Date`)     AS output_date
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` ile
# MAGIC     WHERE ile.`Order Type`     = 'Production'
# MAGIC       AND ile.`Entry Type`     = 'Output'
# MAGIC       AND ile.`Order Line No.` = 10000
# MAGIC     GROUP BY ile.`Order No.`
# MAGIC ),
# MAGIC 
# MAGIC -- Output Value (Line 10000 only) from VE
# MAGIC ile_output_val AS (
# MAGIC     SELECT
# MAGIC         ile.`Order No.`,
# MAGIC         ABS(SUM(
# MAGIC             CASE
# MAGIC                 WHEN ve.`Cost Amount (Actual)` <> 0 THEN ve.`Cost Amount (Actual)`
# MAGIC                 ELSE ve.`Cost Amount (Expected)`
# MAGIC             END
# MAGIC         )) AS output_value
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` ile
# MAGIC     INNER JOIN Silver_BC_Lakehouse.bc.`Value Entry` ve
# MAGIC         ON  ve.`Item Ledger Entry No.` = ile.`Entry No.`
# MAGIC     WHERE ile.`Order Type`     = 'Production'
# MAGIC       AND ile.`Entry Type`     = 'Output'
# MAGIC       AND ile.`Order Line No.` = 10000
# MAGIC       AND ve.`Entry Type`      = 'Direct Cost'
# MAGIC     GROUP BY ile.`Order No.`
# MAGIC ),
# MAGIC 
# MAGIC -- Sales Price LCY
# MAGIC sales_price AS (
# MAGIC     SELECT
# MAGIC         sl.`Document No.`    AS sales_order_no,
# MAGIC         sl.`Line No.`        AS sales_line_no,
# MAGIC         sl.`No.`             AS item_no,
# MAGIC         CASE
# MAGIC             WHEN COALESCE(sh.`Currency Code`, '') = ''
# MAGIC                  OR COALESCE(sh.`Currency Factor`, 0) = 0
# MAGIC             THEN sl.`Unit Price`
# MAGIC             ELSE ROUND(sl.`Unit Price` / sh.`Currency Factor`, 5)
# MAGIC         END AS sales_price_lcy
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Sales Line` sl
# MAGIC     INNER JOIN Silver_BC_Lakehouse.bc.`Sales Header` sh
# MAGIC         ON  sl.`Document No.`   = sh.`No.`
# MAGIC         AND sh.`Document Type`  = 'Order'
# MAGIC     WHERE sl.`Document Type` = 'Order'
# MAGIC       AND sl.`Type`          = 'Item'
# MAGIC ),
# MAGIC 
# MAGIC -- Customer Name
# MAGIC customer_info AS (
# MAGIC     SELECT
# MAGIC         sh.`No.`     AS sales_order_no,
# MAGIC         c.`Name`     AS customer_name
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Sales Header` sh
# MAGIC     INNER JOIN Silver_BC_Lakehouse.bc.`Customer` c
# MAGIC         ON sh.`Sell-to Customer No.` = c.`No.`
# MAGIC     WHERE sh.`Document Type` = 'Order'
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     po.`No.`                                          AS prod_order_no,
# MAGIC     po.`Status`                                       AS po_status,
# MAGIC     po.`Source No.`                                    AS item_no,
# MAGIC     po.`Description`                                   AS description,
# MAGIC     po.`Sales Order No.`                               AS sales_order_no,
# MAGIC     ci.customer_name,
# MAGIC 
# MAGIC     -- Dates
# MAGIC     fld.due_date,
# MAGIC     oq.output_date,
# MAGIC 
# MAGIC     -- Quantities
# MAGIC     COALESCE(oq.output_qty, 0)                         AS output_qty,
# MAGIC     COALESCE(fld.remaining_qty, 0)                     AS remaining_qty,
# MAGIC 
# MAGIC     -- Standard BOM Cost (total) = Std Unit Cost x Output Qty
# MAGIC     COALESCE(fld.std_unit_cost, 0)                     AS std_unit_cost,
# MAGIC     ROUND(COALESCE(fld.std_unit_cost, 0)
# MAGIC           * COALESCE(oq.output_qty, 0), 2)             AS standard_bom_cost,
# MAGIC 
# MAGIC     -- Actual Material Cost
# MAGIC     COALESCE(c.consumption_cost, 0)                    AS actual_material_cost,
# MAGIC 
# MAGIC     -- Actual Unit Cost
# MAGIC     CASE
# MAGIC         WHEN COALESCE(oq.output_qty, 0) <> 0
# MAGIC         THEN ROUND(COALESCE(c.consumption_cost, 0) / oq.output_qty, 5)
# MAGIC         ELSE 0
# MAGIC     END                                                AS actual_unit_cost,
# MAGIC 
# MAGIC     -- Variance (Actual - Standard)
# MAGIC     ROUND(COALESCE(c.consumption_cost, 0)
# MAGIC           - (COALESCE(fld.std_unit_cost, 0) * COALESCE(oq.output_qty, 0)), 2)
# MAGIC                                                        AS cost_variance,
# MAGIC 
# MAGIC     -- Variance %
# MAGIC     CASE
# MAGIC         WHEN COALESCE(fld.std_unit_cost, 0) * COALESCE(oq.output_qty, 0) <> 0
# MAGIC         THEN ROUND(
# MAGIC                 COALESCE(c.consumption_cost, 0)
# MAGIC                 / (fld.std_unit_cost * oq.output_qty) * 100, 2)
# MAGIC         ELSE 0
# MAGIC     END                                                AS actual_vs_std_pct,
# MAGIC 
# MAGIC     -- Selling Price (per unit LCY)
# MAGIC     COALESCE(sp.sales_price_lcy, 0)                    AS selling_price_lcy,
# MAGIC 
# MAGIC     -- Total Selling Value = Selling Price x Output Qty
# MAGIC     ROUND(COALESCE(sp.sales_price_lcy, 0)
# MAGIC           * COALESCE(oq.output_qty, 0), 2)             AS total_selling_value,
# MAGIC 
# MAGIC     -- Margin = Selling - Actual Material
# MAGIC     ROUND(COALESCE(sp.sales_price_lcy, 0) * COALESCE(oq.output_qty, 0)
# MAGIC           - COALESCE(c.consumption_cost, 0), 2)        AS margin_amount,
# MAGIC 
# MAGIC     -- Margin %
# MAGIC     CASE
# MAGIC         WHEN COALESCE(sp.sales_price_lcy, 0) * COALESCE(oq.output_qty, 0) <> 0
# MAGIC         THEN ROUND(
# MAGIC                 (sp.sales_price_lcy * oq.output_qty - COALESCE(c.consumption_cost, 0))
# MAGIC                 / (sp.sales_price_lcy * oq.output_qty) * 100, 2)
# MAGIC         ELSE 0
# MAGIC     END                                                AS margin_pct,
# MAGIC 
# MAGIC     -- Output Value
# MAGIC     COALESCE(ov.output_value, 0)                       AS output_value,
# MAGIC 
# MAGIC     current_timestamp()                                AS _load_timestamp
# MAGIC 
# MAGIC FROM Silver_BC_Lakehouse.bc.`Production Order` po
# MAGIC 
# MAGIC LEFT JOIN first_line_detail fld
# MAGIC     ON  po.`No.`     = fld.`Prod. Order No.`
# MAGIC     AND po.`Status`  = fld.po_status
# MAGIC 
# MAGIC LEFT JOIN ile_consumption c
# MAGIC     ON po.`No.` = c.`Order No.`
# MAGIC 
# MAGIC LEFT JOIN ile_output_qty oq
# MAGIC     ON po.`No.` = oq.`Order No.`
# MAGIC 
# MAGIC LEFT JOIN ile_output_val ov
# MAGIC     ON po.`No.` = ov.`Order No.`
# MAGIC 
# MAGIC LEFT JOIN sales_price sp
# MAGIC     ON  fld.pol_sales_order_no      = sp.sales_order_no
# MAGIC     AND fld.pol_sales_order_line_no = sp.sales_line_no
# MAGIC 
# MAGIC LEFT JOIN customer_info ci
# MAGIC     ON po.`Sales Order No.` = ci.sales_order_no
# MAGIC 
# MAGIC WHERE po.`Status` IN ('Released', 'Finished')
# MAGIC ;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # gold profitability by customer

# CELL ********************

# MAGIC %%sql
# MAGIC -- Cell 2: gold_profitability_by_customer (Aggregate per Customer)
# MAGIC 
# MAGIC -- TABLE 2: Aggregate per Customer
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TABLE Gold_Finance_Lakehouse.fa.gold_profitability_by_customer
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     customer_name,
# MAGIC     COUNT(DISTINCT prod_order_no)                      AS po_count,
# MAGIC     SUM(output_qty)                                    AS total_output_qty,
# MAGIC     ROUND(SUM(standard_bom_cost), 2)                   AS total_standard_bom,
# MAGIC     ROUND(SUM(actual_material_cost), 2)                AS total_actual_material,
# MAGIC     CASE
# MAGIC         WHEN SUM(standard_bom_cost) <> 0
# MAGIC         THEN ROUND(SUM(actual_material_cost) / SUM(standard_bom_cost) * 100, 2)
# MAGIC         ELSE 0
# MAGIC     END                                                AS actual_vs_std_pct,
# MAGIC     ROUND(SUM(cost_variance), 2)                       AS total_variance,
# MAGIC     ROUND(SUM(total_selling_value), 2)                 AS total_selling_value,
# MAGIC     ROUND(SUM(margin_amount), 2)                       AS total_margin,
# MAGIC     CASE
# MAGIC         WHEN SUM(total_selling_value) <> 0
# MAGIC         THEN ROUND(SUM(margin_amount) / SUM(total_selling_value) * 100, 2)
# MAGIC         ELSE 0
# MAGIC     END                                                AS margin_pct,
# MAGIC     current_timestamp()                                AS _load_timestamp
# MAGIC FROM Gold_Finance_Lakehouse.fa.gold_prod_order_profitability
# MAGIC WHERE customer_name IS NOT NULL
# MAGIC GROUP BY customer_name
# MAGIC ;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # gold profitability by customer item

# CELL ********************

# MAGIC %%sql
# MAGIC -- Cell 3: gold_profitability_by_customer_item (Aggregate per Customer + Item)
# MAGIC 
# MAGIC -- TABLE 3: Aggregate per Customer + Item
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TABLE Gold_Finance_Lakehouse.fa.gold_profitability_by_customer_item
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     customer_name,
# MAGIC     item_no,
# MAGIC     description,
# MAGIC     COUNT(DISTINCT prod_order_no)                      AS po_count,
# MAGIC     SUM(output_qty)                                    AS total_output_qty,
# MAGIC     ROUND(SUM(standard_bom_cost), 2)                   AS total_standard_bom,
# MAGIC     ROUND(SUM(actual_material_cost), 2)                AS total_actual_material,
# MAGIC     CASE
# MAGIC         WHEN SUM(standard_bom_cost) <> 0
# MAGIC         THEN ROUND(SUM(actual_material_cost) / SUM(standard_bom_cost) * 100, 2)
# MAGIC         ELSE 0
# MAGIC     END                                                AS actual_vs_std_pct,
# MAGIC     ROUND(SUM(cost_variance), 2)                       AS total_variance,
# MAGIC     ROUND(SUM(total_selling_value), 2)                 AS total_selling_value,
# MAGIC     ROUND(SUM(margin_amount), 2)                       AS total_margin,
# MAGIC     CASE
# MAGIC         WHEN SUM(total_selling_value) <> 0
# MAGIC         THEN ROUND(SUM(margin_amount) / SUM(total_selling_value) * 100, 2)
# MAGIC         ELSE 0
# MAGIC     END                                                AS margin_pct,
# MAGIC     -- Avg selling price per unit
# MAGIC     CASE
# MAGIC         WHEN SUM(output_qty) <> 0
# MAGIC         THEN ROUND(SUM(total_selling_value) / SUM(output_qty), 5)
# MAGIC         ELSE 0
# MAGIC     END                                                AS avg_selling_price,
# MAGIC     -- Avg actual unit cost
# MAGIC     CASE
# MAGIC         WHEN SUM(output_qty) <> 0
# MAGIC         THEN ROUND(SUM(actual_material_cost) / SUM(output_qty), 5)
# MAGIC         ELSE 0
# MAGIC     END                                                AS avg_actual_unit_cost,
# MAGIC     current_timestamp()                                AS _load_timestamp
# MAGIC FROM Gold_Finance_Lakehouse.fa.gold_prod_order_profitability
# MAGIC WHERE customer_name IS NOT NULL
# MAGIC GROUP BY customer_name, item_no, description
# MAGIC ;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # gold profitability by customer so

# CELL ********************

# MAGIC %%sql
# MAGIC -- Cell 4: gold_profitability_by_customer_so (Aggregate per Customer + Sales Order)
# MAGIC 
# MAGIC -- TABLE 4: Aggregate per Customer + Sales Order
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TABLE Gold_Finance_Lakehouse.fa.gold_profitability_by_customer_so
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     customer_name,
# MAGIC     sales_order_no,
# MAGIC     COUNT(DISTINCT prod_order_no)                      AS po_count,
# MAGIC     COUNT(DISTINCT item_no)                            AS item_count,
# MAGIC     SUM(output_qty)                                    AS total_output_qty,
# MAGIC     ROUND(SUM(standard_bom_cost), 2)                   AS total_standard_bom,
# MAGIC     ROUND(SUM(actual_material_cost), 2)                AS total_actual_material,
# MAGIC     CASE
# MAGIC         WHEN SUM(standard_bom_cost) <> 0
# MAGIC         THEN ROUND(SUM(actual_material_cost) / SUM(standard_bom_cost) * 100, 2)
# MAGIC         ELSE 0
# MAGIC     END                                                AS actual_vs_std_pct,
# MAGIC     ROUND(SUM(cost_variance), 2)                       AS total_variance,
# MAGIC     ROUND(SUM(total_selling_value), 2)                 AS total_selling_value,
# MAGIC     ROUND(SUM(margin_amount), 2)                       AS total_margin,
# MAGIC     CASE
# MAGIC         WHEN SUM(total_selling_value) <> 0
# MAGIC         THEN ROUND(SUM(margin_amount) / SUM(total_selling_value) * 100, 2)
# MAGIC         ELSE 0
# MAGIC     END                                                AS margin_pct,
# MAGIC     current_timestamp()                                AS _load_timestamp
# MAGIC FROM Gold_Finance_Lakehouse.fa.gold_prod_order_profitability
# MAGIC WHERE customer_name IS NOT NULL
# MAGIC   AND sales_order_no IS NOT NULL
# MAGIC   AND sales_order_no <> ''
# MAGIC GROUP BY customer_name, sales_order_no
# MAGIC ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # Gold Invoice

# CELL ********************

# MAGIC %%sql
# MAGIC -- Notebook: nb_gold_finance_invoice_summary
# MAGIC -- Purpose: Build gold_invoice_summary — invoice-level FG summary for Costing Intelligence "GM by AR" tab
# MAGIC -- Layer: Silver → Gold (Gold_Finance_Lakehouse)
# MAGIC -- Grain: 1 row = 1 invoice_no + 1 salesorder_no + 1 fg_item_no
# MAGIC -- Schedule: Daily (after silver_sales_invoice_header_line refresh)
# MAGIC -- Dependencies:
# MAGIC --   Silver_Finance_Lakehouse.fa.silver_sales_invoice_header_line
# MAGIC --   Silver_BC_Lakehouse.bc.`Item`           ← Product Type, Metal Category
# MAGIC --   Silver_BC_Lakehouse.bc.`Sales Header`   ← Gold/Silver Per Ounce, Currency Factor
# MAGIC -- Engine: Spark SQL (Fabric Notebook — 3 cells)
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Finance_Lakehouse.fa.gold_invoice_summary
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC WITH
# MAGIC -- Aggregate invoice lines to: invoice + SO + FG item
# MAGIC invoice_agg AS (
# MAGIC     SELECT
# MAGIC         si.customer_no,
# MAGIC         si.invoice_no,
# MAGIC         si.salesorder_no,
# MAGIC         si.invoice_posting_date,
# MAGIC         si.item_no                          AS fg_item_no,
# MAGIC         si.item_description                 AS fg_item_description,
# MAGIC         si.item_category                    AS fg_item_category,
# MAGIC         si.item_material                    AS fg_material,
# MAGIC         si.cad_manager_team,
# MAGIC 
# MAGIC         SUM(si.item_quantity)               AS fg_qty,
# MAGIC         SUM(si.invoiceline_amount)          AS sell_amount_fcy,
# MAGIC         SUM(si.item_quantity * si.item_unit_cost_THB) AS actual_cost_thb,
# MAGIC         SUM(si.item_quantity * si.item_unit_cost)     AS actual_cost_fcy
# MAGIC 
# MAGIC     FROM Silver_Finance_Lakehouse.fa.silver_sales_invoice_header_line si
# MAGIC     WHERE si.item_type = 'Item'
# MAGIC       AND si.item_quantity > 0
# MAGIC     GROUP BY
# MAGIC         si.customer_no,
# MAGIC         si.invoice_no,
# MAGIC         si.salesorder_no,
# MAGIC         si.invoice_posting_date,
# MAGIC         si.item_no,
# MAGIC         si.item_description,
# MAGIC         si.item_category,
# MAGIC         si.item_material,
# MAGIC         si.cad_manager_team
# MAGIC ),
# MAGIC 
# MAGIC -- Item enrichment
# MAGIC item_info AS (
# MAGIC     SELECT
# MAGIC         i.`No.`                AS item_no,
# MAGIC         i.`Product Type`       AS product_type,
# MAGIC         i.`Metal Category Code` AS metal_category_code,
# MAGIC         i.`Item Category Code` AS item_category_code
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item` i
# MAGIC ),
# MAGIC 
# MAGIC -- Metal fix price from Sales Header
# MAGIC so_metal AS (
# MAGIC     SELECT
# MAGIC         sh.`No.`               AS sales_order_no,
# MAGIC         sh.`Order Date`        AS so_order_date,
# MAGIC         sh.`Sell-to Customer Name` AS customer_name,
# MAGIC         sh.`Gold Per Ounce`    AS gold_per_ounce_usd,
# MAGIC         sh.`Silver Per Ounce`  AS silver_per_ounce_usd,
# MAGIC         sh.`Currency Code`     AS currency_code,
# MAGIC         sh.`Currency Factor`   AS currency_factor
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Sales Header` sh
# MAGIC     WHERE sh.`Document Type` = 'Order'
# MAGIC ),
# MAGIC 
# MAGIC -- BOM ADJ + PROS from component_variance (aggregate per SO + FG item)
# MAGIC bom_summary AS (
# MAGIC     SELECT
# MAGIC         cv.sales_order_no,
# MAGIC         cv.fg_item_no,
# MAGIC         COUNT(DISTINCT cv.prod_order_no)     AS pros_count,
# MAGIC         SUM(cv.bom_total_cost)               AS bom_adj_thb,
# MAGIC         SUM(cv.actual_cost)                  AS component_actual_thb
# MAGIC     FROM Gold_Finance_Lakehouse.fa.gold_prod_order_component_variance cv
# MAGIC     WHERE cv.sales_order_no IS NOT NULL
# MAGIC     GROUP BY cv.sales_order_no, cv.fg_item_no
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     -- ── Keys ──────────────────────────────────────────────────
# MAGIC     ia.invoice_no,
# MAGIC     ia.salesorder_no,
# MAGIC     ia.fg_item_no,
# MAGIC 
# MAGIC     -- ── Invoice info ──────────────────────────────────────────
# MAGIC     ia.customer_no,
# MAGIC     COALESCE(sm.customer_name, ia.customer_no) AS customer_name,
# MAGIC     ia.invoice_posting_date,
# MAGIC     ia.fg_item_description,
# MAGIC     ia.fg_item_category,
# MAGIC     ia.fg_material,
# MAGIC     ia.cad_manager_team,
# MAGIC 
# MAGIC     -- ── Quantity & Amounts ────────────────────────────────────
# MAGIC     ia.fg_qty,
# MAGIC     ia.sell_amount_fcy,
# MAGIC     ia.actual_cost_fcy,
# MAGIC     ia.actual_cost_thb,
# MAGIC 
# MAGIC     -- ── BOM ADJ + PROS (from component_variance) ─────────────
# MAGIC     COALESCE(bs.bom_adj_thb, 0)              AS bom_adj_thb,
# MAGIC     COALESCE(bs.component_actual_thb, 0)     AS component_actual_thb,
# MAGIC     COALESCE(bs.pros_count, 0)               AS pros_count,
# MAGIC 
# MAGIC     -- ── BOM variance % ───────────────────────────────────────
# MAGIC     CASE WHEN COALESCE(bs.bom_adj_thb, 0) <> 0
# MAGIC          THEN ((COALESCE(bs.component_actual_thb, 0) - bs.bom_adj_thb) / bs.bom_adj_thb) * 100
# MAGIC          ELSE 0
# MAGIC     END                                      AS bom_var_pct,
# MAGIC 
# MAGIC     -- ── Unit prices ───────────────────────────────────────────
# MAGIC     CASE WHEN ia.fg_qty > 0
# MAGIC          THEN ia.sell_amount_fcy / ia.fg_qty
# MAGIC          ELSE 0
# MAGIC     END                                      AS sell_unit_price_fcy,
# MAGIC 
# MAGIC     CASE WHEN ia.fg_qty > 0
# MAGIC          THEN ia.actual_cost_thb / ia.fg_qty
# MAGIC          ELSE 0
# MAGIC     END                                      AS actual_unit_cost_thb,
# MAGIC 
# MAGIC     CASE WHEN ia.fg_qty > 0 AND COALESCE(bs.bom_adj_thb, 0) > 0
# MAGIC          THEN bs.bom_adj_thb / ia.fg_qty
# MAGIC          ELSE 0
# MAGIC     END                                      AS bom_unit_cost_thb,
# MAGIC 
# MAGIC     -- ── Sell amount in THB ────────────────────────────────────
# MAGIC     CASE WHEN sm.currency_factor > 0
# MAGIC          THEN ia.sell_amount_fcy / sm.currency_factor
# MAGIC          ELSE NULL
# MAGIC     END                                      AS sell_amount_thb,
# MAGIC 
# MAGIC     -- ── GM calculation ────────────────────────────────────────
# MAGIC     CASE WHEN sm.currency_factor > 0
# MAGIC          THEN (ia.sell_amount_fcy / sm.currency_factor) - ia.actual_cost_thb
# MAGIC          ELSE NULL
# MAGIC     END                                      AS gm_thb,
# MAGIC 
# MAGIC     CASE WHEN sm.currency_factor > 0 AND ia.sell_amount_fcy > 0
# MAGIC          THEN (((ia.sell_amount_fcy / sm.currency_factor) - ia.actual_cost_thb)
# MAGIC                / (ia.sell_amount_fcy / sm.currency_factor)) * 100
# MAGIC          ELSE NULL
# MAGIC     END                                      AS gm_pct,
# MAGIC 
# MAGIC     -- ── Item enrichment ───────────────────────────────────────
# MAGIC     ii.product_type,
# MAGIC     ii.metal_category_code,
# MAGIC     ii.item_category_code,
# MAGIC 
# MAGIC     -- ── SO metal fix price ────────────────────────────────────
# MAGIC     sm.so_order_date,
# MAGIC     sm.currency_code,
# MAGIC     sm.currency_factor,
# MAGIC     sm.gold_per_ounce_usd,
# MAGIC     sm.silver_per_ounce_usd,
# MAGIC 
# MAGIC     -- ── Metal price THB/gram ──────────────────────────────────
# MAGIC     CASE WHEN sm.gold_per_ounce_usd > 0 AND sm.currency_factor > 0
# MAGIC          THEN (sm.gold_per_ounce_usd / 31.1035) / sm.currency_factor
# MAGIC          ELSE NULL
# MAGIC     END                                      AS gold_thb_per_gram,
# MAGIC 
# MAGIC     CASE WHEN sm.silver_per_ounce_usd > 0 AND sm.currency_factor > 0
# MAGIC          THEN (sm.silver_per_ounce_usd / 31.1035) / sm.currency_factor
# MAGIC          ELSE NULL
# MAGIC     END                                      AS silver_thb_per_gram,
# MAGIC 
# MAGIC     -- ── Flag logic ────────────────────────────────────────────
# MAGIC     CASE
# MAGIC         WHEN sm.currency_factor > 0 AND ia.sell_amount_fcy > 0
# MAGIC              AND (((ia.sell_amount_fcy / sm.currency_factor) - ia.actual_cost_thb)
# MAGIC                   / (ia.sell_amount_fcy / sm.currency_factor)) * 100 < -50
# MAGIC         THEN 'OVER'
# MAGIC         WHEN sm.currency_factor > 0 AND ia.sell_amount_fcy > 0
# MAGIC              AND (((ia.sell_amount_fcy / sm.currency_factor) - ia.actual_cost_thb)
# MAGIC                   / (ia.sell_amount_fcy / sm.currency_factor)) * 100 < 0
# MAGIC         THEN 'WARN'
# MAGIC         ELSE 'OK'
# MAGIC     END                                      AS cost_flag
# MAGIC 
# MAGIC FROM invoice_agg ia
# MAGIC 
# MAGIC LEFT JOIN item_info ii
# MAGIC     ON ia.fg_item_no = ii.item_no
# MAGIC 
# MAGIC LEFT JOIN so_metal sm
# MAGIC     ON ia.salesorder_no = sm.sales_order_no
# MAGIC 
# MAGIC LEFT JOIN bom_summary bs
# MAGIC     ON  ia.salesorder_no = bs.sales_order_no
# MAGIC     AND ia.fg_item_no    = bs.fg_item_no
# MAGIC ;

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
