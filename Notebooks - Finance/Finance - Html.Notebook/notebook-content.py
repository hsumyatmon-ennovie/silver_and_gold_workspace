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

# # gold_item

# CELL ********************

# MAGIC %%sql
# MAGIC -- Notebook: nb_gold_item
# MAGIC -- Purpose: Item master dimension with precious metal flags and alloy type
# MAGIC -- Layer: Silver → Gold
# MAGIC -- Schedule: Daily 06:00 (first — no dependencies)
# MAGIC -- Dependencies: Silver_BC_Lakehouse.bc.Item
# MAGIC -- Target: Gold_Finance_Lakehouse.ct.gold_item
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CELL 1: CREATE TABLE
# MAGIC -- ============================================================
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Finance_Lakehouse.ct.gold_item
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact'   = 'true'
# MAGIC )
# MAGIC AS
# MAGIC SELECT
# MAGIC     it.`No.`                                            AS item_no,
# MAGIC     it.`Description`                                    AS description,
# MAGIC     it.`Item Category Code`                             AS item_category_code,
# MAGIC     it.`Base Unit of Measure`                           AS base_unit_of_measure,
# MAGIC 
# MAGIC     CASE it.`Replenishment System`
# MAGIC         WHEN 0 THEN 'Purchase'
# MAGIC         WHEN 1 THEN 'Prod. Order'
# MAGIC         WHEN 2 THEN 'Assembly'
# MAGIC         ELSE CAST(it.`Replenishment System` AS STRING)
# MAGIC     END                                                 AS replenishment_system,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN it.`Item Category Code` IN (
# MAGIC             'ALLOY', 'PURE METAL', 'SOLDER', 'WIRE', 'MIXED METAL'
# MAGIC         ) THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END                                                 AS is_precious_metal,
# MAGIC 
# MAGIC     it.`Alloy Type`                                     AS alloy_type,
# MAGIC     it.`Inventory Posting Group`                        AS inventory_posting_group
# MAGIC 
# MAGIC FROM Silver_BC_Lakehouse.bc.Item it
# MAGIC ;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CELL 2: OPTIMIZE
# MAGIC -- ============================================================
# MAGIC 
# MAGIC OPTIMIZE Gold_Finance_Lakehouse.ct.gold_item ZORDER BY (item_no);
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CELL 3: QUALITY CHECK
# MAGIC -- ============================================================
# MAGIC 
# MAGIC SELECT 'gold_item' AS tbl,
# MAGIC        COUNT(*) AS total_items,
# MAGIC        SUM(CASE WHEN is_precious_metal = TRUE THEN 1 ELSE 0 END) AS precious_items,
# MAGIC        COUNT(DISTINCT alloy_type) AS alloy_types,
# MAGIC        COUNT(DISTINCT item_category_code) AS category_codes,
# MAGIC        SUM(CASE WHEN item_no IS NULL THEN 1 ELSE 0 END) AS null_keys
# MAGIC FROM Gold_Finance_Lakehouse.ct.gold_item;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # gold item category map

# CELL ********************

# MAGIC %%sql
# MAGIC -- Notebook: nb_gold_item_category_map
# MAGIC -- Purpose: Category mapping — BC child categories → parent categories for report grouping
# MAGIC -- Layer: Silver → Gold
# MAGIC -- Schedule: Daily 06:00 (parallel with nb_01 — no dependencies)
# MAGIC -- Dependencies: Silver_BC_Lakehouse.bc.`Item Category`
# MAGIC -- Target: Gold_Finance_Lakehouse.ct.gold_item_category_map
# MAGIC --
# MAGIC -- DESIGN NOTE:
# MAGIC --   Previously hardcoded 19-row VALUES table. Now sourced from BC Item Category master.
# MAGIC --   BC uses a parent-child hierarchy with [Has Children] flag:
# MAGIC --     Parent rows (Has Children = true):  ACCESSORIES-PRT-CAT, METAL-PRT-CAT, etc.
# MAGIC --     Leaf rows   (Has Children = false): FINDINGS, ALLOY, DIAMOND NAT, etc.
# MAGIC --   We join leaf → parent to get the parent description as the report grouping.
# MAGIC --
# MAGIC --   The color_in_report column is assigned via a CASE on parent category code
# MAGIC --   to maintain consistent report styling. New parent categories get a default gray.
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CELL 1: CREATE TABLE
# MAGIC -- ============================================================
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Finance_Lakehouse.ct.gold_item_category_map
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC SELECT
# MAGIC     child.`Code`                                            AS bc_sub_category,
# MAGIC 
# MAGIC     -- Parent description = report group name
# MAGIC     COALESCE(parent.`Description`, 'Other')                 AS parent_category,
# MAGIC 
# MAGIC     -- Report color: assigned per parent category for consistent styling
# MAGIC     CASE parent.`Code`
# MAGIC         WHEN 'METAL-PRT-CAT'        THEN '#D4A017'     -- Precious Metals (gold)
# MAGIC         WHEN 'DIAMONDS-PRT-CAT'     THEN '#7C4DFF'     -- Diamonds (purple)
# MAGIC         WHEN 'STONE-PEARL-PRT-CAT'  THEN '#00897B'     -- Stones & Pearls (teal)
# MAGIC         WHEN 'ACCESSORIES-PRT-CAT'  THEN '#5C6BC0'     -- Findings/Accessories (indigo)
# MAGIC         WHEN 'PARTS'                THEN '#EF6C00'     -- Casting Parts (orange)
# MAGIC         WHEN 'PLATING-PRT-CAT'      THEN '#1565C0'     -- Plating (blue)
# MAGIC         WHEN 'STORE-ITEM-PRT-CAT'   THEN '#78909C'     -- Consumables (gray-blue)
# MAGIC         WHEN 'FG-PRT-CAT'           THEN '#43A047'     -- Finished Goods (green)
# MAGIC         WHEN 'MST-PRT-CAT'          THEN '#8D6E63'     -- Master Piece (brown)
# MAGIC         ELSE '#78909C'                                  -- Default gray
# MAGIC     END                                                     AS color_in_report
# MAGIC 
# MAGIC FROM Silver_BC_Lakehouse.bc.`Item Category` child
# MAGIC 
# MAGIC LEFT JOIN Silver_BC_Lakehouse.bc.`Item Category` parent
# MAGIC     ON child.`Parent Category` = parent.`Code`
# MAGIC 
# MAGIC WHERE child.`Has Children` = false
# MAGIC   AND child.`Code` IS NOT NULL
# MAGIC   AND child.`Code` != ''
# MAGIC ;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CELL 2: QUALITY CHECK
# MAGIC -- ============================================================
# MAGIC 
# MAGIC SELECT COUNT(*) AS total_rows,
# MAGIC        COUNT(DISTINCT parent_category) AS parent_categories,
# MAGIC        COUNT(DISTINCT bc_sub_category) AS sub_categories,
# MAGIC        SUM(CASE WHEN parent_category = 'Other' THEN 1 ELSE 0 END) AS unmapped
# MAGIC FROM Gold_Finance_Lakehouse.ct.gold_item_category_map;
# MAGIC 
# MAGIC -- Detail check: all mappings
# MAGIC SELECT bc_sub_category, parent_category, color_in_report
# MAGIC FROM Gold_Finance_Lakehouse.ct.gold_item_category_map
# MAGIC ORDER BY parent_category, bc_sub_category;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# # gold sales archive

# CELL ********************

# MAGIC %%sql
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Finance_Lakehouse.ct.gold_sales_archive
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC WITH si_agg AS (
# MAGIC     SELECT
# MAGIC         si.invoice_no,
# MAGIC         si.invoice_posting_date,
# MAGIC         si.customer_no,
# MAGIC         si.salesorder_no,
# MAGIC         SUM(si.invoiceline_amount)                     AS sell_amount_fcy,
# MAGIC         SUM(si.item_quantity * si.item_unit_cost_THB) AS actual_cost_thb,
# MAGIC         SUM(si.item_quantity)                          AS total_qty,
# MAGIC         COUNT(*)                                       AS line_count
# MAGIC     FROM Silver_Finance_Lakehouse.fa.silver_sales_invoice_header_line si
# MAGIC     WHERE si.item_type = 'Item'
# MAGIC       AND si.customer_no IS NOT NULL
# MAGIC       AND si.customer_no <> ''
# MAGIC     GROUP BY
# MAGIC         si.invoice_no,
# MAGIC         si.invoice_posting_date,
# MAGIC         si.customer_no,
# MAGIC         si.salesorder_no
# MAGIC ),
# MAGIC sh_dedup AS (
# MAGIC     SELECT
# MAGIC         `No.`,
# MAGIC         `Currency Code`,
# MAGIC         `Gold Per Ounce`,
# MAGIC         `Silver Per Ounce`,
# MAGIC         `Currency Factor`
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC                ROW_NUMBER() OVER (PARTITION BY `No.` ORDER BY `No.`) AS rn
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Sales Header`
# MAGIC     ) t
# MAGIC     WHERE rn = 1
# MAGIC )
# MAGIC SELECT
# MAGIC     si.invoice_no AS ar_no,
# MAGIC     si.invoice_posting_date AS posting_date,
# MAGIC     si.customer_no,
# MAGIC     COALESCE(cust.`Name`, si.customer_no) AS customer_name,
# MAGIC     si.salesorder_no,
# MAGIC     COALESCE(sh.`Currency Code`, '') AS currency_code,
# MAGIC     si.sell_amount_fcy,
# MAGIC     si.actual_cost_thb,
# MAGIC     si.total_qty,
# MAGIC     si.line_count,
# MAGIC     sh.`Gold Per Ounce` AS gold_per_ounce,
# MAGIC     sh.`Silver Per Ounce` AS silver_per_ounce,
# MAGIC     sh.`Currency Factor` AS currency_factor,
# MAGIC     CASE
# MAGIC         WHEN sh.`Gold Per Ounce` > 0 AND sh.`Currency Factor` > 0
# MAGIC         THEN (sh.`Gold Per Ounce` / 31.1035) / sh.`Currency Factor`
# MAGIC     END AS gold_thb_per_gram,
# MAGIC     CASE
# MAGIC         WHEN sh.`Silver Per Ounce` > 0 AND sh.`Currency Factor` > 0
# MAGIC         THEN (sh.`Silver Per Ounce` / 31.1035) / sh.`Currency Factor`
# MAGIC     END AS silver_thb_per_gram,
# MAGIC     DATE_FORMAT(si.invoice_posting_date, 'yyyy-MM') AS posting_year_month
# MAGIC FROM si_agg si
# MAGIC LEFT JOIN sh_dedup sh
# MAGIC     ON si.salesorder_no = sh.`No.`
# MAGIC LEFT JOIN Silver_BC_Lakehouse.bc.`Customer` cust
# MAGIC     ON si.customer_no = cust.`No.`;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # gold sales archive line

# CELL ********************

# MAGIC %%sql
# MAGIC -- ═══════════════════════════════════════════════════════════════
# MAGIC -- nb_04_gold_sales_archive_line.sql (FIXED v2)
# MAGIC -- Fix: column names match silver_sales_invoice_header_line actual schema
# MAGIC --   invoice_lineno (not invoice_line_no)
# MAGIC --   invoiceline_unit_price (not item_unit_price)
# MAGIC --   + JOIN Item for [Product Type]
# MAGIC -- ═══════════════════════════════════════════════════════════════
# MAGIC 
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Finance_Lakehouse.ct.gold_sales_archive_line
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES('delta.autoOptimize.optimizeWrite' = 'false')
# MAGIC AS
# MAGIC SELECT
# MAGIC     si.invoice_no                              AS ar_no,
# MAGIC     si.invoice_lineno                          AS line_no,
# MAGIC     si.item_no                                 AS item_no,
# MAGIC     si.item_description                        AS description,
# MAGIC     COALESCE(i.`Product Type`, '')             AS product_type,
# MAGIC     si.item_quantity                            AS quantity,
# MAGIC     si.invoiceline_unit_price                  AS unit_price,
# MAGIC     si.invoiceline_amount                      AS amount,
# MAGIC     DATE_FORMAT(si.invoice_posting_date, 'yyyy-MM') AS posting_year_month
# MAGIC 
# MAGIC FROM Silver_Finance_Lakehouse.fa.silver_sales_invoice_header_line si
# MAGIC 
# MAGIC LEFT JOIN Silver_BC_Lakehouse.bc.Item i
# MAGIC     ON si.item_no = i.`No.`
# MAGIC 
# MAGIC WHERE si.item_type = 'Item'
# MAGIC   AND si.item_no IS NOT NULL
# MAGIC   AND TRIM(si.item_no) != ''
# MAGIC   AND si.item_quantity > 0
# MAGIC ;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # gold prod order

# CELL ********************

# MAGIC %%sql
# MAGIC -- Notebook: nb_gold_prod_order (v3)
# MAGIC -- Fix: datetimeRebaseModeInRead + STRING-first date handling for ancient BC dates
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CELL 1: All SETs + CREATE TABLE in SAME CELL
# MAGIC -- ============================================================
# MAGIC 
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC SET spark.microsoft.delta.optimizeWrite.numShuffleBlocks = 1;
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Finance_Lakehouse.ct.gold_prod_order
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'false',
# MAGIC     'delta.autoOptimize.autoCompact'   = 'true'
# MAGIC )
# MAGIC AS
# MAGIC SELECT
# MAGIC     po.`No.`                                            AS pro_no,
# MAGIC     po.`Source No.`                                     AS source_no,
# MAGIC     po.`Sales Order No.`                                AS ar_no,
# MAGIC 
# MAGIC     CAST(po.`Quantity` AS DECIMAL(18,5))                AS quantity,
# MAGIC 
# MAGIC     CASE WHEN CAST(po.`Due Date` AS STRING) < '1900-01-01'
# MAGIC               OR po.`Due Date` IS NULL
# MAGIC          THEN NULL
# MAGIC          ELSE CAST(po.`Due Date` AS DATE)
# MAGIC     END                                                 AS due_date,
# MAGIC 
# MAGIC     CASE WHEN CAST(po.`Finished Date` AS STRING) < '1900-01-01'
# MAGIC               OR po.`Finished Date` IS NULL
# MAGIC          THEN NULL
# MAGIC          ELSE CAST(po.`Finished Date` AS DATE)
# MAGIC     END                                                 AS finished_date,
# MAGIC 
# MAGIC     po.`For Prod.Order No.`                             AS parent_pro_no,
# MAGIC 
# MAGIC     po.`Status`                                         AS status
# MAGIC 
# MAGIC FROM Silver_BC_Lakehouse.bc.`Production Order` po
# MAGIC WHERE po.`Status` = 'Finished'
# MAGIC ;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CELL 2: OPTIMIZE
# MAGIC -- ============================================================
# MAGIC 
# MAGIC REFRESH TABLE Gold_Finance_Lakehouse.ct.gold_prod_order;
# MAGIC OPTIMIZE Gold_Finance_Lakehouse.ct.gold_prod_order ZORDER BY (pro_no, ar_no);
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CELL 3: Re-enable optimizeWrite
# MAGIC -- ============================================================
# MAGIC 
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = true;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # gold prod order component variance

# CELL ********************

# MAGIC %%sql
# MAGIC -- nb_06_gold_prod_order_component_variance (FIX v6.1 - performance + safe actual agg)
# MAGIC --
# MAGIC -- v6.1 Changes (2026-03-25):
# MAGIC --   ★ Performance optimization:
# MAGIC --     - Scan Production Order once via finished_pro
# MAGIC --     - Scan Prod Order Component once via bom
# MAGIC --     - Scan Item Ledger Entry once via ile_base
# MAGIC --     - Remove redundant child_pro IN (SELECT DISTINCT ...)
# MAGIC --     - Build child_pro from bom instead of rescanning POC
# MAGIC --   ★ Correctness protection:
# MAGIC --     - Split actual into qty_agg and cost_agg
# MAGIC --     - Prevent quantity overcount when one ILE entry links to multiple Value Entry rows
# MAGIC --   ★ Logic preserved:
# MAGIC --     - PK logic = Prod.Order No. + Prod.Order Line No. + Line No.
# MAGIC --     - Actual join uses pro_no + item_no + prod_order_line_no
# MAGIC --     - VE still joins at ILE entry level before cost aggregation
# MAGIC --
# MAGIC -- v5 Changes:
# MAGIC --   Added [Prod. Order Line No.] as prod_order_line_no
# MAGIC --   ILE/VE join uses 3-part key
# MAGIC --   BOM Total sums all component lines correctly
# MAGIC -- ═══════════════════════════════════════════════════════════════
# MAGIC 
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS Gold_Finance_Lakehouse.ct.gold_prod_order_component_variance;
# MAGIC 
# MAGIC CREATE TABLE Gold_Finance_Lakehouse.ct.gold_prod_order_component_variance
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES('delta.autoOptimize.optimizeWrite' = 'false')
# MAGIC AS
# MAGIC 
# MAGIC WITH
# MAGIC -- ─── Finished Production Orders (read once) ─────────────────
# MAGIC finished_pro AS (
# MAGIC     SELECT
# MAGIC         po.`No.`        AS pro_no,
# MAGIC         po.`Source No.` AS source_no
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Production Order` po
# MAGIC     WHERE po.`Status` = 'Finished'
# MAGIC ),
# MAGIC 
# MAGIC -- ─── BOM side: Prod Order Component ─────────────────────────
# MAGIC -- PRIMARY KEY: Prod.Order No. + Prod.Order Line No. + Line No.
# MAGIC bom AS (
# MAGIC     SELECT
# MAGIC         poc.`Prod. Order No.`       AS pro_no,
# MAGIC         poc.`Prod. Order Line No.`  AS prod_order_line_no,
# MAGIC         poc.`Line No.`              AS comp_line_no,
# MAGIC         poc.`Item No.`              AS item_no,
# MAGIC         poc.`Description`           AS description,
# MAGIC         poc.`Expected Quantity`     AS bom_quantity,
# MAGIC         poc.`Unit Cost`             AS bom_unit_cost,
# MAGIC         poc.`Cost Amount`           AS bom_total,
# MAGIC         poc.`Unit of Measure Code`  AS uom_code
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Prod Order Component` poc
# MAGIC ),
# MAGIC 
# MAGIC -- ─── Actual side base: filtered ILE (read once) ─────────────
# MAGIC -- ILE.Quantity is NEGATIVE for consumption → ABS()
# MAGIC ile_base AS (
# MAGIC     SELECT
# MAGIC         ile.`Entry No.`       AS ile_entry_no,
# MAGIC         ile.`Order No.`       AS pro_no,
# MAGIC         ile.`Item No.`        AS item_no,
# MAGIC         ile.`Order Line No.`  AS prod_order_line_no,
# MAGIC         ABS(ile.`Quantity`)   AS quantity_abs
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` ile
# MAGIC     WHERE ile.`Entry Type` = 'Consumption'
# MAGIC ),
# MAGIC 
# MAGIC -- ─── Actual quantity only from ILE ──────────────────────────
# MAGIC qty_agg AS (
# MAGIC     SELECT
# MAGIC         ib.pro_no,
# MAGIC         ib.item_no,
# MAGIC         ib.prod_order_line_no,
# MAGIC         SUM(ib.quantity_abs) AS actual_quantity
# MAGIC     FROM ile_base ib
# MAGIC     GROUP BY
# MAGIC         ib.pro_no,
# MAGIC         ib.item_no,
# MAGIC         ib.prod_order_line_no
# MAGIC ),
# MAGIC 
# MAGIC -- ─── Actual cost from VE joined to ILE entry level ──────────
# MAGIC cost_agg AS (
# MAGIC     SELECT
# MAGIC         ib.pro_no,
# MAGIC         ib.item_no,
# MAGIC         ib.prod_order_line_no,
# MAGIC         ABS(SUM(COALESCE(ve.`Cost Amount (Actual)`, 0))) AS actual_cost
# MAGIC     FROM ile_base ib
# MAGIC     LEFT JOIN Silver_BC_Lakehouse.bc.`Value Entry` ve
# MAGIC         ON ve.`Item Ledger Entry No.` = ib.ile_entry_no
# MAGIC     GROUP BY
# MAGIC         ib.pro_no,
# MAGIC         ib.item_no,
# MAGIC         ib.prod_order_line_no
# MAGIC ),
# MAGIC 
# MAGIC -- ─── Final actual aggregation ───────────────────────────────
# MAGIC actual_agg AS (
# MAGIC     SELECT
# MAGIC         q.pro_no,
# MAGIC         q.item_no,
# MAGIC         q.prod_order_line_no,
# MAGIC         q.actual_quantity,
# MAGIC         COALESCE(c.actual_cost, 0) AS actual_cost
# MAGIC     FROM qty_agg q
# MAGIC     LEFT JOIN cost_agg c
# MAGIC         ON q.pro_no = c.pro_no
# MAGIC         AND q.item_no = c.item_no
# MAGIC         AND q.prod_order_line_no = c.prod_order_line_no
# MAGIC ),
# MAGIC 
# MAGIC -- ─── Item enrichment ────────────────────────────────────────
# MAGIC item AS (
# MAGIC     SELECT
# MAGIC         i.`No.`                     AS item_no,
# MAGIC         i.`Item Category Code`      AS item_category_code,
# MAGIC         i.`Replenishment System`    AS replenishment_system,
# MAGIC         CASE
# MAGIC             WHEN i.`Inventory Posting Group` IN ('PURE METAL','ALLOY','MIXED METAL','SOLDER','WIRE')
# MAGIC             THEN TRUE ELSE FALSE
# MAGIC         END                         AS is_precious_metal,
# MAGIC         i.`Inventory Posting Group` AS alloy_type
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item` i
# MAGIC ),
# MAGIC 
# MAGIC -- ─── UOM2 (OZT > CT > DWT) ──────────────────────────────────
# MAGIC uom2 AS (
# MAGIC     SELECT item_no, uom2_code, qty_per_uom2
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             ium.`Item No.`                 AS item_no,
# MAGIC             ium.`Code`                     AS uom2_code,
# MAGIC             ium.`Qty. per Unit of Measure` AS qty_per_uom2,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY ium.`Item No.`
# MAGIC                 ORDER BY CASE ium.`Code`
# MAGIC                     WHEN 'OZT' THEN 1
# MAGIC                     WHEN 'CT'  THEN 2
# MAGIC                     WHEN 'DWT' THEN 3
# MAGIC                     ELSE 999
# MAGIC                 END
# MAGIC             ) AS rn
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Item Unit of Measure` ium
# MAGIC         WHERE ium.`Code` IN ('OZT','CT','DWT')
# MAGIC     ) ranked
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC -- ─── Child PRO detection from BOM + finished_pro ────────────
# MAGIC child_pro AS (
# MAGIC     SELECT
# MAGIC         b.pro_no               AS parent_pro_no,
# MAGIC         b.prod_order_line_no,
# MAGIC         b.comp_line_no,
# MAGIC         b.item_no,
# MAGIC         fp.pro_no              AS child_pro_no
# MAGIC     FROM bom b
# MAGIC     JOIN finished_pro fp
# MAGIC         ON fp.source_no = b.item_no
# MAGIC )
# MAGIC 
# MAGIC -- ─── Final assembly ─────────────────────────────────────────
# MAGIC SELECT
# MAGIC     bom.pro_no,
# MAGIC     bom.prod_order_line_no,
# MAGIC     bom.comp_line_no,
# MAGIC     bom.item_no,
# MAGIC     bom.description,
# MAGIC     COALESCE(item.item_category_code, '')   AS item_category_code,
# MAGIC     COALESCE(item.replenishment_system, '') AS replenishment_system,
# MAGIC     cp.child_pro_no,
# MAGIC 
# MAGIC     -- BOM (planned)
# MAGIC     COALESCE(bom.bom_quantity, 0)           AS bom_quantity,
# MAGIC     COALESCE(bom.bom_unit_cost, 0)          AS bom_unit_cost,
# MAGIC     COALESCE(bom.bom_total, 0)              AS bom_total,
# MAGIC 
# MAGIC     -- Actual
# MAGIC     COALESCE(act.actual_quantity, 0)        AS actual_quantity,
# MAGIC     COALESCE(act.actual_cost, 0)            AS actual_cost,
# MAGIC     CASE
# MAGIC         WHEN COALESCE(act.actual_quantity, 0) > 0
# MAGIC         THEN COALESCE(act.actual_cost, 0) / act.actual_quantity
# MAGIC         ELSE 0
# MAGIC     END                                     AS actual_unit_cost,
# MAGIC 
# MAGIC     -- Variance %
# MAGIC     CASE
# MAGIC         WHEN COALESCE(bom.bom_quantity, 0) > 0
# MAGIC         THEN ROUND((COALESCE(act.actual_quantity, 0) - bom.bom_quantity) / bom.bom_quantity * 100, 2)
# MAGIC         ELSE 0
# MAGIC     END                                     AS qty_variance_pct,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN COALESCE(bom.bom_total, 0) > 0
# MAGIC         THEN ROUND((COALESCE(act.actual_cost, 0) - bom.bom_total) / bom.bom_total * 100, 2)
# MAGIC         ELSE 0
# MAGIC     END                                     AS cost_variance_pct,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN COALESCE(bom.bom_unit_cost, 0) > 0
# MAGIC         THEN ROUND(
# MAGIC             (
# MAGIC                 CASE
# MAGIC                     WHEN COALESCE(act.actual_quantity, 0) > 0
# MAGIC                     THEN COALESCE(act.actual_cost, 0) / act.actual_quantity
# MAGIC                     ELSE 0
# MAGIC                 END
# MAGIC                 - bom.bom_unit_cost
# MAGIC             ) / bom.bom_unit_cost * 100, 2
# MAGIC         )
# MAGIC         ELSE 0
# MAGIC     END                                     AS uc_variance_pct,
# MAGIC 
# MAGIC     -- UOM
# MAGIC     bom.uom_code,
# MAGIC     uom2.uom2_code,
# MAGIC     CASE
# MAGIC         WHEN uom2.qty_per_uom2 > 0
# MAGIC         THEN ROUND(bom.bom_quantity / uom2.qty_per_uom2, 5)
# MAGIC         ELSE NULL
# MAGIC     END                                     AS bom_qty_uom2,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN uom2.qty_per_uom2 > 0
# MAGIC         THEN ROUND(COALESCE(act.actual_quantity, 0) / uom2.qty_per_uom2, 5)
# MAGIC         ELSE NULL
# MAGIC     END                                     AS actual_qty_uom2,
# MAGIC 
# MAGIC     DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM')  AS posting_year_month
# MAGIC 
# MAGIC FROM bom
# MAGIC INNER JOIN finished_pro pro
# MAGIC     ON bom.pro_no = pro.pro_no
# MAGIC LEFT JOIN item
# MAGIC     ON bom.item_no = item.item_no
# MAGIC LEFT JOIN actual_agg act
# MAGIC     ON bom.pro_no = act.pro_no
# MAGIC     AND bom.item_no = act.item_no
# MAGIC     AND bom.prod_order_line_no = act.prod_order_line_no
# MAGIC LEFT JOIN uom2
# MAGIC     ON bom.item_no = uom2.item_no
# MAGIC LEFT JOIN child_pro cp
# MAGIC     ON bom.pro_no = cp.parent_pro_no
# MAGIC     AND bom.prod_order_line_no = cp.prod_order_line_no
# MAGIC     AND bom.comp_line_no = cp.comp_line_no
# MAGIC ;
# MAGIC 
# MAGIC REFRESH TABLE Gold_Finance_Lakehouse.ct.gold_prod_order_component_variance;
# MAGIC 
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # gold_bom_planned

# CELL ********************

# MAGIC %%sql
# MAGIC -- nb_06a: gold_bom_planned
# MAGIC -- Stage 1: BOM side from Prod Order Component (Finished PROs only)
# MAGIC -- Grain: pro_no + prod_order_line_no + comp_line_no
# MAGIC 
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Finance_Lakehouse.ct.gold_bom_planned
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES('delta.autoOptimize.optimizeWrite' = 'false')
# MAGIC AS
# MAGIC SELECT
# MAGIC     poc.`Prod. Order No.`              AS pro_no,
# MAGIC     poc.`Prod. Order Line No.`         AS prod_order_line_no,
# MAGIC     poc.`Line No.`                     AS comp_line_no,
# MAGIC     poc.`Item No.`                     AS item_no,
# MAGIC     poc.`Description`                  AS description,
# MAGIC     poc.`Expected Quantity`             AS bom_quantity,
# MAGIC     poc.`Unit Cost`                    AS bom_unit_cost,
# MAGIC     poc.`Cost Amount`                  AS bom_total,
# MAGIC     poc.`Unit of Measure Code`         AS uom_code
# MAGIC FROM Silver_BC_Lakehouse.bc.`Prod Order Component` poc
# MAGIC WHERE poc.`Prod. Order No.` IN (
# MAGIC     SELECT po.`No.`
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Production Order` po
# MAGIC     WHERE po.`Status` = 'Finished'
# MAGIC );
# MAGIC 
# MAGIC SELECT 'gold_bom_planned' AS tbl, COUNT(*) AS rows FROM Gold_Finance_Lakehouse.ct.gold_bom_planned;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # gold_ile_consumption

# CELL ********************

# MAGIC %%sql
# MAGIC -- nb_06b: gold_ile_consumption
# MAGIC -- Stage 2: Actual quantity from Item Ledger Entry (Consumption)
# MAGIC -- Grain: pro_no + prod_order_line_no + item_no
# MAGIC 
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Finance_Lakehouse.ct.gold_ile_consumption
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES('delta.autoOptimize.optimizeWrite' = 'false')
# MAGIC AS
# MAGIC SELECT
# MAGIC     ile.`Order No.`                    AS pro_no,
# MAGIC     ile.`Item No.`                     AS item_no,
# MAGIC     ile.`Order Line No.`               AS prod_order_line_no,
# MAGIC     ABS(SUM(ile.`Quantity`))            AS actual_quantity
# MAGIC FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` ile
# MAGIC WHERE ile.`Entry Type` = 'Consumption'
# MAGIC   AND ile.`Order No.` IN (
# MAGIC       SELECT po.`No.`
# MAGIC       FROM Silver_BC_Lakehouse.bc.`Production Order` po
# MAGIC       WHERE po.`Status` = 'Finished'
# MAGIC   )
# MAGIC GROUP BY ile.`Order No.`, ile.`Item No.`, ile.`Order Line No.`;
# MAGIC 
# MAGIC SELECT 'gold_ile_consumption' AS tbl, COUNT(*) AS rows FROM Gold_Finance_Lakehouse.ct.gold_ile_consumption;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # gold_ve_cost

# CELL ********************

# MAGIC %%sql
# MAGIC -- nb_06c: gold_ve_cost
# MAGIC -- Stage 3: Actual cost from Value Entry via ILE
# MAGIC -- Grain: pro_no + prod_order_line_no + item_no
# MAGIC 
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Finance_Lakehouse.ct.gold_ve_cost
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES('delta.autoOptimize.optimizeWrite' = 'false')
# MAGIC AS
# MAGIC SELECT
# MAGIC     ile.`Order No.`                    AS pro_no,
# MAGIC     ile.`Item No.`                     AS item_no,
# MAGIC     ile.`Order Line No.`               AS prod_order_line_no,
# MAGIC     ABS(SUM(ve.`Cost Amount (Actual)`)) AS actual_cost
# MAGIC FROM Silver_BC_Lakehouse.bc.`Value Entry` ve
# MAGIC JOIN Silver_BC_Lakehouse.bc.`Item Ledger Entry` ile
# MAGIC     ON ve.`Item Ledger Entry No.` = ile.`Entry No.`
# MAGIC WHERE ile.`Entry Type` = 'Consumption'
# MAGIC   AND ile.`Order No.` IN (
# MAGIC       SELECT po.`No.`
# MAGIC       FROM Silver_BC_Lakehouse.bc.`Production Order` po
# MAGIC       WHERE po.`Status` = 'Finished'
# MAGIC   )
# MAGIC GROUP BY ile.`Order No.`, ile.`Item No.`, ile.`Order Line No.`;
# MAGIC 
# MAGIC SELECT 'gold_ve_cost' AS tbl, COUNT(*) AS rows FROM Gold_Finance_Lakehouse.ct.gold_ve_cost;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # gold_prod_order_component_variance

# CELL ********************

# MAGIC %%sql
# MAGIC -- nb_06d LITE — ไม่มี child_pro (test speed)
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Finance_Lakehouse.ct.gold_prod_order_component_variance
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES('delta.autoOptimize.optimizeWrite' = 'false')
# MAGIC AS
# MAGIC SELECT
# MAGIC     bom.pro_no,
# MAGIC     bom.prod_order_line_no,
# MAGIC     bom.comp_line_no,
# MAGIC     bom.item_no,
# MAGIC     bom.description,
# MAGIC     COALESCE(i.`Item Category Code`, '')   AS item_category_code,
# MAGIC     COALESCE(i.`Replenishment System`, '') AS replenishment_system,
# MAGIC     CAST(NULL AS STRING)                    AS child_pro_no,
# MAGIC 
# MAGIC     COALESCE(bom.bom_quantity, 0)           AS bom_quantity,
# MAGIC     COALESCE(bom.bom_unit_cost, 0)          AS bom_unit_cost,
# MAGIC     COALESCE(bom.bom_total, 0)              AS bom_total,
# MAGIC 
# MAGIC     COALESCE(ile.actual_quantity, 0)         AS actual_quantity,
# MAGIC     COALESCE(ve.actual_cost, 0)              AS actual_cost,
# MAGIC     CASE WHEN COALESCE(ile.actual_quantity, 0) > 0
# MAGIC          THEN COALESCE(ve.actual_cost, 0) / ile.actual_quantity
# MAGIC          ELSE 0 END                          AS actual_unit_cost,
# MAGIC 
# MAGIC     CASE WHEN COALESCE(bom.bom_quantity, 0) > 0
# MAGIC          THEN ROUND((COALESCE(ile.actual_quantity, 0) - bom.bom_quantity) / bom.bom_quantity * 100, 2)
# MAGIC          ELSE 0 END                          AS qty_variance_pct,
# MAGIC     CASE WHEN COALESCE(bom.bom_total, 0) > 0
# MAGIC          THEN ROUND((COALESCE(ve.actual_cost, 0) - bom.bom_total) / bom.bom_total * 100, 2)
# MAGIC          ELSE 0 END                          AS cost_variance_pct,
# MAGIC     CASE WHEN COALESCE(bom.bom_unit_cost, 0) > 0
# MAGIC          THEN ROUND((CASE WHEN COALESCE(ile.actual_quantity, 0) > 0
# MAGIC               THEN COALESCE(ve.actual_cost, 0) / ile.actual_quantity ELSE 0 END
# MAGIC               - bom.bom_unit_cost) / bom.bom_unit_cost * 100, 2)
# MAGIC          ELSE 0 END                          AS uc_variance_pct,
# MAGIC 
# MAGIC     bom.uom_code,
# MAGIC     CAST(NULL AS STRING)                     AS uom2_code,
# MAGIC     CAST(NULL AS DECIMAL(18,5))              AS bom_qty_uom2,
# MAGIC     CAST(NULL AS DECIMAL(18,5))              AS actual_qty_uom2,
# MAGIC     DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM')   AS posting_year_month
# MAGIC 
# MAGIC FROM Gold_Finance_Lakehouse.ct.gold_bom_planned bom
# MAGIC LEFT JOIN Silver_BC_Lakehouse.bc.`Item` i ON bom.item_no = i.`No.`
# MAGIC LEFT JOIN Gold_Finance_Lakehouse.ct.gold_ile_consumption ile
# MAGIC     ON bom.pro_no = ile.pro_no AND bom.item_no = ile.item_no AND bom.prod_order_line_no = ile.prod_order_line_no
# MAGIC LEFT JOIN Gold_Finance_Lakehouse.ct.gold_ve_cost ve
# MAGIC     ON bom.pro_no = ve.pro_no AND bom.item_no = ve.item_no AND bom.prod_order_line_no = ve.prod_order_line_no
# MAGIC ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # gold_invoice_pro_map

# CELL ********************

# MAGIC %%sql
# MAGIC -- ████████████████████████████████████████████████████████████████████████████
# MAGIC -- Notebook: nb_07_gold_invoice_pro_map
# MAGIC -- Purpose: Invoice → PRO lot/serial tracing for Costing Intelligence report
# MAGIC -- Layer: Silver → Gold (Gold_Finance_Lakehouse)
# MAGIC -- Schedule: Daily (after BC mirror)
# MAGIC -- Dependencies:
# MAGIC --   Silver_Finance_Lakehouse.fa.silver_sales_invoice_header_line
# MAGIC --   Silver_BC_Lakehouse.bc.[Item Ledger Entry]
# MAGIC --
# MAGIC -- v3 Changes:
# MAGIC --   + Fixed fan-out: invoice has N lines per shipment, sale ILE has N entries
# MAGIC --     per shipment → N×N cross-join. Fix: use DISTINCT invoice (ar_no, item_no,
# MAGIC --     shipment_no) before joining to sale ILE, and DISTINCT on final output.
# MAGIC --   + Serial No. tracing for items tracked by serial (e.g. DE BEERS gold)
# MAGIC -- ████████████████████████████████████████████████████████████████████████████
# MAGIC 
# MAGIC 
# MAGIC -- ████████████████████████████████████████████████████████████████████████████
# MAGIC -- CELL 0: Config
# MAGIC -- ████████████████████████████████████████████████████████████████████████████
# MAGIC 
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC 
# MAGIC 
# MAGIC -- ████████████████████████████████████████████████████████████████████████████
# MAGIC -- CELL 1: Build gold_invoice_pro_map (Lot + Serial tracing, fan-out fixed)
# MAGIC -- ████████████████████████████████████████████████████████████████████████████
# MAGIC 
# MAGIC DROP TABLE IF EXISTS Gold_Finance_Lakehouse.ct.gold_invoice_pro_map;
# MAGIC 
# MAGIC CREATE TABLE Gold_Finance_Lakehouse.ct.gold_invoice_pro_map
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC WITH
# MAGIC -- ─── Invoice: DISTINCT (ar_no, item_no, shipment_no) to prevent fan-out ─────
# MAGIC -- An invoice with 15 lines for same item+shipment should produce 1 row here,
# MAGIC -- not 15 (which would cross-join with 15 sale ILEs → 225 rows)
# MAGIC invoice_shipments AS (
# MAGIC     SELECT DISTINCT
# MAGIC         si.invoice_no                AS ar_no,
# MAGIC         si.item_no,
# MAGIC         SUM(si.item_quantity)        AS invoice_qty,
# MAGIC         si.shipment_no
# MAGIC     FROM Silver_Finance_Lakehouse.fa.silver_sales_invoice_header_line si
# MAGIC     WHERE si.item_type = 'Item'
# MAGIC       AND si.item_quantity > 0
# MAGIC       AND si.shipment_no IS NOT NULL
# MAGIC       AND TRIM(si.shipment_no) <> ''
# MAGIC     GROUP BY si.invoice_no, si.item_no, si.shipment_no
# MAGIC ),
# MAGIC 
# MAGIC -- ─── Sale ILE: shipment → lot/serial ─────────────────────────────────────────
# MAGIC sale_ile AS (
# MAGIC     SELECT
# MAGIC         ile.`Document No.`           AS shipment_no,
# MAGIC         ile.`Item No.`               AS item_no,
# MAGIC         ile.`Lot No.`                AS lot_no,
# MAGIC         ile.`Serial No.`             AS serial_no,
# MAGIC         ABS(ile.`Quantity`)          AS sale_qty
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` ile
# MAGIC     WHERE ile.`Entry Type` = 'Sale'
# MAGIC       AND ile.`Quantity` < 0
# MAGIC ),
# MAGIC 
# MAGIC -- ─── Output ILE: lot/serial → PRO ────────────────────────────────────────────
# MAGIC output_ile AS (
# MAGIC     SELECT
# MAGIC         ile.`Order No.`              AS pro_no,
# MAGIC         ile.`Item No.`               AS item_no,
# MAGIC         ile.`Lot No.`                AS lot_no,
# MAGIC         ile.`Serial No.`             AS serial_no,
# MAGIC         ABS(ile.`Quantity`)          AS output_qty
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` ile
# MAGIC     WHERE ile.`Entry Type` = 'Output'
# MAGIC       AND ile.`Quantity` > 0
# MAGIC ),
# MAGIC 
# MAGIC -- ═══════════════════════════════════════════════════════════════════════════
# MAGIC -- PATH 1: Lot-based tracing
# MAGIC -- ═══════════════════════════════════════════════════════════════════════════
# MAGIC lot_trace AS (
# MAGIC     SELECT DISTINCT
# MAGIC         inv.ar_no,
# MAGIC         inv.item_no,
# MAGIC         inv.invoice_qty,
# MAGIC         inv.shipment_no,
# MAGIC         sale.lot_no,
# MAGIC         out.pro_no,
# MAGIC         out.output_qty
# MAGIC     FROM invoice_shipments inv
# MAGIC     INNER JOIN sale_ile sale
# MAGIC         ON  inv.shipment_no = sale.shipment_no
# MAGIC         AND inv.item_no     = sale.item_no
# MAGIC         AND sale.lot_no IS NOT NULL
# MAGIC         AND TRIM(sale.lot_no) <> ''
# MAGIC     INNER JOIN output_ile out
# MAGIC         ON  sale.item_no = out.item_no
# MAGIC         AND sale.lot_no  = out.lot_no
# MAGIC         AND out.lot_no IS NOT NULL
# MAGIC         AND TRIM(out.lot_no) <> ''
# MAGIC ),
# MAGIC 
# MAGIC -- ═══════════════════════════════════════════════════════════════════════════
# MAGIC -- PATH 2: Serial-based tracing (for items with Serial No., no Lot No.)
# MAGIC -- ═══════════════════════════════════════════════════════════════════════════
# MAGIC serial_trace AS (
# MAGIC     SELECT DISTINCT
# MAGIC         inv.ar_no,
# MAGIC         inv.item_no,
# MAGIC         inv.invoice_qty,
# MAGIC         inv.shipment_no,
# MAGIC         sale.serial_no    AS lot_no,    -- store serial in lot_no column for compatibility
# MAGIC         out.pro_no,
# MAGIC         out.output_qty
# MAGIC     FROM invoice_shipments inv
# MAGIC     INNER JOIN sale_ile sale
# MAGIC         ON  inv.shipment_no = sale.shipment_no
# MAGIC         AND inv.item_no     = sale.item_no
# MAGIC         AND sale.serial_no IS NOT NULL
# MAGIC         AND TRIM(sale.serial_no) <> ''
# MAGIC         AND (sale.lot_no IS NULL OR TRIM(sale.lot_no) = '')
# MAGIC     INNER JOIN output_ile out
# MAGIC         ON  sale.item_no    = out.item_no
# MAGIC         AND sale.serial_no  = out.serial_no
# MAGIC         AND out.serial_no IS NOT NULL
# MAGIC         AND TRIM(out.serial_no) <> ''
# MAGIC ),
# MAGIC 
# MAGIC -- ═══════════════════════════════════════════════════════════════════════════
# MAGIC -- UNION + Deduplicate
# MAGIC -- ═══════════════════════════════════════════════════════════════════════════
# MAGIC all_traces AS (
# MAGIC     SELECT *, 'LOT' AS trace_method FROM lot_trace
# MAGIC     UNION ALL
# MAGIC     SELECT *, 'SERIAL' AS trace_method FROM serial_trace
# MAGIC ),
# MAGIC 
# MAGIC deduped AS (
# MAGIC     SELECT
# MAGIC         ar_no, item_no, invoice_qty, shipment_no, lot_no, pro_no, output_qty,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY ar_no, item_no, pro_no
# MAGIC             ORDER BY CASE WHEN trace_method = 'SERIAL' THEN 0 ELSE 1 END
# MAGIC         ) AS rn
# MAGIC     FROM all_traces
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     ar_no, item_no, invoice_qty, shipment_no, lot_no, pro_no, output_qty
# MAGIC FROM deduped
# MAGIC WHERE rn = 1;
# MAGIC 
# MAGIC 
# MAGIC -- ████████████████████████████████████████████████████████████████████████████
# MAGIC -- CELL 2: Verify
# MAGIC -- ████████████████████████████████████████████████████████████████████████████
# MAGIC 
# MAGIC -- Row count
# MAGIC SELECT COUNT(*) AS total_rows,
# MAGIC        COUNT(DISTINCT ar_no) AS distinct_ars,
# MAGIC        COUNT(DISTINCT pro_no) AS distinct_pros
# MAGIC FROM Gold_Finance_Lakehouse.ct.gold_invoice_pro_map;
# MAGIC 
# MAGIC -- Verify DE BEERS: SI2601-0030 should have exactly 15 PROs, 1 row each
# MAGIC SELECT ar_no, item_no, pro_no, output_qty, lot_no
# MAGIC FROM Gold_Finance_Lakehouse.ct.gold_invoice_pro_map
# MAGIC WHERE ar_no = 'SI2601-0030'
# MAGIC   AND item_no = 'N000107469-00003'
# MAGIC ORDER BY pro_no;
# MAGIC 
# MAGIC -- Check no fan-out: should be 15 rows, not 225
# MAGIC SELECT COUNT(*) AS row_count
# MAGIC FROM Gold_Finance_Lakehouse.ct.gold_invoice_pro_map
# MAGIC WHERE ar_no = 'SI2601-0030'
# MAGIC   AND item_no = 'N000107469-00003';
# MAGIC 
# MAGIC -- ████████████████████████████████████████████████████████████████████████████
# MAGIC -- CELL 3: OPTIMIZE + VACUUM to ensure SQL endpoint sees latest files
# MAGIC -- ████████████████████████████████████████████████████████████████████████████
# MAGIC 
# MAGIC OPTIMIZE Gold_Finance_Lakehouse.ct.gold_invoice_pro_map;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # gold cas output by line

# CELL ********************

# MAGIC %%sql
# MAGIC -- Notebook: nb_06e_1_gold_cas_output_by_line
# MAGIC -- Purpose: CAS Output qty per (PRO, Order Line, Item) for line-level casting cost allocation
# MAGIC -- Layer: Silver→Gold
# MAGIC -- Schedule: Daily (after nb_06d, before nb_06e_2)
# MAGIC -- Dependencies: Silver_BC_Lakehouse.bc.Item Ledger Entry
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CELL 1: Configuration
# MAGIC -- ============================================================
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CELL 2: Create Table
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TABLE Gold_Finance_Lakehouse.ct.gold_cas_output_by_line
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false')
# MAGIC AS
# MAGIC SELECT
# MAGIC     ile.`Order No.`                             AS cas_pro_no,
# MAGIC     ile.`Order Line No.`                        AS cas_order_line_no,
# MAGIC     ile.`Item No.`                              AS output_item_no,
# MAGIC     SUM(ile.`Quantity`)                         AS output_qty
# MAGIC FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` ile
# MAGIC WHERE ile.`Entry Type` = 'Output'
# MAGIC   AND ile.`Order No.` LIKE 'CAS%'
# MAGIC   AND ile.`Quantity` > 0
# MAGIC GROUP BY
# MAGIC     ile.`Order No.`,
# MAGIC     ile.`Order Line No.`,
# MAGIC     ile.`Item No.`


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # gold cas consumption by line

# CELL ********************

# MAGIC %%sql
# MAGIC -- Notebook: nb_06e_2_gold_cas_consumption_by_line
# MAGIC -- Purpose: CAS Consumption per (PRO, Order Line, Item) — qty from ILE only, cost from VE
# MAGIC -- Layer: Silver→Gold
# MAGIC -- Schedule: Daily (after nb_06e_1, before nb_06e_3)
# MAGIC -- Dependencies: Silver_BC_Lakehouse.bc.Item Ledger Entry, Silver_BC_Lakehouse.bc.Value Entry
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CELL 1: Configuration
# MAGIC -- ============================================================
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CELL 2: Create Table
# MAGIC -- KEY: Two CTEs prevent VE fan-out on quantity
# MAGIC --   ile_qty  → qty from ILE only (no VE join)
# MAGIC --   ile_cost → cost from ILE+VE join (multiple VE rows per ILE is correct for cost)
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TABLE Gold_Finance_Lakehouse.ct.gold_cas_consumption_by_line
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false')
# MAGIC AS
# MAGIC WITH
# MAGIC ile_qty AS (
# MAGIC     SELECT
# MAGIC         ile.`Order No.`                         AS cas_pro_no,
# MAGIC         ile.`Order Line No.`                    AS cas_order_line_no,
# MAGIC         ile.`Item No.`                          AS consumed_item_no,
# MAGIC         ABS(SUM(ile.`Quantity`))                AS consumed_qty
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` ile
# MAGIC     WHERE ile.`Entry Type` = 'Consumption'
# MAGIC       AND ile.`Order No.` LIKE 'CAS%'
# MAGIC       AND ile.`Quantity` < 0
# MAGIC     GROUP BY
# MAGIC         ile.`Order No.`,
# MAGIC         ile.`Order Line No.`,
# MAGIC         ile.`Item No.`
# MAGIC ),
# MAGIC ile_cost AS (
# MAGIC     SELECT
# MAGIC         ile.`Order No.`                         AS cas_pro_no,
# MAGIC         ile.`Order Line No.`                    AS cas_order_line_no,
# MAGIC         ile.`Item No.`                          AS consumed_item_no,
# MAGIC         ABS(SUM(ve.`Cost Amount (Actual)`))     AS consumed_cost
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` ile
# MAGIC     JOIN Silver_BC_Lakehouse.bc.`Value Entry` ve
# MAGIC         ON ve.`Item Ledger Entry No.` = ile.`Entry No.`
# MAGIC     WHERE ile.`Entry Type` = 'Consumption'
# MAGIC       AND ile.`Order No.` LIKE 'CAS%'
# MAGIC       AND ile.`Quantity` < 0
# MAGIC     GROUP BY
# MAGIC         ile.`Order No.`,
# MAGIC         ile.`Order Line No.`,
# MAGIC         ile.`Item No.`
# MAGIC )
# MAGIC SELECT
# MAGIC     q.cas_pro_no,
# MAGIC     q.cas_order_line_no,
# MAGIC     q.consumed_item_no,
# MAGIC     q.consumed_qty,
# MAGIC     COALESCE(c.consumed_cost, 0)                AS consumed_cost
# MAGIC FROM ile_qty q
# MAGIC LEFT JOIN ile_cost c
# MAGIC     ON  q.cas_pro_no        = c.cas_pro_no
# MAGIC     AND q.cas_order_line_no = c.cas_order_line_no
# MAGIC     AND q.consumed_item_no  = c.consumed_item_no
# MAGIC ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

#  # gold cas line cost summary

# CELL ********************

# MAGIC %%sql
# MAGIC -- Notebook: nb_06e_3_gold_cas_line_cost_summary
# MAGIC -- Purpose: JOIN CAS Output + Consumption at Order Line level for build_report.py
# MAGIC -- Layer: Gold→Gold
# MAGIC -- Schedule: Daily (after nb_06e_1 + nb_06e_2, before build_report.py)
# MAGIC -- Dependencies: ct.gold_cas_output_by_line, ct.gold_cas_consumption_by_line
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CELL 1: Configuration
# MAGIC -- ============================================================
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CELL 2: Create Table
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TABLE Gold_Finance_Lakehouse.ct.gold_cas_line_cost_summary
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false')
# MAGIC AS
# MAGIC SELECT
# MAGIC     o.cas_pro_no,
# MAGIC     o.cas_order_line_no,
# MAGIC     o.output_item_no,
# MAGIC     o.output_qty,
# MAGIC     c.consumed_item_no,
# MAGIC     c.consumed_qty,
# MAGIC     c.consumed_cost,
# MAGIC     CASE
# MAGIC         WHEN o.output_qty > 0
# MAGIC         THEN c.consumed_cost / o.output_qty
# MAGIC         ELSE 0
# MAGIC     END                                         AS cost_per_output_unit,
# MAGIC     CASE
# MAGIC         WHEN o.output_qty > 0
# MAGIC         THEN c.consumed_qty / o.output_qty
# MAGIC         ELSE 0
# MAGIC     END                                         AS qty_per_output_unit
# MAGIC FROM Gold_Finance_Lakehouse.ct.gold_cas_output_by_line o
# MAGIC INNER JOIN Gold_Finance_Lakehouse.ct.gold_cas_consumption_by_line c
# MAGIC     ON  o.cas_pro_no       = c.cas_pro_no
# MAGIC     AND o.cas_order_line_no = c.cas_order_line_no
# MAGIC ;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Notebook: nb_gold_pro_line_scrap_bom
# MAGIC -- Purpose: Per PRO-line scrap BOM allocation — maps casting scrap (M-930-SLV-REG-SCRAP etc.)
# MAGIC --          back to each semi-FG output line with correct BOM qty/cost per line
# MAGIC -- Layer: Silver → Gold (Gold_Finance_Lakehouse)
# MAGIC -- Schedule: Daily (after silver BC mirror sync)
# MAGIC -- Dependencies: Silver_BC_Lakehouse.bc.`Prod Order Component`, Silver_BC_Lakehouse.bc.`Prod Order Line`
# MAGIC 
# MAGIC -- ████████████████████████████████████████████████████████████
# MAGIC -- CELL 0: Config
# MAGIC -- ████████████████████████████████████████████████████████████
# MAGIC 
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC 
# MAGIC 
# MAGIC -- ████████████████████████████████████████████████████████████
# MAGIC -- CELL 1: Build gold_pro_line_scrap_bom
# MAGIC -- ████████████████████████████████████████████████████████████
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Finance_Lakehouse.ct.gold_pro_line_scrap_bom
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC WITH
# MAGIC pro_lines AS (
# MAGIC     SELECT
# MAGIC         pol.`Prod. Order No.`       AS pro_no,
# MAGIC         pol.`Line No.`              AS line_no,
# MAGIC         pol.`Item No.`              AS output_item_no,
# MAGIC         pol.`Description`           AS output_description,
# MAGIC         pol.`Quantity`              AS output_qty,
# MAGIC         pol.`Finished Quantity`     AS finished_qty,
# MAGIC         pol.`Unit of Measure Code`  AS output_uom,
# MAGIC         pol.`Status`                AS status
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Prod Order Line` pol
# MAGIC     WHERE pol.`Status` IN ('Released', 'Finished', '3', '4')
# MAGIC       AND pol.`Quantity` > 0
# MAGIC ),
# MAGIC 
# MAGIC scrap_components AS (
# MAGIC     SELECT
# MAGIC         poc.`Prod. Order No.`           AS pro_no,
# MAGIC         poc.`Prod. Order Line No.`      AS pro_line_no,
# MAGIC         poc.`Line No.`                  AS comp_line_no,
# MAGIC         poc.`Item No.`                  AS scrap_item_no,
# MAGIC         poc.`Description`               AS scrap_description,
# MAGIC         poc.`Expected Quantity`         AS bom_qty,
# MAGIC         poc.`Remaining Quantity`        AS remaining_qty,
# MAGIC         poc.`Quantity per`              AS qty_per,
# MAGIC         poc.`Unit Cost`                 AS bom_unit_cost,
# MAGIC         poc.`Cost Amount`               AS bom_cost_amount,
# MAGIC         poc.`Unit of Measure Code`      AS scrap_uom
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Prod Order Component` poc
# MAGIC     WHERE poc.`Status` IN ('Released', 'Finished', '3', '4')
# MAGIC       AND (
# MAGIC           poc.`Item No.` LIKE 'M-930%SCRAP%'
# MAGIC           OR poc.`Item No.` LIKE 'M-930%PURE%'
# MAGIC           OR poc.`Item No.` LIKE 'M-750%SCRAP%'
# MAGIC           OR poc.`Item No.` LIKE 'M-750%PURE%'
# MAGIC           OR poc.`Item No.` LIKE 'M-585%SCRAP%'
# MAGIC           OR poc.`Item No.` LIKE 'M-585%PURE%'
# MAGIC       )
# MAGIC       AND poc.`Expected Quantity` > 0
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     sc.pro_no,
# MAGIC     sc.pro_line_no,
# MAGIC     pl.output_item_no,
# MAGIC     pl.output_description,
# MAGIC     pl.output_qty,
# MAGIC     pl.output_uom,
# MAGIC     sc.comp_line_no,
# MAGIC     sc.scrap_item_no,
# MAGIC     sc.scrap_description,
# MAGIC     sc.bom_qty,
# MAGIC     sc.remaining_qty,
# MAGIC     sc.qty_per,
# MAGIC     sc.bom_unit_cost,
# MAGIC     sc.bom_cost_amount,
# MAGIC     sc.scrap_uom
# MAGIC FROM scrap_components sc
# MAGIC INNER JOIN pro_lines pl
# MAGIC     ON sc.pro_no = pl.pro_no
# MAGIC     AND sc.pro_line_no = pl.line_no
# MAGIC ORDER BY sc.pro_no, sc.pro_line_no, sc.comp_line_no;
# MAGIC 
# MAGIC 
# MAGIC -- ████████████████████████████████████████████████████████████
# MAGIC -- CELL 2: Quality Checks
# MAGIC -- ████████████████████████████████████████████████████████████
# MAGIC 
# MAGIC SELECT COUNT(*) AS total_rows,
# MAGIC        COUNT(DISTINCT pro_no) AS distinct_pros,
# MAGIC        COUNT(DISTINCT scrap_item_no) AS distinct_scrap_items
# MAGIC FROM Gold_Finance_Lakehouse.ct.gold_pro_line_scrap_bom;
# MAGIC 
# MAGIC -- Verify WRO251103698
# MAGIC SELECT pro_no, pro_line_no, output_item_no, scrap_item_no, bom_qty, bom_unit_cost, bom_cost_amount
# MAGIC FROM Gold_Finance_Lakehouse.ct.gold_pro_line_scrap_bom
# MAGIC WHERE pro_no = 'WRO251103698'
# MAGIC ORDER BY pro_line_no;
# MAGIC -- Expected:
# MAGIC -- Line 20000 (C015571): bom_qty = 11.58
# MAGIC -- Line 30000 (C015572): bom_qty = 5.76
# MAGIC -- Line 40000 (C015573): bom_qty = 12.60
# MAGIC -- Line 50000 (C015574): bom_qty = 5.76

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # gold_pro_line_scrap_actual

# CELL ********************

# MAGIC %%sql
# MAGIC -- Notebook: nb_gold_pro_line_scrap_actual
# MAGIC -- Purpose: Per PRO-line ACTUAL scrap consumption — maps actual qty + cost
# MAGIC --          back to each semi-FG output line using ILE Order Line No.
# MAGIC -- Layer: Silver → Gold (Gold_Finance_Lakehouse)
# MAGIC -- Schedule: Daily (after silver BC mirror sync)
# MAGIC -- Dependencies: Silver_BC_Lakehouse.bc.`Item Ledger Entry`,
# MAGIC --               Silver_BC_Lakehouse.bc.`Value Entry`,
# MAGIC --               Silver_BC_Lakehouse.bc.`Prod Order Line`
# MAGIC 
# MAGIC -- ████████████████████████████████████████████████████████████
# MAGIC -- CELL 0: Config
# MAGIC -- ████████████████████████████████████████████████████████████
# MAGIC 
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC 
# MAGIC 
# MAGIC -- ████████████████████████████████████████████████████████████
# MAGIC -- CELL 1: Build gold_pro_line_scrap_actual
# MAGIC -- ████████████████████████████████████████████████████████████
# MAGIC 
# MAGIC -- Grain: 1 row = 1 PRO + 1 PRO Line + 1 scrap item (actual consumption)
# MAGIC -- Key: ILE [Order Line No.] maps consumption back to Prod Order Line
# MAGIC 
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Finance_Lakehouse.ct.gold_pro_line_scrap_actual
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'false',
# MAGIC     'delta.autoOptimize.autoCompact'   = 'true'
# MAGIC )
# MAGIC AS
# MAGIC 
# MAGIC WITH
# MAGIC -- Actual qty from ILE (Consumption entries only)
# MAGIC -- ILE Quantity is NEGATIVE for consumption → use ABS
# MAGIC ile_consumption AS (
# MAGIC     SELECT
# MAGIC         ile.`Order No.`             AS pro_no,
# MAGIC         ile.`Order Line No.`        AS pro_line_no,
# MAGIC         ile.`Item No.`              AS scrap_item_no,
# MAGIC         ile.`Description`           AS scrap_description,
# MAGIC         ile.`Unit of Measure Code`  AS scrap_uom,
# MAGIC         ABS(SUM(ile.`Quantity`))    AS actual_qty,
# MAGIC         COUNT(*)                    AS ile_count
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` ile
# MAGIC     WHERE ile.`Entry Type` = 'Consumption'
# MAGIC       AND ile.`Order No.` LIKE 'WRO%'
# MAGIC       AND (
# MAGIC           ile.`Item No.` LIKE 'M-930%SCRAP%'
# MAGIC           OR ile.`Item No.` LIKE 'M-930%PURE%'
# MAGIC           OR ile.`Item No.` LIKE 'M-750%SCRAP%'
# MAGIC           OR ile.`Item No.` LIKE 'M-750%PURE%'
# MAGIC           OR ile.`Item No.` LIKE 'M-585%SCRAP%'
# MAGIC           OR ile.`Item No.` LIKE 'M-585%PURE%'
# MAGIC       )
# MAGIC       AND ile.`Order Line No.` IS NOT NULL
# MAGIC       AND ile.`Order Line No.` > 0
# MAGIC     GROUP BY
# MAGIC         ile.`Order No.`,
# MAGIC         ile.`Order Line No.`,
# MAGIC         ile.`Item No.`,
# MAGIC         ile.`Description`,
# MAGIC         ile.`Unit of Measure Code`
# MAGIC ),
# MAGIC 
# MAGIC -- Actual cost from VE (Consumption entries only)
# MAGIC -- Must aggregate separately to avoid fan-out with ILE
# MAGIC ve_consumption AS (
# MAGIC     SELECT
# MAGIC         ve.`Order No.`                      AS pro_no,
# MAGIC         ve.`Order Line No.`                 AS pro_line_no,
# MAGIC         ve.`Item No.`                       AS scrap_item_no,
# MAGIC         ABS(SUM(ve.`Cost Amount (Actual)`)) AS actual_cost,
# MAGIC         COUNT(*)                            AS ve_count
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Value Entry` ve
# MAGIC     WHERE ve.`Item Ledger Entry Type` = 'Consumption'
# MAGIC       AND ve.`Order No.` LIKE 'WRO%'
# MAGIC       AND (
# MAGIC           ve.`Item No.` LIKE 'M-930%SCRAP%'
# MAGIC           OR ve.`Item No.` LIKE 'M-930%PURE%'
# MAGIC           OR ve.`Item No.` LIKE 'M-750%SCRAP%'
# MAGIC           OR ve.`Item No.` LIKE 'M-750%PURE%'
# MAGIC           OR ve.`Item No.` LIKE 'M-585%SCRAP%'
# MAGIC           OR ve.`Item No.` LIKE 'M-585%PURE%'
# MAGIC       )
# MAGIC       AND ve.`Order Line No.` IS NOT NULL
# MAGIC       AND ve.`Order Line No.` > 0
# MAGIC     GROUP BY
# MAGIC         ve.`Order No.`,
# MAGIC         ve.`Order Line No.`,
# MAGIC         ve.`Item No.`
# MAGIC ),
# MAGIC 
# MAGIC -- Map PRO Line No. to output item
# MAGIC pro_lines AS (
# MAGIC     SELECT
# MAGIC         pol.`Prod. Order No.`       AS pro_no,
# MAGIC         pol.`Line No.`              AS line_no,
# MAGIC         pol.`Item No.`              AS output_item_no,
# MAGIC         pol.`Description`           AS output_description
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Prod Order Line` pol
# MAGIC     WHERE pol.`Status` IN ('Released', 'Finished', '3', '4')
# MAGIC       AND pol.`Quantity` > 0
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     i.pro_no,
# MAGIC     i.pro_line_no,
# MAGIC     pl.output_item_no,
# MAGIC     pl.output_description,
# MAGIC     i.scrap_item_no,
# MAGIC     i.scrap_description,
# MAGIC     i.scrap_uom,
# MAGIC     i.actual_qty,
# MAGIC     COALESCE(v.actual_cost, 0)      AS actual_cost,
# MAGIC     CASE WHEN i.actual_qty > 0
# MAGIC          THEN COALESCE(v.actual_cost, 0) / i.actual_qty
# MAGIC          ELSE 0 END                 AS actual_unit_cost,
# MAGIC     i.ile_count,
# MAGIC     COALESCE(v.ve_count, 0)         AS ve_count
# MAGIC FROM ile_consumption i
# MAGIC LEFT JOIN ve_consumption v
# MAGIC     ON v.pro_no = i.pro_no
# MAGIC     AND v.pro_line_no = i.pro_line_no
# MAGIC     AND v.scrap_item_no = i.scrap_item_no
# MAGIC LEFT JOIN pro_lines pl
# MAGIC     ON pl.pro_no = i.pro_no
# MAGIC     AND pl.line_no = i.pro_line_no
# MAGIC ORDER BY i.pro_no, i.pro_line_no;
# MAGIC 
# MAGIC 
# MAGIC -- ████████████████████████████████████████████████████████████
# MAGIC -- CELL 2: OPTIMIZE
# MAGIC -- ████████████████████████████████████████████████████████████
# MAGIC 
# MAGIC REFRESH TABLE Gold_Finance_Lakehouse.ct.gold_pro_line_scrap_actual;
# MAGIC OPTIMIZE Gold_Finance_Lakehouse.ct.gold_pro_line_scrap_actual;
# MAGIC 
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = true;
# MAGIC 
# MAGIC -- ████████████████████████████████████████████████████████████
# MAGIC -- CELL 3: Quality Checks
# MAGIC -- ████████████████████████████████████████████████████████████
# MAGIC  
# MAGIC SELECT COUNT(*) AS total_rows,
# MAGIC        COUNT(DISTINCT pro_no) AS distinct_pros,
# MAGIC        COUNT(DISTINCT scrap_item_no) AS distinct_scrap_items
# MAGIC FROM Gold_Finance_Lakehouse.ct.gold_pro_line_scrap_actual;
# MAGIC  
# MAGIC -- Verify WRO251103698 — C015571 should show actual_qty = 13.15 (11.58 + 1.57)
# MAGIC SELECT pro_no, pro_line_no, output_item_no, scrap_item_no,
# MAGIC        actual_qty, actual_cost, actual_unit_cost
# MAGIC FROM Gold_Finance_Lakehouse.ct.gold_pro_line_scrap_actual
# MAGIC WHERE pro_no = 'WRO251103698'
# MAGIC ORDER BY pro_line_no;
# MAGIC -- Expected:
# MAGIC -- Line 20000 (C015571): actual_qty = 13.15 (11.58 + 1.57)
# MAGIC -- Line 30000 (C015572): actual_qty = ?
# MAGIC -- Line 40000 (C015573): actual_qty = ?
# MAGIC -- Line 50000 (C015574): actual_qty = ?
# MAGIC -- Total should = 38.62 (matching ILE total)
# MAGIC 


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
