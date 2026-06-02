# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6fa25cdd-36f9-4f2e-9817-c1f4d946d4d9",
# META       "default_lakehouse_name": "Gold_Production_Lakehouse",
# META       "default_lakehouse_workspace_id": "d74457b3-045c-445d-82c6-9a2e4b9f1436",
# META       "known_lakehouses": [
# META         {
# META           "id": "6fa25cdd-36f9-4f2e-9817-c1f4d946d4d9"
# META         },
# META         {
# META           "id": "c2142e39-5a5b-4dc7-9454-ea57760d3a60"
# META         },
# META         {
# META           "id": "76781d83-17f8-4270-a81d-6759d1ee9a9d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- =====================================================================================
# MAGIC -- nb_gold_production_status_all  —  CELL 1 of 7  (session config)
# MAGIC -- ONE Gold table for the "Production Status" matrix page.
# MAGIC -- Grain : 1 row per prod.gold_production_casting_status row (= one prod-order line / POL).
# MAGIC -- Value : SUM(prod_line_remaining_quantity).  Columns: Department/Operation step.
# MAGIC --
# MAGIC -- FULLY-QUALIFIED 3-part names (schema-enabled lakehouses).
# MAGIC -- ADD THESE LAKEHOUSES TO THE NOTEBOK FIRST (+ Lakehouse in the left panel):
# MAGIC --   * Gold_Production_Lakehouse   (schema: prod)  -> 4 source tables + the target
# MAGIC --   * Silver_BC_Lakehouse         (schema: bc)    -> `Machine Center` (matrix columns)
# MAGIC --
# MAGIC -- Joins (verified from report relationships):
# MAGIC --   casting_status.SOI           = sales_order_sum.SOI
# MAGIC --   casting_status.POL           = production_order.POL
# MAGIC --   casting_status.prod_order_no = asgn_cell.prod_order_no
# MAGIC --   casting_status.Status        = `Machine Center`.`No.`   (current operation step)
# MAGIC --
# MAGIC -- NOTE on machine center: the matrix COLUMNS use `bc Machine Center` (the BC mirror).
# MAGIC -- The custom fields Operation Group/Sequence + Department Group/sequence are physical
# MAGIC -- columns on that mirrored table (confirmed), so the `mc` CTE resolves directly.
# MAGIC --
# MAGIC -- RULE: each block starting with %%sql is ITS OWN notebook cell.
# MAGIC -- =====================================================================================
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC 
# MAGIC 
# MAGIC %%sql
# MAGIC -- CELL 1b  —  allow writing BC blank/sentinel dates (e.g. 0001-01-01) into Parquet.
# MAGIC -- CORRECTED writes values as-is (correct for an all-Fabric / Proleptic-Gregorian stack).
# MAGIC -- Want clean date slicers instead? NULL the sub-1900 sentinels in CELL 2 (ask me).
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInWrite = CORRECTED;
# MAGIC 
# MAGIC 
# MAGIC %%sql
# MAGIC -- =====================================================================================
# MAGIC -- CELL 2 of 7  —  build Gold_Production_Lakehouse.prod.gold_production_status_all (overwrite)
# MAGIC -- Every lookup is de-duped to its own key BEFORE the join, so output stays at FACT grain
# MAGIC -- and SUM(prod_line_remaining_quantity) reproduces the report exactly (no fan-out).
# MAGIC -- =====================================================================================
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_production_status_all
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH
# MAGIC 
# MAGIC -- FACT — production-order-line status (one row per casting_status row)
# MAGIC -- Scoped to Released orders to match the report (its Power Query filters production_order
# MAGIC -- to prod_order_status = "Released"). Remove the WHERE to keep the full all-status history.
# MAGIC fact AS (
# MAGIC     SELECT
# MAGIC         prod_order_no, prod_order_line_no, POL, SOL, SOI,
# MAGIC         sales_order_no, sales_order_line_no,
# MAGIC         FG_item_no, prod_item_line, item_size, item_routing_no,
# MAGIC         commit_week, prod_order_due_date, prod_line_due_date,
# MAGIC         prod_line_start_date, prod_line_end_date,
# MAGIC         prod_order_quantity, prod_line_quantity,
# MAGIC         prod_line_finished_quantity, prod_line_remaining_quantity,
# MAGIC         prod_order_status, Prod_Status, casting_status, casting_prod_order,
# MAGIC         casting_qty_to_tree, casting_qty_passed, casting_qty_reject, casting_tree_no,
# MAGIC         Status AS current_machine_center,
# MAGIC         _modified_any
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_production_casting_status
# MAGIC     WHERE prod_order_status = 'Released'
# MAGIC ),
# MAGIC 
# MAGIC -- OPERATION / STEP dimension  (matrix COLUMNS + slicers) — from BC-mirror `Machine Center`
# MAGIC mc AS (
# MAGIC     SELECT
# MAGIC         `No.`                 AS machine_center_no,
# MAGIC         `Operation Group`     AS operation_group,
# MAGIC         `Operation Sequence`  AS operation_sequence,
# MAGIC         `Department Group`    AS department_group,
# MAGIC         `Department sequence` AS department_sequence
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Machine Center`
# MAGIC ),
# MAGIC 
# MAGIC -- PROD ORDER header de-duped to 1 row per POL  (FG_item_group, due status, description)
# MAGIC -- NOTE: `LINK PC` was a Power-Query-only column (a SharePoint image URL built from FG_item_no),
# MAGIC -- NOT a physical column. It is rebuilt below as `link_pc` directly in SQL, so the page's image
# MAGIC -- column can bind to it with zero model dependency.
# MAGIC po AS (
# MAGIC     SELECT POL, FG_item_group, due_in, due_status, description
# MAGIC     FROM (
# MAGIC         SELECT POL, FG_item_group, due_in, due_status, description,
# MAGIC                ROW_NUMBER() OVER (PARTITION BY POL ORDER BY _modified_any DESC) AS rn
# MAGIC         FROM Gold_Production_Lakehouse.prod.gold_production_order
# MAGIC     ) WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC -- CELL assignment de-duped to 1 row per prod_order_no  (cell_line, prod_line)
# MAGIC cell AS (
# MAGIC     SELECT prod_order_no, cell_line, prod_line
# MAGIC     FROM (
# MAGIC         SELECT prod_order_no, cell_line, prod_line,
# MAGIC                ROW_NUMBER() OVER (PARTITION BY prod_order_no ORDER BY _modified_any DESC) AS rn
# MAGIC         FROM Gold_Production_Lakehouse.prod.gold_production_asgn_cell
# MAGIC     ) WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC -- SALES ORDER summary de-duped to 1 row per SOI
# MAGIC so AS (
# MAGIC     SELECT SOI, cus_abbr, so_abbr, so_type, status_so, requested_week, total_qty, outstanding_qty
# MAGIC     FROM (
# MAGIC         SELECT SOI, CusAbbr AS cus_abbr, so_abbr, so_type, StatusSO AS status_so,
# MAGIC                requested_week, Total_QTY AS total_qty, OutstandingQty AS outstanding_qty,
# MAGIC                ROW_NUMBER() OVER (PARTITION BY SOI ORDER BY so_abbr, CusAbbr) AS rn
# MAGIC         FROM Gold_Production_Lakehouse.prod.gold_sales_order_sum
# MAGIC     ) WHERE rn = 1
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     -- keys / lineage
# MAGIC     f.prod_order_no, f.prod_order_line_no, f.casting_prod_order,
# MAGIC     f.POL AS pol, f.SOL AS sol, f.SOI AS soi,
# MAGIC     f.sales_order_no, f.sales_order_line_no,
# MAGIC     -- sales order context (rows + slicers + filters)
# MAGIC     s.cus_abbr, s.so_abbr, s.so_type, s.status_so,
# MAGIC     s.requested_week, s.total_qty, s.outstanding_qty,
# MAGIC     -- item / FG context (rows + filters)
# MAGIC     f.FG_item_no AS fg_item_no, p.FG_item_group AS fg_item_group,
# MAGIC     f.prod_item_line, f.item_size, f.item_routing_no,
# MAGIC     -- image URL (rebuilt LINK PC) — bind the page's image column to this
# MAGIC     CONCAT('https://fonzoli.sharepoint.com/sites/PictureLibrary/Shared%20Documents/Medium/',
# MAGIC            f.FG_item_no, '.jpg') AS link_pc,
# MAGIC     c.prod_line, c.cell_line,
# MAGIC     -- operation / step dimension  (MATRIX COLUMNS + slicers)
# MAGIC     f.current_machine_center, m.machine_center_no,
# MAGIC     m.department_group, m.department_sequence,
# MAGIC     m.operation_group, m.operation_sequence,
# MAGIC     -- weeks / dates (rows + "Date" slicer)
# MAGIC     f.commit_week,
# MAGIC     CAST(f.prod_line_due_date  AS DATE) AS prod_line_due_date,
# MAGIC     CAST(f.prod_order_due_date AS DATE) AS prod_order_due_date,
# MAGIC     f.prod_line_start_date, f.prod_line_end_date,
# MAGIC     -- status context
# MAGIC     f.prod_order_status, f.Prod_Status AS prod_status, f.casting_status,
# MAGIC     p.due_in, p.due_status,
# MAGIC     -- quantities (MATRIX VALUE = prod_line_remaining_quantity)
# MAGIC     f.prod_order_quantity, f.prod_line_quantity,
# MAGIC     f.prod_line_finished_quantity, f.prod_line_remaining_quantity,
# MAGIC     f.casting_qty_to_tree, f.casting_qty_passed, f.casting_qty_reject,
# MAGIC     -- metadata
# MAGIC     f._modified_any, current_timestamp() AS _gold_built_at
# MAGIC FROM fact f
# MAGIC LEFT JOIN so   s ON f.SOI                  = s.SOI
# MAGIC LEFT JOIN po   p ON f.POL                  = p.POL
# MAGIC LEFT JOIN cell c ON f.prod_order_no        = c.prod_order_no
# MAGIC LEFT JOIN mc   m ON f.current_machine_center = m.machine_center_no;
# MAGIC 
# MAGIC 
# MAGIC %%sql
# MAGIC -- CELL 3 of 7  —  QC: grain integrity (output rows MUST equal Released source fact rows)
# MAGIC SELECT
# MAGIC     (SELECT COUNT(*) FROM Gold_Production_Lakehouse.prod.gold_production_status_all)         AS gold_rows,
# MAGIC     (SELECT COUNT(*) FROM Gold_Production_Lakehouse.prod.gold_production_casting_status
# MAGIC        WHERE prod_order_status = 'Released')                                            AS fact_rows_released,
# MAGIC     CASE WHEN (SELECT COUNT(*) FROM Gold_Production_Lakehouse.prod.gold_production_status_all)
# MAGIC             = (SELECT COUNT(*) FROM Gold_Production_Lakehouse.prod.gold_production_casting_status
# MAGIC                  WHERE prod_order_status = 'Released')
# MAGIC          THEN 'PASS - no fan-out' ELSE 'FAIL - lookup fan-out, check dedupe' END AS grain_check;
# MAGIC 
# MAGIC 
# MAGIC %%sql
# MAGIC -- CELL 4 of 7  —  QC: null business keys
# MAGIC SELECT
# MAGIC     SUM(CASE WHEN prod_order_no IS NULL THEN 1 ELSE 0 END)      AS null_prod_order_no,
# MAGIC     SUM(CASE WHEN prod_order_line_no IS NULL THEN 1 ELSE 0 END) AS null_prod_order_line_no
# MAGIC FROM Gold_Production_Lakehouse.prod.gold_production_status_all;
# MAGIC 
# MAGIC 
# MAGIC %%sql
# MAGIC -- CELL 5 of 7  —  QC: remaining-qty reconciliation (Gold total MUST match Released source)
# MAGIC SELECT
# MAGIC     (SELECT ROUND(SUM(prod_line_remaining_quantity),3) FROM Gold_Production_Lakehouse.prod.gold_production_status_all)         AS gold_remaining,
# MAGIC     (SELECT ROUND(SUM(prod_line_remaining_quantity),3) FROM Gold_Production_Lakehouse.prod.gold_production_casting_status
# MAGIC        WHERE prod_order_status = 'Released')                                                                             AS fact_remaining_released;
# MAGIC 
# MAGIC 
# MAGIC %%sql
# MAGIC -- CELL 6 of 7  —  QC: unmatched operation step / missing sales context
# MAGIC SELECT
# MAGIC     COUNT(*)                                                  AS total_rows,
# MAGIC     SUM(CASE WHEN machine_center_no IS NULL THEN 1 ELSE 0 END) AS unmatched_operation,
# MAGIC     SUM(CASE WHEN cus_abbr IS NULL THEN 1 ELSE 0 END)         AS missing_sales_context
# MAGIC FROM Gold_Production_Lakehouse.prod.gold_production_status_all;
# MAGIC 
# MAGIC 
# MAGIC %%sql
# MAGIC -- CELL 7 of 7  —  QC: remaining qty by operation step (mirrors the matrix totals)
# MAGIC SELECT
# MAGIC     department_group, operation_group,
# MAGIC     MIN(operation_sequence)                    AS operation_sequence,
# MAGIC     ROUND(SUM(prod_line_remaining_quantity),2) AS remaining_qty,
# MAGIC     COUNT(*)                                   AS line_count
# MAGIC FROM Gold_Production_Lakehouse.prod.gold_production_status_all
# MAGIC GROUP BY department_group, operation_group
# MAGIC ORDER BY operation_sequence;

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
