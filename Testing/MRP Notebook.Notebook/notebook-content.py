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
# META           "id": "679b23e5-c539-462b-9512-4f04fb1f383c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
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

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Silver_Planning_Lakehouse.gold.gold_stock_control_level AS
# MAGIC 
# MAGIC WITH base AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         item_description,
# MAGIC         item_uom,
# MAGIC         item_uom2,
# MAGIC         location,
# MAGIC         TYPE,
# MAGIC 
# MAGIC         CAST(COALESCE(rem_uom1, 0) AS DECIMAL(38,10)) AS on_hand,
# MAGIC         CAST(COALESCE(rem_uom2, 0) AS DECIMAL(38,10)) AS on_hand_uom2,
# MAGIC 
# MAGIC         CAST(COALESCE(qty_to_receive, 0) AS DECIMAL(38,10)) AS on_order,
# MAGIC 
# MAGIC         CAST(COALESCE(reorder_point, 0) AS DECIMAL(38,10)) AS reorder_point,
# MAGIC         CAST(COALESCE(item_safety_stock_quantity, 0) AS DECIMAL(38,10)) AS safety_stock,
# MAGIC 
# MAGIC         CAST(COALESCE(reorder_quantity, 0) AS DECIMAL(38,10)) AS reorder_qty,
# MAGIC 
# MAGIC         CAST(COALESCE(minimum_order_quantity, 0) AS DECIMAL(38,10)) AS min_order_qty,
# MAGIC         CAST(COALESCE(maximum_order_quantity, 0) AS DECIMAL(38,10)) AS max_order_qty,
# MAGIC         CAST(COALESCE(maximum_inventory, 0) AS DECIMAL(38,10)) AS max_inventory,
# MAGIC         CAST(COALESCE(order_multiple, 0) AS DECIMAL(38,10)) AS order_multiple,
# MAGIC 
# MAGIC         replenishment_system,
# MAGIC         vendor_no,
# MAGIC         vendor_name,
# MAGIC         vendor_leadtime,
# MAGIC         safety_lead_time
# MAGIC 
# MAGIC     FROM Gold_Inventory_Lakehouse.inv.gold_stock_item
# MAGIC     WHERE reordering_policy = 'Fixed Reorder Qty.'
# MAGIC ),
# MAGIC 
# MAGIC calc AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         (on_hand + on_order) AS projected_available,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN safety_stock > reorder_point THEN safety_stock
# MAGIC             ELSE reorder_point
# MAGIC         END AS min_level,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN (on_hand + on_order) <
# MAGIC                  (CASE WHEN safety_stock > reorder_point THEN safety_stock ELSE reorder_point END)
# MAGIC             THEN
# MAGIC                 (CASE WHEN safety_stock > reorder_point THEN safety_stock ELSE reorder_point END)
# MAGIC                 - (on_hand + on_order)
# MAGIC             ELSE CAST(0 AS DECIMAL(38,10))
# MAGIC         END AS shortage,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN (on_hand + on_order) <
# MAGIC                  (CASE WHEN safety_stock > reorder_point THEN safety_stock ELSE reorder_point END)
# MAGIC             THEN 1 ELSE 0
# MAGIC         END AS need_to_order
# MAGIC     FROM base
# MAGIC ),
# MAGIC 
# MAGIC step1 AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CASE WHEN need_to_order = 1 THEN shortage ELSE CAST(0 AS DECIMAL(38,10)) END AS suggested_raw
# MAGIC     FROM calc
# MAGIC ),
# MAGIC 
# MAGIC step2 AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CASE
# MAGIC             WHEN suggested_raw = 0 THEN CAST(0 AS DECIMAL(38,10))
# MAGIC             WHEN min_order_qty > 0 AND suggested_raw < min_order_qty THEN min_order_qty
# MAGIC             ELSE suggested_raw
# MAGIC         END AS suggested_moq
# MAGIC     FROM step1
# MAGIC ),
# MAGIC 
# MAGIC step3 AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CASE
# MAGIC             WHEN suggested_moq = 0 THEN CAST(0 AS DECIMAL(38,10))
# MAGIC             WHEN order_multiple > 0
# MAGIC                 THEN CEIL(suggested_moq / order_multiple) * order_multiple
# MAGIC             ELSE suggested_moq
# MAGIC         END AS suggested_multiple
# MAGIC     FROM step2
# MAGIC ),
# MAGIC 
# MAGIC step4 AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CASE
# MAGIC             WHEN suggested_multiple = 0 THEN CAST(0 AS DECIMAL(38,10))
# MAGIC             WHEN max_inventory > 0
# MAGIC              AND (on_hand + on_order + suggested_multiple) > max_inventory
# MAGIC             THEN
# MAGIC                 CASE
# MAGIC                     WHEN (max_inventory - (on_hand + on_order)) < 0 THEN CAST(0 AS DECIMAL(38,10))
# MAGIC                     ELSE (max_inventory - (on_hand + on_order))
# MAGIC                 END
# MAGIC             ELSE suggested_multiple
# MAGIC         END AS suggested_after_maxinv
# MAGIC     FROM step3
# MAGIC ),
# MAGIC 
# MAGIC final_calc AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CASE
# MAGIC             WHEN suggested_after_maxinv = 0 THEN CAST(0 AS DECIMAL(38,10))
# MAGIC             WHEN max_order_qty > 0 AND suggested_after_maxinv > max_order_qty THEN max_order_qty
# MAGIC             ELSE suggested_after_maxinv
# MAGIC         END AS suggested_order_qty
# MAGIC     FROM step4
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     item_no,
# MAGIC     item_description,
# MAGIC     location,
# MAGIC     vendor_no,
# MAGIC     vendor_name,
# MAGIC 
# MAGIC     on_hand,
# MAGIC     on_order,
# MAGIC     projected_available,
# MAGIC 
# MAGIC     reorder_point,
# MAGIC     safety_stock,
# MAGIC     min_level,
# MAGIC     shortage,
# MAGIC 
# MAGIC     suggested_order_qty AS qty_to_order,
# MAGIC 
# MAGIC     item_uom,
# MAGIC     replenishment_system,
# MAGIC     vendor_leadtime,
# MAGIC     safety_lead_time
# MAGIC FROM final_calc
# MAGIC ;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Silver_Planning_Lakehouse.gold.gold_stock_control_level_plus AS
# MAGIC 
# MAGIC WITH stock AS (
# MAGIC     SELECT
# MAGIC         s.*,
# MAGIC 
# MAGIC         -- audit fields
# MAGIC         s.vendor_leadtime  AS vendor_leadtime_formula,
# MAGIC         s.safety_lead_time AS safety_lead_time_formula,
# MAGIC 
# MAGIC         -- '45D' -> 45
# MAGIC         COALESCE(try_cast(replace(s.vendor_leadtime,  'D', '') AS INT), 0) AS vendor_lead_days,
# MAGIC         COALESCE(try_cast(replace(s.safety_lead_time, 'D', '') AS INT), 0) AS safety_lead_days
# MAGIC     FROM Silver_Planning_Lakehouse.gold.gold_stock_control_level s
# MAGIC ),
# MAGIC 
# MAGIC stock2 AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         (vendor_lead_days + safety_lead_days) AS total_lead_days
# MAGIC     FROM stock
# MAGIC ),
# MAGIC 
# MAGIC usage_cte AS (
# MAGIC     SELECT item_no, location, avg_daily_usage_6m
# MAGIC     FROM Gold_Inventory_Lakehouse.inv.gold_avg_daily_consumption_usage
# MAGIC ),
# MAGIC 
# MAGIC po_snapshot AS (
# MAGIC     SELECT
# MAGIC         g.item_no,
# MAGIC         g.location,
# MAGIC         CAST(g.order_date AS DATE) AS existing_po_order_date,
# MAGIC         CAST(g.requested_receipt_date AS DATE) AS existing_po_requested_receipt_date,
# MAGIC         CAST(g.promised_receipt_date  AS DATE) AS existing_po_promised_receipt_date,
# MAGIC         COALESCE(CAST(g.promised_receipt_date AS DATE), CAST(g.requested_receipt_date AS DATE)) AS existing_po_due_date,
# MAGIC         CAST(COALESCE(g.qty_to_receive, 0) AS DECIMAL(38,10)) AS existing_po_qty_to_receive
# MAGIC     FROM Gold_Inventory_Lakehouse.inv.gold_stock_item g
# MAGIC ),
# MAGIC 
# MAGIC joined AS (
# MAGIC     SELECT
# MAGIC         s2.*,
# MAGIC         u.avg_daily_usage_6m,
# MAGIC 
# MAGIC         p.existing_po_order_date,
# MAGIC         p.existing_po_requested_receipt_date,
# MAGIC         p.existing_po_promised_receipt_date,
# MAGIC         p.existing_po_due_date,
# MAGIC         p.existing_po_qty_to_receive,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN u.avg_daily_usage_6m IS NULL OR u.avg_daily_usage_6m <= 0 THEN NULL
# MAGIC             ELSE (s2.projected_available - s2.min_level) / u.avg_daily_usage_6m
# MAGIC         END AS days_cover_above_min,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN u.avg_daily_usage_6m IS NULL OR u.avg_daily_usage_6m <= 0 THEN NULL
# MAGIC             WHEN s2.projected_available <= s2.min_level THEN current_date()
# MAGIC             ELSE date_add(
# MAGIC                 current_date(),
# MAGIC                 CAST(CEIL((s2.projected_available - s2.min_level) / u.avg_daily_usage_6m) AS INT)
# MAGIC             )
# MAGIC         END AS hit_min_level_date
# MAGIC 
# MAGIC     FROM stock2 s2
# MAGIC     LEFT JOIN usage_cte u
# MAGIC       ON u.item_no = s2.item_no
# MAGIC      AND u.location = s2.location
# MAGIC     LEFT JOIN po_snapshot p
# MAGIC       ON p.item_no = s2.item_no
# MAGIC      AND p.location = s2.location
# MAGIC ),
# MAGIC 
# MAGIC final AS (
# MAGIC     SELECT
# MAGIC         j.*,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN j.existing_po_due_date IS NULL THEN CAST(false AS BOOLEAN)
# MAGIC             WHEN j.on_order <= 0 THEN CAST(false AS BOOLEAN)
# MAGIC             WHEN j.hit_min_level_date IS NULL THEN CAST(false AS BOOLEAN)
# MAGIC             WHEN j.existing_po_due_date <= j.hit_min_level_date THEN CAST(true AS BOOLEAN)
# MAGIC             ELSE CAST(false AS BOOLEAN)
# MAGIC         END AS incoming_in_time,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN j.avg_daily_usage_6m IS NULL OR j.avg_daily_usage_6m <= 0 THEN NULL
# MAGIC             WHEN j.projected_available <= j.min_level THEN current_date()
# MAGIC             ELSE
# MAGIC                 CASE
# MAGIC                     WHEN date_add(j.hit_min_level_date, -j.total_lead_days) < current_date()
# MAGIC                     THEN current_date()
# MAGIC                     ELSE date_add(j.hit_min_level_date, -j.total_lead_days)
# MAGIC                 END
# MAGIC         END AS recommended_order_date,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN j.avg_daily_usage_6m IS NULL OR j.avg_daily_usage_6m <= 0 THEN NULL
# MAGIC             ELSE date_add(
# MAGIC                 CASE
# MAGIC                     WHEN j.projected_available <= j.min_level THEN current_date()
# MAGIC                     ELSE
# MAGIC                         CASE
# MAGIC                             WHEN date_add(j.hit_min_level_date, -j.total_lead_days) < current_date()
# MAGIC                             THEN current_date()
# MAGIC                             ELSE date_add(j.hit_min_level_date, -j.total_lead_days)
# MAGIC                         END
# MAGIC                 END,
# MAGIC                 j.total_lead_days
# MAGIC             )
# MAGIC         END AS new_order_due_date,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN j.avg_daily_usage_6m IS NULL OR j.avg_daily_usage_6m <= 0
# MAGIC                 THEN 'No usage history (12M / CONSUME)'
# MAGIC 
# MAGIC             WHEN j.projected_available <= j.min_level
# MAGIC                  AND j.on_order > 0
# MAGIC                  AND j.existing_po_due_date IS NOT NULL
# MAGIC                  AND j.hit_min_level_date IS NOT NULL
# MAGIC                  AND j.existing_po_due_date <= j.hit_min_level_date
# MAGIC                 THEN 'Below Min, but PO Incoming in time'
# MAGIC 
# MAGIC             WHEN j.projected_available <= j.min_level
# MAGIC                 THEN 'Reorder Now (At/Below Min)'
# MAGIC 
# MAGIC             WHEN j.hit_min_level_date IS NOT NULL
# MAGIC                  AND date_add(j.hit_min_level_date, -j.total_lead_days) <= current_date()
# MAGIC                 THEN 'Reorder Now (Lead time risk)'
# MAGIC 
# MAGIC             ELSE 'Planned'
# MAGIC         END AS order_timing_status
# MAGIC 
# MAGIC     FROM joined j
# MAGIC )
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM final
# MAGIC ;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Silver_Planning_Lakehouse.gold.gold_vendor_lead_time AS
# MAGIC 
# MAGIC WITH po AS (
# MAGIC     SELECT
# MAGIC         pl.`BC Company`,
# MAGIC         ph.`No.`                   AS po_no,
# MAGIC         pl.`Line No.`              AS po_lineno,
# MAGIC         ph.`Buy-from Vendor No.`   AS vendor_no,
# MAGIC         pl.`No.`                   AS item_no,
# MAGIC         ph.`Order Date`            AS order_date,
# MAGIC         pl.`Expected Receipt Date` AS expected_receipt_date
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Purchase Line` pl
# MAGIC     INNER JOIN Silver_BC_Lakehouse.bc.`Purchase Header` ph
# MAGIC         ON ph.`BC Company` = pl.`BC Company`
# MAGIC        AND ph.`No.`       = pl.`Document No.`
# MAGIC     WHERE
# MAGIC         ph.`Document Type` = 'Order'
# MAGIC         AND pl.`Document Type` = 'Order'
# MAGIC         AND (
# MAGIC             try_cast(pl.`Type` AS INT) = 2
# MAGIC             OR lower(trim(CAST(pl.`Type` AS STRING))) = 'item'
# MAGIC         )
# MAGIC         AND ph.`Order Date` IS NOT NULL
# MAGIC ),
# MAGIC 
# MAGIC rcpt AS (
# MAGIC     SELECT
# MAGIC         rl.`BC Company`,
# MAGIC         rl.`Order No.`      AS po_no,
# MAGIC         rl.`Order Line No.` AS po_lineno,
# MAGIC         rh.`Posting Date`   AS receipt_posting_date
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Purch Rcpt Line` rl
# MAGIC     INNER JOIN Silver_BC_Lakehouse.bc.`Purch Rcpt Header` rh
# MAGIC         ON rh.`BC Company` = rl.`BC Company`
# MAGIC        AND rh.`No.`        = rl.`Document No.`
# MAGIC     WHERE
# MAGIC         try_cast(rl.`Type` AS INT) = 2
# MAGIC         OR lower(trim(CAST(rl.`Type` AS STRING))) = 'item'
# MAGIC ),
# MAGIC 
# MAGIC rcpt_agg AS (
# MAGIC     SELECT
# MAGIC         `BC Company`,
# MAGIC         po_no,
# MAGIC         po_lineno,
# MAGIC         MIN(receipt_posting_date) AS first_receipt_date,
# MAGIC         MAX(receipt_posting_date) AS last_receipt_date
# MAGIC     FROM rcpt
# MAGIC     GROUP BY `BC Company`, po_no, po_lineno
# MAGIC ),
# MAGIC 
# MAGIC base AS (
# MAGIC     SELECT
# MAGIC         po.`BC Company`,
# MAGIC         po.vendor_no,
# MAGIC         po.item_no,
# MAGIC         datediff(ra.first_receipt_date, po.order_date) AS leadtime_first,
# MAGIC         datediff(ra.last_receipt_date,  po.order_date) AS leadtime_last
# MAGIC     FROM po
# MAGIC     INNER JOIN rcpt_agg ra
# MAGIC         ON ra.`BC Company` = po.`BC Company`
# MAGIC        AND ra.po_no        = po.po_no
# MAGIC        AND ra.po_lineno    = po.po_lineno
# MAGIC     WHERE
# MAGIC         ra.first_receipt_date IS NOT NULL
# MAGIC         AND datediff(ra.first_receipt_date, po.order_date) >= 0
# MAGIC ),
# MAGIC 
# MAGIC stats AS (
# MAGIC     SELECT
# MAGIC         `BC Company`,
# MAGIC         vendor_no,
# MAGIC         item_no,
# MAGIC         COUNT(*) AS sample_size,
# MAGIC         AVG(CAST(leadtime_first AS DOUBLE))  AS avg_first,
# MAGIC         stddev_samp(CAST(leadtime_first AS DOUBLE)) AS std_first,
# MAGIC         AVG(CAST(leadtime_last AS DOUBLE))   AS avg_last
# MAGIC     FROM base
# MAGIC     GROUP BY `BC Company`, vendor_no, item_no
# MAGIC ),
# MAGIC 
# MAGIC pctl AS (
# MAGIC     SELECT
# MAGIC         `BC Company`,
# MAGIC         vendor_no,
# MAGIC         item_no,
# MAGIC         percentile_approx(leadtime_first, 0.95) AS p95_first
# MAGIC     FROM base
# MAGIC     GROUP BY `BC Company`, vendor_no, item_no
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     s.`BC Company` as bc_company,
# MAGIC     s.vendor_no,
# MAGIC     s.item_no,
# MAGIC     s.sample_size,
# MAGIC 
# MAGIC     CAST(s.avg_first AS DECIMAL(10,2)) AS avg_leadtime_first_days,
# MAGIC     CAST(p.p95_first AS DECIMAL(10,2)) AS p95_first_days,
# MAGIC     CAST(s.std_first AS DECIMAL(10,2)) AS stddev_first_days,
# MAGIC 
# MAGIC     -- Normal Lead Time
# MAGIC     CEIL(p.p95_first) AS normal_leadtime_days,
# MAGIC 
# MAGIC     -- Safety Lead Time (Statistical Buffer)
# MAGIC     CEIL(COALESCE(s.std_first, 0) * 1.65) AS safety_leadtime_days,
# MAGIC 
# MAGIC     -- Total Lead Time
# MAGIC     CEIL(p.p95_first) + CEIL(COALESCE(s.std_first, 0) * 1.65) AS total_leadtime_days,
# MAGIC 
# MAGIC     -- DateFormula String
# MAGIC     concat(CAST(CEIL(p.p95_first) AS STRING), 'D') AS normal_leadtime_formula,
# MAGIC     concat(CAST(CEIL(COALESCE(s.std_first, 0) * 1.65) AS STRING), 'D') AS safety_leadtime_formula,
# MAGIC     concat(CAST((CEIL(p.p95_first) + CEIL(COALESCE(s.std_first, 0) * 1.65)) AS STRING), 'D') AS total_leadtime_formula
# MAGIC 
# MAGIC FROM stats s
# MAGIC LEFT JOIN pctl p
# MAGIC   ON p.`BC Company` = s.`BC Company`
# MAGIC  AND p.vendor_no = s.vendor_no
# MAGIC  AND p.item_no   = s.item_no
# MAGIC ;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Avg Daily Consumption Usage

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.inv.gold_avg_daily_consumption_usage AS
# MAGIC 
# MAGIC 
# MAGIC WITH item_dim AS (
# MAGIC     SELECT
# MAGIC         `No.`                   AS item_no,
# MAGIC         `Base Unit of Measure`  AS usage_uom
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item`
# MAGIC ),
# MAGIC 
# MAGIC cons AS (
# MAGIC     SELECT
# MAGIC         ile.`Item No.`      AS item_no,
# MAGIC         ile.`Location Code` AS location,
# MAGIC 
# MAGIC         -- UOM1 (Quantity) : 12m
# MAGIC         ABS(SUM(CASE
# MAGIC             WHEN ile.`Posting Date` >= add_months(current_date(), -12)
# MAGIC              AND ile.`Posting Date` <= current_date()
# MAGIC              AND (
# MAGIC                     (ile.`Entry Type` = 'Negative Adjmt.' AND ile.`Document No.` IN ('CONSUME', 'CONSUMED'))
# MAGIC                  OR (ile.`Entry Type` = 'Consumption'     AND ile.`Document No.` LIKE 'W%')
# MAGIC                  )
# MAGIC             THEN CAST(ile.`Quantity` AS DECIMAL(38,10))
# MAGIC             ELSE CAST(0 AS DECIMAL(38,10))
# MAGIC         END)) AS total_consumption_qty_12m,
# MAGIC 
# MAGIC         -- UOM1 (Quantity) : 6m
# MAGIC         ABS(SUM(CASE
# MAGIC             WHEN ile.`Posting Date` >= add_months(current_date(), -6)
# MAGIC              AND ile.`Posting Date` <= current_date()
# MAGIC              AND (
# MAGIC                     (ile.`Entry Type` = 'Negative Adjmt.' AND ile.`Document No.` IN ('CONSUME', 'CONSUMED'))
# MAGIC                  OR (ile.`Entry Type` = 'Consumption'     AND ile.`Document No.` LIKE 'W%')
# MAGIC                  )
# MAGIC             THEN CAST(ile.`Quantity` AS DECIMAL(38,10))
# MAGIC             ELSE CAST(0 AS DECIMAL(38,10))
# MAGIC         END)) AS total_consumption_qty_6m,
# MAGIC 
# MAGIC         -- UOM2 (Units_DU_TSL) : 12m
# MAGIC         ABS(SUM(CASE
# MAGIC             WHEN ile.`Posting Date` >= add_months(current_date(), -12)
# MAGIC              AND ile.`Posting Date` <= current_date()
# MAGIC              AND (
# MAGIC                     (ile.`Entry Type` = 'Negative Adjmt.' AND ile.`Document No.` IN ('CONSUME', 'CONSUMED'))
# MAGIC                  OR (ile.`Entry Type` = 'Consumption'     AND ile.`Document No.` LIKE 'W%')
# MAGIC                  )
# MAGIC             THEN CAST(ile.`Units_DU_TSL` AS DECIMAL(38,10))
# MAGIC             ELSE CAST(0 AS DECIMAL(38,10))
# MAGIC         END)) AS total_consumption_uom2_qty_12m,
# MAGIC 
# MAGIC         -- UOM2 (Units_DU_TSL) : 6m
# MAGIC         ABS(SUM(CASE
# MAGIC             WHEN ile.`Posting Date` >= add_months(current_date(), -6)
# MAGIC              AND ile.`Posting Date` <= current_date()
# MAGIC              AND (
# MAGIC                     (ile.`Entry Type` = 'Negative Adjmt.' AND ile.`Document No.` IN ('CONSUME', 'CONSUMED'))
# MAGIC                  OR (ile.`Entry Type` = 'Consumption'     AND ile.`Document No.` LIKE 'W%')
# MAGIC                  )
# MAGIC             THEN CAST(ile.`Units_DU_TSL` AS DECIMAL(38,10))
# MAGIC             ELSE CAST(0 AS DECIMAL(38,10))
# MAGIC         END)) AS total_consumption_uom2_qty_6m,
# MAGIC 
# MAGIC         -- UOM2 label (ถ้ามีหลายค่าในช่วงเวลา จะเลือก MAX)
# MAGIC         MAX(ile.`Unit of Measure - Units_DU_TSL`) AS usage_uom2
# MAGIC 
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` ile
# MAGIC     WHERE
# MAGIC         ile.`Posting Date` >= add_months(current_date(), -12)
# MAGIC         AND ile.`Posting Date` <= current_date()
# MAGIC         AND (
# MAGIC                 (ile.`Entry Type` = 'Negative Adjmt.' AND ile.`Document No.` IN ('CONSUME', 'CONSUMED'))
# MAGIC              OR (ile.`Entry Type` = 'Consumption'     AND ile.`Document No.` LIKE 'W%')
# MAGIC             )
# MAGIC     GROUP BY
# MAGIC         ile.`Item No.`,
# MAGIC         ile.`Location Code`
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     c.item_no,
# MAGIC     c.location,
# MAGIC 
# MAGIC     -- UOM1
# MAGIC     c.total_consumption_qty_12m,
# MAGIC     c.total_consumption_qty_6m,
# MAGIC     CAST(
# MAGIC         c.total_consumption_qty_12m /
# MAGIC         NULLIF(datediff(current_date(), add_months(current_date(), -12)) + 1, 0)
# MAGIC         AS DECIMAL(38,10)
# MAGIC     ) AS avg_daily_usage_12m,
# MAGIC     CAST(
# MAGIC         c.total_consumption_qty_6m /
# MAGIC         NULLIF(datediff(current_date(), add_months(current_date(), -6)) + 1, 0)
# MAGIC         AS DECIMAL(38,10)
# MAGIC     ) AS avg_daily_usage_6m,
# MAGIC     i.usage_uom,
# MAGIC 
# MAGIC     -- UOM2
# MAGIC     c.total_consumption_uom2_qty_12m,
# MAGIC     c.total_consumption_uom2_qty_6m,
# MAGIC     CAST(
# MAGIC         c.total_consumption_uom2_qty_12m /
# MAGIC         NULLIF(datediff(current_date(), add_months(current_date(), -12)) + 1, 0)
# MAGIC         AS DECIMAL(38,10)
# MAGIC     ) AS avg_daily_usage_uom2_12m,
# MAGIC     CAST(
# MAGIC         c.total_consumption_uom2_qty_6m /
# MAGIC         NULLIF(datediff(current_date(), add_months(current_date(), -6)) + 1, 0)
# MAGIC         AS DECIMAL(38,10)
# MAGIC     ) AS avg_daily_usage_uom2_6m,
# MAGIC     c.usage_uom2,
# MAGIC 
# MAGIC     -- display (เลือกโชว์ UOM2 แบบ 6m)
# MAGIC     CONCAT(
# MAGIC         CAST(
# MAGIC             c.total_consumption_uom2_qty_6m /
# MAGIC             NULLIF(datediff(current_date(), add_months(current_date(), -6)) + 1, 0)
# MAGIC             AS DECIMAL(18,4)
# MAGIC         ),
# MAGIC         ' ',
# MAGIC         c.usage_uom2,
# MAGIC         '/day'
# MAGIC     ) AS usage_display_uom2_6m
# MAGIC 
# MAGIC FROM cons c
# MAGIC LEFT JOIN item_dim i
# MAGIC   ON i.item_no = c.item_no
# MAGIC ;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
