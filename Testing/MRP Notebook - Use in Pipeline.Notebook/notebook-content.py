# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "679b23e5-c539-462b-9512-4f04fb1f383c",
# META       "default_lakehouse_name": "Silver_Planning_Lakehouse",
# META       "default_lakehouse_workspace_id": "d74457b3-045c-445d-82c6-9a2e4b9f1436",
# META       "known_lakehouses": [
# META         {
# META           "id": "679b23e5-c539-462b-9512-4f04fb1f383c"
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

# CELL ********************

# MAGIC %%sql
# MAGIC -- ============================================================
# MAGIC -- MRP / Fabric Requisition Line (Spark SQL - TEMP VIEWS - ALL IN ONE)
# MAGIC -- Source: mrp_v39.txt  :contentReference[oaicite:0]{index=0}
# MAGIC -- Notes on conversion:
# MAGIC --   - T-SQL [Col Name] -> Spark `Col Name`
# MAGIC --   - GETDATE() -> current_date() / current_timestamp()
# MAGIC --   - DATEADD(DAY, n, d) -> date_add(d, n)
# MAGIC --   - DATEADD(MONTH, n, d) -> add_months(d, n)
# MAGIC --   - CAST(... AS VARCHAR) -> CAST(... AS STRING)
# MAGIC --   - BIT -> BOOLEAN
# MAGIC -- ============================================================
# MAGIC 
# MAGIC USE Silver_Planning_Lakehouse.dbo;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 0: STOCK
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_stock_item_sum AS
# MAGIC WITH base AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         COALESCE(location, '')  AS stock_location_code,
# MAGIC         COALESCE(rem_uom1, 0)   AS rem_uom1,
# MAGIC         COALESCE(rem_uom2, 0)   AS rem_uom2
# MAGIC     FROM gold_stock_item
# MAGIC     WHERE COALESCE(location, '') <> 'BROK-MAT'
# MAGIC ),
# MAGIC sum_per_item AS (
# MAGIC     SELECT item_no, SUM(rem_uom1) AS onhand_uom1, SUM(rem_uom2) AS onhand_uom2
# MAGIC     FROM base
# MAGIC     GROUP BY item_no
# MAGIC ),
# MAGIC best_loc AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         stock_location_code AS best_stock_location_code,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY item_no ORDER BY rem_uom2 DESC, stock_location_code) AS rn
# MAGIC     FROM base
# MAGIC )
# MAGIC SELECT
# MAGIC     s.item_no,
# MAGIC     s.onhand_uom1,
# MAGIC     s.onhand_uom2,
# MAGIC     COALESCE(b.best_stock_location_code,'') AS best_stock_location_code
# MAGIC FROM sum_per_item s
# MAGIC LEFT JOIN (SELECT item_no, best_stock_location_code FROM best_loc WHERE rn = 1) b
# MAGIC     ON s.item_no = b.item_no;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 0B: GOLD CONSUMPTION
# MAGIC -- [BUG FIX] con2 เป็นค่าลบอยู่แล้ว → ใช้ + แทน -
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_consumption_uom2 AS
# MAGIC SELECT
# MAGIC     ComponentItemNo AS item_no,
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN COALESCE(exp2,0) + COALESCE(con2,0) > 0
# MAGIC             THEN COALESCE(exp2,0) + COALESCE(con2,0)
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS demand_quantity_uom2_consumption
# MAGIC FROM gold_consumption
# MAGIC WHERE Status IN ('Planned','Firm Planned','Released')
# MAGIC GROUP BY ComponentItemNo;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 1A: CURRENT INVENTORY
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_onhand_ile AS
# MAGIC SELECT
# MAGIC     `BC Company`              AS bc_company,
# MAGIC     `Item No.`                AS item_no,
# MAGIC     ''                        AS location_code,
# MAGIC     SUM(`Remaining Quantity`) AS onhand_qty,
# MAGIC     MAX(`Posting Date`)       AS last_movement_date
# MAGIC FROM item_ledger_entry
# MAGIC WHERE `Open` = 1
# MAGIC   AND COALESCE(`Location Code`,'') <> 'BROK-MAT'
# MAGIC GROUP BY `BC Company`, `Item No.`;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 1B: SALES DEMAND
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_demand_sales AS
# MAGIC SELECT
# MAGIC     `BC Company` AS bc_company,
# MAGIC     `No.`        AS item_no,
# MAGIC     ''           AS location_code,
# MAGIC     COALESCE(`Variant Code`,'') AS variant_code,
# MAGIC     COALESCE(`Shipment Date`,`Requested Delivery Date`,`Promised Delivery Date`) AS demand_date,
# MAGIC     `Outstanding Quantity`      AS demand_qty,
# MAGIC     `Outstanding Qty. (Base)`   AS demand_qty_base,
# MAGIC     `Document No.`              AS sales_order_no,
# MAGIC     `Line No.`                  AS sales_line_no,
# MAGIC     `Sell-to Customer No.`      AS sell_to_customer_no,
# MAGIC     `Dimension Set ID`          AS dimension_set_id,
# MAGIC     `Shortcut Dimension 1 Code` AS shortcut_dim_1,
# MAGIC     `Shortcut Dimension 2 Code` AS shortcut_dim_2,
# MAGIC     `Unit of Measure Code`      AS unit_of_measure_code,
# MAGIC     `Qty. per Unit of Measure`  AS qty_per_uom,
# MAGIC     'Sales'                     AS demand_type,
# MAGIC     CAST(`Document Type` AS STRING) AS demand_subtype,
# MAGIC     CONCAT(`BC Company`,'-SALES-',`Document No.`,'-',CAST(`Line No.` AS STRING)) AS demand_id,
# MAGIC     CAST(NULL AS STRING)        AS prod_order_no,
# MAGIC     CAST(NULL AS INT)           AS prod_order_line_no,
# MAGIC     CAST(NULL AS STRING)        AS original_item_no,
# MAGIC     0                           AS planning_level
# MAGIC FROM sales_line
# MAGIC WHERE `Type` = 'Item'
# MAGIC   AND `Outstanding Quantity` > 0
# MAGIC   AND COALESCE(`Shipment Date`,`Requested Delivery Date`,`Promised Delivery Date`) IS NOT NULL;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 1C: PRODUCTION COMPONENT DEMAND
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_demand_production AS
# MAGIC SELECT
# MAGIC     `BC Company`                  AS bc_company,
# MAGIC     `Item No.`                    AS item_no,
# MAGIC     ''                            AS location_code,
# MAGIC     COALESCE(`Variant Code`,'')   AS variant_code,
# MAGIC     `Due Date`                    AS demand_date,
# MAGIC     `Remaining Quantity`          AS demand_qty,
# MAGIC     `Remaining Qty. (Base)`       AS demand_qty_base,
# MAGIC     'Production'                  AS demand_type,
# MAGIC     CAST(`Status` AS STRING)      AS demand_subtype,
# MAGIC     CONCAT(
# MAGIC         `BC Company`, '-PROD-',
# MAGIC         `Prod. Order No.`, '-',
# MAGIC         CAST(`Prod. Order Line No.` AS STRING), '-',
# MAGIC         `Item No.`, '-',
# MAGIC         CAST(`Line No.` AS STRING)
# MAGIC     ) AS demand_id,
# MAGIC     `Prod. Order No.`             AS prod_order_no,
# MAGIC     `Prod. Order Line No.`        AS prod_order_line_no,
# MAGIC     CAST(NULL AS STRING)          AS original_item_no,
# MAGIC     1                             AS planning_level,
# MAGIC     CAST(NULL AS INT)             AS dimension_set_id,
# MAGIC     CAST(NULL AS STRING)          AS shortcut_dim_1,
# MAGIC     CAST(NULL AS STRING)          AS shortcut_dim_2,
# MAGIC     CAST(NULL AS STRING)          AS unit_of_measure_code,
# MAGIC     CAST(NULL AS DECIMAL(38,20))  AS qty_per_uom
# MAGIC FROM prod_order_component
# MAGIC WHERE `Status` IN ('Planned','Firm Planned','Released')
# MAGIC   AND COALESCE(`Remaining Quantity`,0) > 0
# MAGIC   AND `Due Date` IS NOT NULL;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 1D: COMBINE DEMAND
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_demand_all AS
# MAGIC SELECT
# MAGIC     bc_company,item_no,location_code,variant_code,demand_date,demand_id,
# MAGIC     demand_qty,demand_qty_base,demand_type,demand_subtype,
# MAGIC     sales_order_no,sales_line_no,sell_to_customer_no,
# MAGIC     prod_order_no,prod_order_line_no,original_item_no,planning_level,
# MAGIC     dimension_set_id,shortcut_dim_1,shortcut_dim_2,unit_of_measure_code,qty_per_uom
# MAGIC FROM v_demand_sales
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     bc_company,item_no,location_code,variant_code,demand_date,demand_id,
# MAGIC     demand_qty,demand_qty_base,demand_type,demand_subtype,
# MAGIC     CAST(NULL AS STRING) AS sales_order_no,
# MAGIC     CAST(NULL AS INT)    AS sales_line_no,
# MAGIC     CAST(NULL AS STRING) AS sell_to_customer_no,
# MAGIC     prod_order_no,prod_order_line_no,original_item_no,planning_level,
# MAGIC     dimension_set_id,shortcut_dim_1,shortcut_dim_2,unit_of_measure_code,qty_per_uom
# MAGIC FROM v_demand_production;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 2: PURCHASE INCOMING
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_po_incoming_item AS
# MAGIC SELECT
# MAGIC     pl.`BC Company` AS bc_company,
# MAGIC     pl.`No.`        AS item_no,
# MAGIC     MIN(COALESCE(pl.`Expected Receipt Date`,pl.`Promised Receipt Date`,pl.`Requested Receipt Date`)) AS earliest_po_receipt_date,
# MAGIC     SUM(COALESCE(pl.`Outstanding Quantity`,0) * COALESCE(pl.`Qty. per Unit of Measure`,1)) AS purchase_order_coming_uom1
# MAGIC FROM purchase_line pl
# MAGIC WHERE pl.`Type` = 'Item' AND COALESCE(pl.`Outstanding Quantity`,0) > 0
# MAGIC GROUP BY pl.`BC Company`, pl.`No.`;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 2B: SUPPLY EVENTS
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_supply_po_events AS
# MAGIC SELECT
# MAGIC     pl.`BC Company` AS bc_company,
# MAGIC     pl.`No.`        AS item_no,
# MAGIC     ''              AS location_code,
# MAGIC     COALESCE(pl.`Variant Code`,'') AS variant_code,
# MAGIC     COALESCE(pl.`Expected Receipt Date`,pl.`Promised Receipt Date`,pl.`Requested Receipt Date`) AS supply_date,
# MAGIC     SUM(COALESCE(pl.`Outstanding Quantity`,0) * COALESCE(pl.`Qty. per Unit of Measure`,1)) AS supply_qty_base
# MAGIC FROM purchase_line pl
# MAGIC WHERE pl.`Type` = 'Item'
# MAGIC   AND COALESCE(pl.`Outstanding Quantity`,0) > 0
# MAGIC   AND COALESCE(pl.`Expected Receipt Date`,pl.`Promised Receipt Date`,pl.`Requested Receipt Date`) IS NOT NULL
# MAGIC GROUP BY
# MAGIC     pl.`BC Company`,
# MAGIC     pl.`No.`,
# MAGIC     COALESCE(pl.`Variant Code`,''),
# MAGIC     COALESCE(pl.`Expected Receipt Date`,pl.`Promised Receipt Date`,pl.`Requested Receipt Date`);
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW v_supply_production_events AS
# MAGIC SELECT
# MAGIC     `BC Company`    AS bc_company,
# MAGIC     `Item No.`      AS item_no,
# MAGIC     ''              AS location_code,
# MAGIC     COALESCE(`Variant Code`,'') AS variant_code,
# MAGIC     `Due Date`      AS supply_date,
# MAGIC     SUM(`Remaining Qty. (Base)`) AS supply_qty_base
# MAGIC FROM prod_order_line
# MAGIC WHERE `Remaining Quantity` > 0 AND `Status` IN ('Planned','Firm Planned','Released')
# MAGIC GROUP BY `BC Company`,`Item No.`,COALESCE(`Variant Code`,''),`Due Date`;
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW v_supply_all_events AS
# MAGIC SELECT bc_company,item_no,location_code,variant_code,supply_date,supply_qty_base FROM v_supply_po_events
# MAGIC UNION ALL
# MAGIC SELECT bc_company,item_no,location_code,variant_code,supply_date,supply_qty_base FROM v_supply_production_events;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 3: ITEM PLANNING PARAMETERS
# MAGIC -- [BUG FIX] replenishment_system: COALESCE default = 'Purchase'
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_item_planning_full AS
# MAGIC SELECT
# MAGIC     i.`BC Company` AS bc_company,
# MAGIC     i.`No.`        AS item_no,
# MAGIC 
# MAGIC     COALESCE(CAST(i.`Reordering Policy` AS STRING),'') AS reordering_policy,
# MAGIC     COALESCE(i.`Reorder Point`,0)           AS reorder_point,
# MAGIC     COALESCE(i.`Reorder Quantity`,0)        AS reorder_quantity,
# MAGIC     COALESCE(i.`Maximum Inventory`,0)       AS maximum_inventory,
# MAGIC     COALESCE(i.`Lot Size`,0)                AS lot_size,
# MAGIC     COALESCE(i.`Minimum Order Quantity`,0)  AS min_order_qty,
# MAGIC     COALESCE(i.`Maximum Order Quantity`,0)  AS max_order_qty,
# MAGIC     COALESCE(i.`Order Multiple`,0)          AS order_multiple,
# MAGIC     COALESCE(i.`Safety Stock Quantity`,0)   AS safety_stock,
# MAGIC     i.`Lead Time Calculation`               AS lead_time_calc,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN right(trim(i.`Lead Time Calculation`),1) = 'D'
# MAGIC             THEN CAST(regexp_replace(trim(i.`Lead Time Calculation`), '[DWMY]', '') AS INT)
# MAGIC         WHEN right(trim(i.`Lead Time Calculation`),1) = 'W'
# MAGIC             THEN CAST(regexp_replace(trim(i.`Lead Time Calculation`), '[DWMY]', '') AS INT) * 7
# MAGIC         WHEN right(trim(i.`Lead Time Calculation`),1) = 'M'
# MAGIC             THEN CAST(regexp_replace(trim(i.`Lead Time Calculation`), '[DWMY]', '') AS INT) * 30
# MAGIC         WHEN right(trim(i.`Lead Time Calculation`),1) = 'Y'
# MAGIC             THEN CAST(regexp_replace(trim(i.`Lead Time Calculation`), '[DWMY]', '') AS INT) * 365
# MAGIC         ELSE 0
# MAGIC     END AS lead_time_days,
# MAGIC 
# MAGIC     COALESCE(CAST(i.`Replenishment System` AS STRING), 'Purchase') AS replenishment_system,
# MAGIC     i.`Vendor No.`                          AS default_vendor_no,
# MAGIC     CAST(i.`Description` AS STRING)         AS item_description,
# MAGIC     CAST(i.`Description 2` AS STRING)       AS description_2,
# MAGIC     CAST(i.`Base Unit of Measure` AS STRING) AS base_uom,
# MAGIC     COALESCE(i.`Unit Cost`,0)               AS unit_cost,
# MAGIC     COALESCE(i.`Indirect Cost %`,0)         AS indirect_cost_pct,
# MAGIC     COALESCE(i.`Overhead Rate`,0)           AS overhead_rate,
# MAGIC     COALESCE(i.`Scrap %`,0)                 AS scrap_pct,
# MAGIC     CAST(i.`Production BOM No.` AS STRING)  AS production_bom_no,
# MAGIC     CAST(i.`Routing No.` AS STRING)         AS routing_no,
# MAGIC     CAST(i.`Low-Level Code` AS STRING)      AS low_level_code,
# MAGIC     CAST(i.`Item Category Code` AS STRING)  AS item_category_code,
# MAGIC     CAST(i.`Gen. Prod. Posting Group` AS STRING) AS gen_prod_posting_group,
# MAGIC 
# MAGIC     COALESCE(u_pc.`Code`,u_cm.`Code`) AS secondary_uom,
# MAGIC     COALESCE(u_pc.`Qty. per Unit of Measure`,u_cm.`Qty. per Unit of Measure`,0) AS secondary_qty_per_base
# MAGIC FROM item i
# MAGIC LEFT JOIN item_unit_of_measure u_pc
# MAGIC     ON i.`BC Company` = u_pc.`BC Company`
# MAGIC    AND i.`No.`       = u_pc.`Item No.`
# MAGIC    AND u_pc.`Code` IN ('PC','PCS')
# MAGIC LEFT JOIN item_unit_of_measure u_cm
# MAGIC     ON i.`BC Company` = u_cm.`BC Company`
# MAGIC    AND i.`No.`       = u_cm.`Item No.`
# MAGIC    AND u_cm.`Code` = 'CM';
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 4A: TIMELINE EVENTS
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_timeline_events AS
# MAGIC SELECT
# MAGIC     bc_company,item_no,location_code,variant_code,
# MAGIC     demand_date AS event_date,
# MAGIC     demand_id   AS event_id,
# MAGIC     'DEMAND'    AS event_type,
# MAGIC     -demand_qty_base AS qty_impact
# MAGIC FROM v_demand_all
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     bc_company,item_no,location_code,variant_code,
# MAGIC     supply_date AS event_date,
# MAGIC     CONCAT('SUPPLY-',CAST(supply_date AS STRING),'-',item_no,'-',location_code,'-',variant_code) AS event_id,
# MAGIC     'SUPPLY' AS event_type,
# MAGIC     supply_qty_base AS qty_impact
# MAGIC FROM v_supply_all_events;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 4B: RUNNING AVAILABILITY (only demand rows)
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_running_availability AS
# MAGIC SELECT
# MAGIC     t.*,
# MAGIC     COALESCE(oh.onhand_qty,0) AS onhand_qty,
# MAGIC     COALESCE(oh.onhand_qty,0)
# MAGIC     + COALESCE(
# MAGIC         SUM(CASE WHEN t.event_type='SUPPLY' THEN t.qty_impact ELSE 0 END)
# MAGIC         OVER (
# MAGIC             PARTITION BY t.bc_company,t.item_no,t.location_code,t.variant_code
# MAGIC             ORDER BY t.event_date,t.event_id
# MAGIC             ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
# MAGIC         ), 0
# MAGIC       )
# MAGIC     - COALESCE(
# MAGIC         SUM(CASE WHEN t.event_type='DEMAND' THEN -t.qty_impact ELSE 0 END)
# MAGIC         OVER (
# MAGIC             PARTITION BY t.bc_company,t.item_no,t.location_code,t.variant_code
# MAGIC             ORDER BY t.event_date,t.event_id
# MAGIC             ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
# MAGIC         ), 0
# MAGIC       )
# MAGIC     AS available_before_this_event
# MAGIC FROM v_timeline_events t
# MAGIC LEFT JOIN v_onhand_ile oh
# MAGIC     ON t.bc_company    = oh.bc_company
# MAGIC    AND t.item_no       = oh.item_no
# MAGIC    AND t.location_code = oh.location_code
# MAGIC WHERE t.event_type = 'DEMAND';
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 4C: DEMAND + PLANNING PARAMS
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_demand_with_availability AS
# MAGIC SELECT
# MAGIC     d.*,
# MAGIC     ra.onhand_qty,
# MAGIC     ra.available_before_this_event AS available_before_this_demand,
# MAGIC     ip.safety_stock, ip.reorder_point, ip.reorder_quantity,
# MAGIC     ip.maximum_inventory, ip.lot_size, ip.min_order_qty, ip.max_order_qty,
# MAGIC     ip.order_multiple, ip.reordering_policy, ip.replenishment_system,
# MAGIC     ip.default_vendor_no, ip.item_description, ip.description_2,
# MAGIC     ip.unit_cost, ip.production_bom_no, ip.routing_no, ip.lead_time_days,
# MAGIC     ip.item_category_code, ip.gen_prod_posting_group, ip.low_level_code,
# MAGIC     ip.indirect_cost_pct, ip.overhead_rate, ip.scrap_pct,
# MAGIC     ip.base_uom, ip.secondary_uom, ip.secondary_qty_per_base
# MAGIC FROM v_demand_all d
# MAGIC INNER JOIN v_running_availability ra
# MAGIC     ON d.bc_company=ra.bc_company
# MAGIC    AND d.item_no=ra.item_no
# MAGIC    AND d.location_code=ra.location_code
# MAGIC    AND d.variant_code=ra.variant_code
# MAGIC    AND d.demand_id=ra.event_id
# MAGIC LEFT JOIN v_item_planning_full ip
# MAGIC     ON d.bc_company=ip.bc_company AND d.item_no=ip.item_no;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 5: SHORTAGE
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_shortage_per_demand AS
# MAGIC SELECT
# MAGIC     *,
# MAGIC     CASE
# MAGIC         WHEN (demand_qty_base + safety_stock) - available_before_this_demand > 0
# MAGIC         THEN (demand_qty_base + safety_stock) - available_before_this_demand
# MAGIC         ELSE 0
# MAGIC     END AS shortage_qty,
# MAGIC     available_before_this_demand - demand_qty_base AS available_after_this_demand
# MAGIC FROM v_demand_with_availability
# MAGIC WHERE (available_before_this_demand - demand_qty_base) < safety_stock;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 6: REORDERING POLICY
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_order_proposals_policy AS
# MAGIC SELECT
# MAGIC     spd.*,
# MAGIC     CASE
# MAGIC         WHEN COALESCE(spd.reordering_policy,'') IN ('','Order','Lot-for-Lot')
# MAGIC             THEN spd.shortage_qty
# MAGIC         WHEN spd.reordering_policy = 'Maximum Qty'
# MAGIC             THEN CASE
# MAGIC                 WHEN spd.maximum_inventory - spd.available_after_this_demand > 0
# MAGIC                 THEN spd.maximum_inventory - spd.available_after_this_demand
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         WHEN spd.reordering_policy IN ('Fixed Reorder Qty','Fixed Reorder Qty.')
# MAGIC             THEN CASE
# MAGIC                 WHEN spd.available_after_this_demand < spd.reorder_point
# MAGIC                 THEN CASE
# MAGIC                     WHEN spd.reorder_quantity > spd.shortage_qty THEN spd.reorder_quantity
# MAGIC                     ELSE spd.shortage_qty
# MAGIC                 END
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ELSE spd.shortage_qty
# MAGIC     END AS policy_qty
# MAGIC FROM v_shortage_per_demand spd
# MAGIC WHERE spd.shortage_qty > 0 OR spd.available_after_this_demand < spd.reorder_point;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 7: MIN / MAX / MULTIPLE
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_order_proposals_constrained AS
# MAGIC WITH base AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CASE WHEN policy_qty > min_order_qty THEN policy_qty ELSE min_order_qty END AS qty_after_min
# MAGIC     FROM v_order_proposals_policy
# MAGIC     WHERE policy_qty > 0
# MAGIC ),
# MAGIC with_max AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CASE
# MAGIC             WHEN max_order_qty > 0 THEN CASE WHEN qty_after_min < max_order_qty THEN qty_after_min ELSE max_order_qty END
# MAGIC             ELSE qty_after_min
# MAGIC         END AS qty_after_max
# MAGIC     FROM base
# MAGIC )
# MAGIC SELECT
# MAGIC     *,
# MAGIC     CASE
# MAGIC         WHEN order_multiple > 0 THEN CEIL(qty_after_max / NULLIF(order_multiple,0)) * order_multiple
# MAGIC         ELSE qty_after_max
# MAGIC     END AS final_qty
# MAGIC FROM with_max;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 8: DATE CALCULATIONS
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_order_proposals_dated AS
# MAGIC SELECT
# MAGIC     op.*,
# MAGIC     date_add(op.demand_date, -op.lead_time_days) AS due_date,
# MAGIC     op.demand_date                               AS required_date,
# MAGIC     current_date()                               AS order_date,
# MAGIC     CASE WHEN op.replenishment_system IN ('Prod. Order','Assembly')
# MAGIC          THEN date_add(op.demand_date, -op.lead_time_days) END AS starting_date,
# MAGIC     CASE WHEN op.replenishment_system IN ('Prod. Order','Assembly')
# MAGIC          THEN op.demand_date END AS ending_date
# MAGIC FROM v_order_proposals_constrained op
# MAGIC WHERE op.final_qty > 0;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- FINAL VIEW: v_fabric_requisition_line_prod
# MAGIC -- [BUG FIX] demand_quantity_uom2 fan-out → MAX() OVER PARTITION
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_fabric_requisition_line_prod AS
# MAGIC SELECT
# MAGIC     f.item_no                                                       AS item_no,
# MAGIC     f.item_no                                                       AS no,
# MAGIC     f.item_description                                              AS description,
# MAGIC     f.description_2                                                 AS description_2,
# MAGIC     f.item_category_code                                            AS item_category_code,
# MAGIC     f.demand_type                                                   AS demand_type,
# MAGIC     f.demand_subtype                                                AS demand_subtype,
# MAGIC     f.demand_date                                                   AS demand_date,
# MAGIC     f.sales_order_no                                                AS sales_order_no,
# MAGIC     f.sales_line_no                                                 AS sales_order_line_no,
# MAGIC     f.prod_order_no                                                 AS prod_order_no,
# MAGIC     f.prod_order_line_no                                            AS prod_order_line_no,
# MAGIC     f.base_uom                                                      AS base_uom_uom1,
# MAGIC     f.secondary_uom                                                 AS secondary_uom_uom2,
# MAGIC     f.secondary_qty_per_base                                        AS uom2_per_uom1,
# MAGIC     f.qty_per_uom                                                   AS qty_per_unit_of_measure,
# MAGIC     f.base_uom                                                      AS unit_of_measure_code,
# MAGIC     f.demand_qty                                                    AS demand_quantity_uom1,
# MAGIC     f.demand_qty_base                                               AS demand_quantity_base_uom1,
# MAGIC 
# MAGIC     MAX(COALESCE(cu.demand_quantity_uom2_consumption, 0))
# MAGIC         OVER (PARTITION BY f.bc_company, f.item_no)                 AS demand_quantity_uom2,
# MAGIC 
# MAGIC     f.shortage_qty                                                  AS shortage_quantity_uom1,
# MAGIC     f.shortage_qty                                                  AS shortage_quantity_base_uom1,
# MAGIC     COALESCE(st.onhand_uom1, 0)                                     AS onhand_quantity_uom1,
# MAGIC     COALESCE(st.onhand_uom2, 0)                                     AS onhand_quantity_uom2,
# MAGIC     COALESCE(po.purchase_order_coming_uom1, 0)                      AS po_incoming_quantity_uom1,
# MAGIC 
# MAGIC     CASE WHEN f.secondary_qty_per_base > 0
# MAGIC          THEN COALESCE(po.purchase_order_coming_uom1,0) / f.secondary_qty_per_base
# MAGIC          ELSE NULL END                                              AS po_incoming_quantity_uom2,
# MAGIC 
# MAGIC     po.earliest_po_receipt_date                                     AS earliest_po_receipt_date,
# MAGIC 
# MAGIC     MAX(
# MAGIC         CASE WHEN COALESCE(po.purchase_order_coming_uom1,0) + COALESCE(st.onhand_uom1,0) < f.final_qty
# MAGIC              THEN f.final_qty - COALESCE(po.purchase_order_coming_uom1,0) - COALESCE(st.onhand_uom1,0)
# MAGIC              ELSE 0 END
# MAGIC     ) OVER (PARTITION BY f.bc_company, f.item_no)                   AS need_to_buy_quantity_uom1,
# MAGIC 
# MAGIC     CASE WHEN f.secondary_qty_per_base > 0
# MAGIC          THEN CEIL(
# MAGIC                 CASE WHEN COALESCE(po.purchase_order_coming_uom1,0) + COALESCE(st.onhand_uom1,0) < f.final_qty
# MAGIC                      THEN f.final_qty - COALESCE(po.purchase_order_coming_uom1,0) - COALESCE(st.onhand_uom1,0)
# MAGIC                      ELSE 0 END / f.secondary_qty_per_base
# MAGIC               )
# MAGIC          ELSE NULL END                                              AS need_to_buy_quantity_uom2_from_uom1,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN COALESCE(cu.demand_quantity_uom2_consumption,0) > 0 THEN
# MAGIC             CASE
# MAGIC                 WHEN cu.demand_quantity_uom2_consumption
# MAGIC                      - COALESCE(st.onhand_uom2,0)
# MAGIC                      - CASE WHEN f.secondary_qty_per_base > 0
# MAGIC                             THEN COALESCE(po.purchase_order_coming_uom1,0) / f.secondary_qty_per_base
# MAGIC                             ELSE 0 END > 0
# MAGIC                 THEN cu.demand_quantity_uom2_consumption
# MAGIC                      - COALESCE(st.onhand_uom2,0)
# MAGIC                      - CASE WHEN f.secondary_qty_per_base > 0
# MAGIC                             THEN COALESCE(po.purchase_order_coming_uom1,0) / f.secondary_qty_per_base
# MAGIC                             ELSE 0 END
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ELSE
# MAGIC             CASE WHEN f.secondary_qty_per_base > 0
# MAGIC                  THEN CEIL(
# MAGIC                         CASE WHEN COALESCE(po.purchase_order_coming_uom1,0) + COALESCE(st.onhand_uom1,0) < f.final_qty
# MAGIC                              THEN f.final_qty - COALESCE(po.purchase_order_coming_uom1,0) - COALESCE(st.onhand_uom1,0)
# MAGIC                              ELSE 0 END / f.secondary_qty_per_base
# MAGIC                       )
# MAGIC                  ELSE NULL END
# MAGIC     END                                                             AS need_to_buy_quantity_uom2_from_consumption,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN COALESCE(f.location_code,'') = '' THEN 0
# MAGIC         WHEN ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY f.bc_company, f.item_no
# MAGIC                 ORDER BY f.final_qty DESC, f.demand_date ASC, f.demand_id ASC
# MAGIC              ) = 1 THEN 1
# MAGIC         ELSE 0
# MAGIC     END                                                             AS is_max_need_to_buy,
# MAGIC 
# MAGIC     f.location_code                                                 AS location_code,
# MAGIC     COALESCE(st.best_stock_location_code,'')                        AS best_stock_location_code,
# MAGIC     f.variant_code                                                  AS variant_code,
# MAGIC     f.available_before_this_demand                                  AS available_before_demand_uom1,
# MAGIC     f.safety_stock                                                  AS safety_stock_uom1,
# MAGIC     f.reorder_point                                                 AS reorder_point_uom1,
# MAGIC     f.reorder_quantity                                              AS reorder_quantity_uom1,
# MAGIC     f.reordering_policy                                             AS reordering_policy,
# MAGIC     f.policy_qty                                                    AS policy_quantity_uom1,
# MAGIC     f.qty_after_min                                                 AS qty_after_min_uom1,
# MAGIC     f.qty_after_max                                                 AS qty_after_max_uom1,
# MAGIC     f.final_qty                                                     AS final_quantity_uom1,
# MAGIC     f.default_vendor_no                                             AS vendor_no,
# MAGIC     f.unit_cost                                                     AS unit_cost,
# MAGIC     f.final_qty * f.unit_cost                                       AS cost_amount,
# MAGIC     f.indirect_cost_pct                                             AS indirect_cost_pct,
# MAGIC     f.overhead_rate                                                 AS overhead_rate,
# MAGIC     f.bc_company                                                    AS bc_company,
# MAGIC     CAST('PLANNING' AS STRING)                                      AS worksheet_template_name,
# MAGIC     CAST('DEFAULT' AS STRING)                                       AS journal_batch_name,
# MAGIC 
# MAGIC     (ROW_NUMBER() OVER (
# MAGIC         PARTITION BY f.bc_company
# MAGIC         ORDER BY f.bc_company, f.item_no, f.location_code, f.due_date, f.demand_id
# MAGIC     ) * 10000)                                                      AS line_no,
# MAGIC 
# MAGIC     CAST('Item' AS STRING)                                          AS type,
# MAGIC     f.final_qty                                                     AS quantity,
# MAGIC     f.final_qty                                                     AS worksheet_quantity_uom1,
# MAGIC     f.unit_cost                                                     AS direct_unit_cost,
# MAGIC     f.due_date                                                      AS due_date,
# MAGIC     f.order_date                                                    AS order_date,
# MAGIC     f.shortcut_dim_1                                                AS shortcut_dimension_1_code,
# MAGIC     f.shortcut_dim_2                                                AS shortcut_dimension_2_code,
# MAGIC     f.dimension_set_id                                              AS dimension_set_id,
# MAGIC     f.sell_to_customer_no                                           AS sell_to_customer_no,
# MAGIC     f.unit_of_measure_code                                          AS unit_of_measure_code_demand,
# MAGIC     f.qty_per_uom                                                   AS qty_per_uom_demand,
# MAGIC     f.final_qty                                                     AS quantity_base,
# MAGIC     COALESCE(f.sales_order_no, f.prod_order_no)                     AS demand_order_no,
# MAGIC     COALESCE(f.sales_line_no, f.prod_order_line_no)                 AS demand_line_no,
# MAGIC     f.demand_subtype                                                AS status,
# MAGIC     f.planning_level                                                AS level,
# MAGIC     f.planning_level                                                AS planning_level,
# MAGIC     f.routing_no                                                    AS routing_no,
# MAGIC     f.gen_prod_posting_group                                        AS gen_prod_posting_group,
# MAGIC     f.low_level_code                                                AS low_level_code,
# MAGIC     f.final_qty                                                     AS remaining_quantity,
# MAGIC     f.final_qty                                                     AS remaining_qty_base,
# MAGIC     f.scrap_pct                                                     AS scrap_pct,
# MAGIC     f.starting_date                                                 AS starting_date,
# MAGIC     f.ending_date                                                   AS ending_date,
# MAGIC     f.production_bom_no                                             AS production_bom_no,
# MAGIC     f.replenishment_system                                          AS replenishment_system,
# MAGIC     f.original_item_no                                              AS original_item_no,
# MAGIC 
# MAGIC     CAST(NULL AS STRING)                                            AS requester_id,
# MAGIC     CAST(NULL AS BOOLEAN)                                           AS confirmed,
# MAGIC     CAST(NULL AS STRING)                                            AS recurring_method,
# MAGIC     CAST(NULL AS DATE)                                              AS expiration_date,
# MAGIC     CAST(NULL AS STRING)                                            AS recurring_frequency,
# MAGIC     CAST(NULL AS STRING)                                            AS vendor_item_no,
# MAGIC     CAST(NULL AS STRING)                                            AS ship_to_code,
# MAGIC     CAST(NULL AS STRING)                                            AS order_address_code,
# MAGIC     CAST(NULL AS STRING)                                            AS currency_code,
# MAGIC     CAST(NULL AS DECIMAL(38,20))                                    AS currency_factor,
# MAGIC     CAST(NULL AS STRING)                                            AS purchaser_code,
# MAGIC     CAST(NULL AS BOOLEAN)                                           AS drop_shipment,
# MAGIC     CAST(NULL AS STRING)                                            AS bin_code,
# MAGIC     CAST(NULL AS DECIMAL(38,20))                                    AS qty_rounding_precision,
# MAGIC     CAST(NULL AS DECIMAL(38,20))                                    AS qty_rounding_precision_base,
# MAGIC     CAST(NULL AS STRING)                                            AS demand_ref_no,
# MAGIC 
# MAGIC     MAX(
# MAGIC         CASE WHEN COALESCE(po.purchase_order_coming_uom1,0) + COALESCE(st.onhand_uom1,0) < f.final_qty
# MAGIC              THEN f.final_qty - COALESCE(po.purchase_order_coming_uom1,0) - COALESCE(st.onhand_uom1,0)
# MAGIC              ELSE 0 END
# MAGIC     ) OVER (PARTITION BY f.bc_company, f.item_no)                   AS needed_quantity,
# MAGIC 
# MAGIC     MAX(
# MAGIC         CASE WHEN COALESCE(po.purchase_order_coming_uom1,0) + COALESCE(st.onhand_uom1,0) < f.final_qty
# MAGIC              THEN f.final_qty - COALESCE(po.purchase_order_coming_uom1,0) - COALESCE(st.onhand_uom1,0)
# MAGIC              ELSE 0 END
# MAGIC     ) OVER (PARTITION BY f.bc_company, f.item_no)                   AS needed_quantity_base,
# MAGIC 
# MAGIC     CAST(NULL AS STRING)                                            AS reserve,
# MAGIC     CAST(NULL AS STRING)                                            AS supply_from,
# MAGIC     CAST(NULL AS STRING)                                            AS original_variant_code,
# MAGIC     f.available_before_this_demand                                  AS demand_qty_available,
# MAGIC     CAST(NULL AS STRING)                                            AS user_id,
# MAGIC     CAST(NULL AS BOOLEAN)                                           AS nonstock,
# MAGIC     CAST(NULL AS STRING)                                            AS purchasing_code,
# MAGIC     CAST(NULL AS STRING)                                            AS transfer_from_code,
# MAGIC     CAST(NULL AS DATE)                                              AS transfer_shipment_date,
# MAGIC     CAST(NULL AS STRING)                                            AS price_calculation_method,
# MAGIC     CAST(NULL AS DECIMAL(38,20))                                    AS line_discount_pct,
# MAGIC     CAST(NULL AS INT)                                               AS custom_sorting_order,
# MAGIC     CAST(NULL AS STRING)                                            AS operation_no,
# MAGIC     CAST(NULL AS STRING)                                            AS work_center_no,
# MAGIC     CAST(NULL AS BOOLEAN)                                           AS mps_order,
# MAGIC     CAST(NULL AS STRING)                                            AS planning_flexibility,
# MAGIC     CAST(NULL AS STRING)                                            AS routing_reference_no,
# MAGIC     CAST(NULL AS STRING)                                            AS gen_business_posting_group,
# MAGIC     CAST(NULL AS STRING)                                            AS production_bom_version_code,
# MAGIC     CAST(NULL AS STRING)                                            AS routing_version_code,
# MAGIC     CAST(NULL AS STRING)                                            AS routing_type,
# MAGIC     CAST(NULL AS DECIMAL(38,20))                                    AS original_quantity,
# MAGIC     CAST(NULL AS DECIMAL(38,20))                                    AS finished_quantity,
# MAGIC     CAST(NULL AS DATE)                                              AS original_due_date,
# MAGIC     CAST(NULL AS TIMESTAMP)                                         AS starting_date_time,
# MAGIC     CAST(NULL AS STRING)                                            AS starting_time,
# MAGIC     CAST(NULL AS TIMESTAMP)                                         AS ending_date_time,
# MAGIC     CAST(NULL AS STRING)                                            AS ending_time,
# MAGIC     CAST(NULL AS STRING)                                            AS ref_order_no,
# MAGIC     CAST(NULL AS STRING)                                            AS ref_order_type,
# MAGIC     CAST(NULL AS STRING)                                            AS ref_order_status,
# MAGIC     CAST(NULL AS INT)                                               AS ref_line_no,
# MAGIC     CAST(NULL AS STRING)                                            AS no_series,
# MAGIC     CAST(NULL AS DECIMAL(38,20))                                    AS finished_qty_base,
# MAGIC     CAST(NULL AS BOOLEAN)                                           AS related_to_planning_line,
# MAGIC     CAST(NULL AS STRING)                                            AS planning_line_origin,
# MAGIC     CAST(NULL AS STRING)                                            AS action_message,
# MAGIC     CAST(NULL AS BOOLEAN)                                           AS accept_action_message,
# MAGIC     CAST(NULL AS DECIMAL(38,20))                                    AS net_quantity_base,
# MAGIC     CAST(NULL AS STRING)                                            AS order_promising_id,
# MAGIC     CAST(NULL AS INT)                                               AS order_promising_line_no,
# MAGIC     CAST(NULL AS STRING)                                            AS order_promising_line_id,
# MAGIC 
# MAGIC     CEIL(
# MAGIC         CASE WHEN COALESCE(po.purchase_order_coming_uom1,0) + COALESCE(st.onhand_uom1,0) < f.final_qty
# MAGIC              THEN f.final_qty - COALESCE(po.purchase_order_coming_uom1,0) - COALESCE(st.onhand_uom1,0)
# MAGIC              ELSE 0 END
# MAGIC     )                                                               AS final_uom1,
# MAGIC 
# MAGIC     CEIL(
# MAGIC         COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN COALESCE(cu.demand_quantity_uom2_consumption,0) > 0 THEN
# MAGIC                     CASE
# MAGIC                         WHEN cu.demand_quantity_uom2_consumption
# MAGIC                              - COALESCE(st.onhand_uom2,0)
# MAGIC                              - CASE WHEN f.secondary_qty_per_base > 0
# MAGIC                                     THEN COALESCE(po.purchase_order_coming_uom1,0) / f.secondary_qty_per_base
# MAGIC                                     ELSE 0 END > 0
# MAGIC                         THEN cu.demand_quantity_uom2_consumption
# MAGIC                              - COALESCE(st.onhand_uom2,0)
# MAGIC                              - CASE WHEN f.secondary_qty_per_base > 0
# MAGIC                                     THEN COALESCE(po.purchase_order_coming_uom1,0) / f.secondary_qty_per_base
# MAGIC                                     ELSE 0 END
# MAGIC                         ELSE 0
# MAGIC                     END
# MAGIC                 ELSE
# MAGIC                     CASE WHEN f.secondary_qty_per_base > 0
# MAGIC                          THEN (CASE WHEN COALESCE(po.purchase_order_coming_uom1,0) + COALESCE(st.onhand_uom1,0) < f.final_qty
# MAGIC                                     THEN f.final_qty - COALESCE(po.purchase_order_coming_uom1,0) - COALESCE(st.onhand_uom1,0)
# MAGIC                                     ELSE 0 END) / f.secondary_qty_per_base
# MAGIC                          ELSE NULL END
# MAGIC             END
# MAGIC         , 0)
# MAGIC     )                                                               AS final_uom2
# MAGIC 
# MAGIC FROM v_order_proposals_dated f
# MAGIC LEFT JOIN v_stock_item_sum   st ON f.item_no = st.item_no
# MAGIC LEFT JOIN v_po_incoming_item po ON f.bc_company = po.bc_company AND f.item_no = po.item_no
# MAGIC LEFT JOIN v_consumption_uom2 cu ON f.item_no = cu.item_no;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- SUMMARY VIEW: v_fabric_requisition_line_summary
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TABLE fabric_requisition_line_summary AS
# MAGIC WITH ile_avg AS (
# MAGIC     SELECT
# MAGIC         `Item No.` AS item_no,
# MAGIC         SUM(ABS(`Quantity`)) / 180.0 AS avg_daily_usage
# MAGIC     FROM `Silver_BC_Lakehouse`.`bc`.`Item Ledger Entry`
# MAGIC     WHERE `Entry Type` = 'Consumption'
# MAGIC       AND `Posting Date` >= add_months(current_date(), -6)
# MAGIC     GROUP BY `Item No.`
# MAGIC ),
# MAGIC ranked AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY bc_company, item_no
# MAGIC             ORDER BY shortage_quantity_uom1 DESC, demand_date ASC
# MAGIC         ) AS rn
# MAGIC     FROM v_fabric_requisition_line_prod
# MAGIC ),
# MAGIC agg AS (
# MAGIC     SELECT
# MAGIC         bc_company, item_no,
# MAGIC         SUM(demand_quantity_uom1)      AS total_demand_quantity_uom1,
# MAGIC         SUM(demand_quantity_base_uom1) AS total_demand_quantity_base_uom1,
# MAGIC         SUM(shortage_quantity_uom1)    AS total_shortage_quantity_uom1,
# MAGIC         MIN(demand_date)               AS earliest_demand_date,
# MAGIC         MIN(due_date)                  AS earliest_due_date
# MAGIC     FROM v_fabric_requisition_line_prod
# MAGIC     GROUP BY bc_company, item_no
# MAGIC ),
# MAGIC agg_all AS (
# MAGIC     SELECT
# MAGIC         bc_company, item_no,
# MAGIC         SUM(demand_qty)      AS total_demand_quantity_uom1_all,
# MAGIC         SUM(demand_qty_base) AS total_demand_quantity_base_uom1_all,
# MAGIC         COUNT(*)             AS total_demand_line_count
# MAGIC     FROM v_demand_all
# MAGIC     GROUP BY bc_company, item_no
# MAGIC )
# MAGIC SELECT
# MAGIC     -- ✅ System columns (added)
# MAGIC     uuid()                                       AS `$systemId`,
# MAGIC     current_timestamp()                          AS SystemCreatedAt,
# MAGIC     current_timestamp()                          AS SystemModifiedAt,
# MAGIC     (unix_timestamp(current_timestamp()) * 1000) AS SystemRowVersion,
# MAGIC 
# MAGIC     -- original columns
# MAGIC     r.item_no, r.no, r.description, r.description_2, r.item_category_code,
# MAGIC     r.reordering_policy, r.base_uom_uom1, r.secondary_uom_uom2, r.uom2_per_uom1,
# MAGIC     r.onhand_quantity_uom1, r.onhand_quantity_uom2,
# MAGIC     r.po_incoming_quantity_uom1, r.po_incoming_quantity_uom2, r.earliest_po_receipt_date,
# MAGIC     r.safety_stock_uom1, r.reorder_point_uom1, r.demand_type, r.demand_subtype,
# MAGIC 
# MAGIC     a.total_demand_quantity_uom1        AS demand_quantity_uom1,
# MAGIC     a.total_demand_quantity_base_uom1   AS demand_quantity_base_uom1,
# MAGIC     COALESCE(cu.demand_quantity_uom2_consumption, 0) AS demand_quantity_uom2,
# MAGIC 
# MAGIC     a.total_shortage_quantity_uom1      AS shortage_quantity_uom1,
# MAGIC     a.total_shortage_quantity_uom1      AS shortage_quantity_base_uom1,
# MAGIC 
# MAGIC     r.available_before_demand_uom1,
# MAGIC     r.need_to_buy_quantity_uom1, r.need_to_buy_quantity_uom2_from_uom1,
# MAGIC     r.need_to_buy_quantity_uom2_from_consumption,
# MAGIC     r.final_uom1, r.final_uom2,
# MAGIC 
# MAGIC     a.earliest_demand_date, a.earliest_due_date,
# MAGIC 
# MAGIC     r.sales_order_no, r.sales_order_line_no, r.prod_order_no, r.prod_order_line_no,
# MAGIC     r.vendor_no, r.unit_cost, r.cost_amount, r.indirect_cost_pct, r.overhead_rate,
# MAGIC     r.bc_company, r.worksheet_template_name, r.journal_batch_name, r.line_no, r.type,
# MAGIC     r.quantity, r.worksheet_quantity_uom1, r.direct_unit_cost, r.due_date, r.order_date,
# MAGIC     r.shortcut_dimension_1_code, r.shortcut_dimension_2_code, r.dimension_set_id,
# MAGIC     r.sell_to_customer_no, r.unit_of_measure_code_demand, r.qty_per_uom_demand,
# MAGIC     r.quantity_base, r.demand_order_no, r.demand_line_no, r.status, r.level, r.planning_level,
# MAGIC     r.variant_code, r.qty_per_unit_of_measure, r.unit_of_measure_code,
# MAGIC     r.routing_no, r.gen_prod_posting_group, r.low_level_code,
# MAGIC     r.remaining_quantity, r.remaining_qty_base, r.scrap_pct,
# MAGIC     r.starting_date, r.ending_date, r.production_bom_no, r.replenishment_system,
# MAGIC     r.original_item_no, r.requester_id, r.confirmed, r.currency_code, r.purchaser_code,
# MAGIC     r.action_message, r.accept_action_message, r.mps_order, r.planning_flexibility,
# MAGIC     r.ref_order_no, r.ref_order_type, r.ref_order_status,
# MAGIC     r.needed_quantity, r.needed_quantity_base, r.demand_qty_available,
# MAGIC     r.location_code, r.best_stock_location_code,
# MAGIC     r.policy_quantity_uom1, r.qty_after_min_uom1, r.qty_after_max_uom1, r.final_quantity_uom1,
# MAGIC     r.is_max_need_to_buy,
# MAGIC 
# MAGIC     r.reorder_point_uom1  AS reorder_point,
# MAGIC     r.safety_stock_uom1   AS safety_stock,
# MAGIC     r.reorder_quantity_uom1,
# MAGIC 
# MAGIC     -- FUTURE PROJECTION (60 days)
# MAGIC     ROUND(COALESCE(il.avg_daily_usage, 0), 2)          AS avg_daily_usage,
# MAGIC     ROUND(COALESCE(il.avg_daily_usage, 0) * 60, 0)     AS forecast_demand_60d,
# MAGIC     ROUND(
# MAGIC         COALESCE(r.onhand_quantity_uom1, 0)
# MAGIC       + COALESCE(r.po_incoming_quantity_uom1, 0)
# MAGIC       - COALESCE(aa.total_demand_quantity_uom1_all, 0)
# MAGIC       - (COALESCE(il.avg_daily_usage, 0) * 60)
# MAGIC     , 0)                                               AS projected_available_60d,
# MAGIC 
# MAGIC     -- ALERT FLAG
# MAGIC     CASE
# MAGIC         WHEN a.earliest_due_date <= date_add(current_date(), 14) AND r.final_uom1 > 0
# MAGIC             THEN 'URGENT - ต้องสั่งภายใน 14 วัน'
# MAGIC         WHEN r.final_uom1 > 0
# MAGIC             THEN 'ควรสั่งซื้อ'
# MAGIC         WHEN COALESCE(r.onhand_quantity_uom1, 0)
# MAGIC            + COALESCE(r.po_incoming_quantity_uom1, 0)
# MAGIC            - COALESCE(aa.total_demand_quantity_uom1_all, 0)
# MAGIC            - (COALESCE(il.avg_daily_usage, 0) * 60) < 0
# MAGIC             THEN 'เฝ้าระวัง - คาดว่าจะขาดใน 60 วัน'
# MAGIC         ELSE 'OK'
# MAGIC     END                                                AS alert_flag,
# MAGIC 
# MAGIC     -- projected stockout date
# MAGIC     CASE
# MAGIC         WHEN COALESCE(il.avg_daily_usage, 0) > 0
# MAGIC         THEN date_add(
# MAGIC                 current_date(),
# MAGIC                 CAST(
# MAGIC                     (COALESCE(r.onhand_quantity_uom1, 0) + COALESCE(r.po_incoming_quantity_uom1, 0))
# MAGIC                     / COALESCE(il.avg_daily_usage, 1)
# MAGIC                 AS INT)
# MAGIC              )
# MAGIC         ELSE NULL
# MAGIC     END                                                AS projected_stockout_date,
# MAGIC 
# MAGIC     aa.total_demand_quantity_uom1_all,
# MAGIC     aa.total_demand_quantity_base_uom1_all,
# MAGIC     aa.total_demand_line_count
# MAGIC 
# MAGIC FROM ranked r
# MAGIC JOIN agg a
# MAGIC   ON r.bc_company = a.bc_company AND r.item_no = a.item_no
# MAGIC LEFT JOIN agg_all aa
# MAGIC   ON r.bc_company = aa.bc_company AND r.item_no = aa.item_no
# MAGIC LEFT JOIN v_consumption_uom2 cu
# MAGIC   ON r.item_no = cu.item_no
# MAGIC LEFT JOIN ile_avg il
# MAGIC   ON r.item_no = il.item_no
# MAGIC WHERE r.rn = 1;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # Line

# CELL ********************

# MAGIC %%sql
# MAGIC -- ============================================================
# MAGIC -- MRP / Fabric Requisition Line (BC-ALIGNED FULL CODE) - V3 PATCHED
# MAGIC --
# MAGIC -- PATCHES in this version:
# MAGIC --   V2 patches (already applied):
# MAGIC --     1. v_po_incoming_item:  UOM1 = SUM(Outstanding Quantity) + UOM2 from ledger
# MAGIC --     2. v_supply_po_events:  ไม่คูณ Qty. per UOM แล้ว
# MAGIC --     3. v_onhand_ile:        filter 31 warehouses
# MAGIC --     4. enrich CTE:          purchase_order_coming_uom2 ใช้ค่าตรงจาก po
# MAGIC --
# MAGIC --   V3 NEW PATCH (UOM-aware rounding):
# MAGIC --     5. final_uom1:          CASE WHEN ตาม base_uom
# MAGIC --                             - Integer (PCS, PRS, SET)       → CEIL
# MAGIC --                             - CTS                           → ROUND 2 decimals
# MAGIC --                             - GR                            → ROUND 3 decimals
# MAGIC --                             - CM, CM2                       → ROUND 2 decimals
# MAGIC --                             - Fallback                      → ROUND 2 decimals
# MAGIC --     6. alert_flag:          ใช้ need_to_buy_qty_uom1 > 0 (ไม่ใช่ CEIL) 
# MAGIC --                             เพื่อไม่ให้ alert false positive กับ fractional UOM
# MAGIC -- ============================================================
# MAGIC 
# MAGIC USE Silver_Planning_Lakehouse.dbo;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 0: STOCK (gold_stock_item) - SUM per item (all locations)
# MAGIC --         + best_stock_location_code (highest rem_uom2)
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_stock_item_sum AS
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     item_no,
# MAGIC     COALESCE(location,'')       AS stock_location_code,
# MAGIC     COALESCE(rem_uom1, 0)       AS rem_uom1,
# MAGIC     COALESCE(rem_uom2, 0)       AS rem_uom2
# MAGIC   FROM gold_stock_item
# MAGIC ),
# MAGIC sum_per_item AS (
# MAGIC   SELECT
# MAGIC     item_no,
# MAGIC     SUM(rem_uom1) AS onhand_uom1,
# MAGIC     SUM(rem_uom2) AS onhand_uom2
# MAGIC   FROM base
# MAGIC   GROUP BY item_no
# MAGIC ),
# MAGIC best_loc AS (
# MAGIC   SELECT
# MAGIC     item_no,
# MAGIC     stock_location_code AS best_stock_location_code,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY item_no
# MAGIC       ORDER BY rem_uom2 DESC, stock_location_code
# MAGIC     ) AS rn
# MAGIC   FROM base
# MAGIC )
# MAGIC SELECT
# MAGIC   s.item_no,
# MAGIC   s.onhand_uom1,
# MAGIC   s.onhand_uom2,
# MAGIC   COALESCE(b.best_stock_location_code,'') AS best_stock_location_code
# MAGIC FROM sum_per_item s
# MAGIC LEFT JOIN (SELECT item_no, best_stock_location_code FROM best_loc WHERE rn = 1) b
# MAGIC   ON s.item_no = b.item_no;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 0B: GOLD CONSUMPTION (UOM2 demand)
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_consumption_uom2 AS
# MAGIC SELECT
# MAGIC     ComponentItemNo AS item_no,
# MAGIC     SUM(
# MAGIC       GREATEST(
# MAGIC         COALESCE(exp2,0) + COALESCE(con2,0),
# MAGIC         0
# MAGIC       )
# MAGIC     ) AS demand_quantity_uom2_consumption
# MAGIC FROM gold_consumption
# MAGIC WHERE Status IN ('Planned','Firm Planned','Released')
# MAGIC GROUP BY ComponentItemNo;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 1A: CURRENT INVENTORY (Item Ledger) - timeline availability
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_onhand_ile AS
# MAGIC SELECT
# MAGIC     `BC Company`                 AS bc_company,
# MAGIC     `Item No.`                   AS item_no,
# MAGIC     COALESCE(`Location Code`,'') AS location_code,
# MAGIC     SUM(`Remaining Quantity`)    AS onhand_qty,
# MAGIC     MAX(`Posting Date`)          AS last_movement_date
# MAGIC FROM item_ledger_entry
# MAGIC WHERE `Open` = 1
# MAGIC   AND `Location Code` IN (
# MAGIC         'BAGGING','CASTING','CONSUME','CST_CUT','CST_ROOM','CZ-SYNT',
# MAGIC         'DEBEERS','DIA-LAB','DIA-NAT','EQUIP','FG-NO-PO','FINDINGS',
# MAGIC         'FIN-GOODS','GEMS','KIMAI','MATERIAL','OBSOLETE','OTHERS-MAT',
# MAGIC         'PACKAGING','PEARLS','PLATING','POMELATO','PRE ALLOY','RETURNS',
# MAGIC         'RUB MOLD','SEMI-F','SORTING','STONE-CUT','STR','TOOLS','WAX ROOM'
# MAGIC   )
# MAGIC GROUP BY `BC Company`, `Item No.`, COALESCE(`Location Code`,'');
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 1B: SALES DEMAND (line level)
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_demand_sales AS
# MAGIC SELECT
# MAGIC     `BC Company` AS bc_company,
# MAGIC     `No.`        AS item_no,
# MAGIC     COALESCE(`Location Code`, '') AS location_code,
# MAGIC     COALESCE(`Variant Code`, '')  AS variant_code,
# MAGIC     CAST(COALESCE(`Shipment Date`,`Requested Delivery Date`,`Promised Delivery Date`) AS DATE) AS demand_date,
# MAGIC 
# MAGIC     `Outstanding Quantity`      AS demand_qty,
# MAGIC     `Outstanding Qty. (Base)`   AS demand_qty_base,
# MAGIC 
# MAGIC     `Document No.`              AS sales_order_no,
# MAGIC     `Line No.`                  AS sales_line_no,
# MAGIC     `Sell-to Customer No.`      AS sell_to_customer_no,
# MAGIC 
# MAGIC     `Dimension Set ID`          AS dimension_set_id,
# MAGIC     `Shortcut Dimension 1 Code` AS shortcut_dim_1,
# MAGIC     `Shortcut Dimension 2 Code` AS shortcut_dim_2,
# MAGIC     `Unit of Measure Code`      AS unit_of_measure_code,
# MAGIC     `Qty. per Unit of Measure`  AS qty_per_uom,
# MAGIC 
# MAGIC     'Sales'                         AS demand_type,
# MAGIC     CAST(`Document Type` AS STRING) AS demand_subtype,
# MAGIC 
# MAGIC     CONCAT(`BC Company`, '-SALES-', `Document No.`, '-', CAST(`Line No.` AS STRING)) AS demand_id,
# MAGIC 
# MAGIC     CAST(NULL AS STRING) AS prod_order_no,
# MAGIC     CAST(NULL AS INT)    AS prod_order_line_no,
# MAGIC     CAST(NULL AS STRING) AS original_item_no,
# MAGIC     0                    AS planning_level
# MAGIC FROM sales_line
# MAGIC WHERE `Type` = 'Item'
# MAGIC   AND `Outstanding Quantity` > 0
# MAGIC   AND COALESCE(`Shipment Date`, `Requested Delivery Date`, `Promised Delivery Date`) IS NOT NULL;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 1C: PRODUCTION COMPONENT DEMAND
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_demand_production AS
# MAGIC SELECT
# MAGIC     `BC Company` AS bc_company,
# MAGIC     `Item No.`   AS item_no,
# MAGIC     COALESCE(`Location Code`, '') AS location_code,
# MAGIC     COALESCE(`Variant Code`, '')  AS variant_code,
# MAGIC 
# MAGIC     CAST(MIN(`Due Date`) AS DATE) AS demand_date,
# MAGIC 
# MAGIC     SUM(`Remaining Quantity`)    AS demand_qty,
# MAGIC     SUM(`Remaining Qty. (Base)`) AS demand_qty_base,
# MAGIC 
# MAGIC     'Production' AS demand_type,
# MAGIC     'Released'   AS demand_subtype,
# MAGIC 
# MAGIC     CONCAT(
# MAGIC       `BC Company`, '-PROD-',
# MAGIC       `Item No.`, '-',
# MAGIC       COALESCE(`Location Code`, ''), '-',
# MAGIC       COALESCE(`Variant Code`, ''), '-',
# MAGIC       CAST(MIN(`Due Date`) AS STRING)
# MAGIC     ) AS demand_id,
# MAGIC 
# MAGIC     CAST(NULL AS STRING) AS prod_order_no,
# MAGIC     CAST(NULL AS INT)    AS prod_order_line_no,
# MAGIC     CAST(NULL AS STRING) AS original_item_no,
# MAGIC     1                    AS planning_level,
# MAGIC 
# MAGIC     CAST(NULL AS INT)            AS dimension_set_id,
# MAGIC     CAST(NULL AS STRING)         AS shortcut_dim_1,
# MAGIC     CAST(NULL AS STRING)         AS shortcut_dim_2,
# MAGIC     CAST(NULL AS STRING)         AS unit_of_measure_code,
# MAGIC     CAST(NULL AS DECIMAL(38,20)) AS qty_per_uom
# MAGIC FROM prod_order_component
# MAGIC WHERE `Status` IN ('Planned','Firm Planned','Released')
# MAGIC   AND COALESCE(`Remaining Quantity`,0) > 0
# MAGIC GROUP BY
# MAGIC     `BC Company`,
# MAGIC     `Item No.`,
# MAGIC     COALESCE(`Location Code`, ''),
# MAGIC     COALESCE(`Variant Code`, '');
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 1D: COMBINE DEMAND
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_demand_all AS
# MAGIC SELECT
# MAGIC     bc_company, item_no, location_code, variant_code,
# MAGIC     demand_date, demand_id,
# MAGIC     demand_qty, demand_qty_base,
# MAGIC     demand_type, demand_subtype,
# MAGIC     sales_order_no, sales_line_no, sell_to_customer_no,
# MAGIC     prod_order_no, prod_order_line_no, original_item_no, planning_level,
# MAGIC     dimension_set_id, shortcut_dim_1, shortcut_dim_2,
# MAGIC     unit_of_measure_code, qty_per_uom
# MAGIC FROM v_demand_sales
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     bc_company, item_no, location_code, variant_code,
# MAGIC     demand_date, demand_id,
# MAGIC     demand_qty, demand_qty_base,
# MAGIC     demand_type, demand_subtype,
# MAGIC     CAST(NULL AS STRING) AS sales_order_no,
# MAGIC     CAST(NULL AS INT)    AS sales_line_no,
# MAGIC     CAST(NULL AS STRING) AS sell_to_customer_no,
# MAGIC     prod_order_no, prod_order_line_no, original_item_no, planning_level,
# MAGIC     dimension_set_id, shortcut_dim_1, shortcut_dim_2,
# MAGIC     unit_of_measure_code, qty_per_uom
# MAGIC FROM v_demand_production;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 2: PURCHASE INCOMING (ALL OPEN POs) - SUM PER ITEM
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_po_incoming_item AS
# MAGIC SELECT
# MAGIC     pl.`BC Company` AS bc_company,
# MAGIC     pl.`No.`        AS item_no,
# MAGIC     MIN(COALESCE(pl.`Expected Receipt Date`, pl.`Promised Receipt Date`, pl.`Requested Receipt Date`)) AS earliest_po_receipt_date,
# MAGIC 
# MAGIC     SUM(COALESCE(pl.`Outstanding Quantity`,0))       AS purchase_order_coming_uom1,
# MAGIC     SUM(COALESCE(pl.`Outstanding Units_DU_TSL`,0))   AS purchase_order_coming_uom2
# MAGIC FROM purchase_line pl
# MAGIC WHERE pl.`Type` = 'Item'
# MAGIC   AND COALESCE(pl.`Outstanding Quantity`,0) > 0
# MAGIC GROUP BY pl.`BC Company`, pl.`No.`;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 2B: SUPPLY EVENTS for timeline availability
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_supply_po_events AS
# MAGIC SELECT
# MAGIC     pl.`BC Company` AS bc_company,
# MAGIC     pl.`No.`        AS item_no,
# MAGIC     COALESCE(pl.`Location Code`, '') AS location_code,
# MAGIC     COALESCE(pl.`Variant Code`, '')  AS variant_code,
# MAGIC     CAST(COALESCE(pl.`Expected Receipt Date`,pl.`Promised Receipt Date`,pl.`Requested Receipt Date`) AS DATE) AS supply_date,
# MAGIC     SUM(COALESCE(pl.`Outstanding Quantity`,0)) AS supply_qty_base
# MAGIC FROM purchase_line pl
# MAGIC WHERE pl.`Type` = 'Item'
# MAGIC   AND COALESCE(pl.`Outstanding Quantity`,0) > 0
# MAGIC   AND COALESCE(pl.`Expected Receipt Date`, pl.`Promised Receipt Date`, pl.`Requested Receipt Date`) IS NOT NULL
# MAGIC GROUP BY
# MAGIC     pl.`BC Company`, pl.`No.`,
# MAGIC     COALESCE(pl.`Location Code`, ''), COALESCE(pl.`Variant Code`, ''),
# MAGIC     COALESCE(pl.`Expected Receipt Date`, pl.`Promised Receipt Date`, pl.`Requested Receipt Date`);
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW v_supply_production_events AS
# MAGIC SELECT
# MAGIC     `BC Company` AS bc_company,
# MAGIC     `Item No.`   AS item_no,
# MAGIC     COALESCE(`Location Code`, '') AS location_code,
# MAGIC     COALESCE(`Variant Code`, '')  AS variant_code,
# MAGIC     CAST(`Due Date` AS DATE) AS supply_date,
# MAGIC     SUM(`Remaining Qty. (Base)`) AS supply_qty_base
# MAGIC FROM prod_order_line
# MAGIC WHERE `Remaining Quantity` > 0
# MAGIC   AND `Status` IN ('Planned', 'Firm Planned', 'Released')
# MAGIC GROUP BY `BC Company`, `Item No.`, COALESCE(`Location Code`, ''), COALESCE(`Variant Code`, ''), `Due Date`;
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW v_supply_all_events AS
# MAGIC SELECT bc_company, item_no, location_code, variant_code, supply_date, supply_qty_base FROM v_supply_po_events
# MAGIC UNION ALL
# MAGIC SELECT bc_company, item_no, location_code, variant_code, supply_date, supply_qty_base FROM v_supply_production_events;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 3: ITEM PLANNING PARAMETERS
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_item_planning_full AS
# MAGIC SELECT
# MAGIC     i.`BC Company` AS bc_company,
# MAGIC     i.`No.`        AS item_no,
# MAGIC 
# MAGIC     CAST(COALESCE(i.`Reordering Policy`, '') AS STRING) AS reordering_policy,
# MAGIC     COALESCE(i.`Reorder Point`, 0)          AS reorder_point,
# MAGIC     COALESCE(i.`Reorder Quantity`, 0)       AS reorder_quantity,
# MAGIC     COALESCE(i.`Maximum Inventory`, 0)      AS maximum_inventory,
# MAGIC     COALESCE(i.`Lot Size`, 0)               AS lot_size,
# MAGIC     COALESCE(i.`Minimum Order Quantity`, 0) AS min_order_qty,
# MAGIC     COALESCE(i.`Maximum Order Quantity`, 0) AS max_order_qty,
# MAGIC     COALESCE(i.`Order Multiple`, 0)         AS order_multiple,
# MAGIC     COALESCE(i.`Safety Stock Quantity`, 0)  AS safety_stock,
# MAGIC 
# MAGIC     i.`Lead Time Calculation` AS lead_time_calc,
# MAGIC     CASE
# MAGIC         WHEN RIGHT(TRIM(i.`Lead Time Calculation`), 1) = 'D' THEN CAST(REGEXP_EXTRACT(i.`Lead Time Calculation`, '([0-9]+)', 1) AS INT)
# MAGIC         WHEN RIGHT(TRIM(i.`Lead Time Calculation`), 1) = 'W' THEN CAST(REGEXP_EXTRACT(i.`Lead Time Calculation`, '([0-9]+)', 1) AS INT) * 7
# MAGIC         WHEN RIGHT(TRIM(i.`Lead Time Calculation`), 1) = 'M' THEN CAST(REGEXP_EXTRACT(i.`Lead Time Calculation`, '([0-9]+)', 1) AS INT) * 30
# MAGIC         WHEN RIGHT(TRIM(i.`Lead Time Calculation`), 1) = 'Y' THEN CAST(REGEXP_EXTRACT(i.`Lead Time Calculation`, '([0-9]+)', 1) AS INT) * 365
# MAGIC         ELSE 0
# MAGIC     END AS lead_time_days,
# MAGIC 
# MAGIC     CAST(i.`Replenishment System` AS STRING) AS replenishment_system,
# MAGIC     i.`Vendor No.` AS default_vendor_no,
# MAGIC 
# MAGIC     CAST(i.`Description` AS STRING)          AS item_description,
# MAGIC     CAST(i.`Description 2` AS STRING)        AS description_2,
# MAGIC     CAST(i.`Base Unit of Measure` AS STRING) AS base_uom,
# MAGIC 
# MAGIC     COALESCE(i.`Unit Cost`, 0)       AS unit_cost,
# MAGIC     COALESCE(i.`Indirect Cost %`, 0) AS indirect_cost_pct,
# MAGIC     COALESCE(i.`Overhead Rate`, 0)   AS overhead_rate,
# MAGIC     COALESCE(i.`Scrap %`, 0)         AS scrap_pct,
# MAGIC 
# MAGIC     CAST(i.`Production BOM No.` AS STRING) AS production_bom_no,
# MAGIC     CAST(i.`Routing No.` AS STRING)        AS routing_no,
# MAGIC     CAST(i.`Low-Level Code` AS STRING)     AS low_level_code,
# MAGIC 
# MAGIC     CAST(i.`Item Category Code` AS STRING)       AS item_category_code,
# MAGIC     CAST(i.`Gen. Prod. Posting Group` AS STRING) AS gen_prod_posting_group,
# MAGIC 
# MAGIC     COALESCE(u_pc.`Code`, u_cm.`Code`) AS secondary_uom,
# MAGIC     COALESCE(u_pc.`Qty. per Unit of Measure`, u_cm.`Qty. per Unit of Measure`, 0) AS secondary_qty_per_base
# MAGIC FROM item i
# MAGIC LEFT JOIN item_unit_of_measure u_pc
# MAGIC   ON i.`BC Company` = u_pc.`BC Company`
# MAGIC  AND i.`No.`        = u_pc.`Item No.`
# MAGIC  AND u_pc.`Code` IN ('PC','PCS')
# MAGIC LEFT JOIN item_unit_of_measure u_cm
# MAGIC   ON i.`BC Company` = u_cm.`BC Company`
# MAGIC  AND i.`No.`        = u_cm.`Item No.`
# MAGIC  AND u_cm.`Code` = 'CM';
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 4A: TIMELINE EVENTS
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_timeline_events AS
# MAGIC SELECT
# MAGIC     bc_company, item_no, location_code, variant_code,
# MAGIC     demand_date AS event_date,
# MAGIC     demand_id   AS event_id,
# MAGIC     'DEMAND'    AS event_type,
# MAGIC     -demand_qty_base AS qty_impact
# MAGIC FROM v_demand_all
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     bc_company, item_no, location_code, variant_code,
# MAGIC     supply_date AS event_date,
# MAGIC     CONCAT('SUPPLY-', CAST(supply_date AS STRING), '-', item_no, '-', location_code, '-', variant_code) AS event_id,
# MAGIC     'SUPPLY' AS event_type,
# MAGIC     supply_qty_base AS qty_impact
# MAGIC FROM v_supply_all_events;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 4B: RUNNING AVAILABILITY
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_running_availability AS
# MAGIC SELECT
# MAGIC     t.*,
# MAGIC     COALESCE(oh.onhand_qty, 0) AS onhand_qty,
# MAGIC     COALESCE(oh.onhand_qty, 0)
# MAGIC     + COALESCE(
# MAGIC         SUM(CASE WHEN event_type = 'SUPPLY' THEN qty_impact ELSE 0 END)
# MAGIC         OVER (
# MAGIC           PARTITION BY t.bc_company, t.item_no, t.location_code, t.variant_code
# MAGIC           ORDER BY t.event_date, t.event_id
# MAGIC           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
# MAGIC         ), 0)
# MAGIC     - COALESCE(
# MAGIC         SUM(CASE WHEN event_type = 'DEMAND' THEN -qty_impact ELSE 0 END)
# MAGIC         OVER (
# MAGIC           PARTITION BY t.bc_company, t.item_no, t.location_code, t.variant_code
# MAGIC           ORDER BY t.event_date, t.event_id
# MAGIC           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
# MAGIC         ), 0) AS available_before_this_event
# MAGIC FROM v_timeline_events t
# MAGIC LEFT JOIN v_onhand_ile oh
# MAGIC   ON t.bc_company    = oh.bc_company
# MAGIC  AND t.item_no       = oh.item_no
# MAGIC  AND t.location_code = oh.location_code
# MAGIC WHERE t.event_type = 'DEMAND';
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 4C: DEMAND + PLANNING PARAMS
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_demand_with_availability AS
# MAGIC SELECT
# MAGIC     d.*,
# MAGIC     ra.onhand_qty,
# MAGIC     ra.available_before_this_event AS available_before_this_demand,
# MAGIC     ip.safety_stock, ip.reorder_point, ip.reorder_quantity,
# MAGIC     ip.maximum_inventory, ip.lot_size, ip.min_order_qty, ip.max_order_qty,
# MAGIC     ip.order_multiple, ip.reordering_policy, ip.replenishment_system,
# MAGIC     ip.default_vendor_no, ip.item_description, ip.description_2,
# MAGIC     ip.unit_cost, ip.production_bom_no, ip.routing_no, ip.lead_time_days,
# MAGIC     ip.item_category_code, ip.gen_prod_posting_group, ip.low_level_code,
# MAGIC     ip.indirect_cost_pct, ip.overhead_rate, ip.scrap_pct,
# MAGIC     ip.base_uom, ip.secondary_uom, ip.secondary_qty_per_base
# MAGIC FROM v_demand_all d
# MAGIC INNER JOIN v_running_availability ra
# MAGIC   ON d.bc_company    = ra.bc_company
# MAGIC  AND d.item_no       = ra.item_no
# MAGIC  AND d.location_code = ra.location_code
# MAGIC  AND d.variant_code  = ra.variant_code
# MAGIC  AND d.demand_id     = ra.event_id
# MAGIC LEFT JOIN v_item_planning_full ip
# MAGIC   ON d.bc_company = ip.bc_company
# MAGIC  AND d.item_no    = ip.item_no;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 5: SHORTAGE PER DEMAND (timeline-based)
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_shortage_per_demand AS
# MAGIC SELECT
# MAGIC     *,
# MAGIC     GREATEST((demand_qty_base + safety_stock) - available_before_this_demand, 0) AS shortage_qty,
# MAGIC     (available_before_this_demand - demand_qty_base) AS available_after_this_demand
# MAGIC FROM v_demand_with_availability
# MAGIC WHERE (available_before_this_demand - demand_qty_base) < safety_stock;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 6: REORDERING POLICY (timeline-based)
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_order_proposals_policy AS
# MAGIC SELECT
# MAGIC     spd.*,
# MAGIC     CASE
# MAGIC         WHEN COALESCE(spd.reordering_policy, '') IN ('', 'Order', 'Lot-for-Lot') THEN spd.shortage_qty
# MAGIC         WHEN spd.reordering_policy = 'Maximum Qty' THEN GREATEST(spd.maximum_inventory - spd.available_after_this_demand, 0)
# MAGIC         WHEN spd.reordering_policy IN ('Fixed Reorder Qty', 'Fixed Reorder Qty.') THEN
# MAGIC             CASE WHEN spd.available_after_this_demand < spd.reorder_point THEN GREATEST(spd.reorder_quantity, spd.shortage_qty) ELSE 0 END
# MAGIC         ELSE spd.shortage_qty
# MAGIC     END AS policy_qty
# MAGIC FROM v_shortage_per_demand spd
# MAGIC WHERE spd.shortage_qty > 0
# MAGIC    OR spd.available_after_this_demand < spd.reorder_point;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 7: APPLY MIN/MAX/MULTIPLE (timeline-based)
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_order_proposals_constrained AS
# MAGIC SELECT
# MAGIC     *,
# MAGIC     GREATEST(policy_qty, min_order_qty) AS qty_after_min,
# MAGIC     CASE
# MAGIC         WHEN max_order_qty > 0 THEN LEAST(GREATEST(policy_qty, min_order_qty), max_order_qty)
# MAGIC         ELSE GREATEST(policy_qty, min_order_qty)
# MAGIC     END AS qty_after_max,
# MAGIC     CASE
# MAGIC         WHEN order_multiple > 0 THEN
# MAGIC             CEIL((CASE
# MAGIC                 WHEN max_order_qty > 0 THEN LEAST(GREATEST(policy_qty, min_order_qty), max_order_qty)
# MAGIC                 ELSE GREATEST(policy_qty, min_order_qty)
# MAGIC             END) / order_multiple) * order_multiple
# MAGIC         ELSE
# MAGIC             (CASE
# MAGIC                 WHEN max_order_qty > 0 THEN LEAST(GREATEST(policy_qty, min_order_qty), max_order_qty)
# MAGIC                 ELSE GREATEST(policy_qty, min_order_qty)
# MAGIC             END)
# MAGIC     END AS final_qty
# MAGIC FROM v_order_proposals_policy
# MAGIC WHERE policy_qty > 0;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 8: DATE CALCS
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_order_proposals_dated AS
# MAGIC SELECT
# MAGIC     op.*,
# MAGIC     CASE
# MAGIC         WHEN op.demand_date IS NULL THEN NULL
# MAGIC         WHEN CAST(op.demand_date AS DATE) IS NULL THEN NULL
# MAGIC         WHEN op.lead_time_days IS NULL THEN CAST(op.demand_date AS DATE)
# MAGIC         WHEN date_add(CAST(op.demand_date AS DATE), -CAST(op.lead_time_days AS INT)) < DATE'1900-01-01' THEN NULL
# MAGIC         WHEN date_add(CAST(op.demand_date AS DATE), -CAST(op.lead_time_days AS INT)) > DATE'2100-12-31' THEN NULL
# MAGIC         ELSE CAST(date_add(CAST(op.demand_date AS DATE), -CAST(op.lead_time_days AS INT)) AS DATE)
# MAGIC     END AS due_date,
# MAGIC     CAST(op.demand_date AS DATE)               AS required_date,
# MAGIC     current_date()                             AS order_date,
# MAGIC     CASE WHEN op.replenishment_system IN ('Prod. Order','Assembly')
# MAGIC          THEN CASE
# MAGIC                 WHEN op.demand_date IS NULL THEN NULL
# MAGIC                 WHEN CAST(op.demand_date AS DATE) IS NULL THEN NULL
# MAGIC                 WHEN op.lead_time_days IS NULL THEN CAST(op.demand_date AS DATE)
# MAGIC                 WHEN date_add(CAST(op.demand_date AS DATE), -CAST(op.lead_time_days AS INT)) < DATE'1900-01-01' THEN NULL
# MAGIC                 WHEN date_add(CAST(op.demand_date AS DATE), -CAST(op.lead_time_days AS INT)) > DATE'2100-12-31' THEN NULL
# MAGIC                 ELSE CAST(date_add(CAST(op.demand_date AS DATE), -CAST(op.lead_time_days AS INT)) AS DATE)
# MAGIC               END
# MAGIC     END AS starting_date,
# MAGIC     CASE WHEN op.replenishment_system IN ('Prod. Order','Assembly')
# MAGIC          THEN CAST(op.demand_date AS DATE) END AS ending_date
# MAGIC FROM v_order_proposals_constrained op
# MAGIC WHERE op.final_qty > 0;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- FINAL: fabric_requisition_line
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TABLE fabric_requisition_line AS
# MAGIC WITH
# MAGIC demand_totals AS (
# MAGIC   SELECT
# MAGIC     bc_company,
# MAGIC     item_no,
# MAGIC     SUM(demand_qty)      AS total_demand_quantity_uom1_all,
# MAGIC     SUM(demand_qty_base) AS total_demand_quantity_base_uom1_all,
# MAGIC     COUNT(*)             AS total_demand_line_count,
# MAGIC     MIN(demand_date)     AS earliest_demand_date_all
# MAGIC   FROM v_demand_all
# MAGIC   GROUP BY bc_company, item_no
# MAGIC ),
# MAGIC 
# MAGIC ile_avg AS (
# MAGIC   SELECT
# MAGIC       `Item No.` AS item_no,
# MAGIC       SUM(ABS(`Quantity`)) / 180.0 AS avg_daily_usage
# MAGIC   FROM `Silver_BC_Lakehouse`.`bc`.`Item Ledger Entry`
# MAGIC   WHERE `Entry Type` = 'Consumption'
# MAGIC     AND `Posting Date` >= add_months(current_date(), -6)
# MAGIC   GROUP BY `Item No.`
# MAGIC ),
# MAGIC 
# MAGIC enrich AS (
# MAGIC   SELECT
# MAGIC       op.*,
# MAGIC       COALESCE(st.onhand_uom1, 0)                AS onhand_uom1,
# MAGIC       COALESCE(st.onhand_uom2, 0)                AS onhand_uom2,
# MAGIC       COALESCE(st.best_stock_location_code, '')  AS best_stock_location_code,
# MAGIC       COALESCE(po.purchase_order_coming_uom1, 0) AS purchase_order_coming_uom1,
# MAGIC       po.earliest_po_receipt_date                AS earliest_po_receipt_date,
# MAGIC       COALESCE(po.purchase_order_coming_uom2, 0) AS purchase_order_coming_uom2,
# MAGIC 
# MAGIC       dt.total_demand_quantity_uom1_all,
# MAGIC       dt.total_demand_quantity_base_uom1_all,
# MAGIC       dt.total_demand_line_count,
# MAGIC       dt.earliest_demand_date_all
# MAGIC   FROM v_order_proposals_dated op
# MAGIC   LEFT JOIN v_stock_item_sum st
# MAGIC     ON op.item_no = st.item_no
# MAGIC   LEFT JOIN v_po_incoming_item po
# MAGIC     ON op.bc_company = po.bc_company AND op.item_no = po.item_no
# MAGIC   LEFT JOIN demand_totals dt
# MAGIC     ON op.bc_company = dt.bc_company AND op.item_no = dt.item_no
# MAGIC ),
# MAGIC 
# MAGIC with_need_uom1 AS (
# MAGIC   SELECT
# MAGIC       e.*,
# MAGIC 
# MAGIC       (
# MAGIC         COALESCE(e.onhand_uom1, 0)
# MAGIC         + COALESCE(e.purchase_order_coming_uom1, 0)
# MAGIC         - COALESCE(e.total_demand_quantity_uom1_all, 0)
# MAGIC       ) AS available_after_all_demand_uom1,
# MAGIC 
# MAGIC       CASE
# MAGIC         WHEN COALESCE(e.reordering_policy, '') IN ('', 'Order', 'Lot-for-Lot') THEN
# MAGIC           GREATEST(
# MAGIC             COALESCE(e.safety_stock, 0)
# MAGIC             - (
# MAGIC                 COALESCE(e.onhand_uom1, 0)
# MAGIC               + COALESCE(e.purchase_order_coming_uom1, 0)
# MAGIC               - COALESCE(e.total_demand_quantity_uom1_all, 0)
# MAGIC               ),
# MAGIC             0
# MAGIC           )
# MAGIC 
# MAGIC         WHEN e.reordering_policy = 'Maximum Qty' THEN
# MAGIC           GREATEST(
# MAGIC             COALESCE(e.maximum_inventory, 0)
# MAGIC             - (
# MAGIC                 COALESCE(e.onhand_uom1, 0)
# MAGIC               + COALESCE(e.purchase_order_coming_uom1, 0)
# MAGIC               - COALESCE(e.total_demand_quantity_uom1_all, 0)
# MAGIC               ),
# MAGIC             0
# MAGIC           )
# MAGIC 
# MAGIC         WHEN e.reordering_policy IN ('Fixed Reorder Qty', 'Fixed Reorder Qty.') THEN
# MAGIC           CASE
# MAGIC             WHEN (
# MAGIC               COALESCE(e.onhand_uom1, 0)
# MAGIC               + COALESCE(e.purchase_order_coming_uom1, 0)
# MAGIC               - COALESCE(e.total_demand_quantity_uom1_all, 0)
# MAGIC             ) < COALESCE(e.reorder_point, 0)
# MAGIC             THEN
# MAGIC               GREATEST(
# MAGIC                 COALESCE(e.reorder_quantity, 0),
# MAGIC                 GREATEST(
# MAGIC                   COALESCE(e.safety_stock, 0)
# MAGIC                   - (
# MAGIC                       COALESCE(e.onhand_uom1, 0)
# MAGIC                     + COALESCE(e.purchase_order_coming_uom1, 0)
# MAGIC                     - COALESCE(e.total_demand_quantity_uom1_all, 0)
# MAGIC                     ),
# MAGIC                   0
# MAGIC                 )
# MAGIC               )
# MAGIC             ELSE 0
# MAGIC           END
# MAGIC 
# MAGIC         ELSE
# MAGIC           GREATEST(
# MAGIC             COALESCE(e.safety_stock, 0)
# MAGIC             - (
# MAGIC                 COALESCE(e.onhand_uom1, 0)
# MAGIC               + COALESCE(e.purchase_order_coming_uom1, 0)
# MAGIC               - COALESCE(e.total_demand_quantity_uom1_all, 0)
# MAGIC               ),
# MAGIC             0
# MAGIC           )
# MAGIC       END AS need_to_buy_qty_uom1
# MAGIC   FROM enrich e
# MAGIC ),
# MAGIC 
# MAGIC with_uom2_calc AS (
# MAGIC   SELECT
# MAGIC       n.*,
# MAGIC 
# MAGIC       CASE
# MAGIC         WHEN n.secondary_qty_per_base > 0 THEN CEIL(n.need_to_buy_qty_uom1 / n.secondary_qty_per_base)
# MAGIC         ELSE NULL
# MAGIC       END AS need_to_buy_qty_uom2_from_uom1,
# MAGIC 
# MAGIC       COALESCE(c.demand_quantity_uom2_consumption,0) AS demand_quantity_uom2,
# MAGIC 
# MAGIC       CASE
# MAGIC         WHEN COALESCE(c.demand_quantity_uom2_consumption,0) > 0 THEN
# MAGIC           GREATEST(
# MAGIC             COALESCE(c.demand_quantity_uom2_consumption,0)
# MAGIC             - COALESCE(n.onhand_uom2,0)
# MAGIC             - COALESCE(n.purchase_order_coming_uom2,0),
# MAGIC             0
# MAGIC           )
# MAGIC         ELSE
# MAGIC           CASE
# MAGIC             WHEN n.secondary_qty_per_base > 0 THEN CEIL(n.need_to_buy_qty_uom1 / n.secondary_qty_per_base)
# MAGIC             ELSE NULL
# MAGIC           END
# MAGIC       END AS need_to_buy_qty_uom2_from_consumption
# MAGIC   FROM with_need_uom1 n
# MAGIC   LEFT JOIN v_consumption_uom2 c
# MAGIC     ON n.item_no = c.item_no
# MAGIC ),
# MAGIC 
# MAGIC flag_one AS (
# MAGIC   SELECT
# MAGIC     u.*,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY u.bc_company, u.item_no
# MAGIC       ORDER BY u.demand_date ASC, u.demand_id ASC
# MAGIC     ) AS rn_for_flag
# MAGIC   FROM with_uom2_calc u
# MAGIC ),
# MAGIC 
# MAGIC -- 🔧 V3 PATCH: pre-compute final_uom1/final_uom2 ด้วย UOM-aware rounding
# MAGIC -- แยก CTE ออกมาเพื่อให้ alert_flag ใช้ค่านี้ได้โดยไม่ต้องเขียน CASE ซ้ำ
# MAGIC with_final_values AS (
# MAGIC   SELECT
# MAGIC     f.*,
# MAGIC 
# MAGIC     -- 🔧 UOM-aware final_uom1
# MAGIC     --   Ennovie UOM distribution (Apr 2026):
# MAGIC     --     PCS=9851, PRS=857, GR=216, CTS=119, CM2=78, CM=25, SET=11
# MAGIC     CASE
# MAGIC       -- Integer UOM → ปัดเป็นจำนวนเต็ม
# MAGIC       WHEN UPPER(TRIM(f.base_uom)) IN ('PCS', 'PRS', 'SET')
# MAGIC         THEN CEIL(f.need_to_buy_qty_uom1)
# MAGIC 
# MAGIC       -- CTS (carats, เพชร) → precision 0.01 CTS
# MAGIC       WHEN UPPER(TRIM(f.base_uom)) = 'CTS'
# MAGIC         THEN ROUND(f.need_to_buy_qty_uom1, 2)
# MAGIC 
# MAGIC       -- GR (grams, โลหะ) → precision 0.001 g
# MAGIC       WHEN UPPER(TRIM(f.base_uom)) = 'GR'
# MAGIC         THEN ROUND(f.need_to_buy_qty_uom1, 3)
# MAGIC 
# MAGIC       -- CM / CM2 (linear / area measure) → precision 0.01
# MAGIC       WHEN UPPER(TRIM(f.base_uom)) IN ('CM', 'CM2')
# MAGIC         THEN ROUND(f.need_to_buy_qty_uom1, 2)
# MAGIC 
# MAGIC       -- Fallback สำหรับ UOM ที่ยังไม่ได้ handle → ปัด 2 ทศนิยม
# MAGIC       ELSE ROUND(f.need_to_buy_qty_uom1, 2)
# MAGIC     END AS final_uom1_calc,
# MAGIC 
# MAGIC     -- final_uom2 ยังคง CEIL เหมือนเดิม เพราะ UOM2 ส่วนใหญ่คือ PCS
# MAGIC     CEIL(
# MAGIC       CASE
# MAGIC         WHEN COALESCE(f.need_to_buy_qty_uom2_from_consumption, 0) > 0
# MAGIC           THEN f.need_to_buy_qty_uom2_from_consumption
# MAGIC         ELSE f.need_to_buy_qty_uom2_from_uom1
# MAGIC       END
# MAGIC     ) AS final_uom2_calc
# MAGIC 
# MAGIC   FROM flag_one f
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC   -- =========================================================
# MAGIC   -- SYSTEM COLS
# MAGIC   -- =========================================================
# MAGIC   uuid()                                       AS `$systemId`,
# MAGIC   current_timestamp()                          AS SystemCreatedAt,
# MAGIC   current_timestamp()                          AS SystemModifiedAt,
# MAGIC   (unix_timestamp(current_timestamp()) * 1000) AS SystemRowVersion,
# MAGIC 
# MAGIC   -- 1) ITEM
# MAGIC   f.item_no                                     AS item_no,
# MAGIC   f.item_no                                     AS no,
# MAGIC   f.item_description                            AS description,
# MAGIC   f.description_2                               AS description_2,
# MAGIC   f.item_category_code                          AS item_category_code,
# MAGIC 
# MAGIC   -- 2) DEMAND SOURCE
# MAGIC   f.demand_type                                 AS demand_type,
# MAGIC   f.demand_subtype                              AS demand_subtype,
# MAGIC   f.demand_date                                 AS demand_date,
# MAGIC   f.sales_order_no                              AS sales_order_no,
# MAGIC   f.sales_line_no                               AS sales_order_line_no,
# MAGIC   f.prod_order_no                               AS prod_order_no,
# MAGIC   f.prod_order_line_no                          AS prod_order_line_no,
# MAGIC 
# MAGIC   -- 3) UOM DEFINITIONS
# MAGIC   f.base_uom                                    AS base_uom_uom1,
# MAGIC   f.secondary_uom                               AS secondary_uom_uom2,
# MAGIC   f.secondary_qty_per_base                      AS uom2_per_uom1,
# MAGIC   f.qty_per_uom                                 AS qty_per_unit_of_measure,
# MAGIC   f.base_uom                                    AS unit_of_measure_code,
# MAGIC 
# MAGIC   -- 4) DEMAND QTY (LINE)
# MAGIC   f.demand_qty                                  AS demand_quantity_uom1,
# MAGIC   f.demand_qty_base                             AS demand_quantity_base_uom1,
# MAGIC   f.demand_quantity_uom2                        AS demand_quantity_uom2,
# MAGIC 
# MAGIC   -- 5) SHORTAGE (LINE)
# MAGIC   f.shortage_qty                                AS shortage_quantity_uom1,
# MAGIC   f.shortage_qty                                AS shortage_quantity_base_uom1,
# MAGIC 
# MAGIC   -- 6) STOCK (ON HAND)
# MAGIC   f.onhand_uom1                                 AS onhand_quantity_uom1,
# MAGIC   f.onhand_uom2                                 AS onhand_quantity_uom2,
# MAGIC 
# MAGIC   -- 7) PURCHASE ORDER INCOMING
# MAGIC   f.purchase_order_coming_uom1                  AS po_incoming_quantity_uom1,
# MAGIC   f.purchase_order_coming_uom2                  AS po_incoming_quantity_uom2,
# MAGIC   f.earliest_po_receipt_date                    AS earliest_po_receipt_date,
# MAGIC 
# MAGIC   -- 8) NEED TO BUY (UOM1)
# MAGIC   f.need_to_buy_qty_uom1                        AS need_to_buy_quantity_uom1,
# MAGIC 
# MAGIC   -- 9) NEED TO BUY (UOM2 - METHOD 1)
# MAGIC   f.need_to_buy_qty_uom2_from_uom1              AS need_to_buy_quantity_uom2_from_uom1,
# MAGIC 
# MAGIC   -- 10) NEED TO BUY (UOM2 - METHOD 2)
# MAGIC   f.need_to_buy_qty_uom2_from_consumption       AS need_to_buy_quantity_uom2_from_consumption,
# MAGIC 
# MAGIC   -- 11) FLAG
# MAGIC   CASE WHEN f.rn_for_flag = 1 THEN 1 ELSE 0 END AS is_max_need_to_buy,
# MAGIC 
# MAGIC   -- 12) LOCATION
# MAGIC   f.location_code                               AS location_code,
# MAGIC   f.best_stock_location_code                    AS best_stock_location_code,
# MAGIC   f.variant_code                                AS variant_code,
# MAGIC 
# MAGIC   -- 13) PLANNING DEBUG
# MAGIC   f.available_before_this_demand                AS available_before_demand_uom1,
# MAGIC   f.safety_stock                                AS safety_stock_uom1,
# MAGIC   f.reorder_point                               AS reorder_point_uom1,
# MAGIC   f.reorder_quantity                            AS reorder_quantity_uom1,
# MAGIC   f.reordering_policy                           AS reordering_policy,
# MAGIC   f.policy_qty                                  AS policy_quantity_uom1,
# MAGIC   f.qty_after_min                               AS qty_after_min_uom1,
# MAGIC   f.qty_after_max                               AS qty_after_max_uom1,
# MAGIC   f.final_qty                                   AS final_quantity_uom1,
# MAGIC 
# MAGIC   -- 14) COST
# MAGIC   f.default_vendor_no                           AS vendor_no,
# MAGIC   f.unit_cost                                   AS unit_cost,
# MAGIC   (f.final_qty * f.unit_cost)                   AS cost_amount,
# MAGIC   f.indirect_cost_pct                           AS indirect_cost_pct,
# MAGIC   f.overhead_rate                               AS overhead_rate,
# MAGIC 
# MAGIC   -- 15) BC WORKSHEET COLUMNS (core)
# MAGIC   f.bc_company                                  AS bc_company,
# MAGIC   'PLANNING'                                    AS worksheet_template_name,
# MAGIC   'DEFAULT'                                     AS journal_batch_name,
# MAGIC 
# MAGIC   (ROW_NUMBER() OVER (
# MAGIC      PARTITION BY f.bc_company
# MAGIC      ORDER BY f.bc_company, f.item_no, f.location_code, f.due_date, f.demand_id
# MAGIC    ) * 10000)                                   AS line_no,
# MAGIC 
# MAGIC   'Item'                                        AS type,
# MAGIC   f.final_qty                                   AS quantity,
# MAGIC   f.final_qty                                   AS worksheet_quantity_uom1,
# MAGIC   f.unit_cost                                   AS direct_unit_cost,
# MAGIC   f.due_date                                    AS due_date,
# MAGIC   f.order_date                                  AS order_date,
# MAGIC 
# MAGIC   -- 16) BC WORKSHEET COLUMNS (dimensions & refs)
# MAGIC   f.shortcut_dim_1                              AS shortcut_dimension_1_code,
# MAGIC   f.shortcut_dim_2                              AS shortcut_dimension_2_code,
# MAGIC   f.dimension_set_id                            AS dimension_set_id,
# MAGIC   f.sell_to_customer_no                         AS sell_to_customer_no,
# MAGIC   f.unit_of_measure_code                        AS unit_of_measure_code_demand,
# MAGIC   f.qty_per_uom                                 AS qty_per_uom_demand,
# MAGIC   f.final_qty                                   AS quantity_base,
# MAGIC   COALESCE(f.sales_order_no, f.prod_order_no)    AS demand_order_no,
# MAGIC   COALESCE(f.sales_line_no, f.prod_order_line_no) AS demand_line_no,
# MAGIC   f.demand_subtype                              AS status,
# MAGIC   f.planning_level                              AS level,
# MAGIC   f.planning_level                              AS planning_level,
# MAGIC 
# MAGIC   -- 17) BC PRODUCTION / ROUTING COLUMNS
# MAGIC   f.routing_no                                  AS routing_no,
# MAGIC   f.gen_prod_posting_group                      AS gen_prod_posting_group,
# MAGIC   f.low_level_code                              AS low_level_code,
# MAGIC   f.final_qty                                   AS remaining_quantity,
# MAGIC   f.final_qty                                   AS remaining_qty_base,
# MAGIC   f.scrap_pct                                   AS scrap_pct,
# MAGIC   f.starting_date                               AS starting_date,
# MAGIC   f.ending_date                                 AS ending_date,
# MAGIC   f.production_bom_no                           AS production_bom_no,
# MAGIC   f.replenishment_system                        AS replenishment_system,
# MAGIC   f.original_item_no                            AS original_item_no,
# MAGIC 
# MAGIC   -- 18) NULL PLACEHOLDERS
# MAGIC   CAST(NULL AS STRING)                          AS requester_id,
# MAGIC   CAST(NULL AS BOOLEAN)                         AS confirmed,
# MAGIC   CAST(NULL AS STRING)                          AS recurring_method,
# MAGIC   CAST(NULL AS DATE)                            AS expiration_date,
# MAGIC   CAST(NULL AS STRING)                          AS recurring_frequency,
# MAGIC   CAST(NULL AS STRING)                          AS vendor_item_no,
# MAGIC   CAST(NULL AS STRING)                          AS ship_to_code,
# MAGIC   CAST(NULL AS STRING)                          AS order_address_code,
# MAGIC   CAST(NULL AS STRING)                          AS currency_code,
# MAGIC   CAST(NULL AS DECIMAL(38,20))                  AS currency_factor,
# MAGIC   CAST(NULL AS STRING)                          AS purchaser_code,
# MAGIC   CAST(NULL AS BOOLEAN)                         AS drop_shipment,
# MAGIC   CAST(NULL AS STRING)                          AS bin_code,
# MAGIC   CAST(NULL AS DECIMAL(38,20))                  AS qty_rounding_precision,
# MAGIC   CAST(NULL AS DECIMAL(38,20))                  AS qty_rounding_precision_base,
# MAGIC   CAST(NULL AS STRING)                          AS demand_ref_no,
# MAGIC 
# MAGIC   f.need_to_buy_qty_uom1                        AS needed_quantity,
# MAGIC   f.need_to_buy_qty_uom1                        AS needed_quantity_base,
# MAGIC 
# MAGIC   CAST(NULL AS STRING)                          AS reserve,
# MAGIC   CAST(NULL AS STRING)                          AS supply_from,
# MAGIC   CAST(NULL AS STRING)                          AS original_variant_code,
# MAGIC   f.available_before_this_demand                AS demand_qty_available,
# MAGIC   CAST(NULL AS STRING)                          AS user_id,
# MAGIC   CAST(NULL AS BOOLEAN)                         AS nonstock,
# MAGIC   CAST(NULL AS STRING)                          AS purchasing_code,
# MAGIC   CAST(NULL AS STRING)                          AS transfer_from_code,
# MAGIC   CAST(NULL AS DATE)                            AS transfer_shipment_date,
# MAGIC   CAST(NULL AS STRING)                          AS price_calculation_method,
# MAGIC   CAST(NULL AS DECIMAL(38,20))                  AS line_discount_pct,
# MAGIC   CAST(NULL AS INT)                             AS custom_sorting_order,
# MAGIC   CAST(NULL AS STRING)                          AS operation_no,
# MAGIC   CAST(NULL AS STRING)                          AS work_center_no,
# MAGIC   CAST(NULL AS BOOLEAN)                         AS mps_order,
# MAGIC   CAST(NULL AS STRING)                          AS planning_flexibility,
# MAGIC   CAST(NULL AS STRING)                          AS routing_reference_no,
# MAGIC   CAST(NULL AS STRING)                          AS gen_business_posting_group,
# MAGIC   CAST(NULL AS STRING)                          AS production_bom_version_code,
# MAGIC   CAST(NULL AS STRING)                          AS routing_version_code,
# MAGIC   CAST(NULL AS STRING)                          AS routing_type,
# MAGIC   CAST(NULL AS DECIMAL(38,20))                  AS original_quantity,
# MAGIC   CAST(NULL AS DECIMAL(38,20))                  AS finished_quantity,
# MAGIC   CAST(NULL AS DATE)                            AS original_due_date,
# MAGIC   CAST(NULL AS TIMESTAMP)                       AS starting_date_time,
# MAGIC   CAST(NULL AS STRING)                          AS starting_time,
# MAGIC   CAST(NULL AS TIMESTAMP)                       AS ending_date_time,
# MAGIC   CAST(NULL AS STRING)                          AS ending_time,
# MAGIC   CAST(NULL AS STRING)                          AS ref_order_no,
# MAGIC   CAST(NULL AS STRING)                          AS ref_order_type,
# MAGIC   CAST(NULL AS STRING)                          AS ref_order_status,
# MAGIC   CAST(NULL AS INT)                             AS ref_line_no,
# MAGIC   CAST(NULL AS STRING)                          AS no_series,
# MAGIC   CAST(NULL AS DECIMAL(38,20))                  AS finished_qty_base,
# MAGIC   CAST(NULL AS BOOLEAN)                         AS related_to_planning_line,
# MAGIC   CAST(NULL AS STRING)                          AS planning_line_origin,
# MAGIC   CAST(NULL AS STRING)                          AS action_message,
# MAGIC   CAST(NULL AS BOOLEAN)                         AS accept_action_message,
# MAGIC   CAST(NULL AS DECIMAL(38,20))                  AS net_quantity_base,
# MAGIC   CAST(NULL AS STRING)                          AS order_promising_id,
# MAGIC   CAST(NULL AS INT)                             AS order_promising_line_no,
# MAGIC   CAST(NULL AS STRING)                          AS order_promising_line_id,
# MAGIC 
# MAGIC   -- =========================================================
# MAGIC   -- FINAL ROUNDED VALUES (🔧 V3 PATCH: UOM-aware rounding)
# MAGIC   -- =========================================================
# MAGIC   f.final_uom1_calc AS final_uom1,
# MAGIC   f.final_uom2_calc AS final_uom2,
# MAGIC 
# MAGIC   -- SUMMARY / EXTRA COLS
# MAGIC   f.total_demand_quantity_uom1_all        AS total_demand_quantity_uom1_all,
# MAGIC   f.total_demand_quantity_base_uom1_all   AS total_demand_quantity_base_uom1_all,
# MAGIC   f.total_demand_line_count              AS total_demand_line_count,
# MAGIC   f.earliest_demand_date_all             AS earliest_demand_date_all,
# MAGIC 
# MAGIC   ROUND(COALESCE(il.avg_daily_usage, 0), 2)      AS avg_daily_usage,
# MAGIC   ROUND(COALESCE(il.avg_daily_usage, 0) * 60, 0) AS forecast_demand_60d,
# MAGIC 
# MAGIC   ROUND(
# MAGIC       COALESCE(f.onhand_uom1, 0)
# MAGIC     + COALESCE(f.purchase_order_coming_uom1, 0)
# MAGIC     - COALESCE(f.total_demand_quantity_uom1_all, 0)
# MAGIC     - (COALESCE(il.avg_daily_usage, 0) * 60)
# MAGIC   , 0)                                           AS projected_available_60d,
# MAGIC 
# MAGIC   -- 🔧 V3 PATCH: ใช้ need_to_buy_qty_uom1 > 0 แทน CEIL(...) > 0
# MAGIC   -- เหตุผล: CEIL(0.01 CTS) = 1 จะทำให้ alert_flag = 'ควรสั่งซื้อ' ผิดๆ
# MAGIC   -- ใช้ค่า raw > 0 ตรงไปตรงมากว่า
# MAGIC   CASE
# MAGIC     WHEN f.due_date <= date_add(current_date(), 14) AND f.need_to_buy_qty_uom1 > 0
# MAGIC       THEN 'URGENT - ต้องสั่งภายใน 14 วัน'
# MAGIC     WHEN f.need_to_buy_qty_uom1 > 0
# MAGIC       THEN 'ควรสั่งซื้อ'
# MAGIC     WHEN (
# MAGIC         COALESCE(f.onhand_uom1, 0)
# MAGIC       + COALESCE(f.purchase_order_coming_uom1, 0)
# MAGIC       - COALESCE(f.total_demand_quantity_uom1_all, 0)
# MAGIC       - (COALESCE(il.avg_daily_usage, 0) * 60)
# MAGIC     ) < 0
# MAGIC       THEN 'เฝ้าระวัง - คาดว่าจะขาดใน 60 วัน'
# MAGIC     ELSE 'OK'
# MAGIC   END                                            AS alert_flag,
# MAGIC 
# MAGIC   CASE
# MAGIC     WHEN COALESCE(il.avg_daily_usage, 0) > 0
# MAGIC     THEN date_add(
# MAGIC           current_date(),
# MAGIC           CAST(
# MAGIC             (COALESCE(f.onhand_uom1, 0) + COALESCE(f.purchase_order_coming_uom1, 0))
# MAGIC             / COALESCE(il.avg_daily_usage, 1)
# MAGIC           AS INT)
# MAGIC         )
# MAGIC     ELSE NULL
# MAGIC   END                                            AS projected_stockout_date
# MAGIC 
# MAGIC FROM with_final_values f
# MAGIC LEFT JOIN ile_avg il
# MAGIC   ON f.item_no = il.item_no;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# # Line Prod

# CELL ********************

# MAGIC %%sql
# MAGIC -- ============================================================
# MAGIC -- MRP / Fabric Requisition Line PROD (FULL FIXED CODE)
# MAGIC -- ============================================================
# MAGIC 
# MAGIC USE Silver_Planning_Lakehouse.dbo;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 0: STOCK (gold_stock_item) - SUM per item (all locations)
# MAGIC --         + best_stock_location_code (highest rem_uom2)
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_stock_item_sum AS
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     item_no,
# MAGIC     COALESCE(location,'')       AS stock_location_code,
# MAGIC     COALESCE(rem_uom1, 0)       AS rem_uom1,
# MAGIC     COALESCE(rem_uom2, 0)       AS rem_uom2
# MAGIC   FROM gold_stock_item
# MAGIC ),
# MAGIC sum_per_item AS (
# MAGIC   SELECT
# MAGIC     item_no,
# MAGIC     SUM(rem_uom1) AS onhand_uom1,
# MAGIC     SUM(rem_uom2) AS onhand_uom2
# MAGIC   FROM base
# MAGIC   GROUP BY item_no
# MAGIC ),
# MAGIC best_loc AS (
# MAGIC   SELECT
# MAGIC     item_no,
# MAGIC     stock_location_code AS best_stock_location_code,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY item_no
# MAGIC       ORDER BY rem_uom2 DESC, stock_location_code
# MAGIC     ) AS rn
# MAGIC   FROM base
# MAGIC )
# MAGIC SELECT
# MAGIC   s.item_no,
# MAGIC   s.onhand_uom1,
# MAGIC   s.onhand_uom2,
# MAGIC   COALESCE(b.best_stock_location_code,'') AS best_stock_location_code
# MAGIC FROM sum_per_item s
# MAGIC LEFT JOIN (SELECT item_no, best_stock_location_code FROM best_loc WHERE rn = 1) b
# MAGIC   ON s.item_no = b.item_no;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 0B: GOLD CONSUMPTION (exp2 - con2 logic for UOM2 demand)
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_consumption_uom2 AS
# MAGIC SELECT
# MAGIC     ComponentItemNo AS item_no,
# MAGIC     SUM(GREATEST(COALESCE(exp2,0) - COALESCE(con2,0),0)) AS demand_quantity_uom2_consumption
# MAGIC FROM gold_consumption
# MAGIC WHERE Status IN ('Planned','Firm Planned','Released')
# MAGIC GROUP BY ComponentItemNo;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 1A: CURRENT INVENTORY (Item Ledger) - timeline availability
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_onhand_ile AS
# MAGIC SELECT
# MAGIC     `BC Company`                AS bc_company,
# MAGIC     `Item No.`                  AS item_no,
# MAGIC     COALESCE(`Location Code`,'') AS location_code,
# MAGIC     SUM(`Remaining Quantity`)    AS onhand_qty,
# MAGIC     MAX(`Posting Date`)          AS last_movement_date
# MAGIC FROM item_ledger_entry
# MAGIC WHERE `Open` = 1
# MAGIC GROUP BY `BC Company`, `Item No.`, COALESCE(`Location Code`,'');
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 1B: SALES DEMAND (line level)
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_demand_sales AS
# MAGIC SELECT
# MAGIC     `BC Company` AS bc_company,
# MAGIC     `No.`        AS item_no,
# MAGIC     COALESCE(`Location Code`, '') AS location_code,
# MAGIC     COALESCE(`Variant Code`, '')  AS variant_code,
# MAGIC     COALESCE(`Shipment Date`, `Requested Delivery Date`, `Promised Delivery Date`) AS demand_date,
# MAGIC 
# MAGIC     `Outstanding Quantity`      AS demand_qty,
# MAGIC     `Outstanding Qty. (Base)`   AS demand_qty_base,
# MAGIC 
# MAGIC     `Document No.`              AS sales_order_no,
# MAGIC     `Line No.`                  AS sales_line_no,
# MAGIC     `Sell-to Customer No.`      AS sell_to_customer_no,
# MAGIC 
# MAGIC     `Dimension Set ID`          AS dimension_set_id,
# MAGIC     `Shortcut Dimension 1 Code` AS shortcut_dim_1,
# MAGIC     `Shortcut Dimension 2 Code` AS shortcut_dim_2,
# MAGIC     `Unit of Measure Code`      AS unit_of_measure_code,
# MAGIC     `Qty. per Unit of Measure`  AS qty_per_uom,
# MAGIC 
# MAGIC     'Sales'                     AS demand_type,
# MAGIC     CAST(`Document Type` AS STRING) AS demand_subtype,
# MAGIC 
# MAGIC     CONCAT(`BC Company`, '-SALES-', `Document No.`, '-', CAST(`Line No.` AS STRING)) AS demand_id,
# MAGIC 
# MAGIC     CAST(NULL AS STRING) AS prod_order_no,
# MAGIC     CAST(NULL AS INT)    AS prod_order_line_no,
# MAGIC     CAST(NULL AS STRING) AS original_item_no,
# MAGIC     0                    AS planning_level
# MAGIC 
# MAGIC FROM sales_line
# MAGIC WHERE `Type` = 'Item'
# MAGIC   AND `Outstanding Quantity` > 0
# MAGIC   AND COALESCE(`Shipment Date`, `Requested Delivery Date`, `Promised Delivery Date`) IS NOT NULL;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 1C: PRODUCTION COMPONENT DEMAND (PER PROD ORDER + LINE)
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_demand_production AS
# MAGIC SELECT
# MAGIC     `BC Company` AS bc_company,
# MAGIC     `Item No.`   AS item_no,
# MAGIC     COALESCE(`Location Code`, '') AS location_code,
# MAGIC     COALESCE(`Variant Code`, '')  AS variant_code,
# MAGIC 
# MAGIC     `Due Date` AS demand_date,
# MAGIC 
# MAGIC     SUM(`Remaining Quantity`)     AS demand_qty,
# MAGIC     SUM(`Remaining Qty. (Base)`)  AS demand_qty_base,
# MAGIC 
# MAGIC     'Production' AS demand_type,
# MAGIC     CAST(`Status` AS STRING)   AS demand_subtype,
# MAGIC 
# MAGIC     CONCAT(
# MAGIC       `BC Company`, '-PROD-',
# MAGIC       `Prod. Order No.`, '-',
# MAGIC       CAST(`Prod. Order Line No.` AS STRING), '-',
# MAGIC       `Item No.`, '-',
# MAGIC       CAST(`Due Date` AS STRING)
# MAGIC     ) AS demand_id,
# MAGIC 
# MAGIC     `Prod. Order No.`      AS prod_order_no,
# MAGIC     `Prod. Order Line No.` AS prod_order_line_no,
# MAGIC 
# MAGIC     CAST(NULL AS STRING) AS original_item_no,
# MAGIC     1                    AS planning_level,
# MAGIC 
# MAGIC     CAST(NULL AS INT)    AS dimension_set_id,
# MAGIC     CAST(NULL AS STRING) AS shortcut_dim_1,
# MAGIC     CAST(NULL AS STRING) AS shortcut_dim_2,
# MAGIC     CAST(NULL AS STRING) AS unit_of_measure_code,
# MAGIC     CAST(NULL AS DECIMAL(38,20)) AS qty_per_uom
# MAGIC 
# MAGIC FROM prod_order_component
# MAGIC WHERE `Status` IN ('Planned','Firm Planned','Released')
# MAGIC   AND COALESCE(`Remaining Quantity`,0) > 0
# MAGIC   AND `Due Date` IS NOT NULL
# MAGIC GROUP BY
# MAGIC     `BC Company`,
# MAGIC     `Item No.`,
# MAGIC     COALESCE(`Location Code`, ''),
# MAGIC     COALESCE(`Variant Code`, ''),
# MAGIC     `Due Date`,
# MAGIC     `Prod. Order No.`,
# MAGIC     `Prod. Order Line No.`,
# MAGIC     `Status`;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 1D: COMBINE DEMAND
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_demand_all AS
# MAGIC SELECT
# MAGIC     bc_company, item_no, location_code, variant_code,
# MAGIC     demand_date, demand_id,
# MAGIC     demand_qty, demand_qty_base,
# MAGIC     demand_type, demand_subtype,
# MAGIC 
# MAGIC     sales_order_no, sales_line_no, sell_to_customer_no,
# MAGIC     prod_order_no, prod_order_line_no, original_item_no, planning_level,
# MAGIC 
# MAGIC     dimension_set_id, shortcut_dim_1, shortcut_dim_2,
# MAGIC     unit_of_measure_code, qty_per_uom
# MAGIC FROM v_demand_sales
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT
# MAGIC     bc_company, item_no, location_code, variant_code,
# MAGIC     demand_date, demand_id,
# MAGIC     demand_qty, demand_qty_base,
# MAGIC     demand_type, demand_subtype,
# MAGIC 
# MAGIC     CAST(NULL AS STRING) AS sales_order_no,
# MAGIC     CAST(NULL AS INT)    AS sales_line_no,
# MAGIC     CAST(NULL AS STRING) AS sell_to_customer_no,
# MAGIC 
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     original_item_no,
# MAGIC     planning_level,
# MAGIC 
# MAGIC     dimension_set_id, shortcut_dim_1, shortcut_dim_2,
# MAGIC     unit_of_measure_code, qty_per_uom
# MAGIC FROM v_demand_production;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 2: PURCHASE INCOMING (ALL OPEN POs) - SUM PER ITEM
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_po_incoming_item AS
# MAGIC SELECT
# MAGIC     pl.`BC Company` AS bc_company,
# MAGIC     pl.`No.`        AS item_no,
# MAGIC     MIN(COALESCE(pl.`Expected Receipt Date`, pl.`Promised Receipt Date`, pl.`Requested Receipt Date`)) AS earliest_po_receipt_date,
# MAGIC     SUM(
# MAGIC       COALESCE(pl.`Outstanding Quantity`,0) * COALESCE(pl.`Qty. per Unit of Measure`,1)
# MAGIC     ) AS purchase_order_coming_uom1
# MAGIC FROM purchase_line pl
# MAGIC WHERE pl.`Type` = 'Item'
# MAGIC   AND COALESCE(pl.`Outstanding Quantity`,0) > 0
# MAGIC GROUP BY
# MAGIC     pl.`BC Company`,
# MAGIC     pl.`No.`;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 2B: SUPPLY EVENTS for timeline availability
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_supply_po_events AS
# MAGIC SELECT
# MAGIC     pl.`BC Company` AS bc_company,
# MAGIC     pl.`No.`        AS item_no,
# MAGIC     COALESCE(pl.`Location Code`, '') AS location_code,
# MAGIC     COALESCE(pl.`Variant Code`, '')  AS variant_code,
# MAGIC     COALESCE(pl.`Expected Receipt Date`, pl.`Promised Receipt Date`, pl.`Requested Receipt Date`) AS supply_date,
# MAGIC     SUM(COALESCE(pl.`Outstanding Quantity`,0) * COALESCE(pl.`Qty. per Unit of Measure`,1)) AS supply_qty_base
# MAGIC FROM purchase_line pl
# MAGIC WHERE pl.`Type` = 'Item'
# MAGIC   AND COALESCE(pl.`Outstanding Quantity`,0) > 0
# MAGIC   AND COALESCE(pl.`Expected Receipt Date`, pl.`Promised Receipt Date`, pl.`Requested Receipt Date`) IS NOT NULL
# MAGIC GROUP BY
# MAGIC     pl.`BC Company`,
# MAGIC     pl.`No.`,
# MAGIC     COALESCE(pl.`Location Code`, ''),
# MAGIC     COALESCE(pl.`Variant Code`, ''),
# MAGIC     COALESCE(pl.`Expected Receipt Date`, pl.`Promised Receipt Date`, pl.`Requested Receipt Date`);
# MAGIC 
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW v_supply_production_events AS
# MAGIC SELECT
# MAGIC     `BC Company` AS bc_company,
# MAGIC     `Item No.`   AS item_no,
# MAGIC     COALESCE(`Location Code`, '') AS location_code,
# MAGIC     COALESCE(`Variant Code`, '')  AS variant_code,
# MAGIC     `Due Date`   AS supply_date,
# MAGIC     SUM(`Remaining Qty. (Base)`) AS supply_qty_base
# MAGIC FROM prod_order_line
# MAGIC WHERE `Remaining Quantity` > 0
# MAGIC   AND `Status` IN ('Planned', 'Firm Planned', 'Released')
# MAGIC GROUP BY
# MAGIC     `BC Company`,
# MAGIC     `Item No.`,
# MAGIC     COALESCE(`Location Code`, ''),
# MAGIC     COALESCE(`Variant Code`, ''),
# MAGIC     `Due Date`;
# MAGIC 
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW v_supply_all_events AS
# MAGIC SELECT bc_company, item_no, location_code, variant_code, supply_date, supply_qty_base FROM v_supply_po_events
# MAGIC UNION ALL
# MAGIC SELECT bc_company, item_no, location_code, variant_code, supply_date, supply_qty_base FROM v_supply_production_events;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 3: ITEM PLANNING PARAMETERS
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_item_planning_full AS
# MAGIC SELECT
# MAGIC     i.`BC Company` AS bc_company,
# MAGIC     i.`No.`        AS item_no,
# MAGIC 
# MAGIC     CAST(COALESCE(i.`Reordering Policy`, '') AS STRING) AS reordering_policy,
# MAGIC     COALESCE(i.`Reorder Point`, 0)          AS reorder_point,
# MAGIC     COALESCE(i.`Reorder Quantity`, 0)       AS reorder_quantity,
# MAGIC     COALESCE(i.`Maximum Inventory`, 0)      AS maximum_inventory,
# MAGIC     COALESCE(i.`Lot Size`, 0)               AS lot_size,
# MAGIC     COALESCE(i.`Minimum Order Quantity`, 0) AS min_order_qty,
# MAGIC     COALESCE(i.`Maximum Order Quantity`, 0) AS max_order_qty,
# MAGIC     COALESCE(i.`Order Multiple`, 0)         AS order_multiple,
# MAGIC     COALESCE(i.`Safety Stock Quantity`, 0)  AS safety_stock,
# MAGIC 
# MAGIC     i.`Lead Time Calculation` AS lead_time_calc,
# MAGIC     CASE
# MAGIC         WHEN RIGHT(TRIM(i.`Lead Time Calculation`), 1) = 'D' THEN CAST(REGEXP_EXTRACT(i.`Lead Time Calculation`, '([0-9]+)', 1) AS INT)
# MAGIC         WHEN RIGHT(TRIM(i.`Lead Time Calculation`), 1) = 'W' THEN CAST(REGEXP_EXTRACT(i.`Lead Time Calculation`, '([0-9]+)', 1) AS INT) * 7
# MAGIC         WHEN RIGHT(TRIM(i.`Lead Time Calculation`), 1) = 'M' THEN CAST(REGEXP_EXTRACT(i.`Lead Time Calculation`, '([0-9]+)', 1) AS INT) * 30
# MAGIC         WHEN RIGHT(TRIM(i.`Lead Time Calculation`), 1) = 'Y' THEN CAST(REGEXP_EXTRACT(i.`Lead Time Calculation`, '([0-9]+)', 1) AS INT) * 365
# MAGIC         ELSE 0
# MAGIC     END AS lead_time_days,
# MAGIC 
# MAGIC     CAST(i.`Replenishment System` AS STRING) AS replenishment_system,
# MAGIC     i.`Vendor No.` AS default_vendor_no,
# MAGIC 
# MAGIC     CAST(i.`Description` AS STRING)   AS item_description,
# MAGIC     CAST(i.`Description 2` AS STRING) AS description_2,
# MAGIC     CAST(i.`Base Unit of Measure` AS STRING) AS base_uom,
# MAGIC 
# MAGIC     COALESCE(i.`Unit Cost`, 0)        AS unit_cost,
# MAGIC     COALESCE(i.`Indirect Cost %`, 0)  AS indirect_cost_pct,
# MAGIC     COALESCE(i.`Overhead Rate`, 0)    AS overhead_rate,
# MAGIC     COALESCE(i.`Scrap %`, 0)          AS scrap_pct,
# MAGIC 
# MAGIC     CAST(i.`Production BOM No.` AS STRING) AS production_bom_no,
# MAGIC     CAST(i.`Routing No.` AS STRING)        AS routing_no,
# MAGIC     CAST(i.`Low-Level Code` AS STRING)     AS low_level_code,
# MAGIC 
# MAGIC     CAST(i.`Item Category Code` AS STRING) AS item_category_code,
# MAGIC     CAST(i.`Gen. Prod. Posting Group` AS STRING) AS gen_prod_posting_group,
# MAGIC 
# MAGIC     COALESCE(u_pc.`Code`, u_cm.`Code`) AS secondary_uom,
# MAGIC     COALESCE(u_pc.`Qty. per Unit of Measure`, u_cm.`Qty. per Unit of Measure`, 0) AS secondary_qty_per_base
# MAGIC 
# MAGIC FROM item i
# MAGIC LEFT JOIN item_unit_of_measure u_pc
# MAGIC   ON i.`BC Company` = u_pc.`BC Company`
# MAGIC  AND i.`No.`        = u_pc.`Item No.`
# MAGIC  AND u_pc.`Code` IN ('PC','PCS')
# MAGIC LEFT JOIN item_unit_of_measure u_cm
# MAGIC   ON i.`BC Company` = u_cm.`BC Company`
# MAGIC  AND i.`No.`        = u_cm.`Item No.`
# MAGIC  AND u_cm.`Code` = 'CM';
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 4A: TIMELINE EVENTS
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_timeline_events AS
# MAGIC SELECT
# MAGIC     bc_company, item_no, location_code, variant_code,
# MAGIC     demand_date AS event_date,
# MAGIC     demand_id   AS event_id,
# MAGIC     'DEMAND'    AS event_type,
# MAGIC     -demand_qty_base AS qty_impact
# MAGIC FROM v_demand_all
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     bc_company, item_no, location_code, variant_code,
# MAGIC     supply_date AS event_date,
# MAGIC     CONCAT('SUPPLY-', CAST(supply_date AS STRING), '-', item_no, '-', location_code, '-', variant_code) AS event_id,
# MAGIC     'SUPPLY' AS event_type,
# MAGIC     supply_qty_base AS qty_impact
# MAGIC FROM v_supply_all_events;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 4B: RUNNING AVAILABILITY
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_running_availability AS
# MAGIC SELECT
# MAGIC     t.*,
# MAGIC     COALESCE(oh.onhand_qty, 0) AS onhand_qty,
# MAGIC     COALESCE(oh.onhand_qty, 0)
# MAGIC     + COALESCE(
# MAGIC         SUM(CASE WHEN event_type = 'SUPPLY' THEN qty_impact ELSE 0 END)
# MAGIC         OVER (
# MAGIC           PARTITION BY t.bc_company, t.item_no, t.location_code, t.variant_code
# MAGIC           ORDER BY t.event_date, t.event_id
# MAGIC           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
# MAGIC         ), 0
# MAGIC       )
# MAGIC     - COALESCE(
# MAGIC         SUM(CASE WHEN event_type = 'DEMAND' THEN -qty_impact ELSE 0 END)
# MAGIC         OVER (
# MAGIC           PARTITION BY t.bc_company, t.item_no, t.location_code, t.variant_code
# MAGIC           ORDER BY t.event_date, t.event_id
# MAGIC           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
# MAGIC         ), 0
# MAGIC       ) AS available_before_this_event
# MAGIC FROM v_timeline_events t
# MAGIC LEFT JOIN v_onhand_ile oh
# MAGIC   ON t.bc_company    = oh.bc_company
# MAGIC  AND t.item_no       = oh.item_no
# MAGIC  AND t.location_code = oh.location_code
# MAGIC WHERE t.event_type = 'DEMAND';
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 4C: DEMAND + PLANNING PARAMS
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_demand_with_availability AS
# MAGIC SELECT
# MAGIC     d.*,
# MAGIC     ra.onhand_qty,
# MAGIC     ra.available_before_this_event AS available_before_this_demand,
# MAGIC 
# MAGIC     ip.safety_stock,
# MAGIC     ip.reorder_point,
# MAGIC     ip.reorder_quantity,
# MAGIC     ip.maximum_inventory,
# MAGIC     ip.lot_size,
# MAGIC     ip.min_order_qty,
# MAGIC     ip.max_order_qty,
# MAGIC     ip.order_multiple,
# MAGIC     ip.reordering_policy,
# MAGIC     ip.replenishment_system,
# MAGIC     ip.default_vendor_no,
# MAGIC     ip.item_description,
# MAGIC     ip.description_2,
# MAGIC     ip.unit_cost,
# MAGIC     ip.production_bom_no,
# MAGIC     ip.routing_no,
# MAGIC     ip.lead_time_days,
# MAGIC     ip.item_category_code,
# MAGIC     ip.gen_prod_posting_group,
# MAGIC     ip.low_level_code,
# MAGIC     ip.indirect_cost_pct,
# MAGIC     ip.overhead_rate,
# MAGIC     ip.scrap_pct,
# MAGIC     ip.base_uom,
# MAGIC     ip.secondary_uom,
# MAGIC     ip.secondary_qty_per_base
# MAGIC FROM v_demand_all d
# MAGIC INNER JOIN v_running_availability ra
# MAGIC   ON d.bc_company    = ra.bc_company
# MAGIC  AND d.item_no       = ra.item_no
# MAGIC  AND d.location_code = ra.location_code
# MAGIC  AND d.variant_code  = ra.variant_code
# MAGIC  AND d.demand_id     = ra.event_id
# MAGIC LEFT JOIN v_item_planning_full ip
# MAGIC   ON d.bc_company = ip.bc_company
# MAGIC  AND d.item_no    = ip.item_no;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 5: SHORTAGE PER DEMAND
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_shortage_per_demand AS
# MAGIC SELECT
# MAGIC     *,
# MAGIC     GREATEST((demand_qty_base + safety_stock) - available_before_this_demand, 0) AS shortage_qty,
# MAGIC     (available_before_this_demand - demand_qty_base) AS available_after_this_demand
# MAGIC FROM v_demand_with_availability
# MAGIC WHERE (available_before_this_demand - demand_qty_base) < safety_stock;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 6: REORDERING POLICY  (FULL FIXED)
# MAGIC -- Fix for Fixed Reorder Qty:
# MAGIC --   If available_after_this_demand < reorder_point:
# MAGIC --      policy_qty = GREATEST(reorder_quantity, shortage_qty)
# MAGIC --   else 0
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_order_proposals_policy AS
# MAGIC SELECT
# MAGIC     spd.*,
# MAGIC 
# MAGIC     CASE
# MAGIC         -- Default / Lot-for-Lot: cover the shortage only
# MAGIC         WHEN COALESCE(spd.reordering_policy, '') IN ('', 'Order', 'Lot-for-Lot') THEN
# MAGIC             spd.shortage_qty
# MAGIC 
# MAGIC         -- Maximum Qty: fill up to maximum_inventory based on projected after-demand
# MAGIC         WHEN spd.reordering_policy = 'Maximum Qty' THEN
# MAGIC             GREATEST(spd.maximum_inventory - spd.available_after_this_demand, 0)
# MAGIC 
# MAGIC         -- Fixed Reorder Qty: when below reorder_point, buy at least reorder_quantity,
# MAGIC         -- but ALSO must cover the shortage if shortage is bigger than reorder_quantity
# MAGIC         WHEN spd.reordering_policy IN ('Fixed Reorder Qty', 'Fixed Reorder Qty.') THEN
# MAGIC             CASE
# MAGIC                 WHEN spd.available_after_this_demand < spd.reorder_point THEN
# MAGIC                     GREATEST(spd.reorder_quantity, spd.shortage_qty)
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC 
# MAGIC         -- Fallback: cover shortage
# MAGIC         ELSE spd.shortage_qty
# MAGIC     END AS policy_qty
# MAGIC 
# MAGIC FROM v_shortage_per_demand spd
# MAGIC WHERE spd.shortage_qty > 0
# MAGIC    OR spd.available_after_this_demand < spd.reorder_point;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 7: APPLY MIN/MAX/MULTIPLE
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_order_proposals_constrained AS
# MAGIC SELECT
# MAGIC     *,
# MAGIC     GREATEST(policy_qty, min_order_qty) AS qty_after_min,
# MAGIC     CASE
# MAGIC         WHEN max_order_qty > 0 THEN LEAST(GREATEST(policy_qty, min_order_qty), max_order_qty)
# MAGIC         ELSE GREATEST(policy_qty, min_order_qty)
# MAGIC     END AS qty_after_max,
# MAGIC     CASE
# MAGIC         WHEN order_multiple > 0 THEN
# MAGIC             CEIL(
# MAGIC               (CASE
# MAGIC                 WHEN max_order_qty > 0 THEN LEAST(GREATEST(policy_qty, min_order_qty), max_order_qty)
# MAGIC                 ELSE GREATEST(policy_qty, min_order_qty)
# MAGIC               END) / order_multiple
# MAGIC             ) * order_multiple
# MAGIC         ELSE
# MAGIC             (CASE
# MAGIC               WHEN max_order_qty > 0 THEN LEAST(GREATEST(policy_qty, min_order_qty), max_order_qty)
# MAGIC               ELSE GREATEST(policy_qty, min_order_qty)
# MAGIC             END)
# MAGIC     END AS final_qty
# MAGIC FROM v_order_proposals_policy
# MAGIC WHERE policy_qty > 0;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 8: DATE CALCS
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_order_proposals_dated AS
# MAGIC SELECT
# MAGIC     op.*,
# MAGIC     DATE_SUB(op.demand_date, op.lead_time_days) AS due_date,
# MAGIC     op.demand_date AS required_date,
# MAGIC     CURRENT_DATE() AS order_date,
# MAGIC     CASE WHEN op.replenishment_system IN ('Prod. Order', 'Assembly') THEN DATE_SUB(op.demand_date, op.lead_time_days) ELSE NULL END AS starting_date,
# MAGIC     CASE WHEN op.replenishment_system IN ('Prod. Order', 'Assembly') THEN op.demand_date ELSE NULL END AS ending_date
# MAGIC FROM v_order_proposals_constrained op
# MAGIC WHERE op.final_qty > 0;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- FINAL: fabric_requisition_line
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TABLE fabric_requisition_line_prod AS
# MAGIC WITH enrich AS (
# MAGIC   SELECT
# MAGIC       op.*,
# MAGIC 
# MAGIC       COALESCE(st.onhand_uom1, 0) AS onhand_uom1,
# MAGIC       COALESCE(st.onhand_uom2, 0) AS onhand_uom2,
# MAGIC       COALESCE(st.best_stock_location_code, '') AS best_stock_location_code,
# MAGIC 
# MAGIC       COALESCE(po.purchase_order_coming_uom1, 0) AS purchase_order_coming_uom1,
# MAGIC       po.earliest_po_receipt_date                AS earliest_po_receipt_date,
# MAGIC 
# MAGIC       CASE
# MAGIC         WHEN op.secondary_qty_per_base > 0 THEN (COALESCE(po.purchase_order_coming_uom1, 0) / op.secondary_qty_per_base)
# MAGIC         ELSE NULL
# MAGIC       END AS purchase_order_coming_uom2,
# MAGIC 
# MAGIC       -- need to buy uom1
# MAGIC       GREATEST(
# MAGIC         op.final_qty
# MAGIC         - COALESCE(st.onhand_uom1, 0)
# MAGIC         - COALESCE(po.purchase_order_coming_uom1, 0),
# MAGIC         0
# MAGIC       ) AS need_to_buy_qty_uom1_line
# MAGIC 
# MAGIC   FROM v_order_proposals_dated op
# MAGIC   LEFT JOIN v_stock_item_sum st
# MAGIC     ON op.item_no = st.item_no
# MAGIC   LEFT JOIN v_po_incoming_item po
# MAGIC     ON op.bc_company = po.bc_company
# MAGIC    AND op.item_no    = po.item_no
# MAGIC ),
# MAGIC 
# MAGIC with_uom2_calc AS (
# MAGIC   SELECT
# MAGIC       e.*,
# MAGIC 
# MAGIC       -- uom2 conversion method
# MAGIC       CASE
# MAGIC         WHEN e.secondary_qty_per_base > 0 THEN CEIL(e.need_to_buy_qty_uom1_line / e.secondary_qty_per_base)
# MAGIC         ELSE NULL
# MAGIC       END AS need_to_buy_qty_uom2,
# MAGIC 
# MAGIC       -- consumption demand uom2
# MAGIC       COALESCE(c.demand_quantity_uom2_consumption,0) AS demand_quantity_uom2,
# MAGIC 
# MAGIC       -- uom2 consumption-based method
# MAGIC       CASE
# MAGIC         WHEN COALESCE(c.demand_quantity_uom2_consumption,0) > 0 THEN
# MAGIC           GREATEST(
# MAGIC             COALESCE(c.demand_quantity_uom2_consumption,0)
# MAGIC             - COALESCE(e.onhand_uom2,0)
# MAGIC             - COALESCE(e.purchase_order_coming_uom2,0),
# MAGIC             0
# MAGIC           )
# MAGIC         ELSE
# MAGIC           CASE
# MAGIC             WHEN e.secondary_qty_per_base > 0 THEN CEIL(e.need_to_buy_qty_uom1_line / e.secondary_qty_per_base)
# MAGIC             ELSE NULL
# MAGIC           END
# MAGIC       END AS need_to_buy_qty_uom2_consumption
# MAGIC 
# MAGIC   FROM enrich e
# MAGIC   LEFT JOIN v_consumption_uom2 c
# MAGIC     ON e.item_no = c.item_no
# MAGIC ),
# MAGIC 
# MAGIC with_totals AS (
# MAGIC   SELECT
# MAGIC     u.*,
# MAGIC 
# MAGIC     MAX(u.need_to_buy_qty_uom1_line) OVER (
# MAGIC       PARTITION BY u.bc_company, u.item_no
# MAGIC     ) AS need_to_buy_qty_uom1
# MAGIC 
# MAGIC   FROM with_uom2_calc u
# MAGIC ),
# MAGIC 
# MAGIC flag_one AS (
# MAGIC   SELECT
# MAGIC     t.*,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY t.bc_company, t.item_no
# MAGIC       ORDER BY
# MAGIC         t.need_to_buy_qty_uom1_line DESC,
# MAGIC         t.demand_date ASC,
# MAGIC         t.demand_id ASC
# MAGIC     ) AS rn_for_flag
# MAGIC   FROM with_totals t
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC   -- =========================================================
# MAGIC   -- 1) ITEM
# MAGIC   -- =========================================================
# MAGIC   f.item_no                                      AS item_no,
# MAGIC   f.item_description                             AS description,
# MAGIC   f.description_2                                AS description_2,
# MAGIC   f.item_category_code                           AS item_category_code,
# MAGIC 
# MAGIC   -- =========================================================
# MAGIC   -- 2) DEMAND SOURCE
# MAGIC   -- =========================================================
# MAGIC   f.demand_type                                  AS demand_type,
# MAGIC   f.demand_subtype                               AS demand_subtype,
# MAGIC   f.demand_date                                  AS demand_date,
# MAGIC   f.sales_order_no                               AS sales_order_no,
# MAGIC   f.sales_line_no                                AS sales_order_line_no,
# MAGIC   f.prod_order_no                                AS prod_order_no,
# MAGIC   f.prod_order_line_no                           AS prod_order_line_no,
# MAGIC 
# MAGIC   -- =========================================================
# MAGIC   -- 3) UOM DEFINITIONS
# MAGIC   -- =========================================================
# MAGIC   f.base_uom                                     AS base_uom_uom1,
# MAGIC   f.secondary_uom                                AS secondary_uom_uom2,
# MAGIC   f.secondary_qty_per_base                       AS uom2_per_uom1,
# MAGIC 
# MAGIC   -- =========================================================
# MAGIC   -- 4) DEMAND QTY
# MAGIC   -- =========================================================
# MAGIC   f.demand_qty                                   AS demand_quantity_uom1,
# MAGIC   f.demand_qty_base                              AS demand_quantity_base_uom1,
# MAGIC   f.demand_quantity_uom2                         AS demand_quantity_uom2,
# MAGIC 
# MAGIC   -- =========================================================
# MAGIC   -- 5) SHORTAGE
# MAGIC   -- =========================================================
# MAGIC   f.shortage_qty                                 AS shortage_quantity_uom1,
# MAGIC   f.shortage_qty                                 AS shortage_quantity_base_uom1,
# MAGIC 
# MAGIC   -- =========================================================
# MAGIC   -- 6) STOCK (ON HAND)
# MAGIC   -- =========================================================
# MAGIC   f.onhand_uom1                                  AS onhand_quantity_uom1,
# MAGIC   f.onhand_uom2                                  AS onhand_quantity_uom2,
# MAGIC 
# MAGIC   -- =========================================================
# MAGIC   -- 7) PURCHASE ORDER INCOMING
# MAGIC   -- =========================================================
# MAGIC   f.purchase_order_coming_uom1                   AS po_incoming_quantity_uom1,
# MAGIC   f.purchase_order_coming_uom2                   AS po_incoming_quantity_uom2,
# MAGIC   f.earliest_po_receipt_date                     AS earliest_po_receipt_date,
# MAGIC 
# MAGIC   -- =========================================================
# MAGIC   -- 8) NEED TO BUY (UOM1)
# MAGIC   -- =========================================================
# MAGIC   f.need_to_buy_qty_uom1                         AS need_to_buy_quantity_uom1,
# MAGIC 
# MAGIC   -- =========================================================
# MAGIC   -- 9) NEED TO BUY (UOM2 - METHOD 1: conversion from uom1)
# MAGIC   -- =========================================================
# MAGIC   f.need_to_buy_qty_uom2                         AS need_to_buy_quantity_uom2_from_uom1,
# MAGIC 
# MAGIC   -- =========================================================
# MAGIC   -- 10) NEED TO BUY (UOM2 - METHOD 2: exp2 - con2 logic)
# MAGIC   -- =========================================================
# MAGIC   f.need_to_buy_qty_uom2_consumption             AS need_to_buy_quantity_uom2_from_consumption,
# MAGIC 
# MAGIC   -- =========================================================
# MAGIC   -- 11) FLAG
# MAGIC   -- =========================================================
# MAGIC   CASE
# MAGIC     WHEN COALESCE(f.location_code,'') = '' THEN 0
# MAGIC     WHEN f.rn_for_flag = 1 THEN 1
# MAGIC     ELSE 0
# MAGIC   END                                            AS is_max_need_to_buy,
# MAGIC 
# MAGIC   -- =========================================================
# MAGIC   -- 12) LOCATION
# MAGIC   -- =========================================================
# MAGIC   f.location_code                                AS location_code,
# MAGIC   f.best_stock_location_code                     AS best_stock_location_code,
# MAGIC 
# MAGIC   -- =========================================================
# MAGIC   -- 13) PLANNING DEBUG
# MAGIC   -- =========================================================
# MAGIC   f.available_before_this_demand                 AS available_before_demand_uom1,
# MAGIC   f.safety_stock                                 AS safety_stock_uom1,
# MAGIC   f.reorder_point                                AS reorder_point_uom1,
# MAGIC   f.reordering_policy                            AS reordering_policy,
# MAGIC   f.policy_qty                                   AS policy_quantity_uom1,
# MAGIC   f.qty_after_min                                AS qty_after_min_uom1,
# MAGIC   f.qty_after_max                                AS qty_after_max_uom1,
# MAGIC   f.final_qty                                    AS final_quantity_uom1,
# MAGIC 
# MAGIC   -- =========================================================
# MAGIC   -- 14) COST
# MAGIC   -- =========================================================
# MAGIC   f.default_vendor_no                            AS vendor_no,
# MAGIC   f.unit_cost                                    AS unit_cost,
# MAGIC   (f.final_qty * f.unit_cost)                    AS cost_amount,
# MAGIC 
# MAGIC   -- =========================================================
# MAGIC   -- 15) BC WORKSHEET COLUMNS
# MAGIC   -- =========================================================
# MAGIC   f.bc_company                                   AS bc_company,
# MAGIC   'PLANNING'                                     AS worksheet_template_name,
# MAGIC   'DEFAULT'                                      AS journal_batch_name,
# MAGIC 
# MAGIC   (ROW_NUMBER() OVER (
# MAGIC      PARTITION BY f.bc_company
# MAGIC      ORDER BY f.bc_company, f.item_no, f.location_code, f.due_date, f.demand_id
# MAGIC    ) * 10000)                                    AS line_no,
# MAGIC 
# MAGIC   'Item'                                         AS type,
# MAGIC   f.final_qty                                    AS worksheet_quantity_uom1,
# MAGIC   f.unit_cost                                    AS direct_unit_cost,
# MAGIC   f.due_date                                     AS due_date,
# MAGIC 
# MAGIC   f.shortcut_dim_1                               AS shortcut_dimension_1_code,
# MAGIC   f.shortcut_dim_2                               AS shortcut_dimension_2_code,
# MAGIC   f.dimension_set_id                             AS dimension_set_id,
# MAGIC 
# MAGIC   f.order_date                                   AS order_date,
# MAGIC   f.sell_to_customer_no                          AS sell_to_customer_no,
# MAGIC   f.variant_code                                 AS variant_code,
# MAGIC   f.qty_per_uom                                  AS qty_per_unit_of_measure,
# MAGIC   f.base_uom                                     AS unit_of_measure_code,
# MAGIC   f.final_qty                                    AS quantity_base,
# MAGIC 
# MAGIC   COALESCE(f.sales_order_no, f.prod_order_no)     AS demand_order_no,
# MAGIC   f.demand_subtype                               AS status,
# MAGIC   f.planning_level                               AS level,
# MAGIC   f.routing_no                                   AS routing_no,
# MAGIC   f.gen_prod_posting_group                       AS gen_prod_posting_group,
# MAGIC   f.low_level_code                               AS low_level_code,
# MAGIC   f.final_qty                                    AS remaining_quantity,
# MAGIC   f.scrap_pct                                    AS scrap_pct,
# MAGIC   f.starting_date                                AS starting_date,
# MAGIC   f.ending_date                                  AS ending_date,
# MAGIC   f.production_bom_no                            AS production_bom_no,
# MAGIC   f.indirect_cost_pct                            AS indirect_cost_pct,
# MAGIC   f.overhead_rate                                AS overhead_rate,
# MAGIC   f.replenishment_system                         AS replenishment_system,
# MAGIC 
# MAGIC   -- =========================================================
# MAGIC   -- FINAL ROUNDED VALUES
# MAGIC   -- =========================================================
# MAGIC   CEIL(f.need_to_buy_qty_uom1) AS final_uom1,
# MAGIC 
# MAGIC   CEIL(
# MAGIC      CASE 
# MAGIC         WHEN COALESCE(f.need_to_buy_qty_uom2_consumption,0) > 0 
# MAGIC         THEN f.need_to_buy_qty_uom2_consumption
# MAGIC         ELSE f.need_to_buy_qty_uom2
# MAGIC      END
# MAGIC   ) AS final_uom2
# MAGIC 
# MAGIC FROM flag_one f;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # MRP

# CELL ********************

# MAGIC %%sql
# MAGIC -- ============================================================
# MAGIC -- MRP / Fabric Requisition Line (BC-ALIGNED) — V5
# MAGIC --
# MAGIC -- V5 ARCHITECTURAL CHANGES (from V4 review findings):
# MAGIC --
# MAGIC -- 1. TIME-BUCKET LOT-FOR-LOT (Bug #1 fix)
# MAGIC --    รวม demand ใน 7-day bucket (Mon-Sun) เป็น 1 supply proposal
# MAGIC --    → 1 row ต่อ (item + variant + bucket_week) ไม่ใช่ต่อ demand line
# MAGIC --    → ไม่มี double-counting อีกต่อไป
# MAGIC --
# MAGIC -- 2. ITEM-LEVEL STOCK POOLING (Q1 answer)
# MAGIC --    Demand ทุก location (รวม CS1, CAD, AUTO, ...) แชร์ stock pool รวม
# MAGIC --    ที่รวมจาก 31 valid stock locations
# MAGIC --    → grain ของ planning = (bc_company + item_no + variant_code + bucket_week)
# MAGIC --
# MAGIC -- 3. TWO OUTPUT TABLES (Q2 answer)
# MAGIC --    fabric_requisition_line        = bucket-level (MRP proposal grain)
# MAGIC --    fabric_requisition_line_detail = demand-line-level (drill-down trace)
# MAGIC --
# MAGIC -- 4. LOCATION HANDLING (Q1 answer)
# MAGIC --    v_valid_locations = 31 stock locations
# MAGIC --    Filter เฉพาะ stock + supply views, ไม่ filter demand
# MAGIC --
# MAGIC -- RETAINED from V4:
# MAGIC --    - Location filter บน stock/supply views (Bug #3)
# MAGIC --    - GREATEST(safety_stock, reorder_point) ใน shortage calc (Bug #2)
# MAGIC --    - Supply-first same-day sort (Concern #6)
# MAGIC --    - Single source of truth via ILE (Concern #4)
# MAGIC --
# MAGIC -- RETAINED from V3:
# MAGIC --    - UOM-aware rounding
# MAGIC --    - Alert flag ใช้ raw qty
# MAGIC --    - Min/Max/Multiple ordering
# MAGIC --
# MAGIC -- DEFERRED (ทำใน V6):
# MAGIC --    - Overflow Level check
# MAGIC --    - Rescheduling Period / Dampener Period
# MAGIC -- ============================================================
# MAGIC 
# MAGIC USE Silver_Planning_Lakehouse.dbo;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CONFIG: 31 valid STOCK locations
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_valid_locations AS
# MAGIC SELECT location_code FROM VALUES
# MAGIC   ('BAGGING'),('CASTING'),('CONSUME'),('CST_CUT'),('CST_ROOM'),('CZ-SYNT'),
# MAGIC   ('DEBEERS'),('DIA-LAB'),('DIA-NAT'),('EQUIP'),('FG-NO-PO'),('FINDINGS'),
# MAGIC   ('FIN-GOODS'),('GEMS'),('KIMAI'),('MATERIAL'),('OBSOLETE'),('OTHERS-MAT'),
# MAGIC   ('PACKAGING'),('PEARLS'),('PLATING'),('POMELATO'),('PRE ALLOY'),('RETURNS'),
# MAGIC   ('RUB MOLD'),('SEMI-F'),('SORTING'),('STONE-CUT'),('STR'),('TOOLS'),('WAX ROOM')
# MAGIC AS t(location_code);
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 0A: STOCK (gold_stock_item) — display only
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_stock_item_sum AS
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     item_no,
# MAGIC     COALESCE(location,'')       AS stock_location_code,
# MAGIC     COALESCE(rem_uom1, 0)       AS rem_uom1,
# MAGIC     COALESCE(rem_uom2, 0)       AS rem_uom2
# MAGIC   FROM gold_stock_item
# MAGIC ),
# MAGIC sum_per_item AS (
# MAGIC   SELECT
# MAGIC     item_no,
# MAGIC     SUM(rem_uom1) AS onhand_uom1_display,
# MAGIC     SUM(rem_uom2) AS onhand_uom2_display
# MAGIC   FROM base
# MAGIC   GROUP BY item_no
# MAGIC ),
# MAGIC best_loc AS (
# MAGIC   SELECT
# MAGIC     item_no,
# MAGIC     stock_location_code AS best_stock_location_code,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY item_no
# MAGIC       ORDER BY rem_uom2 DESC, stock_location_code
# MAGIC     ) AS rn
# MAGIC   FROM base
# MAGIC )
# MAGIC SELECT
# MAGIC   s.item_no,
# MAGIC   s.onhand_uom1_display,
# MAGIC   s.onhand_uom2_display,
# MAGIC   COALESCE(b.best_stock_location_code,'') AS best_stock_location_code
# MAGIC FROM sum_per_item s
# MAGIC LEFT JOIN (SELECT item_no, best_stock_location_code FROM best_loc WHERE rn = 1) b
# MAGIC   ON s.item_no = b.item_no;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 0B: GOLD CONSUMPTION (UOM2 demand)
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_consumption_uom2 AS
# MAGIC SELECT
# MAGIC     ComponentItemNo AS item_no,
# MAGIC     SUM(
# MAGIC       GREATEST(
# MAGIC         COALESCE(exp2,0) + COALESCE(con2,0),
# MAGIC         0
# MAGIC       )
# MAGIC     ) AS demand_quantity_uom2_consumption
# MAGIC FROM gold_consumption
# MAGIC WHERE Status IN ('Planned','Firm Planned','Released')
# MAGIC GROUP BY ComponentItemNo;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 1A: ONHAND (ILE) — filter 31 stock locations
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_onhand_ile AS
# MAGIC SELECT
# MAGIC     ile.`BC Company`                 AS bc_company,
# MAGIC     ile.`Item No.`                   AS item_no,
# MAGIC     COALESCE(ile.`Location Code`,'') AS location_code,
# MAGIC     SUM(ile.`Remaining Quantity`)    AS onhand_qty,
# MAGIC     MAX(ile.`Posting Date`)          AS last_movement_date
# MAGIC FROM item_ledger_entry ile
# MAGIC INNER JOIN v_valid_locations vl
# MAGIC   ON ile.`Location Code` = vl.location_code
# MAGIC WHERE ile.`Open` = 1
# MAGIC GROUP BY ile.`BC Company`, ile.`Item No.`, COALESCE(ile.`Location Code`,'');
# MAGIC 
# MAGIC -- 🔧 V5: Item-level onhand pool (Q1 answer)
# MAGIC --   รวม onhand ทุก valid location → ใช้เป็น starting inventory สำหรับ timeline
# MAGIC CREATE OR REPLACE TEMP VIEW v_onhand_item_pool AS
# MAGIC SELECT
# MAGIC     bc_company,
# MAGIC     item_no,
# MAGIC     SUM(onhand_qty) AS onhand_pool_qty
# MAGIC FROM v_onhand_ile
# MAGIC GROUP BY bc_company, item_no;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 1B: SALES DEMAND (no location filter)
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_demand_sales AS
# MAGIC SELECT
# MAGIC     `BC Company` AS bc_company,
# MAGIC     `No.`        AS item_no,
# MAGIC     COALESCE(`Location Code`, '') AS location_code,
# MAGIC     COALESCE(`Variant Code`, '')  AS variant_code,
# MAGIC     CAST(COALESCE(`Shipment Date`,`Requested Delivery Date`,`Promised Delivery Date`) AS DATE) AS demand_date,
# MAGIC 
# MAGIC     `Outstanding Quantity`      AS demand_qty,
# MAGIC     `Outstanding Qty. (Base)`   AS demand_qty_base,
# MAGIC 
# MAGIC     `Document No.`              AS sales_order_no,
# MAGIC     `Line No.`                  AS sales_line_no,
# MAGIC     `Sell-to Customer No.`      AS sell_to_customer_no,
# MAGIC 
# MAGIC     `Dimension Set ID`          AS dimension_set_id,
# MAGIC     `Shortcut Dimension 1 Code` AS shortcut_dim_1,
# MAGIC     `Shortcut Dimension 2 Code` AS shortcut_dim_2,
# MAGIC     `Unit of Measure Code`      AS unit_of_measure_code,
# MAGIC     `Qty. per Unit of Measure`  AS qty_per_uom,
# MAGIC 
# MAGIC     'Sales'                         AS demand_type,
# MAGIC     CAST(`Document Type` AS STRING) AS demand_subtype,
# MAGIC 
# MAGIC     CONCAT(`BC Company`, '-SALES-', `Document No.`, '-', CAST(`Line No.` AS STRING)) AS demand_id,
# MAGIC 
# MAGIC     CAST(NULL AS STRING) AS prod_order_no,
# MAGIC     CAST(NULL AS INT)    AS prod_order_line_no,
# MAGIC     CAST(NULL AS STRING) AS original_item_no,
# MAGIC     0                    AS planning_level
# MAGIC FROM sales_line
# MAGIC WHERE `Type` = 'Item'
# MAGIC   AND `Outstanding Quantity` > 0
# MAGIC   AND COALESCE(`Shipment Date`, `Requested Delivery Date`, `Promised Delivery Date`) IS NOT NULL;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 1C: PRODUCTION COMPONENT DEMAND (no location filter)
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_demand_production AS
# MAGIC SELECT
# MAGIC     `BC Company` AS bc_company,
# MAGIC     `Item No.`   AS item_no,
# MAGIC     COALESCE(`Location Code`, '') AS location_code,
# MAGIC     COALESCE(`Variant Code`, '')  AS variant_code,
# MAGIC 
# MAGIC     CAST(MIN(`Due Date`) AS DATE) AS demand_date,
# MAGIC 
# MAGIC     SUM(`Remaining Quantity`)    AS demand_qty,
# MAGIC     SUM(`Remaining Qty. (Base)`) AS demand_qty_base,
# MAGIC 
# MAGIC     'Production' AS demand_type,
# MAGIC     'Released'   AS demand_subtype,
# MAGIC 
# MAGIC     CONCAT(
# MAGIC       `BC Company`, '-PROD-',
# MAGIC       `Item No.`, '-',
# MAGIC       COALESCE(`Location Code`, ''), '-',
# MAGIC       COALESCE(`Variant Code`, ''), '-',
# MAGIC       CAST(MIN(`Due Date`) AS STRING)
# MAGIC     ) AS demand_id,
# MAGIC 
# MAGIC     CAST(NULL AS STRING) AS prod_order_no,
# MAGIC     CAST(NULL AS INT)    AS prod_order_line_no,
# MAGIC     CAST(NULL AS STRING) AS original_item_no,
# MAGIC     1                    AS planning_level,
# MAGIC 
# MAGIC     CAST(NULL AS INT)            AS dimension_set_id,
# MAGIC     CAST(NULL AS STRING)         AS shortcut_dim_1,
# MAGIC     CAST(NULL AS STRING)         AS shortcut_dim_2,
# MAGIC     CAST(NULL AS STRING)         AS unit_of_measure_code,
# MAGIC     CAST(NULL AS DECIMAL(38,20)) AS qty_per_uom
# MAGIC FROM prod_order_component
# MAGIC WHERE `Status` IN ('Planned','Firm Planned','Released')
# MAGIC   AND COALESCE(`Remaining Quantity`,0) > 0
# MAGIC GROUP BY
# MAGIC     `BC Company`,
# MAGIC     `Item No.`,
# MAGIC     COALESCE(`Location Code`, ''),
# MAGIC     COALESCE(`Variant Code`, '');
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 1D: COMBINE DEMAND
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_demand_all AS
# MAGIC SELECT
# MAGIC     bc_company, item_no, location_code, variant_code,
# MAGIC     demand_date, demand_id,
# MAGIC     demand_qty, demand_qty_base,
# MAGIC     demand_type, demand_subtype,
# MAGIC     sales_order_no, sales_line_no, sell_to_customer_no,
# MAGIC     prod_order_no, prod_order_line_no, original_item_no, planning_level,
# MAGIC     dimension_set_id, shortcut_dim_1, shortcut_dim_2,
# MAGIC     unit_of_measure_code, qty_per_uom
# MAGIC FROM v_demand_sales
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     bc_company, item_no, location_code, variant_code,
# MAGIC     demand_date, demand_id,
# MAGIC     demand_qty, demand_qty_base,
# MAGIC     demand_type, demand_subtype,
# MAGIC     CAST(NULL AS STRING) AS sales_order_no,
# MAGIC     CAST(NULL AS INT)    AS sales_line_no,
# MAGIC     CAST(NULL AS STRING) AS sell_to_customer_no,
# MAGIC     prod_order_no, prod_order_line_no, original_item_no, planning_level,
# MAGIC     dimension_set_id, shortcut_dim_1, shortcut_dim_2,
# MAGIC     unit_of_measure_code, qty_per_uom
# MAGIC FROM v_demand_production;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 1E: 🔧 V5 NEW — BUCKET DEMAND BY 7-DAY WINDOW
# MAGIC --   anchor_date = 2020-01-06 (Monday) — ใช้ formula FLOOR(DATEDIFF/7)
# MAGIC --   ดังนั้น bucket_week = จำนวนสัปดาห์นับจาก Mon 2020-01-06
# MAGIC --   สัปดาห์เริ่มวันจันทร์เสมอ
# MAGIC --   🔧 Pool ด้วย (item + variant) เท่านั้น — ข้าม location (Q1 answer)
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_demand_bucketed AS
# MAGIC SELECT
# MAGIC     bc_company,
# MAGIC     item_no,
# MAGIC     variant_code,
# MAGIC     FLOOR(DATEDIFF(demand_date, DATE'2020-01-06') / 7) AS bucket_week,
# MAGIC     MIN(demand_date) AS bucket_earliest_date,
# MAGIC     MAX(demand_date) AS bucket_latest_date,
# MAGIC     SUM(demand_qty)      AS bucket_demand_qty_uom1,
# MAGIC     SUM(demand_qty_base) AS bucket_demand_qty_base,
# MAGIC     COUNT(*)             AS demand_line_count_in_bucket,
# MAGIC     MIN(planning_level)  AS min_planning_level
# MAGIC FROM v_demand_all
# MAGIC GROUP BY bc_company, item_no, variant_code,
# MAGIC          FLOOR(DATEDIFF(demand_date, DATE'2020-01-06') / 7);
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 2: PURCHASE INCOMING — filter 31 locations
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_po_incoming_item AS
# MAGIC SELECT
# MAGIC     pl.`BC Company` AS bc_company,
# MAGIC     pl.`No.`        AS item_no,
# MAGIC     MIN(COALESCE(pl.`Expected Receipt Date`, pl.`Promised Receipt Date`, pl.`Requested Receipt Date`)) AS earliest_po_receipt_date,
# MAGIC     SUM(COALESCE(pl.`Outstanding Quantity`,0))       AS purchase_order_coming_uom1,
# MAGIC     SUM(COALESCE(pl.`Outstanding Units_DU_TSL`,0))   AS purchase_order_coming_uom2
# MAGIC FROM purchase_line pl
# MAGIC INNER JOIN v_valid_locations vl
# MAGIC   ON COALESCE(pl.`Location Code`,'') = vl.location_code
# MAGIC WHERE pl.`Type` = 'Item'
# MAGIC   AND COALESCE(pl.`Outstanding Quantity`,0) > 0
# MAGIC GROUP BY pl.`BC Company`, pl.`No.`;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 2B: SUPPLY EVENTS — pool per item+variant, bucketed
# MAGIC --   🔧 V5: aggregate ที่ bucket level ตั้งแต่แรก (align กับ demand grain)
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_supply_po_bucketed AS
# MAGIC SELECT
# MAGIC     pl.`BC Company` AS bc_company,
# MAGIC     pl.`No.`        AS item_no,
# MAGIC     COALESCE(pl.`Variant Code`,'') AS variant_code,
# MAGIC     FLOOR(DATEDIFF(
# MAGIC       CAST(COALESCE(pl.`Expected Receipt Date`, pl.`Promised Receipt Date`, pl.`Requested Receipt Date`) AS DATE),
# MAGIC       DATE'2020-01-06'
# MAGIC     ) / 7) AS bucket_week,
# MAGIC     SUM(COALESCE(pl.`Outstanding Quantity`,0)) AS supply_qty_base
# MAGIC FROM purchase_line pl
# MAGIC INNER JOIN v_valid_locations vl
# MAGIC   ON COALESCE(pl.`Location Code`,'') = vl.location_code
# MAGIC WHERE pl.`Type` = 'Item'
# MAGIC   AND COALESCE(pl.`Outstanding Quantity`,0) > 0
# MAGIC   AND COALESCE(pl.`Expected Receipt Date`, pl.`Promised Receipt Date`, pl.`Requested Receipt Date`) IS NOT NULL
# MAGIC GROUP BY
# MAGIC     pl.`BC Company`, pl.`No.`, COALESCE(pl.`Variant Code`,''),
# MAGIC     FLOOR(DATEDIFF(
# MAGIC       CAST(COALESCE(pl.`Expected Receipt Date`, pl.`Promised Receipt Date`, pl.`Requested Receipt Date`) AS DATE),
# MAGIC       DATE'2020-01-06'
# MAGIC     ) / 7);
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW v_supply_prod_bucketed AS
# MAGIC SELECT
# MAGIC     pol.`BC Company` AS bc_company,
# MAGIC     pol.`Item No.`   AS item_no,
# MAGIC     COALESCE(pol.`Variant Code`,'') AS variant_code,
# MAGIC     FLOOR(DATEDIFF(CAST(pol.`Due Date` AS DATE), DATE'2020-01-06') / 7) AS bucket_week,
# MAGIC     SUM(pol.`Remaining Qty. (Base)`) AS supply_qty_base
# MAGIC FROM prod_order_line pol
# MAGIC INNER JOIN v_valid_locations vl
# MAGIC   ON COALESCE(pol.`Location Code`,'') = vl.location_code
# MAGIC WHERE pol.`Remaining Quantity` > 0
# MAGIC   AND pol.`Status` IN ('Planned', 'Firm Planned', 'Released')
# MAGIC GROUP BY pol.`BC Company`, pol.`Item No.`, COALESCE(pol.`Variant Code`,''),
# MAGIC          FLOOR(DATEDIFF(CAST(pol.`Due Date` AS DATE), DATE'2020-01-06') / 7);
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW v_supply_bucketed AS
# MAGIC SELECT
# MAGIC     bc_company, item_no, variant_code, bucket_week,
# MAGIC     SUM(supply_qty_base) AS supply_qty_base
# MAGIC FROM (
# MAGIC   SELECT * FROM v_supply_po_bucketed
# MAGIC   UNION ALL
# MAGIC   SELECT * FROM v_supply_prod_bucketed
# MAGIC ) u
# MAGIC GROUP BY bc_company, item_no, variant_code, bucket_week;
# MAGIC 
# MAGIC -- Item-level supply total (for display / info only)
# MAGIC CREATE OR REPLACE TEMP VIEW v_supply_item_total AS
# MAGIC SELECT
# MAGIC     bc_company, item_no,
# MAGIC     SUM(supply_qty_base) AS po_incoming_total
# MAGIC FROM v_supply_bucketed
# MAGIC GROUP BY bc_company, item_no;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 3: ITEM PLANNING PARAMETERS
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_item_planning_full AS
# MAGIC SELECT
# MAGIC     i.`BC Company` AS bc_company,
# MAGIC     i.`No.`        AS item_no,
# MAGIC 
# MAGIC     CAST(COALESCE(i.`Reordering Policy`, '') AS STRING) AS reordering_policy,
# MAGIC     COALESCE(i.`Reorder Point`, 0)          AS reorder_point,
# MAGIC     COALESCE(i.`Reorder Quantity`, 0)       AS reorder_quantity,
# MAGIC     COALESCE(i.`Maximum Inventory`, 0)      AS maximum_inventory,
# MAGIC     COALESCE(i.`Lot Size`, 0)               AS lot_size,
# MAGIC     COALESCE(i.`Minimum Order Quantity`, 0) AS min_order_qty,
# MAGIC     COALESCE(i.`Maximum Order Quantity`, 0) AS max_order_qty,
# MAGIC     COALESCE(i.`Order Multiple`, 0)         AS order_multiple,
# MAGIC     COALESCE(i.`Safety Stock Quantity`, 0)  AS safety_stock,
# MAGIC 
# MAGIC     i.`Lead Time Calculation` AS lead_time_calc,
# MAGIC     CASE
# MAGIC         WHEN RIGHT(TRIM(i.`Lead Time Calculation`), 1) = 'D' THEN CAST(REGEXP_EXTRACT(i.`Lead Time Calculation`, '([0-9]+)', 1) AS INT)
# MAGIC         WHEN RIGHT(TRIM(i.`Lead Time Calculation`), 1) = 'W' THEN CAST(REGEXP_EXTRACT(i.`Lead Time Calculation`, '([0-9]+)', 1) AS INT) * 7
# MAGIC         WHEN RIGHT(TRIM(i.`Lead Time Calculation`), 1) = 'M' THEN CAST(REGEXP_EXTRACT(i.`Lead Time Calculation`, '([0-9]+)', 1) AS INT) * 30
# MAGIC         WHEN RIGHT(TRIM(i.`Lead Time Calculation`), 1) = 'Y' THEN CAST(REGEXP_EXTRACT(i.`Lead Time Calculation`, '([0-9]+)', 1) AS INT) * 365
# MAGIC         ELSE 0
# MAGIC     END AS lead_time_days,
# MAGIC 
# MAGIC     CAST(i.`Replenishment System` AS STRING) AS replenishment_system,
# MAGIC     i.`Vendor No.` AS default_vendor_no,
# MAGIC 
# MAGIC     CAST(i.`Description` AS STRING)          AS item_description,
# MAGIC     CAST(i.`Description 2` AS STRING)        AS description_2,
# MAGIC     CAST(i.`Base Unit of Measure` AS STRING) AS base_uom,
# MAGIC 
# MAGIC     COALESCE(i.`Unit Cost`, 0)       AS unit_cost,
# MAGIC     COALESCE(i.`Indirect Cost %`, 0) AS indirect_cost_pct,
# MAGIC     COALESCE(i.`Overhead Rate`, 0)   AS overhead_rate,
# MAGIC     COALESCE(i.`Scrap %`, 0)         AS scrap_pct,
# MAGIC 
# MAGIC     CAST(i.`Production BOM No.` AS STRING) AS production_bom_no,
# MAGIC     CAST(i.`Routing No.` AS STRING)        AS routing_no,
# MAGIC     CAST(i.`Low-Level Code` AS STRING)     AS low_level_code,
# MAGIC 
# MAGIC     CAST(i.`Item Category Code` AS STRING)       AS item_category_code,
# MAGIC     CAST(i.`Gen. Prod. Posting Group` AS STRING) AS gen_prod_posting_group,
# MAGIC 
# MAGIC     COALESCE(u_pc.`Code`, u_cm.`Code`) AS secondary_uom,
# MAGIC     COALESCE(u_pc.`Qty. per Unit of Measure`, u_cm.`Qty. per Unit of Measure`, 0) AS secondary_qty_per_base
# MAGIC FROM item i
# MAGIC LEFT JOIN item_unit_of_measure u_pc
# MAGIC   ON i.`BC Company` = u_pc.`BC Company`
# MAGIC  AND i.`No.`        = u_pc.`Item No.`
# MAGIC  AND u_pc.`Code` IN ('PC','PCS')
# MAGIC LEFT JOIN item_unit_of_measure u_cm
# MAGIC   ON i.`BC Company` = u_cm.`BC Company`
# MAGIC  AND i.`No.`        = u_cm.`Item No.`
# MAGIC  AND u_cm.`Code` = 'CM';
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 4: 🔧 V5 BUCKET-LEVEL TIMELINE AVAILABILITY
# MAGIC --   partition: (bc_company + item_no + variant_code)   — pool locations
# MAGIC --   order:     bucket_week
# MAGIC --   starting:  item-level onhand pool
# MAGIC --   running:   onhand + cumulative supply − cumulative demand (both up to, not including, current bucket)
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_bucket_events AS
# MAGIC -- Demand events (negative impact)
# MAGIC SELECT
# MAGIC     bc_company, item_no, variant_code, bucket_week,
# MAGIC     -bucket_demand_qty_base AS demand_qty_base,
# MAGIC     0                       AS supply_qty_base,
# MAGIC     bucket_earliest_date,
# MAGIC     bucket_latest_date,
# MAGIC     bucket_demand_qty_uom1,
# MAGIC     demand_line_count_in_bucket,
# MAGIC     min_planning_level
# MAGIC FROM v_demand_bucketed
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC -- Supply events (positive impact) — align with demand grain
# MAGIC SELECT
# MAGIC     bc_company, item_no, variant_code, bucket_week,
# MAGIC     0                                                     AS demand_qty_base,
# MAGIC     supply_qty_base                                       AS supply_qty_base,
# MAGIC     CAST(NULL AS DATE)                                    AS bucket_earliest_date,
# MAGIC     CAST(NULL AS DATE)                                    AS bucket_latest_date,
# MAGIC     0                                                     AS bucket_demand_qty_uom1,
# MAGIC     0                                                     AS demand_line_count_in_bucket,
# MAGIC     CAST(NULL AS INT)                                     AS min_planning_level
# MAGIC FROM v_supply_bucketed;
# MAGIC 
# MAGIC -- Aggregate per bucket (demand + supply merged)
# MAGIC CREATE OR REPLACE TEMP VIEW v_bucket_netted AS
# MAGIC SELECT
# MAGIC     bc_company, item_no, variant_code, bucket_week,
# MAGIC     SUM(demand_qty_base) AS demand_qty_base,    -- already negative
# MAGIC     SUM(supply_qty_base) AS supply_qty_base,
# MAGIC     MAX(bucket_earliest_date) AS bucket_earliest_date,
# MAGIC     MAX(bucket_latest_date)   AS bucket_latest_date,
# MAGIC     SUM(bucket_demand_qty_uom1) AS bucket_demand_qty_uom1,
# MAGIC     SUM(demand_line_count_in_bucket) AS demand_line_count_in_bucket,
# MAGIC     MIN(min_planning_level) AS min_planning_level
# MAGIC FROM v_bucket_events
# MAGIC GROUP BY bc_company, item_no, variant_code, bucket_week;
# MAGIC 
# MAGIC -- Running availability at start of each bucket
# MAGIC CREATE OR REPLACE TEMP VIEW v_bucket_availability AS
# MAGIC SELECT
# MAGIC     b.*,
# MAGIC     COALESCE(oh.onhand_pool_qty, 0) AS onhand_pool_qty,
# MAGIC     -- Availability BEFORE this bucket = onhand + cum supply − cum demand (both cumulative UP TO but NOT including this bucket)
# MAGIC     COALESCE(oh.onhand_pool_qty, 0)
# MAGIC     + COALESCE(
# MAGIC         SUM(b.supply_qty_base) OVER (
# MAGIC           PARTITION BY b.bc_company, b.item_no, b.variant_code
# MAGIC           ORDER BY b.bucket_week
# MAGIC           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
# MAGIC         ), 0)
# MAGIC     + COALESCE(
# MAGIC         SUM(b.demand_qty_base) OVER (   -- already negative
# MAGIC           PARTITION BY b.bc_company, b.item_no, b.variant_code
# MAGIC           ORDER BY b.bucket_week
# MAGIC           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
# MAGIC         ), 0) AS available_before_bucket
# MAGIC FROM v_bucket_netted b
# MAGIC LEFT JOIN v_onhand_item_pool oh
# MAGIC   ON b.bc_company = oh.bc_company AND b.item_no = oh.item_no
# MAGIC WHERE b.demand_qty_base < 0;   -- 🔧 propose supply เฉพาะ bucket ที่มี demand
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 5: SHORTAGE PER BUCKET
# MAGIC --   shortage_qty = |demand| + GREATEST(safety_stock, reorder_point) − (available_before + supply_in_bucket)
# MAGIC --   ถ้า supply ใน bucket เดียวกันครอบคลุมแล้ว → shortage = 0
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_bucket_shortage AS
# MAGIC SELECT
# MAGIC     ba.*,
# MAGIC     ip.safety_stock, ip.reorder_point, ip.reorder_quantity,
# MAGIC     ip.maximum_inventory, ip.lot_size, ip.min_order_qty, ip.max_order_qty,
# MAGIC     ip.order_multiple, ip.reordering_policy, ip.replenishment_system,
# MAGIC     ip.default_vendor_no, ip.item_description, ip.description_2,
# MAGIC     ip.unit_cost, ip.production_bom_no, ip.routing_no, ip.lead_time_days,
# MAGIC     ip.item_category_code, ip.gen_prod_posting_group, ip.low_level_code,
# MAGIC     ip.indirect_cost_pct, ip.overhead_rate, ip.scrap_pct,
# MAGIC     ip.base_uom, ip.secondary_uom, ip.secondary_qty_per_base,
# MAGIC 
# MAGIC     -- Absolute demand (flip sign)
# MAGIC     ABS(ba.demand_qty_base) AS abs_demand_qty_base,
# MAGIC 
# MAGIC     -- Available AFTER supply in this bucket but BEFORE consuming demand
# MAGIC     ba.available_before_bucket + ba.supply_qty_base AS available_with_bucket_supply,
# MAGIC 
# MAGIC     -- Shortage: need enough to cover demand + safety/reorder floor
# MAGIC     GREATEST(
# MAGIC       (ABS(ba.demand_qty_base) + GREATEST(COALESCE(ip.safety_stock, 0), COALESCE(ip.reorder_point, 0)))
# MAGIC       - (ba.available_before_bucket + ba.supply_qty_base),
# MAGIC       0
# MAGIC     ) AS shortage_qty,
# MAGIC 
# MAGIC     -- Projected availability after consuming demand (for reorder-point check)
# MAGIC     (ba.available_before_bucket + ba.supply_qty_base + ba.demand_qty_base) AS projected_after_bucket
# MAGIC FROM v_bucket_availability ba
# MAGIC LEFT JOIN v_item_planning_full ip
# MAGIC   ON ba.bc_company = ip.bc_company
# MAGIC  AND ba.item_no    = ip.item_no;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 6: APPLY REORDERING POLICY
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_bucket_policy_qty AS
# MAGIC SELECT
# MAGIC     bs.*,
# MAGIC     CASE
# MAGIC         WHEN COALESCE(bs.reordering_policy, '') IN ('', 'Order', 'Lot-for-Lot')
# MAGIC           THEN bs.shortage_qty
# MAGIC 
# MAGIC         WHEN bs.reordering_policy = 'Maximum Qty'
# MAGIC           THEN GREATEST(bs.maximum_inventory - bs.projected_after_bucket, 0)
# MAGIC 
# MAGIC         WHEN bs.reordering_policy IN ('Fixed Reorder Qty', 'Fixed Reorder Qty.') THEN
# MAGIC             CASE WHEN bs.projected_after_bucket < bs.reorder_point
# MAGIC                  THEN GREATEST(bs.reorder_quantity, bs.shortage_qty)
# MAGIC                  ELSE 0
# MAGIC             END
# MAGIC 
# MAGIC         ELSE bs.shortage_qty
# MAGIC     END AS policy_qty
# MAGIC FROM v_bucket_shortage bs
# MAGIC WHERE bs.shortage_qty > 0
# MAGIC    OR bs.projected_after_bucket < GREATEST(COALESCE(bs.safety_stock,0), COALESCE(bs.reorder_point,0));
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 7: APPLY MIN/MAX/MULTIPLE ORDER MODIFIERS
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_bucket_constrained AS
# MAGIC SELECT
# MAGIC     *,
# MAGIC     GREATEST(policy_qty, COALESCE(min_order_qty, 0)) AS qty_after_min,
# MAGIC     CASE
# MAGIC         WHEN COALESCE(max_order_qty, 0) > 0 THEN LEAST(GREATEST(policy_qty, COALESCE(min_order_qty, 0)), max_order_qty)
# MAGIC         ELSE GREATEST(policy_qty, COALESCE(min_order_qty, 0))
# MAGIC     END AS qty_after_max,
# MAGIC     CASE
# MAGIC         WHEN COALESCE(order_multiple, 0) > 0 THEN
# MAGIC             CEIL((CASE
# MAGIC                 WHEN COALESCE(max_order_qty, 0) > 0 THEN LEAST(GREATEST(policy_qty, COALESCE(min_order_qty, 0)), max_order_qty)
# MAGIC                 ELSE GREATEST(policy_qty, COALESCE(min_order_qty, 0))
# MAGIC             END) / order_multiple) * order_multiple
# MAGIC         ELSE
# MAGIC             (CASE
# MAGIC                 WHEN COALESCE(max_order_qty, 0) > 0 THEN LEAST(GREATEST(policy_qty, COALESCE(min_order_qty, 0)), max_order_qty)
# MAGIC                 ELSE GREATEST(policy_qty, COALESCE(min_order_qty, 0))
# MAGIC             END)
# MAGIC     END AS final_qty
# MAGIC FROM v_bucket_policy_qty
# MAGIC WHERE policy_qty > 0;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- STEP 8: DATE CALCS (based on bucket_earliest_date)
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TEMP VIEW v_bucket_dated AS
# MAGIC SELECT
# MAGIC     op.*,
# MAGIC     -- required_date = earliest demand in bucket
# MAGIC     op.bucket_earliest_date AS required_date,
# MAGIC 
# MAGIC     -- due_date = required_date − lead_time (clamp to reasonable range)
# MAGIC     CASE
# MAGIC         WHEN op.bucket_earliest_date IS NULL THEN NULL
# MAGIC         WHEN op.lead_time_days IS NULL THEN op.bucket_earliest_date
# MAGIC         WHEN date_add(op.bucket_earliest_date, -CAST(op.lead_time_days AS INT)) < DATE'1900-01-01' THEN NULL
# MAGIC         WHEN date_add(op.bucket_earliest_date, -CAST(op.lead_time_days AS INT)) > DATE'2100-12-31' THEN NULL
# MAGIC         ELSE date_add(op.bucket_earliest_date, -CAST(op.lead_time_days AS INT))
# MAGIC     END AS due_date,
# MAGIC 
# MAGIC     current_date() AS order_date,
# MAGIC 
# MAGIC     CASE WHEN op.replenishment_system IN ('Prod. Order','Assembly')
# MAGIC          THEN CASE
# MAGIC                 WHEN op.bucket_earliest_date IS NULL THEN NULL
# MAGIC                 WHEN op.lead_time_days IS NULL THEN op.bucket_earliest_date
# MAGIC                 WHEN date_add(op.bucket_earliest_date, -CAST(op.lead_time_days AS INT)) < DATE'1900-01-01' THEN NULL
# MAGIC                 WHEN date_add(op.bucket_earliest_date, -CAST(op.lead_time_days AS INT)) > DATE'2100-12-31' THEN NULL
# MAGIC                 ELSE date_add(op.bucket_earliest_date, -CAST(op.lead_time_days AS INT))
# MAGIC               END
# MAGIC     END AS starting_date,
# MAGIC 
# MAGIC     CASE WHEN op.replenishment_system IN ('Prod. Order','Assembly')
# MAGIC          THEN op.bucket_earliest_date END AS ending_date,
# MAGIC 
# MAGIC     -- Bucket-week to actual date range
# MAGIC     date_add(DATE'2020-01-06', CAST(op.bucket_week * 7 AS INT))      AS bucket_start_date,
# MAGIC     date_add(DATE'2020-01-06', CAST((op.bucket_week * 7) + 6 AS INT)) AS bucket_end_date
# MAGIC FROM v_bucket_constrained op
# MAGIC WHERE op.final_qty > 0;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- FINAL TABLE #1: fabric_requisition_line
# MAGIC --   Grain: 1 row per (bc_company + item_no + variant_code + bucket_week)
# MAGIC --   This is the MRP PROPOSAL table — what Buyers act on
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TABLE fabric_requisition_line AS
# MAGIC WITH
# MAGIC demand_totals AS (
# MAGIC   SELECT
# MAGIC     bc_company, item_no,
# MAGIC     SUM(demand_qty)      AS total_demand_quantity_uom1_all,
# MAGIC     SUM(demand_qty_base) AS total_demand_quantity_base_uom1_all,
# MAGIC     COUNT(*)             AS total_demand_line_count,
# MAGIC     MIN(demand_date)     AS earliest_demand_date_all
# MAGIC   FROM v_demand_all
# MAGIC   GROUP BY bc_company, item_no
# MAGIC ),
# MAGIC 
# MAGIC ile_avg AS (
# MAGIC   SELECT
# MAGIC       `Item No.` AS item_no,
# MAGIC       SUM(ABS(`Quantity`)) / 180.0 AS avg_daily_usage
# MAGIC   FROM `Silver_BC_Lakehouse`.`bc`.`Item Ledger Entry`
# MAGIC   WHERE `Entry Type` = 'Consumption'
# MAGIC     AND `Posting Date` >= add_months(current_date(), -6)
# MAGIC   GROUP BY `Item No.`
# MAGIC ),
# MAGIC 
# MAGIC enrich AS (
# MAGIC   SELECT
# MAGIC       bd.*,
# MAGIC       COALESCE(st.onhand_uom1_display, 0)        AS onhand_uom1,
# MAGIC       COALESCE(st.onhand_uom2_display, 0)        AS onhand_uom2,
# MAGIC       COALESCE(st.best_stock_location_code, '')  AS best_stock_location_code,
# MAGIC       COALESCE(po.purchase_order_coming_uom1, 0) AS purchase_order_coming_uom1,
# MAGIC       po.earliest_po_receipt_date                AS earliest_po_receipt_date,
# MAGIC       COALESCE(po.purchase_order_coming_uom2, 0) AS purchase_order_coming_uom2,
# MAGIC       dt.total_demand_quantity_uom1_all,
# MAGIC       dt.total_demand_quantity_base_uom1_all,
# MAGIC       dt.total_demand_line_count,
# MAGIC       dt.earliest_demand_date_all
# MAGIC   FROM v_bucket_dated bd
# MAGIC   LEFT JOIN v_stock_item_sum st
# MAGIC     ON bd.item_no = st.item_no
# MAGIC   LEFT JOIN v_po_incoming_item po
# MAGIC     ON bd.bc_company = po.bc_company AND bd.item_no = po.item_no
# MAGIC   LEFT JOIN demand_totals dt
# MAGIC     ON bd.bc_company = dt.bc_company AND bd.item_no = dt.item_no
# MAGIC ),
# MAGIC 
# MAGIC with_uom2 AS (
# MAGIC   SELECT
# MAGIC       e.*,
# MAGIC       CASE
# MAGIC         WHEN e.secondary_qty_per_base > 0 THEN CEIL(e.final_qty / e.secondary_qty_per_base)
# MAGIC         ELSE NULL
# MAGIC       END AS need_to_buy_qty_uom2_from_uom1,
# MAGIC 
# MAGIC       COALESCE(c.demand_quantity_uom2_consumption, 0) AS demand_quantity_uom2,
# MAGIC 
# MAGIC       CASE
# MAGIC         WHEN COALESCE(c.demand_quantity_uom2_consumption, 0) > 0 THEN
# MAGIC           GREATEST(
# MAGIC             COALESCE(c.demand_quantity_uom2_consumption, 0)
# MAGIC             - COALESCE(e.onhand_uom2, 0)
# MAGIC             - COALESCE(e.purchase_order_coming_uom2, 0),
# MAGIC             0
# MAGIC           )
# MAGIC         ELSE
# MAGIC           CASE
# MAGIC             WHEN e.secondary_qty_per_base > 0 THEN CEIL(e.final_qty / e.secondary_qty_per_base)
# MAGIC             ELSE NULL
# MAGIC           END
# MAGIC       END AS need_to_buy_qty_uom2_from_consumption
# MAGIC   FROM enrich e
# MAGIC   LEFT JOIN v_consumption_uom2 c
# MAGIC     ON e.item_no = c.item_no
# MAGIC ),
# MAGIC 
# MAGIC with_final_values AS (
# MAGIC   SELECT
# MAGIC     u.*,
# MAGIC 
# MAGIC     -- UOM-aware rounding
# MAGIC     CASE
# MAGIC       WHEN UPPER(TRIM(u.base_uom)) IN ('PCS', 'PRS', 'SET')
# MAGIC         THEN CEIL(u.final_qty)
# MAGIC       WHEN UPPER(TRIM(u.base_uom)) = 'CTS'
# MAGIC         THEN ROUND(u.final_qty, 2)
# MAGIC       WHEN UPPER(TRIM(u.base_uom)) = 'GR'
# MAGIC         THEN ROUND(u.final_qty, 3)
# MAGIC       WHEN UPPER(TRIM(u.base_uom)) IN ('CM', 'CM2')
# MAGIC         THEN ROUND(u.final_qty, 2)
# MAGIC       ELSE ROUND(u.final_qty, 2)
# MAGIC     END AS final_uom1_calc,
# MAGIC 
# MAGIC     CEIL(
# MAGIC       CASE
# MAGIC         WHEN COALESCE(u.need_to_buy_qty_uom2_from_consumption, 0) > 0
# MAGIC           THEN u.need_to_buy_qty_uom2_from_consumption
# MAGIC         ELSE u.need_to_buy_qty_uom2_from_uom1
# MAGIC       END
# MAGIC     ) AS final_uom2_calc,
# MAGIC 
# MAGIC     -- Flag earliest bucket per item
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY u.bc_company, u.item_no
# MAGIC       ORDER BY u.bucket_week ASC
# MAGIC     ) AS rn_earliest_bucket
# MAGIC   FROM with_uom2 u
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC   -- SYSTEM COLS
# MAGIC   uuid()                                       AS `$systemId`,
# MAGIC   current_timestamp()                          AS SystemCreatedAt,
# MAGIC   current_timestamp()                          AS SystemModifiedAt,
# MAGIC   (unix_timestamp(current_timestamp()) * 1000) AS SystemRowVersion,
# MAGIC 
# MAGIC   -- BUCKET IDENTIFIERS (grain keys)
# MAGIC   f.bc_company                                 AS bc_company,
# MAGIC   f.item_no                                    AS item_no,
# MAGIC   f.variant_code                               AS variant_code,
# MAGIC   f.bucket_week                                AS bucket_week,
# MAGIC   f.bucket_start_date                          AS bucket_start_date,
# MAGIC   f.bucket_end_date                            AS bucket_end_date,
# MAGIC 
# MAGIC   -- ITEM
# MAGIC   f.item_no                                     AS no,
# MAGIC   f.item_description                            AS description,
# MAGIC   f.description_2                               AS description_2,
# MAGIC   f.item_category_code                          AS item_category_code,
# MAGIC 
# MAGIC   -- UOM
# MAGIC   f.base_uom                                    AS base_uom_uom1,
# MAGIC   f.secondary_uom                               AS secondary_uom_uom2,
# MAGIC   f.secondary_qty_per_base                      AS uom2_per_uom1,
# MAGIC 
# MAGIC   -- DEMAND AGGREGATE (this bucket)
# MAGIC   f.bucket_demand_qty_uom1                      AS demand_quantity_uom1_bucket,
# MAGIC   f.abs_demand_qty_base                         AS demand_quantity_base_uom1_bucket,
# MAGIC   f.demand_line_count_in_bucket                 AS demand_line_count_in_bucket,
# MAGIC   f.bucket_earliest_date                        AS bucket_earliest_demand_date,
# MAGIC   f.bucket_latest_date                          AS bucket_latest_demand_date,
# MAGIC   f.demand_quantity_uom2                        AS demand_quantity_uom2,
# MAGIC   f.min_planning_level                          AS planning_level,
# MAGIC 
# MAGIC   -- SHORTAGE / AVAILABILITY
# MAGIC   f.shortage_qty                                AS shortage_quantity_uom1,
# MAGIC   f.onhand_pool_qty                             AS onhand_pool_uom1,
# MAGIC   f.available_before_bucket                     AS available_before_bucket_uom1,
# MAGIC   f.available_with_bucket_supply                AS available_with_bucket_supply_uom1,
# MAGIC   f.projected_after_bucket                      AS projected_after_bucket_uom1,
# MAGIC   f.supply_qty_base                             AS supply_in_bucket_uom1,
# MAGIC 
# MAGIC   -- STOCK (display)
# MAGIC   f.onhand_uom1                                 AS onhand_quantity_uom1,
# MAGIC   f.onhand_uom2                                 AS onhand_quantity_uom2,
# MAGIC   f.best_stock_location_code                    AS best_stock_location_code,
# MAGIC 
# MAGIC   -- PO INCOMING (item-total across all buckets)
# MAGIC   f.purchase_order_coming_uom1                  AS po_incoming_quantity_uom1,
# MAGIC   f.purchase_order_coming_uom2                  AS po_incoming_quantity_uom2,
# MAGIC   f.earliest_po_receipt_date                    AS earliest_po_receipt_date,
# MAGIC 
# MAGIC   -- NEED TO BUY (this is the MRP proposal)
# MAGIC   f.final_qty                                   AS need_to_buy_quantity_uom1,
# MAGIC   f.need_to_buy_qty_uom2_from_uom1              AS need_to_buy_quantity_uom2_from_uom1,
# MAGIC   f.need_to_buy_qty_uom2_from_consumption       AS need_to_buy_quantity_uom2_from_consumption,
# MAGIC   f.final_uom1_calc                             AS final_uom1,
# MAGIC   f.final_uom2_calc                             AS final_uom2,
# MAGIC 
# MAGIC   -- FLAG: earliest bucket per item (for alert / display)
# MAGIC   CASE WHEN f.rn_earliest_bucket = 1 THEN 1 ELSE 0 END AS is_earliest_bucket_per_item,
# MAGIC 
# MAGIC   -- PLANNING PARAMS (debug)
# MAGIC   f.safety_stock                                AS safety_stock_uom1,
# MAGIC   f.reorder_point                               AS reorder_point_uom1,
# MAGIC   f.reorder_quantity                            AS reorder_quantity_uom1,
# MAGIC   f.maximum_inventory                           AS maximum_inventory_uom1,
# MAGIC   f.min_order_qty                               AS min_order_qty_uom1,
# MAGIC   f.max_order_qty                               AS max_order_qty_uom1,
# MAGIC   f.order_multiple                              AS order_multiple_uom1,
# MAGIC   f.reordering_policy                           AS reordering_policy,
# MAGIC   f.policy_qty                                  AS policy_quantity_uom1,
# MAGIC   f.qty_after_min                               AS qty_after_min_uom1,
# MAGIC   f.qty_after_max                               AS qty_after_max_uom1,
# MAGIC   f.final_qty                                   AS final_quantity_uom1,
# MAGIC 
# MAGIC   -- COST
# MAGIC   f.default_vendor_no                           AS vendor_no,
# MAGIC   f.unit_cost                                   AS unit_cost,
# MAGIC   (f.final_qty * f.unit_cost)                   AS cost_amount,
# MAGIC   f.indirect_cost_pct                           AS indirect_cost_pct,
# MAGIC   f.overhead_rate                               AS overhead_rate,
# MAGIC 
# MAGIC   -- BC WORKSHEET COLS
# MAGIC   'PLANNING'                                    AS worksheet_template_name,
# MAGIC   'DEFAULT'                                     AS journal_batch_name,
# MAGIC   (ROW_NUMBER() OVER (
# MAGIC      PARTITION BY f.bc_company
# MAGIC      ORDER BY f.item_no, f.variant_code, f.bucket_week
# MAGIC    ) * 10000)                                   AS line_no,
# MAGIC 
# MAGIC   'Item'                                        AS type,
# MAGIC   f.final_qty                                   AS quantity,
# MAGIC   f.unit_cost                                   AS direct_unit_cost,
# MAGIC   f.due_date                                    AS due_date,
# MAGIC   f.order_date                                  AS order_date,
# MAGIC   f.required_date                               AS required_date,
# MAGIC 
# MAGIC   -- PRODUCTION / ROUTING
# MAGIC   f.routing_no                                  AS routing_no,
# MAGIC   f.gen_prod_posting_group                      AS gen_prod_posting_group,
# MAGIC   f.low_level_code                              AS low_level_code,
# MAGIC   f.scrap_pct                                   AS scrap_pct,
# MAGIC   f.starting_date                               AS starting_date,
# MAGIC   f.ending_date                                 AS ending_date,
# MAGIC   f.production_bom_no                           AS production_bom_no,
# MAGIC   f.replenishment_system                        AS replenishment_system,
# MAGIC 
# MAGIC   -- ITEM-LEVEL SUMMARY (for cross-check)
# MAGIC   f.total_demand_quantity_uom1_all              AS total_demand_quantity_uom1_all_item,
# MAGIC   f.total_demand_quantity_base_uom1_all         AS total_demand_quantity_base_uom1_all_item,
# MAGIC   f.total_demand_line_count                     AS total_demand_line_count_item,
# MAGIC   f.earliest_demand_date_all                    AS earliest_demand_date_all_item,
# MAGIC 
# MAGIC   ROUND(COALESCE(il.avg_daily_usage, 0), 2)      AS avg_daily_usage,
# MAGIC   ROUND(COALESCE(il.avg_daily_usage, 0) * 60, 0) AS forecast_demand_60d,
# MAGIC 
# MAGIC   ROUND(
# MAGIC       COALESCE(f.onhand_pool_qty, 0)
# MAGIC     + COALESCE(f.purchase_order_coming_uom1, 0)
# MAGIC     - COALESCE(f.total_demand_quantity_base_uom1_all, 0)
# MAGIC     - (COALESCE(il.avg_daily_usage, 0) * 60)
# MAGIC   , 0)                                           AS projected_available_60d,
# MAGIC 
# MAGIC   CASE
# MAGIC     WHEN f.due_date <= date_add(current_date(), 14) AND f.final_qty > 0
# MAGIC       THEN 'URGENT - ต้องสั่งภายใน 14 วัน'
# MAGIC     WHEN f.final_qty > 0
# MAGIC       THEN 'ควรสั่งซื้อ'
# MAGIC     WHEN (
# MAGIC         COALESCE(f.onhand_pool_qty, 0)
# MAGIC       + COALESCE(f.purchase_order_coming_uom1, 0)
# MAGIC       - COALESCE(f.total_demand_quantity_base_uom1_all, 0)
# MAGIC       - (COALESCE(il.avg_daily_usage, 0) * 60)
# MAGIC     ) < 0
# MAGIC       THEN 'เฝ้าระวัง - คาดว่าจะขาดใน 60 วัน'
# MAGIC     ELSE 'OK'
# MAGIC   END                                            AS alert_flag,
# MAGIC 
# MAGIC   CASE
# MAGIC     WHEN COALESCE(il.avg_daily_usage, 0) > 0
# MAGIC     THEN date_add(
# MAGIC           current_date(),
# MAGIC           CAST(
# MAGIC             (COALESCE(f.onhand_pool_qty, 0) + COALESCE(f.purchase_order_coming_uom1, 0))
# MAGIC             / COALESCE(il.avg_daily_usage, 1)
# MAGIC           AS INT)
# MAGIC         )
# MAGIC     ELSE NULL
# MAGIC   END                                            AS projected_stockout_date
# MAGIC 
# MAGIC FROM with_final_values f
# MAGIC LEFT JOIN ile_avg il
# MAGIC   ON f.item_no = il.item_no;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- FINAL TABLE #2: fabric_requisition_line_detail
# MAGIC --   Grain: 1 row per demand line
# MAGIC --   Purpose: Drill-down from bucket → individual demand sources
# MAGIC --   Foreign key: (bc_company, item_no, variant_code, bucket_week) → fabric_requisition_line
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TABLE fabric_requisition_line_detail AS
# MAGIC SELECT
# MAGIC   uuid()                                       AS `$systemId`,
# MAGIC   current_timestamp()                          AS SystemCreatedAt,
# MAGIC   current_timestamp()                          AS SystemModifiedAt,
# MAGIC   (unix_timestamp(current_timestamp()) * 1000) AS SystemRowVersion,
# MAGIC 
# MAGIC   -- FK to fabric_requisition_line
# MAGIC   d.bc_company   AS bc_company,
# MAGIC   d.item_no      AS item_no,
# MAGIC   d.variant_code AS variant_code,
# MAGIC   FLOOR(DATEDIFF(d.demand_date, DATE'2020-01-06') / 7) AS bucket_week,
# MAGIC 
# MAGIC   -- Actual date bounds of the bucket
# MAGIC   date_add(DATE'2020-01-06',
# MAGIC     CAST(FLOOR(DATEDIFF(d.demand_date, DATE'2020-01-06') / 7) * 7 AS INT)
# MAGIC   ) AS bucket_start_date,
# MAGIC   date_add(DATE'2020-01-06',
# MAGIC     CAST((FLOOR(DATEDIFF(d.demand_date, DATE'2020-01-06') / 7) * 7) + 6 AS INT)
# MAGIC   ) AS bucket_end_date,
# MAGIC 
# MAGIC   -- Demand line detail
# MAGIC   d.demand_id                 AS demand_id,
# MAGIC   d.demand_date               AS demand_date,
# MAGIC   d.demand_qty                AS demand_quantity_uom1,
# MAGIC   d.demand_qty_base           AS demand_quantity_base_uom1,
# MAGIC   d.demand_type               AS demand_type,
# MAGIC   d.demand_subtype            AS demand_subtype,
# MAGIC   d.location_code             AS demand_location_code,   -- 🔧 original location (may NOT be a stock location)
# MAGIC   d.planning_level            AS planning_level,
# MAGIC 
# MAGIC   -- Source references
# MAGIC   d.sales_order_no            AS sales_order_no,
# MAGIC   d.sales_line_no             AS sales_order_line_no,
# MAGIC   d.prod_order_no             AS prod_order_no,
# MAGIC   d.prod_order_line_no        AS prod_order_line_no,
# MAGIC   d.sell_to_customer_no       AS sell_to_customer_no,
# MAGIC   d.original_item_no          AS original_item_no,
# MAGIC 
# MAGIC   -- Dimensions
# MAGIC   d.dimension_set_id          AS dimension_set_id,
# MAGIC   d.shortcut_dim_1            AS shortcut_dimension_1_code,
# MAGIC   d.shortcut_dim_2            AS shortcut_dimension_2_code,
# MAGIC 
# MAGIC   -- UOM on demand line
# MAGIC   d.unit_of_measure_code      AS unit_of_measure_code_demand,
# MAGIC   d.qty_per_uom               AS qty_per_unit_of_measure_demand
# MAGIC FROM v_demand_all d;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- %%sql
# MAGIC -- SELECT
# MAGIC --   'In item table'       AS source,
# MAGIC --   COUNT(DISTINCT `No.`) AS item_count
# MAGIC -- FROM item
# MAGIC -- WHERE `No.` LIKE 'MST-%'
# MAGIC -- UNION ALL
# MAGIC -- SELECT
# MAGIC --   'In demand'                  AS source,
# MAGIC --   COUNT(DISTINCT item_no)      AS item_count
# MAGIC -- FROM fabric_requisition_line_detail
# MAGIC -- WHERE item_no LIKE 'MST-%';
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- DIAG 2: N10915218W-00001 — deep dive ตัวอย่างที่ชัดเจน
# MAGIC -- ดูว่าทำไม 270 demand → 41 proposal
# MAGIC -- ============================================================
# MAGIC SELECT
# MAGIC   bucket_week,
# MAGIC   bucket_start_date,
# MAGIC   demand_quantity_base_uom1_bucket AS bucket_demand,
# MAGIC   onhand_pool_uom1                 AS onhand_pool,
# MAGIC   available_before_bucket_uom1     AS avail_before,
# MAGIC   supply_in_bucket_uom1            AS supply_in_bucket,
# MAGIC   projected_after_bucket_uom1      AS projected_after,
# MAGIC   safety_stock_uom1                AS safety_stock,
# MAGIC   reorder_point_uom1               AS reorder_point,
# MAGIC   shortage_quantity_uom1           AS shortage,
# MAGIC   policy_quantity_uom1             AS policy_qty,
# MAGIC   qty_after_min_uom1               AS qty_min,
# MAGIC   qty_after_max_uom1               AS qty_max,
# MAGIC   final_quantity_uom1              AS final_qty,
# MAGIC   need_to_buy_quantity_uom1        AS need_to_buy,
# MAGIC   reordering_policy,
# MAGIC   min_order_qty_uom1,
# MAGIC   max_order_qty_uom1,
# MAGIC   order_multiple_uom1
# MAGIC FROM fabric_requisition_line
# MAGIC WHERE item_no = 'N10915218W-00001'
# MAGIC ORDER BY bucket_week;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- DIAG 3: ดูทุก demand line ของ N10915218W-00001
# MAGIC -- ใน fabric_requisition_line_detail
# MAGIC -- ============================================================
# MAGIC SELECT
# MAGIC   bucket_week,
# MAGIC   bucket_start_date,
# MAGIC   COUNT(*)                           AS lines,
# MAGIC   SUM(demand_quantity_base_uom1)     AS total_qty,
# MAGIC   MIN(demand_date)                   AS earliest,
# MAGIC   MAX(demand_date)                   AS latest,
# MAGIC   COUNT(DISTINCT demand_location_code) AS distinct_locations
# MAGIC FROM fabric_requisition_line_detail
# MAGIC WHERE item_no = 'N10915218W-00001'
# MAGIC GROUP BY bucket_week, bucket_start_date
# MAGIC ORDER BY bucket_week;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- DIAG 4: Items ที่มี demand แต่ ไม่ใน fabric_requisition_line
# MAGIC -- ตรวจสอบว่าเป็นเพราะ reason อะไร
# MAGIC -- ============================================================
# MAGIC WITH detail_items AS (
# MAGIC   SELECT DISTINCT bc_company, item_no
# MAGIC   FROM fabric_requisition_line_detail
# MAGIC ),
# MAGIC proposed_items AS (
# MAGIC   SELECT DISTINCT bc_company, item_no
# MAGIC   FROM fabric_requisition_line
# MAGIC ),
# MAGIC missing AS (
# MAGIC   SELECT d.bc_company, d.item_no
# MAGIC   FROM detail_items d
# MAGIC   LEFT JOIN proposed_items p
# MAGIC     ON d.bc_company = p.bc_company AND d.item_no = p.item_no
# MAGIC   WHERE p.item_no IS NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   m.item_no,
# MAGIC   CASE
# MAGIC     WHEN m.item_no LIKE 'MST-%'      THEN 'Master item'
# MAGIC     WHEN m.item_no LIKE 'M0%'         THEN 'Model item'
# MAGIC     WHEN m.item_no LIKE 'L-%'         THEN 'Loose item'
# MAGIC     WHEN m.item_no LIKE 'R-%'         THEN 'Ring'
# MAGIC     WHEN m.item_no LIKE 'E-%'         THEN 'Earring'
# MAGIC     WHEN m.item_no LIKE 'N-%'         THEN 'Necklace'
# MAGIC     WHEN m.item_no LIKE 'B-%'         THEN 'Bracelet'
# MAGIC     WHEN m.item_no LIKE 'P-%'         THEN 'Pendant'
# MAGIC     WHEN m.item_no LIKE 'RM-%'        THEN 'Raw material'
# MAGIC     WHEN m.item_no LIKE 'AC-%'        THEN 'Accessory'
# MAGIC     WHEN m.item_no LIKE 'DI-%'        THEN 'Diamond'
# MAGIC     WHEN m.item_no LIKE 'GE-%'        THEN 'Gemstone'
# MAGIC     WHEN m.item_no LIKE 'PL-%'        THEN 'Plating material'
# MAGIC     WHEN m.item_no LIKE 'FCB-%' OR m.item_no LIKE 'FCS-%' OR m.item_no LIKE 'FNBL%' THEN 'Finding'
# MAGIC     ELSE 'Other'
# MAGIC   END AS item_class
# MAGIC FROM missing m
# MAGIC ORDER BY item_class, m.item_no
# MAGIC LIMIT 50
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- DIAG 5: Class summary — กี่ class และมีปัญหาเท่าไหร่
# MAGIC -- ============================================================
# MAGIC WITH detail_items AS (
# MAGIC   SELECT DISTINCT bc_company, item_no
# MAGIC   FROM fabric_requisition_line_detail
# MAGIC ),
# MAGIC proposed_items AS (
# MAGIC   SELECT DISTINCT bc_company, item_no
# MAGIC   FROM fabric_requisition_line
# MAGIC ),
# MAGIC classified AS (
# MAGIC   SELECT
# MAGIC     d.bc_company,
# MAGIC     d.item_no,
# MAGIC     CASE
# MAGIC       WHEN d.item_no LIKE 'MST-%'      THEN 'Master item'
# MAGIC       WHEN d.item_no LIKE 'M0%'         THEN 'Model item (M0*)'
# MAGIC       WHEN d.item_no LIKE 'L-%' OR d.item_no LIKE 'L0%'  THEN 'Loose/L*'
# MAGIC       WHEN d.item_no LIKE 'R-%' OR d.item_no LIKE 'R0%'  THEN 'Ring'
# MAGIC       WHEN d.item_no LIKE 'E-%' OR d.item_no LIKE 'E0%'  THEN 'Earring'
# MAGIC       WHEN d.item_no LIKE 'N-%' OR d.item_no LIKE 'N0%'  THEN 'Necklace'
# MAGIC       WHEN d.item_no LIKE 'B-%' OR d.item_no LIKE 'B0%'  THEN 'Bracelet'
# MAGIC       WHEN d.item_no LIKE 'P-%' OR d.item_no LIKE 'P0%'  THEN 'Pendant'
# MAGIC       WHEN d.item_no LIKE 'RM-%'        THEN 'Raw material'
# MAGIC       WHEN d.item_no LIKE 'AC-%'        THEN 'Accessory'
# MAGIC       WHEN d.item_no LIKE 'DI-%'        THEN 'Diamond'
# MAGIC       WHEN d.item_no LIKE 'GE-%'        THEN 'Gemstone'
# MAGIC       WHEN d.item_no LIKE 'PL-%'        THEN 'Plating material'
# MAGIC       WHEN d.item_no LIKE 'FCB-%' OR d.item_no LIKE 'FCS-%' OR d.item_no LIKE 'FNBL%' THEN 'Finding'
# MAGIC       WHEN d.item_no LIKE 'C%' AND d.item_no LIKE '%SLV%'  THEN 'Charm-Silver'
# MAGIC       ELSE 'Other'
# MAGIC     END AS item_class,
# MAGIC     CASE WHEN p.item_no IS NULL THEN 0 ELSE 1 END AS has_proposal
# MAGIC   FROM detail_items d
# MAGIC   LEFT JOIN proposed_items p
# MAGIC     ON d.bc_company = p.bc_company AND d.item_no = p.item_no
# MAGIC )
# MAGIC SELECT
# MAGIC   item_class,
# MAGIC   COUNT(*) AS total_items,
# MAGIC   SUM(has_proposal) AS proposed_items,
# MAGIC   COUNT(*) - SUM(has_proposal) AS missing_items,
# MAGIC   CAST(SUM(has_proposal) AS FLOAT) / NULLIF(COUNT(*), 0) * 100 AS coverage_pct
# MAGIC FROM classified
# MAGIC GROUP BY item_class
# MAGIC ORDER BY missing_items DESC;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- DIAG 6: ตรวจ item table ว่ามี items ที่ missing หรือไม่
# MAGIC -- (รันใน notebook Spark SQL ถ้าไม่มี item ใน warehouse endpoint)
# MAGIC -- ============================================================
# MAGIC -- %%sql
# MAGIC -- SELECT
# MAGIC --   COUNT(DISTINCT i.`No.`)   AS items_in_master,
# MAGIC --   COUNT(DISTINCT d.item_no) AS items_with_demand,
# MAGIC --   COUNT(DISTINCT CASE WHEN i.`No.` IS NULL THEN d.item_no END) AS demand_without_master
# MAGIC -- FROM (SELECT DISTINCT item_no, bc_company FROM fabric_requisition_line_detail) d
# MAGIC -- LEFT JOIN item i ON d.bc_company = i.`BC Company` AND d.item_no = i.`No.`;

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
