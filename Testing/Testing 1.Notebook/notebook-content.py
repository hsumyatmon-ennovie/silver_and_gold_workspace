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
# MAGIC USE Silver_Planning_Lakehouse.dbo;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # v_onhand (1A)
# ```
# current on-hand by company + item + location.
# ```

# CELL ********************

# MAGIC %%sql
# MAGIC -- 1A
# MAGIC CREATE OR REPLACE TEMP VIEW v_onhand AS
# MAGIC SELECT
# MAGIC   `BC Company`              AS bc_company,
# MAGIC   `Item No.`                AS item_no,
# MAGIC   `Location Code`           AS location_code,
# MAGIC   SUM(`Remaining Quantity`) AS onhand_qty
# MAGIC FROM item_ledger_entry
# MAGIC -- optionally filter to Open = 1 if that is reliable in your data
# MAGIC GROUP BY `BC Company`, `Item No.`, `Location Code`;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # v_demand_sales
# 
# ```
# This produces time-phased demand lines.
# ```

# CELL ********************

# MAGIC %%sql
# MAGIC -- 1B
# MAGIC CREATE OR REPLACE TEMP VIEW v_demand_sales AS
# MAGIC SELECT
# MAGIC   `BC Company`                 AS bc_company,
# MAGIC   `No.`                        AS item_no,
# MAGIC   `Location Code`              AS location_code,
# MAGIC   COALESCE(`Shipment Date`, `Requested Delivery Date`, `Promised Delivery Date`) AS demand_date,
# MAGIC   SUM(`Outstanding Quantity`)  AS demand_qty,
# MAGIC   MIN(`Document No.`)          AS sales_order_no
# MAGIC FROM sales_line
# MAGIC WHERE `Type` = 'Item'
# MAGIC   AND `Outstanding Quantity` > 0
# MAGIC   AND COALESCE(`Shipment Date`, `Requested Delivery Date`, `Promised Delivery Date`) IS NOT NULL
# MAGIC GROUP BY
# MAGIC   `BC Company`, `No.`, `Location Code`,
# MAGIC   COALESCE(`Shipment Date`, `Requested Delivery Date`, `Promised Delivery Date`);


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # v_supply_po
# ```
# 1C) Supply: Open purchase receipts (Outstanding qty by expected receipt date)
# ```

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_supply_po AS
# MAGIC SELECT
# MAGIC   pl.`BC Company`            AS bc_company,
# MAGIC   pl.`No.`                   AS item_no,
# MAGIC   pl.`Location Code`         AS location_code,
# MAGIC   COALESCE(pl.`Expected Receipt Date`, ph.`Expected Receipt Date`, ph.`Promised Receipt Date`) AS supply_date,
# MAGIC   SUM(pl.`Outstanding Quantity`) AS supply_qty,
# MAGIC   MIN(pl.`Document No.`)     AS purchase_order_no
# MAGIC FROM purchase_line pl
# MAGIC LEFT JOIN purchase_header ph
# MAGIC   ON  pl.`BC Company` = ph.`BC Company`
# MAGIC   AND pl.`Document Type` = ph.`Document Type`
# MAGIC   AND pl.`Document No.` = ph.`No.`
# MAGIC WHERE pl.`Type` = 'Item'
# MAGIC   AND pl.`Outstanding Quantity` > 0
# MAGIC   AND COALESCE(pl.`Expected Receipt Date`, ph.`Expected Receipt Date`, ph.`Promised Receipt Date`) IS NOT NULL
# MAGIC GROUP BY
# MAGIC   pl.`BC Company`, pl.`No.`, pl.`Location Code`,
# MAGIC   COALESCE(pl.`Expected Receipt Date`, ph.`Expected Receipt Date`, ph.`Promised Receipt Date`);


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 1D) Planning parameters (from Item master)

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_item_planning AS
# MAGIC SELECT
# MAGIC   `BC Company`              AS bc_company,
# MAGIC   `No.`                     AS item_no,
# MAGIC   -- planning knobs
# MAGIC   `Reordering Policy`       AS reordering_policy,
# MAGIC   `Reorder Point`           AS reorder_point,
# MAGIC   `Reorder Quantity`        AS reorder_quantity,
# MAGIC   `Maximum Inventory`       AS maximum_inventory,
# MAGIC   `Minimum Order Quantity`  AS min_order_qty,
# MAGIC   `Maximum Order Quantity`  AS max_order_qty,
# MAGIC   `Order Multiple`          AS order_multiple,
# MAGIC   `Safety Stock Quantity`   AS safety_stock,
# MAGIC   `Safety Lead Time`        AS safety_lead_time,
# MAGIC   `Lead Time Calculation`   AS lead_time_calc,
# MAGIC   `Replenishment System`    AS replenishment_system,
# MAGIC   `Vendor No.`              AS default_vendor_no,
# MAGIC   `Description`             AS item_description
# MAGIC FROM item;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Step 2 — Build a time-bucketed netting base table
# 
# ```
# For MRP, we net by company + item + location + date.
# 
# 2A) Create a “calendar” of dates from demand/supply
# ```

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_dates AS
# MAGIC SELECT DISTINCT bc_company, item_no, location_code, demand_date AS dt
# MAGIC FROM v_demand_sales
# MAGIC UNION
# MAGIC SELECT DISTINCT bc_company, item_no, location_code, supply_date AS dt
# MAGIC FROM v_supply_po;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ```
# 2B) Combine demand + supply + onhand into one timeline
# ```

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_timeline AS
# MAGIC SELECT
# MAGIC   d.bc_company,
# MAGIC   d.item_no,
# MAGIC   d.location_code,
# MAGIC   d.dt,
# MAGIC   COALESCE(ds.demand_qty, 0) AS demand_qty,
# MAGIC   COALESCE(sp.supply_qty, 0) AS supply_qty,
# MAGIC   COALESCE(oh.onhand_qty, 0) AS onhand_qty
# MAGIC FROM v_dates d
# MAGIC LEFT JOIN v_demand_sales ds
# MAGIC   ON d.bc_company=ds.bc_company AND d.item_no=ds.item_no AND d.location_code=ds.location_code AND d.dt=ds.demand_date
# MAGIC LEFT JOIN v_supply_po sp
# MAGIC   ON d.bc_company=sp.bc_company AND d.item_no=sp.item_no AND d.location_code=sp.location_code AND d.dt=sp.supply_date
# MAGIC LEFT JOIN v_onhand oh
# MAGIC   ON d.bc_company=oh.bc_company AND d.item_no=oh.item_no AND d.location_code=oh.location_code;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Step 3 — Compute projected inventory and shortages (window functions)
# 
# ```
# This is the heart of MRP netting.
# ```

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_projection AS
# MAGIC SELECT
# MAGIC   t.*,
# MAGIC   ip.safety_stock,
# MAGIC   ip.reorder_point,
# MAGIC   ip.replenishment_system,
# MAGIC   ip.default_vendor_no,
# MAGIC   ip.item_description,
# MAGIC 
# MAGIC   -- projected inventory after each date
# MAGIC   (t.onhand_qty
# MAGIC    + SUM(t.supply_qty - t.demand_qty) OVER (
# MAGIC        PARTITION BY t.bc_company, t.item_no, t.location_code
# MAGIC        ORDER BY t.dt
# MAGIC        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC      )
# MAGIC   ) AS projected_qty
# MAGIC FROM v_timeline t
# MAGIC LEFT JOIN v_item_planning ip
# MAGIC   ON t.bc_company = ip.bc_company AND t.item_no = ip.item_no;
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW v_shortage AS
# MAGIC SELECT
# MAGIC   *,
# MAGIC   CASE
# MAGIC     WHEN projected_qty < COALESCE(safety_stock, 0) THEN (COALESCE(safety_stock, 0) - projected_qty)
# MAGIC     WHEN projected_qty < COALESCE(reorder_point, 0) THEN (COALESCE(reorder_point, 0) - projected_qty)
# MAGIC     ELSE 0
# MAGIC   END AS shortage_qty
# MAGIC FROM v_projection;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Step 4 — Turn shortages into “planned replenishment” lines
# 
# For v1: create one requisition line per shortage date.
# 
# Lot sizing (simple v1)
# 
# Use:
# 
# - if reorder_quantity exists → use it
# - else use shortage_qty
# - then apply min/order_multiple rules if present

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_plan_lines AS
# MAGIC SELECT
# MAGIC   s.bc_company,
# MAGIC   s.item_no,
# MAGIC   s.location_code,
# MAGIC   s.dt AS due_date,
# MAGIC 
# MAGIC   -- planned qty
# MAGIC   CASE
# MAGIC     WHEN ip.reorder_quantity IS NOT NULL AND ip.reorder_quantity > 0 THEN ip.reorder_quantity
# MAGIC     ELSE s.shortage_qty
# MAGIC   END AS base_qty,
# MAGIC 
# MAGIC   ip.min_order_qty,
# MAGIC   ip.max_order_qty,
# MAGIC   ip.order_multiple,
# MAGIC   ip.replenishment_system,
# MAGIC   ip.default_vendor_no,
# MAGIC   ip.item_description
# MAGIC FROM v_shortage s
# MAGIC JOIN v_item_planning ip
# MAGIC   ON s.bc_company=ip.bc_company AND s.item_no=ip.item_no
# MAGIC WHERE s.shortage_qty > 0;
# MAGIC 
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW v_plan_lines_sized AS
# MAGIC SELECT
# MAGIC   *,
# MAGIC   -- apply minimum order qty
# MAGIC   CASE
# MAGIC     WHEN min_order_qty IS NOT NULL AND min_order_qty > 0 AND base_qty < min_order_qty THEN min_order_qty
# MAGIC     ELSE base_qty
# MAGIC   END AS qty_min_applied
# MAGIC FROM v_plan_lines;
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW v_plan_lines_final AS
# MAGIC SELECT
# MAGIC   bc_company,
# MAGIC   item_no,
# MAGIC   location_code,
# MAGIC   due_date,
# MAGIC   replenishment_system,
# MAGIC   default_vendor_no,
# MAGIC   item_description,
# MAGIC 
# MAGIC   CASE
# MAGIC     WHEN order_multiple IS NOT NULL AND order_multiple > 0 THEN
# MAGIC       CEIL(qty_min_applied / order_multiple) * order_multiple
# MAGIC     ELSE qty_min_applied
# MAGIC   END AS planned_qty
# MAGIC FROM v_plan_lines_sized;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Step 5 — Output as requisition_line-shaped table (Spark SQL)

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE fabric_requisition_line AS
# MAGIC SELECT
# MAGIC   bc_company                                   AS bc_company,
# MAGIC   'PLANNING'                                   AS worksheet_template_name,
# MAGIC   'DEFAULT'                                    AS journal_batch_name,
# MAGIC 
# MAGIC   ROW_NUMBER() OVER (
# MAGIC     PARTITION BY bc_company
# MAGIC     ORDER BY bc_company, item_no, location_code, due_date
# MAGIC   ) * 10000                                     AS line_no,
# MAGIC 
# MAGIC   'Item'                                        AS type,
# MAGIC   item_no                                       AS no,
# MAGIC   item_description                              AS description,
# MAGIC   CAST(NULL AS STRING)                          AS description_2,
# MAGIC 
# MAGIC   planned_qty                                   AS quantity,
# MAGIC   default_vendor_no                             AS vendor_no,
# MAGIC   CAST(NULL AS DOUBLE)                          AS direct_unit_cost,
# MAGIC   CAST(due_date AS DATE)                        AS due_date,
# MAGIC   CAST(NULL AS STRING)                          AS requester_id,
# MAGIC   CAST(NULL AS BOOLEAN)                         AS confirmed,
# MAGIC   CAST(NULL AS STRING)                          AS shortcut_dimension_1_code,
# MAGIC   CAST(NULL AS STRING)                          AS shortcut_dimension_2_code,
# MAGIC   location_code                                 AS location_code,
# MAGIC 
# MAGIC   CAST(NULL AS STRING)                          AS demand_type,
# MAGIC   CAST(NULL AS STRING)                          AS demand_order_no,
# MAGIC   CAST(NULL AS INT)                             AS demand_line_no,
# MAGIC   CAST(NULL AS DATE)                            AS demand_date,
# MAGIC   CAST(NULL AS DOUBLE)                          AS demand_quantity,
# MAGIC 
# MAGIC   replenishment_system                          AS replenishment_system,
# MAGIC 
# MAGIC   'New'                                         AS action_message,
# MAGIC   TRUE                                          AS accept_action_message,
# MAGIC 
# MAGIC   current_timestamp()                           AS systemcreatedat,
# MAGIC   current_timestamp()                           AS systemmodifiedat
# MAGIC FROM v_plan_lines_final;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 6 — Validate vs BC requisition_line


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE VIEW bc_requisition_line_snake AS
# MAGIC SELECT
# MAGIC   `BC Company`                AS bc_company,
# MAGIC   `Worksheet Template Name`   AS worksheet_template_name,
# MAGIC   `Journal Batch Name`        AS journal_batch_name,
# MAGIC   `Line No.`                  AS line_no,
# MAGIC   `Type`                      AS type,
# MAGIC   `No.`                       AS no,
# MAGIC   `Description`               AS description,
# MAGIC   `Description 2`             AS description_2,
# MAGIC   `Quantity`                  AS quantity,
# MAGIC   `Vendor No.`                AS vendor_no,
# MAGIC   `Direct Unit Cost`          AS direct_unit_cost,
# MAGIC   `Due Date`                  AS due_date,
# MAGIC   `Requester ID`              AS requester_id,
# MAGIC   `Confirmed`                 AS confirmed,
# MAGIC   `Shortcut Dimension 1 Code` AS shortcut_dimension_1_code,
# MAGIC   `Shortcut Dimension 2 Code` AS shortcut_dimension_2_code,
# MAGIC   `Location Code`             AS location_code,
# MAGIC   `Demand Type`               AS demand_type,
# MAGIC   `Demand Order No.`          AS demand_order_no,
# MAGIC   `Demand Line No.`           AS demand_line_no,
# MAGIC   `Demand Date`               AS demand_date,
# MAGIC   `Demand Quantity`           AS demand_quantity,
# MAGIC   `Replenishment System`      AS replenishment_system,
# MAGIC   `Action Message`            AS action_message,
# MAGIC   `Accept Action Message`     AS accept_action_message,
# MAGIC   `SystemCreatedAt`           AS systemcreatedat,
# MAGIC   `SystemModifiedAt`          AS systemmodifiedat
# MAGIC FROM requisition_line;
# MAGIC 
# MAGIC -- count compare
# MAGIC CREATE OR REPLACE VIEW mrp_union_output AS
# MAGIC SELECT * FROM bc_requisition_line_snake
# MAGIC UNION ALL
# MAGIC SELECT * FROM fabric_requisition_line;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- ============================================================================
# MAGIC -- BUSINESS CENTRAL MRP CALCULATION - FIXED VERSION
# MAGIC -- ============================================================================
# MAGIC -- Key Fix: Production component demand = REMAINING qty (what still needs to be consumed)
# MAGIC --          Purchase calculation = Remaining - Inventory - Existing POs
# MAGIC -- ============================================================================
# MAGIC 
# MAGIC USE Silver_Planning_Lakehouse.dbo;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 1: Prepare Item Master with Planning Parameters
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_item_planning_params AS
# MAGIC SELECT
# MAGIC     `No.` AS item_no,
# MAGIC     `Description` AS description,
# MAGIC     `Type` AS type,
# MAGIC     `Replenishment System` AS replenishment_system,
# MAGIC     `Reordering Policy` AS reordering_policy,
# MAGIC     `Manufacturing Policy` AS manufacturing_policy,
# MAGIC     `Reorder Point` AS reorder_point,
# MAGIC     `Reorder Quantity` AS reorder_quantity,
# MAGIC     `Maximum Inventory` AS maximum_inventory,
# MAGIC     `Safety Stock Quantity` AS safety_stock_quantity,
# MAGIC     `Lead Time Calculation` AS lead_time_calculation,
# MAGIC     `Lot Size` AS lot_size,
# MAGIC     `Minimum Order Quantity` AS minimum_order_quantity,
# MAGIC     `Maximum Order Quantity` AS maximum_order_quantity,
# MAGIC     `Order Multiple` AS order_multiple,
# MAGIC     `Safety Lead Time` AS safety_lead_time,
# MAGIC     `Base Unit of Measure` AS base_unit_of_measure,
# MAGIC     `Include Inventory` AS include_inventory,
# MAGIC     `Time Bucket` AS time_bucket,
# MAGIC     `Rescheduling Period` AS rescheduling_period,
# MAGIC     `Lot Accumulation Period` AS lot_accumulation_period,
# MAGIC     `Dampener Period` AS dampener_period,
# MAGIC     `Dampener Quantity` AS dampener_quantity,
# MAGIC     `Overflow Level` AS overflow_level,
# MAGIC     `Low-Level Code` AS low_level_code,
# MAGIC     `Production BOM No.` AS production_bom_no,
# MAGIC     `Routing No.` AS routing_no,
# MAGIC     `Vendor No.` AS vendor_no,
# MAGIC     `Item Category Code` AS item_category_code,
# MAGIC     `Blocked` AS blocked,
# MAGIC     `Sales Blocked` AS sales_blocked,
# MAGIC     `Purchasing Blocked` AS purchasing_blocked,
# MAGIC     `Production Blocked` AS production_blocked
# MAGIC FROM item
# MAGIC WHERE `Type` = 'Inventory'
# MAGIC   AND COALESCE(`Blocked`, FALSE) = FALSE;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 2: Calculate Current On-Hand Inventory by Item and Location
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_inventory_on_hand AS
# MAGIC SELECT
# MAGIC     `Item No.` AS item_no,
# MAGIC     COALESCE(`Location Code`, '') AS location_code,
# MAGIC     COALESCE(`Variant Code`, '') AS variant_code,
# MAGIC     SUM(`Remaining Quantity`) AS on_hand_quantity,
# MAGIC     SUM(`Quantity`) AS total_quantity,
# MAGIC     MAX(`Posting Date`) AS last_movement_date,
# MAGIC     COUNT(*) AS transaction_count
# MAGIC FROM item_ledger_entry
# MAGIC WHERE `Open` = 1
# MAGIC   AND `Remaining Quantity` <> 0
# MAGIC GROUP BY `Item No.`, `Location Code`, `Variant Code`;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 3: Aggregate Demand from All Sources
# MAGIC -- FIXED: Production component demand = REMAINING qty (not consumed qty)
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_demand_all_sources AS
# MAGIC -- Sales Orders Demand
# MAGIC SELECT
# MAGIC     'SALES' AS demand_source,
# MAGIC     `No.` AS item_no,
# MAGIC     COALESCE(`Location Code`, '') AS location_code,
# MAGIC     COALESCE(`Variant Code`, '') AS variant_code,
# MAGIC     `Shipment Date` AS required_date,
# MAGIC     `Outstanding Quantity` AS demand_quantity,
# MAGIC     `Outstanding Qty. (Base)` AS demand_quantity_base,
# MAGIC     `Quantity` AS original_quantity,
# MAGIC     `Document Type` AS document_type,
# MAGIC     `Document No.` AS document_no,
# MAGIC     CAST(`Line No.` AS STRING) AS line_no,
# MAGIC     `Sell-to Customer No.` AS source_no,
# MAGIC     `Planned Shipment Date` AS planned_date,
# MAGIC     `Unit of Measure Code` AS unit_of_measure
# MAGIC FROM sales_line
# MAGIC WHERE `Type` = 'Item'
# MAGIC   AND `Outstanding Quantity` > 0
# MAGIC   AND `Document Type` IN ('Order', 'Invoice')
# MAGIC   AND COALESCE(`Completely Shipped`, FALSE) = FALSE
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC -- Production Order Components (Dependent Demand) - FIXED
# MAGIC -- Demand = REMAINING qty (what still needs to be consumed)
# MAGIC SELECT
# MAGIC     'PROD_COMP' AS demand_source,
# MAGIC     `Item No.` AS item_no,
# MAGIC     COALESCE(`Location Code`, '') AS location_code,
# MAGIC     COALESCE(`Variant Code`, '') AS variant_code,
# MAGIC     `Due Date` AS required_date,
# MAGIC     `Remaining Quantity` AS demand_quantity,  -- ✅ FIXED: Use remaining, not consumed
# MAGIC     `Remaining Qty. (Base)` AS demand_quantity_base,
# MAGIC     `Expected Quantity` AS original_quantity,
# MAGIC     `Status` AS document_type,
# MAGIC     `Prod. Order No.` AS document_no,
# MAGIC     CAST(`Line No.` AS STRING) AS line_no,
# MAGIC     `Item No.` AS source_no,
# MAGIC     `Due Date` AS planned_date,
# MAGIC     `Unit of Measure Code` AS unit_of_measure
# MAGIC FROM prod_order_component
# MAGIC WHERE `Status` IN ('Planned', 'Firm Planned', 'Released')
# MAGIC   AND `Remaining Quantity` > 0
# MAGIC   AND COALESCE(`Completely Picked`, FALSE) = FALSE;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 4: Aggregate Supply from All Sources
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_supply_all_sources AS
# MAGIC -- Purchase Orders Supply
# MAGIC SELECT
# MAGIC     'PURCHASE' AS supply_source,
# MAGIC     `No.` AS item_no,
# MAGIC     COALESCE(`Location Code`, '') AS location_code,
# MAGIC     COALESCE(`Variant Code`, '') AS variant_code,
# MAGIC     `Expected Receipt Date` AS receipt_date,
# MAGIC     `Outstanding Quantity` AS supply_quantity,
# MAGIC     `Outstanding Qty. (Base)` AS supply_quantity_base,
# MAGIC     `Quantity` AS original_quantity,
# MAGIC     `Document Type` AS document_type,
# MAGIC     `Document No.` AS document_no,
# MAGIC     CAST(`Line No.` AS STRING) AS line_no,
# MAGIC     COALESCE(`Planning Flexibility`, 'Unlimited') AS planning_flexibility,
# MAGIC     `Buy-from Vendor No.` AS source_no,
# MAGIC     `Planned Receipt Date` AS planned_date,
# MAGIC     `Unit of Measure Code` AS unit_of_measure
# MAGIC FROM purchase_line
# MAGIC WHERE `Type` = 'Item'
# MAGIC   AND `Outstanding Quantity` > 0
# MAGIC   AND `Document Type` IN ('Order', 'Return Order')
# MAGIC   AND COALESCE(`Completely Received`, FALSE) = FALSE
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC -- Production Orders Supply
# MAGIC SELECT
# MAGIC     'PRODUCTION' AS supply_source,
# MAGIC     `Item No.` AS item_no,
# MAGIC     COALESCE(`Location Code`, '') AS location_code,
# MAGIC     COALESCE(`Variant Code`, '') AS variant_code,
# MAGIC     `Due Date` AS receipt_date,
# MAGIC     `Remaining Quantity` AS supply_quantity,
# MAGIC     `Remaining Qty. (Base)` AS supply_quantity_base,
# MAGIC     `Quantity` AS original_quantity,
# MAGIC     `Status` AS document_type,
# MAGIC     `Prod. Order No.` AS document_no,
# MAGIC     CAST(`Line No.` AS STRING) AS line_no,
# MAGIC     COALESCE(`Planning Flexibility`, 'Unlimited') AS planning_flexibility,
# MAGIC     `Item No.` AS source_no,
# MAGIC     `Starting Date` AS planned_date,
# MAGIC     `Unit of Measure Code` AS unit_of_measure
# MAGIC FROM prod_order_line
# MAGIC WHERE `Status` IN ('Planned', 'Firm Planned', 'Released')
# MAGIC   AND `Remaining Quantity` > 0;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 5: Calculate Net Requirements by Period (Daily Buckets)
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_net_requirements AS
# MAGIC WITH date_range AS (
# MAGIC     SELECT EXPLODE(SEQUENCE(
# MAGIC         CURRENT_DATE,
# MAGIC         DATE_ADD(CURRENT_DATE, 365),
# MAGIC         INTERVAL 1 DAY
# MAGIC     )) AS planning_date
# MAGIC ),
# MAGIC item_location_combinations AS (
# MAGIC     SELECT DISTINCT
# MAGIC         item_no,
# MAGIC         '' AS location_code
# MAGIC     FROM vw_item_planning_params
# MAGIC ),
# MAGIC planning_grid AS (
# MAGIC     SELECT
# MAGIC         il.item_no,
# MAGIC         il.location_code,
# MAGIC         dr.planning_date
# MAGIC     FROM item_location_combinations il
# MAGIC     CROSS JOIN date_range dr
# MAGIC ),
# MAGIC daily_demand AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         location_code,
# MAGIC         required_date AS planning_date,
# MAGIC         SUM(demand_quantity) AS total_demand,
# MAGIC         SUM(demand_quantity_base) AS total_demand_base,
# MAGIC         SUM(original_quantity) AS original_demand,
# MAGIC         COUNT(*) AS demand_line_count
# MAGIC     FROM vw_demand_all_sources
# MAGIC     WHERE required_date >= CURRENT_DATE
# MAGIC     GROUP BY item_no, location_code, required_date
# MAGIC ),
# MAGIC daily_supply AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         location_code,
# MAGIC         receipt_date AS planning_date,
# MAGIC         SUM(supply_quantity) AS total_supply,
# MAGIC         SUM(supply_quantity_base) AS total_supply_base,
# MAGIC         SUM(original_quantity) AS original_supply,
# MAGIC         COUNT(*) AS supply_line_count
# MAGIC     FROM vw_supply_all_sources
# MAGIC     WHERE receipt_date >= CURRENT_DATE
# MAGIC     GROUP BY item_no, location_code, receipt_date
# MAGIC )
# MAGIC SELECT
# MAGIC     pg.item_no,
# MAGIC     pg.location_code,
# MAGIC     pg.planning_date,
# MAGIC     COALESCE(dd.total_demand, 0) AS demand,
# MAGIC     COALESCE(dd.total_demand_base, 0) AS demand_base,
# MAGIC     COALESCE(dd.original_demand, 0) AS original_demand,
# MAGIC     COALESCE(ds.total_supply, 0) AS supply,
# MAGIC     COALESCE(ds.total_supply_base, 0) AS supply_base,
# MAGIC     COALESCE(ds.original_supply, 0) AS original_supply,
# MAGIC     COALESCE(inv.on_hand_quantity, 0) AS starting_inventory,
# MAGIC     COALESCE(p.safety_stock_quantity, 0) AS safety_stock_quantity,
# MAGIC     COALESCE(p.reorder_point, 0) AS reorder_point,
# MAGIC     p.reordering_policy,
# MAGIC     p.replenishment_system,
# MAGIC     p.lead_time_calculation,
# MAGIC     COALESCE(p.reorder_quantity, 0) AS reorder_quantity,
# MAGIC     COALESCE(p.maximum_inventory, 0) AS maximum_inventory,
# MAGIC     COALESCE(p.minimum_order_quantity, 0) AS minimum_order_quantity,
# MAGIC     COALESCE(p.maximum_order_quantity, 0) AS maximum_order_quantity,
# MAGIC     COALESCE(p.order_multiple, 1) AS order_multiple,
# MAGIC     COALESCE(p.lot_size, 0) AS lot_size,
# MAGIC     p.manufacturing_policy,
# MAGIC     p.time_bucket,
# MAGIC     p.dampener_period,
# MAGIC     COALESCE(p.dampener_quantity, 0) AS dampener_quantity,
# MAGIC     COALESCE(p.low_level_code, 0) AS low_level_code,
# MAGIC     p.production_bom_no,
# MAGIC     p.routing_no
# MAGIC FROM planning_grid pg
# MAGIC LEFT JOIN daily_demand dd
# MAGIC     ON pg.item_no = dd.item_no
# MAGIC    AND pg.location_code = dd.location_code
# MAGIC    AND pg.planning_date = dd.planning_date
# MAGIC LEFT JOIN daily_supply ds
# MAGIC     ON pg.item_no = ds.item_no
# MAGIC    AND pg.location_code = ds.location_code
# MAGIC    AND pg.planning_date = ds.planning_date
# MAGIC LEFT JOIN vw_inventory_on_hand inv
# MAGIC     ON pg.item_no = inv.item_no
# MAGIC    AND pg.location_code = inv.location_code
# MAGIC LEFT JOIN vw_item_planning_params p
# MAGIC     ON pg.item_no = p.item_no
# MAGIC WHERE COALESCE(dd.total_demand, 0) > 0
# MAGIC    OR COALESCE(ds.total_supply, 0) > 0
# MAGIC    OR pg.planning_date = CURRENT_DATE;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 6: Calculate Running Projected Inventory with Window Functions
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_projected_inventory AS
# MAGIC SELECT
# MAGIC     item_no,
# MAGIC     location_code,
# MAGIC     planning_date,
# MAGIC     demand,
# MAGIC     demand_base,
# MAGIC     original_demand,
# MAGIC     supply,
# MAGIC     supply_base,
# MAGIC     original_supply,
# MAGIC     starting_inventory,
# MAGIC     safety_stock_quantity,
# MAGIC     reorder_point,
# MAGIC     reordering_policy,
# MAGIC     replenishment_system,
# MAGIC     lead_time_calculation,
# MAGIC     reorder_quantity,
# MAGIC     maximum_inventory,
# MAGIC     minimum_order_quantity,
# MAGIC     maximum_order_quantity,
# MAGIC     order_multiple,
# MAGIC     lot_size,
# MAGIC     manufacturing_policy,
# MAGIC     time_bucket,
# MAGIC     dampener_period,
# MAGIC     dampener_quantity,
# MAGIC     low_level_code,
# MAGIC     production_bom_no,
# MAGIC     routing_no,
# MAGIC     (supply - demand) AS daily_net_change,
# MAGIC     SUM(supply - demand) OVER (
# MAGIC         PARTITION BY item_no, location_code
# MAGIC         ORDER BY planning_date
# MAGIC         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) AS cumulative_net_change,
# MAGIC     starting_inventory + SUM(supply - demand) OVER (
# MAGIC         PARTITION BY item_no, location_code
# MAGIC         ORDER BY planning_date
# MAGIC         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) AS projected_inventory
# MAGIC FROM vw_net_requirements;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 7: Identify Planning Actions with CORRECT Reordering Policy Logic
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_planning_actions AS
# MAGIC SELECT
# MAGIC     item_no,
# MAGIC     location_code,
# MAGIC     planning_date,
# MAGIC     demand,
# MAGIC     supply,
# MAGIC     original_demand,
# MAGIC     original_supply,
# MAGIC     projected_inventory,
# MAGIC     safety_stock_quantity,
# MAGIC     reorder_point,
# MAGIC     reordering_policy,
# MAGIC     replenishment_system,
# MAGIC     lead_time_calculation,
# MAGIC     reorder_quantity,
# MAGIC     maximum_inventory,
# MAGIC     minimum_order_quantity,
# MAGIC     maximum_order_quantity,
# MAGIC     order_multiple,
# MAGIC     lot_size,
# MAGIC     manufacturing_policy,
# MAGIC     production_bom_no,
# MAGIC     routing_no,
# MAGIC     low_level_code,
# MAGIC     
# MAGIC     -- Inventory Status
# MAGIC     CASE
# MAGIC         WHEN projected_inventory < safety_stock_quantity THEN 'CRITICAL_SHORTAGE'
# MAGIC         WHEN projected_inventory < reorder_point THEN 'BELOW_REORDER_POINT'
# MAGIC         WHEN projected_inventory > maximum_inventory AND maximum_inventory > 0 THEN 'EXCESS_INVENTORY'
# MAGIC         WHEN demand > 0 OR supply > 0 THEN 'OK'
# MAGIC         ELSE 'NO_ACTIVITY'
# MAGIC     END AS inventory_status,
# MAGIC     
# MAGIC     -- Variance Quantity
# MAGIC     CASE
# MAGIC         WHEN projected_inventory < reorder_point THEN reorder_point - projected_inventory
# MAGIC         WHEN projected_inventory > maximum_inventory AND maximum_inventory > 0 THEN projected_inventory - maximum_inventory
# MAGIC         ELSE 0
# MAGIC     END AS variance_quantity,
# MAGIC     
# MAGIC     -- ========================================================================
# MAGIC     -- REORDERING POLICY LOGIC - FULLY EXPLAINED
# MAGIC     -- ========================================================================
# MAGIC     CASE reordering_policy
# MAGIC         -- ====================================================================
# MAGIC         -- 1. FIXED REORDER QTY
# MAGIC         -- Always order the same fixed amount when you hit reorder point
# MAGIC         -- Example: Reorder Point = 100, Reorder Qty = 50
# MAGIC         --          When inventory drops below 100, order exactly 50
# MAGIC         -- ====================================================================
# MAGIC         WHEN 'Fixed Reorder Qty.' THEN 
# MAGIC             CASE 
# MAGIC                 WHEN projected_inventory < reorder_point THEN reorder_quantity
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         
# MAGIC         -- ====================================================================
# MAGIC         -- 2. MAXIMUM QTY
# MAGIC         -- Order enough to bring inventory up to maximum level
# MAGIC         -- Example: Max Inventory = 500, Current = 200
# MAGIC         --          Order = 500 - 200 = 300
# MAGIC         -- ====================================================================
# MAGIC         WHEN 'Maximum Qty.' THEN 
# MAGIC             CASE
# MAGIC                 WHEN projected_inventory < reorder_point THEN
# MAGIC                     GREATEST(maximum_inventory - projected_inventory, 0)
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         
# MAGIC         -- ====================================================================
# MAGIC         -- 3. ORDER (Fixed Order Qty)
# MAGIC         -- Order enough to reach reorder point (cover the shortage)
# MAGIC         -- Example: Reorder Point = 150, Current = 80
# MAGIC         --          Order = 150 - 80 = 70
# MAGIC         -- ====================================================================
# MAGIC         WHEN 'Order' THEN
# MAGIC             CASE
# MAGIC                 WHEN projected_inventory < reorder_point THEN 
# MAGIC                     reorder_point - projected_inventory
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         
# MAGIC         -- ====================================================================
# MAGIC         -- 4. LOT-FOR-LOT
# MAGIC         -- Order exactly what's needed for demand (just-in-time)
# MAGIC         -- Example: Demand = 75, Current Inventory = 50
# MAGIC         --          Order = 75 (or 25 if we count inventory)
# MAGIC         -- This is the most responsive but creates many small orders
# MAGIC         -- ====================================================================
# MAGIC         WHEN 'Lot-for-Lot' THEN
# MAGIC             CASE
# MAGIC                 WHEN projected_inventory < safety_stock_quantity THEN
# MAGIC                     -- Cover both the shortage AND the upcoming demand
# MAGIC                     GREATEST(demand + (safety_stock_quantity - projected_inventory), 0)
# MAGIC                 WHEN demand > 0 AND projected_inventory < reorder_point THEN
# MAGIC                     -- Just cover the demand
# MAGIC                     demand
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         
# MAGIC         -- ====================================================================
# MAGIC         -- DEFAULT: Use Fixed Reorder Qty if policy not recognized
# MAGIC         -- ====================================================================
# MAGIC         ELSE 
# MAGIC             CASE 
# MAGIC                 WHEN projected_inventory < reorder_point THEN reorder_quantity
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC     END AS suggested_order_quantity_raw
# MAGIC 
# MAGIC FROM vw_projected_inventory;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 8: Apply Lot Sizing Rules and Generate Final Planning Actions
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_final_planning_actions AS
# MAGIC SELECT
# MAGIC     item_no,
# MAGIC     location_code,
# MAGIC     planning_date AS action_date,
# MAGIC     DATE_SUB(
# MAGIC         planning_date,
# MAGIC         CAST(COALESCE(REGEXP_REPLACE(lead_time_calculation, '[^0-9]', ''), '0') AS INT)
# MAGIC     ) AS suggested_order_date,
# MAGIC     inventory_status,
# MAGIC     projected_inventory,
# MAGIC     safety_stock_quantity,
# MAGIC     reorder_point,
# MAGIC     reorder_quantity,
# MAGIC     variance_quantity,
# MAGIC     suggested_order_quantity_raw,
# MAGIC     original_demand,
# MAGIC     original_supply,
# MAGIC     
# MAGIC     -- Apply minimum order quantity
# MAGIC     GREATEST(suggested_order_quantity_raw, minimum_order_quantity) AS qty_after_minimum,
# MAGIC     
# MAGIC     -- Apply maximum order quantity (if set)
# MAGIC     CASE
# MAGIC         WHEN maximum_order_quantity > 0 THEN
# MAGIC             LEAST(GREATEST(suggested_order_quantity_raw, minimum_order_quantity), maximum_order_quantity)
# MAGIC         ELSE GREATEST(suggested_order_quantity_raw, minimum_order_quantity)
# MAGIC     END AS qty_after_max_min,
# MAGIC     
# MAGIC     -- Apply order multiple (round up to nearest multiple)
# MAGIC     CASE
# MAGIC         WHEN order_multiple > 1 THEN
# MAGIC             CEIL(
# MAGIC                 CASE
# MAGIC                     WHEN maximum_order_quantity > 0 THEN
# MAGIC                         LEAST(GREATEST(suggested_order_quantity_raw, minimum_order_quantity), maximum_order_quantity)
# MAGIC                     ELSE GREATEST(suggested_order_quantity_raw, minimum_order_quantity)
# MAGIC                 END / order_multiple
# MAGIC             ) * order_multiple
# MAGIC         ELSE
# MAGIC             CASE
# MAGIC                 WHEN maximum_order_quantity > 0 THEN
# MAGIC                     LEAST(GREATEST(suggested_order_quantity_raw, minimum_order_quantity), maximum_order_quantity)
# MAGIC                 ELSE GREATEST(suggested_order_quantity_raw, minimum_order_quantity)
# MAGIC             END
# MAGIC     END AS final_suggested_quantity,
# MAGIC     
# MAGIC     replenishment_system,
# MAGIC     reordering_policy,
# MAGIC     lead_time_calculation,
# MAGIC     manufacturing_policy,
# MAGIC     production_bom_no,
# MAGIC     routing_no,
# MAGIC     low_level_code,
# MAGIC     
# MAGIC     CASE
# MAGIC         WHEN inventory_status IN ('CRITICAL_SHORTAGE', 'BELOW_REORDER_POINT') THEN 'New Order Required'
# MAGIC         WHEN inventory_status = 'EXCESS_INVENTORY' THEN 'Reduce/Cancel Orders'
# MAGIC         WHEN inventory_status = 'OK' THEN 'No Action Needed'
# MAGIC         ELSE 'No Activity'
# MAGIC     END AS action_message,
# MAGIC     
# MAGIC     CURRENT_TIMESTAMP AS calculation_timestamp
# MAGIC FROM vw_planning_actions;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 9: Create Final Planning Worksheet Output
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE Silver_Planning_Lakehouse.testing.gold_mrp_planning_worksheet_fixed AS
# MAGIC WITH
# MAGIC po_best AS (
# MAGIC     SELECT
# MAGIC         pl.`No.` AS item_no,
# MAGIC         pl.`Location Code` AS location_code,
# MAGIC         COALESCE(pl.`Variant Code`, '') AS variant_code,
# MAGIC         COALESCE(pl.`Bin Code`, '') AS bin_code,
# MAGIC         COALESCE(pl.`Shortcut Dimension 1 Code`, '') AS shortcut_dimension_1_code,
# MAGIC         COALESCE(pl.`Shortcut Dimension 2 Code`, '') AS shortcut_dimension_2_code,
# MAGIC         COALESCE(pl.`Unit of Measure Code`, '') AS unit_of_measure_code,
# MAGIC         COALESCE(pl.`Qty. per Unit of Measure`, 1) AS qty_per_unit_of_measure,
# MAGIC         COALESCE(pl.`Buy-from Vendor No.`, '') AS vendor_no,
# MAGIC         COALESCE(pl.`Vendor Item No.`, '') AS vendor_item_no,
# MAGIC         COALESCE(pl.`Direct Unit Cost`, 0) AS direct_unit_cost,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY pl.`No.`
# MAGIC             ORDER BY
# MAGIC                 COALESCE(pl.`Planned Receipt Date`, pl.`Expected Receipt Date`, pl.`Order Date`) DESC,
# MAGIC                 COALESCE(pl.`SystemModifiedAt`, pl.`SystemCreatedAt`) DESC,
# MAGIC                 pl.`Line No.` DESC
# MAGIC         ) AS rn
# MAGIC     FROM purchase_line pl
# MAGIC     WHERE pl.`Type` = 'Item'
# MAGIC       AND pl.`Outstanding Quantity` > 0
# MAGIC       AND pl.`Document Type` IN ('Order', 'Return Order')
# MAGIC       AND COALESCE(pl.`Completely Received`, FALSE) = FALSE
# MAGIC ),
# MAGIC so_best AS (
# MAGIC     SELECT
# MAGIC         sl.`No.` AS item_no,
# MAGIC         sl.`Location Code` AS location_code,
# MAGIC         COALESCE(sl.`Variant Code`, '') AS variant_code,
# MAGIC         COALESCE(sl.`Bin Code`, '') AS bin_code,
# MAGIC         COALESCE(sl.`Shortcut Dimension 1 Code`, '') AS shortcut_dimension_1_code,
# MAGIC         COALESCE(sl.`Shortcut Dimension 2 Code`, '') AS shortcut_dimension_2_code,
# MAGIC         COALESCE(sl.`Unit of Measure Code`, '') AS unit_of_measure_code,
# MAGIC         COALESCE(sl.`Qty. per Unit of Measure`, 1) AS qty_per_unit_of_measure,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY sl.`No.`
# MAGIC             ORDER BY
# MAGIC                 COALESCE(sl.`Shipment Date`, sl.`Planned Shipment Date`) DESC,
# MAGIC                 COALESCE(sl.`SystemModifiedAt`, sl.`SystemCreatedAt`) DESC,
# MAGIC                 sl.`Line No.` DESC
# MAGIC         ) AS rn
# MAGIC     FROM sales_line sl
# MAGIC     WHERE sl.`Type` = 'Item'
# MAGIC       AND sl.`Outstanding Quantity` > 0
# MAGIC       AND sl.`Document Type` IN ('Order', 'Invoice')
# MAGIC       AND COALESCE(sl.`Completely Shipped`, FALSE) = FALSE
# MAGIC ),
# MAGIC pol_best AS (
# MAGIC     SELECT
# MAGIC         pol.`Item No.` AS item_no,
# MAGIC         pol.`Location Code` AS location_code,
# MAGIC         COALESCE(pol.`Variant Code`, '') AS variant_code,
# MAGIC         COALESCE(pol.`Bin Code`, '') AS bin_code,
# MAGIC         COALESCE(pol.`Shortcut Dimension 1 Code`, '') AS shortcut_dimension_1_code,
# MAGIC         COALESCE(pol.`Shortcut Dimension 2 Code`, '') AS shortcut_dimension_2_code,
# MAGIC         COALESCE(pol.`Unit of Measure Code`, '') AS unit_of_measure_code,
# MAGIC         COALESCE(pol.`Qty. per Unit of Measure`, 1) AS qty_per_unit_of_measure,
# MAGIC         COALESCE(pol.`Production BOM No.`, '') AS production_bom_no,
# MAGIC         COALESCE(pol.`Routing No.`, '') AS routing_no,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY pol.`Item No.`
# MAGIC             ORDER BY
# MAGIC                 COALESCE(pol.`Due Date`, pol.`Starting Date`) DESC,
# MAGIC                 COALESCE(pol.`SystemModifiedAt`, pol.`SystemCreatedAt`) DESC,
# MAGIC                 pol.`Line No.` DESC
# MAGIC         ) AS rn
# MAGIC     FROM prod_order_line pol
# MAGIC     WHERE pol.`Status` IN ('Planned', 'Firm Planned', 'Released')
# MAGIC       AND pol.`Remaining Quantity` > 0
# MAGIC ),
# MAGIC item_defaults AS (
# MAGIC     SELECT
# MAGIC         i.`No.` AS item_no,
# MAGIC         COALESCE(i.`Description`, '') AS description,
# MAGIC         COALESCE(i.`Description 2`, '') AS description_2,
# MAGIC 
# MAGIC         COALESCE(po.location_code, so.location_code, pol.location_code, '') AS default_location_code,
# MAGIC         COALESCE(po.variant_code,   so.variant_code,   pol.variant_code,   '') AS default_variant_code,
# MAGIC         COALESCE(po.bin_code,       so.bin_code,       pol.bin_code,       '') AS default_bin_code,
# MAGIC         COALESCE(po.shortcut_dimension_1_code, so.shortcut_dimension_1_code, pol.shortcut_dimension_1_code, '') AS shortcut_dimension_1_code,
# MAGIC         COALESCE(po.shortcut_dimension_2_code, so.shortcut_dimension_2_code, pol.shortcut_dimension_2_code, '') AS shortcut_dimension_2_code,
# MAGIC 
# MAGIC         COALESCE(po.unit_of_measure_code, so.unit_of_measure_code, pol.unit_of_measure_code, i.`Base Unit of Measure`, '') AS unit_of_measure_code,
# MAGIC         COALESCE(po.qty_per_unit_of_measure, so.qty_per_unit_of_measure, pol.qty_per_unit_of_measure, 1) AS qty_per_unit_of_measure,
# MAGIC 
# MAGIC         COALESCE(po.vendor_no, i.`Vendor No.`, '') AS vendor_no,
# MAGIC         COALESCE(NULLIF(po.vendor_item_no,''), i.`Vendor Item No.`, '') AS vendor_item_no,
# MAGIC         COALESCE(po.direct_unit_cost, 0) AS direct_unit_cost,
# MAGIC 
# MAGIC         COALESCE(pol.production_bom_no, i.`Production BOM No.`, '') AS fallback_production_bom_no,
# MAGIC         COALESCE(pol.routing_no,        i.`Routing No.`, '')        AS fallback_routing_no,
# MAGIC 
# MAGIC         COALESCE(i.`Item Category Code`, '') AS item_category_code,
# MAGIC         COALESCE(i.`Purchasing Code`, '') AS purchasing_code,
# MAGIC         COALESCE(i.`Metal Category Code`, '') AS metal_category_code,
# MAGIC         COALESCE(i.`Product Type`, '') AS product_type
# MAGIC 
# MAGIC     FROM item i
# MAGIC     LEFT JOIN (SELECT * FROM po_best WHERE rn = 1) po
# MAGIC         ON po.item_no = i.`No.`
# MAGIC     LEFT JOIN (SELECT * FROM so_best WHERE rn = 1) so
# MAGIC         ON so.item_no = i.`No.`
# MAGIC     LEFT JOIN (SELECT * FROM pol_best WHERE rn = 1) pol
# MAGIC         ON pol.item_no = i.`No.`
# MAGIC )
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY
# MAGIC         CASE fa.inventory_status
# MAGIC             WHEN 'CRITICAL_SHORTAGE' THEN 1
# MAGIC             WHEN 'BELOW_REORDER_POINT' THEN 2
# MAGIC             WHEN 'EXCESS_INVENTORY' THEN 3
# MAGIC             ELSE 4
# MAGIC         END,
# MAGIC         fa.suggested_order_date,
# MAGIC         fa.item_no
# MAGIC     ) AS line_no,
# MAGIC     fa.item_no AS no_,
# MAGIC     d.description,
# MAGIC     d.description_2,
# MAGIC 
# MAGIC     fa.inventory_status AS ref_order_status,
# MAGIC     fa.action_message,
# MAGIC     CASE
# MAGIC         WHEN fa.action_message = 'New Order Required' THEN 'TRUE'
# MAGIC         ELSE 'FALSE'
# MAGIC     END AS accept_action_message,
# MAGIC 
# MAGIC     fa.projected_inventory,
# MAGIC     fa.safety_stock_quantity,
# MAGIC     fa.reorder_point,
# MAGIC     fa.reorder_quantity,
# MAGIC     fa.variance_quantity AS shortage_qty,
# MAGIC     fa.original_demand,
# MAGIC     fa.original_supply,
# MAGIC 
# MAGIC     fa.final_suggested_quantity AS quantity,
# MAGIC     fa.final_suggested_quantity AS remaining_quantity,
# MAGIC     fa.final_suggested_quantity AS quantity_base,
# MAGIC     fa.final_suggested_quantity AS remaining_qty_base,
# MAGIC     fa.final_suggested_quantity AS original_quantity,
# MAGIC     fa.final_suggested_quantity AS net_quantity_base,
# MAGIC     0 AS finished_quantity,
# MAGIC 
# MAGIC     fa.suggested_order_date AS order_date,
# MAGIC     fa.suggested_order_date AS starting_date,
# MAGIC     fa.action_date AS due_date,
# MAGIC     fa.action_date AS ending_date,
# MAGIC     fa.action_date AS original_due_date,
# MAGIC 
# MAGIC     fa.replenishment_system,
# MAGIC     fa.replenishment_system AS ref_order_type,
# MAGIC     fa.reordering_policy,
# MAGIC     fa.reordering_policy AS planning_flexibility,
# MAGIC     fa.manufacturing_policy,
# MAGIC     fa.lead_time_calculation,
# MAGIC 
# MAGIC     fa.location_code,
# MAGIC     d.default_location_code,
# MAGIC     d.default_variant_code AS variant_code,
# MAGIC     d.default_bin_code AS bin_code,
# MAGIC     d.shortcut_dimension_1_code,
# MAGIC     d.shortcut_dimension_2_code,
# MAGIC 
# MAGIC     d.unit_of_measure_code,
# MAGIC     d.qty_per_unit_of_measure,
# MAGIC 
# MAGIC     d.vendor_no,
# MAGIC     d.vendor_item_no,
# MAGIC     d.direct_unit_cost,
# MAGIC 
# MAGIC     COALESCE(NULLIF(fa.production_bom_no,''), d.fallback_production_bom_no) AS production_bom_no,
# MAGIC     COALESCE(NULLIF(fa.routing_no,''), d.fallback_routing_no) AS routing_no,
# MAGIC     fa.low_level_code,
# MAGIC 
# MAGIC     d.item_category_code,
# MAGIC     d.metal_category_code,
# MAGIC     d.product_type,
# MAGIC     d.purchasing_code,
# MAGIC 
# MAGIC     fa.calculation_timestamp,
# MAGIC     'Generated by Fabric MRP' AS user_id
# MAGIC 
# MAGIC FROM vw_final_planning_actions fa
# MAGIC LEFT JOIN item_defaults d
# MAGIC     ON d.item_no = fa.item_no
# MAGIC ORDER BY
# MAGIC     CASE fa.inventory_status
# MAGIC         WHEN 'CRITICAL_SHORTAGE' THEN 1
# MAGIC         WHEN 'BELOW_REORDER_POINT' THEN 2
# MAGIC         WHEN 'EXCESS_INVENTORY' THEN 3
# MAGIC         ELSE 4
# MAGIC     END,
# MAGIC     fa.suggested_order_date,
# MAGIC     fa.item_no;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 10: Create Exception Report
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE Silver_Planning_Lakehouse.testing.gold_mrp_exceptions_fixed AS
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (
# MAGIC         ORDER BY
# MAGIC             CASE inventory_status
# MAGIC                 WHEN 'CRITICAL_SHORTAGE' THEN 1
# MAGIC                 WHEN 'BELOW_REORDER_POINT' THEN 2
# MAGIC                 ELSE 3
# MAGIC             END,
# MAGIC             action_date,
# MAGIC             item_no
# MAGIC     ) AS exception_no,
# MAGIC     item_no,
# MAGIC     CASE inventory_status
# MAGIC         WHEN 'CRITICAL_SHORTAGE' THEN 'High'
# MAGIC         WHEN 'BELOW_REORDER_POINT' THEN 'Medium'
# MAGIC         ELSE 'Low'
# MAGIC     END AS priority,
# MAGIC     inventory_status AS exception_type,
# MAGIC     action_message,
# MAGIC     projected_inventory,
# MAGIC     safety_stock_quantity,
# MAGIC     reorder_point,
# MAGIC     reorder_quantity,
# MAGIC     variance_quantity AS shortage_excess_qty,
# MAGIC     final_suggested_quantity AS suggested_action_qty,
# MAGIC     original_demand,
# MAGIC     original_supply,
# MAGIC     action_date,
# MAGIC     location_code,
# MAGIC     replenishment_system,
# MAGIC     reordering_policy,
# MAGIC     calculation_timestamp
# MAGIC FROM vw_final_planning_actions
# MAGIC WHERE inventory_status IN ('CRITICAL_SHORTAGE', 'BELOW_REORDER_POINT', 'EXCESS_INVENTORY')
# MAGIC ORDER BY priority DESC, action_date, item_no;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 11: Summary Statistics
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE Silver_Planning_Lakehouse.testing.gold_mrp_run_summary_fixed AS
# MAGIC SELECT
# MAGIC     CURRENT_TIMESTAMP AS run_timestamp,
# MAGIC     COUNT(DISTINCT item_no) AS items_planned,
# MAGIC     COUNT(*) AS total_planning_lines,
# MAGIC     SUM(CASE WHEN action_message = 'New Order Required' THEN 1 ELSE 0 END) AS new_orders,
# MAGIC     SUM(CASE WHEN action_message = 'Reduce/Cancel Orders' THEN 1 ELSE 0 END) AS cancel_reduce,
# MAGIC     SUM(CASE WHEN action_message = 'No Action Needed' THEN 1 ELSE 0 END) AS no_action_needed,
# MAGIC     SUM(CASE WHEN inventory_status = 'CRITICAL_SHORTAGE' THEN 1 ELSE 0 END) AS critical_shortages,
# MAGIC     SUM(CASE WHEN inventory_status = 'BELOW_REORDER_POINT' THEN 1 ELSE 0 END) AS below_reorder_point,
# MAGIC     SUM(CASE WHEN inventory_status = 'EXCESS_INVENTORY' THEN 1 ELSE 0 END) AS excess_inventory,
# MAGIC     SUM(CASE WHEN inventory_status = 'OK' THEN 1 ELSE 0 END) AS status_ok,
# MAGIC     SUM(CASE WHEN inventory_status = 'NO_ACTIVITY' THEN 1 ELSE 0 END) AS no_activity,
# MAGIC     SUM(final_suggested_quantity) AS total_quantity_to_order,
# MAGIC     MIN(suggested_order_date) AS earliest_action_date,
# MAGIC     MAX(action_date) AS latest_due_date
# MAGIC FROM vw_final_planning_actions;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC USE Silver_Planning_Lakehouse.dbo;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 1: Item Planning Parameters (from gold_consumption_stock_item)
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_item_planning_params AS
# MAGIC SELECT
# MAGIC     item_no,
# MAGIC     MAX(item_description) AS description,
# MAGIC     MAX(replenishment_system) AS replenishment_system,
# MAGIC     MAX(reordering_policy) AS reordering_policy,
# MAGIC     MAX(reorder_point) AS reorder_point,
# MAGIC     MAX(reorder_quantity) AS reorder_quantity,
# MAGIC     MAX(maximum_inventory) AS maximum_inventory,
# MAGIC     MAX(item_safety_stock_quantity) AS safety_stock_quantity,
# MAGIC     MAX(vendor_leadtime) AS lead_time_calculation,
# MAGIC     MAX(minimum_order_quantity) AS minimum_order_quantity,
# MAGIC     MAX(maximum_order_quantity) AS maximum_order_quantity,
# MAGIC     MAX(order_multiple) AS order_multiple,
# MAGIC     MAX(safety_lead_time) AS safety_lead_time,
# MAGIC     MAX(time_bucket) AS time_bucket,
# MAGIC     MAX(lot_accumulation_period) AS lot_accumulation_period,
# MAGIC     MAX(rescheduling_period) AS rescheduling_period,
# MAGIC     MAX(vendor_no) AS vendor_no,
# MAGIC     MAX(vendor_name) AS vendor_name,
# MAGIC     MAX(`type`) AS metal_type,
# MAGIC     MAX(location) AS default_location
# MAGIC FROM gold_consumption_stock_item
# MAGIC WHERE item_no IS NOT NULL
# MAGIC GROUP BY item_no;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 2: On-hand Inventory (normalize + exclude BROK-MAT)
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_inventory_on_hand AS
# MAGIC SELECT
# MAGIC     item_no,
# MAGIC     UPPER(TRIM(COALESCE(location, ''))) AS location_code,
# MAGIC     SUM(rem_uom1) AS on_hand_quantity_uom1,
# MAGIC     SUM(rem_uom2) AS on_hand_quantity_uom2,
# MAGIC     MAX(item_uom) AS uom1,
# MAGIC     MAX(item_uom2) AS uom2,
# MAGIC     SUM(rem_uom1) AS on_hand_quantity
# MAGIC FROM gold_consumption_stock_item
# MAGIC WHERE item_no IS NOT NULL
# MAGIC   AND UPPER(TRIM(COALESCE(location, ''))) <> 'BROK-MAT'
# MAGIC GROUP BY item_no, UPPER(TRIM(COALESCE(location, '')));
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 3: Demand (Sales + Production Components)
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_demand_all_sources AS
# MAGIC -- Sales Orders Demand
# MAGIC SELECT
# MAGIC     'SALES' AS demand_source,
# MAGIC     `No.` AS item_no,
# MAGIC     UPPER(TRIM(COALESCE(`Location Code`, ''))) AS location_code,
# MAGIC     COALESCE(`Variant Code`, '') AS variant_code,
# MAGIC     `Shipment Date` AS required_date,
# MAGIC     `Outstanding Quantity` AS demand_quantity,
# MAGIC     `Outstanding Qty. (Base)` AS demand_quantity_base,
# MAGIC     `Quantity` AS original_quantity,
# MAGIC     `Document Type` AS document_type,
# MAGIC     `Document No.` AS document_no,
# MAGIC     CAST(`Line No.` AS STRING) AS line_no,
# MAGIC     `Sell-to Customer No.` AS source_no,
# MAGIC     `Planned Shipment Date` AS planned_date,
# MAGIC     `Unit of Measure Code` AS unit_of_measure
# MAGIC FROM sales_line
# MAGIC WHERE `Type` = 'Item'
# MAGIC   AND `Outstanding Quantity` > 0
# MAGIC   AND `Document Type` IN ('Order', 'Invoice')
# MAGIC   AND COALESCE(`Completely Shipped`, FALSE) = FALSE
# MAGIC   AND UPPER(TRIM(COALESCE(`Location Code`, ''))) <> 'BROK-MAT'
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC -- Production Component Demand (Dependent demand) from gold_consumption_stock_item
# MAGIC SELECT
# MAGIC     'PROD_COMP' AS demand_source,
# MAGIC     ComponentItemNo AS item_no,
# MAGIC     UPPER(TRIM(COALESCE(location, ''))) AS location_code,
# MAGIC     '' AS variant_code,
# MAGIC     ComponentDueDate AS required_date,
# MAGIC     rem1 AS demand_quantity,
# MAGIC     rem1 AS demand_quantity_base,
# MAGIC     exp1 AS original_quantity,
# MAGIC     CAST(`Status` AS STRING) AS document_type,          -- ✅ FIXED
# MAGIC     ProdOrderNo AS document_no,
# MAGIC     CAST(ComponentLineNo AS STRING) AS line_no,
# MAGIC     ProdOrderItemNo AS source_no,
# MAGIC     ComponentDueDate AS planned_date,
# MAGIC     ComponentUOM AS unit_of_measure
# MAGIC FROM gold_consumption_stock_item
# MAGIC WHERE ComponentItemNo IS NOT NULL
# MAGIC   AND ComponentDueDate IS NOT NULL
# MAGIC   AND rem1 > 0
# MAGIC   AND COALESCE(CompletelyPicked, FALSE) = FALSE
# MAGIC   AND UPPER(TRIM(COALESCE(location, ''))) <> 'BROK-MAT';
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 4: Supply (Purchase + Production Orders)
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_supply_all_sources AS
# MAGIC -- Purchase Orders Supply
# MAGIC SELECT
# MAGIC     'PURCHASE' AS supply_source,
# MAGIC     `No.` AS item_no,
# MAGIC     UPPER(TRIM(COALESCE(`Location Code`, ''))) AS location_code,
# MAGIC     COALESCE(`Variant Code`, '') AS variant_code,
# MAGIC     `Expected Receipt Date` AS receipt_date,
# MAGIC     `Outstanding Quantity` AS supply_quantity,
# MAGIC     `Outstanding Qty. (Base)` AS supply_quantity_base,
# MAGIC     `Quantity` AS original_quantity,
# MAGIC     `Document Type` AS document_type,
# MAGIC     `Document No.` AS document_no,
# MAGIC     CAST(`Line No.` AS STRING) AS line_no,
# MAGIC     COALESCE(`Planning Flexibility`, 'Unlimited') AS planning_flexibility,
# MAGIC     `Buy-from Vendor No.` AS source_no,
# MAGIC     `Planned Receipt Date` AS planned_date,
# MAGIC     `Unit of Measure Code` AS unit_of_measure
# MAGIC FROM purchase_line
# MAGIC WHERE `Type` = 'Item'
# MAGIC   AND `Outstanding Quantity` > 0
# MAGIC   AND `Document Type` IN ('Order', 'Return Order')
# MAGIC   AND COALESCE(`Completely Received`, FALSE) = FALSE
# MAGIC   AND UPPER(TRIM(COALESCE(`Location Code`, ''))) <> 'BROK-MAT'
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC -- Production Orders Supply
# MAGIC SELECT
# MAGIC     'PRODUCTION' AS supply_source,
# MAGIC     `Item No.` AS item_no,
# MAGIC     UPPER(TRIM(COALESCE(`Location Code`, ''))) AS location_code,
# MAGIC     COALESCE(`Variant Code`, '') AS variant_code,
# MAGIC     `Due Date` AS receipt_date,
# MAGIC     `Remaining Quantity` AS supply_quantity,
# MAGIC     `Remaining Qty. (Base)` AS supply_quantity_base,
# MAGIC     `Quantity` AS original_quantity,
# MAGIC     `Status` AS document_type,
# MAGIC     `Prod. Order No.` AS document_no,
# MAGIC     CAST(`Line No.` AS STRING) AS line_no,
# MAGIC     COALESCE(`Planning Flexibility`, 'Unlimited') AS planning_flexibility,
# MAGIC     `Item No.` AS source_no,
# MAGIC     `Starting Date` AS planned_date,
# MAGIC     `Unit of Measure Code` AS unit_of_measure
# MAGIC FROM prod_order_line
# MAGIC WHERE `Status` IN ('Planned', 'Firm Planned', 'Released')
# MAGIC   AND `Remaining Quantity` > 0
# MAGIC   AND UPPER(TRIM(COALESCE(`Location Code`, ''))) <> 'BROK-MAT';
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 5: Net Requirements by Week
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_net_requirements AS
# MAGIC WITH date_range AS (
# MAGIC     SELECT EXPLODE(SEQUENCE(
# MAGIC         DATE_TRUNC('week', CURRENT_DATE),
# MAGIC         DATE_ADD(DATE_TRUNC('week', CURRENT_DATE), 364),
# MAGIC         INTERVAL 7 DAY
# MAGIC     )) AS planning_week_start
# MAGIC ),
# MAGIC weekly_periods AS (
# MAGIC     SELECT
# MAGIC         planning_week_start,
# MAGIC         DATE_ADD(planning_week_start, 6) AS planning_week_end,
# MAGIC         CONCAT('W', WEEKOFYEAR(planning_week_start), '-', YEAR(planning_week_start)) AS week_label
# MAGIC     FROM date_range
# MAGIC ),
# MAGIC item_location_combinations AS (
# MAGIC     SELECT item_no, location_code FROM vw_inventory_on_hand
# MAGIC     UNION
# MAGIC     SELECT item_no, location_code FROM vw_demand_all_sources
# MAGIC     UNION
# MAGIC     SELECT item_no, location_code FROM vw_supply_all_sources
# MAGIC ),
# MAGIC planning_grid AS (
# MAGIC     SELECT
# MAGIC         il.item_no,
# MAGIC         il.location_code,
# MAGIC         wp.planning_week_start,
# MAGIC         wp.planning_week_end,
# MAGIC         wp.week_label
# MAGIC     FROM item_location_combinations il
# MAGIC     CROSS JOIN weekly_periods wp
# MAGIC     WHERE il.location_code <> 'BROK-MAT'
# MAGIC ),
# MAGIC weekly_demand AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         location_code,
# MAGIC         DATE_TRUNC('week', required_date) AS planning_week_start,
# MAGIC         SUM(demand_quantity) AS total_demand,
# MAGIC         SUM(demand_quantity_base) AS total_demand_base,
# MAGIC         SUM(original_quantity) AS original_demand,
# MAGIC         COUNT(*) AS demand_line_count,
# MAGIC         COUNT(CASE WHEN demand_source = 'SALES' THEN 1 END) AS sales_lines,
# MAGIC         COUNT(CASE WHEN demand_source = 'PROD_COMP' THEN 1 END) AS prod_comp_lines,
# MAGIC         MIN(required_date) AS earliest_demand_date,
# MAGIC         MAX(required_date) AS latest_demand_date
# MAGIC     FROM vw_demand_all_sources
# MAGIC     WHERE required_date >= CURRENT_DATE
# MAGIC     GROUP BY item_no, location_code, DATE_TRUNC('week', required_date)
# MAGIC ),
# MAGIC weekly_supply AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         location_code,
# MAGIC         DATE_TRUNC('week', receipt_date) AS planning_week_start,
# MAGIC         SUM(supply_quantity) AS total_supply,
# MAGIC         SUM(supply_quantity_base) AS total_supply_base,
# MAGIC         SUM(original_quantity) AS original_supply,
# MAGIC         COUNT(*) AS supply_line_count,
# MAGIC         COUNT(CASE WHEN supply_source = 'PURCHASE' THEN 1 END) AS purchase_lines,
# MAGIC         COUNT(CASE WHEN supply_source = 'PRODUCTION' THEN 1 END) AS production_lines,
# MAGIC         MIN(receipt_date) AS earliest_receipt_date,
# MAGIC         MAX(receipt_date) AS latest_receipt_date
# MAGIC     FROM vw_supply_all_sources
# MAGIC     WHERE receipt_date >= CURRENT_DATE
# MAGIC     GROUP BY item_no, location_code, DATE_TRUNC('week', receipt_date)
# MAGIC )
# MAGIC SELECT
# MAGIC     pg.item_no,
# MAGIC     pg.location_code,
# MAGIC     pg.planning_week_start,
# MAGIC     pg.planning_week_end,
# MAGIC     pg.week_label,
# MAGIC 
# MAGIC     COALESCE(wd.total_demand, 0) AS demand,
# MAGIC     COALESCE(wd.total_demand_base, 0) AS demand_base,
# MAGIC     COALESCE(wd.original_demand, 0) AS original_demand,
# MAGIC     COALESCE(wd.sales_lines, 0) AS sales_demand_lines,
# MAGIC     COALESCE(wd.prod_comp_lines, 0) AS prod_comp_demand_lines,
# MAGIC     COALESCE(wd.earliest_demand_date, pg.planning_week_start) AS earliest_demand_date,
# MAGIC 
# MAGIC     COALESCE(ws.total_supply, 0) AS supply,
# MAGIC     COALESCE(ws.total_supply_base, 0) AS supply_base,
# MAGIC     COALESCE(ws.original_supply, 0) AS original_supply,
# MAGIC     COALESCE(ws.purchase_lines, 0) AS purchase_supply_lines,
# MAGIC     COALESCE(ws.production_lines, 0) AS production_supply_lines,
# MAGIC     COALESCE(ws.earliest_receipt_date, pg.planning_week_start) AS earliest_receipt_date,
# MAGIC 
# MAGIC     COALESCE(inv.on_hand_quantity, 0) AS starting_inventory,
# MAGIC 
# MAGIC     COALESCE(p.safety_stock_quantity, 0) AS safety_stock_quantity,
# MAGIC     COALESCE(p.reorder_point, 0) AS reorder_point,
# MAGIC     p.reordering_policy,
# MAGIC     p.replenishment_system,
# MAGIC     p.lead_time_calculation,
# MAGIC     COALESCE(p.reorder_quantity, 0) AS reorder_quantity,
# MAGIC     COALESCE(p.maximum_inventory, 0) AS maximum_inventory,
# MAGIC     COALESCE(p.minimum_order_quantity, 0) AS minimum_order_quantity,
# MAGIC     COALESCE(p.maximum_order_quantity, 0) AS maximum_order_quantity,
# MAGIC     COALESCE(NULLIF(p.order_multiple, 0), 1) AS order_multiple,
# MAGIC     p.time_bucket,
# MAGIC     p.lot_accumulation_period,
# MAGIC     p.rescheduling_period,
# MAGIC     p.vendor_no,
# MAGIC     p.vendor_name,
# MAGIC     p.metal_type,
# MAGIC     p.default_location
# MAGIC FROM planning_grid pg
# MAGIC LEFT JOIN weekly_demand wd
# MAGIC     ON pg.item_no = wd.item_no
# MAGIC    AND pg.location_code = wd.location_code
# MAGIC    AND pg.planning_week_start = wd.planning_week_start
# MAGIC LEFT JOIN weekly_supply ws
# MAGIC     ON pg.item_no = ws.item_no
# MAGIC    AND pg.location_code = ws.location_code
# MAGIC    AND pg.planning_week_start = ws.planning_week_start
# MAGIC LEFT JOIN vw_inventory_on_hand inv
# MAGIC     ON pg.item_no = inv.item_no
# MAGIC    AND pg.location_code = inv.location_code
# MAGIC LEFT JOIN vw_item_planning_params p
# MAGIC     ON pg.item_no = p.item_no
# MAGIC WHERE COALESCE(wd.total_demand, 0) > 0
# MAGIC    OR COALESCE(ws.total_supply, 0) > 0
# MAGIC    OR COALESCE(inv.on_hand_quantity, 0) <> 0
# MAGIC    OR pg.planning_week_start = DATE_TRUNC('week', CURRENT_DATE);
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 6: Projected Inventory (BOW/EOW)
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_projected_inventory AS
# MAGIC WITH x AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         (supply - demand) AS weekly_net_change,
# MAGIC         SUM(supply - demand) OVER (
# MAGIC             PARTITION BY item_no, location_code
# MAGIC             ORDER BY planning_week_start
# MAGIC             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC         ) AS cumulative_net_change
# MAGIC     FROM vw_net_requirements
# MAGIC )
# MAGIC SELECT
# MAGIC     *,
# MAGIC     starting_inventory + cumulative_net_change AS projected_inventory_eow,
# MAGIC     (starting_inventory + cumulative_net_change) - weekly_net_change AS projected_inventory_bow
# MAGIC FROM x;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 7: Policy Logic (trigger on BOW)
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_planning_actions AS
# MAGIC SELECT
# MAGIC     *,
# MAGIC     CASE
# MAGIC         WHEN projected_inventory_bow < safety_stock_quantity THEN 'CRITICAL_SHORTAGE'
# MAGIC         WHEN projected_inventory_bow < reorder_point THEN 'BELOW_REORDER_POINT'
# MAGIC         WHEN projected_inventory_bow > maximum_inventory AND maximum_inventory > 0 THEN 'EXCESS_INVENTORY'
# MAGIC         WHEN demand > 0 OR supply > 0 THEN 'OK'
# MAGIC         ELSE 'NO_ACTIVITY'
# MAGIC     END AS inventory_status,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN projected_inventory_bow < reorder_point THEN reorder_point - projected_inventory_bow
# MAGIC         WHEN projected_inventory_bow > maximum_inventory AND maximum_inventory > 0 THEN projected_inventory_bow - maximum_inventory
# MAGIC         ELSE 0
# MAGIC     END AS variance_quantity,
# MAGIC 
# MAGIC     CASE reordering_policy
# MAGIC         WHEN 'Fixed Reorder Qty.' THEN CASE WHEN projected_inventory_bow < reorder_point THEN reorder_quantity ELSE 0 END
# MAGIC         WHEN 'Maximum Qty.' THEN CASE WHEN projected_inventory_bow < reorder_point THEN GREATEST(maximum_inventory - projected_inventory_bow, 0) ELSE 0 END
# MAGIC         WHEN 'Order' THEN CASE WHEN projected_inventory_bow < reorder_point THEN reorder_point - projected_inventory_bow ELSE 0 END
# MAGIC         WHEN 'Lot-for-Lot' THEN
# MAGIC             CASE
# MAGIC                 WHEN projected_inventory_bow < safety_stock_quantity THEN GREATEST(demand + (safety_stock_quantity - projected_inventory_bow), 0)
# MAGIC                 WHEN demand > 0 AND projected_inventory_bow < reorder_point THEN demand
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ELSE CASE WHEN projected_inventory_bow < reorder_point THEN reorder_quantity ELSE 0 END
# MAGIC     END AS suggested_order_quantity_raw
# MAGIC FROM vw_projected_inventory;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 8: Lot sizing + action message (adds qty_after_multiple, qty_after_maxinv)
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_final_planning_actions AS
# MAGIC WITH base AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CAST(COALESCE(NULLIF(REGEXP_REPLACE(lead_time_calculation, '[^0-9]', ''), ''), '0') AS INT) AS lead_time_days
# MAGIC     FROM vw_planning_actions
# MAGIC ),
# MAGIC a AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         earliest_demand_date AS action_date,
# MAGIC         DATE_SUB(planning_week_start, lead_time_days) AS suggested_order_date,
# MAGIC         GREATEST(suggested_order_quantity_raw, minimum_order_quantity) AS qty_after_minimum,
# MAGIC         CASE
# MAGIC             WHEN maximum_order_quantity > 0 THEN LEAST(GREATEST(suggested_order_quantity_raw, minimum_order_quantity), maximum_order_quantity)
# MAGIC             ELSE GREATEST(suggested_order_quantity_raw, minimum_order_quantity)
# MAGIC         END AS qty_after_max_min
# MAGIC     FROM base
# MAGIC ),
# MAGIC b AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CASE WHEN order_multiple > 1 THEN CEIL(qty_after_max_min / order_multiple) * order_multiple ELSE qty_after_max_min END AS qty_after_multiple
# MAGIC     FROM a
# MAGIC ),
# MAGIC c AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CASE
# MAGIC             WHEN maximum_inventory > 0 THEN LEAST(qty_after_multiple, GREATEST(maximum_inventory - projected_inventory_bow, 0))
# MAGIC             ELSE qty_after_multiple
# MAGIC         END AS qty_after_maxinv
# MAGIC     FROM b
# MAGIC )
# MAGIC SELECT
# MAGIC     *,
# MAGIC     qty_after_maxinv AS final_suggested_quantity,
# MAGIC     CASE WHEN qty_after_maxinv > 0 THEN 1 ELSE 0 END AS need_to_order,
# MAGIC     CASE
# MAGIC         WHEN inventory_status IN ('CRITICAL_SHORTAGE', 'BELOW_REORDER_POINT')
# MAGIC              AND (reordering_policy IS NULL OR COALESCE(reorder_quantity, 0) = 0)
# MAGIC             THEN 'Missing planning parameters (no reorder policy/qty)'
# MAGIC         WHEN inventory_status IN ('CRITICAL_SHORTAGE', 'BELOW_REORDER_POINT')
# MAGIC              AND qty_after_maxinv > 0
# MAGIC             THEN 'New Order Required'
# MAGIC         WHEN inventory_status = 'EXCESS_INVENTORY'
# MAGIC             THEN 'Reduce/Cancel Orders'
# MAGIC         WHEN inventory_status = 'OK'
# MAGIC             THEN 'No Action Needed'
# MAGIC         ELSE 'No Activity'
# MAGIC     END AS action_message,
# MAGIC     CURRENT_TIMESTAMP AS calculation_timestamp
# MAGIC FROM c;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 9A: Planning Worksheet (Actions Only)  ✅ FIX: qualify fa.*
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE Silver_Planning_Lakehouse.testing.mrp_planning_worksheet AS
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         ROW_NUMBER() OVER (ORDER BY
# MAGIC             CASE fa.inventory_status
# MAGIC                 WHEN 'CRITICAL_SHORTAGE' THEN 1
# MAGIC                 WHEN 'BELOW_REORDER_POINT' THEN 2
# MAGIC                 WHEN 'EXCESS_INVENTORY' THEN 3
# MAGIC                 ELSE 4
# MAGIC             END,
# MAGIC             fa.planning_week_start,
# MAGIC             fa.item_no,
# MAGIC             fa.location_code
# MAGIC         ) AS line_no,
# MAGIC 
# MAGIC         fa.item_no AS no_,
# MAGIC         p.description,
# MAGIC 
# MAGIC         fa.planning_week_start,
# MAGIC         fa.planning_week_end,
# MAGIC         fa.week_label,
# MAGIC 
# MAGIC         fa.inventory_status AS ref_order_status,
# MAGIC         fa.action_message,
# MAGIC         CASE WHEN fa.action_message = 'New Order Required' THEN TRUE ELSE FALSE END AS accept_action_message,
# MAGIC 
# MAGIC         fa.starting_inventory AS current_inventory,
# MAGIC         fa.projected_inventory_bow,
# MAGIC         fa.projected_inventory_eow,
# MAGIC         fa.safety_stock_quantity,
# MAGIC         fa.reorder_point,
# MAGIC         fa.reorder_quantity,
# MAGIC         fa.variance_quantity AS shortage_qty,
# MAGIC 
# MAGIC         fa.original_demand,
# MAGIC         fa.original_supply,
# MAGIC         fa.sales_demand_lines,
# MAGIC         fa.prod_comp_demand_lines,
# MAGIC         fa.purchase_supply_lines,
# MAGIC         fa.production_supply_lines,
# MAGIC 
# MAGIC         fa.earliest_demand_date,
# MAGIC         fa.earliest_receipt_date,
# MAGIC 
# MAGIC         fa.suggested_order_quantity_raw AS suggested_raw,
# MAGIC         fa.qty_after_minimum,
# MAGIC         fa.qty_after_max_min,
# MAGIC         fa.qty_after_multiple,
# MAGIC         fa.qty_after_maxinv,
# MAGIC 
# MAGIC         fa.final_suggested_quantity AS quantity,
# MAGIC         fa.final_suggested_quantity AS remaining_quantity,
# MAGIC         fa.final_suggested_quantity AS quantity_base,
# MAGIC 
# MAGIC         fa.suggested_order_date AS order_date,
# MAGIC         fa.suggested_order_date AS starting_date,
# MAGIC         fa.action_date AS due_date,
# MAGIC         fa.action_date AS ending_date,
# MAGIC 
# MAGIC         fa.replenishment_system,
# MAGIC         fa.reordering_policy,
# MAGIC         fa.lead_time_calculation,
# MAGIC         fa.minimum_order_quantity,
# MAGIC         fa.maximum_order_quantity,
# MAGIC         fa.order_multiple,
# MAGIC         fa.time_bucket,
# MAGIC         fa.lot_accumulation_period,
# MAGIC 
# MAGIC         fa.location_code,
# MAGIC         fa.default_location,
# MAGIC         fa.vendor_no,
# MAGIC         fa.vendor_name,
# MAGIC         fa.metal_type,
# MAGIC 
# MAGIC         fa.calculation_timestamp,
# MAGIC         'Generated by Fabric MRP with gold_consumption_stock_item' AS user_id
# MAGIC     FROM vw_final_planning_actions fa
# MAGIC     LEFT JOIN vw_item_planning_params p
# MAGIC         ON p.item_no = fa.item_no
# MAGIC ) x
# MAGIC WHERE action_message = 'New Order Required'
# MAGIC ORDER BY line_no;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 9B: Worksheet (ALL rows for debugging) ✅ FIX: no fa.* + p.* collision
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE Silver_Planning_Lakehouse.testing.mrp_planning_worksheet_all AS
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY fa.item_no, fa.location_code, fa.planning_week_start) AS line_no,
# MAGIC     fa.*,
# MAGIC     p.description AS item_description
# MAGIC FROM vw_final_planning_actions fa
# MAGIC LEFT JOIN vw_item_planning_params p
# MAGIC     ON p.item_no = fa.item_no
# MAGIC ORDER BY fa.item_no, fa.location_code, fa.planning_week_start;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 10: Exceptions ✅ FIX: qualify all columns
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE Silver_Planning_Lakehouse.testing.mrp_exceptions AS
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (
# MAGIC         ORDER BY
# MAGIC             CASE fa.inventory_status
# MAGIC                 WHEN 'CRITICAL_SHORTAGE' THEN 1
# MAGIC                 WHEN 'BELOW_REORDER_POINT' THEN 2
# MAGIC                 ELSE 3
# MAGIC             END,
# MAGIC             fa.planning_week_start,
# MAGIC             fa.item_no,
# MAGIC             fa.location_code
# MAGIC     ) AS exception_no,
# MAGIC     fa.item_no,
# MAGIC     fa.location_code,
# MAGIC     fa.planning_week_start,
# MAGIC     fa.planning_week_end,
# MAGIC     fa.week_label,
# MAGIC     CASE fa.inventory_status
# MAGIC         WHEN 'CRITICAL_SHORTAGE' THEN 'High'
# MAGIC         WHEN 'BELOW_REORDER_POINT' THEN 'Medium'
# MAGIC         ELSE 'Low'
# MAGIC     END AS priority,
# MAGIC     fa.inventory_status AS exception_type,
# MAGIC     fa.action_message,
# MAGIC     fa.starting_inventory AS current_inventory,
# MAGIC     fa.projected_inventory_bow,
# MAGIC     fa.projected_inventory_eow,
# MAGIC     fa.safety_stock_quantity,
# MAGIC     fa.reorder_point,
# MAGIC     fa.reorder_quantity,
# MAGIC     fa.variance_quantity AS shortage_excess_qty,
# MAGIC     fa.final_suggested_quantity AS suggested_action_qty,
# MAGIC     fa.original_demand,
# MAGIC     fa.original_supply,
# MAGIC     fa.sales_demand_lines,
# MAGIC     fa.prod_comp_demand_lines,
# MAGIC     fa.action_date,
# MAGIC     fa.suggested_order_date,
# MAGIC     fa.replenishment_system,
# MAGIC     fa.reordering_policy,
# MAGIC     fa.vendor_no,
# MAGIC     fa.vendor_name,
# MAGIC     fa.metal_type,
# MAGIC     fa.calculation_timestamp
# MAGIC FROM vw_final_planning_actions fa
# MAGIC WHERE fa.inventory_status IN ('CRITICAL_SHORTAGE', 'BELOW_REORDER_POINT', 'EXCESS_INVENTORY')
# MAGIC ORDER BY priority DESC, fa.planning_week_start, fa.item_no;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 11: Summary ✅ FIX: qualify columns
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE Silver_Planning_Lakehouse.testing.mrp_run_summary AS
# MAGIC SELECT
# MAGIC     CURRENT_TIMESTAMP AS run_timestamp,
# MAGIC     COUNT(DISTINCT fa.item_no) AS items_planned,
# MAGIC     COUNT(*) AS total_planning_lines,
# MAGIC 
# MAGIC     SUM(CASE WHEN fa.action_message = 'New Order Required' THEN 1 ELSE 0 END) AS new_orders,
# MAGIC     SUM(CASE WHEN fa.action_message = 'Reduce/Cancel Orders' THEN 1 ELSE 0 END) AS cancel_reduce,
# MAGIC     SUM(CASE WHEN fa.action_message = 'No Action Needed' THEN 1 ELSE 0 END) AS no_action_needed,
# MAGIC     SUM(CASE WHEN fa.action_message LIKE 'Missing planning parameters%' THEN 1 ELSE 0 END) AS missing_parameters,
# MAGIC 
# MAGIC     SUM(CASE WHEN fa.inventory_status = 'CRITICAL_SHORTAGE' THEN 1 ELSE 0 END) AS critical_shortages,
# MAGIC     SUM(CASE WHEN fa.inventory_status = 'BELOW_REORDER_POINT' THEN 1 ELSE 0 END) AS below_reorder_point,
# MAGIC     SUM(CASE WHEN fa.inventory_status = 'EXCESS_INVENTORY' THEN 1 ELSE 0 END) AS excess_inventory,
# MAGIC     SUM(CASE WHEN fa.inventory_status = 'OK' THEN 1 ELSE 0 END) AS status_ok,
# MAGIC     SUM(CASE WHEN fa.inventory_status = 'NO_ACTIVITY' THEN 1 ELSE 0 END) AS no_activity,
# MAGIC 
# MAGIC     SUM(fa.final_suggested_quantity) AS total_quantity_to_order,
# MAGIC     SUM(fa.original_demand) AS total_demand,
# MAGIC     SUM(fa.original_supply) AS total_supply,
# MAGIC 
# MAGIC     MIN(fa.suggested_order_date) AS earliest_action_date,
# MAGIC     MAX(fa.action_date) AS latest_due_date,
# MAGIC 
# MAGIC     SUM(fa.sales_demand_lines) AS total_sales_lines,
# MAGIC     SUM(fa.prod_comp_demand_lines) AS total_prod_comp_lines,
# MAGIC     SUM(fa.purchase_supply_lines) AS total_purchase_lines,
# MAGIC     SUM(fa.production_supply_lines) AS total_production_lines
# MAGIC FROM vw_final_planning_actions fa;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- ============================================================================
# MAGIC -- COMPLETE MRP PLANNING SCRIPT WITH DUAL UOM SUPPORT
# MAGIC -- ============================================================================
# MAGIC -- This script handles exp1/exp2, con1/con2, rem1/rem2 for production components
# MAGIC -- exp = Expected quantity (what BOM says we need)
# MAGIC -- con = Consumed quantity (what we already used)
# MAGIC -- rem = Remaining quantity (what we still need to pick/use)
# MAGIC -- 1 = Base Unit of Measure (UOM1)
# MAGIC -- 2 = Secondary Unit of Measure (UOM2)
# MAGIC -- ============================================================================
# MAGIC 
# MAGIC USE Silver_Planning_Lakehouse.dbo;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 1: Item Planning Parameters (from gold_consumption_stock_item)
# MAGIC -- ============================================================================
# MAGIC -- Purpose: Get all planning rules for each item (when/how much to order)
# MAGIC -- One row per item with consolidated parameters
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_item_planning_params AS
# MAGIC SELECT
# MAGIC     item_no,
# MAGIC     MAX(item_description) AS description,
# MAGIC     MAX(replenishment_system) AS replenishment_system,      -- Purchase/Production
# MAGIC     MAX(reordering_policy) AS reordering_policy,            -- Fixed Qty/Max Qty/Lot-for-Lot
# MAGIC     MAX(reorder_point) AS reorder_point,                    -- Trigger level
# MAGIC     MAX(reorder_quantity) AS reorder_quantity,              -- Default order size
# MAGIC     MAX(maximum_inventory) AS maximum_inventory,            -- Ceiling
# MAGIC     MAX(item_safety_stock_quantity) AS safety_stock_quantity, -- Buffer stock
# MAGIC     MAX(vendor_leadtime) AS lead_time_calculation,          -- Days to delivery
# MAGIC     MAX(minimum_order_quantity) AS minimum_order_quantity,  -- Vendor minimum
# MAGIC     MAX(maximum_order_quantity) AS maximum_order_quantity,  -- Vendor/storage max
# MAGIC     MAX(order_multiple) AS order_multiple,                  -- Lot sizing (pallet qty, etc)
# MAGIC     MAX(safety_lead_time) AS safety_lead_time,
# MAGIC     MAX(time_bucket) AS time_bucket,
# MAGIC     MAX(lot_accoumlation_period) AS lot_accoumlation_period,
# MAGIC     MAX(rescheduling_period) AS rescheduling_period,
# MAGIC     MAX(vendor_no) AS vendor_no,
# MAGIC     MAX(vendor_name) AS vendor_name,
# MAGIC     MAX(`type`) AS metal_type,
# MAGIC     MAX(location) AS default_location,
# MAGIC     MAX(item_uom) AS base_uom,                             -- Base unit (EA, KG, etc)
# MAGIC     MAX(item_uom2) AS secondary_uom                         -- Secondary unit (BOX, PALLET, etc)
# MAGIC FROM gold_consumption_stock_item
# MAGIC WHERE item_no IS NOT NULL
# MAGIC GROUP BY item_no;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 2: On-hand Inventory (normalize + exclude BROK-MAT)
# MAGIC -- ============================================================================
# MAGIC -- Purpose: Current inventory position by item and location
# MAGIC -- Tracks both UOM1 and UOM2 quantities
# MAGIC -- EXCLUDES: BROK-MAT location (broken/damaged materials - can't use!)
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_inventory_on_hand AS
# MAGIC SELECT
# MAGIC     item_no,
# MAGIC     UPPER(TRIM(COALESCE(location, ''))) AS location_code,
# MAGIC     
# MAGIC     -- UOM1 (Base Unit) quantities
# MAGIC     SUM(rem_uom1) AS on_hand_quantity_uom1,
# MAGIC     
# MAGIC     -- UOM2 (Secondary Unit) quantities  
# MAGIC     SUM(rem_uom2) AS on_hand_quantity_uom2,
# MAGIC     
# MAGIC     -- Unit labels
# MAGIC     MAX(item_uom) AS uom1,
# MAGIC     MAX(item_uom2) AS uom2,
# MAGIC     
# MAGIC     -- Primary inventory (use UOM1 as base)
# MAGIC     SUM(rem_uom1) AS on_hand_quantity
# MAGIC FROM gold_consumption_stock_item
# MAGIC WHERE item_no IS NOT NULL
# MAGIC   AND UPPER(TRIM(COALESCE(location, ''))) <> 'BROK-MAT'  -- Exclude damaged goods
# MAGIC GROUP BY item_no, UPPER(TRIM(COALESCE(location, '')));
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 3: Demand (Sales + Production Components)
# MAGIC -- ============================================================================
# MAGIC -- Purpose: Collect ALL sources of demand (what we need)
# MAGIC -- Two types:
# MAGIC --   1. SALES: Customer orders (independent demand)
# MAGIC --   2. PROD_COMP: Production consumes components (dependent demand)
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_demand_all_sources AS
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- 3A: Sales Orders Demand (Customers want finished goods)
# MAGIC -- ============================================================================
# MAGIC SELECT
# MAGIC     'SALES' AS demand_source,
# MAGIC     `No.` AS item_no,
# MAGIC     UPPER(TRIM(COALESCE(`Location Code`, ''))) AS location_code,
# MAGIC     COALESCE(`Variant Code`, '') AS variant_code,
# MAGIC     `Shipment Date` AS required_date,                      -- When customer needs it
# MAGIC     `Outstanding Quantity` AS demand_quantity,             -- What's still needed
# MAGIC     `Outstanding Qty. (Base)` AS demand_quantity_base,     -- In base UOM
# MAGIC     `Quantity` AS original_quantity,                       -- Original order amount
# MAGIC     `Document Type` AS document_type,                      -- Order/Invoice
# MAGIC     `Document No.` AS document_no,                         -- Order number
# MAGIC     CAST(`Line No.` AS STRING) AS line_no,
# MAGIC     `Sell-to Customer No.` AS source_no,                   -- Customer ID
# MAGIC     `Planned Shipment Date` AS planned_date,
# MAGIC     `Unit of Measure Code` AS unit_of_measure,
# MAGIC     
# MAGIC     -- Not applicable for sales orders
# MAGIC     0 AS demand_quantity_uom2,
# MAGIC     0 AS expected_quantity_uom1,
# MAGIC     0 AS expected_quantity_uom2,
# MAGIC     0 AS consumed_quantity_uom1,
# MAGIC     0 AS consumed_quantity_uom2
# MAGIC FROM sales_line
# MAGIC WHERE `Type` = 'Item'
# MAGIC   AND `Outstanding Quantity` > 0                           -- Only unfulfilled
# MAGIC   AND `Document Type` IN ('Order', 'Invoice')
# MAGIC   AND COALESCE(`Completely Shipped`, FALSE) = FALSE
# MAGIC   AND UPPER(TRIM(COALESCE(`Location Code`, ''))) <> 'BROK-MAT'
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- 3B: Production Component Demand (Production CONSUMES materials)
# MAGIC -- ============================================================================
# MAGIC -- KEY CONCEPT: When we make bicycles, we CONSUME wheels/frames/bolts
# MAGIC -- This consumption CREATES DEMAND for those components
# MAGIC -- We track BOTH units of measure (exp1/exp2, con1/con2, rem1/rem2)
# MAGIC -- ============================================================================
# MAGIC SELECT
# MAGIC     'PROD_COMP' AS demand_source,
# MAGIC     ComponentItemNo AS item_no,                            -- The component being consumed
# MAGIC     UPPER(TRIM(COALESCE(location, ''))) AS location_code,
# MAGIC     '' AS variant_code,
# MAGIC     ComponentDueDate AS required_date,                     -- When production needs it
# MAGIC     
# MAGIC     -- PRIMARY DEMAND: Remaining in UOM1 (base unit)
# MAGIC     rem1 AS demand_quantity,                               -- Still needs to be picked
# MAGIC     rem1 AS demand_quantity_base,
# MAGIC     
# MAGIC     -- ORIGINAL: What BOM said we'd need (UOM1)
# MAGIC     exp1 AS original_quantity,
# MAGIC     
# MAGIC     CAST(`Status` AS STRING) AS document_type,             -- Production order status
# MAGIC     ProdOrderNo AS document_no,                            -- Production order number
# MAGIC     CAST(ComponentLineNo AS STRING) AS line_no,
# MAGIC     ProdOrderItemNo AS source_no,                          -- What finished good we're making
# MAGIC     ComponentDueDate AS planned_date,
# MAGIC     ComponentUOM AS unit_of_measure,
# MAGIC     
# MAGIC     -- EXPECTED QUANTITIES (what BOM says we need)
# MAGIC     COALESCE(exp1, 0) AS expected_quantity_uom1,           -- Expected in UOM1
# MAGIC     COALESCE(exp2, 0) AS expected_quantity_uom2,           -- Expected in UOM2
# MAGIC     
# MAGIC     -- CONSUMED QUANTITIES (what we already used/picked)
# MAGIC     COALESCE(con1, 0) AS consumed_quantity_uom1,           -- Consumed in UOM1
# MAGIC     COALESCE(con2, 0) AS consumed_quantity_uom2,           -- Consumed in UOM2
# MAGIC     
# MAGIC     -- REMAINING QUANTITIES (calculated: exp - con)
# MAGIC     -- rem1 exists in table, rem2 must be calculated
# MAGIC     COALESCE(exp2, 0) - COALESCE(con2, 0) AS demand_quantity_uom2  -- rem2 = exp2 - con2
# MAGIC FROM gold_consumption_stock_item
# MAGIC WHERE ComponentItemNo IS NOT NULL                          -- Has a component
# MAGIC   AND ComponentDueDate IS NOT NULL                         -- Has a due date
# MAGIC   AND rem1 > 0                                            -- Still has remaining need
# MAGIC   AND COALESCE(CompletelyPicked, FALSE) = FALSE           -- Not fully picked yet
# MAGIC   AND UPPER(TRIM(COALESCE(location, ''))) <> 'BROK-MAT';
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 4: Supply (Purchase + Production Orders)
# MAGIC -- ============================================================================
# MAGIC -- Purpose: Collect ALL sources of supply (what's coming)
# MAGIC -- Two types:
# MAGIC --   1. PURCHASE: Buying from vendors (external supply)
# MAGIC --   2. PRODUCTION: Manufacturing internally (internal supply)
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_supply_all_sources AS
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- 4A: Purchase Orders Supply (Buying from vendors)
# MAGIC -- ============================================================================
# MAGIC SELECT
# MAGIC     'PURCHASE' AS supply_source,
# MAGIC     `No.` AS item_no,
# MAGIC     UPPER(TRIM(COALESCE(`Location Code`, ''))) AS location_code,
# MAGIC     COALESCE(`Variant Code`, '') AS variant_code,
# MAGIC     `Expected Receipt Date` AS receipt_date,               -- When we expect delivery
# MAGIC     `Outstanding Quantity` AS supply_quantity,             -- What's still coming
# MAGIC     `Outstanding Qty. (Base)` AS supply_quantity_base,
# MAGIC     `Quantity` AS original_quantity,
# MAGIC     `Document Type` AS document_type,                      -- Order/Return Order
# MAGIC     `Document No.` AS document_no,                         -- PO number
# MAGIC     CAST(`Line No.` AS STRING) AS line_no,
# MAGIC     COALESCE(`Planning Flexibility`, 'Unlimited') AS planning_flexibility, -- Can we reschedule?
# MAGIC     `Buy-from Vendor No.` AS source_no,                    -- Vendor ID
# MAGIC     `Planned Receipt Date` AS planned_date,
# MAGIC     `Unit of Measure Code` AS unit_of_measure,
# MAGIC     
# MAGIC     -- Not applicable for purchase orders
# MAGIC     0 AS supply_quantity_uom2
# MAGIC FROM purchase_line
# MAGIC WHERE `Type` = 'Item'
# MAGIC   AND `Outstanding Quantity` > 0
# MAGIC   AND `Document Type` IN ('Order', 'Return Order')
# MAGIC   AND COALESCE(`Completely Received`, FALSE) = FALSE
# MAGIC   AND UPPER(TRIM(COALESCE(`Location Code`, ''))) <> 'BROK-MAT'
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- 4B: Production Orders Supply (Manufacturing items internally)
# MAGIC -- ============================================================================
# MAGIC -- KEY CONCEPT: Production order outputs the FINISHED GOOD
# MAGIC -- Same production order consumes COMPONENTS (tracked in demand above)
# MAGIC -- ============================================================================
# MAGIC SELECT
# MAGIC     'PRODUCTION' AS supply_source,
# MAGIC     `Item No.` AS item_no,                                 -- Finished good being made
# MAGIC     UPPER(TRIM(COALESCE(`Location Code`, ''))) AS location_code,
# MAGIC     COALESCE(`Variant Code`, '') AS variant_code,
# MAGIC     `Due Date` AS receipt_date,                            -- When production finishes
# MAGIC     `Remaining Quantity` AS supply_quantity,               -- What's still being made
# MAGIC     `Remaining Qty. (Base)` AS supply_quantity_base,
# MAGIC     `Quantity` AS original_quantity,
# MAGIC     `Status` AS document_type,                             -- Planned/Released/etc
# MAGIC     `Prod. Order No.` AS document_no,                      -- Production order number
# MAGIC     CAST(`Line No.` AS STRING) AS line_no,
# MAGIC     COALESCE(`Planning Flexibility`, 'Unlimited') AS planning_flexibility,
# MAGIC     `Item No.` AS source_no,
# MAGIC     `Starting Date` AS planned_date,
# MAGIC     `Unit of Measure Code` AS unit_of_measure,
# MAGIC     
# MAGIC     -- Not applicable for production orders
# MAGIC     0 AS supply_quantity_uom2
# MAGIC FROM prod_order_line
# MAGIC WHERE `Status` IN ('Planned', 'Firm Planned', 'Released')  -- Only active production
# MAGIC   AND `Remaining Quantity` > 0
# MAGIC   AND UPPER(TRIM(COALESCE(`Location Code`, ''))) <> 'BROK-MAT';
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 5: Net Requirements by Week
# MAGIC -- ============================================================================
# MAGIC -- Purpose: Create time-phased planning grid (52 weeks forward)
# MAGIC -- Shows supply vs demand week-by-week for every item/location
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_net_requirements AS
# MAGIC 
# MAGIC -- 5A: Generate 52 weeks of planning periods
# MAGIC WITH date_range AS (
# MAGIC     SELECT EXPLODE(SEQUENCE(
# MAGIC         DATE_TRUNC('week', CURRENT_DATE),                  -- Start: This week Monday
# MAGIC         DATE_ADD(DATE_TRUNC('week', CURRENT_DATE), 364),   -- End: 52 weeks out
# MAGIC         INTERVAL 7 DAY                                     -- Step: Weekly
# MAGIC     )) AS planning_week_start
# MAGIC ),
# MAGIC 
# MAGIC -- 5B: Create week labels and boundaries
# MAGIC weekly_periods AS (
# MAGIC     SELECT
# MAGIC         planning_week_start,
# MAGIC         DATE_ADD(planning_week_start, 6) AS planning_week_end,  -- Sunday
# MAGIC         CONCAT('W', WEEKOFYEAR(planning_week_start), '-', YEAR(planning_week_start)) AS week_label
# MAGIC     FROM date_range
# MAGIC ),
# MAGIC 
# MAGIC -- 5C: Get all item/location combinations that exist anywhere
# MAGIC item_location_combinations AS (
# MAGIC     SELECT item_no, location_code FROM vw_inventory_on_hand
# MAGIC     UNION
# MAGIC     SELECT item_no, location_code FROM vw_demand_all_sources
# MAGIC     UNION
# MAGIC     SELECT item_no, location_code FROM vw_supply_all_sources
# MAGIC ),
# MAGIC 
# MAGIC -- 5D: Create complete planning grid (every item × every week)
# MAGIC -- This ensures we have rows even for weeks with no activity
# MAGIC planning_grid AS (
# MAGIC     SELECT
# MAGIC         il.item_no,
# MAGIC         il.location_code,
# MAGIC         wp.planning_week_start,
# MAGIC         wp.planning_week_end,
# MAGIC         wp.week_label
# MAGIC     FROM item_location_combinations il
# MAGIC     CROSS JOIN weekly_periods wp                           -- Cartesian product!
# MAGIC     WHERE il.location_code <> 'BROK-MAT'
# MAGIC ),
# MAGIC 
# MAGIC -- 5E: Aggregate demand by week
# MAGIC weekly_demand AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         location_code,
# MAGIC         DATE_TRUNC('week', required_date) AS planning_week_start,
# MAGIC         
# MAGIC         -- Total demand quantities
# MAGIC         SUM(demand_quantity) AS total_demand,
# MAGIC         SUM(demand_quantity_base) AS total_demand_base,
# MAGIC         SUM(original_quantity) AS original_demand,
# MAGIC         
# MAGIC         -- Secondary UOM totals
# MAGIC         SUM(demand_quantity_uom2) AS total_demand_uom2,
# MAGIC         
# MAGIC         -- Production component specific totals
# MAGIC         SUM(expected_quantity_uom1) AS total_expected_uom1,
# MAGIC         SUM(expected_quantity_uom2) AS total_expected_uom2,
# MAGIC         SUM(consumed_quantity_uom1) AS total_consumed_uom1,
# MAGIC         SUM(consumed_quantity_uom2) AS total_consumed_uom2,
# MAGIC         
# MAGIC         -- Line counts by source
# MAGIC         COUNT(*) AS demand_line_count,
# MAGIC         COUNT(CASE WHEN demand_source = 'SALES' THEN 1 END) AS sales_lines,
# MAGIC         COUNT(CASE WHEN demand_source = 'PROD_COMP' THEN 1 END) AS prod_comp_lines,
# MAGIC         
# MAGIC         -- Date range within week
# MAGIC         MIN(required_date) AS earliest_demand_date,
# MAGIC         MAX(required_date) AS latest_demand_date
# MAGIC     FROM vw_demand_all_sources
# MAGIC     WHERE required_date >= CURRENT_DATE                    -- Only future demand
# MAGIC     GROUP BY item_no, location_code, DATE_TRUNC('week', required_date)
# MAGIC ),
# MAGIC 
# MAGIC -- 5F: Aggregate supply by week
# MAGIC weekly_supply AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         location_code,
# MAGIC         DATE_TRUNC('week', receipt_date) AS planning_week_start,
# MAGIC         
# MAGIC         -- Total supply quantities
# MAGIC         SUM(supply_quantity) AS total_supply,
# MAGIC         SUM(supply_quantity_base) AS total_supply_base,
# MAGIC         SUM(original_quantity) AS original_supply,
# MAGIC         
# MAGIC         -- Secondary UOM totals
# MAGIC         SUM(supply_quantity_uom2) AS total_supply_uom2,
# MAGIC         
# MAGIC         -- Line counts by source
# MAGIC         COUNT(*) AS supply_line_count,
# MAGIC         COUNT(CASE WHEN supply_source = 'PURCHASE' THEN 1 END) AS purchase_lines,
# MAGIC         COUNT(CASE WHEN supply_source = 'PRODUCTION' THEN 1 END) AS production_lines,
# MAGIC         
# MAGIC         -- Date range within week
# MAGIC         MIN(receipt_date) AS earliest_receipt_date,
# MAGIC         MAX(receipt_date) AS latest_receipt_date
# MAGIC     FROM vw_supply_all_sources
# MAGIC     WHERE receipt_date >= CURRENT_DATE                     -- Only future supply
# MAGIC     GROUP BY item_no, location_code, DATE_TRUNC('week', receipt_date)
# MAGIC )
# MAGIC 
# MAGIC -- 5G: Join everything together - THE MAGIC HAPPENS HERE!
# MAGIC SELECT
# MAGIC     pg.item_no,
# MAGIC     pg.location_code,
# MAGIC     pg.planning_week_start,
# MAGIC     pg.planning_week_end,
# MAGIC     pg.week_label,
# MAGIC 
# MAGIC     -- DEMAND SIDE (what we need)
# MAGIC     COALESCE(wd.total_demand, 0) AS demand,
# MAGIC     COALESCE(wd.total_demand_base, 0) AS demand_base,
# MAGIC     COALESCE(wd.original_demand, 0) AS original_demand,
# MAGIC     COALESCE(wd.total_demand_uom2, 0) AS demand_uom2,
# MAGIC     COALESCE(wd.sales_lines, 0) AS sales_demand_lines,
# MAGIC     COALESCE(wd.prod_comp_lines, 0) AS prod_comp_demand_lines,
# MAGIC     COALESCE(wd.earliest_demand_date, pg.planning_week_start) AS earliest_demand_date,
# MAGIC     
# MAGIC     -- PRODUCTION COMPONENT DETAILS (exp/con/rem tracking)
# MAGIC     COALESCE(wd.total_expected_uom1, 0) AS expected_consumption_uom1,
# MAGIC     COALESCE(wd.total_expected_uom2, 0) AS expected_consumption_uom2,
# MAGIC     COALESCE(wd.total_consumed_uom1, 0) AS already_consumed_uom1,
# MAGIC     COALESCE(wd.total_consumed_uom2, 0) AS already_consumed_uom2,
# MAGIC 
# MAGIC     -- SUPPLY SIDE (what's coming)
# MAGIC     COALESCE(ws.total_supply, 0) AS supply,
# MAGIC     COALESCE(ws.total_supply_base, 0) AS supply_base,
# MAGIC     COALESCE(ws.original_supply, 0) AS original_supply,
# MAGIC     COALESCE(ws.total_supply_uom2, 0) AS supply_uom2,
# MAGIC     COALESCE(ws.purchase_lines, 0) AS purchase_supply_lines,
# MAGIC     COALESCE(ws.production_lines, 0) AS production_supply_lines,
# MAGIC     COALESCE(ws.earliest_receipt_date, pg.planning_week_start) AS earliest_receipt_date,
# MAGIC 
# MAGIC     -- INVENTORY (current position)
# MAGIC     COALESCE(inv.on_hand_quantity, 0) AS starting_inventory,
# MAGIC     COALESCE(inv.on_hand_quantity_uom1, 0) AS starting_inventory_uom1,
# MAGIC     COALESCE(inv.on_hand_quantity_uom2, 0) AS starting_inventory_uom2,
# MAGIC 
# MAGIC     -- PLANNING PARAMETERS (rules)
# MAGIC     COALESCE(p.safety_stock_quantity, 0) AS safety_stock_quantity,
# MAGIC     COALESCE(p.reorder_point, 0) AS reorder_point,
# MAGIC     p.reordering_policy,
# MAGIC     p.replenishment_system,
# MAGIC     p.lead_time_calculation,
# MAGIC     COALESCE(p.reorder_quantity, 0) AS reorder_quantity,
# MAGIC     COALESCE(p.maximum_inventory, 0) AS maximum_inventory,
# MAGIC     COALESCE(p.minimum_order_quantity, 0) AS minimum_order_quantity,
# MAGIC     COALESCE(p.maximum_order_quantity, 0) AS maximum_order_quantity,
# MAGIC     COALESCE(NULLIF(p.order_multiple, 0), 1) AS order_multiple,
# MAGIC     p.time_bucket,
# MAGIC     p.lot_accoumlation_period,
# MAGIC     p.rescheduling_period,
# MAGIC     p.vendor_no,
# MAGIC     p.vendor_name,
# MAGIC     p.metal_type,
# MAGIC     p.default_location,
# MAGIC     p.base_uom,
# MAGIC     p.secondary_uom
# MAGIC 
# MAGIC FROM planning_grid pg
# MAGIC 
# MAGIC -- LEFT JOINS preserve all planning grid rows (even with no activity)
# MAGIC LEFT JOIN weekly_demand wd
# MAGIC     ON pg.item_no = wd.item_no
# MAGIC    AND pg.location_code = wd.location_code
# MAGIC    AND pg.planning_week_start = wd.planning_week_start
# MAGIC    
# MAGIC LEFT JOIN weekly_supply ws
# MAGIC     ON pg.item_no = ws.item_no
# MAGIC    AND pg.location_code = ws.location_code
# MAGIC    AND pg.planning_week_start = ws.planning_week_start
# MAGIC    
# MAGIC LEFT JOIN vw_inventory_on_hand inv
# MAGIC     ON pg.item_no = inv.item_no
# MAGIC    AND pg.location_code = inv.location_code
# MAGIC    
# MAGIC LEFT JOIN vw_item_planning_params p
# MAGIC     ON pg.item_no = p.item_no
# MAGIC 
# MAGIC -- Only keep weeks with activity (reduces ~90% of rows!)
# MAGIC WHERE COALESCE(wd.total_demand, 0) > 0
# MAGIC    OR COALESCE(ws.total_supply, 0) > 0
# MAGIC    OR COALESCE(inv.on_hand_quantity, 0) <> 0
# MAGIC    OR pg.planning_week_start = DATE_TRUNC('week', CURRENT_DATE);
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 6: Projected Inventory (BOW/EOW)
# MAGIC -- ============================================================================
# MAGIC -- Purpose: Calculate inventory position throughout planning horizon
# MAGIC -- BOW = Beginning of Week (check reorder point HERE!)
# MAGIC -- EOW = End of Week (after supply/demand)
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_projected_inventory AS
# MAGIC WITH x AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         -- Net change this week
# MAGIC         (supply - demand) AS weekly_net_change,
# MAGIC         
# MAGIC         -- Running total (cumulative from week 1 to current week)
# MAGIC         SUM(supply - demand) OVER (
# MAGIC             PARTITION BY item_no, location_code           -- Separate per item/location
# MAGIC             ORDER BY planning_week_start                  -- Chronological order
# MAGIC             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW  -- Running sum
# MAGIC         ) AS cumulative_net_change
# MAGIC     FROM vw_net_requirements
# MAGIC )
# MAGIC SELECT
# MAGIC     *,
# MAGIC     -- End of Week = Starting inventory + all changes up to now
# MAGIC     starting_inventory + cumulative_net_change AS projected_inventory_eow,
# MAGIC     
# MAGIC     -- Beginning of Week = EOW minus this week's change
# MAGIC     -- This is what we CHECK AGAINST REORDER POINT!
# MAGIC     (starting_inventory + cumulative_net_change) - weekly_net_change AS projected_inventory_bow
# MAGIC FROM x;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 7: Policy Logic (trigger on BOW)
# MAGIC -- ============================================================================
# MAGIC -- Purpose: Apply inventory policies and determine actions needed
# MAGIC -- Checks BOW inventory against safety stock and reorder point
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_planning_actions AS
# MAGIC SELECT
# MAGIC     *,
# MAGIC     
# MAGIC     -- INVENTORY STATUS CLASSIFICATION
# MAGIC     CASE
# MAGIC         WHEN projected_inventory_bow < safety_stock_quantity 
# MAGIC             THEN 'CRITICAL_SHORTAGE'                       -- URGENT! Below safety buffer
# MAGIC         WHEN projected_inventory_bow < reorder_point 
# MAGIC             THEN 'BELOW_REORDER_POINT'                     -- Need to order
# MAGIC         WHEN projected_inventory_bow > maximum_inventory AND maximum_inventory > 0 
# MAGIC             THEN 'EXCESS_INVENTORY'                        -- Too much stock
# MAGIC         WHEN demand > 0 OR supply > 0 
# MAGIC             THEN 'OK'                                      -- Active but healthy
# MAGIC         ELSE 'NO_ACTIVITY'                                 -- Dormant
# MAGIC     END AS inventory_status,
# MAGIC 
# MAGIC     -- VARIANCE FROM TARGET
# MAGIC     CASE
# MAGIC         WHEN projected_inventory_bow < reorder_point 
# MAGIC             THEN reorder_point - projected_inventory_bow   -- How far below
# MAGIC         WHEN projected_inventory_bow > maximum_inventory AND maximum_inventory > 0 
# MAGIC             THEN projected_inventory_bow - maximum_inventory  -- How far above
# MAGIC         ELSE 0
# MAGIC     END AS variance_quantity,
# MAGIC 
# MAGIC     -- SUGGESTED ORDER QUANTITY (raw, before constraints)
# MAGIC     -- Different calculation per reordering policy
# MAGIC     CASE reordering_policy
# MAGIC     
# MAGIC         -- Policy 1: Fixed Reorder Quantity
# MAGIC         -- Always order same amount when below reorder point
# MAGIC         WHEN 'Fixed Reorder Qty.' THEN 
# MAGIC             CASE WHEN projected_inventory_bow < reorder_point 
# MAGIC                 THEN reorder_quantity 
# MAGIC                 ELSE 0 
# MAGIC             END
# MAGIC             
# MAGIC         -- Policy 2: Maximum Quantity
# MAGIC         -- Order up to maximum inventory level
# MAGIC         WHEN 'Maximum Qty.' THEN 
# MAGIC             CASE WHEN projected_inventory_bow < reorder_point 
# MAGIC                 THEN GREATEST(maximum_inventory - projected_inventory_bow, 0) 
# MAGIC                 ELSE 0 
# MAGIC             END
# MAGIC             
# MAGIC         -- Policy 3: Order
# MAGIC         -- Order exactly to reorder point
# MAGIC         WHEN 'Order' THEN 
# MAGIC             CASE WHEN projected_inventory_bow < reorder_point 
# MAGIC                 THEN reorder_point - projected_inventory_bow 
# MAGIC                 ELSE 0 
# MAGIC             END
# MAGIC             
# MAGIC         -- Policy 4: Lot-for-Lot (Just-in-Time)
# MAGIC         -- Order exact demand + safety buffer if critical
# MAGIC         WHEN 'Lot-for-Lot' THEN
# MAGIC             CASE
# MAGIC                 WHEN projected_inventory_bow < safety_stock_quantity 
# MAGIC                     THEN GREATEST(demand + (safety_stock_quantity - projected_inventory_bow), 0)
# MAGIC                 WHEN demand > 0 AND projected_inventory_bow < reorder_point 
# MAGIC                     THEN demand
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC             
# MAGIC         -- Default: Use fixed reorder quantity
# MAGIC         ELSE 
# MAGIC             CASE WHEN projected_inventory_bow < reorder_point 
# MAGIC                 THEN reorder_quantity 
# MAGIC                 ELSE 0 
# MAGIC             END
# MAGIC     END AS suggested_order_quantity_raw
# MAGIC     
# MAGIC FROM vw_projected_inventory;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 8: Lot Sizing + Action Message
# MAGIC -- ============================================================================
# MAGIC -- Purpose: Apply real-world constraints to suggested quantities
# MAGIC -- Sequential steps: min → max → multiple → capacity
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_final_planning_actions AS
# MAGIC 
# MAGIC -- 8A: Parse lead time to days
# MAGIC WITH base AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CAST(COALESCE(NULLIF(REGEXP_REPLACE(lead_time_calculation, '[^0-9]', ''), ''), '0') AS INT) AS lead_time_days
# MAGIC     FROM vw_planning_actions
# MAGIC ),
# MAGIC 
# MAGIC -- 8B: Apply minimum order quantity
# MAGIC a AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         earliest_demand_date AS action_date,
# MAGIC         DATE_SUB(planning_week_start, lead_time_days) AS suggested_order_date,  -- Back-calculate
# MAGIC         GREATEST(suggested_order_quantity_raw, minimum_order_quantity) AS qty_after_minimum
# MAGIC     FROM base
# MAGIC ),
# MAGIC 
# MAGIC -- 8C: Apply maximum order quantity
# MAGIC b AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CASE
# MAGIC             WHEN maximum_order_quantity > 0 THEN 
# MAGIC                 LEAST(qty_after_minimum, maximum_order_quantity)
# MAGIC             ELSE 
# MAGIC                 qty_after_minimum
# MAGIC         END AS qty_after_max_min
# MAGIC     FROM a
# MAGIC ),
# MAGIC 
# MAGIC -- 8D: Round to order multiple (lot sizing)
# MAGIC c AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CASE WHEN order_multiple > 1 
# MAGIC             THEN CEIL(qty_after_max_min / order_multiple) * order_multiple 
# MAGIC             ELSE qty_after_max_min 
# MAGIC         END AS qty_after_multiple
# MAGIC     FROM b
# MAGIC ),
# MAGIC 
# MAGIC -- 8E: Check maximum inventory capacity
# MAGIC d AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CASE
# MAGIC             WHEN maximum_inventory > 0 THEN 
# MAGIC                 LEAST(qty_after_multiple, GREATEST(maximum_inventory - projected_inventory_bow, 0))
# MAGIC             ELSE 
# MAGIC                 qty_after_multiple
# MAGIC         END AS qty_after_maxinv
# MAGIC     FROM c
# MAGIC )
# MAGIC 
# MAGIC -- 8F: Generate final action message
# MAGIC SELECT
# MAGIC     *,
# MAGIC     qty_after_maxinv AS final_suggested_quantity,
# MAGIC     CASE WHEN qty_after_maxinv > 0 THEN 1 ELSE 0 END AS need_to_order,
# MAGIC     
# MAGIC     -- ACTION MESSAGE LOGIC
# MAGIC     CASE
# MAGIC         -- Missing parameters
# MAGIC         WHEN inventory_status IN ('CRITICAL_SHORTAGE', 'BELOW_REORDER_POINT')
# MAGIC              AND (reordering_policy IS NULL OR COALESCE(reorder_quantity, 0) = 0)
# MAGIC             THEN 'Missing planning parameters (no reorder policy/qty)'
# MAGIC             
# MAGIC         -- Need to order
# MAGIC         WHEN inventory_status IN ('CRITICAL_SHORTAGE', 'BELOW_REORDER_POINT')
# MAGIC              AND qty_after_maxinv > 0
# MAGIC             THEN 'New Order Required'
# MAGIC             
# MAGIC         -- Too much inventory
# MAGIC         WHEN inventory_status = 'EXCESS_INVENTORY'
# MAGIC             THEN 'Reduce/Cancel Orders'
# MAGIC             
# MAGIC         -- Healthy
# MAGIC         WHEN inventory_status = 'OK'
# MAGIC             THEN 'No Action Needed'
# MAGIC             
# MAGIC         ELSE 'No Activity'
# MAGIC     END AS action_message,
# MAGIC     
# MAGIC     CURRENT_TIMESTAMP AS calculation_timestamp
# MAGIC FROM d;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 9A: Planning Worksheet (Actions Only)
# MAGIC -- ============================================================================
# MAGIC -- Purpose: Executable work list for planners
# MAGIC -- FILTERED: Only lines requiring action (New Order Required)
# MAGIC -- SORTED: By priority (Critical → Below Reorder → Excess)
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE Silver_Planning_Lakehouse.testing.mrp_planning_worksheet AS
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         ROW_NUMBER() OVER (ORDER BY
# MAGIC             CASE fa.inventory_status
# MAGIC                 WHEN 'CRITICAL_SHORTAGE' THEN 1
# MAGIC                 WHEN 'BELOW_REORDER_POINT' THEN 2
# MAGIC                 WHEN 'EXCESS_INVENTORY' THEN 3
# MAGIC                 ELSE 4
# MAGIC             END,
# MAGIC             fa.planning_week_start,
# MAGIC             fa.item_no,
# MAGIC             fa.location_code
# MAGIC         ) AS line_no,
# MAGIC 
# MAGIC         fa.item_no AS no_,
# MAGIC         p.description,
# MAGIC 
# MAGIC         fa.planning_week_start,
# MAGIC         fa.planning_week_end,
# MAGIC         fa.week_label,
# MAGIC 
# MAGIC         fa.inventory_status AS ref_order_status,
# MAGIC         fa.action_message,
# MAGIC         CASE WHEN fa.action_message = 'New Order Required' THEN TRUE ELSE FALSE END AS accept_action_message,
# MAGIC 
# MAGIC         -- INVENTORY POSITIONS
# MAGIC         fa.starting_inventory AS current_inventory,
# MAGIC         fa.starting_inventory_uom1,
# MAGIC         fa.starting_inventory_uom2,
# MAGIC         fa.projected_inventory_bow,
# MAGIC         fa.projected_inventory_eow,
# MAGIC         fa.safety_stock_quantity,
# MAGIC         fa.reorder_point,
# MAGIC         fa.reorder_quantity,
# MAGIC         fa.variance_quantity AS shortage_qty,
# MAGIC 
# MAGIC         -- DEMAND/SUPPLY TOTALS
# MAGIC         fa.original_demand,
# MAGIC         fa.demand_uom2,
# MAGIC         fa.original_supply,
# MAGIC         fa.supply_uom2,
# MAGIC         fa.sales_demand_lines,
# MAGIC         fa.prod_comp_demand_lines,
# MAGIC         fa.purchase_supply_lines,
# MAGIC         fa.production_supply_lines,
# MAGIC 
# MAGIC         -- PRODUCTION COMPONENT CONSUMPTION TRACKING
# MAGIC         fa.expected_consumption_uom1,
# MAGIC         fa.expected_consumption_uom2,
# MAGIC         fa.already_consumed_uom1,
# MAGIC         fa.already_consumed_uom2,
# MAGIC         (fa.expected_consumption_uom1 - fa.already_consumed_uom1) AS remaining_to_consume_uom1,
# MAGIC         (fa.expected_consumption_uom2 - fa.already_consumed_uom2) AS remaining_to_consume_uom2,
# MAGIC 
# MAGIC         -- DATES
# MAGIC         fa.earliest_demand_date,
# MAGIC         fa.earliest_receipt_date,
# MAGIC 
# MAGIC         -- LOT SIZING STEPS (audit trail)
# MAGIC         fa.suggested_order_quantity_raw AS suggested_raw,
# MAGIC         fa.qty_after_minimum,
# MAGIC         fa.qty_after_max_min,
# MAGIC         fa.qty_after_multiple,
# MAGIC         fa.qty_after_maxinv,
# MAGIC 
# MAGIC         -- ORDER DETAILS
# MAGIC         fa.final_suggested_quantity AS quantity,
# MAGIC         fa.final_suggested_quantity AS remaining_quantity,
# MAGIC         fa.final_suggested_quantity AS quantity_base,
# MAGIC 
# MAGIC         fa.suggested_order_date AS order_date,
# MAGIC         fa.suggested_order_date AS starting_date,
# MAGIC         fa.action_date AS due_date,
# MAGIC         fa.action_date AS ending_date,
# MAGIC 
# MAGIC         -- PLANNING PARAMETERS
# MAGIC         fa.replenishment_system,
# MAGIC         fa.reordering_policy,
# MAGIC         fa.lead_time_calculation,
# MAGIC         fa.lead_time_days,
# MAGIC         fa.minimum_order_quantity,
# MAGIC         fa.maximum_order_quantity,
# MAGIC         fa.order_multiple,
# MAGIC         fa.time_bucket,
# MAGIC         fa.lot_accoumlation_period,
# MAGIC 
# MAGIC         -- LOCATION/VENDOR
# MAGIC         fa.location_code,
# MAGIC         fa.default_location,
# MAGIC         fa.vendor_no,
# MAGIC         fa.vendor_name,
# MAGIC         fa.metal_type,
# MAGIC         
# MAGIC         -- UNITS OF MEASURE
# MAGIC         fa.base_uom,
# MAGIC         fa.secondary_uom,
# MAGIC 
# MAGIC         -- AUDIT
# MAGIC         fa.calculation_timestamp,
# MAGIC         'Generated by Fabric MRP with full UOM support' AS user_id
# MAGIC         
# MAGIC     FROM vw_final_planning_actions fa
# MAGIC     LEFT JOIN vw_item_planning_params p
# MAGIC         ON p.item_no = fa.item_no
# MAGIC ) x
# MAGIC WHERE action_message = 'New Order Required'
# MAGIC ORDER BY line_no;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 9B: Worksheet (ALL rows for debugging)
# MAGIC -- ============================================================================
# MAGIC -- Purpose: Complete diagnostic view
# MAGIC -- INCLUDES: All weeks, all statuses (even "No Action Needed")
# MAGIC -- USE FOR: Debugging, analysis, "why didn't item X trigger?"
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE Silver_Planning_Lakehouse.testing.mrp_planning_worksheet_all AS
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY fa.item_no, fa.location_code, fa.planning_week_start) AS line_no,
# MAGIC     fa.*,
# MAGIC     p.description AS item_description
# MAGIC FROM vw_final_planning_actions fa
# MAGIC LEFT JOIN vw_item_planning_params p
# MAGIC     ON p.item_no = fa.item_no
# MAGIC ORDER BY fa.item_no, fa.location_code, fa.planning_week_start;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 10: Exceptions Report
# MAGIC -- ============================================================================
# MAGIC -- Purpose: Items requiring immediate attention
# MAGIC -- FILTERED: Only problem items (shortages, excess)
# MAGIC -- SORTED: By priority (High → Medium → Low)
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE Silver_Planning_Lakehouse.testing.mrp_exceptions AS
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (
# MAGIC         ORDER BY
# MAGIC             CASE fa.inventory_status
# MAGIC                 WHEN 'CRITICAL_SHORTAGE' THEN 1
# MAGIC                 WHEN 'BELOW_REORDER_POINT' THEN 2
# MAGIC                 ELSE 3
# MAGIC             END,
# MAGIC             fa.planning_week_start,
# MAGIC             fa.item_no,
# MAGIC             fa.location_code
# MAGIC     ) AS exception_no,
# MAGIC     
# MAGIC     fa.item_no,
# MAGIC     fa.location_code,
# MAGIC     fa.planning_week_start,
# MAGIC     fa.planning_week_end,
# MAGIC     fa.week_label,
# MAGIC     
# MAGIC     -- PRIORITY
# MAGIC     CASE fa.inventory_status
# MAGIC         WHEN 'CRITICAL_SHORTAGE' THEN 'High'
# MAGIC         WHEN 'BELOW_REORDER_POINT' THEN 'Medium'
# MAGIC         ELSE 'Low'
# MAGIC     END AS priority,
# MAGIC     
# MAGIC     fa.inventory_status AS exception_type,
# MAGIC     fa.action_message,
# MAGIC     
# MAGIC     -- INVENTORY METRICS
# MAGIC     fa.starting_inventory AS current_inventory,
# MAGIC     fa.starting_inventory_uom1,
# MAGIC     fa.starting_inventory_uom2,
# MAGIC     fa.projected_inventory_bow,
# MAGIC     fa.projected_inventory_eow,
# MAGIC     fa.safety_stock_quantity,
# MAGIC     fa.reorder_point,
# MAGIC     fa.reorder_quantity,
# MAGIC     fa.variance_quantity AS shortage_excess_qty,
# MAGIC     fa.final_suggested_quantity AS suggested_action_qty,
# MAGIC     
# MAGIC     -- ACTIVITY
# MAGIC     fa.original_demand,
# MAGIC     fa.demand_uom2,
# MAGIC     fa.original_supply,
# MAGIC     fa.supply_uom2,
# MAGIC     fa.sales_demand_lines,
# MAGIC     fa.prod_comp_demand_lines,
# MAGIC     
# MAGIC     -- PRODUCTION CONSUMPTION
# MAGIC     fa.expected_consumption_uom1,
# MAGIC     fa.expected_consumption_uom2,
# MAGIC     fa.already_consumed_uom1,
# MAGIC     fa.already_consumed_uom2,
# MAGIC     (fa.expected_consumption_uom1 - fa.already_consumed_uom1) AS remaining_to_consume_uom1,
# MAGIC     
# MAGIC     -- DATES
# MAGIC     fa.action_date,
# MAGIC     fa.suggested_order_date,
# MAGIC     
# MAGIC     -- PARAMETERS
# MAGIC     fa.replenishment_system,
# MAGIC     fa.reordering_policy,
# MAGIC     fa.vendor_no,
# MAGIC     fa.vendor_name,
# MAGIC     fa.metal_type,
# MAGIC     fa.base_uom,
# MAGIC     fa.secondary_uom,
# MAGIC     fa.calculation_timestamp
# MAGIC     
# MAGIC FROM vw_final_planning_actions fa
# MAGIC WHERE fa.inventory_status IN ('CRITICAL_SHORTAGE', 'BELOW_REORDER_POINT', 'EXCESS_INVENTORY')
# MAGIC ORDER BY priority DESC, fa.planning_week_start, fa.item_no;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Step 11: Run Summary
# MAGIC -- ============================================================================
# MAGIC -- Purpose: High-level metrics for this MRP execution
# MAGIC -- Single row with aggregate statistics
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE Silver_Planning_Lakehouse.testing.mrp_run_summary AS
# MAGIC SELECT
# MAGIC     CURRENT_TIMESTAMP AS run_timestamp,
# MAGIC     COUNT(DISTINCT fa.item_no) AS items_planned,
# MAGIC     COUNT(*) AS total_planning_lines,
# MAGIC 
# MAGIC     -- ACTION COUNTS
# MAGIC     SUM(CASE WHEN fa.action_message = 'New Order Required' THEN 1 ELSE 0 END) AS new_orders,
# MAGIC     SUM(CASE WHEN fa.action_message = 'Reduce/Cancel Orders' THEN 1 ELSE 0 END) AS cancel_reduce,
# MAGIC     SUM(CASE WHEN fa.action_message = 'No Action Needed' THEN 1 ELSE 0 END) AS no_action_needed,
# MAGIC     SUM(CASE WHEN fa.action_message LIKE 'Missing planning parameters%' THEN 1 ELSE 0 END) AS missing_parameters,
# MAGIC 
# MAGIC     -- STATUS COUNTS
# MAGIC     SUM(CASE WHEN fa.inventory_status = 'CRITICAL_SHORTAGE' THEN 1 ELSE 0 END) AS critical_shortages,
# MAGIC     SUM(CASE WHEN fa.inventory_status = 'BELOW_REORDER_POINT' THEN 1 ELSE 0 END) AS below_reorder_point,
# MAGIC     SUM(CASE WHEN fa.inventory_status = 'EXCESS_INVENTORY' THEN 1 ELSE 0 END) AS excess_inventory,
# MAGIC     SUM(CASE WHEN fa.inventory_status = 'OK' THEN 1 ELSE 0 END) AS status_ok,
# MAGIC     SUM(CASE WHEN fa.inventory_status = 'NO_ACTIVITY' THEN 1 ELSE 0 END) AS no_activity,
# MAGIC 
# MAGIC     -- QUANTITY TOTALS
# MAGIC     SUM(fa.final_suggested_quantity) AS total_quantity_to_order,
# MAGIC     SUM(fa.original_demand) AS total_demand,
# MAGIC     SUM(fa.original_supply) AS total_supply,
# MAGIC     
# MAGIC     -- PRODUCTION CONSUMPTION TOTALS
# MAGIC     SUM(fa.expected_consumption_uom1) AS total_expected_consumption,
# MAGIC     SUM(fa.already_consumed_uom1) AS total_already_consumed,
# MAGIC     SUM(fa.expected_consumption_uom1 - fa.already_consumed_uom1) AS total_remaining_to_consume,
# MAGIC 
# MAGIC     -- DATE RANGE
# MAGIC     MIN(fa.suggested_order_date) AS earliest_action_date,
# MAGIC     MAX(fa.action_date) AS latest_due_date,
# MAGIC 
# MAGIC     -- SOURCE BREAKDOWN
# MAGIC     SUM(fa.sales_demand_lines) AS total_sales_lines,
# MAGIC     SUM(fa.prod_comp_demand_lines) AS total_prod_comp_lines,
# MAGIC     SUM(fa.purchase_supply_lines) AS total_purchase_lines,
# MAGIC     SUM(fa.production_supply_lines) AS total_production_lines
# MAGIC     
# MAGIC FROM vw_final_planning_actions fa;
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- EXECUTION COMPLETE
# MAGIC -- ============================================================================
# MAGIC -- Query the output tables:
# MAGIC -- • mrp_planning_worksheet       → Action items for planners
# MAGIC -- • mrp_planning_worksheet_all   → Full diagnostic view
# MAGIC -- • mrp_exceptions               → Priority alerts
# MAGIC -- • mrp_run_summary              → Executive metrics
# MAGIC -- ============================================================================

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
