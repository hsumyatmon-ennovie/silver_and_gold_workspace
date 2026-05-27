# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "76781d83-17f8-4270-a81d-6759d1ee9a9d",
# META       "default_lakehouse_name": "Gold_Inventory_Lakehouse",
# META       "default_lakehouse_workspace_id": "d74457b3-045c-445d-82c6-9a2e4b9f1436",
# META       "known_lakehouses": [
# META         {
# META           "id": "76781d83-17f8-4270-a81d-6759d1ee9a9d"
# META         },
# META         {
# META           "id": "9c785c00-bff3-4379-a2f9-c17fd9df2974"
# META         },
# META         {
# META           "id": "1d620310-5acc-4534-93f9-f52f082a1887"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "e5cdc0c7-6c3a-46d5-8bb9-65942390419d",
# META       "known_warehouses": [
# META         {
# META           "id": "e5cdc0c7-6c3a-46d5-8bb9-65942390419d",
# META           "type": "Lakewarehouse"
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

from pyspark.sql import functions as F, Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold_Inventory_Lakehouse.mrp.gold_Item_Master

# CELL ********************

# MAGIC %%sql
# MAGIC -- =====================================================================
# MAGIC -- Notebook: nb_gold_item_master  (PATCHED — Bug Fix 2026-04-27)
# MAGIC -- Layer:    Bronze → Gold MRP
# MAGIC -- Output:   Gold_Inventory_Lakehouse.mrp.gold_Item_Master
# MAGIC --           Gold_Inventory_Lakehouse.mrp.gold_Item_Master_DQ_Issues
# MAGIC -- =====================================================================
# MAGIC -- 🐛 BUG FIX (2026-04-27):
# MAGIC --   Old archetype rule "WHEN item_category IN ('CASTING', 'ALLOY') THEN 'ALLOY'"
# MAGIC --   caused 8,887 CASTING items to be classified as ALLOY archetype, which then
# MAGIC --   created duplicate plans through both MPS BOM explosion (in V6) AND ALLOY
# MAGIC --   plan generation. This led to AC-CZ-000011 demand inflation 1,292 PCS vs
# MAGIC --   actual ~738 PCS (75% over-counted), and total system inflation in
# MAGIC --   fabric_requisition_line of 78.25M ฿ → ~30-40M ฿ expected.
# MAGIC --
# MAGIC --   Root cause: CASTING items have BOMs (SEMI level) and should be managed
# MAGIC --   by MPS BOM explosion alone. Only true raw alloy bullion (item_category
# MAGIC --   = 'ALLOY') should trigger ALLOY archetype plans.
# MAGIC --
# MAGIC --   Side-effect verified: Q-Side 5 confirmed 0 items will fall to UNKNOWN.
# MAGIC -- =====================================================================
# MAGIC -- HOW TO USE:
# MAGIC --   - แต่ละ "block" ที่คั่นด้วย ----- = 1 cell ใน Fabric notebook
# MAGIC --   - หรือ paste ทั้งไฟล์ใน cell เดียวก็ได้
# MAGIC --   - Run ตามลำดับ
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 1: Setup schema ==============
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS Gold_Inventory_Lakehouse.mrp;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 2: Spark settings ==============
# MAGIC -- Disable optimizeWrite (Ennovie known issue)
# MAGIC 
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 3: Build gold_Item_Master  ⭐ PATCHED ==============
# MAGIC -- 32 columns: 9 base + 21 planning + 2 helpers + Ennovie extras + DQ flags
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.gold_Item_Master
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH parsed AS (
# MAGIC     SELECT
# MAGIC         -- === Group 0: Identity ===
# MAGIC         `No.`                       AS item_no,
# MAGIC         Description                 AS description,
# MAGIC         `Item Category Code`        AS item_category,
# MAGIC         `Base Unit of Measure`      AS base_uom,
# MAGIC         `Replenishment System`      AS replenishment_system,
# MAGIC         `Unit Cost`                 AS unit_cost,
# MAGIC         `Last DateTime Modified`    AS last_modified,
# MAGIC         CAST(Blocked AS BOOLEAN)    AS is_blocked,
# MAGIC         
# MAGIC         -- === Group 1: Archetype Routing ===
# MAGIC         `Reordering Policy`         AS reordering_policy,
# MAGIC         `Manufacturing Policy`      AS mfg_policy,
# MAGIC         COALESCE(NULLIF(Reserve, ''), 'Never') AS reserve_policy,
# MAGIC         
# MAGIC         -- === Group 2: Exception Control ===
# MAGIC         CASE
# MAGIC             WHEN `Dampener Period` RLIKE '^[0-9]+D$' 
# MAGIC                 THEN CAST(REGEXP_EXTRACT(`Dampener Period`, '^([0-9]+)D$', 1) AS INT)
# MAGIC             WHEN `Dampener Period` RLIKE '^[0-9]+W$' 
# MAGIC                 THEN CAST(REGEXP_EXTRACT(`Dampener Period`, '^([0-9]+)W$', 1) AS INT) * 7
# MAGIC             WHEN `Dampener Period` RLIKE '^[0-9]+M$' 
# MAGIC                 THEN CAST(REGEXP_EXTRACT(`Dampener Period`, '^([0-9]+)M$', 1) AS INT) * 30
# MAGIC             WHEN `Dampener Period` RLIKE '^[0-9]+Y$' 
# MAGIC                 THEN CAST(REGEXP_EXTRACT(`Dampener Period`, '^([0-9]+)Y$', 1) AS INT) * 365
# MAGIC             ELSE 0
# MAGIC         END                         AS dampener_period_days,
# MAGIC         COALESCE(`Dampener Quantity`, 0)    AS dampener_qty,
# MAGIC         CAST(Critical AS BOOLEAN)           AS critical_flag,
# MAGIC         CASE
# MAGIC             WHEN `Item Tracking Code` LIKE '%LOT%' AND `Item Tracking Code` LIKE '%SN%' THEN 'Both'
# MAGIC             WHEN `Item Tracking Code` LIKE '%LOT%' THEN 'Lot'
# MAGIC             WHEN `Item Tracking Code` LIKE '%SN%'  THEN 'Serial'
# MAGIC             ELSE 'None'
# MAGIC         END                         AS tracking_policy,
# MAGIC         
# MAGIC         -- === Group 3: Netting ===
# MAGIC         COALESCE(`Safety Stock Quantity`, 0) AS safety_stock_qty,
# MAGIC         CASE
# MAGIC             WHEN `Safety Lead Time` RLIKE '^[0-9]+D$' 
# MAGIC                 THEN CAST(REGEXP_EXTRACT(`Safety Lead Time`, '^([0-9]+)D$', 1) AS INT)
# MAGIC             WHEN `Safety Lead Time` RLIKE '^[0-9]+W$' 
# MAGIC                 THEN CAST(REGEXP_EXTRACT(`Safety Lead Time`, '^([0-9]+)W$', 1) AS INT) * 7
# MAGIC             WHEN `Safety Lead Time` RLIKE '^[0-9]+M$' 
# MAGIC                 THEN CAST(REGEXP_EXTRACT(`Safety Lead Time`, '^([0-9]+)M$', 1) AS INT) * 30
# MAGIC             ELSE 0
# MAGIC         END                         AS safety_lead_days_static,
# MAGIC         COALESCE(CAST(`Include Inventory` AS BOOLEAN), TRUE) AS include_inventory,
# MAGIC         
# MAGIC         -- === Group 4: Lot Sizing ===
# MAGIC         COALESCE(`Reorder Point`, 0)      AS rop_qty,
# MAGIC         COALESCE(`Reorder Quantity`, 0)   AS roq_qty,
# MAGIC         COALESCE(`Maximum Inventory`, 0)  AS max_inv_qty,
# MAGIC         COALESCE(`Overflow Level`, 0)     AS overflow_qty,
# MAGIC         COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN `Lot Accumulation Period` RLIKE '^[0-9]+D$' 
# MAGIC                     THEN CAST(REGEXP_EXTRACT(`Lot Accumulation Period`, '^([0-9]+)D$', 1) AS INT)
# MAGIC                 WHEN `Lot Accumulation Period` RLIKE '^[0-9]+W$' 
# MAGIC                     THEN CAST(REGEXP_EXTRACT(`Lot Accumulation Period`, '^([0-9]+)W$', 1) AS INT) * 7
# MAGIC                 WHEN `Lot Accumulation Period` RLIKE '^[0-9]+M$' 
# MAGIC                     THEN CAST(REGEXP_EXTRACT(`Lot Accumulation Period`, '^([0-9]+)M$', 1) AS INT) * 30
# MAGIC                 ELSE NULL
# MAGIC             END,
# MAGIC             CASE WHEN `Item Category Code` = 'PEARL' THEN 14 ELSE 7 END
# MAGIC         )                           AS lot_accum_days,
# MAGIC         CASE
# MAGIC             WHEN `Rescheduling Period` RLIKE '^[0-9]+D$' 
# MAGIC                 THEN CAST(REGEXP_EXTRACT(`Rescheduling Period`, '^([0-9]+)D$', 1) AS INT)
# MAGIC             WHEN `Rescheduling Period` RLIKE '^[0-9]+W$' 
# MAGIC                 THEN CAST(REGEXP_EXTRACT(`Rescheduling Period`, '^([0-9]+)W$', 1) AS INT) * 7
# MAGIC             ELSE 0
# MAGIC         END                         AS reschedule_days,
# MAGIC         
# MAGIC         -- === Group 5: Order Shape ===
# MAGIC         COALESCE(`Minimum Order Quantity`, 0) AS min_order_qty,
# MAGIC         CASE 
# MAGIC             WHEN COALESCE(`Maximum Order Quantity`, 0) = 0 THEN 999999
# MAGIC             ELSE `Maximum Order Quantity`
# MAGIC         END                                   AS max_order_qty,
# MAGIC         CASE 
# MAGIC             WHEN COALESCE(`Order Multiple`, 0) = 0 THEN 1
# MAGIC             ELSE `Order Multiple`
# MAGIC         END                                   AS order_multiple,
# MAGIC         COALESCE(`Scrap %`, 0)                AS scrap_pct,
# MAGIC         
# MAGIC         -- === Group 6: Sourcing — static ===
# MAGIC         NULLIF(`Vendor No.`, '')              AS vendor_no,
# MAGIC         COALESCE(NULLIF(`Lead Time Calculation`, ''), '0D') AS lead_time_calc_raw,
# MAGIC         CASE
# MAGIC             WHEN `Lead Time Calculation` RLIKE '^[0-9]+D$' 
# MAGIC                 THEN CAST(REGEXP_EXTRACT(`Lead Time Calculation`, '^([0-9]+)D$', 1) AS INT)
# MAGIC             WHEN `Lead Time Calculation` RLIKE '^[0-9]+W$' 
# MAGIC                 THEN CAST(REGEXP_EXTRACT(`Lead Time Calculation`, '^([0-9]+)W$', 1) AS INT) * 7
# MAGIC             WHEN `Lead Time Calculation` RLIKE '^[0-9]+M$' 
# MAGIC                 THEN CAST(REGEXP_EXTRACT(`Lead Time Calculation`, '^([0-9]+)M$', 1) AS INT) * 30
# MAGIC             ELSE 0
# MAGIC         END                                   AS base_lead_days_static,
# MAGIC         COALESCE(NULLIF(`Purch. Unit of Measure`, ''), `Base Unit of Measure`) AS purch_uom,
# MAGIC         
# MAGIC         -- === Ennovie-specific extras ===
# MAGIC         Alloy                       AS alloy,
# MAGIC         `Alloy Type`                AS alloy_type,
# MAGIC         `Metal Category Code`       AS metal_category,
# MAGIC         `Material Type`             AS material_type,
# MAGIC         `Approval Status`           AS approval_status,
# MAGIC         `Production Blocked`        AS production_blocked,
# MAGIC         CAST(`Sales Blocked` AS BOOLEAN)        AS sales_blocked,
# MAGIC         CAST(`Purchasing Blocked` AS BOOLEAN)   AS purchasing_blocked
# MAGIC         
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item`
# MAGIC     WHERE `BC Company` = 'Ennovie'
# MAGIC ),
# MAGIC 
# MAGIC with_archetype AS (
# MAGIC     SELECT
# MAGIC         p.*,
# MAGIC         
# MAGIC         -- === Group 7: Computed helpers ===
# MAGIC         p.base_lead_days_static + p.safety_lead_days_static AS effective_lead_days,
# MAGIC         
# MAGIC         -- =====================================================================
# MAGIC         -- Archetype derivation — STRICT MODE  ⭐ PATCHED 2026-04-27
# MAGIC         -- 
# MAGIC         -- Philosophy: ระบบยึด logic + มาตรฐาน
# MAGIC         -- ถ้า user ใส่ข้อมูลไม่ครบ → flag UNKNOWN + DQ issue
# MAGIC         -- ไม่ใช้ silent fallback (จะทำให้ engine ใช้ข้อมูลผิดโดยไม่รู้ตัว)
# MAGIC         --
# MAGIC         -- 🐛 PATCH: CASTING items เปลี่ยนจาก 'ALLOY' archetype → 'MPS' หรือ 'L4L'
# MAGIC         --    ตามที่ Replenishment System บอก เพราะ CASTING มี BOM และเป็น SEMI level
# MAGIC         --    → ต้องผ่าน MPS BOM explosion เท่านั้น (ไม่ trigger ALLOY plan แยก)
# MAGIC         --    ALLOY archetype จะเหลือแค่ raw bullion (item_category = 'ALLOY') ที่ purchase
# MAGIC         --
# MAGIC         -- Required fields per category:
# MAGIC         --   - All items: Item Category Code, Description
# MAGIC         --   - MPS items: Replenishment System, Manufacturing Policy
# MAGIC         --   - L4L/ROP/ORDER items: Reordering Policy, Vendor No., Lead Time Calculation
# MAGIC         --   - ROP items: Reorder Point, Reorder Quantity
# MAGIC         -- =====================================================================
# MAGIC         CASE
# MAGIC             -- === SKIP: items นอก MRP scope (legitimate) ===
# MAGIC             -- MST (master molds), PLATING SOLUTION (chemical batch, internal)
# MAGIC             WHEN p.item_category IN ('MST', 'PLATING SOLUTION', 'MASTER') THEN 'SKIP'
# MAGIC             
# MAGIC             -- === ⭐ PATCHED: ALLOY = raw alloy bullion ONLY ===
# MAGIC             -- CASTING items moved to MPS (Prod.Order) or L4L (Purchase + L4L)
# MAGIC             -- to prevent double-counting in MRP demand explosion
# MAGIC             WHEN p.item_category = 'ALLOY' THEN 'ALLOY'
# MAGIC             
# MAGIC             -- === ⭐ PATCHED: MPS = FG + SEMI + CASTING produced internally ===
# MAGIC             -- CASTING items have BOMs (SEMI level) — managed via MPS BOM explosion
# MAGIC             WHEN p.item_category IN ('CASTING', 'FG', 'SEMI-FG', 'MIXED METAL') 
# MAGIC                  AND p.replenishment_system = 'Prod. Order' THEN 'MPS'
# MAGIC             
# MAGIC             -- === ⭐ PATCHED: L4L = Outsource casting (purchase from external) ===
# MAGIC             -- CASTING items with Purchase + Lot-for-Lot policy = outsourced
# MAGIC             WHEN p.item_category = 'CASTING' 
# MAGIC                  AND p.replenishment_system = 'Purchase' 
# MAGIC                  AND p.reordering_policy = 'Lot-for-Lot' THEN 'L4L'
# MAGIC             
# MAGIC             -- === ORDER: per-SO planning (DIAMOND NAT) ===
# MAGIC             WHEN p.item_category = 'DIAMOND NAT' THEN 'ORDER'
# MAGIC             WHEN p.reordering_policy = 'Order' THEN 'ORDER'
# MAGIC             
# MAGIC             -- === FINDINGS: must have proper Replenishment + Policy ===
# MAGIC             -- Purchase + Lot-for-Lot → L4L
# MAGIC             WHEN p.item_category = 'FINDINGS' 
# MAGIC                  AND p.replenishment_system = 'Purchase' 
# MAGIC                  AND p.reordering_policy = 'Lot-for-Lot' THEN 'L4L'
# MAGIC             -- Prod. Order → MPS (semi-finished produced internally)
# MAGIC             WHEN p.item_category = 'FINDINGS' 
# MAGIC                  AND p.replenishment_system = 'Prod. Order' THEN 'MPS'
# MAGIC             -- Anything else for FINDINGS = data incomplete → UNKNOWN
# MAGIC             
# MAGIC             -- === L4L: stones, beads, leather (must have Lot-for-Lot policy) ===
# MAGIC             WHEN p.item_category IN (
# MAGIC                 'DIAMONDS LAB', 'GEMSTONES', 'SYNT STONE', 'BEAD', 
# MAGIC                 'PEARLS', 'LEATHER'
# MAGIC             ) AND p.reordering_policy = 'Lot-for-Lot' THEN 'L4L'
# MAGIC             -- Stones without policy = data incomplete → UNKNOWN
# MAGIC             
# MAGIC             -- === ROP: packaging, consumables, supplies ===
# MAGIC             -- Category-based assignment is acceptable here because
# MAGIC             -- these categories are inherently ROP-managed in Ennovie
# MAGIC             WHEN p.item_category IN (
# MAGIC                 'PACKAGING', 'CONSUMABLES', 'PLATING CHEMICAL',
# MAGIC                 'STATIONARY', 'TOOLS', 'EQUIPMENT'
# MAGIC             ) THEN 'ROP'
# MAGIC             
# MAGIC             -- === Generic policy-based routing (last legitimate path) ===
# MAGIC             WHEN p.reordering_policy = 'Lot-for-Lot' THEN 'L4L'
# MAGIC             WHEN p.reordering_policy IN ('Fixed Reorder Qty.', 'Maximum Qty.') THEN 'ROP'
# MAGIC             
# MAGIC             -- === Everything else = data quality issue ===
# MAGIC             -- User ต้องไปแก้ใน BC: เลือก Reordering Policy ที่เหมาะสม
# MAGIC             -- หรือใส่ Replenishment System ให้ครบ
# MAGIC             ELSE 'UNKNOWN'
# MAGIC         END AS archetype
# MAGIC     FROM parsed p
# MAGIC ),
# MAGIC 
# MAGIC with_leadtime AS (
# MAGIC     SELECT
# MAGIC         i.*,
# MAGIC         -- vw_Vendor_Item_LeadTime not available — placeholder NULLs
# MAGIC         -- Uncomment LEFT JOIN below and remove these CASTs once view is accessible
# MAGIC         CAST(NULL AS INT)       AS lt_sample_size,
# MAGIC         CAST(NULL AS DECIMAL(10,2)) AS lt_avg_days,
# MAGIC         CAST(NULL AS DECIMAL(10,2)) AS lt_p95_days,
# MAGIC         CAST(NULL AS DECIMAL(10,2)) AS lt_stddev_days,
# MAGIC         CAST(NULL AS DOUBLE)    AS lt_recommended_normal_days,
# MAGIC         CAST(NULL AS DOUBLE)    AS lt_recommended_safety_days,
# MAGIC         CAST(NULL AS DOUBLE)    AS lt_recommended_total_days
# MAGIC     FROM with_archetype i
# MAGIC     -- Original join (re-enable when view is available):
# MAGIC     -- LEFT JOIN Silver_BC_Lakehouse.bc.vw_Vendor_Item_LeadTime lt
# MAGIC     --        ON i.item_no = lt.ItemNo
# MAGIC     --       AND i.vendor_no = lt.VendorNo
# MAGIC     --       AND lt.`BC Company` = 'Ennovie'
# MAGIC ),
# MAGIC 
# MAGIC with_dq AS (
# MAGIC     SELECT
# MAGIC         w.*,
# MAGIC         FILTER(ARRAY(
# MAGIC             -- DQ-1: Reserve policy must be 'Never' (Management doc rule)
# MAGIC             CASE WHEN reserve_policy <> 'Never' AND is_blocked = FALSE
# MAGIC                 THEN STRUCT(
# MAGIC                     'WRONG_RESERVE_POLICY' AS issue_code,
# MAGIC                     'HIGH' AS severity,
# MAGIC                     CONCAT('Reserve = ''', reserve_policy, ''' but Ennovie policy is ''Never''') AS message
# MAGIC                 )
# MAGIC             END,
# MAGIC             
# MAGIC             -- DQ-2: UNKNOWN archetype — user needs to fill in BC
# MAGIC             -- Engine จะ skip item นี้จนกว่าจะแก้ไข
# MAGIC             CASE WHEN archetype = 'UNKNOWN' AND is_blocked = FALSE
# MAGIC                 THEN STRUCT(
# MAGIC                     'INCOMPLETE_ITEM_DATA' AS issue_code,
# MAGIC                     'HIGH' AS severity,
# MAGIC                     CONCAT(
# MAGIC                         'User must complete item card in BC. ',
# MAGIC                         'Item Category=''',  COALESCE(item_category, 'MISSING'), ''', ',
# MAGIC                         'Reordering Policy=''', COALESCE(reordering_policy, 'MISSING'), ''', ',
# MAGIC                         'Replenishment System=''', COALESCE(replenishment_system, 'MISSING'), ''', ',
# MAGIC                         'Manufacturing Policy=''', COALESCE(mfg_policy, 'MISSING'), '''. ',
# MAGIC                         CASE 
# MAGIC                             WHEN COALESCE(item_category, '') = '' 
# MAGIC                                 THEN 'ACTION: Set Item Category Code first.'
# MAGIC                             WHEN item_category = 'FINDINGS' 
# MAGIC                                 THEN 'ACTION: Set both Replenishment System (Purchase or Prod. Order) AND Reordering Policy (Lot-for-Lot recommended for purchased findings).'
# MAGIC                             WHEN item_category IN ('DIAMONDS LAB', 'GEMSTONES', 'SYNT STONE', 'BEAD', 'PEARLS', 'LEATHER')
# MAGIC                                 THEN 'ACTION: Set Reordering Policy = Lot-for-Lot'
# MAGIC                             ELSE 'ACTION: Set Reordering Policy and verify Item Category Code is correct.'
# MAGIC                         END
# MAGIC                     ) AS message
# MAGIC                 )
# MAGIC             END,
# MAGIC             
# MAGIC             -- DQ-3: ROP needs both rop_qty and roq_qty
# MAGIC             CASE WHEN archetype = 'ROP' 
# MAGIC                  AND (rop_qty = 0 OR roq_qty = 0) 
# MAGIC                  AND is_blocked = FALSE
# MAGIC                 THEN STRUCT(
# MAGIC                     'MISSING_ROP_OR_ROQ' AS issue_code,
# MAGIC                     'MEDIUM' AS severity,
# MAGIC                     CONCAT('rop_qty=', CAST(rop_qty AS STRING), ', roq_qty=', CAST(roq_qty AS STRING)) AS message
# MAGIC                 )
# MAGIC             END,
# MAGIC             
# MAGIC             -- DQ-4: Purchase archetype but no vendor
# MAGIC             CASE WHEN archetype IN ('L4L', 'ROP', 'ORDER', 'ALLOY')
# MAGIC                  AND vendor_no IS NULL
# MAGIC                  AND is_blocked = FALSE
# MAGIC                 THEN STRUCT(
# MAGIC                     'MISSING_VENDOR' AS issue_code,
# MAGIC                     'MEDIUM' AS severity,
# MAGIC                     'Purchase archetype but no vendor on Item card' AS message
# MAGIC                 )
# MAGIC             END,
# MAGIC             
# MAGIC             -- DQ-5: Purchase archetype but no lead time
# MAGIC             CASE WHEN archetype IN ('L4L', 'ROP', 'ORDER')
# MAGIC                  AND base_lead_days_static = 0
# MAGIC                  AND is_blocked = FALSE
# MAGIC                 THEN STRUCT(
# MAGIC                     'MISSING_LEAD_TIME' AS issue_code,
# MAGIC                     'MEDIUM' AS severity,
# MAGIC                     'Purchase archetype but Lead Time Calculation is 0 or empty' AS message
# MAGIC                 )
# MAGIC             END,
# MAGIC             
# MAGIC             -- DQ-6: Static vs dynamic lead time mismatch (>50% diff)
# MAGIC             CASE WHEN lt_avg_days IS NOT NULL
# MAGIC                  AND base_lead_days_static > 0
# MAGIC                  AND ABS(lt_avg_days - base_lead_days_static) / base_lead_days_static > 0.5
# MAGIC                 THEN STRUCT(
# MAGIC                     'LEAD_TIME_DRIFT' AS issue_code,
# MAGIC                     'LOW' AS severity,
# MAGIC                     CONCAT(
# MAGIC                         'Static=', CAST(base_lead_days_static AS STRING), 'd, ',
# MAGIC                         'vendor avg=', CAST(lt_avg_days AS STRING), 'd. Consider updating BC.'
# MAGIC                     ) AS message
# MAGIC                 )
# MAGIC             END
# MAGIC         ), x -> x IS NOT NULL) AS dq_issues
# MAGIC     FROM with_leadtime w
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     *,
# MAGIC     SIZE(dq_issues) > 0    AS has_dq_issue,
# MAGIC     SIZE(dq_issues)        AS dq_issue_count,
# MAGIC     CURRENT_TIMESTAMP()    AS silver_loaded_at
# MAGIC FROM with_dq;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 4: Build DQ Issues table (flattened) ==============
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.gold_Item_Master_DQ_Issues
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     item_no,
# MAGIC     description,
# MAGIC     item_category,
# MAGIC     archetype,
# MAGIC     reserve_policy,
# MAGIC     vendor_no,
# MAGIC     issue.issue_code   AS issue_code,
# MAGIC     issue.severity     AS severity,
# MAGIC     issue.message      AS message,
# MAGIC     silver_loaded_at   AS detected_at
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Item_Master
# MAGIC LATERAL VIEW EXPLODE(dq_issues) AS issue
# MAGIC WHERE has_dq_issue = TRUE;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 5: Validation — row counts ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     'Bronze Item rows'              AS metric,
# MAGIC     COUNT(*)                        AS value
# MAGIC FROM Silver_BC_Lakehouse.bc.`Item`
# MAGIC WHERE `BC Company` = 'Ennovie'
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Gold Item Master rows',
# MAGIC     COUNT(*)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Item_Master
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Active items (not blocked)',
# MAGIC     COUNT(*)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Item_Master
# MAGIC WHERE is_blocked = FALSE
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Items with DQ issues',
# MAGIC     COUNT(*)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Item_Master
# MAGIC WHERE has_dq_issue = TRUE
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Total DQ issue records',
# MAGIC     COUNT(*)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Item_Master_DQ_Issues
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Duplicate item_no',
# MAGIC     COUNT(*) - COUNT(DISTINCT item_no)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Item_Master;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 6: ⭐ PATCH VERIFICATION — archetype distribution ==============
# MAGIC -- Run this immediately after rebuild to confirm patch worked
# MAGIC -- 
# MAGIC -- EXPECTED RESULT after patch:
# MAGIC --   archetype | total | active | casting | alloy
# MAGIC --   MPS       | ~26K  | 20,777 | 8,886   | 0       ← +CASTING moved here
# MAGIC --   ALLOY     | ~50   | 17     | 0       | 17      ← only true bullion
# MAGIC --   L4L       | ~5K   | 4,521  | 1       | 0       ← +1 outsource casting
# MAGIC --   SKIP      | 15,989| 10,280 | 0       | 0       (unchanged)
# MAGIC --   ROP       | 2,375 | 1,545  | 0       | 0       (unchanged)
# MAGIC --   ORDER     | 306   | 166    | 0       | 0       (unchanged)
# MAGIC --   UNKNOWN   | ~12K  | ~45    | 0       | 0       (unchanged)
# MAGIC 
# MAGIC SELECT 
# MAGIC     archetype,
# MAGIC     COUNT(*)                                              AS total_items,
# MAGIC     COUNT(CASE WHEN is_blocked = FALSE THEN 1 END)        AS active_items,
# MAGIC     COUNT(CASE WHEN item_category = 'CASTING' THEN 1 END) AS casting_items,
# MAGIC     COUNT(CASE WHEN item_category = 'ALLOY'   THEN 1 END) AS true_alloy_items,
# MAGIC     COUNT(CASE WHEN has_dq_issue = TRUE THEN 1 END)       AS with_dq_issues
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Item_Master
# MAGIC GROUP BY archetype
# MAGIC ORDER BY total_items DESC;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 7: Validation — DQ issues by code ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     issue_code,
# MAGIC     severity,
# MAGIC     COUNT(*) AS occurrences
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Item_Master_DQ_Issues
# MAGIC GROUP BY issue_code, severity
# MAGIC ORDER BY 
# MAGIC     CASE severity WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
# MAGIC     occurrences DESC;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 8: Sample 5 items per archetype ==============
# MAGIC 
# MAGIC WITH ranked AS (
# MAGIC     SELECT 
# MAGIC         item_no, description, item_category, archetype,
# MAGIC         reordering_policy, vendor_no, rop_qty, roq_qty,
# MAGIC         base_lead_days_static, lt_recommended_total_days,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY archetype ORDER BY item_no) AS rn
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Item_Master
# MAGIC     WHERE is_blocked = FALSE
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM ranked
# MAGIC WHERE rn <= 5
# MAGIC ORDER BY archetype, item_no;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 9: Action list — items ที่ user ต้องไปแก้ใน BC ==============
# MAGIC -- ใช้ output นี้เป็น checklist ให้ทีม Procurement / Planner แก้ใน BC
# MAGIC 
# MAGIC SELECT 
# MAGIC     item_category,
# MAGIC     reordering_policy,
# MAGIC     replenishment_system,
# MAGIC     mfg_policy,
# MAGIC     COUNT(*) AS items_affected,
# MAGIC     COLLECT_LIST(item_no) AS sample_items_first_5
# MAGIC FROM (
# MAGIC     SELECT 
# MAGIC         item_no, item_category, reordering_policy, 
# MAGIC         replenishment_system, mfg_policy,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY item_category, reordering_policy, replenishment_system, mfg_policy 
# MAGIC             ORDER BY item_no
# MAGIC         ) AS rn
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Item_Master
# MAGIC     WHERE archetype = 'UNKNOWN' 
# MAGIC       AND is_blocked = FALSE
# MAGIC )
# MAGIC WHERE rn <= 5
# MAGIC GROUP BY item_category, reordering_policy, replenishment_system, mfg_policy
# MAGIC ORDER BY items_affected DESC;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 10: Items ที่ user ต้องแก้ใน BC — full list ==============
# MAGIC -- Export ใช้ส่งให้ทีม Procurement / Planner ทำงานต่อ
# MAGIC 
# MAGIC SELECT 
# MAGIC     item_no,
# MAGIC     description,
# MAGIC     item_category,
# MAGIC     reordering_policy,
# MAGIC     replenishment_system,
# MAGIC     mfg_policy,
# MAGIC     -- Suggested action
# MAGIC     CASE 
# MAGIC         WHEN COALESCE(item_category, '') = '' 
# MAGIC             THEN 'Set Item Category Code'
# MAGIC         WHEN item_category = 'FINDINGS' 
# MAGIC             THEN 'Set Replenishment System + Reordering Policy (Lot-for-Lot)'
# MAGIC         WHEN item_category IN ('DIAMONDS LAB', 'GEMSTONES', 'SYNT STONE', 'BEAD', 'PEARLS', 'LEATHER')
# MAGIC             THEN 'Set Reordering Policy = Lot-for-Lot'
# MAGIC         ELSE 'Review Item Category and Reordering Policy'
# MAGIC     END AS suggested_action
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Item_Master
# MAGIC WHERE archetype = 'UNKNOWN' 
# MAGIC   AND is_blocked = FALSE
# MAGIC ORDER BY item_category, item_no;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold_Inventory_Lakehouse.mrp.gold_SO

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- =====================================================================
# MAGIC -- Notebook: nb_gold_so  (Pure Spark SQL)
# MAGIC -- Layer:    Bronze → Gold MRP
# MAGIC -- Output:   Gold_Inventory_Lakehouse.mrp.gold_SO
# MAGIC -- Run order: หลัง nb_gold_item_master
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 1: Setup schema ==============
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS Gold_Inventory_Lakehouse.mrp;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 2: Build gold_SO ==============
# MAGIC -- Joins: Sales Header + Sales Line + Customer + gold_Item_Master
# MAGIC -- Filter: Document Type = 'Order' (excludes quotes, blanket orders, returns)
# MAGIC -- 1 row per Sales Line with denormalized Header + Customer + Item info
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.gold_SO
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH header AS (
# MAGIC     SELECT
# MAGIC         `No.`                       AS so_no,
# MAGIC         `Sell-to Customer No.`      AS customer_no,
# MAGIC         `Bill-to Customer No.`      AS bill_to_customer_no,
# MAGIC         `Sell-to Customer Name`     AS sell_to_customer_name,
# MAGIC         Status                      AS status,
# MAGIC         `Order Date`                AS order_date,
# MAGIC         `Document Date`             AS document_date,
# MAGIC         `Posting Date`              AS posting_date,
# MAGIC         `Shipment Date`             AS header_shipment_date,
# MAGIC         `Requested Delivery Date`   AS header_requested_delivery_date,
# MAGIC         `Promised Delivery Date`    AS header_promised_delivery_date,
# MAGIC         `Currency Code`             AS currency_code,
# MAGIC         `Currency Factor`           AS currency_factor,
# MAGIC         `Gold Per Ounce`            AS gold_per_ounce_at_so,
# MAGIC         `Silver Per Ounce`          AS silver_per_ounce_at_so,
# MAGIC         CAST(`Do not delay` AS BOOLEAN) AS dnd_flag,
# MAGIC         `Do not exceed date`        AS dnd_exceed_date,
# MAGIC         `GROSS WT.`                 AS gross_wt,
# MAGIC         Carton                      AS carton,
# MAGIC         `Prod. Order Type.`         AS prod_order_type,
# MAGIC         Remark                      AS so_remark,
# MAGIC         `Released Date`             AS released_date,
# MAGIC         `Reopen Date`               AS reopen_date,
# MAGIC         Reserve                     AS header_reserve_policy,
# MAGIC         `Salesperson Code`          AS salesperson_code,
# MAGIC         `On Hold`                   AS on_hold,
# MAGIC         `Customer Posting Group`    AS customer_posting_group,
# MAGIC         `Gen. Bus. Posting Group`   AS gen_bus_posting_group,
# MAGIC         `Ship-to Code`              AS ship_to_code,
# MAGIC         `Ship-to Country/Region Code` AS ship_to_country,
# MAGIC         `External Document No.`     AS external_doc_no,
# MAGIC         `Quote No.`                 AS source_quote_no,
# MAGIC         `SystemModifiedAt`          AS header_last_modified
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Sales Header`
# MAGIC     WHERE `BC Company` = 'Ennovie'
# MAGIC       AND `Document Type` = 'Order'
# MAGIC ),
# MAGIC 
# MAGIC line AS (
# MAGIC     SELECT
# MAGIC         `Document No.`              AS so_no,
# MAGIC         `Line No.`                  AS so_line_no,
# MAGIC         Type                        AS line_type,
# MAGIC         `No.`                       AS item_no,
# MAGIC         Description                 AS description,
# MAGIC         `Item Category Code`        AS line_item_category,
# MAGIC         `Variant Code`              AS variant_code,
# MAGIC         `Location Code`             AS location_code,
# MAGIC         Quantity                    AS qty_ordered,
# MAGIC         `Quantity Shipped`          AS qty_shipped,
# MAGIC         `Outstanding Quantity`      AS qty_outstanding,
# MAGIC         `Qty. Shipped Not Invoiced` AS qty_shipped_not_invoiced,
# MAGIC         `Unit of Measure Code`      AS uom,
# MAGIC         `Qty. per Unit of Measure`  AS qty_per_uom,
# MAGIC         `Shipment Date`             AS line_shipment_date,
# MAGIC         `Requested Delivery Date`   AS line_requested_delivery_date,
# MAGIC         `Promised Delivery Date`    AS line_promised_delivery_date,
# MAGIC         `Planned Delivery Date`     AS line_planned_delivery_date,
# MAGIC         `Planned Shipment Date`     AS line_planned_shipment_date,
# MAGIC         `Unit Price`                AS unit_price,
# MAGIC         `Line Amount`               AS line_amount,
# MAGIC         `Unit Cost`                 AS unit_cost,
# MAGIC         `Line Discount %`           AS line_discount_pct,
# MAGIC         `Line Discount Amount`      AS line_discount_amount,
# MAGIC         Reserve                     AS line_reserve_policy,
# MAGIC         CAST(`Special Order` AS BOOLEAN) AS is_special_order,
# MAGIC         `Special Order Purchase No.`     AS special_order_po_no,
# MAGIC         `Shipment No.`              AS shipment_no,
# MAGIC         `Purchase Order No.`        AS linked_po_no,
# MAGIC         CAST(`Drop Shipment` AS BOOLEAN) AS is_drop_shipment,
# MAGIC         `Shortcut Dimension 1 Code` AS dim1_code,
# MAGIC         `Shortcut Dimension 2 Code` AS dim2_code,
# MAGIC         `Diamond Price`             AS diamond_price,
# MAGIC         `Stone Price`               AS stone_price,
# MAGIC         `Metal Price`               AS metal_price,
# MAGIC         `Finding Price`             AS finding_price,
# MAGIC         `Labour Price`              AS labour_price,
# MAGIC         `SystemModifiedAt`          AS line_last_modified
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Sales Line`
# MAGIC     WHERE `BC Company` = 'Ennovie'
# MAGIC       AND `Document Type` = 'Order'
# MAGIC ),
# MAGIC 
# MAGIC customer AS (
# MAGIC     SELECT
# MAGIC         `No.`                   AS customer_no,
# MAGIC         Name                    AS customer_name,
# MAGIC         `Country/Region Code`   AS customer_country,
# MAGIC         `Customer Posting Group` AS posting_group,
# MAGIC         Priority                AS customer_priority,
# MAGIC         Blocked                 AS customer_blocked
# MAGIC     FROM Silver_BC_Lakehouse.bc.Customer
# MAGIC     WHERE `BC Company` = 'Ennovie'
# MAGIC ),
# MAGIC 
# MAGIC item_master AS (
# MAGIC     SELECT 
# MAGIC         item_no,
# MAGIC         archetype       AS item_archetype,
# MAGIC         is_blocked      AS item_is_blocked
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Item_Master
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     -- === Identity ===
# MAGIC     l.so_no,
# MAGIC     l.so_line_no,
# MAGIC     h.status,
# MAGIC     
# MAGIC     -- === Open SO logic ===
# MAGIC     (l.qty_outstanding > 0 
# MAGIC         AND h.status NOT IN ('Closed', 'Cancelled')
# MAGIC         AND COALESCE(h.on_hold, '') = ''
# MAGIC     ) AS is_open,
# MAGIC     
# MAGIC     -- === Customer ===
# MAGIC     h.customer_no,
# MAGIC     c.customer_name,
# MAGIC     -- Customer tier mapping — Ennovie actual customers (22 active)
# MAGIC     -- Tier 1 (priority weight 250-500): strategic top accounts
# MAGIC     -- Tier 2 (priority weight 100-200): regular accounts
# MAGIC     -- Tier 3 (priority weight 50-100): minor / occasional accounts
# MAGIC     -- INTERNAL: not a paying customer (internal transfers)
# MAGIC     -- DISTRIBUTOR: 3rd-party distributors (BTB, DHTG)
# MAGIC     CASE
# MAGIC         -- INTERNAL
# MAGIC         WHEN h.customer_no = 'CD-00004' THEN 'ENNOVIE_INT'   -- Ennovie internal
# MAGIC         WHEN h.customer_no = 'CD-00003' THEN 'BKP'           -- Bangkok Kraft
# MAGIC         
# MAGIC         -- Tier 1
# MAGIC         WHEN h.customer_no = 'CI-00002' THEN 'DB'            -- De Beers Jewellers
# MAGIC         WHEN h.customer_no = 'CI-00022' THEN 'MV'            -- Monica Vinader
# MAGIC         WHEN h.customer_no = 'CI-00020' THEN 'MS'            -- Missoma
# MAGIC         WHEN h.customer_no = 'CI-00042' THEN 'AOL'           -- Aspinal of London
# MAGIC         WHEN h.customer_no = 'CI-00040' THEN 'VW'            -- Vivienne Westwood
# MAGIC         
# MAGIC         -- Tier 2
# MAGIC         WHEN h.customer_no = 'CI-00008' THEN 'SZ'            -- Sézane
# MAGIC         WHEN h.customer_no = 'CI-00048' THEN 'AL'            -- Abbott Lyon
# MAGIC         WHEN h.customer_no = 'CI-00051' THEN 'AG'            -- Agapée
# MAGIC         WHEN h.customer_no = 'CI-00029' THEN 'AM'            -- Astrid & Miyu
# MAGIC         WHEN h.customer_no = 'CI-00009' THEN 'KM'            -- Kimai
# MAGIC         WHEN h.customer_no IN ('CI-00016', 'CI-00011') THEN 'CC'  -- Clocks and Colours (UK + EU)
# MAGIC         WHEN h.customer_no = 'CI-00047' THEN 'AP'            -- Agatha Paris
# MAGIC         
# MAGIC         -- Tier 3
# MAGIC         WHEN h.customer_no = 'CI-00027' THEN 'TGF'           -- The Great Frog
# MAGIC         WHEN h.customer_no = 'CI-00004' THEN 'GS'            -- Guess Europe
# MAGIC         WHEN h.customer_no = 'CI-00044' THEN 'RB'            -- Rag & Bone
# MAGIC         WHEN h.customer_no = 'CI-00049' THEN 'DS'            -- Dorsey
# MAGIC         
# MAGIC         -- Distributors
# MAGIC         WHEN h.customer_no = 'CI-00013' THEN 'BTB'           -- BTB B.V. (Netherlands)
# MAGIC         WHEN h.customer_no = 'CI-00018' THEN 'DHTG'          -- DHTG Ltd
# MAGIC         
# MAGIC         -- Pattern fallback for unmapped (will appear as DQ if unmapped)
# MAGIC         WHEN h.customer_no LIKE 'CD-%' THEN 'OTHER_DOMESTIC'
# MAGIC         WHEN h.customer_no LIKE 'CI-%' THEN 'OTHER_INTL'
# MAGIC         WHEN COALESCE(h.customer_no, '') = '' THEN 'NO_CUSTOMER'
# MAGIC         ELSE 'UNMAPPED'
# MAGIC     END AS customer_tier_code,
# MAGIC     -- Numeric priority weight (used by MRP engine for conflict resolution)
# MAGIC     -- Per management doc: DB=500, MV=450, MS=250, VW=200
# MAGIC     -- Other tiers extrapolated proportionally
# MAGIC     CASE
# MAGIC         -- INTERNAL / Strategic
# MAGIC         WHEN h.customer_no = 'CD-00004' THEN 100             -- Ennovie internal (low priority — internal transfers)
# MAGIC         WHEN h.customer_no = 'CD-00003' THEN 400             -- Bangkok Kraft (strategic partner)
# MAGIC         
# MAGIC         -- Tier 1 (per management doc)
# MAGIC         WHEN h.customer_no = 'CI-00002' THEN 500             -- De Beers
# MAGIC         WHEN h.customer_no = 'CI-00022' THEN 450             -- Monica Vinader
# MAGIC         WHEN h.customer_no = 'CI-00020' THEN 250             -- Missoma
# MAGIC         WHEN h.customer_no = 'CI-00042' THEN 200             -- Aspinal of London
# MAGIC         WHEN h.customer_no = 'CI-00040' THEN 200             -- Vivienne Westwood
# MAGIC         
# MAGIC         -- Tier 2
# MAGIC         WHEN h.customer_no IN ('CI-00008', 'CI-00048', 'CI-00051', 'CI-00029', 'CI-00009', 
# MAGIC                                 'CI-00016', 'CI-00011', 'CI-00047') THEN 150
# MAGIC         
# MAGIC         -- Tier 3
# MAGIC         WHEN h.customer_no IN ('CI-00027', 'CI-00004', 'CI-00044', 'CI-00049') THEN 75
# MAGIC         
# MAGIC         -- Distributors
# MAGIC         WHEN h.customer_no IN ('CI-00013', 'CI-00018') THEN 100
# MAGIC         
# MAGIC         -- Default
# MAGIC         ELSE 50
# MAGIC     END AS customer_priority_weight,
# MAGIC     c.customer_country,
# MAGIC     c.customer_priority,
# MAGIC     h.customer_posting_group,
# MAGIC     h.salesperson_code,
# MAGIC     h.ship_to_code,
# MAGIC     h.ship_to_country,
# MAGIC     
# MAGIC     -- === Item ===
# MAGIC     l.item_no,
# MAGIC     l.description,
# MAGIC     l.line_item_category    AS item_category,
# MAGIC     im.item_archetype,
# MAGIC     l.variant_code,
# MAGIC     l.location_code,
# MAGIC     l.uom,
# MAGIC     
# MAGIC     -- === Quantity ===
# MAGIC     l.qty_ordered,
# MAGIC     l.qty_shipped,
# MAGIC     l.qty_outstanding,
# MAGIC     l.qty_per_uom,
# MAGIC     
# MAGIC     -- === Dates ===
# MAGIC     h.order_date,
# MAGIC     h.document_date,
# MAGIC     -- Ship date: line-level → header-level fallback
# MAGIC     COALESCE(
# MAGIC         l.line_promised_delivery_date,
# MAGIC         l.line_requested_delivery_date,
# MAGIC         l.line_planned_shipment_date,
# MAGIC         l.line_shipment_date,
# MAGIC         h.header_promised_delivery_date,
# MAGIC         h.header_requested_delivery_date,
# MAGIC         h.header_shipment_date
# MAGIC     ) AS ship_date,
# MAGIC     COALESCE(l.line_requested_delivery_date, h.header_requested_delivery_date) AS requested_ship_date,
# MAGIC     COALESCE(l.line_promised_delivery_date, h.header_promised_delivery_date)   AS promised_ship_date,
# MAGIC     DATEDIFF(
# MAGIC         COALESCE(
# MAGIC             l.line_promised_delivery_date,
# MAGIC             l.line_requested_delivery_date,
# MAGIC             h.header_promised_delivery_date,
# MAGIC             h.header_requested_delivery_date
# MAGIC         ),
# MAGIC         CURRENT_DATE()
# MAGIC     ) AS days_until_ship,
# MAGIC     h.released_date,
# MAGIC     
# MAGIC     -- === Pricing — currency ===
# MAGIC     h.currency_code,
# MAGIC     h.currency_factor,
# MAGIC     l.unit_price,
# MAGIC     l.unit_cost,
# MAGIC     l.line_discount_pct,
# MAGIC     l.line_discount_amount,
# MAGIC     l.line_amount,
# MAGIC     -- THB-converted line amount
# MAGIC     CASE
# MAGIC         WHEN h.currency_code IS NULL OR h.currency_code = '' OR h.currency_code = 'THB'
# MAGIC             THEN l.line_amount
# MAGIC         ELSE l.line_amount * COALESCE(h.currency_factor, 1.0)
# MAGIC     END AS line_amount_thb,
# MAGIC     
# MAGIC     -- === London Fix snapshot at SO time ===
# MAGIC     h.gold_per_ounce_at_so,
# MAGIC     h.silver_per_ounce_at_so,
# MAGIC     
# MAGIC     -- === Ennovie pricing breakdown ===
# MAGIC     l.diamond_price,
# MAGIC     l.stone_price,
# MAGIC     l.metal_price,
# MAGIC     l.finding_price,
# MAGIC     l.labour_price,
# MAGIC     h.gross_wt,
# MAGIC     h.carton,
# MAGIC     
# MAGIC     -- === Priority signals ===
# MAGIC     h.dnd_flag,
# MAGIC     COALESCE(h.dnd_flag, FALSE) AS is_dnd,
# MAGIC     h.dnd_exceed_date,
# MAGIC     h.on_hold,
# MAGIC     
# MAGIC     -- === Reserve policy (line override > header) ===
# MAGIC     COALESCE(
# MAGIC         NULLIF(l.line_reserve_policy, ''),
# MAGIC         h.header_reserve_policy,
# MAGIC         'Never'
# MAGIC     ) AS reserve_policy_effective,
# MAGIC     h.header_reserve_policy,
# MAGIC     l.line_reserve_policy,
# MAGIC     
# MAGIC     -- === Linked documents ===
# MAGIC     l.is_special_order,
# MAGIC     l.special_order_po_no,
# MAGIC     l.is_drop_shipment,
# MAGIC     l.linked_po_no,
# MAGIC     h.source_quote_no,
# MAGIC     h.external_doc_no,
# MAGIC     l.shipment_no,
# MAGIC     
# MAGIC     -- === Custom fields ===
# MAGIC     h.prod_order_type,
# MAGIC     h.so_remark,
# MAGIC     l.dim1_code,
# MAGIC     l.dim2_code,
# MAGIC     
# MAGIC     -- === Metadata ===
# MAGIC     h.header_last_modified,
# MAGIC     l.line_last_modified,
# MAGIC     CURRENT_TIMESTAMP() AS silver_loaded_at
# MAGIC 
# MAGIC FROM line l
# MAGIC INNER JOIN header h
# MAGIC         ON l.so_no = h.so_no
# MAGIC LEFT JOIN customer c
# MAGIC        ON h.customer_no = c.customer_no
# MAGIC LEFT JOIN item_master im
# MAGIC        ON l.item_no = im.item_no;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 3: Validation — row counts ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     'Bronze Sales Header (Order)' AS metric,
# MAGIC     COUNT(*) AS value
# MAGIC FROM Silver_BC_Lakehouse.bc.`Sales Header`
# MAGIC WHERE `BC Company` = 'Ennovie' AND `Document Type` = 'Order'
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Bronze Sales Line (Order)',
# MAGIC     COUNT(*)
# MAGIC FROM Silver_BC_Lakehouse.bc.`Sales Line`
# MAGIC WHERE `BC Company` = 'Ennovie' AND `Document Type` = 'Order'
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Gold SO total rows',
# MAGIC     COUNT(*)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_SO
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Gold SO open rows',
# MAGIC     COUNT(*)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_SO
# MAGIC WHERE is_open = TRUE
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Open SO with DND flag',
# MAGIC     COUNT(*)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_SO
# MAGIC WHERE is_open = TRUE AND is_dnd = TRUE
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Duplicate (so_no, line_no)',
# MAGIC     COUNT(*) - COUNT(DISTINCT CONCAT(so_no, '-', so_line_no))
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_SO;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 4: Validation — status distribution ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     status,
# MAGIC     COUNT(*) AS line_count,
# MAGIC     SUM(qty_outstanding) AS total_outstanding_qty,
# MAGIC     SUM(line_amount_thb) AS total_amount_thb
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_SO
# MAGIC GROUP BY status
# MAGIC ORDER BY line_count DESC;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 5: Validation — open SO by customer tier ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     customer_tier_code,
# MAGIC     MAX(customer_priority_weight) AS priority_weight,
# MAGIC     COUNT(*) AS open_lines,
# MAGIC     COUNT(DISTINCT so_no) AS open_so_count,
# MAGIC     COUNT(DISTINCT customer_no) AS unique_customers,
# MAGIC     SUM(qty_outstanding) AS total_qty,
# MAGIC     ROUND(SUM(line_amount_thb), 2) AS total_amount_thb
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_SO
# MAGIC WHERE is_open = TRUE
# MAGIC GROUP BY customer_tier_code
# MAGIC ORDER BY priority_weight DESC NULLS LAST, total_amount_thb DESC NULLS LAST;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 6: Customer mapping check ==============
# MAGIC -- ใช้ output นี้ปรับ tier mapping ใน CELL 2 ตามรหัสลูกค้าจริง
# MAGIC 
# MAGIC SELECT 
# MAGIC     customer_tier_code,
# MAGIC     customer_no,
# MAGIC     customer_name,
# MAGIC     COUNT(*) AS so_lines
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_SO
# MAGIC WHERE is_open = TRUE
# MAGIC GROUP BY customer_tier_code, customer_no, customer_name
# MAGIC ORDER BY customer_tier_code, customer_no;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 7: Items missing in master ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     item_no,
# MAGIC     description,
# MAGIC     COUNT(*) AS so_lines,
# MAGIC     SUM(qty_outstanding) AS total_qty
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_SO
# MAGIC WHERE is_open = TRUE
# MAGIC   AND item_archetype IS NULL
# MAGIC GROUP BY item_no, description
# MAGIC ORDER BY so_lines DESC;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 8: Top 20 open SO lines by revenue ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     so_no,
# MAGIC     so_line_no,
# MAGIC     customer_no,
# MAGIC     customer_tier_code,
# MAGIC     is_dnd,
# MAGIC     item_no,
# MAGIC     description,
# MAGIC     qty_outstanding,
# MAGIC     ship_date,
# MAGIC     days_until_ship,
# MAGIC     line_amount_thb,
# MAGIC     item_archetype
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_SO
# MAGIC WHERE is_open = TRUE
# MAGIC ORDER BY line_amount_thb DESC NULLS LAST
# MAGIC LIMIT 20;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold_Inventory_Lakehouse.mrp.gold_Inventory (partitioned by item_category)

# CELL ********************

# MAGIC %%sql
# MAGIC -- =====================================================================
# MAGIC -- Notebook: nb_gold_inventory  (Pure Spark SQL)
# MAGIC -- Layer:    Bronze → Gold MRP
# MAGIC -- Output:   Gold_Inventory_Lakehouse.mrp.gold_Inventory (partitioned by item_category)
# MAGIC -- Run order: หลัง nb_gold_item_master
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 1: Setup schema ==============
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS Gold_Inventory_Lakehouse.mrp;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 2: Spark settings ==============
# MAGIC -- Disable optimizeWrite (Ennovie known issue with partitioned Delta + JOINs)
# MAGIC -- Set Parquet datetime rebase mode (Ennovie known issue with old date encoding)
# MAGIC 
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInWrite = CORRECTED;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 3: Build gold_Inventory ==============
# MAGIC -- Strategy: ใช้ Remaining Quantity > 0 AND Open = true แทนการ aggregate full ILE history
# MAGIC -- Grain: 1 row per (item × location × lot × serial × variant)
# MAGIC -- Excludes: BROK-MAT, BLOCK, QC-HOLD locations (configurable)
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.gold_Inventory
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (item_category)
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'false'
# MAGIC )
# MAGIC AS
# MAGIC WITH ile_open AS (
# MAGIC     -- ดึงเฉพาะ entries ที่ยัง open + remaining > 0 (เร็วกว่า aggregate full history)
# MAGIC     SELECT
# MAGIC         `Item No.`              AS item_no,
# MAGIC         `Location Code`         AS location_code,
# MAGIC         NULLIF(`Lot No.`, '')   AS lot_no,
# MAGIC         NULLIF(`Serial No.`, '') AS serial_no,
# MAGIC         NULLIF(`Variant Code`, '') AS variant_code,
# MAGIC         `Posting Date`          AS posting_date,
# MAGIC         `Remaining Quantity`    AS remaining_qty,
# MAGIC         `Entry No.`             AS entry_no,
# MAGIC         `Expiration Date`       AS expiration_date
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry`
# MAGIC     WHERE `BC Company` = 'Ennovie'
# MAGIC       AND `Remaining Quantity` > 0
# MAGIC       AND `Open` = TRUE
# MAGIC ),
# MAGIC 
# MAGIC aggregated AS (
# MAGIC     -- Aggregate ที่ grain (item × location × lot × serial × variant)
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         location_code,
# MAGIC         lot_no,
# MAGIC         serial_no,
# MAGIC         variant_code,
# MAGIC         SUM(remaining_qty)                                              AS qty_on_hand,
# MAGIC         COUNT(*)                                                        AS ile_entry_count,
# MAGIC         MIN(posting_date)                                               AS oldest_entry_date,
# MAGIC         MAX(posting_date)                                               AS last_movement_date,
# MAGIC         MIN(CASE WHEN expiration_date > DATE'0001-01-01' 
# MAGIC                  THEN expiration_date END)                              AS earliest_expiration_date,
# MAGIC         MIN(entry_no)                                                   AS earliest_entry_no
# MAGIC     FROM ile_open
# MAGIC     GROUP BY item_no, location_code, lot_no, serial_no, variant_code
# MAGIC     HAVING SUM(remaining_qty) > 0
# MAGIC ),
# MAGIC 
# MAGIC with_item AS (
# MAGIC     -- Join with Item Master for archetype + tracking_policy + denorm
# MAGIC     SELECT
# MAGIC         a.item_no,
# MAGIC         im.description              AS item_description,
# MAGIC         im.item_category,
# MAGIC         im.archetype,
# MAGIC         im.material_type,
# MAGIC         im.alloy_type,
# MAGIC         im.metal_category,
# MAGIC         a.location_code,
# MAGIC         -- Blocked location flag
# MAGIC         CASE 
# MAGIC             WHEN a.location_code IN ('BROK-MAT', 'BLOCK', 'QC-HOLD') THEN TRUE 
# MAGIC             ELSE FALSE 
# MAGIC         END AS is_blocked_location,
# MAGIC         a.lot_no,
# MAGIC         a.serial_no,
# MAGIC         a.variant_code,
# MAGIC         im.tracking_policy,
# MAGIC         a.qty_on_hand,
# MAGIC         -- Available qty (excludes blocked locations)
# MAGIC         CASE 
# MAGIC             WHEN a.location_code IN ('BROK-MAT', 'BLOCK', 'QC-HOLD') THEN 0.0
# MAGIC             ELSE a.qty_on_hand
# MAGIC         END AS qty_available,
# MAGIC         im.base_uom,
# MAGIC         im.unit_cost                AS estimated_unit_cost,
# MAGIC         a.qty_on_hand * COALESCE(im.unit_cost, 0)   AS estimated_total_cost,
# MAGIC         a.oldest_entry_date,
# MAGIC         a.last_movement_date,
# MAGIC         DATEDIFF(CURRENT_DATE(), a.oldest_entry_date) AS days_in_stock,
# MAGIC         -- Stale: >180 days, not in MPS production scope
# MAGIC         ((DATEDIFF(CURRENT_DATE(), a.oldest_entry_date) > 180) 
# MAGIC          AND (im.archetype <> 'MPS' OR im.archetype IS NULL)) AS is_stale,
# MAGIC         a.earliest_expiration_date,
# MAGIC         -- Expiration alerts
# MAGIC         (a.earliest_expiration_date IS NOT NULL 
# MAGIC          AND DATEDIFF(a.earliest_expiration_date, CURRENT_DATE()) <= 30
# MAGIC          AND DATEDIFF(a.earliest_expiration_date, CURRENT_DATE()) >= 0) AS expires_within_30_days,
# MAGIC         (a.earliest_expiration_date IS NOT NULL 
# MAGIC          AND a.earliest_expiration_date < CURRENT_DATE()) AS is_expired,
# MAGIC         -- Tracking consistency check
# MAGIC         CASE
# MAGIC             WHEN im.tracking_policy = 'Lot' AND a.lot_no IS NULL THEN 'MISSING_LOT'
# MAGIC             WHEN im.tracking_policy = 'Serial' AND a.serial_no IS NULL THEN 'MISSING_SERIAL'
# MAGIC             WHEN im.tracking_policy = 'None' 
# MAGIC                  AND (a.lot_no IS NOT NULL OR a.serial_no IS NOT NULL) THEN 'UNEXPECTED_TRACKING'
# MAGIC             ELSE 'OK'
# MAGIC         END AS tracking_match,
# MAGIC         a.ile_entry_count,
# MAGIC         a.earliest_entry_no,
# MAGIC         im.is_blocked   AS item_is_blocked,
# MAGIC         CURRENT_TIMESTAMP() AS silver_loaded_at
# MAGIC     FROM aggregated a
# MAGIC     LEFT JOIN Gold_Inventory_Lakehouse.mrp.gold_Item_Master im
# MAGIC            ON a.item_no = im.item_no
# MAGIC )
# MAGIC 
# MAGIC SELECT * FROM with_item;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 4: Validation — row counts and totals ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     'Bronze open ILE rows' AS metric,
# MAGIC     COUNT(*) AS value
# MAGIC FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry`
# MAGIC WHERE `BC Company` = 'Ennovie' 
# MAGIC   AND `Remaining Quantity` > 0 
# MAGIC   AND `Open` = TRUE
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Gold Inventory rows',
# MAGIC     COUNT(*)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Inventory
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Total qty_on_hand',
# MAGIC     CAST(SUM(qty_on_hand) AS BIGINT)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Inventory
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Total qty_available',
# MAGIC     CAST(SUM(qty_available) AS BIGINT)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Inventory
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Locked in blocked locations',
# MAGIC     CAST(SUM(qty_on_hand - qty_available) AS BIGINT)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Inventory
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Stale items (>180d, non-MPS)',
# MAGIC     COUNT(*)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Inventory
# MAGIC WHERE is_stale = TRUE
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Expiring within 30 days',
# MAGIC     COUNT(*)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Inventory
# MAGIC WHERE expires_within_30_days = TRUE
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Already expired',
# MAGIC     COUNT(*)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Inventory
# MAGIC WHERE is_expired = TRUE
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Items missing in master',
# MAGIC     COUNT(*)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Inventory
# MAGIC WHERE archetype IS NULL;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 5: Top 15 locations by qty ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     location_code,
# MAGIC     is_blocked_location,
# MAGIC     COUNT(*) AS rows_count,
# MAGIC     ROUND(SUM(qty_on_hand), 2) AS qty_on_hand,
# MAGIC     COUNT(DISTINCT item_no) AS unique_items
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Inventory
# MAGIC GROUP BY location_code, is_blocked_location
# MAGIC ORDER BY qty_on_hand DESC
# MAGIC LIMIT 15;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 6: Inventory by archetype ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     archetype,
# MAGIC     COUNT(*) AS rows_count,
# MAGIC     ROUND(SUM(qty_on_hand), 2) AS qty_on_hand,
# MAGIC     ROUND(SUM(qty_available), 2) AS qty_available,
# MAGIC     COUNT(DISTINCT item_no) AS unique_items,
# MAGIC     ROUND(SUM(estimated_total_cost), 2) AS estimated_total_cost
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Inventory
# MAGIC GROUP BY archetype
# MAGIC ORDER BY qty_available DESC NULLS LAST;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 7: Tracking consistency ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     tracking_policy,
# MAGIC     tracking_match,
# MAGIC     COUNT(*) AS rows_count,
# MAGIC     ROUND(SUM(qty_on_hand), 2) AS qty_on_hand
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Inventory
# MAGIC GROUP BY tracking_policy, tracking_match
# MAGIC ORDER BY tracking_policy, rows_count DESC;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 8: Aging buckets ==============
# MAGIC 
# MAGIC WITH bucketed AS (
# MAGIC     SELECT 
# MAGIC         CASE
# MAGIC             WHEN days_in_stock <= 30  THEN '0-30 days'
# MAGIC             WHEN days_in_stock <= 90  THEN '31-90 days'
# MAGIC             WHEN days_in_stock <= 180 THEN '91-180 days'
# MAGIC             WHEN days_in_stock <= 365 THEN '181-365 days'
# MAGIC             ELSE '>365 days'
# MAGIC         END AS age_bucket,
# MAGIC         qty_available,
# MAGIC         qty_on_hand,
# MAGIC         estimated_total_cost
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Inventory
# MAGIC )
# MAGIC SELECT 
# MAGIC     age_bucket,
# MAGIC     COUNT(*) AS rows_count,
# MAGIC     ROUND(SUM(qty_available), 2) AS qty_available,
# MAGIC     ROUND(SUM(qty_on_hand), 2) AS qty_on_hand,
# MAGIC     ROUND(SUM(estimated_total_cost), 2) AS estimated_total_cost
# MAGIC FROM bucketed
# MAGIC GROUP BY age_bucket
# MAGIC ORDER BY 
# MAGIC     CASE age_bucket 
# MAGIC         WHEN '0-30 days'    THEN 1
# MAGIC         WHEN '31-90 days'   THEN 2
# MAGIC         WHEN '91-180 days'  THEN 3
# MAGIC         WHEN '181-365 days' THEN 4
# MAGIC         ELSE 5
# MAGIC     END;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 9: Top 20 items by available qty ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     item_no,
# MAGIC     item_description,
# MAGIC     item_category,
# MAGIC     archetype,
# MAGIC     location_code,
# MAGIC     lot_no,
# MAGIC     qty_available,
# MAGIC     days_in_stock,
# MAGIC     estimated_total_cost,
# MAGIC     earliest_expiration_date
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Inventory
# MAGIC WHERE is_blocked_location = FALSE
# MAGIC ORDER BY qty_available DESC
# MAGIC LIMIT 20;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 10: Spot check inventory totals ==============
# MAGIC -- คุณสามารถ copy item_no จาก output ของ cell ก่อนหน้าไป cross-check 
# MAGIC -- กับ Item Card balance ใน BC ดูว่าเลขตรงกันไหม
# MAGIC 
# MAGIC SELECT 
# MAGIC     item_no,
# MAGIC     item_description,
# MAGIC     item_category,
# MAGIC     SUM(CASE WHEN is_blocked_location = FALSE THEN qty_on_hand ELSE 0 END) AS qty_unblocked,
# MAGIC     SUM(CASE WHEN is_blocked_location = TRUE  THEN qty_on_hand ELSE 0 END) AS qty_blocked,
# MAGIC     SUM(qty_on_hand) AS total_qty,
# MAGIC     COUNT(DISTINCT location_code) AS location_count,
# MAGIC     COUNT(DISTINCT lot_no) AS lot_count
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Inventory
# MAGIC GROUP BY item_no, item_description, item_category
# MAGIC ORDER BY total_qty DESC
# MAGIC LIMIT 5;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # nb_gold_planned_orders_mps  (Phase 3 — Archetype 1 of 5)

# CELL ********************

# MAGIC %%sql
# MAGIC -- =====================================================================
# MAGIC -- Notebook: nb_gold_planned_orders_mps  (PATCHED v4 — Bug 1 + Bug 2 + Bug 3)
# MAGIC -- Layer:    Gold MRP Engine (Phase 3 — Archetype 1 of 5)
# MAGIC -- Output:   Gold_Inventory_Lakehouse.mrp.gold_bom_exploded
# MAGIC --           Gold_Inventory_Lakehouse.mrp.gold_planned_orders_mps
# MAGIC --           Gold_Inventory_Lakehouse.mrp.gold_dependent_demand_from_mps
# MAGIC --
# MAGIC -- 🐛 BUG 3 FIX (NEW v4, deployed 2026-05-06): Finished PRO supply netting
# MAGIC --
# MAGIC --   v3 ใช้ qty_outstanding ของ SO โดยหักเฉพาะ Active PRO (Released/Firm Planned/Planned)
# MAGIC --   ไม่ได้หัก Finished PROs (ผลิตเสร็จแล้ว แต่ Sales ยังไม่ posted shipment)
# MAGIC --
# MAGIC --   Symptom: SP000003157 (ENNOVIE internal sample order) มี:
# MAGIC --     Lines 10-30 → WSP250900210/211/212 (Finished) ผลิตเสร็จแล้ว
# MAGIC --     Line 40    → WSP250900213 (Released) ยังกำลังผลิต
# MAGIC --     แต่ MPS engine ยัง plan FG ใหม่ทุก line ทั้งที่ 3 lines เสร็จแล้ว
# MAGIC --     → BOM explode → phantom component demand (เช่น FCB-000591-BR 7.6 CM ใน W03)
# MAGIC --
# MAGIC --   Impact (verify_so_pro_coverage.sql 2026-05-06):
# MAGIC --     - 389 SO lines (9.3% of 4,137 open MPS lines) มี COVERED_BY_FINISHED
# MAGIC --     - 268 lines เป็น INTERNAL_SAMPLE (SP-prefix)
# MAGIC --     - 121 lines เป็น B2B_ORDER (RO-prefix)
# MAGIC --     - Total qty 2,491 พุ่งเข้า phantom plans
# MAGIC --
# MAGIC --   v4 fix:
# MAGIC --     1. NEW CTE: finished_pro_net_supply
# MAGIC --        - Mirror ของ active_pro_net_supply but Status = 'Finished'
# MAGIC --        - Aggregate (item_no, so_no, so_line_no) → SUM(Quantity)
# MAGIC --     2. MODIFIED CTE so_demand:
# MAGIC --        - Subtract finished_pro_qty เพิ่มจาก active_pro_qty
# MAGIC --        - Add audit column finished_pro_supply_qty
# MAGIC --     3. SELECT: add audit columns finished_pro_supply_qty
# MAGIC --
# MAGIC --   Cross-bug interaction (Bug 1 + Bug 2 + Bug 3):
# MAGIC --     - Bug 1: subtract Active PRO supply (Quantity) from SO demand
# MAGIC --     - Bug 2: subtract Active PRO consumption (Remaining Qty) from inventory
# MAGIC --     - Bug 3: subtract Finished PRO supply (Quantity) from SO demand  ← NEW
# MAGIC --     - These operate on independent data flows — no double-counting risk
# MAGIC --
# MAGIC -- 🐛 BUG 1 FIX (deployed in v2, retained in v3):
# MAGIC --   Engine ใช้ qty_outstanding ตรงๆ จาก gold_SO โดยไม่หัก Quantity 
# MAGIC --   ของ active PROs ที่ planner firm ไปแล้ว → over-count demand
# MAGIC --   v2 fix: active_pro_net_supply CTE — subtract by (item, so, so_line)
# MAGIC --
# MAGIC -- 🐛 BUG 2 FIX (NEW in v3): Engine inventory not netted
# MAGIC --
# MAGIC --   Engine ใช้ qty_available ตรงๆ จาก gold_Inventory โดยไม่หัก
# MAGIC --   Remaining Quantity ของ Active PRO components → engine คิดว่า stock พอ
# MAGIC --   ทั้งที่จริงถูกจองหมดแล้วโดย active PROs
# MAGIC --
# MAGIC --   Systemic impact (after SKIP exclusion):
# MAGIC --     - MPS: 1,167 items affected, 307,364 qty hidden shortage (87.7% of total)
# MAGIC --
# MAGIC --   v3 fix:
# MAGIC --     1. NEW CTE active_pro_consumption — รวม Remaining Quantity 
# MAGIC --        ของ Active PRO components ระดับ item_no (CDC dedup pattern)
# MAGIC --        EXCLUDE archetype=SKIP (phantom items)
# MAGIC --     2. MODIFIED CTE available_inventory → atp_inventory
# MAGIC --        - Subtract pro_committed_qty ออกจาก qty_available
# MAGIC --        - GREATEST(...0) clamp non-negative
# MAGIC --        - เพิ่ม audit columns: onhand_gross, pro_committed
# MAGIC --     3. SELECT: replace inv reference + add audit columns to output
# MAGIC --
# MAGIC --   Cross-bug interaction (Bug 1 + Bug 2):
# MAGIC --     - Bug 1: subtract active PRO output (Quantity) from SO demand
# MAGIC --     - Bug 2: subtract active PRO consumption (Remaining Qty) from inventory
# MAGIC --     - These operate on DIFFERENT data flows — no double-counting risk
# MAGIC -- =====================================================================
# MAGIC -- ⚠️ v4 changes vs v3: ONLY CELL 6 (so_demand) + new finished_pro_net_supply CTE
# MAGIC --   Cells 1-5, 7-12 unchanged from v3
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 1: Setup (UNCHANGED) ==============
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS Gold_Inventory_Lakehouse.mrp;
# MAGIC 
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInWrite = CORRECTED;
# MAGIC SET spark.sql.parquet.int96RebaseModeInRead = CORRECTED;
# MAGIC SET spark.sql.parquet.int96RebaseModeInWrite = CORRECTED;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 2: Build gold_bom_exploded (UNCHANGED from v2) ==============
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.gold_bom_exploded
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH 
# MAGIC active_versions AS (
# MAGIC     SELECT 
# MAGIC         `Production BOM No.` AS bom_no,
# MAGIC         `Version Code`       AS active_version_code,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY `Production BOM No.` 
# MAGIC             ORDER BY `Starting Date` DESC NULLS LAST, `Version Code` DESC
# MAGIC         ) AS rn
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Production BOM Version`
# MAGIC     WHERE `BC Company` = 'Ennovie'
# MAGIC       AND Status = 'Certified'
# MAGIC       AND (`Starting Date` IS NULL OR `Starting Date` <= CURRENT_DATE())
# MAGIC ),
# MAGIC 
# MAGIC bom_with_version AS (
# MAGIC     SELECT 
# MAGIC         h.`No.`                                  AS bom_no,
# MAGIC         COALESCE(av.active_version_code, '')     AS effective_version_code
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Production BOM Header` h
# MAGIC     LEFT JOIN active_versions av 
# MAGIC            ON h.`No.` = av.bom_no AND av.rn = 1
# MAGIC     WHERE h.`BC Company` = 'Ennovie'
# MAGIC       AND h.Status = 'Certified'
# MAGIC ),
# MAGIC 
# MAGIC bom_effective_lines AS (
# MAGIC     SELECT 
# MAGIC         bl.`Production BOM No.`        AS bom_no,
# MAGIC         bl.`No.`                       AS component_item,
# MAGIC         bl.Description                 AS component_description,
# MAGIC         bl.`Quantity per`              AS qty_per_parent,
# MAGIC         bl.`Unit of Measure Code`      AS uom,
# MAGIC         COALESCE(bl.`Scrap %`, 0)      AS line_scrap_pct
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Production BOM Line` bl
# MAGIC     INNER JOIN bom_with_version bwv 
# MAGIC             ON bl.`Production BOM No.` = bwv.bom_no
# MAGIC            AND COALESCE(bl.`Version Code`, '') = bwv.effective_version_code
# MAGIC     WHERE bl.`BC Company` = 'Ennovie'
# MAGIC       AND bl.Type = 'Item'
# MAGIC       AND COALESCE(bl.`No.`, '') <> ''
# MAGIC       AND COALESCE(bl.`Quantity per`, 0) > 0
# MAGIC       AND (bl.`Starting Date` IS NULL 
# MAGIC            OR bl.`Starting Date` = DATE'0001-01-01'
# MAGIC            OR bl.`Starting Date` <= CURRENT_DATE())
# MAGIC       AND (bl.`Ending Date` IS NULL 
# MAGIC            OR bl.`Ending Date` = DATE'0001-01-01'
# MAGIC            OR bl.`Ending Date` >= CURRENT_DATE())
# MAGIC ),
# MAGIC 
# MAGIC items_with_bom AS (
# MAGIC     SELECT 
# MAGIC         i.`No.`                AS item_no,
# MAGIC         i.Description          AS item_description,
# MAGIC         i.`Production BOM No.` AS production_bom_no
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item` i
# MAGIC     WHERE i.`BC Company` = 'Ennovie'
# MAGIC       AND i.Blocked = false
# MAGIC       AND i.`Production BOM No.` IS NOT NULL
# MAGIC       AND i.`Production BOM No.` <> ''
# MAGIC ),
# MAGIC 
# MAGIC level1 AS (
# MAGIC     SELECT 
# MAGIC         iwb.item_no                          AS root_item,
# MAGIC         iwb.item_description                 AS root_description,
# MAGIC         iwb.item_no                          AS parent_item,
# MAGIC         bel.component_item                   AS component_item,
# MAGIC         bel.component_description            AS component_description,
# MAGIC         1                                    AS bom_level,
# MAGIC         bel.qty_per_parent                   AS qty_per_parent,
# MAGIC         bel.qty_per_parent                   AS qty_per_root,
# MAGIC         bel.line_scrap_pct                   AS line_scrap_pct,
# MAGIC         bel.uom                              AS uom,
# MAGIC         CONCAT(iwb.item_no, ' > ', bel.component_item) AS bom_path
# MAGIC     FROM items_with_bom iwb
# MAGIC     INNER JOIN bom_effective_lines bel 
# MAGIC             ON iwb.production_bom_no = bel.bom_no
# MAGIC ),
# MAGIC 
# MAGIC level2 AS (
# MAGIC     SELECT 
# MAGIC         L1.root_item,
# MAGIC         L1.root_description,
# MAGIC         L1.component_item                       AS parent_item,
# MAGIC         bel.component_item                      AS component_item,
# MAGIC         bel.component_description               AS component_description,
# MAGIC         2                                       AS bom_level,
# MAGIC         bel.qty_per_parent                      AS qty_per_parent,
# MAGIC         L1.qty_per_root * bel.qty_per_parent    AS qty_per_root,
# MAGIC         bel.line_scrap_pct                      AS line_scrap_pct,
# MAGIC         bel.uom                                 AS uom,
# MAGIC         CONCAT(L1.bom_path, ' > ', bel.component_item) AS bom_path
# MAGIC     FROM level1 L1
# MAGIC     INNER JOIN items_with_bom iwb 
# MAGIC             ON L1.component_item = iwb.item_no
# MAGIC     INNER JOIN bom_effective_lines bel 
# MAGIC             ON iwb.production_bom_no = bel.bom_no
# MAGIC ),
# MAGIC 
# MAGIC level3 AS (
# MAGIC     SELECT 
# MAGIC         L2.root_item,
# MAGIC         L2.root_description,
# MAGIC         L2.component_item                       AS parent_item,
# MAGIC         bel.component_item                      AS component_item,
# MAGIC         bel.component_description               AS component_description,
# MAGIC         3                                       AS bom_level,
# MAGIC         bel.qty_per_parent                      AS qty_per_parent,
# MAGIC         L2.qty_per_root * bel.qty_per_parent    AS qty_per_root,
# MAGIC         bel.line_scrap_pct                      AS line_scrap_pct,
# MAGIC         bel.uom                                 AS uom,
# MAGIC         CONCAT(L2.bom_path, ' > ', bel.component_item) AS bom_path
# MAGIC     FROM level2 L2
# MAGIC     INNER JOIN items_with_bom iwb 
# MAGIC             ON L2.component_item = iwb.item_no
# MAGIC     INNER JOIN bom_effective_lines bel 
# MAGIC             ON iwb.production_bom_no = bel.bom_no
# MAGIC )
# MAGIC 
# MAGIC SELECT 
# MAGIC     root_item, root_description,
# MAGIC     parent_item, component_item, component_description,
# MAGIC     bom_level, qty_per_parent, qty_per_root, line_scrap_pct, uom, bom_path,
# MAGIC     CURRENT_TIMESTAMP() AS exploded_at
# MAGIC FROM level1
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT 
# MAGIC     root_item, root_description,
# MAGIC     parent_item, component_item, component_description,
# MAGIC     bom_level, qty_per_parent, qty_per_root, line_scrap_pct, uom, bom_path,
# MAGIC     CURRENT_TIMESTAMP() AS exploded_at
# MAGIC FROM level2
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT 
# MAGIC     root_item, root_description,
# MAGIC     parent_item, component_item, component_description,
# MAGIC     bom_level, qty_per_parent, qty_per_root, line_scrap_pct, uom, bom_path,
# MAGIC     CURRENT_TIMESTAMP() AS exploded_at
# MAGIC FROM level3;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 3: Validation — gold_bom_exploded (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     'Total BOM rows exploded'   AS metric,
# MAGIC     COUNT(*)                    AS value
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_bom_exploded
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Unique root items',
# MAGIC     COUNT(DISTINCT root_item)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_bom_exploded
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Max BOM depth (level)',
# MAGIC     MAX(bom_level)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_bom_exploded
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Average components per root',
# MAGIC     CAST(COUNT(*) / NULLIF(COUNT(DISTINCT root_item), 0) AS BIGINT)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_bom_exploded;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 4: BOM depth distribution (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     bom_level,
# MAGIC     COUNT(*) AS rows_count,
# MAGIC     COUNT(DISTINCT root_item) AS root_items,
# MAGIC     COUNT(DISTINCT component_item) AS component_items
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_bom_exploded
# MAGIC GROUP BY bom_level
# MAGIC ORDER BY bom_level;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 5: Sample BOM explosion for 1 deep FG (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     bom_level,
# MAGIC     parent_item,
# MAGIC     component_item,
# MAGIC     component_description,
# MAGIC     qty_per_parent,
# MAGIC     qty_per_root,
# MAGIC     line_scrap_pct,
# MAGIC     bom_path
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_bom_exploded
# MAGIC WHERE root_item = (
# MAGIC     SELECT root_item
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_bom_exploded
# MAGIC     GROUP BY root_item
# MAGIC     ORDER BY MAX(bom_level) DESC, COUNT(*) DESC
# MAGIC     LIMIT 1
# MAGIC )
# MAGIC ORDER BY bom_level, parent_item, component_item
# MAGIC LIMIT 50;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 6 (PATCHED v3): Build gold_planned_orders_mps ==============
# MAGIC --
# MAGIC -- Changes from v2:
# MAGIC --   1. NEW CTE: active_pro_consumption (Bug 2 fix)
# MAGIC --      → aggregate Remaining Quantity ของ active PRO components ระดับ item_no
# MAGIC --      → CDC dedup: latest SystemRowVersion per (PRO No., PRO Line No., Line No.)
# MAGIC --      → EXCLUDE archetype=SKIP via INNER JOIN to item_master
# MAGIC --   2. MODIFIED CTE: available_inventory → atp_inventory
# MAGIC --      → LEFT JOIN active_pro_consumption
# MAGIC --      → qty_available_atp = GREATEST(qty_available_gross - pro_committed, 0)
# MAGIC --      → expose audit columns
# MAGIC --   3. SELECT: 
# MAGIC --      → replace inv references with atp
# MAGIC --      → add audit columns: onhand_gross_qty, active_pro_committed_qty
# MAGIC -- 
# MAGIC -- Existing v2 logic (Bug 1 fix) is RETAINED:
# MAGIC --   - active_pro_net_supply CTE (subtracts PRO output from SO demand)
# MAGIC --   - so_demand uses net qty_outstanding
# MAGIC --   - audit columns so_outstanding_qty_gross + active_pro_supply_qty
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.gold_planned_orders_mps
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH 
# MAGIC -- ⭐ v2 (KEPT): Active PRO net supply for Bug 1 (SO demand netting)
# MAGIC active_pro_net_supply AS (
# MAGIC     SELECT 
# MAGIC         h.`Source No.`             AS item_no,
# MAGIC         h.`Sales Order No.`        AS so_no,
# MAGIC         h.`Sales Order Line No.`   AS so_line_no,
# MAGIC         SUM(h.Quantity)            AS active_pro_qty
# MAGIC     FROM (
# MAGIC         SELECT 
# MAGIC             *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `No.` 
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Production Order`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND Status IN ('Released', 'Firm Planned', 'Planned')
# MAGIC     ) h
# MAGIC     WHERE h.rn = 1
# MAGIC       AND h.`Sales Order No.` IS NOT NULL
# MAGIC       AND h.`Sales Order No.` <> ''
# MAGIC       AND h.`Sales Order Line No.` IS NOT NULL
# MAGIC       AND h.`Sales Order Line No.` > 0
# MAGIC       AND h.`Source No.` IS NOT NULL
# MAGIC       AND h.`Source No.` <> ''
# MAGIC     GROUP BY 
# MAGIC         h.`Source No.`,
# MAGIC         h.`Sales Order No.`,
# MAGIC         h.`Sales Order Line No.`
# MAGIC ),
# MAGIC 
# MAGIC -- ⭐ NEW v4: Finished PRO net supply for Bug 3 (SO demand netting against Finished)
# MAGIC -- Mirror pattern ของ active_pro_net_supply แต่ Status = 'Finished'
# MAGIC -- Captures ของที่ผลิตเสร็จแล้ว แต่ Sales ยังไม่ posted shipment
# MAGIC finished_pro_net_supply AS (
# MAGIC     SELECT 
# MAGIC         h.`Source No.`             AS item_no,
# MAGIC         h.`Sales Order No.`        AS so_no,
# MAGIC         h.`Sales Order Line No.`   AS so_line_no,
# MAGIC         SUM(h.Quantity)            AS finished_pro_qty
# MAGIC     FROM (
# MAGIC         SELECT 
# MAGIC             *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `No.` 
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Production Order`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND Status = 'Finished'
# MAGIC     ) h
# MAGIC     WHERE h.rn = 1
# MAGIC       AND h.`Sales Order No.` IS NOT NULL
# MAGIC       AND h.`Sales Order No.` <> ''
# MAGIC       AND h.`Sales Order Line No.` IS NOT NULL
# MAGIC       AND h.`Sales Order Line No.` > 0
# MAGIC       AND h.`Source No.` IS NOT NULL
# MAGIC       AND h.`Source No.` <> ''
# MAGIC     GROUP BY 
# MAGIC         h.`Source No.`,
# MAGIC         h.`Sales Order No.`,
# MAGIC         h.`Sales Order Line No.`
# MAGIC ),
# MAGIC 
# MAGIC -- ⭐ v4 (UPDATED): SO demand with Active + Finished PRO netting (Bug 1 + Bug 3)
# MAGIC so_demand AS (
# MAGIC     SELECT 
# MAGIC         so.so_no, so.so_line_no, so.customer_no, so.customer_name,
# MAGIC         so.customer_tier_code, so.customer_priority_weight,
# MAGIC         so.is_dnd, so.dnd_exceed_date,
# MAGIC         so.ship_date, so.days_until_ship,
# MAGIC         so.requested_ship_date, so.promised_ship_date,
# MAGIC         so.item_no, so.description, so.item_category, so.item_archetype,
# MAGIC         -- ⭐ v4: subtract BOTH Active and Finished PRO supply
# MAGIC         GREATEST(
# MAGIC             so.qty_outstanding 
# MAGIC                 - COALESCE(apns.active_pro_qty, 0)
# MAGIC                 - COALESCE(fpns.finished_pro_qty, 0),
# MAGIC             0
# MAGIC         )                                AS qty_outstanding,
# MAGIC         so.qty_outstanding               AS qty_outstanding_gross,
# MAGIC         COALESCE(apns.active_pro_qty, 0) AS active_pro_supply_qty,
# MAGIC         -- ⭐ v4: NEW audit column for Bug 3
# MAGIC         COALESCE(fpns.finished_pro_qty, 0) AS finished_pro_supply_qty,
# MAGIC         so.uom, so.line_amount_thb,
# MAGIC         so.gold_per_ounce_at_so, so.silver_per_ounce_at_so
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_SO so
# MAGIC     LEFT JOIN active_pro_net_supply apns
# MAGIC         ON  apns.item_no    = so.item_no
# MAGIC        AND apns.so_no      = so.so_no
# MAGIC        AND apns.so_line_no = so.so_line_no
# MAGIC     LEFT JOIN finished_pro_net_supply fpns
# MAGIC         ON  fpns.item_no   = so.item_no
# MAGIC        AND fpns.so_no      = so.so_no
# MAGIC        AND fpns.so_line_no = so.so_line_no
# MAGIC     WHERE so.is_open = TRUE
# MAGIC       AND so.item_archetype = 'MPS'
# MAGIC       AND so.qty_outstanding > 0
# MAGIC       AND GREATEST(
# MAGIC             so.qty_outstanding 
# MAGIC                 - COALESCE(apns.active_pro_qty, 0)
# MAGIC                 - COALESCE(fpns.finished_pro_qty, 0),
# MAGIC             0
# MAGIC           ) > 0
# MAGIC ),
# MAGIC 
# MAGIC -- ⭐ NEW v3: Active PRO Component Consumption (Bug 2 fix)
# MAGIC --   CDC dedup + EXCLUDE archetype=SKIP (phantom items)
# MAGIC active_pro_consumption AS (
# MAGIC     SELECT 
# MAGIC         c.`Item No.`                AS item_no,
# MAGIC         SUM(c.`Remaining Quantity`) AS pro_committed_qty
# MAGIC     FROM (
# MAGIC         SELECT 
# MAGIC             *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `Prod. Order No.`, `Prod. Order Line No.`, `Line No.`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Prod Order Component`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND Status IN ('Released', 'Firm Planned', 'Planned')
# MAGIC           AND `Remaining Quantity` > 0
# MAGIC     ) c
# MAGIC     INNER JOIN Gold_Inventory_Lakehouse.mrp.gold_Item_Master im 
# MAGIC             ON im.item_no = c.`Item No.`
# MAGIC     WHERE c.rn = 1
# MAGIC       AND im.archetype <> 'SKIP'
# MAGIC     GROUP BY c.`Item No.`
# MAGIC ),
# MAGIC 
# MAGIC -- ⭐ MODIFIED v3: ATP inventory (replaces available_inventory from v2)
# MAGIC atp_inventory AS (
# MAGIC     SELECT 
# MAGIC         inv.item_no,
# MAGIC         SUM(inv.qty_available)                          AS qty_available_gross,
# MAGIC         COALESCE(MAX(apc.pro_committed_qty), 0)         AS pro_committed_qty,
# MAGIC         GREATEST(
# MAGIC             SUM(inv.qty_available) - COALESCE(MAX(apc.pro_committed_qty), 0),
# MAGIC             0
# MAGIC         )                                               AS qty_available_atp
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Inventory inv
# MAGIC     LEFT JOIN active_pro_consumption apc ON apc.item_no = inv.item_no
# MAGIC     WHERE inv.is_blocked_location = FALSE
# MAGIC       AND inv.is_expired = FALSE
# MAGIC     GROUP BY inv.item_no
# MAGIC ),
# MAGIC 
# MAGIC item_master_mps AS (
# MAGIC     SELECT 
# MAGIC         item_no, scrap_pct, rop_qty, roq_qty,
# MAGIC         min_order_qty, max_order_qty, order_multiple, safety_stock_qty,
# MAGIC         lot_accum_days, base_lead_days_static, safety_lead_days_static,
# MAGIC         effective_lead_days, item_category, archetype
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Item_Master
# MAGIC     WHERE archetype = 'MPS' AND is_blocked = FALSE
# MAGIC ),
# MAGIC 
# MAGIC bom_lookup AS (
# MAGIC     SELECT 
# MAGIC         `No.` AS item_no,
# MAGIC         `Production BOM No.` AS production_bom_no
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item`
# MAGIC     WHERE `BC Company` = 'Ennovie'
# MAGIC       AND `Production BOM No.` IS NOT NULL
# MAGIC       AND `Production BOM No.` <> ''
# MAGIC ),
# MAGIC 
# MAGIC bom_summary AS (
# MAGIC     SELECT 
# MAGIC         root_item,
# MAGIC         COUNT(DISTINCT component_item) AS bom_components_count,
# MAGIC         MAX(bom_level) AS bom_max_depth
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_bom_exploded
# MAGIC     GROUP BY root_item
# MAGIC )
# MAGIC 
# MAGIC SELECT 
# MAGIC     UUID() AS plan_id,
# MAGIC     
# MAGIC     d.so_no                         AS triggering_so_no,
# MAGIC     d.so_line_no                    AS triggering_so_line,
# MAGIC     d.customer_no, d.customer_name,
# MAGIC     d.customer_tier_code            AS customer_tier,
# MAGIC     d.customer_priority_weight      AS priority_weight,
# MAGIC     d.is_dnd, d.dnd_exceed_date,
# MAGIC     d.ship_date, d.days_until_ship,
# MAGIC     d.requested_ship_date, d.promised_ship_date,
# MAGIC     
# MAGIC     d.item_no, d.description, d.item_category,
# MAGIC     'MPS' AS archetype,
# MAGIC     d.uom,
# MAGIC     
# MAGIC     -- v2: net qty_outstanding (after PRO supply subtraction)
# MAGIC     d.qty_outstanding               AS so_outstanding_qty,
# MAGIC     
# MAGIC     -- v2 audit columns (Bug 1)
# MAGIC     d.qty_outstanding_gross         AS so_outstanding_qty_gross,
# MAGIC     d.active_pro_supply_qty         AS active_pro_supply_qty,
# MAGIC     -- ⭐ v4 audit column (Bug 3)
# MAGIC     d.finished_pro_supply_qty       AS finished_pro_supply_qty,
# MAGIC     
# MAGIC     -- ⭐ v3: ATP-netted inventory (Bug 2)
# MAGIC     COALESCE(atp.qty_available_atp, 0)   AS onhand_available_qty,
# MAGIC     
# MAGIC     -- ⭐ v3: NEW audit columns (Bug 2)
# MAGIC     COALESCE(atp.qty_available_gross, 0) AS onhand_gross_qty,
# MAGIC     COALESCE(atp.pro_committed_qty, 0)   AS active_pro_committed_qty,
# MAGIC     
# MAGIC     -- ⭐ v3: shortage_qty now uses ATP
# MAGIC     GREATEST(d.qty_outstanding - COALESCE(atp.qty_available_atp, 0), 0) AS shortage_qty,
# MAGIC     COALESCE(im.scrap_pct, 0) AS scrap_pct,
# MAGIC     ROUND(
# MAGIC         GREATEST(d.qty_outstanding - COALESCE(atp.qty_available_atp, 0), 0) 
# MAGIC             * (1 + COALESCE(im.scrap_pct, 0) / 100.0),
# MAGIC         4
# MAGIC     ) AS planned_qty,
# MAGIC     
# MAGIC     bl.production_bom_no,
# MAGIC     bs.bom_components_count,
# MAGIC     bs.bom_max_depth,
# MAGIC     
# MAGIC     im.min_order_qty, im.max_order_qty, im.order_multiple, im.lot_accum_days,
# MAGIC     im.base_lead_days_static AS lead_time_days,
# MAGIC     im.safety_lead_days_static AS safety_lead_days,
# MAGIC     im.effective_lead_days,
# MAGIC     
# MAGIC     d.line_amount_thb,
# MAGIC     d.gold_per_ounce_at_so, d.silver_per_ounce_at_so,
# MAGIC     
# MAGIC     -- ⭐ v3: COVERED_BY_STOCK now reflects ATP (not gross on_hand)
# MAGIC     CASE 
# MAGIC         WHEN bl.production_bom_no IS NULL THEN 'BLOCKED_NO_BOM'
# MAGIC         WHEN bs.bom_components_count IS NULL THEN 'BLOCKED_BOM_NOT_CERTIFIED'
# MAGIC         WHEN d.qty_outstanding - COALESCE(atp.qty_available_atp, 0) <= 0 THEN 'COVERED_BY_STOCK'
# MAGIC         WHEN d.is_dnd = TRUE THEN 'PROPOSED_DND'
# MAGIC         ELSE 'PROPOSED'
# MAGIC     END AS plan_status,
# MAGIC     
# MAGIC     CASE 
# MAGIC         WHEN bl.production_bom_no IS NULL THEN 'Item has no Production BOM No. — set in BC'
# MAGIC         WHEN bs.bom_components_count IS NULL THEN 'BOM exists but no Certified version found'
# MAGIC         ELSE NULL
# MAGIC     END AS exception_reason,
# MAGIC     
# MAGIC     CURRENT_TIMESTAMP() AS plan_run_at
# MAGIC 
# MAGIC FROM so_demand d
# MAGIC LEFT JOIN atp_inventory atp     ON d.item_no = atp.item_no
# MAGIC LEFT JOIN item_master_mps im    ON d.item_no = im.item_no
# MAGIC LEFT JOIN bom_lookup bl         ON d.item_no = bl.item_no
# MAGIC LEFT JOIN bom_summary bs        ON d.item_no = bs.root_item;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 7: Build gold_dependent_demand_from_mps (UNCHANGED from v2) ==============
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.gold_dependent_demand_from_mps
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT 
# MAGIC     UUID() AS dependent_demand_id,
# MAGIC     
# MAGIC     po.plan_id           AS source_plan_id,
# MAGIC     po.triggering_so_no  AS source_so_no,
# MAGIC     po.triggering_so_line AS source_so_line,
# MAGIC     po.customer_no       AS source_customer_no,
# MAGIC     po.customer_tier     AS source_customer_tier,
# MAGIC     po.priority_weight   AS source_priority_weight,
# MAGIC     po.is_dnd            AS source_is_dnd,
# MAGIC     po.ship_date         AS parent_ship_date,
# MAGIC     
# MAGIC     po.item_no           AS parent_item,
# MAGIC     po.description       AS parent_description,
# MAGIC     po.planned_qty       AS parent_planned_qty,
# MAGIC     
# MAGIC     bom.bom_level,
# MAGIC     bom.parent_item      AS bom_parent_item,
# MAGIC     bom.component_item,
# MAGIC     bom.component_description,
# MAGIC     bom.bom_path,
# MAGIC     bom.qty_per_root,
# MAGIC     bom.line_scrap_pct,
# MAGIC     bom.uom              AS component_uom,
# MAGIC     
# MAGIC     im.archetype         AS component_archetype,
# MAGIC     im.item_category     AS component_category,
# MAGIC     
# MAGIC     ROUND(
# MAGIC         po.planned_qty * bom.qty_per_root * (1 + bom.line_scrap_pct / 100.0),
# MAGIC         4
# MAGIC     ) AS dependent_demand_qty,
# MAGIC     
# MAGIC     DATE_SUB(po.ship_date, COALESCE(im.effective_lead_days, 0)) AS need_date,
# MAGIC     
# MAGIC     CURRENT_TIMESTAMP() AS created_at
# MAGIC 
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_mps po
# MAGIC INNER JOIN Gold_Inventory_Lakehouse.mrp.gold_bom_exploded bom 
# MAGIC         ON po.item_no = bom.root_item
# MAGIC LEFT JOIN Gold_Inventory_Lakehouse.mrp.gold_Item_Master im 
# MAGIC        ON bom.component_item = im.item_no
# MAGIC WHERE po.plan_status IN ('PROPOSED', 'PROPOSED_DND')
# MAGIC   AND po.planned_qty > 0;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 8: Validation — gold_planned_orders_mps (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     plan_status,
# MAGIC     COUNT(*) AS plan_count,
# MAGIC     COUNT(DISTINCT triggering_so_no) AS unique_so,
# MAGIC     COUNT(DISTINCT item_no) AS unique_items,
# MAGIC     ROUND(SUM(planned_qty), 2) AS total_planned_qty,
# MAGIC     ROUND(SUM(line_amount_thb), 2) AS total_amount_thb
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_mps
# MAGIC GROUP BY plan_status
# MAGIC ORDER BY plan_count DESC;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 9: Top 20 planned orders by priority (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     triggering_so_no, customer_tier, priority_weight, is_dnd,
# MAGIC     item_no, description,
# MAGIC     so_outstanding_qty, onhand_available_qty, shortage_qty,
# MAGIC     scrap_pct, planned_qty,
# MAGIC     bom_components_count, bom_max_depth,
# MAGIC     ship_date, days_until_ship, plan_status
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_mps
# MAGIC WHERE plan_status IN ('PROPOSED', 'PROPOSED_DND')
# MAGIC ORDER BY 
# MAGIC     is_dnd DESC,
# MAGIC     priority_weight DESC, 
# MAGIC     days_until_ship ASC,
# MAGIC     line_amount_thb DESC
# MAGIC LIMIT 20;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 10: Validation — dependent demand by archetype (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     component_archetype,
# MAGIC     component_category,
# MAGIC     COUNT(*) AS demand_lines,
# MAGIC     COUNT(DISTINCT component_item) AS unique_components,
# MAGIC     ROUND(SUM(dependent_demand_qty), 2) AS total_demand_qty
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_dependent_demand_from_mps
# MAGIC GROUP BY component_archetype, component_category
# MAGIC ORDER BY demand_lines DESC;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 11: Sample dependent demand for top SO (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     bom_level,
# MAGIC     bom_parent_item,
# MAGIC     component_item,
# MAGIC     component_description,
# MAGIC     component_archetype,
# MAGIC     qty_per_root,
# MAGIC     line_scrap_pct,
# MAGIC     parent_planned_qty,
# MAGIC     dependent_demand_qty,
# MAGIC     need_date
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_dependent_demand_from_mps
# MAGIC WHERE source_so_no = (
# MAGIC     SELECT triggering_so_no
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_mps
# MAGIC     WHERE plan_status = 'PROPOSED'
# MAGIC     ORDER BY planned_qty DESC
# MAGIC     LIMIT 1
# MAGIC )
# MAGIC ORDER BY bom_level, bom_parent_item, component_item
# MAGIC LIMIT 50;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 12: Exception report — items missing BOM (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     item_no, description, customer_no, customer_tier,
# MAGIC     so_outstanding_qty, ship_date,
# MAGIC     plan_status, exception_reason
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_mps
# MAGIC WHERE plan_status LIKE 'BLOCKED%'
# MAGIC ORDER BY priority_weight DESC, ship_date ASC
# MAGIC LIMIT 30;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # nb_gold_planned_orders_alloy  (Phase 3 — Archetype 2 of 5)

# CELL ********************

# MAGIC %%sql
# MAGIC -- =====================================================================
# MAGIC -- Notebook: nb_gold_planned_orders_alloy  (Phase 3 — Archetype 2 of 5)
# MAGIC -- Layer:    Gold MRP Engine
# MAGIC -- Output:   Gold_Inventory_Lakehouse.mrp.gold_planned_orders_alloy
# MAGIC -- 
# MAGIC -- ALLOY Archetype = Casting Production Orders + Raw Alloy Bar Purchases
# MAGIC -- 
# MAGIC -- Demand sources:
# MAGIC --   1. Direct SO demand for items with archetype='ALLOY' (rare)
# MAGIC --   2. Dependent demand from MPS BOM where component_archetype='ALLOY'
# MAGIC -- 
# MAGIC -- Key features:
# MAGIC --   - Batch grouping by Metal Category Code (14KR, 14KY, 18KR, Silver 925, etc.)
# MAGIC --     → Multiple FG demands of same alloy = 1 Casting PRO
# MAGIC --   - Apply scrap %  
# MAGIC --   - Distinguish: CASTING items (produce) vs ALLOY items (purchase bullion)
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 1: Setup ==============
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS Gold_Inventory_Lakehouse.mrp;
# MAGIC 
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInWrite = CORRECTED;
# MAGIC SET spark.sql.parquet.int96RebaseModeInRead = CORRECTED;
# MAGIC SET spark.sql.parquet.int96RebaseModeInWrite = CORRECTED;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 2: Build gold_planned_orders_alloy ==============
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.gold_planned_orders_alloy
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH 
# MAGIC -- === Step 1: Direct SO demand (rare for ALLOY) ===
# MAGIC direct_demand AS (
# MAGIC     SELECT 
# MAGIC         so.so_no                            AS source_so_no,
# MAGIC         so.so_line_no                       AS source_so_line,
# MAGIC         so.customer_no                      AS source_customer_no,
# MAGIC         so.customer_tier_code               AS source_customer_tier,
# MAGIC         so.customer_priority_weight         AS source_priority_weight,
# MAGIC         so.is_dnd                           AS source_is_dnd,
# MAGIC         so.ship_date                        AS source_ship_date,
# MAGIC         so.item_no,
# MAGIC         so.description                      AS item_description,
# MAGIC         so.item_category,
# MAGIC         so.qty_outstanding                  AS demand_qty,
# MAGIC         'SO_DIRECT'                         AS source_demand_type
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_SO so
# MAGIC     WHERE so.is_open = TRUE
# MAGIC       AND so.item_archetype = 'ALLOY'
# MAGIC       AND so.qty_outstanding > 0
# MAGIC ),
# MAGIC 
# MAGIC -- === Step 2: Dependent demand from MPS BOM ===
# MAGIC dependent_demand AS (
# MAGIC     SELECT 
# MAGIC         d.source_so_no,
# MAGIC         d.source_so_line,
# MAGIC         d.source_customer_no,
# MAGIC         d.source_customer_tier,
# MAGIC         d.source_priority_weight,
# MAGIC         d.source_is_dnd,
# MAGIC         d.parent_ship_date                  AS source_ship_date,
# MAGIC         d.component_item                    AS item_no,
# MAGIC         d.component_description             AS item_description,
# MAGIC         d.component_category                AS item_category,
# MAGIC         d.dependent_demand_qty              AS demand_qty,
# MAGIC         'COMPONENT_FROM_MPS'                AS source_demand_type
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_dependent_demand_from_mps d
# MAGIC     WHERE d.component_archetype = 'ALLOY'
# MAGIC       AND d.dependent_demand_qty > 0
# MAGIC ),
# MAGIC 
# MAGIC -- === Step 3: Combine all demand ===
# MAGIC all_demand AS (
# MAGIC     SELECT * FROM direct_demand
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM dependent_demand
# MAGIC ),
# MAGIC 
# MAGIC -- === Step 4: Aggregate by item × ship_date (grouping for batching) ===
# MAGIC -- Note: Cross-SO conflict resolution is naive — Phase 4 will refine
# MAGIC aggregated_demand AS (
# MAGIC     SELECT 
# MAGIC         item_no,
# MAGIC         MAX(item_description)               AS item_description,
# MAGIC         MAX(item_category)                  AS item_category,
# MAGIC         source_ship_date,
# MAGIC         SUM(demand_qty)                     AS total_demand_qty,
# MAGIC         COUNT(DISTINCT source_so_no)        AS contributing_so_count,
# MAGIC         MAX(source_priority_weight)         AS max_priority_weight,
# MAGIC         BOOL_OR(source_is_dnd)              AS has_dnd_demand
# MAGIC     FROM all_demand
# MAGIC     GROUP BY item_no, source_ship_date
# MAGIC ),
# MAGIC 
# MAGIC -- === Step 4b: Top contributing SO per (item × ship_date) — for traceability ===
# MAGIC top_so_per_demand AS (
# MAGIC     SELECT 
# MAGIC         item_no,
# MAGIC         source_ship_date,
# MAGIC         source_so_no,
# MAGIC         source_customer_tier,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY item_no, source_ship_date 
# MAGIC             ORDER BY source_priority_weight DESC, source_ship_date ASC
# MAGIC         ) AS rn
# MAGIC     FROM all_demand
# MAGIC ),
# MAGIC 
# MAGIC -- === Step 5: Inventory available per item ===
# MAGIC available_inventory AS (
# MAGIC     SELECT 
# MAGIC         item_no,
# MAGIC         SUM(qty_available) AS qty_available_total
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Inventory
# MAGIC     WHERE is_blocked_location = FALSE
# MAGIC       AND is_expired = FALSE
# MAGIC     GROUP BY item_no
# MAGIC ),
# MAGIC 
# MAGIC -- === Step 6: Item master enrichment ===
# MAGIC item_master_alloy AS (
# MAGIC     SELECT 
# MAGIC         item_no, scrap_pct, rop_qty, roq_qty,
# MAGIC         min_order_qty, max_order_qty, order_multiple,
# MAGIC         safety_stock_qty, lot_accum_days,
# MAGIC         base_lead_days_static, safety_lead_days_static, effective_lead_days,
# MAGIC         item_category, archetype,
# MAGIC         metal_category, alloy, alloy_type, material_type, vendor_no
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Item_Master
# MAGIC     WHERE archetype = 'ALLOY' AND is_blocked = FALSE
# MAGIC ),
# MAGIC 
# MAGIC -- === Step 7: BOM info (for casting items that produce) ===
# MAGIC bom_lookup AS (
# MAGIC     SELECT 
# MAGIC         `No.` AS item_no,
# MAGIC         `Production BOM No.` AS production_bom_no
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item`
# MAGIC     WHERE `BC Company` = 'Ennovie'
# MAGIC       AND `Production BOM No.` IS NOT NULL
# MAGIC       AND `Production BOM No.` <> ''
# MAGIC )
# MAGIC 
# MAGIC -- === Final: Generate planned ALLOY orders ===
# MAGIC SELECT 
# MAGIC     UUID() AS plan_id,
# MAGIC     
# MAGIC     -- Source traceability
# MAGIC     tsd.source_so_no                    AS triggering_so_no,
# MAGIC     tsd.source_customer_tier            AS customer_tier,
# MAGIC     ad.max_priority_weight              AS priority_weight,
# MAGIC     ad.has_dnd_demand                   AS is_dnd,
# MAGIC     ad.contributing_so_count            AS contributing_so_count,
# MAGIC     
# MAGIC     -- Item info
# MAGIC     ad.item_no,
# MAGIC     ad.item_description,
# MAGIC     ad.item_category,
# MAGIC     'ALLOY'                             AS archetype,
# MAGIC     
# MAGIC     -- Alloy-specific metadata (for batch grouping in execution)
# MAGIC     im.metal_category,
# MAGIC     im.alloy,
# MAGIC     im.alloy_type,
# MAGIC     im.material_type,
# MAGIC     
# MAGIC     -- Sub-archetype: CASTING_PRO (produce) vs ALLOY_PURCHASE (buy bullion)
# MAGIC     CASE 
# MAGIC         WHEN ad.item_category = 'CASTING' THEN 'CASTING_PRO'
# MAGIC         WHEN ad.item_category = 'ALLOY' THEN 'ALLOY_PURCHASE'
# MAGIC         ELSE 'CASTING_PRO'  -- default
# MAGIC     END AS sub_archetype,
# MAGIC     
# MAGIC     -- Sourcing (for ALLOY_PURCHASE)
# MAGIC     im.vendor_no,
# MAGIC     
# MAGIC     -- Quantities
# MAGIC     ad.total_demand_qty                 AS demand_qty,
# MAGIC     COALESCE(inv.qty_available_total, 0) AS onhand_available_qty,
# MAGIC     GREATEST(ad.total_demand_qty - COALESCE(inv.qty_available_total, 0), 0) AS shortage_qty,
# MAGIC     COALESCE(im.scrap_pct, 0)           AS scrap_pct,
# MAGIC     ROUND(
# MAGIC         GREATEST(ad.total_demand_qty - COALESCE(inv.qty_available_total, 0), 0) 
# MAGIC             * (1 + COALESCE(im.scrap_pct, 0) / 100.0),
# MAGIC         4
# MAGIC     ) AS planned_qty,
# MAGIC     
# MAGIC     -- Lot sizing
# MAGIC     im.min_order_qty,
# MAGIC     im.max_order_qty,
# MAGIC     im.order_multiple,
# MAGIC     
# MAGIC     -- Lead time / scheduling
# MAGIC     im.base_lead_days_static            AS lead_time_days,
# MAGIC     im.safety_lead_days_static          AS safety_lead_days,
# MAGIC     im.effective_lead_days,
# MAGIC     ad.source_ship_date                 AS need_by_date,
# MAGIC     DATE_SUB(ad.source_ship_date, COALESCE(im.effective_lead_days, 0)) AS suggested_start_date,
# MAGIC     
# MAGIC     -- BOM (for CASTING_PRO)
# MAGIC     bl.production_bom_no,
# MAGIC     
# MAGIC     -- Plan status
# MAGIC     CASE 
# MAGIC         WHEN ad.item_category = 'ALLOY' AND im.vendor_no IS NULL 
# MAGIC             THEN 'BLOCKED_NO_VENDOR'
# MAGIC         WHEN ad.item_category = 'CASTING' AND bl.production_bom_no IS NULL 
# MAGIC             THEN 'BLOCKED_NO_BOM'
# MAGIC         WHEN ad.total_demand_qty - COALESCE(inv.qty_available_total, 0) <= 0 
# MAGIC             THEN 'COVERED_BY_STOCK'
# MAGIC         WHEN ad.has_dnd_demand = TRUE 
# MAGIC             THEN 'PROPOSED_DND'
# MAGIC         ELSE 'PROPOSED'
# MAGIC     END AS plan_status,
# MAGIC     
# MAGIC     CASE 
# MAGIC         WHEN ad.item_category = 'ALLOY' AND im.vendor_no IS NULL 
# MAGIC             THEN 'ALLOY purchase item has no vendor — set in BC'
# MAGIC         WHEN ad.item_category = 'CASTING' AND bl.production_bom_no IS NULL 
# MAGIC             THEN 'Casting item has no Production BOM — set in BC'
# MAGIC         ELSE NULL
# MAGIC     END AS exception_reason,
# MAGIC     
# MAGIC     CURRENT_TIMESTAMP() AS plan_run_at
# MAGIC 
# MAGIC FROM aggregated_demand ad
# MAGIC LEFT JOIN top_so_per_demand tsd 
# MAGIC        ON ad.item_no = tsd.item_no 
# MAGIC       AND ad.source_ship_date = tsd.source_ship_date 
# MAGIC       AND tsd.rn = 1
# MAGIC LEFT JOIN available_inventory inv ON ad.item_no = inv.item_no
# MAGIC LEFT JOIN item_master_alloy im     ON ad.item_no = im.item_no
# MAGIC LEFT JOIN bom_lookup bl            ON ad.item_no = bl.item_no;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 3: Validation — plan status summary ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     sub_archetype,
# MAGIC     plan_status,
# MAGIC     COUNT(*) AS plan_count,
# MAGIC     COUNT(DISTINCT item_no) AS unique_items,
# MAGIC     ROUND(SUM(planned_qty), 2) AS total_planned_qty
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_alloy
# MAGIC GROUP BY sub_archetype, plan_status
# MAGIC ORDER BY sub_archetype, plan_count DESC;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 4: Casting demand by metal category ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     metal_category,
# MAGIC     plan_status,
# MAGIC     COUNT(*) AS plans,
# MAGIC     COUNT(DISTINCT item_no) AS unique_castings,
# MAGIC     ROUND(SUM(planned_qty), 2) AS total_planned_qty,
# MAGIC     SUM(contributing_so_count) AS total_contributing_so
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_alloy
# MAGIC WHERE sub_archetype = 'CASTING_PRO'
# MAGIC   AND plan_status IN ('PROPOSED', 'PROPOSED_DND')
# MAGIC GROUP BY metal_category, plan_status
# MAGIC ORDER BY metal_category, plan_status;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 5: Top 20 alloy plans by priority ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     item_no,
# MAGIC     item_description,
# MAGIC     sub_archetype,
# MAGIC     metal_category,
# MAGIC     customer_tier,
# MAGIC     priority_weight,
# MAGIC     is_dnd,
# MAGIC     contributing_so_count,
# MAGIC     demand_qty,
# MAGIC     onhand_available_qty,
# MAGIC     shortage_qty,
# MAGIC     planned_qty,
# MAGIC     need_by_date,
# MAGIC     suggested_start_date,
# MAGIC     plan_status
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_alloy
# MAGIC WHERE plan_status IN ('PROPOSED', 'PROPOSED_DND')
# MAGIC ORDER BY 
# MAGIC     is_dnd DESC,
# MAGIC     priority_weight DESC,
# MAGIC     need_by_date ASC,
# MAGIC     planned_qty DESC
# MAGIC LIMIT 20;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 6: Exception report ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     sub_archetype,
# MAGIC     item_no,
# MAGIC     item_description,
# MAGIC     plan_status,
# MAGIC     exception_reason,
# MAGIC     demand_qty,
# MAGIC     contributing_so_count
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_alloy
# MAGIC WHERE plan_status LIKE 'BLOCKED%'
# MAGIC ORDER BY priority_weight DESC NULLS LAST, demand_qty DESC
# MAGIC LIMIT 30;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # nb_gold_planned_orders_l4l  (Phase 3 — Archetype 3 of 5)

# CELL ********************

# MAGIC %%sql
# MAGIC -- =====================================================================
# MAGIC -- Notebook: nb_gold_planned_orders_l4l  (PATCHED v2 — Bug 2 ATP Netting)
# MAGIC -- Layer:    Gold MRP Engine
# MAGIC -- Output:   Gold_Inventory_Lakehouse.mrp.gold_planned_orders_l4l
# MAGIC --
# MAGIC -- 🐛 BUG FIX (2026-04-29) — Bug 2: Engine inventory not netted
# MAGIC --
# MAGIC --   Engine ใช้ qty_available ตรงๆ จาก gold_Inventory โดยไม่หัก
# MAGIC --   Remaining Quantity ของ Active PROs (Released/Firm Planned/Planned)
# MAGIC --   ที่ commit material อยู่แล้ว → engine report COVERED_BY_STOCK
# MAGIC --   ทั้งที่ stock จริงถูกจองหมดแล้ว
# MAGIC --
# MAGIC --   Reference case: DI-RD-000362 (Lab Diamond Round-White GVS2)
# MAGIC --     - Stock available:           219.88 CT
# MAGIC --     - Active PRO commitments:    742.97 CT (BC: 742.569)
# MAGIC --     - Real deficit:              523.09 CT
# MAGIC --     - Engine before fix: COVERED_BY_STOCK (planned_qty=0) ❌
# MAGIC --     - Engine after fix:  PROPOSED with shortage ~523 CT ✅
# MAGIC --
# MAGIC --   Systemic impact (after SKIP exclusion):
# MAGIC --     - L4L: 182 items affected, 41,681 qty hidden shortage (12% of total)
# MAGIC --
# MAGIC --   v2 fix:
# MAGIC --     1. NEW CTE active_pro_consumption — รวม Remaining Quantity 
# MAGIC --        ของ Active PRO components ระดับ item_no (CDC dedup pattern)
# MAGIC --        EXCLUDE archetype=SKIP (phantom items: PLATING SOLUTION + MST)
# MAGIC --     2. MODIFIED CTE available_inventory → atp_inventory
# MAGIC --        - Subtract pro_committed_qty ออกจาก qty_available
# MAGIC --        - GREATEST(...0) clamp non-negative
# MAGIC --        - เพิ่ม audit columns: onhand_gross, pro_committed
# MAGIC --     3. SELECT: replace inv reference + add audit columns to output
# MAGIC --
# MAGIC --   Cross-bug interaction (Bug 1 + Bug 2):
# MAGIC --     - Bug 1 (MPS): subtract active PRO output (Quantity) from SO demand
# MAGIC --     - Bug 2 (L4L): subtract active PRO consumption (Remaining Qty) from inventory
# MAGIC --     - These operate on DIFFERENT data flows — no double-counting risk
# MAGIC --     - L4L receives clean dependent_demand จาก MPS (Bug 1 effect)
# MAGIC --       AND netted inventory (Bug 2 effect, this patch)
# MAGIC -- =====================================================================
# MAGIC -- ⚠️ ONLY CELL 2 changes vs v1 — Cells 1, 3-7 unchanged
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 1: Setup (UNCHANGED) ==============
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS Gold_Inventory_Lakehouse.mrp;
# MAGIC 
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInWrite = CORRECTED;
# MAGIC SET spark.sql.parquet.int96RebaseModeInRead = CORRECTED;
# MAGIC SET spark.sql.parquet.int96RebaseModeInWrite = CORRECTED;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 2 (PATCHED v2): Build gold_planned_orders_l4l ==============
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.gold_planned_orders_l4l
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH 
# MAGIC -- === Step 1: Direct SO demand (UNCHANGED) ===
# MAGIC direct_demand AS (
# MAGIC     SELECT 
# MAGIC         so.so_no                            AS source_so_no,
# MAGIC         so.so_line_no                       AS source_so_line,
# MAGIC         so.customer_no                      AS source_customer_no,
# MAGIC         so.customer_tier_code               AS source_customer_tier,
# MAGIC         so.customer_priority_weight         AS source_priority_weight,
# MAGIC         so.is_dnd                           AS source_is_dnd,
# MAGIC         so.ship_date                        AS source_ship_date,
# MAGIC         so.item_no,
# MAGIC         so.description                      AS item_description,
# MAGIC         so.item_category,
# MAGIC         so.qty_outstanding                  AS demand_qty,
# MAGIC         'SO_DIRECT'                         AS source_demand_type
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_SO so
# MAGIC     WHERE so.is_open = TRUE
# MAGIC       AND so.item_archetype = 'L4L'
# MAGIC       AND so.qty_outstanding > 0
# MAGIC ),
# MAGIC 
# MAGIC -- === Step 2: Dependent demand from MPS BOM (UNCHANGED) ===
# MAGIC -- NOTE: After Bug 1 patch, this table reflects clean SO demand 
# MAGIC --       (active PRO supply already subtracted at MPS level)
# MAGIC dependent_demand AS (
# MAGIC     SELECT 
# MAGIC         d.source_so_no,
# MAGIC         d.source_so_line,
# MAGIC         d.source_customer_no,
# MAGIC         d.source_customer_tier,
# MAGIC         d.source_priority_weight,
# MAGIC         d.source_is_dnd,
# MAGIC         d.parent_ship_date                  AS source_ship_date,
# MAGIC         d.component_item                    AS item_no,
# MAGIC         d.component_description             AS item_description,
# MAGIC         d.component_category                AS item_category,
# MAGIC         d.dependent_demand_qty              AS demand_qty,
# MAGIC         'COMPONENT_FROM_MPS'                AS source_demand_type
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_dependent_demand_from_mps d
# MAGIC     WHERE d.component_archetype = 'L4L'
# MAGIC       AND d.dependent_demand_qty > 0
# MAGIC ),
# MAGIC 
# MAGIC -- === Step 3: Combine all demand (UNCHANGED) ===
# MAGIC all_demand AS (
# MAGIC     SELECT * FROM direct_demand
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM dependent_demand
# MAGIC ),
# MAGIC 
# MAGIC -- === Step 4: Item master enrichment (UNCHANGED) ===
# MAGIC item_master_l4l AS (
# MAGIC     SELECT 
# MAGIC         item_no, scrap_pct, lot_accum_days,
# MAGIC         min_order_qty, max_order_qty, order_multiple,
# MAGIC         safety_stock_qty,
# MAGIC         base_lead_days_static, safety_lead_days_static, effective_lead_days,
# MAGIC         item_category, archetype,
# MAGIC         vendor_no, purch_uom, base_uom
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Item_Master
# MAGIC     WHERE archetype = 'L4L' AND is_blocked = FALSE
# MAGIC ),
# MAGIC 
# MAGIC -- === Step 5: Compute lot bucket per demand row (UNCHANGED) ===
# MAGIC demand_bucketed AS (
# MAGIC     SELECT 
# MAGIC         ad.*,
# MAGIC         im.lot_accum_days,
# MAGIC         im.scrap_pct,
# MAGIC         im.min_order_qty,
# MAGIC         im.max_order_qty,
# MAGIC         im.order_multiple,
# MAGIC         im.vendor_no,
# MAGIC         im.effective_lead_days,
# MAGIC         im.purch_uom,
# MAGIC         im.base_uom,
# MAGIC         CAST(DATEDIFF(ad.source_ship_date, DATE'2020-01-01') 
# MAGIC              / GREATEST(im.lot_accum_days, 1) AS BIGINT) AS lot_bucket_id,
# MAGIC         DATE_ADD(
# MAGIC             DATE'2020-01-01', 
# MAGIC             CAST(
# MAGIC                 CAST(DATEDIFF(ad.source_ship_date, DATE'2020-01-01') 
# MAGIC                      / GREATEST(im.lot_accum_days, 1) AS BIGINT) 
# MAGIC                 * GREATEST(im.lot_accum_days, 1) 
# MAGIC                 + GREATEST(im.lot_accum_days, 1) - 1
# MAGIC                 AS INT
# MAGIC             )
# MAGIC         ) AS bucket_end_date
# MAGIC     FROM all_demand ad
# MAGIC     LEFT JOIN item_master_l4l im ON ad.item_no = im.item_no
# MAGIC ),
# MAGIC 
# MAGIC -- === Step 6: Aggregate within lot bucket (UNCHANGED) ===
# MAGIC aggregated_demand AS (
# MAGIC     SELECT 
# MAGIC         item_no,
# MAGIC         MAX(item_description)               AS item_description,
# MAGIC         MAX(item_category)                  AS item_category,
# MAGIC         lot_bucket_id,
# MAGIC         MIN(source_ship_date)               AS earliest_ship_date,
# MAGIC         MAX(source_ship_date)               AS latest_ship_date,
# MAGIC         MAX(bucket_end_date)                AS bucket_end_date,
# MAGIC         SUM(demand_qty)                     AS total_demand_qty,
# MAGIC         COUNT(*)                            AS demand_lines_count,
# MAGIC         COUNT(DISTINCT source_so_no)        AS contributing_so_count,
# MAGIC         MAX(source_priority_weight)         AS max_priority_weight,
# MAGIC         BOOL_OR(source_is_dnd)              AS has_dnd_demand,
# MAGIC         MAX(lot_accum_days)                 AS lot_accum_days,
# MAGIC         MAX(scrap_pct)                      AS scrap_pct,
# MAGIC         MAX(min_order_qty)                  AS min_order_qty,
# MAGIC         MAX(max_order_qty)                  AS max_order_qty,
# MAGIC         MAX(order_multiple)                 AS order_multiple,
# MAGIC         MAX(vendor_no)                      AS vendor_no,
# MAGIC         MAX(effective_lead_days)            AS effective_lead_days,
# MAGIC         MAX(purch_uom)                      AS purch_uom,
# MAGIC         MAX(base_uom)                       AS base_uom
# MAGIC     FROM demand_bucketed
# MAGIC     GROUP BY item_no, lot_bucket_id
# MAGIC ),
# MAGIC 
# MAGIC -- === Step 7: Identify top contributing SO per bucket (UNCHANGED) ===
# MAGIC top_so_per_bucket AS (
# MAGIC     SELECT 
# MAGIC         item_no,
# MAGIC         lot_bucket_id,
# MAGIC         source_so_no,
# MAGIC         source_customer_tier,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY item_no, lot_bucket_id 
# MAGIC             ORDER BY source_priority_weight DESC, source_ship_date ASC
# MAGIC         ) AS rn
# MAGIC     FROM demand_bucketed
# MAGIC ),
# MAGIC 
# MAGIC -- ⭐ NEW v2: Active PRO Component Consumption (Bug 2 fix)
# MAGIC --
# MAGIC --   Logic:
# MAGIC --     - Source: Prod Order Component (NOT header — เพราะเราต้องการ qty ของ raw material ที่ commit ไว้)
# MAGIC --     - Filter: Status IN (Released, Firm Planned, Planned) — active commitments only
# MAGIC --     - CDC dedup: latest SystemRowVersion per (PRO No., PRO Line No., Line No.)
# MAGIC --     - Aggregate: SUM(Remaining Quantity) by Item No.
# MAGIC --     - EXCLUDE archetype=SKIP via INNER JOIN to item_master:
# MAGIC --         SKIP items (PLATING SOLUTION + MST master molds) เป็น phantom items
# MAGIC --         ไม่มี physical stock จริง (on_hand=0 ทุกตัว) — ไม่ต้องหัก inventory
# MAGIC active_pro_consumption AS (
# MAGIC     SELECT 
# MAGIC         c.`Item No.`                AS item_no,
# MAGIC         SUM(c.`Remaining Quantity`) AS pro_committed_qty
# MAGIC     FROM (
# MAGIC         SELECT 
# MAGIC             *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `Prod. Order No.`, `Prod. Order Line No.`, `Line No.`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Prod Order Component`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND Status IN ('Released', 'Firm Planned', 'Planned')
# MAGIC           AND `Remaining Quantity` > 0
# MAGIC     ) c
# MAGIC     INNER JOIN Gold_Inventory_Lakehouse.mrp.gold_Item_Master im 
# MAGIC             ON im.item_no = c.`Item No.`
# MAGIC     WHERE c.rn = 1
# MAGIC       AND im.archetype <> 'SKIP'
# MAGIC     GROUP BY c.`Item No.`
# MAGIC ),
# MAGIC 
# MAGIC -- ⭐ MODIFIED v2: ATP inventory (replaces available_inventory)
# MAGIC --   ATP = Available-To-Promise = qty_available_gross − active_pro_committed
# MAGIC atp_inventory AS (
# MAGIC     SELECT 
# MAGIC         inv.item_no,
# MAGIC         SUM(inv.qty_available)                          AS qty_available_gross,
# MAGIC         COALESCE(MAX(apc.pro_committed_qty), 0)         AS pro_committed_qty,
# MAGIC         GREATEST(
# MAGIC             SUM(inv.qty_available) - COALESCE(MAX(apc.pro_committed_qty), 0),
# MAGIC             0
# MAGIC         )                                               AS qty_available_atp
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Inventory inv
# MAGIC     LEFT JOIN active_pro_consumption apc ON apc.item_no = inv.item_no
# MAGIC     WHERE inv.is_blocked_location = FALSE
# MAGIC       AND inv.is_expired = FALSE
# MAGIC     GROUP BY inv.item_no
# MAGIC )
# MAGIC 
# MAGIC -- === Final: Generate planned L4L purchase orders ===
# MAGIC SELECT 
# MAGIC     UUID() AS plan_id,
# MAGIC     
# MAGIC     -- Source traceability
# MAGIC     tsb.source_so_no                    AS triggering_so_no,
# MAGIC     tsb.source_customer_tier            AS customer_tier,
# MAGIC     ad.max_priority_weight              AS priority_weight,
# MAGIC     ad.has_dnd_demand                   AS is_dnd,
# MAGIC     ad.contributing_so_count            AS contributing_so_count,
# MAGIC     ad.demand_lines_count               AS demand_lines_aggregated,
# MAGIC     
# MAGIC     -- Item info
# MAGIC     ad.item_no,
# MAGIC     ad.item_description,
# MAGIC     ad.item_category,
# MAGIC     'L4L'                               AS archetype,
# MAGIC     
# MAGIC     -- Sourcing
# MAGIC     ad.vendor_no,
# MAGIC     ad.purch_uom,
# MAGIC     ad.base_uom,
# MAGIC     
# MAGIC     -- Lot bucketing
# MAGIC     ad.lot_bucket_id,
# MAGIC     ad.lot_accum_days,
# MAGIC     ad.earliest_ship_date,
# MAGIC     ad.latest_ship_date,
# MAGIC     ad.bucket_end_date,
# MAGIC     
# MAGIC     -- Quantities (⭐ v2: now uses ATP-netted inventory)
# MAGIC     ad.total_demand_qty                 AS demand_qty,
# MAGIC     COALESCE(atp.qty_available_atp, 0)  AS onhand_available_qty,
# MAGIC     GREATEST(ad.total_demand_qty - COALESCE(atp.qty_available_atp, 0), 0) AS shortage_qty,
# MAGIC     
# MAGIC     -- ⭐ v2: NEW audit columns (visibility into ATP calculation)
# MAGIC     COALESCE(atp.qty_available_gross, 0)  AS onhand_gross_qty,
# MAGIC     COALESCE(atp.pro_committed_qty, 0)    AS active_pro_committed_qty,
# MAGIC     
# MAGIC     ad.scrap_pct,
# MAGIC     
# MAGIC     -- Apply lot sizing constraints to planned_qty (UNCHANGED logic, uses ATP shortage)
# MAGIC     LEAST(
# MAGIC         GREATEST(
# MAGIC             COALESCE(ad.min_order_qty, 0),
# MAGIC             CEIL(
# MAGIC                 GREATEST(ad.total_demand_qty - COALESCE(atp.qty_available_atp, 0), 0) 
# MAGIC                     * (1 + COALESCE(ad.scrap_pct, 0) / 100.0)
# MAGIC                 / GREATEST(ad.order_multiple, 1)
# MAGIC             ) * GREATEST(ad.order_multiple, 1)
# MAGIC         ),
# MAGIC         ad.max_order_qty
# MAGIC     ) AS planned_qty,
# MAGIC     
# MAGIC     -- Lot sizing context
# MAGIC     ad.min_order_qty,
# MAGIC     ad.max_order_qty,
# MAGIC     ad.order_multiple,
# MAGIC     
# MAGIC     -- Lead time / scheduling
# MAGIC     ad.effective_lead_days              AS lead_time_days,
# MAGIC     ad.earliest_ship_date               AS need_by_date,
# MAGIC     DATE_SUB(ad.earliest_ship_date, COALESCE(ad.effective_lead_days, 0)) AS suggested_order_date,
# MAGIC     
# MAGIC     -- Plan status (⭐ v2: COVERED_BY_STOCK now reflects ATP, not gross on_hand)
# MAGIC     CASE 
# MAGIC         WHEN ad.vendor_no IS NULL 
# MAGIC             THEN 'BLOCKED_NO_VENDOR'
# MAGIC         WHEN ad.effective_lead_days IS NULL OR ad.effective_lead_days = 0
# MAGIC             THEN 'BLOCKED_NO_LEAD_TIME'
# MAGIC         WHEN ad.total_demand_qty - COALESCE(atp.qty_available_atp, 0) <= 0 
# MAGIC             THEN 'COVERED_BY_STOCK'
# MAGIC         WHEN ad.has_dnd_demand = TRUE 
# MAGIC             THEN 'PROPOSED_DND'
# MAGIC         ELSE 'PROPOSED'
# MAGIC     END AS plan_status,
# MAGIC     
# MAGIC     CASE 
# MAGIC         WHEN ad.vendor_no IS NULL 
# MAGIC             THEN 'No vendor on Item card — set in BC'
# MAGIC         WHEN ad.effective_lead_days IS NULL OR ad.effective_lead_days = 0
# MAGIC             THEN 'No Lead Time Calculation on Item card — set in BC'
# MAGIC         ELSE NULL
# MAGIC     END AS exception_reason,
# MAGIC     
# MAGIC     CURRENT_TIMESTAMP() AS plan_run_at
# MAGIC 
# MAGIC FROM aggregated_demand ad
# MAGIC LEFT JOIN top_so_per_bucket tsb 
# MAGIC        ON ad.item_no = tsb.item_no 
# MAGIC       AND ad.lot_bucket_id = tsb.lot_bucket_id 
# MAGIC       AND tsb.rn = 1
# MAGIC LEFT JOIN atp_inventory atp ON ad.item_no = atp.item_no;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 3: Validation — plan status summary (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     plan_status,
# MAGIC     COUNT(*) AS plan_count,
# MAGIC     COUNT(DISTINCT item_no) AS unique_items,
# MAGIC     ROUND(SUM(planned_qty), 2) AS total_planned_qty,
# MAGIC     ROUND(AVG(demand_lines_aggregated), 1) AS avg_aggregation_factor
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_l4l
# MAGIC GROUP BY plan_status
# MAGIC ORDER BY plan_count DESC;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 4: L4L plans by item category (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     item_category,
# MAGIC     plan_status,
# MAGIC     COUNT(*) AS plan_count,
# MAGIC     COUNT(DISTINCT item_no) AS unique_items,
# MAGIC     ROUND(SUM(planned_qty), 2) AS total_planned_qty
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_l4l
# MAGIC WHERE plan_status IN ('PROPOSED', 'PROPOSED_DND')
# MAGIC GROUP BY item_category, plan_status
# MAGIC ORDER BY item_category, plan_status;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 5: Top 20 L4L plans by priority (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     item_no,
# MAGIC     item_description,
# MAGIC     item_category,
# MAGIC     customer_tier,
# MAGIC     priority_weight,
# MAGIC     is_dnd,
# MAGIC     contributing_so_count,
# MAGIC     demand_lines_aggregated,
# MAGIC     demand_qty,
# MAGIC     onhand_available_qty,
# MAGIC     shortage_qty,
# MAGIC     planned_qty,
# MAGIC     vendor_no,
# MAGIC     earliest_ship_date,
# MAGIC     suggested_order_date,
# MAGIC     plan_status
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_l4l
# MAGIC WHERE plan_status IN ('PROPOSED', 'PROPOSED_DND')
# MAGIC ORDER BY 
# MAGIC     is_dnd DESC,
# MAGIC     priority_weight DESC,
# MAGIC     earliest_ship_date ASC,
# MAGIC     planned_qty DESC
# MAGIC LIMIT 20;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 6: Aggregation effectiveness (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     'Total demand lines (input)' AS metric,
# MAGIC     SUM(demand_lines_aggregated) AS value
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_l4l
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Total POs generated (output)',
# MAGIC     COUNT(*)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_l4l
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Aggregation ratio (lines per PO)',
# MAGIC     CAST(SUM(demand_lines_aggregated) / NULLIF(COUNT(*), 0) AS BIGINT)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_l4l;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 7: Exception report (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     item_category,
# MAGIC     plan_status,
# MAGIC     COUNT(*) AS blocked_count,
# MAGIC     COUNT(DISTINCT item_no) AS unique_items,
# MAGIC     ROUND(SUM(demand_qty), 2) AS demand_qty
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_l4l
# MAGIC WHERE plan_status LIKE 'BLOCKED%'
# MAGIC GROUP BY item_category, plan_status
# MAGIC ORDER BY blocked_count DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # nb_gold_planned_orders_rop  (Phase 3 — Archetype 4 of 5)

# CELL ********************

# MAGIC %%sql
# MAGIC -- =====================================================================
# MAGIC -- Notebook: nb_gold_planned_orders_rop  (PATCHED v3 — honor reordering_policy)
# MAGIC -- Layer:    Gold MRP Engine
# MAGIC -- Output:   Gold_Inventory_Lakehouse.mrp.gold_planned_orders_rop
# MAGIC --
# MAGIC -- ROP Archetype = Reorder Point (threshold-based)
# MAGIC --
# MAGIC -- 🐛 BUG FIX v3 (2026-05-02): Engine ignored reordering_policy field
# MAGIC --   v2 logic: planned_qty CASE ดูจาก max_inv_qty > 0 เท่านั้น
# MAGIC --             → Fixed Reorder Qty. items ที่มี max_inv_qty > 0 จะถูกคำนวณ
# MAGIC --               เป็น (max_inv - net_position) แทน ROQ ตายตัว
# MAGIC --             → 81+ items (~70% ของ Fixed Reorder Qty. items) คำนวณผิด
# MAGIC --             → FCB-000230-BR: planned_qty=33,037 (max-inv) → ควรเป็น 6,500 (ROQ)
# MAGIC --
# MAGIC --   v3 fix: planned_qty CASE based on reordering_policy field
# MAGIC --     - Fixed Reorder Qty. → ROQ ตายตัว (BC standard)
# MAGIC --     - Maximum Qty.       → max_inv - net_position (top-up to max)
# MAGIC --     - Lot-for-Lot        → demand - on_hand (defensive: shouldn't see in ROP)
# MAGIC --     - Order              → demand_qty (defensive)
# MAGIC --     - Default fallback   → ROQ
# MAGIC --
# MAGIC -- 🐛 BUG 1 FIX v2 (PRESERVED): Active PRO output not subtracted from SO demand
# MAGIC --   active_pro_net_supply CTE — same pattern as MPS/MST_DIRECT
# MAGIC --
# MAGIC -- 🐛 BUG 2 FIX v2 (PRESERVED): Engine inventory not netted from PRO commits
# MAGIC --   active_pro_consumption CTE (EXCLUDE archetype='SKIP')
# MAGIC --   atp_inventory CTE replaces available_inventory
# MAGIC --
# MAGIC -- ⚠️ Changes vs v2: Cell 2 only (all_rop_items adds reordering_policy,
# MAGIC --    net_position passes it through, final SELECT uses policy-aware CASE)
# MAGIC --    Cells 1, 3-7 unchanged. Cell 8 enhanced with policy distribution check.
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 1: Setup (UNCHANGED from v2) ==============
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS Gold_Inventory_Lakehouse.mrp;
# MAGIC 
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInWrite = CORRECTED;
# MAGIC SET spark.sql.parquet.int96RebaseModeInRead = CORRECTED;
# MAGIC SET spark.sql.parquet.int96RebaseModeInWrite = CORRECTED;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 2 (PATCHED v3): Build gold_planned_orders_rop ==============
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.gold_planned_orders_rop
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH 
# MAGIC -- v2: Active PRO net supply for Bug 1 (UNCHANGED)
# MAGIC active_pro_net_supply AS (
# MAGIC     SELECT 
# MAGIC         h.`Source No.`             AS item_no,
# MAGIC         h.`Sales Order No.`        AS so_no,
# MAGIC         h.`Sales Order Line No.`   AS so_line_no,
# MAGIC         SUM(h.Quantity)            AS active_pro_qty
# MAGIC     FROM (
# MAGIC         SELECT 
# MAGIC             *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `No.` 
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Production Order`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND Status IN ('Released', 'Firm Planned', 'Planned')
# MAGIC     ) h
# MAGIC     WHERE h.rn = 1
# MAGIC       AND h.`Sales Order No.` IS NOT NULL
# MAGIC       AND h.`Sales Order No.` <> ''
# MAGIC       AND h.`Sales Order Line No.` IS NOT NULL
# MAGIC       AND h.`Sales Order Line No.` > 0
# MAGIC       AND h.`Source No.` IS NOT NULL
# MAGIC       AND h.`Source No.` <> ''
# MAGIC     GROUP BY 
# MAGIC         h.`Source No.`,
# MAGIC         h.`Sales Order No.`,
# MAGIC         h.`Sales Order Line No.`
# MAGIC ),
# MAGIC 
# MAGIC -- v2: Direct SO demand with Bug 1 netting (UNCHANGED)
# MAGIC direct_demand AS (
# MAGIC     SELECT 
# MAGIC         so.so_no                            AS source_so_no,
# MAGIC         so.customer_no                      AS source_customer_no,
# MAGIC         so.customer_tier_code               AS source_customer_tier,
# MAGIC         so.customer_priority_weight         AS source_priority_weight,
# MAGIC         so.is_dnd                           AS source_is_dnd,
# MAGIC         so.ship_date                        AS source_ship_date,
# MAGIC         so.item_no,
# MAGIC         GREATEST(
# MAGIC             so.qty_outstanding - COALESCE(apns.active_pro_qty, 0),
# MAGIC             0
# MAGIC         )                                   AS demand_qty,
# MAGIC         so.qty_outstanding                  AS demand_qty_gross,
# MAGIC         COALESCE(apns.active_pro_qty, 0)    AS active_pro_supply_qty
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_SO so
# MAGIC     LEFT JOIN active_pro_net_supply apns
# MAGIC         ON  apns.item_no    = so.item_no
# MAGIC        AND apns.so_no      = so.so_no
# MAGIC        AND apns.so_line_no = so.so_line_no
# MAGIC     WHERE so.is_open = TRUE
# MAGIC       AND so.item_archetype = 'ROP'
# MAGIC       AND so.qty_outstanding > 0
# MAGIC       AND GREATEST(
# MAGIC             so.qty_outstanding - COALESCE(apns.active_pro_qty, 0), 0
# MAGIC           ) > 0
# MAGIC ),
# MAGIC 
# MAGIC -- Dependent demand from MPS BOM (UNCHANGED)
# MAGIC dependent_demand AS (
# MAGIC     SELECT 
# MAGIC         d.source_so_no,
# MAGIC         d.source_customer_no,
# MAGIC         d.source_customer_tier,
# MAGIC         d.source_priority_weight,
# MAGIC         d.source_is_dnd,
# MAGIC         d.parent_ship_date                  AS source_ship_date,
# MAGIC         d.component_item                    AS item_no,
# MAGIC         d.dependent_demand_qty              AS demand_qty,
# MAGIC         d.dependent_demand_qty              AS demand_qty_gross,
# MAGIC         CAST(0 AS DECIMAL(38,20))           AS active_pro_supply_qty
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_dependent_demand_from_mps d
# MAGIC     WHERE d.component_archetype = 'ROP'
# MAGIC       AND d.dependent_demand_qty > 0
# MAGIC ),
# MAGIC 
# MAGIC -- Combine and aggregate per item (UNCHANGED)
# MAGIC all_demand AS (
# MAGIC     SELECT * FROM direct_demand
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM dependent_demand
# MAGIC ),
# MAGIC 
# MAGIC aggregated_demand AS (
# MAGIC     SELECT 
# MAGIC         item_no,
# MAGIC         SUM(demand_qty)                     AS total_demand_qty,
# MAGIC         SUM(demand_qty_gross)               AS total_demand_qty_gross,
# MAGIC         SUM(active_pro_supply_qty)          AS total_active_pro_supply,
# MAGIC         COUNT(DISTINCT source_so_no)        AS contributing_so_count,
# MAGIC         MAX(source_priority_weight)         AS max_priority_weight,
# MAGIC         BOOL_OR(source_is_dnd)              AS has_dnd_demand,
# MAGIC         MIN(source_ship_date)               AS earliest_ship_date,
# MAGIC         MAX(source_ship_date)               AS latest_ship_date
# MAGIC     FROM all_demand
# MAGIC     GROUP BY item_no
# MAGIC ),
# MAGIC 
# MAGIC -- ⭐ v3 PATCH: All ROP items — added reordering_policy
# MAGIC all_rop_items AS (
# MAGIC     SELECT 
# MAGIC         item_no, item_category, archetype,
# MAGIC         reordering_policy,                  -- ⭐ NEW v3
# MAGIC         rop_qty, roq_qty, max_inv_qty, safety_stock_qty,
# MAGIC         scrap_pct, min_order_qty, max_order_qty, order_multiple,
# MAGIC         base_lead_days_static, safety_lead_days_static, effective_lead_days,
# MAGIC         vendor_no, purch_uom, base_uom,
# MAGIC         description AS item_description
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Item_Master
# MAGIC     WHERE archetype = 'ROP' AND is_blocked = FALSE
# MAGIC ),
# MAGIC 
# MAGIC -- v2: Active PRO Component Consumption (UNCHANGED)
# MAGIC active_pro_consumption AS (
# MAGIC     SELECT 
# MAGIC         c.`Item No.`                AS item_no,
# MAGIC         SUM(c.`Remaining Quantity`) AS pro_committed_qty
# MAGIC     FROM (
# MAGIC         SELECT 
# MAGIC             *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `Prod. Order No.`, `Prod. Order Line No.`, `Line No.`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Prod Order Component`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND Status IN ('Released', 'Firm Planned', 'Planned')
# MAGIC           AND `Remaining Quantity` > 0
# MAGIC     ) c
# MAGIC     INNER JOIN Gold_Inventory_Lakehouse.mrp.gold_Item_Master im 
# MAGIC             ON im.item_no = c.`Item No.`
# MAGIC     WHERE c.rn = 1
# MAGIC       AND im.archetype <> 'SKIP'
# MAGIC     GROUP BY c.`Item No.`
# MAGIC ),
# MAGIC 
# MAGIC -- v2: ATP inventory (UNCHANGED)
# MAGIC atp_inventory AS (
# MAGIC     SELECT 
# MAGIC         inv.item_no,
# MAGIC         SUM(inv.qty_available)                          AS qty_available_gross,
# MAGIC         COALESCE(MAX(apc.pro_committed_qty), 0)         AS pro_committed_qty,
# MAGIC         GREATEST(
# MAGIC             SUM(inv.qty_available) - COALESCE(MAX(apc.pro_committed_qty), 0),
# MAGIC             0
# MAGIC         )                                               AS qty_available_atp
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Inventory inv
# MAGIC     LEFT JOIN active_pro_consumption apc ON apc.item_no = inv.item_no
# MAGIC     WHERE inv.is_blocked_location = FALSE
# MAGIC       AND inv.is_expired = FALSE
# MAGIC     GROUP BY inv.item_no
# MAGIC ),
# MAGIC 
# MAGIC -- ⭐ v3 PATCH: net_position — passes reordering_policy through
# MAGIC net_position AS (
# MAGIC     SELECT 
# MAGIC         ri.item_no,
# MAGIC         ri.item_description,
# MAGIC         ri.item_category,
# MAGIC         ri.archetype,
# MAGIC         ri.reordering_policy,               -- ⭐ NEW v3
# MAGIC         ri.vendor_no,
# MAGIC         ri.purch_uom,
# MAGIC         ri.base_uom,
# MAGIC         ri.rop_qty,
# MAGIC         ri.roq_qty,
# MAGIC         ri.max_inv_qty,
# MAGIC         ri.safety_stock_qty,
# MAGIC         ri.scrap_pct,
# MAGIC         ri.min_order_qty,
# MAGIC         ri.max_order_qty,
# MAGIC         ri.order_multiple,
# MAGIC         ri.base_lead_days_static,
# MAGIC         ri.safety_lead_days_static,
# MAGIC         ri.effective_lead_days,
# MAGIC         
# MAGIC         -- Demand info (Bug 1 netted)
# MAGIC         COALESCE(ad.total_demand_qty, 0)            AS allocated_demand_qty,
# MAGIC         COALESCE(ad.total_demand_qty_gross, 0)      AS allocated_demand_qty_gross,
# MAGIC         COALESCE(ad.total_active_pro_supply, 0)     AS active_pro_supply_qty,
# MAGIC         COALESCE(ad.contributing_so_count, 0)       AS contributing_so_count,
# MAGIC         COALESCE(ad.max_priority_weight, 0)         AS max_priority_weight,
# MAGIC         COALESCE(ad.has_dnd_demand, FALSE)          AS has_dnd_demand,
# MAGIC         ad.earliest_ship_date,
# MAGIC         ad.latest_ship_date,
# MAGIC         
# MAGIC         -- Bug 2: ATP-netted inventory
# MAGIC         COALESCE(atp.qty_available_atp, 0)          AS onhand_available_qty,
# MAGIC         COALESCE(atp.qty_available_gross, 0)        AS onhand_gross_qty,
# MAGIC         COALESCE(atp.pro_committed_qty, 0)          AS active_pro_committed_qty,
# MAGIC         
# MAGIC         -- Net position uses ATP
# MAGIC         COALESCE(atp.qty_available_atp, 0) - COALESCE(ad.total_demand_qty, 0) 
# MAGIC             AS net_position_qty,
# MAGIC         
# MAGIC         -- Trigger conditions (use ATP)
# MAGIC         (COALESCE(atp.qty_available_atp, 0) - COALESCE(ad.total_demand_qty, 0) 
# MAGIC             <= COALESCE(ri.rop_qty, 0)) AS rop_triggered,
# MAGIC         
# MAGIC         (COALESCE(ad.total_demand_qty, 0) > COALESCE(atp.qty_available_atp, 0)) 
# MAGIC             AS has_shortage
# MAGIC     FROM all_rop_items ri
# MAGIC     LEFT JOIN aggregated_demand ad ON ri.item_no = ad.item_no
# MAGIC     LEFT JOIN atp_inventory atp ON ri.item_no = atp.item_no
# MAGIC )
# MAGIC 
# MAGIC -- === Final: Generate planned ROP orders ===
# MAGIC SELECT 
# MAGIC     UUID() AS plan_id,
# MAGIC     
# MAGIC     -- Item info
# MAGIC     item_no,
# MAGIC     item_description,
# MAGIC     item_category,
# MAGIC     'ROP' AS archetype,
# MAGIC     reordering_policy,                  -- ⭐ NEW v3 audit column
# MAGIC     
# MAGIC     -- Sourcing
# MAGIC     vendor_no,
# MAGIC     purch_uom,
# MAGIC     base_uom,
# MAGIC     
# MAGIC     -- Priority context
# MAGIC     max_priority_weight                 AS priority_weight,
# MAGIC     has_dnd_demand                      AS is_dnd,
# MAGIC     contributing_so_count,
# MAGIC     
# MAGIC     -- ROP parameters
# MAGIC     rop_qty,
# MAGIC     roq_qty,
# MAGIC     max_inv_qty,
# MAGIC     safety_stock_qty,
# MAGIC     
# MAGIC     -- Quantities (v2: ATP-aware)
# MAGIC     onhand_available_qty,
# MAGIC     onhand_gross_qty,
# MAGIC     active_pro_committed_qty,
# MAGIC     active_pro_supply_qty,
# MAGIC     allocated_demand_qty                AS demand_qty,
# MAGIC     allocated_demand_qty_gross          AS demand_qty_gross,
# MAGIC     net_position_qty,
# MAGIC     GREATEST(allocated_demand_qty - onhand_available_qty, 0) AS shortage_qty,
# MAGIC     has_shortage,
# MAGIC     rop_triggered,
# MAGIC     
# MAGIC     scrap_pct,
# MAGIC     
# MAGIC     -- ⭐ v3 PATCH: Compute planned qty by reordering_policy (BC standard MRP)
# MAGIC     CASE
# MAGIC         -- BC standard: Fixed Reorder Qty. = order ROQ ตายตัวทุกครั้งที่ trigger
# MAGIC         WHEN reordering_policy = 'Fixed Reorder Qty.' THEN
# MAGIC             COALESCE(roq_qty, 0)
# MAGIC         
# MAGIC         -- BC standard: Maximum Qty. = top up to max_inv_qty
# MAGIC         WHEN reordering_policy = 'Maximum Qty.' AND COALESCE(max_inv_qty, 0) > 0 THEN
# MAGIC             GREATEST(max_inv_qty - net_position_qty, 0)
# MAGIC         
# MAGIC         -- Defensive: ROP archetype shouldn't see L4L (mapped to L4L engine), 
# MAGIC         -- but if config drift causes one to land here — handle gracefully
# MAGIC         WHEN reordering_policy = 'Lot-for-Lot' THEN
# MAGIC             GREATEST(allocated_demand_qty - onhand_available_qty, 0)
# MAGIC         
# MAGIC         -- Defensive: ROP archetype shouldn't see Order policy
# MAGIC         WHEN reordering_policy = 'Order' THEN
# MAGIC             allocated_demand_qty
# MAGIC         
# MAGIC         -- Last-resort fallback (e.g., Maximum Qty. with max_inv_qty=0)
# MAGIC         ELSE COALESCE(roq_qty, 0)
# MAGIC     END                                 AS policy_quantity,
# MAGIC     
# MAGIC     -- ⭐ v3: Audit column — debug which branch was used
# MAGIC     CASE
# MAGIC         WHEN reordering_policy = 'Fixed Reorder Qty.' THEN 'FIXED_ROQ'
# MAGIC         WHEN reordering_policy = 'Maximum Qty.' AND COALESCE(max_inv_qty, 0) > 0 THEN 'MAX_INV'
# MAGIC         WHEN reordering_policy = 'Lot-for-Lot' THEN 'L4L_DEFENSIVE'
# MAGIC         WHEN reordering_policy = 'Order' THEN 'ORDER_DEFENSIVE'
# MAGIC         ELSE 'FALLBACK_ROQ'
# MAGIC     END                                 AS policy_branch_used,
# MAGIC     
# MAGIC     -- Apply min/max/multiple modifiers (UNCHANGED structure, just on new policy_qty)
# MAGIC     LEAST(
# MAGIC         GREATEST(
# MAGIC             COALESCE(min_order_qty, 0),
# MAGIC             CASE
# MAGIC                 WHEN reordering_policy = 'Fixed Reorder Qty.' THEN
# MAGIC                     COALESCE(roq_qty, 0)
# MAGIC                 WHEN reordering_policy = 'Maximum Qty.' AND COALESCE(max_inv_qty, 0) > 0 THEN
# MAGIC                     GREATEST(max_inv_qty - net_position_qty, 0)
# MAGIC                 WHEN reordering_policy = 'Lot-for-Lot' THEN
# MAGIC                     GREATEST(allocated_demand_qty - onhand_available_qty, 0)
# MAGIC                 WHEN reordering_policy = 'Order' THEN
# MAGIC                     allocated_demand_qty
# MAGIC                 ELSE COALESCE(roq_qty, 0)
# MAGIC             END
# MAGIC         ),
# MAGIC         max_order_qty
# MAGIC     ) AS planned_qty_raw,
# MAGIC     
# MAGIC     CEIL(
# MAGIC         LEAST(
# MAGIC             GREATEST(
# MAGIC                 COALESCE(min_order_qty, 0),
# MAGIC                 CASE
# MAGIC                     WHEN reordering_policy = 'Fixed Reorder Qty.' THEN
# MAGIC                         COALESCE(roq_qty, 0)
# MAGIC                     WHEN reordering_policy = 'Maximum Qty.' AND COALESCE(max_inv_qty, 0) > 0 THEN
# MAGIC                         GREATEST(max_inv_qty - net_position_qty, 0)
# MAGIC                     WHEN reordering_policy = 'Lot-for-Lot' THEN
# MAGIC                         GREATEST(allocated_demand_qty - onhand_available_qty, 0)
# MAGIC                     WHEN reordering_policy = 'Order' THEN
# MAGIC                         allocated_demand_qty
# MAGIC                     ELSE COALESCE(roq_qty, 0)
# MAGIC                 END
# MAGIC             ),
# MAGIC             max_order_qty
# MAGIC         ) / GREATEST(order_multiple, 1)
# MAGIC     ) * GREATEST(order_multiple, 1) AS planned_qty,
# MAGIC     
# MAGIC     -- Lot sizing context
# MAGIC     min_order_qty,
# MAGIC     max_order_qty,
# MAGIC     order_multiple,
# MAGIC     
# MAGIC     -- Lead time / scheduling
# MAGIC     base_lead_days_static               AS lead_time_days,
# MAGIC     safety_lead_days_static,
# MAGIC     effective_lead_days,
# MAGIC     
# MAGIC     earliest_ship_date                  AS need_by_date,
# MAGIC     DATE_SUB(
# MAGIC         COALESCE(earliest_ship_date, CURRENT_DATE()), 
# MAGIC         COALESCE(effective_lead_days, 0)
# MAGIC     ) AS suggested_order_date,
# MAGIC     
# MAGIC     -- Plan status (UNCHANGED)
# MAGIC     CASE 
# MAGIC         WHEN vendor_no IS NULL 
# MAGIC             THEN 'BLOCKED_NO_VENDOR'
# MAGIC         WHEN COALESCE(rop_qty, 0) = 0 AND COALESCE(roq_qty, 0) = 0
# MAGIC             THEN 'BLOCKED_NO_ROP_PARAMS'
# MAGIC         WHEN NOT rop_triggered AND allocated_demand_qty <= onhand_available_qty
# MAGIC             THEN 'COVERED_BY_STOCK'
# MAGIC         WHEN has_dnd_demand = TRUE AND rop_triggered
# MAGIC             THEN 'PROPOSED_DND'
# MAGIC         WHEN rop_triggered
# MAGIC             THEN 'PROPOSED'
# MAGIC         ELSE 'NO_TRIGGER'
# MAGIC     END AS plan_status,
# MAGIC     
# MAGIC     -- ⭐ v3: Enhanced exception_reason — flag policy/parameter mismatches
# MAGIC     CASE 
# MAGIC         WHEN vendor_no IS NULL 
# MAGIC             THEN 'No vendor on Item card — set in BC'
# MAGIC         WHEN COALESCE(rop_qty, 0) = 0 AND COALESCE(roq_qty, 0) = 0
# MAGIC             THEN 'ROP item but no Reorder Point/Quantity set in BC'
# MAGIC         WHEN reordering_policy = 'Fixed Reorder Qty.' AND COALESCE(roq_qty, 0) = 0
# MAGIC             THEN 'Fixed Reorder Qty. policy but ROQ=0 in BC'
# MAGIC         WHEN reordering_policy = 'Maximum Qty.' AND COALESCE(max_inv_qty, 0) = 0
# MAGIC             THEN 'Maximum Qty. policy but Max Inv=0 in BC — engine fell back to ROQ'
# MAGIC         WHEN reordering_policy NOT IN ('Fixed Reorder Qty.', 'Maximum Qty.', 'Lot-for-Lot', 'Order')
# MAGIC             THEN CONCAT('Unexpected reordering_policy: ', COALESCE(reordering_policy, 'NULL'), ' — engine fell back to ROQ')
# MAGIC         ELSE NULL
# MAGIC     END AS exception_reason,
# MAGIC     
# MAGIC     CURRENT_TIMESTAMP() AS plan_run_at
# MAGIC 
# MAGIC FROM net_position
# MAGIC WHERE rop_triggered = TRUE
# MAGIC    OR has_shortage = TRUE
# MAGIC    OR allocated_demand_qty > 0;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 3: Validation — plan status summary (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     plan_status,
# MAGIC     COUNT(*) AS plan_count,
# MAGIC     COUNT(DISTINCT item_no) AS unique_items,
# MAGIC     ROUND(SUM(planned_qty), 2) AS total_planned_qty
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_rop
# MAGIC GROUP BY plan_status
# MAGIC ORDER BY plan_count DESC;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 4: ROP plans by item category (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     item_category,
# MAGIC     plan_status,
# MAGIC     COUNT(*) AS plan_count,
# MAGIC     COUNT(DISTINCT item_no) AS unique_items,
# MAGIC     ROUND(SUM(planned_qty), 2) AS total_planned_qty
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_rop
# MAGIC WHERE plan_status IN ('PROPOSED', 'PROPOSED_DND')
# MAGIC GROUP BY item_category, plan_status
# MAGIC ORDER BY item_category, plan_status;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 5: Top 20 ROP plans by priority (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     item_no,
# MAGIC     item_description,
# MAGIC     item_category,
# MAGIC     vendor_no,
# MAGIC     priority_weight,
# MAGIC     is_dnd,
# MAGIC     contributing_so_count,
# MAGIC     rop_qty,
# MAGIC     onhand_available_qty,
# MAGIC     demand_qty,
# MAGIC     net_position_qty,
# MAGIC     shortage_qty,
# MAGIC     roq_qty,
# MAGIC     planned_qty,
# MAGIC     need_by_date,
# MAGIC     suggested_order_date,
# MAGIC     plan_status
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_rop
# MAGIC WHERE plan_status IN ('PROPOSED', 'PROPOSED_DND')
# MAGIC ORDER BY 
# MAGIC     is_dnd DESC,
# MAGIC     priority_weight DESC,
# MAGIC     (onhand_available_qty / NULLIF(rop_qty, 0)) ASC,
# MAGIC     planned_qty DESC
# MAGIC LIMIT 20;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 6: Net position health check (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN net_position_qty < 0 THEN 'CRITICAL: Negative net position'
# MAGIC         WHEN rop_triggered = TRUE THEN 'TRIGGERED: Below ROP'
# MAGIC         WHEN net_position_qty <= rop_qty * 1.5 THEN 'WATCH: Within 1.5x ROP'
# MAGIC         ELSE 'OK: Above 1.5x ROP'
# MAGIC     END AS health_status,
# MAGIC     COUNT(*) AS items,
# MAGIC     ROUND(SUM(onhand_available_qty), 2) AS total_onhand,
# MAGIC     ROUND(SUM(demand_qty), 2) AS total_demand
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_rop
# MAGIC GROUP BY health_status
# MAGIC ORDER BY 
# MAGIC     CASE 
# MAGIC         WHEN health_status LIKE 'CRITICAL%' THEN 1
# MAGIC         WHEN health_status LIKE 'TRIGGERED%' THEN 2
# MAGIC         WHEN health_status LIKE 'WATCH%' THEN 3
# MAGIC         ELSE 4
# MAGIC     END;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 7: Exception report (UNCHANGED, but new v3 reasons may appear) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     item_no,
# MAGIC     item_description,
# MAGIC     item_category,
# MAGIC     plan_status,
# MAGIC     exception_reason,
# MAGIC     rop_qty,
# MAGIC     roq_qty,
# MAGIC     onhand_available_qty,
# MAGIC     demand_qty
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_rop
# MAGIC WHERE plan_status LIKE 'BLOCKED%'
# MAGIC    OR exception_reason IS NOT NULL
# MAGIC ORDER BY priority_weight DESC NULLS LAST, demand_qty DESC
# MAGIC LIMIT 30;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 8 (ENHANCED v3): Policy distribution + Bug 1/2/3 validation ==============
# MAGIC 
# MAGIC -- 8a. Policy distribution — confirm all policies handled
# MAGIC SELECT 
# MAGIC     reordering_policy,
# MAGIC     policy_branch_used,
# MAGIC     COUNT(*)                          AS plan_count,
# MAGIC     COUNT(DISTINCT item_no)           AS unique_items,
# MAGIC     ROUND(SUM(planned_qty), 0)        AS total_planned_qty,
# MAGIC     ROUND(AVG(planned_qty), 0)        AS avg_planned_qty
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_rop
# MAGIC WHERE plan_status IN ('PROPOSED', 'PROPOSED_DND')
# MAGIC GROUP BY reordering_policy, policy_branch_used
# MAGIC ORDER BY plan_count DESC;
# MAGIC 
# MAGIC -- 8b. Spot-check: Fixed Reorder Qty. items should have planned_qty close to roq_qty
# MAGIC -- (close — not exact — because of min_order_qty, max_order_qty, order_multiple modifiers)
# MAGIC SELECT
# MAGIC     item_no,
# MAGIC     item_description,
# MAGIC     reordering_policy,
# MAGIC     policy_branch_used,
# MAGIC     rop_qty,
# MAGIC     roq_qty,
# MAGIC     max_inv_qty,
# MAGIC     onhand_available_qty,
# MAGIC     net_position_qty,
# MAGIC     planned_qty,
# MAGIC     -- Should match expectation
# MAGIC     CASE
# MAGIC         WHEN reordering_policy = 'Fixed Reorder Qty.' AND planned_qty >= roq_qty
# MAGIC                                                       AND planned_qty <= GREATEST(roq_qty, min_order_qty) * 1.5 + order_multiple
# MAGIC             THEN 'OK: planned_qty ≈ ROQ (±modifiers)'
# MAGIC         WHEN reordering_policy = 'Fixed Reorder Qty.' 
# MAGIC             THEN CONCAT('VERIFY: ROQ=', CAST(roq_qty AS STRING), ' min=', CAST(min_order_qty AS STRING), 
# MAGIC                         ' mult=', CAST(order_multiple AS STRING), ' → planned=', CAST(planned_qty AS STRING))
# MAGIC         ELSE 'N/A (other policy)'
# MAGIC     END AS verdict
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_rop
# MAGIC WHERE reordering_policy = 'Fixed Reorder Qty.'
# MAGIC   AND plan_status IN ('PROPOSED', 'PROPOSED_DND')
# MAGIC ORDER BY 
# MAGIC     CASE WHEN planned_qty < roq_qty OR planned_qty > GREATEST(roq_qty, min_order_qty) * 1.5 + order_multiple
# MAGIC          THEN 0 ELSE 1 END,
# MAGIC     planned_qty DESC
# MAGIC LIMIT 20;
# MAGIC 
# MAGIC -- 8c. FCB-000230-BR canary — should now show planned_qty=6500 (was 33037)
# MAGIC SELECT 
# MAGIC     'FCB-000230-BR canary' AS check_name,
# MAGIC     item_no,
# MAGIC     reordering_policy,
# MAGIC     policy_branch_used,
# MAGIC     roq_qty,
# MAGIC     onhand_available_qty,
# MAGIC     planned_qty,
# MAGIC     plan_status
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_rop
# MAGIC WHERE item_no = 'FCB-000230-BR';
# MAGIC 
# MAGIC -- 8d. Overall validation summary
# MAGIC SELECT 
# MAGIC     'rop_v3_validate'                              AS check_name,
# MAGIC     COUNT(*)                                       AS total_plans,
# MAGIC     SUM(active_pro_supply_qty)                     AS total_pro_output_supply,
# MAGIC     SUM(active_pro_committed_qty)                  AS total_pro_consumption,
# MAGIC     SUM(planned_qty)                               AS total_planned,
# MAGIC     SUM(CASE WHEN plan_status = 'COVERED_BY_STOCK' THEN 1 ELSE 0 END) AS covered_count,
# MAGIC     SUM(CASE WHEN plan_status LIKE '%PROPOSED%' THEN 1 ELSE 0 END)    AS proposed_count,
# MAGIC     SUM(CASE WHEN policy_branch_used = 'FIXED_ROQ'    THEN 1 ELSE 0 END) AS fixed_roq_branch,
# MAGIC     SUM(CASE WHEN policy_branch_used = 'MAX_INV'      THEN 1 ELSE 0 END) AS max_inv_branch,
# MAGIC     SUM(CASE WHEN policy_branch_used = 'FALLBACK_ROQ' THEN 1 ELSE 0 END) AS fallback_branch,
# MAGIC     MAX(plan_run_at)                               AS last_run
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_rop;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

#  # nb_gold_planned_orders_order  (Phase 3 — Archetype 5 of 5)

# CELL ********************

# MAGIC %%sql
# MAGIC -- =====================================================================
# MAGIC -- Notebook: nb_gold_planned_orders_order  (PATCHED v2 — Bug 1 + Bug 2)
# MAGIC -- Layer:    Gold MRP Engine
# MAGIC -- Output:   Gold_Inventory_Lakehouse.mrp.gold_planned_orders_order
# MAGIC -- 
# MAGIC -- ORDER Archetype = Per-SO Order Planning (NO aggregation)
# MAGIC -- 1 SO line = 1 PO line — preserves per-SO grain for diamond traceability
# MAGIC --
# MAGIC -- 🐛 BUG 1 FIX (NEW v2): Active PRO output not subtracted from SO demand
# MAGIC --   v2 fix: active_pro_net_supply CTE — same pattern as MPS/MST_DIRECT/ROP
# MAGIC --   Effect: direct_demand qty_outstanding now nets PRO supply per-SO
# MAGIC --   Note: Per-SO grain preserved — netting happens at SO+line level
# MAGIC --
# MAGIC -- 🐛 BUG 2 FIX (NEW v2): Engine inventory not netted from PRO commitments
# MAGIC --   v2 fix:
# MAGIC --     1. NEW CTE active_pro_consumption (EXCLUDE archetype='SKIP')
# MAGIC --     2. NEW CTE atp_inventory (replaces available_inventory)
# MAGIC --     3. SELECT: shortage uses ATP + add audit columns
# MAGIC --   Note: ORDER items are mostly diamonds — Bug 2 patch is safety net
# MAGIC --
# MAGIC -- ⚠️ ONLY CELL 2 changes vs v1 — Cells 1, 3-7 unchanged
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 1: Setup (UNCHANGED) ==============
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS Gold_Inventory_Lakehouse.mrp;
# MAGIC 
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInWrite = CORRECTED;
# MAGIC SET spark.sql.parquet.int96RebaseModeInRead = CORRECTED;
# MAGIC SET spark.sql.parquet.int96RebaseModeInWrite = CORRECTED;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 2 (PATCHED v2): Build gold_planned_orders_order ==============
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.gold_planned_orders_order
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH 
# MAGIC -- ⭐ NEW v2: Active PRO net supply for Bug 1 (same pattern as MPS/MST_DIRECT)
# MAGIC active_pro_net_supply AS (
# MAGIC     SELECT 
# MAGIC         h.`Source No.`             AS item_no,
# MAGIC         h.`Sales Order No.`        AS so_no,
# MAGIC         h.`Sales Order Line No.`   AS so_line_no,
# MAGIC         SUM(h.Quantity)            AS active_pro_qty
# MAGIC     FROM (
# MAGIC         SELECT 
# MAGIC             *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `No.` 
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Production Order`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND Status IN ('Released', 'Firm Planned', 'Planned')
# MAGIC     ) h
# MAGIC     WHERE h.rn = 1
# MAGIC       AND h.`Sales Order No.` IS NOT NULL
# MAGIC       AND h.`Sales Order No.` <> ''
# MAGIC       AND h.`Sales Order Line No.` IS NOT NULL
# MAGIC       AND h.`Sales Order Line No.` > 0
# MAGIC       AND h.`Source No.` IS NOT NULL
# MAGIC       AND h.`Source No.` <> ''
# MAGIC     GROUP BY 
# MAGIC         h.`Source No.`,
# MAGIC         h.`Sales Order No.`,
# MAGIC         h.`Sales Order Line No.`
# MAGIC ),
# MAGIC 
# MAGIC -- === Step 1 (PATCHED v2): Direct SO demand with Bug 1 netting ===
# MAGIC direct_demand AS (
# MAGIC     SELECT 
# MAGIC         so.so_no                            AS source_so_no,
# MAGIC         so.so_line_no                       AS source_so_line,
# MAGIC         so.customer_no                      AS source_customer_no,
# MAGIC         so.customer_name                    AS source_customer_name,
# MAGIC         so.customer_tier_code               AS source_customer_tier,
# MAGIC         so.customer_priority_weight         AS source_priority_weight,
# MAGIC         so.is_dnd                           AS source_is_dnd,
# MAGIC         so.dnd_exceed_date                  AS source_dnd_exceed_date,
# MAGIC         so.ship_date                        AS source_ship_date,
# MAGIC         so.requested_ship_date              AS source_requested_ship_date,
# MAGIC         so.promised_ship_date               AS source_promised_ship_date,
# MAGIC         so.item_no,
# MAGIC         so.description                      AS item_description,
# MAGIC         so.item_category,
# MAGIC         -- ⭐ Bug 1: net PRO supply at SO+line grain
# MAGIC         GREATEST(
# MAGIC             so.qty_outstanding - COALESCE(apns.active_pro_qty, 0),
# MAGIC             0
# MAGIC         )                                   AS demand_qty,
# MAGIC         so.qty_outstanding                  AS demand_qty_gross,
# MAGIC         COALESCE(apns.active_pro_qty, 0)    AS active_pro_supply_qty,
# MAGIC         so.line_amount_thb                  AS line_amount_thb,
# MAGIC         'SO_DIRECT'                         AS source_demand_type,
# MAGIC         CAST(NULL AS STRING)                AS parent_item,
# MAGIC         CAST(NULL AS STRING)                AS bom_path
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_SO so
# MAGIC     LEFT JOIN active_pro_net_supply apns
# MAGIC         ON  apns.item_no    = so.item_no
# MAGIC        AND apns.so_no      = so.so_no
# MAGIC        AND apns.so_line_no = so.so_line_no
# MAGIC     WHERE so.is_open = TRUE
# MAGIC       AND so.item_archetype = 'ORDER'
# MAGIC       AND so.qty_outstanding > 0
# MAGIC       AND GREATEST(
# MAGIC             so.qty_outstanding - COALESCE(apns.active_pro_qty, 0), 0
# MAGIC           ) > 0
# MAGIC ),
# MAGIC 
# MAGIC -- === Step 2: Dependent demand from MPS BOM (UNCHANGED — already aware) ===
# MAGIC dependent_demand AS (
# MAGIC     SELECT 
# MAGIC         d.source_so_no,
# MAGIC         d.source_so_line,
# MAGIC         d.source_customer_no,
# MAGIC         CAST(NULL AS STRING)                AS source_customer_name,
# MAGIC         d.source_customer_tier,
# MAGIC         d.source_priority_weight,
# MAGIC         d.source_is_dnd,
# MAGIC         CAST(NULL AS DATE)                  AS source_dnd_exceed_date,
# MAGIC         d.parent_ship_date                  AS source_ship_date,
# MAGIC         CAST(NULL AS DATE)                  AS source_requested_ship_date,
# MAGIC         CAST(NULL AS DATE)                  AS source_promised_ship_date,
# MAGIC         d.component_item                    AS item_no,
# MAGIC         d.component_description             AS item_description,
# MAGIC         d.component_category                AS item_category,
# MAGIC         d.dependent_demand_qty              AS demand_qty,
# MAGIC         d.dependent_demand_qty              AS demand_qty_gross,
# MAGIC         CAST(0 AS DECIMAL(38,20))           AS active_pro_supply_qty,
# MAGIC         CAST(NULL AS DECIMAL(28,8))         AS line_amount_thb,
# MAGIC         'COMPONENT_FROM_MPS'                AS source_demand_type,
# MAGIC         d.parent_item                       AS parent_item,
# MAGIC         d.bom_path                          AS bom_path
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_dependent_demand_from_mps d
# MAGIC     WHERE d.component_archetype = 'ORDER'
# MAGIC       AND d.dependent_demand_qty > 0
# MAGIC ),
# MAGIC 
# MAGIC -- === Step 3: Combine — preserve per-SO grain (NO aggregation) ===
# MAGIC all_demand AS (
# MAGIC     SELECT * FROM direct_demand
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM dependent_demand
# MAGIC ),
# MAGIC 
# MAGIC -- === Step 4: Item master enrichment (UNCHANGED) ===
# MAGIC item_master_order AS (
# MAGIC     SELECT 
# MAGIC         item_no, scrap_pct,
# MAGIC         min_order_qty, max_order_qty, order_multiple,
# MAGIC         base_lead_days_static, safety_lead_days_static, effective_lead_days,
# MAGIC         item_category, archetype,
# MAGIC         vendor_no, purch_uom, base_uom,
# MAGIC         tracking_policy
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Item_Master
# MAGIC     WHERE archetype = 'ORDER' AND is_blocked = FALSE
# MAGIC ),
# MAGIC 
# MAGIC -- ⭐ NEW v2: Active PRO Component Consumption (Bug 2 fix)
# MAGIC active_pro_consumption AS (
# MAGIC     SELECT 
# MAGIC         c.`Item No.`                AS item_no,
# MAGIC         SUM(c.`Remaining Quantity`) AS pro_committed_qty
# MAGIC     FROM (
# MAGIC         SELECT 
# MAGIC             *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `Prod. Order No.`, `Prod. Order Line No.`, `Line No.`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Prod Order Component`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND Status IN ('Released', 'Firm Planned', 'Planned')
# MAGIC           AND `Remaining Quantity` > 0
# MAGIC     ) c
# MAGIC     INNER JOIN Gold_Inventory_Lakehouse.mrp.gold_Item_Master im 
# MAGIC             ON im.item_no = c.`Item No.`
# MAGIC     WHERE c.rn = 1
# MAGIC       AND im.archetype <> 'SKIP'
# MAGIC     GROUP BY c.`Item No.`
# MAGIC ),
# MAGIC 
# MAGIC -- ⭐ MODIFIED v2: ATP inventory (replaces available_inventory)
# MAGIC atp_inventory AS (
# MAGIC     SELECT 
# MAGIC         inv.item_no,
# MAGIC         SUM(inv.qty_available)                          AS qty_available_gross,
# MAGIC         COUNT(DISTINCT inv.lot_no)                      AS distinct_lots,
# MAGIC         COALESCE(MAX(apc.pro_committed_qty), 0)         AS pro_committed_qty,
# MAGIC         GREATEST(
# MAGIC             SUM(inv.qty_available) - COALESCE(MAX(apc.pro_committed_qty), 0),
# MAGIC             0
# MAGIC         )                                               AS qty_available_atp
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Inventory inv
# MAGIC     LEFT JOIN active_pro_consumption apc ON apc.item_no = inv.item_no
# MAGIC     WHERE inv.is_blocked_location = FALSE
# MAGIC       AND inv.is_expired = FALSE
# MAGIC     GROUP BY inv.item_no
# MAGIC )
# MAGIC 
# MAGIC -- === Final: Generate planned ORDER POs (1 row per SO line, NO aggregation) ===
# MAGIC SELECT 
# MAGIC     UUID() AS plan_id,
# MAGIC     
# MAGIC     -- MANDATORY traceability (per-SO grain)
# MAGIC     ad.source_so_no                     AS triggering_so_no,
# MAGIC     ad.source_so_line                   AS triggering_so_line,
# MAGIC     ad.source_customer_no               AS customer_no,
# MAGIC     ad.source_customer_name             AS customer_name,
# MAGIC     ad.source_customer_tier             AS customer_tier,
# MAGIC     ad.source_priority_weight           AS priority_weight,
# MAGIC     ad.source_is_dnd                    AS is_dnd,
# MAGIC     ad.source_dnd_exceed_date           AS dnd_exceed_date,
# MAGIC     
# MAGIC     -- Demand source type
# MAGIC     ad.source_demand_type,
# MAGIC     ad.parent_item,
# MAGIC     ad.bom_path,
# MAGIC     
# MAGIC     -- Item info
# MAGIC     ad.item_no,
# MAGIC     ad.item_description,
# MAGIC     ad.item_category,
# MAGIC     'ORDER'                             AS archetype,
# MAGIC     
# MAGIC     -- Sourcing
# MAGIC     im.vendor_no,
# MAGIC     im.purch_uom,
# MAGIC     im.base_uom,
# MAGIC     im.tracking_policy,
# MAGIC     
# MAGIC     -- Quantities (v2: Bug 1 + Bug 2 aware)
# MAGIC     ad.demand_qty,
# MAGIC     -- ⭐ v2: NEW audit columns (Bug 1)
# MAGIC     ad.demand_qty_gross,
# MAGIC     ad.active_pro_supply_qty,
# MAGIC     
# MAGIC     -- ⭐ v2: ATP-netted inventory (Bug 2)
# MAGIC     COALESCE(atp.qty_available_atp, 0)  AS onhand_available_qty,
# MAGIC     -- ⭐ v2: NEW audit columns (Bug 2)
# MAGIC     COALESCE(atp.qty_available_gross, 0) AS onhand_gross_qty,
# MAGIC     COALESCE(atp.pro_committed_qty, 0)   AS active_pro_committed_qty,
# MAGIC     
# MAGIC     COALESCE(atp.distinct_lots, 0)      AS available_distinct_lots,
# MAGIC     -- ⭐ v2: shortage uses ATP
# MAGIC     GREATEST(ad.demand_qty - COALESCE(atp.qty_available_atp, 0), 0) AS shortage_qty,
# MAGIC     COALESCE(im.scrap_pct, 0)           AS scrap_pct,
# MAGIC     
# MAGIC     -- ⭐ v2: Planned qty uses ATP
# MAGIC     GREATEST(
# MAGIC         COALESCE(im.min_order_qty, 0),
# MAGIC         ROUND(
# MAGIC             GREATEST(ad.demand_qty - COALESCE(atp.qty_available_atp, 0), 0) 
# MAGIC                 * (1 + COALESCE(im.scrap_pct, 0) / 100.0),
# MAGIC             4
# MAGIC         )
# MAGIC     ) AS planned_qty,
# MAGIC     
# MAGIC     -- Lot sizing context
# MAGIC     im.min_order_qty,
# MAGIC     im.max_order_qty,
# MAGIC     im.order_multiple,
# MAGIC     
# MAGIC     -- Lead time / scheduling
# MAGIC     im.base_lead_days_static            AS lead_time_days,
# MAGIC     im.safety_lead_days_static          AS safety_lead_days,
# MAGIC     im.effective_lead_days,
# MAGIC     ad.source_ship_date                 AS need_by_date,
# MAGIC     ad.source_requested_ship_date,
# MAGIC     ad.source_promised_ship_date,
# MAGIC     DATE_SUB(ad.source_ship_date, COALESCE(im.effective_lead_days, 0)) AS suggested_order_date,
# MAGIC     
# MAGIC     -- Pricing context
# MAGIC     ad.line_amount_thb,
# MAGIC     
# MAGIC     -- ⭐ v2: Plan status uses ATP
# MAGIC     CASE 
# MAGIC         WHEN im.vendor_no IS NULL 
# MAGIC             THEN 'BLOCKED_NO_VENDOR'
# MAGIC         WHEN im.tracking_policy = 'None'
# MAGIC             THEN 'BLOCKED_NO_TRACKING'
# MAGIC         WHEN ad.demand_qty - COALESCE(atp.qty_available_atp, 0) <= 0 
# MAGIC             THEN 'COVERED_BY_STOCK'
# MAGIC         WHEN ad.source_is_dnd = TRUE 
# MAGIC             THEN 'PROPOSED_DND'
# MAGIC         ELSE 'PROPOSED'
# MAGIC     END AS plan_status,
# MAGIC     
# MAGIC     CASE 
# MAGIC         WHEN im.vendor_no IS NULL 
# MAGIC             THEN 'ORDER item has no vendor on Item card — set in BC'
# MAGIC         WHEN im.tracking_policy = 'None'
# MAGIC             THEN 'ORDER item must use Lot or Serial tracking — set Item Tracking Code in BC'
# MAGIC         ELSE NULL
# MAGIC     END AS exception_reason,
# MAGIC     
# MAGIC     CURRENT_TIMESTAMP() AS plan_run_at
# MAGIC 
# MAGIC FROM all_demand ad
# MAGIC LEFT JOIN item_master_order im ON ad.item_no = im.item_no
# MAGIC LEFT JOIN atp_inventory atp ON ad.item_no = atp.item_no;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 3: Validation — plan status summary (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     plan_status,
# MAGIC     source_demand_type,
# MAGIC     COUNT(*) AS plan_count,
# MAGIC     COUNT(DISTINCT triggering_so_no) AS unique_so,
# MAGIC     COUNT(DISTINCT item_no) AS unique_items,
# MAGIC     ROUND(SUM(planned_qty), 2) AS total_planned_qty
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_order
# MAGIC GROUP BY plan_status, source_demand_type
# MAGIC ORDER BY plan_status, source_demand_type;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 4: ORDER plans by customer tier (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     customer_tier,
# MAGIC     COUNT(*) AS plan_count,
# MAGIC     COUNT(DISTINCT triggering_so_no) AS unique_so,
# MAGIC     COUNT(DISTINCT item_no) AS unique_diamonds,
# MAGIC     ROUND(SUM(planned_qty), 2) AS total_planned_qty,
# MAGIC     ROUND(SUM(line_amount_thb), 2) AS total_amount_thb
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_order
# MAGIC WHERE plan_status IN ('PROPOSED', 'PROPOSED_DND')
# MAGIC GROUP BY customer_tier
# MAGIC ORDER BY total_amount_thb DESC NULLS LAST;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 5: Top 20 ORDER plans by priority (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     triggering_so_no,
# MAGIC     triggering_so_line,
# MAGIC     customer_no,
# MAGIC     customer_tier,
# MAGIC     priority_weight,
# MAGIC     is_dnd,
# MAGIC     item_no,
# MAGIC     item_description,
# MAGIC     source_demand_type,
# MAGIC     parent_item,
# MAGIC     demand_qty,
# MAGIC     onhand_available_qty,
# MAGIC     shortage_qty,
# MAGIC     planned_qty,
# MAGIC     vendor_no,
# MAGIC     tracking_policy,
# MAGIC     need_by_date,
# MAGIC     suggested_order_date,
# MAGIC     plan_status
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_order
# MAGIC WHERE plan_status IN ('PROPOSED', 'PROPOSED_DND')
# MAGIC ORDER BY 
# MAGIC     is_dnd DESC,
# MAGIC     priority_weight DESC,
# MAGIC     need_by_date ASC,
# MAGIC     line_amount_thb DESC NULLS LAST
# MAGIC LIMIT 20;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 6: Per-SO grain validation (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     'Distinct triggering SO+line combinations' AS metric,
# MAGIC     COUNT(DISTINCT CONCAT(triggering_so_no, '-', triggering_so_line)) AS value
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_order
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Total plan rows',
# MAGIC     COUNT(*)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_order
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Plans from SO_DIRECT',
# MAGIC     COUNT(*)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_order
# MAGIC WHERE source_demand_type = 'SO_DIRECT'
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Plans from COMPONENT_FROM_MPS',
# MAGIC     COUNT(*)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_order
# MAGIC WHERE source_demand_type = 'COMPONENT_FROM_MPS';
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 7: Exception report (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     item_no,
# MAGIC     item_description,
# MAGIC     plan_status,
# MAGIC     exception_reason,
# MAGIC     triggering_so_no,
# MAGIC     customer_tier,
# MAGIC     demand_qty
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_order
# MAGIC WHERE plan_status LIKE 'BLOCKED%'
# MAGIC ORDER BY priority_weight DESC NULLS LAST, demand_qty DESC
# MAGIC LIMIT 30;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 8 (NEW v2): Bug 1 + Bug 2 validation ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     'order_v2_validate'                            AS check_name,
# MAGIC     COUNT(*)                                       AS total_plans,
# MAGIC     SUM(active_pro_supply_qty)                     AS total_pro_output_supply,  -- Bug 1
# MAGIC     SUM(active_pro_committed_qty)                  AS total_pro_consumption,    -- Bug 2
# MAGIC     SUM(planned_qty)                               AS total_planned,
# MAGIC     SUM(CASE WHEN plan_status = 'COVERED_BY_STOCK' THEN 1 ELSE 0 END) AS covered_count,
# MAGIC     SUM(CASE WHEN plan_status LIKE '%PROPOSED%' THEN 1 ELSE 0 END)    AS proposed_count,
# MAGIC     MAX(plan_run_at)                               AS last_run
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_order;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # nb_gold_planned_orders_mst_direct  (Phase 4 — Bug Fix #1)

# CELL ********************

# MAGIC %%sql
# MAGIC -- =====================================================================
# MAGIC -- Notebook: nb_gold_planned_orders_mst_direct  (PATCHED v4 — Bug 1 + Bug 2 + Bug 3)
# MAGIC -- Layer:    Gold MRP Engine — Coverage Extension
# MAGIC -- Output:   Gold_Inventory_Lakehouse.mrp.gold_planned_orders_mst_direct
# MAGIC --
# MAGIC -- 🐛 BUG 3 FIX (NEW v4, deployed 2026-05-06): Finished PRO supply netting
# MAGIC --   Same pattern as MPS v4 — see nb_gold_planned_orders_mps_v4 for full details
# MAGIC --   Captures Master Sample SO lines ที่ PRO ผลิตเสร็จแล้ว แต่ Sales ยังไม่ posted shipment
# MAGIC --
# MAGIC --
# MAGIC -- Purpose:
# MAGIC --   Captures 752 SO lines (~757 unique items) ที่:
# MAGIC --     - item_archetype = 'SKIP' (master molds normally not planned)
# MAGIC --     - item_category = 'MST' (Master molds)
# MAGIC --     - มี SO เปิดอยู่
# MAGIC -- 
# MAGIC -- Plan archetype: 'MST_DIRECT' (sub-archetype ของ SKIP)
# MAGIC --
# MAGIC -- 🐛 BUG 1 FIX (deployed in v2, retained in v3):
# MAGIC --   v2 fix: active_pro_net_supply CTE — same pattern as MPS
# MAGIC --
# MAGIC -- 🐛 BUG 2 FIX (NEW in v3): Engine inventory not netted
# MAGIC --   v3 fix:
# MAGIC --     1. NEW CTE active_pro_consumption (EXCLUDE SKIP)
# MAGIC --     2. MODIFIED CTE available_inventory → atp_inventory
# MAGIC --     3. SELECT: replace inv reference + add audit columns
# MAGIC -- =====================================================================
# MAGIC -- ⚠️ v4 changes vs v3: ONLY CELL 2 (mst_so_demand) + new finished_pro_net_supply CTE
# MAGIC --   Cells 1, 3-6 unchanged from v3
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 1: Setup (UNCHANGED) ==============
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS Gold_Inventory_Lakehouse.mrp;
# MAGIC 
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInWrite = CORRECTED;
# MAGIC SET spark.sql.parquet.int96RebaseModeInRead = CORRECTED;
# MAGIC SET spark.sql.parquet.int96RebaseModeInWrite = CORRECTED;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 2 (PATCHED v3): Build gold_planned_orders_mst_direct ==============
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.gold_planned_orders_mst_direct
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH 
# MAGIC -- ⭐ v2 (KEPT): Active PRO net supply for Bug 1
# MAGIC active_pro_net_supply AS (
# MAGIC     SELECT 
# MAGIC         h.`Source No.`             AS item_no,
# MAGIC         h.`Sales Order No.`        AS so_no,
# MAGIC         h.`Sales Order Line No.`   AS so_line_no,
# MAGIC         SUM(h.Quantity)            AS active_pro_qty
# MAGIC     FROM (
# MAGIC         SELECT 
# MAGIC             *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `No.` 
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Production Order`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND Status IN ('Released', 'Firm Planned', 'Planned')
# MAGIC     ) h
# MAGIC     WHERE h.rn = 1
# MAGIC       AND h.`Sales Order No.` IS NOT NULL
# MAGIC       AND h.`Sales Order No.` <> ''
# MAGIC       AND h.`Sales Order Line No.` IS NOT NULL
# MAGIC       AND h.`Sales Order Line No.` > 0
# MAGIC       AND h.`Source No.` IS NOT NULL
# MAGIC       AND h.`Source No.` <> ''
# MAGIC     GROUP BY 
# MAGIC         h.`Source No.`,
# MAGIC         h.`Sales Order No.`,
# MAGIC         h.`Sales Order Line No.`
# MAGIC ),
# MAGIC 
# MAGIC -- ⭐ NEW v4: Finished PRO net supply for Bug 3 (SO demand netting against Finished)
# MAGIC finished_pro_net_supply AS (
# MAGIC     SELECT 
# MAGIC         h.`Source No.`             AS item_no,
# MAGIC         h.`Sales Order No.`        AS so_no,
# MAGIC         h.`Sales Order Line No.`   AS so_line_no,
# MAGIC         SUM(h.Quantity)            AS finished_pro_qty
# MAGIC     FROM (
# MAGIC         SELECT 
# MAGIC             *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `No.` 
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Production Order`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND Status = 'Finished'
# MAGIC     ) h
# MAGIC     WHERE h.rn = 1
# MAGIC       AND h.`Sales Order No.` IS NOT NULL
# MAGIC       AND h.`Sales Order No.` <> ''
# MAGIC       AND h.`Sales Order Line No.` IS NOT NULL
# MAGIC       AND h.`Sales Order Line No.` > 0
# MAGIC       AND h.`Source No.` IS NOT NULL
# MAGIC       AND h.`Source No.` <> ''
# MAGIC     GROUP BY 
# MAGIC         h.`Source No.`,
# MAGIC         h.`Sales Order No.`,
# MAGIC         h.`Sales Order Line No.`
# MAGIC ),
# MAGIC 
# MAGIC -- ⭐ v4 (UPDATED): MST SO demand with Active + Finished PRO netting (Bug 1 + Bug 3)
# MAGIC mst_so_demand AS (
# MAGIC     SELECT 
# MAGIC         so.so_no, so.so_line_no,
# MAGIC         so.customer_no, so.customer_name, so.customer_tier_code,
# MAGIC         so.customer_priority_weight,
# MAGIC         so.is_dnd, so.dnd_exceed_date,
# MAGIC         so.ship_date, so.days_until_ship,
# MAGIC         so.requested_ship_date, so.promised_ship_date,
# MAGIC         so.item_no, so.description, so.item_category,
# MAGIC         -- ⭐ v4: subtract BOTH Active and Finished PRO supply
# MAGIC         GREATEST(
# MAGIC             so.qty_outstanding 
# MAGIC                 - COALESCE(apns.active_pro_qty, 0)
# MAGIC                 - COALESCE(fpns.finished_pro_qty, 0),
# MAGIC             0
# MAGIC         )                                AS qty_outstanding,
# MAGIC         so.qty_outstanding               AS qty_outstanding_gross,
# MAGIC         COALESCE(apns.active_pro_qty, 0) AS active_pro_supply_qty,
# MAGIC         -- ⭐ v4: NEW audit column for Bug 3
# MAGIC         COALESCE(fpns.finished_pro_qty, 0) AS finished_pro_supply_qty,
# MAGIC         so.uom, so.line_amount_thb
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_SO so
# MAGIC     LEFT JOIN active_pro_net_supply apns
# MAGIC         ON  apns.item_no    = so.item_no
# MAGIC        AND apns.so_no      = so.so_no
# MAGIC        AND apns.so_line_no = so.so_line_no
# MAGIC     LEFT JOIN finished_pro_net_supply fpns
# MAGIC         ON  fpns.item_no   = so.item_no
# MAGIC        AND fpns.so_no      = so.so_no
# MAGIC        AND fpns.so_line_no = so.so_line_no
# MAGIC     WHERE so.is_open = TRUE
# MAGIC       AND so.item_archetype = 'SKIP'
# MAGIC       AND so.item_category = 'MST'
# MAGIC       AND so.qty_outstanding > 0
# MAGIC       AND GREATEST(
# MAGIC             so.qty_outstanding 
# MAGIC                 - COALESCE(apns.active_pro_qty, 0)
# MAGIC                 - COALESCE(fpns.finished_pro_qty, 0),
# MAGIC             0
# MAGIC           ) > 0
# MAGIC ),
# MAGIC 
# MAGIC -- ⭐ NEW v3: Active PRO Component Consumption (Bug 2 fix)
# MAGIC active_pro_consumption AS (
# MAGIC     SELECT 
# MAGIC         c.`Item No.`                AS item_no,
# MAGIC         SUM(c.`Remaining Quantity`) AS pro_committed_qty
# MAGIC     FROM (
# MAGIC         SELECT 
# MAGIC             *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `Prod. Order No.`, `Prod. Order Line No.`, `Line No.`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Prod Order Component`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND Status IN ('Released', 'Firm Planned', 'Planned')
# MAGIC           AND `Remaining Quantity` > 0
# MAGIC     ) c
# MAGIC     INNER JOIN Gold_Inventory_Lakehouse.mrp.gold_Item_Master im 
# MAGIC             ON im.item_no = c.`Item No.`
# MAGIC     WHERE c.rn = 1
# MAGIC       AND im.archetype <> 'SKIP'
# MAGIC     GROUP BY c.`Item No.`
# MAGIC ),
# MAGIC 
# MAGIC -- ⭐ MODIFIED v3: ATP inventory (replaces available_inventory)
# MAGIC atp_inventory AS (
# MAGIC     SELECT 
# MAGIC         inv.item_no,
# MAGIC         SUM(inv.qty_available)                          AS qty_available_gross,
# MAGIC         COALESCE(MAX(apc.pro_committed_qty), 0)         AS pro_committed_qty,
# MAGIC         GREATEST(
# MAGIC             SUM(inv.qty_available) - COALESCE(MAX(apc.pro_committed_qty), 0),
# MAGIC             0
# MAGIC         )                                               AS qty_available_atp
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Inventory inv
# MAGIC     LEFT JOIN active_pro_consumption apc ON apc.item_no = inv.item_no
# MAGIC     WHERE inv.is_blocked_location = FALSE
# MAGIC       AND inv.is_expired = FALSE
# MAGIC     GROUP BY inv.item_no
# MAGIC ),
# MAGIC 
# MAGIC item_master_mst AS (
# MAGIC     SELECT 
# MAGIC         item_no, scrap_pct,
# MAGIC         min_order_qty, max_order_qty, order_multiple,
# MAGIC         base_lead_days_static, safety_lead_days_static, effective_lead_days,
# MAGIC         item_category, archetype
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Item_Master
# MAGIC     WHERE archetype = 'SKIP' AND item_category = 'MST' AND is_blocked = FALSE
# MAGIC )
# MAGIC 
# MAGIC SELECT 
# MAGIC     UUID() AS plan_id,
# MAGIC     
# MAGIC     d.so_no                         AS triggering_so_no,
# MAGIC     d.so_line_no                    AS triggering_so_line,
# MAGIC     d.customer_no, d.customer_name,
# MAGIC     d.customer_tier_code            AS customer_tier,
# MAGIC     d.customer_priority_weight      AS priority_weight,
# MAGIC     d.is_dnd, d.dnd_exceed_date,
# MAGIC     d.ship_date, d.days_until_ship,
# MAGIC     d.requested_ship_date, d.promised_ship_date,
# MAGIC     
# MAGIC     d.item_no, d.description, d.item_category,
# MAGIC     'MST_DIRECT' AS archetype,
# MAGIC     d.uom,
# MAGIC     
# MAGIC     -- v2: net qty (Bug 1)
# MAGIC     d.qty_outstanding               AS so_outstanding_qty,
# MAGIC     d.qty_outstanding_gross         AS so_outstanding_qty_gross,
# MAGIC     d.active_pro_supply_qty         AS active_pro_supply_qty,
# MAGIC     
# MAGIC     -- ⭐ v3: ATP-netted inventory (Bug 2)
# MAGIC     COALESCE(atp.qty_available_atp, 0)   AS onhand_available_qty,
# MAGIC     
# MAGIC     -- ⭐ v3: NEW audit columns (Bug 2)
# MAGIC     COALESCE(atp.qty_available_gross, 0) AS onhand_gross_qty,
# MAGIC     COALESCE(atp.pro_committed_qty, 0)   AS active_pro_committed_qty,
# MAGIC     
# MAGIC     -- ⭐ v3: shortage uses ATP
# MAGIC     GREATEST(d.qty_outstanding - COALESCE(atp.qty_available_atp, 0), 0) AS shortage_qty,
# MAGIC     COALESCE(im.scrap_pct, 0) AS scrap_pct,
# MAGIC     ROUND(
# MAGIC         GREATEST(d.qty_outstanding - COALESCE(atp.qty_available_atp, 0), 0)
# MAGIC             * (1 + COALESCE(im.scrap_pct, 0) / 100.0),
# MAGIC         4
# MAGIC     ) AS planned_qty,
# MAGIC     
# MAGIC     im.min_order_qty, im.max_order_qty, im.order_multiple,
# MAGIC     im.base_lead_days_static  AS lead_time_days,
# MAGIC     im.safety_lead_days_static AS safety_lead_days,
# MAGIC     im.effective_lead_days,
# MAGIC     
# MAGIC     d.line_amount_thb,
# MAGIC     
# MAGIC     DATE_SUB(d.ship_date, COALESCE(im.effective_lead_days, 14)) AS suggested_order_date,
# MAGIC     DATEDIFF(d.ship_date, CURRENT_DATE())                       AS days_until_need_by,
# MAGIC     DATEDIFF(CURRENT_DATE(), d.ship_date)                       AS days_past_need_by,
# MAGIC     
# MAGIC     CASE 
# MAGIC         WHEN d.ship_date IS NULL OR d.ship_date <= DATE'1900-01-01' THEN FALSE
# MAGIC         WHEN DATE_SUB(d.ship_date, COALESCE(im.effective_lead_days, 14)) < CURRENT_DATE() THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_order_date_overdue,
# MAGIC     
# MAGIC     CASE 
# MAGIC         WHEN d.ship_date IS NULL OR d.ship_date <= DATE'1900-01-01' THEN FALSE
# MAGIC         WHEN d.ship_date < CURRENT_DATE() THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_need_date_overdue,
# MAGIC     
# MAGIC     -- ⭐ v3: plan_status uses ATP-netted
# MAGIC     CASE 
# MAGIC         WHEN d.qty_outstanding - COALESCE(atp.qty_available_atp, 0) <= 0 
# MAGIC             THEN 'COVERED_BY_STOCK'
# MAGIC         
# MAGIC         WHEN d.ship_date < CURRENT_DATE() AND d.is_dnd = TRUE 
# MAGIC             AND d.ship_date > DATE'1900-01-01'
# MAGIC             THEN 'CRITICAL_LATE_DND'
# MAGIC         WHEN d.ship_date < CURRENT_DATE() 
# MAGIC             AND d.ship_date > DATE'1900-01-01'
# MAGIC             THEN 'CRITICAL_LATE'
# MAGIC         
# MAGIC         WHEN DATE_SUB(d.ship_date, COALESCE(im.effective_lead_days, 14)) < CURRENT_DATE() 
# MAGIC             AND d.is_dnd = TRUE
# MAGIC             AND d.ship_date > DATE'1900-01-01'
# MAGIC             THEN 'OVERDUE_PROPOSED_DND'
# MAGIC         WHEN DATE_SUB(d.ship_date, COALESCE(im.effective_lead_days, 14)) < CURRENT_DATE() 
# MAGIC             AND d.ship_date > DATE'1900-01-01'
# MAGIC             THEN 'OVERDUE_PROPOSED'
# MAGIC         
# MAGIC         WHEN d.is_dnd = TRUE 
# MAGIC             THEN 'PROPOSED_DND'
# MAGIC         ELSE 'PROPOSED'
# MAGIC     END AS plan_status,
# MAGIC     
# MAGIC     NULL AS exception_reason,
# MAGIC     
# MAGIC     CURRENT_TIMESTAMP() AS plan_run_at
# MAGIC 
# MAGIC FROM mst_so_demand d
# MAGIC LEFT JOIN atp_inventory atp     ON d.item_no = atp.item_no
# MAGIC LEFT JOIN item_master_mst im    ON d.item_no = im.item_no;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 3: Validation — plan status summary (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     plan_status,
# MAGIC     COUNT(*) AS plan_count,
# MAGIC     COUNT(DISTINCT item_no) AS unique_items,
# MAGIC     COUNT(DISTINCT customer_no) AS unique_customers,
# MAGIC     ROUND(SUM(planned_qty), 2) AS total_planned_qty,
# MAGIC     ROUND(SUM(line_amount_thb), 2) AS total_amount_thb
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_mst_direct
# MAGIC GROUP BY plan_status
# MAGIC ORDER BY plan_count DESC;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 4: MST plans by customer tier (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     customer_tier,
# MAGIC     plan_status,
# MAGIC     COUNT(*) AS plan_count,
# MAGIC     COUNT(DISTINCT item_no) AS unique_items,
# MAGIC     ROUND(SUM(planned_qty), 2) AS total_planned_qty
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_mst_direct
# MAGIC WHERE plan_status NOT IN ('COVERED_BY_STOCK')
# MAGIC GROUP BY customer_tier, plan_status
# MAGIC ORDER BY customer_tier, plan_status;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 5: Top 30 most urgent MST plans (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     item_no, description,
# MAGIC     customer_tier, priority_weight, is_dnd,
# MAGIC     so_outstanding_qty, onhand_available_qty, shortage_qty, planned_qty,
# MAGIC     ship_date, days_until_need_by, days_past_need_by,
# MAGIC     suggested_order_date,
# MAGIC     is_order_date_overdue, is_need_date_overdue,
# MAGIC     plan_status
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_mst_direct
# MAGIC WHERE plan_status NOT IN ('COVERED_BY_STOCK')
# MAGIC ORDER BY 
# MAGIC     CASE plan_status
# MAGIC         WHEN 'CRITICAL_LATE_DND' THEN 1
# MAGIC         WHEN 'CRITICAL_LATE' THEN 2
# MAGIC         WHEN 'OVERDUE_PROPOSED_DND' THEN 3
# MAGIC         WHEN 'OVERDUE_PROPOSED' THEN 4
# MAGIC         WHEN 'PROPOSED_DND' THEN 5
# MAGIC         ELSE 6
# MAGIC     END,
# MAGIC     days_past_need_by DESC NULLS LAST,
# MAGIC     priority_weight DESC,
# MAGIC     days_until_need_by ASC NULLS LAST
# MAGIC LIMIT 30;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 6: Coverage check vs original SKIP gap (UNCHANGED) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     'Total open MST SOs (gap to fix)' AS metric,
# MAGIC     COUNT(*) AS value
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_SO
# MAGIC WHERE is_open = TRUE
# MAGIC   AND item_archetype = 'SKIP'
# MAGIC   AND item_category = 'MST'
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'MST plans created (should match)',
# MAGIC     COUNT(*)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_mst_direct
# MAGIC 
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Unique MST items planned',
# MAGIC     COUNT(DISTINCT item_no)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_mst_direct;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # UNIFIED setup (Python)

# CELL ********************

# =====================================================================
# 📦 fabric_requisition_line v9.4 — Cell 1: Python Setup
# =====================================================================
# Build on:  v9.3 (deployed 2026-05-04 01:07Z)
# 
# 🟢 NEW v9.4: Backward scheduling PO date with module split
#   - Module COMPONENT  (diamond/gemstone/pearl/finding) → semi_start - lead_time
#   - Module METAL_ALLOY (mixed metal/casting/mst)       → wax_start - lead_time
#   - Working days using silver_holiday_calendar
#   - Clamp wax_start, semi_start ≥ today
#   - PO date allowed to go negative (stock-out signal)
#
# 🔴 BREAKING: schema adds 1 column 'module', changes suggested_order_date logic
#    Rollback table: fabric_requisition_line_v93_archive (drop after 14 days)
# =====================================================================

from pyspark.sql.types import DateType, IntegerType
from datetime import date, timedelta

# =====================================================
# Step 1: Load holiday calendar (broadcast)
# =====================================================
holidays_df = spark.sql("""
    SELECT DISTINCT CAST(holiday_date AS DATE) AS holiday_date
    FROM Silver_Commons_Lakehouse.cmn.silver_holiday_calendar
    WHERE holiday_date BETWEEN DATE'2025-01-01' AND DATE'2027-12-31'
""")

holiday_set = set(row['holiday_date'] for row in holidays_df.collect())
holiday_bc = spark.sparkContext.broadcast(holiday_set)
print(f"Loaded {len(holiday_set)} holidays")

# =====================================================
# Step 2: Working day helper functions
# =====================================================
def is_working_day(d):
    if d is None:
        return False
    return d.weekday() < 5 and d not in holiday_bc.value

def count_working_days(start, end):
    if start is None or end is None or end <= start:
        return 0
    count = 0
    d = start + timedelta(days=1)
    while d <= end:
        if is_working_day(d):
            count += 1
        d += timedelta(days=1)
    return count

def subtract_working_days(from_date, n):
    if from_date is None or n is None or n < 0:
        return from_date
    d = from_date
    while n > 0:
        d -= timedelta(days=1)
        if is_working_day(d):
            n -= 1
    return d

def prev_working_day(from_date):
    if from_date is None:
        return None
    d = from_date - timedelta(days=1)
    while not is_working_day(d):
        d -= timedelta(days=1)
    return d

# =====================================================
# Step 3: Stage calculators
# =====================================================
def calc_wax_start(ship_date, today):
    """Wax-WH Start (earliest stage), clamped to today"""
    if ship_date is None or today is None or ship_date <= today:
        return today
    awd = count_working_days(today, ship_date)
    if awd <= 0:
        return today
    days_fg   = max(1, round(0.40 * awd))
    days_semi = max(1, round(0.30 * awd))
    days_wax  = max(1, awd - days_fg - days_semi)
    fg_end     = ship_date
    fg_start   = subtract_working_days(fg_end, days_fg - 1)
    semi_end   = prev_working_day(fg_start)
    semi_start = subtract_working_days(semi_end, days_semi - 1)
    wax_end    = prev_working_day(semi_start)
    wax_start  = subtract_working_days(wax_end, days_wax - 1)
    if wax_start < today:
        wax_start = today
    return wax_start

def calc_semi_start(ship_date, today):
    """Semi Start (skipping wax), clamped to today"""
    if ship_date is None or today is None or ship_date <= today:
        return today
    awd = count_working_days(today, ship_date)
    if awd <= 0:
        return today
    days_fg   = max(1, round(0.40 * awd))
    days_semi = max(1, round(0.30 * awd))
    fg_end     = ship_date
    fg_start   = subtract_working_days(fg_end, days_fg - 1)
    semi_end   = prev_working_day(fg_start)
    semi_start = subtract_working_days(semi_end, days_semi - 1)
    if semi_start < today:
        semi_start = today
    return semi_start

# =====================================================
# Step 4: Register UDFs
# =====================================================
spark.udf.register("calc_wax_start",      calc_wax_start,         DateType())
spark.udf.register("calc_semi_start",     calc_semi_start,        DateType())
spark.udf.register("sub_working_days",    subtract_working_days,  DateType())
spark.udf.register("count_working_days",  count_working_days,     IntegerType())

print("✅ UDFs registered: calc_wax_start, calc_semi_start, sub_working_days, count_working_days")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # nb_gold_planned_orders_unified  (Phase 4 — Bug Fix #2: OVERDUE)

# CELL ********************

# MAGIC %%sql
# MAGIC -- =====================================================================
# MAGIC -- STANDALONE: Unified UNION Refresh — v6 (datetime rebase fix)
# MAGIC -- 
# MAGIC -- Schema confirmed via DESCRIBE TABLE on all 6 archetype tables 2026-04-29
# MAGIC -- v6: added datetime rebase mode SETs (CORRECTED) to handle ancient dates
# MAGIC -- 
# MAGIC -- Output: Gold_Inventory_Lakehouse.mrp.gold_planned_orders_unified
# MAGIC -- Run after L4L v2 deployed.
# MAGIC -- ระยะเวลา: ~30 วินาที
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC -- Datetime rebase configuration (required for ancient dates < 1582-10-15)
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInWrite = CORRECTED;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC SET spark.sql.parquet.int96RebaseModeInWrite = CORRECTED;
# MAGIC SET spark.sql.parquet.int96RebaseModeInRead = CORRECTED;
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.gold_planned_orders_unified
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH 
# MAGIC 
# MAGIC -- ============== MPS ==============
# MAGIC -- MPS uses: triggering_so_no, description, so_outstanding_qty, ship_date
# MAGIC -- MPS has no suggested_order_date column → calc inline
# MAGIC mps_plans AS (
# MAGIC     SELECT 
# MAGIC         plan_id,
# MAGIC         'MPS' AS archetype,
# MAGIC         triggering_so_no AS source_so_no,
# MAGIC         customer_tier,
# MAGIC         priority_weight,
# MAGIC         is_dnd,
# MAGIC         item_no,
# MAGIC         description,
# MAGIC         item_category,
# MAGIC         CAST(so_outstanding_qty AS DECIMAL(38,20))   AS demand_qty,
# MAGIC         CAST(onhand_available_qty AS DECIMAL(38,20)) AS onhand_available_qty,
# MAGIC         CAST(shortage_qty AS DECIMAL(38,19))         AS shortage_qty,
# MAGIC         CAST(planned_qty AS DECIMAL(38,20))          AS planned_qty,
# MAGIC         ship_date AS need_by_date,
# MAGIC         DATE_SUB(ship_date, COALESCE(effective_lead_days, 0)) AS suggested_order_date,
# MAGIC         plan_status,
# MAGIC         exception_reason
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_mps
# MAGIC ),
# MAGIC 
# MAGIC -- ============== ALLOY ==============
# MAGIC -- ALLOY uses: triggering_so_no, item_description, demand_qty, need_by_date, suggested_start_date
# MAGIC alloy_plans AS (
# MAGIC     SELECT 
# MAGIC         plan_id,
# MAGIC         archetype,
# MAGIC         triggering_so_no AS source_so_no,
# MAGIC         customer_tier,
# MAGIC         priority_weight,
# MAGIC         is_dnd,
# MAGIC         item_no,
# MAGIC         item_description AS description,
# MAGIC         item_category,
# MAGIC         CAST(demand_qty AS DECIMAL(38,20))           AS demand_qty,
# MAGIC         CAST(onhand_available_qty AS DECIMAL(38,20)) AS onhand_available_qty,
# MAGIC         CAST(shortage_qty AS DECIMAL(38,19))         AS shortage_qty,
# MAGIC         CAST(planned_qty AS DECIMAL(38,20))          AS planned_qty,
# MAGIC         need_by_date,
# MAGIC         suggested_start_date AS suggested_order_date,
# MAGIC         plan_status,
# MAGIC         exception_reason
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_alloy
# MAGIC ),
# MAGIC 
# MAGIC -- ============== L4L ==============
# MAGIC -- L4L uses: triggering_so_no, item_description, demand_qty, earliest_ship_date, suggested_order_date
# MAGIC l4l_plans AS (
# MAGIC     SELECT 
# MAGIC         plan_id,
# MAGIC         archetype,
# MAGIC         triggering_so_no AS source_so_no,
# MAGIC         customer_tier,
# MAGIC         priority_weight,
# MAGIC         is_dnd,
# MAGIC         item_no,
# MAGIC         item_description AS description,
# MAGIC         item_category,
# MAGIC         CAST(demand_qty AS DECIMAL(38,20))           AS demand_qty,
# MAGIC         CAST(onhand_available_qty AS DECIMAL(38,20)) AS onhand_available_qty,
# MAGIC         CAST(shortage_qty AS DECIMAL(38,19))         AS shortage_qty,
# MAGIC         CAST(planned_qty AS DECIMAL(38,20))          AS planned_qty,
# MAGIC         earliest_ship_date AS need_by_date,
# MAGIC         suggested_order_date,
# MAGIC         plan_status,
# MAGIC         exception_reason
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_l4l
# MAGIC ),
# MAGIC 
# MAGIC -- ============== ROP ==============
# MAGIC -- ROP has no triggering_so_no, no customer_tier directly
# MAGIC rop_plans AS (
# MAGIC     SELECT 
# MAGIC         plan_id,
# MAGIC         archetype,
# MAGIC         CAST(NULL AS STRING) AS source_so_no,
# MAGIC         CAST(priority_weight AS STRING) AS customer_tier,
# MAGIC         priority_weight,
# MAGIC         is_dnd,
# MAGIC         item_no,
# MAGIC         item_description AS description,
# MAGIC         item_category,
# MAGIC         CAST(demand_qty AS DECIMAL(38,20))           AS demand_qty,
# MAGIC         CAST(onhand_available_qty AS DECIMAL(38,20)) AS onhand_available_qty,
# MAGIC         CAST(shortage_qty AS DECIMAL(38,19))         AS shortage_qty,
# MAGIC         CAST(planned_qty AS DECIMAL(38,20))          AS planned_qty,
# MAGIC         need_by_date,
# MAGIC         suggested_order_date,
# MAGIC         plan_status,
# MAGIC         exception_reason
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_rop
# MAGIC ),
# MAGIC 
# MAGIC -- ============== ORDER ==============
# MAGIC -- ORDER uses: triggering_so_no, item_description, demand_qty, need_by_date, suggested_order_date
# MAGIC order_plans AS (
# MAGIC     SELECT 
# MAGIC         plan_id,
# MAGIC         archetype,
# MAGIC         triggering_so_no AS source_so_no,
# MAGIC         customer_tier,
# MAGIC         priority_weight,
# MAGIC         is_dnd,
# MAGIC         item_no,
# MAGIC         item_description AS description,
# MAGIC         item_category,
# MAGIC         CAST(demand_qty AS DECIMAL(38,20))           AS demand_qty,
# MAGIC         CAST(onhand_available_qty AS DECIMAL(38,20)) AS onhand_available_qty,
# MAGIC         CAST(shortage_qty AS DECIMAL(38,19))         AS shortage_qty,
# MAGIC         CAST(planned_qty AS DECIMAL(38,20))          AS planned_qty,
# MAGIC         need_by_date,
# MAGIC         suggested_order_date,
# MAGIC         plan_status,
# MAGIC         exception_reason
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_order
# MAGIC ),
# MAGIC 
# MAGIC -- ============== MST_DIRECT ==============
# MAGIC -- MST_DIRECT uses: triggering_so_no, description, so_outstanding_qty, ship_date, suggested_order_date
# MAGIC -- exception_reason is VOID type → don't reference; just use NULL literal
# MAGIC mst_plans AS (
# MAGIC     SELECT 
# MAGIC         plan_id,
# MAGIC         archetype,
# MAGIC         triggering_so_no AS source_so_no,
# MAGIC         customer_tier,
# MAGIC         priority_weight,
# MAGIC         is_dnd,
# MAGIC         item_no,
# MAGIC         description,
# MAGIC         item_category,
# MAGIC         CAST(so_outstanding_qty AS DECIMAL(38,20))   AS demand_qty,
# MAGIC         CAST(onhand_available_qty AS DECIMAL(38,20)) AS onhand_available_qty,
# MAGIC         CAST(shortage_qty AS DECIMAL(38,19))         AS shortage_qty,
# MAGIC         CAST(planned_qty AS DECIMAL(38,20))          AS planned_qty,
# MAGIC         ship_date AS need_by_date,
# MAGIC         suggested_order_date,
# MAGIC         plan_status,
# MAGIC         CAST(NULL AS STRING) AS exception_reason
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_mst_direct
# MAGIC ),
# MAGIC 
# MAGIC all_plans AS (
# MAGIC     SELECT * FROM mps_plans
# MAGIC     UNION ALL SELECT * FROM alloy_plans
# MAGIC     UNION ALL SELECT * FROM l4l_plans
# MAGIC     UNION ALL SELECT * FROM rop_plans
# MAGIC     UNION ALL SELECT * FROM order_plans
# MAGIC     UNION ALL SELECT * FROM mst_plans
# MAGIC )
# MAGIC 
# MAGIC SELECT 
# MAGIC     plan_id, archetype, source_so_no, customer_tier, priority_weight, is_dnd,
# MAGIC     item_no, description, item_category,
# MAGIC     demand_qty, onhand_available_qty, shortage_qty, planned_qty,
# MAGIC     need_by_date, suggested_order_date,
# MAGIC     
# MAGIC     DATEDIFF(CURRENT_DATE(), suggested_order_date) AS days_past_suggested_order,
# MAGIC     DATEDIFF(need_by_date, CURRENT_DATE())         AS days_until_need_by,
# MAGIC     DATEDIFF(CURRENT_DATE(), need_by_date)         AS days_past_need_by,
# MAGIC     
# MAGIC     CASE 
# MAGIC         WHEN suggested_order_date IS NULL OR suggested_order_date <= DATE'1900-01-01' THEN FALSE
# MAGIC         WHEN suggested_order_date < CURRENT_DATE() THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_order_date_overdue,
# MAGIC     
# MAGIC     CASE 
# MAGIC         WHEN need_by_date IS NULL OR need_by_date <= DATE'1900-01-01' THEN FALSE
# MAGIC         WHEN need_by_date < CURRENT_DATE() THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_need_date_overdue,
# MAGIC     
# MAGIC     plan_status AS original_plan_status,
# MAGIC     
# MAGIC     CASE 
# MAGIC         WHEN plan_status LIKE 'BLOCKED%' THEN plan_status
# MAGIC         WHEN plan_status = 'COVERED_BY_STOCK' THEN plan_status
# MAGIC         WHEN plan_status = 'NO_TRIGGER' THEN plan_status
# MAGIC         WHEN need_by_date IS NOT NULL AND need_by_date > DATE'1900-01-01'
# MAGIC              AND need_by_date < CURRENT_DATE() AND is_dnd = TRUE 
# MAGIC             THEN 'CRITICAL_LATE_DND'
# MAGIC         WHEN need_by_date IS NOT NULL AND need_by_date > DATE'1900-01-01'
# MAGIC              AND need_by_date < CURRENT_DATE() 
# MAGIC             THEN 'CRITICAL_LATE'
# MAGIC         WHEN suggested_order_date IS NOT NULL AND suggested_order_date > DATE'1900-01-01'
# MAGIC              AND suggested_order_date < CURRENT_DATE() AND is_dnd = TRUE 
# MAGIC             THEN 'OVERDUE_PROPOSED_DND'
# MAGIC         WHEN suggested_order_date IS NOT NULL AND suggested_order_date > DATE'1900-01-01'
# MAGIC              AND suggested_order_date < CURRENT_DATE() 
# MAGIC             THEN 'OVERDUE_PROPOSED'
# MAGIC         ELSE plan_status
# MAGIC     END AS plan_status,
# MAGIC     
# MAGIC     CASE 
# MAGIC         WHEN plan_status LIKE 'BLOCKED%' THEN 0
# MAGIC         WHEN plan_status = 'COVERED_BY_STOCK' OR plan_status = 'NO_TRIGGER' THEN 9
# MAGIC         WHEN need_by_date IS NOT NULL AND need_by_date > DATE'1900-01-01'
# MAGIC              AND need_by_date < CURRENT_DATE() AND is_dnd = TRUE THEN 1
# MAGIC         WHEN need_by_date IS NOT NULL AND need_by_date > DATE'1900-01-01'
# MAGIC              AND need_by_date < CURRENT_DATE() THEN 2
# MAGIC         WHEN suggested_order_date IS NOT NULL AND suggested_order_date > DATE'1900-01-01'
# MAGIC              AND suggested_order_date < CURRENT_DATE() AND is_dnd = TRUE THEN 3
# MAGIC         WHEN suggested_order_date IS NOT NULL AND suggested_order_date > DATE'1900-01-01'
# MAGIC              AND suggested_order_date < CURRENT_DATE() THEN 4
# MAGIC         WHEN is_dnd = TRUE THEN 5
# MAGIC         ELSE 6
# MAGIC     END AS escalation_level,
# MAGIC     
# MAGIC     exception_reason,
# MAGIC     CURRENT_TIMESTAMP() AS unified_at
# MAGIC     
# MAGIC FROM all_plans;
# MAGIC 
# MAGIC 
# MAGIC -- ============== Verify timestamp ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     'L4L' AS table_name, MAX(plan_run_at) AS last_built
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_l4l
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT 
# MAGIC     'UNIFIED' AS table_name, MAX(unified_at) AS last_built
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_unified;
# MAGIC 
# MAGIC -- Expected: UNIFIED.last_built > L4L.last_built
# MAGIC 
# MAGIC 
# MAGIC -- ============== Verify DI-RD-000362 canary ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     plan_status, item_no,
# MAGIC     demand_qty, onhand_available_qty, shortage_qty, planned_qty,
# MAGIC     need_by_date, suggested_order_date
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_unified
# MAGIC WHERE item_no = 'DI-RD-000362'
# MAGIC ORDER BY need_by_date;
# MAGIC 
# MAGIC -- Expected: plan_status NOT 'COVERED_BY_STOCK'
# MAGIC --           shortage_qty > 0, planned_qty > 0

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # DQ Report Export Queries v2 — Phase 4 Step 3

# CELL ********************

# MAGIC %%sql
# MAGIC -- =====================================================================
# MAGIC -- DQ Report Export Queries v3 — Phase 4 Step 3
# MAGIC -- 4 Sheets (Master Data sheet dropped — to be requested separately from MD team)
# MAGIC -- 
# MAGIC -- Workflow:
# MAGIC --   1. Run each query in Fabric Spark SQL notebook
# MAGIC --   2. Export each result → CSV with names:
# MAGIC --        sheet1_critical_late.csv
# MAGIC --        sheet2_procurement.csv
# MAGIC --        sheet3_engineering.csv
# MAGIC --        sheet4_summary.csv
# MAGIC --   3. Send all 4 CSVs to Claude → will generate DQ_Report.xlsx
# MAGIC -- 
# MAGIC -- Audience for each sheet:
# MAGIC --   Sheet 1 (Critical Late):     Production Manager
# MAGIC --   Sheet 2 (Procurement):       Procurement Lead
# MAGIC --   Sheet 3 (Engineering):       Engineering Manager
# MAGIC --   Sheet 4 (Summary):           Leadership / DPA
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC 
# MAGIC -- ============== Common settings (run before each query) ==============
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInWrite = CORRECTED;
# MAGIC SET spark.sql.parquet.int96RebaseModeInRead = CORRECTED;
# MAGIC SET spark.sql.parquet.int96RebaseModeInWrite = CORRECTED;
# MAGIC 
# MAGIC 
# MAGIC -- =====================================================================
# MAGIC -- SHEET 1: Critical Late Items  (For Production Manager)
# MAGIC -- Expected rows: ~1,761 (525 + 986 + 29 + 221)
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC SELECT 
# MAGIC     plan_status               AS `Plan Status`,
# MAGIC     archetype                 AS `Archetype`,
# MAGIC     customer_tier             AS `Customer Tier`,
# MAGIC     is_dnd                    AS `DND`,
# MAGIC     source_so_no              AS `Source SO`,
# MAGIC     item_no                   AS `Item No`,
# MAGIC     description               AS `Description`,
# MAGIC     item_category             AS `Category`,
# MAGIC     demand_qty                AS `Demand Qty`,
# MAGIC     onhand_available_qty      AS `On Hand`,
# MAGIC     shortage_qty              AS `Shortage`,
# MAGIC     planned_qty               AS `Planned Qty`,
# MAGIC     need_by_date              AS `Need By Date`,
# MAGIC     suggested_order_date      AS `Should Have Started`,
# MAGIC     days_past_suggested_order AS `Days Late (Order)`,
# MAGIC     days_past_need_by         AS `Days Past Need-By`
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_unified
# MAGIC WHERE escalation_level <= 4
# MAGIC   AND escalation_level > 0
# MAGIC ORDER BY 
# MAGIC     escalation_level,
# MAGIC     days_past_need_by DESC NULLS LAST,
# MAGIC     days_past_suggested_order DESC NULLS LAST,
# MAGIC     customer_tier;
# MAGIC 
# MAGIC 
# MAGIC -- =====================================================================
# MAGIC -- SHEET 2: Procurement Issues  (For Procurement Lead)
# MAGIC -- Expected rows: ~1,020 (658 + 62 + 300)
# MAGIC -- 
# MAGIC -- Filter: BLOCKED_NO_VENDOR, BLOCKED_NO_LEAD_TIME, BLOCKED_NO_ROP_PARAMS
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC SELECT 
# MAGIC     archetype                 AS `Archetype`,
# MAGIC     plan_status               AS `Issue Type`,
# MAGIC     item_no                   AS `Item No`,
# MAGIC     description               AS `Description`,
# MAGIC     item_category             AS `Category`,
# MAGIC     demand_qty                AS `Demand Qty`,
# MAGIC     onhand_available_qty      AS `On Hand`,
# MAGIC     customer_tier             AS `Top Customer Tier`,
# MAGIC     source_so_no              AS `Sample SO No`,
# MAGIC     need_by_date              AS `Need By Date`,
# MAGIC     exception_reason          AS `What's Missing`
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_unified
# MAGIC WHERE plan_status IN ('BLOCKED_NO_VENDOR', 'BLOCKED_NO_LEAD_TIME', 'BLOCKED_NO_ROP_PARAMS')
# MAGIC ORDER BY 
# MAGIC     plan_status,
# MAGIC     archetype,
# MAGIC     demand_qty DESC NULLS LAST;
# MAGIC 
# MAGIC 
# MAGIC -- =====================================================================
# MAGIC -- SHEET 3: Engineering Issues  (For Engineering Manager)
# MAGIC -- Expected rows: ~25 (BLOCKED_BOM_NOT_CERTIFIED)
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC SELECT 
# MAGIC     plan_status               AS `Issue Type`,
# MAGIC     customer_tier             AS `Customer`,
# MAGIC     is_dnd                    AS `DND`,
# MAGIC     source_so_no              AS `Source SO`,
# MAGIC     item_no                   AS `Item No`,
# MAGIC     description               AS `Description`,
# MAGIC     demand_qty                AS `Demand Qty`,
# MAGIC     need_by_date              AS `Need By Date`,
# MAGIC     days_past_suggested_order AS `Days Late`,
# MAGIC     exception_reason          AS `Issue Detail`
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_unified
# MAGIC WHERE plan_status IN ('BLOCKED_BOM_NOT_CERTIFIED', 'BLOCKED_NO_BOM')
# MAGIC ORDER BY 
# MAGIC     is_dnd DESC NULLS LAST,
# MAGIC     customer_tier,
# MAGIC     days_past_suggested_order DESC NULLS LAST,
# MAGIC     item_no;
# MAGIC 
# MAGIC 
# MAGIC -- =====================================================================
# MAGIC -- SHEET 4: Executive Summary  (For Leadership)
# MAGIC -- 8 rows — 1 per status category
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC WITH stats AS (
# MAGIC     SELECT 
# MAGIC         plan_status,
# MAGIC         COUNT(*) AS plan_count,
# MAGIC         SUM(planned_qty) AS total_qty,
# MAGIC         COUNT(DISTINCT item_no) AS unique_items,
# MAGIC         COUNT(DISTINCT source_so_no) AS unique_so
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_unified
# MAGIC     WHERE plan_status IN (
# MAGIC         'CRITICAL_LATE_DND', 'CRITICAL_LATE',
# MAGIC         'OVERDUE_PROPOSED_DND', 'OVERDUE_PROPOSED',
# MAGIC         'BLOCKED_NO_VENDOR', 'BLOCKED_NO_LEAD_TIME',
# MAGIC         'BLOCKED_NO_ROP_PARAMS', 'BLOCKED_BOM_NOT_CERTIFIED'
# MAGIC     )
# MAGIC     GROUP BY plan_status
# MAGIC )
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN plan_status = 'CRITICAL_LATE_DND' THEN 1
# MAGIC         WHEN plan_status = 'CRITICAL_LATE' THEN 2
# MAGIC         WHEN plan_status = 'OVERDUE_PROPOSED_DND' THEN 3
# MAGIC         WHEN plan_status = 'OVERDUE_PROPOSED' THEN 4
# MAGIC         WHEN plan_status = 'BLOCKED_NO_VENDOR' THEN 5
# MAGIC         WHEN plan_status = 'BLOCKED_NO_LEAD_TIME' THEN 6
# MAGIC         WHEN plan_status = 'BLOCKED_NO_ROP_PARAMS' THEN 7
# MAGIC         WHEN plan_status = 'BLOCKED_BOM_NOT_CERTIFIED' THEN 8
# MAGIC     END                       AS `Priority`,
# MAGIC     plan_status               AS `Status`,
# MAGIC     CASE 
# MAGIC         WHEN plan_status LIKE 'CRITICAL%' OR plan_status LIKE 'OVERDUE%' THEN 'Production'
# MAGIC         WHEN plan_status IN ('BLOCKED_NO_VENDOR', 'BLOCKED_NO_LEAD_TIME') THEN 'Procurement'
# MAGIC         WHEN plan_status = 'BLOCKED_NO_ROP_PARAMS' THEN 'Master Data'
# MAGIC         WHEN plan_status = 'BLOCKED_BOM_NOT_CERTIFIED' THEN 'Engineering'
# MAGIC     END                       AS `Owner Team`,
# MAGIC     CASE plan_status
# MAGIC         WHEN 'CRITICAL_LATE_DND' THEN 'Items late + DND customer'
# MAGIC         WHEN 'CRITICAL_LATE' THEN 'Items late, not DND'
# MAGIC         WHEN 'OVERDUE_PROPOSED_DND' THEN 'Order start passed + DND'
# MAGIC         WHEN 'OVERDUE_PROPOSED' THEN 'Order start passed'
# MAGIC         WHEN 'BLOCKED_NO_VENDOR' THEN 'No vendor on Item card'
# MAGIC         WHEN 'BLOCKED_NO_LEAD_TIME' THEN 'No Lead Time set'
# MAGIC         WHEN 'BLOCKED_NO_ROP_PARAMS' THEN 'ROP item missing reorder params'
# MAGIC         WHEN 'BLOCKED_BOM_NOT_CERTIFIED' THEN 'BOM exists but not Certified'
# MAGIC     END                       AS `Description`,
# MAGIC     plan_count                AS `Count`,
# MAGIC     unique_items              AS `Unique Items`,
# MAGIC     unique_so                 AS `Unique SO`,
# MAGIC     ROUND(total_qty, 2)       AS `Total Qty`
# MAGIC FROM stats
# MAGIC ORDER BY `Priority`;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # nb_gold_view_planned_orders   (Phase 5 — View 1 of 7)

# CELL ********************

# MAGIC %%sql
# MAGIC -- =====================================================================
# MAGIC -- Notebook: nb_gold_view_planned_orders   (Phase 5 — View 1 of 7)
# MAGIC -- Layer:    Gold MRP Engine — Public API Layer
# MAGIC -- Output:   Gold_Inventory_Lakehouse.mrp.gold_MRP_Planned_Orders
# MAGIC -- 
# MAGIC -- Purpose:
# MAGIC --   Master list ของ planned orders พร้อม:
# MAGIC --     - Denormalized vendor info
# MAGIC --     - Estimated cost (unit + total)
# MAGIC --     - Priority aggregation
# MAGIC --     - Revenue impact
# MAGIC --     - Source SO traceability
# MAGIC -- 
# MAGIC -- Consumer: HTML Report, Power BI, Power Automate write-back
# MAGIC -- Refresh:  Every MRP run
# MAGIC -- 
# MAGIC -- Adapted from spec (some fields simplified for pragmatic build):
# MAGIC --   - planning_run_id: use unified_at as proxy (until orchestration in Phase 6)
# MAGIC --   - parent_chain, casting_inputs: TBD (need PRO data — defer to View 5)
# MAGIC --   - karat, color: derived from item_category + item_no pattern
# MAGIC --   - scheduling_mode: BACKWARD if order_date >= today, FORWARD if late
# MAGIC --   - schedule_warning: structured JSON for late items
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 1: Setup ==============
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS Gold_Inventory_Lakehouse.mrp;
# MAGIC 
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInWrite = CORRECTED;
# MAGIC SET spark.sql.parquet.int96RebaseModeInRead = CORRECTED;
# MAGIC SET spark.sql.parquet.int96RebaseModeInWrite = CORRECTED;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 2: Build gold_MRP_Planned_Orders ==============
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.gold_MRP_Planned_Orders
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH 
# MAGIC -- Determine order_type per archetype
# MAGIC plans_with_type AS (
# MAGIC     SELECT 
# MAGIC         u.*,
# MAGIC         CASE 
# MAGIC             WHEN u.archetype IN ('ALLOY', 'MPS') THEN 'PRODUCTION'
# MAGIC             WHEN u.archetype = 'MST_DIRECT' THEN 'PRODUCTION'
# MAGIC             WHEN u.archetype IN ('L4L', 'ROP', 'ORDER') THEN 'PURCHASE'
# MAGIC             ELSE 'OTHER'
# MAGIC         END AS order_type,
# MAGIC         
# MAGIC         -- Scheduling mode
# MAGIC         CASE 
# MAGIC             WHEN u.suggested_order_date IS NULL THEN 'UNKNOWN'
# MAGIC             WHEN u.suggested_order_date <= DATE'1900-01-01' THEN 'UNKNOWN'
# MAGIC             WHEN u.suggested_order_date >= CURRENT_DATE() THEN 'BACKWARD'
# MAGIC             ELSE 'FORWARD'
# MAGIC         END AS scheduling_mode,
# MAGIC         
# MAGIC         -- Delay days (0 if backward, positive if forward)
# MAGIC         CASE 
# MAGIC             WHEN u.suggested_order_date IS NULL THEN 0
# MAGIC             WHEN u.suggested_order_date <= DATE'1900-01-01' THEN 0
# MAGIC             WHEN u.suggested_order_date < CURRENT_DATE() 
# MAGIC                 THEN DATEDIFF(CURRENT_DATE(), u.suggested_order_date)
# MAGIC             ELSE 0
# MAGIC         END AS delay_days
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_unified u
# MAGIC ),
# MAGIC 
# MAGIC -- Item master enrichment for denormalized fields
# MAGIC item_enrichment AS (
# MAGIC     SELECT 
# MAGIC         item_no,
# MAGIC         description AS im_description,
# MAGIC         item_category AS im_category,
# MAGIC         base_uom AS uom,
# MAGIC         unit_cost,
# MAGIC         vendor_no AS im_vendor_no,
# MAGIC         scrap_pct,
# MAGIC         CASE 
# MAGIC             WHEN item_category = 'CASTING' AND item_no LIKE '%14KY%' THEN '14KY'
# MAGIC             WHEN item_category = 'CASTING' AND item_no LIKE '%14KR%' THEN '14KR'
# MAGIC             WHEN item_category = 'CASTING' AND item_no LIKE '%14KW%' THEN '14KW'
# MAGIC             WHEN item_category = 'CASTING' AND item_no LIKE '%18KY%' THEN '18KY'
# MAGIC             WHEN item_category = 'CASTING' AND item_no LIKE '%18KR%' THEN '18KR'
# MAGIC             WHEN item_category = 'CASTING' AND item_no LIKE '%18KW%' THEN '18KW'
# MAGIC             WHEN item_category = 'CASTING' AND item_no LIKE '%9KY%' THEN '9KY'
# MAGIC             WHEN item_category = 'CASTING' AND item_no LIKE '%9KR%' THEN '9KR'
# MAGIC             WHEN item_category = 'CASTING' AND item_no LIKE '%9KW%' THEN '9KW'
# MAGIC             WHEN item_category = 'CASTING' AND item_no LIKE '%SLV%' THEN 'Silver925'
# MAGIC             ELSE NULL
# MAGIC         END AS karat,
# MAGIC         CASE 
# MAGIC             WHEN item_no LIKE '%KY%' OR item_no LIKE '%-Y%' THEN 'Yellow'
# MAGIC             WHEN item_no LIKE '%KR%' OR item_no LIKE '%-R%' THEN 'Rose'
# MAGIC             WHEN item_no LIKE '%KW%' OR item_no LIKE '%SLV%' THEN 'White'
# MAGIC             ELSE NULL
# MAGIC         END AS color
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Item_Master
# MAGIC ),
# MAGIC 
# MAGIC -- Vendor info from BC (use BC vendor mirror if exists, else fallback)
# MAGIC -- Note: backtick required for `No.` (BC dot-notation)
# MAGIC -- If Vendor table not yet mirrored, this returns NULL safely
# MAGIC vendor_lookup AS (
# MAGIC     SELECT 
# MAGIC         `No.` AS vendor_no,
# MAGIC         Name AS vendor_name
# MAGIC     FROM Silver_BC_Lakehouse.bc.Vendor
# MAGIC     WHERE `BC Company` = 'Ennovie'
# MAGIC ),
# MAGIC 
# MAGIC -- SO link aggregation (revenue + so list per plan)
# MAGIC so_aggregation AS (
# MAGIC     SELECT 
# MAGIC         u.plan_id,
# MAGIC         COUNT(DISTINCT u.source_so_no) AS affected_so_count,
# MAGIC         COLLECT_LIST(DISTINCT u.source_so_no) AS affected_so_list,
# MAGIC         -- For now, use linked SO line_amount as revenue proxy
# MAGIC         -- (will be refined in Phase 5 cross-SO module)
# MAGIC         SUM(COALESCE(so.line_amount_thb, 0)) AS total_revenue_impacted
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_unified u
# MAGIC     LEFT JOIN Gold_Inventory_Lakehouse.mrp.gold_SO so 
# MAGIC         ON u.source_so_no = so.so_no 
# MAGIC         AND u.item_no = so.item_no
# MAGIC         AND so.is_open = TRUE
# MAGIC     GROUP BY u.plan_id
# MAGIC )
# MAGIC 
# MAGIC -- Final SELECT with all denormalized fields
# MAGIC SELECT 
# MAGIC     -- Run identification (placeholder until Phase 6 orchestration)
# MAGIC     DATE_FORMAT(p.unified_at, 'yyyyMMdd_HHmm') AS planning_run_id,
# MAGIC     p.plan_id AS planned_order_id,
# MAGIC     
# MAGIC     -- Order classification
# MAGIC     p.order_type,
# MAGIC     p.archetype,
# MAGIC     
# MAGIC     -- Item info (denormalized)
# MAGIC     p.item_no,
# MAGIC     COALESCE(im.im_description, p.description)  AS item_description,
# MAGIC     COALESCE(im.im_category, p.item_category)   AS item_category,
# MAGIC     im.uom,
# MAGIC     im.karat,
# MAGIC     im.color,
# MAGIC     
# MAGIC     -- Quantities
# MAGIC     p.demand_qty,
# MAGIC     p.onhand_available_qty,
# MAGIC     p.shortage_qty,
# MAGIC     p.planned_qty AS qty,
# MAGIC     
# MAGIC     -- Dates & scheduling
# MAGIC     p.suggested_order_date AS release_date,
# MAGIC     p.need_by_date AS due_date,
# MAGIC     p.scheduling_mode,
# MAGIC     p.need_by_date AS original_due_date,
# MAGIC     p.delay_days,
# MAGIC     
# MAGIC     -- Schedule warning (structured JSON for FORWARD mode)
# MAGIC     CASE 
# MAGIC         WHEN p.scheduling_mode = 'FORWARD' THEN 
# MAGIC             CONCAT(
# MAGIC                 '{"reason":"lead_time_insufficient",',
# MAGIC                 '"delay_days":', CAST(p.delay_days AS STRING), ',',
# MAGIC                 '"cs_action_required":', 
# MAGIC                 CASE WHEN p.delay_days > 7 THEN 'true' ELSE 'false' END,
# MAGIC                 ',"plan_status":"', p.plan_status, '"}'
# MAGIC             )
# MAGIC         ELSE NULL
# MAGIC     END AS schedule_warning,
# MAGIC     
# MAGIC     -- Vendor info (denormalized from BC)
# MAGIC     im.im_vendor_no AS vendor_no,
# MAGIC     v.vendor_name,
# MAGIC     
# MAGIC     -- Cost info (estimated from Item Master)
# MAGIC     COALESCE(im.unit_cost, 0) AS estimated_unit_cost,
# MAGIC     ROUND(p.planned_qty * COALESCE(im.unit_cost, 0), 4) AS estimated_total_cost,
# MAGIC     
# MAGIC     -- Priority info
# MAGIC     p.priority_weight AS priority_score,
# MAGIC     p.is_dnd,
# MAGIC     p.customer_tier,
# MAGIC     
# MAGIC     -- Affected SOs aggregation
# MAGIC     p.source_so_no AS primary_source_so,
# MAGIC     COALESCE(soa.affected_so_count, 1) AS affected_so_count,
# MAGIC     COALESCE(soa.affected_so_list, ARRAY(p.source_so_no)) AS affected_so_list,
# MAGIC     COALESCE(soa.total_revenue_impacted, 0) AS total_revenue_impacted,
# MAGIC     
# MAGIC     -- Plan status & escalation
# MAGIC     p.plan_status,
# MAGIC     p.original_plan_status,
# MAGIC     p.escalation_level,
# MAGIC     p.exception_reason,
# MAGIC     
# MAGIC     -- Days metrics (intelligence layer)
# MAGIC     p.days_past_suggested_order,
# MAGIC     p.days_until_need_by,
# MAGIC     p.days_past_need_by,
# MAGIC     
# MAGIC     -- Tracking flags
# MAGIC     p.is_order_date_overdue,
# MAGIC     p.is_need_date_overdue,
# MAGIC     
# MAGIC     -- Metadata
# MAGIC     p.unified_at AS created_at,
# MAGIC     CURRENT_TIMESTAMP() AS view_built_at
# MAGIC     
# MAGIC FROM plans_with_type p
# MAGIC LEFT JOIN item_enrichment im ON p.item_no = im.item_no
# MAGIC LEFT JOIN vendor_lookup v ON im.im_vendor_no = v.vendor_no
# MAGIC LEFT JOIN so_aggregation soa ON p.plan_id = soa.plan_id;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 3: Validation — row count + column check ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     COUNT(*) AS total_planned_orders,
# MAGIC     COUNT(DISTINCT planning_run_id) AS run_count,
# MAGIC     COUNT(DISTINCT archetype) AS archetype_count,
# MAGIC     COUNT(DISTINCT item_no) AS unique_items,
# MAGIC     SUM(CASE WHEN scheduling_mode = 'BACKWARD' THEN 1 ELSE 0 END) AS backward_count,
# MAGIC     SUM(CASE WHEN scheduling_mode = 'FORWARD' THEN 1 ELSE 0 END) AS forward_count,
# MAGIC     SUM(CASE WHEN vendor_name IS NOT NULL THEN 1 ELSE 0 END) AS with_vendor_name,
# MAGIC     SUM(CASE WHEN estimated_unit_cost > 0 THEN 1 ELSE 0 END) AS with_cost,
# MAGIC     ROUND(SUM(estimated_total_cost), 2) AS total_estimated_cost,
# MAGIC     ROUND(SUM(total_revenue_impacted), 2) AS total_revenue_impacted
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_MRP_Planned_Orders;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 4: Order type breakdown ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     order_type,
# MAGIC     archetype,
# MAGIC     COUNT(*) AS plan_count,
# MAGIC     COUNT(DISTINCT item_no) AS unique_items,
# MAGIC     SUM(CASE WHEN scheduling_mode = 'FORWARD' THEN 1 ELSE 0 END) AS forward_count,
# MAGIC     ROUND(AVG(delay_days), 1) AS avg_delay,
# MAGIC     ROUND(SUM(qty), 2) AS total_qty,
# MAGIC     ROUND(SUM(estimated_total_cost), 2) AS total_cost,
# MAGIC     ROUND(SUM(total_revenue_impacted), 2) AS total_revenue
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_MRP_Planned_Orders
# MAGIC GROUP BY order_type, archetype
# MAGIC ORDER BY order_type, archetype;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 5: Forward scheduling — CS-facing query ==============
# MAGIC -- Items ที่ลีดไทม์ไม่ทันแล้ว — CS ต้องแจ้งลูกค้า
# MAGIC 
# MAGIC SELECT 
# MAGIC     planning_run_id,
# MAGIC     archetype,
# MAGIC     customer_tier,
# MAGIC     is_dnd,
# MAGIC     primary_source_so,
# MAGIC     item_no,
# MAGIC     item_description,
# MAGIC     qty,
# MAGIC     original_due_date,
# MAGIC     delay_days,
# MAGIC     plan_status,
# MAGIC     schedule_warning,
# MAGIC     affected_so_count,
# MAGIC     ROUND(total_revenue_impacted, 2) AS revenue_impacted_thb,
# MAGIC     ROUND(estimated_total_cost, 2) AS estimated_cost
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_MRP_Planned_Orders
# MAGIC WHERE scheduling_mode = 'FORWARD'
# MAGIC   AND delay_days > 0
# MAGIC ORDER BY 
# MAGIC     is_dnd DESC NULLS LAST,
# MAGIC     delay_days DESC,
# MAGIC     total_revenue_impacted DESC NULLS LAST
# MAGIC LIMIT 30;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 6: Vendor coverage check ==============
# MAGIC -- ดูว่า PURCHASE archetypes มี vendor info ครบหรือไม่
# MAGIC 
# MAGIC SELECT 
# MAGIC     archetype,
# MAGIC     COUNT(*) AS total_plans,
# MAGIC     SUM(CASE WHEN vendor_no IS NOT NULL THEN 1 ELSE 0 END) AS with_vendor_no,
# MAGIC     SUM(CASE WHEN vendor_name IS NOT NULL THEN 1 ELSE 0 END) AS with_vendor_name,
# MAGIC     ROUND(100.0 * SUM(CASE WHEN vendor_name IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_with_name
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_MRP_Planned_Orders
# MAGIC WHERE order_type = 'PURCHASE'
# MAGIC GROUP BY archetype
# MAGIC ORDER BY archetype;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 7: Cost coverage check ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     archetype,
# MAGIC     COUNT(*) AS total_plans,
# MAGIC     SUM(CASE WHEN estimated_unit_cost > 0 THEN 1 ELSE 0 END) AS with_cost,
# MAGIC     ROUND(100.0 * SUM(CASE WHEN estimated_unit_cost > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_with_cost,
# MAGIC     ROUND(SUM(estimated_total_cost), 2) AS total_cost_thb,
# MAGIC     ROUND(AVG(estimated_unit_cost), 2) AS avg_unit_cost
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_MRP_Planned_Orders
# MAGIC GROUP BY archetype
# MAGIC ORDER BY total_cost_thb DESC NULLS LAST;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # nb_gold_view_actions   (Phase 5 — View 2 of 7)

# CELL ********************

# MAGIC %%sql
# MAGIC -- =====================================================================
# MAGIC -- Notebook: nb_gold_view_actions   (Phase 5 — View 2 of 7)
# MAGIC -- Layer:    Gold MRP Engine — Public API Layer
# MAGIC -- Output:   Gold_Inventory_Lakehouse.mrp.gold_MRP_Actions
# MAGIC -- 
# MAGIC -- Purpose:
# MAGIC --   Action messages พร้อม severity, recommended_action, dampener
# MAGIC --   Derived from plan_status + escalation_level + days_past_*
# MAGIC -- 
# MAGIC -- Consumer: Claude AI agent, Teams alerts, Power Automate notifications
# MAGIC -- Refresh:  Every MRP run
# MAGIC -- 
# MAGIC -- Logic:
# MAGIC --   - Action types: EXPEDITE, EXPEDITE_DND, CREATE, CREATE_URGENT, REVIEW_BOM, FIX_DATA
# MAGIC --   - Severity: critical, high, medium, low (from escalation_level)
# MAGIC --   - Dampener: skip if days_off < 3 days (avoid alert fatigue)
# MAGIC --   - Status: pending (not yet acted on)
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 1: Setup ==============
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS Gold_Inventory_Lakehouse.mrp;
# MAGIC 
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInWrite = CORRECTED;
# MAGIC SET spark.sql.parquet.int96RebaseModeInRead = CORRECTED;
# MAGIC SET spark.sql.parquet.int96RebaseModeInWrite = CORRECTED;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 2: Build gold_MRP_Actions ==============
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.gold_MRP_Actions
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH 
# MAGIC -- Source: actionable plans (exclude COVERED_BY_STOCK and NO_TRIGGER)
# MAGIC actionable_plans AS (
# MAGIC     SELECT *
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_MRP_Planned_Orders
# MAGIC     WHERE plan_status NOT IN ('COVERED_BY_STOCK', 'NO_TRIGGER')
# MAGIC       AND escalation_level <= 6
# MAGIC )
# MAGIC 
# MAGIC SELECT 
# MAGIC     -- Identity
# MAGIC     UUID()                  AS action_id,
# MAGIC     planning_run_id,
# MAGIC     planned_order_id,
# MAGIC     
# MAGIC     -- Action classification
# MAGIC     CASE 
# MAGIC         WHEN plan_status = 'CRITICAL_LATE_DND'        THEN 'EXPEDITE_DND'
# MAGIC         WHEN plan_status = 'CRITICAL_LATE'            THEN 'EXPEDITE'
# MAGIC         WHEN plan_status = 'OVERDUE_PROPOSED_DND'     THEN 'CREATE_URGENT_DND'
# MAGIC         WHEN plan_status = 'OVERDUE_PROPOSED'         THEN 'CREATE_URGENT'
# MAGIC         WHEN plan_status LIKE 'BLOCKED_NO_VENDOR%'    THEN 'FIX_VENDOR'
# MAGIC         WHEN plan_status LIKE 'BLOCKED_NO_LEAD%'      THEN 'FIX_LEAD_TIME'
# MAGIC         WHEN plan_status LIKE 'BLOCKED_NO_ROP%'       THEN 'FIX_ROP_PARAMS'
# MAGIC         WHEN plan_status LIKE 'BLOCKED_BOM%'          THEN 'CERTIFY_BOM'
# MAGIC         WHEN plan_status = 'PROPOSED_DND'             THEN 'CREATE_DND'
# MAGIC         WHEN plan_status = 'PROPOSED'                 THEN 'CREATE'
# MAGIC         ELSE 'REVIEW'
# MAGIC     END AS action_type,
# MAGIC     
# MAGIC     -- Severity (4 levels)
# MAGIC     CASE 
# MAGIC         WHEN escalation_level = 1 THEN 'critical'  -- CRITICAL_LATE_DND
# MAGIC         WHEN escalation_level = 2 THEN 'high'      -- CRITICAL_LATE
# MAGIC         WHEN escalation_level = 3 THEN 'high'      -- OVERDUE_DND
# MAGIC         WHEN escalation_level = 4 THEN 'medium'    -- OVERDUE
# MAGIC         WHEN escalation_level = 5 THEN 'medium'    -- PROPOSED_DND
# MAGIC         WHEN escalation_level = 6 THEN 'low'       -- PROPOSED
# MAGIC         WHEN escalation_level = 0 THEN 'high'      -- BLOCKED (data issue)
# MAGIC         ELSE 'low'
# MAGIC     END AS severity,
# MAGIC     
# MAGIC     -- Owner team (who needs to act)
# MAGIC     CASE 
# MAGIC         WHEN plan_status LIKE 'CRITICAL%' OR plan_status LIKE 'OVERDUE%' THEN 'Production'
# MAGIC         WHEN plan_status IN ('BLOCKED_NO_VENDOR', 'BLOCKED_NO_LEAD_TIME') THEN 'Procurement'
# MAGIC         WHEN plan_status = 'BLOCKED_NO_ROP_PARAMS' THEN 'Master Data'
# MAGIC         WHEN plan_status = 'BLOCKED_BOM_NOT_CERTIFIED' THEN 'Engineering'
# MAGIC         WHEN plan_status LIKE 'PROPOSED%' AND order_type = 'PURCHASE' THEN 'Procurement'
# MAGIC         WHEN plan_status LIKE 'PROPOSED%' AND order_type = 'PRODUCTION' THEN 'Production'
# MAGIC         ELSE 'Planning'
# MAGIC     END AS owner_team,
# MAGIC     
# MAGIC     -- Item/order info (denormalized)
# MAGIC     item_no,
# MAGIC     item_description,
# MAGIC     archetype,
# MAGIC     qty AS shortfall_qty,
# MAGIC     
# MAGIC     -- Date info
# MAGIC     release_date AS target_release_date,
# MAGIC     due_date AS target_due_date,
# MAGIC     delay_days AS days_off,
# MAGIC     
# MAGIC     -- Dampener logic (avoid alert fatigue)
# MAGIC     -- Dampener applied if: small delay AND not DND AND not critical
# MAGIC     CASE 
# MAGIC         WHEN delay_days < 3 AND is_dnd = FALSE 
# MAGIC             AND plan_status NOT IN ('CRITICAL_LATE_DND', 'CRITICAL_LATE')
# MAGIC             THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS dampener_applied,
# MAGIC     
# MAGIC     CASE 
# MAGIC         WHEN delay_days < 3 AND is_dnd = FALSE 
# MAGIC             AND plan_status NOT IN ('CRITICAL_LATE_DND', 'CRITICAL_LATE')
# MAGIC             THEN 'minor_delay_below_threshold'
# MAGIC         ELSE NULL
# MAGIC     END AS dampener_reason,
# MAGIC     
# MAGIC     -- Customer/SO context
# MAGIC     primary_source_so,
# MAGIC     customer_tier,
# MAGIC     is_dnd,
# MAGIC     affected_so_count,
# MAGIC     affected_so_list,
# MAGIC     total_revenue_impacted AS revenue_at_risk,
# MAGIC     
# MAGIC     -- Recommended action (human-readable)
# MAGIC     CASE 
# MAGIC         WHEN plan_status = 'CRITICAL_LATE_DND' THEN 
# MAGIC             CONCAT('🚨 EXPEDITE NOW — DND customer ', COALESCE(customer_tier, ''), 
# MAGIC                 ', ', CAST(delay_days AS STRING), ' days late, ', 
# MAGIC                 CAST(affected_so_count AS STRING), ' SO(s) affected. Contact customer + supplier immediately.')
# MAGIC         WHEN plan_status = 'CRITICAL_LATE' THEN 
# MAGIC             CONCAT('🚨 EXPEDITE — ', CAST(delay_days AS STRING), ' days late, item: ', item_no)
# MAGIC         WHEN plan_status = 'OVERDUE_PROPOSED_DND' THEN 
# MAGIC             CONCAT('⚡ CREATE PO/PRO URGENTLY — DND ', COALESCE(customer_tier, ''), 
# MAGIC                 ', should have ordered ', CAST(delay_days AS STRING), ' days ago.')
# MAGIC         WHEN plan_status = 'OVERDUE_PROPOSED' THEN 
# MAGIC             CONCAT('⚠️ CREATE PO/PRO — overdue by ', CAST(delay_days AS STRING), ' days.')
# MAGIC         WHEN plan_status = 'BLOCKED_NO_VENDOR' THEN 
# MAGIC             CONCAT('❌ Procurement: Set vendor on Item card for ', item_no, ' (qty ', CAST(qty AS STRING), ')')
# MAGIC         WHEN plan_status = 'BLOCKED_NO_LEAD_TIME' THEN 
# MAGIC             CONCAT('❌ Procurement: Set Lead Time Calculation on Item ', item_no)
# MAGIC         WHEN plan_status = 'BLOCKED_NO_ROP_PARAMS' THEN 
# MAGIC             CONCAT('❌ Master Data: Set Reorder Point/Quantity for ', item_no)
# MAGIC         WHEN plan_status = 'BLOCKED_BOM_NOT_CERTIFIED' THEN 
# MAGIC             CONCAT('❌ Engineering: Certify BOM for FG ', item_no, 
# MAGIC                 ' (customer: ', COALESCE(customer_tier, 'unknown'), ')')
# MAGIC         WHEN plan_status = 'PROPOSED_DND' THEN 
# MAGIC             CONCAT('🟡 Create ', 
# MAGIC                 CASE WHEN order_type = 'PURCHASE' THEN 'PO' ELSE 'Production Order' END,
# MAGIC                 ' — DND customer, on time')
# MAGIC         WHEN plan_status = 'PROPOSED' THEN 
# MAGIC             CONCAT('🟢 Routine: Create ',
# MAGIC                 CASE WHEN order_type = 'PURCHASE' THEN 'PO' ELSE 'Production Order' END)
# MAGIC         ELSE 'Review plan status'
# MAGIC     END AS recommended_action,
# MAGIC     
# MAGIC     -- Plan status reference
# MAGIC     plan_status,
# MAGIC     escalation_level,
# MAGIC     exception_reason,
# MAGIC     
# MAGIC     -- Status (initial state)
# MAGIC     'pending' AS status,
# MAGIC     
# MAGIC     -- Decision log placeholder (Phase 6 will populate)
# MAGIC     CAST(NULL AS STRING) AS decision_log,
# MAGIC     
# MAGIC     -- Metadata
# MAGIC     CURRENT_TIMESTAMP() AS created_at
# MAGIC     
# MAGIC FROM actionable_plans;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 3: Validation — overall stats ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     COUNT(*) AS total_actions,
# MAGIC     SUM(CASE WHEN dampener_applied THEN 1 ELSE 0 END) AS dampened,
# MAGIC     SUM(CASE WHEN NOT dampener_applied THEN 1 ELSE 0 END) AS active_alerts,
# MAGIC     COUNT(DISTINCT owner_team) AS owner_teams,
# MAGIC     COUNT(DISTINCT action_type) AS action_types,
# MAGIC     ROUND(SUM(revenue_at_risk), 2) AS total_revenue_at_risk
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_MRP_Actions;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 4: Severity × Owner Team distribution ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     severity,
# MAGIC     owner_team,
# MAGIC     COUNT(*) AS action_count,
# MAGIC     SUM(CASE WHEN dampener_applied THEN 1 ELSE 0 END) AS dampened,
# MAGIC     SUM(CASE WHEN NOT dampener_applied THEN 1 ELSE 0 END) AS active_alerts,
# MAGIC     ROUND(SUM(revenue_at_risk), 2) AS revenue_at_risk
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_MRP_Actions
# MAGIC GROUP BY severity, owner_team
# MAGIC ORDER BY 
# MAGIC     CASE severity 
# MAGIC         WHEN 'critical' THEN 1 
# MAGIC         WHEN 'high' THEN 2 
# MAGIC         WHEN 'medium' THEN 3 
# MAGIC         ELSE 4 
# MAGIC     END,
# MAGIC     action_count DESC;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 5: Action type breakdown ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     action_type,
# MAGIC     severity,
# MAGIC     owner_team,
# MAGIC     COUNT(*) AS action_count,
# MAGIC     ROUND(AVG(days_off), 1) AS avg_days_off,
# MAGIC     SUM(CASE WHEN is_dnd THEN 1 ELSE 0 END) AS dnd_count
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_MRP_Actions
# MAGIC GROUP BY action_type, severity, owner_team
# MAGIC ORDER BY action_count DESC;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 6: Top 30 critical active alerts (no dampener) ==============
# MAGIC 
# MAGIC SELECT 
# MAGIC     severity,
# MAGIC     owner_team,
# MAGIC     action_type,
# MAGIC     customer_tier,
# MAGIC     is_dnd,
# MAGIC     primary_source_so,
# MAGIC     item_no,
# MAGIC     item_description,
# MAGIC     shortfall_qty,
# MAGIC     days_off,
# MAGIC     affected_so_count,
# MAGIC     ROUND(revenue_at_risk, 2) AS revenue_at_risk_thb,
# MAGIC     recommended_action
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_MRP_Actions
# MAGIC WHERE NOT dampener_applied
# MAGIC   AND severity IN ('critical', 'high')
# MAGIC ORDER BY 
# MAGIC     CASE severity WHEN 'critical' THEN 1 ELSE 2 END,
# MAGIC     revenue_at_risk DESC NULLS LAST,
# MAGIC     days_off DESC NULLS LAST
# MAGIC LIMIT 30;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # nb_gold_view_pro_dependency   (Phase 5 — View 5 of 7)

# CELL ********************

# MAGIC %%sql
# MAGIC -- =====================================================================
# MAGIC -- Notebook: nb_gold_view_pro_dependency  (Phase 5 — View 5 of 7) — v2 PATCHED
# MAGIC -- Layer:    Gold MRP Engine — Public API Layer
# MAGIC -- Output:   Gold_Inventory_Lakehouse.mrp.gold_PRO_Dependency
# MAGIC --
# MAGIC -- ⚡ v2 CHANGES (Apr 26, 2026):
# MAGIC --   [FIX 1] is_overdue: BC `Finished Date` is `0001-01-01` (not NULL) for active PROs.
# MAGIC --           Use ` > DATE'1900-01-01'` check, not `IS NULL`.
# MAGIC --   [FIX 2] Apply same fix to all date NULL handling: pro_finished_date,
# MAGIC --           line_due_date, line_starting_date, line_ending_date, pro_due_date.
# MAGIC --   [FIX 3] Add `is_finished_via_completion` flag (line_finished_qty >= line_qty)
# MAGIC --           because Released PROs may be physically complete but not yet posted.
# MAGIC --   [NEW]   Cell 7 rewritten with proper grain matching (item-level dedup)
# MAGIC --           + new flag column `has_existing_pro` baked into the table itself,
# MAGIC --           so HTML report can filter "true new" actions without a self-join.
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 1: Setup ==============
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS Gold_Inventory_Lakehouse.mrp;
# MAGIC 
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInWrite = CORRECTED;
# MAGIC SET spark.sql.parquet.int96RebaseModeInRead = CORRECTED;
# MAGIC SET spark.sql.parquet.int96RebaseModeInWrite = CORRECTED;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 2: Build gold_PRO_Dependency (PATCHED) ==============
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.gold_PRO_Dependency
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH
# MAGIC -- BC NULL date sentinel = '0001-01-01'. Anything <= 1900-01-01 = effectively NULL.
# MAGIC -- We pre-clean dates in the source CTEs so downstream logic becomes simple.
# MAGIC 
# MAGIC active_pro_headers AS (
# MAGIC     SELECT
# MAGIC         h.`No.`                          AS pro_no,
# MAGIC         h.Status                         AS pro_status,
# MAGIC         h.`Source No.`                   AS source_item_no,
# MAGIC         h.Description                    AS pro_description,
# MAGIC         h.`Source Type`                  AS source_type,
# MAGIC         CASE WHEN h.`For Prod.Order No.` = '' THEN NULL ELSE h.`For Prod.Order No.` END AS parent_pro_no,
# MAGIC         CASE WHEN h.`For Item` = '' THEN NULL ELSE h.`For Item` END AS for_item,
# MAGIC         CASE WHEN h.`Sales Order No.` = '' THEN NULL ELSE h.`Sales Order No.` END AS header_so_no,
# MAGIC         h.`Sales Order Line No.`         AS header_so_line_no,
# MAGIC         -- Clean BC sentinel dates (0001-01-01) → NULL
# MAGIC         CASE WHEN h.`Starting Date` > DATE'1900-01-01' THEN h.`Starting Date` END AS pro_starting_date,
# MAGIC         CASE WHEN h.`Ending Date`   > DATE'1900-01-01' THEN h.`Ending Date`   END AS pro_ending_date,
# MAGIC         CASE WHEN h.`Due Date`      > DATE'1900-01-01' THEN h.`Due Date`      END AS pro_due_date,
# MAGIC         CASE WHEN h.`Finished Date` > DATE'1900-01-01' THEN h.`Finished Date` END AS pro_finished_date,
# MAGIC         h.Quantity                       AS pro_qty,
# MAGIC         h.`Unit Cost`                    AS pro_unit_cost,
# MAGIC         h.`Cost Amount`                  AS pro_cost_amount,
# MAGIC         h.`Location Code`                AS pro_location,
# MAGIC         h.`Low-Level Code`               AS low_level_code,
# MAGIC         h.`Replan Ref. Status`           AS replan_status,
# MAGIC         h.`Ready to Finish Flag`         AS ready_to_finish,
# MAGIC         h.`Cons vs Exp > Tol Flag`       AS consumption_variance_flag,
# MAGIC         h.Blocked                        AS pro_blocked
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Production Order` h
# MAGIC     WHERE h.`BC Company` = 'Ennovie'
# MAGIC       AND h.Status IN ('Released', 'Firm Planned', 'Planned')
# MAGIC ),
# MAGIC 
# MAGIC pro_lines AS (
# MAGIC     SELECT
# MAGIC         l.`Prod. Order No.`              AS pro_no,
# MAGIC         l.`Line No.`                     AS line_no,
# MAGIC         l.`Item No.`                     AS item_no,
# MAGIC         l.Description                    AS line_description,
# MAGIC         l.Quantity                       AS line_qty,
# MAGIC         l.`Finished Quantity`            AS line_finished_qty,
# MAGIC         l.`Remaining Quantity`           AS line_remaining_qty,
# MAGIC         -- Clean BC sentinel dates
# MAGIC         CASE WHEN l.`Due Date`      > DATE'1900-01-01' THEN l.`Due Date`      END AS line_due_date,
# MAGIC         CASE WHEN l.`Starting Date` > DATE'1900-01-01' THEN l.`Starting Date` END AS line_starting_date,
# MAGIC         CASE WHEN l.`Ending Date`   > DATE'1900-01-01' THEN l.`Ending Date`   END AS line_ending_date,
# MAGIC         l.`Planning Level Code`          AS planning_level_code,
# MAGIC         CASE l.`Planning Level Code`
# MAGIC             WHEN 0 THEN 'FG'
# MAGIC             WHEN 1 THEN 'SEMI'
# MAGIC             WHEN 2 THEN 'WAX_WH'
# MAGIC             ELSE CONCAT('LEVEL_', CAST(l.`Planning Level Code` AS STRING))
# MAGIC         END                              AS planning_level_label,
# MAGIC         CASE WHEN l.`Production BOM No.` = '' THEN NULL ELSE l.`Production BOM No.` END AS bom_no,
# MAGIC         l.`Production BOM Version Code`  AS bom_version,
# MAGIC         CASE WHEN l.`Routing No.` = '' THEN NULL ELSE l.`Routing No.` END AS routing_no,
# MAGIC         CASE WHEN l.`Sales Order No.` = '' THEN NULL ELSE l.`Sales Order No.` END AS line_so_no,
# MAGIC         l.`Sales Order Line No.`         AS line_so_line_no,
# MAGIC         CASE WHEN l.`RFID Code` = '' THEN NULL ELSE l.`RFID Code` END AS rfid_code,
# MAGIC         l.`Unit Cost`                    AS line_unit_cost,
# MAGIC         l.`Cost Amount`                  AS line_cost_amount,
# MAGIC         l.Priority                       AS line_priority,
# MAGIC         l.`Location Code`                AS line_location,
# MAGIC         l.`MPS Order`                    AS mps_order_flag
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Prod Order Line` l
# MAGIC     WHERE l.`BC Company` = 'Ennovie'
# MAGIC       AND l.Status IN ('Released', 'Firm Planned', 'Planned')
# MAGIC ),
# MAGIC 
# MAGIC component_summary AS (
# MAGIC     SELECT
# MAGIC         c.`Prod. Order No.`              AS pro_no,
# MAGIC         c.`Prod. Order Line No.`         AS line_no,
# MAGIC         COUNT(*)                         AS component_count,
# MAGIC         COUNT(DISTINCT c.`Item No.`)     AS unique_components,
# MAGIC         SUM(c.Quantity)                  AS total_component_qty,
# MAGIC         SUM(c.`Remaining Quantity`)      AS total_remaining_qty,
# MAGIC         SUM(c.`Cost Amount`)             AS total_component_cost,
# MAGIC         COLLECT_LIST(DISTINCT c.`Item No.`) AS component_items
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Prod Order Component` c
# MAGIC     WHERE c.`BC Company` = 'Ennovie'
# MAGIC       AND c.Status IN ('Released', 'Firm Planned', 'Planned')
# MAGIC     GROUP BY c.`Prod. Order No.`, c.`Prod. Order Line No.`
# MAGIC ),
# MAGIC 
# MAGIC so_enrichment AS (
# MAGIC     SELECT DISTINCT
# MAGIC         so.so_no,
# MAGIC         so.so_line_no,
# MAGIC         so.customer_no,
# MAGIC         so.customer_name,
# MAGIC         so.customer_tier_code,
# MAGIC         so.is_dnd,
# MAGIC         so.dnd_exceed_date,
# MAGIC         so.ship_date         AS so_ship_date,
# MAGIC         so.line_amount_thb   AS so_line_amount_thb
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_SO so
# MAGIC     WHERE so.is_open = TRUE
# MAGIC ),
# MAGIC 
# MAGIC -- [NEW v2] Build a set of (item_no, source_so_no) pairs that the engine
# MAGIC -- has flagged as "needs new plan". We'll left-join this into the final
# MAGIC -- view so each PRO line knows whether it overlaps an engine recommendation.
# MAGIC engine_plan_keys AS (
# MAGIC     SELECT DISTINCT
# MAGIC         item_no,
# MAGIC         source_so_no
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_unified
# MAGIC     WHERE plan_status NOT IN ('COVERED_BY_STOCK', 'NO_TRIGGER')
# MAGIC       AND plan_status NOT LIKE 'BLOCKED%'
# MAGIC )
# MAGIC 
# MAGIC -- =========================
# MAGIC -- Final SELECT — denormalized PRO chain
# MAGIC -- =========================
# MAGIC SELECT
# MAGIC     -- PRO identification
# MAGIC     h.pro_no,
# MAGIC     h.pro_status,
# MAGIC     h.source_item_no,
# MAGIC     h.pro_description,
# MAGIC 
# MAGIC     -- Line info
# MAGIC     l.line_no,
# MAGIC     l.item_no                            AS line_item_no,
# MAGIC     l.line_description,
# MAGIC     l.planning_level_code,
# MAGIC     l.planning_level_label,
# MAGIC 
# MAGIC     -- BOM info
# MAGIC     l.bom_no,
# MAGIC     l.bom_version,
# MAGIC     l.routing_no,
# MAGIC 
# MAGIC     -- Quantities
# MAGIC     l.line_qty,
# MAGIC     l.line_finished_qty,
# MAGIC     l.line_remaining_qty,
# MAGIC     ROUND(
# MAGIC         CASE
# MAGIC             WHEN l.line_qty > 0
# MAGIC                 THEN 100.0 * l.line_finished_qty / l.line_qty
# MAGIC             ELSE 0
# MAGIC         END, 1
# MAGIC     )                                    AS pct_complete,
# MAGIC 
# MAGIC     -- [FIX 3] Physical completion flag (qty produced ≥ qty planned)
# MAGIC     CASE
# MAGIC         WHEN l.line_qty > 0 AND l.line_finished_qty >= l.line_qty THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END                                  AS is_finished_via_completion,
# MAGIC 
# MAGIC     -- Dates (cleaned)
# MAGIC     l.line_starting_date,
# MAGIC     l.line_ending_date,
# MAGIC     l.line_due_date,
# MAGIC     h.pro_finished_date,
# MAGIC     h.pro_due_date,
# MAGIC 
# MAGIC     -- Days metrics
# MAGIC     CASE
# MAGIC         WHEN l.line_due_date IS NULL THEN NULL
# MAGIC         ELSE DATEDIFF(l.line_due_date, CURRENT_DATE())
# MAGIC     END                                  AS days_until_due,
# MAGIC 
# MAGIC     -- [FIX 1+2] is_overdue: line is overdue iff
# MAGIC     --   1) line_due_date is real (not BC sentinel)
# MAGIC     --   2) due date in the past
# MAGIC     --   3) PRO not yet posted as finished (pro_finished_date IS NULL after cleaning)
# MAGIC     --   4) line not yet physically complete
# MAGIC     CASE
# MAGIC         WHEN l.line_due_date IS NULL                          THEN FALSE  -- no real due date
# MAGIC         WHEN l.line_due_date >= CURRENT_DATE()                THEN FALSE  -- not yet due
# MAGIC         WHEN h.pro_finished_date IS NOT NULL                  THEN FALSE  -- already posted finished
# MAGIC         WHEN l.line_qty > 0
# MAGIC          AND l.line_finished_qty >= l.line_qty                THEN FALSE  -- physically complete
# MAGIC         ELSE TRUE
# MAGIC     END                                  AS is_overdue,
# MAGIC 
# MAGIC     -- Days overdue (positive number when late, NULL when not overdue)
# MAGIC     CASE
# MAGIC         WHEN l.line_due_date IS NULL                          THEN NULL
# MAGIC         WHEN l.line_due_date >= CURRENT_DATE()                THEN NULL
# MAGIC         WHEN h.pro_finished_date IS NOT NULL                  THEN NULL
# MAGIC         WHEN l.line_qty > 0
# MAGIC          AND l.line_finished_qty >= l.line_qty                THEN NULL
# MAGIC         ELSE DATEDIFF(CURRENT_DATE(), l.line_due_date)
# MAGIC     END                                  AS days_overdue,
# MAGIC 
# MAGIC     -- Cost
# MAGIC     l.line_unit_cost,
# MAGIC     l.line_cost_amount,
# MAGIC 
# MAGIC     -- Parent linkage
# MAGIC     h.parent_pro_no,
# MAGIC     h.for_item,
# MAGIC 
# MAGIC     -- SO linkage (line OR header)
# MAGIC     COALESCE(l.line_so_no, h.header_so_no)              AS source_so_no,
# MAGIC     COALESCE(l.line_so_line_no, h.header_so_line_no)    AS source_so_line_no,
# MAGIC 
# MAGIC     -- Customer info
# MAGIC     soe.customer_no,
# MAGIC     soe.customer_name,
# MAGIC     soe.customer_tier_code               AS customer_tier,
# MAGIC     soe.is_dnd,
# MAGIC     soe.so_ship_date,
# MAGIC     soe.so_line_amount_thb,
# MAGIC 
# MAGIC     -- Component summary
# MAGIC     COALESCE(cs.component_count, 0)      AS component_count,
# MAGIC     COALESCE(cs.unique_components, 0)    AS unique_components,
# MAGIC     cs.total_component_qty,
# MAGIC     cs.total_remaining_qty,
# MAGIC     cs.total_component_cost,
# MAGIC     cs.component_items,
# MAGIC 
# MAGIC     -- Status flags
# MAGIC     l.mps_order_flag,
# MAGIC     h.ready_to_finish,
# MAGIC     h.consumption_variance_flag,
# MAGIC     h.replan_status,
# MAGIC     h.pro_blocked,
# MAGIC     l.rfid_code,
# MAGIC 
# MAGIC     -- Location
# MAGIC     l.line_location,
# MAGIC 
# MAGIC     -- Priority
# MAGIC     l.line_priority,
# MAGIC 
# MAGIC     -- Type classification
# MAGIC     CASE
# MAGIC         WHEN h.pro_no LIKE 'CAS%'                       THEN 'CASTING'
# MAGIC         WHEN h.pro_no LIKE 'WRO%'                       THEN 'FG_PRODUCTION'
# MAGIC         WHEN h.pro_no LIKE 'WSEMI%' OR h.pro_no LIKE 'SEMI%' THEN 'SEMI'
# MAGIC         WHEN l.planning_level_code = 0                  THEN 'FG_PRODUCTION'
# MAGIC         WHEN l.planning_level_code = 1                  THEN 'SEMI'
# MAGIC         WHEN l.planning_level_code = 2                  THEN 'WAX_WH'
# MAGIC         ELSE 'OTHER'
# MAGIC     END                                  AS pro_type,
# MAGIC 
# MAGIC     -- [NEW v2] Engine overlap flag — TRUE if MRP engine also wants this (item, SO)
# MAGIC     CASE
# MAGIC         WHEN ek.item_no IS NOT NULL THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END                                  AS has_engine_recommendation,
# MAGIC 
# MAGIC     -- Metadata
# MAGIC     CURRENT_TIMESTAMP()                  AS view_built_at
# MAGIC 
# MAGIC FROM active_pro_headers h
# MAGIC INNER JOIN pro_lines l
# MAGIC     ON h.pro_no = l.pro_no
# MAGIC LEFT JOIN component_summary cs
# MAGIC     ON l.pro_no = cs.pro_no AND l.line_no = cs.line_no
# MAGIC LEFT JOIN so_enrichment soe
# MAGIC     ON COALESCE(l.line_so_no, h.header_so_no) = soe.so_no
# MAGIC    AND COALESCE(l.line_so_line_no, h.header_so_line_no) = soe.so_line_no
# MAGIC LEFT JOIN engine_plan_keys ek
# MAGIC     ON ek.item_no = l.item_no
# MAGIC    AND COALESCE(ek.source_so_no, '') = COALESCE(l.line_so_no, h.header_so_no, '');
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 3: Validation — overall stats (PATCHED) ==============
# MAGIC -- Expected: overdue_lines should now be > 0 (was 0 in v1 due to BC sentinel bug)
# MAGIC 
# MAGIC SELECT
# MAGIC     COUNT(*)                                                  AS total_pro_lines,
# MAGIC     COUNT(DISTINCT pro_no)                                    AS unique_pros,
# MAGIC     COUNT(DISTINCT line_item_no)                              AS unique_items,
# MAGIC     COUNT(DISTINCT source_so_no)                              AS linked_so_count,
# MAGIC     SUM(CASE WHEN source_so_no IS NOT NULL THEN 1 ELSE 0 END) AS lines_with_so,
# MAGIC     SUM(CASE WHEN is_overdue THEN 1 ELSE 0 END)               AS overdue_lines,
# MAGIC     SUM(CASE WHEN is_dnd = TRUE THEN 1 ELSE 0 END)            AS dnd_lines,
# MAGIC     SUM(CASE WHEN has_engine_recommendation THEN 1 ELSE 0 END) AS lines_with_engine_overlap,
# MAGIC     SUM(CASE WHEN is_finished_via_completion THEN 1 ELSE 0 END) AS physically_complete_lines,
# MAGIC     ROUND(SUM(line_cost_amount), 2)                           AS total_cost_amount,
# MAGIC     ROUND(SUM(so_line_amount_thb), 2)                         AS total_revenue_linked
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_PRO_Dependency;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 4: PRO type × status × planning_level distribution ==============
# MAGIC 
# MAGIC SELECT
# MAGIC     pro_type,
# MAGIC     pro_status,
# MAGIC     planning_level_label,
# MAGIC     COUNT(*) AS line_count,
# MAGIC     COUNT(DISTINCT pro_no) AS unique_pros,
# MAGIC     SUM(CASE WHEN is_overdue THEN 1 ELSE 0 END) AS overdue,
# MAGIC     SUM(CASE WHEN is_dnd THEN 1 ELSE 0 END) AS dnd_count,
# MAGIC     SUM(CASE WHEN has_engine_recommendation THEN 1 ELSE 0 END) AS engine_overlap,
# MAGIC     ROUND(AVG(pct_complete), 1) AS avg_pct_complete,
# MAGIC     ROUND(SUM(line_cost_amount), 2) AS total_cost
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_PRO_Dependency
# MAGIC GROUP BY pro_type, pro_status, planning_level_label
# MAGIC ORDER BY pro_type, pro_status, planning_level_label;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 5: Top 30 overdue active PROs (PATCHED) ==============
# MAGIC -- Expected: should now return rows (was empty in v1)
# MAGIC 
# MAGIC SELECT
# MAGIC     pro_no,
# MAGIC     pro_status,
# MAGIC     pro_type,
# MAGIC     planning_level_label,
# MAGIC     customer_tier,
# MAGIC     is_dnd,
# MAGIC     source_so_no,
# MAGIC     line_item_no,
# MAGIC     line_description,
# MAGIC     line_qty,
# MAGIC     line_finished_qty,
# MAGIC     pct_complete,
# MAGIC     line_due_date,
# MAGIC     days_until_due,
# MAGIC     days_overdue,
# MAGIC     component_count,
# MAGIC     rfid_code,
# MAGIC     has_engine_recommendation
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_PRO_Dependency
# MAGIC WHERE is_overdue = TRUE
# MAGIC   AND pro_status IN ('Released', 'Firm Planned')
# MAGIC ORDER BY
# MAGIC     is_dnd DESC NULLS LAST,
# MAGIC     days_overdue DESC NULLS LAST,
# MAGIC     pct_complete ASC
# MAGIC LIMIT 30;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 6: SO linkage coverage (UNCHANGED — was already correct) ==============
# MAGIC 
# MAGIC SELECT
# MAGIC     pro_type,
# MAGIC     COUNT(*) AS total_lines,
# MAGIC     SUM(CASE WHEN source_so_no IS NOT NULL THEN 1 ELSE 0 END) AS with_so,
# MAGIC     SUM(CASE WHEN customer_no IS NOT NULL THEN 1 ELSE 0 END) AS with_customer,
# MAGIC     ROUND(100.0 * SUM(CASE WHEN source_so_no IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_with_so
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_PRO_Dependency
# MAGIC GROUP BY pro_type
# MAGIC ORDER BY total_lines DESC;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 7: Engine plans vs Active PROs cross-check (REWRITTEN v2) ==============
# MAGIC -- v1 problem: 1 plan ↔ many PRO lines (FG+SEMI+WAX) caused fan-out → ratios > 100%
# MAGIC -- v2 fix: dedup BOTH sides to (archetype, plan_status, item_no, source_so_no) grain
# MAGIC -- before joining, so ratios are bounded 0-100% and comparable.
# MAGIC 
# MAGIC WITH plans_unique AS (
# MAGIC     SELECT DISTINCT
# MAGIC         archetype,
# MAGIC         plan_status,
# MAGIC         item_no,
# MAGIC         COALESCE(source_so_no, '') AS source_so_no_key,
# MAGIC         plan_id
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_unified
# MAGIC     WHERE plan_status NOT IN ('COVERED_BY_STOCK', 'NO_TRIGGER')
# MAGIC       AND plan_status NOT LIKE 'BLOCKED%'
# MAGIC ),
# MAGIC -- Dedup PRO Dependency to one row per (item, SO) — this matches plan grain
# MAGIC pros_unique AS (
# MAGIC     SELECT DISTINCT
# MAGIC         line_item_no                          AS item_no,
# MAGIC         COALESCE(source_so_no, '')            AS source_so_no_key,
# MAGIC         pro_no
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_PRO_Dependency
# MAGIC )
# MAGIC SELECT
# MAGIC     p.archetype,
# MAGIC     p.plan_status,
# MAGIC     COUNT(DISTINCT p.plan_id)               AS planned_orders,
# MAGIC     COUNT(DISTINCT pr.pro_no)               AS distinct_matching_pros,
# MAGIC     -- "Plan keys" (item+SO combos) covered by at least one active PRO
# MAGIC     COUNT(DISTINCT CASE WHEN pr.pro_no IS NOT NULL
# MAGIC                         THEN CONCAT(p.item_no, '||', p.source_so_no_key) END) AS plan_keys_with_pro,
# MAGIC     COUNT(DISTINCT CONCAT(p.item_no, '||', p.source_so_no_key)) AS plan_keys_total,
# MAGIC     ROUND(
# MAGIC         100.0 * COUNT(DISTINCT CASE WHEN pr.pro_no IS NOT NULL
# MAGIC                                     THEN CONCAT(p.item_no, '||', p.source_so_no_key) END)
# MAGIC               / NULLIF(COUNT(DISTINCT CONCAT(p.item_no, '||', p.source_so_no_key)), 0)
# MAGIC     , 1)                                     AS pct_plan_keys_covered_by_pro
# MAGIC FROM plans_unique p
# MAGIC LEFT JOIN pros_unique pr
# MAGIC     ON pr.item_no = p.item_no
# MAGIC    AND pr.source_so_no_key = p.source_so_no_key
# MAGIC GROUP BY p.archetype, p.plan_status
# MAGIC ORDER BY planned_orders DESC;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 8 [NEW]: Engine "true new" actionability summary ==============
# MAGIC -- For HTML report — answers "of N engine plans, how many are TRUE NEW vs duplicating
# MAGIC -- an in-flight PRO?" — clean number to put in KPI cards.
# MAGIC 
# MAGIC WITH plans_dedup AS (
# MAGIC     SELECT DISTINCT
# MAGIC         archetype,
# MAGIC         plan_status,
# MAGIC         item_no,
# MAGIC         COALESCE(source_so_no, '') AS so_key,
# MAGIC         plan_id
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_unified
# MAGIC ),
# MAGIC pro_keys AS (
# MAGIC     SELECT DISTINCT
# MAGIC         line_item_no AS item_no,
# MAGIC         COALESCE(source_so_no, '') AS so_key
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_PRO_Dependency
# MAGIC )
# MAGIC SELECT
# MAGIC     p.archetype,
# MAGIC     COUNT(DISTINCT p.plan_id)                                    AS total_plans,
# MAGIC     COUNT(DISTINCT CASE WHEN pk.item_no IS NULL
# MAGIC                         THEN p.plan_id END)                       AS true_new_plans,
# MAGIC     COUNT(DISTINCT CASE WHEN pk.item_no IS NOT NULL
# MAGIC                         THEN p.plan_id END)                       AS plans_with_existing_pro,
# MAGIC     ROUND(
# MAGIC         100.0 * COUNT(DISTINCT CASE WHEN pk.item_no IS NULL
# MAGIC                                     THEN p.plan_id END)
# MAGIC               / NULLIF(COUNT(DISTINCT p.plan_id), 0)
# MAGIC     , 1)                                                          AS pct_true_new
# MAGIC FROM plans_dedup p
# MAGIC LEFT JOIN pro_keys pk
# MAGIC     ON pk.item_no = p.item_no
# MAGIC    AND pk.so_key  = p.so_key
# MAGIC GROUP BY p.archetype
# MAGIC ORDER BY total_plans DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

#  # nb_gold_view_material_availability  (Phase 5 — View 6 of 7)

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- =====================================================================
# MAGIC -- Notebook: nb_gold_view_material_availability v4.3+ (PATCHED)
# MAGIC -- Output:   Gold_Inventory_Lakehouse.mrp.gold_Material_Availability
# MAGIC --
# MAGIC -- ⭐ v4.3+ PATCH: Add consumption_status filter
# MAGIC --   Excludes FIRMED_BOM demand where component is already COMPLETED
# MAGIC --   (all qty consumed in active PROs)
# MAGIC --
# MAGIC -- 🐛 BUG FIX (2026-05-12) — preserved from v4.3:
# MAGIC --   Bug 4: Purchase UoM Mismatch — use Outstanding Qty. (Base)
# MAGIC --
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC -- ============== CELL 2: Build gold_Material_Availability v4.3+ ==============
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.gold_Material_Availability
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH
# MAGIC -- =====================================================
# MAGIC -- SHARED LOOKUPS (used by all 3 demand types)
# MAGIC -- =====================================================
# MAGIC 
# MAGIC -- Holidays: dedupe (calendar has duplicates)
# MAGIC holidays_distinct AS (
# MAGIC     SELECT DISTINCT Holiday_Date AS holiday_date
# MAGIC     FROM Silver_Commons_Lakehouse.cmn.silver_holiday_calendar
# MAGIC     WHERE Holiday_Date IS NOT NULL
# MAGIC ),
# MAGIC 
# MAGIC -- Category lead time average (Tier 2 fallback)
# MAGIC category_lead_time AS (
# MAGIC     SELECT
# MAGIC         `Item Category Code` AS item_category,
# MAGIC         CAST(ROUND(AVG(
# MAGIC             CASE
# MAGIC                 WHEN `Lead Time Calculation` IS NULL OR `Lead Time Calculation` = '' THEN NULL
# MAGIC                 WHEN `Lead Time Calculation` LIKE '%D'
# MAGIC                     THEN CAST(REGEXP_REPLACE(`Lead Time Calculation`, '[^0-9]', '') AS INT)
# MAGIC                 ELSE NULL
# MAGIC             END
# MAGIC         ), 0) AS INT)        AS avg_lead_time_days
# MAGIC     FROM Silver_BC_Lakehouse.bc.Item
# MAGIC     WHERE `BC Company` = 'Ennovie'
# MAGIC       AND Blocked = false
# MAGIC       AND `Item Category Code` IS NOT NULL
# MAGIC     GROUP BY `Item Category Code`
# MAGIC     HAVING SUM(CASE WHEN `Lead Time Calculation` IS NULL OR `Lead Time Calculation` = ''
# MAGIC                     THEN 0 ELSE 1 END) >= 5
# MAGIC ),
# MAGIC 
# MAGIC -- Item master with parsed lead time
# MAGIC item_master_enriched AS (
# MAGIC     SELECT
# MAGIC         i.`No.`                                                              AS item_no,
# MAGIC         i.Description                                                        AS item_description,
# MAGIC         i.`Item Category Code`                                               AS item_category,
# MAGIC         i.`Material Type`                                                    AS material_type,
# MAGIC         i.`Metal Category Code`                                              AS metal_category,
# MAGIC         i.`Vendor No.`                                                       AS preferred_vendor_no,
# MAGIC         i.`Base Unit of Measure`                                             AS base_uom,
# MAGIC         i.`Replenishment System`                                             AS repl_system,
# MAGIC         i.`Reordering Policy`                                                AS reorder_policy,
# MAGIC         i.Critical                                                           AS is_critical,
# MAGIC         CASE
# MAGIC             WHEN i.`Lead Time Calculation` IS NULL OR i.`Lead Time Calculation` = '' THEN NULL
# MAGIC             WHEN i.`Lead Time Calculation` LIKE '%D'
# MAGIC                 THEN CAST(REGEXP_REPLACE(i.`Lead Time Calculation`, '[^0-9]', '') AS INT)
# MAGIC             ELSE NULL
# MAGIC         END                                                                  AS lead_time_days_raw,
# MAGIC         i.`Last Direct Cost`                                                 AS last_direct_cost,
# MAGIC         i.`Standard Cost`                                                    AS standard_cost
# MAGIC     FROM Silver_BC_Lakehouse.bc.Item i
# MAGIC     WHERE i.`BC Company` = 'Ennovie'
# MAGIC       AND i.Blocked = false
# MAGIC ),
# MAGIC 
# MAGIC -- Smart routing: which items have BOM (i.e., produced) vs leaf (i.e., procured)?
# MAGIC items_with_bom AS (
# MAGIC     SELECT DISTINCT root_item AS item_no
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_bom_exploded
# MAGIC ),
# MAGIC 
# MAGIC -- Vendor name lookup
# MAGIC vendor_lookup AS (
# MAGIC     SELECT DISTINCT
# MAGIC         v.`No.`                                                              AS vendor_no,
# MAGIC         v.Name                                                               AS vendor_name
# MAGIC     FROM Silver_BC_Lakehouse.bc.Vendor v
# MAGIC     WHERE v.`BC Company` = 'Ennovie'
# MAGIC ),
# MAGIC 
# MAGIC -- SO enrichment
# MAGIC so_enrichment AS (
# MAGIC     SELECT DISTINCT
# MAGIC         so.so_no,
# MAGIC         so.so_line_no,
# MAGIC         so.customer_no,
# MAGIC         so.customer_name,
# MAGIC         so.customer_tier_code,
# MAGIC         so.is_dnd,
# MAGIC         so.ship_date         AS so_ship_date,
# MAGIC         so.line_amount_thb   AS so_line_amount_thb
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_SO so
# MAGIC     WHERE so.is_open = TRUE
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- v4.1: CDC dedup for Production Order tables (UNCHANGED)
# MAGIC -- =====================================================
# MAGIC 
# MAGIC -- Active PRO headers (CDC dedup by SystemRowVersion)
# MAGIC active_pro_headers AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             h.`No.`                                                              AS pro_no,
# MAGIC             h.Status                                                             AS pro_status,
# MAGIC             h.`Source No.`                                                       AS source_item_no,
# MAGIC             CASE WHEN h.`Sales Order No.` = '' THEN NULL
# MAGIC                  ELSE h.`Sales Order No.` END                                    AS header_so_no,
# MAGIC             h.`Sales Order Line No.`                                             AS header_so_line_no,
# MAGIC             CASE WHEN h.`Starting Date` > DATE'1900-01-01' THEN h.`Starting Date` END AS pro_starting_date,
# MAGIC             CASE WHEN h.`Due Date`      > DATE'1900-01-01' THEN h.`Due Date`      END AS pro_due_date,
# MAGIC             CASE WHEN h.`Finished Date` > DATE'1900-01-01' THEN h.`Finished Date` END AS pro_finished_date,
# MAGIC             h.Quantity                                                           AS pro_qty,
# MAGIC             h.Description                                                        AS pro_description,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY h.`No.`
# MAGIC                 ORDER BY h.`SystemRowVersion` DESC
# MAGIC             ) AS rn
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Production Order` h
# MAGIC         WHERE h.`BC Company` = 'Ennovie'
# MAGIC           AND h.Status IN ('Released', 'Firm Planned', 'Planned')
# MAGIC     ) t
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC -- Active PRO lines (CDC dedup by SystemRowVersion)
# MAGIC active_pro_lines AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             l.`Prod. Order No.`                                                  AS pro_no,
# MAGIC             l.`Line No.`                                                         AS line_no,
# MAGIC             l.`Item No.`                                                         AS produced_item_no,
# MAGIC             l.`Planning Level Code`                                              AS planning_level_code,
# MAGIC             CASE l.`Planning Level Code`
# MAGIC                 WHEN 0 THEN 'FG'
# MAGIC                 WHEN 1 THEN 'SEMI'
# MAGIC                 WHEN 2 THEN 'WAX_WH'
# MAGIC                 ELSE CONCAT('LEVEL_', CAST(l.`Planning Level Code` AS STRING))
# MAGIC             END                                                                  AS planning_level_label,
# MAGIC             CASE WHEN l.`Sales Order No.` = '' THEN NULL
# MAGIC                  ELSE l.`Sales Order No.` END                                    AS line_so_no,
# MAGIC             l.`Sales Order Line No.`                                             AS line_so_line_no,
# MAGIC             CASE WHEN l.`Starting Date` > DATE'1900-01-01' THEN l.`Starting Date` END AS line_starting_date,
# MAGIC             CASE WHEN l.`Due Date`      > DATE'1900-01-01' THEN l.`Due Date`      END AS line_due_date,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY l.`Prod. Order No.`, l.`Line No.`
# MAGIC                 ORDER BY l.`SystemRowVersion` DESC
# MAGIC             ) AS rn
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Prod Order Line` l
# MAGIC         WHERE l.`BC Company` = 'Ennovie'
# MAGIC           AND l.Status IN ('Released', 'Firm Planned', 'Planned')
# MAGIC     ) t
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- ⭐ CONSUMPTION STATUS LOOKUP (NEW v4.3+)
# MAGIC -- =====================================================
# MAGIC consumption_status_lookup AS (
# MAGIC     SELECT DISTINCT
# MAGIC         ComponentItemNo,
# MAGIC         ProdOrderNo,
# MAGIC         Consump_Status
# MAGIC     FROM Gold_Inventory_Lakehouse.inv.gold_consumption_status
# MAGIC     WHERE Consump_Status = 'COMPLETED'
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- TYPE 1: FIRMED_BOM — Active PRO Components (⭐ with filter)
# MAGIC -- =====================================================
# MAGIC 
# MAGIC type1_pro_components AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             c.`Prod. Order No.`                                                  AS pro_no,
# MAGIC             c.`Prod. Order Line No.`                                             AS pro_line_no,
# MAGIC             c.`Line No.`                                                         AS comp_line_no,
# MAGIC             c.`Item No.`                                                         AS component_item_no,
# MAGIC             c.Description                                                        AS component_description,
# MAGIC             c.Quantity                                                           AS required_qty_total,
# MAGIC             c.`Remaining Quantity`                                               AS required_qty_remaining,
# MAGIC             c.`Expected Quantity`                                                AS required_qty_expected,
# MAGIC             c.`Quantity per`                                                     AS qty_per,
# MAGIC             c.`Cost Amount`                                                      AS component_cost_amount,
# MAGIC             c.`Unit Cost`                                                        AS component_unit_cost,
# MAGIC             c.`Location Code`                                                    AS component_location,
# MAGIC             CASE WHEN c.`Due Date`        > DATE'1900-01-01' THEN c.`Due Date` END AS component_due_date,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY c.`Prod. Order No.`, c.`Prod. Order Line No.`, c.`Line No.`
# MAGIC                 ORDER BY c.`SystemRowVersion` DESC
# MAGIC             ) AS rn
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Prod Order Component` c
# MAGIC         WHERE c.`BC Company` = 'Ennovie'
# MAGIC           AND c.Status IN ('Released', 'Firm Planned', 'Planned')
# MAGIC           AND c.`Remaining Quantity` > 0
# MAGIC           AND c.`Item No.` NOT LIKE 'PL-%'
# MAGIC     ) t
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC type1_demand AS (
# MAGIC     SELECT
# MAGIC         'FIRMED_BOM'                                                         AS demand_type,
# MAGIC         h.pro_no                                                             AS source_record_id,
# MAGIC         CAST(l.line_no AS STRING)                                            AS source_line_id,
# MAGIC         h.pro_status                                                         AS source_status,
# MAGIC         h.source_item_no                                                     AS produced_item_no,
# MAGIC         l.planning_level_code,
# MAGIC         l.planning_level_label,
# MAGIC         h.pro_description                                                    AS source_description,
# MAGIC         c.component_item_no,
# MAGIC         c.component_description,
# MAGIC         c.required_qty_remaining                                             AS required_qty,
# MAGIC         c.qty_per,
# MAGIC         c.component_unit_cost,
# MAGIC         c.component_cost_amount,
# MAGIC         c.component_location,
# MAGIC         COALESCE(c.component_due_date, l.line_starting_date, h.pro_starting_date) AS required_date_raw,
# MAGIC         CASE
# MAGIC             WHEN c.component_due_date IS NOT NULL THEN 'COMPONENT_DUE'
# MAGIC             WHEN l.line_starting_date IS NOT NULL THEN 'LINE_START'
# MAGIC             WHEN h.pro_starting_date IS NOT NULL  THEN 'HEADER_START'
# MAGIC             ELSE 'UNKNOWN'
# MAGIC         END                                                                  AS required_date_source,
# MAGIC         h.pro_due_date,
# MAGIC         l.line_due_date,
# MAGIC         h.pro_starting_date,
# MAGIC         COALESCE(l.line_so_no, h.header_so_no)                               AS source_so_no,
# MAGIC         COALESCE(l.line_so_line_no, h.header_so_line_no)                     AS source_so_line_no,
# MAGIC         100                                                                  AS confidence_score
# MAGIC     FROM active_pro_headers h
# MAGIC     INNER JOIN active_pro_lines l
# MAGIC         ON l.pro_no = h.pro_no
# MAGIC     INNER JOIN type1_pro_components c
# MAGIC         ON c.pro_no = l.pro_no
# MAGIC        AND c.pro_line_no = l.line_no
# MAGIC     -- ⭐ FILTER OUT completed consumption (v4.3+)
# MAGIC     LEFT ANTI JOIN consumption_status_lookup cs
# MAGIC         ON cs.ComponentItemNo = c.component_item_no
# MAGIC        AND cs.ProdOrderNo = h.pro_no
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- TYPE 2: POLICY_TRIGGERED — Independent reorder plans
# MAGIC -- =====================================================
# MAGIC type2_demand AS (
# MAGIC     SELECT
# MAGIC         'POLICY_TRIGGERED'                                                   AS demand_type,
# MAGIC         p.plan_id                                                            AS source_record_id,
# MAGIC         CAST(NULL AS STRING)                                                 AS source_line_id,
# MAGIC         p.plan_status                                                        AS source_status,
# MAGIC         CAST(NULL AS STRING)                                                 AS produced_item_no,
# MAGIC         CAST(NULL AS INT)                                                    AS planning_level_code,
# MAGIC         p.archetype                                                          AS planning_level_label,
# MAGIC         p.description                                                        AS source_description,
# MAGIC         p.item_no                                                            AS component_item_no,
# MAGIC         p.description                                                        AS component_description,
# MAGIC         p.planned_qty                                                        AS required_qty,
# MAGIC         CAST(1 AS DECIMAL(38,20))                                            AS qty_per,
# MAGIC         CAST(NULL AS DECIMAL(38,20))                                         AS component_unit_cost,
# MAGIC         CAST(NULL AS DECIMAL(38,20))                                         AS component_cost_amount,
# MAGIC         CAST(NULL AS STRING)                                                 AS component_location,
# MAGIC         CASE
# MAGIC             WHEN p.need_by_date > DATE'1900-01-01' THEN p.need_by_date
# MAGIC             ELSE NULL
# MAGIC         END                                                                  AS required_date_raw,
# MAGIC         CASE
# MAGIC             WHEN p.need_by_date > DATE'1900-01-01' THEN 'PLAN_NEED_BY'
# MAGIC             ELSE 'SENTINEL_FALLBACK_TODAY_PLUS_LT'
# MAGIC         END                                                                  AS required_date_source,
# MAGIC         CAST(NULL AS DATE)                                                   AS pro_due_date,
# MAGIC         CAST(NULL AS DATE)                                                   AS line_due_date,
# MAGIC         CAST(NULL AS DATE)                                                   AS pro_starting_date,
# MAGIC         p.source_so_no,
# MAGIC         CAST(NULL AS INT)                                                    AS source_so_line_no,
# MAGIC         70                                                                   AS confidence_score
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_unified p
# MAGIC     LEFT ANTI JOIN items_with_bom b ON b.item_no = p.item_no
# MAGIC     WHERE p.plan_status NOT IN ('COVERED_BY_STOCK', 'NO_TRIGGER')
# MAGIC       AND p.plan_status NOT LIKE 'BLOCKED%'
# MAGIC       AND p.planned_qty > 0
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- TYPE 3: PLANNED_BOM
# MAGIC -- =====================================================
# MAGIC type3_plans_with_bom AS (
# MAGIC     SELECT
# MAGIC         p.plan_id,
# MAGIC         p.archetype,
# MAGIC         p.plan_status,
# MAGIC         p.item_no                          AS planned_item_no,
# MAGIC         p.description                      AS planned_item_description,
# MAGIC         p.demand_qty                       AS planned_demand_qty,
# MAGIC         p.shortage_qty                     AS planned_shortage_qty,
# MAGIC         p.need_by_date                     AS planned_need_by_date,
# MAGIC         p.suggested_order_date             AS planned_suggested_order_date,
# MAGIC         p.source_so_no
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_unified p
# MAGIC     INNER JOIN items_with_bom b ON b.item_no = p.item_no
# MAGIC     WHERE p.plan_status NOT IN ('COVERED_BY_STOCK', 'NO_TRIGGER')
# MAGIC       AND p.plan_status NOT LIKE 'BLOCKED%'
# MAGIC       AND p.demand_qty > 0
# MAGIC ),
# MAGIC 
# MAGIC bom_leaf_only AS (
# MAGIC     SELECT b.*
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_bom_exploded b
# MAGIC     LEFT ANTI JOIN Gold_Inventory_Lakehouse.mrp.gold_bom_exploded b2
# MAGIC         ON b2.parent_item = b.component_item
# MAGIC ),
# MAGIC 
# MAGIC type3_demand AS (
# MAGIC     SELECT
# MAGIC         'PLANNED_BOM'                                                        AS demand_type,
# MAGIC         p.plan_id                                                            AS source_record_id,
# MAGIC         bom.bom_path                                                         AS source_line_id,
# MAGIC         p.plan_status                                                        AS source_status,
# MAGIC         p.planned_item_no                                                    AS produced_item_no,
# MAGIC         CAST(NULL AS INT)                                                    AS planning_level_code,
# MAGIC         p.archetype                                                          AS planning_level_label,
# MAGIC         p.planned_item_description                                           AS source_description,
# MAGIC         bom.component_item                                                   AS component_item_no,
# MAGIC         bom.component_description,
# MAGIC         CAST(p.planned_demand_qty * bom.qty_per_root AS DECIMAL(38,20))      AS required_qty,
# MAGIC         bom.qty_per_root                                                     AS qty_per,
# MAGIC         CAST(NULL AS DECIMAL(38,20))                                         AS component_unit_cost,
# MAGIC         CAST(NULL AS DECIMAL(38,20))                                         AS component_cost_amount,
# MAGIC         CAST(NULL AS STRING)                                                 AS component_location,
# MAGIC         CASE
# MAGIC             WHEN p.planned_need_by_date > DATE'1900-01-01' THEN p.planned_need_by_date
# MAGIC             ELSE NULL
# MAGIC         END                                                                  AS required_date_raw,
# MAGIC         CASE
# MAGIC             WHEN p.planned_need_by_date > DATE'1900-01-01' THEN 'PLAN_NEED_BY'
# MAGIC             ELSE 'SENTINEL_FALLBACK_TODAY_PLUS_LT'
# MAGIC         END                                                                  AS required_date_source,
# MAGIC         CAST(NULL AS DATE)                                                   AS pro_due_date,
# MAGIC         CAST(NULL AS DATE)                                                   AS line_due_date,
# MAGIC         CAST(NULL AS DATE)                                                   AS pro_starting_date,
# MAGIC         p.source_so_no,
# MAGIC         CAST(NULL AS INT)                                                    AS source_so_line_no,
# MAGIC         80                                                                   AS confidence_score
# MAGIC     FROM type3_plans_with_bom p
# MAGIC     INNER JOIN bom_leaf_only bom
# MAGIC         ON bom.root_item = p.planned_item_no
# MAGIC     WHERE bom.component_item NOT LIKE 'PL-%'
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- UNION all 3 demand types
# MAGIC -- =====================================================
# MAGIC demand_unified AS (
# MAGIC     SELECT * FROM type1_demand
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM type2_demand
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM type3_demand
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- ENRICH demand with item master (smart lead time)
# MAGIC -- =====================================================
# MAGIC demand_with_lead_time AS (
# MAGIC     SELECT
# MAGIC         d.*,
# MAGIC         im.item_description,
# MAGIC         im.item_category,
# MAGIC         im.material_type,
# MAGIC         im.metal_category,
# MAGIC         im.preferred_vendor_no,
# MAGIC         im.base_uom,
# MAGIC         im.repl_system,
# MAGIC         im.reorder_policy,
# MAGIC         im.is_critical,
# MAGIC         im.last_direct_cost,
# MAGIC         im.standard_cost,
# MAGIC         CAST(COALESCE(im.lead_time_days_raw, cat.avg_lead_time_days, 14) AS INT)  AS lead_time_days,
# MAGIC         CASE
# MAGIC             WHEN im.lead_time_days_raw IS NOT NULL    THEN 'ITEM_CARD'
# MAGIC             WHEN cat.avg_lead_time_days IS NOT NULL   THEN 'CATEGORY_AVG'
# MAGIC             ELSE 'DEFAULT_14D'
# MAGIC         END                                                                  AS lead_time_source
# MAGIC     FROM demand_unified d
# MAGIC     LEFT JOIN item_master_enriched im
# MAGIC         ON im.item_no = d.component_item_no
# MAGIC     LEFT JOIN category_lead_time cat
# MAGIC         ON cat.item_category = im.item_category
# MAGIC ),
# MAGIC 
# MAGIC demand_with_required_date AS (
# MAGIC     SELECT
# MAGIC         d.*,
# MAGIC         CASE
# MAGIC             WHEN d.required_date_raw IS NOT NULL THEN d.required_date_raw
# MAGIC             ELSE DATE_ADD(CURRENT_DATE(), d.lead_time_days)
# MAGIC         END                                                                  AS required_date
# MAGIC     FROM demand_with_lead_time d
# MAGIC ),
# MAGIC 
# MAGIC demand_with_naive_order_date AS (
# MAGIC     SELECT
# MAGIC         d.*,
# MAGIC         DATE_SUB(d.required_date, d.lead_time_days)                          AS order_by_date_calendar
# MAGIC     FROM demand_with_required_date d
# MAGIC     WHERE d.required_date IS NOT NULL
# MAGIC       AND d.lead_time_days <= 180
# MAGIC ),
# MAGIC 
# MAGIC demand_with_offset AS (
# MAGIC     SELECT
# MAGIC         d.*,
# MAGIC         CAST((
# MAGIC             SELECT COUNT(*)
# MAGIC             FROM (
# MAGIC                 SELECT EXPLODE(SEQUENCE(
# MAGIC                     d.order_by_date_calendar,
# MAGIC                     d.required_date,
# MAGIC                     INTERVAL 1 DAY
# MAGIC                 )) AS dt
# MAGIC             )
# MAGIC             WHERE DAYOFWEEK(dt) IN (1, 7)
# MAGIC                OR EXISTS (SELECT 1 FROM holidays_distinct h WHERE h.holiday_date = dt)
# MAGIC         ) AS INT)                                                            AS non_working_days_in_window
# MAGIC     FROM demand_with_naive_order_date d
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- STOCK SNAPSHOT (shared across demand types)
# MAGIC -- =====================================================
# MAGIC inventory_summary AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         SUM(qty_on_hand)                                                     AS total_on_hand,
# MAGIC         SUM(qty_available)                                                   AS total_available,
# MAGIC         COUNT(DISTINCT location_code)                                        AS location_count,
# MAGIC         COUNT(DISTINCT lot_no)                                               AS lot_count
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_inventory
# MAGIC     WHERE COALESCE(is_blocked_location, false) = false
# MAGIC     GROUP BY item_no
# MAGIC ),
# MAGIC 
# MAGIC -- ⭐ v4.3 Bug 4 FIX: use Outstanding Qty. (Base) for incoming_po_qty
# MAGIC open_po_summary AS (
# MAGIC     SELECT
# MAGIC         pl.`No.`                                                             AS item_no,
# MAGIC         SUM(pl.`Outstanding Qty. (Base)`)                                    AS total_incoming_qty,
# MAGIC         MIN(pl.`Expected Receipt Date`)                                      AS earliest_incoming_date,
# MAGIC         MAX(pl.`Expected Receipt Date`)                                      AS latest_incoming_date,
# MAGIC         COUNT(DISTINCT pl.`Document No.`)                                    AS open_po_count
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Purchase Line` pl
# MAGIC     WHERE pl.`BC Company` = 'Ennovie'
# MAGIC       AND pl.`Document Type` = 'Order'
# MAGIC       AND pl.`Outstanding Quantity` > 0
# MAGIC       AND pl.`Expected Receipt Date` > DATE'1900-01-01'
# MAGIC       AND pl.Type = 'Item'
# MAGIC     GROUP BY pl.`No.`
# MAGIC )
# MAGIC 
# MAGIC -- =========================================================
# MAGIC -- Final SELECT
# MAGIC -- =========================================================
# MAGIC SELECT
# MAGIC     -- Demand classification
# MAGIC     d.demand_type,
# MAGIC     d.confidence_score,
# MAGIC 
# MAGIC     -- Source linkage
# MAGIC     d.source_record_id,
# MAGIC     d.source_line_id,
# MAGIC     d.source_status,
# MAGIC     d.produced_item_no,
# MAGIC     d.planning_level_code,
# MAGIC     d.planning_level_label,
# MAGIC     d.source_description,
# MAGIC 
# MAGIC     -- Component
# MAGIC     d.component_item_no,
# MAGIC     d.component_description,
# MAGIC     d.item_category,
# MAGIC     d.material_type,
# MAGIC     d.metal_category,
# MAGIC     d.is_critical                                AS is_critical_item,
# MAGIC     d.repl_system,
# MAGIC     d.reorder_policy,
# MAGIC     d.base_uom,
# MAGIC 
# MAGIC     -- Quantity
# MAGIC     d.required_qty,
# MAGIC     d.qty_per,
# MAGIC     d.component_location,
# MAGIC 
# MAGIC     -- Timing
# MAGIC     d.required_date,
# MAGIC     d.required_date_source,
# MAGIC     d.pro_starting_date,
# MAGIC     d.pro_due_date,
# MAGIC     d.line_due_date,
# MAGIC 
# MAGIC     -- Lead time
# MAGIC     d.lead_time_days,
# MAGIC     d.lead_time_source,
# MAGIC 
# MAGIC     -- Backward calc
# MAGIC     d.order_by_date_calendar                     AS order_by_date_naive,
# MAGIC     d.non_working_days_in_window,
# MAGIC     DATE_SUB(d.order_by_date_calendar, d.non_working_days_in_window) AS order_by_date,
# MAGIC 
# MAGIC     DATEDIFF(d.required_date, CURRENT_DATE())    AS days_until_required,
# MAGIC     DATEDIFF(
# MAGIC         DATE_SUB(d.order_by_date_calendar, d.non_working_days_in_window),
# MAGIC         CURRENT_DATE()
# MAGIC     )                                            AS days_until_order_by,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN DATE_SUB(d.order_by_date_calendar, d.non_working_days_in_window)
# MAGIC              < CURRENT_DATE()                                  THEN 'ALREADY_LATE'
# MAGIC         WHEN DATE_SUB(d.order_by_date_calendar, d.non_working_days_in_window)
# MAGIC              < DATE_ADD(CURRENT_DATE(), 7)                     THEN 'URGENT'
# MAGIC         WHEN DATE_SUB(d.order_by_date_calendar, d.non_working_days_in_window)
# MAGIC              < DATE_ADD(CURRENT_DATE(), 30)                    THEN 'PLAN_AHEAD'
# MAGIC         ELSE 'ON_TRACK'
# MAGIC     END                                                              AS order_status,
# MAGIC 
# MAGIC     -- Customer
# MAGIC     d.source_so_no,
# MAGIC     d.source_so_line_no,
# MAGIC     soe.customer_no,
# MAGIC     soe.customer_name,
# MAGIC     soe.customer_tier_code                       AS customer_tier,
# MAGIC     soe.is_dnd,
# MAGIC     soe.so_ship_date,
# MAGIC     soe.so_line_amount_thb,
# MAGIC 
# MAGIC     -- Stock
# MAGIC     COALESCE(inv.total_on_hand, 0)               AS on_hand_qty,
# MAGIC     COALESCE(inv.total_available, 0)             AS available_qty,
# MAGIC     COALESCE(inv.location_count, 0)              AS stock_location_count,
# MAGIC     COALESCE(inv.lot_count, 0)                   AS stock_lot_count,
# MAGIC 
# MAGIC     -- Incoming PO (v4.3: now in base UoM)
# MAGIC     COALESCE(po.total_incoming_qty, 0)           AS incoming_po_qty,
# MAGIC     po.earliest_incoming_date,
# MAGIC     po.latest_incoming_date,
# MAGIC     COALESCE(po.open_po_count, 0)                AS open_po_count,
# MAGIC 
# MAGIC     -- Net position
# MAGIC     COALESCE(inv.total_available, 0) + COALESCE(po.total_incoming_qty, 0)
# MAGIC         - d.required_qty                                                 AS net_position_for_this_demand,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN COALESCE(inv.total_available, 0) + COALESCE(po.total_incoming_qty, 0)
# MAGIC              >= d.required_qty                                 THEN 'COVERED'
# MAGIC         WHEN COALESCE(inv.total_available, 0) + COALESCE(po.total_incoming_qty, 0)
# MAGIC              >= d.required_qty * 0.5                           THEN 'PARTIAL'
# MAGIC         ELSE 'SHORTAGE'
# MAGIC     END                                                              AS coverage_status,
# MAGIC 
# MAGIC     -- Vendor & cost
# MAGIC     d.preferred_vendor_no,
# MAGIC     vl.vendor_name                               AS preferred_vendor_name,
# MAGIC     d.last_direct_cost,
# MAGIC     d.standard_cost,
# MAGIC     d.component_unit_cost,
# MAGIC     ROUND(
# MAGIC         d.required_qty * COALESCE(d.last_direct_cost, d.standard_cost, d.component_unit_cost, 0),
# MAGIC         2
# MAGIC     )                                                                AS estimated_order_value_thb,
# MAGIC 
# MAGIC     -- Priority score
# MAGIC     (
# MAGIC         CASE soe.customer_tier_code
# MAGIC             WHEN 'TIER_1' THEN 10
# MAGIC             WHEN 'TIER_2' THEN 5
# MAGIC             WHEN 'TIER_3' THEN 2
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC         + CASE WHEN soe.is_dnd = TRUE THEN 5 ELSE 0 END
# MAGIC         + CASE
# MAGIC             WHEN DATE_SUB(d.order_by_date_calendar, d.non_working_days_in_window)
# MAGIC                  < CURRENT_DATE()                              THEN 20
# MAGIC             WHEN DATE_SUB(d.order_by_date_calendar, d.non_working_days_in_window)
# MAGIC                  < DATE_ADD(CURRENT_DATE(), 7)                 THEN 10
# MAGIC             ELSE 0
# MAGIC           END
# MAGIC         + CASE WHEN d.is_critical = TRUE THEN 5 ELSE 0 END
# MAGIC         + CASE d.demand_type
# MAGIC             WHEN 'FIRMED_BOM'        THEN 5
# MAGIC             WHEN 'POLICY_TRIGGERED'  THEN 2
# MAGIC             ELSE 0
# MAGIC           END
# MAGIC     )                                                                AS priority_score,
# MAGIC 
# MAGIC     CURRENT_TIMESTAMP()                          AS view_built_at
# MAGIC 
# MAGIC FROM demand_with_offset d
# MAGIC LEFT JOIN so_enrichment soe
# MAGIC     ON soe.so_no = d.source_so_no
# MAGIC    AND soe.so_line_no = d.source_so_line_no
# MAGIC LEFT JOIN inventory_summary inv
# MAGIC     ON inv.item_no = d.component_item_no
# MAGIC LEFT JOIN open_po_summary po
# MAGIC     ON po.item_no = d.component_item_no
# MAGIC LEFT JOIN vendor_lookup vl
# MAGIC     ON vl.vendor_no = d.preferred_vendor_no;
# MAGIC 
# MAGIC 
# MAGIC -- ============== POST-DEPLOY VERIFICATION ==============
# MAGIC 
# MAGIC -- VC1: DI-RD-000405 should be filtered OUT (0 rows expected)
# MAGIC SELECT
# MAGIC     component_item_no,
# MAGIC     COUNT(*) AS rows,
# MAGIC     SUM(required_qty) AS total_demand
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Material_Availability
# MAGIC WHERE component_item_no = 'DI-RD-000405'
# MAGIC   AND demand_type = 'FIRMED_BOM'
# MAGIC GROUP BY component_item_no;
# MAGIC -- Expected: 0 rows (filtered by LEFT ANTI JOIN)
# MAGIC 
# MAGIC -- VC2: Aggregate check — total FIRMED_BOM demand should DECREASE
# MAGIC SELECT
# MAGIC     demand_type,
# MAGIC     COUNT(*) AS rows,
# MAGIC     SUM(required_qty) AS total_demand,
# MAGIC     SUM(incoming_po_qty) AS total_incoming,
# MAGIC     SUM(CASE WHEN coverage_status = 'SHORTAGE' THEN 1 ELSE 0 END) AS shortage_rows
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Material_Availability
# MAGIC WHERE demand_type = 'FIRMED_BOM'
# MAGIC GROUP BY demand_type;
# MAGIC -- Compare against v4.3 baseline
# MAGIC 
# MAGIC -- VC3: Timestamp verification — V6 must be AFTER unified
# MAGIC SELECT 'unified' AS table_name, MAX(unified_at) AS last_built
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_planned_orders_unified
# MAGIC UNION ALL
# MAGIC SELECT 'V6 (Material_Availability) v4.3+', MAX(view_built_at)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Material_Availability;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # nb_gold_view_traceability_chain   (Phase 5 — View 7 of 7)

# CELL ********************

# MAGIC %%sql
# MAGIC -- =====================================================================
# MAGIC -- Notebook: nb_gold_view_traceability_chain   (Phase 5 — View 7 of 7) — v1.1
# MAGIC -- Layer:    Gold MRP Engine — Public API Layer
# MAGIC -- Output:   mrp.gold_Traceability_Chain
# MAGIC --
# MAGIC -- ⚡ v1.1 CHANGES (Apr 26, 2026):
# MAGIC --   [FIX 1] Added `bom.component_item NOT LIKE 'PL-%'` filter — exclude plating
# MAGIC --           recipes (consistent with V6 v3.4+ behavior).
# MAGIC --   [FIX 2] Cell 4: SUM(DISTINCT so_line_amount_thb) was incorrectly collapsing
# MAGIC --           SO lines with coincidentally-identical values. Replaced with 2-step
# MAGIC --           CTE (dedupe SO lines first, then sum).
# MAGIC --   [FIX 3] Cell 7: Same SUM(DISTINCT) bug — same fix pattern.
# MAGIC --   [NEW]   Cell 9: Verify PL-* exclusion (sanity check, expects 0).
# MAGIC --
# MAGIC -- Purpose:
# MAGIC --   Bidirectional traceability: SO → Production → BOM (all levels) → Material
# MAGIC --   Answer questions like:
# MAGIC --     - "If component X runs short, which SOs are affected?"
# MAGIC --     - "What's revenue at risk for Customer Y?"
# MAGIC --     - "What raw materials does this PO chain consume?"
# MAGIC --
# MAGIC -- Grain: 1 row per (SO, SO_line, BOM_chain_row)
# MAGIC -- Levels: All BOM levels (1, 2, 3) — full chain visibility
# MAGIC --
# MAGIC -- Filter:
# MAGIC --   - SO status NOT IN ('Closed', 'Cancelled')
# MAGIC --   - SO qty_outstanding > 0
# MAGIC --
# MAGIC -- Estimated rows: ~52,778 (confirmed via sizing query)
# MAGIC --
# MAGIC -- Consumer: HTML report drill-down, customer risk dashboard
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 1: Setup ==============
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS mrp;
# MAGIC 
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = CORRECTED;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInWrite = CORRECTED;
# MAGIC SET spark.sql.parquet.int96RebaseModeInRead = CORRECTED;
# MAGIC SET spark.sql.parquet.int96RebaseModeInWrite = CORRECTED;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 2: Build gold_Traceability_Chain ==============
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE mrp.gold_Traceability_Chain
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH
# MAGIC -- =====================================================
# MAGIC -- Open SOs (excluding Closed/Cancelled, only outstanding)
# MAGIC -- =====================================================
# MAGIC open_sos AS (
# MAGIC     SELECT
# MAGIC         so.so_no,
# MAGIC         so.so_line_no,
# MAGIC         so.status                       AS so_status,
# MAGIC         so.is_open,
# MAGIC         so.customer_no,
# MAGIC         so.customer_name,
# MAGIC         so.customer_tier_code           AS customer_tier,
# MAGIC         so.customer_country,
# MAGIC         so.customer_priority_weight,
# MAGIC         so.is_dnd,
# MAGIC         so.dnd_exceed_date,
# MAGIC         so.salesperson_code,
# MAGIC         so.item_no                      AS fg_item_no,
# MAGIC         so.description                  AS fg_description,
# MAGIC         so.item_category                AS fg_item_category,
# MAGIC         so.item_archetype               AS fg_archetype,
# MAGIC         so.variant_code,
# MAGIC         so.location_code                AS so_location_code,
# MAGIC         so.uom                          AS so_uom,
# MAGIC         so.qty_ordered,
# MAGIC         so.qty_shipped,
# MAGIC         so.qty_outstanding,
# MAGIC         so.order_date,
# MAGIC         so.ship_date                    AS so_ship_date,
# MAGIC         so.requested_ship_date,
# MAGIC         so.promised_ship_date,
# MAGIC         so.days_until_ship,
# MAGIC         so.unit_price,
# MAGIC         so.unit_cost,
# MAGIC         so.line_amount_thb              AS so_line_amount_thb,
# MAGIC         so.gross_wt,
# MAGIC         so.is_special_order,
# MAGIC         so.is_drop_shipment
# MAGIC     FROM mrp.gold_SO so
# MAGIC     WHERE so.status NOT IN ('Closed', 'Cancelled')
# MAGIC       AND so.qty_outstanding > 0
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- Active PROs linked to these SOs
# MAGIC -- =====================================================
# MAGIC active_pros_for_so AS (
# MAGIC     SELECT
# MAGIC         h.`No.`                             AS pro_no,
# MAGIC         h.Status                            AS pro_status,
# MAGIC         CASE WHEN h.`Sales Order No.` = '' THEN NULL ELSE h.`Sales Order No.` END AS so_no,
# MAGIC         h.`Sales Order Line No.`            AS so_line_no,
# MAGIC         CASE WHEN h.`Starting Date` > DATE'1900-01-01' THEN h.`Starting Date` END AS pro_starting_date,
# MAGIC         CASE WHEN h.`Due Date`      > DATE'1900-01-01' THEN h.`Due Date`      END AS pro_due_date,
# MAGIC         h.Quantity                          AS pro_qty
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Production Order` h
# MAGIC     WHERE h.`BC Company` = 'Ennovie'
# MAGIC       AND h.Status IN ('Released', 'Firm Planned', 'Planned')
# MAGIC ),
# MAGIC 
# MAGIC pro_aggregated_per_so AS (
# MAGIC     SELECT
# MAGIC         so_no,
# MAGIC         so_line_no,
# MAGIC         COUNT(DISTINCT pro_no)              AS active_pro_count,
# MAGIC         COLLECT_SET(pro_no)                 AS pro_nos,
# MAGIC         MIN(pro_starting_date)              AS earliest_pro_start,
# MAGIC         MAX(pro_due_date)                   AS latest_pro_due,
# MAGIC         SUM(pro_qty)                        AS total_pro_qty
# MAGIC     FROM active_pros_for_so
# MAGIC     WHERE so_no IS NOT NULL
# MAGIC     GROUP BY so_no, so_line_no
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- Engine plans linked to SOs
# MAGIC -- =====================================================
# MAGIC plans_per_so AS (
# MAGIC     SELECT
# MAGIC         source_so_no                        AS so_no,
# MAGIC         item_no,
# MAGIC         COUNT(DISTINCT plan_id)             AS plan_count,
# MAGIC         SUM(demand_qty)                     AS planned_demand_qty,
# MAGIC         SUM(shortage_qty)                   AS planned_shortage_qty,
# MAGIC         COLLECT_SET(archetype)              AS plan_archetypes,
# MAGIC         MIN(need_by_date)                   AS earliest_plan_need_by
# MAGIC     FROM mrp.gold_planned_orders_unified
# MAGIC     WHERE source_so_no IS NOT NULL
# MAGIC       AND plan_status NOT IN ('COVERED_BY_STOCK', 'NO_TRIGGER')
# MAGIC       AND plan_status NOT LIKE 'BLOCKED%'
# MAGIC     GROUP BY source_so_no, item_no
# MAGIC ),
# MAGIC 
# MAGIC -- Aggregate plan info to (so_no) level (rolled up across items)
# MAGIC plans_aggregated_per_so AS (
# MAGIC     SELECT
# MAGIC         so_no,
# MAGIC         SUM(plan_count)                     AS total_plan_count,
# MAGIC         SUM(planned_demand_qty)             AS total_planned_demand,
# MAGIC         SUM(planned_shortage_qty)           AS total_planned_shortage,
# MAGIC         COUNT(DISTINCT item_no)             AS distinct_items_in_plans
# MAGIC     FROM plans_per_so
# MAGIC     GROUP BY so_no
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- Inventory snapshot
# MAGIC -- =====================================================
# MAGIC inventory_summary AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         SUM(qty_on_hand)                    AS total_on_hand,
# MAGIC         SUM(qty_available)                  AS total_available,
# MAGIC         COUNT(DISTINCT location_code)       AS location_count
# MAGIC     FROM mrp.gold_inventory
# MAGIC     WHERE COALESCE(is_blocked_location, false) = false
# MAGIC     GROUP BY item_no
# MAGIC ),
# MAGIC 
# MAGIC open_po_summary AS (
# MAGIC     SELECT
# MAGIC         pl.`No.`                            AS item_no,
# MAGIC         SUM(pl.`Outstanding Quantity`)      AS total_incoming_qty,
# MAGIC         MIN(pl.`Expected Receipt Date`)     AS earliest_incoming_date
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Purchase Line` pl
# MAGIC     WHERE pl.`BC Company` = 'Ennovie'
# MAGIC       AND pl.`Document Type` = 'Order'
# MAGIC       AND pl.`Outstanding Quantity` > 0
# MAGIC       AND pl.`Expected Receipt Date` > DATE'1900-01-01'
# MAGIC       AND pl.Type = 'Item'
# MAGIC     GROUP BY pl.`No.`
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- Item master (for component descriptions + vendor)
# MAGIC -- =====================================================
# MAGIC item_master_lite AS (
# MAGIC     SELECT
# MAGIC         i.`No.`                             AS item_no,
# MAGIC         i.Description                       AS item_description,
# MAGIC         i.`Item Category Code`              AS item_category,
# MAGIC         i.`Material Type`                   AS material_type,
# MAGIC         i.`Vendor No.`                      AS preferred_vendor_no,
# MAGIC         i.`Base Unit of Measure`            AS base_uom,
# MAGIC         i.`Replenishment System`            AS repl_system,
# MAGIC         i.`Reordering Policy`               AS reorder_policy,
# MAGIC         i.Critical                          AS is_critical,
# MAGIC         i.`Last Direct Cost`                AS last_direct_cost,
# MAGIC         i.`Standard Cost`                   AS standard_cost
# MAGIC     FROM Silver_BC_Lakehouse.bc.Item i
# MAGIC     WHERE i.`BC Company` = 'Ennovie'
# MAGIC       AND i.Blocked = false
# MAGIC ),
# MAGIC 
# MAGIC -- Detect leaf rows in BOM (component never appears as parent)
# MAGIC bom_with_leaf_flag AS (
# MAGIC     SELECT
# MAGIC         b.*,
# MAGIC         CASE WHEN NOT EXISTS (
# MAGIC             SELECT 1 FROM mrp.gold_bom_exploded b2
# MAGIC             WHERE b2.parent_item = b.component_item
# MAGIC         ) THEN TRUE ELSE FALSE END          AS is_leaf
# MAGIC     FROM mrp.gold_bom_exploded b
# MAGIC )
# MAGIC 
# MAGIC -- =========================================================
# MAGIC -- Final SELECT — SO × BOM all levels × enrichment
# MAGIC -- =========================================================
# MAGIC SELECT
# MAGIC     -- ========== Customer level ==========
# MAGIC     so.customer_no,
# MAGIC     so.customer_name,
# MAGIC     so.customer_tier,
# MAGIC     so.customer_country,
# MAGIC     so.customer_priority_weight,
# MAGIC     so.is_dnd,
# MAGIC     so.dnd_exceed_date,
# MAGIC     so.salesperson_code,
# MAGIC 
# MAGIC     -- ========== Sales Order level ==========
# MAGIC     so.so_no,
# MAGIC     so.so_line_no,
# MAGIC     so.so_status,
# MAGIC     so.so_ship_date,
# MAGIC     so.requested_ship_date,
# MAGIC     so.promised_ship_date,
# MAGIC     so.days_until_ship,
# MAGIC     so.qty_ordered                          AS so_qty_ordered,
# MAGIC     so.qty_shipped                          AS so_qty_shipped,
# MAGIC     so.qty_outstanding                      AS so_qty_outstanding,
# MAGIC     so.so_line_amount_thb,
# MAGIC     so.is_special_order,
# MAGIC     so.is_drop_shipment,
# MAGIC 
# MAGIC     -- ========== Product (FG) level ==========
# MAGIC     so.fg_item_no,
# MAGIC     so.fg_description,
# MAGIC     so.fg_item_category,
# MAGIC     so.fg_archetype,
# MAGIC     so.variant_code,
# MAGIC     so.so_uom,
# MAGIC     so.unit_price,
# MAGIC 
# MAGIC     -- ========== BOM chain level ==========
# MAGIC     bom.bom_level,
# MAGIC     bom.parent_item,
# MAGIC     bom.component_item,
# MAGIC     bom.component_description,
# MAGIC     bom.bom_path,
# MAGIC     bom.qty_per_parent,
# MAGIC     bom.qty_per_root,
# MAGIC     bom.uom                                 AS bom_uom,
# MAGIC     bom.is_leaf,
# MAGIC     -- Total qty needed for THIS SO line
# MAGIC     CAST(so.qty_outstanding * bom.qty_per_root AS DECIMAL(38,20)) AS qty_required_for_so,
# MAGIC 
# MAGIC     -- ========== Component master ==========
# MAGIC     im.item_category                        AS component_category,
# MAGIC     im.material_type                        AS component_material_type,
# MAGIC     im.preferred_vendor_no                  AS component_vendor_no,
# MAGIC     im.base_uom                             AS component_base_uom,
# MAGIC     im.repl_system                          AS component_repl_system,
# MAGIC     im.reorder_policy                       AS component_reorder_policy,
# MAGIC     im.is_critical                          AS component_is_critical,
# MAGIC 
# MAGIC     -- ========== Stock position (for component) ==========
# MAGIC     COALESCE(inv.total_on_hand, 0)          AS component_on_hand,
# MAGIC     COALESCE(inv.total_available, 0)        AS component_available,
# MAGIC     COALESCE(inv.location_count, 0)         AS component_location_count,
# MAGIC     COALESCE(po.total_incoming_qty, 0)      AS component_incoming_po,
# MAGIC     po.earliest_incoming_date               AS component_earliest_po_date,
# MAGIC 
# MAGIC     -- ========== Production tracking (per SO line) ==========
# MAGIC     COALESCE(pa.active_pro_count, 0)        AS active_pro_count_for_so,
# MAGIC     pa.pro_nos                              AS active_pro_list,
# MAGIC     pa.earliest_pro_start                   AS pro_earliest_start,
# MAGIC     pa.latest_pro_due                       AS pro_latest_due,
# MAGIC 
# MAGIC     -- ========== Engine plan tracking (per SO header) ==========
# MAGIC     COALESCE(pas.total_plan_count, 0)       AS engine_plan_count_for_so,
# MAGIC     COALESCE(pas.distinct_items_in_plans, 0) AS plan_distinct_items,
# MAGIC     pas.total_planned_shortage              AS engine_planned_shortage_total,
# MAGIC 
# MAGIC     -- ========== Procurement status (FOR THIS COMPONENT) ==========
# MAGIC     -- Coverage = on_hand + incoming vs required for this SO line
# MAGIC     CASE
# MAGIC         WHEN COALESCE(inv.total_available, 0) + COALESCE(po.total_incoming_qty, 0)
# MAGIC              >= so.qty_outstanding * bom.qty_per_root              THEN 'COVERED'
# MAGIC         WHEN COALESCE(inv.total_available, 0) + COALESCE(po.total_incoming_qty, 0)
# MAGIC              >= so.qty_outstanding * bom.qty_per_root * 0.5        THEN 'PARTIAL'
# MAGIC         ELSE 'SHORTAGE'
# MAGIC     END                                                            AS procurement_status,
# MAGIC 
# MAGIC     -- Net position
# MAGIC     CAST(
# MAGIC         COALESCE(inv.total_available, 0) + COALESCE(po.total_incoming_qty, 0)
# MAGIC         - so.qty_outstanding * bom.qty_per_root                    AS DECIMAL(38,20)
# MAGIC     )                                                              AS net_position,
# MAGIC 
# MAGIC     -- ========== Risk metrics ==========
# MAGIC     -- Shortage ratio (0 = covered, 1 = fully short)
# MAGIC     CAST(
# MAGIC         CASE
# MAGIC             WHEN bom.qty_per_root = 0 OR so.qty_outstanding = 0 THEN 0
# MAGIC             ELSE GREATEST(0,
# MAGIC                 LEAST(1,
# MAGIC                     1 - (COALESCE(inv.total_available, 0) + COALESCE(po.total_incoming_qty, 0))
# MAGIC                         / (so.qty_outstanding * bom.qty_per_root)
# MAGIC                 )
# MAGIC             )
# MAGIC         END                                                        AS DECIMAL(10,4)
# MAGIC     )                                                              AS shortage_ratio,
# MAGIC 
# MAGIC     -- Revenue at risk = SO line amount × shortage ratio (per component)
# MAGIC     -- NOTE: This is per component — for SO total revenue at risk, take MAX across components
# MAGIC     ROUND(
# MAGIC         so.so_line_amount_thb *
# MAGIC         CASE
# MAGIC             WHEN bom.qty_per_root = 0 OR so.qty_outstanding = 0 THEN 0
# MAGIC             ELSE GREATEST(0,
# MAGIC                 LEAST(1,
# MAGIC                     1 - (COALESCE(inv.total_available, 0) + COALESCE(po.total_incoming_qty, 0))
# MAGIC                         / (so.qty_outstanding * bom.qty_per_root)
# MAGIC                 )
# MAGIC             )
# MAGIC         END,
# MAGIC         2
# MAGIC     )                                                              AS revenue_at_risk_thb,
# MAGIC 
# MAGIC     -- Priority score (for HTML sorting)
# MAGIC     (
# MAGIC         CASE so.customer_tier
# MAGIC             WHEN 'TIER_1' THEN 10
# MAGIC             WHEN 'TIER_2' THEN 5
# MAGIC             WHEN 'TIER_3' THEN 2
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC         + CASE WHEN so.is_dnd = TRUE THEN 5 ELSE 0 END
# MAGIC         + CASE
# MAGIC             WHEN so.so_ship_date < CURRENT_DATE() THEN 20      -- already overdue ship
# MAGIC             WHEN so.so_ship_date < DATE_ADD(CURRENT_DATE(), 7) THEN 10
# MAGIC             ELSE 0
# MAGIC           END
# MAGIC         + CASE
# MAGIC             WHEN COALESCE(inv.total_available, 0) + COALESCE(po.total_incoming_qty, 0)
# MAGIC                  < so.qty_outstanding * bom.qty_per_root           THEN 5  -- shortage
# MAGIC             ELSE 0
# MAGIC           END
# MAGIC         + CASE WHEN bom.is_leaf = TRUE THEN 3 ELSE 0 END           -- leaf = procurement urgency
# MAGIC         + CASE WHEN im.is_critical = TRUE THEN 5 ELSE 0 END
# MAGIC     )                                                              AS priority_score,
# MAGIC 
# MAGIC     -- Metadata
# MAGIC     CURRENT_TIMESTAMP()                     AS view_built_at
# MAGIC 
# MAGIC FROM open_sos so
# MAGIC INNER JOIN bom_with_leaf_flag bom
# MAGIC     ON bom.root_item = so.fg_item_no
# MAGIC    AND bom.component_item NOT LIKE 'PL-%'        -- ⚡ v1.1: exclude plating recipes (consistent with V6)
# MAGIC LEFT JOIN item_master_lite im
# MAGIC     ON im.item_no = bom.component_item
# MAGIC LEFT JOIN inventory_summary inv
# MAGIC     ON inv.item_no = bom.component_item
# MAGIC LEFT JOIN open_po_summary po
# MAGIC     ON po.item_no = bom.component_item
# MAGIC LEFT JOIN pro_aggregated_per_so pa
# MAGIC     ON pa.so_no = so.so_no AND pa.so_line_no = so.so_line_no
# MAGIC LEFT JOIN plans_aggregated_per_so pas
# MAGIC     ON pas.so_no = so.so_no;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 3: Validation — overall stats ==============
# MAGIC 
# MAGIC SELECT
# MAGIC     COUNT(*)                                              AS total_rows,
# MAGIC     COUNT(DISTINCT so_no)                                 AS distinct_sos,
# MAGIC     COUNT(DISTINCT customer_no)                           AS distinct_customers,
# MAGIC     COUNT(DISTINCT fg_item_no)                            AS distinct_fg_items,
# MAGIC     COUNT(DISTINCT component_item)                        AS distinct_components,
# MAGIC     SUM(CASE WHEN bom_level = 1 THEN 1 ELSE 0 END)        AS level_1_rows,
# MAGIC     SUM(CASE WHEN bom_level = 2 THEN 1 ELSE 0 END)        AS level_2_rows,
# MAGIC     SUM(CASE WHEN bom_level = 3 THEN 1 ELSE 0 END)        AS level_3_rows,
# MAGIC     SUM(CASE WHEN is_leaf = TRUE THEN 1 ELSE 0 END)       AS leaf_rows,
# MAGIC     SUM(CASE WHEN procurement_status = 'SHORTAGE' THEN 1 ELSE 0 END) AS shortage_rows,
# MAGIC     SUM(CASE WHEN is_dnd = TRUE THEN 1 ELSE 0 END)        AS dnd_rows,
# MAGIC     ROUND(SUM(so_line_amount_thb), 2)                     AS total_so_value_thb,
# MAGIC     ROUND(SUM(revenue_at_risk_thb), 2)                    AS total_revenue_at_risk_thb
# MAGIC FROM mrp.gold_Traceability_Chain;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 4: Coverage by customer tier ==============
# MAGIC -- v1.1 FIX: SUM(DISTINCT so_line_amount_thb) was wrong — collapsed SO lines with
# MAGIC --           coincidentally identical values. Now: dedupe to SO line grain first,
# MAGIC --           then sum.
# MAGIC 
# MAGIC WITH so_line_unique AS (
# MAGIC     -- One row per SO line with its values
# MAGIC     SELECT DISTINCT
# MAGIC         customer_tier,
# MAGIC         so_no,
# MAGIC         so_line_no,
# MAGIC         so_line_amount_thb,
# MAGIC         is_dnd
# MAGIC     FROM mrp.gold_Traceability_Chain
# MAGIC ),
# MAGIC chain_aggregated AS (
# MAGIC     SELECT
# MAGIC         customer_tier,
# MAGIC         COUNT(DISTINCT so_no)                                 AS distinct_sos,
# MAGIC         COUNT(DISTINCT customer_no)                           AS distinct_customers,
# MAGIC         COUNT(*)                                              AS total_chain_rows,
# MAGIC         SUM(CASE WHEN procurement_status = 'COVERED'  THEN 1 ELSE 0 END) AS covered_rows,
# MAGIC         SUM(CASE WHEN procurement_status = 'PARTIAL'  THEN 1 ELSE 0 END) AS partial_rows,
# MAGIC         SUM(CASE WHEN procurement_status = 'SHORTAGE' THEN 1 ELSE 0 END) AS shortage_rows
# MAGIC     FROM mrp.gold_Traceability_Chain
# MAGIC     GROUP BY customer_tier
# MAGIC ),
# MAGIC so_value_aggregated AS (
# MAGIC     SELECT
# MAGIC         customer_tier,
# MAGIC         ROUND(SUM(so_line_amount_thb), 2)                     AS so_value_thb,
# MAGIC         SUM(CASE WHEN is_dnd THEN 1 ELSE 0 END)               AS dnd_so_lines
# MAGIC     FROM so_line_unique
# MAGIC     GROUP BY customer_tier
# MAGIC ),
# MAGIC revenue_at_risk_aggregated AS (
# MAGIC     -- Worst-case revenue at risk per SO line = MAX(shortage_ratio) on leaf × so_line_amount
# MAGIC     SELECT
# MAGIC         customer_tier,
# MAGIC         ROUND(SUM(
# MAGIC             so_line_amount_thb *
# MAGIC             COALESCE(max_leaf_shortage_ratio, 0)
# MAGIC         ), 2)                                                 AS revenue_at_risk_thb
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             customer_tier,
# MAGIC             so_no,
# MAGIC             so_line_no,
# MAGIC             ANY_VALUE(so_line_amount_thb)               AS so_line_amount_thb,
# MAGIC             MAX(CASE WHEN is_leaf THEN shortage_ratio ELSE 0 END) AS max_leaf_shortage_ratio
# MAGIC         FROM mrp.gold_Traceability_Chain
# MAGIC         GROUP BY customer_tier, so_no, so_line_no
# MAGIC     )
# MAGIC     GROUP BY customer_tier
# MAGIC )
# MAGIC SELECT
# MAGIC     ca.customer_tier,
# MAGIC     ca.distinct_sos,
# MAGIC     ca.distinct_customers,
# MAGIC     ca.total_chain_rows,
# MAGIC     ca.covered_rows,
# MAGIC     ca.partial_rows,
# MAGIC     ca.shortage_rows,
# MAGIC     sva.dnd_so_lines,
# MAGIC     sva.so_value_thb,
# MAGIC     rar.revenue_at_risk_thb,
# MAGIC     ROUND(100.0 * rar.revenue_at_risk_thb / NULLIF(sva.so_value_thb, 0), 1)
# MAGIC                                                               AS pct_revenue_at_risk
# MAGIC FROM chain_aggregated ca
# MAGIC LEFT JOIN so_value_aggregated sva       ON sva.customer_tier = ca.customer_tier
# MAGIC LEFT JOIN revenue_at_risk_aggregated rar ON rar.customer_tier = ca.customer_tier
# MAGIC ORDER BY rar.revenue_at_risk_thb DESC NULLS LAST;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 5: Top 30 SOs at material risk ==============
# MAGIC -- Use leaf-level only (procurement-relevant) and aggregate per SO
# MAGIC 
# MAGIC WITH so_aggregated AS (
# MAGIC     SELECT
# MAGIC         so_no,
# MAGIC         so_line_no,
# MAGIC         ANY_VALUE(customer_name)            AS customer,
# MAGIC         ANY_VALUE(customer_tier)            AS tier,
# MAGIC         ANY_VALUE(is_dnd)                   AS is_dnd,
# MAGIC         ANY_VALUE(fg_item_no)               AS fg_item,
# MAGIC         ANY_VALUE(fg_description)           AS fg_description,
# MAGIC         ANY_VALUE(so_qty_outstanding)       AS qty_outstanding,
# MAGIC         ANY_VALUE(so_ship_date)             AS ship_date,
# MAGIC         ANY_VALUE(so_line_amount_thb)       AS so_line_value_thb,
# MAGIC         ANY_VALUE(active_pro_count_for_so)  AS active_pros,
# MAGIC         ANY_VALUE(engine_plan_count_for_so) AS engine_plans,
# MAGIC         -- Leaf-level metrics
# MAGIC         SUM(CASE WHEN is_leaf AND procurement_status = 'SHORTAGE' THEN 1 ELSE 0 END) AS leaf_shortage_count,
# MAGIC         SUM(CASE WHEN is_leaf THEN 1 ELSE 0 END)                                     AS leaf_total_count,
# MAGIC         MAX(CASE WHEN is_leaf THEN shortage_ratio ELSE 0 END)                        AS max_leaf_shortage_ratio,
# MAGIC         -- Revenue at risk = MAX shortage ratio × so_line_value (worst-component drives risk)
# MAGIC         ROUND(ANY_VALUE(so_line_amount_thb) *
# MAGIC               MAX(CASE WHEN is_leaf THEN shortage_ratio ELSE 0 END), 2)             AS so_revenue_at_risk_thb,
# MAGIC         MAX(priority_score)                 AS max_priority_score
# MAGIC     FROM mrp.gold_Traceability_Chain
# MAGIC     GROUP BY so_no, so_line_no
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM so_aggregated
# MAGIC WHERE leaf_shortage_count > 0
# MAGIC ORDER BY so_revenue_at_risk_thb DESC, max_priority_score DESC
# MAGIC LIMIT 30;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 6: Material → Customer impact map ==============
# MAGIC -- Reverse view: which materials affect which customers most?
# MAGIC 
# MAGIC WITH leaf_only AS (
# MAGIC     SELECT *
# MAGIC     FROM mrp.gold_Traceability_Chain
# MAGIC     WHERE is_leaf = TRUE
# MAGIC       AND procurement_status IN ('SHORTAGE', 'PARTIAL')
# MAGIC )
# MAGIC SELECT
# MAGIC     component_item                          AS material_item,
# MAGIC     component_description                   AS material_description,
# MAGIC     component_category,
# MAGIC     component_vendor_no,
# MAGIC     COUNT(DISTINCT so_no)                   AS impacted_so_count,
# MAGIC     COUNT(DISTINCT customer_no)             AS impacted_customer_count,
# MAGIC     COLLECT_SET(customer_name)              AS impacted_customers,
# MAGIC     SUM(qty_required_for_so)                AS total_qty_required,
# MAGIC     ANY_VALUE(component_on_hand)            AS material_on_hand,
# MAGIC     ANY_VALUE(component_incoming_po)        AS material_incoming,
# MAGIC     ROUND(SUM(revenue_at_risk_thb), 2)      AS total_revenue_at_risk_thb
# MAGIC FROM leaf_only
# MAGIC GROUP BY component_item, component_description, component_category, component_vendor_no
# MAGIC HAVING SUM(revenue_at_risk_thb) > 0
# MAGIC ORDER BY total_revenue_at_risk_thb DESC NULLS LAST
# MAGIC LIMIT 30;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 7: Top customers by revenue at risk ==============
# MAGIC -- v1.1 FIX: SUM(DISTINCT so_line_amount_thb) bug — replaced with proper
# MAGIC --           2-step aggregation (dedupe to SO line first, then sum).
# MAGIC 
# MAGIC WITH so_line_unique AS (
# MAGIC     -- One row per SO line per customer with its value + worst shortage
# MAGIC     SELECT
# MAGIC         customer_no,
# MAGIC         ANY_VALUE(customer_name)                AS customer_name,
# MAGIC         ANY_VALUE(customer_tier)                AS tier,
# MAGIC         so_no,
# MAGIC         so_line_no,
# MAGIC         ANY_VALUE(so_line_amount_thb)           AS so_line_amount_thb,
# MAGIC         ANY_VALUE(fg_item_no)                   AS fg_item_no,
# MAGIC         ANY_VALUE(is_dnd)                       AS is_dnd,
# MAGIC         MAX(CASE WHEN is_leaf THEN shortage_ratio ELSE 0 END) AS max_leaf_shortage_ratio,
# MAGIC         SUM(CASE WHEN is_leaf AND procurement_status = 'SHORTAGE' THEN 1 ELSE 0 END) AS leaf_shortage_count
# MAGIC     FROM mrp.gold_Traceability_Chain
# MAGIC     GROUP BY customer_no, so_no, so_line_no
# MAGIC ),
# MAGIC customer_aggregated AS (
# MAGIC     SELECT
# MAGIC         customer_no,
# MAGIC         ANY_VALUE(customer_name)                            AS customer_name,
# MAGIC         ANY_VALUE(tier)                                     AS tier,
# MAGIC         COUNT(DISTINCT so_no)                               AS distinct_sos,
# MAGIC         COUNT(DISTINCT fg_item_no)                          AS distinct_fg_items,
# MAGIC         ROUND(SUM(so_line_amount_thb), 2)                   AS total_open_so_value_thb,
# MAGIC         ROUND(SUM(so_line_amount_thb * max_leaf_shortage_ratio), 2) AS total_revenue_at_risk_thb,
# MAGIC         SUM(CASE WHEN is_dnd THEN 1 ELSE 0 END)             AS dnd_so_lines,
# MAGIC         SUM(leaf_shortage_count)                            AS leaf_shortage_count
# MAGIC     FROM so_line_unique
# MAGIC     GROUP BY customer_no
# MAGIC )
# MAGIC SELECT
# MAGIC     customer_no,
# MAGIC     customer_name,
# MAGIC     tier,
# MAGIC     distinct_sos,
# MAGIC     distinct_fg_items,
# MAGIC     total_open_so_value_thb,
# MAGIC     total_revenue_at_risk_thb,
# MAGIC     ROUND(100.0 * total_revenue_at_risk_thb / NULLIF(total_open_so_value_thb, 0), 1)
# MAGIC                                                             AS pct_revenue_at_risk,
# MAGIC     dnd_so_lines,
# MAGIC     leaf_shortage_count
# MAGIC FROM customer_aggregated
# MAGIC ORDER BY total_revenue_at_risk_thb DESC NULLS LAST
# MAGIC LIMIT 30;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 8: BOM level distribution sanity check ==============
# MAGIC 
# MAGIC SELECT
# MAGIC     bom_level,
# MAGIC     is_leaf,
# MAGIC     COUNT(*)                                AS rows,
# MAGIC     COUNT(DISTINCT so_no)                   AS distinct_sos,
# MAGIC     COUNT(DISTINCT component_item)          AS distinct_components,
# MAGIC     SUM(CASE WHEN procurement_status = 'SHORTAGE' THEN 1 ELSE 0 END) AS shortage_rows
# MAGIC FROM mrp.gold_Traceability_Chain
# MAGIC GROUP BY bom_level, is_leaf
# MAGIC ORDER BY bom_level, is_leaf;
# MAGIC 
# MAGIC 
# MAGIC -- ============== CELL 9 [v1.1]: Verify PL-* exclusion ==============
# MAGIC -- Expected: 0 rows (PL-* recipes filtered out at JOIN)
# MAGIC 
# MAGIC SELECT
# MAGIC     'PL-* in V7 (should be 0)'      AS check_name,
# MAGIC     COUNT(*)                         AS count_should_be_zero,
# MAGIC     COUNT(DISTINCT component_item)   AS distinct_pl_items
# MAGIC FROM mrp.gold_Traceability_Chain
# MAGIC WHERE component_item LIKE 'PL-%';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold_Inventory_Lakehouse.mrp.gold_pur_item_summary

# CELL ********************

# MAGIC %%sql
# MAGIC -- ============================================================================
# MAGIC -- nb_gold_pur_summary · SINGLE-CELL VERSION (v9.4.9 compatible)
# MAGIC -- ============================================================================
# MAGIC -- Paste entire content into ONE Spark notebook cell, then "Run cell"
# MAGIC -- 
# MAGIC -- What it does (in order):
# MAGIC --   1. Build gold_pur_item_summary  (~1,518 rows, item-level)
# MAGIC --   2. Build gold_pur_bucket_detail (~2,842 rows, bucket-level + flags)
# MAGIC --   3. Update v_pur_item_summary view to point to new table
# MAGIC --   4. Verify with FCB-000591-BR
# MAGIC --
# MAGIC -- Date: 2026-05-05 (updated for v9.4.9)
# MAGIC -- Source: fabric_requisition_line v9.4.9
# MAGIC -- 
# MAGIC -- v9.4.9 ADDITIONS (4 UI enrichment cols carried through to Gold tables):
# MAGIC --   - best_stock_location
# MAGIC --   - replenishment_system  
# MAGIC --   - item_total_demand_uom1
# MAGIC --   - item_total_firmed_demand_uom2
# MAGIC -- ============================================================================
# MAGIC 
# MAGIC -- Environment
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInWrite = CORRECTED;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead  = CORRECTED;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- TABLE A · gold_pur_item_summary (item-level)
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.gold_pur_item_summary
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'false',
# MAGIC   'description' = 'PUR-facing item summary, materialized from fabric_requisition_line v9.4.6 with v9.4.7 policy-aware aggregation. 1 row per item.'
# MAGIC )
# MAGIC AS
# MAGIC WITH item_rows AS (
# MAGIC   SELECT *
# MAGIC   FROM Gold_Inventory_Lakehouse.mrp.fabric_requisition_line
# MAGIC   WHERE should_trigger = TRUE
# MAGIC     AND final_uom1 > 0
# MAGIC ),
# MAGIC ranked AS (
# MAGIC   SELECT
# MAGIC     item_rows.*,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY item_no
# MAGIC       ORDER BY bucket_start_date ASC
# MAGIC     ) AS bucket_rank,
# MAGIC     FIRST_VALUE(final_uom1) OVER (
# MAGIC       PARTITION BY item_no
# MAGIC       ORDER BY bucket_start_date ASC
# MAGIC       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
# MAGIC     ) AS earliest_bucket_final_uom1,
# MAGIC     FIRST_VALUE(final_uom2) OVER (
# MAGIC       PARTITION BY item_no
# MAGIC       ORDER BY bucket_start_date ASC
# MAGIC       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
# MAGIC     ) AS earliest_bucket_final_uom2,
# MAGIC     FIRST_VALUE(suggested_qty_if_open_released) OVER (
# MAGIC       PARTITION BY item_no
# MAGIC       ORDER BY bucket_start_date ASC
# MAGIC       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
# MAGIC     ) AS earliest_bucket_qty_if_open_released
# MAGIC   FROM item_rows
# MAGIC )
# MAGIC SELECT
# MAGIC   -- Identity
# MAGIC   item_no,
# MAGIC   MAX(description)              AS description,
# MAGIC   MAX(item_category_code)       AS item_category_code,
# MAGIC   MAX(item_uom2_code)           AS item_uom2_code,
# MAGIC   MAX(base_uom_uom1)            AS base_uom_uom1,
# MAGIC   MAX(module)                   AS module,
# MAGIC   MAX(archetype)                AS archetype,
# MAGIC   MAX(reordering_policy)        AS reordering_policy,
# MAGIC   MAX(vendor_no)                AS vendor_no,
# MAGIC   MAX(vendor_name)              AS vendor_name,
# MAGIC   
# MAGIC   -- Bucket window
# MAGIC   MIN(bucket_week)              AS earliest_bucket,
# MAGIC   MAX(bucket_week)              AS latest_bucket,
# MAGIC   CONCAT(MIN(bucket_week), ' → ', MAX(bucket_week)) AS bucket_window_display,
# MAGIC   COUNT(*)                      AS bucket_count,
# MAGIC   
# MAGIC   -- Demand (always SUM)
# MAGIC   SUM(demand_quantity_uom1_bucket) AS qty_uom1,
# MAGIC   SUM(firmed_bom_qty_uom2)         AS qty_uom2,
# MAGIC   
# MAGIC   -- Stock state (item-level)
# MAGIC   MAX(onhand_quantity_uom1)        AS onhand_uom1,
# MAGIC   MAX(onhand_quantity_uom2)        AS onhand_uom2,
# MAGIC   MAX(available_qty_atp_uom1)      AS available_atp_uom1,
# MAGIC   MAX(available_qty_atp_uom2)      AS available_atp_uom2,
# MAGIC   MAX(incoming_po_released_qty)    AS incoming_released_uom1,
# MAGIC   MAX(incoming_po_open_qty)        AS incoming_open_uom1,
# MAGIC   MAX(incoming_po_released_qty_uom2) AS incoming_released_uom2,
# MAGIC   MAX(incoming_po_open_qty_uom2)   AS incoming_open_uom2,
# MAGIC   CONCAT('R: ', ROUND(MAX(incoming_po_released_qty), 2), 
# MAGIC          ' | O: ', ROUND(MAX(incoming_po_open_qty), 2)) AS incoming_uom1_display,
# MAGIC   CONCAT('R: ', ROUND(MAX(incoming_po_released_qty_uom2), 2), 
# MAGIC          ' | O: ', ROUND(MAX(incoming_po_open_qty_uom2), 2)) AS incoming_uom2_display,
# MAGIC   MIN(earliest_po_receipt_date)    AS po_receipt_date,
# MAGIC   MAX(CASE WHEN has_pending_open_po THEN 1 ELSE 0 END) = 1 AS has_pending_open_po,
# MAGIC   
# MAGIC   -- Suggested order qty · POLICY-AWARE ★ THE FIX ★
# MAGIC   CASE 
# MAGIC     WHEN MAX(reordering_policy) IN ('Fixed Reorder Qty.', 'Maximum Qty.') 
# MAGIC       THEN MAX(earliest_bucket_final_uom1)
# MAGIC     ELSE 
# MAGIC       SUM(final_uom1)
# MAGIC   END AS final_uom1,
# MAGIC   
# MAGIC   CASE 
# MAGIC     WHEN MAX(reordering_policy) IN ('Fixed Reorder Qty.', 'Maximum Qty.') 
# MAGIC       THEN MAX(earliest_bucket_final_uom2)
# MAGIC     ELSE 
# MAGIC       SUM(final_uom2)
# MAGIC   END AS final_uom2,
# MAGIC   
# MAGIC   CASE 
# MAGIC     WHEN MAX(reordering_policy) IN ('Fixed Reorder Qty.', 'Maximum Qty.') 
# MAGIC       THEN MAX(earliest_bucket_qty_if_open_released)
# MAGIC     ELSE 
# MAGIC       SUM(suggested_qty_if_open_released)
# MAGIC   END AS suggested_qty_if_open_released,
# MAGIC   
# MAGIC   CONCAT(MAX(base_uom_uom1), ' / ', MAX(item_uom2_code)) AS uoms_display,
# MAGIC   
# MAGIC   -- Alerts
# MAGIC   MAX(alert_flag)                  AS alert_flag,
# MAGIC   MAX(CASE alert_flag
# MAGIC         WHEN 'CRITICAL' THEN 5 WHEN 'HIGH' THEN 4
# MAGIC         WHEN 'MEDIUM' THEN 3 WHEN 'LOW' THEN 2
# MAGIC         WHEN 'INFO' THEN 1 ELSE 0
# MAGIC       END)                         AS alert_severity,
# MAGIC   MIN(projected_stockout_date)     AS projected_stockout_date,
# MAGIC   MIN(suggested_order_date_actionable) AS order_due_date,
# MAGIC   MAX(CASE po_urgency
# MAGIC         WHEN 'OVERDUE' THEN 5 WHEN 'URGENT' THEN 4
# MAGIC         WHEN 'HIGH' THEN 3 WHEN 'NORMAL' THEN 2
# MAGIC         WHEN 'LOW' THEN 1 ELSE 0
# MAGIC       END)                         AS po_urgency_severity,
# MAGIC   
# MAGIC   -- Cost · POLICY-AWARE
# MAGIC   MAX(unit_cost)                   AS unit_cost,
# MAGIC   MAX(unit_cost_source)            AS unit_cost_source,
# MAGIC   CASE 
# MAGIC     WHEN MAX(reordering_policy) IN ('Fixed Reorder Qty.', 'Maximum Qty.') 
# MAGIC       THEN MAX(earliest_bucket_final_uom1) * MAX(unit_cost)
# MAGIC     ELSE 
# MAGIC       SUM(cost_amount)
# MAGIC   END AS est_cost,
# MAGIC   
# MAGIC   -- Forecast
# MAGIC   MAX(forecast_demand_60d)         AS forecast_demand_60d,
# MAGIC   MAX(avg_daily_usage)             AS avg_daily_usage,
# MAGIC   
# MAGIC   -- Metadata
# MAGIC   MAX(customer_no)                 AS customer_no,
# MAGIC   MAX(CASE WHEN is_dnd THEN 1 ELSE 0 END) = 1 AS is_dnd,
# MAGIC   MAX(CASE WHEN is_critical THEN 1 ELSE 0 END) = 1 AS is_critical,
# MAGIC   MAX(priority_score)              AS priority_score,
# MAGIC   
# MAGIC   -- ⭐ v9.4.9: UI enrichment cols (carry through from engine)
# MAGIC   MAX(best_stock_location)         AS best_stock_location,
# MAGIC   MAX(replenishment_system)        AS replenishment_system,
# MAGIC   MAX(item_total_demand_uom1)      AS item_total_demand_uom1,
# MAGIC   MAX(item_total_firmed_demand_uom2) AS item_total_firmed_demand_uom2,
# MAGIC   
# MAGIC   MAX(source_version)              AS source_version,
# MAGIC   current_timestamp()              AS materialized_at
# MAGIC FROM ranked
# MAGIC GROUP BY item_no;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- TABLE B · gold_pur_bucket_detail (bucket-level + dedup helpers)
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.gold_pur_bucket_detail
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'false',
# MAGIC   'description' = 'PUR per-bucket detail with dedup helpers. Use is_earliest_bucket=TRUE for ROP items to avoid double-counting in SUM.'
# MAGIC )
# MAGIC AS
# MAGIC SELECT
# MAGIC   f.*,
# MAGIC   CASE WHEN ROW_NUMBER() OVER (
# MAGIC       PARTITION BY f.item_no 
# MAGIC       ORDER BY f.bucket_start_date ASC
# MAGIC   ) = 1 THEN TRUE ELSE FALSE END AS is_earliest_bucket,
# MAGIC   CASE 
# MAGIC       WHEN f.reordering_policy IN ('Fixed Reorder Qty.', 'Maximum Qty.')
# MAGIC         THEN 'POLICY_DRIVEN'
# MAGIC       ELSE 'DEMAND_DRIVEN'
# MAGIC   END AS dedup_class,
# MAGIC   current_timestamp() AS materialized_at
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.fabric_requisition_line f
# MAGIC WHERE f.should_trigger = TRUE 
# MAGIC   AND f.final_uom1 > 0;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- VIEW · point v_pur_item_summary to new table (backward compat)
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE VIEW Gold_Inventory_Lakehouse.mrp.v_pur_item_summary AS
# MAGIC SELECT *
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_pur_item_summary;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- VERIFY · canary FCB-000591-BR + row counts (last query, returns to user)
# MAGIC -- ============================================================================
# MAGIC SELECT
# MAGIC     '✅ DEPLOYED v9.4.9' AS status,
# MAGIC     (SELECT COUNT(*) FROM Gold_Inventory_Lakehouse.mrp.gold_pur_item_summary) AS item_summary_rows,
# MAGIC     (SELECT COUNT(*) FROM Gold_Inventory_Lakehouse.mrp.gold_pur_bucket_detail) AS bucket_detail_rows,
# MAGIC     (SELECT final_uom1 FROM Gold_Inventory_Lakehouse.mrp.gold_pur_item_summary
# MAGIC      WHERE item_no = 'FCB-000591-BR') AS canary_final_uom1,
# MAGIC     (SELECT best_stock_location FROM Gold_Inventory_Lakehouse.mrp.gold_pur_item_summary
# MAGIC      WHERE item_no = 'FCB-000591-BR') AS canary_location,
# MAGIC     (SELECT replenishment_system FROM Gold_Inventory_Lakehouse.mrp.gold_pur_item_summary
# MAGIC      WHERE item_no = 'FCB-000591-BR') AS canary_repl,
# MAGIC     (SELECT item_total_demand_uom1 FROM Gold_Inventory_Lakehouse.mrp.gold_pur_item_summary
# MAGIC      WHERE item_no = 'FCB-000591-BR') AS canary_total_demand,
# MAGIC     CASE WHEN (SELECT final_uom1 FROM Gold_Inventory_Lakehouse.mrp.gold_pur_item_summary
# MAGIC                WHERE item_no = 'FCB-000591-BR') = 8000
# MAGIC          THEN '✅ canary OK'
# MAGIC          ELSE '❌ canary FAIL'
# MAGIC     END AS canary_check;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # fabric_requisition_line v9.4.19

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- =====================================================================
# MAGIC -- 📦 fabric_requisition_line v9.4.19 — Cell 2 (FULL NOTEBOOK)
# MAGIC -- =====================================================================
# MAGIC -- Run AFTER Cell 1 (UDFs registered)
# MAGIC --
# MAGIC -- 🆕 v9.4.19 CHANGES (from v9.4.18):
# MAGIC --
# MAGIC --   🔧 ENGINE FIX: Disable ROP-only refill
# MAGIC --      
# MAGIC --      RATIONALE:
# MAGIC --        Phloy's intent (since v9.4.16): "demand-driven only — no policy refill"
# MAGIC --        However v9.4.16/17/18 still kept ROP refill branch in Fixed Reorder
# MAGIC --        Qty. policy, which triggered orders for items with on_hand <= ROP
# MAGIC --        regardless of FIRMED demand.
# MAGIC --      
# MAGIC --      EVIDENCE:
# MAGIC --        GE-PL-001780 (Pearls · Fixed Reorder Qty. policy):
# MAGIC --          - firmed_bom_qty = 0 (no FIRMED demand)
# MAGIC --          - on_hand = 1, ROP > 1
# MAGIC --          - v9.4.18: should_trigger = TRUE, final_uom1 = 17 (ROP refill)
# MAGIC --          - v9.4.19: should_trigger = FALSE, final_uom1 = 0
# MAGIC --        
# MAGIC --        Demand source displayed as POLICY_TRIGGERED with UUID PRO ref —
# MAGIC --        not a real BC PRO, confusing for Purchasing team.
# MAGIC --      
# MAGIC --      FIX:
# MAGIC --        Layer 5 v6_with_lot_sizing — remove ROP-refill OR branch:
# MAGIC --        
# MAGIC --        BEFORE (v9.4.18):
# MAGIC --          WHEN reordering_policy = 'Fixed Reorder Qty.' THEN
# MAGIC --              (onhand_quantity_uom1 <= COALESCE(reorder_point_uom1, 0) AND bucket_rank = 1)
# MAGIC --              OR shortage_quantity_uom1 > 0
# MAGIC --        
# MAGIC --        AFTER (v9.4.19):
# MAGIC --          WHEN reordering_policy = 'Fixed Reorder Qty.' THEN
# MAGIC --              shortage_quantity_uom1 > 0
# MAGIC --      
# MAGIC --        Same fix applied to:
# MAGIC --          - policy_quantity_uom1 (lot-sized order qty)
# MAGIC --          - policy_qty_if_open_released (with open PO scenario)
# MAGIC --      
# MAGIC --      IMPACT:
# MAGIC --        - 115 Fixed Reorder Qty. items no longer auto-refill on ROP
# MAGIC --        - Only refill when FIRMED demand creates actual shortage
# MAGIC --        - Pearls/Findings/Solder buffer items: rely on Planned PRO timeline
# MAGIC --          to convert to FIRMED early enough for lead time
# MAGIC --      
# MAGIC --      WHAT'S PRESERVED:
# MAGIC --        - L4L policy: still triggers on shortage (unchanged)
# MAGIC --        - All policies: still ROUND UP via min_order_qty / max_order_qty /
# MAGIC --          order_multiple (lot sizing intact)
# MAGIC --        - is_dnd, customer enrichment, all v9.4.16-18 logic
# MAGIC --
# MAGIC -- 🛡️ Changes scope vs v9.4.18: 1 CTE touched
# MAGIC --   - Layer 5 v6_with_lot_sizing — 3 CASE branches simplified
# MAGIC -- All other logic UNCHANGED.
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC 
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = LEGACY;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInWrite = LEGACY;
# MAGIC SET spark.sql.parquet.int96RebaseModeInRead = LEGACY;
# MAGIC SET spark.sql.parquet.int96RebaseModeInWrite = LEGACY;
# MAGIC 
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.fabric_requisition_line
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- LAYER 0: Ship date lookup (UNCHANGED)
# MAGIC -- =====================================================
# MAGIC production_order_dedup AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `No.`, Status, `BC Company`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn_po
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Production Order`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND Status IN ('Released', 'Firm Planned', 'Planned')
# MAGIC           AND `Sales Order No.` IS NOT NULL
# MAGIC           AND `Sales Order No.` <> ''
# MAGIC     )
# MAGIC     WHERE rn_po = 1
# MAGIC ),
# MAGIC 
# MAGIC sales_line_dedup AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `Document No.`, `Document Type`, `Line No.`, `BC Company`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn_sl
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Sales Line`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND `Document Type` = 'Order'
# MAGIC           AND Type = 'Item'
# MAGIC           AND `Outstanding Quantity` > 0
# MAGIC     )
# MAGIC     WHERE rn_sl = 1
# MAGIC ),
# MAGIC 
# MAGIC ship_date_lookup AS (
# MAGIC     SELECT
# MAGIC         v.component_item_no                          AS item_no,
# MAGIC         DATE_TRUNC('week', v.required_date)          AS bucket_start_date,
# MAGIC         MIN(sl.`Shipment Date`)                      AS sales_line_ship_date
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Material_Availability v
# MAGIC     INNER JOIN production_order_dedup po
# MAGIC         ON po.`No.` = v.source_record_id
# MAGIC        AND po.`Status` = v.source_status
# MAGIC     INNER JOIN sales_line_dedup sl
# MAGIC         ON sl.`Document No.` = po.`Sales Order No.`
# MAGIC        AND sl.`No.`          = po.`Source No.`
# MAGIC     WHERE v.demand_type = 'FIRMED_BOM'
# MAGIC       AND v.required_qty > 0
# MAGIC       AND v.required_date IS NOT NULL
# MAGIC     GROUP BY v.component_item_no, DATE_TRUNC('week', v.required_date)
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- LAYER 0.5: PRO Component dedup (UNCHANGED)
# MAGIC -- =====================================================
# MAGIC pro_component_dedup AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `Prod. Order No.`, `Prod. Order Line No.`, `Line No.`, `BC Company`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Prod Order Component`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND Status IN ('Released', 'Firm Planned', 'Planned')
# MAGIC           AND `Remaining Quantity` > 0
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- LAYER 0.6: FIRMED_BOM UOM2 lookup (UNCHANGED)
# MAGIC -- =====================================================
# MAGIC poc_uom2_line_level AS (
# MAGIC     SELECT
# MAGIC         `Prod. Order No.`                              AS pro_no,
# MAGIC         `Prod. Order Line No.`                         AS parent_line_no,
# MAGIC         `Item No.`                                     AS comp_item_no,
# MAGIC         SUM(CAST(`Expected Units_DU_TSL` AS DECIMAL(38,10))) AS expected_units_uom2,
# MAGIC         MAX(`Unit of Measure - Units_DU_TSL`)          AS uom2_code
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Prod Order Component`
# MAGIC     WHERE `BC Company` = 'Ennovie'
# MAGIC       AND `Expected Units_DU_TSL` > 0
# MAGIC     GROUP BY
# MAGIC         `Prod. Order No.`,
# MAGIC         `Prod. Order Line No.`,
# MAGIC         `Item No.`
# MAGIC ),
# MAGIC 
# MAGIC firmed_bom_uom2_lookup AS (
# MAGIC     SELECT
# MAGIC         v.component_item_no                          AS item_no,
# MAGIC         DATE_TRUNC('week', v.required_date)          AS bucket_start_date,
# MAGIC         SUM(poc.expected_units_uom2)                 AS firmed_bom_qty_uom2
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Material_Availability v
# MAGIC     INNER JOIN poc_uom2_line_level poc
# MAGIC         ON poc.pro_no         = v.source_record_id
# MAGIC        AND poc.comp_item_no   = v.component_item_no
# MAGIC        AND poc.parent_line_no = TRY_CAST(v.source_line_id AS INT)
# MAGIC     WHERE v.demand_type = 'FIRMED_BOM'
# MAGIC       AND v.required_qty > 0
# MAGIC       AND v.required_date IS NOT NULL
# MAGIC     GROUP BY v.component_item_no, DATE_TRUNC('week', v.required_date)
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- LAYER 0.7: Primary customer per bucket (UNCHANGED from v9.4.17)
# MAGIC -- =====================================================
# MAGIC customer_per_bucket_qty AS (
# MAGIC     SELECT
# MAGIC         v.component_item_no                  AS item_no,
# MAGIC         DATE_TRUNC('week', v.required_date)  AS bucket_start_date,
# MAGIC         v.customer_no,
# MAGIC         v.customer_name,
# MAGIC         SUM(v.required_qty)                  AS total_qty
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Material_Availability v
# MAGIC     WHERE v.demand_type = 'FIRMED_BOM'
# MAGIC       AND v.required_qty > 0
# MAGIC       AND v.required_date IS NOT NULL
# MAGIC       AND v.customer_no IS NOT NULL
# MAGIC     GROUP BY
# MAGIC         v.component_item_no,
# MAGIC         DATE_TRUNC('week', v.required_date),
# MAGIC         v.customer_no,
# MAGIC         v.customer_name
# MAGIC ),
# MAGIC 
# MAGIC primary_customer AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         bucket_start_date,
# MAGIC         customer_no       AS primary_customer_no,
# MAGIC         customer_name     AS primary_customer_name,
# MAGIC         CONCAT(customer_no, ' ', customer_name) AS customer_display
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             item_no,
# MAGIC             bucket_start_date,
# MAGIC             customer_no,
# MAGIC             customer_name,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY item_no, bucket_start_date
# MAGIC                 ORDER BY total_qty DESC, customer_no ASC
# MAGIC             ) AS rn
# MAGIC         FROM customer_per_bucket_qty
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC customer_count_per_bucket AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         bucket_start_date,
# MAGIC         COUNT(DISTINCT customer_no)          AS customer_count
# MAGIC     FROM customer_per_bucket_qty
# MAGIC     GROUP BY item_no, bucket_start_date
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- LAYER 1: V6 demand bucketing (UNCHANGED from v9.4.18)
# MAGIC -- =====================================================
# MAGIC v6_bucketed_raw AS (
# MAGIC     SELECT
# MAGIC         v.component_item_no                 AS item_no,
# MAGIC         DATE_TRUNC('week', v.required_date) AS bucket_start_date,
# MAGIC         MAX(v.component_description)        AS description,
# MAGIC         MAX(v.item_category)                AS item_category_code,
# MAGIC         MAX(v.material_type)                AS material_type,
# MAGIC         MAX(v.metal_category)               AS metal_category,
# MAGIC         MAX(v.is_critical_item)             AS is_critical_item,
# MAGIC         MAX(v.base_uom)                     AS base_uom_uom1,
# MAGIC 
# MAGIC         SUM(CASE WHEN v.demand_type = 'FIRMED_BOM'
# MAGIC                  THEN v.required_qty ELSE 0 END)         AS demand_quantity_uom1_bucket,
# MAGIC 
# MAGIC         SUM(CASE WHEN v.demand_type = 'FIRMED_BOM' THEN 1 ELSE 0 END)
# MAGIC                                                         AS demand_line_count_in_bucket,
# MAGIC         MIN(CASE WHEN v.demand_type = 'FIRMED_BOM' THEN v.required_date END)
# MAGIC                                                         AS bucket_earliest_demand_date,
# MAGIC         MAX(CASE WHEN v.demand_type = 'FIRMED_BOM' THEN v.required_date END)
# MAGIC                                                         AS bucket_latest_demand_date,
# MAGIC         MIN(CASE WHEN v.demand_type = 'FIRMED_BOM' THEN v.required_date END)
# MAGIC                                                         AS due_date,
# MAGIC         MIN(CASE WHEN v.demand_type = 'FIRMED_BOM' THEN v.order_by_date END)
# MAGIC                                                         AS earliest_order_by_date,
# MAGIC 
# MAGIC         SUM(CASE WHEN v.demand_type = 'FIRMED_BOM'        
# MAGIC                  THEN v.required_qty ELSE 0 END) AS firmed_bom_qty,
# MAGIC         SUM(CASE WHEN v.demand_type = 'POLICY_TRIGGERED'  
# MAGIC                  THEN v.required_qty ELSE 0 END) AS policy_triggered_qty,
# MAGIC         SUM(CASE WHEN v.demand_type = 'PLANNED_BOM'      
# MAGIC                  THEN v.required_qty ELSE 0 END) AS planned_bom_qty,
# MAGIC 
# MAGIC         SUM(CASE WHEN v.demand_type = 'FIRMED_BOM' 
# MAGIC                       AND COALESCE(v.is_dnd, FALSE) = TRUE
# MAGIC                  THEN v.required_qty ELSE 0 END)
# MAGIC                                                 AS dnd_demand_qty,
# MAGIC 
# MAGIC         BOOL_OR(CASE WHEN v.demand_type = 'FIRMED_BOM' 
# MAGIC                           AND COALESCE(v.is_dnd, FALSE) = TRUE
# MAGIC                       THEN TRUE ELSE FALSE END)
# MAGIC                                                 AS has_dnd_demand,
# MAGIC 
# MAGIC         MAX(v.customer_no)                  AS customer_no_max,
# MAGIC         BOOL_OR(COALESCE(v.is_dnd, FALSE))  AS is_dnd,
# MAGIC         MAX(v.priority_score)               AS max_priority_score,
# MAGIC         MAX(v.preferred_vendor_no)          AS vendor_no,
# MAGIC         MAX(v.preferred_vendor_name)        AS vendor_name,
# MAGIC         MAX(v.last_direct_cost)             AS v6_fallback_unit_cost,
# MAGIC         MAX(v.lead_time_days)               AS lead_time_days
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Material_Availability v
# MAGIC     WHERE v.required_qty > 0
# MAGIC       AND v.required_date IS NOT NULL
# MAGIC     GROUP BY
# MAGIC         v.component_item_no,
# MAGIC         DATE_TRUNC('week', v.required_date)
# MAGIC ),
# MAGIC 
# MAGIC v6_bucketed AS (
# MAGIC     SELECT
# MAGIC         v.*,
# MAGIC         CONCAT(
# MAGIC             CAST(EXTRACT(YEAROFWEEK FROM v.bucket_start_date) AS STRING),
# MAGIC             '-W',
# MAGIC             LPAD(CAST(EXTRACT(WEEK FROM v.bucket_start_date) AS STRING), 2, '0')
# MAGIC         )                                            AS bucket_week,
# MAGIC         sdl.sales_line_ship_date,
# MAGIC         COALESCE(fbu.firmed_bom_qty_uom2, 0)         AS firmed_bom_qty_uom2,
# MAGIC         pc.primary_customer_no                       AS customer_no,
# MAGIC         pc.primary_customer_name                     AS customer_name,
# MAGIC         pc.customer_display                          AS customer_display,
# MAGIC         COALESCE(ccb.customer_count, 0)              AS customer_count
# MAGIC     FROM v6_bucketed_raw v
# MAGIC     LEFT JOIN ship_date_lookup sdl
# MAGIC         ON sdl.item_no           = v.item_no
# MAGIC        AND sdl.bucket_start_date = v.bucket_start_date
# MAGIC     LEFT JOIN firmed_bom_uom2_lookup fbu
# MAGIC         ON fbu.item_no           = v.item_no
# MAGIC        AND fbu.bucket_start_date = v.bucket_start_date
# MAGIC     LEFT JOIN primary_customer pc
# MAGIC         ON pc.item_no            = v.item_no
# MAGIC        AND pc.bucket_start_date  = v.bucket_start_date
# MAGIC     LEFT JOIN customer_count_per_bucket ccb
# MAGIC         ON ccb.item_no           = v.item_no
# MAGIC        AND ccb.bucket_start_date = v.bucket_start_date
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- LAYER 2: Item Master + module + UOM2 code (UNCHANGED)
# MAGIC -- =====================================================
# MAGIC bc_item_uom2 AS (
# MAGIC     SELECT
# MAGIC         `No.` AS item_no,
# MAGIC         `Unit of Measure - Units_DU_TSL` AS item_uom2_code
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `No.`, `BC Company`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Item`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC item_planning AS (
# MAGIC     SELECT
# MAGIC         im.item_no,
# MAGIC         im.reordering_policy,
# MAGIC         im.replenishment_system,
# MAGIC         im.archetype,
# MAGIC         im.rop_qty                          AS reorder_point_uom1,
# MAGIC         im.roq_qty                          AS reorder_quantity_uom1,
# MAGIC         im.max_inv_qty                      AS maximum_inventory_uom1,
# MAGIC         im.safety_stock_qty                 AS safety_stock_uom1,
# MAGIC         im.min_order_qty                    AS min_order_qty,
# MAGIC         im.max_order_qty                    AS max_order_qty,
# MAGIC         im.order_multiple                   AS order_multiple,
# MAGIC         im.scrap_pct                        AS scrap_pct,
# MAGIC         im.effective_lead_days,
# MAGIC         im.purch_uom                        AS secondary_uom_uom2,
# MAGIC         bcu.item_uom2_code,
# MAGIC         CASE
# MAGIC             WHEN im.item_category IN (
# MAGIC                 'MIXED METAL', 'CASTING', 'MST',
# MAGIC                 'PURE METAL', 'ALLOY', 'SOLDER', 'SEMI-FG'
# MAGIC             ) THEN 'METAL_ALLOY'
# MAGIC             ELSE 'COMPONENT'
# MAGIC         END                                 AS module
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Item_Master im
# MAGIC     LEFT JOIN bc_item_uom2 bcu
# MAGIC         ON bcu.item_no = im.item_no
# MAGIC     WHERE im.is_blocked = FALSE
# MAGIC       AND COALESCE(im.purchasing_blocked, FALSE) = FALSE
# MAGIC       AND im.archetype <> 'SKIP'
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- LAYER 2.5: Stock location aggregation (UNCHANGED)
# MAGIC -- =====================================================
# MAGIC stock_location_lookup AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         CONCAT_WS(', ', COLLECT_SET(location_code)) AS best_stock_location
# MAGIC     FROM (
# MAGIC         SELECT DISTINCT
# MAGIC             inv.item_no,
# MAGIC             inv.location_code
# MAGIC         FROM Gold_Inventory_Lakehouse.mrp.gold_inventory inv
# MAGIC         WHERE inv.qty_on_hand > 0
# MAGIC           AND COALESCE(inv.is_blocked_location, FALSE) = FALSE
# MAGIC           AND inv.location_code IN (
# MAGIC               'BAGGING','CASTING','CONSUME','CST_CUT','CST_ROOM','CZ-SYNT',
# MAGIC               'DEBEERS','DIA-LAB','DIA-NAT','EQUIP','FG-NO-PO','FINDINGS',
# MAGIC               'FIN-GOODS','GEMS','KIMAI','MATERIAL','OBSOLETE','OTHERS-MAT',
# MAGIC               'PACKAGING','PEARLS','PLATING','POMELATO','PRE ALLOY','RETURNS',
# MAGIC               'RUB MOLD','SEMI-F','SORTING','STONE-CUT','STR','TOOLS','WAX ROOM'
# MAGIC           )
# MAGIC     )
# MAGIC     GROUP BY item_no
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- LAYER 3: Inventory ATP (UNCHANGED from v9.4.16)
# MAGIC -- =====================================================
# MAGIC ile_stock_uom2 AS (
# MAGIC     SELECT
# MAGIC         ile.`Item No.` AS item_no,
# MAGIC         SUM(CAST(ile.`Remaining Quantity` AS DECIMAL(38,10))) AS onhand_uom1_from_ile,
# MAGIC         SUM(CAST(ile.`Units_DU_TSL` AS DECIMAL(38,10)))       AS onhand_uom2
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` ile
# MAGIC     INNER JOIN Gold_Inventory_Lakehouse.mrp.gold_inventory gi
# MAGIC         ON gi.item_no       = ile.`Item No.`
# MAGIC        AND gi.location_code = ile.`Location Code`
# MAGIC        AND gi.lot_no        = ile.`Lot No.`
# MAGIC     WHERE ile.`BC Company` = 'Ennovie'
# MAGIC       AND ile.`Location Code` IN (
# MAGIC           'BAGGING','CASTING','CONSUME','CST_CUT','CST_ROOM','CZ-SYNT',
# MAGIC           'DEBEERS','DIA-LAB','DIA-NAT','EQUIP','FG-NO-PO','FINDINGS',
# MAGIC           'FIN-GOODS','GEMS','KIMAI','MATERIAL','OBSOLETE','OTHERS-MAT',
# MAGIC           'PACKAGING','PEARLS','PLATING','POMELATO','PRE ALLOY','RETURNS',
# MAGIC           'RUB MOLD','SEMI-F','SORTING','STONE-CUT','STR','TOOLS','WAX ROOM'
# MAGIC       )
# MAGIC       AND COALESCE(gi.is_blocked_location, FALSE) = FALSE
# MAGIC     GROUP BY ile.`Item No.`
# MAGIC ),
# MAGIC 
# MAGIC active_pro_consumption AS (
# MAGIC     SELECT
# MAGIC         c.`Item No.`                AS item_no,
# MAGIC         SUM(c.`Remaining Quantity`) AS pro_committed_qty,
# MAGIC         SUM(CAST(c.`Units_DU_TSL` AS DECIMAL(38,10))) AS pro_committed_qty_uom2
# MAGIC     FROM pro_component_dedup c
# MAGIC     INNER JOIN Gold_Inventory_Lakehouse.mrp.gold_Item_Master im
# MAGIC             ON im.item_no = c.`Item No.`
# MAGIC     WHERE im.archetype <> 'SKIP'
# MAGIC     GROUP BY c.`Item No.`
# MAGIC ),
# MAGIC 
# MAGIC inventory_atp AS (
# MAGIC     SELECT
# MAGIC         inv.item_no,
# MAGIC         SUM(inv.qty_on_hand)                                AS onhand_quantity_uom1,
# MAGIC         SUM(inv.qty_available)                              AS available_qty_gross,
# MAGIC         COALESCE(MAX(apc.pro_committed_qty), 0)             AS pro_committed_qty,
# MAGIC         COALESCE(MAX(apc.pro_committed_qty_uom2), 0)        AS pro_committed_qty_uom2,
# MAGIC         COALESCE(MAX(isu.onhand_uom2), 0)                   AS onhand_quantity_uom2,
# MAGIC         SUM(inv.qty_available)                              AS available_qty_atp,
# MAGIC         COALESCE(MAX(isu.onhand_uom2), 0)                   AS available_qty_atp_uom2
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_inventory inv
# MAGIC     LEFT JOIN active_pro_consumption apc ON apc.item_no = inv.item_no
# MAGIC     LEFT JOIN ile_stock_uom2 isu          ON isu.item_no = inv.item_no
# MAGIC     WHERE COALESCE(inv.is_blocked_location, FALSE) = FALSE
# MAGIC       AND inv.location_code IN (
# MAGIC           'BAGGING','CASTING','CONSUME','CST_CUT','CST_ROOM','CZ-SYNT',
# MAGIC           'DEBEERS','DIA-LAB','DIA-NAT','EQUIP','FG-NO-PO','FINDINGS',
# MAGIC           'FIN-GOODS','GEMS','KIMAI','MATERIAL','OBSOLETE','OTHERS-MAT',
# MAGIC           'PACKAGING','PEARLS','PLATING','POMELATO','PRE ALLOY','RETURNS',
# MAGIC           'RUB MOLD','SEMI-F','SORTING','STONE-CUT','STR','TOOLS','WAX ROOM'
# MAGIC       )
# MAGIC     GROUP BY inv.item_no
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- LAYER 4: PO incoming (UNCHANGED)
# MAGIC -- =====================================================
# MAGIC purchase_header_dedup AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `No.`, `Document Type`, `BC Company`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn_ph
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Purchase Header`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND `Document Type` = 'Order'
# MAGIC     )
# MAGIC     WHERE rn_ph = 1
# MAGIC ),
# MAGIC 
# MAGIC purchase_line_dedup AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `Document No.`, `Document Type`, `Line No.`, `BC Company`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn_pl
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Purchase Line`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND `Document Type` = 'Order'
# MAGIC           AND `Outstanding Quantity` > 0
# MAGIC           AND `Expected Receipt Date` > DATE'1900-01-01'
# MAGIC           AND Type = 'Item'
# MAGIC     )
# MAGIC     WHERE rn_pl = 1
# MAGIC ),
# MAGIC 
# MAGIC po_lines_with_status AS (
# MAGIC     SELECT
# MAGIC         pl.`No.`                              AS item_no,
# MAGIC         ph.`No.`                              AS po_no,
# MAGIC         pl.`Line No.`                         AS line_no,
# MAGIC         ph.`Status`                           AS po_status,
# MAGIC         pl.`Outstanding Qty. (Base)`          AS qty,
# MAGIC         COALESCE(
# MAGIC             CAST(pl.`Outstanding Units_DU_TSL` AS DECIMAL(38,10)),
# MAGIC             0
# MAGIC         )                                     AS qty_uom2,
# MAGIC         pl.`Direct Unit Cost`                 AS direct_unit_cost,
# MAGIC         pl.`Expected Receipt Date`            AS receipt_date,
# MAGIC         ph.`Document Date`                    AS doc_date
# MAGIC     FROM purchase_line_dedup pl
# MAGIC     INNER JOIN purchase_header_dedup ph
# MAGIC         ON ph.`No.` = pl.`Document No.`
# MAGIC        AND ph.`Document Type` = pl.`Document Type`
# MAGIC        AND ph.`BC Company` = pl.`BC Company`
# MAGIC ),
# MAGIC 
# MAGIC po_released_agg AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         SUM(qty) AS qty_released,
# MAGIC         SUM(qty_uom2) AS qty_released_uom2,
# MAGIC         MIN(receipt_date) AS earliest_released_receipt,
# MAGIC         COUNT(DISTINCT po_no) AS released_po_count
# MAGIC     FROM po_lines_with_status WHERE po_status = 'Released' GROUP BY item_no
# MAGIC ),
# MAGIC 
# MAGIC po_open_agg AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         SUM(qty) AS qty_open,
# MAGIC         SUM(qty_uom2) AS qty_open_uom2,
# MAGIC         MIN(receipt_date) AS earliest_open_receipt,
# MAGIC         COUNT(DISTINCT po_no) AS open_po_count
# MAGIC     FROM po_lines_with_status WHERE po_status = 'Open' GROUP BY item_no
# MAGIC ),
# MAGIC 
# MAGIC latest_released_po AS (
# MAGIC     SELECT item_no, direct_unit_cost AS released_unit_cost
# MAGIC     FROM (
# MAGIC         SELECT item_no, direct_unit_cost,
# MAGIC             ROW_NUMBER() OVER (PARTITION BY item_no ORDER BY doc_date DESC, line_no DESC) AS rn
# MAGIC         FROM po_lines_with_status WHERE po_status = 'Released'
# MAGIC     ) WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC latest_open_po AS (
# MAGIC     SELECT item_no, direct_unit_cost AS open_unit_cost
# MAGIC     FROM (
# MAGIC         SELECT item_no, direct_unit_cost,
# MAGIC             ROW_NUMBER() OVER (PARTITION BY item_no ORDER BY doc_date DESC, line_no DESC) AS rn
# MAGIC         FROM po_lines_with_status WHERE po_status = 'Open'
# MAGIC     ) WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC ile_avg AS (
# MAGIC     SELECT `Item No.` AS item_no, SUM(ABS(Quantity)) / 180.0 AS avg_daily_usage
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry`
# MAGIC     WHERE `BC Company` = 'Ennovie'
# MAGIC       AND `Entry Type` = 'Consumption'
# MAGIC       AND `Posting Date` >= ADD_MONTHS(CURRENT_DATE(), -6)
# MAGIC     GROUP BY `Item No.`
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- LAYER 5: Combine + lot sizing (⭐ v9.4.19 ROP refill DISABLED)
# MAGIC -- =====================================================
# MAGIC v6_with_running AS (
# MAGIC     SELECT
# MAGIC         v.*,
# MAGIC         ip.module,
# MAGIC         ip.item_uom2_code,
# MAGIC         ip.reordering_policy,
# MAGIC         ip.replenishment_system,
# MAGIC         ip.archetype,
# MAGIC         ip.reorder_point_uom1,
# MAGIC         ip.reorder_quantity_uom1,
# MAGIC         ip.maximum_inventory_uom1,
# MAGIC         ip.safety_stock_uom1,
# MAGIC         ip.min_order_qty,
# MAGIC         ip.max_order_qty,
# MAGIC         ip.order_multiple,
# MAGIC         ip.effective_lead_days,
# MAGIC         ip.secondary_uom_uom2,
# MAGIC         sll.best_stock_location,
# MAGIC         COALESCE(inv.onhand_quantity_uom1, 0)        AS onhand_quantity_uom1,
# MAGIC         COALESCE(inv.onhand_quantity_uom2, 0)        AS onhand_quantity_uom2,
# MAGIC         COALESCE(inv.available_qty_atp, 0)           AS available_qty_atp,
# MAGIC         COALESCE(inv.available_qty_atp_uom2, 0)      AS available_qty_atp_uom2,
# MAGIC         COALESCE(inv.pro_committed_qty, 0)           AS pro_committed_qty,
# MAGIC         COALESCE(inv.pro_committed_qty_uom2, 0)      AS pro_committed_qty_uom2,
# MAGIC         COALESCE(por.qty_released, 0)                AS incoming_po_released_qty,
# MAGIC         COALESCE(por.qty_released_uom2, 0)           AS incoming_po_released_qty_uom2,
# MAGIC         COALESCE(poo.qty_open, 0)                    AS incoming_po_open_qty,
# MAGIC         COALESCE(poo.qty_open_uom2, 0)               AS incoming_po_open_qty_uom2,
# MAGIC         COALESCE(por.earliest_released_receipt, poo.earliest_open_receipt)
# MAGIC                                                      AS earliest_po_receipt_date,
# MAGIC         COALESCE(por.released_po_count, 0)           AS released_po_count,
# MAGIC         COALESCE(poo.open_po_count, 0)               AS open_po_count,
# MAGIC         COALESCE(
# MAGIC             lrp.released_unit_cost, lop.open_unit_cost, v.v6_fallback_unit_cost
# MAGIC         )                                            AS unit_cost,
# MAGIC         CASE
# MAGIC             WHEN lrp.released_unit_cost IS NOT NULL THEN 'RELEASED_PO'
# MAGIC             WHEN lop.open_unit_cost IS NOT NULL     THEN 'OPEN_PO'
# MAGIC             ELSE 'ITEM_MASTER'
# MAGIC         END                                          AS unit_cost_source,
# MAGIC         COALESCE(ila.avg_daily_usage, 0)             AS avg_daily_usage,
# MAGIC         SUM(v.demand_quantity_uom1_bucket) OVER (
# MAGIC             PARTITION BY v.item_no ORDER BY v.bucket_start_date
# MAGIC             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC         )                                            AS cumulative_demand_thru_bucket,
# MAGIC         SUM(v.demand_quantity_uom1_bucket) OVER (
# MAGIC             PARTITION BY v.item_no ORDER BY v.bucket_start_date
# MAGIC             ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
# MAGIC         )                                            AS cumulative_demand_before_bucket,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY v.item_no
# MAGIC             ORDER BY v.bucket_start_date
# MAGIC         )                                            AS bucket_rank,
# MAGIC         SUM(v.demand_quantity_uom1_bucket) OVER (
# MAGIC             PARTITION BY v.item_no
# MAGIC         )                                            AS item_total_demand_uom1,
# MAGIC         SUM(v.firmed_bom_qty_uom2) OVER (
# MAGIC             PARTITION BY v.item_no
# MAGIC         )                                            AS item_total_firmed_demand_uom2
# MAGIC     FROM v6_bucketed v
# MAGIC     LEFT JOIN item_planning ip      ON ip.item_no = v.item_no
# MAGIC     LEFT JOIN stock_location_lookup sll ON sll.item_no = v.item_no
# MAGIC     LEFT JOIN inventory_atp inv     ON inv.item_no = v.item_no
# MAGIC     LEFT JOIN po_released_agg por   ON por.item_no = v.item_no
# MAGIC     LEFT JOIN po_open_agg poo       ON poo.item_no = v.item_no
# MAGIC     LEFT JOIN latest_released_po lrp ON lrp.item_no = v.item_no
# MAGIC     LEFT JOIN latest_open_po lop     ON lop.item_no = v.item_no
# MAGIC     LEFT JOIN ile_avg ila           ON ila.item_no = v.item_no
# MAGIC ),
# MAGIC 
# MAGIC v6_with_policy AS (
# MAGIC     SELECT *,
# MAGIC         GREATEST(
# MAGIC             available_qty_atp + incoming_po_released_qty
# MAGIC                 - COALESCE(cumulative_demand_before_bucket, 0), 0
# MAGIC         )                                            AS available_before_bucket_uom1,
# MAGIC         available_qty_atp + incoming_po_released_qty
# MAGIC             - cumulative_demand_thru_bucket          AS net_position_after_bucket,
# MAGIC         GREATEST(
# MAGIC             demand_quantity_uom1_bucket
# MAGIC                 - GREATEST(available_qty_atp + incoming_po_released_qty
# MAGIC                     - COALESCE(cumulative_demand_before_bucket, 0), 0), 0
# MAGIC         )                                            AS shortage_quantity_uom1,
# MAGIC         GREATEST(
# MAGIC             available_qty_atp + incoming_po_released_qty + incoming_po_open_qty
# MAGIC                 - COALESCE(cumulative_demand_before_bucket, 0), 0
# MAGIC         )                                            AS available_before_bucket_if_open,
# MAGIC         GREATEST(
# MAGIC             demand_quantity_uom1_bucket
# MAGIC                 - GREATEST(available_qty_atp
# MAGIC                     + incoming_po_released_qty + incoming_po_open_qty
# MAGIC                     - COALESCE(cumulative_demand_before_bucket, 0), 0), 0
# MAGIC         )                                            AS shortage_qty_if_open_released
# MAGIC     FROM v6_with_running
# MAGIC ),
# MAGIC 
# MAGIC -- ⭐ v9.4.19: ROP refill DISABLED — only shortage-based trigger
# MAGIC -- All policies now trigger ONLY when shortage > 0 (driven by FIRMED demand)
# MAGIC v6_with_lot_sizing AS (
# MAGIC     SELECT *,
# MAGIC         -- should_trigger: shortage-based only (no ROP refill)
# MAGIC         CASE
# MAGIC             WHEN reordering_policy = 'Lot-for-Lot'        
# MAGIC                 THEN shortage_quantity_uom1 > 0
# MAGIC             WHEN reordering_policy = 'Fixed Reorder Qty.' THEN
# MAGIC                 shortage_quantity_uom1 > 0       -- ⭐ ROP branch REMOVED
# MAGIC             ELSE shortage_quantity_uom1 > 0
# MAGIC         END                                          AS should_trigger,
# MAGIC 
# MAGIC         -- policy_quantity_uom1: shortage-based only
# MAGIC         CASE
# MAGIC             WHEN reordering_policy = 'Lot-for-Lot' THEN shortage_quantity_uom1
# MAGIC             WHEN reordering_policy = 'Fixed Reorder Qty.' THEN
# MAGIC                 CASE
# MAGIC                     WHEN shortage_quantity_uom1 > 0
# MAGIC                         THEN shortage_quantity_uom1   -- ⭐ no ROQ refill branch
# MAGIC                     ELSE 0
# MAGIC                 END
# MAGIC             ELSE GREATEST(shortage_quantity_uom1, 0)
# MAGIC         END                                          AS policy_quantity_uom1,
# MAGIC 
# MAGIC         -- policy_qty_if_open_released: shortage-based only (with open PO scenario)
# MAGIC         CASE
# MAGIC             WHEN reordering_policy = 'Lot-for-Lot' THEN shortage_qty_if_open_released
# MAGIC             WHEN reordering_policy = 'Fixed Reorder Qty.' THEN
# MAGIC                 CASE
# MAGIC                     WHEN shortage_qty_if_open_released > 0
# MAGIC                         THEN shortage_qty_if_open_released   -- ⭐ no ROQ refill
# MAGIC                     ELSE 0
# MAGIC                 END
# MAGIC             ELSE GREATEST(shortage_qty_if_open_released, 0)
# MAGIC         END                                          AS policy_qty_if_open_released
# MAGIC     FROM v6_with_policy
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- LAYER 6: Stage calculation (UNCHANGED)
# MAGIC -- =====================================================
# MAGIC v6_with_stages AS (
# MAGIC     SELECT *,
# MAGIC         CASE WHEN sales_line_ship_date IS NOT NULL
# MAGIC              THEN count_working_days(CURRENT_DATE(), sales_line_ship_date)
# MAGIC              ELSE NULL
# MAGIC         END                                          AS available_working_days,
# MAGIC         CASE WHEN sales_line_ship_date IS NOT NULL
# MAGIC              THEN calc_wax_start(sales_line_ship_date, CURRENT_DATE())
# MAGIC              ELSE NULL
# MAGIC         END                                          AS wax_start,
# MAGIC         CASE WHEN sales_line_ship_date IS NOT NULL
# MAGIC              THEN calc_semi_start(sales_line_ship_date, CURRENT_DATE())
# MAGIC              ELSE NULL
# MAGIC         END                                          AS semi_start
# MAGIC     FROM v6_with_lot_sizing
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- LAYER 7: Order date computation (UNCHANGED)
# MAGIC -- =====================================================
# MAGIC v6_with_order_date AS (
# MAGIC     SELECT *,
# MAGIC         CASE
# MAGIC             WHEN sales_line_ship_date IS NULL THEN
# MAGIC                 DATE_SUB(due_date, COALESCE(effective_lead_days, lead_time_days, 14))
# MAGIC             WHEN module = 'METAL_ALLOY' THEN
# MAGIC                 sub_working_days(wax_start, COALESCE(effective_lead_days, lead_time_days, 14))
# MAGIC             ELSE
# MAGIC                 sub_working_days(semi_start, COALESCE(effective_lead_days, lead_time_days, 14))
# MAGIC         END                                          AS suggested_order_date_raw
# MAGIC     FROM v6_with_stages
# MAGIC ),
# MAGIC 
# MAGIC v6_with_order_dates_full AS (
# MAGIC     SELECT *,
# MAGIC         GREATEST(suggested_order_date_raw, CURRENT_DATE()) AS suggested_order_date_actionable_calc
# MAGIC     FROM v6_with_order_date
# MAGIC )
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- FINAL SELECT (UNCHANGED structure from v9.4.18)
# MAGIC -- =====================================================
# MAGIC SELECT
# MAGIC     UUID()                                           AS systemID,
# MAGIC     'Ennovie'                                        AS bc_company,
# MAGIC     item_no,
# MAGIC     CAST(NULL AS STRING)                             AS variant_code,
# MAGIC     bucket_week,
# MAGIC     bucket_start_date,
# MAGIC     DATE_ADD(bucket_start_date, 6)                   AS bucket_end_date,
# MAGIC     description,
# MAGIC     item_category_code,
# MAGIC     material_type,
# MAGIC     metal_category,
# MAGIC     is_critical_item                                 AS is_critical,
# MAGIC     base_uom_uom1,
# MAGIC     secondary_uom_uom2,
# MAGIC     item_uom2_code,
# MAGIC     module,
# MAGIC 
# MAGIC     demand_quantity_uom1_bucket,
# MAGIC     demand_line_count_in_bucket,
# MAGIC     bucket_earliest_demand_date,
# MAGIC     bucket_latest_demand_date,
# MAGIC     firmed_bom_qty,
# MAGIC     firmed_bom_qty_uom2,
# MAGIC     policy_triggered_qty,
# MAGIC     planned_bom_qty,
# MAGIC 
# MAGIC     dnd_demand_qty,
# MAGIC     has_dnd_demand,
# MAGIC 
# MAGIC     onhand_quantity_uom1,
# MAGIC     onhand_quantity_uom2,
# MAGIC     available_qty_atp                                AS available_qty_atp_uom1,
# MAGIC     available_qty_atp_uom2,
# MAGIC     pro_committed_qty,
# MAGIC     pro_committed_qty_uom2,
# MAGIC     available_before_bucket_uom1,
# MAGIC     net_position_after_bucket                        AS available_after_bucket_uom1,
# MAGIC     shortage_quantity_uom1,
# MAGIC 
# MAGIC     reordering_policy,
# MAGIC     archetype,
# MAGIC     reorder_point_uom1,
# MAGIC     reorder_quantity_uom1,
# MAGIC     maximum_inventory_uom1,
# MAGIC     safety_stock_uom1,
# MAGIC     min_order_qty,
# MAGIC     max_order_qty,
# MAGIC     order_multiple,
# MAGIC 
# MAGIC     incoming_po_released_qty,
# MAGIC     incoming_po_released_qty_uom2,
# MAGIC     incoming_po_open_qty,
# MAGIC     incoming_po_open_qty_uom2,
# MAGIC     earliest_po_receipt_date,
# MAGIC     released_po_count,
# MAGIC     open_po_count,
# MAGIC     (incoming_po_open_qty > 0)                       AS has_pending_open_po,
# MAGIC 
# MAGIC     bucket_rank,
# MAGIC 
# MAGIC     should_trigger,
# MAGIC     policy_quantity_uom1,
# MAGIC     CASE
# MAGIC         WHEN NOT should_trigger THEN 0
# MAGIC         ELSE CEIL(
# MAGIC                 LEAST(
# MAGIC                     GREATEST(COALESCE(min_order_qty, 0), policy_quantity_uom1),
# MAGIC                     COALESCE(NULLIF(max_order_qty, 0), policy_quantity_uom1)
# MAGIC                 ) / GREATEST(order_multiple, 1)
# MAGIC              ) * GREATEST(order_multiple, 1)
# MAGIC     END                                              AS final_uom1,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN NOT should_trigger THEN 0
# MAGIC         ELSE CEIL(
# MAGIC                 LEAST(
# MAGIC                     GREATEST(COALESCE(min_order_qty, 0), policy_qty_if_open_released),
# MAGIC                     COALESCE(NULLIF(max_order_qty, 0), policy_qty_if_open_released)
# MAGIC                 ) / GREATEST(order_multiple, 1)
# MAGIC              ) * GREATEST(order_multiple, 1)
# MAGIC     END                                              AS suggested_qty_if_open_released,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN NOT should_trigger THEN CAST(0 AS DECIMAL(38,20))
# MAGIC         WHEN onhand_quantity_uom1 > 0 AND onhand_quantity_uom2 > 0 THEN
# MAGIC             CAST(
# MAGIC                 CEIL(
# MAGIC                     LEAST(
# MAGIC                         GREATEST(COALESCE(min_order_qty, 0), policy_quantity_uom1),
# MAGIC                         COALESCE(NULLIF(max_order_qty, 0), policy_quantity_uom1)
# MAGIC                     ) / GREATEST(order_multiple, 1)
# MAGIC                 ) * GREATEST(order_multiple, 1)
# MAGIC                 * (onhand_quantity_uom2 / onhand_quantity_uom1)
# MAGIC                 AS DECIMAL(38,20)
# MAGIC             )
# MAGIC         ELSE CAST(NULL AS DECIMAL(38,20))
# MAGIC     END                                              AS final_uom2,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN NOT should_trigger THEN CAST(0 AS DECIMAL(38,20))
# MAGIC         WHEN onhand_quantity_uom1 > 0 AND onhand_quantity_uom2 > 0 THEN
# MAGIC             CAST(
# MAGIC                 CEIL(
# MAGIC                     LEAST(
# MAGIC                         GREATEST(COALESCE(min_order_qty, 0), policy_qty_if_open_released),
# MAGIC                         COALESCE(NULLIF(max_order_qty, 0), policy_qty_if_open_released)
# MAGIC                     ) / GREATEST(order_multiple, 1)
# MAGIC                 ) * GREATEST(order_multiple, 1)
# MAGIC                 * (onhand_quantity_uom2 / onhand_quantity_uom1)
# MAGIC                 AS DECIMAL(38,20)
# MAGIC             )
# MAGIC         ELSE CAST(NULL AS DECIMAL(38,20))
# MAGIC     END                                              AS suggested_qty_if_open_released_uom2,
# MAGIC 
# MAGIC     lead_time_days,
# MAGIC     effective_lead_days,
# MAGIC 
# MAGIC     sales_line_ship_date,
# MAGIC     available_working_days,
# MAGIC     wax_start,
# MAGIC     semi_start,
# MAGIC 
# MAGIC     suggested_order_date_raw                         AS suggested_order_date,
# MAGIC     suggested_order_date_actionable_calc             AS suggested_order_date_actionable,
# MAGIC     CONCAT(
# MAGIC         CAST(EXTRACT(YEAROFWEEK FROM suggested_order_date_raw) AS STRING),
# MAGIC         '-W',
# MAGIC         LPAD(CAST(EXTRACT(WEEK FROM suggested_order_date_raw) AS STRING), 2, '0')
# MAGIC     )                                                AS suggested_order_week,
# MAGIC     CONCAT(
# MAGIC         CAST(EXTRACT(YEAROFWEEK FROM suggested_order_date_actionable_calc) AS STRING),
# MAGIC         '-W',
# MAGIC         LPAD(CAST(EXTRACT(WEEK FROM suggested_order_date_actionable_calc) AS STRING), 2, '0')
# MAGIC     )                                                AS suggested_order_week_actionable,
# MAGIC 
# MAGIC     due_date,
# MAGIC     earliest_order_by_date                           AS order_by_date,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN sales_line_ship_date IS NULL                                     THEN 'NO_SHIP_DATE'
# MAGIC         WHEN suggested_order_date_raw < CURRENT_DATE()                        THEN 'OVERDUE'
# MAGIC         WHEN suggested_order_date_raw <= DATE_ADD(CURRENT_DATE(), 7)          THEN 'URGENT'
# MAGIC         WHEN suggested_order_date_raw <= DATE_ADD(CURRENT_DATE(), 30)         THEN 'SOON'
# MAGIC         ELSE 'OK'
# MAGIC     END                                              AS po_urgency,
# MAGIC 
# MAGIC     avg_daily_usage,
# MAGIC     ROUND(avg_daily_usage * 60, 0)                   AS forecast_demand_60d,
# MAGIC     CASE
# MAGIC         WHEN avg_daily_usage > 0 THEN
# MAGIC             DATE_ADD(CURRENT_DATE(),
# MAGIC                 CAST(GREATEST(onhand_quantity_uom1 + incoming_po_released_qty, 0)
# MAGIC                     / NULLIF(avg_daily_usage, 0) AS INT))
# MAGIC         ELSE NULL
# MAGIC     END                                              AS projected_stockout_date,
# MAGIC 
# MAGIC     customer_no,
# MAGIC     customer_name,
# MAGIC     customer_display,
# MAGIC     customer_count,
# MAGIC 
# MAGIC     is_dnd,
# MAGIC     max_priority_score                               AS priority_score,
# MAGIC 
# MAGIC     vendor_no,
# MAGIC     vendor_name,
# MAGIC     unit_cost,
# MAGIC     unit_cost_source,
# MAGIC     ROUND(
# MAGIC         CASE WHEN NOT should_trigger THEN 0
# MAGIC              ELSE policy_quantity_uom1 * COALESCE(unit_cost, 0)
# MAGIC         END, 2
# MAGIC     )                                                AS cost_amount,
# MAGIC 
# MAGIC     best_stock_location,
# MAGIC     replenishment_system,
# MAGIC     item_total_demand_uom1,
# MAGIC     item_total_firmed_demand_uom2,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN onhand_quantity_uom1 = 0 AND demand_quantity_uom1_bucket > 0 THEN 'STOCKOUT'
# MAGIC         WHEN onhand_quantity_uom1 < COALESCE(safety_stock_uom1, 0) AND COALESCE(safety_stock_uom1, 0) > 0 THEN 'BELOW_SAFETY'
# MAGIC         WHEN onhand_quantity_uom1 < COALESCE(reorder_point_uom1, 0) AND COALESCE(reorder_point_uom1, 0) > 0 THEN 'BELOW_ROP'
# MAGIC         WHEN should_trigger AND due_date <= DATE_ADD(CURRENT_DATE(), 14) THEN 'NEEDS_ORDER'
# MAGIC         ELSE 'OK'
# MAGIC     END                                              AS alert_flag,
# MAGIC 
# MAGIC     'v9.4.19'                                        AS source_version,    -- ⭐ bumped
# MAGIC     CURRENT_TIMESTAMP()                              AS view_built_at
# MAGIC 
# MAGIC FROM v6_with_order_dates_full
# MAGIC WHERE module IS NOT NULL;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- 🆕 v9.4.19 verification — ROP refill DISABLED
# MAGIC -- ============================================================================
# MAGIC 
# MAGIC -- VC42: GE-PL-001780 canary — should NOT trigger anymore
# MAGIC SELECT
# MAGIC     item_no, bucket_week,
# MAGIC     reordering_policy,
# MAGIC     firmed_bom_qty,
# MAGIC     onhand_quantity_uom1,
# MAGIC     reorder_point_uom1,
# MAGIC     shortage_quantity_uom1,
# MAGIC     should_trigger,
# MAGIC     final_uom1,
# MAGIC     alert_flag
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.fabric_requisition_line
# MAGIC WHERE item_no = 'GE-PL-001780'
# MAGIC ORDER BY bucket_week;
# MAGIC -- ✅ PASS if: should_trigger = FALSE, final_uom1 = 0
# MAGIC --           (no FIRMED demand → no trigger)
# MAGIC 
# MAGIC 
# MAGIC -- VC43: Items with Fixed Reorder Qty policy + no FIRMED — should NOT trigger
# MAGIC SELECT 
# MAGIC     COUNT(*) AS rows,
# MAGIC     SUM(CASE WHEN should_trigger THEN 1 ELSE 0 END) AS triggered_rows,
# MAGIC     SUM(final_uom1) AS total_qty
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.fabric_requisition_line
# MAGIC WHERE reordering_policy = 'Fixed Reorder Qty.'
# MAGIC   AND firmed_bom_qty = 0;
# MAGIC -- ✅ PASS if: triggered_rows = 0, total_qty = 0
# MAGIC --           (no FIRMED → no ROP refill)
# MAGIC 
# MAGIC 
# MAGIC -- VC44: Items with Fixed Reorder Qty policy + FIRMED demand — SHOULD trigger
# MAGIC SELECT 
# MAGIC     COUNT(*) AS rows,
# MAGIC     SUM(CASE WHEN should_trigger THEN 1 ELSE 0 END) AS triggered_rows,
# MAGIC     SUM(CASE WHEN shortage_quantity_uom1 > 0 THEN 1 ELSE 0 END) AS shortage_rows
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.fabric_requisition_line
# MAGIC WHERE reordering_policy = 'Fixed Reorder Qty.'
# MAGIC   AND firmed_bom_qty > 0
# MAGIC   AND shortage_quantity_uom1 > 0;
# MAGIC -- ✅ PASS if: triggered_rows = shortage_rows
# MAGIC --           (shortage-based trigger still works)
# MAGIC 
# MAGIC 
# MAGIC -- VC45: Policy distribution — what's still triggering
# MAGIC SELECT
# MAGIC     reordering_policy,
# MAGIC     COUNT(*) AS rows,
# MAGIC     SUM(CASE WHEN should_trigger THEN 1 ELSE 0 END) AS triggered_rows,
# MAGIC     SUM(final_uom1) AS total_final_uom1,
# MAGIC     ROUND(SUM(cost_amount), 0) AS total_cost
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.fabric_requisition_line
# MAGIC GROUP BY reordering_policy
# MAGIC ORDER BY total_final_uom1 DESC;
# MAGIC 
# MAGIC 
# MAGIC -- VC46: Impact summary — compare to v9.4.18
# MAGIC SELECT
# MAGIC     COUNT(DISTINCT item_no) AS total_items,
# MAGIC     SUM(CASE WHEN should_trigger THEN 1 ELSE 0 END) AS triggered_rows,
# MAGIC     SUM(CASE WHEN final_uom1 > 0 THEN 1 ELSE 0 END) AS rows_with_qty,
# MAGIC     SUM(final_uom1) AS total_final_uom1,
# MAGIC     ROUND(SUM(cost_amount), 0) AS total_cost_thb
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.fabric_requisition_line;
# MAGIC -- ℹ️ Expected: lower than v9.4.18 (97.7M THB) — ROP-only items now excluded
# MAGIC --    Items removed = Fixed Reorder Qty. items with firmed_bom_qty = 0
# MAGIC 
# MAGIC 
# MAGIC -- VC47: ROP items "at risk" — visibility query (informational)
# MAGIC -- These are items where on_hand <= ROP, but engine doesn't trigger
# MAGIC -- (because no FIRMED demand). Production team may want to convert
# MAGIC -- Planned → Released PROs for these to ensure timely refill.
# MAGIC SELECT
# MAGIC     item_no,
# MAGIC     description,
# MAGIC     onhand_quantity_uom1,
# MAGIC     reorder_point_uom1,
# MAGIC     reorder_quantity_uom1,
# MAGIC     incoming_po_released_qty,
# MAGIC     planned_bom_qty,
# MAGIC     customer_display
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.fabric_requisition_line
# MAGIC WHERE reordering_policy = 'Fixed Reorder Qty.'
# MAGIC   AND firmed_bom_qty = 0
# MAGIC   AND onhand_quantity_uom1 <= reorder_point_uom1
# MAGIC   AND bucket_rank = 1
# MAGIC ORDER BY (reorder_point_uom1 - onhand_quantity_uom1) DESC
# MAGIC LIMIT 30;
# MAGIC 
# MAGIC 
# MAGIC -- VC48: Cascade timestamp
# MAGIC SELECT 'V6'                          AS layer, MAX(view_built_at) AS last_built
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Material_Availability
# MAGIC UNION ALL
# MAGIC SELECT 'fabric_req v9.4.19', MAX(view_built_at)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.fabric_requisition_line;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- REGRESSION CHECKS — preserved canaries
# MAGIC -- ============================================================================
# MAGIC 
# MAGIC -- VC49: AC-CZ-000022 canary — v9.4.17 customer enrichment preserved
# MAGIC SELECT
# MAGIC     item_no, bucket_week,
# MAGIC     firmed_bom_qty, dnd_demand_qty,
# MAGIC     customer_display, customer_count,
# MAGIC     final_uom1
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.fabric_requisition_line
# MAGIC WHERE item_no = 'AC-CZ-000022'
# MAGIC ORDER BY bucket_start_date;
# MAGIC 
# MAGIC 
# MAGIC -- VC50: AC-CZ-000021 canary — v9.4.16 Bug 7 fix preserved
# MAGIC SELECT
# MAGIC     item_no, bucket_week,
# MAGIC     onhand_quantity_uom1, available_qty_atp_uom1,
# MAGIC     demand_quantity_uom1_bucket, firmed_bom_qty,
# MAGIC     dnd_demand_qty,
# MAGIC     final_uom1, alert_flag
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.fabric_requisition_line
# MAGIC WHERE item_no = 'AC-CZ-000021'
# MAGIC ORDER BY bucket_week;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- =====================================================================
# MAGIC -- 📦 fabric_requisition_line v9.4.22 — Cell 2 (FULL NOTEBOOK)
# MAGIC -- =====================================================================
# MAGIC -- Run AFTER Cell 1 (UDFs registered)
# MAGIC --
# MAGIC -- 🆕 v9.4.22 CHANGES (from v9.4.21):
# MAGIC --
# MAGIC --   🎯 UOM2-FIRST SHORTAGE: For dual-UOM items, check UOM2 sufficiency.
# MAGIC --                            If UOM2 supply meets demand → no alert
# MAGIC --                            (even if UOM1 appears short).
# MAGIC --
# MAGIC --      RATIONALE (per user 2026-05-19):
# MAGIC --        Dual-UOM items track in UOM2 (CM/CTS). Purchasing's source of
# MAGIC --        truth = UOM2 quantity, not weight. If UOM2 supply ≥ UOM2 demand,
# MAGIC --        the bucket is "satisfied" regardless of UOM1 ratio quirks.
# MAGIC --
# MAGIC --      EXAMPLE — FCG-000610-18KWPD W30:
# MAGIC --        UOM1 view: demand 137.46 GR vs avail 96.94 GR → 40.51 short
# MAGIC --        UOM2 view: demand 3,617 CM  vs avail 3,150 CM → 467 short ← USE THIS
# MAGIC --        v9.4.22 reports: shortage_uom1=0 (overridden), final_uom2=467 CM
# MAGIC --
# MAGIC --      LOGIC:
# MAGIC --        For dual-UOM items (item_uom2_code IS NOT NULL):
# MAGIC --          - Compute shortage_uom2 separately (UOM2-level)
# MAGIC --          - If shortage_uom2 = 0 → force shortage_uom1, final_uom1,
# MAGIC --            final_uom2 all to 0 (UOM2 satisfied = no alert)
# MAGIC --          - If shortage_uom2 > 0 → final_uom2 = CEIL(shortage_uom2)
# MAGIC --            (final_uom1 keeps v9.4.21 formula — per user choice Q2)
# MAGIC --
# MAGIC --        For single-UOM items (item_uom2_code IS NULL):
# MAGIC --          - Unchanged from v9.4.21 (UOM1-only logic)
# MAGIC --
# MAGIC --   ✅ SCHEMA PRESERVED: Zero column changes.
# MAGIC --      All column names, types, and positions identical to v9.4.21.
# MAGIC --      Dataverse realtime sync continues without re-mapping.
# MAGIC --
# MAGIC --   ⚠️ BEHAVIOR CHANGE WARNING:
# MAGIC --      For dual-UOM items, `shortage_quantity_uom1` may now report 0
# MAGIC --      even when raw UOM1 calc would say shortage > 0. This is intentional
# MAGIC --      — UOM2 is the source of truth for dual-UOM items.
# MAGIC --      Single-UOM items (PCS-only: C0-/SM-/AC-/FOB/FNJ etc.) unchanged.
# MAGIC --
# MAGIC -- 🛡️ Changes scope vs v9.4.21:
# MAGIC --   - Layer 5: added UOM2 shortage CTEs (cum_demand_uom2_before, etc.)
# MAGIC --   - Final SELECT: shortage_uom1/final_uom1/final_uom2 wrapped with UOM2 check
# MAGIC -- All other logic UNCHANGED.
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC 
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead = LEGACY;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInWrite = LEGACY;
# MAGIC SET spark.sql.parquet.int96RebaseModeInRead = LEGACY;
# MAGIC SET spark.sql.parquet.int96RebaseModeInWrite = LEGACY;
# MAGIC 
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.fabric_requisition_line
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- LAYER 0: Ship date lookup (UNCHANGED)
# MAGIC -- =====================================================
# MAGIC production_order_dedup AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `No.`, Status, `BC Company`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn_po
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Production Order`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND Status IN ('Released', 'Firm Planned', 'Planned')
# MAGIC           AND `Sales Order No.` IS NOT NULL
# MAGIC           AND `Sales Order No.` <> ''
# MAGIC     )
# MAGIC     WHERE rn_po = 1
# MAGIC ),
# MAGIC 
# MAGIC sales_line_dedup AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `Document No.`, `Document Type`, `Line No.`, `BC Company`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn_sl
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Sales Line`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND `Document Type` = 'Order'
# MAGIC           AND Type = 'Item'
# MAGIC           AND `Outstanding Quantity` > 0
# MAGIC     )
# MAGIC     WHERE rn_sl = 1
# MAGIC ),
# MAGIC 
# MAGIC ship_date_lookup AS (
# MAGIC     SELECT
# MAGIC         v.component_item_no                          AS item_no,
# MAGIC         DATE_TRUNC('week', v.required_date)          AS bucket_start_date,
# MAGIC         MIN(sl.`Shipment Date`)                      AS sales_line_ship_date
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Material_Availability v
# MAGIC     INNER JOIN production_order_dedup po
# MAGIC         ON po.`No.` = v.source_record_id
# MAGIC        AND po.`Status` = v.source_status
# MAGIC     INNER JOIN sales_line_dedup sl
# MAGIC         ON sl.`Document No.` = po.`Sales Order No.`
# MAGIC        AND sl.`No.`          = po.`Source No.`
# MAGIC     WHERE v.demand_type = 'FIRMED_BOM'
# MAGIC       AND v.required_qty > 0
# MAGIC       AND v.required_date IS NOT NULL
# MAGIC     GROUP BY v.component_item_no, DATE_TRUNC('week', v.required_date)
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- LAYER 0.5: PRO Component dedup (UNCHANGED)
# MAGIC -- =====================================================
# MAGIC pro_component_dedup AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `Prod. Order No.`, `Prod. Order Line No.`, `Line No.`, `BC Company`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Prod Order Component`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND Status IN ('Released', 'Firm Planned', 'Planned')
# MAGIC           AND `Remaining Quantity` > 0
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- LAYER 0.6: UOM2 lookup from gold_consumption_status (UNCHANGED from v9.4.21)
# MAGIC -- =====================================================
# MAGIC consumption_uom2_line_level AS (
# MAGIC     SELECT
# MAGIC         ProdOrderNo                              AS pro_no,
# MAGIC         ProdOrderLineNo                          AS parent_line_no,
# MAGIC         ComponentItemNo                          AS comp_item_no,
# MAGIC         SUM(
# MAGIC             COALESCE(exp2_from_pc_original, exp2) + COALESCE(con2, 0)
# MAGIC         )                                        AS remaining_units_uom2,
# MAGIC         MAX(ComponentUOM)                        AS uom2_code
# MAGIC     FROM Gold_Inventory_Lakehouse.inv.gold_consumption_status
# MAGIC     WHERE ProdOrderNo     IS NOT NULL
# MAGIC       AND ComponentItemNo IS NOT NULL
# MAGIC     GROUP BY
# MAGIC         ProdOrderNo,
# MAGIC         ProdOrderLineNo,
# MAGIC         ComponentItemNo
# MAGIC ),
# MAGIC 
# MAGIC firmed_bom_uom2_lookup AS (
# MAGIC     SELECT
# MAGIC         v.component_item_no                          AS item_no,
# MAGIC         DATE_TRUNC('week', v.required_date)          AS bucket_start_date,
# MAGIC         SUM(cu.remaining_units_uom2)                 AS firmed_bom_qty_uom2
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Material_Availability v
# MAGIC     INNER JOIN consumption_uom2_line_level cu
# MAGIC         ON cu.pro_no         = v.source_record_id
# MAGIC        AND cu.comp_item_no   = v.component_item_no
# MAGIC        AND cu.parent_line_no = TRY_CAST(v.source_line_id AS INT)
# MAGIC     WHERE v.demand_type = 'FIRMED_BOM'
# MAGIC       AND v.required_qty > 0
# MAGIC       AND v.required_date IS NOT NULL
# MAGIC     GROUP BY v.component_item_no, DATE_TRUNC('week', v.required_date)
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- LAYER 0.7: Primary customer per bucket (UNCHANGED)
# MAGIC -- =====================================================
# MAGIC customer_per_bucket_qty AS (
# MAGIC     SELECT
# MAGIC         v.component_item_no                  AS item_no,
# MAGIC         DATE_TRUNC('week', v.required_date)  AS bucket_start_date,
# MAGIC         v.customer_no,
# MAGIC         v.customer_name,
# MAGIC         SUM(v.required_qty)                  AS total_qty
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Material_Availability v
# MAGIC     WHERE v.demand_type = 'FIRMED_BOM'
# MAGIC       AND v.required_qty > 0
# MAGIC       AND v.required_date IS NOT NULL
# MAGIC       AND v.customer_no IS NOT NULL
# MAGIC     GROUP BY
# MAGIC         v.component_item_no,
# MAGIC         DATE_TRUNC('week', v.required_date),
# MAGIC         v.customer_no,
# MAGIC         v.customer_name
# MAGIC ),
# MAGIC 
# MAGIC primary_customer AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         bucket_start_date,
# MAGIC         customer_no       AS primary_customer_no,
# MAGIC         customer_name     AS primary_customer_name,
# MAGIC         CONCAT(customer_no, ' ', customer_name) AS customer_display
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             item_no,
# MAGIC             bucket_start_date,
# MAGIC             customer_no,
# MAGIC             customer_name,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY item_no, bucket_start_date
# MAGIC                 ORDER BY total_qty DESC, customer_no ASC
# MAGIC             ) AS rn
# MAGIC         FROM customer_per_bucket_qty
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC customer_count_per_bucket AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         bucket_start_date,
# MAGIC         COUNT(DISTINCT customer_no)          AS customer_count
# MAGIC     FROM customer_per_bucket_qty
# MAGIC     GROUP BY item_no, bucket_start_date
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- LAYER 1: V6 demand bucketing (UNCHANGED)
# MAGIC -- =====================================================
# MAGIC v6_bucketed_raw AS (
# MAGIC     SELECT
# MAGIC         v.component_item_no                 AS item_no,
# MAGIC         DATE_TRUNC('week', v.required_date) AS bucket_start_date,
# MAGIC         MAX(v.component_description)        AS description,
# MAGIC         MAX(v.item_category)                AS item_category_code,
# MAGIC         MAX(v.material_type)                AS material_type,
# MAGIC         MAX(v.metal_category)               AS metal_category,
# MAGIC         MAX(v.is_critical_item)             AS is_critical_item,
# MAGIC         MAX(v.base_uom)                     AS base_uom_uom1,
# MAGIC 
# MAGIC         SUM(CASE WHEN v.demand_type = 'FIRMED_BOM'
# MAGIC                  THEN v.required_qty ELSE 0 END)         AS demand_quantity_uom1_bucket,
# MAGIC 
# MAGIC         SUM(CASE WHEN v.demand_type = 'FIRMED_BOM' THEN 1 ELSE 0 END)
# MAGIC                                                         AS demand_line_count_in_bucket,
# MAGIC         MIN(CASE WHEN v.demand_type = 'FIRMED_BOM' THEN v.required_date END)
# MAGIC                                                         AS bucket_earliest_demand_date,
# MAGIC         MAX(CASE WHEN v.demand_type = 'FIRMED_BOM' THEN v.required_date END)
# MAGIC                                                         AS bucket_latest_demand_date,
# MAGIC         MIN(CASE WHEN v.demand_type = 'FIRMED_BOM' THEN v.required_date END)
# MAGIC                                                         AS due_date,
# MAGIC         MIN(CASE WHEN v.demand_type = 'FIRMED_BOM' THEN v.order_by_date END)
# MAGIC                                                         AS earliest_order_by_date,
# MAGIC 
# MAGIC         SUM(CASE WHEN v.demand_type = 'FIRMED_BOM'        
# MAGIC                  THEN v.required_qty ELSE 0 END) AS firmed_bom_qty,
# MAGIC         SUM(CASE WHEN v.demand_type = 'POLICY_TRIGGERED'  
# MAGIC                  THEN v.required_qty ELSE 0 END) AS policy_triggered_qty,
# MAGIC         SUM(CASE WHEN v.demand_type = 'PLANNED_BOM'      
# MAGIC                  THEN v.required_qty ELSE 0 END) AS planned_bom_qty,
# MAGIC 
# MAGIC         SUM(CASE WHEN v.demand_type = 'FIRMED_BOM' 
# MAGIC                       AND COALESCE(v.is_dnd, FALSE) = TRUE
# MAGIC                  THEN v.required_qty ELSE 0 END)
# MAGIC                                                 AS dnd_demand_qty,
# MAGIC 
# MAGIC         BOOL_OR(CASE WHEN v.demand_type = 'FIRMED_BOM' 
# MAGIC                           AND COALESCE(v.is_dnd, FALSE) = TRUE
# MAGIC                       THEN TRUE ELSE FALSE END)
# MAGIC                                                 AS has_dnd_demand,
# MAGIC 
# MAGIC         MAX(v.customer_no)                  AS customer_no_max,
# MAGIC         BOOL_OR(COALESCE(v.is_dnd, FALSE))  AS is_dnd,
# MAGIC         MAX(v.priority_score)               AS max_priority_score,
# MAGIC         MAX(v.preferred_vendor_no)          AS vendor_no,
# MAGIC         MAX(v.preferred_vendor_name)        AS vendor_name,
# MAGIC         MAX(v.last_direct_cost)             AS v6_fallback_unit_cost,
# MAGIC         MAX(v.lead_time_days)               AS lead_time_days
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Material_Availability v
# MAGIC     WHERE v.required_qty > 0
# MAGIC       AND v.required_date IS NOT NULL
# MAGIC     GROUP BY
# MAGIC         v.component_item_no,
# MAGIC         DATE_TRUNC('week', v.required_date)
# MAGIC ),
# MAGIC 
# MAGIC v6_bucketed AS (
# MAGIC     SELECT
# MAGIC         v.*,
# MAGIC         CONCAT(
# MAGIC             CAST(EXTRACT(YEAROFWEEK FROM v.bucket_start_date) AS STRING),
# MAGIC             '-W',
# MAGIC             LPAD(CAST(EXTRACT(WEEK FROM v.bucket_start_date) AS STRING), 2, '0')
# MAGIC         )                                            AS bucket_week,
# MAGIC         sdl.sales_line_ship_date,
# MAGIC         COALESCE(fbu.firmed_bom_qty_uom2, 0)         AS firmed_bom_qty_uom2,
# MAGIC         pc.primary_customer_no                       AS customer_no,
# MAGIC         pc.primary_customer_name                     AS customer_name,
# MAGIC         pc.customer_display                          AS customer_display,
# MAGIC         COALESCE(ccb.customer_count, 0)              AS customer_count
# MAGIC     FROM v6_bucketed_raw v
# MAGIC     LEFT JOIN ship_date_lookup sdl
# MAGIC         ON sdl.item_no           = v.item_no
# MAGIC        AND sdl.bucket_start_date = v.bucket_start_date
# MAGIC     LEFT JOIN firmed_bom_uom2_lookup fbu
# MAGIC         ON fbu.item_no           = v.item_no
# MAGIC        AND fbu.bucket_start_date = v.bucket_start_date
# MAGIC     LEFT JOIN primary_customer pc
# MAGIC         ON pc.item_no            = v.item_no
# MAGIC        AND pc.bucket_start_date  = v.bucket_start_date
# MAGIC     LEFT JOIN customer_count_per_bucket ccb
# MAGIC         ON ccb.item_no           = v.item_no
# MAGIC        AND ccb.bucket_start_date = v.bucket_start_date
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- LAYER 2: Item Master + module + UOM2 code (UNCHANGED)
# MAGIC -- =====================================================
# MAGIC bc_item_uom2 AS (
# MAGIC     SELECT
# MAGIC         `No.` AS item_no,
# MAGIC         `Unit of Measure - Units_DU_TSL` AS item_uom2_code
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `No.`, `BC Company`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Item`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC item_planning AS (
# MAGIC     SELECT
# MAGIC         im.item_no,
# MAGIC         im.reordering_policy,
# MAGIC         im.replenishment_system,
# MAGIC         im.archetype,
# MAGIC         im.rop_qty                          AS reorder_point_uom1,
# MAGIC         im.roq_qty                          AS reorder_quantity_uom1,
# MAGIC         im.max_inv_qty                      AS maximum_inventory_uom1,
# MAGIC         im.safety_stock_qty                 AS safety_stock_uom1,
# MAGIC         im.min_order_qty                    AS min_order_qty,
# MAGIC         im.max_order_qty                    AS max_order_qty,
# MAGIC         im.order_multiple                   AS order_multiple,
# MAGIC         im.scrap_pct                        AS scrap_pct,
# MAGIC         im.effective_lead_days,
# MAGIC         im.purch_uom                        AS secondary_uom_uom2,
# MAGIC         bcu.item_uom2_code,
# MAGIC         CASE
# MAGIC             WHEN im.item_category IN (
# MAGIC                 'MIXED METAL', 'CASTING', 'MST',
# MAGIC                 'PURE METAL', 'ALLOY', 'SOLDER', 'SEMI-FG'
# MAGIC             ) THEN 'METAL_ALLOY'
# MAGIC             ELSE 'COMPONENT'
# MAGIC         END                                 AS module
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Item_Master im
# MAGIC     LEFT JOIN bc_item_uom2 bcu
# MAGIC         ON bcu.item_no = im.item_no
# MAGIC     WHERE im.is_blocked = FALSE
# MAGIC       AND COALESCE(im.purchasing_blocked, FALSE) = FALSE
# MAGIC       AND im.archetype <> 'SKIP'
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- LAYER 2.5: Stock location aggregation (UNCHANGED)
# MAGIC -- =====================================================
# MAGIC stock_location_lookup AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         CONCAT_WS(', ', COLLECT_SET(location_code)) AS best_stock_location
# MAGIC     FROM (
# MAGIC         SELECT DISTINCT
# MAGIC             inv.item_no,
# MAGIC             inv.location_code
# MAGIC         FROM Gold_Inventory_Lakehouse.mrp.gold_inventory inv
# MAGIC         WHERE inv.qty_on_hand > 0
# MAGIC           AND COALESCE(inv.is_blocked_location, FALSE) = FALSE
# MAGIC           AND inv.location_code IN (
# MAGIC               'BAGGING','CASTING','CONSUME','CST_CUT','CST_ROOM','CZ-SYNT',
# MAGIC               'DEBEERS','DIA-LAB','DIA-NAT','EQUIP','FG-NO-PO','FINDINGS',
# MAGIC               'FIN-GOODS','GEMS','KIMAI','MATERIAL','OBSOLETE','OTHERS-MAT',
# MAGIC               'PACKAGING','PEARLS','PLATING','POMELATO','PRE ALLOY','RETURNS',
# MAGIC               'RUB MOLD','SEMI-F','SORTING','STONE-CUT','STR','TOOLS','WAX ROOM'
# MAGIC           )
# MAGIC     )
# MAGIC     GROUP BY item_no
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- LAYER 3: Inventory ATP (UNCHANGED from v9.4.21)
# MAGIC -- =====================================================
# MAGIC ile_stock_uom2 AS (
# MAGIC     SELECT
# MAGIC         ile.`Item No.` AS item_no,
# MAGIC         SUM(CAST(ile.`Remaining Quantity` AS DECIMAL(38,10))) AS onhand_uom1_from_ile,
# MAGIC         SUM(CAST(ile.`Units_DU_TSL` AS DECIMAL(38,10)))       AS onhand_uom2
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` ile
# MAGIC     INNER JOIN Gold_Inventory_Lakehouse.mrp.gold_inventory gi
# MAGIC         ON gi.item_no       = ile.`Item No.`
# MAGIC        AND gi.location_code = ile.`Location Code`
# MAGIC        AND gi.lot_no        = ile.`Lot No.`
# MAGIC     WHERE ile.`BC Company` = 'Ennovie'
# MAGIC       AND ile.`Location Code` IN (
# MAGIC           'BAGGING','CASTING','CONSUME','CST_CUT','CST_ROOM','CZ-SYNT',
# MAGIC           'DEBEERS','DIA-LAB','DIA-NAT','EQUIP','FG-NO-PO','FINDINGS',
# MAGIC           'FIN-GOODS','GEMS','KIMAI','MATERIAL','OBSOLETE','OTHERS-MAT',
# MAGIC           'PACKAGING','PEARLS','PLATING','POMELATO','PRE ALLOY','RETURNS',
# MAGIC           'RUB MOLD','SEMI-F','SORTING','STONE-CUT','STR','TOOLS','WAX ROOM'
# MAGIC       )
# MAGIC       AND COALESCE(gi.is_blocked_location, FALSE) = FALSE
# MAGIC     GROUP BY ile.`Item No.`
# MAGIC ),
# MAGIC 
# MAGIC active_pro_consumption AS (
# MAGIC     SELECT
# MAGIC         c.`Item No.`                AS item_no,
# MAGIC         SUM(c.`Remaining Quantity`) AS pro_committed_qty,
# MAGIC         SUM(CAST(c.`Units_DU_TSL` AS DECIMAL(38,10))) AS pro_committed_qty_uom2
# MAGIC     FROM pro_component_dedup c
# MAGIC     INNER JOIN Gold_Inventory_Lakehouse.mrp.gold_Item_Master im
# MAGIC             ON im.item_no = c.`Item No.`
# MAGIC     WHERE im.archetype <> 'SKIP'
# MAGIC     GROUP BY c.`Item No.`
# MAGIC ),
# MAGIC 
# MAGIC inventory_atp AS (
# MAGIC     SELECT
# MAGIC         inv.item_no,
# MAGIC         SUM(inv.qty_on_hand)                                AS onhand_quantity_uom1,
# MAGIC         SUM(inv.qty_available)                              AS available_qty_gross,
# MAGIC         COALESCE(MAX(apc.pro_committed_qty), 0)             AS pro_committed_qty,
# MAGIC         COALESCE(MAX(apc.pro_committed_qty_uom2), 0)        AS pro_committed_qty_uom2,
# MAGIC         COALESCE(MAX(isu.onhand_uom2), 0)                   AS onhand_quantity_uom2,
# MAGIC         SUM(inv.qty_available)                              AS available_qty_atp,
# MAGIC         COALESCE(MAX(isu.onhand_uom2), 0)                   AS available_qty_atp_uom2
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_inventory inv
# MAGIC     LEFT JOIN active_pro_consumption apc ON apc.item_no = inv.item_no
# MAGIC     LEFT JOIN ile_stock_uom2 isu          ON isu.item_no = inv.item_no
# MAGIC     WHERE COALESCE(inv.is_blocked_location, FALSE) = FALSE
# MAGIC       AND inv.location_code IN (
# MAGIC           'BAGGING','CASTING','CONSUME','CST_CUT','CST_ROOM','CZ-SYNT',
# MAGIC           'DEBEERS','DIA-LAB','DIA-NAT','EQUIP','FG-NO-PO','FINDINGS',
# MAGIC           'FIN-GOODS','GEMS','KIMAI','MATERIAL','OBSOLETE','OTHERS-MAT',
# MAGIC           'PACKAGING','PEARLS','PLATING','POMELATO','PRE ALLOY','RETURNS',
# MAGIC           'RUB MOLD','SEMI-F','SORTING','STONE-CUT','STR','TOOLS','WAX ROOM'
# MAGIC       )
# MAGIC     GROUP BY inv.item_no
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- LAYER 4: PO incoming (UNCHANGED)
# MAGIC -- =====================================================
# MAGIC purchase_header_dedup AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `No.`, `Document Type`, `BC Company`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn_ph
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Purchase Header`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND `Document Type` = 'Order'
# MAGIC     )
# MAGIC     WHERE rn_ph = 1
# MAGIC ),
# MAGIC 
# MAGIC purchase_line_dedup AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `Document No.`, `Document Type`, `Line No.`, `BC Company`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn_pl
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Purchase Line`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND `Document Type` = 'Order'
# MAGIC           AND `Outstanding Quantity` > 0
# MAGIC           AND `Expected Receipt Date` > DATE'1900-01-01'
# MAGIC           AND Type = 'Item'
# MAGIC     )
# MAGIC     WHERE rn_pl = 1
# MAGIC ),
# MAGIC 
# MAGIC po_lines_with_status AS (
# MAGIC     SELECT
# MAGIC         pl.`No.`                              AS item_no,
# MAGIC         ph.`No.`                              AS po_no,
# MAGIC         pl.`Line No.`                         AS line_no,
# MAGIC         ph.`Status`                           AS po_status,
# MAGIC         pl.`Outstanding Qty. (Base)`          AS qty,
# MAGIC         COALESCE(
# MAGIC             CAST(pl.`Outstanding Units_DU_TSL` AS DECIMAL(38,10)),
# MAGIC             0
# MAGIC         )                                     AS qty_uom2,
# MAGIC         pl.`Direct Unit Cost`                 AS direct_unit_cost,
# MAGIC         pl.`Expected Receipt Date`            AS receipt_date,
# MAGIC         ph.`Document Date`                    AS doc_date
# MAGIC     FROM purchase_line_dedup pl
# MAGIC     INNER JOIN purchase_header_dedup ph
# MAGIC         ON ph.`No.` = pl.`Document No.`
# MAGIC        AND ph.`Document Type` = pl.`Document Type`
# MAGIC        AND ph.`BC Company` = pl.`BC Company`
# MAGIC ),
# MAGIC 
# MAGIC po_released_agg AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         SUM(qty) AS qty_released,
# MAGIC         SUM(qty_uom2) AS qty_released_uom2,
# MAGIC         MIN(receipt_date) AS earliest_released_receipt,
# MAGIC         COUNT(DISTINCT po_no) AS released_po_count
# MAGIC     FROM po_lines_with_status WHERE po_status = 'Released' GROUP BY item_no
# MAGIC ),
# MAGIC 
# MAGIC po_open_agg AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         SUM(qty) AS qty_open,
# MAGIC         SUM(qty_uom2) AS qty_open_uom2,
# MAGIC         MIN(receipt_date) AS earliest_open_receipt,
# MAGIC         COUNT(DISTINCT po_no) AS open_po_count
# MAGIC     FROM po_lines_with_status WHERE po_status = 'Open' GROUP BY item_no
# MAGIC ),
# MAGIC 
# MAGIC latest_released_po AS (
# MAGIC     SELECT item_no, direct_unit_cost AS released_unit_cost
# MAGIC     FROM (
# MAGIC         SELECT item_no, direct_unit_cost,
# MAGIC             ROW_NUMBER() OVER (PARTITION BY item_no ORDER BY doc_date DESC, line_no DESC) AS rn
# MAGIC         FROM po_lines_with_status WHERE po_status = 'Released'
# MAGIC     ) WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC latest_open_po AS (
# MAGIC     SELECT item_no, direct_unit_cost AS open_unit_cost
# MAGIC     FROM (
# MAGIC         SELECT item_no, direct_unit_cost,
# MAGIC             ROW_NUMBER() OVER (PARTITION BY item_no ORDER BY doc_date DESC, line_no DESC) AS rn
# MAGIC         FROM po_lines_with_status WHERE po_status = 'Open'
# MAGIC     ) WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC ile_avg AS (
# MAGIC     SELECT `Item No.` AS item_no, SUM(ABS(Quantity)) / 180.0 AS avg_daily_usage
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry`
# MAGIC     WHERE `BC Company` = 'Ennovie'
# MAGIC       AND `Entry Type` = 'Consumption'
# MAGIC       AND `Posting Date` >= ADD_MONTHS(CURRENT_DATE(), -6)
# MAGIC     GROUP BY `Item No.`
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- LAYER 5: Combine + running window — v9.4.22 ADDS UOM2 cumulative
# MAGIC -- =====================================================
# MAGIC v6_with_running AS (
# MAGIC     SELECT
# MAGIC         v.*,
# MAGIC         ip.module,
# MAGIC         ip.item_uom2_code,
# MAGIC         ip.reordering_policy,
# MAGIC         ip.replenishment_system,
# MAGIC         ip.archetype,
# MAGIC         ip.reorder_point_uom1,
# MAGIC         ip.reorder_quantity_uom1,
# MAGIC         ip.maximum_inventory_uom1,
# MAGIC         ip.safety_stock_uom1,
# MAGIC         ip.min_order_qty,
# MAGIC         ip.max_order_qty,
# MAGIC         ip.order_multiple,
# MAGIC         ip.effective_lead_days,
# MAGIC         ip.secondary_uom_uom2,
# MAGIC         sll.best_stock_location,
# MAGIC         COALESCE(inv.onhand_quantity_uom1, 0)        AS onhand_quantity_uom1,
# MAGIC         COALESCE(inv.onhand_quantity_uom2, 0)        AS onhand_quantity_uom2,
# MAGIC         COALESCE(inv.available_qty_atp, 0)           AS available_qty_atp,
# MAGIC         COALESCE(inv.available_qty_atp_uom2, 0)      AS available_qty_atp_uom2,
# MAGIC         COALESCE(inv.pro_committed_qty, 0)           AS pro_committed_qty,
# MAGIC         COALESCE(inv.pro_committed_qty_uom2, 0)      AS pro_committed_qty_uom2,
# MAGIC         COALESCE(por.qty_released, 0)                AS incoming_po_released_qty,
# MAGIC         COALESCE(por.qty_released_uom2, 0)           AS incoming_po_released_qty_uom2,
# MAGIC         COALESCE(poo.qty_open, 0)                    AS incoming_po_open_qty,
# MAGIC         COALESCE(poo.qty_open_uom2, 0)               AS incoming_po_open_qty_uom2,
# MAGIC         COALESCE(por.earliest_released_receipt, poo.earliest_open_receipt)
# MAGIC                                                      AS earliest_po_receipt_date,
# MAGIC         COALESCE(por.released_po_count, 0)           AS released_po_count,
# MAGIC         COALESCE(poo.open_po_count, 0)               AS open_po_count,
# MAGIC         COALESCE(
# MAGIC             lrp.released_unit_cost, lop.open_unit_cost, v.v6_fallback_unit_cost
# MAGIC         )                                            AS unit_cost,
# MAGIC         CASE
# MAGIC             WHEN lrp.released_unit_cost IS NOT NULL THEN 'RELEASED_PO'
# MAGIC             WHEN lop.open_unit_cost IS NOT NULL     THEN 'OPEN_PO'
# MAGIC             ELSE 'ITEM_MASTER'
# MAGIC         END                                          AS unit_cost_source,
# MAGIC         COALESCE(ila.avg_daily_usage, 0)             AS avg_daily_usage,
# MAGIC 
# MAGIC         -- UOM1 cumulative (UNCHANGED)
# MAGIC         SUM(v.demand_quantity_uom1_bucket) OVER (
# MAGIC             PARTITION BY v.item_no ORDER BY v.bucket_start_date
# MAGIC             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC         )                                            AS cumulative_demand_thru_bucket,
# MAGIC         SUM(v.demand_quantity_uom1_bucket) OVER (
# MAGIC             PARTITION BY v.item_no ORDER BY v.bucket_start_date
# MAGIC             ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
# MAGIC         )                                            AS cumulative_demand_before_bucket,
# MAGIC 
# MAGIC         -- 🆕 v9.4.22: UOM2 cumulative for dual-UOM shortage check
# MAGIC         SUM(v.firmed_bom_qty_uom2) OVER (
# MAGIC             PARTITION BY v.item_no ORDER BY v.bucket_start_date
# MAGIC             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC         )                                            AS cumulative_demand_uom2_thru_bucket,
# MAGIC         SUM(v.firmed_bom_qty_uom2) OVER (
# MAGIC             PARTITION BY v.item_no ORDER BY v.bucket_start_date
# MAGIC             ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
# MAGIC         )                                            AS cumulative_demand_uom2_before_bucket,
# MAGIC 
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY v.item_no
# MAGIC             ORDER BY v.bucket_start_date
# MAGIC         )                                            AS bucket_rank,
# MAGIC         SUM(v.demand_quantity_uom1_bucket) OVER (
# MAGIC             PARTITION BY v.item_no
# MAGIC         )                                            AS item_total_demand_uom1,
# MAGIC         SUM(v.firmed_bom_qty_uom2) OVER (
# MAGIC             PARTITION BY v.item_no
# MAGIC         )                                            AS item_total_firmed_demand_uom2
# MAGIC     FROM v6_bucketed v
# MAGIC     LEFT JOIN item_planning ip      ON ip.item_no = v.item_no
# MAGIC     LEFT JOIN stock_location_lookup sll ON sll.item_no = v.item_no
# MAGIC     LEFT JOIN inventory_atp inv     ON inv.item_no = v.item_no
# MAGIC     LEFT JOIN po_released_agg por   ON por.item_no = v.item_no
# MAGIC     LEFT JOIN po_open_agg poo       ON poo.item_no = v.item_no
# MAGIC     LEFT JOIN latest_released_po lrp ON lrp.item_no = v.item_no
# MAGIC     LEFT JOIN latest_open_po lop     ON lop.item_no = v.item_no
# MAGIC     LEFT JOIN ile_avg ila           ON ila.item_no = v.item_no
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- v9.4.22 shortage logic — UOM1 (existing) + UOM2 (new)
# MAGIC -- =====================================================
# MAGIC v6_with_policy AS (
# MAGIC     SELECT *,
# MAGIC         -- UOM1 shortage (UNCHANGED — kept for single-UOM items)
# MAGIC         GREATEST(
# MAGIC             available_qty_atp + incoming_po_released_qty + incoming_po_open_qty
# MAGIC                 - COALESCE(cumulative_demand_before_bucket, 0), 0
# MAGIC         )                                            AS available_before_bucket_uom1,
# MAGIC         available_qty_atp + incoming_po_released_qty + incoming_po_open_qty
# MAGIC             - cumulative_demand_thru_bucket          AS net_position_after_bucket,
# MAGIC         GREATEST(
# MAGIC             demand_quantity_uom1_bucket
# MAGIC                 - GREATEST(available_qty_atp + incoming_po_released_qty + incoming_po_open_qty
# MAGIC                     - COALESCE(cumulative_demand_before_bucket, 0), 0), 0
# MAGIC         )                                            AS shortage_quantity_uom1_raw,
# MAGIC         GREATEST(
# MAGIC             available_qty_atp + incoming_po_released_qty + incoming_po_open_qty
# MAGIC                 - COALESCE(cumulative_demand_before_bucket, 0), 0
# MAGIC         )                                            AS available_before_bucket_if_open,
# MAGIC         GREATEST(
# MAGIC             demand_quantity_uom1_bucket
# MAGIC                 - GREATEST(available_qty_atp
# MAGIC                     + incoming_po_released_qty + incoming_po_open_qty
# MAGIC                     - COALESCE(cumulative_demand_before_bucket, 0), 0), 0
# MAGIC         )                                            AS shortage_qty_if_open_released,
# MAGIC 
# MAGIC         -- 🆕 v9.4.22: UOM2 shortage (for dual-UOM items)
# MAGIC         GREATEST(
# MAGIC             available_qty_atp_uom2 + incoming_po_released_qty_uom2 + incoming_po_open_qty_uom2
# MAGIC                 - COALESCE(cumulative_demand_uom2_before_bucket, 0), 0
# MAGIC         )                                            AS available_before_bucket_uom2,
# MAGIC         GREATEST(
# MAGIC             firmed_bom_qty_uom2
# MAGIC                 - GREATEST(available_qty_atp_uom2 + incoming_po_released_qty_uom2 + incoming_po_open_qty_uom2
# MAGIC                     - COALESCE(cumulative_demand_uom2_before_bucket, 0), 0), 0
# MAGIC         )                                            AS shortage_quantity_uom2_raw
# MAGIC     FROM v6_with_running
# MAGIC ),
# MAGIC 
# MAGIC -- 🆕 v9.4.22: UOM2-first override for dual-UOM items
# MAGIC v6_with_uom2_override AS (
# MAGIC     SELECT *,
# MAGIC         -- If item is dual-UOM AND UOM2 supply is sufficient → suppress UOM1 alert
# MAGIC         -- For single-UOM items: shortage_uom1 stays as raw value
# MAGIC         CASE
# MAGIC             WHEN item_uom2_code IS NOT NULL          -- dual-UOM item
# MAGIC                  AND shortage_quantity_uom2_raw <= 0  -- UOM2 sufficient
# MAGIC                 THEN 0                                -- ← override to "no shortage"
# MAGIC             ELSE shortage_quantity_uom1_raw           -- UOM1-only logic
# MAGIC         END                                          AS shortage_quantity_uom1
# MAGIC     FROM v6_with_policy
# MAGIC ),
# MAGIC 
# MAGIC v6_with_lot_sizing AS (
# MAGIC     SELECT *,
# MAGIC         CASE
# MAGIC             WHEN reordering_policy = 'Lot-for-Lot'        
# MAGIC                 THEN shortage_quantity_uom1 > 0
# MAGIC             WHEN reordering_policy = 'Fixed Reorder Qty.' THEN
# MAGIC                 shortage_quantity_uom1 > 0
# MAGIC             ELSE shortage_quantity_uom1 > 0
# MAGIC         END                                          AS should_trigger,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN reordering_policy = 'Lot-for-Lot' THEN shortage_quantity_uom1
# MAGIC             WHEN reordering_policy = 'Fixed Reorder Qty.' THEN
# MAGIC                 CASE
# MAGIC                     WHEN shortage_quantity_uom1 > 0
# MAGIC                         THEN shortage_quantity_uom1
# MAGIC                     ELSE 0
# MAGIC                 END
# MAGIC             ELSE GREATEST(shortage_quantity_uom1, 0)
# MAGIC         END                                          AS policy_quantity_uom1,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN reordering_policy = 'Lot-for-Lot' THEN shortage_qty_if_open_released
# MAGIC             WHEN reordering_policy = 'Fixed Reorder Qty.' THEN
# MAGIC                 CASE
# MAGIC                     WHEN shortage_qty_if_open_released > 0
# MAGIC                         THEN shortage_qty_if_open_released
# MAGIC                     ELSE 0
# MAGIC                 END
# MAGIC             ELSE GREATEST(shortage_qty_if_open_released, 0)
# MAGIC         END                                          AS policy_qty_if_open_released
# MAGIC     FROM v6_with_uom2_override
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- LAYER 6: Stage calculation (UNCHANGED)
# MAGIC -- =====================================================
# MAGIC v6_with_stages AS (
# MAGIC     SELECT *,
# MAGIC         CASE WHEN sales_line_ship_date IS NOT NULL
# MAGIC              THEN count_working_days(CURRENT_DATE(), sales_line_ship_date)
# MAGIC              ELSE NULL
# MAGIC         END                                          AS available_working_days,
# MAGIC         CASE WHEN sales_line_ship_date IS NOT NULL
# MAGIC              THEN calc_wax_start(sales_line_ship_date, CURRENT_DATE())
# MAGIC              ELSE NULL
# MAGIC         END                                          AS wax_start,
# MAGIC         CASE WHEN sales_line_ship_date IS NOT NULL
# MAGIC              THEN calc_semi_start(sales_line_ship_date, CURRENT_DATE())
# MAGIC              ELSE NULL
# MAGIC         END                                          AS semi_start
# MAGIC     FROM v6_with_lot_sizing
# MAGIC ),
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- LAYER 7: Order date computation (UNCHANGED)
# MAGIC -- =====================================================
# MAGIC v6_with_order_date AS (
# MAGIC     SELECT *,
# MAGIC         CASE
# MAGIC             WHEN sales_line_ship_date IS NULL THEN
# MAGIC                 DATE_SUB(due_date, COALESCE(effective_lead_days, lead_time_days, 14))
# MAGIC             WHEN module = 'METAL_ALLOY' THEN
# MAGIC                 sub_working_days(wax_start, COALESCE(effective_lead_days, lead_time_days, 14))
# MAGIC             ELSE
# MAGIC                 sub_working_days(semi_start, COALESCE(effective_lead_days, lead_time_days, 14))
# MAGIC         END                                          AS suggested_order_date_raw
# MAGIC     FROM v6_with_stages
# MAGIC ),
# MAGIC 
# MAGIC v6_with_order_dates_full AS (
# MAGIC     SELECT *,
# MAGIC         GREATEST(suggested_order_date_raw, CURRENT_DATE()) AS suggested_order_date_actionable_calc
# MAGIC     FROM v6_with_order_date
# MAGIC )
# MAGIC 
# MAGIC -- =====================================================
# MAGIC -- FINAL SELECT — schema identical to v9.4.21
# MAGIC -- v9.4.22 changes only the VALUES of shortage_quantity_uom1, final_uom1, final_uom2
# MAGIC -- when item is dual-UOM
# MAGIC -- =====================================================
# MAGIC SELECT
# MAGIC     UUID()                                           AS systemID,
# MAGIC     'Ennovie'                                        AS bc_company,
# MAGIC     item_no,
# MAGIC     CAST(NULL AS STRING)                             AS variant_code,
# MAGIC     bucket_week,
# MAGIC     bucket_start_date,
# MAGIC     DATE_ADD(bucket_start_date, 6)                   AS bucket_end_date,
# MAGIC     description,
# MAGIC     item_category_code,
# MAGIC     material_type,
# MAGIC     metal_category,
# MAGIC     is_critical_item                                 AS is_critical,
# MAGIC     base_uom_uom1,
# MAGIC     secondary_uom_uom2,
# MAGIC     item_uom2_code,
# MAGIC     module,
# MAGIC 
# MAGIC     demand_quantity_uom1_bucket,
# MAGIC     demand_line_count_in_bucket,
# MAGIC     bucket_earliest_demand_date,
# MAGIC     bucket_latest_demand_date,
# MAGIC     firmed_bom_qty,
# MAGIC     firmed_bom_qty_uom2,
# MAGIC     policy_triggered_qty,
# MAGIC     planned_bom_qty,
# MAGIC 
# MAGIC     dnd_demand_qty,
# MAGIC     has_dnd_demand,
# MAGIC 
# MAGIC     onhand_quantity_uom1,
# MAGIC     onhand_quantity_uom2,
# MAGIC     available_qty_atp                                AS available_qty_atp_uom1,
# MAGIC     available_qty_atp_uom2,
# MAGIC     pro_committed_qty,
# MAGIC     pro_committed_qty_uom2,
# MAGIC     available_before_bucket_uom1,
# MAGIC     net_position_after_bucket                        AS available_after_bucket_uom1,
# MAGIC     shortage_quantity_uom1,                          -- ⭐ v9.4.22: overridden if dual-UOM & UOM2 sufficient
# MAGIC 
# MAGIC     reordering_policy,
# MAGIC     archetype,
# MAGIC     reorder_point_uom1,
# MAGIC     reorder_quantity_uom1,
# MAGIC     maximum_inventory_uom1,
# MAGIC     safety_stock_uom1,
# MAGIC     min_order_qty,
# MAGIC     max_order_qty,
# MAGIC     order_multiple,
# MAGIC 
# MAGIC     incoming_po_released_qty,
# MAGIC     incoming_po_released_qty_uom2,
# MAGIC     incoming_po_open_qty,
# MAGIC     incoming_po_open_qty_uom2,
# MAGIC     earliest_po_receipt_date,
# MAGIC     released_po_count,
# MAGIC     open_po_count,
# MAGIC     (incoming_po_open_qty > 0)                       AS has_pending_open_po,
# MAGIC 
# MAGIC     bucket_rank,
# MAGIC 
# MAGIC     should_trigger,
# MAGIC     policy_quantity_uom1,
# MAGIC 
# MAGIC     -- final_uom1: UNCHANGED formula (per user choice Q2 — keeps v9.4.21 logic)
# MAGIC     -- For dual-UOM items where UOM2 satisfied → should_trigger=FALSE → 0
# MAGIC     -- For dual-UOM items where UOM2 short → uses UOM1 shortage formula
# MAGIC     CASE
# MAGIC         WHEN NOT should_trigger THEN 0
# MAGIC         ELSE CEIL(
# MAGIC                 LEAST(
# MAGIC                     GREATEST(COALESCE(min_order_qty, 0), policy_quantity_uom1),
# MAGIC                     COALESCE(NULLIF(max_order_qty, 0), policy_quantity_uom1)
# MAGIC                 ) / GREATEST(order_multiple, 1)
# MAGIC              ) * GREATEST(order_multiple, 1)
# MAGIC     END                                              AS final_uom1,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN NOT should_trigger THEN 0
# MAGIC         ELSE CEIL(
# MAGIC                 LEAST(
# MAGIC                     GREATEST(COALESCE(min_order_qty, 0), policy_qty_if_open_released),
# MAGIC                     COALESCE(NULLIF(max_order_qty, 0), policy_qty_if_open_released)
# MAGIC                 ) / GREATEST(order_multiple, 1)
# MAGIC              ) * GREATEST(order_multiple, 1)
# MAGIC     END                                              AS suggested_qty_if_open_released,
# MAGIC 
# MAGIC     -- ⭐ v9.4.22: final_uom2 logic
# MAGIC     -- Dual-UOM items: use shortage_quantity_uom2_raw (UOM2-based) when triggered
# MAGIC     -- Single-UOM items: fall back to v9.4.21 ratio formula (final_uom1 × stock ratio)
# MAGIC     CASE
# MAGIC         WHEN NOT should_trigger THEN CAST(0 AS DECIMAL(38,20))
# MAGIC         WHEN item_uom2_code IS NOT NULL AND shortage_quantity_uom2_raw > 0 THEN
# MAGIC             -- 🆕 Dual-UOM: use UOM2 shortage directly + apply order constraints
# MAGIC             CAST(
# MAGIC                 CEIL(shortage_quantity_uom2_raw) AS DECIMAL(38,20)
# MAGIC             )
# MAGIC         WHEN onhand_quantity_uom1 > 0 AND onhand_quantity_uom2 > 0 THEN
# MAGIC             -- Single-UOM fallback (v9.4.21 formula)
# MAGIC             CAST(
# MAGIC                 CEIL(
# MAGIC                     LEAST(
# MAGIC                         GREATEST(COALESCE(min_order_qty, 0), policy_quantity_uom1),
# MAGIC                         COALESCE(NULLIF(max_order_qty, 0), policy_quantity_uom1)
# MAGIC                     ) / GREATEST(order_multiple, 1)
# MAGIC                 ) * GREATEST(order_multiple, 1)
# MAGIC                 * (onhand_quantity_uom2 / onhand_quantity_uom1)
# MAGIC                 AS DECIMAL(38,20)
# MAGIC             )
# MAGIC         ELSE CAST(NULL AS DECIMAL(38,20))
# MAGIC     END                                              AS final_uom2,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN NOT should_trigger THEN CAST(0 AS DECIMAL(38,20))
# MAGIC         WHEN item_uom2_code IS NOT NULL AND shortage_quantity_uom2_raw > 0 THEN
# MAGIC             CAST(CEIL(shortage_quantity_uom2_raw) AS DECIMAL(38,20))
# MAGIC         WHEN onhand_quantity_uom1 > 0 AND onhand_quantity_uom2 > 0 THEN
# MAGIC             CAST(
# MAGIC                 CEIL(
# MAGIC                     LEAST(
# MAGIC                         GREATEST(COALESCE(min_order_qty, 0), policy_qty_if_open_released),
# MAGIC                         COALESCE(NULLIF(max_order_qty, 0), policy_qty_if_open_released)
# MAGIC                     ) / GREATEST(order_multiple, 1)
# MAGIC                 ) * GREATEST(order_multiple, 1)
# MAGIC                 * (onhand_quantity_uom2 / onhand_quantity_uom1)
# MAGIC                 AS DECIMAL(38,20)
# MAGIC             )
# MAGIC         ELSE CAST(NULL AS DECIMAL(38,20))
# MAGIC     END                                              AS suggested_qty_if_open_released_uom2,
# MAGIC 
# MAGIC     lead_time_days,
# MAGIC     effective_lead_days,
# MAGIC 
# MAGIC     sales_line_ship_date,
# MAGIC     available_working_days,
# MAGIC     wax_start,
# MAGIC     semi_start,
# MAGIC 
# MAGIC     suggested_order_date_raw                         AS suggested_order_date,
# MAGIC     suggested_order_date_actionable_calc             AS suggested_order_date_actionable,
# MAGIC     CONCAT(
# MAGIC         CAST(EXTRACT(YEAROFWEEK FROM suggested_order_date_raw) AS STRING),
# MAGIC         '-W',
# MAGIC         LPAD(CAST(EXTRACT(WEEK FROM suggested_order_date_raw) AS STRING), 2, '0')
# MAGIC     )                                                AS suggested_order_week,
# MAGIC     CONCAT(
# MAGIC         CAST(EXTRACT(YEAROFWEEK FROM suggested_order_date_actionable_calc) AS STRING),
# MAGIC         '-W',
# MAGIC         LPAD(CAST(EXTRACT(WEEK FROM suggested_order_date_actionable_calc) AS STRING), 2, '0')
# MAGIC     )                                                AS suggested_order_week_actionable,
# MAGIC 
# MAGIC     due_date,
# MAGIC     earliest_order_by_date                           AS order_by_date,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN sales_line_ship_date IS NULL                                     THEN 'NO_SHIP_DATE'
# MAGIC         WHEN suggested_order_date_raw < CURRENT_DATE()                        THEN 'OVERDUE'
# MAGIC         WHEN suggested_order_date_raw <= DATE_ADD(CURRENT_DATE(), 7)          THEN 'URGENT'
# MAGIC         WHEN suggested_order_date_raw <= DATE_ADD(CURRENT_DATE(), 30)         THEN 'SOON'
# MAGIC         ELSE 'OK'
# MAGIC     END                                              AS po_urgency,
# MAGIC 
# MAGIC     avg_daily_usage,
# MAGIC     ROUND(avg_daily_usage * 60, 0)                   AS forecast_demand_60d,
# MAGIC     CASE
# MAGIC         WHEN avg_daily_usage > 0 THEN
# MAGIC             DATE_ADD(CURRENT_DATE(),
# MAGIC                 CAST(GREATEST(onhand_quantity_uom1 + incoming_po_released_qty, 0)
# MAGIC                     / NULLIF(avg_daily_usage, 0) AS INT))
# MAGIC         ELSE NULL
# MAGIC     END                                              AS projected_stockout_date,
# MAGIC 
# MAGIC     customer_no,
# MAGIC     customer_name,
# MAGIC     customer_display,
# MAGIC     customer_count,
# MAGIC 
# MAGIC     is_dnd,
# MAGIC     max_priority_score                               AS priority_score,
# MAGIC 
# MAGIC     vendor_no,
# MAGIC     vendor_name,
# MAGIC     unit_cost,
# MAGIC     unit_cost_source,
# MAGIC     ROUND(
# MAGIC         CASE WHEN NOT should_trigger THEN 0
# MAGIC              ELSE policy_quantity_uom1 * COALESCE(unit_cost, 0)
# MAGIC         END, 2
# MAGIC     )                                                AS cost_amount,
# MAGIC 
# MAGIC     best_stock_location,
# MAGIC     replenishment_system,
# MAGIC     item_total_demand_uom1,
# MAGIC     item_total_firmed_demand_uom2,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN onhand_quantity_uom1 = 0 AND demand_quantity_uom1_bucket > 0 THEN 'STOCKOUT'
# MAGIC         WHEN onhand_quantity_uom1 < COALESCE(safety_stock_uom1, 0) AND COALESCE(safety_stock_uom1, 0) > 0 THEN 'BELOW_SAFETY'
# MAGIC         WHEN onhand_quantity_uom1 < COALESCE(reorder_point_uom1, 0) AND COALESCE(reorder_point_uom1, 0) > 0 THEN 'BELOW_ROP'
# MAGIC         WHEN should_trigger AND due_date <= DATE_ADD(CURRENT_DATE(), 14) THEN 'NEEDS_ORDER'
# MAGIC         ELSE 'OK'
# MAGIC     END                                              AS alert_flag,
# MAGIC 
# MAGIC     'v9.4.22'                                        AS source_version,    -- ⭐ BUMPED
# MAGIC     CURRENT_TIMESTAMP()                              AS view_built_at
# MAGIC 
# MAGIC FROM v6_with_order_dates_full
# MAGIC WHERE module IS NOT NULL;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- 🆕 v9.4.22 verification — UOM2-first shortage logic
# MAGIC -- ============================================================================
# MAGIC 
# MAGIC -- VC1: FCG-000610-18KWPD canary — dual-UOM item (item_uom2_code = 'CM')
# MAGIC -- Expected: shortage_quantity_uom1 = 0 when UOM2 sufficient
# MAGIC SELECT
# MAGIC     item_no, bucket_week,
# MAGIC     item_uom2_code,
# MAGIC     demand_quantity_uom1_bucket          AS demand_uom1_gr,
# MAGIC     firmed_bom_qty_uom2                  AS demand_uom2_cm,
# MAGIC     onhand_quantity_uom1                 AS onhand_gr,
# MAGIC     onhand_quantity_uom2                 AS onhand_cm,
# MAGIC     available_before_bucket_uom1         AS avail_before_uom1,
# MAGIC     shortage_quantity_uom1               AS shortage_gr,
# MAGIC     final_uom1                           AS suggested_gr,
# MAGIC     final_uom2                           AS suggested_cm,
# MAGIC     alert_flag
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.fabric_requisition_line
# MAGIC WHERE item_no = 'FCG-000610-18KWPD'
# MAGIC ORDER BY bucket_week;
# MAGIC -- ✅ Expected for dual-UOM W30: final_uom2 ≈ 467 CM (UOM2-based, not 731 from ratio)
# MAGIC 
# MAGIC 
# MAGIC -- VC2: Single-UOM canary — C0- or SM- prefix (PCS only)
# MAGIC -- Expected: behavior unchanged from v9.4.21
# MAGIC SELECT
# MAGIC     item_no, bucket_week,
# MAGIC     item_uom2_code,                      -- should be NULL
# MAGIC     demand_quantity_uom1_bucket          AS demand,
# MAGIC     onhand_quantity_uom1                 AS onhand,
# MAGIC     shortage_quantity_uom1               AS shortage,
# MAGIC     final_uom1                           AS suggested,
# MAGIC     alert_flag
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.fabric_requisition_line
# MAGIC WHERE item_no LIKE 'C0%'
# MAGIC   AND shortage_quantity_uom1 > 0
# MAGIC LIMIT 5;
# MAGIC 
# MAGIC 
# MAGIC -- VC3: Compare alerts dropped due to UOM2 override
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_buckets,
# MAGIC     SUM(CASE WHEN item_uom2_code IS NOT NULL THEN 1 ELSE 0 END) AS dual_uom_buckets,
# MAGIC     SUM(CASE WHEN item_uom2_code IS NOT NULL AND shortage_quantity_uom1 = 0 THEN 1 ELSE 0 END)
# MAGIC         AS dual_uom_no_alert,
# MAGIC     SUM(CASE WHEN should_trigger THEN 1 ELSE 0 END) AS triggered_total
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.fabric_requisition_line
# MAGIC WHERE firmed_bom_qty > 0;
# MAGIC 
# MAGIC 
# MAGIC -- VC4: Cascade timestamp
# MAGIC SELECT 'V6'                          AS layer, MAX(view_built_at) AS last_built
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Material_Availability
# MAGIC UNION ALL
# MAGIC SELECT 'fabric_req v9.4.22', MAX(view_built_at)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.fabric_requisition_line;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # gold_pur_demand_detail

# CELL ********************

# MAGIC %%sql
# MAGIC -- ============================================================================
# MAGIC -- nb_gold_pur_demand_detail v3.3.3 · Fix bug 1 + bug 2
# MAGIC -- ============================================================================
# MAGIC -- v3.3.3 (2026-05-19):
# MAGIC --   🔧 BUG 1 FIX: remaining_uom2 returns NULL when item doesn't track UOM2
# MAGIC --                (when expected_uom2 = 0). Previously returned negative values
# MAGIC --                for Wire items (FNWI) that have con2 but no exp2.
# MAGIC --                Example FNWI000468-00002 / WRO260301248:
# MAGIC --                  Before: 0 + (-5) = -5 ❌
# MAGIC --                  After:  NULL (item doesn't track UOM2) ✅
# MAGIC --
# MAGIC --   🔧 BUG 2 FIX: item_uom2_code uses COALESCE chain:
# MAGIC --                 1st: Prod Order Component (PRO snapshot, matches BC UI)
# MAGIC --                 2nd: BC Item master (fallback when POC blank)
# MAGIC --                Covers items where POC has UOM2 code blank but Item master has it.
# MAGIC --
# MAGIC -- v3.3 logic preserved:
# MAGIC --   - UOM2 calc from gold_consumption_status.exp2_from_pc_original
# MAGIC --   - All columns from v3.1 preserved
# MAGIC -- ============================================================================
# MAGIC 
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInWrite = CORRECTED;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead  = CORRECTED;
# MAGIC 
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.gold_pur_demand_detail
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'false',
# MAGIC   'description' = 'PUR drill-down v3.3.3: UOM2 from gold_consumption_status. Bug fixes: NULL guard for non-UOM2 items + COALESCE chain for item_uom2_code.'
# MAGIC )
# MAGIC AS
# MAGIC WITH
# MAGIC -- Vendor lookup
# MAGIC bc_vendor_lookup AS (
# MAGIC   SELECT vendor_no, vendor_name
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       `No.` AS vendor_no,
# MAGIC       Name AS vendor_name,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         PARTITION BY `No.`, `BC Company`
# MAGIC         ORDER BY SystemRowVersion DESC
# MAGIC       ) AS rn
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Vendor`
# MAGIC     WHERE `BC Company` = 'Ennovie'
# MAGIC   )
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC -- Customer lookup
# MAGIC bc_customer_lookup AS (
# MAGIC   SELECT customer_no, customer_name
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       `No.` AS customer_no,
# MAGIC       Name AS customer_name,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         PARTITION BY `No.`, `BC Company`
# MAGIC         ORDER BY SystemRowVersion DESC
# MAGIC       ) AS rn
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Customer`
# MAGIC     WHERE `BC Company` = 'Ennovie'
# MAGIC   )
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC -- 🔧 v3.3.2: UOM2 code from PRO Component (primary, matches BC UI)
# MAGIC poc_uom2_code_lookup AS (
# MAGIC   SELECT
# MAGIC     `Prod. Order No.`                    AS pro_no,
# MAGIC     `Prod. Order Line No.`               AS parent_line_no,
# MAGIC     `Item No.`                           AS comp_item_no,
# MAGIC     MAX(`Unit of Measure - Units_DU_TSL`) AS uom2_code
# MAGIC   FROM Silver_BC_Lakehouse.bc.`Prod Order Component`
# MAGIC   WHERE `BC Company` = 'Ennovie'
# MAGIC     AND `Unit of Measure - Units_DU_TSL` IS NOT NULL
# MAGIC     AND `Unit of Measure - Units_DU_TSL` <> ''
# MAGIC   GROUP BY
# MAGIC     `Prod. Order No.`,
# MAGIC     `Prod. Order Line No.`,
# MAGIC     `Item No.`
# MAGIC ),
# MAGIC 
# MAGIC -- 🆕 v3.3.3: UOM2 code from BC Item master (fallback for items without POC code)
# MAGIC bc_item_uom2_code AS (
# MAGIC   SELECT
# MAGIC     `No.`                              AS item_no,
# MAGIC     `Unit of Measure - Units_DU_TSL`   AS item_uom2_code
# MAGIC   FROM (
# MAGIC     SELECT *,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         PARTITION BY `No.`, `BC Company`
# MAGIC         ORDER BY SystemRowVersion DESC
# MAGIC       ) AS rn
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item`
# MAGIC     WHERE `BC Company` = 'Ennovie'
# MAGIC       AND `Unit of Measure - Units_DU_TSL` IS NOT NULL
# MAGIC       AND `Unit of Measure - Units_DU_TSL` <> ''
# MAGIC   )
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC -- 🔧 v3.3.3 BUG 1 FIX: UOM2 calc with NULL guard
# MAGIC -- Previously: SUM(COALESCE(exp2_from_pc_original, exp2) + con2)
# MAGIC --             returned negative values when item doesn't track UOM2
# MAGIC --             (exp2 = 0 but con2 < 0, e.g. FNWI Wire items)
# MAGIC -- Now: per-row CASE — if no UOM2 expected → NULL, else compute as before
# MAGIC consumption_uom2_lookup AS (
# MAGIC   SELECT
# MAGIC     ProdOrderNo                  AS pro_no,
# MAGIC     ProdOrderLineNo              AS parent_line_no,
# MAGIC     ComponentItemNo              AS comp_item_no,
# MAGIC 
# MAGIC     -- 🔧 BUG 1 FIX: return NULL when item doesn't track UOM2
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN COALESCE(exp2_from_pc_original, exp2, 0) = 0
# MAGIC           THEN NULL
# MAGIC         ELSE COALESCE(exp2_from_pc_original, exp2) + COALESCE(con2, 0)
# MAGIC       END
# MAGIC     )                            AS remaining_uom2,
# MAGIC 
# MAGIC     SUM(Consump1)                AS remaining_uom1_check,
# MAGIC 
# MAGIC     -- Breakdown (NULL when no UOM2 tracking)
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN COALESCE(exp2_from_pc_original, exp2, 0) = 0 THEN NULL
# MAGIC         ELSE COALESCE(exp2_from_pc_original, exp2)
# MAGIC       END
# MAGIC     )                            AS expected_uom2,
# MAGIC 
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN COALESCE(exp2_from_pc_original, exp2, 0) = 0 THEN NULL
# MAGIC         ELSE exp2
# MAGIC       END
# MAGIC     )                            AS expected_uom2_bom_current,
# MAGIC 
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN COALESCE(exp2_from_pc_original, exp2, 0) = 0 THEN NULL
# MAGIC         ELSE con2
# MAGIC       END
# MAGIC     )                            AS consumed_uom2,
# MAGIC 
# MAGIC     MAX(ComponentUOM)            AS uom1_code,
# MAGIC     MAX(Consump_Status)          AS consumption_status,
# MAGIC     MAX(drift_level)             AS drift_level
# MAGIC   FROM Gold_Inventory_Lakehouse.inv.gold_consumption_status
# MAGIC   WHERE ProdOrderNo     IS NOT NULL
# MAGIC     AND ComponentItemNo IS NOT NULL
# MAGIC   GROUP BY
# MAGIC     ProdOrderNo,
# MAGIC     ProdOrderLineNo,
# MAGIC     ComponentItemNo
# MAGIC )
# MAGIC SELECT
# MAGIC   -- ========== KEY FILTERS ==========
# MAGIC   v.component_item_no                          AS item_no,
# MAGIC   CONCAT(
# MAGIC     CAST(EXTRACT(YEAROFWEEK FROM v.required_date) AS STRING),
# MAGIC     '-W',
# MAGIC     LPAD(CAST(EXTRACT(WEEK FROM v.required_date) AS STRING), 2, '0')
# MAGIC   )                                            AS bucket_week,
# MAGIC   DATE_TRUNC('week', v.required_date)          AS bucket_start_date,
# MAGIC 
# MAGIC   -- ========== DEMAND IDENTITY ==========
# MAGIC   v.required_date,
# MAGIC   v.required_qty,
# MAGIC   v.demand_type,
# MAGIC 
# MAGIC   -- ========== SOURCE ORDER (v3.1 split) ==========
# MAGIC   CASE
# MAGIC     WHEN v.demand_type = 'FIRMED_BOM'
# MAGIC       THEN v.source_record_id
# MAGIC     ELSE NULL
# MAGIC   END                                          AS pro_no,
# MAGIC 
# MAGIC   CASE
# MAGIC     WHEN v.demand_type IN ('PLANNED_BOM', 'POLICY_TRIGGERED')
# MAGIC       THEN v.source_record_id
# MAGIC     ELSE NULL
# MAGIC   END                                          AS planning_ref,
# MAGIC 
# MAGIC   v.source_status                              AS pro_status,
# MAGIC   v.source_so_no                               AS sales_order_no,
# MAGIC   v.produced_item_no                           AS parent_fg,
# MAGIC   v.source_description                         AS parent_fg_description,
# MAGIC 
# MAGIC   v.customer_no,
# MAGIC   customer.customer_name                       AS customer_name,
# MAGIC 
# MAGIC   -- ========== UOM2 ==========
# MAGIC   consumption.remaining_uom2                   AS required_qty_uom2,
# MAGIC 
# MAGIC   -- 🔧 BUG 2 FIX: COALESCE chain — POC first, Item master fallback
# MAGIC   COALESCE(poc_uom.uom2_code, bcu.item_uom2_code) AS item_uom2_code,
# MAGIC 
# MAGIC   consumption.expected_uom2                    AS expected_uom2,
# MAGIC   consumption.expected_uom2_bom_current        AS expected_uom2_bom_current,
# MAGIC   ABS(consumption.consumed_uom2)               AS consumed_uom2,
# MAGIC   consumption.consumption_status               AS consumption_status,
# MAGIC   consumption.drift_level                      AS bom_drift_level,
# MAGIC 
# MAGIC   -- ========== VENDOR & COST ==========
# MAGIC   v.preferred_vendor_no                        AS vendor_no,
# MAGIC   vendor.vendor_name                           AS vendor_name,
# MAGIC   v.qty_per                                    AS bom_ratio,
# MAGIC   v.last_direct_cost                           AS unit_cost,
# MAGIC   v.estimated_order_value_thb                  AS est_value,
# MAGIC 
# MAGIC   -- ========== ITEM CONTEXT ==========
# MAGIC   v.component_description                      AS item_description,
# MAGIC   v.item_category                              AS item_category_code,
# MAGIC   v.base_uom                                   AS base_uom,
# MAGIC   v.material_type,
# MAGIC   v.metal_category,
# MAGIC   v.is_critical_item                           AS is_critical,
# MAGIC   v.is_dnd,
# MAGIC 
# MAGIC   -- ========== ORDER METADATA ==========
# MAGIC   v.coverage_status,
# MAGIC   v.order_status,
# MAGIC   v.order_by_date,
# MAGIC 
# MAGIC   -- ========== METADATA ==========
# MAGIC   current_timestamp()                          AS materialized_at,
# MAGIC   'v3.3.3'                                     AS source_version
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_Material_Availability v
# MAGIC LEFT JOIN bc_vendor_lookup vendor
# MAGIC   ON vendor.vendor_no = v.preferred_vendor_no
# MAGIC LEFT JOIN bc_customer_lookup customer
# MAGIC   ON customer.customer_no = v.customer_no
# MAGIC LEFT JOIN poc_uom2_code_lookup poc_uom
# MAGIC   ON poc_uom.pro_no         = v.source_record_id
# MAGIC  AND poc_uom.comp_item_no   = v.component_item_no
# MAGIC  AND poc_uom.parent_line_no = TRY_CAST(v.source_line_id AS INT)
# MAGIC LEFT JOIN bc_item_uom2_code bcu                                          -- 🆕 v3.3.3
# MAGIC   ON bcu.item_no = v.component_item_no
# MAGIC LEFT JOIN consumption_uom2_lookup consumption
# MAGIC   ON consumption.pro_no         = v.source_record_id
# MAGIC  AND consumption.comp_item_no   = v.component_item_no
# MAGIC  AND consumption.parent_line_no = TRY_CAST(v.source_line_id AS INT)
# MAGIC WHERE v.required_qty > 0
# MAGIC   AND v.required_date IS NOT NULL;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- VIEW
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE VIEW Gold_Inventory_Lakehouse.mrp.v_pur_demand_detail AS
# MAGIC SELECT *
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_pur_demand_detail;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- VERIFY v3.3.3
# MAGIC -- ============================================================================
# MAGIC SELECT
# MAGIC     '✅ DEPLOYED v3.3.3' AS status,
# MAGIC     
# MAGIC     (SELECT COUNT(*) FROM Gold_Inventory_Lakehouse.mrp.gold_pur_demand_detail)
# MAGIC         AS total_rows,
# MAGIC     
# MAGIC     -- Canary 1: FCG-000265-18KR (unchanged behavior, should still be 2.20)
# MAGIC     (SELECT required_qty_uom2
# MAGIC      FROM Gold_Inventory_Lakehouse.mrp.gold_pur_demand_detail
# MAGIC      WHERE item_no = 'FCG-000265-18KR' AND pro_no = 'WRO260305106' LIMIT 1)
# MAGIC         AS canary_fcg_uom2,             -- expect 2.20
# MAGIC     (SELECT item_uom2_code
# MAGIC      FROM Gold_Inventory_Lakehouse.mrp.gold_pur_demand_detail
# MAGIC      WHERE item_no = 'FCG-000265-18KR' AND pro_no = 'WRO260305106' LIMIT 1)
# MAGIC         AS canary_fcg_code,             -- expect 'CM'
# MAGIC     
# MAGIC     -- 🆕 Canary 2: FNWI Wire (bug 1 fix — should now be NULL, not negative)
# MAGIC     (SELECT required_qty_uom2
# MAGIC      FROM Gold_Inventory_Lakehouse.mrp.gold_pur_demand_detail
# MAGIC      WHERE item_no = 'FNWI000468-00002' AND pro_no = 'WRO260301248' LIMIT 1)
# MAGIC         AS canary_fnwi_uom2,            -- expect NULL (was -5 in v3.3.2)
# MAGIC     
# MAGIC     -- 🆕 Coverage check
# MAGIC     (SELECT COUNT(*) FROM Gold_Inventory_Lakehouse.mrp.gold_pur_demand_detail
# MAGIC      WHERE required_qty_uom2 < -0.0001)
# MAGIC         AS negative_uom2_rows,          -- expect ~0 (was 16)
# MAGIC     
# MAGIC     -- item_uom2_code coverage with COALESCE fallback
# MAGIC     (SELECT ROUND(
# MAGIC        100.0 * SUM(CASE WHEN item_uom2_code IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*),
# MAGIC        2
# MAGIC      ) FROM Gold_Inventory_Lakehouse.mrp.gold_pur_demand_detail
# MAGIC      WHERE demand_type = 'FIRMED_BOM')
# MAGIC         AS pct_firmed_uom2_code_filled,  -- expect higher than 23%
# MAGIC     
# MAGIC     -- Scenario matrix
# MAGIC     (SELECT COUNT(*) FROM Gold_Inventory_Lakehouse.mrp.gold_pur_demand_detail
# MAGIC      WHERE demand_type = 'FIRMED_BOM'
# MAGIC        AND item_uom2_code IS NOT NULL AND required_qty_uom2 IS NOT NULL)
# MAGIC         AS both_filled,
# MAGIC     (SELECT COUNT(*) FROM Gold_Inventory_Lakehouse.mrp.gold_pur_demand_detail
# MAGIC      WHERE demand_type = 'FIRMED_BOM'
# MAGIC        AND item_uom2_code IS NULL AND required_qty_uom2 IS NOT NULL)
# MAGIC         AS value_only,                   -- should drop dramatically
# MAGIC     (SELECT COUNT(*) FROM Gold_Inventory_Lakehouse.mrp.gold_pur_demand_detail
# MAGIC      WHERE demand_type = 'FIRMED_BOM'
# MAGIC        AND item_uom2_code IS NULL AND required_qty_uom2 IS NULL)
# MAGIC         AS both_null;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# # gold_pur_oversupply_alert

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- nb_gold_pur_oversupply_alert v1.4 · MASTER/DETAIL SPLIT
# MAGIC -- ============================================================================
# MAGIC -- Outputs:
# MAGIC --   1. Gold_Inventory_Lakehouse.mrp.gold_pur_oversupply_alert         (master)
# MAGIC --   2. Gold_Inventory_Lakehouse.mrp.gold_pur_oversupply_alert_detail  (detail) 🆕
# MAGIC --
# MAGIC -- 🆕 v1.4 CHANGES (2026-05-13):
# MAGIC --   1. REMOVED from master (list columns):
# MAGIC --      - released_po_nos, released_po_dates
# MAGIC --      - released_expected_dates, released_promised_dates
# MAGIC --      - released_po_detail
# MAGIC --      - open_po_nos, open_po_dates
# MAGIC --      - open_expected_dates, open_promised_dates
# MAGIC --      - open_po_detail
# MAGIC --      → Reason: caused MISMATCH between released_po_count (distinct PO from fabric_req)
# MAGIC --                and SIZE(SPLIT(po_nos)) (line-level). DI-RD-000379 case verified:
# MAGIC --                fabric_req=2 distinct POs, list=3 entries (PO2604-0204 appears twice).
# MAGIC --
# MAGIC --   2. KEPT in master (single-value summaries):
# MAGIC --      - mrp_suggested_order_date, days_to_mrp_suggested
# MAGIC --      - released_po_count, open_po_count
# MAGIC --      - earliest/latest release PO date, expected, promised
# MAGIC --      - earliest/latest open PO date, expected, promised
# MAGIC --
# MAGIC --   3. NEW table `gold_pur_oversupply_alert_detail`:
# MAGIC --      - 1 row per PO line (item × po_no × line_no)
# MAGIC --      - For drill-down display when user expands master row
# MAGIC --      - Schema: item_no, po_no, line_no, po_status, doc_date, expected_receipt,
# MAGIC --        promised_receipt, outstanding_qty, outstanding_qty_base, vendor_no,
# MAGIC --        vendor_name
# MAGIC --
# MAGIC -- 🆕 v1.3 CHANGES (superseded):
# MAGIC --   + PO numbers in lists (replaced by detail table)
# MAGIC -- 🆕 v1.2 CHANGES (preserved):
# MAGIC --   + Promised Receipt Date alongside Expected
# MAGIC -- 🆕 v1.1 CHANGES (preserved):
# MAGIC --   + mrp_suggested_order_date + earliest/latest date columns
# MAGIC --
# MAGIC -- CASCADE:
# MAGIC --   Source: fabric_requisition_line v9.4.13+ AND BC Purchase Header/Line
# MAGIC --   Refresh order: V6 → fabric_req → demand_detail → pur_summary → oversupply_alert
# MAGIC --   Run AFTER fabric_requisition_line rebuild.
# MAGIC --
# MAGIC -- DATE: 2026-05-13
# MAGIC -- ============================================================================
# MAGIC 
# MAGIC 
# MAGIC -- Environment
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInWrite = CORRECTED;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead  = CORRECTED;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- 📦 TABLE 1: gold_pur_oversupply_alert (MASTER) v1.4
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.gold_pur_oversupply_alert
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'false',
# MAGIC   'description' = 'PUR over-supply MASTER v1.4. 1 row per item. List columns removed — use gold_pur_oversupply_alert_detail for PO line breakdown.'
# MAGIC )
# MAGIC AS
# MAGIC WITH
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- LAYER A: MRP-suggested order date (from fabric_req)
# MAGIC -- ============================================================
# MAGIC mrp_dates AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         MIN(suggested_order_date_actionable)   AS mrp_suggested_order_date
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.fabric_requisition_line
# MAGIC     WHERE should_trigger = TRUE
# MAGIC       AND suggested_order_date_actionable IS NOT NULL
# MAGIC     GROUP BY item_no
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- LAYER B: PO date lookups (Released + Open separately)
# MAGIC -- ============================================================
# MAGIC purchase_header_dedup AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `No.`, `Document Type`, `BC Company`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn_ph
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Purchase Header`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND `Document Type` = 'Order'
# MAGIC     )
# MAGIC     WHERE rn_ph = 1
# MAGIC ),
# MAGIC 
# MAGIC purchase_line_dedup AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `Document No.`, `Document Type`, `Line No.`, `BC Company`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn_pl
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Purchase Line`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND `Document Type` = 'Order'
# MAGIC           AND `Outstanding Quantity` > 0
# MAGIC           AND `Expected Receipt Date` > DATE'1900-01-01'
# MAGIC           AND Type = 'Item'
# MAGIC     )
# MAGIC     WHERE rn_pl = 1
# MAGIC ),
# MAGIC 
# MAGIC po_dates_per_line AS (
# MAGIC     SELECT
# MAGIC         pl.`No.`                              AS item_no,
# MAGIC         ph.`No.`                              AS po_no,
# MAGIC         pl.`Line No.`                         AS line_no,
# MAGIC         ph.`Status`                           AS po_status,
# MAGIC         ph.`Document Date`                    AS po_doc_date,
# MAGIC         pl.`Expected Receipt Date`            AS po_expected_receipt,
# MAGIC         pl.`Promised Receipt Date`            AS po_promised_receipt,
# MAGIC         pl.`Outstanding Quantity`             AS outstanding_qty,
# MAGIC         pl.`Outstanding Qty. (Base)`          AS outstanding_qty_base,
# MAGIC         ph.`Buy-from Vendor No.`              AS vendor_no
# MAGIC     FROM purchase_line_dedup pl
# MAGIC     INNER JOIN purchase_header_dedup ph
# MAGIC         ON ph.`No.`           = pl.`Document No.`
# MAGIC        AND ph.`Document Type` = pl.`Document Type`
# MAGIC        AND ph.`BC Company`    = pl.`BC Company`
# MAGIC ),
# MAGIC 
# MAGIC -- Released: 1 row per item with summary dates only
# MAGIC released_po_summary AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         COUNT(DISTINCT po_no)         AS released_po_count,
# MAGIC         COUNT(*)                      AS released_line_count,
# MAGIC         MIN(po_doc_date)              AS earliest_released_po_date,
# MAGIC         MAX(po_doc_date)              AS latest_released_po_date,
# MAGIC         MIN(po_expected_receipt)      AS earliest_released_expected,
# MAGIC         MAX(po_expected_receipt)      AS latest_released_expected,
# MAGIC         MIN(po_promised_receipt)      AS earliest_released_promised,
# MAGIC         MAX(po_promised_receipt)      AS latest_released_promised
# MAGIC     FROM po_dates_per_line
# MAGIC     WHERE po_status = 'Released'
# MAGIC     GROUP BY item_no
# MAGIC ),
# MAGIC 
# MAGIC -- Open: same shape
# MAGIC open_po_summary AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         COUNT(DISTINCT po_no)         AS open_po_count,
# MAGIC         COUNT(*)                      AS open_line_count,
# MAGIC         MIN(po_doc_date)              AS earliest_open_po_date,
# MAGIC         MAX(po_doc_date)              AS latest_open_po_date,
# MAGIC         MIN(po_expected_receipt)      AS earliest_open_expected,
# MAGIC         MAX(po_expected_receipt)      AS latest_open_expected,
# MAGIC         MIN(po_promised_receipt)      AS earliest_open_promised,
# MAGIC         MAX(po_promised_receipt)      AS latest_open_promised
# MAGIC     FROM po_dates_per_line
# MAGIC     WHERE po_status = 'Open'
# MAGIC     GROUP BY item_no
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- LAYER C: Per-item rollup from fabric_req (unchanged from v1.3)
# MAGIC -- ============================================================
# MAGIC per_item AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         MAX(description)                                    AS description,
# MAGIC         MAX(item_category_code)                             AS item_category,
# MAGIC         MAX(material_type)                                  AS material_type,
# MAGIC         MAX(metal_category)                                 AS metal_category,
# MAGIC         MAX(base_uom_uom1)                                  AS base_uom,
# MAGIC         MAX(item_uom2_code)                                 AS uom2_code,
# MAGIC         MAX(reordering_policy)                              AS reordering_policy,
# MAGIC         MAX(archetype)                                      AS archetype,
# MAGIC         MAX(replenishment_system)                           AS replenishment_system,
# MAGIC         MAX(vendor_no)                                      AS vendor_no,
# MAGIC         MAX(vendor_name)                                    AS vendor_name,
# MAGIC         MAX(best_stock_location)                            AS best_stock_location,
# MAGIC         MAX(onhand_quantity_uom1)                           AS on_hand_uom1,
# MAGIC         MAX(onhand_quantity_uom2)                           AS on_hand_uom2,
# MAGIC         MAX(incoming_po_released_qty)                       AS incoming_released_uom1,
# MAGIC         MAX(incoming_po_open_qty)                           AS incoming_open_uom1,
# MAGIC         MAX(incoming_po_released_qty_uom2)                  AS incoming_released_uom2,
# MAGIC         MAX(incoming_po_open_qty_uom2)                      AS incoming_open_uom2,
# MAGIC         MAX(earliest_po_receipt_date)                       AS earliest_incoming_date,
# MAGIC         MAX(item_total_demand_uom1)                         AS total_demand_uom1,
# MAGIC         MAX(item_total_firmed_demand_uom2)                  AS total_firmed_demand_uom2,
# MAGIC         COUNT(*)                                            AS bucket_count,
# MAGIC         MIN(bucket_start_date)                              AS earliest_demand_bucket,
# MAGIC         MAX(bucket_start_date)                              AS latest_demand_bucket,
# MAGIC         MAX(CASE WHEN should_trigger THEN 1 ELSE 0 END)     AS has_trigger,
# MAGIC         SUM(final_uom1)                                     AS engine_buy_signal_uom1,
# MAGIC         MAX(alert_flag)                                     AS engine_alert_flag,
# MAGIC         MAX(unit_cost)                                      AS unit_cost,
# MAGIC         MAX(unit_cost_source)                               AS unit_cost_source,
# MAGIC         MAX(customer_no)                                    AS customer_no,
# MAGIC         MAX(CASE WHEN is_dnd THEN 1 ELSE 0 END) = 1         AS is_dnd,
# MAGIC         MAX(CASE WHEN is_critical THEN 1 ELSE 0 END) = 1    AS is_critical
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.fabric_requisition_line
# MAGIC     GROUP BY item_no
# MAGIC ),
# MAGIC 
# MAGIC with_calculations AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         on_hand_uom1 + incoming_released_uom1 + incoming_open_uom1            AS total_supply_uom1,
# MAGIC         on_hand_uom1 + incoming_released_uom1 + incoming_open_uom1
# MAGIC             - COALESCE(total_demand_uom1, 0)                                  AS excess_qty_uom1,
# MAGIC         CASE WHEN COALESCE(total_demand_uom1, 0) > 0
# MAGIC              THEN ROUND((on_hand_uom1 + incoming_released_uom1 + incoming_open_uom1)
# MAGIC                        / total_demand_uom1, 2)
# MAGIC              ELSE NULL
# MAGIC         END                                                                   AS supply_ratio,
# MAGIC         on_hand_uom2 + incoming_released_uom2 + incoming_open_uom2            AS total_supply_uom2,
# MAGIC         on_hand_uom2 + incoming_released_uom2 + incoming_open_uom2
# MAGIC             - COALESCE(total_firmed_demand_uom2, 0)                           AS excess_qty_uom2,
# MAGIC         ROUND(
# MAGIC             (on_hand_uom1 + incoming_released_uom1 + incoming_open_uom1
# MAGIC                 - COALESCE(total_demand_uom1, 0))
# MAGIC             * COALESCE(unit_cost, 0),
# MAGIC             2
# MAGIC         )                                                                     AS excess_value_thb
# MAGIC     FROM per_item
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     -- Identity
# MAGIC     p.item_no,
# MAGIC     p.description,
# MAGIC     p.item_category,
# MAGIC     p.material_type,
# MAGIC     p.metal_category,
# MAGIC     p.base_uom,
# MAGIC     p.uom2_code,
# MAGIC     p.reordering_policy,
# MAGIC     p.archetype,
# MAGIC     p.replenishment_system,
# MAGIC 
# MAGIC     -- Stock breakdown
# MAGIC     ROUND(p.on_hand_uom1, 4)                  AS on_hand_uom1,
# MAGIC     ROUND(p.incoming_released_uom1, 4)        AS incoming_released_uom1,
# MAGIC     ROUND(p.incoming_open_uom1, 4)            AS incoming_open_uom1,
# MAGIC     ROUND(p.total_supply_uom1, 4)             AS total_supply_uom1,
# MAGIC 
# MAGIC     -- Demand
# MAGIC     ROUND(COALESCE(p.total_demand_uom1, 0), 4) AS total_demand_uom1,
# MAGIC     p.bucket_count,
# MAGIC     p.earliest_demand_bucket,
# MAGIC     p.latest_demand_bucket,
# MAGIC 
# MAGIC     -- Over-supply
# MAGIC     ROUND(p.excess_qty_uom1, 4)               AS excess_qty_uom1,
# MAGIC     p.supply_ratio,
# MAGIC 
# MAGIC     -- UOM2
# MAGIC     ROUND(p.on_hand_uom2, 4)                  AS on_hand_uom2,
# MAGIC     ROUND(p.total_supply_uom2, 4)             AS total_supply_uom2,
# MAGIC     ROUND(COALESCE(p.total_firmed_demand_uom2, 0), 4) AS total_firmed_demand_uom2,
# MAGIC     ROUND(p.excess_qty_uom2, 4)               AS excess_qty_uom2,
# MAGIC 
# MAGIC     -- Financial
# MAGIC     ROUND(p.unit_cost, 4)                     AS unit_cost,
# MAGIC     p.unit_cost_source,
# MAGIC     p.excess_value_thb,
# MAGIC 
# MAGIC     -- Categorization
# MAGIC     CASE
# MAGIC         WHEN COALESCE(p.unit_cost, 0) <= 0                          THEN 'NO_COST_DATA'
# MAGIC         WHEN p.item_no LIKE 'M-%-SCRAP%' OR p.item_no LIKE 'M-%SCRAP-%'
# MAGIC                                                                      THEN 'SCRAP_BYPRODUCT'
# MAGIC         WHEN COALESCE(p.total_demand_uom1, 0) = 0                    THEN 'NO_ACTIVE_DEMAND'
# MAGIC         WHEN p.supply_ratio <= 1.5                                   THEN 'HEALTHY_BUFFER'
# MAGIC         ELSE 'OVER_ORDER'
# MAGIC     END                                                              AS excess_category,
# MAGIC 
# MAGIC     -- Severity
# MAGIC     CASE
# MAGIC         WHEN p.supply_ratio > 10  THEN 'SEVERE'
# MAGIC         WHEN p.supply_ratio > 5   THEN 'HIGH'
# MAGIC         WHEN p.supply_ratio > 2   THEN 'MODERATE'
# MAGIC         WHEN p.supply_ratio > 1.5 THEN 'MILD'
# MAGIC         WHEN p.supply_ratio > 1   THEN 'FINE'
# MAGIC         WHEN p.supply_ratio IS NULL THEN 'NO_DEMAND'
# MAGIC         ELSE 'UNDER'
# MAGIC     END                                                              AS severity,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN p.supply_ratio > 10  THEN 5
# MAGIC         WHEN p.supply_ratio > 5   THEN 4
# MAGIC         WHEN p.supply_ratio > 2   THEN 3
# MAGIC         WHEN p.supply_ratio > 1.5 THEN 2
# MAGIC         WHEN p.supply_ratio > 1   THEN 1
# MAGIC         ELSE 0
# MAGIC     END                                                              AS severity_score,
# MAGIC 
# MAGIC     -- ============================================================
# MAGIC     -- 🆕 v1.4 SUMMARY DATE COLUMNS (no lists — drill into _detail table)
# MAGIC     -- ============================================================
# MAGIC     md.mrp_suggested_order_date,
# MAGIC     CASE
# MAGIC         WHEN md.mrp_suggested_order_date IS NOT NULL
# MAGIC             THEN DATEDIFF(md.mrp_suggested_order_date, CURRENT_DATE())
# MAGIC         ELSE NULL
# MAGIC     END                                                              AS days_to_mrp_suggested,
# MAGIC 
# MAGIC     -- Released summary (no lists)
# MAGIC     COALESCE(rps.released_po_count, 0)                               AS released_po_count,
# MAGIC     COALESCE(rps.released_line_count, 0)                             AS released_line_count,
# MAGIC     rps.earliest_released_po_date,
# MAGIC     rps.latest_released_po_date,
# MAGIC     rps.earliest_released_expected,
# MAGIC     rps.latest_released_expected,
# MAGIC     rps.earliest_released_promised,
# MAGIC     rps.latest_released_promised,
# MAGIC 
# MAGIC     -- Open summary (no lists)
# MAGIC     COALESCE(ops.open_po_count, 0)                                   AS open_po_count,
# MAGIC     COALESCE(ops.open_line_count, 0)                                 AS open_line_count,
# MAGIC     ops.earliest_open_po_date,
# MAGIC     ops.latest_open_po_date,
# MAGIC     ops.earliest_open_expected,
# MAGIC     ops.latest_open_expected,
# MAGIC     ops.earliest_open_promised,
# MAGIC     ops.latest_open_promised,
# MAGIC 
# MAGIC     -- Stock context
# MAGIC     p.vendor_no,
# MAGIC     p.vendor_name,
# MAGIC     p.best_stock_location,
# MAGIC 
# MAGIC     -- Engine context
# MAGIC     p.has_trigger,
# MAGIC     p.engine_buy_signal_uom1,
# MAGIC     p.engine_alert_flag,
# MAGIC 
# MAGIC     -- Customer context
# MAGIC     p.customer_no,
# MAGIC     p.is_dnd,
# MAGIC     p.is_critical,
# MAGIC 
# MAGIC     -- Metadata
# MAGIC     'v1.4'                                                           AS source_version,
# MAGIC     current_timestamp()                                              AS materialized_at
# MAGIC 
# MAGIC FROM with_calculations p
# MAGIC LEFT JOIN mrp_dates           md  ON md.item_no  = p.item_no
# MAGIC LEFT JOIN released_po_summary rps ON rps.item_no = p.item_no
# MAGIC LEFT JOIN open_po_summary     ops ON ops.item_no = p.item_no
# MAGIC WHERE p.excess_qty_uom1 > 0;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- 📦 TABLE 2: gold_pur_oversupply_alert_detail (DRILL-DOWN) v1.4 🆕
# MAGIC -- ============================================================================
# MAGIC -- 1 row per PO LINE for items in over-supply
# MAGIC -- Used for drill-down view in dashboard (master row expanded)
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.gold_pur_oversupply_alert_detail
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'false',
# MAGIC   'description' = 'PUR over-supply DETAIL drill-down v1.4. 1 row per PO line (item × po_no × line_no). Joins gold_pur_oversupply_alert (master) for filtering to over-supplied items only.'
# MAGIC )
# MAGIC AS
# MAGIC WITH
# MAGIC 
# MAGIC purchase_header_dedup AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `No.`, `Document Type`, `BC Company`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn_ph
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Purchase Header`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND `Document Type` = 'Order'
# MAGIC     )
# MAGIC     WHERE rn_ph = 1
# MAGIC ),
# MAGIC 
# MAGIC purchase_line_dedup AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `Document No.`, `Document Type`, `Line No.`, `BC Company`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn_pl
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Purchase Line`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND `Document Type` = 'Order'
# MAGIC           AND `Outstanding Quantity` > 0
# MAGIC           AND `Expected Receipt Date` > DATE'1900-01-01'
# MAGIC           AND Type = 'Item'
# MAGIC     )
# MAGIC     WHERE rn_pl = 1
# MAGIC ),
# MAGIC 
# MAGIC vendor_dedup AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `No.`, `BC Company`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn_v
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Vendor`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC     )
# MAGIC     WHERE rn_v = 1
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     pl.`No.`                              AS item_no,
# MAGIC     ph.`No.`                              AS po_no,
# MAGIC     pl.`Line No.`                         AS line_no,
# MAGIC     ph.`Status`                           AS po_status,
# MAGIC     ph.`Document Date`                    AS doc_date,
# MAGIC     pl.`Expected Receipt Date`            AS expected_receipt,
# MAGIC     pl.`Promised Receipt Date`            AS promised_receipt,
# MAGIC     pl.`Outstanding Quantity`             AS outstanding_qty,
# MAGIC     pl.`Outstanding Qty. (Base)`          AS outstanding_qty_base,
# MAGIC     pl.`Unit of Measure Code`             AS uom_code,
# MAGIC     ph.`Buy-from Vendor No.`              AS vendor_no,
# MAGIC     v.Name                                AS vendor_name,
# MAGIC     pl.`Direct Unit Cost`                 AS unit_cost,
# MAGIC     ROUND(pl.`Outstanding Qty. (Base)` * pl.`Direct Unit Cost`, 2)  AS line_value_thb,
# MAGIC     -- Useful flags for UI
# MAGIC     DATEDIFF(pl.`Expected Receipt Date`, CURRENT_DATE())   AS days_to_expected,
# MAGIC     DATEDIFF(pl.`Promised Receipt Date`, CURRENT_DATE())   AS days_to_promised,
# MAGIC     CASE
# MAGIC         WHEN pl.`Promised Receipt Date` IS NULL                        THEN 'NO_PROMISE'
# MAGIC         WHEN pl.`Promised Receipt Date` > pl.`Expected Receipt Date`   THEN 'PROMISED_LATER'
# MAGIC         WHEN pl.`Promised Receipt Date` < pl.`Expected Receipt Date`   THEN 'PROMISED_EARLIER'
# MAGIC         ELSE 'MATCHES'
# MAGIC     END                                                                 AS date_relationship,
# MAGIC     -- Metadata
# MAGIC     'v1.4'                                                              AS source_version,
# MAGIC     current_timestamp()                                                 AS materialized_at
# MAGIC FROM purchase_line_dedup pl
# MAGIC INNER JOIN purchase_header_dedup ph
# MAGIC     ON ph.`No.`           = pl.`Document No.`
# MAGIC    AND ph.`Document Type` = pl.`Document Type`
# MAGIC    AND ph.`BC Company`    = pl.`BC Company`
# MAGIC LEFT JOIN vendor_dedup v
# MAGIC     ON v.`No.` = ph.`Buy-from Vendor No.`
# MAGIC -- Filter: only items that are over-supplied (exist in master table)
# MAGIC INNER JOIN (
# MAGIC     SELECT DISTINCT item_no
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_pur_oversupply_alert
# MAGIC )  master ON master.item_no = pl.`No.`
# MAGIC ;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- VIEW · v_pur_oversupply_actionable (UI-friendly filtered) — UNCHANGED
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE VIEW Gold_Inventory_Lakehouse.mrp.v_pur_oversupply_actionable AS
# MAGIC SELECT *
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_pur_oversupply_alert
# MAGIC WHERE excess_category IN ('OVER_ORDER', 'NO_ACTIVE_DEMAND')
# MAGIC   AND severity IN ('SEVERE', 'HIGH', 'MODERATE');
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- VERIFICATION
# MAGIC -- ============================================================================
# MAGIC 
# MAGIC -- VC1: Master row counts + date coverage
# MAGIC SELECT
# MAGIC     'gold_pur_oversupply_alert v1.4 (MASTER)'                       AS table_name,
# MAGIC     COUNT(*)                                                         AS total_rows,
# MAGIC     SUM(excess_value_thb)                                            AS total_excess_thb,
# MAGIC     SUM(CASE WHEN mrp_suggested_order_date IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_mrp_date,
# MAGIC     SUM(CASE WHEN released_po_count > 0   THEN 1 ELSE 0 END)             AS rows_with_released_po,
# MAGIC     SUM(CASE WHEN open_po_count > 0       THEN 1 ELSE 0 END)             AS rows_with_open_po
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_pur_oversupply_alert;
# MAGIC 
# MAGIC 
# MAGIC -- VC2: Detail row count vs master sum check
# MAGIC SELECT
# MAGIC     'gold_pur_oversupply_alert_detail (DRILL-DOWN)'                  AS table_name,
# MAGIC     COUNT(*)                                                          AS total_detail_rows,
# MAGIC     COUNT(DISTINCT item_no)                                           AS distinct_items,
# MAGIC     COUNT(DISTINCT po_no)                                             AS distinct_pos,
# MAGIC     SUM(CASE WHEN po_status = 'Released' THEN 1 ELSE 0 END)           AS released_lines,
# MAGIC     SUM(CASE WHEN po_status = 'Open'     THEN 1 ELSE 0 END)           AS open_lines
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_pur_oversupply_alert_detail;
# MAGIC 
# MAGIC 
# MAGIC -- VC3: Canary DI-RD-000379 — verify multi-line PO handling
# MAGIC -- Master should show: released_po_count=2 (distinct), released_line_count=3
# MAGIC -- Detail should show: 3 rows
# MAGIC SELECT
# MAGIC     'MASTER' AS source,
# MAGIC     item_no,
# MAGIC     CAST(released_po_count AS STRING)        AS po_count,
# MAGIC     CAST(released_line_count AS STRING)      AS line_count,
# MAGIC     CAST(earliest_released_po_date AS STRING) AS earliest_doc,
# MAGIC     CAST(latest_released_promised AS STRING)  AS latest_promised
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_pur_oversupply_alert
# MAGIC WHERE item_no = 'DI-RD-000379'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     'DETAIL' AS source,
# MAGIC     item_no || ' / ' || po_no || ' / L' || CAST(line_no AS STRING) AS item_no,
# MAGIC     po_status,
# MAGIC     CAST(line_no AS STRING),
# MAGIC     CAST(doc_date AS STRING),
# MAGIC     CAST(promised_receipt AS STRING)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_pur_oversupply_alert_detail
# MAGIC WHERE item_no = 'DI-RD-000379'
# MAGIC ORDER BY source DESC, item_no;
# MAGIC 
# MAGIC 
# MAGIC -- VC4: Canary GE-PL-000223 — single Released PO
# MAGIC SELECT
# MAGIC     item_no,
# MAGIC     po_no,
# MAGIC     po_status,
# MAGIC     doc_date,
# MAGIC     expected_receipt,
# MAGIC     promised_receipt,
# MAGIC     outstanding_qty,
# MAGIC     outstanding_qty_base,
# MAGIC     line_value_thb,
# MAGIC     vendor_name,
# MAGIC     date_relationship
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_pur_oversupply_alert_detail
# MAGIC WHERE item_no = 'GE-PL-000223'
# MAGIC ORDER BY po_status, doc_date, line_no;
# MAGIC 
# MAGIC 
# MAGIC -- VC5: Sync check — distinct PO from detail must match master.released_po_count
# MAGIC -- (this was the MISMATCH problem in v1.3 — now resolved by separating master/detail)
# MAGIC SELECT
# MAGIC     m.item_no,
# MAGIC     m.released_po_count                            AS master_distinct_pos,
# MAGIC     COUNT(DISTINCT d.po_no)                        AS detail_distinct_pos,
# MAGIC     m.released_line_count                          AS master_line_count,
# MAGIC     COUNT(*)                                       AS detail_line_count,
# MAGIC     CASE
# MAGIC         WHEN m.released_po_count   = COUNT(DISTINCT d.po_no)
# MAGIC          AND m.released_line_count = COUNT(*)
# MAGIC         THEN '✅ MATCH'
# MAGIC         ELSE '❌ MISMATCH'
# MAGIC     END                                            AS sync_check
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_pur_oversupply_alert m
# MAGIC LEFT JOIN Gold_Inventory_Lakehouse.mrp.gold_pur_oversupply_alert_detail d
# MAGIC     ON d.item_no = m.item_no AND d.po_status = 'Released'
# MAGIC WHERE m.released_po_count > 0
# MAGIC GROUP BY m.item_no, m.released_po_count, m.released_line_count
# MAGIC HAVING sync_check = '❌ MISMATCH'
# MAGIC    OR m.released_line_count > m.released_po_count   -- multi-line cases (DI-RD-000379)
# MAGIC ORDER BY m.released_line_count DESC
# MAGIC LIMIT 20;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # nb_gold_pur_po_timing

# CELL ********************

# MAGIC %%sql
# MAGIC -- ============================================================================
# MAGIC -- nb_gold_pur_po_timing v1.0 · PO timing tracker with MRP context
# MAGIC -- ============================================================================
# MAGIC -- Purpose:
# MAGIC --   1 row per PO line (item × po_no × line_no) with MRP context joined.
# MAGIC --   Answers: "MRP บอกสั่งวันไหน vs PO เปิดวันไหน vs ของจะมาวันไหน"
# MAGIC --
# MAGIC -- Scope:
# MAGIC --   ALL items that have an OPEN PO line (Document Type = Order, Outstanding > 0)
# MAGIC --   MRP context is OPTIONAL (LEFT JOIN — items without trigger still appear)
# MAGIC --
# MAGIC -- Grain:
# MAGIC --   item_no × po_no × line_no (PO line level)
# MAGIC --
# MAGIC -- Sync:
# MAGIC --   ❌ NOT synced to Dataverse — used in Fabric/HTML only
# MAGIC --
# MAGIC -- Cascade:
# MAGIC --   Run AFTER fabric_requisition_line rebuild.
# MAGIC --   Source: BC Purchase Header/Line + fabric_req + BC Vendor + gold_Item_Master
# MAGIC --
# MAGIC -- v1.0 (2026-05-20)
# MAGIC -- ============================================================================
# MAGIC 
# MAGIC 
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInWrite = CORRECTED;
# MAGIC SET spark.sql.parquet.datetimeRebaseModeInRead  = CORRECTED;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- 📦 TABLE: gold_pur_po_timing v1.0
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.mrp.gold_pur_po_timing
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'false',
# MAGIC   'description' = 'PO timing tracker v1.0. 1 row per PO line with MRP context. Scope: all items with open PO lines. NOT synced to Dataverse.'
# MAGIC )
# MAGIC AS
# MAGIC WITH
# MAGIC 
# MAGIC -- ──────────────────────────────────────────────────────────────────
# MAGIC -- LAYER 1: BC Vendor dedup
# MAGIC -- ──────────────────────────────────────────────────────────────────
# MAGIC vendor_dedup AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             `No.`         AS vendor_no,
# MAGIC             Name          AS vendor_name,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `No.`, `BC Company`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Vendor`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC -- ──────────────────────────────────────────────────────────────────
# MAGIC -- LAYER 2: BC Purchase Header dedup
# MAGIC -- ──────────────────────────────────────────────────────────────────
# MAGIC purchase_header_dedup AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `No.`, `Document Type`, `BC Company`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn_ph
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Purchase Header`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND `Document Type` = 'Order'
# MAGIC     )
# MAGIC     WHERE rn_ph = 1
# MAGIC ),
# MAGIC 
# MAGIC -- ──────────────────────────────────────────────────────────────────
# MAGIC -- LAYER 3: BC Purchase Line dedup — only OUTSTANDING lines
# MAGIC -- ──────────────────────────────────────────────────────────────────
# MAGIC purchase_line_dedup AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `Document No.`, `Document Type`, `Line No.`, `BC Company`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn_pl
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Purchase Line`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC           AND `Document Type` = 'Order'
# MAGIC           AND `Outstanding Quantity` > 0
# MAGIC           AND Type = 'Item'
# MAGIC     )
# MAGIC     WHERE rn_pl = 1
# MAGIC ),
# MAGIC 
# MAGIC -- ──────────────────────────────────────────────────────────────────
# MAGIC -- LAYER 4: MRP context per item (from fabric_req)
# MAGIC -- ──────────────────────────────────────────────────────────────────
# MAGIC -- Aggregate MRP signal per item (1 row per item)
# MAGIC -- Takes EARLIEST suggested_order_date_actionable (most urgent)
# MAGIC -- and SUMS total final_uom1 / final_uom2 across all buckets
# MAGIC mrp_context_per_item AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         MIN(CASE WHEN should_trigger THEN suggested_order_date_actionable END)
# MAGIC             AS mrp_suggested_date,
# MAGIC         MIN(CASE WHEN should_trigger THEN bucket_start_date END)
# MAGIC             AS mrp_earliest_demand_bucket,
# MAGIC         SUM(CASE WHEN should_trigger THEN final_uom1 ELSE 0 END)
# MAGIC             AS mrp_total_to_order_uom1,
# MAGIC         SUM(CASE WHEN should_trigger THEN final_uom2 ELSE 0 END)
# MAGIC             AS mrp_total_to_order_uom2,
# MAGIC         BOOL_OR(should_trigger)
# MAGIC             AS has_mrp_trigger,
# MAGIC         -- Take the most urgent po_urgency from triggered buckets
# MAGIC         MAX(CASE 
# MAGIC                 WHEN should_trigger AND po_urgency = 'OVERDUE'  THEN 4
# MAGIC                 WHEN should_trigger AND po_urgency = 'URGENT'   THEN 3
# MAGIC                 WHEN should_trigger AND po_urgency = 'SOON'     THEN 2
# MAGIC                 WHEN should_trigger AND po_urgency = 'OK'       THEN 1
# MAGIC                 ELSE 0
# MAGIC             END)
# MAGIC             AS mrp_urgency_score
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.fabric_requisition_line
# MAGIC     GROUP BY item_no
# MAGIC ),
# MAGIC 
# MAGIC -- ──────────────────────────────────────────────────────────────────
# MAGIC -- LAYER 5: Item master (description, category, archetype)
# MAGIC -- ──────────────────────────────────────────────────────────────────
# MAGIC item_master_lookup AS (
# MAGIC     SELECT
# MAGIC         item_no,
# MAGIC         description,
# MAGIC         item_category,
# MAGIC         archetype,
# MAGIC         base_uom,
# MAGIC         purch_uom        AS uom2,
# MAGIC         reordering_policy,
# MAGIC         critical_flag    AS is_critical,
# MAGIC         is_blocked,
# MAGIC         purchasing_blocked,
# MAGIC         has_dq_issue
# MAGIC     FROM Gold_Inventory_Lakehouse.mrp.gold_Item_Master
# MAGIC ),
# MAGIC 
# MAGIC -- ──────────────────────────────────────────────────────────────────
# MAGIC -- LAYER 6: BC Item UOM2 code (for items where archetype is not in IM)
# MAGIC -- ──────────────────────────────────────────────────────────────────
# MAGIC bc_item_uom2_lookup AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             `No.`                              AS item_no,
# MAGIC             `Unit of Measure - Units_DU_TSL`   AS item_uom2_code,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY `No.`, `BC Company`
# MAGIC                 ORDER BY SystemRowVersion DESC
# MAGIC             ) AS rn
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Item`
# MAGIC         WHERE `BC Company` = 'Ennovie'
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC )
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- FINAL SELECT: 1 row per PO line + MRP context
# MAGIC -- ============================================================================
# MAGIC SELECT
# MAGIC     -- ========== IDENTITY ==========
# MAGIC     pl.`No.`                                       AS item_no,
# MAGIC     ph.`No.`                                       AS po_no,
# MAGIC     pl.`Line No.`                                  AS line_no,
# MAGIC     ph.`Status`                                    AS po_status,
# MAGIC 
# MAGIC     -- ========== ITEM CONTEXT ==========
# MAGIC     im.description,
# MAGIC     im.item_category,
# MAGIC     im.archetype,
# MAGIC     im.reordering_policy,
# MAGIC     im.base_uom,
# MAGIC     im.uom2                                        AS item_uom2,
# MAGIC     bcu.item_uom2_code,
# MAGIC     im.is_critical,
# MAGIC     im.has_dq_issue,
# MAGIC 
# MAGIC     -- ========== VENDOR ==========
# MAGIC     ph.`Buy-from Vendor No.`                       AS vendor_no,
# MAGIC     v.vendor_name,
# MAGIC 
# MAGIC     -- ========== PO TIMING (from BC) ==========
# MAGIC     ph.`Document Date`                             AS po_opened_date,
# MAGIC     pl.`Expected Receipt Date`                     AS expected_receipt,
# MAGIC     -- Filter BC default date (0001-01-03 = blank) → NULL
# MAGIC     CASE
# MAGIC         WHEN pl.`Promised Receipt Date` < DATE'1900-01-01' THEN NULL
# MAGIC         ELSE pl.`Promised Receipt Date`
# MAGIC     END                                            AS promised_receipt,
# MAGIC 
# MAGIC     pl.`Outstanding Quantity`                      AS outstanding_qty_uom2,
# MAGIC     pl.`Outstanding Qty. (Base)`                   AS outstanding_qty_uom1,
# MAGIC     pl.`Unit of Measure Code`                      AS po_uom,
# MAGIC     pl.`Direct Unit Cost`                          AS unit_cost,
# MAGIC     ROUND(pl.`Outstanding Qty. (Base)` * pl.`Direct Unit Cost`, 2)
# MAGIC                                                    AS line_value_thb,
# MAGIC 
# MAGIC     -- ========== PO DIAGNOSTIC ==========
# MAGIC     DATEDIFF(pl.`Expected Receipt Date`, CURRENT_DATE())
# MAGIC                                                    AS days_to_receipt,
# MAGIC     DATEDIFF(pl.`Expected Receipt Date`, ph.`Document Date`)
# MAGIC                                                    AS lead_time_days_actual,
# MAGIC 
# MAGIC     -- ========== MRP CONTEXT (joined from fabric_req) ==========
# MAGIC     mrp.mrp_suggested_date,
# MAGIC     mrp.mrp_earliest_demand_bucket,
# MAGIC     mrp.mrp_total_to_order_uom1,
# MAGIC     mrp.mrp_total_to_order_uom2,
# MAGIC     COALESCE(mrp.has_mrp_trigger, FALSE)           AS has_mrp_trigger,
# MAGIC     CASE mrp.mrp_urgency_score
# MAGIC         WHEN 4 THEN 'OVERDUE'
# MAGIC         WHEN 3 THEN 'URGENT'
# MAGIC         WHEN 2 THEN 'SOON'
# MAGIC         WHEN 1 THEN 'OK'
# MAGIC         ELSE NULL
# MAGIC     END                                            AS mrp_urgency,
# MAGIC 
# MAGIC     -- ========== TIMING COMPARISON ==========
# MAGIC     DATEDIFF(ph.`Document Date`, mrp.mrp_suggested_date)
# MAGIC                                                    AS days_po_after_mrp_suggest,
# MAGIC     -- negative = PO opened BEFORE MRP suggest (proactive)
# MAGIC     -- positive = PO opened AFTER MRP suggest (delayed)
# MAGIC 
# MAGIC     DATEDIFF(pl.`Expected Receipt Date`, mrp.mrp_suggested_date)
# MAGIC                                                    AS days_receipt_vs_mrp_suggest,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN mrp.mrp_suggested_date IS NULL                                THEN 'NO_MRP_TRIGGER'
# MAGIC         WHEN DATEDIFF(ph.`Document Date`, mrp.mrp_suggested_date) > 7      THEN 'PO_LATE'
# MAGIC         WHEN DATEDIFF(ph.`Document Date`, mrp.mrp_suggested_date) < -30    THEN 'PO_VERY_EARLY'
# MAGIC         WHEN DATEDIFF(ph.`Document Date`, mrp.mrp_suggested_date) < 0      THEN 'PO_BEFORE_MRP'
# MAGIC         ELSE 'ON_TIME'
# MAGIC     END                                            AS timing_status,
# MAGIC 
# MAGIC     -- ========== METADATA ==========
# MAGIC     'v1.0'                                         AS source_version,
# MAGIC     current_timestamp()                            AS materialized_at
# MAGIC 
# MAGIC FROM purchase_line_dedup pl
# MAGIC INNER JOIN purchase_header_dedup ph
# MAGIC     ON ph.`No.`           = pl.`Document No.`
# MAGIC    AND ph.`Document Type` = pl.`Document Type`
# MAGIC    AND ph.`BC Company`    = pl.`BC Company`
# MAGIC LEFT JOIN vendor_dedup v
# MAGIC     ON v.vendor_no = ph.`Buy-from Vendor No.`
# MAGIC LEFT JOIN item_master_lookup im
# MAGIC     ON im.item_no = pl.`No.`
# MAGIC LEFT JOIN bc_item_uom2_lookup bcu
# MAGIC     ON bcu.item_no = pl.`No.`
# MAGIC LEFT JOIN mrp_context_per_item mrp
# MAGIC     ON mrp.item_no = pl.`No.`
# MAGIC WHERE pl.`Expected Receipt Date` > DATE'1900-01-01';   -- safety filter
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- VIEW · v_pur_po_timing (alias)
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE VIEW Gold_Inventory_Lakehouse.mrp.v_pur_po_timing AS
# MAGIC SELECT *
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_pur_po_timing;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- VERIFICATION
# MAGIC -- ============================================================================
# MAGIC 
# MAGIC -- VC1: Volume + distribution
# MAGIC SELECT
# MAGIC     '✅ DEPLOYED v1.0' AS status,
# MAGIC     (SELECT COUNT(*) FROM Gold_Inventory_Lakehouse.mrp.gold_pur_po_timing)
# MAGIC         AS total_po_lines,
# MAGIC     (SELECT COUNT(DISTINCT item_no) FROM Gold_Inventory_Lakehouse.mrp.gold_pur_po_timing)
# MAGIC         AS distinct_items,
# MAGIC     (SELECT COUNT(DISTINCT po_no) FROM Gold_Inventory_Lakehouse.mrp.gold_pur_po_timing)
# MAGIC         AS distinct_pos,
# MAGIC     (SELECT COUNT(DISTINCT vendor_no) FROM Gold_Inventory_Lakehouse.mrp.gold_pur_po_timing)
# MAGIC         AS distinct_vendors;
# MAGIC 
# MAGIC 
# MAGIC -- VC2: Status distribution
# MAGIC SELECT
# MAGIC     po_status,
# MAGIC     COUNT(*) AS po_lines,
# MAGIC     SUM(CASE WHEN has_mrp_trigger THEN 1 ELSE 0 END) AS with_mrp_trigger,
# MAGIC     SUM(CASE WHEN NOT has_mrp_trigger THEN 1 ELSE 0 END) AS no_mrp_trigger,
# MAGIC     ROUND(SUM(line_value_thb), 0) AS total_value_thb
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_pur_po_timing
# MAGIC GROUP BY po_status;
# MAGIC 
# MAGIC 
# MAGIC -- VC3: Timing status distribution
# MAGIC SELECT
# MAGIC     timing_status,
# MAGIC     COUNT(*) AS po_lines,
# MAGIC     ROUND(AVG(days_po_after_mrp_suggest), 1) AS avg_days_po_vs_mrp,
# MAGIC     ROUND(SUM(line_value_thb), 0) AS total_value_thb
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_pur_po_timing
# MAGIC GROUP BY timing_status
# MAGIC ORDER BY po_lines DESC;
# MAGIC 
# MAGIC 
# MAGIC -- VC4: Canary FCG-000610-18KWPD
# MAGIC SELECT
# MAGIC     item_no,
# MAGIC     po_no,
# MAGIC     line_no,
# MAGIC     po_status,
# MAGIC     po_opened_date,
# MAGIC     expected_receipt,
# MAGIC     promised_receipt,                           -- ✅ 0001-01-03 → NULL
# MAGIC     outstanding_qty_uom1                AS qty_uom1,
# MAGIC     outstanding_qty_uom2                AS qty_uom2,
# MAGIC     vendor_no,
# MAGIC     vendor_name,
# MAGIC     -- MRP context
# MAGIC     mrp_suggested_date,
# MAGIC     mrp_total_to_order_uom1             AS mrp_to_order_gr,
# MAGIC     mrp_total_to_order_uom2             AS mrp_to_order_cm,
# MAGIC     -- Diagnostic
# MAGIC     days_po_after_mrp_suggest,
# MAGIC     timing_status
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_pur_po_timing
# MAGIC WHERE item_no = 'FCG-000610-18KWPD'
# MAGIC ORDER BY po_opened_date;
# MAGIC -- Expected:
# MAGIC --   PO2604-0136 / Released / opened 2026-05-08 / expected 2026-06-12 / promised 2026-04-30 / 159.6 GR
# MAGIC --     → mrp_suggested_date 2026-05-20 → days_po_after_mrp_suggest -12 → PO_BEFORE_MRP
# MAGIC --   PO2605-0167 / Open / opened 2026-05-19 / expected 2026-07-03 / promised NULL / 95 GR
# MAGIC --     → days_po_after_mrp_suggest -1 → PO_BEFORE_MRP
# MAGIC 
# MAGIC 
# MAGIC -- VC5: Sample of "PO_LATE" cases (most actionable)
# MAGIC SELECT
# MAGIC     item_no,
# MAGIC     po_no,
# MAGIC     po_status,
# MAGIC     mrp_suggested_date,
# MAGIC     po_opened_date,
# MAGIC     days_po_after_mrp_suggest,
# MAGIC     outstanding_qty_uom1,
# MAGIC     line_value_thb,
# MAGIC     vendor_name
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_pur_po_timing
# MAGIC WHERE timing_status = 'PO_LATE'
# MAGIC ORDER BY days_po_after_mrp_suggest DESC
# MAGIC LIMIT 10;
# MAGIC 
# MAGIC 
# MAGIC -- VC6: Cascade timestamp check
# MAGIC SELECT 'fabric_req'         AS layer, MAX(view_built_at)   AS last_built
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.fabric_requisition_line
# MAGIC UNION ALL
# MAGIC SELECT 'gold_pur_po_timing', MAX(materialized_at)
# MAGIC FROM Gold_Inventory_Lakehouse.mrp.gold_pur_po_timing;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Verification (VC1-VC8)
