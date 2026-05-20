# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3a130b81-98ec-4fd4-a404-95edc1f0ef1e",
# META       "default_lakehouse_name": "Silver_Inventory_Lakehouse",
# META       "default_lakehouse_workspace_id": "d74457b3-045c-445d-82c6-9a2e4b9f1436",
# META       "known_lakehouses": [
# META         {
# META           "id": "3a130b81-98ec-4fd4-a404-95edc1f0ef1e"
# META         },
# META         {
# META           "id": "76781d83-17f8-4270-a81d-6759d1ee9a9d"
# META         },
# META         {
# META           "id": "6fa25cdd-36f9-4f2e-9817-c1f4d946d4d9"
# META         },
# META         {
# META           "id": "1d620310-5acc-4534-93f9-f52f082a1887"
# META         }
# META       ]
# META     },
# META     "mirrored_db": {
# META       "known_mirrored_dbs": []
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

# # Stock Material

# MARKDOWN ********************

# ## All Silver

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.inv.gold_stock_material
# MAGIC AS
# MAGIC /* ================================================================
# MAGIC    Gold Stock Material – V3 (Spark SQL)
# MAGIC    Driver  : Item (filtered by Item Category Code)
# MAGIC    Stock   : LEFT JOIN to Item Ledger Entry (open + target locations)
# MAGIC    PO      : LEFT JOIN to Purchase Line (qty to receive)
# MAGIC    ================================================================ */
# MAGIC 
# MAGIC WITH purchase_line_agg AS (
# MAGIC   SELECT
# MAGIC       `No.`                               AS item_no,
# MAGIC       SUM(`Qty. to Receive`)              AS qty_to_receive,
# MAGIC       MAX(CAST(`Requested Receipt Date` AS DATE)) AS requested_receipt_date,
# MAGIC       MAX(CAST(`Promised Receipt Date`  AS DATE)) AS promised_receipt_date,
# MAGIC       MAX(CAST(`Order Date`             AS DATE)) AS order_date
# MAGIC   FROM `Silver_BC_Lakehouse`.`bc`.`Purchase Line`
# MAGIC   WHERE `Qty. to Receive` > 0
# MAGIC   GROUP BY `No.`
# MAGIC ),
# MAGIC 
# MAGIC stock_agg AS (
# MAGIC   SELECT
# MAGIC       `Item No.`                          AS item_no,
# MAGIC       `Location Code`                     AS entry_type_item_location,
# MAGIC       `Lot No.`                           AS lot_no,
# MAGIC       `Unit of Measure Code`              AS item_uom,
# MAGIC       `Consumption UOM2`                  AS item_uom2,
# MAGIC       MAX(CAST(`Posting Date` AS DATE))   AS last_posting_date,
# MAGIC       SUM(`Quantity`)                     AS qty,
# MAGIC       SUM(`Remaining Quantity`)           AS Total_Rem
# MAGIC   FROM `Silver_BC_Lakehouse`.`bc`.`Item Ledger Entry`
# MAGIC   WHERE `Location Code` IN ('REFINING', 'MATERIAL', 'PRE ALLOY')
# MAGIC     AND (
# MAGIC           (`Open` = true)
# MAGIC        OR (UPPER(CAST(`Open` AS STRING)) IN ('YES', 'TRUE', '1'))
# MAGIC     )
# MAGIC   GROUP BY
# MAGIC       `Item No.`,
# MAGIC       `Location Code`,
# MAGIC       `Lot No.`,
# MAGIC       `Unit of Measure Code`,
# MAGIC       `Consumption UOM2`
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     i.`No.`                      AS item_no,
# MAGIC     i.`Description`              AS item_description,
# MAGIC     i.`Item Category Code`       AS item_category_code,
# MAGIC     i.`Base Unit of Measure`     AS base_uom,
# MAGIC     i.`Safety Stock Quantity`    AS item_safety_stock_quantity,
# MAGIC     i.`Metal Category Code`      AS metal_category_code,
# MAGIC 
# MAGIC     CASE
# MAGIC       WHEN I.`Metal Category Code` IN (
# MAGIC         '9KW','9KY',
# MAGIC         '14KR','14KW','14KY',
# MAGIC         '18KR','18KW','18KY'
# MAGIC       ) THEN 'GOLD'
# MAGIC       WHEN I.`Metal Category Code` = 'SILVER 925' THEN 'SILVER'
# MAGIC       WHEN I.`Metal Category Code` = 'BRASS'      THEN 'BRASS'
# MAGIC       WHEN i.`Metal Category Code` = 'PLATINUM' THEN 'PLATINUM'
# MAGIC       WHEN i.`Metal Category Code` = 'COPPER' THEN 'COPPER'
# MAGIC       ELSE i.`Metal Category Code`
# MAGIC     END AS `TYPE`,
# MAGIC 
# MAGIC     -- Stock fields (NULL = no stock)
# MAGIC     s.entry_type_item_location,
# MAGIC     s.lot_no,
# MAGIC     s.item_uom,
# MAGIC     s.item_uom2,
# MAGIC     s.last_posting_date,
# MAGIC     COALESCE(s.qty, 0)           AS qty,
# MAGIC     COALESCE(s.Total_Rem, 0)     AS Total_Rem,
# MAGIC 
# MAGIC     -- Purchase fields (NULL = no PO)
# MAGIC     COALESCE(pl.qty_to_receive, 0) AS qty_to_receive,
# MAGIC     pl.requested_receipt_date,
# MAGIC     pl.promised_receipt_date,
# MAGIC     pl.order_date
# MAGIC 
# MAGIC FROM `Silver_BC_Lakehouse`.`bc`.`Item` i
# MAGIC LEFT JOIN stock_agg s
# MAGIC   ON i.`No.` = s.item_no
# MAGIC LEFT JOIN purchase_line_agg pl
# MAGIC   ON i.`No.` = pl.item_no
# MAGIC WHERE i.`Item Category Code` IN ('PURE METAL', 'MIXED METAL')
# MAGIC ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Stock Item

# CELL ********************

# MAGIC %%sql
# MAGIC -- ============================================================================
# MAGIC -- gold_stock_item v2 (FULL OUTER JOIN)
# MAGIC -- ----------------------------------------------------------------------------
# MAGIC -- CHANGE LOG:
# MAGIC --   v2 (2026-05-14):
# MAGIC --     - FIX: Items ที่ on-hand = 0 แต่มี PO เปิดอยู่ จะปรากฏใน output แล้ว
# MAGIC --       (เดิม pipeline ใช้ ILE เป็น base + LEFT JOIN PO -> PO-only items หาย)
# MAGIC --     - ใช้ FULL OUTER JOIN ระหว่าง stock_agg และ po_agg
# MAGIC --     - PO ไม่ filter ด้วย location whitelist (เอาตามที่ PO ระบุ)
# MAGIC --     - HAVING clause: ผ่านถ้ามี supply > 0 (on-hand OR PO)
# MAGIC --     - NULL handling: rem_uom1/uom2 -> 0, description fallback จาก Item table
# MAGIC --   v1: ใช้ ILE เป็น base, LEFT JOIN PO -> ตกหล่น PO-only items
# MAGIC -- ============================================================================
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.inv.gold_stock_item AS
# MAGIC 
# MAGIC WITH purchase_line AS (
# MAGIC     SELECT
# MAGIC         pl.`No.` AS item_no,
# MAGIC         pl.`Location Code` AS location,
# MAGIC 
# MAGIC         SUM(CAST(pl.`Outstanding Quantity` AS DECIMAL(38,10))) AS qty_to_receive,
# MAGIC 
# MAGIC         -- Hybrid: ถ้า Outstanding Units_DU_TSL > 0 ใช้เลย, ไม่ก็ proportional
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN COALESCE(CAST(pl.`Outstanding Units_DU_TSL` AS DECIMAL(38,10)), 0) > 0
# MAGIC                     THEN CAST(pl.`Outstanding Units_DU_TSL` AS DECIMAL(38,10))
# MAGIC                 WHEN COALESCE(CAST(pl.`Outstanding Quantity` AS DECIMAL(38,10)), 0) > 0
# MAGIC                      AND COALESCE(CAST(pl.`Quantity` AS DECIMAL(38,10)), 0) <> 0
# MAGIC                     THEN COALESCE(CAST(pl.`Order Quantity_DU_TSL` AS DECIMAL(38,10)), 0)
# MAGIC                          * (CAST(pl.`Outstanding Quantity` AS DECIMAL(38,10))
# MAGIC                             / CAST(pl.`Quantity` AS DECIMAL(38,10)))
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS qty_to_receive_uom2,
# MAGIC 
# MAGIC         MAX(pl.`Requested Receipt Date`) AS requested_receipt_date,
# MAGIC         MAX(pl.`Promised Receipt Date`)  AS promised_receipt_date,
# MAGIC         MAX(pl.`Order Date`)             AS order_date
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Purchase Line` pl
# MAGIC     WHERE pl.`Outstanding Quantity` > 0
# MAGIC       AND pl.`Type` = 'Item'
# MAGIC     GROUP BY pl.`No.`, pl.`Location Code`
# MAGIC ),
# MAGIC 
# MAGIC filtered_ledger AS (
# MAGIC     SELECT
# MAGIC         `Item No.`             AS item_no,
# MAGIC         `Description`          AS item_description,
# MAGIC         `Location Code`        AS entry_type_item_location,
# MAGIC         `Unit of Measure Code` AS item_uom,
# MAGIC         `Remaining Quantity`   AS item_lot_remaining_quantity,
# MAGIC         `Units_DU_TSL`         AS item_lot_remaining_uom2,
# MAGIC         `Quantity`             AS entry_type_item_quantity
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry`
# MAGIC     -- Note: ไม่ filter `Open = 1` เพราะ Units_DU_TSL ของ closed consumption
# MAGIC     -- entries เก็บ negative ที่ต้อง sum เพื่อให้ได้ net UOM2 ที่ถูกต้อง
# MAGIC     -- (Remaining Quantity ของ closed entries = 0 อยู่แล้ว -> SUM UOM1 ไม่กระทบ)
# MAGIC     WHERE `Location Code` IN (
# MAGIC             'BAGGING','CASTING','CONSUME','CST_CUT','CST_ROOM','CZ-SYNT',
# MAGIC             'DEBEERS','DIA-LAB','DIA-NAT','EQUIP','FG-NO-PO','FINDINGS',
# MAGIC             'FIN-GOODS','GEMS','KIMAI','MATERIAL','OBSOLETE','OTHERS-MAT',
# MAGIC             'PACKAGING','PEARLS','PLATING','POMELATO','PRE ALLOY','RETURNS',
# MAGIC             'RUB MOLD','SEMI-F','SORTING','STONE-CUT','STR','TOOLS','WAX ROOM'
# MAGIC     )
# MAGIC ),
# MAGIC 
# MAGIC item_dim AS (
# MAGIC     SELECT
# MAGIC         `No.` AS item_no,
# MAGIC         `Description` AS item_description_master,
# MAGIC         `Base Unit of Measure` AS base_uom,
# MAGIC         `Unit of Measure - Units_DU_TSL` AS uom2,
# MAGIC         `Safety Stock Quantity` AS item_safety_stock_quantity,
# MAGIC         `Replenishment System`,
# MAGIC         `Vendor No.` AS vendor_no,
# MAGIC 
# MAGIC         `Reordering Policy`,
# MAGIC         `Lot Accumulation Period`,
# MAGIC         `Rescheduling Period`,
# MAGIC         `Reorder Point`,
# MAGIC         `Reorder Quantity`,
# MAGIC         `Maximum Inventory`,
# MAGIC         `Time Bucket`,
# MAGIC         `Minimum Order Quantity`,
# MAGIC         `Maximum Order Quantity`,
# MAGIC         `Safety Lead Time`,
# MAGIC         `Order Multiple`
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item`
# MAGIC     WHERE
# MAGIC         (
# MAGIC             try_cast(`Blocked` AS INT) = 0
# MAGIC             OR lower(trim(CAST(`Blocked` AS STRING))) IN ('no','false','0','')
# MAGIC             OR `Blocked` IS NULL
# MAGIC         )
# MAGIC ),
# MAGIC 
# MAGIC vendor_dim AS (
# MAGIC     SELECT
# MAGIC         `No.` AS vendor_no,
# MAGIC         `Name` AS vendor_name,
# MAGIC         `Lead Time Calculation` AS vendor_lead_time_calculation
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Vendor`
# MAGIC ),
# MAGIC 
# MAGIC metal_dim AS (
# MAGIC     SELECT
# MAGIC         `Item` AS item_no,
# MAGIC         `TYPE` AS `TYPE`
# MAGIC     FROM Silver_Inventory_Lakehouse.inv.silver_metal
# MAGIC ),
# MAGIC 
# MAGIC -- Pre-aggregate stock side ที่ (item_no, location) level
# MAGIC stock_agg AS (
# MAGIC     SELECT
# MAGIC         f.item_no,
# MAGIC         f.entry_type_item_location AS location,
# MAGIC         MAX(f.item_description) AS item_description,
# MAGIC         MAX(f.item_uom)         AS item_uom,
# MAGIC         SUM(CAST(f.item_lot_remaining_quantity AS DECIMAL(38,10))) AS rem_uom1,
# MAGIC         SUM(CAST(f.item_lot_remaining_uom2     AS DECIMAL(38,10))) AS rem_uom2
# MAGIC     FROM filtered_ledger f
# MAGIC     GROUP BY f.item_no, f.entry_type_item_location
# MAGIC ),
# MAGIC 
# MAGIC -- FULL OUTER JOIN: รวมทั้ง stock-only, PO-only, และ stock+PO rows
# MAGIC -- COALESCE keys เพื่อกัน NULL ฝั่งใดฝั่งหนึ่ง
# MAGIC combined AS (
# MAGIC     SELECT
# MAGIC         COALESCE(s.item_no,  p.item_no)  AS item_no,
# MAGIC         COALESCE(s.location, p.location) AS location,
# MAGIC 
# MAGIC         -- Stock side (NULL -> 0 สำหรับ PO-only items)
# MAGIC         COALESCE(s.rem_uom1, 0) AS rem_uom1,
# MAGIC         COALESCE(s.rem_uom2, 0) AS rem_uom2,
# MAGIC         s.item_description AS ile_description,
# MAGIC         s.item_uom         AS ile_uom,
# MAGIC 
# MAGIC         -- PO side (NULL -> 0 สำหรับ stock-only items)
# MAGIC         COALESCE(p.qty_to_receive,      0) AS qty_to_receive,
# MAGIC         COALESCE(p.qty_to_receive_uom2, 0) AS qty_to_receive_uom2,
# MAGIC         p.requested_receipt_date,
# MAGIC         p.promised_receipt_date,
# MAGIC         p.order_date
# MAGIC     FROM stock_agg s
# MAGIC     FULL OUTER JOIN purchase_line p
# MAGIC         ON s.item_no  = p.item_no
# MAGIC        AND s.location = p.location
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     uuid() AS systemid,
# MAGIC     c.item_no,
# MAGIC     -- Description: ใช้จาก ILE ก่อน, fallback ไป Item master
# MAGIC     COALESCE(MAX(c.ile_description), MAX(i.item_description_master)) AS item_description,
# MAGIC     c.location,
# MAGIC 
# MAGIC     -- UOM1 / UOM2
# MAGIC     SUM(c.rem_uom1) AS rem_uom1,
# MAGIC     -- UOM: ใช้จาก ILE ก่อน, fallback ไป Item.Base UoM
# MAGIC     COALESCE(MAX(c.ile_uom), MAX(i.base_uom)) AS item_uom,
# MAGIC     SUM(c.rem_uom2) AS rem_uom2,
# MAGIC     MAX(i.uom2) AS item_uom2,
# MAGIC 
# MAGIC     MAX(i.item_safety_stock_quantity) AS item_safety_stock_quantity,
# MAGIC     MAX(t.`TYPE`) AS `TYPE`,
# MAGIC 
# MAGIC     MAX(CAST(c.qty_to_receive      AS DECIMAL(38,10))) AS qty_to_receive,
# MAGIC     MAX(CAST(c.qty_to_receive_uom2 AS DECIMAL(38,10))) AS qty_to_receive_uom2,
# MAGIC     MAX(c.requested_receipt_date) AS requested_receipt_date,
# MAGIC     MAX(c.promised_receipt_date)  AS promised_receipt_date,
# MAGIC     MAX(c.order_date)             AS order_date,
# MAGIC 
# MAGIC     MAX(i.`Reordering Policy`)       AS reordering_policy,
# MAGIC     MAX(i.`Lot Accumulation Period`) AS lot_accumulation_period,
# MAGIC     MAX(i.`Rescheduling Period`)     AS rescheduling_period,
# MAGIC     MAX(i.`Reorder Point`)           AS reorder_point,
# MAGIC     MAX(i.`Reorder Quantity`)        AS reorder_quantity,
# MAGIC     MAX(i.`Maximum Inventory`)       AS maximum_inventory,
# MAGIC     MAX(i.`Time Bucket`)             AS time_bucket,
# MAGIC     MAX(i.`Minimum Order Quantity`)  AS minimum_order_quantity,
# MAGIC     MAX(i.`Maximum Order Quantity`)  AS maximum_order_quantity,
# MAGIC     MAX(i.`Safety Lead Time`)        AS safety_lead_time,
# MAGIC     MAX(i.`Replenishment System`)    AS replenishment_system,
# MAGIC     MAX(i.`Order Multiple`)          AS order_multiple,
# MAGIC 
# MAGIC     MAX(i.vendor_no)                    AS vendor_no,
# MAGIC     MAX(v.vendor_name)                  AS vendor_name,
# MAGIC     MAX(v.vendor_lead_time_calculation) AS vendor_leadtime
# MAGIC 
# MAGIC FROM combined c
# MAGIC INNER JOIN item_dim i
# MAGIC     ON c.item_no = i.item_no         -- INNER: ตัด blocked items ออก (เหมือนเดิม)
# MAGIC LEFT JOIN vendor_dim v
# MAGIC     ON i.vendor_no = v.vendor_no
# MAGIC LEFT JOIN metal_dim t
# MAGIC     ON c.item_no = t.item_no
# MAGIC 
# MAGIC GROUP BY
# MAGIC     c.item_no,
# MAGIC     c.location
# MAGIC HAVING
# MAGIC     SUM(c.rem_uom1) <> 0
# MAGIC  OR SUM(c.rem_uom2) > 0.001
# MAGIC  OR MAX(c.qty_to_receive)      > 0
# MAGIC  OR MAX(c.qty_to_receive_uom2) > 0
# MAGIC ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Bom Sub Bom

# MARKDOWN ********************

# ## All Silver

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -------------------------------------------------
# 1. Config
# -------------------------------------------------
gold_table = "Gold_Inventory_Lakehouse.inv.gold_bom_subbom"

# -------------------------------------------------
# 2. Read source tables (DIRECT replacements)
#   - Silver_Inventory_Lakehouse.inv.silver_item      -> Silver_BC_Lakehouse.bc.`Item`
#   - Silver_Inventory_Lakehouse.inv.silver_bom_line  -> Silver_BC_Lakehouse.bc.`Production BOM Line`
# -------------------------------------------------

# NOTE:
# Your downstream output expects these item fields to exist:
#   inventory, item_inventory_unit, qty_on_purch_order
# But they are not in the provided Item mapping.
# So we create them as NULLs with types aligned to the existing gold table schema:
#   st1 (inventory)      : decimal(18,2)
#   st2 (item_inventory_unit): string
#   purch_qty (qty_on_purch_order): decimal(18,2)

item_df = (
    spark.table("Silver_BC_Lakehouse.bc.`Item`")
    .select(
        F.col("`No.`").alias("item_no"),
        F.col("`Description`").alias("item_description"),
        F.col("`Base Unit of Measure`").alias("item_uom"),
        F.col("`Price Unit Conversion`").alias("item_uom_2"),
        F.col("`Purch. Unit of Measure`").alias("item_purch_uom"),
        F.col("`P.Order Unit of Measure_DU_TSL`").alias("item_purch_uom_2"),
        F.lit(None).cast("decimal(18,2)").alias("inventory"),
        F.lit(None).cast("string").alias("item_inventory_unit"),
        F.lit(None).cast("decimal(18,2)").alias("qty_on_purch_order"),
    )
)

# Cast BOM numeric fields to match your existing gold table schema:
# quantity_per / plating_thickness : decimal(18,3)
bom_df = (
    spark.table("Silver_BC_Lakehouse.bc.`Production BOM Line`")
    .select(
        F.col("`Production BOM No.`").alias("production_bom_no"),
        F.col("`Version Code`").alias("version_code"),
        F.col("`No.`").alias("no"),
        F.col("`Quantity per`").cast("decimal(18,3)").alias("quantity_per"),
        F.col("`Plating Thickness`").cast("decimal(18,3)").alias("plating_thickness"),
        F.col("`Unit of Measure Code`").alias("unit_of_measure_code"),
    )
)

# -------------------------------------------------
# 3. Precompute "best version" per production_bom_no
# -------------------------------------------------
w_ver = (
    Window
    .partitionBy("production_bom_no")
    .orderBy(
        F.when(F.col("version_code").like("P%"), 0).otherwise(1),
        F.col("version_code").desc()
    )
)

best_version = (
    bom_df
    .withColumn("rn", F.row_number().over(w_ver))
    .filter(F.col("rn") == 1)
    .select(
        F.col("production_bom_no"),
        F.col("version_code").alias("best_version_code")
    )
)

# Aliases
ic1    = item_df.alias("ic1")
bom1   = bom_df.alias("bom1")
bom2   = bom_df.alias("bom2")
bom3   = bom_df.alias("bom3")
av1    = best_version.alias("av1")
av2    = best_version.alias("av2")
av3    = best_version.alias("av3")
icBOM1 = item_df.alias("icBOM1")
icBOM2 = item_df.alias("icBOM2")
icBOM3 = item_df.alias("icBOM3")

# -------------------------------------------------
# 4. Build hierarchy FG -> BOM1 -> BOM2 -> BOM3
# -------------------------------------------------

# FG + BOM1 (av1.ver1 logic)
fg_bom1 = (
    ic1
    .join(
        av1,
        F.col("ic1.item_no") == F.col("av1.production_bom_no"),
        "left"
    )
    .join(
        bom1,
        (F.col("bom1.production_bom_no") == F.col("ic1.item_no")) &
        (
            F.col("av1.best_version_code").isNull() |
            (F.col("bom1.version_code") == F.col("av1.best_version_code"))
        ),
        "left"
    )
    .join(
        icBOM1,
        F.col("icBOM1.item_no") == F.col("bom1.no"),
        "left"
    )
)

# BOM2 (av2.ver2 logic)
fg_bom1_bom2 = (
    fg_bom1
    .join(
        av2,
        F.col("bom1.no") == F.col("av2.production_bom_no"),
        "left"
    )
    .join(
        bom2,
        (F.col("bom2.production_bom_no") == F.col("bom1.no")) &
        (
            F.col("av2.best_version_code").isNull() |
            (F.col("bom2.version_code") == F.col("av2.best_version_code"))
        ),
        "left"
    )
    .join(
        icBOM2,
        F.col("icBOM2.item_no") == F.col("bom2.no"),
        "left"
    )
)

# BOM3 (av3.ver3 logic)
fg_bom1_bom2_bom3 = (
    fg_bom1_bom2
    .join(
        av3,
        F.col("bom2.no") == F.col("av3.production_bom_no"),
        "left"
    )
    .join(
        bom3,
        (F.col("bom3.production_bom_no") == F.col("bom2.no")) &
        (
            F.col("av3.best_version_code").isNull() |
            (F.col("bom3.version_code") == F.col("av3.best_version_code"))
        ),
        "left"
    )
    .join(
        icBOM3,
        F.col("icBOM3.item_no") == F.col("bom3.no"),
        "left"
    )
)

# -------------------------------------------------
# 5. Select columns (EXACT names as requested)
# -------------------------------------------------
result_df = (
    fg_bom1_bom2_bom3
    .select(
        # FG
        F.col("ic1.item_no").alias("FG"),
        F.col("ic1.item_description").alias("FG_Des"),
        F.col("bom1.quantity_per").alias("quantity_per"),
        F.col("bom1.plating_thickness").alias("plating_thickness"),
        F.col("bom1.unit_of_measure_code").alias("unit_of_measure_code"),
        F.col("ic1.inventory").alias("st1"),
        F.col("ic1.item_uom").alias("st1_uom"),
        F.col("ic1.item_inventory_unit").alias("st2"),
        F.col("ic1.item_uom_2").alias("st2_uom"),
        F.col("ic1.qty_on_purch_order").alias("purch_qty"),
        F.col("ic1.item_purch_uom").alias("purch_uom"),
        F.col("ic1.item_purch_uom_2").alias("purch_uom2"),

        # BOM1
        F.col("bom1.no").alias("BOM1"),
        F.col("icBOM1.item_description").alias("BOM1_Des"),
        F.col("bom2.quantity_per").alias("FG_Bom_qty"),
        F.col("bom2.plating_thickness").alias("FG_Plating_Thickness"),
        F.col("bom2.unit_of_measure_code").alias("FG_Bom_uom"),
        F.col("icBOM1.inventory").alias("BOM1_st1"),
        F.col("icBOM1.item_uom").alias("BOM1_st1_uom"),
        F.col("icBOM1.item_inventory_unit").alias("BOM1_st2"),
        F.col("icBOM1.item_uom_2").alias("BOM1_st2_uom"),
        F.col("icBOM1.qty_on_purch_order").alias("BOM1_purch_qty"),
        F.col("icBOM1.item_purch_uom").alias("BOM1_purch_uom"),
        F.col("icBOM1.item_purch_uom_2").alias("BOM1_purch_uom2"),

        # BOM2
        F.col("bom2.no").alias("BOM2"),
        F.col("icBOM2.item_description").alias("BOM2_Des"),
        F.col("bom3.quantity_per").alias("BOM1_Bom_qty"),
        F.col("bom3.plating_thickness").alias("BOM1_Plating_Thickness"),
        F.col("bom3.unit_of_measure_code").alias("BOM1_Bom_uom"),
        F.col("icBOM2.inventory").alias("BOM2_st1"),
        F.col("icBOM2.item_uom").alias("BOM2_st1_uom"),
        F.col("icBOM2.item_inventory_unit").alias("BOM2_st2"),
        F.col("icBOM2.item_uom_2").alias("BOM2_st2_uom"),
        F.col("icBOM2.qty_on_purch_order").alias("BOM2_purch_qty"),
        F.col("icBOM2.item_purch_uom").alias("BOM2_purch_uom"),
        F.col("icBOM2.item_purch_uom_2").alias("BOM2_purch_uom2"),

        # BOM3
        F.col("bom3.no").alias("BOM3"),
        F.col("icBOM3.item_description").alias("BOM3_Des"),
        F.lit(None).cast("double").alias("BOM2_Bom_qty"),
        F.lit(None).cast("double").alias("BOM2_Plating_Thickness"),
        F.lit(None).cast("string").alias("BOM2_Bom_uom"),
        F.col("icBOM3.inventory").alias("BOM3_st1"),
        F.col("icBOM3.item_uom").alias("BOM3_st1_uom"),
        F.col("icBOM3.item_inventory_unit").alias("BOM3_st2"),
        F.col("icBOM3.item_uom_2").alias("BOM3_st2_uom"),
        F.col("icBOM3.qty_on_purch_order").alias("BOM3_purch_qty"),
        F.col("icBOM3.item_purch_uom").alias("BOM3_purch_uom"),
        F.col("icBOM3.item_purch_uom_2").alias("BOM3_purch_uom2"),

        # MapKey
        F.concat(
            F.col("ic1.item_no"),
            F.coalesce(F.col("bom1.no"), F.lit("")),
            F.coalesce(F.col("bom2.no"), F.lit("")),
            F.coalesce(F.col("bom3.no"), F.lit(""))
        ).alias("MapKey")
    )
    .distinct()
)

# -------------------------------------------------
# 6. Write gold table (full refresh)
# -------------------------------------------------
(
    result_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_table)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Bom Sub Bom 2

# MARKDOWN ********************

# ## Gold Bom Sub Bom

# CELL ********************

from pyspark.sql import functions as F

# ------------------------------------------------------
# 1. Source gold table created previously
# ------------------------------------------------------
bom_subbom = spark.table("Gold_Inventory_Lakehouse.inv.gold_bom_subbom")

# Alias for self-join
a = bom_subbom.alias("a")
b = bom_subbom.alias("b")

# ------------------------------------------------------
# 2. Apply join + filters to match vBom_SubBom2 logic
# ------------------------------------------------------
result_df = (
    a.join(
        b,
        (F.col("a.FG") == F.col("b.FG")) &
        (F.col("a.BOM1") == F.col("b.BOM1")),
        "inner"
    )
    .filter(
        (F.col("a.BOM1").isNotNull()) &
        (F.col("b.BOM2").isNotNull())
    )
    .select(
        F.col("a.FG"),
        F.col("a.BOM1"),
        F.col("a.quantity_per").alias("BOM1_Qty"),
        F.col("b.BOM2"),
        F.col("b.FG_Bom_qty").alias("BOM2_Qty"),
        F.col("b.FG_Bom_uom").alias("UOM"),
        F.col("b.BOM2_st1"),
        F.col("b.BOM2_purch_qty"),
        F.col("b.BOM2_purch_uom"),
        # multiplication from both levels
        (F.col("a.quantity_per") * F.col("b.FG_Bom_qty")).alias("total_Qty")
    )
    .distinct()
)

# ------------------------------------------------------
# 3. Save as Gold table (full refresh)
# ------------------------------------------------------
(
    result_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("Gold_Inventory_Lakehouse.inv.gold_bom_subbom2")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Bom Sub Bom 2 Summary

# MARKDOWN ********************

# ## Gold Bom Sub Bom 2

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.inv.gold_bom_subbom2_summary
# MAGIC AS
# MAGIC /* ================================================================
# MAGIC    Gold BOM Sub-BOM Metal Summary (Spark SQL Table)
# MAGIC    Purpose : Extract all metal BOM components across every level
# MAGIC              (BOM1 → BOM2 → BOM3) with correct cumulative qty,
# MAGIC              filtered to metals only.
# MAGIC    ================================================================ */
# MAGIC 
# MAGIC WITH bom_all_levels AS (
# MAGIC 
# MAGIC   -- Level 1: BOM1 is a leaf (no BOM2) => total = quantity_per
# MAGIC   SELECT
# MAGIC       b.FG,
# MAGIC       b.BOM1                               AS metal_item,
# MAGIC       b.quantity_per                       AS total_qty,
# MAGIC       'BOM1'                               AS bom_level
# MAGIC   FROM `Gold_Inventory_Lakehouse`.`inv`.`gold_bom_subbom` b
# MAGIC   WHERE b.BOM2 IS NULL
# MAGIC     AND b.BOM1 IS NOT NULL
# MAGIC 
# MAGIC   UNION ALL
# MAGIC 
# MAGIC   -- Level 2: BOM2 is a leaf (no BOM3) => total = quantity_per * FG_Bom_qty
# MAGIC   SELECT
# MAGIC       b.FG,
# MAGIC       b.BOM2                               AS metal_item,
# MAGIC       b.quantity_per * b.FG_Bom_qty        AS total_qty,
# MAGIC       'BOM2'                               AS bom_level
# MAGIC   FROM `Gold_Inventory_Lakehouse`.`inv`.`gold_bom_subbom` b
# MAGIC   WHERE b.BOM3 IS NULL
# MAGIC     AND b.BOM2 IS NOT NULL
# MAGIC 
# MAGIC   UNION ALL
# MAGIC 
# MAGIC   -- Level 3: BOM3 deepest => total = quantity_per * FG_Bom_qty * BOM1_Bom_qty
# MAGIC   SELECT
# MAGIC       b.FG,
# MAGIC       b.BOM3                               AS metal_item,
# MAGIC       b.quantity_per * b.FG_Bom_qty * b.BOM1_Bom_qty AS total_qty,
# MAGIC       'BOM3'                               AS bom_level
# MAGIC   FROM `Gold_Inventory_Lakehouse`.`inv`.`gold_bom_subbom` b
# MAGIC   WHERE b.BOM3 IS NOT NULL
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     a.FG,
# MAGIC     a.metal_item as BOM,
# MAGIC 
# MAGIC     CASE
# MAGIC       WHEN i.`Metal Category Code` IN (
# MAGIC         '9KW','9KY',
# MAGIC         '14KR','14KW','14KY',
# MAGIC         '18KR','18KW','18KY'
# MAGIC       ) THEN 'GOLD'
# MAGIC       WHEN i.`Metal Category Code` = 'SILVER 925' THEN 'SILVER'
# MAGIC       WHEN i.`Metal Category Code` = 'BRASS'      THEN 'BRASS'
# MAGIC       ELSE 'OTHER'
# MAGIC     END AS `TYPE`,
# MAGIC 
# MAGIC     SUM(a.total_qty)                       AS total_Qty,
# MAGIC     a.bom_level
# MAGIC 
# MAGIC FROM bom_all_levels a
# MAGIC INNER JOIN `Silver_BC_Lakehouse`.`bc`.`Item` i
# MAGIC   ON a.metal_item = i.`No.`
# MAGIC WHERE i.`Item Category Code` IN ('ALLOY', 'PURE METAL', 'MIXED METAL')
# MAGIC GROUP BY
# MAGIC     a.FG,
# MAGIC     a.metal_item,
# MAGIC     i.`Metal Category Code`,
# MAGIC     a.bom_level
# MAGIC ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Item Ledger Entry Casting

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SRC_TABLE = "Silver_BC_Lakehouse.bc.`Item Ledger Entry`"
TGT_TABLE = "Gold_Inventory_Lakehouse.inv.gold_item_ledger_entry_casting"
ITEM_PREFIX = "C0"

# ---------------- Full replace: drop & recreate ----------------
spark.sql(f"DROP TABLE IF EXISTS {TGT_TABLE}")

# ---------------- Load source ----------------
ile = spark.table(SRC_TABLE)

# ---------------- Component line column detection (best-effort) ----------------
possible_comp_cols = [
    "Prod. Order Comp. Line No.",
    "Prod. Order Component Line No.",
    "Prod Order Comp Line No",
    "Prod Order Comp. Line No.",
    "Prod. Order Comp Line No",
]
ile_cols_set = set(ile.columns)
COMP_COL_NAME = next((c for c in possible_comp_cols if c in ile_cols_set), None)

comp_col_expr = (
    F.col(f"`{COMP_COL_NAME}`").cast("int")
    if COMP_COL_NAME is not None
    else F.lit(None).cast("int")
)

# ---------------- ILE (match SQL CTE shape/logic) ----------------
ILE = (
    ile.select(
        F.col("`BC Company`").alias("BCCompany"),
        F.col("`Entry No.`").alias("EntryNo"),
        F.col("`Posting Date`").alias("PostingDate"),
        F.col("`Document No.`").alias("DocumentNo"),
        F.col("`Order Line No.`").alias("OrderLineNo"),
        F.col("`Source No.`").alias("SourceNo"),
        F.col("`Item No.`").alias("ItemNo"),
        F.when(F.length(F.trim(F.col("`Lot No.`"))) == 0, F.lit(None))
         .otherwise(F.trim(F.col("`Lot No.`"))).alias("LotNo"),
        F.col("`Unit of Measure Code`").alias("UOM"),
        F.col("`Quantity`").alias("Qty"),
        F.col("`Entry Type`").alias("EntryTypeRaw"),
        F.col("`Entry Type`").cast("int").alias("EntryTypeInt"),
        F.col("`Description`").alias("Description"),
        F.col("`Remaining Quantity`").alias("RemainingQty"),
        F.col("`Location Code`").alias("LocationCode"),
        comp_col_expr.alias("ProdOrderCompLineNo"),
    )
)

# ---------------- Entry type conditions (match SQL) ----------------
is_consumption = (
    (F.col("EntryTypeRaw").isin("Consumption", "Consump.")) |
    (F.col("EntryTypeInt") == F.lit(5))
)

is_output = (
    (F.col("EntryTypeRaw") == F.lit("Output")) |
    (F.col("EntryTypeInt") == F.lit(6))
)

# ---------------- 1) C: Consumption (Item + Lot) ----------------
cons_in = (
    ILE.where(
        (F.col("LotNo").isNotNull()) &
        is_consumption &
        (F.col("ItemNo").startswith(ITEM_PREFIX))
    )
)

C = (
    cons_in.groupBy(
        F.col("BCCompany"),
        F.col("DocumentNo").alias("ConsOrderNo"),
        F.col("OrderLineNo").alias("ConsOrderLineNo"),
        F.col("ItemNo").alias("C_ItemNo"),
        F.col("LotNo").alias("C_LotNo"),
    )
    .agg(
        F.sum(F.col("Qty")).alias("C_ConsQty"),
        F.max(F.struct(F.col("PostingDate"), F.col("EntryNo"))).alias("_c_max_key"),
        F.max(F.struct(F.col("PostingDate"), F.col("EntryNo"), F.col("Description"))).alias("_c_desc_key"),
        F.max(F.struct(F.col("PostingDate"), F.col("EntryNo"), F.col("UOM"))).alias("_c_uom_key"),
        F.max(F.struct(F.col("PostingDate"), F.col("EntryNo"), F.col("RemainingQty"))).alias("_c_rem_key"),
        F.max(F.struct(F.col("PostingDate"), F.col("EntryNo"), F.col("LocationCode"))).alias("_c_loc_key"),
        F.max(F.struct(F.col("PostingDate"), F.col("EntryNo"), F.col("ProdOrderCompLineNo"))).alias("_c_comp_key"),
    )
    .select(
        F.col("BCCompany"),
        F.col("ConsOrderNo"),
        F.col("ConsOrderLineNo"),
        F.col("C_ItemNo"),
        F.col("C_LotNo"),
        F.col("C_ConsQty"),
        F.col("_c_max_key").getField("PostingDate").alias("C_PostingDate"),
        F.col("_c_desc_key").getField("Description").alias("C_Description"),
        F.col("_c_uom_key").getField("UOM").alias("C_UOM"),
        F.col("_c_rem_key").getField("RemainingQty").alias("C_RemainingQty"),
        F.col("_c_loc_key").getField("LocationCode").alias("C_LocationCode"),
        F.col("_c_comp_key").getField("ProdOrderCompLineNo").alias("C_ProdOrderCompLineNo"),
    )
)

# ---------------- 2) O_Ranked: latest Output per Consumption row (to keep all 6) ----------------
O_join = (
    C.alias("c")
     .join(
         ILE.alias("o"),
         (F.col("o.BCCompany") == F.col("c.BCCompany")) &
         (F.col("o.ItemNo") == F.col("c.C_ItemNo")) &
         (F.col("o.LotNo") == F.col("c.C_LotNo")) &
         is_output,
         "inner"
     )
     .select(
         F.col("c.BCCompany").alias("BCCompany"),
         F.col("c.ConsOrderNo").alias("ConsOrderNo"),
         F.col("c.ConsOrderLineNo").alias("ConsOrderLineNo"),
         F.col("c.C_ItemNo").alias("C_ItemNo"),
         F.col("c.C_LotNo").alias("C_LotNo"),
         F.col("c.C_ConsQty").alias("C_ConsQty"),
         F.col("c.C_PostingDate").alias("C_PostingDate"),
         F.col("c.C_Description").alias("C_Description"),
         F.col("c.C_UOM").alias("C_UOM"),
         F.col("c.C_RemainingQty").alias("C_RemainingQty"),
         F.col("c.C_LocationCode").alias("C_LocationCode"),
         F.col("c.C_ProdOrderCompLineNo").alias("C_ProdOrderCompLineNo"),

         F.col("o.DocumentNo").alias("OutOrderNo"),
         F.col("o.OrderLineNo").alias("OutOrderLineNo"),
         F.col("o.SourceNo").alias("O_SourceNo"),
         F.col("o.PostingDate").alias("OutPostingDate"),
         F.col("o.EntryNo").alias("OutEntryNo"),
     )
)

# Keep all 6: rank per (BCCompany, ConsOrderNo, ConsOrderLineNo, Item, Lot)
w_out = (
    Window.partitionBy(
        F.col("BCCompany"),
        F.col("ConsOrderNo"),
        F.col("ConsOrderLineNo"),
        F.col("C_ItemNo"),
        F.col("C_LotNo"),
    )
    .orderBy(F.col("OutPostingDate").desc(), F.col("OutEntryNo").desc())
)

O = (
    O_join.withColumn("rn", F.row_number().over(w_out))
          .where(F.col("rn") == 1)
          .drop("rn")
)

# ---------------- 3) M: Material consumption SUM(ABS(m.Qty)) ----------------
M = (
    O.alias("o")
     .join(
         ILE.alias("m"),
         (F.col("m.BCCompany") == F.col("o.BCCompany")) &
         (F.col("m.DocumentNo") == F.col("o.OutOrderNo")) &
         (F.col("m.SourceNo") == F.col("o.O_SourceNo")) &
         is_consumption,
         "left"
     )
     .groupBy(
         F.col("o.ConsOrderNo"),
         F.col("o.ConsOrderLineNo"),
         F.col("o.C_ItemNo"),
         F.col("o.C_LotNo"),
         F.col("o.C_ConsQty"),
         F.col("o.OutOrderNo"),
         F.col("o.OutOrderLineNo"),
         F.col("o.O_SourceNo"),

         F.col("o.C_PostingDate"),
         F.col("o.C_Description"),
         F.col("o.C_UOM"),
         F.col("o.C_RemainingQty"),
         F.col("o.C_LocationCode"),
         F.col("o.C_ProdOrderCompLineNo"),
     )
     .agg(F.sum(F.abs(F.col("m.Qty"))).alias("material_consumption"))
)

# ---------------- 4) Final output (snake_case) ----------------
final_df = (
    M.select(
        F.to_date(F.col("C_PostingDate")).alias("posting_date"),
        F.col("ConsOrderNo").alias("document_no"),
        F.col("ConsOrderNo").alias("prod_order_no"),
        F.col("ConsOrderLineNo").alias("prod_order_line_no"),
        F.col("C_ProdOrderCompLineNo").alias("prod_order_comp_line_no"),

        F.col("C_ItemNo").alias("item_no"),
        F.col("C_Description").alias("description"),
        F.lit("Consumption").alias("entry_type"),

        F.col("O_SourceNo").alias("source_no"),
        F.col("material_consumption").alias("quantity"),

        F.col("C_RemainingQty").alias("remaining_quantity"),
        F.col("C_UOM").alias("unit_of_measure_code"),
        F.col("C_LocationCode").alias("location_code"),
        F.col("C_LotNo").alias("lot_no"),
    )
)

# ---------------- Write Delta table (full replace) ----------------
(
    final_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TGT_TABLE)
)

print(f"Full replace complete: {TGT_TABLE}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Metal Loss

# CELL ********************

from pyspark.sql import functions as F

target_tbl = "Gold_Inventory_Lakehouse.inv.gold_metal_loss"

spark.conf.set("spark.databricks.delta.columnMapping.enabled", "true")
spark.conf.set("spark.sql.caseSensitive", "false")

source_sql = f"""
WITH s_ranked AS (
    SELECT
        s.*,
        CAST(s.antenna_id AS INT) AS antenna_id_int,
        ROW_NUMBER() OVER (
            PARTITION BY s.prod_order_no, s.prod_order_line_no, s.machine_center_no
            ORDER BY s.created_on DESC
        ) AS rn
    FROM Silver_Production_Lakehouse.prod.silver_prod_order_status s
    WHERE s.antenna_id IS NOT NULL
      AND s.antenna_id <> 1
),
base AS (
    SELECT
        w.created_on,
        w.item_no,
        i.`Metal Category Code` AS `Metal Category Code`,
        i.`Inventory Posting Group` AS `Inventory Posting Group`,
        CASE
            WHEN right(i.`Inventory Posting Group`, 1) = 'S' THEN 'Ag'
            WHEN right(i.`Inventory Posting Group`, 1) = 'G' THEN 'Au'
            WHEN right(i.`Inventory Posting Group`, 1) = 'B' THEN 'Brass'
            WHEN right(i.`Inventory Posting Group`, 1) = 'O' THEN 'Other'
            ELSE NULL
        END AS MaterialType,
        w.prod_order_no,
        w.prod_order_line_no,
        w.machine_center_no,
        w.quantity,
        w.weight,
        w.dust_weight,
        w.sprue_weight,
        s.user_id,
        w.location_code AS cell_routing,
        e.antenna_id,
        e.Employee_Code,
        d.`First Name Thai` AS `First Name Thai`,
        c.cell_line,
        c.prod_line,
        p.sales_order_no,
        p.sales_order_line_no,
        p.FG_item_no,
        p.FG_item_group,
        so.CusNo,
        so.CusName,
        so.CusAbbr,
        so.StatusSO
        -- If you have a better SO "last updated" timestamp, select it here as so_last_updated
    FROM s_ranked s
    JOIN Silver_Production_Lakehouse.prod.silver_pro_weight w
        ON s.prod_order_no = w.prod_order_no
       AND s.prod_order_line_no = w.prod_order_line_no
       AND s.machine_center_no = w.machine_center_no
    LEFT JOIN Silver_Production_Lakehouse.prod.silver_cell_list c
        ON s.user_id = c.email_address
    LEFT JOIN Silver_Commons_Lakehouse.cmn.silver_employee_rfid_mapping e
        ON trim(c.sub_department) = trim(e.sub_department_Eng)
       AND CAST(s.antenna_id AS INT) = CAST(e.antenna_id AS INT)
    LEFT JOIN Silver_BC_Lakehouse.bc.Item i
        ON w.item_no = i.`No.`
    LEFT JOIN Gold_Production_Lakehouse.prod.gold_production_order p
        ON w.prod_order_no = p.prod_order_no
       AND w.prod_order_line_no = p.prod_order_line_no
    LEFT JOIN Gold_Production_Lakehouse.prod.gold_sales_order so
        ON p.sales_order_no = so.SalesorderNo
       AND p.sales_order_line_no = so.SalesLineNo
    LEFT JOIN Silver_Commons_Lakehouse.cmn.silver_employee_data d
        ON e.Employee_Code = d.Employee_Code
    WHERE s.rn = 1
      AND d.End_Date IS NULL
),
latest AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY prod_order_no, prod_order_line_no, machine_center_no
            ORDER BY created_on DESC
        ) AS rn_latest
    FROM base
)
SELECT
    created_on,
    item_no,
    `Metal Category Code` AS metal_category_code,
    `Inventory Posting Group` AS inventory_posting_group,
    MaterialType AS material_type,
    prod_order_no,
    prod_order_line_no,
    machine_center_no,
    quantity,
    weight,
    dust_weight,
    sprue_weight,
    user_id,
    cell_routing,
    antenna_id,
    Employee_Code AS employee_code,
    `First Name Thai` AS first_name_thai,
    cell_line,
    prod_line,
    sales_order_no,
    sales_order_line_no,
    FG_item_no AS fg_item_no,
    FG_item_group AS fg_item_group,
    CusNo AS cus_no,
    CusName AS cus_name,
    CusAbbr AS cus_abbr,
    StatusSO AS status_so
FROM latest
WHERE rn_latest = 1
"""

df_src = spark.sql(source_sql)

if not spark.catalog.tableExists(target_tbl):
    spark.sql(f"""
      CREATE TABLE {target_tbl}
      USING DELTA
      TBLPROPERTIES (
        'delta.columnMapping.mode'='name',
        'delta.minReaderVersion'='2',
        'delta.minWriterVersion'='5'
      )
      AS
      SELECT * FROM (SELECT 1 AS dummy) WHERE 1 = 0
    """)

(
    df_src.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_tbl)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Metal Loss Tracing

# CELL ********************

# MAGIC %%sql
# MAGIC -- =====================================================================================
# MAGIC -- gold_metal_lot_tracing v8.2 (Spark SQL / Delta)
# MAGIC -- Patched from v8.1
# MAGIC -- 
# MAGIC -- v8.1 FIX: step7/dlt_step3 ขาด order_line_no filter (level 3 over-tracing)
# MAGIC -- v8.2 FIX: step2 match ลำดับผิด — item_no match ก่อน lot_no
# MAGIC --           ทำให้จับ metal ของ output คนละ batch (คนละ lot) ใน PO เดียวกัน
# MAGIC --           แก้: สลับลำดับเป็น lot → source (ตัด item match ใน PO เดียวกัน)
# MAGIC --                ถ้า lot ไม่ match → trace ไป PO อื่นผ่าน lot (DLT path)
# MAGIC --
# MAGIC -- Source: Silver_BC_Lakehouse.bc.`Item Ledger Entry`
# MAGIC -- Creates: Gold_Inventory_Lakehouse.inv.gold_metal_lot_tracing (Delta TABLE)
# MAGIC -- =====================================================================================
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.inv.gold_metal_lot_tracing
# MAGIC AS
# MAGIC 
# MAGIC WITH ile AS (
# MAGIC     SELECT
# MAGIC         `Document No.`         AS document_no,
# MAGIC         `Order Line No.`       AS order_line_no,
# MAGIC         `Item No.`             AS item_no,
# MAGIC         `Description`          AS description,
# MAGIC         `Entry Type`           AS entry_type,
# MAGIC         `Source No.`           AS source_no,
# MAGIC         `Quantity`             AS quantity,
# MAGIC         `Unit of Measure Code` AS uom,
# MAGIC         `Location Code`        AS location_code,
# MAGIC         `Lot No.`              AS lot_no
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry`
# MAGIC     WHERE (trim(`Location Code`) <> 'REFINING' OR `Location Code` IS NULL)
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- LEVEL 0: PO has Output + Consumption (M/RM) GR in the same line
# MAGIC -- ============================================================================
# MAGIC level0_direct AS (
# MAGIC     SELECT
# MAGIC         o.document_no           AS origin_po,
# MAGIC         o.order_line_no         AS origin_line_no,
# MAGIC         o.item_no               AS consumed_item_no,
# MAGIC         o.lot_no                AS consumed_lot_no,
# MAGIC         ABS(o.quantity)         AS consumed_pcs,
# MAGIC         o.document_no           AS metal_found_in_po,
# MAGIC         m.item_no               AS metal_item_no,
# MAGIC         m.description           AS metal_description,
# MAGIC         m.lot_no                AS metal_lot_no,
# MAGIC         ABS(m.quantity)         AS metal_qty_gr,
# MAGIC         m.uom                   AS metal_uom,
# MAGIC         m.location_code         AS metal_location,
# MAGIC         m.order_line_no         AS metal_line_no,
# MAGIC         m.source_no             AS metal_source_no,
# MAGIC         0                       AS trace_level
# MAGIC     FROM ile o
# MAGIC     INNER JOIN ile m
# MAGIC         ON  m.document_no   = o.document_no
# MAGIC         AND m.entry_type    = 'Consumption'
# MAGIC         AND (m.item_no LIKE 'M-%' OR m.item_no LIKE 'RM-%')
# MAGIC         AND m.uom           = 'GR'
# MAGIC         AND m.order_line_no = o.order_line_no
# MAGIC     WHERE o.entry_type = 'Output'
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- STEP 1: Consumption (semi items — not metal/findings/diamond)
# MAGIC -- ============================================================================
# MAGIC step1_raw AS (
# MAGIC     SELECT
# MAGIC         document_no    AS origin_po,
# MAGIC         order_line_no  AS origin_line_no,
# MAGIC         item_no        AS consumed_item_no,
# MAGIC         source_no      AS consumed_source_no,
# MAGIC         lot_no         AS consumed_lot_no,
# MAGIC         quantity       AS consumed_qty,
# MAGIC         uom            AS consumed_uom,
# MAGIC         location_code  AS consumed_location
# MAGIC     FROM ile
# MAGIC     WHERE entry_type = 'Consumption'
# MAGIC       AND uom <> 'GR'
# MAGIC       AND item_no NOT LIKE 'M-%'
# MAGIC       AND item_no NOT LIKE 'RM-%'
# MAGIC       AND item_no NOT LIKE 'FOG-%'
# MAGIC       AND item_no NOT LIKE 'DI-%'
# MAGIC ),
# MAGIC 
# MAGIC step1_lotnet AS (
# MAGIC     SELECT
# MAGIC         `Document No.`  AS document_no,
# MAGIC         `Item No.`      AS item_no,
# MAGIC         `Lot No.`       AS lot_no,
# MAGIC         SUM(`Quantity`) AS net_qty
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry`
# MAGIC     WHERE `Entry Type` = 'Consumption'
# MAGIC       AND `Unit of Measure Code` <> 'GR'
# MAGIC       AND `Item No.` NOT LIKE 'M-%'
# MAGIC       AND `Item No.` NOT LIKE 'RM-%'
# MAGIC       AND `Item No.` NOT LIKE 'FOG-%'
# MAGIC       AND `Item No.` NOT LIKE 'DI-%'
# MAGIC     GROUP BY `Document No.`, `Item No.`, `Lot No.`
# MAGIC     HAVING SUM(`Quantity`) <> 0
# MAGIC ),
# MAGIC 
# MAGIC step1_consumption AS (
# MAGIC     SELECT s1.*
# MAGIC     FROM step1_raw s1
# MAGIC     INNER JOIN step1_lotnet ln
# MAGIC         ON  ln.document_no = s1.origin_po
# MAGIC         AND ln.item_no     = s1.consumed_item_no
# MAGIC         AND ln.lot_no      = s1.consumed_lot_no
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- STEP 2 (v8.2): Find outputs in same PO
# MAGIC -- ★ v8.2 CHANGE: ลำดับใหม่ A=lot → B=source → (ตัด item match ใน PO เดียวกัน)
# MAGIC --   เหตุผล: item match ใน PO เดียวกันจะจับ output คนละ batch (คนละ lot)
# MAGIC --           ถ้า lot ไม่ match → ส่งไป nooutput_semi → trace PO อื่นผ่าน lot
# MAGIC -- ============================================================================
# MAGIC 
# MAGIC -- ★ A: lot_no match (แม่นที่สุด — lot เดียวกัน = batch เดียวกัน)
# MAGIC step2a_bylot AS (
# MAGIC     SELECT
# MAGIC         s1.origin_po,
# MAGIC         s1.origin_line_no,
# MAGIC         s1.consumed_item_no,
# MAGIC         s1.consumed_source_no,
# MAGIC         s1.consumed_lot_no,
# MAGIC         ABS(s1.consumed_qty) AS consumed_pcs,
# MAGIC         o.document_no        AS output_po,
# MAGIC         o.order_line_no      AS output_line_no,
# MAGIC         o.item_no            AS output_item_no,
# MAGIC         o.source_no          AS output_source_no,
# MAGIC         o.lot_no             AS output_lot_no
# MAGIC     FROM step1_consumption s1
# MAGIC     INNER JOIN ile o
# MAGIC         ON  o.document_no = s1.origin_po
# MAGIC         AND o.entry_type  = 'Output'
# MAGIC         AND o.lot_no      = s1.consumed_lot_no
# MAGIC         AND o.lot_no IS NOT NULL
# MAGIC         AND o.lot_no <> ''
# MAGIC ),
# MAGIC 
# MAGIC step2a_miss AS (
# MAGIC     SELECT s1.*
# MAGIC     FROM step1_consumption s1
# MAGIC     WHERE NOT EXISTS (
# MAGIC         SELECT 1
# MAGIC         FROM step2a_bylot a
# MAGIC         WHERE a.origin_po       = s1.origin_po
# MAGIC           AND a.origin_line_no  = s1.origin_line_no
# MAGIC           AND a.consumed_lot_no = s1.consumed_lot_no
# MAGIC     )
# MAGIC ),
# MAGIC 
# MAGIC -- ★ B: source_no match (fallback — consumed_source_no = output item_no)
# MAGIC step2b_bysource AS (
# MAGIC     SELECT
# MAGIC         s1.origin_po,
# MAGIC         s1.origin_line_no,
# MAGIC         s1.consumed_item_no,
# MAGIC         s1.consumed_source_no,
# MAGIC         s1.consumed_lot_no,
# MAGIC         ABS(s1.consumed_qty) AS consumed_pcs,
# MAGIC         o.document_no        AS output_po,
# MAGIC         o.order_line_no      AS output_line_no,
# MAGIC         o.item_no            AS output_item_no,
# MAGIC         o.source_no          AS output_source_no,
# MAGIC         o.lot_no             AS output_lot_no
# MAGIC     FROM step2a_miss s1
# MAGIC     INNER JOIN ile o
# MAGIC         ON  o.document_no = s1.origin_po
# MAGIC         AND o.entry_type  = 'Output'
# MAGIC         AND o.item_no     = s1.consumed_source_no
# MAGIC ),
# MAGIC 
# MAGIC -- ★ v8.2: ไม่มี step2c_byitem อีกแล้ว
# MAGIC --   เหตุผล: item match ใน PO เดียวกันจับผิด batch
# MAGIC --   ถ้า lot miss + source miss → ส่งไป nooutput_semi → trace PO อื่น
# MAGIC 
# MAGIC step2_findoutput AS (
# MAGIC     SELECT * FROM step2a_bylot
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM step2b_bysource
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- STEP 3 (Level 1): Find M/RM GR at same output_line_no
# MAGIC -- ============================================================================
# MAGIC direct_metal AS (
# MAGIC     SELECT
# MAGIC         s2.origin_po,
# MAGIC         s2.origin_line_no,
# MAGIC         s2.consumed_item_no,
# MAGIC         s2.consumed_lot_no,
# MAGIC         s2.consumed_pcs,
# MAGIC         s2.output_po          AS metal_found_in_po,
# MAGIC         m.item_no             AS metal_item_no,
# MAGIC         m.description         AS metal_description,
# MAGIC         m.lot_no              AS metal_lot_no,
# MAGIC         ABS(m.quantity)       AS metal_qty_gr,
# MAGIC         m.uom                 AS metal_uom,
# MAGIC         m.location_code       AS metal_location,
# MAGIC         m.order_line_no       AS metal_line_no,
# MAGIC         m.source_no           AS metal_source_no,
# MAGIC         1                     AS trace_level
# MAGIC     FROM step2_findoutput s2
# MAGIC     INNER JOIN ile m
# MAGIC         ON  m.document_no   = s2.output_po
# MAGIC         AND m.entry_type    = 'Consumption'
# MAGIC         AND (m.item_no LIKE 'M-%' OR m.item_no LIKE 'RM-%')
# MAGIC         AND m.uom           = 'GR'
# MAGIC         AND m.order_line_no = s2.output_line_no
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- STEP 4/5: If no direct GR, trace by lot to other PO
# MAGIC -- ============================================================================
# MAGIC needtrace AS (
# MAGIC     SELECT DISTINCT
# MAGIC         s2.origin_po,
# MAGIC         s2.origin_line_no,
# MAGIC         s2.consumed_item_no,
# MAGIC         s2.consumed_lot_no,
# MAGIC         s2.consumed_pcs,
# MAGIC         s2.output_po,
# MAGIC         s2.output_line_no
# MAGIC     FROM step2_findoutput s2
# MAGIC     WHERE NOT EXISTS (
# MAGIC         SELECT 1
# MAGIC         FROM direct_metal AS dm
# MAGIC         WHERE dm.origin_po       = s2.origin_po
# MAGIC           AND dm.origin_line_no  = s2.origin_line_no
# MAGIC           AND dm.consumed_lot_no = s2.consumed_lot_no
# MAGIC     )
# MAGIC ),
# MAGIC 
# MAGIC step5_tracetootherpo AS (
# MAGIC     SELECT
# MAGIC         nt.origin_po,
# MAGIC         nt.origin_line_no,
# MAGIC         nt.consumed_item_no,
# MAGIC         nt.consumed_lot_no,
# MAGIC         nt.consumed_pcs,
# MAGIC         nt.consumed_lot_no AS l1_consumed_lot,
# MAGIC         o2.document_no     AS l2_po,
# MAGIC         o2.order_line_no   AS l2_line_no,
# MAGIC         o2.item_no         AS l2_output_item,
# MAGIC         o2.lot_no          AS l2_lot_no
# MAGIC     FROM needtrace AS nt
# MAGIC     INNER JOIN ile AS o2
# MAGIC         ON  o2.lot_no       = nt.consumed_lot_no
# MAGIC         AND o2.entry_type   = 'Output'
# MAGIC         AND o2.document_no <> nt.origin_po
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- STEP 6 (Level 2): Find GR in L2 PO (direct or internal)
# MAGIC -- ============================================================================
# MAGIC level2_metal_direct AS (
# MAGIC     SELECT
# MAGIC         t.origin_po,
# MAGIC         t.origin_line_no,
# MAGIC         t.consumed_item_no,
# MAGIC         t.consumed_lot_no,
# MAGIC         t.consumed_pcs,
# MAGIC         t.l2_po             AS metal_found_in_po,
# MAGIC         m2.item_no          AS metal_item_no,
# MAGIC         m2.description      AS metal_description,
# MAGIC         m2.lot_no           AS metal_lot_no,
# MAGIC         ABS(m2.quantity)    AS metal_qty_gr,
# MAGIC         m2.uom              AS metal_uom,
# MAGIC         m2.location_code    AS metal_location,
# MAGIC         m2.order_line_no    AS metal_line_no,
# MAGIC         m2.source_no        AS metal_source_no,
# MAGIC         2                   AS trace_level
# MAGIC     FROM step5_tracetootherpo AS t
# MAGIC     INNER JOIN ile AS m2
# MAGIC         ON  m2.document_no   = t.l2_po
# MAGIC         AND m2.entry_type    = 'Consumption'
# MAGIC         AND (m2.item_no LIKE 'M-%' OR m2.item_no LIKE 'RM-%')
# MAGIC         AND m2.uom           = 'GR'
# MAGIC         AND m2.order_line_no = t.l2_line_no
# MAGIC ),
# MAGIC 
# MAGIC level2_needinternal AS (
# MAGIC     SELECT DISTINCT
# MAGIC         t.origin_po,
# MAGIC         t.origin_line_no,
# MAGIC         t.consumed_item_no,
# MAGIC         t.consumed_lot_no,
# MAGIC         t.consumed_pcs,
# MAGIC         t.l2_po,
# MAGIC         t.l2_line_no
# MAGIC     FROM step5_tracetootherpo AS t
# MAGIC     WHERE NOT EXISTS (
# MAGIC         SELECT 1
# MAGIC         FROM level2_metal_direct AS ld
# MAGIC         WHERE ld.origin_po       = t.origin_po
# MAGIC           AND ld.origin_line_no  = t.origin_line_no
# MAGIC           AND ld.consumed_lot_no = t.consumed_lot_no
# MAGIC     )
# MAGIC ),
# MAGIC 
# MAGIC level2_internaltrace AS (
# MAGIC     SELECT
# MAGIC         ni.origin_po,
# MAGIC         ni.origin_line_no,
# MAGIC         ni.consumed_item_no,
# MAGIC         ni.consumed_lot_no,
# MAGIC         ni.consumed_pcs,
# MAGIC         ni.l2_po,
# MAGIC         o_int.order_line_no AS internal_output_line
# MAGIC     FROM level2_needinternal AS ni
# MAGIC     INNER JOIN ile AS c_int
# MAGIC         ON  c_int.document_no   = ni.l2_po
# MAGIC         AND c_int.entry_type    = 'Consumption'
# MAGIC         AND c_int.order_line_no = ni.l2_line_no
# MAGIC         AND c_int.uom          <> 'GR'
# MAGIC         AND c_int.item_no NOT LIKE 'M-%'
# MAGIC         AND c_int.item_no NOT LIKE 'RM-%'
# MAGIC         AND c_int.item_no NOT LIKE 'FOG-%'
# MAGIC         AND c_int.item_no NOT LIKE 'DI-%'
# MAGIC     INNER JOIN ile AS o_int
# MAGIC         ON  o_int.document_no = ni.l2_po
# MAGIC         AND o_int.entry_type  = 'Output'
# MAGIC         AND o_int.item_no     = c_int.source_no
# MAGIC ),
# MAGIC 
# MAGIC level2_metal_internal AS (
# MAGIC     SELECT
# MAGIC         lit.origin_po,
# MAGIC         lit.origin_line_no,
# MAGIC         lit.consumed_item_no,
# MAGIC         lit.consumed_lot_no,
# MAGIC         lit.consumed_pcs,
# MAGIC         lit.l2_po           AS metal_found_in_po,
# MAGIC         m2i.item_no         AS metal_item_no,
# MAGIC         m2i.description     AS metal_description,
# MAGIC         m2i.lot_no          AS metal_lot_no,
# MAGIC         ABS(m2i.quantity)   AS metal_qty_gr,
# MAGIC         m2i.uom             AS metal_uom,
# MAGIC         m2i.location_code   AS metal_location,
# MAGIC         m2i.order_line_no   AS metal_line_no,
# MAGIC         m2i.source_no       AS metal_source_no,
# MAGIC         2                   AS trace_level
# MAGIC     FROM level2_internaltrace AS lit
# MAGIC     INNER JOIN ile AS m2i
# MAGIC         ON  m2i.document_no   = lit.l2_po
# MAGIC         AND m2i.entry_type    = 'Consumption'
# MAGIC         AND (m2i.item_no LIKE 'M-%' OR m2i.item_no LIKE 'RM-%')
# MAGIC         AND m2i.uom           = 'GR'
# MAGIC         AND m2i.order_line_no = lit.internal_output_line
# MAGIC ),
# MAGIC 
# MAGIC level2_metal AS (
# MAGIC     SELECT * FROM level2_metal_direct
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM level2_metal_internal
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- STEP 7 (Level 3): Another layer of trace
# MAGIC -- ★ v8.1 FIX: order_line_no filter
# MAGIC -- ============================================================================
# MAGIC needtrace2 AS (
# MAGIC     SELECT DISTINCT
# MAGIC         t.origin_po,
# MAGIC         t.origin_line_no,
# MAGIC         t.consumed_item_no,
# MAGIC         t.consumed_lot_no,
# MAGIC         t.consumed_pcs,
# MAGIC         t.l2_po,
# MAGIC         t.l2_line_no
# MAGIC     FROM step5_tracetootherpo AS t
# MAGIC     WHERE NOT EXISTS (
# MAGIC         SELECT 1
# MAGIC         FROM level2_metal AS lm
# MAGIC         WHERE lm.origin_po       = t.origin_po
# MAGIC           AND lm.origin_line_no  = t.origin_line_no
# MAGIC           AND lm.consumed_lot_no = t.consumed_lot_no
# MAGIC     )
# MAGIC ),
# MAGIC 
# MAGIC step7_consinl2po AS (
# MAGIC     SELECT
# MAGIC         nt2.origin_po,
# MAGIC         nt2.origin_line_no,
# MAGIC         nt2.consumed_item_no,
# MAGIC         nt2.consumed_lot_no,
# MAGIC         nt2.consumed_pcs,
# MAGIC         nt2.l2_po,
# MAGIC         c2.lot_no AS l2_consumed_lot
# MAGIC     FROM needtrace2 AS nt2
# MAGIC     INNER JOIN ile AS c2
# MAGIC         ON  c2.document_no   = nt2.l2_po
# MAGIC         AND c2.order_line_no = nt2.l2_line_no    -- ★ v8.1 FIX
# MAGIC         AND c2.entry_type    = 'Consumption'
# MAGIC         AND c2.uom          <> 'GR'
# MAGIC         AND c2.item_no NOT LIKE 'M-%'
# MAGIC         AND c2.item_no NOT LIKE 'RM-%'
# MAGIC         AND c2.item_no NOT LIKE 'FOG-%'
# MAGIC         AND c2.item_no NOT LIKE 'DI-%'
# MAGIC ),
# MAGIC 
# MAGIC step7_tracetothirdpo AS (
# MAGIC     SELECT
# MAGIC         s7.origin_po,
# MAGIC         s7.origin_line_no,
# MAGIC         s7.consumed_item_no,
# MAGIC         s7.consumed_lot_no,
# MAGIC         s7.consumed_pcs,
# MAGIC         s7.l2_consumed_lot,
# MAGIC         o3.document_no   AS l3_po,
# MAGIC         o3.order_line_no AS l3_line_no,
# MAGIC         o3.item_no       AS l3_output_item,
# MAGIC         o3.lot_no        AS l3_lot_no
# MAGIC     FROM step7_consinl2po AS s7
# MAGIC     INNER JOIN ile AS o3
# MAGIC         ON  o3.lot_no       = s7.l2_consumed_lot
# MAGIC         AND o3.entry_type   = 'Output'
# MAGIC         AND o3.document_no <> s7.l2_po
# MAGIC ),
# MAGIC 
# MAGIC level3_metal AS (
# MAGIC     SELECT
# MAGIC         t3.origin_po,
# MAGIC         t3.origin_line_no,
# MAGIC         t3.consumed_item_no,
# MAGIC         t3.consumed_lot_no,
# MAGIC         t3.consumed_pcs,
# MAGIC         t3.l3_po           AS metal_found_in_po,
# MAGIC         m3.item_no         AS metal_item_no,
# MAGIC         m3.description     AS metal_description,
# MAGIC         m3.lot_no          AS metal_lot_no,
# MAGIC         ABS(m3.quantity)   AS metal_qty_gr,
# MAGIC         m3.uom             AS metal_uom,
# MAGIC         m3.location_code   AS metal_location,
# MAGIC         m3.order_line_no   AS metal_line_no,
# MAGIC         m3.source_no       AS metal_source_no,
# MAGIC         3                  AS trace_level
# MAGIC     FROM step7_tracetothirdpo AS t3
# MAGIC     INNER JOIN ile AS m3
# MAGIC         ON  m3.document_no   = t3.l3_po
# MAGIC         AND m3.entry_type    = 'Consumption'
# MAGIC         AND (m3.item_no LIKE 'M-%' OR m3.item_no LIKE 'RM-%')
# MAGIC         AND m3.uom           = 'GR'
# MAGIC         AND m3.order_line_no = t3.l3_line_no
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Direct Lot Trace: semi ที่ step2 หา output ใน PO เดียวกันไม่เจอ
# MAGIC -- → trace ผ่าน lot ไป PO อื่นโดยตรง
# MAGIC -- ★ v8.2: จะมี rows มากขึ้นเพราะตัด item match ออกจาก step2
# MAGIC -- ============================================================================
# MAGIC nooutput_semi AS (
# MAGIC     SELECT
# MAGIC         s1.origin_po,
# MAGIC         s1.origin_line_no,
# MAGIC         s1.consumed_item_no,
# MAGIC         s1.consumed_source_no,
# MAGIC         s1.consumed_lot_no,
# MAGIC         ABS(s1.consumed_qty) AS consumed_pcs,
# MAGIC         s1.consumed_location
# MAGIC     FROM step1_consumption s1
# MAGIC     WHERE NOT EXISTS (
# MAGIC         SELECT 1
# MAGIC         FROM step2_findoutput s2
# MAGIC         WHERE s2.origin_po       = s1.origin_po
# MAGIC           AND s2.origin_line_no  = s1.origin_line_no
# MAGIC           AND s2.consumed_lot_no = s1.consumed_lot_no
# MAGIC     )
# MAGIC ),
# MAGIC 
# MAGIC dlt_tracetootherpo AS (
# MAGIC     SELECT
# MAGIC         ns.origin_po,
# MAGIC         ns.origin_line_no,
# MAGIC         ns.consumed_item_no,
# MAGIC         ns.consumed_lot_no,
# MAGIC         ns.consumed_pcs,
# MAGIC         o2.document_no   AS l2d_po,
# MAGIC         o2.order_line_no AS l2d_line_no,
# MAGIC         o2.item_no       AS l2d_output_item,
# MAGIC         o2.lot_no        AS l2d_lot_no
# MAGIC     FROM nooutput_semi AS ns
# MAGIC     INNER JOIN ile AS o2
# MAGIC         ON  o2.lot_no       = ns.consumed_lot_no
# MAGIC         AND o2.entry_type   = 'Output'
# MAGIC         AND o2.document_no <> ns.origin_po
# MAGIC ),
# MAGIC 
# MAGIC dlt_level2d_metal_direct AS (
# MAGIC     SELECT
# MAGIC         dt.origin_po,
# MAGIC         dt.origin_line_no,
# MAGIC         dt.consumed_item_no,
# MAGIC         dt.consumed_lot_no,
# MAGIC         dt.consumed_pcs,
# MAGIC         dt.l2d_po         AS metal_found_in_po,
# MAGIC         m.item_no         AS metal_item_no,
# MAGIC         m.description     AS metal_description,
# MAGIC         m.lot_no          AS metal_lot_no,
# MAGIC         ABS(m.quantity)   AS metal_qty_gr,
# MAGIC         m.uom             AS metal_uom,
# MAGIC         m.location_code   AS metal_location,
# MAGIC         m.order_line_no   AS metal_line_no,
# MAGIC         m.source_no       AS metal_source_no,
# MAGIC         2                 AS trace_level
# MAGIC     FROM dlt_tracetootherpo AS dt
# MAGIC     INNER JOIN ile m
# MAGIC         ON  m.document_no   = dt.l2d_po
# MAGIC         AND m.entry_type    = 'Consumption'
# MAGIC         AND (m.item_no LIKE 'M-%' OR m.item_no LIKE 'RM-%')
# MAGIC         AND m.uom           = 'GR'
# MAGIC         AND m.order_line_no = dt.l2d_line_no
# MAGIC ),
# MAGIC 
# MAGIC dlt_needinternal AS (
# MAGIC     SELECT DISTINCT
# MAGIC         dt.origin_po,
# MAGIC         dt.origin_line_no,
# MAGIC         dt.consumed_item_no,
# MAGIC         dt.consumed_lot_no,
# MAGIC         dt.consumed_pcs,
# MAGIC         dt.l2d_po,
# MAGIC         dt.l2d_line_no
# MAGIC     FROM dlt_tracetootherpo AS dt
# MAGIC     WHERE NOT EXISTS (
# MAGIC         SELECT 1
# MAGIC         FROM dlt_level2d_metal_direct AS dd
# MAGIC         WHERE dd.origin_po       = dt.origin_po
# MAGIC           AND dd.origin_line_no  = dt.origin_line_no
# MAGIC           AND dd.consumed_lot_no = dt.consumed_lot_no
# MAGIC     )
# MAGIC ),
# MAGIC 
# MAGIC dlt_internaltrace AS (
# MAGIC     SELECT
# MAGIC         dni.origin_po,
# MAGIC         dni.origin_line_no,
# MAGIC         dni.consumed_item_no,
# MAGIC         dni.consumed_lot_no,
# MAGIC         dni.consumed_pcs,
# MAGIC         dni.l2d_po,
# MAGIC         o_int.order_line_no AS internal_output_line
# MAGIC     FROM dlt_needinternal AS dni
# MAGIC     INNER JOIN ile AS c_int
# MAGIC         ON  c_int.document_no   = dni.l2d_po
# MAGIC         AND c_int.entry_type    = 'Consumption'
# MAGIC         AND c_int.order_line_no = dni.l2d_line_no
# MAGIC         AND c_int.uom          <> 'GR'
# MAGIC         AND c_int.item_no NOT LIKE 'M-%'
# MAGIC         AND c_int.item_no NOT LIKE 'RM-%'
# MAGIC         AND c_int.item_no NOT LIKE 'FOG-%'
# MAGIC         AND c_int.item_no NOT LIKE 'DI-%'
# MAGIC     INNER JOIN ile AS o_int
# MAGIC         ON  o_int.document_no = dni.l2d_po
# MAGIC         AND o_int.entry_type  = 'Output'
# MAGIC         AND o_int.item_no     = c_int.source_no
# MAGIC ),
# MAGIC 
# MAGIC dlt_level2d_metal_internal AS (
# MAGIC     SELECT
# MAGIC         dit.origin_po,
# MAGIC         dit.origin_line_no,
# MAGIC         dit.consumed_item_no,
# MAGIC         dit.consumed_lot_no,
# MAGIC         dit.consumed_pcs,
# MAGIC         dit.l2d_po         AS metal_found_in_po,
# MAGIC         m.item_no          AS metal_item_no,
# MAGIC         m.description      AS metal_description,
# MAGIC         m.lot_no           AS metal_lot_no,
# MAGIC         ABS(m.quantity)    AS metal_qty_gr,
# MAGIC         m.uom              AS metal_uom,
# MAGIC         m.location_code    AS metal_location,
# MAGIC         m.order_line_no    AS metal_line_no,
# MAGIC         m.source_no        AS metal_source_no,
# MAGIC         2                  AS trace_level
# MAGIC     FROM dlt_internaltrace AS dit
# MAGIC     INNER JOIN ile m
# MAGIC         ON  m.document_no   = dit.l2d_po
# MAGIC         AND m.entry_type    = 'Consumption'
# MAGIC         AND (m.item_no LIKE 'M-%' OR m.item_no LIKE 'RM-%')
# MAGIC         AND m.uom           = 'GR'
# MAGIC         AND m.order_line_no = dit.internal_output_line
# MAGIC ),
# MAGIC 
# MAGIC dlt_level2d_metal AS (
# MAGIC     SELECT * FROM dlt_level2d_metal_direct
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM dlt_level2d_metal_internal
# MAGIC ),
# MAGIC 
# MAGIC -- ★ v8.1 FIX: order_line_no filter
# MAGIC dlt_needtrace3 AS (
# MAGIC     SELECT DISTINCT
# MAGIC         dt.origin_po,
# MAGIC         dt.origin_line_no,
# MAGIC         dt.consumed_item_no,
# MAGIC         dt.consumed_lot_no,
# MAGIC         dt.consumed_pcs,
# MAGIC         dt.l2d_po,
# MAGIC         dt.l2d_line_no
# MAGIC     FROM dlt_tracetootherpo AS dt
# MAGIC     WHERE NOT EXISTS (
# MAGIC         SELECT 1
# MAGIC         FROM dlt_level2d_metal AS dm
# MAGIC         WHERE dm.origin_po       = dt.origin_po
# MAGIC           AND dm.origin_line_no  = dt.origin_line_no
# MAGIC           AND dm.consumed_lot_no = dt.consumed_lot_no
# MAGIC     )
# MAGIC ),
# MAGIC 
# MAGIC dlt_step3_consinpo AS (
# MAGIC     SELECT
# MAGIC         nt3.origin_po,
# MAGIC         nt3.origin_line_no,
# MAGIC         nt3.consumed_item_no,
# MAGIC         nt3.consumed_lot_no,
# MAGIC         nt3.consumed_pcs,
# MAGIC         nt3.l2d_po,
# MAGIC         c3.lot_no AS l2d_consumed_lot
# MAGIC     FROM dlt_needtrace3 AS nt3
# MAGIC     INNER JOIN ile AS c3
# MAGIC         ON  c3.document_no   = nt3.l2d_po
# MAGIC         AND c3.order_line_no = nt3.l2d_line_no   -- ★ v8.1 FIX
# MAGIC         AND c3.entry_type    = 'Consumption'
# MAGIC         AND c3.uom          <> 'GR'
# MAGIC         AND c3.item_no NOT LIKE 'M-%'
# MAGIC         AND c3.item_no NOT LIKE 'RM-%'
# MAGIC         AND c3.item_no NOT LIKE 'FOG-%'
# MAGIC         AND c3.item_no NOT LIKE 'DI-%'
# MAGIC ),
# MAGIC 
# MAGIC dlt_step3_tracetothirdpo AS (
# MAGIC     SELECT
# MAGIC         s3.origin_po,
# MAGIC         s3.origin_line_no,
# MAGIC         s3.consumed_item_no,
# MAGIC         s3.consumed_lot_no,
# MAGIC         s3.consumed_pcs,
# MAGIC         s3.l2d_consumed_lot,
# MAGIC         o3.document_no   AS l3d_po,
# MAGIC         o3.order_line_no AS l3d_line_no,
# MAGIC         o3.item_no       AS l3d_output_item,
# MAGIC         o3.lot_no        AS l3d_lot_no
# MAGIC     FROM dlt_step3_consinpo AS s3
# MAGIC     INNER JOIN ile AS o3
# MAGIC         ON  o3.lot_no       = s3.l2d_consumed_lot
# MAGIC         AND o3.entry_type   = 'Output'
# MAGIC         AND o3.document_no <> s3.l2d_po
# MAGIC ),
# MAGIC 
# MAGIC dlt_level3d_metal AS (
# MAGIC     SELECT
# MAGIC         t3.origin_po,
# MAGIC         t3.origin_line_no,
# MAGIC         t3.consumed_item_no,
# MAGIC         t3.consumed_lot_no,
# MAGIC         t3.consumed_pcs,
# MAGIC         t3.l3d_po          AS metal_found_in_po,
# MAGIC         m3.item_no         AS metal_item_no,
# MAGIC         m3.description     AS metal_description,
# MAGIC         m3.lot_no          AS metal_lot_no,
# MAGIC         ABS(m3.quantity)   AS metal_qty_gr,
# MAGIC         m3.uom             AS metal_uom,
# MAGIC         m3.location_code   AS metal_location,
# MAGIC         m3.order_line_no   AS metal_line_no,
# MAGIC         m3.source_no       AS metal_source_no,
# MAGIC         3                  AS trace_level
# MAGIC     FROM dlt_step3_tracetothirdpo AS t3
# MAGIC     INNER JOIN ile AS m3
# MAGIC         ON  m3.document_no   = t3.l3d_po
# MAGIC         AND m3.entry_type    = 'Consumption'
# MAGIC         AND (m3.item_no LIKE 'M-%' OR m3.item_no LIKE 'RM-%')
# MAGIC         AND m3.uom           = 'GR'
# MAGIC         AND m3.order_line_no = t3.l3d_line_no
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Combine all levels
# MAGIC -- ============================================================================
# MAGIC allmetal AS (
# MAGIC     SELECT * FROM level0_direct
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM direct_metal
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM level2_metal
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM level3_metal
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM dlt_level2d_metal
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM dlt_level3d_metal
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- Aggregate
# MAGIC -- ============================================================================
# MAGIC metalagg AS (
# MAGIC     SELECT
# MAGIC         origin_po,
# MAGIC         origin_line_no,
# MAGIC         consumed_item_no,
# MAGIC         consumed_lot_no,
# MAGIC         consumed_pcs,
# MAGIC         metal_found_in_po,
# MAGIC         metal_item_no,
# MAGIC         metal_description,
# MAGIC         metal_source_no,
# MAGIC         SUM(metal_qty_gr) AS total_metal_qty_gr,
# MAGIC         metal_uom,
# MAGIC         metal_location,
# MAGIC         MIN(trace_level)  AS trace_level
# MAGIC     FROM allmetal
# MAGIC     GROUP BY
# MAGIC         origin_po, origin_line_no,
# MAGIC         consumed_item_no, consumed_lot_no, consumed_pcs,
# MAGIC         metal_found_in_po,
# MAGIC         metal_item_no, metal_description, metal_source_no,
# MAGIC         metal_uom, metal_location
# MAGIC ),
# MAGIC 
# MAGIC outputqty AS (
# MAGIC     SELECT
# MAGIC         document_no,
# MAGIC         order_line_no,
# MAGIC         source_no,
# MAGIC         SUM(quantity) AS output_pcs
# MAGIC     FROM ile
# MAGIC     WHERE entry_type = 'Output'
# MAGIC     GROUP BY document_no, order_line_no, source_no
# MAGIC )
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- FINAL OUTPUT
# MAGIC -- ============================================================================
# MAGIC SELECT
# MAGIC     ma.origin_po,
# MAGIC     ma.origin_line_no,
# MAGIC     ma.consumed_item_no,
# MAGIC     ma.consumed_lot_no,
# MAGIC     ma.consumed_pcs,
# MAGIC     ma.metal_found_in_po,
# MAGIC     ma.metal_item_no,
# MAGIC     ma.metal_description,
# MAGIC     ma.total_metal_qty_gr,
# MAGIC     ma.metal_uom,
# MAGIC     ma.metal_location,
# MAGIC     ma.trace_level,
# MAGIC     oq.output_pcs,
# MAGIC     CASE
# MAGIC         WHEN oq.output_pcs IS NOT NULL AND oq.output_pcs <> 0
# MAGIC             THEN ma.total_metal_qty_gr / oq.output_pcs
# MAGIC         ELSE NULL
# MAGIC     END                                         AS metal_per_piece_gr,
# MAGIC     CASE
# MAGIC         WHEN oq.output_pcs IS NOT NULL AND oq.output_pcs <> 0
# MAGIC             THEN ma.consumed_pcs * (ma.total_metal_qty_gr / oq.output_pcs)
# MAGIC         ELSE NULL
# MAGIC     END                                         AS metal_for_this_line_gr
# MAGIC FROM metalagg AS ma
# MAGIC LEFT JOIN outputqty AS oq
# MAGIC     ON  oq.document_no = ma.metal_found_in_po
# MAGIC     AND oq.source_no   = ma.metal_source_no
# MAGIC ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# # Lot Cost

# CELL ********************

# MAGIC %%sql
# MAGIC -- Notebook: gold_lot_cost
# MAGIC -- Purpose: Multi-level FIFO cost trace from PO consumption to raw material purchase
# MAGIC -- Layer: Silver→Gold
# MAGIC -- Schedule: daily
# MAGIC -- Dependencies: Silver_BC_Lakehouse (Item Ledger Entry, Value Entry, Item Application Entry, Purch Rcpt Line, Production Order, Item, Vendor, Dimension Set Entry)
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.inv.gold_lot_cost
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC -- =============================================
# MAGIC -- CTE 1: Consumption entries from Production Orders
# MAGIC -- =============================================
# MAGIC WITH cte_consumption AS (
# MAGIC     SELECT
# MAGIC         ile.`Entry No.`                 AS consumption_entry_no,
# MAGIC         ile.`Item No.`                  AS consumed_item_no,
# MAGIC         ile.`Posting Date`              AS consumption_date,
# MAGIC         ile.`Quantity`                  AS consumed_qty,
# MAGIC         ile.`Document No.`             AS consumption_doc_no,
# MAGIC         ile.`Order No.`                AS production_order_no,
# MAGIC         ile.`Order Line No.`           AS po_line_no,
# MAGIC         ile.`Location Code`            AS consumption_location,
# MAGIC         ile.`Dimension Set ID`         AS consumption_dim_set_id,
# MAGIC         ile.`Lot No.`                  AS consumption_lot_no,
# MAGIC         ile.`Unit of Measure Code`     AS consumed_uom,
# MAGIC         ile.`Item Category Code`       AS item_category_code
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` ile
# MAGIC     WHERE ile.`Entry Type` = 'Consumption'
# MAGIC       AND ile.`Order Type` = 'Production'
# MAGIC ),
# MAGIC 
# MAGIC -- =============================================
# MAGIC -- CTE 2: Consumption cost (PRIMARY COST SOURCE)
# MAGIC -- =============================================
# MAGIC cte_consumption_cost AS (
# MAGIC     SELECT
# MAGIC         ve.`Item Ledger Entry No.`              AS consumption_entry_no,
# MAGIC         SUM(ve.`Cost Amount (Actual)`)          AS cost_actual,
# MAGIC         SUM(ve.`Cost Amount (Expected)`)        AS cost_expected
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Value Entry` ve
# MAGIC     WHERE ve.`Item Ledger Entry Type` = 'Consumption'
# MAGIC     GROUP BY ve.`Item Ledger Entry No.`
# MAGIC ),
# MAGIC 
# MAGIC -- =============================================
# MAGIC -- CTE 3: Direct application (1 level)
# MAGIC -- =============================================
# MAGIC cte_direct_app AS (
# MAGIC     SELECT
# MAGIC         iae.`Outbound Item Entry No.`   AS consumption_entry_no,
# MAGIC         iae.`Inbound Item Entry No.`    AS inbound_entry_no,
# MAGIC         iae.`Quantity`                  AS applied_qty
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Application Entry` iae
# MAGIC     WHERE iae.`Outbound Item Entry No.` <> 0
# MAGIC ),
# MAGIC 
# MAGIC -- =============================================
# MAGIC -- CTE 4: Inbound ILE details (1 level)
# MAGIC -- =============================================
# MAGIC cte_inbound AS (
# MAGIC     SELECT
# MAGIC         ile.`Entry No.`                 AS inbound_entry_no,
# MAGIC         ile.`Entry Type`               AS inbound_entry_type,
# MAGIC         ile.`Item No.`                  AS inbound_item_no,
# MAGIC         ile.`Document No.`             AS inbound_doc_no,
# MAGIC         ile.`Source No.`               AS inbound_source_no,
# MAGIC         ile.`Posting Date`             AS inbound_date,
# MAGIC         ile.`Lot No.`                  AS inbound_lot_no,
# MAGIC         ile.`Order No.`                AS inbound_order_no,
# MAGIC         ile.`Location Code`            AS inbound_location
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` ile
# MAGIC ),
# MAGIC 
# MAGIC -- =============================================
# MAGIC -- CTE 5: Lot-based origin (direct purchase/pos.adj items)
# MAGIC -- =============================================
# MAGIC cte_lot_origin AS (
# MAGIC     SELECT
# MAGIC         ile.`Lot No.`                   AS lot_no,
# MAGIC         ile.`Item No.`                  AS origin_item_no,
# MAGIC         ile.`Entry Type`               AS origin_entry_type,
# MAGIC         ile.`Entry No.`                AS origin_entry_no,
# MAGIC         ile.`Document No.`             AS origin_doc_no,
# MAGIC         ile.`Source No.`               AS origin_source_no,
# MAGIC         ile.`Posting Date`             AS origin_date
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` ile
# MAGIC     WHERE ile.`Entry Type` IN ('Purchase', 'Positive Adjmt.')
# MAGIC       AND ile.`Lot No.` <> ''
# MAGIC       AND ile.`Lot No.` IS NOT NULL
# MAGIC ),
# MAGIC 
# MAGIC -- =============================================
# MAGIC -- CTE 6: Origin cost from Value Entry
# MAGIC -- =============================================
# MAGIC cte_origin_cost AS (
# MAGIC     SELECT
# MAGIC         ve.`Item Ledger Entry No.`              AS origin_entry_no,
# MAGIC         SUM(ve.`Cost Amount (Actual)`)          AS origin_cost_actual,
# MAGIC         SUM(ve.`Item Ledger Entry Quantity`)    AS origin_qty,
# MAGIC         CASE 
# MAGIC             WHEN SUM(ve.`Cost Amount (Actual)`) <> 0 
# MAGIC             THEN SUM(ve.`Cost Amount (Actual)`) 
# MAGIC                  / NULLIF(SUM(ve.`Item Ledger Entry Quantity`), 0)
# MAGIC             ELSE SUM(ve.`Cost Amount (Expected)`) 
# MAGIC                  / NULLIF(SUM(ve.`Item Ledger Entry Quantity`), 0)
# MAGIC         END                                     AS origin_unit_cost
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Value Entry` ve
# MAGIC     WHERE ve.`Item Ledger Entry Type` IN ('Purchase', 'Positive Adjmt.')
# MAGIC     GROUP BY ve.`Item Ledger Entry No.`
# MAGIC ),
# MAGIC 
# MAGIC -- =============================================
# MAGIC -- CTE 7: Metal trace through Casting/Semi-FG Order
# MAGIC -- Covers: PURE METAL, MIXED METAL, ALLOY, SOLDER
# MAGIC -- =============================================
# MAGIC cte_metal_in_casting AS (
# MAGIC     SELECT
# MAGIC         ile_cast.`Order No.`            AS casting_order_no,
# MAGIC         ile_cast.`Item No.`            AS metal_item_no,
# MAGIC         ile_cast.`Lot No.`             AS metal_lot_no,
# MAGIC         ile_cast.`Quantity`            AS metal_consumed_qty,
# MAGIC         i_metal.`Description`          AS metal_description,
# MAGIC         i_metal.`Item Category Code`   AS metal_category
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` ile_cast
# MAGIC     INNER JOIN Silver_BC_Lakehouse.bc.`Item` i_metal
# MAGIC         ON ile_cast.`Item No.` = i_metal.`No.`
# MAGIC     WHERE ile_cast.`Entry Type` = 'Consumption'
# MAGIC       AND ile_cast.`Order Type` = 'Production'
# MAGIC       AND i_metal.`Item Category Code` IN (
# MAGIC           'PURE METAL',
# MAGIC           'MIXED METAL',
# MAGIC           'ALLOY',
# MAGIC           'SOLDER'
# MAGIC       )
# MAGIC ),
# MAGIC 
# MAGIC -- =============================================
# MAGIC -- CTE 8: Metal origin (lot-based)
# MAGIC -- =============================================
# MAGIC cte_metal_origin AS (
# MAGIC     SELECT
# MAGIC         mc.casting_order_no,
# MAGIC         mc.metal_item_no,
# MAGIC         mc.metal_description,
# MAGIC         mc.metal_category,
# MAGIC         mc.metal_lot_no,
# MAGIC         mc.metal_consumed_qty,
# MAGIC         lo.origin_entry_type            AS metal_origin_type,
# MAGIC         lo.origin_doc_no                AS metal_origin_doc_no,
# MAGIC         lo.origin_date                  AS metal_origin_date,
# MAGIC         lo.origin_source_no             AS metal_origin_source_no,
# MAGIC         lo.origin_entry_no              AS metal_origin_entry_no
# MAGIC     FROM cte_metal_in_casting mc
# MAGIC     LEFT JOIN cte_lot_origin lo
# MAGIC         ON mc.metal_lot_no = lo.lot_no
# MAGIC        AND mc.metal_item_no = lo.origin_item_no
# MAGIC ),
# MAGIC 
# MAGIC -- =============================================
# MAGIC -- CTE 9: Rank metal origins per casting order
# MAGIC -- Priority: PURE METAL > MIXED METAL > ALLOY > SOLDER
# MAGIC -- =============================================
# MAGIC cte_metal_origin_ranked AS (
# MAGIC     SELECT
# MAGIC         mo.*,
# MAGIC         oc.origin_unit_cost             AS metal_origin_unit_cost,
# MAGIC         oc.origin_cost_actual           AS metal_origin_total_cost,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY mo.casting_order_no
# MAGIC             ORDER BY 
# MAGIC                 CASE mo.metal_category 
# MAGIC                     WHEN 'PURE METAL'  THEN 1
# MAGIC                     WHEN 'MIXED METAL' THEN 2
# MAGIC                     WHEN 'ALLOY'       THEN 3
# MAGIC                     WHEN 'SOLDER'      THEN 4
# MAGIC                     ELSE 5 
# MAGIC                 END,
# MAGIC                 ABS(mo.metal_consumed_qty) DESC
# MAGIC         ) AS rn
# MAGIC     FROM cte_metal_origin mo
# MAGIC     LEFT JOIN cte_origin_cost oc
# MAGIC         ON mo.metal_origin_entry_no = oc.origin_entry_no
# MAGIC ),
# MAGIC 
# MAGIC -- =============================================
# MAGIC -- CTE 10: Purch Rcpt Line (direct purchases)
# MAGIC -- =============================================
# MAGIC cte_rcpt_line AS (
# MAGIC     SELECT
# MAGIC         prl.`Document No.`             AS purchase_receipt_no,
# MAGIC         prl.`No.`                      AS item_no,
# MAGIC         prl.`Order No.`                AS purchase_order_no,
# MAGIC         prl.`Direct Unit Cost`         AS receipt_direct_unit_cost,
# MAGIC         prl.`Buy-from Vendor No.`      AS vendor_no_from_rcpt,
# MAGIC         prl.`Vendor Item No.`          AS vendor_item_no
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Purch Rcpt Line` prl
# MAGIC     WHERE prl.`Type` = 'Item'
# MAGIC ),
# MAGIC 
# MAGIC -- =============================================
# MAGIC -- CTE 11: Production Order header
# MAGIC -- =============================================
# MAGIC cte_prod_order AS (
# MAGIC     SELECT
# MAGIC         po.`Status`,
# MAGIC         po.`No.`                        AS production_order_no,
# MAGIC         po.`Source No.`                 AS source_item_no,
# MAGIC         po.`Description`               AS po_description,
# MAGIC         po.`Due Date`                   AS po_due_date,
# MAGIC         po.`Quantity`                   AS po_qty,
# MAGIC         po.`Sales Order No.`           AS po_sales_order_no,
# MAGIC         po.`For Item`                  AS po_for_item,
# MAGIC         po.`Shortcut Dimension 1 Code` AS po_global_dim_1,
# MAGIC         po.`Shortcut Dimension 2 Code` AS po_global_dim_2
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Production Order` po
# MAGIC ),
# MAGIC 
# MAGIC -- =============================================
# MAGIC -- CTE 12: Brand Customer dimension
# MAGIC -- =============================================
# MAGIC cte_brand_customer AS (
# MAGIC     SELECT
# MAGIC         dse.`Dimension Set ID`,
# MAGIC         dse.`Dimension Value Code`      AS brand_customer_code
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Dimension Set Entry` dse
# MAGIC     WHERE dse.`Dimension Code` = 'BRAND CUSTOMER'
# MAGIC )
# MAGIC 
# MAGIC -- =============================================
# MAGIC -- FINAL SELECT
# MAGIC -- =============================================
# MAGIC SELECT
# MAGIC     -- === Production Order ===
# MAGIC     c.production_order_no,
# MAGIC     po.source_item_no,
# MAGIC     po.po_description,
# MAGIC     po.`Status`                             AS po_status,
# MAGIC     po.po_due_date,
# MAGIC     po.po_qty                               AS po_order_qty,
# MAGIC     po.po_sales_order_no,
# MAGIC     po.po_for_item,
# MAGIC     po.po_global_dim_1,
# MAGIC     po.po_global_dim_2,
# MAGIC 
# MAGIC     -- === Consumed Item ===
# MAGIC     c.consumed_item_no,
# MAGIC     i.`Description`                         AS consumed_item_description,
# MAGIC     c.item_category_code,
# MAGIC     i.`Inventory Posting Group`            AS inv_posting_group,
# MAGIC     c.consumed_uom,
# MAGIC     c.consumption_date,
# MAGIC     ABS(c.consumed_qty)                    AS consumed_qty,
# MAGIC     c.consumption_doc_no,
# MAGIC     c.consumption_location,
# MAGIC     c.consumption_lot_no,
# MAGIC 
# MAGIC     -- === Consumption Cost (PRIMARY — BC calculated FIFO) ===
# MAGIC     ABS(COALESCE(cc.cost_actual, 0))       AS consumption_cost_actual,
# MAGIC     ABS(COALESCE(cc.cost_expected, 0))     AS consumption_cost_expected,
# MAGIC     CASE
# MAGIC         WHEN COALESCE(cc.cost_actual, 0) <> 0
# MAGIC         THEN ABS(cc.cost_actual) / NULLIF(ABS(c.consumed_qty), 0)
# MAGIC         ELSE ABS(COALESCE(cc.cost_expected, 0)) / NULLIF(ABS(c.consumed_qty), 0)
# MAGIC     END                                     AS consumption_unit_cost,
# MAGIC 
# MAGIC     -- === Direct Source (1 level application) ===
# MAGIC     inb.inbound_entry_type                 AS direct_source_type,
# MAGIC     inb.inbound_item_no                    AS direct_source_item_no,
# MAGIC     inb.inbound_doc_no                     AS direct_source_doc_no,
# MAGIC     inb.inbound_order_no                   AS direct_source_order_no,
# MAGIC     inb.inbound_lot_no                     AS direct_source_lot_no,
# MAGIC 
# MAGIC     -- === Material Source Classification ===
# MAGIC     CASE 
# MAGIC         WHEN inb.inbound_entry_type = 'Purchase'         THEN 'Purchased'
# MAGIC         WHEN inb.inbound_entry_type = 'Positive Adjmt.'   THEN 'Positive Adjustment'
# MAGIC         WHEN inb.inbound_entry_type = 'Output'            THEN 'Manufactured (Semi-FG)'
# MAGIC         WHEN inb.inbound_entry_type = 'Transfer'          THEN 'Transferred'
# MAGIC         ELSE COALESCE(inb.inbound_entry_type, 'No Application')
# MAGIC     END                                     AS material_source_type,
# MAGIC 
# MAGIC     -- === Purchase Receipt (direct purchase only) ===
# MAGIC     CASE WHEN inb.inbound_entry_type = 'Purchase' 
# MAGIC          THEN inb.inbound_doc_no END       AS purchase_receipt_no,
# MAGIC     CASE WHEN inb.inbound_entry_type = 'Purchase' 
# MAGIC          THEN rcpt.purchase_order_no END   AS purchase_order_no,
# MAGIC     CASE WHEN inb.inbound_entry_type = 'Purchase' 
# MAGIC          THEN rcpt.receipt_direct_unit_cost END AS receipt_direct_unit_cost,
# MAGIC     CASE WHEN inb.inbound_entry_type = 'Purchase' 
# MAGIC          THEN rcpt.vendor_item_no END      AS vendor_item_no,
# MAGIC 
# MAGIC     -- === Vendor (direct purchase) ===
# MAGIC     CASE WHEN inb.inbound_entry_type = 'Purchase' 
# MAGIC          THEN COALESCE(inb.inbound_source_no, rcpt.vendor_no_from_rcpt) 
# MAGIC     END                                     AS purchase_vendor_no,
# MAGIC     CASE WHEN inb.inbound_entry_type = 'Purchase' 
# MAGIC          THEN v_purchase.`Name` END        AS purchase_vendor_name,
# MAGIC 
# MAGIC     -- === Lot-Based Origin (direct purchase/pos.adj items) ===
# MAGIC     lot_orig.origin_entry_type             AS lot_origin_type,
# MAGIC     lot_orig.origin_item_no                AS lot_origin_item_no,
# MAGIC     lot_orig.origin_doc_no                 AS lot_origin_doc_no,
# MAGIC     oc_lot.origin_unit_cost                AS lot_origin_unit_cost,
# MAGIC     v_lot_origin.`Name`                    AS lot_origin_vendor_name,
# MAGIC 
# MAGIC     -- === Metal Origin (through Casting/Semi-FG Order) ===
# MAGIC     -- Priority: PURE METAL > MIXED METAL > ALLOY > SOLDER
# MAGIC     mo.metal_item_no                       AS metal_origin_item_no,
# MAGIC     mo.metal_description                   AS metal_origin_description,
# MAGIC     mo.metal_category                      AS metal_origin_category,
# MAGIC     mo.metal_lot_no                        AS metal_origin_lot_no,
# MAGIC     ABS(mo.metal_consumed_qty)             AS metal_origin_consumed_qty,
# MAGIC     mo.metal_origin_type                   AS metal_origin_entry_type,
# MAGIC     mo.metal_origin_doc_no,
# MAGIC     mo.metal_origin_unit_cost,
# MAGIC     mo.metal_origin_total_cost,
# MAGIC     v_metal.`Name`                         AS metal_origin_vendor_name,
# MAGIC 
# MAGIC     -- === Brand Customer Dimension ===
# MAGIC     bc.brand_customer_code
# MAGIC 
# MAGIC FROM cte_consumption c
# MAGIC 
# MAGIC -- Consumption cost (PRIMARY)
# MAGIC LEFT JOIN cte_consumption_cost cc
# MAGIC     ON c.consumption_entry_no = cc.consumption_entry_no
# MAGIC 
# MAGIC -- Direct application (1 level)
# MAGIC LEFT JOIN cte_direct_app da
# MAGIC     ON c.consumption_entry_no = da.consumption_entry_no
# MAGIC 
# MAGIC -- Inbound ILE details
# MAGIC LEFT JOIN cte_inbound inb
# MAGIC     ON da.inbound_entry_no = inb.inbound_entry_no
# MAGIC 
# MAGIC -- Purch Rcpt Line (direct purchase)
# MAGIC LEFT JOIN cte_rcpt_line rcpt
# MAGIC     ON inb.inbound_doc_no   = rcpt.purchase_receipt_no
# MAGIC    AND inb.inbound_item_no = rcpt.item_no
# MAGIC    AND inb.inbound_entry_type = 'Purchase'
# MAGIC 
# MAGIC -- Vendor (direct purchase)
# MAGIC LEFT JOIN Silver_BC_Lakehouse.bc.`Vendor` v_purchase
# MAGIC     ON COALESCE(inb.inbound_source_no, rcpt.vendor_no_from_rcpt) = v_purchase.`No.`
# MAGIC    AND inb.inbound_entry_type = 'Purchase'
# MAGIC 
# MAGIC -- Lot-based origin (direct items)
# MAGIC LEFT JOIN cte_lot_origin lot_orig
# MAGIC     ON c.consumption_lot_no = lot_orig.lot_no
# MAGIC    AND c.consumed_item_no   = lot_orig.origin_item_no
# MAGIC 
# MAGIC -- Lot origin cost
# MAGIC LEFT JOIN cte_origin_cost oc_lot
# MAGIC     ON lot_orig.origin_entry_no = oc_lot.origin_entry_no
# MAGIC 
# MAGIC -- Lot origin vendor
# MAGIC LEFT JOIN Silver_BC_Lakehouse.bc.`Vendor` v_lot_origin
# MAGIC     ON lot_orig.origin_source_no = v_lot_origin.`No.`
# MAGIC 
# MAGIC -- Metal origin (through Casting Order)
# MAGIC LEFT JOIN cte_metal_origin_ranked mo
# MAGIC     ON inb.inbound_order_no = mo.casting_order_no
# MAGIC    AND inb.inbound_entry_type = 'Output'
# MAGIC    AND mo.rn = 1
# MAGIC 
# MAGIC -- Metal vendor
# MAGIC LEFT JOIN Silver_BC_Lakehouse.bc.`Vendor` v_metal
# MAGIC     ON mo.metal_origin_source_no = v_metal.`No.`
# MAGIC 
# MAGIC -- Item master
# MAGIC LEFT JOIN Silver_BC_Lakehouse.bc.`Item` i
# MAGIC     ON c.consumed_item_no = i.`No.`
# MAGIC 
# MAGIC -- Production Order
# MAGIC LEFT JOIN cte_prod_order po
# MAGIC     ON c.production_order_no = po.production_order_no
# MAGIC 
# MAGIC -- Brand Customer dimension
# MAGIC LEFT JOIN cte_brand_customer bc
# MAGIC     ON c.consumption_dim_set_id = bc.`Dimension Set ID`

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Metal Loss Tracing_including Finding

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.inv.gold_metal_lot_tracing_finding
# MAGIC AS
# MAGIC -- =====================================================================================
# MAGIC -- gold_metal_lot_tracing_finding v9.3 (Spark SQL / Delta)
# MAGIC --
# MAGIC -- CHANGELOG:
# MAGIC -- v9.2: net qty, DLT path, routing AS1
# MAGIC -- v9.3: ★ ใช้ `Item Category Code` จาก Item master แทน hardcode prefix
# MAGIC --        ไม่ต้อง maintain prefix list อีกต่อไป
# MAGIC -- =====================================================================================
# MAGIC 
# MAGIC WITH ile AS (
# MAGIC     SELECT
# MAGIC         `Document No.`         AS document_no,
# MAGIC         `Order Line No.`       AS order_line_no,
# MAGIC         `Item No.`             AS item_no,
# MAGIC         `Description`          AS description,
# MAGIC         `Entry Type`           AS entry_type,
# MAGIC         `Source No.`           AS source_no,
# MAGIC         `Quantity`             AS quantity,
# MAGIC         `Unit of Measure Code` AS uom,
# MAGIC         `Location Code`        AS location_code,
# MAGIC         `Lot No.`              AS lot_no
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry`
# MAGIC     WHERE (trim(`Location Code`) <> 'REFINING' OR `Location Code` IS NULL)
# MAGIC ),
# MAGIC 
# MAGIC -- ★ v9.3: Item master → item_category_code
# MAGIC item_master AS (
# MAGIC     SELECT
# MAGIC         `No.`                  AS item_no,
# MAGIC         `Item Category Code`   AS item_category_code,
# MAGIC         `Base Unit of Measure` AS base_uom
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item`
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- PART A: ทุก consumption — aggregate net qty + item_type จาก Item Category
# MAGIC -- ============================================================================
# MAGIC all_consumption AS (
# MAGIC     SELECT
# MAGIC         `Document No.`           AS document_no,
# MAGIC         `Order Line No.`         AS order_line_no,
# MAGIC         e.`Item No.`             AS item_no,
# MAGIC         MAX(`Description`)       AS description,
# MAGIC         `Lot No.`                AS lot_no,
# MAGIC         SUM(`Quantity`)          AS quantity,
# MAGIC         ABS(SUM(`Quantity`))     AS abs_qty,
# MAGIC         MAX(`Unit of Measure Code`) AS uom,
# MAGIC         MAX(`Location Code`)     AS location_code,
# MAGIC         MAX(`Source No.`)        AS source_no,
# MAGIC         MAX(CASE
# MAGIC             WHEN im.item_category_code IN ('PURE METAL', 'MIXED METAL', 'ALLOY', 'SOLDER', 'WIRE')
# MAGIC                                                         THEN 'METAL'
# MAGIC             WHEN im.item_category_code = 'FINDINGS'     THEN 'FINDINGS'
# MAGIC             WHEN im.item_category_code = 'CASTING'      THEN 'CAST'
# MAGIC             WHEN im.item_category_code = 'SEMI-FG'      THEN 'SEMI'
# MAGIC             WHEN im.item_category_code IN ('DIAMOND NAT', 'DIAMONDS LAB')
# MAGIC                                                         THEN 'DIAMOND'
# MAGIC             WHEN im.item_category_code = 'GEMSTONES'    THEN 'GEMS'
# MAGIC             WHEN im.item_category_code IN ('LEATHER', 'BEAD', 'PEARLS', 'SYNT STONE')
# MAGIC                                                         THEN 'ACCESSORY'
# MAGIC             ELSE 'OTHER'
# MAGIC         END) AS item_type
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` e
# MAGIC     LEFT JOIN item_master im ON im.item_no = e.`Item No.`
# MAGIC     WHERE e.`Entry Type` = 'Consumption'
# MAGIC     GROUP BY `Document No.`, `Order Line No.`, e.`Item No.`, `Lot No.`
# MAGIC     HAVING SUM(`Quantity`) < 0
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- PART B: Metal tracing — เฉพาะ CASTING + SEMI-FG
# MAGIC -- ============================================================================
# MAGIC step1_consumption AS (
# MAGIC     SELECT
# MAGIC         e.`Document No.`    AS origin_po,
# MAGIC         e.`Order Line No.`  AS origin_line_no,
# MAGIC         e.`Item No.`        AS consumed_item_no,
# MAGIC         MAX(e.`Source No.`)  AS consumed_source_no,
# MAGIC         e.`Lot No.`         AS consumed_lot_no,
# MAGIC         SUM(e.`Quantity`)   AS consumed_qty,
# MAGIC         MAX(e.`Unit of Measure Code`) AS consumed_uom,
# MAGIC         MAX(e.`Location Code`) AS consumed_location
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` e
# MAGIC     INNER JOIN item_master im ON im.item_no = e.`Item No.`
# MAGIC     WHERE e.`Entry Type` = 'Consumption'
# MAGIC       AND im.item_category_code IN ('CASTING', 'SEMI-FG')
# MAGIC     GROUP BY e.`Document No.`, e.`Order Line No.`, e.`Item No.`, e.`Lot No.`
# MAGIC     HAVING SUM(e.`Quantity`) < 0
# MAGIC ),
# MAGIC 
# MAGIC -- STEP 2: lot-first → source fallback
# MAGIC step2a_bylot AS (
# MAGIC     SELECT
# MAGIC         s1.origin_po, s1.origin_line_no,
# MAGIC         s1.consumed_item_no, s1.consumed_source_no, s1.consumed_lot_no,
# MAGIC         ABS(s1.consumed_qty) AS consumed_pcs,
# MAGIC         o.document_no AS output_po, o.order_line_no AS output_line_no,
# MAGIC         o.item_no AS output_item_no, o.source_no AS output_source_no, o.lot_no AS output_lot_no
# MAGIC     FROM step1_consumption AS s1
# MAGIC     INNER JOIN ile AS o
# MAGIC         ON  o.document_no = s1.origin_po AND o.entry_type = 'Output'
# MAGIC         AND o.lot_no = s1.consumed_lot_no AND o.lot_no IS NOT NULL AND o.lot_no <> ''
# MAGIC ),
# MAGIC 
# MAGIC step2a_miss AS (
# MAGIC     SELECT s1.*
# MAGIC     FROM step1_consumption AS s1
# MAGIC     WHERE NOT EXISTS (
# MAGIC         SELECT 1 FROM step2a_bylot AS a
# MAGIC         WHERE a.origin_po = s1.origin_po AND a.origin_line_no = s1.origin_line_no
# MAGIC           AND a.consumed_lot_no = s1.consumed_lot_no
# MAGIC     )
# MAGIC ),
# MAGIC 
# MAGIC step2b_bysource AS (
# MAGIC     SELECT
# MAGIC         s1.origin_po, s1.origin_line_no,
# MAGIC         s1.consumed_item_no, s1.consumed_source_no, s1.consumed_lot_no,
# MAGIC         ABS(s1.consumed_qty) AS consumed_pcs,
# MAGIC         o.document_no AS output_po, o.order_line_no AS output_line_no,
# MAGIC         o.item_no AS output_item_no, o.source_no AS output_source_no, o.lot_no AS output_lot_no
# MAGIC     FROM step2a_miss AS s1
# MAGIC     INNER JOIN ile AS o
# MAGIC         ON  o.document_no = s1.origin_po AND o.entry_type = 'Output'
# MAGIC         AND o.item_no = s1.consumed_source_no
# MAGIC ),
# MAGIC 
# MAGIC step2_findoutput AS (
# MAGIC     SELECT * FROM step2a_bylot
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM step2b_bysource
# MAGIC ),
# MAGIC 
# MAGIC -- STEP 3 (Level 1): Metal at output_line_no
# MAGIC direct_metal AS (
# MAGIC     SELECT
# MAGIC         s2.origin_po, s2.origin_line_no,
# MAGIC         s2.consumed_item_no, s2.consumed_lot_no, s2.consumed_pcs,
# MAGIC         s2.output_po AS metal_found_in_po,
# MAGIC         m.item_no AS metal_item_no, m.description AS metal_description, m.lot_no AS metal_lot_no,
# MAGIC         ABS(m.quantity) AS metal_qty_gr, m.uom AS metal_uom, m.location_code AS metal_location,
# MAGIC         m.order_line_no AS metal_line_no, m.source_no AS metal_source_no, 1 AS trace_level
# MAGIC     FROM step2_findoutput AS s2
# MAGIC     INNER JOIN ile AS m
# MAGIC         ON  m.document_no = s2.output_po AND m.entry_type = 'Consumption'
# MAGIC         AND m.uom = 'GR' AND m.order_line_no = s2.output_line_no
# MAGIC     INNER JOIN item_master AS im_m ON im_m.item_no = m.item_no
# MAGIC     WHERE im_m.item_category_code IN ('PURE METAL', 'MIXED METAL', 'ALLOY', 'SOLDER', 'WIRE')
# MAGIC ),
# MAGIC 
# MAGIC -- STEP 4/5: cross-PO trace via lot
# MAGIC needtrace AS (
# MAGIC     SELECT DISTINCT
# MAGIC         s2.origin_po, s2.origin_line_no, s2.consumed_item_no, s2.consumed_lot_no,
# MAGIC         s2.consumed_pcs, s2.output_po, s2.output_line_no
# MAGIC     FROM step2_findoutput AS s2
# MAGIC     WHERE NOT EXISTS (
# MAGIC         SELECT 1 FROM direct_metal AS dm
# MAGIC         WHERE dm.origin_po = s2.origin_po AND dm.origin_line_no = s2.origin_line_no
# MAGIC           AND dm.consumed_lot_no = s2.consumed_lot_no
# MAGIC     )
# MAGIC ),
# MAGIC 
# MAGIC step5_tracetootherpo AS (
# MAGIC     SELECT
# MAGIC         nt.origin_po, nt.origin_line_no, nt.consumed_item_no, nt.consumed_lot_no, nt.consumed_pcs,
# MAGIC         nt.consumed_lot_no AS l1_consumed_lot,
# MAGIC         o2.document_no AS l2_po, o2.order_line_no AS l2_line_no,
# MAGIC         o2.item_no AS l2_output_item, o2.lot_no AS l2_lot_no
# MAGIC     FROM needtrace AS nt
# MAGIC     INNER JOIN ile AS o2
# MAGIC         ON  o2.lot_no = nt.consumed_lot_no AND o2.entry_type = 'Output'
# MAGIC         AND o2.document_no <> nt.origin_po
# MAGIC ),
# MAGIC 
# MAGIC -- STEP 6 (Level 2): direct + internal
# MAGIC level2_metal_direct AS (
# MAGIC     SELECT
# MAGIC         t.origin_po, t.origin_line_no, t.consumed_item_no, t.consumed_lot_no, t.consumed_pcs,
# MAGIC         t.l2_po AS metal_found_in_po,
# MAGIC         m2.item_no AS metal_item_no, m2.description AS metal_description, m2.lot_no AS metal_lot_no,
# MAGIC         ABS(m2.quantity) AS metal_qty_gr, m2.uom AS metal_uom, m2.location_code AS metal_location,
# MAGIC         m2.order_line_no AS metal_line_no, m2.source_no AS metal_source_no, 2 AS trace_level
# MAGIC     FROM step5_tracetootherpo AS t
# MAGIC     INNER JOIN ile AS m2
# MAGIC         ON  m2.document_no = t.l2_po AND m2.entry_type = 'Consumption'
# MAGIC         AND m2.uom = 'GR' AND m2.order_line_no = t.l2_line_no
# MAGIC     INNER JOIN item_master AS im_m2 ON im_m2.item_no = m2.item_no
# MAGIC     WHERE im_m2.item_category_code IN ('PURE METAL', 'MIXED METAL', 'ALLOY', 'SOLDER', 'WIRE')
# MAGIC ),
# MAGIC 
# MAGIC level2_needinternal AS (
# MAGIC     SELECT DISTINCT
# MAGIC         t.origin_po, t.origin_line_no, t.consumed_item_no, t.consumed_lot_no, t.consumed_pcs,
# MAGIC         t.l2_po, t.l2_line_no
# MAGIC     FROM step5_tracetootherpo AS t
# MAGIC     WHERE NOT EXISTS (
# MAGIC         SELECT 1 FROM level2_metal_direct AS ld
# MAGIC         WHERE ld.origin_po = t.origin_po AND ld.origin_line_no = t.origin_line_no
# MAGIC           AND ld.consumed_lot_no = t.consumed_lot_no
# MAGIC     )
# MAGIC ),
# MAGIC 
# MAGIC level2_internaltrace AS (
# MAGIC     SELECT
# MAGIC         ni.origin_po, ni.origin_line_no, ni.consumed_item_no, ni.consumed_lot_no, ni.consumed_pcs,
# MAGIC         ni.l2_po, o_int.order_line_no AS internal_output_line
# MAGIC     FROM level2_needinternal AS ni
# MAGIC     INNER JOIN ile AS c_int
# MAGIC         ON  c_int.document_no = ni.l2_po AND c_int.entry_type = 'Consumption'
# MAGIC         AND c_int.order_line_no = ni.l2_line_no
# MAGIC     INNER JOIN item_master AS im_ci ON im_ci.item_no = c_int.item_no
# MAGIC     INNER JOIN ile AS o_int
# MAGIC         ON  o_int.document_no = ni.l2_po AND o_int.entry_type = 'Output'
# MAGIC         AND o_int.item_no = c_int.source_no
# MAGIC     WHERE im_ci.item_category_code IN ('CASTING', 'SEMI-FG')
# MAGIC ),
# MAGIC 
# MAGIC level2_metal_internal AS (
# MAGIC     SELECT
# MAGIC         lit.origin_po, lit.origin_line_no, lit.consumed_item_no, lit.consumed_lot_no, lit.consumed_pcs,
# MAGIC         lit.l2_po AS metal_found_in_po,
# MAGIC         m2i.item_no AS metal_item_no, m2i.description AS metal_description, m2i.lot_no AS metal_lot_no,
# MAGIC         ABS(m2i.quantity) AS metal_qty_gr, m2i.uom AS metal_uom, m2i.location_code AS metal_location,
# MAGIC         m2i.order_line_no AS metal_line_no, m2i.source_no AS metal_source_no, 2 AS trace_level
# MAGIC     FROM level2_internaltrace AS lit
# MAGIC     INNER JOIN ile AS m2i
# MAGIC         ON  m2i.document_no = lit.l2_po AND m2i.entry_type = 'Consumption'
# MAGIC         AND m2i.uom = 'GR' AND m2i.order_line_no = lit.internal_output_line
# MAGIC     INNER JOIN item_master AS im_m2i ON im_m2i.item_no = m2i.item_no
# MAGIC     WHERE im_m2i.item_category_code IN ('PURE METAL', 'MIXED METAL', 'ALLOY', 'SOLDER', 'WIRE')
# MAGIC ),
# MAGIC 
# MAGIC level2_metal AS (
# MAGIC     SELECT * FROM level2_metal_direct
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM level2_metal_internal
# MAGIC ),
# MAGIC 
# MAGIC -- STEP 7 (Level 3)
# MAGIC needtrace2 AS (
# MAGIC     SELECT DISTINCT
# MAGIC         t.origin_po, t.origin_line_no, t.consumed_item_no, t.consumed_lot_no, t.consumed_pcs,
# MAGIC         t.l2_po, t.l2_line_no
# MAGIC     FROM step5_tracetootherpo AS t
# MAGIC     WHERE NOT EXISTS (
# MAGIC         SELECT 1 FROM level2_metal AS lm
# MAGIC         WHERE lm.origin_po = t.origin_po AND lm.origin_line_no = t.origin_line_no
# MAGIC           AND lm.consumed_lot_no = t.consumed_lot_no
# MAGIC     )
# MAGIC ),
# MAGIC 
# MAGIC step7_consinl2po AS (
# MAGIC     SELECT
# MAGIC         nt2.origin_po, nt2.origin_line_no, nt2.consumed_item_no, nt2.consumed_lot_no, nt2.consumed_pcs,
# MAGIC         nt2.l2_po, c2.lot_no AS l2_consumed_lot
# MAGIC     FROM needtrace2 AS nt2
# MAGIC     INNER JOIN ile AS c2
# MAGIC         ON  c2.document_no = nt2.l2_po AND c2.order_line_no = nt2.l2_line_no
# MAGIC         AND c2.entry_type = 'Consumption'
# MAGIC     INNER JOIN item_master AS im_c2 ON im_c2.item_no = c2.item_no
# MAGIC     WHERE im_c2.item_category_code IN ('CASTING', 'SEMI-FG')
# MAGIC ),
# MAGIC 
# MAGIC step7_tracetothirdpo AS (
# MAGIC     SELECT
# MAGIC         s7.origin_po, s7.origin_line_no, s7.consumed_item_no, s7.consumed_lot_no, s7.consumed_pcs,
# MAGIC         s7.l2_consumed_lot,
# MAGIC         o3.document_no AS l3_po, o3.order_line_no AS l3_line_no,
# MAGIC         o3.item_no AS l3_output_item, o3.lot_no AS l3_lot_no
# MAGIC     FROM step7_consinl2po AS s7
# MAGIC     INNER JOIN ile AS o3
# MAGIC         ON  o3.lot_no = s7.l2_consumed_lot AND o3.entry_type = 'Output'
# MAGIC         AND o3.document_no <> s7.l2_po
# MAGIC ),
# MAGIC 
# MAGIC level3_metal AS (
# MAGIC     SELECT
# MAGIC         t3.origin_po, t3.origin_line_no, t3.consumed_item_no, t3.consumed_lot_no, t3.consumed_pcs,
# MAGIC         t3.l3_po AS metal_found_in_po,
# MAGIC         m3.item_no AS metal_item_no, m3.description AS metal_description, m3.lot_no AS metal_lot_no,
# MAGIC         ABS(m3.quantity) AS metal_qty_gr, m3.uom AS metal_uom, m3.location_code AS metal_location,
# MAGIC         m3.order_line_no AS metal_line_no, m3.source_no AS metal_source_no, 3 AS trace_level
# MAGIC     FROM step7_tracetothirdpo AS t3
# MAGIC     INNER JOIN ile AS m3
# MAGIC         ON  m3.document_no = t3.l3_po AND m3.entry_type = 'Consumption'
# MAGIC         AND m3.uom = 'GR' AND m3.order_line_no = t3.l3_line_no
# MAGIC     INNER JOIN item_master AS im_m3 ON im_m3.item_no = m3.item_no
# MAGIC     WHERE im_m3.item_category_code IN ('PURE METAL', 'MIXED METAL', 'ALLOY', 'SOLDER', 'WIRE')
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- DLT path: step2 ไม่เจอ Output ใน PO เดียวกัน → trace cross-PO
# MAGIC -- ============================================================================
# MAGIC nooutput_semi AS (
# MAGIC     SELECT
# MAGIC         s1.origin_po, s1.origin_line_no, s1.consumed_item_no, s1.consumed_source_no,
# MAGIC         s1.consumed_lot_no, ABS(s1.consumed_qty) AS consumed_pcs, s1.consumed_location
# MAGIC     FROM step1_consumption AS s1
# MAGIC     WHERE NOT EXISTS (
# MAGIC         SELECT 1 FROM step2_findoutput AS s2
# MAGIC         WHERE s2.origin_po = s1.origin_po AND s2.origin_line_no = s1.origin_line_no
# MAGIC           AND s2.consumed_lot_no = s1.consumed_lot_no
# MAGIC     )
# MAGIC ),
# MAGIC 
# MAGIC dlt_tracetootherpo AS (
# MAGIC     SELECT
# MAGIC         ns.origin_po, ns.origin_line_no, ns.consumed_item_no, ns.consumed_lot_no, ns.consumed_pcs,
# MAGIC         o2.document_no AS l2d_po, o2.order_line_no AS l2d_line_no,
# MAGIC         o2.item_no AS l2d_output_item, o2.lot_no AS l2d_lot_no
# MAGIC     FROM nooutput_semi AS ns
# MAGIC     INNER JOIN ile AS o2
# MAGIC         ON  o2.lot_no = ns.consumed_lot_no AND o2.entry_type = 'Output'
# MAGIC         AND o2.document_no <> ns.origin_po
# MAGIC ),
# MAGIC 
# MAGIC dlt_level2d_metal_direct AS (
# MAGIC     SELECT
# MAGIC         dt.origin_po, dt.origin_line_no, dt.consumed_item_no, dt.consumed_lot_no, dt.consumed_pcs,
# MAGIC         dt.l2d_po AS metal_found_in_po,
# MAGIC         m.item_no AS metal_item_no, m.description AS metal_description, m.lot_no AS metal_lot_no,
# MAGIC         ABS(m.quantity) AS metal_qty_gr, m.uom AS metal_uom, m.location_code AS metal_location,
# MAGIC         m.order_line_no AS metal_line_no, m.source_no AS metal_source_no, 2 AS trace_level
# MAGIC     FROM dlt_tracetootherpo AS dt
# MAGIC     INNER JOIN ile AS m
# MAGIC         ON  m.document_no = dt.l2d_po AND m.entry_type = 'Consumption'
# MAGIC         AND m.uom = 'GR' AND m.order_line_no = dt.l2d_line_no
# MAGIC     INNER JOIN item_master AS im_dm ON im_dm.item_no = m.item_no
# MAGIC     WHERE im_dm.item_category_code IN ('PURE METAL', 'MIXED METAL', 'ALLOY', 'SOLDER', 'WIRE')
# MAGIC ),
# MAGIC 
# MAGIC dlt_needinternal AS (
# MAGIC     SELECT DISTINCT
# MAGIC         dt.origin_po, dt.origin_line_no, dt.consumed_item_no, dt.consumed_lot_no, dt.consumed_pcs,
# MAGIC         dt.l2d_po, dt.l2d_line_no
# MAGIC     FROM dlt_tracetootherpo AS dt
# MAGIC     WHERE NOT EXISTS (
# MAGIC         SELECT 1 FROM dlt_level2d_metal_direct AS dd
# MAGIC         WHERE dd.origin_po = dt.origin_po AND dd.origin_line_no = dt.origin_line_no
# MAGIC           AND dd.consumed_lot_no = dt.consumed_lot_no
# MAGIC     )
# MAGIC ),
# MAGIC 
# MAGIC dlt_internaltrace AS (
# MAGIC     SELECT
# MAGIC         dni.origin_po, dni.origin_line_no, dni.consumed_item_no, dni.consumed_lot_no, dni.consumed_pcs,
# MAGIC         dni.l2d_po, o_int.order_line_no AS internal_output_line
# MAGIC     FROM dlt_needinternal AS dni
# MAGIC     INNER JOIN ile AS c_int
# MAGIC         ON  c_int.document_no = dni.l2d_po AND c_int.entry_type = 'Consumption'
# MAGIC         AND c_int.order_line_no = dni.l2d_line_no
# MAGIC     INNER JOIN item_master AS im_dci ON im_dci.item_no = c_int.item_no
# MAGIC     INNER JOIN ile AS o_int
# MAGIC         ON  o_int.document_no = dni.l2d_po AND o_int.entry_type = 'Output'
# MAGIC         AND o_int.item_no = c_int.source_no
# MAGIC     WHERE im_dci.item_category_code IN ('CASTING', 'SEMI-FG')
# MAGIC ),
# MAGIC 
# MAGIC dlt_level2d_metal_internal AS (
# MAGIC     SELECT
# MAGIC         dit.origin_po, dit.origin_line_no, dit.consumed_item_no, dit.consumed_lot_no, dit.consumed_pcs,
# MAGIC         dit.l2d_po AS metal_found_in_po,
# MAGIC         m.item_no AS metal_item_no, m.description AS metal_description, m.lot_no AS metal_lot_no,
# MAGIC         ABS(m.quantity) AS metal_qty_gr, m.uom AS metal_uom, m.location_code AS metal_location,
# MAGIC         m.order_line_no AS metal_line_no, m.source_no AS metal_source_no, 2 AS trace_level
# MAGIC     FROM dlt_internaltrace AS dit
# MAGIC     INNER JOIN ile AS m
# MAGIC         ON  m.document_no = dit.l2d_po AND m.entry_type = 'Consumption'
# MAGIC         AND m.uom = 'GR' AND m.order_line_no = dit.internal_output_line
# MAGIC     INNER JOIN item_master AS im_dmi ON im_dmi.item_no = m.item_no
# MAGIC     WHERE im_dmi.item_category_code IN ('PURE METAL', 'MIXED METAL', 'ALLOY', 'SOLDER', 'WIRE')
# MAGIC ),
# MAGIC 
# MAGIC dlt_level2d_metal AS (
# MAGIC     SELECT * FROM dlt_level2d_metal_direct
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM dlt_level2d_metal_internal
# MAGIC ),
# MAGIC 
# MAGIC -- DLT Level 3
# MAGIC dlt_needtrace3 AS (
# MAGIC     SELECT DISTINCT
# MAGIC         dt.origin_po, dt.origin_line_no, dt.consumed_item_no, dt.consumed_lot_no, dt.consumed_pcs,
# MAGIC         dt.l2d_po, dt.l2d_line_no
# MAGIC     FROM dlt_tracetootherpo AS dt
# MAGIC     WHERE NOT EXISTS (
# MAGIC         SELECT 1 FROM dlt_level2d_metal AS dm
# MAGIC         WHERE dm.origin_po = dt.origin_po AND dm.origin_line_no = dt.origin_line_no
# MAGIC           AND dm.consumed_lot_no = dt.consumed_lot_no
# MAGIC     )
# MAGIC ),
# MAGIC 
# MAGIC dlt_step3_consinpo AS (
# MAGIC     SELECT
# MAGIC         nt3.origin_po, nt3.origin_line_no, nt3.consumed_item_no, nt3.consumed_lot_no, nt3.consumed_pcs,
# MAGIC         nt3.l2d_po, c3.lot_no AS l2d_consumed_lot
# MAGIC     FROM dlt_needtrace3 AS nt3
# MAGIC     INNER JOIN ile AS c3
# MAGIC         ON  c3.document_no = nt3.l2d_po AND c3.order_line_no = nt3.l2d_line_no
# MAGIC         AND c3.entry_type = 'Consumption'
# MAGIC     INNER JOIN item_master AS im_dc3 ON im_dc3.item_no = c3.item_no
# MAGIC     WHERE im_dc3.item_category_code IN ('CASTING', 'SEMI-FG')
# MAGIC ),
# MAGIC 
# MAGIC dlt_step3_tracetothirdpo AS (
# MAGIC     SELECT
# MAGIC         s3.origin_po, s3.origin_line_no, s3.consumed_item_no, s3.consumed_lot_no, s3.consumed_pcs,
# MAGIC         s3.l2d_consumed_lot,
# MAGIC         o3.document_no AS l3d_po, o3.order_line_no AS l3d_line_no,
# MAGIC         o3.item_no AS l3d_output_item, o3.lot_no AS l3d_lot_no
# MAGIC     FROM dlt_step3_consinpo AS s3
# MAGIC     INNER JOIN ile AS o3
# MAGIC         ON  o3.lot_no = s3.l2d_consumed_lot AND o3.entry_type = 'Output'
# MAGIC         AND o3.document_no <> s3.l2d_po
# MAGIC ),
# MAGIC 
# MAGIC dlt_level3d_metal AS (
# MAGIC     SELECT
# MAGIC         t3.origin_po, t3.origin_line_no, t3.consumed_item_no, t3.consumed_lot_no, t3.consumed_pcs,
# MAGIC         t3.l3d_po AS metal_found_in_po,
# MAGIC         m3.item_no AS metal_item_no, m3.description AS metal_description, m3.lot_no AS metal_lot_no,
# MAGIC         ABS(m3.quantity) AS metal_qty_gr, m3.uom AS metal_uom, m3.location_code AS metal_location,
# MAGIC         m3.order_line_no AS metal_line_no, m3.source_no AS metal_source_no, 3 AS trace_level
# MAGIC     FROM dlt_step3_tracetothirdpo AS t3
# MAGIC     INNER JOIN ile AS m3
# MAGIC         ON  m3.document_no = t3.l3d_po AND m3.entry_type = 'Consumption'
# MAGIC         AND m3.uom = 'GR' AND m3.order_line_no = t3.l3d_line_no
# MAGIC     INNER JOIN item_master AS im_dm3 ON im_dm3.item_no = m3.item_no
# MAGIC     WHERE im_dm3.item_category_code IN ('PURE METAL', 'MIXED METAL', 'ALLOY', 'SOLDER', 'WIRE')
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- PART C: Combine all traced metal → summary
# MAGIC -- ============================================================================
# MAGIC allmetal AS (
# MAGIC     SELECT * FROM direct_metal
# MAGIC     UNION ALL SELECT * FROM level2_metal
# MAGIC     UNION ALL SELECT * FROM level3_metal
# MAGIC     UNION ALL SELECT * FROM dlt_level2d_metal
# MAGIC     UNION ALL SELECT * FROM dlt_level3d_metal
# MAGIC ),
# MAGIC 
# MAGIC metalagg AS (
# MAGIC     SELECT
# MAGIC         origin_po, origin_line_no, consumed_item_no, consumed_lot_no, consumed_pcs,
# MAGIC         metal_found_in_po, metal_item_no, metal_description, metal_source_no,
# MAGIC         SUM(metal_qty_gr) AS total_metal_qty_gr, metal_uom, metal_location,
# MAGIC         MIN(trace_level) AS trace_level
# MAGIC     FROM allmetal
# MAGIC     GROUP BY
# MAGIC         origin_po, origin_line_no, consumed_item_no, consumed_lot_no, consumed_pcs,
# MAGIC         metal_found_in_po, metal_item_no, metal_description, metal_source_no,
# MAGIC         metal_uom, metal_location
# MAGIC ),
# MAGIC 
# MAGIC outputqty AS (
# MAGIC     SELECT document_no, order_line_no, source_no, SUM(quantity) AS output_pcs
# MAGIC     FROM ile WHERE entry_type = 'Output'
# MAGIC     GROUP BY document_no, order_line_no, source_no
# MAGIC ),
# MAGIC 
# MAGIC traced_metal AS (
# MAGIC     SELECT
# MAGIC         ma.origin_po, ma.origin_line_no, ma.consumed_item_no, ma.consumed_lot_no, ma.consumed_pcs,
# MAGIC         ma.metal_found_in_po, ma.metal_item_no, ma.metal_description,
# MAGIC         ma.total_metal_qty_gr, ma.metal_uom, ma.metal_location, ma.trace_level,
# MAGIC         oq.output_pcs,
# MAGIC         CASE WHEN oq.output_pcs IS NOT NULL AND oq.output_pcs <> 0
# MAGIC              THEN ma.total_metal_qty_gr / oq.output_pcs ELSE NULL END AS metal_per_piece_gr,
# MAGIC         CASE WHEN oq.output_pcs IS NOT NULL AND oq.output_pcs <> 0
# MAGIC              THEN ma.consumed_pcs * (ma.total_metal_qty_gr / oq.output_pcs) ELSE NULL END AS metal_for_this_line_gr
# MAGIC     FROM metalagg AS ma
# MAGIC     LEFT JOIN outputqty AS oq
# MAGIC         ON  oq.document_no = ma.metal_found_in_po
# MAGIC         AND oq.source_no   = ma.metal_source_no
# MAGIC ),
# MAGIC 
# MAGIC traced_metal_summary AS (
# MAGIC     SELECT
# MAGIC         origin_po, origin_line_no, consumed_item_no, consumed_lot_no,
# MAGIC         SUM(metal_for_this_line_gr) AS traced_metal_gr,
# MAGIC         MIN(trace_level)            AS min_trace_level,
# MAGIC         COUNT(DISTINCT metal_found_in_po) AS traced_po_count
# MAGIC     FROM traced_metal
# MAGIC     GROUP BY origin_po, origin_line_no, consumed_item_no, consumed_lot_no
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- PART D: Routing — Routing Link Code = 'AS1' → assembly before filing
# MAGIC -- ============================================================================
# MAGIC routing_flag AS (
# MAGIC     SELECT
# MAGIC         `Routing No.`   AS routing_no,
# MAGIC         1               AS assembly_before_filing
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Routing Line`
# MAGIC     WHERE `Routing Link Code` = 'AS1'
# MAGIC     GROUP BY `Routing No.`
# MAGIC )
# MAGIC 
# MAGIC -- ============================================================================
# MAGIC -- PART E: FINAL OUTPUT
# MAGIC -- ★ มี AS1 → casting + finding weight
# MAGIC -- ★ ไม่มี AS1 → casting อย่างเดียว
# MAGIC -- ============================================================================
# MAGIC SELECT
# MAGIC     ac.document_no                                AS origin_po,
# MAGIC     ac.order_line_no                              AS origin_line_no,
# MAGIC     ac.item_no,
# MAGIC     ac.description,
# MAGIC     ac.lot_no,
# MAGIC     ac.quantity,
# MAGIC     ac.abs_qty,
# MAGIC     ac.uom,
# MAGIC     ac.location_code,
# MAGIC     ac.source_no,
# MAGIC     ac.item_type,
# MAGIC 
# MAGIC     COALESCE(rf.assembly_before_filing, 0)        AS assembly_before_filing,
# MAGIC 
# MAGIC     -- Metal weight: casting always, finding only if AS1
# MAGIC     CASE
# MAGIC         WHEN ac.item_type = 'METAL'
# MAGIC             THEN ac.abs_qty
# MAGIC         WHEN ac.item_type = 'FINDINGS'
# MAGIC              AND ac.uom = 'GR'
# MAGIC              AND COALESCE(rf.assembly_before_filing, 0) = 1
# MAGIC             THEN ac.abs_qty
# MAGIC         WHEN ac.item_type IN ('SEMI', 'CAST') AND tm.traced_metal_gr IS NOT NULL
# MAGIC             THEN tm.traced_metal_gr
# MAGIC         ELSE NULL
# MAGIC     END                                           AS metal_weight_gr,
# MAGIC 
# MAGIC     -- Metal source
# MAGIC     CASE
# MAGIC         WHEN ac.item_type = 'METAL'
# MAGIC             THEN 'DIRECT'
# MAGIC         WHEN ac.item_type = 'FINDINGS'
# MAGIC              AND ac.uom = 'GR'
# MAGIC              AND COALESCE(rf.assembly_before_filing, 0) = 1
# MAGIC             THEN 'DIRECT_GR'
# MAGIC         WHEN ac.item_type = 'FINDINGS'
# MAGIC              AND ac.uom = 'GR'
# MAGIC              AND COALESCE(rf.assembly_before_filing, 0) = 0
# MAGIC             THEN 'EXCLUDED_ROUTING'
# MAGIC         WHEN ac.item_type IN ('SEMI', 'CAST') AND tm.traced_metal_gr IS NOT NULL
# MAGIC             THEN CONCAT('TRACED_L', tm.min_trace_level)
# MAGIC         WHEN ac.item_type IN ('SEMI', 'CAST') AND tm.traced_metal_gr IS NULL
# MAGIC             THEN 'NOT_FOUND'
# MAGIC         ELSE NULL
# MAGIC     END                                           AS metal_source,
# MAGIC 
# MAGIC     tm.traced_metal_gr,
# MAGIC     tm.min_trace_level,
# MAGIC     tm.traced_po_count
# MAGIC 
# MAGIC FROM all_consumption AS ac
# MAGIC LEFT JOIN traced_metal_summary AS tm
# MAGIC     ON  tm.origin_po        = ac.document_no
# MAGIC     AND tm.origin_line_no   = ac.order_line_no
# MAGIC     AND tm.consumed_item_no = ac.item_no
# MAGIC     AND tm.consumed_lot_no  = ac.lot_no
# MAGIC LEFT JOIN routing_flag AS rf
# MAGIC     ON  rf.routing_no = ac.source_no
# MAGIC 
# MAGIC -- WHERE ac.document_no = 'WRO260201965'  -- ★ เปลี่ยน PO ทดสอบ
# MAGIC -- ORDER BY ac.order_line_no, ac.item_type, ac.item_no, ac.lot_no
# MAGIC ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Metal Loss Summary

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.inv.gold_metal_loss_summary AS
# MAGIC -- ============================================================================
# MAGIC -- Metal Loss Summary v2.3 (Spark SQL)
# MAGIC -- ============================================================================
# MAGIC -- v2.1: แยก consumption เป็น 2 คอลัมน์
# MAGIC -- v2.3: ★ ปรับ item_type ตาม lot tracing v9.3 (Item Category Code)
# MAGIC --   - casting_gr  = METAL + SEMI + CAST
# MAGIC --   - finding_gr  = FINDINGS ที่ metal_source = 'DIRECT_GR'
# MAGIC -- ============================================================================
# MAGIC WITH
# MAGIC 
# MAGIC ile_consumption_by_line AS (
# MAGIC     SELECT
# MAGIC         origin_po      AS prod_order_no,
# MAGIC         origin_line_no AS prod_order_line_no,
# MAGIC         SUM(CASE
# MAGIC             WHEN item_type IN ('METAL', 'SEMI', 'CAST')
# MAGIC             THEN metal_weight_gr
# MAGIC             ELSE 0
# MAGIC         END) AS casting_gr,
# MAGIC         SUM(CASE
# MAGIC             WHEN item_type = 'FINDINGS'
# MAGIC              AND metal_source = 'DIRECT_GR'
# MAGIC             THEN metal_weight_gr
# MAGIC             ELSE 0
# MAGIC         END) AS finding_gr
# MAGIC     FROM Gold_Inventory_Lakehouse.inv.gold_metal_lot_tracing_finding
# MAGIC     GROUP BY origin_po, origin_line_no
# MAGIC ),
# MAGIC 
# MAGIC ile_consumption_by_po AS (
# MAGIC     SELECT
# MAGIC         origin_po AS prod_order_no,
# MAGIC         SUM(CASE
# MAGIC             WHEN item_type IN ('METAL', 'SEMI', 'CAST')
# MAGIC             THEN metal_weight_gr
# MAGIC             ELSE 0
# MAGIC         END) AS casting_gr,
# MAGIC         SUM(CASE
# MAGIC             WHEN item_type = 'FINDINGS'
# MAGIC              AND metal_source = 'DIRECT_GR'
# MAGIC             THEN metal_weight_gr
# MAGIC             ELSE 0
# MAGIC         END) AS finding_gr
# MAGIC     FROM Gold_Inventory_Lakehouse.inv.gold_metal_lot_tracing_finding
# MAGIC     GROUP BY origin_po
# MAGIC ),
# MAGIC 
# MAGIC ml_base AS (
# MAGIC     SELECT
# MAGIC         created_on              AS ml_created_on,
# MAGIC         item_no                 AS ml_item_no,
# MAGIC         metal_category_code     AS ml_metal_category_code,
# MAGIC         inventory_posting_group AS ml_inventory_posting_group,
# MAGIC         material_type           AS ml_material_type,
# MAGIC         prod_order_no           AS ml_prod_order_no,
# MAGIC         prod_order_line_no      AS ml_prod_order_line_no,
# MAGIC         machine_center_no       AS ml_machine_center_no,
# MAGIC         quantity                AS ml_quantity,
# MAGIC         weight                  AS ml_weight,
# MAGIC         dust_weight             AS ml_dust_weight,
# MAGIC         sprue_weight            AS ml_sprue_weight,
# MAGIC         user_id                 AS ml_user_id,
# MAGIC         cell_routing            AS ml_cell_routing,
# MAGIC         antenna_id              AS ml_antenna_id,
# MAGIC         employee_code           AS ml_employee_no,
# MAGIC         first_name_thai         AS ml_employee_first_name_thai,
# MAGIC         cell_line               AS ml_cell_line,
# MAGIC         prod_line               AS ml_prod_line,
# MAGIC         sales_order_no          AS ml_sales_order_no,
# MAGIC         sales_order_line_no     AS ml_sales_order_line_no,
# MAGIC         fg_item_no              AS ml_fg_item_no,
# MAGIC         fg_item_group           AS ml_fg_item_group,
# MAGIC         cus_no                  AS ml_customer_no,
# MAGIC         cus_name                AS ml_customer_name,
# MAGIC         cus_abbr                AS ml_customer_abbr,
# MAGIC         status_so               AS ml_sales_order_status
# MAGIC     FROM `Gold_Inventory_Lakehouse`.`inv`.`gold_metal_loss`
# MAGIC ),
# MAGIC 
# MAGIC ml_sum AS (
# MAGIC     SELECT
# MAGIC         ml_prod_order_no,
# MAGIC         ml_prod_order_line_no,
# MAGIC         ml_item_no,
# MAGIC         SUM(ml_quantity)     AS ml_quantity,
# MAGIC         SUM(ml_weight)       AS ml_weight,
# MAGIC         SUM(ml_dust_weight)  AS ml_dust_weight,
# MAGIC         SUM(ml_sprue_weight) AS ml_sprue_weight,
# MAGIC         MAX(ml_created_on)   AS ml_last_created_on
# MAGIC     FROM ml_base
# MAGIC     GROUP BY
# MAGIC         ml_prod_order_no,
# MAGIC         ml_prod_order_line_no,
# MAGIC         ml_item_no
# MAGIC ),
# MAGIC 
# MAGIC ml_latest AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             b.*,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY b.ml_prod_order_no, b.ml_prod_order_line_no, b.ml_item_no
# MAGIC                 ORDER BY b.ml_created_on DESC
# MAGIC             ) AS rn
# MAGIC         FROM ml_base b
# MAGIC     ) x
# MAGIC     WHERE rn = 1
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC 
# MAGIC     mll.ml_prod_order_no        AS prod_order_no,
# MAGIC     mll.ml_prod_order_line_no   AS prod_order_line_no,
# MAGIC     mll.ml_fg_item_no           AS ml_fg_item_no,
# MAGIC     mll.ml_fg_item_group        AS ml_fg_item_group,
# MAGIC 
# MAGIC     -- ★ v2.3: ตาม lot tracing v9.3
# MAGIC     COALESCE(ile_line.casting_gr, ile_po.casting_gr)   AS consumption_casting_gr,
# MAGIC     COALESCE(ile_line.finding_gr, ile_po.finding_gr)   AS consumption_finding_gr,
# MAGIC 
# MAGIC     COALESCE(ile_line.casting_gr, ile_po.casting_gr, 0)
# MAGIC       + COALESCE(ile_line.finding_gr, ile_po.finding_gr, 0)
# MAGIC                                                         AS consumption_quantity,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN ile_line.casting_gr IS NOT NULL THEN 'LINE'
# MAGIC         WHEN ile_po.casting_gr   IS NOT NULL THEN 'PO'
# MAGIC         ELSE 'NO_MATCH'
# MAGIC     END AS consumption_match_level,
# MAGIC 
# MAGIC     COALESCE(mls.ml_quantity, 0)     AS ml_quantity,
# MAGIC     COALESCE(mls.ml_weight, 0)       AS ml_weight,
# MAGIC     COALESCE(mls.ml_dust_weight, 0)  AS ml_dust_weight,
# MAGIC     COALESCE(mls.ml_sprue_weight, 0) AS ml_sprue_weight,
# MAGIC     mls.ml_last_created_on           AS ml_last_created_on,
# MAGIC 
# MAGIC     mll.ml_created_on                AS ml_created_on,
# MAGIC     mll.ml_item_no                   AS ml_item_no,
# MAGIC     mll.ml_metal_category_code       AS ml_metal_category_code,
# MAGIC     mll.ml_inventory_posting_group   AS ml_inventory_posting_group,
# MAGIC     mll.ml_material_type             AS ml_material_type,
# MAGIC     mll.ml_prod_order_no             AS ml_prod_order_no,
# MAGIC     mll.ml_prod_order_line_no        AS ml_prod_order_line_no,
# MAGIC     mll.ml_machine_center_no         AS ml_machine_center_no,
# MAGIC     mll.ml_user_id                   AS ml_user_id,
# MAGIC     mll.ml_cell_routing              AS ml_cell_routing,
# MAGIC     mll.ml_antenna_id                AS ml_antenna_id,
# MAGIC     mll.ml_employee_no               AS ml_employee_no,
# MAGIC     mll.ml_employee_first_name_thai  AS ml_employee_first_name_thai,
# MAGIC     mll.ml_cell_line                 AS ml_cell_line,
# MAGIC     mll.ml_prod_line                 AS ml_prod_line,
# MAGIC     mll.ml_sales_order_no            AS ml_sales_order_no,
# MAGIC     mll.ml_sales_order_line_no       AS ml_sales_order_line_no,
# MAGIC     mll.ml_customer_no               AS ml_customer_no,
# MAGIC     mll.ml_customer_name             AS ml_customer_name,
# MAGIC     mll.ml_customer_abbr             AS ml_customer_abbr,
# MAGIC     mll.ml_sales_order_status        AS ml_sales_order_status
# MAGIC 
# MAGIC FROM ml_latest mll
# MAGIC 
# MAGIC LEFT JOIN ml_sum mls
# MAGIC     ON  mls.ml_prod_order_no      = mll.ml_prod_order_no
# MAGIC     AND mls.ml_prod_order_line_no = mll.ml_prod_order_line_no
# MAGIC 
# MAGIC LEFT JOIN ile_consumption_by_line ile_line
# MAGIC     ON  ile_line.prod_order_no      = mll.ml_prod_order_no
# MAGIC     AND ile_line.prod_order_line_no = mll.ml_prod_order_line_no
# MAGIC 
# MAGIC LEFT JOIN ile_consumption_by_po ile_po
# MAGIC     ON  ile_po.prod_order_no = mll.ml_prod_order_no
# MAGIC ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Metal Loss By Line - Filter Casting

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.inv.gold_metal_loss_by_line
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH agg AS (
# MAGIC   SELECT
# MAGIC     MAX(ml_last_created_on)                               AS Date,
# MAGIC     ml_prod_line,
# MAGIC     ml_cell_line,
# MAGIC     ml_employee_first_name_thai,
# MAGIC     ml_fg_item_no,
# MAGIC     ml_item_no,
# MAGIC     ml_prod_order_no,
# MAGIC     ml_material_type,
# MAGIC     SUM(consumption_quantity)                             AS total_consumption,
# MAGIC     MAX(ml_weight)                                        AS weight_after_FL,
# MAGIC     MAX(ml_sprue_weight)                                  AS Scrap,
# MAGIC     MAX(ml_dust_weight)                                   AS Dust
# MAGIC   FROM Gold_Inventory_Lakehouse.inv.gold_metal_loss_summary
# MAGIC   GROUP BY
# MAGIC     ml_prod_line,
# MAGIC     ml_cell_line,
# MAGIC     ml_employee_first_name_thai,
# MAGIC     ml_fg_item_no,
# MAGIC     ml_item_no,
# MAGIC     ml_prod_order_no,
# MAGIC     ml_material_type
# MAGIC )
# MAGIC SELECT
# MAGIC   Date,
# MAGIC   ml_prod_line                       AS Line,
# MAGIC   ml_cell_line                       AS Cell,
# MAGIC   ml_employee_first_name_thai        AS Name,
# MAGIC   ml_fg_item_no                      AS FG,
# MAGIC   ml_item_no                         AS item_no,
# MAGIC   ml_prod_order_no                   AS Prod,
# MAGIC   ml_material_type                   AS Metal,
# MAGIC 
# MAGIC   total_consumption,
# MAGIC   weight_after_FL,
# MAGIC   Scrap,
# MAGIC 
# MAGIC   (total_consumption - (weight_after_FL + Scrap)) AS loss_scrap_g,
# MAGIC   (total_consumption - (weight_after_FL + Scrap)) / NULLIF(total_consumption, 0) AS loss_scrap_pct,
# MAGIC   CASE
# MAGIC     WHEN ((total_consumption - (weight_after_FL + Scrap)) / NULLIF(total_consumption, 0)) > 0.03
# MAGIC       THEN 'OVER'
# MAGIC     ELSE 'UNDER'
# MAGIC   END AS loss_scrap_std_flag,
# MAGIC 
# MAGIC   Dust,
# MAGIC 
# MAGIC   (total_consumption - (weight_after_FL + Scrap + Dust)) AS loss_after_dust_g,
# MAGIC   (total_consumption - (weight_after_FL + Scrap + Dust)) / NULLIF(total_consumption, 0) AS loss_after_dust_pct,
# MAGIC   CASE
# MAGIC     WHEN ((total_consumption - (weight_after_FL + Scrap + Dust)) / NULLIF(total_consumption, 0)) > 0.03
# MAGIC       THEN 'OVER'
# MAGIC     ELSE 'UNDER'
# MAGIC   END AS loss_after_scrap_std_flag
# MAGIC FROM agg;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # Gold Metal Loss By Line 2 Pct

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.inv.gold_metal_loss_by_line_2_pct
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC WITH agg AS (
# MAGIC   SELECT
# MAGIC     MAX(ml_last_created_on) AS Date,
# MAGIC     ml_prod_line,
# MAGIC     ml_cell_line,
# MAGIC     ml_employee_no,
# MAGIC     ml_employee_first_name_thai,
# MAGIC     ml_fg_item_no,
# MAGIC     ml_item_no,
# MAGIC     ml_prod_order_no,
# MAGIC     ml_material_type,
# MAGIC     SUM(consumption_casting_gr)  AS total_casting,
# MAGIC     SUM(consumption_finding_gr)  AS total_finding,
# MAGIC     SUM(consumption_quantity)    AS total_consumption,
# MAGIC     MAX(ml_weight)               AS weight_after_FL,
# MAGIC     MAX(ml_sprue_weight)         AS Scrap,
# MAGIC     MAX(ml_dust_weight)          AS Dust
# MAGIC   FROM Gold_Inventory_Lakehouse.inv.gold_metal_loss_summary
# MAGIC   GROUP BY
# MAGIC     ml_prod_line,
# MAGIC     ml_cell_line,
# MAGIC     ml_employee_no,
# MAGIC     ml_employee_first_name_thai,
# MAGIC     ml_fg_item_no,
# MAGIC     ml_item_no,
# MAGIC     ml_prod_order_no,
# MAGIC     ml_material_type
# MAGIC ),
# MAGIC 
# MAGIC -- สรุปราคา metal ต่อ PO จาก gold_lot_cost
# MAGIC cost_per_po AS (
# MAGIC   SELECT
# MAGIC     production_order_no,
# MAGIC     SUM(consumption_cost_actual)
# MAGIC         / NULLIF(SUM(consumed_qty), 0)    AS avg_consumption_unit_cost,
# MAGIC     MAX(metal_origin_unit_cost)            AS metal_origin_unit_cost,
# MAGIC     MAX(metal_origin_item_no)              AS metal_origin_item_no,
# MAGIC     MAX(metal_origin_category)             AS metal_origin_category
# MAGIC   FROM Gold_Inventory_Lakehouse.inv.gold_lot_cost
# MAGIC   WHERE material_source_type = 'Manufactured (Semi-FG)'
# MAGIC     AND consumption_cost_actual > 0
# MAGIC   GROUP BY production_order_no
# MAGIC ),
# MAGIC 
# MAGIC calc AS (
# MAGIC   SELECT
# MAGIC     a.Date,
# MAGIC     a.ml_prod_line AS Line,
# MAGIC     a.ml_cell_line AS Cell,
# MAGIC     a.ml_employee_no AS Code,
# MAGIC     a.ml_employee_first_name_thai AS Name,
# MAGIC     a.ml_fg_item_no AS FG,
# MAGIC     a.ml_item_no AS item_no,
# MAGIC     a.ml_prod_order_no AS Prod,
# MAGIC     a.ml_material_type AS Metal,
# MAGIC 
# MAGIC     a.total_casting,
# MAGIC     a.total_finding,
# MAGIC     a.total_consumption,
# MAGIC     a.weight_after_FL,
# MAGIC     a.Scrap,
# MAGIC     a.Dust,
# MAGIC 
# MAGIC     -- Dust metal content: 20% ของ Dust ที่คืนมา
# MAGIC     (a.Dust * 0.20) AS Dust_metal,
# MAGIC 
# MAGIC     -- total_return ใช้ Dust_metal แทน Dust ดิบ
# MAGIC     (a.weight_after_FL + a.Scrap + (a.Dust * 0.20)) AS total_return,
# MAGIC 
# MAGIC     (a.weight_after_FL * 0.02) AS FL_allowance,
# MAGIC 
# MAGIC     (a.weight_after_FL + a.Scrap + (a.Dust * 0.20)) + (a.weight_after_FL * 0.02)
# MAGIC         AS total_with_allowance,
# MAGIC 
# MAGIC     a.total_consumption
# MAGIC       - ((a.weight_after_FL + a.Scrap + (a.Dust * 0.20)) + (a.weight_after_FL * 0.02))
# MAGIC         AS loss_g,
# MAGIC 
# MAGIC     (a.total_consumption
# MAGIC       - ((a.weight_after_FL + a.Scrap + (a.Dust * 0.20)) + (a.weight_after_FL * 0.02)))
# MAGIC       / NULLIF((a.weight_after_FL + a.Scrap + (a.Dust * 0.20)), 0)
# MAGIC         AS loss_pct,
# MAGIC 
# MAGIC     -- Cost data
# MAGIC     c.avg_consumption_unit_cost,
# MAGIC     c.metal_origin_unit_cost,
# MAGIC     c.metal_origin_item_no,
# MAGIC     c.metal_origin_category
# MAGIC 
# MAGIC   FROM agg a
# MAGIC   LEFT JOIN cost_per_po c
# MAGIC     ON a.ml_prod_order_no = c.production_order_no
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC   Date,
# MAGIC   Line,
# MAGIC   Cell,
# MAGIC   Code,
# MAGIC   Name,
# MAGIC   FG,
# MAGIC   item_no,
# MAGIC   Prod,
# MAGIC   Metal,
# MAGIC 
# MAGIC   total_casting,
# MAGIC   total_finding,
# MAGIC   total_consumption,
# MAGIC   weight_after_FL,
# MAGIC   Scrap,
# MAGIC   Dust,
# MAGIC   Dust_metal,
# MAGIC   total_return,
# MAGIC   FL_allowance,
# MAGIC   total_with_allowance,
# MAGIC 
# MAGIC   loss_g,
# MAGIC   loss_pct,
# MAGIC 
# MAGIC   CASE
# MAGIC     WHEN loss_pct > 0.02 THEN 'Missing'
# MAGIC     ELSE 'OK'
# MAGIC   END AS loss_std_flag,
# MAGIC 
# MAGIC   -- === Cost: ราคา alloy ที่ consume จริง ===
# MAGIC   avg_consumption_unit_cost,
# MAGIC   loss_g * avg_consumption_unit_cost        AS loss_cost_consumption,
# MAGIC 
# MAGIC   -- === Cost: ราคา raw metal (ทอง/เงินบริสุทธิ์) ===
# MAGIC   metal_origin_unit_cost,
# MAGIC   metal_origin_item_no,
# MAGIC   metal_origin_category,
# MAGIC   loss_g * metal_origin_unit_cost           AS loss_cost_raw_metal
# MAGIC 
# MAGIC FROM calc

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Metal Loss by Component

# CELL ********************

from pyspark.sql import functions as F

SOURCE_TABLE  = "Gold_Inventory_Lakehouse.inv.gold_metal_loss_summary"
TARGET_TABLE  = "Gold_Inventory_Lakehouse.inv.gold_metal_loss_by_component"

# -----------------------------
# 1) Read source (FULL LOAD)
# -----------------------------
src = (
    spark.table(SOURCE_TABLE)
         .filter(F.col("consumption_quantity") > 0)   # CST_CUT already enforced upstream (per your note)
)

if src.rdd.isEmpty():
    raise Exception(f"No data found in {SOURCE_TABLE}. Nothing to write.")

# -----------------------------
# 2) Aggregate (NO group by date; keep latest date)
# -----------------------------
gcols = [
    "ml_prod_line",                 # Line
    "ml_cell_line",                 # Cell
    "ml_employee_first_name_thai",  # Name
    "ml_item_no",                   # metal_loss item
    "ml_fg_item_no",                      # POC component item
    "ml_prod_order_no",             # Prod
    "ml_material_type",             # Metal
]

base = (
    src.groupBy(*[F.col(c) for c in gcols])
       .agg(
           F.max(F.col("ml_created_on")).alias("ml_created_on"),  # ✅ latest date for the group
           F.sum(F.col("consumption_quantity")).alias("total_consumption"),
           F.sum(F.col("ml_weight")).alias("weight_after_FL"),
           F.sum(F.col("ml_sprue_weight")).alias("Scrap"),
           F.sum(F.col("ml_dust_weight")).alias("Dust"),
       )
)

# -----------------------------
# 4) Final projection
# -----------------------------
df = (
    base.select(
        F.col("ml_created_on"),  # ✅ now "latest" date per group
        F.col("ml_prod_line"),
        F.col("ml_cell_line"),
        F.col("ml_employee_first_name_thai"),

        F.col("ml_item_no").alias("item_component_no"),
        F.col("ml_fg_item_no").alias("item_no"),

        F.col("ml_prod_order_no"),
        F.col("ml_material_type"),

        F.col("total_consumption"),
        F.col("weight_after_FL"),
        F.col("Scrap"),
        (F.col("total_consumption") - (F.col("weight_after_FL") + F.col("Scrap"))).alias("loss_scrap_g"),
        F.when(F.col("total_consumption") != 0,
               (F.col("total_consumption") - (F.col("weight_after_FL") + F.col("Scrap"))) / F.col("total_consumption")
        ).alias("loss_scrap_pct"),

        F.when(
            F.when(F.col("ml_material_type") == "Ag", F.lit(0.03))
             .when(F.col("ml_material_type") == "Au", F.lit(0.02))
             .otherwise(F.lit(0.03))
            < F.when(F.col("total_consumption") != 0,
                     (F.col("total_consumption") - (F.col("weight_after_FL") + F.col("Scrap"))) / F.col("total_consumption")
            ),
            "OVER"
        ).otherwise("UNDER").alias("loss_scrap_std_flag"),

        F.col("Dust"),
        (F.col("total_consumption") - (F.col("weight_after_FL") + F.col("Scrap") + F.col("Dust"))).alias("loss_after_dust_g"),
        F.when(F.col("total_consumption") != 0,
               (F.col("total_consumption") - (F.col("weight_after_FL") + F.col("Scrap") + F.col("Dust"))) / F.col("total_consumption")
        ).alias("loss_after_dust_pct"),

        F.when(
            F.when(F.col("total_consumption") != 0,
                   (F.col("total_consumption") - (F.col("weight_after_FL") + F.col("Scrap") + F.col("Dust"))) / F.col("total_consumption")
            ) > F.lit(0.03),
            "OVER"
        ).otherwise("UNDER").alias("loss_after_scrap_std_flag"),

        F.current_timestamp().alias("updated_at")
    )
)

# -----------------------------
# 3) Loss calculations
# -----------------------------
total  = F.col("total_consumption")
weight = F.col("weight_after_FL")
sprue  = F.col("Scrap")
dust   = F.col("Dust")

loss_scrap_g = total - (weight + sprue)
loss_scrap_pct = F.when(total != 0, loss_scrap_g / total)

loss_after_dust_g = total - (weight + sprue + dust)
loss_after_dust_pct = F.when(total != 0, loss_after_dust_g / total)

scrap_std_pct = (
    F.when(F.col("ml_material_type") == "Ag", F.lit(0.02))
     .when(F.col("ml_material_type") == "Au", F.lit(0.03))
     .otherwise(F.lit(0.03))
)

# -----------------------------
# 4) Final projection
# -----------------------------
df = (
    base.select(
        F.col("ml_created_on"),
        F.col("ml_prod_line"),
        F.col("ml_cell_line"),
        F.col("ml_employee_first_name_thai"),

        F.col("ml_item_no").alias("item_component_no"),  # metal_loss item
        F.col("ml_fg_item_no").alias("item_no"),              # ✅ POC component item

        F.col("ml_prod_order_no"),
        F.col("ml_material_type"),

        total.alias("total_consumption"),
        weight.alias("weight_after_FL"),
        sprue.alias("Scrap"),
        loss_scrap_g.alias("loss_scrap_g"),
        loss_scrap_pct.alias("loss_scrap_pct"),

        F.when(loss_scrap_pct > scrap_std_pct, "OVER")
         .otherwise("UNDER")
         .alias("loss_scrap_std_flag"),

        dust.alias("Dust"),
        loss_after_dust_g.alias("loss_after_dust_g"),
        loss_after_dust_pct.alias("loss_after_dust_pct"),

        F.when(loss_after_dust_pct > F.lit(0.03), "OVER")
         .otherwise("UNDER")
         .alias("loss_after_scrap_std_flag"),

        F.current_timestamp().alias("updated_at")
    )
)

# -----------------------------
# 5) Cast numerics
# -----------------------------
num_cols = [
    "total_consumption", "weight_after_FL", "Scrap",
    "loss_scrap_g", "loss_scrap_pct",
    "Dust", "loss_after_dust_g", "loss_after_dust_pct"
]
for c in num_cols:
    df = df.withColumn(c, F.col(c).cast("decimal(38,10)"))

# -----------------------------
# 6) FULL REPLACE WRITE
# -----------------------------
(
    df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(TARGET_TABLE)
)

print(f"✅ FULL REPLACE complete: {TARGET_TABLE}")
print("Rows written:", df.count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# # Requirement Casting Metal

# MARKDOWN ********************

# ## Gold Casting Summary + Gold Bom Sub Bom 2 Summary 

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Inventory_Lakehouse.inv.gold_requirement_casting_metal
# MAGIC AS
# MAGIC /* ================================================================
# MAGIC    Gold Requirement Casting Metal – Spark SQL Table
# MAGIC    Casting Summary → BOM Summary → Item (Metal Category Code)
# MAGIC    ================================================================ */
# MAGIC 
# MAGIC SELECT
# MAGIC     C.prod_order_no,
# MAGIC     C.prod_order_line_no,
# MAGIC     C.FG_item_no,
# MAGIC     C.prod_item_line,
# MAGIC     C.item_location,
# MAGIC     C.fg_start_date,
# MAGIC     C.fg_due_date,
# MAGIC     C.casting_start_date,
# MAGIC     C.casting_due_date,
# MAGIC     C.prod_order_status,
# MAGIC     C.current_location_code,
# MAGIC     C.CorrectCurrentLocation,
# MAGIC     C.machine_center_no,
# MAGIC     C.team,
# MAGIC     C.casting_prod_order,
# MAGIC     C.tree_no,
# MAGIC     C.itemCST,
# MAGIC     C.casting_qty_to_tree,
# MAGIC     C.casting_qty_passed,
# MAGIC     C.casting_qty_reject,
# MAGIC     C.casting_status,
# MAGIC     C.CustomerNo,
# MAGIC     C.CustomerAbbr,
# MAGIC     C.metal_category,
# MAGIC     C.new_status,
# MAGIC     C.so_abbr,
# MAGIC     C.new_qty,
# MAGIC     C.total_qty,
# MAGIC     C.in_wh,
# MAGIC     C.remaining_qty,
# MAGIC     C.poi,
# MAGIC 
# MAGIC     B.BOM,
# MAGIC     B.total_Qty                                   AS B_total_Qty,
# MAGIC 
# MAGIC     I.`Metal Category Code`                       AS metal_category_code,
# MAGIC     CASE
# MAGIC       WHEN I.`Metal Category Code` IN (
# MAGIC         '9KW','9KY',
# MAGIC         '14KR','14KW','14KY',
# MAGIC         '18KR','18KW','18KY'
# MAGIC       ) THEN 'GOLD'
# MAGIC       WHEN I.`Metal Category Code` = 'SILVER 925' THEN 'SILVER'
# MAGIC       WHEN I.`Metal Category Code` = 'BRASS'      THEN 'BRASS'
# MAGIC       WHEN i.`Metal Category Code` = 'PLATINUM' THEN 'PLATINUM'
# MAGIC       WHEN i.`Metal Category Code` = 'COPPER' THEN 'COPPER'
# MAGIC       ELSE i.`Metal Category Code`
# MAGIC     END AS `TYPE`,
# MAGIC 
# MAGIC     C.new_qty * B.total_Qty                       AS req_metal
# MAGIC 
# MAGIC FROM `Gold_Production_Lakehouse`.`prod`.`gold_casting_summary` C
# MAGIC LEFT JOIN `Gold_Inventory_Lakehouse`.`inv`.`gold_bom_subbom2_summary` B
# MAGIC   ON C.prod_item_line = B.FG
# MAGIC INNER JOIN `Silver_BC_Lakehouse`.`bc`.`Item` I
# MAGIC   ON B.BOM = I.`No.`
# MAGIC ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Consumption

# CELL ********************

# MAGIC %%sql
# MAGIC -- ============================================================
# MAGIC -- gold_consumption v2 — Active PROs only + drift severity flags
# MAGIC -- Owner: Phloy / DPA team
# MAGIC --
# MAGIC -- Changes from v1 (PRO-version-aware):
# MAGIC --   1. กรอง Status = 'Finished' ออก (ไม่กระทบ ops, ทำให้ NO_BOM_MATCH 25% noise หายไป)
# MAGIC --   2. เพิ่ม drift_level (L1 / L2 / L3 / NONE) สำหรับ rule-based alerting
# MAGIC --   3. เพิ่ม is_casting_pro flag เผื่อใช้ filter ใน downstream
# MAGIC --
# MAGIC -- Drift levels:
# MAGIC --   L1_DRIFT       — PCS drift > 0.5 (PC ค่าเพี้ยน, refresh ได้)
# MAGIC --   L2_PC_ZEROED   — PC = 0 ทั้งที่ BOM > 0 (อันตราย: warehouse ไม่จ่ายของ)
# MAGIC --   L3_BOM_ZEROED  — BOM = 0 ทั้งที่ PC > 0 (BOM data quality issue)
# MAGIC --   NONE           — ไม่มี drift หรือ BOM ไม่ match
# MAGIC --
# MAGIC -- วิธีรันใน Fabric notebook:
# MAGIC --   1. แต่ละ CELL ด้านล่าง = Spark SQL cell แยกกัน
# MAGIC --   2. ห้ามใส่ %%sql ใน cell
# MAGIC -- ============================================================
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CELL A — Spark config
# MAGIC -- ============================================================
# MAGIC SET spark.microsoft.delta.optimizeWrite.enabled = false;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CELL B — Main query
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TABLE `Gold_Inventory_Lakehouse`.`inv`.`gold_consumption`
# MAGIC AS
# MAGIC WITH
# MAGIC     ILE AS (
# MAGIC         SELECT
# MAGIC             `Document No.`     AS ProdOrderNo,
# MAGIC             `Order Line No.`   AS OrderLineNo,
# MAGIC             `Source No.`       AS SourceNo,
# MAGIC             `Item No.`         AS ComponentItemNo,
# MAGIC             SUM(`Quantity`)        AS con1,
# MAGIC             SUM(`Units_DU_TSL`)    AS con2
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry`
# MAGIC         WHERE `Entry Type` = 'Consumption'
# MAGIC         GROUP BY
# MAGIC             `Document No.`,
# MAGIC             `Order Line No.`,
# MAGIC             `Source No.`,
# MAGIC             `Item No.`
# MAGIC     ),
# MAGIC 
# MAGIC     BOM_LINES AS (
# MAGIC         SELECT *
# MAGIC         FROM (
# MAGIC             SELECT
# MAGIC                 pbl.`Production BOM No.`           AS production_bom_no,
# MAGIC                 COALESCE(pbl.`Version Code`, '')   AS version_code,
# MAGIC                 pbl.`No.`                          AS component_item_no,
# MAGIC                 pbl.`Quantity per`                 AS bom_quantity_per,
# MAGIC                 pbl.`Units per_DU_TSL`             AS bom_units_per,
# MAGIC                 COALESCE(pbl.`Scrap %`, 0)         AS bom_scrap_pct,
# MAGIC                 ROW_NUMBER() OVER (
# MAGIC                     PARTITION BY pbl.`Production BOM No.`,
# MAGIC                                  COALESCE(pbl.`Version Code`, ''),
# MAGIC                                  pbl.`No.`
# MAGIC                     ORDER BY pbl.SystemRowVersion DESC
# MAGIC                 ) AS rn
# MAGIC             FROM Silver_BC_Lakehouse.bc.`Production BOM Line` pbl
# MAGIC             WHERE pbl.`Type` = 'Item'
# MAGIC               AND pbl.`No.` IS NOT NULL
# MAGIC               AND pbl.`No.` <> ''
# MAGIC         )
# MAGIC         WHERE rn = 1
# MAGIC     ),
# MAGIC 
# MAGIC     CTE AS (
# MAGIC         SELECT
# MAGIC             pl.`Status` AS Status,
# MAGIC 
# MAGIC             pl.`Prod. Order No.` AS ProdOrderNo,
# MAGIC             pl.`Line No.` AS ProdOrderLineNo,
# MAGIC             pl.`Item No.` AS ProdOrderItemNo,
# MAGIC             pc.`Line No.` AS ComponentLineNo,
# MAGIC             pc.`Item No.` AS ComponentItemNo,
# MAGIC             pc.`Allow Over Consumption` AS AllowOverConsumption,
# MAGIC             pc.`Description` AS Description,
# MAGIC             pc.`Due Date-Time` AS ComponentDueDateTime,
# MAGIC             pc.`Due Date` AS ComponentDueDate,
# MAGIC             pc.`Quantity per` AS QtyPer,
# MAGIC             pc.`Units per_DU_TSL` AS ComponentUnitsPer_DU_TSL,
# MAGIC 
# MAGIC             -- ===== EXP1 / EXP2: ดึงจาก BOM ตาม version ที่ PRO ระบุ =====
# MAGIC             CAST(
# MAGIC                 pl.`Quantity` * bom.bom_quantity_per * (1 + bom.bom_scrap_pct / 100)
# MAGIC                 AS DECIMAL(18, 5)
# MAGIC             ) AS exp1,
# MAGIC 
# MAGIC             pc.`Remaining Quantity` AS rem1,
# MAGIC             COALESCE(ile.con1, 0) AS con1,
# MAGIC 
# MAGIC             CAST(
# MAGIC                 pl.`Quantity` * bom.bom_units_per * (1 + bom.bom_scrap_pct / 100)
# MAGIC                 AS DECIMAL(18, 5)
# MAGIC             ) AS exp2,
# MAGIC             COALESCE(ile.con2, 0) AS con2,
# MAGIC 
# MAGIC             -- ===== Audit: BOM source =====
# MAGIC             pl.`Production BOM No.`                          AS pl_production_bom_no,
# MAGIC             COALESCE(pl.`Production BOM Version Code`, '')   AS pl_bom_version_code,
# MAGIC             bom.bom_quantity_per                             AS bom_quantity_per,
# MAGIC             bom.bom_units_per                                AS bom_units_per,
# MAGIC             bom.bom_scrap_pct                                AS bom_scrap_pct,
# MAGIC             CASE
# MAGIC                 WHEN bom.production_bom_no IS NULL THEN 'NO_BOM_MATCH'
# MAGIC                 ELSE 'BOM_MATCHED'
# MAGIC             END                                              AS exp_source_status,
# MAGIC 
# MAGIC             -- ===== Audit: ค่าเดิมจาก PRO Component =====
# MAGIC             pc.`Expected Quantity`                           AS exp1_from_pc_original,
# MAGIC             pc.`Expected Units_DU_TSL`                       AS exp2_from_pc_original,
# MAGIC 
# MAGIC             -- ===== NEW: drift_level =====
# MAGIC             -- L3: BOM = 0 แต่ PC > 0  → BOM data quality issue
# MAGIC             -- L2: PC = 0 แต่ BOM > 0  → warehouse ไม่จ่ายของ (อันตรายสุด)
# MAGIC             -- L1: PCS drift > 0.5 (จับเฉพาะ drift จริง ไม่ใช่ rounding)
# MAGIC             -- NONE: ไม่ drift หรือ BOM ไม่ match
# MAGIC             CASE
# MAGIC                 WHEN bom.production_bom_no IS NULL                                 THEN 'NONE'
# MAGIC                 WHEN COALESCE(bom.bom_units_per, 0) = 0
# MAGIC                      AND COALESCE(pc.`Expected Units_DU_TSL`, 0) > 0               THEN 'L3_BOM_ZEROED'
# MAGIC                 WHEN COALESCE(pc.`Expected Units_DU_TSL`, 0) = 0
# MAGIC                      AND COALESCE(bom.bom_units_per, 0) > 0                        THEN 'L2_PC_ZEROED'
# MAGIC                 WHEN ABS(
# MAGIC                         CAST(pl.`Quantity` * bom.bom_units_per * (1 + bom.bom_scrap_pct / 100) AS DECIMAL(18,5))
# MAGIC                         - COALESCE(pc.`Expected Units_DU_TSL`, 0)
# MAGIC                      ) > 0.5                                                        THEN 'L1_DRIFT'
# MAGIC                 ELSE 'NONE'
# MAGIC             END                                              AS drift_level,
# MAGIC 
# MAGIC             -- ===== NEW: is_casting_pro flag =====
# MAGIC             CASE
# MAGIC                 WHEN pl.`Prod. Order No.` LIKE 'CAS%' THEN true
# MAGIC                 ELSE false
# MAGIC             END                                              AS is_casting_pro,
# MAGIC 
# MAGIC             pl.`Quantity` AS ProdLineQty,
# MAGIC             pl.`Finished Quantity` AS ProdLineFinishedQty,
# MAGIC             pl.`Remaining Quantity` AS ProdLineRemainingQty,
# MAGIC 
# MAGIC             pc.`Unit of Measure Code` AS ComponentUOM,
# MAGIC             pc.`Qty. Picked` AS QtyPicked,
# MAGIC             pc.`Completely Picked` AS CompletelyPicked,
# MAGIC 
# MAGIC             pc.`SystemCreatedAt` AS SystemCreatedAt,
# MAGIC             pc.`SystemModifiedAt` AS SystemModifiedAt,
# MAGIC 
# MAGIC             CONCAT(pl.`Prod. Order No.`, pl.`Line No.`)                       AS pol,
# MAGIC             CONCAT(pl.`Prod. Order No.`, pl.`Line No.`, pc.`Item No.`)        AS poli,
# MAGIC 
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY pl.`Prod. Order No.`, pl.`Line No.`, pc.`Line No.`, pc.`Item No.`
# MAGIC                 ORDER BY pc.`SystemCreatedAt` DESC
# MAGIC             ) AS rn,
# MAGIC 
# MAGIC             pl.`Starting Date-Time` AS ProdLineStartingDateTime,
# MAGIC             pl.`Ending Date-Time`   AS ProdLineEndingDateTime,
# MAGIC             pl.`Due Date`           AS ProdLineDueDate,
# MAGIC 
# MAGIC             CAST(
# MAGIC                 MAX(CASE WHEN pl.`Line No.` = 10000 THEN pl.`Starting Date-Time` END)
# MAGIC                 OVER (PARTITION BY pl.`Prod. Order No.`) AS DATE
# MAGIC             ) AS FG_Startdate,
# MAGIC 
# MAGIC             MAX(CASE WHEN pl.`Line No.` = 10000 THEN pl.`Due Date` END)
# MAGIC                 OVER (PARTITION BY pl.`Prod. Order No.`) AS FG_duedate,
# MAGIC 
# MAGIC             CAST(
# MAGIC                 MAX(CASE WHEN pl.`Location Code` = 'CST_CUT' THEN pl.`Starting Date-Time` END)
# MAGIC                 OVER (PARTITION BY pl.`Prod. Order No.`, pl.`Item No.`) AS DATE
# MAGIC             ) AS Casting_Startdate,
# MAGIC 
# MAGIC             MAX(CASE WHEN pl.`Location Code` = 'CST_CUT' THEN pl.`Due Date` END)
# MAGIC                 OVER (PARTITION BY pl.`Prod. Order No.`, pl.`Item No.`) AS Casting_duedate
# MAGIC 
# MAGIC         FROM Silver_BC_Lakehouse.bc.`Prod Order Line` AS pl
# MAGIC         LEFT JOIN Silver_BC_Lakehouse.bc.`Prod Order Component` AS pc
# MAGIC             ON pl.`Prod. Order No.` = pc.`Prod. Order No.`
# MAGIC             AND pl.`Line No.`        = pc.`Prod. Order Line No.`
# MAGIC 
# MAGIC         LEFT JOIN ILE AS ile
# MAGIC             ON  pc.`Prod. Order No.` = ile.ProdOrderNo
# MAGIC             AND pc.`Item No.`        = ile.ComponentItemNo
# MAGIC             AND pl.`Item No.`        = ile.SourceNo
# MAGIC 
# MAGIC         LEFT JOIN BOM_LINES AS bom
# MAGIC             ON  bom.production_bom_no = pl.`Production BOM No.`
# MAGIC             AND bom.version_code      = COALESCE(pl.`Production BOM Version Code`, '')
# MAGIC             AND bom.component_item_no = pc.`Item No.`
# MAGIC 
# MAGIC         -- ============================================
# MAGIC         -- ตัด Finished PROs ออก
# MAGIC         -- (Phloy: PROs ที่ผลิตเสร็จแล้วไม่กระทบ ops)
# MAGIC         -- ============================================
# MAGIC         WHERE pl.`Status` <> 'Finished'
# MAGIC     )
# MAGIC SELECT *
# MAGIC FROM CTE
# MAGIC WHERE rn = 1;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CELL C — Validation: row count + match rate
# MAGIC -- ============================================================
# MAGIC SELECT
# MAGIC     Status,
# MAGIC     exp_source_status,
# MAGIC     drift_level,
# MAGIC     COUNT(*)                           AS row_count,
# MAGIC     COUNT(DISTINCT ProdOrderNo)        AS distinct_pros
# MAGIC FROM `Gold_Inventory_Lakehouse`.`inv`.`gold_consumption`
# MAGIC GROUP BY Status, exp_source_status, drift_level
# MAGIC ORDER BY Status, exp_source_status, drift_level;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CELL D — Drift summary (ใช้แทน WHERE ABS(diff) > 0.5 เดิม)
# MAGIC -- ============================================================
# MAGIC SELECT
# MAGIC     drift_level,
# MAGIC     COUNT(*)                           AS row_count,
# MAGIC     COUNT(DISTINCT ProdOrderNo)        AS distinct_pros,
# MAGIC     COUNT(DISTINCT ComponentItemNo)    AS distinct_components,
# MAGIC     ROUND(SUM(CASE
# MAGIC         WHEN drift_level <> 'NONE' THEN ABS(exp2 - exp2_from_pc_original)
# MAGIC         ELSE 0 END), 2)                AS total_pcs_drift
# MAGIC FROM `Gold_Inventory_Lakehouse`.`inv`.`gold_consumption`
# MAGIC WHERE drift_level <> 'NONE'
# MAGIC GROUP BY drift_level
# MAGIC ORDER BY
# MAGIC     CASE drift_level
# MAGIC         WHEN 'L3_BOM_ZEROED' THEN 1
# MAGIC         WHEN 'L2_PC_ZEROED' THEN 2
# MAGIC         WHEN 'L1_DRIFT'     THEN 3
# MAGIC         ELSE 4
# MAGIC     END;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CELL E — Spot check WRO260303649 / FOS-000010-SLV
# MAGIC -- ============================================================
# MAGIC SELECT
# MAGIC     Status,
# MAGIC     ProdOrderNo,
# MAGIC     ComponentItemNo,
# MAGIC     pl_bom_version_code,
# MAGIC     bom_units_per,
# MAGIC     ComponentUnitsPer_DU_TSL              AS pc_pcs_per,
# MAGIC     exp2                                  AS exp2_from_bom,
# MAGIC     exp2_from_pc_original                 AS exp2_from_pc,
# MAGIC     (exp2 - exp2_from_pc_original)        AS exp2_diff,
# MAGIC     drift_level
# MAGIC FROM `Gold_Inventory_Lakehouse`.`inv`.`gold_consumption`
# MAGIC WHERE ProdOrderNo = 'WRO260303649'
# MAGIC   AND ComponentItemNo = 'FOS-000010-SLV';
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CELL F — Top L1 drift list (สำหรับ Riyad refresh PRO)
# MAGIC -- ============================================================
# MAGIC SELECT
# MAGIC     Status,
# MAGIC     ProdOrderNo,
# MAGIC     ProdOrderItemNo,
# MAGIC     ComponentItemNo,
# MAGIC     pl_bom_version_code,
# MAGIC     ProdLineQty,
# MAGIC     bom_units_per,
# MAGIC     ComponentUnitsPer_DU_TSL              AS pc_pcs_per,
# MAGIC     exp2                                  AS exp2_from_bom,
# MAGIC     exp2_from_pc_original                 AS exp2_from_pc,
# MAGIC     (exp2 - exp2_from_pc_original)        AS exp2_diff,
# MAGIC     CompletelyPicked
# MAGIC FROM `Gold_Inventory_Lakehouse`.`inv`.`gold_consumption`
# MAGIC WHERE drift_level = 'L1_DRIFT'
# MAGIC   AND NOT is_casting_pro
# MAGIC ORDER BY ABS(exp2 - exp2_from_pc_original) DESC
# MAGIC LIMIT 100;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CELL G — L2_PC_ZEROED list (อันตราย: warehouse ไม่จ่ายของ)
# MAGIC -- ============================================================
# MAGIC SELECT
# MAGIC     Status,
# MAGIC     ProdOrderNo,
# MAGIC     ProdOrderItemNo,
# MAGIC     ComponentItemNo,
# MAGIC     pl_bom_version_code,
# MAGIC     ProdLineQty,
# MAGIC     bom_units_per,
# MAGIC     ComponentUnitsPer_DU_TSL              AS pc_pcs_per,
# MAGIC     exp2                                  AS exp2_from_bom,
# MAGIC     exp2_from_pc_original                 AS exp2_from_pc,
# MAGIC     CompletelyPicked
# MAGIC FROM `Gold_Inventory_Lakehouse`.`inv`.`gold_consumption`
# MAGIC WHERE drift_level = 'L2_PC_ZEROED'
# MAGIC   AND NOT is_casting_pro
# MAGIC ORDER BY exp2 DESC
# MAGIC LIMIT 100;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- CELL H — L3_BOM_ZEROED list (BOM data quality issue)
# MAGIC -- ============================================================
# MAGIC SELECT
# MAGIC     Status,
# MAGIC     ProdOrderNo,
# MAGIC     ProdOrderItemNo,
# MAGIC     ComponentItemNo,
# MAGIC     pl_production_bom_no,
# MAGIC     pl_bom_version_code,
# MAGIC     bom_units_per                         AS bom_units_per_zero,
# MAGIC     ComponentUnitsPer_DU_TSL              AS pc_pcs_per
# MAGIC FROM `Gold_Inventory_Lakehouse`.`inv`.`gold_consumption`
# MAGIC WHERE drift_level = 'L3_BOM_ZEROED'
# MAGIC   AND NOT is_casting_pro
# MAGIC ORDER BY ProdOrderNo
# MAGIC LIMIT 100;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Consumption Status

# CELL ********************

# MAGIC %%sql
# MAGIC /* gold_consumption_status -- แก้ UOM2 PRIORITY เท่านั้น, ไม่เพิ่มคอลัมน์ */
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE `Gold_Inventory_Lakehouse`.`inv`.`gold_consumption_status`
# MAGIC AS
# MAGIC SELECT
# MAGIC     U.*,
# MAGIC 
# MAGIC     -- Consumption คงเหลือ
# MAGIC     CAST(U.exp1 + COALESCE(U.con1, 0) AS decimal(18,6)) AS Consump1,
# MAGIC     CAST(U.exp2 + COALESCE(U.con2, 0) AS decimal(18,6)) AS Consump2,
# MAGIC 
# MAGIC     CASE
# MAGIC         /* Allow Over Consumption */
# MAGIC         WHEN U.AllowOverConsumption = 1
# MAGIC              AND (
# MAGIC                     (COALESCE(U.exp2, 0) <> 0 AND (U.exp2 + COALESCE(U.con2, 0)) < -0.0001)
# MAGIC                  OR (COALESCE(U.exp2, 0) = 0  AND (U.exp1 + COALESCE(U.con1, 0)) < -0.0001)
# MAGIC              )
# MAGIC             THEN 'COMPLETED OVER CONSUMP'
# MAGIC 
# MAGIC         /* ⭐ PRIORITY 1: UOM2 FIRST */
# MAGIC         WHEN COALESCE(U.exp2, 0) <> 0 THEN
# MAGIC             CASE
# MAGIC                 WHEN ABS(U.exp2 + COALESCE(U.con2, 0)) <= 0.0001
# MAGIC                     THEN 'COMPLETED'
# MAGIC                 ELSE 'NOT COMPLETED'
# MAGIC             END
# MAGIC 
# MAGIC         /* ⭐ PRIORITY 2: UOM1 (fallback) */
# MAGIC         WHEN COALESCE(U.exp1, 0) <> 0 THEN
# MAGIC             CASE
# MAGIC                 WHEN ABS(U.exp1 + COALESCE(U.con1, 0)) <= 0.0001
# MAGIC                     THEN 'COMPLETED'
# MAGIC                 ELSE 'NOT COMPLETED'
# MAGIC             END
# MAGIC 
# MAGIC         ELSE 'NOT COMPLETED'
# MAGIC     END AS Consump_Status
# MAGIC 
# MAGIC FROM `Gold_Inventory_Lakehouse`.`inv`.`gold_consumption` AS U
# MAGIC 
# MAGIC WHERE NOT (
# MAGIC        U.ComponentItemNo  LIKE 'M-%'
# MAGIC     OR U.ComponentItemNo  LIKE 'PS-%'
# MAGIC     OR U.ComponentItemNo  LIKE 'PL-%'
# MAGIC     OR U.ProdOrderItemNo  LIKE 'M-%'
# MAGIC     OR U.ComponentItemNo  LIKE 'RM-%'
# MAGIC     OR U.ComponentItemNo  LIKE 'WA-%'
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Consumption Status Summary

# CELL ********************

# ============================================================
# FULL REFRESH: Consump_Status_Summary (CROSS APPLY version -> Spark window)
# Source: "Gold_Inventory_Lakehouse.inv.gold_consumption_status
# Note: SQL Server CROSS APPLY replaced by window aggregates over ProdOrderNo.
#
# Target table name (edit if you want):
#   Gold_Inventory_Lakehouse.inv.gold_consumption_status_summary
# ============================================================

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT_TBL = "Gold_Inventory_Lakehouse.inv.gold_consumption_status_summary"

src_sql = """
WITH base AS (
    SELECT
        L.`Status`,
        L.ProdOrderNo,
        L.ProdOrderLineNo,
        L.ProdOrderItemNo,
        L.ProdLineStartingDateTime,
        L.ProdLineEndingDateTime,
        L.ProdLineDueDate,
        L.FG_Startdate,
        L.FG_duedate,
        L.Casting_Startdate,
        L.Casting_duedate,
        L.Consump_Status,

        -- counts per ProdOrderNo (replacement for CROSS APPLY)
        SUM(CASE WHEN L.Consump_Status = 'COMPLETED' THEN 1 ELSE 0 END)
            OVER (PARTITION BY L.ProdOrderNo) AS CompletedCnt,

        SUM(CASE WHEN L.Consump_Status = 'NOT COMPLETED' THEN 1 ELSE 0 END)
            OVER (PARTITION BY L.ProdOrderNo) AS NotCompletedCnt,

        ROW_NUMBER() OVER (
            PARTITION BY L.ProdOrderNo
            ORDER BY L.ProdOrderItemNo
        ) AS rn_pick
    FROM Gold_Inventory_Lakehouse.inv.gold_consumption_status L
),
final AS (
    SELECT
        `Status`,
        ProdOrderNo,
        ProdOrderLineNo,
        ProdOrderItemNo,
        ProdLineStartingDateTime,
        ProdLineEndingDateTime,
        ProdLineDueDate,
        FG_Startdate,
        FG_duedate,
        Casting_Startdate,
        Casting_duedate,
        Consump_Status,
        CASE
            WHEN CompletedCnt > 0 AND NotCompletedCnt > 0 THEN 'PARTIAL'
            WHEN CompletedCnt > 0 AND NotCompletedCnt = 0 THEN 'COMPLETED'
            ELSE 'NOT COMPLETED'
        END AS Consump_Status_Summary,
        rn_pick
    FROM base
)
SELECT
    `Status`,
    ProdOrderNo,
    ProdOrderLineNo,
    ProdOrderItemNo,
    ProdLineStartingDateTime,
    ProdLineEndingDateTime,
    ProdLineDueDate,
    FG_Startdate,
    FG_duedate,
    Casting_Startdate,
    Casting_duedate,
    Consump_Status,
    Consump_Status_Summary
FROM final
WHERE rn_pick = 1
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

# CELL ********************

# ============================================================
# FULL REFRESH: Consump_Status_Summary (filtered ProdOrderNo version)
# Source: Silver_BC_Lakehouse.dbo.vProdCom_Ledger
# Logic:
#   - Aggregate Completed/NotCompleted per ProdOrderNo (CTE S)
#   - Join back + pick rn_pick = 1 (1 row per ProdOrderNo)
#
# Target table name (edit if you want):
#   Gold_Inventory_Lakehouse.inv.gold_consumption_status_summary2
# ============================================================

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT_TBL = "Gold_Inventory_Lakehouse.inv.gold_consumption_status_summary2"

src_sql = """
WITH S AS (
    SELECT
        ProdOrderNo,
        SUM(CASE WHEN Consump_Status = 'COMPLETED' THEN 1 ELSE 0 END) AS CompletedCnt,
        SUM(CASE WHEN Consump_Status = 'NOT COMPLETED' THEN 1 ELSE 0 END) AS NotCompletedCnt
    FROM Gold_Inventory_Lakehouse.inv.gold_consumption_status
    WHERE ProdOrderNo = 'WRO250801219'
    GROUP BY ProdOrderNo
),
T AS (
    SELECT
        L.`Status`,
        L.ProdOrderNo,
        L.ProdOrderLineNo,
        L.ProdOrderItemNo,
        L.ProdLineStartingDateTime,
        L.ProdLineEndingDateTime,
        L.ProdLineDueDate,
        L.FG_Startdate,
        L.FG_duedate,
        L.Casting_Startdate,
        L.Casting_duedate,
        L.Consump_Status,

        CASE
            WHEN S.CompletedCnt > 0 AND S.NotCompletedCnt > 0 THEN 'PARTIAL'
            WHEN S.CompletedCnt > 0 AND S.NotCompletedCnt = 0 THEN 'COMPLETED'
            ELSE 'NOT COMPLETED'
        END AS Consump_Status_Summary,

        ROW_NUMBER() OVER (
            PARTITION BY L.ProdOrderNo
            ORDER BY L.ProdOrderItemNo
        ) AS rn_pick
    FROM Gold_Inventory_Lakehouse.inv.gold_consumption_status L
    LEFT JOIN S
        ON S.ProdOrderNo = L.ProdOrderNo
    WHERE L.ProdOrderNo = 'WRO250801219'
)
SELECT *
FROM T
WHERE rn_pick = 1
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
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # Gold Consumption Stock Item

# CELL ********************

# MAGIC %%sql
# MAGIC -- Full reload (Spark SQL)
# MAGIC -- Joins: gold_consumption.ComponentItemNo = gold_stock_item.item_no
# MAGIC -- Output: a new table containing ALL columns from both tables
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE `Gold_Inventory_Lakehouse`.`inv`.`gold_consumption_stock_item`
# MAGIC AS
# MAGIC SELECT
# MAGIC   c.*,
# MAGIC   s.*
# MAGIC FROM `Gold_Inventory_Lakehouse`.`inv`.`gold_consumption` c
# MAGIC LEFT JOIN `Gold_Inventory_Lakehouse`.`inv`.`gold_stock_item` s
# MAGIC   ON c.`ComponentItemNo` = s.`item_no`;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- =============================================================================
# MAGIC -- DEBUG SCRIPT: FOG-000425-18KR
# MAGIC -- Run each block separately to see what's happening at each stage.
# MAGIC -- =============================================================================
# MAGIC 
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC -- STEP 0: Does this item even exist in the Item master? Is it blocked?
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC SELECT
# MAGIC     `No.`,
# MAGIC     `Description`,
# MAGIC     `Blocked`,
# MAGIC     `Replenishment System`,
# MAGIC     `Reordering Policy`,
# MAGIC     `Reorder Point`,
# MAGIC     `Reorder Quantity`,
# MAGIC     `Maximum Inventory`,
# MAGIC     `Safety Stock Quantity`,
# MAGIC     `Vendor No.`,
# MAGIC     `Unit of Measure - Units_DU_TSL`
# MAGIC FROM Silver_BC_Lakehouse.bc.`Item`
# MAGIC WHERE `No.` = 'FOG-000425-18KR';
# MAGIC 
# MAGIC 
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC -- STEP 1: Raw Item Ledger Entries (no filters at all)
# MAGIC -- Shows every entry for this item — gives you the full history
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC SELECT
# MAGIC     `Entry No.`,
# MAGIC     `Item No.`,
# MAGIC     `Location Code`,
# MAGIC     `Entry Type`,
# MAGIC     `Posting Date`,
# MAGIC     `Quantity`,
# MAGIC     `Remaining Quantity`,
# MAGIC     `Units_DU_TSL`,
# MAGIC     `Open`,
# MAGIC     `Unit of Measure Code`,
# MAGIC     `Document No.`,
# MAGIC     `Description`
# MAGIC FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry`
# MAGIC WHERE `Item No.` = 'FOG-000425-18KR'
# MAGIC ORDER BY `Posting Date`, `Entry No.`;
# MAGIC 
# MAGIC 
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC -- STEP 2: Item Ledger filtered to allowed locations (matches filtered_ledger CTE)
# MAGIC -- If you don't see rows here that you saw in Step 1, the location was excluded
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC SELECT
# MAGIC     `Entry No.`,
# MAGIC     `Item No.`,
# MAGIC     `Location Code`,
# MAGIC     `Entry Type`,
# MAGIC     `Quantity`,
# MAGIC     `Remaining Quantity`,
# MAGIC     `Units_DU_TSL`,
# MAGIC     `Open`
# MAGIC FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry`
# MAGIC WHERE `Item No.` = 'FOG-000425-18KR'
# MAGIC   AND `Location Code` IN (
# MAGIC         'BAGGING','CASTING','CONSUME','CST_CUT','CST_ROOM','CZ-SYNT',
# MAGIC         'DEBEERS','DIA-LAB','DIA-NAT','EQUIP','FG-NO-PO','FINDINGS',
# MAGIC         'FIN-GOODS','GEMS','KIMAI','MATERIAL','OBSOLETE','OTHERS-MAT',
# MAGIC         'PACKAGING','PEARLS','PLATING','POMELATO','PRE ALLOY','RETURNS',
# MAGIC         'RUB MOLD','SEMI-F','SORTING','STONE-CUT','STR','TOOLS','WAX ROOM'
# MAGIC   )
# MAGIC ORDER BY `Location Code`, `Posting Date`, `Entry No.`;
# MAGIC 
# MAGIC 
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC -- STEP 3: The aggregation that becomes rem_uom1 / rem_uom2 in the final SELECT
# MAGIC -- This is the KEY debug — shows what the final query will compute per location
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC SELECT
# MAGIC     `Item No.`                                              AS item_no,
# MAGIC     `Location Code`                                         AS location,
# MAGIC 
# MAGIC     COUNT(*)                                                AS entry_count,
# MAGIC     SUM(CASE WHEN `Open` = 1 THEN 1 ELSE 0 END)             AS open_entries,
# MAGIC     SUM(CASE WHEN `Open` = 0 THEN 1 ELSE 0 END)             AS closed_entries,
# MAGIC 
# MAGIC     -- UOM1 (what becomes rem_uom1)
# MAGIC     SUM(CAST(`Remaining Quantity` AS DECIMAL(38,10)))       AS rem_uom1_total,
# MAGIC     SUM(CASE WHEN `Open` = 1
# MAGIC              THEN CAST(`Remaining Quantity` AS DECIMAL(38,10))
# MAGIC              ELSE 0 END)                                    AS rem_uom1_open_only,
# MAGIC 
# MAGIC     -- UOM2 (what becomes rem_uom2) — includes negative consumption entries
# MAGIC     SUM(CAST(`Units_DU_TSL` AS DECIMAL(38,10)))             AS rem_uom2_total,
# MAGIC     SUM(CASE WHEN `Open` = 1
# MAGIC              THEN CAST(`Units_DU_TSL` AS DECIMAL(38,10))
# MAGIC              ELSE 0 END)                                    AS rem_uom2_open_only,
# MAGIC     SUM(CASE WHEN `Units_DU_TSL` > 0
# MAGIC              THEN CAST(`Units_DU_TSL` AS DECIMAL(38,10))
# MAGIC              ELSE 0 END)                                    AS rem_uom2_positive,
# MAGIC     SUM(CASE WHEN `Units_DU_TSL` < 0
# MAGIC              THEN CAST(`Units_DU_TSL` AS DECIMAL(38,10))
# MAGIC              ELSE 0 END)                                    AS rem_uom2_negative
# MAGIC FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry`
# MAGIC WHERE `Item No.` = 'FOG-000425-18KR'
# MAGIC   AND `Location Code` IN (
# MAGIC         'BAGGING','CASTING','CONSUME','CST_CUT','CST_ROOM','CZ-SYNT',
# MAGIC         'DEBEERS','DIA-LAB','DIA-NAT','EQUIP','FG-NO-PO','FINDINGS',
# MAGIC         'FIN-GOODS','GEMS','KIMAI','MATERIAL','OBSOLETE','OTHERS-MAT',
# MAGIC         'PACKAGING','PEARLS','PLATING','POMELATO','PRE ALLOY','RETURNS',
# MAGIC         'RUB MOLD','SEMI-F','SORTING','STONE-CUT','STR','TOOLS','WAX ROOM'
# MAGIC   )
# MAGIC GROUP BY `Item No.`, `Location Code`
# MAGIC ORDER BY `Location Code`;
# MAGIC 
# MAGIC 
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC -- STEP 4: Open Purchase Lines for this item
# MAGIC -- This is what feeds the qty_to_receive / qty_to_receive_uom2 columns
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC SELECT
# MAGIC     `Document No.`,
# MAGIC     `No.`                       AS item_no,
# MAGIC     `Location Code`,
# MAGIC     `Quantity`,
# MAGIC     `Outstanding Quantity`,
# MAGIC     `Order Quantity_DU_TSL`,
# MAGIC     `Outstanding Units_DU_TSL`,
# MAGIC     `Order Date`,
# MAGIC     `Requested Receipt Date`,
# MAGIC     `Promised Receipt Date`,
# MAGIC     `Buy-from Vendor No.`
# MAGIC FROM Silver_BC_Lakehouse.bc.`Purchase Line`
# MAGIC WHERE `No.` = 'FOG-000425-18KR'
# MAGIC   AND `Outstanding Quantity` > 0
# MAGIC ORDER BY `Location Code`, `Document No.`;
# MAGIC 
# MAGIC 
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC -- STEP 5: Purchase Line aggregation per location (matches purchase_line CTE)
# MAGIC -- Shows exactly how qty_to_receive_uom2 hybrid logic resolves
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC SELECT
# MAGIC     `No.`                                                   AS item_no,
# MAGIC     `Location Code`                                         AS location,
# MAGIC 
# MAGIC     SUM(CAST(`Outstanding Quantity` AS DECIMAL(38,10)))     AS qty_to_receive,
# MAGIC 
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN COALESCE(CAST(`Outstanding Units_DU_TSL` AS DECIMAL(38,10)), 0) > 0
# MAGIC                 THEN CAST(`Outstanding Units_DU_TSL` AS DECIMAL(38,10))
# MAGIC             WHEN COALESCE(CAST(`Outstanding Quantity` AS DECIMAL(38,10)), 0) > 0
# MAGIC                  AND COALESCE(CAST(`Quantity` AS DECIMAL(38,10)), 0) <> 0
# MAGIC                 THEN COALESCE(CAST(`Order Quantity_DU_TSL` AS DECIMAL(38,10)), 0)
# MAGIC                      * (CAST(`Outstanding Quantity` AS DECIMAL(38,10))
# MAGIC                         / CAST(`Quantity` AS DECIMAL(38,10)))
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     )                                                       AS qty_to_receive_uom2,
# MAGIC 
# MAGIC     -- diagnostic: did we use the direct value or the proportional fallback?
# MAGIC     SUM(CASE
# MAGIC             WHEN COALESCE(CAST(`Outstanding Units_DU_TSL` AS DECIMAL(38,10)), 0) > 0
# MAGIC             THEN 1 ELSE 0
# MAGIC         END)                                                AS lines_using_direct,
# MAGIC     SUM(CASE
# MAGIC             WHEN COALESCE(CAST(`Outstanding Units_DU_TSL` AS DECIMAL(38,10)), 0) <= 0
# MAGIC             THEN 1 ELSE 0
# MAGIC         END)                                                AS lines_using_proportional
# MAGIC FROM Silver_BC_Lakehouse.bc.`Purchase Line`
# MAGIC WHERE `No.` = 'FOG-000425-18KR'
# MAGIC   AND `Outstanding Quantity` > 0
# MAGIC GROUP BY `No.`, `Location Code`;
# MAGIC 
# MAGIC 
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC -- STEP 6: Metal dim — is this item classified?
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC SELECT *
# MAGIC FROM Silver_Inventory_Lakehouse.inv.silver_metal
# MAGIC WHERE `Item` = 'FOG-000425-18KR';
# MAGIC 
# MAGIC 
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC -- STEP 7: FINAL — run the full query scoped to this item only
# MAGIC -- This is what should land in gold_stock_item for FOG-000425-18KR
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC WITH purchase_line AS (
# MAGIC     SELECT
# MAGIC         pl.`No.` AS item_no,
# MAGIC         pl.`Location Code` AS location,
# MAGIC         SUM(CAST(pl.`Outstanding Quantity` AS DECIMAL(38,10))) AS qty_to_receive,
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN COALESCE(CAST(pl.`Outstanding Units_DU_TSL` AS DECIMAL(38,10)), 0) > 0
# MAGIC                     THEN CAST(pl.`Outstanding Units_DU_TSL` AS DECIMAL(38,10))
# MAGIC                 WHEN COALESCE(CAST(pl.`Outstanding Quantity` AS DECIMAL(38,10)), 0) > 0
# MAGIC                      AND COALESCE(CAST(pl.`Quantity` AS DECIMAL(38,10)), 0) <> 0
# MAGIC                     THEN COALESCE(CAST(pl.`Order Quantity_DU_TSL` AS DECIMAL(38,10)), 0)
# MAGIC                          * (CAST(pl.`Outstanding Quantity` AS DECIMAL(38,10))
# MAGIC                             / CAST(pl.`Quantity` AS DECIMAL(38,10)))
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS qty_to_receive_uom2,
# MAGIC         MAX(pl.`Requested Receipt Date`) AS requested_receipt_date,
# MAGIC         MAX(pl.`Promised Receipt Date`)  AS promised_receipt_date,
# MAGIC         MAX(pl.`Order Date`)             AS order_date
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Purchase Line` pl
# MAGIC     WHERE pl.`Outstanding Quantity` > 0
# MAGIC       AND pl.`No.` = 'FOG-000425-18KR'
# MAGIC     GROUP BY pl.`No.`, pl.`Location Code`
# MAGIC ),
# MAGIC filtered_ledger AS (
# MAGIC     SELECT
# MAGIC         `Item No.`             AS item_no,
# MAGIC         `Description`          AS item_description,
# MAGIC         `Location Code`        AS entry_type_item_location,
# MAGIC         `Unit of Measure Code` AS item_uom,
# MAGIC         `Remaining Quantity`   AS item_lot_remaining_quantity,
# MAGIC         `Units_DU_TSL`         AS item_lot_remaining_uom2,
# MAGIC         `Quantity`             AS entry_type_item_quantity
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry`
# MAGIC     WHERE `Item No.` = 'FOG-000425-18KR'
# MAGIC       AND `Location Code` IN (
# MAGIC             'BAGGING','CASTING','CONSUME','CST_CUT','CST_ROOM','CZ-SYNT',
# MAGIC             'DEBEERS','DIA-LAB','DIA-NAT','EQUIP','FG-NO-PO','FINDINGS',
# MAGIC             'FIN-GOODS','GEMS','KIMAI','MATERIAL','OBSOLETE','OTHERS-MAT',
# MAGIC             'PACKAGING','PEARLS','PLATING','POMELATO','PRE ALLOY','RETURNS',
# MAGIC             'RUB MOLD','SEMI-F','SORTING','STONE-CUT','STR','TOOLS','WAX ROOM'
# MAGIC     )
# MAGIC ),
# MAGIC item_dim AS (
# MAGIC     SELECT
# MAGIC         `No.` AS item_no,
# MAGIC         `Unit of Measure - Units_DU_TSL` AS uom2,
# MAGIC         `Safety Stock Quantity` AS item_safety_stock_quantity,
# MAGIC         `Replenishment System`,
# MAGIC         `Vendor No.` AS vendor_no,
# MAGIC         `Reordering Policy`,
# MAGIC         `Reorder Point`,
# MAGIC         `Reorder Quantity`,
# MAGIC         `Maximum Inventory`
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item`
# MAGIC     WHERE `No.` = 'FOG-000425-18KR'
# MAGIC       AND (
# MAGIC             try_cast(`Blocked` AS INT) = 0
# MAGIC             OR lower(trim(CAST(`Blocked` AS STRING))) IN ('no','false','0','')
# MAGIC             OR `Blocked` IS NULL
# MAGIC       )
# MAGIC )
# MAGIC SELECT
# MAGIC     f.item_no,
# MAGIC     f.entry_type_item_location AS location,
# MAGIC     SUM(CAST(f.item_lot_remaining_quantity AS DECIMAL(38,10))) AS rem_uom1,
# MAGIC     SUM(CAST(f.item_lot_remaining_uom2     AS DECIMAL(38,10))) AS rem_uom2,
# MAGIC     MAX(CAST(p.qty_to_receive      AS DECIMAL(38,10)))         AS qty_to_receive,
# MAGIC     MAX(CAST(p.qty_to_receive_uom2 AS DECIMAL(38,10)))         AS qty_to_receive_uom2,
# MAGIC     MAX(i.`Reordering Policy`)                                 AS reordering_policy,
# MAGIC     MAX(i.`Reorder Point`)                                     AS reorder_point,
# MAGIC     MAX(i.`Reorder Quantity`)                                  AS reorder_quantity,
# MAGIC     MAX(i.`Maximum Inventory`)                                 AS maximum_inventory,
# MAGIC     MAX(i.item_safety_stock_quantity)                          AS safety_stock,
# MAGIC     -- did the HAVING clause survive?
# MAGIC     CASE
# MAGIC         WHEN SUM(CAST(f.item_lot_remaining_quantity AS DECIMAL(38,10))) <> 0 THEN 'pass uom1<>0'
# MAGIC         WHEN SUM(CAST(f.item_lot_remaining_uom2     AS DECIMAL(38,10))) > 0.001 THEN 'pass uom2>0.001'
# MAGIC         ELSE 'FAIL - row would be filtered out'
# MAGIC     END                                                        AS having_check
# MAGIC FROM filtered_ledger f
# MAGIC LEFT JOIN item_dim i      ON f.item_no = i.item_no
# MAGIC LEFT JOIN purchase_line p ON f.item_no = p.item_no
# MAGIC                          AND f.entry_type_item_location = p.location
# MAGIC WHERE i.item_no IS NOT NULL
# MAGIC GROUP BY f.item_no, f.entry_type_item_location
# MAGIC ORDER BY f.entry_type_item_location;

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
# MAGIC -- 🔍 VERIFY exp2_from_pc_original — Pre-flight check before v3.3 patch
# MAGIC -- =====================================================================
# MAGIC -- Purpose: ตรวจสอบว่า exp2_from_pc_original ใน gold_consumption_status
# MAGIC --          มีค่าครบและ != exp2 จริง สำหรับเคส drift
# MAGIC -- =====================================================================
# MAGIC 
# MAGIC 
# MAGIC -- ─────────────────────────────────────────────────────────────────────
# MAGIC -- 🟢 Q1: Overall coverage of exp2_from_pc_original
# MAGIC -- ─────────────────────────────────────────────────────────────────────
# MAGIC SELECT
# MAGIC     COUNT(*)                                                         AS total_rows,
# MAGIC     SUM(CASE WHEN exp2 IS NOT NULL THEN 1 ELSE 0 END)               AS has_exp2,
# MAGIC     SUM(CASE WHEN exp2_from_pc_original IS NOT NULL THEN 1 ELSE 0 END) AS has_original,
# MAGIC     SUM(CASE WHEN exp2_from_pc_original > 0 THEN 1 ELSE 0 END)      AS has_positive_original,
# MAGIC     SUM(CASE WHEN exp2 <> exp2_from_pc_original THEN 1 ELSE 0 END)  AS exp2_differs_from_original,
# MAGIC     ROUND(
# MAGIC         100.0 * SUM(CASE WHEN exp2_from_pc_original IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*),
# MAGIC         2
# MAGIC     )                                                                AS pct_original_filled,
# MAGIC     ROUND(
# MAGIC         100.0 * SUM(CASE WHEN exp2 <> exp2_from_pc_original THEN 1 ELSE 0 END) / COUNT(*),
# MAGIC         2
# MAGIC     )                                                                AS pct_drift
# MAGIC FROM Gold_Inventory_Lakehouse.inv.gold_consumption_status;
# MAGIC 
# MAGIC 
# MAGIC -- ─────────────────────────────────────────────────────────────────────
# MAGIC -- 🟢 Q2: Canary check — WRO260305106 / FCG-000265-18KR
# MAGIC -- ─────────────────────────────────────────────────────────────────────
# MAGIC -- คาดหวัง:
# MAGIC --   exp2                  = 41.60 (BOM master)
# MAGIC --   exp2_from_pc_original = 45.00 (PRO original) ← ต้องการตัวนี้
# MAGIC --   con2                  = -42.80
# MAGIC --   Consump2 (current)    = -1.20
# MAGIC --   Consump2 (ใหม่ใช้ original) = 45 - 42.80 = 2.20 ✅
# MAGIC SELECT
# MAGIC     ProdOrderNo,
# MAGIC     ProdOrderLineNo,
# MAGIC     ComponentItemNo,
# MAGIC     exp1,
# MAGIC     exp2,
# MAGIC     exp2_from_pc_original,
# MAGIC     con1,
# MAGIC     con2,
# MAGIC     Consump1,
# MAGIC     Consump2                                                         AS consump2_current,
# MAGIC     -- Calculate what Consump2 WOULD BE with original
# MAGIC     (COALESCE(exp2_from_pc_original, exp2) + COALESCE(con2, 0))      AS consump2_if_original,
# MAGIC     drift_level,
# MAGIC     Consump_Status
# MAGIC FROM Gold_Inventory_Lakehouse.inv.gold_consumption_status
# MAGIC WHERE ProdOrderNo = 'WRO260305106'
# MAGIC   AND ComponentItemNo = 'FCG-000265-18KR';
# MAGIC 
# MAGIC 
# MAGIC -- ─────────────────────────────────────────────────────────────────────
# MAGIC -- 🟢 Q3: Drift level distribution + which level has original
# MAGIC -- ─────────────────────────────────────────────────────────────────────
# MAGIC SELECT
# MAGIC     drift_level,
# MAGIC     COUNT(*)                                                         AS rows,
# MAGIC     SUM(CASE WHEN exp2_from_pc_original IS NOT NULL THEN 1 ELSE 0 END) AS has_original,
# MAGIC     SUM(CASE WHEN exp2 <> exp2_from_pc_original THEN 1 ELSE 0 END)  AS differs,
# MAGIC     ROUND(AVG(CASE WHEN exp2 > 0 AND exp2_from_pc_original > 0
# MAGIC                    THEN (exp2_from_pc_original - exp2) / exp2 * 100 END), 2) AS avg_drift_pct
# MAGIC FROM Gold_Inventory_Lakehouse.inv.gold_consumption_status
# MAGIC GROUP BY drift_level
# MAGIC ORDER BY rows DESC;
# MAGIC 
# MAGIC 
# MAGIC -- ─────────────────────────────────────────────────────────────────────
# MAGIC -- 🟢 Q4: Negative Consump2 — how many would flip to positive?
# MAGIC -- ─────────────────────────────────────────────────────────────────────
# MAGIC -- ดูว่ามี PRO กี่ใบที่ Consump2 ติดลบ และจะแก้ได้กี่ใบถ้าใช้ original
# MAGIC SELECT
# MAGIC     'Current (using exp2)' AS scenario,
# MAGIC     COUNT(*)                          AS total,
# MAGIC     SUM(CASE WHEN Consump2 < -0.0001 THEN 1 ELSE 0 END) AS negative_consump2,
# MAGIC     ROUND(100.0 * SUM(CASE WHEN Consump2 < -0.0001 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_negative
# MAGIC FROM Gold_Inventory_Lakehouse.inv.gold_consumption_status
# MAGIC WHERE exp2 IS NOT NULL
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT
# MAGIC     'If using exp2_from_pc_original' AS scenario,
# MAGIC     COUNT(*)                          AS total,
# MAGIC     SUM(CASE WHEN (COALESCE(exp2_from_pc_original, exp2) + COALESCE(con2, 0)) < -0.0001 THEN 1 ELSE 0 END) AS negative_consump2,
# MAGIC     ROUND(100.0 * SUM(CASE WHEN (COALESCE(exp2_from_pc_original, exp2) + COALESCE(con2, 0)) < -0.0001 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_negative
# MAGIC FROM Gold_Inventory_Lakehouse.inv.gold_consumption_status
# MAGIC WHERE exp2 IS NOT NULL;

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
