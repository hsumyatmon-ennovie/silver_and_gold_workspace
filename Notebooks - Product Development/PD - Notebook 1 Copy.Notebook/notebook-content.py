# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "81bc6bea-77b8-46fe-9189-dcfc3cd43d2f",
# META       "default_lakehouse_name": "Silver_Product_Dev_Lakehouse",
# META       "default_lakehouse_workspace_id": "d74457b3-045c-445d-82c6-9a2e4b9f1436",
# META       "known_lakehouses": [
# META         {
# META           "id": "81bc6bea-77b8-46fe-9189-dcfc3cd43d2f"
# META         },
# META         {
# META           "id": "b78abb0e-d633-4679-b52a-2cfb0797e5c8"
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

# # Gold Step Name

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Product_Dev_Lakehouse.pd.gold_step_name AS
# MAGIC 
# MAGIC WITH
# MAGIC /* =========================
# MAGIC    CAD (hardcode)
# MAGIC ========================= */
# MAGIC cad AS (
# MAGIC   SELECT *
# MAGIC   FROM (
# MAGIC     SELECT 'CAD' AS Department, 'BACKLOG' AS `Machine center`, 'BACKLOG' AS `Work center`, 'BKLG' AS MAP, 1 AS Sequence, 'CADBACKLOG' AS `KEY`, 'BKLG' AS `KEY - Copy`
# MAGIC     UNION ALL SELECT 'CAD','CAD ASSIGNED TO ENGINEER','CAD ASSIGNED TO ENGINEER','CAD ASGN',2,'CADCAD ASSIGNED TO ENGINEER','ASGN'
# MAGIC     UNION ALL SELECT 'CAD','WAITING FOR MANAGER APPROVAL','WAITING FOR MANAGER APPROVAL','MNG APP',3,'CADWAITING FOR MANAGER APPROVAL','MNG APP'
# MAGIC     UNION ALL SELECT 'CAD','WAITING FOR CUSTOMER APPROVAL','WAITING FOR CUSTOMER APPROVAL','CUS APP',4,'CADWAITING FOR CUSTOMER APPROVAL','CUS APP'
# MAGIC     UNION ALL SELECT 'CAD','CUSTOMER REVISION','CUSTOMER REVISION','CUS REV',5,'CADCUSTOMER REVISION','CUS REV'
# MAGIC     UNION ALL SELECT 'CAD','CAD PRODUCTION READY','CAD PRODUCTION READY','CAD READY',6,'CADCAD PRODUCTION READY','READY'
# MAGIC     UNION ALL SELECT 'CAD','3D PRINTING','3D PRINTING','3D PRINT',7,'CAD3D PRINTING','3D PT'
# MAGIC     UNION ALL SELECT 'CAD','RECEIVE WAX','RECEIVE WAX','WAX CAD',8,'CADRECEIVE WAX','CAD WAX'
# MAGIC   ) x
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    Machine Center base (for SP)
# MAGIC ========================= */
# MAGIC mc AS (
# MAGIC   SELECT
# MAGIC     CAST(`Department Group` AS STRING) AS DeptGroup,
# MAGIC     CAST(`No.` AS STRING)             AS MachineCenter,
# MAGIC     CAST(`Work Center No.` AS STRING) AS WorkCenter,
# MAGIC     CAST(`Operation Group` AS STRING) AS OpGroup
# MAGIC   FROM Silver_BC_Lakehouse.bc.`Machine Center`
# MAGIC   WHERE COALESCE(CAST(Blocked AS INT), 0) = 0
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    MST header (latest)
# MAGIC    Spark: use row_number instead of TOP(1)
# MAGIC ========================= */
# MAGIC mst_hdr AS (
# MAGIC   SELECT RoutingNo, VersionCode
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       h.`No.`          AS RoutingNo,
# MAGIC       h.`Version Nos.` AS VersionCode,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         ORDER BY
# MAGIC           COALESCE(TO_TIMESTAMP(h.`Last Date Modified`), TO_TIMESTAMP('1900-01-01')) DESC,
# MAGIC           COALESCE(TO_TIMESTAMP(h.`SystemModifiedAt`),   TO_TIMESTAMP('1900-01-01')) DESC
# MAGIC       ) AS rn
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Routing Header` h
# MAGIC     WHERE h.`No.` = 'MST'
# MAGIC       AND (h.`Description` = 'Master Silver' OR h.`Search Description` = 'MASTER SILVER')
# MAGIC   ) t
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    MST lines picked
# MAGIC ========================= */
# MAGIC mst_picked AS (
# MAGIC   SELECT
# MAGIC     'MST' AS Department,
# MAGIC     CAST(rl.`No.` AS STRING) AS `Machine center`,
# MAGIC     CAST(COALESCE(rl.`Work Center No.`, rl.`No.`) AS STRING) AS `Work center`,
# MAGIC 
# MAGIC     CASE
# MAGIC       WHEN rl.`No.` = 'WH2F'               AND rl.`Type` = 'Work Center'    THEN 'WAREHOUSE'
# MAGIC       WHEN rl.`No.` = 'PD ADMIN CENTER'    AND rl.`Type` = 'Work Center'    THEN 'ADM'
# MAGIC       WHEN rl.`No.` = 'CASTING'            AND rl.`Type` = 'Machine Center' THEN 'CASTING'
# MAGIC       WHEN rl.`No.` = 'FILING MASTER'      AND rl.`Type` = 'Machine Center' THEN 'FIL'
# MAGIC       WHEN rl.`No.` = 'MASTER PRE SETTING' AND rl.`Type` = 'Machine Center' THEN 'SET'
# MAGIC       WHEN rl.`No.` = 'LASER ENGRAVING'    AND rl.`Type` = 'Machine Center' THEN 'LAS'
# MAGIC       WHEN rl.`No.` = 'SPRUE'              AND rl.`Type` = 'Machine Center' THEN 'SPR'
# MAGIC       WHEN rl.`No.` = 'WAX ROOM'           AND rl.`Type` = 'Machine Center' THEN 'R. MOLD'
# MAGIC     END AS MAP,
# MAGIC 
# MAGIC     CASE
# MAGIC       WHEN rl.`No.` = 'WH2F'               AND rl.`Type` = 'Work Center'    THEN  9
# MAGIC       WHEN rl.`No.` = 'PD ADMIN CENTER'    AND rl.`Type` = 'Work Center'    THEN 10
# MAGIC       WHEN rl.`No.` = 'CASTING'            AND rl.`Type` = 'Machine Center' THEN 11
# MAGIC       WHEN rl.`No.` = 'FILING MASTER'      AND rl.`Type` = 'Machine Center' THEN 12
# MAGIC       WHEN rl.`No.` = 'MASTER PRE SETTING' AND rl.`Type` = 'Machine Center' THEN 13
# MAGIC       WHEN rl.`No.` = 'LASER ENGRAVING'    AND rl.`Type` = 'Machine Center' THEN 14
# MAGIC       WHEN rl.`No.` = 'SPRUE'              AND rl.`Type` = 'Machine Center' THEN 15
# MAGIC       WHEN rl.`No.` = 'WAX ROOM'           AND rl.`Type` = 'Machine Center' THEN 16
# MAGIC     END AS Sequence,
# MAGIC 
# MAGIC     CONCAT('MST', CAST(COALESCE(rl.`Work Center No.`, rl.`No.`) AS STRING)) AS `KEY`,
# MAGIC 
# MAGIC     CASE
# MAGIC       WHEN rl.`No.` = 'WH2F'               AND rl.`Type` = 'Work Center'    THEN 'WH MST'
# MAGIC       WHEN rl.`No.` = 'PD ADMIN CENTER'    AND rl.`Type` = 'Work Center'    THEN 'ADN MST'
# MAGIC       WHEN rl.`No.` = 'CASTING'            AND rl.`Type` = 'Machine Center' THEN 'CAS MST'
# MAGIC       WHEN rl.`No.` = 'FILING MASTER'      AND rl.`Type` = 'Machine Center' THEN 'FL MST'
# MAGIC       WHEN rl.`No.` = 'MASTER PRE SETTING' AND rl.`Type` = 'Machine Center' THEN 'P-SET'
# MAGIC       WHEN rl.`No.` = 'LASER ENGRAVING'    AND rl.`Type` = 'Machine Center' THEN 'ENGRV'
# MAGIC       WHEN rl.`No.` = 'SPRUE'              AND rl.`Type` = 'Machine Center' THEN 'SPR'
# MAGIC       WHEN rl.`No.` = 'WAX ROOM'           AND rl.`Type` = 'Machine Center' THEN 'R-MOLD'
# MAGIC     END AS `KEY - Copy`
# MAGIC 
# MAGIC   FROM Silver_BC_Lakehouse.bc.`Routing Line` rl
# MAGIC   INNER JOIN mst_hdr h
# MAGIC     ON rl.`Routing No.` = h.RoutingNo
# MAGIC    AND (rl.`Version Code` = h.VersionCode OR NULLIF(h.VersionCode, '') IS NULL)
# MAGIC 
# MAGIC   WHERE
# MAGIC     (rl.`Type` = 'Work Center'    AND rl.`No.` IN ('WH2F','PD ADMIN CENTER'))
# MAGIC     OR
# MAGIC     (rl.`Type` = 'Machine Center' AND rl.`No.` IN ('CASTING','FILING MASTER','MASTER PRE SETTING','LASER ENGRAVING','SPRUE','WAX ROOM'))
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    SP targets
# MAGIC ========================= */
# MAGIC sp_targets AS (
# MAGIC   SELECT *
# MAGIC   FROM (
# MAGIC     SELECT 'SP' AS Department, 17 AS Sequence, 'WH2F' AS NeedMachine, 'WAREHOUSE'     AS NeedWork, 'WH'      AS MAP, 'WH SP'  AS KeyCopy
# MAGIC     UNION ALL SELECT 'SP',18,'PROD ADMIN',NULL,'ADM','ADN SP'
# MAGIC     UNION ALL SELECT 'SP',19,'CASTING','CASTING ROOM','CASTING','CAS SP'
# MAGIC     UNION ALL SELECT 'SP',20,'FILING','FILING','FIL','FIL'
# MAGIC     UNION ALL SELECT 'SP',21,'SETTING','SETTING','SET','SET'
# MAGIC     UNION ALL SELECT 'SP',22,'POLISHING','POLISHING','POL','POL'
# MAGIC     UNION ALL SELECT 'SP',23,'PLATING','PLATING ROOM','PLT','PLAT'
# MAGIC     UNION ALL SELECT 'SP',24,'QA','QA ROOM','QA','QA'
# MAGIC   ) x
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    SP pick (Spark replacement for OUTER APPLY TOP(1))
# MAGIC    - join candidates then row_number() to select best match per target
# MAGIC ========================= */
# MAGIC sp_candidates AS (
# MAGIC   SELECT
# MAGIC     t.Department,
# MAGIC     t.Sequence,
# MAGIC     t.MAP,
# MAGIC     t.KeyCopy,
# MAGIC     m.MachineCenter AS `Machine center`,
# MAGIC     m.WorkCenter    AS `Work center`,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY t.Department, t.Sequence
# MAGIC       ORDER BY
# MAGIC         CASE WHEN t.NeedMachine IS NOT NULL AND m.MachineCenter = t.NeedMachine THEN 0 ELSE 1 END,
# MAGIC         CASE WHEN t.NeedWork    IS NOT NULL AND m.WorkCenter    = t.NeedWork    THEN 0 ELSE 1 END
# MAGIC     ) AS rn
# MAGIC   FROM sp_targets t
# MAGIC   LEFT JOIN mc m
# MAGIC     ON m.DeptGroup IN ('PRODUCTION','QUALITY','CASTING','WAREHOUSE')
# MAGIC    AND (t.NeedMachine IS NULL OR m.MachineCenter = t.NeedMachine)
# MAGIC    AND (t.NeedWork    IS NULL OR m.WorkCenter    = t.NeedWork)
# MAGIC ),
# MAGIC 
# MAGIC sp_picked AS (
# MAGIC   SELECT
# MAGIC     Department,
# MAGIC     `Machine center`,
# MAGIC     `Work center`,
# MAGIC     MAP,
# MAGIC     Sequence,
# MAGIC     CONCAT(Department, COALESCE(`Work center`, `Machine center`)) AS `KEY`,
# MAGIC     KeyCopy AS `KEY - Copy`
# MAGIC   FROM sp_candidates
# MAGIC   WHERE rn = 1
# MAGIC )
# MAGIC 
# MAGIC /* =========================
# MAGIC    FINAL
# MAGIC ========================= */
# MAGIC SELECT Department, `Machine center` as Code, `Work center` AS work_center, MAP, Sequence, `KEY`, `KEY - Copy` AS Key_Abbr FROM cad
# MAGIC UNION ALL
# MAGIC SELECT Department, `Machine center` as Code, `Work center` AS work_center, MAP, Sequence, `KEY`, `KEY - Copy` AS Key_Abbr FROM mst_picked
# MAGIC UNION ALL
# MAGIC SELECT Department, `Machine center` as Code, `Work center` AS work_center, MAP, Sequence, `KEY`, `KEY - Copy` AS Key_Abbr FROM sp_picked
# MAGIC ;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Step Master Final

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Product_Dev_Lakehouse.pd.gold_step_master_final  AS
# MAGIC WITH
# MAGIC /* =========================
# MAGIC    CAD (from table)
# MAGIC ========================= */
# MAGIC cad AS (
# MAGIC   SELECT
# MAGIC     'CAD' AS Department,
# MAGIC     CAST(s.pd_step AS STRING) AS `Machine center`,
# MAGIC     CAST(s.pd_step AS STRING) AS `Work center`,
# MAGIC     CASE CAST(s.pd_step AS STRING)
# MAGIC       WHEN 'BACKLOG'                      THEN 'BKLG'
# MAGIC       WHEN 'CAD ASSIGNED TO ENGINEER'     THEN 'CAD ASGN'
# MAGIC       WHEN 'CAD IN PROGRESS'              THEN 'CAD PROG'
# MAGIC       WHEN 'WAITING FOR MANAGER APPROVAL' THEN 'MNG APP'
# MAGIC       WHEN 'WAITING FOR CUSTOMER APPROVAL'THEN 'CUS APP'
# MAGIC       WHEN 'CUSTOMER REVISION'            THEN 'CUS REV'
# MAGIC       WHEN 'CAD PRODUCTION READY'         THEN 'CAD READY'
# MAGIC       WHEN '3D PRINTING'                  THEN '3D PRINT'
# MAGIC       WHEN 'RECEIVE WAX'                  THEN 'WAX CAD'
# MAGIC       ELSE CAST(s.pd_step AS STRING)
# MAGIC     END AS MAP,
# MAGIC     CAST(s.pd_sequence AS INT) AS Sequence,
# MAGIC     CONCAT('CAD', CAST(s.pd_step AS STRING)) AS `KEY`,
# MAGIC     CASE CAST(s.pd_step AS STRING)
# MAGIC       WHEN 'BACKLOG'                      THEN 'BKLG'
# MAGIC       WHEN 'CAD ASSIGNED TO ENGINEER'     THEN 'ASGN'
# MAGIC       WHEN 'CAD IN PROGRESS'              THEN 'PROG'
# MAGIC       WHEN 'WAITING FOR MANAGER APPROVAL' THEN 'MNG APP'
# MAGIC       WHEN 'WAITING FOR CUSTOMER APPROVAL'THEN 'CUS APP'
# MAGIC       WHEN 'CUSTOMER REVISION'            THEN 'CUS REV'
# MAGIC       WHEN 'CAD PRODUCTION READY'         THEN 'READY'
# MAGIC       WHEN '3D PRINTING'                  THEN '3D PT'
# MAGIC       WHEN 'RECEIVE WAX'                  THEN 'CAD WAX'
# MAGIC       ELSE CAST(s.pd_step AS STRING)
# MAGIC     END AS `KEY - Copy`
# MAGIC   FROM Silver_Product_Dev_Lakehouse.pd.silver_pd_step s
# MAGIC   WHERE s.pd_step IS NOT NULL
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    Machine Center base (for SP)
# MAGIC ========================= */
# MAGIC mc AS (
# MAGIC   SELECT
# MAGIC     CAST(`Department Group` AS STRING) AS DeptGroup,
# MAGIC     CAST(`No.` AS STRING)             AS MachineCenter,
# MAGIC     CAST(`Work Center No.` AS STRING) AS WorkCenter,
# MAGIC     CAST(`Operation Group` AS STRING) AS OpGroup
# MAGIC   FROM Silver_BC_Lakehouse.bc.`Machine Center`
# MAGIC   WHERE COALESCE(CAST(Blocked AS INT), 0) = 0
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    MST header (Routing: MST ∙ Master Silver)
# MAGIC ========================= */
# MAGIC mst_hdr AS (
# MAGIC   SELECT
# MAGIC     h.`No.`          AS RoutingNo,
# MAGIC     h.`Version Nos.` AS VersionCode
# MAGIC   FROM Silver_BC_Lakehouse.bc.`Routing Header` h
# MAGIC   WHERE
# MAGIC     h.`No.` = 'MST'
# MAGIC     AND (
# MAGIC       h.`Description` = 'Master Silver'
# MAGIC       OR h.`Search Description` = 'MASTER SILVER'
# MAGIC     )
# MAGIC   ORDER BY
# MAGIC     CAST(h.`Last Date Modified` AS TIMESTAMP) DESC,
# MAGIC     CAST(h.`SystemModifiedAt`   AS TIMESTAMP) DESC
# MAGIC   LIMIT 1
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    MST lines (Routing Line)
# MAGIC ========================= */
# MAGIC mst_picked AS (
# MAGIC   SELECT
# MAGIC     'MST' AS Department,
# MAGIC     CAST(rl.`No.` AS STRING) AS `Machine center`,
# MAGIC     CAST(COALESCE(rl.`Work Center No.`, rl.`No.`) AS STRING) AS `Work center`,
# MAGIC     CASE
# MAGIC       WHEN rl.`No.` = 'WH2F'               AND rl.`Type` = 'Work Center'    THEN 'WAREHOUSE'
# MAGIC       WHEN rl.`No.` = 'PD ADMIN CENTER'    AND rl.`Type` = 'Work Center'    THEN 'ADM'
# MAGIC       WHEN rl.`No.` = 'CASTING'            AND rl.`Type` = 'Machine Center' THEN 'CASTING'
# MAGIC       WHEN rl.`No.` = 'FILING MASTER'      AND rl.`Type` = 'Machine Center' THEN 'FIL'
# MAGIC       WHEN rl.`No.` = 'MASTER PRE SETTING' AND rl.`Type` = 'Machine Center' THEN 'SET'
# MAGIC       WHEN rl.`No.` = 'LASER ENGRAVING'    AND rl.`Type` = 'Machine Center' THEN 'LAS'
# MAGIC       WHEN rl.`No.` = 'SPRUE'              AND rl.`Type` = 'Machine Center' THEN 'SPR'
# MAGIC       WHEN rl.`No.` = 'WAX ROOM'           AND rl.`Type` = 'Machine Center' THEN 'R. MOLD'
# MAGIC     END AS MAP,
# MAGIC     CASE
# MAGIC       WHEN rl.`No.` = 'WH2F'               AND rl.`Type` = 'Work Center'    THEN  9
# MAGIC       WHEN rl.`No.` = 'PD ADMIN CENTER'    AND rl.`Type` = 'Work Center'    THEN 10
# MAGIC       WHEN rl.`No.` = 'CASTING'            AND rl.`Type` = 'Machine Center' THEN 11
# MAGIC       WHEN rl.`No.` = 'FILING MASTER'      AND rl.`Type` = 'Machine Center' THEN 12
# MAGIC       WHEN rl.`No.` = 'MASTER PRE SETTING' AND rl.`Type` = 'Machine Center' THEN 13
# MAGIC       WHEN rl.`No.` = 'LASER ENGRAVING'    AND rl.`Type` = 'Machine Center' THEN 14
# MAGIC       WHEN rl.`No.` = 'SPRUE'              AND rl.`Type` = 'Machine Center' THEN 15
# MAGIC       WHEN rl.`No.` = 'WAX ROOM'           AND rl.`Type` = 'Machine Center' THEN 16
# MAGIC     END AS Sequence,
# MAGIC     CONCAT('MST', CAST(COALESCE(rl.`Work Center No.`, rl.`No.`) AS STRING)) AS `KEY`,
# MAGIC     CASE
# MAGIC       WHEN rl.`No.` = 'WH2F'               AND rl.`Type` = 'Work Center'    THEN 'WH MST'
# MAGIC       WHEN rl.`No.` = 'PD ADMIN CENTER'    AND rl.`Type` = 'Work Center'    THEN 'ADN MST'
# MAGIC       WHEN rl.`No.` = 'CASTING'            AND rl.`Type` = 'Machine Center' THEN 'CAS MST'
# MAGIC       WHEN rl.`No.` = 'FILING MASTER'      AND rl.`Type` = 'Machine Center' THEN 'FL MST'
# MAGIC       WHEN rl.`No.` = 'MASTER PRE SETTING' AND rl.`Type` = 'Machine Center' THEN 'P-SET'
# MAGIC       WHEN rl.`No.` = 'LASER ENGRAVING'    AND rl.`Type` = 'Machine Center' THEN 'ENGRV'
# MAGIC       WHEN rl.`No.` = 'SPRUE'              AND rl.`Type` = 'Machine Center' THEN 'SPR'
# MAGIC       WHEN rl.`No.` = 'WAX ROOM'           AND rl.`Type` = 'Machine Center' THEN 'R-MOLD'
# MAGIC     END AS `KEY - Copy`
# MAGIC   FROM Silver_BC_Lakehouse.bc.`Routing Line` rl
# MAGIC   INNER JOIN mst_hdr h
# MAGIC     ON rl.`Routing No.` = h.RoutingNo
# MAGIC    AND (
# MAGIC      rl.`Version Code` = h.VersionCode
# MAGIC      OR NULLIF(h.VersionCode, '') IS NULL
# MAGIC    )
# MAGIC   WHERE
# MAGIC     (rl.`Type` = 'Work Center'    AND rl.`No.` IN ('WH2F','PD ADMIN CENTER'))
# MAGIC     OR
# MAGIC     (rl.`Type` = 'Machine Center' AND rl.`No.` IN ('CASTING','FILING MASTER','MASTER PRE SETTING','LASER ENGRAVING','SPRUE','WAX ROOM'))
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    SP targets
# MAGIC ========================= */
# MAGIC sp_targets AS (
# MAGIC   SELECT * FROM VALUES
# MAGIC     ('SP',  17, 'WH2F',       'WAREHOUSE',    'WH',     'WH SP'),
# MAGIC     ('SP',  18, 'PROD ADMIN', CAST(NULL AS STRING), 'ADM',    'ADN SP'),
# MAGIC     ('SP',  19, 'CASTING',    'CASTING ROOM', 'CASTING','CAS SP'),
# MAGIC     ('SP',  20, 'FILING',     'FILING',       'FIL',    'FIL'),
# MAGIC     ('SP',  21, 'SETTING',    'SETTING',      'SET',    'SET'),
# MAGIC     ('SP',  22, 'POLISHING',  'POLISHING',    'POL',    'POL'),
# MAGIC     ('SP',  23, 'PLATING',    'PLATING ROOM', 'PLT',    'PLAT'),
# MAGIC     ('SP',  24, 'QA',         'QA ROOM',      'QA',     'QA')
# MAGIC   AS v(Department, Sequence, NeedMachine, NeedWork, MAP, KeyCopy)
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    SP picked (OUTER APPLY -> window pick)
# MAGIC ========================= */
# MAGIC sp_ranked AS (
# MAGIC   SELECT
# MAGIC     t.Department,
# MAGIC     m.MachineCenter,
# MAGIC     m.WorkCenter,
# MAGIC     t.MAP,
# MAGIC     t.Sequence,
# MAGIC     t.KeyCopy,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY t.Department, t.Sequence, t.NeedMachine, t.NeedWork
# MAGIC       ORDER BY
# MAGIC         CASE WHEN t.NeedMachine IS NOT NULL AND m.MachineCenter = t.NeedMachine THEN 0 ELSE 1 END,
# MAGIC         CASE WHEN t.NeedWork    IS NOT NULL AND m.WorkCenter    = t.NeedWork    THEN 0 ELSE 1 END
# MAGIC     ) AS rn
# MAGIC   FROM sp_targets t
# MAGIC   LEFT JOIN mc m
# MAGIC     ON m.DeptGroup IN ('PRODUCTION','QUALITY','CASTING','WAREHOUSE')
# MAGIC    AND (t.NeedMachine IS NULL OR m.MachineCenter = t.NeedMachine)
# MAGIC    AND (t.NeedWork    IS NULL OR m.WorkCenter    = t.NeedWork)
# MAGIC ),
# MAGIC 
# MAGIC sp_picked AS (
# MAGIC   SELECT
# MAGIC     Department,
# MAGIC     MachineCenter AS `Machine center`,
# MAGIC     WorkCenter    AS `Work center`,
# MAGIC     MAP,
# MAGIC     Sequence,
# MAGIC     CONCAT(Department, COALESCE(WorkCenter, MachineCenter)) AS `KEY`,
# MAGIC     KeyCopy AS `KEY - Copy`
# MAGIC   FROM sp_ranked
# MAGIC   WHERE rn = 1
# MAGIC )
# MAGIC 
# MAGIC /* =========================
# MAGIC    FINAL
# MAGIC ========================= */
# MAGIC SELECT Department, `Machine center` as Code, `Work center` AS work_center, MAP, Sequence, `KEY`, `KEY - Copy` AS Key_Abbr FROM cad
# MAGIC UNION ALL
# MAGIC SELECT Department, `Machine center` as Code, `Work center` AS work_center, MAP, Sequence, `KEY`, `KEY - Copy` AS Key_Abbr FROM mst_picked
# MAGIC UNION ALL
# MAGIC SELECT Department, `Machine center` as Code, `Work center` AS work_center, MAP, Sequence, `KEY`, `KEY - Copy` AS Key_Abbr FROM sp_picked
# MAGIC ;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold NPD Sketch Mapping

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Product_Dev_Lakehouse.pd.gold_npd_sketch_mapping AS
# MAGIC SELECT
# MAGIC   sm.created_on        AS created_on,
# MAGIC   sm.modified_on       AS modified_on,
# MAGIC 
# MAGIC   sm.pd_sketch_item    AS sketch_number,
# MAGIC   sm.order_type        AS order_type_name,
# MAGIC   sm.customer_no_name  AS customer_approved_id,
# MAGIC   sm.customer_no       AS customer_no,
# MAGIC 
# MAGIC   sm.item_no           AS item_no,
# MAGIC   sm.item_description  AS item_description,
# MAGIC   sm.item_quantity     AS item_qty,
# MAGIC   sm.item_uom          AS item_uom,
# MAGIC 
# MAGIC   sm.prod_order_type        AS prod_order_type,
# MAGIC   sm.prod_order_no          AS prod_order_no,
# MAGIC   sm.prod_order_line_no     AS prod_orde_line_no,
# MAGIC   sm.prod_order_description AS prod_order_description,
# MAGIC 
# MAGIC   sm.sales_order_no      AS sales_order,
# MAGIC   sm.sales_order_no_1    AS sales_order_1,
# MAGIC   sm.sales_order_line_no AS sales_order_line,
# MAGIC 
# MAGIC   -- Merged Sales Order
# MAGIC   CASE
# MAGIC     WHEN sm.sales_order_no IS NULL AND sm.sales_order_no_1 IS NOT NULL THEN sm.sales_order_no_1
# MAGIC     WHEN sm.sales_order_no_1 IS NULL AND sm.sales_order_no IS NOT NULL THEN sm.sales_order_no
# MAGIC     WHEN sm.sales_order_no = sm.sales_order_no_1 THEN sm.sales_order_no
# MAGIC     ELSE CONCAT(sm.sales_order_no, '/', sm.sales_order_no_1)
# MAGIC   END AS MergedSO,
# MAGIC 
# MAGIC   -- Merged Production Order (Spark way)
# MAGIC   concat_ws(
# MAGIC     '/',
# MAGIC     array_distinct(
# MAGIC       filter(
# MAGIC         array(
# MAGIC           sm.prod_order_no,
# MAGIC           sm.prod_order_no_1,
# MAGIC           sm.prod_order_no_2,
# MAGIC           sm.prod_order_no_3,
# MAGIC           sm.prod_order_no_4
# MAGIC         ),
# MAGIC         x -> x IS NOT NULL
# MAGIC       )
# MAGIC     )
# MAGIC   ) AS MergedProd,
# MAGIC 
# MAGIC   -- DO abbreviation
# MAGIC   CASE
# MAGIC     WHEN sm.sales_order_no IS NULL THEN NULL
# MAGIC     ELSE CONCAT(substring(sm.sales_order_no, 1, 2), substring(sm.sales_order_no, length(sm.sales_order_no) - 3, 4))
# MAGIC   END AS do_abbr
# MAGIC 
# MAGIC FROM Silver_Product_Dev_Lakehouse.pd.silver_sketch_mapping sm;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold NPD Prod Status

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Product_Dev_Lakehouse.pd.gold_npd_prod_status AS
# MAGIC SELECT
# MAGIC     pros.created_on,
# MAGIC     pros.modified_on,
# MAGIC     pros.type_name,
# MAGIC     pros.open,
# MAGIC     pros.prod_order_line_no,
# MAGIC 
# MAGIC     pro.`Sales Order No.` AS sales_order_no,
# MAGIC 
# MAGIC     pros.current_location_code,
# MAGIC     pros.machine_center_no,
# MAGIC     pros.quantity,
# MAGIC     pros.remaining_quantity,
# MAGIC     pros.past_location_code,
# MAGIC 
# MAGIC     pro.`No.`,
# MAGIC     pro.Description,
# MAGIC     pros.operation_no,
# MAGIC     pros.item_no,
# MAGIC 
# MAGIC     pro.Status,
# MAGIC 
# MAGIC     -- SO abbreviation (first 2 + last 4)
# MAGIC     CONCAT(
# MAGIC         substring(pro.`Sales Order No.`, 1, 2),
# MAGIC         substring(
# MAGIC             pro.`Sales Order No.`,
# MAGIC             length(pro.`Sales Order No.`) - 3,
# MAGIC             4
# MAGIC         )
# MAGIC     ) AS SO,
# MAGIC 
# MAGIC     -- KEYPO (prefix + location)
# MAGIC     CONCAT(
# MAGIC         substring(pro.`Sales Order No.`, 1, 2),
# MAGIC         pros.current_location_code
# MAGIC     ) AS KEYPO
# MAGIC 
# MAGIC FROM Silver_BC_Lakehouse.bc.`Production Order` AS pro
# MAGIC 
# MAGIC LEFT JOIN Silver_Production_Lakehouse.prod.silver_prod_order_status AS pros
# MAGIC     ON pro.`No.` = pros.prod_order_no
# MAGIC 
# MAGIC WHERE
# MAGIC     (pro.`Sales Order No.` LIKE 'MT%' OR pro.`Sales Order No.` LIKE 'SP%')
# MAGIC     AND pro.Status IN ('Released', 'Finished');


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold NPD SKT Prod

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Product_Dev_Lakehouse.pd.gold_npd_skt_prod AS
# MAGIC WITH SketchData AS (
# MAGIC   SELECT
# MAGIC     sm.MergedSO AS SO,
# MAGIC 
# MAGIC     -- SO_abbr = first 2 + last 4
# MAGIC     CONCAT(
# MAGIC       substring(sm.MergedSO, 1, 2),
# MAGIC       substring(sm.MergedSO, length(sm.MergedSO) - 3, 4)
# MAGIC     ) AS SO_abbr,
# MAGIC 
# MAGIC     sm.item_no AS item_no,
# MAGIC 
# MAGIC     w.pd_sketch_item AS `เลข_SKT`,
# MAGIC     COALESCE(sm.MergedProd, '') AS `เลข_PROD`,
# MAGIC     st.Code AS Routing,
# MAGIC 
# MAGIC     MAX(to_date(w.created_on)) AS latest_created_on,
# MAGIC     date_format(MAX(to_date(w.created_on)), 'dd-MMM') AS LatestCreated_DayMonth,
# MAGIC 
# MAGIC     'Sketch' AS Source,
# MAGIC     st.Sequence AS Sequence,
# MAGIC 
# MAGIC     COALESCE(
# MAGIC       NULLIF(st.Department, ''),
# MAGIC       CASE
# MAGIC         WHEN sm.MergedProd LIKE 'WMT%'  THEN 'MST'
# MAGIC         WHEN sm.MergedProd LIKE 'WONE%' THEN 'SP'
# MAGIC         WHEN sm.MergedProd LIKE 'WSP%'  THEN 'SP'
# MAGIC         WHEN COALESCE(sm.MergedProd, '') = '' THEN 'CAD'
# MAGIC         ELSE 'Unknown'
# MAGIC       END
# MAGIC     ) AS Department
# MAGIC 
# MAGIC   FROM Silver_Product_Dev_Lakehouse.pd.silver_npd_worklogs w
# MAGIC   LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_npd_sketch_mapping sm
# MAGIC     ON w.pd_sketch_item = sm.sketch_number
# MAGIC   LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_step_master_final st
# MAGIC     ON w.pd_step = st.Code
# MAGIC   WHERE
# MAGIC        sm.MergedProd LIKE 'WMT%'
# MAGIC     OR sm.MergedProd LIKE 'WONE%'
# MAGIC     OR sm.MergedProd LIKE 'WSP%'
# MAGIC     OR COALESCE(sm.MergedProd, '') = ''
# MAGIC   GROUP BY
# MAGIC     sm.MergedSO,
# MAGIC     sm.item_no,
# MAGIC     w.pd_sketch_item,
# MAGIC     sm.MergedProd,
# MAGIC     st.Code,
# MAGIC     st.Sequence,
# MAGIC     st.Department
# MAGIC ),
# MAGIC 
# MAGIC ProductionData AS (
# MAGIC   SELECT
# MAGIC     sm.MergedSO AS SO,
# MAGIC 
# MAGIC     -- SO_abbr = first 2 + last 4
# MAGIC     CONCAT(
# MAGIC       substring(sm.MergedSO, 1, 2),
# MAGIC       substring(sm.MergedSO, length(sm.MergedSO) - 3, 4)
# MAGIC     ) AS SO_abbr,
# MAGIC 
# MAGIC     sm.item_no AS item_no,
# MAGIC 
# MAGIC     w.pd_sketch_item AS `เลข_SKT`,
# MAGIC     COALESCE(sm.MergedProd, '') AS `เลข_PROD`,
# MAGIC     st.Code AS Routing,
# MAGIC 
# MAGIC     MAX(to_date(d.created_on)) AS latest_created_on,
# MAGIC     date_format(MAX(to_date(d.created_on)), 'dd-MMM') AS LatestCreated_DayMonth,
# MAGIC 
# MAGIC     'Production' AS Source,
# MAGIC     st.Sequence AS Sequence,
# MAGIC 
# MAGIC     COALESCE(
# MAGIC       NULLIF(st.Department, ''),
# MAGIC       CASE
# MAGIC         WHEN MAX(d.sales_order_no) LIKE 'MT%' THEN 'MST'
# MAGIC         WHEN MAX(d.sales_order_no) LIKE 'SP%' THEN 'SP'
# MAGIC         WHEN COALESCE(MAX(d.sales_order_no), '') = '' THEN 'CAD'
# MAGIC         ELSE 'Unknown'
# MAGIC       END
# MAGIC     ) AS Department
# MAGIC 
# MAGIC   FROM Silver_Product_Dev_Lakehouse.pd.silver_npd_worklogs w
# MAGIC   LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_npd_sketch_mapping sm
# MAGIC     ON w.pd_sketch_item = sm.sketch_number
# MAGIC   LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_npd_prod_status d
# MAGIC     ON sm.MergedProd = d.`No.`
# MAGIC    AND d.type_name = 'In location in'
# MAGIC   LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_step_master_final st
# MAGIC     ON st.Code = COALESCE(NULLIF(d.machine_center_no, ''), d.current_location_code)
# MAGIC   WHERE
# MAGIC        sm.MergedProd LIKE 'WMT%'
# MAGIC     OR sm.MergedProd LIKE 'WONE%'
# MAGIC     OR sm.MergedProd LIKE 'WSP%'
# MAGIC     OR COALESCE(sm.MergedProd, '') = ''
# MAGIC   GROUP BY
# MAGIC     sm.MergedSO,
# MAGIC     sm.item_no,
# MAGIC     w.pd_sketch_item,
# MAGIC     sm.MergedProd,
# MAGIC     st.Code,
# MAGIC     st.Sequence,
# MAGIC     st.Department
# MAGIC )
# MAGIC 
# MAGIC SELECT * FROM SketchData
# MAGIC UNION ALL
# MAGIC SELECT * FROM ProductionData;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold NPD SO INV

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE  Gold_Product_Dev_Lakehouse.pd.gold_npd_so_inv
# MAGIC SELECT
# MAGIC   inv.salesorder_no        AS SalesorderNo,
# MAGIC   inv.salesorder_lineno    AS SalesorderLineNo,
# MAGIC   inv.shipment_no          AS ShipmentNo,
# MAGIC   inv.shipment_lineno      AS ShipmentLineNo,
# MAGIC   inv.invoice_no           AS InvoiceNo,
# MAGIC   inv.invoice_lineno       AS InvoiceLineNo,
# MAGIC 
# MAGIC   ship.Order_Date,
# MAGIC   ship.Requested_Delivery_Date,
# MAGIC   ship.Promised_Delivery_Date,
# MAGIC 
# MAGIC   inv.invoice_posting_date AS PostingDate,
# MAGIC   inv.customer_no          AS CustomerNo,
# MAGIC   -- CAST(NULL AS STRING) AS CustomerName,  -- source has no customer_name
# MAGIC   inv.item_no              AS ItemFG,
# MAGIC   inv.item_description     AS `Description`,
# MAGIC   inv.item_quantity        AS Quantity,
# MAGIC 
# MAGIC   ship.Qty_to_Ship,
# MAGIC   ship.QtyShip,
# MAGIC   ship.Qty_to_Invoice,
# MAGIC   ship.QtyINV,
# MAGIC 
# MAGIC   inv.item_uom             AS UOM
# MAGIC FROM Silver_Finance_Lakehouse.fa.silver_sales_invoice_header_line AS inv
# MAGIC LEFT JOIN Gold_Customer_Exp_Lakehouse.cx.gold_sales_order_shipment_line AS ship
# MAGIC   ON inv.salesorder_no     = ship.SalesorderNo
# MAGIC  AND inv.salesorder_lineno = ship.LineNo;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold NPD SKT INV

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE  Gold_Product_Dev_Lakehouse.pd.gold_npd_skt_inv
# MAGIC SELECT DISTINCT
# MAGIC 
# MAGIC   -- Fields from gold_npd_sketch_mapping
# MAGIC   sm.sketch_number        AS SketchNumber,
# MAGIC   sm.created_on           AS SketchCreatedOn,
# MAGIC   sm.modified_on          AS SketchModifiedOn,
# MAGIC   sm.MergedSO,
# MAGIC   sm.MergedProd,
# MAGIC   sm.prod_order_type,
# MAGIC   sm.item_no              AS ItemFG,
# MAGIC   sm.item_description     AS ItemDescription,
# MAGIC   sm.item_qty             AS ItemQty,
# MAGIC   sm.item_uom             AS ItemUOM,
# MAGIC   sm.customer_no          AS CustomerNo1,
# MAGIC 
# MAGIC   -- Fields from gold_npd_so_inv
# MAGIC   so.CustomerNo,
# MAGIC   so.ShipmentNo,
# MAGIC   so.ShipmentLineNo,
# MAGIC   so.InvoiceNo,
# MAGIC   so.InvoiceLineNo,
# MAGIC   so.Order_Date,
# MAGIC   so.Requested_Delivery_Date,
# MAGIC   so.Promised_Delivery_Date,
# MAGIC   so.PostingDate          AS InvoicePostingDate,
# MAGIC   so.Quantity             AS InvQuantity,
# MAGIC   so.UOM                  AS InvUOM,
# MAGIC   so.Qty_to_Ship,
# MAGIC   so.QtyShip,
# MAGIC   so.Qty_to_Invoice,
# MAGIC   so.QtyINV
# MAGIC 
# MAGIC FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_sketch_mapping AS sm
# MAGIC 
# MAGIC LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_npd_so_inv AS so
# MAGIC   ON sm.MergedSO = so.SalesorderNo
# MAGIC  AND sm.item_no  = so.ItemFG;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold NPD Time

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE  Gold_Product_Dev_Lakehouse.pd.gold_npd_time
# MAGIC SELECT
# MAGIC   c.sketch_number,
# MAGIC   c.estimated_development_time,
# MAGIC   c.estimated_production_ready_time,
# MAGIC   c.customer_name,
# MAGIC 
# MAGIC   -- estimated_development_time_calc
# MAGIC   CASE
# MAGIC     WHEN EXISTS (
# MAGIC       SELECT 1
# MAGIC       FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs wl
# MAGIC       INNER JOIN Silver_Product_Dev_Lakehouse.pd.silver_pd_step steps
# MAGIC         ON wl.routing_code = steps.pd_step
# MAGIC       WHERE wl.sketch_number_text = c.sketch_number
# MAGIC         AND steps.pd_sequence >= 5
# MAGIC     )
# MAGIC       THEN '0'
# MAGIC     WHEN c.estimated_development_time IS NULL
# MAGIC       THEN ''
# MAGIC     ELSE
# MAGIC       CAST(c.estimated_development_time AS STRING)
# MAGIC   END AS estimated_development_time_calc,
# MAGIC 
# MAGIC   -- estimated_production_ready_time_calc
# MAGIC   CASE
# MAGIC     WHEN EXISTS (
# MAGIC       SELECT 1
# MAGIC       FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs wl
# MAGIC       INNER JOIN Silver_Product_Dev_Lakehouse.pd.silver_pd_step steps
# MAGIC         ON wl.routing_code = steps.pd_step
# MAGIC       WHERE wl.sketch_number_text = c.sketch_number
# MAGIC         AND steps.pd_sequence >= 7
# MAGIC     )
# MAGIC       THEN '0'
# MAGIC     WHEN c.estimated_production_ready_time IS NULL
# MAGIC       THEN ''
# MAGIC     ELSE
# MAGIC       CAST(c.estimated_production_ready_time AS STRING)
# MAGIC   END AS estimated_production_ready_time_calc
# MAGIC 
# MAGIC FROM Gold_Product_Dev_Lakehouse.pd.gold_cad_requests c;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Cad Requests

# CELL ********************

# ============================================================
# FULL REPLACE load using Spark SQL (Overwrite)
# Target table: Gold_Product_Dev_Lakehouse.pd.gold_cad_requests
# Source table: Silver_Product_Dev_Lakehouse.pd.silver_cad_requests
# ============================================================

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

SRC_TBL = "Silver_Product_Dev_Lakehouse.pd.silver_cad_requests"
TGT_TBL = "Gold_Product_Dev_Lakehouse.pd.gold_cad_requests"

# ----------------------------
# 1) Full extract (no incremental filter)
# ----------------------------
src_sql = f"""
SELECT
    created_on AS created_on,
    modified_on AS modified_on,

    pd_sketch_item AS sketch_number,
    1 AS qty_skt,

    pd_sketch_item_piority AS priority,
    pd_sketch_item_piority AS priority_name,

    submission_date AS submission_date,
    requested_date AS customer_request_date,

    WEEKOFYEAR(requested_date) AS cus_req,
    WEEKOFYEAR(requested_date) AS cus_week,

    CASE
        WHEN WEEKOFYEAR(requested_date) - 2 > 0
            THEN WEEKOFYEAR(requested_date) - 2
        ELSE 52 + (WEEKOFYEAR(requested_date) - 2)
    END AS cad_week,

    sketch_image AS cropped_design_image_url,
    dts_status AS status_name,

    est_for_development AS estimated_development_time,
    pd_sketch_collection AS collection_id_name,

    customer_name AS customer_name,

    est_for_prod AS estimated_production_ready_time,
    commited_week AS committed_week,

    customer_design_number AS customer_design_number,

    repair_comment AS reopen_sketch_number,
    cs_comment AS comments,

    additional_image AS additional_photo_url,
    is_archive AS is_archive_name,

    customer_target_price AS customer_target_price,
    created_by_email AS created_by_email,

    cs_noted AS status_description,

    customer_target_price AS target_customer_price,
    engineer_name AS assigned_engineer_name,

    promised_date AS promised_date,
    WEEKOFYEAR(promised_date) AS pro_req,

    cs_description AS description,

    CASE
        WHEN DATE_SUB(requested_date, 14) < CURRENT_DATE()
            THEN 'Overdue'
        ELSE 'On Schedule'
    END AS status
FROM {SRC_TBL}
"""

df_src = spark.sql(src_sql)

# Optional: de-dupe (keep if you still want it)
df_src = df_src.dropDuplicates(["sketch_number", "customer_request_date", "promised_date", "modified_on"])

# ----------------------------
# 2) FULL REPLACE write (overwrite target)
# ----------------------------
(df_src.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TGT_TBL)
)

print(f"Full replace completed (overwrite) into: {TGT_TBL}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold NPD Worklogs

# CELL ********************

# ============================================================
# Incremental load using Spark SQL (your CTE) + Delta MERGE
# Target table: Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs
# Source table: Silver_Product_Dev_Lakehouse.pd.silver_npd_worklogs
# (Replaced source column names to match your table; aliases kept the same)
# ============================================================

from delta.tables import DeltaTable

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

SRC_TBL = "Silver_Product_Dev_Lakehouse.pd.silver_npd_worklogs"
TGT_TBL = "Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs"

# ----------------------------
# 1) Get watermark (max created_on) from target
# ----------------------------
last_watermark = None
if spark.catalog.tableExists(TGT_TBL):
    last_watermark = spark.sql(f"""
        SELECT MAX(created_on) AS max_created
        FROM {TGT_TBL}
    """).collect()[0]["max_created"]

# ----------------------------
# 2) Build incremental filter (7-day lookback)
# ----------------------------
LOOKBACK_DAYS = 7
inc_filter = ""
if last_watermark is not None:
    inc_filter = f"WHERE w.created_on >= DATE_SUB(TIMESTAMP('{last_watermark}'), {LOOKBACK_DAYS})"

# ----------------------------
# 3) Spark SQL version of your query (fixed source column names)
#    Your table columns:
#      created_on, created_by, modified_on, modified_by,
#      pd_sketch_item, pd_sketch_item_1, pd_sketch_item_master,
#      pd_step, pd_step_1, wax_received
#
#    Mappings used:
#      sketch_number_text <- pd_sketch_item
#      pd_routing         <- pd_step
# ----------------------------
src_sql = f"""
WITH wl AS
(
    SELECT
        w.pd_sketch_item AS sketch_number_text,
        w.created_on,
        w.pd_step_1 AS routing_code,
        LAG(w.pd_step_1)
            OVER (
                PARTITION BY w.pd_sketch_item
                ORDER BY w.created_on
            ) AS prev_routing
    FROM {SRC_TBL} w
    {inc_filter}
),
cycles AS
(
    SELECT
        wl.*,
        SUM(
            CASE
                WHEN prev_routing IS NOT NULL
                     AND wl.routing_code = prev_routing
                THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY wl.sketch_number_text
            ORDER BY wl.created_on
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cycle_num
    FROM wl
),
latest_cycle AS
(
    SELECT
        sketch_number_text,
        MAX(cycle_num) AS max_cycle
    FROM cycles
    GROUP BY sketch_number_text
)
SELECT
    c.sketch_number_text AS sketch_number_text,
    c.created_on         AS created_on,
    CAST(NULL AS STRING) AS sequence, --------------------------------------------------------- need to change in silver no col in silver
    c.routing_code       AS routing_code
FROM cycles c
JOIN latest_cycle l
  ON c.sketch_number_text = l.sketch_number_text
 AND c.cycle_num          = l.max_cycle
"""

df_src = spark.sql(src_sql)

# Optional but recommended: de-dupe within batch
df_src = df_src.dropDuplicates(["sketch_number_text", "created_on", "routing_code"])

# ----------------------------
# 4) Create target table if not exists, else MERGE (upsert)
#    Merge key: sketch_number_text + created_on + routing_code
# ----------------------------
if not spark.catalog.tableExists(TGT_TBL):
    (df_src.write.format("delta").mode("overwrite").saveAsTable(TGT_TBL))
    print(f"Created target table: {TGT_TBL}")
else:
    tgt = DeltaTable.forName(spark, TGT_TBL)

    merge_cond = """
        t.sketch_number_text = s.sketch_number_text
    AND t.created_on         = s.created_on
    AND t.routing_code       = s.routing_code
    """

    (tgt.alias("t")
        .merge(df_src.alias("s"), merge_cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"MERGE completed into: {TGT_TBL}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold NPD Filtered Worklogs

# CELL ********************

# ============================================================
# FULL CODE (copy-paste safe): build gold_npd_filtered_worklogs
# Strategy: FULL REFRESH (overwrite) — recommended for correctness
# because the NOT IN (...) depends on full history and incremental
# watermark can leave stale rows.
#
# Target: Gold_Product_Dev_Lakehouse.pd.gold_npd_filtered_worklogs
# Source: Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs
# ============================================================

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

SRC_TBL = "Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs"
TGT_TBL = "Gold_Product_Dev_Lakehouse.pd.gold_npd_filtered_worklogs"

src_sql = f"""
WITH cte AS (
    SELECT
        w.sketch_number_text AS skt_number,
        w.created_on         AS worklog_date,
        w.routing_code       AS routing_step,
        ROW_NUMBER() OVER (
            PARTITION BY w.sketch_number_text
            ORDER BY w.created_on DESC
        ) AS rn
    FROM {SRC_TBL} w
    WHERE w.sketch_number_text NOT IN (
        SELECT DISTINCT sketch_number_text
        FROM {SRC_TBL}
        WHERE routing_code IN ('CASTING', 'CAD PRODUCTION READY', '3D PRINTING')
    )
)
SELECT
    skt_number,
    worklog_date,
    routing_step
FROM cte
WHERE rn = 1
"""

df_src = spark.sql(src_sql)

# Ensure exactly 1 row per sketch (safety)
df_src = df_src.dropDuplicates(["skt_number"])

# FULL REFRESH write
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

# # Gold NPD Engineer Time

# CELL ********************

# ============================================================
# Incremental load using Spark SQL (your SELECT) + Delta MERGE
# Target table: Gold_Product_Dev_Lakehouse.pd.gold_npd_engineer_time
# Source table: Silver_Product_Dev_Lakehouse.pd.silver_engineer_time
# ============================================================

from delta.tables import DeltaTable

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

SRC_TBL = "Silver_Product_Dev_Lakehouse.pd.silver_engineer_time"
TGT_TBL = "Gold_Product_Dev_Lakehouse.pd.gold_npd_engineer_time"

# ----------------------------
# 1) Get watermark (max entry_date) from target
# ----------------------------
last_watermark = None
if spark.catalog.tableExists(TGT_TBL):
    last_watermark = spark.sql(f"""
        SELECT MAX(entry_date) AS max_entry
        FROM {TGT_TBL}
    """).collect()[0]["max_entry"]

# ----------------------------
# 2) Build incremental filter (7-day lookback)
# ----------------------------
LOOKBACK_DAYS = 7
inc_filter = ""
if last_watermark is not None:
    inc_filter = f"WHERE created_on >= DATE_SUB(TIMESTAMP('{last_watermark}'), {LOOKBACK_DAYS})"

# ----------------------------
# 3) Spark SQL (FIXED logged_hours)
#    Key idea: Build full timestamps from entry_date + start/end time.
#              If end < start, assume it ended next day.
# ----------------------------
src_sql = f"""
WITH base AS (
  SELECT
      pd_sketch_item AS skt_number,
      created_on     AS entry_date,
      pd_start       AS start_time,
      pd_stop        AS end_time,

      -- Build start timestamp using entry_date + start_time
      to_timestamp(
        concat(
          date_format(created_on, 'yyyy-MM-dd'),
          ' ',
          date_format(pd_start, 'HH:mm:ss')
        )
      ) AS start_ts,

      -- Build end timestamp using entry_date + end_time (raw)
      to_timestamp(
        concat(
          date_format(created_on, 'yyyy-MM-dd'),
          ' ',
          date_format(pd_stop, 'HH:mm:ss')
        )
      ) AS end_ts_raw
  FROM {SRC_TBL}
  {inc_filter}
)
SELECT
  skt_number,
  entry_date,
  start_time,
  end_time,
  CASE
    WHEN start_ts IS NULL OR end_ts_raw IS NULL THEN NULL
    WHEN end_ts_raw < start_ts THEN (unix_timestamp(end_ts_raw + INTERVAL 1 DAY) - unix_timestamp(start_ts)) / 3600.0
    ELSE (unix_timestamp(end_ts_raw) - unix_timestamp(start_ts)) / 3600.0
  END AS logged_hours
FROM base
"""

df_src = spark.sql(src_sql)

# Optional but recommended: de-dupe within the batch
# Include entry_date too (same times on different days shouldn't collapse)
df_src = df_src.dropDuplicates(["skt_number", "entry_date", "start_time", "end_time"])

# ----------------------------
# 4) Create target table if not exists, else MERGE (upsert)
#    Merge key: skt_number + entry_date + start_time + end_time
# ----------------------------
if not spark.catalog.tableExists(TGT_TBL):
    (df_src.write.format("delta").mode("overwrite").saveAsTable(TGT_TBL))
    print(f"Created target table: {TGT_TBL}")
else:
    tgt = DeltaTable.forName(spark, TGT_TBL)

    merge_cond = """
        t.skt_number = s.skt_number
    AND t.entry_date = s.entry_date
    AND t.start_time = s.start_time
    AND t.end_time   = s.end_time
    """

    (tgt.alias("t")
        .merge(df_src.alias("s"), merge_cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"MERGE completed into: {TGT_TBL}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold NPD Engineer Time Log

# CELL ********************

# ============================================================
# Incremental load using Spark SQL + Delta MERGE
# Target table: Gold_Product_Dev_Lakehouse.pd.gold_npd_engineer_time_log
# Source table: Silver_Product_Dev_Lakehouse.pd.silver_engineer_time
# ============================================================

from delta.tables import DeltaTable

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

SRC_TBL = "Silver_Product_Dev_Lakehouse.pd.silver_engineer_time"
TGT_TBL = "Gold_Product_Dev_Lakehouse.pd.gold_npd_engineer_time_log"

# ----------------------------
# 1) Get watermark (max modified_on) from target
# ----------------------------
last_watermark = None
if spark.catalog.tableExists(TGT_TBL):
    last_watermark = spark.sql(f"""
        SELECT MAX(modified_on) AS max_modified
        FROM {TGT_TBL}
    """).collect()[0]["max_modified"]

# ----------------------------
# 2) Build incremental filter (7-day lookback)
# ----------------------------
LOOKBACK_DAYS = 7
inc_filter = ""
if last_watermark is not None:
    inc_filter = f"WHERE modified_on >= DATE_SUB(TIMESTAMP('{last_watermark}'), {LOOKBACK_DAYS})"

# ----------------------------
# 3) Source query (FIXED time logic)
#    Build start/end timestamps from created_on date + time.
#    If end < start => add 1 day to end.
#    Cap end at 18:30 same day as start.
# ----------------------------
src_sql = f"""
WITH base AS (
  SELECT
      pd_sketch_item     AS sketch_number,
      pd_sketch_verion_1 AS cad_version_name,
      created_on         AS created_on,
      pd_start           AS start_time,
      modified_on        AS modified_on,
      pd_stop            AS end_time,
      pd_step            AS routing_step,

      -- Start timestamp = created_on date + start_time (HH:mm:ss)
      to_timestamp(
        concat(
          date_format(created_on, 'yyyy-MM-dd'),
          ' ',
          date_format(pd_start, 'HH:mm:ss')
        )
      ) AS start_ts,

      -- End timestamp raw = created_on date + end_time (HH:mm:ss)
      to_timestamp(
        concat(
          date_format(created_on, 'yyyy-MM-dd'),
          ' ',
          date_format(pd_stop, 'HH:mm:ss')
        )
      ) AS end_ts_raw,

      -- Daily cutoff timestamp (same date as start)
      to_timestamp(concat(date_format(created_on,'yyyy-MM-dd'), ' 18:30:00')) AS cutoff_ts
  FROM {SRC_TBL}
  {inc_filter}
),
norm AS (
  SELECT
      *,
      -- normalize end: if it is earlier than start, assume it ended next day
      CASE
        WHEN start_ts IS NULL OR end_ts_raw IS NULL THEN NULL
        WHEN end_ts_raw < start_ts THEN end_ts_raw + INTERVAL 1 DAY
        ELSE end_ts_raw
      END AS end_ts_norm
  FROM base
),
calc AS (
  SELECT
      sketch_number,
      cad_version_name,
      created_on,
      start_time,
      modified_on,
      end_time,
      routing_step,

      -- apply cap at 18:30: end used for calculation
      CASE
        WHEN start_ts IS NULL OR end_ts_norm IS NULL THEN NULL
        WHEN cutoff_ts <= start_ts THEN start_ts               -- nothing countable if started after cutoff
        WHEN end_ts_norm > cutoff_ts THEN cutoff_ts            -- cap to 18:30
        ELSE end_ts_norm
      END AS end_ts_capped,

      start_ts
  FROM norm
)
SELECT
    sketch_number,
    cad_version_name,
    created_on,
    start_time,
    modified_on,
    end_time,
    routing_step,

    /* 1) hours for CUSTOMER REVISION only */
    SUM(
      CASE
        WHEN routing_step = 'CUSTOMER REVISION' THEN
          CASE
            WHEN start_ts IS NULL OR end_ts_capped IS NULL THEN 0
            WHEN end_ts_capped <= start_ts THEN 0
            ELSE (unix_timestamp(end_ts_capped) - unix_timestamp(start_ts)) / 3600.0
          END
        ELSE 0
      END
    ) AS customer_revision_hours,

    /* 2) hours for other steps (exclude CUSTOMER REVISION, 3D PRINTING, RECEIVE WAX) */
    SUM(
      CASE
        WHEN routing_step NOT IN ('CUSTOMER REVISION', '3D PRINTING', 'RECEIVE WAX') THEN
          CASE
            WHEN start_ts IS NULL OR end_ts_capped IS NULL THEN 0
            WHEN end_ts_capped <= start_ts THEN 0
            ELSE (unix_timestamp(end_ts_capped) - unix_timestamp(start_ts)) / 3600.0
          END
        ELSE 0
      END
    ) AS total_hours

FROM calc
GROUP BY
    sketch_number,
    cad_version_name,
    created_on,
    start_time,
    modified_on,
    end_time,
    routing_step
"""

df_src = spark.sql(src_sql)

# De-dupe within batch (match the target grain)
df_src = df_src.dropDuplicates([
    "sketch_number",
    "cad_version_name",
    "created_on",
    "start_time",
    "modified_on",
    "end_time",
    "routing_step"
])

# ----------------------------
# 4) Create target table if not exists, else MERGE (upsert)
# ----------------------------
if not spark.catalog.tableExists(TGT_TBL):
    (df_src.write.format("delta").mode("overwrite").saveAsTable(TGT_TBL))
    print(f"Created target table: {TGT_TBL}")
else:
    tgt = DeltaTable.forName(spark, TGT_TBL)

    merge_cond = """
        t.sketch_number      = s.sketch_number
    AND t.cad_version_name   = s.cad_version_name
    AND t.created_on         = s.created_on
    AND t.start_time         = s.start_time
    AND t.modified_on        = s.modified_on
    AND t.end_time           = s.end_time
    AND t.routing_step       = s.routing_step
    """

    (tgt.alias("t")
        .merge(df_src.alias("s"), merge_cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"MERGE completed into: {TGT_TBL}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold NPD SKT

# CELL ********************

# ============================================================
# FULL REFRESH: gold_npd_skt_prod_routing
# Source:
#   - Silver_Product_Dev_Lakehouse.pd.silver_npd_worklogs
#   - Silver_Product_Dev_Lakehouse.pd.silver_sketch_mapping
#   - Silver_BC_Lakehouse.bc.Machine Center
# ============================================================

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT_TBL = "Gold_Product_Dev_Lakehouse.pd.gold_npd_skt"

src_sql = """
SELECT
    w.pd_sketch_item AS `เลข_SKT`,

    COALESCE(sm.prod_order_no, '') AS `เลข_PROD`,

    mc.Code AS `Routing`,

    MAX(CAST(w.created_on AS DATE)) AS `Lasted_Created_On`,

    'Sketch' AS `Source`,

    mc.`Sequence` AS `Sequence`,

    CASE
        WHEN COALESCE(sm.prod_order_no, '') LIKE 'WMT%' THEN 'MST'
        WHEN COALESCE(sm.prod_order_no, '') LIKE 'WSP%' THEN 'SP'
        WHEN COALESCE(sm.prod_order_no, '') = '' THEN 'CAD'
        ELSE 'Unknown'
    END AS `Department`

FROM Silver_Product_Dev_Lakehouse.pd.silver_npd_worklogs w

LEFT JOIN Silver_Product_Dev_Lakehouse.pd.silver_sketch_mapping sm
    ON w.pd_sketch_item = sm.pd_sketch_item

LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_step_name mc
    ON w.pd_step = mc.Code

GROUP BY
    w.pd_sketch_item,
    sm.prod_order_no,
    mc.Code,
    mc.`Sequence`
"""

df_src = spark.sql(src_sql)

# FULL REFRESH write
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

# # Gold NPD Time

# CELL ********************

# ============================================================
# FULL REFRESH (recommended): build gold table from your query
# Reason: EXISTS subqueries depend on full history of worklogs/steps,
# so incremental watermark can leave stale calculated fields.
#
# Target table name (edit if you want):
#   Gold_Product_Dev_Lakehouse.pd.gold_npd_time
# ============================================================

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT_TBL = "Gold_Product_Dev_Lakehouse.pd.gold_npd_time"

src_sql = """
SELECT
    c.pd_sketch_item       ,
    c.est_for_development  ,
    c.est_for_prod         ,
    c.customer_name        ,

    CASE
        WHEN EXISTS (
            SELECT 1
            FROM Silver_Product_Dev_Lakehouse.pd.silver_npd_worklogs wl
            INNER JOIN Gold_Product_Dev_Lakehouse.pd.gold_step_name steps
                ON wl.pd_step = steps.Code
            WHERE wl.pd_sketch_item = c.pd_sketch_item
              AND steps.`Sequence` >= 5
        )
        THEN '0'
        WHEN c.est_for_development IS NULL
        THEN ''
        ELSE CAST(c.est_for_development AS STRING)
    END AS est_for_development_calc,

    CASE
        WHEN EXISTS (
            SELECT 1
            FROM Silver_Product_Dev_Lakehouse.pd.silver_npd_worklogs wl
            INNER JOIN Gold_Product_Dev_Lakehouse.pd.gold_step_name steps
                ON wl.pd_step = steps.Code
            WHERE wl.pd_sketch_item = c.pd_sketch_item
              AND steps.`Sequence` >= 7
        )
        THEN '0'
        WHEN c.est_for_prod IS NULL
        THEN ''
        ELSE CAST(c.est_for_prod AS STRING)
    END AS est_for_ready_time_calc

FROM Silver_Product_Dev_Lakehouse.pd.silver_cad_requests c
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

# # Gold  NPD Current Report

# CELL ********************

# ============================================================
# FULL REFRESH (recommended): build gold table from your query
# Reason: DISTINCT + latest-worklog logic (TOP 1 / OUTER APPLY)
# and joins to production status that can change -> full refresh
# prevents stale routing/status.
#
# Target table name (edit if you want):
#   Gold_Product_Dev_Lakehouse.pd.gold_npd_current_report
# ============================================================

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT_TBL = "Gold_Product_Dev_Lakehouse.pd.gold_npd_current_report"

src_sql = """
WITH logLatest AS (
    SELECT
        l.sketch_number_text,
        l.routing_code,
        l.created_on,
        ROW_NUMBER() OVER (
            PARTITION BY l.sketch_number_text
            ORDER BY l.created_on DESC
        ) AS rn
    FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs l
),
log_pick AS (
    SELECT
        sketch_number_text,
        routing_code,
        created_on
    FROM logLatest
    WHERE rn = 1
)
SELECT DISTINCT
    cad.customer_request_date,
    cad.collection_id_name,
    cad.sketch_number,
    cad.customer_design_number,
    cad.cropped_design_image_url,
    cad.submission_date,
    cad.promised_date,

    CASE
        WHEN lp.routing_code IN ('CAD PRODUCTION READY','3D PRINTING','RECEIVE WAX')
            THEN 'COMPLETED'
        WHEN lp.routing_code = 'WAITING FOR CUSTOMER APPROVAL'
            THEN 'CUSTOMER PENDING APPROVAL'
        ELSE lp.routing_code
    END AS RoutingStepName,

    lp.created_on,

    prod.`No.` AS prod_order_no,

    -- SP production status
    CASE
        WHEN prod.sales_order_no LIKE 'SP%' THEN
            CASE
                WHEN lower(CAST(prod.`Status` AS STRING)) = 'finished' THEN 'FINISHED'
                ELSE 'PENDING'
            END
        ELSE ''
    END AS SP_ProductionStatus,

    -- MST/MT production status
    CASE
        WHEN prod.sales_order_no LIKE 'MST%' OR prod.sales_order_no LIKE 'MT%' THEN
            CASE
                WHEN lower(CAST(prod.`Status` AS STRING)) = 'finished' THEN 'FINISHED'
                ELSE 'PENDING'
            END
        ELSE ''
    END AS MST_ProductionStatus,

    prod.item_no,
    prod.quantity

FROM Gold_Product_Dev_Lakehouse.pd.gold_cad_requests cad
LEFT JOIN log_pick lp
    ON lp.sketch_number_text = cad.sketch_number
LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_npd_prod_status prod
    ON prod.KEYPO = cad.sketch_number
WHERE prod.sales_order_no LIKE 'SP%'
   OR prod.sales_order_no LIKE 'MST%'
   OR prod.sales_order_no LIKE 'MT%'
   OR prod.sales_order_no IS NULL
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

# # Gold Last Routing Step

# CELL ********************

# ============================================================
# FULL REFRESH: gold_npd_last_routing_step
# Source: Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs
# ============================================================

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT_TBL = "Gold_Product_Dev_Lakehouse.pd.gold_npd_last_routing_step"

src_sql = """
SELECT
    w.sketch_number_text AS `SKT_Number`,
    w.routing_code       AS `Last_Routing_Step`,
    MAX(w.created_on)    AS `Last_Created_On`
FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs w
GROUP BY
    w.sketch_number_text,
    w.routing_code
"""

df_src = spark.sql(src_sql)

# FULL REFRESH write
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

# # Gold Last Step

# CELL ********************

# ============================================================
# FULL REFRESH (recommended): filtered worklogs excluding completed routing steps
# Reason: NOT IN subquery depends on full history; incremental watermark can leave stale rows.
#
# Target table name (edit if you want):
#   Gold_Product_Dev_Lakehouse.pd.gold_npd_last_step
# ============================================================

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT_TBL = "Gold_Product_Dev_Lakehouse.pd.gold_npd_last_step"

src_sql = """
SELECT
    w.sketch_number_text        AS `SKT_Number`,
    CAST(w.created_on AS DATE)  AS `Worklog_Date`,
    w.routing_code              AS `Routing_Step`
FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs w
WHERE w.sketch_number_text NOT IN
(
    SELECT DISTINCT w2.sketch_number_text
    FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs w2
    WHERE w2.routing_code IN ('RECEIVE WAX', 'CAD PRODUCTION READY', '3D PRINTING')
      AND w2.sketch_number_text IS NOT NULL
)
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

# # Gold NPD Last Step Combined

# CELL ********************

# ============================================================
# FULL REFRESH: gold_npd_last_step_combined
# Source: Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs
# ============================================================

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT_TBL = "Gold_Product_Dev_Lakehouse.pd.gold_npd_last_step_combined"

src_sql = """
WITH LastStep AS (
    SELECT
        w.sketch_number_text        AS `SKT_Number`,
        CAST(w.created_on AS DATE)  AS `Worklog_Date`,
        w.routing_code              AS `Routing_Step`,
        ROW_NUMBER() OVER (
            PARTITION BY w.sketch_number_text
            ORDER BY CAST(w.created_on AS DATE) DESC, w.created_on DESC
        ) AS rn
    FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs w
)
SELECT
    `SKT_Number`,
    `Worklog_Date`,
    `Routing_Step`
FROM LastStep
WHERE rn = 1
"""

df_src = spark.sql(src_sql)

# FULL REFRESH write
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

# # Gold NPD Last Step 2

# CELL ********************

# ============================================================
# FULL REFRESH: gold_npd_last_step_2
# Source: Gold_Product_Dev_Lakehouse.pd.gold_npd_last_step
# ============================================================

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT_TBL = "Gold_Product_Dev_Lakehouse.pd.gold_npd_last_step_2"

src_sql = """
SELECT
    SKT_Number   ,
    Worklog_Date ,
    Routing_Step ,
    ROW_NUMBER() OVER (
        PARTITION BY SKT_Number
        ORDER BY Worklog_Date DESC
    ) AS rn
FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_last_step
"""

df_src = spark.sql(src_sql)

# FULL REFRESH write
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

# # Gold Last Step 3

# CELL ********************

# ============================================================
# FULL REFRESH: gold_npd_last_step_3
# Source: Gold_Product_Dev_Lakehouse.pd.gold_npd_last_step_2
# ============================================================

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT_TBL = "Gold_Product_Dev_Lakehouse.pd.gold_npd_last_step_3"

src_sql = """
SELECT
    SKT_Number   ,
    Worklog_Date ,
    Routing_Step 
FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_last_step_2
WHERE rn = 1
"""

df_src = spark.sql(src_sql)

# FULL REFRESH write
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

# # Gold NPD Prod

# CELL ********************

# ============================================================
# FULL REFRESH: gold_npd_prod
# Source:
#   - Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs
#   - Gold_Product_Dev_Lakehouse.pd.gold_npd_sketch_mapping
#   - Gold_Product_Dev_Lakehouse.pd.gold_npd_prod_status
#   - Silver_BC_Lakehouse.bc.Machine Center
# ============================================================

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT_TBL = "Gold_Product_Dev_Lakehouse.pd.gold_npd_prod"

src_sql = """
SELECT
    w.sketch_number_text AS `เลข_SKT`,

    COALESCE(sm.prod_order_no, '') AS `เลข_PROD`,

    st.Code AS `Routing`,

    MAX(CAST(d.created_on AS DATE)) AS `Lasted_Created_On`,

    'Production' AS `Source`,

    st.`Sequence` AS `Sequence`,

    CASE
        WHEN sm.prod_order_no LIKE 'WMT%' THEN 'MST'
        WHEN sm.prod_order_no LIKE 'WSP%' THEN 'SP'
        WHEN COALESCE(sm.prod_order_no, '') = '' THEN 'CAD'
        ELSE 'Unknown'
    END AS `Department`

FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs w

LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_npd_sketch_mapping sm
    ON w.sketch_number_text = sm.sketch_number

LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_npd_prod_status d
    ON sm.prod_order_no = d.`No.`

LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_step_name st
    ON d.current_location_code = st.Code


GROUP BY
    w.sketch_number_text,
    sm.prod_order_no,
    st.Code,
    st.`Sequence`
"""

df_src = spark.sql(src_sql)

# FULL REFRESH write
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

# # Gold NPD SP Ship

# CELL ********************

# ============================================================
# FULL REFRESH (recommended): Production Order + Sales/Shipment join
# Reason: multi-table joins + status/dates can change; full refresh avoids stale results.
#
# Target table name (edit if you want):
#   Gold_Product_Dev_Lakehouse.pd.gold_npd_sp_ship
# ============================================================

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT_TBL = "Gold_Product_Dev_Lakehouse.pd.gold_npd_sp_ship"

src_sql = """
SELECT 
    -- Production Order
    po.`Due Date`                  AS `Due_Date`,
    po.`Status`                    AS `Status`,
    po.`No.`                       AS `No`,
    po.`Description`               AS `Prod_Description`,
    po.`Description 2`             AS `Prod_Description_2`,
    po.`Source No.`                AS `Source_No`,
    po.`Routing No.`               AS `Routing_No`,
    po.`Quantity`                  AS `Prod_Quantity`,
    po.`Prod. Order Type.`         AS `Prod_Order_Type`,
    po.`Sales Order No.`           AS `Sales_Order_No`,
    po.`Sales Order Line No.`      AS `Sales_Order_Line_No`,
    po.`Shortcut Dimension 1 Code` AS `Shortcut_Dimension_1_Code`,
    po.`Shortcut Dimension 2 Code` AS `Shortcut_Dimension_2_Code`,
    po.`Location Code`             AS `Prod_Location_Code`,
    po.`Starting Date-Time`        AS `Starting_Date_Time`,
    po.`Ending Date-Time`          AS `Ending_Date_Time`,

    -- Sales/Shipment line + header
    sl.`DocumentNo`                AS `DocumentNo`,
    sl.`LineNo`                    AS `LineNo`,
    sh.`PostDate`                  AS `PostingDate`,
    sh.`CusNo`                     AS `SellToCustomerNo`,
    sh.`CusName`                   AS `SellToCustomerName`,
    COALESCE(sl.`Currency`, sh.`Currency`) AS `CurrencyCode`,
    CAST(NULL AS STRING)           AS `ShipmentNo`,
    CAST(NULL AS STRING)           AS `ShipmentLineNo`,
    sl.`ItemFG`                    AS `Item_No`,
    sl.`Description`               AS `Description`,
    CAST(NULL AS STRING)           AS `Description2`,
    sl.`item_location`             AS `LocationCode`,
    sl.`QTY`                       AS `Quantity`,
    sl.`UOM`                       AS `UnitOfMeasureCode`,

    -- ship dates
    sl.`Order_Date`                AS `Order_Date`,
    sl.`Requested_Delivery_Date`   AS `Requested_Delivery_Date`,
    sl.`Promised_Delivery_Date`    AS `Promised_Delivery_Date`

FROM Silver_BC_Lakehouse.bc.`Production Order` po

LEFT JOIN Silver_BC_Lakehouse.bc.`Item` it
    ON po.`Source No.` = it.`No.`

LEFT JOIN Gold_Customer_Exp_Lakehouse.cx.`gold_sales_order_shipment_line` sl
    ON po.`Sales Order No.` = sl.`SalesorderNo`
   AND po.`Sales Order Line No.` = sl.`LineNo`

LEFT JOIN Gold_Customer_Exp_Lakehouse.cx.`gold_sales_order_shipment_header` sh
    ON sl.`SalesorderNo` = sh.`SalesorderNo`

WHERE 
    po.`Sales Order No.` LIKE 'SP%'
 OR po.`Sales Order No.` LIKE 'MT%'
 OR po.`Sales Order No.` LIKE 'SL%'
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

# # Gold NPD SKT with Status

# CELL ********************

# ============================================================
# FULL REFRESH: gold_npd_skt_with_status
# Alias style: TitleCase_With_Underscores
# ============================================================

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT_TBL = "Gold_Product_Dev_Lakehouse.pd.gold_npd_skt_with_status"

src_sql = """
SELECT 
    w.sketch_number_text                    AS Lek_Skt,
    COALESCE(sm.prod_order_no, '-') AS Lek_Prod,
    mc_cad.Code                           AS Routing,
    CAST(w.created_on AS DATE)              AS Lasted_Created_On,
    'Npd_Worklogs'                          AS Source
FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs w
LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_npd_sketch_mapping sm
    ON w.sketch_number_text = sm.sketch_number
LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_step_name mc_cad
    ON w.routing_code = mc_cad.Code
WHERE mc_cad.Code IS NOT NULL

UNION

SELECT 
    w.sketch_number_text                    AS Lek_Skt,
    sm.prod_order_no                AS Lek_Prod,
    mc_prod.Code                           AS Routing,
    CAST(d.created_on AS DATE)              AS Lasted_Created_On,
    'Npd_DsvcProductionOrderStatus'         AS Source
FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs w
LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_npd_sketch_mapping sm
    ON w.sketch_number_text = sm.sketch_number
LEFT JOIN Silver_Production_Lakehouse.prod.silver_prod_order_status d
    ON sm.prod_order_no = d.prod_order_no
   AND sm.product_order_line_no = d.prod_order_line_no
LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_step_name mc_prod
    ON d.current_location_code = mc_prod.Code
WHERE mc_prod.Code IS NOT NULL
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

# # Gold NPD SKT Prod New

# CELL ********************

# ============================================================
# FULL REFRESH: gold_npd_skt_prod_new
# Source: Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs
# Logic: pick latest routing step per SKT
# ============================================================

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT_TBL = "Gold_Product_Dev_Lakehouse.pd.gold_npd_skt_prod_new"

src_sql = """
WITH LastStep AS (
    SELECT
        w.sketch_number_text        AS `SKT_Number`,
        CAST(w.created_on AS DATE)  AS `Worklog_Date`,
        w.routing_code              AS `Routing_Step`,
        ROW_NUMBER() OVER (
            PARTITION BY w.sketch_number_text
            ORDER BY CAST(w.created_on AS DATE) DESC, w.created_on DESC
        ) AS rn
    FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs w
)
SELECT
    `SKT_Number`,
    `Worklog_Date`,
    `Routing_Step`
FROM LastStep
WHERE rn = 1
"""

df_src = spark.sql(src_sql)

# FULL REFRESH write
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

# # Gold NPD SKT Prod Final Routing

# CELL ********************

# ============================================================
# FULL REFRESH: gold_npd_skt_prod_final_routing
# Source tables:
#   - Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs
#   - Gold_Product_Dev_Lakehouse.pd.gold_npd_sketch_mapping
#   - Gold_Product_Dev_Lakehouse.pd.gold_npd_prod_status
#   - Gold_Product_Dev_Lakehouse.pd.gold_step_name
# Output: latest routing per SKT across Sketch + Production
# ============================================================

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT_TBL = "Gold_Product_Dev_Lakehouse.pd.gold_npd_skt_prod_final_routing"

src_sql = """
WITH SketchData AS (
    SELECT 
        w.sketch_number_text                    AS `เลข SKT`,
        COALESCE(sm.prod_order_no, '')  AS `เลข PROD`,
        mc.Code                                AS `Routing`,
        CAST(MAX(w.created_on) AS DATE)         AS `Lasted Created On`,
        'Sketch'                                AS `Source`,
        mc.`Sequence`                   AS `Sequence`,
        CASE 
            WHEN sm.prod_order_no LIKE 'WMT%' THEN 'MST'
            WHEN sm.prod_order_no LIKE 'WSP%' THEN 'SP'
            WHEN COALESCE(sm.prod_order_no, '') = '' THEN 'CAD'
            ELSE 'Unknown'
        END                                     AS `Department`
    FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs w
    LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_npd_sketch_mapping sm
        ON w.sketch_number_text = sm.sketch_number
    LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_step_name mc
        ON w.routing_code = mc.Code
    GROUP BY 
        w.sketch_number_text,
        sm.prod_order_no,
        mc.Code,
        mc.`Sequence`
),
ProductionData AS (
    SELECT 
        w.sketch_number_text                    AS `เลข SKT`,
        COALESCE(sm.prod_order_no, '')  AS `เลข PROD`,
        mc.Code                                AS `Routing`,
        CAST(MAX(d.created_on) AS DATE)         AS `Lasted Created On`,
        'Production'                            AS `Source`,
        mc.`Sequence`                   AS `Sequence`,
        CASE 
            WHEN sm.prod_order_no LIKE 'WMT%' THEN 'MST'
            WHEN sm.prod_order_no LIKE 'WSP%' THEN 'SP'
            WHEN COALESCE(sm.prod_order_no, '') = '' THEN 'CAD'
            ELSE 'Unknown'
        END                                     AS `Department`
    FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs w
    LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_npd_sketch_mapping sm
        ON w.sketch_number_text = sm.sketch_number
    LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_npd_prod_status d
        ON sm.prod_order_no = d.`No.`
    LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_step_name mc
        ON d.current_location_code = mc.Code
    WHERE d.type_name = 'In location in'
    GROUP BY 
        w.sketch_number_text,
        sm.prod_order_no,
        mc.Code,
        mc.`Sequence`
),
CombinedData AS (
    SELECT * FROM SketchData
    UNION ALL
    SELECT * FROM ProductionData
),
RankedData AS (
    SELECT 
        `เลข SKT`,
        `เลข PROD`,
        `Routing`,
        `Lasted Created On`,
        `Source`,
        `Sequence`,
        `Department`,
        ROW_NUMBER() OVER (
            PARTITION BY `เลข SKT`
            ORDER BY `Lasted Created On` DESC, `Sequence` DESC
        ) AS `Rank`
    FROM CombinedData
)
SELECT 
    `เลข SKT` AS `เลข_SKT`,
    `เลข PROD` AS `เลข_PROD`,
    `Routing`,
    `Lasted Created On` AS `Lasted_Created_On`,
    `Source`,
    `Department`
FROM RankedData
WHERE `Rank` = 1
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

# # Gold NPD SKT Prod Final Routing

# CELL ********************

# ============================================================
# FULL REFRESH: vNPD_SktProd_FinalRouting
# Source tables:
#   - Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs
#   - Gold_Product_Dev_Lakehouse.pd.gold_npd_sketch_mapping
#   - Gold_Product_Dev_Lakehouse.pd.gold_npd_prod_status
#   - Gold_Product_Dev_Lakehouse.pd.gold_step_name
# Output: latest routing per SKT across Sketch + Production
# ============================================================

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT_TBL = "Gold_Product_Dev_Lakehouse.pd.gold_npd_skt_prod_final_routing"

src_sql = """
WITH SketchData AS (
    SELECT 
        w.sketch_number_text                    AS `เลข SKT`,
        COALESCE(sm.prod_order_no, '')  AS `เลข PROD`,
        mc.Code                                AS `Routing`,
        CAST(MAX(w.created_on) AS DATE)         AS `Lasted Created On`,
        'Sketch'                                AS `Source`,
        mc.`Sequence`                   AS `Sequence`,
        CASE 
            WHEN sm.prod_order_no LIKE 'WMT%' THEN 'MST'
            WHEN sm.prod_order_no LIKE 'WSP%' THEN 'SP'
            WHEN COALESCE(sm.prod_order_no, '') = '' THEN 'CAD'
            ELSE 'Unknown'
        END                                     AS `Department`
    FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs w
    LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_npd_sketch_mapping sm
        ON w.sketch_number_text = sm.sketch_number
    LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_step_name mc
        ON w.routing_code = mc.Code
    GROUP BY 
        w.sketch_number_text,
        sm.prod_order_no,
        mc.Code,
        mc.`Sequence`
),
ProductionData AS (
    SELECT 
        w.sketch_number_text                    AS `เลข SKT`,
        COALESCE(sm.prod_order_no, '')  AS `เลข PROD`,
        mc.Code                                AS `Routing`,
        CAST(MAX(d.created_on) AS DATE)         AS `Lasted Created On`,
        'Production'                            AS `Source`,
        mc.`Sequence`                   AS `Sequence`,
        CASE 
            WHEN sm.prod_order_no LIKE 'WMT%' THEN 'MST'
            WHEN sm.prod_order_no LIKE 'WSP%' THEN 'SP'
            WHEN COALESCE(sm.prod_order_no, '') = '' THEN 'CAD'
            ELSE 'Unknown'
        END                                     AS `Department`
    FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs w
    LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_npd_sketch_mapping sm
        ON w.sketch_number_text = sm.sketch_number
    LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_npd_prod_status d
        ON sm.prod_order_no = d.`No.`
    LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_step_name mc
        ON d.current_location_code = mc.Code
    WHERE d.type_name = 'In location in'
    GROUP BY 
        w.sketch_number_text,
        sm.prod_order_no,
        mc.Code,
        mc.`Sequence`
),
CombinedData AS (
    SELECT * FROM SketchData
    UNION ALL
    SELECT * FROM ProductionData
),
RankedData AS (
    SELECT 
        `เลข SKT`,
        `เลข PROD`,
        `Routing`,
        `Lasted Created On`,
        `Source`,
        `Sequence`,
        `Department`,
        ROW_NUMBER() OVER (
            PARTITION BY `เลข SKT`
            ORDER BY `Lasted Created On` DESC, `Sequence` DESC
        ) AS `Rank`
    FROM CombinedData
)
SELECT 
    `เลข SKT` AS `เลข_SKT`,
    `เลข PROD` AS `เลข_PROD`,
    `Routing`,
    `Lasted Created On` AS `Lasted_Created_On`,
    `Source`,
    `Department`
FROM RankedData
WHERE `Rank` = 1
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

# # Gold NPD SKT Prod Cut

# CELL ********************

# ============================================================
# FULL REFRESH: apply "rewind" logic by sequence for gold_npd_skt_prod
# Source:
#   - Gold_Product_Dev_Lakehouse.pd.gold_npd_skt_prod
#   - Gold_Product_Dev_Lakehouse.pd.gold_step_name (Sequence)
#
# Output table name (edit if you want):
#   Gold_Product_Dev_Lakehouse.pd.gold_npd_skt_prod_cut
# ============================================================

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT_TBL = "Gold_Product_Dev_Lakehouse.pd.gold_npd_skt_prod_cut"

src_sql = """
WITH
-- 0) Add Sequence by joining Routing to Machine Center
src AS (
    SELECT
        g.`เลข_SKT`            AS `เลข SKT`,
        g.`เลข_PROD`           AS `เลข PROD`,
        g.`Routing`            AS `Routing`,
        g.`Lasted_Created_On`  AS `Lasted Created On`,
        g.`Source`             AS `Source`,
        g.`Department`         AS `Department`,
        mc.`Sequence`  AS `Sequence`
    FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_skt_prod g
    LEFT JOIN Gold_Product_Dev_Lakehouse.pd.gold_step_name mc
        ON g.`Routing` = mc.Code
),

-- 1) Latest date per SKT+Sequence
Base AS (
    SELECT
        `เลข SKT`,
        `Sequence`,
        MAX(`Lasted Created On`) AS MaxDate
    FROM src
    GROUP BY
        `เลข SKT`,
        `Sequence`
),

-- 2) PrevDate by LAG
OrderedSeq AS (
    SELECT
        `เลข SKT`,
        `Sequence`,
        MaxDate,
        LAG(MaxDate) OVER (
            PARTITION BY `เลข SKT`
            ORDER BY `Sequence`
        ) AS PrevDate
    FROM Base
),

-- 3) First rewind point (MaxDate < PrevDate)
ResetPoint AS (
    SELECT
        `เลข SKT`,
        MIN(`Sequence`) AS ResetSeq
    FROM OrderedSeq
    WHERE
        PrevDate IS NOT NULL
        AND MaxDate < PrevDate
    GROUP BY `เลข SKT`
),

-- 4) Compute last allowed sequence
ValidSeq AS (
    SELECT
        b.`เลข SKT`,
        CASE
            WHEN rp.ResetSeq IS NOT NULL THEN rp.ResetSeq - 1
            ELSE MAX(b.`Sequence`)
        END AS LastSeq
    FROM Base b
    LEFT JOIN ResetPoint rp
        ON b.`เลข SKT` = rp.`เลข SKT`
    GROUP BY
        b.`เลข SKT`,
        rp.ResetSeq
)

-- 5) Final rows where Sequence <= LastSeq
SELECT
    s.`เลข SKT` AS `เลข_SKT`,
    s.`เลข PROD` AS `เลข_PROD`,
    s.`Routing`,
    s.`Lasted Created On` AS `Lasted_Created_On`,
    s.`Source`,
    s.`Department`,
    s.`Sequence`  AS `Sequence`
FROM src s
INNER JOIN ValidSeq vs
    ON s.`เลข SKT`   = vs.`เลข SKT`
   AND s.`Sequence` <= vs.LastSeq
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

# # Gold NPD SO INV

# CELL ********************

# ============================================================
# FULL REFRESH Gold table: gold_npd_so_inv
# Converted from SQL (vNPD_SoInv)
#
# Source:
#   - Silver_Finance_Lakehouse.fa.silver_sales_invoice_header_line  (inv)
#   - Gold_Customer_Exp_Lakehouse.cx.gold_sales_order_shipment_line (ship)
#
# Target:
#   - Gold_Product_Dev_Lakehouse.pd.gold_npd_so_inv
# ============================================================

from pyspark.sql import functions as F

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT  = "Gold_Product_Dev_Lakehouse.pd.gold_npd_so_inv"
INV  = "Silver_Finance_Lakehouse.fa.silver_sales_invoice_header_line"
SHIP = "Gold_Customer_Exp_Lakehouse.cx.gold_sales_order_shipment_line"

inv = spark.table(INV).alias("inv")
ship = spark.table(SHIP).alias("ship")

df = (
    inv.join(
        ship,
        (F.col("inv.salesorder_no") == F.col("ship.SalesorderNo")) &
        (F.col("inv.salesorder_lineno") == F.col("ship.LineNo")),
        "left"
    )
    .select(
        F.col("inv.salesorder_no").alias("SalesorderNo"),
        F.col("inv.salesorder_lineno").alias("SalesorderLineNo"),
        F.col("inv.shipment_no").alias("ShipmentNo"),
        F.col("inv.shipment_lineno").alias("ShipmentLineNo"),
        F.col("inv.invoice_no").alias("InvoiceNo"),
        F.col("inv.invoice_lineno").alias("InvoiceLineNo"),

        F.col("ship.Order_Date").alias("Order_Date"),
        F.col("ship.Requested_Delivery_Date").alias("Requested_Delivery_Date"),
        F.col("ship.Promised_Delivery_Date").alias("Promised_Delivery_Date"),

        F.col("inv.invoice_posting_date").alias("PostingDate"),
        F.col("inv.customer_no").alias("CustomerNo"),

        F.lit(None).cast("string").alias("CustomerName"),  # not available in new invoice table

        F.col("inv.item_no").alias("ItemFG"),
        F.col("inv.item_description").alias("Description"),
        F.col("inv.item_quantity").alias("Quantity"),

        F.col("ship.Qty_to_Ship").alias("Qty_to_Ship"),
        F.col("ship.QtyShip").alias("QtyShip"),
        F.col("ship.Qty_to_Invoice").alias("Qty_to_Invoice"),
        F.col("ship.QtyINV").alias("QtyINV"),

        F.col("inv.item_uom").alias("UOM"),
    )
)

# FULL REFRESH
(
    df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(TGT)
)

print(f"FULL REFRESH completed: {TGT}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# #

# MARKDOWN ********************

# # Gold NPD Prod With Cus Name

# CELL ********************

# ============================================================
# FULL REFRESH Gold table: gold_npd_prodwithcusname
# Converted from SQL view: vNPD_prodwithcusname
#
# Source:
#   - Silver_BC_Lakehouse.bc.Production Order   (p)
#   - Silver_BC_Lakehouse.bc.Sales Header      (s)
#
# Target:
#   - Gold_Product_Dev_Lakehouse.pd.gold_npd_prodwithcusname
# ============================================================

from pyspark.sql import functions as F

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT = "Gold_Product_Dev_Lakehouse.pd.gold_npd_prod_with_cus_name"

PROD = "Silver_BC_Lakehouse.bc.`Production Order`"
SALES = "Silver_BC_Lakehouse.bc.`Sales Header`"

p = spark.table(PROD).alias("p")
s = spark.table(SALES).alias("s")

df = (
    p.join(
        s,
        F.col("p.`Sales Order No.`") == F.col("s.`No.`"),
        "left"
    )
    .select(
        F.col("p.`Status`").alias("Status"),
        F.col("p.`No.`").alias("No"),
        F.col("p.`Description`").alias("Description"),
        F.col("p.`Due Date`").alias("Due_Date"),
        F.col("p.`Finished Date`").alias("Finished_Date"),
        F.col("p.`Quantity`").alias("Quantity"),
        F.col("p.`Sales Order No.`").alias("Sales_Order_No"),
        F.col("p.`Sales Order Line No.`").alias("Sales_Order_Line_No"),
        F.col("p.`Prod. Order Type.`").alias("Prod_Order_Type"),

        F.col("s.`Sell-to Customer No.`").alias("Sell_to_Customer_No"),
        F.col("s.`Sell-to Customer Name`").alias("Sell_to_Customer_Name"),
    )
)

# FULL REFRESH write
(
    df.write
      .format("delta")
      .mode("overwrite")               # ✅ Full Replace
      .option("overwriteSchema", "true")
      .saveAsTable(TGT)
)

print(f"FULL REFRESH completed: {TGT}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold NPD SKT INV

# CELL ********************

# ============================================================
# FULL REFRESH Gold table: gold_npd_sktinv
# Converted from SQL view: vNPD_SKTINV
#
# Source:
#   - Gold_Product_Dev_Lakehouse.pd.gold_npd_sketch_mapping (sm)
#   - Gold_Product_Dev_Lakehouse.pd.gold_npd_so_inv         (so)
#
# Target:
#   - Gold_Product_Dev_Lakehouse.pd.gold_npd_sktinv
# ============================================================

from pyspark.sql import functions as F

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT = "Gold_Product_Dev_Lakehouse.pd.gold_npd_sktinv"
SM  = "Gold_Product_Dev_Lakehouse.pd.gold_npd_sketch_mapping"
SO  = "Gold_Product_Dev_Lakehouse.pd.gold_npd_so_inv"

sm = spark.table(SM).alias("sm")
so = spark.table(SO).alias("so")

df = (
    sm.join(
        so,
        (F.col("sm.merged_so") == F.col("so.SalesorderNo")) &
        (F.col("sm.item_no_name") == F.col("so.ItemFG")),
        "left"
    )
    .select(
        F.col("sm.sketch_number").alias("SketchNumber"),
        F.col("sm.sketch_created_on").alias("SketchCreatedOn"),
        F.col("sm.sketch_modified_on").alias("SketchModifiedOn"),
        F.col("sm.merged_so").alias("MergedSO"),
        F.col("sm.merged_prod").alias("MergedProd"),
        F.col("sm.prod_order_type").alias("ProdOrderType"),

        F.col("sm.item_no_name").alias("ItemFG"),
        F.col("sm.item_description").alias("ItemDescription"),
        F.col("sm.item_quantity").alias("ItemQty"),
        F.col("sm.item_base_unit_of_measure").alias("ItemUOM"),
        F.col("sm.customer_number").alias("CustomerNo1"),

        F.col("so.CustomerNo").alias("CustomerNo"),
        F.col("so.CustomerName").alias("CustomerName"),
        F.col("so.ShipmentNo").alias("ShipmentNo"),
        F.col("so.ShipmentLineNo").alias("ShipmentLineNo"),
        F.col("so.InvoiceNo").alias("InvoiceNo"),
        F.col("so.InvoiceLineNo").alias("InvoiceLineNo"),
        F.col("so.Order_Date").alias("Order_Date"),
        F.col("so.Requested_Delivery_Date").alias("Requested_Delivery_Date"),
        F.col("so.Promised_Delivery_Date").alias("Promised_Delivery_Date"),
        F.col("so.PostingDate").alias("InvoicePostingDate"),
        F.col("so.Quantity").alias("InvQuantity"),
        F.col("so.UOM").alias("InvUOM"),
        F.col("so.Qty_to_Ship").alias("Qty_to_Ship"),
        F.col("so.QtyShip").alias("QtyShip"),
        F.col("so.Qty_to_Invoice").alias("Qty_to_Invoice"),
        F.col("so.QtyINV").alias("QtyINV"),
    )
    .dropDuplicates()  # matches SELECT DISTINCT
)

# FULL REFRESH
(
    df.write
      .format("delta")
      .mode("overwrite")               # ✅ Full Replace
      .option("overwriteSchema", "true")
      .saveAsTable(TGT)
)

print(f"FULL REFRESH completed: {TGT}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold NPD Current Report by CUS

# CELL ********************

# ============================================================
# FULL REFRESH Gold table: gold_npd_current_report_by_cus
# Converted from SQL view: vNPD_CurrentReportbyCUS
#
# Key logic:
#   - Latest worklog per cad.sketch_number (ORDER BY created_on DESC)  -> logLatest
#   - LEFT join sketch mapping -> sn
#   - LEFT join prod with customer name -> prod
#   - Filter: prod.No LIKE 'SP%' OR prod.No IS NULL
#
# Source:
#   - Gold_Product_Dev_Lakehouse.pd.gold_cad_requests
#   - Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs
#   - Gold_Product_Dev_Lakehouse.pd.gold_npd_sketch_mapping
#   - Gold_Product_Dev_Lakehouse.pd.gold_npd_prodwithcusname   (your last table)
#
# Target:
#   - Gold_Product_Dev_Lakehouse.pd.gold_npd_current_report_by_cus
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT  = "Gold_Product_Dev_Lakehouse.pd.gold_npd_current_report_by_cus"
CAD  = "Gold_Product_Dev_Lakehouse.pd.gold_cad_requests"
WLOG = "Gold_Product_Dev_Lakehouse.pd.gold_npd_worklogs"
SN   = "Gold_Product_Dev_Lakehouse.pd.gold_npd_sketch_mapping"
PROD = "Gold_Product_Dev_Lakehouse.pd.gold_npd_prod_with_cus_name"

cad  = spark.table(CAD).alias("cad")
wl   = spark.table(WLOG).alias("l")
sn   = spark.table(SN).alias("sn")
prod = spark.table(PROD).alias("prod")

# -----------------------------
# 1) logLatest = latest worklog per sketch_number_text (created_on desc)
# -----------------------------
w_latest = Window.partitionBy("sketch_number_text").orderBy(F.col("created_on").desc())

logLatest = (
    wl.select(
        F.col("sketch_number_text"),
        F.col("routing_code"),
        F.col("created_on")
    )
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .alias("logLatest")
)

# -----------------------------
# 2) Build final dataset
# -----------------------------
df = (
    cad.join(
        logLatest,
        F.col("cad.sketch_number") == F.col("logLatest.sketch_number_text"),
        "left"
    )
    .join(
        sn,
        F.col("cad.sketch_number") == F.col("sn.sketch_number"),
        "left"
    )
    .join(
        prod,
        F.col("sn.prod_order_no") == F.col("prod.No"),
        "left"
    )
    .filter(
        (F.col("prod.No").like("SP%")) | (F.col("prod.No").isNull())
    )
    .select(
        F.col("cad.customer_request_date"),
        F.col("cad.collection_id_name"),
        F.col("cad.sketch_number"),
        F.col("cad.customer_design_number"),
        F.col("cad.cropped_design_image_url"),
        F.col("cad.submission_date"),
        F.col("cad.promised_date"),

        F.when(F.col("logLatest.routing_code").isin("CAD PRODUCTION READY","3D PRINTING","RECEIVE WAX"), F.lit("COMPLETED"))
         .when(F.col("logLatest.routing_code") == F.lit("WAITING FOR CUSTOMER APPROVAL"), F.lit("CUSTOMER PENDING APPROVAL"))
         .otherwise(F.col("logLatest.routing_code"))
         .alias("RoutingStepName"),

        F.col("logLatest.created_on"),

        F.col("prod.No"),
        F.col("prod.Status"),

        F.when(
            F.col("prod.No").like("SP%"),
            F.when(
                F.lower(F.col("prod.Status")).like("%closed%") | F.lower(F.col("prod.Status")).like("%cancelled%"),
                F.lit("FINISHED")
            ).otherwise(F.lit("PENDING"))
        ).otherwise(F.lit("")).alias("SP_ProductionStatus"),

        F.col("sn.item_no_name"),
        F.col("sn.item_quantity"),
    )
    .dropDuplicates()  # SELECT DISTINCT
)

# -----------------------------
# 3) FULL REFRESH write
# -----------------------------
(
    df.write
      .format("delta")
      .mode("overwrite")               # ✅ Full Replace
      .option("overwriteSchema", "true")
      .saveAsTable(TGT)
)

print(f"FULL REFRESH completed: {TGT}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Posted Sales Inv Amount SH SO

# CELL ********************

# ============================================================
# FULL REFRESH Gold table: gold_posted_salesinv_amount_sh_so
# Converted from SQL view: PostedSalesInv_Amount_SH_SO
#
# Source:
#   - Gold_Customer_Exp_Lakehouse.cx.gold_posted_sales_inv_amount_sh           (inv)
#   - Gold_Customer_Exp_Lakehouse.cx.gold_posted_sales_shipment_amount_used   (sh)
#
# Target:
#   - Gold_Customer_Exp_Lakehouse.cx.gold_posted_salesinv_amount_sh_so
# ============================================================

from pyspark.sql import functions as F

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT = "Gold_Customer_Exp_Lakehouse.cx.gold_posted_sales_inv_amount_sh_so"
INV = "Gold_Customer_Exp_Lakehouse.cx.gold_posted_sales_inv_amount_sh"
SH  = "Gold_Customer_Exp_Lakehouse.cx.gold_posted_sales_shipment_amount_used"

inv = spark.table(INV).alias("inv")
sh  = spark.table(SH).alias("sh")

df = (
    inv.join(
        sh,
        F.col("inv.ShipmentNo") == F.col("sh.shNo"),
        "left"
    )
    .filter(F.col("inv.Quantity") != F.lit(0))
    .select(
        F.lit(None).cast("decimal(38,10)").alias("Unit_Cost_LCY"),
        F.lit(None).cast("decimal(38,10)").alias("Unit_Price"),
        F.col("inv.CurrencyCode").alias("Currency_Code"),
        F.lit(None).cast("decimal(38,10)").alias("relationalExchRateAmount"),
        F.lit(None).cast("decimal(38,10)").alias("amountTHB"),
        F.lit(None).cast("string").alias("Salesperson_Code"),

        F.col("inv.SellToCustomerName").alias("customer"),
        F.col("inv.DocumentNo").alias("invNo"),
        F.col("inv.UnitOfMeasureCode").alias("UOM"),
        F.col("inv.LineNo").alias("Line_No"),

        F.lit(None).cast("date").alias("Document_Date"),
        F.col("inv.Due_Date").alias("Due_Date"),
        F.col("inv.PostingDate").alias("Posting_Date"),
        F.lit(None).cast("date").alias("Shipment_Date"),

        F.lit(None).cast("decimal(38,10)").alias("totalqtyTHB"),
        F.lit(None).cast("string").alias("Gen_Prod_Posting_Group"),

        F.col("inv.Item_No").alias("itemFG"),
        F.col("inv.Description").alias("Description"),
        F.col("inv.Quantity").alias("Quantity"),

        F.col("inv.ShipmentNo").alias("shNo_inv"),
        F.col("sh.shNo").alias("shNo"),

        F.col("inv.Sales_Order_No").alias("Order_No"),
        F.col("sh.Order_No").alias("so"),
    )
    .dropDuplicates()  # SELECT DISTINCT
)

# FULL REFRESH
(
    df.write
      .format("delta")
      .mode("overwrite")               # ✅ Full Replace
      .option("overwriteSchema", "true")
      .saveAsTable(TGT)
)

print(f"FULL REFRESH completed: {TGT}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold NPD FSP Ship

# CELL ********************

# ============================================================
# FULL REFRESH Gold table: gold_npd_fsp_ship
# Converted from SQL view: vNPD_fSPShip
#
# Source:
#   - Gold_Product_Dev_Lakehouse.pd.gold_npd_sketch_mapping               (sk)
#   - Gold_Customer_Exp_Lakehouse.cx.gold_posted_salesinv_amount_sh_so    (sp)
#
# Target:
#   - Gold_Product_Dev_Lakehouse.pd.gold_npd_fsp_ship
# ============================================================

from pyspark.sql import functions as F

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT = "Gold_Product_Dev_Lakehouse.pd.gold_npd_fsp_ship"
SK  = "Gold_Product_Dev_Lakehouse.pd.gold_npd_sketch_mapping"
SP  = "Gold_Customer_Exp_Lakehouse.cx.gold_posted_sales_inv_amount_sh_so"

sk = spark.table(SK).alias("sk")
sp = spark.table(SP).alias("sp")

df = (
    sk.join(
        sp,
        (F.col("sk.merged_so") == F.col("sp.so")) &
        (F.col("sk.item_no_name") == F.col("sp.itemFG")),
        "inner"
    )
    .select(
        F.col("sk.merged_prod").alias("MergedProd"),
        F.col("sk.sales_order_line_no"),
        F.col("sk.prod_order_type"),
        F.col("sk.item_quantity"),
        F.col("sk.item_base_unit_of_measure"),
        F.col("sk.customer_number"),
        F.col("sk.sales_order_no"),
        F.col("sk.so"),
        F.col("sk.merged_so").alias("MergedSO"),
        F.col("sk.sketch_created_on"),
        F.col("sk.sketch_modified_on"),
        F.col("sk.production_order_name"),
        F.col("sk.production_order_line_no_name"),
        F.col("sk.product_order_line_no"),
        F.col("sk.item_description"),


        F.col("sk.customer_approval_id"),
        F.col("sk.production_order_id_name"),
        F.col("sk.order_type_name"),
        F.col("sk.item_no_name"),
        F.col("sk.sketch_number"),

        F.col("sp.invNo").alias("invNo"),
        F.col("sp.shNo").alias("shNo"),
        F.col("sp.shNo_inv").alias("shNo_inv"),
        F.col("sp.Posting_Date").alias("Posting_Date"),
        F.col("sp.customer").alias("customer"),
        F.col("sp.Quantity").alias("Quantity"),
    )
    .dropDuplicates()  # SELECT DISTINCT
)

# FULL REFRESH
(
    df.write
      .format("delta")
      .mode("overwrite")               # ✅ Full Replace
      .option("overwriteSchema", "true")
      .saveAsTable(TGT)
)

print(f"FULL REFRESH completed: {TGT}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Spark SQL (Databricks) version
# MAGIC -- Creates a Delta table from your final UNION result.
# MAGIC -- Adjust catalog/schema/table name as you want.
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Product_Dev_Lakehouse.pd.gold_pd_department_step_map
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH
# MAGIC /* =========================
# MAGIC    CAD (from table)
# MAGIC ========================= */
# MAGIC cad AS (
# MAGIC   SELECT
# MAGIC     'CAD' AS Department,
# MAGIC     CAST(s.pd_step AS STRING) AS `Machine center`,
# MAGIC     CAST(s.pd_step AS STRING) AS `Work center`,
# MAGIC     CASE CAST(s.pd_step AS STRING)
# MAGIC       WHEN 'BACKLOG'                       THEN 'BKLG'
# MAGIC       WHEN 'CAD ASSIGNED TO ENGINEER'      THEN 'CAD ASGN'
# MAGIC       WHEN 'CAD IN PROGRESS'               THEN 'CAD PROG'
# MAGIC       WHEN 'WAITING FOR MANAGER APPROVAL'  THEN 'MNG APP'
# MAGIC       WHEN 'WAITING FOR CUSTOMER APPROVAL' THEN 'CUS APP'
# MAGIC       WHEN 'CUSTOMER REVISION'             THEN 'CUS REV'
# MAGIC       WHEN 'CAD PRODUCTION READY'          THEN 'CAD READY'
# MAGIC       WHEN '3D PRINTING'                   THEN '3D PRINT'
# MAGIC       WHEN 'RECEIVE WAX'                   THEN 'WAX CAD'
# MAGIC       ELSE CAST(s.pd_step AS STRING)
# MAGIC     END AS MAP,
# MAGIC     CAST(s.pd_sequence AS INT) AS Sequence,
# MAGIC     CONCAT('CAD', CAST(s.pd_step AS STRING)) AS `KEY`,
# MAGIC     CASE CAST(s.pd_step AS STRING)
# MAGIC       WHEN 'BACKLOG'                       THEN 'BKLG'
# MAGIC       WHEN 'CAD ASSIGNED TO ENGINEER'      THEN 'ASGN'
# MAGIC       WHEN 'CAD IN PROGRESS'               THEN 'PROG'
# MAGIC       WHEN 'WAITING FOR MANAGER APPROVAL'  THEN 'MNG APP'
# MAGIC       WHEN 'WAITING FOR CUSTOMER APPROVAL' THEN 'CUS APP'
# MAGIC       WHEN 'CUSTOMER REVISION'             THEN 'CUS REV'
# MAGIC       WHEN 'CAD PRODUCTION READY'          THEN 'READY'
# MAGIC       WHEN '3D PRINTING'                   THEN '3D PT'
# MAGIC       WHEN 'RECEIVE WAX'                   THEN 'CAD WAX'
# MAGIC       ELSE CAST(s.pd_step AS STRING)
# MAGIC     END AS `KEY - Copy`
# MAGIC   FROM Silver_Product_Dev_Lakehouse.pd.silver_pd_step s
# MAGIC   WHERE s.pd_step IS NOT NULL
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    Machine Center base (for SP)
# MAGIC ========================= */
# MAGIC mc AS (
# MAGIC   SELECT
# MAGIC     CAST(`Department Group` AS STRING)  AS DeptGroup,
# MAGIC     CAST(`No.` AS STRING)               AS MachineCenter,
# MAGIC     CAST(`Work Center No.` AS STRING)   AS WorkCenter,
# MAGIC     CAST(`Operation Group` AS STRING)   AS OpGroup
# MAGIC   FROM Silver_BC_Lakehouse.bc.`Machine Center`
# MAGIC   WHERE COALESCE(CAST(Blocked AS INT), 0) = 0
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    MST header (Routing: MST ∙ Master Silver)
# MAGIC ========================= */
# MAGIC mst_hdr AS (
# MAGIC   SELECT
# MAGIC     h.`No.`          AS RoutingNo,
# MAGIC     h.`Version Nos.` AS VersionCode
# MAGIC   FROM Silver_BC_Lakehouse.bc.`Routing Header` h
# MAGIC   WHERE
# MAGIC     h.`No.` = 'MST'
# MAGIC     AND (
# MAGIC       h.`Description` = 'Master Silver'
# MAGIC       OR h.`Search Description` = 'MASTER SILVER'
# MAGIC     )
# MAGIC   ORDER BY
# MAGIC     to_timestamp(CAST(h.`Last Date Modified` AS STRING)) DESC,
# MAGIC     to_timestamp(CAST(h.`SystemModifiedAt`   AS STRING)) DESC
# MAGIC   LIMIT 1
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    MST lines (Routing Line)
# MAGIC ========================= */
# MAGIC mst_picked AS (
# MAGIC   SELECT
# MAGIC     'MST' AS Department,
# MAGIC     CAST(rl.`No.` AS STRING) AS `Machine center`,
# MAGIC     CAST(COALESCE(rl.`Work Center No.`, rl.`No.`) AS STRING) AS `Work center`,
# MAGIC     CASE
# MAGIC       WHEN rl.`No.` = 'WH2F'               AND rl.`Type` = 'Work Center'    THEN 'WAREHOUSE'
# MAGIC       WHEN rl.`No.` = 'PD ADMIN CENTER'    AND rl.`Type` = 'Work Center'    THEN 'ADM'
# MAGIC       WHEN rl.`No.` = 'CASTING'            AND rl.`Type` = 'Machine Center' THEN 'CASTING'
# MAGIC       WHEN rl.`No.` = 'FILING MASTER'      AND rl.`Type` = 'Machine Center' THEN 'FIL'
# MAGIC       WHEN rl.`No.` = 'MASTER PRE SETTING' AND rl.`Type` = 'Machine Center' THEN 'SET'
# MAGIC       WHEN rl.`No.` = 'LASER ENGRAVING'    AND rl.`Type` = 'Machine Center' THEN 'LAS'
# MAGIC       WHEN rl.`No.` = 'SPRUE'              AND rl.`Type` = 'Machine Center' THEN 'SPR'
# MAGIC       WHEN rl.`No.` = 'WAX ROOM'           AND rl.`Type` = 'Machine Center' THEN 'R. MOLD'
# MAGIC     END AS MAP,
# MAGIC     CASE
# MAGIC       WHEN rl.`No.` = 'WH2F'               AND rl.`Type` = 'Work Center'    THEN  9
# MAGIC       WHEN rl.`No.` = 'PD ADMIN CENTER'    AND rl.`Type` = 'Work Center'    THEN 10
# MAGIC       WHEN rl.`No.` = 'CASTING'            AND rl.`Type` = 'Machine Center' THEN 11
# MAGIC       WHEN rl.`No.` = 'FILING MASTER'      AND rl.`Type` = 'Machine Center' THEN 12
# MAGIC       WHEN rl.`No.` = 'MASTER PRE SETTING' AND rl.`Type` = 'Machine Center' THEN 13
# MAGIC       WHEN rl.`No.` = 'LASER ENGRAVING'    AND rl.`Type` = 'Machine Center' THEN 14
# MAGIC       WHEN rl.`No.` = 'SPRUE'              AND rl.`Type` = 'Machine Center' THEN 15
# MAGIC       WHEN rl.`No.` = 'WAX ROOM'           AND rl.`Type` = 'Machine Center' THEN 16
# MAGIC     END AS Sequence,
# MAGIC     CONCAT('MST', CAST(COALESCE(rl.`Work Center No.`, rl.`No.`) AS STRING)) AS `KEY`,
# MAGIC     CASE
# MAGIC       WHEN rl.`No.` = 'WH2F'               AND rl.`Type` = 'Work Center'    THEN 'WH MST'
# MAGIC       WHEN rl.`No.` = 'PD ADMIN CENTER'    AND rl.`Type` = 'Work Center'    THEN 'ADN MST'
# MAGIC       WHEN rl.`No.` = 'CASTING'            AND rl.`Type` = 'Machine Center' THEN 'CAS MST'
# MAGIC       WHEN rl.`No.` = 'FILING MASTER'      AND rl.`Type` = 'Machine Center' THEN 'FL MST'
# MAGIC       WHEN rl.`No.` = 'MASTER PRE SETTING' AND rl.`Type` = 'Machine Center' THEN 'P-SET'
# MAGIC       WHEN rl.`No.` = 'LASER ENGRAVING'    AND rl.`Type` = 'Machine Center' THEN 'ENGRV'
# MAGIC       WHEN rl.`No.` = 'SPRUE'              AND rl.`Type` = 'Machine Center' THEN 'SPR'
# MAGIC       WHEN rl.`No.` = 'WAX ROOM'           AND rl.`Type` = 'Machine Center' THEN 'R-MOLD'
# MAGIC     END AS `KEY - Copy`
# MAGIC   FROM Silver_BC_Lakehouse.bc.`Routing Line` rl
# MAGIC   INNER JOIN mst_hdr h
# MAGIC     ON rl.`Routing No.` = h.RoutingNo
# MAGIC    AND (rl.`Version Code` = h.VersionCode OR NULLIF(h.VersionCode,'') IS NULL)
# MAGIC   WHERE
# MAGIC     (rl.`Type` = 'Work Center'    AND rl.`No.` IN ('WH2F','PD ADMIN CENTER'))
# MAGIC     OR
# MAGIC     (rl.`Type` = 'Machine Center' AND rl.`No.` IN ('CASTING','FILING MASTER','MASTER PRE SETTING','LASER ENGRAVING','SPRUE','WAX ROOM'))
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    SP targets (inline table)
# MAGIC ========================= */
# MAGIC sp_targets AS (
# MAGIC   SELECT * FROM VALUES
# MAGIC     ('SP',  17,  'WH2F',        'WAREHOUSE',     'WH',      'WH SP'),
# MAGIC     ('SP',  18,  'PROD ADMIN',  NULL,            'ADM',     'ADN SP'),
# MAGIC     ('SP',  19,  'CASTING',     'CASTING ROOM',  'CASTING', 'CAS SP'),
# MAGIC     ('SP',  20,  'FILING',      'FILING',        'FIL',     'FIL'),
# MAGIC     ('SP',  21,  'SETTING',     'SETTING',       'SET',     'SET'),
# MAGIC     ('SP',  22,  'POLISHING',   'POLISHING',     'POL',     'POL'),
# MAGIC     ('SP',  23,  'PLATING',     'PLATING ROOM',  'PLT',     'PLAT'),
# MAGIC     ('SP',  24,  'QA',          'QA ROOM',       'QA',      'QA')
# MAGIC   AS v(Department, Sequence, NeedMachine, NeedWork, MAP, KeyCopy)
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    OUTER APPLY equivalent:
# MAGIC    pick TOP 1 mc row per target using row_number()
# MAGIC ========================= */
# MAGIC sp_candidates AS (
# MAGIC   SELECT
# MAGIC     t.Department,
# MAGIC     t.Sequence,
# MAGIC     t.NeedMachine,
# MAGIC     t.NeedWork,
# MAGIC     t.MAP,
# MAGIC     t.KeyCopy,
# MAGIC     m.MachineCenter,
# MAGIC     m.WorkCenter,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY t.Department, t.Sequence, t.NeedMachine, t.NeedWork, t.MAP, t.KeyCopy
# MAGIC       ORDER BY
# MAGIC         CASE WHEN t.NeedMachine IS NOT NULL AND m.MachineCenter = t.NeedMachine THEN 0 ELSE 1 END,
# MAGIC         CASE WHEN t.NeedWork    IS NOT NULL AND m.WorkCenter    = t.NeedWork    THEN 0 ELSE 1 END
# MAGIC     ) AS rn
# MAGIC   FROM sp_targets t
# MAGIC   LEFT JOIN mc m
# MAGIC     ON m.DeptGroup IN ('PRODUCTION','QUALITY','CASTING','WAREHOUSE')
# MAGIC    AND (t.NeedMachine IS NULL OR m.MachineCenter = t.NeedMachine)
# MAGIC    AND (t.NeedWork    IS NULL OR m.WorkCenter    = t.NeedWork)
# MAGIC ),
# MAGIC 
# MAGIC sp_picked AS (
# MAGIC   SELECT
# MAGIC     Department,
# MAGIC     MachineCenter AS `Machine center`,
# MAGIC     WorkCenter    AS `Work center`,
# MAGIC     MAP,
# MAGIC     Sequence,
# MAGIC     CONCAT(Department, COALESCE(WorkCenter, MachineCenter)) AS `KEY`,
# MAGIC     KeyCopy AS `KEY - Copy`
# MAGIC   FROM sp_candidates
# MAGIC   WHERE rn = 1
# MAGIC )
# MAGIC 
# MAGIC /* =========================
# MAGIC    FINAL
# MAGIC ========================= */
# MAGIC SELECT Department, `Machine center`, `Work center`, MAP, Sequence, `KEY`, `KEY - Copy` FROM cad
# MAGIC UNION ALL
# MAGIC SELECT Department, `Machine center`, `Work center`, MAP, Sequence, `KEY`, `KEY - Copy` FROM mst_picked
# MAGIC UNION ALL
# MAGIC SELECT Department, `Machine center`, `Work center`, MAP, Sequence, `KEY`, `KEY - Copy` FROM sp_picked
# MAGIC ;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
