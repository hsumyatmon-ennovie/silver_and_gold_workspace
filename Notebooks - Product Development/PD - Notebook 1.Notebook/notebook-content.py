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

# # Gold CAD Requests

# CELL ********************

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

SRC_TBL = "Silver_Product_Dev_Lakehouse.pd.silver_cad_requests"
TGT_TBL = "Gold_Product_Dev_Lakehouse.pd.gold_cad_requests"

src_sql = f"""
SELECT
    created_on AS created_on,
    modified_on AS modified_on,

    pd_sketch_item AS sketch_number,
    1 AS qty_skt,

    pd_sketch_item_piority AS priority,
    pd_sketch_item_piority AS priority_name,

    submission_date AS submission_date,
    promised_date AS customer_request_date,

    WEEKOFYEAR(promised_date) AS cus_req,
    WEEKOFYEAR(promised_date) AS cus_week,

    CASE
        WHEN WEEKOFYEAR(promised_date) - 2 > 0
            THEN WEEKOFYEAR(promised_date) - 2
        ELSE 52 + (WEEKOFYEAR(promised_date) - 2)
    END AS cad_week,

    sketch_image AS cropped_design_image_url,
    dts_status AS status_name,

    CAST(est_for_development AS DECIMAL(18,4)) AS estimated_development_time,
    pd_sketch_collection AS collection_id_name,

    customer_name AS customer_name,

    CAST(est_for_prod AS DECIMAL(18,4)) AS estimated_production_ready_time,
    commited_week AS committed_week,

    customer_design_number AS customer_design_number,

    repair_comment AS reopen_sketch_number,
    cs_comment AS comments,

    additional_image AS additional_photo_url,
    is_archive AS is_archive_name,

    CAST(customer_target_price AS DECIMAL(18,4)) AS customer_target_price,
    created_by_email AS created_by_email,

    cs_noted AS status_description,

    CAST(customer_target_price AS DECIMAL(18,4)) AS target_customer_price,

    -- ✅ Engineer Name Replacement Mapping
    CASE
        WHEN engineer_name LIKE '%Savitree%' THEN 'NAM-CAD'
        WHEN engineer_name LIKE '%Sitanant%' THEN 'YEL-CAD'
        WHEN engineer_name LIKE '%Jaruwan%' THEN 'AE-CAD'
        WHEN engineer_name LIKE '%Kanhathai%' THEN 'AOM-CAD'
        WHEN engineer_name LIKE '%Suttiluk%' THEN 'TIW-CAD'
        WHEN engineer_name LIKE '%Thipsaeng%' THEN 'YEL-CAD'
        WHEN engineer_name LIKE '%Phachiraphithayakul%' THEN 'NAM-CAD'
        WHEN engineer_name LIKE '%Kitjao%' THEN 'AE-CAD'
        WHEN engineer_name LIKE '%Rinthong%' THEN 'TIW-CAD'
        WHEN engineer_name LIKE '%Nawapun%' THEN 'CHOM-CAD'
        WHEN engineer_name LIKE '%Numprai%' THEN 'AOM-CAD'
        WHEN engineer_name LIKE '%Yodcharin%' THEN 'JO-L'
        WHEN engineer_name LIKE '%Saeid%' THEN 'SAM-CAD'
        WHEN engineer_name LIKE '%Siriwan%' THEN 'PUI-CAD'
        ELSE engineer_name
    END AS assigned_engineer_name,

    promised_date AS promised_date,
    WEEKOFYEAR(promised_date) AS pro_req,

    cs_description AS description,

    CASE
        WHEN DATE_SUB(promised_date, 14) < CURRENT_DATE()
            THEN 'Overdue'
        ELSE 'On Schedule'
    END AS status

FROM {SRC_TBL}
"""

df_src = spark.sql(src_sql)

df_src = df_src.dropDuplicates(
    ["sketch_number", "customer_request_date", "promised_date", "modified_on"]
)

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
# MAGIC     SELECT
# MAGIC         'CAD' AS Department,
# MAGIC         CAST(s.pd_step AS STRING) AS `Machine center`,
# MAGIC         CAST(s.pd_step AS STRING) AS `Work center`,
# MAGIC         CASE CAST(s.pd_step AS STRING)
# MAGIC             WHEN 'BACKLOG'                       THEN 'BKLG'
# MAGIC             WHEN 'CAD ASSIGNED TO ENGINEER'      THEN 'CAD ASGN'
# MAGIC             WHEN 'CAD IN PROGRESS'               THEN 'CAD PROG'
# MAGIC             WHEN 'WAITING FOR MANAGER APPROVAL'  THEN 'MNG APP'
# MAGIC             WHEN 'WAITING FOR CUSTOMER APPROVAL' THEN 'CUS APP'
# MAGIC             WHEN 'CUSTOMER REVISION'             THEN 'CUS REV'
# MAGIC             WHEN 'CAD PRODUCTION READY'          THEN 'CAD READY'
# MAGIC             WHEN '3D PRINTING'                   THEN '3D PRINT'
# MAGIC             WHEN 'RECEIVE WAX'                   THEN 'WAX CAD'
# MAGIC             ELSE CAST(s.pd_step AS STRING)
# MAGIC         END AS MAP,
# MAGIC         CAST(s.pd_sequence AS INT) AS Sequence,
# MAGIC         CONCAT('CAD', CAST(s.pd_step AS STRING)) AS `KEY`,
# MAGIC         CASE CAST(s.pd_step AS STRING)
# MAGIC             WHEN 'BACKLOG'                       THEN 'BKLG'
# MAGIC             WHEN 'CAD ASSIGNED TO ENGINEER'      THEN 'ASGN'
# MAGIC             WHEN 'CAD IN PROGRESS'               THEN 'PROG'
# MAGIC             WHEN 'WAITING FOR MANAGER APPROVAL'  THEN 'MNG APP'
# MAGIC             WHEN 'WAITING FOR CUSTOMER APPROVAL' THEN 'CUS APP'
# MAGIC             WHEN 'CUSTOMER REVISION'             THEN 'CUS REV'
# MAGIC             WHEN 'CAD PRODUCTION READY'          THEN 'READY'
# MAGIC             WHEN '3D PRINTING'                   THEN '3D PT'
# MAGIC             WHEN 'RECEIVE WAX'                   THEN 'CAD WAX'
# MAGIC             ELSE CAST(s.pd_step AS STRING)
# MAGIC         END AS `KEY - Copy`
# MAGIC     FROM Silver_Product_Dev_Lakehouse.pd.silver_pd_step s
# MAGIC     WHERE s.pd_step IS NOT NULL
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    Machine Center base (for SP)
# MAGIC ========================= */
# MAGIC mc AS (
# MAGIC     SELECT
# MAGIC         CAST(`Department Group` AS STRING) AS DeptGroup,
# MAGIC         CAST(`No.` AS STRING)              AS MachineCenter,
# MAGIC         CAST(`Work Center No.` AS STRING)  AS WorkCenter,
# MAGIC         CAST(`Operation Group` AS STRING)  AS OpGroup
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Machine Center`
# MAGIC     WHERE COALESCE(CAST(Blocked AS INT), 0) = 0
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    MST header (Routing: MST ∙ Master Silver)
# MAGIC ========================= */
# MAGIC mst_hdr AS (
# MAGIC     SELECT
# MAGIC         h.`No.`          AS RoutingNo,
# MAGIC         h.`Version Nos.` AS VersionCode
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Routing Header` h
# MAGIC     WHERE
# MAGIC         h.`No.` = 'MST'
# MAGIC         AND (
# MAGIC             h.`Description` = 'Master Silver'
# MAGIC             OR h.`Search Description` = 'MASTER SILVER'
# MAGIC         )
# MAGIC     ORDER BY
# MAGIC         to_timestamp(h.`Last Date Modified`) DESC,
# MAGIC         to_timestamp(h.`SystemModifiedAt`) DESC
# MAGIC     LIMIT 1
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    MST lines (Routing Line)
# MAGIC ========================= */
# MAGIC mst_picked AS (
# MAGIC     SELECT
# MAGIC         'MST' AS Department,
# MAGIC         CAST(rl.`No.` AS STRING) AS `Machine center`,
# MAGIC         CAST(COALESCE(rl.`Work Center No.`, rl.`No.`) AS STRING) AS `Work center`,
# MAGIC         CASE
# MAGIC             WHEN rl.`No.` = 'WH2F'               AND rl.`Type` = 'Work Center'    THEN 'WAREHOUSE'
# MAGIC             WHEN rl.`No.` = 'PD ADMIN CENTER'    AND rl.`Type` = 'Work Center'    THEN 'ADM'
# MAGIC             WHEN rl.`No.` = 'CASTING'            AND rl.`Type` = 'Machine Center' THEN 'CASTING'
# MAGIC             WHEN rl.`No.` = 'FILING MASTER'      AND rl.`Type` = 'Machine Center' THEN 'FIL'
# MAGIC             WHEN rl.`No.` = 'MASTER PRE SETTING' AND rl.`Type` = 'Machine Center' THEN 'SET'
# MAGIC             WHEN rl.`No.` = 'LASER ENGRAVING'    AND rl.`Type` = 'Machine Center' THEN 'LAS'
# MAGIC             WHEN rl.`No.` = 'SPRUE'              AND rl.`Type` = 'Machine Center' THEN 'SPR'
# MAGIC             WHEN rl.`No.` = 'WAX ROOM'           AND rl.`Type` = 'Machine Center' THEN 'R. MOLD'
# MAGIC         END AS MAP,
# MAGIC         CASE
# MAGIC             WHEN rl.`No.` = 'WH2F'               AND rl.`Type` = 'Work Center'    THEN  9
# MAGIC             WHEN rl.`No.` = 'PD ADMIN CENTER'    AND rl.`Type` = 'Work Center'    THEN 10
# MAGIC             WHEN rl.`No.` = 'CASTING'            AND rl.`Type` = 'Machine Center' THEN 11
# MAGIC             WHEN rl.`No.` = 'FILING MASTER'      AND rl.`Type` = 'Machine Center' THEN 12
# MAGIC             WHEN rl.`No.` = 'MASTER PRE SETTING' AND rl.`Type` = 'Machine Center' THEN 13
# MAGIC             WHEN rl.`No.` = 'LASER ENGRAVING'    AND rl.`Type` = 'Machine Center' THEN 14
# MAGIC             WHEN rl.`No.` = 'SPRUE'              AND rl.`Type` = 'Machine Center' THEN 15
# MAGIC             WHEN rl.`No.` = 'WAX ROOM'           AND rl.`Type` = 'Machine Center' THEN 16
# MAGIC         END AS Sequence,
# MAGIC         CONCAT('MST', CAST(rl.`No.` AS STRING)) AS `KEY`,
# MAGIC         CASE
# MAGIC             WHEN rl.`No.` = 'WH2F'               AND rl.`Type` = 'Work Center'    THEN 'WH MST'
# MAGIC             WHEN rl.`No.` = 'PD ADMIN CENTER'    AND rl.`Type` = 'Work Center'    THEN 'ADN MST'
# MAGIC             WHEN rl.`No.` = 'CASTING'            AND rl.`Type` = 'Machine Center' THEN 'CAS MST'
# MAGIC             WHEN rl.`No.` = 'FILING MASTER'      AND rl.`Type` = 'Machine Center' THEN 'FL MST'
# MAGIC             WHEN rl.`No.` = 'MASTER PRE SETTING' AND rl.`Type` = 'Machine Center' THEN 'P-SET'
# MAGIC             WHEN rl.`No.` = 'LASER ENGRAVING'    AND rl.`Type` = 'Machine Center' THEN 'ENGRV'
# MAGIC             WHEN rl.`No.` = 'SPRUE'              AND rl.`Type` = 'Machine Center' THEN 'SPR'
# MAGIC             WHEN rl.`No.` = 'WAX ROOM'           AND rl.`Type` = 'Machine Center' THEN 'R-MOLD'
# MAGIC         END AS `KEY - Copy`
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Routing Line` rl
# MAGIC     INNER JOIN mst_hdr h
# MAGIC         ON rl.`Routing No.` = h.RoutingNo
# MAGIC        AND (
# MAGIC             rl.`Version Code` = h.VersionCode
# MAGIC             OR NULLIF(h.VersionCode, '') IS NULL
# MAGIC        )
# MAGIC     WHERE
# MAGIC         (rl.`Type` = 'Work Center'    AND rl.`No.` IN ('WH2F','PD ADMIN CENTER'))
# MAGIC         OR
# MAGIC         (rl.`Type` = 'Machine Center' AND rl.`No.` IN ('CASTING','FILING MASTER','MASTER PRE SETTING','LASER ENGRAVING','SPRUE','WAX ROOM'))
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    SP targets (Machine Center)
# MAGIC ========================= */
# MAGIC sp_targets AS (
# MAGIC     SELECT * FROM VALUES
# MAGIC         ('SP',  17,  'WH2F',       'WAREHOUSE',     'WH',      'WH SP'),
# MAGIC         ('SP',  18,  'PROD ADMIN', NULL,            'ADM',     'ADN SP'),
# MAGIC         ('SP',  19,  'CASTING',    'CASTING ROOM',  'CASTING', 'CAS SP'),
# MAGIC         ('SP',  20,  'FILING',     'FILING',        'FIL',     'FIL'),
# MAGIC         ('SP',  21,  'SETTING',    'SETTING',       'SET',     'SET'),
# MAGIC         ('SP',  22,  'POLISHING',  'POLISHING',     'POL',     'POL'),
# MAGIC         ('SP',  23,  'PLATING',    'PLATING ROOM',  'PLT',     'PLAT'),
# MAGIC         ('SP',  24,  'QA',         'QA ROOM',       'QA',      'QA')
# MAGIC     AS v(Department, Sequence, NeedMachine, NeedWork, MAP, KeyCopy)
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    SP candidates (JOIN + WINDOW instead of OUTER APPLY)
# MAGIC ========================= */
# MAGIC sp_candidates AS (
# MAGIC     SELECT
# MAGIC         t.Department,
# MAGIC         t.Sequence,
# MAGIC         t.NeedMachine,
# MAGIC         t.NeedWork,
# MAGIC         t.MAP,
# MAGIC         t.KeyCopy,
# MAGIC         m.MachineCenter,
# MAGIC         m.WorkCenter,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY t.Department, t.Sequence
# MAGIC             ORDER BY
# MAGIC                 CASE WHEN t.NeedMachine IS NOT NULL AND m.MachineCenter = t.NeedMachine THEN 0 ELSE 1 END,
# MAGIC                 CASE WHEN t.NeedWork    IS NOT NULL AND m.WorkCenter    = t.NeedWork    THEN 0 ELSE 1 END
# MAGIC         ) AS rn
# MAGIC     FROM sp_targets t
# MAGIC     LEFT JOIN mc m
# MAGIC         ON m.DeptGroup IN ('PRODUCTION','QUALITY','CASTING','WAREHOUSE')
# MAGIC        AND (t.NeedMachine IS NULL OR m.MachineCenter = t.NeedMachine)
# MAGIC        AND (t.NeedWork    IS NULL OR m.WorkCenter    = t.NeedWork)
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    SP picked
# MAGIC ========================= */
# MAGIC sp_picked AS (
# MAGIC     SELECT
# MAGIC         Department,
# MAGIC         MachineCenter AS `Machine center`,
# MAGIC         WorkCenter    AS `Work center`,
# MAGIC         MAP,
# MAGIC         Sequence,
# MAGIC         CONCAT(Department, MachineCenter) AS `KEY`,
# MAGIC         KeyCopy AS `KEY - Copy`
# MAGIC     FROM sp_candidates
# MAGIC     WHERE rn = 1
# MAGIC )
# MAGIC 
# MAGIC /* =========================
# MAGIC    FINAL
# MAGIC ========================= */
# MAGIC SELECT Department, `Machine center` as Code , `Work center` as work_center, MAP, Sequence, `KEY`, `KEY - Copy` AS Key_Abbr FROM cad
# MAGIC UNION ALL
# MAGIC SELECT Department, `Machine center` as Code, `Work center` as work_center, MAP, Sequence, `KEY`, `KEY - Copy` AS Key_Abbr FROM mst_picked
# MAGIC UNION ALL
# MAGIC SELECT Department, `Machine center` as Code, `Work center` as work_center, MAP, Sequence, `KEY`, `KEY - Copy` AS Key_Abbr FROM sp_picked
# MAGIC ;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DELETE FROM Gold_Product_Dev_Lakehouse.pd.gold_step_master_final
# MAGIC WHERE Department = 'CAD'
# MAGIC   AND Sequence = 9;
# MAGIC 
# MAGIC DELETE FROM Gold_Product_Dev_Lakehouse.pd.gold_step_master_final
# MAGIC WHERE Department = 'CAD'
# MAGIC   AND Sequence = 3;


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
# MAGIC CREATE OR REPLACE TABLE `Gold_Product_Dev_Lakehouse`.`pd`.`gold_npd_skt_prod` AS
# MAGIC WITH SketchData AS (
# MAGIC     SELECT
# MAGIC         sm.MergedSO AS SO,
# MAGIC         concat(substr(sm.MergedSO, 1, 2), substr(sm.MergedSO, length(sm.MergedSO) - 3, 4)) AS SO_abbr,
# MAGIC         sm.item_no AS item_no,
# MAGIC 
# MAGIC         w.pd_sketch_item AS `เลข_SKT`,
# MAGIC         coalesce(sm.MergedProd, '') AS `เลข_PROD`,
# MAGIC         st.Code AS Routing,
# MAGIC 
# MAGIC         max(cast(w.created_on AS date)) AS latest_created_on,
# MAGIC         date_format(max(cast(w.created_on AS date)), 'dd-MMM') AS LatestCreated_DayMonth,
# MAGIC 
# MAGIC         'Sketch' AS Source,
# MAGIC         st.Sequence AS Sequence,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN st.Code = 'CAD' THEN 'CAD'
# MAGIC             ELSE
# MAGIC                 coalesce(
# MAGIC                     nullif(st.Department, ''),
# MAGIC                     CASE
# MAGIC                         WHEN sm.MergedProd LIKE 'WMT%'  THEN 'MST'
# MAGIC                         WHEN sm.MergedProd LIKE 'WONE%' THEN 'SP'
# MAGIC                         WHEN sm.MergedProd LIKE 'WSP%'  THEN 'SP'
# MAGIC                         WHEN coalesce(sm.MergedProd, '') = '' THEN 'CAD'
# MAGIC                         ELSE 'Unknown'
# MAGIC                     END
# MAGIC                 )
# MAGIC         END AS Department
# MAGIC     FROM `Silver_Product_Dev_Lakehouse`.`pd`.`silver_npd_worklogs` w
# MAGIC     LEFT JOIN `Gold_Product_Dev_Lakehouse`.`pd`.`gold_npd_sketch_mapping` sm
# MAGIC         ON w.pd_sketch_item = sm.sketch_number
# MAGIC     LEFT JOIN `Gold_Product_Dev_Lakehouse`.`pd`.`gold_step_name` st
# MAGIC         ON w.pd_step = st.Code
# MAGIC     WHERE
# MAGIC         sm.MergedProd LIKE 'WMT%'
# MAGIC         OR sm.MergedProd LIKE 'WONE%'
# MAGIC         OR sm.MergedProd LIKE 'WSP%'
# MAGIC         OR coalesce(sm.MergedProd, '') = ''
# MAGIC     GROUP BY
# MAGIC         sm.MergedSO,
# MAGIC         sm.item_no,
# MAGIC         w.pd_sketch_item,
# MAGIC         sm.MergedProd,
# MAGIC         st.Code,
# MAGIC         st.Sequence,
# MAGIC         st.Department
# MAGIC ),
# MAGIC 
# MAGIC ProductionData AS (
# MAGIC     SELECT
# MAGIC         sm.MergedSO AS SO,
# MAGIC         concat(substr(sm.MergedSO, 1, 2), substr(sm.MergedSO, length(sm.MergedSO) - 3, 4)) AS SO_abbr,
# MAGIC         sm.item_no AS item_no,
# MAGIC 
# MAGIC         w.pd_sketch_item AS `เลข_SKT`,
# MAGIC         coalesce(sm.MergedProd, '') AS `เลข_PROD`,
# MAGIC         st.Code AS Routing,
# MAGIC 
# MAGIC         max(cast(d.created_on AS date)) AS latest_created_on,
# MAGIC         date_format(max(cast(d.created_on AS date)), 'dd-MMM') AS LatestCreated_DayMonth,
# MAGIC 
# MAGIC         'Production' AS Source,
# MAGIC         st.Sequence AS Sequence,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN st.Code = 'CAD' THEN 'CAD'
# MAGIC             WHEN sm.MergedProd LIKE 'WMT%'  THEN 'MST'
# MAGIC             WHEN sm.MergedProd LIKE 'WSP%'  THEN 'SP'
# MAGIC             WHEN sm.MergedProd LIKE 'WONE%' THEN 'SP'
# MAGIC             ELSE
# MAGIC                 coalesce(
# MAGIC                     nullif(st.Department, ''),
# MAGIC                     CASE
# MAGIC                         WHEN max(d.sales_order_no) LIKE 'MT%' THEN 'MST'
# MAGIC                         WHEN max(d.sales_order_no) LIKE 'SP%' THEN 'SP'
# MAGIC                         WHEN coalesce(max(d.sales_order_no), '') = '' THEN 'CAD'
# MAGIC                         ELSE 'Unknown'
# MAGIC                     END
# MAGIC                 )
# MAGIC         END AS Department
# MAGIC     FROM `Silver_Product_Dev_Lakehouse`.`pd`.`silver_npd_worklogs` w
# MAGIC     LEFT JOIN `Gold_Product_Dev_Lakehouse`.`pd`.`gold_npd_sketch_mapping` sm
# MAGIC         ON w.pd_sketch_item = sm.sketch_number
# MAGIC     LEFT JOIN `Gold_Product_Dev_Lakehouse`.`pd`.`gold_npd_prod_status` d
# MAGIC         ON sm.MergedProd = d.`No.`
# MAGIC         AND d.type_name = 'In location in'
# MAGIC     LEFT JOIN `Gold_Product_Dev_Lakehouse`.`pd`.`gold_step_name` st
# MAGIC         ON st.Code = coalesce(nullif(d.machine_center_no, ''), d.current_location_code)
# MAGIC     WHERE
# MAGIC         (
# MAGIC             sm.MergedProd LIKE 'WMT%'
# MAGIC             OR sm.MergedProd LIKE 'WONE%'
# MAGIC             OR sm.MergedProd LIKE 'WSP%'
# MAGIC             OR coalesce(sm.MergedProd, '') = ''
# MAGIC         )
# MAGIC         AND NOT (sm.MergedProd LIKE 'WMT%' AND st.Department = 'SP')
# MAGIC     GROUP BY
# MAGIC         sm.MergedSO,
# MAGIC         sm.item_no,
# MAGIC         w.pd_sketch_item,
# MAGIC         sm.MergedProd,
# MAGIC         st.Code,
# MAGIC         st.Sequence,
# MAGIC         st.Department
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     SO,
# MAGIC     SO_abbr,
# MAGIC     item_no,
# MAGIC     `เลข_SKT`,
# MAGIC     `เลข_PROD`,
# MAGIC     Routing,
# MAGIC     latest_created_on,
# MAGIC     LatestCreated_DayMonth,
# MAGIC     Source,
# MAGIC     Sequence,
# MAGIC     Department
# MAGIC FROM SketchData
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT
# MAGIC     SO,
# MAGIC     SO_abbr,
# MAGIC     item_no,
# MAGIC     `เลข_SKT`,
# MAGIC     `เลข_PROD`,
# MAGIC     Routing,
# MAGIC     latest_created_on,
# MAGIC     LatestCreated_DayMonth,
# MAGIC     Source,
# MAGIC     Sequence,
# MAGIC     Department
# MAGIC FROM ProductionData
# MAGIC ;


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
# MAGIC       FROM Silver_Product_Dev_Lakehouse.pd.silver_npd_worklogs wl
# MAGIC       INNER JOIN Silver_Product_Dev_Lakehouse.pd.silver_pd_step steps
# MAGIC         ON wl.pd_step = steps.pd_step
# MAGIC       WHERE wl.pd_sketch_item = c.sketch_number
# MAGIC         AND steps.pd_sequence >= 5
# MAGIC     )
# MAGIC       THEN 0
# MAGIC     WHEN c.estimated_development_time IS NULL
# MAGIC       THEN NULL
# MAGIC     ELSE
# MAGIC       CAST(c.estimated_development_time AS DECIMAL(18,4))
# MAGIC   END AS estimated_development_time_calc,
# MAGIC 
# MAGIC   -- estimated_production_ready_time_calc
# MAGIC   CASE
# MAGIC     WHEN EXISTS (
# MAGIC       SELECT 1
# MAGIC       FROM Silver_Product_Dev_Lakehouse.pd.silver_npd_worklogs wl
# MAGIC       INNER JOIN Silver_Product_Dev_Lakehouse.pd.silver_pd_step steps
# MAGIC         ON wl.pd_step = steps.pd_step
# MAGIC       WHERE wl.pd_sketch_item = c.sketch_number
# MAGIC         AND steps.pd_sequence >= 7
# MAGIC     )
# MAGIC       THEN 0
# MAGIC     WHEN c.estimated_production_ready_time IS NULL
# MAGIC       THEN NULL
# MAGIC     ELSE
# MAGIC       CAST(c.estimated_production_ready_time AS DECIMAL(18,4))
# MAGIC   END AS estimated_production_ready_time_calc
# MAGIC 
# MAGIC FROM Gold_Product_Dev_Lakehouse.pd.gold_cad_requests c;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold NPD Engineer Time Log

# CELL ********************

# MAGIC %%sql
# MAGIC -- Spark SQL (Databricks / Lakehouse) version
# MAGIC -- Creates a Spark TABLE from your query
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Product_Dev_Lakehouse.pd.gold_npd_engineer_time_log AS
# MAGIC -- Spark SQL (Databricks) - create/replace table
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     pd_sketch_item,
# MAGIC     pd_sketch_verion,
# MAGIC     created_on,
# MAGIC     pd_start,
# MAGIC     modified_on,
# MAGIC     pd_stop,
# MAGIC     pd_step,
# MAGIC 
# MAGIC     to_date(created_on) AS work_date,
# MAGIC 
# MAGIC     -- Build timestamps from work_date + time
# MAGIC     to_timestamp(
# MAGIC       concat(date_format(to_date(created_on), 'yyyy-MM-dd'), ' ', pd_start),
# MAGIC       'yyyy-MM-dd HH:mm:ss'
# MAGIC     ) AS start_ts,
# MAGIC 
# MAGIC     to_timestamp(
# MAGIC       concat(date_format(to_date(created_on), 'yyyy-MM-dd'), ' ', pd_stop),
# MAGIC       'yyyy-MM-dd HH:mm:ss'
# MAGIC     ) AS stop_ts,
# MAGIC 
# MAGIC     -- Cutoff 18:30 on work_date
# MAGIC     to_timestamp(
# MAGIC       concat(date_format(to_date(created_on), 'yyyy-MM-dd'), ' 18:30:00'),
# MAGIC       'yyyy-MM-dd HH:mm:ss'
# MAGIC     ) AS cutoff_ts
# MAGIC   FROM Silver_Product_Dev_Lakehouse.pd.silver_engineer_time
# MAGIC ),
# MAGIC calc AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     COALESCE(
# MAGIC       CASE
# MAGIC         WHEN start_ts IS NULL OR stop_ts IS NULL THEN 0.0
# MAGIC 
# MAGIC         -- if stop time after 18:30 OR crosses midnight => cap at cutoff
# MAGIC         WHEN pd_stop > '18:30:00' OR pd_stop < pd_start THEN
# MAGIC           CASE
# MAGIC             WHEN cutoff_ts > start_ts
# MAGIC               THEN timestampdiff(MINUTE, start_ts, cutoff_ts) / 60.0
# MAGIC             ELSE 0.0
# MAGIC           END
# MAGIC 
# MAGIC         -- normal diff within the same day
# MAGIC         ELSE
# MAGIC           CASE
# MAGIC             WHEN stop_ts >= start_ts
# MAGIC               THEN timestampdiff(MINUTE, start_ts, stop_ts) / 60.0
# MAGIC             ELSE 0.0
# MAGIC           END
# MAGIC       END,
# MAGIC     0.0) AS hours_worked
# MAGIC   FROM base
# MAGIC )
# MAGIC SELECT
# MAGIC   pd_sketch_item,
# MAGIC   pd_sketch_verion,
# MAGIC   created_on,
# MAGIC   pd_start,
# MAGIC   modified_on,
# MAGIC   pd_stop,
# MAGIC   pd_step,
# MAGIC 
# MAGIC   COALESCE(
# MAGIC     SUM(CASE WHEN pd_step = 'CUSTOMER REVISION' THEN hours_worked ELSE 0.0 END),
# MAGIC     0.0
# MAGIC   ) AS CustomerRevisionHours,
# MAGIC 
# MAGIC   COALESCE(
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN pd_step NOT IN ('CUSTOMER REVISION', '3D PRINTING', 'RECEIVE WAX')
# MAGIC           THEN hours_worked
# MAGIC         ELSE 0.0
# MAGIC       END
# MAGIC     ),
# MAGIC     0.0
# MAGIC   ) AS TotalHours
# MAGIC FROM calc
# MAGIC GROUP BY
# MAGIC   pd_sketch_item,
# MAGIC   pd_sketch_verion,
# MAGIC   created_on,
# MAGIC   pd_start,
# MAGIC   modified_on,
# MAGIC   pd_stop,
# MAGIC   pd_step;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold NPD Last Step 3

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Product_Dev_Lakehouse.pd.gold_npd_last_step_3 AS
# MAGIC WITH Filtered AS (
# MAGIC   SELECT
# MAGIC     w.pd_sketch_item        AS `SKT Number`,
# MAGIC     w.created_on   AS `Worklog Date`,
# MAGIC     w.pd_step               AS `Routing Step`
# MAGIC   FROM Silver_Product_Dev_Lakehouse.pd.silver_npd_worklogs w
# MAGIC   WHERE w.pd_sketch_item IS NOT NULL
# MAGIC     AND w.pd_sketch_item NOT IN (
# MAGIC       SELECT DISTINCT w2.pd_sketch_item
# MAGIC       FROM Silver_Product_Dev_Lakehouse.pd.silver_npd_worklogs w2
# MAGIC       WHERE w2.pd_step IN ('RECEIVE WAX', 'CAD PRODUCTION READY', '3D PRINTING')
# MAGIC         AND w2.pd_sketch_item IS NOT NULL
# MAGIC     )
# MAGIC ),
# MAGIC 
# MAGIC Ranked AS (
# MAGIC   SELECT
# MAGIC     `SKT Number`,
# MAGIC     `Worklog Date`,
# MAGIC     `Routing Step`,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY `SKT Number`
# MAGIC       ORDER BY `Worklog Date` DESC
# MAGIC     ) AS rn
# MAGIC   FROM Filtered
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC   `SKT Number` as SKT_Number,
# MAGIC   `Worklog Date` as Worklog_Date,
# MAGIC   `Routing Step` as Routing_Step
# MAGIC FROM Ranked
# MAGIC WHERE rn = 1;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold NPD Sales INV head line

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Product_Dev_Lakehouse.pd.gold_npd_sales_inv_head_line AS
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     i.*,
# MAGIC     CONCAT(
# MAGIC       COALESCE(CAST(i.salesorder_no AS STRING), ''),
# MAGIC       COALESCE(CAST(i.item_no      AS STRING), '')
# MAGIC     ) AS SOI
# MAGIC   FROM Silver_Finance_Lakehouse.fa.silver_sales_invoice_header_line i
# MAGIC ),
# MAGIC 
# MAGIC merged AS (
# MAGIC   SELECT
# MAGIC     b.*,
# MAGIC 
# MAGIC     -- from BC Sales Line
# MAGIC     sl.`Requested Delivery Date` AS so_requested_delivery_date,
# MAGIC     sl.`Promised Delivery Date`  AS so_promised_delivery_date,
# MAGIC     sl.`Planned Delivery Date`   AS so_due_date,
# MAGIC     sl.`Planned Shipment Date`   AS so_shipment_date,
# MAGIC 
# MAGIC     -- from BC Sales Header
# MAGIC     sh.`Posting Date`            AS so_sales_order_posting_date,
# MAGIC     sh.`Order Date`              AS so_order_date
# MAGIC   FROM base b
# MAGIC   LEFT JOIN `Silver_BC_Lakehouse`.`bc`.`Sales Line` sl
# MAGIC     ON  b.salesorder_no     = sl.`Document No.`
# MAGIC     AND b.salesorder_lineno = sl.`Line No.`
# MAGIC     -- optional safety if your Sales Line contains multiple doc types with same no/line:
# MAGIC     -- AND sl.`Document Type` = 'Order'
# MAGIC   LEFT JOIN `Silver_BC_Lakehouse`.`bc`.`Sales Header` sh
# MAGIC     ON  b.salesorder_no     = sh.`No.`
# MAGIC     -- optional safety:
# MAGIC     -- AND sh.`Document Type` = 'Order'
# MAGIC ),
# MAGIC 
# MAGIC dedup AS (
# MAGIC   SELECT
# MAGIC     m.*,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY m.invoice_no, m.invoice_lineno
# MAGIC       ORDER BY
# MAGIC         COALESCE(m.so_sales_order_posting_date, to_date('0001-01-01')) DESC,
# MAGIC         COALESCE(m.modified_on, m.created_on) DESC
# MAGIC     ) AS rn
# MAGIC   FROM merged m
# MAGIC   WHERE
# MAGIC     m.invoice_posting_date > to_date('0001-01-01')
# MAGIC     AND m.salesorder_no LIKE 'SP%'
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC   d.*,
# MAGIC   CONCAT(
# MAGIC     COALESCE(CAST(d.salesorder_no AS STRING), ''),
# MAGIC     COALESCE(CAST(d.item_no      AS STRING), '')
# MAGIC   ) AS KEY_SOITEM
# MAGIC FROM dedup d
# MAGIC WHERE d.rn = 1
# MAGIC ORDER BY d.invoice_posting_date ASC;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Product_Dev_Lakehouse.pd.gold_npd_report_overview AS
# MAGIC 
# MAGIC WITH
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- 1) Active sketches (180-day lookback, not archived)
# MAGIC -- ============================================================
# MAGIC active_sketches AS (
# MAGIC     SELECT
# MAGIC         c.sketch_number,
# MAGIC         c.description,
# MAGIC         c.submission_date,
# MAGIC         c.customer_request_date,
# MAGIC         c.promised_date,
# MAGIC         c.cad_week,
# MAGIC         c.cus_req       AS cus_req_week,
# MAGIC         c.pro_req       AS pro_req_week,
# MAGIC         c.committed_week,
# MAGIC         c.cropped_design_image_url,
# MAGIC         c.additional_photo_url,
# MAGIC         c.collection_id_name,
# MAGIC         c.assigned_engineer_name,
# MAGIC         c.estimated_development_time,
# MAGIC         c.estimated_production_ready_time,
# MAGIC         c.customer_target_price,
# MAGIC         c.customer_design_number,
# MAGIC         c.created_by_email,
# MAGIC         c.status_name           AS sketch_status_name,
# MAGIC         c.status                AS schedule_status,         -- 'On Schedule' / 'Overdue' (vs promised_date)
# MAGIC         c.customer_name         AS customer_name_raw,
# MAGIC         c.is_archive_name,
# MAGIC         c.created_on            AS sketch_created_on,
# MAGIC         c.modified_on           AS sketch_modified_on
# MAGIC     FROM Gold_Product_Dev_Lakehouse.pd.gold_cad_requests c
# MAGIC     WHERE c.is_archive_name = false
# MAGIC       AND c.submission_date >= date_sub(current_date(), 180)
# MAGIC       AND c.sketch_number IS NOT NULL
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- 2) Customer enrichment
# MAGIC --    NOTE: gold_cad_requests already has customer_name. We pull
# MAGIC --    customer_abbr and cs_team from gold_customer if available.
# MAGIC --    Adjust the JOIN key once you confirm which field links them.
# MAGIC -- ============================================================
# MAGIC customer_info AS (
# MAGIC     SELECT
# MAGIC         TRIM(cus.customer_name)                 AS customer_name_key,
# MAGIC         MAX(cus.customer_no)                    AS customer_no,
# MAGIC         MAX(cus.customer_abbr)                  AS customer_abbr,
# MAGIC         MAX(cus.cs_team)                        AS cs_team
# MAGIC     FROM Gold_Customer_Exp_Lakehouse.cx.gold_customer cus
# MAGIC     WHERE cus.customer_name IS NOT NULL
# MAGIC     GROUP BY TRIM(cus.customer_name)
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- 3) PRO + item + SO aggregation per sketch
# MAGIC -- ============================================================
# MAGIC sketch_pros AS (
# MAGIC     SELECT
# MAGIC         sm.sketch_number,
# MAGIC         -- arrays for HTML detail panel
# MAGIC         array_distinct(collect_list(sm.item_no))                                                     AS fg_items,
# MAGIC         array_distinct(filter(collect_list(sm.MergedSO), x -> x IS NOT NULL AND x <> ''))            AS linked_sos,
# MAGIC         array_distinct(filter(collect_list(
# MAGIC             CASE WHEN sm.prod_order_no LIKE 'WMT%' THEN sm.prod_order_no END), x -> x IS NOT NULL))  AS master_pros,
# MAGIC         array_distinct(filter(collect_list(
# MAGIC             CASE WHEN sm.prod_order_no LIKE 'WSP%' THEN sm.prod_order_no END), x -> x IS NOT NULL))  AS sample_pros,
# MAGIC         array_distinct(filter(collect_list(
# MAGIC             CASE WHEN sm.prod_order_no LIKE 'WMT%' THEN sm.item_no END), x -> x IS NOT NULL))        AS master_items,
# MAGIC         array_distinct(filter(collect_list(
# MAGIC             CASE WHEN sm.prod_order_no LIKE 'WSP%' THEN sm.item_no END), x -> x IS NOT NULL))        AS sample_items,
# MAGIC         -- boolean flags
# MAGIC         MAX(CASE WHEN sm.prod_order_no LIKE 'WMT%' THEN 1 ELSE 0 END) = 1                            AS has_master_pro,
# MAGIC         MAX(CASE WHEN sm.prod_order_no LIKE 'WSP%' THEN 1 ELSE 0 END) = 1                            AS has_sample_pro
# MAGIC     FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_sketch_mapping sm
# MAGIC     WHERE sm.sketch_number IS NOT NULL
# MAGIC     GROUP BY sm.sketch_number
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- 4) CAD stage timestamps (7 stages)
# MAGIC --    Source: gold_npd_skt_prod Department='CAD'
# MAGIC --    Each Routing string identifies a CAD stage; take MAX(latest_created_on)
# MAGIC -- ============================================================
# MAGIC cad_stages AS (
# MAGIC     SELECT
# MAGIC         sp.`เลข_SKT`                                                                                         AS sketch_number,
# MAGIC         MAX(CASE WHEN sp.Routing = 'BACKLOG'                       THEN sp.latest_created_on END)            AS cad_bklg,
# MAGIC         MAX(CASE WHEN sp.Routing = 'CAD ASSIGNED TO ENGINEER'      THEN sp.latest_created_on END)            AS cad_asgn,
# MAGIC         MAX(CASE WHEN sp.Routing = 'WAITING FOR MANAGER APPROVAL'  THEN sp.latest_created_on END)            AS cad_mng_app,
# MAGIC         MAX(CASE WHEN sp.Routing = 'WAITING FOR CUSTOMER APPROVAL' THEN sp.latest_created_on END)            AS cad_cus_app,
# MAGIC         MAX(CASE WHEN sp.Routing = 'CUSTOMER REVISION'             THEN sp.latest_created_on END)            AS cad_cus_rev,
# MAGIC         MAX(CASE WHEN sp.Routing = 'CAD PRODUCTION READY'          THEN sp.latest_created_on END)            AS cad_ready,
# MAGIC         MAX(CASE WHEN sp.Routing = '3D PRINTING'                   THEN sp.latest_created_on END)            AS cad_3d_pt
# MAGIC     FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_skt_prod sp
# MAGIC     WHERE sp.Department = 'CAD'
# MAGIC       AND sp.`เลข_SKT` IS NOT NULL
# MAGIC     GROUP BY sp.`เลข_SKT`
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- 5) Production stage timestamps (MST 8 + SPL 8)
# MAGIC --    Strategy: use type_name='In location in' events (when a PRO
# MAGIC --    ENTERED a stage), not modified_on. This sidesteps BC's
# MAGIC --    close-out timestamp bump entirely — no close-out detection needed.
# MAGIC --
# MAGIC --    Join chain:
# MAGIC --      gold_npd_sketch_mapping (sketch ↔ PRO)
# MAGIC --        ↓
# MAGIC --      gold_npd_prod_status     (PRO + operation, filter type='In location in')
# MAGIC --        ↓
# MAGIC --      gold_step_master_final   (machine_center / location → Key_Abbr stage label)
# MAGIC --
# MAGIC --    Then pivot Key_Abbr to columns.
# MAGIC -- ============================================================
# MAGIC prod_stage_events AS (
# MAGIC     -- gold_npd_sketch_mapping has only `prod_order_no` (single col) + `MergedProd` (concat of all PROs with '/').
# MAGIC     -- To match a sketch to ALL its PROs, match ps.`No.` against MergedProd's slash-separated tokens.
# MAGIC     SELECT DISTINCT
# MAGIC         sm.sketch_number,
# MAGIC         ps.`No.`                                              AS pro_number,
# MAGIC         ps.operation_no,
# MAGIC         ps.created_on                                         AS event_at,    -- "entered stage" time
# MAGIC         ps.machine_center_no,
# MAGIC         ps.current_location_code,
# MAGIC         sf.Department                                         AS step_dept,    -- 'MST' or 'SP' or 'CAD'
# MAGIC         sf.Key_Abbr                                           AS stage_label   -- 'WH MST', 'ADN MST', 'WH SP', etc.
# MAGIC     FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_sketch_mapping sm
# MAGIC     INNER JOIN Gold_Product_Dev_Lakehouse.pd.gold_npd_prod_status ps
# MAGIC         -- Match against MergedProd (concat'd PRO list) using exact-token search:
# MAGIC         -- '/' + MergedProd + '/' LIKE '%/' + ps.[No.] + '/%'
# MAGIC         -- Guards against partial matches (e.g. 'WMT12' would not match 'WMT123').
# MAGIC         ON CONCAT('/', sm.MergedProd, '/') LIKE CONCAT('%/', ps.`No.`, '/%')
# MAGIC     INNER JOIN Gold_Product_Dev_Lakehouse.pd.gold_step_master_final sf
# MAGIC         ON sf.Code = COALESCE(NULLIF(ps.machine_center_no, ''), ps.current_location_code)
# MAGIC     WHERE ps.type_name = 'In location in'
# MAGIC       AND sm.sketch_number IS NOT NULL
# MAGIC       AND ps.`No.` IS NOT NULL
# MAGIC       AND (ps.`No.` LIKE 'WMT%' OR ps.`No.` LIKE 'WSP%')
# MAGIC ),
# MAGIC 
# MAGIC mst_stages AS (
# MAGIC     SELECT
# MAGIC         e.sketch_number,
# MAGIC         MAX(CASE WHEN e.stage_label = 'WH MST'  THEN e.event_at END) AS mst_wh,
# MAGIC         MAX(CASE WHEN e.stage_label = 'ADN MST' THEN e.event_at END) AS mst_adn,
# MAGIC         MAX(CASE WHEN e.stage_label = 'CAS MST' THEN e.event_at END) AS mst_cas,
# MAGIC         MAX(CASE WHEN e.stage_label = 'FL MST'  THEN e.event_at END) AS mst_fl,
# MAGIC         MAX(CASE WHEN e.stage_label = 'P-SET'   THEN e.event_at END) AS mst_pre_set,
# MAGIC         MAX(CASE WHEN e.stage_label = 'ENGRV'   THEN e.event_at END) AS mst_engr,
# MAGIC         MAX(CASE WHEN e.stage_label = 'SPR'     THEN e.event_at END) AS mst_sprue,
# MAGIC         MAX(CASE WHEN e.stage_label = 'R-MOLD'  THEN e.event_at END) AS mst_rub_mold,
# MAGIC         COUNT(DISTINCT e.pro_number) FILTER (WHERE e.pro_number LIKE 'WMT%') AS mst_pro_count
# MAGIC     FROM prod_stage_events e
# MAGIC     WHERE e.step_dept = 'MST'
# MAGIC     GROUP BY e.sketch_number
# MAGIC ),
# MAGIC 
# MAGIC spl_stages AS (
# MAGIC     SELECT
# MAGIC         e.sketch_number,
# MAGIC         MAX(CASE WHEN e.stage_label = 'WH SP'   THEN e.event_at END) AS spl_wh,
# MAGIC         MAX(CASE WHEN e.stage_label = 'ADN SP'  THEN e.event_at END) AS spl_adm,
# MAGIC         MAX(CASE WHEN e.stage_label = 'CAS SP'  THEN e.event_at END) AS spl_cas,
# MAGIC         MAX(CASE WHEN e.stage_label = 'FIL'     THEN e.event_at END) AS spl_fil,
# MAGIC         MAX(CASE WHEN e.stage_label = 'SET'     THEN e.event_at END) AS spl_set,
# MAGIC         MAX(CASE WHEN e.stage_label = 'POL'     THEN e.event_at END) AS spl_pol,
# MAGIC         MAX(CASE WHEN e.stage_label = 'PLAT'    THEN e.event_at END) AS spl_plat,
# MAGIC         MAX(CASE WHEN e.stage_label = 'QA'      THEN e.event_at END) AS spl_qa,
# MAGIC         COUNT(DISTINCT e.pro_number) FILTER (WHERE e.pro_number LIKE 'WSP%') AS spl_pro_count
# MAGIC     FROM prod_stage_events e
# MAGIC     WHERE e.step_dept = 'SP'
# MAGIC     GROUP BY e.sketch_number
# MAGIC ),
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- 6) Assemble per-sketch row
# MAGIC -- ============================================================
# MAGIC assembled AS (
# MAGIC     SELECT
# MAGIC         a.*,
# MAGIC 
# MAGIC         -- Customer enrichment (best-effort match by name)
# MAGIC         ci.customer_no,
# MAGIC         ci.customer_abbr,
# MAGIC         ci.cs_team,
# MAGIC 
# MAGIC         -- PROs + items
# MAGIC         COALESCE(sp.fg_items,        array())          AS fg_items,
# MAGIC         COALESCE(sp.master_pros,     array())          AS master_pros,
# MAGIC         COALESCE(sp.sample_pros,     array())          AS sample_pros,
# MAGIC         COALESCE(sp.master_items,    array())          AS master_items,
# MAGIC         COALESCE(sp.sample_items,    array())          AS sample_items,
# MAGIC         COALESCE(sp.linked_sos,      array())          AS linked_sos,
# MAGIC         COALESCE(sp.has_master_pro,  false)            AS has_master_pro,
# MAGIC         COALESCE(sp.has_sample_pro,  false)            AS has_sample_pro,
# MAGIC 
# MAGIC         -- CAD stages
# MAGIC         cs.cad_bklg, cs.cad_asgn, cs.cad_mng_app, cs.cad_cus_app,
# MAGIC         cs.cad_cus_rev, cs.cad_ready, cs.cad_3d_pt,
# MAGIC 
# MAGIC         -- MST stages (8)
# MAGIC         ms.mst_wh, ms.mst_adn, ms.mst_cas, ms.mst_fl,
# MAGIC         ms.mst_pre_set, ms.mst_engr, ms.mst_sprue, ms.mst_rub_mold,
# MAGIC         COALESCE(ms.mst_pro_count, 0)                  AS mst_pro_count,
# MAGIC 
# MAGIC         -- SPL stages (8)
# MAGIC         ss.spl_wh, ss.spl_adm, ss.spl_cas, ss.spl_fil,
# MAGIC         ss.spl_set, ss.spl_pol, ss.spl_plat, ss.spl_qa,
# MAGIC         COALESCE(ss.spl_pro_count, 0)                  AS spl_pro_count
# MAGIC 
# MAGIC     FROM active_sketches a
# MAGIC     LEFT JOIN customer_info ci ON TRIM(a.customer_name_raw) = ci.customer_name_key
# MAGIC     LEFT JOIN sketch_pros sp   ON a.sketch_number = sp.sketch_number
# MAGIC     LEFT JOIN cad_stages cs    ON a.sketch_number = cs.sketch_number
# MAGIC     LEFT JOIN mst_stages ms    ON a.sketch_number = ms.sketch_number
# MAGIC     LEFT JOIN spl_stages ss    ON a.sketch_number = ss.sketch_number
# MAGIC )
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- 7) Final: derive status, current_stage, days_in_stage
# MAGIC -- ============================================================
# MAGIC SELECT
# MAGIC     -- Identity
# MAGIC     sketch_number,
# MAGIC     description,
# MAGIC     submission_date,
# MAGIC     customer_request_date,
# MAGIC     promised_date,
# MAGIC     cad_week, cus_req_week, pro_req_week, committed_week,
# MAGIC     cropped_design_image_url,
# MAGIC     additional_photo_url,
# MAGIC     collection_id_name,
# MAGIC     assigned_engineer_name,
# MAGIC     estimated_development_time,
# MAGIC     estimated_production_ready_time,
# MAGIC     customer_target_price,
# MAGIC     customer_design_number,
# MAGIC     created_by_email,
# MAGIC     sketch_status_name,
# MAGIC     schedule_status,
# MAGIC     customer_name_raw                     AS customer_name,
# MAGIC 
# MAGIC     -- Customer
# MAGIC     customer_no,
# MAGIC     customer_abbr,
# MAGIC     cs_team,
# MAGIC 
# MAGIC     -- PROs and items
# MAGIC     fg_items, master_pros, sample_pros, master_items, sample_items, linked_sos,
# MAGIC     has_master_pro, has_sample_pro,
# MAGIC     mst_pro_count, spl_pro_count,
# MAGIC 
# MAGIC     -- CAD stages (7)
# MAGIC     cad_bklg, cad_asgn, cad_mng_app, cad_cus_app,
# MAGIC     cad_cus_rev, cad_ready, cad_3d_pt,
# MAGIC 
# MAGIC     -- MST stages (8)
# MAGIC     mst_wh, mst_adn, mst_cas, mst_fl,
# MAGIC     mst_pre_set, mst_engr, mst_sprue, mst_rub_mold,
# MAGIC 
# MAGIC     -- SPL stages (8)
# MAGIC     spl_wh, spl_adm, spl_cas, spl_fil,
# MAGIC     spl_set, spl_pol, spl_plat, spl_qa,
# MAGIC 
# MAGIC     -- Derived: latest filled timestamp across all stages
# MAGIC     GREATEST(
# MAGIC         COALESCE(spl_qa,    TIMESTAMP '1900-01-01'),
# MAGIC         COALESCE(spl_plat,  TIMESTAMP '1900-01-01'),
# MAGIC         COALESCE(spl_pol,   TIMESTAMP '1900-01-01'),
# MAGIC         COALESCE(spl_set,   TIMESTAMP '1900-01-01'),
# MAGIC         COALESCE(spl_fil,   TIMESTAMP '1900-01-01'),
# MAGIC         COALESCE(spl_cas,   TIMESTAMP '1900-01-01'),
# MAGIC         COALESCE(spl_adm,   TIMESTAMP '1900-01-01'),
# MAGIC         COALESCE(spl_wh,    TIMESTAMP '1900-01-01'),
# MAGIC         COALESCE(mst_rub_mold, TIMESTAMP '1900-01-01'),
# MAGIC         COALESCE(mst_sprue,    TIMESTAMP '1900-01-01'),
# MAGIC         COALESCE(mst_engr,     TIMESTAMP '1900-01-01'),
# MAGIC         COALESCE(mst_pre_set,  TIMESTAMP '1900-01-01'),
# MAGIC         COALESCE(mst_fl,       TIMESTAMP '1900-01-01'),
# MAGIC         COALESCE(mst_cas,      TIMESTAMP '1900-01-01'),
# MAGIC         COALESCE(mst_adn,      TIMESTAMP '1900-01-01'),
# MAGIC         COALESCE(mst_wh,       TIMESTAMP '1900-01-01'),
# MAGIC         COALESCE(cad_3d_pt,    TIMESTAMP '1900-01-01'),
# MAGIC         COALESCE(cad_ready,    TIMESTAMP '1900-01-01'),
# MAGIC         COALESCE(cad_cus_rev,  TIMESTAMP '1900-01-01'),
# MAGIC         COALESCE(cad_cus_app,  TIMESTAMP '1900-01-01'),
# MAGIC         COALESCE(cad_mng_app,  TIMESTAMP '1900-01-01'),
# MAGIC         COALESCE(cad_asgn,     TIMESTAMP '1900-01-01'),
# MAGIC         COALESCE(cad_bklg,     TIMESTAMP '1900-01-01')
# MAGIC     )                                     AS last_stage_at,
# MAGIC 
# MAGIC     -- Derived: current_stage (last filled stage label, walking band priority SPL > MST > CAD)
# MAGIC     CASE
# MAGIC         WHEN spl_qa       IS NOT NULL THEN 'QA'
# MAGIC         WHEN spl_plat     IS NOT NULL THEN 'PLAT'
# MAGIC         WHEN spl_pol      IS NOT NULL THEN 'POL'
# MAGIC         WHEN spl_set      IS NOT NULL THEN 'SET'
# MAGIC         WHEN spl_fil      IS NOT NULL THEN 'FIL'
# MAGIC         WHEN spl_cas      IS NOT NULL THEN 'CAS SP'
# MAGIC         WHEN spl_adm      IS NOT NULL THEN 'ADN SP'
# MAGIC         WHEN spl_wh       IS NOT NULL THEN 'WH SP'
# MAGIC         WHEN mst_rub_mold IS NOT NULL THEN 'R-MOLD'
# MAGIC         WHEN mst_sprue    IS NOT NULL THEN 'SPR'
# MAGIC         WHEN mst_engr     IS NOT NULL THEN 'ENGRV'
# MAGIC         WHEN mst_pre_set  IS NOT NULL THEN 'P-SET'
# MAGIC         WHEN mst_fl       IS NOT NULL THEN 'FL MST'
# MAGIC         WHEN mst_cas      IS NOT NULL THEN 'CAS MST'
# MAGIC         WHEN mst_adn      IS NOT NULL THEN 'ADN MST'
# MAGIC         WHEN mst_wh       IS NOT NULL THEN 'WH MST'
# MAGIC         WHEN cad_3d_pt    IS NOT NULL THEN '3D PT'
# MAGIC         WHEN cad_ready    IS NOT NULL THEN 'READY'
# MAGIC         WHEN cad_cus_rev  IS NOT NULL THEN 'CUS REV'
# MAGIC         WHEN cad_cus_app  IS NOT NULL THEN 'CUS APP'
# MAGIC         WHEN cad_mng_app  IS NOT NULL THEN 'MNG APP'
# MAGIC         WHEN cad_asgn     IS NOT NULL THEN 'ASGN'
# MAGIC         WHEN cad_bklg     IS NOT NULL THEN 'CAD BKLG'
# MAGIC         ELSE 'CAD BKLG'
# MAGIC     END                                   AS current_stage,
# MAGIC 
# MAGIC     -- Derived: status_base (cad / mst / spl / dn)
# MAGIC     CASE
# MAGIC         WHEN spl_qa IS NOT NULL THEN 'dn'
# MAGIC         WHEN (spl_wh IS NOT NULL OR spl_adm IS NOT NULL OR spl_cas IS NOT NULL 
# MAGIC               OR spl_fil IS NOT NULL OR spl_set IS NOT NULL OR spl_pol IS NOT NULL 
# MAGIC               OR spl_plat IS NOT NULL) THEN 'spl'
# MAGIC         WHEN (mst_wh IS NOT NULL OR mst_adn IS NOT NULL OR mst_cas IS NOT NULL 
# MAGIC               OR mst_fl IS NOT NULL OR mst_pre_set IS NOT NULL OR mst_engr IS NOT NULL 
# MAGIC               OR mst_sprue IS NOT NULL OR mst_rub_mold IS NOT NULL) THEN 'mst'
# MAGIC         ELSE 'cad'
# MAGIC     END                                   AS status_base,
# MAGIC 
# MAGIC     -- Derived: stage counts for KPI cards (X / 8 of master, Y / 8 of sample, etc.)
# MAGIC     (CASE WHEN cad_bklg    IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC      CASE WHEN cad_asgn    IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC      CASE WHEN cad_mng_app IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC      CASE WHEN cad_cus_app IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC      CASE WHEN cad_cus_rev IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC      CASE WHEN cad_ready   IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC      CASE WHEN cad_3d_pt   IS NOT NULL THEN 1 ELSE 0 END
# MAGIC     )                                     AS cad_stages_filled,
# MAGIC 
# MAGIC     (CASE WHEN mst_wh       IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC      CASE WHEN mst_adn      IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC      CASE WHEN mst_cas      IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC      CASE WHEN mst_fl       IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC      CASE WHEN mst_pre_set  IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC      CASE WHEN mst_engr     IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC      CASE WHEN mst_sprue    IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC      CASE WHEN mst_rub_mold IS NOT NULL THEN 1 ELSE 0 END
# MAGIC     )                                     AS mst_stages_filled,
# MAGIC 
# MAGIC     (CASE WHEN spl_wh   IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC      CASE WHEN spl_adm  IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC      CASE WHEN spl_cas  IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC      CASE WHEN spl_fil  IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC      CASE WHEN spl_set  IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC      CASE WHEN spl_pol  IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC      CASE WHEN spl_plat IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC      CASE WHEN spl_qa   IS NOT NULL THEN 1 ELSE 0 END
# MAGIC     )                                     AS spl_stages_filled,
# MAGIC 
# MAGIC     -- Derived: days since last activity
# MAGIC     CASE
# MAGIC         WHEN GREATEST(
# MAGIC                 COALESCE(spl_qa, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(spl_plat, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(spl_pol, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(spl_set, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(spl_fil, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(spl_cas, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(spl_adm, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(spl_wh, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(mst_rub_mold, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(mst_sprue, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(mst_engr, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(mst_pre_set, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(mst_fl, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(mst_cas, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(mst_adn, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(mst_wh, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(cad_3d_pt, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(cad_ready, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(cad_cus_rev, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(cad_cus_app, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(cad_mng_app, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(cad_asgn, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(cad_bklg, TIMESTAMP '1900-01-01')
# MAGIC             ) > TIMESTAMP '1900-01-01'
# MAGIC         THEN datediff(current_date(), to_date(GREATEST(
# MAGIC                 COALESCE(spl_qa, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(spl_plat, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(spl_pol, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(spl_set, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(spl_fil, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(spl_cas, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(spl_adm, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(spl_wh, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(mst_rub_mold, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(mst_sprue, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(mst_engr, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(mst_pre_set, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(mst_fl, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(mst_cas, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(mst_adn, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(mst_wh, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(cad_3d_pt, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(cad_ready, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(cad_cus_rev, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(cad_cus_app, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(cad_mng_app, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(cad_asgn, TIMESTAMP '1900-01-01'),
# MAGIC                 COALESCE(cad_bklg, TIMESTAMP '1900-01-01')
# MAGIC             )))
# MAGIC         ELSE NULL
# MAGIC     END                                   AS days_in_stage,
# MAGIC 
# MAGIC     -- Derived: total elapsed since submission
# MAGIC     datediff(current_date(), submission_date) AS total_elapsed_days,
# MAGIC 
# MAGIC     -- Metadata
# MAGIC     sketch_created_on,
# MAGIC     sketch_modified_on,
# MAGIC     current_timestamp()                   AS table_last_updated
# MAGIC 
# MAGIC FROM assembled
# MAGIC ;
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- QUALITY CHECKS (run as separate cell after CREATE)
# MAGIC -- ============================================================
# MAGIC -- 1. Total active sketches (should match gold_cad_requests filter)
# MAGIC -- SELECT COUNT(*) AS active_sketches FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_report_overview;
# MAGIC 
# MAGIC -- 2. Status distribution (sanity check)
# MAGIC -- SELECT status_base, COUNT(*) AS n,
# MAGIC --        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
# MAGIC -- FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_report_overview
# MAGIC -- GROUP BY status_base ORDER BY n DESC;
# MAGIC 
# MAGIC -- 3. SK-4878 verification (canary)
# MAGIC -- SELECT sketch_number, description, status_base, current_stage, days_in_stage,
# MAGIC --        cad_stages_filled, mst_stages_filled, spl_stages_filled,
# MAGIC --        master_pros, sample_pros
# MAGIC -- FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_report_overview
# MAGIC -- WHERE sketch_number = 'SK-4878';
# MAGIC 
# MAGIC -- 4. Customer enrichment hit rate
# MAGIC -- SELECT
# MAGIC --   COUNT(*) AS total,
# MAGIC --   SUM(CASE WHEN customer_abbr IS NULL THEN 1 ELSE 0 END) AS missing_abbr,
# MAGIC --   SUM(CASE WHEN cs_team IS NULL THEN 1 ELSE 0 END) AS missing_cs_team
# MAGIC -- FROM Gold_Product_Dev_Lakehouse.pd.gold_npd_report_overview;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Test

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
