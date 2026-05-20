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
# META           "id": "ad99fdfa-85b1-4480-9f7f-2640bfd65f24"
# META         },
# META         {
# META           "id": "3a130b81-98ec-4fd4-a404-95edc1f0ef1e"
# META         },
# META         {
# META           "id": "e248ea90-8431-4df2-9f29-87866bf9dd5a"
# META         },
# META         {
# META           "id": "ff4d6787-a716-43b6-baaf-972b7426ffa5"
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

# # Production Assign Cell

# MARKDOWN ********************

# ## All Silver

# CELL ********************

# MAGIC %%sql
# MAGIC -- ==============================================================
# MAGIC -- GOLD: gold_production_asgn_cell
# MAGIC -- FULL REPLACE Spark SQL
# MAGIC -- ==============================================================
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_production_asgn_cell
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC WITH rl AS (
# MAGIC     SELECT distinct
# MAGIC         `Prod. Order No.`       AS prod_order_no,
# MAGIC         `Routing Reference No.` AS prod_order_line_no,
# MAGIC         `Routing No.`           AS item_no,
# MAGIC         `No.`                   AS routing_no,
# MAGIC         `SystemModifiedAt`      AS modified_on
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Prod Order Routing Line`
# MAGIC ),
# MAGIC 
# MAGIC rl_f AS (
# MAGIC     SELECT
# MAGIC         prod_order_no,
# MAGIC         item_no,
# MAGIC         prod_order_line_no,
# MAGIC         routing_no,
# MAGIC         modified_on AS _mod_rl_raw
# MAGIC     FROM rl
# MAGIC     WHERE prod_order_line_no = 10000
# MAGIC       AND (
# MAGIC             routing_no LIKE 'CELL%'
# MAGIC          OR routing_no LIKE 'OUTSOURCE%'
# MAGIC       )
# MAGIC ),
# MAGIC 
# MAGIC ranked AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CASE
# MAGIC             WHEN regexp_extract(routing_no, '^CELL(\\d+)$', 1) <> ''
# MAGIC             THEN CAST(regexp_extract(routing_no, '^CELL(\\d+)$', 1) AS INT)
# MAGIC             ELSE NULL
# MAGIC         END AS _suffix_int,
# MAGIC 
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY prod_order_no, prod_order_line_no
# MAGIC             ORDER BY
# MAGIC                 CASE WHEN routing_no = 'CELL108' THEN 1 ELSE 0 END ASC,
# MAGIC                 CASE
# MAGIC                     WHEN regexp_extract(routing_no, '^CELL(\\d+)$', 1) = ''
# MAGIC                     THEN 1
# MAGIC                     ELSE 0
# MAGIC                 END ASC,
# MAGIC                 CASE
# MAGIC                     WHEN regexp_extract(routing_no, '^CELL(\\d+)$', 1) <> ''
# MAGIC                     THEN CAST(regexp_extract(routing_no, '^CELL(\\d+)$', 1) AS INT)
# MAGIC                 END ASC NULLS LAST,
# MAGIC                 routing_no ASC
# MAGIC         ) AS _rn
# MAGIC     FROM rl_f
# MAGIC ),
# MAGIC 
# MAGIC chosen AS (
# MAGIC     SELECT
# MAGIC         prod_order_no,
# MAGIC         item_no,
# MAGIC         prod_order_line_no,
# MAGIC         routing_no AS cell_line,
# MAGIC         _mod_rl_raw
# MAGIC     FROM ranked
# MAGIC     WHERE _rn = 1
# MAGIC ),
# MAGIC 
# MAGIC cells AS (
# MAGIC     SELECT
# MAGIC         cell_line AS cell_line_join,
# MAGIC         prod_line,
# MAGIC         CAST(modified_on AS TIMESTAMP) AS _mod_cl_ts
# MAGIC     FROM Silver_Production_Lakehouse.prod.silver_cell_list
# MAGIC ),
# MAGIC 
# MAGIC joined AS (
# MAGIC     SELECT
# MAGIC         c.prod_order_no,
# MAGIC         c.item_no,
# MAGIC         c.prod_order_line_no,
# MAGIC         c.cell_line,
# MAGIC         cl.prod_line,
# MAGIC         c._mod_rl_raw,
# MAGIC         cl._mod_cl_ts
# MAGIC     FROM chosen c
# MAGIC     LEFT JOIN cells cl
# MAGIC         ON c.cell_line = cl.cell_line_join
# MAGIC ),
# MAGIC 
# MAGIC final_base AS (
# MAGIC     SELECT distinct
# MAGIC         prod_order_no,
# MAGIC         item_no,
# MAGIC         prod_order_line_no,
# MAGIC         cell_line,
# MAGIC         prod_line,
# MAGIC 
# MAGIC         COALESCE(
# MAGIC             GREATEST(
# MAGIC                 CAST(_mod_rl_raw AS TIMESTAMP),
# MAGIC                 _mod_cl_ts
# MAGIC             ),
# MAGIC             CURRENT_TIMESTAMP()
# MAGIC         ) AS _modified_any
# MAGIC 
# MAGIC     FROM joined
# MAGIC )
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM final_base;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Total Time Outsource Time

# MARKDOWN ********************

# ## Silver + Gold Asgn Cell

# CELL ********************

# MAGIC %%sql
# MAGIC -- ==========================================================
# MAGIC -- Job: gold_prod_total_time_outsource_time
# MAGIC -- FULL REPLACE Spark SQL
# MAGIC -- Mirrors [prod].[v_prod_time_summary]
# MAGIC -- ==========================================================
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_prod_total_time_outsource_time
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC WITH r_raw AS (
# MAGIC     SELECT
# MAGIC         `Prod. Order No.`       AS prod_order_no,
# MAGIC         `Routing Reference No.` AS prod_order_line_no,
# MAGIC         `Run Time`              AS run_time,
# MAGIC         `No.`                   AS routing_no,
# MAGIC         `SystemModifiedAt`      AS modified_on
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Prod Order Routing Line`
# MAGIC ),
# MAGIC 
# MAGIC s_raw AS (
# MAGIC     SELECT *
# MAGIC     FROM Silver_Production_Lakehouse.prod.silver_prod_order_status
# MAGIC ),
# MAGIC 
# MAGIC c_raw AS (
# MAGIC     SELECT *
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_production_asgn_cell
# MAGIC ),
# MAGIC 
# MAGIC -- ==========================================================
# MAGIC -- change_ts per (order,line)
# MAGIC -- ==========================================================
# MAGIC r_ts AS (
# MAGIC     SELECT
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no,
# MAGIC         MAX(TO_TIMESTAMP(modified_on)) AS r_mod
# MAGIC     FROM r_raw
# MAGIC     GROUP BY
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no
# MAGIC ),
# MAGIC 
# MAGIC s_ts AS (
# MAGIC     SELECT
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no,
# MAGIC         MAX(TO_TIMESTAMP(modified_on)) AS s_mod
# MAGIC     FROM s_raw
# MAGIC     GROUP BY
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no
# MAGIC ),
# MAGIC 
# MAGIC ts_joined AS (
# MAGIC     SELECT
# MAGIC         COALESCE(r.prod_order_no, s.prod_order_no) AS prod_order_no,
# MAGIC         COALESCE(r.prod_order_line_no, s.prod_order_line_no) AS prod_order_line_no,
# MAGIC 
# MAGIC         GREATEST(
# MAGIC             r.r_mod,
# MAGIC             s.s_mod
# MAGIC         ) AS change_ts
# MAGIC 
# MAGIC     FROM r_ts r
# MAGIC     FULL OUTER JOIN s_ts s
# MAGIC         ON r.prod_order_no = s.prod_order_no
# MAGIC        AND r.prod_order_line_no = s.prod_order_line_no
# MAGIC ),
# MAGIC 
# MAGIC -- ==========================================================
# MAGIC -- total run time
# MAGIC -- ==========================================================
# MAGIC r_total AS (
# MAGIC     SELECT
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no,
# MAGIC         SUM(COALESCE(run_time, 0.0)) AS total_run_time
# MAGIC     FROM r_raw
# MAGIC     GROUP BY
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no
# MAGIC ),
# MAGIC 
# MAGIC -- ==========================================================
# MAGIC -- outsource machine centers
# MAGIC -- ==========================================================
# MAGIC s_user_ops AS (
# MAGIC     SELECT DISTINCT
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no,
# MAGIC         machine_center_no
# MAGIC     FROM s_raw
# MAGIC     WHERE user_id = 'outsource@ennovie.com'
# MAGIC ),
# MAGIC 
# MAGIC -- ==========================================================
# MAGIC -- outsource run time
# MAGIC -- ==========================================================
# MAGIC r_outsource AS (
# MAGIC     SELECT
# MAGIC         r.prod_order_no,
# MAGIC         r.prod_order_line_no,
# MAGIC         SUM(COALESCE(r.run_time, 0.0)) AS outsource_run_time
# MAGIC     FROM r_raw r
# MAGIC     INNER JOIN s_user_ops u
# MAGIC         ON r.prod_order_no = u.prod_order_no
# MAGIC        AND r.prod_order_line_no = u.prod_order_line_no
# MAGIC        AND TRIM(r.routing_no) = TRIM(u.machine_center_no)
# MAGIC 
# MAGIC     GROUP BY
# MAGIC         r.prod_order_no,
# MAGIC         r.prod_order_line_no
# MAGIC ),
# MAGIC 
# MAGIC -- ==========================================================
# MAGIC -- item map
# MAGIC -- ==========================================================
# MAGIC item_map AS (
# MAGIC     SELECT
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no,
# MAGIC         MAX(item_no) AS item_no
# MAGIC     FROM s_raw
# MAGIC     GROUP BY
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no
# MAGIC ),
# MAGIC 
# MAGIC -- ==========================================================
# MAGIC -- latest created_on
# MAGIC -- ==========================================================
# MAGIC s_created AS (
# MAGIC     SELECT
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no,
# MAGIC         MAX(TO_TIMESTAMP(created_on)) AS created_on
# MAGIC     FROM s_raw
# MAGIC     GROUP BY
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no
# MAGIC ),
# MAGIC 
# MAGIC -- ==========================================================
# MAGIC -- final base
# MAGIC -- ==========================================================
# MAGIC final_base AS (
# MAGIC     SELECT
# MAGIC         rt.prod_order_no,
# MAGIC         rt.prod_order_line_no,
# MAGIC 
# MAGIC         COALESCE(im.item_no, c.item_no) AS item_no,
# MAGIC 
# MAGIC         c.cell_line,
# MAGIC         c.prod_line,
# MAGIC 
# MAGIC         rt.total_run_time,
# MAGIC 
# MAGIC         COALESCE(ro.outsource_run_time, 0.0) AS outsource_run_time,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN rt.total_run_time = 0.0 THEN 0.0
# MAGIC             ELSE (
# MAGIC                 COALESCE(ro.outsource_run_time, 0.0)
# MAGIC                 / rt.total_run_time
# MAGIC             ) * 100.0
# MAGIC         END AS outsource_pct,
# MAGIC 
# MAGIC         ts.change_ts,
# MAGIC 
# MAGIC         sc.created_on
# MAGIC 
# MAGIC     FROM r_total rt
# MAGIC 
# MAGIC     LEFT JOIN r_outsource ro
# MAGIC         ON rt.prod_order_no = ro.prod_order_no
# MAGIC        AND rt.prod_order_line_no = ro.prod_order_line_no
# MAGIC 
# MAGIC     LEFT JOIN c_raw c
# MAGIC         ON rt.prod_order_no = c.prod_order_no
# MAGIC        AND rt.prod_order_line_no = c.prod_order_line_no
# MAGIC 
# MAGIC     LEFT JOIN item_map im
# MAGIC         ON rt.prod_order_no = im.prod_order_no
# MAGIC        AND rt.prod_order_line_no = im.prod_order_line_no
# MAGIC 
# MAGIC     LEFT JOIN ts_joined ts
# MAGIC         ON rt.prod_order_no = ts.prod_order_no
# MAGIC        AND rt.prod_order_line_no = ts.prod_order_line_no
# MAGIC 
# MAGIC     LEFT JOIN s_created sc
# MAGIC         ON rt.prod_order_no = sc.prod_order_no
# MAGIC        AND rt.prod_order_line_no = sc.prod_order_line_no
# MAGIC )
# MAGIC 
# MAGIC -- ==========================================================
# MAGIC -- final output
# MAGIC -- ==========================================================
# MAGIC SELECT
# MAGIC     *,
# MAGIC     SHA2(
# MAGIC         CONCAT_WS(
# MAGIC             '||',
# MAGIC             COALESCE(CAST(prod_order_no AS STRING), ''),
# MAGIC             COALESCE(CAST(prod_order_line_no AS STRING), '')
# MAGIC         ),
# MAGIC         256
# MAGIC     ) AS row_id
# MAGIC FROM final_base;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Production Order Output

# MARKDOWN ********************

# ## Silver + Gold Asgn Cell

# CELL ********************

# MAGIC %%sql
# MAGIC -- ==========================================================
# MAGIC -- Job: Gold_Production_Lakehouse.prod.gold_prod_order_output
# MAGIC -- FULL REPLACE Spark SQL
# MAGIC -- Mirrors original SQL logic
# MAGIC -- ==========================================================
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_prod_order_output
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC WITH I AS (
# MAGIC     SELECT
# MAGIC         `Posting Date`          AS posting_date,
# MAGIC         `Document No.`          AS document_no,
# MAGIC         `Order No.`             AS order_no,
# MAGIC         `Order Line No.`        AS order_lineno,
# MAGIC         `Item No.`              AS item_no,
# MAGIC         `Description`           AS item_description,
# MAGIC         `Location Code`         AS entry_type_item_location,
# MAGIC         `Entry Type`            AS entry_type,
# MAGIC         `Quantity`              AS entry_type_item_quantity,
# MAGIC         `SystemCreatedAt`       AS created_on,
# MAGIC         `SystemModifiedAt`      AS modified_on
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry`
# MAGIC ),
# MAGIC 
# MAGIC C AS (
# MAGIC     SELECT
# MAGIC         prod_order_no,
# MAGIC         cell_line,
# MAGIC         prod_line
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_production_asgn_cell
# MAGIC ),
# MAGIC 
# MAGIC P AS (
# MAGIC     SELECT
# MAGIC         `No.`              AS prod_order_no,
# MAGIC         `Sales Order No.`  AS sales_order_no
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Production Order`
# MAGIC ),
# MAGIC 
# MAGIC S AS (
# MAGIC     SELECT
# MAGIC         `Sell-to Customer No.`    AS customer_no,
# MAGIC         `Sell-to Customer Name`   AS customer_name,
# MAGIC         `No.`                     AS sales_order_no,
# MAGIC         `Requested Delivery Date` AS sales_order_requested_date
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Sales Header`
# MAGIC ),
# MAGIC 
# MAGIC base AS (
# MAGIC 
# MAGIC     SELECT DISTINCT
# MAGIC 
# MAGIC         S.sales_order_requested_date AS sales_order_requested_date,
# MAGIC 
# MAGIC         I.posting_date               AS posting_date,
# MAGIC 
# MAGIC         S.customer_no               AS customer_no,
# MAGIC         S.customer_name             AS customer_name,
# MAGIC 
# MAGIC         P.sales_order_no            AS sales_order_no,
# MAGIC 
# MAGIC         I.document_no               AS document_no,
# MAGIC         I.order_no                  AS order_no,
# MAGIC         I.order_lineno              AS order_lineno,
# MAGIC 
# MAGIC         I.item_no                   AS item_no,
# MAGIC         I.item_description          AS item_description,
# MAGIC 
# MAGIC         I.entry_type_item_quantity  AS entry_type_item_quantity,
# MAGIC 
# MAGIC         C.cell_line                 AS cell_line,
# MAGIC         C.prod_line                 AS prod_line
# MAGIC 
# MAGIC     FROM I
# MAGIC 
# MAGIC     LEFT JOIN C
# MAGIC         ON I.document_no = C.prod_order_no
# MAGIC 
# MAGIC     LEFT JOIN P
# MAGIC         ON I.document_no = P.prod_order_no
# MAGIC 
# MAGIC     LEFT JOIN S
# MAGIC         ON P.sales_order_no = S.sales_order_no
# MAGIC 
# MAGIC     WHERE I.entry_type = 'Output'
# MAGIC       AND I.entry_type_item_location = 'FIN-GOODS'
# MAGIC       AND TO_DATE(COALESCE(I.posting_date, I.created_on)) IS NOT NULL
# MAGIC ),
# MAGIC 
# MAGIC final_dedup AS (
# MAGIC 
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC 
# MAGIC         SELECT
# MAGIC             *,
# MAGIC 
# MAGIC             SHA2(
# MAGIC                 CONCAT_WS(
# MAGIC                     '||',
# MAGIC                     COALESCE(CAST(document_no AS STRING), ''),
# MAGIC                     COALESCE(CAST(order_lineno AS STRING), ''),
# MAGIC                     COALESCE(CAST(item_no AS STRING), ''),
# MAGIC                     COALESCE(CAST(posting_date AS STRING), '')
# MAGIC                 ),
# MAGIC                 256
# MAGIC             ) AS row_id,
# MAGIC 
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY
# MAGIC                     SHA2(
# MAGIC                         CONCAT_WS(
# MAGIC                             '||',
# MAGIC                             COALESCE(CAST(document_no AS STRING), ''),
# MAGIC                             COALESCE(CAST(order_lineno AS STRING), ''),
# MAGIC                             COALESCE(CAST(item_no AS STRING), ''),
# MAGIC                             COALESCE(CAST(posting_date AS STRING), '')
# MAGIC                         ),
# MAGIC                         256
# MAGIC                     )
# MAGIC                 ORDER BY
# MAGIC                     document_no,
# MAGIC                     order_no,
# MAGIC                     order_lineno,
# MAGIC                     item_no
# MAGIC             ) AS rn
# MAGIC 
# MAGIC         FROM base
# MAGIC     )
# MAGIC 
# MAGIC     WHERE rn = 1
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     sales_order_requested_date,
# MAGIC     posting_date,
# MAGIC     customer_no,
# MAGIC     customer_name,
# MAGIC     sales_order_no,
# MAGIC     document_no,
# MAGIC     order_no,
# MAGIC     order_lineno,
# MAGIC     item_no,
# MAGIC     item_description,
# MAGIC     entry_type_item_quantity,
# MAGIC     cell_line,
# MAGIC     prod_line,
# MAGIC     row_id
# MAGIC FROM final_dedup;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Output Time Compare

# MARKDOWN ********************

# ## Silver + Gold Asgn Cell

# CELL ********************

# MAGIC %%sql
# MAGIC -- ==========================================================
# MAGIC -- Job: Gold_Production_Lakehouse.prod.gold_output_time_compare
# MAGIC -- FULL REPLACE Spark SQL
# MAGIC -- ==========================================================
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_output_time_compare
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC WITH s AS (
# MAGIC 
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         TO_TIMESTAMP(created_on)  AS created_on_ts,
# MAGIC         TO_TIMESTAMP(modified_on) AS modified_on_ts,
# MAGIC 
# MAGIC         GREATEST(
# MAGIC             TO_TIMESTAMP(created_on),
# MAGIC             TO_TIMESTAMP(modified_on)
# MAGIC         ) AS change_ts
# MAGIC 
# MAGIC     FROM Silver_Production_Lakehouse.prod.silver_prod_order_status
# MAGIC 
# MAGIC     WHERE type_name IN ('In location in', 'To employee')
# MAGIC ),
# MAGIC 
# MAGIC l AS (
# MAGIC 
# MAGIC     SELECT *
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_production_asgn_cell
# MAGIC ),
# MAGIC 
# MAGIC r AS (
# MAGIC 
# MAGIC     SELECT
# MAGIC         `Prod. Order No.`       AS prod_order_no,
# MAGIC         `Routing Reference No.` AS prod_order_line_no,
# MAGIC         `Operation No.`         AS operation_no,
# MAGIC         `Run Time`              AS run_time
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Prod Order Routing Line`
# MAGIC ),
# MAGIC 
# MAGIC joined AS (
# MAGIC 
# MAGIC     SELECT
# MAGIC         s.*,
# MAGIC         l.prod_line,
# MAGIC         l.cell_line,
# MAGIC 
# MAGIC         r.run_time
# MAGIC 
# MAGIC     FROM s
# MAGIC 
# MAGIC     LEFT JOIN l
# MAGIC         ON s.prod_order_no = l.prod_order_no
# MAGIC        AND s.prod_order_line_no = l.prod_order_line_no
# MAGIC 
# MAGIC     LEFT JOIN r
# MAGIC         ON s.prod_order_no = r.prod_order_no
# MAGIC        AND s.prod_order_line_no = r.prod_order_line_no
# MAGIC        AND s.operation_no = r.operation_no
# MAGIC ),
# MAGIC 
# MAGIC transformed AS (
# MAGIC 
# MAGIC     SELECT
# MAGIC 
# MAGIC         -- Bangkok timezone (+7)
# MAGIC         created_on_ts + INTERVAL 7 HOURS AS created_on,
# MAGIC         modified_on_ts + INTERVAL 7 HOURS AS modified_on,
# MAGIC 
# MAGIC         run_time AS standard_time,
# MAGIC 
# MAGIC         run_time * ABS(quantity) AS standard_time_total,
# MAGIC 
# MAGIC         FLOOR(
# MAGIC             (
# MAGIC                 CAST(modified_on_ts AS BIGINT)
# MAGIC                 - CAST(created_on_ts AS BIGINT)
# MAGIC             ) / 3600.0
# MAGIC         ) AS diff_hours,
# MAGIC 
# MAGIC         FLOOR(
# MAGIC             (
# MAGIC                 CAST(modified_on_ts AS BIGINT)
# MAGIC                 - CAST(created_on_ts AS BIGINT)
# MAGIC             ) / 60.0
# MAGIC         ) AS diff_minutes,
# MAGIC 
# MAGIC         antenna_id,
# MAGIC         rfid_transaction_name,
# MAGIC         user_id,
# MAGIC 
# MAGIC         prod_line,
# MAGIC         cell_line,
# MAGIC 
# MAGIC         prod_order_status,
# MAGIC 
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no,
# MAGIC 
# MAGIC         type_name,
# MAGIC 
# MAGIC         open,
# MAGIC 
# MAGIC         operation_no,
# MAGIC         item_no,
# MAGIC 
# MAGIC         quantity,
# MAGIC         remaining_quantity,
# MAGIC 
# MAGIC         sales_order_no,
# MAGIC 
# MAGIC         current_location_code,
# MAGIC         past_location_code,
# MAGIC 
# MAGIC         machine_center_no,
# MAGIC         employee_no,
# MAGIC 
# MAGIC         CASE
# MAGIC 
# MAGIC             WHEN TRIM(current_location_code) IS NULL
# MAGIC                  OR LENGTH(TRIM(current_location_code)) = 0
# MAGIC             THEN COALESCE(
# MAGIC                     NULLIF(TRIM(machine_center_no), ''),
# MAGIC                     TRIM(current_location_code)
# MAGIC                  )
# MAGIC 
# MAGIC             WHEN UPPER(SUBSTR(TRIM(current_location_code), 1, 4)) = 'CELL'
# MAGIC             THEN COALESCE(
# MAGIC                     NULLIF(TRIM(machine_center_no), ''),
# MAGIC                     TRIM(current_location_code)
# MAGIC                  )
# MAGIC 
# MAGIC             ELSE TRIM(current_location_code)
# MAGIC 
# MAGIC         END AS CorrectCurrentLocation,
# MAGIC 
# MAGIC         change_ts
# MAGIC 
# MAGIC     FROM joined
# MAGIC ),
# MAGIC 
# MAGIC with_rowid AS (
# MAGIC 
# MAGIC     SELECT
# MAGIC         *,
# MAGIC 
# MAGIC         MD5(
# MAGIC             CONCAT_WS(
# MAGIC                 '||',
# MAGIC 
# MAGIC                 COALESCE(prod_order_no, ''),
# MAGIC                 COALESCE(CAST(prod_order_line_no AS STRING), ''),
# MAGIC                 COALESCE(CAST(operation_no AS STRING), ''),
# MAGIC                 COALESCE(item_no, ''),
# MAGIC                 COALESCE(employee_no, ''),
# MAGIC                 COALESCE(CAST(antenna_id AS STRING), ''),
# MAGIC                 COALESCE(type_name, ''),
# MAGIC                 COALESCE(CAST(created_on AS STRING), ''),
# MAGIC                 COALESCE(CAST(modified_on AS STRING), '')
# MAGIC             )
# MAGIC         ) AS row_id
# MAGIC 
# MAGIC     FROM transformed
# MAGIC ),
# MAGIC 
# MAGIC dedup AS (
# MAGIC 
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC 
# MAGIC         SELECT
# MAGIC             *,
# MAGIC 
# MAGIC             ROW_NUMBER() OVER (
# MAGIC 
# MAGIC                 PARTITION BY row_id
# MAGIC 
# MAGIC                 ORDER BY
# MAGIC                     modified_on DESC NULLS LAST,
# MAGIC                     created_on DESC NULLS LAST,
# MAGIC                     rfid_transaction_name ASC NULLS LAST,
# MAGIC                     antenna_id ASC NULLS LAST
# MAGIC 
# MAGIC             ) AS rn
# MAGIC 
# MAGIC         FROM with_rowid
# MAGIC     )
# MAGIC 
# MAGIC     WHERE rn = 1
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     created_on,
# MAGIC     modified_on,
# MAGIC     standard_time,
# MAGIC     standard_time_total,
# MAGIC     diff_hours,
# MAGIC     diff_minutes,
# MAGIC     antenna_id,
# MAGIC     rfid_transaction_name,
# MAGIC     user_id,
# MAGIC     prod_line,
# MAGIC     cell_line,
# MAGIC     prod_order_status,
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     type_name,
# MAGIC     open,
# MAGIC     operation_no,
# MAGIC     item_no,
# MAGIC     quantity,
# MAGIC     remaining_quantity,
# MAGIC     sales_order_no,
# MAGIC     current_location_code,
# MAGIC     past_location_code,
# MAGIC     machine_center_no,
# MAGIC     employee_no,
# MAGIC     CorrectCurrentLocation,
# MAGIC     change_ts,
# MAGIC     row_id
# MAGIC FROM dedup;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# # Plating Output

# MARKDOWN ********************

# ## Silver + Gold Sales Order

# CELL ********************

# MAGIC %%sql
# MAGIC -- ==========================================================
# MAGIC -- Job: Gold_Production_Lakehouse.prod.gold_plating_output
# MAGIC -- FULL REPLACE Spark SQL
# MAGIC -- Includes final business-key dedupe cleanup logic
# MAGIC -- ==========================================================
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_plating_output
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC WITH hdr_src AS (
# MAGIC     SELECT
# MAGIC         `No.`              AS prod_order_no,
# MAGIC         `Status`           AS prod_order_status,
# MAGIC         `Sales Order No.`  AS sales_order_no,
# MAGIC         `SystemCreatedAt`  AS created_on,
# MAGIC         `SystemModifiedAt` AS modified_on
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Production Order`
# MAGIC ),
# MAGIC 
# MAGIC hdr_src_1 AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY prod_order_no
# MAGIC                 ORDER BY modified_on DESC NULLS LAST, created_on DESC NULLS LAST
# MAGIC             ) AS rn
# MAGIC         FROM hdr_src
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC so_src_1 AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY SalesorderNo
# MAGIC                 ORDER BY SalesorderNo
# MAGIC             ) AS rn
# MAGIC         FROM Gold_Production_Lakehouse.prod.gold_sales_order
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC it_src AS (
# MAGIC     SELECT
# MAGIC         `No.`                AS item_no,
# MAGIC         `Item Category Code` AS item_category_code,
# MAGIC         `Product Type`       AS prod_type
# MAGIC     FROM Silver_BC_Lakehouse.bc.Item
# MAGIC ),
# MAGIC 
# MAGIC it_src_1 AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY item_no
# MAGIC                 ORDER BY item_no
# MAGIC             ) AS rn
# MAGIC         FROM it_src
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC hdr AS (
# MAGIC     SELECT
# MAGIC         h.prod_order_no,
# MAGIC         h.prod_order_status,
# MAGIC         h.sales_order_no,
# MAGIC         so.CusNo,
# MAGIC         so.CusAbbr
# MAGIC     FROM hdr_src_1 h
# MAGIC     INNER JOIN so_src_1 so
# MAGIC         ON so.SalesorderNo = h.sales_order_no
# MAGIC ),
# MAGIC 
# MAGIC status_enriched AS (
# MAGIC     SELECT
# MAGIC         s.created_on,
# MAGIC         s.modified_on,
# MAGIC         s.created_on AS created_on_time,
# MAGIC 
# MAGIC         s.prod_order_no,
# MAGIC         CAST(s.prod_order_line_no AS STRING) AS prod_order_line_no,
# MAGIC 
# MAGIC         s.type_name,
# MAGIC         s.prod_order_status,
# MAGIC         s.open,
# MAGIC         s.sales_order_no,
# MAGIC 
# MAGIC         s.current_location_code,
# MAGIC         s.past_location_code,
# MAGIC         s.employee_no,
# MAGIC         s.user_id,
# MAGIC 
# MAGIC         s.quantity,
# MAGIC         s.remaining_quantity,
# MAGIC         s.item_no,
# MAGIC         s.machine_center_no,
# MAGIC 
# MAGIC         CAST((-1 * s.quantity) AS BIGINT) AS out_qty,
# MAGIC 
# MAGIC         CONCAT(
# MAGIC             s.prod_order_no,
# MAGIC             CAST(s.prod_order_line_no AS STRING)
# MAGIC         ) AS pol,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN NULLIF(TRIM(s.current_location_code), '') IS NULL
# MAGIC               OR UPPER(SUBSTRING(TRIM(s.current_location_code), 1, 4)) = 'CELL'
# MAGIC             THEN COALESCE(
# MAGIC                     NULLIF(TRIM(s.machine_center_no), ''),
# MAGIC                     TRIM(s.current_location_code)
# MAGIC                  )
# MAGIC             ELSE TRIM(s.current_location_code)
# MAGIC         END AS CorrectCurrentLocation
# MAGIC 
# MAGIC     FROM Silver_Production_Lakehouse.prod.silver_prod_order_status s
# MAGIC ),
# MAGIC 
# MAGIC status_latest AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY
# MAGIC                     prod_order_no,
# MAGIC                     prod_order_line_no,
# MAGIC                     item_no,
# MAGIC                     CorrectCurrentLocation,
# MAGIC                     type_name
# MAGIC                 ORDER BY
# MAGIC                     created_on DESC NULLS LAST,
# MAGIC                     modified_on DESC NULLS LAST
# MAGIC             ) AS rn
# MAGIC         FROM status_enriched
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC final_df0 AS (
# MAGIC     SELECT
# MAGIC         s.created_on,
# MAGIC         s.modified_on,
# MAGIC         s.created_on_time,
# MAGIC 
# MAGIC         s.prod_order_no,
# MAGIC         s.prod_order_line_no,
# MAGIC 
# MAGIC         s.type_name,
# MAGIC         s.prod_order_status,
# MAGIC         s.open,
# MAGIC 
# MAGIC         h.sales_order_no,
# MAGIC 
# MAGIC         s.current_location_code,
# MAGIC         s.past_location_code,
# MAGIC         s.employee_no,
# MAGIC         s.user_id,
# MAGIC 
# MAGIC         s.quantity,
# MAGIC         s.remaining_quantity,
# MAGIC         s.item_no,
# MAGIC         s.machine_center_no,
# MAGIC 
# MAGIC         s.out_qty,
# MAGIC         s.pol,
# MAGIC         s.CorrectCurrentLocation,
# MAGIC 
# MAGIC         h.CusNo,
# MAGIC         h.CusAbbr,
# MAGIC 
# MAGIC         SUBSTRING(s.item_no, 1, 1) AS item_type,
# MAGIC         it.item_category_code AS item_category,
# MAGIC         it.prod_type
# MAGIC 
# MAGIC     FROM status_latest s
# MAGIC     INNER JOIN hdr h
# MAGIC         ON s.prod_order_no = h.prod_order_no
# MAGIC     LEFT JOIN it_src_1 it
# MAGIC         ON it.item_no = s.item_no
# MAGIC ),
# MAGIC 
# MAGIC normalized AS (
# MAGIC     SELECT
# MAGIC         created_on,
# MAGIC         modified_on,
# MAGIC         created_on_time,
# MAGIC 
# MAGIC         TRIM(prod_order_no) AS prod_order_no,
# MAGIC         CAST(prod_order_line_no AS STRING) AS prod_order_line_no,
# MAGIC         TRIM(type_name) AS type_name,
# MAGIC 
# MAGIC         prod_order_status,
# MAGIC         open,
# MAGIC         sales_order_no,
# MAGIC 
# MAGIC         current_location_code,
# MAGIC         past_location_code,
# MAGIC         employee_no,
# MAGIC         user_id,
# MAGIC 
# MAGIC         quantity,
# MAGIC         remaining_quantity,
# MAGIC         TRIM(item_no) AS item_no,
# MAGIC         machine_center_no,
# MAGIC 
# MAGIC         out_qty,
# MAGIC         pol,
# MAGIC 
# MAGIC         TRIM(CorrectCurrentLocation) AS CorrectCurrentLocation,
# MAGIC 
# MAGIC         CusNo,
# MAGIC         CusAbbr,
# MAGIC 
# MAGIC         item_type,
# MAGIC         item_category,
# MAGIC         prod_type
# MAGIC 
# MAGIC     FROM final_df0
# MAGIC ),
# MAGIC 
# MAGIC dedup AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY
# MAGIC                     prod_order_no,
# MAGIC                     prod_order_line_no,
# MAGIC                     item_no,
# MAGIC                     type_name,
# MAGIC                     CorrectCurrentLocation
# MAGIC                 ORDER BY
# MAGIC                     created_on DESC NULLS LAST,
# MAGIC                     modified_on DESC NULLS LAST
# MAGIC             ) AS rn
# MAGIC         FROM normalized
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     created_on,
# MAGIC     modified_on,
# MAGIC     created_on_time,
# MAGIC 
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC 
# MAGIC     type_name,
# MAGIC     prod_order_status,
# MAGIC     open,
# MAGIC 
# MAGIC     sales_order_no,
# MAGIC 
# MAGIC     current_location_code,
# MAGIC     past_location_code,
# MAGIC     employee_no,
# MAGIC     user_id,
# MAGIC 
# MAGIC     quantity,
# MAGIC     remaining_quantity,
# MAGIC 
# MAGIC     item_no,
# MAGIC     machine_center_no,
# MAGIC 
# MAGIC     out_qty,
# MAGIC     pol,
# MAGIC 
# MAGIC     CorrectCurrentLocation,
# MAGIC 
# MAGIC     CusNo,
# MAGIC     CusAbbr,
# MAGIC 
# MAGIC     item_type,
# MAGIC     item_category,
# MAGIC     prod_type
# MAGIC 
# MAGIC FROM dedup;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# # Compare Actual vs Planned

# MARKDOWN ********************

# ## All Silver

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
SOURCE_FIRM   = "Silver_Production_Lakehouse.prod.silver_firm_plan_log"

# CHANGE: use BC mirror Production Order instead of silver_prod_order_header
SOURCE_HEADER = "Silver_BC_Lakehouse.bc.`Production Order`"

TARGET_TABLE  = "Gold_Production_Lakehouse.prod.gold_compare_plan_vs_actual"

# -------------------------------------------------------------------
# Load sources (full refresh to keep cumulative correct)
# -------------------------------------------------------------------
firm_df   = spark.table(SOURCE_FIRM)
header_df = spark.table(SOURCE_HEADER)

# Ensure date types
firm_df = firm_df.withColumn("finish_date", F.to_date("finish_date"))

# BC mirror columns have spaces/dots -> select + rename safely
header_df = (
    header_df.selectExpr(
        "`No.` as prod_order_no",
        "`For Item` as FG_item_no",
        "`Due Date` as prod_order_due_date"
    )
    .withColumn("prod_order_due_date", F.to_date("prod_order_due_date"))
)

# -------------------------------------------------------------------
# Join exactly as in your SQL (adapted to BC columns)
#   F.prod_order_no == H.prod_order_no
#   F.item_no       == H.FG_item_no  (BC: `For Item`)
# -------------------------------------------------------------------
joined_df = (
    firm_df.alias("F")
    .join(
        header_df.alias("H"),
        (F.col("F.prod_order_no") == F.col("H.prod_order_no")) &
        (F.col("F.item_no")       == F.col("H.FG_item_no")),
        "inner"
    )
)

# -------------------------------------------------------------------
# Base SELECT (matches your SQL SELECT DISTINCT)
# -------------------------------------------------------------------
base_df = (
    joined_df
    .select(
        F.col("F.assigned_cell").alias("assigned_cell"),
        F.col("F.customer_name").alias("customer_name"),
        F.col("F.finish_week").alias("finish_week"),
        F.col("F.requested_week").alias("requested_week"),
        F.col("F.item_no").alias("item_no"),
        F.col("F.material").alias("material"),
        F.col("F.prod_order_no").alias("prod_order_no"),
        F.col("F.prod_order_line_no").alias("prod_order_line_no"),
        F.col("F.sales_order_no").alias("sales_order_no"),
        F.col("F.quantity").alias("quantity"),
        F.col("F.remark").alias("remark"),
        F.col("F.finish_date").alias("finish_date"),
        F.col("H.prod_order_due_date").alias("cur_due")
    )
    .distinct()
)

# -------------------------------------------------------------------
# Cumulative quantities:
#   - from earliest requested_week to this requested_week (per cell)
#   - from earliest finish_week    to this finish_week    (per cell)
# -------------------------------------------------------------------
w_req_cum = (
    Window
    .partitionBy("assigned_cell")
    .orderBy("requested_week")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

w_fin_cum = (
    Window
    .partitionBy("assigned_cell")
    .orderBy("finish_week")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

result_df = (
    base_df
    .withColumn("acc_qty_requested_week", F.sum("quantity").over(w_req_cum))
    .withColumn("acc_qty_finish_week",    F.sum("quantity").over(w_fin_cum))
)

# -------------------------------------------------------------------
# Write gold table (full overwrite to keep cumulative consistent)
# -------------------------------------------------------------------
(
    result_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_TABLE)
)

print("Full refresh of gold_compare_plan_vs_actual completed.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Loading Capacity

# MARKDOWN ********************

# ## All Silver

# CELL ********************

from pyspark.sql import functions as F

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
SOURCE_TABLE = "Silver_Production_Lakehouse.prod.silver_loading_capacity"
TARGET_TABLE = "Gold_Production_Lakehouse.prod.gold_loading_capacity"

# -------------------------------------------------------------------
# Helper: add 7 hours and cast to date
# -------------------------------------------------------------------
def add_7h_to_date(col_name: str):
    """
    CAST(DATEADD(HOUR, 7, CAST(col AS datetime)) AS date)
    """
    return F.to_date(
        F.to_timestamp(F.col(col_name)) + F.expr("INTERVAL 7 HOURS")
    )

# -------------------------------------------------------------------
# Load source
# -------------------------------------------------------------------
src = spark.table(SOURCE_TABLE)

# -------------------------------------------------------------------
# Transform (FULL FIXED VERSION)
# -------------------------------------------------------------------
df = (
    src.select(
        # -----------------------------------------------------------
        # Identifiers / status
        # -----------------------------------------------------------
        F.col("prod_order_status"),
        F.col("status"),
        F.col("prod_order_no"),
        F.col("prod_order_line_no"),

        # -----------------------------------------------------------
        # Customer / item
        # -----------------------------------------------------------
        F.col("customer_no"),
        F.col("customer_name"),
        F.col("customer_abbreviation"),
        F.col("sales_order_no"),
        F.col("FG_item_no"),
        F.col("item_no"),
        F.col("item_material"),
        F.col("item_category"),

        # -----------------------------------------------------------
        # Routing / operation
        # -----------------------------------------------------------
        F.col("type_name"),
        F.col("routing_no"),
        F.col("operation_no"),
        F.col("operation_position"),
        F.col("cell_routing"),
        F.col("prod_line"),

        # -----------------------------------------------------------
        # TRUE ISO week (Spark native)
        # -----------------------------------------------------------
        F.weekofyear(
            F.to_timestamp(F.col("prod_order_due_date")) + F.expr("INTERVAL 7 HOURS")
        ).alias("prod_order_due_week"),


        # -----------------------------------------------------------
        # Quantities / capacity
        # -----------------------------------------------------------
        F.col("prod_line_quantity"),
        F.col("prod_line_finished_quantity"),
        F.col("prod_line_remaining_quantity"),
        F.col("require_capacity"),
        F.col("remaining_required_capacity"),
        F.col("actual_capacity_used"),

        # -----------------------------------------------------------
        # Dates (+7h → date)
        # -----------------------------------------------------------
        add_7h_to_date("sales_order_requested_date").alias(
            "sales_order_requested_date"
        ),
        add_7h_to_date("prod_order_due_date").alias(
            "prod_order_due_date"
        ),


        # -----------------------------------------------------------
        # Manpower
        # -----------------------------------------------------------
        F.col("manpower"),
        F.col("ot_manpower"),
        F.col("ot_sat_manpoer"),

        # -----------------------------------------------------------
        # Synthetic watermark
        # -----------------------------------------------------------
        F.current_timestamp().alias("watermark_ts"),
    )
)

# -------------------------------------------------------------------
# Write to Gold (overwrite refresh)
# -------------------------------------------------------------------
(
    df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(TARGET_TABLE)
)

print("gold_loading_capacity refreshed successfully.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }
