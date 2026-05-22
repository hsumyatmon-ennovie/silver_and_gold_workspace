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

# # Production Status

# MARKDOWN ********************

# ## All Silver

# CELL ********************

# MAGIC %%sql
# MAGIC -- ==============================================================
# MAGIC -- FULL REPLACE
# MAGIC -- Source: Silver_Production_Lakehouse.prod.silver_prod_order_status
# MAGIC -- Target: prod.gold_production_status
# MAGIC -- ==============================================================
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_production_status
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact = true
# MAGIC )
# MAGIC AS
# MAGIC WITH
# MAGIC src AS (
# MAGIC   SELECT
# MAGIC     created_on,
# MAGIC     modified_on,
# MAGIC     prod_order_no,
# MAGIC     operation_no,
# MAGIC     CAST(prod_order_line_no AS STRING) AS prod_order_line_no,
# MAGIC     type_name,
# MAGIC     prod_order_status,
# MAGIC     open,
# MAGIC     sales_order_no,
# MAGIC     current_location_code,
# MAGIC     past_location_code,
# MAGIC     employee_no,
# MAGIC     user_id,
# MAGIC     quantity,
# MAGIC     remaining_quantity,
# MAGIC     item_no,
# MAGIC     machine_center_no
# MAGIC   FROM Silver_Production_Lakehouse.prod.silver_prod_order_status
# MAGIC ),
# MAGIC 
# MAGIC calc AS (
# MAGIC   SELECT
# MAGIC     s.*,
# MAGIC 
# MAGIC     CAST((-1 * s.quantity) AS BIGINT) AS out_qty,
# MAGIC     concat_ws('', s.prod_order_no, s.prod_order_line_no) AS pol,
# MAGIC     s.created_on AS created_on_time,
# MAGIC 
# MAGIC     -- trim inputs like PySpark
# MAGIC     trim(s.current_location_code) AS current_location_code_t,
# MAGIC     trim(s.machine_center_no)     AS machine_center_no_t,
# MAGIC 
# MAGIC     -- CorrectCurrentLocation rules
# MAGIC     CASE
# MAGIC       WHEN trim(s.current_location_code) IS NULL OR length(trim(s.current_location_code)) = 0 THEN
# MAGIC         coalesce(
# MAGIC           CASE WHEN length(trim(s.machine_center_no)) > 0 THEN trim(s.machine_center_no) END,
# MAGIC           trim(s.current_location_code)
# MAGIC         )
# MAGIC       WHEN upper(substr(trim(s.current_location_code), 1, 4)) = 'CELL' THEN
# MAGIC         coalesce(
# MAGIC           CASE WHEN length(trim(s.machine_center_no)) > 0 THEN trim(s.machine_center_no) END,
# MAGIC           trim(s.current_location_code)
# MAGIC         )
# MAGIC       ELSE trim(s.current_location_code)
# MAGIC     END AS CorrectCurrentLocation
# MAGIC   FROM src s
# MAGIC ),
# MAGIC 
# MAGIC -- DEDUPE: latest by created_on desc nulls last
# MAGIC dedup AS (
# MAGIC   SELECT *
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       c.*,
# MAGIC       row_number() OVER (
# MAGIC         PARTITION BY c.prod_order_no, c.item_no, c.CorrectCurrentLocation, c.type_name
# MAGIC         ORDER BY c.created_on DESC NULLS LAST
# MAGIC       ) AS rn
# MAGIC     FROM calc c
# MAGIC   ) x
# MAGIC   WHERE x.rn = 1
# MAGIC ),
# MAGIC 
# MAGIC -- Normalize for robust filtering (trim/uppercase exacts)
# MAGIC norm AS (
# MAGIC   SELECT
# MAGIC     -- output columns (match your final_src projection)
# MAGIC     created_on,
# MAGIC     modified_on,
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     operation_no,
# MAGIC     type_name,
# MAGIC     prod_order_status,
# MAGIC     open,
# MAGIC     sales_order_no,
# MAGIC     current_location_code_t AS current_location_code,
# MAGIC     past_location_code,
# MAGIC     employee_no,
# MAGIC     user_id,
# MAGIC     quantity,
# MAGIC     remaining_quantity,
# MAGIC     item_no,
# MAGIC     machine_center_no_t AS machine_center_no,
# MAGIC     out_qty,
# MAGIC     pol,
# MAGIC     created_on_time,
# MAGIC     CorrectCurrentLocation,
# MAGIC 
# MAGIC     upper(trim(type_name))          AS type_name_norm,
# MAGIC     upper(trim(open))               AS open_norm,
# MAGIC     upper(trim(prod_order_status))  AS status_norm
# MAGIC   FROM dedup
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC   created_on,
# MAGIC   modified_on,
# MAGIC   prod_order_no,
# MAGIC   prod_order_line_no,
# MAGIC   operation_no,
# MAGIC   type_name,
# MAGIC   prod_order_status,
# MAGIC   open,
# MAGIC   sales_order_no,
# MAGIC   current_location_code,
# MAGIC   past_location_code,
# MAGIC   employee_no,
# MAGIC   user_id,
# MAGIC   quantity,
# MAGIC   remaining_quantity,
# MAGIC   item_no,
# MAGIC   machine_center_no,
# MAGIC   out_qty,
# MAGIC   pol,
# MAGIC   created_on_time,
# MAGIC   CorrectCurrentLocation
# MAGIC FROM norm
# MAGIC WHERE
# MAGIC   type_name_norm = 'IN LOCATION IN'
# MAGIC   AND open_norm  = 'YES'
# MAGIC   AND status_norm = 'RELEASED'
# MAGIC ;
# MAGIC 
# MAGIC -- Optional maintenance (uncomment if you want)
# MAGIC -- OPTIMIZE prod.gold_production_status
# MAGIC -- ZORDER BY (prod_order_no, item_no, CorrectCurrentLocation, type_name, modified_on);
# MAGIC -- VACUUM prod.gold_production_status RETAIN 168 HOURS;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Production Status Cycle Time

# MARKDOWN ********************

# ## All Silver

# CELL ********************

# MAGIC %%sql
# MAGIC -- ==============================================================
# MAGIC -- FULL REPLACE: gold_production_status_cycle_time
# MAGIC -- ==============================================================
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_production_status_cycle_time
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact = true
# MAGIC )
# MAGIC AS
# MAGIC WITH
# MAGIC -- -------------------------------------------------------------------
# MAGIC -- 0) prod_routing_group : logic ตามสูตรใหม่ (from BC Prod Order Routing Line)
# MAGIC -- -------------------------------------------------------------------
# MAGIC routing_src AS (
# MAGIC   SELECT
# MAGIC     `SystemCreatedAt`       AS created_on,
# MAGIC     `SystemModifiedAt`      AS modified_on,
# MAGIC     `Prod. Order No.`       AS prod_order_no,
# MAGIC     `Status`                AS prod_order_status,
# MAGIC     `Routing Reference No.` AS prod_order_line_no,
# MAGIC     `Routing No.`           AS item_no,
# MAGIC     `Operation No.`         AS operation_no,
# MAGIC     `Type`                  AS operation_type,
# MAGIC     `No.`                   AS routing_no,
# MAGIC     `Run Time`              AS run_time,
# MAGIC 
# MAGIC     CASE
# MAGIC       WHEN `Operation No.` IS NULL THEN CAST(NULL AS INT)
# MAGIC       ELSE CAST(split(concat_ws('', `Operation No.`, '.'), '\\.')[0] AS INT)
# MAGIC     END AS operation_group
# MAGIC   FROM Silver_BC_Lakehouse.bc.`Prod Order Routing Line`
# MAGIC ),
# MAGIC 
# MAGIC routing_calc AS (
# MAGIC   SELECT
# MAGIC     r.*,
# MAGIC     CASE WHEN r.operation_type = 'Machine Center' THEN r.routing_no ELSE NULL END AS routing_no_machine_center,
# MAGIC     max(CASE WHEN r.operation_type = 'Work Center' THEN r.routing_no END)
# MAGIC       OVER (PARTITION BY r.prod_order_no, r.prod_order_line_no, r.operation_group) AS routing_no_work_center
# MAGIC   FROM routing_src r
# MAGIC ),
# MAGIC 
# MAGIC prod_routing_group AS (
# MAGIC   SELECT *
# MAGIC   FROM routing_calc
# MAGIC   WHERE
# MAGIC     operation_type = 'Machine Center'
# MAGIC     OR (operation_group = 9 AND operation_no = '009')
# MAGIC ),
# MAGIC 
# MAGIC routing_map AS (
# MAGIC   SELECT
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     operation_group AS op_major,
# MAGIC     max(routing_no_machine_center) AS routing_no_machine_center,
# MAGIC     max(routing_no_work_center)    AS routing_no_work_center
# MAGIC   FROM prod_routing_group
# MAGIC   GROUP BY prod_order_no, prod_order_line_no, operation_group
# MAGIC ),
# MAGIC 
# MAGIC -- -------------------------------------------------------------------
# MAGIC -- 1) SILVER: silver_prod_order_status_all + map type/status
# MAGIC -- -------------------------------------------------------------------
# MAGIC silver_src AS (
# MAGIC   SELECT
# MAGIC     (createdon + INTERVAL 7 HOURS) AS created_on_bkk,
# MAGIC     cr535_prodorderno     AS prod_order_no,
# MAGIC     cr535_prodorderlineno AS prod_order_line_no,
# MAGIC     cr535_type            AS cr535_type,
# MAGIC     cr535_prodorderstatus AS cr535_prodorderstatus,
# MAGIC     cr535_operationno     AS operation_no,
# MAGIC     cr535_itemno          AS item_no,
# MAGIC     cr535_quantity        AS quantity,
# MAGIC     cr535_machinecenterno AS machine_center_no
# MAGIC   FROM Silver_Production_Lakehouse.prod.silver_prod_order_status_all
# MAGIC ),
# MAGIC 
# MAGIC silver_mapped AS (
# MAGIC   SELECT
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     operation_no,
# MAGIC     item_no,
# MAGIC     quantity,
# MAGIC     machine_center_no,
# MAGIC     date_trunc('second', created_on_bkk) AS created_on_bkk,
# MAGIC 
# MAGIC     CASE cr535_type
# MAGIC       WHEN 184930000 THEN 'In location in'
# MAGIC       WHEN 184930001 THEN 'Out location'
# MAGIC       WHEN 184930002 THEN 'To employee'
# MAGIC       WHEN 184930003 THEN 'From employee'
# MAGIC       ELSE NULL
# MAGIC     END AS type_name,
# MAGIC 
# MAGIC     CASE cr535_prodorderstatus
# MAGIC       WHEN 184930000 THEN 'Simulated'
# MAGIC       WHEN 184930001 THEN 'Planned'
# MAGIC       WHEN 184930002 THEN 'Firm Planned'
# MAGIC       WHEN 184930003 THEN 'Released'
# MAGIC       WHEN 184930004 THEN 'Finished'
# MAGIC       ELSE NULL
# MAGIC     END AS prod_order_status
# MAGIC   FROM silver_src
# MAGIC ),
# MAGIC 
# MAGIC -- -------------------------------------------------------------------
# MAGIC -- 1.1) Join cell + filter Released + 4 types + dedupe
# MAGIC -- -------------------------------------------------------------------
# MAGIC silver_with_cell AS (
# MAGIC   SELECT
# MAGIC     s.prod_order_no,
# MAGIC     s.prod_order_line_no,
# MAGIC     s.type_name,
# MAGIC     s.operation_no,
# MAGIC     s.item_no,
# MAGIC     s.quantity,
# MAGIC     s.machine_center_no,
# MAGIC     l.prod_line,
# MAGIC     l.cell_line,
# MAGIC     s.created_on_bkk
# MAGIC   FROM silver_mapped s
# MAGIC   LEFT JOIN Gold_Production_Lakehouse.prod.gold_production_asgn_cell l
# MAGIC     ON s.prod_order_no = l.prod_order_no
# MAGIC    AND s.prod_order_line_no = l.prod_order_line_no
# MAGIC   WHERE
# MAGIC     s.type_name IN ('In location in','Out location','To employee','From employee')
# MAGIC     AND trim(s.prod_order_status) = 'Released'
# MAGIC ),
# MAGIC 
# MAGIC base_data AS (
# MAGIC   SELECT *
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       swc.*,
# MAGIC       row_number() OVER (
# MAGIC         PARTITION BY
# MAGIC           swc.prod_order_no,
# MAGIC           swc.prod_order_line_no,
# MAGIC           swc.created_on_bkk,
# MAGIC           swc.type_name,
# MAGIC           swc.operation_no,
# MAGIC           swc.machine_center_no
# MAGIC         ORDER BY swc.created_on_bkk, swc.item_no
# MAGIC       ) AS rn
# MAGIC     FROM silver_with_cell swc
# MAGIC   ) x
# MAGIC   WHERE x.rn = 1
# MAGIC ),
# MAGIC 
# MAGIC -- -------------------------------------------------------------------
# MAGIC -- 2) op_major: เลขติดกันชุดแรกด้านหน้า
# MAGIC -- -------------------------------------------------------------------
# MAGIC wr AS (
# MAGIC   SELECT
# MAGIC     b.*,
# MAGIC     CAST(regexp_extract(b.operation_no, '^(\\d+)', 1) AS INT) AS op_major
# MAGIC   FROM base_data b
# MAGIC ),
# MAGIC 
# MAGIC -- -------------------------------------------------------------------
# MAGIC -- 2.2) wr + routing_map
# MAGIC -- -------------------------------------------------------------------
# MAGIC wr_with_routing AS (
# MAGIC   SELECT
# MAGIC     w.*,
# MAGIC     rm.routing_no_machine_center,
# MAGIC     rm.routing_no_work_center
# MAGIC   FROM wr w
# MAGIC   LEFT JOIN routing_map rm
# MAGIC     ON w.prod_order_no = rm.prod_order_no
# MAGIC    AND w.prod_order_line_no = rm.prod_order_line_no
# MAGIC    AND w.op_major = rm.op_major
# MAGIC ),
# MAGIC 
# MAGIC -- -------------------------------------------------------------------
# MAGIC -- 3) หา event ถัดไปตามเวลาในแต่ละ (prod_order_no, line, op_major)
# MAGIC -- -------------------------------------------------------------------
# MAGIC wr_ext AS (
# MAGIC   SELECT
# MAGIC     x.*,
# MAGIC 
# MAGIC     min(CASE WHEN x.type_name = 'Out location'   THEN x.created_on_bkk END)
# MAGIC       OVER (
# MAGIC         PARTITION BY x.prod_order_no, x.prod_order_line_no, x.op_major
# MAGIC         ORDER BY x.created_on_bkk
# MAGIC         ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
# MAGIC       ) AS end_out_created,
# MAGIC 
# MAGIC     min(CASE WHEN x.type_name = 'From employee'  THEN x.created_on_bkk END)
# MAGIC       OVER (
# MAGIC         PARTITION BY x.prod_order_no, x.prod_order_line_no, x.op_major
# MAGIC         ORDER BY x.created_on_bkk
# MAGIC         ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
# MAGIC       ) AS end_from_created,
# MAGIC 
# MAGIC     min(CASE WHEN x.type_name = 'To employee'    THEN x.created_on_bkk END)
# MAGIC       OVER (
# MAGIC         PARTITION BY x.prod_order_no, x.prod_order_line_no, x.op_major
# MAGIC         ORDER BY x.created_on_bkk
# MAGIC         ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
# MAGIC       ) AS end_to_created
# MAGIC 
# MAGIC   FROM wr_with_routing x
# MAGIC ),
# MAGIC 
# MAGIC -- -------------------------------------------------------------------
# MAGIC -- 4) Pair intervals per metric
# MAGIC -- -------------------------------------------------------------------
# MAGIC pair_in_out AS (
# MAGIC   SELECT
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     op_major,
# MAGIC     prod_line,
# MAGIC     cell_line,
# MAGIC     machine_center_no,
# MAGIC     routing_no_machine_center,
# MAGIC     routing_no_work_center,
# MAGIC     item_no,
# MAGIC     quantity,
# MAGIC     created_on_bkk AS t_start,
# MAGIC     end_out_created AS t_end,
# MAGIC     'station' AS metric
# MAGIC   FROM wr_ext
# MAGIC   WHERE type_name = 'In location in' AND end_out_created IS NOT NULL
# MAGIC ),
# MAGIC 
# MAGIC pair_to_from AS (
# MAGIC   SELECT
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     op_major,
# MAGIC     prod_line,
# MAGIC     cell_line,
# MAGIC     machine_center_no,
# MAGIC     routing_no_machine_center,
# MAGIC     routing_no_work_center,
# MAGIC     item_no,
# MAGIC     quantity,
# MAGIC     created_on_bkk AS t_start,
# MAGIC     end_from_created AS t_end,
# MAGIC     'operation' AS metric
# MAGIC   FROM wr_ext
# MAGIC   WHERE type_name = 'To employee' AND end_from_created IS NOT NULL
# MAGIC ),
# MAGIC 
# MAGIC pair_in_to AS (
# MAGIC   SELECT
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     op_major,
# MAGIC     prod_line,
# MAGIC     cell_line,
# MAGIC     machine_center_no,
# MAGIC     routing_no_machine_center,
# MAGIC     routing_no_work_center,
# MAGIC     item_no,
# MAGIC     quantity,
# MAGIC     created_on_bkk AS t_start,
# MAGIC     end_to_created AS t_end,
# MAGIC     'dead' AS metric
# MAGIC   FROM wr_ext
# MAGIC   WHERE type_name = 'In location in' AND end_to_created IS NOT NULL
# MAGIC ),
# MAGIC 
# MAGIC intervals AS (
# MAGIC   SELECT * FROM pair_in_out
# MAGIC   UNION ALL
# MAGIC   SELECT * FROM pair_to_from
# MAGIC   UNION ALL
# MAGIC   SELECT * FROM pair_in_to
# MAGIC ),
# MAGIC 
# MAGIC -- -------------------------------------------------------------------
# MAGIC -- 6) metric_base: min start / max end per key+metric
# MAGIC -- -------------------------------------------------------------------
# MAGIC metric_base AS (
# MAGIC   SELECT
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     op_major,
# MAGIC     prod_line,
# MAGIC     cell_line,
# MAGIC     machine_center_no,
# MAGIC     routing_no_machine_center,
# MAGIC     routing_no_work_center,
# MAGIC     metric,
# MAGIC     min(t_start) AS t_start,
# MAGIC     max(t_end)   AS t_end,
# MAGIC     max(item_no) AS item_no,
# MAGIC     max(quantity) AS quantity
# MAGIC   FROM intervals
# MAGIC   GROUP BY
# MAGIC     prod_order_no, prod_order_line_no, op_major,
# MAGIC     prod_line, cell_line, machine_center_no,
# MAGIC     routing_no_machine_center, routing_no_work_center, metric
# MAGIC ),
# MAGIC 
# MAGIC -- -------------------------------------------------------------------
# MAGIC -- 7) Expand to daily slots using sequence() + explode()
# MAGIC -- -------------------------------------------------------------------
# MAGIC expanded_days AS (
# MAGIC   SELECT
# MAGIC     i.*,
# MAGIC     explode(sequence(to_date(i.t_start), to_date(i.t_end), interval 1 day)) AS d
# MAGIC   FROM intervals i
# MAGIC ),
# MAGIC 
# MAGIC slots AS (
# MAGIC   SELECT
# MAGIC     e.*,
# MAGIC     dayofweek(e.d) AS dow,                     -- 1=Sun .. 7=Sat
# MAGIC     to_timestamp(e.d) AS base_ts               -- midnight
# MAGIC   FROM expanded_days e
# MAGIC ),
# MAGIC 
# MAGIC slots_filtered AS (
# MAGIC   SELECT
# MAGIC     s.*,
# MAGIC 
# MAGIC     (s.base_ts + INTERVAL 8 HOURS)  AS am_start,
# MAGIC     (s.base_ts + INTERVAL 12 HOURS) AS am_end,
# MAGIC 
# MAGIC     (s.base_ts + INTERVAL 13 HOURS) AS pm_start,
# MAGIC     CASE
# MAGIC       WHEN s.dow BETWEEN 2 AND 6 THEN (s.base_ts + INTERVAL 18 HOURS + INTERVAL 20 MINUTES) -- 18:20
# MAGIC       ELSE (s.base_ts + INTERVAL 17 HOURS)                                                  -- Sat 17:00
# MAGIC     END AS pm_end
# MAGIC   FROM slots s
# MAGIC   WHERE s.dow BETWEEN 2 AND 7       -- Mon–Sat
# MAGIC ),
# MAGIC 
# MAGIC -- -------------------------------------------------------------------
# MAGIC -- 9) clip interval per day to AM/PM bands and compute minutes
# MAGIC -- -------------------------------------------------------------------
# MAGIC clip AS (
# MAGIC   SELECT
# MAGIC     sf.*,
# MAGIC 
# MAGIC     CASE
# MAGIC       WHEN to_date(sf.t_start) = sf.d AND sf.t_start > sf.am_start THEN sf.t_start
# MAGIC       ELSE sf.am_start
# MAGIC     END AS am_start_eff,
# MAGIC 
# MAGIC     CASE
# MAGIC       WHEN to_date(sf.t_end) = sf.d AND sf.t_end < sf.am_end THEN sf.t_end
# MAGIC       ELSE sf.am_end
# MAGIC     END AS am_end_eff,
# MAGIC 
# MAGIC     CASE
# MAGIC       WHEN to_date(sf.t_start) = sf.d AND sf.t_start > sf.pm_start THEN sf.t_start
# MAGIC       ELSE sf.pm_start
# MAGIC     END AS pm_start_eff,
# MAGIC 
# MAGIC     CASE
# MAGIC       WHEN to_date(sf.t_end) = sf.d AND sf.t_end < sf.pm_end THEN sf.t_end
# MAGIC       ELSE sf.pm_end
# MAGIC     END AS pm_end_eff
# MAGIC 
# MAGIC   FROM slots_filtered sf
# MAGIC ),
# MAGIC 
# MAGIC clip_minutes AS (
# MAGIC   SELECT
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     op_major,
# MAGIC     prod_line,
# MAGIC     cell_line,
# MAGIC     machine_center_no,
# MAGIC     routing_no_machine_center,
# MAGIC     routing_no_work_center,
# MAGIC     metric,
# MAGIC     t_start,
# MAGIC     t_end,
# MAGIC     d,
# MAGIC 
# MAGIC     CASE
# MAGIC       WHEN am_end_eff > am_start_eff THEN (CAST(am_end_eff AS BIGINT) - CAST(am_start_eff AS BIGINT)) / 60
# MAGIC       ELSE 0
# MAGIC     END AS am_min,
# MAGIC 
# MAGIC     CASE
# MAGIC       WHEN pm_end_eff > pm_start_eff THEN (CAST(pm_end_eff AS BIGINT) - CAST(pm_start_eff AS BIGINT)) / 60
# MAGIC       ELSE 0
# MAGIC     END AS pm_min
# MAGIC   FROM clip
# MAGIC ),
# MAGIC 
# MAGIC -- -------------------------------------------------------------------
# MAGIC -- 10) metric_work: sum working minutes per key+metric
# MAGIC -- -------------------------------------------------------------------
# MAGIC metric_work AS (
# MAGIC   SELECT
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     op_major,
# MAGIC     prod_line,
# MAGIC     cell_line,
# MAGIC     machine_center_no,
# MAGIC     routing_no_machine_center,
# MAGIC     routing_no_work_center,
# MAGIC     metric,
# MAGIC     sum(am_min + pm_min) AS work_min
# MAGIC   FROM clip_minutes
# MAGIC   GROUP BY
# MAGIC     prod_order_no, prod_order_line_no, op_major,
# MAGIC     prod_line, cell_line, machine_center_no,
# MAGIC     routing_no_machine_center, routing_no_work_center, metric
# MAGIC ),
# MAGIC 
# MAGIC -- -------------------------------------------------------------------
# MAGIC -- 11) metric_sum: join base + working minutes
# MAGIC -- -------------------------------------------------------------------
# MAGIC metric_sum AS (
# MAGIC   SELECT
# MAGIC     B.prod_order_no,
# MAGIC     B.prod_order_line_no,
# MAGIC     B.op_major,
# MAGIC     B.prod_line,
# MAGIC     B.cell_line,
# MAGIC     B.machine_center_no,
# MAGIC     B.routing_no_machine_center,
# MAGIC     B.routing_no_work_center,
# MAGIC     B.metric,
# MAGIC     B.t_start,
# MAGIC     B.t_end,
# MAGIC     B.item_no,
# MAGIC     B.quantity,
# MAGIC     coalesce(W.work_min, 0) AS work_min
# MAGIC   FROM metric_base B
# MAGIC   LEFT JOIN metric_work W
# MAGIC     ON B.prod_order_no = W.prod_order_no
# MAGIC    AND B.prod_order_line_no = W.prod_order_line_no
# MAGIC    AND B.op_major = W.op_major
# MAGIC    AND B.prod_line = W.prod_line
# MAGIC    AND B.cell_line = W.cell_line
# MAGIC    AND B.machine_center_no = W.machine_center_no
# MAGIC    AND B.routing_no_machine_center = W.routing_no_machine_center
# MAGIC    AND B.routing_no_work_center = W.routing_no_work_center
# MAGIC    AND B.metric = W.metric
# MAGIC ),
# MAGIC 
# MAGIC -- -------------------------------------------------------------------
# MAGIC -- 12) final aggregation (ResultCTE)
# MAGIC -- -------------------------------------------------------------------
# MAGIC result_cte AS (
# MAGIC   SELECT
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     op_major,
# MAGIC 
# MAGIC     max(prod_line) AS prod_line,
# MAGIC     max(cell_line) AS cell_line,
# MAGIC     max(machine_center_no) AS operation,
# MAGIC     max(routing_no_machine_center) AS routing_no_machine_center,
# MAGIC     max(routing_no_work_center)    AS routing_no_work_center,
# MAGIC     max(item_no) AS item_no,
# MAGIC     max(quantity) AS quantity,
# MAGIC 
# MAGIC     max(CASE WHEN metric = 'station'   THEN t_start END) AS in_created,
# MAGIC     max(CASE WHEN metric = 'station'   THEN t_end   END) AS out_created,
# MAGIC     max(CASE WHEN metric = 'operation' THEN t_start END) AS to_created,
# MAGIC     max(CASE WHEN metric = 'operation' THEN t_end   END) AS from_created,
# MAGIC     max(CASE WHEN metric = 'dead'      THEN t_end   END) AS dead_to_created,
# MAGIC 
# MAGIC     sum(CASE WHEN metric = 'station'   THEN work_min ELSE 0 END) AS station_time,
# MAGIC     sum(CASE WHEN metric = 'operation' THEN work_min ELSE 0 END) AS operation_time,
# MAGIC     sum(CASE WHEN metric = 'dead'      THEN work_min ELSE 0 END) AS dead_time
# MAGIC 
# MAGIC   FROM metric_sum
# MAGIC   GROUP BY prod_order_no, prod_order_line_no, op_major
# MAGIC ),
# MAGIC 
# MAGIC final AS (
# MAGIC   SELECT
# MAGIC     r.*,
# MAGIC     coalesce(
# MAGIC       to_date(r.in_created),
# MAGIC       to_date(r.out_created),
# MAGIC       to_date(r.to_created),
# MAGIC       to_date(r.from_created),
# MAGIC       to_date(r.dead_to_created)
# MAGIC     ) AS in_date
# MAGIC   FROM result_cte r
# MAGIC )
# MAGIC 
# MAGIC SELECT * FROM final
# MAGIC ;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Production All Status

# MARKDOWN ********************

# ## All Silver

# CELL ********************

# MAGIC %%sql
# MAGIC -- ==============================================================
# MAGIC -- FULL REPLACE
# MAGIC -- Source: Silver_Production_Lakehouse.prod.silver_prod_order_status
# MAGIC -- Target: prod.gold_production_all_status
# MAGIC -- ==============================================================
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS prod;
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE prod.gold_production_all_status
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact = true
# MAGIC )
# MAGIC AS
# MAGIC WITH
# MAGIC src AS (
# MAGIC   SELECT
# MAGIC     created_on,
# MAGIC     modified_on,
# MAGIC     prod_order_no,
# MAGIC     operation_no,
# MAGIC     CAST(prod_order_line_no AS STRING) AS prod_order_line_no,
# MAGIC     type_name,
# MAGIC     prod_order_status,
# MAGIC     open,
# MAGIC     sales_order_no,
# MAGIC     current_location_code,
# MAGIC     past_location_code,
# MAGIC     employee_no,
# MAGIC     user_id,
# MAGIC     quantity,
# MAGIC     remaining_quantity,
# MAGIC     item_no,
# MAGIC     machine_center_no
# MAGIC   FROM Silver_Production_Lakehouse.prod.silver_prod_order_status
# MAGIC ),
# MAGIC 
# MAGIC calc AS (
# MAGIC   SELECT
# MAGIC     s.*,
# MAGIC 
# MAGIC     CAST((-1 * s.quantity) AS BIGINT) AS out_qty,
# MAGIC     concat_ws('', s.prod_order_no, s.prod_order_line_no) AS pol,
# MAGIC     s.created_on AS created_on_time,
# MAGIC 
# MAGIC     -- trim like PySpark
# MAGIC     trim(s.current_location_code) AS current_location_code_t,
# MAGIC     trim(s.machine_center_no)     AS machine_center_no_t,
# MAGIC 
# MAGIC     CASE
# MAGIC       WHEN trim(s.current_location_code) IS NULL OR length(trim(s.current_location_code)) = 0 THEN
# MAGIC         coalesce(
# MAGIC           CASE WHEN length(trim(s.machine_center_no)) > 0 THEN trim(s.machine_center_no) END,
# MAGIC           trim(s.current_location_code)
# MAGIC         )
# MAGIC       WHEN upper(substr(trim(s.current_location_code), 1, 4)) = 'CELL' THEN
# MAGIC         coalesce(
# MAGIC           CASE WHEN length(trim(s.machine_center_no)) > 0 THEN trim(s.machine_center_no) END,
# MAGIC           trim(s.current_location_code)
# MAGIC         )
# MAGIC       ELSE trim(s.current_location_code)
# MAGIC     END AS CorrectCurrentLocation
# MAGIC   FROM src s
# MAGIC ),
# MAGIC 
# MAGIC -- DEDUPE: latest by created_on (nulls last) per KEYS
# MAGIC dedup AS (
# MAGIC   SELECT *
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       c.*,
# MAGIC       row_number() OVER (
# MAGIC         PARTITION BY c.prod_order_no, c.item_no, c.CorrectCurrentLocation, c.type_name
# MAGIC         ORDER BY c.created_on DESC NULLS LAST
# MAGIC       ) AS rn
# MAGIC     FROM calc c
# MAGIC   ) x
# MAGIC   WHERE x.rn = 1
# MAGIC ),
# MAGIC 
# MAGIC -- normalization columns exist in PySpark but are not used for filtering in your final_src
# MAGIC norm AS (
# MAGIC   SELECT
# MAGIC     created_on,
# MAGIC     modified_on,
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     operation_no,
# MAGIC     type_name,
# MAGIC     prod_order_status,
# MAGIC     open,
# MAGIC     sales_order_no,
# MAGIC     current_location_code_t AS current_location_code,
# MAGIC     past_location_code,
# MAGIC     employee_no,
# MAGIC     user_id,
# MAGIC     quantity,
# MAGIC     remaining_quantity,
# MAGIC     item_no,
# MAGIC     machine_center_no_t AS machine_center_no,
# MAGIC     out_qty,
# MAGIC     pol,
# MAGIC     created_on_time,
# MAGIC     CorrectCurrentLocation,
# MAGIC 
# MAGIC     upper(trim(type_name))         AS type_name_norm,
# MAGIC     upper(trim(open))              AS open_norm,
# MAGIC     upper(trim(prod_order_status)) AS status_norm
# MAGIC   FROM dedup
# MAGIC )
# MAGIC 
# MAGIC -- IMPORTANT: no filter here (matches your current notebook;
# MAGIC -- the Out location filter is commented out)
# MAGIC SELECT
# MAGIC   created_on,
# MAGIC   modified_on,
# MAGIC   prod_order_no,
# MAGIC   prod_order_line_no,
# MAGIC   operation_no,
# MAGIC   type_name,
# MAGIC   prod_order_status,
# MAGIC   open,
# MAGIC   sales_order_no,
# MAGIC   current_location_code,
# MAGIC   past_location_code,
# MAGIC   employee_no,
# MAGIC   user_id,
# MAGIC   quantity,
# MAGIC   remaining_quantity,
# MAGIC   item_no,
# MAGIC   machine_center_no,
# MAGIC   out_qty,
# MAGIC   pol,
# MAGIC   created_on_time,
# MAGIC   CorrectCurrentLocation
# MAGIC FROM norm
# MAGIC ;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Summary Production Status

# MARKDOWN ********************

# ## Silver + Gold Sales Order + Gold Production Status

# CELL ********************

# MAGIC %%sql
# MAGIC -- =========================================================
# MAGIC -- FULL REPLACE: Gold_Production_Lakehouse.prod.gold_summary_production_status
# MAGIC -- =========================================================
# MAGIC 
# MAGIC -- Replace table fully
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_summary_production_status
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact = true
# MAGIC )
# MAGIC AS
# MAGIC WITH
# MAGIC -- ------------------ Sources ------------------
# MAGIC po_src AS (
# MAGIC   SELECT
# MAGIC     `Status`               AS prod_order_status,
# MAGIC     `No.`                  AS prod_order_no,
# MAGIC     `Description`          AS prod_order_description,
# MAGIC     `Source No.`           AS FG_item_no,
# MAGIC     `Routing No.`          AS item_routing_no,
# MAGIC     `Due Date`             AS prod_order_due_date,
# MAGIC     `Finished Date`        AS prod_order_finished_date,
# MAGIC     `Quantity`             AS prod_order_quantity,
# MAGIC     `Cost Amount`          AS prod_order_cost_amount,
# MAGIC     `No. Series`           AS prod_order_no_series,
# MAGIC     `Starting Date-Time`   AS prod_order_starting_date_time,
# MAGIC     `Ending Date-Time`     AS prod_order_ending_date_time,
# MAGIC     `Sales Order No.`      AS sales_order_no,
# MAGIC     `Sales Order Line No.` AS sales_order_line_no,
# MAGIC     `Prod. Order Type.`    AS prod_order_type,
# MAGIC     `Remark`               AS remark,
# MAGIC     `For Item`             AS ref_item,
# MAGIC     `For Prod.Order No.`   AS ref_prod_order,
# MAGIC     `Location Code`        AS prod_order_location,
# MAGIC     `SystemCreatedAt`      AS created_on,
# MAGIC     `SystemModifiedAt`     AS modified_on
# MAGIC   FROM Silver_BC_Lakehouse.bc.`Production Order`
# MAGIC ),
# MAGIC 
# MAGIC it_src AS (
# MAGIC   SELECT
# MAGIC     `No.`                     AS item_no,
# MAGIC     `Item Category Code`      AS item_category_code
# MAGIC   FROM Silver_BC_Lakehouse.bc.Item
# MAGIC ),
# MAGIC 
# MAGIC pl_src AS (
# MAGIC   SELECT
# MAGIC     `Status`               AS prod_order_status,
# MAGIC     `Prod. Order No.`      AS prod_order_no,
# MAGIC     `Item No.`             AS item_no,
# MAGIC     `Quantity`             AS prod_line_quantity,
# MAGIC     `Finished Quantity`    AS prod_line_finished_quantity,
# MAGIC     `Remaining Quantity`   AS prod_line_remaining_quantity,
# MAGIC     `SystemCreatedAt`      AS created_on,
# MAGIC     `SystemModifiedAt`     AS modified_on
# MAGIC   FROM Silver_BC_Lakehouse.bc.`Prod Order Line`
# MAGIC ),
# MAGIC 
# MAGIC so_src AS (
# MAGIC   SELECT *
# MAGIC   FROM Gold_Production_Lakehouse.prod.gold_sales_order
# MAGIC ),
# MAGIC 
# MAGIC gs_src AS (
# MAGIC   SELECT *
# MAGIC   FROM Gold_Production_Lakehouse.prod.gold_production_status
# MAGIC ),
# MAGIC 
# MAGIC -- engine-anchored due date: MAX(scheduled_end_date) within the latest
# MAGIC -- engine_run per prod_order_no (po_src is at order-header grain), from
# MAGIC -- planning_forward_schedule.
# MAGIC pod_src AS (
# MAGIC   SELECT
# MAGIC     prod_order_no,
# MAGIC     MAX(scheduled_end_date) AS planned_prod_order_due_date
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       prod_order_no,
# MAGIC       scheduled_end_date,
# MAGIC       DENSE_RANK() OVER (
# MAGIC         PARTITION BY prod_order_no
# MAGIC         ORDER BY engine_run_ts DESC
# MAGIC       ) AS run_rank
# MAGIC     FROM Gold_Production_Lakehouse.prod.planning_forward_schedule
# MAGIC   )
# MAGIC   WHERE run_rank = 1
# MAGIC   GROUP BY prod_order_no
# MAGIC ),
# MAGIC 
# MAGIC -- ------------------ Join + filters ------------------
# MAGIC base AS (
# MAGIC   SELECT
# MAGIC     coalesce(pod.planned_prod_order_due_date, po.prod_order_due_date) AS prod_order_due_date,
# MAGIC     po.prod_order_status,
# MAGIC     po.prod_order_no,
# MAGIC     po.prod_order_description,
# MAGIC     po.FG_item_no,
# MAGIC     po.item_routing_no,
# MAGIC     po.prod_order_quantity,
# MAGIC     po.prod_order_type,
# MAGIC     po.sales_order_no,
# MAGIC     po.sales_order_line_no,
# MAGIC     po.prod_order_location,
# MAGIC     po.prod_order_starting_date_time,
# MAGIC     po.prod_order_ending_date_time,
# MAGIC     po.prod_order_finished_date,
# MAGIC 
# MAGIC     pl.prod_line_quantity,
# MAGIC     pl.prod_line_finished_quantity,
# MAGIC     pl.prod_line_remaining_quantity,
# MAGIC 
# MAGIC     so.CusNo,
# MAGIC     so.CusName,
# MAGIC     so.CusAbbr,
# MAGIC     so.so_abbr,
# MAGIC     so.so_type,
# MAGIC     so.TypeofFG,
# MAGIC     so.Total_QTY,
# MAGIC     so.OutstandingQty,
# MAGIC     so.item_quantity_to_ship AS QtytoShip,
# MAGIC     so.item_quantity_shipped AS QtyShipped,
# MAGIC 
# MAGIC     -- watermark (best-effort greatest)
# MAGIC     greatest(
# MAGIC       CAST(po.modified_on AS TIMESTAMP),
# MAGIC       CAST(pl.modified_on AS TIMESTAMP),
# MAGIC       CAST(so._modified_any AS TIMESTAMP)
# MAGIC     ) AS _modified_any
# MAGIC 
# MAGIC   FROM po_src po
# MAGIC   LEFT JOIN it_src it
# MAGIC     ON po.FG_item_no = it.item_no
# MAGIC   LEFT JOIN so_src so
# MAGIC     ON po.sales_order_no = so.SalesorderNo
# MAGIC    AND po.sales_order_line_no = so.SalesLineNo
# MAGIC   LEFT JOIN pl_src pl
# MAGIC     ON po.prod_order_no = pl.prod_order_no
# MAGIC    AND po.FG_item_no = pl.item_no
# MAGIC   LEFT JOIN pod_src pod
# MAGIC     ON po.prod_order_no = pod.prod_order_no
# MAGIC 
# MAGIC   WHERE
# MAGIC     it.item_category_code = 'FG'
# MAGIC     AND (
# MAGIC       po.sales_order_no LIKE 'RO%'
# MAGIC       OR po.sales_order_no LIKE 'RE%'
# MAGIC       OR po.sales_order_no LIKE 'SL%'
# MAGIC       OR po.sales_order_no LIKE 'SP%'
# MAGIC     )
# MAGIC     AND NOT EXISTS (
# MAGIC       SELECT 1
# MAGIC       FROM gs_src s
# MAGIC       WHERE s.prod_order_no = po.prod_order_no
# MAGIC         AND s.type_name = 'In location in'
# MAGIC         AND s.CorrectCurrentLocation = 'PACKING ROOM'
# MAGIC         AND s.open = 'Yes'
# MAGIC     )
# MAGIC ),
# MAGIC 
# MAGIC -- ------------------ Deduplicate to merge grain ------------------
# MAGIC dedup AS (
# MAGIC   SELECT *
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       b.*,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         PARTITION BY b.prod_order_no, b.FG_item_no, b.sales_order_line_no
# MAGIC         ORDER BY
# MAGIC           b._modified_any DESC NULLS LAST,
# MAGIC           b.prod_order_finished_date DESC NULLS LAST,
# MAGIC           b.prod_order_ending_date_time DESC NULLS LAST,
# MAGIC           sha2(
# MAGIC             concat_ws('§',
# MAGIC               coalesce(CAST(b.sales_order_no AS STRING), ''),
# MAGIC               coalesce(CAST(b.FG_item_no AS STRING), '')
# MAGIC             ),
# MAGIC             256
# MAGIC           ) DESC
# MAGIC       ) AS _rn
# MAGIC     FROM base b
# MAGIC   ) x
# MAGIC   WHERE x._rn = 1
# MAGIC ),
# MAGIC 
# MAGIC -- ------------------ Add system columns + row hash ------------------
# MAGIC final AS (
# MAGIC   SELECT
# MAGIC     prod_order_due_date,
# MAGIC     prod_order_status,
# MAGIC     prod_order_no,
# MAGIC     prod_order_description,
# MAGIC     FG_item_no,
# MAGIC     item_routing_no,
# MAGIC     prod_order_quantity,
# MAGIC     prod_order_type,
# MAGIC     sales_order_no,
# MAGIC     sales_order_line_no,
# MAGIC     prod_order_location,
# MAGIC     prod_order_starting_date_time,
# MAGIC     prod_order_ending_date_time,
# MAGIC     prod_order_finished_date,
# MAGIC 
# MAGIC     prod_line_quantity,
# MAGIC     prod_line_finished_quantity,
# MAGIC     prod_line_remaining_quantity,
# MAGIC 
# MAGIC     CusNo,
# MAGIC     CusName,
# MAGIC     CusAbbr,
# MAGIC     so_abbr,
# MAGIC     so_type,
# MAGIC     TypeofFG,
# MAGIC     Total_QTY,
# MAGIC     OutstandingQty,
# MAGIC     QtytoShip,
# MAGIC     QtyShipped,
# MAGIC 
# MAGIC     _modified_any,
# MAGIC     _modified_any AS updated_at,
# MAGIC     current_timestamp() AS load_ts,
# MAGIC     'gold_builder' AS source_system,
# MAGIC 
# MAGIC     -- row_hash across all content cols (incl _modified_any)
# MAGIC     sha2(
# MAGIC       concat_ws('§',
# MAGIC         coalesce(CAST(prod_order_due_date AS STRING), ''),
# MAGIC         coalesce(CAST(prod_order_status AS STRING), ''),
# MAGIC         coalesce(CAST(prod_order_no AS STRING), ''),
# MAGIC         coalesce(CAST(prod_order_description AS STRING), ''),
# MAGIC         coalesce(CAST(FG_item_no AS STRING), ''),
# MAGIC         coalesce(CAST(item_routing_no AS STRING), ''),
# MAGIC         coalesce(CAST(prod_order_quantity AS STRING), ''),
# MAGIC         coalesce(CAST(prod_order_type AS STRING), ''),
# MAGIC         coalesce(CAST(sales_order_no AS STRING), ''),
# MAGIC         coalesce(CAST(sales_order_line_no AS STRING), ''),
# MAGIC         coalesce(CAST(prod_order_location AS STRING), ''),
# MAGIC         coalesce(CAST(prod_order_starting_date_time AS STRING), ''),
# MAGIC         coalesce(CAST(prod_order_ending_date_time AS STRING), ''),
# MAGIC         coalesce(CAST(prod_order_finished_date AS STRING), ''),
# MAGIC         coalesce(CAST(prod_line_quantity AS STRING), ''),
# MAGIC         coalesce(CAST(prod_line_finished_quantity AS STRING), ''),
# MAGIC         coalesce(CAST(prod_line_remaining_quantity AS STRING), ''),
# MAGIC         coalesce(CAST(CusNo AS STRING), ''),
# MAGIC         coalesce(CAST(CusName AS STRING), ''),
# MAGIC         coalesce(CAST(CusAbbr AS STRING), ''),
# MAGIC         coalesce(CAST(so_abbr AS STRING), ''),
# MAGIC         coalesce(CAST(so_type AS STRING), ''),
# MAGIC         coalesce(CAST(TypeofFG AS STRING), ''),
# MAGIC         coalesce(CAST(Total_QTY AS STRING), ''),
# MAGIC         coalesce(CAST(OutstandingQty AS STRING), ''),
# MAGIC         coalesce(CAST(QtytoShip AS STRING), ''),
# MAGIC         coalesce(CAST(QtyShipped AS STRING), ''),
# MAGIC         coalesce(CAST(_modified_any AS STRING), '')
# MAGIC       ),
# MAGIC       256
# MAGIC     ) AS row_hash
# MAGIC   FROM dedup
# MAGIC )
# MAGIC 
# MAGIC SELECT * FROM final;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # production_rfid_validation_errors

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_rfid_validation_errors
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'false',
# MAGIC     'delta.autoOptimize.autoCompact'   = 'true'
# MAGIC )
# MAGIC AS
# MAGIC SELECT 
# MAGIC     CAST(created_on AS DATE) AS error_date,
# MAGIC     cell_no,
# MAGIC     antenna_id,
# MAGIC     error_type,
# MAGIC     COUNT(*) AS error_count,
# MAGIC     MAX(error_message) AS error_message,
# MAGIC     SUM(COUNT(*)) OVER (
# MAGIC         PARTITION BY cell_no, antenna_id, CAST(created_on AS DATE)
# MAGIC     ) AS total_errors_day,
# MAGIC     COUNT(*) OVER (
# MAGIC         PARTITION BY cell_no, antenna_id, CAST(created_on AS DATE)
# MAGIC     ) AS distinct_error_types_day,
# MAGIC     ROUND(
# MAGIC         COUNT(*) / SUM(COUNT(*)) OVER (
# MAGIC             PARTITION BY cell_no, antenna_id, CAST(created_on AS DATE)
# MAGIC         ), 2
# MAGIC     ) AS pct_of_day,
# MAGIC     CURRENT_TIMESTAMP() AS gold_loaded_at
# MAGIC FROM Silver_Production_Lakehouse.prod.silver_rfid_validation_errors_production
# MAGIC WHERE antenna_id = 1
# MAGIC GROUP BY 
# MAGIC     cell_no,
# MAGIC     antenna_id,
# MAGIC     error_type,
# MAGIC     CAST(created_on AS DATE)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # prod_order_duplicate_in_location

# CELL ********************

# MAGIC %%sql
# MAGIC -- =========================================================================
# MAGIC -- Notebook : nb_gold_production_prod_order_duplicate_in_location
# MAGIC -- Purpose  : Identify Released PROs with more than one OPEN "In location in"
# MAGIC --            transaction per line — data quality issue indicating RFID
# MAGIC --            scan-out wasn't recorded properly
# MAGIC -- Layer    : Silver → Gold
# MAGIC -- Sources  : Silver_BC_Lakehouse.bc.`Production Order`
# MAGIC --            Silver_Production_Lakehouse.prod.silver_prod_order_status
# MAGIC -- Target   : Gold_Production_Lakehouse.prod.gold_prod_order_duplicate_in_location
# MAGIC -- Refresh  : Full refresh (CREATE OR REPLACE)
# MAGIC -- =========================================================================
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_prod_order_duplicate_in_location
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'false',
# MAGIC     'delta.autoOptimize.autoCompact'   = 'true'
# MAGIC )
# MAGIC AS
# MAGIC WITH OpenInLocationMoreThanOne AS (
# MAGIC     SELECT 
# MAGIC         s.prod_order_no,
# MAGIC         s.prod_order_line_no
# MAGIC     FROM Silver_Production_Lakehouse.prod.silver_prod_order_status AS s
# MAGIC     WHERE s.open = 'Yes'
# MAGIC       AND s.type_name = 'In location in'
# MAGIC     GROUP BY 
# MAGIC         s.prod_order_no,
# MAGIC         s.prod_order_line_no
# MAGIC     HAVING COUNT(*) > 1
# MAGIC )
# MAGIC SELECT 
# MAGIC     s.created_on,
# MAGIC     s.modified_on,
# MAGIC     s.prod_order_status,
# MAGIC     s.prod_order_no,
# MAGIC     s.prod_order_line_no,
# MAGIC     s.sales_order_no,
# MAGIC     s.item_no,
# MAGIC     s.quantity,
# MAGIC     s.remaining_quantity,
# MAGIC     s.open,
# MAGIC     s.type_name,
# MAGIC     s.operation_no,
# MAGIC     s.current_location_code,
# MAGIC     s.machine_center_no,
# MAGIC     s.past_location_code,
# MAGIC     s.antenna_id,
# MAGIC     s.employee_no,
# MAGIC     s.rfid_transaction_name,
# MAGIC     s.user_id,
# MAGIC     s.rfid_code,
# MAGIC     COUNT(*) OVER (
# MAGIC         PARTITION BY s.prod_order_no, s.prod_order_line_no
# MAGIC     ) AS open_in_location_transaction_count,
# MAGIC     CURRENT_TIMESTAMP() AS gold_loaded_at
# MAGIC FROM Silver_BC_Lakehouse.bc.`Production Order` AS po
# MAGIC JOIN Silver_Production_Lakehouse.prod.silver_prod_order_status AS s
# MAGIC     ON po.`No.` = s.prod_order_no
# MAGIC JOIN OpenInLocationMoreThanOne AS o
# MAGIC     ON s.prod_order_no = o.prod_order_no
# MAGIC    AND s.prod_order_line_no = o.prod_order_line_no
# MAGIC WHERE po.Status = 'Released'
# MAGIC   AND s.open = 'Yes'
# MAGIC   AND s.type_name = 'In location in'

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
