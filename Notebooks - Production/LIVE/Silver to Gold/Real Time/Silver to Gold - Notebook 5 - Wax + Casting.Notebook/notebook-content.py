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

# # Actual Time by Employee

# MARKDOWN ********************

# ## All Silver - need to fix

# CELL ********************

# MAGIC %%sql
# MAGIC -- ==========================================================
# MAGIC -- Job: Gold_Production_Lakehouse.prod.gold_prod_actual_time_by_employees
# MAGIC -- Strictly mirrors SQL view dbo.v_ProdOrderRunTimes_actual
# MAGIC -- FULL REPLACE Spark SQL
# MAGIC -- ==========================================================
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_prod_actual_time_by_employees
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC WITH r_raw AS (
# MAGIC     SELECT
# MAGIC         `Prod. Order No.`       AS prod_order_no,
# MAGIC         `Routing Reference No.` AS prod_order_line_no,
# MAGIC         `No.`                   AS routing_no,
# MAGIC         `Run Time`              AS run_time,
# MAGIC         `SystemModifiedAt`      AS modified_on
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Prod Order Routing Line`
# MAGIC ),
# MAGIC 
# MAGIC r_dedup AS (
# MAGIC     SELECT
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no,
# MAGIC         routing_no AS machine_center_no,
# MAGIC         MAX(run_time) AS run_time
# MAGIC     FROM r_raw
# MAGIC     GROUP BY
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no,
# MAGIC         routing_no
# MAGIC ),
# MAGIC 
# MAGIC s_from_all AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         TO_TIMESTAMP(created_on)  AS created_on_ts,
# MAGIC         TO_TIMESTAMP(modified_on) AS modified_on_ts
# MAGIC     FROM Silver_Production_Lakehouse.prod.silver_prod_order_status
# MAGIC     WHERE type_name = 'From employee'
# MAGIC       AND user_id <> 'outsource@ennovie.com'
# MAGIC ),
# MAGIC 
# MAGIC s_from AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             s.*,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY
# MAGIC                     prod_order_no,
# MAGIC                     prod_order_line_no,
# MAGIC                     machine_center_no,
# MAGIC                     operation_no,
# MAGIC                     item_no,
# MAGIC                     user_id
# MAGIC                 ORDER BY
# MAGIC                     modified_on_ts DESC NULLS LAST,
# MAGIC                     created_on_ts DESC NULLS LAST
# MAGIC             ) AS rn
# MAGIC         FROM s_from_all s
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC to_filter AS (
# MAGIC     SELECT
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no,
# MAGIC         machine_center_no,
# MAGIC         TO_TIMESTAMP(created_on)  AS to_created_on,
# MAGIC         TO_TIMESTAMP(modified_on) AS to_modified_on
# MAGIC     FROM Silver_Production_Lakehouse.prod.silver_prod_order_status
# MAGIC     WHERE type_name = 'To employee'
# MAGIC ),
# MAGIC 
# MAGIC to_employee_intervals AS (
# MAGIC     SELECT
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no,
# MAGIC         machine_center_no,
# MAGIC         CASE
# MAGIC             WHEN to_modified_on IS NULL OR to_created_on IS NULL THEN 0.0
# MAGIC             WHEN to_modified_on < to_created_on THEN 0.0
# MAGIC             ELSE GREATEST(
# MAGIC                 (CAST(to_modified_on AS BIGINT) - CAST(to_created_on AS BIGINT)) / 60.0,
# MAGIC                 0.0
# MAGIC             )
# MAGIC         END AS actual_minutes
# MAGIC     FROM to_filter
# MAGIC ),
# MAGIC 
# MAGIC to_employee_agg AS (
# MAGIC     SELECT
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no,
# MAGIC         machine_center_no,
# MAGIC         SUM(actual_minutes) AS actual_run_time_min
# MAGIC     FROM to_employee_intervals
# MAGIC     GROUP BY
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no,
# MAGIC         machine_center_no
# MAGIC ),
# MAGIC 
# MAGIC final_base AS (
# MAGIC     SELECT
# MAGIC         s.created_on_ts AS created_on,
# MAGIC         s.modified_on_ts AS modified_on,
# MAGIC         s.prod_order_no,
# MAGIC         s.prod_order_line_no,
# MAGIC         s.machine_center_no,
# MAGIC         s.operation_no,
# MAGIC         s.item_no,
# MAGIC         s.user_id,
# MAGIC         s.quantity,
# MAGIC         s.remaining_quantity,
# MAGIC         s.sales_order_no,
# MAGIC         s.current_location_code,
# MAGIC         s.past_location_code,
# MAGIC         s.employee_no,
# MAGIC         l.cell_line,
# MAGIC         l.prod_line,
# MAGIC         l.sub_department,
# MAGIC         r.run_time AS plan_run_time,
# MAGIC         ABS(CAST(s.quantity AS DOUBLE)) * COALESCE(CAST(r.run_time AS DOUBLE), 0.0) AS total_plan_runtime,
# MAGIC         COALESCE(t.actual_run_time_min, 0.0) AS actual_run_time_min,
# MAGIC         GREATEST(s.modified_on_ts, s.created_on_ts) AS change_ts
# MAGIC     FROM s_from s
# MAGIC 
# MAGIC     LEFT JOIN Silver_Production_Lakehouse.prod.silver_cell_list l
# MAGIC         ON s.user_id = l.email_address
# MAGIC 
# MAGIC     LEFT JOIN r_dedup r
# MAGIC         ON s.prod_order_no = r.prod_order_no
# MAGIC        AND s.prod_order_line_no = r.prod_order_line_no
# MAGIC        AND s.machine_center_no = r.machine_center_no
# MAGIC 
# MAGIC     LEFT JOIN to_employee_agg t
# MAGIC         ON s.prod_order_no = t.prod_order_no
# MAGIC        AND s.prod_order_line_no = t.prod_order_line_no
# MAGIC        AND s.machine_center_no = t.machine_center_no
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     *,
# MAGIC     SHA2(
# MAGIC         CONCAT_WS(
# MAGIC             '||',
# MAGIC             COALESCE(CAST(prod_order_no AS STRING), ''),
# MAGIC             COALESCE(CAST(prod_order_line_no AS STRING), ''),
# MAGIC             COALESCE(CAST(machine_center_no AS STRING), ''),
# MAGIC             COALESCE(CAST(operation_no AS STRING), ''),
# MAGIC             COALESCE(CAST(item_no AS STRING), ''),
# MAGIC             COALESCE(CAST(user_id AS STRING), ''),
# MAGIC             COALESCE(CAST(created_on AS STRING), ''),
# MAGIC             COALESCE(CAST(modified_on AS STRING), '')
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

# # Prod Component Ready

# MARKDOWN ********************

# ## All Silver - need to fix

# CELL ********************

# ============================================================
# FULL REFRESH (recommended): Production Order OverallStatus
# Reason: multi-level aggregates (LineStatus -> OrderAgg) across BC tables.
#
# Target table name (edit if you want):
#   Gold_Production_Lakehouse.prod.gold_prod_component_ready
# ============================================================

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT_TBL = "Gold_Production_Lakehouse.prod.gold_prod_component_ready"

src_sql = """
WITH LineStatus AS (
    SELECT
        pl.`Prod. Order No.` AS prodOrderNo,
        pl.`Line No.`        AS prodOrderLineNo,
        CASE
            -- Main line finished => Finished
            WHEN pl.`Line No.` = 10000
             AND COALESCE(pl.`Finished Quantity`, 0) = COALESCE(pl.`Quantity`, 0)
                THEN 'Finished'
            -- Any line finished by quantity
            WHEN COALESCE(pl.`Finished Quantity`, 0) = COALESCE(pl.`Quantity`, 0)
                THEN 'Finished'
            -- No consumption at all
            WHEN SUM(COALESCE(pc.`Qty. Picked`, 0)) = 0
                THEN 'Not Consumed'
            -- Any component still with meaningful remaining qty -> partial
            WHEN SUM(CASE WHEN COALESCE(pc.`Remaining Quantity`, 0) > 0.1 THEN 1 ELSE 0 END) > 0
                THEN 'Partial Consumed'
            -- Otherwise fully consumed (within tolerance) but not finished by quantity
            ELSE 'Consumed'
        END AS LineStatus
    FROM Silver_BC_Lakehouse.bc.`Prod Order Line` pl
    LEFT JOIN Silver_BC_Lakehouse.bc.`Prod Order Component` pc
        ON  pl.`Prod. Order No.` = pc.`Prod. Order No.`
        AND pl.`Line No.`        = pc.`Prod. Order Line No.`
    WHERE
        pc.`Item No.` NOT LIKE 'M-%'
        AND pc.`Item No.` NOT LIKE 'PL-%'
        AND pc.`Item No.` NOT LIKE 'RM-%'
    GROUP BY
        pl.`Prod. Order No.`,
        pl.`Line No.`,
        pl.`Item No.`,
        pl.`Finished Quantity`,
        pl.`Quantity`
),
OrderAgg AS (
    SELECT
        po.`No.` AS prodOrderNo,
        SUM(CASE WHEN ls.prodOrderLineNo = 10000 AND ls.LineStatus = 'Finished' THEN 1 ELSE 0 END) AS hasMainFinished,
        SUM(CASE WHEN ls.LineStatus = 'Partial Consumed' THEN 1 ELSE 0 END) AS anyPartial,
        SUM(CASE WHEN ls.LineStatus = 'Not Consumed' THEN 1 ELSE 0 END) AS notConsumedLines,
        COUNT(ls.prodOrderNo) AS lineCount
    FROM Silver_BC_Lakehouse.bc.`Production Order` po
    LEFT JOIN LineStatus ls
        ON po.`No.` = ls.prodOrderNo
    GROUP BY po.`No.`
)
SELECT
    po.`Status`                                      AS `Status`,
    po.`No.`                                         AS prodOrderNo,
    po.`Due Date`                                    AS `Due_Date`,
    po.`Sales Order No.`                             AS `Sales_Order_No`,
    CONCAT(substr(po.`Sales Order No.`, 1, 2), substr(po.`Sales Order No.`, length(po.`Sales Order No.`) - 3, 4)) AS new_so,
    substr(po.`Sales Order No.`, 1, 2)               AS typeSO,
    po.`Source No.`                                  AS `Source_No`,
    CAST(po.`Starting Date-Time` AS DATE)            AS Starting_Date,
    CAST(po.`Ending Date-Time` AS DATE)              AS Ending_Date,
    CASE
        WHEN oa.lineCount = 0                   THEN 'Finished'          -- no lines => follow order
        WHEN oa.hasMainFinished > 0             THEN 'Finished'
        WHEN oa.anyPartial > 0                  THEN 'Partial Consumed'
        WHEN oa.notConsumedLines = oa.lineCount THEN 'Not Consumed'
        ELSE 'Finished'  -- remaining: all lines are Finished/Consumed
    END AS OverallStatus
FROM Silver_BC_Lakehouse.bc.`Production Order` po
LEFT JOIN OrderAgg oa
    ON po.`No.` = oa.prodOrderNo
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

# # Waxing and Casting Status

# MARKDOWN ********************

# ## Silver + Gold Emp Team Wax

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_waxing_and_casting_status
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH src AS (
# MAGIC     SELECT
# MAGIC         created_on,
# MAGIC         modified_on,
# MAGIC         prod_order_no,
# MAGIC         prod_order_line_no,
# MAGIC         type_name,
# MAGIC         prod_order_status,
# MAGIC         operation_no,
# MAGIC         `open`,
# MAGIC         sales_order_no,
# MAGIC         current_location_code,
# MAGIC         past_location_code,
# MAGIC         employee_no,
# MAGIC         antenna_id,
# MAGIC         rfid_transaction_name,
# MAGIC         user_id,
# MAGIC         quantity,
# MAGIC         remaining_quantity,
# MAGIC         item_no,
# MAGIC         machine_center_no
# MAGIC     FROM `Silver_Production_Lakehouse`.`prod`.`silver_prod_order_status`
# MAGIC ),
# MAGIC data_calc AS (
# MAGIC     SELECT
# MAGIC         s.created_on,
# MAGIC         s.modified_on,
# MAGIC         s.prod_order_no,
# MAGIC         CAST(s.prod_order_line_no AS STRING) AS prod_order_line_no,
# MAGIC         s.type_name,
# MAGIC         s.prod_order_status,
# MAGIC         s.operation_no,
# MAGIC         s.`open`,
# MAGIC         s.sales_order_no,
# MAGIC         s.current_location_code,
# MAGIC         s.past_location_code,
# MAGIC         s.employee_no,
# MAGIC         s.antenna_id,
# MAGIC         s.rfid_transaction_name,
# MAGIC         s.user_id,
# MAGIC         s.quantity,
# MAGIC         s.remaining_quantity,
# MAGIC         s.item_no,
# MAGIC         s.machine_center_no,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN s.current_location_code IS NULL OR length(trim(s.current_location_code)) = 0
# MAGIC                 THEN NULL
# MAGIC             ELSE trim(s.current_location_code)
# MAGIC         END AS cur_loc,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN s.machine_center_no IS NULL OR length(trim(s.machine_center_no)) = 0
# MAGIC                 THEN NULL
# MAGIC             ELSE trim(s.machine_center_no)
# MAGIC         END AS mach_no
# MAGIC     FROM src s
# MAGIC ),
# MAGIC data_with_calc AS (
# MAGIC     SELECT
# MAGIC         d.created_on,
# MAGIC         d.modified_on,
# MAGIC         d.prod_order_no,
# MAGIC         d.prod_order_line_no,
# MAGIC         d.type_name,
# MAGIC         d.prod_order_status,
# MAGIC         d.operation_no,
# MAGIC         d.`open`,
# MAGIC         d.sales_order_no,
# MAGIC         d.current_location_code,
# MAGIC         d.past_location_code,
# MAGIC         d.employee_no,
# MAGIC         d.antenna_id,
# MAGIC         d.rfid_transaction_name,
# MAGIC         d.user_id,
# MAGIC         d.quantity,
# MAGIC         d.remaining_quantity,
# MAGIC         d.item_no,
# MAGIC         d.machine_center_no,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN d.cur_loc IS NULL OR upper(substr(coalesce(d.cur_loc, ''), 1, 4)) = 'CELL'
# MAGIC                 THEN coalesce(d.mach_no, d.cur_loc)
# MAGIC             ELSE d.cur_loc
# MAGIC         END AS CorrectCurrentLocation,
# MAGIC 
# MAGIC         CAST(-1 * d.quantity AS BIGINT) AS out_qty,
# MAGIC 
# MAGIC         concat(d.prod_order_no, d.prod_order_line_no) AS pol,
# MAGIC 
# MAGIC         d.created_on AS created_on_time
# MAGIC     FROM data_calc d
# MAGIC ),
# MAGIC dedup_pre AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             dc.*,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY
# MAGIC                     dc.prod_order_no,
# MAGIC                     dc.item_no,
# MAGIC                     dc.CorrectCurrentLocation,
# MAGIC                     dc.type_name
# MAGIC                 ORDER BY
# MAGIC                     CAST(dc.modified_on AS TIMESTAMP) DESC,
# MAGIC                     CAST(dc.created_on AS TIMESTAMP) DESC
# MAGIC             ) AS rn
# MAGIC         FROM data_with_calc dc
# MAGIC     ) x
# MAGIC     WHERE x.rn = 1
# MAGIC ),
# MAGIC joined AS (
# MAGIC     SELECT
# MAGIC         p.created_on,
# MAGIC         p.modified_on,
# MAGIC         p.prod_order_no,
# MAGIC         p.prod_order_line_no,
# MAGIC         p.type_name,
# MAGIC         p.operation_no,
# MAGIC         p.prod_order_status,
# MAGIC         p.`open`,
# MAGIC         p.sales_order_no,
# MAGIC         p.current_location_code,
# MAGIC         p.past_location_code,
# MAGIC         p.employee_no,
# MAGIC         p.antenna_id,
# MAGIC         p.rfid_transaction_name,
# MAGIC         p.user_id,
# MAGIC         p.quantity,
# MAGIC         p.remaining_quantity,
# MAGIC         p.item_no,
# MAGIC         p.machine_center_no,
# MAGIC         p.out_qty,
# MAGIC         p.pol,
# MAGIC         p.created_on_time,
# MAGIC         coalesce(w.team, '0') AS team,
# MAGIC         p.CorrectCurrentLocation
# MAGIC     FROM dedup_pre p
# MAGIC     LEFT JOIN `Gold_Production_Lakehouse`.`prod`.`gold_emp_team_wax` w
# MAGIC         ON p.employee_no = w.Employee_Code
# MAGIC ),
# MAGIC filtered AS (
# MAGIC     SELECT *
# MAGIC     FROM joined
# MAGIC     WHERE item_no LIKE 'C0%'
# MAGIC       AND prod_order_status = 'Released'
# MAGIC )
# MAGIC SELECT
# MAGIC     f.created_on,
# MAGIC     f.modified_on,
# MAGIC     f.prod_order_no,
# MAGIC     f.prod_order_line_no,
# MAGIC     f.type_name,
# MAGIC     f.operation_no,
# MAGIC     f.prod_order_status,
# MAGIC     f.`open`,
# MAGIC     f.sales_order_no,
# MAGIC     f.current_location_code,
# MAGIC     f.past_location_code,
# MAGIC     f.employee_no,
# MAGIC     f.antenna_id,
# MAGIC     f.rfid_transaction_name,
# MAGIC     f.user_id,
# MAGIC     f.quantity,
# MAGIC     f.remaining_quantity,
# MAGIC     f.item_no,
# MAGIC     f.machine_center_no,
# MAGIC     f.out_qty,
# MAGIC     f.pol,
# MAGIC     f.created_on_time,
# MAGIC     f.team,
# MAGIC     f.CorrectCurrentLocation,
# MAGIC 
# MAGIC     coalesce(
# MAGIC         CAST(f.modified_on AS TIMESTAMP),
# MAGIC         CAST(f.created_on AS TIMESTAMP),
# MAGIC         current_timestamp()
# MAGIC     ) AS _modified_any
# MAGIC FROM filtered f;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Wax Output

# MARKDOWN ********************

# ## All Gold

# CELL ********************

# MAGIC %%sql
# MAGIC -- ==============================================================
# MAGIC -- gold_wax_output — Full Replace Table (Spark SQL)
# MAGIC -- Added: parts_per_mold + mold_shots for injection/filing/setting
# MAGIC -- ==============================================================
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_wax_output
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC WITH base AS (
# MAGIC     SELECT
# MAGIC         e.created_on,
# MAGIC         TO_DATE(e.created_on) AS work_date,
# MAGIC 
# MAGIC         -- UTC+7 derivatives
# MAGIC         e.created_on + INTERVAL 7 HOURS AS created_on_plus7_dt,
# MAGIC         TO_DATE(e.created_on + INTERVAL 7 HOURS) AS work_date_plus7,
# MAGIC         DATE_FORMAT(e.created_on + INTERVAL 7 HOURS, 'HH:mm:ss') AS created_time_plus7,
# MAGIC 
# MAGIC         e.team,
# MAGIC         e.prod_order_no,
# MAGIC         e.prod_order_line_no,
# MAGIC         e.item_no,
# MAGIC 
# MAGIC         CAST(REGEXP_REPLACE(TRIM(CAST(e.operation_no AS STRING)), '\\.', '') AS INT) AS op_seq_clean,
# MAGIC 
# MAGIC         ABS(e.quantity) AS qty,
# MAGIC 
# MAGIC         s.CusAbbr,
# MAGIC         s.so_abbr,
# MAGIC         e.machine_center_no,
# MAGIC 
# MAGIC         UPPER(
# MAGIC             REGEXP_REPLACE(
# MAGIC                 TRIM(REGEXP_REPLACE(e.machine_center_no, '-', ' ')),
# MAGIC                 '\\s+',
# MAGIC                 ' '
# MAGIC             )
# MAGIC         ) AS mc_norm,
# MAGIC 
# MAGIC         LOWER(TRIM(e.type_name)) AS type_name_norm,
# MAGIC 
# MAGIC         e.user_id AS user_id,
# MAGIC         LOWER(TRIM(e.user_id)) AS user_id_norm
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_waxing_and_casting_status e
# MAGIC     LEFT JOIN Gold_Production_Lakehouse.prod.gold_sales_order s
# MAGIC         ON e.sales_order_no = s.SalesorderNo
# MAGIC     WHERE LOWER(TRIM(e.type_name)) IN ('from employee', 'from employee accept', 'employee')
# MAGIC ),
# MAGIC 
# MAGIC ranked AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CASE
# MAGIC             WHEN mc_norm LIKE '%WAX%INJ%' THEN 1001
# MAGIC             WHEN mc_norm LIKE '%WAX%FIL%' THEN 1002
# MAGIC             WHEN mc_norm LIKE '%WAX%SET%' THEN 1003
# MAGIC             ELSE NULL
# MAGIC         END AS op_seq_inferred,
# MAGIC 
# MAGIC         CASE WHEN mc_norm LIKE '%WAX%INJ%' THEN qty ELSE 0 END AS qty_wax_injection,
# MAGIC         CASE WHEN mc_norm LIKE '%WAX%FIL%' THEN qty ELSE 0 END AS qty_wax_filing,
# MAGIC         CASE WHEN mc_norm LIKE '%WAX%SET%' THEN qty ELSE 0 END AS qty_wax_setting
# MAGIC     FROM base
# MAGIC ),
# MAGIC 
# MAGIC scored AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         COALESCE(op_seq_clean, op_seq_inferred, -1) AS op_seq_final,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN COALESCE(op_seq_clean, op_seq_inferred) = 1001 AND mc_norm LIKE '%WAX%INJ%' THEN 0
# MAGIC             WHEN COALESCE(op_seq_clean, op_seq_inferred) = 1002 AND mc_norm LIKE '%WAX%FIL%' THEN 0
# MAGIC             WHEN COALESCE(op_seq_clean, op_seq_inferred) = 1003 AND mc_norm LIKE '%WAX%SET%' THEN 0
# MAGIC             ELSE 1
# MAGIC         END AS priority,
# MAGIC 
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY prod_order_no, prod_order_line_no, COALESCE(op_seq_clean, op_seq_inferred, -1)
# MAGIC             ORDER BY
# MAGIC                 CASE
# MAGIC                     WHEN COALESCE(op_seq_clean, op_seq_inferred) = 1001 AND mc_norm LIKE '%WAX%INJ%' THEN 0
# MAGIC                     WHEN COALESCE(op_seq_clean, op_seq_inferred) = 1002 AND mc_norm LIKE '%WAX%FIL%' THEN 0
# MAGIC                     WHEN COALESCE(op_seq_clean, op_seq_inferred) = 1003 AND mc_norm LIKE '%WAX%SET%' THEN 0
# MAGIC                     ELSE 1
# MAGIC                 END ASC,
# MAGIC                 created_on ASC NULLS LAST
# MAGIC         ) AS rn
# MAGIC     FROM ranked
# MAGIC ),
# MAGIC 
# MAGIC -- NEW: parts_per_mold lookup
# MAGIC mold_parts AS (
# MAGIC     SELECT
# MAGIC         part_item,
# MAGIC         MAX(parts_per_mold) AS parts_per_mold
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_mold_item_parts
# MAGIC     WHERE parts_per_mold IS NOT NULL
# MAGIC     GROUP BY part_item
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     DATE_FORMAT(s.created_on_plus7_dt, 'HH:mm:ss') AS created_on_plus7,
# MAGIC     s.work_date_plus7,
# MAGIC     s.created_time_plus7,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN s.user_id_norm = 'outsource@ennovie.com' THEN 'O'
# MAGIC         ELSE CAST(s.team AS STRING)
# MAGIC     END AS team,
# MAGIC 
# MAGIC     s.prod_order_no,
# MAGIC     s.prod_order_line_no,
# MAGIC     s.item_no,
# MAGIC     s.op_seq_final AS op_seq,
# MAGIC     s.machine_center_no,
# MAGIC     s.CusAbbr,
# MAGIC     s.so_abbr,
# MAGIC     s.qty_wax_injection,
# MAGIC     s.qty_wax_filing,
# MAGIC     s.qty_wax_setting,
# MAGIC 
# MAGIC     -- NEW: mold info
# MAGIC     mp.parts_per_mold,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN mp.parts_per_mold IS NOT NULL
# MAGIC             THEN CEIL(s.qty_wax_injection / CAST(mp.parts_per_mold AS DECIMAL(18,6)))
# MAGIC         ELSE s.qty_wax_injection
# MAGIC     END AS mold_shots_injection,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN mp.parts_per_mold IS NOT NULL
# MAGIC             THEN CEIL(s.qty_wax_filing / CAST(mp.parts_per_mold AS DECIMAL(18,6)))
# MAGIC         ELSE s.qty_wax_filing
# MAGIC     END AS mold_shots_filing,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN mp.parts_per_mold IS NOT NULL
# MAGIC             THEN CEIL(s.qty_wax_setting / CAST(mp.parts_per_mold AS DECIMAL(18,6)))
# MAGIC         ELSE s.qty_wax_setting
# MAGIC     END AS mold_shots_setting,
# MAGIC 
# MAGIC     CURRENT_TIMESTAMP() AS _modified_any
# MAGIC FROM scored s
# MAGIC LEFT JOIN mold_parts mp
# MAGIC     ON mp.part_item = s.item_no
# MAGIC WHERE s.rn = 1
# MAGIC ORDER BY op_seq ASC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Casting part Runtime

# CELL ********************

# MAGIC %%sql
# MAGIC -- ==============================================================
# MAGIC -- gold_mold_item_parts — mold master with unpivoted parts (Spark SQL)
# MAGIC -- ==============================================================
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_mold_item_parts
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT  base.item_no_text,
# MAGIC         base.customer_no,
# MAGIC         base.customer_name_text,
# MAGIC         base.modification_level_name,
# MAGIC         base.parts_per_mold,
# MAGIC         base.part_item,
# MAGIC         base.part_role,
# MAGIC         wi.runtime                                                  AS wax_inject_runtime,
# MAGIC         wi.standardtime                                             AS wax_inject_standardtime,
# MAGIC         ROUND(wi.runtime      / NULLIF(base.parts_per_mold, 0), 4) AS wax_inject_runtime_per_piece,
# MAGIC         ROUND(wi.standardtime / NULLIF(base.parts_per_mold, 0), 4) AS wax_inject_standardtime_per_piece,
# MAGIC         wf.runtime                                                  AS wax_filing_runtime,
# MAGIC         wf.standardtime                                             AS wax_filing_standardtime,
# MAGIC         ROUND(wf.runtime      / NULLIF(base.parts_per_mold, 0), 4) AS wax_filing_runtime_per_piece,
# MAGIC         ROUND(wf.standardtime / NULLIF(base.parts_per_mold, 0), 4) AS wax_filing_standardtime_per_piece
# MAGIC FROM (
# MAGIC     SELECT  sm.item_no_text,
# MAGIC             sm.customer_no,
# MAGIC             sm.customer_name_text,
# MAGIC             sm.modification_level_name,
# MAGIC             sm.parts_per_mold,
# MAGIC             v.part_item,
# MAGIC             v.part_role
# MAGIC     FROM Silver_Production_Lakehouse.prod.silver_mold_master sm
# MAGIC     LATERAL VIEW INLINE(ARRAY(
# MAGIC         NAMED_STRUCT('part_item', sm.item_no_text,               'part_role', 'Main'),
# MAGIC         NAMED_STRUCT('part_item', sm.secondary_casted_part_name, 'part_role', 'Secondary'),
# MAGIC         NAMED_STRUCT('part_item', sm.third_casted_part_name,     'part_role', 'Third')
# MAGIC     )) v AS part_item, part_role
# MAGIC     WHERE v.part_item IS NOT NULL
# MAGIC       AND (sm.mold_blocked = false OR sm.mold_blocked IS NULL)
# MAGIC ) base
# MAGIC LEFT JOIN Silver_Production_Lakehouse.prod.silver_mold_modification wi
# MAGIC     ON wi.wm_modification_name = base.modification_level_name
# MAGIC    AND wi.machine_center = 'WAX INJECT'
# MAGIC LEFT JOIN Silver_Production_Lakehouse.prod.silver_mold_modification wf
# MAGIC     ON wf.wm_modification_name = base.modification_level_name
# MAGIC    AND wf.machine_center = 'WAX FILINIG'
# MAGIC GROUP BY base.item_no_text,
# MAGIC          base.customer_no,
# MAGIC          base.customer_name_text,
# MAGIC          base.modification_level_name,
# MAGIC          base.parts_per_mold,
# MAGIC          base.part_item,
# MAGIC          base.part_role,
# MAGIC          wi.runtime,
# MAGIC          wi.standardtime,
# MAGIC          wf.runtime,
# MAGIC          wf.standardtime

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Wax Output Efficiency Detail

# CELL ********************

# MAGIC %%sql
# MAGIC -- ==============================================================
# MAGIC -- gold_wax_output_efficiency_Detail — Spark SQL (fixed)
# MAGIC -- ==============================================================
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_wax_output_efficiency_Detail
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC WITH wax_output AS (
# MAGIC     SELECT
# MAGIC         w.work_date_plus7,
# MAGIC         w.team,
# MAGIC         w.prod_order_no,
# MAGIC         w.prod_order_line_no,
# MAGIC         w.machine_center_no,
# MAGIC         e.item_no,
# MAGIC         e.antenna_id,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN w.machine_center_no LIKE '%INJ%'  THEN w.qty_wax_injection
# MAGIC             WHEN w.machine_center_no LIKE '%FIL%'  THEN w.qty_wax_filing
# MAGIC             WHEN w.machine_center_no LIKE '%SET%'  THEN w.qty_wax_setting
# MAGIC             ELSE 0
# MAGIC         END AS qty_output,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN w.team = 'O' THEN 'Outsource'
# MAGIC             ELSE 'In-house'
# MAGIC         END AS team_type
# MAGIC 
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_wax_output w
# MAGIC     LEFT JOIN (
# MAGIC         SELECT DISTINCT
# MAGIC             prod_order_no,
# MAGIC             prod_order_line_no,
# MAGIC             machine_center_no,
# MAGIC             item_no,
# MAGIC             antenna_id
# MAGIC         FROM Gold_Production_Lakehouse.prod.gold_waxing_and_casting_status
# MAGIC         WHERE LOWER(TRIM(type_name)) IN ('from employee', 'from employee accept', 'employee')
# MAGIC     ) e
# MAGIC         ON  w.prod_order_no      = e.prod_order_no
# MAGIC         AND w.prod_order_line_no = e.prod_order_line_no
# MAGIC         AND w.machine_center_no  = e.machine_center_no
# MAGIC ),
# MAGIC 
# MAGIC item_latest AS (
# MAGIC     SELECT
# MAGIC         part_item,
# MAGIC         modification_level_name,
# MAGIC         parts_per_mold,
# MAGIC         wax_inject_runtime,
# MAGIC         wax_inject_standardtime,
# MAGIC         wax_filing_runtime,
# MAGIC         wax_filing_standardtime,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY part_item
# MAGIC             ORDER BY parts_per_mold DESC
# MAGIC         ) AS rn
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_mold_item_parts
# MAGIC ),
# MAGIC 
# MAGIC thai_name AS (
# MAGIC     SELECT
# MAGIC         TRIM(CAST(employee_code AS STRING)) AS employee_code,
# MAGIC         TRIM(CAST(first_name_thai AS STRING)) AS first_name_thai,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY employee_code
# MAGIC             ORDER BY Work_Day DESC
# MAGIC         ) AS rn
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_employee_time_group_rfid
# MAGIC     WHERE first_name_thai IS NOT NULL
# MAGIC ),
# MAGIC 
# MAGIC emp_dedup AS (
# MAGIC     SELECT
# MAGIC         ew.Employee_Code,
# MAGIC         tn.first_name_thai,
# MAGIC         ew.machine_center_no,
# MAGIC         CAST(ew.antenna_id AS STRING) AS antenna_id,
# MAGIC         CAST(ew.team AS STRING) AS team,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY ew.Employee_Code, ew.machine_center_no, ew.antenna_id
# MAGIC             ORDER BY ew.modified_on DESC
# MAGIC         ) AS rn
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_emp_team_wax ew
# MAGIC     LEFT JOIN thai_name tn
# MAGIC         ON tn.employee_code = ew.Employee_Code
# MAGIC        AND tn.rn = 1
# MAGIC ),
# MAGIC 
# MAGIC emp_agg AS (
# MAGIC     SELECT
# MAGIC         team              AS emp_team,
# MAGIC         machine_center_no AS emp_machine_center,
# MAGIC         antenna_id        AS emp_antenna_id,
# MAGIC         COUNT(*)          AS headcount,
# MAGIC         concat_ws(',', sort_array(collect_set(Employee_Code)))     AS employee_codes,
# MAGIC         concat_ws(',', sort_array(collect_set(first_name_thai)))   AS employee_names
# MAGIC     FROM emp_dedup
# MAGIC     WHERE rn = 1
# MAGIC     GROUP BY team, machine_center_no, antenna_id
# MAGIC ),
# MAGIC 
# MAGIC emp_team_map AS (
# MAGIC     SELECT
# MAGIC         Employee_Code,
# MAGIC         CAST(antenna_id AS STRING) AS antenna_id,
# MAGIC         CAST(team AS STRING)       AS team,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY Employee_Code, antenna_id
# MAGIC             ORDER BY modified_on DESC
# MAGIC         ) AS rn
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_emp_team_wax
# MAGIC ),
# MAGIC 
# MAGIC emp_hours AS (
# MAGIC     SELECT
# MAGIC         CAST(r.Work_Day AS DATE)          AS work_date,
# MAGIC         TRIM(CAST(r.antenna_id AS STRING)) AS antenna_id,
# MAGIC         t.team,
# MAGIC         SUM(CAST(r.total_workhour AS DOUBLE)) AS total_work_min
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_employee_time_group_rfid r
# MAGIC     INNER JOIN emp_team_map t
# MAGIC         ON  r.employee_code = t.Employee_Code
# MAGIC         AND TRIM(CAST(r.antenna_id AS STRING)) = t.antenna_id
# MAGIC         AND t.rn = 1
# MAGIC     WHERE r.antenna_id IS NOT NULL
# MAGIC       AND UPPER(TRIM(r.sub_department)) = 'WAX ROOM'
# MAGIC     GROUP BY
# MAGIC         CAST(r.Work_Day AS DATE),
# MAGIC         TRIM(CAST(r.antenna_id AS STRING)),
# MAGIC         t.team
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     o.work_date_plus7,
# MAGIC     o.team,
# MAGIC     o.team_type,
# MAGIC     o.machine_center_no,
# MAGIC     o.antenna_id,
# MAGIC     o.prod_order_no,
# MAGIC     o.prod_order_line_no,
# MAGIC     o.item_no,
# MAGIC     o.qty_output,
# MAGIC 
# MAGIC     il.modification_level_name,
# MAGIC     il.parts_per_mold,
# MAGIC 
# MAGIC     CEIL(CAST(o.qty_output AS DOUBLE) / NULLIF(il.parts_per_mold, 0)) AS cycles,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN o.machine_center_no LIKE '%INJ%' THEN il.wax_inject_runtime
# MAGIC         WHEN o.machine_center_no LIKE '%FIL%' THEN il.wax_filing_runtime
# MAGIC         ELSE NULL
# MAGIC     END AS planned_min_per_cycle,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN o.machine_center_no LIKE '%INJ%' THEN il.wax_inject_standardtime
# MAGIC         WHEN o.machine_center_no LIKE '%FIL%' THEN il.wax_filing_standardtime
# MAGIC         ELSE NULL
# MAGIC     END AS standard_min_per_cycle,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN o.machine_center_no LIKE '%INJ%'
# MAGIC             THEN CEIL(CAST(o.qty_output AS DOUBLE) / NULLIF(il.parts_per_mold, 0)) * il.wax_inject_runtime
# MAGIC         WHEN o.machine_center_no LIKE '%FIL%'
# MAGIC             THEN CEIL(CAST(o.qty_output AS DOUBLE) / NULLIF(il.parts_per_mold, 0)) * il.wax_filing_runtime
# MAGIC         ELSE NULL
# MAGIC     END AS planned_time_min,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN o.machine_center_no LIKE '%INJ%'
# MAGIC             THEN CEIL(CAST(o.qty_output AS DOUBLE) / NULLIF(il.parts_per_mold, 0)) * il.wax_inject_standardtime
# MAGIC         WHEN o.machine_center_no LIKE '%FIL%'
# MAGIC             THEN CEIL(CAST(o.qty_output AS DOUBLE) / NULLIF(il.parts_per_mold, 0)) * il.wax_filing_standardtime
# MAGIC         ELSE NULL
# MAGIC     END AS standard_time_min,
# MAGIC 
# MAGIC     emp.headcount,
# MAGIC     emp.employee_codes,
# MAGIC     emp.employee_names,
# MAGIC 
# MAGIC     eh.total_work_min,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN eh.total_work_min IS NULL               THEN NULL
# MAGIC         WHEN eh.total_work_min = 0                   THEN NULL
# MAGIC         WHEN il.parts_per_mold IS NULL               THEN NULL
# MAGIC         WHEN il.parts_per_mold = 0                   THEN NULL
# MAGIC         WHEN o.machine_center_no LIKE '%INJ%'
# MAGIC             THEN (CEIL(CAST(o.qty_output AS DOUBLE) / NULLIF(il.parts_per_mold, 0)) * il.wax_inject_runtime)
# MAGIC                  / eh.total_work_min * 100.0
# MAGIC         WHEN o.machine_center_no LIKE '%FIL%'
# MAGIC             THEN (CEIL(CAST(o.qty_output AS DOUBLE) / NULLIF(il.parts_per_mold, 0)) * il.wax_filing_runtime)
# MAGIC                  / eh.total_work_min * 100.0
# MAGIC         ELSE NULL
# MAGIC     END AS efficiency_pct,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN il.wax_inject_runtime IS NULL AND il.wax_filing_runtime IS NULL
# MAGIC                                             THEN 'Missing Modification'
# MAGIC         WHEN il.parts_per_mold IS NULL      THEN 'Missing Parts Per Mold'
# MAGIC         WHEN il.parts_per_mold = 0          THEN 'Missing Parts Per Mold'
# MAGIC         WHEN o.team_type = 'Outsource'      THEN 'Outsource'
# MAGIC         WHEN eh.total_work_min IS NULL      THEN 'Missing Workhour'
# MAGIC         WHEN eh.total_work_min = 0          THEN 'Missing Workhour'
# MAGIC         ELSE 'Complete'
# MAGIC     END AS data_quality_flag,
# MAGIC 
# MAGIC     CURRENT_TIMESTAMP() AS _modified_any
# MAGIC 
# MAGIC FROM wax_output o
# MAGIC 
# MAGIC LEFT JOIN item_latest il
# MAGIC     ON il.part_item = o.item_no
# MAGIC    AND il.rn = 1
# MAGIC 
# MAGIC LEFT JOIN emp_agg emp
# MAGIC     ON  emp.emp_team = o.team
# MAGIC     AND emp.emp_machine_center = o.machine_center_no
# MAGIC     AND emp.emp_antenna_id = CAST(o.antenna_id AS STRING)
# MAGIC 
# MAGIC LEFT JOIN emp_hours eh
# MAGIC     ON  eh.work_date = o.work_date_plus7
# MAGIC     AND eh.antenna_id = CAST(o.antenna_id AS STRING)
# MAGIC     AND eh.team = o.team
# MAGIC 
# MAGIC ORDER BY
# MAGIC     o.work_date_plus7 DESC,
# MAGIC     o.team,
# MAGIC     o.machine_center_no,
# MAGIC     o.prod_order_no

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# # Wax Output Efficiency

# CELL ********************

# MAGIC %%sql
# MAGIC -- ==============================================================
# MAGIC -- gold_wax_output_efficiency — day / team / antenna (Spark SQL)
# MAGIC -- ==============================================================
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_wax_output_efficiency
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC WITH
# MAGIC chosen_qty AS (
# MAGIC     SELECT
# MAGIC         w.work_date_plus7,
# MAGIC         w.team,
# MAGIC         w.prod_order_no,
# MAGIC         w.prod_order_line_no,
# MAGIC         w.machine_center_no,
# MAGIC         e.item_no,
# MAGIC         e.antenna_id,
# MAGIC 
# MAGIC         COALESCE(
# MAGIC             NULLIF(MAX(w.qty_wax_injection), 0),
# MAGIC             NULLIF(MAX(w.qty_wax_filing), 0),
# MAGIC             NULLIF(MAX(w.qty_wax_setting), 0),
# MAGIC             0
# MAGIC         ) AS chosen_qty
# MAGIC 
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_wax_output w
# MAGIC     LEFT JOIN (
# MAGIC         SELECT DISTINCT
# MAGIC             prod_order_no,
# MAGIC             prod_order_line_no,
# MAGIC             machine_center_no,
# MAGIC             item_no,
# MAGIC             antenna_id
# MAGIC         FROM Gold_Production_Lakehouse.prod.gold_waxing_and_casting_status
# MAGIC         WHERE LOWER(TRIM(type_name)) IN ('from employee', 'from employee accept', 'employee')
# MAGIC     ) e
# MAGIC         ON  w.prod_order_no      = e.prod_order_no
# MAGIC         AND w.prod_order_line_no = e.prod_order_line_no
# MAGIC         AND w.machine_center_no  = e.machine_center_no
# MAGIC     GROUP BY
# MAGIC         w.work_date_plus7, w.team, w.prod_order_no,
# MAGIC         w.prod_order_line_no, w.machine_center_no,
# MAGIC         e.item_no, e.antenna_id
# MAGIC ),
# MAGIC 
# MAGIC item_latest AS (
# MAGIC     SELECT
# MAGIC         part_item,
# MAGIC         modification_level_name,
# MAGIC         parts_per_mold,
# MAGIC         wax_inject_runtime,
# MAGIC         wax_inject_standardtime,
# MAGIC         wax_filing_runtime,
# MAGIC         wax_filing_standardtime,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY part_item
# MAGIC             ORDER BY parts_per_mold DESC
# MAGIC         ) AS rn
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_mold_item_parts
# MAGIC ),
# MAGIC 
# MAGIC detail AS (
# MAGIC     SELECT
# MAGIC         q.work_date_plus7,
# MAGIC         q.team,
# MAGIC         q.antenna_id,
# MAGIC         q.prod_order_no,
# MAGIC         q.prod_order_line_no,
# MAGIC         q.machine_center_no,
# MAGIC         q.item_no,
# MAGIC         q.chosen_qty,
# MAGIC         il.parts_per_mold,
# MAGIC         il.wax_inject_runtime,
# MAGIC         il.wax_filing_runtime,
# MAGIC 
# MAGIC         CEIL(CAST(q.chosen_qty AS DOUBLE) / NULLIF(il.parts_per_mold, 0)) AS cycles,
# MAGIC 
# MAGIC         CASE
# MAGIC             WHEN q.machine_center_no = 'WAX INJECT'
# MAGIC                 THEN CEIL(CAST(q.chosen_qty AS DOUBLE) / NULLIF(il.parts_per_mold, 0)) * il.wax_inject_runtime
# MAGIC             WHEN q.machine_center_no = 'WAX FILINIG'
# MAGIC                 THEN CEIL(CAST(q.chosen_qty AS DOUBLE) / NULLIF(il.parts_per_mold, 0)) * il.wax_filing_runtime
# MAGIC             ELSE NULL
# MAGIC         END AS planned_time_min
# MAGIC 
# MAGIC     FROM chosen_qty q
# MAGIC     LEFT JOIN item_latest il
# MAGIC         ON il.part_item = q.item_no
# MAGIC        AND il.rn = 1
# MAGIC ),
# MAGIC 
# MAGIC output_agg AS (
# MAGIC     SELECT
# MAGIC         work_date_plus7              AS created_date,
# MAGIC         team,
# MAGIC         antenna_id,
# MAGIC         SUM(cycles)                  AS out_qty,
# MAGIC         SUM(planned_time_min)        AS total_runtime_qty
# MAGIC     FROM detail
# MAGIC     GROUP BY work_date_plus7, team, antenna_id
# MAGIC ),
# MAGIC 
# MAGIC emp_team_map AS (
# MAGIC     SELECT
# MAGIC         Employee_Code,
# MAGIC         CAST(antenna_id AS STRING) AS antenna_id,
# MAGIC         CAST(team AS STRING)       AS team,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY Employee_Code, antenna_id
# MAGIC             ORDER BY modified_on DESC
# MAGIC         ) AS rn
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_emp_team_wax
# MAGIC ),
# MAGIC 
# MAGIC emp_dedup AS (
# MAGIC     SELECT
# MAGIC         CAST(r.Work_Day AS DATE)          AS created_date,
# MAGIC         TRIM(CAST(r.antenna_id AS STRING)) AS antenna_id,
# MAGIC         t.team,
# MAGIC         r.employee_code,
# MAGIC         r.first_name_thai,
# MAGIC         MAX(CAST(r.total_workhour AS DOUBLE)) AS emp_work_min
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_employee_time_group_rfid r
# MAGIC     INNER JOIN emp_team_map t
# MAGIC         ON  r.employee_code = t.Employee_Code
# MAGIC         AND TRIM(CAST(r.antenna_id AS STRING)) = t.antenna_id
# MAGIC         AND t.rn = 1
# MAGIC     WHERE r.antenna_id IS NOT NULL
# MAGIC       AND UPPER(TRIM(r.sub_department)) = 'WAX ROOM'
# MAGIC     GROUP BY
# MAGIC         CAST(r.Work_Day AS DATE),
# MAGIC         TRIM(CAST(r.antenna_id AS STRING)),
# MAGIC         t.team,
# MAGIC         r.employee_code,
# MAGIC         r.first_name_thai
# MAGIC ),
# MAGIC 
# MAGIC emp_hours AS (
# MAGIC     SELECT
# MAGIC         created_date,
# MAGIC         antenna_id,
# MAGIC         team,
# MAGIC         SUM(emp_work_min)                                          AS total_workhour,
# MAGIC         concat_ws(',', sort_array(collect_set(employee_code)))     AS employee_code,
# MAGIC         concat_ws(',', sort_array(collect_set(first_name_thai)))   AS first_name_thai
# MAGIC     FROM emp_dedup
# MAGIC     GROUP BY created_date, antenna_id, team
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     oa.created_date,
# MAGIC     oa.team,
# MAGIC     'WAX ROOM'                         AS sub_department,
# MAGIC     oa.antenna_id,
# MAGIC     oa.out_qty,
# MAGIC     oa.total_runtime_qty,
# MAGIC     eh.total_workhour,
# MAGIC     eh.employee_code,
# MAGIC     eh.first_name_thai,
# MAGIC     'WAX & CASTING'                    AS prod_line,
# MAGIC     'WAX'                              AS cell_line,
# MAGIC     'WAX'                              AS m_group,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN eh.total_workhour IS NULL      THEN NULL
# MAGIC         WHEN eh.total_workhour = 0          THEN NULL
# MAGIC         WHEN oa.total_runtime_qty IS NULL   THEN NULL
# MAGIC         WHEN oa.total_runtime_qty = 0       THEN 0.0
# MAGIC         ELSE (oa.total_runtime_qty / eh.total_workhour) * 100.0
# MAGIC     END AS efficiency_pct,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN oa.total_runtime_qty IS NULL   THEN 'Missing Modification'
# MAGIC         WHEN eh.total_workhour IS NULL      THEN 'Missing Workhour'
# MAGIC         WHEN eh.total_workhour = 0          THEN 'Missing Workhour'
# MAGIC         ELSE 'Complete'
# MAGIC     END AS data_quality_flag,
# MAGIC 
# MAGIC     CURRENT_TIMESTAMP()                AS created_on,
# MAGIC     CURRENT_TIMESTAMP()                AS modified_on,
# MAGIC     CURRENT_TIMESTAMP()                AS change_ts
# MAGIC 
# MAGIC FROM output_agg oa
# MAGIC 
# MAGIC LEFT JOIN emp_hours eh
# MAGIC     ON  eh.created_date = oa.created_date
# MAGIC     AND eh.antenna_id = CAST(oa.antenna_id AS STRING)
# MAGIC     AND eh.team = oa.team
# MAGIC 
# MAGIC ORDER BY
# MAGIC     oa.created_date DESC,
# MAGIC     oa.team,
# MAGIC     oa.antenna_id

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# # Wax Cycle Time Compared with Standard

# MARKDOWN ********************

# ## Silver + Gold Waxing and Casting Status

# CELL ********************

# MAGIC %%sql
# MAGIC -- ==============================================================
# MAGIC -- gold_wax_cycle_time_compare — Materialized Delta Table (Spark SQL)
# MAGIC -- ==============================================================
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_wax_cycle_time_compare
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH
# MAGIC -- ------------------ 1) LOAD STATUS ------------------
# MAGIC base AS (
# MAGIC   SELECT
# MAGIC     prod_order_no,
# MAGIC     CAST(prod_order_line_no AS STRING) AS prod_order_line_no,
# MAGIC     created_on,
# MAGIC     type_name,
# MAGIC     team,
# MAGIC     machine_center_no,
# MAGIC     employee_no,
# MAGIC     out_qty,
# MAGIC     user_id,
# MAGIC     antenna_id
# MAGIC   FROM Gold_Production_Lakehouse.prod.gold_waxing_and_casting_status
# MAGIC   WHERE machine_center_no IN ('WAX INJECT','WAX FILINIG','WAX SETTING')
# MAGIC ),
# MAGIC 
# MAGIC -- ------------------ 1.1) ROUTING FROM MIRROR ------------------
# MAGIC -- Mirror schema -> logical columns:
# MAGIC --   `Prod. Order No.`       -> prod_order_no
# MAGIC --   `Routing Reference No.` -> prod_order_line_no
# MAGIC --   `Routing No.`           -> machine_center_no
# MAGIC --   `Run Time`              -> run_time_min
# MAGIC route AS (
# MAGIC   SELECT
# MAGIC     CAST(`Prod. Order No.` AS STRING)               AS prod_order_no,
# MAGIC     CAST(`Routing Reference No.` AS STRING)         AS prod_order_line_no,
# MAGIC     CAST(`Routing No.` AS STRING)                   AS machine_center_no,
# MAGIC     CAST(`Run Time` AS DOUBLE)                      AS run_time_min
# MAGIC   FROM Silver_BC_Lakehouse.bc.`Prod Order Routing Line`
# MAGIC ),
# MAGIC 
# MAGIC -- ------------------ 2) NORMALIZE (Start/End) ------------------
# MAGIC normalized AS (
# MAGIC   SELECT
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     machine_center_no,
# MAGIC     team,
# MAGIC     employee_no,
# MAGIC     user_id,
# MAGIC     out_qty,
# MAGIC     antenna_id,
# MAGIC     created_on,
# MAGIC     CASE
# MAGIC       WHEN type_name = 'To employee'   THEN 'Start'
# MAGIC       WHEN type_name = 'From employee' THEN 'End'
# MAGIC       ELSE NULL
# MAGIC     END AS event_type
# MAGIC   FROM base
# MAGIC   WHERE type_name IN ('To employee','From employee')
# MAGIC ),
# MAGIC 
# MAGIC -- ------------------ 3) SPLIT STARTS / ENDS ------------------
# MAGIC starts AS (
# MAGIC   SELECT
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     machine_center_no,
# MAGIC     team,
# MAGIC     employee_no,
# MAGIC     user_id AS user_id_start,
# MAGIC     antenna_id,
# MAGIC     created_on AS start_time
# MAGIC   FROM normalized
# MAGIC   WHERE event_type = 'Start'
# MAGIC ),
# MAGIC 
# MAGIC ends AS (
# MAGIC   SELECT
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     machine_center_no,
# MAGIC     antenna_id,
# MAGIC     created_on AS end_time,
# MAGIC     COALESCE(CAST(out_qty AS DOUBLE), 1.0) AS qty_out
# MAGIC   FROM normalized
# MAGIC   WHERE event_type = 'End'
# MAGIC ),
# MAGIC 
# MAGIC -- ------------------ 4) CLOSEST-END-AFTER-START PAIRING ------------------
# MAGIC starts_idxed AS (
# MAGIC   SELECT
# MAGIC     s.*,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY s.prod_order_no, s.prod_order_line_no, s.machine_center_no
# MAGIC       ORDER BY s.start_time ASC
# MAGIC     ) AS start_id
# MAGIC   FROM starts s
# MAGIC ),
# MAGIC 
# MAGIC candidates AS (
# MAGIC   SELECT
# MAGIC     s.prod_order_no,
# MAGIC     s.prod_order_line_no,
# MAGIC     s.machine_center_no,
# MAGIC     s.team,
# MAGIC     s.employee_no,
# MAGIC     s.user_id_start,
# MAGIC     s.antenna_id,
# MAGIC     s.start_time,
# MAGIC     e.end_time,
# MAGIC     e.qty_out,
# MAGIC     CAST(unix_timestamp(e.end_time) - unix_timestamp(s.start_time) AS BIGINT) AS duration_sec,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY s.prod_order_no, s.prod_order_line_no, s.machine_center_no, s.start_id
# MAGIC       ORDER BY (unix_timestamp(e.end_time) - unix_timestamp(s.start_time)) ASC
# MAGIC     ) AS best_rn
# MAGIC   FROM starts_idxed s
# MAGIC   INNER JOIN ends e
# MAGIC     ON  s.prod_order_no      = e.prod_order_no
# MAGIC     AND s.prod_order_line_no = e.prod_order_line_no
# MAGIC     AND s.machine_center_no  = e.machine_center_no
# MAGIC     AND e.end_time > s.start_time
# MAGIC ),
# MAGIC 
# MAGIC paired AS (
# MAGIC   SELECT
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     machine_center_no,
# MAGIC     team,
# MAGIC     employee_no,
# MAGIC     user_id_start,
# MAGIC     antenna_id,
# MAGIC     start_time,
# MAGIC     end_time,
# MAGIC     qty_out,
# MAGIC     duration_sec,
# MAGIC     TO_DATE(end_time) AS work_date,
# MAGIC     CASE
# MAGIC       WHEN LOWER(COALESCE(user_id_start, '')) = 'outsource@ennovie.com' THEN 'Outsource'
# MAGIC       ELSE 'In-house'
# MAGIC     END AS work_type
# MAGIC   FROM candidates
# MAGIC   WHERE best_rn = 1
# MAGIC ),
# MAGIC 
# MAGIC -- ------------------ 5) AGGREGATE TO MATCH SQL VIEW ------------------
# MAGIC actual_agg AS (
# MAGIC   SELECT
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     CAST(team AS STRING) AS team,
# MAGIC     machine_center_no,
# MAGIC     work_type,
# MAGIC     MAX(work_date) AS last_exit_date,
# MAGIC     MIN(work_date) AS first_entry_date,
# MAGIC     COUNT(1)       AS records,
# MAGIC     SUM(duration_sec) AS sum_sec,
# MAGIC     SUM(qty_out)      AS total_qty,
# MAGIC     CASE
# MAGIC       WHEN SUM(qty_out) IS NULL OR SUM(qty_out) = 0 THEN CAST(NULL AS DOUBLE)
# MAGIC       ELSE (SUM(duration_sec) / 60.0) / SUM(qty_out)
# MAGIC     END AS AvgPerPiece
# MAGIC   FROM paired
# MAGIC   GROUP BY prod_order_no, prod_order_line_no, CAST(team AS STRING), machine_center_no, work_type
# MAGIC ),
# MAGIC 
# MAGIC -- ------------------ 6) STANDARD TIME (minutes per piece) ------------------
# MAGIC routing_std AS (
# MAGIC   SELECT
# MAGIC     prod_order_no,
# MAGIC     prod_order_line_no,
# MAGIC     machine_center_no,
# MAGIC     SUM(run_time_min) AS standard_min_per_piece
# MAGIC   FROM route
# MAGIC   GROUP BY prod_order_no, prod_order_line_no, machine_center_no
# MAGIC )
# MAGIC 
# MAGIC -- ------------------ 7) JOIN + FINAL SHAPE ------------------
# MAGIC SELECT
# MAGIC   a.prod_order_no,
# MAGIC   a.prod_order_line_no,
# MAGIC   a.team,
# MAGIC   a.machine_center_no,
# MAGIC   a.last_exit_date,
# MAGIC   a.first_entry_date,
# MAGIC   a.work_type,
# MAGIC   a.total_qty,
# MAGIC   a.AvgPerPiece AS Actual_Min_Per_Piece,
# MAGIC   r.standard_min_per_piece AS Standard_Min_Per_Piece,
# MAGIC   (a.AvgPerPiece - r.standard_min_per_piece) AS Diff_Min_Per_Piece,
# MAGIC   CASE
# MAGIC     WHEN r.standard_min_per_piece > 0 AND a.AvgPerPiece IS NOT NULL
# MAGIC       THEN (r.standard_min_per_piece / a.AvgPerPiece) * 100.0
# MAGIC     ELSE NULL
# MAGIC   END AS Efficiency_Percent,
# MAGIC   'min/piece' AS Unit,
# MAGIC   current_timestamp() AS _modified_any
# MAGIC FROM actual_agg a
# MAGIC LEFT JOIN routing_std r
# MAGIC   ON  r.prod_order_no      = a.prod_order_no
# MAGIC   AND r.prod_order_line_no = a.prod_order_line_no
# MAGIC   AND r.machine_center_no  = a.machine_center_no
# MAGIC ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Casting Summary

# MARKDOWN ********************

# ## Gold + Silver (Major)

# CELL ********************

TARGET = "Gold_Production_Lakehouse.prod.gold_casting_summary"

full_sql = f"""
CREATE OR REPLACE TABLE {TARGET}
USING DELTA
AS
WITH
-- 1) Source tables
pl AS (
    SELECT
        prod_line_due_date,
        prod_line_start_date,
        prod_line_end_date,
        prod_order_due_date,
        prod_order_no,
        prod_order_line_no,
        FG_item_no,
        prod_item_line,
        item_location,
        prod_line_quantity,
        prod_line_finished_quantity,
        prod_line_remaining_quantity,
        sales_order_no,
        ref_prod_order,
        prod_order_status
    FROM Gold_Production_Lakehouse.prod.gold_production_order
    WHERE prod_order_status IN ('Released', 'Firm Planned')
),

/* =========================
   EMP (match first query)
   - keep both type_name
   - CorrectCurrentLocation + current_location_code: from 'In location in'
   - machine_center_no (+ team/user_id): from 'To employee'
   - keep modified_in_location_in / modified_to_employee
   ========================= */
emp_raw AS (
    SELECT
        created_on,
        modified_on,
        prod_order_no,
        prod_order_line_no,
        type_name,
        prod_order_status,
        `open`,
        sales_order_no,
        current_location_code,
        past_location_code,
        employee_no,
        user_id,
        quantity,
        remaining_quantity,
        item_no,
        machine_center_no,
        out_qty,
        pol,
        created_on_time,
        CorrectCurrentLocation,
        team,
        ROW_NUMBER() OVER (
            PARTITION BY prod_order_no, prod_order_line_no, item_no, type_name
            ORDER BY COALESCE(modified_on, created_on) DESC
        ) AS rn_type
    FROM Gold_Production_Lakehouse.prod.gold_waxing_and_casting_status
    WHERE type_name IN ('In location in', 'To employee')
      AND `open` = 'Yes'
),

emp AS (
    SELECT
        prod_order_no,
        prod_order_line_no,
        item_no,

        -- From 'In location in'
        MAX(CASE WHEN type_name = 'In location in' THEN CorrectCurrentLocation END) AS CorrectCurrentLocation,
        MAX(CASE WHEN type_name = 'In location in' THEN current_location_code END)   AS current_location_code,

        -- From 'To employee'
        MAX(CASE WHEN type_name = 'To employee' THEN machine_center_no END) AS machine_center_no,
        MAX(CASE WHEN type_name = 'To employee' THEN team END)              AS team,
        MAX(CASE WHEN type_name = 'To employee' THEN user_id END)           AS user_id,

        -- Keep these timestamps (like first query)
        MAX(CASE WHEN type_name = 'In location in' THEN COALESCE(modified_on, created_on) END) AS modified_in_location_in,
        MAX(CASE WHEN type_name = 'To employee'    THEN COALESCE(modified_on, created_on) END) AS modified_to_employee
    FROM emp_raw
    WHERE rn_type = 1
    GROUP BY prod_order_no, prod_order_line_no, item_no
),

cast AS (
    SELECT
        cp.prod_order_no,
        cp.prod_order_line_no,
        cp.casting_prod_order,
        ct.casting_tree_no,
        cp.item_no AS itemCST,
        cp.casting_qty_to_tree,
        cp.casting_qty_passed,
        (cp.casting_qty_to_tree - cp.casting_qty_passed) AS casting_qty_reject,
        ct.casting_status
    FROM Silver_Production_Lakehouse.prod.silver_casting_parts cp
    LEFT JOIN Silver_Production_Lakehouse.prod.silver_casting_tree ct
        ON cp.casting_prod_order = ct.casting_prod_order
),

it AS (
    SELECT
        `No.`                 AS item_no,
        `Description`         AS item_description,
        `Metal Category Code` AS item_metal_category,
        `Item Category Code`  AS item_category_code
    FROM Silver_BC_Lakehouse.bc.`Item`
),

sales AS (
    SELECT
        SalesorderNo AS salesorder_no,
        CusNo        AS CustomerNo,
        CusAbbr      AS CustomerAbbr
    FROM Gold_Production_Lakehouse.prod.gold_sales_order
),

-- 2) ProdLineData (window maxes)
prod_line AS (
    SELECT
        p.*,

        -- OPTION C: keep nulls null (no inheritance); just rename
        p.sales_order_no AS sales_order,

        CAST(
            MAX(CASE WHEN p.prod_order_line_no = 10000 THEN p.prod_line_start_date END)
            OVER (PARTITION BY p.prod_order_no)
            AS date
        ) AS fg_start_date,

        -- fg_due_date inherits prod_order_due_date from gold_production_order,
        -- which Notebook 4 populates from MAX(scheduled_end_date) in the latest
        -- engine_run of planning_forward_schedule (with BC `Due Date` fallback).
        -- Wrapped in MAX OVER for shape compatibility with the rest of the
        -- window-based projections; prod_order_due_date is constant within a
        -- prod_order_no so MAX is a no-op.
        MAX(p.prod_order_due_date) OVER (PARTITION BY p.prod_order_no) AS fg_due_date,

        -- ✅ FIX: normalize item_location + use MIN for "casting due/start" (usually the scheduled/target date you want)
        MAX(
            CASE
                WHEN UPPER(TRIM(p.item_location)) = 'CST_CUT' THEN p.prod_line_due_date
            END
        ) OVER (PARTITION BY p.prod_order_no, p.prod_item_line) AS casting_due_date,

        CAST(
            MIN(
                CASE
                    WHEN UPPER(TRIM(p.item_location)) = 'CST_CUT' THEN p.prod_line_start_date
                END
            ) OVER (PARTITION BY p.prod_order_no, p.prod_item_line)
            AS date
        ) AS casting_start_date

    FROM pl p
),

-- 3) FinalBase (match first query: include the same placeholder columns + joins + rn)
final_base AS (
    SELECT
        p.prod_order_no,
        p.prod_order_line_no,
        p.FG_item_no,
        p.prod_item_line,
        p.item_location,
        p.prod_line_quantity,
        p.prod_line_finished_quantity,
        p.prod_line_remaining_quantity,
        p.prod_line_due_date,
        p.prod_line_start_date,
        p.prod_line_end_date,
        p.sales_order,
        p.fg_start_date,
        p.fg_due_date,
        p.casting_start_date,
        p.casting_due_date,

        -- keep prod line status
        p.prod_order_status AS prod_order_status,

        -- fields from emp (placeholders exactly like first query)
        NULL AS created_on,
        NULL AS modified_on,
        NULL AS type_name,
        e.user_id,
        'Yes' AS `open`,
        NULL AS emp_sales_order_no,
        NULL AS quantity,
        NULL AS remaining_quantity,

        e.current_location_code,
        e.CorrectCurrentLocation,
        e.machine_center_no,
        e.team,

        -- keep these (present in first query's emp select)
        e.modified_in_location_in,
        e.modified_to_employee,

        c.casting_prod_order,
        c.casting_tree_no,
        c.itemCST,
        c.casting_qty_to_tree,
        c.casting_qty_passed,
        c.casting_qty_reject,
        c.casting_status,

        i.item_description,
        i.item_metal_category,
        i.item_category_code,

        s.CustomerNo,
        s.CustomerAbbr,

        CASE
            WHEN i.item_metal_category = 'SILVER 925' THEN 'SILVER'
            WHEN i.item_metal_category IN ('14KW','14KY','14KR','18KR','18KW','18KY','9KW','9KY') THEN 'GOLD'
            ELSE i.item_metal_category
        END AS metal_category,

        CASE
            WHEN c.casting_status IS NOT NULL AND LTRIM(RTRIM(c.casting_status)) <> '' THEN LTRIM(RTRIM(c.casting_status))
            WHEN e.machine_center_no IS NOT NULL AND LTRIM(RTRIM(e.machine_center_no)) <> '' THEN LTRIM(RTRIM(e.machine_center_no))
            ELSE 'Not Start'
        END AS new_status,

        -- OPTION C: downstream SO derived ONLY from prod line SO
        p.sales_order AS new_so,

        CASE
            WHEN c.casting_qty_to_tree IS NOT NULL AND c.casting_qty_to_tree <> 0 THEN c.casting_qty_to_tree
            ELSE p.prod_line_quantity
        END AS new_qty,

        ROW_NUMBER() OVER (
            PARTITION BY p.prod_order_no, p.prod_item_line
            ORDER BY p.prod_order_line_no DESC
        ) AS rn

    FROM prod_line p
    LEFT JOIN emp e
        ON  p.prod_order_no      = e.prod_order_no
        AND p.prod_order_line_no = e.prod_order_line_no
        AND p.prod_item_line     = e.item_no
    LEFT JOIN cast c
        ON  p.prod_order_no      = c.prod_order_no
        AND p.prod_order_line_no = c.prod_order_line_no
    LEFT JOIN it i
        ON i.item_no = p.prod_item_line
    LEFT JOIN sales s
        ON p.sales_order = s.salesorder_no
),

-- 4) Apply rn filter + business filters + compute window aggs (match first query)
result AS (
    SELECT
        fb.prod_order_no,
        fb.prod_order_line_no,
        fb.FG_item_no,
        fb.prod_item_line,
        fb.item_location,
        fb.fg_start_date,
        fb.fg_due_date,
        fb.casting_start_date,
        fb.casting_due_date,

        -- ✅ ADDED: line_wax_due_date = prod_line_end_date - 3 days (Spark-safe)
        -- CASE
        --     WHEN fb.prod_line_end_date IS NOT NULL
        --     THEN date_sub(fb.prod_line_end_date, 3)
        --     ELSE NULL
        -- END AS line_wax_due_date,

        CASE
            WHEN prod_line_end_date <= current_date() 
            THEN prod_line_end_date
            ELSE GREATEST(date_sub(prod_line_end_date, 3), current_date())
        END AS line_wax_due_date,

        fb.prod_order_status,
        fb.current_location_code,
        fb.CorrectCurrentLocation,
        fb.machine_center_no,
        fb.team,
        fb.casting_prod_order,

        LTRIM(RTRIM(REPLACE(fb.casting_tree_no, 'TREE No.', ''))) AS tree_no,

        fb.itemCST,
        fb.casting_qty_to_tree,
        fb.casting_qty_passed,
        fb.casting_qty_reject,
        fb.casting_status,
        fb.CustomerNo,
        fb.CustomerAbbr,
        fb.metal_category,
        fb.new_status,

        CASE
            WHEN fb.new_so IS NULL THEN NULL
            ELSE CONCAT(SUBSTRING(fb.new_so, 1, 2), RIGHT(fb.new_so, 4))
        END AS so_abbr,

        fb.new_qty,

        SUM(fb.new_qty) OVER (PARTITION BY fb.prod_order_no, fb.prod_item_line) AS total_qty,

        SUM(
            CASE
                WHEN UPPER(LTRIM(RTRIM(fb.new_status))) = 'COMPLETE'
                    THEN COALESCE(fb.casting_qty_to_tree, 0)
                ELSE 0
            END
        ) OVER (PARTITION BY fb.prod_order_no, fb.prod_item_line) AS in_wh,

        CASE
            WHEN
                ( SUM(fb.new_qty) OVER (PARTITION BY fb.prod_order_no, fb.prod_item_line)
                  - SUM(CASE WHEN UPPER(LTRIM(RTRIM(fb.new_status))) = 'COMPLETE'
                             THEN COALESCE(fb.casting_qty_to_tree, 0) ELSE 0 END
                       ) OVER (PARTITION BY fb.prod_order_no, fb.prod_item_line)
                ) = 0
            THEN NULL
            ELSE
                ( SUM(fb.new_qty) OVER (PARTITION BY fb.prod_order_no, fb.prod_item_line)
                  - SUM(CASE WHEN UPPER(LTRIM(RTRIM(fb.new_status))) = 'COMPLETE'
                             THEN COALESCE(fb.casting_qty_to_tree, 0) ELSE 0 END
                       ) OVER (PARTITION BY fb.prod_order_no, fb.prod_item_line)
                )
        END AS remaining_qty,

        CONCAT(fb.prod_order_no, fb.prod_item_line) AS poi,

        -- SAFE watermark equivalent (only candidate was modified_on; in first query it's NULL)
        fb.modified_on AS _modified_any

    FROM final_base fb
    WHERE fb.rn = 1
      AND fb.prod_item_line LIKE 'C%'
      AND fb.prod_line_remaining_quantity > 0
)

SELECT
    prod_order_no,
    prod_order_line_no,
    FG_item_no,
    prod_item_line,
    item_location,
    fg_start_date,
    fg_due_date,
    casting_start_date,
    casting_due_date,

    -- ✅ ADDED to final output
    line_wax_due_date,

    prod_order_status,
    current_location_code,
    CorrectCurrentLocation,
    machine_center_no,
    team,
    casting_prod_order,
    tree_no,
    itemCST,
    CAST(casting_qty_to_tree AS DECIMAL(38,10)) AS casting_qty_to_tree,
    CAST(casting_qty_passed  AS DECIMAL(38,10)) AS casting_qty_passed,
    CAST(casting_qty_reject  AS DECIMAL(38,10)) AS casting_qty_reject,
    casting_status,
    CustomerNo,
    CustomerAbbr,
    metal_category,
    new_status,
    so_abbr,
    CAST(new_qty AS DECIMAL(38,10)) AS new_qty,
    CAST(total_qty AS DECIMAL(38,10)) AS total_qty,
    CAST(in_wh     AS DECIMAL(38,10)) AS in_wh,
    CAST(remaining_qty AS DECIMAL(38,10)) AS remaining_qty,
    poi,
    _modified_any
FROM result
"""

spark.sql(full_sql)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Casting with Mold

# CELL ********************

# MAGIC %%sql
# MAGIC -- FULL RELOAD (rebuild table from scratch)
# MAGIC DROP TABLE IF EXISTS Gold_Production_Lakehouse.prod.gold_casting_with_mold;
# MAGIC 
# MAGIC CREATE TABLE Gold_Production_Lakehouse.prod.gold_casting_with_mold
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH latest_task AS (
# MAGIC     SELECT
# MAGIC         t.mold_no,
# MAGIC         t.employee_id,
# MAGIC         t.assigned_to,
# MAGIC         t.mold_task_status,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY t.mold_no
# MAGIC             ORDER BY COALESCE(t.modified_on, t.created_on) DESC
# MAGIC         ) AS rn
# MAGIC     FROM Silver_Production_Lakehouse.prod.silver_mold_task t
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    BASE : gold_casting_summary
# MAGIC ========================= */
# MAGIC base AS (
# MAGIC     SELECT
# MAGIC         c.prod_order_no,
# MAGIC         c.prod_order_line_no,
# MAGIC         c.FG_item_no,
# MAGIC         c.prod_item_line,
# MAGIC         c.item_location,
# MAGIC         c.fg_start_date,
# MAGIC         c.fg_due_date,
# MAGIC         c.casting_start_date AS line_wax_start_date,
# MAGIC         c.line_wax_due_date,
# MAGIC         c.casting_due_date,
# MAGIC         c.prod_order_status,
# MAGIC         c.CustomerNo,
# MAGIC         c.CustomerAbbr,
# MAGIC         c.metal_category,
# MAGIC         c.new_status,
# MAGIC         c.so_abbr,
# MAGIC         c.new_qty,
# MAGIC         c.total_qty,
# MAGIC         c.in_wh,
# MAGIC         c.remaining_qty
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_casting_summary c
# MAGIC     WHERE c.new_status = 'Not Start'
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    Mold Masters
# MAGIC ========================= */
# MAGIC molds AS (
# MAGIC     SELECT DISTINCT
# MAGIC         c.prod_item_line,
# MAGIC         m.mold_no,
# MAGIC         m.mold_status,
# MAGIC         m.mold_approved,
# MAGIC         m.mold_blocked,
# MAGIC         m.location_name
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_casting_summary c
# MAGIC     LEFT JOIN Silver_Production_Lakehouse.prod.silver_mold_master m
# MAGIC         ON TRIM(c.prod_item_line) = TRIM(m.item_name)
# MAGIC     WHERE c.new_status = 'Not Start'
# MAGIC       AND (
# MAGIC             m.mold_blocked IS NULL
# MAGIC             OR UPPER(TRIM(CAST(m.mold_blocked AS STRING))) NOT IN ('YES','TRUE','1')
# MAGIC           )
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    Mold + Latest Task
# MAGIC ========================= */
# MAGIC mold_with_task AS (
# MAGIC     SELECT
# MAGIC         mo.prod_item_line,
# MAGIC         mo.mold_no,
# MAGIC         mo.mold_approved,
# MAGIC         mo.mold_blocked,
# MAGIC         mo.location_name,
# MAGIC 
# MAGIC         /* ===== Master status ===== */
# MAGIC         CASE
# MAGIC             WHEN mo.mold_status IN ('Pending','Pending Approval','In Progress') THEN 'Pending'
# MAGIC             WHEN mo.mold_status = 'Quality Failed' THEN 'Quality Failed'
# MAGIC             WHEN mo.mold_status = 'Completed' THEN 'Completed'
# MAGIC         END AS master_status_group,
# MAGIC 
# MAGIC         /* ===== Task status ===== */
# MAGIC         CASE
# MAGIC             WHEN lt.mold_task_status IN ('Pending','In Progress','Ready For QC','Done')
# MAGIC                 THEN lt.mold_task_status
# MAGIC         END AS task_status_group,
# MAGIC 
# MAGIC         lt.employee_id,
# MAGIC         lt.assigned_to
# MAGIC     FROM molds mo
# MAGIC     LEFT JOIN latest_task lt
# MAGIC         ON mo.mold_no = lt.mold_no
# MAGIC        AND lt.rn = 1
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    KPI ต่อ prod_item_line
# MAGIC ========================= */
# MAGIC kpi AS (
# MAGIC     SELECT
# MAGIC         prod_item_line,
# MAGIC 
# MAGIC         /* ---------- MASTER ---------- */
# MAGIC         COUNT(DISTINCT CASE WHEN master_status_group = 'Pending' THEN mold_no END) AS master_pending_mold_cnt,
# MAGIC         COUNT(DISTINCT CASE WHEN master_status_group = 'Quality Failed' THEN mold_no END) AS master_failed_mold_cnt,
# MAGIC         COUNT(DISTINCT CASE WHEN master_status_group = 'Completed' THEN mold_no END) AS master_completed_mold_cnt,
# MAGIC 
# MAGIC         COUNT(DISTINCT CASE
# MAGIC             WHEN master_status_group = 'Pending'
# MAGIC              AND mold_approved = 'No'
# MAGIC              AND mold_blocked = 'No'
# MAGIC              AND location_name = 'Storage'
# MAGIC             THEN mold_no
# MAGIC         END) AS master_completed_mold_not_approved_cnt,
# MAGIC 
# MAGIC         CASE WHEN COUNT(DISTINCT CASE WHEN master_status_group = 'Pending' THEN mold_no END) > 0
# MAGIC             THEN 'Pending' END AS master_pending_status,
# MAGIC 
# MAGIC         CASE WHEN COUNT(DISTINCT CASE WHEN master_status_group = 'Quality Failed' THEN mold_no END) > 0
# MAGIC             THEN 'Quality Failed' END AS master_failed_status,
# MAGIC 
# MAGIC         CASE WHEN COUNT(DISTINCT CASE WHEN master_status_group = 'Completed' THEN mold_no END) > 0
# MAGIC             THEN 'Completed' END AS master_completed_status,
# MAGIC 
# MAGIC         /* ---------- TASK ---------- */
# MAGIC         COUNT(DISTINCT CASE WHEN task_status_group = 'Pending' THEN mold_no END) AS task_pending_mold_cnt,
# MAGIC         COUNT(DISTINCT CASE WHEN task_status_group = 'In Progress' THEN mold_no END) AS task_inprogress_mold_cnt,
# MAGIC         COUNT(DISTINCT CASE WHEN task_status_group = 'Ready For QC' THEN mold_no END) AS task_ready_for_qc_mold_cnt,
# MAGIC         COUNT(DISTINCT CASE WHEN task_status_group = 'Done' THEN mold_no END) AS task_done_mold_cnt,
# MAGIC 
# MAGIC         COUNT(DISTINCT COALESCE(CAST(employee_id AS STRING), assigned_to)) AS task_employee_cnt
# MAGIC     FROM mold_with_task
# MAGIC     GROUP BY prod_item_line
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     b.*,
# MAGIC 
# MAGIC     /* =========================
# MAGIC        QTY RANGE (based on new_qty)
# MAGIC     ========================= */
# MAGIC     CASE
# MAGIC         WHEN b.new_qty BETWEEN 0 AND 100 THEN '0 - 100'
# MAGIC         WHEN b.new_qty BETWEEN 101 AND 500 THEN '101 - 500'
# MAGIC         WHEN b.new_qty BETWEEN 501 AND 1000 THEN '501 - 1000'
# MAGIC         WHEN b.new_qty BETWEEN 1001 AND 2000 THEN '1001 - 2000'
# MAGIC         WHEN b.new_qty BETWEEN 2001 AND 3000 THEN '2001 - 3000'
# MAGIC         WHEN b.new_qty >= 3001 THEN 'more than 3001'
# MAGIC         ELSE 'Unknown'
# MAGIC     END AS qty_range,
# MAGIC 
# MAGIC     /* MASTER */
# MAGIC     k.master_pending_mold_cnt,
# MAGIC     k.master_failed_mold_cnt,
# MAGIC     k.master_completed_mold_cnt,
# MAGIC     k.master_completed_mold_not_approved_cnt,
# MAGIC     k.master_pending_status,
# MAGIC     k.master_failed_status,
# MAGIC     k.master_completed_status,
# MAGIC 
# MAGIC     /* TASK */
# MAGIC     k.task_pending_mold_cnt,
# MAGIC     k.task_inprogress_mold_cnt,
# MAGIC     k.task_ready_for_qc_mold_cnt,
# MAGIC     k.task_done_mold_cnt,
# MAGIC     k.task_employee_cnt
# MAGIC FROM base b
# MAGIC LEFT JOIN kpi k
# MAGIC     ON b.prod_item_line = k.prod_item_line
# MAGIC ;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Casting With Mold Not Start

# CELL ********************

# MAGIC %%sql
# MAGIC -- gold_casting_with_mold_not_start (Spark SQL)
# MAGIC -- Fixed: dedup silver_mold_master + silver_mold_task by latest load_ts
# MAGIC -- Added: parts_per_mold from gold_mold_item_parts (MAX across item_1/2/3)
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_casting_with_mold_not_start
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH
# MAGIC /* =========================
# MAGIC    PRE) Dedup silver_mold_master: keep latest load_ts per mold_no
# MAGIC ========================= */
# MAGIC mold_master_latest AS (
# MAGIC     SELECT
# MAGIC         mold_no, item_name, secondary_casted_part_name, third_casted_part_name,
# MAGIC         mold_blocked, mold_approved, mold_status, load_ts
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             m.*,
# MAGIC             ROW_NUMBER() OVER (PARTITION BY m.mold_no ORDER BY m.load_ts DESC) AS _rn
# MAGIC         FROM Silver_Production_Lakehouse.prod.silver_mold_master m
# MAGIC     ) mm
# MAGIC     WHERE mm._rn = 1
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    0) Demand: gold_casting_summary (Not Start only)
# MAGIC ========================= */
# MAGIC base_demand AS (
# MAGIC     SELECT
# MAGIC         ltrim(rtrim(c.prod_item_line)) AS item_name,
# MAGIC         c.CustomerAbbr,
# MAGIC         c.metal_category,
# MAGIC         sum(c.new_qty) AS item_qty,
# MAGIC         min(c.casting_start_date) AS line_wax_start_date
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_casting_summary c
# MAGIC     WHERE c.new_status = 'Not Start'
# MAGIC       AND nullif(ltrim(rtrim(c.prod_item_line)), '') IS NOT NULL
# MAGIC     GROUP BY
# MAGIC         ltrim(rtrim(c.prod_item_line)),
# MAGIC         c.CustomerAbbr,
# MAGIC         c.metal_category
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    1) Mold + Item (exclude blocked, unpivot 3 item columns)
# MAGIC ========================= */
# MAGIC mold_items AS (
# MAGIC     SELECT DISTINCT
# MAGIC         substring(m.mold_no, 1, instr(concat(m.mold_no, '-'), '-') - 1) AS mold_group,
# MAGIC         e.item_name
# MAGIC     FROM mold_master_latest m
# MAGIC     LATERAL VIEW explode(array(
# MAGIC         nullif(ltrim(rtrim(m.item_name)), ''),
# MAGIC         nullif(ltrim(rtrim(m.secondary_casted_part_name)), ''),
# MAGIC         nullif(ltrim(rtrim(m.third_casted_part_name)), '')
# MAGIC     )) e AS item_name
# MAGIC     WHERE e.item_name IS NOT NULL
# MAGIC       AND (
# MAGIC             m.mold_blocked IS NULL
# MAGIC             OR upper(ltrim(rtrim(CAST(m.mold_blocked AS STRING)))) NOT IN ('YES','TRUE','1')
# MAGIC           )
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    2) Item Set: 1 mold_group -> sorted item_1/2/3 + item_set_key
# MAGIC ========================= */
# MAGIC item_set AS (
# MAGIC     SELECT
# MAGIC         mold_group,
# MAGIC         max(CASE WHEN rn = 1 THEN item_name END) AS item_1,
# MAGIC         max(CASE WHEN rn = 2 THEN item_name END) AS item_2,
# MAGIC         max(CASE WHEN rn = 3 THEN item_name END) AS item_3,
# MAGIC         concat(
# MAGIC             coalesce(max(CASE WHEN rn = 1 THEN item_name END), ''), '|',
# MAGIC             coalesce(max(CASE WHEN rn = 2 THEN item_name END), ''), '|',
# MAGIC             coalesce(max(CASE WHEN rn = 3 THEN item_name END), '')
# MAGIC         ) AS item_set_key
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             mold_group,
# MAGIC             item_name,
# MAGIC             row_number() OVER (PARTITION BY mold_group ORDER BY item_name) AS rn
# MAGIC         FROM mold_items
# MAGIC     ) x
# MAGIC     GROUP BY mold_group
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    3) item -> mold_group (1 item = 1 group only)
# MAGIC ========================= */
# MAGIC item_to_group AS (
# MAGIC     SELECT
# MAGIC         mi.item_name,
# MAGIC         min(mi.mold_group) AS mold_group
# MAGIC     FROM mold_items mi
# MAGIC     GROUP BY mi.item_name
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    4) Demand mapped to item_set_key (LEFT JOIN keeps all Not Start)
# MAGIC ========================= */
# MAGIC demand_mapped AS (
# MAGIC     SELECT
# MAGIC         d.CustomerAbbr,
# MAGIC         d.metal_category,
# MAGIC         d.item_name,
# MAGIC         d.item_qty,
# MAGIC         d.line_wax_start_date,
# MAGIC 
# MAGIC         itg.mold_group,
# MAGIC         s.item_set_key,
# MAGIC 
# MAGIC         coalesce(s.item_set_key, concat('NO_MOLD|', d.item_name)) AS demand_group_key,
# MAGIC         CASE WHEN s.item_set_key IS NULL THEN 0 ELSE 1 END AS has_mold_flag
# MAGIC     FROM base_demand d
# MAGIC     LEFT JOIN item_to_group itg
# MAGIC       ON itg.item_name = d.item_name
# MAGIC     LEFT JOIN item_set s
# MAGIC       ON s.mold_group = itg.mold_group
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    5) Rank items within group (CUS, METAL, demand_group_key)
# MAGIC ========================= */
# MAGIC demand_ranked AS (
# MAGIC     SELECT
# MAGIC         dm.*,
# MAGIC         row_number() OVER (
# MAGIC             PARTITION BY dm.CustomerAbbr, dm.metal_category, dm.demand_group_key
# MAGIC             ORDER BY dm.item_name
# MAGIC         ) AS rn
# MAGIC     FROM demand_mapped dm
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    6) Pivot ITEM 1/2/3 + QTY 1/2/3
# MAGIC ========================= */
# MAGIC demand_pivot AS (
# MAGIC     SELECT
# MAGIC         CustomerAbbr,
# MAGIC         metal_category,
# MAGIC         demand_group_key,
# MAGIC         max(item_set_key) AS item_set_key,
# MAGIC 
# MAGIC         max(CASE WHEN rn = 1 THEN item_name END) AS item_1,
# MAGIC         max(CASE WHEN rn = 2 THEN item_name END) AS item_2,
# MAGIC         max(CASE WHEN rn = 3 THEN item_name END) AS item_3,
# MAGIC 
# MAGIC         sum(CASE WHEN rn = 1 THEN item_qty ELSE 0 END) AS qty_1,
# MAGIC         sum(CASE WHEN rn = 2 THEN item_qty ELSE 0 END) AS qty_2,
# MAGIC         sum(CASE WHEN rn = 3 THEN item_qty ELSE 0 END) AS qty_3,
# MAGIC 
# MAGIC         sum(item_qty) AS total_qty,
# MAGIC         min(line_wax_start_date) AS line_wax_start_date,
# MAGIC         max(has_mold_flag) AS has_mold_flag
# MAGIC     FROM demand_ranked
# MAGIC     WHERE rn <= 3
# MAGIC     GROUP BY
# MAGIC         CustomerAbbr,
# MAGIC         metal_category,
# MAGIC         demand_group_key
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    7) Latest task per mold_no (dedup by load_ts)
# MAGIC ========================= */
# MAGIC task_latest AS (
# MAGIC     SELECT
# MAGIC         t.*,
# MAGIC         row_number() OVER (
# MAGIC             PARTITION BY t.mold_no
# MAGIC             ORDER BY coalesce(t.modified_on, t.created_on) DESC, t.load_ts DESC
# MAGIC         ) AS rn
# MAGIC     FROM Silver_Production_Lakehouse.prod.silver_mold_task t
# MAGIC ),
# MAGIC task_current AS (
# MAGIC     SELECT
# MAGIC         mold_no,
# MAGIC         employee_id,
# MAGIC         assigned_to,
# MAGIC         mold_task_status,
# MAGIC         coalesce(modified_on, created_on) AS task_last_update
# MAGIC     FROM task_latest
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    8) Stock base: mold master (deduped) + item_set + task
# MAGIC ========================= */
# MAGIC stock_base AS (
# MAGIC     SELECT
# MAGIC         s.item_set_key,
# MAGIC         s.mold_group,
# MAGIC         m.mold_no,
# MAGIC         m.mold_approved,
# MAGIC         m.mold_status,
# MAGIC         tc.employee_id,
# MAGIC         tc.assigned_to,
# MAGIC         tc.mold_task_status,
# MAGIC         tc.task_last_update
# MAGIC     FROM mold_master_latest m
# MAGIC     JOIN item_set s
# MAGIC       ON s.mold_group = substring(m.mold_no, 1, instr(concat(m.mold_no, '-'), '-') - 1)
# MAGIC     LEFT JOIN task_current tc
# MAGIC       ON tc.mold_no = m.mold_no
# MAGIC     WHERE (
# MAGIC             m.mold_blocked IS NULL
# MAGIC             OR upper(ltrim(rtrim(CAST(m.mold_blocked AS STRING)))) NOT IN ('YES','TRUE','1')
# MAGIC           )
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    9) KPI mold counts (COUNT DISTINCT to prevent duplicates)
# MAGIC ========================= */
# MAGIC stock_kpi AS (
# MAGIC     SELECT
# MAGIC         item_set_key,
# MAGIC         count(DISTINCT mold_no) AS mold_stock,
# MAGIC         count(DISTINCT CASE WHEN upper(ltrim(rtrim(CAST(mold_approved AS STRING)))) IN ('YES','TRUE','1') THEN mold_no END) AS mold_app,
# MAGIC         count(DISTINCT CASE WHEN upper(ltrim(rtrim(CAST(mold_status AS STRING)))) IN ('PENDING','PENDING APPROVAL','IN PROGRESS') THEN mold_no END) AS mold_pend,
# MAGIC         count(DISTINCT CASE WHEN upper(ltrim(rtrim(CAST(mold_status AS STRING)))) = 'QUALITY FAILED' THEN mold_no END) AS mold_fail
# MAGIC     FROM stock_base
# MAGIC     GROUP BY item_set_key
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    10) Mold group list (comma-separated, sorted)
# MAGIC ========================= */
# MAGIC stock_moldgroup_list AS (
# MAGIC     SELECT
# MAGIC         item_set_key,
# MAGIC         concat_ws(',', sort_array(collect_set(mold_group))) AS mold
# MAGIC     FROM stock_base
# MAGIC     GROUP BY item_set_key
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    11) Distinct task per item_set_key
# MAGIC ========================= */
# MAGIC stock_task_distinct AS (
# MAGIC     SELECT DISTINCT
# MAGIC         sb.item_set_key,
# MAGIC         sb.mold_no,
# MAGIC         nullif(ltrim(rtrim(CAST(sb.employee_id AS STRING))), '') AS employee_id,
# MAGIC         nullif(ltrim(rtrim(CAST(sb.assigned_to AS STRING))), '') AS assigned_to,
# MAGIC         nullif(ltrim(rtrim(CAST(sb.mold_task_status AS STRING))), '') AS task_status,
# MAGIC         sb.task_last_update
# MAGIC     FROM stock_base sb
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    12) Task status counts
# MAGIC ========================= */
# MAGIC task_counts AS (
# MAGIC     SELECT
# MAGIC         item_set_key,
# MAGIC         count(DISTINCT CASE WHEN task_status = 'Pending'      THEN mold_no END) AS mold_task_pending,
# MAGIC         count(DISTINCT CASE WHEN task_status = 'In Progress'  THEN mold_no END) AS mold_task_inprogress,
# MAGIC         count(DISTINCT CASE WHEN task_status = 'Ready For QC' THEN mold_no END) AS mold_task_ready_for_qc,
# MAGIC         count(DISTINCT CASE WHEN task_status = 'Done'         THEN mold_no END) AS mold_task_done,
# MAGIC         count(DISTINCT CASE WHEN task_status IS NOT NULL THEN mold_no END) AS mold_has_task,
# MAGIC         max(task_last_update) AS task_last_update
# MAGIC     FROM stock_task_distinct
# MAGIC     GROUP BY item_set_key
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    13) Employee / assigned_to lists (comma-separated, sorted)
# MAGIC ========================= */
# MAGIC task_emp_list AS (
# MAGIC     SELECT
# MAGIC         item_set_key,
# MAGIC         concat_ws(',', sort_array(collect_set(employee_id))) AS task_employee_id
# MAGIC     FROM stock_task_distinct
# MAGIC     WHERE employee_id IS NOT NULL
# MAGIC     GROUP BY item_set_key
# MAGIC ),
# MAGIC task_assigned_list AS (
# MAGIC     SELECT
# MAGIC         item_set_key,
# MAGIC         concat_ws(',', sort_array(collect_set(assigned_to))) AS task_assigned_to
# MAGIC     FROM stock_task_distinct
# MAGIC     WHERE assigned_to IS NOT NULL
# MAGIC     GROUP BY item_set_key
# MAGIC ),
# MAGIC 
# MAGIC stock_task_summary AS (
# MAGIC     SELECT
# MAGIC         c.item_set_key,
# MAGIC         e.task_employee_id,
# MAGIC         a.task_assigned_to,
# MAGIC         c.mold_task_pending,
# MAGIC         c.mold_task_inprogress,
# MAGIC         c.mold_task_ready_for_qc,
# MAGIC         c.mold_task_done,
# MAGIC         c.mold_has_task,
# MAGIC         c.task_last_update
# MAGIC     FROM task_counts c
# MAGIC     LEFT JOIN task_emp_list e
# MAGIC       ON e.item_set_key = c.item_set_key
# MAGIC     LEFT JOIN task_assigned_list a
# MAGIC       ON a.item_set_key = c.item_set_key
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    14) Combine stock + task summary
# MAGIC ========================= */
# MAGIC stock_by_set AS (
# MAGIC     SELECT
# MAGIC         k.item_set_key,
# MAGIC         l.mold,
# MAGIC         k.mold_stock,
# MAGIC         k.mold_app,
# MAGIC         k.mold_pend,
# MAGIC         k.mold_fail,
# MAGIC         ts.task_employee_id,
# MAGIC         ts.task_assigned_to,
# MAGIC         ts.mold_task_pending,
# MAGIC         ts.mold_task_inprogress,
# MAGIC         ts.mold_task_ready_for_qc,
# MAGIC         ts.mold_task_done,
# MAGIC         ts.mold_has_task,
# MAGIC         ts.task_last_update
# MAGIC     FROM stock_kpi k
# MAGIC     LEFT JOIN stock_moldgroup_list l
# MAGIC       ON l.item_set_key = k.item_set_key
# MAGIC     LEFT JOIN stock_task_summary ts
# MAGIC       ON ts.item_set_key = k.item_set_key
# MAGIC ),
# MAGIC 
# MAGIC /* =========================
# MAGIC    NEW 15) parts_per_mold: deduplicate per part_item, MAX across item_1/2/3
# MAGIC ========================= */
# MAGIC mold_parts AS (
# MAGIC     SELECT
# MAGIC         part_item,
# MAGIC         MAX(parts_per_mold) AS parts_per_mold
# MAGIC     FROM Gold_Production_Lakehouse.prod.gold_mold_item_parts
# MAGIC     WHERE parts_per_mold IS NOT NULL
# MAGIC     GROUP BY part_item
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     d.CustomerAbbr AS cus,
# MAGIC     d.metal_category AS metal,
# MAGIC 
# MAGIC     d.item_1 AS item,
# MAGIC     d.item_2 AS item_2,
# MAGIC     d.item_3 AS item_3,
# MAGIC 
# MAGIC     CAST(d.qty_1 AS DECIMAL(18,2)) AS qty_1,
# MAGIC     CAST(d.qty_2 AS DECIMAL(18,2)) AS qty_2,
# MAGIC     CAST(d.qty_3 AS DECIMAL(18,2)) AS qty_3,
# MAGIC     CAST(d.total_qty AS DECIMAL(18,2)) AS total_qty,
# MAGIC 
# MAGIC     d.line_wax_start_date,
# MAGIC 
# MAGIC     CAST(d.has_mold_flag AS INT) AS has_mold_flag,
# MAGIC     CASE WHEN d.has_mold_flag = 1 THEN 'HAS_MOLD' ELSE 'NO_MOLD' END AS mold_flag,
# MAGIC 
# MAGIC     coalesce(s.mold,'') AS mold,
# MAGIC 
# MAGIC     coalesce(s.mold_stock,0) AS mold_stock,
# MAGIC     coalesce(s.mold_app,0)   AS mold_app,
# MAGIC     coalesce(s.mold_pend,0)  AS mold_pend,
# MAGIC     coalesce(s.mold_fail,0)  AS mold_fail,
# MAGIC 
# MAGIC     coalesce(s.task_employee_id,'') AS task_employee_id,
# MAGIC     coalesce(s.task_assigned_to,'') AS task_assigned_to,
# MAGIC 
# MAGIC     coalesce(s.mold_task_pending,0)      AS mold_task_pending,
# MAGIC     coalesce(s.mold_task_inprogress,0)   AS mold_task_inprogress,
# MAGIC     coalesce(s.mold_task_ready_for_qc,0) AS mold_task_ready_for_qc,
# MAGIC     coalesce(s.mold_task_done,0)         AS mold_task_done,
# MAGIC 
# MAGIC     coalesce(s.mold_has_task,0)          AS mold_has_task,
# MAGIC     s.task_last_update,
# MAGIC 
# MAGIC     -- NEW: parts_per_mold = MAX across item_1, item_2, item_3
# MAGIC     GREATEST(
# MAGIC         coalesce(mp1.parts_per_mold, 0),
# MAGIC         coalesce(mp2.parts_per_mold, 0),
# MAGIC         coalesce(mp3.parts_per_mold, 0)
# MAGIC     ) AS parts_per_mold
# MAGIC 
# MAGIC FROM demand_pivot d
# MAGIC LEFT JOIN stock_by_set s
# MAGIC   ON s.item_set_key = d.item_set_key
# MAGIC -- NEW: JOIN mold_parts for each of the 3 items
# MAGIC LEFT JOIN mold_parts mp1
# MAGIC   ON mp1.part_item = d.item_1
# MAGIC LEFT JOIN mold_parts mp2
# MAGIC   ON mp2.part_item = d.item_2
# MAGIC LEFT JOIN mold_parts mp3
# MAGIC   ON mp3.part_item = d.item_3
# MAGIC ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Mold Status

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_mold_status
# MAGIC USING DELTA
# MAGIC AS
# MAGIC /* ==========================================================
# MAGIC    M_Stock : นับ stock ที่ Location = MASTER
# MAGIC    Rule: Open = TRUE + Location = MASTER + Quantity > 0
# MAGIC    ไม่กรอง Document No. เพราะ mold เข้า MASTER ได้หลายทาง
# MAGIC    (ทำพิมพ์เสร็จแล้ว, WMT Transfer, ADJ STOCK, คืนพิมพ์ ฯลฯ)
# MAGIC ========================================================== */
# MAGIC WITH M_Stock AS (
# MAGIC     SELECT
# MAGIC         l.`Item No.` AS M_ItemNo,
# MAGIC         SUM(l.`Quantity`) AS Stock_Qty_Open
# MAGIC     FROM `Silver_BC_Lakehouse`.`bc`.`Item Ledger Entry` l
# MAGIC     WHERE UPPER(CAST(l.`Open` AS STRING)) IN ('YES','1','TRUE')
# MAGIC       AND l.`Location Code` = 'MASTER'
# MAGIC       AND l.`Quantity` > 0
# MAGIC     GROUP BY l.`Item No.`
# MAGIC ),
# MAGIC 
# MAGIC /* ==========================================================
# MAGIC    M_LastLocation_Open : entry ล่าสุดที่ยัง Open
# MAGIC    ORDER BY Entry No. DESC (แม่นกว่า Document No. DESC)
# MAGIC ========================================================== */
# MAGIC M_LastLocation_Open AS (
# MAGIC     SELECT
# MAGIC         x.`Item No.`       AS M_ItemNo,
# MAGIC         x.`Posting Date`,
# MAGIC         x.`Entry Type`,
# MAGIC         x.`Document No.`,
# MAGIC         x.`Quantity`,
# MAGIC         x.`Location Code`,
# MAGIC         CASE 
# MAGIC             WHEN x.`Location Code` = 'CST_CUT'
# MAGIC              AND (   x.`Entry Type` = 'Output'
# MAGIC                   OR x.`Document No.` LIKE 'พิมพ์ยังไม่เสร็จ%'
# MAGIC                  )
# MAGIC             THEN 1
# MAGIC             WHEN x.`Document No.` LIKE 'ทำพิมพ์เสร็จแล้ว%'
# MAGIC              AND x.`Location Code` = 'CST_CUT'
# MAGIC             THEN 1
# MAGIC             ELSE 0 
# MAGIC         END AS Is_Incomplete
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             l.*,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY l.`Item No.`
# MAGIC                 ORDER BY l.`Posting Date` DESC, l.`Entry No.` DESC
# MAGIC             ) AS rn
# MAGIC         FROM `Silver_BC_Lakehouse`.`bc`.`Item Ledger Entry` l
# MAGIC         WHERE UPPER(CAST(l.`Open` AS STRING)) IN ('YES','1','TRUE')
# MAGIC     ) x
# MAGIC     WHERE x.rn = 1
# MAGIC )
# MAGIC SELECT
# MAGIC     c.`No.`                      AS C0_ItemNo,
# MAGIC     c.`Description`              AS C0_Description,
# MAGIC 
# MAGIC     c.`MST Mapping`              AS M_MappingMaster,
# MAGIC 
# MAGIC     m.`Description`              AS M_Description,
# MAGIC     m.`Base Unit of Measure`     AS M_BaseUOM,
# MAGIC     m.`Inventory Posting Group`  AS M_InvPostingGroup,
# MAGIC     m.`Metal Category Code`      AS M_MetalCategory,
# MAGIC     m.`Product Type`             AS M_ProductType,
# MAGIC 
# MAGIC     CASE WHEN ll.Is_Incomplete = 1 THEN 0 
# MAGIC          ELSE COALESCE(s.Stock_Qty_Open, 0)
# MAGIC     END                              AS M_Stock_Open_Qty,
# MAGIC 
# MAGIC     ll.`Posting Date`                AS Last_Posting_Date_Open,
# MAGIC     ll.`Entry Type`                  AS Last_Entry_Type_Open,
# MAGIC     ll.`Document No.`                AS Last_Document_No_Open,
# MAGIC     CASE WHEN ll.Is_Incomplete = 1 THEN 0 
# MAGIC          ELSE ll.`Quantity` 
# MAGIC     END                              AS Last_Quantity_Open,
# MAGIC     CASE WHEN ll.Is_Incomplete = 1 THEN 'ยังไม่มีของ' 
# MAGIC          ELSE ll.`Location Code` 
# MAGIC     END                              AS Last_Location_Open
# MAGIC 
# MAGIC FROM `Silver_BC_Lakehouse`.`bc`.`Item` c
# MAGIC LEFT JOIN `Silver_BC_Lakehouse`.`bc`.`Item` m
# MAGIC     ON m.`No.` = c.`MST Mapping`
# MAGIC LEFT JOIN M_Stock s
# MAGIC     ON s.M_ItemNo = c.`MST Mapping`
# MAGIC LEFT JOIN M_LastLocation_Open ll
# MAGIC     ON ll.M_ItemNo = c.`MST Mapping`
# MAGIC WHERE
# MAGIC     c.`Blocked` = '0'
# MAGIC     AND c.`No.` LIKE 'C0%'
# MAGIC ORDER BY
# MAGIC     c.`No.`;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Mold Master

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.prod.gold_mold_master
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT 
# MAGIC     c.cus,
# MAGIC     c.metal,
# MAGIC     c.item,
# MAGIC     c.item_2,
# MAGIC     c.item_3,
# MAGIC     c.qty_1,
# MAGIC     c.qty_2,
# MAGIC     c.qty_3,
# MAGIC     c.total_qty,
# MAGIC     c.line_wax_start_date,
# MAGIC     c.has_mold_flag,
# MAGIC     c.mold_flag,
# MAGIC     c.mold,
# MAGIC     sm.mold_size_name,
# MAGIC     c.mold_stock,
# MAGIC     c.mold_app,
# MAGIC     c.mold_pend,
# MAGIC     c.mold_fail,
# MAGIC     c.task_employee_id,
# MAGIC     c.task_assigned_to,
# MAGIC     c.mold_task_pending,
# MAGIC     c.mold_task_inprogress,
# MAGIC     c.mold_task_ready_for_qc,
# MAGIC     c.mold_task_done,
# MAGIC     c.mold_has_task,
# MAGIC     c.task_last_update,
# MAGIC     c.parts_per_mold,
# MAGIC     m.C0_ItemNo,
# MAGIC     m.C0_Description,
# MAGIC     CASE
# MAGIC         WHEN m.M_Stock_Open_Qty IS NULL 
# MAGIC              OR m.M_Stock_Open_Qty = 0
# MAGIC         THEN CASE
# MAGIC                 WHEN c.cus = 'BKP' THEN 'BRW_MST'
# MAGIC                 ELSE 'NO_MST'
# MAGIC              END
# MAGIC         ELSE 'HAS_MST'
# MAGIC     END AS M_MasterStatus,
# MAGIC     m.M_Description,
# MAGIC     m.M_BaseUOM,
# MAGIC     m.M_InvPostingGroup,
# MAGIC     m.M_MetalCategory,
# MAGIC     m.M_ProductType,
# MAGIC     m.M_Stock_Open_Qty,
# MAGIC     m.Last_Posting_Date_Open,
# MAGIC     m.Last_Entry_Type_Open,
# MAGIC     m.Last_Document_No_Open,
# MAGIC     m.Last_Quantity_Open,
# MAGIC     m.Last_Location_Open,
# MAGIC     m.M_MappingMaster
# MAGIC FROM Gold_Production_Lakehouse.prod.gold_casting_with_mold_not_start c
# MAGIC JOIN Gold_Production_Lakehouse.prod.gold_mold_status m
# MAGIC     ON c.item = m.C0_ItemNo
# MAGIC LEFT JOIN (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT 
# MAGIC             *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY split(CAST(mold_no AS STRING), '-')[0]
# MAGIC                 ORDER BY modified_on DESC, load_ts DESC
# MAGIC             ) AS rn
# MAGIC         FROM Silver_Production_Lakehouse.prod.silver_mold_master
# MAGIC     ) x
# MAGIC     WHERE rn = 1
# MAGIC ) sm
# MAGIC     ON CAST(c.mold AS STRING) = split(CAST(sm.mold_no AS STRING), '-')[0];

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # APS rule

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.planning.gold_scheduling_customer_rule
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     customer_no, material_type, cell_code, priority_rank,
# MAGIC     CASE
# MAGIC         WHEN cell_code IN ('CELL105','CELL109')                                         THEN 'PRODLINE1'
# MAGIC         WHEN cell_code IN ('CELL201','CELL202','CELL203','CELL204','CELL205','CELL218') THEN 'PRODLINE2'
# MAGIC         WHEN cell_code IN ('CELL103','CELL104','CELL206','CELL207','CELL208','CELL209','CELL210','CELL219','CELL220') THEN 'PRODLINE3'
# MAGIC         WHEN cell_code IN ('CELL211','CELL212','CELL213')                               THEN 'PRODLINE4'
# MAGIC         WHEN cell_code IN ('CELL214','CELL215','CELL216','CELL217')                     THEN 'PRODLINE5'
# MAGIC         ELSE 'UNKNOWN'
# MAGIC     END AS production_line,
# MAGIC     current_timestamp() AS _load_timestamp
# MAGIC FROM VALUES
# MAGIC         -- Bangkok Kraft Production
# MAGIC         ('CD-00003', 'gold', 'CELL103', 1),
# MAGIC         ('CD-00003', 'gold', 'CELL109', 2),
# MAGIC         ('CD-00003', 'silver', 'CELL211', 1),
# MAGIC         ('CD-00003', 'silver', 'CELL212', 2),
# MAGIC         ('CD-00003', 'silver', 'CELL213', 3),
# MAGIC         ('CD-00003', 'silver', 'CELL214', 4),
# MAGIC         ('CD-00003', 'silver', 'CELL215', 5),
# MAGIC         ('CD-00003', 'silver', 'CELL216', 6),
# MAGIC         ('CD-00003', 'silver', 'CELL217', 7),
# MAGIC         ('CD-00003', 'silver', 'CELL219', 8),
# MAGIC         ('CD-00003', 'silver', 'CELL206', 9),
# MAGIC         ('CD-00003', 'silver', 'CELL207', 10),
# MAGIC         ('CD-00003', 'silver', 'CELL109', 11),
# MAGIC         ('CD-00003', 'bangle', 'CELL211', 1),
# MAGIC         ('CD-00003', 'bangle', 'CELL212', 2),
# MAGIC         ('CD-00003', 'bangle', 'CELL213', 3),
# MAGIC         ('CD-00003', 'bangle', 'CELL206', 4),
# MAGIC         ('CD-00003', 'bangle', 'CELL207', 5),
# MAGIC         ('CD-00003', 'bangle', 'CELL214', 6),
# MAGIC         ('CD-00003', 'bangle', 'CELL215', 7),
# MAGIC         ('CD-00003', 'bangle', 'CELL216', 8),
# MAGIC         ('CD-00003', 'bangle', 'CELL217', 9),
# MAGIC         ('CD-00003', 'bangle', 'CELL109', 10),
# MAGIC         ('CD-00003', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CD-00003', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CD-00003', 'bangle-lock', 'CELL210', 3),
# MAGIC         ('CD-00003', 'bangle-lock', 'CELL208', 4),
# MAGIC         ('CD-00003', 'bangle-lock', 'CELL109', 5),
# MAGIC         -- ENNOVIE
# MAGIC         ('CD-00004', 'gold', 'CELL103', 1),
# MAGIC         ('CD-00004', 'silver', 'CELL205', 1),
# MAGIC         ('CD-00004', 'silver', 'CELL201', 2),
# MAGIC         ('CD-00004', 'silver', 'CELL202', 3),
# MAGIC         ('CD-00004', 'silver', 'CELL203', 4),
# MAGIC         ('CD-00004', 'silver', 'CELL204', 5),
# MAGIC         ('CD-00004', 'silver', 'CELL214', 6),
# MAGIC         ('CD-00004', 'silver', 'CELL215', 7),
# MAGIC         ('CD-00004', 'silver', 'CELL216', 8),
# MAGIC         ('CD-00004', 'silver', 'CELL217', 9),
# MAGIC         ('CD-00004', 'silver', 'CELL211', 10),
# MAGIC         ('CD-00004', 'silver', 'CELL212', 11),
# MAGIC         ('CD-00004', 'silver', 'CELL213', 12),
# MAGIC         ('CD-00004', 'silver', 'CELL206', 13),
# MAGIC         ('CD-00004', 'silver', 'CELL207', 14),
# MAGIC         ('CD-00004', 'silver', 'CELL208', 15),
# MAGIC         ('CD-00004', 'silver', 'CELL209', 16),
# MAGIC         ('CD-00004', 'silver', 'CELL210', 17),
# MAGIC         ('CD-00004', 'silver', 'CELL219', 18),
# MAGIC         ('CD-00004', 'silver', 'CELL109', 19),
# MAGIC         ('CD-00004', 'bangle', 'CELL201', 1),
# MAGIC         ('CD-00004', 'bangle', 'CELL202', 2),
# MAGIC         ('CD-00004', 'bangle', 'CELL203', 3),
# MAGIC         ('CD-00004', 'bangle', 'CELL204', 4),
# MAGIC         ('CD-00004', 'bangle', 'CELL205', 5),
# MAGIC         ('CD-00004', 'bangle', 'CELL214', 6),
# MAGIC         ('CD-00004', 'bangle', 'CELL215', 7),
# MAGIC         ('CD-00004', 'bangle', 'CELL216', 8),
# MAGIC         ('CD-00004', 'bangle', 'CELL217', 9),
# MAGIC         ('CD-00004', 'bangle', 'CELL109', 10),
# MAGIC         ('CD-00004', 'bangle', 'CELL211', 11),
# MAGIC         ('CD-00004', 'bangle', 'CELL212', 12),
# MAGIC         ('CD-00004', 'bangle', 'CELL213', 13),
# MAGIC         ('CD-00004', 'bangle', 'CELL206', 14),
# MAGIC         ('CD-00004', 'bangle', 'CELL207', 15),
# MAGIC         ('CD-00004', 'bangle', 'CELL208', 16),
# MAGIC         ('CD-00004', 'bangle', 'CELL209', 17),
# MAGIC         ('CD-00004', 'bangle', 'CELL210', 18),
# MAGIC         ('CD-00004', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CD-00004', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CD-00004', 'bangle-lock', 'CELL210', 3),
# MAGIC         ('CD-00004', 'bangle-lock', 'CELL208', 4),
# MAGIC         -- DE BEERS JEWELLERS
# MAGIC         ('CI-00002', 'gold', 'CELL109', 1),
# MAGIC         ('CI-00002', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00002', 'bangle-lock', 'CELL208', 2),
# MAGIC         -- GUESS EUROPE SAGL
# MAGIC         ('CI-00004', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00004', 'silver', 'CELL206', 1),
# MAGIC         ('CI-00004', 'silver', 'CELL207', 2),
# MAGIC         ('CI-00004', 'silver', 'CELL208', 3),
# MAGIC         ('CI-00004', 'silver', 'CELL209', 4),
# MAGIC         ('CI-00004', 'silver', 'CELL210', 5),
# MAGIC         ('CI-00004', 'silver', 'CELL219', 6),
# MAGIC         ('CI-00004', 'silver', 'CELL201', 7),
# MAGIC         ('CI-00004', 'silver', 'CELL202', 8),
# MAGIC         ('CI-00004', 'silver', 'CELL203', 9),
# MAGIC         ('CI-00004', 'silver', 'CELL204', 10),
# MAGIC         ('CI-00004', 'silver', 'CELL205', 11),
# MAGIC         ('CI-00004', 'silver', 'CELL214', 12),
# MAGIC         ('CI-00004', 'silver', 'CELL215', 13),
# MAGIC         ('CI-00004', 'silver', 'CELL216', 14),
# MAGIC         ('CI-00004', 'silver', 'CELL217', 15),
# MAGIC         ('CI-00004', 'silver', 'CELL211', 16),
# MAGIC         ('CI-00004', 'silver', 'CELL212', 17),
# MAGIC         ('CI-00004', 'silver', 'CELL213', 18),
# MAGIC         ('CI-00004', 'bangle', 'CELL206', 1),
# MAGIC         ('CI-00004', 'bangle', 'CELL207', 2),
# MAGIC         ('CI-00004', 'bangle', 'CELL208', 3),
# MAGIC         ('CI-00004', 'bangle', 'CELL209', 4),
# MAGIC         ('CI-00004', 'bangle', 'CELL210', 5),
# MAGIC         ('CI-00004', 'bangle', 'CELL201', 6),
# MAGIC         ('CI-00004', 'bangle', 'CELL202', 7),
# MAGIC         ('CI-00004', 'bangle', 'CELL203', 8),
# MAGIC         ('CI-00004', 'bangle', 'CELL204', 9),
# MAGIC         ('CI-00004', 'bangle', 'CELL205', 10),
# MAGIC         ('CI-00004', 'bangle', 'CELL109', 11),
# MAGIC         ('CI-00004', 'bangle', 'CELL214', 12),
# MAGIC         ('CI-00004', 'bangle', 'CELL215', 13),
# MAGIC         ('CI-00004', 'bangle', 'CELL216', 14),
# MAGIC         ('CI-00004', 'bangle', 'CELL217', 15),
# MAGIC         ('CI-00004', 'bangle', 'CELL211', 16),
# MAGIC         ('CI-00004', 'bangle', 'CELL212', 17),
# MAGIC         ('CI-00004', 'bangle', 'CELL213', 18),
# MAGIC         ('CI-00004', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00004', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00004', 'bangle-lock', 'CELL210', 3),
# MAGIC         ('CI-00004', 'bangle-lock', 'CELL208', 4),
# MAGIC         -- SA SEZANE
# MAGIC         ('CI-00008', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00008', 'silver', 'CELL214', 1),
# MAGIC         ('CI-00008', 'silver', 'CELL215', 2),
# MAGIC         ('CI-00008', 'silver', 'CELL216', 3),
# MAGIC         ('CI-00008', 'silver', 'CELL217', 4),
# MAGIC         ('CI-00008', 'silver', 'CELL201', 5),
# MAGIC         ('CI-00008', 'silver', 'CELL202', 6),
# MAGIC         ('CI-00008', 'silver', 'CELL203', 7),
# MAGIC         ('CI-00008', 'silver', 'CELL204', 8),
# MAGIC         ('CI-00008', 'silver', 'CELL205', 9),
# MAGIC         ('CI-00008', 'silver', 'CELL211', 10),
# MAGIC         ('CI-00008', 'silver', 'CELL212', 11),
# MAGIC         ('CI-00008', 'silver', 'CELL213', 12),
# MAGIC         ('CI-00008', 'silver', 'CELL206', 13),
# MAGIC         ('CI-00008', 'silver', 'CELL207', 14),
# MAGIC         ('CI-00008', 'silver', 'CELL208', 15),
# MAGIC         ('CI-00008', 'silver', 'CELL209', 16),
# MAGIC         ('CI-00008', 'silver', 'CELL210', 17),
# MAGIC         ('CI-00008', 'silver', 'CELL219', 18),
# MAGIC         ('CI-00008', 'silver', 'CELL109', 19),
# MAGIC         ('CI-00008', 'brass', 'CELL214', 1),
# MAGIC         ('CI-00008', 'brass', 'CELL215', 2),
# MAGIC         ('CI-00008', 'brass', 'CELL216', 3),
# MAGIC         ('CI-00008', 'brass', 'CELL217', 4),
# MAGIC         ('CI-00008', 'brass', 'CELL201', 5),
# MAGIC         ('CI-00008', 'brass', 'CELL202', 6),
# MAGIC         ('CI-00008', 'brass', 'CELL203', 7),
# MAGIC         ('CI-00008', 'brass', 'CELL204', 8),
# MAGIC         ('CI-00008', 'brass', 'CELL205', 9),
# MAGIC         ('CI-00008', 'brass', 'CELL211', 10),
# MAGIC         ('CI-00008', 'brass', 'CELL212', 11),
# MAGIC         ('CI-00008', 'brass', 'CELL213', 12),
# MAGIC         ('CI-00008', 'brass', 'CELL206', 13),
# MAGIC         ('CI-00008', 'brass', 'CELL207', 14),
# MAGIC         ('CI-00008', 'brass', 'CELL208', 15),
# MAGIC         ('CI-00008', 'brass', 'CELL209', 16),
# MAGIC         ('CI-00008', 'brass', 'CELL210', 17),
# MAGIC         ('CI-00008', 'brass', 'CELL219', 18),
# MAGIC         ('CI-00008', 'bangle', 'CELL214', 1),
# MAGIC         ('CI-00008', 'bangle', 'CELL215', 2),
# MAGIC         ('CI-00008', 'bangle', 'CELL216', 3),
# MAGIC         ('CI-00008', 'bangle', 'CELL217', 4),
# MAGIC         ('CI-00008', 'bangle', 'CELL201', 5),
# MAGIC         ('CI-00008', 'bangle', 'CELL202', 6),
# MAGIC         ('CI-00008', 'bangle', 'CELL203', 7),
# MAGIC         ('CI-00008', 'bangle', 'CELL204', 8),
# MAGIC         ('CI-00008', 'bangle', 'CELL205', 9),
# MAGIC         ('CI-00008', 'bangle', 'CELL103', 10),
# MAGIC         ('CI-00008', 'bangle', 'CELL211', 11),
# MAGIC         ('CI-00008', 'bangle', 'CELL212', 12),
# MAGIC         ('CI-00008', 'bangle', 'CELL213', 13),
# MAGIC         ('CI-00008', 'bangle', 'CELL206', 14),
# MAGIC         ('CI-00008', 'bangle', 'CELL207', 15),
# MAGIC         ('CI-00008', 'bangle', 'CELL208', 16),
# MAGIC         ('CI-00008', 'bangle', 'CELL209', 17),
# MAGIC         ('CI-00008', 'bangle', 'CELL210', 18),
# MAGIC         ('CI-00008', 'bangle', 'CELL109', 19),
# MAGIC         ('CI-00008', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00008', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00008', 'bangle-lock', 'CELL210', 3),
# MAGIC         -- KIMAI LTD
# MAGIC         ('CI-00009', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00009', 'bangle', 'CELL206', 1),
# MAGIC         ('CI-00009', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00009', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00009', 'bangle-lock', 'CELL210', 3),
# MAGIC         -- BTB b.v.
# MAGIC         ('CI-00013', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00013', 'silver', 'CELL208', 1),
# MAGIC         ('CI-00013', 'silver', 'CELL207', 2),
# MAGIC         ('CI-00013', 'silver', 'CELL209', 3),
# MAGIC         ('CI-00013', 'silver', 'CELL210', 4),
# MAGIC         ('CI-00013', 'silver', 'CELL219', 5),
# MAGIC         ('CI-00013', 'silver', 'CELL211', 6),
# MAGIC         ('CI-00013', 'silver', 'CELL212', 7),
# MAGIC         ('CI-00013', 'silver', 'CELL213', 8),
# MAGIC         ('CI-00013', 'silver', 'CELL214', 9),
# MAGIC         ('CI-00013', 'silver', 'CELL215', 10),
# MAGIC         ('CI-00013', 'silver', 'CELL216', 11),
# MAGIC         ('CI-00013', 'silver', 'CELL217', 12),
# MAGIC         ('CI-00013', 'bangle', 'CELL206', 1),
# MAGIC         ('CI-00013', 'bangle', 'CELL207', 2),
# MAGIC         ('CI-00013', 'bangle', 'CELL208', 3),
# MAGIC         ('CI-00013', 'bangle', 'CELL209', 4),
# MAGIC         ('CI-00013', 'bangle', 'CELL210', 5),
# MAGIC         ('CI-00013', 'bangle', 'CELL201', 6),
# MAGIC         ('CI-00013', 'bangle', 'CELL202', 7),
# MAGIC         ('CI-00013', 'bangle', 'CELL203', 8),
# MAGIC         ('CI-00013', 'bangle', 'CELL204', 9),
# MAGIC         ('CI-00013', 'bangle', 'CELL109', 10),
# MAGIC         ('CI-00013', 'bangle', 'CELL214', 11),
# MAGIC         ('CI-00013', 'bangle', 'CELL215', 12),
# MAGIC         ('CI-00013', 'bangle', 'CELL216', 13),
# MAGIC         ('CI-00013', 'bangle', 'CELL217', 14),
# MAGIC         ('CI-00013', 'bangle', 'CELL211', 15),
# MAGIC         ('CI-00013', 'bangle', 'CELL212', 16),
# MAGIC         ('CI-00013', 'bangle', 'CELL213', 17),
# MAGIC         ('CI-00013', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00013', 'bangle-lock', 'CELL208', 2),
# MAGIC         ('CI-00013', 'bangle-lock', 'CELL209', 3),
# MAGIC         ('CI-00013', 'bangle-lock', 'CELL210', 4),
# MAGIC         -- CLOCKS & COLOURS LTD
# MAGIC         ('CI-00016', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00016', 'silver', 'CELL211', 1),
# MAGIC         ('CI-00016', 'silver', 'CELL212', 2),
# MAGIC         ('CI-00016', 'silver', 'CELL213', 3),
# MAGIC         ('CI-00016', 'silver', 'CELL219', 4),
# MAGIC         ('CI-00016', 'bangle', 'CELL211', 1),
# MAGIC         ('CI-00016', 'bangle', 'CELL212', 2),
# MAGIC         ('CI-00016', 'bangle', 'CELL213', 3),
# MAGIC         ('CI-00016', 'bangle', 'CELL206', 4),
# MAGIC         ('CI-00016', 'bangle', 'CELL207', 5),
# MAGIC         ('CI-00016', 'bangle', 'CELL208', 6),
# MAGIC         ('CI-00016', 'bangle', 'CELL209', 7),
# MAGIC         ('CI-00016', 'bangle', 'CELL210', 8),
# MAGIC         ('CI-00016', 'bangle', 'CELL214', 9),
# MAGIC         ('CI-00016', 'bangle', 'CELL215', 10),
# MAGIC         ('CI-00016', 'bangle', 'CELL216', 11),
# MAGIC         ('CI-00016', 'bangle', 'CELL217', 12),
# MAGIC         ('CI-00016', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00016', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00016', 'bangle-lock', 'CELL210', 3),
# MAGIC         -- DHTG LTD
# MAGIC         ('CI-00018', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00018', 'silver', 'CELL214', 1),
# MAGIC         ('CI-00018', 'silver', 'CELL211', 2),
# MAGIC         ('CI-00018', 'silver', 'CELL212', 3),
# MAGIC         ('CI-00018', 'silver', 'CELL213', 4),
# MAGIC         ('CI-00018', 'silver', 'CELL219', 5),
# MAGIC         ('CI-00018', 'bangle', 'CELL214', 1),
# MAGIC         ('CI-00018', 'bangle', 'CELL215', 2),
# MAGIC         ('CI-00018', 'bangle', 'CELL216', 3),
# MAGIC         ('CI-00018', 'bangle', 'CELL217', 4),
# MAGIC         ('CI-00018', 'bangle', 'CELL201', 5),
# MAGIC         ('CI-00018', 'bangle', 'CELL202', 6),
# MAGIC         ('CI-00018', 'bangle', 'CELL203', 7),
# MAGIC         ('CI-00018', 'bangle', 'CELL204', 8),
# MAGIC         ('CI-00018', 'bangle', 'CELL205', 9),
# MAGIC         ('CI-00018', 'bangle', 'CELL109', 10),
# MAGIC         ('CI-00018', 'bangle', 'CELL211', 11),
# MAGIC         ('CI-00018', 'bangle', 'CELL212', 12),
# MAGIC         ('CI-00018', 'bangle', 'CELL213', 13),
# MAGIC         ('CI-00018', 'bangle', 'CELL206', 14),
# MAGIC         ('CI-00018', 'bangle', 'CELL207', 15),
# MAGIC         ('CI-00018', 'bangle', 'CELL208', 16),
# MAGIC         ('CI-00018', 'bangle', 'CELL209', 17),
# MAGIC         ('CI-00018', 'bangle', 'CELL210', 18),
# MAGIC         ('CI-00018', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00018', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00018', 'bangle-lock', 'CELL210', 3),
# MAGIC         -- MISSOMA LTD
# MAGIC         ('CI-00020', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00020', 'silver', 'CELL214', 1),
# MAGIC         ('CI-00020', 'silver', 'CELL215', 2),
# MAGIC         ('CI-00020', 'silver', 'CELL216', 3),
# MAGIC         ('CI-00020', 'silver', 'CELL217', 4),
# MAGIC         ('CI-00020', 'silver', 'CELL211', 5),
# MAGIC         ('CI-00020', 'silver', 'CELL212', 6),
# MAGIC         ('CI-00020', 'silver', 'CELL213', 7),
# MAGIC         ('CI-00020', 'silver', 'CELL201', 8),
# MAGIC         ('CI-00020', 'silver', 'CELL202', 9),
# MAGIC         ('CI-00020', 'silver', 'CELL203', 10),
# MAGIC         ('CI-00020', 'silver', 'CELL204', 11),
# MAGIC         ('CI-00020', 'silver', 'CELL205', 12),
# MAGIC         ('CI-00020', 'silver', 'CELL206', 13),
# MAGIC         ('CI-00020', 'silver', 'CELL207', 14),
# MAGIC         ('CI-00020', 'silver', 'CELL208', 15),
# MAGIC         ('CI-00020', 'silver', 'CELL209', 16),
# MAGIC         ('CI-00020', 'silver', 'CELL210', 17),
# MAGIC         ('CI-00020', 'silver', 'CELL219', 18),
# MAGIC         ('CI-00020', 'brass', 'CELL214', 1),
# MAGIC         ('CI-00020', 'brass', 'CELL215', 2),
# MAGIC         ('CI-00020', 'brass', 'CELL216', 3),
# MAGIC         ('CI-00020', 'brass', 'CELL217', 4),
# MAGIC         ('CI-00020', 'brass', 'CELL211', 5),
# MAGIC         ('CI-00020', 'brass', 'CELL212', 6),
# MAGIC         ('CI-00020', 'brass', 'CELL213', 7),
# MAGIC         ('CI-00020', 'brass', 'CELL201', 8),
# MAGIC         ('CI-00020', 'brass', 'CELL202', 9),
# MAGIC         ('CI-00020', 'brass', 'CELL203', 10),
# MAGIC         ('CI-00020', 'brass', 'CELL204', 11),
# MAGIC         ('CI-00020', 'brass', 'CELL205', 12),
# MAGIC         ('CI-00020', 'brass', 'CELL206', 13),
# MAGIC         ('CI-00020', 'brass', 'CELL207', 14),
# MAGIC         ('CI-00020', 'brass', 'CELL208', 15),
# MAGIC         ('CI-00020', 'brass', 'CELL209', 16),
# MAGIC         ('CI-00020', 'brass', 'CELL210', 17),
# MAGIC         ('CI-00020', 'brass', 'CELL219', 18),
# MAGIC         ('CI-00020', 'bangle', 'CELL214', 1),
# MAGIC         ('CI-00020', 'bangle', 'CELL215', 2),
# MAGIC         ('CI-00020', 'bangle', 'CELL216', 3),
# MAGIC         ('CI-00020', 'bangle', 'CELL217', 4),
# MAGIC         ('CI-00020', 'bangle', 'CELL201', 5),
# MAGIC         ('CI-00020', 'bangle', 'CELL202', 6),
# MAGIC         ('CI-00020', 'bangle', 'CELL203', 7),
# MAGIC         ('CI-00020', 'bangle', 'CELL204', 8),
# MAGIC         ('CI-00020', 'bangle', 'CELL205', 9),
# MAGIC         ('CI-00020', 'bangle', 'CELL103', 10),
# MAGIC         ('CI-00020', 'bangle', 'CELL211', 11),
# MAGIC         ('CI-00020', 'bangle', 'CELL212', 12),
# MAGIC         ('CI-00020', 'bangle', 'CELL213', 13),
# MAGIC         ('CI-00020', 'bangle', 'CELL206', 14),
# MAGIC         ('CI-00020', 'bangle', 'CELL207', 15),
# MAGIC         ('CI-00020', 'bangle', 'CELL208', 16),
# MAGIC         ('CI-00020', 'bangle', 'CELL209', 17),
# MAGIC         ('CI-00020', 'bangle', 'CELL210', 18),
# MAGIC         ('CI-00020', 'bangle', 'CELL109', 19),
# MAGIC         ('CI-00020', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00020', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00020', 'bangle-lock', 'CELL210', 3),
# MAGIC         ('CI-00020', 'bangle-lock', 'CELL208', 4),
# MAGIC         ('CI-00020', 'bangle-lock', 'CELL205', 5),
# MAGIC         ('CI-00020', 'bangle-lock', 'CELL217', 6),
# MAGIC         -- MONICA VINADER LTD
# MAGIC         ('CI-00022', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00022', 'silver', 'CELL205', 1),
# MAGIC         ('CI-00022', 'silver', 'CELL201', 2),
# MAGIC         ('CI-00022', 'silver', 'CELL202', 3),
# MAGIC         ('CI-00022', 'silver', 'CELL203', 4),
# MAGIC         ('CI-00022', 'silver', 'CELL204', 5),
# MAGIC         ('CI-00022', 'silver', 'CELL214', 6),
# MAGIC         ('CI-00022', 'silver', 'CELL215', 7),
# MAGIC         ('CI-00022', 'silver', 'CELL216', 8),
# MAGIC         ('CI-00022', 'silver', 'CELL217', 9),
# MAGIC         ('CI-00022', 'silver', 'CELL211', 10),
# MAGIC         ('CI-00022', 'silver', 'CELL212', 11),
# MAGIC         ('CI-00022', 'silver', 'CELL213', 12),
# MAGIC         ('CI-00022', 'silver', 'CELL206', 13),
# MAGIC         ('CI-00022', 'silver', 'CELL207', 14),
# MAGIC         ('CI-00022', 'silver', 'CELL208', 15),
# MAGIC         ('CI-00022', 'silver', 'CELL209', 16),
# MAGIC         ('CI-00022', 'silver', 'CELL210', 17),
# MAGIC         ('CI-00022', 'silver', 'CELL219', 18),
# MAGIC         ('CI-00022', 'silver', 'CELL109', 19),
# MAGIC         ('CI-00022', 'bangle', 'CELL201', 1),
# MAGIC         ('CI-00022', 'bangle', 'CELL202', 2),
# MAGIC         ('CI-00022', 'bangle', 'CELL203', 3),
# MAGIC         ('CI-00022', 'bangle', 'CELL204', 4),
# MAGIC         ('CI-00022', 'bangle', 'CELL205', 5),
# MAGIC         ('CI-00022', 'bangle', 'CELL214', 6),
# MAGIC         ('CI-00022', 'bangle', 'CELL215', 7),
# MAGIC         ('CI-00022', 'bangle', 'CELL216', 8),
# MAGIC         ('CI-00022', 'bangle', 'CELL217', 9),
# MAGIC         ('CI-00022', 'bangle', 'CELL103', 10),
# MAGIC         ('CI-00022', 'bangle', 'CELL109', 11),
# MAGIC         ('CI-00022', 'bangle', 'CELL211', 12),
# MAGIC         ('CI-00022', 'bangle', 'CELL212', 13),
# MAGIC         ('CI-00022', 'bangle', 'CELL213', 14),
# MAGIC         ('CI-00022', 'bangle', 'CELL206', 15),
# MAGIC         ('CI-00022', 'bangle', 'CELL207', 16),
# MAGIC         ('CI-00022', 'bangle', 'CELL208', 17),
# MAGIC         ('CI-00022', 'bangle', 'CELL209', 18),
# MAGIC         ('CI-00022', 'bangle', 'CELL210', 19),
# MAGIC         ('CI-00022', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00022', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00022', 'bangle-lock', 'CELL210', 3),
# MAGIC         ('CI-00022', 'bangle-lock', 'CELL211', 4),
# MAGIC         -- THE GREAT FROG
# MAGIC         ('CI-00027', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00027', 'silver', 'CELL211', 1),
# MAGIC         ('CI-00027', 'silver', 'CELL212', 2),
# MAGIC         ('CI-00027', 'silver', 'CELL213', 3),
# MAGIC         ('CI-00027', 'silver', 'CELL214', 4),
# MAGIC         ('CI-00027', 'silver', 'CELL215', 5),
# MAGIC         ('CI-00027', 'silver', 'CELL216', 6),
# MAGIC         ('CI-00027', 'silver', 'CELL217', 7),
# MAGIC         ('CI-00027', 'silver', 'CELL206', 8),
# MAGIC         ('CI-00027', 'silver', 'CELL207', 9),
# MAGIC         ('CI-00027', 'silver', 'CELL208', 10),
# MAGIC         ('CI-00027', 'silver', 'CELL209', 11),
# MAGIC         ('CI-00027', 'silver', 'CELL210', 12),
# MAGIC         ('CI-00027', 'silver', 'CELL219', 13),
# MAGIC         ('CI-00027', 'silver', 'CELL109', 14),
# MAGIC         ('CI-00027', 'bangle', 'CELL211', 1),
# MAGIC         ('CI-00027', 'bangle', 'CELL212', 2),
# MAGIC         ('CI-00027', 'bangle', 'CELL213', 3),
# MAGIC         ('CI-00027', 'bangle', 'CELL214', 4),
# MAGIC         ('CI-00027', 'bangle', 'CELL215', 5),
# MAGIC         ('CI-00027', 'bangle', 'CELL216', 6),
# MAGIC         ('CI-00027', 'bangle', 'CELL217', 7),
# MAGIC         ('CI-00027', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00027', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00027', 'bangle-lock', 'CELL210', 3),
# MAGIC         -- ASTRID & MIYU
# MAGIC         ('CI-00029', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00029', 'silver', 'CELL211', 1),
# MAGIC         ('CI-00029', 'silver', 'CELL212', 2),
# MAGIC         ('CI-00029', 'silver', 'CELL213', 3),
# MAGIC         ('CI-00029', 'silver', 'CELL214', 4),
# MAGIC         ('CI-00029', 'silver', 'CELL215', 5),
# MAGIC         ('CI-00029', 'silver', 'CELL216', 6),
# MAGIC         ('CI-00029', 'silver', 'CELL217', 7),
# MAGIC         ('CI-00029', 'silver', 'CELL219', 8),
# MAGIC         ('CI-00029', 'silver', 'CELL206', 9),
# MAGIC         ('CI-00029', 'silver', 'CELL210', 10),
# MAGIC         ('CI-00029', 'silver', 'CELL207', 11),
# MAGIC         ('CI-00029', 'silver', 'CELL208', 12),
# MAGIC         ('CI-00029', 'silver', 'CELL209', 13),
# MAGIC         ('CI-00029', 'silver', 'CELL201', 14),
# MAGIC         ('CI-00029', 'silver', 'CELL202', 15),
# MAGIC         ('CI-00029', 'silver', 'CELL203', 16),
# MAGIC         ('CI-00029', 'silver', 'CELL204', 17),
# MAGIC         ('CI-00029', 'silver', 'CELL205', 18),
# MAGIC         ('CI-00029', 'silver', 'CELL103', 19),
# MAGIC         ('CI-00029', 'silver', 'CELL109', 20),
# MAGIC         ('CI-00029', 'bangle', 'CELL211', 1),
# MAGIC         ('CI-00029', 'bangle', 'CELL212', 2),
# MAGIC         ('CI-00029', 'bangle', 'CELL213', 3),
# MAGIC         ('CI-00029', 'bangle', 'CELL214', 4),
# MAGIC         ('CI-00029', 'bangle', 'CELL215', 5),
# MAGIC         ('CI-00029', 'bangle', 'CELL216', 6),
# MAGIC         ('CI-00029', 'bangle', 'CELL217', 7),
# MAGIC         ('CI-00029', 'bangle', 'CELL201', 8),
# MAGIC         ('CI-00029', 'bangle', 'CELL202', 9),
# MAGIC         ('CI-00029', 'bangle', 'CELL203', 10),
# MAGIC         ('CI-00029', 'bangle', 'CELL204', 11),
# MAGIC         ('CI-00029', 'bangle', 'CELL205', 12),
# MAGIC         ('CI-00029', 'bangle', 'CELL103', 13),
# MAGIC         ('CI-00029', 'bangle', 'CELL109', 14),
# MAGIC         ('CI-00029', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00029', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00029', 'bangle-lock', 'CELL210', 3),
# MAGIC         ('CI-00029', 'bangle-lock', 'CELL208', 4),
# MAGIC         -- SEPAJATI LIMITED
# MAGIC         ('CI-00039', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00039', 'silver', 'CELL201', 1),
# MAGIC         ('CI-00039', 'silver', 'CELL202', 2),
# MAGIC         ('CI-00039', 'silver', 'CELL203', 3),
# MAGIC         ('CI-00039', 'silver', 'CELL204', 4),
# MAGIC         ('CI-00039', 'silver', 'CELL205', 5),
# MAGIC         ('CI-00039', 'silver', 'CELL214', 6),
# MAGIC         ('CI-00039', 'silver', 'CELL215', 7),
# MAGIC         ('CI-00039', 'silver', 'CELL216', 8),
# MAGIC         ('CI-00039', 'silver', 'CELL217', 9),
# MAGIC         ('CI-00039', 'silver', 'CELL211', 10),
# MAGIC         ('CI-00039', 'silver', 'CELL212', 11),
# MAGIC         ('CI-00039', 'silver', 'CELL213', 12),
# MAGIC         ('CI-00039', 'silver', 'CELL206', 13),
# MAGIC         ('CI-00039', 'silver', 'CELL207', 14),
# MAGIC         ('CI-00039', 'silver', 'CELL208', 15),
# MAGIC         ('CI-00039', 'silver', 'CELL209', 16),
# MAGIC         ('CI-00039', 'silver', 'CELL210', 17),
# MAGIC         ('CI-00039', 'silver', 'CELL219', 18),
# MAGIC         ('CI-00039', 'silver', 'CELL109', 19),
# MAGIC         ('CI-00039', 'bangle', 'CELL201', 1),
# MAGIC         ('CI-00039', 'bangle', 'CELL202', 2),
# MAGIC         ('CI-00039', 'bangle', 'CELL203', 3),
# MAGIC         ('CI-00039', 'bangle', 'CELL204', 4),
# MAGIC         ('CI-00039', 'bangle', 'CELL205', 5),
# MAGIC         ('CI-00039', 'bangle', 'CELL214', 6),
# MAGIC         ('CI-00039', 'bangle', 'CELL215', 7),
# MAGIC         ('CI-00039', 'bangle', 'CELL216', 8),
# MAGIC         ('CI-00039', 'bangle', 'CELL217', 9),
# MAGIC         ('CI-00039', 'bangle', 'CELL103', 10),
# MAGIC         ('CI-00039', 'bangle', 'CELL109', 11),
# MAGIC         ('CI-00039', 'bangle', 'CELL211', 12),
# MAGIC         ('CI-00039', 'bangle', 'CELL212', 13),
# MAGIC         ('CI-00039', 'bangle', 'CELL213', 14),
# MAGIC         ('CI-00039', 'bangle', 'CELL206', 15),
# MAGIC         ('CI-00039', 'bangle', 'CELL207', 16),
# MAGIC         ('CI-00039', 'bangle', 'CELL208', 17),
# MAGIC         ('CI-00039', 'bangle', 'CELL209', 18),
# MAGIC         ('CI-00039', 'bangle', 'CELL210', 19),
# MAGIC         ('CI-00039', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00039', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00039', 'bangle-lock', 'CELL210', 3),
# MAGIC         -- VIVIENNE WESTWOOD JEWELLERY
# MAGIC         ('CI-00040', 'gold', 'CELL109', 1),
# MAGIC         ('CI-00040', 'silver', 'CELL211', 1),
# MAGIC         ('CI-00040', 'silver', 'CELL212', 2),
# MAGIC         ('CI-00040', 'silver', 'CELL213', 3),
# MAGIC         ('CI-00040', 'silver', 'CELL214', 4),
# MAGIC         ('CI-00040', 'silver', 'CELL215', 5),
# MAGIC         ('CI-00040', 'silver', 'CELL216', 6),
# MAGIC         ('CI-00040', 'silver', 'CELL217', 7),
# MAGIC         ('CI-00040', 'silver', 'CELL219', 8),
# MAGIC         ('CI-00040', 'silver', 'CELL206', 9),
# MAGIC         ('CI-00040', 'silver', 'CELL207', 10),
# MAGIC         ('CI-00040', 'silver', 'CELL208', 11),
# MAGIC         ('CI-00040', 'silver', 'CELL209', 12),
# MAGIC         ('CI-00040', 'silver', 'CELL210', 13),
# MAGIC         ('CI-00040', 'silver', 'CELL201', 14),
# MAGIC         ('CI-00040', 'silver', 'CELL202', 15),
# MAGIC         ('CI-00040', 'silver', 'CELL203', 16),
# MAGIC         ('CI-00040', 'silver', 'CELL204', 17),
# MAGIC         ('CI-00040', 'silver', 'CELL205', 18),
# MAGIC         ('CI-00040', 'silver', 'CELL103', 19),
# MAGIC         ('CI-00040', 'silver', 'CELL109', 20),
# MAGIC         ('CI-00040', 'brass', 'CELL211', 1),
# MAGIC         ('CI-00040', 'brass', 'CELL212', 2),
# MAGIC         ('CI-00040', 'brass', 'CELL213', 3),
# MAGIC         ('CI-00040', 'brass', 'CELL214', 4),
# MAGIC         ('CI-00040', 'brass', 'CELL215', 5),
# MAGIC         ('CI-00040', 'brass', 'CELL216', 6),
# MAGIC         ('CI-00040', 'brass', 'CELL217', 7),
# MAGIC         ('CI-00040', 'brass', 'CELL219', 8),
# MAGIC         ('CI-00040', 'brass', 'CELL206', 9),
# MAGIC         ('CI-00040', 'brass', 'CELL207', 10),
# MAGIC         ('CI-00040', 'brass', 'CELL208', 11),
# MAGIC         ('CI-00040', 'brass', 'CELL209', 12),
# MAGIC         ('CI-00040', 'brass', 'CELL210', 13),
# MAGIC         ('CI-00040', 'brass', 'CELL201', 14),
# MAGIC         ('CI-00040', 'brass', 'CELL202', 15),
# MAGIC         ('CI-00040', 'brass', 'CELL203', 16),
# MAGIC         ('CI-00040', 'brass', 'CELL204', 17),
# MAGIC         ('CI-00040', 'brass', 'CELL205', 18),
# MAGIC         ('CI-00040', 'bangle', 'CELL206', 1),
# MAGIC         ('CI-00040', 'bangle', 'CELL207', 2),
# MAGIC         ('CI-00040', 'bangle', 'CELL208', 3),
# MAGIC         ('CI-00040', 'bangle', 'CELL209', 4),
# MAGIC         ('CI-00040', 'bangle', 'CELL210', 5),
# MAGIC         ('CI-00040', 'bangle', 'CELL201', 6),
# MAGIC         ('CI-00040', 'bangle', 'CELL202', 7),
# MAGIC         ('CI-00040', 'bangle', 'CELL203', 8),
# MAGIC         ('CI-00040', 'bangle', 'CELL204', 9),
# MAGIC         ('CI-00040', 'bangle', 'CELL205', 10),
# MAGIC         ('CI-00040', 'bangle', 'CELL103', 11),
# MAGIC         ('CI-00040', 'bangle', 'CELL109', 12),
# MAGIC         ('CI-00040', 'bangle', 'CELL214', 13),
# MAGIC         ('CI-00040', 'bangle', 'CELL215', 14),
# MAGIC         ('CI-00040', 'bangle', 'CELL216', 15),
# MAGIC         ('CI-00040', 'bangle', 'CELL217', 16),
# MAGIC         ('CI-00040', 'bangle', 'CELL211', 17),
# MAGIC         ('CI-00040', 'bangle', 'CELL212', 18),
# MAGIC         ('CI-00040', 'bangle', 'CELL213', 19),
# MAGIC         ('CI-00040', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00040', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00040', 'bangle-lock', 'CELL210', 3),
# MAGIC         -- ASPINAL OF LONDON
# MAGIC         ('CI-00042', 'silver', 'CELL203', 1),
# MAGIC         ('CI-00042', 'silver', 'CELL201', 2),
# MAGIC         ('CI-00042', 'silver', 'CELL204', 3),
# MAGIC         ('CI-00042', 'silver', 'CELL205', 4),
# MAGIC         ('CI-00042', 'silver', 'CELL211', 5),
# MAGIC         ('CI-00042', 'silver', 'CELL212', 6),
# MAGIC         ('CI-00042', 'silver', 'CELL213', 7),
# MAGIC         ('CI-00042', 'silver', 'CELL214', 8),
# MAGIC         ('CI-00042', 'silver', 'CELL215', 9),
# MAGIC         ('CI-00042', 'silver', 'CELL216', 10),
# MAGIC         ('CI-00042', 'silver', 'CELL217', 11),
# MAGIC         ('CI-00042', 'silver', 'CELL219', 12),
# MAGIC         ('CI-00042', 'silver', 'CELL206', 13),
# MAGIC         ('CI-00042', 'silver', 'CELL208', 14),
# MAGIC         ('CI-00042', 'silver', 'CELL210', 15),
# MAGIC         ('CI-00042', 'silver', 'CELL209', 16),
# MAGIC         ('CI-00042', 'silver', 'CELL207', 17),
# MAGIC         ('CI-00042', 'silver', 'CELL103', 18),
# MAGIC         ('CI-00042', 'silver', 'CELL109', 19),
# MAGIC         ('CI-00042', 'bangle', 'CELL211', 1),
# MAGIC         ('CI-00042', 'bangle', 'CELL212', 2),
# MAGIC         ('CI-00042', 'bangle', 'CELL213', 3),
# MAGIC         ('CI-00042', 'bangle', 'CELL214', 4),
# MAGIC         ('CI-00042', 'bangle', 'CELL215', 5),
# MAGIC         ('CI-00042', 'bangle', 'CELL216', 6),
# MAGIC         ('CI-00042', 'bangle', 'CELL217', 7),
# MAGIC         ('CI-00042', 'bangle', 'CELL201', 8),
# MAGIC         ('CI-00042', 'bangle', 'CELL202', 9),
# MAGIC         ('CI-00042', 'bangle', 'CELL203', 10),
# MAGIC         ('CI-00042', 'bangle', 'CELL204', 11),
# MAGIC         ('CI-00042', 'bangle', 'CELL205', 12),
# MAGIC         ('CI-00042', 'bangle', 'CELL103', 13),
# MAGIC         ('CI-00042', 'bangle', 'CELL109', 14),
# MAGIC         ('CI-00042', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00042', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00042', 'bangle-lock', 'CELL210', 3),
# MAGIC         ('CI-00042', 'bangle-lock', 'CELL208', 4),
# MAGIC         -- RAG&BONE
# MAGIC         ('CI-00044', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00044', 'silver', 'CELL205', 1),
# MAGIC         ('CI-00044', 'silver', 'CELL201', 2),
# MAGIC         ('CI-00044', 'silver', 'CELL202', 3),
# MAGIC         ('CI-00044', 'silver', 'CELL203', 4),
# MAGIC         ('CI-00044', 'silver', 'CELL204', 5),
# MAGIC         ('CI-00044', 'silver', 'CELL214', 6),
# MAGIC         ('CI-00044', 'silver', 'CELL215', 7),
# MAGIC         ('CI-00044', 'silver', 'CELL216', 8),
# MAGIC         ('CI-00044', 'silver', 'CELL217', 9),
# MAGIC         ('CI-00044', 'silver', 'CELL211', 10),
# MAGIC         ('CI-00044', 'silver', 'CELL212', 11),
# MAGIC         ('CI-00044', 'silver', 'CELL213', 12),
# MAGIC         ('CI-00044', 'silver', 'CELL206', 13),
# MAGIC         ('CI-00044', 'silver', 'CELL207', 14),
# MAGIC         ('CI-00044', 'silver', 'CELL208', 15),
# MAGIC         ('CI-00044', 'silver', 'CELL209', 16),
# MAGIC         ('CI-00044', 'silver', 'CELL210', 17),
# MAGIC         ('CI-00044', 'silver', 'CELL219', 18),
# MAGIC         ('CI-00044', 'silver', 'CELL109', 19),
# MAGIC         ('CI-00044', 'bangle', 'CELL201', 1),
# MAGIC         ('CI-00044', 'bangle', 'CELL202', 2),
# MAGIC         ('CI-00044', 'bangle', 'CELL203', 3),
# MAGIC         ('CI-00044', 'bangle', 'CELL204', 4),
# MAGIC         ('CI-00044', 'bangle', 'CELL205', 5),
# MAGIC         ('CI-00044', 'bangle', 'CELL214', 6),
# MAGIC         ('CI-00044', 'bangle', 'CELL215', 7),
# MAGIC         ('CI-00044', 'bangle', 'CELL216', 8),
# MAGIC         ('CI-00044', 'bangle', 'CELL217', 9),
# MAGIC         ('CI-00044', 'bangle', 'CELL103', 10),
# MAGIC         ('CI-00044', 'bangle', 'CELL109', 11),
# MAGIC         ('CI-00044', 'bangle', 'CELL211', 12),
# MAGIC         ('CI-00044', 'bangle', 'CELL212', 13),
# MAGIC         ('CI-00044', 'bangle', 'CELL213', 14),
# MAGIC         ('CI-00044', 'bangle', 'CELL206', 15),
# MAGIC         ('CI-00044', 'bangle', 'CELL207', 16),
# MAGIC         ('CI-00044', 'bangle', 'CELL208', 17),
# MAGIC         ('CI-00044', 'bangle', 'CELL209', 18),
# MAGIC         ('CI-00044', 'bangle', 'CELL210', 19),
# MAGIC         ('CI-00044', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00044', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00044', 'bangle-lock', 'CELL210', 3),
# MAGIC         -- AGATHA PARIS
# MAGIC         ('CI-00047', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00047', 'silver', 'CELL201', 1),
# MAGIC         ('CI-00047', 'silver', 'CELL202', 2),
# MAGIC         ('CI-00047', 'silver', 'CELL203', 3),
# MAGIC         ('CI-00047', 'silver', 'CELL204', 4),
# MAGIC         ('CI-00047', 'silver', 'CELL205', 5),
# MAGIC         ('CI-00047', 'silver', 'CELL214', 6),
# MAGIC         ('CI-00047', 'silver', 'CELL215', 7),
# MAGIC         ('CI-00047', 'silver', 'CELL216', 8),
# MAGIC         ('CI-00047', 'silver', 'CELL217', 9),
# MAGIC         ('CI-00047', 'silver', 'CELL211', 10),
# MAGIC         ('CI-00047', 'silver', 'CELL212', 11),
# MAGIC         ('CI-00047', 'silver', 'CELL213', 12),
# MAGIC         ('CI-00047', 'silver', 'CELL206', 13),
# MAGIC         ('CI-00047', 'silver', 'CELL207', 14),
# MAGIC         ('CI-00047', 'silver', 'CELL208', 15),
# MAGIC         ('CI-00047', 'silver', 'CELL209', 16),
# MAGIC         ('CI-00047', 'silver', 'CELL210', 17),
# MAGIC         ('CI-00047', 'silver', 'CELL219', 18),
# MAGIC         ('CI-00047', 'silver', 'CELL109', 19),
# MAGIC         ('CI-00047', 'brass', 'CELL201', 1),
# MAGIC         ('CI-00047', 'brass', 'CELL202', 2),
# MAGIC         ('CI-00047', 'brass', 'CELL203', 3),
# MAGIC         ('CI-00047', 'brass', 'CELL204', 4),
# MAGIC         ('CI-00047', 'brass', 'CELL205', 5),
# MAGIC         ('CI-00047', 'brass', 'CELL214', 6),
# MAGIC         ('CI-00047', 'brass', 'CELL215', 7),
# MAGIC         ('CI-00047', 'brass', 'CELL216', 8),
# MAGIC         ('CI-00047', 'brass', 'CELL217', 9),
# MAGIC         ('CI-00047', 'brass', 'CELL211', 10),
# MAGIC         ('CI-00047', 'brass', 'CELL212', 11),
# MAGIC         ('CI-00047', 'brass', 'CELL213', 12),
# MAGIC         ('CI-00047', 'brass', 'CELL206', 13),
# MAGIC         ('CI-00047', 'brass', 'CELL207', 14),
# MAGIC         ('CI-00047', 'brass', 'CELL208', 15),
# MAGIC         ('CI-00047', 'brass', 'CELL209', 16),
# MAGIC         ('CI-00047', 'brass', 'CELL210', 17),
# MAGIC         ('CI-00047', 'brass', 'CELL219', 18),
# MAGIC         ('CI-00047', 'bangle', 'CELL201', 1),
# MAGIC         ('CI-00047', 'bangle', 'CELL202', 2),
# MAGIC         ('CI-00047', 'bangle', 'CELL203', 3),
# MAGIC         ('CI-00047', 'bangle', 'CELL204', 4),
# MAGIC         ('CI-00047', 'bangle', 'CELL205', 5),
# MAGIC         ('CI-00047', 'bangle', 'CELL214', 6),
# MAGIC         ('CI-00047', 'bangle', 'CELL215', 7),
# MAGIC         ('CI-00047', 'bangle', 'CELL216', 8),
# MAGIC         ('CI-00047', 'bangle', 'CELL217', 9),
# MAGIC         ('CI-00047', 'bangle', 'CELL103', 10),
# MAGIC         ('CI-00047', 'bangle', 'CELL109', 11),
# MAGIC         ('CI-00047', 'bangle', 'CELL211', 12),
# MAGIC         ('CI-00047', 'bangle', 'CELL212', 13),
# MAGIC         ('CI-00047', 'bangle', 'CELL213', 14),
# MAGIC         ('CI-00047', 'bangle', 'CELL206', 15),
# MAGIC         ('CI-00047', 'bangle', 'CELL207', 16),
# MAGIC         ('CI-00047', 'bangle', 'CELL208', 17),
# MAGIC         ('CI-00047', 'bangle', 'CELL209', 18),
# MAGIC         ('CI-00047', 'bangle', 'CELL210', 19),
# MAGIC         ('CI-00047', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00047', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00047', 'bangle-lock', 'CELL210', 3),
# MAGIC         -- ABBOTT LYON
# MAGIC         ('CI-00048', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00048', 'silver', 'CELL205', 1),
# MAGIC         ('CI-00048', 'silver', 'CELL201', 2),
# MAGIC         ('CI-00048', 'silver', 'CELL202', 3),
# MAGIC         ('CI-00048', 'silver', 'CELL203', 4),
# MAGIC         ('CI-00048', 'silver', 'CELL204', 5),
# MAGIC         ('CI-00048', 'silver', 'CELL214', 6),
# MAGIC         ('CI-00048', 'silver', 'CELL215', 7),
# MAGIC         ('CI-00048', 'silver', 'CELL216', 8),
# MAGIC         ('CI-00048', 'silver', 'CELL217', 9),
# MAGIC         ('CI-00048', 'silver', 'CELL211', 10),
# MAGIC         ('CI-00048', 'silver', 'CELL212', 11),
# MAGIC         ('CI-00048', 'silver', 'CELL213', 12),
# MAGIC         ('CI-00048', 'silver', 'CELL206', 13),
# MAGIC         ('CI-00048', 'silver', 'CELL207', 14),
# MAGIC         ('CI-00048', 'silver', 'CELL208', 15),
# MAGIC         ('CI-00048', 'silver', 'CELL209', 16),
# MAGIC         ('CI-00048', 'silver', 'CELL210', 17),
# MAGIC         ('CI-00048', 'silver', 'CELL219', 18),
# MAGIC         ('CI-00048', 'silver', 'CELL109', 19),
# MAGIC         ('CI-00048', 'bangle', 'CELL201', 1),
# MAGIC         ('CI-00048', 'bangle', 'CELL202', 2),
# MAGIC         ('CI-00048', 'bangle', 'CELL203', 3),
# MAGIC         ('CI-00048', 'bangle', 'CELL204', 4),
# MAGIC         ('CI-00048', 'bangle', 'CELL205', 5),
# MAGIC         ('CI-00048', 'bangle', 'CELL214', 6),
# MAGIC         ('CI-00048', 'bangle', 'CELL215', 7),
# MAGIC         ('CI-00048', 'bangle', 'CELL216', 8),
# MAGIC         ('CI-00048', 'bangle', 'CELL217', 9),
# MAGIC         ('CI-00048', 'bangle', 'CELL103', 10),
# MAGIC         ('CI-00048', 'bangle', 'CELL109', 11),
# MAGIC         ('CI-00048', 'bangle', 'CELL211', 12),
# MAGIC         ('CI-00048', 'bangle', 'CELL212', 13),
# MAGIC         ('CI-00048', 'bangle', 'CELL213', 14),
# MAGIC         ('CI-00048', 'bangle', 'CELL206', 15),
# MAGIC         ('CI-00048', 'bangle', 'CELL207', 16),
# MAGIC         ('CI-00048', 'bangle', 'CELL208', 17),
# MAGIC         ('CI-00048', 'bangle', 'CELL209', 18),
# MAGIC         ('CI-00048', 'bangle', 'CELL210', 19),
# MAGIC         ('CI-00048', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00048', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00048', 'bangle-lock', 'CELL210', 3)
# MAGIC AS t(customer_no, material_type, cell_code, priority_rank);
# MAGIC 
# MAGIC -- 2. `gold_scheduling_category_rule` (6 BANGLE rules)
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.planning.gold_scheduling_category_rule
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     product_category, cell_code, priority_rank,
# MAGIC     CASE
# MAGIC         WHEN cell_code IN ('CELL105','CELL109')                                         THEN 'PRODLINE1'
# MAGIC         WHEN cell_code IN ('CELL201','CELL202','CELL203','CELL204','CELL205','CELL218') THEN 'PRODLINE2'
# MAGIC         WHEN cell_code IN ('CELL103','CELL104','CELL206','CELL207','CELL208','CELL209','CELL210','CELL219','CELL220') THEN 'PRODLINE3'
# MAGIC         WHEN cell_code IN ('CELL211','CELL212','CELL213')                               THEN 'PRODLINE4'
# MAGIC         WHEN cell_code IN ('CELL214','CELL215','CELL216','CELL217')                     THEN 'PRODLINE5'
# MAGIC         ELSE 'UNKNOWN'
# MAGIC     END AS production_line,
# MAGIC     current_timestamp() AS _load_timestamp
# MAGIC FROM VALUES
# MAGIC         ('BANGLE', 'CELL207', 1),
# MAGIC         ('BANGLE', 'CELL208', 2),
# MAGIC         ('BANGLE', 'CELL209', 3),
# MAGIC         ('BANGLE', 'CELL210', 4),
# MAGIC         ('BANGLE', 'CELL211', 5),
# MAGIC         ('BANGLE', 'CELL217', 6)
# MAGIC AS t(product_category, cell_code, priority_rank);
# MAGIC 
# MAGIC -- 3. `gold_scheduling_additional_rule` (39 rules, **incl. default**)
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.planning.gold_scheduling_additional_rule
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     rule_name,
# MAGIC     cell_code,
# MAGIC     CAST(is_allowed AS BOOLEAN) AS is_allowed,
# MAGIC     CASE
# MAGIC         WHEN cell_code IN ('CELL105','CELL109')                                         THEN 'PRODLINE1'
# MAGIC         WHEN cell_code IN ('CELL201','CELL202','CELL203','CELL204','CELL205','CELL218') THEN 'PRODLINE2'
# MAGIC         WHEN cell_code IN ('CELL103','CELL104','CELL206','CELL207','CELL208','CELL209','CELL210','CELL219','CELL220') THEN 'PRODLINE3'
# MAGIC         WHEN cell_code IN ('CELL211','CELL212','CELL213')                               THEN 'PRODLINE4'
# MAGIC         WHEN cell_code IN ('CELL214','CELL215','CELL216','CELL217')                     THEN 'PRODLINE5'
# MAGIC         ELSE 'UNKNOWN'
# MAGIC     END AS production_line,
# MAGIC     current_timestamp() AS _load_timestamp
# MAGIC FROM VALUES
# MAGIC         -- OXIDIZE
# MAGIC         ('OXIDIZE', 'CELL105', true),
# MAGIC         ('OXIDIZE', 'CELL109', true),
# MAGIC         ('OXIDIZE', 'CELL207', true),
# MAGIC         ('OXIDIZE', 'CELL208', true),
# MAGIC         ('OXIDIZE', 'CELL209', true),
# MAGIC         ('OXIDIZE', 'CELL210', true),
# MAGIC         ('OXIDIZE', 'CELL211', true),
# MAGIC         ('OXIDIZE', 'CELL212', true),
# MAGIC         ('OXIDIZE', 'CELL213', true),
# MAGIC         ('OXIDIZE', 'CELL214', true),
# MAGIC         ('OXIDIZE', 'CELL215', true),
# MAGIC         ('OXIDIZE', 'CELL216', true),
# MAGIC         ('OXIDIZE', 'CELL219', true),
# MAGIC 
# MAGIC         -- BANGLE_LOCK
# MAGIC         ('BANGLE_LOCK', 'CELL207', true),
# MAGIC         ('BANGLE_LOCK', 'CELL208', true),
# MAGIC         ('BANGLE_LOCK', 'CELL209', true),
# MAGIC         ('BANGLE_LOCK', 'CELL210', true),
# MAGIC         ('BANGLE_LOCK', 'CELL211', true),
# MAGIC 
# MAGIC         -- BANGLE_LOCK_T2
# MAGIC         ('BANGLE_LOCK_T2', 'CELL209', true),
# MAGIC 
# MAGIC         -- default
# MAGIC         ('default', 'CELL105', true),
# MAGIC         ('default', 'CELL109', true),
# MAGIC         ('default', 'CELL201', true),
# MAGIC         ('default', 'CELL202', true),
# MAGIC         ('default', 'CELL203', true),
# MAGIC         ('default', 'CELL204', true),
# MAGIC         ('default', 'CELL205', true),
# MAGIC         ('default', 'CELL206', true),
# MAGIC         ('default', 'CELL207', true),
# MAGIC         ('default', 'CELL208', true),
# MAGIC         ('default', 'CELL209', true),
# MAGIC         ('default', 'CELL210', true),
# MAGIC         ('default', 'CELL211', true),
# MAGIC         ('default', 'CELL212', true),
# MAGIC         ('default', 'CELL213', true),
# MAGIC         ('default', 'CELL214', true),
# MAGIC         ('default', 'CELL215', true),
# MAGIC         ('default', 'CELL216', true),
# MAGIC         ('default', 'CELL217', true),
# MAGIC         ('default', 'CELL219', true)
# MAGIC AS t(rule_name, cell_code, is_allowed);
# MAGIC 
# MAGIC -- 4. `gold_scheduling_customer_override`
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.planning.gold_scheduling_customer_override
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     customer_no, material_type, forced_cell, applies_to, reason,
# MAGIC     CASE
# MAGIC         WHEN forced_cell IN ('CELL105','CELL109')                                         THEN 'PRODLINE1'
# MAGIC         WHEN forced_cell IN ('CELL201','CELL202','CELL203','CELL204','CELL205','CELL218') THEN 'PRODLINE2'
# MAGIC         WHEN forced_cell IN ('CELL103','CELL104','CELL206','CELL207','CELL208','CELL209','CELL210','CELL219','CELL220') THEN 'PRODLINE3'
# MAGIC         WHEN forced_cell IN ('CELL211','CELL212','CELL213')                               THEN 'PRODLINE4'
# MAGIC         WHEN forced_cell IN ('CELL214','CELL215','CELL216','CELL217')                     THEN 'PRODLINE5'
# MAGIC         ELSE 'UNKNOWN'
# MAGIC     END AS production_line,
# MAGIC     current_timestamp() AS _load_timestamp
# MAGIC FROM VALUES
# MAGIC     ('CI-00040', 'gold', 'CELL109', 'ALL', 'VW gold always routes to CELL109 (Normal & SP)')
# MAGIC AS t(customer_no, material_type, forced_cell, applies_to, reason);
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- 5. v_gold_scheduling_customer_rule_active
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE VIEW Gold_Production_Lakehouse.planning.v_gold_scheduling_customer_rule_active
# MAGIC AS
# MAGIC SELECT
# MAGIC     r.customer_no,
# MAGIC     c.`Name` AS customer_name,
# MAGIC     c.`DSVC Branch ID` AS customer_branch,
# MAGIC     r.material_type,
# MAGIC     r.cell_code,
# MAGIC     r.priority_rank,
# MAGIC     r.production_line,
# MAGIC     r._load_timestamp
# MAGIC FROM Gold_Production_Lakehouse.planning.gold_scheduling_customer_rule r
# MAGIC INNER JOIN Silver_BC_Lakehouse.bc.Customer c
# MAGIC     ON c.`No.` = r.customer_no
# MAGIC WHERE c.`Blocked` IS NULL;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- 6. v_gold_scheduling_item_category
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE VIEW Gold_Production_Lakehouse.planning.v_gold_scheduling_item_category
# MAGIC AS
# MAGIC SELECT
# MAGIC     i.`No.` AS item_no,
# MAGIC     i.`Description` AS item_description,
# MAGIC     i.`Item Category Code` AS item_category_code,
# MAGIC     i.`Product Type` AS product_type,
# MAGIC     i.`Sub Product Type` AS sub_product_type,
# MAGIC     i.`Skill Level` AS skill_level,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN UPPER(TRIM(i.`Product Type`)) = 'BANGLE'
# MAGIC              AND UPPER(TRIM(COALESCE(i.`Sub Product Type`, ''))) IN ('BG-LOCK-TNG', 'BG-LOCK-PSH')
# MAGIC             THEN 'BANGLE-LOCK'
# MAGIC         WHEN UPPER(TRIM(i.`Product Type`)) = 'BANGLE' THEN 'BANGLE'
# MAGIC         ELSE 'OTHER'
# MAGIC     END AS scheduling_category,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN UPPER(TRIM(i.`Product Type`)) = 'BANGLE'
# MAGIC              AND UPPER(TRIM(COALESCE(i.`Sub Product Type`, ''))) IN ('BG-LOCK-TNG', 'BG-LOCK-PSH')
# MAGIC             THEN 'bangle-lock'
# MAGIC         WHEN UPPER(TRIM(i.`Product Type`)) = 'BANGLE' THEN 'bangle'
# MAGIC         ELSE NULL
# MAGIC     END AS bangle_material_type,
# MAGIC 
# MAGIC     current_timestamp() AS _load_timestamp
# MAGIC FROM Silver_BC_Lakehouse.bc.Item i
# MAGIC WHERE i.`Blocked` = '0';
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- 7. v_gold_scheduling_cell_pool
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE VIEW Gold_Production_Lakehouse.planning.v_gold_scheduling_cell_pool
# MAGIC AS
# MAGIC SELECT
# MAGIC     ic.item_no,
# MAGIC     ic.scheduling_category,
# MAGIC     ic.bangle_material_type,
# MAGIC     cr.customer_no,
# MAGIC     cr.customer_name,
# MAGIC     cr.material_type,
# MAGIC     cr.cell_code,
# MAGIC     cr.priority_rank,
# MAGIC     cr.production_line
# MAGIC FROM Gold_Production_Lakehouse.planning.v_gold_scheduling_item_category ic
# MAGIC CROSS JOIN Gold_Production_Lakehouse.planning.v_gold_scheduling_customer_rule_active cr
# MAGIC WHERE
# MAGIC     (ic.bangle_material_type IS NOT NULL AND cr.material_type = ic.bangle_material_type)
# MAGIC     OR ic.bangle_material_type IS NULL;
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC -- 8. Smoke tests
# MAGIC     -- 8.1 Row counts for all tables/views
# MAGIC 
# MAGIC SELECT 'customer_rule'        AS object_name, COUNT(*) AS row_count FROM Gold_Production_Lakehouse.planning.gold_scheduling_customer_rule
# MAGIC UNION ALL SELECT 'category_rule',     COUNT(*) FROM Gold_Production_Lakehouse.planning.gold_scheduling_category_rule
# MAGIC UNION ALL SELECT 'additional_rule',   COUNT(*) FROM Gold_Production_Lakehouse.planning.gold_scheduling_additional_rule
# MAGIC UNION ALL SELECT 'customer_override', COUNT(*) FROM Gold_Production_Lakehouse.planning.gold_scheduling_customer_override
# MAGIC UNION ALL SELECT 'v_cust_rule_active',COUNT(*) FROM Gold_Production_Lakehouse.planning.v_gold_scheduling_customer_rule_active
# MAGIC UNION ALL SELECT 'v_item_category',   COUNT(*) FROM Gold_Production_Lakehouse.planning.v_gold_scheduling_item_category
# MAGIC UNION ALL SELECT 'v_cell_pool',       COUNT(*) FROM Gold_Production_Lakehouse.planning.v_gold_scheduling_cell_pool
# MAGIC ORDER BY object_name;
# MAGIC 
# MAGIC     -- 8.2 Additional rule breakdown
# MAGIC 
# MAGIC SELECT rule_name, COUNT(*) AS cell_count, COLLECT_LIST(cell_code) AS cells
# MAGIC FROM Gold_Production_Lakehouse.planning.gold_scheduling_additional_rule
# MAGIC WHERE is_allowed = TRUE
# MAGIC GROUP BY rule_name
# MAGIC ORDER BY
# MAGIC     CASE rule_name
# MAGIC         WHEN 'OXIDIZE' THEN 1
# MAGIC         WHEN 'BANGLE_LOCK' THEN 2
# MAGIC         WHEN 'BANGLE_LOCK_T2' THEN 3
# MAGIC         WHEN 'default' THEN 4
# MAGIC         ELSE 99
# MAGIC     END;
# MAGIC 
# MAGIC     -- 8.3 VW gold override is in place
# MAGIC 
# MAGIC SELECT customer_no, material_type, forced_cell, applies_to, reason
# MAGIC FROM Gold_Production_Lakehouse.planning.gold_scheduling_customer_override;
# MAGIC     -- 8.4 Sample: VW silver pool order (verify ordering matches Editor)
# MAGIC 
# MAGIC SELECT priority_rank, cell_code, production_line
# MAGIC FROM Gold_Production_Lakehouse.planning.v_gold_scheduling_customer_rule_active
# MAGIC WHERE customer_no = 'CI-00040' AND material_type = 'silver'
# MAGIC ORDER BY priority_rank;

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
