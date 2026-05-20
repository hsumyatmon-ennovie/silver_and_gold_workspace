-- Auto Generated (Do not modify) CC556F4348DB33AF02C5CF25F82907C0955B0CC16184D0893CB1C5CD73C18CD3
CREATE VIEW [prod].[vw_casting_production_status] 
AS
WITH steps AS (
    SELECT DISTINCT
        UPPER(LTRIM(RTRIM(current_operation))) AS current_operation_norm,
        UPPER(LTRIM(RTRIM(operation_abb)))     AS operation_abb_norm
    FROM [Gold_Commons_Warehouse].[cmn].[gold_prod_step_casting_production]
    WHERE [department] <> 'OTHER'
),
base AS (
    SELECT
        pcs.*,
        UPPER(LTRIM(RTRIM(COALESCE(pcs.[Status],'')))) AS status_norm
    FROM [Gold_Production_Warehouse].[prod].[gold_production_casting_status] pcs
    LEFT JOIN [Gold_Production_Warehouse].[prod].[gold_sales_order] sl
        ON sl.SOL = pcs.SOL
    WHERE pcs.[Status] <> 'COMPLETE'
      AND sl.[StatusSO] = 'Released'
),
joined AS (
    SELECT
        b.*,
        s.operation_abb_norm
    FROM base b
    LEFT JOIN steps s
      ON s.current_operation_norm = b.status_norm
),
ranked AS (
    SELECT
        j.*,
        ROW_NUMBER() OVER (
            PARTITION BY j.prod_order_no, j.prod_order_line_no
            ORDER BY
                CASE WHEN j.prod_line_end_date IS NULL OR j.prod_line_end_date = '19000101' THEN 1 ELSE 0 END DESC,
                j.prod_line_start_date DESC
        ) AS rn
    FROM joined j
),
cur AS (
    SELECT *
    FROM ranked
    WHERE rn = 1
      AND COALESCE(operation_abb_norm, '') <> 'FIN'
),
calc AS (
    SELECT
        c.*,
        DATEDIFF(day, c.prod_line_start_date,
                      COALESCE(NULLIF(c.prod_line_end_date, '19000101'), CAST(GETDATE() AS date))) AS days_in_stage,
        CASE
            WHEN c.operation_abb_norm = 'C. INS'     THEN 15
            WHEN c.operation_abb_norm = 'PCK'        THEN 16
            WHEN c.operation_abb_norm IN ('QC','DIA T.') THEN 13
            WHEN c.operation_abb_norm = 'QA'         THEN 14
            WHEN c.operation_abb_norm = 'QA1'        THEN 9
            WHEN c.operation_abb_norm = 'PLT'        THEN 10
            WHEN c.operation_abb_norm = 'GLU'        THEN 12
            WHEN c.operation_abb_norm = 'POL'        THEN 8
            WHEN c.operation_abb_norm = 'SHI'        THEN 9
            WHEN c.operation_abb_norm = 'LAS'        THEN 5
            WHEN c.operation_abb_norm IN ('SET')     THEN 6
            WHEN c.operation_abb_norm IN ('TUM','FIL') THEN 2
            ELSE 12
        END AS sla_days
    FROM cur c
),
with_due AS (
    SELECT
        a.*,
        DATEADD(day, a.sla_days, a.prod_line_start_date) AS sla_due_date,
        CASE
            WHEN a.prod_order_due_date IS NULL THEN DATEADD(day, a.sla_days, a.prod_line_start_date)
            WHEN DATEADD(day, a.sla_days, a.prod_line_start_date) IS NULL THEN a.prod_order_due_date
            WHEN DATEADD(day, a.sla_days, a.prod_line_start_date) <= a.prod_order_due_date
                 THEN DATEADD(day, a.sla_days, a.prod_line_start_date)
            ELSE a.prod_order_due_date
        END AS effective_due_date
    FROM calc a
),
classed AS (
    SELECT
        w.*,
        DATEDIFF(day, CAST(GETDATE() AS date), w.effective_due_date) AS days_until_due,
        CASE
            WHEN DATEDIFF(day, CAST(GETDATE() AS date), w.effective_due_date) < 0 THEN 'Overdue'
            WHEN DATEDIFF(day, CAST(GETDATE() AS date), w.effective_due_date) <= 2 THEN 'At risk'
            ELSE 'On time'
        END AS schedule_class,
        CASE
            WHEN DATEDIFF(day, CAST(GETDATE() AS date), w.effective_due_date) < 0 THEN 'Yes'
            ELSE 'No'
        END AS will_be_delayed
    FROM with_due w
)
SELECT
    prod_order_no,
    prod_order_line_no,
    sales_order_no,
    sales_order_line_no,
    FG_item_no,
    status_norm                  AS current_status,
    operation_abb_norm           AS operation_abb,
    prod_line_start_date         AS stage_start_date,
    prod_line_end_date           AS stage_end_date,
    days_in_stage,
    sla_days,
    sla_due_date,
    prod_order_due_date,
    effective_due_date,
    days_until_due,
    schedule_class,
    will_be_delayed,
    prod_line_quantity,
    prod_line_finished_quantity,
    prod_line_remaining_quantity
FROM classed
-- ORDER BY will_be_delayed DESC, schedule_class, days_until_due, prod_order_no, prod_order_line_no;