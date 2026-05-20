DECLARE @Today date = CAST(GETDATE() AS date);
DECLARE @WarnWindowDays int = 2;  -- “At risk” window

WITH steps AS (
    SELECT DISTINCT
        UPPER(LTRIM(RTRIM(current_operation))) AS current_operation_norm,
        UPPER(LTRIM(RTRIM(operation_abb)))     AS operation_abb_norm
    FROM [Gold_Commons_Warehouse].[cmn].[gold_prod_step_casting_production]
    WHERE [department] <> 'OTHER'
),
base AS (
    SELECT DISTINCT
        pcs.*,
        -- pcs.prod_line_start_date,
        -- pcs.prod_line_end_date,
        -- pcs.prod_line_remaining_quantity,
        -- pcs.prod_order_due_date,                       -- <— bring in shop due date
        UPPER(LTRIM(RTRIM(COALESCE(pcs.[Status],'')))) AS status_norm
    FROM [Gold_Production_Warehouse].[prod].[gold_production_casting_status] pcs
    LEFT JOIN [Gold_Production_Warehouse].[prod].[gold_sales_order] sl
        ON sl.SOL = pcs.SOL
    WHERE pcs.[Status] <> 'COMPLETE'
      AND sl.[StatusSO] = 'Released'
    -- AND sl.[SalesorderNo] = 'RO000003043'      -- your test filter
    --  AND pcs.prod_order_line_no = '10000'
     -- and pcs.[Status] = 'WAREHOUS'
),
joined AS (
    SELECT DISTINCT
        b.*,
        s.operation_abb_norm
    FROM base b
    LEFT JOIN steps s
      ON s.current_operation_norm = b.status_norm
),
calc AS (
    SELECT DISTINCT
        j.*,

        -- How long the line has been running
        DATEDIFF(day, j.prod_line_start_date,
                      COALESCE(NULLIF(j.prod_line_end_date, '19000101'), @Today)) AS days_taken,

        /* SLA (target days from FG start) mapped to your operation_abb list */
        CASE
            -- Customer-facing
            WHEN j.operation_abb_norm = 'C. INS'     THEN 15
            WHEN j.operation_abb_norm = 'PCK'        THEN 16

            -- QA / QC
            WHEN j.operation_abb_norm IN ('QC','DIA T.') THEN 13
            WHEN j.operation_abb_norm = 'QA'         THEN 14
            WHEN j.operation_abb_norm = 'QA1'        THEN 9   -- pre-plating check (no extra day)

            -- Plating / Glue
            WHEN j.operation_abb_norm = 'PLT'        THEN 10
            WHEN j.operation_abb_norm = 'GLU'        THEN 12

            -- Polishing
            WHEN j.operation_abb_norm = 'POL'        THEN 8   -- step 1–2 (+2)
            WHEN j.operation_abb_norm = 'SHI'        THEN 9   -- final shine (+1)

            -- Laser
            WHEN j.operation_abb_norm = 'LAS'        THEN 5

            -- Stone setting / stringing
            WHEN j.operation_abb_norm IN ('SET')     THEN 6

            -- Surface/finish ops treated as early Finishing
            WHEN j.operation_abb_norm IN ('TUM','FIL') THEN 2

            ELSE 12  -- default baseline (min FG lead time)
        END AS sla_days
    FROM joined j
    WHERE j.operation_abb_norm NOT IN ('FIN')  -- your exclusion
),
with_due AS (
    SELECT DISTINCT
        c.*,

        -- SLA-based due date from FG start
        DATEADD(day, c.sla_days, c.prod_line_start_date) AS sla_due_date,

        -- EFFECTIVE due date = earlier of SLA due vs prod_order_due_date (when both exist)
        CASE
            WHEN c.prod_order_due_date IS NULL THEN DATEADD(day, c.sla_days, c.prod_line_start_date)
            WHEN DATEADD(day, c.sla_days, c.prod_line_start_date) IS NULL THEN c.prod_order_due_date
            WHEN DATEADD(day, c.sla_days, c.prod_line_start_date) <= c.prod_order_due_date
                 THEN DATEADD(day, c.sla_days, c.prod_line_start_date)
            ELSE c.prod_order_due_date
        END AS effective_due_date
    FROM calc c
),
classed AS (
    SELECT DISTINCT
        w.*,

        -- Days until the effective due date (negative => overdue)
        DATEDIFF(day, @Today, w.effective_due_date) AS days_until_due,

        CASE
            WHEN DATEDIFF(day, @Today, w.effective_due_date) < 0 THEN 'Overdue'
            WHEN DATEDIFF(day, @Today, w.effective_due_date) <= @WarnWindowDays THEN 'At risk'
            ELSE 'On time'
        END AS schedule_class
    FROM with_due w
)

-- Per-operation: counts & quantities by class (for the filtered order/line)
SELECT 
    COALESCE(NULLIF(status_norm,''), '(UNKNOWN)')      AS operation_status,
    COALESCE(operation_abb_norm, '(UNKNOWN ABBR)')     AS operation_abb,
    COUNT(*)                                           AS total_lines,
    SUM(CASE WHEN schedule_class = 'On time' THEN 1 ELSE 0 END) AS on_time_lines,
    SUM(CASE WHEN schedule_class = 'At risk' THEN 1 ELSE 0 END) AS at_risk_lines,
    SUM(CASE WHEN schedule_class = 'Overdue' THEN 1 ELSE 0 END) AS overdue_lines,

    SUM(prod_line_remaining_quantity) AS total_remaining_qty,
    SUM(CASE WHEN schedule_class = 'On time' THEN prod_line_remaining_quantity ELSE 0 END) AS on_time_qty,
    SUM(CASE WHEN schedule_class = 'At risk' THEN prod_line_remaining_quantity ELSE 0 END) AS at_risk_qty,
    SUM(CASE WHEN schedule_class = 'Overdue' THEN prod_line_remaining_quantity ELSE 0 END) AS overdue_qty
FROM classed
GROUP BY
    COALESCE(NULLIF(status_norm,''), '(UNKNOWN)'),
    COALESCE(operation_abb_norm, '(UNKNOWN ABBR)')
ORDER BY operation_abb, operation_status;

-- If you also want a single KPI row (overall quantities), uncomment:
/*
SELECT
    SUM(prod_line_remaining_quantity) AS total_remaining_qty,
    SUM(CASE WHEN schedule_class = 'On time' THEN prod_line_remaining_quantity ELSE 0 END) AS on_time_qty,
    SUM(CASE WHEN schedule_class = 'At risk' THEN prod_line_remaining_quantity ELSE 0 END) AS at_risk_qty,
    SUM(CASE WHEN schedule_class = 'Overdue' THEN prod_line_remaining_quantity ELSE 0 END) AS overdue_qty
FROM classed;
*/
