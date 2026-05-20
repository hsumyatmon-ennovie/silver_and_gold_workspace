CREATE OR ALTER VIEW [prod].[gold_production_casting_status] AS
WITH ProdOrder AS (
    SELECT
        po.sales_order_no,
        po.sales_order_line_no,
        po.prod_order_no,
        pl.prod_order_line_no,
        po.FG_item_no,
        po.item_routing_no,
        po.prod_order_starting_date_time,
        po.prod_order_ending_date_time,
        po.prod_order_due_date,
        DATEPART(WEEK, po.prod_order_due_date) AS commit_week,
        pl.prod_line_due_date,
        po.prod_order_finished_date,
        po.prod_order_quantity,
        po.prod_order_status,
        po.ref_prod_order,
        po.ref_item,
        pl.prod_line_start_date,
        pl.prod_line_end_date,
        pl.prod_line_quantity,
        pl.prod_line_finished_quantity,
        pl.prod_line_remaining_quantity,
        pl.item_location,
        pl.item_no AS prod_item_line,
        CONCAT(po.sales_order_no, po.sales_order_line_no) AS SOL,
        CONCAT(po.prod_order_no, pl.prod_order_line_no) AS POL
    FROM [Silver_Production_Warehouse].[prod].[silver_production_order] AS po
    LEFT JOIN [Silver_Production_Warehouse].[prod].[silver_production_order_line] AS pl
      ON po.prod_order_no = pl.prod_order_no
),
LatestStatus AS (
    SELECT
        s.prod_order_no,
        TRY_CONVERT(int, s.prod_order_line_no) AS prod_order_line_no_int,
        s.created_on                            AS status_created_on_utc,
        s.current_location_code                 AS current_location_code_latest,
        s.machine_center_no                     AS machine_center_no,
        ROW_NUMBER() OVER (
            PARTITION BY s.prod_order_no,
                         TRY_CONVERT(int, s.prod_order_line_no),
                         s.current_location_code,
                         s.machine_center_no
            ORDER BY s.created_on DESC
        ) AS rn
    FROM [Silver_Production_Warehouse].[prod].[silver_production_status] s
    WHERE s.type_name = 'In location in'
      AND s.[open] = 'Yes'
      AND s.prod_order_status = 'Released'
),
ProdWithLatest AS (
    SELECT
        po.*,
        ls.status_created_on_utc,
        ls.current_location_code_latest,
        ls.machine_center_no
    FROM ProdOrder po
    LEFT JOIN LatestStatus ls
      ON ls.prod_order_no = po.prod_order_no
     AND ls.prod_order_line_no_int = TRY_CONVERT(int, po.prod_order_line_no)
     AND ls.rn = 1
),
Production AS (
    SELECT
        pl.*,
        it.item_category                AS itemFG_Category,
        cp.item_no                      AS itemCST,
        cp.casting_prod_order,
        cp.casting_qty_to_tree,
        cp.casting_qty_passed,
        cp.casting_qty_reject,
        ct.casting_tree_no,
        ct.casting_status,
        CASE
            WHEN NULLIF(LTRIM(RTRIM(pl.current_location_code_latest)), '') IS NULL
                THEN LTRIM(RTRIM(pl.machine_center_no))
            WHEN LEFT(UPPER(LTRIM(RTRIM(pl.current_location_code_latest))), 4) = 'CELL'
                THEN LTRIM(RTRIM(pl.machine_center_no))
            ELSE LTRIM(RTRIM(pl.current_location_code_latest))
        END AS [Prod Status]
    FROM ProdWithLatest pl
    LEFT JOIN [Silver_Inventory_Warehouse].[inv].[silver_item] it
           ON pl.ref_item = it.item_no
    LEFT JOIN [Silver_Production_Warehouse].[prod].[silver_casting_parts] cp
           ON pl.prod_order_no        = cp.prod_order_no
          AND pl.prod_order_line_no   = cp.prod_order_line_no
    LEFT JOIN [Silver_Production_Warehouse].[prod].[silver_casting_tree]  ct
           ON cp.casting_prod_order   = ct.casting_prod_order
    WHERE (pl.prod_order_status IS NULL OR pl.prod_order_status IN ('Released', 'Finished'))
      AND (it.item_category   IS NULL OR it.item_category   IN ('FG','CASTING','SEMI-FG'))
)
SELECT *
FROM Production;
