CREATE OR ALTER VIEW [prod].[gold_production_order] AS WITH ProdOrder AS (
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
        DATEPART (WEEK , po.prod_order_due_date)
        AS [Commit Week],
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
        CONCAT (po.sales_order_no , po.sales_order_line_no)
        AS SOL,
        CONCAT (po.prod_order_no , pl.prod_order_line_no)
        AS POL,
        CASE
            WHEN po.prod_order_due_date IS NULL THEN NULL
            WHEN DATEDIFF(DAY, CAST(GETDATE() AS date), po.prod_order_due_date) < 0
                THEN CONCAT('Overdue ', ABS(DATEDIFF(DAY, CAST(GETDATE() AS date), po.prod_order_due_date)), 'd')
            ELSE CONCAT('Due ', DATEDIFF(DAY, CAST(GETDATE() AS date), po.prod_order_due_date), 'd')
        END AS [Due In],
        CASE
            WHEN po.prod_order_due_date IS NULL THEN NULL
            WHEN DATEDIFF(DAY, CAST(GETDATE() AS date), po.prod_order_due_date) < 0 THEN 'Overdue'
            WHEN DATEDIFF(DAY, CAST(GETDATE() AS date), po.prod_order_due_date) <= 3 THEN 'At risk'
            ELSE 'On time'
        END AS [Due Status]
    FROM
        [Silver_Production_Warehouse].[prod].[silver_production_order] AS po
        LEFT JOIN [Silver_Production_Warehouse].[prod].[silver_production_order_line] AS pl ON po.prod_order_no = pl.prod_order_no
)
SELECT DISTINCT * FROM ProdOrder;
