-- Auto Generated (Do not modify) DC55A3AA9BED3FF15F9C0A2F3C7FD2389AD1C1E5034824CBE13B2242D1DF4AB7
CREATE     VIEW [prod].[gold_production_order_list] AS (
    SELECT DISTINCT
        po.prod_order_no,

        po.FG_item_no,
        po.item_routing_no,
        po.sales_order_no,
        po.sales_order_line_no,
        po.prod_order_starting_date_time,
        po.prod_order_ending_date_time,
        po.prod_order_due_date,
        DATEPART (WEEK , po.prod_order_due_date)        AS [Commit Week],
        po.prod_order_finished_date,
        po.prod_order_quantity,
        po.prod_order_status,
        po.ref_prod_order,
        po.ref_item,

        CONCAT (po.sales_order_no , po.sales_order_line_no)
        AS SOL,
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
)