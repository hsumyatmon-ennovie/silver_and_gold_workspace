CREATE OR ALTER VIEW [prod].[gold_sales_order] AS WITH Sales AS (
    SELECT
        sh.sales_order_no AS SalesorderNo,
        sh.customer_no AS CusNo,
        sh.customer_name AS CusName,
        c.customer_abbreviation AS CusAbbr,
        sh.sales_order_status AS StatusSO,
        sh.sales_order_requested_date AS ReqDate,
        sh.sales_order_promised_date AS PmDate,
        sh.sales_order_cs_reference AS CSNoted,
        sh.cs_team AS CSteam,
        sh.sales_order_external_document,
        sh.sales_order_released_date,
        DATEPART (WEEK , sh.sales_order_requested_date)
        AS [Requested Week],
        sl.sales_order_line_no AS SalesLineNo,
        sl.item_no AS ItemFG,
        sl.item_description,
        sl.item_quantity AS Total_QTY,
        sl.sales_order_shipment_date AS LineShipmentDate,
        sl.item_material AS TypeofFG,
        CAST (COALESCE (sl.item_quantity , 0) - COALESCE (sl.item_quantity_shipped , 0) AS DECIMAL (18 , 4))
        AS OutstandingQty,
        CONCAT (sh.sales_order_no , sl.sales_order_line_no)
        AS SOL,
        LEFT(sh.sales_order_no, 2) + RIGHT(sh.sales_order_no, 4) AS [SO Abbr]
    FROM
        [Silver_Customer_Exp_Warehouse].[cx].[silver_sales_header] sh
        LEFT JOIN [Silver_Customer_Exp_Warehouse].[cx].[silver_sales_line] sl ON sh.sales_order_no = sl.sales_order_no
        LEFT JOIN [Silver_Customer_Exp_Warehouse].[cx].[silver_customers] c ON c.customer_no = sh.customer_no
    WHERE
        sh.sales_order_status
        IN (
            'Open',
            'Released',
            'Pending Approval',
            'Pending Prepayment',
            'Closed'
        )
        AND sh.sales_order_type IN ('Order')
        
)
SELECT DISTINCT * FROM Sales;
