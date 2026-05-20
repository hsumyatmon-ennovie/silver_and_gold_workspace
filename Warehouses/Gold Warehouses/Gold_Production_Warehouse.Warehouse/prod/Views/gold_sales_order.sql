-- Auto Generated (Do not modify) F2849E02E15AAC1CB64BF73D29C125B54B0DEB4E965141296993D700667931B8
CREATE     VIEW [prod].[gold_sales_order] AS WITH Sales AS (
    SELECT
        sh.sales_order_no AS SalesorderNo,
        sh.customer_no AS CustomerNo,
        sh.customer_name AS CustomerName,
        c.customer_abbreviation AS CustomerAbbr,
        sh.sales_order_status AS StatusSO,
        DATEPART (WEEK , sh.sales_order_requested_date)
        AS [Requested Week],
        sh.sales_order_requested_date AS RequestedDate,
        sh.sales_order_promised_date AS PromisedDate,
        sh.sales_order_cs_reference AS CSNoted,
        sh.cs_team AS CSteam,
        sh.sales_order_external_document as ExternalDocument,
        sh.sales_order_released_date as ReleasedDate,
        sl.sales_order_line_no AS SalesLineNo,
        sl.item_no AS ItemFG,
        sl.item_description,
        sl.item_quantity AS TotalQTY,
        CAST (COALESCE (sl.item_quantity , 0) - COALESCE (sl.item_quantity_shipped , 0) AS DECIMAL (18 , 4))
        AS OutstandingQty,
        sl.sales_order_shipment_date AS SalesLineShipmentDate,
        sl.item_material AS TypeofFG,
        CONCAT (sh.sales_order_no , sl.sales_order_line_no)
        AS SOL,
        LEFT(sh.sales_order_no, 2) + RIGHT(sh.sales_order_no, 4) AS [SO Abbr],
        LEFT(sh.sales_order_no, 2) AS [SO Type]
       
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