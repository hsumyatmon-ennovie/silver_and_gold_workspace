-- Auto Generated (Do not modify) 2FA1CD200D76636A5D920447A00871487AFBD960E8BA086DA942F03C8B60C7EA
CREATE   VIEW [cx].[gold_sales_order] AS 
SELECT DISTINCT
  -- Header fields
  sh.[sales_order_no] AS SalesorderNo, 
  sh.[customer_no] AS CusNo, 
  sh.[customer_name] AS CusName, 
  sh.[sales_order_status] AS StatusSO, 
  sh.[sales_order_document_date] AS DocDate, 
  sh.[sales_order_posting_date] AS PostDate, 
  sh.[sales_order_order_date] AS OrderDate, 
  sh.[sales_order_due_date] AS DueDate, 
  sh.[sales_order_requested_date] AS ReqDate, 
  sh.[sales_order_promised_date] AS PmDate, 
  sh.[sales_order_shipment_date] AS ShipDate, 
  sh.[sales_order_cs_reference] AS CSNoted, 
  sh.[cs_team] AS CSteam, 
  sh.[sales_order_currency] AS Currency, 
  sh.[ship_to_customer] AS ShiptoName, 
  sh.[bill_to_customer] AS BilltoName, 
  sh.[sales_order_location] AS LOCATION, 
  sh.[sales_order_external_document], 
  sh.[sales_order_released_date], 
  DATEPART (
    WEEK, sh.[sales_order_requested_date]
  ) AS ShipmentWeek, 
  -- Line fields
  sl.[sales_order_line_no] AS [LineNo], 
  sl.[item_no] AS ItemFG, 
  sl.[item_description], 
  sl.[item_posting_group], 
  sl.[item_location] AS LineLocation, 
  CASE WHEN h.HyphenPos = 2 THEN LEFT (
    sl.[item_no], 
    CASE WHEN LEN (sl.[item_no]) > = 8 THEN 8 ELSE LEN (sl.[item_no]) END
  ) WHEN h.HyphenPos = 11 THEN LEFT (
    sl.[item_no], 
    CASE WHEN LEN (sl.[item_no]) > = 10 THEN 10 ELSE LEN (sl.[item_no]) END
  ) WHEN h.HyphenPos > 0 THEN LEFT (sl.[item_no], h.HyphenPos - 1) ELSE sl.[item_no] END AS Itemgroup, 
  sl.[item_quantity] AS QTY, 
  sl.[item_uom] AS UOM, 
  sl.[item_unit_cost], 
  sl.[sales_line_unit_price] AS UnitPrice, 
  sl.[item_quantity_to_ship], 
  sl.[item_quantity_shipped] AS QtyShip, 
  sl.[item_quantity_to_invoice], 
  sl.[item_quantity_invoiced] AS QtyINV, 
  sl.[sales_line_requested_date], 
  sl.[sales_line_promised_date], 
  sl.[sales_line_plan_delivery], 
  sl.[sales_line_plan_shipment], 
  sl.[sales_order_shipment_date] AS LineShipmentDate, 
  sl.[item_material] AS TypeofFG 
FROM 
  [Silver_Customer_Exp_Warehouse].[cx].[silver_sales_header] AS sh 
  JOIN [Silver_Customer_Exp_Warehouse].[cx].[silver_sales_line] AS sl ON sh.[sales_order_no] = sl.[sales_order_no] -- COMPUTE FIRST hyphen position once per ROW
  CROSS APPLY (
    VALUES 
      (
        NULLIF (
          CHARINDEX ('-', sl.[item_no]), 
          0
        )
      )
  ) AS h (HyphenPos) -- FILTER statuses
WHERE 
  sh.[sales_order_status] IN (
    'Open', 'Released', 'Pending Approval', 
    'Pending Prepayment', 'Closed'
  );