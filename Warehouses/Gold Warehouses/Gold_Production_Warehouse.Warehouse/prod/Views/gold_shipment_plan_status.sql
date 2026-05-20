-- Auto Generated (Do not modify) 4861EB5C52C2C6A6A43C829073CE6221F7E7557C1AE9EA7E948E4EB7E96816C4
CREATE   VIEW [prod].[gold_shipment_plan_status]
AS
/* =========================
   GOLD: Sales ↔ Production (+Routing→LatestStatus via silver_production_status) + simple casting join
   ========================= */
 
-- 1) Sales + customer abbreviation  (คงเดิม)
WITH Sales AS (
    SELECT
        sh.sales_order_no                              AS SalesorderNo,
        sh.customer_no                                 AS CusNo,
        sh.customer_name                               AS CusName,
        c.customer_abbreviation                        AS CusAbbr,
        sh.sales_order_status                          AS StatusSO,
        sh.sales_order_requested_date                  AS ReqDate,
        sh.sales_order_promised_date                   AS PmDate,
        sh.sales_order_cs_reference                    AS CSNoted,
        sh.cs_team                                     AS CSteam,
        sh.sales_order_external_document,
        sh.sales_order_released_date,
        DATEPART(WEEK, sh.sales_order_requested_date)  AS ShipmentWeek,
        sl.sales_order_line_no                         AS SalesLineNo,
        sl.item_no                                     AS ItemFG,
        sl.item_description,
        sl.item_quantity                               AS Total_QTY,
        sl.sales_order_shipment_date                   AS LineShipmentDate,
        sl.item_material                               AS TypeofFG,
        CAST(COALESCE(sl.item_quantity, 0) - COALESCE(sl.item_quantity_shipped, 0) AS decimal(18,4)) AS OutstandingQty
    FROM [Silver_Customer_Exp_Warehouse].[cx].[silver_sales_header] sh
    LEFT JOIN [Silver_Customer_Exp_Warehouse].[cx].[silver_sales_line] sl
           ON sh.sales_order_no = sl.sales_order_no
    LEFT JOIN [Silver_Customer_Exp_Warehouse].[cx].[silver_customers]  c
           ON c.customer_no = sh.customer_no
    WHERE sh.sales_order_status IN ('Open','Released','Pending Approval','Pending Prepayment','Closed')
        AND sh.sales_order_type = 'Order'
),
-- 2) Production core:
ProdOrder AS (
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
        DATEPART(WEEK, po.prod_order_due_date) AS [commit_week],
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
        pl.item_no AS [prod_item_line]
    FROM [Silver_Production_Warehouse].[prod].[silver_production_order] as po
    LEFT JOIN [Silver_Production_Warehouse].[prod].[silver_production_order_line] as pl ON po.prod_order_no = pl.prod_order_no
),
 
/* 3) Routing: เลือก CELL ตัวแรกต่อ (prod_order_no, prod_order_line_no) โดยดัน CELL108 ไปท้าย  (คงเดิม) */
RoutingRanked AS (
    SELECT
        rl.prod_order_no,
        rl.item_no,
        rl.prod_order_lineno,
        rl.routing_no,
        ROW_NUMBER() OVER (
            PARTITION BY rl.prod_order_no, rl.prod_order_lineno
            ORDER BY
                CASE WHEN rl.routing_no = 'CELL108' THEN 1 ELSE 0 END,
                TRY_CONVERT(int, STUFF(rl.routing_no, 1, 4, '')),
                rl.routing_no
        ) AS rn
    FROM [Silver_Production_Warehouse].[prod].[silver_routing_lines] rl
    WHERE rl.prod_order_lineno = 10000
      AND rl.routing_no LIKE 'CELL%'
),
Routing AS (
    SELECT
        prod_order_no,
        item_no,
        prod_order_lineno,
        routing_no AS first_cell_no
    FROM RoutingRanked
    WHERE rn = 1
),
 
-- 4) Production + Routing  (คงเดิม)
ProdRouting AS (
    SELECT
        po.*,
        r.item_no            AS routing_item_no,
        r.prod_order_lineno  AS routing_line_no,
        r.first_cell_no
    FROM ProdOrder po
    LEFT JOIN Routing r
           ON r.prod_order_no = po.prod_order_no
),
-- 4.1) Production + Routing + PROD LINE  (คงเดิม)
ProdRoutingProdLine AS (
    SELECT
        p.*,
c.[cell_line],
c.[prod_line]
    FROM ProdRouting p
    LEFT JOIN [Silver_Production_Warehouse].[prod].[silver_cell_list] as c
           ON p.first_cell_no = c.[cell_line]
),
/* 5) LatestStatus: ใช้ silver_production_status ตามเงื่อนไขตัวอย่าง
      - type_name = 'In location in'
      - open      = 'Yes'
      - prod_order_status = 'Released'
      และเลือก "แถวล่าสุด" ต่อ (prod_order_no, prod_order_line_no) โดย created_on DESC */
LatestStatus AS (
    SELECT
        s.prod_order_no,
        TRY_CONVERT(int, s.prod_order_line_no) AS prod_order_line_no_int,
        s.created_on                            AS status_created_on_utc,
        s.current_location_code                 AS current_location_code_latest,
        s.machine_center_no                    AS machine_center_no,
        ROW_NUMBER() OVER (
            PARTITION BY s.prod_order_no, TRY_CONVERT(int, s.prod_order_line_no), s.current_location_code, s.machine_center_no
            ORDER BY s.created_on DESC
        ) AS rn
    FROM [Silver_Production_Warehouse].[prod].[silver_production_status] s
    WHERE s.type_name = 'In location in'
      AND s.[open] = 'Yes'
      AND s.prod_order_status = 'Released'
 
),
 
-- 6) ProdRouting + LatestStatus
ProdWithLatest AS (
    SELECT
        pr.*,
        ls.status_created_on_utc,
        ls.current_location_code_latest,
        ls.machine_center_no
    FROM ProdRoutingProdLine pr
    LEFT JOIN LatestStatus ls
           ON ls.rn = 1
          AND ls.prod_order_no          = pr.prod_order_no
          AND ls.prod_order_line_no_int = pr.prod_order_line_no   -- ใช้เลขบรรทัด "production" จาก routing
),
 
/* 7) Casting: join แบบง่าย ๆ จาก silver_casting_parts + silver_casting_tree */
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
        ct.casting_status
    FROM ProdWithLatest pl
    LEFT JOIN [Silver_Inventory_Warehouse].[inv].[silver_item] it
           ON pl.ref_item = it.item_no
    LEFT JOIN [Silver_Production_Warehouse].[prod].[silver_casting_parts] cp
           ON pl.prod_order_no       = cp.prod_order_no
          AND pl.prod_order_line_no = cp.prod_order_line_no
    LEFT JOIN [Silver_Production_Warehouse].[prod].[silver_casting_tree]  ct
           ON cp.casting_prod_order  = ct.casting_prod_order
    WHERE (pl.prod_order_status IS NULL OR pl.prod_order_status IN ('Released', 'Finished'))
      AND (it.item_category   IS NULL OR it.item_category   IN ('FG','CASTING','SEMI-FG'))
)
 
-- 8) Final
SELECT DISTINCT
    s.ShipmentWeek AS [Requested Week],
    p.commit_week AS [Commit Week],
        CASE
        WHEN p.prod_order_due_date IS NULL THEN NULL
        WHEN DATEDIFF(DAY, CAST(GETDATE() AS date), p.prod_order_due_date) < 0
             THEN CONCAT('Overdue ', ABS(DATEDIFF(DAY, CAST(GETDATE() AS date), p.prod_order_due_date)), 'd')
        ELSE CONCAT('Due ', DATEDIFF(DAY, CAST(GETDATE() AS date), p.prod_order_due_date), 'd')
    END AS [Due In],
    CASE
        WHEN p.prod_order_due_date IS NULL THEN NULL
        WHEN DATEDIFF(DAY, CAST(GETDATE() AS date), p.prod_order_due_date) < 0 THEN 'Overdue'
        WHEN DATEDIFF(DAY, CAST(GETDATE() AS date), p.prod_order_due_date) <= 3 THEN 'At risk'
        ELSE 'On time'
    END AS [Due Status],
    p.prod_line AS [Line],
    p.first_cell_no AS [Cell],
    s.CusAbbr AS [Customer],
    s.StatusSO AS [Status SO],
    s.SalesorderNo AS [SO],
    LEFT(s.SalesorderNo, 2) + RIGHT(s.SalesorderNo, 4) AS [SO Abbr],
    s.SalesLineNo AS [SO Line],
    s.ItemFG AS [FG],
    s.item_description AS [FG Description],
    s.CSteam AS [CS Team],
    s.CSNoted AS [CS Noted],
    s.sales_order_external_document AS [CS Document],
    s.TypeofFG AS [FG Type],
    s.Total_QTY AS [Total QTY],
    s.OutstandingQty AS [Outstanding QTY],
    p.prod_order_no AS [Prod],
    p.prod_order_due_date as [Prod Due Date],
    MAX(CASE WHEN p.prod_order_line_no = 10000 THEN p.prod_order_starting_date_time END)
        OVER (PARTITION BY p.prod_order_no) AS [FG Start Date],
    MAX(CASE WHEN p.item_location = 'CST_CUT' THEN p.prod_line_due_date END)
        OVER (PARTITION BY p.prod_order_no, p.ref_item)                              AS [Casting Due Date],
    p.prod_order_line_no AS [Prod Line],
    p.prod_item_line AS [Item],
    p.itemCST AS [Item CAS],
 
    CASE
        WHEN NULLIF(LTRIM(RTRIM(p.[current_location_code_latest])), '') IS NULL
            THEN LTRIM(RTRIM(p.[machine_center_no]))
        WHEN LEFT(UPPER(LTRIM(RTRIM(p.[current_location_code_latest]))), 4) = 'CELL'
            THEN LTRIM(RTRIM(p.[machine_center_no]))
        ELSE LTRIM(RTRIM(p.[current_location_code_latest]))
    END AS [Prod Status],
 
    p.prod_order_quantity AS [Prod Qty],
    p.prod_line_remaining_quantity as [Prod Remaining QTY],
    p.prod_line_finished_quantity as [Prod Finished QTY],
    p.casting_prod_order AS [Prod CAS],
    p.casting_tree_no AS [CAS Tree],
    p.casting_qty_to_tree AS [CAS Qty],
    p.casting_status AS [CAS Status],
    CONCAT(s.SalesorderNo, s.SalesLineNo) AS SOL
 
FROM Sales s
LEFT JOIN Production p
       ON  s.SalesorderNo = p.sales_order_no
       AND s.SalesLineNo  = p.sales_order_line_no