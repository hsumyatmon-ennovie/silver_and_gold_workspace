-- Auto Generated (Do not modify) 58FD0E54A9BFFF36B16BEE2F7C97CDDE7047850CCBF05B1D127E28F4771EC179
CREATE     VIEW [prod].[gold_production_casting_status]
AS
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
        s.prod_order_line_no,                    
        s.created_on,                          
        s.current_location_code,              
        s.machine_center_no,                
        s.[type_name],
        s.[open],
        s.prod_order_status,
        CASE
            WHEN NULLIF(LTRIM(RTRIM(s.current_location_code)), '') IS NULL
                THEN LTRIM(RTRIM(s.machine_center_no))
            WHEN LEFT(UPPER(LTRIM(RTRIM(s.current_location_code))), 4) = 'CELL'
                THEN LTRIM(RTRIM(s.machine_center_no))
            ELSE LTRIM(RTRIM(s.current_location_code))
        END   AS [Prod Status]
    FROM [Silver_Production_Warehouse].[prod].[silver_production_status] s
),
DedupData AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY prod_order_no,prod_order_line_no,[type_name],[Prod Status]
            ORDER BY created_on DESC
            ) AS rn
        FROM LatestStatus
),
LatestStatus_1 AS (
    SELECT *
    FROM DedupData
    WHERE rn = 1
        AND [type_name] = 'In location in'
        AND [open] = 'Yes'
        AND prod_order_status = 'Released'
),
ProdWithLatest AS (
    SELECT
        po.*,
        l.[Prod Status]
    FROM ProdOrder po
    LEFT JOIN LatestStatus_1 l
      ON po.prod_order_no = l.prod_order_no
     AND po.prod_order_line_no = l.prod_order_line_no
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
 
        -- Status (ถ้าพาร์ต => WAX)
        CASE
            WHEN ct.casting_status IS NOT NULL THEN  ct.casting_status
            WHEN pl.[Prod Status]          IS NOT NULL THEN pl.[Prod Status]
            WHEN it.item_category = 'CASTING'          THEN 'WAX'
            ELSE 'RELEASED'
        END AS [Status]
 
    FROM ProdWithLatest pl
    LEFT JOIN [Silver_Inventory_Warehouse].[inv].[silver_item] it
           ON pl.prod_item_line = it.item_no
 
    LEFT JOIN [Silver_Production_Warehouse].[prod].[silver_casting_parts] cp
           ON pl.prod_order_no      = cp.prod_order_no
          AND pl.prod_order_line_no = cp.prod_order_line_no
 
    LEFT JOIN [Silver_Production_Warehouse].[prod].[silver_casting_tree] ct
           ON cp.casting_prod_order = ct.casting_prod_order
 
    WHERE (pl.prod_order_status IS NULL OR pl.prod_order_status IN ('Released', 'Finished'))
      AND (it.item_category   IS NULL OR it.item_category   IN ('FG','CASTING','SEMI-FG'))
)
SELECT DISTINCT *
FROM Production