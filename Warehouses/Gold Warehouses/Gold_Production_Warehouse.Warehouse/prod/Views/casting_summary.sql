-- Auto Generated (Do not modify) 94DF99745C6937C6ED815459CBFC9CFBD3988226A2E021CA7DCDF83F3A2EAEBC
CREATE           VIEW [prod].[casting_summary] AS
WITH ProdLineData AS (
    SELECT
        pl.prod_line_due_date,
        pl.prod_line_start_date,
        pl.prod_line_end_date,
        pl.prod_order_no,
        pl.prod_order_line_no,
        pl.FG_item_no,
        pl.prod_item_line,
        pl.item_location,
        pl.prod_line_quantity,
        pl.prod_line_finished_quantity,
        pl.prod_line_remaining_quantity,
        pl.sales_order_no,
        COALESCE(pl.sales_order_no, fgpo.sales_order_no) AS [Sales Order],
 
        CAST(MAX(CASE WHEN pl.prod_order_line_no = 10000 THEN pl.prod_line_start_date END)
             OVER (PARTITION BY pl.prod_order_no) AS DATE) AS FG_Startdate,
 
        MAX(CASE WHEN pl.prod_order_line_no = 10000 THEN pl.prod_line_due_date END)
             OVER (PARTITION BY pl.prod_order_no) AS FG_duedate,
 
        MAX(CASE WHEN pl.item_location = 'CST_CUT' THEN pl.prod_line_due_date END)
             OVER (PARTITION BY pl.prod_order_no, pl.prod_item_line) AS Casting_duedate,
 
        CAST(MAX(CASE WHEN pl.item_location = 'CST_CUT' THEN pl.prod_line_start_date END)
             OVER (PARTITION BY pl.prod_order_no, pl.prod_item_line) AS DATE) AS Casting_Startdate
 
    FROM [Gold_Production_Warehouse].[prod].[gold_production_order] AS pl
    LEFT JOIN [Gold_Production_Warehouse].[prod].[gold_production_order] AS fgpo
        ON pl.ref_prod_order = fgpo.prod_order_no
    WHERE pl.prod_order_status IN ('Released', 'Firm Planned')
 
 
),
EmployeeData AS (
    SELECT
        [created_on],
        [modified_on],
        [prod_order_no],
        [prod_order_line_no],
        [type_name],
        [prod_order_status],
        [open],
        [sales_order_no],
        [current_location_code],
        [past_location_code],
        [employee_no],
        [user_id],
        [quantity],
        [remaining_quantity],
        [item_no],
        [machine_center_no],
        [out_qty],
        [pol],
        [created_on_time],
        [CorrectCurrentLocation]
    FROM [Gold_Production_Warehouse].[prod].[gold_production_status_casting]
    -- WHERE prod_order_status = 'Released'
),
CastingData AS (
    SELECT
        cp.prod_order_no,
        cp.prod_order_line_no,
        cp.casting_prod_order,
        ct.casting_tree_no,
        cp.item_no AS itemCST,
        cp.casting_qty_to_tree,
        cp.casting_qty_passed,
        cp.casting_qty_to_tree - cp.casting_qty_passed AS casting_qty_reject,
        ct.casting_status,
        CONCAT(cp.prod_order_no, cp.prod_order_line_no) AS pol
    FROM [Silver_Production_Warehouse].[prod].[silver_casting_parts] cp
    LEFT JOIN [Silver_Production_Warehouse].[prod].[silver_casting_tree] ct
           ON cp.casting_prod_order = ct.casting_prod_order
),
ItemData AS (
    SELECT
        item_no,
        item_description,
        item_metal_category,
        item_category
    FROM [Silver_Inventory_Warehouse].[inv].[silver_item]
),
Salesorder AS (
    SELECT
        [SalesorderNo],
        [CustomerNo],
        [CustomerAbbr]
    FROM [Gold_Production_Warehouse].[prod].[gold_sales_order]
),
FinalBase AS (
    SELECT
        p.[prod_order_no],
        p.[prod_order_line_no],
        p.[FG_item_no],
        p.[prod_item_line],
        p.[item_location],
        p.[prod_line_quantity],
        p.[prod_line_finished_quantity],
        p.[prod_line_remaining_quantity],
        p.[prod_line_due_date],
        p.[prod_line_start_date],
        p.[prod_line_end_date],
        p.[Sales Order] AS Sales_Order,
        p.FG_Startdate,
        p.FG_duedate,
        p.Casting_Startdate,
        p.Casting_duedate,
        e.[created_on],
        e.[modified_on],
        e.[type_name],
        e.[prod_order_status],
        e.[user_id],
        e.[open],
        e.[sales_order_no] AS emp_sales_order_no,
        e.[quantity],
        e.[remaining_quantity],
        e.[current_location_code],
        e.[machine_center_no],
        e.[CorrectCurrentLocation],
        c.casting_prod_order,
        c.casting_tree_no,
        c.itemCST,
        c.casting_qty_to_tree,
        c.casting_qty_passed,
        c.casting_qty_reject,
        c.casting_status,
        i.item_description,
        i.item_metal_category,
        i.item_category,
        s.[CustomerNo],
        s.[CustomerAbbr],
        CASE
            WHEN i.item_metal_category = 'SILVER 925' THEN 'SILVER'
            WHEN i.item_metal_category IN ('14KW','14KY','14KR','18KR','18KW','18KY','9KW','9KY') THEN 'GOLD'
            ELSE i.item_metal_category
        END AS MetalCategory,
CASE
    WHEN (c.casting_status IS NULL OR LTRIM(RTRIM(c.casting_status)) = '')
        --  AND (e.machine_center_no IS NULL OR LTRIM(RTRIM(e.machine_center_no)) = '')
        THEN 'Not Start'
    WHEN (c.casting_status IS NULL OR LTRIM(RTRIM(c.casting_status)) = '')
         AND NULLIF(LTRIM(RTRIM(e.machine_center_no)), '') IS NOT NULL
        THEN LTRIM(RTRIM(e.machine_center_no))
    ELSE LTRIM(RTRIM(c.casting_status))
END AS New_Status,
 
        COALESCE(p.[Sales Order], e.[sales_order_no]) AS NewSO,
        CASE
            WHEN c.casting_qty_to_tree IS NOT NULL AND c.casting_qty_to_tree <> 0
                THEN c.casting_qty_to_tree
            ELSE p.prod_line_quantity
        END AS New_Qty,
        ROW_NUMBER() OVER (
            PARTITION BY p.prod_order_no, p.prod_item_line
            ORDER BY p.prod_order_line_no DESC
        ) AS rn
    FROM ProdLineData p
    LEFT JOIN EmployeeData e
        ON p.prod_order_no = e.prod_order_no
       AND p.prod_order_line_no = e.prod_order_line_no
       AND p.prod_item_line = e.item_no
    LEFT JOIN CastingData c
        ON p.prod_order_no = c.prod_order_no
       AND p.prod_order_line_no = c.prod_order_line_no
    LEFT JOIN ItemData i
        ON i.item_no = p.[prod_item_line]
    LEFT JOIN Salesorder s
        ON COALESCE(p.[Sales Order], e.[sales_order_no]) = s.SalesorderNo
    -- WHERE prod_order_status IN ('Released', 'Firm Planned')
)
SELECT
    [prod_order_no],
    [prod_order_line_no],
    [FG_item_no],
    [prod_item_line],
    [item_location],
    [FG_Startdate],
    [FG_duedate],
    [Casting_Startdate],
    [Casting_duedate],
    [current_location_code],
    [machine_center_no],
    [CorrectCurrentLocation],
    [casting_prod_order],
    LTRIM(RTRIM(REPLACE([casting_tree_no], 'TREE No.', ''))) AS TreeNo,
    [itemCST],
    [casting_qty_to_tree],
    [casting_qty_passed],
    [casting_qty_reject],
    [casting_status],
    [CustomerNo],
    [CustomerAbbr],
    [MetalCategory],
    [New_Status],
    LEFT([NewSO], 2) + RIGHT([NewSO], 4) AS [SO Abbr],
    [New_Qty],
 
        -- ✅ คอลัมน์สรุปตาม DAX เดิม (คำนวณต่อใบคำสั่งผลิต)
    SUM([New_Qty]) OVER (PARTITION BY prod_order_no, prod_item_line) AS TotalQty,
    SUM(CASE
            WHEN [New_Status] IN ('Finished','complete')
                 THEN [casting_qty_to_tree]
            ELSE 0
        END
    ) OVER (PARTITION BY prod_order_no, prod_item_line) AS In_WH,
    CASE
        WHEN (SUM([New_Qty]) OVER (PARTITION BY prod_order_no, prod_item_line)
              - SUM(CASE
                        WHEN [New_Status] IN ('Complete')
                             THEN [casting_qty_to_tree]
                        ELSE 0
                    END) OVER (PARTITION BY prod_order_no, prod_item_line)
             ) = 0
            THEN NULL
        ELSE (SUM([New_Qty]) OVER (PARTITION BY prod_order_no, prod_item_line)
              - SUM(CASE
                        WHEN [New_Status] IN ('Complete')
                             THEN [casting_qty_to_tree]
                        ELSE 0
                    END) OVER (PARTITION BY [prod_order_no])
             )
    END AS RemainingQty,
    CONCAT (prod_order_no,prod_item_line) AS poi
 
FROM FinalBase
WHERE rn = 1
    AND [prod_item_line] LIKE 'C%'                 -- ✅ เฉพาะที่ขึ้นต้นด้วย C
    AND [prod_line_remaining_quantity] > 0        -- ✅ เฉพาะที่เหลือมากกว่า 0