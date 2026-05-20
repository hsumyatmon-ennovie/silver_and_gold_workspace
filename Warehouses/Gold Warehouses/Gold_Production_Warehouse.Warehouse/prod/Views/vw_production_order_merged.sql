-- Auto Generated (Do not modify) 2526CBE03CFE7C7A19505DE3DCF79CD88D5E33C26B55AD09D22835277AE5BFC4
CREATE   VIEW prod.vw_production_order_merged
AS
/* --------------------------
   CTE 1: Production orders + lines (your first view’s logic)
---------------------------*/
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
        DATEPART(WEEK, po.prod_order_due_date) AS [Commit Week],
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
        CONCAT(po.prod_order_no, pl.prod_order_line_no) AS POL,
        CASE
            WHEN po.prod_order_due_date IS NULL THEN NULL
            WHEN DATEDIFF(DAY, CAST(GETDATE() AS date), po.prod_order_due_date) < 0
                THEN CONCAT('Overdue ', ABS(DATEDIFF(DAY, CAST(GETDATE() AS date), po.prod_order_due_date)), 'd')
            ELSE CONCAT('Due ', DATEDIFF(DAY, CAST(GETDATE() AS date), po.prod_order_due_date), 'd')
        END AS [Due In (PO)],
        CASE
            WHEN po.prod_order_due_date IS NULL THEN NULL
            WHEN DATEDIFF(DAY, CAST(GETDATE() AS date), po.prod_order_due_date) < 0 THEN 'Overdue'
            WHEN DATEDIFF(DAY, CAST(GETDATE() AS date), po.prod_order_due_date) <= 3 THEN 'At risk'
            ELSE 'On time'
        END AS [Due Status (PO)]
    FROM [Silver_Production_Warehouse].[prod].[silver_production_order] AS po
    LEFT JOIN [Silver_Production_Warehouse].[prod].[silver_production_order_line] AS pl
        ON po.prod_order_no = pl.prod_order_no
),

/* --------------------------
   CTE 2: Line-level aggregation for completion % by prod_order_no
---------------------------*/
ps AS (
    SELECT
        p.prod_order_no,
        p.item_no,
        p.item_uom,
        SUM(CASE WHEN LOWER(p.item_location) IN ('casting','cst_cut') THEN COALESCE(p.prod_line_quantity,0)            ELSE 0 END) AS total_casting_qty,
        SUM(CASE WHEN LOWER(p.item_location) IN ('casting','cst_cut') THEN COALESCE(p.prod_line_finished_quantity,0)   ELSE 0 END) AS total_casting_finished_qty,
        SUM(CASE WHEN LOWER(p.item_location) = 'fin-goods'       THEN COALESCE(p.prod_line_quantity,0)                 ELSE 0 END) AS total_production_qty,
        SUM(CASE WHEN LOWER(p.item_location) = 'fin-goods'       THEN COALESCE(p.prod_line_finished_quantity,0)        ELSE 0 END) AS total_production_finished_qty,
        SUM(CASE WHEN LOWER(p.item_location) NOT IN ('casting','fin-goods','cst_cut') THEN COALESCE(p.prod_line_quantity,0)          ELSE 0 END) AS total_semi_qty,
        SUM(CASE WHEN LOWER(p.item_location) NOT IN ('casting','fin-goods','cst_cut') THEN COALESCE(p.prod_line_finished_quantity,0) ELSE 0 END) AS total_semi_finished_qty
    FROM [Silver_Production_Warehouse].[prod].[silver_production_order_line] AS p
    WHERE p.item_no NOT LIKE 'M%'
      AND p.item_uom = 'PCS'
    GROUP BY p.prod_order_no, p.item_no, p.item_uom
),

/* --------------------------
   CTE 3: Count prod orders per SO Abbr (from shipment plan)
---------------------------*/
so_prod_counts AS (
    SELECT
        gp.[SO Abbr],
        COUNT(DISTINCT gp.Prod) AS total_prod_orders
    FROM [Gold_Production_Warehouse].[prod].[gold_shipment_plan_status] AS gp
    WHERE gp.Prod IS NOT NULL
    GROUP BY gp.[SO Abbr]
)

/* --------------------------
   Final: Merge everything
   - Join shipment plan (gp) to prod order (po) via prod_order_no
   - Add completion metrics from ps and counts from spc
---------------------------*/
SELECT DISTINCT
    -- Shipment plan context
    gp.[SO Abbr],
    gp.[Customer],
    gp.[Due In]      AS [Due In (GP)],
    gp.[Due Status]  AS [Due Status (GP)],
    spc.total_prod_orders,

    -- Completion metrics (overall & breakdown)
    CAST((
        (
          CASE WHEN ps.total_casting_qty     IS NULL OR ps.total_casting_qty     = 0 THEN 0 ELSE CAST(CEILING(((CAST(ps.total_casting_finished_qty     AS DECIMAL(18,4)) / NULLIF(ps.total_casting_qty,     0)) * 100) * 100) / 100 AS DECIMAL(10,2)) END
        + CASE WHEN ps.total_production_qty  IS NULL OR ps.total_production_qty  = 0 THEN 0 ELSE CAST(CEILING(((CAST(ps.total_production_finished_qty  AS DECIMAL(18,4)) / NULLIF(ps.total_production_qty,  0)) * 100) * 100) / 100 AS DECIMAL(10,2)) END
        + CASE WHEN ps.total_semi_qty        IS NULL OR ps.total_semi_qty        = 0 THEN 0 ELSE CAST(CEILING(((CAST(ps.total_semi_finished_qty        AS DECIMAL(18,4)) / NULLIF(ps.total_semi_qty,        0)) * 100) * 100) / 100 AS DECIMAL(10,2)) END
        ) / 3
    ) AS DECIMAL(10,2)) AS [Completion %],

    CASE WHEN ps.total_casting_qty    IS NULL OR ps.total_casting_qty    = 0 THEN 0 ELSE CAST(CEILING(((CAST(ps.total_casting_finished_qty    AS DECIMAL(18,4)) / NULLIF(ps.total_casting_qty,    0)) * 100) * 100) / 100 AS DECIMAL(10,2)) END AS casting_percent,
    CASE WHEN ps.total_production_qty IS NULL OR ps.total_production_qty = 0 THEN 0 ELSE CAST(CEILING(((CAST(ps.total_production_finished_qty AS DECIMAL(18,4)) / NULLIF(ps.total_production_qty, 0)) * 100) * 100) / 100 AS DECIMAL(10,2)) END AS production_percent,
    CASE WHEN ps.total_semi_qty       IS NULL OR ps.total_semi_qty       = 0 THEN 0 ELSE CAST(CEILING(((CAST(ps.total_semi_finished_qty       AS DECIMAL(18,4)) / NULLIF(ps.total_semi_qty,       0)) * 100) * 100) / 100 AS DECIMAL(10,2)) END AS semi_percent,

    -- Production order detail (from first view)
    po.sales_order_no,
    po.sales_order_line_no,
    po.prod_order_no,
    po.prod_order_line_no,
    po.FG_item_no,
    po.item_routing_no,
    po.prod_order_starting_date_time,
    po.prod_order_ending_date_time,
    po.prod_order_due_date,
    po.[Commit Week],
    po.prod_line_due_date,
    po.prod_order_finished_date,
    po.prod_order_quantity,
    po.prod_order_status,
    po.ref_prod_order,
    po.ref_item,
    po.prod_line_start_date,
    po.prod_line_end_date,
    po.prod_line_quantity,
    po.prod_line_finished_quantity,
    po.prod_line_remaining_quantity,
    po.item_location,
    po.prod_item_line,
    po.SOL,
    po.POL,
    po.[Due In (PO)],
    po.[Due Status (PO)]
FROM [Gold_Production_Warehouse].[prod].[gold_shipment_plan_status] AS gp
LEFT JOIN ProdOrder AS po
    ON po.prod_order_no = gp.Prod               -- link GP to production order
LEFT JOIN ps
    ON ps.prod_order_no = gp.Prod               -- completion per prod order
LEFT JOIN so_prod_counts AS spc
    ON spc.[SO Abbr] = gp.[SO Abbr];