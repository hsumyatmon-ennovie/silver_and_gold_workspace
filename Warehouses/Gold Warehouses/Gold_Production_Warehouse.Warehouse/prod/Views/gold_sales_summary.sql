-- Auto Generated (Do not modify) 636EEEE3C4C06848241FD992A327C5DCE42D18F3574EC587F0BF2B7ECEEFDEAC
CREATE   VIEW prod.gold_sales_summary
AS
WITH ps AS (
    SELECT
        p.prod_order_no,
        p.item_no,
        p.item_uom,
        SUM(CASE WHEN LOWER(p.item_location) IN ('casting','cst_cut') 
                 THEN COALESCE(p.prod_line_quantity,0) ELSE 0 END) AS total_casting_qty,
        SUM(CASE WHEN LOWER(p.item_location) IN ('casting','cst_cut') 
                 THEN COALESCE(p.prod_line_finished_quantity,0) ELSE 0 END) AS total_casting_finished_qty,
        SUM(CASE WHEN LOWER(p.item_location) = 'fin-goods' 
                 THEN COALESCE(p.prod_line_quantity,0) ELSE 0 END) AS total_production_qty,
        SUM(CASE WHEN LOWER(p.item_location) = 'fin-goods' 
                 THEN COALESCE(p.prod_line_finished_quantity,0) ELSE 0 END) AS total_production_finished_qty,
        SUM(CASE WHEN LOWER(p.item_location) NOT IN ('casting','fin-goods','cst_cut') 
                 THEN COALESCE(p.prod_line_quantity,0) ELSE 0 END) AS total_semi_qty,
        SUM(CASE WHEN LOWER(p.item_location) NOT IN ('casting','fin-goods','cst_cut') 
                 THEN COALESCE(p.prod_line_finished_quantity,0) ELSE 0 END) AS total_semi_finished_qty
    FROM Silver_Production_Warehouse.prod.silver_production_order_line AS p
    WHERE p.item_no NOT LIKE 'M%'    -- exclude molds/materials?
      AND p.item_uom = 'PCS'
    GROUP BY p.prod_order_no, p.item_no, p.item_uom
),
so_prod_counts AS (
    SELECT
        gp.[SO Abbr],
        COUNT(DISTINCT gp.Prod) AS total_prod_orders
    FROM Gold_Production_Warehouse.prod.gold_shipment_plan_status AS gp
    WHERE gp.Prod IS NOT NULL
    GROUP BY gp.[SO Abbr]
)
SELECT
    gp.[SO Abbr],
    gp.[Customer],
    gp.[Due In],
    gp.[Due Status],
    gp.[Prod],
    spc.total_prod_orders,

    -- Per-stage %
    CAST(ROUND(
        CASE WHEN ps.total_casting_qty = 0 THEN 0 
             ELSE (TRY_CONVERT(decimal(12,4), ps.total_casting_finished_qty) 
                  / NULLIF(ps.total_casting_qty,0)) * 100 END, 2) AS decimal(10,2)) AS [Casting],

    CAST(ROUND(
        CASE WHEN ps.total_production_qty = 0 THEN 0 
             ELSE (TRY_CONVERT(decimal(12,4), ps.total_production_finished_qty) 
                  / NULLIF(ps.total_production_qty,0)) * 100 END, 2) AS decimal(10,2)) AS [Production],

    CAST(ROUND(
        CASE WHEN ps.total_semi_qty = 0 THEN 0 
             ELSE (TRY_CONVERT(decimal(12,4), ps.total_semi_finished_qty) 
                  / NULLIF(ps.total_semi_qty,0)) * 100 END, 2) AS decimal(10,2)) AS [Semi],

    -- Overall completion = mean of the three stage %s
    CAST(ROUND((
        (
            CASE WHEN ps.total_casting_qty = 0 THEN 0 
                 ELSE (TRY_CONVERT(decimal(12,4), ps.total_casting_finished_qty) 
                      / NULLIF(ps.total_casting_qty,0)) * 100 END
          + CASE WHEN ps.total_production_qty = 0 THEN 0 
                 ELSE (TRY_CONVERT(decimal(12,4), ps.total_production_finished_qty) 
                      / NULLIF(ps.total_production_qty,0)) * 100 END
          + CASE WHEN ps.total_semi_qty = 0 THEN 0 
                 ELSE (TRY_CONVERT(decimal(12,4), ps.total_semi_finished_qty) 
                      / NULLIF(ps.total_semi_qty,0)) * 100 END
        ) / 3.0
    ), 2) AS decimal(10,2)) AS [Completion]
FROM ps
LEFT JOIN Gold_Production_Warehouse.prod.gold_shipment_plan_status AS gp
    ON gp.Prod = ps.prod_order_no
LEFT JOIN so_prod_counts AS spc
    ON spc.[SO Abbr] = gp.[SO Abbr]
GROUP BY
    gp.[SO Abbr], gp.[Customer], gp.[Due In], gp.[Due Status], gp.[Prod], spc.total_prod_orders,
    ps.total_casting_qty, ps.total_casting_finished_qty,
    ps.total_production_qty, ps.total_production_finished_qty,
    ps.total_semi_qty, ps.total_semi_finished_qty;