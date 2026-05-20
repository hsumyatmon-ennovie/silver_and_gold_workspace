-- Auto Generated (Do not modify) 716129D36FD17552F6CE03F0B04E190F29A417C27189C5D97F6D32F8AA75FCA4
CREATE   VIEW prod.testing2 AS
WITH ps AS (
    SELECT
        p.prod_order_no,
        p.item_no,
        p.item_uom,
        SUM(CASE WHEN LOWER(p.item_location) IN ('casting','cst_cut') THEN COALESCE(p.prod_line_quantity,0) ELSE 0 END) AS total_casting_qty,
        SUM(CASE WHEN LOWER(p.item_location) IN ('casting','cst_cut') THEN COALESCE(p.prod_line_finished_quantity,0) ELSE 0 END) AS total_casting_finished_qty,
        SUM(CASE WHEN LOWER(p.item_location) = 'fin-goods' THEN COALESCE(p.prod_line_quantity,0) ELSE 0 END) AS total_production_qty,
        SUM(CASE WHEN LOWER(p.item_location) = 'fin-goods' THEN COALESCE(p.prod_line_finished_quantity,0) ELSE 0 END) AS total_production_finished_qty,
        SUM(CASE WHEN LOWER(p.item_location) NOT IN ('casting','fin-goods','cst_cut') THEN COALESCE(p.prod_line_quantity,0) ELSE 0 END) AS total_semi_qty,
        SUM(CASE WHEN LOWER(p.item_location) NOT IN ('casting','fin-goods','cst_cut') THEN COALESCE(p.prod_line_finished_quantity,0) ELSE 0 END) AS total_semi_finished_qty
    FROM Silver_Production_Warehouse.prod.silver_production_order_line AS p
    WHERE p.item_no NOT LIKE 'M%'
    AND p.item_uom = 'PCS'
    GROUP BY p.prod_order_no, p.item_no, p.item_uom
),
so_prod_counts AS (  -- << counts prod orders per SO
    SELECT
        gp.[SO Abbr],
        COUNT(DISTINCT gp.Prod) AS total_prod_orders
    FROM Gold_Production_Warehouse.prod.gold_shipment_plan_status AS gp
    WHERE gp.Prod IS NOT NULL
    GROUP BY gp.[SO Abbr]
)
SELECT DISTINCT
    gp.[SO Abbr],
    gp.[Customer],
    gp.[Due In],
    gp.[Due Status],
    gp.[Prod],
    spc.total_prod_orders,   -- << here it is

    CAST((
        (
          CASE WHEN ps.total_casting_qty     IS NULL OR ps.total_casting_qty     = 0 THEN 0 ELSE CAST(CEILING(((CAST(ps.total_casting_finished_qty     AS DECIMAL(18,4)) / NULLIF(ps.total_casting_qty,     0)) * 100) * 100) / 100 AS DECIMAL(10,2)) END
        + CASE WHEN ps.total_production_qty  IS NULL OR ps.total_production_qty  = 0 THEN 0 ELSE CAST(CEILING(((CAST(ps.total_production_finished_qty  AS DECIMAL(18,4)) / NULLIF(ps.total_production_qty,  0)) * 100) * 100) / 100 AS DECIMAL(10,2)) END
        + CASE WHEN ps.total_semi_qty        IS NULL OR ps.total_semi_qty        = 0 THEN 0 ELSE CAST(CEILING(((CAST(ps.total_semi_finished_qty        AS DECIMAL(18,4)) / NULLIF(ps.total_semi_qty,        0)) * 100) * 100) / 100 AS DECIMAL(10,2)) END
        ) / 3
    ) AS DECIMAL(10,2)) AS [Completion],

    CASE WHEN ps.total_casting_qty    IS NULL OR ps.total_casting_qty    = 0 THEN 0 ELSE CAST(CEILING(((CAST(ps.total_casting_finished_qty    AS DECIMAL(18,4)) / NULLIF(ps.total_casting_qty,    0)) * 100) * 100) / 100 AS DECIMAL(10,2)) END AS casting_percent,
    CASE WHEN ps.total_production_qty IS NULL OR ps.total_production_qty = 0 THEN 0 ELSE CAST(CEILING(((CAST(ps.total_production_finished_qty AS DECIMAL(18,4)) / NULLIF(ps.total_production_qty, 0)) * 100) * 100) / 100 AS DECIMAL(10,2)) END AS production_percent,
    CASE WHEN ps.total_semi_qty       IS NULL OR ps.total_semi_qty       = 0 THEN 0 ELSE CAST(CEILING(((CAST(ps.total_semi_finished_qty       AS DECIMAL(18,4)) / NULLIF(ps.total_semi_qty,       0)) * 100) * 100) / 100 AS DECIMAL(10,2)) END AS semi_percent

FROM ps
LEFT JOIN Gold_Production_Warehouse.prod.gold_shipment_plan_status AS gp ON gp.Prod = ps.prod_order_no 
LEFT JOIN so_prod_counts AS spc ON spc.[SO Abbr] = gp.[SO Abbr]
GROUP BY
    gp.[SO Abbr], gp.[Customer], gp.[Due In], gp.[Due Status], gp.[Prod],
    spc.total_prod_orders,
    ps.total_casting_qty, ps.total_casting_finished_qty,
    ps.total_production_qty, ps.total_production_finished_qty,
    ps.total_semi_qty, ps.total_semi_finished_qty;