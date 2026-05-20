-- Auto Generated (Do not modify) 5649B7ADEEE11B9F0DA81993504C8E266E8B069DEB356EB3AA1720B222AF9331
CREATE   VIEW [prod].[gold_production_asgn_cell] AS
WITH RoutingRanked AS (
    SELECT
        rl.prod_order_no,
        rl.item_no,
        rl.prod_order_lineno,
        rl.routing_no,
        ROW_NUMBER() OVER (
            PARTITION BY rl.prod_order_no, rl.prod_order_lineno
            ORDER BY
                CASE WHEN rl.routing_no = 'CELL108' THEN 1 ELSE 0 END,
                CASE WHEN TRY_CONVERT(int, STUFF(rl.routing_no, 1, 4, '')) IS NULL THEN 1 ELSE 0 END,
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
        routing_no AS cell_line
    FROM RoutingRanked
    WHERE rn = 1
),
RoutingProdLine AS (
    SELECT
        p.*,
        c.[prod_line]
    FROM Routing p
    LEFT JOIN [Silver_Production_Warehouse].[prod].[silver_cell_list] as c
           ON p.[cell_line] = c.[cell_line]
)
SELECT *
FROM RoutingProdLine;