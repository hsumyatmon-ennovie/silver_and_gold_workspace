-- Auto Generated (Do not modify) 3F8F33159902CD15297D23FA3F507936EA1CF2EC5AE2463B05E8B0C540C742F0
CREATE   VIEW [prod].[gold_remaining_by_operation_group]
AS
WITH LatestPerPOL AS (
    -- Source: your compiled/filtered production + latest status view
    SELECT
        gpcs.prod_order_no,
        gpcs.prod_order_line_no,
        -- Operation “code” derived in your view (machine/loc normalized)
        LTRIM(RTRIM(gpcs.[Prod Status]))         AS operation_code,
        -- Prefer line-level remaining qty; fall back to status remaining if ever needed
        CAST(gpcs.prod_line_remaining_quantity AS BIGINT) AS remaining_qty
        -- If you also want a fallback, uncomment the COALESCE line below and remove the CAST above:
        -- COALESCE(CAST(gpcs.prod_line_remaining_quantity AS BIGINT), CAST(gps.remaining_quantity AS BIGINT), 0) AS remaining_qty
    FROM [prod].[gold_production_casting_status] AS gpcs
    -- NOTE: gpcs already reflects rn=1 / In location in / open=Yes / Released via its inner CTEs
),
OpMap AS (
    -- Map normalized op code to operation group
    SELECT
        sc.operation_group
    FROM [Gold_Commons_Warehouse].[cmn].[gold_prod_step_casting_production] AS sc
)
SELECT
    m.operation_group,
    SUM(COALESCE(l.remaining_qty, 0)) AS total_remaining_quantity,
    COUNT(DISTINCT CONCAT(l.prod_order_no, '-', l.prod_order_line_no)) AS open_pol_count
FROM LatestPerPOL AS l
INNER JOIN OpMap AS m
    ON m.operation_group = l.operation_code
GROUP BY
    m.operation_group;