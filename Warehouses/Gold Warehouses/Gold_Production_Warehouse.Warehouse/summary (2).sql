CREATE or ALTER VIEW prod.testing AS
SELECT 
    gp.[SO Abbr], 
    gp.[Customer], 
    gp.[Due In], 
    gp.[Due Status],
    -- Casting %
    CAST(
        CASE 
            WHEN gp.[Total QTY] IS NULL OR gp.[Total QTY] = 0 THEN 0
            ELSE CAST(CEILING(((CAST(gp.[CAS Qty] AS DECIMAL(10,2)) / gp.[Total QTY]) * 100) * 100) / 100 AS DECIMAL(10,2))
        END AS VARCHAR(10)
    ) + '%' AS Casting,
    
    -- Production %
    CAST(
        CASE 
            WHEN gp.[Total QTY] IS NULL OR gp.[Total QTY] = 0 THEN 0
            ELSE CAST(CEILING(((CAST(gp.[Prod Qty] AS DECIMAL(10,2)) / gp.[Total QTY]) * 100) * 100) / 100 AS DECIMAL(10,2))
        END AS VARCHAR(10)
    ) + '%' AS Production,
    
    -- Average %
    CAST(
        CASE 
            WHEN gp.[Total QTY] IS NULL OR gp.[Total QTY] = 0 THEN 0
            ELSE CAST(
                CEILING(
                    (((CAST(gp.[CAS Qty] AS DECIMAL(10,2)) / gp.[Total QTY]) * 100) + 
                     ((CAST(gp.[Prod Qty] AS DECIMAL(10,2)) / gp.[Total QTY]) * 100)) / 2 * 100
                ) / 100 AS DECIMAL(10,2)
            )
        END AS VARCHAR(10)
    ) + '%' AS [Completetion]
FROM Gold_Production_Warehouse.prod.gold_shipment_plan_status as gp
LEFT JOIN Silver_Production_Warehouse.prod.silver_production_status as p on p.prod_order_no = gp.Prod;
