-- Auto Generated (Do not modify) 4E59A2091BF4D16821A2CC1108E6A0288CC273BAF8219E13A801193DE88FEAA4
CREATE       VIEW [prod].[gold_production_status] AS WITH
    DataWithCalc AS (
        SELECT
            created_on,
            modified_on,
            prod_order_no,
            CAST (prod_order_line_no AS VARCHAR (255)) AS prod_order_line_no,
            type_name,
            prod_order_status,
            operation_no,
            [open],
            sales_order_no,
            current_location_code,
            past_location_code,
            employee_no,
            user_id,
            quantity,
            remaining_quantity,
            item_no,
            machine_center_no,
-- OUT Qty
            CAST (-1 * quantity AS BIGINT)  AS [out_qty],
-- pol: prod_order_no + prod_order_line_no
            CONCAT (prod_order_no , CAST (prod_order_line_no AS VARCHAR (255))) AS [pol],
-- created_on_time: ONLY TIME +7
            created_on AS [created_on_time],
CASE
    WHEN NULLIF(LTRIM(RTRIM([current_location_code])), '') IS NULL
        THEN COALESCE(
            NULLIF(LTRIM(RTRIM([machine_center_no])), ''), LTRIM(RTRIM([current_location_code]))
        )
    WHEN LEFT(UPPER(LTRIM(RTRIM([current_location_code]))), 4) = 'CELL'
        THEN COALESCE(
            NULLIF(LTRIM(RTRIM([machine_center_no])), ''), LTRIM(RTRIM([current_location_code]))
        )
    ELSE LTRIM(RTRIM([current_location_code]))
END AS [CorrectCurrentLocation]
 
        FROM [Silver_Production_Warehouse].[prod].[silver_production_status]
    ),
    DedupData AS (
        SELECT
            *,
            ROW_NUMBER () OVER (PARTITION BY prod_order_no , item_no , [CorrectCurrentLocation] , type_name
            ORDER BY created_on DESC)
            AS rn
        FROM DataWithCalc
    )
SELECT
    created_on,
    modified_on,
    prod_order_no,
    prod_order_line_no,
    type_name,
    operation_no,
    prod_order_status,
    [open],
    sales_order_no,
    current_location_code,
    past_location_code,
    employee_no,
    user_id,
    quantity,
    remaining_quantity,
    item_no,
    machine_center_no,
    out_qty,
    pol,
    created_on_time,
[CorrectCurrentLocation]
 
FROM DedupData
WHERE    rn = 1
AND [type_name] = 'In location in'
AND [open] = 'Yes'
AND [prod_order_status] = 'Released';