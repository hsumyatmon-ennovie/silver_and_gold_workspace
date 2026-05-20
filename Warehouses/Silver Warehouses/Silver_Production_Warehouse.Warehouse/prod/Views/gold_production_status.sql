-- Auto Generated (Do not modify) C55C8077E98E0E24EAAF4C73FD88B743589B150794C3607A04620D02D4B748A7
CREATE   VIEW [prod].[gold_production_status] AS WITH
    DataWithCalc AS (
        SELECT
            created_on,
            modified_on,
            prod_order_no,
            CAST (prod_order_line_no AS VARCHAR (255))
            AS prod_order_line_no,
            job_sheet_no,
            type_name,
            prod_order_status,
            [open],
            sales_order_no,
            current_location_code,
            past_location_code,
            employee_no,
            user_id,
            date_out,
            quantity,
            date_in,
            time_in,
            remaining_quantity,
            operation_no,
            item_no,
            entry_no_auto_no,
            machine_center_no,
            -- OUT Qty
            CAST (-1 * quantity AS BIGINT)
            AS [out_qty],
            -- pol: prod_order_no + prod_order_line_no
            CONCAT (prod_order_no , CAST (prod_order_line_no AS VARCHAR (255)))
            AS [pol],
            -- created_on_time: ONLY TIME +7
            created_on AS [created_on_time],
            -- CELL: map email → CELLXXX
            CASE WHEN user_id = 'cell101@ennovie.com' THEN 'CELL101' WHEN user_id = 'cell102@ennovie.com' THEN 'CELL102' WHEN user_id = 'cell103@ennovie.com' THEN 'CELL103' WHEN user_id = 'cell106@ennovie.com' THEN 'CELL106' WHEN user_id = 'cell108@ennovie.com' THEN 'CELL108' WHEN user_id = 'cell201@ennovie.com' THEN 'CELL201' WHEN user_id = 'cell202@ennovie.com' THEN 'CELL202' WHEN user_id = 'cell203@ennovie.com' THEN 'CELL203' WHEN user_id = 'cell204@ennovie.com' THEN 'CELL204' WHEN user_id = 'cell206@ennovie.com' THEN 'CELL206' WHEN user_id = 'cell207@ennovie.com' THEN 'CELL207' WHEN user_id = 'cell208@ennovie.com' THEN 'CELL208' WHEN user_id = 'cell209@ennovie.com' THEN 'CELL209' WHEN user_id = 'cell210@ennovie.com' THEN 'CELL210' WHEN user_id = 'cell211@ennovie.com' THEN 'CELL211' WHEN user_id = 'cell212@ennovie.com' THEN 'CELL212' WHEN user_id = 'cell213@ennovie.com' THEN 'CELL213' WHEN user_id = 'cell214@ennovie.com' THEN 'CELL214' WHEN user_id = 'cell215@ennovie.com' THEN 'CELL215' WHEN user_id = 'cell216@ennovie.com' THEN 'CELL216' WHEN user_id = 'cell217@ennovie.com' THEN 'CELL217' WHEN user_id = 'cell219@ennovie.com' THEN 'CELL219' WHEN user_id = 'cell220@ennovie.com' THEN 'CELL220' WHEN user_id = 'cell205@ennovie.com' THEN 'CELL205' WHEN user_id = 'sample1st@ennovie.com' THEN 'CELL106' WHEN user_id = 'cell107@ennovie.com' THEN 'CELL107' ELSE user_id END
            AS [cell]
        FROM [PROD_Silver_Warehouse].[prod].[silver_production_status]
    ),
    DedupData AS (
        SELECT
            *,
            ROW_NUMBER () OVER (PARTITION BY prod_order_no , item_no , current_location_code , machine_center_no , type_name ORDER BY created_on DESC)
            AS rn
        FROM DataWithCalc
    )
SELECT
    created_on,
    modified_on,
    prod_order_no,
    prod_order_line_no,
    job_sheet_no,
    type_name,
    prod_order_status,
    [open],
    sales_order_no,
    current_location_code,
    past_location_code,
    employee_no,
    user_id,
    date_out,
    quantity,
    date_in,
    time_in,
    remaining_quantity,
    operation_no,
    item_no,
    entry_no_auto_no,
    machine_center_no,
    out_qty,
    pol,
    created_on_time,
    cell
FROM DedupData WHERE
    rn = 1;