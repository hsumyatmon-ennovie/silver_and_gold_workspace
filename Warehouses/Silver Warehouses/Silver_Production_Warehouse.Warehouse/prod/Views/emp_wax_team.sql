-- Auto Generated (Do not modify) 34724425D082D0A44CD97CEE201C7A9D4C593CBEE3074A35C88C23E78E973BCF
CREATE   VIEW [prod].[emp_wax_team]
AS
WITH emp_map AS (
    SELECT 
        [cr535_employeeid],
        [cr535_employeename],
        [cr535_lastname],
        [cr535_antenna],
        [cr535_cellno],
        [cr535_machinecenterno],
        [cr535_readerid],
        CONCAT(cr535_antenna, cr535_readerid) AS pKey
    FROM [Silver_Production_Warehouse].[prod].[silver_employee_rfid_mapping] AS e
),
wax_team AS (
    SELECT 
        [team],
        [reader_id],
        [antenna],
        CONCAT(antenna, reader_id) AS pKey
    FROM [Silver_Production_Warehouse].[prod].[silver_wax_team]
)
SELECT 
    e.cr535_employeeid,
    e.cr535_employeename,
    e.cr535_lastname,
    e.cr535_machinecenterno,
    e.cr535_cellno,
    w.team,
    e.cr535_readerid,
    e.cr535_antenna,
    CONCAT(e.cr535_machinecenterno,e.cr535_cellno,e.cr535_antenna) AS wKey
FROM emp_map e
LEFT JOIN wax_team w
    ON e.pKey = w.pKey
-- WHERE e.cr535_cellno = 'WAX ROOM';