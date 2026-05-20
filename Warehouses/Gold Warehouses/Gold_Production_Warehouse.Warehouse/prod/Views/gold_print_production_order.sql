-- Auto Generated (Do not modify) 9DB0177BA5EBF0EF2ABAE51E50442C7E3AE7412C59E27590F04F21E327305934
CREATE     VIEW [prod].[gold_print_production_order] AS (SELECT [prodorder_status],
            [prodorder_no],
            [prodorder_lineno],
            [prodorder_start_date],
            [prodorder_due_date],
            [salesorder_no],
            [item_no],
            [prodorder_print],
            CONCAT(prodorder_no,item_no) AS poi
FROM [Gold_Production_Warehouse].[prod].[gold_print_productionorder_casting]
WHERE prodorder_status = 'Released')