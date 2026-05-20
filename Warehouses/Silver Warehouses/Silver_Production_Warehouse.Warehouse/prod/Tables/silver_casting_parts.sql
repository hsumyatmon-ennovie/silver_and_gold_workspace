CREATE TABLE [prod].[silver_casting_parts] (

	[versionnumber] varchar(50) NULL, 
	[entity_id] varchar(200) NULL, 
	[created_on] datetime2(6) NULL, 
	[modified_on] datetime2(6) NULL, 
	[prod_order_no] varchar(8000) NULL, 
	[prod_order_line_no] bigint NULL, 
	[casting_prod_order] varchar(8000) NULL, 
	[item_no] varchar(8000) NULL, 
	[casting_qty_to_tree] decimal(18,2) NULL, 
	[casting_stone_weight] decimal(18,4) NULL, 
	[casting_qty_passed] decimal(18,2) NULL, 
	[casting_qty_passed_weight] decimal(18,4) NULL, 
	[casting_qty_reject] decimal(18,2) NULL, 
	[casting_qty_reject_weight] decimal(18,4) NULL, 
	[casting_to_warehouse] varchar(8000) NULL, 
	[casting_warehouse_lot] varchar(8000) NULL, 
	[casting_batch_no] varchar(8000) NULL, 
	[sink_modified_on] datetime2(6) NULL
);