CREATE TABLE [prod].[silver_routing_lines] (

	[versionnumber] varchar(50) NULL, 
	[entity_id] varchar(200) NULL, 
	[created_on] datetime2(6) NULL, 
	[modified_on] datetime2(6) NULL, 
	[prod_order_no] varchar(8000) NULL, 
	[prod_order_status] varchar(8000) NULL, 
	[prod_order_lineno] varchar(8000) NULL, 
	[item_no] varchar(8000) NULL, 
	[routing_link_code] varchar(8000) NULL, 
	[location_code] varchar(8000) NULL, 
	[previous_operation_no] varchar(8000) NULL, 
	[next_operation_no] varchar(8000) NULL, 
	[routing_status] varchar(8000) NULL, 
	[operation_no] varchar(8000) NULL, 
	[type_name] varchar(8000) NULL, 
	[routing_no] varchar(8000) NULL, 
	[run_time] decimal(18,4) NULL, 
	[starting_date_time] datetime2(6) NULL, 
	[ending_date_time] datetime2(6) NULL, 
	[sink_modified_on] datetime2(6) NULL
);