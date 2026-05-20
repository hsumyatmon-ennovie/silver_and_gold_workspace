# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "warehouse": {
# META       "default_warehouse": "e5cdc0c7-6c3a-46d5-8bb9-65942390419d",
# META       "known_warehouses": [
# META         {
# META           "id": "e5cdc0c7-6c3a-46d5-8bb9-65942390419d",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.planning.gold_scheduling_customer_rule
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     customer_no, material_type, cell_code, priority_rank,
# MAGIC     CASE
# MAGIC         WHEN cell_code IN ('CELL105','CELL109')                                         THEN 'PRODLINE1'
# MAGIC         WHEN cell_code IN ('CELL201','CELL202','CELL203','CELL204','CELL205','CELL218') THEN 'PRODLINE2'
# MAGIC         WHEN cell_code IN ('CELL103','CELL104','CELL206','CELL207','CELL208','CELL209','CELL210','CELL219','CELL220') THEN 'PRODLINE3'
# MAGIC         WHEN cell_code IN ('CELL211','CELL212','CELL213')                               THEN 'PRODLINE4'
# MAGIC         WHEN cell_code IN ('CELL214','CELL215','CELL216','CELL217')                     THEN 'PRODLINE5'
# MAGIC         ELSE 'UNKNOWN'
# MAGIC     END AS production_line,
# MAGIC     current_timestamp() AS _load_timestamp
# MAGIC FROM VALUES
# MAGIC         -- Bangkok Kraft Production
# MAGIC         ('CD-00003', 'gold', 'CELL103', 1),
# MAGIC         ('CD-00003', 'gold', 'CELL109', 2),
# MAGIC         ('CD-00003', 'silver', 'CELL211', 1),
# MAGIC         ('CD-00003', 'silver', 'CELL212', 2),
# MAGIC         ('CD-00003', 'silver', 'CELL213', 3),
# MAGIC         ('CD-00003', 'silver', 'CELL214', 4),
# MAGIC         ('CD-00003', 'silver', 'CELL215', 5),
# MAGIC         ('CD-00003', 'silver', 'CELL216', 6),
# MAGIC         ('CD-00003', 'silver', 'CELL217', 7),
# MAGIC         ('CD-00003', 'silver', 'CELL219', 8),
# MAGIC         ('CD-00003', 'silver', 'CELL206', 9),
# MAGIC         ('CD-00003', 'silver', 'CELL207', 10),
# MAGIC         ('CD-00003', 'silver', 'CELL109', 11),
# MAGIC         ('CD-00003', 'bangle', 'CELL211', 1),
# MAGIC         ('CD-00003', 'bangle', 'CELL212', 2),
# MAGIC         ('CD-00003', 'bangle', 'CELL213', 3),
# MAGIC         ('CD-00003', 'bangle', 'CELL206', 4),
# MAGIC         ('CD-00003', 'bangle', 'CELL207', 5),
# MAGIC         ('CD-00003', 'bangle', 'CELL214', 6),
# MAGIC         ('CD-00003', 'bangle', 'CELL215', 7),
# MAGIC         ('CD-00003', 'bangle', 'CELL216', 8),
# MAGIC         ('CD-00003', 'bangle', 'CELL217', 9),
# MAGIC         ('CD-00003', 'bangle', 'CELL109', 10),
# MAGIC         ('CD-00003', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CD-00003', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CD-00003', 'bangle-lock', 'CELL210', 3),
# MAGIC         ('CD-00003', 'bangle-lock', 'CELL208', 4),
# MAGIC         ('CD-00003', 'bangle-lock', 'CELL109', 5),
# MAGIC         -- ENNOVIE
# MAGIC         ('CD-00004', 'gold', 'CELL103', 1),
# MAGIC         ('CD-00004', 'silver', 'CELL205', 1),
# MAGIC         ('CD-00004', 'silver', 'CELL201', 2),
# MAGIC         ('CD-00004', 'silver', 'CELL202', 3),
# MAGIC         ('CD-00004', 'silver', 'CELL203', 4),
# MAGIC         ('CD-00004', 'silver', 'CELL204', 5),
# MAGIC         ('CD-00004', 'silver', 'CELL214', 6),
# MAGIC         ('CD-00004', 'silver', 'CELL215', 7),
# MAGIC         ('CD-00004', 'silver', 'CELL216', 8),
# MAGIC         ('CD-00004', 'silver', 'CELL217', 9),
# MAGIC         ('CD-00004', 'silver', 'CELL211', 10),
# MAGIC         ('CD-00004', 'silver', 'CELL212', 11),
# MAGIC         ('CD-00004', 'silver', 'CELL213', 12),
# MAGIC         ('CD-00004', 'silver', 'CELL206', 13),
# MAGIC         ('CD-00004', 'silver', 'CELL207', 14),
# MAGIC         ('CD-00004', 'silver', 'CELL208', 15),
# MAGIC         ('CD-00004', 'silver', 'CELL209', 16),
# MAGIC         ('CD-00004', 'silver', 'CELL210', 17),
# MAGIC         ('CD-00004', 'silver', 'CELL219', 18),
# MAGIC         ('CD-00004', 'silver', 'CELL109', 19),
# MAGIC         ('CD-00004', 'bangle', 'CELL201', 1),
# MAGIC         ('CD-00004', 'bangle', 'CELL202', 2),
# MAGIC         ('CD-00004', 'bangle', 'CELL203', 3),
# MAGIC         ('CD-00004', 'bangle', 'CELL204', 4),
# MAGIC         ('CD-00004', 'bangle', 'CELL205', 5),
# MAGIC         ('CD-00004', 'bangle', 'CELL214', 6),
# MAGIC         ('CD-00004', 'bangle', 'CELL215', 7),
# MAGIC         ('CD-00004', 'bangle', 'CELL216', 8),
# MAGIC         ('CD-00004', 'bangle', 'CELL217', 9),
# MAGIC         ('CD-00004', 'bangle', 'CELL109', 10),
# MAGIC         ('CD-00004', 'bangle', 'CELL211', 11),
# MAGIC         ('CD-00004', 'bangle', 'CELL212', 12),
# MAGIC         ('CD-00004', 'bangle', 'CELL213', 13),
# MAGIC         ('CD-00004', 'bangle', 'CELL206', 14),
# MAGIC         ('CD-00004', 'bangle', 'CELL207', 15),
# MAGIC         ('CD-00004', 'bangle', 'CELL208', 16),
# MAGIC         ('CD-00004', 'bangle', 'CELL209', 17),
# MAGIC         ('CD-00004', 'bangle', 'CELL210', 18),
# MAGIC         ('CD-00004', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CD-00004', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CD-00004', 'bangle-lock', 'CELL210', 3),
# MAGIC         ('CD-00004', 'bangle-lock', 'CELL208', 4),
# MAGIC         -- DE BEERS JEWELLERS
# MAGIC         ('CI-00002', 'gold', 'CELL109', 1),
# MAGIC         ('CI-00002', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00002', 'bangle-lock', 'CELL208', 2),
# MAGIC         -- GUESS EUROPE SAGL
# MAGIC         ('CI-00004', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00004', 'silver', 'CELL206', 1),
# MAGIC         ('CI-00004', 'silver', 'CELL207', 2),
# MAGIC         ('CI-00004', 'silver', 'CELL208', 3),
# MAGIC         ('CI-00004', 'silver', 'CELL209', 4),
# MAGIC         ('CI-00004', 'silver', 'CELL210', 5),
# MAGIC         ('CI-00004', 'silver', 'CELL219', 6),
# MAGIC         ('CI-00004', 'silver', 'CELL201', 7),
# MAGIC         ('CI-00004', 'silver', 'CELL202', 8),
# MAGIC         ('CI-00004', 'silver', 'CELL203', 9),
# MAGIC         ('CI-00004', 'silver', 'CELL204', 10),
# MAGIC         ('CI-00004', 'silver', 'CELL205', 11),
# MAGIC         ('CI-00004', 'silver', 'CELL214', 12),
# MAGIC         ('CI-00004', 'silver', 'CELL215', 13),
# MAGIC         ('CI-00004', 'silver', 'CELL216', 14),
# MAGIC         ('CI-00004', 'silver', 'CELL217', 15),
# MAGIC         ('CI-00004', 'silver', 'CELL211', 16),
# MAGIC         ('CI-00004', 'silver', 'CELL212', 17),
# MAGIC         ('CI-00004', 'silver', 'CELL213', 18),
# MAGIC         ('CI-00004', 'bangle', 'CELL206', 1),
# MAGIC         ('CI-00004', 'bangle', 'CELL207', 2),
# MAGIC         ('CI-00004', 'bangle', 'CELL208', 3),
# MAGIC         ('CI-00004', 'bangle', 'CELL209', 4),
# MAGIC         ('CI-00004', 'bangle', 'CELL210', 5),
# MAGIC         ('CI-00004', 'bangle', 'CELL201', 6),
# MAGIC         ('CI-00004', 'bangle', 'CELL202', 7),
# MAGIC         ('CI-00004', 'bangle', 'CELL203', 8),
# MAGIC         ('CI-00004', 'bangle', 'CELL204', 9),
# MAGIC         ('CI-00004', 'bangle', 'CELL205', 10),
# MAGIC         ('CI-00004', 'bangle', 'CELL109', 11),
# MAGIC         ('CI-00004', 'bangle', 'CELL214', 12),
# MAGIC         ('CI-00004', 'bangle', 'CELL215', 13),
# MAGIC         ('CI-00004', 'bangle', 'CELL216', 14),
# MAGIC         ('CI-00004', 'bangle', 'CELL217', 15),
# MAGIC         ('CI-00004', 'bangle', 'CELL211', 16),
# MAGIC         ('CI-00004', 'bangle', 'CELL212', 17),
# MAGIC         ('CI-00004', 'bangle', 'CELL213', 18),
# MAGIC         ('CI-00004', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00004', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00004', 'bangle-lock', 'CELL210', 3),
# MAGIC         ('CI-00004', 'bangle-lock', 'CELL208', 4),
# MAGIC         -- SA SEZANE
# MAGIC         ('CI-00008', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00008', 'silver', 'CELL214', 1),
# MAGIC         ('CI-00008', 'silver', 'CELL215', 2),
# MAGIC         ('CI-00008', 'silver', 'CELL216', 3),
# MAGIC         ('CI-00008', 'silver', 'CELL217', 4),
# MAGIC         ('CI-00008', 'silver', 'CELL201', 5),
# MAGIC         ('CI-00008', 'silver', 'CELL202', 6),
# MAGIC         ('CI-00008', 'silver', 'CELL203', 7),
# MAGIC         ('CI-00008', 'silver', 'CELL204', 8),
# MAGIC         ('CI-00008', 'silver', 'CELL205', 9),
# MAGIC         ('CI-00008', 'silver', 'CELL211', 10),
# MAGIC         ('CI-00008', 'silver', 'CELL212', 11),
# MAGIC         ('CI-00008', 'silver', 'CELL213', 12),
# MAGIC         ('CI-00008', 'silver', 'CELL206', 13),
# MAGIC         ('CI-00008', 'silver', 'CELL207', 14),
# MAGIC         ('CI-00008', 'silver', 'CELL208', 15),
# MAGIC         ('CI-00008', 'silver', 'CELL209', 16),
# MAGIC         ('CI-00008', 'silver', 'CELL210', 17),
# MAGIC         ('CI-00008', 'silver', 'CELL219', 18),
# MAGIC         ('CI-00008', 'silver', 'CELL109', 19),
# MAGIC         ('CI-00008', 'brass', 'CELL214', 1),
# MAGIC         ('CI-00008', 'brass', 'CELL215', 2),
# MAGIC         ('CI-00008', 'brass', 'CELL216', 3),
# MAGIC         ('CI-00008', 'brass', 'CELL217', 4),
# MAGIC         ('CI-00008', 'brass', 'CELL201', 5),
# MAGIC         ('CI-00008', 'brass', 'CELL202', 6),
# MAGIC         ('CI-00008', 'brass', 'CELL203', 7),
# MAGIC         ('CI-00008', 'brass', 'CELL204', 8),
# MAGIC         ('CI-00008', 'brass', 'CELL205', 9),
# MAGIC         ('CI-00008', 'brass', 'CELL211', 10),
# MAGIC         ('CI-00008', 'brass', 'CELL212', 11),
# MAGIC         ('CI-00008', 'brass', 'CELL213', 12),
# MAGIC         ('CI-00008', 'brass', 'CELL206', 13),
# MAGIC         ('CI-00008', 'brass', 'CELL207', 14),
# MAGIC         ('CI-00008', 'brass', 'CELL208', 15),
# MAGIC         ('CI-00008', 'brass', 'CELL209', 16),
# MAGIC         ('CI-00008', 'brass', 'CELL210', 17),
# MAGIC         ('CI-00008', 'brass', 'CELL219', 18),
# MAGIC         ('CI-00008', 'bangle', 'CELL214', 1),
# MAGIC         ('CI-00008', 'bangle', 'CELL215', 2),
# MAGIC         ('CI-00008', 'bangle', 'CELL216', 3),
# MAGIC         ('CI-00008', 'bangle', 'CELL217', 4),
# MAGIC         ('CI-00008', 'bangle', 'CELL201', 5),
# MAGIC         ('CI-00008', 'bangle', 'CELL202', 6),
# MAGIC         ('CI-00008', 'bangle', 'CELL203', 7),
# MAGIC         ('CI-00008', 'bangle', 'CELL204', 8),
# MAGIC         ('CI-00008', 'bangle', 'CELL205', 9),
# MAGIC         ('CI-00008', 'bangle', 'CELL103', 10),
# MAGIC         ('CI-00008', 'bangle', 'CELL211', 11),
# MAGIC         ('CI-00008', 'bangle', 'CELL212', 12),
# MAGIC         ('CI-00008', 'bangle', 'CELL213', 13),
# MAGIC         ('CI-00008', 'bangle', 'CELL206', 14),
# MAGIC         ('CI-00008', 'bangle', 'CELL207', 15),
# MAGIC         ('CI-00008', 'bangle', 'CELL208', 16),
# MAGIC         ('CI-00008', 'bangle', 'CELL209', 17),
# MAGIC         ('CI-00008', 'bangle', 'CELL210', 18),
# MAGIC         ('CI-00008', 'bangle', 'CELL109', 19),
# MAGIC         ('CI-00008', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00008', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00008', 'bangle-lock', 'CELL210', 3),
# MAGIC         -- KIMAI LTD
# MAGIC         ('CI-00009', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00009', 'bangle', 'CELL206', 1),
# MAGIC         ('CI-00009', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00009', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00009', 'bangle-lock', 'CELL210', 3),
# MAGIC         -- BTB b.v.
# MAGIC         ('CI-00013', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00013', 'silver', 'CELL208', 1),
# MAGIC         ('CI-00013', 'silver', 'CELL207', 2),
# MAGIC         ('CI-00013', 'silver', 'CELL209', 3),
# MAGIC         ('CI-00013', 'silver', 'CELL210', 4),
# MAGIC         ('CI-00013', 'silver', 'CELL219', 5),
# MAGIC         ('CI-00013', 'silver', 'CELL211', 6),
# MAGIC         ('CI-00013', 'silver', 'CELL212', 7),
# MAGIC         ('CI-00013', 'silver', 'CELL213', 8),
# MAGIC         ('CI-00013', 'silver', 'CELL214', 9),
# MAGIC         ('CI-00013', 'silver', 'CELL215', 10),
# MAGIC         ('CI-00013', 'silver', 'CELL216', 11),
# MAGIC         ('CI-00013', 'silver', 'CELL217', 12),
# MAGIC         ('CI-00013', 'bangle', 'CELL206', 1),
# MAGIC         ('CI-00013', 'bangle', 'CELL207', 2),
# MAGIC         ('CI-00013', 'bangle', 'CELL208', 3),
# MAGIC         ('CI-00013', 'bangle', 'CELL209', 4),
# MAGIC         ('CI-00013', 'bangle', 'CELL210', 5),
# MAGIC         ('CI-00013', 'bangle', 'CELL201', 6),
# MAGIC         ('CI-00013', 'bangle', 'CELL202', 7),
# MAGIC         ('CI-00013', 'bangle', 'CELL203', 8),
# MAGIC         ('CI-00013', 'bangle', 'CELL204', 9),
# MAGIC         ('CI-00013', 'bangle', 'CELL109', 10),
# MAGIC         ('CI-00013', 'bangle', 'CELL214', 11),
# MAGIC         ('CI-00013', 'bangle', 'CELL215', 12),
# MAGIC         ('CI-00013', 'bangle', 'CELL216', 13),
# MAGIC         ('CI-00013', 'bangle', 'CELL217', 14),
# MAGIC         ('CI-00013', 'bangle', 'CELL211', 15),
# MAGIC         ('CI-00013', 'bangle', 'CELL212', 16),
# MAGIC         ('CI-00013', 'bangle', 'CELL213', 17),
# MAGIC         ('CI-00013', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00013', 'bangle-lock', 'CELL208', 2),
# MAGIC         ('CI-00013', 'bangle-lock', 'CELL209', 3),
# MAGIC         ('CI-00013', 'bangle-lock', 'CELL210', 4),
# MAGIC         -- CLOCKS & COLOURS LTD
# MAGIC         ('CI-00016', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00016', 'silver', 'CELL211', 1),
# MAGIC         ('CI-00016', 'silver', 'CELL212', 2),
# MAGIC         ('CI-00016', 'silver', 'CELL213', 3),
# MAGIC         ('CI-00016', 'silver', 'CELL219', 4),
# MAGIC         ('CI-00016', 'bangle', 'CELL211', 1),
# MAGIC         ('CI-00016', 'bangle', 'CELL212', 2),
# MAGIC         ('CI-00016', 'bangle', 'CELL213', 3),
# MAGIC         ('CI-00016', 'bangle', 'CELL206', 4),
# MAGIC         ('CI-00016', 'bangle', 'CELL207', 5),
# MAGIC         ('CI-00016', 'bangle', 'CELL208', 6),
# MAGIC         ('CI-00016', 'bangle', 'CELL209', 7),
# MAGIC         ('CI-00016', 'bangle', 'CELL210', 8),
# MAGIC         ('CI-00016', 'bangle', 'CELL214', 9),
# MAGIC         ('CI-00016', 'bangle', 'CELL215', 10),
# MAGIC         ('CI-00016', 'bangle', 'CELL216', 11),
# MAGIC         ('CI-00016', 'bangle', 'CELL217', 12),
# MAGIC         ('CI-00016', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00016', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00016', 'bangle-lock', 'CELL210', 3),
# MAGIC         -- DHTG LTD
# MAGIC         ('CI-00018', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00018', 'silver', 'CELL214', 1),
# MAGIC         ('CI-00018', 'silver', 'CELL211', 2),
# MAGIC         ('CI-00018', 'silver', 'CELL212', 3),
# MAGIC         ('CI-00018', 'silver', 'CELL213', 4),
# MAGIC         ('CI-00018', 'silver', 'CELL219', 5),
# MAGIC         ('CI-00018', 'bangle', 'CELL214', 1),
# MAGIC         ('CI-00018', 'bangle', 'CELL215', 2),
# MAGIC         ('CI-00018', 'bangle', 'CELL216', 3),
# MAGIC         ('CI-00018', 'bangle', 'CELL217', 4),
# MAGIC         ('CI-00018', 'bangle', 'CELL201', 5),
# MAGIC         ('CI-00018', 'bangle', 'CELL202', 6),
# MAGIC         ('CI-00018', 'bangle', 'CELL203', 7),
# MAGIC         ('CI-00018', 'bangle', 'CELL204', 8),
# MAGIC         ('CI-00018', 'bangle', 'CELL205', 9),
# MAGIC         ('CI-00018', 'bangle', 'CELL109', 10),
# MAGIC         ('CI-00018', 'bangle', 'CELL211', 11),
# MAGIC         ('CI-00018', 'bangle', 'CELL212', 12),
# MAGIC         ('CI-00018', 'bangle', 'CELL213', 13),
# MAGIC         ('CI-00018', 'bangle', 'CELL206', 14),
# MAGIC         ('CI-00018', 'bangle', 'CELL207', 15),
# MAGIC         ('CI-00018', 'bangle', 'CELL208', 16),
# MAGIC         ('CI-00018', 'bangle', 'CELL209', 17),
# MAGIC         ('CI-00018', 'bangle', 'CELL210', 18),
# MAGIC         ('CI-00018', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00018', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00018', 'bangle-lock', 'CELL210', 3),
# MAGIC         -- MISSOMA LTD
# MAGIC         ('CI-00020', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00020', 'silver', 'CELL214', 1),
# MAGIC         ('CI-00020', 'silver', 'CELL215', 2),
# MAGIC         ('CI-00020', 'silver', 'CELL216', 3),
# MAGIC         ('CI-00020', 'silver', 'CELL217', 4),
# MAGIC         ('CI-00020', 'silver', 'CELL211', 5),
# MAGIC         ('CI-00020', 'silver', 'CELL212', 6),
# MAGIC         ('CI-00020', 'silver', 'CELL213', 7),
# MAGIC         ('CI-00020', 'silver', 'CELL201', 8),
# MAGIC         ('CI-00020', 'silver', 'CELL202', 9),
# MAGIC         ('CI-00020', 'silver', 'CELL203', 10),
# MAGIC         ('CI-00020', 'silver', 'CELL204', 11),
# MAGIC         ('CI-00020', 'silver', 'CELL205', 12),
# MAGIC         ('CI-00020', 'silver', 'CELL206', 13),
# MAGIC         ('CI-00020', 'silver', 'CELL207', 14),
# MAGIC         ('CI-00020', 'silver', 'CELL208', 15),
# MAGIC         ('CI-00020', 'silver', 'CELL209', 16),
# MAGIC         ('CI-00020', 'silver', 'CELL210', 17),
# MAGIC         ('CI-00020', 'silver', 'CELL219', 18),
# MAGIC         ('CI-00020', 'brass', 'CELL214', 1),
# MAGIC         ('CI-00020', 'brass', 'CELL215', 2),
# MAGIC         ('CI-00020', 'brass', 'CELL216', 3),
# MAGIC         ('CI-00020', 'brass', 'CELL217', 4),
# MAGIC         ('CI-00020', 'brass', 'CELL211', 5),
# MAGIC         ('CI-00020', 'brass', 'CELL212', 6),
# MAGIC         ('CI-00020', 'brass', 'CELL213', 7),
# MAGIC         ('CI-00020', 'brass', 'CELL201', 8),
# MAGIC         ('CI-00020', 'brass', 'CELL202', 9),
# MAGIC         ('CI-00020', 'brass', 'CELL203', 10),
# MAGIC         ('CI-00020', 'brass', 'CELL204', 11),
# MAGIC         ('CI-00020', 'brass', 'CELL205', 12),
# MAGIC         ('CI-00020', 'brass', 'CELL206', 13),
# MAGIC         ('CI-00020', 'brass', 'CELL207', 14),
# MAGIC         ('CI-00020', 'brass', 'CELL208', 15),
# MAGIC         ('CI-00020', 'brass', 'CELL209', 16),
# MAGIC         ('CI-00020', 'brass', 'CELL210', 17),
# MAGIC         ('CI-00020', 'brass', 'CELL219', 18),
# MAGIC         ('CI-00020', 'bangle', 'CELL214', 1),
# MAGIC         ('CI-00020', 'bangle', 'CELL215', 2),
# MAGIC         ('CI-00020', 'bangle', 'CELL216', 3),
# MAGIC         ('CI-00020', 'bangle', 'CELL217', 4),
# MAGIC         ('CI-00020', 'bangle', 'CELL201', 5),
# MAGIC         ('CI-00020', 'bangle', 'CELL202', 6),
# MAGIC         ('CI-00020', 'bangle', 'CELL203', 7),
# MAGIC         ('CI-00020', 'bangle', 'CELL204', 8),
# MAGIC         ('CI-00020', 'bangle', 'CELL205', 9),
# MAGIC         ('CI-00020', 'bangle', 'CELL103', 10),
# MAGIC         ('CI-00020', 'bangle', 'CELL211', 11),
# MAGIC         ('CI-00020', 'bangle', 'CELL212', 12),
# MAGIC         ('CI-00020', 'bangle', 'CELL213', 13),
# MAGIC         ('CI-00020', 'bangle', 'CELL206', 14),
# MAGIC         ('CI-00020', 'bangle', 'CELL207', 15),
# MAGIC         ('CI-00020', 'bangle', 'CELL208', 16),
# MAGIC         ('CI-00020', 'bangle', 'CELL209', 17),
# MAGIC         ('CI-00020', 'bangle', 'CELL210', 18),
# MAGIC         ('CI-00020', 'bangle', 'CELL109', 19),
# MAGIC         ('CI-00020', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00020', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00020', 'bangle-lock', 'CELL210', 3),
# MAGIC         ('CI-00020', 'bangle-lock', 'CELL208', 4),
# MAGIC         ('CI-00020', 'bangle-lock', 'CELL205', 5),
# MAGIC         ('CI-00020', 'bangle-lock', 'CELL217', 6),
# MAGIC         -- MONICA VINADER LTD
# MAGIC         ('CI-00022', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00022', 'silver', 'CELL205', 1),
# MAGIC         ('CI-00022', 'silver', 'CELL201', 2),
# MAGIC         ('CI-00022', 'silver', 'CELL202', 3),
# MAGIC         ('CI-00022', 'silver', 'CELL203', 4),
# MAGIC         ('CI-00022', 'silver', 'CELL204', 5),
# MAGIC         ('CI-00022', 'silver', 'CELL214', 6),
# MAGIC         ('CI-00022', 'silver', 'CELL215', 7),
# MAGIC         ('CI-00022', 'silver', 'CELL216', 8),
# MAGIC         ('CI-00022', 'silver', 'CELL217', 9),
# MAGIC         ('CI-00022', 'silver', 'CELL211', 10),
# MAGIC         ('CI-00022', 'silver', 'CELL212', 11),
# MAGIC         ('CI-00022', 'silver', 'CELL213', 12),
# MAGIC         ('CI-00022', 'silver', 'CELL206', 13),
# MAGIC         ('CI-00022', 'silver', 'CELL207', 14),
# MAGIC         ('CI-00022', 'silver', 'CELL208', 15),
# MAGIC         ('CI-00022', 'silver', 'CELL209', 16),
# MAGIC         ('CI-00022', 'silver', 'CELL210', 17),
# MAGIC         ('CI-00022', 'silver', 'CELL219', 18),
# MAGIC         ('CI-00022', 'silver', 'CELL109', 19),
# MAGIC         ('CI-00022', 'bangle', 'CELL201', 1),
# MAGIC         ('CI-00022', 'bangle', 'CELL202', 2),
# MAGIC         ('CI-00022', 'bangle', 'CELL203', 3),
# MAGIC         ('CI-00022', 'bangle', 'CELL204', 4),
# MAGIC         ('CI-00022', 'bangle', 'CELL205', 5),
# MAGIC         ('CI-00022', 'bangle', 'CELL214', 6),
# MAGIC         ('CI-00022', 'bangle', 'CELL215', 7),
# MAGIC         ('CI-00022', 'bangle', 'CELL216', 8),
# MAGIC         ('CI-00022', 'bangle', 'CELL217', 9),
# MAGIC         ('CI-00022', 'bangle', 'CELL103', 10),
# MAGIC         ('CI-00022', 'bangle', 'CELL109', 11),
# MAGIC         ('CI-00022', 'bangle', 'CELL211', 12),
# MAGIC         ('CI-00022', 'bangle', 'CELL212', 13),
# MAGIC         ('CI-00022', 'bangle', 'CELL213', 14),
# MAGIC         ('CI-00022', 'bangle', 'CELL206', 15),
# MAGIC         ('CI-00022', 'bangle', 'CELL207', 16),
# MAGIC         ('CI-00022', 'bangle', 'CELL208', 17),
# MAGIC         ('CI-00022', 'bangle', 'CELL209', 18),
# MAGIC         ('CI-00022', 'bangle', 'CELL210', 19),
# MAGIC         ('CI-00022', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00022', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00022', 'bangle-lock', 'CELL210', 3),
# MAGIC         ('CI-00022', 'bangle-lock', 'CELL211', 4),
# MAGIC         -- THE GREAT FROG
# MAGIC         ('CI-00027', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00027', 'silver', 'CELL211', 1),
# MAGIC         ('CI-00027', 'silver', 'CELL212', 2),
# MAGIC         ('CI-00027', 'silver', 'CELL213', 3),
# MAGIC         ('CI-00027', 'silver', 'CELL214', 4),
# MAGIC         ('CI-00027', 'silver', 'CELL215', 5),
# MAGIC         ('CI-00027', 'silver', 'CELL216', 6),
# MAGIC         ('CI-00027', 'silver', 'CELL217', 7),
# MAGIC         ('CI-00027', 'silver', 'CELL206', 8),
# MAGIC         ('CI-00027', 'silver', 'CELL207', 9),
# MAGIC         ('CI-00027', 'silver', 'CELL208', 10),
# MAGIC         ('CI-00027', 'silver', 'CELL209', 11),
# MAGIC         ('CI-00027', 'silver', 'CELL210', 12),
# MAGIC         ('CI-00027', 'silver', 'CELL219', 13),
# MAGIC         ('CI-00027', 'silver', 'CELL109', 14),
# MAGIC         ('CI-00027', 'bangle', 'CELL211', 1),
# MAGIC         ('CI-00027', 'bangle', 'CELL212', 2),
# MAGIC         ('CI-00027', 'bangle', 'CELL213', 3),
# MAGIC         ('CI-00027', 'bangle', 'CELL214', 4),
# MAGIC         ('CI-00027', 'bangle', 'CELL215', 5),
# MAGIC         ('CI-00027', 'bangle', 'CELL216', 6),
# MAGIC         ('CI-00027', 'bangle', 'CELL217', 7),
# MAGIC         ('CI-00027', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00027', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00027', 'bangle-lock', 'CELL210', 3),
# MAGIC         -- ASTRID & MIYU
# MAGIC         ('CI-00029', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00029', 'silver', 'CELL211', 1),
# MAGIC         ('CI-00029', 'silver', 'CELL212', 2),
# MAGIC         ('CI-00029', 'silver', 'CELL213', 3),
# MAGIC         ('CI-00029', 'silver', 'CELL214', 4),
# MAGIC         ('CI-00029', 'silver', 'CELL215', 5),
# MAGIC         ('CI-00029', 'silver', 'CELL216', 6),
# MAGIC         ('CI-00029', 'silver', 'CELL217', 7),
# MAGIC         ('CI-00029', 'silver', 'CELL219', 8),
# MAGIC         ('CI-00029', 'silver', 'CELL206', 9),
# MAGIC         ('CI-00029', 'silver', 'CELL210', 10),
# MAGIC         ('CI-00029', 'silver', 'CELL207', 11),
# MAGIC         ('CI-00029', 'silver', 'CELL208', 12),
# MAGIC         ('CI-00029', 'silver', 'CELL209', 13),
# MAGIC         ('CI-00029', 'silver', 'CELL201', 14),
# MAGIC         ('CI-00029', 'silver', 'CELL202', 15),
# MAGIC         ('CI-00029', 'silver', 'CELL203', 16),
# MAGIC         ('CI-00029', 'silver', 'CELL204', 17),
# MAGIC         ('CI-00029', 'silver', 'CELL205', 18),
# MAGIC         ('CI-00029', 'silver', 'CELL103', 19),
# MAGIC         ('CI-00029', 'silver', 'CELL109', 20),
# MAGIC         ('CI-00029', 'bangle', 'CELL211', 1),
# MAGIC         ('CI-00029', 'bangle', 'CELL212', 2),
# MAGIC         ('CI-00029', 'bangle', 'CELL213', 3),
# MAGIC         ('CI-00029', 'bangle', 'CELL214', 4),
# MAGIC         ('CI-00029', 'bangle', 'CELL215', 5),
# MAGIC         ('CI-00029', 'bangle', 'CELL216', 6),
# MAGIC         ('CI-00029', 'bangle', 'CELL217', 7),
# MAGIC         ('CI-00029', 'bangle', 'CELL201', 8),
# MAGIC         ('CI-00029', 'bangle', 'CELL202', 9),
# MAGIC         ('CI-00029', 'bangle', 'CELL203', 10),
# MAGIC         ('CI-00029', 'bangle', 'CELL204', 11),
# MAGIC         ('CI-00029', 'bangle', 'CELL205', 12),
# MAGIC         ('CI-00029', 'bangle', 'CELL103', 13),
# MAGIC         ('CI-00029', 'bangle', 'CELL109', 14),
# MAGIC         ('CI-00029', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00029', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00029', 'bangle-lock', 'CELL210', 3),
# MAGIC         ('CI-00029', 'bangle-lock', 'CELL208', 4),
# MAGIC         -- SEPAJATI LIMITED
# MAGIC         ('CI-00039', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00039', 'silver', 'CELL201', 1),
# MAGIC         ('CI-00039', 'silver', 'CELL202', 2),
# MAGIC         ('CI-00039', 'silver', 'CELL203', 3),
# MAGIC         ('CI-00039', 'silver', 'CELL204', 4),
# MAGIC         ('CI-00039', 'silver', 'CELL205', 5),
# MAGIC         ('CI-00039', 'silver', 'CELL214', 6),
# MAGIC         ('CI-00039', 'silver', 'CELL215', 7),
# MAGIC         ('CI-00039', 'silver', 'CELL216', 8),
# MAGIC         ('CI-00039', 'silver', 'CELL217', 9),
# MAGIC         ('CI-00039', 'silver', 'CELL211', 10),
# MAGIC         ('CI-00039', 'silver', 'CELL212', 11),
# MAGIC         ('CI-00039', 'silver', 'CELL213', 12),
# MAGIC         ('CI-00039', 'silver', 'CELL206', 13),
# MAGIC         ('CI-00039', 'silver', 'CELL207', 14),
# MAGIC         ('CI-00039', 'silver', 'CELL208', 15),
# MAGIC         ('CI-00039', 'silver', 'CELL209', 16),
# MAGIC         ('CI-00039', 'silver', 'CELL210', 17),
# MAGIC         ('CI-00039', 'silver', 'CELL219', 18),
# MAGIC         ('CI-00039', 'silver', 'CELL109', 19),
# MAGIC         ('CI-00039', 'bangle', 'CELL201', 1),
# MAGIC         ('CI-00039', 'bangle', 'CELL202', 2),
# MAGIC         ('CI-00039', 'bangle', 'CELL203', 3),
# MAGIC         ('CI-00039', 'bangle', 'CELL204', 4),
# MAGIC         ('CI-00039', 'bangle', 'CELL205', 5),
# MAGIC         ('CI-00039', 'bangle', 'CELL214', 6),
# MAGIC         ('CI-00039', 'bangle', 'CELL215', 7),
# MAGIC         ('CI-00039', 'bangle', 'CELL216', 8),
# MAGIC         ('CI-00039', 'bangle', 'CELL217', 9),
# MAGIC         ('CI-00039', 'bangle', 'CELL103', 10),
# MAGIC         ('CI-00039', 'bangle', 'CELL109', 11),
# MAGIC         ('CI-00039', 'bangle', 'CELL211', 12),
# MAGIC         ('CI-00039', 'bangle', 'CELL212', 13),
# MAGIC         ('CI-00039', 'bangle', 'CELL213', 14),
# MAGIC         ('CI-00039', 'bangle', 'CELL206', 15),
# MAGIC         ('CI-00039', 'bangle', 'CELL207', 16),
# MAGIC         ('CI-00039', 'bangle', 'CELL208', 17),
# MAGIC         ('CI-00039', 'bangle', 'CELL209', 18),
# MAGIC         ('CI-00039', 'bangle', 'CELL210', 19),
# MAGIC         ('CI-00039', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00039', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00039', 'bangle-lock', 'CELL210', 3),
# MAGIC         -- VIVIENNE WESTWOOD JEWELLERY
# MAGIC         ('CI-00040', 'gold', 'CELL109', 1),
# MAGIC         ('CI-00040', 'silver', 'CELL211', 1),
# MAGIC         ('CI-00040', 'silver', 'CELL212', 2),
# MAGIC         ('CI-00040', 'silver', 'CELL213', 3),
# MAGIC         ('CI-00040', 'silver', 'CELL214', 4),
# MAGIC         ('CI-00040', 'silver', 'CELL215', 5),
# MAGIC         ('CI-00040', 'silver', 'CELL216', 6),
# MAGIC         ('CI-00040', 'silver', 'CELL217', 7),
# MAGIC         ('CI-00040', 'silver', 'CELL219', 8),
# MAGIC         ('CI-00040', 'silver', 'CELL206', 9),
# MAGIC         ('CI-00040', 'silver', 'CELL207', 10),
# MAGIC         ('CI-00040', 'silver', 'CELL208', 11),
# MAGIC         ('CI-00040', 'silver', 'CELL209', 12),
# MAGIC         ('CI-00040', 'silver', 'CELL210', 13),
# MAGIC         ('CI-00040', 'silver', 'CELL201', 14),
# MAGIC         ('CI-00040', 'silver', 'CELL202', 15),
# MAGIC         ('CI-00040', 'silver', 'CELL203', 16),
# MAGIC         ('CI-00040', 'silver', 'CELL204', 17),
# MAGIC         ('CI-00040', 'silver', 'CELL205', 18),
# MAGIC         ('CI-00040', 'silver', 'CELL103', 19),
# MAGIC         ('CI-00040', 'silver', 'CELL109', 20),
# MAGIC         ('CI-00040', 'brass', 'CELL211', 1),
# MAGIC         ('CI-00040', 'brass', 'CELL212', 2),
# MAGIC         ('CI-00040', 'brass', 'CELL213', 3),
# MAGIC         ('CI-00040', 'brass', 'CELL214', 4),
# MAGIC         ('CI-00040', 'brass', 'CELL215', 5),
# MAGIC         ('CI-00040', 'brass', 'CELL216', 6),
# MAGIC         ('CI-00040', 'brass', 'CELL217', 7),
# MAGIC         ('CI-00040', 'brass', 'CELL219', 8),
# MAGIC         ('CI-00040', 'brass', 'CELL206', 9),
# MAGIC         ('CI-00040', 'brass', 'CELL207', 10),
# MAGIC         ('CI-00040', 'brass', 'CELL208', 11),
# MAGIC         ('CI-00040', 'brass', 'CELL209', 12),
# MAGIC         ('CI-00040', 'brass', 'CELL210', 13),
# MAGIC         ('CI-00040', 'brass', 'CELL201', 14),
# MAGIC         ('CI-00040', 'brass', 'CELL202', 15),
# MAGIC         ('CI-00040', 'brass', 'CELL203', 16),
# MAGIC         ('CI-00040', 'brass', 'CELL204', 17),
# MAGIC         ('CI-00040', 'brass', 'CELL205', 18),
# MAGIC         ('CI-00040', 'bangle', 'CELL206', 1),
# MAGIC         ('CI-00040', 'bangle', 'CELL207', 2),
# MAGIC         ('CI-00040', 'bangle', 'CELL208', 3),
# MAGIC         ('CI-00040', 'bangle', 'CELL209', 4),
# MAGIC         ('CI-00040', 'bangle', 'CELL210', 5),
# MAGIC         ('CI-00040', 'bangle', 'CELL201', 6),
# MAGIC         ('CI-00040', 'bangle', 'CELL202', 7),
# MAGIC         ('CI-00040', 'bangle', 'CELL203', 8),
# MAGIC         ('CI-00040', 'bangle', 'CELL204', 9),
# MAGIC         ('CI-00040', 'bangle', 'CELL205', 10),
# MAGIC         ('CI-00040', 'bangle', 'CELL103', 11),
# MAGIC         ('CI-00040', 'bangle', 'CELL109', 12),
# MAGIC         ('CI-00040', 'bangle', 'CELL214', 13),
# MAGIC         ('CI-00040', 'bangle', 'CELL215', 14),
# MAGIC         ('CI-00040', 'bangle', 'CELL216', 15),
# MAGIC         ('CI-00040', 'bangle', 'CELL217', 16),
# MAGIC         ('CI-00040', 'bangle', 'CELL211', 17),
# MAGIC         ('CI-00040', 'bangle', 'CELL212', 18),
# MAGIC         ('CI-00040', 'bangle', 'CELL213', 19),
# MAGIC         ('CI-00040', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00040', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00040', 'bangle-lock', 'CELL210', 3),
# MAGIC         -- ASPINAL OF LONDON
# MAGIC         ('CI-00042', 'silver', 'CELL203', 1),
# MAGIC         ('CI-00042', 'silver', 'CELL201', 2),
# MAGIC         ('CI-00042', 'silver', 'CELL204', 3),
# MAGIC         ('CI-00042', 'silver', 'CELL205', 4),
# MAGIC         ('CI-00042', 'silver', 'CELL211', 5),
# MAGIC         ('CI-00042', 'silver', 'CELL212', 6),
# MAGIC         ('CI-00042', 'silver', 'CELL213', 7),
# MAGIC         ('CI-00042', 'silver', 'CELL214', 8),
# MAGIC         ('CI-00042', 'silver', 'CELL215', 9),
# MAGIC         ('CI-00042', 'silver', 'CELL216', 10),
# MAGIC         ('CI-00042', 'silver', 'CELL217', 11),
# MAGIC         ('CI-00042', 'silver', 'CELL219', 12),
# MAGIC         ('CI-00042', 'silver', 'CELL206', 13),
# MAGIC         ('CI-00042', 'silver', 'CELL208', 14),
# MAGIC         ('CI-00042', 'silver', 'CELL210', 15),
# MAGIC         ('CI-00042', 'silver', 'CELL209', 16),
# MAGIC         ('CI-00042', 'silver', 'CELL207', 17),
# MAGIC         ('CI-00042', 'silver', 'CELL103', 18),
# MAGIC         ('CI-00042', 'silver', 'CELL109', 19),
# MAGIC         ('CI-00042', 'bangle', 'CELL211', 1),
# MAGIC         ('CI-00042', 'bangle', 'CELL212', 2),
# MAGIC         ('CI-00042', 'bangle', 'CELL213', 3),
# MAGIC         ('CI-00042', 'bangle', 'CELL214', 4),
# MAGIC         ('CI-00042', 'bangle', 'CELL215', 5),
# MAGIC         ('CI-00042', 'bangle', 'CELL216', 6),
# MAGIC         ('CI-00042', 'bangle', 'CELL217', 7),
# MAGIC         ('CI-00042', 'bangle', 'CELL201', 8),
# MAGIC         ('CI-00042', 'bangle', 'CELL202', 9),
# MAGIC         ('CI-00042', 'bangle', 'CELL203', 10),
# MAGIC         ('CI-00042', 'bangle', 'CELL204', 11),
# MAGIC         ('CI-00042', 'bangle', 'CELL205', 12),
# MAGIC         ('CI-00042', 'bangle', 'CELL103', 13),
# MAGIC         ('CI-00042', 'bangle', 'CELL109', 14),
# MAGIC         ('CI-00042', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00042', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00042', 'bangle-lock', 'CELL210', 3),
# MAGIC         ('CI-00042', 'bangle-lock', 'CELL208', 4),
# MAGIC         -- RAG&BONE
# MAGIC         ('CI-00044', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00044', 'silver', 'CELL205', 1),
# MAGIC         ('CI-00044', 'silver', 'CELL201', 2),
# MAGIC         ('CI-00044', 'silver', 'CELL202', 3),
# MAGIC         ('CI-00044', 'silver', 'CELL203', 4),
# MAGIC         ('CI-00044', 'silver', 'CELL204', 5),
# MAGIC         ('CI-00044', 'silver', 'CELL214', 6),
# MAGIC         ('CI-00044', 'silver', 'CELL215', 7),
# MAGIC         ('CI-00044', 'silver', 'CELL216', 8),
# MAGIC         ('CI-00044', 'silver', 'CELL217', 9),
# MAGIC         ('CI-00044', 'silver', 'CELL211', 10),
# MAGIC         ('CI-00044', 'silver', 'CELL212', 11),
# MAGIC         ('CI-00044', 'silver', 'CELL213', 12),
# MAGIC         ('CI-00044', 'silver', 'CELL206', 13),
# MAGIC         ('CI-00044', 'silver', 'CELL207', 14),
# MAGIC         ('CI-00044', 'silver', 'CELL208', 15),
# MAGIC         ('CI-00044', 'silver', 'CELL209', 16),
# MAGIC         ('CI-00044', 'silver', 'CELL210', 17),
# MAGIC         ('CI-00044', 'silver', 'CELL219', 18),
# MAGIC         ('CI-00044', 'silver', 'CELL109', 19),
# MAGIC         ('CI-00044', 'bangle', 'CELL201', 1),
# MAGIC         ('CI-00044', 'bangle', 'CELL202', 2),
# MAGIC         ('CI-00044', 'bangle', 'CELL203', 3),
# MAGIC         ('CI-00044', 'bangle', 'CELL204', 4),
# MAGIC         ('CI-00044', 'bangle', 'CELL205', 5),
# MAGIC         ('CI-00044', 'bangle', 'CELL214', 6),
# MAGIC         ('CI-00044', 'bangle', 'CELL215', 7),
# MAGIC         ('CI-00044', 'bangle', 'CELL216', 8),
# MAGIC         ('CI-00044', 'bangle', 'CELL217', 9),
# MAGIC         ('CI-00044', 'bangle', 'CELL103', 10),
# MAGIC         ('CI-00044', 'bangle', 'CELL109', 11),
# MAGIC         ('CI-00044', 'bangle', 'CELL211', 12),
# MAGIC         ('CI-00044', 'bangle', 'CELL212', 13),
# MAGIC         ('CI-00044', 'bangle', 'CELL213', 14),
# MAGIC         ('CI-00044', 'bangle', 'CELL206', 15),
# MAGIC         ('CI-00044', 'bangle', 'CELL207', 16),
# MAGIC         ('CI-00044', 'bangle', 'CELL208', 17),
# MAGIC         ('CI-00044', 'bangle', 'CELL209', 18),
# MAGIC         ('CI-00044', 'bangle', 'CELL210', 19),
# MAGIC         ('CI-00044', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00044', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00044', 'bangle-lock', 'CELL210', 3),
# MAGIC         -- AGATHA PARIS
# MAGIC         ('CI-00047', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00047', 'silver', 'CELL201', 1),
# MAGIC         ('CI-00047', 'silver', 'CELL202', 2),
# MAGIC         ('CI-00047', 'silver', 'CELL203', 3),
# MAGIC         ('CI-00047', 'silver', 'CELL204', 4),
# MAGIC         ('CI-00047', 'silver', 'CELL205', 5),
# MAGIC         ('CI-00047', 'silver', 'CELL214', 6),
# MAGIC         ('CI-00047', 'silver', 'CELL215', 7),
# MAGIC         ('CI-00047', 'silver', 'CELL216', 8),
# MAGIC         ('CI-00047', 'silver', 'CELL217', 9),
# MAGIC         ('CI-00047', 'silver', 'CELL211', 10),
# MAGIC         ('CI-00047', 'silver', 'CELL212', 11),
# MAGIC         ('CI-00047', 'silver', 'CELL213', 12),
# MAGIC         ('CI-00047', 'silver', 'CELL206', 13),
# MAGIC         ('CI-00047', 'silver', 'CELL207', 14),
# MAGIC         ('CI-00047', 'silver', 'CELL208', 15),
# MAGIC         ('CI-00047', 'silver', 'CELL209', 16),
# MAGIC         ('CI-00047', 'silver', 'CELL210', 17),
# MAGIC         ('CI-00047', 'silver', 'CELL219', 18),
# MAGIC         ('CI-00047', 'silver', 'CELL109', 19),
# MAGIC         ('CI-00047', 'brass', 'CELL201', 1),
# MAGIC         ('CI-00047', 'brass', 'CELL202', 2),
# MAGIC         ('CI-00047', 'brass', 'CELL203', 3),
# MAGIC         ('CI-00047', 'brass', 'CELL204', 4),
# MAGIC         ('CI-00047', 'brass', 'CELL205', 5),
# MAGIC         ('CI-00047', 'brass', 'CELL214', 6),
# MAGIC         ('CI-00047', 'brass', 'CELL215', 7),
# MAGIC         ('CI-00047', 'brass', 'CELL216', 8),
# MAGIC         ('CI-00047', 'brass', 'CELL217', 9),
# MAGIC         ('CI-00047', 'brass', 'CELL211', 10),
# MAGIC         ('CI-00047', 'brass', 'CELL212', 11),
# MAGIC         ('CI-00047', 'brass', 'CELL213', 12),
# MAGIC         ('CI-00047', 'brass', 'CELL206', 13),
# MAGIC         ('CI-00047', 'brass', 'CELL207', 14),
# MAGIC         ('CI-00047', 'brass', 'CELL208', 15),
# MAGIC         ('CI-00047', 'brass', 'CELL209', 16),
# MAGIC         ('CI-00047', 'brass', 'CELL210', 17),
# MAGIC         ('CI-00047', 'brass', 'CELL219', 18),
# MAGIC         ('CI-00047', 'bangle', 'CELL201', 1),
# MAGIC         ('CI-00047', 'bangle', 'CELL202', 2),
# MAGIC         ('CI-00047', 'bangle', 'CELL203', 3),
# MAGIC         ('CI-00047', 'bangle', 'CELL204', 4),
# MAGIC         ('CI-00047', 'bangle', 'CELL205', 5),
# MAGIC         ('CI-00047', 'bangle', 'CELL214', 6),
# MAGIC         ('CI-00047', 'bangle', 'CELL215', 7),
# MAGIC         ('CI-00047', 'bangle', 'CELL216', 8),
# MAGIC         ('CI-00047', 'bangle', 'CELL217', 9),
# MAGIC         ('CI-00047', 'bangle', 'CELL103', 10),
# MAGIC         ('CI-00047', 'bangle', 'CELL109', 11),
# MAGIC         ('CI-00047', 'bangle', 'CELL211', 12),
# MAGIC         ('CI-00047', 'bangle', 'CELL212', 13),
# MAGIC         ('CI-00047', 'bangle', 'CELL213', 14),
# MAGIC         ('CI-00047', 'bangle', 'CELL206', 15),
# MAGIC         ('CI-00047', 'bangle', 'CELL207', 16),
# MAGIC         ('CI-00047', 'bangle', 'CELL208', 17),
# MAGIC         ('CI-00047', 'bangle', 'CELL209', 18),
# MAGIC         ('CI-00047', 'bangle', 'CELL210', 19),
# MAGIC         ('CI-00047', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00047', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00047', 'bangle-lock', 'CELL210', 3),
# MAGIC         -- ABBOTT LYON
# MAGIC         ('CI-00048', 'gold', 'CELL103', 1),
# MAGIC         ('CI-00048', 'silver', 'CELL205', 1),
# MAGIC         ('CI-00048', 'silver', 'CELL201', 2),
# MAGIC         ('CI-00048', 'silver', 'CELL202', 3),
# MAGIC         ('CI-00048', 'silver', 'CELL203', 4),
# MAGIC         ('CI-00048', 'silver', 'CELL204', 5),
# MAGIC         ('CI-00048', 'silver', 'CELL214', 6),
# MAGIC         ('CI-00048', 'silver', 'CELL215', 7),
# MAGIC         ('CI-00048', 'silver', 'CELL216', 8),
# MAGIC         ('CI-00048', 'silver', 'CELL217', 9),
# MAGIC         ('CI-00048', 'silver', 'CELL211', 10),
# MAGIC         ('CI-00048', 'silver', 'CELL212', 11),
# MAGIC         ('CI-00048', 'silver', 'CELL213', 12),
# MAGIC         ('CI-00048', 'silver', 'CELL206', 13),
# MAGIC         ('CI-00048', 'silver', 'CELL207', 14),
# MAGIC         ('CI-00048', 'silver', 'CELL208', 15),
# MAGIC         ('CI-00048', 'silver', 'CELL209', 16),
# MAGIC         ('CI-00048', 'silver', 'CELL210', 17),
# MAGIC         ('CI-00048', 'silver', 'CELL219', 18),
# MAGIC         ('CI-00048', 'silver', 'CELL109', 19),
# MAGIC         ('CI-00048', 'bangle', 'CELL201', 1),
# MAGIC         ('CI-00048', 'bangle', 'CELL202', 2),
# MAGIC         ('CI-00048', 'bangle', 'CELL203', 3),
# MAGIC         ('CI-00048', 'bangle', 'CELL204', 4),
# MAGIC         ('CI-00048', 'bangle', 'CELL205', 5),
# MAGIC         ('CI-00048', 'bangle', 'CELL214', 6),
# MAGIC         ('CI-00048', 'bangle', 'CELL215', 7),
# MAGIC         ('CI-00048', 'bangle', 'CELL216', 8),
# MAGIC         ('CI-00048', 'bangle', 'CELL217', 9),
# MAGIC         ('CI-00048', 'bangle', 'CELL103', 10),
# MAGIC         ('CI-00048', 'bangle', 'CELL109', 11),
# MAGIC         ('CI-00048', 'bangle', 'CELL211', 12),
# MAGIC         ('CI-00048', 'bangle', 'CELL212', 13),
# MAGIC         ('CI-00048', 'bangle', 'CELL213', 14),
# MAGIC         ('CI-00048', 'bangle', 'CELL206', 15),
# MAGIC         ('CI-00048', 'bangle', 'CELL207', 16),
# MAGIC         ('CI-00048', 'bangle', 'CELL208', 17),
# MAGIC         ('CI-00048', 'bangle', 'CELL209', 18),
# MAGIC         ('CI-00048', 'bangle', 'CELL210', 19),
# MAGIC         ('CI-00048', 'bangle-lock', 'CELL207', 1),
# MAGIC         ('CI-00048', 'bangle-lock', 'CELL209', 2),
# MAGIC         ('CI-00048', 'bangle-lock', 'CELL210', 3)
# MAGIC AS t(customer_no, material_type, cell_code, priority_rank);
# MAGIC 
# MAGIC -- 2. `gold_scheduling_category_rule` (6 BANGLE rules)
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.planning.gold_scheduling_category_rule
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     product_category, cell_code, priority_rank,
# MAGIC     CASE
# MAGIC         WHEN cell_code IN ('CELL105','CELL109')                                         THEN 'PRODLINE1'
# MAGIC         WHEN cell_code IN ('CELL201','CELL202','CELL203','CELL204','CELL205','CELL218') THEN 'PRODLINE2'
# MAGIC         WHEN cell_code IN ('CELL103','CELL104','CELL206','CELL207','CELL208','CELL209','CELL210','CELL219','CELL220') THEN 'PRODLINE3'
# MAGIC         WHEN cell_code IN ('CELL211','CELL212','CELL213')                               THEN 'PRODLINE4'
# MAGIC         WHEN cell_code IN ('CELL214','CELL215','CELL216','CELL217')                     THEN 'PRODLINE5'
# MAGIC         ELSE 'UNKNOWN'
# MAGIC     END AS production_line,
# MAGIC     current_timestamp() AS _load_timestamp
# MAGIC FROM VALUES
# MAGIC         ('BANGLE', 'CELL207', 1),
# MAGIC         ('BANGLE', 'CELL208', 2),
# MAGIC         ('BANGLE', 'CELL209', 3),
# MAGIC         ('BANGLE', 'CELL210', 4),
# MAGIC         ('BANGLE', 'CELL211', 5),
# MAGIC         ('BANGLE', 'CELL217', 6)
# MAGIC AS t(product_category, cell_code, priority_rank);
# MAGIC 
# MAGIC -- 3. `gold_scheduling_additional_rule` (39 rules, **incl. default**)
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.planning.gold_scheduling_additional_rule
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     rule_name,
# MAGIC     cell_code,
# MAGIC     CAST(is_allowed AS BOOLEAN) AS is_allowed,
# MAGIC     CASE
# MAGIC         WHEN cell_code IN ('CELL105','CELL109')                                         THEN 'PRODLINE1'
# MAGIC         WHEN cell_code IN ('CELL201','CELL202','CELL203','CELL204','CELL205','CELL218') THEN 'PRODLINE2'
# MAGIC         WHEN cell_code IN ('CELL103','CELL104','CELL206','CELL207','CELL208','CELL209','CELL210','CELL219','CELL220') THEN 'PRODLINE3'
# MAGIC         WHEN cell_code IN ('CELL211','CELL212','CELL213')                               THEN 'PRODLINE4'
# MAGIC         WHEN cell_code IN ('CELL214','CELL215','CELL216','CELL217')                     THEN 'PRODLINE5'
# MAGIC         ELSE 'UNKNOWN'
# MAGIC     END AS production_line,
# MAGIC     current_timestamp() AS _load_timestamp
# MAGIC FROM VALUES
# MAGIC         -- OXIDIZE
# MAGIC         ('OXIDIZE', 'CELL105', true),
# MAGIC         ('OXIDIZE', 'CELL109', true),
# MAGIC         ('OXIDIZE', 'CELL207', true),
# MAGIC         ('OXIDIZE', 'CELL208', true),
# MAGIC         ('OXIDIZE', 'CELL209', true),
# MAGIC         ('OXIDIZE', 'CELL210', true),
# MAGIC         ('OXIDIZE', 'CELL211', true),
# MAGIC         ('OXIDIZE', 'CELL212', true),
# MAGIC         ('OXIDIZE', 'CELL213', true),
# MAGIC         ('OXIDIZE', 'CELL214', true),
# MAGIC         ('OXIDIZE', 'CELL215', true),
# MAGIC         ('OXIDIZE', 'CELL216', true),
# MAGIC         ('OXIDIZE', 'CELL219', true),
# MAGIC 
# MAGIC         -- BANGLE_LOCK
# MAGIC         ('BANGLE_LOCK', 'CELL207', true),
# MAGIC         ('BANGLE_LOCK', 'CELL208', true),
# MAGIC         ('BANGLE_LOCK', 'CELL209', true),
# MAGIC         ('BANGLE_LOCK', 'CELL210', true),
# MAGIC         ('BANGLE_LOCK', 'CELL211', true),
# MAGIC 
# MAGIC         -- BANGLE_LOCK_T2
# MAGIC         ('BANGLE_LOCK_T2', 'CELL209', true),
# MAGIC 
# MAGIC         -- default
# MAGIC         ('default', 'CELL105', true),
# MAGIC         ('default', 'CELL109', true),
# MAGIC         ('default', 'CELL201', true),
# MAGIC         ('default', 'CELL202', true),
# MAGIC         ('default', 'CELL203', true),
# MAGIC         ('default', 'CELL204', true),
# MAGIC         ('default', 'CELL205', true),
# MAGIC         ('default', 'CELL206', true),
# MAGIC         ('default', 'CELL207', true),
# MAGIC         ('default', 'CELL208', true),
# MAGIC         ('default', 'CELL209', true),
# MAGIC         ('default', 'CELL210', true),
# MAGIC         ('default', 'CELL211', true),
# MAGIC         ('default', 'CELL212', true),
# MAGIC         ('default', 'CELL213', true),
# MAGIC         ('default', 'CELL214', true),
# MAGIC         ('default', 'CELL215', true),
# MAGIC         ('default', 'CELL216', true),
# MAGIC         ('default', 'CELL217', true),
# MAGIC         ('default', 'CELL219', true)
# MAGIC AS t(rule_name, cell_code, is_allowed);
# MAGIC 
# MAGIC -- 4. `gold_scheduling_customer_override`
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Production_Lakehouse.planning.gold_scheduling_customer_override
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     customer_no, material_type, forced_cell, applies_to, reason,
# MAGIC     CASE
# MAGIC         WHEN forced_cell IN ('CELL105','CELL109')                                         THEN 'PRODLINE1'
# MAGIC         WHEN forced_cell IN ('CELL201','CELL202','CELL203','CELL204','CELL205','CELL218') THEN 'PRODLINE2'
# MAGIC         WHEN forced_cell IN ('CELL103','CELL104','CELL206','CELL207','CELL208','CELL209','CELL210','CELL219','CELL220') THEN 'PRODLINE3'
# MAGIC         WHEN forced_cell IN ('CELL211','CELL212','CELL213')                               THEN 'PRODLINE4'
# MAGIC         WHEN forced_cell IN ('CELL214','CELL215','CELL216','CELL217')                     THEN 'PRODLINE5'
# MAGIC         ELSE 'UNKNOWN'
# MAGIC     END AS production_line,
# MAGIC     current_timestamp() AS _load_timestamp
# MAGIC FROM VALUES
# MAGIC     ('CI-00040', 'gold', 'CELL109', 'ALL', 'VW gold always routes to CELL109 (Normal & SP)')
# MAGIC AS t(customer_no, material_type, forced_cell, applies_to, reason);
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- 5. v_gold_scheduling_customer_rule_active
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE VIEW Gold_Production_Lakehouse.planning.v_gold_scheduling_customer_rule_active
# MAGIC AS
# MAGIC SELECT
# MAGIC     r.customer_no,
# MAGIC     c.`Name` AS customer_name,
# MAGIC     c.`DSVC Branch ID` AS customer_branch,
# MAGIC     r.material_type,
# MAGIC     r.cell_code,
# MAGIC     r.priority_rank,
# MAGIC     r.production_line,
# MAGIC     r._load_timestamp
# MAGIC FROM Gold_Production_Lakehouse.planning.gold_scheduling_customer_rule r
# MAGIC INNER JOIN Silver_BC_Lakehouse.bc.Customer c
# MAGIC     ON c.`No.` = r.customer_no
# MAGIC WHERE c.`Blocked` IS NULL;
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- 6. v_gold_scheduling_item_category
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE VIEW Gold_Production_Lakehouse.planning.v_gold_scheduling_item_category
# MAGIC AS
# MAGIC SELECT
# MAGIC     i.`No.` AS item_no,
# MAGIC     i.`Description` AS item_description,
# MAGIC     i.`Item Category Code` AS item_category_code,
# MAGIC     i.`Product Type` AS product_type,
# MAGIC     i.`Sub Product Type` AS sub_product_type,
# MAGIC     i.`Skill Level` AS skill_level,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN UPPER(TRIM(i.`Product Type`)) = 'BANGLE'
# MAGIC              AND UPPER(TRIM(COALESCE(i.`Sub Product Type`, ''))) IN ('BG-LOCK-TNG', 'BG-LOCK-PSH')
# MAGIC             THEN 'BANGLE-LOCK'
# MAGIC         WHEN UPPER(TRIM(i.`Product Type`)) = 'BANGLE' THEN 'BANGLE'
# MAGIC         ELSE 'OTHER'
# MAGIC     END AS scheduling_category,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN UPPER(TRIM(i.`Product Type`)) = 'BANGLE'
# MAGIC              AND UPPER(TRIM(COALESCE(i.`Sub Product Type`, ''))) IN ('BG-LOCK-TNG', 'BG-LOCK-PSH')
# MAGIC             THEN 'bangle-lock'
# MAGIC         WHEN UPPER(TRIM(i.`Product Type`)) = 'BANGLE' THEN 'bangle'
# MAGIC         ELSE NULL
# MAGIC     END AS bangle_material_type,
# MAGIC 
# MAGIC     current_timestamp() AS _load_timestamp
# MAGIC FROM Silver_BC_Lakehouse.bc.Item i
# MAGIC WHERE i.`Blocked` = '0';
# MAGIC 
# MAGIC 
# MAGIC -- ============================================================
# MAGIC -- 7. v_gold_scheduling_cell_pool
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE VIEW Gold_Production_Lakehouse.planning.v_gold_scheduling_cell_pool
# MAGIC AS
# MAGIC SELECT
# MAGIC     ic.item_no,
# MAGIC     ic.scheduling_category,
# MAGIC     ic.bangle_material_type,
# MAGIC     cr.customer_no,
# MAGIC     cr.customer_name,
# MAGIC     cr.material_type,
# MAGIC     cr.cell_code,
# MAGIC     cr.priority_rank,
# MAGIC     cr.production_line
# MAGIC FROM Gold_Production_Lakehouse.planning.v_gold_scheduling_item_category ic
# MAGIC CROSS JOIN Gold_Production_Lakehouse.planning.v_gold_scheduling_customer_rule_active cr
# MAGIC WHERE
# MAGIC     (ic.bangle_material_type IS NOT NULL AND cr.material_type = ic.bangle_material_type)
# MAGIC     OR ic.bangle_material_type IS NULL;
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC -- 8. Smoke tests
# MAGIC     -- 8.1 Row counts for all tables/views
# MAGIC 
# MAGIC SELECT 'customer_rule'        AS object_name, COUNT(*) AS row_count FROM Gold_Production_Lakehouse.planning.gold_scheduling_customer_rule
# MAGIC UNION ALL SELECT 'category_rule',     COUNT(*) FROM Gold_Production_Lakehouse.planning.gold_scheduling_category_rule
# MAGIC UNION ALL SELECT 'additional_rule',   COUNT(*) FROM Gold_Production_Lakehouse.planning.gold_scheduling_additional_rule
# MAGIC UNION ALL SELECT 'customer_override', COUNT(*) FROM Gold_Production_Lakehouse.planning.gold_scheduling_customer_override
# MAGIC UNION ALL SELECT 'v_cust_rule_active',COUNT(*) FROM Gold_Production_Lakehouse.planning.v_gold_scheduling_customer_rule_active
# MAGIC UNION ALL SELECT 'v_item_category',   COUNT(*) FROM Gold_Production_Lakehouse.planning.v_gold_scheduling_item_category
# MAGIC UNION ALL SELECT 'v_cell_pool',       COUNT(*) FROM Gold_Production_Lakehouse.planning.v_gold_scheduling_cell_pool
# MAGIC ORDER BY object_name;
# MAGIC 
# MAGIC     -- 8.2 Additional rule breakdown
# MAGIC 
# MAGIC SELECT rule_name, COUNT(*) AS cell_count, COLLECT_LIST(cell_code) AS cells
# MAGIC FROM Gold_Production_Lakehouse.planning.gold_scheduling_additional_rule
# MAGIC WHERE is_allowed = TRUE
# MAGIC GROUP BY rule_name
# MAGIC ORDER BY
# MAGIC     CASE rule_name
# MAGIC         WHEN 'OXIDIZE' THEN 1
# MAGIC         WHEN 'BANGLE_LOCK' THEN 2
# MAGIC         WHEN 'BANGLE_LOCK_T2' THEN 3
# MAGIC         WHEN 'default' THEN 4
# MAGIC         ELSE 99
# MAGIC     END;
# MAGIC 
# MAGIC     -- 8.3 VW gold override is in place
# MAGIC 
# MAGIC SELECT customer_no, material_type, forced_cell, applies_to, reason
# MAGIC FROM Gold_Production_Lakehouse.planning.gold_scheduling_customer_override;
# MAGIC     -- 8.4 Sample: VW silver pool order (verify ordering matches Editor)
# MAGIC 
# MAGIC SELECT priority_rank, cell_code, production_line
# MAGIC FROM Gold_Production_Lakehouse.planning.v_gold_scheduling_customer_rule_active
# MAGIC WHERE customer_no = 'CI-00040' AND material_type = 'silver'
# MAGIC ORDER BY priority_rank;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
