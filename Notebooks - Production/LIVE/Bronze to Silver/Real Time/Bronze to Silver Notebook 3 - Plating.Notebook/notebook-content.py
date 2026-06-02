# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ad99fdfa-85b1-4480-9f7f-2640bfd65f24",
# META       "default_lakehouse_name": "Silver_Production_Lakehouse",
# META       "default_lakehouse_workspace_id": "d74457b3-045c-445d-82c6-9a2e4b9f1436",
# META       "known_lakehouses": [
# META         {
# META           "id": "3ea0efcd-03d5-44f1-8e70-99f52a5c2a22"
# META         },
# META         {
# META           "id": "ff4d6787-a716-43b6-baaf-972b7426ffa5"
# META         },
# META         {
# META           "id": "869b263b-1a86-424b-bd97-94bd586442b2"
# META         },
# META         {
# META           "id": "a29dcd6d-29cc-499a-b3a3-7b030d3e7cb5"
# META         },
# META         {
# META           "id": "ad99fdfa-85b1-4480-9f7f-2640bfd65f24"
# META         },
# META         {
# META           "id": "e248ea90-8431-4df2-9f29-87866bf9dd5a"
# META         },
# META         {
# META           "id": "3a130b81-98ec-4fd4-a404-95edc1f0ef1e"
# META         },
# META         {
# META           "id": "785307fd-af78-4359-969a-51c937ec834b"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Helpers

# CELL ********************

spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead",  "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead",     "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite",    "LEGACY")

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")


# Just cause sales data have dates older than 1900s


from datetime import datetime


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # silver_plating

# CELL ********************

# MAGIC %%sql
# MAGIC -- ─── 1. silver_plating_lab_tests ───
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_plating_lab_tests
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     CASE 
# MAGIC         WHEN cr535_testtype LIKE '%184930000%' THEN 'Element Test'
# MAGIC         WHEN cr535_testtype LIKE '%184930001%' THEN 'ICP Test'
# MAGIC         ELSE CAST(cr535_testtype AS STRING)
# MAGIC     END AS test_type,
# MAGIC     cr535_elementtext          AS element_text,
# MAGIC     cr535_platingsolutionname  AS plating_solution_name,
# MAGIC     cr535_solutionguid         AS solution_guid,
# MAGIC     cr535_tankname             AS tank_name,
# MAGIC     cr535_testname             AS test_name,
# MAGIC     createdon                  AS created_on,
# MAGIC     modifiedon                 AS modified_on
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.cr535_platinglabtests;
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC -- ─── 2. silver_plating_item ───
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_plating_item
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     cr535_description          AS description,
# MAGIC     cr535_platingitem          AS platingitem,
# MAGIC     CASE 
# MAGIC         WHEN cr535_thicknesstype LIKE '%184930000%' THEN 'Average'
# MAGIC         WHEN cr535_thicknesstype LIKE '%184930001%' THEN 'Minimum'
# MAGIC         ELSE CAST(cr535_thicknesstype AS STRING)
# MAGIC     END AS thicknesstype,
# MAGIC     cr535_totalthickness       AS totalthickness,
# MAGIC     createdon                  AS created_on,
# MAGIC     dts_itemdescription        AS itemdescription,
# MAGIC     dts_itemnumbername         AS itemnumbername,
# MAGIC     dts_jigid                  AS jigid,
# MAGIC     dts_jigid_entitytype       AS jigid_entitytype,
# MAGIC     dts_jigidname              AS jigidname,
# MAGIC     dts_platinglosspercentage  AS platinglosspercentage,
# MAGIC     dts_surfacearea            AS surfacearea,
# MAGIC     modifiedon                 AS modified_on
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.cr535_platingitem;
# MAGIC 
# MAGIC 
# MAGIC -- ─── 3. silver_plating_solution ───
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_plating_solution
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     cr535_solutionname         AS solution_name,
# MAGIC     cr535_solutiontype         AS solution_type,
# MAGIC     createdon                  AS created_on,
# MAGIC     modifiedon                 AS modified_on
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.cr535_platingsolution1;
# MAGIC 
# MAGIC 
# MAGIC -- ─── 4. silver_plating_solution_element_icp ───
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_plating_solution_element_icp
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     CASE 
# MAGIC     	WHEN cr535_unit LIKE '%184930000%' THEN 'ml/L'
# MAGIC     	WHEN cr535_unit LIKE '%184930001%' THEN 'g/L'
# MAGIC 		WHEN cr535_unit LIKE '%184930002%' THEN 'ppm'
# MAGIC 		WHEN cr535_unit LIKE '%184930003%' THEN 'cm²'
# MAGIC 		WHEN cr535_unit LIKE '%184930004%' THEN '%'
# MAGIC     	ELSE CAST(cr535_unit  AS STRING)
# MAGIC 	END AS unit,
# MAGIC     cr535_elementname          AS element_name,
# MAGIC     cr535_holdmax              AS hold_max,
# MAGIC     cr535_holdmin              AS hold_min,
# MAGIC     cr535_max                  AS max,
# MAGIC     cr535_min                  AS min,
# MAGIC     cr535_target               AS target,
# MAGIC     createdon                  AS created_on,
# MAGIC     dts_platingsolutionname    AS plating_solution_name,
# MAGIC     dts_rowid                  AS row_id,
# MAGIC     modifiedon                 AS modified_on
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.dts_platingsolutionelementicp;
# MAGIC 
# MAGIC 
# MAGIC -- ─── 5. silver_element_master ───
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_plating_element_master
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     CASE 
# MAGIC         WHEN dts_category LIKE '%780350000%' THEN 'Precious Metal'
# MAGIC         WHEN dts_category LIKE '%780350001%' THEN 'Non-Precious Metal'
# MAGIC         ELSE CAST(dts_category AS STRING)
# MAGIC     END AS category,
# MAGIC     CASE 
# MAGIC         WHEN dts_unit LIKE '%780350000%' THEN 'GR/LT'
# MAGIC         WHEN dts_unit LIKE '%780350001%' THEN 'ML/LT'
# MAGIC         ELSE CAST(dts_unit AS STRING)
# MAGIC     END AS unit,
# MAGIC     createdon                  AS created_on,
# MAGIC     dts_elementname            AS element_name,
# MAGIC     dts_premium_percentage     AS premium_percentage,
# MAGIC     modifiedon                 AS modified_on
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.dts_elementmaster;
# MAGIC 
# MAGIC -- ─── 6. silver_jigs ───
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_plating_jig
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     CASE 
# MAGIC         WHEN dts_inspectresult LIKE '%780350000%' THEN 'Pass'
# MAGIC         WHEN dts_inspectresult LIKE '%780350001%' THEN 'Fail'
# MAGIC         WHEN dts_inspectresult LIKE '%780350002%' THEN 'Warning'
# MAGIC         ELSE CAST(dts_inspectresult AS STRING)
# MAGIC     END AS inspectresult,
# MAGIC     CASE 
# MAGIC         WHEN dts_jigstatus LIKE '%780350000%' THEN 'Available'
# MAGIC         WHEN dts_jigstatus LIKE '%780350001%' THEN 'In Use'
# MAGIC         WHEN dts_jigstatus LIKE '%780350002%' THEN 'Maintenance'
# MAGIC         WHEN dts_jigstatus LIKE '%184930001%' THEN 'Inactive'
# MAGIC         ELSE CAST(dts_jigstatus AS STRING)
# MAGIC     END AS jig_status,
# MAGIC     cr535_jigdescription       AS jig_description,
# MAGIC     cr535_jigname              AS jig_name,
# MAGIC     createdon                  AS created_on,
# MAGIC     dts_capacity               AS capacity,
# MAGIC     dts_dayssincelastinspection AS days_since_last_inspection,
# MAGIC     dts_lastinspectdate        AS last_inspect_date,
# MAGIC     dts_lastmaintenance        AS last_maintenance,
# MAGIC     dts_rfidtag                AS rfid_tag,
# MAGIC     modifiedon                 AS modified_on
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.cr535_jigs;
# MAGIC 
# MAGIC 
# MAGIC -- ─── 7. silver_plating_jig_mapping ───
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_plating_jig_mapping
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     CASE 
# MAGIC         WHEN dts_status LIKE '%780350000%' THEN 'Pending'
# MAGIC         WHEN dts_status LIKE '%780350002%' THEN 'In Progress'
# MAGIC         WHEN dts_status LIKE '%780350001%' THEN 'Completed'
# MAGIC         WHEN dts_status LIKE '%780350003%' THEN 'Rejected'
# MAGIC         ELSE CAST(dts_status AS STRING)
# MAGIC     END AS status,
# MAGIC     dts_platingitem            AS plating_item,
# MAGIC     createdon                  AS created_on,
# MAGIC     dts_jigs                   AS jigs,
# MAGIC     dts_name                   AS name,
# MAGIC     dts_platingitemname        AS plating_item_name,
# MAGIC     dts_surface                AS surface,
# MAGIC     dts_totalquantity          AS total_quantity,
# MAGIC     modifiedon                 AS modified_on
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.dts_platingjigmapping;
# MAGIC 
# MAGIC 
# MAGIC -- ─── 8. silver_plating_machine ───
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_plating_machine
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     CASE 
# MAGIC         WHEN dts_currentstatus LIKE '%780350000%' THEN 'Operational'
# MAGIC         WHEN dts_currentstatus LIKE '%780350001%' THEN 'Offline'
# MAGIC         WHEN dts_currentstatus LIKE '%780350002%' THEN 'Maintenance'
# MAGIC         ELSE CAST(dts_currentstatus AS STRING)
# MAGIC     END AS current_status,
# MAGIC     dts_locations              AS locations,
# MAGIC     cr535_capacity             AS capacity,
# MAGIC     cr535_currentrange         AS current_range,
# MAGIC     cr535_machinename          AS machine_name1,
# MAGIC     cr535_notes                AS notes,
# MAGIC     createdon                  AS created_on,
# MAGIC     dts_machineid              AS machine_id,
# MAGIC     dts_machinename            AS machine_name,
# MAGIC     dts_temperaturecontroller  AS temperature_controller,
# MAGIC     modifiedon                 AS modified_on
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.cr535_platingmachine;
# MAGIC 
# MAGIC 
# MAGIC -- ─── 9. silver_plating_prep_jig ───
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_plating_prep_jig
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     CASE 
# MAGIC         WHEN dts_priority LIKE '%780350000%' THEN 'Rush'
# MAGIC         WHEN dts_priority LIKE '%780350001%' THEN 'Normal'
# MAGIC         ELSE CAST(dts_priority AS STRING)
# MAGIC     END AS priority,
# MAGIC     cr535_jigname              AS jig_name,
# MAGIC     cr535_newcolumn            AS new_column,
# MAGIC     cr535_platingitemnoname    AS plating_item_no_name,
# MAGIC     createdon                  AS created_on,
# MAGIC     modifiedon                 AS modified_on,
# MAGIC     dts_notes                  AS notes
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.cr535_platingprep_jig;
# MAGIC 
# MAGIC 
# MAGIC -- ─── 10. silver_plating_prep_jig_items ───
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_plating_prep_jig_items
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     cr535_bomqty               AS bom_qty,
# MAGIC     cr535_fgitemno             AS fg_item_no,
# MAGIC     dts_fgitemname             AS fg_item_name,
# MAGIC     cr535_prodorderno          AS prod_order_no,
# MAGIC     cr535_prodorderlineno      AS prod_order_line_no,
# MAGIC     dts_quantity,
# MAGIC     cr535_quantity,
# MAGIC     cr535_parentjigname        AS parent_jig_name,
# MAGIC     createdon                  AS created_on,
# MAGIC     modifiedon                 AS modified_on,
# MAGIC     dts_routinglinkcode        AS routing_link_code,
# MAGIC     cr535_platingitem          AS plating_item
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.cr535_platingprep_jigitems;
# MAGIC 
# MAGIC 
# MAGIC -- ─── 11. silver_plating_prep_jig_solutions ───
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_plating_prep_jig_solutions
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     cr535_amp                  AS amp,
# MAGIC     cr535_parentjigname        AS parent_jig_name,
# MAGIC     cr535_solutionname         AS solution_name,
# MAGIC     cr535_thickness            AS thickness,
# MAGIC     cr535_time                 AS time,
# MAGIC     cr535_totalsurface         AS total_surface,
# MAGIC     cr535_volt                 AS volt,
# MAGIC     createdon                  AS created_on,
# MAGIC     dts_sequence               AS sequence,
# MAGIC     modifiedon                 AS modified_on,
# MAGIC     cr535_jignotext            AS jig_no_text
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.cr535_platingprep_jigsolutions;
# MAGIC 
# MAGIC 
# MAGIC -- ─── 12. silver_plating_replenishers_v2 ───
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_plating_replenishers_v2
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     cr535_premium_percentage   AS premium_percentage,
# MAGIC     cr535_replenishersname     AS replenishers_name,
# MAGIC     createdon                  AS created_on,
# MAGIC     dts_baseconcentration      AS base_concentration,
# MAGIC     dts_description            AS description,
# MAGIC     dts_elementidname          AS element_id_name,
# MAGIC     dts_notes                  AS notes,
# MAGIC     modifiedon                 AS modified_on
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.dts_platingreplenishersv2;
# MAGIC 
# MAGIC 
# MAGIC -- ─── 13. silver_plating_routing ───
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_plating_routing
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     CASE 
# MAGIC         WHEN cr535_type LIKE '%184930000%' THEN 'prep'
# MAGIC         WHEN cr535_type LIKE '%184930001%' THEN 'plate'
# MAGIC         WHEN cr535_type LIKE '%184930004%' THEN 'pen'
# MAGIC         WHEN cr535_type LIKE '%184930005%' THEN 'mask'
# MAGIC         WHEN cr535_type LIKE '%184930006%' THEN 'unmask'
# MAGIC         WHEN cr535_type LIKE '%184930002%' THEN 'finish'
# MAGIC         WHEN cr535_type LIKE '%184930003%' THEN 'dry'
# MAGIC         ELSE CAST(cr535_type AS STRING)
# MAGIC     END AS type,
# MAGIC     CASE 
# MAGIC         WHEN cr535_xrflayer LIKE '%184930000%' THEN 'D1'
# MAGIC         WHEN cr535_xrflayer LIKE '%184930001%' THEN 'D2'
# MAGIC         ELSE CAST(cr535_xrflayer AS STRING)
# MAGIC     END AS xrflayer,
# MAGIC     cr535_amp                  AS amp,
# MAGIC     cr535_elementpremium       AS element_premium,
# MAGIC     cr535_elementtext          AS element_text,
# MAGIC     cr535_ispreciousmetal      AS is_precious_metal,
# MAGIC     cr535_platingroutingitem   AS plating_routing_item,
# MAGIC     cr535_solutiondensity      AS solution_density,
# MAGIC     cr535_volt                 AS volt,
# MAGIC     createdon                  AS created_on,
# MAGIC     dts_currentdensity         AS current_density,
# MAGIC     dts_description            AS description,
# MAGIC     dts_fixedtime              AS fixed_time,
# MAGIC     dts_machineidname          AS machine_id_name,
# MAGIC     dts_platingsolutionidname  AS plating_solution_id_name,
# MAGIC     dts_platingthickness       AS plating_thickness,
# MAGIC     dts_routingversionidname   AS routing_version_id_name,
# MAGIC     dts_stepnumber             AS step_number,
# MAGIC     modifiedon                 AS modified_on
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.dts_platingrouting;
# MAGIC 
# MAGIC 
# MAGIC -- ─── 14. silver_plating_routing_parent ───
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_plating_routing_parent
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     createdon                  AS created_on,
# MAGIC     dts_itemnoname             AS itemnoname,
# MAGIC     modifiedon                 AS modified_on
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.dts_platingroutingparent;
# MAGIC 
# MAGIC 
# MAGIC -- ─── 15. silver_plating_routing_version ───
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_plating_routing_version
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     CASE 
# MAGIC         WHEN dts_approvalstatus LIKE '%780350000%' THEN 'Draft'
# MAGIC         WHEN dts_approvalstatus LIKE '%780350001%' THEN 'Pending Approval'
# MAGIC         WHEN dts_approvalstatus LIKE '%780350002%' THEN 'Approved'
# MAGIC         WHEN dts_approvalstatus LIKE '%780350003%' THEN 'Rejected'
# MAGIC         ELSE CAST(dts_approvalstatus AS STRING)
# MAGIC     END AS approvalstatus,
# MAGIC     createdon                  AS created_on,
# MAGIC     dts_parentitemnotext       AS parentitemnotext,
# MAGIC     dts_rejectionreason        AS rejectionreason,
# MAGIC     dts_totalsteps             AS totalsteps,
# MAGIC     dts_versioncode            AS versioncode,
# MAGIC     dts_versionid              AS versionid,
# MAGIC     modifiedon                 AS modified_on
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.dts_platingroutingversion;
# MAGIC 
# MAGIC 
# MAGIC -- ─── 16. silver_plating_solution_composition ───
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_plating_solution_composition
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     CASE 
# MAGIC         WHEN cr535_preciousmetaltype LIKE '%184930000%' THEN 'None'
# MAGIC         WHEN cr535_preciousmetaltype LIKE '%184930001%' THEN 'Silver'
# MAGIC         WHEN cr535_preciousmetaltype LIKE '%184930002%' THEN 'Gold'
# MAGIC         WHEN cr535_preciousmetaltype LIKE '%184930003%' THEN 'Platinum'
# MAGIC         WHEN cr535_preciousmetaltype LIKE '%184930004%' THEN 'Palladium'
# MAGIC         WHEN cr535_preciousmetaltype LIKE '%184930005%' THEN 'Rhodium'
# MAGIC         WHEN cr535_preciousmetaltype LIKE '%184930006%' THEN 'Ruthenium'
# MAGIC         WHEN cr535_preciousmetaltype LIKE '%780350001%' THEN 'Indium'
# MAGIC         WHEN cr535_preciousmetaltype LIKE '%184930007%' THEN 'Cobalt'
# MAGIC         WHEN cr535_preciousmetaltype LIKE '%184930008%' THEN 'Zinc'
# MAGIC         WHEN cr535_preciousmetaltype LIKE '%184930009%' THEN 'Tin'
# MAGIC         WHEN cr535_preciousmetaltype LIKE '%184930010%' THEN 'Organic'
# MAGIC         WHEN cr535_preciousmetaltype LIKE '%184930011%' THEN 'Copper'
# MAGIC         ELSE CAST(cr535_preciousmetaltype AS STRING)
# MAGIC     END AS precious_metal_type,
# MAGIC     CASE 
# MAGIC         WHEN wm_interval LIKE '%127740000%' THEN 'Extra Ad hoc'
# MAGIC         WHEN wm_interval LIKE '%127740001%' THEN 'AMP/Min'
# MAGIC         ELSE CAST(wm_interval AS STRING)
# MAGIC     END AS interval,
# MAGIC     cr535_replenisherquantity   AS replenisher_quantity,
# MAGIC     cr535_replenishmentinterval AS replenishment_interval,
# MAGIC     cr535_uom                   AS uom,
# MAGIC     createdon                   AS created_on,
# MAGIC     dts_density                 AS density,
# MAGIC     dts_maxconcentration        AS max_concentration,
# MAGIC     dts_minconcentration        AS min_concentration,
# MAGIC     dts_platingsolutionidname   AS plating_solution_id_name,
# MAGIC     dts_replenisherquantity     AS replenisher_quantity_dts,
# MAGIC     dts_replenishersidname      AS replenishers_id_name,
# MAGIC     dts_solutioncompositionid   AS solution_composition_id,
# MAGIC     modifiedon                  AS modified_on
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.dts_platingsolutioncomposition;
# MAGIC 
# MAGIC 
# MAGIC -- ─── 17. silver_plating_solution_master_data ───
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_plating_solution_master_data
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     CASE 
# MAGIC         WHEN cr535_icpfrequency LIKE '%184930000%' THEN 'Daily'
# MAGIC         WHEN cr535_icpfrequency LIKE '%184930001%' THEN 'Weekly'
# MAGIC         WHEN cr535_icpfrequency LIKE '%184930002%' THEN 'Bi-Weekly'
# MAGIC         WHEN cr535_icpfrequency LIKE '%184930003%' THEN 'Monthly'
# MAGIC         WHEN cr535_icpfrequency LIKE '%184930004%' THEN 'Custom'
# MAGIC         ELSE CAST(cr535_icpfrequency AS STRING)
# MAGIC     END AS icp_frequency,
# MAGIC     CASE 
# MAGIC         WHEN dts_controlmode LIKE '%780350000%' THEN 'CC'
# MAGIC         WHEN dts_controlmode LIKE '%780350001%' THEN 'CV'
# MAGIC         ELSE CAST(dts_controlmode AS STRING)
# MAGIC     END AS control_mode,
# MAGIC     CASE 
# MAGIC         WHEN dts_solutionstatus LIKE '%780350000%' THEN 'OK'
# MAGIC         WHEN dts_solutionstatus LIKE '%780350001%' THEN 'WARNING'
# MAGIC         WHEN dts_solutionstatus LIKE '%780350002%' THEN 'HOLD'
# MAGIC         WHEN dts_solutionstatus LIKE '%780350003%' THEN 'BLOCKED'
# MAGIC         ELSE CAST(dts_solutionstatus AS STRING)
# MAGIC     END AS solution_status,
# MAGIC     CASE 
# MAGIC         WHEN cr535_icptestdays LIKE '%184930000%' THEN 'Mo'
# MAGIC         WHEN cr535_icptestdays LIKE '%184930001%' THEN 'Tu'
# MAGIC         WHEN cr535_icptestdays LIKE '%184930002%' THEN 'Wed'
# MAGIC         WHEN cr535_icptestdays LIKE '%184930003%' THEN 'Thur'
# MAGIC         WHEN cr535_icptestdays LIKE '%184930005%' THEN 'Fri'
# MAGIC         WHEN cr535_icptestdays LIKE '%184930004%' THEN 'Sat'
# MAGIC         ELSE CAST(cr535_icptestdays AS STRING)
# MAGIC     END AS icp_test_days,
# MAGIC     cr535_elementname           AS element_name,
# MAGIC     cr535_fixedtime             AS fixed_time,
# MAGIC     cr535_phrangemax            AS ph_range_max,
# MAGIC     cr535_phrangemin            AS ph_range_min,
# MAGIC     cr535_platinglosspercentage AS plating_loss_percentage,
# MAGIC     cr535_variablefactor        AS variable_factor,
# MAGIC     cr535_voltmax               AS volt_max,
# MAGIC     cr535_voltmin               AS volt_min,
# MAGIC     cr535_voltoptimal           AS volt_optimal,
# MAGIC     createdon                   AS created_on,
# MAGIC     dts_alloydeposition         AS alloy_deposition,
# MAGIC     dts_ampereperdm2            AS ampereperdm2,
# MAGIC     dts_currentdensitymax       AS current_density_max,
# MAGIC     dts_density                 AS density,
# MAGIC     dts_efficiency_percentage   AS efficiency_percentage,
# MAGIC     dts_finemetaldeposition     AS fine_metal_deposition,
# MAGIC     dts_makeupdate              AS make_update,
# MAGIC     dts_phrangeadvised          AS ph_range_advised,
# MAGIC     dts_platingmachinename      AS plating_machine_name,
# MAGIC     dts_platingmachines         AS plating_machines,
# MAGIC     dts_platingsolution         AS plating_solution,
# MAGIC     dts_replenishmentcycle      AS replenishment_cycle,
# MAGIC     dts_solutiondescription     AS solution_description,
# MAGIC     dts_temprangeadvised        AS temp_range_advised,
# MAGIC     dts_temprangemax            AS temp_range_max,
# MAGIC     dts_temprangemin            AS temp_range_min,
# MAGIC     modifiedon                  AS modified_on,
# MAGIC     cr535_advised               AS advised,
# MAGIC     cr535_type                  AS type,
# MAGIC     dts_hotwatertemp            AS hot_water_temp
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.dts_platingsolutionmasterdata;
# MAGIC 
# MAGIC 
# MAGIC -- ─── 18. silver_plating_solution_specific_replenisher ───
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_plating_solution_specific_replenisher
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     createdon                   AS created_on,
# MAGIC     dts_elementidname           AS element_id_name,
# MAGIC     dts_maxconcentration        AS max_concentration,
# MAGIC     dts_minconcentration        AS min_concentration,
# MAGIC     dts_solutionidname          AS solution_id_name,
# MAGIC     dts_unitofmeasure           AS unit_of_measure,
# MAGIC     modifiedon                  AS modified_on
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.dts_platingsolutionspecificreplenisher;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # silver_plating_lab_test_result

# CELL ********************

# MAGIC %%sql
# MAGIC -- 19. silver_plating_lab_test_result
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_plating_lab_test_result
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN `cr535_testtype` LIKE '%184930000%' THEN 'ICP Test'
# MAGIC         WHEN `cr535_testtype` LIKE '%184930001%' THEN 'Element Test'
# MAGIC         WHEN `cr535_testtype` LIKE '%780350001%' THEN 'XRF'
# MAGIC         WHEN `cr535_testtype` LIKE '%780350002%' THEN 'Xray karat %'
# MAGIC         WHEN `cr535_testtype` LIKE '%780350003%' THEN 'Xray gr/l'
# MAGIC         ELSE CAST(`cr535_testtype` AS STRING)
# MAGIC     END AS `test_type`,
# MAGIC     `dts_correctiveactionidname` AS `corrective_action_id`,
# MAGIC     `cr535_elementname`          AS `element`,
# MAGIC     `cr535_solutionname`         AS `solution`,
# MAGIC     `createdon`                  AS `created_on`,
# MAGIC     CASE
# MAGIC         WHEN `dts_status` LIKE '%780350000%' THEN 'Pass'
# MAGIC         WHEN `dts_status` LIKE '%780350001%' THEN 'Warning'
# MAGIC         WHEN `dts_status` LIKE '%780350002%' THEN 'Fail'
# MAGIC         ELSE CAST(`dts_status` AS STRING)
# MAGIC     END AS `status`,
# MAGIC     `dts_customerno`     AS `customer_no`,
# MAGIC     `dts_elementidname`  AS `solution_element`,
# MAGIC     `dts_machineid`      AS `machine_id`,
# MAGIC     `dts_name`           AS `name`,
# MAGIC     `dts_tankidname`     AS `tank_id`,
# MAGIC     `dts_testdate`       AS `testdate`,
# MAGIC     `modifiedon`         AS `modified_on`,
# MAGIC     `dts_unit`           AS `unit`,
# MAGIC     `dts_value`          AS `value`
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.dts_labtestresult;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # silver_xrf_inspection_session

# CELL ********************

# MAGIC %%sql
# MAGIC -- silver_xrf_inspection_session
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_plating_xrf_inspection_session
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN `dts_source` LIKE '%780350000%' THEN 'Manual'
# MAGIC         WHEN `dts_source` LIKE '%780350001%' THEN 'File Import'
# MAGIC         ELSE CAST(`dts_source` AS STRING)
# MAGIC     END AS `source`,
# MAGIC     CASE
# MAGIC         WHEN `dts_verdict` LIKE '%780350000%' THEN 'Pass'
# MAGIC         WHEN `dts_verdict` LIKE '%780350001%' THEN 'Fail'
# MAGIC         WHEN `dts_verdict` LIKE '%780350002%' THEN 'Marginal'
# MAGIC         ELSE CAST(`dts_verdict` AS STRING)
# MAGIC     END AS `verdict`,
# MAGIC     `createdon`                AS `created_on`,
# MAGIC     `dts_customername`         AS `customer_name`,
# MAGIC     `dts_customerno`           AS `customer_no`,
# MAGIC     `dts_inspectiondate`       AS `inspection_date`,
# MAGIC     `dts_inspector`            AS `inspector`,
# MAGIC     `dts_jigno`                AS `jig_no`,
# MAGIC     `dts_name`                 AS `session_id`,
# MAGIC     `dts_platingitem`          AS `plating_item`,
# MAGIC     `dts_ppn`                  AS `ppn`,
# MAGIC     `dts_solution`             AS `solution`,
# MAGIC     `dts_toleranceprofilename` AS `tolerance_profile`,
# MAGIC     `modifiedon`               AS `modified_on`
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.dts_xrfinspectionsession

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # silver_plating_xrf_reading

# CELL ********************

# MAGIC %%sql
# MAGIC -- silver_xrf_reading
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_plating_xrf_reading
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN `dts_d1verdict` LIKE '%780350000%' THEN 'Pass'
# MAGIC         WHEN `dts_d1verdict` LIKE '%780350001%' THEN 'Fail'
# MAGIC         WHEN `dts_d1verdict` LIKE '%780350002%' THEN 'Marginal'
# MAGIC         WHEN `dts_d1verdict` LIKE '%780350003%' THEN 'Skip'
# MAGIC         ELSE CAST(`dts_d1verdict` AS STRING)
# MAGIC     END AS `d1_verdict`,
# MAGIC     CASE
# MAGIC         WHEN `dts_d2verdict` LIKE '%780350000%' THEN 'Pass'
# MAGIC         WHEN `dts_d2verdict` LIKE '%780350001%' THEN 'Fail'
# MAGIC         WHEN `dts_d2verdict` LIKE '%780350002%' THEN 'Marginal'
# MAGIC         WHEN `dts_d2verdict` LIKE '%780350003%' THEN 'Skip'
# MAGIC         ELSE CAST(`dts_d2verdict` AS STRING)
# MAGIC     END AS `d2_verdict`,
# MAGIC     CASE
# MAGIC         WHEN `dts_jigzone` LIKE '%780350000%' THEN 'Top'
# MAGIC         WHEN `dts_jigzone` LIKE '%780350001%' THEN 'Middle'
# MAGIC         WHEN `dts_jigzone` LIKE '%780350002%' THEN 'Lower'
# MAGIC         ELSE CAST(`dts_jigzone` AS STRING)
# MAGIC     END AS `jig_zone`,
# MAGIC     `dts_ag3m`                  AS `ag3m`,
# MAGIC     `createdon`                 AS `created_on`,
# MAGIC     `dts_d1m`                   AS `d1m`,
# MAGIC     `dts_d1platingsolutiontext` AS `d1_plating_solution_text`,
# MAGIC     `dts_d1targetused`          AS `d1_target_used`,
# MAGIC     `dts_d1tolminus`            AS `d1_tol_minus`,
# MAGIC     `dts_d1tolplus`             AS `d1_tol_plus`,
# MAGIC     `dts_d2m`                   AS `d2m`,
# MAGIC     `dts_d2platingsolutiontext` AS `d2_plating_solution_text`,
# MAGIC     `dts_d2targetused`          AS `d2_target_used`,
# MAGIC     `dts_d2tolminus`            AS `d2_tol_minus`,
# MAGIC     `dts_d2tolplus`             AS `d2_tol_plus`,
# MAGIC     `dts_itemcode`              AS `item_code`,
# MAGIC     `dts_itemno`                AS `item_no`,
# MAGIC     `dts_name`                  AS `xrf_session_id`,
# MAGIC     `dts_mg`                    AS `mq`,
# MAGIC     `dts_pieceno`               AS `piece_no`,
# MAGIC     `dts_spotno`                AS `spot_no`,
# MAGIC     `dts_xrfsessionname`        AS `xrf_session`,
# MAGIC     `modifiedon`                AS `modified_on`
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.dts_xrfreading

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # silver_plating_xrf_tolerance_limit

# CELL ********************

# MAGIC %%sql
# MAGIC -- silver_plating_xrf_tolerance_limit
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_plating_xrf_tolerance_limit
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN `dts_layer` LIKE '%780350000%' THEN 'd1'
# MAGIC         WHEN `dts_layer` LIKE '%780350001%' THEN 'd2'
# MAGIC         ELSE CAST(`dts_layer` AS STRING)
# MAGIC     END AS `layer`,
# MAGIC     CASE
# MAGIC         WHEN `dts_metaltype` LIKE '%780350000%' THEN 'AG'
# MAGIC         WHEN `dts_metaltype` LIKE '%780350001%' THEN 'PT'
# MAGIC         WHEN `dts_metaltype` LIKE '%780350002%' THEN '14K'
# MAGIC         WHEN `dts_metaltype` LIKE '%780350003%' THEN '18K'
# MAGIC         WHEN `dts_metaltype` LIKE '%780350004%' THEN '23K'
# MAGIC         ELSE CAST(`dts_metaltype` AS STRING)
# MAGIC     END AS `metal_type`,
# MAGIC     `createdon`           AS `created_on`,
# MAGIC     `dts_name`            AS `tolerance_limit_id`,
# MAGIC     `dts_platingitemcode` AS `plating_item_code`,
# MAGIC     `dts_profileidname`   AS `profile_id_name`,
# MAGIC     `dts_tolminus`        AS `tol_minus`,
# MAGIC     `dts_tolplus`         AS `tol_plus`,
# MAGIC     `modifiedon`          AS `modified_on`
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.dts_xrftolerancelimit

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # silver_plating_xrf_tolerance_profile

# CELL ********************

# MAGIC %%sql
# MAGIC -- silver_plating_xrf_tolerance_profile
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_plating_xrf_tolerance_profile
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN `dts_status` LIKE '%780350000%' THEN 'Active'
# MAGIC         WHEN `dts_status` LIKE '%780350001%' THEN 'Inactive'
# MAGIC         ELSE CAST(`dts_status` AS STRING)
# MAGIC     END AS `status`,
# MAGIC     `createdon`      AS `created_on`,
# MAGIC     `dts_customerno` AS `customer_no`,
# MAGIC     `modifiedon`     AS `modified_on`,
# MAGIC     `dts_name`       AS `profile_id`
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.dts_xrftoleranceprofile

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

NS = "Silver_Production_Lakehouse.prod"     # lakehouse.schema (ปรับตามของคุณ; ถ้า attach เป็น default แล้วใช้แค่ "prod")
PREFIX = "silver_plating_"

tables = sorted(r.tableName for r in spark.sql(f"SHOW TABLES IN {NS}").collect()
                if r.tableName.startswith(PREFIX))

data = [(t, i, col, dtype)
        for t in tables
        for i, (col, dtype) in enumerate(spark.table(f"{NS}.{t}").dtypes)]

catalog = spark.createDataFrame(
    data, "table string, ordinal int, column string, data_type string")

display(catalog.orderBy("table", "ordinal").drop("ordinal"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
