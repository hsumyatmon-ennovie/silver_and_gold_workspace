# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "81bc6bea-77b8-46fe-9189-dcfc3cd43d2f",
# META       "default_lakehouse_name": "Silver_Product_Dev_Lakehouse",
# META       "default_lakehouse_workspace_id": "d74457b3-045c-445d-82c6-9a2e4b9f1436",
# META       "known_lakehouses": [
# META         {
# META           "id": "81bc6bea-77b8-46fe-9189-dcfc3cd43d2f"
# META         },
# META         {
# META           "id": "785307fd-af78-4359-969a-51c937ec834b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Silver_Product_Dev_Lakehouse.pd.silver_pd_step AS
# MAGIC SELECT DISTINCT
# MAGIC     dts_sequenceorder AS pd_sequence,
# MAGIC     dts_stepname      AS pd_step
# MAGIC FROM  dataverse_ennoviedev_cds2_workspace_unq85a0b4fa330ef111afc0000d3a80b.dts_npd_cadroutingsteps
# MAGIC WHERE dts_sequenceorder IS NOT NULL
# MAGIC   AND dts_stepname IS NOT NULL;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

df = spark.read.format("delta").load(
    "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/"
    "dataverse_ennoviedev_cds2_workspace_unq85a0b4fa330ef111afc0000d3a80b.Lakehouse/"
    "Tables/dts_npd_cadroutingsteps"
)

display(df.limit(10))

silver_pd_step_df = (
    df.selectExpr(
        "dts_sequenceorder as pd_sequence",
        "dts_stepname as pd_step"
    )
    .where("dts_sequenceorder is not null and dts_stepname is not null")
    .distinct()
)

display(silver_pd_step_df)

silver_pd_step_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .save("Tables/pd/silver_pd_step")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
