# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "76781d83-17f8-4270-a81d-6759d1ee9a9d",
# META       "default_lakehouse_name": "Gold_Inventory_Lakehouse",
# META       "default_lakehouse_workspace_id": "d74457b3-045c-445d-82c6-9a2e4b9f1436",
# META       "known_lakehouses": [
# META         {
# META           "id": "76781d83-17f8-4270-a81d-6759d1ee9a9d"
# META         }
# META       ]
# META     },
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

spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead",  "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead",     "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite",    "LEGACY")

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql import functions as F, Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Inventory Balance

# CELL ********************

# Spark (Delta) incremental load -> gold_inventory_balance
# Implements your SQL:
#  - Item (i) INNER JOIN Item Ledger Entry (ile) on (Item No = No) AND (BC Company)
#  - Filter: i.Blocked = 0
#  - Group by: BC Company, Item No, Description, Base UOM, UOM2, Location Code
#  - Aggregates: SUM(Remaining Quantity), SUM(Units_DU_TSL)
#  - HAVING: SUM(Remaining Quantity) <> 0
#
# Increment strategy:
#  - watermark driven by Item Ledger Entry SystemModifiedAt (fallback SystemCreatedAt)
#  - rebuild impacted (BC Company, Item No, Location Code) groups so sums stay correct
# MERGE key:
#  - (BC_Company, Item_No, Location_Code)

from pyspark.sql import functions as F

TARGET = "Gold_Inventory_Lakehouse.inv.gold_inventory_balance"
I_SRC   = "Silver_BC_Lakehouse.bc.`Item`"
ILE_SRC = "Silver_BC_Lakehouse.bc.`Item Ledger Entry`"

# 1) Create target if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    BC_Company STRING,
    Item_No STRING,
    Item_Description STRING,
    UOM STRING,
    UOM2 STRING,
    Location_Code STRING,
    Remaining_Quantity_Sum DECIMAL(38,10),
    UOM2_QTY DECIMAL(38,10),

    -- tracking
    ILE_SystemCreatedAt TIMESTAMP,
    ILE_SystemModifiedAt TIMESTAMP
)
USING DELTA
""")

# 2) Watermark from target
wm = (
    spark.table(TARGET)
    .select(F.max(F.col("ILE_SystemModifiedAt")).alias("wm"))
    .collect()[0]["wm"]
)
if wm is None:
    wm = "1900-01-01 00:00:00"

# 3) Read sources
i   = spark.table(I_SRC).alias("i")
ile = spark.table(ILE_SRC).alias("ile")

d38 = lambda c: F.col(c).cast("decimal(38,10)")

# 4) Find impacted groups from incremental ILE changes
ile_inc = (
    ile.withColumn("wm_ts", F.coalesce(F.col("`SystemModifiedAt`"), F.col("`SystemCreatedAt`")))
       .filter(F.col("wm_ts") > F.to_timestamp(F.lit(str(wm))))
       .select(
           F.col("`BC Company`").alias("BC_Company"),
           F.col("`Item No.`").alias("Item_No"),
           F.col("`Location Code`").alias("Location_Code"),
       )
       .distinct()
       .alias("ile_inc")
)

# First run -> rebuild all groups
first_run = (str(wm) == "1900-01-01 00:00:00")
if first_run:
    impacted = (
        ile.select(
            F.col("`BC Company`").alias("BC_Company"),
            F.col("`Item No.`").alias("Item_No"),
            F.col("`Location Code`").alias("Location_Code"),
        ).distinct().alias("imp")
    )
else:
    impacted = ile_inc.alias("imp")

# 5) Rebuild ILE rows for impacted groups
ile_rb = (
    ile.alias("ile0")
       .join(
           impacted,
           on=(
               (F.col("ile0.`BC Company`") == F.col("imp.BC_Company")) &
               (F.col("ile0.`Item No.`") == F.col("imp.Item_No")) &
               (F.col("ile0.`Location Code`") == F.col("imp.Location_Code"))
           ),
           how="inner"
       )
       .alias("ile_rb")
)

# 6) Join to Item + filter blocked
joined = (
    ile_rb.join(
        i,
        on=(
            (F.col("ile_rb.`Item No.`") == F.col("i.`No.`")) &
            (F.col("ile_rb.`BC Company`") == F.col("i.`BC Company`"))
        ),
        how="inner"
    )
    .filter(F.col("i.`Blocked`") == F.lit(0))
)

# 7) Aggregate
agg = (
    joined.groupBy(
        F.col("i.`BC Company`").alias("BC_Company"),
        F.col("i.`No.`").alias("Item_No"),
        F.col("i.`Description`").alias("Item_Description"),
        F.col("i.`Base Unit of Measure`").alias("UOM"),
        F.col("i.`Unit of Measure - Units_DU_TSL`").alias("UOM2"),
        F.col("ile_rb.`Location Code`").alias("Location_Code"),
    )
    .agg(
        F.sum(d38("ile_rb.`Remaining Quantity`")).alias("Remaining_Quantity_Sum"),
        F.sum(d38("ile_rb.`Units_DU_TSL`")).alias("UOM2_QTY"),
        F.max(F.col("ile_rb.`SystemCreatedAt`")).alias("ILE_SystemCreatedAt"),
        F.max(F.col("ile_rb.`SystemModifiedAt`")).alias("ILE_SystemModifiedAt"),
    )
    .filter(F.col("Remaining_Quantity_Sum") != F.lit(0).cast("decimal(38,10)"))  # HAVING SUM(Remaining Qty) <> 0
)

agg.createOrReplaceTempView("gold_inventory_balance_inc")

# 8) MERGE
spark.sql(f"""
MERGE INTO {TARGET} AS t
USING gold_inventory_balance_inc AS s
ON  t.BC_Company = s.BC_Company
AND t.Item_No = s.Item_No
AND t.Location_Code = s.Location_Code
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Component Status

# CELL ********************

# Spark (Delta) FULL REPLACE -> gold_component_status
# Hardened: uses try_cast for all DECIMAL fields to avoid ANSI cast failures (e.g., 'CST_CUT')
# Full refresh strategy: TRUNCATE + INSERT with explicit column list (no SELECT *)

from pyspark.sql import functions as F

TARGET = "Gold_Inventory_Lakehouse.inv.gold_component_status"

SH_SRC  = "Silver_BC_Lakehouse.bc.`Sales Header`"
PO_SRC  = "Silver_BC_Lakehouse.bc.`Production Order`"
POL_SRC = "Silver_BC_Lakehouse.bc.`Prod Order Line`"
POC_SRC = "Silver_BC_Lakehouse.bc.`Prod Order Component`"
ILE_SRC = "Silver_BC_Lakehouse.bc.`Item Ledger Entry`"
I_SRC   = "Silver_BC_Lakehouse.bc.`Item`"

# -----------------------------
# Helpers
# -----------------------------
def dec38_try(sql_col: str):
    """
    sql_col must be a SQL-valid column reference like:
      - "po.`Quantity`"
      - "poc_f.`Remaining Quantity`"
      - "ile.`Quantity`"
    Returns decimal(38,10) or NULL if malformed (ANSI-safe).
    """
    return F.expr(f"try_cast({sql_col} as decimal(38,10))")

DEC0 = F.lit(0).cast("decimal(38,10)")

# -----------------------------
# 1) Create target table if not exists
# -----------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    BC_Company STRING,
    Document_Date DATE,
    Requested_Delivery_Date DATE,
    Sell_to_Customer_Name STRING,
    SO_STATUS STRING,

    Prod_Status STRING,
    SalesOrderNo STRING,
    SalesLineNo INT,
    Prod__OrderNo STRING,
    FG_Item STRING,
    FG_QTY DECIMAL(38,10),

    EnnovieDueDate DATE,
    EnnovieStattingDate TIMESTAMP,
    EnnovieEndingDate TIMESTAMP,

    Prod__Order_LineNo INT,
    ComponentLineNo INT,
    Component_Item STRING,
    Description STRING,
    Component_Quantity DECIMAL(38,10),
    Component_Quantityper DECIMAL(38,10),
    Expected_Quantity DECIMAL(38,10),
    Remaining_Quantity DECIMAL(38,10),
    Unit_of_Measure_Code STRING,
    Component_UOM2Per DECIMAL(38,10),
    Component_UOM2 STRING,
    Component_UOM2Expected DECIMAL(38,10),

    Component_ActUOM2 DECIMAL(38,10),
    Act__Consumption__Qty DECIMAL(38,10),

    Location_Code STRING,
    SystemModifiedAt TIMESTAMP,
    Item_Category_Code STRING
)
USING DELTA
""")

# -----------------------------
# 2) Read sources
# -----------------------------
sh  = spark.table(SH_SRC).alias("sh")
po  = spark.table(PO_SRC).alias("po")
pol = spark.table(POL_SRC).alias("pol")
poc = spark.table(POC_SRC).alias("poc")
ile = spark.table(ILE_SRC).alias("ile")
it  = spark.table(I_SRC).alias("i")

# -----------------------------
# 3) Apply WHERE filter early (same semantics)
# Remaining Quantity > 0, but tolerant to malformed values
# -----------------------------
poc_f = (
    poc.alias("poc_f")
       .filter(F.coalesce(dec38_try("poc_f.`Remaining Quantity`"), DEC0) > DEC0)
)

# -----------------------------
# 4) Join chain (same as SQL)
# -----------------------------
joined = (
    sh.join(
        po,
        on=(
            (F.col("po.`BC Company`") == F.col("sh.`BC Company`")) &
            (F.col("po.`Sales Order No.`") == F.col("sh.`No.`"))
        ),
        how="inner"
    )
    .join(
        pol,
        on=(
            (F.col("pol.`BC Company`") == F.col("po.`BC Company`")) &
            (F.col("pol.`Prod. Order No.`") == F.col("po.`No.`"))
        ),
        how="inner"
    )
    .join(
        poc_f,
        on=(
            (F.col("poc_f.`BC Company`") == F.col("pol.`BC Company`")) &
            (F.col("poc_f.`Prod. Order No.`") == F.col("pol.`Prod. Order No.`")) &
            (F.col("poc_f.`Prod. Order Line No.`") == F.col("pol.`Line No.`"))
        ),
        how="inner"
    )
    .join(
        it,
        on=(
            (F.col("i.`BC Company`") == F.col("poc_f.`BC Company`")) &
            (F.col("i.`No.`") == F.col("poc_f.`Item No.`"))
        ),
        how="left"
    )
    .join(
        ile,
        on=(
            (F.col("ile.`BC Company`") == F.col("poc_f.`BC Company`")) &
            (F.col("ile.`Entry Type`") == F.lit("Consumption")) &
            (F.col("ile.`Order No.`") == F.col("poc_f.`Prod. Order No.`")) &
            (F.col("ile.`Prod. Order Comp. Line No.`") == F.col("poc_f.`Line No.`")) &
            (F.col("ile.`Item No.`") == F.col("poc_f.`Item No.`"))
        ),
        how="left"
    )
    .filter(F.col("po.`Status`").isin(["Released", "Firm Planned"]))
    .filter(~F.col("po.`No.`").like("CAS%"))
)

# -----------------------------
# 5) Aggregate (GROUP BY + SUM)
# All decimal expressions use try_cast to avoid ANSI failures
# -----------------------------
agg = (
    joined.groupBy(
        F.col("sh.`BC Company`").alias("BC_Company"),
        F.to_date(F.col("sh.`Document Date`")).alias("Document_Date"),
        F.to_date(F.col("sh.`Requested Delivery Date`")).alias("Requested_Delivery_Date"),
        F.col("sh.`Sell-to Customer Name`").alias("Sell_to_Customer_Name"),
        F.col("sh.`Status`").alias("SO_STATUS"),

        F.col("po.`Status`").alias("Prod_Status"),
        F.col("po.`Sales Order No.`").alias("SalesOrderNo"),
        F.col("po.`Sales Order Line No.`").cast("int").alias("SalesLineNo"),
        F.col("po.`No.`").alias("Prod__OrderNo"),
        F.col("po.`Source No.`").alias("FG_Item"),
        dec38_try("po.`Quantity`").alias("FG_QTY"),

        F.to_date(F.col("pol.`Due Date`")).alias("EnnovieDueDate"),
        F.col("pol.`Starting Date-Time`").alias("EnnovieStattingDate"),
        F.col("pol.`Ending Date-Time`").alias("EnnovieEndingDate"),

        F.col("poc_f.`Prod. Order Line No.`").cast("int").alias("Prod__Order_LineNo"),
        F.col("poc_f.`Line No.`").cast("int").alias("ComponentLineNo"),
        F.col("poc_f.`Item No.`").alias("Component_Item"),
        F.col("poc_f.`Description`").alias("Description"),

        dec38_try("poc_f.`Quantity`").alias("Component_Quantity"),
        dec38_try("poc_f.`Quantity per`").alias("Component_Quantityper"),
        dec38_try("poc_f.`Expected Quantity`").alias("Expected_Quantity"),
        dec38_try("poc_f.`Remaining Quantity`").alias("Remaining_Quantity"),

        F.col("poc_f.`Unit of Measure Code`").alias("Unit_of_Measure_Code"),
        dec38_try("poc_f.`Units per_DU_TSL`").alias("Component_UOM2Per"),
        F.col("poc_f.`Unit of Measure - Units_DU_TSL`").alias("Component_UOM2"),
        dec38_try("poc_f.`Expected Units_DU_TSL`").alias("Component_UOM2Expected"),

        F.col("poc_f.`Location Code`").alias("Location_Code"),
        F.col("poc_f.`SystemModifiedAt`").alias("SystemModifiedAt"),

        F.col("i.`Item Category Code`").alias("Item_Category_Code"),
    )
    .agg(
        F.coalesce(F.sum(dec38_try("ile.`Units_DU_TSL`")), DEC0).alias("Component_ActUOM2"),
        F.coalesce(F.sum(dec38_try("ile.`Quantity`")),     DEC0).alias("Act__Consumption__Qty"),
    )
)

# -----------------------------
# 6) Force final types to exactly match target (prevents implicit ANSI casts on insert)
# -----------------------------
final_df = agg.select(
    F.col("BC_Company").cast("string").alias("BC_Company"),
    F.col("Document_Date").cast("date").alias("Document_Date"),
    F.col("Requested_Delivery_Date").cast("date").alias("Requested_Delivery_Date"),
    F.col("Sell_to_Customer_Name").cast("string").alias("Sell_to_Customer_Name"),
    F.col("SO_STATUS").cast("string").alias("SO_STATUS"),

    F.col("Prod_Status").cast("string").alias("Prod_Status"),
    F.col("SalesOrderNo").cast("string").alias("SalesOrderNo"),
    F.col("SalesLineNo").cast("int").alias("SalesLineNo"),
    F.col("Prod__OrderNo").cast("string").alias("Prod__OrderNo"),
    F.col("FG_Item").cast("string").alias("FG_Item"),
    F.expr("try_cast(FG_QTY as decimal(38,10))").alias("FG_QTY"),

    F.col("EnnovieDueDate").cast("date").alias("EnnovieDueDate"),
    F.col("EnnovieStattingDate").cast("timestamp").alias("EnnovieStattingDate"),
    F.col("EnnovieEndingDate").cast("timestamp").alias("EnnovieEndingDate"),

    F.col("Prod__Order_LineNo").cast("int").alias("Prod__Order_LineNo"),
    F.col("ComponentLineNo").cast("int").alias("ComponentLineNo"),
    F.col("Component_Item").cast("string").alias("Component_Item"),
    F.col("Description").cast("string").alias("Description"),
    F.expr("try_cast(Component_Quantity as decimal(38,10))").alias("Component_Quantity"),
    F.expr("try_cast(Component_Quantityper as decimal(38,10))").alias("Component_Quantityper"),
    F.expr("try_cast(Expected_Quantity as decimal(38,10))").alias("Expected_Quantity"),
    F.expr("try_cast(Remaining_Quantity as decimal(38,10))").alias("Remaining_Quantity"),
    F.col("Unit_of_Measure_Code").cast("string").alias("Unit_of_Measure_Code"),
    F.expr("try_cast(Component_UOM2Per as decimal(38,10))").alias("Component_UOM2Per"),
    F.col("Component_UOM2").cast("string").alias("Component_UOM2"),
    F.expr("try_cast(Component_UOM2Expected as decimal(38,10))").alias("Component_UOM2Expected"),

    F.expr("try_cast(Component_ActUOM2 as decimal(38,10))").alias("Component_ActUOM2"),
    F.expr("try_cast(Act__Consumption__Qty as decimal(38,10))").alias("Act__Consumption__Qty"),

    F.col("Location_Code").cast("string").alias("Location_Code"),
    F.col("SystemModifiedAt").cast("timestamp").alias("SystemModifiedAt"),
    F.col("Item_Category_Code").cast("string").alias("Item_Category_Code"),
)

final_df.createOrReplaceTempView("gold_component_status_full_typed")

# -----------------------------
# 7) FULL REPLACE write (TRUNCATE + explicit INSERT)
# -----------------------------
spark.sql(f"TRUNCATE TABLE {TARGET}")

spark.sql(f"""
INSERT INTO {TARGET} (
    BC_Company,
    Document_Date,
    Requested_Delivery_Date,
    Sell_to_Customer_Name,
    SO_STATUS,

    Prod_Status,
    SalesOrderNo,
    SalesLineNo,
    Prod__OrderNo,
    FG_Item,
    FG_QTY,

    EnnovieDueDate,
    EnnovieStattingDate,
    EnnovieEndingDate,

    Prod__Order_LineNo,
    ComponentLineNo,
    Component_Item,
    Description,
    Component_Quantity,
    Component_Quantityper,
    Expected_Quantity,
    Remaining_Quantity,
    Unit_of_Measure_Code,
    Component_UOM2Per,
    Component_UOM2,
    Component_UOM2Expected,

    Component_ActUOM2,
    Act__Consumption__Qty,

    Location_Code,
    SystemModifiedAt,
    Item_Category_Code
)
SELECT
    BC_Company,
    Document_Date,
    Requested_Delivery_Date,
    Sell_to_Customer_Name,
    SO_STATUS,

    Prod_Status,
    SalesOrderNo,
    SalesLineNo,
    Prod__OrderNo,
    FG_Item,
    FG_QTY,

    EnnovieDueDate,
    EnnovieStattingDate,
    EnnovieEndingDate,

    Prod__Order_LineNo,
    ComponentLineNo,
    Component_Item,
    Description,
    Component_Quantity,
    Component_Quantityper,
    Expected_Quantity,
    Remaining_Quantity,
    Unit_of_Measure_Code,
    Component_UOM2Per,
    Component_UOM2,
    Component_UOM2Expected,

    Component_ActUOM2,
    Act__Consumption__Qty,

    Location_Code,
    SystemModifiedAt,
    Item_Category_Code
FROM gold_component_status_full_typed
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Item Location Master

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DecimalType
from delta.tables import DeltaTable

# ----------------------------
# Config
# ----------------------------
SOURCE_TABLE = "Silver_BC_Lakehouse.bc.Item"
TARGET_TABLE = "Gold_Inventory_Lakehouse.inv.gold_item_location_master"

item_categories = [
    "STONE_DIAMOND",
    "SYNT STONE",
    "PEARLS",
    "WIRE",
    "STORE ITEM",
    "BEAD",
    "SEMI-FG",
    "FINDINGS",
    "DIAMOND",
    "FG",
    "GEMSTONES",
    "ACCESSORY"
]

# ----------------------------
# Build source dataframe
# ----------------------------
itm = spark.table(SOURCE_TABLE)

df_src = (
    itm.filter(F.col("`Item Category Code`").isin(item_categories))
       .select(
           F.col("`No.`").alias("ItemNo"),
           F.col("`Base Unit of Measure`").alias("UOM"),
           F.lit(None).cast(StringType()).alias("LocationCode"),
           F.lit(0).cast(DecimalType(18, 6)).alias("Remaining_Quantity_Sum"),
           F.lit(0).cast(DecimalType(18, 6)).alias("UOM2"),
       )
)

# Optional: de-duplicate by business key to avoid merge conflicts
df_src = df_src.dropDuplicates(["ItemNo"])

# ----------------------------
# Create target table if missing
# ----------------------------
if not spark.catalog.tableExists(TARGET_TABLE):
    (df_src.limit(0)
          .write
          .format("delta")
          .mode("overwrite")
          .saveAsTable(TARGET_TABLE))

# ----------------------------
# Merge (upsert)
# ----------------------------
tgt = DeltaTable.forName(spark, TARGET_TABLE)

(tgt.alias("t")
    .merge(
        df_src.alias("s"),
        "t.ItemNo = s.ItemNo"
    )
    .whenMatchedUpdate(set={
        "ItemNo": "s.ItemNo",
        "UOM": "s.UOM",
        "LocationCode": "s.LocationCode",
        "Remaining_Quantity_Sum": "s.Remaining_Quantity_Sum",
        "UOM2": "s.UOM2"
    })
    .whenNotMatchedInsert(values={
        "ItemNo": "s.ItemNo",
        "UOM": "s.UOM",
        "LocationCode": "s.LocationCode",
        "Remaining_Quantity_Sum": "s.Remaining_Quantity_Sum",
        "UOM2": "s.UOM2"
    })
    .execute()
)

print(f"MERGE completed into {TARGET_TABLE}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Inventory Location

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable

# ----------------------------
# Config
# ----------------------------
SRC_BALANCE_TBL = "Gold_Inventory_Lakehouse.inv.gold_inventory_balance"
SRC_ITEM_LOC_TBL = "Gold_Inventory_Lakehouse.inv.gold_item_location_master"
TARGET_TBL = "Gold_Inventory_Lakehouse.inv.gold_inventory_location"

# ----------------------------
# Read sources
# ----------------------------
df_balance = (
    spark.table(SRC_BALANCE_TBL)
         .select(
             F.col("Item_No"),
             F.col("UOM"),
             F.col("Location_Code"),
             F.col("Remaining_Quantity_Sum"),
             F.col("UOM2_QTY").alias("UOM2")
         )
)

df_item_loc = (
    spark.table(SRC_ITEM_LOC_TBL)
         .select(
             F.col("ItemNo").alias("Item_No"),
             F.col("UOM"),
             F.col("LocationCode").alias("Location_Code"),
             F.col("Remaining_Quantity_Sum"),
             F.col("UOM2")
         )
)

# Union (like UNION ALL)
df_union = df_balance.unionByName(df_item_loc)

# Apply BROK-MAT rule + aggregate
df_src = (
    df_union.withColumn(
        "RemQty_part",
        F.when(F.col("Location_Code") == F.lit("BROK-MAT"), F.lit(0))
         .otherwise(F.col("Remaining_Quantity_Sum"))
         .cast(DecimalType(18, 6))
    )
    .withColumn(
        "RemQty2_part",
        F.when(F.col("Location_Code") == F.lit("BROK-MAT"), F.lit(0))
         .otherwise(F.col("UOM2"))
         .cast(DecimalType(18, 6))
    )
    .groupBy("Item_No", "UOM", "Location_Code")
    .agg(
        F.sum("RemQty_part").alias("RemQty"),
        F.sum("RemQty2_part").alias("RemQty2")
    )
)

# Optional: if you want TOP(100) behavior (deterministic ordering not guaranteed without orderBy)
# df_src = df_src.limit(100)

# ----------------------------
# Create target table if missing
# ----------------------------
if not spark.catalog.tableExists(TARGET_TBL):
    (df_src.limit(0)
          .write
          .format("delta")
          .mode("overwrite")
          .saveAsTable(TARGET_TBL))

# ----------------------------
# MERGE (upsert) into target
# Key = Item_No + UOM + Location_Code (matches GROUP BY)
# ----------------------------
tgt = DeltaTable.forName(spark, TARGET_TBL)

(tgt.alias("t")
    .merge(
        df_src.alias("s"),
        "t.Item_No = s.Item_No AND t.UOM = s.UOM AND t.Location_Code = s.Location_Code"
    )
    .whenMatchedUpdate(set={
        "RemQty": "s.RemQty",
        "RemQty2": "s.RemQty2"
    })
    .whenNotMatchedInsert(values={
        "Item_No": "s.Item_No",
        "UOM": "s.UOM",
        "Location_Code": "s.Location_Code",
        "RemQty": "s.RemQty",
        "RemQty2": "s.RemQty2"
    })
    .execute()
)

print(f"MERGE completed into {TARGET_TBL}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Sum Inventory

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable

# ----------------------------
# Config
# ----------------------------
SOURCE_TABLE = "Gold_Inventory_Lakehouse.inv.gold_inventory_location"   # vInventory_Location equivalent
TARGET_TABLE = "Gold_Inventory_Lakehouse.inv.gold_sum_inventory"

# ----------------------------
# Read source
# ----------------------------
df_src_raw = spark.table(SOURCE_TABLE)

# ----------------------------
# Apply window aggregation
# ----------------------------
from pyspark.sql.window import Window

w = Window.partitionBy("Item_No", "UOM")

df_src = (
    df_src_raw
        .withColumn(
            "RemQty_1",
            F.sum(F.col("RemQty").cast(DecimalType(18, 6))).over(w)
        )
        .withColumn(
            "RemQty_2",
            F.sum(F.col("RemQty2").cast(DecimalType(18, 6))).over(w)
        )
        .select(
            "Item_No",
            "UOM",
            "Location_Code",
            "RemQty_1",
            "RemQty_2"
        )
)

# Optional: remove duplicates caused by window function
df_src = df_src.dropDuplicates(["Item_No", "UOM", "Location_Code"])

# ----------------------------
# Create target table if missing
# ----------------------------
if not spark.catalog.tableExists(TARGET_TABLE):
    (df_src.limit(0)
          .write
          .format("delta")
          .mode("overwrite")
          .saveAsTable(TARGET_TABLE))

# ----------------------------
# MERGE (upsert)
# Key = Item_No + UOM + Location_Code
# ----------------------------
tgt = DeltaTable.forName(spark, TARGET_TABLE)

(tgt.alias("t")
    .merge(
        df_src.alias("s"),
        """
        t.Item_No = s.Item_No
        AND t.UOM = s.UOM
        AND t.Location_Code = s.Location_Code
        """
    )
    .whenMatchedUpdate(set={
        "RemQty_1": "s.RemQty_1",
        "RemQty_2": "s.RemQty_2"
    })
    .whenNotMatchedInsert(values={
        "Item_No": "s.Item_No",
        "UOM": "s.UOM",
        "Location_Code": "s.Location_Code",
        "RemQty_1": "s.RemQty_1",
        "RemQty_2": "s.RemQty_2"
    })
    .execute()
)

print(f"MERGE completed into {TARGET_TABLE}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold MS

# CELL ********************

# MAGIC %%sql
# MAGIC -- Full replace (overwrite) table every run
# MAGIC CREATE OR REPLACE TABLE `Gold_Inventory_Lakehouse`.`inv`.`gold_ms`
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     ms.`Document_Date`           AS Documentdate,
# MAGIC     ms.`SalesOrderNo`            AS SalesOrder,
# MAGIC     ms.`SO_STATUS`               AS Statusorder,
# MAGIC     ms.`Sell_to_Customer_Name`   AS CustomerName,
# MAGIC     ms.`FG_Item`                 AS ItemFG,
# MAGIC     ms.`FG_QTY`                  AS FGQty,
# MAGIC     ms.`Prod__OrderNo`           AS Prodno,
# MAGIC     ms.`Prod__Order_LineNo`      AS ProdLine,
# MAGIC     ms.`Component_Item`          AS ComponentItem,
# MAGIC     ms.`Description`             AS Description,
# MAGIC     st.`UOM`                     AS Uom1,
# MAGIC 
# MAGIC     -- Shipment week
# MAGIC     concat(
# MAGIC         year(ms.`EnnovieDueDate`),
# MAGIC         '-',
# MAGIC         lpad(cast(weekofyear(ms.`EnnovieDueDate`) as string), 2, '0')
# MAGIC     ) AS ShipmentWeek,
# MAGIC 
# MAGIC     ms.`EnnovieDueDate`          AS Shipmentdate,
# MAGIC     ms.`Expected_Quantity`       AS Expected1,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN st.`UOM` IN ('CM', 'CMS', 'PCS')
# MAGIC              AND ms.`Remaining_Quantity` = 0
# MAGIC         THEN NULL
# MAGIC         ELSE ms.`Remaining_Quantity`
# MAGIC     END AS Remaining1,
# MAGIC 
# MAGIC     ms.`Remaining_Quantity`      AS Requirement1,
# MAGIC     st.`RemQty_1`                AS Inventory1,
# MAGIC 
# MAGIC     ms.`Component_UOM2Per`       AS ComponentQTY,
# MAGIC     ms.`Component_UOM2Expected`  AS TotalPieces,
# MAGIC 
# MAGIC     ms.`Component_UOM2Expected` + ms.`Component_ActUOM2` AS Requirement2,
# MAGIC 
# MAGIC     st.`RemQty_2`                AS Inventory2,
# MAGIC     concat(ms.`Prod__OrderNo`, ms.`Prod__Order_LineNo`)  AS pol,
# MAGIC 
# MAGIC     -- Purchase date
# MAGIC     date_add(ms.`EnnovieDueDate`, -16) AS Purchasedate,
# MAGIC 
# MAGIC     -- Purchase week
# MAGIC     concat(
# MAGIC         year(date_add(ms.`EnnovieDueDate`, -16)),
# MAGIC         '-',
# MAGIC         lpad(cast(weekofyear(date_add(ms.`EnnovieDueDate`, -16)) as string), 2, '0')
# MAGIC     ) AS PurchaseWeek,
# MAGIC 
# MAGIC     current_timestamp() AS _load_ts
# MAGIC FROM `Gold_Inventory_Lakehouse`.`inv`.`gold_component_status` ms
# MAGIC LEFT JOIN `Gold_Inventory_Lakehouse`.`inv`.`gold_sum_inventory` st
# MAGIC     ON st.`Item_No` = ms.`Component_Item`
# MAGIC WHERE ms.`SO_STATUS` IN ('Open','Released','Pending Approval','Pending Prepayment')
# MAGIC   AND st.`Location_Code` IN (
# MAGIC         'BAGGING','BONDED','CZ-SYNT','DEBEERS','DIAMOND','PEARLS','WIP WHS','POMELATO',
# MAGIC         'FINDINGS','OTHERS-MAT','GEMS','SORTING','KIMAI','JEWELRY','BROK-MAT',' '
# MAGIC   );
# MAGIC 
# MAGIC -- Optional: performance maintenance
# MAGIC OPTIMIZE `Gold_Inventory_Lakehouse`.`inv`.`gold_ms`
# MAGIC ZORDER BY (SalesOrder, Prodno, ComponentItem);


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold MS Report

# CELL ********************

# MAGIC %%sql
# MAGIC -- ============================================================
# MAGIC -- Spark SQL (Fabric Lakehouse) FULL REPLACE table for vMsReport
# MAGIC -- Source: Gold_Inventory_Lakehouse.inv.gold_ms
# MAGIC -- Logic: keep 1 row per (SalesOrder, Prodno, ProdLine, ComponentItem)
# MAGIC -- ============================================================
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE `Gold_Inventory_Lakehouse`.`inv`.`gold_MsReport`
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   Documentdate,
# MAGIC   Prodno,
# MAGIC   PurchaseWeek,
# MAGIC   Purchasedate,
# MAGIC   ShipmentWeek,
# MAGIC   Shipmentdate,
# MAGIC   SalesOrder,
# MAGIC   CustomerName,
# MAGIC   ItemFG,
# MAGIC   ComponentItem,
# MAGIC   Description,
# MAGIC   Uom1,
# MAGIC   Expected1,
# MAGIC   Remaining1,
# MAGIC   Requirement1,
# MAGIC   Inventory1,
# MAGIC   ComponentQTY,
# MAGIC   TotalPieces,
# MAGIC   Requirement2,
# MAGIC   Inventory2,
# MAGIC   FGQty,
# MAGIC   pol,
# MAGIC   ProdLine
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     Documentdate,
# MAGIC     Prodno,
# MAGIC     PurchaseWeek,
# MAGIC     Purchasedate,
# MAGIC     ShipmentWeek,
# MAGIC     Shipmentdate,
# MAGIC     SalesOrder,
# MAGIC     CustomerName,
# MAGIC     ItemFG,
# MAGIC     ComponentItem,
# MAGIC     Description,
# MAGIC     Uom1,
# MAGIC     Expected1,
# MAGIC     Remaining1,
# MAGIC     Requirement1,
# MAGIC     Inventory1,
# MAGIC     ComponentQTY,
# MAGIC     TotalPieces,
# MAGIC     Requirement2,
# MAGIC     Inventory2,
# MAGIC     FGQty,
# MAGIC     pol,
# MAGIC     ProdLine,
# MAGIC     row_number() OVER (
# MAGIC       PARTITION BY concat(SalesOrder, '_', Prodno, '_', cast(ProdLine as string), '_', ComponentItem)
# MAGIC       ORDER BY pol
# MAGIC     ) AS RowNum
# MAGIC   FROM `Gold_Inventory_Lakehouse`.`inv`.`gold_ms`
# MAGIC ) RankedData
# MAGIC WHERE RowNum = 1;
# MAGIC 
# MAGIC -- Optional: performance maintenance
# MAGIC OPTIMIZE `Gold_Inventory_Lakehouse`.`inv`.`gold_MsReport`
# MAGIC ZORDER BY (SalesOrder, Prodno, ComponentItem);


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("""
    WITH item_dim AS (
        SELECT
            d.`No.` AS item_no,
            MAX(CASE WHEN d.`Dimension Code` = 'CUSTOMER NAME' 
                     THEN d.`Dimension Value Code` END) AS customer_name,
            MAX(CASE WHEN d.`Dimension Code` = 'PRODUCT GROUP' 
                     THEN d.`Dimension Value Code` END) AS product_group
        FROM `Silver_BC_Lakehouse`.bc.`Default Dimension` AS d
        WHERE d.`Table ID` = 27
          AND d.`Dimension Code` IN ('CUSTOMER NAME', 'PRODUCT GROUP')
        GROUP BY d.`No.`
    ),

    -- ============================================================
    -- A) MASTER ROUTING — sequential anchor matching
    -- ============================================================
    master_ops AS (
        SELECT
            rl.`Routing No.`,
            rl.`Version Code`,
            TRY_CAST(rl.`Operation No.` AS DECIMAL(10,2)) AS op_seq,
            rl.`No.`         AS center_no
        FROM `Silver_BC_Lakehouse`.bc.`Routing Line` rl
        WHERE TRY_CAST(rl.`Operation No.` AS DECIMAL(10,2)) IS NOT NULL
    ),

    -- A.2) หา op_seq ของแต่ละ anchor — เอาตัวแรกของแต่ละ anchor (MIN op_seq)
    master_anchors AS (
        SELECT
            `Routing No.`,
            `Version Code`,
            MIN(CASE WHEN UPPER(TRIM(center_no)) = 'RELEASED'            THEN op_seq END) AS released_seq,
            MIN(CASE WHEN UPPER(TRIM(center_no)) = 'WH2F'                THEN op_seq END) AS wh2f_seq,
            MIN(CASE WHEN UPPER(TRIM(center_no)) = 'PROD ADMIN'          THEN op_seq END) AS prod_admin_seq,
            MIN(CASE WHEN UPPER(TRIM(center_no)) = 'TUMBLING ROOM'       THEN op_seq END) AS tumbling_seq,
            MIN(CASE WHEN UPPER(TRIM(center_no)) = 'STEEL BALL TRUMBING' THEN op_seq END) AS steel_ball_seq,
            MIN(CASE WHEN UPPER(TRIM(center_no)) = 'QC AFTER TUMBLING'   THEN op_seq END) AS qc_seq
        FROM master_ops
        GROUP BY `Routing No.`, `Version Code`
    ),

    -- A.3) ยืนยันว่า:
    --   1) ทุก anchor มีอยู่ครบ (NOT NULL)
    --   2) เรียงตามลำดับ released < wh2f < prod_admin < tumbling < steel_ball < qc
    --   3) ระหว่าง steel_ball กับ qc ต้องมี CELL* (work center) คั่นอย่างน้อย 1 ตัว
    master_routings AS (
        SELECT DISTINCT
            ma.`Routing No.`,
            ma.`Version Code`
        FROM master_anchors ma
        WHERE ma.released_seq    IS NOT NULL
          AND ma.wh2f_seq        IS NOT NULL
          AND ma.prod_admin_seq  IS NOT NULL
          AND ma.tumbling_seq    IS NOT NULL
          AND ma.steel_ball_seq  IS NOT NULL
          AND ma.qc_seq          IS NOT NULL
          AND ma.released_seq   < ma.wh2f_seq
          AND ma.wh2f_seq       < ma.prod_admin_seq
          AND ma.prod_admin_seq < ma.tumbling_seq
          AND ma.tumbling_seq   < ma.steel_ball_seq
          AND ma.steel_ball_seq < ma.qc_seq
          -- ต้องมี CELL* work center อยู่ระหว่าง steel_ball กับ qc
          AND EXISTS (
              SELECT 1
              FROM master_ops cell
              WHERE cell.`Routing No.`  = ma.`Routing No.`
                AND cell.`Version Code` = ma.`Version Code`
                AND cell.op_seq > ma.steel_ball_seq
                AND cell.op_seq < ma.qc_seq
                AND UPPER(TRIM(cell.center_no)) LIKE 'CELL%'
          )
    ),

    master_fg AS (
        SELECT DISTINCT
            i.`No.`         AS fg_item_no,
            i.`Description` AS fg_description,
            i.`Routing No.` AS routing_no,
            'MASTER'        AS source
        FROM master_routings mr
        INNER JOIN `Silver_BC_Lakehouse`.bc.Item i
            ON i.`Routing No.` = mr.`Routing No.`
        WHERE i.Blocked = '0'
    ),

    -- ============================================================
    -- B) PROD ORDER ROUTING — active PROs only, same logic
    -- ============================================================
    pro_ops AS (
        SELECT
            prl.`Prod. Order No.` AS prod_order_no,
            TRY_CAST(prl.`Operation No.` AS DECIMAL(10,2)) AS op_seq,
            prl.`No.`             AS center_no,
            po.`Source No.`       AS po_item_no
        FROM `Silver_BC_Lakehouse`.bc.`Prod Order Routing Line` prl
        INNER JOIN `Silver_BC_Lakehouse`.bc.`Production Order` po
            ON po.`No.`   = prl.`Prod. Order No.`
           AND po.Status = prl.Status
        WHERE po.Status IN ('Released','Firm Planned','Planned')
          AND TRY_CAST(prl.`Operation No.` AS DECIMAL(10,2)) IS NOT NULL
    ),

    pro_anchors AS (
        SELECT
            prod_order_no,
            MAX(po_item_no) AS po_item_no,
            MIN(CASE WHEN UPPER(TRIM(center_no)) = 'RELEASED'            THEN op_seq END) AS released_seq,
            MIN(CASE WHEN UPPER(TRIM(center_no)) = 'WH2F'                THEN op_seq END) AS wh2f_seq,
            MIN(CASE WHEN UPPER(TRIM(center_no)) = 'PROD ADMIN'          THEN op_seq END) AS prod_admin_seq,
            MIN(CASE WHEN UPPER(TRIM(center_no)) = 'TUMBLING ROOM'       THEN op_seq END) AS tumbling_seq,
            MIN(CASE WHEN UPPER(TRIM(center_no)) = 'STEEL BALL TRUMBING' THEN op_seq END) AS steel_ball_seq,
            MIN(CASE WHEN UPPER(TRIM(center_no)) = 'QC AFTER TUMBLING'   THEN op_seq END) AS qc_seq
        FROM pro_ops
        GROUP BY prod_order_no
    ),

    pro_fg AS (
        SELECT DISTINCT
            pa.po_item_no AS fg_item_no
        FROM pro_anchors pa
        WHERE pa.released_seq    IS NOT NULL
          AND pa.wh2f_seq        IS NOT NULL
          AND pa.prod_admin_seq  IS NOT NULL
          AND pa.tumbling_seq    IS NOT NULL
          AND pa.steel_ball_seq  IS NOT NULL
          AND pa.qc_seq          IS NOT NULL
          AND pa.released_seq   < pa.wh2f_seq
          AND pa.wh2f_seq       < pa.prod_admin_seq
          AND pa.prod_admin_seq < pa.tumbling_seq
          AND pa.tumbling_seq   < pa.steel_ball_seq
          AND pa.steel_ball_seq < pa.qc_seq
          AND EXISTS (
              SELECT 1
              FROM pro_ops cell
              WHERE cell.prod_order_no = pa.prod_order_no
                AND cell.op_seq > pa.steel_ball_seq
                AND cell.op_seq < pa.qc_seq
                AND UPPER(TRIM(cell.center_no)) LIKE 'CELL%'
          )
    ),

    pro_fg_full AS (
        SELECT
            p.fg_item_no,
            i.`Description` AS fg_description,
            i.`Routing No.` AS routing_no,
            'PRO_ROUTING'   AS source
        FROM pro_fg p
        LEFT JOIN `Silver_BC_Lakehouse`.bc.Item i
            ON i.`No.` = p.fg_item_no
    ),

    -- ============================================================
    -- C) UNION + dedupe ให้ 1 row ต่อ FG
    -- ============================================================
    combined AS (
        SELECT * FROM master_fg
        UNION ALL
        SELECT * FROM pro_fg_full
    )

    SELECT
        c.fg_item_no,
        MAX(c.fg_description) AS fg_description,
        MAX(c.routing_no)     AS routing_no,
        dim.customer_name,
        dim.product_group,
        CASE WHEN COUNT(DISTINCT c.source) = 2 THEN 'BOTH'
             WHEN MAX(c.source) = 'MASTER'      THEN 'MASTER_ONLY'
             ELSE 'PRO_ONLY'
        END AS coverage
    FROM combined c
    LEFT JOIN item_dim dim
        ON dim.item_no = c.fg_item_no
    GROUP BY c.fg_item_no, dim.customer_name, dim.product_group
    ORDER BY dim.customer_name, c.fg_item_no
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

display(spark.sql("""
    WITH item_dim AS (
        SELECT
            d.`No.` AS item_no,
            MAX(CASE WHEN d.`Dimension Code` = 'CUSTOMER NAME' 
                     THEN d.`Dimension Value Code` END) AS customer_name,
            MAX(CASE WHEN d.`Dimension Code` = 'PRODUCT GROUP' 
                     THEN d.`Dimension Value Code` END) AS product_group
        FROM `Silver_BC_Lakehouse`.bc.`Default Dimension` AS d
        WHERE d.`Table ID` = 27
          AND d.`Dimension Code` IN ('CUSTOMER NAME', 'PRODUCT GROUP')
        GROUP BY d.`No.`
    ),

    pro_ops AS (
        SELECT
            prl.`Prod. Order No.` AS prod_order_no,
            prl.Status            AS pro_status,
            TRY_CAST(prl.`Operation No.` AS DECIMAL(10,2)) AS op_seq,
            prl.`No.`             AS center_no
        FROM `Silver_BC_Lakehouse`.bc.`Prod Order Routing Line` prl
        WHERE prl.Status IN ('Released','Firm Planned')           -- เปลี่ยนตรงนี้
          AND TRY_CAST(prl.`Operation No.` AS DECIMAL(10,2)) IS NOT NULL
    ),

    pro_anchors AS (
        SELECT
            prod_order_no,
            pro_status,
            MIN(CASE WHEN UPPER(TRIM(center_no)) = 'RELEASED'            THEN op_seq END) AS released_seq,
            MIN(CASE WHEN UPPER(TRIM(center_no)) = 'WH2F'                THEN op_seq END) AS wh2f_seq,
            MIN(CASE WHEN UPPER(TRIM(center_no)) = 'PROD ADMIN'          THEN op_seq END) AS prod_admin_seq,
            MIN(CASE WHEN UPPER(TRIM(center_no)) = 'TUMBLING ROOM'       THEN op_seq END) AS tumbling_seq,
            MIN(CASE WHEN UPPER(TRIM(center_no)) = 'STEEL BALL TRUMBING' THEN op_seq END) AS steel_ball_seq,
            MIN(CASE WHEN UPPER(TRIM(center_no)) = 'QC AFTER TUMBLING'   THEN op_seq END) AS qc_seq
        FROM pro_ops
        GROUP BY prod_order_no, pro_status
    ),

    matching_pros AS (
        SELECT
            pa.prod_order_no,
            pa.pro_status
        FROM pro_anchors pa
        WHERE pa.released_seq    IS NOT NULL
          AND pa.wh2f_seq        IS NOT NULL
          AND pa.prod_admin_seq  IS NOT NULL
          AND pa.tumbling_seq    IS NOT NULL
          AND pa.steel_ball_seq  IS NOT NULL
          AND pa.qc_seq          IS NOT NULL
          AND pa.released_seq   < pa.wh2f_seq
          AND pa.wh2f_seq       < pa.prod_admin_seq
          AND pa.prod_admin_seq < pa.tumbling_seq
          AND pa.tumbling_seq   < pa.steel_ball_seq
          AND pa.steel_ball_seq < pa.qc_seq
          AND EXISTS (
              SELECT 1
              FROM pro_ops cell
              WHERE cell.prod_order_no = pa.prod_order_no
                AND cell.pro_status    = pa.pro_status
                AND cell.op_seq > pa.steel_ball_seq
                AND cell.op_seq < pa.qc_seq
                AND UPPER(TRIM(cell.center_no)) LIKE 'CELL%'
          )
    ),

    pro_line_qty AS (
        SELECT
            pol.`Prod. Order No.` AS prod_order_no,
            pol.Status            AS pro_status,
            SUM(pol.`Quantity`)            AS line_qty,
            SUM(pol.`Finished Quantity`)   AS finished_qty
        FROM `Silver_BC_Lakehouse`.bc.`Prod Order Line` pol
        WHERE pol.Status IN ('Released','Firm Planned')           -- เปลี่ยนตรงนี้
        GROUP BY pol.`Prod. Order No.`, pol.Status
    )

    SELECT
        mp.prod_order_no,
        mp.pro_status,
        po.`Source No.`        AS fg_item_no,
        po.`Description`       AS fg_description,
        po.`Routing No.`       AS routing_no,
        po.`Quantity`          AS header_qty,
        pq.finished_qty,
        po.`Starting Date`     AS starting_date,
        po.`Ending Date`       AS ending_date,
        po.`Due Date`          AS due_date,
        po.`Sales Order No.`   AS sales_order_no,
        dim.customer_name,
        dim.product_group
    FROM matching_pros mp
    INNER JOIN `Silver_BC_Lakehouse`.bc.`Production Order` po
        ON po.`No.`   = mp.prod_order_no
       AND po.Status = mp.pro_status
    LEFT JOIN pro_line_qty pq
        ON pq.prod_order_no = mp.prod_order_no
       AND pq.pro_status    = mp.pro_status
    LEFT JOIN item_dim dim
        ON dim.item_no = po.`Source No.`
    ORDER BY mp.pro_status, dim.customer_name, mp.prod_order_no
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
