# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "edcc8d2f-2684-446b-939e-eba9a81a7917",
# META       "default_lakehouse_name": "Gold_Customer_Exp_Lakehouse",
# META       "default_lakehouse_workspace_id": "d74457b3-045c-445d-82c6-9a2e4b9f1436",
# META       "known_lakehouses": [
# META         {
# META           "id": "edcc8d2f-2684-446b-939e-eba9a81a7917"
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

# Just cause sales data have dates older than 1900s


from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Sales Order Shipment Header

# CELL ********************

# Spark (Delta) FULL REPLACE load -> gold_sales_order_shipment_header
# Source: [Silver_BC_Lakehouse].[bc].[Sales Header]
# Primary key = (BC Company, Document Type, No.)
# Target: Gold_Customer_Exp_Lakehouse.cx.gold_sales_order_shipment_header

from pyspark.sql import functions as F

TARGET = "Gold_Customer_Exp_Lakehouse.cx.gold_sales_order_shipment_header"
SRC = "Silver_BC_Lakehouse.bc.`Sales Header`"  # backticks because of space

# 1) Create target table if it doesn't exist (Delta)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    CusNo STRING,
    CusName STRING,
    SalesorderNo STRING,
    StatusSO STRING,
    DocDate DATE,
    PostDate DATE,
    OrderDate DATE,
    DueDate DATE,
    ReqDate DATE,
    PmDate DATE,
    ShipDate DATE,
    DocumentType STRING,
    CSNoted STRING,
    CSteam STRING,
    Currency STRING,
    ShiptoName STRING,
    BilltoName STRING,
    LOCATION STRING,
    ExternalDocumentNo STRING,
    ReleasedDate DATE,
    ShipmentWeek STRING,
    ShipType STRING,

    -- keys / tracking
    BC_Company STRING,
    BC_DocumentType STRING,
    BC_No STRING,
    SystemCreatedAt TIMESTAMP,
    SystemModifiedAt TIMESTAMP
)
USING DELTA
""")

# 2) Read source (FULL dataset)
src = spark.table(SRC)

# 3) Filter rows (same status filter as before)
src_f = (
    src.filter(
        F.col("Status").isin(
            "Open",
            "Released",
            "Pending Approval",
            "Pending Prepayment",
            "Closed"
        )
    )
)

# 4) Transform to gold schema
no_col = F.col("`No.`")

# DocumentType: prefix before first digit (e.g., "SO" from "SO12345").
# If starts with digit or no non-digit prefix, fall back to full No.
doc_prefix = F.regexp_extract(no_col, r"^[^0-9]+", 0)

gold_full = (
    src_f.select(
        F.col("`Sell-to Customer No.`").alias("CusNo"),
        F.col("`Sell-to Customer Name`").alias("CusName"),
        no_col.alias("SalesorderNo"),
        F.col("`Status`").alias("StatusSO"),
        F.col("`Document Date`").cast("date").alias("DocDate"),
        F.col("`Posting Date`").cast("date").alias("PostDate"),
        F.col("`Order Date`").cast("date").alias("OrderDate"),
        F.col("`Due Date`").cast("date").alias("DueDate"),
        F.col("`Requested Delivery Date`").cast("date").alias("ReqDate"),
        F.col("`Promised Delivery Date`").cast("date").alias("PmDate"),
        F.col("`Shipment Date`").cast("date").alias("ShipDate"),

        F.when(doc_prefix != "", doc_prefix).otherwise(no_col).alias("DocumentType"),

        F.col("`Your Reference`").alias("CSNoted"),
        F.col("`Salesperson Code`").alias("CSteam"),
        F.col("`Currency Code`").alias("Currency"),
        F.col("`Ship-to Name`").alias("ShiptoName"),
        F.col("`Bill-to Name`").alias("BilltoName"),
        F.col("`Location Code`").alias("LOCATION"),
        F.col("`External Document No.`").alias("ExternalDocumentNo"),
        F.col("`Released Date`").cast("date").alias("ReleasedDate"),
        F.col("`Shipping Advice`").alias("ShipType"),

        # Shipment week (Spark-safe)
        F.lpad(
            F.weekofyear(F.col("`Requested Delivery Date`")).cast("string"),
            2,
            "0"
        ).alias("ShipmentWeek"),

        # keys / tracking
        F.col("`BC Company`").alias("BC_Company"),
        F.col("`Document Type`").alias("BC_DocumentType"),
        no_col.alias("BC_No"),
        F.col("`SystemCreatedAt`").alias("SystemCreatedAt"),
        F.col("`SystemModifiedAt`").alias("SystemModifiedAt"),
    )
)

# 5) FULL REPLACE the target
# Option A (recommended): overwrite via saveAsTable (keeps it Delta + replaces data)
(
    gold_full
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET)
)

# --- If you prefer SQL TRUNCATE + INSERT instead, use Option B and comment out Option A ---
# gold_full.createOrReplaceTempView("gold_sales_order_shipment_full")
# spark.sql(f"TRUNCATE TABLE {TARGET}")
# spark.sql(f"INSERT INTO {TARGET} SELECT * FROM gold_sales_order_shipment_full")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Sales Order Shipment Line

# CELL ********************

from pyspark.sql import functions as F

TARGET = "Gold_Customer_Exp_Lakehouse.cx.gold_sales_order_shipment_line"
SL_SRC = "Silver_BC_Lakehouse.bc.`Sales Line`"
SH_SRC = "Silver_BC_Lakehouse.bc.`Sales Header`"

# 1) Create target table if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    SalesorderNo STRING,
    LineNo INT,
    ItemFG STRING,
    Description STRING,
    Gen_Prod_Posting_Group STRING,
    item_location STRING,
    Itemgroup STRING,
    QTY DECIMAL(38,10),
    UOM STRING,
    Unit_Cost_LCY DECIMAL(38,10),
    UnitPrice DECIMAL(38,10),
    Qty_to_Ship DECIMAL(38,10),
    QtyShip DECIMAL(38,10),
    Qty_to_Invoice DECIMAL(38,10),
    QtyINV DECIMAL(38,10),
    Order_Date DATE,
    Requested_Delivery_Date DATE,
    Promised_Delivery_Date DATE,
    Planned_Delivery_Date DATE,
    Planned_Shipment_Date DATE,
    Shipment_Date DATE,
    TypeofFG STRING,
    Shortcut_Dimension2_Code STRING,
    Currency STRING,
    ExternalDocumentNo STRING,
    ReleasedDate DATE,

    -- keys / tracking
    DocumentNo STRING,
    DocumentType STRING,
    LineNo_Key INT,
    SL_SystemCreatedAt TIMESTAMP,
    SL_SystemModifiedAt TIMESTAMP,
    SH_SystemCreatedAt TIMESTAMP,
    SH_SystemModifiedAt TIMESTAMP
)
USING DELTA
""")

# 2) Read sources (FULL dataset)
sl = spark.table(SL_SRC)
sh = spark.table(SH_SRC)

# --- DocumentType = Order filter (safe for int or string) ---
is_order_sl = (F.col("`Document Type`").cast("string") == F.lit("Order"))
is_order_sh = (F.col("`Document Type`").cast("string") == F.lit("Order"))

sl_f = sl.filter(is_order_sl)
sh_f = sh.filter(is_order_sh)

# 3) Join (exactly like your SQL intent)
joined = (
    sl_f.alias("SalesLineShip")
      .join(
          sh_f.alias("SO"),
          on=(F.col("SalesLineShip.`Document No.`") == F.col("SO.`No.`")),
          how="inner"
      )
)

# 4) Itemgroup logic (same as your SQL)
itemgroup = (
    F.when(F.substring(F.col("SalesLineShip.`No.`"), 2, 1) == F.lit("-"),
           F.substring(F.col("SalesLineShip.`No.`"), 1, 8))
     .when(F.substring(F.col("SalesLineShip.`No.`"), 11, 1) == F.lit("-"),
           F.substring(F.col("SalesLineShip.`No.`"), 1, 10))
     .otherwise(F.col("SalesLineShip.`No.`"))
)

gold_full = joined.select(
    F.col("SalesLineShip.`Document No.`").alias("SalesorderNo"),
    F.col("SalesLineShip.`Line No.`").cast("int").alias("LineNo"),
    F.col("SalesLineShip.`No.`").alias("ItemFG"),
    F.col("SalesLineShip.`Description`").alias("Description"),
    F.col("SalesLineShip.`Gen. Prod. Posting Group`").alias("Gen_Prod_Posting_Group"),
    F.col("SalesLineShip.`Location Code`").alias("item_location"),
    itemgroup.alias("Itemgroup"),

    F.col("SalesLineShip.`Quantity`").cast("decimal(38,10)").alias("QTY"),
    F.col("SalesLineShip.`Unit of Measure`").alias("UOM"),
    F.col("SalesLineShip.`Unit Cost (LCY)`").cast("decimal(38,10)").alias("Unit_Cost_LCY"),
    F.col("SalesLineShip.`Unit Price`").cast("decimal(38,10)").alias("UnitPrice"),
    F.col("SalesLineShip.`Qty. to Ship`").cast("decimal(38,10)").alias("Qty_to_Ship"),
    F.col("SalesLineShip.`Quantity Shipped`").cast("decimal(38,10)").alias("QtyShip"),
    F.col("SalesLineShip.`Qty. to Invoice`").cast("decimal(38,10)").alias("Qty_to_Invoice"),
    F.col("SalesLineShip.`Quantity Invoiced`").cast("decimal(38,10)").alias("QtyINV"),

    F.col("SO.`Order Date`").cast("date").alias("Order_Date"),
    F.col("SalesLineShip.`Requested Delivery Date`").cast("date").alias("Requested_Delivery_Date"),
    F.col("SalesLineShip.`Promised Delivery Date`").cast("date").alias("Promised_Delivery_Date"),
    F.col("SalesLineShip.`Planned Delivery Date`").cast("date").alias("Planned_Delivery_Date"),
    F.col("SalesLineShip.`Planned Shipment Date`").cast("date").alias("Planned_Shipment_Date"),
    F.col("SO.`Shipment Date`").cast("date").alias("Shipment_Date"),

    F.col("SalesLineShip.`Type`").alias("TypeofFG"),
    F.col("SalesLineShip.`Shortcut Dimension 2 Code`").alias("Shortcut_Dimension2_Code"),

    F.col("SO.`Currency Code`").alias("Currency"),
    F.col("SO.`External Document No.`").alias("ExternalDocumentNo"),
    F.col("SO.`Released Date`").cast("date").alias("ReleasedDate"),

    # keys / tracking (kept for downstream consistency)
    F.col("SalesLineShip.`Document No.`").alias("DocumentNo"),
    F.col("SalesLineShip.`Document Type`").cast("string").alias("DocumentType"),
    F.col("SalesLineShip.`Line No.`").cast("int").alias("LineNo_Key"),
    F.col("SalesLineShip.`SystemCreatedAt`").alias("SL_SystemCreatedAt"),
    F.col("SalesLineShip.`SystemModifiedAt`").alias("SL_SystemModifiedAt"),
    F.col("SO.`SystemCreatedAt`").alias("SH_SystemCreatedAt"),
    F.col("SO.`SystemModifiedAt`").alias("SH_SystemModifiedAt"),
)

# 5) FULL REPLACE target (overwrite data)
(
    gold_full
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET)
)

# --- Alternative FULL REPLACE option (SQL): TRUNCATE + INSERT ---
# gold_full.createOrReplaceTempView("gold_sales_order_shipment_line_full")
# spark.sql(f"TRUNCATE TABLE {TARGET}")
# spark.sql(f"INSERT INTO {TARGET} SELECT * FROM gold_sales_order_shipment_line_full")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Sales Order Shipment Fact

# CELL ********************

# Spark (Delta) FULL REPLACE load -> gold_sales_order_shipment_fact
# Source (gold): Gold_Customer_Exp_Lakehouse.cx.gold_sales_order_shipment_header (so)
#               Gold_Customer_Exp_Lakehouse.cx.gold_sales_order_shipment_line (sol)
# Join: so.SalesorderNo = sol.SalesorderNo   (LEFT JOIN)

from pyspark.sql import functions as F

TARGET = "Gold_Customer_Exp_Lakehouse.cx.gold_sales_order_shipment_fact"
SO_SRC = "Gold_Customer_Exp_Lakehouse.cx.gold_sales_order_shipment_header"
SOL_SRC = "Gold_Customer_Exp_Lakehouse.cx.gold_sales_order_shipment_line"

# 1) Create target table if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    ShipmentWeek STRING,
    CusNo STRING,
    CusName STRING,
    ShiptoName STRING,
    BilltoName STRING,
    StatusSO STRING,
    DocDate DATE,
    PostDate DATE,
    OrderDate DATE,
    DueDate DATE,
    ReqDate DATE,
    PmDate DATE,
    ShipDate DATE,
    PlannedDeli DATE,
    PlannedShip DATE,
    ExternalDocumentNo STRING,
    ReleasedDate DATE,
    CSNoted STRING,
    CSteam STRING,
    DocumentType STRING,
    SalesorderNo STRING,
    ShipType STRING,

    linenoo INT,
    ItemFG STRING,
    Description STRING,
    Itemgroup STRING,
    GenprodGroup STRING,
    Location_Code STRING,
    UOM STRING,
    Totalqty DECIMAL(38,10),
    QtytoShip DECIMAL(38,10),
    QtyShip DECIMAL(38,10),
    QtytoInv DECIMAL(38,10),
    QtyINV DECIMAL(38,10),
    Outstanding DECIMAL(38,10),
    UnitCostLCY DECIMAL(38,10),
    UnitPrice DECIMAL(38,10),
    Currency STRING,
    TypeofFG STRING,
    Shortcut_Dimension2_Code STRING,

    -- keep tracking columns (even if not used for full replace)
    so_SystemModifiedAt TIMESTAMP,
    sol_SystemModifiedAt TIMESTAMP
)
USING DELTA
""")

# 2) Read sources (FULL dataset)
so = spark.table(SO_SRC).alias("so")
sol = spark.table(SOL_SRC).alias("sol")

# 3) Build FULL fact
fact_full = (
    so.join(sol, on="SalesorderNo", how="left")
      .select(
          F.col("so.ShipmentWeek").alias("ShipmentWeek"),
          F.col("so.CusNo").alias("CusNo"),
          F.col("so.CusName").alias("CusName"),
          F.col("so.ShiptoName").alias("ShiptoName"),
          F.col("so.BilltoName").alias("BilltoName"),
          F.col("so.StatusSO").alias("StatusSO"),
          F.col("so.DocDate").cast("date").alias("DocDate"),
          F.col("so.PostDate").cast("date").alias("PostDate"),
          F.col("so.OrderDate").cast("date").alias("OrderDate"),
          F.col("so.DueDate").cast("date").alias("DueDate"),
          F.col("so.ReqDate").cast("date").alias("ReqDate"),
          F.col("so.PmDate").cast("date").alias("PmDate"),
          F.col("so.ShipDate").cast("date").alias("ShipDate"),
          F.col("sol.Planned_Delivery_Date").cast("date").alias("PlannedDeli"),
          F.col("sol.Planned_Shipment_Date").cast("date").alias("PlannedShip"),
          F.col("so.ExternalDocumentNo").alias("ExternalDocumentNo"),
          F.col("so.ReleasedDate").cast("date").alias("ReleasedDate"),
          F.col("so.ShipType").alias("ShipType"),
          F.col("so.CSNoted").alias("CSNoted"),
          F.col("so.CSteam").alias("CSteam"),
          F.col("so.DocumentType").alias("DocumentType"),
          F.col("so.SalesorderNo").alias("SalesorderNo"),

          F.col("sol.LineNo").cast("int").alias("linenoo"),
          F.col("sol.ItemFG").alias("ItemFG"),
          F.col("sol.Description").alias("Description"),
          F.col("sol.Itemgroup").alias("Itemgroup"),
          F.col("sol.Gen_Prod_Posting_Group").alias("GenprodGroup"),
          F.col("sol.item_location").alias("Location_Code"),
          F.col("sol.UOM").alias("UOM"),
          F.col("sol.QTY").cast("decimal(38,10)").alias("Totalqty"),
          F.col("sol.Qty_to_Ship").cast("decimal(38,10)").alias("QtytoShip"),
          F.col("sol.QtyShip").cast("decimal(38,10)").alias("QtyShip"),
          F.col("sol.Qty_to_Invoice").cast("decimal(38,10)").alias("QtytoInv"),
          F.col("sol.QtyINV").cast("decimal(38,10)").alias("QtyINV"),

          (F.coalesce(F.col("sol.QTY"), F.lit(0).cast("decimal(38,10)")) -
           F.coalesce(F.col("sol.QtyShip"), F.lit(0).cast("decimal(38,10)"))).alias("Outstanding"),

          F.col("sol.Unit_Cost_LCY").cast("decimal(38,10)").alias("UnitCostLCY"),
          F.col("sol.UnitPrice").cast("decimal(38,10)").alias("UnitPrice"),
          F.col("so.Currency").alias("Currency"),
          F.col("sol.TypeofFG").alias("TypeofFG"),
          F.col("sol.Shortcut_Dimension2_Code").alias("Shortcut_Dimension2_Code"),

          # tracking columns (preserved)
          F.col("so.SystemModifiedAt").alias("so_SystemModifiedAt"),
          F.col("sol.SL_SystemModifiedAt").alias("sol_SystemModifiedAt"),
      )
)

# 4) FULL REPLACE target
(
    fact_full
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET)
)

# --- Alternative FULL REPLACE option (SQL): TRUNCATE + INSERT ---
# fact_full.createOrReplaceTempView("gold_sales_order_shipment_fact_full")
# spark.sql(f"TRUNCATE TABLE {TARGET}")
# spark.sql(f"INSERT INTO {TARGET} SELECT * FROM gold_sales_order_shipment_fact_full")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Sales Order Shipment Fact Currency

# CELL ********************

from pyspark.sql import functions as F

TARGET = "Gold_Customer_Exp_Lakehouse.cx.gold_sales_order_shipment_fact_currency"
SP_SRC = "Gold_Customer_Exp_Lakehouse.cx.gold_sales_order_shipment_fact"
CUR_SRC = "Silver_BC_Lakehouse.bc.`Currency Exchange Rate`"

# 1) Create target table if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    ShipmentWeek STRING,
    CusNo STRING,
    CusName STRING,
    ShiptoName STRING,
    BilltoName STRING,
    StatusSO STRING,
    PostDate DATE,
    DocDate DATE,
    OrderDate DATE,
    DueDate DATE,
    ReqDate DATE,
    PmDate DATE,
    ShipDate DATE,
    ShipType STRING,

    Itemgroup STRING,
    GenprodGroup STRING,
    Location_Code STRING,
    ExternalDocumentNo STRING,
    ReleasedDate DATE,
    CSNoted STRING,
    CSteam STRING,
    DocumentType STRING,
    SalesorderNo STRING,
    linenoo INT,
    ItemFG STRING,
    Description STRING,
    TypeofFG STRING,
    UOM STRING,
    Totalqty DECIMAL(38,10),
    QtytoShip DECIMAL(38,10),
    QtyShip DECIMAL(38,10),
    QtytoInv DECIMAL(38,10),
    QtyINV DECIMAL(38,10),
    UnitPrice DECIMAL(38,10),
    Currency STRING,
    Outstanding DECIMAL(38,10),
    UnitCostLCY DECIMAL(38,10),
    Shortcut_Dimension2_Code STRING,

    relationalExchRateAmount DECIMAL(38,10),
    Amount DECIMAL(38,10),
    AmountTHB DECIMAL(38,10),
    TotalTHB DECIMAL(38,10),
    TotalShipTHB DECIMAL(38,10),
    OutstandingAmountTHB DECIMAL(38,10),
    QtyShipNotInv DECIMAL(38,10),
    ShipNotInvoiceTHB DECIMAL(38,10),
    Outs DECIMAL(38,10),

    so_sol STRING,
    SOI STRING,

    -- tracking copied from sp (preserved)
    so_SystemModifiedAt TIMESTAMP,
    sol_SystemModifiedAt TIMESTAMP
)
USING DELTA
""")

# 2) Read sources (FULL dataset)
sp = spark.table(SP_SRC).alias("sp")
cur = spark.table(CUR_SRC).alias("cur")

# 3) Join to currency rate (left join)
# Note: kept your exact join keys: Currency Code + Starting Date = OrderDate
joined = (
    sp.join(
        cur,
        on=(
            (F.col("cur.`Currency Code`") == F.col("sp.Currency")) &
            (F.col("cur.`Starting Date`").cast("date") == F.col("sp.OrderDate").cast("date"))
            # If you want THB-only, uncomment:
            # & (F.col("cur.`Relational Currency Code`") == F.lit("THB"))
        ),
        how="left"
    )
)

# 4) Relational exchange rate amount logic:
# SQL: ISNULL(NULLIF(cur.[Relational Exch. Rate Amount], 0), 1)
rate = (
    F.when(
        (F.col("cur.`Relational Exch. Rate Amount`").isNull()) |
        (F.col("cur.`Relational Exch. Rate Amount`") == F.lit(0)),
        F.lit(1)
    )
    .otherwise(F.col("cur.`Relational Exch. Rate Amount`"))
    .cast("decimal(38,10)")
)

d38 = lambda c: F.col(c).cast("decimal(38,10)")

fx_full = joined.select(
    F.col("sp.ShipmentWeek").alias("ShipmentWeek"),
    F.col("sp.CusNo").alias("CusNo"),
    F.col("sp.CusName").alias("CusName"),
    F.col("sp.ShiptoName").alias("ShiptoName"),
    F.col("sp.BilltoName").alias("BilltoName"),
    F.col("sp.StatusSO").alias("StatusSO"),
    F.col("sp.PostDate").cast("date").alias("PostDate"),
    F.col("sp.DocDate").cast("date").alias("DocDate"),
    F.col("sp.OrderDate").cast("date").alias("OrderDate"),
    F.col("sp.DueDate").cast("date").alias("DueDate"),
    F.col("sp.ReqDate").cast("date").alias("ReqDate"),
    F.col("sp.PmDate").cast("date").alias("PmDate"),
    F.col("sp.ShipDate").cast("date").alias("ShipDate"),
    F.col("sp.ShipType").alias("ShipType"),

    F.col("sp.Itemgroup").alias("Itemgroup"),
    F.col("sp.GenprodGroup").alias("GenprodGroup"),
    F.col("sp.Location_Code").alias("Location_Code"),
    F.col("sp.ExternalDocumentNo").alias("ExternalDocumentNo"),
    F.col("sp.ReleasedDate").cast("date").alias("ReleasedDate"),
    F.col("sp.CSNoted").alias("CSNoted"),
    F.col("sp.CSteam").alias("CSteam"),
    F.col("sp.DocumentType").alias("DocumentType"),
    F.col("sp.SalesorderNo").alias("SalesorderNo"),
    F.col("sp.linenoo").cast("int").alias("linenoo"),
    F.col("sp.ItemFG").alias("ItemFG"),
    F.col("sp.Description").alias("Description"),
    F.col("sp.TypeofFG").alias("TypeofFG"),
    F.col("sp.UOM").alias("UOM"),

    d38("sp.Totalqty").alias("Totalqty"),
    d38("sp.QtytoShip").alias("QtytoShip"),
    d38("sp.QtyShip").alias("QtyShip"),
    d38("sp.QtytoInv").alias("QtytoInv"),
    d38("sp.QtyINV").alias("QtyINV"),
    d38("sp.UnitPrice").alias("UnitPrice"),
    F.col("sp.Currency").alias("Currency"),
    d38("sp.Outstanding").alias("Outstanding"),
    d38("sp.UnitCostLCY").alias("UnitCostLCY"),
    F.col("sp.Shortcut_Dimension2_Code").alias("Shortcut_Dimension2_Code"),

    rate.alias("relationalExchRateAmount"),

    (d38("sp.Totalqty") * d38("sp.UnitPrice")).alias("Amount"),
    (d38("sp.UnitPrice") * rate).alias("AmountTHB"),
    (d38("sp.Totalqty") * (d38("sp.UnitPrice") * rate)).alias("TotalTHB"),
    (d38("sp.QtyShip") * (d38("sp.UnitPrice") * rate)).alias("TotalShipTHB"),
    ((d38("sp.Totalqty") - d38("sp.QtyShip")) * (d38("sp.UnitPrice") * rate)).alias("OutstandingAmountTHB"),
    (d38("sp.QtyShip") - d38("sp.QtyINV")).alias("QtyShipNotInv"),
    ((d38("sp.QtyShip") - d38("sp.QtyINV")) * (d38("sp.UnitPrice") * rate)).alias("ShipNotInvoiceTHB"),
    (
        (d38("sp.Totalqty") * (d38("sp.UnitPrice") * rate)) -
        (d38("sp.QtyShip") * (d38("sp.UnitPrice") * rate))
    ).alias("Outs"),

    F.concat(F.col("sp.SalesorderNo"), F.col("sp.linenoo").cast("string")).alias("so_sol"),
    F.concat(F.col("sp.SalesorderNo"), F.col("sp.ItemFG")).alias("SOI"),

    F.col("sp.so_SystemModifiedAt").alias("so_SystemModifiedAt"),
    F.col("sp.sol_SystemModifiedAt").alias("sol_SystemModifiedAt"),
)

# 4) FULL REPLACE target (overwrite)
(
    fx_full
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET)
)

# --- Alternative FULL REPLACE option (SQL): TRUNCATE + INSERT ---
# fx_full.createOrReplaceTempView("gold_sales_order_shipment_fact_fx_full")
# spark.sql(f"TRUNCATE TABLE {TARGET}")
# spark.sql(f"INSERT INTO {TARGET} SELECT * FROM gold_sales_order_shipment_fact_fx_full")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Posted Sales Inv

# CELL ********************

# Spark (Delta) FULL REPLACE -> gold_posted_sales_inv
# Source:
#   invL = Silver_BC_Lakehouse.bc.`Sales Invoice Line`   (I)
#   invH = Silver_BC_Lakehouse.bc.`Sales Invoice Header` (H)
#   salesLine = Silver_BC_Lakehouse.bc.`Sales Line`      (S)
# Join:
#   I.`Document No.` = H.`No.`  (LEFT JOIN)
#   I.`Order No.` = S.`Document No.` AND I.`Order Line No.` = S.`Line No.` (LEFT JOIN)
# Strategy:
#   rebuild full dataset then OVERWRITE target delta table

from pyspark.sql import functions as F

TARGET = "Gold_Customer_Exp_Lakehouse.cx.gold_posted_sales_inv"
INVL_SRC = "Silver_BC_Lakehouse.bc.`Sales Invoice Line`"
INVH_SRC = "Silver_BC_Lakehouse.bc.`Sales Invoice Header`"
SALESL_SRC = "Silver_BC_Lakehouse.bc.`Sales Line`"

d38 = lambda c: F.col(c).cast("decimal(38,10)")

# 0) Create target table if not exists (Delta)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    customer STRING,
    CusNo STRING,
    invNo STRING,
    Line_No INT,
    Document_Date DATE,
    Due_Date DATE,
    Posting_Date DATE,
    Shipment_Date DATE,
    Salesperson_Code STRING,
    Order_No STRING,

    Gen_Prod_Posting_Group STRING,
    itemFG STRING,
    Description STRING,
    Quantity DECIMAL(38,10),
    UOM STRING,
    Unit_Cost_LCY DECIMAL(38,10),
    Unit_Price DECIMAL(38,10),
    Currency_Code STRING,

    -- tracking
    INVH_No STRING,
    INVL_DocumentNo STRING,
    INVL_LineNo INT,
    INVH_SystemCreatedAt TIMESTAMP,
    INVH_SystemModifiedAt TIMESTAMP,
    INVL_SystemCreatedAt TIMESTAMP,
    INVL_SystemModifiedAt TIMESTAMP
)
USING DELTA
""")

# 1) Read sources
invh   = spark.table(INVH_SRC).alias("H")   # Sales Invoice Header
invl   = spark.table(INVL_SRC).alias("I")   # Sales Invoice Line
salesl = spark.table(SALESL_SRC).alias("S") # Sales Line

# 2) Join invoice lines -> invoice header (LEFT JOIN)
base = invl.join(
    invh,
    on=(F.col("I.`Document No.`") == F.col("H.`No.`")),
    how="left"
)

# 3) Join invoice lines -> sales line (LEFT JOIN)
joined = base.join(
    salesl,
    on=(
        (F.col("I.`Order No.`") == F.col("S.`Document No.`")) &
        (F.col("I.`Order Line No.`") == F.col("S.`Line No.`"))
    ),
    how="left"
)

# 4) Select into gold shape (keep your aliases)
gold_full = joined.select(
    F.col("H.`Sell-to Customer Name`").alias("customer"),
    F.col("H.`Sell-to Customer No.`").alias("CusNo"),
    F.col("H.`No.`").alias("invNo"),
    F.col("I.`Order Line No.`").cast("int").alias("Line_No"),
    F.col("H.`Document Date`").cast("date").alias("Document_Date"),
    F.col("H.`Due Date`").cast("date").alias("Due_Date"),
    F.col("H.`Posting Date`").cast("date").alias("Posting_Date"),
    F.col("H.`Shipment Date`").cast("date").alias("Shipment_Date"),
    F.col("H.`Salesperson Code`").alias("Salesperson_Code"),
    F.col("H.`Order No.`").alias("Order_No"),

    F.col("I.`Gen. Prod. Posting Group`").alias("Gen_Prod_Posting_Group"),
    F.col("I.`No.`").alias("itemFG"),
    F.col("I.`Description`").alias("Description"),
    d38("I.`Quantity`").alias("Quantity"),
    F.col("I.`Unit of Measure Code`").alias("UOM"),
    d38("I.`Unit Cost (LCY)`").alias("Unit_Cost_LCY"),
    d38("I.`Unit Price`").alias("Unit_Price"),

    F.col("H.`Currency Code`").alias("Currency_Code"),

    # tracking
    F.col("H.`No.`").alias("INVH_No"),
    F.col("I.`Document No.`").alias("INVL_DocumentNo"),
    F.col("I.`Order Line No.`").cast("int").alias("INVL_LineNo"),
    F.col("H.`SystemCreatedAt`").alias("INVH_SystemCreatedAt"),
    F.col("H.`SystemModifiedAt`").alias("INVH_SystemModifiedAt"),
    F.col("I.`SystemCreatedAt`").alias("INVL_SystemCreatedAt"),
    F.col("I.`SystemModifiedAt`").alias("INVL_SystemModifiedAt"),
)

# 5) FULL REPLACE: overwrite the target delta table
(
    gold_full.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # gold_posted_sales_inv_join_so

# CELL ********************

from pyspark.sql import functions as F

TARGET = "Gold_Customer_Exp_Lakehouse.cx.gold_posted_sales_inv_join_so"
INV_SRC  = "Gold_Customer_Exp_Lakehouse.cx.gold_posted_sales_inv"
FACT_SRC = "Gold_Customer_Exp_Lakehouse.cx.gold_sales_order_shipment_fact_currency"

d38 = lambda c: F.col(c).cast("decimal(38,10)")

# 0) Create target table if not exists (Delta)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    customer STRING,
    CusNo STRING,
    invNo STRING,
    Line_No INT,
    Document_Date DATE,
    Due_Date DATE,
    Posting_Date DATE,
    Shipment_Date DATE,
    Salesperson_Code STRING,
    Gen_Prod_Posting_Group STRING,
    itemFG STRING,
    Description STRING,
    Quantity DECIMAL(38,10),
    UOM STRING,
    Unit_Cost_LCY DECIMAL(38,10),
    Unit_Price DECIMAL(38,10),
    Currency_Code STRING,

    INVH_No STRING,
    INVL_DocumentNo STRING,
    INVL_LineNo INT,
    INVH_SystemCreatedAt TIMESTAMP,
    INVH_SystemModifiedAt TIMESTAMP,
    INVL_SystemCreatedAt TIMESTAMP,
    INVL_SystemModifiedAt TIMESTAMP,
    Order_No STRING,

    SalesorderNo STRING,
    linenoo INT,
    DocDate DATE,
    OrderDate DATE,
    DueDate DATE,
    ReqDate DATE,
    PmDate DATE,
    ShipDate DATE
)
USING DELTA
""")

# 1) Read sources
inv  = spark.table(INV_SRC).alias("inv")
fact = spark.table(FACT_SRC).alias("fact")

# 2) Build FULL dataset
gold_full = (
    inv.join(
        fact,
        on=(
            (F.col("fact.SalesorderNo") == F.col("inv.Order_No")) &
            (F.col("fact.linenoo").cast("int") == F.col("inv.Line_No").cast("int"))
        ),
        how="left"
    )
    .select(
        F.col("inv.customer").alias("customer"),
        F.col("inv.CusNo").alias("CusNo"),
        F.col("inv.invNo").alias("invNo"),
        F.col("inv.Line_No").cast("int").alias("Line_No"),
        F.to_date(F.col("inv.Document_Date")).alias("Document_Date"),
        F.to_date(F.col("inv.Due_Date")).alias("Due_Date"),
        F.to_date(F.col("inv.Posting_Date")).alias("Posting_Date"),
        F.to_date(F.col("inv.Shipment_Date")).alias("Shipment_Date"),
        F.col("inv.Salesperson_Code").alias("Salesperson_Code"),
        F.col("inv.Gen_Prod_Posting_Group").alias("Gen_Prod_Posting_Group"),
        F.col("inv.itemFG").alias("itemFG"),
        F.col("inv.Description").alias("Description"),
        d38("inv.Quantity").alias("Quantity"),
        F.col("inv.UOM").alias("UOM"),
        d38("inv.Unit_Cost_LCY").alias("Unit_Cost_LCY"),
        d38("inv.Unit_Price").alias("Unit_Price"),
        F.col("inv.Currency_Code").alias("Currency_Code"),

        # tracking from inv
        F.col("inv.INVH_No").alias("INVH_No"),
        F.col("inv.INVL_DocumentNo").alias("INVL_DocumentNo"),
        F.col("inv.INVL_LineNo").cast("int").alias("INVL_LineNo"),
        F.col("inv.INVH_SystemCreatedAt").alias("INVH_SystemCreatedAt"),
        F.col("inv.INVH_SystemModifiedAt").alias("INVH_SystemModifiedAt"),
        F.col("inv.INVL_SystemCreatedAt").alias("INVL_SystemCreatedAt"),
        F.col("inv.INVL_SystemModifiedAt").alias("INVL_SystemModifiedAt"),
        F.col("inv.Order_No").alias("Order_No"),

        # from fact
        F.col("fact.SalesorderNo").alias("SalesorderNo"),
        F.col("fact.linenoo").cast("int").alias("linenoo"),
        F.to_date(F.col("fact.DocDate")).alias("DocDate"),
        F.to_date(F.col("fact.OrderDate")).alias("OrderDate"),
        F.to_date(F.col("fact.DueDate")).alias("DueDate"),
        F.to_date(F.col("fact.ReqDate")).alias("ReqDate"),
        F.to_date(F.col("fact.PmDate")).alias("PmDate"),
        F.to_date(F.col("fact.ShipDate")).alias("ShipDate"),
    )
)

# 3) FULL REPLACE: overwrite the entire Delta table
(
    gold_full.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Posted Sales Inv Amount

# CELL ********************

# Spark (Delta) FULL REPLACE -> gold_posted_sales_inv_amount
# Source:
#   inv = Gold_Customer_Exp_Lakehouse.cx.gold_posted_sales_inv_join_so
#   cur = Silver_BC_Lakehouse.bc.`Currency Exchange Rate`
# Join:
#   inv.Shipment_Date = cur.`Starting Date`
#   inv.Currency_Code = cur.`Currency Code`
# Window:
#   rn = row_number() over (partition by invNo, Line_No order by Document_Date desc)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

TARGET = "Gold_Customer_Exp_Lakehouse.cx.gold_posted_sales_inv_amount"
INV_SRC = "Gold_Customer_Exp_Lakehouse.cx.gold_posted_sales_inv_join_so"
CUR_SRC = "Silver_BC_Lakehouse.bc.`Currency Exchange Rate`"

d38 = lambda c: F.col(c).cast("decimal(38,10)")

# 0) Create target table if not exists (Delta)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    customer STRING,
    CusNo STRING,
    invNo STRING,
    Line_No INT,
    Document_Date DATE,
    Due_Date DATE,
    Posting_Date DATE,
    Shipment_Date DATE,
    Salesperson_Code STRING,
    Order_No STRING,
    Gen_Prod_Posting_Group STRING,
    itemFG STRING,
    Description STRING,
    Quantity DECIMAL(38,10),
    UOM STRING,
    Unit_Cost_LCY DECIMAL(38,10),
    Unit_Price DECIMAL(38,10),
    Currency_Code STRING,

    relationalExchRateAmount DECIMAL(38,10),
    amountTHB DECIMAL(38,10),
    totalqtyTHB DECIMAL(38,10),

    SalesorderNo STRING,
    linenoo INT,
    DocDate DATE,
    OrderDate DATE,
    DueDate DATE,
    ReqDate DATE,
    PmDate DATE,
    ShipDate DATE,

    rn INT,

    -- tracking passthrough
    INVH_SystemCreatedAt TIMESTAMP,
    INVH_SystemModifiedAt TIMESTAMP
)
USING DELTA
""")

# 1) Read sources
inv = spark.table(INV_SRC).alias("inv")
cur = spark.table(CUR_SRC).alias("cur")

# 2) Join currency rate (LEFT JOIN)
joined = (
    inv.join(
        cur,
        on=(
            (F.col("inv.Shipment_Date").cast("date") == F.col("cur.`Starting Date`").cast("date")) &
            (F.col("inv.Currency_Code") == F.col("cur.`Currency Code`"))
            # & (F.col("cur.`Relational Currency Code`") == F.lit("THB"))  # <- add if needed
        ),
        how="left"
    )
)

# 3) Rate logic
# SQL: ISNULL(NULLIF(cur.[Relational Exch. Rate Amount], 0), 1)
rate = (
    F.when(
        (F.col("cur.`Relational Exch. Rate Amount`").isNull()) |
        (F.col("cur.`Relational Exch. Rate Amount`") == F.lit(0)),
        F.lit(1)
    )
    .otherwise(F.col("cur.`Relational Exch. Rate Amount`"))
    .cast("decimal(38,10)")
)

# 4) Window rn
w = (
    Window
    .partitionBy(F.col("inv.invNo"), F.col("inv.Line_No"))
    .orderBy(F.col("inv.Document_Date").desc())
)

gold_full = (
    joined
    .withColumn("rn", F.row_number().over(w))
    .select(
        F.col("inv.customer").alias("customer"),
        F.col("inv.CusNo").alias("CusNo"),
        F.col("inv.invNo").alias("invNo"),
        F.col("inv.Line_No").cast("int").alias("Line_No"),
        F.col("inv.Document_Date").cast("date").alias("Document_Date"),
        F.col("inv.Due_Date").cast("date").alias("Due_Date"),
        F.col("inv.Posting_Date").cast("date").alias("Posting_Date"),
        F.col("inv.Shipment_Date").cast("date").alias("Shipment_Date"),
        F.col("inv.Salesperson_Code").alias("Salesperson_Code"),
        F.col("inv.Order_No").alias("Order_No"),
        F.col("inv.Gen_Prod_Posting_Group").alias("Gen_Prod_Posting_Group"),
        F.col("inv.itemFG").alias("itemFG"),
        F.col("inv.Description").alias("Description"),
        d38("inv.Quantity").alias("Quantity"),
        F.col("inv.UOM").alias("UOM"),
        d38("inv.Unit_Cost_LCY").alias("Unit_Cost_LCY"),
        d38("inv.Unit_Price").alias("Unit_Price"),
        F.col("inv.Currency_Code").alias("Currency_Code"),

        rate.alias("relationalExchRateAmount"),
        (d38("inv.Unit_Price") * rate).alias("amountTHB"),
        (d38("inv.Quantity") * (d38("inv.Unit_Price") * rate)).alias("totalqtyTHB"),

        F.col("inv.SalesorderNo").alias("SalesorderNo"),
        F.col("inv.linenoo").cast("int").alias("linenoo"),
        F.col("inv.DocDate").cast("date").alias("DocDate"),
        F.col("inv.OrderDate").cast("date").alias("OrderDate"),
        F.col("inv.DueDate").cast("date").alias("DueDate"),
        F.col("inv.ReqDate").cast("date").alias("ReqDate"),
        F.col("inv.PmDate").cast("date").alias("PmDate"),
        F.col("inv.ShipDate").cast("date").alias("ShipDate"),

        F.col("rn").cast("int").alias("rn"),

        # tracking passthrough
        F.col("inv.INVH_SystemCreatedAt").alias("INVH_SystemCreatedAt"),
        F.col("inv.INVH_SystemModifiedAt").alias("INVH_SystemModifiedAt"),
    )
)

# 5) FULL REPLACE: overwrite the target delta table
(
    gold_full.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Posted Sales Inv Amount Merged

# CELL ********************

from pyspark.sql import functions as F

TARGET = "Gold_Customer_Exp_Lakehouse.cx.gold_posted_sales_inv_amount_merged"

BC_SRC  = "Gold_Customer_Exp_Lakehouse.cx.gold_posted_sales_inv_amount"
SAP_SRC = "Silver_Commons_Lakehouse.cmn.silver_AR_SAP_022018_072025"
CUS_SRC = "Silver_BC_Lakehouse.bc.`Customer`"
CL_SRC  = "Silver_BC_Lakehouse.bc.`Cust Ledger Entry`"

d38 = lambda c: F.col(c).cast("decimal(38,10)")

# 0) Create target table if not exists (Delta)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    customer STRING,
    CusNo STRING,
    invNo STRING,
    Line_No INT,
    Document_Date DATE,
    Due_Date DATE,
    Posting_Date DATE,
    Shipment_Date DATE,
    Salesperson_Code STRING,
    Order_No STRING,
    Gen_Prod_Posting_Group STRING,
    Item_Category STRING,
    itemFG STRING,
    Description STRING,
    Quantity DECIMAL(38,10),
    UOM STRING,
    Unit_Cost_LCY DECIMAL(38,10),
    Unit_Price DECIMAL(38,10),
    Currency_Code STRING,
    relationalExchRateAmount DECIMAL(38,10),
    amountTHB DECIMAL(38,10),
    totalqtyTHB DECIMAL(38,10),

    SalesorderNo STRING,
    linenoo INT,
    DocDate DATE,
    OrderDate DATE,
    DueDate DATE,
    ReqDate DATE,
    PmDate DATE,
    ShipDate DATE,

    rn INT,
    INVH_SystemCreatedAt TIMESTAMP,
    INVH_SystemModifiedAt TIMESTAMP,
    source_system STRING,

    AR_Amount_THB DECIMAL(38,10)
)
USING DELTA
""")

# =========================
# 1) Read sources
# =========================
bc_src  = spark.table(BC_SRC).alias("bc")
sap_raw = spark.table(SAP_SRC).alias("sap")
cus     = spark.table(CUS_SRC).alias("cus")

# =========================
# 2) BC side (aligned schema)
# =========================
bc = (
    bc_src
    .withColumn("Posting_Date", F.to_date(F.col("Posting_Date")))
    .withColumn("Item_Category", F.expr("right(Gen_Prod_Posting_Group, 3)"))
    .select(
        F.col("customer"),
        F.col("CusNo"),
        F.col("invNo"),
        F.col("Line_No").cast("int").alias("Line_No"),
        F.to_date(F.col("Document_Date")).alias("Document_Date"),
        F.to_date(F.col("Due_Date")).alias("Due_Date"),
        F.to_date(F.col("Posting_Date")).alias("Posting_Date"),
        F.to_date(F.col("Shipment_Date")).alias("Shipment_Date"),
        F.col("Salesperson_Code"),
        F.col("Order_No"),
        F.col("Gen_Prod_Posting_Group"),
        F.col("Item_Category"),
        F.col("itemFG"),
        F.col("Description"),
        d38("Quantity").alias("Quantity"),
        F.col("UOM"),
        d38("Unit_Cost_LCY").alias("Unit_Cost_LCY"),
        d38("Unit_Price").alias("Unit_Price"),
        F.col("Currency_Code"),
        d38("relationalExchRateAmount").alias("relationalExchRateAmount"),
        d38("amountTHB").alias("amountTHB"),
        d38("totalqtyTHB").alias("totalqtyTHB"),

        F.col("SalesorderNo").alias("SalesorderNo"),
        F.col("linenoo").cast("int").alias("linenoo"),
        F.to_date(F.col("DocDate")).alias("DocDate"),
        F.to_date(F.col("OrderDate")).alias("OrderDate"),
        F.to_date(F.col("DueDate")).alias("DueDate"),
        F.to_date(F.col("ReqDate")).alias("ReqDate"),
        F.to_date(F.col("PmDate")).alias("PmDate"),
        F.to_date(F.col("ShipDate")).alias("ShipDate"),

        F.col("rn").cast("int").alias("rn"),
        F.col("INVH_SystemCreatedAt"),
        F.col("INVH_SystemModifiedAt"),
        F.lit("BC").alias("source_system"),

        # placeholder; computed after cust ledger join
        F.lit(None).cast("decimal(38,10)").alias("AR_Amount_THB"),
    )
)

# =========================
# 3) SAP side (aligned schema)
# =========================
sap = (
    sap_raw
    .withColumn("Posting_Date", F.to_date(F.col("posting_date")))
    .join(
        cus,
        on=(F.upper(F.col("cus.`Name`")) == F.upper(F.col("sap.customer_name"))),
        how="left"
    )
    .select(
        F.col("sap.customer_name").alias("customer"),
        F.col("cus.`No.`").alias("CusNo"),
        F.lit(None).cast("string").alias("invNo"),
        F.lit(None).cast("int").alias("Line_No"),
        F.lit(None).cast("date").alias("Document_Date"),
        F.lit(None).cast("date").alias("Due_Date"),
        F.col("Posting_Date").alias("Posting_Date"),
        F.lit(None).cast("date").alias("Shipment_Date"),
        F.lit(None).cast("string").alias("Salesperson_Code"),
        F.lit(None).cast("string").alias("Order_No"),
        F.lit(None).cast("string").alias("Gen_Prod_Posting_Group"),
        F.lit(None).cast("string").alias("Item_Category"),
        F.lit(None).cast("string").alias("itemFG"),
        F.lit(None).cast("string").alias("Description"),
        F.lit(None).cast("decimal(38,10)").alias("Quantity"),
        F.lit(None).cast("string").alias("UOM"),
        F.lit(None).cast("decimal(38,10)").alias("Unit_Cost_LCY"),
        F.lit(None).cast("decimal(38,10)").alias("Unit_Price"),
        F.lit(None).cast("string").alias("Currency_Code"),
        F.lit(None).cast("decimal(38,10)").alias("relationalExchRateAmount"),
        F.col("sap.AR_amount").cast("decimal(38,10)").alias("amountTHB"),
        F.lit(None).cast("decimal(38,10)").alias("totalqtyTHB"),

        F.lit(None).cast("string").alias("SalesorderNo"),
        F.lit(None).cast("int").alias("linenoo"),
        F.lit(None).cast("date").alias("DocDate"),
        F.lit(None).cast("date").alias("OrderDate"),
        F.lit(None).cast("date").alias("DueDate"),
        F.lit(None).cast("date").alias("ReqDate"),
        F.lit(None).cast("date").alias("PmDate"),
        F.lit(None).cast("date").alias("ShipDate"),

        F.lit(None).cast("int").alias("rn"),
        F.lit(None).cast("timestamp").alias("INVH_SystemCreatedAt"),
        F.lit(None).cast("timestamp").alias("INVH_SystemModifiedAt"),
        F.lit("SAP").alias("source_system"),

        # placeholder; computed after cust ledger join
        F.lit(None).cast("decimal(38,10)").alias("AR_Amount_THB"),
    )
)

# =========================
# 4) Union BC + SAP
# =========================
base_union = bc.unionByName(sap, allowMissingColumns=True).alias("u")

# =========================
# 5) Cust Ledger join + AR_Amount_THB
# =========================
cl = (
    spark.table(CL_SRC)
    .select(
        F.col("`Document No.`").alias("Document_No"),
        F.col("`Customer No.`").alias("Customer_No"),
        F.col("`Customer Name`").alias("Customer_Name"),
        F.to_date(F.col("`Posting Date`")).alias("Ledger_Posting_Date"),
        F.col("`Sales (LCY)`").cast("decimal(38,10)").alias("Sales_LCY"),
    )
    .alias("cl")
)

final_df = (
    base_union.join(cl, on=(F.col("u.invNo") == F.col("cl.Document_No")), how="left")
    .withColumn(
        "AR_Amount_THB",
        F.coalesce(F.col("cl.Sales_LCY"), F.col("u.amountTHB")).cast("decimal(38,10)")
    )
    .select(
        "u.customer","u.CusNo","u.invNo","u.Line_No","u.Document_Date","u.Due_Date","u.Posting_Date","u.Shipment_Date",
        "u.Salesperson_Code","u.Order_No","u.Gen_Prod_Posting_Group","u.Item_Category","u.itemFG","u.Description",
        "u.Quantity","u.UOM","u.Unit_Cost_LCY","u.Unit_Price","u.Currency_Code","u.relationalExchRateAmount",
        "u.amountTHB","u.totalqtyTHB","u.SalesorderNo","u.linenoo","u.DocDate","u.OrderDate","u.DueDate",
        "u.ReqDate","u.PmDate","u.ShipDate","u.rn","u.INVH_SystemCreatedAt","u.INVH_SystemModifiedAt",
        "u.source_system",
        F.col("AR_Amount_THB")
    )
)

# =========================
# 6) FULL REPLACE: overwrite target
# =========================
(
    final_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Posted Sales Shipment

# CELL ********************

# Spark (Delta) FULL REPLACE -> gold_posted_sales_shipment
# Source:
#   sh  = Silver_BC_Lakehouse.bc.`Sales Shipment Header`
#   shL = Silver_BC_Lakehouse.bc.`Sales Shipment Line`
# Join:
#   sh.`No.` = shL.`Document No.`
# Strategy:
#   rebuild full dataset then OVERWRITE target delta table

from pyspark.sql import functions as F

TARGET = "Gold_Customer_Exp_Lakehouse.cx.gold_posted_sales_shipment"
SH_SRC = "Silver_BC_Lakehouse.bc.`Sales Shipment Header`"
SHL_SRC = "Silver_BC_Lakehouse.bc.`Sales Shipment Line`"

d38 = lambda c: F.col(c).cast("decimal(38,10)")

# 1) Create target table if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    shNo STRING,
    Line_No INT,
    cusName STRING,
    CusNo STRING,
    Posting_Date DATE,
    Document_Date DATE,
    Requested_Delivery_Date DATE,
    Promised_Delivery_Date DATE,
    Planned_Delivery_Date DATE,
    Planned_Shipment_Date DATE,
    Shipment_Date DATE,
    Salesperson_Code STRING,
    Order_No STRING,

    item STRING,
    Description STRING,
    uom STRING,
    Quantity DECIMAL(38,10),
    Quantity_Invoiced DECIMAL(38,10),
    Qty_Shipped_Not_Invoiced DECIMAL(38,10),
    Shortcut_Dimension2_Code STRING,

    soi STRING,

    -- tracking
    SH_No STRING,
    SHL_DocumentNo STRING,
    SHL_LineNo INT,
    SH_SystemCreatedAt TIMESTAMP,
    SH_SystemModifiedAt TIMESTAMP,
    SHL_SystemCreatedAt TIMESTAMP,
    SHL_SystemModifiedAt TIMESTAMP
)
USING DELTA
""")

# 2) Read sources (FULL dataset)
sh  = spark.table(SH_SRC).alias("sh")
shl = spark.table(SHL_SRC).alias("shL")

# 3) Join header + line (INNER JOIN to match your incremental rebuild logic)
joined = sh.join(
    shl,
    on=(F.col("sh.`No.`") == F.col("shL.`Document No.`")),
    how="inner"
)

# 4) Select gold schema
gold_full = joined.select(
    F.col("sh.`No.`").alias("shNo"),
    F.col("shL.`Line No.`").cast("int").alias("Line_No"),
    F.col("sh.`Sell-to Customer Name`").alias("cusName"),
    F.col("sh.`Sell-to Customer No.`").alias("CusNo"),
    F.col("sh.`Posting Date`").cast("date").alias("Posting_Date"),
    F.col("sh.`Document Date`").cast("date").alias("Document_Date"),
    F.col("sh.`Requested Delivery Date`").cast("date").alias("Requested_Delivery_Date"),
    F.col("sh.`Promised Delivery Date`").cast("date").alias("Promised_Delivery_Date"),
    F.col("shL.`Planned Delivery Date`").cast("date").alias("Planned_Delivery_Date"),
    F.col("shL.`Planned Shipment Date`").cast("date").alias("Planned_Shipment_Date"),
    F.col("sh.`Shipment Date`").cast("date").alias("Shipment_Date"),
    F.col("sh.`Salesperson Code`").alias("Salesperson_Code"),
    F.col("shL.`Order No.`").alias("Order_No"),

    F.col("shL.`No.`").alias("item"),
    F.col("shL.`Description`").alias("Description"),
    F.col("shL.`Unit of Measure Code`").alias("uom"),
    d38("shL.`Quantity`").alias("Quantity"),
    d38("shL.`Quantity Invoiced`").alias("Quantity_Invoiced"),
    d38("shL.`Qty. Shipped Not Invoiced`").alias("Qty_Shipped_Not_Invoiced"),
    F.col("shL.`Shortcut Dimension 2 Code`").alias("Shortcut_Dimension2_Code"),

    F.concat(F.col("shL.`Order No.`"), F.col("shL.`No.`")).alias("soi"),

    # tracking
    F.col("sh.`No.`").alias("SH_No"),
    F.col("shL.`Document No.`").alias("SHL_DocumentNo"),
    F.col("shL.`Line No.`").cast("int").alias("SHL_LineNo"),
    F.col("sh.`SystemCreatedAt`").alias("SH_SystemCreatedAt"),
    F.col("sh.`SystemModifiedAt`").alias("SH_SystemModifiedAt"),
    F.col("shL.`SystemCreatedAt`").alias("SHL_SystemCreatedAt"),
    F.col("shL.`SystemModifiedAt`").alias("SHL_SystemModifiedAt"),
)

# 5) FULL REPLACE: overwrite the entire Delta table
(
    gold_full.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Posted Sales Shipment Amount

# CELL ********************

# Spark (Delta) FULL REPLACE -> gold_posted_sales_shipment_amount
# Source:
#   sh = Gold_Customer_Exp_Lakehouse.cx.gold_posted_sales_shipment
#   so = Gold_Customer_Exp_Lakehouse.cx.gold_sales_order_shipment_fact_currency
# Join:
#   sh.soi = so.SOI  (LEFT JOIN)
# Strategy:
#   rebuild full dataset then OVERWRITE target delta table

from pyspark.sql import functions as F
from pyspark.sql.window import Window

TARGET = "Gold_Customer_Exp_Lakehouse.cx.gold_posted_sales_shipment_amount"
SH_SRC = "Gold_Customer_Exp_Lakehouse.cx.gold_posted_sales_shipment"
SO_SRC = "Gold_Customer_Exp_Lakehouse.cx.gold_sales_order_shipment_fact_currency"

d38 = lambda c: F.col(c).cast("decimal(38,10)")

# 1) Create target table if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    shNo STRING,
    Line_No INT,
    cusName STRING,
    CusNo STRING,
    Document_Date DATE,
    Requested_Delivery_Date DATE,
    Promised_Delivery_Date DATE,
    Planned_Delivery_Date DATE,
    Planned_Shipment_Date DATE,
    Posting_Date DATE,
    Shipment_Date DATE,
    Salesperson_Code STRING,
    Order_No STRING,

    OrderDate DATE,
    ReqDate DATE,
    ShipDate DATE,
    ShipType STRING,
    StatusSO STRING,

    item STRING,
    Description STRING,
    uom STRING,
    Quantity DECIMAL(38,10),
    Quantity_Invoiced DECIMAL(38,10),
    Qty_Shipped_Not_Invoiced DECIMAL(38,10),

    UnitPrice DECIMAL(38,10),
    AmountShipped DECIMAL(38,10),
    AmountTHB DECIMAL(38,10),
    AmountShippednotInvoiced DECIMAL(38,10),

    rn INT,

    Shortcut_Dimension2_Code STRING,
    soi STRING,

    -- tracking
    SH_SystemCreatedAt TIMESTAMP,
    SH_SystemModifiedAt TIMESTAMP
)
USING DELTA
""")

# 2) Read sources (FULL dataset)
sh = spark.table(SH_SRC).alias("sh")
so = spark.table(SO_SRC).alias("so")

# 3) Join like your SQL
joined = sh.join(
    so,
    on=(F.col("sh.soi") == F.col("so.SOI")),
    how="left"
)

# 4) Window ROW_NUMBER() PARTITION BY shNo, Line_No ORDER BY Document_Date DESC
w = (
    Window
    .partitionBy(F.col("sh.shNo"), F.col("sh.Line_No"))
    .orderBy(F.col("sh.Document_Date").desc())
)

gold_full = (
    joined
    .withColumn("rn", F.row_number().over(w))
    .select(
        F.col("sh.shNo").alias("shNo"),
        F.col("sh.Line_No").cast("int").alias("Line_No"),
        F.col("sh.cusName").alias("cusName"),
        F.col("sh.CusNo").alias("CusNo"),
        F.col("sh.Document_Date").cast("date").alias("Document_Date"),
        F.col("sh.Requested_Delivery_Date").cast("date").alias("Requested_Delivery_Date"),
        F.col("sh.Promised_Delivery_Date").cast("date").alias("Promised_Delivery_Date"),
        F.col("sh.Planned_Delivery_Date").cast("date").alias("Planned_Delivery_Date"),
        F.col("sh.Planned_Shipment_Date").cast("date").alias("Planned_Shipment_Date"),
        F.col("sh.Posting_Date").cast("date").alias("Posting_Date"),
        F.col("sh.Shipment_Date").cast("date").alias("Shipment_Date"),
        F.col("sh.Salesperson_Code").alias("Salesperson_Code"),
        F.col("sh.Order_No").alias("Order_No"),

        F.col("so.OrderDate").cast("date").alias("OrderDate"),
        F.col("so.ReqDate").cast("date").alias("ReqDate"),
        F.col("so.ShipDate").cast("date").alias("ShipDate"),
        F.col("so.ShipType").alias("ShipType"),
        F.col("so.StatusSO").alias("StatusSO"),

        F.col("sh.item").alias("item"),
        F.col("sh.Description").alias("Description"),
        F.col("sh.uom").alias("uom"),
        d38("sh.Quantity").alias("Quantity"),
        d38("sh.Quantity_Invoiced").alias("Quantity_Invoiced"),
        d38("sh.Qty_Shipped_Not_Invoiced").alias("Qty_Shipped_Not_Invoiced"),

        d38("so.UnitPrice").alias("UnitPrice"),

        # Note: your original incremental code uses Qty_Shipped_Not_Invoiced * UnitPrice
        (d38("sh.Qty_Shipped_Not_Invoiced") * d38("so.UnitPrice")).alias("AmountShipped"),

        d38("so.AmountTHB").alias("AmountTHB"),
        (d38("sh.Qty_Shipped_Not_Invoiced") * d38("so.AmountTHB")).alias("AmountShippednotInvoiced"),

        F.col("rn").cast("int").alias("rn"),

        F.col("sh.Shortcut_Dimension2_Code").alias("Shortcut_Dimension2_Code"),
        F.col("sh.soi").alias("soi"),

        # tracking passthrough
        F.col("sh.SH_SystemCreatedAt").alias("SH_SystemCreatedAt"),
        F.col("sh.SH_SystemModifiedAt").alias("SH_SystemModifiedAt"),
    )
)

# If you only want rn=1, uncomment:
# gold_full = gold_full.filter(F.col("rn") == 1)

# 5) FULL REPLACE: overwrite the target delta table
(
    gold_full.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Posted Sales Shipment Amount Used

# CELL ********************

# Spark (Delta) FULL REPLACE -> gold_posted_sales_shipment_amount_used
# Source:
#   Gold_Customer_Exp_Lakehouse.cx.gold_posted_sales_shipment_amount
# Logic:
#   take ONLY rn = 1
# Strategy:
#   rebuild full dataset then OVERWRITE target delta table

from pyspark.sql import functions as F

TARGET = "Gold_Customer_Exp_Lakehouse.cx.gold_posted_sales_shipment_amount_used"
SRC = "Gold_Customer_Exp_Lakehouse.cx.gold_posted_sales_shipment_amount"

d38 = lambda c: F.col(c).cast("decimal(38,10)")

# 1) Create target table if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    shNo STRING,
    Line_No INT,
    cusName STRING,
    CusNo STRING,
    Document_Date DATE,
    Requested_Delivery_Date DATE,
    Promised_Delivery_Date DATE,
    Planned_Delivery_Date DATE,
    Planned_Shipment_Date DATE,
    Posting_Date DATE,
    Shipment_Date DATE,
    Salesperson_Code STRING,
    Order_No STRING,
    OrderDate DATE,
    ReqDate DATE,
    ShipDate DATE,
    ShipType STRING,
    StatusSO STRING,
    item STRING,
    Description STRING,
    uom STRING,
    Quantity DECIMAL(38,10),
    Quantity_Invoiced DECIMAL(38,10),
    Qty_Shipped_Not_Invoiced DECIMAL(38,10),
    UnitPrice DECIMAL(38,10),
    AmountShipped DECIMAL(38,10),
    AmountTHB DECIMAL(38,10),
    AmountShippednotInvoiced DECIMAL(38,10),
    Shortcut_Dimension2_Code STRING,
    soi STRING,

    -- tracking
    SH_SystemCreatedAt TIMESTAMP,
    SH_SystemModifiedAt TIMESTAMP
)
USING DELTA
""")

# 2) Read source (FULL dataset) and keep rn = 1
src = spark.table(SRC)

gold_full = (
    src
    .filter(F.col("rn") == 1)
    .select(
        F.col("shNo").alias("shNo"),
        F.col("Line_No").cast("int").alias("Line_No"),
        F.col("cusName").alias("cusName"),
        F.col("CusNo").alias("CusNo"),
        F.col("Document_Date").cast("date").alias("Document_Date"),
        F.col("Requested_Delivery_Date").cast("date").alias("Requested_Delivery_Date"),
        F.col("Promised_Delivery_Date").cast("date").alias("Promised_Delivery_Date"),
        F.col("Planned_Delivery_Date").cast("date").alias("Planned_Delivery_Date"),
        F.col("Planned_Shipment_Date").cast("date").alias("Planned_Shipment_Date"),
        F.col("Posting_Date").cast("date").alias("Posting_Date"),
        F.col("Shipment_Date").cast("date").alias("Shipment_Date"),
        F.col("Salesperson_Code").alias("Salesperson_Code"),
        F.col("Order_No").alias("Order_No"),
        F.col("OrderDate").cast("date").alias("OrderDate"),
        F.col("ReqDate").cast("date").alias("ReqDate"),
        F.col("ShipDate").cast("date").alias("ShipDate"),
        F.col("ShipType").alias("ShipType"),
        F.col("StatusSO").alias("StatusSO"),
        F.col("item").alias("item"),
        F.col("Description").alias("Description"),
        F.col("uom").alias("uom"),
        d38("Quantity").alias("Quantity"),
        d38("Quantity_Invoiced").alias("Quantity_Invoiced"),
        d38("Qty_Shipped_Not_Invoiced").alias("Qty_Shipped_Not_Invoiced"),
        d38("UnitPrice").alias("UnitPrice"),
        d38("AmountShipped").alias("AmountShipped"),
        d38("AmountTHB").alias("AmountTHB"),
        d38("AmountShippednotInvoiced").alias("AmountShippednotInvoiced"),
        F.col("Shortcut_Dimension2_Code").alias("Shortcut_Dimension2_Code"),
        F.col("soi").alias("soi"),

        # tracking
        F.col("SH_SystemCreatedAt").alias("SH_SystemCreatedAt"),
        F.col("SH_SystemModifiedAt").alias("SH_SystemModifiedAt"),
    )
)

# 3) FULL REPLACE: overwrite the target delta table
(
    gold_full.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# # Gold Posted Sales Inv Amount SH

# CELL ********************

# ============================================================
# FULL REFRESH (recommended): Production Order + Sales/Shipment join
# Reason: multi-table joins + status/dates can change; full refresh avoids stale results.
#
# Target table name (edit if you want):
#   Gold_Customer_Exp_Lakehouse.cx.gold_posted_sales_inv_amount_sh
# ============================================================

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT_TBL = "Gold_Customer_Exp_Lakehouse.cx.gold_posted_sales_inv_amount_sh"

src_sql = """
WITH pod_src AS (
    -- engine-anchored due date: MAX(scheduled_end_date) within the latest
    -- engine_run per prod_order_no, from planning_forward_schedule.
    SELECT
        prod_order_no,
        MAX(scheduled_end_date) AS planned_prod_order_due_date
    FROM (
        SELECT
            prod_order_no,
            scheduled_end_date,
            DENSE_RANK() OVER (
                PARTITION BY prod_order_no
                ORDER BY engine_run_ts DESC
            ) AS run_rank
        FROM Gold_Production_Lakehouse.prod.planning_forward_schedule
    )
    WHERE run_rank = 1
    GROUP BY prod_order_no
)
SELECT
    -- Production Order
    COALESCE(pod.planned_prod_order_due_date, po.`Due Date`) AS `Due_Date`,
    po.`Status`                    AS `Status`,
    po.`No.`                       AS `No`,
    po.`Description`               AS `Prod_Description`,
    po.`Description 2`             AS `Prod_Description_2`,
    po.`Source No.`                AS `Source_No`,
    po.`Routing No.`               AS `Routing_No`,
    po.`Quantity`                  AS `Prod_Quantity`,
    po.`Prod. Order Type.`         AS `Prod_Order_Type`,
    po.`Sales Order No.`           AS `Sales_Order_No`,
    po.`Sales Order Line No.`      AS `Sales_Order_Line_No`,
    po.`Shortcut Dimension 1 Code` AS `Shortcut_Dimension_1_Code`,
    po.`Shortcut Dimension 2 Code` AS `Shortcut_Dimension_2_Code`,
    po.`Location Code`             AS `Prod_Location_Code`,
    po.`Starting Date-Time`        AS `Starting_Date_Time`,
    po.`Ending Date-Time`          AS `Ending_Date_Time`,

    -- Sales/Shipment line + header
    sl.`DocumentNo`                AS `DocumentNo`,
    sl.`LineNo`                    AS `LineNo`,
    sh.`PostDate`                  AS `PostingDate`,
    sh.`CusNo`                     AS `SellToCustomerNo`,
    sh.`CusName`                   AS `SellToCustomerName`,
    COALESCE(sl.`Currency`, sh.`Currency`) AS `CurrencyCode`,
    CAST(NULL AS STRING)           AS `ShipmentNo`,
    CAST(NULL AS STRING)           AS `ShipmentLineNo`,
    sl.`ItemFG`                    AS `Item_No`,
    sl.`Description`               AS `Description`,
    CAST(NULL AS STRING)           AS `Description2`,
    sl.`item_location`             AS `LocationCode`,
    sl.`QTY`                       AS `Quantity`,
    sl.`UOM`                       AS `UnitOfMeasureCode`,

    -- ship dates
    sl.`Order_Date`                AS `Order_Date`,
    sl.`Requested_Delivery_Date`   AS `Requested_Delivery_Date`,
    sl.`Promised_Delivery_Date`    AS `Promised_Delivery_Date`

FROM Silver_BC_Lakehouse.bc.`Production Order` po

LEFT JOIN pod_src pod
    ON pod.prod_order_no = po.`No.`

LEFT JOIN Silver_BC_Lakehouse.bc.`Item` it
    ON po.`Source No.` = it.`No.`

LEFT JOIN Gold_Customer_Exp_Lakehouse.cx.`gold_sales_order_shipment_line` sl
    ON po.`Sales Order No.` = sl.`SalesorderNo`
   AND po.`Sales Order Line No.` = sl.`LineNo`

LEFT JOIN Gold_Customer_Exp_Lakehouse.cx.`gold_sales_order_shipment_header` sh
    ON sl.`SalesorderNo` = sh.`SalesorderNo`

WHERE 
    po.`Sales Order No.` LIKE 'SP%'
 OR po.`Sales Order No.` LIKE 'MT%'
 OR po.`Sales Order No.` LIKE 'SL%'
"""

df_src = spark.sql(src_sql)

(
    df_src.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TGT_TBL)
)

print(f"FULL REFRESH completed into: {TGT_TBL}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
