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
# META         },
# META         {
# META           "id": "76781d83-17f8-4270-a81d-6759d1ee9a9d"
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

# # Gold Fact Sale Item Budget

# CELL ********************

from pyspark.sql import functions as F

TARGET = "Gold_Customer_Exp_Lakehouse.cx.gold_fact_sale_item_budget"
SRC = "Silver_BC_Lakehouse.bc.`Item Budget Entry`"

# 1) Create target table if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    entryNo INT,
    budgetName STRING,
    budgDate DATE,
    itemNo STRING,
    sourceType STRING,
    sourceNo STRING,
    Description STRING,
    qty DECIMAL(38,10),
    salesAmt DECIMAL(38,10),
    userID STRING,
    locationCode STRING,

    -- tracking
    SystemCreatedAt TIMESTAMP,
    SystemModifiedAt TIMESTAMP
)
USING DELTA
""")

# 2) Watermark (incremental)
wm = (
    spark.table(TARGET)
    .select(F.max(F.col("SystemModifiedAt")).alias("wm"))
    .collect()[0]["wm"]
)
if wm is None:
    wm = "1900-01-01 00:00:00"

# 3) Read source
src = spark.table(SRC)

# 4) Incremental filter
inc = (
    src.withColumn(
        "wm_ts",
        F.coalesce(F.col("`SystemModifiedAt`"), F.col("`SystemCreatedAt`"))
    )
    .filter(F.col("wm_ts") > F.to_timestamp(F.lit(str(wm))))
)

d38 = lambda c: F.col(c).cast("decimal(38,10)")

# 5) Select with backticks + aliases
gold_inc = inc.select(
    F.col("`Entry No.`").cast("int").alias("entryNo"),
    F.col("`Budget Name`").alias("budgetName"),
    F.col("`Date`").cast("date").alias("budgDate"),
    F.col("`Item No.`").alias("itemNo"),
    F.col("`Source Type`").alias("sourceType"),
    F.col("`Source No.`").alias("sourceNo"),
    F.col("`Description`").alias("Description"),
    d38("`Quantity`").alias("qty"),
    d38("`Sales Amount`").alias("salesAmt"),
    F.col("`User ID`").alias("userID"),
    F.col("`Location Code`").alias("locationCode"),

    # tracking
    F.col("`SystemCreatedAt`").alias("SystemCreatedAt"),
    F.col("`SystemModifiedAt`").alias("SystemModifiedAt"),
)

gold_inc.createOrReplaceTempView("gold_fact_sale_item_budget_inc")

# 6) MERGE into target
# Key: Entry No. (BC unique)
spark.sql(f"""
MERGE INTO {TARGET} AS t
USING gold_fact_sale_item_budget_inc AS s
ON  t.entryNo = s.entryNo
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Item Group

# CELL ********************

# Spark (Delta) FULL reload -> gold_item_group
# Implements your SQL logic on Silver_BC_Lakehouse.bc.`Item`
# - itemno_des = concat(`No.`, `Description`)
# - item_group logic (dash at position 2 -> replace with 000 then take 10; else take 10)
# - item_group_count = row_number over partition by (dash removed + '000' then take 10; else take 10)
# - sub_item_group per your SQL (always 'sub-item group')

from pyspark.sql import functions as F
from pyspark.sql.window import Window

TARGET = "Gold_Inventory_Lakehouse.inv.gold_item_group"
SRC = "Silver_BC_Lakehouse.bc.`Item`"

# 1) Create target table if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    No STRING,
    Description STRING,
    itemno_des STRING,
    item_group STRING,
    Item_Category_Code STRING,
    Gen_Prod_Posting_Group STRING,
    Inventory_Posting_Group STRING,
    item_group_count INT,
    sub_item_group STRING,

    -- tracking
    SystemCreatedAt TIMESTAMP,
    SystemModifiedAt TIMESTAMP
)
USING DELTA
""")

# 2) Read full source (no watermark / incremental filter)
src = spark.table(SRC)

no_col = F.col("`No.`")
dash_at_pos2 = (F.substring(no_col, 2, 1) == F.lit("-"))

# STUFF([No.], 2, 1, '000') -> replace char at pos2 with '000'
stuff_pos2_000 = F.concat(
    F.substring(no_col, 1, 1),
    F.lit("000"),
    F.substring(no_col, 3, 1000)
)

item_group = F.when(
    dash_at_pos2,
    F.substring(stuff_pos2_000, 1, 10)
).otherwise(
    F.substring(no_col, 1, 10)
)

has_dash_any = F.instr(no_col, "-") > 0
partition_key = F.when(
    has_dash_any,
    F.substring(F.concat(F.regexp_replace(no_col, "-", ""), F.lit("000")), 1, 10)
).otherwise(
    F.substring(no_col, 1, 10)
)

w = Window.partitionBy(partition_key).orderBy(no_col)

gold_full = src.select(
    F.col("`No.`").alias("No"),
    F.col("`Description`").alias("Description"),
    F.concat(F.col("`No.`"), F.col("`Description`")).alias("itemno_des"),
    item_group.alias("item_group"),
    F.col("`Item Category Code`").alias("Item_Category_Code"),
    F.col("`Gen. Prod. Posting Group`").alias("Gen_Prod_Posting_Group"),
    F.col("`Inventory Posting Group`").alias("Inventory_Posting_Group"),
    F.row_number().over(w).cast("int").alias("item_group_count"),
    F.lit("sub-item group").alias("sub_item_group"),
    F.col("`SystemCreatedAt`").alias("SystemCreatedAt"),
    F.col("`SystemModifiedAt`").alias("SystemModifiedAt"),
)

# 3) FULL RELOAD: overwrite target table
(
    gold_full
    .write
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

# # Gold Posted Credit Memo

# CELL ********************

# Spark (Delta) incremental load -> gold_posted_credit_memo
# Source:
#   cn  = Silver_BC_Lakehouse.bc.`Sales CrMemo Header`
#   cnl = Silver_BC_Lakehouse.bc.`Sales CrMemo Line`
# Join:
#   cn.`No.` = cnl.`Document No.`  (LEFT JOIN)
# Increment:
#   by header SystemModifiedAt (fallback SystemCreatedAt)
#   rebuild all lines for impacted documents, then MERGE by (Document_No, Line_No)

from pyspark.sql import functions as F

TARGET = "Gold_Customer_Exp_Lakehouse.cx.gold_posted_credit_memo"
CN_SRC = "Silver_BC_Lakehouse.bc.`Sales CrMemo Header`"
CNL_SRC = "Silver_BC_Lakehouse.bc.`Sales CrMemo Line`"

# 1) Create target table if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    Sell_to_Customer_No STRING,
    Posting_Date DATE,
    Document_Date DATE,
    Sales_person_Code STRING,
    Currency_Code STRING,
    Shortcut_Dimension1_Code STRING,
    Shortcut_Dimension2_Code STRING,

    Document_No STRING,
    Line_No INT,
    DocLine STRING,

    item STRING,
    Description STRING,
    qty DECIMAL(38,10),
    uom STRING,
    Unit_Cost_LCY DECIMAL(38,10),
    Unit_Price DECIMAL(38,10),
    Line_Amount DECIMAL(38,10),

    TypeofFG STRING,
    GenProd STRING,

    -- tracking for incremental + merge keys
    CN_No STRING,
    CNL_DocumentNo STRING,
    CNL_LineNo INT,
    CN_SystemCreatedAt TIMESTAMP,
    CN_SystemModifiedAt TIMESTAMP,
    CNL_SystemCreatedAt TIMESTAMP,
    CNL_SystemModifiedAt TIMESTAMP
)
USING DELTA
""")

# 2) Watermark from target (first run -> 1900-01-01)
wm = (
    spark.table(TARGET)
    .select(F.max(F.col("CN_SystemModifiedAt")).alias("wm"))
    .collect()[0]["wm"]
)
if wm is None:
    wm = "1900-01-01 00:00:00"

# 3) Read sources
cn = spark.table(CN_SRC).alias("cn")
cnl = spark.table(CNL_SRC).alias("cnl")

# 4) Find impacted credit memo documents from header changes
cn_imp = (
    cn.withColumn("wm_ts", F.coalesce(F.col("SystemModifiedAt"), F.col("SystemCreatedAt")))
      .filter(F.col("wm_ts") > F.to_timestamp(F.lit(str(wm))))
      .select(F.col("`No.`").alias("CN_No"))
      .distinct()
      .alias("imp")
)

# 5) Rebuild rows for impacted documents (LEFT JOIN)
cn_f = cn.join(cn_imp, on=(F.col("cn.`No.`") == F.col("imp.CN_No")), how="inner").alias("cn_f")
cnl_f = cnl.join(cn_imp, on=(F.col("cnl.`Document No.`") == F.col("imp.CN_No")), how="inner").alias("cnl_f")

joined = cn_f.join(
    cnl_f,
    on=(F.col("cn_f.`No.`") == F.col("cnl_f.`Document No.`")),
    how="left"
)

d38 = lambda col_: F.col(col_).cast("decimal(38,10)")

gold_inc = joined.select(
    F.col("cn_f.`Sell-to Customer No.`").alias("Sell_to_Customer_No"),
    F.col("cn_f.`Posting Date`").cast("date").alias("Posting_Date"),
    F.col("cn_f.`Document Date`").cast("date").alias("Document_Date"),
    F.col("cn_f.`Salesperson Code`").alias("Sales_person_Code"),
    F.col("cn_f.`Currency Code`").alias("Currency_Code"),
    F.col("cn_f.`Shortcut Dimension 1 Code`").alias("Shortcut_Dimension1_Code"),
    F.col("cn_f.`Shortcut Dimension 2 Code`").alias("Shortcut_Dimension2_Code"),

    F.col("cnl_f.`Document No.`").alias("Document_No"),
    F.col("cnl_f.`Line No.`").cast("int").alias("Line_No"),
    F.concat(F.col("cnl_f.`Document No.`"), F.col("cnl_f.`Line No.`").cast("string")).alias("DocLine"),

    F.col("cnl_f.`No.`").alias("item"),
    F.col("cnl_f.`Description`").alias("Description"),
    d38("cnl_f.`Quantity`").alias("qty"),
    F.col("cnl_f.`Unit of Measure Code`").alias("uom"),
    d38("cnl_f.`Unit Cost (LCY)`").alias("Unit_Cost_LCY"),
    d38("cnl_f.`Unit Price`").alias("Unit_Price"),
    d38("cnl_f.`Line Amount`").alias("Line_Amount"),

    F.col("cnl_f.`Shortcut Dimension 2 Code`").alias("TypeofFG"),
    F.col("cnl_f.`Gen. Prod. Posting Group`").alias("GenProd"),

    # tracking
    F.col("cn_f.`No.`").alias("CN_No"),
    F.col("cnl_f.`Document No.`").alias("CNL_DocumentNo"),
    F.col("cnl_f.`Line No.`").cast("int").alias("CNL_LineNo"),
    F.col("cn_f.`SystemCreatedAt`").alias("CN_SystemCreatedAt"),
    F.col("cn_f.`SystemModifiedAt`").alias("CN_SystemModifiedAt"),
    F.col("cnl_f.`SystemCreatedAt`").alias("CNL_SystemCreatedAt"),
    F.col("cnl_f.`SystemModifiedAt`").alias("CNL_SystemModifiedAt"),
)

gold_inc.createOrReplaceTempView("gold_posted_credit_memo_inc")

# 6) MERGE into target
# Key: Document_No + Line_No (line can be null if header has no lines; handle null-safe match)
spark.sql(f"""
MERGE INTO {TARGET} AS t
USING gold_posted_credit_memo_inc AS s
ON  t.Document_No = s.Document_No
AND (
      (t.Line_No = s.Line_No)
   OR (t.Line_No IS NULL AND s.Line_No IS NULL)
)
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Posted Credit Memo Amount

# CELL ********************

# Spark (Delta) incremental load -> gold_posted_credit_memo_amount
# Source:
#   cn  = Gold_Customer_Exp_Lakehouse.cx.gold_posted_credit_memo
#   cur = Silver_BC_Lakehouse.bc.`Currency Exchange Rate`
# Join:
#   cn.Posting_Date = cur.`Starting Date`
#   cn.Currency_Code = cur.`Currency Code`
#   cur.`Relational Currency Code` = 'THB'
# Increment:
#   driven by CN_SystemModifiedAt from cn (fallback CN_SystemCreatedAt)

from pyspark.sql import functions as F

TARGET = "Gold_Customer_Exp_Lakehouse.cx.gold_posted_credit_memo_amount"
CN_SRC  = "Gold_Customer_Exp_Lakehouse.cx.gold_posted_credit_memo"
CUR_SRC = "Silver_BC_Lakehouse.bc.`Currency Exchange Rate`"

# 1) Create target table if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    Sell_to_Customer_No STRING,
    Posting_Date DATE,
    Document_Date DATE,
    Sales_person_Code STRING,
    Currency_Code STRING,
    Shortcut_Dimension1_Code STRING,
    Shortcut_Dimension2_Code STRING,
    Document_No STRING,
    Line_No INT,
    DocLine STRING,
    item STRING,
    Description STRING,
    qty DECIMAL(38,10),
    uom STRING,
    Unit_Cost_LCY DECIMAL(38,10),
    Unit_Price DECIMAL(38,10),
    Line_Amount DECIMAL(38,10),
    TypeofFG STRING,
    GenProd STRING,

    relationalExchRateAmount DECIMAL(38,10),
    amountTHB DECIMAL(38,10),
    amountqtyTHB DECIMAL(38,10),

    -- tracking passthrough for incremental
    CN_SystemCreatedAt TIMESTAMP,
    CN_SystemModifiedAt TIMESTAMP
)
USING DELTA
""")

# 2) Watermark from target (first run -> 1900-01-01)
wm_row = spark.table(TARGET).select(F.max(F.col("CN_SystemModifiedAt")).alias("wm")).collect()[0]
wm = wm_row["wm"] if wm_row and wm_row["wm"] is not None else "1900-01-01 00:00:00"

# 3) Read sources
cn  = spark.table(CN_SRC).alias("cn")
cur = spark.table(CUR_SRC).alias("cur")

# 4) Incremental rows from cn based on header modified/created time
cn_inc = (
    cn.withColumn("wm_ts", F.coalesce(F.col("CN_SystemModifiedAt"), F.col("CN_SystemCreatedAt")))
      .filter(F.col("wm_ts") > F.to_timestamp(F.lit(str(wm))))
      .alias("cn_inc")
)

# 5) Join currency rate (LEFT JOIN, THB only)
joined = (
    cn_inc.join(
        cur,
        on=(
            (F.col("cn_inc.Posting_Date").cast("date") == F.col("cur.`Starting Date`").cast("date")) &
            (F.col("cn_inc.Currency_Code") == F.col("cur.`Currency Code`")) 
            # (F.col("cur.`Relational Currency Code`") == F.lit("THB"))
        ),
        how="left"
    )
)

# 6) Rate logic
# SQL: ISNULL(NULLIF(cur.[Relational Exch. Rate Amount], 0), 1)
# FIX: Avoid F.nullif (can error with unresolved datatype). Use WHEN/OTHERWISE instead.
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

gold_inc = joined.select(
    F.col("cn_inc.Sell_to_Customer_No").alias("Sell_to_Customer_No"),
    F.col("cn_inc.Posting_Date").cast("date").alias("Posting_Date"),
    F.col("cn_inc.Document_Date").cast("date").alias("Document_Date"),
    F.col("cn_inc.Sales_person_Code").alias("Sales_person_Code"),
    F.col("cn_inc.Currency_Code").alias("Currency_Code"),
    F.col("cn_inc.Shortcut_Dimension1_Code").alias("Shortcut_Dimension1_Code"),
    F.col("cn_inc.Shortcut_Dimension2_Code").alias("Shortcut_Dimension2_Code"),
    F.col("cn_inc.Document_No").alias("Document_No"),
    F.col("cn_inc.Line_No").cast("int").alias("Line_No"),
    F.col("cn_inc.DocLine").alias("DocLine"),
    F.col("cn_inc.item").alias("item"),
    F.col("cn_inc.Description").alias("Description"),
    d38("cn_inc.qty").alias("qty"),
    F.col("cn_inc.uom").alias("uom"),
    d38("cn_inc.Unit_Cost_LCY").alias("Unit_Cost_LCY"),
    d38("cn_inc.Unit_Price").alias("Unit_Price"),
    d38("cn_inc.Line_Amount").alias("Line_Amount"),
    F.col("cn_inc.TypeofFG").alias("TypeofFG"),
    F.col("cn_inc.GenProd").alias("GenProd"),

    rate.alias("relationalExchRateAmount"),

    # cn.Unit_Price * rate
    (d38("cn_inc.Unit_Price") * rate).alias("amountTHB"),

    # cn.qty * (cn.Unit_Price * rate)
    (d38("cn_inc.qty") * (d38("cn_inc.Unit_Price") * rate)).alias("amountqtyTHB"),

    # tracking passthrough
    F.col("cn_inc.CN_SystemCreatedAt").alias("CN_SystemCreatedAt"),
    F.col("cn_inc.CN_SystemModifiedAt").alias("CN_SystemModifiedAt"),
)

gold_inc.createOrReplaceTempView("gold_posted_credit_memo_amount_inc")

# 7) MERGE into target
# Key: Document_No + Line_No
spark.sql(f"""
MERGE INTO {TARGET} AS t
USING gold_posted_credit_memo_amount_inc AS s
ON  t.Document_No = s.Document_No
AND (
      (t.Line_No = s.Line_No)
   OR (t.Line_No IS NULL AND s.Line_No IS NULL)
)
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Output FG

# CELL ********************

# Spark (Delta) FULL REPLACE -> gold_output_FG
# Source:
#   i = Silver_BC_Lakehouse.bc.`Item Ledger Entry`
#   p = Silver_BC_Lakehouse.bc.`Production Order`
# Join:
#   i.`Document No.` = p.`No.`  (LEFT JOIN)
# Filter:
#   i.`Entry Type` = 'Output'
#   i.`Location Code` = 'FIN-GOODS'
# Strategy:
#   rebuild full dataset then OVERWRITE target delta table

from pyspark.sql import functions as F

TARGET = "Gold_Customer_Exp_Lakehouse.cx.gold_output_FG"
ILE_SRC = "Silver_BC_Lakehouse.bc.`Item Ledger Entry`"
PO_SRC  = "Silver_BC_Lakehouse.bc.`Production Order`"

d38 = lambda c: F.col(c).cast("decimal(38,10)")

# 1) Read sources
ile = spark.table(ILE_SRC).alias("i")
po  = spark.table(PO_SRC).alias("p")

# 2) Filter Item Ledger Entry rows
ile_f = (
    ile.filter(F.col("`Entry Type`") == F.lit("Output"))
       .filter(F.col("`Location Code`") == F.lit("FIN-GOODS"))
       .alias("i_f")
)

# 3) Join to Production Order (LEFT JOIN)
joined = ile_f.join(
    po,
    on=(F.col("i_f.`Document No.`") == F.col("p.`No.`")),
    how="left"
)

# 4) Select gold shape
gold_full = joined.select(
    F.col("i_f.`Posting Date`").cast("date").alias("PostingDate"),
    F.col("i_f.`Document No.`").alias("Docno"),
    F.col("i_f.`Source No.`").alias("Item_No"),
    F.col("i_f.`Description`").alias("Description"),
    F.col("i_f.`Location Code`").alias("location_code"),
    d38("i_f.`Quantity`").alias("Quantity"),
    F.col("i_f.`Unit of Measure Code`").alias("UOM"),
    F.col("p.`Sales Order No.`").alias("sales_order"),
    F.col("p.`Sales Order Line No.`").cast("int").alias("sales_order_line_no"),

    # tracking
    F.col("i_f.`Document No.`").alias("ILE_DocumentNo"),
    F.col("i_f.`Source No.`").alias("ILE_ItemNo"),
    F.col("i_f.`Posting Date`").cast("date").alias("ILE_PostingDate"),
    F.col("i_f.`SystemCreatedAt`").alias("ILE_SystemCreatedAt"),
    F.col("i_f.`SystemModifiedAt`").alias("ILE_SystemModifiedAt"),
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

# # Gold Out FG

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

TARGET = "Gold_Customer_Exp_Lakehouse.cx.gold_out_FG"
OP_SRC = "Gold_Customer_Exp_Lakehouse.cx.gold_output_FG"
SC_SRC = "Gold_Customer_Exp_Lakehouse.cx.gold_sales_order_shipment_fact_currency"

op = spark.table(OP_SRC)
sc = spark.table(SC_SRC) 

def table_exists(full_name: str) -> bool:
    # works for 3-part names in most Spark environments
    try:
        return spark.catalog.tableExists(full_name)
    except Exception:
        # fallback: SHOW TABLES (safer when catalog.tableExists is finicky)
        cat, sch, tbl = full_name.split(".")
        return (
            spark.sql(f"SHOW TABLES IN {cat}.{sch}")
                 .filter(F.col("tableName") == tbl)
                 .limit(1)
                 .count() > 0
        )

target_exists = table_exists(TARGET)

# --- watermark from target (only if exists) ---
if target_exists:
    wm = (
        spark.table(TARGET)
        .select(F.max(F.col("ILE_SystemModifiedAt")).alias("wm"))
        .collect()[0]["wm"]
    )
    wm = wm if wm is not None else "1900-01-01 00:00:00"
else:
    wm = "1900-01-01 00:00:00"

# --- incremental op rows ---
op_inc = (
    op.withColumn("wm_ts", F.coalesce(F.col("ILE_SystemModifiedAt"), F.col("ILE_SystemCreatedAt")))
      .filter(F.col("wm_ts") > F.to_timestamp(F.lit(str(wm))))
)

# impacted partitions (sales_order + line)
imp_parts = op_inc.select("sales_order", "sales_order_line_no").distinct()

# ✅ LEFT SEMI JOIN (no dup cols)
op_rb = (
    op.alias("op")
      .join(
          imp_parts.alias("imp"),
          on=(
              (F.col("op.sales_order") == F.col("imp.sales_order")) &
              (F.col("op.sales_order_line_no") == F.col("imp.sales_order_line_no"))
          ),
          how="left_semi"
      )
      .alias("op_rb")
)

d38 = lambda c: F.col(c).cast("decimal(38,10)")

w_sum = Window.partitionBy(F.col("op_rb.sales_order"), F.col("op_rb.sales_order_line_no"))
w_rn = Window.partitionBy(
    F.concat_ws("_",
        F.col("op_rb.sales_order"),
        F.col("op_rb.sales_order_line_no").cast("string"),
        F.col("op_rb.Item_No")
    )
).orderBy(F.col("op_rb.PostingDate").asc_nulls_last())

joined = (
    op_rb.alias("op_rb")
        .join(
            sc.alias("sc"),
            on=(
                (F.col("sc.SalesorderNo") == F.col("op_rb.sales_order")) &
                (F.col("sc.linenoo").cast("int") == F.col("op_rb.sales_order_line_no").cast("int"))
            ),
            how="left"
        )
)

gold_inc = (
    joined
    .withColumn("qtyFG", F.sum(d38("op_rb.Quantity")).over(w_sum))
    .withColumn("RowNum", F.row_number().over(w_rn))
    .select(
        # ✅ ADDED PostingDate
        F.col("op_rb.PostingDate").cast("date").alias("PostingDate"),

        F.col("op_rb.Docno").alias("Docno"),
        F.col("op_rb.Item_No").alias("Itemno"),
        F.col("op_rb.UOM").alias("uom"),
        F.col("qtyFG").alias("qtyFG"),
        F.col("RowNum").cast("int").alias("RowNum"),
        F.col("op_rb.sales_order").alias("sales_order"),
        F.col("op_rb.sales_order_line_no").cast("int").alias("sales_order_line_no"),
        F.concat(F.col("op_rb.Docno"), F.col("op_rb.Item_No")).alias("SOI"),
        F.concat(F.col("op_rb.sales_order"), F.col("op_rb.sales_order_line_no").cast("string")).alias("sol"),

        F.col("sc.CusName").alias("CusName"),
        d38("sc.Totalqty").alias("Totalqty"),
        d38("sc.QtyShip").alias("QtyShip"),
        d38("sc.QtyINV").alias("QtyINV"),
        d38("sc.UnitPrice").alias("UnitPrice"),
        d38("sc.AmountTHB").alias("AmountTHB"),
        F.col("sc.StatusSO").alias("StatusSO"),

        F.col("op_rb.ILE_SystemCreatedAt").alias("ILE_SystemCreatedAt"),
        F.col("op_rb.ILE_SystemModifiedAt").alias("ILE_SystemModifiedAt"),
    )
)

# --- create table on first run; otherwise merge ---
if not target_exists:
    # First load: create Delta table
    (gold_inc.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TARGET)
    )
else:
    gold_inc.createOrReplaceTempView("gold_out_FG_inc")

    spark.sql(f"""
    MERGE INTO {TARGET} AS t
    USING gold_out_FG_inc AS s
    ON  t.sales_order = s.sales_order
    AND t.sales_order_line_no = s.sales_order_line_no
    AND t.Itemno = s.Itemno
    AND t.RowNum = s.RowNum
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Out FG Sum

# CELL ********************

# Spark (Delta) incremental load -> gold_out_FG_amount_used (gold_out_FG_sum)
# FIX: PostingDate column does NOT exist in source gold_out_FG.
#      Derive Postingdate from Docno if Docno contains a date; otherwise fallback to ILE timestamps (so it won't be NULL).

from pyspark.sql import functions as F

TARGET = "Gold_Customer_Exp_Lakehouse.cx.gold_out_FG_sum"
SRC    = "Gold_Customer_Exp_Lakehouse.cx.gold_out_FG"

# 1) Create target table if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    Postingdate DATE,
    Itemno STRING,
    uom STRING,
    qtyFG DECIMAL(38,10),
    Totalqty DECIMAL(38,10),
    QtyShip DECIMAL(38,10),

    outputqty DECIMAL(38,10),
    outputvalue DECIMAL(38,10),
    outputqty_amount DECIMAL(38,10),

    AmountTHB DECIMAL(38,10),
    RowNum INT,
    sales_order STRING,
    sales_order_line_no INT,
    SOI STRING,
    sol STRING,
    CusName STRING,
    StatusSO STRING,
    QtyINV DECIMAL(38,10),

    -- tracking
    ILE_SystemCreatedAt TIMESTAMP,
    ILE_SystemModifiedAt TIMESTAMP
)
USING DELTA
""")

# 2) Watermark from target
wm_row = spark.table(TARGET).select(F.max(F.col("ILE_SystemModifiedAt")).alias("wm")).collect()[0]
wm = wm_row["wm"] if wm_row and wm_row["wm"] is not None else "1900-01-01 00:00:00"

src = spark.table(SRC)

# 3) Incremental rows from source + RowNum = 1
inc = (
    src.filter(F.col("RowNum") == 1)
       .withColumn("wm_ts", F.coalesce(F.col("ILE_SystemModifiedAt"), F.col("ILE_SystemCreatedAt")))
       .filter(F.col("wm_ts") > F.to_timestamp(F.lit(str(wm))))
)

d38 = lambda c: F.col(c).cast("decimal(38,10)")

totalqty = d38("Totalqty")
qtyship  = F.coalesce(d38("QtyShip"), F.lit(0).cast("decimal(38,10)"))
qtyfg    = d38("qtyFG")
amtthb   = F.coalesce(d38("AmountTHB"), F.lit(0).cast("decimal(38,10)"))

cond = (totalqty - qtyship) > F.lit(0).cast("decimal(38,10)")

# --- FIX: robust Postingdate derivation (avoid NULLs) ---
# Try multiple patterns from Docno; if no date found, fallback to ILE timestamps.
docno = F.col("Docno")

docno_date_candidates = F.coalesce(
    # If Docno is directly ISO-like: 2025-01-12
    F.to_date(docno),

    # If Docno contains ISO date inside text: "SO-2025-01-12-001"
    F.to_date(F.regexp_extract(docno, r'(\d{4}-\d{2}-\d{2})', 1)),

    # If Docno contains yyyymmdd: "20250112" or "SO20250112-001"
    F.to_date(F.regexp_extract(docno, r'(\d{8})', 1), "yyyyMMdd"),

    # If Docno contains dd/MM/yyyy: "12/01/2025"
    F.to_date(F.regexp_extract(docno, r'(\d{2}/\d{2}/\d{4})', 1), "dd/MM/yyyy"),
)

posting_date_expr = F.coalesce(
    docno_date_candidates,
    F.to_date(F.col("ILE_SystemModifiedAt")),
    F.to_date(F.col("ILE_SystemCreatedAt")),
    F.lit(None).cast("date")
)
# -----------------------------------------------

gold_inc = inc.select(
    posting_date_expr.alias("Postingdate"),
    F.col("Itemno").alias("Itemno"),
    F.col("uom").alias("uom"),
    qtyfg.alias("qtyFG"),
    totalqty.alias("Totalqty"),
    d38("QtyShip").alias("QtyShip"),

    F.when(cond, qtyfg - qtyship).otherwise(F.lit(None).cast("decimal(38,10)")).alias("outputqty"),

    # NOTE: matches your SQL exactly (even though formula looks unusual):
    # (Totalqty - qtyFG) - (QtyShip * AmountTHB)
    F.when(
        cond,
        (totalqty - qtyfg) - (qtyship * amtthb)
    ).otherwise(F.lit(None).cast("decimal(38,10)")).alias("outputvalue"),

    F.when(cond, (qtyfg - qtyship) * amtthb).otherwise(F.lit(None).cast("decimal(38,10)")).alias("outputqty_amount"),

    d38("AmountTHB").alias("AmountTHB"),
    F.col("RowNum").cast("int").alias("RowNum"),
    F.col("sales_order").alias("sales_order"),
    F.col("sales_order_line_no").cast("int").alias("sales_order_line_no"),
    F.col("SOI").alias("SOI"),
    F.col("sol").alias("sol"),
    F.col("CusName").alias("CusName"),
    F.col("StatusSO").alias("StatusSO"),
    d38("QtyINV").alias("QtyINV"),

    # tracking passthrough
    F.col("ILE_SystemCreatedAt").alias("ILE_SystemCreatedAt"),
    F.col("ILE_SystemModifiedAt").alias("ILE_SystemModifiedAt"),
)

gold_inc.createOrReplaceTempView("gold_out_FG_amount_used_inc")

# 4) MERGE into target
# Key: sales_order + sales_order_line_no + Itemno (RowNum is always 1 here)
spark.sql(f"""
MERGE INTO {TARGET} AS t
USING gold_out_FG_amount_used_inc AS s
ON  t.sales_order = s.sales_order
AND t.sales_order_line_no = s.sales_order_line_no
AND t.Itemno = s.Itemno
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Customer

# CELL ********************

from pyspark.sql import functions as F

TARGET = "Gold_Customer_Exp_Lakehouse.cx.gold_customer"
SRC = "Silver_BC_Lakehouse.bc.`Customer`"

# 1) Create target table if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    customer_no STRING,
    customer_name STRING,
    customer_abbr STRING,
    customer_budget DECIMAL(38,10),
    customer_currency STRING,
    cs_team STRING,
    customer_country STRING,
    cs_cad_mng STRING,
    sample_time DECIMAL(38,10),
    master_time DECIMAL(38,10),
    cad_time DECIMAL(38,10),

    -- tracking
    SystemCreatedAt TIMESTAMP,
    SystemModifiedAt TIMESTAMP
)
USING DELTA
""")

# 2) Watermark
wm = (
    spark.table(TARGET)
    .select(F.max(F.col("SystemModifiedAt")).alias("wm"))
    .collect()[0]["wm"]
)
if wm is None:
    wm = "1900-01-01 00:00:00"

# 3) Read source
src = spark.table(SRC)

# 4) Incremental filter + Blocked IS NULL
inc = (
    src
    .withColumn(
        "wm_ts",
        F.coalesce(F.col("`SystemModifiedAt`"), F.col("`SystemCreatedAt`"))
    )
    .filter(F.col("wm_ts") > F.to_timestamp(F.lit(str(wm))))
    .filter(F.col("`Blocked`").isNull())
)


d38 = lambda c: F.col(c).cast("decimal(38,10)")

# 5) Select with backticks on ALL source columns
gold_inc = inc.select(
    F.col("`No.`").alias("customer_no"),
    F.col("`Name`").alias("customer_name"),
    F.col("`DSVC Branch ID`").alias("customer_abbr"),
    d38("`Budgeted Amount`").alias("customer_budget"),
    F.col("`Currency Code`").alias("customer_currency"),
    F.col("`Salesperson Code`").alias("cs_team"),
    F.col("`Country/Region Code`").alias("customer_country"),
    F.col("`Responsibility Center`").alias("cs_cad_mng"),
    d38("`Sample STD.Time`").alias("sample_time"),
    d38("`Master STD.Time`").alias("master_time"),
    d38("`CAD STD.Time`").alias("cad_time"),

    # tracking passthrough
    F.col("`SystemCreatedAt`").alias("SystemCreatedAt"),
    F.col("`SystemModifiedAt`").alias("SystemModifiedAt"),
)

gold_inc.createOrReplaceTempView("gold_customer_inc")

# 6) MERGE
spark.sql(f"""
MERGE INTO {TARGET} AS t
USING gold_customer_inc AS s
ON  t.customer_no = s.customer_no
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold FG Cus

# CELL ********************

from pyspark.sql import functions as F

TARGET = "Gold_Customer_Exp_Lakehouse.cx.gold_FG_cus"

ITEM_SRC = "Silver_BC_Lakehouse.bc.`Item`"
DIM_SRC  = "Silver_BC_Lakehouse.bc.`Default Dimension`"
CUS_SRC  = "Silver_BC_Lakehouse.bc.`Customer`"

# 1) Create target table if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    ItemFG STRING,
    Description STRING,
    metal STRING,
    item_category STRING,
    product_type STRING,
    CusAbbr STRING,
    CusNo STRING,
    CusName STRING,

    -- tracking
    Item_SystemCreatedAt TIMESTAMP,
    Item_SystemModifiedAt TIMESTAMP
)
USING DELTA
""")

# 2) Watermark (incremental based on Item)
wm = (
    spark.table(TARGET)
    .select(F.max(F.col("Item_SystemModifiedAt")).alias("wm"))
    .collect()[0]["wm"]
)
if wm is None:
    wm = "1900-01-01 00:00:00"

# 3) Read sources
i = spark.table(ITEM_SRC).alias("i")
d = spark.table(DIM_SRC).alias("d")
c = spark.table(CUS_SRC).alias("c")

# 4) Incremental Items
i_inc = (
    i.withColumn(
        "wm_ts",
        F.coalesce(F.col("`SystemModifiedAt`"), F.col("`SystemCreatedAt`"))
    )
    .filter(F.col("wm_ts") > F.to_timestamp(F.lit(str(wm))))
    .alias("i_inc")
)

# 5) Join (exactly matching your SQL logic)
joined = (
    i_inc
    .join(
        d,
        on=(F.col("i_inc.`No.`") == F.col("d.`No.`")),
        how="left"
    )
    .join(
        c,
        on=(F.col("d.`Dimension Value Code`") == F.col("c.`DSVC Branch ID`")),
        how="inner"
    )
    .filter(F.col("d.`Dimension Code`") == F.lit("CUSTOMER NAME"))
)

# 6) Select with backticks
gold_inc = joined.select(
    F.col("i_inc.`No.`").alias("ItemFG"),
    F.col("i_inc.`Description`").alias("Description"),
    F.col("i_inc.`Global Dimension 2 Code`").alias("metal"),
    F.col("i_inc.`Item Category Code`").alias("item_category"),
    F.col("i_inc.`Product Type`").alias("product_type"),
    F.col("d.`Dimension Value Code`").alias("CusAbbr"),
    F.col("c.`No.`").alias("CusNo"),
    F.col("c.`Name`").alias("CusName"),

    # tracking
    F.col("i_inc.`SystemCreatedAt`").alias("Item_SystemCreatedAt"),
    F.col("i_inc.`SystemModifiedAt`").alias("Item_SystemModifiedAt"),
)

gold_inc.createOrReplaceTempView("gold_Fg_cus_inc")

# 7) MERGE into target
# Key: ItemFG + CusAbbr (one FG can map to one customer branch)
spark.sql(f"""
MERGE INTO {TARGET} AS t
USING gold_Fg_cus_inc AS s
ON  t.ItemFG = s.ItemFG
AND t.CusAbbr = s.CusAbbr
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Inventory FG

# CELL ********************

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# ✅ CHANGE THIS to whatever gold table name you want
TGT_TBL = "Gold_Customer_Exp_Lakehouse.cx.gold_inventory_FG_amount"

src_sql = """
WITH stock AS (
    SELECT
        l.`Item No.`               AS item_no,
        MAX(l.`Description`)       AS item_description,
        l.`Location Code`          AS entry_type_item_location,
        l.`Unit of Measure Code`   AS item_uom,
        SUM(l.`Remaining Quantity`) AS item_lot_remaining_quantity
    FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` AS l
    INNER JOIN Silver_BC_Lakehouse.bc.`Item` AS i
        ON l.`Item No.` = i.`No.`
    WHERE l.`Open` = '1'
      AND i.`Item Category Code` = 'FG'
    GROUP BY
        l.`Item No.`,
        l.`Location Code`,
        l.`Unit of Measure Code`
),
item_dim AS (
    SELECT
        i.`No.` AS item_no,
        d.`Dimension Value Code` AS CusAbbr,
        c.`No.` AS CusNo,
        c.`Name` AS CusName,
        i.`Unit Cost` AS `Unit Cost`
    FROM Silver_BC_Lakehouse.bc.`Item` AS i
    LEFT JOIN Silver_BC_Lakehouse.bc.`Default Dimension` AS d
        ON i.`No.` = d.`No.`
       AND d.`Dimension Code` = 'CUSTOMER NAME'
    LEFT JOIN Silver_BC_Lakehouse.bc.`Customer` AS c
        ON d.`Dimension Value Code` = c.`DSVC Branch ID`
    WHERE i.`Item Category Code` = 'FG'
),
so AS (
    SELECT
        s.`ItemFG`         AS item_no,
        s.`SalesorderNo`,
        s.`StatusSO`,
        SUM(s.`Outstanding`) AS Outstanding
    FROM Gold_Customer_Exp_Lakehouse.cx.`gold_sales_order_shipment_fact_currency` AS s
    WHERE s.`StatusSO` <> 'Closed'
    GROUP BY
        s.`ItemFG`,
        s.`SalesorderNo`,
        s.`StatusSO`
    HAVING SUM(s.`Outstanding`) > 0
),
final AS (
    SELECT
        st.item_no,
        st.item_description,
        st.entry_type_item_location,
        st.item_uom,
        st.item_lot_remaining_quantity,
        d.CusAbbr,
        d.CusNo,
        d.CusName,
        CONCAT(st.item_no, ' - ', st.item_description) AS FGdes,
        d.`Unit Cost` AS Unit_Cost,
        so.Outstanding,
        so.SalesorderNo,
        so.StatusSO
    FROM stock st
    LEFT JOIN item_dim d
        ON st.item_no = d.item_no
    LEFT JOIN so
        ON st.item_no = so.item_no
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY item_no, entry_type_item_location, SalesorderNo
            ORDER BY item_lot_remaining_quantity DESC
        ) AS RowNum
    FROM final
)
SELECT *
FROM ranked
"""

df_src = spark.sql(src_sql)

# FULL REFRESH write
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
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # Gold Summary Production Status Amount

# CELL ********************

from pyspark.sql import functions as F

TARGET = "Gold_Customer_Exp_Lakehouse.cx.gold_summary_production_status_amount"

P_SRC = "Gold_Production_Lakehouse.prod.gold_summary_production_status"
S_SRC = "Gold_Customer_Exp_Lakehouse.cx.gold_sales_order_shipment_fact_currency"

# ---------- helpers ----------
d38 = lambda c: F.col(c).cast("decimal(38,10)")

# ---------- 1) Create target table if not exists ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    prod_order_due_date DATE,
    prod_order_status STRING,
    prod_order_no STRING,
    prod_order_description STRING,
    FG_item_no STRING,
    item_routing_no STRING,
    prod_order_quantity DECIMAL(38,10),
    prod_order_type STRING,
    sales_order_no STRING,
    sales_order_line_no INT,
    prod_order_location STRING,
    prod_order_starting_date_time TIMESTAMP,
    prod_order_ending_date_time TIMESTAMP,
    prod_order_finished_date DATE,
    prod_line_quantity DECIMAL(38,10),
    prod_line_finished_quantity DECIMAL(38,10),
    prod_line_remaining_quantity DECIMAL(38,10),
    CusNo STRING,
    CusName STRING,
    CusAbbr STRING,
    so_abbr STRING,
    so_type STRING,
    TypeofFG STRING,
    Total_QTY DECIMAL(38,10),
    OutstandingQty DECIMAL(38,10),
    QtytoShip DECIMAL(38,10),
    QtyShipped DECIMAL(38,10),
    AmountTHB DECIMAL(38,10),
    Finish_Price DECIMAL(38,10)
)
USING DELTA
""")

# ---------- 2) Read sources ----------
p = spark.table(P_SRC).alias("p")
s = spark.table(S_SRC).alias("s")

# ---------- 3) Full join + projection ----------
gold_full = (
    p.join(
        s,
        on=(
            (F.col("p.sales_order_no") == F.col("s.SalesorderNo")) &
            (F.col("p.sales_order_line_no") == F.col("s.linenoo"))
        ),
        how="left"
    )
    .select(
        F.col("p.prod_order_due_date").alias("prod_order_due_date"),
        F.col("p.prod_order_status").alias("prod_order_status"),
        F.col("p.prod_order_no").alias("prod_order_no"),
        F.col("p.prod_order_description").alias("prod_order_description"),
        F.col("p.FG_item_no").alias("FG_item_no"),
        F.col("p.item_routing_no").alias("item_routing_no"),
        d38("p.prod_order_quantity").alias("prod_order_quantity"),
        F.col("p.prod_order_type").alias("prod_order_type"),
        F.col("p.sales_order_no").alias("sales_order_no"),
        F.col("p.sales_order_line_no").cast("int").alias("sales_order_line_no"),
        F.col("p.prod_order_location").alias("prod_order_location"),
        F.col("p.prod_order_starting_date_time").alias("prod_order_starting_date_time"),
        F.col("p.prod_order_ending_date_time").alias("prod_order_ending_date_time"),
        F.col("p.prod_order_finished_date").alias("prod_order_finished_date"),
        d38("p.prod_line_quantity").alias("prod_line_quantity"),
        d38("p.prod_line_finished_quantity").alias("prod_line_finished_quantity"),
        d38("p.prod_line_remaining_quantity").alias("prod_line_remaining_quantity"),
        F.col("p.CusNo").alias("CusNo"),
        F.col("p.CusName").alias("CusName"),
        F.col("p.CusAbbr").alias("CusAbbr"),
        F.col("p.so_abbr").alias("so_abbr"),
        F.col("p.so_type").alias("so_type"),
        F.col("p.TypeofFG").alias("TypeofFG"),
        d38("p.Total_QTY").alias("Total_QTY"),
        d38("p.OutstandingQty").alias("OutstandingQty"),
        d38("p.QtytoShip").alias("QtytoShip"),
        d38("p.QtyShipped").alias("QtyShipped"),
        d38("s.AmountTHB").alias("AmountTHB"),
        (d38("p.prod_line_remaining_quantity") * d38("s.AmountTHB")).alias("Finish_Price"),
    )
)

# ---------- 4) Full replace write ----------
# Overwrite the Delta table contents (keeps table definition)
(
    gold_full.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET)
)

# Optional: maintenance
# spark.sql(f"OPTIMIZE {TARGET}")
spark.sql(f"VACUUM {TARGET} RETAIN 168 HOURS")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Stock FG Cus

# CELL ********************

# Spark (Delta) incremental load -> gold_stockfg_cus (FIXED Customers aggregation)
# Fix: remove array_union/array_remove on aggregate Columns (causes NOT_ITERABLE).
# Use collect_set on a single "name_candidate" expression, then concat_ws; fallback to 'N/A' if empty.

from pyspark.sql import functions as F

TARGET = "Gold_Customer_Exp_Lakehouse.cx.gold_stockfg_cus"

GL_SRC = "Silver_BC_Lakehouse.bc.`GL Entry`"
C_SRC  = "Silver_BC_Lakehouse.bc.`Customer`"
AR_SRC = "Silver_Commons_Lakehouse.cmn.silver_Old_AR_SAP"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    G_L_Account_No STRING,
    Document_No STRING,
    Source_No STRING,
    GL_Date DATE,
    TotalGLAmount DECIMAL(38,10),
    Customers STRING,
    TotaloldAmount DECIMAL(38,10)
)
USING DELTA
""")

wm_date = (
    spark.table(TARGET)
    .select(F.max(F.col("GL_Date")).alias("wm"))
    .collect()[0]["wm"]
)
if wm_date is None:
    wm_date = "1900-01-01"

LOOKBACK_DAYS = 30
start_date = F.date_sub(F.to_date(F.lit(str(wm_date))), LOOKBACK_DAYS)

gl = spark.table(GL_SRC).alias("QGL40")
c  = spark.table(C_SRC).alias("C")
ar = spark.table(AR_SRC).alias("oldAR")

gl_p = (
    gl.filter(F.col("`G/L Account No.`").like("4%"))
      .withColumn("gl_post_date", F.to_date(F.col("`Posting Date`")))
      .select(
          F.col("`G/L Account No.`").cast("string").alias("gl_acc"),
          F.col("`Document No.`").alias("gl_doc"),
          F.col("`Source No.`").alias("gl_src"),
          F.col("`Amount`").cast("decimal(38,10)").alias("gl_amt"),
          F.col("gl_post_date").alias("gl_post_date")
      )
      .alias("gl_p")
)

c_p = (
    c.select(
        F.col("`No.`").alias("cus_no"),
        F.col("`Name`").alias("cus_name")
    )
    .alias("c_p")
)

gl_c = (
    gl_p.join(c_p, on=(F.col("gl_p.gl_src") == F.col("c_p.cus_no")), how="left")
        .alias("gl_c")
)

ar_p = (
    ar.withColumn("ar_date", F.to_date(F.col("posting_date")))
      .select(
          F.col("ar_date").alias("ar_date"),
          F.col("customer_name").alias("ar_customer_name"),
          F.col("AR_amount").cast("decimal(38,10)").alias("AR_amount")
      )
      .alias("ar_p")
)

gl_win = gl_c.filter(F.col("gl_post_date") >= start_date).alias("gl_win")
ar_win = ar_p.filter(F.col("ar_date") >= start_date).alias("ar_win")

j = gl_win.join(
    ar_win,
    on=(F.col("gl_win.gl_post_date") == F.col("ar_win.ar_date")),
    how="full"
).alias("j")

GL_Date = F.coalesce(F.col("j.gl_post_date"), F.col("j.ar_date"))

acc_set = [
    "40110","40120","40130","40140",
    "40210","40220","40230","40240",
    "40310","40320","40330","40340",
    "40410",
    "40510","40520",
    "40540",
]

TotalGLAmount = F.sum(
    F.when(F.col("j.gl_acc").isin(acc_set), -F.col("j.gl_amt"))
     .otherwise(F.lit(0).cast("decimal(38,10)"))
)

TotaloldAmount = F.sum(
    F.coalesce(F.col("j.AR_amount"), F.lit(0).cast("decimal(38,10)"))
)

# ✅ FIXED Customers aggregation
valid_src = (
    (F.col("j.gl_src").isNotNull()) &
    (F.length(F.trim(F.col("j.gl_src"))) > 0) &
    (F.col("j.cus_name").isNotNull())
)

# Prefer Customer name when valid source, otherwise use oldAR name (can be null)
name_candidate = F.when(valid_src, F.col("j.cus_name")).otherwise(F.col("j.ar_customer_name"))

names_set = F.collect_set(name_candidate)

Customers = F.when(
    F.size(names_set) > 0,
    F.concat_ws(", ", F.array_sort(names_set))
).otherwise(F.lit("N/A"))

agg = (
    j.groupBy(
        GL_Date.alias("GL_Date"),
        F.col("j.gl_acc").alias("G_L_Account_No"),
        F.col("j.gl_doc").alias("Document_No"),
        F.col("j.gl_src").alias("Source_No"),
    )
    .agg(
        TotalGLAmount.cast("decimal(38,10)").alias("TotalGLAmount"),
        Customers.alias("Customers"),
        TotaloldAmount.cast("decimal(38,10)").alias("TotaloldAmount"),
    )
)

agg.createOrReplaceTempView("gold_gl40_oldar_customers_inc")

spark.sql(f"""
MERGE INTO {TARGET} AS t
USING gold_gl40_oldar_customers_inc AS s
ON  t.GL_Date = s.GL_Date
AND t.G_L_Account_No <=> s.G_L_Account_No
AND t.Document_No <=> s.Document_No
AND t.Source_No <=> s.Source_No
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Stock FG

# CELL ********************

# ============================================================
# FULL REFRESH (recommended): FG Inventory with customer dimension + amount
# Reason: aggregates over Item Ledger Entry (Open) + joins -> full refresh avoids stale results.
#
# Target table name (edit if you want):
#   Gold_Inventory_Lakehouse.inv.gold_stock_fg
# ============================================================

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

TGT_TBL = "Gold_Customer_Exp_Lakehouse.cx.gold_stock_fg"

src_sql = """
WITH stock AS (
    SELECT
        l.`Item No.`                 AS item_no,
        MAX(l.`Description`)         AS item_description,
        l.`Location Code`            AS entry_type_item_location,
        l.`Unit of Measure Code`     AS item_uom,
        SUM(l.`Remaining Quantity`)  AS inv_fg
    FROM Silver_BC_Lakehouse.bc.`Item Ledger Entry` l
    INNER JOIN Silver_BC_Lakehouse.bc.`Item` i
        ON l.`Item No.` = i.`No.`
    WHERE l.`Open` = '1'
      AND i.`Item Category Code` = 'FG'
    GROUP BY
        l.`Item No.`,
        l.`Location Code`,
        l.`Unit of Measure Code`
),
item_dim AS (
    -- Force 1 row per item_no
    SELECT
        i.`No.` AS item_no,
        MAX(d.`Dimension Value Code`) AS CusAbbr,
        MAX(c.`No.`) AS CusNo,
        MAX(c.`Name`) AS CusName,
        MAX(i.`Unit Cost`) AS inv_cost
    FROM Silver_BC_Lakehouse.bc.`Item` i
    LEFT JOIN Silver_BC_Lakehouse.bc.`Default Dimension` d
        ON i.`No.` = d.`No.`
       AND d.`Dimension Code` = 'CUSTOMER NAME'
    LEFT JOIN Silver_BC_Lakehouse.bc.`Customer` c
        ON d.`Dimension Value Code` = c.`DSVC Branch ID`
    WHERE i.`Item Category Code` = 'FG'
    GROUP BY i.`No.`
)
SELECT
    s.item_no,
    s.item_description,
    s.entry_type_item_location,
    s.item_uom,
    d.CusAbbr AS Customer,
    d.CusNo,
    d.CusName,
    CONCAT(s.item_no, ' - ', s.item_description) AS FGdes,

    s.inv_fg AS INV_FG,
    d.inv_cost AS INV_Cost,
    (s.inv_fg * d.inv_cost) AS INV_Amt
FROM stock s
LEFT JOIN item_dim d
    ON s.item_no = d.item_no
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

# MARKDOWN ********************

# # Gold Sales Price List

# CELL ********************

# MAGIC %%sql
# MAGIC -- Spark SQL (Delta) version
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Gold_Customer_Exp_Lakehouse.cx.gold_sales_price_list
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH
# MAGIC all_item_vendor AS (
# MAGIC     SELECT DISTINCT
# MAGIC         rl.`BC Company` AS bc_company,
# MAGIC         rl.`No.` AS item_no,
# MAGIC         rl.`Buy-from Vendor No.` AS vendor_no
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Purch Rcpt Line` rl
# MAGIC     WHERE rl.`Type` = 'Item'
# MAGIC 
# MAGIC     UNION
# MAGIC 
# MAGIC     SELECT DISTINCT
# MAGIC         iv.`BC Company` AS bc_company,
# MAGIC         iv.`Item No.` AS item_no,
# MAGIC         iv.`Vendor No.` AS vendor_no
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item Vendor` iv
# MAGIC 
# MAGIC     UNION
# MAGIC 
# MAGIC     SELECT DISTINCT
# MAGIC         pl.`BC Company` AS bc_company,
# MAGIC         pl.`No.` AS item_no,
# MAGIC         pl.`Buy-from Vendor No.` AS vendor_no
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Purchase Line` pl
# MAGIC     WHERE pl.`Type` = 'Item'
# MAGIC       AND pl.`Document Type` = 'Order'
# MAGIC       AND pl.`Outstanding Quantity` > 0
# MAGIC 
# MAGIC     UNION
# MAGIC 
# MAGIC     SELECT DISTINCT
# MAGIC         i.`BC Company` AS bc_company,
# MAGIC         i.`No.` AS item_no,
# MAGIC         i.`Vendor No.` AS vendor_no
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Item` i
# MAGIC     WHERE i.`Vendor No.` IS NOT NULL AND i.`Vendor No.` <> ''
# MAGIC 
# MAGIC     UNION
# MAGIC 
# MAGIC     SELECT DISTINCT
# MAGIC         pl.`BC Company` AS bc_company,
# MAGIC         pl.`Asset No.` AS item_no,
# MAGIC         pl.`Source No.` AS vendor_no
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Price List Line` pl
# MAGIC     WHERE pl.`Source Type` = 'Vendor'
# MAGIC       AND pl.`Asset Type` = 'Item'
# MAGIC       AND pl.`Status` = 'Active'
# MAGIC ),
# MAGIC receipt_summary AS (
# MAGIC     SELECT
# MAGIC         rl.`BC Company` AS bc_company,
# MAGIC         rl.`No.` AS item_no,
# MAGIC         rl.`Buy-from Vendor No.` AS vendor_no,
# MAGIC         MAX(rh.`Buy-from Vendor Name`) AS vendor_name,
# MAGIC         MAX(rl.`Vendor Item No.`) AS vendor_item_no,
# MAGIC         MAX(rl.`Unit of Measure Code`) AS unit_of_measure_code,
# MAGIC         COUNT(*) AS total_receipts,
# MAGIC         SUM(rl.`Quantity`) AS total_qty_received,
# MAGIC         MIN(rl.`Direct Unit Cost`) AS min_price,
# MAGIC         MAX(rl.`Direct Unit Cost`) AS max_price,
# MAGIC         AVG(rl.`Direct Unit Cost`) AS avg_price,
# MAGIC         SUM(rl.`Quantity` * rl.`Direct Unit Cost`) AS total_amount,
# MAGIC         MIN(rl.`Posting Date`) AS first_receipt_date,
# MAGIC         MAX(rl.`Posting Date`) AS last_receipt_date,
# MAGIC         MAX(rh.`Currency Code`) AS currency_code
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Purch Rcpt Line` rl
# MAGIC     JOIN Silver_BC_Lakehouse.bc.`Purch Rcpt Header` rh
# MAGIC       ON rl.`Document No.` = rh.`No.`
# MAGIC      AND rl.`BC Company` = rh.`BC Company`
# MAGIC     WHERE rl.`Type` = 'Item'
# MAGIC     GROUP BY rl.`BC Company`, rl.`No.`, rl.`Buy-from Vendor No.`
# MAGIC ),
# MAGIC latest_receipt AS (
# MAGIC     SELECT
# MAGIC         rl.`BC Company` AS bc_company,
# MAGIC         rl.`No.` AS item_no,
# MAGIC         rl.`Buy-from Vendor No.` AS vendor_no,
# MAGIC         rl.`Direct Unit Cost` AS latest_price,
# MAGIC         rl.`Posting Date` AS latest_receipt_date,
# MAGIC         rl.`Document No.` AS latest_receipt_no,
# MAGIC         rl.`Quantity` AS latest_qty,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY rl.`BC Company`, rl.`No.`, rl.`Buy-from Vendor No.`
# MAGIC             ORDER BY rl.`Posting Date` DESC, rl.`Document No.` DESC
# MAGIC         ) AS rn
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Purch Rcpt Line` rl
# MAGIC     WHERE rl.`Type` = 'Item'
# MAGIC ),
# MAGIC previous_receipt AS (
# MAGIC     SELECT
# MAGIC         rl.`BC Company` AS bc_company,
# MAGIC         rl.`No.` AS item_no,
# MAGIC         rl.`Buy-from Vendor No.` AS vendor_no,
# MAGIC         rl.`Direct Unit Cost` AS direct_unit_cost,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY rl.`BC Company`, rl.`No.`, rl.`Buy-from Vendor No.`
# MAGIC             ORDER BY rl.`Posting Date` DESC, rl.`Document No.` DESC
# MAGIC         ) AS rn
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Purch Rcpt Line` rl
# MAGIC     WHERE rl.`Type` = 'Item'
# MAGIC ),
# MAGIC outstanding_po AS (
# MAGIC     SELECT
# MAGIC         pl.`BC Company` AS bc_company,
# MAGIC         pl.`No.` AS item_no,
# MAGIC         pl.`Buy-from Vendor No.` AS vendor_no,
# MAGIC         COUNT(DISTINCT pl.`Document No.`) AS open_po_count,
# MAGIC         SUM(pl.`Outstanding Quantity`) AS outstanding_qty,
# MAGIC         SUM(pl.`Outstanding Quantity` * pl.`Direct Unit Cost`) AS outstanding_amount,
# MAGIC         MIN(pl.`Expected Receipt Date`) AS earliest_expected_date
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Purchase Line` pl
# MAGIC     JOIN Silver_BC_Lakehouse.bc.`Purchase Header` ph
# MAGIC       ON pl.`Document No.` = ph.`No.`
# MAGIC      AND pl.`Document Type` = ph.`Document Type`
# MAGIC      AND pl.`BC Company` = ph.`BC Company`
# MAGIC     WHERE pl.`Document Type` = 'Order'
# MAGIC       AND pl.`Type` = 'Item'
# MAGIC       AND pl.`Outstanding Quantity` > 0
# MAGIC     GROUP BY pl.`BC Company`, pl.`No.`, pl.`Buy-from Vendor No.`
# MAGIC ),
# MAGIC vendor_price_list AS (
# MAGIC     SELECT
# MAGIC         pl.`BC Company` AS bc_company,
# MAGIC         pl.`Asset No.` AS item_no,
# MAGIC         pl.`Source No.` AS vendor_no,
# MAGIC         pl.`Direct Unit Cost` AS pl_buy_price,
# MAGIC         pl.`Currency Code` AS pl_buy_currency,
# MAGIC         pl.`Unit of Measure Code` AS pl_buy_uom,
# MAGIC         pl.`Price List Code` AS pl_buy_code,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY pl.`BC Company`, pl.`Asset No.`, pl.`Source No.`
# MAGIC             ORDER BY pl.`Starting Date` DESC, pl.`Line No.` DESC
# MAGIC         ) AS rn
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Price List Line` pl
# MAGIC     WHERE pl.`Source Type` = 'Vendor'
# MAGIC       AND pl.`Asset Type` = 'Item'
# MAGIC       AND pl.`Status` = 'Active'
# MAGIC ),
# MAGIC sell_price_list AS (
# MAGIC     SELECT
# MAGIC         pl.`BC Company` AS bc_company,
# MAGIC         pl.`Asset No.` AS item_no,
# MAGIC         pl.`Source No.` AS customer_price_group,
# MAGIC         pl.`Unit Price` AS pl_sell_price,
# MAGIC         pl.`Currency Code` AS pl_sell_currency,
# MAGIC         pl.`Unit of Measure Code` AS pl_sell_uom,
# MAGIC         pl.`Price List Code` AS pl_sell_code,
# MAGIC         pl.`Starting Date` AS pl_sell_start_date,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY pl.`BC Company`, pl.`Asset No.`
# MAGIC             ORDER BY pl.`Starting Date` DESC, pl.`Line No.` DESC
# MAGIC         ) AS rn
# MAGIC     FROM Silver_BC_Lakehouse.bc.`Price List Line` pl
# MAGIC     WHERE pl.`Source Type` = 'Customer Price Group'
# MAGIC       AND pl.`Asset Type` = 'Item'
# MAGIC       AND pl.`Status` = 'Active'
# MAGIC       AND pl.`Unit Price` > 0
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     i.`No.` AS item,
# MAGIC     i.`Description` AS description,
# MAGIC     i.`Item Category Code` AS category,
# MAGIC     aiv.vendor_no AS vendor,
# MAGIC     COALESCE(rs.vendor_name, v.`Name`) AS vendor_name,
# MAGIC     COALESCE(rs.vendor_item_no, iv.`Vendor Item No.`) AS vendor_item,
# MAGIC     CASE WHEN i.`Vendor No.` = aiv.vendor_no THEN 'Yes' ELSE 'No' END AS is_default_vendor,
# MAGIC 
# MAGIC     CAST(lr.latest_price AS DECIMAL(18,4)) AS buy_price_receipt,
# MAGIC     i.`Base Unit of Measure` AS unit_of_measure,
# MAGIC 
# MAGIC     COALESCE(
# MAGIC         CASE WHEN rs.currency_code = '' THEN NULL ELSE rs.currency_code END,
# MAGIC         'THB'
# MAGIC     ) AS buy_currency,
# MAGIC 
# MAGIC     CAST(vpl.pl_buy_price AS DECIMAL(18,4)) AS buy_price_price_list,
# MAGIC     COALESCE(
# MAGIC         CASE WHEN vpl.pl_buy_currency = '' THEN NULL ELSE vpl.pl_buy_currency END,
# MAGIC         'THB'
# MAGIC     ) AS buy_pl_currency,
# MAGIC     vpl.pl_buy_uom AS buy_pl_uom,
# MAGIC 
# MAGIC     CAST(spl.pl_sell_price AS DECIMAL(18,4)) AS sell_price,
# MAGIC     spl.pl_sell_currency AS sell_currency,
# MAGIC     spl.pl_sell_uom AS sell_uom,
# MAGIC     spl.customer_price_group AS customer_price_group,
# MAGIC     spl.pl_sell_start_date AS sell_price_start_date,
# MAGIC 
# MAGIC     CAST(
# MAGIC         CASE
# MAGIC             WHEN spl.pl_sell_price > 0 AND lr.latest_price > 0
# MAGIC              AND spl.pl_sell_uom = COALESCE(rs.unit_of_measure_code, i.`Base Unit of Measure`)
# MAGIC             THEN spl.pl_sell_price - lr.latest_price
# MAGIC         END
# MAGIC     AS DECIMAL(18,4)) AS margin_sell_buy,
# MAGIC 
# MAGIC     CAST(
# MAGIC         CASE
# MAGIC             WHEN spl.pl_sell_price > 0 AND lr.latest_price > 0
# MAGIC              AND spl.pl_sell_uom = COALESCE(rs.unit_of_measure_code, i.`Base Unit of Measure`)
# MAGIC             THEN ROUND((spl.pl_sell_price - lr.latest_price) * 100.0 / spl.pl_sell_price, 2)
# MAGIC         END
# MAGIC     AS DECIMAL(18,2)) AS margin_percent,
# MAGIC 
# MAGIC     CAST(pr.direct_unit_cost AS DECIMAL(18,4)) AS previous_buy_price,
# MAGIC 
# MAGIC     CAST(
# MAGIC         CASE
# MAGIC             WHEN pr.direct_unit_cost > 0 AND lr.latest_price IS NOT NULL
# MAGIC             THEN ROUND((lr.latest_price - pr.direct_unit_cost) * 100.0 / pr.direct_unit_cost, 2)
# MAGIC         END
# MAGIC     AS DECIMAL(18,2)) AS buy_price_change_percent,
# MAGIC 
# MAGIC     CAST(rs.min_price AS DECIMAL(18,4)) AS min_buy_price,
# MAGIC     CAST(rs.max_price AS DECIMAL(18,4)) AS max_buy_price,
# MAGIC     CAST(rs.avg_price AS DECIMAL(18,4)) AS avg_buy_price,
# MAGIC 
# MAGIC     COALESCE(rs.total_receipts, 0) AS total_receipts,
# MAGIC     CAST(COALESCE(rs.total_qty_received, 0) AS DECIMAL(18,4)) AS total_qty_received,
# MAGIC     CAST(COALESCE(rs.total_amount, 0) AS DECIMAL(18,4)) AS total_amount,
# MAGIC 
# MAGIC     rs.first_receipt_date AS first_receipt_date,
# MAGIC     lr.latest_receipt_date AS last_receipt_date,
# MAGIC     lr.latest_receipt_no AS last_receipt_no,
# MAGIC     CAST(lr.latest_qty AS DECIMAL(18,4)) AS last_qty,
# MAGIC 
# MAGIC     COALESCE(po.open_po_count, 0) AS open_po_count,
# MAGIC     CAST(COALESCE(po.outstanding_qty, 0) AS DECIMAL(18,4)) AS outstanding_qty,
# MAGIC     CAST(COALESCE(po.outstanding_amount, 0) AS DECIMAL(18,4)) AS outstanding_amount,
# MAGIC     po.earliest_expected_date AS expected_receipt_date,
# MAGIC 
# MAGIC     i.`Vendor No.` AS default_vendor,
# MAGIC     CAST(i.`Last Direct Cost` AS DECIMAL(18,4)) AS item_card_price,
# MAGIC     iv.`Lead Time Calculation` AS lead_time
# MAGIC FROM all_item_vendor aiv
# MAGIC JOIN Silver_BC_Lakehouse.bc.`Item` i
# MAGIC   ON aiv.item_no = i.`No.`
# MAGIC  AND aiv.bc_company = i.`BC Company`
# MAGIC LEFT JOIN Silver_BC_Lakehouse.bc.`Vendor` v
# MAGIC   ON aiv.vendor_no = v.`No.`
# MAGIC  AND aiv.bc_company = v.`BC Company`
# MAGIC LEFT JOIN Silver_BC_Lakehouse.bc.`Item Vendor` iv
# MAGIC   ON aiv.item_no = iv.`Item No.`
# MAGIC  AND aiv.vendor_no = iv.`Vendor No.`
# MAGIC  AND aiv.bc_company = iv.`BC Company`
# MAGIC LEFT JOIN receipt_summary rs
# MAGIC   ON aiv.item_no = rs.item_no
# MAGIC  AND aiv.vendor_no = rs.vendor_no
# MAGIC  AND aiv.bc_company = rs.bc_company
# MAGIC LEFT JOIN latest_receipt lr
# MAGIC   ON aiv.item_no = lr.item_no
# MAGIC  AND aiv.vendor_no = lr.vendor_no
# MAGIC  AND aiv.bc_company = lr.bc_company
# MAGIC  AND lr.rn = 1
# MAGIC LEFT JOIN previous_receipt pr
# MAGIC   ON aiv.item_no = pr.item_no
# MAGIC  AND aiv.vendor_no = pr.vendor_no
# MAGIC  AND aiv.bc_company = pr.bc_company
# MAGIC  AND pr.rn = 2
# MAGIC LEFT JOIN outstanding_po po
# MAGIC   ON aiv.item_no = po.item_no
# MAGIC  AND aiv.vendor_no = po.vendor_no
# MAGIC  AND aiv.bc_company = po.bc_company
# MAGIC LEFT JOIN vendor_price_list vpl
# MAGIC   ON aiv.item_no = vpl.item_no
# MAGIC  AND aiv.vendor_no = vpl.vendor_no
# MAGIC  AND aiv.bc_company = vpl.bc_company
# MAGIC  AND vpl.rn = 1
# MAGIC LEFT JOIN sell_price_list spl
# MAGIC   ON aiv.item_no = spl.item_no
# MAGIC  AND aiv.bc_company = spl.bc_company
# MAGIC  AND spl.rn = 1
# MAGIC WHERE i.`Blocked` = 0
# MAGIC ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
