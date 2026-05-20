# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6fa25cdd-36f9-4f2e-9817-c1f4d946d4d9",
# META       "default_lakehouse_name": "Gold_Production_Lakehouse",
# META       "default_lakehouse_workspace_id": "d74457b3-045c-445d-82c6-9a2e4b9f1436",
# META       "known_lakehouses": [
# META         {
# META           "id": "ad99fdfa-85b1-4480-9f7f-2640bfd65f24"
# META         },
# META         {
# META           "id": "e248ea90-8431-4df2-9f29-87866bf9dd5a"
# META         },
# META         {
# META           "id": "869b263b-1a86-424b-bd97-94bd586442b2"
# META         },
# META         {
# META           "id": "3a130b81-98ec-4fd4-a404-95edc1f0ef1e"
# META         },
# META         {
# META           "id": "6fa25cdd-36f9-4f2e-9817-c1f4d946d4d9"
# META         },
# META         {
# META           "id": "ff4d6787-a716-43b6-baaf-972b7426ffa5"
# META         },
# META         {
# META           "id": "3ea0efcd-03d5-44f1-8e70-99f52a5c2a22"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, current_timestamp, lit, concat
from pyspark.sql.types import StringType, StructType, StructField
from urllib.parse import urlparse, parse_qs, unquote
import requests, msal, base64

# --- CONFIG ---
SRC_PATH = "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/25adf76d-df18-41c8-97e8-55789149bd80/Tables/dts_cadrequests"
OUT_PATH = "abfss://d74457b3-045c-445d-82c6-9a2e4b9f1436@onelake.dfs.fabric.microsoft.com/ad99fdfa-85b1-4480-9f7f-2640bfd65f24/Tables/prod/silver_cad_images"

TENANT_ID = "<your-tenant-id>"
CLIENT_ID = "<your-client-id>"
CLIENT_SECRET = "<your-client-secret>"
DATAVERSE_BASE = "https://ennoprod.crm5.dynamics.com"

spark = SparkSession.builder.getOrCreate()
src = spark.read.format("delta").load(SRC_PATH)

# --- Parse the portal URL ---
def parse_portal(url):
    try:
        q = parse_qs(urlparse(url).query)
        entity = q.get("Entity", q.get("entity", [None]))[0]
        attr   = q.get("Attribute", q.get("attribute", [None]))[0]
        rid    = q.get("Id", q.get("id", [None]))[0]
        if rid:
            rid = unquote(rid).replace("{","").replace("}","").strip()
        return entity, attr, rid
    except Exception:
        return None, None, None

parse_schema = StructType([
    StructField("Entity", StringType(), True),
    StructField("Attribute", StringType(), True),
    StructField("Id", StringType(), True),
])

@udf(parse_schema)
def parse_udf(url):
    return parse_portal(url)

parsed = src.withColumn("parsed", parse_udf(col("PIC_POTAL"))) \
            .select(col("PIC_POTAL").alias("PortalUrl"),
                    col("parsed.Entity").alias("Entity"),
                    col("parsed.Attribute").alias("Attribute"),
                    col("parsed.Id").alias("Id"))

parsed = parsed.filter(col("Entity").isNotNull() & col("Attribute").isNotNull() & col("Id").isNotNull())

# --- Token for Dataverse ---
def get_token():
    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
        client_credential=CLIENT_SECRET
    )
    token = app.acquire_token_for_client(scopes=[DATAVERSE_BASE + "/.default"])
    return token.get("access_token")

TOKEN = get_token()
BC_TOKEN = spark.sparkContext.broadcast(TOKEN)

# --- Fetch function ---
def fetch_partition(it):
    import requests, base64
    token = BC_TOKEN.value
    headers = {"Authorization": f"Bearer {token}", "Accept": "image/*"}
    for row in it:
        rid, entity, attr, portal = row.Id, row.Entity, row.Attribute, row.PortalUrl
        url = f"{DATAVERSE_BASE}/api/data/v9.2/{entity}({rid})/{attr}/$value"
        try:
            r = requests.get(url, headers=headers, timeout=20)
            if r.status_code == 200:
                b64 = base64.b64encode(r.content).decode("ascii")
                uri = f"data:image/jpeg;base64,{b64}"
                yield (rid, portal, uri, "OK")
            else:
                yield (rid, portal, None, f"HTTP_{r.status_code}")
        except Exception as e:
            yield (rid, portal, None, f"ERR_{type(e).__name__}")

fetched = parsed.rdd.mapPartitions(fetch_partition)
schema = StructType([
    StructField("Id", StringType(), True),
    StructField("PortalUrl", StringType(), True),
    StructField("ImageURI", StringType(), True),
    StructField("Status", StringType(), True),
])
df = spark.createDataFrame(fetched, schema)
df = df.withColumn("FetchedAt", current_timestamp())

df.write.mode("overwrite").format("delta").save(OUT_PATH)
print("Image mapping table saved to:", OUT_PATH)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

H_SRC = "Silver_Production_Lakehouse.prod.silver_prod_order_header"
S_SRC = "Silver_Production_Lakehouse.prod.silver_prod_order_status"
L_SRC = "Gold_Production_Lakehouse.prod.gold_production_asgn_cell"

h = spark.table(H_SRC).alias("h")
s = spark.table(S_SRC).alias("s")
l = spark.table(L_SRC).alias("l")

print("header rows:", h.count())
print("status rows:", s.count())
print("asgn_cell rows:", l.count())

# 1) check header Released distribution (case/space issues are common)
h_status = (h
  .withColumn("status_norm", F.upper(F.trim(F.col("prod_order_status"))))
  .groupBy("status_norm").count()
  .orderBy(F.desc("count"))
)
h_status.show(20, False)

# 2) check type_name distribution (case/space issues)
s_types = (s
  .withColumn("type_norm", F.upper(F.trim(F.col("type_name"))))
  .groupBy("type_norm").count()
  .orderBy(F.desc("count"))
)
s_types.show(20, False)

# 3) how many header/status join rows pass "Released" (normalized) + 4 event types?
s_norm = s.withColumn("type_norm", F.upper(F.trim(F.col("type_name"))))
h_norm = h.withColumn("status_norm", F.upper(F.trim(F.col("prod_order_status"))))
base0 = (h_norm.join(s_norm, F.col("h.prod_order_no")==F.col("s.prod_order_no"), "inner")
              .filter(F.col("status_norm")=="RELEASED")
              .filter(F.col("type_norm").isin("IN LOCATION IN","OUT LOCATION","TO EMPLOYEE","FROM EMPLOYEE")))
print("base0 (before time conversion) rows:", base0.count())

# 4) created_on null rate and BKK date span
nulls = base0.filter(F.col("s.created_on").isNull()).count()
print("rows with s.created_on IS NULL:", nulls)
span = (base0
  .withColumn("created_on_ts", F.to_timestamp("s.created_on"))
  .withColumn("created_on_bkk", F.col("created_on_ts") + F.expr("INTERVAL 7 HOURS"))
  .select(F.min(F.to_date("created_on_bkk")).alias("min_bkk"),
          F.max(F.to_date("created_on_bkk")).alias("max_bkk"))
).collect()[0]
print("created_on_bkk date span:", span["min_bkk"], "→", span["max_bkk"])



from pyspark.sql import functions as F

# Rebuild normalized base quickly (without date window yet)
base_norm = (
    h.withColumn("status_norm", F.upper(F.trim("prod_order_status")))
     .join(s.withColumn("type_norm", F.upper(F.trim("type_name"))), F.col("h.prod_order_no")==F.col("s.prod_order_no"), "inner")
     .join(l, (F.col("s.prod_order_no")==F.col("l.prod_order_no"))&(F.col("s.prod_order_line_no")==F.col("l.prod_order_line_no")), "left")
     .filter(F.col("status_norm")=="RELEASED")
     .filter(F.col("type_norm").isin("IN LOCATION IN","OUT LOCATION","TO EMPLOYEE","FROM EMPLOYEE"))
     .withColumn("created_on_ts", F.to_timestamp("s.created_on"))
     .withColumn("created_on_bkk", F.date_trunc("second", F.col("created_on_ts")+F.expr("INTERVAL 7 HOURS")))
     .select(
         F.col("h.prod_order_no").alias("prod_order_no"),
         F.col("s.prod_order_line_no").alias("prod_order_line_no"),
         F.col("s.type_name").alias("type_name"),
         F.col("s.operation_no").alias("operation_no"),
         F.col("s.item_no").alias("item_no"),
         F.col("s.quantity").alias("quantity"),
         F.col("s.machine_center_no").alias("machine_center_no"),
         F.col("l.prod_line").alias("prod_line"),
         F.col("l.cell_line").alias("cell_line"),
         F.col("created_on_bkk"),
     )
)

print("base_norm rows:", base_norm.count())

# op_major (fill nulls with -1 so NULL==NULL pairing works like “same bucket”)
op_major = F.when(F.instr(F.col("operation_no").cast("string"), ".")>0,
                  F.regexp_extract(F.col("operation_no").cast("string"), r"^([^.]*)", 1).cast("int")) \
            .otherwise(F.col("operation_no").cast("int"))
wr = base_norm.withColumn("op_major", F.coalesce(op_major, F.lit(-1)))

def next_time(df_all, type_start, type_end):
    s_alias = (df_all.filter(F.upper(F.trim("type_name"))==F.lit(type_start))
               .select(F.col("prod_order_no").alias("s_po"),
                       F.col("prod_order_line_no").alias("s_line"),
                       F.col("op_major").alias("s_op"),
                       "prod_line","cell_line","machine_center_no",
                       F.col("created_on_bkk").alias("t_start")))
    e_alias = (df_all.filter(F.upper(F.trim("type_name"))==F.lit(type_end))
               .select(F.col("prod_order_no").alias("e_po"),
                       F.col("prod_order_line_no").alias("e_line"),
                       F.col("op_major").alias("e_op"),
                       F.col("created_on_bkk").alias("cand_end")))
    joined = s_alias.join(e_alias,
                          (F.col("s_po")==F.col("e_po"))&
                          (F.col("s_line")==F.col("e_line"))&
                          (F.col("s_op")==F.col("e_op"))&
                          (F.col("cand_end")>=F.col("t_start")),
                          "left")
    grouped = (joined.groupBy("s_po","s_line","s_op","prod_line","cell_line","machine_center_no","t_start")
                     .agg(F.min("cand_end").alias("t_end")))
    return (grouped.filter(F.col("t_end").isNotNull())
                   .select(F.col("s_po").alias("prod_order_no"),
                           F.col("s_line").alias("prod_order_line_no"),
                           F.col("s_op").alias("op_major"),
                           "prod_line","cell_line","machine_center_no","t_start","t_end"))

p_in_out  = next_time(wr, "IN LOCATION IN", "OUT LOCATION")
p_to_from = next_time(wr, "TO EMPLOYEE",    "FROM EMPLOYEE")
p_in_to   = next_time(wr, "IN LOCATION IN", "TO EMPLOYEE")

print("pairs in→out:",  p_in_out.count())
print("pairs to→from:", p_to_from.count())
print("pairs in→to:",   p_in_to.count())



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Planned & Actual from header + all IO lines (daily + cumulative) with visualization
from pyspark.sql import functions as F
import matplotlib.pyplot as plt

# =========================
# CONFIG (optional)
# =========================
USE_FIN_GOODS_ONLY = True       # filter io.entry_type_item_location = 'FIN-GOODS'
FILTER_WRO_PREFIX  = False      # restrict to orders like 'WRO%'
PLANNED_DATE_COL   = "prod_order_ending_date_time"  # or "prod_order_due_date"

# =========================
# 1) Planned (from header, order-level)
# =========================
planned_sql = f"""
SELECT
  CAST(poh.{PLANNED_DATE_COL} AS DATE) AS date,
  SUM(poh.prod_order_quantity) AS planned_qty
FROM Silver_Production_Lakehouse.prod.silver_prod_order_header poh
{"WHERE poh.prod_order_no LIKE 'WRO%'" if FILTER_WRO_PREFIX else ""}
GROUP BY CAST(poh.{PLANNED_DATE_COL} AS DATE)
"""
planned_daily = spark.sql(planned_sql).where(F.col("date").isNotNull())

# =========================
# 2) Actual (sum ALL IO lines per order; use posting_date fallback created_on)
#    Join to header so optional order filters match universe of orders
# =========================
actual_base = spark.sql("""
SELECT
  poh.prod_order_no,
  COALESCE(io.posting_date, io.created_on) AS actual_dt,
  io.entry_type_item_location,
  io.entry_type_item_quantity AS actual_output_qty
FROM Silver_Production_Lakehouse.prod.silver_prod_order_header poh
JOIN Gold_Production_Lakehouse.prod.gold_inv_output io
  ON io.order_no = poh.prod_order_no
""")

if FILTER_WRO_PREFIX:
    actual_base = actual_base.filter(F.col("prod_order_no").like("WRO%"))

if USE_FIN_GOODS_ONLY:
    actual_base = actual_base.filter(F.col("entry_type_item_location") == "FIN-GOODS")

actual_daily = (
    actual_base
    .withColumn("date", F.to_date(F.col("actual_dt")))
    .where(F.col("date").isNotNull())
    .groupBy("date")
    .agg(F.sum("actual_output_qty").alias("actual_output_qty"))
)

# =========================
# 3) Combine timelines (full outer join on date)
# =========================
daily = (
    planned_daily.join(actual_daily, on="date", how="full")
    .na.fill({"planned_qty": 0, "actual_output_qty": 0})
    .orderBy("date")
)

# =========================
# 4) Cumulative since earliest date
# =========================
from pyspark.sql.window import Window
w_day = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
daily_cum = (
    daily
    .withColumn("cum_planned_qty", F.sum("planned_qty").over(w_day))
    .withColumn("cum_actual_qty",  F.sum("actual_output_qty").over(w_day))
)

# =========================
# 5) Plot: per-day planned vs actual
# =========================
pdf_daily = daily.toPandas()
plt.figure(figsize=(11, 5))
plt.plot(pdf_daily["date"], pdf_daily["planned_qty"], label="Planned (Daily)")
plt.plot(pdf_daily["date"], pdf_daily["actual_output_qty"], label="Actual (Daily)")
title_suffix = []
if FILTER_WRO_PREFIX: title_suffix.append("WRO*")
if USE_FIN_GOODS_ONLY: title_suffix.append("FIN-GOODS")
plt.title("Planned vs Actual — Daily" + (f" ({', '.join(title_suffix)})" if title_suffix else ""))
plt.xlabel("Date"); plt.ylabel("Quantity")
plt.legend(); plt.xticks(rotation=45); plt.tight_layout(); plt.show()

# =========================
# 6) Plot: cumulative (since earliest date)
# =========================
pdf_cum = daily_cum.select("date", "cum_planned_qty", "cum_actual_qty").toPandas()
plt.figure(figsize=(11, 5))
plt.plot(pdf_cum["date"], pdf_cum["cum_planned_qty"], label="Cumulative Planned")
plt.plot(pdf_cum["date"], pdf_cum["cum_actual_qty"],  label="Cumulative Actual")
plt.title("Planned vs Actual — Cumulative (Since Earliest Date)" + (f" ({', '.join(title_suffix)})" if title_suffix else ""))
plt.xlabel("Date"); plt.ylabel("Accumulated Quantity")
plt.legend(); plt.xticks(rotation=45); plt.tight_layout(); plt.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================================
# Job: gold_prod_planned_vs_actual  (incremental by date)
# FIN-GOODS on both sides + WRO% orders only
#
# Sources:
#   - Silver_Production_Lakehouse.prod.silver_prod_order_line (pol)
#   - prod.gold_inv_output  (io)
# Target:
#   - prod.gold_prod_planned_vs_actual  (CURRENT lakehouse)
# ==========================================================

from pyspark.sql import functions as F, Window as W
from delta.tables import DeltaTable
from datetime import date, datetime
from pyspark.sql.utils import AnalysisException

POL_SRC = "Silver_Production_Lakehouse.prod.silver_prod_order_line"  # pol
IO_SRC  = "prod.gold_inv_output"                                    # io (current lakehouse)

TARGET_SCHEMA = "prod"
TARGET_TABLE  = "gold_prod_planned_vs_actual"
TARGET        = f"{TARGET_SCHEMA}.{TARGET_TABLE}"

# ---------------- widgets / params ----------------
def get_widget(name: str, default: str) -> str:
    try:
        import dbutils  # type: ignore
        return dbutils.widgets.get(name)  # type: ignore
    except Exception:
        return default

try:
    dbutils.widgets.text("full_reload", "false")  # type: ignore
    dbutils.widgets.text("since", "")             # yyyy-MM-dd  # type: ignore
    dbutils.widgets.text("until", "")             # yyyy-MM-dd  # type: ignore
except Exception:
    pass

full_reload = get_widget("full_reload", "false").strip().lower() == "true"
since_param = get_widget("since", "").strip()
until_param = get_widget("until", "").strip()

def parse_date_or_none(s: str):
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()

def table_exists(name: str) -> bool:
    try:
        spark.table(name)
        return True
    except AnalysisException:
        return False

def pick_watermark_from_target(table_name: str):
    try:
        df = spark.table(table_name)
    except Exception:
        return None
    if "date" in df.columns:
        return df.agg(F.max("date").alias("mx")).collect()[0]["mx"]
    return None

since_date = parse_date_or_none(since_param)
until_date = parse_date_or_none(until_param) or date.today()

if not full_reload and since_date is None and table_exists(TARGET):
    wm = pick_watermark_from_target(TARGET)
    if wm is not None:
        since_date = wm
if full_reload or since_date is None:
    since_date = date(1900, 1, 1)

print(f"Incremental window (change_ts date): {since_date} -> {until_date}")

# ---------------- Load raw sources ----------------
pol_raw = spark.table(POL_SRC)
io_raw  = spark.table(IO_SRC)

# ✅ Apply filters EARLY
#    - pol.item_location = 'FIN-GOODS'
#    - pol.prod_order_no LIKE 'WRO%'
pol_fg_raw = (
    pol_raw
    .filter(F.col("item_location") == F.lit("FIN-GOODS"))
    .filter(F.col("prod_order_no").startswith("WRO"))  # same as LIKE 'WRO%'
).alias("pol")

#    - io.entry_type_item_location = 'FIN-GOODS'
io_fg_raw  = io_raw.filter(F.col("entry_type_item_location") == F.lit("FIN-GOODS")).alias("io")

# ---------------- Build per-(order,line) change_ts to scope rows ----------------
pol_ts = (pol_fg_raw
          .groupBy("prod_order_no","prod_order_line_no")
          .agg(F.max("modified_on").alias("pol_mod")))

io_ts = (io_fg_raw
         .groupBy(F.col("order_no").alias("prod_order_no"),
                  F.col("order_lineno").alias("prod_order_line_no"))
         .agg(F.max("modified_on").alias("io_mod")))

ts_joined = (pol_ts.join(io_ts, ["prod_order_no","prod_order_line_no"], "full")
                  .select(
                      F.coalesce(pol_ts["prod_order_no"], io_ts["prod_order_no"]).alias("prod_order_no"),
                      F.coalesce(pol_ts["prod_order_line_no"], io_ts["prod_order_line_no"]).alias("prod_order_line_no"),
                      "pol_mod","io_mod")
                  .withColumn("change_ts", F.greatest("pol_mod","io_mod"))
           )

ts_windowed = ts_joined.filter(
    (F.to_date("change_ts") >= F.lit(since_date)) &
    (F.to_date("change_ts") <= F.lit(until_date))
)

scope = ts_windowed.select("prod_order_no","prod_order_line_no").distinct()

# Restrict sources to scope (already filtered to FIN-GOODS + WRO%)
pol = pol_fg_raw.join(scope, ["prod_order_no","prod_order_line_no"], "inner")
io  = (io_fg_raw
       .join(scope,
             (io_fg_raw["order_no"] == scope["prod_order_no"]) &
             (io_fg_raw["order_lineno"] == scope["prod_order_line_no"]),
             "inner")
       .drop(scope["prod_order_no"]).drop(scope["prod_order_line_no"])
      )

# ---------------- OrderedData (LEFT JOIN pol→io) ----------------
ordered = (pol.alias("pol")
    .join(io.alias("io"),
          (F.col("io.order_no")    == F.col("pol.prod_order_no")) &
          (F.col("io.order_lineno")== F.col("pol.prod_order_line_no")),
          "left")
    .select(
        F.col("pol.prod_order_no").alias("prod_order_no"),
        F.col("pol.prod_order_line_no").alias("prod_order_line_no"),
        F.col("pol.prod_line_due_date").alias("prod_line_due_date"),
        F.col("pol.prod_line_start_date").alias("planned_start_date"),
        F.col("pol.prod_line_end_date").alias("planned_end_date"),
        F.col("pol.prod_line_quantity").alias("planned_qty"),
        F.col("io.created_on").alias("actual_start_date"),
        F.col("io.posting_date").alias("actual_end_date"),
        F.col("io.entry_type_item_quantity").alias("actual_output_qty"),
    )
    .withColumn(
        "rn",
        F.row_number().over(
            W.partitionBy("prod_order_no","prod_order_line_no")
             .orderBy(F.col("actual_start_date").desc_nulls_last())
        )
    )
)

# ---------------- LatestOnly (rn = 1) ----------------
latest = (ordered
    .filter(F.col("rn")==1)
    .select(
        "prod_order_no",
        "prod_order_line_no",
        "prod_line_due_date",
        "planned_start_date",
        "planned_end_date",
        "planned_qty",
        "actual_start_date",
        "actual_end_date",
        "actual_output_qty"
    )
)

# ---------------- DailyPlanned / DailyActual ----------------
daily_planned = (latest
    .filter(F.col("planned_end_date").isNotNull())
    .groupBy(F.to_date("planned_end_date").alias("date"))
    .agg(F.sum("planned_qty").alias("planned_qty"))
)

daily_actual = (latest
    .filter(F.coalesce(F.col("actual_end_date"), F.col("actual_start_date")).isNotNull())
    .groupBy(F.to_date(F.coalesce("actual_end_date","actual_start_date")).alias("date"))
    .agg(F.sum("actual_output_qty").alias("actual_output_qty"))
)

# ---------------- Combined (FULL OUTER, fill nulls) ----------------
combined = (daily_planned.alias("p")
    .join(daily_actual.alias("a"), on="date", how="full")
    .select(
        F.col("date"),
        F.coalesce(F.col("p.planned_qty"), F.lit(0)).alias("planned_qty"),
        F.coalesce(F.col("a.actual_output_qty"), F.lit(0)).alias("actual_output_qty"),
    )
)

# ---------------- Cumulative sums ----------------
w_cum = W.orderBy(F.col("date")).rowsBetween(W.unboundedPreceding, W.currentRow)

final_df = (combined
    .select(
        "date",
        "planned_qty",
        "actual_output_qty",
        F.sum("planned_qty").over(w_cum).alias("cum_planned_qty"),
        F.sum("actual_output_qty").over(w_cum).alias("cum_actual_qty"),
    )
)

# Attach a change_ts (latest date in this batch) for reference
batch_ts = final_df.agg(F.max("date").alias("mx")).collect()[0]["mx"]
final_df = final_df.withColumn("change_ts", F.to_timestamp(F.lit(batch_ts)))

# ---------------- Ensure schema & upsert by date ----------------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")

def merge_or_create(target, df):
    if not table_exists(target):
        print(f"Creating {target} ...")
        (df.write
           .format("delta")
           .mode("overwrite")
           .option("overwriteSchema","true")
           .saveAsTable(target))
        return
    print(f"Merging into {target} ...")
    tgt = DeltaTable.forName(spark, target)
    (tgt.alias("t")
        .merge(df.alias("s"), "t.date <=> s.date")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

merge_or_create(TARGET, final_df)
print(f"✅ Done: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================================
# Job: gold_emp_daily_head_count  (incremental)
# Source: Silver_Commons_Lakehouse.cmn.silver_employee_time
# Target: CURRENT Lakehouse -> cmn.gold_emp_daily_head_count
# ==========================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window as W
from delta.tables import DeltaTable
from datetime import date, datetime
from pyspark.sql.utils import AnalysisException

SRC = "Silver_Commons_Lakehouse.cmn.silver_employee_time"

# ---- Use 2-part name (schema.table) in the CURRENT Lakehouse
TARGET_SCHEMA = "prod"
TARGET_TABLE  = "gold_emp_daily_head_count"
TARGET        = f"Gold_Production_Lakehouse.{TARGET_SCHEMA}.{TARGET_TABLE}"

# ---------------- widgets / params ----------------
def get_widget(name: str, default: str) -> str:
    try:
        import dbutils  # type: ignore
        return dbutils.widgets.get(name)  # type: ignore
    except Exception:
        return default

try:
    dbutils.widgets.text("full_reload", "false")  # type: ignore
    dbutils.widgets.text("since", "")             # yyyy-MM-dd  # type: ignore
    dbutils.widgets.text("until", "")             # yyyy-MM-dd  # type: ignore
except Exception:
    pass

full_reload = get_widget("full_reload", "false").strip().lower() == "true"
since_param = get_widget("since", "").strip()
until_param = get_widget("until", "").strip()

def parse_date_or_none(s: str):
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()

def table_exists(name: str) -> bool:
    try:
        spark.table(name)
        return True
    except AnalysisException:
        return False

# Use max(work_date) in target as watermark
def pick_watermark_from_target(table_name: str):
    try:
        df = spark.table(table_name)
    except Exception:
        return None
    if "work_date" in df.columns:
        return df.agg(F.max("work_date").alias("mx")).collect()[0]["mx"]
    return None

since_date = parse_date_or_none(since_param)
until_date = parse_date_or_none(until_param) or date.today()

# If no explicit window and target exists, use target watermark
if not full_reload and since_date is None and table_exists(TARGET):
    wm = pick_watermark_from_target(TARGET)
    if wm is not None:
        since_date = wm

# If still not set, infer from source (first run) or default very old
src_df_for_bounds = spark.table(SRC).filter(F.col("Work_Day").isNotNull())
if full_reload or since_date is None:
    bounds = src_df_for_bounds.agg(
        F.min(F.to_date("Work_Day")).alias("min_d"),
        F.max(F.to_date("Work_Day")).alias("max_d")
    ).collect()[0]
    since_date = bounds["min_d"] or date(1900, 1, 1)
    until_date = bounds["max_d"] or until_date

print(f"Incremental window (Work_Day date): {since_date} -> {until_date}")

# ---------------- load + scope ----------------
src = (
    spark.table(SRC)
         .filter(F.col("Work_Day").isNotNull())
         .withColumn("WorkDate", F.to_date("Work_Day"))
         .filter(
             (F.col("WorkDate") >= F.lit(since_date)) &
             (F.col("WorkDate") <= F.lit(until_date))
         )
)

# ---------------- punches (coalesce timestamps & leave mins) ----------------
in_ts  = F.coalesce(F.to_timestamp("actual_date_time_in"),
                    F.to_timestamp("Standard_Time_In"))
out_ts = F.coalesce(F.to_timestamp("actual_date_time_out"),
                    F.to_timestamp("Standard_Time_Out"))
br_s   = F.to_timestamp("Break_Start")
br_e   = F.to_timestamp("Break_End")

leave_min = F.coalesce(
    F.col("Total_Leave_Hours").cast("double") * F.lit(60.0),
    F.col("Total_Leave_Days").cast("double")  * F.lit(560.0),
    F.lit(0.0)
)

punches = (
    src.select(
        F.col("Identity_ID"),
        F.col("WorkDate"),
        in_ts.alias("in_dt"),
        out_ts.alias("out_dt"),
        br_s.alias("br_s"),
        br_e.alias("br_e"),
        leave_min.alias("leave_min")
    )
)

# ---------------- row_minutes (per row) ----------------
mins_work = (F.col("out_dt").cast("long") - F.col("in_dt").cast("long")) / F.lit(60.0)
mins_break = F.when(
    F.col("br_s").isNull() | F.col("br_e").isNull(),
    F.lit(0.0)
).otherwise((F.col("br_e").cast("long") - F.col("br_s").cast("long")) / F.lit(60.0))

row_minutes = (
    punches
      .filter(F.col("in_dt").isNotNull() & F.col("out_dt").isNotNull())
      .select(
          "Identity_ID","WorkDate","leave_min",
          F.greatest(F.lit(0.0), mins_work - mins_break).alias("work_min_row")
      )
)

# ---------------- day_person (sum rows per person/day, subtract leave) ----------------
day_person = (
    row_minutes.groupBy("Identity_ID","WorkDate")
               .agg(
                   F.greatest(
                       F.lit(0.0),
                       F.sum("work_min_row") - F.max("leave_min")
                   ).alias("work_min_day")
               )
)

# ---------------- final daily summary ----------------
full_day = F.lit(560.0)

final_df = (
    day_person.groupBy("WorkDate")
              .agg(
                  F.sum(F.when(F.col("work_min_day") >= full_day, F.lit(1)).otherwise(F.lit(0))).alias("full_day_head_count"),
                  F.sum(F.col("work_min_day") / full_day).alias("FTE_attendance"),
                  F.sum(F.when(F.col("work_min_day") > full_day, F.col("work_min_day") - full_day).otherwise(F.lit(0.0))).alias("total_OT_minutes"),
              )
              .withColumnRenamed("WorkDate", "work_date")
)

# Optional helper timestamp for future windows
batch_change_ts = final_df.agg(F.max("work_date").alias("mx")).collect()[0]["mx"]
final_df = final_df.withColumn("change_ts", F.to_timestamp(F.lit(batch_change_ts)))

final_df = final_df.select("work_date","full_day_head_count","FTE_attendance","total_OT_minutes","change_ts")

# ---------------- create schema (safe) ----------------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")

# ---------------- upsert (one row per work_date) ----------------
def merge_or_create(target, df):
    if not table_exists(target):
        print(f"Creating {target} ...")
        (df.write
           .format("delta")
           .mode("overwrite")
           .option("overwriteSchema","true")
           .saveAsTable(target))
        return
    print(f"Merging into {target} ...")
    tgt = DeltaTable.forName(spark, target)
    (tgt.alias("t")
        .merge(df.alias("s"), "t.work_date <=> s.work_date")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

merge_or_create(TARGET, final_df)
print(f"✅ Done: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================================
# Job: Gold_Production_Lakehouse.prod.prod_repair  (incremental)
# Mirrors your vWRE_LineDetail logic:
# - Pair main lines to WRE repair lines by item_no (exclude 'M-%')
# - Summarize WRE per main line and compute pct
# - change_ts watermark from main (ML) + related repair (WH/WL)
# ==========================================================

from pyspark.sql import functions as F
from pyspark.sql import Window as W
from delta.tables import DeltaTable
from datetime import date, datetime
from pyspark.sql.utils import AnalysisException

# ---------- Sources ----------
MH_SRC = "Silver_Production_Lakehouse.prod.silver_prod_order_header"  # main header
ML_SRC = "Silver_Production_Lakehouse.prod.silver_prod_order_line"    # main line
WH_SRC = "Silver_Production_Lakehouse.prod.silver_prod_order_header"  # repair header (WRE*)
WL_SRC = "Silver_Production_Lakehouse.prod.silver_prod_order_line"    # repair line

# ---------- Target ----------
TARGET = "Gold_Production_Lakehouse.prod.gold_prod_repair"

# ---------- Widgets / params ----------
def get_widget(name: str, default: str) -> str:
    try:
        import dbutils  # type: ignore
        return dbutils.widgets.get(name)  # type: ignore
    except Exception:
        return default

try:
    dbutils.widgets.text("full_reload", "false")  # type: ignore
    dbutils.widgets.text("since", "")             # yyyy-MM-dd  # type: ignore
    dbutils.widgets.text("until", "")             # yyyy-MM-dd  # type: ignore
except Exception:
    pass

full_reload = get_widget("full_reload", "false").strip().lower() == "true"
since_param = get_widget("since", "").strip()
until_param = get_widget("until", "").strip()

def parse_date_or_none(s: str):
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()

def table_exists(name: str) -> bool:
    try:
        spark.table(name)
        return True
    except AnalysisException:
        return False

def pick_watermark_from_target(table_name: str):
    try:
        df = spark.table(table_name)
    except Exception:
        return None
    if "change_ts" in df.columns:
        return df.agg(F.max(F.to_date("change_ts")).alias("mx")).collect()[0]["mx"]
    return None

since_date = parse_date_or_none(since_param)
until_date = parse_date_or_none(until_param) or date.today()

if not full_reload and since_date is None and table_exists(TARGET):
    wm = pick_watermark_from_target(TARGET)
    if wm is not None:
        since_date = wm
if full_reload or since_date is None:
    since_date = date(1900, 1, 1)

print(f"Incremental window (change_ts date): {since_date} -> {until_date}")

# ---------- Load sources ----------
mh_raw = spark.table(MH_SRC).alias("mh")
ml_raw = spark.table(ML_SRC).alias("ml")
wh_raw = spark.table(WH_SRC).alias("wh")
wl_raw = spark.table(WL_SRC).alias("wl")

# ==========================================================
# 1) change_ts per (main order, main line)
#    = greatest( ML.modified_on , max(WH/WL for related WRE orders) )
# ==========================================================

# ML timestamps per (main order, line)
ml_ts = (
    ml_raw
    .groupBy(
        F.col("prod_order_no").alias("m_prod"),
        F.col("prod_order_line_no").alias("m_line"),
    )
    .agg(F.max("modified_on").alias("ml_mod"))
)

# WRE headers only
wh_wre = wh_raw.filter(F.col("prod_order_no").like("WRE%")).alias("wh")

# Max WH.modified_on per main order (via ref_prod_order)
wh_ts = (
    wh_wre
    .groupBy(F.col("ref_prod_order").alias("m_prod"))
    .agg(F.max("modified_on").alias("wh_mod"))
)

# Max WL.modified_on per main order (map WRE -> main via WH.ref_prod_order)
wl_ts = (
    wl_raw
    .groupBy(F.col("prod_order_no").alias("wre_prod"))
    .agg(F.max("modified_on").alias("wl_mod"))
)

# Map each WRE order to its main order, then roll WL to main
wl_map = (
    wh_wre.select(F.col("prod_order_no").alias("wre_prod"),
                  F.col("ref_prod_order").alias("m_prod"))
    .distinct()
    .join(wl_ts, on="wre_prod", how="inner")
    .groupBy("m_prod")
    .agg(F.max("wl_mod").alias("wl_mod"))
)

# Combine WH + WL per main order
w_mod_per_main = (
    wh_ts.alias("A")
    .join(wl_map.alias("B"), on="m_prod", how="full")
    .select(
        F.coalesce(F.col("A.m_prod"), F.col("B.m_prod")).alias("m_prod"),
        F.greatest(F.col("A.wh_mod"), F.col("B.wl_mod")).alias("w_mod"),
    )
)

# Join ML line TS with w_mod on main order
change_ts_line = (
    ml_ts.alias("mlt")
    .join(w_mod_per_main.alias("wm"), F.col("mlt.m_prod") == F.col("wm.m_prod"), "left")
    .select(
        F.col("mlt.m_prod").alias("prod_order_no"),
        F.col("mlt.m_line").alias("prod_order_line_no"),
        F.greatest(F.col("mlt.ml_mod"), F.col("wm.w_mod")).alias("change_ts"),
    )
)

# Incremental scope
ts_windowed = change_ts_line.filter(
    (F.to_date("change_ts") >= F.lit(since_date)) &
    (F.to_date("change_ts") <= F.lit(until_date))
)
scope = ts_windowed.select("prod_order_no", "prod_order_line_no").distinct()

# ==========================================================
# 2) pair_strict (main ↔ WRE by item, exclude M-)
# ==========================================================
ml = ml_raw.join(scope, ["prod_order_no", "prod_order_line_no"], "inner").alias("ml")
mh = mh_raw.alias("mh")
wh = wh_wre.alias("wh")                          # already filtered to WRE%
wl = wl_raw.alias("wl")

pair_strict = (
    ml
    .join(mh, F.col("mh.prod_order_no") == F.col("ml.prod_order_no"), "inner")
    .join(wh, F.col("wh.ref_prod_order") == F.col("ml.prod_order_no"), "inner")
    .join(wl, F.col("wl.prod_order_no") == F.col("wh.prod_order_no"), "inner")
    .where(
        (F.col("ml.item_no") == F.col("wl.item_no")) &
        (~F.col("ml.item_no").like("M-%")) &
        (~F.col("wl.item_no").like("M-%"))
    )
    .select(
        F.col("ml.prod_order_no").alias("main_prod"),
        F.col("ml.prod_order_line_no").alias("main_line_no"),
        F.col("ml.item_no").alias("main_item_no"),
        F.col("ml.item_location").alias("main_item_location"),
        F.col("ml.prod_line_quantity").alias("main_line_qty"),
        F.col("mh.prod_order_status").alias("main_status"),
        F.col("mh.created_on").alias("main_created_on"),

        F.col("wl.prod_order_no").alias("wre_prod"),
        F.col("wl.prod_order_line_no").alias("wre_line_no"),
        F.col("wl.item_no").alias("wre_item_no"),
        F.col("wl.prod_line_quantity").alias("wre_line_qty"),
        F.col("wh.prod_order_status").alias("wre_status"),
    )
)

# ==========================================================
# 3) Summaries and final shape
# ==========================================================
pair_strict_summary = (
    pair_strict
    .groupBy("main_prod", "main_line_no", "main_item_no")
    .agg(
        F.min("main_item_location").alias("main_item_location"),
        F.min("main_created_on").alias("main_created_on"),
        F.min("main_status").alias("main_status"),
        F.sum("main_line_qty").alias("main_line_qty"),
        F.sum("wre_line_qty").alias("wre_total_qty"),
    )
)

final_df = (
    pair_strict.alias("ps")
    .join(
        pair_strict_summary.alias("s"),
        on=[
            F.col("ps.main_prod") == F.col("s.main_prod"),
            F.col("ps.main_line_no") == F.col("s.main_line_no"),
            F.col("ps.main_item_no") == F.col("s.main_item_no"),
        ],
        how="inner",
    )
    .select(
        F.col("ps.main_prod"),
        F.col("ps.main_line_no"),
        F.col("ps.main_item_no"),
        F.col("s.main_item_location"),
        F.col("s.main_created_on"),
        F.col("s.main_line_qty"),
        F.col("s.wre_total_qty"),
        F.when(F.col("s.main_line_qty") == 0, F.lit(None))
         .otherwise((F.col("s.wre_total_qty") / F.col("s.main_line_qty")) * F.lit(100.0))
         .cast("decimal(18,2)").alias("wre_pct_line"),
        F.col("ps.wre_prod"),
        F.col("ps.wre_line_no"),
        F.col("ps.wre_item_no"),
        F.col("ps.wre_line_qty"),
        F.col("ps.main_status"),
        F.col("ps.wre_status"),
    )
    .alias("f")
    .join(
        ts_windowed.alias("wm"),
        on=[
            F.col("f.main_prod") == F.col("wm.prod_order_no"),
            F.col("f.main_line_no") == F.col("wm.prod_order_line_no"),
        ],
        how="left",
    )
    .select("f.*", F.col("wm.change_ts").alias("change_ts"))
)

# Deterministic row_id (MERGE key)
row_id_cols = ["main_prod", "main_line_no", "main_item_no", "wre_prod", "wre_line_no"]
final_df = final_df.withColumn(
    "row_id",
    F.sha2(
        F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in row_id_cols]),
        256,
    ),
)

# ==========================================================
# 4) Write (create or merge)
# ==========================================================
def merge_or_create(target, df):
    if not table_exists(target):
        print(f"Creating {target} ...")
        (df.write
           .format("delta")
           .mode("overwrite")
           .option("overwriteSchema","true")
           .saveAsTable(target))
        return
    print(f"Merging into {target} ...")
    tgt = DeltaTable.forName(spark, target)
    (
        tgt.alias("t")
        .merge(df.alias("s"), "t.row_id <=> s.row_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

merge_or_create(TARGET, final_df)
print(f"✅ Done: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================================
# Job: Gold_Production_Lakehouse.prod.gold_prod_actual_time_by_employees
# Strictly mirrors SQL view dbo.v_ProdOrderRunTimes_actual
# Incremental by date(change_ts)
#
# SQL logic recapped:
#   s_from: latest "From employee" per key (ROW_NUMBER de-dup)
#   r_dedup: MAX(run_time) per (order,line,routing_no AS machine_center_no)
#   to_employee_intervals: "To employee" minute deltas per row (clamped >= 0)
#   to_employee_agg: SUM(actual_minutes) per (order,line,machine_center_no)
#   final: LEFT joins to cell list, r_dedup, to_employee_agg
#
# change_ts = greatest(s.created_on_ts, s.modified_on_ts)
# ==========================================================

from pyspark.sql import functions as F, Window as W
from delta.tables import DeltaTable
from datetime import datetime, date
from pyspark.sql.utils import AnalysisException

# -------- Sources ----------
S_SRC  = "Silver_Production_Lakehouse.prod.silver_prod_order_status"   # s
R_SRC  = "Silver_Production_Lakehouse.prod.silver_prod_routing_line"   # r
CL_SRC = "Silver_Production_Lakehouse.prod.silver_cell_list"           # l  (email → cell_line/prod_line)

# -------- Target ----------
TARGET = "Gold_Production_Lakehouse.prod.gold_prod_actual_time_by_employees"

# -------- Widgets / params ----------
def get_widget(name: str, default: str) -> str:
    try:
        import dbutils  # type: ignore
        return dbutils.widgets.get(name)  # type: ignore
    except Exception:
        return default

try:
    dbutils.widgets.text("full_reload", "false")  # type: ignore
    dbutils.widgets.text("since", "")             # yyyy-MM-dd  # type: ignore
    dbutils.widgets.text("until", "")             # yyyy-MM-dd  # type: ignore
except Exception:
    pass

full_reload = get_widget("full_reload", "false").strip().lower() == "true"
since_param = get_widget("since", "").strip()
until_param = get_widget("until", "").strip()

def parse_date_or_none(s: str):
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()

def table_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False

def pick_watermark_from_target(table_name: str):
    try:
        df = spark.table(table_name)
    except Exception:
        return None
    if "change_ts" in df.columns:
        return df.agg(F.max(F.to_date("change_ts")).alias("mx")).collect()[0]["mx"]
    return None

since_date = parse_date_or_none(since_param)
until_date = parse_date_or_none(until_param) or date.today()

if not full_reload and since_date is None and table_exists(TARGET):
    wm = pick_watermark_from_target(TARGET)
    if wm is not None:
        since_date = wm
if full_reload or since_date is None:
    since_date = date(1900, 1, 1)

print(f"Incremental window (date(change_ts)): {since_date} -> {until_date}")

# -------- Load sources ----------
s_raw  = spark.table(S_SRC).alias("s")
r_raw  = spark.table(R_SRC).alias("r")
cl_raw = spark.table(CL_SRC).alias("cl")

# -------- Build incremental scope on (prod_order_no, prod_order_line_no, machine_center_no) ----------
# We take max(modified_on) from status (both From/To) and routing per key, then greatest() to a change_ts per key.
s_ts = (
    s_raw
    .select("prod_order_no","prod_order_line_no","machine_center_no","modified_on")
    .groupBy("prod_order_no","prod_order_line_no","machine_center_no")
    .agg(F.max("modified_on").alias("s_mod"))
)

r_ts = (
    r_raw
    .select(
        F.col("prod_order_no").alias("r_order"),
        F.col("prod_order_line_no").alias("r_line"),
        F.col("routing_no").alias("r_mc"),
        F.col("modified_on").alias("r_modified")
    )
    .groupBy("r_order","r_line","r_mc")
    .agg(F.max("r_modified").alias("r_mod"))
)

ts_joined = (
    s_ts.alias("ss")
    .join(
        r_ts.alias("rr"),
        on=[
            F.col("ss.prod_order_no")      == F.col("rr.r_order"),
            F.col("ss.prod_order_line_no") == F.col("rr.r_line"),
            F.col("ss.machine_center_no")  == F.col("rr.r_mc"),
        ],
        how="full"
    )
    .select(
        F.coalesce(F.col("ss.prod_order_no"),      F.col("rr.r_order")).alias("prod_order_no"),
        F.coalesce(F.col("ss.prod_order_line_no"), F.col("rr.r_line")).alias("prod_order_line_no"),
        F.coalesce(F.col("ss.machine_center_no"),  F.col("rr.r_mc")).alias("machine_center_no"),
        F.col("ss.s_mod").alias("s_mod"),
        F.col("rr.r_mod").alias("r_mod"),
    )
    .withColumn("change_ts", F.greatest(F.col("s_mod"), F.col("r_mod")))
)

ts_windowed = ts_joined.filter(
    (F.to_date("change_ts") >= F.lit(since_date)) &
    (F.to_date("change_ts") <= F.lit(until_date))
)

scope = ts_windowed.select("prod_order_no","prod_order_line_no","machine_center_no").distinct()

# Restrict S and R to scope (inner join on the three-key)
s_scoped = (
    s_raw.alias("s")
    .join(
        scope.alias("sc"),
        on=[
            F.col("s.prod_order_no")      == F.col("sc.prod_order_no"),
            F.col("s.prod_order_line_no") == F.col("sc.prod_order_line_no"),
            F.col("s.machine_center_no")  == F.col("sc.machine_center_no"),
        ],
        how="inner"
    )
    .select("s.*")  # keep only s columns
)

r_scoped = (
    r_raw.alias("r")
    .join(
        scope.alias("sc"),
        on=[
            F.col("r.prod_order_no")      == F.col("sc.prod_order_no"),
            F.col("r.prod_order_line_no") == F.col("sc.prod_order_line_no"),
            F.col("r.routing_no")         == F.col("sc.machine_center_no"),
        ],
        how="inner"
    )
    .select("r.*")
)

# -------- r_dedup (MAX run_time per (order,line,routing_no AS machine_center_no)) ----------
r_dedup = (
    r_scoped
    .groupBy(
        F.col("prod_order_no").alias("r_order"),
        F.col("prod_order_line_no").alias("r_line"),
        F.col("routing_no").alias("r_mc"),
    )
    .agg(F.max("run_time").alias("run_time"))
    .select(
        F.col("r_order").alias("prod_order_no"),
        F.col("r_line").alias("prod_order_line_no"),
        F.col("r_mc").alias("machine_center_no"),
        "run_time"
    )
)

# -------- s_from (latest "From employee" per partition with ROW_NUMBER) ----------
# Partition per SQL: (order, line, machine, operation_no, item_no, user_id)
# ORDER BY modified_on DESC, created_on DESC
s_from_all = (
    s_scoped
    .filter(F.col("type_name") == F.lit("From employee"))
    .withColumn("created_on_ts",  F.to_timestamp("created_on"))
    .withColumn("modified_on_ts", F.to_timestamp("modified_on"))
)

w_rn = (
    W.partitionBy(
        "prod_order_no","prod_order_line_no","machine_center_no",
        "operation_no","item_no","user_id"
    )
    .orderBy(F.col("modified_on_ts").desc_nulls_last(), F.col("created_on_ts").desc_nulls_last())
)

s_from = (
    s_from_all
    .withColumn("rn", F.row_number().over(w_rn))
    .filter(F.col("rn") == 1)
    .select(
        "created_on","modified_on","created_on_ts","modified_on_ts",
        "prod_order_no","prod_order_line_no","machine_center_no",
        "operation_no","item_no","user_id","quantity","remaining_quantity",
        "sales_order_no","current_location_code","past_location_code","employee_no"
    )
)

# -------- to_employee_intervals / agg (SUM minutes, clamp >= 0) ----------
to_filter = (
    s_scoped
    .filter(F.col("type_name") == F.lit("To employee"))
    .withColumn("to_created_on",  F.to_timestamp("created_on"))
    .withColumn("to_modified_on", F.to_timestamp("modified_on"))
)

to_minutes = F.when(
    F.col("to_modified_on").isNull() | F.col("to_created_on").isNull(), F.lit(0.0)
).when(
    F.col("to_modified_on") < F.col("to_created_on"), F.lit(0.0)
).otherwise(
    (F.col("to_modified_on").cast("long") - F.col("to_created_on").cast("long")) / 60.0
)

to_employee_intervals = (
    to_filter
    .select("prod_order_no","prod_order_line_no","machine_center_no","to_created_on","to_modified_on")
    .withColumn("actual_minutes", F.greatest(to_minutes, F.lit(0.0)))
)

to_employee_agg = (
    to_employee_intervals
    .groupBy("prod_order_no","prod_order_line_no","machine_center_no")
    .agg(F.sum("actual_minutes").alias("actual_run_time_min"))
)

# -------- Final SELECT (same columns & calc as SQL) ----------
final_df = (
    s_from.alias("s")
    .join(
        cl_raw.select(
            F.col("email_address").alias("email_address"),
            "cell_line","prod_line"
        ).alias("l"),
        F.col("s.user_id") == F.col("l.email_address"), "left"
    )
    .join(
        r_dedup.alias("r"),
        on=[
            F.col("s.prod_order_no")      == F.col("r.prod_order_no"),
            F.col("s.prod_order_line_no") == F.col("r.prod_order_line_no"),
            F.col("s.machine_center_no")  == F.col("r.machine_center_no"),
        ],
        how="left"
    )
    .join(
        to_employee_agg.alias("t"),
        on=[
            F.col("s.prod_order_no")      == F.col("t.prod_order_no"),
            F.col("s.prod_order_line_no") == F.col("t.prod_order_line_no"),
            F.col("s.machine_center_no")  == F.col("t.machine_center_no"),
        ],
        how="left"
    )
    .select(
        # Use TIMESTAMP-casted columns to avoid type mismatch in greatest()
        F.col("s.created_on_ts").alias("created_on"),
        F.col("s.modified_on_ts").alias("modified_on"),
        F.col("s.prod_order_no").alias("prod_order_no"),
        F.col("s.prod_order_line_no").alias("prod_order_line_no"),
        F.col("s.machine_center_no").alias("machine_center_no"),
        F.col("s.operation_no").alias("operation_no"),
        F.col("s.item_no").alias("item_no"),
        F.col("s.user_id").alias("user_id"),
        F.col("s.quantity").alias("quantity"),
        F.col("s.remaining_quantity").alias("remaining_quantity"),
        F.col("s.sales_order_no").alias("sales_order_no"),
        F.col("s.current_location_code").alias("current_location_code"),
        F.col("s.past_location_code").alias("past_location_code"),
        F.col("s.employee_no").alias("employee_no"),
        F.col("l.cell_line").alias("cell_line"),
        F.col("l.prod_line").alias("prod_line"),
        F.col("r.run_time").alias("plan_run_time"),
        (F.abs(F.col("s.quantity")).cast("double") * F.coalesce(F.col("r.run_time").cast("double"), F.lit(0.0))).alias("total_plan_runtime"),
        F.coalesce(F.col("t.actual_run_time_min"), F.lit(0.0)).alias("actual_run_time_min"),
        # change_ts for incremental watermark
        F.greatest(F.col("s.modified_on_ts"), F.col("s.created_on_ts")).alias("change_ts")
    )
)

# -------- Optional: normalize/trims (safe on string-like cols) ----------
def normalize(df):
    out = df
    for c in ["prod_order_no","machine_center_no","user_id","current_location_code","past_location_code","employee_no","item_no"]:
        if c in out.columns:
            out = out.withColumn(c, F.trim(F.col(c)))
    return out

final_df = normalize(final_df)

# -------- Deterministic row_id for MERGE ----------
row_id_cols = [
    "prod_order_no","prod_order_line_no","machine_center_no",
    "operation_no","item_no","user_id","created_on","modified_on"
]
final_df = final_df.withColumn(
    "row_id",
    F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in row_id_cols]), 256)
)

# -------- Create or MERGE ----------
def merge_or_create(target, df):
    if not table_exists(target):
        print(f"Creating {target} ...")
        (df.write
           .format("delta")
           .mode("overwrite")
           .option("overwriteSchema","true")
           .saveAsTable(target))
        return
    print(f"Merging into {target} ...")
    tgt = DeltaTable.forName(spark, target)
    (tgt.alias("tgt")
        .merge(df.alias("src"), "src.row_id <=> tgt.row_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

merge_or_create(TARGET, final_df)
print(f"✅ Done: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================================
# Job: Gold_Production_Lakehouse.prod.gold_prod_cum_planned_vs_actual_qty
# Logic mirrors your SQL:
#  - planned_qty: total planned quantity per day (from header)
#  - actual_output_qty: total actual quantity per day (from gold_inv_output)
#  - cum_planned_qty / cum_actual_qty: cumulative totals by date
# Incremental by date
# ==========================================================

from pyspark.sql import functions as F
from pyspark.sql import Window as W
from delta.tables import DeltaTable
from datetime import datetime, date

# ---------- Config ----------
POH_SRC = "Silver_Production_Lakehouse.prod.silver_prod_order_header"
IO_SRC  = "Gold_Production_Lakehouse.prod.gold_inv_output"
TARGET  = "Gold_Production_Lakehouse.prod.gold_prod_cum_planned_vs_actual_qty"

# ---------- Params ----------
def get_widget(name: str, default: str) -> str:
    try:
        import dbutils  # type: ignore
        return dbutils.widgets.get(name)  # type: ignore
    except Exception:
        return default

try:
    dbutils.widgets.text("full_reload", "false")
    dbutils.widgets.text("since", "")     # yyyy-MM-dd
    dbutils.widgets.text("until", "")     # yyyy-MM-dd
except Exception:
    pass

full_reload = get_widget("full_reload", "false").strip().lower() == "true"
since_param = get_widget("since", "").strip()
until_param = get_widget("until", "").strip()

def parse_date_or_none(s: str):
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()

def table_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False

def pick_watermark_from_target(table_name: str):
    try:
        df = spark.table(table_name)
    except Exception:
        return None
    if "date" in df.columns:
        return df.agg(F.max(F.col("date")).alias("mx")).collect()[0]["mx"]
    return None

since_date = parse_date_or_none(since_param)
until_date = parse_date_or_none(until_param) or date.today()

if not full_reload and since_date is None and table_exists(TARGET):
    wm = pick_watermark_from_target(TARGET)
    if wm is not None:
        since_date = wm
if full_reload or since_date is None:
    since_date = date(1900, 1, 1)

print(f"Incremental window: {since_date} -> {until_date}")

# ---------- PlannedDaily ----------
# Use header planned end date & quantity per day
POH = spark.table(POH_SRC)
planned_daily = (
    POH
    .filter(F.col("prod_order_ending_date_time").isNotNull())
    .withColumn("date", F.to_date("prod_order_ending_date_time"))
    .groupBy("date")
    .agg(F.sum(F.col("prod_order_quantity").cast("double")).alias("planned_qty"))
)

# ---------- ActualDaily ----------
IO = spark.table(IO_SRC)
actual_daily = (
    IO
    .filter(
        (F.col("entry_type_item_location") == F.lit("FIN-GOODS")) &
        (F.coalesce(F.col("posting_date"), F.col("created_on")).isNotNull())
    )
    .withColumn("date", F.to_date(F.coalesce(F.col("posting_date"), F.col("created_on"))))
    .groupBy("date")
    .agg(F.sum(F.col("entry_type_item_quantity").cast("double")).alias("actual_output_qty"))
)

# ---------- Combine Planned + Actual ----------
combined = (
    planned_daily.alias("p")
    .join(actual_daily.alias("a"), F.col("p.date") == F.col("a.date"), "full")
    .select(
        F.coalesce(F.col("p.date"), F.col("a.date")).alias("date"),
        F.coalesce(F.col("p.planned_qty"), F.lit(0.0)).alias("planned_qty"),
        F.coalesce(F.col("a.actual_output_qty"), F.lit(0.0)).alias("actual_output_qty"),
    )
)

# ---------- Add Cumulative Columns (fixed window) ----------
cum_w = W.orderBy(F.col("date")).rowsBetween(W.unboundedPreceding, W.currentRow)

final_df = (
    combined
    .withColumn("cum_planned_qty", F.sum("planned_qty").over(cum_w))
    .withColumn("cum_actual_qty", F.sum("actual_output_qty").over(cum_w))
    .filter((F.col("date") >= F.lit(since_date)) & (F.col("date") <= F.lit(until_date)))
)

# ---------- Write / Merge ----------
def merge_or_create(target, df):
    if not table_exists(target):
        print(f"Creating {target} ...")
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .saveAsTable(target)
        )
        return

    print(f"Merging into {target} ...")
    tgt = DeltaTable.forName(spark, target)
    cond = "src.date <=> tgt.date"
    (
        tgt.alias("tgt")
           .merge(df.alias("src"), cond)
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute()
    )

merge_or_create(TARGET, final_df)
print(f"✅ Done: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Planned vs Actual (Header-level, FIN-GOODS) with variance & cumulative, plus charts
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import matplotlib.pyplot as plt

# =========================
# 1) PlannedDaily (header; FIN-GOODS)
# =========================
planned_daily = spark.sql("""
SELECT
  CAST(poh.prod_order_ending_date_time AS date) AS date,
  SUM(poh.prod_order_quantity) AS planned_qty
FROM Silver_Production_Lakehouse.prod.silver_prod_order_header poh
WHERE poh.prod_order_location = 'FIN-GOODS'
GROUP BY CAST(poh.prod_order_ending_date_time AS date)
""")

# =========================
# 2) ActualDaily (sum ALL IO lines joined to headers; FIN-GOODS)
# =========================
actual_daily = spark.sql("""
SELECT
  CAST(COALESCE(io.posting_date, io.created_on) AS date) AS date,
  SUM(io.entry_type_item_quantity) AS actual_output_qty
FROM Silver_Production_Lakehouse.prod.silver_prod_order_header poh
JOIN Gold_Production_Lakehouse.prod.gold_inv_output io
  ON io.order_no = poh.prod_order_no
WHERE poh.prod_order_location = 'FIN-GOODS'
  AND io.entry_type_item_location = 'FIN-GOODS'
GROUP BY CAST(COALESCE(io.posting_date, io.created_on) AS date)
""")

# =========================
# 3) Combine + variance + cumulative (matches your SELECT)
# =========================
combined = (
    planned_daily.alias("p")
    .join(actual_daily.alias("a"), on="date", how="full")
    .select(
        F.col("date"),
        F.coalesce(F.col("p.planned_qty"), F.lit(0)).alias("planned_qty"),
        F.coalesce(F.col("a.actual_output_qty"), F.lit(0)).alias("actual_output_qty"),
    )
    .orderBy("date")
)

combined = combined.withColumn(
    "variance_qty",
    F.col("planned_qty") - F.col("actual_output_qty")
)

w = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
combined = (
    combined
    .withColumn("cum_planned_qty", F.sum("planned_qty").over(w))
    .withColumn("cum_actual_qty",  F.sum("actual_output_qty").over(w))
    .withColumn("cum_variance_qty", F.sum(F.col("variance_qty")).over(w))
)

# Cache if the table is large and you plan to reuse:
# combined.cache()

# =========================
# 4) Visuals
# =========================
pdf = combined.toPandas()

# Per-day planned vs actual
plt.figure(figsize=(12, 5))
plt.plot(pdf["date"], pdf["planned_qty"], label="Planned (Daily)")
plt.plot(pdf["date"], pdf["actual_output_qty"], label="Actual (Daily)")
plt.title("Planned vs Actual — Daily (Header = FIN-GOODS)")
plt.xlabel("Date"); plt.ylabel("Quantity")
plt.legend(); plt.xticks(rotation=45); plt.tight_layout(); plt.show()

# Cumulative planned vs actual
plt.figure(figsize=(12, 5))
plt.plot(pdf["date"], pdf["cum_planned_qty"], label="Cumulative Planned")
plt.plot(pdf["date"], pdf["cum_actual_qty"],  label="Cumulative Actual")
plt.title("Planned vs Actual — Cumulative (Header = FIN-GOODS)")
plt.xlabel("Date"); plt.ylabel("Accumulated Quantity")
plt.legend(); plt.xticks(rotation=45); plt.tight_layout(); plt.show()

# Optional: Daily variance bars (planned - actual) and cumulative variance line
plt.figure(figsize=(12, 5))
plt.bar(pdf["date"], pdf["variance_qty"], alpha=0.4, label="Variance (Daily)")
plt.plot(pdf["date"], pdf["cum_variance_qty"], label="Cumulative Variance", linewidth=2)
plt.title("Variance — Daily & Cumulative (Header = FIN-GOODS)")
plt.xlabel("Date"); plt.ylabel("Qty (Planned - Actual)")
plt.legend(); plt.xticks(rotation=45); plt.tight_layout(); plt.show()

# If you also want to inspect the aggregated table inline:
# display(spark.createDataFrame(pdf))   # Databricks


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================================
# Job: Gold_Production_Lakehouse.prod.gold_prod_efficiency  (incremental by Work_Day)
#
# Mirrors the SQL you provided:
# - cell_agg from Silver_Commons_Lakehouse.cmn.silver_employee_time
# - run_agg from Gold_Production_Lakehouse.prod.gold_prod_actual_time_by_employees
# - LEFT JOIN on Work_Day and sub_department = sub_department_Eng (trimmed)
#
# Watermark: max(Work_Day) in target
# ==========================================================

from pyspark.sql import functions as F, Window as W
from delta.tables import DeltaTable
from datetime import date, datetime

# ---------- Sources ----------
EMP_SRC = "Silver_Commons_Lakehouse.cmn.silver_employee_time"
RUN_SRC = "Gold_Production_Lakehouse.prod.gold_prod_actual_time_by_employees"

# ---------- Target ----------
TARGET = "Gold_Production_Lakehouse.prod.gold_prod_efficiency"

# ---------- Params / widgets ----------
def get_widget(name: str, default: str) -> str:
    try:
        import dbutils  # type: ignore
        return dbutils.widgets.get(name)  # type: ignore
    except Exception:
        return default

try:
    dbutils.widgets.text("full_reload", "false")  # type: ignore
    dbutils.widgets.text("since", "")             # yyyy-MM-dd  # type: ignore
    dbutils.widgets.text("until", "")             # yyyy-MM-dd  # type: ignore
except Exception:
    pass

full_reload = get_widget("full_reload", "false").strip().lower() == "true"
since_param = get_widget("since", "").strip()
until_param = get_widget("until", "").strip()

def parse_date_or_none(s: str):
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()

def table_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False

def pick_watermark_from_target(table_name: str):
    try:
        df = spark.table(table_name)
    except Exception:
        return None
    if "Work_Day" in df.columns:
        return df.agg(F.max("Work_Day").alias("mx")).collect()[0]["mx"]
    return None

since_date = parse_date_or_none(since_param)
until_date = parse_date_or_none(until_param) or date.today()

if not full_reload and since_date is None and table_exists(TARGET):
    wm = pick_watermark_from_target(TARGET)
    if wm is not None:
        since_date = wm
if full_reload or since_date is None:
    since_date = date(1900, 1, 1)

print(f"Incremental window (Work_Day): {since_date} -> {until_date}")

# ---------- Load sources ----------
emp = spark.table(EMP_SRC)
run = spark.table(RUN_SRC)

# ---------- cell_agg (mirrors your SQL) ----------
cell_agg = (
    emp
    .filter(F.col("Division_Eng") == F.lit("PRODUCTION"))
    .select(
        F.trim(F.col("sub_department_Eng")).alias("sub_department_Eng"),
        F.col("Work_Day").cast("date").alias("Work_Day"),
        F.col("Employee_Code").alias("Employee_Code"),
        F.col("late_time_in_minutes").cast("double").alias("late_time_in_minutes"),
        F.col("before_time_out_minutes").cast("double").alias("before_time_out_minutes"),
        F.col("Count_Day_Include").cast("double").alias("Count_Day_Include"),
        F.col("Count_Day_Absent").cast("double").alias("Count_Day_Absent"),
        F.col("OT_Hour_2").cast("double").alias("OT_Hour_2"),
        F.col("Total_Leave_Days").cast("double").alias("Total_Leave_Days"),
        F.col("Total_Leave_Hours").cast("double").alias("Total_Leave_Hours"),
    )
    .groupBy("sub_department_Eng", "Work_Day")
    .agg(
        F.count(F.col("Employee_Code")).alias("total_employee"),
        F.sum(F.coalesce(F.col("late_time_in_minutes"), F.lit(0.0))).alias("total_late_minutes"),
        F.sum(F.coalesce(F.col("before_time_out_minutes"), F.lit(0.0))).alias("total_early_minutes"),
        F.sum(F.coalesce(F.col("Count_Day_Include"), F.lit(0.0))).alias("total_day_include"),
        F.sum(F.coalesce(F.col("Count_Day_Absent"), F.lit(0.0))).alias("total_day_absent"),
        F.sum(F.coalesce(F.col("OT_Hour_2"), F.lit(0.0))).alias("total_ot_hours"),
        F.sum(F.coalesce(F.col("Total_Leave_Days"), F.lit(0.0))).alias("total_leave_days"),
        F.sum(F.coalesce(F.col("Total_Leave_Hours"), F.lit(0.0))).alias("total_leave_hours"),
    )
)

# ---------- run_agg (mirrors your SQL) ----------
run_agg = (
    run
    .select(
        F.col("sub_department").alias("subdep_key"),
        F.col("cell_line").alias("cellline_key"),
        F.col("prod_line"),
        F.to_date("created_on").alias("Work_Day"),
        F.abs(F.col("quantity")).cast("double").alias("qty_pos"),
        F.col("total_plan_runtime").cast("double").alias("total_plan_runtime"),
        F.col("actual_run_time_min").cast("double").alias("total_actual_run_time_min"),
    )
    .groupBy("subdep_key", "cellline_key", "prod_line", "Work_Day")
    .agg(
        F.sum("qty_pos").alias("total_quantity_positive"),
        F.sum("total_plan_runtime").alias("total_plan_runtime"),
        F.sum("total_actual_run_time_min").alias("total_actual_run_time_min"),
    )
)

# ---------- Join (exactly as your SQL: LEFT on Work_Day + subdep_key = sub_department_Eng) ----------
joined = (
    cell_agg.alias("c")
    .join(
        run_agg.alias("r"),
        on=[
            F.col("r.Work_Day") == F.col("c.Work_Day"),
            F.col("r.subdep_key") == F.col("c.sub_department_Eng"),
        ],
        how="left"
    )
    .select(
        F.col("c.sub_department_Eng").alias("sub_department_Eng"),
        F.col("c.Work_Day").alias("Work_Day"),
        F.col("r.prod_line").alias("prod_line"),
        F.col("c.total_employee").alias("total_employee"),
        F.col("c.total_late_minutes").alias("total_late_minutes"),
        F.col("c.total_early_minutes").alias("total_early_minutes"),
        F.col("c.total_day_include").alias("total_day_include"),
        F.col("c.total_day_absent").alias("total_day_absent"),
        F.col("c.total_ot_hours").alias("total_ot_hours"),
        F.col("c.total_leave_days").alias("total_leave_days"),
        F.col("c.total_leave_hours").alias("total_leave_hours"),
        F.col("r.total_quantity_positive").alias("total_quantity_positive"),
        F.col("r.total_plan_runtime").alias("total_plan_runtime"),
        F.col("r.total_actual_run_time_min").alias("total_actual_run_time_min"),
    )
)

# ---------- Incremental window on Work_Day ----------
final_df = joined.filter(
    (F.col("Work_Day") >= F.lit(since_date)) &
    (F.col("Work_Day") <= F.lit(until_date))
)

# ---------- Deterministic row_id for MERGE ----------
final_df = final_df.withColumn(
    "row_id",
    F.sha2(
        F.concat_ws(
            "||",
            F.coalesce(F.col("Work_Day").cast("string"), F.lit("")),
            F.coalesce(F.col("sub_department_Eng"), F.lit("")),
            F.coalesce(F.col("prod_line").cast("string"), F.lit("")),
        ),
        256
    )
)

# ---------- Write (create or merge) ----------
def merge_or_create(target, df):
    if not table_exists(target):
        print(f"Creating {target} ...")
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema","true")
              .saveAsTable(target)
        )
        return
    print(f"Merging into {target} ...")
    tgt = DeltaTable.forName(spark, target)
    (
        tgt.alias("t")
           .merge(
               df.alias("s"),
               "t.row_id <=> s.row_id"
           )
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute()
    )

merge_or_create(TARGET, final_df)
print(f"✅ Done: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ================================================================
# Job: Gold_Production_Lakehouse.prod.gold_prod_planned_vs_actual_qty  (incremental)
# Mirrors your final SQL (robust casting, no FIN-GOODS filters)
# - planned_qty: SUM(header.prod_order_quantity) per TRY_CONVERT(date, ending_date)
# - actual_output_qty: SUM(io.entry_type_item_quantity) per TRY_CONVERT(date, posting/created)
# - variance_qty = planned_qty - actual_output_qty
# - cum_* = cumulative totals by date
# ================================================================

from pyspark.sql import functions as F, Window as W
from delta.tables import DeltaTable
from datetime import datetime, date

# ---------- Sources ----------
POH_SRC = "Silver_Production_Lakehouse.prod.silver_prod_order_header"
IO_SRC  = "Gold_Production_Lakehouse.prod.gold_inv_output"
TARGET  = "Gold_Production_Lakehouse.prod.gold_prod_planned_vs_actual_qty"

# ---------- Parameters ----------
def get_widget(name: str, default: str) -> str:
    try:
        import dbutils  # type: ignore
        return dbutils.widgets.get(name)  # type: ignore
    except Exception:
        return default

try:
    dbutils.widgets.text("full_reload", "false")
    dbutils.widgets.text("since", "")
    dbutils.widgets.text("until", "")
except Exception:
    pass

full_reload = get_widget("full_reload", "false").strip().lower() == "true"
since_param = get_widget("since", "").strip()
until_param = get_widget("until", "").strip()

def parse_date_or_none(s: str):
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()

def table_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False

def pick_watermark_from_target(table_name: str):
    try:
        df = spark.table(table_name)
    except Exception:
        return None
    if "date" in df.columns:
        return df.agg(F.max("date").alias("mx")).collect()[0]["mx"]
    return None

since_date = parse_date_or_none(since_param)
until_date = parse_date_or_none(until_param) or date.today()

if not full_reload and since_date is None and table_exists(TARGET):
    wm = pick_watermark_from_target(TARGET)
    if wm is not None:
        since_date = wm

if full_reload or since_date is None:
    since_date = date(1900, 1, 1)

print(f"Incremental window (date): {since_date} -> {until_date}")

# ---------- Load sources ----------
poh = spark.table(POH_SRC).alias("poh")
io  = spark.table(IO_SRC).alias("io")

# ---------- PlannedDaily ----------
planned_daily = (
    poh
    .withColumn("date", F.to_date("prod_order_ending_date_time"))
    .groupBy("date")
    .agg(F.sum(F.col("prod_order_quantity")).cast("double").alias("planned_qty"))
)

# ---------- ActualDaily ----------
actual_daily = (
    poh.join(io, F.col("io.order_no") == F.col("poh.prod_order_no"), "inner")
       .withColumn("date", F.to_date(F.col("io.posting_date")))
       .groupBy("date")
       .agg(F.sum(F.col("io.total_qty")).cast("double").alias("actual_output_qty"))
)


# ---------- Combine ----------
combined = (
    planned_daily.alias("p")
    .join(actual_daily.alias("a"), F.col("p.date") == F.col("a.date"), "full")
    .select(
        F.coalesce(F.col("p.date"), F.col("a.date")).alias("date"),
        F.coalesce(F.col("p.planned_qty"), F.lit(0.0)).alias("planned_qty"),
        F.coalesce(F.col("a.actual_output_qty"), F.lit(0.0)).alias("actual_output_qty"),
    )
)

# ---------- Variance + cumulative ----------
w = W.orderBy(F.col("date")).rowsBetween(W.unboundedPreceding, W.currentRow)
final_df_all = (
    combined
    .withColumn("variance_qty", F.col("planned_qty") - F.col("actual_output_qty"))
    .withColumn("cum_planned_qty", F.sum("planned_qty").over(w))
    .withColumn("cum_actual_qty", F.sum("actual_output_qty").over(w))
    .withColumn("cum_variance_qty", F.sum(F.col("planned_qty") - F.col("actual_output_qty")).over(w))
)

# ---------- Incremental filter ----------
final_df = final_df_all.filter(
    (F.col("date") >= F.lit(since_date)) & (F.col("date") <= F.lit(until_date))
)

# ---------- Write / Merge ----------
def merge_or_create(target, df):
    if not table_exists(target):
        print(f"Creating {target} ...")
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .saveAsTable(target)
        )
        return

    print(f"Merging into {target} ...")
    tgt = DeltaTable.forName(spark, target)
    (
        tgt.alias("tgt")
           .merge(df.alias("src"), "tgt.date <=> src.date")
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute()
    )

merge_or_create(TARGET, final_df)
print(f"✅ Done: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Visualize data from Gold_Production_Lakehouse.prod.gold_prod_planned_vs_actual_qty
# with optional START_DATE and END_DATE slice

from pyspark.sql import functions as F
import matplotlib.pyplot as plt

# =========================
# CONFIG: set your desired date range here
# =========================
START_DATE = "2025-01-01"   # e.g. "2025-10-01" or None for no lower bound
END_DATE   = "2025-11-30"   # e.g. "2025-11-30" or None for no upper bound

# =========================
# 1) Load all rows, cast date, sort ascending
# =========================
df = (
    spark.table("Gold_Production_Lakehouse.prod.gold_prod_planned_vs_actual_qty")
         .select(
             F.to_date("date").alias("date"),
             "planned_qty",
             "actual_output_qty",
             "variance_qty",
             "cum_planned_qty",
             "cum_actual_qty",
             "cum_variance_qty"
         )
         .orderBy(F.col("date").asc())
         .na.fill({
             "planned_qty": 0,
             "actual_output_qty": 0,
             "variance_qty": 0,
             "cum_planned_qty": 0,
             "cum_actual_qty": 0,
             "cum_variance_qty": 0
         })
)

# =========================
# 2) Filter by date slice (if set)
# =========================
if START_DATE:
    df = df.filter(F.col("date") >= F.lit(START_DATE))
if END_DATE:
    df = df.filter(F.col("date") <= F.lit(END_DATE))

pdf = df.toPandas()

# =========================
# 3) Daily Planned vs Actual
# =========================
plt.figure(figsize=(12, 5))
plt.plot(pdf["date"], pdf["planned_qty"], label="Planned (Daily)")
plt.plot(pdf["date"], pdf["actual_output_qty"], label="Actual (Daily)")
plt.title(f"Planned vs Actual — Daily\nDate Range: {START_DATE or 'Start'} to {END_DATE or 'End'}")
plt.xlabel("Date"); plt.ylabel("Quantity")
plt.legend(); plt.xticks(rotation=45); plt.tight_layout(); plt.show()

# =========================
# 4) Cumulative Planned vs Actual
# =========================
plt.figure(figsize=(12, 5))
plt.plot(pdf["date"], pdf["cum_planned_qty"], label="Cumulative Planned")
plt.plot(pdf["date"], pdf["cum_actual_qty"],  label="Cumulative Actual")
plt.title(f"Planned vs Actual — Cumulative\nDate Range: {START_DATE or 'Start'} to {END_DATE or 'End'}")
plt.xlabel("Date"); plt.ylabel("Accumulated Quantity")
plt.legend(); plt.xticks(rotation=45); plt.tight_layout(); plt.show()

# =========================
# 5) Variance (Daily & Cumulative)
# =========================
plt.figure(figsize=(12, 5))
plt.bar(pdf["date"], pdf["variance_qty"], alpha=0.4, label="Variance (Daily)")
plt.plot(pdf["date"], pdf["cum_variance_qty"], label="Cumulative Variance", linewidth=2)
plt.title(f"Variance — Daily & Cumulative\nDate Range: {START_DATE or 'Start'} to {END_DATE or 'End'}")
plt.xlabel("Date"); plt.ylabel("Qty (Planned - Actual)")
plt.legend(); plt.xticks(rotation=45); plt.tight_layout(); plt.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ================================================================
# Job: Gold_Production_Lakehouse.prod.gold_prod_cycle_time  (safe merge)
# Mirrors v_gold_production_status_cycle_time_distinct
#   - DISTINCT join between:
#       gold_prod_status_cycle_time_header (H)
#       gold_production_status_cycle_time (S)
# Fix:
#   - Use a truly unique MERGE key (row_id built from ALL output cols)
#   - dropDuplicates(["row_id"]) before MERGE to prevent multi-match
# ================================================================

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# ---------- Sources ----------
H_SRC = "Gold_Production_Lakehouse.prod.gold_prod_status_cycle_time_header"
S_SRC = "Gold_Production_Lakehouse.prod.gold_production_status_cycle_time"

# ---------- Target ----------
TARGET = "Gold_Production_Lakehouse.prod.gold_prod_cycle_time"

# ---------- Utils ----------
def table_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False

def merge_or_create(target, df):
    if not table_exists(target):
        print(f"Creating {target} ...")
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .saveAsTable(target)
        )
        return
    print(f"Merging into {target} ...")
    tgt = DeltaTable.forName(spark, target)
    (
        tgt.alias("tgt")
           .merge(df.alias("src"), "tgt.row_id <=> src.row_id")
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute()
    )

# ---------- Load ----------
H = spark.table(H_SRC).alias("H")
S = spark.table(S_SRC).alias("S")

# ---------- Join + Distinct (mirror the view) ----------
joined = (
    H.join(
        S,
        (F.col("H.prod_order_no") == F.col("S.prod_order_no")) &
        (F.col("H.prod_order_line_no") == F.col("S.prod_order_line_no")),
        "inner"
    )
    .select(
        F.col("H.customer_name").alias("customer_name"),
        F.col("H.sales_order_no").alias("sales_order_no"),
        F.col("H.sales_order_line_no").alias("sales_order_line_no"),
        F.col("H.FG_item_no").alias("FG_item_no"),
        F.col("H.customer_no").alias("customer_no"),
        F.col("S.prod_order_no").alias("prod_order_no"),
        F.col("S.prod_order_line_no").alias("prod_order_line_no"),
        F.col("S.op_major").alias("op_major"),
        F.col("S.prod_line").alias("prod_line"),
        F.col("S.cell_line").alias("cell_line"),
        F.col("S.operation").alias("operation"),
        F.col("S.operation time").alias("operation_time"),
        F.col("S.Dead Time").alias("dead_time"),
        F.col("S.item_no").alias("item_no"),
        F.col("S.quantity").alias("quantity"),
        F.col("S.in_created").alias("in_created"),
        F.col("S.out_created").alias("out_created"),
        F.col("S.to_created").alias("to_created"),
        F.col("S.from_created").alias("from_created"),
        F.col("S.dead_to_created").alias("dead_to_created"),
        F.col("S.station time").alias("station_time"),
    )
    .distinct()
)

# ---------- Build a truly unique MERGE key ----------
row_id_cols = [
    "customer_name","sales_order_no","sales_order_line_no","FG_item_no","customer_no",
    "prod_order_no","prod_order_line_no","op_major","prod_line","cell_line","operation",
    "operation_time","dead_time","item_no","quantity",
    "in_created","out_created","to_created","from_created","dead_to_created","station_time"
]

joined = joined.withColumn(
    "row_id",
    F.sha2(
        F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in row_id_cols]),
        256
    )
)

# Extra safety: remove any accidental duplicate row_ids before MERGE
src = joined.dropDuplicates(["row_id"])

# ---------- Write / Merge ----------
merge_or_create(TARGET, src)
print(f"✅ Done: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession

# 1. Create SparkSession
spark = SparkSession.builder \
    .appName("Gold_Eff_Demo") \
    .enableHiveSupport() \
    .getOrCreate()

# 2. Sample data
data = [
    ("LINE 1", 0.12, 0.98, 0.82, 2450, 0.92, 0.97, 2, 3, 0.15, 1.72),
    ("LINE 2", 0.18, 0.95, 0.78, 2260, 0.88, 0.96, 4, 5, 0.22, 1.80),
    ("LINE 3", 0.25, 0.92, 0.74, 1940, 0.85, 0.98, 1, 6, 0.30, 1.95),
    ("LINE 4", 0.10, 0.99, 0.85, 2670, 0.95, 0.99, 0, 2, 0.10, 1.65),
    ("LINE 5", 0.33, 0.90, 0.69, 1720, 0.80, 0.94, 6, 8, 0.45, 2.10),
    ("LINE 6", 0.08, 1.00, 0.88, 2900, 0.98, 1.00, 0, 1, 0.05, 1.55),
]

columns = [
    "Line",
    "Risk_By_Line_Percent",
    "Completion_Percent",
    "Line_Utilization_Percent",
    "Output_Pcs",
    "Efficiency_Percent",
    "Attendance_Percent",
    "OT_Percent",
    "Outs_Percent",
    "Repair_Rate_Percent",
    "Cycle_Time_Per_PC"
]

# 3. Create DataFrame
df = spark.createDataFrame(data, columns)

# 4. Show DataFrame
df.show(truncate=False)

# 5. Create database if it doesn't exist
spark.sql("CREATE DATABASE IF NOT EXISTS Gold_Production_Lakehouse.prod")

# 6. Save DataFrame as table
df.write.mode("overwrite").saveAsTable("Gold_Production_Lakehouse.prod.gold_eff_demo")

# 7. Verify table creation
spark.sql("SHOW TABLES IN Gold_Production_Lakehouse.prod").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================================
# Job: Gold_Production_Lakehouse.prod.gold_from_emp_time  (FULL REPLACE)
# Mirrors your SQL view:
#   - CTE base from silver_prod_order_status (From employee)
#   - Joins routing_line, cell_list, silver_machine_center, gold_employee_time_group
#   - Adds created_date, out_qty, total_runtime_qty, and brings E.total_workhour / employee_code
# Behavior:
#   - FULL REPLACE: no incremental window, no merge/upsert
# ==========================================================

from pyspark.sql import functions as F

# ---------- Sources ----------
S_SRC  = "Silver_Production_Lakehouse.prod.silver_prod_order_status"   # S
R_SRC  = "Silver_Production_Lakehouse.prod.silver_prod_routing_line"   # R
C_SRC  = "Silver_Production_Lakehouse.prod.silver_cell_list"           # C
M_SRC  = "Silver_Commons_Lakehouse.cmn.silver_machine_center"          # M
E_SRC  = "Gold_Production_Lakehouse.prod.gold_employee_time_group"     # E

# ---------- Target ----------
TARGET = "Gold_Production_Lakehouse.prod.gold_from_emp_time"

# ---------- Load sources ----------
S_raw = spark.table(S_SRC)
R     = spark.table(R_SRC)
C     = spark.table(C_SRC)
M     = spark.table(M_SRC)
E     = spark.table(E_SRC)

# ---------- CTE base (as in SQL) ----------
base = (
    S_raw
    .select(
        F.to_date("created_on").alias("created_date"),
        "created_on",
        "modified_on",
        "prod_order_no",
        "prod_order_line_no",
        "prod_order_status",
        "type_name",
        "operation_no",
        "user_id",
        "machine_center_no",
        "employee_no",
        "antenna_id",
        "item_no",
        "quantity",
        "remaining_quantity",
        "sales_order_no",
    )
    .where(F.col("type_name") == F.lit("From employee"))
)

# ---------- Joins (mirror SQL) ----------
# Routing line: by (prod_order_no, prod_order_line_no, operation_no)
R_slim = (
    R.select(
        F.col("prod_order_no").alias("r_prod_order_no"),
        F.col("prod_order_line_no").alias("r_prod_order_line_no"),
        F.col("operation_no").alias("r_operation_no"),
        F.col("run_time").cast("double").alias("run_time"),
    )
)

# Cell list: by user_id -> email_address
C_slim = (
    C.select(
        F.col("email_address").alias("email_address"),
        "prod_line",
        "cell_line",
        "sub_department",
    )
)

# Machine center: by machine_center_no
M_slim = (
    M.select(
        "machine_center_no",
        "machine_employee_mapping"
    )
)

# gold_employee_time_group: join on date/sub_department/antenna
E_slim = (
    E.select(
        F.col("Work_Day").alias("e_work_day"),
        F.col("sub_department").alias("e_sub_department"),
        F.col("antenna_id").alias("e_antenna_id"),
        "total_workhour",
        "employee_code"
    )
)

joined = (
    base.alias("S")
    .join(
        R_slim.alias("R"),
        (F.col("S.prod_order_no")      == F.col("R.r_prod_order_no")) &
        (F.col("S.prod_order_line_no") == F.col("R.r_prod_order_line_no")) &
        (F.col("S.operation_no")       == F.col("R.r_operation_no")),
        "left",
    )
    .join(
        C_slim.alias("C"),
        F.col("S.user_id") == F.col("C.email_address"),
        "left",
    )
    .join(
        M_slim.alias("M"),
        F.col("S.machine_center_no") == F.col("M.machine_center_no"),
        "left",
    )
    .join(
        E_slim.alias("E"),
        (F.col("S.created_date")   == F.col("E.e_work_day")) &
        (F.col("C.sub_department") == F.col("E.e_sub_department")) &
        (F.col("S.antenna_id")     == F.col("E.e_antenna_id")),
        "left",
    )
)

# ---------- Final select (exact column set / aliases as in SQL) ----------
final_df = (
    joined
    .select(
        F.col("S.created_date").alias("created_date"),
        F.col("S.created_on").alias("created_on"),
        F.col("S.modified_on").alias("modified_on"),
        F.col("S.prod_order_no").alias("prod_order_no"),
        F.col("S.prod_order_line_no").alias("prod_order_line_no"),
        F.col("S.operation_no").alias("operation_no"),
        F.col("R.run_time").alias("run_time"),
        F.col("S.user_id").alias("user_id"),
        F.col("C.prod_line").alias("prod_line"),
        F.col("C.cell_line").alias("cell_line"),
        F.col("C.sub_department").alias("sub_department"),
        F.col("S.machine_center_no").alias("machine_center_no"),
        F.col("M.machine_employee_mapping").alias("m_group"),
        F.col("S.employee_no").alias("employee_no"),
        F.col("S.antenna_id").alias("antenna_id"),
        F.col("S.item_no").alias("item_no"),
        F.col("S.sales_order_no").alias("sales_order_no"),
        F.abs(F.col("S.quantity")).alias("out_qty"),
        (F.abs(F.col("S.quantity")).cast("double") * F.coalesce(F.col("R.run_time").cast("double"), F.lit(0.0))).alias("total_runtime_qty"),
        F.col("E.total_workhour").alias("total_workhour"),
        F.col("E.employee_code").alias("employee_code"),
    )
)

# ---------- Optional lineage/debug column ----------
final_df = final_df.withColumn(
    "change_ts",
    F.greatest(F.to_timestamp("created_on"), F.to_timestamp("modified_on"))
)

# ---------- Write (FULL REPLACE) ----------
print(f"Overwriting table: {TARGET}")
(
    final_df.write
        .format("delta")
        .mode("overwrite")                 # replace all data
        .option("overwriteSchema", "true") # update schema if needed
        .saveAsTable(TARGET)
)

print(f"✅ Full replace completed for {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ================================================================
# Job: Gold_Production_Lakehouse.prod.gold_employee_time_group  (incremental)
# Mirrors the provided SQL:
#   SELECT
#     T.Work_Day, D.Department, D.Position, D.sub_department,
#     D.Employee_Code, D.AntennaID, D.[Machine Center],
#     SUM(T.Count_Day_Include + T.OT_Hour_2) AS total_workhour
#   GROUP BY the same fields
# Notes:
#   - Incremental on Work_Day (date)
#   - Column names normalized to snake_case for AntennaID/Machine Center
#     -> antenna_id, machine_center
# ================================================================

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import date, datetime

# ---------- Sources ----------
D_SRC = "Silver_Commons_Lakehouse.cmn.silver_employee_data"   # D
T_SRC = "Silver_Commons_Lakehouse.cmn.silver_employee_time"   # T

# ---------- Target ----------
TARGET = "Gold_Production_Lakehouse.prod.gold_employee_time_group"

# ---------- Widgets / params ----------
def get_widget(name: str, default: str) -> str:
    try:
        import dbutils  # type: ignore
        return dbutils.widgets.get(name)  # type: ignore
    except Exception:
        return default

try:
    dbutils.widgets.text("full_reload", "false")  # type: ignore
    dbutils.widgets.text("since", "")             # yyyy-MM-dd  # type: ignore
    dbutils.widgets.text("until", "")             # yyyy-MM-dd  # type: ignore
except Exception:
    pass

full_reload = get_widget("full_reload", "false").strip().lower() == "true"
since_param = get_widget("since", "").strip()
until_param = get_widget("until", "").strip()

def parse_date_or_none(s: str):
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()

def table_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False

def pick_watermark_from_target(table_name: str):
    try:
        df = spark.table(table_name)
    except Exception:
        return None
    if "Work_Day" in df.columns:
        return df.agg(F.max(F.col("Work_Day")).alias("mx")).collect()[0]["mx"]
    return None

since_date = parse_date_or_none(since_param)
until_date = parse_date_or_none(until_param) or date.today()

if not full_reload and since_date is None and table_exists(TARGET):
    wm = pick_watermark_from_target(TARGET)
    if wm is not None:
        since_date = wm
if full_reload or since_date is None:
    since_date = date(1900, 1, 1)

print(f"Incremental window (Work_Day): {since_date} -> {until_date}")

# ---------- Load ----------
D = spark.table(D_SRC).alias("D")
T = spark.table(T_SRC).alias("T")

# ---------- Early window filter on Work_Day ----------
T_win = T.filter(
    (F.to_date(F.col("Work_Day")) >= F.lit(since_date)) &
    (F.to_date(F.col("Work_Day")) <= F.lit(until_date))
).alias("T")

# ---------- Join + Aggregate (SQL-equivalent) ----------
joined = (
    D.join(T_win, F.col("D.Employee_Code") == F.col("T.Employee_Code"), "inner")
)

# Ensure numeric inputs for the SUM expression
workhour_expr = F.coalesce(F.col("T.Count_Day_Include").cast("double"), F.lit(0.0)) + \
                F.coalesce(F.col("T.OT_Hour_2").cast("double"), F.lit(0.0))

group_cols = [
    F.col("D.`Machine Center`").alias("machine_center"),
    F.col("D.AntennaID").alias("antenna_id"),
    F.col("D.Department").alias("department"),
    F.col("D.Position").alias("position"),
    F.col("D.sub_department").alias("sub_department"),
    F.col("D.Employee_Code").alias("employee_code"),
    F.to_date(F.col("T.Work_Day")).alias("Work_Day"),
]

agg_df = (
    joined
    .groupBy(*group_cols)
    .agg(F.sum(workhour_expr).alias("total_workhour"))
)

# ---------- Final select + optional trimming ----------
def trim_if_string(c):
    return F.when(F.col(c).isNotNull(), F.trim(F.col(c))).otherwise(F.col(c)).alias(c)

final_df = (
    agg_df.select(
        F.col("Work_Day"),
        trim_if_string("department"),
        trim_if_string("position"),
        trim_if_string("sub_department"),
        trim_if_string("employee_code"),
        trim_if_string("antenna_id"),
        trim_if_string("machine_center"),
        F.col("total_workhour").cast("double").alias("total_workhour"),
    )
)

# ---------- Deterministic row_id (MERGE key) ----------
row_id_cols = [
    "Work_Day", "department", "position", "sub_department",
    "employee_code", "antenna_id", "machine_center"
]
final_df = final_df.withColumn(
    "row_id",
    F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in row_id_cols]), 256)
)

# ---------- Write / Merge ----------
def merge_or_create(target, df):
    if not table_exists(target):
        print(f"Creating {target} ...")
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema","true")
              .saveAsTable(target)
        )
        return
    print(f"Merging into {target} ...")
    tgt = DeltaTable.forName(spark, target)
    (
        tgt.alias("tgt")
           .merge(df.alias("src"), "tgt.row_id <=> src.row_id")
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute()
    )

merge_or_create(TARGET, final_df)
print(f"✅ Done: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ================================================================
# Job: Gold_Production_Lakehouse.prod.gold_rfid_transaction (incremental)
# ================================================================

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime, date

# ---------- Sources ----------
S_SRC = "Silver_Production_Lakehouse.prod.silver_prod_order_status"  # S
C_SRC = "Silver_Production_Lakehouse.prod.silver_cell_list"          # C
M_SRC = "Silver_Commons_Lakehouse.cmn.silver_machine_center"         # M

# ---------- Target ----------
TARGET = "Gold_Production_Lakehouse.prod.gold_rfid_transaction"

# ---------- Widgets / params ----------
def get_widget(name: str, default: str) -> str:
    try:
        import dbutils  # type: ignore
        return dbutils.widgets.get(name)  # type: ignore
    except Exception:
        return default

try:
    dbutils.widgets.text("full_reload", "false")  # type: ignore
    dbutils.widgets.text("since", "")             # yyyy-MM-dd  # type: ignore
    dbutils.widgets.text("until", "")             # yyyy-MM-dd  # type: ignore
except Exception:
    pass

full_reload = get_widget("full_reload", "false").strip().lower() == "true"
since_param = get_widget("since", "").strip()
until_param = get_widget("until", "").strip()

def parse_date_or_none(s: str):
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()

def table_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False

def pick_watermark_from_target(table_name: str):
    try:
        df = spark.table(table_name)
    except Exception:
        return None
    if "change_ts" in df.columns:
        return df.agg(F.max(F.to_date("change_ts"))).collect()[0][0]
    elif "created_date" in df.columns:
        return df.agg(F.max("created_date")).collect()[0][0]
    return None

since_date = parse_date_or_none(since_param)
until_date = parse_date_or_none(until_param) or date.today()

if not full_reload and since_date is None and table_exists(TARGET):
    wm = pick_watermark_from_target(TARGET)
    if wm is not None:
        since_date = wm

if full_reload or since_date is None:
    since_date = date(1900, 1, 1)

print(f"Incremental window (date(change_ts)): {since_date} -> {until_date}")

# ---------- Load sources ----------
S = spark.table(S_SRC).alias("S")
C = spark.table(C_SRC).alias("C")
M = spark.table(M_SRC).alias("M")

# ---------- Base casting + watermark ----------
S_cast = (
    S.withColumn("created_on_ts",  F.to_timestamp("created_on"))
     .withColumn("modified_on_ts", F.to_timestamp("modified_on"))
     .withColumn("created_date",   F.to_date("created_on_ts"))
     .withColumn("change_ts",      F.greatest("created_on_ts", "modified_on_ts"))
)

S_win = S_cast.filter(
    (F.to_date("change_ts") >= F.lit(since_date)) &
    (F.to_date("change_ts") <= F.lit(until_date))
)

# ---------- Joins ----------
C_slim = C.select(
    F.col("email_address").alias("c_email"),
    "prod_line","cell_line","sub_department"
)
M_slim = M.select("machine_center_no","machine_employee_mapping")

joined = (
    S_win.alias("S")
        .join(C_slim.alias("C"), F.col("S.user_id") == F.col("C.c_email"), "left")
        .join(M_slim.alias("M"), F.col("S.machine_center_no") == F.col("M.machine_center_no"), "left")
)

# ---------- Derived fields (PySpark-safe) ----------
rfid_trim = F.trim(F.col("S.rfid_transaction_name"))
has_rfid  = (rfid_trim.isNotNull()) & (rfid_trim != "")

is_scanned_int = F.when(has_rfid, F.lit(1)).otherwise(F.lit(0))
scan_status    = F.when(has_rfid, F.lit("Scanned")).otherwise(F.lit("Not scanned"))

move_direction = (
    F.when(F.col("S.type_name").like("%Out location%"),  F.lit("OUT_LOC"))
     .when(F.col("S.type_name").like("%In location%"),   F.lit("IN_LOC"))
     .when(F.col("S.type_name").like("%To employee%"),   F.lit("TO_EMP"))
     .when(F.col("S.type_name").like("%From employee%"), F.lit("FROM_EMP"))
     .otherwise(F.lit("OTHER"))
)

month_start  = F.to_date(F.date_trunc("month", F.col("S.created_on_ts")))
weekday_name = F.date_format(F.col("S.created_on_ts"), "EEEE")
created_hour = F.hour(F.col("S.created_on_ts"))
operation_no_int = F.col("S.operation_no").cast("int")

# ---------- Final select ----------
final_df = (
    joined.select(
        # original
        F.col("S.created_date").alias("created_date"),
        F.col("S.created_on_ts").alias("created_on"),
        F.col("S.modified_on_ts").alias("modified_on"),
        F.col("S.prod_order_no").alias("prod_order_no"),
        F.col("S.prod_order_line_no").alias("prod_order_line_no"),
        F.col("S.prod_order_status").alias("prod_order_status"),
        F.col("S.type_name").alias("type_name"),
        F.col("S.operation_no").alias("operation_no"),
        F.col("S.user_id").alias("user_id"),
        F.col("S.rfid_transaction_name").alias("rfid_transaction_name"),
        F.col("S.machine_center_no").alias("machine_center_no"),
        F.col("S.employee_no").alias("employee_no"),
        F.col("S.antenna_id").alias("antenna_id"),
        F.col("S.item_no").alias("item_no"),
        F.col("S.quantity").alias("quantity"),
        F.col("S.remaining_quantity").alias("remaining_quantity"),
        F.col("S.sales_order_no").alias("sales_order_no"),
        F.col("C.prod_line").alias("prod_line"),
        F.col("C.cell_line").alias("cell_line"),
        F.col("C.sub_department").alias("sub_department"),
        F.col("M.machine_employee_mapping").alias("m_group"),

        # derived
        is_scanned_int.alias("is_rfid_scanned"),   # 0/1
        scan_status.alias("scan_status"),
        move_direction.alias("move_direction"),
        month_start.alias("month_start"),
        weekday_name.alias("weekday_name"),
        created_hour.alias("created_hour"),
        operation_no_int.alias("operation_no_int"),

        # watermark
        F.col("S.change_ts").alias("change_ts")
    )
)

# ---------- MERGE key ----------
row_id_cols = [
    "prod_order_no","prod_order_line_no","operation_no",
    "user_id","machine_center_no","employee_no","antenna_id",
    "item_no","type_name","created_on"
]
final_df = final_df.withColumn(
    "row_id",
    F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in row_id_cols]), 256)
)

# ---------- Write / Merge ----------
def merge_or_create(target, df):
    if not table_exists(target):
        print(f"Creating {target} ...")
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema","true")
              .saveAsTable(target)
        )
        return
    print(f"Merging into {target} ...")
    tgt = DeltaTable.forName(spark, target)
    (
        tgt.alias("tgt")
           .merge(df.alias("src"), "tgt.row_id <=> src.row_id")
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute()
    )

merge_or_create(TARGET, final_df)
print(f"✅ Done: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Emp Time Summary

# CELL ********************

# ==========================================================
# Job: Gold_Production_Lakehouse.prod.gold_from_emp_time_summary (incremental)
# Mirrors the provided SQL:
#   base -> joined -> orders_by_orderline -> order_agg
# + employee distinct/agg with STRING_AGG-equivalent
# Watermark: created_date (DATE(created_on))
# ==========================================================

from pyspark.sql import functions as F
from pyspark.sql import Window as W
from delta.tables import DeltaTable
from datetime import datetime, date

# ---------- Sources ----------
S_SRC = "Silver_Production_Lakehouse.prod.silver_prod_order_status"      # S
R_SRC = "Silver_Production_Lakehouse.prod.silver_prod_routing_line"      # r
C_SRC = "Silver_Production_Lakehouse.prod.silver_cell_list"              # c
M_SRC = "Silver_Commons_Lakehouse.cmn.silver_machine_center"             # m
E_SRC = "Gold_Production_Lakehouse.prod.gold_employee_time_group"        # E (employee-level rows)

# ---------- Target ----------
TARGET = "Gold_Production_Lakehouse.prod.gold_from_emp_time_summary"

# ---------- Widgets / params ----------
def get_widget(name: str, default: str) -> str:
    try:
        import dbutils  # type: ignore
        return dbutils.widgets.get(name)  # type: ignore
    except Exception:
        return default

try:
    dbutils.widgets.text("full_reload", "false")  # type: ignore
    dbutils.widgets.text("since", "")             # yyyy-MM-dd  # type: ignore
    dbutils.widgets.text("until", "")             # yyyy-MM-dd  # type: ignore
except Exception:
    pass

full_reload = get_widget("full_reload", "false").strip().lower() == "true"
since_param = get_widget("since", "").strip()
until_param = get_widget("until", "").strip()

def parse_date_or_none(s: str):
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()

def table_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False

def pick_watermark_from_target(table_name: str):
    try:
        df = spark.table(table_name)
    except Exception:
        return None
    if "created_date" in df.columns:
        return df.agg(F.max("created_date")).collect()[0][0]
    return None

since_date = parse_date_or_none(since_param)
until_date = parse_date_or_none(until_param) or date.today()

if not full_reload and since_date is None and table_exists(TARGET):
    wm = pick_watermark_from_target(TARGET)
    if wm is not None:
        since_date = wm

if full_reload or since_date is None:
    since_date = date(1900, 1, 1)

print(f"Incremental window (created_date): {since_date} -> {until_date}")

# ---------- Load sources ----------
S = spark.table(S_SRC)
R = spark.table(R_SRC)
C = spark.table(C_SRC)
M = spark.table(M_SRC)
E = spark.table(E_SRC)

# ==========================================================
# base
# ==========================================================
base = (
    S.select(
        F.to_date("created_on").alias("created_date"),
        "created_on",
        "modified_on",
        "prod_order_no",
        "prod_order_line_no",
        "prod_order_status",
        "type_name",
        "operation_no",
        "user_id",
        "machine_center_no",
        "employee_no",
        "antenna_id",
        "item_no",
        "quantity",
        "remaining_quantity",
        "sales_order_no",
    )
    .where(F.col("type_name") == F.lit("From employee"))
)

# Incremental filter
base = base.filter(
    (F.col("created_date") >= F.lit(since_date)) &
    (F.col("created_date") <= F.lit(until_date))
)

# ==========================================================
# joined
# ==========================================================
r_slim = R.select(
    F.col("prod_order_no").alias("r_prod_order_no"),
    F.col("prod_order_line_no").alias("r_prod_order_line_no"),
    F.col("operation_no").alias("r_operation_no"),
    F.col("run_time").cast("double").alias("run_time"),
)

c_slim = C.select(
    F.col("email_address").alias("email_address"),
    "prod_line",
    "cell_line",
    "sub_department",
)

m_slim = M.select(
    "machine_center_no",
    "machine_employee_mapping"
)

joined = (
    base.alias("b")
    .join(
        r_slim.alias("r"),
        (F.col("b.prod_order_no")      == F.col("r.r_prod_order_no")) &
        (F.col("b.prod_order_line_no") == F.col("r.r_prod_order_line_no")) &
        (F.col("b.operation_no")       == F.col("r.r_operation_no")),
        "left",
    )
    .join(
        c_slim.alias("c"),
        F.col("b.user_id") == F.col("c.email_address"),
        "left",
    )
    .join(
        m_slim.alias("m"),
        F.col("b.machine_center_no") == F.col("m.machine_center_no"),
        "left",
    )
    .select(
        F.col("b.created_date"),
        F.col("b.created_on"),
        F.col("b.modified_on"),
        F.col("b.prod_order_no"),
        F.col("b.prod_order_line_no"),
        F.col("b.operation_no"),
        F.col("b.user_id"),
        F.col("b.machine_center_no"),
        F.col("b.antenna_id"),
        F.col("b.item_no"),
        F.col("b.sales_order_no"),
        F.col("c.prod_line"),
        F.col("c.cell_line"),
        F.col("c.sub_department"),
        F.col("m.machine_employee_mapping").alias("m_group"),
        F.col("r.run_time"),
        F.col("b.quantity")
    )
)

# ==========================================================
# orders_by_orderline (1 row per order + line; skip operation)
# ==========================================================
orders_by_orderline = (
    joined.groupBy(
        "created_date",
        "prod_line",
        "cell_line",
        "sub_department",
        "m_group",
        "antenna_id",
        "item_no",
        "sales_order_no",
        "prod_order_no",
        "prod_order_line_no",
    )
    .agg(
        F.max(F.abs(F.col("quantity"))).alias("order_qty"),
        F.max(F.coalesce(F.col("run_time"), F.lit(0.0))).alias("run_time"),
        F.min("created_on").alias("created_on_min"),
        F.max("modified_on").alias("modified_on_max"),
    )
)

# ==========================================================
# order_agg (drop item_no / sales_order_no; roll up)
# ==========================================================
order_agg = (
    orders_by_orderline.groupBy(
        "created_date",
        "prod_line",
        "cell_line",
        "sub_department",
        "m_group",
        "antenna_id",
    )
    .agg(
        F.sum("order_qty").alias("out_qty"),
        F.sum(F.col("order_qty") * F.col("run_time")).alias("total_runtime_qty"),
        F.min("created_on_min").alias("created_on_min"),
        F.max("modified_on_max").alias("modified_on_max"),
    )
)

# ==========================================================
# emp_distinct / emp_agg  (STRING_AGG equivalent)
# ==========================================================
emp_distinct = (
    E.select(
        F.col("Work_Day").alias("created_date"),
        "sub_department",
        "antenna_id",
        "employee_code",
        "total_workhour",
    )
    .distinct()
)

emp_agg = (
    emp_distinct.groupBy("created_date", "sub_department", "antenna_id")
    .agg(
        F.sum("total_workhour").alias("total_workhour"),
        F.concat_ws(",", F.sort_array(F.collect_set("employee_code"))).alias("employee_code_list"),
    )
)

# ==========================================================
# Final SELECT (as in SQL)
# ==========================================================
final_df = (
    order_agg.alias("oa")
    .join(
        emp_agg.alias("ea"),
        on=[
            F.col("oa.created_date")   == F.col("ea.created_date"),
            F.col("oa.sub_department") == F.col("ea.sub_department"),
            F.col("oa.antenna_id")     == F.col("ea.antenna_id"),
        ],
        how="left",
    )
    .select(
        F.col("oa.created_date").alias("created_date"),
        F.col("oa.created_on_min").alias("created_on"),
        F.col("oa.modified_on_max").alias("modified_on"),
        F.col("oa.prod_line").alias("prod_line"),
        F.col("oa.cell_line").alias("cell_line"),
        F.col("oa.sub_department").alias("sub_department"),
        F.col("oa.m_group").alias("m_group"),
        F.col("oa.antenna_id").alias("antenna_id"),
        F.col("oa.out_qty").alias("out_qty"),
        F.col("oa.total_runtime_qty").alias("total_runtime_qty"),
        F.col("ea.total_workhour").alias("total_workhour"),
        F.col("ea.employee_code_list").alias("employee_code"),
    )
)

# Stable change_ts for lineage (optional)
final_df = final_df.withColumn(
    "change_ts",
    F.greatest(F.to_timestamp("created_on"), F.to_timestamp("modified_on"))
)

# Deterministic MERGE key: one row per created_date/subdep/antenna/prod/cell/m_group
key_cols = [
    "created_date","sub_department","antenna_id","prod_line","cell_line","m_group"
]
final_df = final_df.withColumn(
    "row_id",
    F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in key_cols]), 256)
)

# Deduplicate source by row_id to avoid multiple source matches in MERGE
dedup_w = W.partitionBy("row_id").orderBy(
    F.col("change_ts").desc_nulls_last(),
    F.col("created_on").desc_nulls_last()
)
final_df = (final_df
    .withColumn("rn", F.row_number().over(dedup_w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# ---------- Write (create or merge) ----------
def merge_or_create(target, df):
    if not table_exists(target):
        print(f"Creating {target} ...")
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema","true")
              .saveAsTable(target)
        )
        return
    print(f"Merging into {target} ...")
    tgt = DeltaTable.forName(spark, target)
    (
        tgt.alias("tgt")
           .merge(df.alias("src"), "tgt.row_id <=> src.row_id")
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute()
    )

merge_or_create(TARGET, final_df)
print(f"✅ Done: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# List all tables visible from this Lakehouse’s Spark catalog
tables = spark.catalog.listTables()

for t in tables:
    print(f"{t.database}.{t.name}  |  type={t.tableType}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import lit

df = spark.read.format("delta").load("abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/prod/cr535_prodordercomponent")

df_new = df.withColumn("cr535_expectedunits_du_tsl", lit(None).cast("DECIMAL(18,2)"))

df_new.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/prod/cr535_prodordercomponent")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------
silver_status_table = "Silver_Production_Lakehouse.prod.silver_prod_order_status"
cell_asgn_table    = "Gold_Production_Lakehouse.prod.gold_production_asgn_cell"
routing_table      = "Silver_Production_Lakehouse.prod.silver_prod_routing_line"
target_table       = "Gold_Production_Lakehouse.prod.gold_production_status_cycle_time"  # fixed: removed stray 5000

# ----------------------------------------------------------------------
# 1) BASE DATA: join + filter + shift created_on -> BKK (+7h, truncate seconds)
# ----------------------------------------------------------------------
s = spark.table(silver_status_table)
l = spark.table(cell_asgn_table)

base_data = (
    s.join(
        l,
        on=["prod_order_no", "prod_order_line_no"],
        how="left"
    )
    .where(
        (s.type_name.isin("In location in", "Out location", "To employee", "From employee")) &
        (F.trim(s.prod_order_status) == F.lit("Released"))
    )
    .select(
        s.prod_order_no,
        s.prod_order_line_no,
        s.type_name,
        s.operation_no,
        s.item_no,
        s.quantity,
        s.machine_center_no,
        l.prod_line,
        l.cell_line,
        # UTC -> BKK (+7h), truncate to seconds
        F.date_trunc("second", s.created_on + F.expr("INTERVAL 7 HOURS")).alias("created_on_bkk")
    )
)

# ----------------------------------------------------------------------
# 2) op_major: leading numeric chunk (supports '009,01', '10.02', '20A', etc.)
# ----------------------------------------------------------------------
wr = base_data.withColumn(
    "op_major",
    F.regexp_extract(F.col("operation_no"), r"^(\d+)", 1).cast("int")
)

# ----------------------------------------------------------------------
# 2.1) routing_map: prod+line+op_major -> routing_no (Machine Center only)
# ----------------------------------------------------------------------
r = spark.table(routing_table)

routing_map = (
    r.withColumn(
        "op_major",
        F.regexp_extract(F.col("operation_no"), r"^(\d+)", 1).cast("int")
    )
    .filter(F.col("operation_type") == "Machine Center")
    .groupBy("prod_order_no", "prod_order_line_no", "op_major")
    .agg(F.max("routing_no").alias("routing_no_machine_center"))
)

# ----------------------------------------------------------------------
# 2.2) join routing_map on prod_order_no + prod_order_line_no + op_major
# ----------------------------------------------------------------------
wr_with_routing = wr.join(
    routing_map,
    on=["prod_order_no", "prod_order_line_no", "op_major"],
    how="left"
)

# ----------------------------------------------------------------------
# 3) wr_ext: get next event times per type in each (prod_order_no, line, op_major)
# ----------------------------------------------------------------------
w_event = (
    Window
    .partitionBy("prod_order_no", "prod_order_line_no", "op_major")
    .orderBy("created_on_bkk")
    .rowsBetween(0, Window.unboundedFollowing)
)

wr_ext = (
    wr_with_routing
    .withColumn(
        "end_out_created",
        F.min(F.when(F.col("type_name") == "Out location", F.col("created_on_bkk"))).over(w_event)
    )
    .withColumn(
        "end_from_created",
        F.min(F.when(F.col("type_name") == "From employee", F.col("created_on_bkk"))).over(w_event)
    )
    .withColumn(
        "end_to_created",
        F.min(F.when(F.col("type_name") == "To employee", F.col("created_on_bkk"))).over(w_event)
    )
)

# ----------------------------------------------------------------------
# 4) Pair intervals by metric
# ----------------------------------------------------------------------
pair_in_out = (
    wr_ext
    .filter((F.col("type_name") == "In location in") & F.col("end_out_created").isNotNull())
    .select(
        "prod_order_no", "prod_order_line_no", "op_major",
        "prod_line", "cell_line", "machine_center_no", "routing_no_machine_center",
        F.col("created_on_bkk"),
        F.col("end_out_created").alias("end_created")
    )
)

pair_to_from = (
    wr_ext
    .filter((F.col("type_name") == "To employee") & F.col("end_from_created").isNotNull())
    .select(
        "prod_order_no", "prod_order_line_no", "op_major",
        "prod_line", "cell_line", "machine_center_no", "routing_no_machine_center",
        F.col("created_on_bkk"),
        F.col("end_from_created").alias("end_created")
    )
)

pair_in_to = (
    wr_ext
    .filter((F.col("type_name") == "In location in") & F.col("end_to_created").isNotNull())
    .select(
        "prod_order_no", "prod_order_line_no", "op_major",
        "prod_line", "cell_line", "machine_center_no", "routing_no_machine_center",
        F.col("created_on_bkk"),
        F.col("end_to_created").alias("end_created")
    )
)

# ----------------------------------------------------------------------
# 5) Build intervals (t_start → t_end) with metric
# ----------------------------------------------------------------------
intervals = (
    pair_in_out.select(
        "prod_order_no", "prod_order_line_no", "op_major",
        "prod_line", "cell_line", "machine_center_no", "routing_no_machine_center",
        F.lit("station").alias("metric"),
        F.col("created_on_bkk").alias("t_start"),
        F.col("end_created").alias("t_end")
    )
    .unionByName(
        pair_to_from.select(
            "prod_order_no", "prod_order_line_no", "op_major",
            "prod_line", "cell_line", "machine_center_no", "routing_no_machine_center",
            F.lit("operation").alias("metric"),
            F.col("created_on_bkk").alias("t_start"),
            F.col("end_created").alias("t_end")
        )
    )
    .unionByName(
        pair_in_to.select(
            "prod_order_no", "prod_order_line_no", "op_major",
            "prod_line", "cell_line", "machine_center_no", "routing_no_machine_center",
            F.lit("dead").alias("metric"),
            F.col("created_on_bkk").alias("t_start"),
            F.col("end_created").alias("t_end")
        )
    )
)

# ----------------------------------------------------------------------
# 6) metric_base: ALWAYS keep the interval info per metric
# ----------------------------------------------------------------------
metric_base = (
    intervals
    .groupBy(
        "prod_order_no", "prod_order_line_no", "op_major",
        "prod_line", "cell_line", "machine_center_no",
        "routing_no_machine_center", "metric"
    )
    .agg(
        F.min("t_start").alias("t_start"),
        F.max("t_end").alias("t_end")
    )
)

# ----------------------------------------------------------------------
# 7) Expand by day for working-slot calculation
# ----------------------------------------------------------------------
expand_days = (
    intervals
    .withColumn(
        "d",
        F.explode(F.sequence(F.to_date("t_start"), F.to_date("t_end")))
    )
)

# ----------------------------------------------------------------------
# 8) Working slots (Mon–Fri 08:00–12:00 & 13:00–18:20; Sat 08:00–12:00 & 13:00–17:00)
# ----------------------------------------------------------------------
slots = (
    expand_days
    .withColumn("dow", (F.datediff(F.col("d"), F.lit("1753-01-07")) % 7))
    .filter((F.col("dow") >= 1) & (F.col("dow") <= 6))  # Mon–Sat
    .withColumn("base_ts", F.col("d").cast("timestamp"))
    .withColumn("am_start", F.col("base_ts") + F.expr("INTERVAL 8 HOURS"))
    .withColumn("am_end",   F.col("base_ts") + F.expr("INTERVAL 12 HOURS"))
    .withColumn("pm_start", F.col("base_ts") + F.expr("INTERVAL 13 HOURS"))
    .withColumn(
        "pm_end",
        F.when(
            F.col("dow").between(1, 5),
            F.col("base_ts") + F.expr("INTERVAL 18 HOURS") + F.expr("INTERVAL 20 MINUTES")
        ).otherwise(F.col("base_ts") + F.expr("INTERVAL 17 HOURS"))
    )
)

# ----------------------------------------------------------------------
# 9) Clip to working minutes per day/slot (AM + PM)
# ----------------------------------------------------------------------
clip = (
    slots
    # AM
    .withColumn(
        "am_start_eff",
        F.when(
            (F.to_date("t_start") == F.col("d")) & (F.col("t_start") > F.col("am_start")),
            F.col("t_start")
        ).otherwise(F.col("am_start"))
    )
    .withColumn(
        "am_end_eff",
        F.when(
            (F.to_date("t_end") == F.col("d")) & (F.col("t_end") < F.col("am_end")),
            F.col("t_end")
        ).otherwise(F.col("am_end"))
    )
    .withColumn(
        "am_min",
        F.when(
            F.col("am_end_eff") > F.col("am_start_eff"),
            (F.col("am_end_eff").cast("long") - F.col("am_start_eff").cast("long")) / 60
        ).otherwise(F.lit(0))
    )
    # PM
    .withColumn(
        "pm_start_eff",
        F.when(
            (F.to_date("t_start") == F.col("d")) & (F.col("t_start") > F.col("pm_start")),
            F.col("t_start")
        ).otherwise(F.col("pm_start"))
    )
    .withColumn(
        "pm_end_eff",
        F.when(
            (F.to_date("t_end") == F.col("d")) & (F.col("t_end") < F.col("pm_end")),
            F.col("t_end")
        ).otherwise(F.col("pm_end"))
    )
    .withColumn(
        "pm_min",
        F.when(
            F.col("pm_end_eff") > F.col("pm_start_eff"),
            (F.col("pm_end_eff").cast("long") - F.col("pm_start_eff").cast("long")) / 60
        ).otherwise(F.lit(0))
    )
    .select(
        "prod_order_no", "prod_order_line_no", "op_major",
        "prod_line", "cell_line", "machine_center_no",
        "routing_no_machine_center",
        "metric", "t_start", "t_end", "d",
        F.col("am_min").cast("int").alias("am_min"),
        F.col("pm_min").cast("int").alias("pm_min")
    )
)

# ----------------------------------------------------------------------
# 10) metric_work: aggregate working minutes where there are slots
# ----------------------------------------------------------------------
metric_work = (
    clip
    .groupBy(
        "prod_order_no", "prod_order_line_no", "op_major",
        "prod_line", "cell_line", "machine_center_no",
        "routing_no_machine_center", "metric"
    )
    .agg(F.sum(F.col("am_min") + F.col("pm_min")).alias("work_min"))
)

# ----------------------------------------------------------------------
# 11) metric_sum: join base intervals with working minutes (COALESCE to 0)
# ----------------------------------------------------------------------
metric_sum = (
    metric_base.alias("b")
    .join(
        metric_work.alias("w"),
        on=[
            "prod_order_no", "prod_order_line_no", "op_major",
            "prod_line", "cell_line", "machine_center_no",
            "routing_no_machine_center", "metric"
        ],
        how="left"
    )
    .select(
        "b.prod_order_no", "b.prod_order_line_no", "b.op_major",
        "b.prod_line", "b.cell_line", "b.machine_center_no",
        "b.routing_no_machine_center", "b.metric", "b.t_start", "b.t_end",
        F.coalesce(F.col("work_min"), F.lit(0)).alias("work_min")
    )
)

# ----------------------------------------------------------------------
# 12) FINAL RESULT
# ----------------------------------------------------------------------
result_df = (
    metric_sum
    .groupBy("prod_order_no", "prod_order_line_no", "op_major")
    .agg(
        F.max("prod_line").alias("prod_line"),
        F.max("cell_line").alias("cell_line"),
        F.max("machine_center_no").alias("operation"),
        F.max("routing_no_machine_center").alias("routing_no_machine_center"),
        F.max(F.when(F.col("metric") == "station",   F.col("t_start"))).alias("in_created"),
        F.max(F.when(F.col("metric") == "station",   F.col("t_end"))).alias("out_created"),
        F.max(F.when(F.col("metric") == "operation", F.col("t_start"))).alias("to_created"),
        F.max(F.when(F.col("metric") == "operation", F.col("t_end"))).alias("from_created"),
        F.max(F.when(F.col("metric") == "dead",      F.col("t_end"))).alias("dead_to_created"),
        F.sum(F.when(F.col("metric") == "station",   F.col("work_min")).otherwise(F.lit(0))).alias("station_time"),
        F.sum(F.when(F.col("metric") == "operation", F.col("work_min")).otherwise(F.lit(0))).alias("operation_time"),
        F.sum(F.when(F.col("metric") == "dead",      F.col("work_min")).otherwise(F.lit(0))).alias("dead_time")
    )
)

# ----------------------------------------------------------------------
# 13) FULL REPLACE WRITE
# ----------------------------------------------------------------------
(
    result_df
    .write
    .format("delta")
    .mode("overwrite")                  # full replace
    .option("overwriteSchema", "true")  # keep schema in sync
    .saveAsTable(target_table)
)

print(f"✅ Full replace completed: {target_table}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ================================================================
# Job: Gold_Production_Lakehouse.prod.gold_production_routing_group (incremental)
#
# Mirrors SQL view [dbo].[prod_routing_group]:
#
# WITH src AS (
#   SELECT
#       t.created_on, t.modified_on, t.prod_order_no, t.prod_order_status,
#       t.prod_order_line_no, t.item_no, t.operation_no, t.operation_type,
#       t.routing_no, t.run_time,
#       operation_group = CASE WHEN operation_no IS NULL THEN NULL
#                              ELSE TRY_CONVERT(int, LEFT(operation_no, CHARINDEX('.', operation_no + '.') - 1))
#                         END
#   FROM silver_prod_routing_line t
# )
# , calc AS (
#   SELECT
#     src.*,
#     routing_no_machine_center = CASE WHEN operation_type='Machine Center' THEN routing_no ELSE NULL END,
#     routing_no_work_center    = MAX(CASE WHEN operation_type='Work Center' THEN routing_no END)
#                                   OVER (PARTITION BY prod_order_no, prod_order_line_no, operation_group)
#   FROM src
# )
# SELECT *,
#        CONCAT(prod_order_no, prod_order_line_no, operation_group) AS rol
# FROM calc
# WHERE operation_type='Machine Center'
#    OR (operation_group=9 AND operation_no='009')
#
# Incremental strategy:
#   - change_ts = greatest(created_on, modified_on) as timestamp
#   - watermark on date(change_ts)
#   - MERGE on row_id (hash of prod_order_no/line/operation_no/routing_no/operation_type)
# ================================================================

from pyspark.sql import functions as F
from pyspark.sql import Window as W
from delta.tables import DeltaTable
from datetime import datetime, date

# ---------- Source ----------
T_SRC = "Silver_Production_Lakehouse.prod.silver_prod_routing_line"

# ---------- Target ----------
TARGET = "Gold_Production_Lakehouse.prod.gold_production_routing_group"

# ---------- Widgets / params ----------
def get_widget(name: str, default: str) -> str:
    try:
        import dbutils  # type: ignore
        return dbutils.widgets.get(name)  # type: ignore
    except Exception:
        return default

try:
    dbutils.widgets.text("full_reload", "false")  # type: ignore
    dbutils.widgets.text("since", "")             # yyyy-MM-dd  # type: ignore
    dbutils.widgets.text("until", "")             # yyyy-MM-dd  # type: ignore
except Exception:
    pass

full_reload = get_widget("full_reload", "false").strip().lower() == "true"
since_param = get_widget("since", "").strip()
until_param = get_widget("until", "").strip()

def parse_date_or_none(s: str):
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()

def table_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False

def pick_watermark_from_target(table_name: str):
    """
    Use max(to_date(change_ts)) from target as watermark.
    """
    try:
        df = spark.table(table_name)
    except Exception:
        return None
    if "change_ts" in df.columns:
        return df.agg(F.max(F.to_date("change_ts")).alias("mx")).collect()[0]["mx"]
    return None

since_date = parse_date_or_none(since_param)
until_date = parse_date_or_none(until_param) or date.today()

if not full_reload and since_date is None and table_exists(TARGET):
    wm = pick_watermark_from_target(TARGET)
    if wm is not None:
        since_date = wm

if full_reload or since_date is None:
    since_date = date(1900, 1, 1)

print(f"Incremental window (date(change_ts)): {since_date} -> {until_date}")

# ---------- Load source ----------
t_raw = spark.table(T_SRC).alias("t_raw")

# ==========================================================
# 1) Add change_ts + filter incremental window
# ==========================================================
t_ts = (
    t_raw
    .withColumn("created_on_ts",  F.to_timestamp("created_on"))
    .withColumn("modified_on_ts", F.to_timestamp("modified_on"))
    .withColumn("change_ts",      F.greatest("created_on_ts", "modified_on_ts"))
)

t_win = (
    t_ts
    .filter(
        (F.to_date("change_ts") >= F.lit(since_date)) &
        (F.to_date("change_ts") <= F.lit(until_date))
    )
)

# ==========================================================
# 2) src CTE: add operation_group
#    operation_group = int(part before '.'); e.g. '030.01' -> 30
# ==========================================================
src = (
    t_win
    .select(
        "created_on",
        "modified_on",
        "prod_order_no",
        "prod_order_status",
        "prod_order_line_no",
        "item_no",
        "operation_no",
        "operation_type",
        "routing_no",
        "run_time",
        "change_ts"
    )
    .withColumn(
        "operation_group",
        F.when(F.col("operation_no").isNull(), F.lit(None).cast("int"))
         .otherwise(
             F.substring_index("operation_no", ".", 1).cast("int")
         )
    )
)

# ==========================================================
# 3) calc CTE: routing_no_machine_center + routing_no_work_center (window MAX)
# ==========================================================
w_wc = W.partitionBy("prod_order_no", "prod_order_line_no", "operation_group")

calc = (
    src
    .withColumn(
        "routing_no_machine_center",
        F.when(F.col("operation_type") == F.lit("Machine Center"), F.col("routing_no"))
         .otherwise(F.lit(None).cast(src.schema["routing_no"].dataType))
    )
    .withColumn(
        "routing_no_work_center",
        F.max(
            F.when(F.col("operation_type") == F.lit("Work Center"), F.col("routing_no"))
        ).over(w_wc)
    )
)

# ==========================================================
# 4) Final filter + add rol (as in view)
#     rol = CONCAT(prod_order_no, prod_order_line_no, operation_group)
# ==========================================================
final_df = (
    calc
    .filter(
        (F.col("operation_type") == F.lit("Machine Center")) |
        ((F.col("operation_group") == F.lit(9)) & (F.col("operation_no") == F.lit("009")))
    )
    .withColumn(
        "rol",
        F.concat(
            F.coalesce(F.col("prod_order_no").cast("string"), F.lit("")),
            F.coalesce(F.col("prod_order_line_no").cast("string"), F.lit("")),
            F.coalesce(F.col("operation_group").cast("string"), F.lit(""))
        )
    )
)

# ==========================================================
# 5) Add row_id for MERGE (deterministic key) + dedup
# ==========================================================
row_id_cols = [
    "prod_order_no",
    "prod_order_line_no",
    "operation_no",
    "routing_no",
    "operation_type"
]

final_with_id = final_df.withColumn(
    "row_id",
    F.sha2(
        F.concat_ws(
            "||",
            *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in row_id_cols]
        ),
        256
    )
)

# ensure we keep the latest change_ts per row_id
w_dedup = W.partitionBy("row_id").orderBy(F.col("change_ts").desc_nulls_last())

final_dedup = (
    final_with_id
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# ---------- Write / Merge ----------
def merge_or_create(target, df):
    if not table_exists(target):
        print(f"Creating {target} ...")
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .saveAsTable(target)
        )
        return
    print(f"Merging into {target} ...")
    tgt = DeltaTable.forName(spark, target)
    (
        tgt.alias("tgt")
           .merge(df.alias("src"), "tgt.row_id <=> src.row_id")
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute()
    )

merge_or_create(TARGET, final_dedup)
print(f"✅ Done: {TARGET}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import base64
from openai import AzureOpenAI
from azure.identity import DefaultAzureCredential, get_bearer_token_provider
      
endpoint = os.getenv("ENDPOINT_URL", "https://ennone-gpt.openai.azure.com/openai/v1/chat/completions")
deployment = os.getenv("DEPLOYMENT_NAME", "gpt-4.1-mini")
      
# Initialize Azure OpenAI client with Entra ID authentication
token_provider = get_bearer_token_provider(
    DefaultAzureCredential(),
    "https://cognitiveservices.azure.com/.default"
)

client = AzureOpenAI(
    azure_endpoint=endpoint,
    azure_ad_token_provider=token_provider,
    api_version="2025-01-01-preview",
)


# IMAGE_PATH = "YOUR_IMAGE_PATH"
# encoded_image = base64.b64encode(open(IMAGE_PATH, 'rb').read()).decode('ascii')
chat_prompt = [
    {
        "role": "system",
        "content": [
            {
                "type": "text",
                "text": "You are an AI assistant that helps people by planning and giving insights and prediction based on the available data information."
            }
        ]
    }
]

# Include speech result if speech is enabled
messages = chat_prompt

completion = client.chat.completions.create(
    model=deployment,
    messages=messages,
    max_tokens=13107,
    temperature=0.7,
    top_p=0.95,
    frequency_penalty=0,
    presence_penalty=0,
    stop=None,
    stream=False
)

print(completion.to_json())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pip install --upgrade openai

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import Window

# Source tables
time_df = spark.table("Gold_Production_Lakehouse.prod.gold_employee_time")
emp_df  = spark.table("Gold_Production_Lakehouse.prod.gold_employee_data_daily")

# Last 7 calendar days from today
seven_days_ago = F.date_sub(F.current_date(), 7)

# 1) Daily present count per department (time table)
emp_daily = (
    time_df
    .filter(
        (F.col("Work_Day") >= seven_days_ago) &
        (F.col("Department_Eng").like("PROD  LINE %"))
    )
    .groupBy("Work_Day", "Department_Eng")
    .agg(F.countDistinct("Employee_Code").alias("present_count"))
)

# 2) Total employees per department (master, only active)
emp_master = (
    emp_df
    .filter(
        (F.col("end_date").isNull()) &
        (F.col("department").like("PROD  LINE  %"))
    )
    .groupBy("department")
    .agg(F.countDistinct("employee_code").alias("total_employees"))
)

# 3) Join + daily attendance rate
attendance = (
    emp_daily.alias("d")
    .join(
        emp_master.alias("m"),
        F.col("d.Department_Eng") == F.col("m.department"),
        "inner"
    )
    .select(
        F.col("d.Work_Day").alias("work_day"),
        F.col("d.Department_Eng").alias("department"),
        F.col("present_count"),
        F.col("total_employees"),
        (F.col("present_count") / F.col("total_employees")).alias("attendance_rate")  # 0–1
    )
)

# 4) Weekly average attendance per department (over last 7 days window)
dept_window = Window.partitionBy("department")

attendance_with_week_avg = (
    attendance
    .withColumn(
        "weekly_avg_attendance_rate",
        F.avg("attendance_rate").over(dept_window)
    )
)

# 5) Write as full-replace Delta table
(
    attendance_with_week_avg
    .write
    .format("delta")
    .mode("overwrite")                      # full replace
    .option("overwriteSchema", "true")      # update schema if needed
    .saveAsTable("Gold_Production_Lakehouse.prod.gold_delta_attendance")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Gold_Production_Lakehouse.prod.gold_delta_attendance LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import date, timedelta

# ------------------------------------------------------------------
# 1. Basic setup: source tables and date range (LAST 7 DAYS)
# ------------------------------------------------------------------
RAW_DATE_PROD = "in_created"
RAW_DATE_EMP  = "Work_Day"
DATE_COL      = "work_date"

LINE_COL_PROD_RAW = "prod_line"
LINE_COL_EMP_RAW  = "Department_Eng"
LINE_NUM_COL      = "line_num"
LINE_LABEL_COL    = "prod_line"  # final label column (from Department_Eng)

df_prod = spark.table("Gold_Production_Lakehouse.prod.gold_standard_vs_actual_time")
df_emp  = spark.table("Gold_Production_Lakehouse.prod.gold_employee_time")

# Normalize date + line on production
df_prod = (
    df_prod
    .withColumn(DATE_COL, F.to_date(F.col(RAW_DATE_PROD)))
    .withColumn(
        LINE_NUM_COL,
        F.regexp_extract(F.col(LINE_COL_PROD_RAW), r'(\d+)', 1).cast("int")
    )
)

# Normalize date + line on employee side
df_emp = (
    df_emp
    .withColumn(DATE_COL, F.to_date(F.col(RAW_DATE_EMP)))
    .withColumn(
        LINE_NUM_COL,
        F.regexp_extract(F.col(LINE_COL_EMP_RAW), r'(\d+)', 1).cast("int")
    )
)

# Date window (Python, for replaceWhere)
today_py = date.today()
week_start_py = today_py - timedelta(days=6)

start_date_str = week_start_py.isoformat()
end_date_str   = today_py.isoformat()

# Date window (Spark, for filters)
today_col = F.current_date()
week_start_col = F.date_sub(today_col, 6)

# ------------------------------------------------------------------
# 2. Productive Minutes PER DATE + LINE (sum of CELL work centers)
# ------------------------------------------------------------------
df_prod_filtered = (
    df_prod
    .filter(
        (F.col(DATE_COL).between(week_start_col, today_col)) &
        # more robust CELL filter (anywhere in string)
        (F.col("routing_no_work_center").contains("CELL"))
    )
)

df_productive = (
    df_prod_filtered
    .groupBy(DATE_COL, LINE_NUM_COL)
    .agg(
        F.sum("operation_time").cast("double").alias("productive_minutes")
    )
)

# ------------------------------------------------------------------
# 3. Employee Time PER DATE + LINE (no sub_department_Eng grouping)
#    prod_line label comes from Department_Eng
# ------------------------------------------------------------------
df_emp_filtered = (
    df_emp
    .filter(
        (F.col(DATE_COL).between(week_start_col, today_col)) &
        (F.col("Division_Eng") == "PRODUCTION") &
        (F.col("Overtime_Type") == "N") &
        (F.col("C_DayInc_D2").isNotNull()) &
        (F.col("Department_Eng").contains("PROD  LINE  ")) &
        (F.col("sub_department_Eng").contains("CELL")) &
        (
            F.col("Day_Memo_reason").isNull() |
            (~F.col("Day_Memo_reason").contains("วันหยุด"))
        )
    )
)

df_hours = (
    df_emp_filtered
    .groupBy(DATE_COL, LINE_NUM_COL)
    .agg(
        F.sum("C_DayInc_D2").cast("double").alias("total_work_min_standard_weekday"),
        F.countDistinct("Employee_Code").alias("headcount"),
        F.first("Department_Eng", ignorenulls=True).alias(LINE_LABEL_COL)  # prod_line
    )
)

# ------------------------------------------------------------------
# 4. Join → capacity minutes → line utilization
# ------------------------------------------------------------------
df_util = (
    df_productive
    .join(df_hours, on=[DATE_COL, LINE_NUM_COL], how="inner")
    .withColumn(
        "capacity_minutes",
        F.col("total_work_min_standard_weekday")
    )
    .withColumn(
        "line_utilization",
        F.when(
            F.col("capacity_minutes") != 0,
            F.col("productive_minutes") / F.col("capacity_minutes")
        )
    )
)

# ------------------------------------------------------------------
# FINAL SELECT
# ------------------------------------------------------------------
df_util = df_util.select(
    DATE_COL,
    LINE_LABEL_COL,  # Department_Eng value as prod_line
    LINE_NUM_COL,
    "headcount",
    "productive_minutes",
    "total_work_min_standard_weekday",
    "capacity_minutes",
    "line_utilization"
)

# ------------------------------------------------------------------
# 5. Write to Delta with FULL REPLACE for the 7-day window
# ------------------------------------------------------------------
target_table = "Gold_Production_Lakehouse.prod.gold_delta_line_utilization"

(
    df_util.write
    .format("delta")
    .mode("overwrite")
    .option(
        "replaceWhere",
        f"{DATE_COL} >= '{start_date_str}' AND {DATE_COL} <= '{end_date_str}'"
    )
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Gold_Production_Lakehouse.prod.gold_delta_line_utilization LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Gold_Production_Lakehouse.prod.gold_employee_time LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Gold_Production_Lakehouse.prod.gold_standard_vs_actual_time LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Gold_Production_Lakehouse.prod.gold_delta_prod_line_crew LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Gold_Production_Lakehouse.prod.gold_delta_attendance LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1) Read source tables
emp_time_df = spark.table("Gold_Production_Lakehouse.prod.gold_from_emp_time_summary")
cell_dept_df = spark.table("Gold_Production_Lakehouse.prod.gold_delta_cell_and_department")

# 2) Join on trimmed sub_department
joined_df = (
    emp_time_df.alias("et")
    .join(
        cell_dept_df.alias("cd"),
        on=F.trim(F.col("cd.sub_department")) == F.trim(F.col("et.sub_department")),
        how="left"
    )
)

# 3) Filter rows (NOT NULLs, last 1 week, department LIKE 'PROD  LINE  %')
one_week_ago = F.date_sub(F.current_date(), 7)

filtered_df = (
    joined_df
    .where(F.col("et.total_workhour").isNotNull())
    .where(F.col("et.employee_code").isNotNull())
    .where(F.col("et.first_name_thai").isNotNull())
    .where(F.col("et.created_date") >= one_week_ago)
    .where(F.col("cd.department").like("PROD  LINE  %"))
)

# 4) Base aggregation (equivalent to the CTE "base")
base_df = (
    filtered_df
    .groupBy(
        F.col("et.created_date").alias("created_date"),
        F.col("cd.department").alias("department"),
        F.col("cd.sub_department").alias("sub_department"),
        F.col("et.m_group").alias("machine_center_group"),
    )
    .agg(
        F.sum("et.out_qty").alias("total_out_qty"),
        F.sum("et.total_runtime_qty").alias("total_runtime_qty"),
        F.sum("et.total_workhour").alias("total_workhour"),
        F.concat_ws(", ", F.collect_list("et.employee_code")).alias("employee_codes"),
        F.concat_ws(", ", F.collect_list("et.first_name_thai")).alias("employee_names"),
    )
)

# 5) Window for department-level totals per day
dept_day_w = Window.partitionBy("created_date", "department")

# 6) Add efficiency columns
result_df = (
    base_df
    .withColumn(
        "efficiency_pct",
        F.when(F.col("total_workhour") == 0, F.lit(0.0))
         .otherwise(F.col("total_runtime_qty") * 100.0 / F.col("total_workhour"))
    )
    .withColumn(
        "dept_total_runtime",
        F.sum("total_runtime_qty").over(dept_day_w)
    )
    .withColumn(
        "dept_total_workhour",
        F.sum("total_workhour").over(dept_day_w)
    )
    .withColumn(
        "department_efficiency_pct",
        F.when(F.col("dept_total_workhour") == 0, F.lit(0.0))
         .otherwise(F.col("dept_total_runtime") * 100.0 / F.col("dept_total_workhour"))
    )
    .drop("dept_total_runtime", "dept_total_workhour")  # optional cleanup
)

# 7) Full replace target table
result_df.write.mode("overwrite").saveAsTable(
    "Gold_Production_Lakehouse.prod.gold_delta_efficiency_rate"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

# ---------------------------------------------------------
# 1. Config
# ---------------------------------------------------------
silver_hdr_tbl  = "Silver_Production_Lakehouse.prod.silver_prod_order_header"
silver_line_tbl = "Silver_Production_Lakehouse.prod.silver_prod_order_line"

gold_casting_header_tbl = "Gold_Production_Lakehouse.prod.gold_casting_header_line"
gold_repair_status_tbl  = "Gold_Production_Lakehouse.prod.gold_casting_repair_status"


# ---------------------------------------------------------
# 2. Build source dataframe (same logic as your SQL)
# ---------------------------------------------------------
def build_source_df():
    main = spark.table(silver_hdr_tbl).alias("main")
    ml   = spark.table(silver_line_tbl).alias("ml")
    wre  = spark.table(silver_hdr_tbl).alias("wre")
    wl   = spark.table(silver_line_tbl).alias("wl")
    c    = spark.table(gold_casting_header_tbl).alias("c")

    df = (
        main
        .join(ml, ml.prod_order_no == main.prod_order_no, "inner")
        .join(
            wre,
            (wre.ref_prod_order == main.prod_order_no) &
            (wre.prod_order_no.like("WRE%")),
            "inner"
        )
        .join(
            wl,
            (wl.prod_order_no == wre.prod_order_no) &
            (wl.item_no == ml.item_no),
            "inner"
        )
        .join(
            c,
            (c.prod_order_no == wre.prod_order_no) &
            (c.prod_order_line_no == wl.prod_order_line_no),
            "left"
        )
        .select(
            # Main side
            F.col("main.created_on").alias("main_created_on"),
            F.col("main.prod_order_no").alias("main_prod_no"),
            F.col("ml.prod_order_line_no").alias("main_line_no"),
            F.col("ml.item_no").alias("main_item_no"),
            F.col("ml.item_location").alias("main_item_location"),
            F.col("ml.prod_line_quantity").alias("main_qty"),
            F.col("ml.prod_line_finished_quantity").alias("main_finished_qty"),
            F.col("ml.prod_line_remaining_quantity").alias("main_remaining_qty"),
            F.col("ml.prod_line_start_date").alias("main_start_date"),
            F.col("ml.prod_line_end_date").alias("main_end_date"),
            F.col("ml.prod_line_due_date").alias("main_due_date"),

            # WRE side
            F.col("wre.created_on").alias("wre_created_on"),
            F.col("wre.prod_order_no").alias("wre_prod_no"),
            F.col("wl.prod_order_line_no").alias("wre_line_no"),
            F.col("wl.item_no").alias("wre_item_no"),
            F.col("wl.item_location").alias("wre_item_location"),
            F.col("wl.prod_line_quantity").alias("wre_qty"),
            F.col("wl.prod_line_finished_quantity").alias("wre_finished_qty"),
            F.col("wl.prod_line_remaining_quantity").alias("wre_remaining_qty"),
            F.col("wl.prod_line_start_date").alias("wre_start_date"),
            F.col("wl.prod_line_end_date").alias("wre_end_date"),
            F.col("wl.prod_line_due_date").alias("wre_due_date"),

            # Casting gold status
            F.col("c.casting_prod_order"),
            F.col("c.casting_tree_no"),
            F.col("c.casting_status"),
            F.col("c.casting_qty_passed"),
            F.col("c.casting_qty_reject")
        )
    )

    return df


# ---------------------------------------------------------
# 3. Detect last loaded watermark (using wre_created_on)
# ---------------------------------------------------------
try:
    gold_df = spark.table(gold_repair_status_tbl)
    gold_exists = True
    max_wre_created_loaded = gold_df.agg(F.max("wre_created_on")).collect()[0][0]
except AnalysisException:
    gold_exists = False
    max_wre_created_loaded = None


# ---------------------------------------------------------
# 4. Build incremental source
# ---------------------------------------------------------
source_full_df = build_source_df()

if max_wre_created_loaded is not None:
    source_inc_df = source_full_df.filter(
        F.col("wre_created_on") > F.lit(max_wre_created_loaded)
    )
else:
    # initial full load
    source_inc_df = source_full_df

if source_inc_df.rdd.isEmpty():
    print("No new or updated WRE records to load into gold_casting_repair_status.")
else:
    # -----------------------------------------------------
    # 5. Create or MERGE into gold_casting_repair_status
    # -----------------------------------------------------
    if not gold_exists:
        (
            source_inc_df
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(gold_repair_status_tbl)
        )
        print(f"Created and loaded: {gold_repair_status_tbl}")
    else:
        delta_gold = DeltaTable.forName(spark, gold_repair_status_tbl)

        (
            delta_gold.alias("t")
            .merge(
                source_inc_df.alias("s"),
                # natural key (one row per WRE order + line)
                "t.wre_prod_no = s.wre_prod_no AND t.wre_line_no = s.wre_line_no"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"Incrementally updated: {gold_repair_status_tbl}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

# ---------------------------------------------------------
# 1. Config
# ---------------------------------------------------------
gold_production_order_tbl  = "Gold_Production_Lakehouse.prod.gold_production_order"
silver_routing_line_tbl    = "Silver_Production_Lakehouse.prod.silver_prod_routing_line"
silver_status_tbl          = "Silver_Production_Lakehouse.prod.silver_prod_order_status"

gold_routing_runtime_tbl   = "Gold_Production_Lakehouse.prod.gold_routing_remaining_runtime"


# ---------------------------------------------------------
# 2. Build source dataframe (logic of v_gold_routing_remaining_runtime)
#    Optional watermark on _modified_any for incremental load
# ---------------------------------------------------------
def build_source_df(max_modified_any=None):
    # ---------- Header CTE ----------
    g = spark.table(gold_production_order_tbl).alias("g")

    header = g.filter(F.col("g.prod_order_status").isin("Released", "Firm Planned"))

    # Incremental filter when watermark exists
    if max_modified_any is not None:
        header = header.filter(F.col("g._modified_any") > F.lit(max_modified_any))

    header = header.select(
        F.col("g.prod_order_status"),
        F.col("g.prod_order_no"),
        F.col("g.prod_order_line_no"),
        F.col("g.sales_order_no"),
        F.col("g.sales_order_line_no"),
        F.col("g.FG_item_no"),
        F.col("g.prod_item_line"),
        F.col("g.item_routing_no"),
        F.col("g.prod_order_quantity"),
        F.col("g.prod_order_starting_date_time"),
        F.col("g.prod_order_ending_date_time"),
        F.col("g.prod_order_finished_date"),
        F.col("g.prod_order_due_date"),
        F.col("g.commit_week"),
        F.col("g.ref_prod_order"),
        F.col("g.ref_item"),
        F.col("g.prod_line_due_date"),
        F.col("g.prod_line_start_date"),
        F.col("g.prod_line_end_date"),
        F.col("g.prod_line_quantity"),
        F.col("g.prod_line_finished_quantity"),
        F.col("g.prod_line_remaining_quantity"),
        F.col("g.item_location"),
        F.col("g.SOL"),
        F.col("g.POL"),
        F.col("g.due_in"),
        F.col("g.due_status"),
        F.col("g._modified_any")
    ).alias("h")

    # ---------- RoutingPlan CTE ----------
    r = spark.table(silver_routing_line_tbl).alias("r")

    routing_plan = (
        r.join(
            header,
            (F.col("h.prod_order_no") == F.col("r.prod_order_no")) &
            (F.col("h.prod_order_line_no") == F.col("r.prod_order_line_no")),
            "inner"
        )
        .select(
            F.col("r.prod_order_no"),
            F.col("r.prod_order_line_no"),
            F.col("r.operation_no"),
            F.col("r.operation_type"),
            F.col("r.routing_no"),
            F.col("r.run_time"),
            F.col("r.item_no")
        )
        .alias("p")
    )

    # ---------- DoneOperation CTE ----------
    s = spark.table(silver_status_tbl).alias("s")
    done_operation = (
        s.select(
            F.col("s.created_on"),
            F.col("s.prod_order_no"),
            F.col("s.prod_order_line_no"),
            F.col("s.operation_no"),
            F.col("s.item_no")
        )
        .distinct()
        .alias("d")
    )

    # ---------- NextMachineCenter CTE ----------
    r1 = routing_plan.alias("r1")
    r2 = routing_plan.alias("r2")

    next_mc = (
        r1.join(
            r2,
            (F.col("r2.prod_order_no")      == F.col("r1.prod_order_no")) &
            (F.col("r2.prod_order_line_no") == F.col("r1.prod_order_line_no")) &
            (F.col("r2.operation_no")       >  F.col("r1.operation_no")) &
            (F.col("r2.operation_type")     == F.lit("Machine Center")),
            "inner"
        )
        .groupBy(
            F.col("r1.prod_order_no"),
            F.col("r1.prod_order_line_no"),
            F.col("r1.operation_no").alias("current_op")
        )
        .agg(F.min("r2.operation_no").alias("next_op"))
        .alias("nm")
    )

    # ---------- Final SELECT ----------
    p  = routing_plan.alias("p")
    d  = done_operation.alias("d")
    nm = next_mc.alias("nm")
    h2 = header.alias("h")
    mc = routing_plan.alias("mc")

    result = (
        p
        # DoneOperation
        .join(
            d,
            (F.col("d.prod_order_no")      == F.col("p.prod_order_no")) &
            (F.col("d.prod_order_line_no") == F.col("p.prod_order_line_no")) &
            (F.col("d.operation_no")       == F.col("p.operation_no")),
            "left"
        )
        # NextMachineCenter
        .join(
            nm,
            (F.col("nm.prod_order_no")      == F.col("p.prod_order_no")) &
            (F.col("nm.prod_order_line_no") == F.col("p.prod_order_line_no")) &
            (F.col("nm.current_op")         == F.col("p.operation_no")),
            "left"
        )
        # RoutingPlan as mc for next machine center
        .join(
            mc,
            (F.col("mc.prod_order_no")      == F.col("nm.prod_order_no")) &
            (F.col("mc.prod_order_line_no") == F.col("nm.prod_order_line_no")) &
            (F.col("mc.operation_no")       == F.col("nm.next_op")),
            "left"
        )
        # Header (gold_production_order)
        .join(
            h2,
            (F.col("h.prod_order_no")      == F.col("p.prod_order_no")) &
            (F.col("h.prod_order_line_no") == F.col("p.prod_order_line_no")),
            "left"
        )
        .select(
            # From DoneOperation / RoutingPlan
            F.col("d.created_on"),
            F.col("p.prod_order_no"),
            F.col("p.prod_order_line_no"),
            F.col("p.operation_no"),
            F.col("p.operation_type"),
            F.col("p.routing_no"),
            F.col("p.item_no"),

            # ✅ remaining_run_time CASE (fixed isNotNull())
            F.when(F.col("d.operation_no").isNotNull(), F.lit(0))
             .otherwise(F.col("p.run_time"))
             .alias("remaining_run_time"),

            # machine_center_used CASE
            F.when(F.col("p.operation_no").like("009%"), F.col("p.routing_no"))
             .when(
                 (F.col("p.operation_type") == F.lit("Work Center")) &
                 (F.col("p.routing_no").like("CELL%")),
                 F.col("mc.routing_no")
             )
             .otherwise(F.lit(None))
             .alias("machine_center_used"),

            # All fields from Header
            F.col("h.prod_order_status"),
            F.col("h.sales_order_no"),
            F.col("h.sales_order_line_no"),
            F.col("h.FG_item_no"),
            F.col("h.prod_item_line"),
            F.col("h.item_routing_no"),
            F.col("h.prod_order_quantity"),
            F.col("h.prod_order_starting_date_time"),
            F.col("h.prod_order_ending_date_time"),
            F.col("h.prod_order_finished_date"),
            F.col("h.prod_order_due_date"),
            F.col("h.commit_week"),
            F.col("h.ref_prod_order"),
            F.col("h.ref_item"),
            F.col("h.prod_line_due_date"),
            F.col("h.prod_line_start_date"),
            F.col("h.prod_line_end_date"),
            F.col("h.prod_line_quantity"),
            F.col("h.prod_line_finished_quantity"),
            F.col("h.prod_line_remaining_quantity"),
            F.col("h.item_location"),
            F.col("h.SOL"),
            F.col("h.POL"),
            F.col("h.due_in"),
            F.col("h.due_status"),
            F.col("h._modified_any")
        )
    )

    return result


# ---------------------------------------------------------
# 3. Get last loaded watermark from GOLD table (_modified_any)
# ---------------------------------------------------------
try:
    gold_df = spark.table(gold_routing_runtime_tbl)
    gold_exists = True
    max_modified_any_loaded = gold_df.agg(F.max("_modified_any")).collect()[0][0]
except AnalysisException:
    gold_exists = False
    max_modified_any_loaded = None


# ---------------------------------------------------------
# 4. Build incremental source data
# ---------------------------------------------------------
source_inc_df = build_source_df(max_modified_any=max_modified_any_loaded)

if source_inc_df.rdd.isEmpty():
    print("No new or updated production orders to load into gold_routing_remaining_runtime.")
else:
    # -----------------------------------------------------
    # 5. Create or MERGE into gold_routing_remaining_runtime
    # -----------------------------------------------------
    if not gold_exists:
        (
            source_inc_df
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(gold_routing_runtime_tbl)
        )
        print(f"Created and loaded: {gold_routing_runtime_tbl}")
    else:
        delta_gold = DeltaTable.forName(spark, gold_routing_runtime_tbl)

        (
            delta_gold.alias("t")
            .merge(
                source_inc_df.alias("s"),
                # one row per prod_order_no + line + operation
                "t.prod_order_no = s.prod_order_no AND "
                "t.prod_order_line_no = s.prod_order_line_no AND "
                "t.operation_no = s.operation_no"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"Incrementally updated: {gold_routing_runtime_tbl}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
