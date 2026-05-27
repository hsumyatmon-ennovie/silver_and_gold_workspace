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
# META           "id": "869b263b-1a86-424b-bd97-94bd586442b2"
# META         },
# META         {
# META           "id": "3ea0efcd-03d5-44f1-8e70-99f52a5c2a22"
# META         },
# META         {
# META           "id": "ad99fdfa-85b1-4480-9f7f-2640bfd65f24"
# META         },
# META         {
# META           "id": "ff4d6787-a716-43b6-baaf-972b7426ffa5"
# META         },
# META         {
# META           "id": "e248ea90-8431-4df2-9f29-87866bf9dd5a"
# META         },
# META         {
# META           "id": "3a130b81-98ec-4fd4-a404-95edc1f0ef1e"
# META         },
# META         {
# META           "id": "81bc6bea-77b8-46fe-9189-dcfc3cd43d2f"
# META         },
# META         {
# META           "id": "6fa25cdd-36f9-4f2e-9817-c1f4d946d4d9"
# META         },
# META         {
# META           "id": "1d620310-5acc-4534-93f9-f52f082a1887"
# META         },
# META         {
# META           "id": "785307fd-af78-4359-969a-51c937ec834b"
# META         }
# META       ]
# META     },
# META     "mirrored_db": {
# META       "known_mirrored_dbs": []
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

# CELL ********************

# Notebook: nb_silver_dataverse_pipeline_optimized

# Purpose: Optimized Dataverse Bronze-to-Silver pipeline — CU reduction rewrite

# Layer: Bronze → Silver

# Schedule: Hourly / On-demand

# Dependencies: Dataverse mirrored Bronze Delta tables

#

# CHANGELOG vs Original:

# ──────────────────────────────────────────────────────────────

# 1. Removed get_last_modified() full-table scan → uses watermark control table

# 2. Removed diagnose_dupe_keys() from production path (debug only)

# 3. Removed enforce_unique() double .count() → log-only via accumulator

# 4. Removed OPTIMIZE+VACUUM from every-table loop → separate maintenance

# 5. Fixed double apply_enum_maps() call (was called twice per table)

# 6. Reduced full_load_repartitions default 64 → auto-scaled by row count

# 7. Added isEmpty() check instead of limit(1).count()

# 8. Consolidated metrics into single action instead of multiple .count()

# ──────────────────────────────────────────────────────────────
 
from typing import Dict, List, Optional, Callable

from pyspark.sql import SparkSession, DataFrame, functions as F, types as T

from pyspark.sql.window import Window

from pyspark.storagelevel import StorageLevel

import uuid

from datetime import datetime
 
# spark = SparkSession.builder.getOrCreate()  # uncomment if needed
 
# ==============================================================

# WATERMARK CONTROL TABLE — replaces full-table scan

# ==============================================================

WATERMARK_TABLE = "Silver_Commons_Lakehouse.meta.watermark_control"
 
 
def ensure_watermark_table():

    """Create the watermark control table if it doesn't exist."""

    spark.sql(f"""

        CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (

            table_name STRING,

            last_modified_ts TIMESTAMP,

            row_count LONG,

            last_run_ts TIMESTAMP

        ) USING DELTA

    """)
 
 
def get_last_modified_from_control(full_target: str):

    """

    Read watermark from control table — O(1) instead of full table scan.

    Falls back to None (= full load) if no entry exists.

    """

    ensure_watermark_table()

    row = (

        spark.table(WATERMARK_TABLE)

        .filter(F.col("table_name") == full_target)

        .select(F.max("last_modified_ts").alias("wm"))

        .collect()[0]

    )

    return row["wm"]
 
 
def update_watermark(full_target: str, max_ts, row_count: int):

    """Upsert watermark after successful load."""

    spark.sql(f"""

        MERGE INTO {WATERMARK_TABLE} t

        USING (SELECT

            '{full_target}' AS table_name,

            TIMESTAMP '{max_ts}' AS last_modified_ts,

            {row_count} AS row_count,

            current_timestamp() AS last_run_ts

        ) s

        ON t.table_name = s.table_name

        WHEN MATCHED THEN UPDATE SET *

        WHEN NOT MATCHED THEN INSERT *

    """)
 
 
# ==============================================================

# BASIC UTILS (unchanged)

# ==============================================================

def table_exists(full_table: str) -> bool:

    return spark.catalog.tableExists(full_table)
 
 
# ==============================================================

# TIMESTAMP UTILS (unchanged)

# ==============================================================

DEFAULT_TS_FORMATS: List[str] = [

    "yyyy-MM-dd'T'HH:mm:ss.SSSX",

    "yyyy-MM-dd'T'HH:mm:ssX",

    "yyyy-MM-dd HH:mm:ss.SSS",

    "yyyy-MM-dd HH:mm:ss",

]
 
 
def to_timestamp_multi(col_expr, formats: List[str] = None):

    fmts = formats or DEFAULT_TS_FORMATS

    return F.coalesce(*[F.to_timestamp(col_expr, fmt) for fmt in fmts])
 
 
def read_source_delta(path: str, sink_modified_on: str, created_src_col: str) -> DataFrame:

    df = spark.read.format("delta").load(path)

    cols_lower = {c.lower(): c for c in df.columns}

    smo = cols_lower.get(sink_modified_on.lower())

    csc = cols_lower.get(created_src_col.lower())

    if smo is None and csc is None:

        raise ValueError(

            f"Neither '{sink_modified_on}' nor '{created_src_col}' exist in source {path}"

        )

    raw = F.coalesce(F.col(smo)) if smo else F.lit(None)

    raw = F.coalesce(raw, F.col(csc)) if csc else raw

    return df.withColumn("_coalesced_mod_ts", to_timestamp_multi(raw))
 
 
def apply_watermark(df: DataFrame, last_mod, lookback_minutes: int) -> DataFrame:

    if last_mod is None:

        return df

    if "_coalesced_mod_ts" not in df.columns:

        raise ValueError("`_coalesced_mod_ts` missing; ensure read_source_delta builds it.")

    threshold = (

        F.lit(last_mod).cast("timestamp") - F.expr(f"INTERVAL {lookback_minutes} MINUTES")

    )

    return df.filter(F.col("_coalesced_mod_ts") >= threshold)
 
 
# ==============================================================

# SELECT / TYPES / SCHEMA (unchanged)

# ==============================================================

def rename_select(

    df: DataFrame, col_map: Dict[str, str], passthrough_cols: Optional[List[str]] = None

) -> DataFrame:

    passthrough_cols = passthrough_cols or []

    cols_lower = {c.lower(): c for c in df.columns}

    selected = []

    for src, dst in col_map.items():

        real_src = cols_lower.get(src.lower())

        if real_src:

            selected.append(F.col(real_src).alias(dst))

        else:

            selected.append(F.lit(None).alias(dst))

    for p in passthrough_cols:

        if p in df.columns:

            selected.append(F.col(p))

    out = df.select(*selected)

    lowered = [c.lower() for c in out.columns if c not in passthrough_cols]

    dupes = sorted({n for n in lowered if lowered.count(n) > 1})

    if dupes:

        raise ValueError(

            f"Duplicate target columns after renaming (case-insensitive): {dupes}. "

            f"Check col_map for collisions."

        )

    return out
 
 
def coerce_types_and_enrich(

    df: DataFrame,

    ts_cols: List[str],

    double_cols: List[str],

    decimal_cols: Dict[str, str],

    source_label: str = "Dataverse",

    ts_format: str = "yyyy-MM-dd'T'HH:mm:ss.SSSX",

) -> DataFrame:

    out = df

    cols_lower = {c.lower(): c for c in out.columns}

    for c in ts_cols:

        real = cols_lower.get(c.lower())

        if real:

            out = out.withColumn(real, F.to_timestamp(F.col(real), ts_format))

    for c in double_cols:

        real = cols_lower.get(c.lower())

        if real:

            out = out.withColumn(real, F.col(real).cast("double"))

    for c, dec_type in decimal_cols.items():

        real = cols_lower.get(c.lower())

        if real:

            out = out.withColumn(real, F.col(real).cast(dec_type))

    return (

        out.withColumn("load_ts", F.current_timestamp())

        .withColumn("source_system", F.lit(source_label))

    )
 
 
def ensure_columns(

    df: DataFrame,

    target_cols: List[str],

    ts_cols: List[str],

    double_cols: List[str],

    decimal_cols: Dict[str, str],

    keep_extra: bool = True,

    passthrough_cols: Optional[List[str]] = None,

) -> DataFrame:

    passthrough_cols = passthrough_cols or []

    cols_lower = {c.lower(): c for c in df.columns}

    dec_lower = {k.lower(): v for k, v in decimal_cols.items()}

    ts_lower = [x.lower() for x in ts_cols]

    dbl_lower = [x.lower() for x in double_cols]
 
    def _dtype_str(c):

        c_l = c.lower()

        if c_l in dec_lower:

            return dec_lower[c_l]

        if c_l in ts_lower or c.lower() == "load_ts":

            return "TIMESTAMP"

        if c_l in dbl_lower:

            return "DOUBLE"

        return "STRING"
 
    out = df

    for c in target_cols:

        if c.lower() not in cols_lower:

            out = out.withColumn(c, F.lit(None).cast(_dtype_str(c)))

    if keep_extra:

        current_cols = out.columns

        extra_cols = [c for c in current_cols if c not in target_cols and c not in passthrough_cols]

        return out.select(

            *target_cols,

            *[c for c in passthrough_cols if c in current_cols],

            *extra_cols,

        )

    else:

        return out.select(*target_cols)
 
 
def cast_to_target_types(df: DataFrame, ddl_types: Dict[str, str]) -> DataFrame:

    out = df

    cols_lower = {c.lower(): c for c in out.columns}

    for c, typ in ddl_types.items():

        real = cols_lower.get(c.lower())

        if real:

            out = out.withColumn(real, F.col(real).cast(typ))

    return out
 
 
def dedupe_latest(

    df: DataFrame, key_cols: List[str], order_col: str = "_coalesced_mod_ts"

) -> DataFrame:

    if order_col not in df.columns:

        raise ValueError(f"order_col '{order_col}' not found")

    w = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc_nulls_last())

    return (

        df.withColumn("_rn", F.row_number().over(w))

        .filter(F.col("_rn") == 1)

        .drop("_rn")

    )
 
 
# ==============================================================

# DATETIME SANITIZER (unchanged)

# ==============================================================

def fix_ancient_datetimes(

    df, ts_cols=None, date_cols=None, policy="null",

    floor_ts="1900-01-01 00:00:00", floor_date="1900-01-01",

):

    ts_cols = ts_cols or []

    date_cols = date_cols or []

    bad_ts_literals = [

        "0001-01-01 00:00:00", "0001-01-01T00:00:00",

        "1753-01-01 00:00:00", "1753-01-01T00:00:00",

        "1899-12-30 00:00:00", "1899-12-30T00:00:00",

    ]

    bad_date_literals = ["0001-01-01", "1753-01-01", "1899-12-30"]

    out = df

    for c in ts_cols:

        if c in out.columns:

            col_ts = F.to_timestamp(F.col(c))

            is_bad_literal = F.lower(F.col(c).cast("string")).isin(

                [s.lower() for s in bad_ts_literals]

            )

            is_too_old = col_ts < F.to_timestamp(F.lit(floor_ts))

            bad = is_bad_literal | is_too_old

            if policy == "null":

                out = out.withColumn(c, F.when(bad, F.lit(None).cast(T.TimestampType())).otherwise(col_ts))

            else:

                out = out.withColumn(c, F.when(bad, F.to_timestamp(F.lit(floor_ts))).otherwise(col_ts))

    for c in date_cols:

        if c in out.columns:

            col_date = F.to_date(F.col(c))

            is_bad_literal = F.lower(F.col(c).cast("string")).isin(

                [s.lower() for s in bad_date_literals]

            )

            is_too_old = col_date < F.to_date(F.lit(floor_date))

            bad = is_bad_literal | is_too_old

            if policy == "null":

                out = out.withColumn(c, F.when(bad, F.lit(None).cast(T.DateType())).otherwise(col_date))

            else:

                out = out.withColumn(c, F.when(bad, F.to_date(F.lit(floor_date))).otherwise(col_date))

    return out
 
 
# ==============================================================

# ENUM MAPPER (unchanged — included for completeness)

# ==============================================================

def apply_enum_map(df: DataFrame, col_name: str, mapping: dict, default_passthrough=True) -> DataFrame:

    if col_name not in df.columns:

        return df

    pairs = []

    for k, v in mapping.items():

        pairs.extend([F.lit(int(k)), F.lit(v)])

    map_expr = F.create_map(*pairs)

    mapped = F.element_at(map_expr, F.col(col_name).cast("int"))

    new_col = (

        F.coalesce(mapped, F.col(col_name).cast("string"))

        if default_passthrough

        else mapped.cast("string")

    )

    return df.withColumn(col_name, new_col.cast("string"))
 
 
def apply_enum_maps(df: DataFrame) -> DataFrame:

    """Apply all known enum mappings."""

    OPEN_MAP = {0: "No", 1: "Yes"}

    TYPE_NAME_MAP = {

        184930000: "In location in", 184930001: "Out location",

        184930002: "To employee", 184930003: "From employee",

    }

    PROD_ORDER_STATUS_MAP = {

        184930000: "Simulated", 184930001: "Planned", 184930002: "Firm Planned",

        184930003: "Released", 184930004: "Finished",

    }

    OPERATION_TYPE_MAP = {184930000: "Work Center", 184930001: "Machine Center"}

    ROUTING_STATUS_MAP = {184930000: "Planned", 184930001: "In Progress", 184930002: "Finished"}

    CASTING_OUTPUT_STATUS_MAP = {

        780350000: "Modify Qty And Add Scrap Item No", 780350001: "Consume Components",

        780350002: "Output Master Alloy And Scrap", 780350003: "Consume Master Alloy",

        780350004: "Output Casting Parts", 780350005: "Complete",

    }

    ITEM_RENDERING_MAP = {

        184930000: "Fixed Reorder Qty", 184930001: "Maximum Qty",

        184930002: "Order", 184930003: "Lot-for-Lot",

    }

    RESERVE_MAP = {184930000: "Never", 184930001: "Optional", 184930002: "Always"}

    ORDER_TRACKING_POLICY_MAP = {

        184930000: "None", 184930001: "Tracking Only", 184930002: "Tracking & Action Msg",

    }

    REPLENISHMENT_SYSTEM_MAP = {

        184930000: "Purchase", 184930001: "Prod Order",

        184930002: "Transfer", 184930003: "Assembly",

    }

    MANUFACTURING_POLICY_MAP = {184930000: "Make-to-Stock", 184930001: "Make-to-Order"}

    REPAIR_POSTING_STATUS_MAP = {

        780350000: "Create Repair PRO", 780350001: "Main PRO - Post Negative Consumption",

        780350002: "Repair PRO - Post Consumption", 780350003: "Complete",

    }

    SALES_ORDER_HEADER_STATUS_MAP = {

        0: "Open", 1: "Released", 2: "Pending Approval",

        3: "Pending Prepayment", 11: "Closed", 10: "Cancelled",

    }

    SALES_ORDER_TYPE_MAP = {

        184930000: "Quote", 184930001: "Order", 184930002: "Invoice",

        184930003: "Credit Memo", 184930004: "Blanket Order", 184930005: "Return Order",

    }

    SKETCH_STATUS_MAP = {780350000: "Active", 780350002: "On Hold", 780350003: "Cancelled"}

    SKETCH_ARCHIVE_STATUS_MAP = {0: "No", 1: "Yes"}

    SKETCH_REPAIR_STATUS_MAP = {0: "No", 1: "Yes"}

    STATECODE_MAP = {0: "No", 1: "Yes"}

    PRIORITY_MAP = {780350000: "Low", 780350001: "Medium", 780350002: "High"}

    SKETCH_MAPPING_MASTER_STATUS_MAP = {0: "No", 1: "Yes"}

    ORDER_TYPE_MAP = {184930000: "Masterpiece", 184930001: "Sample"}

    WAX_RECEIVED_MAP = {0: "No", 1: "Yes"}

    BATCH_PRINT_MAP = {0: "No", 1: "Yes"}

    MANAGER_DECISION_STATUS_MAP = {

        184930000: "Pending", 184930001: "On Hold", 184930002: "Approved",

        184930003: "Rejected", 184930004: "Missing Information",

    }

    CUSTOMER_DECISION_STATUS_MAP = {

        184930000: "Pending", 184930001: "Approved", 184930002: "Improvement",

    }

    SEND_PRINT_MAP = {0: "No", 1: "Yes"}

    SEND_WAX_MAP = {0: "No", 1: "Yes"}

    SKETCH_ORDER_TYPE_MAP = {184930000: "Masterpiece", 184930001: "Sample"}

    CAD_STATUS_MAP = {780350000: "Active", 780350002: "On Hold", 780350003: "Cancelled"}
 
    out = df

    out = apply_enum_map(out, "open", OPEN_MAP)

    out = apply_enum_map(out, "type_", TYPE_NAME_MAP)

    out = apply_enum_map(out, "type_name", TYPE_NAME_MAP)

    out = apply_enum_map(out, "prod_order_status", PROD_ORDER_STATUS_MAP)

    out = apply_enum_map(out, "operation_type", OPERATION_TYPE_MAP)

    out = apply_enum_map(out, "routing_status", ROUTING_STATUS_MAP)

    out = apply_enum_map(out, "casting_output_status", CASTING_OUTPUT_STATUS_MAP)

    out = apply_enum_map(out, "item_reordering_policy", ITEM_RENDERING_MAP)

    out = apply_enum_map(out, "item_reserve", RESERVE_MAP)

    out = apply_enum_map(out, "item_order_tracking", ORDER_TRACKING_POLICY_MAP)

    out = apply_enum_map(out, "item_replenishment", REPLENISHMENT_SYSTEM_MAP)

    out = apply_enum_map(out, "item_manufacturing_policy", MANUFACTURING_POLICY_MAP)

    out = apply_enum_map(out, "repair_posting_status", REPAIR_POSTING_STATUS_MAP)

    out = apply_enum_map(out, "sales_order_status", SALES_ORDER_HEADER_STATUS_MAP)

    out = apply_enum_map(out, "sales_order_no_delay", OPEN_MAP)

    out = apply_enum_map(out, "sales_order_type", SALES_ORDER_TYPE_MAP)

    out = apply_enum_map(out, "sketch_status", SKETCH_STATUS_MAP)

    out = apply_enum_map(out, "sketch_archive_status", SKETCH_ARCHIVE_STATUS_MAP)

    out = apply_enum_map(out, "sketch_repair_status", SKETCH_REPAIR_STATUS_MAP)

    out = apply_enum_map(out, "state_code", STATECODE_MAP)

    out = apply_enum_map(out, "sketch_piority", PRIORITY_MAP)

    out = apply_enum_map(out, "pd_sketch_item_piority", PRIORITY_MAP)

    out = apply_enum_map(out, "sketch_mapping_master_status", SKETCH_MAPPING_MASTER_STATUS_MAP)

    out = apply_enum_map(out, "wax_received", WAX_RECEIVED_MAP)

    out = apply_enum_map(out, "batch_print", BATCH_PRINT_MAP)

    out = apply_enum_map(out, "mng_decision_status", MANAGER_DECISION_STATUS_MAP)

    out = apply_enum_map(out, "customer_decision_status", CUSTOMER_DECISION_STATUS_MAP)

    out = apply_enum_map(out, "send_print", SEND_PRINT_MAP)

    out = apply_enum_map(out, "send_wax", SEND_WAX_MAP)

    out = apply_enum_map(out, "order_type", SKETCH_ORDER_TYPE_MAP)

    out = apply_enum_map(out, "cad_status", CAD_STATUS_MAP)

    return out
 
 
# ==============================================================

# DELTA OPS (unchanged)

# ==============================================================

def create_managed_table(

    target_schema, full_target, ddl_cols, ddl_types, partition_cols=None,

):

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")

    ddl = ",\n".join([f"`{c}` {ddl_types.get(c, 'STRING')}" for c in ddl_cols])

    part = (

        f" PARTITIONED BY ({', '.join([f'`{p}`' for p in (partition_cols or [])])})"

        if partition_cols else ""

    )

    spark.sql(f"CREATE TABLE IF NOT EXISTS {full_target} ({ddl}) USING DELTA{part}")
 
 
def merge_upsert(staging_view, full_target, key_cols, target_cols):

    on_expr = " AND ".join([f"(s.{k} <=> t.{k})" for k in key_cols])

    non_key_cols = [c for c in target_cols if c not in set(key_cols)]

    set_expr = ", ".join([f"t.{c}=s.{c}" for c in non_key_cols])

    spark.sql(f"""

        MERGE INTO {full_target} t

        USING {staging_view} s

        ON {on_expr}

        WHEN MATCHED THEN UPDATE SET {set_expr}

        WHEN NOT MATCHED THEN INSERT ({", ".join(target_cols)})

        VALUES ({", ".join([f"s.{c}" for c in target_cols])})

    """)
 
 
def maintain(full_target, zorder_cols=None, vacuum_hours=168):

    z_clause = ""

    if zorder_cols:

        tgt_cols = set(spark.table(full_target).columns)

        z_keep = [c for c in zorder_cols if c in tgt_cols]

        if z_keep:

            z_clause = f" ZORDER BY ({', '.join([f'`{c}`' for c in z_keep])})"

    spark.sql(f"OPTIMIZE {full_target}{z_clause}")

    spark.sql(f"VACUUM {full_target} RETAIN {vacuum_hours} HOURS")
 
 
# ==============================================================

# CORE: OPTIMIZED run_silver_table

# ==============================================================

def run_silver_table(cfg: Dict) -> Dict:

    """

    OPTIMIZED: Read → Watermark → Rename/Type → Enum → Dedupe → Seed/Merge

    Key changes from original:

    ─────────────────────────────────────────────────────

    [FIX-1] Watermark from control table, not full scan

    [FIX-2] Removed diagnose_dupe_keys() — was debug only

    [FIX-3] Removed enforce_unique() double .count()

    [FIX-4] OPTIMIZE/VACUUM moved out of per-table loop

    [FIX-5] apply_enum_maps called ONCE (not twice)

    [FIX-6] Repartition auto-scaled, not hardcoded 64

    [FIX-7] isEmpty() instead of limit(1).count()

    [FIX-8] Single persist + single aggregate for metrics

    ─────────────────────────────────────────────────────

    """

    # ── Unpack config ──

    source_path      = cfg["source_path"]

    target_schema    = cfg["target_schema"]

    target_table     = cfg["target_table"]

    full_target      = f"{target_schema}.{target_table}"

    key_cols         = cfg.get("natural_key") or cfg.get("business_key")

    col_map          = cfg["col_map"]
 
    sink_modified_on    = cfg.get("SinkModifiedOn", "modifiedon")

    created_src_col     = cfg.get("created_src_col", "createdon")

    modified_target_col = cfg.get("modified_target_col", "modified_on")

    lookback_minutes    = cfg.get("lookback_minutes", 120)

    full_load           = cfg.get("full_load", False)

    full_repartitions   = cfg.get("full_load_repartitions", None)

    ts_cols             = cfg.get("ts_cols", [])

    double_cols         = cfg.get("double_cols", [])

    decimal_cols        = cfg.get("decimal_cols", {})

    target_cols         = cfg.get("target_cols", list(col_map.values()) + ["load_ts", "source_system"])

    ddl_overrides_cfg   = cfg.get("ddl_overrides", {})

    ddl_overrides       = {**ddl_overrides_cfg, modified_target_col: "STRING"}

    custom_transform    = cfg.get("custom_transform")

    source_label        = cfg.get("load_source_label", "Dataverse")

    do_dedupe           = cfg.get("dedupe", True)
 
    # ── [FIX-4] Maintenance is OFF by default — run separately ──

    maintenance = cfg.get("maintenance", {"optimize": False})
 
    # ── 0) Validate key columns ──

    missing_key = [k for k in key_cols if k not in target_cols]

    if missing_key:

        raise ValueError(

            f"Key columns not in target_cols: {missing_key}. "

            f"Check col_map and target_cols."

        )
 
    # ── 1) Read source ──

    base = read_source_delta(source_path, sink_modified_on, created_src_col)
 
    # ── [FIX-1] Watermark from control table ──

    if full_load:

        last_mod = None

    else:

        last_mod = get_last_modified_from_control(full_target)

    base = apply_watermark(base, last_mod, lookback_minutes)
 
    # ── 2) Rename (preserve ops col) ──

    df = rename_select(base, col_map, passthrough_cols=["_coalesced_mod_ts"])
 
    # ── 2.2) Type coercion + metadata ──

    df = coerce_types_and_enrich(df, ts_cols, double_cols, decimal_cols, source_label)
 
    # ── 2.3) Sanitize ancient datetimes ──

    df = fix_ancient_datetimes(df, ts_cols=ts_cols, date_cols=[], policy="null")
 
    # ── [FIX-5] Enum maps — called ONCE ──

    # If custom_transform IS apply_enum_maps, skip the hardcoded call.

    # If custom_transform is something ELSE, call both.

    if custom_transform is apply_enum_maps:

        # Only call once

        df = apply_enum_maps(df)

    else:

        df = apply_enum_maps(df)

        if callable(custom_transform):

            df = custom_transform(df)
 
    # ── 4) Ensure schema ──

    df = ensure_columns(

        df, target_cols, ts_cols, double_cols, decimal_cols,

        keep_extra=True, passthrough_cols=["_coalesced_mod_ts"],

    )
 
    # ── 4.1) DDL types ──

    ddl_types = {c: "STRING" for c in target_cols}

    for c in ts_cols + ["load_ts"]:

        ddl_types[c] = "TIMESTAMP"

    for c in double_cols:

        ddl_types[c] = "DOUBLE"

    for c, dec in decimal_cols.items():

        ddl_types[c] = dec

    ddl_types.update(ddl_overrides)
 
    # ── 4.2) Cast types ──

    df = cast_to_target_types(df, ddl_types)
 
    # ── [FIX-6] Auto-scale repartitions ──

    if full_load and full_repartitions:

        # Let Spark decide for small tables; only repartition for large ones

        # This avoids 64 empty partitions for tables with 500 rows

        df = df.repartition(min(full_repartitions, 8))
 
    # ── 5) Dedupe (no .count() — just window) ──

    if do_dedupe:

        if "_coalesced_mod_ts" not in df.columns:

            raise ValueError("Internal `_coalesced_mod_ts` missing.")

        stg = dedupe_latest(df, key_cols, "_coalesced_mod_ts")

    else:

        stg = df
 
    # ── [FIX-8] Single persist + single isEmpty check ──

    stg = stg.persist(StorageLevel.MEMORY_AND_DISK)
 
    # ── [FIX-7] isEmpty() instead of limit(1).count() ──

    if stg.isEmpty():

        stg.unpersist()

        print(f"[SKIP] {full_target}: no changes found.")

        return {"table": full_target, "rows": 0, "mode": "skip"}
 
    # ── 6) Create table + seed or merge ──

    first_load = last_mod is None

    create_managed_table(target_schema, full_target, target_cols, ddl_types)
 
    tmp_view = f"stg_tmp_{target_table}_{uuid.uuid4().hex[:8]}"
 
    if full_load and first_load:

        stg.select(*target_cols).write.format("delta").mode("append").saveAsTable(full_target)

        mode = "seed"

    else:

        # ── [FIX-2 & FIX-3] No diagnose_dupe_keys, no enforce_unique double-count ──

        # dedupe_latest already handles uniqueness via row_number window.

        # If there are still dupes (e.g. null keys), the MERGE <=> handles it.

        stg.select(*target_cols).createOrReplaceTempView(tmp_view)

        merge_upsert(tmp_view, full_target, key_cols, target_cols)

        mode = "merge"
 
    # ── [FIX-8] Single aggregate for all metrics ──

    metrics = stg.agg(

        F.count("*").alias("cnt"),

        F.max("_coalesced_mod_ts").alias("max_ts"),

    ).collect()[0]
 
    row_count = metrics["cnt"]

    max_mod = metrics["max_ts"]
 
    stg.unpersist()
 
    # ── [FIX-1] Update watermark control table ──

    if max_mod is not None:

        update_watermark(full_target, max_mod, row_count)
 
    # ── [FIX-4] Maintenance only if explicitly enabled ──

    if maintenance.get("optimize", False):

        zcols = maintenance.get("zorder_cols")

        if isinstance(zcols, str):

            zcols = [zcols]

        maintain(

            full_target, zorder_cols=zcols,

            vacuum_hours=maintenance.get("vacuum_hours", 168),

        )
 
    print(f"[{mode.upper()}] {full_target} rows={row_count} max_ts={max_mod}")
 
    return {

        "table": full_target,

        "rows": row_count,

        "mode": mode,

        "max_modified_on": max_mod,

    }
 
 
# ==============================================================

# SEPARATE MAINTENANCE NOTEBOOK (run weekly, not per-table)

# ==============================================================

def run_maintenance_all(table_list: List[str], zorder_cols=None, vacuum_hours=168):

    """

    Run OPTIMIZE + VACUUM on all Silver tables.

    Schedule this as a SEPARATE notebook, weekly.

    """

    for tbl in table_list:

        try:

            print(f"[MAINTAIN] {tbl} ...")

            maintain(tbl, zorder_cols=zorder_cols, vacuum_hours=vacuum_hours)

            print(f"[MAINTAIN] {tbl} done.")

        except Exception as e:

            print(f"[MAINTAIN] {tbl} FAILED: {e}")
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # CX

# MARKDOWN ********************

# ## Customer

# CELL ********************

# ==============================================================
# Customer
# ==============================================================

customer_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/cr535_customers",  

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "cx",
    "target_table":  "silver_customer",

    # ---- NATURAL KEY ----
    # typically prod_order_no + prod_order_line_no uniquely identifies a line
    "business_key": ["customer_no"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "modifiedon": "modified_on",
        "cr535_no": "customer_no",
        "cr535_name": "customer_name",
        "cr535_locationcode": "customer_location",
        "cr535_cadstdtimeday": "customer_cad_std_time",
        "cr535_masterstdtimeday": "customer_master_std_time",
        "cr535_samplestdtimeday": "customer_sample_std_time",
        "cr535_branchidname": "customer_abbreviation",
        "cr535_salespersoncode": "cs_team",
        "cr535_responsibilitycenter": "cad_manager_team",
        "cr535_address": "customer_address",
        "cr535_address2": "customer_address2",
        "cr535_city": "customer_city",
        "cr535_countryregioncode": "customer_country",
        "cr535_phoneno": "customer_phone",
        "cr535_mobilephoneno": "customer_moblie_phone", 
        "cr535_contact": "customer_contact",
        "cr535_email": "customer_email",
        "SinkModifiedOn": "SinkModifiedOn",
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "SinkModifiedOn",
    "created_src_col": "createdon",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": ["created_on", "modified_on"],
    "ddl_overrides": {
        "customer_cad_std_time": "DOUBLE",
        "customer_master_std_time": "DOUBLE",
        "customer_sample_std_time": "DOUBLE",
    },

    # ---- FINAL COLUMNS ----
    "target_cols": [
        "created_on", "modified_on",
        "customer_no", "customer_name", "customer_location",
        "customer_cad_std_time", "customer_master_std_time", "customer_sample_std_time",
        "customer_abbreviation", "cs_team", "cad_manager_team",
        "customer_address", "customer_address2", "customer_city", "customer_country",
        "customer_phone", "customer_mobile_phone", "customer_contact", "customer_email", "SinkModifiedOn"
        "load_ts", "source_system"
    ],

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 360,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sales Header

# CELL ********************

# ==============================================================
# Sales Header (Full Mapping - Extended)
# ==============================================================

sales_header_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/dbo/cx.bronze_order_header",

     # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "cx",
    "target_table":  "silver_sales_header",

    # ---- NATURAL KEY ----
    # typically prod_order_no + prod_order_line_no uniquely identifies a line
    "business_key": ["sales_order_no"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "created_on": "created_on",
        "modified_on": "modified_on",
        "customer_no": "customer_no",
        "customer_name": "customer_name",
        "salesorder_type": "sales_order_type",
        "salesorder_no": "sales_order_no",
        "salesorder_status": "sales_order_status",
        "salesorder_released_date": "sales_order_released_date",
        "salesorder_document_date": "sales_order_document_date",
        "salesorder_posting_date": "sales_order_posting_date",
        "salesorder_order_date": "sales_order_order_date",
        "salesorder_due_date": "sales_order_due_date",
        "salesorder_requested_date": "sales_order_requested_date",
        "salesorder_promised_date": "sales_order_promised_date",
        "salesorder_shipment_date": "sales_order_shipment_date",
        "salesorder_cs_reference": "sales_order_cs_reference",
        "cs_team": "cs_team",
        "cad_manager_team": "cad_manager_team",
        "salesorder_currency": "sales_order_currency",
        "ship_to_customer": "ship_to_customer",
        "salesorder_external_document": "sales_order_external_document",
        "bill_to_customer": "bill_to_customer",
        "salesorder_location": "sales_order_location",
        "item_material": "item_material",
        "salesorder_amount_VAT": "sales_order_amount_VAT",
        "salesorder_amount": "sales_order_amount",
        "Donotdelay": "sales_order_no_delay",
        "DonotExceedDate": "sales_order_no_exceed_date"
    },

    # ---- WATERMARK SETTINGS ----
    # No SinkModifiedOn, so use modified_on directly
    "modified_src_col": "modified_on",
    "created_src_col": "created_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on", #"modified_on",
        "sales_order_released_date",
        "sales_order_document_date",
        "sales_order_posting_date",
        "sales_order_order_date",
        "sales_order_due_date",
        "sales_order_requested_date",
        "sales_order_promised_date",
        "sales_order_shipment_date",
        "sales_order_no_exceed_date"
    ],
    "decimal_cols": {
        "sales_order_amount_VAT": "DECIMAL(18,2)",
        "sales_order_amount": "DECIMAL(18,2)"
    },
    "ddl_overrides": {
        "sales_order_amount_VAT": "DECIMAL(18,2)",
        "sales_order_amount": "DECIMAL(18,2)"
    },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on", "modified_on",
        "customer_no", "customer_name",
        "sales_order_type", "sales_order_no", "sales_order_status",
        "sales_order_released_date", "sales_order_document_date", "sales_order_posting_date",
        "sales_order_order_date", "sales_order_due_date", "sales_order_requested_date",
        "sales_order_promised_date", "sales_order_shipment_date",
        "sales_order_cs_reference", "cs_team", "cad_manager_team",
        "sales_order_currency", "ship_to_customer", "sales_order_external_document",
        "bill_to_customer", "sales_order_location", "item_material",
        "sales_order_amount_VAT", "sales_order_amount", "sales_order_no_delay", "sales_order_no_exceed_date",
        "load_ts", "source_system"
    ],

    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": False,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
} 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sales Line

# CELL ********************

# ==============================================================
# Sales Line
# ==============================================================

sales_line_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/dbo/cx.bronze_order_line",  

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "cx",
    "target_table":  "silver_sales_line",

    # ---- BUSINESS KEY ----
    "business_key": ["sales_order_no", "sales_order_line_no"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "created_on": "created_on",
        "modified_on": "modified_on",
        "salesorder_type": "sales_order_type",
        "customer_no": "customer_no",
        "salesorder_no": "sales_order_no",
        "salesline_customer_noted": "sales_line_customer_noted",
        "salesline_customer_remark": "sales_line_customer_remark",
        "salesorder_lineno": "sales_order_line_no",
        "item_no": "item_no",
        "item_description": "item_description",
        "item_reference": "item_reference",
        "item_quantity": "item_quantity",
        "item_quantity_shipped": "item_quantity_shipped",
        "item_quantity_invoiced": "item_quantity_invoiced",
        "item_outstanding": "item_outstanding",
        "item_quantity_to_ship": "item_quantity_to_ship",
        "item_quantity_to_invoice": "item_quantity_to_invoice",
        "item_uom": "item_uom",
        "item_location": "item_location",
        "item_posting_group": "item_posting_group",
        "salesline_unit_price": "sales_line_unit_price",
        "item_unit_cost": "item_unit_cost",
        "salesline_amount": "sales_line_amount",
        "salesline_amount_VAT": "sales_line_amount_VAT",
        "salesline_outstanding_amount": "sales_line_outstanding_amount",
        "salesline_requested_date": "sales_line_requested_date",
        "salesline_promised_date": "sales_line_promised_date",
        "salesline_plan_delivery": "sales_line_plan_delivery",
        "salesline_plan_shipment": "sales_line_plan_shipment",
        "salesorder_shipment_date": "sales_order_shipment_date",
        "salesorder_currency": "sales_order_currency",
        "item_material": "item_material"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "modified_on",   # no SinkModifiedOn available
    "created_src_col": "created_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on", #"modified_on",
        "sales_line_requested_date", "sales_line_promised_date",
        "sales_line_plan_delivery", "sales_line_plan_shipment",
        "sales_order_shipment_date"
    ],
    "decimal_cols": {
        "item_quantity":                "DECIMAL(18,2)",
        "item_quantity_shipped":        "DECIMAL(18,2)",
        "item_quantity_invoiced":       "DECIMAL(18,2)",
        "item_outstanding":             "DECIMAL(18,2)",
        "item_quantity_to_ship":        "DECIMAL(18,2)",
        "item_quantity_to_invoice":     "DECIMAL(18,2)",
        "sales_line_unit_price":        "DECIMAL(18,2)",
        "item_unit_cost":               "DECIMAL(18,2)",
        "sales_line_amount":            "DECIMAL(18,2)",
        "sales_line_amount_VAT":        "DECIMAL(18,2)",
        "sales_line_outstanding_amount":"DECIMAL(18,2)"
    },
    "ddl_overrides": {
        "item_quantity":                "DECIMAL(18,2)",
        "item_quantity_shipped":        "DECIMAL(18,2)",
        "item_quantity_invoiced":       "DECIMAL(18,2)",
        "item_outstanding":             "DECIMAL(18,2)",
        "item_quantity_to_ship":        "DECIMAL(18,2)",
        "item_quantity_to_invoice":     "DECIMAL(18,2)",
        "sales_line_unit_price":        "DECIMAL(18,2)",
        "item_unit_cost":               "DECIMAL(18,2)",
        "sales_line_amount":            "DECIMAL(18,2)",
        "sales_line_amount_VAT":        "DECIMAL(18,2)",
        "sales_line_outstanding_amount":"DECIMAL(18,2)"
    },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on", "modified_on",
        "sales_order_type", "customer_no",
        "sales_order_no", "sales_order_line_no",
        "sales_line_customer_noted", "sales_line_customer_remark",
        "item_no", "item_description", "item_reference",
        "item_quantity", "item_quantity_shipped", "item_quantity_invoiced",
        "item_outstanding", "item_quantity_to_ship", "item_quantity_to_invoice",
        "item_uom", "item_location", "item_posting_group",
        "sales_line_unit_price", "item_unit_cost",
        "sales_line_amount", "sales_line_amount_VAT", "sales_line_outstanding_amount",
        "sales_line_requested_date", "sales_line_promised_date",
        "sales_line_plan_delivery", "sales_line_plan_shipment",
        "sales_order_shipment_date",
        "sales_order_currency", "item_material",
        "load_ts", "source_system"
    ],

    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Item Budget

# CELL ********************

# ==============================================================
# Finance • Item Budget
# ==============================================================

item_budget_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/dbo/cx.bronze_item_budget",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "cx",
    "target_table":  "silver_item_budget",

    # ---- NATURAL KEY ----
    # Entry_No uniquely identifies an item budget record
    "business_key": ["Entry_No"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "Entry_No": "Entry_No",
        "Budget_Name": "Budget_Name",
        "Budget_Date": "Budget_Date",
        "Item_No": "Item_No",
        "Source_Type": "Source_Type",
        "Source_No": "Source_No",
        "Quantity": "Quantity",
        "Sales_Amount": "Sales_Amount",
        "User_ID": "User_ID",
        "Location_Code": "Location_Code",
        "created_on": "created_on",
        "modified_on": "modified_on"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "modified_on",
    "created_src_col": "created_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on",
        "modified_on",
        "Budget_Date"
    ],
    "decimal_cols": {
        "Quantity": "DECIMAL(18,2)",
        "Sales_Amount": "DECIMAL(18,2)"
    },
    "ddl_overrides": {
        "Sales_Amount": "DECIMAL(18,2)"
    },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on", "modified_on",
        "Entry_No", "Budget_Name", "Budget_Date",
        "Item_No", "Source_Type", "Source_No",
        "Quantity", "Sales_Amount",
        "User_ID", "Location_Code"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; set to False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Inventory

# MARKDOWN ********************

# ## Item Ledger Entry

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import types as T


item_ledger_cfg = {
    # ---- SOURCE (Bronze Delta) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/dbo/inv.bronze_item_ledger",

    # ---- TARGET (Silver Lakehouse managed) ----
    "target_schema": "inv",                 # your driver will prefix with lakehouse
    "target_table":  "silver_item_ledger",

    # ---- BUSINESS KEY ----
    "business_key": ["order_no",
    "order_lineno",
    "item_no",
    "item_lot",
    "entry_type",
    "posting_date"],

    # ---- COLUMN MAP (bronze -> silver) ----
    "col_map": {
        "posting_date":                 "posting_date",
        "document_no":                  "document_no",
        "order_no":                     "order_no",
        "order_lineno":                 "order_lineno",
        "item_no":                      "item_no",
        "item_description":             "item_description",
        "partition_month":              "partition_month",
        "entry_type_item_location":     "entry_type_item_location",
        "entry_no":                     "entry_no",
        "item_lot_invoice_quantity":    "item_lot_invoice_quantity",
        "item_lot_remaining_quantity":  "item_lot_remaining_quantity",
        "created_on":                   "created_on",
        "modified_on":                  "modified_on",
        "item_uom":                     "item_uom",
        "item_uom2":                    "item_uom2",
        "entry_type_item_quantity2":    "entry_type_item_quantity2",
        "open":                         "open",
        "item_lot":                     "item_lot",
        "item_lot_for_DB":              "item_lot_for_db",
        "entry_type":                   "entry_type",
        "sales_amount_actual":          "sales_amount_actual",
        "sales_amount_expected":        "sales_amount_expected",
        "cost_amount_actual":           "cost_amount_actual",
        "cost_amount_expected":         "cost_amount_expected",
        "entry_type_item_quantity":     "entry_type_item_quantity"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col":   "modified_on",
    "created_src_col":    "created_on",
    "modified_target_col":"modified_on",

    # ---- TYPES (for your runner to apply) ----
    "ts_cols": ["posting_date","created_on","modified_on"],
    "decimal_cols": {
        "item_lot_invoice_quantity":   T.DecimalType(18,4),
        "item_lot_remaining_quantity": T.DecimalType(18,4),
        "entry_type_item_quantity":    T.DecimalType(18,4),
        "entry_type_item_quantity2":   T.DecimalType(18,4),
        "sales_amount_actual":         T.DecimalType(18,4),
        "sales_amount_expected":       T.DecimalType(18,4),
        "cost_amount_actual":          T.DecimalType(18,4),
        "cost_amount_expected":        T.DecimalType(18,4)
    },
    "ddl_overrides": {
        "posting_date":                "TIMESTAMP",
        "created_on":                  "TIMESTAMP",
        "modified_on":                 "TIMESTAMP",
        "item_lot_invoice_quantity":   "DECIMAL(18,4)",
        "item_lot_remaining_quantity": "DECIMAL(18,4)",
        "entry_type_item_quantity":    "DECIMAL(18,4)",
        "entry_type_item_quantity2":   "DECIMAL(18,4)",
        "sales_amount_actual":         "DECIMAL(18,4)",
        "sales_amount_expected":       "DECIMAL(18,4)",
        "cost_amount_actual":          "DECIMAL(18,4)",
        "cost_amount_expected":        "DECIMAL(18,4)"
    },

    # ---- FINAL ORDER (CREATE/MERGE) ----
    "target_cols": [
        "posting_date","document_no","order_no","order_lineno",
        "item_no","item_description",
        "partition_month","entry_type_item_location","entry_no",
        "item_lot_invoice_quantity","item_lot_remaining_quantity",
        "item_uom","item_uom2",
        "entry_type_item_quantity","entry_type_item_quantity2",
        "entry_type","open","item_lot","item_lot_for_db",
        "sales_amount_actual","sales_amount_expected","cost_amount_actual","cost_amount_expected",
        "created_on","modified_on",
        "load_ts","source_system"
    ],

    # ---- hook the transform ----
    "custom_transform": apply_enum_maps,

    # ---- load behavior ----
    "dedupe": True,
    "lookback_minutes": 360,
    "full_load": True,                 # flip to False when you want incremental-by-modified
    "full_load_repartitions": 16
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Purchase Order Header

# CELL ********************

# ==============================================================
# inv • Purchase Order
# ==============================================================

purchase_order_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/dbo/inv.bronze_purchase_order",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "inv",
    "target_table":  "silver_purchase_order",

    # ---- NATURAL KEY ----
    # purchase_order_no typically unique per record
    "business_key": ["purchase_order_no"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "`No.`": "purchase_order_no",
        "Status": "purchase_order_status",
        "Buy-from VendorNo": "purchase_order_vendor_no",
        "Buy-from Vendor Name": "purchase_order_vendor_name",
        "Posting Description": "purchase_order_type",
        "Document Date": "purchase_order_docment_date",
        "Order Date": "purchase_order_date",
        "Posting Date": "purchase_order_posting_date",
        "Due Date": "purchase_order_due_date",
        "Expected Receipt Date": "purchase_order_expected_receipt_date",
        "Requested Receipt Date": "purchase_order_requested_receipt_date",
        "Promised Receipt Date": "purchase_order_promised_receipt_date",
        "Invoice Receipt Date": "purchase_order_invoice_receipt_date",
        "Vendor Invoice_No": "purchase_order_vendor_reference",
        "Your Reference": "purchase_order_reference",
        "Remark": "purchase_order_remark",
        "Currency Code": "purchase_order_currency",
        "Lead Time Calculation": "purchase_order_leadtime",
        "Shortcut Dimension 1 Code": "purchase_order_department",
        "SystemCreatedAt": "created_on"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "Posting Date",
    "created_src_col": "Posting Date",
    "modified_target_col": "Posting Date",

    # ---- TYPES ----
    "ts_cols": [
        "purchase_order_docment_date",
        "purchase_order_date",
        "purchase_order_posting_date",
        "purchase_order_due_date",
        "purchase_order_expected_receipt_date",
        "purchase_order_requested_receipt_date",
        "purchase_order_promised_receipt_date",
        "purchase_order_invoice_receipt_date"
    ],
    # "decimal_cols": {
    #     "purchase_order_leadtime": "DECIMAL(18,2)"
    # },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "purchase_order_no",
        "purchase_order_status",
        "purchase_order_vendor_no",
        "purchase_order_vendor_name",
        "purchase_order_type",
        "purchase_order_docment_date",
        "purchase_order_date",
        "purchase_order_posting_date",
        "purchase_order_due_date",
        "purchase_order_expected_receipt_date",
        "purchase_order_requested_receipt_date",
        "purchase_order_promised_receipt_date",
        "purchase_order_invoice_receipt_date",
        "purchase_order_vendor_reference",
        "purchase_order_reference",
        "purchase_order_remark",
        "purchase_order_currency",
        "purchase_order_leadtime",
        "purchase_order_department"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Purchase Order Line

# CELL ********************

# ==============================================================
# inv • Purchase Line
# ==============================================================

purchase_line_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/dbo/inv.bronze_purchase_line",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "inv",
    "target_table":  "silver_purchase_line",

    # ---- NATURAL KEY ----
    # Usually purchase_order_no + purch_line uniquely identify a record
    "business_key": ["purchase_order_no", "purch_line"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "document_type": "document_type",
        "purchase_order_no": "purchase_order_no",
        "purch_type": "purch_type",
        "purch_line": "purch_line",
        "item_no": "item_no",
        "item_reference_no": "item_reference_no",
        "item_description": "item_description",
        "item_description_2": "item_description_2",
        "purch_line_request_date": "purch_line_request_date",
        "purch_line_promise_date": "purch_line_promise_date",
        "item_uom": "item_uom",
        "purch_line_quantity": "purch_line_quantity",
        "purch_line_uom": "purch_line_uom",
        "purch_line_direct_cost": "purch_line_direct_cost",
        "purch_line_unit_cost": "purch_line_unit_cost",
        "purch_line_total_amount": "purch_line_total_amount",
        "purch_line_VAT": "purch_line_VAT",
        "purch_line_location": "purch_line_location",
        "purch_line_uom_received": "purch_line_uom_received",
        "purch_line_qty_to_received": "purch_line_qty_to_received",
        "purch_line_qty_received": "purch_line_qty_received",
        "purch_line_qty_to_invoice": "purch_line_qty_to_invoice",
        "purch_line_qty_invoice": "purch_line_qty_invoice",
        "item_posting_group": "item_posting_group",
        "purch_line_plan_receipt_date": "purch_line_plan_receipt_date",
        "purch_line_expected_receipt_date": "purch_line_expected_receipt_date",
        "purch_line_lead_time": "purch_line_lead_time",
        "purch_line_return_reason": "purch_line_return_reason"
    },

    # ---- WATERMARK SETTINGS ----
    # Use expected_receipt_date for watermarking if no modified_on column exists
    "modified_src_col": "purch_line_expected_receipt_date",
    "created_src_col": "purch_line_request_date",
    "modified_target_col": "purch_line_expected_receipt_date",

    # ---- TYPES ----
    "ts_cols": [
        "purch_line_request_date",
        "purch_line_promise_date",
        "purch_line_plan_receipt_date",
        "purch_line_expected_receipt_date"
    ],
    # "decimal_cols": {
    #     "purch_line_quantity": "DECIMAL(18,2)",
    #     "purch_line_direct_cost": "DECIMAL(18,2)",
    #     "purch_line_unit_cost": "DECIMAL(18,2)",
    #     "purch_line_total_amount": "DECIMAL(18,2)",
    #     "purch_line_VAT": "DECIMAL(18,2)",
    #     "purch_line_qty_received": "DECIMAL(18,2)",
    #     "purch_line_qty_invoice": "DECIMAL(18,2)"
    # },
    # "ddl_overrides": {
    #     "purch_line_total_amount": "DECIMAL(18,2)",
    #     "purch_line_VAT": "DECIMAL(18,2)"
    # },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "document_type",
        "purchase_order_no",
        "purch_type",
        "purch_line",
        "item_no",
        "item_reference_no",
        "item_description",
        "item_description_2",
        "purch_line_request_date",
        "purch_line_promise_date",
        "item_uom",
        "purch_line_quantity",
        "purch_line_uom",
        "purch_line_direct_cost",
        "purch_line_unit_cost",
        "purch_line_total_amount",
        "purch_line_VAT",
        "purch_line_location",
        "purch_line_uom_received",
        "purch_line_qty_to_received",
        "purch_line_qty_received",
        "purch_line_qty_to_invoice",
        "purch_line_qty_invoice",
        "item_posting_group",
        "purch_line_plan_receipt_date",
        "purch_line_expected_receipt_date",
        "purch_line_lead_time",
        "purch_line_return_reason"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                 # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Item UOM

# CELL ********************

# ==============================================================
# NPD Item UOM
# ==============================================================

item_uom_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/cr535_itemunitsofmeasure",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "inv",
    "target_table":  "silver_item_uom",

    # ---- NATURAL KEY ----
    # Typically item_no uniquely identifies each UOM entry
    "business_key": ["item_no"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "modifiedon": "modified_on",
        "cr535_itemno": "item_no",
        "cr535_code": "item_uom",
        "cr535_qtyperunitofmeasure": "quantity_per_uom"
    },

    # ---- WATERMARK SETTINGS ----
    # No SinkModifiedOn, so use modified_on directly
    "modified_src_col": "modified_on",
    "created_src_col": "created_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on",
        "modified_on"
    ],
    "decimal_cols": {
        "quantity_per_uom": "DECIMAL(18,2)"
    },
    "ddl_overrides": {
        "quantity_per_uom": "DECIMAL(18,4)"
    },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        # watermarks
        "created_on", "modified_on",

        # item information
        "item_no", "item_uom", "quantity_per_uom"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Output Finished Goods

# CELL ********************

# ==============================================================
# prod • Output Finish Goods
# ==============================================================

output_finish_goods_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/dbo/inv.bronze_output_finish goods",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "inv",
    "target_table":  "silver_output_finish_goods",

    # ---- NATURAL KEY ----
    # Typically document_no + item_no uniquely identifies an entry
    "business_key": ["document_no", "item_no"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "posting_date": "posting_date",
        "document_no": "document_no",
        "document_status": "document_status",
        "item_no": "item_no",
        "item_uom": "item_uom",
        "item_quantity": "item_quantity",
        "entry_type": "entry_type",
        "salesorder_no": "salesorder_no",
        "salesorder_lineno": "salesorder_lineno"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "posting_date",
    "created_src_col": "posting_date",
    "modified_target_col": "posting_date",

    # ---- TYPES ----
    "ts_cols": [
        "posting_date"
    ],
    "decimal_cols": {
        "item_quantity": "DECIMAL(18,2)"
    },
    "ddl_overrides": {
        "item_quantity": "DECIMAL(18,4)"
    },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "posting_date",
        "document_no",
        "document_status",
        "item_no",
        "item_uom",
        "item_quantity",
        "entry_type",
        "salesorder_no",
        "salesorder_lineno"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Metal

# CELL ********************

# PySpark in Microsoft Fabric Notebook

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# 1) Point to your CSV in the Lakehouse Files area
#    Example path after you upload: Files/silver_metal.csv
csv_path = "Files/Type of Metal(Type Metal).csv"   # change if needed

# 2) Define schema (strings are safest for codes/descriptions)
schema = StructType([
    StructField("Item",        StringType(), True),
    StructField("Description", StringType(), True),
    StructField("UOM",         StringType(), True),
    StructField("TYPE",        StringType(), True),
])

# 3) Read the CSV
df = (spark.read
      .format("csv")
      .option("header", "true")
      .option("multiLine", "true")      # keeps descriptions with commas/quotes intact
      .option("quote", '"')
      .option("escape", '"')
      .schema(schema)
      .load(csv_path))

# # 5) Ensure schema exists, then write a managed Delta table
# spark.sql("CREATE SCHEMA IF NOT EXISTS inv")

(df
  .select("Item", "Description", "UOM", "TYPE")
  .write
  .mode("overwrite")      # change to 'append' if you want to add to existing
  .format("delta")
  .saveAsTable("inv.silver_metal"))

# 6) Quick verification
print("Row count:", spark.table("inv.silver_metal").count())
spark.table("inv.silver_metal").orderBy("Item").show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # Finance

# MARKDOWN ********************

# ## Credit Memo Header

# CELL ********************

# ==============================================================
# Credit Memo Header
# ==============================================================

credit_memo_header_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/dbo/fa.bronze_creditmemo_header",  

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "fa",
    "target_table":  "silver_credit_memo_header",

    # ---- BUSINESS KEY ----
    "business_key": ["credit_memo_no"],

    # ---- COLUMN MAP ----
    "col_map": {
        "creditmemo_no":                    "credit_memo_no",
        "creditmemo_no1":                   "credit_memo_no1",
        "return_order_no":                  "return_order_no",
        "customer_group":                   "customer_group",
        "customer_no":                      "customer_no",
        "creditmemo_document_date":         "credit_memo_document_date",
        "creditmemo_due_date":              "credit_memo_due_date",
        "creditmemo_posting_date":          "credit_memo_posting_date",
        "customer_VAT_registration_no":     "customer_VAT_registration_no",
        "creditmemo_VAT_reporting_date":    "credit_memo_VAT_reporting_date",
        "creditmemo_external_document":     "credit_memo_external_document",
        "creditmemo_cs_reference":          "credit_memo_cs_reference",
        "creditmemo_noted":                 "credit_memo_noted",
        "cs_team":                          "cs_team",
        "cad_manager_team":                 "cad_manager_team",
        "creditmemo_cancel":                "credit_memo_cancel",
        "currency_code":                    "currency_code",
        "currency_factor":                  "currency_factor",
        "item_material":                    "item_material",
        "customer_country":                 "customer_country",
        "customer_contact":                 "customer_contact",
        "creditmemo_location":              "credit_memo_location",
        "creditmemo_qty_printed":           "credit_memo_qty_printed",
        "creditmemo_department":            "credit_memo_department",
        "created_on":                       "created_on",
        "modified_on":                      "modified_on"
    },

    # ---- WATERMARK ----
    "modified_src_col": "modified_on",
    "created_src_col": "created_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on","modified_on",
        "credit_memo_document_date","credit_memo_due_date",
        "credit_memo_posting_date","credit_memo_VAT_reporting_date"
    ],

    "decimal_cols": {
        "currency_factor": "DECIMAL(18,6)"
    },

    "ddl_overrides": {
        "currency_factor": "DECIMAL(18,6)"
    },

    # ---- FINAL TARGET COLS ----
    "target_cols": [
        "credit_memo_no",
        "credit_memo_no1",
        "return_order_no",
        "customer_group",
        "customer_no",
        "credit_memo_document_date",
        "credit_memo_due_date",
        "credit_memo_posting_date",
        "customer_VAT_registration_no",
        "credit_memo_VAT_reporting_date",
        "credit_memo_external_document",
        "credit_memo_cs_reference",
        "credit_memo_noted",
        "cs_team",
        "cad_manager_team",
        "credit_memo_cancel",
        "currency_code",
        "currency_factor",
        "item_material",
        "customer_country",
        "customer_contact",
        "credit_memo_location",
        "credit_memo_qty_printed",
        "credit_memo_department",
        "created_on",
        "modified_on",
        "load_ts",
        "source_system"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,


    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Credit Memo Line

# CELL ********************

# ==============================================================
# Credit Memo Line
# ==============================================================

credit_memo_line_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/dbo/fa.bronze_creditmemo_line",  

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "fa",
    "target_table":  "silver_credit_memo_line",

    # ---- BUSINESS KEY ----
    "business_key": ["credit_memo_no", "credit_memo_line_no"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "creditmemo_posting_date": "credit_memo_posting_date",
        "creditmemo_no": "credit_memo_no",
        "creditmemo_lineno": "credit_memo_line_no",
        "customer_no": "customer_no",
        "item_no": "item_no",
        "item_description": "item_description",
        "item_location": "item_location",
        "item_quantity": "item_quantity",
        "item_uom": "item_uom",
        "creditmemo_unit_price": "credit_memo_unit_price",
        "item_unit_cost": "item_unit_cost",
        "item_unit_cost_THB": "item_unit_cost_THB",
        "VATBaseAmount": "VATBaseAmount",
        "LineAmount": "LineAmount",
        "creditmemo_line_amount": "credit_memo_line_amount",
        "creditmemo_line_amount_VAT": "credit_memo_line_amount_VAT",
        "creditmemo_line_department": "credit_memo_line_department",
        "item_material": "item_material",
        "item_posting_group": "item_posting_group",
        "cad_manager_team": "cad_manager_team",
        "created_on": "created_on",
        "modified_on": "modified_on"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col":   "modified_on",
    "created_src_col":    "created_on",
    "modified_target_col":"modified_on",

    # ---- TYPES ----
    "ts_cols": ["created_on", "modified_on", "credit_memo_posting_date"],

    "decimal_cols": {
        "item_quantity":              "DECIMAL(18,2)",
        "credit_memo_unit_price":      "DECIMAL(18,2)",
        "item_unit_cost":             "DECIMAL(18,2)",
        "item_unit_cost_THB":         "DECIMAL(18,2)",
        "VATBaseAmount":              "DECIMAL(18,2)",
        "LineAmount":                 "DECIMAL(18,2)",
        "credit_memo_line_amount":     "DECIMAL(18,2)",
        "credit_memo_line_amount_VAT": "DECIMAL(18,2)"
    },

    "ddl_overrides": {
        "item_quantity":              "DECIMAL(18,2)",
        "credit_memo_unit_price":      "DECIMAL(18,2)",
        "item_unit_cost":             "DECIMAL(18,2)",
        "item_unit_cost_THB":         "DECIMAL(18,2)",
        "VATBaseAmount":              "DECIMAL(18,2)",
        "LineAmount":                 "DECIMAL(18,2)",
        "credit_memo_line_amount":     "DECIMAL(18,2)",
        "credit_memo_line_amount_VAT": "DECIMAL(18,2)"
    },

    # ---- FINAL TARGET COLS (order for CREATE TABLE / MERGE) ----
    "target_cols": [
        "credit_memo_posting_date",
        "credit_memo_no",
        "credit_memo_line_no",
        "customer_no",
        "item_no",
        "item_description",
        "item_location",
        "item_quantity",
        "item_uom",
        "credit_memo_unit_price",
        "item_unit_cost",
        "item_unit_cost_THB",
        "VATBaseAmount",
        "LineAmount",
        "credit_memo_line_amount",
        "credit_memo_line_amount_VAT",
        "credit_memo_line_department",
        "item_material",
        "item_posting_group",
        "cad_manager_team",
        "created_on",
        "modified_on",
        "load_ts",
        "source_system"
    ],


    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,


    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Customer Ledger Entries

# CELL ********************

# ==============================================================
# Customer Ledger Entries
# ==============================================================

customer_ledger_entries_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/dbo/fa.bronze_customer_ledger_entries",  

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "fa",
    "target_table":  "silver_customer_ledger_entries",

    # ---- BUSINESS KEY ----
    "business_key": ["Entry_No", "Document_No", "Document_Type", "External_Document_No",
        "AppliesTo_Doc_No", "AppliesTo_Doc_Type", "AppliesTo_External_Doc_No",
        "AppliesTo_ID", "Applying_Entry_No", "No_Series", "Your_Reference",
        "Message_To_Recipient",

        # Key Dates
        "Posting_Date", "Document_Date", "Due_Date", "Closed_Date",
        "Payment_Discount_Date", "Payment_Disc_Tolerance_Date", "Date_Filter",

        # Customer & Sales Info
        "Customer_No", "Customer_", "SellTo_Customer_No",
        "Customer_Posting_Group", "Salesperson_Code",
        "Direct_Debit_Mandate_ID", "Payment_Method_Code", "Payment_Reference",
        "Recipient_Bank_Account", "Exported_To_Payment_File",
        "Last_Issued_Reminder_Level",

        # Currencies & Factors
        "Currency_Code", "Adjusted_Currency_Factor", "Original_Currency_Factor",
        "Closed_By_Currency_Code", "Closed_By_Currency_Amount",

        # Amounts
        "Amount", "Amount_LCY", "Amount_To_Apply",
        "Original_Amount", "Original_Amount_LCY",
        "Credit_Amount", "Credit_Amount_LCY",
        "Debit_Amount", "Debit_Amount_LCY",
        "Remaining_Amount", "Remaining_Amount_LCY",
        "Closed_By_Amount", "Closed_By_Amount_LCY",
        "Closed_By_Entry_No", "Invoice_Discount_LCY",
        "Payment_Discount_Given_LCY", "Payment_Tolerance_LCY",
        "Original_Payment_Disc_Possible", "Remaining_Payment_Disc_Possible",
        "Max_Payment_Tolerance", "Original_Payment_Disc_Possible_LCY",
        "Profit_LCY", "Sales_LCY",

        # Flags / Booleans
        "Open", "On_Hold", "Prepayment", "Positive",
        "Calculate_Interest", "Closing_Interest_Calculated",
        "Reversed", "Reversed_Entry_No", "Reversed_By_Entry_No",

        # Accounting / Metadata
        "Bal_Account_No", "Bal_Account_Type",
        "Dimension_Set_ID", "Global_Dimension1_Code", "Global_Dimension2_Code",
        "IC_Partner_Code", "Journal_Batch_", "Journal_Template_",
        "Reason_Code", "Description",

        # System Columns
        "System_Created_At", "System_Modified_At"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "Entry_No": "Entry_No",
        "Document_No": "Document_No",
        "Document_Type": "Document_Type",
        "External_Document_No": "External_Document_No",
        "AppliesTo_Doc_No": "AppliesTo_Doc_No",
        "AppliesTo_Doc_Type": "AppliesTo_Doc_Type",
        "AppliesTo_External_Doc_No": "AppliesTo_External_Doc_No",
        "AppliesTo_ID": "AppliesTo_ID",
        "Applying_Entry_No": "Applying_Entry_No",
        "No_Series": "No_Series",
        "Your_Reference": "Your_Reference",
        "Message_To_Recipient": "Message_To_Recipient",
        "Posting_Date": "Posting_Date",
        "Document_Date": "Document_Date",
        "Due_Date": "Due_Date",
        "Closed_Date": "Closed_Date",
        "Payment_Discount_Date": "Payment_Discount_Date",
        "Payment_Disc_Tolerance_Date": "Payment_Disc_Tolerance_Date",
        "Date_Filter": "Date_Filter",
        "Customer_No": "Customer_No",
        "Customer_": "Customer_",
        "SellTo_Customer_No": "SellTo_Customer_No",
        "Customer_Posting_Group": "Customer_Posting_Group",
        "Salesperson_Code": "Salesperson_Code",
        "Direct_Debit_Mandate_ID": "Direct_Debit_Mandate_ID",
        "Payment_Method_Code": "Payment_Method_Code",
        "Payment_Reference": "Payment_Reference",
        "Recipient_Bank_Account": "Recipient_Bank_Account",
        "Exported_To_Payment_File": "Exported_To_Payment_File",
        "Last_Issued_Reminder_Level": "Last_Issued_Reminder_Level",
        "Max_Payment_Tolerance": "Max_Payment_Tolerance",
        "Remaining_Payment_Disc_Possible": "Remaining_Payment_Disc_Possible",
        "Original_Payment_Disc_Possible": "Original_Payment_Disc_Possible",
        "Currency_Code": "Currency_Code",
        "Adjusted_Currency_Factor": "Adjusted_Currency_Factor",
        "Original_Currency_Factor": "Original_Currency_Factor",
        "Amount": "Amount",
        "Amount_LCY": "Amount_LCY",
        "Amount_To_Apply": "Amount_To_Apply",
        "Original_Amount": "Original_Amount",
        "Original_Amount_LCY": "Original_Amount_LCY",
        "Credit_Amount": "Credit_Amount",
        "Credit_Amount_LCY": "Credit_Amount_LCY",
        "Debit_Amount": "Debit_Amount",
        "Debit_Amount_LCY": "Debit_Amount_LCY",
        "Remaining_Amount": "Remaining_Amount",
        "Remaining_Amounlt_LCY": "Remaining_Amount_LCY",
        "Closed_By_Amount": "Closed_By_Amount",
        "Closed_By_Amount_LCY": "Cosed_By_Amount_LCY",
        "Closed_By_Currency_Amount": "Closed_By_Currency_Amount",
        "Closed_By_Currency_Code": "Closed_By_Currency_Code",
        "Closed_By_Entry_No": "Closed_By_Entry_No",
        "Invoice_Discount_LCY": "Invoice_Discount_LCY",
        "Payment_Discount_Given_LCY": "Payment_Discount_Given_LCY",
        "Payment_Tolerance_LCY": "Payment_Tolerance_LCY",
        "Original_Payment_Disc_Possible_LCY": "Original_Payment_Disc_Possible_LCY",
        "Profit_LCY": "Profit_LCY",
        "Sales_LCY": "Sales_LCY",
        "Open": "Open",
        "On_Hold": "On_Hold",
        "Prepayment": "Prepayment",
        "Positive": "Positive",
        "Calculate_Interest": "Calculate_Interest",
        "Closing_Interest_Calculated": "Closing_Interest_Calculated",
        "Reversed": "Reversed",
        "Reversed_Entry_No": "Reversed_Entry_No",
        "Reversed_By_Entry_No": "Reversed_By_Entry_No",
        "Bal_Account_No": "Bal_Account_No",
        "Bal_Account_Type": "Bal_Account_Type",
        "Dimension_Set_ID": "Dimension_Set_ID",
        "Global_Dimension1_Code": "Global_Dimension1_Code",
        "Global_Dimension2_Code": "Global_Dimension2_Code",
        "IC_Partner_Code": "IC_Partner_Code",
        "Journal_Batch_": "Journal_Batch_",
        "Journal_Template_": "Journal_Template_",
        "Reason_Code": "Reason_Code",
        "Description": "Description",
        "System_Created_At": "System_Created_At",
        "System_Modified_At": "System_Modified_At"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col":   "System_Modified_At",
    "created_src_col":    "System_Created_At",
    "modified_target_col":"System_Modified_At",

    # ---- TYPES ----
    "ts_cols": [
        "System_Created_At","System_Modified_At",
        "Posting_Date","Document_Date","Due_Date",
        "Closed_Date","Payment_Discount_Date",
        "Payment_Disc_Tolerance_Date","Date_Filter"
    ],
    "decimal_cols": {
        "Max_Payment_Tolerance":               "DECIMAL(18,2)",
        "Remaining_Payment_Disc_Possible":     "DECIMAL(18,2)",
        "Original_Payment_Disc_Possible":      "DECIMAL(18,2)",
        "Adjusted_Currency_Factor":            "DECIMAL(18,2)",
        "Original_Currency_Factor":            "DECIMAL(18,2)",
        "Amount":                              "DECIMAL(18,2)",
        "Amount_LCY":                          "DECIMAL(18,2)",
        "Amount_To_Apply":                     "DECIMAL(18,2)",
        "Original_Amount":                     "DECIMAL(18,2)",
        "Original_Amount_LCY":                 "DECIMAL(18,2)",
        "Credit_Amount":                       "DECIMAL(18,2)",
        "Credit_Amount_LCY":                   "DECIMAL(18,2)",
        "Debit_Amount":                        "DECIMAL(18,2)",
        "Debit_Amount_LCY":                    "DECIMAL(18,2)",
        "Remaining_Amount":                    "DECIMAL(18,2)",
        "Remaining_Amount_LCY":                "DECIMAL(18,2)",
        "Closed_By_Amount":                    "DECIMAL(18,2)",
        "Closed_By_Amount_LCY":                "DECIMAL(18,2)",
        "Closed_By_Currency_Amount":           "DECIMAL(18,2)",
        "Invoice_Discount_LCY":                "DECIMAL(18,2)",
        "Payment_Discount_Given_LCY":          "DECIMAL(18,2)",
        "Payment_Tolerance_LCY":               "DECIMAL(18,2)",
        "Original_Payment_Disc_Possible_LCY":  "DECIMAL(18,2)",
        "Profit_LCY":                          "DECIMAL(18,2)",
        "Sales_LCY":                           "DECIMAL(18,2)"
    },
    "ddl_overrides": {
        "Max_Payment_Tolerance":               "DECIMAL(18,2)",
        "Remaining_Payment_Disc_Possible":     "DECIMAL(18,2)",
        "Original_Payment_Disc_Possible":      "DECIMAL(18,2)",
        "Adjusted_Currency_Factor":            "DECIMAL(18,2)",
        "Original_Currency_Factor":            "DECIMAL(18,2)",
        "Amount":                              "DECIMAL(18,2)",
        "Amount_LCY":                          "DECIMAL(18,2)",
        "Amount_To_Apply":                     "DECIMAL(18,2)",
        "Original_Amount":                     "DECIMAL(18,2)",
        "Original_Amount_LCY":                 "DECIMAL(18,2)",
        "Credit_Amount":                       "DECIMAL(18,2)",
        "Credit_Amount_LCY":                   "DECIMAL(18,2)",
        "Debit_Amount":                        "DECIMAL(18,2)",
        "Debit_Amount_LCY":                    "DECIMAL(18,2)",
        "Remaining_Amount":                    "DECIMAL(18,2)",
        "Remaining_Amount_LCY":                "DECIMAL(18,2)",
        "Closed_By_Amount":                    "DECIMAL(18,2)",
        "Closed_By_Amount_LCY":                "DECIMAL(18,2)",
        "Closed_By_Currency_Amount":           "DECIMAL(18,2)",
        "Invoice_Discount_LCY":                "DECIMAL(18,2)",
        "Payment_Discount_Given_LCY":          "DECIMAL(18,2)",
        "Payment_Tolerance_LCY":               "DECIMAL(18,2)",
        "Original_Payment_Disc_Possible_LCY":  "DECIMAL(18,2)",
        "Profit_LCY":                          "DECIMAL(18,2)",
        "Sales_LCY":                           "DECIMAL(18,2)"
    },


    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
    # Identifiers
        "Entry_No", "Document_No", "Document_Type", "External_Document_No",
        "AppliesTo_Doc_No", "AppliesTo_Doc_Type", "AppliesTo_External_Doc_No",
        "AppliesTo_ID", "Applying_Entry_No", "No_Series", "Your_Reference",
        "Message_To_Recipient",

        # Key Dates
        "Posting_Date", "Document_Date", "Due_Date", "Closed_Date",
        "Payment_Discount_Date", "Payment_Disc_Tolerance_Date", "Date_Filter",

        # Customer & Sales Info
        "Customer_No", "Customer_", "SellTo_Customer_No",
        "Customer_Posting_Group", "Salesperson_Code",
        "Direct_Debit_Mandate_ID", "Payment_Method_Code", "Payment_Reference",
        "Recipient_Bank_Account", "Exported_To_Payment_File",
        "Last_Issued_Reminder_Level",

        # Currencies & Factors
        "Currency_Code", "Adjusted_Currency_Factor", "Original_Currency_Factor",
        "Closed_By_Currency_Code", "Closed_By_Currency_Amount",

        # Amounts
        "Amount", "Amount_LCY", "Amount_To_Apply",
        "Original_Amount", "Original_Amount_LCY",
        "Credit_Amount", "Credit_Amount_LCY",
        "Debit_Amount", "Debit_Amount_LCY",
        "Remaining_Amount", "Remaining_Amount_LCY",
        "Closed_By_Amount", "Closed_By_Amount_LCY",
        "Closed_By_Entry_No", "Invoice_Discount_LCY",
        "Payment_Discount_Given_LCY", "Payment_Tolerance_LCY",
        "Original_Payment_Disc_Possible", "Remaining_Payment_Disc_Possible",
        "Max_Payment_Tolerance", "Original_Payment_Disc_Possible_LCY",
        "Profit_LCY", "Sales_LCY",

        # Flags / Booleans
        "Open", "On_Hold", "Prepayment", "Positive",
        "Calculate_Interest", "Closing_Interest_Calculated",
        "Reversed", "Reversed_Entry_No", "Reversed_By_Entry_No",

        # Accounting / Metadata
        "Bal_Account_No", "Bal_Account_Type",
        "Dimension_Set_ID", "Global_Dimension1_Code", "Global_Dimension2_Code",
        "IC_Partner_Code", "Journal_Batch_", "Journal_Template_",
        "Reason_Code", "Description",

        # System Columns
        "System_Created_At", "System_Modified_At",

        # ETL / Metadata Columns
        "load_ts", "source_system"
    ],


    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,


    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Exchange rate

# CELL ********************

# ==============================================================
# Exchange Rate
# ==============================================================

exchange_rate_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/dbo/fa.bronze_exchange_rate",  

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "fa",
    "target_table":  "silver_exchange_rate",

    # ---- BUSINESS KEY ----
    "business_key": ["currency_date"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "created_on":               "created_on",
        "modified_on":              "modified_on",
        "currency_date":            "currency_date",
        "adjustment_rate_amount":   "adjustment_rate_amount",
        "currency_code":            "currency_code",
        "exchange_rate_amount":     "exchange_rate_amount",
        "fix_exchange_rate_amount": "fix_exchange_rate_amount",
        "THB_adjmt_exch_rate_amt":  "THB_adjmt_exch_rate_amt",
        "THB_exch_rate_amt":        "THB_exch_rate_amt",
        "currency_THB":             "currency_THB"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col":   "modified_on",
    "created_src_col":    "created_on",
    "modified_target_col":"modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on","modified_on","currency_date"
    ],
    "decimal_cols": {
        "adjustment_rate_amount":   "DECIMAL(18,6)",
        "exchange_rate_amount":     "DECIMAL(18,6)",
        "fix_exchange_rate_amount": "DECIMAL(18,6)",
        "THB_adjmt_exch_rate_amt":  "DECIMAL(18,6)",
        "THB_exch_rate_amt":        "DECIMAL(18,6)"
    },
    "ddl_overrides": {
        "adjustment_rate_amount":   "DECIMAL(18,6)",
        "exchange_rate_amount":     "DECIMAL(18,6)",
        "fix_exchange_rate_amount": "DECIMAL(18,6)",
        "THB_adjmt_exch_rate_amt":  "DECIMAL(18,6)",
        "THB_exch_rate_amt":        "DECIMAL(18,6)"
    },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on","modified_on","currency_date",
        "currency_code","currency_THB",
        "adjustment_rate_amount","exchange_rate_amount","fix_exchange_rate_amount",
        "THB_adjmt_exch_rate_amt","THB_exch_rate_amt",
        "load_ts","source_system"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,


    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## GL Entry

# CELL ********************

# ==============================================================
# Finance • G/L Entry (Simplified Field Mapping)
# ==============================================================

gl_entry_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/dbo/fa.bronze_gl_entry",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "finance",
    "target_table":  "silver_gl_entry",

    # ---- NATURAL KEY ----
    "business_key": ["entry_no"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "entryNo": "entry_no",
        "postingDate": "Posting_Date",
        "vatReportingDate": "vat_Reporting_Date",
        "documentDate": "Document_date",
        "documentType": "Document_type",
        "documentNo": "Document_No",
        "gLAccountNo": "G_L_Account_No",
        "gLAccountName": "G_L_Account_Name",
        "description": "Description",
        "amount": "Amount",
        "creditAmount": "credit_amount",
        "debitAmount": "debit_amount",
        "sourceCode": "source_Code",
        "sourceNo": "source_No",
        "sourceType": "source_Type",
        "balAccountNo": "bal_Account_No",
        "balAccountType": "bal_Account_Type",
        "journalBatchName": "journal_Batch_Name",
        "journalTemplName": "journal_Templ_Name",
        "externalDocumentNo": "external_Document_No",
        "systemCreatedAt": "created_on",
        "systemModifiedAt": "modified_on"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "systemModifiedAt",
    "created_src_col": "systemCreatedAt",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on",
        "modified_on",
        "Posting_Date",
        "vat_Reporting_Date",
        "Document_date"
    ],
    "decimal_cols": {
        "Amount": "DECIMAL(18,2)",
        "credit_amount": "DECIMAL(18,2)",
        "debit_amount": "DECIMAL(18,2)"
    },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "entry_no",
        "Posting_Date",
        "vat_Reporting_Date",
        "Document_date",
        "Document_type",
        "Document_No",
        "G_L_Account_No",
        "G_L_Account_Name",
        "Description",
        "Amount",
        "credit_amount",
        "debit_amount",
        "source_Code",
        "source_No",
        "source_Type",
        "bal_Account_No",
        "bal_Account_Type",
        "journal_Batch_Name",
        "journal_Templ_Name",
        "external_Document_No",
        "created_on",
        "modified_on"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                 # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## GL 40

# CELL ********************

# ==============================================================
# Gl 40
# ==============================================================

gl_40_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/dbo/fa.bronze_gl_40",  

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "fa",
    "target_table":  "silver_gl_40",

    # ---- BUSINESS KEY ----
    "business_key": [
        "posting_date",
        "document_no",
        "gl_account_no",
        "description",
        "amount",
        "customer_vendor_no"
    ],

    # ---- COLUMN MAP ----
    "col_map": {
        "Posting_Date":       "posting_date",
        "Document_No":        "document_no",
        "G_L_Account_No":     "gl_account_no",
        "Description":        "description",
        "Amount":             "amount",
        "customer_vendor_no": "customer_vendor_no"
    },

    # ---- WATERMARK ----
    "modified_src_col": "Posting_Date",  # safest available change marker
    "created_src_col":  "Posting_Date",
    "modified_target_col": "posting_date",

    # ---- TYPES ----
    "ts_cols": ["posting_date"],

    "decimal_cols": {
        "amount": "DECIMAL(18,2)"
    },

    "ddl_overrides": {
        "amount": "DECIMAL(18,2)"
    },

    # ---- FINAL TARGET COLS ----
    "target_cols": [
        "posting_date",
        "document_no",
        "gl_account_no",
        "description",
        "amount",
        "customer_vendor_no",
        "load_ts",
        "source_system"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,


    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## GL 103

# CELL ********************

# ==============================================================
# Gl 103
# ==============================================================

gl_103_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/dbo/fa.bronze_gL103",  

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "fa",
    "target_table":  "silver_gl_103",

    # ---- BUSINESS KEY ----
    "business_key": [
        "entry_no",
        "posting_date",
        "document_no",
        "gl_account_no",
        "description",
        "amount",
        "debit_amount",
        "credit_amount",
        "source_no",
        "system_modified_at",
        "etag"
    ],

    # ---- COLUMN MAP (source -> target, snake_case standard) ----
    "col_map": {
        "EntryNo":          "entry_no",
        "PostingDate":      "posting_date",
        "DocumentNo":       "document_no",
        "GLAccountNo":      "gl_account_no",
        "Description":      "description",
        "Amount":           "amount",
        "DebitAmount":      "debit_amount",
        "CreditAmount":     "credit_amount",
        "SourceNo":         "source_no",
        "SystemModifiedAt": "system_modified_at",
        "ETag":             "etag"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col":   "SystemModifiedAt",
    "created_src_col":    "PostingDate",
    "modified_target_col":"system_modified_at",

    # ---- TYPES ----
    "ts_cols": ["posting_date", "system_modified_at"],

    "decimal_cols": {
        "amount":       "DECIMAL(18,2)",
        "debit_amount": "DECIMAL(18,2)",
        "credit_amount":"DECIMAL(18,2)"
    },

    "ddl_overrides": {
        "amount":       "DECIMAL(18,2)",
        "debit_amount": "DECIMAL(18,2)",
        "credit_amount":"DECIMAL(18,2)"
    },

    # ---- TARGET COLS ----
    "target_cols": [
        "entry_no",
        "posting_date",
        "document_no",
        "gl_account_no",
        "description",
        "amount",
        "debit_amount",
        "credit_amount",
        "source_no",
        "system_modified_at",
        "etag",
        "load_ts",
        "source_system"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,


    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## GL 501

# CELL ********************

# ==============================================================
# Gl 103
# ==============================================================

gl_501_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/dbo/fa.bronze_gL103",  

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "fa",
    "target_table":  "silver_gl_501",

    # ---- BUSINESS KEY ----
    "business_key": [
        "entry_no",
        "posting_date",
        "document_no",
        "gl_account_no",
        "description",
        "amount",
        "debit_amount",
        "credit_amount",
        "source_no",
        "system_modified_at",
        "etag"
    ],

    # ---- COLUMN MAP (source -> target, snake_case standard) ----
    "col_map": {
        "EntryNo":          "entry_no",
        "PostingDate":      "posting_date",
        "DocumentNo":       "document_no",
        "GLAccountNo":      "gl_account_no",
        "Description":      "description",
        "Amount":           "amount",
        "DebitAmount":      "debit_amount",
        "CreditAmount":     "credit_amount",
        "SourceNo":         "source_no",
        "SystemModifiedAt": "system_modified_at",
        "ETag":             "etag"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col":   "SystemModifiedAt",
    "created_src_col":    "PostingDate",
    "modified_target_col":"system_modified_at",

    # ---- TYPES ----
    "ts_cols": ["posting_date", "system_modified_at"],

    "decimal_cols": {
        "amount":       "DECIMAL(18,2)",
        "debit_amount": "DECIMAL(18,2)",
        "credit_amount":"DECIMAL(18,2)"
    },

    "ddl_overrides": {
        "amount":       "DECIMAL(18,2)",
        "debit_amount": "DECIMAL(18,2)",
        "credit_amount":"DECIMAL(18,2)"
    },

    # ---- TARGET COLS ----
    "target_cols": [
        "entry_no",
        "posting_date",
        "document_no",
        "gl_account_no",
        "description",
        "amount",
        "debit_amount",
        "credit_amount",
        "source_no",
        "system_modified_at",
        "etag",
        "load_ts",
        "source_system"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,


    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sales Invoice Header

# CELL ********************

# ==============================================================
# Sales Invoice Header
# ==============================================================

sales_invoice_header_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/dbo/fa.bronze_sales_invoice_header",  

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "fa",
    "target_table":  "silver_sales_invoice_header",

    # ---- BUSINESS KEY ----
    "business_key": ["invoice_no"],

    "col_map": {
        "invoice_no":                 "invoice_no",
        "invoice_no1":                "invoice_no1",
        "salesorder_no":              "sales_order_no",
        "customer_group":             "customer_group",
        "customer_no":                "customer_no",
        "customer_":              "customer_",
        "invoice_document_date":      "invoice_document_date",
        "invoice_order_date":         "invoice_order_date",
        "invoice_due_date":           "invoice_due_date",
        "invoice_posting_date":       "invoice_posting_date",
        "invoice_shipment_date":      "invoice_shipment_date",
        "invoice_VAT_reporting_date": "invoice_VAT_reporting_date",
        "invoice_external_document":  "invoice_external_document",
        "invoice_cs_reference":       "invoice_cs_reference",
        "invoice_currency":           "invoice_currency",
        "invoice_amount":             "invoice_amount",
        "invoice_amount_VAT":         "invoice_amount_VAT",
        "invoice_remaining_amount":   "invoice_remaining_amount",
        "invoice_cancelled":          "invoice_cancelled",
        "invoice_closed":             "invoice_closed",
        "invoice_corrective":         "invoice_corrective",
        "cs_team":                    "cs_team",
        "cad_manager_team":           "cad_manager_team",
        "invoice_location":           "invoice_location",
        "invoice_department":         "invoice_department",
        "item_material":              "item_material",
        "modified_on":                "modified_on"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col":   "modified_on",
    "created_src_col":    "modified_on",     # no created_on in the list; safe fallback
    "modified_target_col":"modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "modified_on",
        "invoice_document_date","invoice_order_date","invoice_due_date",
        "invoice_posting_date","invoice_shipment_date","invoice_VAT_reporting_date"
    ],
    "decimal_cols": {
        "invoice_amount":           "DECIMAL(18,2)",
        "invoice_amount_VAT":       "DECIMAL(18,2)",
        "invoice_remaining_amount": "DECIMAL(18,2)"
    },
    "ddl_overrides": {
        "invoice_amount":           "DECIMAL(18,2)",
        "invoice_amount_VAT":       "DECIMAL(18,2)",
        "invoice_remaining_amount": "DECIMAL(18,2)"
    },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "modified_on",
        "invoice_no","invoice_no1","salesorder_no",
        "customer_group","customer_no","customer_",
        "invoice_document_date","invoice_order_date","invoice_due_date",
        "invoice_posting_date","invoice_shipment_date","invoice_VAT_reporting_date",
        "invoice_external_document","invoice_cs_reference","invoice_currency",
        "invoice_amount","invoice_amount_VAT","invoice_remaining_amount",
        "invoice_cancelled","invoice_closed","invoice_corrective",
        "cs_team","cad_manager_team","invoice_location","invoice_department",
        "item_material",
        "load_ts","source_system"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,


    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sales Invoice Line

# CELL ********************

# ==============================================================
# Sales Invoice Line
# ==============================================================

sales_invoice_line_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/dbo/fa.bronze_sales_invoice_line",  

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "fa",
    "target_table":  "silver_sales_invoice_line",

    # ---- BUSINESS KEY ----
    "business_key": ["invoice_no", "invoice_line_no", "shipment_no", "shipment_line_no", "sales_order_no", "sales_order_line_no"],

      # ---- COLUMN MAP (source -> target). 1:1 mapping here.
    "col_map": {
        "customer_no":              "customer_no",
        "invoice_no":               "invoice_no",
        "invoice_lineno":           "invoice_line_no",
        "shipment_no":              "shipment_no",
        "shipment_lineno":          "shipment_line_no",
        "salesorder_no":            "sales_order_no",
        "salesorder_lineno":        "sales_order_line_no",
        "shipment_date":            "shipment_date",
        "invoice_posting_date":     "invoice_posting_date",
        "item_type":                "item_type",
        "item_no":                  "item_no",
        "item_reference":           "item_reference",
        "item_description":         "item_description",
        "item_category":            "item_category",
        "item_quantity":            "item_quantity",
        "item_uom":                 "item_uom",
        "invoiceline_unit_price":   "invoiceline_unit_price",
        "item_unit_cost":           "item_unit_cost",
        "item_unit_cost_THB":       "item_unit_cost_THB",
        "invoiceline_amount":       "invoice_line_amount",
        "invoiceline_amount_VAT":   "invoice_line_amount_VAT",
        "item_posting_group":       "item_posting_group",
        "item_location":            "item_location",
        "invoiceline_return_reason":"invoiceline_return_reason",
        "cad_manager_team":         "cad_manager_team",
        "invoiceline_department":   "invoice_line_department",
        "item_material":            "item_material",
        "created_on":               "created_on",
        "modified_on":              "modified_on"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col":   "modified_on",
    "created_src_col":    "created_on",
    "modified_target_col":"modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on","modified_on","shipment_date","invoice_posting_date"
    ],
    "decimal_cols": {
        "item_quantity":             "DECIMAL(18,2)",
        "invoiceline_unit_price":    "DECIMAL(18,2)",
        "item_unit_cost":            "DECIMAL(18,2)",
        "item_unit_cost_THB":        "DECIMAL(18,2)",
        "invoiceline_amount":        "DECIMAL(18,2)",
        "invoiceline_amount_VAT":    "DECIMAL(18,2)"
    },
    "ddl_overrides": {
        "item_quantity":             "DECIMAL(18,2)",
        "invoiceline_unit_price":    "DECIMAL(18,2)",
        "item_unit_cost":            "DECIMAL(18,2)",
        "item_unit_cost_THB":        "DECIMAL(18,2)",
        "invoiceline_amount":        "DECIMAL(18,2)",
        "invoiceline_amount_VAT":    "DECIMAL(18,2)"
    },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on","modified_on",
        "customer_no","invoice_no","invoice_line_no",
        "shipment_no","shipment_line_no",
        "sales_order_no","sales_order_line_no",
        "shipment_date","invoice_posting_date",
        "item_type","item_no","item_reference","item_description","item_category",
        "item_quantity","item_uom",
        "invoice_line_unit_price","item_unit_cost","item_unit_cost_THB",
        "invoice_line_amount","invoice_line_amount_VAT",
        "item_posting_group","item_location","invoice_line_return_reason",
        "cad_manager_team","invoice_line_department","item_material",
        "load_ts","source_system"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,


    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Shipment Header

# CELL ********************

# ==============================================================
# Shipment Header
# ==============================================================

shipment_header_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/dbo/fa.bronze_shipment_header",  

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "fa",
    "target_table":  "silver_shipment_header",

    # ---- BUSINESS KEY ----
    "business_key": ["shipment_no", "sales_order_no"],

     # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "shipment_no":              "shipment_no",
        "salesorder_no":            "sales_order_no",
        "customer_no":              "customer_no",
        "customer_":            "customer_",
        "shipment_document_date":   "shipment_document_date",
        "shipment_order_date":      "shipment_order_date",
        "shipment_requested_date":  "shipment_requested_date",
        "shipment_promised_date":   "shipment_promised_date",
        "shipment_due_date":        "shipment_due_date",
        "shipment_posting_date":    "shipment_posting_date",
        "shipment_shipment_date":   "shipment_shipment_date",
        "shipment_cs_reference":    "shipment_cs_reference",
        "cs_team":                  "cs_team",
        "shipment_currency":        "shipment_currency",
        "ship_to_customer":         "ship_to_customer",
        "bill_to_customer":         "bill_to_customer",
        "shipment_location":        "shipment_location",
        "shipment_department":      "shipment_department",
        "item_material":            "item_material",
        "created_on":               "created_on",
        "modified_on":              "modified_on"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col":   "modified_on",
    "created_src_col":    "created_on",
    "modified_target_col":"modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on","modified_on",
        "shipment_document_date","shipment_order_date","shipment_requested_date",
        "shipment_promised_date","shipment_due_date","shipment_posting_date",
        "shipment_shipment_date"
    ],
    "decimal_cols": {},  # no numeric conversions here
    "ddl_overrides": {},

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on","modified_on",
        "shipment_no","sales_order_no","customer_no","customer_",
        "shipment_document_date","shipment_order_date",
        "shipment_requested_date","shipment_promised_date","shipment_due_date",
        "shipment_posting_date","shipment_shipment_date",
        "shipment_cs_reference","cs_team","shipment_currency",
        "ship_to_customer","bill_to_customer","shipment_location",
        "shipment_department","item_material",
        "load_ts","source_system"
    ],

    "custom_transform": apply_enum_maps,


    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Shipment Line

# CELL ********************

# ==============================================================
# Shipment Line
# ==============================================================

shipment_line_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/dbo/fa.bronze_shipment_line",  

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "fa",
    "target_table":  "silver_shipment_line",

    # ---- BUSINESS KEY ----
    "business_key": ["shipment_no", "shipment_line_no", "sales_order_no", "sales_order_line_no"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "created_on": "created_on",
        "modified_on": "modified_on",
        "shipment_no": "shipment_no",
        "shipment_lineno": "shipment_line_no",
        "salesorder_no": "sales_order_no",
        "salesorder_lineno": "sales_order_line_no",
        "customer_no": "customer_no",
        "shipmentline_requested_date": "shipment_line_requested_date",
        "shipmentline_promised_date": "shipment_line_promised_date",
        "shipmentline_posting_date": "shipment_posting_date",
        "shipmentline_shipment_date": "shipment_line_shipment_date",
        "shipmentline_plan_delivery": "shipment_line_planned_delivery",
        "shipmentline_plan_shipment": "shipment_line_planned_shipment",
        "item_no": "item_no",
        "item_description": "item_description",
        "item_uom": "item_uom",
        "item_quantity": "item_quantity",
        "item_quantity_invoiced": "item_quantity_invoiced",
        "item_shipped_not_invoiced": "item_shipped_not_invoiced",
        "shipment_currency": "shipment_currency",
        "item_location": "item_location",
        "shipment_department": "shipment_department",
        "item_material": "item_material"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "modified_on",   # no SinkModifiedOn available
    "created_src_col": "created_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on", #"modified_on",
        "shipmentline_requested_date", "shipmentline_promised_date",
        "shipmentline_posting_date", "shipmentline_shipment_date",
        "shipment_line_planned_delivery", "shipment_line_planned_shipment"
    ],
    "decimal_cols": {
        "item_quantity":             "DECIMAL(18,2)",
        "item_quantity_invoiced":    "DECIMAL(18,2)",
        "item_shipped_not_invoiced": "DECIMAL(18,2)"
    },
    "ddl_overrides": {
        "item_quantity":             "DECIMAL(18,2)",
        "item_quantity_invoiced":    "DECIMAL(18,2)",
        "item_shipped_not_invoiced": "DECIMAL(18,2)"
    },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on","modified_on",
        "shipment_no","shipment_line_no",
        "sales_order_no","sales_order_line_no",
        "customer_no",
        "shipment_line_requested_date","shipment_line_promised_date",
        "shipment_posting_date","shipment_line_shipment_date",
        "shipment_line_planned_delivery","shipment_line_planned_shipment",
        "item_no","item_description","item_uom","item_location","item_material",
        "item_quantity","item_quantity_invoiced","item_shipped_not_invoiced",
        "shipment_currency","shipment_department",
        "load_ts","source_system"
    ],

    "custom_transform": apply_enum_maps,


    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Commons

# MARKDOWN ********************

# ## Employee RFID Mapping

# CELL ********************

employee_rfid_mapping_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/cr535_employeerfidmapping",  

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "cmn",
    "target_table":  "silver_employee_rfid_mapping",

    # ---- NATURAL KEY ----
    # typically prod_order_no + prod_order_line_no uniquely identifies a line
    "business_key": ["Employee_Code"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "modifiedon": "modified_on",
        "cr535_employeerfidmappingid": "employee_rfid_mapping",
        "cr535_employeeid": "Employee_Code",
        "cr535_employeename": "First_Name_Eng",
        "cr535_lastname": "Last_Name_Eng",
        "cr535_antenna": "antenna_id",
        "cr535_cellno": "sub_department_Eng",
        "cr535_machinecenterno": "machine_center_no",
        "cr535_readerid": "reader_id",
    },

    # ---- WATERMARK SETTINGS ----
    # No SinkModifiedOn, so use modified_on directly
    "modified_src_col": "modified_on",
    "created_src_col": "created_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on", "modified_on"
    ],
    # "decimal_cols": {
    #     "sales_order_amount_VAT": "DECIMAL(18,2)",
    #     "sales_order_amount": "DECIMAL(18,2)"
    # },
    # "ddl_overrides": {
    #     "sales_order_amount_VAT": "DECIMAL(18,2)",
    #     "sales_order_amount": "DECIMAL(18,2)"
    # },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on", "modified_on",
        "employee_rfid_mapping", "Employee_Code",
        "First_Name_Eng", "Last_Name_Eng", "antenna_id",
        "sub_department_Eng", "machine_center_no", "reader_id",
    ],

    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Machine Center

# CELL ********************

# ==============================================================
# Machine Center
# ==============================================================

machine_center_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/cr535_machinecenter",  

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "cmn",
    "target_table":  "silver_machine_center",

    # ---- NATURAL KEY ----
    # typically prod_order_no + prod_order_line_no uniquely identifies a line
    "business_key": ["machine_center_no"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "modifiedon": "modified_on",
        "cr535_blocked": "blocked",
        "cr535_no": "machine_center_no",
        "cr535_workcenterno": "work_center_no",
        "cr535_": "machine_",
        "cr535_machineemployeemapping": "machine_employee_mapping",
        "cr535_departmentgroup": "department_group",
        "cr535_departmentsequence": "department_sequence",
        "cr535_reportmapping": "report_mapping",
        "cr535_reportsequence": "report_sequence",
        "cr535_costingmapping": "costing_mapping",
        "cr535_costingsequence": "costing_sequence",
    },

    # ---- WATERMARK SETTINGS ----
    # No SinkModifiedOn, so use modified_on directly
    "modified_src_col": "modified_on",
    "created_src_col": "created_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on", "modified_on"
    ],
    # "decimal_cols": {
    #     "sales_order_amount_VAT": "DECIMAL(18,2)",
    #     "sales_order_amount": "DECIMAL(18,2)"
    # },
    # "ddl_overrides": {
    #     "sales_order_amount_VAT": "DECIMAL(18,2)",
    #     "sales_order_amount": "DECIMAL(18,2)"
    # },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on", "modified_on",
        "blocked", "machine_center_no",
        "work_center_no", "machine_", "machine_employee_mapping",
        "department_group", "department_sequence", "report_mapping", 
        "report_sequence", "costing_mapping", "costing_sequence",
    ],

    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell List

# CELL ********************

# ==============================================================
# Cell List
# ==============================================================

cell_list_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/cr535_cellemaillist",  

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "prod",
    "target_table":  "silver_cell_list",

    # ---- NATURAL KEY ----
    # typically prod_order_no + prod_order_line_no uniquely identifies a line
    "business_key": ["email_address"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "modifiedon": "modified_on",
        "cr535_emailaddress": "email_address",
        "cr535_locationcode": "cell_line",
        "cr535_prodline": "prod_line",
        "cr535_subdepartment": "sub_department",
    },

    # ---- WATERMARK SETTINGS ----
    # No SinkModifiedOn, so use modified_on directly
    "modified_src_col": "modified_on",
    "created_src_col": "created_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on", "modified_on"
    ],


    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on", "modified_on",
        "email_address", "cell_line",
        "prod_line", "sub_department",  ],

    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## No of Employees in Each Cell

# CELL ********************

# ==============================================================
# Cell List
# ==============================================================

emp_in_cell_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/cr535_silver_cell_employee",  

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "cmn",
    "target_table":  "silver_emp_in_cell",

    # ---- NATURAL KEY ----
    # typically prod_order_no + prod_order_line_no uniquely identifies a line
    "business_key": ["cell_employee_id"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "modifiedon": "modified_on",
        "cr535_cell_no": "cell_no",
        "cr535_member": "member",
        "cr535_name": "prod_line",
        "cr535_position": "position",
        "cr535_silver_cell_employeeid": "cell_employee_id"
    },

    # ---- WATERMARK SETTINGS ----
    # No SinkModifiedOn, so use modified_on directly
    "modified_src_col": "modified_on",
    "created_src_col": "created_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on", "modified_on"
    ],


    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on", "modified_on",
        "cell_no", "member",
        "prod_line", "position",  "cell_employee_id"],

    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Product Development

# MARKDOWN ********************

# ## NPD Worklogs

# CELL ********************

# ==============================================================
# NPD WorkLogs
# ==============================================================

npd_worklogs_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/dts_npd_worklogs",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "pd",
    "target_table":  "silver_npd_worklogs",

    # ---- NATURAL KEY ----
    # Best practical key from your output columns
    "business_key": ["pd_sketch_item", "pd_step"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "createdbyname": "created_by",
        "modifiedon": "modified_on",
        "modifiedbyname": "modified_by",

        "cr535_sketchnumbertext": "pd_sketch_item",
        "dts_cadrequestidname": "pd_sketch_item_1",
        "cr535_mastersketchnumber": "pd_sketch_item_master",

        "cr535_routingstepstext": "pd_step",
        "dts_routingstepsidname": "pd_step_1",

        "cr535_iswaxreceivedcompleted": "wax_received",
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "modified_on",
    "created_src_col": "created_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on",
        "modified_on",
    ],

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on",
        "created_by",
        "modified_on",
        "modified_by",

        "pd_sketch_item",
        "pd_sketch_item_1",
        "pd_sketch_item_master",

        "pd_step",
        "pd_step_1",

        "wax_received",
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Routing Step

# CELL ********************

# ==============================================================
# NPD Routing Steps
# ==============================================================

routing_steps_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/cr535_npd_routingsteps",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "pd",
    "target_table":  "silver_routing_steps",

    # ---- NATURAL KEY ----
    # Typically pd_routing uniquely identifies the routing step
    "business_key": ["pd_routing"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "modifiedon": "modified_on",
        "dts_stepname": "pd_routing",
        "dts_sequenceorder": "pd_routing_sequence",

        "statecodename": "state_code",
        "statuscodename": "status_code",
    },

    # ---- WATERMARK SETTINGS ----
    # No SinkModifiedOn, so use modified_on directly
    "modified_src_col": "modified_on",
    "created_src_col": "created_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on",
        "modified_on"
    ],
    # "decimal_cols": {
    #     "pd_routing_sequence": T.DecimalType(10, 0)
    # },
    # "ddl_overrides": {
    #     "pd_routing_sequence": "INT"
    # },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        # watermarks
        "created_on", "modified_on",

        # routing details
        "pd_routing", "pd_routing_sequence",

        # optional unmapped placeholders
        "state_code", "status_code"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sketch Mapping

# CELL ********************

# ==============================================================
# Sketch Mapping (UPDATED to match provided SELECT mapping)
# ==============================================================

sketch_mapping_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/cr535_npd_sketchmapping",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "pd",
    "target_table":  "silver_sketch_mapping",

    # ---- NATURAL KEY ----
    # Your new mapping no longer outputs pd_routing, so use a practical compound key
    "business_key": ["pd_sketch_item", "prod_order_no"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        # watermarks
        "createdon": "created_on",
        "modifiedon": "modified_on",

        # sketch
        "cr535_sketchnumber": "pd_sketch_item",

        # order type
        "cr535_ordertype": "order_type",

        # customer (NOTE: your SQL shows customer_no twice; keep both sources but map to distinct targets)
        # SQL:
        #   cr535_customerapprovalidname as customer_no,
        #   cr535_customernumber as customer_no,
        #
        # To avoid duplicate target columns, we store the "name" version separately.
        "cr535_customerapprovalidname": "customer_no_name",
        "cr535_customernumber": "customer_no",

        # item
        "cr535_itemnoname": "item_no",
        "dts_itemnumbertext": "item_no_1",
        "cr535_itemdescription": "item_description",
        "cr535_itemquantity": "item_quantity",
        "cr535_itembaseunitofmeasure": "item_uom",

        # production
        "cr535_prodordertypemaster": "prod_order_type",
        "cr535_productionidtext": "prod_order_no",
        "cr535_productionorderidname": "prod_order_no_1",
        "cr535_productionorderlineidtext": "prod_order_no_2",
        "cr535_productionorderlinenoname": "prod_order_no_3",
        "cr535_productionorderno2": "prod_order_no_4",
        "cr535_productorderlineno": "prod_order_line_no",
        "cr535_productionorderlineno2": "prod_order_line_no_1",
        "cr535_productionordername": "prod_order_description",

        # sales
        "cr535_salesorderno": "sales_order_no",
        "cr535_so": "sales_order_no_1",
        "cr535_salesorderlineno": "sales_order_line_no",
        "cr535_salesorderlinenomaster": "sales_order_line_no_1",
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "modified_on",
    "created_src_col": "created_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on",
        "modified_on",
    ],

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on",
        "modified_on",

        "pd_sketch_item",
        "order_type",

        # customer (split to avoid duplicate target column)
        "customer_no_name",
        "customer_no",

        "item_no",
        "item_no_1",
        "item_description",
        "item_quantity",
        "item_uom",

        "prod_order_type",
        "prod_order_no",
        "prod_order_no_1",
        "prod_order_no_2",
        "prod_order_no_3",
        "prod_order_no_4",
        "prod_order_line_no",
        "prod_order_line_no_1",
        "prod_order_description",

        "sales_order_no",
        "sales_order_no_1",
        "sales_order_line_no",
        "sales_order_line_no_1",
    ],

    # ---- CUSTOM TRANSFORM ----
    # keep as-is (only applies if your pipeline expects it)
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## NPD Sketch Master

# CELL ********************

# ==============================================================
# NPD Sketch Master
# ==============================================================

sketch_master_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/cr535_npd_sketchmaster",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "pd",
    "target_table":  "silver_sketch_master",

    # ---- NATURAL KEY ----
    # Typically pd_sketch_item uniquely identifies the sketch
    "business_key": ["pd_sketch_item"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "modifiedon": "modified_on",
        "dts_isarchive": "sketch_archive_status",
        "dts_sketchnumber": "pd_sketch_item",
        "dts_mastersketchnumber": "master_sketch",
        "dts_status": "sketch_status",
        "dts_submissiondate": "sketch_submission_date",
        "dts_customerrequestdate": "sketch_request_date",
        "cr535_promiseddate": "sketch_promised_date",
        "dts_estimateddevelopmenttime": "sketch_development_time",
        "dts_estimatedproductionreadytime": "sketch_production_time",
        "dts_committedweek": "sketch_committed_date",
        "dts_priority": "sketch_piority",
        "dts_customer": "customer_no",
        "cr535_customernametext": "customer_name",
        "dts_collectionid": "sketch_customer_collection",
        "dts_customerdesignnumber": "sketch_customer_design",
        "cr535_description": "sketch_noted_for_cad",
        "dts_comments": "sketch_comment",
        "dts_targetcustomerprice": "sketch_target_price",
        "cr535_assginedengineer": "sketch_assgined_engineer",
        "dts_croppeddesignimage_url": "sketch_image",
        "cr535_additionalphoto_url": "sketch_addition_image",
        "cr535_statusdescription": "sketch_cs_noted",
        "cr535_skipmappingsample": "sketch_mapping_sample_status",
        "cr535_skipmappingmasterpiece": "sketch_mapping_master_status",
        "cr535_repaircomments": "sketch_repair_comment",
        "cr535_isopenforrepair": "sketch_repair_status",

        # no targets defined — left blank (you can fill these if needed)
        # "cr535_createdbyemail": "",
        # "statecode": "",
        # "statuscode": "",
        # "dts_priority": "",
        # "dts_customertargetprice": "",
        # "cr535_reopensketchnumber": "",

        # Optional suggestions (uncomment if applicable)
        "cr535_createdbyemail": "created_by_email",
        "statecode": "state_code",
        "statuscode": "status_code",
        "dts_customertargetprice": "customer_target_price",
        "cr535_reopensketchnumber": "reopen_sketch_number",
    },

    # ---- WATERMARK SETTINGS ----
    # No SinkModifiedOn, so use modified_on directly
    "modified_src_col": "modified_on",
    "created_src_col": "created_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on",
        "modified_on",
        "sketch_submission_date",
        "sketch_request_date",
        "sketch_promised_date",
        "sketch_committed_date"
    ],
    # "decimal_cols": {
    #     "sketch_target_price": "DECIMAL(18,2)"
    # },
    # "ddl_overrides": {
    #     "sketch_target_price": "DECIMAL(18,2)"
    # },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        # watermarks
        "created_on", "modified_on",

        # primary sketch identifiers
        "pd_sketch_item", "master_sketch",

        # timestamps
        "sketch_submission_date", "sketch_request_date",
        "sketch_promised_date", "sketch_committed_date",

        # sketch details
        "sketch_status", "sketch_archive_status",
        "sketch_development_time", "sketch_production_time",
        "sketch_piority", "sketch_target_price",

        # customer info
        "customer_no", "customer_name",
        "sketch_customer_collection", "sketch_customer_design",

        # design & notes
        "sketch_noted_for_cad", "sketch_comment",
        "sketch_assgined_engineer",
        "sketch_image", "sketch_addition_image",
        "sketch_cs_noted",

        # mapping & repair
        "sketch_mapping_sample_status", "sketch_mapping_master_status",
        "sketch_repair_comment", "sketch_repair_status",

        # additional metadata (optional if filled)
        "statecodename", "statuscodename"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cad Request

# CELL ********************

# ==============================================================
# CAD Requests
# ==============================================================

cad_requests_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/dts_cadrequests",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "pd",
    "target_table":  "silver_cad_requests",

    # ---- NATURAL KEY ----
    # CAD request record is usually unique per sketch number
    "business_key": ["pd_sketch_item"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "createdbyname": "created_by",
        "modifiedon": "modified_on",
        "modifiedbyname": "modified_by",

        "dts_status": "cad_status",

        "dts_sketchnumber": "pd_sketch_item",
        "dts_mastersketchnumber": "pd_sketch_item_master",
        "dts_priority": "pd_sketch_item_piority",

        "dts_customerdesignnumber": "customer_design_number",

        "cr535_statusdescription": "cs_description",
        "cr535_description": "cs_noted",
        "dts_comments": "cs_comment",

        "dts_submissiondate": "submission_date",
        "dts_customerrequestdate": "requested_date",
        "cr535_promiseddate": "promised_date",
        "dts_committedweek": "commited_week",

        "dts_collectionidname": "pd_sketch_collection",

        "dts_customername": "customer_no",
        "cr535_customernametext": "customer_name",

        "dts_targetcustomerprice": "customer_price",
        "dts_customertargetprice": "customer_target_price",

        "dts_croppeddesignimage_url": "sketch_image",
        "cr535_additionalphoto_url": "additional_image",

        "cr535_assginedengineername": "engineer_name",

        # no alias in your SQL, so keep clean target name
        "cr535_createdbyemail": "created_by_email",

        "cr535_repaircomments": "repair_comment",

        "dts_estimateddevelopmenttime": "est_for_development",
        "dts_estimatedproductionreadytime": "est_for_prod",

        "dts_isarchive": "is_archive",
        "cr535_reopensketchnumber": "reopen_sketch",
        "cr535_isopenforrepair": "is_open_for_repair",

        "cr535_skipmappingmasterpiece": "skip_mst",
        "cr535_skipmappingsample": "skip_sp",
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "modified_on",
    "created_src_col": "created_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on",
        "modified_on",
        "submission_date",
        "requested_date",
        "promised_date",
    ],

    # If your framework supports decimal typing overrides, you can uncomment:
    # "decimal_cols": {
    #     "customer_price": "DECIMAL(18,2)",
    #     "customer_target_price": "DECIMAL(18,2)",
    #     "est_for_development": "DECIMAL(18,2)",
    #     "est_for_prod": "DECIMAL(18,2)",
    # },
    # "ddl_overrides": {
    #     "customer_price": "DECIMAL(18,2)",
    #     "customer_target_price": "DECIMAL(18,2)",
    #     "est_for_development": "DECIMAL(18,2)",
    #     "est_for_prod": "DECIMAL(18,2)",
    # },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on",
        "created_by",
        "modified_on",
        "modified_by",

        "dts_status",

        "pd_sketch_item",
        "pd_sketch_item_master",
        "pd_sketch_item_piority",
        "customer_design_number",

        "cs_description",
        "cs_noted",
        "cs_comment",

        "submission_date",
        "requested_date",
        "promised_date",
        "commited_week",

        "pd_sketch_collection",

        "customer_no",
        "customer_name",
        "customer_price",
        "customer_target_price",

        "sketch_image",
        "additional_image",

        "engineer_name",
        "created_by_email",

        "repair_comment",

        "est_for_development",
        "est_for_prod",

        "is_archive",
        "reopen_sketch",
        "is_open_for_repair",
        "skip_mst",
        "skip_sp",
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Engineer Time Log

# CELL ********************

# ==============================================================
# Engineer Time
# ==============================================================

engineer_time_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/dts_engineertimelog",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "pd",
    "target_table":  "silver_engineer_time",

    # ---- NATURAL KEY ----
    # Time logs are best uniquely identified by sketch + step + start time
    "business_key": ["pd_sketch_item", "pd_step", "pd_start"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "createdbyname": "created_by",
        "modifiedon": "modified_on",
        "modifiedbyname": "modified_by",

        "cr535_sketchnumbertext": "pd_sketch_item",
        "dts_cadrequestidname": "pd_sketch_item_1",

        # note: keeping your spelling to stay consistent with the SELECT
        "cr535_cadversionsidtext": "pd_sketch_verion",
        "dts_npd_cadversionsname": "pd_sketch_verion_1",

        "cr535_routingsteps": "pd_step",

        "dts_starttime": "pd_start",
        "dts_endtime": "pd_stop",
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "modified_on",
    "created_src_col": "created_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on",
        "modified_on",
    ],

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on",
        "created_by",
        "modified_on",
        "modified_by",

        "pd_sketch_item",
        "pd_sketch_item_1",

        "pd_sketch_verion",
        "pd_sketch_verion_1",

        "pd_step",

        "pd_start",
        "pd_stop",
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Manager Version

# CELL ********************

# ==============================================================
# Sketch Manager (CAD Versions)
# ==============================================================

sketch_manager_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/dts_npd_cadversions",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "pd",
    "target_table":  "silver_sketch_manager",

    # ---- NATURAL KEY ----
    # Version records are typically unique per sketch + version number
    "business_key": ["pd_sketch_item", "pd_sketch_verion"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "createdbyname": "created_by",
        "modifiedon": "modified_on",

        # NOTE: your SQL has [modifiedbyname] as created_by (typo).
        # Keeping a clean target name instead:
        "modifiedbyname": "modified_by",

        "cr535_sketchnumber": "pd_sketch_item",
        "dts_cadrequestidname": "pd_sketch_item_1",

        "dts_managerdecisiondate": "mng_decision_date",
        "cr535_approvalstatus": "mng_decision_status",
        "cr535_managerapprovalstatusresponse": "mng_decision_comment",

        "cr535_salespersoncode": "cs_team",
        "cr535_versionnumberdisplay": "pd_sketch_mng_verion",

        "dts_changesdescription": "cs_noted",
        "dts_improvementestimatedate": "pd_sketch_improve_date",
        "dts_improvementestimatehour": "pd_sketch_improve_hour",

        "dts_mastersketchnumber": "pd_sketch_item_master",
        "dts_versionnumber": "pd_sketch_verion",
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "modified_on",
    "created_src_col": "created_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on",
        "modified_on",
        "mng_decision_date",
        "pd_sketch_improve_date",
    ],

    # if your framework supports numeric typing overrides, you can uncomment:
    "decimal_cols": {
        "pd_sketch_improve_hour": "DECIMAL(18,2)"
    },
    "ddl_overrides": {
        "pd_sketch_improve_hour": "DECIMAL(18,2)"
    },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on",
        "created_by",
        "modified_on",
        "modified_by",

        "pd_sketch_item",
        "pd_sketch_item_1",

        "mng_decision_date",
        "mng_decision_status",
        "mng_decision_comment",

        "cs_team",
        "pd_sketch_mng_verion",

        "cs_noted",
        "pd_sketch_improve_date",
        "pd_sketch_improve_hour",

        "pd_sketch_item_master",
        "pd_sketch_verion",
    ],

    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sketch Customer Approval

# CELL ********************

# ==============================================================
# Sketch Customer Approval
# ==============================================================

sketch_customer_approval_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/dts_npd_customerapproval",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "pd",
    "target_table":  "silver_sketch_customer_approval",

    # ---- NATURAL KEY ----
    # Customer approval is typically per sketch + version
    "business_key": ["pd_sketch_item", "pd_sketch_verion"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "createdbyname": "created_by",
        "modifiedon": "modified_on",
        "modifiedbyname": "modified_by",

        "cr535_customerlookupname": "customer_no",
        "cr535_customername": "customer_name",
        "cr535_salespersoncode": "cs_team",

        "cr535_cadrequestdisplay": "pd_sketch_item",
        "cr535_cadrequestidname": "pd_sketch_item_1",
        "cr535_cadversionidname": "pd_sketch_verion",

        "cr535_customerapprovalstatus": "customer_decision_status",
        "cr535_approvedversiondisplay": "pd_sketch_customer_verion",

        "cr535_customerchangesdescription": "cs_noted",
        "dts_customerdecisiondate": "customer_decision_date",

        "dts_name": "system_version",
        "dts_sendto3dprinting": "send_print",
        "dts_sendtowaxing": "send_wax",
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "modified_on",
    "created_src_col": "created_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on",
        "modified_on",
        "customer_decision_date",
    ],

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on",
        "created_by",
        "modified_on",
        "modified_by",

        "customer_no",
        "customer_name",
        "cs_team",

        "pd_sketch_item",
        "pd_sketch_item_1",
        "pd_sketch_verion",

        "customer_decision_status",
        "pd_sketch_customer_verion",

        "cs_noted",
        "customer_decision_date",

        "system_version",
        "send_print",
        "send_wax",
    ],

    # ---- CUSTOM TRANSFORM ----
    # Use this to map customer_decision_status codes if needed
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sketch Size

# CELL ********************

# ==============================================================
# Sketch Size (Child CAD Size Request)
# ==============================================================

sketch_size_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/dts_npd_childcadsizerequest",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "pd",
    "target_table":  "silver_sketch_size",

    # ---- NATURAL KEY ----
    # Best practical key from your output columns
    "business_key": ["pd_sketch_item_size"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "createdbyyominame": "created_by",
        "modifiedon": "modified_on",

        "dts_parentidname": "link_size",
        "cr535_mastersketchnumber": "pd_sketch_item_master",
        "cr535_sizedescription": "item_type",
        "cr535_sketchnumbername": "pd_sketch_item",
        "dts_mastersizeidname": "pd_sketch_item_size",
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "modified_on",
    "created_src_col": "created_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on",
        "modified_on",
    ],

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on",
        "created_by",
        "modified_on",
        "link_size",
        "pd_sketch_item_master",
        "item_type",
        "pd_sketch_item",
        "pd_sketch_item_size",
    ],

    # ---- CUSTOM TRANSFORM ----
    # If you have enum mapping or normalization logic, keep it; otherwise set to None.
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sketch Master

# CELL ********************

# ==============================================================
# Sketch Size Master (CAD Size Request)
# ==============================================================

sketch_size_master_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/dts_npd_cadsizerequest",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "pd",
    "target_table":  "silver_sketch_size_master",

    # ---- NATURAL KEY ----
    # Master size request identifier
    "business_key": ["pd_sketch_item_master"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "createdbyname": "created_by",
        "modifiedon": "modified_on",
        "modifiedbyname": "modified_by",

        "dts_name": "link_size",
        "cr535_cadrequestidtext": "pd_sketch_item_master",
        "dts_cadrequestidname": "pd_sketch_item",
        "dts_itemidname": "item_no",
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "modified_on",
    "created_src_col": "created_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on",
        "modified_on",
    ],

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on",
        "created_by",
        "modified_on",
        "modified_by",
        "link_size",
        "pd_sketch_item_master",
        "pd_sketch_item",
        "item_no",
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3D Printing Batch Master

# CELL ********************

# ==============================================================
# 3D Printing Batch Master
# ==============================================================

printing_batch_master_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/cr535_npd_3dprintingbatchmaster",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "pd",
    "target_table":  "silver_3dprintingbatch_master",

    # ---- NATURAL KEY ----
    "business_key": ["pd_sketch_batch"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "createdbyname": "created_by",
        "modifiedon": "modified_on",

        "cr535_batchno": "pd_sketch_batch",
        "dts_isreadytoprint": "batch_print",
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "modified_on",
    "created_src_col": "created_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on",
        "modified_on",
    ],

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on",
        "created_by",
        "modified_on",
        "pd_sketch_batch",
        "batch_print",
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3D Printing batch

# CELL ********************

# ==============================================================
# 3D Printing Batch
# ==============================================================

printing_batch_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/cr535_npd_3dprintingbatch",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "pd",
    "target_table":  "silver_3d_printing_batch",

    # ---- NATURAL KEY ----
    # Practical key: batch + sketch (or add prod_order_no_1 if you want tighter uniqueness)
    "business_key": ["pd_sketch_batch", "pd_sketch_item_1"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "createdbyname": "created_by",
        "modifiedon": "modified_on",

        "cr535_sketchnumbername": "pd_sketch_item",
        "cr535_sketchnumbertext": "pd_sketch_item_1",
        "cr535_issketcharchive": "pd_sketch_item_archive",

        "cr535_batchnotext": "pd_sketch_batch",

        "cr535_itemnoname": "item_no",
        "cr535_itemnotext": "item_no_1",
        "cr535_itemnodescription": "item_description",

        "cr535_productionorderidname": "prod_order_no",
        "cr535_productionorderidtext": "prod_order_no_1",
        "cr535_productionorderqty": "prod_order_no_qty",

        "cr535_totalitems": "item_batch_qty",
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "modified_on",
    "created_src_col": "created_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on",
        "modified_on",
    ],

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on",
        "created_by",
        "modified_on",

        "pd_sketch_item",
        "pd_sketch_item_1",
        "pd_sketch_item_archive",
        "pd_sketch_batch",

        "item_no",
        "item_no_1",
        "item_description",

        "prod_order_no",
        "prod_order_no_1",
        "prod_order_no_qty",

        "item_batch_qty",
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Wax Receive

# CELL ********************

# ==============================================================
# Wax Receive
# ==============================================================

wax_receive_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/cr535_npd_waxreceive",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "pd",
    "target_table":  "silver_wax_receive",

    # ---- NATURAL KEY ----
    # Wax receive is typically unique per sketch + batch
    "business_key": ["pd_sketch_item", "pd_sketch_batch"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "createdbyname": "created_by",
        "modifiedon": "modified_on",

        "cr535_sketchnumbertext": "pd_sketch_item",
        "cr535_batchdetailidtext": "pd_sketch_batch",

        "cr535_itemreceivestatus": "wax_status",
        "cr535_rejectioncomments": "reject_comment",
        "cr535_rejectionreason": "reject_reason",
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "modified_on",
    "created_src_col": "created_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on",
        "modified_on",
    ],

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on",
        "created_by",
        "modified_on",

        "pd_sketch_item",
        "pd_sketch_batch",

        "wax_status",
        "reject_comment",
        "reject_reason",
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## PD step

# CELL ********************

# ==============================================================
# PD Step (CAD Routing Steps)
# ==============================================================

pd_step_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/dts_npd_cadroutingsteps",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "pd",
    "target_table":  "silver_pd_step",

    # ---- NATURAL KEY ----
    # Step name is typically unique in routing
    "business_key": ["pd_step", "pd_sequence", ],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "dts_sequenceorder": "pd_sequence",
        "dts_stepname": "pd_step",
        "modifiedon": "modified_on",
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "modified_on",
    "created_src_col": "modified_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "modified_on",
    ],

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "modified_on",
        "pd_sequence",
        "pd_step",
    ],

    # ---- CUSTOM TRANSFORM ----
    # No enum logic needed; set to None if your framework allows
    "custom_transform": None,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 0,
    "full_load": True,                 # dimension-style table
    "full_load_repartitions": 8
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sketch Collection

# CELL ********************

# ==============================================================
# Sketch Collection
# ==============================================================

sketch_collection_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/dts_npd_collection",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "pd",
    "target_table":  "silver_sketch_collection",

    # ---- NATURAL KEY ----
    # Collection name should be unique
    "business_key": ["pd_sketch_collection"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "createdbyname": "created_by",
        "modifiedon": "modified_on",

        "dts_collection": "pd_sketch_collection",
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "modified_on",
    "created_src_col": "created_on",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on",
        "modified_on",
    ],

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on",
        "created_by",
        "modified_on",
        "pd_sketch_collection",
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": None,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # set True if you want dimension-style reloads
    "full_load_repartitions": 8
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Production

# MARKDOWN ********************

# ## Production Routing Line

# CELL ********************

# ==============================================================
# Production Routing Line
# ==============================================================

prod_routing_line_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/dbo/prod.bronze_prod_order_routing_line",  

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "prod",
    "target_table":  "silver_prod_routing_line",

    # ---- NATURAL KEY ----
    # typically prod_order_no + prod_order_line_no uniquely identifies a line
    "business_key": ["prod_order_no", "prod_order_line_no", "previous_operation_no", "next_operation_no", "operation_no", "routing_no"],

    # ---- COLUMN MAP (source -> target) ----
    # "col_map": {
    #     "createdon": "created_on",
    #     "modifiedon": "modified_on",
    #     "cr535_prodorderno": "prod_order_no",
    #     "cr535_status": "prod_order_status",
    #     "cr535_routingreferenceno": "prod_order_line_no",
    #     "cr535_routingno": "item_no",
    #     "cr535_routinglinkcode": "routing_link_code",
    #     "cr535_locationcode": "location_code",
    #     "cr535_previousoperationno": "previous_operation_no",
    #     "cr535_nextoperationno": "next_operation_no",
    #     "cr535_routingstatus": "routing_status",
    #     "cr535_operationno": "operation_no",
    #     "cr535_type": "operation_type",
    #     "cr535_no": "routing_no",
    #     "cr535_runtime": "run_time",
    #     "cr535_startingdatetime": "starting_date_time",
    #     "cr535_endingdatetime": "ending_date_time",
    #     "SinkModifiedOn": "SinkModifiedOn"
    # },

    "col_map": {
        "systemCreatedAt": "created_on",
        "systemModifiedAt": "modified_on",
        "prodOrderNo": "prod_order_no",
        "status": "prod_order_status",
        "routingReferenceNo": "prod_order_line_no",
        "routingNo": "item_no",
        "routingLinkCode": "routing_link_code",
        "locationCode": "location_code",
        "previousOperationNo": "previous_operation_no",
        "nextOperationNo": "next_operation_no",
        "routingStatus": "routing_status",
        "operationNo": "operation_no",
        "type": "operation_type",
        "no": "routing_no",
        "runTime": "run_time",
        "startingDateTime": "starting_date_time",
        "endingDateTime": "ending_date_time",
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col":   "systemModifiedAt",
    "created_src_col":    "systemCreatedAt",
    "modified_target_col":"modified_on",     # final column after coalesce in transform

    # ---- TYPES ----
    "ts_cols": [
        "created_on", "modified_on",
        "starting_date_time",
        "ending_date_time",
    ],

    "decimal_cols": {
        "run_time": "DECIMAL(18,2)",
    },

    "ddl_overrides": {
        "prod_order_status": "STRING",
        "routing_status":    "STRING",
        "operation_type":         "STRING",
    },



    # ---- FINAL COLUMN ORDER ----a
    "target_cols": [
        "created_on", "modified_on",
        "prod_order_no", "prod_order_status", "prod_order_line_no",
        "item_no", "routing_link_code", "location_code",
        "previous_operation_no", "next_operation_no",
        "routing_status", "operation_no", "operation_type", "routing_no",
        "run_time",
        "starting_date_time", "ending_date_time",
        "load_ts", "source_system",
    ],


    # ---- CUSTOM TRANSFORM (optional: can reuse prod_order_header’s style) ----
    "custom_transform":  apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": False,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Production Bom

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import types as T

def transform_prod_bom(df):
    """Transform Production BOM table for Silver Lakehouse."""
    def ts(c):
        try:
            return parse_any_ts(c)
        except NameError:
            return F.to_timestamp(c)

    # --- Cast timestamp fields ---
    for c in ["createdon", "modifiedon"]:
        if c in df.columns:
            df = df.withColumn(c, ts(c))

    # --- Numeric columns (quantities, weights, scrap, etc.) ---
    numeric_cols = [
        "cr535_units_du_tsl", "cr535_quantity", "cr535_unitsper_du_tsl",
        "cr535_quantityper", "cr535_platingthickness", "cr535_scrap", "cr535_weight"
    ]
    for c in numeric_cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast(T.DecimalType(18, 4)))

    # --- Trim text columns ---
    for c in ["cr535_productionbomno","cr535_versioncode","cr535_no",
              "cr535_description","cr535_unitofmeasurecode","cr535_locationcode",
              "cr535_routinglinkcode"]:
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c)))

    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prod_bom_cfg = {
    # ---- SOURCE ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/prod/bronze_prod_bom_line",

    # ---- TARGET ----
    "target_schema": "prod",
    "target_table":  "silver_production_bom",

    # ---- NATURAL KEY ----
    "natural_key": ["bom_no", "bom_version", "item_no"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon":                   "created_on",
        "modifiedon":                  "modified_on",
        "cr535_productionbomno":       "bom_no",
        "cr535_versioncode":           "bom_version",
        "cr535_no":                    "item_no",
        "cr535_description":           "item_description",
        "cr535_units_du_tsl":          "bom_item_qty_uom2",
        "cr535_quantity":              "bom_item_qty",
        "cr535_unitsper_du_tsl":       "bom_item_qty_per_uom2",
        "cr535_quantityper":           "bom_item_qty_per",
        "cr535_unitofmeasurecode":     "bom_item_uom",
        "cr535_platingthickness":      "bom_item_plating_thickness",
        "cr535_locationcode":          "bom_item_location",
        "cr535_scrap":                 "bom_item_scrap",
        "cr535_routinglinkcode":       "routing_link_code",
        "cr535_weight":                "bom_item_weight"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col":   "modifiedon",
    "created_src_col":    "createdon",
    "modified_target_col":"modified_on",

    # ---- TYPES ----
    "ts_cols": ["created_on", "modified_on"],
    "decimal_cols": {
        "bom_item_qty_uom2":          T.DecimalType(18,4),
        "bom_item_qty":               T.DecimalType(18,4),
        "bom_item_qty_per_uom2":      T.DecimalType(18,4),
        "bom_item_qty_per":           T.DecimalType(18,4),
        "bom_item_plating_thickness": T.DecimalType(18,4),
        "bom_item_scrap":             T.DecimalType(18,4),
        "bom_item_weight":            T.DecimalType(18,4)
    },
    "ddl_overrides": {
        "bom_item_qty_uom2":          "DECIMAL(18,4)",
        "bom_item_qty":               "DECIMAL(18,4)",
        "bom_item_qty_per_uom2":      "DECIMAL(18,4)",
        "bom_item_qty_per":           "DECIMAL(18,4)",
        "bom_item_plating_thickness": "DECIMAL(18,4)",
        "bom_item_scrap":             "DECIMAL(18,4)",
        "bom_item_weight":            "DECIMAL(18,4)"
    },

    # ---- FINAL TARGET COLS ----
    "target_cols": [
        "created_on","modified_on",
        "bom_no","bom_version","item_no","item_description",
        "bom_item_qty_uom2","bom_item_qty","bom_item_qty_per_uom2","bom_item_qty_per",
        "bom_item_uom","bom_item_plating_thickness","bom_item_location","bom_item_scrap",
        "routing_link_code","bom_item_weight",
        "load_ts","source_system"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": transform_prod_bom,

    # ---- LOAD BEHAVIOR ----
    "lookback_minutes": 360,
    "full_load": True,  # or False if incremental is ready
    "full_load_repartitions": 8
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Production Bom Line

# CELL ********************

# ==============================================================
# Inventory • BOM Line (ALL columns, snake_case)
# ==============================================================

bom_line_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/dbo/inv.bom_line",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "inv",
    "target_table": "silver_bom_line",

    # ---- BUSINESS KEY ----
    "business_key": ["production_bom_no", "version_code", "line_no"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "Production_BOM_No": "production_bom_no",
        "Version_Code": "version_code",
        "Line_No": "line_no",
        "Type": "type",
        "No": "no",
        "Description": "description",
        "Calculation_Formula": "calculation_formula",
        "Length": "length",
        "Width": "width",
        "Depth": "depth",
        "Weight": "weight",
        "Units_per_DU_TSL": "units_per_du_tsl",
        "Unit_of_Measure_Units_DU_TSL": "unit_of_measure_units_du_tsl",
        "Quantity_per": "quantity_per",
        "Unit_of_Measure_Code": "unit_of_measure_code",
        "Plating_Thickness": "plating_thickness",
        "Location_Code": "location_code",
        "Scrap_Percent": "scrap_percent",
        "Routing_Link_Code": "routing_link_code",
        "CO2e_per_Unit": "co2e_per_unit",
        "Position": "position",
        "Position_2": "position_2",
        "Position_3": "position_3",
        "Lead_Time_Offset": "lead_time_offset",
        "Starting_Date": "starting_date",
        "Ending_Date": "ending_date"
    },

    # ---- WATERMARK SETTINGS ----
    # Assuming your source does not include created/modified timestamps.
    # Use Starting_Date for incremental logic if needed, or skip watermarking entirely.
    "modified_src_col": "Starting_Date",   # fallback (if incremental loads use date)
    "created_src_col": "Starting_Date",
    "modified_target_col": "starting_date",

    # ---- TYPES ----
    "ts_cols": ["starting_date", "ending_date"],

    "decimal_cols": {
        "length": "DECIMAL(18,3)",
        "width": "DECIMAL(18,3)",
        "depth": "DECIMAL(18,3)",
        "weight": "DECIMAL(18,3)",
        "quantity_per": "DECIMAL(18,3)",
        "plating_thickness": "DECIMAL(18,3)",
        "scrap_percent": "DECIMAL(9,4)",
        "co2e_per_unit": "DECIMAL(18,4)"
    },

    "ddl_overrides": {
        "length": "DECIMAL(18,3)",
        "width": "DECIMAL(18,3)",
        "depth": "DECIMAL(18,3)",
        "weight": "DECIMAL(18,3)",
        "quantity_per": "DECIMAL(18,3)",
        "plating_thickness": "DECIMAL(18,3)",
        "scrap_percent": "DECIMAL(9,4)",
        "co2e_per_unit": "DECIMAL(18,4)"
    },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "production_bom_no", "version_code", "line_no",
        "type", "no", "description", "calculation_formula",
        "length", "width", "depth", "weight",
        "units_per_du_tsl", "unit_of_measure_units_du_tsl",
        "quantity_per", "unit_of_measure_code", "plating_thickness",
        "location_code", "scrap_percent", "routing_link_code",
        "co2e_per_unit", "position", "position_2", "position_3",
        "lead_time_offset", "starting_date", "ending_date"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 360,
    "full_load": True,   # usually full load since BOM lines rarely update frequently
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Issue Header

# CELL ********************

def transform_issue_header(df):
    """Transform Sub-Contract Issue Header for Silver Lakehouse."""
    # timestamps
    for c in ["createdon", "modifiedon", "cr535_duedate"]:
        if c in df.columns:
            df = df.withColumn(c, _ts(c))

    # boolean flags → Yes/No (apply only if present)
    yn_cols = [
        "cr535_isposted", "cr535_isapproved", "cr535_isreturnposted",
        "cr535_isrecieveposted", "cr535_isdeductionposted", "cr535_isautogenerated"
    ]
    for c in yn_cols:
        if c in df.columns:
            df = df.withColumn(c, _yn(c))

    # tidy text
    text_cols = [
        "createdbyname", "modifiedbyname", "cr535_vendorname",
        "cr535_jobtypename", "cr535_subcontractpricegroupname",
        "cr535_vendorsearchname", "cr535_subcontractdocumentno",
        "cr535_subreceivedocumentno", "cr535_subreturndocumentno"
    ]
    for c in text_cols:
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c)))

    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

issue_header_cfg = {
    # ---- SOURCE ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/cr535_subcontractissueheaders",

    # ---- TARGET ----
    "target_schema": "prod",
    "target_table":  "silver_sub_contract_issue_header",

    # ---- NATURAL KEY ----
    # If this isn't unique, add a surrogate or include created_on
    "natural_key": ["sub_contract_document_no"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon":                         "created_on",
        "modifiedon":                        "modified_on",
        "createdbyname":                     "created_by",
        "cr535_newcolumn":                   "sub_contract_header",
        "cr535_duedate":                     "due_date",
        "cr535_vendorname":                  "vendor",
        "cr535_jobtypename":                 "job_type",
        "cr535_subcontractpricegroupname":   "sub_contract_price_group",
        "cr535_weightlossallowed":           "weight_loss_allowed",
        "cr535_isposted":                    "is_posted",
        "cr535_subcontractdocumentno":       "sub_contract_document_no",
        "cr535_isapproved":                  "is_approved",
        "cr535_vendorsearchname":            "vendor_search_name",
        "cr535_isreturnposted":              "is_return_posted",
        "cr535_totalreturndust":             "total_return_dust",
        "cr535_isrecieveposted":             "is_receive_posted",
        "cr535_isdeductionposted":           "is_deduction_posted",
        "cr535_subreceivedocumentno":        "sub_received_document_no",
        "cr535_subreturndocumentno":         "sub_return_document_no",
        "cr535_isautogenerated":             "is_autogenerated",
        "cr535_statementid":                 "statement_id",
        "cr535_caltotalrecieveqty":          "cal_total_receive_qty",
        "cr535_caltotalissueqty":            "cal_total_issue_qty"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col":    "modifiedon",
    "created_src_col":     "createdon",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": ["created_on", "modified_on", "due_date"],
    "decimal_cols": {},
    "ddl_overrides": {},

    # ---- FINAL TARGET COLS ----
    "target_cols": [
        "created_on","modified_on","created_by","sub_contract_header","due_date",
        "vendor","job_type","sub_contract_price_group","weight_loss_allowed",
        "is_posted","sub_contract_document_no","is_approved","vendor_search_name",
        "is_return_posted","total_return_dust","is_receive_posted","is_deduction_posted",
        "sub_received_document_no","sub_return_document_no","is_autogenerated",
        "statement_id","cal_total_receive_qty","cal_total_issue_qty",
        "load_ts","source_system"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": transform_issue_header,

    "dedupe": True,
    "lookback_minutes": 360,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Issue Line

# CELL ********************

from pyspark.sql import functions as F

def transform_sub_contract_issue_lines(df):
    """Transform Sub-Contract Issue Lines for Silver Lakehouse."""

    # --- Timestamp parsing ---
    for c in ["createdon", "modifiedon"]:
        if c in df.columns:
            df = df.withColumn(c, F.to_timestamp(c))

    # --- Boolean normalization ---
    if "cr535_select" in df.columns:
        df = df.withColumn(
            "cr535_select",
            F.when(F.col("cr535_select").isin(1, "1", True, "TRUE", "Y"), F.lit("Yes"))
             .when(F.col("cr535_select").isin(0, "0", False, "FALSE", "N"), F.lit("No"))
             .otherwise(None)
        )

    # --- Numeric casts ---
    numeric_cols = [
        "cr535_issuequantity", "cr535_issuepcsgr", "cr535_unitcost",
        "cr535_returnqty", "cr535_issueweight", "cr535_deductionqty", "cr535_deductionweight",
        "cr535_totalreceiveqty", "cr535_totalreceiveweight"
    ]
    for c in numeric_cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("decimal(18,4)"))

    # --- Trim text columns ---
    text_cols = [
        "createdbyname", "cr535_productionorderno", "cr535_jobsheetno",
        "cr535_subcontractheadername", "cr535_currentlocation", "cr535_temprocess",
        "cr535_machinecenterno", "cr535_operationno"
    ]
    for c in text_cols:
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c)))

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

issue_lines_cfg = {
    # ---- SOURCE ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/cr535_subcontractissuelines",

    # ---- TARGET ----
    "target_schema": "prod",
    "target_table":  "silver_sub_contract_issue_lines",


    # ---- NATURAL KEY ----
    "natural_key": ["prod_order_no", "prod_order_line_no"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon":                     "created_on",
        "modifiedon":                    "modified_on",
        "createdbyname":                 "created_by",
        "cr535_productionorderno":       "prod_order_no",
        "cr535_select":                  "selected",
        "cr535_productionorderlineno":   "prod_order_line_no",
        "cr535_jobsheetno":              "job_sheet_no",
        "cr535_issuequantity":           "issue_quantity",
        "cr535_subcontractheadername":   "sub_contract_header",
        "cr535_issuepcsgr":              "issue_pcsgr",
        "cr535_unitcost":                "unit_cost",
        "cr535_currentlocation":         "current_location",
        "cr535_returnqty":               "return_qty",
        "cr535_issueweight":             "issue_weight",
        "cr535_deductionqty":            "deduction_qty",
        "cr535_deductionweight":         "deduction_weight",
        "cr535_subcontractheaderid":     "sub_contract_header_id",
        "cr535_temprocess":              "temprocess",
        "cr535_totalreceiveqty":         "total_receive_qty",
        "cr535_totalreceiveweight":      "total_receive_weight",
        "cr535_machinecenterno":         "machine_center_no",
        "cr535_operationno":             "operation_no"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col":    "modifiedon",
    "created_src_col":     "createdon",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": ["created_on", "modified_on"],
    "decimal_cols": {
        "issue_quantity":       "decimal(18,4)",
        "issue_pcsgr":          "decimal(18,4)",
        "unit_cost":            "decimal(18,4)",
        "return_qty":           "decimal(18,4)",
        "issue_weight":         "decimal(18,4)",
        "deduction_qty":        "decimal(18,4)",
        "deduction_weight":     "decimal(18,4)",
        "total_receive_qty":    "decimal(18,4)",
        "total_receive_weight": "decimal(18,4)"
    },
    "ddl_overrides": {},

    # ---- FINAL TARGET COLS ----
    "target_cols": [
        "created_on",
        "modified_on",
        "created_by",
        "prod_order_no",
        "selected",
        "prod_order_line_no",
        "job_sheet_no",
        "issue_quantity",
        "sub_contract_header",
        "issue_pcsgr",
        "unit_cost",
        "current_location",
        "return_qty",
        "issue_weight",
        "deduction_qty",
        "deduction_weight",
        "sub_contract_header_id",
        "temprocess",
        "total_receive_qty",
        "total_receive_weight",
        "machine_center_no",
        "operation_no",
        "load_ts",
        "source_system"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": transform_sub_contract_issue_lines,

    "dedupe": True,
    "lookback_minutes": 360,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Plating Prep JIG

# CELL ********************

# ==============================================================
# prod • Plating Production
# ==============================================================

plating_prep_jig_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/cr535_platingprep_jig",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "prod",
    "target_table":  "silver_plating_prep_jig",

    # ---- NATURAL KEY ----
    # Typically plating_prod_no uniquely identifies a record
    "business_key": ["plating_prod_no"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "modifiedon": "modified_on",
        "cr535_newcolumn": "plating_prod_no",
        "cr535_jigname": "jig_name",
        "cr535_platingitemnoname": "jig_item_plating"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "modifiedon",
    "created_src_col": "createdon",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on",
        "modified_on"
    ],

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on",
        "modified_on",
        "plating_prod_no",
        "jig_name",
        "jig_item_plating"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sub Contract Price List

# CELL ********************

# ==============================================================
# prod • Sub Contract Price List
# ==============================================================

sub_contract_price_list_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/cr535_subcontpricegrouplist",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "prod",
    "target_table":  "silver_sub_contract_price_list",

    # ---- NATURAL KEY ----
    "business_key": ["sub_code"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "modifiedon": "modified_on",
        "cr535_code": "sub_code",
        "cr535_description": "sub_description",
        "cr535_weightlossallowed": "weight_loss"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "modifiedon",
    "created_src_col": "createdon",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on",
        "modified_on"
    ],

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on",
        "modified_on",
        "sub_code",
        "sub_description",
        "weight_loss"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sub Contract Deduction Lines

# CELL ********************

# ==============================================================
# prod • Sub Contract Deduction Lines
# ==============================================================

sub_contract_deduction_lines_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/cr535_subcontractdeductionlines",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "prod",
    "target_table":  "silver_sub_contract_deduction_lines",

    # ---- NATURAL KEY ----
    "business_key": ["defect_id", "prodorder_no"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "modifiedon": "modified_on",
        "createdbyname": "created_by",
        "cr535_defectidname": "defect_id",
        "cr535_defectname": "defect_reason",
        "cr535_subcontractissueheaderid": "sub_document",
        "cr535_subcontractissuelinesidname": "prodorder_no",
        "cr535_totaldeductionqty": "total_defect_qty",
        "cr535_totaldeductionweight": "total_defect_weight",
        "cr535_thb_pcs": "defect_amount"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "modifiedon",
    "created_src_col": "createdon",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on",
        "modified_on"
    ],
    "decimal_cols": {
        "defect_amount": "DECIMAL(18,2)",
        "total_defect_qty": "DECIMAL(18,2)",
        "total_defect_weight": "DECIMAL(18,2)"
    },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on",
        "modified_on",
        "created_by",
        "defect_id",
        "defect_reason",
        "sub_document",
        "prodorder_no",
        "total_defect_qty",
        "total_defect_weight",
        "defect_amount"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Firm Plan Log

# CELL ********************

# ==============================================================
# Manufacturing • Firm Plan Log
# ==============================================================

firm_plan_log_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/cr535_firmplan_log",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "prod",
    "target_table":  "silver_firm_plan_log",

    # ---- NATURAL KEY ----
    "business_key": ["prod_order_no", "prod_order_line_no"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "modifiedon": "modified_on",
        "cr535_assigned_cell": "assigned_cell",
        "cr535_customer_name": "customer_name",
        "cr535_finish_date": "finish_date",
        "cr535_finish_week": "finish_week",
        "cr535_item_no": "item_no",
        "cr535_load_timestamp": "load_timestamp",
        "cr535_material": "material",
        "cr535_name": "plan_name",
        "cr535_prod_order_line_no": "prod_order_line_no",
        "cr535_prod_order_no": "prod_order_no",
        "cr535_quantity": "quantity",
        "cr535_remark": "remark",
        "cr535_requested_week": "requested_week",
        "cr535_sales_order_no": "sales_order_no"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "modifiedon",
    "created_src_col": "createdon",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on",
        "modified_on",
        "finish_date",
        "load_timestamp"
    ],
    # Optionally, you can define decimals if quantity is not integer
    "decimal_cols": {
        "quantity": "DECIMAL(18,2)",
        "finish_week": "DECIMAL(18,0)",
        "required_week": "DECIMAL(18,0)"
    },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on", "modified_on",
        "assigned_cell", "customer_name",
        "finish_date", "finish_week", "requested_week",
        "item_no", "material", "plan_name",
        "prod_order_no", "prod_order_line_no",
        "sales_order_no",
        "quantity", "remark"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": True,
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Load Tables

# CELL ********************

# ==============================================================
# EXECUTE PIPELINE FOR ALL TABLES
# ==============================================================

LAKEHOUSES = {
    # "Silver_Product_Dev_Lakehouse": [
    #     sketch_mapping_cfg,
    #     npd_worklogs_cfg,
    #     printing_batch_cfg,
    #     printing_batch_master_cfg,
    #     wax_receive_cfg,
    #     # pd_step_cfg,
    #     sketch_size_cfg,
    #     sketch_size_master_cfg,
    #     engineer_time_cfg,
    #     sketch_manager_cfg,
    #     sketch_collection_cfg,
    #     sketch_customer_approval_cfg,
    #     # # routing_steps_cfg,
    #     cad_requests_cfg,
    # ],
    "Silver_Production_Lakehouse": [
        # plating_prep_jig_cfg,
        # sub_contract_price_list_cfg,
        # sub_contract_deduction_lines_cfg,
        # prod_routing_line_cfg,
        cell_list_cfg,
        # firm_plan_log_cfg,
        # issue_header_cfg,
        # issue_lines_cfg,
        # prod_bom_cfg,
        
    ],
    # "Silver_Customer_Exp_Lakehouse": [
    #     customer_cfg,
    #     sales_header_cfg,
    #     sales_line_cfg,
    #     item_budget_cfg
    # ],
    # "Silver_Commons_Lakehouse": [
    #     employee_rfid_mapping_cfg,
    #     machine_center_cfg,
    #     emp_in_cell_cfg
    # ],
    # "Silver_Inventory_Lakehouse": [
    #     # purchase_order_cfg,
    #     purchase_line_cfg,
    #     item_uom_cfg,
    #     output_finish_goods_cfg,
    #     bom_line_cfg,
    # ],
    # "Silver_Finance_Lakehouse": [
    #     credit_memo_header_cfg,
    #     credit_memo_line_cfg,
    #     customer_ledger_entries_cfg,
    #     exchange_rate_cfg,
    #     # gl_entry_cfg,
    #     gl_40_cfg,
    #     gl_103_cfg,
    #     gl_501_cfg,
    #     sales_invoice_header_cfg,
    #     sales_invoice_line_cfg,
    #     shipment_header_cfg,
    #     shipment_line_cfg
    # ],
}

ALL_RESULTS = []

for lakehouse, tables in LAKEHOUSES.items():
    print(f"\n=== RUNNING LOADS FOR {lakehouse} ===")
    for cfg in tables:
        # Update the full target name dynamically
        cfg["target_schema"] = f"{lakehouse}.{cfg['target_schema']}"
        result = run_silver_table(cfg)
        ALL_RESULTS.append(result)

print("\n=== PIPELINE SUMMARY ===")
for r in ALL_RESULTS:
    print(r)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# ==============================================================
# EXECUTE PIPELINE FOR ALL TABLES (DROP + RELOAD)
# ==============================================================

LAKEHOUSES = {
    "Silver_Production_Lakehouse": [
        # plating_prep_jig_cfg,
        # sub_contract_price_list_cfg,
        # sub_contract_deduction_lines_cfg,
        # prod_routing_line_cfg,
        cell_list_cfg,
        firm_plan_log_cfg,
        issue_header_cfg,
        issue_lines_cfg,
        # prod_bom_cfg,
    ],
    "Silver_Product_Dev_Lakehouse": [
        sketch_mapping_cfg,
        npd_worklogs_cfg,
        printing_batch_cfg,
        printing_batch_master_cfg,
        wax_receive_cfg,
        # pd_step_cfg,
        sketch_size_cfg,
        sketch_size_master_cfg,
        engineer_time_cfg,
        sketch_manager_cfg,
        sketch_collection_cfg,
        sketch_customer_approval_cfg,
        # routing_steps_cfg,
        cad_requests_cfg,
    ],
    "Silver_Commons_Lakehouse": [
        employee_rfid_mapping_cfg,
        machine_center_cfg,
        emp_in_cell_cfg
    ],
}

ALL_RESULTS = []

for lakehouse, tables in LAKEHOUSES.items():
    print(f"\n=== RUNNING LOADS FOR {lakehouse} ===")
    for cfg in tables:
        # Build the fully-qualified target schema without mutating the original cfg
        full_target_schema = f"{lakehouse}.{cfg['target_schema']}"
        full_table = f"{full_target_schema}.{cfg['target_table']}"

        # 1. Drop the target table first
        try:
            spark.sql(f"DROP TABLE IF EXISTS {full_table}")
            print(f"  🗑️  Dropped: {full_table}")
        except Exception as e:
            print(f"  ⚠️  Drop failed for {full_table}: {e}")

        # 2. Run the load with the qualified schema
        cfg_run = {**cfg, "target_schema": full_target_schema}
        result = run_silver_table(cfg_run)
        ALL_RESULTS.append(result)

print("\n=== PIPELINE SUMMARY ===")
for r in ALL_RESULTS:
    print(r)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Databricks PySpark job
from pyspark.sql import functions as F, types as T

# # 0) Select catalog & schema
# spark.sql("USE CATALOG Silver_Commons_Lakehouse")
# spark.sql("USE SCHEMA cmn")

# 1) Ensure break_windows table exists with sensible defaults
bw_table = "Silver_Commons_Lakehouse.cmn.break_windows"

if not spark.catalog.tableExists("Silver_Commons_Lakehouse.cmn", "break_windows"):
    bw_defaults = spark.createDataFrame(
        [
            ("PRODUCTION", None, None, "12:15 PM", "1:15 PM"),
            ("OFFICE",     None, None, "11:45 AM", "12:45 PM"),
        ],
        schema=["DeptKey","LineNo","CellNo","BreakStart","BreakEnd"]
    )
    (bw_defaults
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(bw_table))
else:
    # make sure the two defaults exist (idempotent insert via DataFrame ops)
    bw = spark.table(bw_table)
    need_prod = bw.filter(
        (F.col("DeptKey")=="PRODUCTION") & F.col("LineNo").isNull() & F.col("CellNo").isNull()
    ).limit(1).count()==0
    need_offc = bw.filter(
        (F.col("DeptKey")=="OFFICE") & F.col("LineNo").isNull() & F.col("CellNo").isNull()
    ).limit(1).count()==0

    rows = []
    if need_prod:
        rows.append(("PRODUCTION", None, None, "12:15 PM", "1:15 PM"))
    if need_offc:
        rows.append(("OFFICE", None, None, "11:45 AM", "12:45 PM"))

    if rows:
        spark.createDataFrame(rows, ["DeptKey","LineNo","CellNo","BreakStart","BreakEnd"])\
             .write.mode("append").saveAsTable(bw_table)

# 2) Load source attendance
src_tbl = "Silver_Commons_Lakehouse.cmn.silver_employee_time"
df_src = spark.table(src_tbl)

# 3) Parse division/line/cell keys
df_keys = (
    df_src
    .withColumn("DivisionKey", F.upper(F.trim(F.col("Division_Eng"))))
    .withColumn("ParsedLineNo", F.regexp_extract(F.col("Department_Eng"), r"(\d+)", 1).cast("int"))
    .withColumn("ParsedCellNo", F.regexp_extract(F.col("sub_department_Eng"), r"(\d+)", 1).cast("int"))
)

# 4) Load break windows and split by specificity for prioritized joins
bw_full = spark.table(bw_table)
bw_exact = bw_full.filter(F.col("LineNo").isNotNull() & F.col("CellNo").isNotNull()) \
                  .selectExpr("DeptKey as bw1_DeptKey","LineNo as bw1_LineNo","CellNo as bw1_CellNo",
                              "BreakStart as bw1_BreakStart","BreakEnd as bw1_BreakEnd")
bw_line  = bw_full.filter(F.col("LineNo").isNotNull() & F.col("CellNo").isNull()) \
                  .selectExpr("DeptKey as bw2_DeptKey","LineNo as bw2_LineNo",
                              "BreakStart as bw2_BreakStart","BreakEnd as bw2_BreakEnd")
bw_div   = bw_full.filter(F.col("LineNo").isNull() & F.col("CellNo").isNull()) \
                  .selectExpr("DeptKey as bw3_DeptKey",
                              "BreakStart as bw3_BreakStart","BreakEnd as bw3_BreakEnd")

# 5) Join in priority order: (Div+Line+Cell) -> (Div+Line) -> (Div)
d = df_keys.alias("s") \
    .join(F.broadcast(bw_exact), (F.col("s.DivisionKey")==F.col("bw1_DeptKey")) &
                                 (F.col("s.ParsedLineNo")==F.col("bw1_LineNo")) &
                                 (F.col("s.ParsedCellNo")==F.col("bw1_CellNo")), "left") \
    .join(F.broadcast(bw_line),  (F.col("s.DivisionKey")==F.col("bw2_DeptKey")) &
                                 (F.col("s.ParsedLineNo")==F.col("bw2_LineNo")), "left") \
    .join(F.broadcast(bw_div),   (F.col("s.DivisionKey")==F.col("bw3_DeptKey")), "left")

# 6) Resolve final break start/end (fallback to division defaults if no match)
ResolvedBreakStart = F.coalesce(
    F.col("bw1_BreakStart"),
    F.col("bw2_BreakStart"),
    F.col("bw3_BreakStart"),
    F.when(F.col("s.DivisionKey")=="OFFICE", F.lit("11:45 AM")).otherwise(F.lit("12:15 PM"))
)
ResolvedBreakEnd = F.coalesce(
    F.col("bw1_BreakEnd"),
    F.col("bw2_BreakEnd"),
    F.col("bw3_BreakEnd"),
    F.when(F.col("s.DivisionKey")=="OFFICE", F.lit("12:45 PM")).otherwise(F.lit("1:15 PM"))
)

d = d.withColumn("ResolvedBreakStart", ResolvedBreakStart)\
     .withColumn("ResolvedBreakEnd",   ResolvedBreakEnd)

# 7) Convert times to "minutes since midnight"
def to_minutes_from_ap(col_str):
    # Parse 'h:mm AM/PM' to minutes since midnight using a dummy date
    ts = F.to_timestamp(F.concat(F.lit("2000-01-01 "), col_str), "yyyy-MM-dd h:mm a")
    return (F.hour(ts)*60 + F.minute(ts))

brk_start_min = to_minutes_from_ap(F.col("ResolvedBreakStart"))
brk_end_min   = to_minutes_from_ap(F.col("ResolvedBreakEnd"))

d = (
    d.withColumn("min_in",  F.hour("actual_date_time_in")*60  + F.minute("actual_date_time_in"))
     .withColumn("min_out", F.hour("actual_date_time_out")*60 + F.minute("actual_date_time_out"))
     .withColumn("brk_start_min", brk_start_min)
     .withColumn("brk_end_min",   brk_end_min)
)

# 8) Break flags (inclusive). For open end, change <= brk_end_min to < brk_end_min
d = (
    d.withColumn("IsBreak_In",  (F.col("min_in").between(F.col("brk_start_min"), F.col("brk_end_min"))))
     .withColumn("IsBreak_Out", (F.col("min_out").between(F.col("brk_start_min"), F.col("brk_end_min"))))
)

# 9) Weekend / working-hours flags for IN and OUT (AM/PM rules)
# dayofweek(): 1 = Sunday, 7 = Saturday
dow_in  = F.dayofweek("actual_date_time_in")
dow_out = F.dayofweek("actual_date_time_out")

min_in  = F.col("min_in")
min_out = F.col("min_out")

is_weekend_in  = dow_in.isin(1,7)
is_weekend_out = dow_out.isin(1,7)

is_work_in = F.when(dow_in==1, F.lit(False)) \
    .when(dow_in==7, min_in.between(8*60, 17*60)) \
    .otherwise(min_in.between(8*60, 18*60 + 20))

is_work_out = F.when(dow_out==1, F.lit(False)) \
    .when(dow_out==7, min_out.between(8*60, 17*60)) \
    .otherwise(min_out.between(8*60, 18*60 + 20))

d = (d
     .withColumn("IsWeekend_In",  is_weekend_in)
     .withColumn("IsWeekend_Out", is_weekend_out)
     .withColumn("IsWorkingHours_In",  is_work_in)
     .withColumn("IsWorkingHours_Out", is_work_out)
     .withColumn("WeekdayName_In",  F.date_format("actual_date_time_in",  "EEEE"))
     .withColumn("WeekdayName_Out", F.date_format("actual_date_time_out", "EEEE"))
     .withColumn("LocalTime12h_In",  F.date_format("actual_date_time_in",  "MMM d yyyy h:mm:ssa"))
     .withColumn("LocalTime12h_Out", F.date_format("actual_date_time_out", "MMM d yyyy h:mm:ssa"))
)

# 10) Select final columns (original + flags)
final_cols = [
    "Company_Name_Eng","Division_Eng","Department_Eng","Position_Eng","sub_department_Eng","level_employee_Eng",
    "Seat_No","OT_Hour_2","OT_Hour_3","OT_Hour_4","OT_Hour_5","OT_Hour_6","Day_Remark",
    "Total_Leave_Days","Total_Leave_Hours","Break_Start","Break_End","Overtime_Type","OT_Hour_1",
    "actual_date_time_in","actual_date_time_out","late_time_in_minutes","before_time_out_minutes",
    "Count_Day_Include","Count_Day_Absent","Identity_ID","Shift_Name","ShiftCode","Standard_Time_In",
    "Standard_Time_Out","Work_Day","Approval_Level_Thai","Employee_Code","First_Name_Thai",
    "First_Name_Eng","Last_Name_Thai","Last_Name_Eng",
    # parsed keys & resolved breaks
    "DivisionKey","ParsedLineNo","ParsedCellNo","ResolvedBreakStart","ResolvedBreakEnd",
    # break flags
    "IsBreak_In","IsBreak_Out",
    # weekend/work hours flags
    "IsWeekend_In","IsWorkingHours_In","WeekdayName_In","LocalTime12h_In",
    "IsWeekend_Out","IsWorkingHours_Out","WeekdayName_Out","LocalTime12h_Out"
]

df_out = d.select(*final_cols)

# 11) Write results to a managed Delta table in the same schema
out_tbl = "Silver_Commons_Lakehouse.cmn.employee_time_flags"
(df_out
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(out_tbl))

# 12) (Optional) quick peek
display(df_out.limit(100))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Silver_Inventory_Lakehouse.inv.silver_metal_ratio_master AS
# MAGIC SELECT 
# MAGIC     Id as id,           
# MAGIC     SinkCreatedOn as sink_created_on,
# MAGIC     SinkModifiedOn as sink_modified_on,  
# MAGIC     createdon as created_on,    
# MAGIC     modifiedon as modified_on,      
# MAGIC     dts_metalname as metal_name,
# MAGIC     dts_waxtometalratio as wax_to_metal_ration,
# MAGIC     dts_addtobaseweight as add_to_base_weight,
# MAGIC     dts_tolerance as tolerance
# MAGIC FROM Dataverse_link.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.dts_jcwaxtometalratiomaster

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
