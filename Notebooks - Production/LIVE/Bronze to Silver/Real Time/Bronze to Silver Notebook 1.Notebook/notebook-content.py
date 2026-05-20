# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3a130b81-98ec-4fd4-a404-95edc1f0ef1e",
# META       "default_lakehouse_name": "Silver_Inventory_Lakehouse",
# META       "default_lakehouse_workspace_id": "d74457b3-045c-445d-82c6-9a2e4b9f1436",
# META       "known_lakehouses": [
# META         {
# META           "id": "ad99fdfa-85b1-4480-9f7f-2640bfd65f24"
# META         },
# META         {
# META           "id": "ff4d6787-a716-43b6-baaf-972b7426ffa5"
# META         },
# META         {
# META           "id": "8f995de4-1748-4cb3-9ede-81e72298306c"
# META         },
# META         {
# META           "id": "3a130b81-98ec-4fd4-a404-95edc1f0ef1e"
# META         },
# META         {
# META           "id": "3ea0efcd-03d5-44f1-8e70-99f52a5c2a22"
# META         },
# META         {
# META           "id": "e248ea90-8431-4df2-9f29-87866bf9dd5a"
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

# # Helper Functions

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

# ==============================================================
# FABRIC SILVER LAKEHOUSE PIPELINE — HYBRID OPTIMIZED VERSION
# Purpose:
# - Keeps current workflow pattern (watermark from target Silver table)
# - Disables maintenance by default
# - Removes expensive duplicate diagnostics from production path
# - Makes dedupe behavior consistent with cfg["dedupe"]
# - Uses isEmpty() instead of limit(1).count()
# - Consolidates metrics into a single aggregate
# ==============================================================

from typing import Dict, List, Optional, Callable
from pyspark.sql import SparkSession, DataFrame, functions as F, types as T
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
import uuid

# spark = SparkSession.builder.getOrCreate()  # uncomment if needed


# -------------------- BASIC UTILS --------------------
def table_exists(full_table: str) -> bool:
    return spark.catalog.tableExists(full_table)


def get_last_modified(full_table: str, modified_col: str):
    """
    Read last processed timestamp from Silver (string column parsed on the fly).
    Returns None if table does not exist.
    """
    if not table_exists(full_table):
        return None

    row = (
        spark.table(full_table)
        .select(F.max(F.to_timestamp(F.col(modified_col))).alias("mx"))
        .collect()[0]
    )
    return row["mx"]


def _ts(col):
    return F.to_timestamp(col)


def _yn(col):
    return (
        F.when(F.col(col).isin(1, "1", True, "TRUE", "Y", "y"), F.lit("Yes"))
        .when(F.col(col).isin(0, "0", False, "FALSE", "N", "n"), F.lit("No"))
        .otherwise(None)
    )


# -------------------- OPS TIMESTAMP (WATERMARK) --------------------
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
    """
    Read Bronze Delta and create an ops-only TIMESTAMP column `_coalesced_mod_ts`
    without touching original columns.
    """
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
    """Filter using ops-only timestamp column."""
    if last_mod is None:
        return df

    if "_coalesced_mod_ts" not in df.columns:
        raise ValueError("`_coalesced_mod_ts` missing; ensure read_source_delta builds it.")

    threshold = (
        F.lit(last_mod).cast("timestamp")
        - F.expr(f"INTERVAL {lookback_minutes} MINUTES")
    )
    return df.filter(F.col("_coalesced_mod_ts") >= threshold)


# -------------------- SELECT / TYPES / SCHEMA --------------------
def rename_select(
    df: DataFrame,
    col_map: Dict[str, str],
    passthrough_cols: Optional[List[str]] = None,
) -> DataFrame:
    """
    Select and rename based on mapping; add missing mapped columns as NULL.
    Also keeps any passthrough_cols (e.g. _coalesced_mod_ts).
    """
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
    """
    Cast columns and add metadata.
    Does NOT touch modified_on unless listed in ts_cols.
    """
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
    """
    Ensure expected columns exist (adds missing as NULLs).
    Keeps extra columns when keep_extra=True.
    """
    passthrough_cols = passthrough_cols or []
    cols_lower = {c.lower(): c for c in df.columns}

    dec_lower = {k.lower(): v for k, v in decimal_cols.items()}
    ts_lower = [x.lower() for x in ts_cols]
    dbl_lower = [x.lower() for x in double_cols]

    def _dtype_str(c):
        c_l = c.lower()
        if c_l in dec_lower:
            return dec_lower[c_l]
        if c_l in ts_lower or c_l == "load_ts":
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
        extra_cols = [
            c for c in current_cols if c not in target_cols and c not in passthrough_cols
        ]
        return out.select(
            *target_cols,
            *[c for c in passthrough_cols if c in current_cols],
            *extra_cols,
        )

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
    df: DataFrame,
    key_cols: List[str],
    order_col: str = "_coalesced_mod_ts",
) -> DataFrame:
    if order_col not in df.columns:
        raise ValueError(f"order_col '{order_col}' not found")

    w = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc_nulls_last())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# -------------------- DELTA OPS --------------------
def create_managed_table(
    target_schema: str,
    full_target: str,
    ddl_cols: List[str],
    ddl_types: Dict[str, str],
    partition_cols: Optional[List[str]] = None,
):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")

    ddl = ",\n".join([f"`{c}` {ddl_types.get(c, 'STRING')}" for c in ddl_cols])
    part = (
        f" PARTITIONED BY ({', '.join([f'`{p}`' for p in (partition_cols or [])])})"
        if partition_cols
        else ""
    )

    spark.sql(f"CREATE TABLE IF NOT EXISTS {full_target} ({ddl}) USING DELTA{part}")


def merge_upsert(
    staging_view: str,
    full_target: str,
    key_cols: List[str],
    target_cols: List[str],
):
    """
    SCD1 MERGE (null-safe) — does NOT update key columns.
    """
    on_expr = " AND ".join([f"(s.{k} <=> t.{k})" for k in key_cols])
    non_key_cols = [c for c in target_cols if c not in set(key_cols)]
    set_expr = ", ".join([f"t.{c}=s.{c}" for c in non_key_cols])

    spark.sql(
        f"""
        MERGE INTO {full_target} t
        USING {staging_view} s
        ON {on_expr}
        WHEN MATCHED THEN UPDATE SET {set_expr}
        WHEN NOT MATCHED THEN INSERT ({", ".join(target_cols)})
        VALUES ({", ".join([f"s.{c}" for c in target_cols])})
        """
    )


def maintain(
    full_target: str,
    zorder_cols: Optional[List[str]] = None,
    vacuum_hours: int = 168,
):
    z_clause = ""
    if zorder_cols:
        tgt_cols = set(spark.table(full_target).columns)
        z_keep = [c for c in zorder_cols if c in tgt_cols]
        if z_keep:
            z_clause = f" ZORDER BY ({', '.join([f'`{c}`' for c in z_keep])})"

    spark.sql(f"OPTIMIZE {full_target}{z_clause}")
    spark.sql(f"VACUUM {full_target} RETAIN {vacuum_hours} HOURS")


# -------------------- OPTIONAL ENUM MAPPER --------------------
def apply_enum_map(
    df: DataFrame,
    col_name: str,
    mapping: dict,
    default_passthrough: bool = True,
) -> DataFrame:
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
    OPEN_MAP = {0: "No", 1: "Yes"}
    TYPE_NAME_MAP = {
        184930000: "In location in",
        184930001: "Out location",
        184930002: "To employee",
        184930003: "From employee",
    }
    PROD_ORDER_STATUS_MAP = {
        184930000: "Simulated",
        184930001: "Planned",
        184930002: "Firm Planned",
        184930003: "Released",
        184930004: "Finished",
    }
    OPERATION_TYPE_MAP = {
        184930000: "Work Center",
        184930001: "Machine Center",
    }
    ROUTING_STATUS_MAP = {
        184930000: "Planned",
        184930001: "In Progress",
        184930002: "Finished",
    }
    CASTING_OUTPUT_STATUS_MAP = {
        780350000: "Modify Qty And Add Scrap Item No",
        780350001: "Consume Components",
        780350002: "Output Master Alloy And Scrap",
        780350003: "Consume Master Alloy",
        780350004: "Output Casting Parts",
        780350005: "Complete",
    }
    RESERVE_MAP = {
        184930000: "Never",
        184930001: "Optional",
        184930002: "Always",
    }
    REPLENISHMENT_SYSTEM_MAP = {
        184930000: "Purchase",
        184930001: "Prod Order",
        184930002: "Transfer",
        184930003: "Assembly",
    }
    REPAIR_POSTING_STATUS_MAP = {
        780350000: "Create Repair PRO",
        780350001: "Main PRO - Post Negative Consumption",
        780350002: "Repair PRO - Post Consumption",
        780350003: "Complete",
    }
    ITEM_TYPE_MAP = {
        184930000: "Inventory",
        184930001: "Service",
        184930002: "Non-Inventory",
    }
    ITEM_ORDER_TRACKING_MAP = {
        184930000: "None",
        184930001: "Tracking Only",
        184930002: "Tracking & Action Msg.",
    }
    ITEM_MANUFACTURING_POLICY_MAP = {
        184930000: "Make-to-Stock",
        184930001: "Make-to-Order",
    }
    ITEM_REORDERING_POLICY_MAP = {
        184930000: "Fixed Reorder Qty.",
        184930001: "Maximum Qty",
        184930002: "Order",
        184930003: "Lot-for-Lot",
    }

    out = df
    out = apply_enum_map(out, "open", OPEN_MAP)
    out = apply_enum_map(out, "type_name", TYPE_NAME_MAP)
    out = apply_enum_map(out, "prod_order_status", PROD_ORDER_STATUS_MAP)
    out = apply_enum_map(out, "operation_type", OPERATION_TYPE_MAP)
    out = apply_enum_map(out, "routing_status", ROUTING_STATUS_MAP)
    out = apply_enum_map(out, "casting_output_status", CASTING_OUTPUT_STATUS_MAP)
    out = apply_enum_map(out, "item_reordering_policy", ITEM_REORDERING_POLICY_MAP)
    out = apply_enum_map(out, "item_reserve", RESERVE_MAP)
    out = apply_enum_map(out, "item_order_tracking", ITEM_ORDER_TRACKING_MAP)
    out = apply_enum_map(out, "item_replenishment", REPLENISHMENT_SYSTEM_MAP)
    out = apply_enum_map(out, "item_manufacturing_policy", ITEM_MANUFACTURING_POLICY_MAP)
    out = apply_enum_map(out, "item_block", OPEN_MAP)
    out = apply_enum_map(out, "item_type", ITEM_TYPE_MAP)
    out = apply_enum_map(out, "repair_posting_status", REPAIR_POSTING_STATUS_MAP)
    return out


# -------------------- DATETIME SANITIZER --------------------
def fix_ancient_datetimes(
    df,
    ts_cols=None,
    date_cols=None,
    policy="null",                   # "null" or "floor"
    floor_ts="1900-01-01 00:00:00",
    floor_date="1900-01-01",
):
    """
    Sanitize ancient/sentinel datetimes that break Spark 3's Proleptic Gregorian handling.
    """
    ts_cols = ts_cols or []
    date_cols = date_cols or []

    bad_ts_literals = [
        "0001-01-01 00:00:00",
        "0001-01-01T00:00:00",
        "1753-01-01 00:00:00",
        "1753-01-01T00:00:00",
        "1899-12-30 00:00:00",
        "1899-12-30T00:00:00",
    ]
    bad_date_literals = [
        "0001-01-01",
        "1753-01-01",
        "1899-12-30",
    ]

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
                out = out.withColumn(
                    c,
                    F.when(bad, F.lit(None).cast(T.TimestampType())).otherwise(col_ts),
                )
            else:
                out = out.withColumn(
                    c,
                    F.when(bad, F.to_timestamp(F.lit(floor_ts))).otherwise(col_ts),
                )

    for c in date_cols:
        if c in out.columns:
            col_date = F.to_date(F.col(c))
            is_bad_literal = F.lower(F.col(c).cast("string")).isin(
                [s.lower() for s in bad_date_literals]
            )
            is_too_old = col_date < F.to_date(F.lit(floor_date))
            bad = is_bad_literal | is_too_old

            if policy == "null":
                out = out.withColumn(
                    c,
                    F.when(bad, F.lit(None).cast(T.DateType())).otherwise(col_date),
                )
            else:
                out = out.withColumn(
                    c,
                    F.when(bad, F.to_date(F.lit(floor_date))).otherwise(col_date),
                )

    return out


# -------------------- CORE: RUN ONE TABLE --------------------
def run_silver_table(cfg: Dict) -> Dict:
    """
    Read → Watermark → Rename/Type → Custom → Ensure/Align → Optional Dedupe
    → Seed/Merge → Optional Maintain

    Notes:
    - `modified_on` stays STRING in Silver unless you explicitly cast it in ts_cols
    - `_coalesced_mod_ts` is used for watermark filtering and metrics
    - dedupe is controlled ONLY by cfg["dedupe"]
    """
    print(cfg)

    # ---- unpack config ----
    source_path = cfg["source_path"]
    target_schema = cfg["target_schema"]
    target_table = cfg["target_table"]
    full_target = f"{target_schema}.{target_table}"
    key_cols = cfg.get("natural_key") or cfg.get("business_key")
    col_map = cfg["col_map"]

    sink_modified_on = cfg.get("SinkModifiedOn", "modifiedon")
    created_src_col = cfg.get("created_src_col", "createdon")
    modified_target_col = cfg.get("modified_target_col", "modified_on")
    lookback_minutes = cfg.get("lookback_minutes", 120)
    full_load = cfg.get("full_load", False)
    full_repartitions = cfg.get("full_load_repartitions", None)
    ts_cols = cfg.get("ts_cols", [])
    double_cols = cfg.get("double_cols", [])
    decimal_cols = cfg.get("decimal_cols", {})
    target_cols = cfg.get(
        "target_cols",
        list(col_map.values()) + ["load_ts", "source_system"],
    )
    ddl_overrides_cfg = cfg.get("ddl_overrides", {})
    ddl_overrides = {**ddl_overrides_cfg, modified_target_col: "STRING"}
    custom_transform = cfg.get("custom_transform")
    source_label = cfg.get("load_source_label", "Dataverse")

    # Maintenance OFF by default
    maintenance = cfg.get(
        "maintenance",
        {"optimize": False, "vacuum_hours": 168, "zorder_cols": ["_coalesced_mod_ts"]},
    )

    do_dedupe = cfg.get("dedupe", True)

    # ---- validate key columns ----
    if not key_cols:
        raise ValueError("cfg must include 'natural_key' or 'business_key'.")

    missing_key_after_rename = [k for k in key_cols if k not in target_cols]
    if missing_key_after_rename:
        raise ValueError(
            f"Key columns not present in target_cols after rename: {missing_key_after_rename}. "
            f"Make sure they’re mapped in col_map and included in target_cols."
        )

    # ---- 1) read + watermark ----
    base = read_source_delta(source_path, sink_modified_on, created_src_col)
    last_mod = None if full_load else get_last_modified(full_target, modified_target_col)
    base = apply_watermark(base, last_mod, lookback_minutes)

    # ---- 2) rename while preserving ops col ----
    df = rename_select(base, col_map, passthrough_cols=["_coalesced_mod_ts"])

    # ---- 2.1) type coercion + metadata ----
    df = coerce_types_and_enrich(df, ts_cols, double_cols, decimal_cols, source_label)

    # ---- 2.2) sanitize bad ancient datetimes ----
    df = fix_ancient_datetimes(
        df,
        ts_cols=ts_cols,
        date_cols=[],
        policy="null",
        floor_ts="1900-01-01 00:00:00",
        floor_date="1900-01-01",
    )

    # ---- 2.3) custom transform ----
    if callable(custom_transform):
        df = custom_transform(df)

    # ---- 3) ensure schema alignment ----
    df = ensure_columns(
        df,
        target_cols,
        ts_cols,
        double_cols,
        decimal_cols,
        keep_extra=True,
        passthrough_cols=["_coalesced_mod_ts"],
    )

    # ---- 3.1) build DDL types ----
    ddl_types = {c: "STRING" for c in target_cols}
    for c in ts_cols + ["load_ts"]:
        ddl_types[c] = "TIMESTAMP"
    for c in double_cols:
        ddl_types[c] = "DOUBLE"
    for c, dec in decimal_cols.items():
        ddl_types[c] = dec
    ddl_types.update(ddl_overrides)

    # ---- 3.2) cast to target types ----
    df = cast_to_target_types(df, ddl_types)

    # ---- 3.3) optional repartition before write/merge ----
    if full_load and full_repartitions:
        df = df.repartition(full_repartitions)

    # ---- 4) optional dedupe ----
    if do_dedupe:
        if "_coalesced_mod_ts" not in df.columns:
            raise ValueError("Internal `_coalesced_mod_ts` missing after projection.")
        stg = dedupe_latest(df, key_cols, "_coalesced_mod_ts")
    else:
        stg = df

    # ---- 5) materialize once ----
    stg = stg.persist(StorageLevel.MEMORY_AND_DISK)

    if stg.isEmpty():
        stg.unpersist()
        print(f"[SKIP] {full_target}: no changes found.")
        return {
            "table": full_target,
            "rows": 0,
            "mode": "skip",
            "max_modified_on": None,
            "updated_rows": 0,
            "inserted_rows": 0,
        }

    # ---- 6) create table idempotently ----
    create_managed_table(target_schema, full_target, target_cols, ddl_types)

    # ---- 7) seed or merge ----
    first_load = last_mod is None
    tmp_view = f"stg_tmp_{target_table}_{uuid.uuid4().hex[:8]}"

    merge_df = stg

    if full_load and first_load:
        merge_df.select(*target_cols).write.format("delta").mode("append").saveAsTable(full_target)
        mode = "seed"
        inserted_rows = None
        updated_rows = 0
    else:
        merge_df.select(*target_cols).createOrReplaceTempView(tmp_view)
        merge_upsert(tmp_view, full_target, key_cols, target_cols)
        mode = "merge"
        inserted_rows = None
        updated_rows = None

    print("staging columns:", stg.columns)
    print("target columns:", spark.table(full_target).columns)

    # ---- 8) metrics in a single aggregate ----
    metrics = stg.agg(
        F.count("*").alias("cnt"),
        F.max(F.col("_coalesced_mod_ts")).alias("max_ts"),
    ).collect()[0]

    row_count = metrics["cnt"]
    max_mod = metrics["max_ts"]

    # ---- 9) maintenance ----
    if maintenance.get("optimize", False):
        zcols = maintenance.get("zorder_cols")
        if isinstance(zcols, str):
            zcols = [zcols]
        maintain(
            full_target,
            zorder_cols=zcols,
            vacuum_hours=maintenance.get("vacuum_hours", 168),
        )

    stg.unpersist()

    print(f"[{mode.upper()}] {full_target} rows={row_count} max(_coalesced_mod_ts)={max_mod}")

    return {
        "table": full_target,
        "rows": row_count,
        "mode": mode,
        "max_modified_on": max_mod,
        "updated_rows": updated_rows,
        "inserted_rows": inserted_rows,
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Production

# MARKDOWN ********************

# ## Production Order Status

# CELL ********************

# ==============================================================
# PRODUCTION ORDER STATUS — CLEAN CONFIG (drop-in)
# ==============================================================

prod_status_cfg = {
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/25adf76d-df18-41c8-97e8-55789149bd80/Tables/cr535_dsvcproductionorderstatus",
    "target_schema": "prod",
    "target_table": "silver_prod_order_status",

    # natural key must be in *target* names after mapping
    # "business_key": ["prod_order_no", "prod_order_line_no", "current_location_code", "past_location_code", "machine_center_no"],

    "business_key": ["prod_order_no", "prod_order_line_no", "operation_no", "type_name", "employee_no"],


    # SOURCE -> TARGET mapping
    # We make SinkModifiedOn the displayed modified_on; we do NOT also map modifiedon to avoid collisions.
    "col_map": {
        "createdon":                   "created_on",
        "modifiedon":                  "modified_on",           # display column (STRING, stored as-is)
        "cr535_prodorderno":           "prod_order_no",
        "cr535_prodorderlineno":       "prod_order_line_no",
        "cr535_jobsheetno":            "job_sheet_no",
        "cr535_type":                  "type_name",
        "cr535_prodorderstatus":       "prod_order_status",
        "cr535_open":                  "open",
        "cr535_salesorderno":          "sales_order_no",
        "cr535_locationcode":          "current_location_code",
        "cr535_otherlocationcode":     "past_location_code",
        "cr535_employeeno":            "employee_no",
        "cr535_userid":                "user_id",
        "cr535_dateout":               "date_out",
        "cr535_quantity":              "quantity",
        "cr535_datein":                "date_in",
        "cr535_timein":                "time_in",
        "cr535_remainingquantity":     "remaining_quantity",
        "cr535_operationno":           "operation_no",
        "cr535_itemno":                "item_no",
        "cr535_entrynoautono":         "entry_no_auto_no",
        "cr535_antennaid":             "antenna_id",
        "cr535_rfidcode":              "rfid_code",
        "cr535_rfidtransaction":       "rfid_transaction_name",
        "cr535_qaapproved":            "qa_approved_name",
        "cr535_machinecenterno":       "machine_center_no",
        "SinkModifiedOn":              "SinkModifiedOn"
    },

    # tell the loader which *Bronze* columns to use for watermark creation
    # (expected key is exactly "SinkModifiedOn" per our run_silver_table)
    "SinkModifiedOn": "SinkModifiedOn",
    "created_src_col": "createdon",

    # tell the loader which *Silver* column is the string self-watermark
    "modified_target_col": "modified_on",

    # typing
    "ts_cols": ["created_on", "date_in", "date_out", "modified_on"],
    "decimal_cols": {
        "quantity": "DECIMAL(18,2)",
        "remaining_quantity": "DECIMAL(18,2)"
    },
    # if you want doubles instead, leave decimal_cols empty and add to double_cols instead
    "double_cols": [],

    # enum mapping
    "custom_transform": apply_enum_maps,

    # force any schema bits (and ensure modified_on stays STRING)
    "ddl_overrides": {
        "quantity": "DECIMAL(18,2)",
        "remaining_quantity": "DECIMAL(18,2)",
    },

    # run mode
    "dedupe": True,
    "lookback_minutes": 360,
    "full_load": False,                 # flip to False after first seed
    "full_load_repartitions": 64,

    
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Production Order Header

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- helper: keep latest created_on ---
def keep_latest_created_prod_order_header(df):
    w = Window.partitionBy("prod_order_no").orderBy(F.col("created_on").desc())

    return (
        df
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
    )

# --- final custom transform used by the runner ---
def custom_transform_prod_order_header(df):
    # 1. Apply your enum maps (existing step)
    df = apply_enum_maps(df)

    # 2. Keep only latest created_on per prod_order_no
    df = keep_latest_created_prod_order_header(df)

    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================================
# PRODUCTION ORDER HEADER — CLEAN CONFIG (drop-in)
# ==============================================================

prod_header_cfg = {
    # ---- SOURCE (Bronze delta path) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/25adf76d-df18-41c8-97e8-55789149bd80/Tables/cr535_productionorder",

    # ---- TARGET (Silver managed table) ----
    "target_schema": "prod",
    "target_table":  "silver_prod_order_header",

    # runner expects `business_key`
    "business_key": ["prod_order_no"],

    # ---- COLUMN MAPPING (source -> target) ----
    # map both source modified columns to TEMP names; transform will coalesce into final `modified_on` (STRING)
    "col_map": {
        "createdon":                         "created_on",
        "modifiedon":                        "modified_on",        
        "cr535_noseries":                    "prod_order_type",
        "cr535_no":                          "prod_order_no",
        "cr535_status":                      "prod_order_status",
        "cr535_description":                 "prod_order_description",
        "cr535_sourceno":                    "FG_item_no",
        "cr535_routingno":                   "item_routing_no",
        "cr535_shortcutdimension2code":      "item_material",
        "cr535_locationcode":                "prod_order_location",
        "cr535_quantity":                    "prod_order_quantity",
        "cr535_costamount":                  "prod_order_cost_amount",
        "cr535_startingdatetime":            "prod_order_starting_date_time",
        "cr535_endingdatetime":              "prod_order_ending_date_time",
        "cr535_duedate":                     "prod_order_due_date",
        "cr535_finisheddate":                "prod_order_finished_date",
        "cr535_salesorderno":                "sales_order_no",
        "cr535_salesorderlineno":            "sales_order_line_no",
        "cr535_foritem":                     "ref_item",
        "cr535_forprodorderno":              "ref_prod_order",
        "cr535_remark":                      "remark",
        "SinkModifiedOn":                    "SinkModifiedOn"
    },

    # ---- INCREMENTAL WATERMARK ----
    # these keys match what the runner reads (don’t use "modified_src_col")
    "SinkModifiedOn": "SinkModifiedOn",     # Bronze column name
    "created_src_col": "createdon",

    # final Silver self-watermark column name (STRING)
    "modified_target_col": "modified_on",

    # ---- TYPES (post-transform) ----
    # DO NOT include 'modified_on' here — we keep it STRING
    "ts_cols": [
        "created_on",
        "prod_order_starting_date_time",
        "prod_order_ending_date_time",
        "prod_order_due_date",
        "prod_order_finished_date",
    ],
    "decimal_cols": {
        "prod_order_quantity":    "DECIMAL(18,2)",
        "prod_order_cost_amount": "DECIMAL(18,2)",
    },
    "ddl_overrides": {
        "prod_order_quantity":    "DECIMAL(18,2)",
        "prod_order_cost_amount": "DECIMAL(18,2)",
        "prod_order_status":      "STRING",
        "modified_on":            "STRING",   # belt & suspenders
    },

    # ---- FINAL COLUMN ORDER (must include modified_on) ----
    "target_cols": [
        "created_on","modified_on",
        "prod_order_type","prod_order_no","prod_order_status","prod_order_description",
        "FG_item_no","item_routing_no","item_material","prod_order_location",
        "prod_order_quantity","prod_order_cost_amount",
        "prod_order_starting_date_time","prod_order_ending_date_time",
        "prod_order_due_date","prod_order_finished_date",
        "sales_order_no","sales_order_line_no",
        "ref_item","ref_prod_order", "remark", "SinkModifiedOn",
        "load_ts","source_system"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": custom_transform_prod_order_header,

    # ---- RUNTIME ----
    "dedupe": True,
    "lookback_minutes": 360,
    "full_load": False,                # flip to False after first seed
    "full_load_repartitions": 64,
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Production Order Line

# CELL ********************

# ==============================================================
# PRODUCTION ORDER LINE
# ==============================================================

prod_line_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/25adf76d-df18-41c8-97e8-55789149bd80/Tables/cr535_prodorderline",  

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "prod",
    "target_table":  "silver_prod_order_line",

    # ---- NATURAL KEY ----
    # typically prod_order_no + prod_order_line_no uniquely identifies a line
    "business_key": ["prod_order_no", "prod_order_line_no"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon":                        "created_on",
        "modifiedon":                       "modified_on",     
        "cr535_prodorderno":                "prod_order_no",
        "cr535_status":                     "prod_order_status",
        "cr535_lineno":                     "prod_order_line_no",
        "cr535_itemno":                     "item_no",
        "cr535_duedate":                    "prod_line_due_date",
        "cr535_description":                "item_description",
        "cr535_locationcode":               "item_location",
        "cr535_startingdatetime":           "prod_line_start_date",
        "cr535_endingdatetime":             "prod_line_end_date",
        "cr535_unitofmeasurecode":          "item_uom",
        "cr535_quantity":                   "prod_line_quantity",
        "cr535_finishedquantity":           "prod_line_finished_quantity",
        "cr535_remainingquantity":          "prod_line_remaining_quantity",
        "cr535_salesorderno":               "sales_order_no",
        "cr535_salesorderlineno":           "sales_order_line_no",
        "cr535_shortcutdimension2code":     "item_material",
        "cr535_unitcost":                   "prod_line_unit_cost",
        "cr535_routingno":                  "item_routing_no",
        "cr535_routingreferenceno":         "item_routing_line",
        "cr535_productionbomno":            "bom_no",
        "cr535_productionbomversioncode":   "bom_version",
        "cr535_routingversioncode":         "item_routing_version",
        "cr535_costamount":                 "prod_line_cost_amount",
        "SinkModifiedOn":                   "SinkModifiedOn"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col":   "SinkModifiedOn",
    "created_src_col":    "createdon",
    "modified_target_col":"modified_on",     # final column after coalesce in transform

    # ---- TYPES ----
    "ts_cols": [
        "created_on", "modified_on",
        "prod_line_due_date", "prod_line_start_date", "prod_line_end_date"
    ],
    "decimal_cols": {
        "prod_line_quantity":           T.DecimalType(18,2),
        "prod_line_finished_quantity":  T.DecimalType(18,2),
        "prod_line_remaining_quantity": T.DecimalType(18,2),
        "prod_line_unit_cost":          T.DecimalType(18,2),
        "prod_line_cost_amount":        T.DecimalType(18,2)
    },
    "ddl_overrides": {
        "prod_line_quantity":           "DECIMAL(18,2)",
        "prod_line_finished_quantity":  "DECIMAL(18,2)",
        "prod_line_remaining_quantity": "DECIMAL(18,2)",
        "prod_line_unit_cost":          "DECIMAL(18,2)",
        "prod_line_cost_amount":        "DECIMAL(18,2)",
        "prod_order_status":            "STRING"
    },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on","modified_on",
        "prod_order_no","prod_order_status","prod_order_line_no",
        "item_no","item_description","item_material","item_location","item_uom",
        "prod_line_quantity","prod_line_finished_quantity","prod_line_remaining_quantity",
        "prod_line_unit_cost","prod_line_cost_amount",
        "prod_line_due_date","prod_line_start_date","prod_line_end_date",
        "sales_order_no","sales_order_line_no",
        "item_routing_no","item_routing_line","item_routing_version",
        "bom_no","bom_version", "SinkModifiedOn",
        "load_ts","source_system"
    ],

    # ---- CUSTOM TRANSFORM (optional: can reuse prod_order_header’s style) ----
    "custom_transform": apply_enum_maps,  # or define your own if needed

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 360,
    "full_load": False,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Prod Order Repair

# CELL ********************

# ==============================================================
# Production Tracking • Repair Summary (grid columns only)
# ==============================================================

prod_tracking_repair_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/dts_productiontrackingrepairprolog",
    
    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "prod",
    "target_table":  "silver_prod_order_repair",

    # ---- NATURAL KEY ----
    "business_key": ["prod_order_no", "prod_order_line_no", "repair_prod_order_no"],

    # ---- COLUMN MAP (source -> target) ----
    # Only the columns visible in the screenshot
    "col_map": {
        "dts_mainproductionorderno":      "prod_order_no",
        "dts_mainproductionorderlineno":  "prod_order_line_no",
        "dts_repairproductionorderno":    "repair_prod_order_no",
        "dts_repairquantity":             "repair_quantity",

        "dts_postingstatus":            "repair_posting_status",
        "cr535_defecttype":             "defect_type",

        "createdon":                     "created_on",
        "createdbyname":                 "created_by",
        "modifiedon":                    "modified_on",
        "cr535_workcenterno":            "work_center_no"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "modifiedon",   # if not present, change to createdon
    "created_src_col": "createdon",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": ["created_on", "modified_on"],

    "decimal_cols": {
        "repair_quantity":                "DECIMAL(18,2)",
    },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on",
        "modified_on",
        "created_by",
        "prod_order_no",
        "prod_order_line_no",
        "repair_prod_order_no",
        "repair_quantity",
        "repair_posting_status",
        "defect_type",
        "work_center_no"
        
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": False,
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Casting Parts

# CELL ********************

# ==============================================================
# CASTING PARTS
# ==============================================================

casting_part_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/dts_jccastedpartsprod",  

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "prod",
    "target_table":  "silver_casting_parts",

    # ---- NATURAL KEY ----
    # typically prod_order_no + prod_order_line_no uniquely identifies a line
    "business_key": ["prod_order_no", "prod_order_line_no", "casting_prod_order"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "modifiedon": "modified_on",
        "dts_productionorderno": "prod_order_no",
        "dts_prodorderlineno": "prod_order_line_no",
        "dts_castingtreename": "casting_prod_order",
        "dts_itemno": "item_no",
        "dts_quantityassignedtotree": "casting_qty_to_tree",
        "dts_stoneweight": "casting_stone_weight",
        "dts_passedquantity": "casting_qty_passed",
        "dts_passedweight": "casting_qty_passed_weight",
        "dts_rejectquantity": "casting_qty_reject",
        "dts_rejectweight": "casting_qty_reject_weight",
        "dts_warehousetransfercomplete": "casting_to_warehouse",
        "dts_warehousetransferlotno": "casting_warehouse_lot",
        "cr535_batchno": "casting_batch_no",
        "SinkModifiedOn": "SinkModifiedOn"
    },

    # ---- WATERMARK SETTINGS ----
    "SinkModifiedOn":    "SinkModifiedOn",   # reader will build _coalesced_mod_ts from this
    "created_src_col":   "createdon",
    "modified_target_col":"modified_on",     # display column (STRING); don’t parse it


    # ---- TYPES ----
    "ts_cols": [
        "created_on", "modified_on", "SinkModifiedOn"
    ],
    "decimal_cols": {
        "casting_qty_to_tree":        "DECIMAL(18,2)",
        "casting_stone_weight":       "DECIMAL(18,2)",
        "casting_qty_passed":         "DECIMAL(18,2)",
        "casting_qty_passed_weight":  "DECIMAL(18,2)",
        "casting_qty_reject":         "DECIMAL(18,2)",
        "casting_qty_reject_weight":  "DECIMAL(18,2)",
    },
    "ddl_overrides": {
       "casting_to_warehouse": "STRING",
    },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on", "modified_on",
        "prod_order_no", "prod_order_line_no",
        "casting_prod_order",
        "item_no",
        "casting_qty_to_tree",
        "casting_stone_weight",
        "casting_qty_passed", "casting_qty_passed_weight",
        "casting_qty_reject", "casting_qty_reject_weight",
        "casting_to_warehouse",
        "casting_warehouse_lot",
        "casting_batch_no",
        "SinkModifiedOn",
        "load_ts", "source_system",
    ],


    # ---- CUSTOM TRANSFORM (optional: can reuse prod_order_header’s style) ----
    "custom_transform": lambda df: df.withColumn(
        "casting_to_warehouse",
        F.when(F.col("casting_to_warehouse") == 1, F.lit("Yes"))
        .when(F.col("casting_to_warehouse") == 0, F.lit("No"))
        .otherwise(F.col("casting_to_warehouse").cast("string"))
    ), 

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 360,
    "full_load": False,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Casting Tree

# CELL ********************

# ==============================================================
# CASTING TREE
# ==============================================================

casting_tree_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/dts_jccastingtreeprod",  

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "prod",
    "target_table":  "silver_casting_tree",

    # ---- NATURAL KEY ----
    # typically prod_order_no + prod_order_line_no uniquely identifies a line
    "business_key": ["casting_prod_order", "casting_tree_no"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "createdon": "created_on",
        "modifiedon": "modified_on",
        "dts_castingtreeno": "casting_prod_order",
        "dts_treeno": "casting_tree_no",
        "cr535_castingstatusname": "casting_status",
        "dts_masteralloy": "master_alloy",
        "cr535_outputmasteralloylotno": "output_master_alloy_lot",
        "dts_outputbaseitemnoname": "output_item",
        "dts_outputdustitemnoname": "output_item_dust",
        "dts_outputsprueitemnoname": "output_item_spure",
        "dts_outputbaseweight": "output_item_weight",
        "dts_outputactualweight": "output_item_actual",
        "dts_outputdustweight": "output_item_dust_weight",
        "dts_outputsprueweight": "output_item_spure_weight",
        "dts_castingscrapitemno": "scrap_item",
        "dts_outputscraplotno": "scrap_item_output_lot",
        "dts_warehousetransferbasecomplete": "casting_to_warehouse",
        "dts_warehousetransferbaselotno": "casting_warehouse_lot",
        "dts_warehousetransferspruecomplete": "warehouse_to_spure",
        "dts_warehousetransferspruelotno": "warehouse_to_spure_lot",
        "dts_castingoutputstatus": "casting_output_status",
        "dts_stoneweight": "casting_stone_weight",
        "dts_baseweight": "casting_base_weight",
        "dts_totaltreeweight": "casting_total_tree_weight",
        "dts_netwaxweight": "casting_net_wax_weight",
        "dts_batchnotext": "casting_batch_no",
        "SinkModifiedOn": "SinkModifiedOn"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col":   "SinkModifiedOn",
    "created_src_col":    "createdon",
    "modified_target_col":"modified_on",     # final column after coalesce in transform

        # ---- TYPES ----
    "ts_cols": [
        "created_on", "modified_on", "SinkModifiedOn"
    ],
    "decimal_cols": {
        "output_item_weight":        "DECIMAL(18,2)",
        "output_item_actual":        "DECIMAL(18,2)",
        "output_item_dust_weight":   "DECIMAL(18,2)",
        "output_item_spure_weight":  "DECIMAL(18,2)",
        "casting_stone_weight":      "DECIMAL(18,2)",
        "casting_base_weight":       "DECIMAL(18,2)",
        "casting_total_tree_weight": "DECIMAL(18,2)",
        "casting_net_wax_weight":    "DECIMAL(18,2)",
    },
    "ddl_overrides": {
        "casting_output_status": "STRING",
        "casting_status": "STRING",
        "casting_to_warehouse": "STRING",
        "warehouse_to_spure": "STRING",
    },


    # ---- FINAL COLUMN ORDER ----a
    "target_cols": [
        "created_on", "modified_on",
        "casting_prod_order", "casting_tree_no", "casting_status",
        "master_alloy", "output_master_alloy_lot",
        "output_item", "output_item_dust", "output_item_spure",
        "output_item_weight", "output_item_actual",
        "output_item_dust_weight", "output_item_spure_weight",
        "scrap_item", "scrap_item_output_lot",
        "casting_to_warehouse", "casting_warehouse_lot",
        "warehouse_to_spure", "warehouse_to_spure_lot",
        "casting_output_status",
        "casting_stone_weight", "casting_base_weight",
        "casting_total_tree_weight", "casting_net_wax_weight",
        "casting_batch_no", "SinkModifiedOn"
        "load_ts", "source_system",
    ],


    # ---- CUSTOM TRANSFORM (optional: can reuse prod_order_header’s style) ----
    "custom_transform":  lambda df: (
    df.withColumn(
        "casting_to_warehouse",
        F.when(F.col("casting_to_warehouse") == 1, "Yes")
         .when(F.col("casting_to_warehouse") == 0, "No")
         .otherwise(F.col("casting_to_warehouse").cast("string"))
    )
    .withColumn(
        "warehouse_to_spure",
        F.when(F.col("warehouse_to_spure") == 1, "Yes")
         .when(F.col("warehouse_to_spure") == 0, "No")
         .otherwise(F.col("warehouse_to_spure").cast("string"))
    )
    .withColumn(
        "casting_output_status",
        F.when(F.col("casting_output_status") == 780350000, "Modify Qty And Add Scrap Item No")
         .when(F.col("casting_output_status") == 780350001, "Consume Components")
         .when(F.col("casting_output_status") == 780350002, "Output Master Alloy And Scrap")
         .when(F.col("casting_output_status") == 780350003, "Consume Master Alloy")
         .when(F.col("casting_output_status") == 780350004, "Output Casting Parts")
         .when(F.col("casting_output_status") == 780350005, "Complete")
         .otherwise(F.col("casting_output_status").cast("string"))
    )
),

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 360,
    "full_load": False,                # first run full; then set False for incrementals
    "full_load_repartitions": 64
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Casting Log

# CELL ********************

def transform_casting_prod_order(df):
    """Transform Casting Production Order Log for Silver Lakehouse."""
    def ts(c):
        try:
            return parse_any_ts(c)
        except NameError:
            return F.to_timestamp(c)

    # --- Parse timestamps ---
    for c in ["createdon", "modifiedon"]:
        if c in df.columns:
            df = df.withColumn(c, ts(c))

    # --- Map boolean 0/1 → Yes/No ---
    if "cr535_postedtobc" in df.columns:
        df = df.withColumn(
            "cr535_postedtobc",
            F.when(F.col("cr535_postedtobc").isin(1, "1", True, "TRUE", "Y"), F.lit("Yes"))
             .when(F.col("cr535_postedtobc").isin(0, "0", False, "FALSE", "N"), F.lit("No"))
             .otherwise(None)
        )

    # --- Trim strings ---
    text_cols = [
        "createdbyname", "modifiedbyname", "cr535_orderno",
        "cr535_modulename", "cr535_useremail"
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

casting_log_cfg = {
    # ---- SOURCE ----
    "source_path": "abfss://Dataverse_link@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/cr535_auditlog",

    # ---- TARGET ----
    "target_schema": "prod",
    "target_table":  "silver_casting_log",

    # ---- NATURAL KEY ----
    "natural_key": ["casting_prod_order", "created_on"],

    # ---- COLUMN MAP (source -> target, snake_case) ----
    "col_map": {
        "createdon":          "created_on",
        "createdbyname":      "created_by",
        "modifiedon":         "modified_on",
        "modifiedbyname":     "modified_by",
        "cr535_orderno":      "casting_prod_order",
        "cr535_modulename":   "casting_status",
        "cr535_postedtobc":   "is_posted_to_BC",
        "cr535_useremail":    "user_id"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col":   "modifiedon",
    "created_src_col":    "createdon",
    "modified_target_col":"modified_on",

    # ---- TYPES ----
    "ts_cols": ["created_on", "modified_on"],
    "decimal_cols": {},
    "ddl_overrides": {},

    # ---- FINAL TARGET COLS ----
    "target_cols": [
        "created_on",
        "created_by",
        "modified_on",
        "modified_by",
        "casting_prod_order",
        "casting_status",
        "is_posted_to_BC",
        "user_id",
        "load_ts",
        "source_system"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": transform_casting_prod_order,

    "dedupe": True,
    "lookback_minutes": 360,
    "full_load": False,                # first run full; then set False for incrementals
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

# ## Item

# CELL ********************

# ==============================================================
# Inventory • Item (ALL columns, snake_case)
# ==============================================================

item_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/3ea0efcd-03d5-44f1-8e70-99f52a5c2a22/Tables/dbo/inv.bronze_item",

    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "inv",
    "target_table": "silver_item",

    # ---- BUSINESS KEY ----
    "business_key": ["item_no"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "item_no": "item_no",
        "item_description": "item_description",
        "item_description_2": "item_description_2",
        "item_block": "item_block",
        "item_type": "item_type",
        "prod_type": "prod_type",
        "item_replenishment": "item_replenishment",
        "item_uom": "item_uom",
        "item_uom2": "item_uom_2",
        "ratio_qty_to_qty2": "ratio_qty_to_qty_2",
        "Last_Date_Modified": "modified_on",
        "GTIN": "gtin",
        "Item_Category_Code": "item_category_code",
        "CommonItemNo": "common_item_no",
        "Scarp_Item": "scrap_item",
        "item_metal_category": "item_metal_category",
        "Casting_Temp": "casting_temp",
        "Flask_Temp": "flask_temp",
        "Oven_Temp": "oven_temp",
        "Search_Description": "search_description",
        "Inventory": "inventory",
        "item_inventory_unit": "item_inventory_unit",
        "InventoryNonFoundation": "inventory_non_foundation",
        "Qty_on_Purch_Order": "qty_on_purch_order",
        "Qty_on_Purch_Order_2": "qty_on_purch_order_2",
        "Qty_on_Prod_Order": "qty_on_prod_order",
        "Qty_on_Prod_Order_2": "qty_on_prod_order_2",
        "Qty_on_Component_Lines": "qty_on_component_lines",
        "Qty_on_Component_Lines_2": "qty_on_component_lines_2",
        "Qty_on_Sales_Order": "qty_on_sales_order",
        "Qty_on_Sales_Order_2": "qty_on_sales_order_2",
        "Qty_on_Sales_Return": "qty_on_sales_return",
        "Qty_on_Job_Order": "qty_on_job_order",
        "Qty_on_Assembly_Order": "qty_on_assembly_order",
        "Qty_on_Asm_Component": "qty_on_asm_component",
        "Net_Weight": "net_weight",
        "Gross_Weight": "gross_weight",
        "Unit_Volume": "unit_volume",
        "Over_Receipt_Code": "over_receipt_code",
        "Over_Under_Rcpt_Tol_Percent_DU_TSL": "over_under_rcpt_tol_percent_du_tsl",
        "Units_Over_Receipt_Code_DU_TSL": "units_over_receipt_code_du_tsl",
        "Units_O_U_Rct_Tol_Percent_DU_TSL": "units_o_u_rct_tol_percent_du_tsl",
        "Trans_Ord_Receipt_Qty": "trans_ord_receipt_qty",
        "Trans_Ord_Shipment_Qty": "trans_ord_shipment_qty",
        "Qty_in_Transit": "qty_in_transit",
        "prod_bom_status": "prod_bom_status",
        "prod_bom_active_ver_code": "prod_bom_active_ver_code",
        "single_level_material_cost": "single_level_material_cost",
        "prod_bom_uom": "prod_bom_uom",
        "prod_bom_version_nos": "prod_bom_version_nos",
        "routing_status": "routing_status",
        "routing_active_ver_code": "routing_active_ver_code",
        "single_level_costs": "single_level_costs",
        "routing_type": "routing_type",
        "routing_ver": "routing_ver",
        "Costing_Method": "costing_method",
        "Standard_Cost": "standard_cost",
        "item_cost": "item_cost",
        "Indirect_Cost_Percent": "indirect_cost_percent",
        "Last_Direct_Cost": "last_direct_cost",
        "Net_Invoiced_Qty": "net_invoiced_qty",
        "Cost_is_Adjusted": "cost_is_adjusted",
        "Excluded_from_Cost_Adjustment": "excluded_from_cost_adjustment",
        "Cost_is_Posted_to_G_L": "cost_is_posted_to_gl",
        "Inventory_Value_Zero": "inventory_value_zero",
        "Gen_Prod_Posting_Group": "gen_prod_posting_group",
        "VAT_Prod_Posting_Group": "vat_prod_posting_group",
        "Tax_Group_Code": "tax_group_code",
        "Inventory_Posting_Group": "inventory_posting_group",
        "Default_Deferral_Template_Code": "default_deferral_template_code",
        "item_price": "item_price",
        "CalcUnitPriceExclVAT": "calc_unit_price_excl_vat",
        "Price_Includes_VAT": "price_includes_vat",
        "Price_Profit_Calculation": "price_profit_calculation",
        "Profit_Percent": "profit_percent",
        "SpecialSalesPriceListTxt": "special_sales_price_list_txt",
        "Item_Disc_Group": "item_disc_group",
        "Sales_Unit_of_Measure": "sales_unit_of_measure",
        "S_Order_Unit_of_Measure_DU_TSL": "s_order_unit_of_measure_du_tsl",
        "Service_Commitment_Option": "service_commitment_option",
        "Sales_Blocked": "sales_blocked",
        "Service_Blocked": "service_blocked",
        "VAT_Bus_Posting_Gr_Price": "vat_bus_posting_gr_price",
        "Replenishment_System": "replenishment_system",
        "Lead_Time_Calculation": "lead_time_calculation",
        "item_buy_from": "item_buy_from",
        "item_vendor_code": "item_vendor_code",
        "item_purch_uom": "item_purch_uom",
        "item_purch_uom_2": "item_purch_uom_2",
        "item_purch_block": "item_purch_block",
        "item_manufacturing_policy": "item_manufacturing_policy",
        "Routing_No": "routing_no",
        "Production_BOM_No": "production_bom_no",
        "Purchase_Bom": "purchase_bom",
        "Rounding_Precision": "rounding_precision",
        "Flushing_Method": "flushing_method",
        "Overhead_Rate": "overhead_rate",
        "Scrap_Percent": "scrap_percent",
        "Lot_Size": "lot_size",
        "Allow_Whse_Overpick": "allow_whse_overpick",
        "Production_Blocked": "production_blocked",
        "Assembly_Policy": "assembly_policy",
        "AssemblyBOM": "assembly_bom",
        "item_reordering_policy": "item_reordering_policy",
        "item_reserve": "item_reserve",
        "item_order_tracking": "item_order_tracking",
        "Stockkeeping_Unit_Exists": "stockkeeping_unit_exists",
        "item_dampener_period": "item_dampener_period",
        "item_dampener_quantity": "item_dampener_quantity",
        "Critical": "critical",
        "item_safety_stock_lead_time": "item_safety_stock_lead_time",
        "item_safety_stock_quantity": "item_safety_stock_quantity",
        "item_include_inventory": "item_include_inventory",
        "Lot_Accumulation_Period": "lot_accumulation_period",
        "Rescheduling_Period": "rescheduling_period",
        "item_reorder_point": "item_reorder_point",
        "item_reorder_quantity": "item_reorder_quantity",
        "item_maximum_inventory": "item_maximum_inventory",
        "Overflow_Level": "overflow_level",
        "Time_Bucket": "time_bucket",
        "item_minimum_order_quantity": "item_minimum_order_quantity",
        "item_maximum_order_quantity": "item_maximum_order_quantity",
        "item_order_multiple": "item_order_multiple",
        "Item_Tracking_Code": "item_tracking_code",
        "Lot_Nos": "lot_nos"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "Last_Date_Modified",
    "created_src_col": "Last_Date_Modified",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": ["modified_on"],

    # ---- DECIMAL COLUMNS ----
    "decimal_cols": {
        "ratio_qty_to_qty_2": "DECIMAL(18,6)",
        "inventory": "DECIMAL(18,2)",
        "inventory_non_foundation": "DECIMAL(18,2)",
        "qty_on_purch_order": "DECIMAL(18,2)",
        "qty_on_purch_order_2": "DECIMAL(18,2)",
        "qty_on_prod_order": "DECIMAL(18,2)",
        "qty_on_prod_order_2": "DECIMAL(18,2)",
        "qty_on_component_lines": "DECIMAL(18,2)",
        "qty_on_component_lines_2": "DECIMAL(18,2)",
        "qty_on_sales_order": "DECIMAL(18,2)",
        "qty_on_sales_order_2": "DECIMAL(18,2)",
        "qty_on_sales_return": "DECIMAL(18,2)",
        "qty_on_job_order": "DECIMAL(18,2)",
        "qty_on_assembly_order": "DECIMAL(18,2)",
        "qty_on_asm_component": "DECIMAL(18,2)",
        "net_weight": "DECIMAL(18,4)",
        "gross_weight": "DECIMAL(18,4)",
        "unit_volume": "DECIMAL(18,4)",
        "over_under_rcpt_tol_percent_du_tsl": "DECIMAL(18,4)",
        "units_over_receipt_code_du_tsl": "DECIMAL(18,4)",
        "units_o_u_rct_tol_percent_du_tsl": "DECIMAL(18,4)",
        "trans_ord_receipt_qty": "DECIMAL(18,2)",
        "trans_ord_shipment_qty": "DECIMAL(18,2)",
        "qty_in_transit": "DECIMAL(18,2)",
        "single_level_material_cost": "DECIMAL(18,4)",
        "single_level_costs": "DECIMAL(18,4)",
        "standard_cost": "DECIMAL(18,4)",
        "item_cost": "DECIMAL(18,4)",
        "indirect_cost_percent": "DECIMAL(18,4)",
        "last_direct_cost": "DECIMAL(18,4)",
        "net_invoiced_qty": "DECIMAL(18,2)",
        "item_price": "DECIMAL(18,2)",
        "calc_unit_price_excl_vat": "DECIMAL(18,2)",
        "price_profit_calculation": "DECIMAL(18,4)",
        "profit_percent": "DECIMAL(18,4)",
        "rounding_precision": "DECIMAL(18,4)",
        "overhead_rate": "DECIMAL(18,4)",
        "scrap_percent": "DECIMAL(18,4)",
        "lot_size": "DECIMAL(18,2)",
        "item_dampener_quantity": "DECIMAL(18,2)",
        "item_safety_stock_quantity": "DECIMAL(18,2)",
        "item_reorder_point": "DECIMAL(18,2)",
        "item_reorder_quantity": "DECIMAL(18,2)",
        "item_maximum_inventory": "DECIMAL(18,2)",
        "item_minimum_order_quantity": "DECIMAL(18,2)",
        "item_maximum_order_quantity": "DECIMAL(18,2)",
        "item_order_multiple": "DECIMAL(18,2)"
    },

    "ddl_overrides": {},

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "item_no", "modified_on",
        "item_description", "item_description_2", "search_description",
        "item_block", "item_type", "prod_type", "item_replenishment",
        "item_uom", "item_uom_2", "ratio_qty_to_qty_2",
        "gtin", "item_category_code", "common_item_no", "scrap_item",
        "item_metal_category", "casting_temp", "flask_temp", "oven_temp",
        "inventory", "inventory_non_foundation", "item_inventory_unit",
        "qty_on_purch_order", "qty_on_purch_order_2", "qty_on_prod_order",
        "qty_on_prod_order_2", "qty_on_component_lines", "qty_on_component_lines_2",
        "qty_on_sales_order", "qty_on_sales_order_2", "qty_on_sales_return",
        "qty_on_job_order", "qty_on_assembly_order", "qty_on_asm_component",
        "trans_ord_receipt_qty", "trans_ord_shipment_qty", "qty_in_transit",
        "net_weight", "gross_weight", "unit_volume",
        "over_receipt_code", "over_under_rcpt_tol_percent_du_tsl",
        "units_over_receipt_code_du_tsl", "units_o_u_rct_tol_percent_du_tsl",
        "prod_bom_status", "prod_bom_active_ver_code", "single_level_material_cost",
        "prod_bom_uom", "prod_bom_version_nos", "routing_status",
        "routing_active_ver_code", "single_level_costs", "routing_type",
        "routing_ver", "costing_method", "standard_cost", "item_cost",
        "indirect_cost_percent", "last_direct_cost", "net_invoiced_qty",
        "cost_is_adjusted", "excluded_from_cost_adjustment", "cost_is_posted_to_gl",
        "inventory_value_zero", "gen_prod_posting_group", "vat_prod_posting_group",
        "tax_group_code", "inventory_posting_group", "default_deferral_template_code",
        "item_price", "calc_unit_price_excl_vat", "price_includes_vat",
        "price_profit_calculation", "profit_percent", "special_sales_price_list_txt",
        "item_disc_group", "sales_unit_of_measure", "s_order_unit_of_measure_du_tsl",
        "service_commitment_option", "sales_blocked", "service_blocked",
        "vat_bus_posting_gr_price", "replenishment_system", "lead_time_calculation",
        "item_buy_from", "item_vendor_code", "item_purch_uom", "item_purch_uom_2",
        "item_purch_block", "item_manufacturing_policy", "routing_no",
        "production_bom_no", "purchase_bom", "rounding_precision",
        "flushing_method", "overhead_rate", "scrap_percent", "lot_size",
        "allow_whse_overpick", "production_blocked", "assembly_policy",
        "assembly_bom", "item_reordering_policy", "item_reserve", "item_order_tracking",
        "stockkeeping_unit_exists", "item_dampener_period", "item_dampener_quantity",
        "critical", "item_safety_stock_lead_time", "item_safety_stock_quantity",
        "item_include_inventory", "lot_accumulation_period", "rescheduling_period",
        "item_reorder_point", "item_reorder_quantity", "item_maximum_inventory",
        "overflow_level", "time_bucket", "item_minimum_order_quantity",
        "item_maximum_order_quantity", "item_order_multiple", "item_tracking_code",
        "lot_nos"
    ],

    # ---- CUSTOM TRANSFORM ----
    "custom_transform": apply_enum_maps,

    # ---- LOAD BEHAVIOR ----
    "dedupe": True,
    "lookback_minutes": 360,
    "full_load": False,
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
    "Silver_Production_Lakehouse": [
        prod_tracking_repair_cfg,
        casting_part_cfg,
        casting_tree_cfg,
        casting_log_cfg,
        # prod_status_cfg,
        # prod_header_cfg,
        # prod_line_cfg,
    ],
    # "Silver_Inventory_Lakehouse": [
    #     item_cfg,
    # ]
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
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Dedupe

# CELL ********************

# ==============================================
# MULTI-LAKEHOUSE DEDUPE (RUNS EVERY 3 HOURS @ :00)
# ==============================================

from pyspark.sql import functions as F, Window
from datetime import datetime

# ---------------------------
# CONFIGURE HERE
# ---------------------------

# Lakehouses attached to this notebook (add/remove as needed)
LAKEHOUSES = [
    "Silver_Production_Lakehouse",
    # "Silver_QA_Lakehouse",
]

# Tables to dedupe in each lakehouse
# keys = partition columns for row_number()
# order = column used to pick the "latest" within each partition
DEDUPE_SPECS = [
    # prod schema
    {
        "schema": "prod",
        "table":  "silver_prod_order_status",
        "keys":   ["prod_order_no","prod_order_line_no","type_name","current_location_code","machine_center_no","past_location_code"],
        "order":  "created_on",
    },
    {
        "schema": "prod",
        "table":  "silver_prod_order_header",
        "keys":   ["prod_order_no"],
        "order":  "modified_on",
    },
    {
        "schema": "prod",
        "table":  "silver_prod_order_line",
        "keys":   ["prod_order_no","prod_order_line_no"],
        "order":  "modified_on",
    },
    {
        "schema": "prod",
        "table":  "silver_prod_routing_line",
        "keys":   ["prod_order_no","prod_order_line_no","previous_operation_no","next_operation_no","operation_no"],
        "order":  "modified_on",
    },
    {
        "schema": "prod",
        "table":  "silver_casting_parts",
        "keys":   ["prod_order_no","prod_order_line_no","casting_prod_order"],
        "order":  "modified_on",
    },
    {
        "schema": "prod",
        "table":  "silver_casting_tree",
        "keys":   ["casting_prod_order","casting_tree_no"],
        "order":  "modified_on",
    },
        {
        "schema": "prod",
        "table":  "silver_production_bom",
        "keys":   ["bom_no", "bom_version", "item_no"],
        "order":  "modified_on",
    },

    # # cx / sales schema
    # {
    #     "schema": "cx",
    #     "table":  "silver_sales_header",
    #     "keys":   ["sales_order_no"],
    #     "order":  "modified_on",
    # },
    # {
    #     "schema": "cx",
    #     "table":  "silver_sales_line",
    #     "keys":   ["sales_order_no","sales_order_line_no"],
    #     "order":  "modified_on",
    # },

    # master schema (uncomment if you want to dedupe item master too)
    # {
    #     "schema": "master",
    #     "table":  "silver_item_master",
    #     "keys":   ["item_no"],
    #     "order":  "modified_on",
    # },
]

# Maintenance knobs
DO_OPTIMIZE = True
DO_VACUUM = True
VACUUM_HOURS = 168

# Manual override: set to True to run even if we're not on a 3-hour boundary
FORCE_DEDUPE = False

# ---------------------------
# HELPERS
# ---------------------------

def table_exists(qualified_table: str) -> bool:
    """Return True if <lakehouse>.<schema>.<table> exists."""
    try:
        return spark.catalog.tableExists(qualified_table)
    except Exception:
        return False

def should_run_dedupe() -> bool:
    """Return True when the current hour is multiple of 3 and minute is 0 (or forced)."""
    if FORCE_DEDUPE:
        print("[dedupe] FORCE_DEDUPE=True; running regardless of time window.")
        return True
    now = datetime.now()
    hour, minute = now.hour, now.minute
    if hour % 3 == 0 and minute == 0:
        print(f"[dedupe] {now:%Y-%m-%d %H:%M} — window open (hour % 3 == 0 and minute == 0).")
        return True
    print(f"[dedupe] {now:%Y-%m-%d %H:%M} — window closed (next at hour multiple of 3, minute 00).")
    return False

def dedupe_overwrite(full_table: str, partition_cols: list, order_col: str) -> dict:
    """
    Deduplicate a Delta table in place.
    Keeps only the latest record per partition (based on order_col DESC).
    Returns a result dict for summary.
    """
    result = {
        "table": full_table,
        "before": 0,
        "after": 0,
        "deleted": 0,
        "pct_deleted": 0.0,
        "status": "skipped",
        "error": None,
    }

    print(f"\n[dedupe] Table: {full_table}")
    print(f"[dedupe] Partition keys: {partition_cols}")
    print(f"[dedupe] Order by: {order_col}")

    if not table_exists(full_table):
        print(f"[dedupe] Table not found; skipping: {full_table}")
        return result

    df = spark.table(full_table)
    total_before = df.count()
    result["before"] = total_before

    if total_before == 0:
        print("[dedupe] Empty table; skipping.")
        result["status"] = "empty"
        return result

    # Guard: order column must exist
    if order_col not in df.columns:
        print(f"[dedupe] Column '{order_col}' not found; skipping.")
        result["status"] = "order_col_missing"
        return result

    # Window + row_number to keep latest per partition
    w = Window.partitionBy(*partition_cols).orderBy(F.col(order_col).desc())
    out = df.withColumn("_rn", F.row_number().over(w)) \
            .filter(F.col("_rn") == 1) \
            .drop("_rn")

    total_after = out.count()
    deleted = total_before - total_after
    pct = (deleted / total_before * 100) if total_before else 0.0

    # Write back
    (out.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table))

    # Maintenance
    if DO_OPTIMIZE:
        spark.sql(f"OPTIMIZE {full_table}")
    if DO_VACUUM:
        spark.sql(f"VACUUM {full_table} RETAIN {VACUUM_HOURS} HOURS")

    # Logs
    print(f"[dedupe] Removed duplicates: {deleted:,} ({pct:.2f}%)")
    print(f"[dedupe] Rows after: {total_after:,}")
    if DO_OPTIMIZE:
        print(f"[dedupe] OPTIMIZE done.")
    if DO_VACUUM:
        print(f"[dedupe] VACUUM retain {VACUUM_HOURS}h done.")

    # Result
    result.update({
        "after": total_after,
        "deleted": deleted,
        "pct_deleted": round(pct, 2),
        "status": "ok",
    })
    return result

# ---------------------------
# RUN (time-gated)
# ---------------------------

RUN_RESULTS = []

if should_run_dedupe():
    for lakehouse in LAKEHOUSES:
        print(f"\n=== DEDUPE for lakehouse: {lakehouse} ===")
        for spec in DEDUPE_SPECS:
            full_table = f"{lakehouse}.{spec['schema']}.{spec['table']}"
            try:
                res = dedupe_overwrite(full_table, spec["keys"], spec["order"])
                RUN_RESULTS.append(res)
            except Exception as e:
                msg = str(e)
                print(f"[dedupe] ERROR on {full_table}: {msg}")
                RUN_RESULTS.append({
                    "table": full_table,
                    "before": None,
                    "after": None,
                    "deleted": None,
                    "pct_deleted": None,
                    "status": "error",
                    "error": msg,
                })
else:
    print("\n[dedupe] Not a dedupe window. Set FORCE_DEDUPE=True to override.")

# ---------------------------
# SUMMARY
# ---------------------------

print("\n=== DEDUPE SUMMARY ===")
if not RUN_RESULTS:
    print("(No dedupe run this time.)")
else:
    for r in RUN_RESULTS:
        print(
            f"{r['table']}: status={r['status']}"
            + ("" if r.get("error") is None else f", error={r['error']}")
            + ("" if r['status'] != "ok" else f", before={r['before']:,}, after={r['after']:,}, "
               f"deleted={r['deleted']:,} ({r['pct_deleted']}%)")
        )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Merge Tables

# MARKDOWN ********************

# ## Sub Contract Receive Line

# CELL ********************

# Incremental build for: analytics.sub_contract_receive_lines
# - Source: Append_Bronze_Lakehouse.prod.bronze_sub_contract_receive_lines (rl)
#           Append_Bronze_Lakehouse.prod.bronze_sub_contract_receive_lines_details (rld)
# - Watermark: updated_at = coalesce(modified_on, created_on)

from pyspark.sql import functions as F
from delta.tables import DeltaTable

TARGET_DB = "Silver_Production_Lakehouse.prod"
TARGET_TABLE = "silver_sub_contract_receive_lines"
FULL_TARGET = f"{TARGET_DB}.{TARGET_TABLE}"

RL = "`ENG-Bronze`.Append_Bronze_Lakehouse.prod.bronze_sub_contract_receive_lines"
RLD = "`ENG-Bronze`.Append_Bronze_Lakehouse.prod.bronze_sub_contract_receive_lines_details"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {TARGET_DB}")

# 1) Build the source dataframe (your SQL, expressed in PySpark)
rl = spark.table(RL).alias("rl")
rld = spark.table(RLD).alias("rld")

df_src = (
    rl.join(
        rld,
        F.col("rld.cr535_subcontractreceivelinesprod") == F.col("rl.cr535_subreceivedocumentno"),
        "left"
    )
    .select(
        F.col("rl.createdon").alias("created_on"),
        F.col("rl.modifiedon").alias("modified_on"),
        F.col("rl.createdbyname").alias("created_by_name"),
        F.col("rl.cr535_receiveid").alias("receive_id"),
        F.col("rl.cr535_subreceivedocumentno").alias("sub_receive_document_no"),
        F.col("rl.cr535_totalreceiveqty").cast("decimal(10,0)").alias("total_received_quantity"),
        F.col("rl.cr535_totalreceiveweight").cast("decimal(10,0)").alias("total_received_weight"),
        F.col("rld.cr535_subcontractreceivelinesidname").alias("sub_contract_line_id_name"),
        F.col("rl.cr535_subcontractissueheaderid").alias("sub_contract_issue_header_id"),
        F.col("rl.cr535_subcontractheader").alias("sub_contract_header"),
        F.col("rl.cr535_subcontractheadername").alias("sub_contract_header_name"),
        F.col("rl.cr535_temprocess").alias("temprocess")
        # NOTE: if you need temprocess_name later, add the column here when it exists in bronze
    )
    .withColumn("updated_at", F.coalesce(F.col("modified_on"), F.col("created_on")))
    .withColumn("load_ts", F.current_timestamp())  # optional lineage
)

# 2) Figure out the watermark from the target (max(updated_at))
def get_last_watermark():
    if spark.catalog.tableExists(FULL_TARGET):
        w = spark.table(FULL_TARGET).agg(F.max("updated_at").alias("wm")).collect()[0]["wm"]
        return w
    return None

last_wm = get_last_watermark()

# 3) Filter source to new/changed rows (incremental)
if last_wm is not None:
    incr = df_src.filter(F.col("updated_at") > F.lit(last_wm))
else:
    incr = df_src

# Short-circuit: nothing new to process
if incr.rdd.isEmpty():
    print("No new or updated rows. ✌️")
else:
    # 4) Create target if missing (managed Delta table)
    if not spark.catalog.tableExists(FULL_TARGET):
        (
            incr
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(FULL_TARGET)
        )
        # Optional: basic constraints/indexing-ish hints
        spark.sql(f"ALTER TABLE {FULL_TARGET} SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)")
        # If you want a ZORDER later, run OPTIMIZE … ZORDER BY (receive_id, sub_contract_line_id_name)

    else:
        # 5) Upsert via MERGE on business key (receive_id + sub_contract_line_id_name)
        tgt = DeltaTable.forName(spark, FULL_TARGET)
        (
            tgt.alias("t")
            .merge(
                incr.alias("s"),
                """
                t.receive_id = s.receive_id
                AND coalesce(t.sub_contract_line_id_name, '') = coalesce(s.sub_contract_line_id_name, '')
                """
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    # Optional housekeeping
    # spark.sql(f"OPTIMIZE {FULL_TARGET} ZORDER BY (receive_id, sub_contract_line_id_name)")
    # spark.sql(f"VACUUM {FULL_TARGET} RETAIN 168 HOURS")  # 7 days, adjust to your retention policy


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
