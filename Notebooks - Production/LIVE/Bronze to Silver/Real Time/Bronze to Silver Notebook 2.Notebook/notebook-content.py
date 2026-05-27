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

# CELL ********************

# ==============================================================
# FABRIC SILVER LAKEHOUSE PIPELINE — HYBRID OPTIMIZED VERSION
# DEDUPE DEFAULT = TRUE
#
# Changes applied:
# 1. Maintenance OFF by default
# 2. Removed diagnose_dupe_keys() from production path
# 3. Removed enforce_unique() double count path
# 4. Dedupe behavior now follows cfg["dedupe"] consistently
# 5. Uses isEmpty() instead of limit(1).count()
# 6. Consolidates metrics into one aggregate
# 7. Avoids extra count on seed path
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
    """Read last processed timestamp from Silver (string column parsed on the fly)."""
    if not table_exists(full_table):
        return None
    row = (
        spark.table(full_table)
        .select(F.max(F.to_timestamp(F.col(modified_col))).alias("mx"))
        .collect()[0]
    )
    return row["mx"]  # datetime or None


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
    passthrough_cols: Optional[List[str]] = None
) -> DataFrame:
    """
    Select and rename based on mapping; add missing mapped columns as NULL.
    Also keeps any 'passthrough_cols' (e.g., _coalesced_mod_ts).
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
    """Cast columns and add metadata. (Does NOT touch modified_on unless listed in ts_cols)"""
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
    Keeps extra columns when keep_extra=True (e.g., _coalesced_mod_ts).
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
        extra_cols = [c for c in current_cols if c not in target_cols and c not in passthrough_cols]
        return out.select(
            *target_cols,
            *[c for c in passthrough_cols if c in current_cols],
            *extra_cols
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


def dedupe_latest(df: DataFrame, key_cols: List[str], order_col: str = "_coalesced_mod_ts") -> DataFrame:
    """
    Keep latest row per key by order_col DESC. Used when dedupe=True.
    """
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
        if partition_cols else ""
    )
    spark.sql(f"CREATE TABLE IF NOT EXISTS {full_target} ({ddl}) USING DELTA{part}")


def merge_upsert(staging_view: str, full_target: str, key_cols: List[str], target_cols: List[str]):
    """SCD1 MERGE (null-safe) — does NOT update key columns."""
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


def maintain(full_target: str, zorder_cols: Optional[List[str]] = None, vacuum_hours: int = 168):
    z_clause = ""
    if zorder_cols:
        tgt_cols = set(spark.table(full_target).columns)
        z_keep = [c for c in zorder_cols if c in tgt_cols]
        if z_keep:
            z_clause = f" ZORDER BY ({', '.join([f'`{c}`' for c in z_keep])})"
    spark.sql(f"OPTIMIZE {full_target}{z_clause}")
    spark.sql(f"VACUUM {full_target} RETAIN {vacuum_hours} HOURS")


# -------------------- OPTIONAL ENUM MAPPER --------------------
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
    """
    All enum mappings combined into a single helper.
    """
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

    ITEM_RENDERING_MAP = {
        184930000: "Fixed Reorder Qty",
        184930001: "Maximum Qty",
        184930002: "Order",
        184930003: "Lot-for-Lot",
    }

    RESERVE_MAP = {
        184930000: "Never",
        184930001: "Optional",
        184930002: "Always",
    }

    ORDER_TRACKING_POLICY_MAP = {
        184930000: "None",
        184930001: "Tracking Only",
        184930002: "Tracking & Action Msg",
    }

    REPLENISHMENT_SYSTEM_MAP = {
        184930000: "Purchase",
        184930001: "Prod Order",
        184930002: "Transfer",
        184930003: "Assembly",
    }

    MANUFACTURING_POLICY_MAP = {
        184930000: "Make-to-Stock",
        184930001: "Make-to-Order",
    }

    REPAIR_POSTING_STATUS_MAP = {
        780350000: "Create Repair PRO",
        780350001: "Main PRO - Post Negative Consumption",
        780350002: "Repair PRO - Post Consumption",
        780350003: "Complete",
    }

    MOLD_STATE_MAP = {
        127740000: "Pending",
        127740001: "Making",
        127740002: "In Progress",
        127740003: "Completed",
        127740004: "Pending Approval",
        127740005: "Quality Failed",
        127740006: "Expired",
    }

    MOLD_MATERIAL_MAP = {
        127740000: "Rubber",
        127740001: "Silicone",
        127740002: "Other",
    }

    MOLD_TASK_STATUS_MAP = {
        127740000: "Pending",
        127740001: "In Progress",
        127740002: "Ready For QC",
        127740003: "Done",
    }

    MOLD_PRIORITY_MAP = {
        127740000: "Urgent",
        127740001: "High",
        127740002: "Normal",
    }

    WAX_TYPE_MAP = {
        184930000: "Green Wax",
    }

    out = df
    out = apply_enum_map(out, "open", OPEN_MAP)
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
    out = apply_enum_map(out, "mold_approved", OPEN_MAP)
    out = apply_enum_map(out, "mold_blocked", OPEN_MAP)
    out = apply_enum_map(out, "mold_expired", OPEN_MAP)
    out = apply_enum_map(out, "mold_powder", OPEN_MAP)
    out = apply_enum_map(out, "mold_status", MOLD_STATE_MAP)
    out = apply_enum_map(out, "mold_material", MOLD_MATERIAL_MAP)
    out = apply_enum_map(out, "mold_task_status", MOLD_TASK_STATUS_MAP)
    out = apply_enum_map(out, "mold_priority", MOLD_PRIORITY_MAP)
    out = apply_enum_map(out, "wax_type", WAX_TYPE_MAP)
    return out


# -------------------- DATETIME SANITIZER --------------------
def fix_ancient_datetimes(
    df,
    ts_cols=None,
    date_cols=None,
    policy="null",
    floor_ts="1900-01-01 00:00:00",
    floor_date="1900-01-01"
):
    """
    Sanitize ancient/sentinel datetimes that break Spark 3's Proleptic Gregorian handling.
    - policy="null": set bad values to NULL
    - policy="floor": clamp to floor_ts/floor_date
    Works for both TimestampType and DateType columns (and string-ish inputs).
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
            is_bad_literal = F.lower(F.col(c).cast("string")).isin([s.lower() for s in bad_ts_literals])
            is_too_old = col_ts < F.to_timestamp(F.lit(floor_ts))
            bad = is_bad_literal | is_too_old
            if policy == "null":
                out = out.withColumn(
                    c,
                    F.when(bad, F.lit(None).cast(T.TimestampType())).otherwise(col_ts)
                )
            else:
                out = out.withColumn(
                    c,
                    F.when(bad, F.to_timestamp(F.lit(floor_ts))).otherwise(col_ts)
                )

    for c in date_cols:
        if c in out.columns:
            col_date = F.to_date(F.col(c))
            is_bad_literal = F.lower(F.col(c).cast("string")).isin([s.lower() for s in bad_date_literals])
            is_too_old = col_date < F.to_date(F.lit(floor_date))
            bad = is_bad_literal | is_too_old
            if policy == "null":
                out = out.withColumn(
                    c,
                    F.when(bad, F.lit(None).cast(T.DateType())).otherwise(col_date)
                )
            else:
                out = out.withColumn(
                    c,
                    F.when(bad, F.to_date(F.lit(floor_date))).otherwise(col_date)
                )

    return out


# -------------------- CORE: RUN ONE TABLE --------------------
def run_silver_table(cfg: Dict) -> Dict:
    """
    Read → Watermark → Rename/Type → Enum → Custom → Ensure/Align → Optional Dedupe → Seed/Merge → Optional Maintain
    - `modified_on` stays STRING in Silver (display as-is)
    - `_coalesced_mod_ts` is used for watermark; metrics use it too
    """
    print(cfg)

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
    target_cols = cfg.get("target_cols", list(col_map.values()) + ["load_ts", "source_system"])
    ddl_overrides_cfg = cfg.get("ddl_overrides", {})
    ddl_overrides = {**ddl_overrides_cfg, modified_target_col: "STRING"}
    custom_transform = cfg.get("custom_transform")
    source_label = cfg.get("load_source_label", "Dataverse")

    # Maintenance OFF by default
    maintenance = cfg.get(
        "maintenance",
        {"optimize": False, "vacuum_hours": 168, "zorder_cols": ["_coalesced_mod_ts"]}
    )

    do_dedupe = cfg.get("dedupe", True)  # default ON

    if not key_cols:
        raise ValueError("cfg must include 'natural_key' or 'business_key'.")

    missing_key_after_rename = [k for k in key_cols if k not in target_cols]
    if missing_key_after_rename:
        raise ValueError(
            f"Key columns not present in target_cols after rename: {missing_key_after_rename}. "
            f"Make sure they’re mapped in col_map and included in target_cols."
        )

    # 1) read + watermark
    base = read_source_delta(source_path, sink_modified_on, created_src_col)
    last_mod = None if full_load else get_last_modified(full_target, modified_target_col)
    base = apply_watermark(base, last_mod, lookback_minutes)

    # 2) rename while preserving ops col
    df = rename_select(base, col_map, passthrough_cols=["_coalesced_mod_ts"])

    # 2.2) type coercion + metadata
    df = coerce_types_and_enrich(df, ts_cols, double_cols, decimal_cols, source_label)

    # 2.3) sanitize bad ancient datetimes
    df = fix_ancient_datetimes(
        df,
        ts_cols=ts_cols,
        date_cols=[],
        policy="null",
        floor_ts="1900-01-01 00:00:00",
        floor_date="1900-01-01"
    )

    # 2.4) enum mapping
    df = apply_enum_maps(df)

    # 3) custom transform
    if callable(custom_transform):
        df = custom_transform(df)

    # 4) ensure schema
    df = ensure_columns(
        df,
        target_cols,
        ts_cols,
        double_cols,
        decimal_cols,
        keep_extra=True,
        passthrough_cols=["_coalesced_mod_ts"],
    )

    # 4.1) DDL types
    ddl_types = {c: "STRING" for c in target_cols}
    for c in ts_cols + ["load_ts"]:
        ddl_types[c] = "TIMESTAMP"
    for c in double_cols:
        ddl_types[c] = "DOUBLE"
    for c, dec in decimal_cols.items():
        ddl_types[c] = dec
    ddl_types.update(ddl_overrides)

    # 4.2) align DF -> DDL
    df = cast_to_target_types(df, ddl_types)

    # 4.3) optional repartition before write/merge
    if full_load and full_repartitions:
        df = df.repartition(full_repartitions)

    # 5) optional dedupe
    if do_dedupe:
        if "_coalesced_mod_ts" not in df.columns:
            raise ValueError("Internal `_coalesced_mod_ts` missing after projection.")
        stg = dedupe_latest(df, key_cols, "_coalesced_mod_ts")
    else:
        stg = df

    # 5.1) materialize
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

    # 6) first-load decision
    first_load = (last_mod is None)

    # 6.1) create table idempotently
    create_managed_table(target_schema, full_target, target_cols, ddl_types)

    # 7) seed or merge
    tmp_view = f"stg_tmp_{target_table}_{uuid.uuid4().hex[:8]}"

    if full_load and first_load:
        stg.select(*target_cols).write.format("delta").mode("append").saveAsTable(full_target)
        mode = "seed"
        inserted_rows = None
        updated_rows = 0
    else:
        stg.select(*target_cols).createOrReplaceTempView(tmp_view)
        merge_upsert(tmp_view, full_target, key_cols, target_cols)
        mode = "merge"
        inserted_rows = None
        updated_rows = None

    print("staging columns:", stg.columns)
    print("target columns:", spark.table(full_target).columns)

    # 8) metrics (single aggregate)
    metrics = stg.agg(
        F.count("*").alias("cnt"),
        F.max(F.col("_coalesced_mod_ts")).alias("max_ts"),
    ).collect()[0]

    row_count = metrics["cnt"]
    max_mod = metrics["max_ts"]

    # 9) maintenance
    if maintenance.get("optimize", False):
        zcols = maintenance.get("zorder_cols")
        if isinstance(zcols, str):
            zcols = [zcols]
        maintain(
            full_target,
            zorder_cols=zcols,
            vacuum_hours=maintenance.get("vacuum_hours", 168)
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

# ## Casting Parts

# CELL ********************

# ==============================================================
# CASTING PARTS
# ==============================================================

casting_part_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/25adf76d-df18-41c8-97e8-55789149bd80/Tables/dts_jccastedpartsprod",  

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
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/25adf76d-df18-41c8-97e8-55789149bd80/Tables/dts_jccastingtreeprod",  

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
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/25adf76d-df18-41c8-97e8-55789149bd80/Tables/cr535_auditlog",

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

# ## Production Order Component

# CELL ********************

# ==============================================================
# CONFIG: TABLE 1 — PRODUCTION ORDER COMPONENT (FIXED)
# ==============================================================

prod_component_cfg = {
    "source_path": "abfss://a873b8b8-df07-446b-8592-ed8b6ea2884a@onelake.dfs.fabric.microsoft.com/25adf76d-df18-41c8-97e8-55789149bd80/Tables/cr535_prodordercomponent",
    "target_schema": "prod",
    "target_table": "silver_prod_order_component",

    # Make sure these match the *target* column names after col_map
    "business_key": ["prod_order_no", "prod_order_line_no"],

    # Source → Target mapping
    "col_map": {
        "createdon": "created_on",
        "modifiedon": "modified_on",                # keep as STRING in Silver
        "cr535_status": "prod_order_status",
        "cr535_prodorderno": "prod_order_no",
        "cr535_prodorderlineno": "prod_order_line_no",
        "cr535_suppliedbylineno": "prod_order_sup_line_no",
        "cr535_duedate": "component_due_date",      # <— note the exact target name
        "cr535_itemno": "item_no",
        "cr535_lineno": "component_line",
        "cr535_description": "item_description",
        "cr535_routinglinkcode": "routing_link_code",
        "cr535_qtyperunitofmeasure": "bom_item_qty_uom",
        "cr535_quantityper": "bom_item_qty_per",
        "cr535_quantity": "bom_item_qty",
        "cr535_locationcode": "component_location",
        "cr535_unitofmeasurecode": "item_uom",
        "cr535_expectedquantity": "component_expected",
        "cr535_remainingquantity": "component_remaining",
        "cr535_shortcutdimension2code": "item_material",
        "cr535_qtypicked": "component_qty_picked",
        "cr535_completelypicked": "component_picked_status",
        "SinkModifiedOn": "SinkModifiedOn"
    },

    # Timestamps to actually parse
    "ts_cols": ["created_on", "component_due_date", "modified_on"],

    # Cast these to DOUBLE
    "double_cols": [
        "bom_item_qty_uom",
        "bom_item_qty_per",
        "bom_item_qty",
        "component_expected",
        "component_remaining",
        "component_qty_picked",
    ],

    # If any decimals are truly DECIMAL, declare them here; else leave as strings/doubles
    "decimal_cols": {
        # example: "component_expected": "DECIMAL(18,2)"
    },

    # Keep enums nice (optional)
    "custom_transform": apply_enum_maps,

    # Force schema where needed — keep modified_on as STRING
    "ddl_overrides": {
        "prod_order_status": "STRING"
    },

    # Ops knobs
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": False,                 # first run: True; subsequent runs: False
    "full_load_repartitions": 64,

    # Optional maintenance tuning
    # "maintenance": {"optimize": True, "vacuum_hours": 168, "zorder_cols": ["_coalesced_mod_ts"]},
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ProWeight

# CELL ********************

# ==============================================================
# Production • Weight
# ==============================================================

pro_weight_cfg = {
    # ---- SOURCE (Bronze Delta table path) ----
    "source_path": "abfss://Dataverse@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/cr535_proweight",
                    
    # ---- TARGET (Silver Lakehouse managed table) ----
    "target_schema": "prod",
    "target_table": "silver_pro_weight",

    # ---- BUSINESS KEY ----
    # prod order + line normally identifies a record
    "business_key": ["prod_order_no", "prod_order_line_no"],

    # ---- COLUMN MAP (source -> target) ----
    "col_map": {
        "SinkCreatedOn": "sink_created_on",
        "SinkModifiedOn": "sink_modified_on",

        "cr535_antennaid": "antenna_id",
        "cr535_itemno": "item_no",
        "cr535_locationcode": "location_code",
        "cr535_machinecenterno": "machine_center_no",
        "cr535_prodorderno": "prod_order_no",
        "cr535_prodorderlineno": "prod_order_line_no",

        "cr535_qty": "quantity",
        "cr535_weight": "weight",
        "cr535_dustweight": "dust_weight",
        "cr535_sprueweight": "sprue_weight",

        "createdon": "created_on",
        "modifiedon": "modified_on"
    },

    # ---- WATERMARK SETTINGS ----
    "modified_src_col": "SinkModifiedOn",
    "created_src_col": "SinkCreatedOn",
    "modified_target_col": "modified_on",

    # ---- TYPES ----
    "ts_cols": [
        "created_on",
        "modified_on",
        "sink_created_on",
        "sink_modified_on"
    ],

    "decimal_cols": {
        "quantity": "DECIMAL(18,2)",
        "weight": "DECIMAL(18,4)",
        "dust_weight": "DECIMAL(18,4)",
        "sprue_weight": "DECIMAL(18,4)"
    },

    "ddl_overrides": {
        "quantity": "DECIMAL(18,2)",
        "weight": "DECIMAL(18,4)",
        "dust_weight": "DECIMAL(18,4)",
        "sprue_weight": "DECIMAL(18,4)"
    },

    # ---- FINAL COLUMN ORDER ----
    "target_cols": [
        "created_on",
        "modified_on",
        "sink_created_on",
        "sink_modified_on",

        "antenna_id",
        "item_no",
        "location_code",
        "machine_center_no",
        "prod_order_no",
        "prod_order_line_no",

        "quantity",
        "weight",
        "dust_weight",
        "sprue_weight",

        "load_ts",
        "source_system"
    ],

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

# ## Mold Master

# CELL ********************

# ==============================================================
# CONFIG: TABLE — WM_MOLDMASTER (Dataverse)  ✅ UPDATED (only requested cols)
# ==============================================================

wm_moldmaster_cfg = {
    "source_path": "abfss://Dataverse@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/wm_moldmaster",
    "target_schema": "prod",
    "target_table": "silver_mold_master",

    # Keys
    "business_key": ["mold_no"],

    # Source → Target mapping (ONLY the cols you asked to keep)
    "col_map": {
        "SinkCreatedOn": "sink_created_on",
        "SinkModifiedOn": "sink_modified_on",

        "cr535_waxtype": "wax_type",
        "wm_moldtaskname": "mold_task_name",

        "wm_moldno": "mold_no",
        "createdon": "created_on",
        "createdbyname": "created_by_name",
        "wm_approved": "mold_approved",

        "wm_bincode": "bin_code",
        "wm_allocatedbinname": "allocated_bin_name",

        "cr535_secondarycastedpartname": "secondary_casted_part_name",

        "cr535_thirdcastedpart": "third_casted_part_id",
        "cr535_thirdcastedpartname": "third_casted_part_name",

        "wm_blocked": "mold_blocked",

        "wm_customerno": "customer_no",
        "wm_customername": "customer_name",
        "wm_customernametext": "customer_name_text",

        "wm_datemade": "date_made",
        "wm_expired": "mold_expired",
        "wm_expirydate": "expiry_date",

        "wm_ndcastedparttext": "secondary_casted_part_text",
        "wm_rdcastedpart": "primary_casted_part",

        "Id": "id",
        "wm_id": "wm_id",

        "wm_itemname": "item_name",
        "wm_itemnotext": "item_no_text",

        "wm_locationname": "location_name",
        "wm_locationtext": "location_text",

        "wm_material": "mold_material",
        "wm_maxshots": "max_shots",

        "wm_modificationlevelname": "modification_level_name",

        "modifiedon": "modified_on",
        "modifiedbyname": "modified_by_name",

        "wm_moldrequestname": "mold_request_name",

        "wm_moldsizename": "mold_size_name",
        "wm_moldsizetext": "mold_size_text",

        "wm_nfcuid": "nfc_uid",

        "cr535_partpermold": "parts_per_mold",
        "wm_powder": "mold_powder",

        "cr535_shelflifeyear": "shelf_life_year",
        "wm_state": "mold_status",

        "wm_teamidname": "team_name",
        "wm_totalshotsrun": "total_shots_run",

        "wm_waxclamp": "wax_clamp",
        "wm_waxpressure": "wax_pressure",
        "wm_waxtime": "wax_time",
        "wm_waxvacuum": "wax_vacuum",
    },

    # Timestamps to parse (ONLY those present in the kept cols)
    "ts_cols": [
        "created_on",
        "modified_on",
        "date_made",
        "expiry_date",
        "sink_created_on",
        "sink_modified_on",
    ],

    # Cast likely-numeric fields to DOUBLE (ONLY those present)
    "double_cols": [
        "parts_per_mold",
        "shelf_life_year",
        "max_shots",
        "total_shots_run",
        "wax_clamp",
        "wax_pressure",
        "wax_time",
        "wax_vacuum",
    ],

    # If any should be DECIMAL, declare them here
    "decimal_cols": {
        # example: "wax_pressure": "DECIMAL(18,2)"
    },

    # Optional enum cleanup (safe to keep)
    "custom_transform": apply_enum_maps,

    # DDL overrides removed because those cols are not in the kept list
    "ddl_overrides": {},

    # Ops knobs
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": False,                 # first run: True; subsequent runs: False
    "full_load_repartitions": 64,

    # Optional maintenance tuning
    # "maintenance": {"optimize": True, "vacuum_hours": 168, "zorder_cols": ["_coalesced_mod_ts"]},
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Mold Task

# CELL ********************

# ==============================================================
# CONFIG: TABLE — WM_MOLDTASK (Dataverse) ✅ UPDATED (only requested cols)
# ==============================================================

wm_moldtask_cfg = {
    "source_path": "abfss://Dataverse@onelake.dfs.fabric.microsoft.com/dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.Lakehouse/Tables/wm_moldtask",
    "target_schema": "prod",
    "target_table": "silver_mold_task",

    # Dataverse primary key
    "business_key": ["mold_no"],

    # Source → Target mapping (ONLY the cols you asked to keep)
    "col_map": {
        "wm_binalloc": "bin_alloc",
        "wm_family": "family",
        "wm_id": "wm_id",
        "wm_nfcuid": "nfc_uid",

        "wm_moldno": "mold_no",
        "createdon": "created_on",
        "createdbyname": "created_by_name",

        "wm_employeeid": "employee_id",
        "wm_assignedto": "assigned_to",
        "wm_datemade": "date_made",
        "wm_startdatetime": "start_datetime",
        "wm_dueat": "due_at",

        "cr535_qcapproveddate": "qc_approved_date",
        "wm_qcdueat": "qc_due_at",

        "wm_itemidname": "item_name",
        "wm_priority": "mold_priority",
        "wm_status": "mold_task_status",

        "modifiedon": "modified_on",
        "modifiedbyname": "modified_by_name",
    },

    # Timestamps to parse (ONLY those present)
    "ts_cols": [
        "created_on",
        "modified_on",
        "date_made",
        "start_datetime",
        "due_at",
        "qc_approved_date",
        "qc_due_at",
    ],

    # Numeric fields to cast to DOUBLE (ONLY those present)
    "double_cols": [
        "priority",
    ],

    "decimal_cols": {
        # example: "priority": "DECIMAL(10,2)"
    },

    # Optional enum cleanup (safe to keep)
    "custom_transform": apply_enum_maps,

    # DDL overrides removed because those code/type cols are not in kept list
    "ddl_overrides": {},

    # Ops knobs
    "dedupe": True,
    "lookback_minutes": 120,
    "full_load": False,                 # first run: True; subsequent runs: False
    "full_load_repartitions": 64,

    # Optional maintenance tuning
    # "maintenance": {"optimize": True, "vacuum_hours": 168, "zorder_cols": ["_coalesced_mod_ts"]},
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
        # prod_component_cfg,
        # casting_part_cfg,
        # casting_tree_cfg,
        # casting_log_cfg,
        pro_weight_cfg,
        wm_moldtask_cfg,
        wm_moldmaster_cfg
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

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_mold_modification AS
# MAGIC SELECT
# MAGIC     wm_machinecentertext as machine_center,
# MAGIC     wm_modificationname as wm_modification_name,          
# MAGIC     wm_runtime as runtime,
# MAGIC     wm_standardtime as standardtime
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.wm_moldmodificationmachinecentermap LIMIT 1000

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE Silver_Production_Lakehouse.prod.silver_rfid_validation_errors_production
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     SinkCreatedOn,
# MAGIC     SinkModifiedOn,
# MAGIC 
# MAGIC     cr535_error AS error_code,
# MAGIC     CASE cr535_error
# MAGIC         WHEN 184930000 THEN 'No weight recorded'
# MAGIC         WHEN 184930001 THEN 'Location cannot be moved. Please check the procedures for picking up and returning the workpiece from the technician before proceeding.'
# MAGIC         WHEN 184930002 THEN 'No Open Employee Transaction Found'
# MAGIC         WHEN 184930003 THEN 'Item Already Issued'
# MAGIC         WHEN 184930004 THEN 'No Open Location Entry Exists'
# MAGIC         WHEN 184930005 THEN 'Lock Validation Enabled'
# MAGIC         WHEN 184930006 THEN 'PRO Line Not Found'
# MAGIC         WHEN 184930007 THEN 'Machine Center Not Found'
# MAGIC         WHEN 184930008 THEN 'Cannot Start Employee Tran. - MC not found for the current WC'
# MAGIC         WHEN 184930009 THEN 'The work cannot be transferred because there are still raw materials that have not been fully disbursed. Please contact the Warehouse.'
# MAGIC         ELSE NULL
# MAGIC     END AS error,
# MAGIC 
# MAGIC     cr535_errortype AS error_type_code,
# MAGIC     CASE cr535_errortype
# MAGIC         WHEN 184930000 THEN 'Pending Employee Transaction'
# MAGIC         WHEN 184930001 THEN 'Item Already Issued'
# MAGIC         WHEN 184930002 THEN 'No Weight Record Found'
# MAGIC         WHEN 184930003 THEN 'No Open Transaction Found'
# MAGIC         WHEN 184930004 THEN 'Lock Validation Enabled'
# MAGIC         WHEN 184930005 THEN 'PRO Line Not Found'
# MAGIC         WHEN 184930006 THEN 'Machine Center Not Found'
# MAGIC         WHEN 184930007 THEN 'Cannot Start Employee Tran. - MC not found for the current WC'
# MAGIC         WHEN 184930008 THEN 'Cannot Stop Employee Tran - Stop Emp Cell not same as start Emp Cell'
# MAGIC         WHEN 184930009 THEN 'QC Not Approved'
# MAGIC         ELSE NULL
# MAGIC     END AS error_type,
# MAGIC 
# MAGIC     cr535_antennaid        AS antenna_id,
# MAGIC     cr535_cellno           AS cell_no,
# MAGIC     cr535_errormessage     AS error_message,
# MAGIC     cr535_prodorderlineno  AS prod_order_line_no,
# MAGIC     cr535_prodorderno      AS prod_order_no,
# MAGIC     createdon              AS created_on,
# MAGIC     modifiedon             AS modified_on
# MAGIC 
# MAGIC FROM Dataverse.dataverse_ennovieprodu_cds2_workspace_unq09bbc58ecdb9ee119073000d3a099.dbo.cr535_rfidvalidationerrorsproduction;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
