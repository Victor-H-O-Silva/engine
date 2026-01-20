from __future__ import annotations

from typing import Dict, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession

from pipeline_runtime.contracts.run_context import RunContext
from pipeline_runtime.writers.table_ref import TableRef
from pipeline_runtime.writers.writer_errors import PreconditionFailed


def _table_exists(spark: SparkSession, target_fqn: str) -> bool:
    parts = [p.strip() for p in str(target_fqn).split(".") if p.strip() != ""]
    if len(parts) != 3:
        raise ValueError(f"Expected target_fqn as catalog.schema.table, got: {target_fqn}")
    c, s, t = parts

    rows = spark.sql(
        f"""
        SELECT 1 FROM {c}.information_schema.tables
        WHERE table_schema = '{s}' AND table_name = '{t}'
        LIMIT 1
        """
    ).collect()
    return len(rows) > 0


def _create_table_if_missing(
    spark: SparkSession,
    df: DataFrame,
    target: TableRef,
    *,
    partition_cols: Optional[Sequence[str]] = None,
    table_properties: Optional[Dict[str, str]] = None,
) -> bool:
    if _table_exists(spark, target.fqn):
        return False

    w = df.write.format("delta").mode("overwrite")
    if partition_cols:
        w = w.partitionBy(*list(partition_cols))

    if table_properties:
        for k, v in table_properties.items():
            w = w.option(f"delta.{k}", str(v))

    w.saveAsTable(target.fqn)
    return True


def _require_columns(df: DataFrame, cols: Sequence[str], *, what: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise PreconditionFailed(f"{what}: missing required columns: {missing}")


def _normalize_partition_cols(partition_cols: Optional[Sequence[str]]) -> Optional[Sequence[str]]:
    if partition_cols is None:
        return None
    out = [str(c).strip() for c in partition_cols if str(c).strip() != ""]
    return out or None


def _rc_tags(rc: RunContext) -> Dict[str, str]:
    return {
        "_rt_run_id": rc.run_id,
        "_rt_intent_hash": rc.intent_hash,
        "_rt_env": rc.env,
        "_rt_mode": rc.mode.value,
        "_rt_window_start_utc": rc.window_start_utc.isoformat(),
        "_rt_window_end_utc": rc.window_end_utc.isoformat(),
        "_rt_framework_version": rc.framework_version,
    }
