from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline_runtime.contracts.run_context import RunContext
from pipeline_runtime.safety.safety_config import SafetyConfig
from pipeline_runtime.safety.safety_events import SafetyEvent, allow, block
from pipeline_runtime.writers.writer_errors import PreconditionFailed


def _split_fqn(fqn: str) -> Tuple[str, str, str]:
    parts = [p.strip() for p in str(fqn).split(".") if p.strip() != ""]
    if len(parts) != 3:
        raise ValueError(f"Expected fully-qualified name <catalog>.<schema>.<table>, got: {fqn}")
    return parts[0], parts[1], parts[2]


def _table_exists(spark: SparkSession, fqn: str) -> bool:
    cat, sch, tbl = _split_fqn(fqn)
    q = f"""
    SELECT 1
    FROM {cat}.information_schema.tables
    WHERE table_schema = '{sch}'
      AND table_name = '{tbl}'
    LIMIT 1
    """
    return spark.sql(q).count() > 0


def _count_distinct_partitions_existing(
    spark: SparkSession,
    target_fqn: str,
    partition_cols: Sequence[str],
    replace_where: str,
) -> int:
    df = spark.table(target_fqn).where(replace_where)
    if not partition_cols:
        return df.count()
    return df.select(*[F.col(c) for c in partition_cols]).dropDuplicates().count()


def enforce_snapshot_overwrite_policy(
    *,
    spark: SparkSession,
    rc: RunContext,
    target_fqn: str,
    partition_cols: Optional[Sequence[str]],
    replace_where: Optional[str],
    cfg: SafetyConfig,
) -> Tuple[List[SafetyEvent], int]:
    events: List[SafetyEvent] = []
    part_cols = [str(c).strip() for c in (partition_cols or []) if str(c).strip() != ""]
    params = rc.params or {}

    allow_full_overwrite_param = cfg.get_param_bool(params, cfg.allow_full_overwrite_param_key, False)
    allow_snapshot_wo_replacewhere_param = cfg.get_param_bool(params, cfg.allow_snapshot_without_replacewhere_param_key, False)
    env_allows_full = cfg.full_overwrite_allowed_for_env(rc.env)

    require_replace_where = bool(cfg.require_replace_where_for_snapshot)

    replace_where_present = replace_where is not None and str(replace_where).strip() != ""

    if require_replace_where and (not replace_where_present):
        if not (env_allows_full and (allow_full_overwrite_param or allow_snapshot_wo_replacewhere_param)):
            ev = block(
                code="SNAPSHOT_REPLACEWHERE_REQUIRED",
                message="Snapshot overwrite requires replaceWhere; full overwrite not allowed by policy.",
                details={
                    "env": rc.env,
                    "target": target_fqn,
                    "allow_full_overwrite_env": env_allows_full,
                    "allow_full_overwrite_param": allow_full_overwrite_param,
                    "allow_snapshot_without_replacewhere_param": allow_snapshot_wo_replacewhere_param,
                },
            )
            events.append(ev)
            raise PreconditionFailed(ev.message)

        events.append(
            allow(
                code="SNAPSHOT_FULL_OVERWRITE_ALLOWED",
                message="Full overwrite allowed by env + explicit param override.",
                details={
                    "env": rc.env,
                    "target": target_fqn,
                },
            )
        )
        return events, -1

    if not _table_exists(spark, target_fqn):
        events.append(
            allow(
                code="TARGET_MISSING_CREATE_ALLOWED",
                message="Target table missing; create is allowed.",
                details={"target": target_fqn},
            )
        )
        return events, 0

    if replace_where_present:
        max_default = cfg.max_partitions_overwrite_for_env(rc.env)
        max_override = cfg.get_param_int(params, cfg.max_param_partitions_key, max_default)
        max_allowed = max_override if max_override > 0 else max_default

        affected = _count_distinct_partitions_existing(
            spark=spark,
            target_fqn=target_fqn,
            partition_cols=part_cols,
            replace_where=str(replace_where).strip(),
        )

        events.append(
            allow(
                code="SNAPSHOT_AFFECTED_PARTITIONS_ESTIMATED",
                message="Estimated affected partitions for snapshot overwrite.",
                details={
                    "target": target_fqn,
                    "replace_where": str(replace_where).strip(),
                    "partition_cols": part_cols,
                    "affected_partitions": affected,
                    "max_allowed": max_allowed,
                    "env": rc.env,
                },
            )
        )

        if affected > max_allowed:
            ev = block(
                code="SNAPSHOT_MAX_PARTITIONS_EXCEEDED",
                message="Snapshot overwrite exceeds max affected partitions threshold.",
                details={
                    "target": target_fqn,
                    "affected_partitions": affected,
                    "max_allowed": max_allowed,
                    "env": rc.env,
                },
            )
            events.append(ev)
            raise PreconditionFailed(ev.message)

        return events, affected

    return events, 0


def enforce_append_dedup_policy(
    *,
    rc: RunContext,
    cfg: SafetyConfig,
    dedup_key_cols: Optional[Sequence[str]],
) -> List[SafetyEvent]:
    events: List[SafetyEvent] = []
    require = cfg.require_append_dedup_for_env(rc.env)

    if require and (not dedup_key_cols or len([c for c in dedup_key_cols if str(c).strip() != ""]) == 0):
        ev = block(
            code="APPEND_DEDUP_REQUIRED",
            message="Append writer requires dedup_key_cols in this environment.",
            details={"env": rc.env},
        )
        events.append(ev)
        raise PreconditionFailed(ev.message)

    events.append(
        allow(
            code="APPEND_DEDUP_POLICY_OK",
            message="Append dedup policy satisfied.",
            details={"env": rc.env, "dedup_enabled": bool(dedup_key_cols)},
        )
    )
    return events
