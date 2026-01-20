from __future__ import annotations

from typing import Dict, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline_runtime.contracts.run_context import RunContext, ExecutionMode, SnapshotPolicy
from pipeline_runtime.writers.base import (
    _create_table_if_missing,
    _normalize_partition_cols,
    _rc_tags,
)
from pipeline_runtime.writers.table_ref import TableRef
from pipeline_runtime.writers.writer_errors import DestructiveOperationNotAllowed, PreconditionFailed
from pipeline_runtime.writers.writer_result import WriterResult


class DeltaSnapshotWriter:
    def __init__(
        self,
        *,
        spark: SparkSession,
        table_properties: Optional[Dict[str, str]] = None,
    ) -> None:
        self._spark = spark
        self._table_properties = table_properties or {}

    def write(
        self,
        *,
        rc: RunContext,
        df: DataFrame,
        target: TableRef,
        partition_cols: Optional[Sequence[str]] = None,
        replace_where: Optional[str] = None,
        require_snapshot_token: bool = True,
    ) -> WriterResult:
        if not rc.allow_destructive:
            raise DestructiveOperationNotAllowed("SNAPSHOT requires allow_destructive=true")

        if require_snapshot_token:
            if rc.mode != ExecutionMode.SNAPSHOT:
                raise PreconditionFailed("SNAPSHOT writer requires rc.mode == snapshot")

            if rc.snapshot_policy == SnapshotPolicy.ALLOWLIST_ONLY:
                token = (rc.snapshot_name_token or "").strip().lower()
                parts = [p.strip().lower() for p in (rc.pipeline_name or "").split("_") if p.strip() != ""]
                if token == "" or token not in parts:
                    raise PreconditionFailed(
                        f"snapshot policy requires allowlist-by-name-token; pipeline_name must contain '{token}'"
                    )

        partition_cols = _normalize_partition_cols(partition_cols)

        tags = _rc_tags(rc)
        staged = df
        for k, v in tags.items():
            staged = staged.withColumn(k, F.lit(v))

        if rc.dry_run:
            return WriterResult(
                strategy="SNAPSHOT",
                target=target.fqn,
                created_table=False,
                rows_written=None,
                rows_inserted=None,
                rows_updated=None,
                rows_deleted=None,
                output_metadata={
                    "replace_where": replace_where,
                    "partition_cols": list(partition_cols) if partition_cols else None,
                    "dry_run": True,
                },
            )
            
        created = _create_table_if_missing(
            self._spark,
            staged,
            target,
            partition_cols=partition_cols,
            table_properties=self._table_properties,
        )

        if created:
            return WriterResult(
                strategy="SNAPSHOT",
                target=target.fqn,
                created_table=True,
                rows_written=None,
                rows_inserted=None,
                rows_updated=None,
                rows_deleted=None,
                output_metadata={
                    "created": True,
                    "replace_where": replace_where,
                    "partition_cols": list(partition_cols) if partition_cols else None,
                },
            )

        w = staged.write.format("delta").mode("overwrite")

        if replace_where:
            w = w.option("replaceWhere", replace_where)

        w.saveAsTable(target.fqn)

        return WriterResult(
            strategy="SNAPSHOT",
            target=target.fqn,
            created_table=False,
            rows_written=None,
            rows_inserted=None,
            rows_updated=None,
            rows_deleted=None,
            output_metadata={
                "replace_where": replace_where,
                "partition_cols": list(partition_cols) if partition_cols else None,
            },
        )
