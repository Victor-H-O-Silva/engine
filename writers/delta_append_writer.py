from __future__ import annotations

from typing import Dict, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline_runtime.contracts.run_context import RunContext
from pipeline_runtime.writers.base import (
    _create_table_if_missing,
    _normalize_partition_cols,
    _rc_tags,
    _require_columns,
)
from pipeline_runtime.writers.table_ref import TableRef
from pipeline_runtime.writers.writer_errors import PreconditionFailed
from pipeline_runtime.writers.writer_result import WriterResult


class DeltaAppendWriter:
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
        dedup_key_cols: Optional[Sequence[str]] = None,
        dedup_predicate_sql: Optional[str] = None,
    ) -> WriterResult:
        partition_cols = _normalize_partition_cols(partition_cols)
        dedup_key_cols = None if dedup_key_cols is None else [str(c).strip() for c in dedup_key_cols if str(c).strip() != ""]

        tags = _rc_tags(rc)
        staged = df
        for k, v in tags.items():
            staged = staged.withColumn(k, F.lit(v))

        if rc.dry_run:
            return WriterResult(
                strategy="APPEND",
                target=target.fqn,
                created_table=False,
                rows_written=None,
                rows_inserted=None,
                rows_updated=None,
                rows_deleted=None,
                output_metadata={
                    "partition_cols": list(partition_cols) if partition_cols else None,
                    "dedup_key_cols": list(dedup_key_cols) if dedup_key_cols else None,
                    "dedup_predicate_sql": dedup_predicate_sql,
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

        to_write = staged

        if dedup_key_cols:
            _require_columns(staged, dedup_key_cols, what="APPEND dedup")
            existing = self._spark.table(target.fqn)
            if dedup_predicate_sql:
                existing = existing.where(dedup_predicate_sql)

            e = existing.select(*[F.col(c).alias(c) for c in dedup_key_cols]).dropDuplicates()
            s = staged.select(*[F.col(c).alias(c) for c in dedup_key_cols]).dropDuplicates()

            new_keys = s.join(e, on=dedup_key_cols, how="left_anti")
            to_write = staged.join(new_keys, on=dedup_key_cols, how="inner")

        to_write.write.format("delta").mode("append").saveAsTable(target.fqn)

        return WriterResult(
            strategy="APPEND",
            target=target.fqn,
            created_table=created,
            rows_written=None,
            rows_inserted=None,
            rows_updated=None,
            rows_deleted=None,
            output_metadata={
                "partition_cols": list(partition_cols) if partition_cols else None,
                "dedup_key_cols": list(dedup_key_cols) if dedup_key_cols else None,
                "dedup_predicate_sql": dedup_predicate_sql,
            },
        )
