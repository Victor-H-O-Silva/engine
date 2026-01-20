from __future__ import annotations

from typing import Dict, Optional, Sequence

from delta.tables import DeltaTable
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


class DeltaMergeWriter:
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
        key_cols: Sequence[str],
        partition_cols: Optional[Sequence[str]] = None,
        update_all_when_matched: bool = True,
        insert_all_when_not_matched: bool = True,
        soft_delete: bool = False,
        soft_delete_col: str = "_is_deleted",
    ) -> WriterResult:
        key_cols = [str(c).strip() for c in key_cols if str(c).strip() != ""]
        if not key_cols:
            raise PreconditionFailed("MERGE requires non-empty key_cols")

        _require_columns(df, key_cols, what="MERGE")
        partition_cols = _normalize_partition_cols(partition_cols)

        tags = _rc_tags(rc)
        staged = df
        for k, v in tags.items():
            staged = staged.withColumn(k, F.lit(v))

        safety_events = []

        if rc.dry_run:
            return WriterResult(
                strategy="MERGE",
                target=target.fqn,
                created_table=False,
                rows_written=None,
                rows_inserted=None,
                rows_updated=None,
                rows_deleted=None,
                output_metadata={
                    "keys": key_cols,
                    "partition_cols": list(partition_cols) if partition_cols else None,
                    "dry_run": True,
                    "safety_events": safety_events,
                },
            )

        created = _create_table_if_missing(
            self._spark,
            staged,
            target,
            partition_cols=partition_cols,
            table_properties=self._table_properties,
        )

        dt = DeltaTable.forName(self._spark, target.fqn)
        src = staged.alias("s")
        tgt = dt.alias("t")

        cond = " AND ".join([f"t.`{c}` <=> s.`{c}`" for c in key_cols])

        m = tgt.merge(src, cond)
        
        if soft_delete:
            if soft_delete_col not in staged.columns: raise PreconditionFailed(f"soft_delete=True requires column {soft_delete_col}")
            m = m.whenMatchedUpdateAll(condition=f"s.`{soft_delete_col}` = false")
            m = m.whenMatchedUpdate(
                condition=f"s.`{soft_delete_col}` = true",
                set={soft_delete_col: F.lit(True)},
            )
            if insert_all_when_not_matched:
                m = m.whenNotMatchedInsertAll(condition=f"s.`{soft_delete_col}` = false")
        else:
            if update_all_when_matched: m = m.whenMatchedUpdateAll()
            if insert_all_when_not_matched: m = m.whenNotMatchedInsertAll()

        m.execute()

        return WriterResult(
            strategy="MERGE",
            target=target.fqn,
            created_table=created,
            rows_written=None,
            rows_inserted=None,
            rows_updated=None,
            rows_deleted=None,
            output_metadata={
                "keys": key_cols,
                "partition_cols": list(partition_cols) if partition_cols else None,
                "dry_run": False,
                "safety_events": safety_events,
            },
        )
