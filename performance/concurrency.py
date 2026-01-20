from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

from pipeline_runtime.performance.errors import ConcurrencyLimitExceededError


@dataclass(frozen=True)
class ConcurrencyState:
    scope: str
    scope_key: str
    active: int
    max_active: int


class ConcurrencyGuard:
    def __init__(self, spark, run_history_table_fqn: str):
        self._spark = spark
        self._run_history = run_history_table_fqn

    def _count_active(self, scope: str, scope_key: str, active_statuses: Tuple[str, ...]) -> int:
        statuses = ",".join([f"'{s}'" for s in active_statuses])
        q = f"""
        SELECT COUNT(1) AS c
        FROM {self._run_history}
        WHERE concurrency_scope = '{scope}'
          AND concurrency_scope_key = '{scope_key}'
          AND status IN ({statuses})
        """
        row = self._spark.sql(q).collect()[0]
        return int(row["c"])

    def enforce(
        self,
        scope: str,
        scope_key: str,
        max_active: int,
        active_statuses: Tuple[str, ...],
    ) -> ConcurrencyState:
        if max_active <= 0:
            return ConcurrencyState(scope=scope, scope_key=scope_key, active=0, max_active=max_active)

        active = self._count_active(scope=scope, scope_key=scope_key, active_statuses=active_statuses)
        if active >= max_active:
            raise ConcurrencyLimitExceededError(scope=scope, scope_key=scope_key, active=active, max_active=max_active)
        return ConcurrencyState(scope=scope, scope_key=scope_key, active=active, max_active=max_active)