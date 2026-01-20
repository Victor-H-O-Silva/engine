from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple


@dataclass(frozen=True)
class SparkConfPolicy:
    conf: Dict[str, str]
    allow_keys_prefixes: Tuple[str, ...]
    deny_keys_prefixes: Tuple[str, ...]
    deny_exact_keys: Tuple[str, ...]


@dataclass(frozen=True)
class ConcurrencyPolicy:
    enabled: bool
    max_active: int
    scope: str
    scope_key: Optional[str]
    active_statuses: Tuple[str, ...]


@dataclass(frozen=True)
class WindowChunkingPolicy:
    enabled: bool
    unit: str
    size: int
    align_to_unit_boundary: bool


@dataclass(frozen=True)
class PerformanceControls:
    spark_conf: SparkConfPolicy
    concurrency: ConcurrencyPolicy
    window_chunking: WindowChunkingPolicy


@dataclass(frozen=True)
class PerformanceDecision:
    applied_spark_conf: Dict[str, str]
    concurrency_scope: Optional[str]
    concurrency_scope_key: Optional[str]
    concurrency_active_count: Optional[int]
    concurrency_max_active: Optional[int]
    chunk_count: int
    chunk_unit: Optional[str]
    chunk_size: Optional[int]
    diagnostics: Dict[str, Any]