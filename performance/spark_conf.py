from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Tuple

from pipeline_runtime.performance.errors import SparkConfDeniedError, SparkConfInvalidValueError


@dataclass(frozen=True)
class SparkConfSnapshot:
    previous: Dict[str, Optional[str]]


class SparkConfManager:
    def __init__(
        self,
        spark,
        allow_keys_prefixes: Tuple[str, ...],
        deny_keys_prefixes: Tuple[str, ...],
        deny_exact_keys: Tuple[str, ...],
    ):
        self._spark = spark
        self._allow_prefixes = allow_keys_prefixes
        self._deny_prefixes = deny_keys_prefixes
        self._deny_exact = set(deny_exact_keys)

    def _is_allowed(self, key: str) -> bool:
        if key in self._deny_exact:
            return False
        for p in self._deny_prefixes:
            if key.startswith(p):
                return False
        if not self._allow_prefixes:
            return True
        for p in self._allow_prefixes:
            if key.startswith(p):
                return True
        return False

    def snapshot(self, keys: Iterable[str]) -> SparkConfSnapshot:
        prev: Dict[str, Optional[str]] = {}
        for k in keys:
            try:
                prev[k] = self._spark.conf.get(k)
            except Exception:
                prev[k] = None
        return SparkConfSnapshot(previous=prev)

    def apply(self, conf: Dict[str, str]) -> SparkConfSnapshot:
        for k, v in conf.items():
            if not self._is_allowed(k):
                raise SparkConfDeniedError(k)
            if v is None:
                raise SparkConfInvalidValueError(k, "None")
            if not isinstance(v, str):
                raise SparkConfInvalidValueError(k, str(v))

        snap = self.snapshot(conf.keys())

        for k, v in conf.items():
            self._spark.conf.set(k, v)

        return snap

    def restore(self, snapshot: SparkConfSnapshot) -> None:
        for k, prev in snapshot.previous.items():
            if prev is None:
                try:
                    self._spark.conf.unset(k)
                except Exception:
                    pass
            else:
                self._spark.conf.set(k, prev)