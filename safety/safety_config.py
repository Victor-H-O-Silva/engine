from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass(frozen=True)
class SafetyConfig:
    require_replace_where_for_snapshot: bool = True
    allow_full_overwrite_in_dev: bool = True
    allow_full_overwrite_in_qa: bool = False
    allow_full_overwrite_in_prod: bool = False

    max_partitions_overwrite_dev: int = 10_000
    max_partitions_overwrite_qa: int = 1_000
    max_partitions_overwrite_prod: int = 100

    require_append_dedup_in_qa: bool = True
    require_append_dedup_in_prod: bool = True

    max_param_partitions_key: str = "max_partitions_overwrite"
    allow_full_overwrite_param_key: str = "allow_full_overwrite"
    allow_snapshot_without_replacewhere_param_key: str = "allow_snapshot_without_replacewhere"

    @staticmethod
    def default() -> "SafetyConfig":
        return SafetyConfig()

    def max_partitions_overwrite_for_env(self, env: str) -> int:
        e = (env or "").strip().lower()
        if e == "prod":
            return self.max_partitions_overwrite_prod
        if e == "qa":
            return self.max_partitions_overwrite_qa
        return self.max_partitions_overwrite_dev

    def full_overwrite_allowed_for_env(self, env: str) -> bool:
        e = (env or "").strip().lower()
        if e == "prod":
            return self.allow_full_overwrite_in_prod
        if e == "qa":
            return self.allow_full_overwrite_in_qa
        return self.allow_full_overwrite_in_dev

    def require_append_dedup_for_env(self, env: str) -> bool:
        e = (env or "").strip().lower()
        if e == "prod":
            return self.require_append_dedup_in_prod
        if e == "qa":
            return self.require_append_dedup_in_qa
        return False

    def get_param_bool(self, params: Optional[Dict], key: str, default: bool = False) -> bool:
        if not params or key not in params:
            return default
        v = params.get(key)
        if isinstance(v, bool):
            return v
        s = str(v).strip().lower()
        return s in ("1", "true", "yes", "y")

    def get_param_int(self, params: Optional[Dict], key: str, default: int) -> int:
        if not params or key not in params:
            return default
        v = params.get(key)
        if v is None or str(v).strip() == "":
            return default
        try:
            return int(str(v).strip())
        except Exception:
            return default
