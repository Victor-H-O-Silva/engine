from __future__ import annotations

import json
import re
import uuid
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional, Tuple, Mapping

# Enum for supported execution modes
class ExecutionMode(str, Enum):
    INCREMENTAL = "incremental"
    BACKFILL = "backfill"
    SNAPSHOT = "snapshot"
    APPEND = "append"

# Enum for snapshot policy options
class SnapshotPolicy(str, Enum):
    ALLOWLIST_ONLY = "allowlist_only"

# Configuration for contract validation and defaults
@dataclass(frozen=True)
class ContractConfig:
    allowed_envs: Tuple[str, ...] = ("dev", "qa", "prod")
    log_timezone: str = "America/Sao_Paulo"
    snapshot_policy: SnapshotPolicy = SnapshotPolicy.ALLOWLIST_ONLY
    snapshot_name_token: str = "snap"
    pipeline_name_regex: str = r"^[a-z][a-z0-9_]{1,127}$"
    enforce_windows_for_all_modes: bool = True
    max_param_bytes: int = 200_000

# Returns current UTC datetime
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

# Ensures a datetime is in UTC
def _ensure_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)

# Parses an ISO timestamp string to a UTC datetime
def _parse_iso_ts(value: str) -> datetime:
    v = value.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    dt = datetime.fromisoformat(v)
    return _ensure_utc(dt)

# Serializes an object to compact JSON
def _compact_json(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False, sort_keys=True)

# Safely loads a JSON string, returning None for empty/None input
def _safe_load_json(s: str) -> Any:
    if s is None:
        return None
    t = s.strip()
    if t == "":
        return None
    return json.loads(t)

# Returns a SHA256 hash of a string
def _hash_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

# Canonicalizes pipeline names (lowercase, trimmed)
def _canonicalize_pipeline_name(name: str) -> str:
    return (name or "").strip().lower()

# Ensures orchestrator is set in trigger dict, defaulting to "databricks"
def _extract_orchestrator_defaults(trigger: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    t = trigger or {}
    out = dict(t)
    if "orchestrator" not in out or out["orchestrator"] is None or str(out["orchestrator"]).strip() == "":
        out["orchestrator"] = "databricks"
    return out

# Main context object for a pipeline run
@dataclass(frozen=True)
class RunContext:
    run_id: str
    attempt: int = 1
    run_ts_utc: datetime = field(default_factory=_now_utc)
    env: str = ""
    pipeline_name: str = ""
    mode: ExecutionMode = ExecutionMode.INCREMENTAL
    window_start_utc: datetime = field(default_factory=_now_utc)
    window_end_utc: datetime = field(default_factory=_now_utc)
    params: Dict[str, Any] = field(default_factory=dict)
    trigger: Dict[str, Any] = field(default_factory=dict)
    framework_version: str = "0.1.0-workspace"
    log_timezone: str = "America/Sao_Paulo"
    allow_destructive: bool = False
    dry_run: bool = False
    intent_hash: str = ""
    snapshot_policy: SnapshotPolicy = SnapshotPolicy.ALLOWLIST_ONLY
    snapshot_name_token: str = "snap"


    # Returns a normalized version of this context, filling defaults and computing intent_hash
    def normalized(self, cfg: ContractConfig) -> RunContext:
        run_id = (self.run_id or "").strip()
        attempt = int(self.attempt) if self.attempt is not None else 1
        if attempt < 1:
            attempt = 1
        run_ts_utc = _ensure_utc(self.run_ts_utc)
        env = (self.env or "").strip().lower()
        pipeline_name = _canonicalize_pipeline_name(self.pipeline_name)
        mode = ExecutionMode(self.mode)
        window_start_utc = _ensure_utc(self.window_start_utc)
        window_end_utc = _ensure_utc(self.window_end_utc)
        params = self.params or {}
        trigger = _extract_orchestrator_defaults(self.trigger)
        framework_version = (self.framework_version or "").strip()
        log_timezone = (self.log_timezone or cfg.log_timezone).strip()
        allow_destructive = bool(self.allow_destructive)
        dry_run = bool(self.dry_run)
        snapshot_policy = self.snapshot_policy or cfg.snapshot_policy
        snapshot_name_token = (self.snapshot_name_token or cfg.snapshot_name_token).strip().lower()

        tmp = RunContext(
            run_id=run_id,
            attempt=attempt,
            run_ts_utc=run_ts_utc,
            env=env,
            pipeline_name=pipeline_name,
            mode=mode,
            window_start_utc=window_start_utc,
            window_end_utc=window_end_utc,
            params=params,
            trigger=trigger,
            framework_version=framework_version,
            log_timezone=log_timezone,
            allow_destructive=allow_destructive,
            dry_run=dry_run,
            intent_hash="",
            snapshot_policy=snapshot_policy,
            snapshot_name_token=snapshot_name_token,
        )

        ih = tmp.compute_intent_hash()

        return RunContext(
            run_id=tmp.run_id,
            attempt=attempt,
            run_ts_utc=tmp.run_ts_utc,
            env=tmp.env,
            pipeline_name=tmp.pipeline_name,
            mode=tmp.mode,
            window_start_utc=tmp.window_start_utc,
            window_end_utc=tmp.window_end_utc,
            params=tmp.params,
            trigger=tmp.trigger,
            framework_version=tmp.framework_version,
            log_timezone=tmp.log_timezone,
            allow_destructive=tmp.allow_destructive,
            dry_run=tmp.dry_run,
            intent_hash=ih,
            snapshot_policy=tmp.snapshot_policy,
            snapshot_name_token=tmp.snapshot_name_token,
        )


    # Validates this context against the provided config, raising ValueError on issues
    def validate(self, cfg: ContractConfig) -> None:
        if self.run_id is None or str(self.run_id).strip() == "":
            raise ValueError("run_id is required")
        
        if not isinstance(self.attempt, int):
            raise ValueError("attempt must be an integer")

        if self.attempt < 1:
            raise ValueError("attempt must be >= 1")

        if self.env not in cfg.allowed_envs:
            raise ValueError(f"env must be one of {cfg.allowed_envs}, got '{self.env}'")

        if self.pipeline_name is None or self.pipeline_name.strip() == "":
            raise ValueError("pipeline_name is required")

        if re.match(cfg.pipeline_name_regex, self.pipeline_name) is None:
            raise ValueError(f"pipeline_name must match {cfg.pipeline_name_regex}, got '{self.pipeline_name}'")

        if self.framework_version is None or self.framework_version.strip() == "":
            raise ValueError("framework_version is required")

        if cfg.enforce_windows_for_all_modes:
            if self.window_start_utc is None or self.window_end_utc is None:
                raise ValueError("window_start_utc and window_end_utc are required for all modes")
        else:
            if self.mode in (ExecutionMode.INCREMENTAL, ExecutionMode.BACKFILL, ExecutionMode.APPEND):
                if self.window_start_utc is None or self.window_end_utc is None:
                    raise ValueError("window_start_utc and window_end_utc are required for this mode")

        if self.window_start_utc >= self.window_end_utc:
            raise ValueError("window_start_utc must be strictly less than window_end_utc")

        if self.mode == ExecutionMode.SNAPSHOT:
            if self.snapshot_policy == SnapshotPolicy.ALLOWLIST_ONLY:
                if self.snapshot_name_token not in self.pipeline_name.split("_"):
                    raise ValueError(
                        f"snapshot policy requires allowlist-by-name-token; pipeline_name must contain '{self.snapshot_name_token}'"
                    )
            if self.env == "prod" and not self.allow_destructive:
                raise ValueError("snapshot in prod requires allow_destructive=true")

        params_json = _compact_json(self.params or {})
        if len(params_json.encode("utf-8")) > cfg.max_param_bytes:
            raise ValueError(f"params_json exceeds max bytes {cfg.max_param_bytes}")

        trigger_json = _compact_json(self.trigger or {})
        if len(trigger_json.encode("utf-8")) > cfg.max_param_bytes:
            raise ValueError(f"trigger_json exceeds max bytes {cfg.max_param_bytes}")

        if self.intent_hash is None or self.intent_hash.strip() == "":
            raise ValueError("intent_hash must be computed and non-empty")

    # Computes a hash representing the "intent" of this run (for idempotency, etc)
    def compute_intent_hash(self) -> str:
        payload = {
            "env": self.env,
            "pipeline_name": self.pipeline_name,
            "mode": self.mode.value,
            "window_start_utc": self.window_start_utc.isoformat(),
            "window_end_utc": self.window_end_utc.isoformat(),
            "params": self.params or {},
            "framework_version": self.framework_version,
            "allow_destructive": bool(self.allow_destructive),
            "dry_run": bool(self.dry_run),
        }
        return _hash_str(_compact_json(payload))

    # Serializes this context to a dict (for JSON, etc)
    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "attempt": int(self.attempt),
            "run_ts_utc": _ensure_utc(self.run_ts_utc).isoformat(),
            "env": self.env,
            "pipeline_name": self.pipeline_name,
            "mode": self.mode.value if isinstance(self.mode, ExecutionMode) else str(self.mode),
            "window_start_utc": _ensure_utc(self.window_start_utc).isoformat(),
            "window_end_utc": _ensure_utc(self.window_end_utc).isoformat(),
            "framework_version": self.framework_version,
            "log_timezone": self.log_timezone,
            "allow_destructive": bool(self.allow_destructive),
            "dry_run": bool(self.dry_run),
            "intent_hash": self.intent_hash,
            "snapshot_policy": self.snapshot_policy.value if isinstance(self.snapshot_policy, SnapshotPolicy) else str(self.snapshot_policy),
            "snapshot_name_token": self.snapshot_name_token,
            "params_json": _compact_json(self.params or {}),
            "trigger_json": _compact_json(self.trigger or {}),
        }

    # Serializes this context to a compact JSON string
    def to_json(self) -> str:
        return _compact_json(self.to_dict())

# Builds a RunContext from a mapping of arguments and an optional config
def build_run_context(args: Mapping[str, Any], cfg: Optional[ContractConfig] = None) -> RunContext:
    cfg = cfg or ContractConfig()
    a = {str(k): v for k, v in (args or {}).items()}

    run_id = str(a.get("run_id") or "").strip()
    if run_id == "":
        run_id = str(uuid.uuid4())

    attempt_raw = a.get("attempt")
    if attempt_raw is None or str(attempt_raw).strip() == "":
        attempt_raw = a.get("run_attempt")
    if attempt_raw is None or str(attempt_raw).strip() == "":
        attempt_raw = a.get("attempt_id")
    attempt = 1
    if attempt_raw is not None and str(attempt_raw).strip() != "":
        attempt = int(str(attempt_raw).strip())
    if attempt < 1:
        attempt = 1

    env = str(a.get("env") or "").strip().lower()
    pipeline_name = _canonicalize_pipeline_name(str(a.get("pipeline_name") or ""))

    mode_raw = str(a.get("mode") or "").strip().lower()
    if mode_raw == "":
        mode_raw = ExecutionMode.INCREMENTAL.value
    mode = ExecutionMode(mode_raw)

    run_ts_raw = a.get("run_ts_utc")
    run_ts_utc = _parse_iso_ts(str(run_ts_raw)) if run_ts_raw not in (None, "") else _now_utc()

    ws_raw = a.get("window_start_utc")
    we_raw = a.get("window_end_utc")
    if ws_raw is None or str(ws_raw).strip() == "":
        raise ValueError("window_start_utc is required")
    if we_raw is None or str(we_raw).strip() == "":
        raise ValueError("window_end_utc is required")
    window_start_utc = _parse_iso_ts(str(ws_raw))
    window_end_utc = _parse_iso_ts(str(we_raw))

    params_raw = a.get("params_json")
    params = _safe_load_json(str(params_raw)) if params_raw not in (None, "") else {}
    if params is None:
        params = {}
    if not isinstance(params, dict):
        raise ValueError("params_json must decode to a JSON object")

    trigger_raw = a.get("trigger_json")
    trigger = _safe_load_json(str(trigger_raw)) if trigger_raw not in (None, "") else {}
    if trigger is None:
        trigger = {}
    if not isinstance(trigger, dict):
        raise ValueError("trigger_json must decode to a JSON object")

    framework_version = str(a.get("framework_version") or "0.1.0-workspace").strip()
    log_timezone = str(a.get("log_timezone") or cfg.log_timezone).strip()

    allow_destructive_raw = a.get("allow_destructive")
    dry_run_raw = a.get("dry_run")
    allow_destructive = str(allow_destructive_raw).strip().lower() in ("1", "true", "yes", "y") if allow_destructive_raw is not None else False
    dry_run = str(dry_run_raw).strip().lower() in ("1", "true", "yes", "y") if dry_run_raw is not None else False

    snapshot_policy = cfg.snapshot_policy
    snapshot_name_token = cfg.snapshot_name_token

    rc = RunContext(
        run_id=run_id,
        attempt=attempt,
        run_ts_utc=run_ts_utc,
        env=env,
        pipeline_name=pipeline_name,
        mode=mode,
        window_start_utc=window_start_utc,
        window_end_utc=window_end_utc,
        params=params,
        trigger=trigger,
        framework_version=framework_version,
        log_timezone=log_timezone,
        allow_destructive=allow_destructive,
        dry_run=dry_run,
        intent_hash="",
        snapshot_policy=snapshot_policy,
        snapshot_name_token=snapshot_name_token,
    ).normalized(cfg)

    rc.validate(cfg)
    return rc

# Reads widget values for the given keys, returning a dict of key->value
def get_dbutils_widget_args(keys: Tuple[str, ...]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k in keys:
        try:
            out[k] = dbutils.widgets.get(k)
        except Exception:
            pass
    return out

# Builds a RunContext from Databricks widgets and an optional config
def build_run_context_from_widgets(cfg: Optional[ContractConfig] = None) -> RunContext:
    keys = (
        "run_id",
        "run_ts_utc",
        "env",
        "pipeline_name",
        "mode",
        "window_start_utc",
        "window_end_utc",
        "params_json",
        "trigger_json",
        "framework_version",
        "log_timezone",
        "allow_destructive",
        "dry_run",
    )
    args = get_dbutils_widget_args(keys)
    return build_run_context(args, cfg=cfg)