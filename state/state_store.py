from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, Tuple, List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row
from pipeline_runtime.state.state_paths import StateTables


def _compact_json(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False, sort_keys=True)

@dataclass(frozen=True)
class PriorSuccess:
    run_id: str
    ended_at: Optional[datetime]
    status: str
    intent_hash: Optional[str]
    window_end_utc: Optional[str]

@dataclass(frozen=True)
class LockResult:
    acquired: bool
    owner_run_id: Optional[str]
    expires_at: Optional[datetime]


class StateStore:
    def __init__(
        self,
        spark: SparkSession,
        tables: StateTables,
        table_properties: Optional[Dict[str, str]] = None,
        *,
        allow_schema_create: bool = False,
    ) -> None:
        self.spark = spark
        self.tables = tables
        self.table_properties = table_properties or {}
        self.allow_schema_create = allow_schema_create

    def ensure_schema(self) -> None:
        if not self.allow_schema_create:
            return
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.tables.schema}")

    def ensure_tables(self) -> None:
        if self.allow_schema_create:
            self.ensure_schema()

        run_props = self._tblprops_sql(self.table_properties)
        wm_props = self._tblprops_sql(self.table_properties)
        lock_props = self._tblprops_sql(self.table_properties)

        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.tables.run_history} (
              pipeline_name STRING NOT NULL,
              run_id STRING NOT NULL,
              attempt INT,
              env STRING,
              mode STRING,
              framework_version STRING,
              trigger_ts TIMESTAMP,
              started_at TIMESTAMP,
              ended_at TIMESTAMP,
              status STRING,
              params_json STRING,
              metrics_json STRING,
              error_json STRING,
              actor STRING,
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
            USING DELTA
            {run_props}
            """
        )

        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.tables.watermarks} (
              pipeline_name STRING NOT NULL,
              watermark_name STRING NOT NULL,
              watermark_value STRING,
              watermark_ts TIMESTAMP,
              run_id STRING,
              updated_at TIMESTAMP
            )
            USING DELTA
            {wm_props}
            """
        )

        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.tables.locks} (
              pipeline_name STRING NOT NULL,
              lock_name STRING NOT NULL,
              owner_run_id STRING NOT NULL,
              acquired_at TIMESTAMP,
              heartbeat_at TIMESTAMP,
              expires_at TIMESTAMP
            )
            USING DELTA
            {lock_props}
            """
        )

    def record_run_start(
        self,
        pipeline_name: str,
        run_id: str,
        *,
        attempt: Optional[int] = None,
        env: Optional[str] = None,
        mode: Optional[str] = None,
        framework_version: Optional[str] = None,
        trigger_ts: Optional[datetime] = None,
        params: Optional[Dict[str, Any]] = None,
        actor: Optional[str] = None,
    ) -> None:
        self.ensure_tables()

        trigger_ts_lit = F.lit(trigger_ts).cast("timestamp") if trigger_ts else F.current_timestamp()
        params_json = _compact_json(params or {})

        src = (
            self.spark.createDataFrame(
                [
                    (
                        pipeline_name,
                        run_id,
                        attempt,
                        env,
                        mode,
                        framework_version,
                        None,
                        None,
                        "RUNNING",
                        params_json,
                        None,
                        None,
                        actor,
                    )
                ],
                """
                pipeline_name string, run_id string, attempt int, env string, mode string,
                framework_version string, started_at timestamp, ended_at timestamp, status string,
                params_json string, metrics_json string, error_json string, actor string
                """,
            )
            .withColumn("trigger_ts", trigger_ts_lit)
            .withColumn("started_at", F.current_timestamp())
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
        )

        src.createOrReplaceTempView("_src_run_start")

        self.spark.sql(
            f"""
            MERGE INTO {self.tables.run_history} t
            USING _src_run_start s
            ON t.pipeline_name = s.pipeline_name AND t.run_id = s.run_id
            WHEN MATCHED THEN UPDATE SET
              t.attempt = s.attempt,
              t.env = s.env,
              t.mode = s.mode,
              t.framework_version = s.framework_version,
              t.trigger_ts = s.trigger_ts,
              t.started_at = COALESCE(t.started_at, s.started_at),
              t.ended_at = s.ended_at,
              t.status = s.status,
              t.params_json = s.params_json,
              t.metrics_json = s.metrics_json,
              t.error_json = s.error_json,
              t.actor = s.actor,
              t.updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT (
              pipeline_name, run_id, attempt, env, mode, framework_version, trigger_ts,
              started_at, ended_at, status, params_json, metrics_json, error_json, actor,
              created_at, updated_at
            ) VALUES (
              s.pipeline_name, s.run_id, s.attempt, s.env, s.mode, s.framework_version, s.trigger_ts,
              s.started_at, s.ended_at, s.status, s.params_json, s.metrics_json, s.error_json, s.actor,
              s.created_at, s.updated_at
            )
            """
        )

    def record_run_end(
        self,
        pipeline_name: str,
        run_id: str,
        *,
        status: str,
        metrics: Optional[Dict[str, Any]] = None,
        error: Optional[Dict[str, Any]] = None,
        ended_at: Optional[datetime] = None,
    ) -> None:
        self.ensure_tables()

        ended_at_lit = F.lit(ended_at).cast("timestamp") if ended_at else F.current_timestamp()
        metrics_json = _compact_json(metrics or {}) if metrics is not None else None
        error_json = _compact_json(error or {}) if error is not None else None

        src = (
            self.spark.createDataFrame(
                [(pipeline_name, run_id, status, metrics_json, error_json)],
                "pipeline_name string, run_id string, status string, metrics_json string, error_json string",
            )
            .withColumn("ended_at", ended_at_lit)
            .withColumn("updated_at", F.current_timestamp())
        )

        src.createOrReplaceTempView("_src_run_end")

        self.spark.sql(
            f"""
            MERGE INTO {self.tables.run_history} t
            USING _src_run_end s
            ON t.pipeline_name = s.pipeline_name AND t.run_id = s.run_id
            WHEN MATCHED THEN UPDATE SET
              t.ended_at = s.ended_at,
              t.status = s.status,
              t.metrics_json = s.metrics_json,
              t.error_json = s.error_json,
              t.updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT (
              pipeline_name, run_id, status, metrics_json, error_json, ended_at, created_at, updated_at
            ) VALUES (
              s.pipeline_name, s.run_id, s.status, s.metrics_json, s.error_json, s.ended_at, s.updated_at, s.updated_at
            )
            """
        )

    def get_watermark(
        self, pipeline_name: str, watermark_name: str
    ) -> Optional[Tuple[Optional[str], Optional[datetime], Optional[str]]]:
        self.ensure_tables()

        df = (
            self.spark.table(self.tables.watermarks)
            .where((F.col("pipeline_name") == pipeline_name) & (F.col("watermark_name") == watermark_name))
            .orderBy(F.col("updated_at").desc())
            .select("watermark_value", "watermark_ts", "run_id")
            .limit(1)
        )

        rows = df.collect()
        if not rows:
            return None
        r = rows[0]
        return (r["watermark_value"], r["watermark_ts"], r["run_id"])

    def upsert_watermark(
        self,
        pipeline_name: str,
        watermark_name: str,
        *,
        watermark_value: Optional[str] = None,
        watermark_ts: Optional[datetime] = None,
        run_id: Optional[str] = None,
    ) -> None:
        self.ensure_tables()

        src = (
            self.spark.createDataFrame(
                [(pipeline_name, watermark_name, watermark_value, watermark_ts, run_id)],
                "pipeline_name string, watermark_name string, watermark_value string, watermark_ts timestamp, run_id string",
            )
            .withColumn("updated_at", F.current_timestamp())
        )

        src.createOrReplaceTempView("_src_watermark")

        self.spark.sql(
            f"""
            MERGE INTO {self.tables.watermarks} t
            USING _src_watermark s
            ON t.pipeline_name = s.pipeline_name AND t.watermark_name = s.watermark_name
            WHEN MATCHED THEN UPDATE SET
              t.watermark_value = s.watermark_value,
              t.watermark_ts = s.watermark_ts,
              t.run_id = s.run_id,
              t.updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT (
              pipeline_name, watermark_name, watermark_value, watermark_ts, run_id, updated_at
            ) VALUES (
              s.pipeline_name, s.watermark_name, s.watermark_value, s.watermark_ts, s.run_id, s.updated_at
            )
            """
        )

    def try_acquire_lock(
        self,
        pipeline_name: str,
        lock_name: str,
        owner_run_id: str,
        *,
        ttl_seconds: int = 3600,
    ) -> LockResult:
        self.ensure_tables()

        src = (
            self.spark.createDataFrame(
                [(pipeline_name, lock_name, owner_run_id, int(ttl_seconds))],
                "pipeline_name string, lock_name string, owner_run_id string, ttl_seconds int",
            )
            .withColumn("now_ts", F.current_timestamp())
            .withColumn("expires_at", (F.col("now_ts").cast("long") + F.col("ttl_seconds")).cast("timestamp"))
            .withColumn("acquired_at", F.col("now_ts"))
            .withColumn("heartbeat_at", F.col("now_ts"))
            .select("pipeline_name", "lock_name", "owner_run_id", "acquired_at", "heartbeat_at", "expires_at")
        )

        src.createOrReplaceTempView("_src_lock")

        self.spark.sql(
            f"""
            MERGE INTO {self.tables.locks} t
            USING _src_lock s
            ON t.pipeline_name = s.pipeline_name AND t.lock_name = s.lock_name
            WHEN MATCHED AND (t.expires_at IS NULL OR t.expires_at < current_timestamp() OR t.owner_run_id = s.owner_run_id)
              THEN UPDATE SET
                t.owner_run_id = s.owner_run_id,
                t.acquired_at = COALESCE(t.acquired_at, s.acquired_at),
                t.heartbeat_at = s.heartbeat_at,
                t.expires_at = s.expires_at
            WHEN NOT MATCHED THEN INSERT (
              pipeline_name, lock_name, owner_run_id, acquired_at, heartbeat_at, expires_at
            ) VALUES (
              s.pipeline_name, s.lock_name, s.owner_run_id, s.acquired_at, s.heartbeat_at, s.expires_at
            )
            """
        )

        row = (
            self.spark.table(self.tables.locks)
            .where((F.col("pipeline_name") == pipeline_name) & (F.col("lock_name") == lock_name))
            .select("owner_run_id", "expires_at")
            .limit(1)
            .collect()
        )

        if not row:
            return LockResult(acquired=False, owner_run_id=None, expires_at=None)

        owner = row[0]["owner_run_id"]
        expires = row[0]["expires_at"]
        return LockResult(acquired=(owner == owner_run_id), owner_run_id=owner, expires_at=expires)

    def heartbeat_lock(
        self,
        pipeline_name: str,
        lock_name: str,
        owner_run_id: str,
        *,
        ttl_seconds: int = 3600,
    ) -> bool:
        self.ensure_tables()

        src = (
            self.spark.createDataFrame(
                [(pipeline_name, lock_name, owner_run_id, int(ttl_seconds))],
                "pipeline_name string, lock_name string, owner_run_id string, ttl_seconds int",
            )
            .withColumn("now_ts", F.current_timestamp())
            .withColumn("expires_at", (F.col("now_ts").cast("long") + F.col("ttl_seconds")).cast("timestamp"))
            .withColumn("heartbeat_at", F.col("now_ts"))
            .select("pipeline_name", "lock_name", "owner_run_id", "heartbeat_at", "expires_at")
        )

        src.createOrReplaceTempView("_src_heartbeat")

        self.spark.sql(
            f"""
            MERGE INTO {self.tables.locks} t
            USING _src_heartbeat s
            ON t.pipeline_name = s.pipeline_name AND t.lock_name = s.lock_name AND t.owner_run_id = s.owner_run_id
            WHEN MATCHED THEN UPDATE SET
              t.heartbeat_at = s.heartbeat_at,
              t.expires_at = s.expires_at
            """
        )

        row = (
            self.spark.table(self.tables.locks)
            .where((F.col("pipeline_name") == pipeline_name) & (F.col("lock_name") == lock_name))
            .select("owner_run_id")
            .limit(1)
            .collect()
        )

        return bool(row) and row[0]["owner_run_id"] == owner_run_id

    def release_lock(self, pipeline_name: str, lock_name: str, owner_run_id: str) -> None:
        self.ensure_tables()
        self.spark.sql(
            f"""
            DELETE FROM {self.tables.locks}
            WHERE pipeline_name = {self._sql_str(pipeline_name)}
              AND lock_name = {self._sql_str(lock_name)}
              AND owner_run_id = {self._sql_str(owner_run_id)}
            """
        )

    def _tblprops_sql(self, props: Dict[str, str]) -> str:
        if not props:
            return ""
        items = ", ".join([f"'{k}'='{v}'" for k, v in props.items()])
        return f"TBLPROPERTIES ({items})"

    def _sql_str(self, s: str) -> str:
        return "'" + s.replace("'", "''") + "'"

    def get_last_success_by_intent(
        self,
        *,
        pipeline_name: str,
        env: Optional[str],
        intent_hash: str,
    ) -> Optional[PriorSuccess]:
        self.ensure_tables()

        ih = str(intent_hash).strip()
        if ih == "":
            return None

        df = (
            self.spark.table(self.tables.run_history)
            .where(F.col("pipeline_name") == pipeline_name)
            .where(F.col("status") == "SUCCESS")
            .where(F.col("metrics_json").isNotNull())
        )

        if env is not None and str(env).strip() != "":
            df = df.where(F.col("env") == str(env).strip())

        df = (
            df.withColumn("_intent_hash", F.get_json_object(F.col("metrics_json"), "$.intent_hash"))
            .withColumn("_window_end_utc", F.get_json_object(F.col("metrics_json"), "$.window_end_utc"))
            .where(F.col("_intent_hash") == ih)
            .orderBy(F.col("ended_at").desc_nulls_last(), F.col("updated_at").desc_nulls_last())
            .select("run_id", "ended_at", "status", "_intent_hash", "_window_end_utc")
            .limit(1)
        )

        rows: List[Row] = df.collect()
        if not rows:
            return None

        r = rows[0]
        return PriorSuccess(
            run_id=r["run_id"],
            ended_at=r["ended_at"],
            status=r["status"],
            intent_hash=r["_intent_hash"],
            window_end_utc=r["_window_end_utc"],
        )

    def watermark_committed_for_run(
        self,
        *,
        pipeline_name: str,
        watermark_name: str,
        watermark_value: str,
        run_id: str,
    ) -> bool:
        self.ensure_tables()

        wv = str(watermark_value).strip()
        rid = str(run_id).strip()
        if wv == "" or rid == "":
            return False

        df = (
            self.spark.table(self.tables.watermarks)
            .where(F.col("pipeline_name") == pipeline_name)
            .where(F.col("watermark_name") == watermark_name)
            .where(F.col("watermark_value") == wv)
            .where(F.col("run_id") == rid)
            .select("pipeline_name")
            .limit(1)
        )

        return df.count() > 0    
    
