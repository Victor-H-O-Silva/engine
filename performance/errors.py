from __future__ import annotations


class PerformanceControlError(RuntimeError):
    pass


class SparkConfDeniedError(PerformanceControlError):
    def __init__(self, key: str):
        super().__init__(f"Spark conf key is denied by policy: {key}")
        self.key = key


class SparkConfInvalidValueError(PerformanceControlError):
    def __init__(self, key: str, value: str):
        super().__init__(f"Spark conf value is invalid for key={key}: {value}")
        self.key = key
        self.value = value


class ConcurrencyLimitExceededError(PerformanceControlError):
    def __init__(self, scope: str, scope_key: str, active: int, max_active: int):
        super().__init__(
            f"Concurrency limit exceeded for scope={scope} scope_key={scope_key}: active={active} max_active={max_active}"
        )
        self.scope = scope
        self.scope_key = scope_key
        self.active = active
        self.max_active = max_active


class WindowChunkingError(PerformanceControlError):
    pass