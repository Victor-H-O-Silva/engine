from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol, runtime_checkable

from pipeline_runtime.contracts.run_context import RunContext


@dataclass(frozen=True)
class PipelineOutput:
    row_count: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = None


@runtime_checkable
class Pipeline(Protocol):
    name: str

    def validate_input(self, rc: RunContext) -> None:
        ...

    def extract(self, rc: RunContext) -> Any:
        ...

    def transform(self, rc: RunContext, data: Any) -> Any:
        ...

    def load(self, rc: RunContext, data: Any) -> PipelineOutput:
        ...

    def validate_output(self, rc: RunContext, out: PipelineOutput) -> None:
        ...
