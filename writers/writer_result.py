from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class WriterResult:
    strategy: str
    target: str
    created_table: bool
    rows_written: Optional[int]
    rows_inserted: Optional[int]
    rows_updated: Optional[int]
    rows_deleted: Optional[int]
    output_metadata: Dict[str, Any]
