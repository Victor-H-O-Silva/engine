from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TableRef:
    catalog: str
    schema: str
    table: str

    @property
    def fqn(self) -> str:
        c = self.catalog.strip()
        s = self.schema.strip()
        t = self.table.strip()
        if c == "" or s == "" or t == "":
            raise ValueError("TableRef requires non-empty catalog, schema, table")
        return f"{c}.{s}.{t}"
