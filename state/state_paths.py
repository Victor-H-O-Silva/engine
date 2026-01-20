from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True)
class StateTables:
    catalog: str
    schema: str
    run_history: str
    watermarks: str
    locks: str


def _split_schema(schema: str) -> Tuple[str, str]:
    s = (schema or "").strip()
    if s == "":
        raise ValueError("schema must be provided (either 'schema' or 'catalog.schema')")
    parts = [p.strip() for p in s.split(".") if p.strip() != ""]
    if len(parts) == 1:
        return "", parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    raise ValueError(f"Invalid schema '{schema}'. Expected 'schema' or 'catalog.schema'.")


def state_tables(
    catalog: str = "",
    schema: str = "",
) -> StateTables:
    schema = (schema or "").strip()
    catalog = (catalog or "").strip()

    cat_in, sch_in = _split_schema(schema)

    if catalog != "":
        catalog_in = catalog
        schema_in = sch_in
    else:
        catalog_in = cat_in
        schema_in = sch_in

    fq_schema = f"{catalog_in}.{schema_in}" if catalog_in else schema_in

    return StateTables(
        catalog=catalog_in,
        schema=fq_schema,
        run_history=f"{fq_schema}.run_history",
        watermarks=f"{fq_schema}.watermarks",
        locks=f"{fq_schema}.locks",
    )
