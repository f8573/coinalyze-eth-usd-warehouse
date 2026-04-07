from __future__ import annotations

from importlib.resources import files


def load_schema_sql() -> str:
    return files("realtime_querydb.sql").joinpath("schema.sql").read_text(encoding="utf-8")
