from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    database_url: str
    coinalyze_api_key: str
    max_symbols_per_request: int = 10
    min_request_interval_seconds: float = 2.0
    backoff_jitter_ratio: float = 0.10
    probe_chunk_days: int = 30
    backfill_chunk_days: int = 30
    active_window_probe_days: int = 14
    max_candidate_markets_per_run: int = 1
    max_backfill_chunks_per_market_per_run: int = 1

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv(dotenv_path=Path.cwd() / ".env", override=False)
        database_url = _required_env("DATABASE_URL")
        api_key = _required_env("COINALYZE_API_KEY")
        return cls(
            database_url=database_url,
            coinalyze_api_key=api_key,
            max_symbols_per_request=int(os.getenv("MAX_SYMBOLS_PER_REQUEST", "10")),
            min_request_interval_seconds=float(os.getenv("MIN_REQUEST_INTERVAL_SECONDS", "2.0")),
            backoff_jitter_ratio=float(os.getenv("BACKOFF_JITTER_RATIO", "0.10")),
            probe_chunk_days=int(os.getenv("PROBE_CHUNK_DAYS", "30")),
            backfill_chunk_days=int(os.getenv("BACKFILL_CHUNK_DAYS", "30")),
            active_window_probe_days=int(os.getenv("ACTIVE_WINDOW_PROBE_DAYS", "14")),
            max_candidate_markets_per_run=int(os.getenv("MAX_CANDIDATE_MARKETS_PER_RUN", "1")),
            max_backfill_chunks_per_market_per_run=int(os.getenv("MAX_BACKFILL_CHUNKS_PER_MARKET_PER_RUN", "1")),
        )


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if value:
        return value
    raise RuntimeError(f"Missing required environment variable {name}. Set it in the environment or in .env.")
