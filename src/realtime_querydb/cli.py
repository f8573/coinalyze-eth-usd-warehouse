from __future__ import annotations

import argparse

from .coinalyze import CoinalyzeClient
from .config import Settings
from .repository import WarehouseRepository
from .service import WarehouseService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ETH-USD Coinalyze warehouse CLI")
    parser.add_argument(
        "command",
        choices=("init-db", "discover", "evaluate", "backfill", "backfill-pending", "extend-active", "refresh", "sync-all", "sync-daily", "sync-live"),
        help="Command to execute.",
    )
    return parser


def create_service() -> WarehouseService:
    settings = Settings.from_env()
    client = CoinalyzeClient(
        settings.coinalyze_api_key,
        min_request_interval_seconds=settings.min_request_interval_seconds,
        max_symbols_per_request=settings.max_symbols_per_request,
        backoff_jitter_ratio=settings.backoff_jitter_ratio,
    )
    repository = WarehouseRepository(settings.database_url)
    return WarehouseService(client=client, repository=repository, settings=settings)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    service = create_service()
    command = args.command
    if command == "init-db":
        service.init_db()
    elif command == "discover":
        service.discover_markets()
    elif command == "evaluate":
        service.evaluate_candidates()
    elif command == "backfill":
        service.backfill_active_markets()
    elif command == "backfill-pending":
        service.backfill_pending_active_markets()
    elif command == "extend-active":
        service.extend_active_markets()
    elif command == "refresh":
        service.refresh_active_markets()
    elif command == "sync-all":
        service.sync_all()
    elif command == "sync-daily":
        service.sync_daily()
    elif command == "sync-live":
        service.run_live_forever()
    else:
        parser.error(f"Unsupported command: {command}")
