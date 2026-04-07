from __future__ import annotations

import sys
import unittest
from dataclasses import replace
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from realtime_querydb.config import Settings
from realtime_querydb.models import ActivationStatus, Granularity, Market, MarketType, MetricName, UTC
from realtime_querydb.service import (
    WarehouseService,
    build_future_markets,
    build_spot_markets,
    filter_windowed_timestamps,
    is_supported_quote_asset,
)


class FakeRepository:
    def __init__(self) -> None:
        self.providers = []
        self.markets = []
        self.activation_updates = []
        self.sync_updates = []
        self.ohlcv = []
        self.oi = []
        self.funding = []
        self.sync_states = {}

    def apply_schema(self) -> None:
        return None

    def upsert_providers(self, providers) -> None:
        self.providers.extend(providers)

    def upsert_markets(self, markets) -> None:
        self.markets = list(markets)

    def list_markets(self, status=None):
        if status is None:
            return list(self.markets)
        return [market for market in self.markets if market.activation_status is status]

    def set_market_activation(self, **kwargs) -> None:
        self.activation_updates.append(kwargs)
        for index, market in enumerate(self.markets):
            if market.market_id == kwargs["market_id"]:
                self.markets[index] = market.__class__(
                    market_id=market.market_id,
                    coinalyze_symbol=market.coinalyze_symbol,
                    provider_code=market.provider_code,
                    market_type=market.market_type,
                    base_asset=market.base_asset,
                    quote_asset=market.quote_asset,
                    symbol_on_exchange=market.symbol_on_exchange,
                    is_perpetual=market.is_perpetual,
                    margined=market.margined,
                    oi_unit=market.oi_unit,
                    has_buy_sell_data=market.has_buy_sell_data,
                    has_ohlcv_data=market.has_ohlcv_data,
                    required_granularities=market.required_granularities,
                    activation_status=kwargs["status"],
                    eligibility_checked_at=kwargs["checked_at"],
                    shared_complete_from=kwargs["shared_from"],
                    shared_complete_to=kwargs["shared_to"],
                    exclusion_reason=kwargs["exclusion_reason"],
                )
                break

    def upsert_sync_state(self, **kwargs) -> None:
        self.sync_updates.append(kwargs)
        self.sync_states[(kwargs["market_id"], kwargs["granularity"], kwargs["metric_name"])] = kwargs

    def list_sync_states(self, market_id) -> dict:
        return {
            (granularity, metric_name): SimpleSyncState(values)
            for (state_market_id, granularity, metric_name), values in self.sync_states.items()
            if state_market_id == market_id
        }

    def upsert_ohlcv_bars(self, **kwargs) -> None:
        if not kwargs["bars"]:
            return
        self.ohlcv.append(kwargs)

    def upsert_open_interest_bars(self, **kwargs) -> None:
        if not kwargs["bars"]:
            return
        self.oi.append(kwargs)

    def upsert_funding_rate_bars(self, **kwargs) -> None:
        if not kwargs["bars"]:
            return
        self.funding.append(kwargs)


class FakeClient:
    def __init__(self, history_by_key):
        self.history_by_key = history_by_key
        self.max_symbols_per_request = 10

    def get_exchanges(self):
        return []

    def get_future_markets(self):
        return []

    def get_spot_markets(self):
        return []

    def get_history(self, *, symbols, metric_name, granularity, start, end_inclusive, on_retry_after=None):
        start_ts = int(start.timestamp())
        end_ts = int(end_inclusive.timestamp())
        return {
            symbol: [
                item
                for item in self.history_by_key.get((symbol, metric_name, granularity), [])
                if start_ts <= int(item["t"]) <= end_ts
            ]
            for symbol in symbols
        }


class SimpleSyncState:
    def __init__(self, values) -> None:
        self.market_id = values["market_id"]
        self.granularity = values["granularity"]
        self.metric_name = values["metric_name"]
        self.oldest_backfilled_bucket = values["oldest_backfilled_bucket"]
        self.newest_loaded_bucket = values["newest_loaded_bucket"]
        self.retry_after_until = values["retry_after_until"]
        self.last_attempt_at = values["last_attempt_at"]
        self.last_error = values["last_error"]


def make_history(start: datetime, count: int, granularity: Granularity, gap_indexes: set[int] | None = None):
    history = []
    current = start
    skipped = gap_indexes or set()
    for index in range(count):
        if index not in skipped:
            history.append(
                {
                    "t": int(current.timestamp()),
                    "o": 1,
                    "h": 1,
                    "l": 1,
                    "c": 1,
                    "v": 1,
                    "bv": 1,
                    "tx": 1,
                    "btx": 1,
                }
            )
        current += granularity.delta
    return history


class ServiceTests(unittest.TestCase):
    def test_supported_quote_asset_filter(self) -> None:
        self.assertTrue(is_supported_quote_asset("USD"))
        self.assertTrue(is_supported_quote_asset("USD_UM"))
        self.assertTrue(is_supported_quote_asset("USD1"))
        self.assertFalse(is_supported_quote_asset("USDT"))
        self.assertFalse(is_supported_quote_asset("USDC"))
        self.assertFalse(is_supported_quote_asset("EUR"))

    def test_market_builders_filter_eth_usd_scope(self) -> None:
        future_markets = build_future_markets(
            [
                {
                    "symbol": "ETHUSD_PERP.A",
                    "exchange": "A",
                    "symbol_on_exchange": "ETHUSD_PERP",
                    "base_asset": "ETH",
                    "quote_asset": "USD",
                    "is_perpetual": True,
                    "margined": "COIN",
                    "oi_lq_vol_denominated_in": "QUOTE_ASSET",
                    "has_buy_sell_data": True,
                    "has_ohlcv_data": True,
                },
                {
                    "symbol": "ETHUSDT_PERP.A",
                    "exchange": "A",
                    "symbol_on_exchange": "ETHUSDT",
                    "base_asset": "ETH",
                    "quote_asset": "USDT",
                    "is_perpetual": True,
                    "margined": "STABLE",
                    "oi_lq_vol_denominated_in": "BASE_ASSET",
                    "has_buy_sell_data": True,
                    "has_ohlcv_data": True,
                },
            ]
        )
        spot_markets = build_spot_markets(
            [
                {
                    "symbol": "ETHUSD.C",
                    "exchange": "C",
                    "symbol_on_exchange": "ETH-USD",
                    "base_asset": "ETH",
                    "quote_asset": "USD",
                    "has_buy_sell_data": True,
                },
                {
                    "symbol": "ETHUSDC.C",
                    "exchange": "C",
                    "symbol_on_exchange": "ETH-USDC",
                    "base_asset": "ETH",
                    "quote_asset": "USDC",
                    "has_buy_sell_data": True,
                },
            ]
        )

        self.assertEqual([market.coinalyze_symbol for market in future_markets], ["ETHUSD_PERP.A"])
        self.assertEqual([market.coinalyze_symbol for market in spot_markets], ["ETHUSD.C"])

    def test_evaluate_market_uses_shared_overlap_across_metrics_and_granularities(self) -> None:
        market = Market(
            market_id=1,
            coinalyze_symbol="ETHUSD_PERP.A",
            provider_code="A",
            market_type=MarketType.PERP,
            base_asset="ETH",
            quote_asset="USD",
            symbol_on_exchange="ETHUSD_PERP",
            is_perpetual=True,
            margined="COIN",
            oi_unit="QUOTE_ASSET",
            has_buy_sell_data=True,
            has_ohlcv_data=True,
        )
        start_15m = datetime(2026, 4, 1, 0, 15, tzinfo=UTC)
        start_1h = datetime(2026, 4, 1, 0, 0, tzinfo=UTC)
        start_1d = datetime(2026, 4, 2, 0, 0, tzinfo=UTC)
        history = {}
        for metric in market.required_metrics:
            history[(market.coinalyze_symbol, metric, Granularity.FIFTEEN_MIN)] = make_history(start_15m, 400, Granularity.FIFTEEN_MIN)
            history[(market.coinalyze_symbol, metric, Granularity.ONE_HOUR)] = make_history(start_1h, 120, Granularity.ONE_HOUR)
            history[(market.coinalyze_symbol, metric, Granularity.DAILY)] = make_history(start_1d, 3, Granularity.DAILY)
        repository = FakeRepository()
        client = FakeClient(history)
        service = WarehouseService(
            client=client,
            repository=repository,
            settings=Settings(database_url="postgresql://unused", coinalyze_api_key="unused"),
            now=lambda: datetime(2026, 4, 10, tzinfo=UTC),
        )

        evaluated = service.evaluate_market(market)

        self.assertEqual(evaluated.activation_status, ActivationStatus.ACTIVE)
        self.assertEqual(evaluated.shared_complete_from, datetime(2026, 4, 2, 0, 0, tzinfo=UTC))
        self.assertEqual(evaluated.shared_complete_to, datetime(2026, 4, 5, 0, 0, tzinfo=UTC))

    def test_evaluate_market_excludes_when_granularity_is_missing(self) -> None:
        market = Market(
            market_id=2,
            coinalyze_symbol="ETHUSD.C",
            provider_code="C",
            market_type=MarketType.SPOT,
            base_asset="ETH",
            quote_asset="USD",
            symbol_on_exchange="ETH-USD",
            is_perpetual=False,
            margined=None,
            oi_unit=None,
            has_buy_sell_data=True,
            has_ohlcv_data=True,
        )
        history = {
            (market.coinalyze_symbol, MetricName.OHLCV, Granularity.FIFTEEN_MIN): make_history(
                datetime(2026, 4, 1, 0, 0, tzinfo=UTC), 10, Granularity.FIFTEEN_MIN
            ),
            (market.coinalyze_symbol, MetricName.OHLCV, Granularity.ONE_HOUR): make_history(
                datetime(2026, 4, 1, 0, 0, tzinfo=UTC), 10, Granularity.ONE_HOUR
            ),
        }
        repository = FakeRepository()
        client = FakeClient(history)
        service = WarehouseService(
            client=client,
            repository=repository,
            settings=Settings(database_url="postgresql://unused", coinalyze_api_key="unused"),
            now=lambda: datetime(2026, 4, 10, tzinfo=UTC),
        )

        evaluated = service.evaluate_market(market)

        self.assertEqual(evaluated.activation_status, ActivationStatus.EXCLUDED)
        self.assertEqual(evaluated.exclusion_reason, "missing_ohlcv_daily")

    def test_evaluate_market_uses_contiguous_suffix_instead_of_sparse_min_max_range(self) -> None:
        market = Market(
            market_id=3,
            coinalyze_symbol="ETHUSD.C",
            provider_code="C",
            market_type=MarketType.SPOT,
            base_asset="ETH",
            quote_asset="USD",
            symbol_on_exchange="ETH-USD",
            is_perpetual=False,
            margined=None,
            oi_unit=None,
            has_buy_sell_data=True,
            has_ohlcv_data=True,
        )
        history = {
            (market.coinalyze_symbol, MetricName.OHLCV, Granularity.FIFTEEN_MIN): make_history(
                datetime(2026, 4, 1, 0, 0, tzinfo=UTC),
                5 * 24 * 4,
                Granularity.FIFTEEN_MIN,
            ),
            (market.coinalyze_symbol, MetricName.OHLCV, Granularity.ONE_HOUR): make_history(
                datetime(2026, 4, 1, 0, 0, tzinfo=UTC),
                5 * 24,
                Granularity.ONE_HOUR,
            ),
            (market.coinalyze_symbol, MetricName.OHLCV, Granularity.DAILY): make_history(
                datetime(2026, 4, 1, 0, 0, tzinfo=UTC),
                5,
                Granularity.DAILY,
                gap_indexes={2},
            ),
        }
        repository = FakeRepository()
        client = FakeClient(history)
        service = WarehouseService(
            client=client,
            repository=repository,
            settings=Settings(database_url="postgresql://unused", coinalyze_api_key="unused"),
            now=lambda: datetime(2026, 4, 6, tzinfo=UTC),
        )

        evaluated = service.evaluate_market(market)

        self.assertEqual(evaluated.activation_status, ActivationStatus.ACTIVE)
        self.assertEqual(evaluated.shared_complete_from, datetime(2026, 4, 4, 0, 0, tzinfo=UTC))
        self.assertEqual(evaluated.shared_complete_to, datetime(2026, 4, 6, 0, 0, tzinfo=UTC))

    def test_filter_windowed_timestamps_enforces_full_bucket_containment(self) -> None:
        timestamps = [
            datetime(2026, 4, 1, 0, 0, tzinfo=UTC),
            datetime(2026, 4, 1, 1, 0, tzinfo=UTC),
            datetime(2026, 4, 1, 2, 0, tzinfo=UTC),
        ]

        selected = filter_windowed_timestamps(
            timestamps,
            granularity=Granularity.ONE_HOUR,
            shared_from=datetime(2026, 4, 1, 0, 30, tzinfo=UTC),
            shared_to=datetime(2026, 4, 1, 3, 0, tzinfo=UTC),
        )

        self.assertEqual(
            selected,
            [
                datetime(2026, 4, 1, 1, 0, tzinfo=UTC),
                datetime(2026, 4, 1, 2, 0, tzinfo=UTC),
            ],
        )

    def test_evaluate_candidates_limit_only_processes_requested_count(self) -> None:
        candidate_one = Market(
            market_id=10,
            coinalyze_symbol="ETHUSD.C",
            provider_code="C",
            market_type=MarketType.SPOT,
            base_asset="ETH",
            quote_asset="USD",
            symbol_on_exchange="ETH-USD",
            is_perpetual=False,
            margined=None,
            oi_unit=None,
            has_buy_sell_data=True,
            has_ohlcv_data=True,
        )
        candidate_two = replace(candidate_one, market_id=11, coinalyze_symbol="ETHUSD.K", provider_code="K")
        history = {}
        for symbol in (candidate_one.coinalyze_symbol, candidate_two.coinalyze_symbol):
            for granularity in Granularity.required():
                history[(symbol, MetricName.OHLCV, granularity)] = make_history(
                    datetime(2026, 4, 1, tzinfo=UTC),
                    10,
                    granularity,
                )
        repository = FakeRepository()
        repository.markets = [candidate_one, candidate_two]
        client = FakeClient(history)
        service = WarehouseService(
            client=client,
            repository=repository,
            settings=Settings(database_url="postgresql://unused", coinalyze_api_key="unused"),
            now=lambda: datetime(2026, 4, 10, tzinfo=UTC),
        )

        evaluated = service.evaluate_candidates(limit=1)

        self.assertEqual(len(evaluated), 1)
        self.assertEqual(len(repository.activation_updates), 1)

    def test_extend_active_markets_does_not_jump_forward_across_recent_gap(self) -> None:
        market = Market(
            market_id=12,
            coinalyze_symbol="ETHUSD.C",
            provider_code="C",
            market_type=MarketType.SPOT,
            base_asset="ETH",
            quote_asset="USD",
            symbol_on_exchange="ETH-USD",
            is_perpetual=False,
            margined=None,
            oi_unit=None,
            has_buy_sell_data=True,
            has_ohlcv_data=True,
            activation_status=ActivationStatus.ACTIVE,
            shared_complete_from=datetime(2026, 4, 1, 0, 0, tzinfo=UTC),
            shared_complete_to=datetime(2026, 4, 2, 0, 0, tzinfo=UTC),
        )
        history = {
            (market.coinalyze_symbol, MetricName.OHLCV, Granularity.FIFTEEN_MIN): make_history(
                datetime(2026, 4, 4, 0, 0, tzinfo=UTC),
                2 * 24 * 4,
                Granularity.FIFTEEN_MIN,
            ),
            (market.coinalyze_symbol, MetricName.OHLCV, Granularity.ONE_HOUR): make_history(
                datetime(2026, 4, 4, 0, 0, tzinfo=UTC),
                2 * 24,
                Granularity.ONE_HOUR,
            ),
            (market.coinalyze_symbol, MetricName.OHLCV, Granularity.DAILY): make_history(
                datetime(2026, 4, 4, 0, 0, tzinfo=UTC),
                2,
                Granularity.DAILY,
            ),
        }
        repository = FakeRepository()
        repository.markets = [market]
        client = FakeClient(history)
        service = WarehouseService(
            client=client,
            repository=repository,
            settings=Settings(database_url="postgresql://unused", coinalyze_api_key="unused"),
            now=lambda: datetime(2026, 4, 6, tzinfo=UTC),
        )

        updated = service.extend_active_markets()

        self.assertEqual(updated[0].shared_complete_to, datetime(2026, 4, 2, 0, 0, tzinfo=UTC))
        self.assertEqual(repository.activation_updates, [])

    def test_extend_active_markets_moves_shared_from_backward_when_all_series_attach(self) -> None:
        market = Market(
            market_id=13,
            coinalyze_symbol="ETHUSD.C",
            provider_code="C",
            market_type=MarketType.SPOT,
            base_asset="ETH",
            quote_asset="USD",
            symbol_on_exchange="ETH-USD",
            is_perpetual=False,
            margined=None,
            oi_unit=None,
            has_buy_sell_data=True,
            has_ohlcv_data=True,
            activation_status=ActivationStatus.ACTIVE,
            shared_complete_from=datetime(2026, 4, 5, 0, 0, tzinfo=UTC),
            shared_complete_to=datetime(2026, 4, 7, 0, 0, tzinfo=UTC),
        )
        history = {
            (market.coinalyze_symbol, MetricName.OHLCV, Granularity.FIFTEEN_MIN): make_history(
                datetime(2026, 4, 3, 0, 0, tzinfo=UTC),
                4 * 24 * 4,
                Granularity.FIFTEEN_MIN,
            ),
            (market.coinalyze_symbol, MetricName.OHLCV, Granularity.ONE_HOUR): make_history(
                datetime(2026, 4, 3, 0, 0, tzinfo=UTC),
                4 * 24,
                Granularity.ONE_HOUR,
            ),
            (market.coinalyze_symbol, MetricName.OHLCV, Granularity.DAILY): make_history(
                datetime(2026, 4, 3, 0, 0, tzinfo=UTC),
                4,
                Granularity.DAILY,
            ),
        }
        repository = FakeRepository()
        repository.markets = [market]
        client = FakeClient(history)
        service = WarehouseService(
            client=client,
            repository=repository,
            settings=Settings(database_url="postgresql://unused", coinalyze_api_key="unused"),
            now=lambda: datetime(2026, 4, 7, 12, 0, tzinfo=UTC),
        )

        updated = service.extend_active_markets()

        self.assertEqual(updated[0].shared_complete_from, datetime(2026, 4, 3, 0, 0, tzinfo=UTC))
        self.assertEqual(repository.activation_updates[-1]["shared_from"], datetime(2026, 4, 3, 0, 0, tzinfo=UTC))

    def test_backfill_market_pending_uses_sync_state_to_load_prior_chunk(self) -> None:
        market = Market(
            market_id=14,
            coinalyze_symbol="ETHUSD.C",
            provider_code="C",
            market_type=MarketType.SPOT,
            base_asset="ETH",
            quote_asset="USD",
            symbol_on_exchange="ETH-USD",
            is_perpetual=False,
            margined=None,
            oi_unit=None,
            has_buy_sell_data=True,
            has_ohlcv_data=True,
            activation_status=ActivationStatus.ACTIVE,
            shared_complete_from=datetime(2026, 3, 1, 0, 0, tzinfo=UTC),
            shared_complete_to=datetime(2026, 4, 1, 0, 0, tzinfo=UTC),
        )
        history = {
            (market.coinalyze_symbol, MetricName.OHLCV, Granularity.ONE_HOUR): make_history(
                datetime(2026, 3, 1, 0, 0, tzinfo=UTC),
                24 * 31,
                Granularity.ONE_HOUR,
            )
        }
        repository = FakeRepository()
        repository.markets = [market]
        repository.sync_states[(market.market_id, Granularity.ONE_HOUR, MetricName.OHLCV)] = {
            "market_id": market.market_id,
            "granularity": Granularity.ONE_HOUR,
            "metric_name": MetricName.OHLCV,
            "oldest_backfilled_bucket": datetime(2026, 3, 20, 0, 0, tzinfo=UTC),
            "newest_loaded_bucket": datetime(2026, 3, 31, 23, 0, tzinfo=UTC),
            "retry_after_until": None,
            "last_attempt_at": datetime(2026, 4, 1, 0, 0, tzinfo=UTC),
            "last_error": None,
        }
        client = FakeClient(history)
        service = WarehouseService(
            client=client,
            repository=repository,
            settings=Settings(
                database_url="postgresql://unused",
                coinalyze_api_key="unused",
                backfill_chunk_days=5,
            ),
            now=lambda: datetime(2026, 4, 10, tzinfo=UTC),
        )

        service.backfill_market_pending(market, max_chunks_per_market=1)

        self.assertEqual(len(repository.ohlcv), 1)
        loaded_bars = repository.ohlcv[0]["bars"]
        self.assertEqual(loaded_bars[0].bucket_start, datetime(2026, 3, 15, 0, 0, tzinfo=UTC))
        self.assertEqual(loaded_bars[-1].bucket_start, datetime(2026, 3, 19, 23, 0, tzinfo=UTC))

    def test_backfill_market_pending_prioritizes_right_edge_catch_up(self) -> None:
        market = Market(
            market_id=15,
            coinalyze_symbol="ETHUSD.C",
            provider_code="C",
            market_type=MarketType.SPOT,
            base_asset="ETH",
            quote_asset="USD",
            symbol_on_exchange="ETH-USD",
            is_perpetual=False,
            margined=None,
            oi_unit=None,
            has_buy_sell_data=True,
            has_ohlcv_data=True,
            activation_status=ActivationStatus.ACTIVE,
            shared_complete_from=datetime(2026, 3, 1, 0, 0, tzinfo=UTC),
            shared_complete_to=datetime(2026, 4, 1, 0, 0, tzinfo=UTC),
        )
        history = {
            (market.coinalyze_symbol, MetricName.OHLCV, Granularity.ONE_HOUR): make_history(
                datetime(2026, 3, 1, 0, 0, tzinfo=UTC),
                24 * 31,
                Granularity.ONE_HOUR,
            )
        }
        repository = FakeRepository()
        repository.markets = [market]
        repository.sync_states[(market.market_id, Granularity.ONE_HOUR, MetricName.OHLCV)] = {
            "market_id": market.market_id,
            "granularity": Granularity.ONE_HOUR,
            "metric_name": MetricName.OHLCV,
            "oldest_backfilled_bucket": datetime(2026, 3, 20, 0, 0, tzinfo=UTC),
            "newest_loaded_bucket": datetime(2026, 3, 30, 23, 0, tzinfo=UTC),
            "retry_after_until": None,
            "last_attempt_at": datetime(2026, 4, 1, 0, 0, tzinfo=UTC),
            "last_error": None,
        }
        client = FakeClient(history)
        service = WarehouseService(
            client=client,
            repository=repository,
            settings=Settings(
                database_url="postgresql://unused",
                coinalyze_api_key="unused",
                backfill_chunk_days=5,
            ),
            now=lambda: datetime(2026, 4, 10, tzinfo=UTC),
        )

        service.backfill_market_pending(market, max_chunks_per_market=1)

        self.assertEqual(len(repository.ohlcv), 1)
        loaded_bars = repository.ohlcv[0]["bars"]
        self.assertEqual(loaded_bars[0].bucket_start, datetime(2026, 3, 31, 0, 0, tzinfo=UTC))
        self.assertEqual(loaded_bars[-1].bucket_start, datetime(2026, 3, 31, 23, 0, tzinfo=UTC))

    def test_backfill_market_pending_uses_common_oldest_metric_boundary(self) -> None:
        market = Market(
            market_id=16,
            coinalyze_symbol="ETHUSD_PERP.A",
            provider_code="A",
            market_type=MarketType.PERP,
            base_asset="ETH",
            quote_asset="USD",
            symbol_on_exchange="ETHUSD_PERP",
            is_perpetual=True,
            margined="COIN",
            oi_unit="QUOTE_ASSET",
            has_buy_sell_data=True,
            has_ohlcv_data=True,
            activation_status=ActivationStatus.ACTIVE,
            shared_complete_from=datetime(2026, 3, 1, 0, 0, tzinfo=UTC),
            shared_complete_to=datetime(2026, 4, 1, 0, 0, tzinfo=UTC),
        )
        history = {}
        for metric in market.required_metrics:
            history[(market.coinalyze_symbol, metric, Granularity.ONE_HOUR)] = make_history(
                datetime(2026, 3, 1, 0, 0, tzinfo=UTC),
                24 * 31,
                Granularity.ONE_HOUR,
            )
        repository = FakeRepository()
        repository.markets = [market]
        sync_rows = {
            MetricName.OHLCV: datetime(2026, 3, 20, 0, 0, tzinfo=UTC),
            MetricName.OI_NATIVE: datetime(2026, 3, 18, 0, 0, tzinfo=UTC),
            MetricName.OI_USD: datetime(2026, 3, 15, 0, 0, tzinfo=UTC),
            MetricName.FUNDING: datetime(2026, 3, 19, 0, 0, tzinfo=UTC),
        }
        for metric_name, oldest_loaded in sync_rows.items():
            repository.sync_states[(market.market_id, Granularity.ONE_HOUR, metric_name)] = {
                "market_id": market.market_id,
                "granularity": Granularity.ONE_HOUR,
                "metric_name": metric_name,
                "oldest_backfilled_bucket": oldest_loaded,
                "newest_loaded_bucket": datetime(2026, 3, 31, 23, 0, tzinfo=UTC),
                "retry_after_until": None,
                "last_attempt_at": datetime(2026, 4, 1, 0, 0, tzinfo=UTC),
                "last_error": None,
            }
        client = FakeClient(history)
        service = WarehouseService(
            client=client,
            repository=repository,
            settings=Settings(
                database_url="postgresql://unused",
                coinalyze_api_key="unused",
                backfill_chunk_days=5,
            ),
            now=lambda: datetime(2026, 4, 10, tzinfo=UTC),
        )

        service.backfill_market_pending(market, max_chunks_per_market=1)

        self.assertEqual(len(repository.ohlcv), 1)
        self.assertEqual(len(repository.oi), 1)
        self.assertEqual(len(repository.funding), 1)
        loaded_bars = repository.ohlcv[0]["bars"]
        self.assertEqual(loaded_bars[0].bucket_start, datetime(2026, 3, 15, 0, 0, tzinfo=UTC))
        self.assertEqual(loaded_bars[-1].bucket_start, datetime(2026, 3, 19, 23, 0, tzinfo=UTC))
