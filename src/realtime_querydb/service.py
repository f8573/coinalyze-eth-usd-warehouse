from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Callable, Iterable, Sequence

from .coinalyze import CoinalyzeClient
from .config import Settings
from .models import (
    ActivationStatus,
    FundingRateBar,
    Granularity,
    Market,
    MarketType,
    MetricName,
    OhlcvBar,
    OpenInterestBar,
    SeriesWindow,
    UTC,
    compute_shared_window,
)

if TYPE_CHECKING:
    from .repository import WarehouseRepository

EXCLUDED_QUOTE_ASSETS = {"USDT", "USDC", "USDE", "TUSD", "PYUSD"}
UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def is_supported_quote_asset(quote_asset: str | None) -> bool:
    if not quote_asset:
        return False
    quote = quote_asset.upper()
    if quote in EXCLUDED_QUOTE_ASSETS:
        return False
    return quote == "USD" or quote.startswith("USD_") or (quote.startswith("USD") and quote[3:].isdigit())


def build_future_markets(payloads: Iterable[dict[str, object]]) -> list[Market]:
    markets: list[Market] = []
    for item in payloads:
        if item.get("base_asset") != "ETH":
            continue
        if not bool(item.get("is_perpetual")):
            continue
        if not is_supported_quote_asset(item.get("quote_asset")):
            continue
        markets.append(
            Market(
                coinalyze_symbol=str(item["symbol"]),
                provider_code=str(item["exchange"]),
                market_type=MarketType.PERP,
                base_asset="ETH",
                quote_asset=str(item["quote_asset"]),
                symbol_on_exchange=str(item["symbol_on_exchange"]),
                is_perpetual=True,
                margined=str(item["margined"]) if item.get("margined") is not None else None,
                oi_unit=str(item["oi_lq_vol_denominated_in"]) if item.get("oi_lq_vol_denominated_in") is not None else None,
                has_buy_sell_data=bool(item.get("has_buy_sell_data", False)),
                has_ohlcv_data=bool(item.get("has_ohlcv_data", True)),
            )
        )
    return markets


def build_spot_markets(payloads: Iterable[dict[str, object]]) -> list[Market]:
    markets: list[Market] = []
    for item in payloads:
        if item.get("base_asset") != "ETH":
            continue
        if not is_supported_quote_asset(item.get("quote_asset")):
            continue
        markets.append(
            Market(
                coinalyze_symbol=str(item["symbol"]),
                provider_code=str(item["exchange"]),
                market_type=MarketType.SPOT,
                base_asset="ETH",
                quote_asset=str(item["quote_asset"]),
                symbol_on_exchange=str(item["symbol_on_exchange"]),
                is_perpetual=False,
                margined=None,
                oi_unit=None,
                has_buy_sell_data=bool(item.get("has_buy_sell_data", False)),
                has_ohlcv_data=True,
            )
        )
    return markets


def floor_bucket_start(moment: datetime, granularity: Granularity) -> datetime:
    moment = moment.astimezone(UTC).replace(second=0, microsecond=0)
    if granularity is Granularity.FIFTEEN_MIN:
        minute = moment.minute - (moment.minute % 15)
        return moment.replace(minute=minute)
    if granularity is Granularity.ONE_HOUR:
        return moment.replace(minute=0)
    return moment.replace(hour=0, minute=0)


def last_closed_bucket_start(moment: datetime, granularity: Granularity) -> datetime:
    return floor_bucket_start(moment, granularity) - granularity.delta


def history_request_to_inclusive(end_exclusive: datetime, granularity: Granularity) -> datetime:
    return end_exclusive - granularity.delta


def build_contiguous_suffix_window(
    *,
    metric_name: MetricName,
    granularity: Granularity,
    history: Sequence[dict[str, object]],
    anchor_end_exclusive: datetime | None = None,
) -> SeriesWindow | None:
    if not history:
        return None
    timestamps = sorted({datetime.fromtimestamp(int(item["t"]), UTC) for item in history})
    if not timestamps:
        return None
    delta = granularity.delta
    if anchor_end_exclusive is None:
        end_index = len(timestamps) - 1
        end_exclusive = timestamps[end_index] + delta
    else:
        anchor_timestamp = anchor_end_exclusive - delta
        try:
            end_index = timestamps.index(anchor_timestamp)
        except ValueError:
            return None
        end_exclusive = anchor_end_exclusive
    start_index = end_index
    while start_index > 0 and timestamps[start_index - 1] + delta == timestamps[start_index]:
        start_index -= 1
    return SeriesWindow(
        metric_name=metric_name,
        granularity=granularity,
        start=timestamps[start_index],
        end_exclusive=end_exclusive,
        point_count=end_index - start_index + 1,
    )


def filter_windowed_timestamps(
    timestamps: Iterable[datetime],
    *,
    granularity: Granularity,
    shared_from: datetime,
    shared_to: datetime,
) -> list[datetime]:
    selected: list[datetime] = []
    for timestamp in sorted(timestamps):
        if timestamp >= shared_from and timestamp + granularity.delta <= shared_to:
            selected.append(timestamp)
    return selected


def iter_batches(items: Sequence[Market], batch_size: int) -> Iterable[Sequence[Market]]:
    for index in range(0, len(items), batch_size):
        yield items[index : index + batch_size]


def first_bucket_start_within_window(shared_from: datetime, granularity: Granularity) -> datetime:
    bucket_start = floor_bucket_start(shared_from, granularity)
    if bucket_start < shared_from:
        return bucket_start + granularity.delta
    return bucket_start


def last_bucket_start_within_window(shared_to: datetime, granularity: Granularity) -> datetime:
    return floor_bucket_start(shared_to - granularity.delta, granularity)


class WarehouseService:
    def __init__(
        self,
        *,
        client: CoinalyzeClient,
        repository: WarehouseRepository,
        settings: Settings,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._client = client
        self._repository = repository
        self._settings = settings
        self._now = now or (lambda: datetime.now(UTC))

    def init_db(self) -> None:
        self._repository.apply_schema()

    def discover_markets(self) -> list[Market]:
        providers = self._client.get_exchanges()
        self._repository.upsert_providers(providers)
        markets = build_future_markets(self._client.get_future_markets())
        markets.extend(build_spot_markets(self._client.get_spot_markets()))
        self._repository.upsert_markets(markets)
        self._repository.exclude_undiscovered_markets([market.coinalyze_symbol for market in markets])
        return markets

    def evaluate_candidates(self, limit: int | None = None) -> list[Market]:
        evaluated: list[Market] = []
        candidates = self._repository.list_markets(ActivationStatus.CANDIDATE)
        if limit is not None:
            candidates = candidates[:limit]
        if not candidates:
            return evaluated
        recent_windows = self._collect_recent_candidate_windows(candidates)
        for market in candidates:
            evaluated.append(self._evaluate_market_from_windows(market, recent_windows))
        return evaluated

    def evaluate_market(self, market: Market) -> Market:
        recent_windows = self._collect_recent_candidate_windows([market])
        return self._evaluate_market_from_windows(market, recent_windows)

    def _evaluate_market_from_windows(
        self,
        market: Market,
        windows_by_series: dict[tuple[str, Granularity, MetricName], SeriesWindow],
    ) -> Market:
        checked_at = self._now()
        windows: list[SeriesWindow] = []
        for granularity in market.required_granularities:
            for metric_name in market.required_metrics:
                window = windows_by_series.get((market.coinalyze_symbol, granularity, metric_name))
                if window is None:
                    excluded = replace(
                        market,
                        activation_status=ActivationStatus.EXCLUDED,
                        eligibility_checked_at=checked_at,
                        shared_complete_from=None,
                        shared_complete_to=None,
                        exclusion_reason=f"missing_{metric_name.value}_{granularity.value}",
                    )
                    if excluded.market_id is not None:
                        self._repository.set_market_activation(
                            market_id=excluded.market_id,
                            status=excluded.activation_status,
                            checked_at=checked_at,
                            shared_from=None,
                            shared_to=None,
                            exclusion_reason=excluded.exclusion_reason,
                        )
                    return excluded
                windows.append(window)
        shared_window = compute_shared_window(windows)
        if shared_window is None:
            excluded = replace(
                market,
                activation_status=ActivationStatus.EXCLUDED,
                eligibility_checked_at=checked_at,
                shared_complete_from=None,
                shared_complete_to=None,
                exclusion_reason="no_shared_overlap",
            )
            if excluded.market_id is not None:
                self._repository.set_market_activation(
                    market_id=excluded.market_id,
                    status=excluded.activation_status,
                    checked_at=checked_at,
                    shared_from=None,
                    shared_to=None,
                    exclusion_reason=excluded.exclusion_reason,
                )
            return excluded
        active = replace(
            market,
            activation_status=ActivationStatus.ACTIVE,
            eligibility_checked_at=checked_at,
            shared_complete_from=shared_window[0],
            shared_complete_to=shared_window[1],
            exclusion_reason=None,
        )
        if active.market_id is not None:
            self._repository.set_market_activation(
                market_id=active.market_id,
                status=active.activation_status,
                checked_at=checked_at,
                shared_from=active.shared_complete_from,
                shared_to=active.shared_complete_to,
                exclusion_reason=None,
            )
        return active

    def backfill_active_markets(self) -> None:
        for market in self._repository.list_markets(ActivationStatus.ACTIVE):
            self.backfill_market(market)

    def backfill_pending_active_markets(self, max_chunks_per_market: int | None = None) -> None:
        chunk_limit = max_chunks_per_market if max_chunks_per_market is not None else self._settings.max_backfill_chunks_per_market_per_run
        for market in self._repository.list_markets(ActivationStatus.ACTIVE):
            self.backfill_market_pending(market, max_chunks_per_market=chunk_limit)

    def extend_active_markets(self) -> list[Market]:
        active_markets = self._repository.list_markets(ActivationStatus.ACTIVE)
        updated_markets: list[Market] = []
        for market in active_markets:
            updated_markets.append(self._extend_active_market(market))
        return updated_markets

    def _extend_active_market(self, market: Market) -> Market:
        if market.market_id is None:
            raise ValueError("Market must have a database id before extending.")
        if market.shared_complete_from is None or market.shared_complete_to is None:
            return market
        original_from = market.shared_complete_from
        original_to = market.shared_complete_to
        backward_shared_from = self._probe_backward_shared_from(market)
        if backward_shared_from is not None and backward_shared_from < market.shared_complete_from:
            market = replace(market, shared_complete_from=backward_shared_from)
        forward_shared_to = self._probe_forward_shared_to(market)
        if forward_shared_to is not None and forward_shared_to > market.shared_complete_to:
            market = replace(market, shared_complete_to=forward_shared_to)
        if market.shared_complete_from == original_from and market.shared_complete_to == original_to:
            return market
        checked_at = self._now()
        updated = replace(market, eligibility_checked_at=checked_at)
        self._repository.set_market_activation(
            market_id=updated.market_id,
            status=updated.activation_status,
            checked_at=checked_at,
            shared_from=updated.shared_complete_from,
            shared_to=updated.shared_complete_to,
            exclusion_reason=updated.exclusion_reason,
        )
        return updated

    def _probe_backward_shared_from(self, market: Market) -> datetime | None:
        windows: list[SeriesWindow] = []
        for granularity in market.required_granularities:
            anchor_end = first_bucket_start_within_window(market.shared_complete_from, granularity)
            end_inclusive = anchor_end - granularity.delta
            if end_inclusive < UNIX_EPOCH:
                return None
            start = max(UNIX_EPOCH, anchor_end - timedelta(days=self._settings.backfill_chunk_days))
            for metric_name in market.required_metrics:
                history = self._client.get_history(
                    symbols=[market.coinalyze_symbol],
                    metric_name=metric_name,
                    granularity=granularity,
                    start=start,
                    end_inclusive=end_inclusive,
                    on_retry_after=self._retry_notifier(market.market_id, granularity, metric_name),
                ).get(market.coinalyze_symbol, [])
                window = build_contiguous_suffix_window(
                    metric_name=metric_name,
                    granularity=granularity,
                    history=history,
                    anchor_end_exclusive=anchor_end,
                )
                if window is None:
                    return None
                windows.append(window)
        shared_window = compute_shared_window(windows)
        if shared_window is None:
            return None
        return shared_window[0]

    def _probe_forward_shared_to(self, market: Market) -> datetime | None:
        now = self._now()
        windows: list[SeriesWindow] = []
        for granularity in market.required_granularities:
            end_inclusive = last_closed_bucket_start(now, granularity)
            current_last_bucket = last_bucket_start_within_window(market.shared_complete_to, granularity)
            if end_inclusive < current_last_bucket:
                return None
            start = max(UNIX_EPOCH, current_last_bucket - timedelta(days=self._settings.probe_chunk_days))
            series_edge_end = current_last_bucket + granularity.delta
            for metric_name in market.required_metrics:
                history = self._client.get_history(
                    symbols=[market.coinalyze_symbol],
                    metric_name=metric_name,
                    granularity=granularity,
                    start=start,
                    end_inclusive=end_inclusive,
                    on_retry_after=self._retry_notifier(market.market_id, granularity, metric_name),
                ).get(market.coinalyze_symbol, [])
                window = build_contiguous_suffix_window(
                    metric_name=metric_name,
                    granularity=granularity,
                    history=history,
                )
                if window is None or window.start > series_edge_end:
                    return None
                windows.append(window)
        shared_window = compute_shared_window(windows)
        if shared_window is None or shared_window[0] > market.shared_complete_to:
            return None
        return shared_window[1]

    def refresh_active_markets(self) -> None:
        active_markets = self._repository.list_markets(ActivationStatus.ACTIVE)
        if not active_markets:
            return
        self._refresh_recent_batches(active_markets)

    def sync_all(self) -> None:
        self.discover_markets()
        self.evaluate_candidates()
        self.backfill_active_markets()
        self.extend_active_markets()
        self.refresh_active_markets()

    def sync_daily(self) -> None:
        self.discover_markets()
        self.evaluate_candidates(limit=self._settings.max_candidate_markets_per_run)
        self.extend_active_markets()
        self.backfill_pending_active_markets(self._settings.max_backfill_chunks_per_market_per_run)
        self.refresh_active_markets()

    def backfill_market(self, market: Market) -> None:
        if market.market_id is None:
            raise ValueError("Market must have a database id before backfilling.")
        if market.shared_complete_from is None or market.shared_complete_to is None:
            raise ValueError("Market must have a shared_complete window before backfilling.")
        for granularity in market.required_granularities:
            for chunk_start, chunk_end in self._iter_forward_chunks(
                market.shared_complete_from,
                market.shared_complete_to,
            ):
                if market.market_type is MarketType.SPOT:
                    self._load_spot_chunk(market, granularity, chunk_start, chunk_end)
                else:
                    self._load_perp_chunk(market, granularity, chunk_start, chunk_end)

    def refresh_market(self, market: Market) -> None:
        if market.market_id is None:
            raise ValueError("Market must have a database id before refresh.")
        if market.shared_complete_from is None or market.shared_complete_to is None:
            return
        now = self._now()
        for granularity in market.required_granularities:
            last_closed = last_closed_bucket_start(now, granularity)
            refresh_start = last_closed - (granularity.delta * (granularity.refresh_lookback_bars - 1))
            refresh_end = last_closed + granularity.delta
            chunk_start = max(refresh_start, market.shared_complete_from)
            chunk_end = min(refresh_end, market.shared_complete_to)
            if chunk_start >= chunk_end:
                continue
            if market.market_type is MarketType.SPOT:
                self._load_spot_chunk(market, granularity, chunk_start, chunk_end)
            else:
                self._load_perp_chunk(market, granularity, chunk_start, chunk_end)

    def backfill_market_pending(self, market: Market, *, max_chunks_per_market: int) -> None:
        if market.market_id is None:
            raise ValueError("Market must have a database id before backfilling.")
        if market.shared_complete_from is None or market.shared_complete_to is None:
            return
        for granularity in market.required_granularities:
            remaining_chunks = max_chunks_per_market
            while remaining_chunks > 0:
                sync_states = self._repository.list_sync_states(market.market_id)
                chunk = self._next_pending_chunk(market, granularity, sync_states)
                if chunk is None:
                    break
                chunk_start, chunk_end = chunk
                if market.market_type is MarketType.SPOT:
                    loaded_count = self._load_spot_chunk(market, granularity, chunk_start, chunk_end)
                else:
                    loaded_count = self._load_perp_chunk(market, granularity, chunk_start, chunk_end)
                if loaded_count <= 0:
                    break
                remaining_chunks -= 1

    def _next_pending_chunk(
        self,
        market: Market,
        granularity: Granularity,
        sync_states,
    ) -> tuple[datetime, datetime] | None:
        target_start = first_bucket_start_within_window(market.shared_complete_from, granularity)
        target_end = market.shared_complete_to
        if target_start >= target_end:
            return None
        target_last_bucket = last_bucket_start_within_window(target_end, granularity)
        required_states = [
            sync_states.get((granularity, metric_name))
            for metric_name in market.required_metrics
        ]
        if any(state is None or state.newest_loaded_bucket is None for state in required_states):
            chunk_end = target_end
            chunk_start = max(target_start, chunk_end - timedelta(days=self._settings.backfill_chunk_days))
            if chunk_start >= chunk_end:
                return None
            return chunk_start, chunk_end
        common_newest_loaded = min(state.newest_loaded_bucket for state in required_states)
        if common_newest_loaded < target_last_bucket:
            chunk_start = max(target_start, common_newest_loaded + granularity.delta)
            chunk_end = min(target_end, chunk_start + timedelta(days=self._settings.backfill_chunk_days))
            if chunk_start >= chunk_end:
                return None
            return chunk_start, chunk_end
        if any(state.oldest_backfilled_bucket is None for state in required_states):
            return None
        common_oldest_loaded = max(state.oldest_backfilled_bucket for state in required_states)
        if common_oldest_loaded <= target_start:
            return None
        chunk_end = common_oldest_loaded
        chunk_start = max(target_start, chunk_end - timedelta(days=self._settings.backfill_chunk_days))
        if chunk_start >= chunk_end:
            return None
        return chunk_start, chunk_end

    def _collect_recent_candidate_windows(
        self,
        markets: Sequence[Market],
    ) -> dict[tuple[str, Granularity, MetricName], SeriesWindow]:
        recent_windows: dict[tuple[str, Granularity, MetricName], SeriesWindow] = {}
        now = self._now()
        spot_markets = [market for market in markets if market.market_type is MarketType.SPOT]
        perp_markets = [market for market in markets if market.market_type is MarketType.PERP]
        for granularity in Granularity.required():
            end_inclusive = last_closed_bucket_start(now, granularity)
            start = max(UNIX_EPOCH, end_inclusive - timedelta(days=self._settings.active_window_probe_days))
            if spot_markets:
                spot_histories = self._fetch_batch_histories(
                    spot_markets,
                    metric_name=MetricName.OHLCV,
                    granularity=granularity,
                    start=start,
                    end_inclusive=end_inclusive,
                )
                for market in spot_markets:
                    window = build_contiguous_suffix_window(
                        metric_name=MetricName.OHLCV,
                        granularity=granularity,
                        history=spot_histories.get(market.coinalyze_symbol, []),
                    )
                    if window is not None:
                        recent_windows[(market.coinalyze_symbol, granularity, MetricName.OHLCV)] = window
            if perp_markets:
                for metric_name in (
                    MetricName.OHLCV,
                    MetricName.OI_NATIVE,
                    MetricName.OI_USD,
                    MetricName.FUNDING,
                ):
                    perp_histories = self._fetch_batch_histories(
                        perp_markets,
                        metric_name=metric_name,
                        granularity=granularity,
                        start=start,
                        end_inclusive=end_inclusive,
                    )
                    for market in perp_markets:
                        window = build_contiguous_suffix_window(
                            metric_name=metric_name,
                            granularity=granularity,
                            history=perp_histories.get(market.coinalyze_symbol, []),
                        )
                        if window is not None:
                            recent_windows[(market.coinalyze_symbol, granularity, metric_name)] = window
        return recent_windows

    def _refresh_recent_batches(self, markets: Sequence[Market]) -> None:
        now = self._now()
        spot_markets = [market for market in markets if market.market_type is MarketType.SPOT]
        perp_markets = [market for market in markets if market.market_type is MarketType.PERP]
        for granularity in Granularity.required():
            last_closed = last_closed_bucket_start(now, granularity)
            end_exclusive = last_closed + granularity.delta
            start = end_exclusive - (granularity.delta * granularity.refresh_lookback_bars)
            end_inclusive = history_request_to_inclusive(end_exclusive, granularity)
            if spot_markets:
                spot_histories = self._fetch_batch_histories(
                    spot_markets,
                    metric_name=MetricName.OHLCV,
                    granularity=granularity,
                    start=start,
                    end_inclusive=end_inclusive,
                )
                for market in spot_markets:
                    if market.market_id is None or market.shared_complete_from is None or market.shared_complete_to is None:
                        continue
                    bars = [
                        OhlcvBar.from_api(item, market.has_buy_sell_data)
                        for item in spot_histories.get(market.coinalyze_symbol, [])
                    ]
                    filtered = [
                        bar
                        for bar in bars
                        if bar.bucket_start >= market.shared_complete_from
                        and bar.bucket_start + granularity.delta <= market.shared_complete_to
                    ]
                    self._repository.upsert_ohlcv_bars(
                        market_id=market.market_id,
                        granularity=granularity,
                        bars=filtered,
                    )
                    self._update_sync_success(market.market_id, granularity, MetricName.OHLCV, filtered)
            if perp_markets:
                ohlcv_histories = self._fetch_batch_histories(
                    perp_markets,
                    metric_name=MetricName.OHLCV,
                    granularity=granularity,
                    start=start,
                    end_inclusive=end_inclusive,
                )
                oi_native_histories = self._fetch_batch_histories(
                    perp_markets,
                    metric_name=MetricName.OI_NATIVE,
                    granularity=granularity,
                    start=start,
                    end_inclusive=end_inclusive,
                )
                oi_usd_histories = self._fetch_batch_histories(
                    perp_markets,
                    metric_name=MetricName.OI_USD,
                    granularity=granularity,
                    start=start,
                    end_inclusive=end_inclusive,
                )
                funding_histories = self._fetch_batch_histories(
                    perp_markets,
                    metric_name=MetricName.FUNDING,
                    granularity=granularity,
                    start=start,
                    end_inclusive=end_inclusive,
                )
                for market in perp_markets:
                    if market.market_id is None or market.shared_complete_from is None or market.shared_complete_to is None:
                        continue
                    ohlcv_map = {
                        datetime.fromtimestamp(int(item["t"]), UTC): item
                        for item in ohlcv_histories.get(market.coinalyze_symbol, [])
                    }
                    oi_native_map = {
                        datetime.fromtimestamp(int(item["t"]), UTC): item
                        for item in oi_native_histories.get(market.coinalyze_symbol, [])
                    }
                    oi_usd_map = {
                        datetime.fromtimestamp(int(item["t"]), UTC): item
                        for item in oi_usd_histories.get(market.coinalyze_symbol, [])
                    }
                    funding_map = {
                        datetime.fromtimestamp(int(item["t"]), UTC): item
                        for item in funding_histories.get(market.coinalyze_symbol, [])
                    }
                    timestamps = filter_windowed_timestamps(
                        set(ohlcv_map).intersection(oi_native_map, oi_usd_map, funding_map),
                        granularity=granularity,
                        shared_from=market.shared_complete_from,
                        shared_to=market.shared_complete_to,
                    )
                    ohlcv_bars = [OhlcvBar.from_api(ohlcv_map[timestamp], market.has_buy_sell_data) for timestamp in timestamps]
                    open_interest_bars = [
                        OpenInterestBar(
                            bucket_start=timestamp,
                            native_open=float(oi_native_map[timestamp]["o"]),
                            native_high=float(oi_native_map[timestamp]["h"]),
                            native_low=float(oi_native_map[timestamp]["l"]),
                            native_close=float(oi_native_map[timestamp]["c"]),
                            usd_open=float(oi_usd_map[timestamp]["o"]),
                            usd_high=float(oi_usd_map[timestamp]["h"]),
                            usd_low=float(oi_usd_map[timestamp]["l"]),
                            usd_close=float(oi_usd_map[timestamp]["c"]),
                        )
                        for timestamp in timestamps
                    ]
                    funding_bars = [FundingRateBar.from_api(funding_map[timestamp]) for timestamp in timestamps]
                    self._repository.upsert_ohlcv_bars(market_id=market.market_id, granularity=granularity, bars=ohlcv_bars)
                    self._repository.upsert_open_interest_bars(
                        market_id=market.market_id,
                        granularity=granularity,
                        bars=open_interest_bars,
                    )
                    self._repository.upsert_funding_rate_bars(
                        market_id=market.market_id,
                        granularity=granularity,
                        bars=funding_bars,
                    )
                    self._update_sync_success(market.market_id, granularity, MetricName.OHLCV, ohlcv_bars)
                    self._update_sync_success(market.market_id, granularity, MetricName.OI_NATIVE, open_interest_bars)
                    self._update_sync_success(market.market_id, granularity, MetricName.OI_USD, open_interest_bars)
                    self._update_sync_success(market.market_id, granularity, MetricName.FUNDING, funding_bars)

    def _fetch_batch_histories(
        self,
        markets: Sequence[Market],
        *,
        metric_name: MetricName,
        granularity: Granularity,
        start: datetime,
        end_inclusive: datetime,
    ) -> dict[str, list[dict[str, object]]]:
        histories: dict[str, list[dict[str, object]]] = {}
        for batch in iter_batches(list(markets), self._client.max_symbols_per_request):
            histories.update(
                self._client.get_history(
                    symbols=[market.coinalyze_symbol for market in batch],
                    metric_name=metric_name,
                    granularity=granularity,
                    start=start,
                    end_inclusive=end_inclusive,
                    on_retry_after=self._retry_notifier_for_batch(
                        [market.market_id for market in batch if market.market_id is not None],
                        granularity,
                        metric_name,
                    ),
                )
            )
        return histories

    def _load_spot_chunk(
        self,
        market: Market,
        granularity: Granularity,
        chunk_start: datetime,
        chunk_end: datetime,
    ) -> int:
        assert market.market_id is not None
        history = self._client.get_history(
            symbols=[market.coinalyze_symbol],
            metric_name=MetricName.OHLCV,
            granularity=granularity,
            start=chunk_start,
            end_inclusive=history_request_to_inclusive(chunk_end, granularity),
            on_retry_after=self._retry_notifier(market.market_id, granularity, MetricName.OHLCV),
        ).get(market.coinalyze_symbol, [])
        bars = [OhlcvBar.from_api(item, market.has_buy_sell_data) for item in history]
        filtered = [
            bar
            for bar in bars
            if bar.bucket_start >= market.shared_complete_from
            and bar.bucket_start + granularity.delta <= market.shared_complete_to
        ]
        self._repository.upsert_ohlcv_bars(
            market_id=market.market_id,
            granularity=granularity,
            bars=filtered,
        )
        self._update_sync_success(market.market_id, granularity, MetricName.OHLCV, filtered)
        return len(filtered)

    def _load_perp_chunk(
        self,
        market: Market,
        granularity: Granularity,
        chunk_start: datetime,
        chunk_end: datetime,
    ) -> int:
        assert market.market_id is not None
        ohlcv_history = self._client.get_history(
            symbols=[market.coinalyze_symbol],
            metric_name=MetricName.OHLCV,
            granularity=granularity,
            start=chunk_start,
            end_inclusive=history_request_to_inclusive(chunk_end, granularity),
            on_retry_after=self._retry_notifier(market.market_id, granularity, MetricName.OHLCV),
        ).get(market.coinalyze_symbol, [])
        oi_native_history = self._client.get_history(
            symbols=[market.coinalyze_symbol],
            metric_name=MetricName.OI_NATIVE,
            granularity=granularity,
            start=chunk_start,
            end_inclusive=history_request_to_inclusive(chunk_end, granularity),
            on_retry_after=self._retry_notifier(market.market_id, granularity, MetricName.OI_NATIVE),
        ).get(market.coinalyze_symbol, [])
        oi_usd_history = self._client.get_history(
            symbols=[market.coinalyze_symbol],
            metric_name=MetricName.OI_USD,
            granularity=granularity,
            start=chunk_start,
            end_inclusive=history_request_to_inclusive(chunk_end, granularity),
            on_retry_after=self._retry_notifier(market.market_id, granularity, MetricName.OI_USD),
        ).get(market.coinalyze_symbol, [])
        funding_history = self._client.get_history(
            symbols=[market.coinalyze_symbol],
            metric_name=MetricName.FUNDING,
            granularity=granularity,
            start=chunk_start,
            end_inclusive=history_request_to_inclusive(chunk_end, granularity),
            on_retry_after=self._retry_notifier(market.market_id, granularity, MetricName.FUNDING),
        ).get(market.coinalyze_symbol, [])

        ohlcv_map = {datetime.fromtimestamp(int(item["t"]), UTC): item for item in ohlcv_history}
        oi_native_map = {datetime.fromtimestamp(int(item["t"]), UTC): item for item in oi_native_history}
        oi_usd_map = {datetime.fromtimestamp(int(item["t"]), UTC): item for item in oi_usd_history}
        funding_map = {datetime.fromtimestamp(int(item["t"]), UTC): item for item in funding_history}
        timestamps = filter_windowed_timestamps(
            set(ohlcv_map).intersection(oi_native_map, oi_usd_map, funding_map),
            granularity=granularity,
            shared_from=market.shared_complete_from,
            shared_to=market.shared_complete_to,
        )
        ohlcv_bars = [OhlcvBar.from_api(ohlcv_map[timestamp], market.has_buy_sell_data) for timestamp in timestamps]
        open_interest_bars = [
            OpenInterestBar(
                bucket_start=timestamp,
                native_open=float(oi_native_map[timestamp]["o"]),
                native_high=float(oi_native_map[timestamp]["h"]),
                native_low=float(oi_native_map[timestamp]["l"]),
                native_close=float(oi_native_map[timestamp]["c"]),
                usd_open=float(oi_usd_map[timestamp]["o"]),
                usd_high=float(oi_usd_map[timestamp]["h"]),
                usd_low=float(oi_usd_map[timestamp]["l"]),
                usd_close=float(oi_usd_map[timestamp]["c"]),
            )
            for timestamp in timestamps
        ]
        funding_bars = [FundingRateBar.from_api(funding_map[timestamp]) for timestamp in timestamps]
        self._repository.upsert_ohlcv_bars(market_id=market.market_id, granularity=granularity, bars=ohlcv_bars)
        self._repository.upsert_open_interest_bars(
            market_id=market.market_id,
            granularity=granularity,
            bars=open_interest_bars,
        )
        self._repository.upsert_funding_rate_bars(
            market_id=market.market_id,
            granularity=granularity,
            bars=funding_bars,
        )
        self._update_sync_success(market.market_id, granularity, MetricName.OHLCV, ohlcv_bars)
        self._update_sync_success(market.market_id, granularity, MetricName.OI_NATIVE, open_interest_bars)
        self._update_sync_success(market.market_id, granularity, MetricName.OI_USD, open_interest_bars)
        self._update_sync_success(market.market_id, granularity, MetricName.FUNDING, funding_bars)
        return len(timestamps)

    def _iter_forward_chunks(self, start: datetime, end: datetime) -> Iterable[tuple[datetime, datetime]]:
        cursor = start
        while cursor < end:
            next_cursor = min(cursor + timedelta(days=self._settings.backfill_chunk_days), end)
            yield cursor, next_cursor
            cursor = next_cursor

    def _retry_notifier(
        self,
        market_id: int,
        granularity: Granularity,
        metric_name: MetricName,
    ) -> Callable[[float], None]:
        def notify(retry_after_seconds: float) -> None:
            now = self._now()
            self._repository.upsert_sync_state(
                market_id=market_id,
                granularity=granularity,
                metric_name=metric_name,
                oldest_backfilled_bucket=None,
                newest_loaded_bucket=None,
                retry_after_until=now + timedelta(seconds=retry_after_seconds),
                last_attempt_at=now,
                last_error="429 Too Many Requests",
            )

        return notify

    def _retry_notifier_for_batch(
        self,
        market_ids: Sequence[int],
        granularity: Granularity,
        metric_name: MetricName,
    ) -> Callable[[float], None]:
        def notify(retry_after_seconds: float) -> None:
            for market_id in market_ids:
                self._retry_notifier(market_id, granularity, metric_name)(retry_after_seconds)

        return notify

    def _update_sync_success(
        self,
        market_id: int,
        granularity: Granularity,
        metric_name: MetricName,
        bars: Sequence[object],
    ) -> None:
        if not bars:
            return
        bucket_starts = [bar.bucket_start for bar in bars]
        self._repository.upsert_sync_state(
            market_id=market_id,
            granularity=granularity,
            metric_name=metric_name,
            oldest_backfilled_bucket=min(bucket_starts),
            newest_loaded_bucket=max(bucket_starts),
            retry_after_until=None,
            last_attempt_at=self._now(),
            last_error=None,
        )
