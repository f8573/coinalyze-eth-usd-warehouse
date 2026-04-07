from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Iterable, Iterator, Sequence

import psycopg
from psycopg.rows import dict_row

from .database import load_schema_sql
from .models import (
    ActivationStatus,
    FundingRateBar,
    Granularity,
    Market,
    MarketType,
    MetricName,
    OhlcvBar,
    OpenInterestBar,
    Provider,
    SyncState,
)


class WarehouseRepository:
    def __init__(self, database_url: str) -> None:
        self._database_url = database_url

    @contextmanager
    def _connect(self) -> Iterator[psycopg.Connection]:
        with psycopg.connect(self._database_url, row_factory=dict_row) as connection:
            yield connection

    def apply_schema(self) -> None:
        with self._connect() as connection:
            connection.execute(load_schema_sql())
            connection.commit()

    def upsert_providers(self, providers: Iterable[Provider]) -> None:
        provider_rows = [(provider.provider_code, provider.provider_name) for provider in providers]
        if not provider_rows:
            return
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO provider (provider_code, provider_name, last_discovered_at)
                    VALUES (%s, %s, now())
                    ON CONFLICT (provider_code) DO UPDATE
                    SET provider_name = excluded.provider_name,
                        last_discovered_at = now()
                    """,
                    provider_rows,
                )
            connection.commit()

    def upsert_markets(self, markets: Iterable[Market]) -> None:
        market_rows = [
            (
                market.coinalyze_symbol,
                market.provider_code,
                market.market_type.value,
                market.base_asset,
                market.quote_asset,
                market.symbol_on_exchange,
                market.is_perpetual,
                market.margined,
                market.oi_unit,
                market.has_buy_sell_data,
                market.has_ohlcv_data,
                [granularity.value for granularity in market.required_granularities],
            )
            for market in markets
        ]
        if not market_rows:
            return
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO market (
                        coinalyze_symbol,
                        provider_code,
                        market_type,
                        base_asset,
                        quote_asset,
                        symbol_on_exchange,
                        is_perpetual,
                        margined,
                        oi_unit,
                        has_buy_sell_data,
                        has_ohlcv_data,
                        last_discovered_at,
                        required_granularities
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s)
                    ON CONFLICT (coinalyze_symbol) DO UPDATE
                    SET provider_code = excluded.provider_code,
                        market_type = excluded.market_type,
                        base_asset = excluded.base_asset,
                        quote_asset = excluded.quote_asset,
                        symbol_on_exchange = excluded.symbol_on_exchange,
                        is_perpetual = excluded.is_perpetual,
                        margined = excluded.margined,
                        oi_unit = excluded.oi_unit,
                        has_buy_sell_data = excluded.has_buy_sell_data,
                        has_ohlcv_data = excluded.has_ohlcv_data,
                        last_discovered_at = now(),
                        required_granularities = excluded.required_granularities
                    """,
                    market_rows,
                )
            connection.commit()

    def exclude_undiscovered_markets(self, current_symbols: Sequence[str]) -> None:
        with self._connect() as connection:
            if current_symbols:
                connection.execute(
                    """
                    UPDATE market
                    SET activation_status = 'excluded',
                        exclusion_reason = 'delisted_or_removed'
                    WHERE coinalyze_symbol <> ALL(%s)
                    """,
                    (list(current_symbols),),
                )
            else:
                connection.execute(
                    """
                    UPDATE market
                    SET activation_status = 'excluded',
                        exclusion_reason = 'delisted_or_removed'
                    """
                )
            connection.commit()

    def list_markets(self, status: ActivationStatus | None = None) -> list[Market]:
        query = """
            SELECT
                market_id,
                coinalyze_symbol,
                provider_code,
                market_type,
                base_asset,
                quote_asset,
                symbol_on_exchange,
                is_perpetual,
                margined,
                oi_unit,
                has_buy_sell_data,
                has_ohlcv_data,
                last_discovered_at,
                required_granularities,
                activation_status,
                eligibility_checked_at,
                shared_complete_from,
                shared_complete_to,
                exclusion_reason
            FROM market
        """
        params: Sequence[object] = ()
        if status is not None:
            query += " WHERE activation_status = %s"
            params = (status.value,)
        query += " ORDER BY provider_code, market_type, coinalyze_symbol"
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()
        markets: list[Market] = []
        for row in rows:
            markets.append(
                Market(
                    market_id=row["market_id"],
                    coinalyze_symbol=row["coinalyze_symbol"],
                    provider_code=row["provider_code"],
                    market_type=MarketType(row["market_type"]),
                    base_asset=row["base_asset"],
                    quote_asset=row["quote_asset"],
                    symbol_on_exchange=row["symbol_on_exchange"],
                    is_perpetual=row["is_perpetual"],
                    margined=row["margined"],
                    oi_unit=row["oi_unit"],
                    has_buy_sell_data=row["has_buy_sell_data"],
                    has_ohlcv_data=row["has_ohlcv_data"],
                    required_granularities=tuple(Granularity(value) for value in row["required_granularities"]),
                    activation_status=ActivationStatus(row["activation_status"]),
                    eligibility_checked_at=row["eligibility_checked_at"],
                    shared_complete_from=row["shared_complete_from"],
                    shared_complete_to=row["shared_complete_to"],
                    exclusion_reason=row["exclusion_reason"],
                )
            )
        return markets

    def list_sync_states(self, market_id: int) -> dict[tuple[Granularity, MetricName], SyncState]:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        market_id,
                        granularity,
                        metric_name,
                        oldest_backfilled_bucket,
                        newest_loaded_bucket,
                        retry_after_until,
                        last_attempt_at,
                        last_error
                    FROM granularity_sync_state
                    WHERE market_id = %s
                    """,
                    (market_id,),
                )
                rows = cursor.fetchall()
        states: dict[tuple[Granularity, MetricName], SyncState] = {}
        for row in rows:
            granularity = Granularity(row["granularity"])
            metric_name = MetricName(row["metric_name"])
            states[(granularity, metric_name)] = SyncState(
                market_id=row["market_id"],
                granularity=granularity,
                metric_name=metric_name,
                oldest_backfilled_bucket=row["oldest_backfilled_bucket"],
                newest_loaded_bucket=row["newest_loaded_bucket"],
                retry_after_until=row["retry_after_until"],
                last_attempt_at=row["last_attempt_at"],
                last_error=row["last_error"],
            )
        return states

    def set_market_activation(
        self,
        *,
        market_id: int,
        status: ActivationStatus,
        checked_at: datetime,
        shared_from: datetime | None,
        shared_to: datetime | None,
        exclusion_reason: str | None,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE market
                SET activation_status = %s,
                    eligibility_checked_at = %s,
                    shared_complete_from = %s,
                    shared_complete_to = %s,
                    exclusion_reason = %s
                WHERE market_id = %s
                """,
                (status.value, checked_at, shared_from, shared_to, exclusion_reason, market_id),
            )
            connection.commit()

    def upsert_sync_state(
        self,
        *,
        market_id: int,
        granularity: Granularity,
        metric_name: MetricName,
        oldest_backfilled_bucket: datetime | None,
        newest_loaded_bucket: datetime | None,
        retry_after_until: datetime | None,
        last_attempt_at: datetime,
        last_error: str | None,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO granularity_sync_state (
                    market_id,
                    granularity,
                    metric_name,
                    oldest_backfilled_bucket,
                    newest_loaded_bucket,
                    retry_after_until,
                    last_attempt_at,
                    last_error
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (market_id, granularity, metric_name) DO UPDATE
                SET oldest_backfilled_bucket = COALESCE(
                        LEAST(granularity_sync_state.oldest_backfilled_bucket, excluded.oldest_backfilled_bucket),
                        granularity_sync_state.oldest_backfilled_bucket,
                        excluded.oldest_backfilled_bucket
                    ),
                    newest_loaded_bucket = COALESCE(
                        GREATEST(granularity_sync_state.newest_loaded_bucket, excluded.newest_loaded_bucket),
                        granularity_sync_state.newest_loaded_bucket,
                        excluded.newest_loaded_bucket
                    ),
                    retry_after_until = excluded.retry_after_until,
                    last_attempt_at = excluded.last_attempt_at,
                    last_error = excluded.last_error
                """,
                (
                    market_id,
                    granularity.value,
                    metric_name.value,
                    oldest_backfilled_bucket,
                    newest_loaded_bucket,
                    retry_after_until,
                    last_attempt_at,
                    last_error,
                ),
            )
            connection.commit()

    def upsert_ohlcv_bars(
        self,
        *,
        market_id: int,
        granularity: Granularity,
        bars: Sequence[OhlcvBar],
    ) -> None:
        if not bars:
            return
        rows = [
            (
                market_id,
                granularity.value,
                bar.bucket_start,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
                bar.buy_volume,
                bar.trade_count,
                bar.buy_trade_count,
            )
            for bar in bars
        ]
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO ohlcv_fact (
                        market_id,
                        granularity,
                        bucket_start,
                        open,
                        high,
                        low,
                        close,
                        volume,
                        buy_volume,
                        trade_count,
                        buy_trade_count
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (market_id, granularity, bucket_start) DO UPDATE
                    SET open = excluded.open,
                        high = excluded.high,
                        low = excluded.low,
                        close = excluded.close,
                        volume = excluded.volume,
                        buy_volume = excluded.buy_volume,
                        trade_count = excluded.trade_count,
                        buy_trade_count = excluded.buy_trade_count
                    """,
                    rows,
                )
            connection.commit()

    def upsert_open_interest_bars(
        self,
        *,
        market_id: int,
        granularity: Granularity,
        bars: Sequence[OpenInterestBar],
    ) -> None:
        if not bars:
            return
        rows = [
            (
                market_id,
                granularity.value,
                bar.bucket_start,
                bar.native_open,
                bar.native_high,
                bar.native_low,
                bar.native_close,
                bar.usd_open,
                bar.usd_high,
                bar.usd_low,
                bar.usd_close,
            )
            for bar in bars
        ]
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO open_interest_fact (
                        market_id,
                        granularity,
                        bucket_start,
                        native_open,
                        native_high,
                        native_low,
                        native_close,
                        usd_open,
                        usd_high,
                        usd_low,
                        usd_close
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (market_id, granularity, bucket_start) DO UPDATE
                    SET native_open = excluded.native_open,
                        native_high = excluded.native_high,
                        native_low = excluded.native_low,
                        native_close = excluded.native_close,
                        usd_open = excluded.usd_open,
                        usd_high = excluded.usd_high,
                        usd_low = excluded.usd_low,
                        usd_close = excluded.usd_close
                    """,
                    rows,
                )
            connection.commit()

    def upsert_funding_rate_bars(
        self,
        *,
        market_id: int,
        granularity: Granularity,
        bars: Sequence[FundingRateBar],
    ) -> None:
        if not bars:
            return
        rows = [
            (
                market_id,
                granularity.value,
                bar.bucket_start,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
            )
            for bar in bars
        ]
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO funding_rate_fact (
                        market_id,
                        granularity,
                        bucket_start,
                        open,
                        high,
                        low,
                        close
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (market_id, granularity, bucket_start) DO UPDATE
                    SET open = excluded.open,
                        high = excluded.high,
                        low = excluded.low,
                        close = excluded.close
                    """,
                    rows,
                )
            connection.commit()
