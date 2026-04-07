from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Iterable

UTC = timezone.utc


class Granularity(str, Enum):
    FIFTEEN_MIN = "15min"
    ONE_HOUR = "1hour"
    DAILY = "daily"

    @property
    def delta(self) -> timedelta:
        if self is Granularity.FIFTEEN_MIN:
            return timedelta(minutes=15)
        if self is Granularity.ONE_HOUR:
            return timedelta(hours=1)
        return timedelta(days=1)

    @property
    def refresh_lookback_bars(self) -> int:
        if self is Granularity.FIFTEEN_MIN:
            return 12
        if self is Granularity.ONE_HOUR:
            return 6
        return 2

    @classmethod
    def required(cls) -> tuple["Granularity", ...]:
        return (cls.FIFTEEN_MIN, cls.ONE_HOUR, cls.DAILY)


class MarketType(str, Enum):
    SPOT = "spot"
    PERP = "perp"


class ActivationStatus(str, Enum):
    CANDIDATE = "candidate"
    ACTIVE = "active"
    EXCLUDED = "excluded"


class MetricName(str, Enum):
    OHLCV = "ohlcv"
    OI_NATIVE = "oi_native"
    OI_USD = "oi_usd"
    FUNDING = "funding"


@dataclass(frozen=True)
class Provider:
    provider_code: str
    provider_name: str


@dataclass(frozen=True)
class Market:
    coinalyze_symbol: str
    provider_code: str
    market_type: MarketType
    base_asset: str
    quote_asset: str
    symbol_on_exchange: str
    is_perpetual: bool
    margined: str | None
    oi_unit: str | None
    has_buy_sell_data: bool
    has_ohlcv_data: bool
    required_granularities: tuple[Granularity, ...] = field(default_factory=Granularity.required)
    activation_status: ActivationStatus = ActivationStatus.CANDIDATE
    eligibility_checked_at: datetime | None = None
    shared_complete_from: datetime | None = None
    shared_complete_to: datetime | None = None
    exclusion_reason: str | None = None
    market_id: int | None = None

    @property
    def required_metrics(self) -> tuple[MetricName, ...]:
        if self.market_type is MarketType.SPOT:
            return (MetricName.OHLCV,)
        return (
            MetricName.OHLCV,
            MetricName.OI_NATIVE,
            MetricName.OI_USD,
            MetricName.FUNDING,
        )


@dataclass(frozen=True)
class SeriesWindow:
    metric_name: MetricName
    granularity: Granularity
    start: datetime
    end_exclusive: datetime
    point_count: int


@dataclass(frozen=True)
class OhlcvBar:
    bucket_start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    buy_volume: float | None
    trade_count: float | None
    buy_trade_count: float | None

    @classmethod
    def from_api(cls, payload: dict[str, object], has_buy_sell_data: bool) -> "OhlcvBar":
        buy_volume = float(payload["bv"]) if has_buy_sell_data else None
        trade_count = float(payload["tx"]) if has_buy_sell_data else None
        buy_trade_count = float(payload["btx"]) if has_buy_sell_data else None
        return cls(
            bucket_start=datetime.fromtimestamp(int(payload["t"]), UTC),
            open=float(payload["o"]),
            high=float(payload["h"]),
            low=float(payload["l"]),
            close=float(payload["c"]),
            volume=float(payload["v"]),
            buy_volume=buy_volume,
            trade_count=trade_count,
            buy_trade_count=buy_trade_count,
        )


@dataclass(frozen=True)
class OpenInterestBar:
    bucket_start: datetime
    native_open: float
    native_high: float
    native_low: float
    native_close: float
    usd_open: float
    usd_high: float
    usd_low: float
    usd_close: float


@dataclass(frozen=True)
class FundingRateBar:
    bucket_start: datetime
    open: float
    high: float
    low: float
    close: float

    @classmethod
    def from_api(cls, payload: dict[str, object]) -> "FundingRateBar":
        return cls(
            bucket_start=datetime.fromtimestamp(int(payload["t"]), UTC),
            open=float(payload["o"]),
            high=float(payload["h"]),
            low=float(payload["l"]),
            close=float(payload["c"]),
        )


@dataclass(frozen=True)
class SyncState:
    market_id: int
    granularity: Granularity
    metric_name: MetricName
    oldest_backfilled_bucket: datetime | None
    newest_loaded_bucket: datetime | None
    retry_after_until: datetime | None
    last_attempt_at: datetime
    last_error: str | None


def compute_shared_window(windows: Iterable[SeriesWindow]) -> tuple[datetime, datetime] | None:
    window_list = list(windows)
    if not window_list:
        return None
    start = max(window.start for window in window_list)
    end = min(window.end_exclusive for window in window_list)
    if start >= end:
        return None
    return start, end
