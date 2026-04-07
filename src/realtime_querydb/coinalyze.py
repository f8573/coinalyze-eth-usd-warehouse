from __future__ import annotations

import json
import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Callable, Mapping, Sequence
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, build_opener

from .models import Granularity, MetricName, Provider


class CoinalyzeClientError(RuntimeError):
    """Raised when the Coinalyze API returns a non-recoverable error."""


class CoinalyzeClient:
    BASE_URL = "https://api.coinalyze.net/v1"

    def __init__(
        self,
        api_key: str,
        *,
        min_request_interval_seconds: float = 1.0,
        max_symbols_per_request: int = 10,
        backoff_jitter_ratio: float = 0.10,
        opener=None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        random_value: Callable[[], float] = random.random,
    ) -> None:
        self._api_key = api_key
        self._min_request_interval_seconds = min_request_interval_seconds
        self._max_symbols_per_request = max_symbols_per_request
        self._backoff_jitter_ratio = backoff_jitter_ratio
        self._opener = opener or build_opener()
        self._sleep = sleep
        self._monotonic = monotonic
        self._random_value = random_value
        self._next_request_at = 0.0

    @property
    def max_symbols_per_request(self) -> int:
        return self._max_symbols_per_request

    def get_exchanges(self) -> list[Provider]:
        response = self._request_json("/exchanges")
        return [
            Provider(provider_code=item["code"], provider_name=item["name"])
            for item in response
        ]

    def get_future_markets(self) -> list[dict[str, object]]:
        return self._request_json("/future-markets")

    def get_spot_markets(self) -> list[dict[str, object]]:
        return self._request_json("/spot-markets")

    def get_history(
        self,
        *,
        symbols: Sequence[str],
        metric_name: MetricName,
        granularity: Granularity,
        start: datetime,
        end_inclusive: datetime,
        on_retry_after: Callable[[float], None] | None = None,
    ) -> dict[str, list[dict[str, object]]]:
        if not symbols:
            return {}
        if len(symbols) > self._max_symbols_per_request:
            raise ValueError(
                f"Requested {len(symbols)} symbols, but max_symbols_per_request is {self._max_symbols_per_request}."
            )
        endpoint, extra_params = self._metric_endpoint(metric_name)
        params = {
            "symbols": ",".join(symbols),
            "interval": granularity.value,
            "from": int(start.timestamp()),
            "to": int(end_inclusive.timestamp()),
            **extra_params,
        }
        response = self._request_json(endpoint, params=params, on_retry_after=on_retry_after)
        return {entry["symbol"]: entry.get("history", []) for entry in response}

    def _metric_endpoint(self, metric_name: MetricName) -> tuple[str, Mapping[str, str]]:
        if metric_name is MetricName.OHLCV:
            return "/ohlcv-history", {}
        if metric_name is MetricName.OI_NATIVE:
            return "/open-interest-history", {}
        if metric_name is MetricName.OI_USD:
            return "/open-interest-history", {"convert_to_usd": "true"}
        if metric_name is MetricName.FUNDING:
            return "/funding-rate-history", {}
        raise ValueError(f"Unsupported metric: {metric_name}")

    def _request_json(
        self,
        path: str,
        *,
        params: Mapping[str, object] | None = None,
        on_retry_after: Callable[[float], None] | None = None,
    ):
        query = f"?{urlencode(params)}" if params else ""
        url = f"{self.BASE_URL}{path}{query}"
        while True:
            self._wait_for_request_slot()
            request = Request(url, headers={"api_key": self._api_key, "Accept": "application/json"})
            try:
                with self._opener.open(request) as response:
                    payload = response.read().decode("utf-8")
                self._next_request_at = self._monotonic() + self._min_request_interval_seconds
                return json.loads(payload)
            except HTTPError as exc:
                body = exc.read().decode("utf-8", errors="replace")
                if exc.code == 429:
                    retry_after = self._retry_after_seconds(exc.headers.get("Retry-After"))
                    if on_retry_after is not None:
                        on_retry_after(retry_after)
                    delay = retry_after + (retry_after * self._backoff_jitter_ratio * self._random_value())
                    self._sleep(delay)
                    continue
                raise CoinalyzeClientError(
                    f"Coinalyze request failed with status {exc.code}: {body}"
                ) from exc

    def _wait_for_request_slot(self) -> None:
        now = self._monotonic()
        if now < self._next_request_at:
            self._sleep(self._next_request_at - now)

    @staticmethod
    def _retry_after_seconds(header_value: str | None) -> float:
        if not header_value:
            return 1.0
        try:
            value = float(header_value)
            return max(value, 0.0)
        except ValueError:
            retry_at = parsedate_to_datetime(header_value)
            now = datetime.now(timezone.utc)
            return max((retry_at - now).total_seconds(), 0.0)
