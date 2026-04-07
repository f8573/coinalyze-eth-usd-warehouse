from __future__ import annotations

import io
import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from realtime_querydb.coinalyze import CoinalyzeClient
from realtime_querydb.models import Granularity, MetricName


class FakeResponse:
    def __init__(self, payload: object) -> None:
        self._payload = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class FakeOpener:
    def __init__(self, responses):
        self._responses = iter(responses)

    def open(self, request):
        response = next(self._responses)
        if isinstance(response, Exception):
            raise response
        return response


class CoinalyzeClientTests(unittest.TestCase):
    def test_retry_after_notifier_and_sleep_are_used(self) -> None:
        sleeps: list[float] = []
        notifications: list[float] = []
        responses = [
            HTTPError(
                url="https://example.com",
                code=429,
                msg="Too Many Requests",
                hdrs={"Retry-After": "2.5"},
                fp=io.BytesIO(b'{"message":"Too Many Requests"}'),
            ),
            FakeResponse([{"symbol": "ETHUSD_PERP.A", "history": []}]),
        ]
        client = CoinalyzeClient(
            "test-key",
            opener=FakeOpener(responses),
            sleep=sleeps.append,
            monotonic=lambda: 0.0,
            random_value=lambda: 0.0,
        )

        result = client.get_history(
            symbols=["ETHUSD_PERP.A"],
            metric_name=MetricName.FUNDING,
            granularity=Granularity.ONE_HOUR,
            start=datetime(2026, 4, 1, tzinfo=timezone.utc),
            end_inclusive=datetime(2026, 4, 2, tzinfo=timezone.utc),
            on_retry_after=notifications.append,
        )

        self.assertEqual(result, {"ETHUSD_PERP.A": []})
        self.assertEqual(notifications, [2.5])
        self.assertEqual(sleeps, [2.5])
