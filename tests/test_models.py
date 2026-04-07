from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from realtime_querydb.models import Granularity, MetricName, OhlcvBar, SeriesWindow, UTC, compute_shared_window


class ModelTests(unittest.TestCase):
    def test_compute_shared_window_uses_latest_start_and_earliest_end(self) -> None:
        windows = [
            SeriesWindow(
                metric_name=MetricName.OHLCV,
                granularity=Granularity.FIFTEEN_MIN,
                start=datetime(2026, 1, 1, 0, 15, tzinfo=UTC),
                end_exclusive=datetime(2026, 1, 10, 0, 0, tzinfo=UTC),
                point_count=100,
            ),
            SeriesWindow(
                metric_name=MetricName.FUNDING,
                granularity=Granularity.DAILY,
                start=datetime(2026, 1, 2, 0, 0, tzinfo=UTC),
                end_exclusive=datetime(2026, 1, 8, 0, 0, tzinfo=UTC),
                point_count=8,
            ),
        ]

        shared = compute_shared_window(windows)

        self.assertEqual(
            shared,
            (
                datetime(2026, 1, 2, 0, 0, tzinfo=UTC),
                datetime(2026, 1, 8, 0, 0, tzinfo=UTC),
            ),
        )

    def test_ohlcv_bar_nulls_buy_side_fields_when_provider_has_no_breakdown(self) -> None:
        payload = {
            "t": int(datetime(2026, 4, 4, 0, 0, tzinfo=UTC).timestamp()),
            "o": 1,
            "h": 2,
            "l": 0.5,
            "c": 1.5,
            "v": 100,
            "bv": 0,
            "tx": 0,
            "btx": 0,
        }

        bar = OhlcvBar.from_api(payload, has_buy_sell_data=False)

        self.assertIsNone(bar.buy_volume)
        self.assertIsNone(bar.trade_count)
        self.assertIsNone(bar.buy_trade_count)
