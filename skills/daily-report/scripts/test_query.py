import sys
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))

from query import (
    build_time_ranges,
    get_clickhouse_config,
    load_token,
    query_collection,
    query_collection_by_supplier,
    query_collector_timeslots,
    query_labeling,
    render,
    render_collector_timeslots,
    render_supplier,
)


class DailyReportTests(unittest.TestCase):
    def test_load_token_reads_from_config(self):
        token = load_token({"token": "abc"})
        self.assertEqual(token, "abc")

    def test_get_clickhouse_config_reads_from_config(self):
        config = {
            "clickhouse": {
                "host": "127.0.0.1",
                "port": 9000,
                "database": "asset",
                "user": "tester",
                "password": "secret",
            }
        }
        result = get_clickhouse_config(config)
        self.assertEqual(result["host"], "127.0.0.1")
        self.assertEqual(result["user"], "tester")
        self.assertEqual(result["password"], "secret")

    def test_get_clickhouse_config_supports_env_override(self):
        config = {"clickhouse": {"host": "ignored", "user": "ignored", "password": "ignored"}}
        with patch.dict(
            "os.environ",
            {
                "DAILY_REPORT_CH_HOST": "env-host",
                "DAILY_REPORT_CH_PORT": "9440",
                "DAILY_REPORT_CH_DB": "env-db",
                "DAILY_REPORT_CH_USER": "env-user",
                "DAILY_REPORT_CH_PASS": "env-pass",
            },
            clear=True,
        ):
            result = get_clickhouse_config(config)
        self.assertEqual(result["host"], "env-host")
        self.assertEqual(result["port"], 9440)
        self.assertEqual(result["database"], "env-db")
        self.assertEqual(result["user"], "env-user")
        self.assertEqual(result["password"], "env-pass")

    def test_get_clickhouse_config_uses_defaults_when_missing(self):
        result = get_clickhouse_config({})
        self.assertEqual(result["host"], "10.23.206.206")
        self.assertEqual(result["port"], 9000)
        self.assertEqual(result["database"], "asset")
        self.assertEqual(result["user"], "guwenjie")
        self.assertTrue(result["password"])

    def test_query_collection_returns_empty_for_empty_projects(self):
        rows = query_collection(object(), date(2026, 3, 19), set(), {})
        self.assertEqual(rows, {})

    def test_query_labeling_returns_empty_for_empty_projects(self):
        rows = query_labeling(object(), date(2026, 3, 19), set(), {})
        self.assertEqual(rows, {})

    def test_query_collection_by_supplier_returns_empty_for_empty_projects(self):
        rows = query_collection_by_supplier(object(), date(2026, 3, 19), set())
        self.assertEqual(rows, [])

    def test_query_collector_timeslots_returns_empty_for_empty_projects(self):
        rows = query_collector_timeslots(object(), date(2026, 3, 19), set())
        self.assertEqual(rows, [])

    def test_render_handles_empty_sections(self):
        output = render(date(2026, 3, 19), {}, {})
        self.assertIn("今日无符合条件的采集/质检数据。", output)
        self.assertIn("今日无符合条件的标注数据。", output)

    def test_render_supplier_handles_empty_sections(self):
        output = render_supplier([])
        self.assertIn("今日无供应商采集明细。", output)

    def test_render_collector_timeslots_handles_empty_sections(self):
        output = render_collector_timeslots([])
        self.assertIn("今日无采集员时间段明细。", output)

    def test_build_time_ranges_splits_by_gap(self):
        from datetime import datetime

        points = [
            datetime.fromisoformat("2026-03-19T09:05:00"),
            datetime.fromisoformat("2026-03-19T09:20:00"),
            datetime.fromisoformat("2026-03-19T10:40:00"),
            datetime.fromisoformat("2026-03-19T11:00:00"),
        ]
        ranges = build_time_ranges(points, gap_minutes=45)
        self.assertEqual(len(ranges), 2)
        self.assertEqual(ranges[0][0].hour, 9)
        self.assertEqual(ranges[0][1].hour, 9)
        self.assertEqual(ranges[1][0].hour, 10)
        self.assertEqual(ranges[1][1].hour, 11)


if __name__ == "__main__":
    unittest.main()
