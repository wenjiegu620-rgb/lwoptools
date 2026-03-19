import io
import sys
import unittest
from contextlib import redirect_stderr
from datetime import date
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))

from query import (
    get_clickhouse_config,
    load_token,
    query_collection,
    query_collection_by_supplier,
    query_labeling,
    render,
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

    def test_get_clickhouse_config_fails_when_missing_required_fields(self):
        stderr = io.StringIO()
        with redirect_stderr(stderr), self.assertRaises(SystemExit):
            get_clickhouse_config({})
        self.assertIn("Clickhouse 配置缺失", stderr.getvalue())

    def test_query_collection_returns_empty_for_empty_projects(self):
        rows = query_collection(object(), date(2026, 3, 19), set(), {})
        self.assertEqual(rows, {})

    def test_query_labeling_returns_empty_for_empty_projects(self):
        rows = query_labeling(object(), date(2026, 3, 19), set(), {})
        self.assertEqual(rows, {})

    def test_query_collection_by_supplier_returns_empty_for_empty_projects(self):
        rows = query_collection_by_supplier(object(), date(2026, 3, 19), set())
        self.assertEqual(rows, [])

    def test_render_handles_empty_sections(self):
        output = render(date(2026, 3, 19), {}, {})
        self.assertIn("今日无符合条件的采集/质检数据。", output)
        self.assertIn("今日无符合条件的标注数据。", output)

    def test_render_supplier_handles_empty_sections(self):
        output = render_supplier([])
        self.assertIn("今日无供应商采集明细。", output)


if __name__ == "__main__":
    unittest.main()
