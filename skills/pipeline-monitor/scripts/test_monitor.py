"""
pipeline-monitor 单元测试
不依赖真实 Clickhouse，通过 mock 验证核心逻辑。
"""

import sys
import json
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, call

# 把 scripts 目录加入 path，使 monitor/query 可 import
sys.path.insert(0, str(Path(__file__).parent))
import monitor
import query


# ──────────────────────────────────────────────
# 1. is_in_silence：Bug 修复验证
# ──────────────────────────────────────────────

class TestIsInSilence(unittest.TestCase):

    def test_key_not_present_returns_false(self):
        self.assertFalse(monitor.is_in_silence({}, "proj:node", 2))

    def test_no_last_alert_time_returns_false(self):
        """Bug 修复：key 存在但只有 consecutive_growth，不应崩掉"""
        alerts_sent = {"proj:node": {"consecutive_growth": 1}}
        self.assertFalse(monitor.is_in_silence(alerts_sent, "proj:node", 2))

    def test_within_silence_window_returns_true(self):
        recent = (datetime.now() - timedelta(hours=1)).isoformat(timespec="seconds")
        alerts_sent = {"proj:node": {"last_alert_time": recent, "consecutive_growth": 1}}
        self.assertTrue(monitor.is_in_silence(alerts_sent, "proj:node", 2))

    def test_expired_silence_returns_false(self):
        old = (datetime.now() - timedelta(hours=3)).isoformat(timespec="seconds")
        alerts_sent = {"proj:node": {"last_alert_time": old, "consecutive_growth": 1}}
        self.assertFalse(monitor.is_in_silence(alerts_sent, "proj:node", 2))


# ──────────────────────────────────────────────
# 2. check_alerts：连续增长计数 + 静默期
# ──────────────────────────────────────────────

BASE_CONFIG = {
    "alert": {
        "observe_threshold": 10,
        "observe_rate": 0.15,
        "warn_threshold": 30,
        "warn_growth": 20,
        "critical_threshold": 50,
        "critical_consecutive": 2,
        "silence_hours": 2,
    },
    "node_owners": {},
}


class TestCheckAlerts(unittest.TestCase):

    def _make_snapshot(self, failed=0, consecutive=0, last_alert_time=None):
        entry = {"consecutive_growth": consecutive}
        if last_alert_time:
            entry["last_alert_time"] = last_alert_time
        return {
            "time": None,
            "data": {"proj": {"data_cut": {"failed": failed, "pending": 0}}},
            "history": [],
            "alerts_sent": {"proj:data_cut": entry} if (consecutive or last_alert_time) else {},
        }

    def test_no_alert_below_threshold(self):
        current = {"proj": {"data_cut": {"failed": 20, "pending": 0}}}
        snapshot = self._make_snapshot(failed=0)
        alerts, _ = monitor.check_alerts(current, snapshot, BASE_CONFIG)
        self.assertEqual(alerts, [])

    def test_warn_alert_triggered(self):
        current = {"proj": {"data_cut": {"failed": 35, "pending": 0}}}
        # history 中 1h 前失败数为 10，增速 = 25 > 20
        snapshot = {
            "time": None,
            "data": {"proj": {"data_cut": {"failed": 10, "pending": 0}}},
            "history": [{"time": "x", "data": {"proj": {"data_cut": {"failed": 10, "pending": 0}}}}],
            "alerts_sent": {},
        }
        alerts, _ = monitor.check_alerts(current, snapshot, BASE_CONFIG)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["level"], "warn")

    def test_critical_alert_requires_consecutive_growth(self):
        current = {"proj": {"data_cut": {"failed": 60, "pending": 0}}}
        # consecutive_growth=1，只有 1 次增长，不够 critical_consecutive=2
        snapshot = self._make_snapshot(failed=55, consecutive=1)
        alerts, _ = monitor.check_alerts(current, snapshot, BASE_CONFIG)
        # 应升为 critical（本轮增长使 consecutive=2）
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["level"], "critical")

    def test_silence_suppresses_repeat_alert(self):
        recent = (datetime.now() - timedelta(hours=1)).isoformat(timespec="seconds")
        current = {"proj": {"data_cut": {"failed": 60, "pending": 0}}}
        snapshot = self._make_snapshot(failed=55, consecutive=2, last_alert_time=recent)
        alerts, _ = monitor.check_alerts(current, snapshot, BASE_CONFIG)
        self.assertEqual(alerts, [])

    def test_consecutive_growth_written_even_without_alert(self):
        """每轮都应更新 consecutive_growth，即使未报警（为下一轮判断准备）"""
        current = {"proj": {"data_cut": {"failed": 15, "pending": 0}}}
        snapshot = self._make_snapshot(failed=10)
        _, new_alerts_sent = monitor.check_alerts(current, snapshot, BASE_CONFIG)
        self.assertIn("proj:data_cut", new_alerts_sent)
        self.assertEqual(new_alerts_sent["proj:data_cut"]["consecutive_growth"], 1)
        # 关键：没有 last_alert_time，下一轮 is_in_silence 不应崩
        self.assertNotIn("last_alert_time", new_alerts_sent["proj:data_cut"])


# ──────────────────────────────────────────────
# 3. SQL 参数化：验证不拼接用户输入
# ──────────────────────────────────────────────

class TestSQLParameterization(unittest.TestCase):

    def _mock_client(self):
        client = MagicMock()
        client.execute.return_value = []
        return client

    def test_query_status_with_node_uses_params(self):
        client = self._mock_client()
        query.query_status(client, "DM_sample", "data_cut")
        sql, params = client.execute.call_args[0]
        # node_name 不应硬编码在 SQL 字符串中
        self.assertNotIn("data_cut", sql)
        self.assertEqual(params["node_name"], "data_cut")
        self.assertIn("%DM_sample%", params["keyword"])

    def test_query_status_without_node_no_node_param(self):
        client = self._mock_client()
        query.query_status(client, "DM_sample")
        sql, params = client.execute.call_args[0]
        self.assertNotIn("node_name", params)

    def test_query_trend_with_node_uses_params(self):
        client = self._mock_client()
        query.query_trend(client, "DM_sample", "data_cut")
        sql, params = client.execute.call_args[0]
        self.assertNotIn("data_cut", sql)
        self.assertEqual(params["node_name"], "data_cut")

    def test_query_trend_without_node_no_node_param(self):
        client = self._mock_client()
        query.query_trend(client, "DM_sample")
        sql, params = client.execute.call_args[0]
        self.assertNotIn("node_name", params)

    def _mock_ck_module(self, mock_client):
        """构造假 clickhouse_driver 模块注入 sys.modules"""
        import types
        fake_ck = types.ModuleType("clickhouse_driver")
        fake_ck.Client = MagicMock(return_value=mock_client)
        return fake_ck

    def test_monitor_project_filter_uses_params(self):
        """monitored_projects 关键字通过 %(keywords)s 传递，不拼 SQL"""
        config = {
            "clickhouse": {
                "host": "localhost", "port": 9000,
                "database": "asset", "user": "u", "password": "p",
            },
            "monitored_projects": ["DM_sample", "orange_wrist"],
        }
        mock_client = self._mock_client()
        with patch.dict(sys.modules, {"clickhouse_driver": self._mock_ck_module(mock_client)}):
            monitor.query_clickhouse(config)
        sql, params = mock_client.execute.call_args[0]
        self.assertNotIn("DM_sample", sql)
        self.assertNotIn("orange_wrist", sql)
        self.assertIn("keywords", params)
        self.assertEqual(params["keywords"], ["DM_sample", "orange_wrist"])

    def test_monitor_all_projects_no_params(self):
        """monitored_projects=all 时不传 keywords 参数"""
        config = {
            "clickhouse": {
                "host": "localhost", "port": 9000,
                "database": "asset", "user": "u", "password": "p",
            },
            "monitored_projects": ["all"],
        }
        mock_client = self._mock_client()
        with patch.dict(sys.modules, {"clickhouse_driver": self._mock_ck_module(mock_client)}):
            monitor.query_clickhouse(config)
        sql, params = mock_client.execute.call_args[0]
        self.assertNotIn("keywords", params)
        self.assertNotIn("multiSearchAnyCaseInsensitive", sql)


if __name__ == "__main__":
    unittest.main(verbosity=2)
