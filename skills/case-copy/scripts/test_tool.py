import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from tool import (
    COPY_FAILED_LABEL,
    COPY_PENDING_LABEL,
    COPY_SUCCESS_LABEL,
    AssetAPI,
    build_report_rows,
    normalize_env,
    summarize_copy_result,
)


class CaseCopyTests(unittest.TestCase):
    def test_normalize_env_accepts_case_insensitive_values(self):
        self.assertEqual(normalize_env("Prod"), "prod")
        self.assertEqual(normalize_env(" DEV "), "dev")

    def test_normalize_env_rejects_invalid_values(self):
        with self.assertRaises(ValueError):
            normalize_env("staging")

    def test_asset_api_uses_normalized_env(self):
        api = AssetAPI("token", "tester", "Prod")
        self.assertEqual(api.base, "https://assetserver.lightwheel.net")

    def test_summarize_copy_result_tracks_confirmed_failed_and_unknown(self):
        result = {
            "success": True,
            "data": {
                "successIds": ["case-1"],
                "failedIds": ["case-2"],
            },
        }
        summary = summarize_copy_result(result, ["case-1", "case-2", "case-3"])
        self.assertEqual(summary["confirmed_ids"], {"case-1"})
        self.assertEqual(summary["failed_ids"], {"case-2"})
        self.assertEqual(summary["unknown_ids"], {"case-3"})

    def test_summarize_copy_result_marks_all_unknown_when_api_lacks_item_status(self):
        result = {"success": True, "data": {"message": "ok"}}
        summary = summarize_copy_result(result, ["case-1", "case-2"])
        self.assertEqual(summary["confirmed_ids"], set())
        self.assertEqual(summary["failed_ids"], set())
        self.assertEqual(summary["unknown_ids"], {"case-1", "case-2"})

    def test_build_report_rows_includes_copy_status(self):
        success_cases = [{"id": "case-1", "name": "ok"}]
        fail_cases = [{"id": "case-2", "name": "bad"}, {"id": "case-3", "name": "pending"}]
        rows = build_report_rows(
            success_cases,
            fail_cases,
            {
                "confirmed_ids": {"case-1"},
                "failed_ids": {"case-2"},
                "unknown_ids": {"case-3"},
            },
        )
        status_by_id = {row["case_uuid"]: row["copy_status"] for row in rows}
        self.assertEqual(status_by_id["case-1"], COPY_SUCCESS_LABEL)
        self.assertEqual(status_by_id["case-2"], COPY_FAILED_LABEL)
        self.assertEqual(status_by_id["case-3"], COPY_PENDING_LABEL)


if __name__ == "__main__":
    unittest.main()
