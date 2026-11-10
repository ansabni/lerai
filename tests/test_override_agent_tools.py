import unittest
import os
from unittest.mock import patch

from lerai.override_agent.tools import _compact_deploy_result


class CompactDeployResultTests(unittest.TestCase):
    def test_extracts_token_from_offline_diff_string(self):
        response = {
            "success": True,
            "returncode": 0,
            "offline_diff": "{'token': 'abc123', 'diffs': {'blc.csv': {'diff': '...'}}}",
        }

        compact = _compact_deploy_result(response)

        self.assertEqual(compact["ok"], True)
        self.assertEqual(compact["success"], True)
        self.assertEqual(compact["returncode"], 0)
        self.assertEqual(compact["git_leroy_output_token"], "abc123")
        self.assertNotIn("offline_diff", compact)

    def test_handles_invalid_offline_diff_string(self):
        response = {
            "success": "true",
            "returncode": "0",
            "offline_diff": "not-a-dict",
        }

        compact = _compact_deploy_result(response)

        self.assertEqual(compact["ok"], True)
        self.assertEqual(compact["success"], True)
        self.assertEqual(compact["returncode"], "0")
        self.assertNotIn("offline_token", compact)

    def test_compact_deploy_result_applies_truncation_marker(self):
        long_text = "x" * 80
        response = {
            "success": False,
            "returncode": 1,
            "message": long_text,
            "offline_run_errors": [long_text],
            "stderr": long_text,
            "offline_run_log": long_text,
        }

        with patch.dict(os.environ, {"LERAI_ERROR_TEXT_MAX_CHARS": "20"}, clear=False):
            compact = _compact_deploy_result(response)

        self.assertIn("[TRUNCATED field=message", compact["message"])
        self.assertIn("[TRUNCATED field=offline_run_errors", compact["offline_run_errors"])
        self.assertIn("[TRUNCATED field=stderr", compact["stderr"])
        self.assertIn("[TRUNCATED field=offline_run_log", compact["offline_run_log"])


if __name__ == "__main__":
    unittest.main()
