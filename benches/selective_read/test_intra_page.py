"""Check the Python experiment boundary and first-query reporting."""

from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from intra_page import run
from run import query_count


class IntraPageTest(unittest.TestCase):
    def test_two_builds_use_defaults_and_report_first_query(self):
        record = {"status": "success", "settings": {"provider": {
            "warmup": "Automatic", "scan_options": "experimental_intra_page_reads: true"}},
            "queries": [{"completion_ns": n * 10**9, "output_rows": 3} for n in (7, 4)],
            "initialization_ns": 2 * 10**9, "session_elapsed_ns": 13 * 10**9,
            "supervision": {"process_cpu_ns": 9, "peak_rss_bytes": 100}}
        with tempfile.TemporaryDirectory() as directory:
            args = SimpleNamespace(output=Path(directory), binary=Path("candidate"),
                baseline_binary=Path("baseline"), state=Path("state"), reference=Path("reference"),
                local_table=None, network_warmup=False, no_warmup=False)
            with patch("intra_page.storage.native.call", return_value={"record": record, "network": {"requests": 10}}) as invoke, \
                 patch("intra_page.save"), patch("builtins.print"):
                for mode in ("baseline", "candidate"):
                    sample = run(args, {"execution_mode": "reuse"}, mode, "timing", 0)
                    self.assertEqual(invoke.call_args.args[0], "intra-page")
                    request = invoke.call_args.args[1]
                    self.assertEqual(request["mode"], mode)
                    self.assertEqual(request["binary"], "candidate")
                    self.assertEqual(request["baseline_binary"], "baseline")
                    self.assertFalse(request["network_warmup"] or request["no_warmup"])
                    self.assertEqual(sample["seconds"], 7)
                    self.assertEqual(sample["initialization_seconds"], 2)
                    self.assertEqual(sample["query_seconds"], [7, 4])
                    self.assertEqual(sample["network"], {"requests": 10})

    def test_concurrent_request_counts(self):
        payload = {"execution_mode": "reuse", "comparison_revision": 6,
                   "purpose": "timing", "concurrent_queries": 4}
        for count in (0, 1, 3, True, "4"):
            with self.subTest(count=count), self.assertRaises(ValueError):
                query_count(payload | {"concurrent_queries": count})
        with self.assertRaises(ValueError):
            query_count(payload | {"execution_mode": "open"})
        with self.assertRaises(ValueError):
            query_count(payload | {"comparison_revision": 5})


if __name__ == "__main__":
    unittest.main()
