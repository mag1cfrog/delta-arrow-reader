"""Check request overlap and avoid mistaking repeated reads for SDK retries."""

import json
from pathlib import Path
import tempfile
import unittest

from intra_page import network_metrics
from run import query_count
from supervise import Progress


class NetworkMetricsTest(unittest.TestCase):
    def test_concurrent_completion_keeps_one_deadline(self):
        payload = {"execution_mode": "reuse", "comparison_revision": 6,
                   "purpose": "timing", "concurrent_queries": 4}
        progress = Progress(payload, 30, 2)

        def send(phase, index=None):
            query = ({"query_index": index, "output_rows": 894, "output_batches": 2,
                      "completion_ns": 10, "first_batch_ns": 3} if phase == "query_end" else None)
            progress.receive({"phase": phase, "query_index": index, "query": query,
                              "initialization_ns": 1})

        send("initialization")
        send("concurrent_queries")
        deadline = progress.deadline
        send("query_end", 2)
        self.assertEqual(progress.deadline, deadline)
        self.assertEqual(progress.phase, "concurrent_queries")
        with self.assertRaises(ValueError):
            send("query_end", 2)
        for index in (0, 3, 1):
            send("query_end", index)
        self.assertEqual(progress.phase, "cleanup")
        self.assertEqual([q["query_index"] for q in progress.queries], [0, 1, 2, 3])
        for count in (0, 1, 3, True, "4"):
            with self.subTest(count=count), self.assertRaises(ValueError):
                query_count(payload | {"concurrent_queries": count})
        with self.assertRaises(ValueError):
            query_count(payload | {"execution_mode": "open"})
        with self.assertRaises(ValueError):
            query_count(payload | {"comparison_revision": 5})

    def test_overlapping_and_repeated_ranges(self):
        base = {"method": "GET", "object": "part.parquet", "range": "bytes=0-7",
                "incomplete_body": False, "status": 206}
        records = [base | {"started_ns": start, "ended_ns": end}
                   for start, end in ((1, 4), (2, 3), (4, 5))]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "requests.jsonl"
            path.write_text("\n".join(json.dumps(r) for r in records))
            metrics = network_metrics({"requests": 3, "response_bytes": 24, "by_class": {}}, path)
        self.assertEqual(metrics["peak_active_requests"], 2)
        self.assertEqual(metrics["repeated_identical_requests"], 2)
        self.assertIsNone(metrics["retries"])
        self.assertIsNone(network_metrics(None, None))


if __name__ == "__main__":
    unittest.main()
