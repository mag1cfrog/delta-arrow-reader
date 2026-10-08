"""Check request overlap and avoid mistaking repeated reads for SDK retries."""

import json
from pathlib import Path
import tempfile
import unittest

from intra_page import network_metrics


class NetworkMetricsTest(unittest.TestCase):
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
