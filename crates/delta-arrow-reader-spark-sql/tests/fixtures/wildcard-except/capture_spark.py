"""Capture wildcard EXCEPT semantics from source SQL and registered inputs."""

import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from capture_spark_requests import capture_requested_queries

request_path = HERE / "requests.json"
capture_requested_queries(json.loads(request_path.read_text()), Path(sys.argv[1]),
                          [Path(__file__), request_path], [request_path])
