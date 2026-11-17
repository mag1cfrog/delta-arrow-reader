"""Capture bounded execution-feature controls from explicit source inputs."""
import json
from pathlib import Path
import sys
sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from capture_spark_requests import capture_requested_queries
request = HERE / "requests.json"
capture_requested_queries(json.loads(request.read_text()), Path(sys.argv[1]), [Path(__file__), request], [request])
