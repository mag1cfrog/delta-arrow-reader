"""Capture distribution values and result types from explicit duplicate inputs."""
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from capture_spark_requests import capture_requested_queries

request = HERE / "distribution-requests.json"
capture_requested_queries(
    json.loads(request.read_text()), Path(sys.argv[1]), [Path(__file__), request], [request],
    spark_settings={"spark.sql.shuffle.partitions": "3", "spark.sql.adaptive.enabled": "false"},
)
