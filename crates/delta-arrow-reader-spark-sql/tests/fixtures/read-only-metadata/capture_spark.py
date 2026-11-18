"""Capture read-only metadata against explicit schemas and registered table kinds."""
import json
import os
from pathlib import Path
import sys

sys.dont_write_bytecode = True
DRIVER = Path(__file__).resolve()
HERE = DRIVER.parent
sys.path.insert(0, str(HERE.parent))
from capture_date_range_747_spark import capture
from capture_spark_requests import capture_requested_queries

request_path = HERE / "requests.json"
request = json.loads(request_path.read_text())
output = Path(sys.argv[1]).resolve()
warehouse = output.with_suffix(".warehouse")
warehouse.mkdir()
os.chdir(warehouse)
initialized = set()


def capture_metadata(session, case, encode):
    identity = session._jsparkSession.sessionUUID()
    if identity not in initialized:
        for sql in request["setup_sql"]:
            session.sql(sql).collect()
        initialized.add(identity)
    return capture(session, case, encode)


capture_requested_queries(
    request, output, [DRIVER, request_path], [request_path],
    capture_query=capture_metadata,
    spark_settings={"spark.sql.shuffle.partitions": "1", "spark.sql.adaptive.enabled": "false"},
)
