"""Capture configured timezone spellings using the maintained Spark driver."""
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from capture_spark_requests import capture_requested_queries
from capture_date_range_747_spark import capture

def capture_display(session, case, encode):
    session.conf.set("spark.sql.session.timeZone", case["timezone"])
    assert session.conf.get("spark.sql.session.timeZone") == case["timezone"]
    return capture(session, case, encode)


request = HERE / "display-requests.json"
capture_requested_queries(json.loads(request.read_text()), Path(sys.argv[1]),
                          [Path(__file__), request], [request], capture_query=capture_display)
