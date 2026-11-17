"""Capture ordinary admission controls using only the recorded source inputs."""
import json
from pathlib import Path
import sys
sys.dont_write_bytecode=True
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent))
from capture_spark_requests import capture_requested_queries
from capture_date_range_747_spark import capture

def capture_control(session,case,encode):
    if case["id"]=="qualified":
        source=session.table("source")
        source.createOrReplaceGlobalTempView("registered_table")
        assert session.table("global_temp.registered_table").schema==source.schema
    return capture(session,case,encode)

request=HERE/"requests.json"
capture_requested_queries(json.loads(request.read_text()),Path(sys.argv[1]),[Path(__file__),request],[request],capture_query=capture_control)
