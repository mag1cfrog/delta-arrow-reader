"""Independent Spark 4.2.0 probes for sort expression lowering and display names."""

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

FIXTURES = Path(__file__).resolve().parent
sys.path.insert(0, str(FIXTURES))

from pyspark.sql import SparkSession
from capture_floating_group_keys_spark import capture_query


def capture(spark, sql):
    result = capture_query(spark, sql)
    return {"status": "error" if "condition" in result else "ok", **result}


def queries():
    for name, sort in [
        ("direct", "p.A"),
        ("hypot_expansion", "hypot(p.a,p.a)"),
        ("between_expansion", "p.a BETWEEN 1 AND 3"),
        ("log_expansion", "ln(p.a)"),
    ]:
        yield name, (f"SELECT (SELECT p.a FROM VALUES (1) q(x) ORDER BY {sort} LIMIT 1) "
                     "FROM VALUES (2) p(a)")
    for name, expression in [
        ("hypot_same_projection", "hypot(p.a,p.a)"),
        ("between_same_projection", "p.a BETWEEN 1 AND 3"),
    ]:
        yield name, (f"SELECT (SELECT {expression} FROM VALUES (1) q(x) "
                     f"ORDER BY {expression} LIMIT 1) FROM VALUES (2) p(a)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-review-790-connected")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        records = [{"spark_version": spark.version, "timezone": "UTC", "inputs": [],
                    "capture_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    "helper_script_sha256": hashlib.sha256((FIXTURES / "capture_floating_group_keys_spark.py").read_bytes()).hexdigest()}]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in queries():
                records.append({"id": name, "ansi": ansi, "sql": sql,
                                "expected": capture(spark, sql)})
        assert len({(r["id"], r["ansi"]) for r in records[1:]}) == len(records)-1
        with args.output.open("x") as output:
            output.write("".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
        print(json.dumps({"observations": len(records)-1,
                          "errors": sum(r.get("expected", {}).get("status") == "error" for r in records)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
