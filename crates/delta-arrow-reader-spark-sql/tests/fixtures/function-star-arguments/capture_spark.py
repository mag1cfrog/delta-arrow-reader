"""Capture Spark's column expansion inside function arguments."""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile


QUERIES = {
    "struct_star": "SELECT struct(*) AS r FROM numbers",
    "struct_qualified": "SELECT struct(n.*) AS r FROM numbers n",
    "hash_star": "SELECT hash(*) AS r FROM numbers",
    "hash_qualified": "SELECT hash(n.*) AS r FROM numbers n",
    "hash_beside_literal": "SELECT hash(17, n.*, 29) AS r FROM numbers n",
    "coalesce_star": "SELECT coalesce(*) AS r FROM numbers",
    "coalesce_qualified": "SELECT coalesce(n.*) AS r FROM numbers n",
    "coalesce_beside_literal": "SELECT coalesce(n.*, 99) AS r FROM numbers n",
    "concat_ws_star": "SELECT concat_ws('|', *) AS r FROM strings",
    "concat_ws_qualified": "SELECT concat_ws('|', s.*) AS r FROM strings s",
    "concat_ws_beside_literal": "SELECT concat_ws('|', 'head', s.*, 'tail') AS r FROM strings s",
    "datediff_star": "SELECT datediff(*) AS r FROM dates",
    "datediff_qualified": "SELECT datediff(d.*) AS r FROM dates d",
    "date_diff_star": "SELECT date_diff(*) AS r FROM dates",
    "nested_struct_star": "SELECT struct(details.*) AS r FROM nested",
    "nested_hash_star": "SELECT hash(details.*) AS r FROM nested",
    "nested_coalesce_star": "SELECT coalesce(details.*) AS r FROM nested",
    "nested_concat_ws_star": "SELECT concat_ws('|', details.*) AS r FROM nested",
    "using_join_hash": "SELECT hash(*) AS r FROM numbers x JOIN (SELECT a, b + 10 AS b FROM numbers) y USING (a)",
    "using_join_qualified_hash": "SELECT hash(x.*, y.*) AS r FROM numbers x JOIN (SELECT a, b + 10 AS b FROM numbers) y USING (a)",
    "count_star": "SELECT count(*) AS r FROM numbers",
    "count_distinct_star": "SELECT count(DISTINCT *) AS r FROM numbers",
    "array_star": "SELECT array(*) AS r FROM numbers",
    "concat_arrays_star": "SELECT concat(*) AS r FROM arrays",
    "aggregate_star": "SELECT aggregate(*, (acc, x) -> acc + x) AS r FROM reductions",
    "aggregate_qualified_star": "SELECT aggregate(r.*, (acc, x) -> acc + x) AS r FROM reductions r",
    "count_window_star": "SELECT count(*) OVER () AS r FROM numbers",
    'count_nested': 'SELECT count(details.*) AS r FROM nested',
    'count_distinct_nested': 'SELECT count(DISTINCT details.*) AS r FROM nested',
    'count_beside_literal': 'SELECT count(*, 1) AS r FROM numbers',
    'count_distinct_beside_literal': 'SELECT count(DISTINCT *, 1) AS r FROM numbers',
    'count_beside_null': 'SELECT count(*, NULL) AS r FROM numbers',
    'count_repeated': 'SELECT count(*, *) AS r FROM numbers',
    'count_distinct_repeated': 'SELECT count(DISTINCT *, *) AS r FROM numbers',
    'count_qualified_beside_literal': 'SELECT count(n.*, 1) AS r FROM numbers n',
    'count_distinct_qualified_beside_literal': 'SELECT count(DISTINCT n.*, 1) AS r FROM numbers n',
    'count_repeated_qualified': 'SELECT count(n.*, n.*) AS r FROM numbers n',
    'count_window_beside_literal': 'SELECT count(*, 1) OVER () AS r FROM numbers',
    'count_window_nested': 'SELECT count(details.*) OVER () AS r FROM nested',
    'count_window_repeated': 'SELECT count(*, *) OVER () AS r FROM numbers',
    "sum_window_star": "SELECT sum(*) OVER () AS r FROM (SELECT a FROM numbers)",
}


def encode(value):
    if value is None:
        return None
    if isinstance(value, (tuple, list)):
        return [encode(v) for v in value]
    if isinstance(value, datetime.datetime):
        return str(int(value.replace(tzinfo=datetime.timezone.utc).timestamp() * 1_000_000))
    return str(value).lower() if isinstance(value, bool) else str(value)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert not args.output.exists()
    os.environ["PYSPARK_PYTHON"] = sys.executable

    import pyspark
    from pyspark.sql import SparkSession, types

    spark = (SparkSession.builder.master("local[1]").appName("dar-function-star-arguments")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0"
        spark.sparkContext.setLogLevel("ERROR")
        def pair_schema(data_type):
            return types.StructType([types.StructField("a", data_type), types.StructField("b", data_type)])
        nested_type = types.StructType([types.StructField("x", types.StringType()),
                                       types.StructField("y", types.StringType())])
        definitions = [
            ("numbers", pair_schema(types.IntegerType()), [(1, 2), (None, 3), (4, None), (None, None)]),
            ("strings", pair_schema(types.StringType()), [("A", "B"), (None, "C"), ("", None), (None, None)]),
            ("dates", types.StructType([types.StructField("end_ts", types.TimestampType()),
                                        types.StructField("start_ts", types.TimestampType())]),
             [(datetime.datetime(2026, 5, 3), datetime.datetime(2026, 5, 1)),
              (datetime.datetime(2026, 5, 1), datetime.datetime(2026, 5, 3)),
              (None, datetime.datetime(2026, 5, 1)), (datetime.datetime(2026, 5, 1), None)]),
            ("nested", types.StructType([types.StructField("details", nested_type)]),
             [(("A", "B"),), ((None, "C"),), (None,)]),
            ("arrays", pair_schema(types.ArrayType(types.IntegerType())),
             [([1, 2], [10, 20]), ([1, None], [5]), (None, [2]), ([], [])]),
            ("reductions", types.StructType([types.StructField("xs", types.ArrayType(types.IntegerType())),
                                             types.StructField("initial", types.IntegerType())]),
             [([1, 2], 10), ([], 4), (None, 5), ([1, None], 2)]),
        ]
        inputs = []
        for name, schema, rows in definitions:
            spark.createDataFrame(rows, schema).createOrReplaceTempView(name)
            inputs.append({"name": name, "schema": json.loads(schema.json()),
                           "rows": [encode(row) for row in rows]})
        jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
        with ZipFile(jar) as archive:
            build = archive.read("spark-version-info.properties").decode()
        records = [{"spark_version": spark.version, "spark_build": build, "inputs": inputs,
                    "capture_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}]
        for ansi in [False, True]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in QUERIES.items():
                frame = spark.sql(sql)
                rows = [encode(row) for row in frame.collect()]
                rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                schema = json.loads(frame._jdf.queryExecution().executedPlan().schema().json())
                records.append({"id": name, "sql": sql, "ansi": ansi,
                                "schema": schema, "rows": rows})
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations": len(records) - 1}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
