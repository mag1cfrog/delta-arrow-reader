"""Capture grouping and DISTINCT results with PySpark 4.2.0 and Java 21.

Input floats are recorded as bits, including distinct signed NaN payloads.
Complex keys are provider columns so constructor nullability is tested separately.
"""
import json
import math
import os
import struct
import sys
from pathlib import Path

from py4j.protocol import Py4JError
from pyspark.sql import SparkSession, types


def encode(value, data_type, *, input_value=False):
    if value is None:
        return None
    if isinstance(data_type, (types.FloatType, types.DoubleType)):
        if math.isnan(value) and not input_value:
            return "nan"
        return struct.pack("!f" if isinstance(data_type, types.FloatType) else "!d", value).hex()
    if isinstance(data_type, types.ArrayType):
        return [encode(item, data_type.elementType, input_value=input_value) for item in value]
    if isinstance(data_type, types.StructType):
        return [encode(item, field.dataType, input_value=input_value)
                for item, field in zip(value, data_type)]
    return str(value)


def inputs():
    for kind, value_type, encoding, nan_bits in [
        ("float", types.FloatType(), "!f", ["7fc01234", "ffc04321"]),
        ("double", types.DoubleType(), "!d", ["7ff8000000001234", "fff8000000004321"]),
    ]:
        nan_a, nan_b = [struct.unpack(encoding, bytes.fromhex(bits))[0] for bits in nan_bits]
        finite = [-0.0, 0.0, -0.0, 1.5, 1.5, -2.0, None]
        mixed = finite + [nan_a, nan_b, nan_a, float("inf"), -float("inf"), None]
        record = types.StructType([types.StructField("value", value_type, True)])
        for shape, key_type, wrap, extra in [
            ("scalar", value_type, lambda v: v, []),
            ("array", types.ArrayType(value_type), lambda v: [v, None], [None, [], [None]]),
            ("struct", record, lambda v: (v,), [None]),
            ("nested", types.ArrayType(record), lambda v: [(v,), None], [None, [], [None]]),
            ("struct_array", types.StructType([types.StructField("values", types.ArrayType(value_type))]),
             lambda v: ([v, None],), [None, (None,), ([],)]),
        ]:
            for source, values in [("mixed", [wrap(v) for v in mixed] + extra),
                                   ("zeros", [wrap(-0.0), wrap(0.0), wrap(-0.0)]),
                                   ("nulls", [None, None]), ("empty", [])]:
                yield f"{kind}_{shape}_{source}", kind, shape, key_type, values
        yield f"{kind}_scalar_finite", kind, "scalar", value_type, finite
        yield f"{kind}_scalar_negative_zero", kind, "scalar", value_type, [-0.0, -0.0]


def queries(table, kind, shape):
    yield "count", f"SELECT COUNT(*) AS rows,COUNT(k) AS nonnull,COUNT(DISTINCT k) AS unique_keys FROM {table}"
    if shape in ["scalar", "array"]:
        yield "set", f"SELECT SORT_ARRAY(COLLECT_SET(k)) AS keys FROM {table}"
        yield "distinct_list", f"SELECT SORT_ARRAY(COLLECT_LIST(DISTINCT k)) AS keys FROM {table}"
    yield "tuples", f"SELECT COUNT(DISTINCT k,id%2) AS unique_pairs,COUNT(DISTINCT k,k) AS repeated FROM {table}"
    yield "filtered_count", f"SELECT COUNT(DISTINCT k) FILTER(WHERE id%2=0) AS unique_keys FROM {table}"
    if shape in ["scalar", "array"]:
        yield "filter", f"SELECT COUNT(DISTINCT k) FILTER(WHERE id%2=0) AS unique_keys,SORT_ARRAY(COLLECT_SET(k) FILTER(WHERE id%2=0)) AS keys FROM {table}"
    yield "group", f"SELECT k,COUNT(*) AS n FROM {table} GROUP BY k"
    if not table.endswith("empty"):
        yield "rollup", f"SELECT k,COUNT(*) AS n,GROUPING(k) AS total FROM {table} GROUP BY ROLLUP(k) ORDER BY GROUPING(k),k"
    yield "having", f"SELECT k AS key,COUNT(*) AS n FROM {table} GROUP BY k HAVING COUNT(*)>1 ORDER BY key"
    if shape == "scalar":
        yield "sum_avg", f"SELECT SUM(DISTINCT k) AS total,AVG(DISTINCT k) AS mean FROM {table}"
        yield "sum_filter", f"SELECT SUM(DISTINCT k) FILTER(WHERE id%2=0) AS total,AVG(DISTINCT k) FILTER(WHERE id%2=0) AS mean FROM {table}"
        yield "payload", f"SELECT k,SORT_ARRAY(COLLECT_LIST(CAST(k AS STRING))) AS originals,COUNT(k) AS n FROM {table} GROUP BY k"
        if not table.endswith(("mixed", "zeros")):
            yield "minmax", f"SELECT MIN(DISTINCT k) AS low,MAX(DISTINCT k) AS high FROM {table}"
    if shape in ["scalar", "array"] and table.endswith("mixed"):
        yield "window_set", f"SELECT id,SORT_ARRAY(COLLECT_SET(k) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)) AS keys FROM {table}"
    if shape in ["array", "nested"] and table.endswith("zeros"):
        yield "computed", f"SELECT COUNT(DISTINCT ARRAY(k)) AS n FROM {table}"
    if shape == "scalar" and table.endswith("mixed"):
        for label, key in [("scalar", f"CAST('-0.0' AS {kind})"),
                           ("array", f"ARRAY(CAST('-0.0' AS {kind}))")]:
            yield f"constant_set_{label}", f"SELECT SORT_ARRAY(COLLECT_SET({key})) AS keys FROM {table}"
        yield "string_sum", f"SELECT SUM(DISTINCT CAST(k AS STRING)) AS total FROM {table}"
        for label, key in [("struct", "NAMED_STRUCT('value',k)"),
                           ("nested", "ARRAY(NAMED_STRUCT('value',k))")]:
            yield f"computed_{label}", f"SELECT COUNT(DISTINCT {key}) AS n FROM {table}"
        sign_key = f"ARRAY(CAST(CASE WHEN CAST(k AS STRING)='-0.0' THEN -1 ELSE 1 END AS {kind}))"
        yield "computed_sign", f"SELECT COUNT(DISTINCT {sign_key}) AS n FROM {table}"
        yield "computed_payload", f"SELECT COUNT(DISTINCT NAMED_STRUCT('value',k,'original',CAST(k AS STRING))) AS n FROM {table}"
        for label, key in [("sign", sign_key),
                           ("payload", "NAMED_STRUCT('value',k,'original',CAST(k AS STRING))")]:
            grouped = f"SELECT {key} AS key,COUNT(*) AS n FROM {table} GROUP BY {key}"
            yield f"group_computed_{label}", f"SELECT COUNT(*) AS groups,SUM(n) AS rows FROM ({grouped}) grouped"
        yield "live_cast", f"SELECT COUNT(DISTINCT ARRAY(CAST(CASE WHEN id=0 THEN 'bad' ELSE '1.0' END AS {kind}))) AS n FROM {table}"
        yield "dead_cast", f"SELECT COUNT(DISTINCT ARRAY(IF(id<0,CAST('bad' AS {kind}),k))) AS n FROM {table}"


def constant_set_queries():
    for kind in ["float", "double"]:
        for operator in ["UNION ALL", "UNION", "INTERSECT", "EXCEPT"]:
            right = "1.0" if operator == "EXCEPT" else "0.0"
            source = f"(SELECT CAST('-0.0' AS {kind}) AS k {operator} SELECT CAST('{right}' AS {kind}) AS k) t"
            for shape, key in [("scalar", "k"), ("array", "ARRAY(k)"),
                               ("struct", "NAMED_STRUCT('value',k)"),
                               ("nested", "ARRAY(NAMED_STRUCT('value',k))")]:
                grouped = f"SELECT {key} AS key,COUNT(*) AS n FROM {source} GROUP BY {key}"
                yield f"constant_{kind}_{operator}_{shape}", f"SELECT COUNT(*) AS groups,SUM(n) AS rows,COUNT_IF(CAST(key AS STRING) LIKE '%-0.0%') AS negative_zero_keys FROM ({grouped}) grouped"


def capture_query(spark, sql):
    try:
        frame = spark.sql(sql)
        rows = [encode(row, frame.schema) for row in frame.collect()]
        rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
        return {"schema": frame.schema.jsonValue(), "rows": rows}
    except Exception as error:
        condition = getattr(error, "getCondition", lambda: None)()
        cause = getattr(error, "java_exception", None)
        while condition is None and cause is not None:
            try:
                condition = cause.getCondition()
            except Py4JError:
                pass
            cause = cause.getCause()
        assert condition is not None, (sql, str(error))
        return {"condition": condition}


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-floating-group-keys")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        metadata = {"spark_version": spark.version, "timezone": "UTC", "inputs": []}
        definitions = []
        for table, kind, shape, key_type, values in inputs():
            schema = types.StructType([types.StructField("k", key_type, True),
                                       types.StructField("id", types.IntegerType(), False)])
            rows = [(value, i) for i, value in enumerate(values)]
            spark.createDataFrame(rows, schema).createOrReplaceTempView(table)
            metadata["inputs"].append({"name": table, "schema": schema.jsonValue(),
                                       "rows": [encode(row, schema, input_value=True) for row in rows]})
            definitions.extend((f"{table}_{label}", sql) for label, sql in queries(table, kind, shape))
        records = [metadata]
        definitions.extend(constant_set_queries())
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in definitions:
                records.append({"id": name, "ansi": ansi, "sql": sql,
                                "expected": capture_query(spark, sql)})
        assert len({(r["id"], r["ansi"]) for r in records[1:]}) == len(records)-1
        Path(__file__).with_name("floating-group-keys-spark.jsonl").write_text(
            "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
        print(json.dumps({"observations": len(records)-1, "inputs": len(metadata["inputs"])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
