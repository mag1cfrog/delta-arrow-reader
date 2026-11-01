"""Capture SORT_ARRAY and ARRAY_JOIN values and complete schemas with Spark 4.2.0."""
import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession, types


PROFILES = {
    "nonnull": (False, False, False, True),
    "nullable": (True, True, True, True),
    "array_nullable": (True, False, False, True),
    "delimiter_nullable": (False, True, False, True),
    "replacement_nullable": (False, False, True, True),
    "nonnull_elements": (False, False, False, False),
}
ROWS = [
    [0, ["b", None, "a", "b"], ",", "?"],
    [1, [], ":", "-"],
    [2, [None], "|", "~"],
    [3, ["c"], "", "NULL"],
    [4, None, ",", "x"],
    [5, ["a", None], None, "?"],
    [6, ["b", None], ",", None],
    [7, [], ",", None],
]
EXPRESSIONS = {
    "sort_asc": "SORT_ARRAY(a)",
    "sort_desc": "SORT_ARRAY(a,false)",
    "join": "ARRAY_JOIN(a,d)",
    "join_replacement": "ARRAY_JOIN(a,d,r)",
    "join_null_replacement": "ARRAY_JOIN(a,d,CAST(NULL AS STRING))",
    "join_null_delimiter": "ARRAY_JOIN(a,CAST(NULL AS STRING),r)",
    "composed": "ARRAY_JOIN(SORT_ARRAY(a),d,r)",
    "empty_input": "ARRAY_JOIN(SORT_ARRAY(a),d)",
}
SCALAR_EXPRESSIONS = {
    "join_null_array": "ARRAY_JOIN(NULL, ',')",
    "join_null_array_replacement": "ARRAY_JOIN(NULL, ',', NULL)",
    "join_null_replacement": "ARRAY_JOIN(ARRAY('a', NULL), ',', NULL)",
    "join_replacement": "ARRAY_JOIN(ARRAY('a', NULL), ',', '?')",
    "join": "ARRAY_JOIN(ARRAY('a', NULL), ',')",
    "sort_null": "SORT_ARRAY(NULL)",
    "sort": "SORT_ARRAY(ARRAY('b', NULL, 'a'))",
    "join_null_delimiter": "ARRAY_JOIN(ARRAY('a'), NULL)",
}


def encode_value(value):
    if value is None:
        return None
    if isinstance(value, list):
        return [encode_value(element) for element in value]
    return str(value)


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-array-sort-join")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        tables = []
        for name, (array_null, delimiter_null, replacement_null, element_null) in PROFILES.items():
            rows = []
            for index, array, delimiter, replacement in ROWS:
                if not array_null and array is None:
                    array = []
                if not element_null and array is not None:
                    array = [element for element in array if element is not None]
                rows.append([index, array, delimiter if delimiter_null or delimiter is not None else "",
                             replacement if replacement_null or replacement is not None else "missing"])
            schema = types.StructType([
                types.StructField("id", types.IntegerType(), False),
                types.StructField("a", types.ArrayType(types.StringType(), element_null), array_null),
                types.StructField("d", types.StringType(), delimiter_null),
                types.StructField("r", types.StringType(), replacement_null),
            ])
            spark.createDataFrame(rows, schema).createOrReplaceTempView(name)
            tables.append({"name": name, "schema": schema.jsonValue(), "rows": rows})
        records = [{"spark_version": spark.version, "timezone": "UTC", "tables": tables}]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for table in PROFILES:
                for name, expression in EXPRESSIONS.items():
                    predicate = " WHERE false" if name == "empty_input" else ""
                    sql = f"SELECT id,{expression} AS result FROM {table}{predicate} ORDER BY id"
                    frame = spark.sql(sql)
                    records.append({"id": f"{table}_{name}", "ansi": ansi, "sql": sql,
                                    "schema": frame.schema.jsonValue(),
                                    "rows": [[encode_value(value) for value in row] for row in frame.collect()]})
            for name, expression in SCALAR_EXPRESSIONS.items():
                sql = f"SELECT {expression} AS result"
                frame = spark.sql(sql)
                records.append({"id": f"scalar_{name}", "ansi": ansi, "sql": sql,
                                "schema": frame.schema.jsonValue(),
                                "rows": [[encode_value(value) for value in row] for row in frame.collect()]})
        assert len(records) == 113
        Path(__file__).with_name("array-sort-join-spark.jsonl").write_text(
            "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
        print(json.dumps({"observations": len(records) - 1}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
