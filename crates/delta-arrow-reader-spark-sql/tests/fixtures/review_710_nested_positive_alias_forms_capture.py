"""Independent Spark alias metadata stages for identity-lowered nested fields."""
import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, types


def encode(value, data_type):
    if value is None:
        return None
    if isinstance(data_type, types.StructType):
        return [encode(item, field.dataType) for item, field in zip(value, data_type)]
    return str(value)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]")
             .appName("dar-review-710-nested-positive-alias-forms")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        nested = types.StructType([
            types.StructField("item", types.IntegerType(), True, {"source": "leaf"})])
        schema = types.StructType([
            types.StructField("nested", nested, True, {"source": "record"})])
        records = [{"spark_version": spark.version, "timezone": "UTC", "inputs": [],
                    "capture_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}]
        for provider, values in [("populated", [((1,),), ((None,),), (None,)]), ("empty", [])]:
            table = f"nested_positive_alias_{provider}"
            spark.createDataFrame(values, schema).createOrReplaceTempView(table)
            records[0]["inputs"].append({"name": table, "schema": schema.jsonValue(),
                                         "rows": [encode(row, schema) for row in values]})
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for provider in ["populated", "empty"]:
                for label, expression in [
                    ("direct_bracket_alias", "nested['item'] AS r"),
                    ("unary_positive_bracket_alias", "+nested['item'] AS r"),
                    ("positive_bracket_alias", "POSITIVE(nested['item']) AS r"),
                    ("direct_constructed_bracket_alias", "NAMED_STRUCT('item',nested.item)['item'] AS r"),
                    ("unary_positive_constructed_bracket_alias", "+(NAMED_STRUCT('item',nested.item)['item']) AS r"),
                    ("positive_constructed_bracket_alias", "POSITIVE(NAMED_STRUCT('item',nested.item)['item']) AS r"),
                ]:
                    sql = f"SELECT {expression} FROM nested_positive_alias_{provider}"
                    record = {"id": f"{provider}_{label}", "ansi": ansi, "sql": sql}
                    try:
                        frame = spark.sql(sql)
                        record["public_schema"] = frame.schema.jsonValue()
                        execution = frame._jdf.queryExecution()
                        record["analyzed_schema"] = json.loads(execution.analyzed().schema().json())
                        record["optimized_schema"] = json.loads(execution.optimizedPlan().schema().json())
                        record["executed_schema_before_collect"] = json.loads(execution.executedPlan().schema().json())
                        record["analyzed_plan"] = execution.analyzed().toString()
                        record["optimized_plan"] = execution.optimizedPlan().toString()
                        rows = [encode(row, frame.schema) for row in frame.collect()]
                        record["executed_schema_after_collect"] = json.loads(execution.executedPlan().schema().json())
                        record["physical_plan"] = execution.executedPlan().toString()
                        rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                        record["expected"] = {"schema": frame.schema.jsonValue(), "rows": rows}
                    except Exception as error:
                        record["error"] = {"condition": getattr(error, "getCondition", lambda: None)(),
                                           "message": str(error)}
                    records.append(record)
        assert len(records) == 25
        assert len({(r["id"], r["ansi"]) for r in records[1:]}) == 24
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations": len(records) - 1, "output": str(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
