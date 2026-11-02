"""Independent Spark extraction-only classification around IDENTIFIER clauses."""
import argparse
import hashlib
import importlib.util
import json
import os
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    helper_path = Path(__file__).with_name("capture_generated_alias_metadata_spark_v2.py")
    spec = importlib.util.spec_from_file_location("alias_capture", helper_path)
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (helper.SparkSession.builder.master("local[1]")
             .appName("dar-review-710-identifier-extraction")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        t = helper.types
        schema = t.StructType([
            t.StructField("items", t.ArrayType(t.IntegerType(), True), True, {"source":"array"}),
            t.StructField("nested", t.StructType([
                t.StructField("item",t.IntegerType(),True,{"source":"child"})
            ]), True, {"source":"record"}),
        ])
        values = [([2,3],(2,)),(None,None)]
        table = "review_extract_inputs"
        spark.createDataFrame(values,schema).createOrReplaceTempView(table)
        records = [{"spark_version":spark.version,"timezone":"UTC",
                    "capture_script_sha256":hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    "inputs":[{"name":table,"schema":schema.jsonValue(),
                               "rows":[helper.encode(row,schema) for row in values]}]}]
        expressions = [("ordinary_array","items[0]"),
                       ("identifier_array","IDENTIFIER('items')[0]"),
                       ("ordinary_field","nested.item"),
                       ("identifier_field","IDENTIFIER('nested').item")]
        for ansi in [True,False]:
            spark.conf.set("spark.sql.ansi.enabled",str(ansi).lower())
            for label,expression in expressions:
                for suffix,alias in [("generated",""),("explicit"," AS value")]:
                    sql = f"SELECT {expression}{alias} FROM {table}"
                    records.append({"id":f"{label}_{suffix}","ansi":ansi,"sql":sql,
                                    **helper.capture(spark,sql)})
        assert len(records) == 17
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record,separators=(",",":"))+"\n")
        print(json.dumps({"observations":len(records)-1,"output":str(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
