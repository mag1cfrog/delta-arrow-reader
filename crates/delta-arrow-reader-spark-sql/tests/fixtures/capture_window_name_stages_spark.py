"""Recapture original #727 SQL and missing window-label boundaries on pinned Spark."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

import pyspark
from pyspark.sql import SparkSession, types

from capture_floating_group_keys_spark import encode


REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"


def additional_queries(table):
    yield "literal_arguments", (
        "SELECT id,SUM(1) OVER (PARTITION BY g),"
        "LAG(1,2,-99) OVER (ORDER BY id),"
        "NTH_VALUE(CAST(NULL AS INT),2) IGNORE NULLS OVER (ORDER BY id) "
        f"FROM {table}")
    yield "coerced_default", (
        f"SELECT id,LAG(CAST(v AS BIGINT),1,-99) OVER (ORDER BY id) FROM {table}")
    yield "aggregate_named", (
        f"SELECT id,SUM(CAST(v AS BIGINT)) OVER w,COUNT(*) OVER w FROM {table} "
        "WINDOW w AS (PARTITION BY CAST(g AS BIGINT) "
        "ORDER BY k DESC NULLS LAST,id ASC NULLS LAST "
        "ROWS BETWEEN (1+1) PRECEDING AND CURRENT ROW)")
    yield "zero_frame", (
        "SELECT id,SUM(v) OVER (ORDER BY id ROWS BETWEEN 0 PRECEDING AND CURRENT ROW),"
        f"SUM(v) OVER (ORDER BY id RANGE BETWEEN 0 PRECEDING AND CURRENT ROW) FROM {table}")
    yield "folded_ignore_nulls", (
        "SELECT id,NTH_VALUE(v,2,CAST(true AS BOOLEAN)) OVER (ORDER BY id),"
        f"FIRST_VALUE(v,CAST(true AS BOOLEAN)) OVER (ORDER BY id) FROM {table}")
    yield "composed", (
        "SELECT id,(SUM(v) OVER (ORDER BY id)+CAST(1 AS BIGINT)),"
        f"CAST(LAG(v) OVER (ORDER BY id) AS BIGINT) FROM {table}")
    yield "outer_order", (
        f"SELECT id,LAG(v) OVER (ORDER BY id) FROM {table} ORDER BY id")


def decode(value, data_type):
    if value is None:
        return None
    if isinstance(data_type, types.StructType):
        return tuple(decode(item, field.dataType) for item, field in zip(value, data_type))
    if isinstance(data_type, types.BooleanType):
        assert value in ["True", "False", "true", "false"], value
        return value.lower() == "true"
    assert isinstance(data_type, types.IntegerType), data_type
    return int(value)


def capture_case(spark, case):
    phase = "sql"
    stages = {}
    plans = {}
    record = dict(case, schema_stages=stages, spark_plans=plans)
    try:
        frame = spark.sql(case["sql"])
        execution = frame._jdf.queryExecution()
        for name, get_plan, next_phase in [
            ("analyzed", execution.analyzed, "analyzed_schema"),
            ("optimized", execution.optimizedPlan, "optimized_schema"),
            ("executed_before_collect", execution.executedPlan, "executed_before_collect"),
        ]:
            phase = next_phase
            plan = get_plan()
            stages[name] = json.loads(plan.schema().json())
            plans[name] = plan.toString()
        phase = "public_schema"
        schema = frame.schema
        assert schema.jsonValue() == stages["analyzed"], case
        phase = "collect"
        collected = frame.collect()
        phase = "encode"
        rows = [encode(row, schema) for row in collected]
        record["rows_raw"] = rows.copy()
        phase = "executed_schema"
        stages["executed"] = json.loads(execution.executedPlan().schema().json())
        plans["executed"] = execution.executedPlan().toString()
        phase = "encode"
        if not case.get("ordered"):
            rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
        record["expected"] = {"schema": stages["analyzed"], "rows": rows}
    except Exception as error:
        condition = getattr(error, "getCondition", lambda: None)()
        assert condition is not None, (case, phase, str(error))
        record.update(expected={"condition": condition}, spark_error_phase=phase,
                      spark_error=str(error), spark_error_details={
                          "message_parameters": getattr(error, "getMessageParameters", lambda: {})(),
                          "sql_state": getattr(error, "getSqlState", lambda: None)(),
                      })
        if phase == "collect":
            phase = "executed_schema"
            stages["executed"] = json.loads(execution.executedPlan().schema().json())
            plans["executed"] = execution.executedPlan().toString()
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    fixture_dir = Path(__file__).parent
    records = []
    source_hashes = {}
    metadata = None
    for corpus, name in [("original", "window-names-spark.jsonl"),
                         ("alias_free_control", "window-names-pivot-controls-spark.jsonl")]:
        source = fixture_dir / name
        source_hashes[name] = hashlib.sha256(source.read_bytes()).hexdigest()
        captured = [json.loads(line) for line in source.read_text().splitlines()]
        if metadata is None:
            metadata = captured[0]
        assert metadata == captured[0]
        records.extend(dict(case, corpus=corpus) for case in captured[1:])
    assert len(records) == 294
    for ansi in [True, False]:
        for table in ["inputs", "empty_inputs", "null_inputs"]:
            records.extend({"corpus": "supplement", "id": f"{table}_{name}",
                            "sql": sql, "ansi": ansi, "ordered": name == "outer_order"}
                           for name, sql in additional_queries(table))
        for control in [case for case in records if case["corpus"] == "alias_free_control"
                        and case["ansi"] == ansi]:
            records.append({"corpus": "supplement", "id": control["id"] + "_parenthesized",
                            "sql": control["sql"].replace(" FOR g IN ", " FOR (g) IN "),
                            "ansi": ansi})
    assert len(records) == 354
    assert len({(case["corpus"], case["id"], case["ansi"]) for case in records}) == len(records)
    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert "revision=" + REVISION in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-727-window-name-stages")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        header = dict(metadata, spark_build=build, spark_revision=REVISION,
                      source_hashes=source_hashes,
                      capture_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                      spark_settings={key: spark.conf.get(key) for key in [
                          "spark.sql.shuffle.partitions", "spark.sql.session.timeZone",
                          "spark.sql.adaptive.enabled", "spark.sql.caseSensitive",
                          "spark.sql.optimizer.excludedRules"]})
        for provider in metadata["inputs"]:
            schema = types.StructType.fromJson(provider["schema"])
            values = [decode(row, schema) for row in provider["rows"]]
            assert [encode(row, schema, input_value=True) for row in values] == provider["rows"]
            spark.createDataFrame(values, schema).createOrReplaceTempView(provider["name"])
        reference_drift = []
        with args.output.open("x") as output:
            output.write(json.dumps(header, separators=(",", ":")) + "\n")
            for case in records:
                spark.conf.set("spark.sql.ansi.enabled", str(case["ansi"]).lower())
                observation = capture_case(spark, case)
                if case["corpus"] != "supplement":
                    observation["source_expected"] = case["expected"]
                    if observation["expected"] != case["expected"]:
                        reference_drift.append((case["corpus"], case["id"], case["ansi"]))
                output.write(json.dumps(observation, separators=(",", ":")) + "\n")
                output.flush()
        print(json.dumps({"observations": len(records), "reference_drift": reference_drift,
                          "output": str(args.output)}))
        assert not reference_drift, reference_drift
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
