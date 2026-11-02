"""Replay a C06 JSONL fixture independently with Spark 4.2.0.

Pass --input FIXTURE and --output FRESH_PATH. The output must not exist.
Only recorded inputs, SQL, ANSI flags and settings drive the replay. Source
result fields are ignored. Query order and returned row order are preserved.
"""

import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import re
import sys


def load_fixture(path):
    payload = path.read_bytes()
    records = [json.loads(line) for line in payload.splitlines() if line.strip()]
    if len(records) < 2:
        raise ValueError("fixture must contain metadata followed by cases")
    metadata = records[0]
    cases = []
    for record in records[1:]:
        case = {key: record[key] for key in ["id", "sql", "ansi"]}
        if not isinstance(case["sql"], str) or type(case["ansi"]) is not bool:
            raise ValueError(f"invalid SQL or ANSI flag for {case['id']}")
        for key in ["original_id", "settings", "timezone"]:
            if key in record:
                case[key] = record[key]
        cases.append(case)
    if len({(case["id"], case["ansi"]) for case in cases}) != len(cases):
        raise ValueError("duplicate case ID/ANSI pairs")
    return metadata, cases, hashlib.sha256(payload).hexdigest()


def decode(value, kind):
    if value is None:
        return None
    if kind in {"integer", "long"}:
        return int(value)
    if kind == "binary":
        return bytes.fromhex(value)
    if kind == "string" and isinstance(value, str):
        return value
    raise ValueError(f"unsupported input value/type: {value!r}, {kind!r}")


def encode(value):
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (int, str)):
        return str(value)
    raise TypeError(f"unexpected C06 output value: {type(value).__name__}")


def register_inputs(spark, metadata):
    from pyspark.sql.types import StructType

    definitions = metadata.get("registered_inputs", metadata.get("inputs"))
    if not isinstance(definitions, list):
        raise ValueError("metadata must contain a registered_inputs or inputs array")
    if len({table["name"] for table in definitions}) != len(definitions):
        raise ValueError("duplicate registered input names")
    registered = []
    for table in definitions:
        fields = table["schema"]["fields"]
        rows = []
        for row in table["rows"]:
            if len(row) != len(fields):
                raise ValueError(f"input row width differs from schema: {table['name']}")
            rows.append(tuple(decode(value, field["type"]) for value, field in zip(row, fields)))
        frame = spark.createDataFrame(rows, StructType.fromJson(table["schema"]))
        frame.createOrReplaceTempView(table["name"])
        registered.append({"name": table["name"], "schema": frame.schema.jsonValue(),
                           "rows": [[encode(value) for value in row] for row in rows]})
    return registered


def optional_call(target, name):
    try:
        return getattr(target, name)()
    except Exception:
        return None


def describe_error(error, phase):
    condition = optional_call(error, "getCondition") or optional_call(error, "getErrorClass")
    parameters = optional_call(error, "getMessageParameters")
    sql_state = optional_call(error, "getSqlState")
    causes = []
    origin = getattr(error, "java_exception", None)
    if origin is None:
        origin = getattr(error, "_origin", None)
    for _ in range(12):
        if origin is None:
            break
        cause = {"class": origin.getClass().getName(), "message": origin.getMessage()}
        java_condition = optional_call(origin, "getCondition") or optional_call(origin, "getErrorClass")
        if java_condition:
            cause["condition"] = java_condition
            java_parameters = optional_call(origin, "getMessageParameters")
            if java_parameters is not None:
                cause["parameters"] = dict(java_parameters)
            if not condition:
                condition, parameters = java_condition, java_parameters
                sql_state = optional_call(origin, "getSqlState")
        causes.append(cause)
        origin = origin.getCause()
    message = str(error)
    # Java's replacement parser can throw these before Spark assigns a condition.
    known_unstructured = any(
        cause["class"] == "java.util.regex.PatternSyntaxException" or
        (cause["class"] in {"java.lang.IllegalArgumentException", "java.lang.IndexOutOfBoundsException"}
         and re.search(r"(?i)(group reference|group with name|no group|group index|named capturing group|character to be escaped)",
                       cause["message"] or ""))
        for cause in causes)
    if not condition and not known_unstructured:
        raise RuntimeError(f"unstructured error during {phase}; capture aborted: {message}") from error
    return {"status": "error", "phase": phase, "condition": condition,
            "parameters": dict(parameters) if parameters is not None else {},
            "sql_state": sql_state, "message": message,
            "python_class": f"{type(error).__module__}.{type(error).__name__}", "java_causes": causes}


def observe(spark, sql):
    schemas = {}
    phase = "sql"
    try:
        frame = spark.sql(sql)
        phase = "dataframe_schema"
        schemas["schema"] = frame.schema.jsonValue()
        execution = frame._jdf.queryExecution()
        phase = "analyzed_schema"
        schemas["analyzed_schema"] = json.loads(execution.analyzed().schema().json())
        phase = "optimized_schema"
        schemas["optimized_schema"] = json.loads(execution.optimizedPlan().schema().json())
        phase = "collect"
        rows = [[encode(value) for value in row] for row in frame.collect()]
        phase = "physical_schema"
        schemas["physical_schema"] = json.loads(execution.executedPlan().schema().json())
        return {"status": "ok", **schemas, "rows": rows}
    except Exception as error:
        return {**describe_error(error, phase), **schemas}


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def runtime_identity(spark):
    import pyspark
    from pyspark import version

    jvm = spark.sparkContext._jvm
    java_properties = {name: jvm.java.lang.System.getProperty(name) for name in [
        "java.version", "java.vendor", "java.runtime.name", "java.runtime.version",
        "java.vm.name", "java.vm.vendor", "java.vm.version", "java.specification.version",
        "user.language", "user.country", "file.encoding",
    ]}
    package = getattr(getattr(jvm.org.apache.spark, "package$"), "MODULE$")
    build = {}
    for name in ["SPARK_VERSION", "SPARK_REVISION", "SPARK_BRANCH", "SPARK_BUILD_USER",
                 "SPARK_REPO_URL", "SPARK_BUILD_DATE"]:
        try:
            build[name] = {"value": getattr(package, name)(), "source": f"org.apache.spark.{name}"}
        except Exception as error:
            build[name] = {"value": None, "unavailable": str(error)}
    distribution = importlib.metadata.distribution("pyspark")
    pip_metadata = {key: distribution.metadata.get_all(key) for key in [
        "Name", "Version", "Summary", "Home-page", "Requires-Python", "Requires-Dist",
    ]}
    pip_metadata["direct_url.json"] = distribution.read_text("direct_url.json")
    try:
        pip_version = importlib.metadata.version("pip")
    except importlib.metadata.PackageNotFoundError:
        pip_version = None
    artifacts = {}
    for name, class_name in [("spark_core", "org.apache.spark.SparkContext"),
                             ("spark_sql", "org.apache.spark.sql.execution.SparkPlan"),
                             ("spark_catalyst", "org.apache.spark.sql.catalyst.plans.logical.LogicalPlan")]:
        location = jvm.java.lang.Class.forName(class_name).getProtectionDomain().getCodeSource().getLocation().toURI()
        artifact = Path(str(location.getPath()))
        if not artifact.is_file():
            raise RuntimeError(f"cannot hash loaded {name} artifact: {artifact}")
        artifacts[name] = {"loaded_class": class_name, "path": str(artifact),
                           "sha256": file_sha256(artifact), "bytes": artifact.stat().st_size}
    script = Path(__file__).resolve()
    artifacts["capture_script"] = {"path": str(script), "sha256": file_sha256(script),
                                   "bytes": script.stat().st_size}
    return {"spark_version": spark.version, "spark_build": build, "artifacts": artifacts,
            "java_properties": java_properties, "java_default_locale": jvm.java.util.Locale.getDefault().toLanguageTag(),
            "python": {"version": sys.version, "executable": sys.executable,
                       "implementation": platform.python_implementation(), "compiler": platform.python_compiler()},
            "pyspark": {"version": pyspark.__version__, "build_version": version.__version__,
                        "module": pyspark.__file__, "pip_version": pip_version, "pip_metadata": pip_metadata}}


def replay_settings(metadata):
    # Process IDs, timestamps and the previous driver's port belong to that run.
    volatile = {"spark.app.id", "spark.app.submitTime", "spark.app.startTime",
                "spark.driver.port", "spark.executor.id"}
    context = metadata.get("spark_context_settings", {})
    settings = {key: value for key, value in context.items() if key not in volatile}
    placeholders = {}
    for key, value in metadata.get("settings", {}).items():
        if value.startswith("<") and value.endswith(">"):
            placeholders[key] = value
        else:
            settings[key] = value
    return settings, placeholders, {key: context[key] for key in volatile if key in context}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.input.resolve() == args.output.resolve() or args.output.exists() or args.output.is_symlink():
        parser.error("--output must be a fresh path, distinct from --input")
    source, cases, source_sha256 = load_fixture(args.input)
    settings, placeholders, process_settings = replay_settings(source)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.master(settings.pop("spark.master", "local[1]"))
    for key, value in settings.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        registered = register_inputs(spark, source)
        metadata = {**source, **runtime_identity(spark), "registered_inputs": registered,
                    "settings": {row.key: row.value for row in spark.sql("SET -v").collect()},
                    "spark_context_settings": dict(spark.sparkContext.getConf().getAll()),
                    "replay_source": {"path": str(args.input.resolve()), "sha256": source_sha256,
                                      "spark_version": source.get("spark_version"),
                                      "spark_build": source.get("spark_build"), "artifacts": source.get("artifacts")},
                    "replay": {"observations": len(cases), "inherited_setting_placeholders": placeholders,
                               "omitted_process_settings": process_settings}}
        records = [metadata]
        case_keys = {key for case in cases for key in case.get("settings", {})}
        if any("timezone" in case for case in cases):
            case_keys.add("spark.sql.session.timeZone")
        baseline = {key: spark.conf.get(key, None) for key in case_keys}
        for case in cases:
            for key, value in baseline.items():
                spark.conf.unset(key) if value is None else spark.conf.set(key, value)
            for key, value in case.get("settings", {}).items():
                spark.conf.set(key, value)
            if "timezone" in case:
                spark.conf.set("spark.sql.session.timeZone", case["timezone"])
            spark.conf.set("spark.sql.ansi.enabled", str(case["ansi"]).lower())
            observed = observe(spark, case["sql"])
            records.append({**case, "expected": observed})
            print(f"{case['id']} ansi={case['ansi']}: {observed['status']}", file=sys.stderr, flush=True)
        payload = "".join(json.dumps(record, separators=(",", ":"), ensure_ascii=True) + "\n" for record in records)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x", encoding="utf-8") as target:
            target.write(payload)
        print(json.dumps({"output": str(args.output), "observations": len(cases),
                          "errors": sum(record["expected"]["status"] == "error" for record in records[1:])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
