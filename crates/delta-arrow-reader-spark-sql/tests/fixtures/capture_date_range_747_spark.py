"""Recapture all 548 original DATE RANGE cases from Spark; --self-check starts no engine.

Only the sibling source-only manifest supplies SQL and providers. Its historical
source hashes identify provenance; those absent source files are not opened.
"""

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import traceback

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
MANIFEST = HERE / "date-range-747-cases.json"
VERSION = "4.2.0"
REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
STAGES = ["public", "analyzed", "optimized", "executed_before_collect", "executed"]
SETTINGS = {"spark.sql.shuffle.partitions": "1", "spark.sql.session.timeZone": "UTC"}
OBSERVED_SETTINGS = ["spark.sql.session.timeZone", "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled", "spark.sql.ansi.enabled", "spark.sql.caseSensitive",
    "spark.sql.optimizer.excludedRules", "spark.sql.parser.escapedStringLiterals",
    "spark.sql.variable.substitute"]


def inventory():
    manifest = json.loads(MANIFEST.read_text())
    cases = [case for corpus in manifest["corpora"] for case in corpus["cases"]]
    assert manifest["expected_record_count"] == len(cases) == 548
    assert len({(case["corpus"], case["id"], case["ansi"]) for case in cases}) == 548
    assert len({(case["id"], case["ansi"]) for case in cases}) == 548
    assert dict(Counter(case["ownership"] for case in cases)) == manifest["ownership_counts"] == {
        "numeric_date_primary": 480, "date_interval_owner": 42, "timestamp_control": 6,
        "null_offset_owner": 14, "original_five_column_primary": 6}
    assert sum(case["ordered"] for case in cases) == 24
    assert {corpus["corpus"]: len(corpus["cases"]) for corpus in manifest["corpora"]} == {
        "date-range-frames-spark": 294, "date-range-wrap-spark": 56,
        "date-range-consumers-spark": 48, "date-range-admission-probes-spark": 104,
        "date-range-constant-keys-spark": 16, "date-range-unbounded-suffix-spark": 24,
        "original": 6}
    for corpus in manifest["corpora"]:
        assert corpus["metadata"]["spark_version"] == VERSION
        assert corpus["metadata"]["timezone"] == "UTC"
        inputs = corpus["metadata"]["inputs"]
        assert len({table["name"] for table in inputs}) == len(inputs)
        for table in inputs:
            fields = table["schema"]["fields"]
            assert table["schema"]["type"] == "struct" and fields
            for row in table["rows"]:
                assert len(row) == len(fields)
                for value, field in zip(row, fields):
                    assert field["type"] in ["integer", "boolean"]
                    if value is None:
                        assert field["nullable"]
                    elif field["type"] == "integer":
                        assert str(int(value)) == value and -(2**31) <= int(value) < 2**31
                    else:
                        assert value in ["True", "False", "true", "false"]
        for case in corpus["cases"]:
            assert set(case) == {"corpus", "id", "ansi", "sql", "ordered", "ownership"}
            assert case["corpus"] == corpus["corpus"] and isinstance(case["sql"], str)
            assert type(case["ansi"]) is bool and type(case["ordered"]) is bool
        if corpus["corpus"] == "original":
            assert {case["id"] for case in corpus["cases"]} == {
                "inputs_date_range", "empty_inputs_date_range", "null_inputs_date_range"}
            assert all(len(table["schema"]["fields"]) == 5 for table in inputs)
    return manifest



def optional(target, method):
    try:
        return getattr(target, method)()
    except Exception:
        return None


def error_details(error, phase):
    condition = optional(error, 'getCondition') or optional(error, 'getErrorClass')
    parameters = optional(error, 'getMessageParameters')
    state = optional(error, 'getSqlState')
    result = {'phase': phase, 'condition': condition, 'python_class': type(error).__module__ + '.' + type(error).__name__,
              'message': str(error), 'parameters': dict(parameters) if parameters is not None else {},
              'sql_state': state, 'java_causes': [], 'cause_chain_complete': True}
    cause = getattr(error, 'java_exception', None)
    if cause is None:
        cause = getattr(error, '_origin', None)
    seen = set()
    try:
        while cause is not None:
            identity = getattr(cause, '_target_id', None)
            if identity is not None and identity in seen:
                result.update(cause_chain_complete=False, cause_chain_unavailable='repeated Java cause identity')
                break
            if identity is not None:
                seen.add(identity)
            java_condition = optional(cause, 'getCondition') or optional(cause, 'getErrorClass')
            java_parameters = optional(cause, 'getMessageParameters')
            entry = {'class': cause.getClass().getName(), 'message': cause.getMessage(), 'condition': java_condition,
                     'parameters': dict(java_parameters) if java_parameters is not None else {}, 'sql_state': optional(cause, 'getSqlState')}
            result['java_causes'].append(entry)
            if result['condition'] is None and java_condition is not None:
                result.update(condition=java_condition, parameters=entry['parameters'], sql_state=entry['sql_state'])
            cause = cause.getCause()
    except Exception as cause_error:
        result.update(cause_chain_complete=False, cause_chain_unavailable=type(cause_error).__name__ + ': ' + str(cause_error))
    return result


def capture(spark, case, encode):
    record = dict(case, schema_stages={}, spark_plans={}, schema_stage_availability={
        stage: {'available': False, 'reason': 'not reached'} for stage in STAGES})
    frame = execution = None
    phase = 'sql'
    try:
        frame = spark.sql(case['sql'])
        phase = 'public_schema'
        schema = frame.schema
        record['schema_stages']['public'] = schema.jsonValue()
        record['schema_stage_availability']['public'] = {'available': True}
        phase = 'query_execution'
        execution = frame._jdf.queryExecution()
        for stage, accessor in [('analyzed', execution.analyzed), ('optimized', execution.optimizedPlan),
                                ('executed_before_collect', execution.executedPlan)]:
            phase = stage + '_schema'
            plan = accessor()
            record['schema_stages'][stage] = json.loads(plan.schema().json())
            record['schema_stage_availability'][stage] = {'available': True}
            record['spark_plans'][stage] = plan.toString()
        phase = 'collect'
        materialized = frame.collect()
        phase = 'encode'
        rows = [encode(row, schema) for row in materialized]
        multiset = sorted(rows, key=lambda row: json.dumps(row, separators=(',', ':')))
        record.update(status='ok', rows_raw=rows, rows_multiset=multiset,
                      expected={'schema': record['schema_stages']['public'], 'rows': rows if case['ordered'] else multiset})
        phase = 'executed_schema_after_collect'
        plan = execution.executedPlan()
        record['schema_stages']['executed'] = json.loads(plan.schema().json())
        record['schema_stage_availability']['executed'] = {'available': True}
        record['spark_plans']['executed'] = plan.toString()
    except Exception as error:
        details = error_details(error, phase)
        record.update(status='error' if details['condition'] is not None and phase != 'encode' and 'expected' not in record else 'capture_failure',
                      spark_error_phase=phase, spark_error=str(error), spark_error_details=details)
        if record['status'] == 'error':
            record['expected'] = {'condition': details['condition']}
        else:
            record['capture_failure'] = details
        accessors = {}
        if frame is not None:
            accessors['public'] = lambda: frame.schema.jsonValue()
            if execution is None:
                try:
                    execution = frame._jdf.queryExecution()
                except Exception as execution_error:
                    record['query_execution_error'] = error_details(execution_error, 'query_execution_after_failure')
        if execution is not None:
            for stage, method in [('analyzed', execution.analyzed), ('optimized', execution.optimizedPlan),
                                  ('executed_before_collect', execution.executedPlan), ('executed', execution.executedPlan)]:
                if stage == 'executed_before_collect' and phase in ['collect', 'executed_schema_after_collect', 'encode']:
                    continue
                if stage == 'executed' and phase not in ['collect', 'executed_schema_after_collect', 'encode']:
                    continue
                accessors[stage] = method
        for stage in STAGES:
            if stage in record['schema_stages']:
                continue
            if stage not in accessors:
                record['schema_stage_availability'][stage] = {'available': False, 'reason':
                    'spark.sql failed before a DataFrame existed' if frame is None else
                    'collect was not attempted; no after-collect stage exists' if stage == 'executed' else
                    'stage was not reached before failure at ' + phase}
                continue
            try:
                observed = accessors[stage]()
                if stage == 'public':
                    record['schema_stages'][stage] = observed
                else:
                    record['schema_stages'][stage] = json.loads(observed.schema().json())
                    record['spark_plans'][stage] = observed.toString()
                record['schema_stage_availability'][stage] = {'available': True, 'observed_after_failure': True}
            except Exception as stage_error:
                record['schema_stage_availability'][stage] = {'available': False, 'reason': 'actual stage accessor failed',
                    'error': error_details(stage_error, stage + '_schema_after_failure')}
    return record


def cleanup(spark, gateway):
    result = {'owned_gateway': gateway is not None, 'errors': []}
    proc = None if gateway is None else getattr(gateway, 'proc', None)
    result['owned_pid'] = None if proc is None else proc.pid
    def close():
        for name, operation in [('spark.stop', None if spark is None else spark.stop),
                                ('gateway.shutdown', None if gateway is None else gateway.shutdown),
                                ('gateway.close', None if gateway is None else gateway.close)]:
            if operation is not None:
                try:
                    operation()
                    result[name] = 'completed'
                except Exception as error:
                    result['errors'].append({'operation': name, 'error': type(error).__name__ + ': ' + str(error)})
    closer = threading.Thread(target=close, daemon=True)
    closer.start()
    closer.join(timeout=20)
    result['close_thread_finished'] = not closer.is_alive()
    if closer.is_alive():
        result['errors'].append({'operation': 'close', 'error': 'owned shutdown exceeded 20 seconds'})
    if proc is not None:
        try:
            if proc.stdin is not None:
                proc.stdin.close()
                result['stdin_closed'] = True
        except Exception as error:
            result['errors'].append({'operation': 'stdin.close', 'error': str(error)})
        try:
            result['exit_code'] = proc.wait(timeout=10)
            result['reaped'] = True
        except subprocess.TimeoutExpired:
            result['errors'].append({'operation': 'wait', 'error': 'owned JVM exceeded 10 seconds; kill and wait required'})
            proc.kill()
            result['killed'] = True
            result['exit_code'] = proc.wait(timeout=10)
            result['reaped'] = True
        closer.join(timeout=1)
    return result


def self_check(request):
    assert "pyspark" not in sys.modules
    schema = {'type': 'struct', 'fields': [{'name': 'v', 'type': 'integer', 'nullable': True, 'metadata': {'kept': 'exact'}}]}
    class SparkError(Exception):
        def getCondition(self): return 'SELF_CHECK_ACTUAL'
    class Schema:
        def jsonValue(self): return schema
        def json(self): return json.dumps(schema)
    class Plan:
        def schema(self): return Schema()
        def toString(self): return 'actual mock plan'
    class Execution:
        def analyzed(self): return Plan()
        def optimizedPlan(self):
            if failure == 'optimized': raise SparkError('optimizer')
            return Plan()
        def executedPlan(self):
            if failure == 'optimized': raise SparkError('physical after optimizer')
            return Plan()
    class Frame:
        schema = Schema()
        def queryExecution(self): return Execution()
        def collect(self):
            if failure == 'collect': raise SparkError('collection')
            return [[2], [1], [2]]
    class Spark:
        def sql(self, sql):
            if failure == 'sql': raise SparkError('SQL')
            frame = Frame()
            frame._jdf = frame
            return frame
    case = {'corpus': 'self_check', 'id': 'case', 'ansi': True, 'sql': 'SELECT v FROM t', 'ordered': False}
    failure = None
    observed = capture(Spark(), case, lambda row, _: list(map(str, row)))
    assert observed['rows_raw'] == [['2'], ['1'], ['2']]
    assert observed['expected']['rows'] == [['1'], ['2'], ['2']] == observed['rows_multiset']
    assert observed['expected']['schema'] == schema
    assert all(item['available'] for item in observed['schema_stage_availability'].values())
    assert capture(Spark(), dict(case, ordered=True), lambda row, _: row)['expected']['rows'] == [[2], [1], [2]]
    for failure, phase, available in [('sql', 'sql', []), ('optimized', 'optimized_schema', ['public', 'analyzed']),
                                      ('collect', 'collect', STAGES)]:
        observed = capture(Spark(), case, lambda row, _: row)
        assert observed['spark_error_phase'] == phase
        assert observed['expected'] == {'condition': 'SELF_CHECK_ACTUAL'}
        assert sorted(observed['schema_stages']) == sorted(available)
        assert all(item['available'] or item['reason'] for item in observed['schema_stage_availability'].values())
    class Pipe:
        def close(self): pass
    class Proc:
        pid = 123
        stdin = Pipe()
        calls = 0
        def wait(self, timeout):
            self.calls += 1
            if self.calls == 1: raise subprocess.TimeoutExpired('self_check_owned', timeout)
            return -9
        def kill(self): self.killed = True
    class Gateway:
        proc = Proc()
        def shutdown(self): pass
        def close(self): pass
    class StopFailure:
        def stop(self): raise RuntimeError('preserve stop failure')
    receipt = cleanup(StopFailure(), Gateway())
    assert receipt['reaped'] and receipt['killed'] and receipt['exit_code'] == -9
    assert len(receipt['errors']) == 2
    print(json.dumps({'self_check': 'passed', 'observations': sum(len(c['cases']) for c in request['corpora']),
                      'checks': ['manifest SQL/provider identity', 'duplicate multiset/raw ordering',
                                 'full schema Fields', 'SQL/optimizer/collect failures and partial stages', 'stop failure and owned kill/wait']}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", nargs="?", type=Path)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    manifest = inventory()
    if args.self_check:
        assert args.output is None
        self_check(manifest)
        return
    assert args.output is not None and args.output.parent.is_dir(), "provide a fresh output path"
    lifecycle_path = args.output.with_suffix(".lifecycle.json")
    for path in [args.output, lifecycle_path]:
        assert not path.exists() and not path.is_symlink(), path
    assert os.environ.get("TZ") == "UTC", "run with TZ=UTC"
    assert not any(key in os.environ for key in ["PYSPARK_GATEWAY_PORT", "PYSPARK_GATEWAY_SECRET",
                                               "SPARK_REMOTE", "SPARK_CONNECT_MODE_ENABLED"])
    os.environ["PYSPARK_PYTHON"] = sys.executable
    from capture_floating_group_keys_spark import encode
    from capture_window_name_stages_spark import decode
    from recapture_string_binary_spark import file_sha256, runtime_identity
    from pyspark import SparkContext
    from pyspark.sql import SparkSession, types

    assert SparkContext._gateway is None and SparkContext._active_spark_context is None
    assert SparkSession._instantiatedSession is None and SparkSession._activeSession is None
    sources = [Path(__file__).resolve(), MANIFEST, HERE / "capture_floating_group_keys_spark.py",
               HERE / "capture_window_name_stages_spark.py", HERE / "recapture_string_binary_spark.py"]
    source_receipts = [{"path": str(path), "sha256": file_sha256(path)} for path in sources]
    spark = gateway = None
    lifecycle = {"spark_only": True, "reads_expected_outputs": False, "capture_pid": os.getpid(),
                 "foreign_gateway_reuse_rejected": True}
    with args.output.open("x") as output, lifecycle_path.open("x") as lifecycle_file:
        try:
            builder = SparkSession.builder.master("local[1]").appName("dar-747-original-recapture")
            builder = builder.config("spark.ui.enabled", "false")
            for key, value in SETTINGS.items():
                builder = builder.config(key, value)
            spark = builder.getOrCreate()
            gateway = SparkContext._gateway
            assert gateway is not None and gateway.proc is not None, "owned JVM required"
            spark.sparkContext.setLogLevel("ERROR")
            identity = runtime_identity(spark)
            assert identity["spark_version"] == VERSION
            assert identity["spark_build"]["SPARK_REVISION"]["value"] == REVISION
            assert identity["java_properties"]["java.specification.version"] == "21"
            assert spark.sparkContext.master == "local[1]"
            assert spark.sparkContext.getConf().get("spark.ui.enabled") == "false"
            sessions, header_corpora = [], []
            for corpus in manifest["corpora"]:
                session = spark.newSession()
                for key, value in SETTINGS.items():
                    session.conf.set(key, value)
                receipts = []
                for item in corpus["metadata"]["inputs"]:
                    schema = types.StructType.fromJson(item["schema"])
                    rows = [decode(row, schema) for row in item["rows"]]
                    assert [encode(row, schema, input_value=True) for row in rows] == item["rows"]
                    frame = session.createDataFrame(rows, schema)
                    frame.createOrReplaceTempView(item["name"])
                    assert frame.schema.jsonValue() == item["schema"]
                    receipts.append({**item, "num_partitions": frame.rdd.getNumPartitions(),
                        "construction": "createDataFrame(list, original StructType)",
                        "schema_observed": frame.schema.jsonValue()})
                sessions.append((corpus, session))
                header_corpora.append({key: corpus[key] for key in ["corpus", "source", "metadata"]} | {
                    "registered_inputs": receipts, "session_uuid": session._jsparkSession.sessionUUID()})
            header = {**identity, "issue": 747, "corpora": header_corpora,
                "input_request": {"path": str(MANIFEST), "sha256": source_receipts[1]["sha256"]},
                "capture_files": source_receipts, "requested_settings": SETTINGS,
                "master": spark.sparkContext.master,
                "spark_context_settings": dict(spark.sparkContext.getConf().getAll()),
                "effective_settings": {key: spark.conf.get(key) for key in OBSERVED_SETTINGS},
                "adaptive_policy": "Not explicitly set; actual effective default observed.",
                "historical_source_provenance": "corpus.source paths/SHA identify the manifest's original source; absent historical source files are not opened or rehashed.",
                "owned_gateway": {"pid": gateway.proc.pid, "parent_pid": os.getpid()}}
            output.write(json.dumps(header, separators=(",", ":")) + "\n")
            output.flush()
            observations = []
            for corpus, session in sessions:
                for case in corpus["cases"]:
                    session.conf.set("spark.sql.ansi.enabled", str(case["ansi"]).lower())
                    observed = capture(session, case, encode)
                    observed["effective_settings"] = {key: session.conf.get(key) for key in OBSERVED_SETTINGS}
                    observations.append(observed)
                    output.write(json.dumps(observed, separators=(",", ":")) + "\n")
                    output.flush()
            assert len(observations) == 548
            assert len({(row["corpus"], row["id"], row["ansi"]) for row in observations}) == 548
            assert all(file_sha256(path) == receipt["sha256"] for path, receipt in zip(sources, source_receipts))
            failures = [(row["corpus"], row["id"], row["ansi"]) for row in observations if "capture_failure" in row]
            print(json.dumps({"output": str(args.output), "observations": 548,
                              "capture_failures": failures}), flush=True)
            assert not failures, ("instrumentation failures retained", failures)
            lifecycle["capture_status"] = "completed"
        except BaseException:
            lifecycle.update(capture_status="failed", original_failure=traceback.format_exc())
            raise
        finally:
            gateway = gateway or SparkContext._gateway
            try:
                lifecycle["shutdown"] = cleanup(spark, gateway)
            except BaseException:
                lifecycle["shutdown_failure"] = traceback.format_exc()
            lifecycle_file.write(json.dumps(lifecycle, indent=2) + "\n")
            lifecycle_file.flush()
            if sys.exc_info()[0] is None:
                assert "shutdown_failure" not in lifecycle and not lifecycle["shutdown"]["errors"], lifecycle
                assert lifecycle["shutdown"].get("reaped"), lifecycle


if __name__ == "__main__":
    main()
