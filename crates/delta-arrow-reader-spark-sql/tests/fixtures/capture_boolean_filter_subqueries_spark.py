"""Capture which scalar subqueries Spark discards inside Boolean filters.

Use PySpark 4.2.0 and Java 21. Literal Boolean reduction is checked separately
from later constant folding, local VALUES conversion and required errors.
"""
import json
from pathlib import Path

from py4j.protocol import Py4JError
from pyspark.sql import SparkSession


spark = (SparkSession.builder.master("local[1]").appName("dar-boolean-filter-subqueries")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
try:
    assert spark.version == "4.2.0", spark.version
    spark.sparkContext.setLogLevel("ERROR")
    scalar = "(SELECT CAST('bad' AS INT) FROM range(1))>0"
    predicates = [
        ("false_left", f"false AND {scalar}"),
        ("false_right", f"{scalar} AND false"),
        ("true_left", f"true OR {scalar}"),
        ("true_right", f"{scalar} OR true"),
        ("not_literal", f"NOT true AND {scalar}"),
        ("nested_literals", f"(true AND false) AND ({scalar} OR false)"),
        ("required_and", f"true AND {scalar}"),
        ("required_or", f"false OR {scalar}"),
        ("folded_comparison", f"(1=2) AND {scalar}"),
        ("folded_cast", f"CAST(NULL AS BOOLEAN) AND {scalar}"),
    ]
    queries = []
    for negated in [False, True]:
        op = "NOT EXISTS" if negated else "EXISTS"
        for name, predicate in predicates:
            queries.append((f"{name}_{op.lower().replace(' ', '_')}",
                            f"SELECT k,{op}(SELECT id FROM VALUES(0),(1) t(id) "
                            f"WHERE id=o.k AND ({predicate})) AS r "
                            "FROM VALUES(0),(1),(4),(CAST(NULL AS BIGINT)) o(k) "
                            "ORDER BY k NULLS FIRST"))
    for name, value in [
        ("double", "CAST('bad' AS DOUBLE)"),
        ("decimal", "CAST('bad' AS DECIMAL(10,2))"),
        ("divide", "7 DIV 0"),
    ]:
        for condition in ["false", "true"]:
            queries.append((f"{name}_{condition}",
                            f"SELECT EXISTS(SELECT id FROM range(1) WHERE {condition} "
                            f"AND (SELECT {value} FROM range(1))>0) AS r"))
    for name, source in [("one_row", ""), ("local", " FROM VALUES(0) t(id)")]:
        for condition in ["false", "true"]:
            queries.append((f"{name}_{condition}",
                            f"SELECT EXISTS(SELECT id FROM range(1) WHERE {condition} "
                            f"AND (SELECT CAST('bad' AS INT){source})>0) AS r"))
    queries += [
        ("local_input_required", "SELECT EXISTS(SELECT id FROM VALUES(CAST('bad-local' AS INT)) t(id) "
         f"WHERE false AND ({scalar})) AS r"),
        ("local_output_unused", "SELECT EXISTS(SELECT CAST('bad' AS INT) FROM VALUES(0) t(id) "
         f"WHERE false AND ({scalar})) AS r"),
        ("missing_column", "SELECT EXISTS(SELECT id FROM range(1) WHERE false "
         "AND (SELECT missing_column FROM range(1))>0) AS r"),
        ("invalid_argument_count", "SELECT EXISTS(SELECT id FROM range(1) WHERE false "
         "AND (SELECT abs() FROM range(1))>0) AS r"),
        ("cardinality_unused", "SELECT EXISTS(SELECT id FROM range(1) WHERE false "
         "AND (SELECT id FROM range(2))>0) AS r"),
        ("cardinality_required", "SELECT EXISTS(SELECT id FROM range(1) WHERE true "
         "AND (SELECT id FROM range(2))>0) AS r"),
    ]
    queries.extend((f"projection_{name}", f"SELECT {predicate} AS r")
                   for name, predicate in predicates)
    for name, value in [
        ("double", "CAST('bad' AS DOUBLE)"),
        ("decimal", "CAST('bad' AS DECIMAL(10,2))"),
        ("divide", "7 DIV 0"),
        ("cardinality", "id"),
    ]:
        source = "range(2)" if name == "cardinality" else "range(1)"
        for condition in ["false", "true"]:
            queries.append((f"correlated_{name}_{condition}",
                            "SELECT k,EXISTS(SELECT id FROM VALUES(0),(1) t(id) "
                            f"WHERE id=o.k AND {condition} "
                            f"AND (SELECT {value} FROM {source})>0) AS r "
                            "FROM VALUES(0),(1),(4),(CAST(NULL AS BIGINT)) o(k) "
                            "ORDER BY k NULLS FIRST"))
    for condition in ["false", "true"]:
        for name, output, input_sql in [
            ("local_input", "id", "VALUES(CAST('bad-local' AS INT)) t(id)"),
            ("local_output", "CAST('bad' AS INT)", "VALUES(0),(1) t(id)"),
        ]:
            queries.append((f"correlated_{name}_{condition}",
                            f"SELECT k,EXISTS(SELECT {output} FROM {input_sql} "
                            f"WHERE id=o.k AND {condition} AND ({scalar})) AS r "
                            "FROM VALUES(0),(1),(4),(CAST(NULL AS BIGINT)) o(k) "
                            "ORDER BY k NULLS FIRST"))
    for name, prefix in [("false", "false AND"), ("true", "true OR")]:
        for output_name, output in [("id", "id"), ("cast", "CAST('bad' AS INT)")]:
            queries.append((f"removed_correlation_{name}_{output_name}",
                            f"SELECT k,EXISTS(SELECT {output} FROM range(1) "
                            f"WHERE {prefix} (id=o.k AND ({scalar}))) AS r "
                            "FROM VALUES(0),(1),(4),(CAST(NULL AS BIGINT)) o(k) "
                            "ORDER BY k NULLS FIRST"))
    assert len(dict(queries)) == len(queries)
    records = [{"spark_version": spark.version, "timezone": "UTC"}]
    for ansi in [True, False]:
        spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
        for name, sql in queries:
            try:
                frame = spark.sql(sql)
                schema = frame.schema.jsonValue()
                rows = [[None if value is None else str(value).lower()
                         for value in row] for row in frame.collect()]
                expected = {"status": "ok", "schema": schema, "rows": rows}
            except Exception as error:
                condition = getattr(error, "getCondition", lambda: None)()
                cause = getattr(error, "java_exception", None)
                while condition is None and cause is not None:
                    try:
                        condition = cause.getCondition()
                    except Py4JError:
                        pass
                    cause = cause.getCause()
                assert condition is not None, (name, str(error))
                expected = {"status": "error", "condition": condition}
            records.append({"id": name, "ansi": ansi, "sql": sql, "expected": expected})
    Path(__file__).with_name("boolean-filter-subqueries-spark.jsonl").write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
    print(json.dumps({"observations": len(records) - 1,
                      "errors": sum(r.get("expected", {}).get("status") == "error" for r in records)}))
finally:
    spark.stop()
