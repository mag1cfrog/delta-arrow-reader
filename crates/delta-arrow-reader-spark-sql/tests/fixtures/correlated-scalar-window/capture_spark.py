"""Capture correlated window values, executed types, and the scalar row guard."""

import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession, types

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from capture_floating_group_keys_spark import encode


def main():
    output = Path(sys.argv[1])
    assert not output.exists()
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-correlated-window")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.adaptive.enabled", "false")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0"
        spark.sparkContext.setLogLevel("ERROR")
        integer = types.IntegerType()
        ordinary_schema = types.StructType([types.StructField("k", integer, False),
                                            types.StructField("v", integer, False)])
        inner_schema = types.StructType([types.StructField(name, integer)
                                        for name in ["k", "g", "v"]])
        outer_schema = types.StructType([types.StructField("id", integer, False),
                                         types.StructField("k", integer)])
        definitions = [
            ("ordinary", ordinary_schema, [(1, 10), (1, 20), (2, 30)]),
            ("window_inner", inner_schema, [(1, 7, 10), (1, 7, 10), (1, 7, 20),
                                            (1, 9, 40), (2, 7, 30), (2, 9, None),
                                            (3, None, None), (None, 7, 50)]),
            ("outer_keys", outer_schema, [(0, 0), (1, 1), (2, 1), (3, 2),
                                            (4, 3), (5, None)]),
        ]
        records = [{"spark_version": spark.version, "timezone": "UTC", "inputs": []}]
        for name, schema, rows in definitions:
            spark.createDataFrame(rows, schema).createOrReplaceTempView(name)
            records[0]["inputs"].append({"name": name, "schema": schema.jsonValue(),
                "rows": [[encode(value, field.dataType, input_value=True)
                          for value, field in zip(row, schema)] for row in rows]})

        def scalar(inner, value="MAX(q.w)", outer="outer_keys o", key="o.id"):
            return (f"SELECT {key},(SELECT {value} FROM ({inner}) q) AS r "
                    f"FROM {outer} ORDER BY {key}")

        def window(expression, source="window_inner", predicate="i.k=o.k"):
            return f"SELECT i.g,i.v,{expression} AS w FROM {source} i WHERE {predicate}"

        ranked = window("ROW_NUMBER() OVER (ORDER BY i.v NULLS FIRST)")
        filtered = f"SELECT p.g,p.v FROM ({ranked}) p WHERE p.w>2"
        expressions = [
            ("issue_row_number", "SELECT o.k,(SELECT q.v FROM (SELECT i.v,ROW_NUMBER() OVER "
             "(ORDER BY i.v DESC) AS rn FROM ordinary i WHERE i.k=o.k) q WHERE q.rn=1) AS r "
             "FROM VALUES (0),(1),(2) o(k) ORDER BY o.k"),
            ("duplicate_outer_rows", scalar(window("ROW_NUMBER() OVER (ORDER BY i.v DESC)"),
             "q.v", "(SELECT k FROM outer_keys) o", "o.k")
             .replace(") q) AS r", ") q WHERE q.w=1) AS r")),
            ("partitioned_rank", scalar(window("RANK() OVER (PARTITION BY i.g ORDER BY i.v)"))),
            ("dense_rank_ties", scalar(window("DENSE_RANK() OVER (ORDER BY i.v)"))),
            ("rows_frame", scalar(window("SUM(i.v) OVER (ORDER BY i.v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)"), "SUM(q.w*(q.v*q.v+1))")),
            ("range_frame_ties", scalar(window("SUM(i.v) OVER (ORDER BY i.v RANGE BETWEEN 10 PRECEDING AND CURRENT ROW)"), "SUM(q.w*(q.v*q.v+1))")),
            ("whole_partition_count", scalar(window("COUNT(*) OVER (PARTITION BY i.g)"), "SUM(q.w)")),
            ("null_safe_correlation", scalar(window("COUNT(i.v) OVER ()", predicate="i.k<=>o.k"))),
            ("local_filter", scalar(window("SUM(i.v) OVER ()", predicate="i.k=o.k AND i.v>=20"))),
            ("filtered_before_window", scalar("SELECT p.v,SUM(p.v) OVER (ORDER BY p.v "
             f"ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS w FROM ({filtered}) p", "SUM(q.w*(q.v*q.v+1))")),
            ("all_filtered_before_window", scalar("SELECT p.v,COUNT(*) OVER () AS w "
             f"FROM ({ranked}) p WHERE p.w>99")),
            ("different_window_specs", scalar("SELECT i.v,ROW_NUMBER() OVER (ORDER BY i.v DESC) AS rn,"
             "SUM(i.g) OVER (PARTITION BY i.g) AS w FROM window_inner i WHERE i.k=o.k",
             "q.w*100+q.v").replace(") q) AS r", ") q WHERE q.rn=1) AS r")),
            ("lag_and_lead", scalar(window("COALESCE(LAG(i.v,1+0,-1) OVER (ORDER BY i.v),-2)"
             "+COALESCE(LEAD(i.v,1,-3) OVER (ORDER BY i.v),-4)"))),
            ("computed_keys_and_arguments", scalar(window("SUM(i.v+o.id) OVER "
             "(PARTITION BY i.g+o.id ORDER BY i.v+o.id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING)"), "SUM(q.w*(q.v*q.v+1))")),
            ("distinct_before_window", scalar("SELECT p.v,COUNT(*) OVER () AS w FROM "
             "(SELECT DISTINCT i.v FROM window_inner i WHERE i.k=o.k) p")),
            ("limit_before_window", scalar("SELECT p.v,SUM(p.v) OVER () AS w FROM "
             "(SELECT i.v FROM window_inner i WHERE i.k=o.k ORDER BY i.v DESC LIMIT 2) p", "SUM(q.w)")),
            ("group_before_window", scalar("SELECT p.g,SUM(p.s) OVER (ORDER BY p.g) AS w FROM "
             "(SELECT i.g,SUM(i.v) AS s FROM window_inner i WHERE i.k=o.k GROUP BY i.g) p", "SUM(q.w*q.g)")),
            ("filtered_computed_argument", scalar("SELECT p.v,SUM(100 DIV (p.v-10)) OVER () AS w "
             f"FROM ({ranked}) p WHERE p.w>2")),
            ("uncorrelated_window_later_filter", scalar("SELECT SUM(i.v) OVER () AS w FROM ordinary i")
             .replace(") q) AS r", ") q WHERE o.k=1) AS r")),
            ("sorted_first_and_last", scalar("SELECT FIRST_VALUE(p.v) OVER ()*100+"
             "LAST_VALUE(p.v) OVER () AS w FROM (SELECT i.v FROM window_inner i "
             "WHERE i.k=o.k ORDER BY i.v DESC) p")),
            ("lag_row_default_sum", scalar(window("LAG(i.v,1,i.v+o.id) OVER (ORDER BY i.v)"), "SUM(q.w)")),
            ("nth_value_and_ntile", scalar(window("NTH_VALUE(i.v,1+1) OVER (ORDER BY i.v "
             "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)*100+"
             "NTILE(1+1) OVER (ORDER BY i.v)"), "SUM(q.w)")),
            ("lag_filtered_default_guard", "SELECT o.k,(SELECT MAX(q.w) FROM (SELECT "
             "LAG(p.v,1,100 DIV (p.v-10)) OVER (ORDER BY p.v) AS w FROM (SELECT i.v,"
             "ROW_NUMBER() OVER (ORDER BY i.v) AS rn FROM ordinary i WHERE i.k=o.k) p "
             "WHERE p.rn>1) q) AS r FROM VALUES (0),(1),(2) o(k) ORDER BY o.k"),
            ("filtered_computed_window_filters", scalar("SELECT "
             "SIZE(COLLECT_SET(100 DIV (p.v-20)) OVER ())*100+"
             "SIZE(COLLECT_LIST(100 DIV (p.v-20)) OVER ()) AS w "
             "FROM (SELECT i.v,ROW_NUMBER() OVER (ORDER BY i.v DESC) AS rn "
             "FROM window_inner i WHERE i.k=o.k) p WHERE p.rn<>2")),
            ("filtered_computed_partition_order", "SELECT o.k,(SELECT MAX(q.w) FROM (SELECT "
             "SUM(p.v) OVER (PARTITION BY 100 DIV (p.v-10) ORDER BY 100 DIV (p.v-10) "
             "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS w FROM (SELECT i.v,"
             "ROW_NUMBER() OVER (ORDER BY i.v) AS rn FROM ordinary i WHERE i.k=o.k) p "
             "WHERE p.rn>1) q) AS r FROM VALUES (0),(1),(2) o(k) ORDER BY o.k"),
            ("filtered_computed_input_order", "SELECT o.k,(SELECT MAX(q.w) FROM (SELECT "
             "FIRST_VALUE(p.v) OVER () AS w FROM (SELECT r.v FROM (SELECT i.v,"
             "ROW_NUMBER() OVER (ORDER BY i.v) AS rn FROM ordinary i WHERE i.k=o.k) r "
             "WHERE r.rn>1 ORDER BY 100 DIV (r.v-10)) p) q) AS r "
             "FROM VALUES (0),(1),(2) o(k) ORDER BY o.k"),
        ]
        guard_sql = ("SELECT o.k,(SELECT q.v FROM (SELECT i.v,ROW_NUMBER() OVER "
                     "(ORDER BY i.v) AS rn FROM ordinary i WHERE i.k=o.k) q WHERE q.rn<=2) AS r "
                     "FROM VALUES (0),(1),(2) o(k) ORDER BY o.k")
        for ansi in [False, True]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in expressions:
                print(json.dumps({"id": name, "ansi": ansi}), flush=True)
                frame = spark.sql(sql)
                rows = [encode(row, frame.schema) for row in frame.collect()]
                records.append({"id": name, "ansi": ansi, "sql": sql, "ordered": True,
                    "rows": rows, "schema": json.loads(frame._jdf.queryExecution().executedPlan().schema().json())})
            try:
                spark.sql(guard_sql).collect()
            except Exception as error:
                assert error.getCondition() == "SCALAR_SUBQUERY_TOO_MANY_ROWS", error
                records.append({"id": "cardinality_guard", "ansi": ansi, "sql": guard_sql,
                                "cardinality_error": True})
            else:
                raise AssertionError("scalar cardinality guard did not run")
        assert len(records) == 55
        with output.open("x") as destination:
            for record in records:
                destination.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations": len(records) - 1}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
