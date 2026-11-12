"""Capture valid correlated UNION ALL results and the scalar row-count guard."""

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
    spark = (SparkSession.builder.master("local[1]").appName("dar-correlated-union")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0"
        spark.sparkContext.setLogLevel("ERROR")
        schema = types.StructType([types.StructField("k", types.IntegerType()),
                                   types.StructField("v", types.IntegerType())])
        outer_schema = types.StructType([types.StructField("id", types.IntegerType(), False),
                                         types.StructField("k", types.IntegerType())])
        definitions = [
            ("ordinary", schema, [(1, 10), (1, 20), (2, 30)]),
            ("bag_inner", schema, [(1, 10), (1, 10), (1, 20), (2, 30),
                                    (2, None), (3, None), (None, 40)]),
            ("outer_keys", outer_schema, [(0, 0), (1, 1), (2, 1), (3, 2),
                                            (4, 3), (5, None)]),
        ]
        records = [{"spark_version": spark.version, "timezone": "UTC", "inputs": []}]
        for name, schema, rows in definitions:
            spark.createDataFrame(rows, schema).createOrReplaceTempView(name)
            records[0]["inputs"].append({"name": name, "schema": schema.jsonValue(),
                "rows": [[encode(value, field.dataType, input_value=True)
                          for value, field in zip(row, schema)] for row in rows]})
        union = ("SELECT i.v FROM bag_inner i WHERE i.k=o.k "
                 "UNION ALL SELECT j.v FROM bag_inner j WHERE j.k=o.k")
        single = ("SELECT i.v FROM ordinary i WHERE i.k=o.k AND i.v=20 "
                  "UNION ALL SELECT j.v FROM ordinary j WHERE j.k=o.k AND j.v=30")
        expressions = [
            ("issue_sum", "SELECT o.k,(SELECT SUM(q.v) FROM (" + single + ") q) AS r "
             "FROM VALUES (0),(1),(2) o(k) ORDER BY o.k"),
            ("duplicate_branch_sum", "SELECT o.id,o.k,(SELECT SUM(q.v) FROM (" + union + ") q) AS r "
             "FROM outer_keys o ORDER BY o.id"),
            ("duplicate_outer_rows", "SELECT o.k,(SELECT SUM(q.v) FROM (" + union + ") q) AS r "
             "FROM (SELECT k FROM outer_keys) o ORDER BY o.k"),
            ("multiple_union_columns", "SELECT o.id,(SELECT SUM(q.key*100+q.v) FROM ("
             "SELECT i.k AS key,i.v AS v FROM bag_inner i WHERE i.k=o.k UNION ALL "
             "SELECT j.v AS other_key,j.k AS other_value FROM ordinary j WHERE j.k=o.k) q) AS r "
             "FROM outer_keys o ORDER BY o.id"),
            ("duplicate_branch_count", "SELECT o.id,(SELECT COUNT(*) FROM (" + union + ") q) AS r "
             "FROM outer_keys o ORDER BY o.id"),
            ("distinct_after_union", "SELECT o.id,(SELECT COUNT(*) FROM (SELECT DISTINCT q.v FROM (" + union +
             ") q) d) AS r FROM outer_keys o ORDER BY o.id"),
            ("group_after_union", "SELECT o.id,(SELECT MAX(d.s) FROM (SELECT q.v,SUM(q.v) AS s FROM (" +
             union + ") q GROUP BY q.v) d) AS r FROM outer_keys o ORDER BY o.id"),
            ("limit_after_union", "SELECT o.id,(SELECT SUM(d.v) FROM (SELECT q.v FROM (" + union +
             ") q ORDER BY q.v DESC LIMIT 2) d) AS r FROM outer_keys o ORDER BY o.id"),
            ("scalar_one_or_zero", "SELECT o.id,(SELECT q.v FROM (" + single + ") q) AS r "
             "FROM outer_keys o ORDER BY o.id"),
            ("three_branches", "SELECT o.id,(SELECT SUM(q.v) FROM ("
             "SELECT i.v FROM bag_inner i WHERE i.k=o.k AND i.v<20 UNION ALL "
             "SELECT j.v FROM ordinary j WHERE j.k=o.k AND j.v>=20 UNION ALL "
             "SELECT n.v FROM bag_inner n WHERE n.k=o.k AND n.v IS NULL) q) AS r "
             "FROM outer_keys o ORDER BY o.id"),
            ("non_equality_filters", "SELECT o.id,(SELECT SUM(q.amount) FROM ("
             "SELECT i.v AS amount FROM ordinary i WHERE i.k<=o.k AND i.v<20 UNION ALL "
             "SELECT j.k AS different FROM ordinary j WHERE j.k>o.k AND j.v>=20) q) AS r "
             "FROM outer_keys o ORDER BY o.id"),
            ("uncorrelated_branch", "SELECT o.id,(SELECT SUM(q.v) FROM ("
             "SELECT i.v FROM ordinary i WHERE i.k=o.k UNION ALL "
             "SELECT j.v FROM ordinary j WHERE j.k=2) q) AS r FROM outer_keys o ORDER BY o.id"),
            ("outer_only_filter", "SELECT o.id,(SELECT COUNT(*) FROM ("
             "SELECT i.v FROM ordinary i WHERE o.k=o.k UNION ALL "
             "SELECT j.v FROM ordinary j WHERE j.k=o.k) q) AS r FROM outer_keys o ORDER BY o.id"),
            ("two_outer_columns", "SELECT o.id,(SELECT SUM(q.v) FROM ("
             "SELECT i.v FROM bag_inner i WHERE i.k=o.k AND i.v>=o.id UNION ALL "
             "SELECT j.v FROM ordinary j WHERE j.k=o.k AND j.v<o.id) q) AS r "
             "FROM outer_keys o ORDER BY o.id"),
            ("null_safe_keys", "SELECT o.id,(SELECT SUM(q.v) FROM ("
             "SELECT i.v FROM bag_inner i WHERE i.k<=>o.k UNION ALL "
             "SELECT j.v FROM ordinary j WHERE j.k=o.k) q) AS r FROM outer_keys o ORDER BY o.id"),
            ("correlated_projection", "SELECT o.id,(SELECT SUM(q.v) FROM ("
             "SELECT i.v+o.k AS v FROM ordinary i WHERE i.k=o.k AND i.v=20 UNION ALL "
             "SELECT j.v-o.k AS v FROM ordinary j WHERE j.k=o.k AND j.v=30) q) AS r "
             "FROM outer_keys o ORDER BY o.id"),
            ("volatile_outer", "SELECT o.id,COALESCE((SELECT SUM(q.v) FROM ("
             "SELECT o.r AS v FROM ordinary i WHERE i.k=o.k AND i.v>=20 UNION ALL "
             "SELECT o.r AS v FROM ordinary j WHERE j.k=o.k AND j.v>=20) q)=2*o.r,FALSE) AS r "
             "FROM (SELECT id,k,RAND() AS r FROM outer_keys) o ORDER BY o.id"),
        ]
        guard_sql = ("SELECT o.id,(SELECT q.v FROM (" + union +
                     ") q) AS r FROM outer_keys o ORDER BY o.id")
        for ansi in [False, True]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in expressions:
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
        assert len(records) == 37
        with output.open("x") as destination:
            for record in records:
                destination.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations": len(records) - 1}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
