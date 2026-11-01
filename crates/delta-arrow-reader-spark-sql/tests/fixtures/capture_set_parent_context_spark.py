"""Capture context-dependent zero results without inspecting DAR output."""
import json
import os
import sys
import struct
from pathlib import Path

from pyspark.sql import SparkSession, types

os.environ["PYSPARK_PYTHON"] = sys.executable
origin = Path(__file__).with_name("set-operation-float-keys-spark.jsonl")
metadata = json.loads(origin.read_text().splitlines()[0])
queries=[]
negative="CAST('-0.0' AS DOUBLE)"
positive="CAST('0.0' AS DOUBLE)"
outer=f"SELECT q.k FROM double_mixed_left p LEFT JOIN (SELECT {negative} AS k,label FROM double_negative) q ON p.label=q.label"
inner=f"({outer}) INTERSECT (SELECT k FROM double_positive)"
for name,query in [
    ("outer_nonnull_filter",f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM ({inner}) s WHERE k IS NOT NULL"),
    ("outer_literal_set",f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM (({inner}) INTERSECT (SELECT {positive} AS k)) s"),
    ("outer_constant_set",f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM (({inner}) EXCEPT (SELECT CAST(NULL AS DOUBLE) AS k)) s"),
]: queries.append((name,query))
for depth in [2,6]:
    left=f"SELECT q.a,q.b FROM double_mixed_left p LEFT JOIN (SELECT {negative} AS a,{negative} AS b,label FROM double_negative) q ON p.label=q.label"
    for _ in range(depth): left=f"SELECT a+b AS a,a+b AS b FROM ({left}) q"
    queries.append((f"nullable_diamond_{depth}",f"SELECT CAST(a AS STRING) AS text,CAST(b AS STRING) AS negated FROM (({left}) INTERSECT (SELECT {positive} AS a,{positive} AS b)) q"))
group=f"SELECT k FROM ({outer}) q GROUP BY k"
queries.extend([
    ("shared_cte_contexts",f"WITH s AS ({inner}) SELECT 'filtered' AS text,CAST(k AS STRING) AS negated FROM s WHERE k IS NOT NULL UNION ALL SELECT 'unfiltered' AS text,CAST(k AS STRING) AS negated FROM s"),
    ("grouped_nullable_side",f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM (({group}) INTERSECT SELECT {positive} AS k) s"),
    ("outer_filter_after_limit",f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM ({inner} LIMIT 1) s WHERE k IS NOT NULL"),
    ("outer_join_predicate",f"SELECT CAST(s.k AS STRING) AS text,CAST(-s.k AS STRING) AS negated FROM ({inner}) s INNER JOIN double_positive r ON s.k=r.k"),
    ("outer_window_partition_filter",f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM (SELECT k,COUNT(*) OVER (PARTITION BY k) AS n FROM ({inner}) s) w WHERE k IS NOT NULL AND n>0"),
    ("right_cast_alias",f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM (({outer}) INTERSECT (SELECT CAST(s AS DOUBLE) AS k FROM (SELECT '0.0' AS s FROM double_positive) q)) q"),
])
for name, query in [
    ("union_left_filter", f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM (({inner}) UNION ALL SELECT CAST(1.0 AS DOUBLE) AS k) u WHERE k IS NOT NULL"),
    ("union_right_filter", f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM ((SELECT CAST(1.0 AS DOUBLE) AS k) UNION ALL ({inner})) u WHERE k IS NOT NULL"),
    ("union_unfiltered", f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM (({inner}) UNION ALL SELECT CAST(1.0 AS DOUBLE) AS k) u"),
]: queries.append((name, query))
for name, predicate in [
    ("predicate_eq", "k=0"), ("predicate_ge", "k>=0"), ("predicate_le", "k<=0"),
    ("predicate_bool_null_accepting", "(k IS NOT NULL) IS NOT NULL"),
    ("predicate_bool_value", "(k IS NOT NULL)=false"),
    ("predicate_null_accepting_or", "k=0 OR k IS NULL"),
]: queries.append((name, f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM ({inner}) s WHERE {predicate}"))
for name,predicate in [
    ("predicate_abs", "abs(k)=0"),
    ("predicate_coalesce", "coalesce(k,1)=0"),
    ("predicate_coalesce_accepts_null", "coalesce(k,0)=0"),
    ("predicate_not_isnull", "NOT (k IS NULL)"),
]: queries.append((name,f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM ({inner}) s WHERE {predicate}"))
queries.append(("volatile_projection_limit", f"SELECT CAST(k AS STRING) AS text,CAST(z>=0 AND z<2 AS STRING) AS negated FROM (SELECT k,r+r AS z FROM (SELECT k,rand(1) AS r FROM ({inner}) s) a LIMIT 1) b"))
queries.extend([
    ("unused_volatile_projection_limit", f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM (SELECT k,r+r AS z FROM (SELECT k,rand(1) AS r FROM ({inner}) s) a LIMIT 1) b"),
    ("single_volatile_projection_limit", f"SELECT CAST(k AS STRING) AS text,CAST(r>=0 AND r<1 AS STRING) AS negated FROM (SELECT k,rand(1) AS r FROM ({inner}) s LIMIT 1) q"),
    ("scalar_above_limit", f"SELECT CAST(k AS STRING) AS text,CAST((SELECT COUNT(*) FROM double_positive) AS STRING) AS negated FROM ({inner} LIMIT 1) q"),
    ("exists_above_limit", f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM ({inner} LIMIT 1) q WHERE EXISTS(SELECT k FROM double_positive)"),
    ("unused_volatile_filtered_limit", f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM (SELECT k,rand(1) AS r FROM ({inner}) s LIMIT 1) q WHERE k IS NOT NULL"),
    ("direct_limit", f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM ({inner} LIMIT 1) b"),
    ("identity_projection_limit", f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM (SELECT k FROM ({inner}) s LIMIT 1) b"),
    ("nested_identity_projection_limit", f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM (SELECT k FROM (SELECT k FROM ({inner}) s) a LIMIT 1) b"),
    ("unused_constant_projection_limit", f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM (SELECT k,r+r AS z FROM (SELECT k,1 AS r FROM ({inner}) s) a LIMIT 1) b"),
    ("unused_function_projection_limit", f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM (SELECT k,r+r AS z FROM (SELECT k,abs(k) AS r FROM ({inner}) s) a LIMIT 1) b"),
    ("single_unused_volatile_projection_limit", f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM (SELECT k,rand(1) AS r FROM ({inner}) s LIMIT 1) b"),
    ("unused_volatile_alias_limit", f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM (SELECT k,r AS z FROM (SELECT k,rand(1) AS r FROM ({inner}) s) a LIMIT 1) b"),
    ("unused_volatile_filtered_nested_limit", f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM (SELECT k,r+r AS z FROM (SELECT k,rand(1) AS r FROM ({inner}) s) a LIMIT 1) b WHERE k IS NOT NULL"),
    ("unused_volatile_cast_before_limit", f"SELECT text,negated FROM (SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated,r+r AS z FROM (SELECT k,rand(1) AS r FROM ({inner}) s) a LIMIT 1) b"),
])
extra=[]
for operator in ["INTERSECT", "EXCEPT", "INTERSECT ALL", "EXCEPT ALL"]:
    if operator.startswith("EXCEPT"):
        rhs="SELECT CAST(1.0 AS DOUBLE) AS k FROM double_positive"
    else: rhs="SELECT k FROM double_positive"
    source=f"SELECT k FROM double_negative {operator} {rhs}"
    for suffix,clause in [("one","LIMIT 1"),("two","LIMIT 2"),("offset","LIMIT 1 OFFSET 1"),("ordered","ORDER BY k LIMIT 1")]:
        extra.append((operator.lower().replace(" ","_")+"_"+suffix,f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM ({source} {clause}) s"))
queries.extend(extra)
queries=[(kind+"_"+name,sql.replace("DOUBLE",kind.upper()).replace("double_",kind+"_")) for kind in ["float","double"] for name,sql in queries]
assert len(queries)==110
spark = (SparkSession.builder.master("local[1]").appName("dar-nullable-set-constants")
    .config("spark.ui.enabled","false").config("spark.sql.shuffle.partitions","2")
    .config("spark.sql.session.timeZone","UTC").config("spark.sql.adaptive.enabled","false")
    .config("spark.sql.ui.retainedExecutions","1").config("spark.ui.retainedJobs","1")
    .config("spark.ui.retainedStages","1").getOrCreate())
assert spark.version == "4.2.0"
spark.sparkContext.setLogLevel("ERROR")
try:
    for item in metadata["inputs"]:
        is_float = item["type"] == "float"
        data_type = types.FloatType() if is_float else types.DoubleType()
        def number(bits):
            return None if bits is None else struct.unpack(">f" if is_float else ">d",bytes.fromhex(bits))[0]
        rows = [(number(row[0]),row[1]) for row in item["rows"]]
        schema = types.StructType([types.StructField("k",data_type,True),types.StructField("label",types.StringType(),True)])
        spark.createDataFrame(spark.sparkContext.parallelize(rows,2),schema).createOrReplaceTempView(item["name"])
    for kind in ["float","double"]:
        data_type=types.FloatType() if kind=="float" else types.DoubleType()
        spark.createDataFrame([(0.0,)],types.StructType([types.StructField("k",data_type,False)])).createOrReplaceTempView(kind+"_nonnull")
    path=Path(__file__).with_name("set-parent-context-spark.jsonl")
    with path.open("x") as output:
        output.write(json.dumps({**metadata,"adaptive":False,"input_partitions":2,"nonnull_tables":["float_nonnull","double_nonnull"]})+"\n")
        count=0
        for ansi in [False,True]:
            spark.conf.set("spark.sql.ansi.enabled",str(ansi).lower())
            for name,sql in queries:
                frame=spark.sql(sql)
                rows=[list(row) for row in frame.collect()]
                rows.sort(key=lambda row:json.dumps(row,separators=(",",":")))
                plan=frame._jdf.queryExecution().optimizedPlan().toString()
                output.write(json.dumps({"id":name,"sql":sql,"ansi":ansi,"schema":frame.schema.jsonValue(),"rows":rows,"optimized_plan":plan},separators=(",",":"))+"\n")
                output.flush()
                count+=1
                if count%8==0:print(json.dumps({"observations":count}),flush=True)
        assert count==220
finally:
    spark.stop()
