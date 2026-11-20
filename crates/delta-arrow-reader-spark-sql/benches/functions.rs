//! Frozen, execution-only function measurements for issue #1391.

use std::{collections::BTreeSet, error::Error, fs, path::Path, sync::Arc, time::Instant};

use arrow::{
    array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array,
        Float64Builder, Int32Array, Int64Array, Int64Builder, ListArray, ListBuilder, MapBuilder,
        StringArray, StringBuilder, StructArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    },
    buffer::{NullBuffer, OffsetBuffer},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
#[expect(
    deprecated,
    reason = "Only untimed preflight uses the standard batch guard metrics."
)]
use datafusion::{
    datasource::MemTable,
    execution::{
        SessionStateBuilder, memory_pool::GreedyMemoryPool, runtime_env::RuntimeEnvBuilder,
    },
    logical_expr::{AggregateUDF, ScalarUDF, WindowUDF},
    physical_expr::{
        PhysicalExpr, ScalarFunctionExpr,
        expressions::Column,
        window::{
            PlainAggregateWindowExpr, SlidingAggregateWindowExpr, StandardWindowExpr, WindowExpr,
        },
    },
    physical_plan::{
        ExecutionPlan,
        aggregates::AggregateExec,
        coalesce_batches::CoalesceBatchesExec,
        displayable, execute_stream,
        projection::ProjectionExec,
        windows::{BoundedWindowAggExec, WindowAggExec, WindowUDFExpr},
    },
    prelude::{SessionConfig, SessionContext},
};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use futures_util::TryStreamExt;
use serde::Deserialize;
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "../tests/support/spark_results.rs"]
mod spark_results;
use spark_results::encode_value;

type Result<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const ROWS: usize = 1_000_000;
const BATCH: usize = 8192;

#[derive(Deserialize)]
struct Case {
    id: String,
    spark_sql: Option<String>,
    native_sql: Option<String>,
    projection_sql: Option<String>,
}
#[derive(Deserialize)]
struct Manifest {
    functions: Vec<Case>,
}

fn seeded(row: usize) -> u64 {
    let mut value = (row as u64).wrapping_add(0x1391_5eed_2026_1119);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn input() -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    for start in (0..ROWS).step_by(BATCH) {
        let end = (start + BATCH).min(ROWS);
        let values = (start..end).map(seeded).collect::<Vec<_>>();
        let valid = |index: usize| !(start + index).is_multiple_of(16);
        let ints = values
            .iter()
            .enumerate()
            .map(|(n, v)| valid(n).then_some((v % 31) as i64 - 15))
            .collect::<Vec<_>>();
        let positive = values
            .iter()
            .enumerate()
            .map(|(n, v)| valid(n).then_some((v % 15 + 1) as i32))
            .collect::<Vec<_>>();
        let i: ArrayRef = Arc::new(Int64Array::from(ints));
        let mut arr = ListBuilder::new(Int64Builder::new());
        let mut vector = ListBuilder::new(Float64Builder::new());
        let mut map = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        let mut map2 = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        for (n, v) in values.iter().enumerate() {
            if valid(n) {
                arr.values().append_value((v % 15) as i64);
                arr.values().append_null();
                arr.values().append_value((v % 7) as i64);
                vector.values().append_value((v % 15) as f64);
                vector.values().append_value((v % 7) as f64);
                map.keys().append_value("a");
                map.values().append_value((v % 15) as i64);
                map.keys().append_value("b");
                map.values().append_null();
                map2.keys().append_value("c");
                map2.values().append_value((v % 15) as i64);
                map2.keys().append_value("d");
                map2.values().append_null();
            }
            arr.append(valid(n));
            vector.append(valid(n));
            map.append(valid(n))?;
            map2.append(valid(n))?;
        }
        let string_column = |f: &dyn Fn(u64) -> String| -> ArrayRef {
            Arc::new(StringArray::from(
                values
                    .iter()
                    .enumerate()
                    .map(|(n, v)| valid(n).then(|| f(*v)))
                    .collect::<Vec<_>>(),
            ))
        };
        let mut hll = datasketches::hll::HllSketch::new(4, datasketches::hll::HllType::Hll8);
        hll.update(1_i64);
        let hll = hll.serialize();
        let mut theta = datasketches::theta::ThetaSketch::builder().lg_k(5).build();
        theta.update(1_i64);
        let theta = theta.compact(true).serialize();
        let mut wkb = vec![1_u8, 1, 0, 0, 0];
        wkb.extend_from_slice(&1.0_f64.to_le_bytes());
        wkb.extend_from_slice(&2.0_f64.to_le_bytes());
        let arr: ArrayRef = Arc::new(arr.finish());
        let nulls = NullBuffer::from((0..end - start).map(valid).collect::<Vec<_>>());
        let st: ArrayRef = Arc::new(StructArray::try_new(
            vec![Arc::new(Field::new("x", DataType::Int64, true))].into(),
            vec![i.clone()],
            Some(nulls.clone()),
        )?);
        let offsets = OffsetBuffer::new(
            (0..=end - start)
                .map(|n| n as i32)
                .collect::<Vec<_>>()
                .into(),
        );
        let nested: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", arr.data_type().clone(), true)),
            offsets.clone(),
            arr.clone(),
            Some(nulls.clone()),
        ));
        let records: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", st.data_type().clone(), true)),
            offsets,
            st.clone(),
            Some(nulls),
        ));
        // AES-128-ECB/PKCS7 of b"aGVsbG8=" with the public fixture key b"0123456789abcdef".
        let cipher = [
            0x5e_u8, 0x97, 0x3c, 0x58, 0x3b, 0x41, 0xba, 0xf1, 0x44, 0x78, 0x82, 0xf2, 0xf2, 0x19,
            0xec, 0xb7,
        ];
        let fields = vec![
            (
                "id",
                Arc::new(Int64Array::from_iter_values((start..end).map(|n| n as i64))) as ArrayRef,
            ),
            (
                "grp",
                Arc::new(Int32Array::from_iter_values(
                    (start..end).map(|n| (n % 4096) as i32),
                )) as ArrayRef,
            ),
            (
                "high_key",
                Arc::new(Int64Array::from_iter_values(
                    (start..end).map(|n| (n % 250_000) as i64),
                )) as ArrayRef,
            ),
            ("i", i.clone()),
            ("p", Arc::new(Int32Array::from(positive)) as ArrayRef),
            (
                "d",
                Arc::new(Float64Array::from(
                    values
                        .iter()
                        .enumerate()
                        .map(|(n, v)| valid(n).then_some((v % 8 + 1) as f64 / 10.0))
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            (
                "dec",
                Arc::new(
                    Decimal128Array::from(
                        values
                            .iter()
                            .enumerate()
                            .map(|(n, v)| valid(n).then_some((v % 100_000) as i128 + 1))
                            .collect::<Vec<_>>(),
                    )
                    .with_precision_and_scale(15, 2)?,
                ) as ArrayRef,
            ),
            ("s", string_column(&|v| format!("item-{:04}", v % 1000))),
            ("s2", string_column(&|v| format!("x{:02}", v % 17))),
            (
                "map_s",
                string_column(&|v| format!("item-{:04}:x{:02}", v % 1000, v % 17)),
            ),
            (
                "json_array_s",
                string_column(&|v| format!("[{},2,3]", v % 15 + 1)),
            ),
            ("time_s", string_column(&|v| format!("12:34:{:02}", v % 60))),
            (
                "num_s",
                string_column(&|v| format!("{}.{:02}", v % 1000, v % 100)),
            ),
            (
                "date_s",
                string_column(&|v| format!("2020-01-{:02}", v % 28 + 1)),
            ),
            (
                "ts_s",
                string_column(&|v| format!("2020-01-{:02} 12:34:56", v % 28 + 1)),
            ),
            (
                "json_s",
                string_column(&|v| format!("{{\"x\":{},\"a\":[1,2]}}", v % 31)),
            ),
            ("csv_s", string_column(&|v| format!("{},item", v % 31))),
            (
                "xml_s",
                string_column(&|v| format!("<r><x>{}</x></r>", v % 31)),
            ),
            (
                "url_s",
                string_column(&|v| format!("https://example.com/a?q={}", v % 31)),
            ),
            (
                "b",
                Arc::new(BinaryArray::from_iter(
                    values
                        .iter()
                        .enumerate()
                        .map(|(n, _)| valid(n).then_some(b"aGVsbG8=".as_slice())),
                )) as ArrayRef,
            ),
            ("hex_s", string_column(&|v| format!("{:08x}", v % 1000))),
            (
                "hll_b",
                Arc::new(BinaryArray::from_iter(
                    values
                        .iter()
                        .enumerate()
                        .map(|(n, _)| valid(n).then_some(hll.as_slice())),
                )) as ArrayRef,
            ),
            (
                "theta_b",
                Arc::new(BinaryArray::from_iter(
                    values
                        .iter()
                        .enumerate()
                        .map(|(n, _)| valid(n).then_some(theta.as_slice())),
                )) as ArrayRef,
            ),
            (
                "dt",
                Arc::new(Date32Array::from(
                    values
                        .iter()
                        .enumerate()
                        .map(|(n, v)| valid(n).then_some(18_262 + (v % 365) as i32))
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            (
                "ts",
                Arc::new(
                    TimestampMicrosecondArray::from(
                        values
                            .iter()
                            .enumerate()
                            .map(|(n, v)| {
                                valid(n).then_some(
                                    1_577_836_800_000_000 + (v % 86400) as i64 * 1_000_000,
                                )
                            })
                            .collect::<Vec<_>>(),
                    )
                    .with_timezone("UTC"),
                ) as ArrayRef,
            ),
            (
                "flag",
                Arc::new(BooleanArray::from(
                    values
                        .iter()
                        .enumerate()
                        .map(|(n, v)| valid(n).then_some(v % 2 == 0))
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            ("arr", arr),
            ("nested", nested),
            ("records", records),
            (
                "cipher",
                Arc::new(BinaryArray::from_iter(
                    values
                        .iter()
                        .enumerate()
                        .map(|(n, _)| valid(n).then_some(cipher.as_slice())),
                )) as ArrayRef,
            ),
            ("vec", Arc::new(vector.finish()) as ArrayRef),
            (
                "wkb",
                Arc::new(BinaryArray::from_iter(
                    values
                        .iter()
                        .enumerate()
                        .map(|(n, _)| valid(n).then_some(wkb.as_slice())),
                )) as ArrayRef,
            ),
            ("m", Arc::new(map.finish()) as ArrayRef),
            ("m2", Arc::new(map2.finish()) as ArrayRef),
            (
                "tm",
                Arc::new(Time64MicrosecondArray::from(
                    values
                        .iter()
                        .enumerate()
                        .map(|(n, v)| {
                            valid(n).then_some((12 * 3600 + 34 * 60 + (v % 60) as i64) * 1_000_000)
                        })
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            ("st", st),
        ];
        batches.push(RecordBatch::try_from_iter(fields)?);
    }
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        ROWS
    );
    Ok(batches)
}

struct Sessions {
    native: SessionContext,
    spark: SessionContext,
}
impl Sessions {
    fn get(&self, configuration: &str) -> &SessionContext {
        if configuration == "native" {
            &self.native
        } else {
            &self.spark
        }
    }
}

fn sessions() -> Result<Sessions> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(GreedyMemoryPool::new(2 * 1024 * 1024 * 1024)))
        .build_arc()?;
    let plain = SessionStateBuilder::new()
        .with_default_features()
        .with_config(
            SessionConfig::new()
                .with_target_partitions(1)
                .with_batch_size(BATCH),
        )
        .with_runtime_env(runtime.clone())
        .build();
    let rules = plain
        .physical_optimizers()
        .iter()
        .filter(|rule| rule.name() != "aggregate_statistics")
        .cloned()
        .collect::<Vec<_>>();
    let native = SessionContext::new_with_state(
        SessionStateBuilder::new_from_existing(plain.clone())
            .with_physical_optimizer_rules(rules.clone())
            .build(),
    );
    let spark = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_target_partitions(1)
                        .with_batch_size(BATCH),
                )
                .with_runtime_env(runtime)
                .with_physical_optimizer_rules(rules),
        )
        .build(),
    );
    let batches = input()?;
    let table = Arc::new(MemTable::try_new(batches[0].schema(), vec![batches])?);
    native.register_table("bench", table.clone())?;
    spark.register_table("bench", table)?;
    Ok(Sessions { native, spark })
}

async fn plan(
    context: &SessionContext,
    sql: &str,
    configuration: &str,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(if configuration == "native" {
        context.sql(sql).await?.create_physical_plan().await?
    } else {
        plan_query(
            context,
            sql,
            &SparkSqlOptions {
                ansi_mode: configuration == "spark-on",
                session_timezone: "UTC".into(),
                ..Default::default()
            },
        )
        .await?
        .create_physical_plan(context)
        .await?
    })
}

fn types(schema: &Schema) -> Value {
    json!(
        schema
            .fields()
            .iter()
            .map(|field| format!("{:?}", field.data_type()))
            .collect::<Vec<_>>()
    )
}

#[derive(Clone, Debug, PartialEq)]
enum Kernel {
    Scalar(ScalarUDF),
    Aggregate(AggregateUDF),
    Window(WindowUDF),
}

#[derive(Default)]
struct FunctionEvidence {
    kernels: Vec<Kernel>,
    expressions: Vec<Arc<dyn PhysicalExpr>>,
    unknown_window: bool,
}
impl FunctionEvidence {
    fn expression(&mut self, expression: &Arc<dyn PhysicalExpr>) {
        if let Some(function) = expression.downcast_ref::<ScalarFunctionExpr>() {
            self.kernels.push(Kernel::Scalar(function.fun().clone()));
        }
        for child in expression.children() {
            self.expression(child);
        }
    }
    fn window(&mut self, expression: &dyn WindowExpr) {
        if let Some(standard) = expression.as_any().downcast_ref::<StandardWindowExpr>() {
            if let Some(function) = standard
                .get_standard_func_expr()
                .as_any()
                .downcast_ref::<WindowUDFExpr>()
            {
                self.kernels
                    .push(Kernel::Window(function.fun().as_ref().clone()));
                for argument in function.args() {
                    self.expressions.push(argument.clone());
                    self.expression(argument);
                }
            } else {
                self.unknown_window = true;
            }
        } else if let Some(aggregate) = expression
            .as_any()
            .downcast_ref::<PlainAggregateWindowExpr>()
        {
            self.kernels.push(Kernel::Aggregate(
                aggregate.get_aggregate_expr().fun().clone(),
            ));
            self.expressions
                .extend(aggregate.get_aggregate_expr().expressions());
        } else if let Some(aggregate) = expression
            .as_any()
            .downcast_ref::<SlidingAggregateWindowExpr>()
        {
            self.kernels.push(Kernel::Aggregate(
                aggregate.get_aggregate_expr().fun().clone(),
            ));
            self.expressions
                .extend(aggregate.get_aggregate_expr().expressions());
        } else {
            self.unknown_window = true;
        }
    }
    fn visit(&mut self, plan: &dyn ExecutionPlan) {
        if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
            for projected in projection.expr() {
                if projected.expr.downcast_ref::<Column>().is_none() {
                    self.expressions.push(projected.expr.clone());
                    self.expression(&projected.expr);
                }
            }
        }
        if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
            for expression in aggregate.aggr_expr() {
                self.kernels
                    .push(Kernel::Aggregate(expression.fun().clone()));
                for argument in expression.expressions() {
                    self.expressions.push(argument.clone());
                    self.expression(&argument);
                }
            }
        }
        if let Some(window) = plan.downcast_ref::<WindowAggExec>() {
            for expression in window.window_expr() {
                self.window(expression.as_ref());
            }
        }
        if let Some(window) = plan.downcast_ref::<BoundedWindowAggExec>() {
            for expression in window.window_expr() {
                self.window(expression.as_ref());
            }
        }
        for child in plan.children() {
            self.visit(child.as_ref());
        }
    }
    fn same(&self, other: &Self) -> bool {
        !self.unknown_window
            && !other.unknown_window
            && (!self.kernels.is_empty() || !self.expressions.is_empty())
            && self.kernels == other.kernels
            && self.expressions.len() == other.expressions.len()
            && self
                .expressions
                .iter()
                .zip(&other.expressions)
                .all(|(a, b)| same_expression(a, b))
    }
    fn record(&self) -> Value {
        json!({"kernels":format!("{:?}",self.kernels),"expressions":self.expressions.iter().map(|expr| format!("{expr:?}")).collect::<Vec<_>>(),"unknown_window":self.unknown_window})
    }
}

fn same_expression(a: &Arc<dyn PhysicalExpr>, b: &Arc<dyn PhysicalExpr>) -> bool {
    if a.eq(b) {
        return true;
    }
    let (Some(a), Some(b)) = (
        a.downcast_ref::<ScalarFunctionExpr>(),
        b.downcast_ref::<ScalarFunctionExpr>(),
    ) else {
        return false;
    };
    // These macro-generated native kernels ignore ConfigOptions (datafusion-functions/src/macros.rs).
    const PURE: &[&str] = &[
        "acos", "acosh", "asin", "asinh", "atan", "atanh", "cbrt", "cos", "cosh", "degrees", "exp",
        "radians", "sin", "sinh", "sqrt", "tan", "tanh",
    ];
    PURE.contains(&a.fun().name())
        && a.fun() == b.fun()
        && a.return_type() == b.return_type()
        && a.args().len() == b.args().len()
        && a.args()
            .iter()
            .zip(b.args())
            .all(|(a, b)| same_expression(a, b))
}

async fn complete_rows(
    context: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<(Vec<String>, Value)> {
    let executed_types = types(plan.schema().as_ref());
    let mut guards = Vec::new();
    let plan = guard_sources(plan, &mut guards)?;
    if guards.is_empty() {
        return Err("physical statement does not execute an input source".into());
    }
    let mut stream = execute_stream(plan, context.task_ctx())?;
    let mut rows = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        if types(batch.schema().as_ref()) != executed_types {
            return Err("actual executed batch type differs from the physical schema".into());
        }
        for row in 0..batch.num_rows() {
            let values = batch
                .columns()
                .iter()
                .map(|column| encode_value(column.as_ref(), row))
                .collect::<Result<Vec<_>>>()?;
            rows.push(serde_json::to_string(&values)?);
        }
    }
    let consumed = guards
        .iter()
        .map(|guard| {
            guard
                .metrics()
                .and_then(|metrics| metrics.output_rows())
                .unwrap_or(0)
        })
        .sum::<usize>();
    if consumed != ROWS {
        return Err(format!("source guard consumed {consumed} rows; expected {ROWS}").into());
    }
    rows.sort_unstable();
    Ok((rows, executed_types))
}

#[expect(
    deprecated,
    reason = "Only untimed preflight uses the standard batch guard metrics."
)]
fn guard_sources(
    plan: Arc<dyn ExecutionPlan>,
    guards: &mut Vec<Arc<CoalesceBatchesExec>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if plan.children().is_empty() {
        if plan.name() == "DataSourceExec" {
            let guard = Arc::new(CoalesceBatchesExec::new(plan, BATCH));
            guards.push(guard.clone());
            return Ok(guard);
        }
        return Ok(plan);
    }
    let children = plan
        .children()
        .iter()
        .map(|child| guard_sources((*child).clone(), guards))
        .collect::<Result<Vec<_>>>()?;
    Ok(plan.with_new_children(children)?)
}

fn compare(left: &(Vec<String>, Value), right: &(Vec<String>, Value)) -> Value {
    let same_types = left.1 == right.1;
    let same_rows = left.0 == right.0;
    json!({"equal":same_types && same_rows,"same_types":same_types,"same_rows":same_rows,
        "native_rows":left.0.len(),"spark_rows":right.0.len(),"native_types":left.1,"spark_types":right.1,
        "first_difference":left.0.iter().zip(&right.0).position(|(a,b)| a != b)})
}

fn self_check() {
    let a = (vec!["a".into(), "b".into(), "b".into()], json!(["Int64"]));
    assert_eq!(compare(&a, &a)["equal"], true);
    for bad in [
        (vec!["a".into(), "b".into()], json!(["Int64"])),
        (vec!["a".into(), "b".into(), "c".into()], json!(["Int64"])),
        (vec!["a".into(), "b".into(), "b".into()], json!(["Int32"])),
        (
            vec!["a".into(), "b".into(), "null".into()],
            json!(["Int64"]),
        ),
        (
            vec!["a".into(), "b".into(), "b".into(), "b".into()],
            json!(["Int64"]),
        ),
    ] {
        assert_eq!(compare(&a, &bad)["equal"], false);
    }
}

async fn preflight(sessions: &Sessions, case: &Case, work: &Path) -> Result<Value> {
    fs::create_dir_all(work)?;
    let sql = case.spark_sql.as_ref().ok_or("excluded case has no SQL")?;
    let mut results = Vec::new();
    let mut native = None;
    let mut native_plan = None;
    let mut native_functions = None;
    if let Some(sql) = &case.native_sql {
        let context = sessions.get("native");
        match plan(context, sql, "native").await {
            Ok(physical) => {
                let text = displayable(physical.as_ref()).indent(true).to_string();
                fs::write(work.join("native.plan"), &text)?;
                native_plan = Some(text);
                let mut evidence = FunctionEvidence::default();
                evidence.visit(physical.as_ref());
                native_functions = Some(evidence);
                match complete_rows(context, physical).await {
                    Ok(rows) => { results.push(json!({"configuration":"native","status":"ok","rows":rows.0.len(),"types":rows.1,"source_guard_rows":ROWS})); native=Some(rows); }
                    Err(error) => results.push(json!({"configuration":"native","status":"execution_error","error":error.to_string()})),
                }
            }
            Err(error) => results.push(json!({"configuration":"native","status":"planning_error","error":error.to_string()})),
        }
    }
    for configuration in ["spark-off", "spark-on"] {
        let context = sessions.get(configuration);
        let physical = match plan(context, sql, configuration).await {
            Ok(physical) => physical,
            Err(error) => {
                results.push(json!({"configuration":configuration,"status":"planning_error","error":error.to_string()}));
                continue;
            }
        };
        let text = displayable(physical.as_ref()).indent(true).to_string();
        let mut evidence = FunctionEvidence::default();
        evidence.visit(physical.as_ref());
        fs::write(work.join(format!("{configuration}.plan")), &text)?;
        let rows = match complete_rows(context, physical).await {
            Ok(rows) => rows,
            Err(error) => {
                results.push(json!({"configuration":configuration,"status":"execution_error","error":error.to_string()}));
                continue;
            }
        };
        let comparison = native.as_ref().map(|native| compare(native, &rows));
        let same = case.native_sql.as_ref() == case.spark_sql.as_ref()
            && native_plan.as_ref() == Some(&text);
        let same_function = case.native_sql.as_ref() == case.spark_sql.as_ref()
            && native_functions
                .as_ref()
                .is_some_and(|native| native.same(&evidence));
        results.push(json!({"configuration":configuration,"status":"ok","rows":rows.0.len(),"types":rows.1,"source_guard_rows":ROWS,"comparison":comparison,"identical_plan_candidate":same,"same_physical_function":same_function,"function_evidence":evidence.record(),"native_function_evidence":native_functions.as_ref().map(FunctionEvidence::record)}));
    }
    let projection = case
        .projection_sql
        .as_ref()
        .ok_or("missing plain projection")?;
    let mut baselines = Vec::new();
    for configuration in ["native", "spark-off", "spark-on"] {
        let context = sessions.get(configuration);
        let physical = plan(context, projection, configuration).await?;
        let schema = types(physical.schema().as_ref());
        let text = displayable(physical.as_ref()).indent(true).to_string();
        fs::write(work.join(format!("projection-{configuration}.plan")), &text)?;
        let mut stream = execute_stream(physical, context.task_ctx())?;
        let mut rows = 0;
        while let Some(batch) = stream.try_next().await? {
            rows += batch.num_rows();
        }
        if rows != ROWS {
            return Err("plain projection did not consume all input rows".into());
        }
        baselines.push(json!({"configuration":configuration,"rows":rows,"types":schema}));
    }
    Ok(json!({"id":case.id,"input_rows":ROWS,"results":results,"baselines":baselines}))
}

async fn drain(
    context: &SessionContext,
    physical: Arc<dyn ExecutionPlan>,
) -> Result<(u128, usize)> {
    let start = Instant::now();
    let mut stream = execute_stream(physical, context.task_ctx())?;
    let mut rows = 0;
    let mut output = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        rows += batch.num_rows();
        output.push(batch);
    }
    let elapsed = start.elapsed().as_nanos();
    std::hint::black_box(output);
    Ok((elapsed, rows))
}

async fn measure(context: &SessionContext, case: &Case, configuration: &str) -> Result<Value> {
    let sql = if configuration == "native" {
        case.native_sql.as_ref().or(case.projection_sql.as_ref())
    } else {
        case.spark_sql.as_ref()
    }
    .ok_or("no timed SQL")?;
    // Planning and plan rendering stay outside both clocks; each execution gets a fresh plan.
    let mut samples = Vec::new();
    let mut executed_types = None;
    for iteration in 0..4 {
        let physical = plan(context, sql, configuration).await?;
        let schema = types(physical.schema().as_ref());
        if executed_types
            .as_ref()
            .is_some_and(|previous| previous != &schema)
        {
            return Err("physical types changed between retained executions".into());
        }
        executed_types = Some(schema);
        let baseline = plan(
            context,
            case.projection_sql.as_ref().ok_or("missing projection")?,
            configuration,
        )
        .await?;
        let ((elapsed, rows), (projection, projection_rows)) = if iteration % 2 == 0 {
            let query = drain(context, physical).await?;
            (query, drain(context, baseline).await?)
        } else {
            let plain = drain(context, baseline).await?;
            (drain(context, physical).await?, plain)
        };
        if projection_rows != ROWS {
            return Err("timed projection input rows changed".into());
        }
        if iteration > 0 {
            samples.push(json!({"iteration":iteration,"execution_ns":elapsed,"projection_ns":projection,"increment_ns":elapsed as i128 - projection as i128,"output_rows":rows,"input_rows":ROWS}));
        }
    }
    let status = fs::read_to_string("/proc/self/status")?;
    let peak_rss = status
        .lines()
        .find(|line| line.starts_with("VmHWM:"))
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or("missing Linux peak RSS")?
        .parse::<u64>()?
        * 1024;
    Ok(
        json!({"id":case.id,"configuration":configuration,"types":executed_types,"samples":samples,"process_peak_rss_bytes":peak_rss}),
    )
}

async fn run(args: &[String]) -> Result<Value> {
    self_check();
    if args == ["check"] {
        return Ok(json!({"status":"passed"}));
    }
    if args == ["fixture"] {
        let batches = input()?;
        let schema = batches[0].schema();
        let columns = schema.fields().iter().enumerate().map(|(n,field)| json!({"name":field.name(),"type":format!("{:?}",field.data_type()),"nulls":batches.iter().map(|batch|batch.column(n).null_count()).sum::<usize>()})).collect::<Vec<_>>();
        return Ok(
            json!({"rows":ROWS,"seed":"0x13915eed20261119","batch_rows":BATCH,"batches":batches.len(),"columns":columns,"null_rule":"row index divisible by 16, except nonnull id/grp/high_key"}),
        );
    }
    if args == ["registry"] {
        let state = SessionStateBuilder::new().with_default_features().build();
        let keys = |values: Vec<String>| values.into_iter().collect::<BTreeSet<_>>();
        return Ok(json!({
            "scalar":keys(sail_plan::function::BUILT_IN_SCALAR_FUNCTIONS.keys().map(|name| name.to_string()).collect()),
            "generator":keys(sail_plan::function::BUILT_IN_GENERATOR_FUNCTIONS.keys().map(|name| name.to_string()).collect()),
            "native_scalar":keys(state.scalar_functions().keys().cloned().collect()),
            "native_aggregate":keys(state.aggregate_functions().keys().cloned().collect()),
            "native_window":keys(state.window_functions().keys().cloned().collect())
        }));
    }
    if args.len() < 4 || !["preflight", "measure", "measure-set"].contains(&args[0].as_str()) {
        return Err("usage: functions check | registry | preflight MANIFEST ID WORK | measure MANIFEST ID CONFIG".into());
    }
    let manifest: Manifest = serde_json::from_slice(&fs::read(&args[1])?)?;
    if args[0] == "measure-set" {
        let ids: Vec<String> = serde_json::from_slice(&fs::read(&args[2])?)?;
        if !["native", "spark-off", "spark-on"].contains(&args[3].as_str()) {
            return Err("invalid configuration".into());
        }
        let contexts = sessions()?;
        for id in ids {
            let case = manifest
                .functions
                .iter()
                .find(|case| case.id == id)
                .ok_or("unknown set case")?;
            let measured = tokio::time::timeout(
                std::time::Duration::from_secs(120),
                measure(contexts.get(&args[3]), case, &args[3]),
            )
            .await;
            let result = match measured {
                Ok(Ok(mut result)) => {
                    result["status"] = json!("ok");
                    result
                }
                Ok(Err(error)) => {
                    json!({"id":id,"configuration":args[3],"status":"execution_error","error":error.to_string()})
                }
                Err(_) => {
                    json!({"id":id,"configuration":args[3],"status":"case_timeout","error":"fixed 120-second case bound"})
                }
            };
            println!("{}", serde_json::to_string(&result)?);
        }
        return Ok(json!({"status":"complete"}));
    }
    let case = manifest
        .functions
        .iter()
        .find(|case| case.id == args[2])
        .ok_or("unknown case")?;
    let contexts = sessions()?;
    if args[0] == "preflight" {
        preflight(&contexts, case, Path::new(&args[3])).await
    } else {
        if !["native", "spark-off", "spark-on"].contains(&args[3].as_str()) {
            return Err("invalid configuration".into());
        }
        measure(contexts.get(&args[3]), case, &args[3]).await
    }
}

fn main() -> Result {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    let result = runtime.block_on(run(&args))?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
