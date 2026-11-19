//! Fixed in-memory NULLIF #620 protocol. See reports/nullif-620-protocol.md.

use std::error::Error;
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, Int32Array, new_null_array,
};
use arrow::compute::{filter, filter_record_batch};
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type, Int32Type, Schema};
use arrow::ipc::{reader::FileReader, writer::FileWriter};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, Volatility, create_udf};
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr_common::utils::scatter;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, collect, displayable, projection::ProjectionExec,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

type BenchResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
const MANIFEST: &str = include_str!("nullif_620/manifest.json");

fn input_value(fraction: &str, row: usize, left: bool) -> Option<i32> {
    let valid = if left {
        match fraction {
            "none" => true,
            "sparse" => !row.is_multiple_of(11),
            "half" => !row.is_multiple_of(2),
            "all" => false,
            _ => unreachable!("fixed manifest fraction"),
        }
    } else {
        fraction == "none" || !row.is_multiple_of(13)
    };
    valid.then_some(if left || row.is_multiple_of(3) {
        (row % 17) as i32
    } else {
        0
    })
}

fn expected(case: &Value, fraction: &str, row: usize) -> Option<f64> {
    let a = input_value(fraction, row, true).map(f64::from);
    let b = input_value(fraction, row, false).map(f64::from);
    if let Some(op) = case["operator"].as_str() {
        return a.zip(b).map(|(a, b)| match op {
            "+" => a + b,
            "-" => a - b,
            "*" => a * b,
            _ => unreachable!("fixed arithmetic operator"),
        });
    }
    let operand = |kind: &str, column: Option<f64>| match kind {
        "column" => column,
        "plus_one" => column.map(|v| v + 1.0),
        "zero" => Some(0.0),
        "one" => Some(1.0),
        _ => unreachable!("fixed operand"),
    };
    let left = operand(case["left"].as_str().unwrap_or_default(), a);
    let right = operand(case["right"].as_str().unwrap_or_default(), b);
    left.filter(|v| Some(*v) != right)
}

fn input(
    family: &str,
    fraction: &str,
    rows: usize,
    batch_rows: usize,
) -> BenchResult<Vec<RecordBatch>> {
    let data_type = if family == "nullif" {
        DataType::Float64
    } else {
        DataType::Int32
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", data_type.clone(), fraction != "none"),
        Field::new("b", data_type, fraction != "none"),
    ]));
    let columns = [true, false].map(|left| {
        let values = (0..rows).map(|row| input_value(fraction, row, left));
        if family == "nullif" {
            Arc::new(Float64Array::from_iter(values.map(|v| v.map(f64::from)))) as ArrayRef
        } else {
            Arc::new(Int32Array::from_iter(values)) as ArrayRef
        }
    });
    let batch = RecordBatch::try_new(schema, columns.to_vec())?;
    Ok(if rows == 0 {
        vec![batch]
    } else {
        (0..rows)
            .step_by(batch_rows)
            .map(|offset| batch.slice(offset, batch_rows.min(rows - offset)))
            .collect()
    })
}

fn session(batches: &[RecordBatch], batch_rows: usize) -> BenchResult<SessionContext> {
    // Native and Spark planning share this configured session and dependency binary.
    let session = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(batch_rows)
                        .with_target_partitions(2),
                ),
        )
        .build(),
    );
    session.register_table(
        "inputs",
        Arc::new(MemTable::try_new(
            batches[0].schema(),
            vec![batches.to_vec()],
        )?),
    )?;
    Ok(session)
}

async fn plan(
    session: &SessionContext,
    configuration: &str,
    sql: &str,
) -> BenchResult<(Arc<dyn ExecutionPlan>, FieldRef)> {
    if configuration == "native" {
        let frame = session.sql(sql).await?;
        let logical = frame.logical_plan().schema().field(0).clone();
        Ok((frame.create_physical_plan().await?, logical))
    } else {
        let query = plan_query(
            session,
            sql,
            &SparkSqlOptions {
                ansi_mode: configuration == "spark-on",
                ..SparkSqlOptions::default()
            },
        )
        .await?;
        let logical = query.logical_plan.schema().field(0).clone();
        Ok((query.create_physical_plan(session).await?, logical))
    }
}

fn values(batches: &[RecordBatch]) -> BenchResult<Vec<Option<u64>>> {
    let mut values = Vec::new();
    for batch in batches {
        if batch.num_columns() != 1 {
            return Err("expected one result column".into());
        }
        match batch.column(0).data_type() {
            DataType::Float64 => values.extend(
                batch
                    .column(0)
                    .as_primitive::<Float64Type>()
                    .iter()
                    .map(|v| v.map(f64::to_bits)),
            ),
            DataType::Int32 => values.extend(
                batch
                    .column(0)
                    .as_primitive::<Int32Type>()
                    .iter()
                    .map(|v| v.map(|v| i64::from(v) as u64)),
            ),
            _ => return Err("unexpected executed result type".into()),
        }
    }
    Ok(values)
}

fn validate(
    batches: &[RecordBatch],
    case: &Value,
    family: &str,
    fraction: &str,
    rows: usize,
) -> BenchResult<bool> {
    let data_type = if family == "nullif" {
        DataType::Float64
    } else {
        DataType::Int32
    };
    if batches
        .iter()
        .any(|b| b.num_columns() != 1 || b.column(0).data_type() != &data_type)
        || batches.iter().map(RecordBatch::num_rows).sum::<usize>() != rows
    {
        return Ok(false);
    }
    let actual = values(batches)?;
    Ok(actual.len() == rows
        && actual.into_iter().enumerate().all(|(row, value)| {
            value
                == expected(case, fraction, row).map(|v| {
                    if family == "nullif" {
                        v.to_bits()
                    } else {
                        (v as i64) as u64
                    }
                })
        }))
}

fn partitions(plan: &dyn ExecutionPlan) -> Value {
    json!({"operator":plan.name(),"output_partitions":plan.output_partitioning().partition_count(),
        "inputs":plan.children().iter().map(|child| partitions(child.as_ref())).collect::<Vec<_>>()})
}

fn write_ipc(path: &Path, plan: &dyn ExecutionPlan, batches: &[RecordBatch]) -> BenchResult {
    let mut writer = FileWriter::try_new(File::create(path)?, plan.schema().as_ref())?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(())
}

fn read_ipc(path: &str) -> BenchResult<(Arc<Schema>, Vec<RecordBatch>)> {
    let reader = FileReader::try_new(File::open(path)?, None)?;
    Ok((reader.schema(), reader.collect::<Result<Vec<_>, _>>()?))
}

fn projection(plan: &dyn ExecutionPlan) -> BenchResult<&ProjectionExec> {
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        return Ok(projection);
    }
    for child in plan.children() {
        if let Ok(found) = projection(child.as_ref()) {
            return Ok(found);
        }
    }
    Err("expected a NULLIF projection for stage replay".into())
}

// Baseline replay: scalar_function.rs:233-360 at e48dab25; scatter: utils.rs:77.
// Stage timers describe this replay; their sums are not a wall-time decomposition.
fn replay(
    function: &ScalarFunctionExpr,
    batch: &RecordBatch,
    ns: &mut [u128; 7],
) -> BenchResult<(ArrayRef, usize)> {
    let started = Instant::now();
    let first = function.args()[0].evaluate(batch)?;
    ns[0] += started.elapsed().as_nanos();
    let started = Instant::now();
    let mask = match &first {
        ColumnarValue::Array(array) => array
            .logical_nulls()
            .filter(|nulls| nulls.null_count() > 0)
            .map(|nulls| BooleanArray::new(nulls.inner().clone(), None)),
        _ => None,
    };
    ns[1] += started.elapsed().as_nanos();
    if mask.as_ref().is_some_and(|mask| mask.true_count() == 0) {
        return Ok((new_null_array(function.return_type(), batch.num_rows()), 0));
    }
    let selected;
    let selected_batch = if let Some(mask) = &mask {
        let started = Instant::now();
        selected = filter_record_batch(batch, mask)?;
        ns[2] += started.elapsed().as_nanos();
        &selected
    } else {
        batch
    };
    let started = Instant::now();
    let selected_first = match (&mask, first) {
        (Some(mask), ColumnarValue::Array(array)) => ColumnarValue::Array(
            if let Some(column) = function.args()[0].downcast_ref::<Column>() {
                selected_batch.column(column.index()).clone()
            } else {
                filter(array.as_ref(), mask)?
            },
        ),
        (_, first) => first,
    };
    ns[3] += started.elapsed().as_nanos();
    let mut args = vec![selected_first];
    let started = Instant::now();
    for expr in &function.args()[1..] {
        args.push(expr.evaluate(selected_batch)?);
    }
    ns[4] += started.elapsed().as_nanos();
    let started = Instant::now();
    let result = function
        .fun()
        .invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields: function
                .args()
                .iter()
                .map(|expr| expr.return_field(selected_batch.schema_ref()))
                .collect::<Result<Vec<_>, _>>()?,
            number_rows: selected_batch.num_rows(),
            return_field: Arc::new(Field::new(
                "r",
                function.return_type().clone(),
                function.nullable(),
            )),
            config_options: Arc::new(function.config_options().clone()),
        })?
        .into_array(selected_batch.num_rows())?;
    ns[5] += started.elapsed().as_nanos();
    let started = Instant::now();
    let result = if let Some(mask) = mask {
        scatter(&mask, result.as_ref())?
    } else {
        result
    };
    ns[6] += started.elapsed().as_nanos();
    Ok((result, selected_batch.num_rows()))
}

async fn effects(manifest: &Value) -> BenchResult {
    for configuration in ["spark-off", "spark-on"] {
        for fraction in ["none", "sparse", "half", "all"] {
            for rows in [0, 17] {
                let batches = input("nullif", fraction, rows, 8)?;
                let session = session(&batches, 8)?;
                let counts = [Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0))];
                for (name, count) in ["left_effect", "right_effect"].into_iter().zip(&counts) {
                    let count = count.clone();
                    session.register_udf(create_udf(
                        name,
                        vec![DataType::Float64],
                        DataType::Float64,
                        Volatility::Volatile,
                        Arc::new(move |args| {
                            let arrays = ColumnarValue::values_to_arrays(args)?;
                            count.fetch_add(arrays[0].len(), Ordering::SeqCst);
                            Ok(ColumnarValue::Array(arrays[0].clone()))
                        }),
                    ));
                }
                let (plan, _) = plan(
                    &session,
                    configuration,
                    "SELECT NULLIF(left_effect(a),right_effect(b)) AS r FROM inputs",
                )
                .await?;
                assert_eq!(counts.each_ref().map(|c| c.load(Ordering::SeqCst)), [0, 0]);
                let result = collect(plan, session.task_ctx()).await?;
                assert!(validate(
                    &result,
                    &manifest["families"]["nullif"][2],
                    "nullif",
                    fraction,
                    rows
                )?);
                let required = (0..rows)
                    .filter(|row| input_value(fraction, *row, true).is_some())
                    .count();
                let actual = counts.each_ref().map(|c| c.load(Ordering::SeqCst));
                assert_eq!(actual, [rows, required]);
                println!(
                    "{}",
                    json!({"kind":"effects","configuration":configuration,"fraction":fraction,"rows":rows,"planning_evaluated_rows":[0,0],"execution_evaluated_rows":actual})
                );
            }
        }
    }
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> BenchResult {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let manifest: Value = serde_json::from_str(MANIFEST)?;
    if args.first().map(String::as_str) == Some("compare") {
        let (left_schema, left) = read_ipc(&args[1])?;
        let (right_schema, right) = read_ipc(&args[2])?;
        let equal = left_schema.field(0).data_type() == right_schema.field(0).data_type()
            && values(&left)? == values(&right)?;
        println!(
            "{}",
            json!({"equal":equal,"left_schema":format!("{left_schema:?}"),"right_schema":format!("{right_schema:?}")})
        );
        if !equal {
            std::process::exit(2);
        }
        return Ok(());
    }
    let command = args
        .first()
        .ok_or("expected preflight, measure, attribute or compare")?
        .as_str();
    let family = args.get(1).ok_or("expected nullif or checked")?.as_str();
    let cases = manifest["families"][family]
        .as_array()
        .ok_or("unknown family")?;
    let output = if command == "preflight" {
        let path = Path::new(args.get(2).ok_or("expected output directory")?);
        fs::create_dir_all(path)?;
        Some(path)
    } else {
        None
    };
    let arm = args.get(2).map(String::as_str).unwrap_or("main");
    let replica = args
        .get(3)
        .map(|v| v.parse::<usize>())
        .transpose()?
        .unwrap_or(1);
    let excluded: Vec<String> = if command == "measure" {
        serde_json::from_str::<Value>(&fs::read_to_string(
            args.get(4).ok_or("expected eligibility receipt")?,
        )?)?["excluded"]
            .as_array()
            .ok_or("missing exclusions")?
            .iter()
            .map(|v| v.as_str().unwrap_or_default().to_owned())
            .collect()
    } else {
        vec![]
    };
    if !["preflight", "measure", "attribute"].contains(&command) {
        return Err("unknown command".into());
    }
    let mut configurations = vec!["native", "spark-off", "spark-on"];
    if command == "attribute" {
        configurations = vec![arm];
    } else if arm == "native-b" {
        configurations = vec!["native"];
    } else {
        configurations.rotate_left((replica - 1) % 3);
    }
    let rows = manifest["rows"].as_u64().ok_or("missing rows")? as usize;
    for batch_rows in manifest["batch_rows"].as_array().ok_or("missing batches")? {
        let batch_rows = batch_rows.as_u64().ok_or("invalid batch rows")? as usize;
        for fraction in manifest["null_fractions"]
            .as_array()
            .ok_or("missing fractions")?
        {
            let fraction = fraction.as_str().ok_or("invalid fraction")?;
            let batches = input(family, fraction, rows, batch_rows)?;
            let session = session(&batches, batch_rows)?;
            for case in cases {
                let id = format!(
                    "{family}-{}-{fraction}-{batch_rows}",
                    case["name"].as_str().ok_or("missing case name")?
                );
                if excluded.contains(&id) {
                    continue;
                }
                let sql = format!(
                    "SELECT {} AS r FROM inputs",
                    case["sql"].as_str().ok_or("missing SQL")?
                );
                for configuration in &configurations {
                    if command == "attribute" {
                        let (plan, _) = plan(&session, configuration, &sql).await?;
                        let projection = projection(plan.as_ref())?;
                        let expression = &projection.expr()[0].expr;
                        let function = expression
                            .downcast_ref::<ScalarFunctionExpr>()
                            .ok_or("expected scalar function")?;
                        let columns = projection
                            .input()
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| batches[0].schema().index_of(f.name()))
                            .collect::<Result<Vec<_>, _>>()?;
                        for sample in 0..4 {
                            let mut ns = [0; 7];
                            let mut selected_rows = 0;
                            for batch in &batches {
                                let batch = batch.project(&columns)?;
                                let (replayed, selected) = replay(function, &batch, &mut ns)?;
                                let actual =
                                    expression.evaluate(&batch)?.into_array(batch.num_rows())?;
                                assert_eq!(replayed.data_type(), actual.data_type());
                                let schema = Arc::new(Schema::new(vec![Field::new(
                                    "r",
                                    actual.data_type().clone(),
                                    true,
                                )]));
                                assert_eq!(
                                    values(&[RecordBatch::try_new(
                                        schema.clone(),
                                        vec![replayed]
                                    )?])?,
                                    values(&[RecordBatch::try_new(schema, vec![actual])?])?
                                );
                                selected_rows += selected;
                            }
                            if sample > 0 {
                                println!(
                                    "{}",
                                    json!({"kind":"attribution","id":id,"configuration":configuration,"sample":sample-1,"rows":rows,"input_batches":batches.len(),"selected_rows":selected_rows,"stage_ns":ns})
                                );
                            }
                        }
                        continue;
                    }
                    for sample in 0..if output.is_some() { 1 } else { 4 } {
                        let started = (command == "measure").then(Instant::now);
                        let (plan, logical_field) = plan(&session, configuration, &sql).await?;
                        let planning_ns = started.map(|t| t.elapsed().as_nanos());
                        let started = (command == "measure").then(Instant::now);
                        let result = collect(plan.clone(), session.task_ctx()).await?;
                        let execution_ns = started.map(|t| t.elapsed().as_nanos());
                        let formula_match = validate(&result, case, family, fraction, rows)?;
                        if let Some(output) = output {
                            let path = output.join(format!("{id}-{configuration}.arrow"));
                            write_ipc(&path, plan.as_ref(), &result)?;
                            fs::write(
                                output.join(format!("{id}-{configuration}.plan.txt")),
                                displayable(plan.as_ref()).indent(true).to_string(),
                            )?;
                            let mut executed_schemas = result
                                .iter()
                                .map(|b| format!("{:?}", b.schema()))
                                .collect::<Vec<_>>();
                            executed_schemas.sort();
                            executed_schemas.dedup();
                            println!(
                                "{}",
                                json!({"kind":"preflight","id":id,"configuration":configuration,"rows":result.iter().map(RecordBatch::num_rows).sum::<usize>(),"expected_rows":rows,"input_batches":batches.len(),"output_batches":result.len(),"plan_partitions":partitions(plan.as_ref()),"formula_match":formula_match,"logical_field":format!("{logical_field:?}"),"physical_schema":format!("{:?}",plan.schema()),"executed_schemas":executed_schemas,"executed_types":result.iter().map(|b| format!("{:?}",b.column(0).data_type())).collect::<Vec<_>>(),"ipc_path":path})
                            );
                        } else {
                            assert!(formula_match, "output changed: {id} {configuration}");
                            if sample > 0 {
                                println!(
                                    "{}",
                                    json!({"kind":"sample","id":id,"configuration":configuration,"arm":arm,"replica":replica,"sample":sample-1,"rows":rows,"output_batches":result.len(),"planning_calls":1,"execution_calls":1,"planning_ns":planning_ns,"execution_ns":execution_ns})
                                );
                            }
                        }
                    }
                }
            }
        }
    }
    if command == "preflight" && family == "nullif" {
        effects(&manifest).await?;
    }
    Ok(())
}
