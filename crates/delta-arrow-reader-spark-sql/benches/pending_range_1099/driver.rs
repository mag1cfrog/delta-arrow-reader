//! Exercise SQL-planned bounded windows with explicit source batch boundaries.

use std::{
    collections::{BTreeMap, VecDeque},
    error::Error,
    sync::{Arc, Mutex},
    task::Poll,
    time::Instant,
};

use arrow::{
    array::{ArrayRef, Int32Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::{Result as DfResult, exec_datafusion_err},
    datasource::{TableProvider, TableType},
    execution::{SessionStateBuilder, TaskContext},
    logical_expr::Expr,
    physical_expr::{
        LexOrdering, PhysicalSortExpr,
        expressions::{Column, col},
    },
    physical_optimizer::aggregate_statistics::PreserveAggregateStatisticsSchema,
    physical_plan::{
        ExecutionPlan, SendableRecordBatchStream, displayable,
        stream::RecordBatchStreamAdapter,
        streaming::{PartitionStream, StreamingTableExec},
        windows::BoundedWindowAggExec,
    },
    prelude::{SessionConfig, SessionContext},
};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use futures_util::{FutureExt, StreamExt, stream};
use sail_plan::expression_errors::SparkSqlExpressionSemantics;
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "../../tests/support/spark_results.rs"]
mod spark_results;
use spark_results::{capture_schema, encode_value, validate_batch};

type Result<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

// @observations@

#[derive(Debug, Default)]
struct Gate {
    queue: VecDeque<RecordBatch>,
    eof: bool,
    eof_polled: bool,
    delivered: Vec<usize>,
}

#[derive(Debug)]
struct Source {
    schema: SchemaRef,
    gate: Arc<Mutex<Gate>>,
}

impl PartitionStream for Source {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        let gate = Arc::clone(&self.gate);
        let batches = stream::poll_fn(move |_| {
            let mut gate = match gate.lock() {
                Ok(gate) => gate,
                Err(_) => {
                    return Poll::Ready(Some(Err(exec_datafusion_err!("probe gate poisoned"))));
                }
            };
            if let Some(batch) = gate.queue.pop_front() {
                gate.delivered.push(batch.num_rows());
                Poll::Ready(Some(Ok(batch)))
            } else if gate.eof {
                gate.eof_polled = true;
                Poll::Ready(None)
            } else {
                Poll::Pending
            }
        });
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            batches,
        ))
    }
}

#[derive(Debug)]
struct Provider(Arc<Source>);

#[async_trait]
impl TableProvider for Provider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.0.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        assert!(filters.is_empty() && limit.is_none());
        let ordering = LexOrdering::new(vec![PhysicalSortExpr {
            expr: col("k", &self.0.schema)?,
            options: arrow::compute::SortOptions {
                descending: false,
                nulls_first: true,
            },
        }])
        .ok_or_else(|| exec_datafusion_err!("missing source order"))?;
        Ok(Arc::new(StreamingTableExec::try_new(
            Arc::clone(&self.0.schema),
            vec![self.0.clone()],
            projection.cloned(),
            Some(ordering),
            false,
            None,
        )?))
    }
}

fn batch(schema: &SchemaRef, start: usize, rows: usize, key: i32) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int32Array::from_iter_values(
                start as i32..(start + rows) as i32,
            )) as ArrayRef,
            Arc::new(Int32Array::from_value(key, rows)),
        ],
    )?)
}

struct Planned {
    session: SessionContext,
    physical: Arc<dyn ExecutionPlan>,
    stages: Value,
    gate: Arc<Mutex<Gate>>,
    input_schema: SchemaRef,
    plan: String,
    order_expression: String,
    argument_expressions: Vec<String>,
}

fn inspect(plan: &Arc<dyn ExecutionPlan>, windows: &mut Vec<(String, Vec<String>)>) -> Result {
    assert!(
        !matches!(
            plan.name(),
            "SortExec"
                | "SortPreservingMergeExec"
                | "CoalesceBatchesExec"
                | "RepartitionExec"
                | "WindowAggExec"
        ),
        "blocking or batch-changing plan: {}",
        plan.name()
    );
    if let Some(window) = plan.downcast_ref::<BoundedWindowAggExec>() {
        assert_eq!(window.window_expr().len(), 1);
        let expression = &window.window_expr()[0];
        assert_eq!(expression.order_by().len(), 1);
        let order = &expression.order_by()[0];
        assert!(
            order.expr.as_any().is::<Column>(),
            "ORDER expression must remain a Column"
        );
        assert_eq!(
            order.options,
            arrow::compute::SortOptions {
                descending: false,
                nulls_first: true
            }
        );
        windows.push((
            format!("{:?}", order.expr),
            expression
                .expressions()
                .iter()
                .map(|arg| format!("{arg:?}"))
                .collect(),
        ));
    }
    for child in plan.children() {
        inspect(child, windows)?;
    }
    Ok(())
}

async fn plan(sql: &str, configuration: &str, batch_size: usize) -> Result<Planned> {
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(batch_size);
    let builder = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config);
    let session = SessionContext::new_with_state(if configuration == "native" {
        builder.build()
    } else {
        configure_spark_sql(builder).build()
    });
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("k", DataType::Int32, false),
    ]));
    let gate = Arc::new(Mutex::new(Gate::default()));
    session.register_table(
        "pending_peers",
        Arc::new(Provider(Arc::new(Source {
            schema: Arc::clone(&input_schema),
            gate: Arc::clone(&gate),
        }))),
    )?;
    let (physical, public, optimized) = if configuration == "native" {
        let query = session.sql(sql).await?;
        let public = capture_schema(query.schema().fields(), None)?;
        let optimized = session.state().optimize(query.logical_plan())?;
        let optimized = capture_schema(optimized.schema().fields(), None)?;
        (query.create_physical_plan().await?, public, optimized)
    } else {
        let query = plan_query(
            &session,
            sql,
            &SparkSqlOptions {
                ansi_mode: configuration == "spark-on",
                session_timezone: "UTC".into(),
                ..SparkSqlOptions::default()
            },
        )
        .await?;
        let public = capture_schema(
            query.logical_plan.schema().fields(),
            Some(&query.column_names),
        )?;
        let mut state = session.state();
        let extensions = &mut state.config_mut().options_mut().extensions;
        extensions.insert(SparkSqlExpressionSemantics::default());
        extensions.insert(PreserveAggregateStatisticsSchema::default());
        let optimized = state.optimize(&query.logical_plan)?;
        let optimized = capture_schema(optimized.schema().fields(), Some(&query.column_names))?;
        (
            query.create_physical_plan(&session).await?,
            public,
            optimized,
        )
    };
    let mut windows = Vec::new();
    inspect(&physical, &mut windows)?;
    assert_eq!(
        windows.len(),
        1,
        "SQL must reach exactly one real bounded executor"
    );
    let (order_expression, argument_expressions) = windows.remove(0);
    let before = capture_schema(physical.schema().fields(), None)?;
    let plan_text = displayable(physical.as_ref()).indent(true).to_string();
    assert!(plan_text.len() <= 16_384, "plan diagnostic exceeded bound");
    Ok(Planned {
        session,
        physical,
        stages: json!({"public":public,"analyzed":public,
            "optimized":optimized,"executed_before_collect":before}),
        gate,
        input_schema,
        plan: plan_text,
        order_expression,
        argument_expressions,
    })
}

struct Execution {
    batches: Vec<RecordBatch>,
    error: Option<String>,
    before_eof_rows: usize,
    before_lookahead_rows: usize,
    before_eof: Value,
    after_eof: Value,
    delivered: Vec<usize>,
    eof_polled: bool,
}

async fn drain(
    output: &mut SendableRecordBatchStream,
    batches: &mut Vec<RecordBatch>,
) -> Result<bool> {
    loop {
        match output.next().now_or_never() {
            None => return Ok(false),
            Some(None) => return Ok(true),
            Some(Some(batch)) => batches.push(batch?),
        }
    }
}

async fn execute(
    planned: &Planned,
    inputs: &[RecordBatch],
    progressive: bool,
    regions: bool,
) -> Result<Execution> {
    *planned.gate.lock().map_err(|_| "probe gate poisoned")? = Gate::default();
    reset_events(regions);
    let mut output = planned.physical.execute(0, planned.session.task_ctx())?;
    let mut batches = Vec::new();
    let mut error = None;
    for input in inputs {
        planned
            .gate
            .lock()
            .map_err(|_| "probe gate poisoned")?
            .queue
            .push_back(input.clone());
        match drain(&mut output, &mut batches).await {
            Ok(ended) => assert!(!ended, "input EOF was not released"),
            Err(cause) => {
                error = Some(cause.to_string());
                break;
            }
        }
    }
    let before_lookahead_rows = batches.iter().map(RecordBatch::num_rows).sum();
    if progressive && error.is_none() {
        assert_eq!(before_lookahead_rows, 0);
        planned
            .gate
            .lock()
            .map_err(|_| "probe gate poisoned")?
            .queue
            .push_back(batch(&planned.input_schema, 4, 1, 2)?);
        assert!(!drain(&mut output, &mut batches).await?);
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            4,
            "completed peers must be emitted before physical EOF"
        );
    }
    let before_eof_rows = batches.iter().map(RecordBatch::num_rows).sum();
    let before_eof = snapshot_events();
    assert!(
        !planned
            .gate
            .lock()
            .map_err(|_| "probe gate poisoned")?
            .eof_polled
    );
    if error.is_none() {
        planned.gate.lock().map_err(|_| "probe gate poisoned")?.eof = true;
        match drain(&mut output, &mut batches).await {
            Ok(ended) => assert!(ended, "finite source remained pending after EOF"),
            Err(cause) => error = Some(cause.to_string()),
        }
    }
    let after_eof = snapshot_events();
    let gate = planned.gate.lock().map_err(|_| "probe gate poisoned")?;
    let expected = inputs
        .iter()
        .map(RecordBatch::num_rows)
        .chain(progressive.then_some(1))
        .collect::<Vec<_>>();
    assert_eq!(
        gate.delivered,
        expected[..gate.delivered.len()],
        "source boundaries changed"
    );
    if error.is_none() {
        assert_eq!(gate.delivered, expected);
        assert!(gate.eof_polled);
    }
    Ok(Execution {
        batches,
        error,
        before_eof_rows,
        before_lookahead_rows,
        before_eof,
        after_eof,
        delivered: gate.delivered.clone(),
        eof_polled: gate.eof_polled,
    })
}

fn observation(planned: &Planned, execution: &Execution) -> Result<Value> {
    assert!(
        execution
            .error
            .as_ref()
            .is_none_or(|error| error.len() <= 16_384),
        "error diagnostic exceeded its declared bound"
    );
    let mut stages = planned.stages.clone();
    stages["executed"] = capture_schema(planned.physical.schema().fields(), None)?;
    let rows = if execution.error.is_none() {
        let mut rows = Vec::new();
        for batch in &execution.batches {
            validate_batch(planned.physical.schema().as_ref(), batch)?;
            for row in 0..batch.num_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| encode_value(column.as_ref(), row))
                        .collect::<Result<Vec<_>>>()?,
                );
            }
        }
        rows.sort_by_key(|row| row[0].as_str().and_then(|id| id.parse::<i32>().ok()));
        Some(rows)
    } else {
        None
    };
    Ok(
        json!({"schema_stages":stages,"rows":rows,"error":execution.error,
        "phase":if execution.error.is_some() { "collect" } else { "encode" }}),
    )
}

fn boundary_summary(sizes: &[usize]) -> BTreeMap<usize, usize> {
    let mut summary = BTreeMap::new();
    for size in sizes {
        *summary.entry(*size).or_default() += 1;
    }
    summary
}

/// Run the frozen probe; scheduling and eligibility are checked by run.py as well.
pub async fn run() -> Result {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    assert!(matches!(
        args.first().map(String::as_str),
        Some("trace" | "regions" | "measure")
    ));
    let operation = &args[0];
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(args.get(1).ok_or("missing manifest")?)?)?;
    let eligibility = if operation == "trace" {
        Value::Null
    } else {
        let proof: Value = serde_json::from_slice(&std::fs::read(
            args.get(2).ok_or("missing exact eligibility proof")?,
        )?)?;
        assert_eq!(proof["issue"], 1099);
        assert_eq!(proof["all_spark_matches"], true);
        proof
    };
    let mut ordinal = args
        .get(3)
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(0);
    for shape in manifest["shapes"].as_array().ok_or("missing shapes")? {
        let shape_id = shape["id"].as_str().ok_or("missing shape ID")?;
        if operation != "trace" && shape_id != "constant" {
            continue;
        }
        let mut sizes = manifest["row_counts"]
            .as_array()
            .ok_or("missing sizes")?
            .iter()
            .map(|n| n.as_u64().map(|n| n as usize).ok_or("bad size"))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if operation == "trace" && shape_id == "constant" {
            sizes.push(4);
        }
        for rows in sizes {
            let progressive = rows == 4;
            for size in manifest["batch_sizes"]
                .as_array()
                .ok_or("missing batches")?
            {
                let batch_size = size.as_u64().ok_or("bad batch size")? as usize;
                let configurations = ["native", "spark-off", "spark-on"];
                for offset in 0..3 {
                    let configuration = configurations[(ordinal + offset) % 3];
                    let id = format!(
                        "{}:{rows}:{batch_size}:{configuration}",
                        if progressive { "progressive" } else { shape_id }
                    );
                    let planned = plan(
                        shape["sql"].as_str().ok_or("missing SQL")?,
                        configuration,
                        batch_size,
                    )
                    .await?;
                    let key = shape["key"].as_i64().ok_or("missing key")? as i32;
                    let inputs = (0..rows)
                        .step_by(batch_size)
                        .map(|start| {
                            batch(
                                &planned.input_schema,
                                start,
                                batch_size.min(rows - start),
                                key,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    if operation == "trace" {
                        let execution = execute(&planned, &inputs, progressive, false).await?;
                        if !progressive && (shape_id == "constant" || configuration == "spark-off")
                        {
                            assert_eq!(
                                execution.before_eof_rows, 0,
                                "unfinished peer group emitted output"
                            );
                        }
                        if shape_id == "wrapped" && configuration == "spark-on" {
                            assert!(execution.error.as_ref().is_some_and(|error| {
                                error.contains("[ARITHMETIC_OVERFLOW] RANGE comparison target")
                            }));
                            assert_eq!(execution.delivered.len(), 1);
                            assert_eq!(execution.before_eof_rows, 0);
                        }
                        if !execution.before_eof.is_null() && !progressive {
                            let counters = &execution.before_eof["counters"];
                            assert_eq!(counters["input_batches"], execution.delivered.len());
                            assert_eq!(
                                counters["input_rows"],
                                execution.delivered.iter().sum::<usize>()
                            );
                            assert_eq!(counters["input_batch_min"], batch_size);
                            assert_eq!(counters["input_batch_max"], batch_size);
                        }
                        println!(
                            "{}",
                            json!({"id":id,"shape":if progressive { "progressive" } else { shape_id },
                            "rows":rows,"batch":batch_size,"configuration":configuration,"sql":shape["sql"],
                            "observation":observation(&planned,&execution)?,"plan":planned.plan,
                            "order_expression":planned.order_expression,"order_is_column":true,
                            "argument_expressions":planned.argument_expressions,
                            "source_batch_histogram":boundary_summary(&execution.delivered),
                            "source_eof_polled":execution.eof_polled,
                            "before_lookahead_rows":execution.before_lookahead_rows,"before_eof_rows":execution.before_eof_rows,
                            "before_eof":execution.before_eof,"after_eof":execution.after_eof})
                        );
                    } else {
                        let expected = eligibility["cases"]
                            .get(&id)
                            .ok_or("context has no exact native eligibility")?;
                        let warmups = if operation == "measure" { 2 } else { 1 };
                        let samples = if operation == "measure" { 7 } else { 1 };
                        let mut walls = Vec::new();
                        let mut regions = Vec::new();
                        for repeat in 0..warmups + samples {
                            // Planning and input construction precede the execution timer; encoding follows it.
                            let started = (operation == "measure").then(Instant::now);
                            let execution = execute(
                                &planned,
                                &inputs,
                                false,
                                operation == "regions" && repeat >= warmups,
                            )
                            .await?;
                            let elapsed = started.map(|start| start.elapsed().as_nanos() as u64);
                            assert_eq!(
                                &observation(&planned, &execution)?,
                                expected,
                                "timed result or Field changed"
                            );
                            if repeat >= warmups {
                                if let Some(elapsed) = elapsed {
                                    walls.push(elapsed);
                                }
                                if operation == "regions" {
                                    regions.push(execution.after_eof);
                                }
                            }
                        }
                        println!(
                            "{}",
                            json!({"id":id,"configuration":configuration,"rows":rows,"batch":batch_size,
                            "operation":operation,"samples_ns":walls,"regions":regions,
                            "timing_scope":"real executor plus gated source coordination and output collection; excludes planning, input construction and encoding"})
                        );
                    }
                }
                ordinal += 1;
            }
        }
    }
    Ok(())
}
