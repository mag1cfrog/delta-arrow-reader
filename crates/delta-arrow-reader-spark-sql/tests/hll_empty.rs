//! Empty HLL sketches keep their configuration and remain usable by downstream functions.

use std::collections::BTreeSet;
use std::error::Error;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::functions_aggregate::count::Count;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, ScalarUDF, Signature};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_function::aggregate::hll_sketch::{HllSketchAggFunction, HllUnionAggFunction};
use sail_function::scalar::misc::hll_sketch::HllSketchEstimateFunction;
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

fn encode_arrow_cell(array: &ArrayRef, row: usize) -> TestResult<Value> {
    if array.is_null(row) {
        Ok(Value::Null)
    } else if let Some(binary) = array.as_any().downcast_ref::<BinaryArray>() {
        let sketch = datasketches::hll::HllSketch::deserialize(binary.value(row))?;
        Ok(json!({"lg_config_k":sketch.lg_config_k(),"estimate":sketch.estimate().round() as i64}))
    } else {
        Ok(array_value_to_string(array.as_ref(), row)?.into())
    }
}

async fn execute_hll_query(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let sql = case["sql"].as_str().ok_or("missing SQL")?;
    let (logical, physical, names) = if case["frontend"] == "datafusion" {
        let frame = session.sql(sql).await?;
        let logical = frame.logical_plan().clone();
        let names = logical
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        (logical, frame.create_physical_plan().await?, names)
    } else {
        let query = plan_query(
            session,
            sql,
            &SparkSqlOptions {
                ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
                session_timezone: "UTC".into(),
                ..SparkSqlOptions::default()
            },
        )
        .await?;
        let physical = query.create_physical_plan(session).await?;
        (query.logical_plan, physical, query.column_names)
    };
    let fields = physical.schema().fields().iter().enumerate().map(|(i, field)| {
        let logical_field = logical.schema().field(i);
        assert_eq!(logical_field.data_type(), field.data_type());
        assert_eq!(logical_field.is_nullable(), field.is_nullable());
        assert_eq!(logical_field.metadata(), field.metadata());
        assert_eq!(names[i], *field.name());
        let data_type = match field.data_type() {
            DataType::Int32 => "integer",
            DataType::Int64 => "long",
            DataType::Binary => "binary",
            DataType::Boolean => "boolean",
            other => return Err(format!("unexpected result type {other:?}").into()),
        };
        Ok(json!({"name":field.name(),"type":data_type,"nullable":field.is_nullable(),"metadata":field.metadata()}))
    }).collect::<TestResult<Vec<_>>>()?;
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|array| encode_arrow_cell(array, row))
                    .collect::<TestResult<Vec<_>>>()?,
            );
        }
    }
    Ok(json!({"status":"ok","schema":{"type":"struct","fields":fields},"rows":rows}))
}

#[tokio::test]
async fn hll_empty_groups_and_windows_match_spark() -> TestResult {
    let records = include_str!("fixtures/hll-empty-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len(), 139);
    let identities = records[1..]
        .iter()
        .map(|case| {
            Ok((
                case["id"].as_str().ok_or("missing ID")?,
                case["ansi"].as_bool().ok_or("missing ANSI")?,
            ))
        })
        .collect::<TestResult<BTreeSet<_>>>()?;
    assert_eq!(identities.len(), 138);
    let mut failures = Vec::new();
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_batch_size(batch_size)
                            .with_target_partitions(2),
                    ),
            )
            .build(),
        );
        session.register_udaf(AggregateUDF::from(HllSketchAggFunction::new()));
        session.register_udaf(AggregateUDF::from(HllUnionAggFunction::new()));
        session.register_udf(ScalarUDF::from(HllSketchEstimateFunction::new()));
        let rows: Vec<(i32, Option<i32>)> =
            serde_json::from_value(records[0]["input_rows"].clone())?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("value", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(Int32Array::from_iter(rows.iter().map(|r| r.1))),
            ],
        )?;
        let batches = (0..batch.num_rows())
            .step_by(batch_size)
            .map(|offset| batch.slice(offset, batch_size.min(batch.num_rows() - offset)))
            .collect();
        session.register_table(
            "inputs",
            Arc::new(MemTable::try_new(schema, vec![batches])?),
        )?;
        session.register_batch(
            "invalid_sketches",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "sketch",
                    DataType::Binary,
                    false,
                )])),
                vec![Arc::new(BinaryArray::from(vec![b"".as_slice()]))],
            )?,
        )?;
        for case in &records[1..] {
            let actual = execute_hll_query(&session, case).await;
            let expected = &case["expected"];
            let matches = match &actual {
                Ok(actual) => actual == expected,
                Err(error) => {
                    let message = error.to_string();
                    match expected["condition"].as_str() {
                        Some("HLL_INVALID_LG_K") => {
                            message.contains("lgConfigK")
                                && (message.contains("between 4 and 21")
                                    || message.contains("non-null integer literal"))
                        }
                        Some("HLL_K_MUST_BE_CONSTANT") => {
                            message.contains("lgConfigK to be a constant integer")
                        }
                        Some("HLL_INVALID_INPUT_SKETCH_BUFFER") => {
                            message.contains("invalid HLL sketch")
                        }
                        Some("HLL_UNION_DIFFERENT_LG_K") => message
                            .contains("cannot union HLL sketches with different lgConfigK values"),
                        _ => false,
                    }
                }
            };
            if !matches {
                failures.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {expected}, got {actual:?}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CountWithEmptyWindowDefault {
    count: Count,
    nullable: bool,
    default: Option<i64>,
}

impl AggregateUDFImpl for CountWithEmptyWindowDefault {
    fn name(&self) -> &str {
        "count_with_empty_window_default"
    }

    fn signature(&self) -> &Signature {
        self.count.signature()
    }

    fn return_type(&self, args: &[DataType]) -> DataFusionResult<DataType> {
        self.count.return_type(args)
    }

    fn is_nullable(&self) -> bool {
        self.nullable
    }

    fn accumulator(&self, args: AccumulatorArgs) -> DataFusionResult<Box<dyn Accumulator>> {
        self.count.accumulator(args)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DataFusionResult<Vec<FieldRef>> {
        self.count.state_fields(args)
    }

    fn default_value(&self, _: &DataType) -> DataFusionResult<ScalarValue> {
        Ok(ScalarValue::Int64(self.default))
    }
}

#[tokio::test]
async fn native_empty_windows_preserve_explicit_and_nullable_defaults() -> TestResult {
    for (nullable, default, empty) in [
        (false, Some(42), Some(42)),
        (true, None, None),
        (false, None, Some(0)),
    ] {
        let session = SessionContext::new();
        session.register_udaf(AggregateUDF::from(CountWithEmptyWindowDefault {
            count: Count::new(),
            nullable,
            default,
        }));
        for (frame, expected) in [
            (
                "ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING",
                vec![empty, Some(1), Some(2)],
            ),
            (
                "ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING",
                vec![Some(2), Some(1), empty],
            ),
        ] {
            let batches = session.sql(&format!(
                "SELECT count_with_empty_window_default(id) OVER (ORDER BY id {frame}) AS n FROM (VALUES (1),(2),(3)) t(id) ORDER BY id"
            )).await?.collect().await?;
            let mut actual = Vec::new();
            for batch in batches {
                assert_eq!(batch.schema().field(0).is_nullable(), nullable);
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or("expected BIGINT")?;
                actual.extend(values.iter());
            }
            assert_eq!(
                actual, expected,
                "{frame}: nullable={nullable}, default={default:?}"
            );
        }
    }
    Ok(())
}
