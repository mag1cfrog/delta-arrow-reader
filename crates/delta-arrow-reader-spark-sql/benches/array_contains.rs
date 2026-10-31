//! Measure array membership over registered columns and constant SQL arrays.
//! Run the same Cargo profile on each revision; timings do not assert a threshold.

use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, AsArray, Int32Array, Int32Builder, ListBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

const ROWS: usize = 16_384;
const SAMPLES: usize = 11;

fn median(samples: &[u128]) -> u128 {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    sorted[sorted.len() / 2]
}

async fn measure_lookup(
    length: usize,
    contains_null: bool,
    hit: bool,
    literal_array: bool,
) -> Result<Value, Box<dyn Error>> {
    let session = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(SessionConfig::new().with_target_partitions(2)),
        )
        .build(),
    );
    let needle = if hit { length - 1 } else { length } as i32;
    let mut arrays = ListBuilder::new(Int32Builder::new()).with_field(Field::new(
        "item",
        DataType::Int32,
        contains_null,
    ));
    for _ in 0..ROWS {
        for value in 0..length {
            arrays
                .values()
                .append_option((!contains_null || value != 0).then_some(value as i32));
        }
        arrays.append(true);
    }
    let arrays = arrays.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", arrays.data_type().clone(), false),
        Field::new("n", DataType::Int32, false),
    ]));
    session.register_batch(
        "inputs",
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrays),
                Arc::new(Int32Array::from(vec![needle; ROWS])),
            ],
        )?,
    )?;
    let array_expression = if literal_array {
        let values = (0..length)
            .map(|value| {
                if contains_null && value == 0 {
                    "NULL".to_owned()
                } else {
                    value.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(",");
        format!("ARRAY({values})")
    } else {
        "a".to_owned()
    };
    let sql = format!("SELECT ARRAY_CONTAINS({array_expression}, n) AS present FROM inputs");
    let mut planning_ns = Vec::new();
    let mut execution_ns = Vec::new();
    let mut result_counts = None;
    for sample in 0..SAMPLES + 2 {
        let start = Instant::now();
        let query = plan_query(&session, &sql, &SparkSqlOptions::default()).await?;
        let plan = query.create_physical_plan(&session).await?;
        let planning = start.elapsed().as_nanos();
        let start = Instant::now();
        let batches = collect(plan, session.task_ctx()).await?;
        let execution = start.elapsed().as_nanos();
        let (mut rows, mut matches, mut nulls) = (0, 0, 0);
        for batch in batches {
            let present = batch.column(0).as_boolean();
            rows += present.len();
            matches += present.true_count();
            nulls += present.null_count();
        }
        assert_eq!(rows, ROWS);
        assert_eq!(matches, if hit { ROWS } else { 0 });
        let counts = (matches, nulls);
        if let Some(previous) = result_counts {
            assert_eq!(counts, previous, "repeats must preserve lookup results");
        }
        result_counts = Some(counts);
        if sample >= 2 {
            planning_ns.push(planning);
            execution_ns.push(execution);
        }
    }
    Ok(json!({
        "length": length, "contains_null": contains_null, "hit": hit,
        "array_source": if literal_array { "literal" } else { "column" },
        "rows": ROWS, "matched_rows": result_counts.map(|counts| counts.0), "null_rows": result_counts.map(|counts| counts.1),
        "planning_median_ns": median(&planning_ns), "execution_median_ns": median(&execution_ns),
        "planning_samples_ns": planning_ns, "execution_samples_ns": execution_ns,
    }))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error>> {
    for length in [4, 32, 256] {
        for contains_null in [false, true] {
            for hit in [false, true] {
                for literal_array in [false, true] {
                    println!(
                        "{}",
                        measure_lookup(length, contains_null, hit, literal_array).await?
                    );
                }
            }
        }
    }
    Ok(())
}
