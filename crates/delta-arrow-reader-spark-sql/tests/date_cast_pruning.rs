//! DATE-cast values and stored timestamp ranges over real Delta files (#1197).

use std::{collections::BTreeSet, fs, sync::Arc};

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Int32Type, TimeUnit, TimestampMicrosecondType};
use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{Expr, col, lit};
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader::{
    DeltaTableBuilder, WarmupMode,
    datafusion::{DeltaTableProvider, ScanOptions, collect_scan_metrics},
};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use parquet::arrow::ArrowWriter;
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "../../delta-arrow-reader-core/tests/reader/support.rs"]
mod reader_support;
#[allow(dead_code)]
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use reader_support::RealParquetDeltaTable;
use spark_results::{TestResult, capture_schema, validate_batch};

fn references() -> TestResult<(Value, Vec<Value>)> {
    let mut cases = Vec::new();
    let mut metadata = Value::Null;
    for (source, reference, count) in [
        (
            include_str!("fixtures/date-cast-pruning/requests.json"),
            include_str!("fixtures/date-cast-pruning/spark.jsonl"),
            16,
        ),
        (
            include_str!("fixtures/date-cast-pruning/addendum-requests.json"),
            include_str!("fixtures/date-cast-pruning/addendum-spark.jsonl"),
            8,
        ),
    ] {
        let request: Value = serde_json::from_str(source)?;
        let records = reference
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<Vec<Value>, _>>()?;
        assert_eq!(records[0]["spark_version"], "4.2.0");
        assert_eq!(
            records[0]["spark_build"]["SPARK_REVISION"]["value"],
            "32f7299601108917fb01920a54e084595b7b3bf8"
        );
        let corpora = request["corpora"].as_array().ok_or("missing corpora")?;
        let definitions = corpora
            .iter()
            .flat_map(|corpus| corpus["cases"].as_array().into_iter().flatten())
            .collect::<Vec<_>>();
        assert_eq!(definitions.len(), count);
        assert_eq!(records.len() - 1, definitions.len());
        for (case, definition) in records[1..].iter().zip(definitions) {
            for key in [
                "corpus",
                "id",
                "sql",
                "ansi",
                "timezone",
                "kind",
                "column",
                "day",
                "predicates",
                "controls",
            ] {
                assert_eq!(case[key], definition[key], "source identity {key}");
            }
            assert_eq!(case["expected"]["status"], "ok");
        }
        for corpus in corpora {
            if metadata.is_null() {
                metadata = corpus["metadata"].clone();
            }
            assert_eq!(corpus["metadata"], metadata);
        }
        cases.extend(records.into_iter().skip(1));
    }
    assert_eq!(cases.len(), 24);
    Ok((metadata, cases))
}

fn context(batch_size: usize, pruning: bool) -> SessionContext {
    let mut builder = configure_spark_sql(
        SessionStateBuilder::new()
            .with_default_features()
            .with_config(
                SessionConfig::new()
                    .with_target_partitions(1)
                    .with_batch_size(batch_size),
            ),
    );
    if !pruning && let Some(optimizer) = builder.optimizer() {
        optimizer
            .rules
            .retain(|rule| rule.name() != "push_down_filter");
    }
    SessionContext::new_with_state(builder.build())
}

async fn physical(
    session: &SessionContext,
    case: &Value,
    sql: &str,
) -> TestResult<Arc<dyn ExecutionPlan>> {
    let query = plan_query(
        session,
        sql,
        &SparkSqlOptions {
            session_timezone: case["timezone"].as_str().ok_or("missing timezone")?.into(),
            ansi_mode: case["ansi"].as_bool().ok_or("missing ansi")?,
            ..Default::default()
        },
    )
    .await?;
    Ok(query.create_physical_plan(session).await?)
}

async fn rows(session: &SessionContext, plan: Arc<dyn ExecutionPlan>) -> TestResult<Vec<Value>> {
    let schema = plan.schema();
    let mut rows = Vec::new();
    for batch in collect(plan, session.task_ctx()).await? {
        validate_batch(schema.as_ref(), &batch)?;
        for row in 0..batch.num_rows() {
            rows.push(Value::from(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        if column.is_null(row) {
                            Ok(Value::Null)
                        } else {
                            Ok(json!(match column.data_type() {
                                DataType::Timestamp(TimeUnit::Microsecond, _) => column
                                    .as_primitive::<TimestampMicrosecondType>()
                                    .value(row)
                                    .to_string(),
                                _ => arrow::util::display::array_value_to_string(
                                    column.as_ref(),
                                    row
                                )?,
                            }))
                        }
                    })
                    .collect::<TestResult<Vec<_>>>()?,
            ));
        }
    }
    rows.sort_by_key(Value::to_string);
    Ok(rows)
}

#[tokio::test]
async fn date_cast_comparisons_match_spark() -> TestResult {
    let (metadata, cases) = references()?;
    for batch_size in [1, 32] {
        let session = context(batch_size, true);
        spark_inputs::register_recorded_inputs_with_partition_count(
            &session, &metadata, batch_size, 1,
        )?;
        for case in &cases {
            let plan = physical(&session, case, case["sql"].as_str().ok_or("missing SQL")?).await?;
            let actual_schema = capture_schema(plan.schema().fields(), None)?;
            for field in plan.schema().fields() {
                if let DataType::Timestamp(_, Some(zone)) = field.data_type() {
                    assert_eq!(
                        zone.as_ref(),
                        case["timezone"].as_str().ok_or("missing timezone")?
                    );
                }
            }
            let types = |schema: &Value| -> TestResult<Vec<Value>> {
                Ok(schema["fields"]
                    .as_array()
                    .ok_or("missing fields")?
                    .iter()
                    .map(|field| field["type"].clone())
                    .collect())
            };
            assert_eq!(
                types(&actual_schema)?,
                types(&case["schema_stages"]["executed"])?,
                "{case}"
            );
            let actual_rows = rows(&session, plan).await?;
            println!(
                "date_cast_value_observation={}",
                json!({"corpus":case["corpus"],
                "id":case["id"],"timezone":case["timezone"],"ansi":case["ansi"],
                "batch_size":batch_size,"rows":actual_rows,"executed":actual_schema})
            );
            assert_eq!(json!(actual_rows), case["expected"]["rows"], "{case}");
        }
    }
    Ok(())
}

fn parquet(batch: &RecordBatch) -> TestResult<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None)?;
    writer.write(batch)?;
    writer.close()?;
    Ok(bytes)
}

fn fixture(metadata: &Value) -> TestResult<RealParquetDeltaTable> {
    let input = &metadata["inputs"][0];
    let batch = spark_inputs::decode_recorded_batch(input)?;
    let protocol = json!({"protocol":{"minReaderVersion":3,"minWriterVersion":7,
        "readerFeatures":["timestampNtz"],"writerFeatures":["timestampNtz"]}});
    let delta_metadata = json!({"metaData":{"id":"date-cast-pruning",
        "format":{"provider":"parquet","options":{}},
        "schemaString":input["schema"].to_string(),"partitionColumns":[],"configuration":{}}});
    let fixture = RealParquetDeltaTable::new_with_raw_parquet(
        "spark-date-cast-pruning",
        &parquet(&batch)?,
        batch.num_rows(),
        &protocol,
        &delta_metadata,
    )
    .map_err(|error| error.to_string())?;
    let mut actions = vec![protocol, delta_metadata];
    let mut offset = 0;
    for (index, group) in metadata["file_ids"]
        .as_array()
        .ok_or("missing files")?
        .iter()
        .enumerate()
    {
        let group = group.as_array().ok_or("missing file ids")?;
        let part = batch.slice(offset, group.len());
        let ids = part.column(0).as_primitive::<Int32Type>();
        assert_eq!(
            json!(
                ids.values()
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            ),
            json!(group)
        );
        let bytes = parquet(&part)?;
        let name = format!("part-{index}.parquet");
        fs::write(fixture.path().join(&name), &bytes)?;
        let mut minimum = json!({});
        let mut maximum = json!({});
        let mut nulls = json!({});
        for column in 0..part.num_columns() {
            let field = part.schema().field(column).name().clone();
            let values = (0..part.num_rows())
                .filter(|row| !part.column(column).is_null(*row))
                .map(|row| {
                    arrow::util::display::array_value_to_string(part.column(column).as_ref(), row)
                })
                .collect::<Result<Vec<_>, _>>()?;
            nulls[&field] = json!(part.column(column).null_count());
            if let (Some(min), Some(max)) = (values.iter().min(), values.iter().max()) {
                if column == 0 {
                    minimum[&field] = json!(ids.values().iter().min());
                    maximum[&field] = json!(ids.values().iter().max());
                } else {
                    minimum[&field] = json!(min);
                    maximum[&field] = json!(max);
                }
            }
        }
        let stats = json!({"numRecords":part.num_rows(),"minValues":minimum,
            "maxValues":maximum,"nullCount":nulls});
        actions.push(
            json!({"add":{"path":name,"partitionValues":{},"size":bytes.len(),
            "modificationTime":0,"dataChange":true,"stats":stats.to_string()}}),
        );
        offset += group.len();
    }
    assert_eq!(offset, batch.num_rows());
    fs::remove_file(fixture.path().join(fixture.data_file_path()))?;
    fs::write(
        fixture.path().join("_delta_log/00000000000000000000.json"),
        actions
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n",
    )?;
    Ok(fixture)
}

fn native_predicate(case: &Value, name: &str, reference_row: &Value) -> TestResult<Expr> {
    let column = case["column"].as_str().ok_or("missing column")?;
    let values = reference_row.as_array().ok_or("missing row")?;
    let zone = (column == "ts").then(|| Arc::<str>::from("UTC"));
    let bound = |index: usize| -> TestResult<Expr> {
        Ok(lit(ScalarValue::TimestampMicrosecond(
            Some(
                values[index]
                    .as_str()
                    .ok_or("missing Spark bound")?
                    .parse()?,
            ),
            zone.clone(),
        )))
    };
    if case["kind"] == "fixed_ntz" {
        let named_zone = case["timezone"] == "America/Los_Angeles";
        let (expression, literal) = if named_zone {
            let conversion = sail_function::scalar::datetime::spark_timestamp_to_ntz::SparkTimestampToNtz::for_session_zone(
                "America/Los_Angeles".into(), false).ok_or("missing conversion")?;
            let literal = lit(ScalarValue::TimestampMicrosecond(
                Some(
                    values[values.len() - 2]
                        .as_str()
                        .ok_or("missing NTZ literal")?
                        .parse()?,
                ),
                None,
            ));
            (
                datafusion::logical_expr::ScalarUDF::from(conversion).call(vec![col(column)]),
                literal,
            )
        } else {
            (col(column), bound(values.len() - 1)?)
        };
        return Ok(match name {
            "eq" | "reverse_eq" | "try_cast" | "to_ntz" => expression.eq(literal),
            "ne" => expression.not_eq(literal),
            "lt" => expression.lt(literal),
            "le" => expression.lt_eq(literal),
            "gt" => expression.gt(literal),
            "ge" => expression.gt_eq(literal),
            _ => return Err(format!("unexpected predicate {name}").into()),
        });
    }
    let start = bound(values.len() - 2)?;
    let next = bound(values.len() - 1)?;
    Ok(match name {
        "eq" | "reverse_eq" | "between" | "to_date" => {
            col(column).gt_eq(start).and(col(column).lt(next))
        }
        "ne" | "reverse_ne" => col(column).lt(start).or(col(column).gt_eq(next)),
        "lt" | "reverse_gt" => col(column).lt(start),
        "le" | "reverse_ge" => col(column).lt(next),
        "gt" | "reverse_lt" => col(column).gt_eq(next),
        "ge" | "reverse_le" => col(column).gt_eq(start),
        _ => return Err(format!("unexpected predicate {name}").into()),
    })
}

#[tokio::test]
async fn date_cast_filters_prune_real_delta_files() -> TestResult {
    let (metadata, cases) = references()?;
    let fixture = fixture(&metadata)?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned())
        .with_warmup(WarmupMode::QueryPlanning)
        .load_table()
        .await?;
    let provider = Arc::new(DeltaTableProvider::try_new(table, ScanOptions::default())?);
    let session = context(32, true);
    session.register_table("inputs", provider.clone())?;
    let control = context(32, false);
    control.register_table("inputs", provider)?;
    let files = metadata["file_ids"].as_array().ok_or("missing files")?;
    let mut differences = Vec::new();
    for case in &cases {
        let expected = case["expected"]["rows"]
            .as_array()
            .ok_or("missing Spark rows")?;
        for (index, predicate) in case["predicates"]
            .as_array()
            .ok_or("missing predicates")?
            .iter()
            .enumerate()
        {
            let sql = format!(
                "SELECT id FROM inputs WHERE {}",
                predicate["sql"].as_str().ok_or("missing predicate")?
            );
            let selected: BTreeSet<_> = expected
                .iter()
                .filter(|row| row[index + 1] == "true")
                .map(|row| row[0].as_str().ok_or("missing id"))
                .collect::<Result<_, _>>()?;
            let expected_rows = selected.iter().map(|id| json!([id])).collect::<Vec<_>>();
            let name = predicate["name"].as_str().ok_or("missing name")?;
            let selected_files = files
                .iter()
                .filter(|group| {
                    group.as_array().is_some_and(|ids| {
                        ids.iter()
                            .any(|id| id.as_str().is_some_and(|id| selected.contains(id)))
                    })
                })
                .count();
            let plan = physical(&session, case, &sql).await?;
            let metrics = collect_scan_metrics(plan.as_ref());
            assert_eq!(metrics.len(), 1);
            assert_eq!(metrics[0].snapshot().reader_metrics.file_tasks_started, 0);
            assert_eq!(rows(&session, plan).await?, expected_rows, "{case}: {name}");
            let started = metrics[0].snapshot().reader_metrics.file_tasks_started;
            let native = session
                .table("inputs")
                .await?
                .filter(native_predicate(case, name, &expected[0])?)?
                .select_columns(&["id"])?
                .create_physical_plan()
                .await?;
            let native_metrics = collect_scan_metrics(native.as_ref());
            assert_eq!(native_metrics.len(), 1);
            assert_eq!(
                rows(&session, native).await?,
                expected_rows,
                "native {case}: {name}"
            );
            let native_started = native_metrics[0]
                .snapshot()
                .reader_metrics
                .file_tasks_started;
            if matches!(name, "ne" | "reverse_ne")
                || (case["kind"] == "fixed_ntz" && case["timezone"] == "America/Los_Angeles")
            {
                // Delta retains OR ranges, timestamp <> and named-zone NTZ conversions.
                assert_eq!(native_started, files.len() as u64, "native {case}: {name}");
            } else {
                assert!(
                    native_started >= selected_files as u64 && native_started < files.len() as u64,
                    "native range must prune files conservatively: {case}: {name}"
                );
            }
            println!(
                "date_cast_file_observation={}",
                json!({"column":case["column"],
                "day":case["day"],"timezone":case["timezone"],"ansi":case["ansi"],
                "predicate":name,"selected_files":selected_files,"file_tasks_started":started,
                "native_file_tasks_started":native_started})
            );
            if started != native_started {
                differences.push(json!({"case":case["corpus"],"day":case["day"],
                    "ansi":case["ansi"],"predicate":name,"expected":native_started,"actual":started}));
            }
            if name == "eq" {
                let unpruned = physical(&control, case, &sql).await?;
                let metrics = collect_scan_metrics(unpruned.as_ref());
                assert_eq!(metrics.len(), 1);
                assert_eq!(rows(&control, unpruned).await?, expected_rows);
                assert_eq!(
                    metrics[0].snapshot().reader_metrics.file_tasks_started,
                    files.len() as u64
                );
            }
        }
    }
    assert!(
        differences.is_empty(),
        "file pruning differences: {differences:#?}"
    );
    Ok(())
}
