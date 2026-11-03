//! Single-row correlated projections use their outer row without a scalar executor.

use std::collections::BTreeSet;
use std::error::Error;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arrow::array::Array;
use arrow::datatypes::DataType;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, Expr, LogicalPlan, Volatility, create_udf};
use datafusion::optimizer::AnalyzerRule;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_plan::expression_errors::{PreserveSparkExpressionErrors, SparkSqlErrorSemantics};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

fn references() -> TestResult<Vec<Value>> {
    let records = include_str!("fixtures/single-row-correlated-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records.len(), 203);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    Ok(records)
}

fn scalar_metadata_stage_records() -> TestResult<Vec<Value>> {
    let mut records = include_str!(
        "fixtures/correlated-scalar-metadata-stages/supplement/supplement-spark.jsonl"
    )
    .lines()
    .map(serde_json::from_str)
    .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records.len() - 1, 68);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    let ordering =
        include_str!("fixtures/correlated-scalar-metadata-stages/ordering/ordering-spark.jsonl")
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(ordering.len() - 1, 8);
    let boundaries = include_str!(
        "fixtures/correlated-scalar-metadata-stages/boundaries/boundaries-spark.jsonl"
    )
    .lines()
    .map(serde_json::from_str)
    .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(boundaries.len() - 1, 12);
    for extra in [ordering, boundaries] {
        let inputs = records[0]["inputs"]
            .as_array_mut()
            .ok_or("missing inputs")?;
        for input in extra[0]["inputs"]
            .as_array()
            .ok_or("missing extra inputs")?
        {
            if let Some(existing) = inputs
                .iter()
                .find(|existing| existing["name"] == input["name"])
            {
                assert_eq!(existing, input);
            } else {
                inputs.push(input.clone());
            }
        }
        records.extend(extra.into_iter().skip(1));
    }
    assert_eq!(records.len() - 1, 88);
    Ok(records)
}

fn main_scalar_metadata_stage_records() -> TestResult<Vec<Value>> {
    let records = include_str!(
        "fixtures/correlated-scalar-metadata-stages/main-replay/main-scalar-stages-spark.jsonl"
    )
    .lines()
    .map(serde_json::from_str)
    .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records.len() - 1, 152);
    assert_eq!(records[0]["original_observations"], 202);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    Ok(records)
}

fn scalar_metadata_stage_case<'a>(records: &'a [Value], original: &Value) -> TestResult<&'a Value> {
    let case = records[1..]
        .iter()
        .find(|case| case.get("original_observation") == Some(original))
        .ok_or("missing original scalar observation")?;
    assert_eq!(case["sql"], original["sql"]);
    if original["expected"]["status"] == "ok" {
        assert_eq!(case["public_schema"], original["expected"]["schema"]);
        assert_eq!(case["expected"]["rows"], original["expected"]["rows"]);
    } else {
        assert_eq!(case["error"]["condition"], original["expected"]["condition"]);
    }
    Ok(case)
}

#[tokio::test]
async fn scalar_metadata_stages_and_arrow_extension_contract_match() -> TestResult {
    let records = scalar_metadata_stage_records()?;
    let extension_contract: Value = serde_json::from_str(include_str!(
        "fixtures/correlated-scalar-metadata-stages/dar-extension-expectations.json"
    ))?;
    let exceptions = extension_contract["cases"]
        .as_array()
        .ok_or("missing extension expectations")?;
    assert_eq!(exceptions.len(), 8);
    for exception in exceptions {
        let case = records[1..]
            .iter()
            .find(|case| {
                case["group"] == exception["group"]
                    && case["id"] == exception["id"]
                    && case["ansi"] == exception["ansi"]
            })
            .ok_or("missing extension reference")?;
        for (dar, spark) in [
            ("dar_public_schema", "public_schema"),
            ("dar_executed_schema", "executed_schema_after_collect"),
        ] {
            let mut schema = exception[dar].clone();
            assert_eq!(schema["fields"][1]["name"], "scalar_value");
            for (key, value) in [
                ("ARROW:extension:name", "arrow.json"),
                ("ARROW:extension:metadata", ""),
            ] {
                assert_eq!(schema["fields"][1]["metadata"][key], value);
                if case[spark]["fields"][1]["metadata"].get(key).is_none() {
                    schema["fields"][1]["metadata"]
                        .as_object_mut()
                        .ok_or("missing extension metadata")?
                        .remove(key);
                }
            }
            assert_eq!(schema, case[spark]);
        }
    }
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["group"].as_str().ok_or("missing group")?,
            case["id"].as_str().ok_or("missing ID")?,
            case.get("input").and_then(Value::as_str),
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
        assert!(case.get("error").is_none());
        assert_eq!(case["public_schema"], case["analyzed_schema"]);
        assert_eq!(case["public_schema"], case["expected"]["schema"]);
        assert_eq!(
            case["optimized_schema"],
            case["executed_schema_before_collect"]
        );
        assert_eq!(
            case["executed_schema_before_collect"],
            case["executed_schema_after_collect"]
        );
    }
    let mut differences = Vec::new();
    let mut comparisons = 0;
    let mut extension_comparisons = 0;
    for batch_size in [1, 32] {
        let session = session(batch_size);
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        let calls = register_counter(&session);
        for case in &records[1..] {
            let exception = exceptions.iter().find(|entry| {
                entry["group"] == case["group"]
                    && entry["id"] == case["id"]
                    && entry["ansi"] == case["ansi"]
            });
            let mut query_case = case.clone();
            let public = if let Some(exception) = exception {
                assert_eq!(exception["sql"], case["sql"]);
                query_case["executed_schema_after_collect"] =
                    exception["dar_executed_schema"].clone();
                extension_comparisons += 1;
                &exception["dar_public_schema"]
            } else {
                &case["public_schema"]
            };
            calls.store(0, Ordering::SeqCst);
            let observed =
                spark_results::capture_spark_query_observation(&session, &query_case).await;
            let evaluated_rows = calls.load(Ordering::SeqCst) as u64;
            let matches = observed.as_ref().is_ok_and(|actual| {
                actual["result"]["schema"] == *public
                    && actual["result"]["rows"] == case["expected"]["rows"]
                    && actual["physical_schema"] == query_case["executed_schema_after_collect"]
            }) && Some(evaluated_rows) == case["evaluated_rows"].as_u64();
            eprintln!(
                "scalar_metadata_stages: {}",
                json!({"group":case["group"],"id":case["id"],"input":case["input"],
                    "ansi":case["ansi"],"batch_size":batch_size,"matches_dar_contract":matches,
                    "intentional_reference_difference":exception.map(|_| &extension_contract["policy"]),
                    "expected_spark":case,"expected_dar_public":public,
                    "expected_dar_executed":query_case["executed_schema_after_collect"],
                    "actual":observed.as_ref().ok(),"error":observed.as_ref().err().map(|e| e.to_string()),
                    "evaluated_rows":evaluated_rows})
            );
            if !matches {
                differences.push(format!(
                    "{} {} input={} ANSI={} batch={batch_size}: expected public {public}, executed {}, rows {} and evaluations {}, got {observed:?} and {evaluated_rows}",
                    case["group"], case["id"], case["input"], case["ansi"],
                    query_case["executed_schema_after_collect"], case["expected"]["rows"],
                    case["evaluated_rows"]
                ));
            }
            comparisons += 1;
        }
    }
    assert_eq!(comparisons, 176);
    assert_eq!(extension_comparisons, 16);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn scalar_executed_reference_cannot_hide_a_metadata_mismatch() -> TestResult {
    let records = scalar_metadata_stage_records()?;
    let mut case = records[1..]
        .iter()
        .find(|case| case["id"] == "nonnull_inputs_bare")
        .ok_or("missing nonnullable control")?
        .clone();
    case["executed_schema_after_collect"]["fields"][1]["metadata"] =
        json!({"unrecorded":"metadata"});
    let session = session(32);
    spark_inputs::register_recorded_inputs(&session, &records[0], 32)?;
    let result = spark_results::capture_spark_query_observation(&session, &case).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("executed schema"));
    Ok(())
}

#[tokio::test]
async fn scalar_precollection_schema_keeps_required_engine_errors() -> TestResult {
    let records = main_scalar_metadata_stage_records()?;
    let case = records[1..]
        .iter()
        .find(|case| case["id"] == "inputs_fallible_live_bare" && case["ansi"] == true)
        .ok_or("missing division-error control")?;
    assert!(case.get("executed_schema_after_collect").is_none());
    assert!(case.get("executed_schema_before_collect").is_some());
    let session = session(32);
    spark_inputs::register_recorded_inputs(&session, &records[0], 32)?;
    let error = spark_results::capture_spark_query_observation(&session, case)
        .await
        .unwrap_err();
    assert!(error.downcast_ref::<datafusion::error::DataFusionError>().is_some());
    assert!(error.to_string().contains("Divide by zero error"));
    let mut wrong = case.clone();
    wrong["executed_schema_before_collect"]["fields"][1]["metadata"] =
        json!({"unrecorded":"metadata"});
    let error = spark_results::capture_spark_query_observation(&session, &wrong)
        .await
        .unwrap_err();
    assert!(error.downcast_ref::<datafusion::error::DataFusionError>().is_none());
    assert!(error.to_string().contains("executed schema"));
    Ok(())
}

fn session(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
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
    )
}

fn register_counter(session: &SessionContext) -> Arc<AtomicUsize> {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    session.register_udf(create_udf(
        "count_row",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Volatile,
        Arc::new(move |arguments| {
            let arrays = ColumnarValue::values_to_arrays(arguments)?;
            assert!(!arrays[0].is_empty(), "evaluated an empty batch");
            counter.fetch_add(arrays[0].len(), Ordering::SeqCst);
            Ok(ColumnarValue::Array(arrays[0].clone()))
        }),
    ));
    calls
}

fn contains_scalar(plan: &LogicalPlan) -> TestResult<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            found |= expr.contains_scalar_subquery();
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(found)
}

#[tokio::test]
async fn single_row_projections_match_spark() -> TestResult {
    let records = references()?;
    let stages = main_scalar_metadata_stage_records()?;
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session(batch_size);
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        let calls = register_counter(&session);
        let mut checked = 0;
        for case in &records[1..] {
            let id = case["id"].as_str().ok_or("missing ID")?;
            // Correlated inner relations, projected IN and volatile results
            // retain separate execution contracts; their raw references stay intact.
            if id.contains("_correlated_")
                || id.contains("_projected_in")
                || id.contains("_volatile_direct_")
                || id.contains("_volatile_nested_")
                || id == "volatile_rand_raw"
            {
                continue;
            }
            let stage_case = scalar_metadata_stage_case(&stages, case)?;
            calls.store(0, Ordering::SeqCst);
            let actual = spark_results::capture_spark_query_result(&session, stage_case).await;
            let expected = &case["expected"];
            let matches = match &actual {
                Ok(actual) if expected["status"] == "ok" => {
                    actual["rows"] == expected["rows"] && actual["schema"] == expected["schema"]
                }
                Err(error) if expected["status"] == "error" => {
                    let error = error.to_string();
                    match expected["condition"].as_str() {
                        Some("DIVIDE_BY_ZERO") => error.contains("Divide by zero error"),
                        Some("CAST_INVALID_INPUT") => {
                            error.contains("Cannot cast string 'bad' to value of Int32 type")
                        }
                        Some("UNRESOLVED_COLUMN.WITHOUT_SUGGESTION") => {
                            error.contains("cannot resolve attribute")
                                && error.contains("p")
                                && error.contains("id")
                        }
                        Some("SCALAR_SUBQUERY_TOO_MANY_ROWS") => {
                            error.contains("Scalar subquery returned more than one row")
                        }
                        _ => false,
                    }
                }
                _ => false,
            };
            assert_eq!(
                calls.load(Ordering::SeqCst),
                0,
                "{id}: unused volatile projection executed"
            );
            if !matches {
                differences.push(format!(
                    "{id} ANSI={} batch={batch_size}: expected {expected}, got {actual:?}",
                    case["ansi"]
                ));
            }
            checked += 1;
        }
        assert_eq!(checked, 152);
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn single_row_metadata_and_volatile_siblings_match_spark() -> TestResult {
    let records = include_str!("fixtures/single-row-correlated-metadata-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records.len(), 25);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    let stages = scalar_metadata_stage_records()?;
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session(batch_size);
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        let calls = register_counter(&session);
        let mut checked = 0;
        for case in &records[1..] {
            let stage_case = scalar_metadata_stage_case(&stages, case)?;
            let id = case["id"].as_str().ok_or("missing ID")?;
            calls.store(0, Ordering::SeqCst);
            let planned = async {
                let query = plan_query(
                    &session,
                    case["sql"].as_str().ok_or("missing SQL")?,
                    &SparkSqlOptions {
                        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                        session_timezone: "UTC".into(),
                        ..SparkSqlOptions::default()
                    },
                )
                .await?;
                query.create_physical_plan(&session).await?;
                TestResult::Ok(())
            }
            .await;
            assert_eq!(
                calls.load(Ordering::SeqCst),
                0,
                "{id} ANSI={} batch={batch_size}: planning executed a volatile sibling",
                case["ansi"]
            );
            let actual = match planned {
                Ok(()) => spark_results::capture_spark_query_result(&session, stage_case).await,
                Err(error) => Err(error),
            };
            let expected = &case["expected"];
            assert_eq!(expected["status"], "ok");
            let expected_calls = case["evaluated_rows"].as_u64().ok_or("missing row count")?;
            let observed_calls = calls.load(Ordering::SeqCst) as u64;
            let matches = actual.as_ref().is_ok_and(|actual| {
                actual["rows"] == expected["rows"] && actual["schema"] == expected["schema"]
            });
            if !matches || observed_calls != expected_calls {
                differences.push(format!(
                    "{id} ANSI={} batch={batch_size}: expected {expected} and {expected_calls} evaluated rows, got {actual:?} and {observed_calls} evaluated rows",
                    case["ansi"]
                ));
            }
            checked += 1;
        }
        assert_eq!(checked, 24);
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn single_row_case_results_preserve_public_and_executed_metadata() -> TestResult {
    let records = include_str!("fixtures/single-row-correlated-case-metadata-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records.len(), 13);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    let stages = scalar_metadata_stage_records()?;
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session(batch_size);
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        let mut checked = 0;
        for case in &records[1..] {
            let stage_case = scalar_metadata_stage_case(&stages, case)?;
            let query = plan_query(
                &session,
                case["sql"].as_str().ok_or("missing SQL")?,
                &SparkSqlOptions {
                    ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                    session_timezone: "UTC".into(),
                    ..SparkSqlOptions::default()
                },
            )
            .await?;
            let mut scalar_fields = 0;
            query.logical_plan.apply_with_subqueries(|node| {
                node.apply_expressions(|expr| {
                    expr.apply(|expr| {
                        if let Expr::ScalarSubquery(subquery) = expr {
                            // The inner SELECT owns its marker; the public scalar drops it.
                            assert_eq!(
                                subquery.subquery.schema().field(0).metadata(),
                                &std::collections::HashMap::from([(
                                    "__autoGeneratedAlias".to_owned(),
                                    "true".to_owned()
                                )]),
                            );
                            scalar_fields += 1;
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })
                })?;
                Ok(TreeNodeRecursion::Continue)
            })?;
            assert_eq!(scalar_fields, 1, "{}", case["id"]);
            let actual = spark_results::capture_spark_query_result(&session, stage_case).await;
            let expected = &case["expected"];
            assert_eq!(expected["status"], "ok");
            let matches = actual.as_ref().is_ok_and(|actual| {
                actual["rows"] == expected["rows"] && actual["schema"] == expected["schema"]
            });
            if !matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {expected}, got {actual:?}",
                    case["id"], case["ansi"]
                ));
            }
            checked += 1;
        }
        assert_eq!(checked, 12);
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn single_row_scalars_preserve_resolved_schema_and_null_safety() -> TestResult {
    let records = include_str!("fixtures/single-row-correlated-nonnullable-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records.len(), 9);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    let stages = scalar_metadata_stage_records()?;
    for batch_size in [1, 32] {
        let session = session(batch_size);
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let stage_case = scalar_metadata_stage_case(&stages, case)?;
            let query = plan_query(
                &session,
                case["sql"].as_str().ok_or("missing SQL")?,
                &SparkSqlOptions {
                    ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                    session_timezone: "UTC".into(),
                    ..SparkSqlOptions::default()
                },
            )
            .await?;
            let mut state = session.state();
            state
                .config_mut()
                .options_mut()
                .extensions
                .insert(SparkSqlErrorSemantics::default());
            let analyzed = state.analyzer().execute_and_check(
                query.logical_plan.clone(),
                state.config_options(),
                |_, _| {},
            )?;
            let resolved = query.logical_plan.schema();
            let fields = resolved
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    assert_eq!(field.data_type(), &DataType::Int32);
                    json!({"name":query.column_names[index], "type":"integer",
                        "nullable":field.is_nullable(), "metadata":field.metadata()})
                })
                .collect::<Vec<_>>();
            assert_eq!(
                json!({"type":"struct", "fields":fields}),
                case["expected"]["schema"],
                "{} batch={batch_size}",
                case["id"]
            );
            assert_eq!(analyzed.schema().fields().len(), resolved.fields().len());
            for (index, field) in analyzed.schema().fields().iter().enumerate() {
                let declared = resolved.field(index);
                assert_eq!(field.name(), declared.name());
                assert_eq!(field.data_type(), declared.data_type());
                assert_eq!(field.metadata(), declared.metadata());
                // Internal CAST preparation can already narrow the scalar field.
                assert!(!field.is_nullable() || declared.is_nullable());
            }
            let physical = query.create_physical_plan(&session).await?;
            let physical_schema = physical.schema();
            assert_eq!(
                spark_results::capture_schema(physical_schema.fields(), None)?,
                stage_case["executed_schema_after_collect"],
                "{} ANSI={} batch={batch_size}",
                case["id"],
                case["ansi"]
            );
            eprintln!(
                "SINGLE_ROW_NONNULLABLE_SCHEMAS {}",
                json!({
                    "id":case["id"], "ansi":case["ansi"], "batch_size":batch_size,
                    "resolved_schema":format!("{resolved:?}"),
                    "analyzed_schema":format!("{:?}", analyzed.schema()),
                    "physical_schema":format!("{physical_schema:?}")
                })
            );
            assert_eq!(
                physical_schema.fields().len(),
                analyzed.schema().fields().len()
            );
            for (index, field) in physical_schema.fields().iter().enumerate() {
                let declared = analyzed.schema().field(index);
                assert_eq!(field.name(), &query.column_names[index]);
                assert_eq!(field.data_type(), declared.data_type());
                assert_eq!(field.metadata(), declared.metadata());
                // Physical nullability must still fit both prior declarations.
                assert!(!field.is_nullable() || declared.is_nullable());
            }
            let mut rows = Vec::new();
            for batch in collect(physical, session.task_ctx()).await? {
                spark_results::validate_batch(physical_schema.as_ref(), &batch)?;
                for row in 0..batch.num_rows() {
                    rows.push(
                        batch
                            .columns()
                            .iter()
                            .enumerate()
                            .map(|(index, column)| {
                                if column.is_null(row) {
                                    assert!(resolved.field(index).is_nullable());
                                    assert!(analyzed.schema().field(index).is_nullable());
                                    assert!(physical_schema.field(index).is_nullable());
                                    Ok(Value::Null)
                                } else {
                                    arrow::util::display::array_value_to_string(
                                        column.as_ref(),
                                        row,
                                    )
                                    .map(Value::String)
                                }
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    );
                }
            }
            rows.sort_by_key(|row| json!(row).to_string());
            assert_eq!(
                json!(rows),
                case["expected"]["rows"],
                "{} batch={batch_size}",
                case["id"]
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn scalar_results_preserve_arrow_extensions_and_native_metadata() -> TestResult {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;

    let extensions = [
        ("ARROW:extension:name".into(), "arrow.json".into()),
        ("ARROW:extension:metadata".into(), "".into()),
    ]
    .into_iter()
    .collect::<std::collections::HashMap<String, String>>();
    let mut inherited = extensions.clone();
    inherited.insert("origin".into(), "outer".into());
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true).with_metadata(inherited.clone()),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![0, 1, 2])),
            Arc::new(StringArray::from(vec![
                Some("{\"n\":1}"),
                None,
                Some("null"),
            ])),
        ],
    )?;
    let configured = session(32);
    let native = SessionContext::new();
    for context in [&configured, &native] {
        context.register_batch("extension_values", batch.clone())?;
    }
    for ansi_mode in [true, false] {
        for suffix in ["", ",CAST(7 AS INT) AS incidental"] {
            let sql = format!(
                "SELECT p.value AS direct_value,(SELECT p.value) AS scalar_value{suffix} FROM extension_values p ORDER BY p.id"
            );
            let query = plan_query(
                &configured,
                &sql,
                &SparkSqlOptions {
                    ansi_mode,
                    ..SparkSqlOptions::default()
                },
            )
            .await?;
            let physical = query.create_physical_plan(&configured).await?;
            for (index, expected) in [&inherited, &extensions].into_iter().enumerate() {
                let logical = query.logical_plan.schema().field(index);
                let field = physical.schema().field(index).clone();
                assert_eq!(logical.data_type(), &DataType::Utf8);
                assert!(logical.is_nullable());
                assert_eq!(logical.metadata(), expected);
                assert_eq!(field.name(), &query.column_names[index]);
                assert_eq!(field.data_type(), logical.data_type());
                assert_eq!(field.is_nullable(), logical.is_nullable());
                assert_eq!(field.metadata(), logical.metadata());
            }
            let mut values = Vec::new();
            for batch in collect(physical, configured.task_ctx()).await? {
                assert_eq!(batch.column(0), batch.column(1));
                values.extend(
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or("expected JSON strings")?
                        .iter()
                        .map(|value| value.map(str::to_owned)),
                );
            }
            assert_eq!(
                values,
                vec![Some("{\"n\":1}".into()), None, Some("null".into())]
            );
        }
    }
    let sql = "SELECT value,(SELECT value FROM extension_values WHERE id=0) AS scalar_value FROM extension_values ORDER BY id";
    let expected = native.sql(sql).await?.collect().await?;
    let actual = configured.sql(sql).await?.collect().await?;
    assert_eq!(actual, expected);
    for batch in actual {
        assert_eq!(batch.schema().field(0).metadata(), &inherited);
    }
    Ok(())
}

#[tokio::test]
async fn native_sql_and_projected_predicates_keep_their_contracts() -> TestResult {
    let records = references()?;
    let configured = session(32);
    let native = SessionContext::new();
    for context in [&configured, &native] {
        spark_inputs::register_recorded_inputs(context, &records[0], 32)?;
    }
    let spark = plan_query(
        &configured,
        "SELECT p.id,(SELECT p.id) AS r FROM inputs p",
        &SparkSqlOptions::default(),
    )
    .await?;
    collect(
        spark.create_physical_plan(&configured).await?,
        configured.task_ctx(),
    )
    .await?;
    for sql in [
        "SELECT p.id,(SELECT MAX(i.id) FROM inputs i) AS r FROM inputs p ORDER BY p.id NULLS FIRST",
        "SELECT p.id,p.id IN (SELECT i.id FROM inputs i) AS included,EXISTS(SELECT i.id FROM inputs i WHERE i.id=p.id) AS present FROM inputs p ORDER BY p.id NULLS FIRST",
        "SELECT p.id,p.id IN (SELECT i.id FROM empty_inputs i) AS included,EXISTS(SELECT i.id FROM empty_inputs i WHERE i.id=p.id) AS present FROM inputs p ORDER BY p.id NULLS FIRST",
    ] {
        let expected = native.sql(sql).await?.collect().await?;
        let actual = configured.sql(sql).await?.collect().await?;
        assert_eq!(actual, expected, "{sql}");
    }
    for ansi_mode in [true, false] {
        for sql in [
            "SELECT p.id,p.id IN (SELECT i.id FROM inputs i) AS included,EXISTS(SELECT i.id FROM inputs i WHERE i.id=p.id) AS present FROM inputs p ORDER BY p.id NULLS FIRST",
            "SELECT p.id,p.id IN (SELECT i.id FROM empty_inputs i) AS included,EXISTS(SELECT i.id FROM empty_inputs i WHERE i.id=p.id) AS present FROM inputs p ORDER BY p.id NULLS FIRST",
        ] {
            let query = plan_query(
                &configured,
                sql,
                &SparkSqlOptions {
                    ansi_mode,
                    ..SparkSqlOptions::default()
                },
            )
            .await?;
            let batches = collect(
                query.create_physical_plan(&configured).await?,
                configured.task_ctx(),
            )
            .await?;
            let mut rows = Vec::new();
            for batch in &batches {
                for row in 0..batch.num_rows() {
                    use arrow::array::Array;
                    rows.push(
                        (0..batch.num_columns())
                            .map(|column| {
                                let array = batch.column(column);
                                if array.is_null(row) {
                                    Ok(None)
                                } else {
                                    arrow::util::display::array_value_to_string(array.as_ref(), row)
                                        .map(Some)
                                }
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    );
                }
            }
            let empty = sql.contains("FROM empty_inputs");
            assert_eq!(
                rows[0],
                vec![
                    None,
                    if empty { Some("false".into()) } else { None },
                    Some("false".into())
                ]
            );
            assert_eq!(rows.len(), 4);
            for (index, row) in rows[1..].iter().enumerate() {
                let truth = (!empty).to_string();
                assert_eq!(
                    row,
                    &vec![Some(index.to_string()), Some(truth.clone()), Some(truth)]
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn lowering_does_not_evaluate_volatile_siblings() -> TestResult {
    let records = references()?;
    for batch_size in [1, 32] {
        let session = session(batch_size);
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        let calls = register_counter(&session);
        for ansi_mode in [true, false] {
            for (table, expected_rows) in [("inputs", 4), ("empty_inputs", 0)] {
                for suffix in ["", ",CAST(7 AS INT) AS incidental"] {
                    calls.store(0, Ordering::SeqCst);
                    let sql = format!(
                        "SELECT (SELECT p.id) AS r,count_row(p.id) AS observed{suffix} FROM {table} p"
                    );
                    let query = plan_query(
                        &session,
                        &sql,
                        &SparkSqlOptions {
                            ansi_mode,
                            ..SparkSqlOptions::default()
                        },
                    )
                    .await?;
                    let physical = query.create_physical_plan(&session).await?;
                    assert_eq!(
                        calls.load(Ordering::SeqCst),
                        0,
                        "{sql}: planning executed rows"
                    );
                    let batches = collect(physical, session.task_ctx()).await?;
                    assert_eq!(calls.load(Ordering::SeqCst), expected_rows, "{sql}");
                    assert_eq!(
                        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                        expected_rows
                    );
                    for batch in batches {
                        assert_eq!(batch.column(0), batch.column(1), "{sql}");
                    }
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn real_relations_and_volatile_aliases_are_not_inlined() -> TestResult {
    let records = references()?;
    let session = session(1);
    spark_inputs::register_recorded_inputs(&session, &records[0], 1)?;
    let calls = register_counter(&session);
    let mut options = session.state().config_options().as_ref().clone();
    options.extensions.insert(SparkSqlErrorSemantics::default());
    let mut checked = 0;
    for case in &records[1..] {
        let id = case["id"].as_str().ok_or("missing ID")?;
        if !id.contains("_correlated_")
            && !id.contains("_volatile_direct_")
            && !id.contains("_volatile_nested_")
        {
            continue;
        }
        let query = plan_query(
            &session,
            case["sql"].as_str().ok_or("missing SQL")?,
            &SparkSqlOptions {
                ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
                ..SparkSqlOptions::default()
            },
        )
        .await?;
        let prepared = PreserveSparkExpressionErrors.analyze(query.logical_plan, &options)?;
        assert!(
            contains_scalar(&prepared)?,
            "{id}: lost the retained scalar execution boundary"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "{id}: planning executed a volatile producer"
        );
        checked += 1;
    }
    assert_eq!(checked, 42);
    Ok(())
}

#[tokio::test]
async fn single_row_scalar_plan_route() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    session.register_batch(
        "inputs",
        arrow::record_batch::RecordBatch::try_from_iter([(
            "id",
            std::sync::Arc::new(arrow::array::Int32Array::from(vec![Some(1), None]))
                as arrow::array::ArrayRef,
        )])?,
    )?;
    for suffix in ["", ",CAST(7 AS INT) AS incidental"] {
        let sql = format!(
            "SELECT p.id AS id,(SELECT p.id) AS correlated_id{suffix} \
             FROM (SELECT id FROM inputs) p"
        );
        let query = plan_query(&session, &sql, &SparkSqlOptions::default()).await?;
        let mut state = session.state();
        state
            .config_mut()
            .options_mut()
            .extensions
            .insert(SparkSqlErrorSemantics::default());
        eprintln!(
            "SQL: {sql}\nRESOLVED:\n{}",
            query.logical_plan.display_indent_schema()
        );
        let analyzed = state.analyzer().execute_and_check(
            query.logical_plan.clone(),
            state.config_options(),
            |_, _| {},
        )?;
        eprintln!("ANALYZED:\n{}", analyzed.display_indent_schema());
        let optimized = state.optimize(&query.logical_plan)?;
        eprintln!("OPTIMIZED:\n{}", optimized.display_indent_schema());
        assert!(!contains_scalar(&analyzed)?, "{sql}");
        assert!(!contains_scalar(&optimized)?, "{sql}");
        query.create_physical_plan(&session).await?;
    }
    Ok(())
}
