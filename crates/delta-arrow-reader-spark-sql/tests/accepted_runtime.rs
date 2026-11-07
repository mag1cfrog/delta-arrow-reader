//! Replay the final accepted runtime captures through the consolidated API.
//!
//! This verifies preservation, including documented unresolved differences. It
//! does not classify those historical outputs as Spark-compatible. Ordering is
//! checked separately in the public API tests; this corpus compares row multisets.
//! Explicit parallel-error alternatives are explained in fixtures/README.md.
//! Reviewed fixes replace their exact historical outcomes; old bugs are not passes.

use std::collections::BTreeMap;
use std::error::Error;
use std::io::BufRead;

use arrow::array::Array;
use arrow::util::display::array_value_to_string;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use flate2::read::GzDecoder;
use serde::Deserialize;
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Deserialize)]
struct RecordedQuery {
    id: String,
    sql: String,
    batch_size: usize,
    ansi: bool,
    expected: Value,
}

async fn observe(query: &RecordedQuery) -> TestResult<Value> {
    let session = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(query.batch_size)
                        .with_target_partitions(2),
                ),
        )
        .build(),
    );
    let options = SparkSqlOptions {
        ansi_mode: query.ansi,
        ..SparkSqlOptions::default()
    };
    let planned = match plan_query(&session, &query.sql, &options).await {
        Ok(planned) => planned,
        Err(error) => {
            let message = match error {
                DataFusionError::External(error) => error.to_string(),
                error => error.to_string(),
            };
            return Ok(json!({"status": "planning_error", "error": message}));
        }
    };
    let logical_nullable: Vec<_> = planned
        .logical_plan
        .schema()
        .fields()
        .iter()
        .map(|field| field.is_nullable())
        .collect();
    let executed: datafusion::error::Result<_> = async {
        let physical = planned.create_physical_plan(&session).await?;
        let schema = physical.schema();
        let batches = collect(physical, session.task_ctx()).await?;
        Ok((schema, batches))
    }
    .await;
    let (schema, batches) = match executed {
        Ok(executed) => executed,
        Err(error) => {
            return Ok(json!({
                "status": "execution_error",
                "error": error.to_string(),
                "output_names": planned.column_names,
            }));
        }
    };
    let mut rows = Vec::new();
    for batch in batches {
        for row_index in 0..batch.num_rows() {
            let row = batch
                .columns()
                .iter()
                .map(|column| {
                    if column.is_null(row_index) {
                        Ok(Value::Null)
                    } else {
                        array_value_to_string(column.as_ref(), row_index).map(Value::String)
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            rows.push(Value::Array(row));
        }
    }
    rows.sort_by_cached_key(Value::to_string);
    Ok(json!({
        "status": "ok",
        "types": schema.fields().iter().map(|field| format!("{:?}", field.data_type())).collect::<Vec<_>>(),
        "rows": rows,
        "output_names": planned.column_names,
        "logical_nullable": logical_nullable,
        "physical_nullable": schema.fields().iter().map(|field| field.is_nullable()).collect::<Vec<_>>(),
    }))
}

fn apply_reviewed_integral_cast_condition(
    query: &mut RecordedQuery,
    conditions: &mut BTreeMap<String, usize>,
    parallel_errors: &BTreeMap<String, Value>,
) -> TestResult {
    let Some(batch_size) = conditions.remove(&query.id) else {
        return Ok(());
    };
    if !query.ansi
        || query.batch_size != batch_size
        || query.expected["status"] != "execution_error"
        || parallel_errors.contains_key(&query.id)
    {
        return Err(format!("invalid integral-cast correction settings for {}", query.id).into());
    }
    let error = query.expected["error"]
        .as_str()
        .ok_or("missing cast error")?;
    let root = "Arrow error: Cast error: Cannot cast string ";
    if error.matches(root).count() != 1
        || error.contains("CAST_INVALID_INPUT")
        || ![8, 16, 32, 64]
            .iter()
            .any(|width| error.ends_with(&format!(" to value of Int{width} type")))
    {
        return Err(format!("invalid integral-cast correction cause for {}", query.id).into());
    }
    query.expected["error"] =
        json!(error.replacen(root, &format!("[CAST_INVALID_INPUT]\ncaused by\n{root}"), 1));
    Ok(())
}

fn matches_recorded_outcome(
    query: &RecordedQuery,
    actual: &Value,
    parallel_errors: &BTreeMap<String, Value>,
) -> bool {
    std::iter::once(&query.expected)
        .chain(parallel_errors.get(&query.id))
        .any(|expected| {
            expected.as_object().is_some_and(|fields| {
                fields.iter().all(|(field, value)| {
                    let actual_value = actual.get(field);
                    // Only this analyzer name changed during consolidation.
                    let normalized_error = if field == "error" {
                        actual_value.and_then(Value::as_str).map(|message| {
                            Value::String(message.replace(
                                "preserve_spark_expression_errors\n",
                                "spark_decimal_null_propagation\n",
                            ))
                        })
                    } else {
                        None
                    };
                    normalized_error.as_ref().or(actual_value) == Some(value)
                })
            })
        })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preserve_accepted_query_outcomes() -> TestResult {
    let compressed = include_bytes!("fixtures/accepted-runtime.jsonl.gz");
    let reader = std::io::BufReader::new(GzDecoder::new(&compressed[..]));
    let parallel_errors: BTreeMap<String, Value> =
        serde_json::from_str(include_str!("fixtures/parallel-error-outcomes.json"))?;
    let mut corrections: BTreeMap<String, Value> =
        serde_json::from_str(include_str!("fixtures/reviewed-corrections.json"))?;
    let mut cast_conditions: BTreeMap<String, usize> = serde_json::from_str(include_str!(
        "fixtures/reviewed-integral-cast-conditions.json"
    ))?;
    assert_eq!(cast_conditions.len(), 817);
    let mut checked = 0;
    let mut failures = Vec::new();
    for line in reader.lines() {
        let mut query: RecordedQuery = serde_json::from_str(&line?)?;
        if let Some(corrected) = corrections.remove(&query.id) {
            query.expected = corrected;
        }
        apply_reviewed_integral_cast_condition(&mut query, &mut cast_conditions, &parallel_errors)?;
        if let Some(rows) = query.expected.get_mut("rows").and_then(Value::as_array_mut) {
            rows.sort_by_cached_key(Value::to_string);
        }
        let actual = observe(&query).await?;
        if !matches_recorded_outcome(&query, &actual, &parallel_errors) {
            failures.push(format!(
                "{}: expected {} or {:?}, got {actual}\nSQL: {}",
                query.id,
                query.expected,
                parallel_errors.get(&query.id),
                query.sql,
            ));
        }
        checked += 1;
        if checked % 1000 == 0 {
            eprintln!("Replayed {checked} accepted-runtime observations");
        }
        if failures.len() == 20 {
            return Err(format!(
                "Stopped after {checked} observations with 20 differences:\n{}",
                failures.join("\n")
            )
            .into());
        }
    }
    let manifest: Value = serde_json::from_str(include_str!("fixtures/accepted-runtime.json"))?;
    assert!(
        corrections.is_empty(),
        "corrections must refer to replayed observations"
    );
    assert!(
        cast_conditions.is_empty(),
        "cast corrections must refer to replayed observations"
    );
    assert_eq!(
        checked,
        manifest["unique_observations"]
            .as_u64()
            .ok_or("missing corpus count")?,
        "the complete deduplicated corpus must run"
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[test]
fn unary_negation_corrections_preserve_the_recorded_contract() -> TestResult {
    let references = include_str!("fixtures/unary-negate-fields/historical/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(references.len(), 6);
    let corrections: BTreeMap<String, Value> =
        serde_json::from_str(include_str!("fixtures/reviewed-corrections.json"))?;
    let parallel_errors: BTreeMap<String, Value> =
        serde_json::from_str(include_str!("fixtures/parallel-error-outcomes.json"))?;
    let reader = std::io::BufReader::new(GzDecoder::new(
        &include_bytes!("fixtures/accepted-runtime.jsonl.gz")[..],
    ));
    let schema = json!({"type":"struct","fields":[{
        "name":"r","type":"long","nullable":false,"metadata":{}
    }]});
    let mut checked = std::collections::BTreeSet::new();
    for line in reader.lines() {
        let original: RecordedQuery = serde_json::from_str(&line?)?;
        let Some(reference) = references[1..]
            .iter()
            .find(|reference| reference["id"].as_str() == Some(original.id.as_str()))
        else {
            continue;
        };
        assert!(checked.insert(original.id.clone()));
        assert_eq!(reference["sql"], original.sql);
        assert_eq!(reference["ansi"], original.ansi);
        assert_eq!(original.batch_size, 1);
        assert!(!parallel_errors.contains_key(&original.id));
        assert_eq!(reference["expected"]["schema"], schema);
        for stage in [
            "public",
            "analyzed",
            "optimized",
            "executed_before_collect",
            "executed",
        ] {
            assert_eq!(reference["schema_stages"][stage], schema, "{stage}");
        }
        let mut spark_rows = reference["expected"]["rows"]
            .as_array()
            .ok_or("missing Spark rows")?
            .clone();
        spark_rows.sort_by_cached_key(Value::to_string);
        assert_eq!(json!(spark_rows), original.expected["rows"]);
        let corrected = corrections
            .get(&original.id)
            .ok_or("missing unary correction")?;
        let mut expected = original.expected.clone();
        expected["logical_nullable"] = json!([false]);
        expected["physical_nullable"] = json!([false]);
        assert_eq!(*corrected, expected);
        let mut query = original.clone();
        query.expected = corrected.clone();
        assert!(matches_recorded_outcome(
            &query,
            corrected,
            &parallel_errors
        ));
        assert!(!matches_recorded_outcome(
            &query,
            &original.expected,
            &parallel_errors
        ));
        for (field, value) in [
            ("logical_nullable", json!([true])),
            ("physical_nullable", json!([true])),
            ("rows", json!([["wrong value"]])),
            ("types", json!(["Int32"])),
            ("output_names", json!(["wrong_name"])),
            ("status", json!("execution_error")),
        ] {
            let mut wrong = corrected.clone();
            wrong[field] = value;
            assert!(
                !matches_recorded_outcome(&query, &wrong, &parallel_errors),
                "{field}"
            );
        }
    }
    assert_eq!(checked.len(), 5);
    assert_eq!(
        checked,
        references[1..]
            .iter()
            .map(|reference| {
                reference["id"]
                    .as_str()
                    .ok_or("missing reference ID")
                    .map(str::to_owned)
            })
            .collect::<Result<std::collections::BTreeSet<_>, _>>()?
    );
    Ok(())
}

#[test]
fn integral_cast_corrections_require_the_condition_and_original_contract() -> TestResult {
    let mut conditions: BTreeMap<String, usize> = serde_json::from_str(include_str!(
        "fixtures/reviewed-integral-cast-conditions.json"
    ))?;
    assert_eq!(conditions.len(), 817);
    let mut corrections: BTreeMap<String, Value> =
        serde_json::from_str(include_str!("fixtures/reviewed-corrections.json"))?;
    let parallel_errors: BTreeMap<String, Value> =
        serde_json::from_str(include_str!("fixtures/parallel-error-outcomes.json"))?;
    let reader = std::io::BufReader::new(GzDecoder::new(
        &include_bytes!("fixtures/accepted-runtime.jsonl.gz")[..],
    ));
    let mut exemplar = None;
    let mut checked = 0;
    for line in reader.lines() {
        let mut query: RecordedQuery = serde_json::from_str(&line?)?;
        if let Some(corrected) = corrections.remove(&query.id) {
            query.expected = corrected;
        }
        if !conditions.contains_key(&query.id) {
            continue;
        }
        let original = query.clone();
        apply_reviewed_integral_cast_condition(&mut query, &mut conditions, &parallel_errors)?;
        assert!(matches_recorded_outcome(
            &query,
            &query.expected,
            &parallel_errors
        ));
        assert!(!matches_recorded_outcome(
            &query,
            &original.expected,
            &parallel_errors
        ));
        exemplar.get_or_insert((original, query));
        checked += 1;
    }
    assert_eq!(checked, 817);
    assert!(conditions.is_empty());
    let (original, corrected) = exemplar.ok_or("missing integral-cast correction")?;
    let error = corrected.expected["error"]
        .as_str()
        .ok_or("missing error")?;
    for (field, value) in [
        ("error", original.expected["error"].clone()),
        (
            "error",
            json!(format!("[CAST_INVALID_INPUT]\ncaused by\n{error}")),
        ),
        ("error", json!(format!("{error} (different input)"))),
        ("status", json!("planning_error")),
        ("status", json!("ok")),
        ("output_names", json!(["wrong_name"])),
    ] {
        let mut wrong = corrected.expected.clone();
        wrong[field] = value;
        assert!(!matches_recorded_outcome(
            &corrected,
            &wrong,
            &parallel_errors
        ));
    }
    for (ansi, batch_size) in [
        (false, original.batch_size),
        (true, original.batch_size + 1),
    ] {
        let mut wrong = original.clone();
        wrong.ansi = ansi;
        wrong.batch_size = batch_size;
        let mut conditions = BTreeMap::from([(original.id.clone(), original.batch_size)]);
        assert!(
            apply_reviewed_integral_cast_condition(&mut wrong, &mut conditions, &parallel_errors)
                .is_err()
        );
    }
    for (field, value) in [
        ("status", json!("planning_error")),
        ("error", json!("unrelated error")),
        (
            "error",
            json!("Arrow error: Cast error: Cannot cast string 'bad' to value of Float64 type"),
        ),
        ("error", corrected.expected["error"].clone()),
    ] {
        let mut wrong = original.clone();
        wrong.expected[field] = value;
        let mut conditions = BTreeMap::from([(original.id.clone(), original.batch_size)]);
        assert!(
            apply_reviewed_integral_cast_condition(&mut wrong, &mut conditions, &parallel_errors)
                .is_err()
        );
    }
    let mut with_alternative = original.clone();
    let mut conditions = BTreeMap::from([(original.id.clone(), original.batch_size)]);
    let alternative = BTreeMap::from([(original.id.clone(), original.expected.clone())]);
    assert!(
        apply_reviewed_integral_cast_condition(
            &mut with_alternative,
            &mut conditions,
            &alternative,
        )
        .is_err()
    );
    let mut unlisted = original.clone();
    unlisted.id.push_str("/unlisted");
    let mut conditions = BTreeMap::from([(original.id, original.batch_size)]);
    apply_reviewed_integral_cast_condition(&mut unlisted, &mut conditions, &parallel_errors)?;
    assert_eq!(conditions.len(), 1);
    assert!(!matches_recorded_outcome(
        &unlisted,
        &corrected.expected,
        &parallel_errors
    ));
    Ok(())
}

#[test]
fn parallel_error_alternatives_preserve_other_guarantees() -> TestResult {
    let parallel_errors: BTreeMap<String, Value> =
        serde_json::from_str(include_str!("fixtures/parallel-error-outcomes.json"))?;
    assert_eq!(parallel_errors.len(), 20);
    let integral_cast_ids = [
        "replay/cast-empty-group-scalar-cases/1449/only_int_union_exists_true",
        "replay/cast-empty-group-scalar-cases/1450/only_int_union_count_true",
    ];
    let reader = std::io::BufReader::new(GzDecoder::new(
        &include_bytes!("fixtures/accepted-runtime.jsonl.gz")[..],
    ));
    let mut checked = 0;
    let mut checked_integral_casts = 0;
    for line in reader.lines() {
        let mut query: RecordedQuery = serde_json::from_str(&line?)?;
        let Some(alternative) = parallel_errors.get(&query.id) else {
            continue;
        };
        assert!(query.ansi);
        assert_eq!(query.batch_size, 2);
        assert_eq!(alternative["status"], "execution_error");
        assert_eq!(alternative["output_names"], query.expected["output_names"]);
        assert_ne!(alternative, &query.expected);
        assert!(matches_recorded_outcome(
            &query,
            &query.expected,
            &parallel_errors
        ));
        assert!(matches_recorded_outcome(
            &query,
            alternative,
            &parallel_errors
        ));

        if integral_cast_ids.contains(&query.id.as_str()) {
            let cause = "Arrow error: Cast error: Cannot cast string 'bad' to value of Int32 type";
            let error = format!("[CAST_INVALID_INPUT]\ncaused by\n{cause}");
            assert_eq!(alternative["error"], error);
            for wrong_error in [
                cause.to_owned(),
                format!("[CAST_INVALID_INPUT]\ncaused by\n{error}"),
            ] {
                let mut wrong = alternative.clone();
                wrong["error"] = json!(wrong_error);
                assert!(!matches_recorded_outcome(&query, &wrong, &parallel_errors));
            }
            checked_integral_casts += 1;
        }

        for (field, value) in [
            ("status", json!("planning_error")),
            ("error", json!("unrelated error")),
            (
                "error",
                json!(format!(
                    "{} (different input)",
                    alternative["error"].as_str().ok_or("missing error")?
                )),
            ),
            ("output_names", json!(["wrong_name"])),
        ] {
            let mut wrong = alternative.clone();
            wrong[field] = value;
            assert!(!matches_recorded_outcome(&query, &wrong, &parallel_errors));
        }
        if query.expected["status"] == "ok" {
            for (field, value) in [
                ("rows", json!([[null]])),
                ("rows", json!([["false"]])),
                ("rows", json!([["0"]])),
                ("types", json!(["Utf8"])),
                ("output_names", json!(["wrong_name"])),
                ("logical_nullable", json!([true])),
                ("physical_nullable", json!([true])),
            ] {
                let mut wrong = query.expected.clone();
                wrong[field] = value;
                assert!(!matches_recorded_outcome(&query, &wrong, &parallel_errors));
            }
        } else {
            let wrong = json!({"status":"ok", "rows":[[null]], "output_names":["r"]});
            assert!(!matches_recorded_outcome(&query, &wrong, &parallel_errors));
        }
        query.id.push_str("/unlisted");
        assert!(!matches_recorded_outcome(
            &query,
            alternative,
            &parallel_errors
        ));
        checked += 1;
    }
    assert_eq!(
        checked,
        parallel_errors.len(),
        "every exception must identify a captured query"
    );
    assert_eq!(checked_integral_casts, integral_cast_ids.len());
    Ok(())
}

#[tokio::test]
async fn grouped_integral_cast_keeps_the_parallel_error_envelope() -> TestResult {
    let parallel_errors: BTreeMap<String, Value> =
        serde_json::from_str(include_str!("fixtures/parallel-error-outcomes.json"))?;
    let query = RecordedQuery {
        id: "grouped_integral_cast_branch".to_owned(),
        sql: concat!(
            "SELECT CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) AS r ",
            "FROM range(3) ",
            "GROUP BY CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT)"
        )
        .to_owned(),
        batch_size: 2,
        ansi: true,
        expected: parallel_errors
            .get("replay/cast-empty-group-scalar-cases/1450/only_int_union_count_true")
            .ok_or("missing integral cast alternative")?
            .clone(),
    };
    assert_eq!(observe(&query).await?, query.expected);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rand_divisor_correction_requires_its_wrapper_and_original_cause() -> TestResult {
    let corrections: BTreeMap<String, Value> =
        serde_json::from_str(include_str!("fixtures/reviewed-corrections.json"))?;
    let parallel_errors: BTreeMap<String, Value> =
        serde_json::from_str(include_str!("fixtures/parallel-error-outcomes.json"))?;
    let reader = std::io::BufReader::new(GzDecoder::new(
        &include_bytes!("fixtures/accepted-runtime.jsonl.gz")[..],
    ));
    let mut original = None;
    for line in reader.lines() {
        let query: RecordedQuery = serde_json::from_str(&line?)?;
        if query.id == "replay/existing-nonansi-zero-cases/112/volatile_divisor_true" {
            original = Some(query);
            break;
        }
    }
    let original = original.ok_or("missing recorded RAND divisor")?;
    assert!(original.ansi);
    assert_eq!(original.batch_size, 3);
    assert_eq!(
        original.sql,
        concat!(
            "SELECT CAST(2 AS DOUBLE) / CASE WHEN RAND(42) >= 0 ",
            "THEN CAST('-0.0' AS DOUBLE) ELSE CAST(3 AS DOUBLE) END AS r FROM range(4)"
        )
    );
    assert!(!parallel_errors.contains_key(&original.id));
    let cause = "Arrow error: Divide by zero error";
    assert_eq!(original.expected["error"], cause);
    let mut expected = original.expected.clone();
    expected["error"] = json!(format!("spark_decimal_null_propagation\ncaused by\n{cause}"));
    assert_eq!(corrections.get(&original.id), Some(&expected));
    let mut query = original.clone();
    query.expected = expected;
    let raw_error = format!("preserve_spark_expression_errors\ncaused by\n{cause}");
    let mut raw_expected = query.expected.clone();
    raw_expected["error"] = json!(raw_error.clone());
    let actual = observe(&original).await?;
    assert_eq!(actual, raw_expected);
    assert!(matches_recorded_outcome(&query, &actual, &parallel_errors));
    for (field, value) in [
        ("error", json!(cause)),
        (
            "error",
            json!(format!("preserve_spark_expression_errors\ncaused by\n{raw_error}")),
        ),
        ("error", json!(format!("{raw_error} (different cause)"))),
        ("status", json!("planning_error")),
        ("status", json!("ok")),
        ("output_names", json!(["wrong_name"])),
    ] {
        let mut wrong = actual.clone();
        wrong[field] = value;
        assert!(!matches_recorded_outcome(&query, &wrong, &parallel_errors));
    }
    let mut unlisted = original;
    unlisted.id.push_str("/unlisted");
    assert!(!corrections.contains_key(&unlisted.id));
    assert!(!matches_recorded_outcome(&unlisted, &actual, &parallel_errors));
    Ok(())
}
