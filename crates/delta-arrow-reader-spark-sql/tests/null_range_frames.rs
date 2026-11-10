//! Compare NULL RANGE and NULL ROWS admission with Spark, with explicit typed-NULL ROWS rejection.

use std::collections::{BTreeMap, BTreeSet};

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/null_rows.rs"]
mod null_rows;
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{
    STAGES, TestResult, case_identity, compare_exact_spark_query_stages_with_optimized,
    native_result_matches_spark_reference, validate_spark_reference_result,
};

const REQUEST: &str = include_str!("fixtures/null-range-754-cases.json");
const REFERENCE: &str = include_str!("fixtures/null-range-754-spark.jsonl");
const ORIGINAL_REQUEST: &str = include_str!("fixtures/date-range-747-cases.json");
const ORIGINAL_REFERENCE: &str = include_str!("fixtures/date-range-747-spark.jsonl");
const BOUNDARY_REQUEST: &str = include_str!("fixtures/null-range-754-boundary-cases.json");
const BOUNDARY_REFERENCE: &str = include_str!("fixtures/null-range-754-boundary-spark.jsonl");
const PRIOR_ROWS_REJECTIONS: &str =
    include_str!("fixtures/null-range-754-prior-rows-rejections.json");
const ORIGINAL_NULL_ROWS_IDS: [&str; 4] = [
    "admission_rows_lower_typed_null",
    "admission_rows_upper_typed_null",
    "admission_rows_lower_bare_null",
    "admission_rows_upper_bare_null",
];
fn validate_null_range_references(request: &Value, records: &[Value]) -> TestResult {
    if request["issue"] != 754
        || request["expected_record_count"] != 582
        || request["original_record_count"] != 14
        || request["supplement_record_count"] != 568
        || request["batch_sizes"] != json!([1, 32])
        || request["target_partitions"] != 2
        || request["provider_partitions"] != 2
        || records.len() != 583
    {
        return Err("NULL RANGE source or reference inventory differs".into());
    }
    let corpora = request["corpora"].as_array().ok_or("missing corpora")?;
    let original: Value = serde_json::from_str(ORIGINAL_REQUEST)?;
    let mut original_corpora = Vec::new();
    for corpus in original["corpora"]
        .as_array()
        .ok_or("missing original corpora")?
    {
        let cases = corpus["cases"]
            .as_array()
            .ok_or("missing original cases")?
            .iter()
            .filter(|case| case["ownership"] == "null_offset_owner")
            .cloned()
            .collect::<Vec<_>>();
        if !cases.is_empty() {
            let mut selected = corpus.clone();
            selected["cases"] = json!(cases);
            original_corpora.push(selected);
        }
    }
    if corpora.len() != 3
        || &corpora[..2] != original_corpora.as_slice()
        || corpora[2]["corpus"] != "null-range-754-supplement"
        || corpora[2]["cases"]
            .as_array()
            .is_none_or(|cases| cases.len() != 568)
    {
        return Err("original NULL RANGE SQL, identities or providers changed".into());
    }
    let mut definitions = BTreeMap::new();
    for corpus in corpora {
        for case in corpus["cases"].as_array().ok_or("missing source cases")? {
            if case["corpus"] != corpus["corpus"]
                || !case["ordered"].is_boolean()
                || !case["sql"].is_string()
                || case["ownership"] != "null_offset_owner"
                || definitions.insert(case_identity(case)?, case).is_some()
            {
                return Err("malformed or duplicate NULL RANGE source identity".into());
            }
        }
    }
    if definitions.len() != 582
        || definitions
            .values()
            .filter(|case| case["ordered"] == true)
            .count()
            != 24
    {
        return Err("NULL RANGE source identity or ordering inventory differs".into());
    }
    let header = &records[0];
    if header["issue"] != 754
        || header["spark_version"] != "4.2.0"
        || header["spark_build"]["SPARK_REVISION"]["value"]
            != "32f7299601108917fb01920a54e084595b7b3bf8"
        || header["expected_record_count"] != 582
        || header["effective_settings"]["spark.sql.session.timeZone"] != "UTC"
        || header["effective_settings"]["spark.sql.shuffle.partitions"] != "1"
    {
        return Err("unexpected NULL RANGE Spark reference runtime".into());
    }
    let captured = header["corpora"]
        .as_array()
        .ok_or("missing provider receipts")?;
    if captured.len() != corpora.len() {
        return Err("NULL RANGE captured provider inventory differs".into());
    }
    for corpus in corpora {
        let receipt = captured
            .iter()
            .find(|item| item["corpus"] == corpus["corpus"])
            .ok_or("missing captured corpus")?;
        let inputs = corpus["metadata"]["inputs"]
            .as_array()
            .ok_or("missing inputs")?;
        let providers = receipt["registered_inputs"]
            .as_array()
            .ok_or("missing actual providers")?;
        if receipt["metadata"] != corpus["metadata"]
            || receipt["source"] != corpus["source"]
            || providers.len() != inputs.len()
        {
            return Err("NULL RANGE captured providers differ from source".into());
        }
        for (input, provider) in inputs.iter().zip(providers) {
            if ["name", "schema", "rows"]
                .iter()
                .any(|key| input[*key] != provider[*key])
                || provider["schema_observed"] != input["schema"]
                || provider["num_partitions"]
                    .as_u64()
                    .is_none_or(|count| count == 0)
            {
                return Err("NULL RANGE actual provider receipt differs".into());
            }
        }
    }
    let original_records = ORIGINAL_REFERENCE
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let originals = original_records
        .iter()
        .filter(|case| case["ownership"] == "null_offset_owner")
        .map(|case| Ok((case_identity(case)?, case)))
        .collect::<TestResult<BTreeMap<_, _>>>()?;
    let mut seen = BTreeSet::new();
    let mut original_count = 0;
    for case in &records[1..] {
        let key = case_identity(case)?;
        let definition = definitions
            .get(&key)
            .ok_or("unexpected NULL RANGE reference")?;
        if !seen.insert(key.clone())
            || ["sql", "ordered", "ownership", "coverage"]
                .iter()
                .any(|field| case.get(*field) != definition.get(*field))
            || !matches!(case["status"].as_str(), Some("ok" | "error"))
            || case.get("capture_failure").is_some()
        {
            return Err("NULL RANGE reference identity or source contract differs".into());
        }
        let settings = case["effective_settings"]
            .as_object()
            .ok_or("missing actual settings")?;
        let header_settings = header["effective_settings"]
            .as_object()
            .ok_or("missing header settings")?;
        if settings.len() != 8
            || header_settings.len() != 8
            || settings.iter().any(|(name, value)| {
                if name == "spark.sql.ansi.enabled" {
                    value.as_str() != Some(if key.2 { "true" } else { "false" })
                } else {
                    header_settings.get(name) != Some(value)
                }
            })
        {
            return Err("NULL RANGE actual settings differ from source ANSI/runtime".into());
        }
        if let Some(original) = originals.get(&key) {
            original_count += 1;
            if [
                "status",
                "expected",
                "schema_stages",
                "schema_stage_availability",
                "rows_multiset",
            ]
            .iter()
            .any(|field| case.get(*field) != original.get(*field))
            {
                return Err("original independent NULL RANGE outcome changed".into());
            }
        }
        validate_spark_reference_result(case)?;
    }
    if seen.len() != definitions.len() || originals.len() != 14 || original_count != 14 {
        return Err("missing original or supplementary NULL RANGE reference identity".into());
    }
    Ok(())
}

fn parse_null_range_fixture() -> TestResult<(Value, Vec<Value>)> {
    let request = serde_json::from_str(REQUEST)?;
    let records = REFERENCE
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    validate_null_range_references(&request, &records)?;
    Ok((request, records))
}

fn is_original_null_rows_case(case: &Value) -> bool {
    case["corpus"] == "null-range-754-supplement"
        && case["id"]
            .as_str()
            .is_some_and(|id| ORIGINAL_NULL_ROWS_IDS.contains(&id))
}

fn validate_prior_rows_rejections(records: &[Value], controls: &[Value]) -> TestResult {
    let mut expected = BTreeSet::new();
    for case in records[1..]
        .iter()
        .filter(|case| is_original_null_rows_case(case))
    {
        for batch in [1_u64, 32] {
            expected.insert((case_identity(case)?, batch));
        }
    }
    let mut seen = BTreeSet::new();
    for control in controls {
        let batch = control["batch"]
            .as_u64()
            .ok_or("missing prior ROWS batch")?;
        if !seen.insert((case_identity(control)?, batch)) {
            return Err("duplicate prior ROWS rejection identity/batch".into());
        }
        let observation = &control["observation"];
        let error = "External error: invalid argument: invalid window boundary offset";
        if observation["native_phase"] != "resolve"
            || observation["error"] != error
            || observation["optimized_error"] != error
            || observation["matches"] != false
            || observation["optimized_matches"] != false
            || observation.get("observer_error").is_some()
            || [
                "public_schema",
                "optimized_schema",
                "before_collect",
                "physical_schema",
                "optimized_observation",
                "rows",
                "rows_raw",
            ]
            .iter()
            .any(|field| !observation.get(*field).is_some_and(Value::is_null))
        {
            return Err("prior ROWS profile is not the required resolution rejection".into());
        }
    }
    if controls.len() != 16 || expected.len() != 16 || seen != expected {
        return Err("prior ROWS rejection inventory differs".into());
    }
    Ok(())
}

fn prior_rows_rejection_matches(
    case: &Value,
    batch: usize,
    observation: &Value,
    controls: &[Value],
) -> bool {
    controls.iter().any(|control| {
        control["corpus"] == case["corpus"]
            && control["id"] == case["id"]
            && control["ansi"] == case["ansi"]
            && control["batch"] == batch
            && control["observation"] == *observation
    })
}

fn validate_float_decimal_references(request: &Value, records: &[Value]) -> TestResult {
    let corpora = request["corpora"]
        .as_array()
        .ok_or("missing boundary corpora")?;
    let [corpus] = corpora.as_slice() else {
        return Err("expected one float/decimal boundary corpus".into());
    };
    let header = records.first().ok_or("missing boundary capture header")?;
    let definitions = corpus["cases"].as_array().ok_or("missing boundary cases")?;
    if request["issue"] != 754
        || request["expected_record_count"] != 16
        || request["spark_version"] != "4.2.0"
        || request["spark_revision"] != "32f7299601108917fb01920a54e084595b7b3bf8"
        || definitions.len() != 16
        || records.len() != 17
        || corpus["corpus"] != "null-range-boundary-754-controls"
        || corpus["metadata"]["inputs"] != json!([])
        || corpus["metadata"]["spark_version"] != request["spark_version"]
        || corpus["metadata"]["timezone"] != "UTC"
        || header["issue"] != 754
        || header["expected_record_count"] != 16
        || header["spark_only"] != true
        || header["reads_expected_outputs"] != false
        || header["spark_version"] != request["spark_version"]
        || header["effective_settings"]["spark.sql.session.timeZone"] != "UTC"
        || header["spark_build"]["SPARK_REVISION"]["value"] != request["spark_revision"]
        || header["schema_stage_protocol"] != json!(STAGES.map(|(name, _)| name))
        || header["corpora"]
            .as_array()
            .is_none_or(|corpora| corpora.len() != 1)
        || ["corpus", "source", "metadata"]
            .iter()
            .any(|field| header["corpora"][0][*field] != corpus[*field])
        || header["corpora"][0]["registered_inputs"] != json!([])
    {
        return Err("float/decimal source or capture inventory differs".into());
    }
    let mut expected = BTreeSet::new();
    let mut sql_modes = BTreeMap::<String, BTreeSet<bool>>::new();
    for definition in definitions {
        let key = case_identity(definition)?;
        let sql = definition["sql"].as_str().ok_or("missing boundary SQL")?;
        if definition["corpus"] != corpus["corpus"]
            || definition["ordered"] != true
            || !definition["coverage"].is_object()
            || !expected.insert(key.clone())
        {
            return Err("duplicate or incomplete boundary source identity".into());
        }
        sql_modes.entry(sql.into()).or_default().insert(key.2);
    }
    if sql_modes.len() != 8
        || sql_modes
            .values()
            .any(|modes| *modes != BTreeSet::from([false, true]))
    {
        return Err("boundary sources require eight ordered SQL queries in both ANSI modes".into());
    }
    let mut seen = BTreeSet::new();
    for case in &records[1..] {
        let key = case_identity(case)?;
        let definition = definitions
            .iter()
            .find(|definition| case_identity(definition).ok().as_ref() == Some(&key))
            .ok_or("unexpected boundary reference identity")?;
        if !seen.insert(key.clone())
            || case["status"] != "ok"
            || case.get("capture_failure").is_some()
            || case.get("spark_error_details").is_some()
            || ["sql", "ordered", "coverage"]
                .iter()
                .any(|field| case.get(*field) != definition.get(*field))
        {
            return Err("boundary reference differs from source or capture failed".into());
        }
        let settings = case["effective_settings"]
            .as_object()
            .ok_or("missing boundary settings")?;
        let header_settings = header["effective_settings"]
            .as_object()
            .ok_or("missing boundary header settings")?;
        if settings.len() != 8
            || header_settings.len() != 8
            || settings.iter().any(|(name, value)| {
                if name == "spark.sql.ansi.enabled" {
                    value.as_str() != Some(if key.2 { "true" } else { "false" })
                } else {
                    header_settings.get(name) != Some(value)
                }
            })
        {
            return Err("boundary actual settings differ from source ANSI/runtime".into());
        }
        validate_spark_reference_result(case)?;
    }
    if seen != expected {
        return Err("missing float/decimal boundary reference identity".into());
    }
    Ok(())
}

fn parse_float_decimal_fixture() -> TestResult<(Value, Vec<Value>)> {
    let request = serde_json::from_str(BOUNDARY_REQUEST)?;
    let records = BOUNDARY_REFERENCE
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    validate_float_decimal_references(&request, &records)?;
    Ok((request, records))
}

#[tokio::test]
async fn null_range_float_and_decimal_boundaries_match_spark() -> TestResult {
    let (request, records) = parse_float_decimal_fixture()?;
    let mut completed = BTreeSet::new();
    let mut differences = Vec::new();
    for batch in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_batch_size(batch)
                            .with_target_partitions(2),
                    ),
            )
            .build(),
        );
        spark_inputs::register_recorded_inputs(
            &session,
            &request["corpora"][0]["metadata"],
            batch,
        )?;
        for case in &records[1..] {
            let observation = compare_exact_spark_query_stages_with_optimized(&session, case)
                .await
                .unwrap_or_else(|error| json!({"observer_error":error.to_string()}));
            let matches = native_result_matches_spark_reference(&observation, case);
            let unique = completed.insert((case_identity(case)?, batch));
            if !matches || !unique {
                differences.push(format!(
                    "{} ANSI={} batch={batch}",
                    case["id"], case["ansi"]
                ));
            }
            eprintln!(
                "NULL_RANGE_BOUNDARY_OBSERVATION {}",
                json!({
                    "issue":754,"corpus":case["corpus"],"id":case["id"],"ansi":case["ansi"],
                    "batch":batch,"target_partitions":2,"reference":case,
                    "observation":observation,"spark_matches":matches
                })
            );
        }
    }
    assert_eq!(completed.len(), 32);
    assert!(
        differences.is_empty(),
        "float/decimal NULL RANGE differences:\n{}",
        differences.join("\n")
    );
    Ok(())
}

#[test]
fn float_decimal_references_reject_identity_fields_and_order_loss() -> TestResult {
    let (request, records) = parse_float_decimal_fixture()?;
    let mut missing = records.clone();
    missing.pop();
    assert!(validate_float_decimal_references(&request, &missing).is_err());
    let mut duplicate = records.clone();
    duplicate[2] = duplicate[1].clone();
    assert!(validate_float_decimal_references(&request, &duplicate).is_err());
    for pointer in [
        "/schema_stages/public/fields/0/metadata",
        "/schema_stage_availability/executed/available",
        "/rows_raw",
    ] {
        let mut changed = records.clone();
        *changed[1]
            .pointer_mut(pointer)
            .ok_or("missing boundary fault target")? = Value::Null;
        assert!(
            validate_float_decimal_references(&request, &changed).is_err(),
            "accepted lost {pointer}"
        );
    }
    let mut changed = records.clone();
    changed[1]["ordered"] = json!(false);
    assert!(validate_float_decimal_references(&request, &changed).is_err());
    let mut changed = records.clone();
    changed[1]["expected"]["rows"]
        .as_array_mut()
        .ok_or("missing ordered boundary rows")?
        .reverse();
    assert!(validate_float_decimal_references(&request, &changed).is_err());
    let mut changed = records.clone();
    changed[1]["capture_failure"] = json!({"message":"observer failed"});
    assert!(validate_float_decimal_references(&request, &changed).is_err());
    Ok(())
}

#[tokio::test]
async fn dictionary_float_null_ranges_match_the_recorded_logical_values() -> TestResult {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, DictionaryArray, Float32Array, Float64Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::window_frame::RangeOffsetArithmetic;
    use datafusion::physical_optimizer::aggregate_statistics::PreserveAggregateStatisticsSchema;
    use delta_arrow_reader_spark_sql::{SparkSqlOptions, plan_query};
    use sail_plan::expression_errors::SparkSqlExpressionSemantics;

    let (_, records) = parse_float_decimal_fixture()?;
    let mut completed = BTreeSet::new();
    let mut differences = Vec::new();
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
        for reference in records[1..].iter().filter(|case| {
            matches!(
                case["coverage"]["key_type"].as_str(),
                Some("FLOAT" | "DOUBLE")
            )
        }) {
            let kind = reference["coverage"]["key_type"]
                .as_str()
                .ok_or("missing float type")?;
            let ansi_mode = reference["ansi"].as_bool().ok_or("missing ANSI mode")?;
            let id = reference["id"].as_str().ok_or("missing boundary ID")?;
            let rows = reference["coverage"]["inline_values"]
                .as_array()
                .ok_or("missing source VALUES")?;
            let suffix = format!(" AS {kind})");
            let mut ids = Vec::new();
            let mut values = Vec::new();
            let mut source_rows = Vec::new();
            for row in rows {
                let id: i32 = row[0].as_i64().ok_or("missing source id")?.try_into()?;
                let expression = row[1].as_str().ok_or("missing source float literal")?;
                let literal = expression
                    .strip_prefix("CAST(")
                    .and_then(|value| value.strip_suffix(&suffix))
                    .ok_or("unexpected source float cast")?;
                let value = match literal {
                    "NULL" => None,
                    "'-Infinity'" => Some(f64::NEG_INFINITY),
                    "'0'" => Some(0.0),
                    "'Infinity'" => Some(f64::INFINITY),
                    "'NaN'" => Some(f64::NAN),
                    _ => return Err("unexpected recorded float boundary literal".into()),
                };
                ids.push(id);
                values.push(value);
                source_rows.push(format!("({id},{expression})"));
            }
            let source_clause = format!(
                " FROM VALUES {} AS t(id,k) ORDER BY id",
                source_rows.join(",")
            );
            let source_sql = reference["sql"].as_str().ok_or("missing Spark SQL")?;
            let select = source_sql
                .strip_suffix(&source_clause)
                .ok_or("recorded VALUES clause changed")?;
            let window_tail = select
                .strip_prefix("SELECT id,COUNT(*) OVER (ORDER BY k ")
                .ok_or("unexpected recorded window key expression")?;
            let value_type = if kind == "FLOAT" {
                DataType::Float32
            } else {
                DataType::Float64
            };
            let primitive_values: ArrayRef = if kind == "FLOAT" {
                Arc::new(Float32Array::from(
                    values
                        .iter()
                        .map(|value| value.map(|value| value as f32))
                        .collect::<Vec<_>>(),
                ))
            } else {
                Arc::new(Float64Array::from(values.clone()))
            };
            // Each source has one logical NULL. Exercise both Arrow dictionary validity forms.
            let null_key = id.contains("infinity");
            let keys = values
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    if null_key && value.is_none() {
                        Ok(None)
                    } else {
                        Ok(Some(i32::try_from(index)?))
                    }
                })
                .collect::<Result<Vec<_>, std::num::TryFromIntError>>()?;
            let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(keys),
                primitive_values.clone(),
            )?);
            assert_eq!(
                arrow::compute::cast(dictionary.as_ref(), &value_type)?.to_data(),
                primitive_values.to_data()
            );
            let ids = Arc::new(Int32Array::from(ids));
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("k", dictionary.data_type().clone(), true),
            ]));
            let input = RecordBatch::try_new(schema.clone(), vec![ids.clone(), dictionary])?;
            assert_eq!(input.column(0).to_data(), ids.to_data());
            let mut partitions = vec![Vec::new(); 2];
            for offset in (0..input.num_rows()).step_by(batch_size) {
                partitions[(offset / batch_size) % 2]
                    .push(input.slice(offset, batch_size.min(input.num_rows() - offset)));
            }
            let table = format!("dictionary_{id}_{ansi_mode}");
            session.register_table(&table, Arc::new(MemTable::try_new(schema, partitions)?))?;
            let encoded_sql = format!("{select} FROM {table} ORDER BY id");
            let decoded_sql = format!(
                "SELECT id,COUNT(*) OVER (ORDER BY CAST(k AS {kind}) {window_tail} FROM {table} ORDER BY id"
            );
            for (decoded, sql) in [(false, encoded_sql), (true, decoded_sql.clone())] {
                let query = plan_query(
                    &session,
                    &sql,
                    &SparkSqlOptions {
                        ansi_mode,
                        ..Default::default()
                    },
                )
                .await?;
                let expected_policy = RangeOffsetArithmetic::Spark { ansi_mode };
                assert_eq!(
                    find_single_window_frame(&query.logical_plan)?.range_offset_arithmetic(),
                    expected_policy
                );
                let mut state = session.state();
                let extensions = &mut state.config_mut().options_mut().extensions;
                extensions.insert(SparkSqlExpressionSemantics::default());
                extensions.insert(PreserveAggregateStatisticsSchema::default());
                let analyzed = state.analyzer().execute_and_check(
                    query.logical_plan,
                    state.config_options(),
                    |_, _| {},
                )?;
                assert_eq!(
                    find_single_window_frame(&analyzed)?.range_offset_arithmetic(),
                    expected_policy
                );

                // Change only the native input binding; both queries retain the same Spark expectation.
                let mut native_case = reference.clone();
                native_case["sql"] = json!(sql);
                let observation =
                    compare_exact_spark_query_stages_with_optimized(&session, &native_case)
                        .await
                        .unwrap_or_else(|error| json!({"observer_error":error.to_string()}));
                let matches = native_result_matches_spark_reference(&observation, reference);
                if !completed.insert((case_identity(reference)?, batch_size, decoded)) || !matches {
                    differences.push(format!(
                        "{id} ANSI={ansi_mode} batch={batch_size} decoded={decoded}"
                    ));
                }
                eprintln!(
                    "DICTIONARY_NULL_RANGE_OBSERVATION {}",
                    json!({
                        "id":id,"ansi":ansi_mode,"batch":batch_size,"decoded":decoded,"null_dictionary_key":null_key,
                        "native_sql":native_case["sql"],"equivalent_decoded_sql":decoded_sql,
                        "reference":reference,"observation":observation,"spark_matches":matches
                    })
                );
            }
        }
    }
    assert_eq!(completed.len(), 32);
    assert!(
        differences.is_empty(),
        "dictionary NULL RANGE differences:\n{}",
        differences.join("\n")
    );
    Ok(())
}

#[tokio::test]
async fn null_range_frames_match_spark_with_explicit_typed_rows_difference() -> TestResult {
    let (request, records) = parse_null_range_fixture()?;
    let rows_controls: Vec<Value> = serde_json::from_str(PRIOR_ROWS_REJECTIONS)?;
    validate_prior_rows_rejections(&records, &rows_controls)?;
    let mut completed = BTreeSet::new();
    let mut strict_contexts = 0;
    let mut typed_rows_contexts = 0;
    let mut differences = Vec::new();
    for corpus in request["corpora"].as_array().ok_or("missing corpora")? {
        for batch in [1, 32] {
            let session = SessionContext::new_with_state(
                configure_spark_sql(
                    SessionStateBuilder::new()
                        .with_default_features()
                        .with_config(
                            SessionConfig::new()
                                .with_batch_size(batch)
                                .with_target_partitions(2),
                        ),
                )
                .build(),
            );
            let provider_error =
                spark_inputs::register_recorded_inputs(&session, &corpus["metadata"], batch)
                    .err()
                    .map(|error| error.to_string());
            for case in records[1..]
                .iter()
                .filter(|case| case["corpus"] == corpus["corpus"])
            {
                let identity = (case_identity(case)?, batch);
                let observation = match &provider_error {
                    Some(error) => {
                        json!({"observer_error":error,"observer_phase":"register_inputs"})
                    }
                    None => compare_exact_spark_query_stages_with_optimized(&session, case)
                        .await
                        .unwrap_or_else(|error| json!({"observer_error":error.to_string()})),
                };
                let matches = native_result_matches_spark_reference(&observation, case);
                // Validated source identities cannot abort observation collection on a query difference.
                let unique = completed.insert(identity);
                let rows_case = is_original_null_rows_case(case);
                let typed_rows =
                    rows_case && null_rows::unsupported_null_bound_location(case).is_some();
                let matches_prior_rejection = rows_case
                    && prior_rows_rejection_matches(case, batch, &observation, &rows_controls);
                let exact_rows_rejection =
                    !rows_case || null_rows::matches_contract(case, &observation)?;
                let contract_matches = if typed_rows {
                    typed_rows_contexts += 1;
                    !matches && exact_rows_rejection
                } else {
                    strict_contexts += 1;
                    matches && exact_rows_rejection
                };
                if !contract_matches || !unique {
                    differences.push(format!(
                        "{}:{} ANSI={} batch={batch}",
                        case["corpus"], case["id"], case["ansi"]
                    ));
                }
                let record = json!({"issue":754,"corpus":case["corpus"],"id":case["id"],
                    "ansi":case["ansi"],"batch":batch,"target_partitions":2,"provider_partitions":2,
                    "source_ownership_tag":case["ownership"],"primary":!typed_rows,
                    "intentional_typed_null_difference":typed_rows,
                    "prior_rejection_matches":if rows_case { Some(matches_prior_rejection) } else { None },
                    "reference":case,"observation":observation,"spark_matches":matches});
                eprintln!("NULL_RANGE_OBSERVATION {record}");
            }
        }
    }
    eprintln!(
        "NULL_RANGE_SUMMARY {}",
        json!({"observations":completed.len(),
        "strict_spark_contexts":strict_contexts,"intentional_typed_rows_contexts":typed_rows_contexts,
        "differences":differences.len(),"expected_observations":1164})
    );
    assert_eq!(completed.len(), 1164);
    assert_eq!(strict_contexts, 1156);
    assert_eq!(typed_rows_contexts, 8);
    assert!(
        differences.is_empty(),
        "{} NULL RANGE or NULL ROWS admission differences:\n{}",
        differences.len(),
        differences.join("\n")
    );
    Ok(())
}

#[test]
fn prior_rows_controls_reject_inventory_and_observation_changes() -> TestResult {
    let (_, records) = parse_null_range_fixture()?;
    let controls: Vec<Value> = serde_json::from_str(PRIOR_ROWS_REJECTIONS)?;
    validate_prior_rows_rejections(&records, &controls)?;
    let mut missing = controls.clone();
    missing.pop();
    assert!(validate_prior_rows_rejections(&records, &missing).is_err());
    let mut duplicate = controls.clone();
    duplicate[1] = duplicate[0].clone();
    assert!(validate_prior_rows_rejections(&records, &duplicate).is_err());

    let control = &controls[0];
    let batch: usize = control["batch"]
        .as_u64()
        .ok_or("missing control batch")?
        .try_into()?;
    let observation = &control["observation"];
    assert!(prior_rows_rejection_matches(
        control,
        batch,
        observation,
        &controls
    ));
    for (field, value) in [
        ("native_phase", json!("collect")),
        ("error", json!("another rejection cause")),
        ("optimized_error", json!("another optimizer cause")),
        ("public_schema", json!({"type":"struct","fields":[]})),
        ("rows", json!([])),
        ("rows_raw", json!([])),
    ] {
        let mut changed = observation.clone();
        changed[field] = value;
        assert!(
            !prior_rows_rejection_matches(control, batch, &changed, &controls),
            "accepted changed {field}"
        );
        let mut changed_controls = controls.clone();
        changed_controls[0]["observation"] = changed;
        assert!(validate_prior_rows_rejections(&records, &changed_controls).is_err());
    }
    let mut missing_field = observation.clone();
    missing_field
        .as_object_mut()
        .ok_or("missing control observation")?
        .remove("physical_schema");
    assert!(!prior_rows_rejection_matches(
        control,
        batch,
        &missing_field,
        &controls
    ));
    let mut changed_controls = controls.clone();
    changed_controls[0]["observation"] = missing_field;
    assert!(validate_prior_rows_rejections(&records, &changed_controls).is_err());
    let mut success = observation.clone();
    success["error"] = Value::Null;
    success["optimized_error"] = Value::Null;
    success["native_phase"] = json!("encode");
    success["rows"] = json!([]);
    assert!(!prior_rows_rejection_matches(
        control, batch, &success, &controls
    ));
    Ok(())
}

#[test]
fn null_range_references_reject_contract_loss() -> TestResult {
    let (request, records) = parse_null_range_fixture()?;
    let success = records
        .iter()
        .position(|case| case["corpus"] == "null-range-754-supplement" && case["status"] == "ok")
        .ok_or("missing successful supplemental reference")?;
    for field in [
        "id",
        "schema_stages",
        "schema_stage_availability",
        "rows_raw",
    ] {
        let mut changed = records.clone();
        changed[success]
            .as_object_mut()
            .ok_or("missing record")?
            .remove(field);
        assert!(
            validate_null_range_references(&request, &changed).is_err(),
            "accepted missing {field}"
        );
    }
    let mut missing = records.clone();
    missing.pop();
    assert!(validate_null_range_references(&request, &missing).is_err());
    let mut duplicate = records.clone();
    duplicate[2] = duplicate[1].clone();
    assert!(validate_null_range_references(&request, &duplicate).is_err());
    for pointer in [
        "/schema_stages/public/fields/0/metadata",
        "/schema_stages/optimized",
        "/expected/rows",
    ] {
        let mut changed = records.clone();
        *changed[success]
            .pointer_mut(pointer)
            .ok_or("missing schema/row fault target")? = Value::Null;
        assert!(
            validate_null_range_references(&request, &changed).is_err(),
            "accepted lost {pointer}"
        );
    }
    let ordered = records
        .iter()
        .position(|case| {
            case["ordered"] == true
                && case["rows_raw"]
                    .as_array()
                    .is_some_and(|rows| rows.len() > 1)
        })
        .ok_or("missing ordered reference")?;
    let mut changed = records.clone();
    changed[ordered]["expected"]["rows"]
        .as_array_mut()
        .ok_or("missing ordered rows")?
        .reverse();
    assert!(validate_null_range_references(&request, &changed).is_err());
    let mut changed = records.clone();
    changed[ordered]["ordered"] = json!(false);
    assert!(validate_null_range_references(&request, &changed).is_err());
    let mut changed_source = request.clone();
    changed_source["corpora"][0]["cases"][0]["sql"] = json!("changed original SQL");
    assert!(validate_null_range_references(&changed_source, &records).is_err());
    let mut changed = records.clone();
    changed[0]["corpora"][0]["registered_inputs"][0]["schema_observed"] = Value::Null;
    assert!(validate_null_range_references(&request, &changed).is_err());
    Ok(())
}

#[test]
fn null_range_collect_errors_keep_partial_fields_and_causes() -> TestResult {
    let (request, records) = parse_null_range_fixture()?;
    let collect_error = records
        .iter()
        .position(|case| case["status"] == "error" && case["spark_error_phase"] == "collect")
        .ok_or("missing collect error")?;
    for pointer in [
        "/schema_stages/public/fields/0/metadata",
        "/schema_stages/analyzed/fields/0/metadata",
        "/schema_stages/optimized/fields/0/metadata",
        "/schema_stages/executed_before_collect/fields/0/metadata",
        "/schema_stages/executed/fields/0/metadata",
        "/expected/condition",
        "/spark_error_phase",
        "/spark_error_details/cause_chain_complete",
    ] {
        let mut changed = records.clone();
        *changed[collect_error]
            .pointer_mut(pointer)
            .ok_or("missing collect fault target")? = Value::Null;
        assert!(
            validate_null_range_references(&request, &changed).is_err(),
            "accepted lost {pointer}"
        );
    }
    let mut changed = records.clone();
    changed[collect_error]["spark_error_details"]["java_causes"] = json!([]);
    assert!(validate_null_range_references(&request, &changed).is_err());
    Ok(())
}

#[test]
fn null_offset_unparser_keeps_typed_expressions_and_native_unbounded_bounds() -> TestResult {
    use arrow::datatypes::DataType;
    use datafusion::common::ScalarValue;
    use datafusion::functions_aggregate::count::count_udaf;
    use datafusion::logical_expr::expr::WindowFunction;
    use datafusion::logical_expr::{Expr, WindowFrame, WindowFrameBound, WindowFrameUnits, col};
    use datafusion::sql::{sqlparser::ast, unparser::expr_to_sql};

    for (data_type, sql_type) in [
        (DataType::Int32, ast::DataType::Integer(None)),
        (
            DataType::Float64,
            ast::DataType::Double(ast::ExactNumberInfo::None),
        ),
        (
            DataType::Decimal128(9, 2),
            ast::DataType::Decimal(ast::ExactNumberInfo::PrecisionAndScale(9, 2)),
        ),
        (
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Float64)),
            ast::DataType::Double(ast::ExactNumberInfo::None),
        ),
    ] {
        for explicit in [false, true] {
            let (start, end) = if explicit {
                (
                    WindowFrameBound::PrecedingNullOffset(data_type.clone()),
                    WindowFrameBound::FollowingNullOffset(data_type.clone()),
                )
            } else {
                let value = ScalarValue::try_from(&data_type)?;
                (
                    WindowFrameBound::Preceding(value.clone()),
                    WindowFrameBound::Following(value),
                )
            };
            let mut call = WindowFunction::new(count_udaf(), vec![col("v")]);
            call.params.order_by = vec![col("k").sort(true, true)];
            call.params.window_frame = WindowFrame::new_bounds(WindowFrameUnits::Range, start, end);
            let ast::Expr::Function(function) = expr_to_sql(&Expr::from(call))? else {
                return Err("missing unparsed window function".into());
            };
            let Some(ast::WindowType::WindowSpec(window)) = function.over else {
                return Err("missing unparsed window specification".into());
            };
            let frame = window.window_frame.ok_or("missing unparsed frame")?;
            for (bound, preceding) in [
                (frame.start_bound, true),
                (frame.end_bound.ok_or("missing end bound")?, false),
            ] {
                let expression = match bound {
                    ast::WindowFrameBound::Preceding(expression) if preceding => expression,
                    ast::WindowFrameBound::Following(expression) if !preceding => expression,
                    _ => return Err("unparser changed frame direction".into()),
                };
                if explicit {
                    let Some(ast::Expr::Cast {
                        expr, data_type, ..
                    }) = expression.as_deref()
                    else {
                        return Err("explicit NULL offset lost its typed expression".into());
                    };
                    assert_eq!(expr.to_string(), "NULL");
                    assert_eq!(*data_type, sql_type);
                } else {
                    assert!(
                        expression.is_none(),
                        "native unbounded sentinel became an expression"
                    );
                }
            }
        }
    }
    Ok(())
}

#[test]
fn null_offset_unparser_rejects_unsupported_payload_types() -> TestResult {
    use arrow::datatypes::DataType;
    use datafusion::functions_aggregate::count::count_udaf;
    use datafusion::logical_expr::expr::WindowFunction;
    use datafusion::logical_expr::{Expr, WindowFrame, WindowFrameBound, WindowFrameUnits, col};
    use datafusion::sql::unparser::expr_to_sql;

    let mut call = WindowFunction::new(count_udaf(), vec![col("v")]);
    call.params.order_by = vec![col("k").sort(true, true)];
    call.params.window_frame = WindowFrame::new_bounds(
        WindowFrameUnits::Range,
        WindowFrameBound::PrecedingNullOffset(DataType::BinaryView),
        WindowFrameBound::CurrentRow,
    );
    assert!(expr_to_sql(&Expr::from(call)).is_err_and(|error| {
        error
            .to_string()
            .contains("Unsupported DataType: conversion: BinaryView")
    }));
    Ok(())
}

#[tokio::test]
async fn date_null_interval_bounds_keep_their_offset_types() -> TestResult {
    use arrow::datatypes::{DataType, IntervalUnit};
    use datafusion::logical_expr::WindowFrameBound;
    use datafusion::logical_expr::window_frame::RangeOffsetArithmetic;
    use datafusion::physical_optimizer::aggregate_statistics::PreserveAggregateStatisticsSchema;
    use delta_arrow_reader_spark_sql::{SparkSqlOptions, plan_query};
    use sail_plan::expression_errors::SparkSqlExpressionSemantics;

    let (request, _) = parse_null_range_fixture()?;
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    spark_inputs::register_recorded_inputs(&session, &request["corpora"][2]["metadata"], 1)?;
    for ansi_mode in [true, false] {
        let options = SparkSqlOptions {
            ansi_mode,
            ..SparkSqlOptions::default()
        };
        let sql = |qualifier: &str, upper: &str| {
            format!(
                "SELECT COUNT(*) OVER (ORDER BY DATE_ADD(DATE '2020-01-01',k) \
                RANGE BETWEEN CAST(NULL AS INTERVAL {qualifier}) PRECEDING AND {upper}) AS n \
                FROM null_range_no_null_keys"
            )
        };
        let analyze = |plan: &datafusion::logical_expr::LogicalPlan| -> TestResult<_> {
            let mut state = session.state();
            let extensions = &mut state.config_mut().options_mut().extensions;
            extensions.insert(SparkSqlExpressionSemantics::default());
            extensions.insert(PreserveAggregateStatisticsSchema::default());
            let analyzed = state.analyzer().execute_and_check(
                plan.clone(),
                state.config_options(),
                |_, _| {},
            )?;
            find_single_window_frame(&analyzed)
        };

        // A NULL YEAR interval keeps its temporal representation and coercion, an unbounded
        // sentinel; as a FOLLOWING bound its frame reaches the partition end where Spark's frame
        // is empty (#1236).
        let query = plan_query(&session, &sql("YEAR", "CURRENT ROW"), &options).await?;
        for frame in [
            find_single_window_frame(&query.logical_plan)?,
            analyze(&query.logical_plan)?,
        ] {
            let WindowFrameBound::Preceding(value) = &frame.start_bound else {
                return Err("DATE YEAR interval NULL was retagged as a numeric offset".into());
            };
            assert!(value.is_null());
            assert_eq!(
                value.data_type(),
                DataType::Interval(IntervalUnit::YearMonth)
            );
            assert!(frame.start_bound.is_unbounded());
            assert!(!frame.has_null_offset());
            assert_eq!(
                frame.range_offset_arithmetic(),
                RangeOffsetArithmetic::DataFusion
            );
        }

        // Beside a DAY offset, a YEAR offset is a bound of another type, which Spark rejects.
        let error = plan_query(
            &session,
            &sql("YEAR", "INTERVAL '1' DAY FOLLOWING"),
            &options,
        )
        .await
        .err()
        .ok_or("a YEAR and a DAY bound must not plan")?;
        assert!(
            error
                .to_string()
                .contains("SPECIFIED_WINDOW_FRAME_DIFF_TYPES"),
            "{error}"
        );

        // A NULL DAY interval is a NULL INT day offset over a DATE key (#947), as Spark adds
        // a DAY interval to a DATE as its day count.
        for upper in ["CURRENT ROW", "INTERVAL '1' DAY FOLLOWING"] {
            let query = plan_query(&session, &sql("DAY", upper), &options).await?;
            for frame in [
                find_single_window_frame(&query.logical_plan)?,
                analyze(&query.logical_plan)?,
            ] {
                assert_eq!(
                    frame.start_bound,
                    WindowFrameBound::PrecedingNullOffset(DataType::Int32)
                );
                assert!(frame.has_null_offset());
                assert_eq!(
                    frame.range_offset_arithmetic(),
                    RangeOffsetArithmetic::Spark { ansi_mode }
                );
            }
        }
    }
    Ok(())
}

fn find_single_window_frame(
    plan: &datafusion::logical_expr::LogicalPlan,
) -> TestResult<datafusion::logical_expr::WindowFrame> {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::logical_expr::Expr;

    let mut frames = Vec::new();
    plan.apply(|node| {
        node.apply_expressions(|expression| {
            expression.apply(|expression| {
                if let Expr::WindowFunction(function) = expression {
                    frames.push(function.params.window_frame.clone());
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    if frames.len() != 1 {
        return Err("expected one window frame".into());
    }
    frames.pop().ok_or_else(|| "missing window frame".into())
}

#[tokio::test]
async fn null_range_producer_arithmetic_survives_analyzer_coercion() -> TestResult {
    use datafusion::logical_expr::WindowFrameBound;
    use datafusion::logical_expr::window_frame::RangeOffsetArithmetic;
    use datafusion::physical_optimizer::aggregate_statistics::PreserveAggregateStatisticsSchema;
    use delta_arrow_reader_spark_sql::{SparkSqlOptions, plan_query};
    use sail_plan::expression_errors::SparkSqlExpressionSemantics;

    let (request, _) = parse_null_range_fixture()?;
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    spark_inputs::register_recorded_inputs(&session, &request["corpora"][2]["metadata"], 1)?;
    for ansi_mode in [true, false] {
        for (key, offset_type, null_offset, uses_spark) in [
            ("CAST(k AS TINYINT)", "TINYINT", true, true),
            ("CAST(k AS SMALLINT)", "SMALLINT", true, true),
            ("k", "INT", true, true),
            ("CAST(k AS BIGINT)", "BIGINT", true, true),
            ("DATE_ADD(DATE '2020-01-01',k)", "INT", true, true),
            ("CAST(k AS FLOAT)", "FLOAT", true, true),
            ("CAST(k AS DOUBLE)", "DOUBLE", true, true),
            ("CAST(k AS DECIMAL(12,2))", "DECIMAL(12,2)", true, true),
            ("CAST(k AS FLOAT)", "FLOAT", false, false),
            ("CAST(k AS DOUBLE)", "DOUBLE", false, false),
            ("CAST(k AS DECIMAL(12,2))", "DECIMAL(12,2)", false, false),
            ("CAST(k AS TINYINT)", "TINYINT", false, true),
            ("CAST(k AS SMALLINT)", "SMALLINT", false, true),
            ("k", "INT", false, true),
            ("CAST(k AS BIGINT)", "BIGINT", false, true),
            ("DATE_ADD(DATE '2020-01-01',k)", "INT", false, false),
        ] {
            let lower = if null_offset { "NULL" } else { "1" };
            let sql = format!(
                "SELECT COUNT(*) OVER (ORDER BY {key} \
                RANGE BETWEEN CAST({lower} AS {offset_type}) PRECEDING \
                AND CAST(2 AS {offset_type}) FOLLOWING) AS n FROM null_range_no_null_keys"
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
            let resolved = find_single_window_frame(&query.logical_plan)?;
            let expected = if uses_spark {
                RangeOffsetArithmetic::Spark { ansi_mode }
            } else {
                RangeOffsetArithmetic::DataFusion
            };
            assert_eq!(resolved.range_offset_arithmetic(), expected, "{sql}");
            let mut state = session.state();
            let extensions = &mut state.config_mut().options_mut().extensions;
            extensions.insert(SparkSqlExpressionSemantics::default());
            extensions.insert(PreserveAggregateStatisticsSchema::default());
            let analyzed = state.analyzer().execute_and_check(
                query.logical_plan,
                state.config_options(),
                |_, _| {},
            )?;
            assert_eq!(
                find_single_window_frame(&analyzed)?.range_offset_arithmetic(),
                expected,
                "{sql}"
            );
        }
        for frame in [
            "RANGE BETWEEN CAST(0 AS INT) PRECEDING AND CAST(0 AS INT) FOLLOWING",
            "RANGE BETWEEN 0 PRECEDING AND CURRENT ROW",
            "RANGE 0 PRECEDING",
            "RANGE BETWEEN 0 FOLLOWING AND CURRENT ROW",
            "RANGE BETWEEN CURRENT ROW AND CURRENT ROW",
            "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
        ] {
            let sql = format!(
                "SELECT COUNT(*) OVER (ORDER BY k {frame}) AS n FROM null_range_no_null_keys"
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
            let resolved = find_single_window_frame(&query.logical_plan)?;
            assert_eq!(
                resolved.range_offset_arithmetic(),
                RangeOffsetArithmetic::DataFusion,
                "{sql}"
            );
            if !frame.contains("UNBOUNDED") {
                assert_eq!(resolved.start_bound, WindowFrameBound::CurrentRow, "{sql}");
                assert_eq!(resolved.end_bound, WindowFrameBound::CurrentRow, "{sql}");
            }
            let mut state = session.state();
            let extensions = &mut state.config_mut().options_mut().extensions;
            extensions.insert(SparkSqlExpressionSemantics::default());
            extensions.insert(PreserveAggregateStatisticsSchema::default());
            let analyzed = state.analyzer().execute_and_check(
                query.logical_plan,
                state.config_options(),
                |_, _| {},
            )?;
            let analyzed = find_single_window_frame(&analyzed)?;
            assert_eq!(
                analyzed.range_offset_arithmetic(),
                RangeOffsetArithmetic::DataFusion,
                "{sql}"
            );
            if !frame.contains("UNBOUNDED") {
                assert_eq!(analyzed.start_bound, WindowFrameBound::CurrentRow, "{sql}");
                assert_eq!(analyzed.end_bound, WindowFrameBound::CurrentRow, "{sql}");
            }
        }
    }
    Ok(())
}
