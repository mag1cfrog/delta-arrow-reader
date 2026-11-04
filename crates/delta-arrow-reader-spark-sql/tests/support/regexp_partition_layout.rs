//! Exact retained error receipts and independently observed provider-layout comparisons.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{
    ExecutionPlanProperties, collect, collect_partitioned, displayable,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

use super::string_binary::{TestResult, capture_observation, register_inputs};

fn receipt_key(case: &Value, representation: usize, batch_size: usize) -> Value {
    json!({"family":"fresh","fixture":"regexp-replace-nonboundary-spark.jsonl",
        "id":case["id"],"ansi":case["ansi"],"representation":representation,"batch_size":batch_size})
}

fn receipt_matches(
    receipt: &Value,
    case: &Value,
    representation: usize,
    batch_size: usize,
    actual: &Value,
) -> bool {
    receipt["key"] == receipt_key(case, representation, batch_size)
        && receipt["sql"] == case["sql"]
        && receipt["spark_expected"] == case["expected"]
        && receipt["native_actual"] == *actual
        && receipt["matches_reference"] == false
        && receipt["full_compatibility_pass"] == false
}

fn negative_receipts() -> TestResult<Vec<Value>> {
    let receipts = include_str!("../fixtures/regexp-replace-partition-layout-negative.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(receipts.len(), 6);
    let mut keys = HashSet::new();
    for receipt in &receipts {
        let key = &receipt["key"];
        assert_eq!(key["family"], "fresh");
        assert_eq!(key["fixture"], "regexp-replace-nonboundary-spark.jsonl");
        assert_eq!(key["id"], "nonboundary_probe_column_literal_pattern");
        assert_eq!(key["batch_size"], 1);
        assert!(key["ansi"].as_bool().is_some());
        assert!(
            key["representation"]
                .as_u64()
                .is_some_and(|value| value < 3)
        );
        assert!(keys.insert(key.to_string()));
        assert!(receipt["native_actual"].get("rows").is_none());
        assert_eq!(receipt["native_actual"]["error_stage"], "execute");
        assert_eq!(receipt["matches_reference"], false);
        assert_eq!(receipt["full_compatibility_pass"], false);
    }
    Ok(receipts)
}

pub(super) fn known_partition_layout_error(
    case: &Value,
    representation: usize,
    batch_size: usize,
    actual: &Value,
) -> TestResult<bool> {
    if case["id"] != "nonboundary_probe_column_literal_pattern" || batch_size != 1 {
        return Ok(false);
    }
    let matched = negative_receipts()?
        .iter()
        .any(|receipt| receipt_matches(receipt, case, representation, batch_size, actual));
    if matched {
        eprintln!(
            "c06 retained partition layout difference: {}",
            json!({
            "key":receipt_key(case,representation,batch_size),"sql":case["sql"],
            "spark_expected":case["expected"],"actual":actual,
            "matches_reference":false,"full_compatibility_pass":false})
        );
    }
    Ok(matched)
}

fn matched_batches(layout: &str) -> Option<&'static [usize]> {
    match layout {
        "original_registration" => Some(&[1, 32]),
        "native_batch_1" => Some(&[1]),
        "native_batch_32" => Some(&[32]),
        _ => None,
    }
}

fn matched_key(reference: &Value, representation: usize, batch_size: usize) -> Option<Value> {
    let layout = reference["layout"].as_str()?;
    if representation >= 3 || !matched_batches(layout)?.contains(&batch_size) {
        return None;
    }
    let sql = match reference["id"].as_str()? {
        "nonboundary_probe_column_literal_pattern" => {
            r"SELECT k,regexp_replace(s,r'\B',replacement) AS r FROM rr_nonboundary ORDER BY k"
        }
        "nonboundary_probe_column_literal_pattern_isolated_k_6" => {
            r"SELECT k,regexp_replace(s,r'\B',replacement) AS r FROM rr_nonboundary WHERE k = 6 ORDER BY k"
        }
        "nonboundary_probe_column_literal_pattern_isolated_k_9" => {
            r"SELECT k,regexp_replace(s,r'\B',replacement) AS r FROM rr_nonboundary WHERE k = 9 ORDER BY k"
        }
        _ => return None,
    };
    if reference["sql"] != sql || reference["ansi"].as_bool().is_none() {
        return None;
    }
    Some(
        json!({"layout":layout,"id":reference["id"],"ansi":reference["ansi"],
        "representation":representation,"batch_size":batch_size}),
    )
}

fn partition_identity(providers: &Value) -> Option<Value> {
    Some(
        json!(providers.as_array()?.iter().map(|provider| Some(json!({
        "name":provider.get("name")?,"schema":provider.get("schema")?,
        "actual_partition_count":provider.get("actual_partition_count")?,
        "partitions":provider.get("partitions")?.as_array()?.iter().map(|part| Some(json!({
            "partition":part.get("partition")?,"keys":part.get("keys")?,"rows":part.get("rows")?
        }))).collect::<Option<Vec<_>>>()?
    }))).collect::<Option<Vec<_>>>()?),
    )
}

fn matched_error_matches(reference: &Value, actual: &Value) -> bool {
    let expected = &reference["actual"];
    let Some(parameters) = expected["parameters"].as_object() else {
        return false;
    };
    let Some(causes) = expected["java_causes"].as_array() else {
        return false;
    };
    if parameters.len() != 4
        || causes.len() != 2
        || expected["status"] != "error"
        || expected["error_stage"] != "collect"
        || expected["phase"] != "collect"
        || expected["physical_schema_stage"] != "executedPlan before collect"
        || expected["condition"] != "INVALID_REGEXP_REPLACE"
        || causes[0]["class"] != "org.apache.spark.SparkRuntimeException"
        || causes[0]["condition"] != expected["condition"]
        || causes[0]["parameters"] != expected["parameters"]
        || expected["dataframe_schema"].is_null()
        || expected["analyzed_schema"].is_null()
        || expected["physical_schema"].is_null()
        || actual["error_stage"] != "execute"
        || actual.get("rows").is_some()
        || expected.get("rows").is_some()
        || actual["analyzed_schema"] != expected["analyzed_schema"]
        || actual["analyzed_schema"] != expected["dataframe_schema"]
        || actual["physical_schema"] != expected["physical_schema"]
    {
        return false;
    }
    let values = ["source", "pattern", "replacement", "position"]
        .map(|key| expected["parameters"][key].as_str());
    let [
        Some(source),
        Some(pattern),
        Some(replacement),
        Some(position),
    ] = values
    else {
        return false;
    };
    let (Some(class), Some(message)) = (causes[1]["class"].as_str(), causes[1]["message"].as_str())
    else {
        return false;
    };
    actual["error"]
        == format!(
            "execute: Execution error: [INVALID_REGEXP_REPLACE] source={source:?}, pattern={pattern:?}, replacement={replacement:?}, position={position}: Execution error: {class}: {message}"
        )
}

fn layout_shape_matches(reference: &Value) -> bool {
    let Some(providers) = reference["actual_registered_inputs"].as_array() else {
        return false;
    };
    let Some(provider) = providers
        .iter()
        .find(|provider| provider["name"] == "rr_nonboundary")
    else {
        return false;
    };
    let Some(parts) = provider["partitions"].as_array() else {
        return false;
    };
    match reference["layout"].as_str() {
        Some("original_registration") => {
            provider["actual_partition_count"] == 1 && parts.len() == 1
        }
        Some("native_batch_1") => {
            provider["actual_partition_count"] == 2
                && parts.len() == 2
                && parts.iter().enumerate().all(|(partition, part)| {
                    part["keys"].as_array().is_some_and(|keys| {
                        keys.len() == 7
                            && keys
                                .iter()
                                .enumerate()
                                .all(|(index, key)| key == &(index * 2 + partition + 1).to_string())
                    })
                })
        }
        Some("native_batch_32") => {
            provider["actual_partition_count"] == 2
                && parts.len() == 2
                && parts[0]["rows"]
                    .as_array()
                    .is_some_and(|rows| rows.len() == 14)
                && parts[1]["rows"] == json!([])
        }
        _ => false,
    }
}

fn matched_result_matches(
    reference: &Value,
    representation: usize,
    batch_size: usize,
    providers: &Value,
    actual: &Value,
) -> bool {
    matched_key(reference, representation, batch_size).is_some()
        && layout_shape_matches(reference)
        && partition_identity(providers).is_some()
        && partition_identity(providers)
            == partition_identity(&reference["actual_registered_inputs"])
        && matched_error_matches(reference, actual)
}

fn rows(batches: &[RecordBatch]) -> TestResult<Value> {
    Ok(json!(
        batches
            .iter()
            .map(|batch| (0..batch.num_rows())
                .map(|row| {
                    batch
                        .columns()
                        .iter()
                        .map(|column| {
                            if column
                                .logical_nulls()
                                .as_ref()
                                .is_some_and(|nulls| nulls.is_null(row))
                            {
                                Ok(Value::Null)
                            } else {
                                Ok(json!(array_value_to_string(column.as_ref(), row)?))
                            }
                        })
                        .collect::<TestResult<Vec<_>>>()
                })
                .collect::<TestResult<Vec<_>>>())
            .collect::<TestResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
    ))
}

fn input_schema(schema: &Schema) -> TestResult<Value> {
    Ok(
        json!({"type":"struct","fields":schema.fields().iter().map(|field| {
        let data_type = match field.data_type() {
            DataType::Int32 => "integer",
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string",
            other => return Err(format!("unexpected partition-control input type: {other}").into()),
        };
        Ok(json!({"name":field.name(),"type":data_type,"nullable":field.is_nullable(),"metadata":field.metadata()}))
    }).collect::<TestResult<Vec<_>>>()?}),
    )
}

async fn provider_layout(session: &SessionContext, table: &str) -> TestResult<Value> {
    let provider = session.table_provider(table).await?;
    let state = session.state();
    let scan = provider.scan(&state, None, &[], None).await?;
    let count = scan.output_partitioning().partition_count();
    let schema = format!("{:?}", scan.schema());
    let plan = displayable(scan.as_ref()).indent(true).to_string();
    let partitions = collect_partitioned(scan, session.task_ctx()).await?;
    let actual = partitions
        .iter()
        .enumerate()
        .map(|(index, batches)| {
            let values = rows(batches)?;
            let keys = values
                .as_array()
                .ok_or("missing provider rows")?
                .iter()
                .map(|row| row[0].clone())
                .collect::<Vec<_>>();
            Ok(json!({"partition":index,"keys":keys,"rows":values,
            "batch_rows":batches.iter().map(RecordBatch::num_rows).collect::<Vec<_>>()}))
        })
        .collect::<TestResult<Vec<_>>>()?;
    Ok(
        json!({"name":table,"schema":input_schema(provider.schema().as_ref())?,
        "provider_arrow_schema":format!("{:?}",provider.schema()),
        "scan_arrow_schema":schema,"scan_plan":plan,"actual_partition_count":count,"partitions":actual}),
    )
}

async fn materialize_original_partition(
    session: &SessionContext,
    table: &str,
) -> TestResult<Value> {
    let provider = session.table_provider(table).await?;
    let schema = provider.schema();
    let sql = format!("SELECT * FROM {table} ORDER BY k");
    let frame = session.sql(&sql).await?;
    let logical_plan = frame.logical_plan().display_indent_schema().to_string();
    let logical_schema = format!("{:?}", frame.logical_plan().schema());
    let physical = frame.create_physical_plan().await?;
    let physical_plan = displayable(physical.as_ref()).indent(true).to_string();
    let physical_schema = format!("{:?}", physical.schema());
    assert!(
        !logical_plan.contains("regexp") && !physical_plan.contains("regexp"),
        "source materialization evaluated a regex expression"
    );
    let materialized = collect(physical, session.task_ctx()).await?;
    let mut batches = Vec::new();
    for batch in &materialized {
        assert_eq!(
            batch.schema(),
            schema,
            "materialization changed the actual source schema"
        );
        batches.push(RecordBatch::try_new(
            Arc::clone(&schema),
            batch.columns().to_vec(),
        )?);
    }
    session.deregister_table(table)?;
    session.register_table(
        table,
        Arc::new(MemTable::try_new(Arc::clone(&schema), vec![batches])?),
    )?;
    let registered = session.table_provider(table).await?.schema();
    let same_schema_arc = Arc::ptr_eq(&schema, &registered);
    let same_field_arcs = schema
        .fields()
        .iter()
        .zip(registered.fields())
        .all(|(before, after)| Arc::ptr_eq(before, after));
    assert!(
        same_schema_arc && same_field_arcs,
        "source schema or field Arcs changed"
    );
    Ok(
        json!({"table":table,"materialization_sql":sql,"source_arrow_schema":format!("{schema:?}"),
        "materialization_logical_plan":logical_plan,"materialization_logical_arrow_schema":logical_schema,
        "materialization_logical_stage":"Native DataFrame logical plan before the session analyzer/optimizer",
        "materialization_physical_plan":physical_plan,"materialization_physical_arrow_schema":physical_schema,
        "materialized_rows":rows(&materialized)?,
        "materialized_arrow_schemas":materialized.iter().map(|batch| format!("{:?}",batch.schema())).collect::<Vec<_>>(),
        "materialized_batch_rows":materialized.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
        "source_schema_arc_retained":same_schema_arc,"source_field_arcs_retained":same_field_arcs}),
    )
}

#[tokio::test]
async fn regexp_errors_match_all_observed_provider_layouts() -> TestResult {
    let diagnostic = include_str!("../fixtures/regexp-replace-partition-layout-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(diagnostic[0]["spark_version"], "4.2.0");
    let references = diagnostic
        .iter()
        .filter(|record| record["kind"] == "observation")
        .collect::<Vec<_>>();
    assert_eq!(references.len(), 18);
    let frozen: Value = serde_json::from_str(
        include_str!("../fixtures/regexp-replace-nonboundary-spark.jsonl")
            .lines()
            .next()
            .ok_or("missing input metadata")?,
    )?;
    let mut keys = HashSet::new();
    let mut reference_keys = HashSet::new();
    let mut layout_counts = [0; 3];
    let mut differences = Vec::new();
    eprintln!(
        "c06 matched partition capture header: {}",
        json!({
        "spark_capture_header":diagnostic[0],"references":18,"expected_observations":72,
        "input_fixture":"regexp-replace-nonboundary-spark.jsonl",
        "original_480_full_compatibility_pass":false})
    );
    for reference in references {
        assert!(
            reference_keys.insert(
                json!([reference["layout"], reference["id"], reference["ansi"]]).to_string()
            )
        );
        let layout = reference["layout"]
            .as_str()
            .ok_or("missing reference layout")?;
        for representation in 0..3 {
            for &batch_size in matched_batches(layout).ok_or("unknown reference layout")? {
                let key = matched_key(reference, representation, batch_size)
                    .ok_or("invalid matched-layout key or SQL")?;
                assert!(keys.insert(key.to_string()));
                let target = if layout == "original_registration" {
                    1
                } else {
                    2
                };
                let session = SessionContext::new_with_state(
                    configure_spark_sql(
                        SessionStateBuilder::new()
                            .with_default_features()
                            .with_config(
                                SessionConfig::new()
                                    .with_batch_size(batch_size)
                                    .with_target_partitions(target),
                            ),
                    )
                    .build(),
                );
                register_inputs(&session, &frozen, representation, batch_size)?;
                let mut setup = Vec::new();
                if layout == "original_registration" {
                    for table in ["rr_nonboundary", "rr_nonboundary_empty"] {
                        let materialized = materialize_original_partition(&session, table).await?;
                        let input = frozen["registered_inputs"]
                            .as_array()
                            .ok_or("missing inputs")?
                            .iter()
                            .find(|input| input["name"] == table)
                            .ok_or("missing source table")?;
                        assert_eq!(materialized["materialized_rows"], input["rows"]);
                        setup.push(materialized);
                    }
                }
                let mut providers = Vec::new();
                for table in ["rr_nonboundary", "rr_nonboundary_empty"] {
                    providers.push(provider_layout(&session, table).await?);
                }
                let providers = json!(providers);
                let actual = capture_observation(
                    &session,
                    &json!({"sql":reference["sql"],"ansi":reference["ansi"]}),
                )
                .await?;
                let matched = matched_result_matches(
                    reference,
                    representation,
                    batch_size,
                    &providers,
                    &actual,
                );
                eprintln!(
                    "c06 matched partition observation: {}",
                    json!({"key":key,"sql":reference["sql"],
                    "native_target_partitions":target,"actual_providers":providers,"setup":setup,
                    "spark_reference":reference,"actual":actual,"matches_reference":matched,
                    "comparison_stages":{"analyzed":"public native fields vs Spark analyzed/DataFrame fields",
                        "physical":"native pre-collect vs Spark executedPlan before collect","native_optimized":"unavailable"}})
                );
                if !matched {
                    differences.push(format!(
                        "matched-layout error comparison failed for {key}: {actual}"
                    ));
                }
                layout_counts[match layout {
                    "original_registration" => 0,
                    "native_batch_1" => 1,
                    _ => 2,
                }] += 1;
            }
        }
    }
    assert_eq!(keys.len(), 72);
    assert_eq!(reference_keys.len(), 18);
    assert_eq!(layout_counts, [36, 18, 18]);
    eprintln!(
        "c06 matched partition summary: {}",
        json!({"references":18,"observations":72,
        "layout_observations":layout_counts,"matches_reference":differences.is_empty(),
        "unexpected_differences":differences.len(),
        "original_480_full_compatibility_pass":false})
    );
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[test]
fn negative_receipts_reject_changed_keys_fields_rows_stages_and_causes() -> TestResult {
    for receipt in negative_receipts()? {
        let case = json!({"id":receipt["key"]["id"],"ansi":receipt["key"]["ansi"],
            "sql":receipt["sql"],"expected":receipt["spark_expected"]});
        let representation = receipt["key"]["representation"]
            .as_u64()
            .ok_or("missing representation")? as usize;
        let actual = &receipt["native_actual"];
        assert!(receipt_matches(&receipt, &case, representation, 1, actual));
        for field in [
            "family",
            "fixture",
            "id",
            "ansi",
            "representation",
            "batch_size",
        ] {
            let mut changed = receipt.clone();
            changed["key"][field] = json!("wrong");
            assert!(!receipt_matches(&changed, &case, representation, 1, actual));
        }
        for (field, value) in [
            ("sql", json!("SELECT 1")),
            ("spark_expected", Value::Null),
            ("matches_reference", json!(true)),
            ("full_compatibility_pass", json!(true)),
        ] {
            let mut changed = receipt.clone();
            changed[field] = value;
            assert!(!receipt_matches(&changed, &case, representation, 1, actual));
        }
        for field in ["analyzed_schema", "physical_schema"] {
            for (attribute, value) in [
                ("name", json!("wrong")),
                ("type", json!("wrong")),
                ("nullable", json!(true)),
                ("metadata", json!({"wrong":"value"})),
            ] {
                let mut changed = actual.clone();
                changed[field]["fields"][0][attribute] = value;
                assert!(!receipt_matches(
                    &receipt,
                    &case,
                    representation,
                    1,
                    &changed
                ));
            }
        }
        for (field, value) in [
            ("rows", json!([])),
            ("error_stage", json!("physical")),
            (
                "error",
                json!("execute: [INVALID_REGEXP_REPLACE] No group 9"),
            ),
            ("analyzed_arrow_schema", json!("wrong")),
            ("physical_arrow_schema", json!("wrong")),
        ] {
            let mut changed = actual.clone();
            changed[field] = value;
            assert!(!receipt_matches(
                &receipt,
                &case,
                representation,
                1,
                &changed
            ));
        }
        assert!(!receipt_matches(
            &receipt,
            &case,
            representation,
            32,
            actual
        ));
    }
    Ok(())
}

#[test]
fn matched_layouts_reject_wrong_mapping_fields_parameters_and_error_stage() -> TestResult {
    let records = include_str!("../fixtures/regexp-replace-partition-layout-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let receipt = negative_receipts()?
        .into_iter()
        .find(|receipt| receipt["key"]["representation"] == 0 && receipt["key"]["ansi"] == true)
        .ok_or("missing exact native receipt")?;
    let reference = records
        .iter()
        .find(|record| {
            record["kind"] == "observation"
                && record["layout"] == "native_batch_1"
                && record["id"] == receipt["key"]["id"]
                && record["ansi"] == true
        })
        .ok_or("missing partition reference")?;
    let providers = &reference["actual_registered_inputs"];
    let actual = &receipt["native_actual"];
    assert!(matched_result_matches(reference, 0, 1, providers, actual));
    assert!(!matched_result_matches(reference, 0, 32, providers, actual));
    assert!(!matched_result_matches(reference, 3, 1, providers, actual));
    for (field, value) in [
        ("layout", json!("native_batch_32")),
        ("id", json!("wrong")),
        ("sql", json!("SELECT 1")),
        ("ansi", Value::Null),
    ] {
        let mut changed = reference.clone();
        changed[field] = value;
        assert!(!matched_result_matches(&changed, 0, 1, providers, actual));
    }
    let mut changed = reference.clone();
    changed["layout"] = json!("original_registration");
    assert!(!matched_result_matches(&changed, 0, 1, providers, actual));
    for field in ["analyzed_schema", "physical_schema"] {
        for (attribute, value) in [
            ("name", json!("wrong")),
            ("type", json!("wrong")),
            ("nullable", json!(true)),
            ("metadata", json!({"wrong":"value"})),
        ] {
            let mut changed = actual.clone();
            changed[field]["fields"][0][attribute] = value;
            assert!(!matched_result_matches(
                reference, 0, 1, providers, &changed
            ));
        }
    }
    for (field, value) in [
        ("rows", json!([])),
        ("error_stage", json!("physical")),
        ("error", json!("No group 9")),
    ] {
        let mut changed = actual.clone();
        changed[field] = value;
        assert!(!matched_result_matches(
            reference, 0, 1, providers, &changed
        ));
    }
    for field in ["source", "pattern", "replacement", "position"] {
        let mut changed = reference.clone();
        changed["actual"]["parameters"][field] = json!("wrong");
        changed["actual"]["java_causes"][0]["parameters"][field] = json!("wrong");
        assert!(!matched_result_matches(&changed, 0, 1, providers, actual));
    }
    let mut changed = reference.clone();
    changed["actual"]["physical_schema_stage"] = json!("post-collect");
    assert!(!matched_result_matches(&changed, 0, 1, providers, actual));
    let mut changed = reference.clone();
    changed["actual"]["java_causes"][1]["message"] = json!("No group 9");
    assert!(!matched_result_matches(&changed, 0, 1, providers, actual));
    for field in ["actual_partition_count", "schema", "partitions"] {
        let mut changed = providers.clone();
        changed[0][field] = Value::Null;
        assert!(!matched_result_matches(reference, 0, 1, &changed, actual));
    }
    for field in ["keys", "rows"] {
        let mut changed = providers.clone();
        changed[0]["partitions"][0][field] = json!([]);
        assert!(!matched_result_matches(reference, 0, 1, &changed, actual));
    }
    Ok(())
}
