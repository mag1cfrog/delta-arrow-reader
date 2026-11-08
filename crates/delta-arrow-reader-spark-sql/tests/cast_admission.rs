//! Unsupported casts fail before a resolved query can expose output Fields.

use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[allow(
    dead_code,
    reason = "These tests reuse the maintained provider decoder."
)]
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(
    dead_code,
    reason = "These tests reuse complete Spark stage comparisons."
)]
#[path = "support/spark_results.rs"]
mod spark_results;
#[allow(
    dead_code,
    reason = "These tests reuse the original string layout registration."
)]
#[path = "support/string_binary.rs"]
mod string_binary;
use spark_results::TestResult;

const ORIGINALS: [(&str, &str, &str, &str); 4] = [
    (
        "original_binary_struct_int",
        "SELECT substring('abcdef',CAST(CAST(X'01' AS STRUCT<x:INT>) AS INT),CAST(NULL AS STRING)) AS r;",
        "BINARY",
        "STRUCT",
    ),
    (
        "original_null_struct_int",
        "SELECT substring('abcdef',CAST(CAST(NULL AS STRUCT<x:INT>) AS INT),CAST(NULL AS STRING)) AS r;",
        "STRUCT",
        "INT",
    ),
    (
        "original_cast_binary_int",
        "SELECT substring('abcdef',CAST(X'01' AS INT),CAST(NULL AS STRING)) AS r;",
        "BINARY",
        "INT",
    ),
    (
        "original_try_binary_int",
        "SELECT substring('abcdef',TRY_CAST(X'01' AS INT),CAST(NULL AS STRING)) AS r;",
        "BINARY",
        "INT",
    ),
];

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

fn cast_requests() -> TestResult<Value> {
    Ok(serde_json::from_str(include_str!(
        "fixtures/cast-admission-1098-cases.json"
    ))?)
}

fn spark_references() -> TestResult<Vec<Value>> {
    let records = include_str!("fixtures/cast-admission-1098-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 31);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_build"]["SPARK_REVISION"]["value"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    let request = cast_requests()?;
    let cases = request["corpora"][0]["cases"]
        .as_array()
        .ok_or("missing cases")?;
    let mut seen = BTreeSet::new();
    for case in &records[1..] {
        let id = case["id"].as_str().ok_or("missing ID")?;
        let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
        assert!(seen.insert((id, ansi)));
        let input = cases
            .iter()
            .find(|input| input["id"] == id && input["ansi"] == ansi)
            .ok_or("unexpected recorded case")?;
        for key in ["corpus", "sql", "ownership", "providers"] {
            assert_eq!(case[key], input[key], "{id}, ANSI={ansi}: {key}");
        }
        assert!(case.get("capture_failure").is_none(), "{case}");
    }
    assert_eq!(seen.len(), cases.len());
    Ok(records)
}

fn assert_analysis_rejection(case: &Value) {
    assert_eq!(case["status"], "error", "{case}");
    assert_eq!(case["spark_error_phase"], "sql", "{case}");
    assert_eq!(
        case["spark_error_details"]["condition"], "DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION",
        "{case}"
    );
    assert_eq!(case["spark_error_details"]["sql_state"], "42K09", "{case}");
    assert_eq!(case["schema_stages"], json!({}), "{case}");
    assert!(case.get("rows_raw").is_none(), "{case}");
    for stage in [
        "public",
        "analyzed",
        "optimized",
        "executed_before_collect",
        "executed",
    ] {
        assert_eq!(
            case["schema_stage_availability"][stage]["available"], false,
            "{case}"
        );
    }
}

fn normalize_diagnostic_text(value: &str) -> String {
    let mut in_quoted_identifier = false;
    let mut text = String::new();
    for c in value.chars() {
        if c == '`' {
            in_quoted_identifier = !in_quoted_identifier;
            text.push(c);
        } else if in_quoted_identifier {
            text.push(c);
        } else if !c.is_ascii_whitespace() {
            text.push(c.to_ascii_uppercase());
        }
    }
    text
}

fn normalize_cast_type(value: &str) -> String {
    let value = value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .unwrap_or(value);
    normalize_diagnostic_text(value)
}

fn original_cast_admission_error_matches(message: &str, source: &str, target: &str) -> bool {
    let diagnostic = message
        .strip_prefix("External error: analysis error: ")
        .unwrap_or(message);
    let compact = normalize_diagnostic_text(diagnostic);
    let expected = format!(
        "[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION]CANNOTCAST\"{}\"TO\"{}\".SQLSTATE:42K09",
        normalize_cast_type(source),
        normalize_cast_type(target)
    );
    compact == expected
}

#[test]
fn cast_admission_diagnostics_reject_wrong_types_states_and_literal_substitutes() {
    let condition = "[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION]";
    let diagnostic = "Cannot cast \"STRUCT<x:INT>\" to \"INT\". SQLSTATE: 42K09";
    let source = "\"STRUCT<x: INT>\"";
    let target = "\"INT\"";
    assert!(original_cast_admission_error_matches(
        &format!("External error: analysis error: {condition} {diagnostic}"),
        source,
        target,
    ));
    assert!(original_cast_admission_error_matches(
        &format!("{condition} Cannot cast \"BINARY\" to \"STRUCT<x:INT>\". SQLSTATE: 42K09"),
        "\"BINARY\"",
        "\"STRUCT<x:INT>\"",
    ));
    for invalid in [
        diagnostic.replace("STRUCT<x:INT>", "STRUCT<x:BINARY>"),
        diagnostic.replace("to \"INT\"", "to \"BIGINT\""),
        diagnostic.replace("to \"INT\"", "to \"INT32\""),
        diagnostic.replace("42K09", "42K00"),
        diagnostic.replace("42K09", "42K099"),
        diagnostic.replace(" SQLSTATE: 42K09", ""),
        "SELECT CAST(NULL AS STRUCT<x:INT>) AS r /* INT, SQLSTATE:42K09 */".into(),
        format!("'{diagnostic}'"),
        format!("\"{diagnostic}\""),
        "{\"srcType\":\"STRUCT<x:INT>\",\"targetType\":\"INT\",\"sqlState\":\"42K09\"}".into(),
        format!("{diagnostic}; input=abcdef"),
        format!("{diagnostic}; input=X'01'"),
        format!("{diagnostic}; input=Binary(\"01\")"),
        format!("{diagnostic}; input=0x01"),
        format!("{diagnostic}; diagnostic index=101"),
    ] {
        assert!(
            !original_cast_admission_error_matches(
                &format!("{condition} {invalid}"),
                source,
                target,
            ),
            "{invalid}"
        );
    }
    assert!(!original_cast_admission_error_matches(
        diagnostic, source, target
    ));
    assert!(!original_cast_admission_error_matches(
        &format!("input=Binary(\"01\"): {condition} {diagnostic}"),
        source,
        target,
    ));
    assert!(!original_cast_admission_error_matches(
        &format!("{condition} Cannot cast \"BINARY\" to \"STRUCT<x:BINARY>\". SQLSTATE:42K09"),
        "\"BINARY\"",
        "\"STRUCT<x:INT>\"",
    ));
}

fn register_inputs(
    session: &SessionContext,
    request: &Value,
    representation: usize,
    batch_size: usize,
) -> TestResult {
    let providers = request["corpora"][0]["metadata"]["inputs"]
        .as_array()
        .ok_or("missing providers")?;
    let strings = providers
        .iter()
        .filter(|provider| {
            provider["name"]
                .as_str()
                .is_some_and(|name| name.starts_with("substring_"))
        })
        .cloned()
        .collect::<Vec<_>>();
    let floats = providers
        .iter()
        .filter(|provider| {
            provider["name"]
                .as_str()
                .is_some_and(|name| name.starts_with("floating_"))
        })
        .cloned()
        .collect::<Vec<_>>();
    string_binary::register_inputs(
        session,
        &json!({"inputs":strings}),
        representation,
        batch_size,
    )?;
    spark_inputs::register_recorded_inputs_with_slices(
        session,
        &json!({"inputs":floats,"native":request["native"]}),
        batch_size,
    )
}

async fn compare_provider_with_spark(
    session: &SessionContext,
    provider: &Value,
) -> TestResult<Value> {
    let reference = &provider["observation"];
    if provider["name"]
        .as_str()
        .is_some_and(|name| name.starts_with("substring_"))
    {
        let actual = string_binary::capture_observation(session, reference).await?;
        let matches = actual.get("error").is_none()
            && actual["analyzed_schema"] == reference["schema_stages"]["analyzed"]
            && actual["physical_schema"] == reference["schema_stages"]["executed"]
            && actual["rows"] == reference["expected"]["rows"];
        return Ok(json!({"matches":matches,"observation":actual}));
    }
    spark_results::compare_exact_spark_query_stages(session, reference).await
}

#[tokio::test]
async fn original_casts_reject_before_fields_in_all_48_contexts() -> TestResult {
    let request = cast_requests()?;
    let references = spark_references()?;
    let mut seen = BTreeSet::new();
    let mut provider_receipts = 0;
    for representation in 0..3 {
        for batch_size in [1, 32] {
            let session = session(batch_size);
            register_inputs(&session, &request, representation, batch_size)?;
            for (id, sql, source, target) in ORIGINALS {
                for ansi_mode in [true, false] {
                    assert!(seen.insert((id, ansi_mode, representation, batch_size)));
                    let reference = references[1..]
                        .iter()
                        .find(|case| case["id"] == id && case["ansi"] == ansi_mode)
                        .ok_or("missing original Spark reference")?;
                    assert_eq!(reference["sql"], sql);
                    assert_analysis_rejection(reference);
                    let parameters = &reference["spark_error_details"]["parameters"];
                    let oracle_source = if source == "STRUCT" {
                        "STRUCT<X:INT>"
                    } else {
                        source
                    };
                    let oracle_target = if target == "STRUCT" {
                        "STRUCT<X:INT>"
                    } else {
                        target
                    };
                    let source_type = parameters["srcType"]
                        .as_str()
                        .ok_or("missing source type")?;
                    let target_type = parameters["targetType"]
                        .as_str()
                        .ok_or("missing target type")?;
                    assert_eq!(normalize_cast_type(source_type), oracle_source);
                    assert_eq!(normalize_cast_type(target_type), oracle_target);
                    let receipts = reference["provider_readbacks"]
                        .as_array()
                        .ok_or("missing provider readbacks")?;
                    assert_eq!(receipts.len(), 4);
                    assert_eq!(
                        json!(
                            receipts
                                .iter()
                                .map(|provider| &provider["name"])
                                .collect::<Vec<_>>()
                        ),
                        reference["providers"]
                    );
                    for provider in receipts {
                        assert_eq!(provider["observation"]["status"], "ok", "{provider}");
                        let comparison = compare_provider_with_spark(&session, provider).await?;
                        assert_eq!(comparison["matches"], true, "{id}: {comparison}");
                        provider_receipts += 1;
                    }
                    let error = plan_query(
                        &session,
                        sql,
                        &SparkSqlOptions {
                            ansi_mode,
                            session_timezone: "UTC".into(),
                            ..Default::default()
                        },
                    )
                    .await
                    .expect_err("illegal cast exposed a resolved query and analyzed Fields");
                    let message = error.to_string();
                    assert!(
                        message.contains("[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION]"),
                        "{sql}: {error:?}"
                    );
                    assert!(
                        original_cast_admission_error_matches(&message, source_type, target_type),
                        "{id}: expected rejected {source_type} to {target_type}, SQLSTATE 42K09, and no input literals; got {error:?}"
                    );
                }
            }
        }
    }
    assert_eq!(seen.len(), 48);
    assert_eq!(provider_receipts, 192);
    Ok(())
}

#[tokio::test]
async fn shared_cast_controls_preserve_spark_and_inherited_native_stages() -> TestResult {
    let session = session(32);
    let references = spark_references()?;
    let mut controls = 0;
    for reference in &references[1..] {
        if reference["ownership"] == "original_cast_admission" {
            continue;
        }
        if reference["ownership"] == "shared_cast_rejection_control" {
            assert_analysis_rejection(reference);
        }
        let comparison =
            spark_results::compare_exact_spark_query_stages_with_optimized(&session, reference)
                .await?;
        if reference["ownership"] == "shared_cast_rejection_control" {
            let types = &reference["spark_error_details"]["parameters"];
            assert!(
                original_cast_admission_error_matches(
                    comparison["error"].as_str().ok_or("missing cast error")?,
                    types["srcType"].as_str().ok_or("missing source type")?,
                    types["targetType"].as_str().ok_or("missing target type")?,
                ),
                "{comparison}"
            );
        }
        if matches!(
            reference["id"].as_str(),
            Some("valid_nested_rename" | "numeric_double_timestamp")
        ) {
            // Existing analyzed-nullability gaps remain strict differences from Spark.
            let inherited: Value = serde_json::from_str(include_str!(
                "fixtures/cast-admission-1098-native-controls.json"
            ))?;
            let control = inherited["cases"]
                .as_array()
                .ok_or("missing controls")?
                .iter()
                .find(|control| {
                    control["id"] == reference["id"] && control["ansi"] == reference["ansi"]
                })
                .ok_or("missing inherited control")?;
            assert_eq!(comparison["matches"], false, "{comparison}");
            assert_eq!(control["reference"], *reference);
            assert_eq!(comparison, control["observation"], "{reference}");
        } else {
            assert_eq!(comparison["matches"], true, "{reference}: {comparison}");
        }
        assert_eq!(
            comparison["optimized_matches"], true,
            "{reference}: {comparison}"
        );
        if reference["spark_error_phase"] == "sql" {
            assert_eq!(comparison["native_phase"], "resolve", "{comparison}");
            for stage in [
                "public_schema",
                "before_collect",
                "physical_schema",
                "optimized_schema",
                "rows",
            ] {
                assert!(comparison[stage].is_null(), "{comparison}");
            }
        }
        controls += 1;
    }
    assert_eq!(controls, 22);
    Ok(())
}

#[test]
fn native_cast_identity_keeps_explicit_target_field_validation()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::{cast, col, execution_props::ExecutionProps, expr};
    use datafusion::physical_expr::{
        create_physical_expr,
        expressions::{CastExpr, Column},
    };

    for fields in [
        Fields::empty(),
        Fields::from(vec![Field::new("x", DataType::Int32, true)]),
    ] {
        let empty_struct = fields.is_empty();
        let source_type = DataType::Struct(fields);
        let schema = Schema::new(vec![Field::new("s", source_type.clone(), false)]);
        let df_schema = DFSchema::try_from(schema.clone())?;
        let properties = ExecutionProps::new();
        let identity = cast(col("s"), source_type.clone());
        let physical = create_physical_expr(&identity, &df_schema, &properties)?;
        assert!(physical.downcast_ref::<Column>().is_some());
        assert_eq!(physical.return_field(&schema)?.as_ref(), schema.field(0));

        let target = Arc::new(
            Field::new("explicit_target", source_type, true)
                .with_metadata([("target_meta".into(), "retained".into())].into()),
        );
        let explicit = expr::Expr::Cast(expr::Cast::new_from_field(
            Box::new(col("s")),
            Arc::clone(&target),
        ));
        let physical = create_physical_expr(&explicit, &df_schema, &properties);
        if empty_struct {
            assert!(
                physical
                    .as_ref()
                    .is_err_and(|error| error.to_string().contains("Unsupported CAST")),
                "{physical:?}"
            );
        } else {
            let physical = physical?;
            assert!(physical.downcast_ref::<CastExpr>().is_some());
            assert_eq!(physical.return_field(&schema)?, target);
        }
    }
    Ok(())
}

#[tokio::test]
async fn unsupported_union_casts_keep_storage_types_without_metadata() -> TestResult {
    use arrow::datatypes::{DataType, Field, Schema, UnionFields, UnionMode};
    use datafusion::datasource::MemTable;
    use std::sync::Arc;

    for (mode, source_type) in [
        (UnionMode::Dense, "dense_union<7:payload:binary>"),
        (UnionMode::Sparse, "sparse_union<7:payload:binary>"),
    ] {
        let session = session(32);
        let fields = UnionFields::try_new(
            [7],
            [Field::new("payload", DataType::Binary, true)
                .with_metadata([("private".into(), "metadata-sentinel".into())].into())],
        )?;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "u",
            DataType::Union(fields, mode),
            true,
        )]));
        session.register_table("t", Arc::new(MemTable::try_new(schema, vec![vec![]])?))?;
        for ansi_mode in [true, false] {
            for function in ["CAST", "TRY_CAST"] {
                let sql = format!("SELECT {function}(u AS INT) AS r FROM t");
                let error = plan_query(
                    &session,
                    &sql,
                    &SparkSqlOptions {
                        ansi_mode,
                        ..Default::default()
                    },
                )
                .await
                .expect_err("unsupported Union cast exposed Fields")
                .to_string();
                assert!(
                    original_cast_admission_error_matches(&error, source_type, "int"),
                    "{error}"
                );
                assert!(!error.contains("metadata-sentinel"), "{error}");
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn rejected_casts_distinguish_nested_nullability_and_map_ordering() -> TestResult {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use std::sync::Arc;

    let session = session(32);
    let entries = Field::new(
        "entries",
        DataType::Struct(
            vec![
                Field::new("key", DataType::Int32, false),
                Field::new("value", DataType::Int32, true)
                    .with_metadata([("private".into(), "metadata-sentinel".into())].into()),
            ]
            .into(),
        ),
        false,
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "m",
        DataType::Map(Arc::new(entries), true),
        true,
    )]));
    session.register_table("t", Arc::new(MemTable::try_new(schema, vec![vec![]])?))?;
    for ansi_mode in [true, false] {
        for (sql, source, target) in [
            (
                "SELECT CAST(CAST(NULL AS STRUCT<x:INT>) AS STRUCT<x:INT NOT NULL>) AS r",
                "struct<x:int>",
                "struct<x:int not null>",
            ),
            (
                "SELECT CAST(m AS MAP<INT,INT>) AS r FROM t",
                "map<int,int> [keys_sorted=true]",
                "map<int,int>",
            ),
            (
                "SELECT TRY_CAST(m AS MAP<INT,INT>) AS r FROM t",
                "map<int,int> [keys_sorted=true]",
                "map<int,int>",
            ),
        ] {
            let error = plan_query(
                &session,
                sql,
                &SparkSqlOptions {
                    ansi_mode,
                    ..Default::default()
                },
            )
            .await
            .expect_err("unsupported cast exposed Fields")
            .to_string();
            assert!(
                original_cast_admission_error_matches(&error, source, target),
                "{error}"
            );
            assert!(!error.contains("metadata-sentinel"), "{error}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn try_cast_preserves_arrow_admission_for_nullable_struct_children() -> TestResult {
    let session = session(32);
    for ansi_mode in [true, false] {
        // Arrow admits this pair while ordinary native CAST rejects it.
        plan_query(
            &session,
            "SELECT TRY_CAST(CAST(NULL AS STRUCT<x:INT>) AS STRUCT<x:INT NOT NULL>) AS r",
            &SparkSqlOptions {
                ansi_mode,
                ..Default::default()
            },
        )
        .await?;
    }
    Ok(())
}

#[test]
fn cast_diagnostic_comparison_keeps_identifier_boundaries_and_spaces() {
    let condition = "[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION]";
    for source in [
        "struct<`x:int,y`:binary>",
        "struct<`a``b`:binary>",
        "struct<`a b`:binary>",
        "struct<``:binary>",
    ] {
        let message = format!("{condition} cannot cast \"{source}\" to \"int\". SQLSTATE: 42K09");
        assert!(original_cast_admission_error_matches(
            &message, source, "int"
        ));
    }
    assert!(!original_cast_admission_error_matches(
        &format!("{condition} cannot cast \"struct<x:int,y:binary>\" to \"int\". SQLSTATE: 42K09"),
        "struct<`x:int,y`:binary>",
        "int",
    ));
    assert!(!original_cast_admission_error_matches(
        &format!("{condition} cannot cast \"struct<`ab`:binary>\" to \"int\". SQLSTATE: 42K09"),
        "struct<`a b`:binary>",
        "int",
    ));
}
