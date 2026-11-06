//! Complete generated window labels remain usable through outer SQL consumers.

use std::collections::BTreeSet;
use std::sync::{
    Arc, LazyLock,
    atomic::{AtomicUsize, Ordering},
};

use arrow::array::AsArray;
use arrow::datatypes::{DataType, Int32Type};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility, create_udf,
};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_inputs::register_recorded_inputs;
use spark_results::{
    TestResult, compare_exact_spark_query_stages_with_optimized, matches_spark_int32_cast_input,
};

fn window_name_references() -> TestResult<Vec<Value>> {
    let staged = include_str!("fixtures/window-name-stages-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(staged.len() - 1, 354);
    assert!(staged[0]["spark_build"].as_str().is_some_and(|build| {
        build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
    }));
    let mut records = Vec::new();
    for (corpus, source) in [
        (
            "original",
            include_str!("fixtures/window-names-spark.jsonl"),
        ),
        (
            "alias_free_control",
            include_str!("fixtures/window-names-pivot-controls-spark.jsonl"),
        ),
    ] {
        let mut captured = source
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        if records.is_empty() {
            records.push(captured.remove(0));
        } else {
            assert_eq!(records[0], captured.remove(0));
        }
        for mut case in captured {
            case["corpus"] = json!(corpus);
            records.push(case);
        }
    }
    assert_eq!(records.len() - 1, 294);
    assert_eq!(staged[0]["inputs"], records[0]["inputs"]);
    for original in &records[1..] {
        let recaptured = staged[1..]
            .iter()
            .find(|case| {
                case["corpus"] == original["corpus"]
                    && case["id"] == original["id"]
                    && case["ansi"] == original["ansi"]
            })
            .ok_or("recapture omitted an original reference")?;
        assert_eq!(recaptured["sql"], original["sql"]);
        assert_eq!(recaptured["expected"], original["expected"]);
        assert_eq!(recaptured["source_expected"], original["expected"]);
        if original["corpus"] == "alias_free_control" {
            let id = format!(
                "{}_parenthesized",
                original["id"].as_str().ok_or("missing ID")?
            );
            assert!(staged[1..].iter().any(|case| {
                case["corpus"] == "supplement"
                    && case["id"] == id
                    && case["ansi"] == original["ansi"]
            }));
        }
    }
    for table in ["inputs", "empty_inputs", "null_inputs"] {
        for name in [
            "literal_arguments",
            "coerced_default",
            "aggregate_named",
            "zero_frame",
            "folded_ignore_nulls",
            "composed",
            "outer_order",
        ] {
            for ansi in [true, false] {
                assert!(staged[1..].iter().any(|case| {
                    case["corpus"] == "supplement"
                        && case["id"] == format!("{table}_{name}")
                        && case["ansi"] == ansi
                }));
            }
        }
    }
    let mut errors = 0;
    let mut ordered = 0;
    for case in &staged[1..] {
        if case["expected"].get("rows").is_some() {
            for stage in [
                "analyzed",
                "optimized",
                "executed_before_collect",
                "executed",
            ] {
                assert!(case["schema_stages"].get(stage).is_some());
            }
        } else {
            errors += 1;
            assert_eq!(
                case["expected"]["condition"],
                "INVALID_PARAMETER_VALUE.BOOLEAN"
            );
            assert_eq!(case["spark_error_phase"], "sql");
            assert_eq!(case["schema_stages"], json!({}));
            assert_eq!(case["spark_error_details"]["sql_state"], "22023");
            assert_eq!(
                case["spark_error_details"]["message_parameters"],
                json!({
                    "functionName":"`nth_value`", "parameter":"`ignoreNulls`",
                    "invalidValue":"\"CAST(true AS BOOLEAN)\"",
                })
            );
        }
        if case["ordered"] == true {
            ordered += 1;
            assert_eq!(case["expected"]["rows"], case["rows_raw"]);
            assert!(
                case["sql"]
                    .as_str()
                    .is_some_and(|sql| sql.ends_with(" ORDER BY id"))
            );
        }
    }
    assert_eq!(errors, 6);
    assert_eq!(ordered, 6);
    Ok(staged)
}

fn matches_window_boolean_parameter_error(observation: &Value) -> bool {
    observation["native_phase"] == "resolve"
        && observation["error"].as_str().is_some_and(|error| {
            [
                "[INVALID_PARAMETER_VALUE.BOOLEAN]",
                "`ignoreNulls` in `nth_value`",
                "expects a boolean literal, but got \"CAST(true AS BOOLEAN)\"",
            ]
            .into_iter()
            .all(|cause| error.contains(cause))
        })
}

#[test]
fn window_parameter_errors_require_the_specific_boolean_cause_during_resolution() {
    let error = "[INVALID_PARAMETER_VALUE.BOOLEAN] The value of parameter(s) `ignoreNulls` in `nth_value` is invalid: expects a boolean literal, but got \"CAST(true AS BOOLEAN)\".";
    assert!(matches_window_boolean_parameter_error(&json!({
        "native_phase":"resolve", "error":error,
    })));
    for observation in [
        json!({"native_phase":"collect", "error":error}),
        json!({"native_phase":"resolve", "error":"unsupported window function"}),
        json!({"native_phase":"resolve", "error":error.replace("nth_value", "lead")}),
        json!({"native_phase":"resolve", "error":error.replace("ignoreNulls", "offset")}),
        json!({"native_phase":"resolve", "error":error.replace("CAST(true AS BOOLEAN)", "NULL")}),
        json!({"native_phase":"resolve", "error":error.replace("INVALID_PARAMETER_VALUE.BOOLEAN", "DATATYPE_MISMATCH")}),
        json!({"native_phase":"encode", "error":null}),
    ] {
        assert!(!matches_window_boolean_parameter_error(&observation));
    }
}

#[tokio::test]
async fn generated_window_names_match_spark() -> TestResult {
    let records = window_name_references()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["corpus"].as_str().ok_or("missing corpus")?,
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
    }
    let mut differences = Vec::new();
    let report_path = std::env::var_os("DAR_WINDOW_NAMES_REPORT");
    let mut observations = Vec::new();
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
        register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let actual = compare_exact_spark_query_stages_with_optimized(&session, case).await;
            let cause_matches = case["expected"].get("condition").is_none()
                || actual
                    .as_ref()
                    .is_ok_and(matches_window_boolean_parameter_error);
            let matches = actual.as_ref().is_ok_and(|result| {
                result["matches"] == true && result["optimized_matches"] == true
            }) && cause_matches;
            if report_path.is_some() {
                observations.push(json!({
                    "corpus":case["corpus"], "id":case["id"],
                    "ansi":case["ansi"], "batch":batch_size, "reference":case,
                    "actual":actual.as_ref().map_or_else(
                        |error| json!({"error":error.to_string()}), |result| result.clone()),
                    "cause_matches":cause_matches, "matches":matches,
                }));
            }
            if !matches {
                differences.push(format!(
                    "{}:{} ANSI={} batch={batch_size} cause_matches={cause_matches}: {actual:?}",
                    case["corpus"], case["id"], case["ansi"]
                ));
            }
        }
    }
    if let Some(path) = report_path {
        std::fs::write(path, serde_json::to_vec_pretty(&observations)?)?;
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn generated_interval_window_names_match_original_spark_stages() -> TestResult {
    let records = include_str!("fixtures/interval-qualifiers/interval-qualifiers-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let cases = records[1..]
        .iter()
        .filter(|case| {
            case["context"] == "window"
                && matches!(
                    case["id"].as_str(),
                    Some(
                        "range_day"
                            | "range_hour"
                            | "range_day_hour"
                            | "range_day_second"
                            | "range_year"
                            | "range_month"
                    )
                )
        })
        .collect::<Vec<_>>();
    assert_eq!(cases.len(), 12);
    let mut observations = Vec::new();
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_batch_size(batch_size)
                            .with_target_partitions(1),
                    ),
            )
            .build(),
        );
        register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &cases {
            let actual = compare_exact_spark_query_stages_with_optimized(&session, case).await;
            let matches = actual.as_ref().is_ok_and(|result| {
                result["matches"] == true && result["optimized_matches"] == true
            });
            observations.push(json!({
                "id":case["id"], "ansi":case["ansi"], "batch":batch_size,
                "reference":case, "matches":matches,
                "actual":actual.map_or_else(|error| json!({"error":error.to_string()}), |result| result),
            }));
        }
    }
    if let Some(path) = std::env::var_os("DAR_WINDOW_NAMES_REPORT") {
        std::fs::write(
            std::path::PathBuf::from(path).with_extension("interval.json"),
            serde_json::to_vec_pretty(&observations)?,
        )?;
    }
    let differences = observations
        .iter()
        .filter(|observation| observation["matches"] != true)
        .collect::<Vec<_>>();
    assert!(differences.is_empty(), "{differences:#?}");
    Ok(())
}

#[tokio::test]
async fn generated_window_names_cover_source_boundaries() -> TestResult {
    let records = include_str!("fixtures/window-name-boundaries-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len() - 1, 40);
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    // Python UDF admission is research evidence; Rust UDF capability tests are separate below.
    let (python_udf, cases): (Vec<_>, Vec<_>) = records[1..].iter().partition(|case| {
        matches!(
            case["id"].as_str(),
            Some(
                "immutable_offset_727_implicit"
                    | "immutable_offset_727_explicit"
                    | "volatile_offset_727_implicit"
                    | "volatile_offset_727_explicit"
            )
        )
    });
    assert_eq!(python_udf.len(), 8);
    // Typed Spark metadata cannot be registered through Arrow's string-valued metadata API.
    let (typed_metadata, mut cases): (Vec<_>, Vec<_>) = cases
        .into_iter()
        .partition(|case| case["id"] == "aliases_metadata");
    assert_eq!(typed_metadata.len(), 2);
    assert_eq!(cases.len(), 30);
    let mut input_metadata = records[0].clone();
    input_metadata["inputs"]
        .as_array_mut()
        .ok_or("missing inputs")?
        .retain(|input| input["name"] != "metadata_inputs");
    let string_metadata = include_str!("fixtures/window-name-string-metadata-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(string_metadata.len() - 1, 2);
    assert_eq!(
        string_metadata[0]["spark_revision"],
        records[0]["spark_revision"]
    );
    assert!(
        string_metadata[1..]
            .iter()
            .all(|case| case["id"] == "aliases_string_metadata")
    );
    assert_eq!(
        string_metadata[1..]
            .iter()
            .filter_map(|case| case["ansi"].as_bool())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([false, true])
    );
    let string_inputs = string_metadata[0]["inputs"]
        .as_array()
        .ok_or("missing string-metadata inputs")?
        .iter()
        .filter(|input| input["name"] == "metadata_string_inputs")
        .collect::<Vec<_>>();
    assert_eq!(string_inputs.len(), 1);
    input_metadata["inputs"]
        .as_array_mut()
        .ok_or("missing inputs")?
        .push((*string_inputs[0]).clone());
    cases.extend(&string_metadata[1..]);
    assert_eq!(cases.len(), 32);
    let mut observations = Vec::new();
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
        register_recorded_inputs(&session, &input_metadata, batch_size)?;
        for case in &cases {
            let actual = compare_exact_spark_query_stages_with_optimized(&session, case).await;
            let matches = actual.as_ref().is_ok_and(|result| {
                result["matches"] == true
                    && result["optimized_matches"] == true
                    && matches_spark_int32_cast_input(case, result)
            });
            observations.push(json!({
                "id":case["id"], "ansi":case["ansi"], "batch":batch_size,
                "reference":case, "matches":matches,
                "actual":actual.map_or_else(|error| json!({"error":error.to_string()}), |result| result),
            }));
        }
    }
    if let Some(path) = std::env::var_os("DAR_WINDOW_NAMES_REPORT") {
        std::fs::write(
            std::path::PathBuf::from(path).with_extension("boundaries.json"),
            serde_json::to_vec_pretty(&observations)?,
        )?;
    }
    let differences = observations
        .iter()
        .filter(|observation| observation["matches"] != true)
        .collect::<Vec<_>>();
    assert!(differences.is_empty(), "{differences:#?}");
    Ok(())
}

#[tokio::test]
async fn window_names_do_not_evaluate_value_or_default_arguments() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    session.register_udf(create_udf(
        "observed_value",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Volatile,
        Arc::new(move |arguments| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(arguments[0].clone())
        }),
    ));
    let observed = calls.clone();
    session.register_udf(create_udf(
        "observed_default",
        vec![],
        DataType::Int32,
        Volatility::Volatile,
        Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(ColumnarValue::Scalar(
                datafusion::common::ScalarValue::Int32(Some(-99)),
            ))
        }),
    ));
    let observed = calls.clone();
    session.register_udf(create_udf(
        "observed_key",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(move |arguments| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(arguments[0].clone())
        }),
    ));
    let observed = calls.clone();
    session.register_udf(create_udf(
        "observed_filter",
        vec![DataType::Boolean],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(move |arguments| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(arguments[0].clone())
        }),
    ));
    for ansi_mode in [true, false] {
        let query = plan_query(&session,
            "SELECT SUM(observed_value(v)) OVER (ORDER BY id),LAG(observed_value(v),1,observed_default()) OVER (ORDER BY id) FROM VALUES(1,2),(2,3) inputs(id,v)",
            &SparkSqlOptions { ansi_mode, ..SparkSqlOptions::default() },
        ).await?;
        assert!(query.column_names[0].contains("observed_value(v)"));
        assert!(query.column_names[1].contains("observed_default()"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let query = plan_query(&session,
            "SELECT SUM(v) FILTER (WHERE observed_filter(p)) OVER (PARTITION BY observed_key(g) ORDER BY observed_key(id)) FROM VALUES(1,1,2,true),(2,1,3,false) inputs(id,g,v,p)",
            &SparkSqlOptions { ansi_mode, ..SparkSqlOptions::default() },
        ).await?;
        for display in [
            "observed_filter(p)",
            "PARTITION BY observed_key(g)",
            "ORDER BY observed_key(id) ASC NULLS FIRST",
        ] {
            assert!(
                query.column_names[0].contains(display),
                "{}",
                query.column_names[0]
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[tokio::test]
async fn offset_window_names_respect_folding_permission_and_evaluate_once() -> TestResult {
    static CALLS: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct ObservedOffset {
        name: &'static str,
        allow_folding: bool,
    }

    impl ScalarUDFImpl for ObservedOffset {
        fn name(&self) -> &str {
            self.name
        }

        fn signature(&self) -> &Signature {
            static SIGNATURE: LazyLock<Signature> =
                LazyLock::new(|| Signature::exact(vec![], Volatility::Immutable));
            &SIGNATURE
        }

        fn supports_constant_folding(&self) -> bool {
            self.allow_folding
        }

        fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
            Ok(DataType::Int32)
        }

        fn invoke_with_args(
            &self,
            _: ScalarFunctionArgs,
        ) -> datafusion::common::Result<ColumnarValue> {
            CALLS.fetch_add(1, Ordering::SeqCst);
            Ok(ColumnarValue::Scalar(
                datafusion::common::ScalarValue::Int32(Some(1)),
            ))
        }
    }

    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    session.register_udf(
        ScalarUDF::from(ObservedOffset {
            name: "abs",
            allow_folding: false,
        })
        .with_aliases(["unfolded_offset"]),
    );
    session.register_udf(ScalarUDF::from(ObservedOffset {
        name: "folded_offset",
        allow_folding: true,
    }));
    session.register_udf(create_udf(
        "volatile_offset",
        vec![],
        DataType::Int32,
        Volatility::Volatile,
        Arc::new(|_| {
            CALLS.fetch_add(1, Ordering::SeqCst);
            Ok(ColumnarValue::Scalar(
                datafusion::common::ScalarValue::Int32(Some(1)),
            ))
        }),
    ));
    for ansi_mode in [true, false] {
        let options = SparkSqlOptions {
            ansi_mode,
            ..SparkSqlOptions::default()
        };
        for function in ["LAG", "LEAD"] {
            for offset in [
                "unfolded_offset()",
                "1 + unfolded_offset()",
                "volatile_offset()",
            ] {
                for explicit in [false, true] {
                    let frame = if !explicit {
                        ""
                    } else if function == "LAG" {
                        " ROWS BETWEEN -1 FOLLOWING AND -1 FOLLOWING"
                    } else {
                        " ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING"
                    };
                    let sql = format!(
                        "SELECT {function}(v,{offset}) OVER (ORDER BY id{frame}) FROM VALUES(1,10),(2,20) inputs(id,v)"
                    );
                    let before = CALLS.load(Ordering::SeqCst);
                    let result = plan_query(&session, &sql, &options).await;
                    assert!(
                        result.is_err_and(|error| error
                            .to_string()
                            .contains("[WINDOW_OFFSET_FRAME_MISMATCH]")),
                        "{sql}"
                    );
                    assert_eq!(CALLS.load(Ordering::SeqCst), before, "{sql}");
                }
            }
        }
        for offset in ["folded_offset()", "abs(-1)"] {
            for frame in ["", " ROWS BETWEEN -1 FOLLOWING AND -1 FOLLOWING"] {
                let sql = format!(
                    "SELECT LAG(v,{offset},-99) OVER (ORDER BY id{frame}) FROM VALUES(1,10),(2,20) inputs(id,v)"
                );
                let before = CALLS.load(Ordering::SeqCst);
                let query = plan_query(&session, &sql, &options).await?;
                assert!(
                    query.column_names[0].contains(offset),
                    "{}",
                    query.column_names[0]
                );
                let expected_calls = before + usize::from(offset == "folded_offset()");
                assert_eq!(CALLS.load(Ordering::SeqCst), expected_calls, "{sql}");
                let physical = query.create_physical_plan(&session).await?;
                let mut values = collect(physical, session.task_ctx())
                    .await?
                    .into_iter()
                    .flat_map(|batch| {
                        batch
                            .column(0)
                            .as_primitive::<Int32Type>()
                            .values()
                            .to_vec()
                    })
                    .collect::<Vec<_>>();
                values.sort_unstable();
                assert_eq!(values, [-99, 10], "{sql}");
                assert_eq!(CALLS.load(Ordering::SeqCst), expected_calls, "{sql}");
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn generated_window_names_leave_native_sql_unchanged() -> TestResult {
    let metadata: Value = serde_json::from_str(
        include_str!("fixtures/window-names-spark.jsonl")
            .lines()
            .next()
            .ok_or("missing metadata")?,
    )?;
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured] {
        register_recorded_inputs(session, &metadata, 2)?;
    }
    let sqls = [
        "SELECT id,ROW_NUMBER() OVER (ORDER BY id),LAG(v) OVER (ORDER BY id),SUM(v) OVER (PARTITION BY g) FROM inputs ORDER BY id",
        "SELECT SUM(v),CAST(v AS BIGINT),ABS(v) FROM inputs GROUP BY v ORDER BY v",
        "SELECT SUM(v) AS total,CAST(v AS BIGINT) AS converted,ABS(v) AS magnitude FROM inputs GROUP BY v ORDER BY v",
        "SELECT id,SUM(v) OVER (ORDER BY id) FROM empty_inputs ORDER BY id",
    ];
    let mut observations = Vec::new();
    for after_spark_query in [false, true] {
        if after_spark_query {
            let query = plan_query(
                &configured,
                "SELECT ROW_NUMBER() OVER (ORDER BY id),SUM(v) OVER (PARTITION BY g) FROM inputs",
                &SparkSqlOptions::default(),
            )
            .await?;
            let physical = query.create_physical_plan(&configured).await?;
            collect(physical, configured.task_ctx()).await?;
        }
        for sql in sqls {
            let mut results = Vec::new();
            for (name, session) in [("native", &native), ("configured", &configured)] {
                let mut phase = "sql";
                let result: TestResult<_> = async {
                    let frame = session.sql(sql).await?;
                    let public_schema = frame.schema().clone();
                    phase = "create_physical_plan";
                    let physical = frame.create_physical_plan().await?;
                    let before_collect = physical.schema();
                    phase = "collect";
                    let batches = collect(physical.clone(), session.task_ctx()).await?;
                    let physical_schema = physical.schema();
                    phase = "concat_batches";
                    let rows = arrow::compute::concat_batches(&physical_schema, &batches)?;
                    Ok((public_schema, before_collect, physical_schema, rows))
                }
                .await;
                results.push((name, phase, result.map_err(|error| error.to_string())));
            }
            observations.push((after_spark_query, sql, results));
        }
    }
    eprintln!("native isolation observations: {observations:#?}");
    for (index, observation) in observations.iter().enumerate() {
        let results = &observation.2;
        assert_eq!(results[0].1, results[1].1, "{observation:?}");
        assert_eq!(results[0].2, results[1].2, "{observation:?}");
        let actual = &results[0].2;
        if index % sqls.len() == 1 {
            assert!(actual.as_ref().is_err_and(|error| error.contains(
                "Projections require unique expression names but the expression \"CAST(inputs.v AS Int64)\" at position 1 and \"inputs.v\" at position 3 have the same name."
            )), "original duplicate-name query: {observation:?}");
        } else {
            let (public, before, after, rows) = actual
                .as_ref()
                .map_err(|error| format!("{observation:?}: {error}"))?;
            assert_eq!(public.fields(), before.fields(), "{observation:?}");
            assert_eq!(before, after, "{observation:?}");
            assert_eq!(&rows.schema(), after, "{observation:?}");
        }
        if index >= sqls.len() {
            assert_eq!(
                observations[index - sqls.len()].2,
                *results,
                "{observation:?}"
            );
        }
    }
    Ok(())
}
