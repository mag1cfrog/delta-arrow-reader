//! CONCAT retains constant operand errors before propagating NULL values.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::StringArray;
use arrow::error::ArrowError;
use arrow::util::display::array_value_to_string;
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_observation, capture_result, register_inputs};

fn matches_reference(case: &Value, actual: &Value) -> bool {
    let expected = &case["expected"];
    if actual["analyzed_schema"] != expected["analyzed_schema"]
        || actual["analyzed_arrow_schema"]
            .as_str()
            .is_none_or(|schema| schema.is_empty())
    {
        return false;
    }
    match expected["status"].as_str() {
        Some("ok") => {
            actual.get("error").is_none()
                && actual.get("error_stage").is_none()
                && actual["physical_schema"] == expected["physical_schema"]
                && actual["physical_arrow_schema"].is_string()
                && actual["rows"] == expected["rows"]
        }
        Some("error") => {
            expected["phase"] == "optimized_schema"
                && expected["condition"] == "DIVIDE_BY_ZERO"
                && actual["error_stage"] == "physical"
                && actual.get("physical_schema") == Some(&Value::Null)
                && actual.get("physical_arrow_schema") == Some(&Value::Null)
                && actual.get("rows").is_none()
                && actual["error"].as_str().is_some_and(|error| {
                    error.starts_with("physical:")
                        && error.ends_with("Arrow error: Divide by zero error")
                })
        }
        _ => false,
    }
}

struct NativeObservation {
    fields: Value,
    error: Option<DataFusionError>,
}

impl NativeObservation {
    fn receipt(&self) -> Value {
        let mut receipt = self.fields.clone();
        if let Some(error) = &self.error {
            receipt["error"] = json!(error.to_string());
            receipt["error_debug"] = json!(format!("{error:?}"));
            receipt["root_error"] = json!(error.find_root().to_string());
            receipt["root_error_debug"] = json!(format!("{:?}", error.find_root()));
        }
        receipt
    }
}

fn is_native_divide_by_zero(error: &DataFusionError) -> bool {
    matches!(error.find_root(), DataFusionError::ArrowError(error, _)
        if matches!(error.as_ref(), ArrowError::DivideByZero))
}

fn native_public_observations_match(
    actual: &NativeObservation,
    expected: &NativeObservation,
) -> bool {
    // Public collect outcomes can expose different internal preparation stages.
    // Compare every schema available in both; retain all stages in the receipts.
    ["public_stage", "analyzed", "rows"]
        .into_iter()
        .all(|key| actual.fields[key] == expected.fields[key])
        && ["physical_schema", "schemas"].into_iter().all(|key| {
            actual.fields[key].is_null()
                || expected.fields[key].is_null()
                || actual.fields[key] == expected.fields[key]
        })
        && match (&actual.error, &expected.error) {
            (None, None) => true,
            (Some(actual), Some(expected)) => {
                is_native_divide_by_zero(actual) && is_native_divide_by_zero(expected)
            }
            _ => false,
        }
}

async fn native_observation(session: &SessionContext, sql: &str) -> TestResult<NativeObservation> {
    let mut observation = NativeObservation {
        fields: json!({"public_stage":"sql","stage":"sql","analyzed":null,
            "physical_schema":null,"schemas":null,"rows":null}),
        error: None,
    };
    let frame = match session.sql(sql).await {
        Ok(frame) => frame,
        Err(error) => {
            observation.error = Some(error);
            return Ok(observation);
        }
    };
    observation.fields["analyzed"] = json!(format!("{:?}", frame.schema()));
    observation.fields["public_stage"] = json!("collect");
    observation.fields["stage"] = json!("physical");
    let task_ctx = Arc::new(frame.task_ctx());
    let physical = match frame.create_physical_plan().await {
        Ok(physical) => physical,
        Err(error) => {
            observation.error = Some(error);
            return Ok(observation);
        }
    };
    observation.fields["physical_schema"] = json!(format!("{:?}", physical.schema()));
    observation.fields["stage"] = json!("execute");
    match datafusion::physical_plan::collect(physical, task_ctx).await {
        Err(error) => observation.error = Some(error),
        Ok(batches) => {
            let mut rows = Vec::new();
            let mut schemas = Vec::new();
            for batch in batches {
                schemas.push(format!("{:?}", batch.schema()));
                for row in 0..batch.num_rows() {
                    rows.push(
                        batch
                            .columns()
                            .iter()
                            .map(|column| {
                                if column.is_null(row) {
                                    Ok(Value::Null)
                                } else {
                                    Ok(json!(array_value_to_string(column.as_ref(), row)?))
                                }
                            })
                            .collect::<TestResult<Vec<_>>>()?,
                    );
                }
            }
            observation.fields["stage"] = json!("complete");
            observation.fields["public_stage"] = json!("complete");
            observation.fields["schemas"] = json!(schemas);
            observation.fields["rows"] = json!(rows);
        }
    }
    Ok(observation)
}

#[tokio::test]
async fn array_concat_resolution_preserves_native_sql_and_registered_udfs() -> TestResult {
    use arrow::datatypes::DataType;

    let resolution_error = |routine: &str, namespace: &str| {
        json!({
            "error":format!("resolve: External error: analysis error: [UNRESOLVED_ROUTINE] Cannot resolve routine `{routine}` on search path [`system`.`builtin`, `system`.`session`, {namespace}]. SQLSTATE: 42883"),
            "error_stage":"resolve","analyzed_schema":null,"physical_schema":null,
            "analyzed_arrow_schema":null,"physical_arrow_schema":null
        })
    };
    let mut failures = Vec::new();
    for batch_size in [1, 32] {
        let config = SessionConfig::new()
            .with_batch_size(batch_size)
            .with_target_partitions(2);
        let plain = SessionContext::new_with_config(config.clone());
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(config),
            )
            .build(),
        );
        let native_sql = [
            "SELECT array_concat(make_array(1,2),make_array(3)) AS r",
            "SELECT array_concat(CAST(NULL AS INTEGER[]),make_array(1)) AS r",
            "SELECT array_concat(CAST(NULL AS INTEGER[]),make_array(CAST(1/0 AS INT))) AS r",
        ];
        let mut before = Vec::new();
        for sql in native_sql {
            before.push((
                native_observation(&plain, sql).await?,
                native_observation(&session, sql).await?,
            ));
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&calls);
        session.register_udf(create_udf(
            "array_concat_probe",
            vec![],
            DataType::Int32,
            Volatility::Immutable,
            Arc::new(move |_| {
                counted.fetch_add(1, Ordering::SeqCst);
                datafusion::common::exec_err!("array_concat child must not execute")
            }),
        ));
        for ansi in [false, true] {
            for (sql, routine) in [
                (
                    "SELECT array_concat(CAST(NULL AS ARRAY<INT>),array(CAST(1/0 AS INT))) AS r",
                    "array_concat",
                ),
                (
                    "SELECT ARRAY_CONCAT(array(1),array(2)) AS r",
                    "ARRAY_CONCAT",
                ),
                (
                    "SELECT array_concat(CAST(NULL AS ARRAY<INT>),array(array_concat_probe())) AS r",
                    "array_concat",
                ),
            ] {
                let actual = capture_observation(&session, &json!({"sql":sql,"ansi":ansi})).await?;
                let expected = resolution_error(routine, "`datafusion`.`public`");
                let receipt = json!({"sql":sql,"ansi":ansi,"batch_size":batch_size,
                    "actual":actual,"expected":expected,"calls":calls.load(Ordering::SeqCst)});
                eprintln!("array_concat resolution control: {receipt}");
                if actual != expected || calls.load(Ordering::SeqCst) != 0 {
                    failures.push(receipt);
                }
            }
            let sql = "SELECT concat(array(1),array(2)) AS r";
            let actual = capture_observation(&session, &json!({"sql":sql,"ansi":ansi})).await?;
            if actual["rows"] != json!([[["1", "2"]]]) {
                failures
                    .push(json!({"sql":sql,"ansi":ansi,"batch_size":batch_size,"actual":actual}));
            }
        }
        for (index, (sql, (plain_before, configured_before))) in
            native_sql.iter().zip(before).enumerate()
        {
            let plain_after = native_observation(&plain, sql).await?;
            let configured_after = native_observation(&session, sql).await?;
            let receipt = json!({"sql":sql,"batch_size":batch_size,
                "plain_before":plain_before.receipt(),"plain_after":plain_after.receipt(),
                "configured_before":configured_before.receipt(),"configured_after":configured_after.receipt()});
            eprintln!("array_concat native control: {receipt}");
            let expected_outcome = match index {
                0 => plain_before.fields["rows"] == json!([["[1, 2, 3]"]]),
                1 => plain_before.fields["rows"] == json!([["[1]"]]),
                _ => plain_before
                    .error
                    .as_ref()
                    .is_some_and(is_native_divide_by_zero),
            };
            if !expected_outcome
                || !native_public_observations_match(&configured_before, &plain_before)
                || !native_public_observations_match(&configured_after, &plain_after)
                || plain_before.receipt() != plain_after.receipt()
                || configured_before.receipt() != configured_after.receipt()
            {
                failures.push(receipt);
            }
        }
        session.deregister_udf("array_concat");
        for ansi in [false, true] {
            let sql = "SELECT array_concat(array(1),array(2)) AS r";
            let actual = capture_observation(&session, &json!({"sql":sql,"ansi":ansi})).await?;
            let receipt = json!({"sql":sql,"ansi":ansi,"batch_size":batch_size,"actual":actual});
            eprintln!("array_concat absent native routine control: {receipt}");
            if actual != resolution_error("array_concat", "`datafusion`.`public`") {
                failures.push(receipt);
            }
        }
        let counted = Arc::clone(&calls);
        session.register_udf(
            create_udf(
                "array_concat",
                vec![DataType::Utf8],
                DataType::Utf8,
                Volatility::Volatile,
                Arc::new(move |arguments| {
                    counted.fetch_add(1, Ordering::SeqCst);
                    Ok(arguments[0].clone())
                }),
            )
            .with_aliases(["registered_array_concat"]),
        );
        for ansi in [false, true] {
            for routine in ["array_concat", "ARRAY_CONCAT", "registered_array_concat"] {
                calls.store(0, Ordering::SeqCst);
                let sql = format!("SELECT {routine}('host') AS r");
                let actual = capture_observation(&session, &json!({"sql":sql,"ansi":ansi})).await?;
                let receipt = json!({"sql":sql,"ansi":ansi,"batch_size":batch_size,
                    "actual":actual,"calls":calls.load(Ordering::SeqCst)});
                eprintln!("array_concat registered routine control: {receipt}");
                if actual["rows"] != json!([["host"]]) || calls.load(Ordering::SeqCst) != 1 {
                    failures.push(receipt);
                }
            }
        }
    }
    for (catalog, schema, namespace) in [
        ("spark_catalog", "p", "`spark_catalog`.`p`"),
        (
            "catalog`name",
            "schema`name",
            "`catalog``name`.`schema``name`",
        ),
    ] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new().with_default_catalog_and_schema(catalog, schema),
                    ),
            )
            .build(),
        );
        for ansi in [false, true] {
            let sql = "SELECT array_concat(array(1),array(2)) AS r";
            let actual = capture_observation(&session, &json!({"sql":sql,"ansi":ansi})).await?;
            let expected = resolution_error("array_concat", namespace);
            let receipt = json!({"sql":sql,"ansi":ansi,"catalog":catalog,"schema":schema,
                "actual":actual,"expected":expected});
            eprintln!("array_concat namespace control: {receipt}");
            if actual != expected {
                failures.push(receipt);
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
    Ok(())
}

#[tokio::test]
async fn concat_preparation_preserves_native_queries_and_computed_operands() -> TestResult {
    use arrow::datatypes::DataType;
    for batch_size in [1, 32] {
        let config = SessionConfig::new()
            .with_batch_size(batch_size)
            .with_target_partitions(1);
        let plain = SessionContext::new_with_config(config.clone());
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(config),
            )
            .build(),
        );
        let native_sql = [
            "SELECT concat('a',NULL) AS r",
            "SELECT concat(concat('a',NULL),CAST(1/0 AS VARCHAR)) AS r",
            "SELECT concat(CAST(1/0 AS VARCHAR),concat(NULL,'a')) AS r",
        ];
        let mut before = Vec::new();
        for sql in native_sql {
            let plain_before = native_observation(&plain, sql).await?;
            let configured_before = native_observation(&session, sql).await?;
            eprintln!(
                "c06 concat native observation: {}",
                json!({"context":"before","batch_size":batch_size,"sql":sql,
                    "plain":plain_before.receipt(),"configured":configured_before.receipt(),
                    "strict_fields_match":plain_before.fields == configured_before.fields,
                    "public_outcomes_match":native_public_observations_match(
                        &configured_before, &plain_before)})
            );
            before.push((plain_before, configured_before));
        }
        for observation in [&before[0].0, &before[0].1] {
            assert_eq!(observation.fields["rows"], json!([["a"]]));
        }
        for (plain, configured) in &before[1..] {
            for observation in [plain, configured] {
                assert!(
                    observation
                        .error
                        .as_ref()
                        .is_some_and(is_native_divide_by_zero),
                    "{}",
                    observation.receipt()
                );
            }
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        session.register_udf(create_udf(
            "concat_counted",
            vec![DataType::Utf8],
            DataType::Utf8,
            Volatility::Volatile,
            Arc::new(move |args| {
                let arrays = ColumnarValue::values_to_arrays(args)?;
                observed.fetch_add(arrays[0].len(), Ordering::SeqCst);
                Ok(args[0].clone())
            }),
        ));
        session.register_batch(
            "concat_counted_input",
            arrow::record_batch::RecordBatch::try_from_iter([(
                "s",
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), None]))
                    as arrow::array::ArrayRef,
            )])?,
        )?;
        for ansi in [false, true] {
            for (sql, expected_calls, expected_rows) in [
                (
                    "SELECT concat(concat_counted(s),'x') AS r FROM concat_counted_input",
                    3,
                    json!([["ax"], ["bx"], [null]]),
                ),
                (
                    "SELECT concat(concat(concat_counted(s),'x'),'y') AS r FROM concat_counted_input",
                    3,
                    json!([["axy"], ["bxy"], [null]]),
                ),
            ] {
                calls.store(0, Ordering::SeqCst);
                let actual = capture_result(&session, &json!({"sql":sql,"ansi":ansi})).await?;
                assert_eq!(actual["rows"], expected_rows, "{sql}, ANSI={ansi}");
                assert_eq!(
                    calls.load(Ordering::SeqCst),
                    expected_calls,
                    "{sql}, ANSI={ansi}"
                );
            }
        }
        for (sql, (plain_before, configured_before)) in native_sql.iter().zip(before) {
            let plain_after = native_observation(&plain, sql).await?;
            let configured_after = native_observation(&session, sql).await?;
            let before_matches =
                native_public_observations_match(&configured_before, &plain_before);
            let after_matches = native_public_observations_match(&configured_after, &plain_after);
            let receipt = json!({"context":"before_after","batch_size":batch_size,"sql":sql,
                "plain_before":plain_before.receipt(),"plain_after":plain_after.receipt(),
                "configured_before":configured_before.receipt(),
                "configured_after":configured_after.receipt(),
                "strict_fields_match":plain_after.fields == configured_after.fields,
                "public_outcomes_match_before":before_matches,
                "public_outcomes_match_after":after_matches});
            eprintln!("c06 concat native observation: {receipt}");
            assert_eq!(plain_after.receipt(), plain_before.receipt(), "{receipt}");
            assert_eq!(
                configured_after.receipt(),
                configured_before.receipt(),
                "{receipt}"
            );
            assert!(before_matches && after_matches, "{receipt}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn concat_preparation_respects_folding_opt_out_and_reachability() -> TestResult {
    use std::sync::LazyLock;

    use arrow::array::BooleanArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature};
    use delta_arrow_reader_spark_sql::{SparkSqlOptions, plan_query};

    static OPAQUE_CALLS: AtomicUsize = AtomicUsize::new(0);
    static OPAQUE_ROWS: AtomicUsize = AtomicUsize::new(0);
    static ALLOWED_CALLS: AtomicUsize = AtomicUsize::new(0);
    static ALLOWED_ROWS: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct FoldingPermission {
        allowed: bool,
        fail: bool,
    }

    impl ScalarUDFImpl for FoldingPermission {
        fn name(&self) -> &str {
            if self.allowed {
                "concat_fold_allowed"
            } else {
                "concat_fold_opaque"
            }
        }

        fn signature(&self) -> &Signature {
            static SIGNATURE: LazyLock<Signature> =
                LazyLock::new(|| Signature::any(1, Volatility::Immutable));
            &SIGNATURE
        }

        fn supports_constant_folding(&self) -> bool {
            self.allowed
        }

        fn return_type(&self, args: &[DataType]) -> datafusion::common::Result<DataType> {
            Ok(args[0].clone())
        }

        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::common::Result<ColumnarValue> {
            let (calls, rows) = if self.allowed {
                (&ALLOWED_CALLS, &ALLOWED_ROWS)
            } else {
                (&OPAQUE_CALLS, &OPAQUE_ROWS)
            };
            calls.fetch_add(1, Ordering::SeqCst);
            rows.fetch_add(args.number_rows, Ordering::SeqCst);
            if self.fail && self.allowed {
                Err(ArrowError::DivideByZero.into())
            } else if self.fail {
                datafusion::common::exec_err!("opaque CONCAT operand was invoked")
            } else {
                Ok(args.args[0].clone())
            }
        }
    }

    fn counters() -> [usize; 4] {
        [&OPAQUE_CALLS, &OPAQUE_ROWS, &ALLOWED_CALLS, &ALLOWED_ROWS]
            .map(|counter| counter.load(Ordering::SeqCst))
    }

    fn reset() {
        for counter in [&OPAQUE_CALLS, &OPAQUE_ROWS, &ALLOWED_CALLS, &ALLOWED_ROWS] {
            counter.store(0, Ordering::SeqCst);
        }
    }

    async fn assert_null(
        session: &SessionContext,
        sql: &str,
        ansi: bool,
        rows: usize,
        data_type: DataType,
    ) -> TestResult {
        reset();
        let query = plan_query(
            session,
            sql,
            &SparkSqlOptions {
                ansi_mode: ansi,
                session_timezone: "UTC".into(),
                ..Default::default()
            },
        )
        .await?;
        assert_eq!(counters(), [0; 4], "after planning: {sql}, ANSI={ansi}");
        assert_eq!(query.column_names, ["r"]);
        let field = query.logical_plan.schema().field(0);
        assert_eq!(field.data_type(), &data_type);
        assert!(field.is_nullable());
        assert!(field.metadata().is_empty());
        let physical = query.create_physical_plan(session).await?;
        let expected = [0; 4];
        assert_eq!(counters(), expected, "after physical: {sql}, ANSI={ansi}");
        let schema = Arc::new(Schema::new(vec![Field::new("r", data_type, true)]));
        assert_eq!(physical.schema(), schema);
        let batches = datafusion::physical_plan::collect(physical, session.task_ctx()).await?;
        assert_eq!(counters(), expected, "after collect: {sql}, ANSI={ansi}");
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            rows
        );
        for batch in batches {
            assert_eq!(batch.schema(), schema);
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch.column(0).null_count(), batch.num_rows());
        }
        eprintln!(
            "c06 concat optout NULL control: {}",
            json!({
                "sql":sql,"ansi":ansi,"after_planning":[0,0,0,0],
                "after_physical":expected,"after_collect":counters(),"rows":rows
            })
        );
        Ok(())
    }

    async fn assert_required_error(session: &SessionContext, sql: &str, ansi: bool) -> TestResult {
        reset();
        let query = plan_query(
            session,
            sql,
            &SparkSqlOptions {
                ansi_mode: ansi,
                session_timezone: "UTC".into(),
                ..Default::default()
            },
        )
        .await?;
        assert_eq!(counters(), [0; 4], "after planning: {sql}");
        assert_eq!(query.column_names, ["r"]);
        assert_eq!(
            query.logical_plan.schema().field(0).data_type(),
            &DataType::Utf8
        );
        match query.create_physical_plan(session).await {
            Err(error) => assert!(is_native_divide_by_zero(&error), "{sql}: {error:?}"),
            Ok(_) => return Err(format!("required constant error disappeared: {sql}").into()),
        }
        assert_eq!(counters(), [0; 4], "{sql}");
        Ok(())
    }

    let session = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_target_partitions(1)
                        .with_batch_size(32),
                ),
        )
        .build(),
    );
    session.register_batch(
        "concat_optout_flags",
        arrow::record_batch::RecordBatch::try_from_iter([(
            "flag",
            Arc::new(BooleanArray::from(vec![false, false])) as arrow::array::ArrayRef,
        )])?,
    )?;
    session.register_udf(ScalarUDF::from(FoldingPermission {
        allowed: true,
        fail: false,
    }));
    let mut original_null_controls = 0;
    for fail in [false, true] {
        session.register_udf(
            ScalarUDF::from(FoldingPermission {
                allowed: false,
                fail,
            })
            .with_aliases(["concat_fold_opaque_alias"]),
        );
        for ansi in [false, true] {
            for argument in ["concat_fold_opaque(1/1)", "concat_fold_opaque(1+2)"] {
                for operands in [format!("NULL,{argument}"), format!("{argument},NULL")] {
                    assert_null(
                        &session,
                        &format!("SELECT concat({operands}) AS r"),
                        ansi,
                        1,
                        DataType::Utf8,
                    )
                    .await?;
                    original_null_controls += 1;
                }
            }
            for argument in [
                "concat_fold_opaque_alias(1/1)",
                "concat_fold_opaque(CASE WHEN true THEN 1 ELSE 1/0 END)",
            ] {
                for operands in [format!("NULL,{argument}"), format!("{argument},NULL")] {
                    assert_null(
                        &session,
                        &format!("SELECT concat({operands}) AS r"),
                        ansi,
                        1,
                        DataType::Utf8,
                    )
                    .await?;
                }
            }
            for expression in [
                "concat(concat(NULL,concat_fold_opaque(1/1)),'x')",
                "concat('x',concat(concat_fold_opaque(1/1),NULL))",
            ] {
                assert_null(
                    &session,
                    &format!("SELECT {expression} AS r"),
                    ansi,
                    1,
                    DataType::Utf8,
                )
                .await?;
            }
            for operands in [
                "NULL,concat_fold_opaque(1/0)",
                "concat_fold_opaque(1/0),NULL",
            ] {
                assert_null(&session, &format!("SELECT CASE WHEN false THEN concat({operands}) ELSE CAST(NULL AS STRING) END AS r"),
                    ansi, 1, DataType::Utf8).await?;
                assert_null(&session, &format!("SELECT CASE WHEN flag THEN concat({operands}) ELSE CAST(NULL AS STRING) END AS r FROM concat_optout_flags"),
                    ansi, 2, DataType::Utf8).await?;
                // The whole opaque operand is discarded with the known NULL.
                // Its child is not a required fold on this path.
                assert_null(
                    &session,
                    &format!("SELECT concat({operands}) AS r"),
                    ansi,
                    1,
                    DataType::Utf8,
                )
                .await?;
            }
            // These mixed native-extension contexts retain the old host NULL
            // path, including independent constants. They are not Spark oracles.
            for operands in [
                "NULL,concat_fold_opaque(1/1),CAST(1/0 AS STRING)",
                "CAST(1/0 AS STRING),concat_fold_opaque(1/1),NULL",
                "NULL,concat_fold_allowed(concat_fold_opaque(1/1)),CAST(1/0 AS STRING)",
                "CAST(1/0 AS STRING),concat_fold_allowed(concat_fold_opaque(1/1)),NULL",
            ] {
                assert_null(
                    &session,
                    &format!("SELECT concat({operands}) AS r"),
                    ansi,
                    1,
                    DataType::Utf8,
                )
                .await?;
            }
            for (sql, rows) in [
                (
                    "SELECT flag FROM (SELECT flag,concat(NULL,concat_fold_opaque(1/0)) AS unused FROM concat_optout_flags) t",
                    json!([["false"], ["false"]]),
                ),
                (
                    "SELECT concat(NULL,concat_fold_opaque(1/0)) AS r FROM concat_optout_flags LIMIT 0",
                    json!([]),
                ),
            ] {
                reset();
                let actual = capture_result(&session, &json!({"sql":sql,"ansi":ansi})).await?;
                assert_eq!(actual["rows"], rows, "{sql}, ANSI={ansi}");
                assert_eq!(counters(), [0; 4], "{sql}, ANSI={ansi}");
            }
            for operands in ["NULL,CAST(1/0 AS STRING)", "CAST(1/0 AS STRING),NULL"] {
                let sql = format!("SELECT concat({operands}) AS r");
                if ansi {
                    assert_required_error(&session, &sql, ansi).await?;
                } else {
                    assert_null(&session, &sql, ansi, 1, DataType::Utf8).await?;
                }
            }
            for operands in [
                "CAST(NULL AS ARRAY<DOUBLE>),concat_fold_opaque(array(1/1))",
                "concat_fold_opaque(array(1/1)),CAST(NULL AS ARRAY<DOUBLE>)",
            ] {
                assert_null(
                    &session,
                    &format!("SELECT concat({operands}) AS r"),
                    ansi,
                    1,
                    DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true))),
                )
                .await?;
            }
            let argument = "concat_fold_opaque(CASE WHEN flag THEN 1/0 ELSE 1 END)";
            for operands in [format!("NULL,{argument}"), format!("{argument},NULL")] {
                assert_null(
                    &session,
                    &format!("SELECT concat({operands}) AS r FROM concat_optout_flags"),
                    ansi,
                    2,
                    DataType::Utf8,
                )
                .await?;
            }
            let argument = "concat_fold_opaque(CAST(CAST(1/1 AS STRING) AS BINARY))";
            for operands in [
                format!("CAST(NULL AS BINARY),{argument}"),
                format!("{argument},CAST(NULL AS BINARY)"),
            ] {
                assert_null(
                    &session,
                    &format!("SELECT concat({operands}) AS r"),
                    ansi,
                    1,
                    DataType::Binary,
                )
                .await?;
            }
        }
    }
    assert_eq!(original_null_controls, 16);

    for fail in [false, true] {
        session.register_udf(ScalarUDF::from(FoldingPermission {
            allowed: true,
            fail,
        }));
        for operands in [
            "NULL,concat_fold_opaque(concat_fold_allowed(1/1))",
            "concat_fold_opaque(concat_fold_allowed(1/1)),NULL",
            "NULL,concat_fold_opaque(1/1),concat_fold_allowed(1/1)",
            "concat_fold_allowed(1/1),concat_fold_opaque(1/1),NULL",
        ] {
            let sql = format!("SELECT concat({operands}) AS r");
            // supports_constant_folding permits argument folding; it does not
            // require visiting an argument discarded by a known NULL parent.
            for ansi in [false, true] {
                assert_null(&session, &sql, ansi, 1, DataType::Utf8).await?;
            }
        }
    }

    // Standalone Spark prechecks already invoke the opt-out UDF for division.
    // Keep that existing behavior outside the CONCAT repair.
    session.register_udf(ScalarUDF::from(FoldingPermission {
        allowed: false,
        fail: false,
    }));
    for ansi in [false, true] {
        reset();
        let query = plan_query(
            &session,
            "SELECT concat_fold_opaque(1/1) AS r",
            &SparkSqlOptions {
                ansi_mode: ansi,
                ..Default::default()
            },
        )
        .await?;
        assert_eq!(counters(), [0; 4]);
        let physical = query.create_physical_plan(&session).await?;
        assert_eq!(counters(), [1, 1, 0, 0]);
        let batches = datafusion::physical_plan::collect(physical, session.task_ctx()).await?;
        assert_eq!(counters(), [2, 2, 0, 0]);
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );
        assert_eq!(
            array_value_to_string(batches[0].column(0).as_ref(), 0)?,
            "1.0"
        );
    }
    Ok(())
}

#[test]
fn native_public_parity_rejects_unrelated_errors_and_changed_fields() {
    // Synthetic receipts exercise comparison rules without running a native query.
    let fields = json!({"public_stage":"collect","stage":"physical",
        "analyzed":"complete analyzed schema","physical_schema":"complete physical schema",
        "schemas":["complete batch schema"],"rows":null});
    let expected = NativeObservation {
        fields: fields.clone(),
        error: Some(ArrowError::DivideByZero.into()),
    };
    let mut actual = NativeObservation {
        fields: fields.clone(),
        error: Some(
            DataFusionError::from(ArrowError::DivideByZero)
                .context("preserve_spark_expression_errors"),
        ),
    };
    assert!(native_public_observations_match(&actual, &expected));
    assert_ne!(actual.receipt()["error"], expected.receipt()["error"]);
    for (key, value) in [
        ("public_stage", json!("sql")),
        ("analyzed", json!("changed schema")),
        ("physical_schema", json!("unexpected schema")),
        ("schemas", json!(["changed batch schema"])),
        ("rows", json!([[null]])),
    ] {
        actual.fields = fields.clone();
        actual.fields[key] = value;
        assert!(
            !native_public_observations_match(&actual, &expected),
            "{key}"
        );
    }
    actual.fields = fields.clone();
    for error in [
        None,
        Some(DataFusionError::Execution("Divide by zero error".into())),
        Some(ArrowError::ComputeError("Divide by zero error".into()).into()),
    ] {
        actual.error = error;
        assert!(!native_public_observations_match(&actual, &expected));
        if actual.error.is_some() {
            assert!(!native_public_observations_match(&actual, &actual));
        }
    }
}

#[tokio::test]
async fn concat_constant_operands_match_spark_before_null_folding() -> TestResult {
    let mut keys = HashSet::new();
    let mut differences = Vec::new();
    for (fixture, references) in [
        (
            include_str!("fixtures/concat-constant-errors-spark.jsonl"),
            8,
        ),
        (
            include_str!("fixtures/concat-constant-errors-controls-spark.jsonl"),
            14,
        ),
        (
            include_str!("fixtures/concat-constant-errors-descendants-spark.jsonl"),
            8,
        ),
    ] {
        let records = fixture
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(records.len(), references + 1);
        assert_eq!(records[0]["spark_version"], "4.2.0");
        for representation in 0..3 {
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
                register_inputs(&session, &records[0], representation, batch_size)?;
                for case in &records[1..] {
                    assert!(keys.insert((
                        case["id"].as_str().ok_or("missing ID")?.to_owned(),
                        case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                        representation,
                        batch_size,
                    )));
                    let actual = capture_observation(&session, case).await?;
                    let matches = matches_reference(case, &actual);
                    let observation = json!({"id":case["id"],"sql":case["sql"],
                    "ansi":case["ansi"],"representation":representation,
                    "batch_size":batch_size,"expected":case["expected"],
                    "actual":actual,"matches_reference":matches});
                    eprintln!("c06 concat constant observation: {observation}");
                    if !matches {
                        differences.push(observation);
                    }
                }
            }
        }
    }
    assert_eq!(keys.len(), 180);
    eprintln!(
        "c06 concat constant summary: {}",
        json!({
        "observations":keys.len(),"strict_failure_count":differences.len(),
        "full_compatibility_pass":differences.is_empty()})
    );
    assert!(differences.is_empty(), "{differences:#?}");
    Ok(())
}

#[test]
fn constant_error_check_rejects_values_fields_and_unrelated_errors() -> TestResult {
    let case = include_str!("fixtures/concat-constant-errors-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .find(|case| case["ansi"] == true)
        .ok_or("missing ANSI reference")?;
    // Synthetic matcher controls, not a native capture.
    let actual = json!({"analyzed_schema":case["expected"]["analyzed_schema"],
        "analyzed_arrow_schema":"completed analyzed schema",
        "physical_schema":null,"physical_arrow_schema":null,
        "error_stage":"physical","error":"physical: Arrow error: Divide by zero error"});
    assert!(matches_reference(&case, &actual));
    for (key, value) in [
        ("error", json!("physical: arbitrary failure")),
        ("error_stage", json!("execute")),
        ("rows", json!([[null]])),
        ("physical_schema", json!({"type":"struct","fields":[]})),
        ("analyzed_arrow_schema", Value::Null),
    ] {
        let mut changed = actual.clone();
        changed[key] = value;
        assert!(!matches_reference(&case, &changed), "{key}: {changed}");
    }
    for key in ["name", "type", "nullable", "metadata"] {
        let mut changed = actual.clone();
        changed["analyzed_schema"]["fields"][0][key] = json!("wrong");
        assert!(!matches_reference(&case, &changed), "{key}: {changed}");
    }
    Ok(())
}
