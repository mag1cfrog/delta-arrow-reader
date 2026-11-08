//! Substring evaluates each required operand once and skips NULL-discarded rows.

use std::sync::{Arc, Mutex};

use arrow::array::Int32Array;
use arrow::datatypes::DataType;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[allow(
    dead_code,
    reason = "This test uses input registration from the shared observer."
)]
#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, c06_receipts, register_inputs_with_receipts};

#[expect(
    dead_code,
    reason = "This target reuses only the accepted native async producer."
)]
#[path = "../vendor/sail/crates/sail-plan/src/test_utils/regexp_literal_producer.rs"]
mod literal_producer;

fn row_visits(observed: &[Arc<Mutex<Vec<i32>>>; 3]) -> TestResult<Value> {
    let rows = observed
        .iter()
        .map(|rows| {
            Ok(rows
                .lock()
                .map_err(|_| "poisoned substring row recorder")?
                .clone())
        })
        .collect::<TestResult<Vec<_>>>()?;
    let mut sorted = rows.clone();
    for values in &mut sorted {
        values.sort_unstable();
    }
    Ok(json!({"row_visits":rows,"sorted_row_visits":sorted,
        "row_visit_counts":rows.iter().map(Vec::len).collect::<Vec<_>>(),
        "invocation_count":{"status":"unavailable","reason":"Existing recorder retains row visits, not call boundaries."},
        "argument_fields":{"status":"unavailable","reason":"create_udf callback exposes ColumnarValues without Fields."},
        "return_field":{"status":"unavailable","reason":"create_udf callback exposes values without a return Field."}}))
}

async fn check_counted(
    session: &SessionContext,
    sql: &str,
    ansi_mode: bool,
    observed: &[Arc<Mutex<Vec<i32>>>; 3],
    expected: [Vec<i32>; 3],
    row_count: usize,
    representation: usize,
) -> TestResult {
    for rows in observed {
        rows.lock()
            .map_err(|_| "poisoned substring row recorder")?
            .clear();
    }
    let before_resolve = row_visits(observed)?;
    let query = plan_query(
        session,
        sql,
        &SparkSqlOptions {
            ansi_mode,
            ..Default::default()
        },
    )
    .await?;
    let after_resolve = row_visits(observed)?;
    let mut receipt = c06_receipts::new("public_physical_collect");
    receipt["public_names"] = json!(query.column_names);
    receipt["analyzed"] = c06_receipts::schema(query.logical_plan.schema().as_arrow());
    let physical = query.create_physical_plan(session).await?;
    let after_physical = row_visits(observed)?;
    receipt["before_collect"] = c06_receipts::schema(physical.schema().as_ref());
    receipt["physical"] = c06_receipts::schema(physical.schema().as_ref());
    receipt["physical_plan"] = json!(format!("{physical:?}"));
    for rows in observed {
        assert!(
            rows.lock()
                .map_err(|_| "poisoned substring row recorder")?
                .is_empty(),
            "planning evaluated a volatile substring operand: {sql}"
        );
    }
    let batches = collect(physical, session.task_ctx()).await?;
    let after_collect = row_visits(observed)?;
    receipt["output"] = c06_receipts::batches(&batches, string_binary::encode_value)?;
    receipt["callbacks"] = json!({"before_resolve":before_resolve,"after_resolve":after_resolve,
        "after_physical":after_physical,"after_collect_or_error":after_collect,"raw_order":"per_operand_row_visit_order"});
    c06_receipts::finish(&mut receipt);
    c06_receipts::emit(
        "native",
        json!({"control":"counted_success","sql":sql,"ansi":ansi_mode,
        "representation":representation,"batch_size":session.state().config().batch_size(),
        "target_partitions":session.state().config().target_partitions()}),
        receipt,
    );
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        row_count
    );
    for (rows, expected) in observed.iter().zip(expected) {
        let mut rows = rows
            .lock()
            .map_err(|_| "poisoned substring row recorder")?
            .clone();
        rows.sort_unstable();
        assert_eq!(rows, expected, "{sql}, ANSI={ansi_mode}");
    }
    Ok(())
}

#[tokio::test]
async fn substring_evaluates_each_needed_operand_once() -> TestResult {
    let header: Value = serde_json::from_str(
        include_str!("fixtures/substring-arguments-spark.jsonl")
            .lines()
            .next()
            .ok_or("missing fixture header")?,
    )?;
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
            let providers =
                register_inputs_with_receipts(&session, &header, representation, batch_size)
                    .await?;
            for provider in providers.as_array().ok_or("missing provider receipts")? {
                c06_receipts::emit(
                    "provider",
                    json!({"family":"counted","fixture":"substring-arguments-spark.jsonl",
                    "representation":representation,"batch_size":batch_size,"target_partitions":2}),
                    provider.clone(),
                );
            }
            let string_type = match representation {
                0 => DataType::Utf8,
                1 => DataType::LargeUtf8,
                _ => DataType::Utf8View,
            };
            let binary_type = match representation {
                0 => DataType::Binary,
                1 => DataType::LargeBinary,
                _ => DataType::BinaryView,
            };
            let observed: [Arc<Mutex<Vec<i32>>>; 3] =
                std::array::from_fn(|_| Arc::new(Mutex::new(Vec::new())));
            for (name, index, data_type) in [
                ("count_substring_source", 0, string_type.clone()),
                ("count_substring_binary", 0, binary_type),
                ("count_substring_position", 1, string_type.clone()),
                ("count_substring_length", 2, string_type),
            ] {
                let rows = Arc::clone(&observed[index]);
                let callback =
                    create_udf(
                        name,
                        vec![DataType::Int32, data_type.clone()],
                        data_type.clone(),
                        Volatility::Volatile,
                        Arc::new(move |args| {
                            let arrays = ColumnarValue::values_to_arrays(args)?;
                            let ids = arrays[0].as_any().downcast_ref::<Int32Array>().ok_or_else(
                                || {
                                    datafusion::common::DataFusionError::Internal(
                                        "substring counted operand expected INT row IDs".into(),
                                    )
                                },
                            )?;
                            rows.lock().unwrap().extend(ids.values().iter().copied());
                            Ok(ColumnarValue::Array(Arc::clone(&arrays[1])))
                        }),
                    );
                c06_receipts::emit(
                    "callback_declaration",
                    json!({"control":"counted","name":name,
                    "representation":representation,"batch_size":batch_size,"target_partitions":2}),
                    json!({"signature":format!("{:?}",callback.signature()),"return_type":format!("{data_type:?}"),
                        "volatility":format!("{:?}",callback.signature().volatility),
                        "supports_constant_folding":callback.inner().supports_constant_folding(),
                        "argument_fields":{"status":"unavailable","reason":"create_udf declares argument types, not Fields."}}),
                );
                session.register_udf(callback);
            }
            for ansi_mode in [true, false] {
                for (source, source_function) in [
                    ("s", "count_substring_source"),
                    ("b", "count_substring_binary"),
                ] {
                    for (relation, expected_ids) in [
                        ("substring_text WHERE id<=6", vec![1, 2, 3, 4, 5, 6]),
                        ("substring_text WHERE id=5", vec![5]),
                        ("substring_text WHERE id=6", vec![6]),
                        ("substring_text_empty", vec![]),
                    ] {
                        let sql = format!(
                            "SELECT substring({source_function}(id,{source}),count_substring_position(id,p),count_substring_length(id,n)) AS r FROM {relation}"
                        );
                        let expected = [
                            expected_ids.clone(),
                            expected_ids.iter().copied().filter(|id| *id != 5).collect(),
                            expected_ids
                                .iter()
                                .copied()
                                .filter(|id| *id != 5 && *id != 6)
                                .collect(),
                        ];
                        check_counted(
                            &session,
                            &sql,
                            ansi_mode,
                            &observed,
                            expected,
                            expected_ids.len(),
                            representation,
                        )
                        .await?;
                    }
                    for length_type in ["INT", "STRING"] {
                        let sql = format!(
                            "SELECT substring({source_function}(id,{source}),count_substring_position(id,p),CAST(NULL AS {length_type})) AS r FROM substring_text"
                        );
                        check_counted(
                            &session,
                            &sql,
                            ansi_mode,
                            &observed,
                            [vec![], vec![], vec![]],
                            12,
                            representation,
                        )
                        .await?;
                    }
                }
                for (sql, expected, row_count) in [
                    (
                        "SELECT substring(count_substring_source(id,CAST(NULL AS STRING)),count_substring_position(id,'bad'),count_substring_length(id,'bad')) AS r FROM substring_text WHERE id=1",
                        [vec![1], vec![], vec![]],
                        1,
                    ),
                    (
                        "SELECT substring(count_substring_source(id,s),count_substring_position(id,CAST(NULL AS STRING)),count_substring_length(id,'bad')) AS r FROM substring_text WHERE id=1",
                        [vec![1], vec![1], vec![]],
                        1,
                    ),
                    (
                        "SELECT CASE WHEN false THEN substring(count_substring_source(id,s),count_substring_position(id,'bad'),count_substring_length(id,'bad')) ELSE 'ok' END AS r FROM substring_text WHERE id=1",
                        [vec![], vec![], vec![]],
                        1,
                    ),
                    (
                        "SELECT COALESCE('ok',substring(count_substring_source(id,s),count_substring_position(id,'bad'),count_substring_length(id,'bad'))) AS r FROM substring_text WHERE id=1",
                        [vec![], vec![], vec![]],
                        1,
                    ),
                    (
                        "SELECT substring(count_substring_source(id,s),count_substring_position(id,p),count_substring_length(id,n)) AS r FROM VALUES (1,'abcdef','2','3'),(2,CAST(NULL AS STRING),'bad','bad'),(3,'abcdef',CAST(NULL AS STRING),'bad') AS t(id,s,p,n)",
                        [vec![1, 2, 3], vec![1, 3], vec![1]],
                        3,
                    ),
                ] {
                    check_counted(
                        &session,
                        sql,
                        ansi_mode,
                        &observed,
                        expected,
                        row_count,
                        representation,
                    )
                    .await?;
                }
            }
            // A failing later child must not trigger a replay of earlier volatile operands.
            for rows in &observed {
                rows.lock().unwrap().clear();
            }
            let before_resolve = row_visits(&observed)?;
            let query = plan_query(
                &session,
                "SELECT substring(count_substring_source(id,s),count_substring_position(id,p),count_substring_length(id,n)) AS r FROM substring_text WHERE id=7",
                &SparkSqlOptions { ansi_mode: true, ..Default::default() },
            )
            .await?;
            let after_resolve = row_visits(&observed)?;
            let mut receipt = c06_receipts::new("public_physical_collect");
            receipt["public_names"] = json!(query.column_names);
            receipt["analyzed"] = c06_receipts::schema(query.logical_plan.schema().as_arrow());
            let physical = query.create_physical_plan(&session).await?;
            let after_physical = row_visits(&observed)?;
            receipt["before_collect"] = c06_receipts::schema(physical.schema().as_ref());
            receipt["physical"] = c06_receipts::schema(physical.schema().as_ref());
            receipt["physical_plan"] = json!(format!("{physical:?}"));
            assert!(observed.iter().all(|rows| rows.lock().unwrap().is_empty()));
            let error = collect(physical, session.task_ctx()).await.unwrap_err();
            receipt["error"] = c06_receipts::failure(&error, "execute");
            receipt["callbacks"] = json!({"before_resolve":before_resolve,"after_resolve":after_resolve,
                "after_physical":after_physical,"after_collect_or_error":row_visits(&observed)?,
                "raw_order":"per_operand_row_visit_order"});
            c06_receipts::finish(&mut receipt);
            c06_receipts::emit(
                "native",
                json!({"control":"counted_later_error","ansi":true,
                "sql":"SELECT substring(count_substring_source(id,s),count_substring_position(id,p),count_substring_length(id,n)) AS r FROM substring_text WHERE id=7",
                "representation":representation,"batch_size":batch_size,"target_partitions":2}),
                receipt,
            );
            let error = error.to_string();
            assert!(
                error.contains("Cannot cast string 'bad' to value of Int32 type"),
                "{error}"
            );
            for rows in &observed {
                assert_eq!(*rows.lock().unwrap(), vec![7]);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn substring_keeps_native_async_descendant_values_fields_and_invocations() -> TestResult {
    use std::sync::atomic::Ordering;

    use arrow::compute::concat_batches;
    use literal_producer::{Kind, producer};

    let header: Value = serde_json::from_str(
        include_str!("fixtures/substring-arguments-spark.jsonl")
            .lines()
            .next()
            .ok_or("missing fixture header")?,
    )?;
    for representation in 0..3 {
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
            let providers =
                register_inputs_with_receipts(&session, &header, representation, batch_size)
                    .await?;
            for provider in providers.as_array().ok_or("missing provider receipts")? {
                c06_receipts::emit(
                    "provider",
                    json!({"family":"async","fixture":"substring-arguments-spark.jsonl",
                    "representation":representation,"batch_size":batch_size,"target_partitions":1}),
                    provider.clone(),
                );
            }
            let source_schema = session.table_provider("substring_text").await?.schema();
            let source_field = Arc::clone(&source_schema.fields()[source_schema.index_of("s")?]);
            for ansi_mode in [true, false] {
                for (length, reached_async) in [
                    ("2", true),
                    ("CAST(NULL AS INT)", false),
                    ("CAST(NULL AS STRING)", false),
                ] {
                    let (callback, counts) = producer(Kind::AsyncFold);
                    assert!(callback.as_async().is_some());
                    let declaration = json!({"signature":format!("{:?}",callback.signature()),
                        "volatility":format!("{:?}",callback.signature().volatility),
                        "supports_constant_folding":callback.inner().supports_constant_folding(),
                        "source_field":c06_receipts::field(&source_field),
                        "outer_sync_entry_attempts":{"status":"unobservable","reason":"Native AsyncScalarUDF wrapper exposes no synchronous-entry counter."}});
                    session.register_udf(callback);
                    let options = SparkSqlOptions {
                        ansi_mode,
                        ..Default::default()
                    };
                    let control_sql = format!(
                        "SELECT id,substring(s,2,{length}) AS r FROM substring_text ORDER BY id"
                    );
                    let control = plan_query(&session, &control_sql, &options).await?;
                    let control_plan = control.create_physical_plan(&session).await?;
                    let mut plain_receipt = c06_receipts::new("public_physical_collect");
                    plain_receipt["public_names"] = json!(control.column_names);
                    plain_receipt["analyzed"] =
                        c06_receipts::schema(control.logical_plan.schema().as_arrow());
                    plain_receipt["before_collect"] =
                        c06_receipts::schema(control_plan.schema().as_ref());
                    plain_receipt["physical"] =
                        c06_receipts::schema(control_plan.schema().as_ref());
                    plain_receipt["physical_plan"] = json!(format!("{control_plan:?}"));
                    let control_schema = control_plan.schema();
                    let control_batches = collect(control_plan, session.task_ctx()).await?;
                    plain_receipt["output"] =
                        c06_receipts::batches(&control_batches, string_binary::encode_value)?;
                    c06_receipts::finish(&mut plain_receipt);
                    c06_receipts::emit(
                        "native",
                        json!({"control":"async_plain","sql":control_sql,"ansi":ansi_mode,
                        "representation":representation,"batch_size":batch_size,"target_partitions":1}),
                        plain_receipt,
                    );
                    let expected = concat_batches(&control_schema, &control_batches)?;
                    let sql =
                        control_sql.replace("substring(s,", "substring(literal_async_fold(s),");
                    let before_resolve = counts.snapshot();
                    let query = plan_query(&session, &sql, &options)
                        .await
                        .map_err(|error| {
                            format!("async substring resolve: {sql}, ANSI={ansi_mode}: {error:?}")
                        })?;
                    assert_eq!(query.column_names, control.column_names, "{sql}");
                    assert_eq!(
                        query.column_names,
                        vec!["id".to_owned(), "r".to_owned()],
                        "{sql}"
                    );
                    assert_eq!(
                        string_binary::capture_schema(
                            query.logical_plan.schema().fields(),
                            Some(&query.column_names)
                        )?,
                        string_binary::capture_schema(
                            control.logical_plan.schema().fields(),
                            Some(&control.column_names)
                        )?,
                        "{sql}"
                    );
                    let result = query.logical_plan.schema().field(1);
                    assert_eq!(result.data_type(), source_field.data_type(), "{sql}");
                    assert!(
                        result.is_nullable() && result.metadata().is_empty(),
                        "{sql}"
                    );
                    // These receipts count inner producer calls. The native wrapper's
                    // rejected sync entries are not exposed by this product helper.
                    assert_eq!(counts.calls.load(Ordering::SeqCst), 0, "{sql}");
                    let after_resolve = counts.snapshot();
                    let mut receipt = c06_receipts::new("public_physical_collect");
                    receipt["public_names"] = json!(query.column_names);
                    receipt["analyzed"] =
                        c06_receipts::schema(query.logical_plan.schema().as_arrow());
                    counts.phase.store(1, Ordering::SeqCst);
                    let physical = query
                        .create_physical_plan(&session)
                        .await
                        .map_err(|error| {
                            format!(
                                "async substring physical: {sql}, analyzed={:?}: {error:?}",
                                query.logical_plan.schema()
                            )
                        })?;
                    let after_physical = counts.snapshot();
                    let actual_schema = physical.schema();
                    receipt["before_collect"] = c06_receipts::schema(actual_schema.as_ref());
                    receipt["physical"] = c06_receipts::schema(actual_schema.as_ref());
                    receipt["physical_plan"] = json!(format!("{physical:?}"));
                    assert_eq!(actual_schema.field(1).name(), "r", "{sql}");
                    assert_eq!(actual_schema, control_schema, "{sql}");
                    assert_eq!(counts.calls.load(Ordering::SeqCst), 0, "{sql}");
                    counts.phase.store(2, Ordering::SeqCst);
                    let batches = collect(physical, session.task_ctx()).await.map_err(|error| {
                        format!("async substring execute: {sql}, analyzed={:?}, physical={actual_schema:?}: {error:?}", query.logical_plan.schema())
                    })?;
                    for batch in &batches {
                        assert_eq!(batch.schema(), actual_schema, "{sql}");
                    }
                    let actual = concat_batches(&actual_schema, &batches)?;
                    assert_eq!(actual, expected, "{sql}");
                    assert_eq!(actual.num_rows(), 12, "{sql}");
                    let snapshot = counts.snapshot();
                    receipt["output"] =
                        c06_receipts::batches(&batches, string_binary::encode_value)?;
                    receipt["callbacks"] = json!({"declaration":declaration,"before_resolve":before_resolve,
                        "after_resolve":after_resolve,"after_physical":after_physical,"after_collect_or_error":snapshot,
                        "event_order":"raw_producer_event_order"});
                    c06_receipts::finish(&mut receipt);
                    c06_receipts::emit(
                        "native",
                        json!({"control":"async","sql":sql,"ansi":ansi_mode,
                        "representation":representation,"batch_size":batch_size,"target_partitions":1}),
                        receipt,
                    );
                    let events = snapshot["events"]
                        .as_array()
                        .ok_or("missing async events")?;
                    assert_eq!(counts.calls.load(Ordering::SeqCst), events.len(), "{sql}");
                    assert_eq!(
                        events.len(),
                        if reached_async {
                            12_usize.div_ceil(batch_size)
                        } else {
                            0
                        },
                        "{sql}"
                    );
                    let mut rows = 0;
                    for event in events {
                        assert_eq!(event["phase"], 2, "{sql}: {event}");
                        let row_count = event["number_rows"]
                            .as_u64()
                            .ok_or("missing async row count")?;
                        assert!(
                            row_count > 0 && row_count <= batch_size as u64,
                            "{sql}: {event}"
                        );
                        rows += row_count;
                        assert_eq!(
                            event["arg_fields"],
                            format!("{:?}", [Arc::clone(&source_field)]),
                            "{sql}"
                        );
                        assert_eq!(event["return_field"], format!("{source_field:?}"), "{sql}");
                        assert!(
                            event["arguments"]
                                .as_str()
                                .is_some_and(|value| !value.is_empty()),
                            "{sql}"
                        );
                    }
                    // Existing native simplification removes known-NULL calls before
                    // async extraction. Non-NULL calls execute the source once per row.
                    assert_eq!(rows, if reached_async { 12 } else { 0 }, "{sql}");
                    if reached_async {
                        assert_eq!(
                            arrow::util::display::array_value_to_string(
                                actual.column(1).as_ref(),
                                0
                            )?,
                            "bc",
                            "{sql}"
                        );
                    } else {
                        assert_eq!(actual.column(1).null_count(), 12, "{sql}");
                    }
                }
            }
        }
    }
    Ok(())
}
