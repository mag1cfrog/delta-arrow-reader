//! Independent Spark extrema rows and schemas, retaining the original float bits.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Float32Type, Float64Type};
use arrow::util::display::array_value_to_string;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_inputs::register_recorded_inputs;
use spark_results::TestResult;

async fn native_snapshot(session: &SessionContext, sql: &str) -> TestResult<Value> {
    let frame = session.sql(sql).await?;
    let schema = frame.schema().as_arrow().clone();
    let physical = frame.create_physical_plan().await?;
    eprintln!(
        "native {sql}: {}",
        datafusion::physical_plan::displayable(physical.as_ref()).indent(true)
    );
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| encode_raw(column.as_ref(), row))
                    .collect::<TestResult<Vec<_>>>()?,
            );
        }
    }
    rows.sort_by_key(|row| json!(row).to_string());
    Ok(json!({"schema":format!("{schema:?}"),"rows":rows}))
}

fn encode_raw(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Float32 => json!(format!(
            "{:08x}",
            column.as_primitive::<Float32Type>().value(row).to_bits()
        )),
        DataType::Float64 => json!(format!(
            "{:016x}",
            column.as_primitive::<Float64Type>().value(row).to_bits()
        )),
        _ => json!(array_value_to_string(column, row)?),
    })
}

async fn capture_raw(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
            ..Default::default()
        },
    )
    .await?;
    let physical = query.create_physical_plan(session).await?;
    let plan = datafusion::physical_plan::displayable(physical.as_ref())
        .indent(true)
        .to_string();
    let fields = physical
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let logical = query.logical_plan.schema().field(index);
            assert_eq!(field.name(), &query.column_names[index]);
            assert_eq!(field.data_type(), logical.data_type());
            assert_eq!(field.is_nullable(), logical.is_nullable());
            assert_eq!(field.metadata(), logical.metadata());
            let kind = match field.data_type() {
                DataType::Float32 => "float",
                DataType::Float64 => "double",
                DataType::Int32 => "integer",
                DataType::Int64 => "long",
                data_type => return Err(format!("unexpected output type: {data_type}").into()),
            };
            Ok(json!({"name":field.name(),"type":kind,"nullable":field.is_nullable(),"metadata":field.metadata()}))
        })
        .collect::<TestResult<Vec<_>>>()?;
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| encode_raw(column.as_ref(), row))
                    .collect::<TestResult<Vec<_>>>()?,
            );
        }
    }
    rows.sort_by_key(|row| json!(row).to_string());
    Ok(json!({"schema":{"type":"struct","fields":fields},"rows":rows,"plan":plan}))
}

async fn reverse_input_partitions(session: &SessionContext, metadata: &Value) -> TestResult {
    use datafusion::datasource::MemTable;

    for input in metadata["inputs"].as_array().ok_or("missing inputs")? {
        let name = input["name"].as_str().ok_or("missing table name")?;
        let provider = session.deregister_table(name)?.ok_or("missing provider")?;
        let table = provider
            .downcast_ref::<MemTable>()
            .ok_or("expected MemTable")?;
        let mut partitions = Vec::with_capacity(table.batches.len());
        for partition in table.batches.iter().rev() {
            partitions.push(partition.read().await.clone());
        }
        session.register_table(
            name,
            Arc::new(MemTable::try_new(provider.schema(), partitions)?),
        )?;
    }
    Ok(())
}

fn reference_status(actual: &Value, case: &Value) -> TestResult<&'static str> {
    let mut expected_schema = case["expected"]["schema"].clone();
    let mut status = "matched";
    if actual["schema"] != expected_schema {
        if !matches!(
            case["id"].as_str().ok_or("missing ID")?,
            "float_mixed_default_names" | "double_mixed_default_names"
        ) {
            return Ok("mismatch");
        }
        for (index, field) in expected_schema["fields"]
            .as_array_mut()
            .ok_or("missing fields")?
            .iter_mut()
            .enumerate()
        {
            if field["metadata"]["__autoGeneratedAlias"] == "true"
                && actual["schema"]["fields"][index]["metadata"]
                    .get("__autoGeneratedAlias")
                    .is_none()
            {
                field["metadata"]
                    .as_object_mut()
                    .ok_or("missing metadata")?
                    .remove("__autoGeneratedAlias");
            }
        }
        if actual["schema"] != expected_schema {
            return Ok("mismatch");
        }
        status = "matched_except_generated_alias_marker";
    }
    let Some(allowed) = case.get("allowed_rows") else {
        return Ok(if actual["rows"] == case["expected"]["rows"] {
            status
        } else {
            "mismatch"
        });
    };
    let rows = actual["rows"].as_array().ok_or("missing actual rows")?;
    let allowed = allowed.as_array().ok_or("missing permitted rows")?;
    if rows.len() != allowed.len() {
        return Ok("mismatch");
    }
    let fields = actual["schema"]["fields"]
        .as_array()
        .ok_or("missing fields")?;
    for (row, permitted) in rows.iter().zip(allowed) {
        let row = row.as_array().ok_or("missing row")?;
        let permitted = permitted.as_array().ok_or("missing permitted cells")?;
        if row.len() != permitted.len() {
            return Ok("mismatch");
        }
        for (cell, choices) in row.iter().zip(permitted) {
            if !choices.as_array().ok_or("missing choices")?.contains(cell) {
                return Ok("mismatch");
            }
        }
        for (plain, distinct) in [("low", "unique_low"), ("high", "unique_high")] {
            if let (Some(plain), Some(distinct)) = (
                fields.iter().position(|field| field["name"] == plain),
                fields.iter().position(|field| field["name"] == distinct),
            ) && row[plain] != row[distinct]
            {
                return Ok("mismatch");
            }
        }
    }
    if let Some(names) = case.get("constant_window_fields") {
        for name in names.as_array().ok_or("missing constant window fields")? {
            let index = fields
                .iter()
                .position(|field| field["name"] == *name)
                .ok_or("missing constant window output")?;
            if let Some(first) = rows.first()
                && rows.iter().any(|row| row[index] != first[index])
            {
                return Ok("mismatch");
            }
        }
    }
    Ok(status)
}

#[test]
fn unordered_whole_windows_keep_input_payloads_and_one_extremal_representative() -> TestResult {
    let expected = json!({"schema":{"type":"struct","fields":[
        {"name":"id","type":"integer","nullable":false,"metadata":{}},
        {"name":"k","type":"float","nullable":true,"metadata":{}},
        {"name":"low","type":"float","nullable":true,"metadata":{}},
        {"name":"high","type":"float","nullable":true,"metadata":{}}
    ]},"rows":[["0","80000000","80000000","80000000"],
               ["1","00000000","80000000","80000000"]]});
    let case = json!({"id":"float_zeros_negative_all","expected":expected,
    "constant_window_fields":["low","high"],"allowed_rows":[
        [["0"],["80000000"],["80000000","00000000"],["80000000","00000000"]],
        [["1"],["00000000"],["80000000","00000000"],["80000000","00000000"]]
    ]});
    let mut actual = expected.clone();
    for row in actual["rows"].as_array_mut().ok_or("missing rows")? {
        row[2] = json!("00000000");
        row[3] = json!("00000000");
    }
    assert_eq!(reference_status(&actual, &case)?, "matched");
    let mut inconsistent = actual.clone();
    inconsistent["rows"][1][2] = json!("80000000");
    assert_eq!(reference_status(&inconsistent, &case)?, "mismatch");
    let mut changed_input = actual.clone();
    changed_input["rows"][0][1] = json!("00000000");
    assert_eq!(reference_status(&changed_input, &case)?, "mismatch");
    let mut fabricated = actual.clone();
    fabricated["rows"][0][3] = json!("7fc00000");
    assert_eq!(reference_status(&fabricated, &case)?, "mismatch");
    let ordered = json!({"id":"float_zeros_negative_all_ordered","expected":expected});
    assert_eq!(reference_status(&actual, &ordered)?, "mismatch");
    Ok(())
}

#[test]
fn generated_alias_exception_keeps_the_other_schema_and_value_contracts() -> TestResult {
    let expected = json!({"schema":{"type":"struct","fields":[{
        "name":"min(k)","type":"float","nullable":true,
        "metadata":{"__autoGeneratedAlias":"true","owner":"fixture"}
    }]},"rows":[["80000000"]]});
    let case = json!({"id":"float_mixed_default_names","expected":expected});
    let mut actual = expected.clone();
    actual["schema"]["fields"][0]["metadata"]
        .as_object_mut()
        .ok_or("missing metadata")?
        .remove("__autoGeneratedAlias");
    assert_eq!(
        reference_status(&actual, &case)?,
        "matched_except_generated_alias_marker"
    );
    for (key, value) in [
        ("type", json!("double")),
        ("nullable", json!(false)),
        ("name", json!("other")),
        ("metadata", json!({})),
    ] {
        let mut wrong = actual.clone();
        wrong["schema"]["fields"][0][key] = value;
        assert_eq!(reference_status(&wrong, &case)?, "mismatch");
    }
    let mut wrong = actual.clone();
    wrong["rows"][0][0] = json!("00000000");
    assert_eq!(reference_status(&wrong, &case)?, "mismatch");
    let mut unrelated = case.clone();
    unrelated["id"] = json!("float_unrelated_default_names");
    assert_eq!(reference_status(&actual, &unrelated)?, "mismatch");
    Ok(())
}

#[tokio::test]
async fn floating_extrema_match_independent_spark_bits_and_schemas() -> TestResult {
    let path = std::env::var_os("DAR_FLOAT_EXTREMA_FIXTURE")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/floating-extrema-spark.jsonl")
        });
    let records = std::fs::read_to_string(path)?
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records.len() - 1,
        if records[0]["probe"] == true { 24 } else { 256 }
    );
    assert!(
        records[0]["raw_bits"]
            .as_str()
            .ok_or("missing raw-bit method")?
            .starts_with("JVM")
    );
    let mut identities = std::collections::BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((case["id"].to_string(), case["ansi"].to_string())));
    }
    for input in records[0]["inputs"].as_array().ok_or("missing inputs")? {
        let mut rows = input["rows"]
            .as_array()
            .ok_or("missing input rows")?
            .clone();
        rows.sort_by_key(ToString::to_string);
        assert_eq!(json!(rows), input["jvm_roundtrip"]["rows"]);
    }
    let mut reports = Vec::new();
    let mut failures = Vec::new();
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
        let schema_control =
            spark_results::capture_spark_query_result(&session, &records[1]).await?;
        assert_eq!(schema_control["schema"], records[1]["expected"]["schema"]);
        for reverse in [false, true] {
            if reverse {
                reverse_input_partitions(&session, &records[0]).await?;
            }
            for case in &records[1..] {
                if reverse && case.get("constant_window_fields").is_none() {
                    continue;
                }
                let actual = capture_raw(&session, case).await;
                let status = match &actual {
                    Ok(actual) => reference_status(actual, case)?,
                    Err(_) => "error",
                };
                let matched = matches!(status, "matched" | "matched_except_generated_alias_marker");
                reports.push(json!({"id":case["id"],"ansi":case["ansi"],"batch":batch_size,
                "provider_partition_order":if reverse { "reversed" } else { "forward" },
                "expected":case["expected"],"allowed_rows":case.get("allowed_rows"),
                "constant_window_fields":case.get("constant_window_fields"),
                "actual":actual.as_ref().ok(),"error":actual.as_ref().err().map(ToString::to_string),"matched":matched,"status":status}));
                if !matched {
                    failures.push(format!(
                        "{} ANSI={} batch={batch_size} reversed={reverse}: {actual:?}",
                        case["id"], case["ansi"]
                    ));
                }
            }
        }
    }
    if let Some(path) = std::env::var_os("DAR_FLOAT_EXTREMA_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&reports)?)?;
    }
    println!(
        "{}",
        json!({"comparisons":reports.len(),
        "matched":reports.iter().filter(|record| record["status"] == "matched").count(),
        "matched_except_generated_alias_marker":reports.iter().filter(|record| record["status"] == "matched_except_generated_alias_marker").count(),
        "mismatched":failures.len()})
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[tokio::test]
async fn grouped_extrema_limits_keep_spark_order_and_avoid_the_native_heap() -> TestResult {
    use datafusion::physical_plan::aggregates::AggregateExec;

    let records = include_str!("fixtures/floating-extrema-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    register_recorded_inputs(&session, &records[0], 1)?;
    for kind in ["float", "double"] {
        for ansi in [true, false] {
            let reference = records[1..]
                .iter()
                .find(|case| case["id"] == format!("{kind}_mixed_group") && case["ansi"] == ansi)
                .ok_or("missing grouped Spark reference")?;
            for (function, direction, group, value_index) in
                [("MIN", "ASC", "0", 1), ("MAX", "DESC", "1", 2)]
            {
                let row_index = reference["expected"]["rows"]
                    .as_array()
                    .ok_or("missing rows")?
                    .iter()
                    .position(|row| row[0] == group)
                    .ok_or("missing winning group")?;
                let row = &reference["expected"]["rows"][row_index];
                let allowed = &reference["allowed_rows"][row_index];
                let mut value_field =
                    reference["expected"]["schema"]["fields"][value_index].clone();
                value_field["name"] = json!("extremum");
                let sql = format!(
                    "SELECT id%2 AS g,{function}(k) AS extremum FROM {kind}_mixed GROUP BY id%2 ORDER BY extremum {direction} LIMIT 1"
                );
                let case = json!({"id":format!("{kind}_limited_{function}"),"ansi":ansi,"sql":sql,
                    "expected":{"schema":{"type":"struct","fields":[reference["expected"]["schema"]["fields"][0],value_field]},"rows":[[row[0],row[value_index]]]},
                    "allowed_rows":[[allowed[0],allowed[value_index]]]});
                let actual = capture_raw(&session, &case).await?;
                assert_eq!(
                    reference_status(&actual, &case)?,
                    "matched",
                    "{sql}: {actual}"
                );
                let query = plan_query(
                    &session,
                    &sql,
                    &SparkSqlOptions {
                        ansi_mode: ansi,
                        ..Default::default()
                    },
                )
                .await?;
                let mut pending = vec![query.create_physical_plan(&session).await?];
                let mut aggregates = 0;
                while let Some(node) = pending.pop() {
                    if let Some(aggregate) = node.downcast_ref::<AggregateExec>() {
                        aggregates += 1;
                        assert!(
                            aggregate.get_minmax_desc().is_none(),
                            "{sql}: native heap shortcut was advertised"
                        );
                    }
                    pending.extend(node.children().into_iter().cloned());
                }
                assert!(aggregates > 0, "{sql}: aggregate was not exercised");
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn registered_provider_statistics_preserve_safe_shortcuts_and_raw_boundaries() -> TestResult {
    use arrow::array::Int32Array;
    use arrow::record_batch::RecordBatch;
    use datafusion::common::{ScalarValue, stats::Precision};

    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for (name, rows, value, bits, min_shortcut, max_shortcut) in [
        (
            "many_numeric",
            3,
            "1.5",
            Some("3ff8000000000000"),
            true,
            false,
        ),
        (
            "one_numeric",
            1,
            "1.5",
            Some("3ff8000000000000"),
            true,
            true,
        ),
        ("zeros", 3, "'-0.0'", Some("8000000000000000"), false, false),
        ("nulls", 3, "NULL", None, true, true),
        ("empty", 0, "1.5", None, true, true),
    ] {
        let ids = format!("statistics_ids_{name}");
        let table = format!("statistics_values_{name}");
        session.register_batch(
            ids.as_str(),
            RecordBatch::try_from_iter(vec![(
                "id",
                Arc::new(Int32Array::from_iter_values(0..rows)) as arrow::array::ArrayRef,
            )])?,
        )?;
        let view = session
            .sql(&format!("SELECT CAST({value} AS DOUBLE) AS k FROM {ids}"))
            .await?;
        session.register_table(table.as_str(), view.into_view())?;
        let source = session
            .table(table.as_str())
            .await?
            .create_physical_plan()
            .await?;
        let statistics = source.partition_statistics(None)?;
        assert_eq!(statistics.num_rows, Precision::Exact(rows as usize));
        if value == "1.5" && rows > 0 {
            assert_eq!(
                statistics.column_statistics[0].min_value,
                Precision::Exact(ScalarValue::Float64(Some(1.5)))
            );
            assert_eq!(
                statistics.column_statistics[0].max_value,
                Precision::Exact(ScalarValue::Float64(Some(1.5)))
            );
        }
        for (function, shortcut) in [("MIN", min_shortcut), ("MAX", max_shortcut)] {
            let sql = format!("SELECT {function}(k) AS result FROM {table}");
            let actual = capture_raw(&session, &json!({"sql":sql,"ansi":true})).await?;
            assert_eq!(actual["rows"], json!([[bits]]), "{sql}: {actual}");
            assert_eq!(
                !actual["plan"]
                    .as_str()
                    .ok_or("missing plan")?
                    .contains("AggregateExec:"),
                shortcut,
                "{sql}: {actual}"
            );
            println!("registered statistics {sql}: {statistics:?}; {actual}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn floating_extrema_dictionary_and_statistics_diagnostic() -> TestResult {
    use arrow::array::{ArrayRef, DictionaryArray, Float32Array, Float64Array, Int32Array};
    use arrow::datatypes::{Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;

    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let native = SessionContext::new();
    let double_values: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(-0.0),
        Some(0.0),
        Some(f64::from_bits(0xfff8000000004321)),
        Some(f64::from_bits(0x7ff8000000001234)),
        Some(f64::NEG_INFINITY),
        Some(f64::INFINITY),
        None,
    ]));
    let float_values: ArrayRef = Arc::new(Float32Array::from(vec![
        Some(-0.0),
        Some(0.0),
        Some(f32::from_bits(0xffc04321)),
        Some(f32::from_bits(0x7fc01234)),
        Some(f32::NEG_INFINITY),
        Some(f32::INFINITY),
        None,
    ]));
    for values in [float_values, double_values] {
        let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![
                Some(0),
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
                Some(6),
                None,
            ]),
            values.clone(),
        )?);
        for (table, values) in [
            ("plain_extrema", values),
            ("dictionary_extrema", dictionary),
        ] {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "k",
                values.data_type().clone(),
                true,
            )]));
            let batch = RecordBatch::try_new(schema.clone(), vec![values])?;
            let provider = Arc::new(MemTable::try_new(schema, vec![vec![batch]])?);
            session.deregister_table(table)?;
            native.deregister_table(table)?;
            session.register_table(table, provider.clone())?;
            native.register_table(table, provider)?;
            let scan = session.table(table).await?.create_physical_plan().await?;
            eprintln!(
                "{table} scan statistics: {:?}",
                scan.partition_statistics(None)?
            );
            let queries = [
                format!("SELECT MIN(k) AS low,MAX(k) AS high FROM {table}"),
                format!("SELECT MIN(k) AS low,MAX(k) AS high FROM {table} GROUP BY CAST(1 AS INT)"),
                format!("SELECT MIN(k) OVER () AS low,MAX(k) OVER () AS high FROM {table}"),
            ];
            let mut before_first_spark = Vec::new();
            for sql in queries {
                let plain = native_snapshot(&native, &sql).await?;
                let configured = native_snapshot(&session, &sql).await?;
                eprintln!(
                    "native before first Spark query {sql}: plain={plain}, configured={configured}"
                );
                before_first_spark.push((sql, plain, configured));
            }
            for (sql, native_before, configured_before) in before_first_spark {
                assert_eq!(
                    configured_before, native_before,
                    "{sql}: native contexts differ before first Spark query"
                );
                let actual = capture_raw(&session, &json!({"sql":sql,"ansi":true})).await;
                eprintln!("extrema diagnostic {sql}: {actual:?}");
                let actual = actual?;
                for row in actual["rows"].as_array().ok_or("missing extrema rows")? {
                    let low = row[0].as_str().ok_or("missing low bits")?;
                    let high = row[1].as_str().ok_or("missing high bits")?;
                    assert!(matches!(low, "ff800000" | "fff0000000000000"));
                    assert!(matches!(
                        high,
                        "ffc04321" | "7fc01234" | "fff8000000004321" | "7ff8000000001234"
                    ));
                }
                let native_after = native_snapshot(&session, &sql).await?;
                assert_eq!(
                    configured_before, native_after,
                    "{sql}: native result changed after Spark planning"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "manual baseline/candidate execution cost comparison"]
async fn floating_extrema_bounded_cost() -> TestResult {
    use arrow::array::{ArrayRef, Float64Array, Int32Array};
    use arrow::record_batch::RecordBatch;
    use std::time::Instant;

    let session = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(1024)
                        .with_target_partitions(1),
                ),
        )
        .build(),
    );
    for rows in [4096, 8192] {
        session.deregister_table("extrema_cost")?;
        session.register_batch(
            "extrema_cost",
            RecordBatch::try_from_iter(vec![
                (
                    "id",
                    Arc::new(Int32Array::from_iter_values(0..rows)) as ArrayRef,
                ),
                (
                    "k",
                    Arc::new(Float64Array::from_iter_values(
                        (0..rows).map(|id| f64::from(id % 97) / 4.0 - 12.0),
                    )) as ArrayRef,
                ),
            ])?,
        )?;
        for sql in [
            "SELECT MIN(k),MAX(k) FROM extrema_cost".to_owned(),
            "SELECT id%1024 AS g,MIN(k),MAX(k) FROM extrema_cost GROUP BY id%1024".to_owned(),
            String::from(
                "SELECT MIN(k) OVER w,MAX(k) OVER w FROM extrema_cost WINDOW w AS (ORDER BY id ROWS BETWEEN 7 PRECEDING AND CURRENT ROW)",
            ),
            String::from(
                "SELECT MIN(k) OVER w,MAX(k) OVER w FROM extrema_cost WINDOW w AS (ORDER BY id ROWS BETWEEN 511 PRECEDING AND CURRENT ROW)",
            ),
        ] {
            for spark_frontend in [false, true] {
                let physical = if spark_frontend {
                    plan_query(&session, &sql, &SparkSqlOptions::default())
                        .await?
                        .create_physical_plan(&session)
                        .await?
                } else {
                    session.sql(&sql).await?.create_physical_plan().await?
                };
                let mut timings = Vec::new();
                for repeat in 0..4 {
                    let started = Instant::now();
                    let batches = collect(physical.clone(), session.task_ctx()).await?;
                    let elapsed = started.elapsed().as_micros();
                    let expected_rows = if sql.contains("OVER w") {
                        rows as usize
                    } else if sql.contains("GROUP BY") {
                        1024
                    } else {
                        1
                    };
                    assert_eq!(
                        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                        expected_rows
                    );
                    if repeat > 0 {
                        timings.push(elapsed);
                    }
                }
                timings.sort_unstable();
                println!(
                    "{}",
                    json!({"rows":rows,"sql":sql,"spark_frontend":spark_frontend,"microseconds":timings,"median_microseconds":timings[1],"plan":datafusion::physical_plan::displayable(physical.as_ref()).indent(true).to_string()})
                );
            }
        }
    }
    Ok(())
}
