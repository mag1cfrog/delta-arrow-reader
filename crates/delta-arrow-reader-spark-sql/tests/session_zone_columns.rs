//! TIMESTAMP columns are evaluated in the session time zone like Spark, whatever zone their
//! Arrow type is annotated with, including inside structs, arrays and maps.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, LargeListArray, ListArray, MapArray, StringArray,
    StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::compute::cast;
use arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, DecimalType, DurationMicrosecondType, Field, Fields,
    Float64Type, Int32Type, Int64Type, Schema, SchemaRef, TimeUnit, TimestampMicrosecondType,
};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{
    BinaryExpr, Expr, LogicalPlan, Operator, TableProviderFilterPushDown, TableType,
};
use datafusion::physical_optimizer::aggregate_statistics::PreserveAggregateStatisticsSchema;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_plan::expression_errors::SparkSqlExpressionSemantics;
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
use spark_inputs::decode_recorded_batch;

#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, validate_batch};

/// Zones DAR annotates the registered instants with. Delta reads TIMESTAMP as UTC; the others
/// are a named zone with historical offsets and its current fixed offset.
const ANNOTATIONS: [&str; 3] = ["UTC", "Asia/Kathmandu", "+05:45"];

fn session_zone_references() -> TestResult<Vec<Value>> {
    let records = include_str!("fixtures/session-zone-columns/session-zone-columns-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"].as_str().is_some_and(
            |build| build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
        )
    );
    assert_eq!(records.len() - 1, 348);
    Ok(records)
}

/// The Arrow type family DAR uses for a recorded Spark type.
fn spark_family(spark_type: &Value) -> TestResult<String> {
    let spark_type = spark_type.as_str().ok_or("expected a primitive type")?;
    Ok(match spark_type {
        "integer" => "int32".to_owned(),
        "long" => "int64".to_owned(),
        "boolean" => "boolean".to_owned(),
        "double" => "float64".to_owned(),
        "date" => "date32".to_owned(),
        "string" => "string".to_owned(),
        "timestamp" => "timestamp".to_owned(),
        "timestamp_ntz" => "timestamp_ntz".to_owned(),
        "interval day to second" => "interval day to second".to_owned(),
        decimal if decimal.starts_with("decimal(") => decimal.replace(' ', ""),
        other => return Err(format!("unexpected Spark type {other}").into()),
    })
}

/// A TIMESTAMP result must be annotated with the session zone, as Spark's Arrow output is.
fn dar_family(data_type: &DataType, timezone: &str) -> TestResult<String> {
    Ok(match data_type {
        DataType::Int32 => "int32".to_owned(),
        DataType::Int64 => "int64".to_owned(),
        DataType::Boolean => "boolean".to_owned(),
        DataType::Float64 => "float64".to_owned(),
        DataType::Date32 => "date32".to_owned(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".to_owned(),
        DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) if zone.as_ref() == timezone => {
            "timestamp".to_owned()
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp_ntz".to_owned(),
        DataType::Duration(TimeUnit::Microsecond) => "interval day to second".to_owned(),
        DataType::Decimal128(precision, scale) => format!("decimal({precision},{scale})"),
        other => format!("unexpected {other}"),
    })
}

fn spark_fields(schema: &Value) -> TestResult<Value> {
    schema["fields"]
        .as_array()
        .ok_or("missing recorded fields")?
        .iter()
        .map(|field| {
            Ok(
                json!({"name": field["name"], "type": spark_family(&field["type"])?,
                "nullable": field["nullable"], "metadata": field["metadata"]}),
            )
        })
        .collect::<TestResult<Vec<_>>>()
        .map(Value::from)
}

fn dar_fields(fields: &Fields, names: Option<&[String]>, timezone: &str) -> TestResult<Value> {
    fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let name = names.map_or(field.name().as_str(), |names| names[index].as_str());
            Ok(
                json!({"name": name, "type": dar_family(field.data_type(), timezone)?,
                "nullable": field.is_nullable(), "metadata": field.metadata()}),
            )
        })
        .collect::<TestResult<Vec<_>>>()
        .map(Value::from)
}

/// Encode a value as the capture does: TIMESTAMP as UTC-epoch microseconds, DOUBLE as its
/// IEEE 754 bits and DECIMAL as plain text.
fn encode_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Int32 => json!(column.as_primitive::<Int32Type>().value(row).to_string()),
        DataType::Int64 => json!(column.as_primitive::<Int64Type>().value(row).to_string()),
        DataType::Date32 => json!(column.as_primitive::<Date32Type>().value(row).to_string()),
        DataType::Boolean => json!(column.as_boolean().value(row).to_string()),
        DataType::Float64 => json!(format!(
            "{:016x}",
            column.as_primitive::<Float64Type>().value(row).to_bits()
        )),
        DataType::Decimal128(precision, scale) => json!(Decimal128Type::format_decimal(
            column.as_primitive::<Decimal128Type>().value(row),
            *precision,
            *scale,
        )),
        DataType::Timestamp(TimeUnit::Microsecond, _) => json!(
            column
                .as_primitive::<TimestampMicrosecondType>()
                .value(row)
                .to_string()
        ),
        DataType::Duration(TimeUnit::Microsecond) => json!(
            column
                .as_primitive::<DurationMicrosecondType>()
                .value(row)
                .to_string()
        ),
        DataType::Utf8 => json!(column.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => json!(column.as_string::<i64>().value(row)),
        DataType::Utf8View => json!(column.as_string_view().value(row)),
        other => return Err(format!("unexpected result type {other}").into()),
    })
}

fn options(case: &Value) -> TestResult<SparkSqlOptions> {
    Ok(SparkSqlOptions {
        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        session_timezone: case["timezone"].as_str().ok_or("missing time zone")?.into(),
        ..SparkSqlOptions::default()
    })
}

/// Observe analyzed (public names), optimized and executed fields, typed rows and the
/// failing phase in the record's session time zone.
async fn observe(session: &SessionContext, case: &Value) -> Value {
    let mut observation = json!({});
    let mut phase = "resolve";
    let result: TestResult<Vec<Vec<Value>>> = async {
        let timezone = case["timezone"].as_str().ok_or("missing time zone")?;
        let query = plan_query(
            session,
            case["sql"].as_str().ok_or("missing SQL")?,
            &options(case)?,
        )
        .await?;
        observation["analyzed"] = dar_fields(
            query.logical_plan.schema().fields(),
            Some(&query.column_names),
            timezone,
        )?;
        phase = "optimized";
        let optimized = optimize(session, &query.logical_plan)?;
        observation["optimized"] = dar_fields(
            optimized.schema().fields(),
            Some(&query.column_names),
            timezone,
        )?;
        let physical = query.create_physical_plan(session).await?;
        phase = "collect";
        let batches = collect(physical.clone(), session.task_ctx()).await?;
        let schema = physical.schema();
        observation["executed"] = dar_fields(schema.fields(), None, timezone)?;
        let mut rows = Vec::new();
        for batch in batches {
            validate_batch(schema.as_ref(), &batch)?;
            for row in 0..batch.num_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| encode_value(column.as_ref(), row))
                        .collect::<TestResult<Vec<_>>>()?,
                );
            }
        }
        rows.sort_by_key(|row| json!(row).to_string());
        Ok(rows)
    }
    .await;
    match result {
        Ok(rows) => observation["rows"] = json!(rows),
        Err(error) => {
            observation["error_phase"] = json!(phase);
            observation["error"] = json!(error.to_string());
        }
    }
    observation
}

fn optimize(session: &SessionContext, plan: &LogicalPlan) -> TestResult<LogicalPlan> {
    let mut state = session.state();
    let extensions = &mut state.config_mut().options_mut().extensions;
    extensions.insert(SparkSqlExpressionSemantics::default());
    extensions.insert(PreserveAggregateStatisticsSchema::default());
    Ok(state.optimize(plan)?)
}

/// The recorded stages and outcome that differ from the observation. Spark records no
/// failures; a retained DAR failure must occur at its stage with its message.
fn differing_parts(case: &Value, observation: &Value) -> TestResult<Vec<&'static str>> {
    let mut differing = Vec::new();
    for stage in ["analyzed", "optimized", "executed"] {
        if let Some(expected) = case["schema_stages"].get(stage)
            && observation[stage] != spark_fields(expected)?
        {
            differing.push(stage);
        }
    }
    let outcome_matches = match case["expected"]["status"].as_str() {
        Some("ok") => observation["rows"] == case["expected"]["rows"],
        Some("error") => {
            observation["error_phase"] == case["spark_error_phase"]
                && case["expected"]["dar_message"]
                    .as_str()
                    .zip(observation["error"].as_str())
                    .is_some_and(|(message, error)| error.contains(message))
        }
        _ => false,
    };
    if !outcome_matches {
        differing.push("outcome");
    }
    Ok(differing)
}

/// Differences outside the session zone, each owned by an issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RetainedDifference {
    /// #1090: GREATEST is nullable where Spark declares it required.
    NullableGreatest,
    /// #1052: in Los Angeles, row 10, 9999-12-31, truncated to its quarter starts at
    /// 9999-10-01 00:00 local time, which Java's recurring rules read in PDT and the date
    /// library, whose Los Angeles rules end in 2099, in PST, an hour later.
    RecurringDstAfter2099,
    /// #1029: in a session zone Spark accepts but Arrow cannot parse, such as `PST`, a
    /// column keeps its own zone, so DAR evaluates it as a session in that zone would.
    UnparsedSessionZone,
    /// #1094: TO_JSON prints TIMESTAMP fractions in microseconds where Spark prints
    /// milliseconds, and the Los Angeles local mean time offset without its seconds.
    JsonTimestampText,
}

/// Session zones Spark accepts that Arrow cannot parse.
const UNPARSED_ZONES: [&str; 3] = ["Z", "PST", "GMT+08:00"];

fn retained_difference(case: &Value) -> Option<RetainedDifference> {
    let id = case["id"].as_str()?;
    let timezone = case["timezone"].as_str()?;
    if UNPARSED_ZONES.contains(&timezone) {
        Some(RetainedDifference::UnparsedSessionZone)
    } else if id == "to_json" {
        Some(RetainedDifference::JsonTimestampText)
    } else if id == "date_trunc_quarter" && timezone == "America/Los_Angeles" {
        Some(RetainedDifference::RecurringDstAfter2099)
    } else if id == "greatest_hour" {
        Some(RetainedDifference::NullableGreatest)
    } else {
        None
    }
}

/// The registered `ts` instant of each `zone_inputs` key.
fn registered_instants(metadata: &Value) -> TestResult<Vec<(Value, Option<i64>)>> {
    let input = metadata["inputs"]
        .as_array()
        .ok_or("missing inputs")?
        .iter()
        .find(|input| input["name"] == "zone_inputs")
        .ok_or("missing zone_inputs")?;
    input["rows"]
        .as_array()
        .ok_or("missing rows")?
        .iter()
        .map(|row| {
            let instant = row[1].as_str().map(str::parse::<i64>).transpose()?;
            Ok((row[0].clone(), instant))
        })
        .collect()
}

/// The registered instant of each row of a record's result, keyed by its first column.
fn row_instants<'r>(
    rows: &'r mut [Value],
    instants: &[(Value, Option<i64>)],
) -> TestResult<Vec<(&'r mut Value, i64)>> {
    let mut paired = Vec::new();
    for row in rows {
        let instant = instants
            .iter()
            .find(|(key, _)| *key == row[0])
            .ok_or("missing registered row")?
            .1;
        if let Some(instant) = instant {
            paired.push((row, instant));
        }
    }
    Ok(paired)
}

/// Replace only the retained part of Spark's expectation with DAR's current result for
/// columns annotated with `annotation`.
fn current_expectation(
    case: &Value,
    difference: RetainedDifference,
    annotation: &str,
    records: &[Value],
) -> TestResult<Value> {
    let metadata = &records[0];
    let mut current = case.clone();
    if case["expected"]["status"] != "ok" {
        return Err(format!("expected a Spark result: {case}").into());
    }
    match difference {
        RetainedDifference::UnparsedSessionZone => {
            // The record of the same query in a session in the column's own zone.
            let own_zone = records[1..]
                .iter()
                .find(|record| {
                    record["id"] == case["id"]
                        && record["ansi"] == case["ansi"]
                        && record["timezone"] == annotation
                })
                .ok_or("missing record in the annotation's zone")?;
            current["expected"] = own_zone["expected"].clone();
        }
        RetainedDifference::JsonTimestampText => {
            let instants = registered_instants(metadata)?;
            let rows = current["expected"]["rows"]
                .as_array_mut()
                .ok_or("missing rows")?;
            for (row, instant) in row_instants(rows, &instants)? {
                let mut text = row[1].as_str().ok_or("missing JSON")?.to_owned();
                let point = text.find('.').ok_or("missing fraction")?;
                // Offsets are whole seconds, so the local time keeps the instant's
                // sub-millisecond digits.
                text.insert_str(point + 4, &format!("{:03}", instant.rem_euclid(1_000)));
                row[1] = json!(text.replace("-07:52:58", "-07:52"));
            }
        }
        RetainedDifference::RecurringDstAfter2099 => {
            let mut changed = 0;
            for row in current["expected"]["rows"]
                .as_array_mut()
                .ok_or("missing rows")?
            {
                if row[0] == "10" {
                    let spark = row[1].as_str().ok_or("missing result")?.parse::<i64>()?;
                    row[1] = json!((spark + 3_600_000_000).to_string());
                    changed += 1;
                }
            }
            if changed != 1 {
                return Err(format!("expected row 10 once: {case}").into());
            }
        }
        RetainedDifference::NullableGreatest => {
            for stage in ["analyzed", "optimized", "executed"] {
                let field = &mut current["schema_stages"][stage]["fields"][1];
                if field["nullable"] != false {
                    return Err(format!("expected Spark's required GREATEST: {case}").into());
                }
                field["nullable"] = json!(true);
            }
        }
    }
    Ok(current)
}

fn session_context(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
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
    )
}

fn register(
    session: &SessionContext,
    name: &str,
    batch: &RecordBatch,
    batch_size: usize,
) -> TestResult {
    let mut partitions = vec![Vec::new(), Vec::new()];
    for (chunk, offset) in (0..batch.num_rows()).step_by(batch_size).enumerate() {
        partitions[chunk % 2].push(batch.slice(offset, batch_size.min(batch.num_rows() - offset)));
    }
    session.register_table(
        name,
        Arc::new(MemTable::try_new(batch.schema(), partitions)?),
    )?;
    Ok(())
}

/// The recorded input with its TIMESTAMP column annotated with `annotation`. The stored
/// instants do not change.
fn annotated_input(input: &Value, annotation: &str) -> TestResult<RecordBatch> {
    let batch = decode_recorded_batch(input)?;
    let annotated = DataType::Timestamp(TimeUnit::Microsecond, Some(annotation.into()));
    let schema = batch.schema();
    let mut fields = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        if matches!(field.data_type(), DataType::Timestamp(_, Some(_))) {
            fields.push(Field::clone(field).with_data_type(annotated.clone()));
            columns.push(cast(column, &annotated)?);
        } else {
            fields.push(Field::clone(field));
            columns.push(Arc::clone(column));
        }
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

/// `nested_inputs` as stored columns: `named_struct('t', ts)`, `array(ts)` and `map('x', ts)`
/// for each row of `zone_inputs`, which Spark defines as a view. `la` and `fa` hold the
/// array as a large list and a fixed-size list, encodings Spark does not have.
fn nested_input(zone_inputs: &RecordBatch) -> TestResult<RecordBatch> {
    let keys = zone_inputs
        .column_by_name("k")
        .ok_or("missing key column")?;
    let instants = zone_inputs
        .column_by_name("ts")
        .ok_or("missing TIMESTAMP column")?;
    let instant_type = instants.data_type().clone();
    let rows = zone_inputs.num_rows();
    let struct_fields = Fields::from(vec![Field::new("t", instant_type.clone(), true)]);
    let structs = StructArray::try_new(struct_fields.clone(), vec![Arc::clone(instants)], None)?;
    let item = Arc::new(Field::new_list_field(instant_type.clone(), true));
    let lists = ListArray::try_new(
        Arc::clone(&item),
        OffsetBuffer::from_lengths(vec![1; rows]),
        Arc::clone(instants),
        None,
    )?;
    let large_lists = LargeListArray::try_new(
        Arc::clone(&item),
        OffsetBuffer::from_lengths(vec![1; rows]),
        Arc::clone(instants),
        None,
    )?;
    let fixed_lists =
        FixedSizeListArray::try_new(Arc::clone(&item), 1, Arc::clone(instants), None)?;
    let entry_fields = Fields::from(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", instant_type, true),
    ]);
    let entries = StructArray::try_new(
        entry_fields.clone(),
        vec![
            Arc::new(StringArray::from(vec!["x"; rows])),
            Arc::clone(instants),
        ],
        None,
    )?;
    let entries_field = Arc::new(Field::new("entries", DataType::Struct(entry_fields), false));
    let maps = MapArray::try_new(
        Arc::clone(&entries_field),
        OffsetBuffer::from_lengths(vec![1; rows]),
        entries,
        None,
        false,
    )?;
    let schema = Schema::new(vec![
        Field::new("k", keys.data_type().clone(), false),
        Field::new("s", DataType::Struct(struct_fields), false),
        Field::new("a", DataType::List(Arc::clone(&item)), false),
        Field::new("m", DataType::Map(entries_field, false), false),
        Field::new("la", DataType::LargeList(Arc::clone(&item)), false),
        Field::new("fa", DataType::FixedSizeList(item, 1), false),
    ]);
    Ok(RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::clone(keys),
            Arc::new(structs),
            Arc::new(lists),
            Arc::new(maps),
            Arc::new(large_lists),
            Arc::new(fixed_lists),
        ],
    )?)
}

/// The record's query over the large-list and fixed-size-list copies of its array, for each
/// record that reads `a`. Spark has only one array encoding, so the same record applies.
fn list_encoding_cases(case: &Value) -> TestResult<Vec<Value>> {
    let sql = case["sql"].as_str().ok_or("missing SQL")?;
    if !sql.contains("(a[0]") && !sql.contains("element_at(a,") {
        return Ok(Vec::new());
    }
    Ok(["la", "fa"]
        .into_iter()
        .map(|column| {
            let mut case = case.clone();
            case["sql"] = json!(
                sql.replace("(a[0]", &format!("({column}[0]"))
                    .replace("element_at(a,", &format!("element_at({column},"))
            );
            case
        })
        .collect())
}

/// Register the recorded inputs with every TIMESTAMP annotated with `annotation`.
fn register_inputs(
    session: &SessionContext,
    metadata: &Value,
    annotation: &str,
    batch_size: usize,
) -> TestResult {
    let mut zone_inputs = None;
    for input in metadata["inputs"].as_array().ok_or("missing inputs")? {
        let name = input["name"].as_str().ok_or("missing table name")?;
        let batch = annotated_input(input, annotation)?;
        register(session, name, &batch, batch_size)?;
        if name == "zone_inputs" {
            zone_inputs = Some(batch);
        }
    }
    let nested = nested_input(&zone_inputs.ok_or("missing zone_inputs")?)?;
    let name = metadata["nested_inputs"]["name"]
        .as_str()
        .ok_or("missing nested input name")?;
    register(session, name, &nested, batch_size)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timestamp_columns_follow_the_session_zone_like_spark() -> TestResult {
    let records = session_zone_references()?;
    let mut retained = BTreeMap::new();
    let mut list_encoding_records = 0;
    for case in &records[1..] {
        if let Some(difference) = retained_difference(case) {
            *retained.entry(difference).or_insert(0) += 1;
        }
        list_encoding_records += usize::from(!list_encoding_cases(case)?.is_empty());
    }
    assert_eq!(
        retained,
        BTreeMap::from([
            (RetainedDifference::NullableGreatest, 4),
            (RetainedDifference::RecurringDstAfter2099, 2),
            (RetainedDifference::UnparsedSessionZone, 12),
            (RetainedDifference::JsonTimestampText, 4),
        ])
    );
    assert_eq!(list_encoding_records, 12);
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        for annotation in ANNOTATIONS {
            let session = session_context(batch_size);
            register_inputs(&session, &records[0], annotation, batch_size)?;
            for case in &records[1..] {
                let expectation = match retained_difference(case) {
                    Some(difference) => {
                        current_expectation(case, difference, annotation, &records)?
                    }
                    None => case.clone(),
                };
                let encoded = list_encoding_cases(case)?;
                for observed_case in std::iter::once(case).chain(&encoded) {
                    let observation = observe(&session, observed_case).await;
                    let differing = differing_parts(&expectation, &observation)?;
                    if !differing.is_empty() {
                        differences.push(json!({
                            "id": case["id"], "timezone": case["timezone"],
                            "ansi": case["ansi"], "batch": batch_size,
                            "annotation": annotation, "sql": observed_case["sql"],
                            "differing": differing, "expected": expectation["expected"],
                            "schema_stages": expectation["schema_stages"],
                            "observed": observation,
                        }));
                    }
                }
            }
        }
    }
    assert!(
        differences.is_empty(),
        "{} differences:\n{}",
        differences.len(),
        differences
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    );
    Ok(())
}

/// Native DataFusion SQL keeps each column's own annotation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_sql_keeps_the_column_zone() -> TestResult {
    let records = session_zone_references()?;
    let session = session_context(32);
    register_inputs(&session, &records[0], "UTC", 32)?;
    let frame = session
        .sql("SELECT ts, date_part('hour', ts) AS h FROM zone_inputs WHERE k = 0")
        .await?;
    let batches = frame.collect().await?;
    let batch = batches.first().ok_or("missing batch")?;
    assert_eq!(
        batch.schema().field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(encode_value(batch.column(1).as_ref(), 0)?, json!("13"));
    Ok(())
}

/// A table that accepts filters as inexact, as a pruning Delta scan does, and ignores them.
#[derive(Debug)]
struct PruningTable {
    table: MemTable,
    /// Refuse filters that read a column through a cast, as the Delta provider refuses
    /// filters it cannot translate; otherwise accept every filter.
    bare_columns_only: bool,
}

impl PruningTable {
    fn new(batch: RecordBatch, bare_columns_only: bool) -> TestResult<Self> {
        Ok(Self {
            table: MemTable::try_new(batch.schema(), vec![vec![batch]])?,
            bare_columns_only,
        })
    }
}

impl TableProvider for PruningTable {
    fn schema(&self) -> SchemaRef {
        self.table.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        filters
            .iter()
            .map(|filter| {
                Ok(
                    if self.bare_columns_only && reads_a_column_through_a_cast(filter)? {
                        TableProviderFilterPushDown::Unsupported
                    } else {
                        TableProviderFilterPushDown::Inexact
                    },
                )
            })
            .collect()
    }

    // The trait's `async_trait` signature, written out because the crate's tests do not
    // depend on `async-trait`.
    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        projection: Option<&'life2 Vec<usize>>,
        _filters: &'life3 [Expr],
        limit: Option<usize>,
    ) -> Pin<
        Box<
            dyn Future<Output = datafusion::error::Result<Arc<dyn ExecutionPlan>>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        self.table.scan(state, projection, &[], limit)
    }
}

/// A comparison with a session-zone literal still reaches the scan as a predicate on the
/// stored column, so a Delta scan can prune with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_zone_filters_reach_the_scan_on_the_stored_column() -> TestResult {
    let records = session_zone_references()?;
    let input = records[0]["inputs"]
        .as_array()
        .ok_or("missing inputs")?
        .iter()
        .find(|input| input["name"] == "zone_inputs")
        .ok_or("missing zone_inputs")?;
    let batch = annotated_input(input, "UTC")?;
    let session = session_context(32);
    session.register_table("pruned_inputs", Arc::new(PruningTable::new(batch, false)?))?;
    // 2024-01-02 00:00 is 1704153600 seconds after the epoch in UTC and 08:00 UTC in
    // Los Angeles.
    for (timezone, seconds) in [
        ("UTC", 1_704_153_600_i64),
        ("America/Los_Angeles", 1_704_182_400),
    ] {
        let case = json!({"ansi": true, "timezone": timezone});
        let sql = "SELECT k FROM pruned_inputs WHERE ts >= TIMESTAMP '2024-01-02 00:00:00'";
        let query = plan_query(&session, sql, &options(&case)?).await?;
        let optimized = optimize(&session, &query.logical_plan)?;
        let mut filters = Vec::new();
        optimized.apply(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                filters.extend(scan.filters.iter().cloned());
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        let stored_column_filter = match filters.as_slice() {
            [Expr::BinaryExpr(BinaryExpr { left, op, right })] => {
                matches!(left.as_ref(), Expr::Column(column) if column.name == "ts")
                    && *op == Operator::GtEq
                    && matches!(right.as_ref(), Expr::Literal(value, _) if *value
                    == ScalarValue::TimestampMicrosecond(
                        Some(seconds * 1_000_000),
                        Some("UTC".into()),
                    ))
            }
            _ => false,
        };
        assert!(stored_column_filter, "{timezone}: {filters:?}\n{optimized}");
    }
    Ok(())
}

/// Whether `expr` reads a column through a cast, such as the session-zone relabel.
fn reads_a_column_through_a_cast(expr: &Expr) -> datafusion::error::Result<bool> {
    expr.exists(|expr| {
        Ok(matches!(expr, Expr::Cast(cast) if matches!(cast.expr.as_ref(), Expr::Column(_))))
    })
}

/// Whether `expr` reads a column through a cast other than as a function argument, as HOUR
/// reads the relabeled value.
fn reads_a_cast_outside_a_function(expr: &Expr) -> datafusion::error::Result<bool> {
    let mut found = false;
    expr.apply(|expr| {
        Ok(match expr {
            Expr::ScalarFunction(_) => TreeNodeRecursion::Jump,
            Expr::Cast(cast) if matches!(cast.expr.as_ref(), Expr::Column(_)) => {
                found = true;
                TreeNodeRecursion::Stop
            }
            _ => TreeNodeRecursion::Continue,
        })
    })?;
    Ok(found)
}

/// #1096: NULL tests, comparisons, BETWEEN and IN over a TIMESTAMP column relabeled with the
/// session zone, and their negations, reach the scan once as predicates on the stored column,
/// so a Delta scan can prune with them, and keep their rows. They do so also when the scan
/// refuses filters that read a cast, as the Delta provider does, for each annotation in both
/// ANSI modes. A session in the column's own zone needs no relabel.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn relabeled_predicates_reach_the_scan_on_the_stored_column() -> TestResult {
    let records = session_zone_references()?;
    let instants = registered_instants(&records[0])?;
    let input = records[0]["inputs"]
        .as_array()
        .ok_or("missing inputs")?
        .iter()
        .find(|input| input["name"] == "zone_inputs")
        .ok_or("missing zone_inputs")?;
    let session = session_context(32);
    // Each annotation, read by a scan that accepts filters and by one that refuses filters
    // that read a cast.
    let mut tables = Vec::new();
    for (index, annotation) in ANNOTATIONS.into_iter().enumerate() {
        let batch = annotated_input(input, annotation)?;
        for bare_columns_only in [false, true] {
            let kind = if bare_columns_only {
                "refusing"
            } else {
                "accepting"
            };
            let table = format!("{kind}_{index}");
            session.register_table(
                table.as_str(),
                Arc::new(PruningTable::new(batch.clone(), bare_columns_only)?),
            )?;
            tables.push((table, annotation));
        }
    }
    // Two registered instants, 2024-01-01 07:30 UTC and 2024-03-10 10:00 UTC, the start of
    // daylight saving time in Los Angeles, written in each session zone.
    const FIRST: i64 = 1_704_094_200_000_000;
    const SECOND: i64 = 1_710_064_800_000_000;
    for (timezone, first, second) in [
        ("UTC", "2024-01-01 07:30:00", "2024-03-10 10:00:00"),
        (
            "America/Los_Angeles",
            "2023-12-31 23:30:00",
            "2024-03-10 03:00:00",
        ),
    ] {
        type Keeps = Box<dyn Fn(Option<i64>) -> Option<bool>>;
        // DataFusion rewrites an IN list of up to three items as equalities; a longer list
        // stays an IN list.
        let four = format!(
            "TIMESTAMP '{first}', TIMESTAMP '{second}', \
             TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2001-01-01 00:00:00'"
        );
        let predicates: [(String, Keeps); 15] = [
            ("ts IS NULL".into(), Box::new(|ts| Some(ts.is_none()))),
            ("ts IS NOT NULL".into(), Box::new(|ts| Some(ts.is_some()))),
            ("NOT (ts IS NULL)".into(), Box::new(|ts| Some(ts.is_some()))),
            (
                format!("ts = TIMESTAMP '{first}'"),
                Box::new(|ts| ts.map(|ts| ts == FIRST)),
            ),
            (
                format!("TIMESTAMP '{first}' > ts"),
                Box::new(|ts| ts.map(|ts| ts < FIRST)),
            ),
            (
                format!("ts BETWEEN TIMESTAMP '{first}' AND TIMESTAMP '{second}'"),
                Box::new(|ts| ts.map(|ts| (FIRST..=SECOND).contains(&ts))),
            ),
            (
                format!("ts NOT BETWEEN TIMESTAMP '{first}' AND TIMESTAMP '{second}'"),
                Box::new(|ts| ts.map(|ts| !(FIRST..=SECOND).contains(&ts))),
            ),
            (
                format!("ts IN (TIMESTAMP '{first}', TIMESTAMP '{second}')"),
                Box::new(|ts| ts.map(|ts| ts == FIRST || ts == SECOND)),
            ),
            (
                format!("ts NOT IN (TIMESTAMP '{first}', TIMESTAMP '{second}')"),
                Box::new(|ts| ts.map(|ts| ts != FIRST && ts != SECOND)),
            ),
            (
                format!("ts IN ({four})"),
                Box::new(|ts| ts.map(|ts| ts == FIRST || ts == SECOND)),
            ),
            (
                format!("ts NOT IN ({four})"),
                Box::new(|ts| ts.map(|ts| ts != FIRST && ts != SECOND)),
            ),
            (
                format!("ts <=> TIMESTAMP '{first}' OR ts <=> TIMESTAMP '{second}'"),
                Box::new(|ts| Some(ts == Some(FIRST) || ts == Some(SECOND))),
            ),
            // HOUR keeps the relabel, which no longer makes it shared with the IN list.
            (
                format!("ts IN ({four}) AND hour(ts) >= 0"),
                Box::new(|ts| ts.map(|ts| ts == FIRST || ts == SECOND)),
            ),
            (
                format!("ts IN (TIMESTAMP '{first}', NULL)"),
                Box::new(|ts| ts.and_then(|ts| (ts == FIRST).then_some(true))),
            ),
            (
                format!("ts > TIMESTAMP '{second}' OR ts IS NULL"),
                Box::new(|ts| Some(ts.is_none_or(|ts| ts > SECOND))),
            ),
        ];
        for (predicate, keeps) in &predicates {
            for ansi in [true, false] {
                for (table, annotation) in &tables {
                    let context =
                        format!("{timezone} ansi={ansi} {annotation} {table} {predicate}");
                    let case = json!({"ansi": ansi, "timezone": timezone});
                    let sql = format!("SELECT k FROM {table} WHERE {predicate}");
                    let query = plan_query(&session, &sql, &options(&case)?).await?;
                    let optimized = optimize(&session, &query.logical_plan)?;
                    let mut filters = Vec::new();
                    let mut filter_predicates = Vec::new();
                    let mut relabels = Vec::new();
                    optimized.apply(|node| {
                        match node {
                            LogicalPlan::TableScan(scan) => {
                                filters.extend(scan.filters.iter().cloned())
                            }
                            LogicalPlan::Filter(filter) => {
                                filter_predicates.push(filter.predicate.clone())
                            }
                            _ => {}
                        }
                        for expr in node.expressions() {
                            if reads_a_cast_outside_a_function(&expr)? {
                                relabels.push(expr);
                            }
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                    assert!(!filters.is_empty(), "{context}: no filter reached the scan");
                    for (position, filter) in filters.iter().enumerate() {
                        assert!(
                            !reads_a_cast_outside_a_function(filter)?,
                            "{context}: {filter} reads the relabel\n{optimized}"
                        );
                        assert!(
                            !filters[..position].contains(filter),
                            "{context}: {filter} is pushed twice\n{optimized}"
                        );
                    }
                    // The filter that keeps the scan's rows reads the stored column too, also
                    // through no projection of the relabel.
                    assert_eq!(filter_predicates.len(), 1, "{context}\n{optimized}");
                    assert!(
                        relabels.is_empty(),
                        "{context}: the plan reads the relabel\n{optimized}"
                    );
                    let physical = query.create_physical_plan(&session).await?;
                    let mut rows = Vec::new();
                    for batch in collect(physical, session.task_ctx()).await? {
                        for row in 0..batch.num_rows() {
                            rows.push(encode_value(batch.column(0).as_ref(), row)?);
                        }
                    }
                    rows.sort_by_key(Value::to_string);
                    let mut expected = instants
                        .iter()
                        .filter(|(_, ts)| keeps(*ts) == Some(true))
                        .map(|(key, _)| key.clone())
                        .collect::<Vec<_>>();
                    expected.sort_by_key(Value::to_string);
                    assert_eq!(rows, expected, "{context}");
                }
            }
        }
    }
    // Native DataFusion SQL keeps its own cast.
    let native = session
        .sql(
            "SELECT k FROM accepting_0 WHERE \
             arrow_cast(ts, 'Timestamp(Microsecond, Some(\"America/Los_Angeles\"))') IS NULL",
        )
        .await?
        .into_optimized_plan()?;
    let mut filters = Vec::new();
    native.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            filters.extend(scan.filters.iter().cloned());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    assert!(
        matches!(filters.as_slice(), [filter] if reads_a_column_through_a_cast(filter)?),
        "native: {filters:?}\n{native}"
    );
    Ok(())
}
