//! DATE_FORMAT tokens, date/timestamp parsing and timestamp type contracts match Spark.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, Date32Type, Int32Type, Int64Type, TimeUnit, TimestampMicrosecondType,
};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_optimizer::aggregate_statistics::PreserveAggregateStatisticsSchema;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_plan::expression_errors::SparkSqlExpressionSemantics;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
use spark_inputs::register_recorded_inputs;

#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, capture_schema, validate_batch};

const LOS_ANGELES: &str = "America/Los_Angeles";

fn temporal_references() -> TestResult<Vec<Value>> {
    let records = include_str!("fixtures/temporal-contracts/temporal-contracts-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"].as_str().is_some_and(
            |build| build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
        )
    );
    assert_eq!(records.len() - 1, 6020);
    Ok(records)
}

fn date_format_zone_references() -> TestResult<(Vec<Value>, Value)> {
    let records = include_str!("fixtures/date-format-zones/spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    let requests: Value =
        serde_json::from_str(include_str!("fixtures/date-format-zones/requests.json"))?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len() - 1, 122);
    let mut requested = Vec::new();
    for corpus in requests["corpora"].as_array().ok_or("missing corpora")? {
        requested.extend(corpus["cases"].as_array().ok_or("missing cases")?);
    }
    assert_eq!(requested.len(), 122);
    for (case, request) in records[1..].iter().zip(requested) {
        for key in [
            "corpus", "id", "sql", "ansi", "timezone", "ordered", "scope",
        ] {
            assert_eq!(case[key], request[key], "case identity {key}");
        }
    }
    Ok((records, requests))
}

fn date_format_session(batch_size: usize) -> SessionContext {
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

fn date_format_result_differences(
    case: &Value,
    observed: &Value,
    expected_rows: &Value,
) -> Vec<&'static str> {
    assert_eq!(case["expected"]["status"], "ok");
    let executed_types = |schema: &Value| {
        schema["fields"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|field| field["type"].clone())
            .collect::<Vec<_>>()
    };
    let mut differing = Vec::new();
    if executed_types(&case["schema_stages"]["executed"]) != executed_types(&observed["executed"]) {
        differing.push("executed types");
    }
    if observed["rows"] != *expected_rows {
        differing.push("rows");
    }
    differing
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn date_format_common_zones_match_spark() -> TestResult {
    let (records, requests) = date_format_zone_references()?;
    let cases: Vec<_> = records[1..]
        .iter()
        .filter(|case| matches!(case["scope"].as_str(), Some("common" | "alias")))
        .collect();
    assert_eq!(cases.len(), 104);
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = date_format_session(batch_size);
        register_recorded_inputs(&session, &requests["corpora"][0]["metadata"], batch_size)?;
        for case in &cases {
            let observed = observe(&session, case).await;
            println!(
                "format_zone_observation={}",
                json!({"id":case["id"],
                "timezone":case["timezone"], "ansi":case["ansi"], "scope":case["scope"],
                "batch":batch_size,"expected":case["expected"],
                "executed":case["schema_stages"]["executed"], "actual":observed})
            );
            let differing =
                date_format_result_differences(case, &observed, &case["expected"]["rows"]);
            if !differing.is_empty() {
                differences.push(json!({"id":case["id"], "timezone":case["timezone"],
                    "ansi":case["ansi"],"batch":batch_size,"differing":differing,
                    "expected":case["expected"],"actual":observed}));
            }
        }
    }
    assert!(
        differences.is_empty(),
        "{} differences: {}",
        differences.len(),
        json!(differences)
    );
    Ok(())
}

/// Recorded display-name differences, separate from exact Spark comparisons: long zone
/// names keep region IDs and Kathmandu's short name falls back to its numeric offset.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn localized_zone_names_keep_documented_fallbacks() -> TestResult {
    let (records, requests) = date_format_zone_references()?;
    let cases: Vec<_> = records[1..]
        .iter()
        .filter(|case| {
            matches!(
                case["scope"].as_str(),
                Some("long_boundary" | "short_boundary")
            )
        })
        .collect();
    assert_eq!(cases.len(), 18);
    for batch_size in [1, 32] {
        let session = date_format_session(batch_size);
        register_recorded_inputs(&session, &requests["corpora"][0]["metadata"], batch_size)?;
        for case in &cases {
            let fallback = if case["scope"] == "short_boundary" {
                "+05:45"
            } else {
                text(case, "timezone")?
            };
            let mut rows = case["expected"]["rows"].clone();
            for row in rows.as_array_mut().ok_or("missing rows")? {
                for value in row.as_array_mut().ok_or("missing row")?.iter_mut().skip(1) {
                    if !value.is_null() {
                        *value = json!(fallback);
                    }
                }
            }
            let observed = observe(&session, case).await;
            println!(
                "format_zone_boundary_observation={}",
                json!({"id":case["id"],
                "timezone":case["timezone"],"ansi":case["ansi"],"scope":case["scope"],
                "batch":batch_size,"spark":case["expected"],"fallback_rows":rows,
                "executed":case["schema_stages"]["executed"],"actual":observed})
            );
            assert!(
                date_format_result_differences(case, &observed, &rows).is_empty(),
                "zone fallback: {case}; {observed}"
            );
        }
    }
    Ok(())
}

/// Spark records by id, session time zone and ANSI mode, and the registered rows that
/// DATE_FORMAT column queries read.
struct References<'a> {
    records: HashMap<(String, String, bool), &'a Value>,
    /// `temporal_values` rows: key, DATE, TIMESTAMP, TIMESTAMP_NTZ and a per-row pattern.
    values: &'a [Value],
}

impl<'a> References<'a> {
    fn new(header: &'a Value, records: &'a [Value]) -> TestResult<Self> {
        let mut index = HashMap::new();
        for record in records {
            let key = (
                text(record, "id")?.to_owned(),
                text(record, "timezone")?.to_owned(),
                record["ansi"].as_bool().ok_or("missing ANSI mode")?,
            );
            if let Some(duplicate) = index.insert(key, record) {
                return Err(format!("duplicate record {}", duplicate["id"]).into());
            }
        }
        let values = header["inputs"]
            .as_array()
            .ok_or("missing inputs")?
            .iter()
            .find(|input| input["name"] == "temporal_values")
            .and_then(|input| input["rows"].as_array())
            .ok_or("missing temporal_values rows")?;
        Ok(Self {
            records: index,
            values,
        })
    }

    /// The record with `id` in `timezone` and the same ANSI mode as `case`.
    fn sibling(&self, case: &Value, id: &str, timezone: &str) -> TestResult<&'a Value> {
        let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
        self.records
            .get(&(id.to_owned(), timezone.to_owned(), ansi))
            .copied()
            .ok_or_else(|| format!("missing record {id} in {timezone}").into())
    }

    /// The `temporal_values` row with `key`.
    fn values_row(&self, key: &Value) -> TestResult<&'a Value> {
        self.values
            .iter()
            .find(|row| row[0] == *key)
            .ok_or_else(|| format!("missing temporal_values row {key}").into())
    }
}

/// Encode one value by its typed Arrow representation, as the capture encodes Spark's
/// internal rows: DATE days, TIMESTAMP UTC-epoch and TIMESTAMP_NTZ wall-clock microseconds.
fn encode_typed_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Int32 => json!(column.as_primitive::<Int32Type>().value(row).to_string()),
        DataType::Date32 => json!(column.as_primitive::<Date32Type>().value(row).to_string()),
        DataType::Int64 => json!(column.as_primitive::<Int64Type>().value(row).to_string()),
        DataType::Timestamp(TimeUnit::Microsecond, _) => json!(
            column
                .as_primitive::<TimestampMicrosecondType>()
                .value(row)
                .to_string()
        ),
        DataType::Utf8 => json!(column.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => json!(column.as_string::<i64>().value(row)),
        DataType::Utf8View => json!(column.as_string_view().value(row)),
        other => return Err(format!("unexpected temporal result type: {other}").into()),
    })
}

fn sort_rows(rows: &mut [Value]) {
    rows.sort_by_key(Value::to_string);
}

/// Observe analyzed (public names), optimized and executed schemas, typed rows and the
/// failing phase in the record's session time zone.
async fn observe(session: &SessionContext, case: &Value) -> Value {
    let mut observation = json!({});
    let mut phase = "resolve";
    let result: TestResult<Vec<Value>> = async {
        let options = SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            session_timezone: case["timezone"].as_str().ok_or("missing time zone")?.into(),
            ..SparkSqlOptions::default()
        };
        let query = plan_query(
            session,
            case["sql"].as_str().ok_or("missing SQL")?,
            &options,
        )
        .await?;
        observation["analyzed"] = capture_schema(
            query.logical_plan.schema().fields(),
            Some(&query.column_names),
        )?;
        phase = "optimized";
        let mut state = session.state();
        let extensions = &mut state.config_mut().options_mut().extensions;
        extensions.insert(SparkSqlExpressionSemantics::default());
        extensions.insert(PreserveAggregateStatisticsSchema::default());
        let optimized = state.optimize(&query.logical_plan)?;
        observation["optimized"] =
            capture_schema(optimized.schema().fields(), Some(&query.column_names))?;
        let physical = query.create_physical_plan(session).await?;
        phase = "collect";
        let batches = collect(physical.clone(), session.task_ctx()).await?;
        let schema = physical.schema();
        observation["executed"] = capture_schema(schema.fields(), None)?;
        let mut rows = Vec::new();
        for batch in batches {
            validate_batch(schema.as_ref(), &batch)?;
            for row in 0..batch.num_rows() {
                rows.push(Value::from(
                    batch
                        .columns()
                        .iter()
                        .map(|column| encode_typed_value(column.as_ref(), row))
                        .collect::<TestResult<Vec<_>>>()?,
                ));
            }
        }
        sort_rows(&mut rows);
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

/// DAR parser messages accepted for a Spark error condition at the same stage, besides the
/// condition itself. DAR reports these parse failures with its parser's message instead of
/// Spark's condition.
fn accepted_messages(condition: &str) -> &'static [&'static str] {
    match condition {
        "CAST_INVALID_INPUT" => &[
            "error in SQL parser: found",
            "invalid argument: DateValue {",
            "invalid argument: TimeValue {",
            "invalid timestamp: leap seconds are not supported",
        ],
        "INVALID_TYPED_LITERAL" => &[
            "invalid argument: found",
            "invalid argument: DateValue {",
            "invalid argument: TimeValue {",
        ],
        "CANNOT_PARSE_TIMESTAMP" => &[
            "expected numeric datetime field at byte offset",
            "expected datetime literal",
            "invalid parsed date:",
            "datetime value does not match format at byte offset",
            "Invalid value for HourOfDay",
        ],
        _ => &[],
    }
}

/// Match the expected failing stage and an accepted form of Spark's error condition, or
/// the DAR message that a retained expectation names.
fn matches_expected_error(expectation: &Value, observation: &Value) -> bool {
    let expected_phase = match expectation["spark_error_phase"].as_str() {
        Some("sql") => "resolve",
        Some(phase) => phase,
        None => return false,
    };
    let Some(error) = observation["error"].as_str() else {
        return false;
    };
    let expected = &expectation["expected"];
    let message_matches = match (
        expected["condition"].as_str(),
        expected["dar_message"].as_str(),
    ) {
        (Some(condition), None) => {
            error.contains(&format!("[{condition}]"))
                || accepted_messages(condition)
                    .iter()
                    .any(|message| error.contains(message))
        }
        (None, Some(message)) => error.contains(message),
        _ => false,
    };
    observation["error_phase"] == expected_phase && message_matches
}

/// The recorded stages and outcome parts of an expectation that differ from the observation.
fn differing_parts(expectation: &Value, observation: &Value) -> Vec<&'static str> {
    let mut differing = Vec::new();
    for stage in ["analyzed", "optimized", "executed"] {
        if expectation["schema_stages"]
            .get(stage)
            .is_some_and(|expected| observation[stage] != *expected)
        {
            differing.push(stage);
        }
    }
    let outcome_matches = match expectation["expected"]["status"].as_str() {
        Some("ok") => observation["rows"] == expectation["expected"]["rows"],
        Some("error") => matches_expected_error(expectation, observation),
        _ => false,
    };
    if !outcome_matches {
        differing.push("outcome");
    }
    differing
}

/// Existing differences from Spark, each named for DAR's current behavior and owned by an
/// issue. A record can have several.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RetainedDifference {
    /// #1236: typed TIMESTAMP literals roll second 60 over to the next minute.
    RolledOverLeapSecond,
    /// #1010: invalid datetime patterns use DAR's message, and `#`, `{` and `}` are
    /// formatted as literal text.
    DarPatternErrors,
    /// #1011: DATE and TIMESTAMP strings with years beyond the date library's range behave
    /// like the invalid string 'bad'.
    OutOfRangeStrings,
    /// #1236: long localized names use region IDs, and historical short names use TZDB LMT.
    LocalizedZoneNames,
    /// #1016: DATE_FORMAT, ANSI formatted parsing and temporal-to-STRING casts are nullable
    /// at analysis, where Spark declares them required.
    NullableFormattingResults,
}

/// Strings beyond the date library's range in the `parse_inputs` table.
const OUT_OF_RANGE_INPUTS: [&str; 3] = ["timestamp_min", "timestamp_max", "timestamp_over_max"];

fn text<'v>(value: &'v Value, name: &str) -> TestResult<&'v str> {
    value[name]
        .as_str()
        .ok_or_else(|| format!("missing {name}").into())
}

/// Queries without a FROM clause read only literals.
fn reads_only_literals(case: &Value) -> TestResult<bool> {
    Ok(!text(case, "sql")?.contains(" FROM "))
}

/// The `parse_inputs` key a column query selects besides the NULL row.
fn parse_input_key(case: &Value) -> TestResult<&str> {
    let sql = text(case, "sql")?;
    let start = sql.find("WHERE k IN (").ok_or("missing input key")? + "WHERE k IN (".len();
    let length = sql[start..].find(',').ok_or("missing input key end")?;
    Ok(&sql[start..start + length])
}

/// A record without Spark's free-text error message, which no comparison reads.
fn without_messages(record: &Value) -> Value {
    let mut record = record.clone();
    if let Some(expected) = record["expected"].as_object_mut() {
        expected.remove("message");
        expected.remove("exception");
    }
    record
}

/// #1011: DAR currently treats an out-of-range string exactly like the invalid string 'bad'
/// for the same operator: replace the outcome, stages and failing stage with that record's.
fn rewrite_out_of_range(
    current: &mut Value,
    case: &Value,
    references: &References,
) -> TestResult<bool> {
    let input = text(case, "input")?;
    let id = text(case, "id")?.replacen(&format!("parse_{input}_"), "parse_bad_", 1);
    let bad = references.sibling(case, &id, text(case, "timezone")?)?;
    let mut derived = case.clone();
    derived["expected"] = bad["expected"].clone();
    derived["schema_stages"] = bad["schema_stages"].clone();
    match bad.get("spark_error_phase") {
        Some(phase) => derived["spark_error_phase"] = phase.clone(),
        None => {
            derived
                .as_object_mut()
                .ok_or("record is not an object")?
                .remove("spark_error_phase");
        }
    }
    if !reads_only_literals(case)? && derived["expected"]["status"] == "ok" {
        let (bad_key, key) = (parse_input_key(bad)?, parse_input_key(case)?);
        let rows = derived["expected"]["rows"]
            .as_array_mut()
            .ok_or("missing rows")?;
        for row in rows.iter_mut() {
            if row[0] == bad_key {
                row[0] = json!(key);
            }
        }
        sort_rows(rows);
    }
    if without_messages(&derived) == without_messages(current) {
        return Ok(false);
    }
    *current = derived;
    Ok(true)
}

/// Whether a datetime pattern has one of `letters` outside quoted text.
fn has_pattern_letter(pattern: &str, letters: &[char]) -> bool {
    let mut quoted = false;
    pattern.chars().any(|ch| {
        if ch == '\'' {
            quoted = !quoted;
            return false;
        }
        !quoted && letters.contains(&ch)
    })
}

/// Pattern letters that print a zone name or offset.
const ZONE_LETTERS: &[char] = &['z', 'V', 'O', 'X', 'x', 'Z'];
/// Pattern letters that print an hour.
const HOUR_LETTERS: &[char] = &['H', 'h', 'K', 'k'];

/// The input kinds a DATE_FORMAT record formats in each output column.
fn input_kinds(case: &Value) -> TestResult<Vec<&str>> {
    case["inputs"]
        .as_array()
        .ok_or("missing input kinds")?
        .iter()
        .map(|kind| kind.as_str().ok_or_else(|| "missing input kind".into()))
        .collect()
}

/// The pattern a DATE_FORMAT record applies to a row: its own, or the row's registered one.
fn row_pattern<'a>(
    case: &'a Value,
    row: &[Value],
    references: &References<'a>,
) -> TestResult<&'a str> {
    if let Some(pattern) = case["pattern"].as_str() {
        return Ok(pattern);
    }
    if case["id"] != "format_column_pattern" {
        return Err(format!("unknown pattern for {}", case["id"]).into());
    }
    let key = row.first().ok_or("empty row")?;
    references.values_row(key)?[4]
        .as_str()
        .ok_or_else(|| "missing registered pattern".into())
}

/// Keep only the localized-name boundary: modern short names, offsets and local-time
/// interpretation use the raw Spark results. This corpus has long `zzzz` patterns and
/// Los Angeles local-mean-time rows whose installed TZDB short name is LMT rather than PST.
fn rewrite_localized_zone_names(
    current: &mut Value,
    case: &Value,
    references: &References,
    retained: &mut BTreeSet<RetainedDifference>,
) -> TestResult {
    let timezone = text(case, "timezone")?;
    let kinds = input_kinds(case)?;
    let literal = reads_only_literals(case)?;
    let rows = current["expected"]["rows"]
        .as_array_mut()
        .ok_or("missing rows")?;
    for row in rows {
        if row
            .as_array()
            .ok_or("missing row")?
            .iter()
            .skip(1)
            .all(Value::is_null)
        {
            continue;
        }
        let pattern = if let Some(pattern) = case["pattern"].as_str() {
            pattern
        } else if case["id"] == "format_column_pattern" {
            row_pattern(case, row.as_array().ok_or("missing row")?, references)?
        } else {
            continue;
        };
        let long_name = pattern == "zzzz";
        let short_name = !long_name && has_pattern_letter(pattern, &['z']);
        if !long_name && !short_name {
            continue;
        }
        let inputs = if !literal {
            Some(references.values_row(&row[0])?)
        } else {
            None
        };
        let row = row.as_array_mut().ok_or("missing row")?;
        for (index, kind) in kinds.iter().enumerate() {
            if matches!(*kind, "key" | "null") {
                continue;
            }
            let Some(value) = row[index].as_str() else {
                continue;
            };
            let mut replaced = value.to_owned();
            if long_name {
                for name in [
                    "Coordinated Universal Time",
                    "Pacific Standard Time",
                    "Pacific Daylight Time",
                ] {
                    replaced = replaced.replace(name, timezone);
                }
            } else if timezone == LOS_ANGELES
                && let Some(inputs) = inputs
            {
                let source_index = match *kind {
                    "date" => 1,
                    "timestamp_column" => 2,
                    "timestamp_ntz" => 3,
                    other => return Err(format!("unexpected historical input {other}").into()),
                };
                if let Some(stored) = inputs[source_index].as_str() {
                    let stored = stored.parse::<i128>()?;
                    let micros = if *kind == "date" {
                        stored * 86_400_000_000
                    } else {
                        stored
                    };
                    // Los Angeles standardized its local mean time on 1883-11-18 at 20:00 UTC.
                    if micros < -2_717_640_000_000_000 {
                        replaced = replaced.replace("PST", "LMT");
                    }
                }
            }
            if replaced != value {
                row[index] = json!(replaced);
                retained.insert(RetainedDifference::LocalizedZoneNames);
            }
        }
    }
    Ok(())
}

/// #1010: DAR formats `#`, `{` and `}` as literal text for every non-NULL input.
fn reserved_pattern_rows(
    case: &Value,
    pattern: &str,
    references: &References,
) -> TestResult<Vec<Value>> {
    let kinds = input_kinds(case)?;
    if reads_only_literals(case)? {
        return Ok(vec![json!(vec![pattern; kinds.len()])]);
    }
    let mut rows = references
        .values
        .iter()
        .map(|values| {
            kinds
                .iter()
                .map(|kind| {
                    let source = match *kind {
                        "key" => return Ok(values[0].clone()),
                        "date" => &values[1],
                        "timestamp_column" => &values[2],
                        "timestamp_ntz" => &values[3],
                        other => return Err(format!("unexpected input kind {other}").into()),
                    };
                    Ok(if source.is_null() {
                        Value::Null
                    } else {
                        json!(pattern)
                    })
                })
                .collect::<TestResult<Vec<_>>>()
                .map(Value::from)
        })
        .collect::<TestResult<Vec<_>>>()?;
    sort_rows(&mut rows);
    Ok(rows)
}

/// Replace only the retained parts of Spark's expectation with the current DAR result.
fn current_expectation(
    case: &Value,
    references: &References,
) -> TestResult<(Value, BTreeSet<RetainedDifference>)> {
    let mut current = case.clone();
    let mut retained = BTreeSet::new();
    let section = text(case, "section")?;
    let timezone = text(case, "timezone")?;
    let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
    let literal = reads_only_literals(case)?;
    let input = case["input"].as_str().unwrap_or_default();
    let operator = case["operator"].as_str().unwrap_or_default();

    if section == "parse"
        && OUT_OF_RANGE_INPUTS.contains(&input)
        && rewrite_out_of_range(&mut current, case, references)?
    {
        retained.insert(RetainedDifference::OutOfRangeStrings);
    }
    if section == "parse"
        && input == "second_60"
        && matches!(
            operator,
            "timestamp_literal" | "timestamp_ntz_literal" | "timestamp_ltz_literal"
        )
    {
        if case["expected"]["condition"] != "INVALID_TYPED_LITERAL"
            || case["spark_error_phase"] != "sql"
        {
            return Err("expected Spark's invalid typed literal for second 60".into());
        }
        // Midnight of 2024-01-02 in the session zone, or as a wall-clock time, with the
        // fields of the same typed literal for a valid time.
        let midnight = if timezone == LOS_ANGELES && operator != "timestamp_ntz_literal" {
            "1704182400000000"
        } else {
            "1704153600000000"
        };
        let valid = text(case, "id")?.replacen("parse_second_60_", "parse_fraction_0_", 1);
        current["expected"] = json!({"status": "ok", "rows": [[midnight]]});
        current["schema_stages"] =
            references.sibling(case, &valid, timezone)?["schema_stages"].clone();
        current
            .as_object_mut()
            .ok_or("record is not an object")?
            .remove("spark_error_phase");
        retained.insert(RetainedDifference::RolledOverLeapSecond);
    }
    if current["expected"]["status"] == "error" {
        let condition = current["expected"]["condition"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        if condition.starts_with("INCONSISTENT_BEHAVIOR_CROSS_VERSION.DATETIME_")
            || condition.starts_with("INVALID_DATETIME_PATTERN.")
        {
            let pattern = case["pattern"].as_str().unwrap_or_default();
            if section == "format" && matches!(pattern, "#" | "{" | "}") {
                let rows = reserved_pattern_rows(case, pattern, references)?;
                current["expected"] = json!({"status": "ok", "rows": rows});
                current
                    .as_object_mut()
                    .ok_or("record is not an object")?
                    .remove("spark_error_phase");
            } else {
                current["expected"] = json!({"status": "error",
                    "dar_message": "invalid datetime pattern: "});
                current["spark_error_phase"] = if literal
                    && ansi
                    && section == "formatted"
                    && matches!(
                        operator,
                        "to_date" | "to_timestamp" | "to_timestamp_ntz" | "to_timestamp_ltz"
                    ) {
                    case["spark_error_phase"].clone()
                } else {
                    json!("collect")
                };
            }
            retained.insert(RetainedDifference::DarPatternErrors);
        }
    }
    // Look up the analyzed stage without inserting it into records that failed earlier.
    if literal
        && let Some(fields) = current
            .pointer_mut("/schema_stages/analyzed/fields")
            .and_then(Value::as_array_mut)
    {
        let required = match section {
            "format" => 0..fields.len(),
            "formatted"
                if ansi
                    && !matches!(operator, "try_to_timestamp" | "try_to_date")
                    && !matches!(input, "null_string" | "null_pattern") =>
            {
                0..1
            }
            "convert" if matches!(operator, "timestamp_to_string" | "ntz_to_string") => 0..1,
            _ => 0..0,
        };
        let mut declared_nullable = false;
        for field in &mut fields[required] {
            if field["nullable"] == false {
                field["nullable"] = json!(true);
                declared_nullable = true;
            }
        }
        if declared_nullable {
            retained.insert(RetainedDifference::NullableFormattingResults);
        }
    }
    if section == "format" && case["expected"]["status"] == "ok" {
        rewrite_localized_zone_names(&mut current, case, references, &mut retained)?;
    }
    Ok((current, retained))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn temporal_contracts_match_spark() -> TestResult {
    let records = temporal_references()?;
    let references = References::new(&records[0], &records[1..])?;
    let mut expectations = Vec::new();
    let mut retained = BTreeMap::new();
    for case in &records[1..] {
        let (expectation, differences) = current_expectation(case, &references)?;
        for difference in differences {
            *retained.entry(difference).or_insert(0) += 1;
        }
        expectations.push(expectation);
    }
    let retained = retained.into_iter().collect::<Vec<_>>();
    let mut differences = Vec::new();
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
        for (case, expectation) in records[1..].iter().zip(&expectations) {
            let observation = observe(&session, case).await;
            let differing = differing_parts(expectation, &observation);
            if !differing.is_empty() {
                differences.push(json!({
                    "id": case["id"], "timezone": case["timezone"], "ansi": case["ansi"],
                    "batch": batch_size, "sql": case["sql"], "differing": differing,
                    "spark": {"expected": case["expected"],
                        "spark_error_phase": case["spark_error_phase"],
                        "schema_stages": case["schema_stages"]},
                    "current": {"expected": expectation["expected"],
                        "spark_error_phase": expectation["spark_error_phase"]},
                    "observed": observation,
                }));
            }
        }
    }
    assert!(
        differences.is_empty(),
        "retained {retained:?}; {} differences:\n{}",
        differences.len(),
        differences
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert_eq!(
        retained,
        [
            (RetainedDifference::RolledOverLeapSecond, 12),
            (RetainedDifference::DarPatternErrors, 340),
            (RetainedDifference::OutOfRangeStrings, 236),
            (RetainedDifference::LocalizedZoneNames, 16),
            (RetainedDifference::NullableFormattingResults, 712),
        ]
    );
    Ok(())
}

/// The accepted parser messages are specific to their conditions, and a retained DAR
/// message must occur at the expected stage.
#[test]
fn parser_messages_name_their_conditions() {
    const PARSED_DATE: &str = "Execution error: invalid parsed date: input is out of range";
    const PATTERN: &str = "Execution error: invalid datetime pattern: unexpected ']'";
    const DATE_VALUE: &str = "External error: invalid argument: DateValue { year: 2024 }";
    let error = |condition: &str, phase: &str| {
        json!({"expected": {"status": "error", "condition": condition},
            "spark_error_phase": phase})
    };
    let retained = |message: &str, phase: &str| {
        json!({"expected": {"status": "error", "dar_message": message},
            "spark_error_phase": phase})
    };
    let observed = |message: &str, phase: &str| json!({"error": message, "error_phase": phase});
    assert!(matches_expected_error(
        &error("CANNOT_PARSE_TIMESTAMP", "collect"),
        &observed(PARSED_DATE, "collect")
    ));
    assert!(!matches_expected_error(
        &error("CANNOT_PARSE_TIMESTAMP", "optimized"),
        &observed(PARSED_DATE, "collect")
    ));
    assert!(!matches_expected_error(
        &error("CAST_INVALID_INPUT", "collect"),
        &observed(PARSED_DATE, "collect")
    ));
    assert!(!matches_expected_error(
        &error("INVALID_DATETIME_PATTERN.WITH_SUGGESTION", "collect"),
        &observed(PATTERN, "collect")
    ));
    assert!(matches_expected_error(
        &error("INVALID_TYPED_LITERAL", "sql"),
        &observed(DATE_VALUE, "resolve")
    ));
    assert!(matches_expected_error(
        &retained("invalid datetime pattern: ", "collect"),
        &observed(PATTERN, "collect")
    ));
    assert!(!matches_expected_error(
        &retained("invalid datetime pattern: ", "optimized"),
        &observed(PATTERN, "collect")
    ));
    assert!(!matches_expected_error(
        &retained("invalid datetime pattern: ", "collect"),
        &observed(PARSED_DATE, "collect")
    ));
}

/// Pattern letters inside quoted text are literal text.
#[test]
fn pattern_letters_outside_quotes_are_fields() {
    for pattern in ["z", "HH:mm VV", "'T'X", "yyyy 'it''s' O"] {
        assert!(has_pattern_letter(pattern, ZONE_LETTERS), "{pattern}");
    }
    for pattern in ["HH:mm", "'z'", "yyyy-MM-dd'T'HH", "EEEE, MMMM d, yyyy"] {
        assert!(!has_pattern_letter(pattern, ZONE_LETTERS), "{pattern}");
    }
    for pattern in ["HH", "hh:mm a", "K", "yyyy-MM-dd'T'kk"] {
        assert!(has_pattern_letter(pattern, HOUR_LETTERS), "{pattern}");
    }
    // A zone name on the skipped local time prints no hour.
    for pattern in ["z", "'HH' VV", "mm:ss", "a"] {
        assert!(!has_pattern_letter(pattern, HOUR_LETTERS), "{pattern}");
    }
}
