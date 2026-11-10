//! Every built-in scalar function gives a dictionary-encoded STRING argument, as Delta's provider
//! gives a STRING partition column, the values and executed result type it gives the same
//! argument as a plain STRING (#1242). Comparisons with literals keep the bare column, so the
//! Delta scan can still prune by them.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_plan::function::{BUILT_IN_GENERATOR_FUNCTIONS, BUILT_IN_SCALAR_FUNCTIONS};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// STRING values for many function families: a date, a timestamp, a number, padded text, JSON,
/// a list, a URL, a format pattern and NULL.
const STRINGS: [Option<&str>; 9] = [
    Some("2024-05-17"),
    Some("2024-03-10 02:30:00"),
    Some("12"),
    Some(" Hello, World "),
    Some(r#"{"a": 1, "b": [1, 2]}"#),
    Some("a,b,c"),
    Some("https://spark.apache.org/docs?q=1#top"),
    Some("yyyy-MM-dd"),
    None,
];
const PATTERNS: [Option<&str>; 9] = [
    Some("yyyy-MM-dd"),
    Some("yyyy-MM-dd HH:mm:ss"),
    Some("1"),
    Some("l"),
    Some("$.a"),
    Some(","),
    Some("HOST"),
    Some("UTC"),
    Some("x"),
];

/// Argument lists, `s` and `t` being the STRING columns under test and `n` an INT column.
const SHAPES: [&str; 12] = [
    "s",
    "s, t",
    "s, 1",
    "s, n",
    "s, 'a'",
    "s, 1, 2",
    "s, t, t",
    "s, 1, 'x'",
    "'a', s",
    "1, s",
    "t, s",
    "s, s",
];

/// Aggregates that take a STRING argument; their registry is not public.
const AGGREGATES: [&str; 22] = [
    "min(s)",
    "max(s)",
    "count(s)",
    "count(DISTINCT s)",
    "first(s)",
    "last(s)",
    "any_value(s)",
    "first_value(s)",
    "last_value(s)",
    "collect_list(s)",
    "collect_set(s)",
    "mode(s)",
    "approx_count_distinct(s)",
    "max_by(s, n)",
    "min_by(s, n)",
    "max_by(n, s)",
    "min_by(n, s)",
    "string_agg(s, ',')",
    "listagg(s, ',')",
    "array_agg(s)",
    "hll_sketch_agg(s)",
    "count_min_sketch(s, 0.1D, 0.9D, 1)",
];

/// Conversions written as expressions rather than function calls, and window functions.
const EXPRESSIONS: [&str; 23] = [
    "CAST(s AS INT)",
    "CAST(s AS BIGINT)",
    "CAST(s AS DOUBLE)",
    "CAST(s AS DECIMAL(10, 2))",
    "CAST(t AS BOOLEAN)",
    "CAST(s AS DATE)",
    "CAST(s AS TIMESTAMP)",
    "CAST(s AS BINARY)",
    "CAST(s AS STRING)",
    "TRY_CAST(s AS INT)",
    "s::int",
    "s BETWEEN 1 AND 20",
    "s IS DISTINCT FROM 12",
    "s IS NOT DISTINCT FROM 'a,b,c'",
    "collect_list(s) OVER (ORDER BY n)",
    "max(s) OVER (ORDER BY n)",
    "lag(s) OVER (ORDER BY n)",
    "lead(s, 1, 'x') OVER (ORDER BY n)",
    "first_value(s) OVER (ORDER BY n)",
    "nth_value(s, 2) OVER (ORDER BY n)",
    "count(s) OVER (PARTITION BY s ORDER BY n)",
    "CASE s WHEN 'a,b,c' THEN 1 ELSE 0 END",
    "CASE WHEN n > 4 THEN s ELSE t END",
];

/// Comparisons and predicates over the bare column, which must keep it bare for pruning.
const PREDICATES: [&str; 10] = [
    "s = 'a,b,c'",
    "s <> 'a,b,c'",
    "s < 'b'",
    "s <=> 'a,b,c'",
    "s IN ('a,b,c', '12')",
    "s LIKE 'a%'",
    "s IS NULL",
    "s = t",
    "s = 12",
    "s BETWEEN 'a' AND 'z'",
];

fn input_batch(dictionary: bool) -> TestResult<RecordBatch> {
    let strings: ArrayRef = Arc::new(StringArray::from(STRINGS.to_vec()));
    let patterns: ArrayRef = Arc::new(StringArray::from(PATTERNS.to_vec()));
    let numbers: ArrayRef = Arc::new(Int32Array::from((1..=9).collect::<Vec<i32>>()));
    let string_type = if dictionary {
        DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8))
    } else {
        DataType::Utf8
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("s", string_type.clone(), true),
        Field::new("t", string_type.clone(), true),
        Field::new("n", DataType::Int32, false),
    ]));
    Ok(RecordBatch::try_new(
        schema,
        vec![
            arrow::compute::cast(&strings, &string_type)?,
            arrow::compute::cast(&patterns, &string_type)?,
            numbers,
        ],
    )?)
}

fn session(dictionary: bool) -> TestResult<SessionContext> {
    let session = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(SessionConfig::new().with_target_partitions(1)),
        )
        .build(),
    );
    let batch = input_batch(dictionary)?;
    session.register_table(
        "inputs",
        Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]])?),
    )?;
    Ok(session)
}

/// A result: its type, a dictionary counting as its values, and its rows as text.
#[derive(Debug, PartialEq)]
struct Outcome {
    data_type: DataType,
    rows: Vec<String>,
}

fn value_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Dictionary(_, value) => value_type(value),
        other => other.clone(),
    }
}

/// Run `sql` in its own task, so that a panic is a failure of this call alone.
async fn run(
    session: &SessionContext,
    sql: &str,
    ansi_mode: bool,
    timezone: &str,
) -> Result<Outcome, String> {
    let (session, sql, timezone) = (session.clone(), sql.to_owned(), timezone.to_owned());
    let task = tokio::spawn(async move {
        let options = SparkSqlOptions {
            ansi_mode,
            session_timezone: timezone.into(),
            ..SparkSqlOptions::default()
        };
        execute(&session, &sql, &options)
            .await
            .map_err(|error| error.to_string())
    });
    task.await
        .unwrap_or_else(|error| Err(format!("panicked: {error}")))
}

async fn execute(
    session: &SessionContext,
    sql: &str,
    options: &SparkSqlOptions,
) -> TestResult<Outcome> {
    let query = plan_query(session, sql, options).await?;
    let physical = query.create_physical_plan(session).await?;
    let schema = physical.schema();
    let batches = collect(physical, session.task_ctx()).await?;
    let field = schema.field(0);
    let data_type = value_type(field.data_type());
    let mut rows = Vec::new();
    for batch in batches {
        let column = arrow::compute::cast(batch.column(0), &data_type)?;
        let formatter = ArrayFormatter::try_new(column.as_ref(), &FormatOptions::default())?;
        for row in 0..batch.num_rows() {
            rows.push(formatter.value(row).to_string());
        }
    }
    Ok(Outcome { data_type, rows })
}

/// The calls the sweep makes: each identifier-named built-in scalar and generator function with
/// each argument shape, each predicate, each aggregate and each other expression.
fn calls() -> Vec<String> {
    let names = BUILT_IN_SCALAR_FUNCTIONS
        .keys()
        .chain(BUILT_IN_GENERATOR_FUNCTIONS.keys())
        .filter(|name| name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
        .collect::<BTreeSet<_>>();
    let mut calls = names
        .into_iter()
        .flat_map(|name| SHAPES.iter().map(move |shape| format!("{name}({shape})")))
        .collect::<Vec<_>>();
    calls.extend(PREDICATES.iter().map(|predicate| predicate.to_string()));
    calls.extend(AGGREGATES.iter().map(|aggregate| aggregate.to_string()));
    calls.extend(EXPRESSIONS.iter().map(|expression| expression.to_string()));
    calls
}

/// The calls whose dictionary-encoded result differs from the plain STRING one, where the plain
/// call succeeds and gives the same rows twice.
async fn sweep(ansi_mode: bool, timezone: &str) -> TestResult<Vec<String>> {
    let plain = session(false)?;
    let dictionary = session(true)?;
    let mut differences = Vec::new();
    for call in calls() {
        let sql = format!("SELECT {call} AS r FROM inputs");
        let Ok(expected) = run(&plain, &sql, ansi_mode, timezone).await else {
            continue;
        };
        if run(&plain, &sql, ansi_mode, timezone).await.ok().as_ref() != Some(&expected) {
            continue;
        }
        match run(&dictionary, &sql, ansi_mode, timezone).await {
            Ok(observed) if observed == expected => {}
            Ok(observed) => differences.push(format!(
                "{call}: {:?} {:?} instead of {:?} {:?}",
                observed.data_type, observed.rows, expected.data_type, expected.rows
            )),
            Err(error) => {
                let error = error.lines().next().unwrap_or_default();
                differences.push(format!("{call}: fails: {error}"));
            }
        }
    }
    Ok(differences)
}

fn assert_no_differences(differences: &[String]) {
    assert!(
        differences.is_empty(),
        "{} differences:\n{}",
        differences.len(),
        differences.join("\n")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dictionary_strings_match_plain_strings_with_ansi() -> TestResult {
    let mut differences = sweep(true, "UTC").await?;
    differences.extend(sweep(true, "America/Los_Angeles").await?);
    assert_no_differences(&differences);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dictionary_strings_match_plain_strings_without_ansi() -> TestResult {
    let mut differences = sweep(false, "UTC").await?;
    differences.extend(sweep(false, "America/Los_Angeles").await?);
    assert_no_differences(&differences);
    Ok(())
}

/// A comparison of the bare dictionary-encoded column with a STRING literal keeps the column
/// uncast, so that the Delta scan can still prune by it. LIKE casts it whatever its encoding, and
/// the scan does not prune by LIKE.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn string_predicates_keep_the_dictionary_column() -> TestResult {
    let session = session(true)?;
    let options = SparkSqlOptions::default();
    for predicate in PREDICATES
        .iter()
        .filter(|predicate| !predicate.contains("12") && !predicate.contains("LIKE"))
    {
        let sql = format!("SELECT n FROM inputs WHERE {predicate}");
        let query = plan_query(&session, &sql, &options).await?;
        let plan = session.state().optimize(&query.logical_plan)?;
        let text = plan.display_indent().to_string();
        assert!(
            !text.contains("CAST(inputs.s") && !text.contains("CAST(inputs.t"),
            "{predicate} casts the dictionary column:\n{text}"
        );
    }
    Ok(())
}
