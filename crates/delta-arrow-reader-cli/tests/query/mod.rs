//! Exact SQL results, read-only admission, output ceilings, and safe failures.

use std::{
    fs,
    io::Cursor,
    path::PathBuf,
    process::{Command, Output},
};

use arrow::{
    array::{Array, Int32Array, Int64Array, StringArray},
    compute::{cast, concat_batches, sort_to_indices, take_record_batch},
    datatypes::DataType,
    ipc::reader::{FileReader, StreamReader},
    record_batch::RecordBatch,
};

use super::{diagnostics::assert_single_terminal_record, run_with_timeout};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
const IPC_END: &[u8] = &[255, 255, 255, 255, 0, 0, 0, 0];

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus")
        .join(name)
}

/// Keeps the SQL file alive for every command built from this fixture.
struct SqlCommandFixture {
    directory: tempfile::TempDir,
    table: PathBuf,
}

impl SqlCommandFixture {
    fn new(table: impl Into<PathBuf>, sql: impl AsRef<[u8]>) -> TestResult<Self> {
        let directory = tempfile::tempdir()?;
        fs::write(directory.path().join("query.sql"), sql)?;
        Ok(Self {
            directory,
            table: table.into(),
        })
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
        command
            .arg("query")
            .arg(&self.table)
            .arg("--sql-file")
            .arg(self.directory.path().join("query.sql"))
            .arg("--diagnostics=json");
        command
    }
}

fn decode_query_result(output: &Output) -> TestResult<RecordBatch> {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.ends_with(IPC_END));
    let reader = StreamReader::try_new(Cursor::new(&output.stdout), None)?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    let result = concat_batches(&schema, &batches)?;
    let record = assert_single_terminal_record(&output.stderr, "completion");
    assert_eq!(record["scope"], "query");
    assert_eq!(record["completion"], "complete");
    assert_eq!(record["stop_reason"], "end_of_input");
    assert_eq!(record["rows"], result.num_rows().to_string());
    assert_eq!(record["output_bytes"], output.stdout.len().to_string());
    Ok(result)
}

fn read_expected_rows(name: &str) -> TestResult<RecordBatch> {
    let reader = FileReader::try_new(
        fs::File::open(fixture_path(name).join("expected.arrow"))?,
        None,
    )?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    Ok(concat_batches(&schema, &batches)?)
}

#[test]
fn published_sql_examples_execute_with_the_advertised_table_binding() -> TestResult {
    let output = run_with_timeout(Command::new(env!("CARGO_BIN_EXE_dar")).args([
        "capabilities",
        "--command",
        "query",
    ]))?;
    assert!(output.status.success());
    let inventory: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let support = &inventory["read_support"]["query"];
    assert_eq!(support["registered_tables"], serde_json::json!(["t"]));
    assert_eq!(support["max_table_bindings"], 1);
    assert_eq!(support["self_joins"], true);
    for example in support["sql_input"]["examples"].as_array().unwrap() {
        let fixture = SqlCommandFixture::new(
            fixture_path("partitioned").join("table"),
            example.as_str().unwrap(),
        )?;
        let result = decode_query_result(&run_with_timeout(&mut fixture.command())?)?;
        assert!(result.num_rows() > 0, "{example}");
    }
    Ok(())
}

#[test]
fn sql_preserves_values_nulls_deletion_vectors_ordering_and_empty_results() -> TestResult {
    for name in ["partitioned", "deletion_vectors", "nested_mapping"] {
        let expected = read_expected_rows(name)?;
        let columns = if name == "partitioned" {
            vec!["id", "value"]
        } else {
            vec!["id"]
        };
        let indices = columns
            .iter()
            .map(|column| expected.schema().index_of(column))
            .collect::<Result<Vec<_>, _>>()?;
        let expected = expected.project(&indices)?;
        let expected =
            take_record_batch(&expected, &sort_to_indices(expected.column(0), None, None)?)?;
        let fixture = SqlCommandFixture::new(
            fixture_path(name).join("table"),
            format!("SELECT {} FROM t ORDER BY id", columns.join(", ")),
        )?;
        let actual = decode_query_result(&run_with_timeout(&mut fixture.command())?)?;
        assert_eq!(actual.columns(), expected.columns());
        // Spark's oracle omits Delta column-mapping metadata; compare the logical fields.
        for (actual, expected) in actual
            .schema()
            .fields()
            .iter()
            .zip(expected.schema().fields())
        {
            assert_eq!(actual.name(), expected.name());
            assert_eq!(actual.data_type(), expected.data_type());
            assert_eq!(actual.is_nullable(), expected.is_nullable());
        }
        let fixture = SqlCommandFixture::new(
            fixture_path(name).join("table"),
            "SELECT count(*) AS n FROM t",
        )?;
        let actual = decode_query_result(&run_with_timeout(&mut fixture.command())?)?;
        assert_eq!(actual.num_rows(), 1);
        assert_eq!(
            actual
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            expected.num_rows() as i64
        );
        let fixture =
            SqlCommandFixture::new(fixture_path(name).join("table"), "SELECT 1 AS n FROM t")?;
        let actual = decode_query_result(&run_with_timeout(&mut fixture.command())?)?;
        assert_eq!(actual.num_rows(), expected.num_rows());
        assert!(
            actual
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .all(|value| value == Some(1))
        );
    }
    for (sql, rows) in [
        ("SELECT id FROM t WHERE id < 0", 0),
        ("SELECT count(*) AS n FROM t WHERE id < 0", 1),
    ] {
        let fixture = SqlCommandFixture::new(fixture_path("partitioned").join("table"), sql)?;
        let actual = decode_query_result(&run_with_timeout(&mut fixture.command())?)?;
        assert_eq!(actual.num_rows(), rows);
        assert_eq!(actual.num_columns(), 1);
        if rows == 1 {
            assert_eq!(
                actual
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                0
            );
        }
    }
    Ok(())
}

#[test]
fn ctes_filters_groups_and_aggregates_match_fixture_values() -> TestResult {
    let expected = read_expected_rows("partitioned")?;
    let ids = expected
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let values = expected
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let regions = expected
        .column_by_name("region")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut totals = std::collections::BTreeMap::<String, (i64, i64)>::new();
    for row in 0..expected.num_rows() {
        if ids.value(row) >= 100 && !regions.is_null(row) && !values.is_null(row) {
            let (count, total) = totals.entry(regions.value(row).into()).or_default();
            *count += 1;
            *total += i64::from(values.value(row));
        }
    }
    let fixture = SqlCommandFixture::new(
        fixture_path("partitioned").join("table"),
        "WITH selected AS (SELECT region, value FROM t WHERE id >= 100 AND region IS NOT NULL AND value IS NOT NULL) SELECT region, count(*) AS n, sum(value) AS total FROM selected GROUP BY region ORDER BY region",
    )?;
    let actual = decode_query_result(&run_with_timeout(&mut fixture.command())?)?;
    let regions = cast(actual.column(0), &DataType::Utf8)?;
    let regions = regions.as_any().downcast_ref::<StringArray>().unwrap();
    let counts = actual
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sums = actual
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(actual.num_rows(), totals.len());
    for (row, (region, (count, total))) in totals.into_iter().enumerate() {
        assert_eq!(regions.value(row), region);
        assert_eq!(counts.value(row), count);
        assert_eq!(sums.value(row), total);
    }
    Ok(())
}

#[test]
fn sql_input_is_bounded_read_only_and_validated_before_table_io() -> TestResult {
    for sql in [
        b"SELECT 1; SELECT 2".to_vec(),
        b"SELECT FROM".to_vec(),
        vec![b' '; 65537],
        vec![0xff],
        b"COPY t TO 'secret-output'".to_vec(),
        b"CREATE TABLE secret (id INT)".to_vec(),
        b"INSERT INTO t VALUES (1)".to_vec(),
        b"SET target_partitions = 1".to_vec(),
        b"EXPLAIN ANALYZE SELECT * FROM t".to_vec(),
        b"SELECT * FROM read_parquet('https://secret/file.parquet')".to_vec(),
        format!("SELECT {} FROM t", vec!["1"; 10000].join("+")).into_bytes(),
        vec!["SELECT 1"; 2000].join(" UNION ALL ").into_bytes(),
    ] {
        let fixture = SqlCommandFixture::new("secret-missing-table", sql)?;
        let output = run_with_timeout(&mut fixture.command())?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["code"], "invalid_sql_input");
        assert_eq!(record["recovery"], "fix_input");
    }
    let fixture = SqlCommandFixture::new(
        fixture_path("partitioned").join("table"),
        "SELECT secret_missing_column FROM t",
    )?;
    let output = run_with_timeout(&mut fixture.command())?;
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert_eq!(
        assert_single_terminal_record(&output.stderr, "diagnostic")["code"],
        "sql_planning"
    );
    Ok(())
}

#[test]
fn query_ceilings_fail_instead_of_returning_a_successful_sample() -> TestResult {
    let fixture = SqlCommandFixture::new(
        fixture_path("partitioned").join("table"),
        "SELECT id FROM t ORDER BY id LIMIT 3",
    )?;
    let output = run_with_timeout(fixture.command().args(["--max-rows", "3"]))?;
    assert_eq!(decode_query_result(&output)?.num_rows(), 3);
    let exact_bytes = output.stdout.len().to_string();
    decode_query_result(&run_with_timeout(
        fixture.command().args(["--max-output-bytes", &exact_bytes]),
    )?)?;
    let short_by_one = (output.stdout.len() - 1).to_string();
    for (flag, value, reason) in [
        ("--max-rows", "2", "output_row_limit_exceeded"),
        ("--max-output-bytes", "8", "output_byte_limit_exceeded"),
        (
            "--max-output-bytes",
            short_by_one.as_str(),
            "output_byte_limit_exceeded",
        ),
    ] {
        let output = run_with_timeout(fixture.command().args([flag, value]))?;
        assert_eq!(output.status.code(), Some(1));
        assert!(!output.stdout.ends_with(IPC_END));
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["code"], "output_limit");
        assert_eq!(record["reason"], reason);
        assert_eq!(record["recovery"], "adjust_budget");
    }
    let fixture = SqlCommandFixture::new(
        fixture_path("partitioned").join("table"),
        "SELECT id, count(*) FROM t GROUP BY id",
    )?;
    let output = run_with_timeout(fixture.command().args(["--memory-limit-bytes", "1"]))?;
    assert_eq!(output.status.code(), Some(1));
    assert!(!output.stdout.ends_with(IPC_END));
    let record = assert_single_terminal_record(&output.stderr, "diagnostic");
    assert_eq!(record["source_kind"], "resource_exhausted");
    assert_eq!(record["recovery"], "adjust_budget");
    Ok(())
}

#[test]
fn explicit_snapshot_selection_keeps_deleted_rows_in_the_historical_query() -> TestResult {
    let fixture = SqlCommandFixture::new(
        fixture_path("deletion_vectors").join("table"),
        "SELECT id FROM t ORDER BY id",
    )?;
    let output = run_with_timeout(fixture.command().args(["--table-version", "0"]))?;
    let actual = decode_query_result(&output)?;
    let ids = actual
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.values().as_ref(), &(1..=12).collect::<Vec<_>>());
    let record = assert_single_terminal_record(&output.stderr, "completion");
    assert_eq!(record["bindings"][0]["table_version"], "0");
    Ok(())
}

#[test]
fn sql_cannot_discover_unregistered_files() -> TestResult {
    let fixture = SqlCommandFixture::new(fixture_path("partitioned").join("table"), "SELECT 1")?;
    let external = fixture.directory.path().join("secret.csv");
    fs::write(&external, "id\n99\n")?;
    fs::write(
        fixture.directory.path().join("query.sql"),
        format!("SELECT * FROM \"{}\"", external.display()),
    )?;
    let output = run_with_timeout(&mut fixture.command())?;
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert_eq!(
        assert_single_terminal_record(&output.stderr, "diagnostic")["code"],
        "sql_planning"
    );

    Ok(())
}

#[test]
fn query_arguments_only_offer_direct_storage() -> TestResult {
    let output =
        run_with_timeout(Command::new(env!("CARGO_BIN_EXE_dar")).args(["query", "--help"]))?;
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout)?;
    assert!(!help.contains("--databricks-profile"));
    assert!(!help.contains("uc://"));

    for table in ["uc://main.sales.orders", "UC://main.sales.orders"] {
        // Reject the unsupported target before attempting SQL file I/O or authorization.
        let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
        command
            .args(["query", table, "--sql-file", "secret-missing.sql"])
            .env("PATH", "");
        let output = run_with_timeout(&mut command)?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let diagnostic = String::from_utf8(output.stderr)?;
        assert!(diagnostic.contains("Unity Catalog SQL queries are not supported yet."));
        assert!(!diagnostic.contains("Databricks profile"));

        let output = run_with_timeout(command.arg("--diagnostics=json"))?;
        assert_eq!(output.status.code(), Some(2));
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["code"], "unsupported_uc_query");
        assert_eq!(record["phase"], "configuration");
        assert_eq!(record["recovery"], "fix_input");
    }
    for table in ["secret-table", "uc://main.sales.orders"] {
        let output = run_with_timeout(Command::new(env!("CARGO_BIN_EXE_dar")).args([
            "query",
            table,
            "--sql-file",
            "secret-missing.sql",
            "--databricks-profile",
            "secret-profile",
            "--diagnostics=json",
        ]))?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["code"], "invalid_cli_argument");
        assert_eq!(record["argument_kind"], "UnknownArgument");
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn cancelling_or_closing_a_blocked_query_pipe_exits_without_completion() -> TestResult {
    use std::{
        os::fd::AsRawFd,
        process::Stdio,
        thread,
        time::{Duration, Instant},
    };

    for signal in [Some(libc::SIGINT), Some(libc::SIGTERM), None] {
        let fixture = SqlCommandFixture::new(
            fixture_path("partitioned").join("table"),
            "SELECT repeat('x', 1048576) AS text FROM t LIMIT 1",
        )?;
        let mut child = fixture
            .command()
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let fd = child.stdout.as_ref().unwrap().as_raw_fd();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let mut buffered = 0;
            // SAFETY: buffered is writable, and fd still refers to the child's stdout pipe.
            assert_eq!(unsafe { libc::ioctl(fd, libc::FIONREAD, &mut buffered) }, 0);
            // Wait past the small schema: this 1 MiB batch cannot fit in an unread pipe.
            if buffered >= 1024 {
                break;
            }
            if child.try_wait()?.is_some() || Instant::now() >= deadline {
                let _ = child.kill();
                let output = child.wait_with_output()?;
                panic!(
                    "query did not fill its output pipe: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            thread::sleep(Duration::from_millis(10));
        }
        if let Some(signal) = signal {
            // SAFETY: the PID belongs to the live query process created above.
            assert_eq!(unsafe { libc::kill(child.id() as i32, signal) }, 0);
        } else {
            drop(child.stdout.take());
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while child.try_wait()?.is_none() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        if child.try_wait()?.is_none() {
            child.kill()?;
            child.wait()?;
            panic!("query did not exit after cancellation or pipe closure");
        }
        let output = child.wait_with_output()?;
        assert_eq!(
            output.status.code(),
            Some(signal.map_or(3, |signal| 128 + signal))
        );
        assert!(!output.stdout.ends_with(IPC_END));
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(
            record["code"],
            if signal.is_some() {
                "command_cancelled"
            } else {
                "output_write"
            }
        );
    }
    Ok(())
}
