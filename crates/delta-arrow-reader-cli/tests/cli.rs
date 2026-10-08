use std::{
    fs::{self, File},
    io::{self, Read},
    path::Path,
    process::{Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

use arrow::{
    datatypes::{DataType, Fields, Schema},
    ipc::reader::FileReader,
};
use serde_json::Value;

#[test]
fn external_writer_schemas_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let corpus = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/reader/fixtures/external_writer/corpus");
    for (name, version) in [
        ("partitioned", "0"),
        ("nested_mapping", "1"),
        ("deletion_vectors", "1"),
    ] {
        let fixture = corpus.join(name);
        let output = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .arg("inspect")
                .arg(fixture.join("table")),
        )?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stderr.is_empty());
        let value: Value = serde_json::from_slice(&output.stdout)?;
        assert_eq!(value["table_version"], version);
        let schema: Schema = serde_json::from_value(value["schema"].clone())?;
        let expected =
            FileReader::try_new(File::open(fixture.join("expected.arrow"))?, None)?.schema();
        // Spark's IPC export omits Delta column-mapping metadata. Check the
        // complete logical types against IPC and metadata against Spark's log.
        let mut delta_schema = Value::Null;
        for version in 0..=version.parse::<u64>()? {
            let log =
                fs::read_to_string(fixture.join(format!("table/_delta_log/{version:020}.json")))?;
            for line in log.lines() {
                let action: Value = serde_json::from_str(line)?;
                if let Some(schema) = action["metaData"]["schemaString"].as_str() {
                    delta_schema = serde_json::from_str(schema)?;
                }
            }
        }
        assert_fixture_schema_fields(
            schema.fields(),
            expected.fields(),
            delta_schema["fields"].as_array().unwrap(),
        );
        assert_eq!(schema.metadata(), expected.metadata());
    }
    Ok(())
}

fn run_with_timeout(command: &mut Command) -> io::Result<Output> {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    // Drain both pipes while waiting so a full pipe cannot block the child.
    thread::scope(|scope| {
        let stdout = scope.spawn(move || {
            let mut bytes = Vec::new();
            stdout.read_to_end(&mut bytes).map(|_| bytes)
        });
        let stderr = scope.spawn(move || {
            let mut bytes = Vec::new();
            stderr.read_to_end(&mut bytes).map(|_| bytes)
        });
        let deadline = Instant::now() + Duration::from_secs(15);
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break Ok(status),
                Err(error) => break Err(error),
                Ok(None) if Instant::now() >= deadline => {
                    break Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "CLI fixture timed out",
                    ));
                }
                Ok(None) => thread::sleep(Duration::from_millis(10)),
            }
        };
        if status.is_err() {
            let _ = child.kill();
            let _ = child.wait();
        }
        Ok(Output {
            status: status?,
            stdout: stdout.join().unwrap()?,
            stderr: stderr.join().unwrap()?,
        })
    })
}

fn assert_fixture_schema_fields(actual: &Fields, oracle: &Fields, delta: &[Value]) {
    assert_eq!(actual.len(), oracle.len());
    assert_eq!(actual.len(), delta.len());
    for ((actual, expected), delta) in actual.iter().zip(oracle).zip(delta) {
        assert_eq!(actual.name(), expected.name());
        assert_eq!(actual.is_nullable(), expected.is_nullable());
        let metadata = delta["metadata"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    value
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| value.to_string()),
                )
            })
            .collect();
        assert_eq!(actual.metadata(), &metadata);
        match (actual.data_type(), expected.data_type()) {
            (DataType::Struct(actual), DataType::Struct(expected)) => assert_fixture_schema_fields(
                actual,
                expected,
                delta["type"]["fields"].as_array().unwrap(),
            ),
            (actual, expected) => assert_eq!(actual, expected),
        }
    }
}

#[test]
#[cfg(target_os = "linux")]
fn process_contract() -> Result<(), Box<dyn std::error::Error>> {
    // Python's stdlib supplies bounded subprocess waits, HTTP, pipes, and signals.
    let status = Command::new("python3")
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/process.py"))
        .arg(env!("CARGO_BIN_EXE_dar"))
        .arg(env!("CARGO_PKG_VERSION"))
        .status()?;
    assert!(status.success(), "CLI process contract failed");
    Ok(())
}
