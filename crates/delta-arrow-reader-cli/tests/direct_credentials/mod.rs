//! Explicit cloud credential sources exercised through real CLI reads.

mod azure;
mod s3;

use std::{
    fs,
    io::Cursor,
    process::{Command, Output},
};

use arrow::{compute::concat_batches, ipc::reader::StreamReader};
use serde_json::{Value, json};

use super::{
    cloud_storage::{
        CloudStorageProtocol, MockCloudStorageServer, expected_partitioned_rows,
        partitioned_objects,
    },
    diagnostics::assert_single_terminal_record,
    run_with_timeout, sort_by_id,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Owns the selected options file and the storage endpoint that records the resulting requests.
struct DirectStorageFixture {
    directory: tempfile::TempDir,
    server: MockCloudStorageServer,
    protocol: CloudStorageProtocol,
}

impl DirectStorageFixture {
    fn new(protocol: CloudStorageProtocol) -> Result<Self, Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        fs::write(
            directory.path().join("query.sql"),
            "SELECT id FROM t ORDER BY id",
        )?;
        Ok(Self {
            directory,
            protocol,
            server: MockCloudStorageServer::with_objects(
                protocol,
                partitioned_objects()?,
                |_, _| {},
            )?,
        })
    }

    fn write_options(&self, mut options: Value) -> TestResult {
        let endpoint_key = match self.protocol {
            CloudStorageProtocol::S3 => "aws_endpoint",
            CloudStorageProtocol::Azure => "azure_storage_endpoint",
        };
        options[endpoint_key] = json!(self.server.endpoint);
        options["allow_http"] = json!("true");
        fs::write(
            self.directory.path().join("options.json"),
            serde_json::to_vec(&options)?,
        )?;
        Ok(())
    }

    fn command(&self, operation: &str) -> Command {
        let table = match self.protocol {
            CloudStorageProtocol::S3 => "s3://bucket/table",
            CloudStorageProtocol::Azure => "az://container/table",
        };
        let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
        command
            .env_clear()
            .env("RUST_LOG", "trace")
            .args([operation, table, "--storage-options-file"])
            .arg(self.directory.path().join("options.json"));
        command
    }
}

fn assert_read_result(output: Output, operation: &str) -> TestResult {
    let expected = expected_partitioned_rows()?;
    let expected_ids = sort_by_id(&expected)?.column_by_name("id").unwrap().clone();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains("secret"));
    assert_single_terminal_record(&output.stderr, "completion");
    if operation == "inspect" {
        let result: Value = serde_json::from_slice(&output.stdout)?;
        assert_eq!(result["table_version"], "0");
        assert_eq!(
            result["schema"]["fields"].as_array().unwrap().len(),
            expected.num_columns()
        );
    } else {
        assert!(output.stdout.ends_with(&[255, 255, 255, 255, 0, 0, 0, 0]));
        let reader = StreamReader::try_new(Cursor::new(output.stdout), None)?;
        let schema = reader.schema();
        let actual = concat_batches(&schema, &reader.collect::<Result<Vec<_>, _>>()?)?;
        assert_eq!(sort_by_id(&actual)?.column(0), &expected_ids);
    }
    Ok(())
}
