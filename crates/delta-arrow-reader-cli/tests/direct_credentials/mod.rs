//! Explicit cloud credential sources exercised through real CLI reads.

mod azure_blob;
mod gcs;
mod mock_credential_proxy;
mod s3;

use std::{
    fs,
    io::Cursor,
    path::Path,
    process::{Command, Output},
};

use arrow::{compute::concat_batches, ipc::reader::StreamReader};
use serde_json::{Value, json};

use super::{
    diagnostics::assert_single_terminal_record,
    mock_cloud_storage::{
        CloudStorageProtocol, MockCloudStorageServer, expected_partitioned_rows,
        partitioned_objects,
    },
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
            CloudStorageProtocol::AzureBlob => "azure_storage_endpoint",
            CloudStorageProtocol::Gcs => "google_base_url",
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
            CloudStorageProtocol::AzureBlob => "az://container/table",
            CloudStorageProtocol::Gcs => "gs://bucket/table",
        };
        let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
        command
            .env_clear()
            .env("RUST_LOG", "trace")
            .args([operation, table, "--storage-options-file"])
            .arg(self.directory.path().join("options.json"));
        command
    }

    fn verify_request_signatures(&self, signing_keys: Value) -> TestResult {
        let protocol = match self.protocol {
            CloudStorageProtocol::S3 => "s3",
            CloudStorageProtocol::AzureBlob => "azure_blob",
            CloudStorageProtocol::Gcs => panic!("GCS uses JWT verification"),
        };
        let captured_requests = self.directory.path().join("signed-requests.json");
        fs::write(
            &captured_requests,
            serde_json::to_vec(&json!({
                "protocol": protocol,
                "requests": *self.server.requests.lock().unwrap(),
                "signing_keys": signing_keys,
            }))?,
        )?;
        let output = run_with_timeout(
            Command::new("python3")
                // Keep PYTHONOPTIMIZE from disabling the verifier's assertions.
                .arg("-I")
                .arg(
                    Path::new(env!("CARGO_MANIFEST_DIR"))
                        .join("tests/direct_credentials/verify_request_signatures.py"),
                )
                .arg(captured_requests),
        )?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
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

#[test]
fn help_lists_only_installed_sources_without_loading_inputs() -> TestResult {
    for operation in ["inspect", "scan", "query"] {
        let output = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .env_clear()
                .env("AWS_ACCESS_KEY_ID", "secret-unused-key")
                .args([
                    operation,
                    "s3://secret-unavailable-bucket/table",
                    "--auth-source",
                    "env",
                    "--storage-options-file",
                    "secret-missing-options.json",
                    "--help",
                ]),
        )?;
        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        let help = String::from_utf8(output.stdout)?;
        for source in [
            "json",
            "env",
            "aws-web-identity",
            "aws-container",
            "azure-workload-identity",
            "azure-managed-identity",
        ] {
            assert!(help.contains(&format!("- {source}:")), "{help}");
        }
        for unavailable in ["aws-imds", "azure-cli", "gcp-metadata", "gcp-adc", "secret"] {
            assert!(!help.contains(unavailable), "{help}");
        }
    }
    Ok(())
}

#[test]
fn unexposed_provider_names_fail_before_credentials_or_storage() -> TestResult {
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
    fixture.write_options(json!({}))?;
    for source in [
        "aws-imds",
        "azure-cli",
        "gcp-metadata",
        "gcp-adc",
        "secret-source",
    ] {
        let output = run_with_timeout(
            fixture
                .command("inspect")
                .args(["--auth-source", source, "--diagnostics=json"])
                .env("AWS_ACCESS_KEY_ID", "secret-unused-key")
                .env("AWS_SECRET_ACCESS_KEY", "secret-unused-password"),
        )?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["code"], "invalid_cli_argument");
    }
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}
