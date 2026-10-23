//! Preview uses UC grants and terminates pending grant requests at its deadline.

use super::*;
use crate::{diagnostics::assert_single_terminal_record, mock_cloud_storage::partitioned_objects};

#[test]
fn preview_reads_with_each_unity_catalog_credential_kind() -> TestResult {
    for kind in [
        StorageCredentialKind::S3,
        StorageCredentialKind::R2,
        StorageCredentialKind::AzureBlobSas,
        StorageCredentialKind::AzureBlobBearer,
        StorageCredentialKind::Gcs,
    ] {
        let fixture = DatabricksCliFixture::new(kind)?;
        let server = MockCloudStorageServer::with_objects(
            kind.storage_protocol(),
            partitioned_objects()?,
            |_, _| {},
        )?;
        fixture.use_storage_endpoint(kind, &server.endpoint)?;
        let output = run_with_timeout(fixture.table_command("preview").args([
            "--column",
            "id",
            "--limit",
            "3",
            "--table-version",
            "0",
            "--diagnostics=json",
        ]))?;
        assert!(
            output.status.success(),
            "{kind:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let sample: Value = serde_json::from_slice(&output.stdout)?;
        assert_eq!(sample["completion"], "sample");
        assert_eq!(sample["row_count"], "3");
        assert_eq!(sample["stop_reason"], "row_limit");
        assert_eq!(sample["bindings"][0]["table_version"], "0");
        assert_eq!(sample["schema"]["fields"].as_array().unwrap().len(), 1);
        let receipt = assert_single_terminal_record(&output.stderr, "completion");
        assert_eq!(receipt["command"], "preview");
        assert_eq!(receipt["output_bytes"], output.stdout.len().to_string());
        for sensitive in ["secret", PROFILE, TABLE_NAME, TABLE_ID] {
            assert!(!String::from_utf8_lossy(&output.stdout).contains(sensitive));
            assert!(!String::from_utf8_lossy(&output.stderr).contains(sensitive));
        }
    }
    Ok(())
}
