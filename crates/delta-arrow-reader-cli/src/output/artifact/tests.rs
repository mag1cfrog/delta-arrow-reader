#![allow(clippy::unwrap_used)]
use std::sync::Arc;

use arrow::array::Int32Array;
use futures_util::stream;

use super::*;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn stage_fixture(directory: &Path, manifest: bool) -> Result<PendingArtifactPublication, Error> {
    let mut staged = StagedResult::new(
        &directory.join("rows"),
        RowOutputFormat::Arrow,
        manifest.then(|| json!({})),
        AbortHandle::new_pair().0,
    )?;
    staged
        .write_all(b"result")
        .map_err(|source| Error::ArtifactIo {
            source,
            reason: "artifact_write_failed",
        })?;
    staged.prepare_publication(&crate::diagnostics::build_completion_record(
        "scan",
        Some(42),
        OutputStats {
            rows_written: 2,
            batches_written: 1,
            bytes_written: 6,
        },
        None,
    ))
}

#[test]
fn publication_never_replaces_existing_or_racing_destinations() -> TestResult {
    for (manifest, name) in [
        (false, "rows"),
        (true, "rows"),
        (true, "rows.manifest.json"),
    ] {
        let directory = tempfile::tempdir()?;
        let existing = directory.path().join(name);
        fs::write(&existing, b"existing data")?;
        assert!(
            StagedResult::new(
                &directory.path().join("rows"),
                RowOutputFormat::Arrow,
                manifest.then(|| json!({})),
                AbortHandle::new_pair().0,
            )
            .is_err()
        );
        assert_eq!(fs::read(&existing)?, b"existing data");
        fs::remove_file(&existing)?;
        let complete = stage_fixture(directory.path(), manifest)?;
        fs::write(&existing, b"racing writer")?;
        let (cancel, _) = AbortHandle::new_pair();
        let error = complete
            .publish(&cancel)
            .err()
            .ok_or("expected publication failure")?;
        assert!(
            matches!(error, Error::ArtifactIo { source, .. } if source.kind() == io::ErrorKind::AlreadyExists)
        );
        assert_eq!(fs::read(&existing)?, b"racing writer");
        if name == "rows" {
            assert!(!directory.path().join("rows.manifest.json").exists());
        } else {
            assert_eq!(fs::read(directory.path().join("rows"))?, b"result");
        }
        assert!(fs::read_dir(directory.path())?.all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".dar-")
        }));
    }
    Ok(())
}

#[test]
fn cancelled_publication_leaves_no_final_or_temporary_files() -> TestResult {
    for manifest in [false, true] {
        let directory = tempfile::tempdir()?;
        let complete = stage_fixture(directory.path(), manifest)?;
        let (cancel, _) = AbortHandle::new_pair();
        cancel.abort();
        assert!(matches!(
            complete.publish(&cancel),
            Err(Error::Cancelled { .. })
        ));
        assert_eq!(fs::read_dir(directory.path())?.count(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn late_stream_errors_discard_both_output_formats() -> TestResult {
    for (manifest, format) in [
        (false, RowOutputFormat::Arrow),
        (false, RowOutputFormat::Json),
        (true, RowOutputFormat::Arrow),
        (true, RowOutputFormat::Json),
    ] {
        let directory = tempfile::tempdir()?;
        let staged = StagedResult::new(
            &directory.path().join("rows"),
            format,
            manifest.then(|| json!({})),
            AbortHandle::new_pair().0,
        )?;
        let batch = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int32Array::from(vec![1, 2])) as arrow::array::ArrayRef,
        )])?;
        let result = staged
            .write_rows(
                batch.schema(),
                stream::iter([Ok(batch), Err(Error::Cancelled { status: 130 })]),
                vec![json!({"alias": "t", "table_version": "0"})],
                None,
            )
            .await;
        assert!(matches!(result, Err(Error::Cancelled { status: 130 })));
        assert_eq!(fs::read_dir(directory.path())?.count(), 0);
    }
    Ok(())
}

#[test]
fn artifact_json_limits_include_escaping_utf8_and_newline() -> TestResult {
    for (limit, reason) in [
        (MAX_RECEIPT_BYTES, "artifact_receipt_too_large"),
        (MAX_MANIFEST_BYTES, "artifact_manifest_too_large"),
    ] {
        let value = json!("x".repeat(limit - 3));
        assert_eq!(encode_bounded_json(&value, limit, reason)?.len(), limit);
        assert!(matches!(
            encode_bounded_json(&value, limit - 1, reason),
            Err(Error::OutputLimit { .. })
        ));
    }
    assert_eq!(encode_bounded_json(&json!("\u{e9}"), 5, "limit")?.len(), 5);
    assert!(encode_bounded_json(&json!("\u{e9}"), 4, "limit").is_err());
    Ok(())
}
