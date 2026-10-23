use std::{collections::HashMap, sync::Arc};

use arrow_array::Int32Array;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};

use super::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

struct SmokeFixture {
    output: tempfile::TempDir,
    expected_path: PathBuf,
    batch: RecordBatch,
}

impl SmokeFixture {
    fn new() -> TestResult<Self> {
        let expected_path = crate::repository::repository_root()
            .join("crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned/expected.arrow");
        let mut reader = FileReader::try_new(File::open(&expected_path)?, None)?;
        let id = reader.schema().index_of("id")?;
        let batch = reader.next().ok_or("empty fixture")??.project(&[id])?;
        assert!(batch.num_rows() >= 2);
        let fixture = Self {
            output: tempfile::tempdir()?,
            expected_path,
            batch,
        };
        fs::write(
            fixture.output.path().join("inspect.json"),
            br#"{"format_version":1,"table_version":"0"}"#,
        )?;
        fixture.write_batches(&[fixture.batch.slice(0, 2)])?;
        fixture.write_count_query(360)?;
        Ok(fixture)
    }

    fn write_count_query(&self, count: i64) -> TestResult {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![count]))],
        )?;
        let mut writer = StreamWriter::try_new(Vec::new(), &schema)?;
        writer.write(&batch)?;
        writer.finish()?;
        let stdout = writer.into_inner()?;
        let completion = serde_json::json!({
            "format_version": 1, "kind": "completion", "terminal": true,
            "command": "query", "completion": "complete", "scope": "query",
            "stop_reason": "end_of_input", "has_more": false,
            "bindings": [{"alias": "t", "table_version": "0"}],
            "rows": "1", "batches": "1", "output_bytes": stdout.len().to_string(),
        });
        fs::write(self.output.path().join("query.arrow"), stdout)?;
        fs::write(
            self.output.path().join("query.stderr"),
            format!("{completion}\n"),
        )?;
        Ok(())
    }

    fn write_batches(&self, batches: &[RecordBatch]) -> TestResult {
        let schema = batches.first().map_or(self.batch.schema(), |b| b.schema());
        let mut writer = StreamWriter::try_new(
            File::create(self.output.path().join("scan.arrow"))?,
            &schema,
        )?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
        Ok(())
    }

    fn check(&self) -> Result<()> {
        check_smoke_output(self.output.path(), &self.expected_path)
    }
}

#[test]
fn smoke_scan_accepts_fixture_rows_across_batches_in_any_order() -> TestResult {
    let fixture = SmokeFixture::new()?;
    fixture.check()?;
    fixture.write_batches(&[
        fixture.batch.slice(1, 1),
        fixture.batch.slice(0, 0),
        fixture.batch.slice(0, 1),
    ])?;
    fixture.check()?;
    let metadata = HashMap::from([("test".into(), "ignored metadata".into())]);
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            fixture
                .batch
                .schema()
                .field(0)
                .clone()
                .with_metadata(metadata.clone()),
        ],
        metadata,
    ));
    fixture.write_batches(&[RecordBatch::try_new(
        schema,
        vec![fixture.batch.column(0).slice(0, 2)],
    )?])?;
    fixture.check()?;
    Ok(())
}

#[test]
fn smoke_scan_rejects_missing_extra_duplicate_and_unknown_rows() -> TestResult {
    let fixture = SmokeFixture::new()?;
    for batches in [
        vec![],
        vec![fixture.batch.slice(0, 1)],
        vec![fixture.batch.slice(0, 2), fixture.batch.slice(0, 1)],
        vec![fixture.batch.slice(0, 1), fixture.batch.slice(0, 1)],
        vec![RecordBatch::try_new(
            fixture.batch.schema(),
            vec![Arc::new(Int32Array::from(vec![i32::MIN, i32::MAX]))],
        )?],
    ] {
        fixture.write_batches(&batches)?;
        assert!(fixture.check().is_err(), "accepted {batches:?}");
    }
    Ok(())
}

#[test]
fn smoke_scan_requires_the_projected_fixture_schema() -> TestResult {
    let fixture = SmokeFixture::new()?;
    for (name, data_type, nullable) in [
        ("wrong", DataType::Int32, true),
        ("id", DataType::Int64, true),
        ("id", DataType::Int32, false),
    ] {
        let schema = Arc::new(Schema::new(vec![Field::new(name, data_type, nullable)]));
        fixture.write_batches(&[RecordBatch::new_empty(schema)])?;
        let error = fixture.check().expect_err("schema mismatch must fail");
        assert!(error.to_string().contains("schema differs"), "{error}");
    }
    let mut reader = FileReader::try_new(File::open(&fixture.expected_path)?, None)?;
    fixture.write_batches(&[reader.next().ok_or("empty fixture")??.slice(0, 2)])?;
    assert!(
        fixture
            .check()
            .unwrap_err()
            .to_string()
            .contains("schema differs")
    );
    Ok(())
}

#[test]
fn smoke_scan_rejects_truncated_corrupt_and_trailing_ipc_bytes() -> TestResult {
    let fixture = SmokeFixture::new()?;
    let path = fixture.output.path().join("scan.arrow");
    let valid = fs::read(&path)?;
    for payload in [
        valid[..valid.len() - 8].to_vec(),
        valid[..valid.len() - 1].to_vec(),
        vec![
            255, 255, 255, 255, 4, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 0, 0, 0, 0,
        ],
        [valid.as_slice(), b"junk", &valid[valid.len() - 8..]].concat(),
        [valid.as_slice(), valid.as_slice()].concat(),
    ] {
        fs::write(&path, payload)?;
        assert!(fixture.check().is_err());
    }
    Ok(())
}

#[test]
fn smoke_inspection_requires_the_expected_snapshot() -> TestResult {
    let fixture = SmokeFixture::new()?;
    for inspection in [
        r#"{"format_version":2,"table_version":"0"}"#,
        r#"{"format_version":1,"table_version":"1"}"#,
        r#"{"format_version":1,"table_version":0}"#,
        "{}",
        "null",
        "invalid JSON",
    ] {
        fs::write(fixture.output.path().join("inspect.json"), inspection)?;
        assert!(fixture.check().is_err(), "accepted {inspection}");
    }
    Ok(())
}

#[test]
fn smoke_query_requires_the_exact_aggregate_complete_ipc_and_pinned_binding() -> TestResult {
    let fixture = SmokeFixture::new()?;
    fixture.write_count_query(359)?;
    assert!(fixture.check().is_err());
    fixture.write_count_query(360)?;
    fixture.check()?;
    let stdout = fixture.output.path().join("query.arrow");
    let valid = fs::read(&stdout)?;
    fs::write(&stdout, &valid[..valid.len() - 8])?;
    assert!(fixture.check().is_err());
    fs::write(&stdout, valid)?;
    let stderr = fixture.output.path().join("query.stderr");
    let completion: Value = serde_json::from_slice(&fs::read(&stderr)?)?;
    let mut changed = completion.clone();
    changed["bindings"][0]["table_version"] = serde_json::json!("1");
    for record in [
        format!("{changed}\n"),
        String::new(),
        format!("{completion}\n{completion}\n"),
    ] {
        fs::write(&stderr, record)?;
        assert!(fixture.check().is_err());
    }
    Ok(())
}

#[test]
#[cfg(unix)]
fn extracted_files_require_exact_members_permissions_and_external_location() -> TestResult {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let temporary = tempfile::tempdir()?;
    let root = fs::canonicalize(temporary.path())?;
    let repository = root.join("repository");
    let extracted = root.join("extracted");
    fs::create_dir(&repository)?;
    fs::create_dir(&extracted)?;
    for (name, mode) in MEMBERS {
        let path = extracted.join(name);
        fs::write(&path, name)?;
        fs::set_permissions(&path, fs::Permissions::from_mode(mode))?;
    }
    assert_eq!(
        check_extracted_files(&extracted, &repository)?,
        extracted.join("dar")
    );
    for (name, mode) in MEMBERS {
        let path = extracted.join(name);
        for wrong in [0o600, mode | 0o4000] {
            fs::set_permissions(&path, fs::Permissions::from_mode(wrong))?;
            assert!(check_extracted_files(&extracted, &repository).is_err());
        }
        fs::set_permissions(&path, fs::Permissions::from_mode(mode))?;
        let saved = root.join(name);
        fs::rename(&path, &saved)?;
        assert!(check_extracted_files(&extracted, &repository).is_err());
        symlink(&saved, &path)?;
        assert!(check_extracted_files(&extracted, &repository).is_err());
        fs::remove_file(&path)?;
        fs::create_dir(&path)?;
        assert!(check_extracted_files(&extracted, &repository).is_err());
        fs::remove_dir(&path)?;
        fs::rename(saved, path)?;
    }
    fs::write(extracted.join("extra"), "unexpected")?;
    assert!(check_extracted_files(&extracted, &repository).is_err());
    fs::remove_file(extracted.join("extra"))?;
    let linked = root.join("linked");
    symlink(&extracted, &linked)?;
    assert!(check_extracted_files(&linked, &repository).is_err());
    assert!(check_extracted_files(&extracted, &extracted).is_err());
    let inside = repository.join("extracted");
    fs::rename(&extracted, &inside)?;
    assert!(check_extracted_files(&inside, &repository).is_err());
    let linked_repository = root.join("linked-repository");
    symlink(&repository, &linked_repository)?;
    assert!(check_extracted_files(&linked_repository.join("extracted"), &repository).is_err());
    Ok(())
}
