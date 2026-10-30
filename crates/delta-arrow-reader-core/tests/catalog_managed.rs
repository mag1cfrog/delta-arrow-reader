//! Catalog confirmation controls visible commits without replacing either Parquet reader.
#![allow(clippy::unwrap_used)]

use std::{error::Error, fs};

use arrow::array::Int32Array;
use delta_arrow_reader::{
    DeltaCatalogCommit, DeltaCatalogSnapshot, DeltaComparison, DeltaPredicate, DeltaScalar,
    DeltaScanExecutionOptions, DeltaSnapshotSelection, DeltaTable, DeltaTableBuilder,
    ParquetReaderBackend, WarmupMode,
};
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "reader/support.rs"]
mod support;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

struct CatalogTableFixture {
    table: support::RealParquetDeltaTable,
    commit: DeltaCatalogCommit,
}

impl CatalogTableFixture {
    fn new() -> TestResult<Self> {
        let table = support::RealParquetDeltaTable::new_with_rows("catalog-managed", 3)?;
        let log = table.path().join("_delta_log");
        let original = format!(
            "{}{}",
            fs::read_to_string(log.join("00000000000000000000.json"))?,
            fs::read_to_string(log.join("00000000000000000001.json"))?,
        );
        let mut creation = String::new();
        let mut insertion = String::new();
        for line in original.lines() {
            let mut action: Value = serde_json::from_str(line)?;
            if action.get("add").is_some() {
                insertion.push_str(&format!("{action}\n"));
                continue;
            }
            if action.get("protocol").is_some() {
                action = json!({"protocol": {
                    "minReaderVersion": 3, "minWriterVersion": 7,
                    "readerFeatures": ["catalogManaged", "vacuumProtocolCheck"],
                    "writerFeatures": ["catalogManaged", "vacuumProtocolCheck", "inCommitTimestamp"]
                }});
            }
            if let Some(metadata) = action.get_mut("metaData") {
                metadata["configuration"]["delta.enableInCommitTimestamps"] = json!("true");
            }
            creation.push_str(&format!("{action}\n"));
        }
        fs::write(log.join("00000000000000000000.json"), creation)?;
        let staged = log.join("_staged_commits");
        fs::create_dir(&staged)?;
        let file_name = "00000000000000000001.aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.json";
        fs::write(staged.join(file_name), &insertion)?;
        let removal = format!(
            "{}\n",
            json!({"remove": {
                "path": table.data_file_path(), "deletionTimestamp": 2, "dataChange": true
            }})
        );
        // Conflicting published and unratified staged commits must not replace the catalog tail.
        fs::write(log.join("00000000000000000001.json"), &removal)?;
        fs::write(log.join("00000000000000000002.json"), &removal)?;
        fs::write(
            staged.join("00000000000000000001.bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb.json"),
            removal,
        )?;
        Ok(Self {
            table,
            commit: DeltaCatalogCommit {
                version: 1,
                file_name: file_name.into(),
                file_size: insertion.len() as u64,
                last_modified_ms: 1,
            },
        })
    }

    fn catalog_snapshot(&self) -> DeltaCatalogSnapshot {
        DeltaCatalogSnapshot {
            max_version: 1,
            commits: vec![self.commit.clone()],
        }
    }

    fn builder(&self) -> DeltaTableBuilder {
        DeltaTableBuilder::new(self.table.path().to_string_lossy()).with_warmup(WarmupMode::None)
    }
}

async fn read_ids(table: &DeltaTable, filtered: bool) -> TestResult<Vec<i32>> {
    let mut builder = table.scan().with_projection(["id"]);
    if filtered {
        builder = builder.with_predicate(DeltaPredicate::Compare {
            column: "id".into(),
            op: DeltaComparison::Gt,
            value: DeltaScalar::Int32(1),
        });
    }
    let mut stream = builder.build().await?.into_stream();
    let mut ids = Vec::new();
    while let Some(batch) = stream.next_batch().await? {
        ids.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
        );
    }
    ids.sort();
    Ok(ids)
}

#[tokio::test]
async fn catalog_tail_and_ceiling_control_latest_and_pinned_reads() -> TestResult {
    let fixture = CatalogTableFixture::new()?;
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        for (selection, version, expected) in [
            (DeltaSnapshotSelection::Latest, 1, vec![1, 2, 3]),
            (DeltaSnapshotSelection::Version(0), 0, vec![]),
            (DeltaSnapshotSelection::Version(1), 1, vec![1, 2, 3]),
        ] {
            let table = fixture
                .builder()
                .with_catalog_snapshot(fixture.catalog_snapshot())
                .with_snapshot_selection(selection)
                .with_execution_options(
                    DeltaScanExecutionOptions::new().with_parquet_backend(backend),
                )
                .load_table()
                .await?;
            assert_eq!(table.version(), version);
            assert_eq!(read_ids(&table, false).await?, expected);
            assert_eq!(
                read_ids(&table, true).await?,
                if version == 0 { vec![] } else { vec![2, 3] }
            );
        }
    }
    let snapshot = fixture
        .builder()
        .with_catalog_snapshot(fixture.catalog_snapshot())
        .load_snapshot()
        .await?;
    assert_eq!(snapshot.version(), 1);
    snapshot.validate_protocol()?;
    // Once published, the catalog may return no staged tail at all.
    let log = fixture.table.path().join("_delta_log");
    fs::copy(
        log.join("_staged_commits").join(&fixture.commit.file_name),
        log.join("00000000000000000001.json"),
    )?;
    let table = fixture
        .builder()
        .with_catalog_snapshot(DeltaCatalogSnapshot {
            max_version: 1,
            commits: vec![],
        })
        .load_table()
        .await?;
    assert_eq!(read_ids(&table, false).await?, vec![1, 2, 3]);
    Ok(())
}

#[tokio::test]
async fn pinned_reads_before_catalog_enablement_use_the_historical_protocol() -> TestResult {
    let mut fixture = CatalogTableFixture::new()?;
    let log = fixture.table.path().join("_delta_log");
    let creation_path = log.join("00000000000000000000.json");
    let staged_path = log.join("_staged_commits").join(&fixture.commit.file_name);
    let catalog_metadata = fs::read_to_string(&creation_path)?;
    let mut historical_creation = String::new();
    for line in catalog_metadata.lines() {
        let mut action: Value = serde_json::from_str(line)?;
        if action.get("protocol").is_some() {
            action = json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}});
        }
        if let Some(metadata) = action.get_mut("metaData") {
            metadata["configuration"] = json!({});
        }
        historical_creation.push_str(&format!("{action}\n"));
    }
    historical_creation.push_str(&fs::read_to_string(&staged_path)?);
    fs::write(creation_path, historical_creation)?;
    fs::write(staged_path, &catalog_metadata)?;
    fixture.commit.file_size = catalog_metadata.len() as u64;

    for version in [0, 1] {
        let table = fixture
            .builder()
            .with_catalog_snapshot(fixture.catalog_snapshot())
            .with_snapshot_selection(DeltaSnapshotSelection::Version(version))
            .load_table()
            .await?;
        assert_eq!(table.version(), version);
        assert_eq!(read_ids(&table, false).await?, vec![1, 2, 3]);
        assert_eq!(
            table.refresh().await.unwrap_err().reason(),
            "catalog_refresh_requires_new_catalog_snapshot"
        );
        assert_eq!(table.version(), version);
    }
    // A missing staged upgrade must not fall back to a conflicting ordinary published commit.
    fs::remove_file(log.join("_staged_commits").join(&fixture.commit.file_name))?;
    fs::copy(
        log.join("00000000000000000000.json"),
        log.join("00000000000000000001.json"),
    )?;
    let mut snapshot = fixture.catalog_snapshot();
    snapshot.max_version = 2;
    let mut next_commit = fixture.commit.clone();
    next_commit.version = 2;
    next_commit.file_name = "00000000000000000002.aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.json".into();
    snapshot.commits.push(next_commit);
    assert!(
        fixture
            .builder()
            .with_catalog_snapshot(snapshot)
            .with_snapshot_selection(DeltaSnapshotSelection::Version(1))
            .load_table()
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn catalog_confirmation_cannot_be_omitted_or_reused_for_refresh() -> TestResult {
    let fixture = CatalogTableFixture::new()?;
    assert!(fixture.builder().load_table().await.is_err());
    assert!(
        fixture
            .builder()
            .with_catalog_snapshot(fixture.catalog_snapshot())
            .with_snapshot_selection(DeltaSnapshotSelection::Version(2))
            .load_table()
            .await
            .is_err()
    );
    let table = fixture
        .builder()
        .with_catalog_snapshot(fixture.catalog_snapshot())
        .load_table()
        .await?;
    let error = table.refresh().await.unwrap_err();
    assert_eq!(
        error.reason(),
        "catalog_refresh_requires_new_catalog_snapshot"
    );
    assert_eq!(read_ids(&table, false).await?, vec![1, 2, 3]);
    let ordinary = support::RealParquetDeltaTable::new_default("ordinary-catalog-rejection")?;
    assert!(
        DeltaTableBuilder::new(ordinary.path().to_string_lossy())
            .with_catalog_snapshot(DeltaCatalogSnapshot {
                max_version: 0,
                commits: vec![]
            })
            .load_table()
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn catalog_tail_order_gaps_and_version_limit_are_validated() -> TestResult {
    let fixture = CatalogTableFixture::new()?;
    let mut duplicate = fixture.catalog_snapshot();
    duplicate.commits.push(fixture.commit.clone());
    let mut gap = fixture.catalog_snapshot();
    gap.max_version = 3;
    let mut third = fixture.commit.clone();
    third.version = 3;
    third.file_name = "00000000000000000003.aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.json".into();
    gap.commits.push(third);
    let mut reversed = gap.clone();
    reversed.commits.reverse();
    for snapshot in [
        duplicate,
        gap,
        reversed,
        DeltaCatalogSnapshot {
            max_version: u64::MAX,
            commits: vec![],
        },
    ] {
        assert!(
            fixture
                .builder()
                .with_catalog_snapshot(snapshot)
                .load_table()
                .await
                .is_err()
        );
    }
    Ok(())
}
