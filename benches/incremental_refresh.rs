//! Compare fresh and incremental Delta snapshot refresh strategies.

use std::{collections::HashSet, env, error::Error, hint::black_box, io, sync::Arc, time::Instant};

use delta_kernel::{
    Engine, Snapshot, SnapshotRef, engine_data::FilteredEngineData, log_replay::FileActionKey,
    scan::state::ScanFile,
};
use delta_kernel_default_engine::DefaultEngineBuilder;
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use url::Url;

const DEFAULT_FILES: usize = 4_000;
const DEFAULT_REPETITIONS: usize = 10;
const TABLE_METADATA: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"incremental-refresh-benchmark","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":0}}"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Config {
    files: usize,
    repetitions: usize,
}

impl Config {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, Box<dyn Error>> {
        let mut files = DEFAULT_FILES;
        let mut repetitions = DEFAULT_REPETITIONS;
        let mut args = args.into_iter();
        while let Some(argument) = args.next() {
            if argument == "--bench" {
                continue;
            }
            let value = args.next().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{argument} requires a value"),
                )
            })?;
            match argument.as_str() {
                "--files" => files = value.parse()?,
                "--repetitions" => repetitions = value.parse()?,
                _ => return Err(invalid(format!("unknown argument: {argument}")).into()),
            }
        }
        if files == 0 || repetitions == 0 {
            return Err(invalid("files and repetitions must be greater than zero").into());
        }
        Ok(Self { files, repetitions })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RefreshStrategy {
    FreshSnapshotAndListing,
    IncrementalSnapshotAndFullListing,
    IncrementalSnapshotAndListingDelta,
}

impl RefreshStrategy {
    const ALL: [Self; 3] = [
        Self::FreshSnapshotAndListing,
        Self::IncrementalSnapshotAndFullListing,
        Self::IncrementalSnapshotAndListingDelta,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::FreshSnapshotAndListing => "fresh_snapshot_and_listing",
            Self::IncrementalSnapshotAndFullListing => "incremental_snapshot_and_full_listing",
            Self::IncrementalSnapshotAndListingDelta => "incremental_snapshot_and_listing_delta",
        }
    }
}

struct Fixture {
    table_url: Url,
    storage: Arc<InMemory>,
}

impl Fixture {
    async fn create(files: usize) -> Result<Self, Box<dyn Error>> {
        let table_url = Url::parse("memory:///")?;
        let storage = Arc::new(InMemory::new());
        let mut initial_commit = String::from(TABLE_METADATA);
        for index in 0..files {
            initial_commit.push('\n');
            initial_commit.push_str(&add_action(&file_path(index)));
        }
        write_commit(storage.as_ref(), 0, initial_commit).await?;
        write_commit(storage.as_ref(), 1, add_action(&file_path(files))).await?;
        Ok(Self { table_url, storage })
    }

    fn engine(&self) -> Arc<dyn Engine> {
        Arc::new(DefaultEngineBuilder::new(self.storage.clone()).build())
    }
}

struct LayeredListing {
    base: Arc<HashSet<FileActionKey>>,
    added: HashSet<FileActionKey>,
    hidden_from_base: HashSet<FileActionKey>,
    add_batches: Vec<FilteredEngineData>,
}

impl LayeredListing {
    fn active_keys(&self) -> HashSet<FileActionKey> {
        let mut active = self.base.as_ref().clone();
        active.retain(|key| !self.hidden_from_base.contains(key));
        active.extend(self.added.iter().cloned());
        active
    }
}

enum PreparedListing {
    Materialized(HashSet<FileActionKey>),
    Layered(LayeredListing),
}

struct PreparedRefresh {
    snapshot: SnapshotRef,
    listing: PreparedListing,
}

impl PreparedRefresh {
    fn active_keys(&self) -> HashSet<FileActionKey> {
        match &self.listing {
            PreparedListing::Materialized(keys) => keys.clone(),
            PreparedListing::Layered(listing) => listing.active_keys(),
        }
    }
}

#[derive(Debug)]
struct Measurement {
    strategy: RefreshStrategy,
    repetition: usize,
    elapsed_micros: u128,
    active_files: usize,
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::parse(env::args().skip(1))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run(config))
}

async fn run(config: Config) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::create(config.files).await?;
    let engine = fixture.engine();
    let base_snapshot = Snapshot::builder_for(fixture.table_url.clone())
        .at_version(0)
        .build(engine.as_ref())?;
    let base_keys = Arc::new(active_file_keys(&base_snapshot, engine.as_ref())?);
    let target_snapshot = Snapshot::builder_for(fixture.table_url.clone())
        .at_version(1)
        .build(engine.as_ref())?;
    let expected_keys = active_file_keys(&target_snapshot, engine.as_ref())?;

    let mut measurements = Vec::with_capacity(config.repetitions.saturating_mul(3));
    for repetition in 1..=config.repetitions {
        let mut strategies = RefreshStrategy::ALL;
        if repetition % 2 == 0 {
            strategies.reverse();
        }
        for strategy in strategies {
            measurements.push(measure(
                &fixture,
                engine.as_ref(),
                &base_snapshot,
                Arc::clone(&base_keys),
                &expected_keys,
                strategy,
                repetition,
            )?);
        }
    }
    print_measurements(config.files, &measurements);
    Ok(())
}

fn measure(
    fixture: &Fixture,
    engine: &dyn Engine,
    base_snapshot: &SnapshotRef,
    base_keys: Arc<HashSet<FileActionKey>>,
    expected_keys: &HashSet<FileActionKey>,
    strategy: RefreshStrategy,
    repetition: usize,
) -> Result<Measurement, Box<dyn Error>> {
    let started_at = Instant::now();
    let prepared = match strategy {
        RefreshStrategy::FreshSnapshotAndListing => {
            prepare_fresh_refresh(&fixture.table_url, engine)?
        }
        RefreshStrategy::IncrementalSnapshotAndFullListing => {
            prepare_incremental_refresh_with_full_listing(base_snapshot, engine)?
        }
        RefreshStrategy::IncrementalSnapshotAndListingDelta => {
            prepare_incremental_refresh_with_listing_delta(base_snapshot, base_keys, engine)?
        }
    };
    let elapsed_micros = started_at.elapsed().as_micros();

    let active_keys = prepared.active_keys();
    if active_keys != *expected_keys || prepared.snapshot.version() != 1 {
        return Err(invalid(format!(
            "{} produced the wrong target snapshot",
            strategy.name()
        ))
        .into());
    }
    if let PreparedListing::Layered(listing) = &prepared.listing {
        black_box(&listing.add_batches);
    }
    Ok(Measurement {
        strategy,
        repetition,
        elapsed_micros,
        active_files: active_keys.len(),
    })
}

fn prepare_fresh_refresh(
    table_url: &Url,
    engine: &dyn Engine,
) -> Result<PreparedRefresh, delta_kernel::Error> {
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine)?;
    let listing = active_file_keys(&snapshot, engine)?;
    Ok(PreparedRefresh {
        snapshot,
        listing: PreparedListing::Materialized(listing),
    })
}

fn prepare_incremental_refresh_with_full_listing(
    base_snapshot: &SnapshotRef,
    engine: &dyn Engine,
) -> Result<PreparedRefresh, delta_kernel::Error> {
    let snapshot = Snapshot::builder_from(Arc::clone(base_snapshot)).build(engine)?;
    let listing = active_file_keys(&snapshot, engine)?;
    Ok(PreparedRefresh {
        snapshot,
        listing: PreparedListing::Materialized(listing),
    })
}

fn prepare_incremental_refresh_with_listing_delta(
    base_snapshot: &SnapshotRef,
    base_keys: Arc<HashSet<FileActionKey>>,
    engine: &dyn Engine,
) -> Result<PreparedRefresh, delta_kernel::Error> {
    let snapshot = Snapshot::builder_from(Arc::clone(base_snapshot)).build(engine)?;
    let listing = Arc::clone(&snapshot)
        .incremental_scan_builder(base_snapshot.version())
        .build(engine)?
        .ok_or_else(|| delta_kernel::Error::generic("incremental commits are unavailable"))?
        .into_listing()?;
    let added = listing.summary.live_adds;
    let mut hidden_from_base = listing.summary.removes;
    hidden_from_base.extend(added.iter().filter(|key| base_keys.contains(*key)).cloned());
    Ok(PreparedRefresh {
        snapshot,
        listing: PreparedListing::Layered(LayeredListing {
            base: base_keys,
            added,
            hidden_from_base,
            add_batches: listing.add_files,
        }),
    })
}

fn active_file_keys(
    snapshot: &SnapshotRef,
    engine: &dyn Engine,
) -> Result<HashSet<FileActionKey>, delta_kernel::Error> {
    fn collect(keys: &mut HashSet<FileActionKey>, file: ScanFile) {
        keys.insert(FileActionKey::new(file.path, None));
    }

    let scan = Arc::clone(snapshot).scan_builder().build()?;
    let mut keys = HashSet::new();
    for metadata in scan.scan_metadata(engine)? {
        keys = metadata?.visit_scan_files(keys, collect)?;
    }
    Ok(keys)
}

fn add_action(path: &str) -> String {
    format!(
        r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":262,"modificationTime":0,"dataChange":true}}}}"#
    )
}

fn file_path(index: usize) -> String {
    format!("part-{index:08}.parquet")
}

async fn write_commit(
    storage: &InMemory,
    version: u64,
    contents: String,
) -> Result<(), object_store::Error> {
    let path = Path::from(format!("_delta_log/{version:020}.json"));
    storage.put(&path, contents.into()).await?;
    Ok(())
}

fn print_measurements(base_files: usize, measurements: &[Measurement]) {
    println!("base_files,strategy,repetition,elapsed_micros,active_files");
    for measurement in measurements {
        println!(
            "{},{},{},{},{}",
            base_files,
            measurement.strategy.name(),
            measurement.repetition,
            measurement.elapsed_micros,
            measurement.active_files,
        );
    }
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn config_accepts_the_cargo_bench_flag() -> Result<(), Box<dyn Error>> {
        assert_eq!(
            Config::parse(["--files", "8", "--repetitions", "2", "--bench"].map(String::from))?,
            Config {
                files: 8,
                repetitions: 2,
            }
        );
        Ok(())
    }

    #[test]
    fn refresh_strategies_produce_the_same_target_listing() -> Result<(), Box<dyn Error>> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let fixture = Fixture::create(8).await?;
            let engine = fixture.engine();
            let base = Snapshot::builder_for(fixture.table_url.clone())
                .at_version(0)
                .build(engine.as_ref())?;
            let base_keys = Arc::new(active_file_keys(&base, engine.as_ref())?);
            let expected = prepare_fresh_refresh(&fixture.table_url, engine.as_ref())?;
            let expected_keys = expected.active_keys();

            for strategy in RefreshStrategy::ALL {
                let measurement = measure(
                    &fixture,
                    engine.as_ref(),
                    &base,
                    Arc::clone(&base_keys),
                    &expected_keys,
                    strategy,
                    1,
                )?;
                assert_eq!(measurement.active_files, 9);
            }
            Ok(())
        })
    }
}
