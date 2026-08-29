//! Synthetic benchmark for Parquet page-index range reads.
//!
//! Run with `cargo bench --bench page_index`. The benchmark compares equivalent indexed and
//! unindexed Delta fixtures for localized matches and matches scattered across every data page.

use std::env;
use std::error::Error;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{Array, ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use delta_arrow_reader::{
    DeltaComparison, DeltaPredicate, DeltaScalar, DeltaTable, DeltaTableBuilder,
};
use futures_util::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::json;

const DEFAULT_REPETITIONS: usize = 3;
const MAX_REPETITIONS: usize = 128;
const DATA_FILE: &str = "part.parquet";
const MATCH_VALUE: &str = "match";
const OTHER_VALUE: &str = "other";
const PAYLOAD_FILLER: &str = concat!(
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
);
const BENCHMARK_SHAPE: FixtureShape = FixtureShape {
    row_groups: 2,
    rows_per_group: 4_096,
    rows_per_page: 128,
    payload_columns: 16,
};

#[derive(Debug, Clone, Copy)]
struct FixtureShape {
    row_groups: usize,
    rows_per_group: usize,
    rows_per_page: usize,
    payload_columns: usize,
}

impl FixtureShape {
    fn validate(self) -> Result<(), io::Error> {
        if self.row_groups == 0
            || self.rows_per_group == 0
            || self.rows_per_page == 0
            || self.payload_columns == 0
            || !self.rows_per_group.is_multiple_of(self.rows_per_page)
            || self.pages_per_group() > self.rows_per_page
        {
            return Err(invalid("invalid page-index fixture dimensions"));
        }
        i32::try_from(self.row_count()).map_err(io::Error::other)?;
        Ok(())
    }

    const fn row_count(self) -> usize {
        self.row_groups.saturating_mul(self.rows_per_group)
    }

    const fn pages_per_group(self) -> usize {
        self.rows_per_group / self.rows_per_page
    }

    fn expected_ids(self, layout: MatchLayout) -> Vec<i32> {
        (0..self.row_count())
            .filter(|row| layout.matches(*row % self.rows_per_group, self))
            .filter_map(|row| i32::try_from(row).ok())
            .collect()
    }
}

#[derive(Debug)]
struct Config {
    repetitions: usize,
    temp_dir: PathBuf,
    retain_fixtures: bool,
}

impl Config {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, Box<dyn Error>> {
        let mut repetitions = DEFAULT_REPETITIONS;
        let mut temp_dir = env::temp_dir();
        let mut retain_fixtures = false;
        let mut args = args.into_iter();
        while let Some(argument) = args.next() {
            match argument.as_str() {
                "--repetitions" => repetitions = required_arg(&mut args, &argument)?.parse()?,
                "--temp-dir" => temp_dir = required_arg(&mut args, &argument)?.into(),
                "--retain-fixtures" => retain_fixtures = true,
                "--bench" => {}
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                other => return Err(invalid(format!("unknown argument: {other}")).into()),
            }
        }
        if !(1..=MAX_REPETITIONS).contains(&repetitions) {
            return Err(invalid("repetitions must be between 1 and 128").into());
        }
        Ok(Self {
            repetitions,
            temp_dir,
            retain_fixtures,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum MatchLayout {
    Localized,
    Scattered,
}

impl MatchLayout {
    const fn name(self) -> &'static str {
        match self {
            Self::Localized => "localized",
            Self::Scattered => "scattered",
        }
    }

    fn matches(self, row_in_group: usize, shape: FixtureShape) -> bool {
        match self {
            Self::Localized => row_in_group < shape.pages_per_group(),
            Self::Scattered => row_in_group.is_multiple_of(shape.rows_per_page),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PageIndexPresence {
    Present,
    Absent,
}

impl PageIndexPresence {
    const fn name(self) -> &'static str {
        match self {
            Self::Present => "present",
            Self::Absent => "absent",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct BenchmarkCase {
    layout: MatchLayout,
    page_index: PageIndexPresence,
}

impl BenchmarkCase {
    const ALL: [Self; 4] = [
        Self {
            layout: MatchLayout::Localized,
            page_index: PageIndexPresence::Present,
        },
        Self {
            layout: MatchLayout::Localized,
            page_index: PageIndexPresence::Absent,
        },
        Self {
            layout: MatchLayout::Scattered,
            page_index: PageIndexPresence::Present,
        },
        Self {
            layout: MatchLayout::Scattered,
            page_index: PageIndexPresence::Absent,
        },
    ];
}

#[derive(Debug)]
struct Fixture {
    path: PathBuf,
    data_file_bytes: u64,
    shape: FixtureShape,
    case: BenchmarkCase,
    retain: bool,
}

impl Fixture {
    fn create(
        temp_root: &Path,
        shape: FixtureShape,
        case: BenchmarkCase,
        retain: bool,
    ) -> Result<Self, Box<dyn Error>> {
        shape.validate()?;
        let path = temp_root.join(format!(
            "delta-arrow-reader-page-index-{}-{}-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
            case.layout.name(),
            case.page_index.name(),
        ));
        fs::create_dir_all(path.join("_delta_log"))?;

        let schema = benchmark_schema(shape.payload_columns);
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(shape.rows_per_group))
            .set_write_batch_size(shape.rows_per_page)
            .set_data_page_row_count_limit(shape.rows_per_page)
            .set_dictionary_enabled(false)
            .set_offset_index_disabled(case.page_index == PageIndexPresence::Absent)
            .build();
        let data_path = path.join(DATA_FILE);
        let mut writer = ArrowWriter::try_new(
            File::create(&data_path)?,
            Arc::clone(&schema),
            Some(properties),
        )?;
        for row_group in 0..shape.row_groups {
            writer.write(&benchmark_batch(
                Arc::clone(&schema),
                row_group.saturating_mul(shape.rows_per_group),
                shape,
                case.layout,
            )?)?;
        }
        writer.close()?;
        let data_file_bytes = fs::metadata(&data_path)?.len();
        write_delta_log(&path, shape, data_file_bytes)?;

        Ok(Self {
            path,
            data_file_bytes,
            shape,
            case,
            retain,
        })
    }

    fn uri(&self) -> String {
        self.path.to_string_lossy().into_owned()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if !self.retain {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

struct LoadedCase {
    fixture: Fixture,
    table: DeltaTable,
}

impl LoadedCase {
    async fn create(
        temp_root: &Path,
        shape: FixtureShape,
        case: BenchmarkCase,
        retain: bool,
    ) -> Result<Self, Box<dyn Error>> {
        let fixture = Fixture::create(temp_root, shape, case, retain)?;
        let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;
        Ok(Self { fixture, table })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Measurement {
    case: BenchmarkCase,
    repetition: usize,
    data_file_bytes: u64,
    qualifying_rows: usize,
    result_fingerprint: String,
    first_batch_micros: u64,
    total_micros: u64,
    range_gets: u64,
    full_gets: u64,
    bytes_received: u64,
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::parse(env::args().skip(1))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run(&config, BENCHMARK_SHAPE))
}

async fn run(config: &Config, shape: FixtureShape) -> Result<(), Box<dyn Error>> {
    let mut cases = Vec::with_capacity(BenchmarkCase::ALL.len());
    for case in BenchmarkCase::ALL {
        let loaded =
            LoadedCase::create(&config.temp_dir, shape, case, config.retain_fixtures).await?;
        if config.retain_fixtures {
            eprintln!(
                "retained page-index fixture: {}",
                loaded.fixture.path.display()
            );
        }
        cases.push(loaded);
    }

    let mut measurements = Vec::with_capacity(config.repetitions.saturating_mul(cases.len()));
    for repetition in 1..=config.repetitions {
        let mut order = [0, 1, 2, 3];
        if repetition % 2 == 0 {
            order.reverse();
        }
        for index in order {
            measurements.push(measure_case(&cases[index], repetition).await?);
        }
    }
    validate_pairs(&measurements)?;
    measurements.sort_by_key(|measurement| (measurement.case, measurement.repetition));

    println!(
        "match_layout,page_index,repetition,parquet_file_bytes,qualifying_rows,result_fingerprint,time_to_first_batch_micros,total_micros,range_get_operations,full_get_operations,bytes_received"
    );
    for measurement in measurements {
        println!(
            "{},{},{},{},{},{},{},{},{},{},{}",
            measurement.case.layout.name(),
            measurement.case.page_index.name(),
            measurement.repetition,
            measurement.data_file_bytes,
            measurement.qualifying_rows,
            measurement.result_fingerprint,
            measurement.first_batch_micros,
            measurement.total_micros,
            measurement.range_gets,
            measurement.full_gets,
            measurement.bytes_received,
        );
    }
    Ok(())
}

async fn measure_case(
    loaded: &LoadedCase,
    repetition: usize,
) -> Result<Measurement, Box<dyn Error>> {
    let projection = std::iter::once("row_id".to_owned())
        .chain((0..loaded.fixture.shape.payload_columns).map(payload_name))
        .collect::<Vec<_>>();
    let scan = loaded
        .table
        .scan()
        .with_projection(projection)
        .with_predicate(DeltaPredicate::Compare {
            column: "event_id".to_owned(),
            op: DeltaComparison::Eq,
            value: DeltaScalar::Utf8(MATCH_VALUE.to_owned()),
        })
        .with_target_partitions(1)?
        .build()
        .await?;
    let mut stream = scan.into_stream();
    let metrics = stream.metrics();
    let started = Instant::now();
    let mut first_batch_micros = None;
    let mut ids = Vec::new();
    let mut hash = 14_695_981_039_346_656_037_u64;

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        first_batch_micros.get_or_insert_with(|| saturating_u64(started.elapsed().as_micros()));
        validate_and_hash_batch(&batch, loaded.fixture.shape, &mut ids, &mut hash)?;
    }
    let total_micros = saturating_u64(started.elapsed().as_micros());
    let expected_ids = loaded
        .fixture
        .shape
        .expected_ids(loaded.fixture.case.layout);
    if ids != expected_ids {
        return Err(io::Error::other(format!(
            "{} fixture returned unexpected row IDs",
            loaded.fixture.case.layout.name()
        ))
        .into());
    }
    let snapshot = metrics.snapshot();
    Ok(Measurement {
        case: loaded.fixture.case,
        repetition,
        data_file_bytes: loaded.fixture.data_file_bytes,
        qualifying_rows: ids.len(),
        result_fingerprint: format!("fnv1a64:{hash:016x}"),
        first_batch_micros: first_batch_micros
            .ok_or_else(|| io::Error::other("scan returned no batches"))?,
        total_micros,
        range_gets: direct_metric(
            snapshot.parquet_data_file_range_get_operations,
            "range GET count",
        )?,
        full_gets: direct_metric(
            snapshot.parquet_data_file_full_get_operations,
            "full GET count",
        )?,
        bytes_received: direct_metric(snapshot.parquet_data_file_bytes_received, "bytes received")?,
    })
}

fn validate_pairs(measurements: &[Measurement]) -> Result<(), io::Error> {
    for repetition in 1..=measurements
        .iter()
        .map(|measurement| measurement.repetition)
        .max()
        .unwrap_or(0)
    {
        for layout in [MatchLayout::Localized, MatchLayout::Scattered] {
            let pair = measurements
                .iter()
                .filter(|measurement| {
                    measurement.repetition == repetition && measurement.case.layout == layout
                })
                .collect::<Vec<_>>();
            let [first, second] = pair.as_slice() else {
                return Err(io::Error::other("benchmark result pair is incomplete"));
            };
            if first.qualifying_rows != second.qualifying_rows
                || first.result_fingerprint != second.result_fingerprint
            {
                return Err(io::Error::other(format!(
                    "indexed and unindexed {} fixtures returned different results",
                    layout.name()
                )));
            }
        }
    }
    Ok(())
}

fn validate_and_hash_batch(
    batch: &RecordBatch,
    shape: FixtureShape,
    ids: &mut Vec<i32>,
    hash: &mut u64,
) -> Result<(), io::Error> {
    if batch.num_columns() != shape.payload_columns.saturating_add(1) {
        return Err(io::Error::other("benchmark output schema changed"));
    }
    let row_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| io::Error::other("row_id was not Int32"))?;
    let payloads = batch.columns()[1..]
        .iter()
        .map(|column| {
            column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| io::Error::other("payload column was not Utf8"))
        })
        .collect::<Result<Vec<_>, _>>()?;

    for row in 0..batch.num_rows() {
        if row_ids.is_null(row) {
            return Err(io::Error::other("row_id was null"));
        }
        let row_id = row_ids.value(row);
        ids.push(row_id);
        hash_optional_i32(hash, Some(row_id));
        for (payload_index, column) in payloads.iter().enumerate() {
            let actual = (!column.is_null(row)).then(|| column.value(row));
            let expected = payload_value(row_id, payload_index);
            if actual != expected.as_deref() {
                return Err(io::Error::other("payload value or null placement changed"));
            }
            hash_optional_str(hash, actual);
        }
    }
    Ok(())
}

fn benchmark_schema(payload_columns: usize) -> SchemaRef {
    let mut fields = vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("event_id", DataType::Utf8, false),
    ];
    fields.extend(
        (0..payload_columns).map(|index| Field::new(payload_name(index), DataType::Utf8, true)),
    );
    Arc::new(Schema::new(fields))
}

fn benchmark_batch(
    schema: SchemaRef,
    first_row: usize,
    shape: FixtureShape,
    layout: MatchLayout,
) -> Result<RecordBatch, Box<dyn Error>> {
    let rows = first_row..first_row.saturating_add(shape.rows_per_group);
    let row_ids = rows
        .clone()
        .map(i32::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    let mut columns = Vec::with_capacity(shape.payload_columns.saturating_add(2));
    columns.push(Arc::new(Int32Array::from(row_ids.clone())) as ArrayRef);
    columns.push(
        Arc::new(StringArray::from_iter_values(rows.clone().map(|row| {
            if layout.matches(row % shape.rows_per_group, shape) {
                MATCH_VALUE
            } else {
                OTHER_VALUE
            }
        }))) as ArrayRef,
    );
    for payload_index in 0..shape.payload_columns {
        columns.push(Arc::new(StringArray::from_iter(
            row_ids
                .iter()
                .map(|row_id| payload_value(*row_id, payload_index)),
        )));
    }
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn payload_value(row_id: i32, payload_index: usize) -> Option<String> {
    let row_id_usize = usize::try_from(row_id).ok()?;
    (!(row_id_usize + payload_index).is_multiple_of(17))
        .then(|| format!("payload-{payload_index:03}-{row_id:08}-{PAYLOAD_FILLER}"))
}

fn payload_name(index: usize) -> String {
    format!("payload_{index:03}")
}

fn write_delta_log(root: &Path, shape: FixtureShape, data_file_bytes: u64) -> io::Result<()> {
    let mut fields = vec![
        json!({"name": "row_id", "type": "integer", "nullable": false, "metadata": {}}),
        json!({"name": "event_id", "type": "string", "nullable": false, "metadata": {}}),
    ];
    fields.extend((0..shape.payload_columns).map(|index| {
        json!({"name": payload_name(index), "type": "string", "nullable": true, "metadata": {}})
    }));
    let schema_string = json!({"type": "struct", "fields": fields}).to_string();
    let protocol = json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}});
    let metadata = json!({
        "metaData": {
            "id": "delta-arrow-reader-page-index-benchmark",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema_string,
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1_587_968_585_495_i64,
        }
    });
    let add = json!({
        "add": {
            "path": DATA_FILE,
            "partitionValues": {},
            "size": data_file_bytes,
            "modificationTime": 1_587_968_586_000_i64,
            "dataChange": true,
        }
    });
    fs::write(
        root.join("_delta_log/00000000000000000000.json"),
        format!("{protocol}\n{metadata}\n{add}\n"),
    )
}

fn hash_optional_i32(hash: &mut u64, value: Option<i32>) {
    let (marker, bytes) = match value {
        Some(value) => (1, value.to_le_bytes()),
        None => (0, [0; 4]),
    };
    fnv1a64_update(hash, &[marker]);
    fnv1a64_update(hash, &bytes);
}

fn hash_optional_str(hash: &mut u64, value: Option<&str>) {
    match value {
        Some(value) => {
            fnv1a64_update(hash, &[1]);
            fnv1a64_update(
                hash,
                &u64::try_from(value.len()).unwrap_or(u64::MAX).to_le_bytes(),
            );
            fnv1a64_update(hash, value.as_bytes());
        }
        None => fnv1a64_update(hash, &[0]),
    }
}

fn fnv1a64_update(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(1_099_511_628_211);
    }
}

fn direct_metric(value: Option<u64>, name: &str) -> Result<u64, io::Error> {
    value.ok_or_else(|| io::Error::other(format!("direct backend did not report {name}")))
}

fn saturating_u64(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn required_arg(
    args: &mut impl Iterator<Item = String>,
    argument: &str,
) -> Result<String, io::Error> {
    args.next()
        .ok_or_else(|| invalid(format!("missing value for {argument}")))
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn print_help() {
    println!(
        "cargo bench --bench page_index -- [--repetitions N] [--temp-dir PATH] [--retain-fixtures]"
    );
}

#[cfg(test)]
#[allow(dead_code, unused_imports)]
mod tests {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    use super::*;

    type TestResult = Result<(), Box<dyn Error>>;

    const TEST_SHAPE: FixtureShape = FixtureShape {
        row_groups: 2,
        rows_per_group: 512,
        rows_per_page: 64,
        payload_columns: 8,
    };

    #[test]
    fn fixtures_control_offset_index_presence() -> TestResult {
        for case in BenchmarkCase::ALL {
            let fixture = Fixture::create(&env::temp_dir(), TEST_SHAPE, case, false)?;
            let reader = SerializedFileReader::new(File::open(fixture.path.join(DATA_FILE))?)?;
            let has_offset_index = reader
                .metadata()
                .row_group(0)
                .column(0)
                .offset_index_offset()
                .is_some();
            assert_eq!(
                has_offset_index,
                case.page_index == PageIndexPresence::Present
            );
        }
        Ok(())
    }

    #[test]
    fn indexed_and_unindexed_cases_return_identical_results() -> TestResult {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let mut measurements = Vec::new();
            for case in BenchmarkCase::ALL {
                let loaded = LoadedCase::create(&env::temp_dir(), TEST_SHAPE, case, false).await?;
                measurements.push(measure_case(&loaded, 1).await?);
            }
            validate_pairs(&measurements)?;
            assert!(measurements.iter().all(|measurement| {
                measurement.qualifying_rows
                    == TEST_SHAPE
                        .row_groups
                        .saturating_mul(TEST_SHAPE.pages_per_group())
            }));
            Ok::<_, Box<dyn Error>>(())
        })
    }

    #[test]
    fn parser_accepts_documented_options() -> TestResult {
        let config = Config::parse(
            [
                "--repetitions",
                "5",
                "--temp-dir",
                "target/page-index",
                "--retain-fixtures",
            ]
            .map(str::to_owned),
        )?;
        assert_eq!(config.repetitions, 5);
        assert_eq!(config.temp_dir, PathBuf::from("target/page-index"));
        assert!(config.retain_fixtures);
        assert!(Config::parse(["--repetitions", "0"].map(str::to_owned)).is_err());
        assert!(Config::parse(["--unknown"].map(str::to_owned)).is_err());
        Ok(())
    }
}
