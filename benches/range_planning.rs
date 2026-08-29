//! Synthetic fixtures and controlled transport for Parquet range-planning benchmarks.

use std::{
    collections::BTreeMap,
    env,
    error::Error,
    fs::{self, File},
    io,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use arrow::{
    array::{Array, ArrayRef, Int32Array, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use delta_arrow_reader::{
    DeltaComparison, DeltaPredicate, DeltaScalar, DeltaScanExecutionOptions, DeltaStorageOptions,
    DeltaTable, DeltaTableBuilder,
    diagnostics::parquet_range_planning::{
        Policy as ParquetRangeReadPolicy, snapshot as range_planning_snapshot,
    },
};
use futures_util::StreamExt;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use serde_json::json;

#[path = "range_planning/controlled_http.rs"]
mod controlled_http;

use controlled_http::ControlledHttpServer;

const MATCH_VALUE: &str = "match";
const OTHER_VALUE: &str = "other";
const DEFAULT_REPETITIONS: usize = 3;
const MAX_REPETITIONS: usize = 128;
static NEXT_FIXTURE_ID: AtomicU64 = AtomicU64::new(0);
const BENCHMARK_SHAPE: FixtureShape = FixtureShape {
    data_files: 4,
    row_groups: 2,
    rows_per_group: 256,
    rows_per_page: 32,
    payload_columns: 48,
};
const PAYLOAD_FILLER: &str = concat!(
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
);

#[derive(Debug, Clone, Copy)]
struct FixtureShape {
    data_files: usize,
    row_groups: usize,
    rows_per_group: usize,
    rows_per_page: usize,
    payload_columns: usize,
}

impl FixtureShape {
    fn validate(self) -> Result<(), io::Error> {
        if self.data_files == 0
            || self.row_groups < 2
            || self.rows_per_group == 0
            || self.rows_per_page == 0
            || self.payload_columns < 4
            || !self.rows_per_group.is_multiple_of(self.rows_per_page)
        {
            return Err(invalid("invalid range-planning fixture dimensions"));
        }
        i32::try_from(self.row_count()).map_err(io::Error::other)?;
        Ok(())
    }

    const fn row_count(self) -> usize {
        self.data_files
            .saturating_mul(self.row_groups)
            .saturating_mul(self.rows_per_group)
    }

    const fn rows_per_file(self) -> usize {
        self.row_groups.saturating_mul(self.rows_per_group)
    }

    fn expected_row_ids(self) -> Vec<i32> {
        (0..self.row_count())
            .filter(|row| (row % self.rows_per_group).is_multiple_of(self.rows_per_page))
            .filter_map(|row| i32::try_from(row).ok())
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectionDensity {
    Dense,
    Sparse,
}

impl ProjectionDensity {
    const ALL: [Self; 2] = [Self::Dense, Self::Sparse];

    const fn name(self) -> &'static str {
        match self {
            Self::Dense => "dense",
            Self::Sparse => "sparse",
        }
    }

    fn payload_indices(self, payload_columns: usize) -> impl Iterator<Item = usize> {
        let step = match self {
            Self::Dense => 2,
            Self::Sparse => 4,
        };
        (0..payload_columns).step_by(step)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchmarkPolicy {
    Automatic,
    ExactRanges,
    MergeRangesWithinOneMegabyte,
    StoreImplementation,
}

impl BenchmarkPolicy {
    const ALL: [Self; 4] = [
        Self::Automatic,
        Self::ExactRanges,
        Self::MergeRangesWithinOneMegabyte,
        Self::StoreImplementation,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::Automatic => "automatic",
            Self::ExactRanges => "exact_ranges",
            Self::MergeRangesWithinOneMegabyte => "merge_ranges_within_one_megabyte",
            Self::StoreImplementation => "store_implementation",
        }
    }

    const fn diagnostic_policy(self) -> ParquetRangeReadPolicy {
        match self {
            Self::Automatic => ParquetRangeReadPolicy::Automatic,
            Self::ExactRanges => ParquetRangeReadPolicy::ExactRanges,
            Self::MergeRangesWithinOneMegabyte => {
                ParquetRangeReadPolicy::MergeRangesWithinOneMegabyte
            }
            Self::StoreImplementation => ParquetRangeReadPolicy::StoreImplementation,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct BenchmarkCase {
    profile: TransportProfile,
    density: ProjectionDensity,
    policy: BenchmarkPolicy,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TransportProfile {
    name: &'static str,
    request_latency: Duration,
    shared_throughput_bytes_per_second: u64,
}

impl TransportProfile {
    const LOW_LATENCY_LOW_THROUGHPUT: Self = Self {
        name: "low_latency_low_throughput",
        request_latency: Duration::from_millis(1),
        shared_throughput_bytes_per_second: 4 * 1024 * 1024,
    };
    const BALANCED: Self = Self {
        name: "balanced",
        request_latency: Duration::from_millis(8),
        shared_throughput_bytes_per_second: 64 * 1024 * 1024,
    };
    const HIGH_LATENCY_HIGH_THROUGHPUT: Self = Self {
        name: "high_latency_high_throughput",
        request_latency: Duration::from_millis(20),
        shared_throughput_bytes_per_second: 512 * 1024 * 1024,
    };

    const ALL: [Self; 3] = [
        Self::LOW_LATENCY_LOW_THROUGHPUT,
        Self::BALANCED,
        Self::HIGH_LATENCY_HIGH_THROUGHPUT,
    ];
}

#[derive(Debug, Clone)]
struct Measurement {
    case: BenchmarkCase,
    repetition: usize,
    data_file_bytes: u64,
    qualifying_rows: usize,
    result_fingerprint: String,
    exact_range_count: u64,
    exact_range_bytes: u64,
    planned_request_count: u64,
    range_concurrency: u64,
    planned_request_waves: u64,
    planned_bytes: u64,
    actual_range_requests: u64,
    actual_range_bytes: u64,
    selected_candidate: &'static str,
    predicted_micros: Option<u64>,
    observed_plan_micros: u64,
    prediction_error_micros: Option<i128>,
    total_micros: u64,
    cold_start_plans: u64,
    cost_based_exact_plans: u64,
    cost_based_merged_plans: u64,
    store_delegated_plans: u64,
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::parse(env::args().skip(1))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run(&config, BENCHMARK_SHAPE))
}

async fn run(config: &Config, shape: FixtureShape) -> Result<(), Box<dyn Error>> {
    let mut measurements = Vec::new();
    for profile in TransportProfile::ALL {
        let fixture = Fixture::create(&config.temp_dir, shape, profile, config.retain_fixtures)?;
        if config.retain_fixtures {
            eprintln!(
                "retained range-planning fixture: {}",
                fixture.path.display()
            );
        }
        let table = DeltaTableBuilder::new(&fixture.table_uri)
            .with_storage_options(fixture.storage_options.clone())
            .load_table()
            .await?;
        for repetition in 1..=config.repetitions {
            let mut policies = BenchmarkPolicy::ALL;
            if repetition % 2 == 0 {
                policies.reverse();
            }
            for density in ProjectionDensity::ALL {
                for policy in policies {
                    measurements.push(
                        measure_case(
                            &fixture,
                            &table,
                            shape,
                            BenchmarkCase {
                                profile,
                                density,
                                policy,
                            },
                            repetition,
                        )
                        .await?,
                    );
                }
            }
        }
    }
    validate_measurements(&measurements)?;
    measurements.sort_by_key(|measurement| {
        (
            measurement.case.profile.name,
            measurement.case.density.name(),
            measurement.case.policy.name(),
            measurement.repetition,
        )
    });
    print_measurements(&measurements);
    Ok(())
}

#[derive(Debug)]
struct Fixture {
    path: PathBuf,
    table_uri: String,
    storage_options: DeltaStorageOptions,
    server: ControlledHttpServer,
    data_file_bytes: u64,
    retain: bool,
}

impl Fixture {
    fn create(
        temp_root: &Path,
        shape: FixtureShape,
        profile: TransportProfile,
        retain: bool,
    ) -> Result<Self, Box<dyn Error>> {
        shape.validate()?;
        let path = fixture_path(
            temp_root,
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
        );
        fs::create_dir_all(path.join("_delta_log"))?;

        let schema = benchmark_schema(shape.payload_columns);
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(shape.rows_per_group))
            .set_write_batch_size(shape.rows_per_page)
            .set_data_page_row_count_limit(shape.rows_per_page)
            .set_dictionary_enabled(false)
            .set_compression(Compression::UNCOMPRESSED)
            .set_offset_index_disabled(false)
            .build();
        let mut data_files = Vec::with_capacity(shape.data_files);
        let mut data_file_bytes = 0_u64;
        for data_file_index in 0..shape.data_files {
            let name = data_file_name(data_file_index);
            let data_path = path.join(&name);
            let mut writer = ArrowWriter::try_new(
                File::create(&data_path)?,
                Arc::clone(&schema),
                Some(properties.clone()),
            )?;
            for row_group in 0..shape.row_groups {
                writer.write(&benchmark_batch(
                    Arc::clone(&schema),
                    data_file_index
                        .saturating_mul(shape.rows_per_file())
                        .saturating_add(row_group.saturating_mul(shape.rows_per_group)),
                    shape,
                )?)?;
            }
            writer.close()?;
            let bytes = fs::metadata(data_path)?.len();
            data_file_bytes = data_file_bytes.saturating_add(bytes);
            data_files.push((name, bytes));
        }
        write_delta_log(&path, shape, &data_files)?;

        let server = ControlledHttpServer::start(path.clone(), profile)?;
        let table_uri = server.url().to_owned();
        Ok(Self {
            path,
            table_uri,
            storage_options: BTreeMap::from([("allow_http".to_owned(), "true".to_owned())]),
            server,
            data_file_bytes,
            retain,
        })
    }
}

fn fixture_path(temp_root: &Path, created_at_nanos: u128) -> PathBuf {
    let sequence = NEXT_FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
    temp_root.join(format!(
        "delta-arrow-reader-range-planning-{}-{created_at_nanos}-{sequence}",
        std::process::id(),
    ))
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.stop();
        if !self.retain {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

async fn measure_case(
    fixture: &Fixture,
    table: &DeltaTable,
    shape: FixtureShape,
    case: BenchmarkCase,
    repetition: usize,
) -> Result<Measurement, Box<dyn Error>> {
    let payload_indices = case
        .density
        .payload_indices(shape.payload_columns)
        .collect::<Vec<_>>();
    let projection = std::iter::once("row_id".to_owned())
        .chain(payload_indices.iter().copied().map(payload_name))
        .collect::<Vec<_>>();
    let execution_options = DeltaScanExecutionOptions::new()
        .with_parquet_range_read_policy(case.policy.diagnostic_policy())
        .with_max_concurrent_file_reads_per_scan(Some(1))?
        .with_max_concurrent_file_reads_per_partition(1)?
        .with_prefetch_files_per_partition(0);
    let scan = table
        .scan()
        .with_projection(projection)
        .with_predicate(DeltaPredicate::Compare {
            column: "event_id".to_owned(),
            op: DeltaComparison::Eq,
            value: DeltaScalar::Utf8(MATCH_VALUE.to_owned()),
        })
        .with_target_partitions(1)?
        .with_execution_options(execution_options)
        .build()
        .await?;
    let mut stream = scan.into_stream();
    let metrics = stream.metrics();
    fixture.server.reset_stats();
    let started = Instant::now();
    let mut row_ids = Vec::new();
    while let Some(batch) = stream.next().await {
        validate_batch(&batch?, &payload_indices, &mut row_ids)?;
    }
    let total_micros = saturating_u64(started.elapsed().as_micros());
    row_ids.sort_unstable();
    if row_ids != shape.expected_row_ids() {
        return Err(io::Error::other("benchmark scan returned unexpected row IDs").into());
    }

    let snapshot = metrics.snapshot();
    let diagnostic = range_planning_snapshot(&metrics);
    let server = fixture.server.stats();
    let exact_range_count = direct_metric(
        snapshot.parquet_data_file_exact_ranges_requested,
        "exact range count",
    )?;
    let exact_range_bytes = direct_metric(
        snapshot.parquet_data_file_exact_range_bytes_requested,
        "exact range bytes",
    )?;
    let planned_request_count = direct_metric(
        snapshot.parquet_data_file_physical_range_requests_planned,
        "planned request count",
    )?;
    let planned_bytes = direct_metric(
        snapshot.parquet_data_file_physical_range_bytes_planned,
        "planned bytes",
    )?;
    let cold_start_plans = direct_metric(
        snapshot.parquet_data_file_cold_start_range_plans,
        "cold-start plans",
    )?;
    let cost_based_exact_plans = direct_metric(
        snapshot.parquet_data_file_cost_based_exact_range_plans,
        "cost-based exact plans",
    )?;
    let cost_based_merged_plans = direct_metric(
        snapshot.parquet_data_file_cost_based_merged_range_plans,
        "cost-based merged plans",
    )?;
    let store_delegated_plans = direct_metric(
        snapshot.parquet_data_file_store_delegated_range_plans,
        "store-delegated plans",
    )?;
    let predicted_micros = (case.policy != BenchmarkPolicy::StoreImplementation).then(|| {
        predicted_plan_micros(
            diagnostic.physical_range_request_waves_planned,
            planned_bytes,
            case.profile,
        )
    });
    let prediction_error_micros = predicted_micros.map(|predicted| {
        i128::from(diagnostic.successful_plan_time_micros) - i128::from(predicted)
    });

    Ok(Measurement {
        case,
        repetition,
        data_file_bytes: fixture.data_file_bytes,
        qualifying_rows: row_ids.len(),
        result_fingerprint: row_id_fingerprint(&row_ids),
        exact_range_count,
        exact_range_bytes,
        planned_request_count,
        range_concurrency: diagnostic.max_concurrent_physical_range_requests,
        planned_request_waves: diagnostic.physical_range_request_waves_planned,
        planned_bytes,
        actual_range_requests: server.range_requests,
        actual_range_bytes: server.range_bytes,
        selected_candidate: selected_candidate(
            case.policy,
            cold_start_plans,
            cost_based_exact_plans,
            cost_based_merged_plans,
        ),
        predicted_micros,
        observed_plan_micros: diagnostic.successful_plan_time_micros,
        prediction_error_micros,
        total_micros,
        cold_start_plans,
        cost_based_exact_plans,
        cost_based_merged_plans,
        store_delegated_plans,
    })
}

fn validate_batch(
    batch: &RecordBatch,
    payload_indices: &[usize],
    row_ids: &mut Vec<i32>,
) -> Result<(), io::Error> {
    if batch.num_columns() != payload_indices.len().saturating_add(1) {
        return Err(io::Error::other("benchmark output schema changed"));
    }
    let ids = batch
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
        if ids.is_null(row) {
            return Err(io::Error::other("row_id was null"));
        }
        let row_id = ids.value(row);
        row_ids.push(row_id);
        for (column, payload_index) in payloads.iter().zip(payload_indices) {
            let actual = (!column.is_null(row)).then(|| column.value(row));
            let expected = payload_value(row_id, *payload_index);
            if actual != expected.as_deref() {
                return Err(io::Error::other("payload value or null placement changed"));
            }
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
) -> Result<RecordBatch, Box<dyn Error>> {
    let rows = first_row..first_row.saturating_add(shape.rows_per_group);
    let row_ids = rows
        .clone()
        .map(i32::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    let mut columns = Vec::with_capacity(shape.payload_columns.saturating_add(2));
    columns.push(Arc::new(Int32Array::from(row_ids.clone())) as ArrayRef);
    columns.push(Arc::new(StringArray::from_iter_values(rows.map(|row| {
        if (row % shape.rows_per_group).is_multiple_of(shape.rows_per_page) {
            MATCH_VALUE
        } else {
            OTHER_VALUE
        }
    }))) as ArrayRef);
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

fn data_file_name(index: usize) -> String {
    format!("part-{index:03}.parquet")
}

fn write_delta_log(
    root: &Path,
    shape: FixtureShape,
    data_files: &[(String, u64)],
) -> io::Result<()> {
    let mut fields = vec![
        json!({"name": "row_id", "type": "integer", "nullable": false, "metadata": {}}),
        json!({"name": "event_id", "type": "string", "nullable": false, "metadata": {}}),
    ];
    fields.extend((0..shape.payload_columns).map(|index| {
        json!({"name": payload_name(index), "type": "string", "nullable": true, "metadata": {}})
    }));
    let protocol = json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}});
    let metadata = json!({
        "metaData": {
            "id": "delta-arrow-reader-range-planning-benchmark",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": json!({"type": "struct", "fields": fields}).to_string(),
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1_587_968_585_495_i64,
        }
    });
    let adds = data_files.iter().map(|(path, size)| {
        json!({
            "add": {
                "path": path,
                "partitionValues": {},
                "size": size,
                "modificationTime": 1_587_968_586_000_i64,
                "dataChange": true,
            }
        })
    });
    fs::write(
        root.join("_delta_log/00000000000000000000.json"),
        std::iter::once(protocol)
            .chain(std::iter::once(metadata))
            .chain(adds)
            .map(|action| format!("{action}\n"))
            .collect::<String>(),
    )
}

fn validate_measurements(measurements: &[Measurement]) -> Result<(), io::Error> {
    for profile in TransportProfile::ALL {
        for density in ProjectionDensity::ALL {
            for repetition in 1..=measurements
                .iter()
                .map(|measurement| measurement.repetition)
                .max()
                .unwrap_or(0)
            {
                let group = measurements
                    .iter()
                    .filter(|measurement| {
                        measurement.case.profile == profile
                            && measurement.case.density == density
                            && measurement.repetition == repetition
                    })
                    .collect::<Vec<_>>();
                let Some(first) = group.first() else {
                    return Err(io::Error::other("benchmark result group is empty"));
                };
                if group.len() != BenchmarkPolicy::ALL.len()
                    || group.iter().any(|measurement| {
                        measurement.qualifying_rows != first.qualifying_rows
                            || measurement.result_fingerprint != first.result_fingerprint
                    })
                {
                    return Err(io::Error::other(
                        "range policies returned different query results",
                    ));
                }
            }
        }
    }

    let automatic = measurements
        .iter()
        .filter(|measurement| measurement.case.policy == BenchmarkPolicy::Automatic)
        .collect::<Vec<_>>();
    if !automatic
        .iter()
        .any(|measurement| measurement.cost_based_exact_plans != 0)
        || !automatic
            .iter()
            .any(|measurement| measurement.cost_based_merged_plans != 0)
    {
        let decisions = automatic
            .iter()
            .map(|measurement| {
                format!(
                    "{}/{}:cold={},exact={},merged={}",
                    measurement.case.profile.name,
                    measurement.case.density.name(),
                    measurement.cold_start_plans,
                    measurement.cost_based_exact_plans,
                    measurement.cost_based_merged_plans,
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        return Err(io::Error::other(format!(
            "automatic benchmark cases did not exercise both exact and merged decisions: {decisions}"
        )));
    }
    Ok(())
}

fn selected_candidate(
    policy: BenchmarkPolicy,
    cold_start_plans: u64,
    cost_based_exact_plans: u64,
    cost_based_merged_plans: u64,
) -> &'static str {
    match policy {
        BenchmarkPolicy::ExactRanges => "exact_ranges",
        BenchmarkPolicy::MergeRangesWithinOneMegabyte => "merge_ranges_within_one_megabyte",
        BenchmarkPolicy::StoreImplementation => "store_implementation",
        BenchmarkPolicy::Automatic
            if cost_based_exact_plans != 0 && cost_based_merged_plans != 0 =>
        {
            "automatic_mixed"
        }
        BenchmarkPolicy::Automatic if cost_based_merged_plans != 0 => "automatic_merged",
        BenchmarkPolicy::Automatic if cost_based_exact_plans != 0 => "automatic_exact",
        BenchmarkPolicy::Automatic if cold_start_plans != 0 => "automatic_cold_start",
        BenchmarkPolicy::Automatic => "automatic_no_range_plan",
    }
}

fn predicted_plan_micros(waves: u64, planned_bytes: u64, profile: TransportProfile) -> u64 {
    let latency_micros = profile
        .request_latency
        .as_micros()
        .saturating_mul(waves.into());
    let transfer_micros = u128::from(planned_bytes).saturating_mul(1_000_000)
        / u128::from(profile.shared_throughput_bytes_per_second);
    saturating_u64(latency_micros.saturating_add(transfer_micros))
}

fn row_id_fingerprint(row_ids: &[i32]) -> String {
    let mut hash = 14_695_981_039_346_656_037_u64;
    for row_id in row_ids {
        for byte in row_id.to_le_bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(1_099_511_628_211);
        }
    }
    format!("fnv1a64:{hash:016x}")
}

fn print_measurements(measurements: &[Measurement]) {
    println!(
        "transport_profile,projection_density,policy,repetition,parquet_file_bytes,qualifying_rows,result_fingerprint,request_latency_micros,shared_throughput_bytes_per_second,range_concurrency,selected_candidate,exact_range_count,exact_range_bytes,planned_request_count,planned_request_waves,planned_bytes,actual_range_requests,actual_range_bytes,byte_amplification,predicted_micros,observed_plan_micros,prediction_error_micros,total_micros,cold_start_plans,cost_based_exact_plans,cost_based_merged_plans,store_delegated_plans"
    );
    for measurement in measurements {
        println!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            measurement.case.profile.name,
            measurement.case.density.name(),
            measurement.case.policy.name(),
            measurement.repetition,
            measurement.data_file_bytes,
            measurement.qualifying_rows,
            measurement.result_fingerprint,
            saturating_u64(measurement.case.profile.request_latency.as_micros()),
            measurement.case.profile.shared_throughput_bytes_per_second,
            measurement.range_concurrency,
            measurement.selected_candidate,
            measurement.exact_range_count,
            measurement.exact_range_bytes,
            measurement.planned_request_count,
            measurement.planned_request_waves,
            measurement.planned_bytes,
            measurement.actual_range_requests,
            measurement.actual_range_bytes,
            byte_amplification(measurement.planned_bytes, measurement.exact_range_bytes),
            optional(measurement.predicted_micros),
            measurement.observed_plan_micros,
            optional(measurement.prediction_error_micros),
            measurement.total_micros,
            measurement.cold_start_plans,
            measurement.cost_based_exact_plans,
            measurement.cost_based_merged_plans,
            measurement.store_delegated_plans,
        );
    }
}

fn byte_amplification(planned_bytes: u64, exact_bytes: u64) -> String {
    if planned_bytes == 0 || exact_bytes == 0 {
        return String::new();
    }
    format!("{:.6}", planned_bytes as f64 / exact_bytes as f64)
}

fn optional(value: Option<impl ToString>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
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

fn print_help() {
    println!(
        "cargo bench --bench range_planning -- [--repetitions N] [--temp-dir PATH] [--retain-fixtures]"
    );
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[cfg(test)]
#[allow(dead_code, unused_imports)]
mod tests {
    use delta_arrow_reader::DeltaTableBuilder;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    use super::*;

    type TestResult = Result<(), Box<dyn Error>>;

    const TEST_SHAPE: FixtureShape = FixtureShape {
        data_files: 2,
        row_groups: 2,
        rows_per_group: 256,
        rows_per_page: 64,
        payload_columns: 8,
    };

    const TEST_PROFILE: TransportProfile = TransportProfile {
        name: "test",
        request_latency: Duration::ZERO,
        shared_throughput_bytes_per_second: u64::MAX,
    };

    #[test]
    fn fixture_has_indexed_row_groups_and_interleaved_projections() -> TestResult {
        let fixture = Fixture::create(&std::env::temp_dir(), TEST_SHAPE, TEST_PROFILE, false)?;
        let reader = SerializedFileReader::new(File::open(fixture.path.join(data_file_name(0)))?)?;
        assert_eq!(reader.metadata().num_row_groups(), TEST_SHAPE.row_groups);
        assert!(reader.metadata().row_groups().iter().all(|row_group| {
            row_group
                .columns()
                .iter()
                .all(|column| column.offset_index_offset().is_some())
        }));
        assert_eq!(
            ProjectionDensity::Dense
                .payload_indices(TEST_SHAPE.payload_columns)
                .collect::<Vec<_>>(),
            vec![0, 2, 4, 6]
        );
        assert_eq!(
            ProjectionDensity::Sparse
                .payload_indices(TEST_SHAPE.payload_columns)
                .collect::<Vec<_>>(),
            vec![0, 4]
        );
        assert_eq!(
            TEST_SHAPE.expected_row_ids().len(),
            TEST_SHAPE.data_files
                * TEST_SHAPE.row_groups
                * (TEST_SHAPE.rows_per_group / TEST_SHAPE.rows_per_page)
        );
        Ok(())
    }

    #[test]
    fn controlled_http_fixture_loads_as_a_delta_table() -> TestResult {
        let fixture = Fixture::create(&std::env::temp_dir(), TEST_SHAPE, TEST_PROFILE, false)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let table = runtime.block_on(
            DeltaTableBuilder::new(&fixture.table_uri)
                .with_storage_options(fixture.storage_options.clone())
                .load_table(),
        )?;
        assert_eq!(table.version(), 0);
        assert_eq!(
            table.schema().fields().len(),
            TEST_SHAPE.payload_columns + 2
        );
        assert!(fixture.data_file_bytes > 0);
        Ok(())
    }

    #[test]
    fn all_range_policies_return_identical_remote_query_results() -> TestResult {
        let fixture = Fixture::create(&std::env::temp_dir(), TEST_SHAPE, TEST_PROFILE, false)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let table = DeltaTableBuilder::new(&fixture.table_uri)
                .with_storage_options(fixture.storage_options.clone())
                .load_table()
                .await?;
            let mut measurements = Vec::new();
            for policy in BenchmarkPolicy::ALL {
                let case = BenchmarkCase {
                    profile: TEST_PROFILE,
                    density: ProjectionDensity::Sparse,
                    policy,
                };
                let measurement = measure_case(&fixture, &table, TEST_SHAPE, case, 1)
                    .await
                    .map_err(|error| benchmark_test_error(case, error.as_ref()))?;
                measurements.push(measurement);
            }
            let first = measurements
                .first()
                .ok_or_else(|| io::Error::other("benchmark produced no measurements"))?;
            assert!(measurements.iter().all(|measurement| {
                measurement.qualifying_rows == first.qualifying_rows
                    && measurement.result_fingerprint == first.result_fingerprint
                    && measurement.actual_range_requests != 0
                    && measurement.actual_range_bytes != 0
            }));
            assert!(measurements.iter().all(|measurement| {
                if measurement.case.policy == BenchmarkPolicy::StoreImplementation {
                    measurement.planned_request_waves == 0 && measurement.predicted_micros.is_none()
                } else {
                    measurement.planned_request_waves != 0 && measurement.predicted_micros.is_some()
                }
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
                "target/range-planning",
                "--retain-fixtures",
            ]
            .map(str::to_owned),
        )?;
        assert_eq!(config.repetitions, 5);
        assert_eq!(config.temp_dir, PathBuf::from("target/range-planning"));
        assert!(config.retain_fixtures);
        assert!(Config::parse(["--repetitions", "0"].map(str::to_owned)).is_err());
        assert!(Config::parse(["--unknown"].map(str::to_owned)).is_err());
        Ok(())
    }

    #[test]
    fn transport_profile_names_are_stable() {
        assert_eq!(
            TransportProfile::ALL.map(|profile| profile.name),
            [
                "low_latency_low_throughput",
                "balanced",
                "high_latency_high_throughput"
            ]
        );
    }

    #[test]
    fn fixture_paths_are_unique_when_the_clock_does_not_advance() {
        let temp_root = Path::new("benchmark-fixtures");
        let first = fixture_path(temp_root, 123);
        let second = fixture_path(temp_root, 123);

        assert_ne!(first, second);
        assert_eq!(first.parent(), Some(temp_root));
        assert_eq!(second.parent(), Some(temp_root));
    }

    fn benchmark_test_error(case: BenchmarkCase, error: &(dyn Error + 'static)) -> io::Error {
        let mut message = format!(
            "range benchmark failed: profile={} projection={} policy={}: {error}",
            case.profile.name,
            case.density.name(),
            case.policy.name(),
        );
        let mut source = error.source();
        while let Some(error) = source {
            message.push_str("\ncaused by: ");
            message.push_str(&error.to_string());
            source = error.source();
        }
        io::Error::other(message)
    }

    #[test]
    fn benchmark_test_errors_include_the_case_and_source_chain() {
        let source = io::Error::new(io::ErrorKind::ConnectionReset, "connection reset by peer");
        let error = parquet::errors::ParquetError::External(Box::new(source));
        let reported = benchmark_test_error(
            BenchmarkCase {
                profile: TEST_PROFILE,
                density: ProjectionDensity::Sparse,
                policy: BenchmarkPolicy::ExactRanges,
            },
            &error,
        )
        .to_string();

        assert!(reported.contains("profile=test projection=sparse policy=exact_ranges"));
        assert!(reported.contains("\ncaused by: connection reset by peer"));
    }
}
