//! Frozen reader benchmark parity harness from delta-funnel issue #459.

use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use delta_arrow_reader::{
    DeltaScanExecutionOptions, DeltaStorageOptions, DeltaTableBuilder,
    ParquetMetadataPreparationLimits, ParquetMetadataPreparationReport, ParquetReaderBackend,
    datafusion::{ScanMetricsSnapshot, ScanOptions, collect_scan_metrics, register_table},
};
use delta_kernel::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use delta_kernel::actions::deletion_vector_writer::{
    KernelDeletionVector, StreamingDeletionVectorWriter,
};
use futures_util::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

const MIB: u64 = 1024 * 1024;
const BENCHMARK_SCHEMA_VERSION: u32 = 32;
const DEFAULT_REPETITIONS: usize = 3;
const DEFAULT_QUERIES_PER_TABLE: usize = 1;
const MAX_REPETITIONS: usize = 128;
const MAX_QUERIES_PER_TABLE: usize = 128;
const MODIFICATION_TIME_MS: i64 = 1_587_968_586_000;
const FROZEN_PARQUET_CREATED_BY: &str = "parquet-rs version 58.3.0";
const PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
const DELETION_VECTOR_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#;
const RELATIVE_DV_ID: &str = "vBn[lx{q8@P<9BNH/isA";
const RELATIVE_DV_FILE: &str = "deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin";
const EXPECTED_FIXTURE_FINGERPRINTS: [(&str, &str); 5] = [
    ("provider_few_larger_files", "fnv1a64:a3f6509701b2a6fc"),
    ("provider_many_small_files", "fnv1a64:05a1a9efa301e8be"),
    ("provider_many_unequal_files", "fnv1a64:e29235befe1d61e3"),
    (
        "provider_few_larger_files_sparse_dv",
        "fnv1a64:e1509da31486f25a",
    ),
    ("provider_4096_tiny_files", "fnv1a64:25e64f77ded44bc9"),
];

const CSV_HEADER: [&str; 96] = [
    "benchmark_schema_version",
    "benchmark_mode",
    "host_os",
    "host_arch",
    "host_available_parallelism",
    "seed",
    "workload_case_count",
    "workload_case",
    "provider_exec_storage_profile",
    "query_case",
    "parquet_backend",
    "scan_metadata_mode",
    "parquet_metadata_mode",
    "scheduling_mode",
    "scan_target_partitions",
    "max_concurrent_file_reads_per_scan",
    "max_concurrent_file_reads_per_partition",
    "output_buffer_batches_per_partition",
    "prefetch_files_per_partition",
    "repetitions",
    "queries_per_table",
    "file_count",
    "row_count",
    "data_file_bytes",
    "deletion_vector_file_count",
    "deletion_vector_deleted_rows",
    "deletion_vector_deleted_rows_per_file",
    "provider_stats_scan_count",
    "provider_stats_scan_partitions_planned",
    "provider_stats_files_planned",
    "provider_stats_estimated_input_rows",
    "provider_stats_estimated_input_bytes",
    "provider_stats_scan_partitions_started_p50",
    "provider_stats_scan_partitions_completed_p50",
    "provider_stats_file_tasks_started_p50",
    "provider_stats_file_tasks_completed_p50",
    "provider_stats_dynamic_partition_tasks_pruned_p50",
    "provider_stats_dynamic_partition_tasks_kept_p50",
    "provider_stats_dynamic_filters_received_p50",
    "provider_stats_dynamic_filters_accepted_p50",
    "provider_stats_dynamic_filters_rejected_p50",
    "provider_stats_dynamic_partition_filter_checks_p50",
    "provider_stats_dynamic_partition_tasks_kept_unusable_metadata_p50",
    "provider_stats_dynamic_partition_tasks_kept_unevaluable_filter_p50",
    "provider_stats_scheduler_batches_emitted_p50",
    "provider_stats_scheduler_rows_emitted_p50",
    "provider_stats_deletion_vector_payloads_loaded_p50",
    "provider_stats_deletion_vectors_applied_p50",
    "provider_stats_deletion_vector_rows_deleted_p50",
    "provider_stats_deletion_vector_failures_p50",
    "provider_stats_deletion_vector_coordinate_rejections_p50",
    "produced_rows",
    "produced_batches",
    "process_peak_rss_bytes",
    "process_peak_rss_delta_bytes",
    "table_initialization_micros_p50",
    "table_initialization_micros_p95",
    "table_initialization_micros_p99",
    "parquet_metadata_preparation_micros_p50",
    "parquet_metadata_preparation_micros_p95",
    "parquet_metadata_preparation_micros_p99",
    "prepared_parquet_metadata_file_count_p50",
    "prepared_parquet_metadata_memory_bytes_p50",
    "parquet_metadata_preparation_range_get_operations_p50",
    "parquet_metadata_preparation_full_get_operations_p50",
    "parquet_metadata_preparation_bytes_received_p50",
    "session_total_micros_p50",
    "session_total_micros_p95",
    "session_total_micros_p99",
    "planning_micros_p50",
    "planning_micros_p95",
    "planning_micros_p99",
    "time_to_first_batch_micros_p50",
    "time_to_first_batch_micros_p95",
    "time_to_first_batch_micros_p99",
    "total_micros_p50",
    "total_micros_p95",
    "total_micros_p99",
    "source_rows_per_second_p50",
    "source_rows_per_second_p95",
    "source_rows_per_second_p99",
    "batch_latency_micros_p50",
    "batch_latency_micros_p95",
    "batch_latency_micros_p99",
    "min_total_micros",
    "max_total_micros",
    "execution_profile_operator_count_max",
    "execution_profile_metric_count_max",
    "execution_profile_mode",
    "parquet_metadata_size_hint_bytes",
    "parquet_full_file_read_threshold_bytes",
    "provider_stats_parquet_data_file_range_get_operations_p50",
    "provider_stats_parquet_data_file_full_get_operations_p50",
    "provider_stats_parquet_data_file_bytes_received_p50",
    "provider_stats_estimated_parquet_task_bytes_admitted_p50",
    "fixture_fingerprint",
];

#[derive(Debug)]
struct Config {
    output: Option<PathBuf>,
    temp_dir: PathBuf,
    storage: StorageProfile,
    workload: Workload,
    query: Query,
    backend: ParquetReaderBackend,
    scan_metadata_mode: ScanMetadataMode,
    parquet_metadata_mode: ParquetMetadataMode,
    queries_per_table: usize,
    repetitions: usize,
    metadata_size_hint_bytes: Option<usize>,
    full_file_read_threshold_bytes: Option<usize>,
    retain_fixture: bool,
    seed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScanMetadataMode {
    Lazy,
    Eager,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParquetMetadataMode {
    OnDemand,
    Prepared,
}

#[derive(Debug, Clone, Copy)]
struct StorageProfile {
    name: &'static str,
    open_latency_micros: u64,
    read_latency_micros: u64,
    bandwidth_bytes_per_second: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
enum Workload {
    FewLarger,
    ManySmall,
    ManyUnequal,
    FewLargerSparseDv,
    FourThousandTinyFiles,
}

#[derive(Debug, Clone, Copy)]
enum Query {
    FullRows,
    ProjectId,
    FilterTailIds,
}

#[derive(Debug, Clone)]
struct FileSpec {
    path: String,
    rows: usize,
}

#[derive(Debug)]
struct Fixture {
    path: PathBuf,
    table_uri: String,
    storage_options: DeltaStorageOptions,
    server: Option<DelayedHttpServer>,
    file_count: usize,
    row_count: usize,
    data_file_bytes: u64,
    fingerprint: String,
    deletion_vector_file_count: usize,
    deletion_vector_deleted_rows: usize,
    deletion_vector_deleted_rows_per_file: usize,
    retain: bool,
}

#[derive(Debug)]
struct DeletionVectorFixture {
    descriptor: DeletionVectorDescriptor,
    bytes: Vec<u8>,
}

#[derive(Debug)]
struct DelayedHttpServer {
    shutdown: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
    url: String,
}

#[derive(Debug)]
struct QueryMeasurement {
    planning_micros: u64,
    first_batch_micros: u64,
    total_micros: u64,
    source_rows_per_second: u64,
    produced_rows: usize,
    produced_batches: usize,
    process_peak_rss_bytes: Option<u64>,
    process_peak_rss_delta_bytes: Option<u64>,
    batch_latency_micros: Vec<u64>,
    metrics: Vec<ScanMetricsSnapshot>,
}

#[derive(Debug)]
struct RepetitionMeasurement {
    table_initialization_micros: u64,
    parquet_metadata_preparation: Option<ParquetMetadataPreparationMeasurement>,
    session_total_micros: u64,
    #[cfg(test)]
    #[allow(dead_code)]
    table_load_count: usize,
    #[cfg(test)]
    #[allow(dead_code)]
    table_registration_count: usize,
    query_measurements: Vec<QueryMeasurement>,
}

#[derive(Debug)]
struct ParquetMetadataPreparationMeasurement {
    micros: u64,
    prepared: ParquetMetadataPreparationReport,
}

#[derive(Debug, Clone, Copy)]
struct Percentiles {
    p50: u64,
    p95: u64,
    p99: u64,
}

#[derive(Debug)]
struct Summary {
    repetitions: usize,
    produced_rows: usize,
    produced_batches: usize,
    planning: Percentiles,
    first_batch: Percentiles,
    total: Percentiles,
    rows_per_second: Percentiles,
    batch_latency: Percentiles,
    min_total: u64,
    max_total: u64,
    peak_rss: Option<u64>,
    peak_rss_delta: Option<u64>,
    table_initialization: Percentiles,
    parquet_metadata_preparation: ParquetMetadataPreparationSummary,
    session_total: Percentiles,
    read: ReadSummary,
}

#[derive(Debug)]
struct ParquetMetadataPreparationSummary {
    micros: Option<Percentiles>,
    file_count: Option<u64>,
    memory_bytes: Option<u64>,
    range_gets: Option<u64>,
    full_gets: Option<u64>,
    bytes_received: Option<u64>,
}

#[derive(Debug)]
struct ReadSummary {
    scan_count: usize,
    scan_partitions_planned: u64,
    files_planned: u64,
    estimated_input_rows: Option<u64>,
    estimated_input_bytes: Option<u64>,
    scan_partitions_started: u64,
    scan_partitions_completed: u64,
    file_tasks_started: u64,
    file_tasks_completed: u64,
    dynamic_partition_tasks_pruned: u64,
    dynamic_partition_tasks_kept: u64,
    dynamic_filters_received: u64,
    dynamic_filters_accepted: u64,
    dynamic_filters_rejected: u64,
    dynamic_partition_filter_checks: u64,
    dynamic_partition_tasks_kept_unusable_metadata: u64,
    dynamic_partition_tasks_kept_unevaluable_filter: u64,
    scheduler_batches_emitted: u64,
    scheduler_rows_emitted: u64,
    deletion_vector_payloads_loaded: u64,
    deletion_vectors_applied: u64,
    deletion_vector_rows_deleted: u64,
    deletion_vector_failures: u64,
    deletion_vector_coordinate_rejections: u64,
    range_gets: Option<u64>,
    full_gets: Option<u64>,
    bytes_received: Option<u64>,
    estimated_task_bytes_admitted: Option<u64>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::parse(env::args().skip(1))?;
    let mut output: Box<dyn Write> = match &config.output {
        Some(path) => {
            if let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                fs::create_dir_all(parent)?;
            }
            Box::new(File::create(path)?)
        }
        None => Box::new(io::stdout().lock()),
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run(&config, &mut output))
}

impl Config {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, Box<dyn Error>> {
        let mut config = Self {
            output: None,
            temp_dir: env::temp_dir(),
            storage: StorageProfile::local(),
            workload: Workload::FewLarger,
            query: Query::FullRows,
            backend: ParquetReaderBackend::Direct,
            scan_metadata_mode: ScanMetadataMode::Lazy,
            parquet_metadata_mode: ParquetMetadataMode::OnDemand,
            queries_per_table: DEFAULT_QUERIES_PER_TABLE,
            repetitions: DEFAULT_REPETITIONS,
            metadata_size_hint_bytes: Some(65_536),
            full_file_read_threshold_bytes: None,
            retain_fixture: false,
            seed: 0,
        };
        let mut args = args.into_iter();
        while let Some(argument) = args.next() {
            match argument.as_str() {
                "--mode" => {
                    if required_arg(&mut args, &argument)?.replace('-', "_") != "provider_exec" {
                        return Err(invalid("only provider-exec mode is portable").into());
                    }
                }
                "--seed" => config.seed = required_arg(&mut args, &argument)?.parse()?,
                "--output" => config.output = Some(required_arg(&mut args, &argument)?.into()),
                "--provider-exec-temp-dir" => {
                    config.temp_dir = required_arg(&mut args, &argument)?.into()
                }
                "--provider-exec-storage-profile" => {
                    config.storage = StorageProfile::parse(&required_arg(&mut args, &argument)?)?
                }
                "--provider-exec-workload" => {
                    config.workload = Workload::parse(&required_arg(&mut args, &argument)?)?
                }
                "--provider-exec-query" => {
                    config.query = Query::parse(&required_arg(&mut args, &argument)?)?
                }
                "--provider-exec-backend" => {
                    config.backend = match required_arg(&mut args, &argument)?
                        .replace('-', "_")
                        .as_str()
                    {
                        "direct_parquet" => ParquetReaderBackend::Direct,
                        "delta_kernel" => ParquetReaderBackend::DeltaKernel,
                        other => return Err(invalid(format!("unknown backend: {other}")).into()),
                    }
                }
                "--provider-exec-scan-metadata-mode" => {
                    config.scan_metadata_mode =
                        ScanMetadataMode::parse(&required_arg(&mut args, &argument)?)?
                }
                "--provider-exec-parquet-metadata-mode" => {
                    config.parquet_metadata_mode =
                        ParquetMetadataMode::parse(&required_arg(&mut args, &argument)?)?
                }
                "--provider-exec-queries-per-table" => {
                    config.queries_per_table = required_arg(&mut args, &argument)?.parse()?;
                    if !(1..=MAX_QUERIES_PER_TABLE).contains(&config.queries_per_table) {
                        return Err(invalid("queries per table must be between 1 and 128").into());
                    }
                }
                "--provider-exec-scheduling-profile" => {
                    let profile = required_arg(&mut args, &argument)?.replace('-', "_");
                    if profile != "prefetch_2_ap_target_scan_3x" {
                        return Err(invalid(format!(
                            "unsupported frozen scheduling profile: {profile}"
                        ))
                        .into());
                    }
                }
                "--provider-exec-parquet-metadata-size-hint-bytes" => {
                    config.metadata_size_hint_bytes =
                        parse_optional_positive(&required_arg(&mut args, &argument)?)?
                }
                "--provider-exec-parquet-full-file-read-threshold-bytes" => {
                    config.full_file_read_threshold_bytes =
                        parse_optional_positive(&required_arg(&mut args, &argument)?)?
                }
                "--provider-exec-repetitions" => {
                    config.repetitions = required_arg(&mut args, &argument)?.parse()?;
                    if !(1..=MAX_REPETITIONS).contains(&config.repetitions) {
                        return Err(invalid("repetitions must be between 1 and 128").into());
                    }
                }
                "--provider-exec-retain-fixtures" => config.retain_fixture = true,
                "--bench" => {}
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                other => return Err(invalid(format!("unknown argument: {other}")).into()),
            }
        }
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.parquet_metadata_mode == ParquetMetadataMode::Prepared
            && (self.backend != ParquetReaderBackend::Direct
                || self.scan_metadata_mode != ScanMetadataMode::Eager)
        {
            return Err(invalid(
                "prepared Parquet metadata requires the direct backend and eager scan metadata",
            )
            .into());
        }
        if matches!(self.workload, Workload::FourThousandTinyFiles)
            && self.scan_metadata_mode != ScanMetadataMode::Eager
        {
            return Err(invalid("the 4,096-file workload requires eager scan metadata").into());
        }
        let case = (
            self.workload,
            self.query,
            self.backend,
            self.storage.name,
            self.metadata_size_hint_bytes,
            self.full_file_read_threshold_bytes,
        );
        let frozen = matches!(
            case,
            (
                Workload::FewLarger,
                Query::FullRows,
                ParquetReaderBackend::Direct | ParquetReaderBackend::DeltaKernel,
                "local",
                Some(65_536),
                None,
            ) | (
                Workload::FewLarger,
                Query::ProjectId,
                ParquetReaderBackend::Direct,
                "local",
                Some(65_536),
                None,
            ) | (
                Workload::ManyUnequal,
                Query::FilterTailIds,
                ParquetReaderBackend::Direct,
                "local" | "s3_throttled",
                Some(65_536),
                None,
            ) | (
                Workload::ManySmall,
                Query::ProjectId,
                ParquetReaderBackend::Direct,
                "local",
                Some(65_536),
                None,
            ) | (
                Workload::FewLarger,
                Query::FullRows,
                ParquetReaderBackend::Direct,
                "local",
                None | Some(8),
                None,
            ) | (
                Workload::FewLarger,
                Query::FullRows,
                ParquetReaderBackend::Direct,
                "local",
                Some(65_536),
                Some(1_000 | 1_000_000),
            ) | (
                Workload::FewLargerSparseDv,
                Query::ProjectId,
                ParquetReaderBackend::Direct | ParquetReaderBackend::DeltaKernel,
                "local",
                Some(65_536),
                None,
            ) | (
                Workload::FewLarger,
                Query::FullRows,
                ParquetReaderBackend::Direct,
                "s3_throttled",
                Some(65_536),
                None,
            ) | (
                Workload::FourThousandTinyFiles,
                Query::ProjectId,
                ParquetReaderBackend::Direct,
                "local" | "s3_throttled",
                Some(65_536),
                None,
            )
        );
        if self.seed != 0 || !frozen {
            return Err(invalid("configuration is not one of the 15 frozen cases").into());
        }
        Ok(())
    }
}

fn required_arg(
    args: &mut impl Iterator<Item = String>,
    argument: &str,
) -> Result<String, io::Error> {
    args.next()
        .ok_or_else(|| invalid(format!("missing value for {argument}")))
}

impl ScanMetadataMode {
    fn parse(value: &str) -> Result<Self, io::Error> {
        match value {
            "lazy" => Ok(Self::Lazy),
            "eager" => Ok(Self::Eager),
            other => Err(invalid(format!("unknown scan metadata mode: {other}"))),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Lazy => "lazy",
            Self::Eager => "eager",
        }
    }
}

impl ParquetMetadataMode {
    fn parse(value: &str) -> Result<Self, io::Error> {
        match value.replace('-', "_").as_str() {
            "on_demand" => Ok(Self::OnDemand),
            "prepared" => Ok(Self::Prepared),
            other => Err(invalid(format!("unknown Parquet metadata mode: {other}"))),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::OnDemand => "on_demand",
            Self::Prepared => "prepared",
        }
    }
}

impl StorageProfile {
    const fn local() -> Self {
        Self {
            name: "local",
            open_latency_micros: 0,
            read_latency_micros: 0,
            bandwidth_bytes_per_second: None,
        }
    }

    const fn throttled() -> Self {
        Self {
            name: "s3_throttled",
            open_latency_micros: 15_000,
            read_latency_micros: 8_000,
            bandwidth_bytes_per_second: Some(32 * MIB),
        }
    }

    fn parse(value: &str) -> Result<Self, io::Error> {
        match value.replace('-', "_").as_str() {
            "local" => Ok(Self::local()),
            "s3_throttled" => Ok(Self::throttled()),
            other => Err(invalid(format!("unknown storage profile: {other}"))),
        }
    }

    fn uses_http(self) -> bool {
        self.name != "local"
    }
}

impl Workload {
    fn parse(value: &str) -> Result<Self, io::Error> {
        match value {
            "provider_few_larger_files" => Ok(Self::FewLarger),
            "provider_many_small_files" => Ok(Self::ManySmall),
            "provider_many_unequal_files" => Ok(Self::ManyUnequal),
            "provider_few_larger_files_sparse_dv" => Ok(Self::FewLargerSparseDv),
            "provider_4096_tiny_files" => Ok(Self::FourThousandTinyFiles),
            other => Err(invalid(format!("unknown frozen workload: {other}"))),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::FewLarger => "provider_few_larger_files",
            Self::ManySmall => "provider_many_small_files",
            Self::ManyUnequal => "provider_many_unequal_files",
            Self::FewLargerSparseDv => "provider_few_larger_files_sparse_dv",
            Self::FourThousandTinyFiles => "provider_4096_tiny_files",
        }
    }

    fn files(self) -> Vec<FileSpec> {
        let rows = match self {
            Self::FewLarger | Self::FewLargerSparseDv => vec![8_192; 4],
            Self::ManySmall => vec![128; 64],
            Self::ManyUnequal => (0..64)
                .map(|index| {
                    if index >= 32 && index % 4 == 0 {
                        8_192
                    } else {
                        128
                    }
                })
                .collect(),
            Self::FourThousandTinyFiles => vec![1; 4_096],
        };
        rows.into_iter()
            .enumerate()
            .map(|(index, rows)| FileSpec {
                path: format!("part-{index:05}.parquet"),
                rows,
            })
            .collect()
    }

    const fn deleted_rows(self) -> &'static [u64] {
        match self {
            Self::FewLargerSparseDv => &[1, 4_096, 8_191],
            Self::FewLarger | Self::ManySmall | Self::ManyUnequal | Self::FourThousandTinyFiles => {
                &[]
            }
        }
    }
}

impl Query {
    fn parse(value: &str) -> Result<Self, io::Error> {
        match value {
            "full_rows" => Ok(Self::FullRows),
            "project_id" => Ok(Self::ProjectId),
            "filter_tail_ids" => Ok(Self::FilterTailIds),
            other => Err(invalid(format!("unknown frozen query: {other}"))),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::FullRows => "full_rows",
            Self::ProjectId => "project_id",
            Self::FilterTailIds => "filter_tail_ids",
        }
    }

    const fn sql(self) -> &'static str {
        match self {
            Self::FullRows => "select * from orders",
            Self::ProjectId => "select id from orders",
            Self::FilterTailIds => "select id from orders where id > 4096",
        }
    }

    const fn expected_columns(self) -> &'static [(&'static str, DataType)] {
        match self {
            Self::FullRows => &[
                ("id", DataType::Int32),
                ("customer_name", DataType::Utf8View),
            ],
            Self::ProjectId | Self::FilterTailIds => &[("id", DataType::Int32)],
        }
    }
}

fn parse_optional_positive(value: &str) -> Result<Option<usize>, Box<dyn Error>> {
    if value == "disabled" {
        return Ok(None);
    }
    let value = value.parse::<usize>()?;
    if value == 0 {
        return Err(invalid("Parquet byte options must be positive or disabled").into());
    }
    Ok(Some(value))
}

fn print_help() {
    println!("delta-arrow-reader frozen provider-exec benchmark; use the #470 comparison commands");
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

async fn run(config: &Config, output: &mut dyn Write) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::create(
        &config.temp_dir,
        config.workload,
        config.storage,
        config.retain_fixture,
    )?;
    if config.retain_fixture {
        eprintln!("retained provider-exec fixture: {}", fixture.path.display());
    }
    let available_parallelism = thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .ok();
    let target_partitions = available_parallelism.unwrap_or(4).max(1);
    let repetitions = benchmark(config, &fixture, target_partitions).await?;
    writeln!(output, "{}", CSV_HEADER.join(","))?;
    writeln!(
        output,
        "{}",
        csv_row(
            config,
            &fixture,
            available_parallelism,
            target_partitions,
            &repetitions,
        )
        .join(",")
    )?;
    Ok(())
}

impl Fixture {
    fn create(
        temp_root: &Path,
        workload: Workload,
        storage: StorageProfile,
        retain: bool,
    ) -> Result<Self, Box<dyn Error>> {
        let path = temp_root.join(format!(
            "{}-delta-arrow-reader-{}-{}",
            std::process::id(),
            workload.name(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        let log_path = path.join("_delta_log");
        fs::create_dir_all(&log_path)?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("customer_name", DataType::Utf8, true),
        ]));
        let files = workload.files();
        let deleted_rows = workload.deleted_rows();
        let deletion_vector = (!deleted_rows.is_empty())
            .then(|| deletion_vector_fixture(deleted_rows))
            .transpose()?;
        let mut next_row_id = 1_usize;
        let mut data_file_bytes = 0_u64;
        let mut add_actions = Vec::with_capacity(files.len());

        for file in &files {
            if deleted_rows
                .last()
                .is_some_and(|index| *index >= u64::try_from(file.rows).unwrap_or(u64::MAX))
            {
                return Err(invalid("deletion-vector index exceeds file row count").into());
            }
            let first_row_id = next_row_id;
            next_row_id = next_row_id.saturating_add(file.rows);
            let batch = simple_orders_batch(Arc::clone(&schema), first_row_id, file.rows)?;
            let properties = WriterProperties::builder()
                .set_created_by(FROZEN_PARQUET_CREATED_BY.to_owned())
                .set_max_row_group_row_count(Some(file.rows))
                .build();
            let data_path = path.join(&file.path);
            let mut writer = ArrowWriter::try_new(
                File::create(&data_path)?,
                Arc::clone(&schema),
                Some(properties),
            )?;
            writer.write(&batch)?;
            writer.close()?;
            let size = fs::metadata(&data_path)?.len();
            data_file_bytes = data_file_bytes.saturating_add(size);
            add_actions.push(add_json(
                file,
                size,
                first_row_id,
                deletion_vector.as_ref(),
            )?);
        }

        if let Some(deletion_vector) = &deletion_vector {
            fs::write(path.join(RELATIVE_DV_FILE), &deletion_vector.bytes)?;
        }
        let protocol = if deletion_vector.is_some() {
            DELETION_VECTOR_PROTOCOL_JSON
        } else {
            PROTOCOL_JSON
        };
        fs::write(
            log_path.join("00000000000000000000.json"),
            format!("{protocol}\n{}\n", metadata_json()),
        )?;
        fs::write(
            log_path.join("00000000000000000001.json"),
            format!("{}\n", add_actions.join("\n")),
        )?;
        let mut fixture_files = files
            .iter()
            .map(|file| file.path.clone())
            .collect::<Vec<_>>();
        fixture_files.extend([
            "_delta_log/00000000000000000000.json".to_owned(),
            "_delta_log/00000000000000000001.json".to_owned(),
        ]);
        if deletion_vector.is_some() {
            fixture_files.push(RELATIVE_DV_FILE.to_owned());
        }
        let fingerprint = fixture_fingerprint(&path, fixture_files)?;
        validate_fixture_fingerprint(workload, &fingerprint)?;
        let server = storage
            .uses_http()
            .then(|| DelayedHttpServer::start(path.clone(), storage))
            .transpose()?;
        let table_uri = server
            .as_ref()
            .map(|server| server.url.clone())
            .unwrap_or_else(|| path.to_string_lossy().into_owned());
        let storage_options = if server.is_some() {
            BTreeMap::from([("allow_http".to_owned(), "true".to_owned())])
        } else {
            DeltaStorageOptions::new()
        };
        let row_count = files.iter().map(|file| file.rows).sum();
        let deleted_per_file = deleted_rows.len();

        Ok(Self {
            path,
            table_uri,
            storage_options,
            server,
            file_count: files.len(),
            row_count,
            data_file_bytes,
            fingerprint,
            deletion_vector_file_count: usize::from(deletion_vector.is_some()) * files.len(),
            deletion_vector_deleted_rows: deleted_per_file * files.len(),
            deletion_vector_deleted_rows_per_file: deleted_per_file,
            retain,
        })
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.take();
        if !self.retain {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

fn simple_orders_batch(
    schema: Arc<Schema>,
    first_id: usize,
    rows: usize,
) -> Result<RecordBatch, Box<dyn Error>> {
    let first_id_i32 = i32::try_from(first_id)?;
    let row_count = i32::try_from(rows)?;
    let ids = (first_id_i32..first_id_i32 + row_count).collect::<Vec<_>>();
    let names = (0..rows)
        .map(|offset| Some(format!("customer-{}", first_id.saturating_add(offset))))
        .collect::<Vec<_>>();
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
        ],
    )?)
}

fn add_json(
    file: &FileSpec,
    size: u64,
    first_row_id: usize,
    deletion_vector: Option<&DeletionVectorFixture>,
) -> Result<String, Box<dyn Error>> {
    let max_id = first_row_id.saturating_add(file.rows).saturating_sub(1);
    let stats = format!(
        r#"{{"numRecords":{},"minValues":{{"id":{},"customer_name":"customer-{}"}},"maxValues":{{"id":{},"customer_name":"customer-{}"}},"nullCount":{{"id":0,"customer_name":0}}}}"#,
        file.rows, first_row_id, first_row_id, max_id, max_id
    );
    let deletion_vector = deletion_vector.map_or_else(String::new, |fixture| {
        let descriptor = &fixture.descriptor;
        format!(
            r#","deletionVector":{{"storageType":"{}","pathOrInlineDv":"{}","offset":{},"sizeInBytes":{},"cardinality":{}}}"#,
            descriptor.storage_type,
            descriptor.path_or_inline_dv,
            descriptor.offset.unwrap_or(0),
            descriptor.size_in_bytes,
            descriptor.cardinality,
        )
    });
    Ok(format!(
        r#"{{"add":{{"path":"{}","partitionValues":{{}},"size":{size},"modificationTime":{MODIFICATION_TIME_MS},"dataChange":true,"stats":"{}"{deletion_vector}}}}}"#,
        file.path,
        json_escape(&stats),
    ))
}

fn metadata_json() -> String {
    let schema = r#"{"type":"struct","fields":[{"name":"id","type":"integer","nullable":false,"metadata":{}},{"name":"customer_name","type":"string","nullable":true,"metadata":{}}]}"#;
    format!(
        r#"{{"metaData":{{"id":"delta-funnel-provider-exec-benchmark","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{}","partitionColumns":[],"configuration":{{}},"createdTime":1587968585495}}}}"#,
        json_escape(schema)
    )
}

fn deletion_vector_fixture(deleted_rows: &[u64]) -> Result<DeletionVectorFixture, Box<dyn Error>> {
    let mut bytes = Vec::new();
    let mut writer = StreamingDeletionVectorWriter::new(&mut bytes);
    let mut deletion_vector = KernelDeletionVector::new();
    deletion_vector.add_deleted_row_indexes(deleted_rows.iter().copied());
    let result = writer.write_deletion_vector(deletion_vector)?;
    writer.finalize()?;
    Ok(DeletionVectorFixture {
        descriptor: DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: RELATIVE_DV_ID.to_owned(),
            offset: Some(result.offset),
            size_in_bytes: result.size_in_bytes,
            cardinality: result.cardinality,
        },
        bytes,
    })
}

fn json_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

fn fixture_fingerprint(root: &Path, mut paths: Vec<String>) -> io::Result<String> {
    paths.sort_unstable();
    let mut hash = 14_695_981_039_346_656_037_u64;
    let mut buffer = [0_u8; 8 * 1024];
    for relative_path in paths {
        let path = relative_path.as_bytes();
        fnv1a64_update(
            &mut hash,
            &u64::try_from(path.len())
                .map_err(io::Error::other)?
                .to_le_bytes(),
        );
        fnv1a64_update(&mut hash, path);
        let mut file = File::open(root.join(&relative_path))?;
        fnv1a64_update(&mut hash, &file.metadata()?.len().to_le_bytes());
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            fnv1a64_update(&mut hash, &buffer[..read]);
        }
    }
    Ok(format!("fnv1a64:{hash:016x}"))
}

fn validate_fixture_fingerprint(workload: Workload, fingerprint: &str) -> io::Result<()> {
    let expected = EXPECTED_FIXTURE_FINGERPRINTS
        .iter()
        .find_map(|(name, expected)| (*name == workload.name()).then_some(*expected))
        .ok_or_else(|| invalid("frozen workload is missing its fingerprint"))?;
    if fingerprint != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "fixture {} drifted: expected {expected}, got {fingerprint}",
                workload.name()
            ),
        ));
    }
    Ok(())
}

fn fnv1a64_update(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(1_099_511_628_211);
    }
}

impl DelayedHttpServer {
    fn start(root: PathBuf, profile: StorageProfile) -> Result<Self, Box<dyn Error>> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let url = format!("http://{}/", listener.local_addr()?);
        let shutdown = Arc::new(AtomicBool::new(false));
        let worker_shutdown = Arc::clone(&shutdown);
        let handle = thread::spawn(move || serve_http(listener, root, profile, worker_shutdown));
        Ok(Self {
            shutdown,
            handle: Some(handle),
            url,
        })
    }
}

impl Drop for DelayedHttpServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn serve_http(
    listener: TcpListener,
    root: PathBuf,
    profile: StorageProfile,
    shutdown: Arc<AtomicBool>,
) {
    while !shutdown.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _)) => {
                let root = root.clone();
                let shutdown = Arc::clone(&shutdown);
                let _ = thread::Builder::new()
                    .name("delta-arrow-reader-bench-http".to_owned())
                    .spawn(move || {
                        let _ = handle_http(stream, &root, profile, &shutdown);
                    });
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(_) => break,
        }
    }
}

fn handle_http(
    mut stream: TcpStream,
    root: &Path,
    profile: StorageProfile,
    shutdown: &AtomicBool,
) -> io::Result<()> {
    let Some(request) = read_http_request(&stream)? else {
        return Ok(());
    };
    if shutdown.load(Ordering::Relaxed) {
        let _ = stream.shutdown(Shutdown::Both);
        return Ok(());
    }
    delayed_sleep(profile.open_latency_micros);
    match request.method.as_str() {
        "PROPFIND" => propfind(&mut stream, root, &request),
        "HEAD" => file_response(
            &mut stream,
            root,
            &request.path,
            request.headers.get("range").map(String::as_str),
            true,
            profile,
        ),
        "GET" => file_response(
            &mut stream,
            root,
            &request.path,
            request.headers.get("range").map(String::as_str),
            false,
            profile,
        ),
        _ => write_response(
            stream,
            405,
            "Method Not Allowed",
            &[("Content-Length", "0".to_owned())],
            &[],
        ),
    }
}

struct HttpRequest {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
}

fn read_http_request(stream: &TcpStream) -> io::Result<Option<HttpRequest>> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    if reader.read_line(&mut request_line)? == 0 {
        return Ok(None);
    }
    let mut parts = request_line.split_whitespace();
    let (Some(method), Some(target)) = (parts.next(), parts.next()) else {
        return Ok(None);
    };
    let mut headers = BTreeMap::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            break;
        }
        if let Some((key, value)) = line.split_once(':') {
            headers.insert(key.trim().to_ascii_lowercase(), value.trim().to_owned());
        }
    }
    let path = target.split('?').next().unwrap_or_default();
    let path = percent_decode(path.trim_start_matches('/'))?;
    if path
        .split('/')
        .any(|component| component == ".." || component.contains('\\'))
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "invalid delayed HTTP path",
        ));
    }
    Ok(Some(HttpRequest {
        method: method.to_owned(),
        path,
        headers,
    }))
}

fn propfind(stream: &mut TcpStream, root: &Path, request: &HttpRequest) -> io::Result<()> {
    let requested = root.join(&request.path);
    if !requested.exists() {
        return write_response(
            stream,
            404,
            "Not Found",
            &[("Content-Length", "0".to_owned())],
            &[],
        );
    }
    let recursive = request.headers.get("depth").map(String::as_str) != Some("0");
    let mut entries = Vec::new();
    collect_entries(root, &request.path, &requested, recursive, &mut entries)?;
    let body = multistatus_xml(&entries)?;
    write_response(
        stream,
        207,
        "Multi-Status",
        &[
            ("Content-Type", "application/xml; charset=utf-8".to_owned()),
            ("Content-Length", body.len().to_string()),
        ],
        body.as_bytes(),
    )
}

struct HttpEntry {
    href: String,
    size: u64,
    is_dir: bool,
    modified: SystemTime,
}

fn collect_entries(
    root: &Path,
    relative: &str,
    path: &Path,
    recursive: bool,
    entries: &mut Vec<HttpEntry>,
) -> io::Result<()> {
    let metadata = fs::metadata(path)?;
    entries.push(HttpEntry {
        href: format!("/{}", relative.trim_start_matches('/')),
        size: if metadata.is_file() {
            metadata.len()
        } else {
            0
        },
        is_dir: metadata.is_dir(),
        modified: metadata.modified().unwrap_or(UNIX_EPOCH),
    });
    if metadata.is_dir() && recursive {
        let mut children = fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
        children.sort_by_key(std::fs::DirEntry::path);
        for child in children {
            let child_path = child.path();
            let child_relative = child_path
                .strip_prefix(root)
                .map_err(|_| io::Error::other("delayed HTTP path escaped root"))?
                .to_string_lossy()
                .replace('\\', "/");
            collect_entries(root, &child_relative, &child_path, true, entries)?;
        }
    }
    Ok(())
}

fn multistatus_xml(entries: &[HttpEntry]) -> io::Result<String> {
    let mut xml = String::from(r#"<?xml version="1.0" encoding="utf-8"?><multistatus>"#);
    for entry in entries {
        let resource_type = if entry.is_dir {
            "<resourcetype><collection/></resourcetype>"
        } else {
            "<resourcetype/>"
        };
        xml.push_str(&format!(
            "<response><href>{}</href><propstat><prop><getlastmodified>{}</getlastmodified><getcontentlength>{}</getcontentlength>{resource_type}<getetag>\"{}\"</getetag></prop><status>HTTP/1.1 200 OK</status></propstat></response>",
            xml_escape(&entry.href),
            http_date(entry.modified),
            entry.size,
            etag(entry.size, entry.modified)?,
        ));
    }
    xml.push_str("</multistatus>");
    Ok(xml)
}

fn file_response(
    stream: &mut TcpStream,
    root: &Path,
    request_path: &str,
    range_header: Option<&str>,
    head_only: bool,
    profile: StorageProfile,
) -> io::Result<()> {
    let path = root.join(request_path);
    let metadata = match fs::metadata(&path) {
        Ok(metadata) if metadata.is_file() => metadata,
        _ => {
            return write_response(
                stream,
                404,
                "Not Found",
                &[("Content-Length", "0".to_owned())],
                &[],
            );
        }
    };
    let size = metadata.len();
    let modified = metadata.modified().unwrap_or(UNIX_EPOCH);
    let range = parse_range(range_header, size)?;
    let (status, text, start, end) = match range {
        Some((start, end)) => (206, "Partial Content", start, end),
        None => (200, "OK", 0, size),
    };
    let content_len = end.saturating_sub(start);
    let mut headers = vec![
        ("Accept-Ranges", "bytes".to_owned()),
        ("Content-Length", content_len.to_string()),
        ("Last-Modified", http_date(modified)),
        ("ETag", format!("\"{}\"", etag(size, modified)?)),
    ];
    if status == 206 {
        headers.push((
            "Content-Range",
            format!("bytes {start}-{}/{}", end.saturating_sub(1), size),
        ));
    }
    delayed_sleep(profile.read_latency_micros);
    delayed_sleep(transfer_delay(
        content_len,
        profile.bandwidth_bytes_per_second,
    ));
    if head_only {
        return write_response(stream, status, text, &headers, &[]);
    }
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(start))?;
    let mut body = Vec::new();
    file.take(content_len).read_to_end(&mut body)?;
    write_response(stream, status, text, &headers, &body)
}

fn parse_range(header: Option<&str>, size: u64) -> io::Result<Option<(u64, u64)>> {
    let Some(range) = header.and_then(|value| value.strip_prefix("bytes=")) else {
        return Ok(None);
    };
    if let Some(suffix) = range.strip_prefix('-') {
        let suffix = suffix
            .parse::<u64>()
            .map_err(|_| invalid("invalid HTTP suffix range"))?;
        return Ok(Some((size.saturating_sub(suffix), size)));
    }
    let (start, end) = range.split_once('-').unwrap_or((range, ""));
    let start = start
        .parse::<u64>()
        .map_err(|_| invalid("invalid HTTP range start"))?;
    let end = if end.is_empty() {
        size
    } else {
        end.parse::<u64>()
            .map_err(|_| invalid("invalid HTTP range end"))?
            .saturating_add(1)
            .min(size)
    };
    if start > end || end > size {
        return Err(invalid("invalid HTTP range bounds"));
    }
    Ok(Some((start, end)))
}

fn write_response(
    mut stream: impl Write,
    status: u16,
    text: &str,
    headers: &[(&str, String)],
    body: &[u8],
) -> io::Result<()> {
    write!(stream, "HTTP/1.1 {status} {text}\r\nConnection: close\r\n")?;
    for (key, value) in headers {
        write!(stream, "{key}: {value}\r\n")?;
    }
    write!(stream, "\r\n")?;
    stream.write_all(body)
}

fn delayed_sleep(micros: u64) {
    if micros != 0 {
        thread::sleep(Duration::from_micros(micros));
    }
}

fn transfer_delay(bytes: u64, bandwidth: Option<u64>) -> u64 {
    bandwidth
        .filter(|bandwidth| *bandwidth != 0)
        .map(|bandwidth| saturating_u64(u128::from(bytes) * 1_000_000 / u128::from(bandwidth)))
        .unwrap_or(0)
}

fn http_date(time: SystemTime) -> String {
    let seconds = time
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    chrono::DateTime::from_timestamp(i64::try_from(seconds).unwrap_or(0), 0)
        .map(|date| date.format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .unwrap_or_else(|| "Thu, 01 Jan 1970 00:00:00 GMT".to_owned())
}

fn etag(size: u64, modified: SystemTime) -> io::Result<String> {
    let nanos = modified
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_nanos();
    Ok(format!("{size:x}-{nanos:x}"))
}

fn percent_decode(value: &str) -> io::Result<String> {
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(invalid("invalid percent encoding"));
            }
            output.push(hex(bytes[index + 1])? * 16 + hex(bytes[index + 2])?);
            index += 3;
        } else {
            output.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(output).map_err(|error| invalid(error.to_string()))
}

fn hex(byte: u8) -> io::Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(invalid("invalid hex digit")),
    }
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

async fn benchmark(
    config: &Config,
    fixture: &Fixture,
    target_partitions: usize,
) -> Result<Vec<RepetitionMeasurement>, Box<dyn Error>> {
    let mut repetitions = Vec::with_capacity(config.repetitions);
    for _ in 0..config.repetitions {
        repetitions.push(run_repetition(config, fixture, target_partitions).await?);
    }
    Ok(repetitions)
}

async fn run_repetition(
    config: &Config,
    fixture: &Fixture,
    target_partitions: usize,
) -> Result<RepetitionMeasurement, Box<dyn Error>> {
    let context = SessionContext::new();
    let execution_options = DeltaScanExecutionOptions::new()
        .with_parquet_backend(config.backend)
        .with_max_concurrent_file_reads_per_scan(Some(target_partitions.saturating_mul(3).max(1)))?
        .with_max_concurrent_file_reads_per_partition(3)?
        .with_output_buffer_batches_per_partition(1)?
        .with_prefetch_files_per_partition(2)
        .with_parquet_metadata_size_hint_bytes(config.metadata_size_hint_bytes)?
        .with_parquet_full_file_read_threshold_bytes(config.full_file_read_threshold_bytes)?;
    let builder = DeltaTableBuilder::new(&fixture.table_uri)
        .with_storage_options(fixture.storage_options.clone())
        .with_execution_options(execution_options);
    let scan_options = ScanOptions {
        execution_options,
        target_partitions: Some(target_partitions),
        intra_file_repartitioning: Default::default(),
        use_arrow_view_types: true,
    };
    let session_started = Instant::now();
    let table_initialization_started = session_started;
    let table = match (config.scan_metadata_mode, config.parquet_metadata_mode) {
        (ScanMetadataMode::Lazy, ParquetMetadataMode::OnDemand) => builder.load_table().await?,
        (ScanMetadataMode::Eager, ParquetMetadataMode::OnDemand) => {
            builder.load_table_with_eager_scan_metadata().await?
        }
        (ScanMetadataMode::Eager, ParquetMetadataMode::Prepared) => {
            builder
                .load_table_with_prepared_parquet_metadata(ParquetMetadataPreparationLimits {
                    max_files: fixture.file_count,
                    max_retained_metadata_bytes: usize::MAX,
                })
                .await?
        }
        (ScanMetadataMode::Lazy, ParquetMetadataMode::Prepared) => {
            return Err("prepared Parquet metadata requires eager scan metadata".into());
        }
    };
    #[cfg(test)]
    let table_load_count = 1;
    let table_initialization_micros =
        saturating_u64(table_initialization_started.elapsed().as_micros());
    let parquet_metadata_preparation =
        table
            .parquet_metadata_preparation_report()
            .cloned()
            .map(|prepared| ParquetMetadataPreparationMeasurement {
                micros: saturating_u64(prepared.preparation_duration.as_micros()),
                prepared,
            });
    register_table(&context, "orders", table, scan_options)?;
    #[cfg(test)]
    let table_registration_count = 1;

    let mut query_measurements = Vec::with_capacity(config.queries_per_table);
    for _ in 0..config.queries_per_table {
        query_measurements.push(measure_query(config, fixture, &context).await?);
    }
    let session_total_micros = saturating_u64(session_started.elapsed().as_micros());
    Ok(RepetitionMeasurement {
        table_initialization_micros,
        parquet_metadata_preparation,
        session_total_micros,
        #[cfg(test)]
        table_load_count,
        #[cfg(test)]
        table_registration_count,
        query_measurements,
    })
}

async fn measure_query(
    config: &Config,
    fixture: &Fixture,
    context: &SessionContext,
) -> Result<QueryMeasurement, Box<dyn Error>> {
    let query_started = Instant::now();
    let planning_started = Instant::now();
    let dataframe = context.sql(config.query.sql()).await?;
    let plan = dataframe.create_physical_plan().await?;
    let output_schema = plan.schema();
    let metrics_plan = Arc::clone(&plan);
    let planning_micros = saturating_u64(planning_started.elapsed().as_micros());
    let peak_before = process_peak_rss_bytes();
    let execution_started = Instant::now();
    let mut stream = datafusion::physical_plan::execute_stream(plan, context.task_ctx())?;
    let mut produced_rows = 0_usize;
    let mut produced_batches = 0_usize;
    let mut first_batch_micros = None;
    let mut previous_batch_at = execution_started;
    let mut batch_latency_micros = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let now = Instant::now();
        first_batch_micros.get_or_insert_with(|| {
            saturating_u64(now.duration_since(execution_started).as_micros())
        });
        batch_latency_micros.push(saturating_u64(
            now.duration_since(previous_batch_at).as_micros(),
        ));
        previous_batch_at = now;
        produced_rows = produced_rows.saturating_add(batch.num_rows());
        produced_batches = produced_batches.saturating_add(1);
    }
    let total_micros = saturating_u64(query_started.elapsed().as_micros()).max(1);
    validate_schema(config.query, output_schema.fields())?;
    validate_fixture_fingerprint(config.workload, &fixture.fingerprint)?;
    let expected_rows = expected_rows(config.workload, config.query, fixture);
    if produced_rows != expected_rows || produced_batches == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "query output drifted: expected {expected_rows} rows and at least one batch, got {produced_rows} rows in {produced_batches} batches"
            ),
        )
        .into());
    }
    let peak_rss = process_peak_rss_bytes();
    let metrics = collect_scan_metrics(metrics_plan.as_ref())
        .into_iter()
        .map(|metrics| metrics.snapshot())
        .collect::<Vec<_>>();
    if metrics.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "query plan exposed no Delta scan metrics",
        )
        .into());
    }
    validate_deletion_vector_metrics(fixture, &metrics)?;
    Ok(QueryMeasurement {
        planning_micros,
        first_batch_micros: first_batch_micros.unwrap_or(0),
        total_micros,
        source_rows_per_second: saturating_u64(
            (fixture.row_count as u128).saturating_mul(1_000_000) / u128::from(total_micros),
        ),
        produced_rows,
        produced_batches,
        process_peak_rss_bytes: peak_rss,
        process_peak_rss_delta_bytes: peak_before
            .zip(peak_rss)
            .map(|(before, after)| after.saturating_sub(before)),
        batch_latency_micros,
        metrics,
    })
}

fn expected_rows(workload: Workload, query: Query, fixture: &Fixture) -> usize {
    match query {
        Query::FilterTailIds => fixture.row_count.saturating_sub(4_096),
        Query::FullRows | Query::ProjectId => fixture.row_count.saturating_sub(
            workload
                .deleted_rows()
                .len()
                .saturating_mul(fixture.file_count),
        ),
    }
}

fn validate_deletion_vector_metrics(
    fixture: &Fixture,
    metrics: &[ScanMetricsSnapshot],
) -> Result<(), Box<dyn Error>> {
    let total = |select: fn(&delta_arrow_reader::DeltaScanMetricsSnapshot) -> u64| {
        metrics.iter().fold(0_u64, |sum, snapshot| {
            sum.saturating_add(select(&snapshot.reader_metrics))
        })
    };
    let expected = (
        u64::try_from(fixture.deletion_vector_file_count)?,
        u64::try_from(fixture.deletion_vector_file_count)?,
        u64::try_from(fixture.deletion_vector_deleted_rows)?,
        0,
        0,
    );
    let actual = (
        total(|reader| reader.deletion_vector_payloads_loaded),
        total(|reader| reader.deletion_vectors_applied),
        total(|reader| reader.deletion_vector_rows_deleted),
        total(|reader| reader.deletion_vector_failures),
        total(|reader| reader.deletion_vector_coordinate_rejections),
    );
    if actual != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("deletion-vector metrics drifted: expected {expected:?}, got {actual:?}"),
        )
        .into());
    }
    Ok(())
}

fn validate_schema(
    query: Query,
    fields: &datafusion::arrow::datatypes::Fields,
) -> Result<(), Box<dyn Error>> {
    let expected = query.expected_columns();
    let actual = fields
        .iter()
        .map(|field| (field.name().as_str(), field.data_type()))
        .collect::<Vec<_>>();
    if actual.len() != expected.len()
        || actual
            .iter()
            .zip(expected)
            .any(|((name, data_type), (expected_name, expected_type))| {
                name != expected_name || *data_type != expected_type
            })
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("query output schema drifted: {actual:?}"),
        )
        .into());
    }
    Ok(())
}

fn summarize(repetitions: &[RepetitionMeasurement]) -> Summary {
    let measurements = repetitions
        .iter()
        .flat_map(|repetition| &repetition.query_measurements)
        .collect::<Vec<_>>();
    let values = |select: fn(&QueryMeasurement) -> u64| {
        measurements
            .iter()
            .map(|measurement| select(measurement))
            .collect::<Vec<_>>()
    };
    let totals = values(|measurement| measurement.total_micros);
    let batch_latency = measurements
        .iter()
        .flat_map(|measurement| measurement.batch_latency_micros.iter().copied())
        .collect::<Vec<_>>();
    Summary {
        repetitions: repetitions.len(),
        produced_rows: measurements
            .iter()
            .map(|measurement| measurement.produced_rows)
            .max()
            .unwrap_or(0),
        produced_batches: measurements
            .iter()
            .map(|measurement| measurement.produced_batches)
            .max()
            .unwrap_or(0),
        planning: percentiles(&values(|measurement| measurement.planning_micros)),
        first_batch: percentiles(&values(|measurement| measurement.first_batch_micros)),
        total: percentiles(&totals),
        rows_per_second: percentiles(&values(|measurement| measurement.source_rows_per_second)),
        batch_latency: percentiles(&batch_latency),
        min_total: totals.iter().copied().min().unwrap_or(0),
        max_total: totals.iter().copied().max().unwrap_or(0),
        peak_rss: measurements
            .iter()
            .filter_map(|measurement| measurement.process_peak_rss_bytes)
            .max(),
        peak_rss_delta: measurements
            .iter()
            .filter_map(|measurement| measurement.process_peak_rss_delta_bytes)
            .max(),
        table_initialization: percentiles(
            &repetitions
                .iter()
                .map(|repetition| repetition.table_initialization_micros)
                .collect::<Vec<_>>(),
        ),
        parquet_metadata_preparation: summarize_parquet_metadata_preparation(repetitions),
        session_total: percentiles(
            &repetitions
                .iter()
                .map(|repetition| repetition.session_total_micros)
                .collect::<Vec<_>>(),
        ),
        read: summarize_read(&measurements),
    }
}

fn summarize_parquet_metadata_preparation(
    repetitions: &[RepetitionMeasurement],
) -> ParquetMetadataPreparationSummary {
    let Some(measurements) = repetitions
        .iter()
        .map(|repetition| repetition.parquet_metadata_preparation.as_ref())
        .collect::<Option<Vec<_>>>()
    else {
        return ParquetMetadataPreparationSummary {
            micros: None,
            file_count: None,
            memory_bytes: None,
            range_gets: None,
            full_gets: None,
            bytes_received: None,
        };
    };
    ParquetMetadataPreparationSummary {
        micros: Some(percentiles(
            &measurements
                .iter()
                .map(|measurement| measurement.micros)
                .collect::<Vec<_>>(),
        )),
        file_count: Some(percentile(
            &measurements
                .iter()
                .map(|measurement| saturating_u64(measurement.prepared.files_prepared as u128))
                .collect::<Vec<_>>(),
            50,
        )),
        memory_bytes: Some(percentile(
            &measurements
                .iter()
                .map(|measurement| {
                    saturating_u64(measurement.prepared.estimated_retained_metadata_bytes as u128)
                })
                .collect::<Vec<_>>(),
            50,
        )),
        range_gets: optional_percentile(
            measurements.iter().map(|measurement| {
                measurement
                    .prepared
                    .read_metrics
                    .parquet_data_file_range_get_operations
            }),
            50,
        ),
        full_gets: optional_percentile(
            measurements.iter().map(|measurement| {
                measurement
                    .prepared
                    .read_metrics
                    .parquet_data_file_full_get_operations
            }),
            50,
        ),
        bytes_received: optional_percentile(
            measurements.iter().map(|measurement| {
                measurement
                    .prepared
                    .read_metrics
                    .parquet_data_file_bytes_received
            }),
            50,
        ),
    }
}

fn summarize_read(measurements: &[&QueryMeasurement]) -> ReadSummary {
    let snapshots = measurements
        .iter()
        .map(|measurement| measurement.metrics.as_slice())
        .collect::<Vec<_>>();
    let counter = |reader: fn(&delta_arrow_reader::DeltaScanMetricsSnapshot) -> u64| {
        percentile(
            &snapshots
                .iter()
                .map(|snapshots| {
                    snapshots
                        .iter()
                        .map(|snapshot| reader(&snapshot.reader_metrics))
                        .sum()
                })
                .collect::<Vec<_>>(),
            50,
        )
    };
    let dynamic = |select: fn(&ScanMetricsSnapshot) -> u64| {
        percentile(
            &snapshots
                .iter()
                .map(|snapshots| snapshots.iter().map(select).sum())
                .collect::<Vec<_>>(),
            50,
        )
    };
    let optional = |select: fn(&delta_arrow_reader::DeltaScanMetricsSnapshot) -> Option<u64>| {
        optional_percentile(
            snapshots.iter().map(|snapshots| {
                snapshots.iter().try_fold(0_u64, |sum, snapshot| {
                    sum.checked_add(select(&snapshot.reader_metrics)?)
                })
            }),
            50,
        )
    };
    let optional_max =
        |select: fn(&delta_arrow_reader::DeltaScanMetricsSnapshot) -> Option<u64>| {
            snapshots
                .iter()
                .filter_map(|snapshots| {
                    snapshots.iter().try_fold(0_u64, |sum, snapshot| {
                        sum.checked_add(select(&snapshot.reader_metrics)?)
                    })
                })
                .max()
        };
    ReadSummary {
        scan_count: snapshots
            .iter()
            .map(|snapshots| snapshots.len())
            .max()
            .unwrap_or(0),
        scan_partitions_planned: snapshots
            .iter()
            .map(|snapshots| {
                snapshots
                    .iter()
                    .map(|snapshot| snapshot.reader_metrics.scan_partitions_planned)
                    .sum()
            })
            .max()
            .unwrap_or(0),
        files_planned: snapshots
            .iter()
            .map(|snapshots| {
                snapshots
                    .iter()
                    .map(|snapshot| snapshot.reader_metrics.files_planned)
                    .sum()
            })
            .max()
            .unwrap_or(0),
        estimated_input_rows: optional_max(|reader| reader.estimated_input_rows),
        estimated_input_bytes: optional_max(|reader| reader.estimated_input_bytes),
        scan_partitions_started: counter(|reader| reader.scan_partitions_started),
        scan_partitions_completed: counter(|reader| reader.scan_partitions_completed),
        file_tasks_started: counter(|reader| reader.file_tasks_started),
        file_tasks_completed: counter(|reader| reader.file_tasks_completed),
        dynamic_partition_tasks_pruned: dynamic(|snapshot| snapshot.dynamic_partition_tasks_pruned),
        dynamic_partition_tasks_kept: dynamic(|snapshot| snapshot.dynamic_partition_tasks_kept),
        dynamic_filters_received: dynamic(|snapshot| snapshot.dynamic_filters_received),
        dynamic_filters_accepted: dynamic(|snapshot| snapshot.dynamic_filters_accepted),
        dynamic_filters_rejected: dynamic(|snapshot| snapshot.dynamic_filters_rejected),
        dynamic_partition_filter_checks: dynamic(|snapshot| {
            snapshot.dynamic_partition_filter_checks
        }),
        dynamic_partition_tasks_kept_unusable_metadata: dynamic(|snapshot| {
            snapshot.dynamic_partition_tasks_kept_unusable_metadata
        }),
        dynamic_partition_tasks_kept_unevaluable_filter: dynamic(|snapshot| {
            snapshot.dynamic_partition_tasks_kept_unevaluable_filter
        }),
        scheduler_batches_emitted: counter(|reader| reader.scheduler_batches_emitted),
        scheduler_rows_emitted: counter(|reader| reader.scheduler_rows_emitted),
        deletion_vector_payloads_loaded: counter(|reader| reader.deletion_vector_payloads_loaded),
        deletion_vectors_applied: counter(|reader| reader.deletion_vectors_applied),
        deletion_vector_rows_deleted: counter(|reader| reader.deletion_vector_rows_deleted),
        deletion_vector_failures: counter(|reader| reader.deletion_vector_failures),
        deletion_vector_coordinate_rejections: counter(|reader| {
            reader.deletion_vector_coordinate_rejections
        }),
        range_gets: optional(|reader| reader.parquet_data_file_range_get_operations),
        full_gets: optional(|reader| reader.parquet_data_file_full_get_operations),
        bytes_received: optional(|reader| reader.parquet_data_file_bytes_received),
        estimated_task_bytes_admitted: optional(|reader| {
            reader.estimated_parquet_task_bytes_admitted
        }),
    }
}

fn csv_row(
    config: &Config,
    fixture: &Fixture,
    available_parallelism: Option<usize>,
    target_partitions: usize,
    repetitions: &[RepetitionMeasurement],
) -> Vec<String> {
    let summary = summarize(repetitions);
    let read = &summary.read;
    let preparation = &summary.parquet_metadata_preparation;
    let row = vec![
        BENCHMARK_SCHEMA_VERSION.to_string(),
        "provider_exec".to_owned(),
        env::consts::OS.to_owned(),
        env::consts::ARCH.to_owned(),
        optional_usize(available_parallelism),
        config.seed.to_string(),
        "1".to_owned(),
        config.workload.name().to_owned(),
        config.storage.name.to_owned(),
        config.query.name().to_owned(),
        backend_name(config.backend).to_owned(),
        config.scan_metadata_mode.name().to_owned(),
        config.parquet_metadata_mode.name().to_owned(),
        "prefetch_2_ap_target_scan_3x".to_owned(),
        target_partitions.to_string(),
        target_partitions.saturating_mul(3).max(1).to_string(),
        "3".to_owned(),
        "1".to_owned(),
        "2".to_owned(),
        summary.repetitions.to_string(),
        config.queries_per_table.to_string(),
        fixture.file_count.to_string(),
        fixture.row_count.to_string(),
        fixture.data_file_bytes.to_string(),
        fixture.deletion_vector_file_count.to_string(),
        fixture.deletion_vector_deleted_rows.to_string(),
        fixture.deletion_vector_deleted_rows_per_file.to_string(),
        read.scan_count.to_string(),
        read.scan_partitions_planned.to_string(),
        read.files_planned.to_string(),
        optional(read.estimated_input_rows),
        optional(read.estimated_input_bytes),
        read.scan_partitions_started.to_string(),
        read.scan_partitions_completed.to_string(),
        read.file_tasks_started.to_string(),
        read.file_tasks_completed.to_string(),
        read.dynamic_partition_tasks_pruned.to_string(),
        read.dynamic_partition_tasks_kept.to_string(),
        read.dynamic_filters_received.to_string(),
        read.dynamic_filters_accepted.to_string(),
        read.dynamic_filters_rejected.to_string(),
        read.dynamic_partition_filter_checks.to_string(),
        read.dynamic_partition_tasks_kept_unusable_metadata
            .to_string(),
        read.dynamic_partition_tasks_kept_unevaluable_filter
            .to_string(),
        read.scheduler_batches_emitted.to_string(),
        read.scheduler_rows_emitted.to_string(),
        read.deletion_vector_payloads_loaded.to_string(),
        read.deletion_vectors_applied.to_string(),
        read.deletion_vector_rows_deleted.to_string(),
        read.deletion_vector_failures.to_string(),
        read.deletion_vector_coordinate_rejections.to_string(),
        summary.produced_rows.to_string(),
        summary.produced_batches.to_string(),
        optional(summary.peak_rss),
        optional(summary.peak_rss_delta),
        summary.table_initialization.p50.to_string(),
        summary.table_initialization.p95.to_string(),
        summary.table_initialization.p99.to_string(),
        optional(preparation.micros.map(|micros| micros.p50)),
        optional(preparation.micros.map(|micros| micros.p95)),
        optional(preparation.micros.map(|micros| micros.p99)),
        optional(preparation.file_count),
        optional(preparation.memory_bytes),
        optional(preparation.range_gets),
        optional(preparation.full_gets),
        optional(preparation.bytes_received),
        summary.session_total.p50.to_string(),
        summary.session_total.p95.to_string(),
        summary.session_total.p99.to_string(),
        summary.planning.p50.to_string(),
        summary.planning.p95.to_string(),
        summary.planning.p99.to_string(),
        summary.first_batch.p50.to_string(),
        summary.first_batch.p95.to_string(),
        summary.first_batch.p99.to_string(),
        summary.total.p50.to_string(),
        summary.total.p95.to_string(),
        summary.total.p99.to_string(),
        summary.rows_per_second.p50.to_string(),
        summary.rows_per_second.p95.to_string(),
        summary.rows_per_second.p99.to_string(),
        summary.batch_latency.p50.to_string(),
        summary.batch_latency.p95.to_string(),
        summary.batch_latency.p99.to_string(),
        summary.min_total.to_string(),
        summary.max_total.to_string(),
        "0".to_owned(),
        "0".to_owned(),
        "disabled".to_owned(),
        optional_usize(config.metadata_size_hint_bytes),
        optional_usize(config.full_file_read_threshold_bytes),
        optional(read.range_gets),
        optional(read.full_gets),
        optional(read.bytes_received),
        optional(read.estimated_task_bytes_admitted),
        fixture.fingerprint.clone(),
    ];
    assert_eq!(row.len(), CSV_HEADER.len());
    row
}

fn backend_name(backend: ParquetReaderBackend) -> &'static str {
    match backend {
        ParquetReaderBackend::Direct => "direct_parquet",
        ParquetReaderBackend::DeltaKernel => "delta_kernel",
    }
}

fn percentiles(values: &[u64]) -> Percentiles {
    Percentiles {
        p50: percentile(values, 50),
        p95: percentile(values, 95),
        p99: percentile(values, 99),
    }
}

fn percentile(values: &[u64], percentile: u64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let rank = percentile
        .saturating_mul(sorted.len() as u64)
        .div_ceil(100)
        .saturating_sub(1);
    sorted[usize::try_from(rank)
        .unwrap_or(usize::MAX)
        .min(sorted.len().saturating_sub(1))]
}

fn optional_percentile(
    values: impl IntoIterator<Item = Option<u64>>,
    percentile_rank: u64,
) -> Option<u64> {
    Some(percentile(
        &values.into_iter().collect::<Option<Vec<_>>>()?,
        percentile_rank,
    ))
}

fn saturating_u64(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn optional(value: Option<u64>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
}

fn optional_usize(value: Option<usize>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
}

fn process_peak_rss_bytes() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    process_status_memory_kib(&status, "VmHWM").map(|kib| kib.saturating_mul(1024))
}

fn process_status_memory_kib(status: &str, key: &str) -> Option<u64> {
    status.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        if name != key {
            return None;
        }
        let mut fields = value.split_whitespace();
        let kib = fields.next()?.parse::<u64>().ok()?;
        fields.next().is_none_or(|unit| unit == "kB").then_some(kib)
    })
}

#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use super::*;

    type TestResult = Result<(), Box<dyn Error>>;

    fn config(
        workload: Workload,
        query: Query,
        backend: ParquetReaderBackend,
        storage: StorageProfile,
        metadata_size_hint_bytes: Option<usize>,
        full_file_read_threshold_bytes: Option<usize>,
    ) -> Config {
        Config {
            output: None,
            temp_dir: env::temp_dir(),
            storage,
            workload,
            query,
            backend,
            scan_metadata_mode: ScanMetadataMode::Lazy,
            parquet_metadata_mode: ParquetMetadataMode::OnDemand,
            queries_per_table: DEFAULT_QUERIES_PER_TABLE,
            repetitions: 1,
            metadata_size_hint_bytes,
            full_file_read_threshold_bytes,
            retain_fixture: false,
            seed: 0,
        }
    }

    fn only_query(repetition: &RepetitionMeasurement) -> Result<&QueryMeasurement, Box<dyn Error>> {
        let [measurement] = repetition.query_measurements.as_slice() else {
            return Err(invalid("expected exactly one query measurement").into());
        };
        Ok(measurement)
    }

    #[test]
    fn frozen_matrix_contains_exactly_fifteen_cases() -> TestResult {
        let local = StorageProfile::local();
        let throttled = StorageProfile::throttled();
        let cases = [
            config(
                Workload::FewLarger,
                Query::FullRows,
                ParquetReaderBackend::Direct,
                local,
                Some(65_536),
                None,
            ),
            config(
                Workload::FewLarger,
                Query::ProjectId,
                ParquetReaderBackend::Direct,
                local,
                Some(65_536),
                None,
            ),
            config(
                Workload::FewLarger,
                Query::FullRows,
                ParquetReaderBackend::DeltaKernel,
                local,
                Some(65_536),
                None,
            ),
            config(
                Workload::ManyUnequal,
                Query::FilterTailIds,
                ParquetReaderBackend::Direct,
                local,
                Some(65_536),
                None,
            ),
            config(
                Workload::ManySmall,
                Query::ProjectId,
                ParquetReaderBackend::Direct,
                local,
                Some(65_536),
                None,
            ),
            config(
                Workload::FewLarger,
                Query::FullRows,
                ParquetReaderBackend::Direct,
                local,
                None,
                None,
            ),
            config(
                Workload::FewLarger,
                Query::FullRows,
                ParquetReaderBackend::Direct,
                local,
                Some(8),
                None,
            ),
            config(
                Workload::FewLarger,
                Query::FullRows,
                ParquetReaderBackend::Direct,
                local,
                Some(65_536),
                Some(1_000_000),
            ),
            config(
                Workload::FewLarger,
                Query::FullRows,
                ParquetReaderBackend::Direct,
                local,
                Some(65_536),
                Some(1_000),
            ),
            config(
                Workload::FewLargerSparseDv,
                Query::ProjectId,
                ParquetReaderBackend::Direct,
                local,
                Some(65_536),
                None,
            ),
            config(
                Workload::FewLargerSparseDv,
                Query::ProjectId,
                ParquetReaderBackend::DeltaKernel,
                local,
                Some(65_536),
                None,
            ),
            config(
                Workload::FewLarger,
                Query::FullRows,
                ParquetReaderBackend::Direct,
                throttled,
                Some(65_536),
                None,
            ),
            config(
                Workload::FourThousandTinyFiles,
                Query::ProjectId,
                ParquetReaderBackend::Direct,
                local,
                Some(65_536),
                None,
            ),
            config(
                Workload::FourThousandTinyFiles,
                Query::ProjectId,
                ParquetReaderBackend::Direct,
                throttled,
                Some(65_536),
                None,
            ),
            config(
                Workload::ManyUnequal,
                Query::FilterTailIds,
                ParquetReaderBackend::Direct,
                throttled,
                Some(65_536),
                None,
            ),
        ];
        assert_eq!(cases.len(), 15);
        for mut case in cases {
            let scan_metadata_modes = if matches!(case.workload, Workload::FourThousandTinyFiles) {
                &[ScanMetadataMode::Eager][..]
            } else {
                &[ScanMetadataMode::Lazy, ScanMetadataMode::Eager][..]
            };
            for &mode in scan_metadata_modes {
                case.scan_metadata_mode = mode;
                case.validate()?;
            }
        }
        let outside = config(
            Workload::ManySmall,
            Query::ProjectId,
            ParquetReaderBackend::DeltaKernel,
            local,
            Some(65_536),
            None,
        );
        assert!(outside.validate().is_err());
        let mut prepared = config(
            Workload::FewLarger,
            Query::FullRows,
            ParquetReaderBackend::Direct,
            local,
            Some(65_536),
            None,
        );
        prepared.scan_metadata_mode = ScanMetadataMode::Eager;
        prepared.parquet_metadata_mode = ParquetMetadataMode::Prepared;
        prepared.validate()?;
        prepared.scan_metadata_mode = ScanMetadataMode::Lazy;
        assert!(prepared.validate().is_err());
        Ok(())
    }

    #[test]
    fn parser_preserves_frozen_options_and_rejects_invalid_values() -> TestResult {
        let parsed = Config::parse(
            [
                "--mode",
                "provider-exec",
                "--seed",
                "0",
                "--provider-exec-storage-profile",
                "local",
                "--provider-exec-workload",
                "provider_few_larger_files",
                "--provider-exec-query",
                "full_rows",
                "--provider-exec-backend",
                "direct_parquet",
                "--provider-exec-scan-metadata-mode",
                "eager",
                "--provider-exec-parquet-metadata-mode",
                "prepared",
                "--provider-exec-queries-per-table",
                "128",
                "--provider-exec-scheduling-profile",
                "prefetch_2_ap_target_scan_3x",
                "--provider-exec-parquet-metadata-size-hint-bytes",
                "65536",
                "--provider-exec-parquet-full-file-read-threshold-bytes",
                "disabled",
                "--provider-exec-repetitions",
                "5",
                "--provider-exec-temp-dir",
                "target/frozen-fixtures",
                "--provider-exec-retain-fixtures",
                "--output",
                "target/frozen.csv",
            ]
            .map(str::to_owned),
        )?;
        assert_eq!(parsed.seed, 0);
        assert_eq!(parsed.storage.name, "local");
        assert_eq!(parsed.workload.name(), "provider_few_larger_files");
        assert_eq!(parsed.query.name(), "full_rows");
        assert!(matches!(parsed.backend, ParquetReaderBackend::Direct));
        assert_eq!(parsed.scan_metadata_mode, ScanMetadataMode::Eager);
        assert_eq!(parsed.parquet_metadata_mode, ParquetMetadataMode::Prepared);
        assert_eq!(parsed.queries_per_table, 128);
        assert_eq!(parsed.repetitions, 5);
        assert_eq!(parsed.metadata_size_hint_bytes, Some(65_536));
        assert_eq!(parsed.full_file_read_threshold_bytes, None);
        assert_eq!(parsed.temp_dir, PathBuf::from("target/frozen-fixtures"));
        assert!(parsed.retain_fixture);
        assert_eq!(parsed.output, Some(PathBuf::from("target/frozen.csv")));
        let debug = format!("{parsed:?}");
        assert!(debug.contains("scan_metadata_mode: Eager"));
        assert!(debug.contains("queries_per_table: 128"));

        let defaults = Config::parse(Vec::<String>::new())?;
        assert_eq!(defaults.seed, 0);
        assert_eq!(defaults.storage.name, "local");
        assert_eq!(defaults.workload.name(), "provider_few_larger_files");
        assert_eq!(defaults.query.name(), "full_rows");
        assert!(matches!(defaults.backend, ParquetReaderBackend::Direct));
        assert_eq!(defaults.scan_metadata_mode, ScanMetadataMode::Lazy);
        assert_eq!(
            defaults.parquet_metadata_mode,
            ParquetMetadataMode::OnDemand
        );
        assert_eq!(defaults.queries_per_table, DEFAULT_QUERIES_PER_TABLE);
        assert_eq!(defaults.repetitions, DEFAULT_REPETITIONS);
        assert_eq!(defaults.metadata_size_hint_bytes, Some(65_536));
        assert_eq!(defaults.full_file_read_threshold_bytes, None);
        assert!(!defaults.retain_fixture);
        assert_eq!(defaults.output, None);

        let full_read = Config::parse(
            [
                "--provider-exec-parquet-full-file-read-threshold-bytes",
                "1000000",
            ]
            .map(str::to_owned),
        )?;
        assert_eq!(full_read.full_file_read_threshold_bytes, Some(1_000_000));
        assert_eq!(
            Config::parse(["--provider-exec-scan-metadata-mode", "lazy"].map(str::to_owned))?
                .scan_metadata_mode,
            ScanMetadataMode::Lazy
        );
        assert_eq!(
            Config::parse(["--provider-exec-scan-metadata-mode", "eager"].map(str::to_owned))?
                .scan_metadata_mode,
            ScanMetadataMode::Eager
        );
        assert!(
            Config::parse(["--provider-exec-scan-metadata-mode", "automatic"].map(str::to_owned))
                .is_err()
        );
        assert!(Config::parse(["--provider-exec-scan-metadata-mode"].map(str::to_owned)).is_err());
        assert_eq!(
            Config::parse(
                [
                    "--provider-exec-scan-metadata-mode",
                    "eager",
                    "--provider-exec-parquet-metadata-mode",
                    "prepared",
                ]
                .map(str::to_owned),
            )?
            .parquet_metadata_mode,
            ParquetMetadataMode::Prepared
        );
        assert!(
            Config::parse(["--provider-exec-parquet-metadata-mode", "prepared"].map(str::to_owned))
                .is_err()
        );
        assert!(
            Config::parse(
                ["--provider-exec-parquet-metadata-mode", "automatic"].map(str::to_owned)
            )
            .is_err()
        );
        for count in ["1", "128"] {
            assert_eq!(
                Config::parse(["--provider-exec-queries-per-table", count].map(str::to_owned))?
                    .queries_per_table,
                count.parse::<usize>()?
            );
        }
        for count in ["0", "129"] {
            assert!(
                Config::parse(["--provider-exec-queries-per-table", count].map(str::to_owned))
                    .is_err()
            );
        }
        assert!(Config::parse(["--provider-exec-queries-per-table"].map(str::to_owned)).is_err());
        assert!(Config::parse(["--provider-exec-repetitions", "0"].map(str::to_owned)).is_err());
        assert!(Config::parse(["--provider-exec-repetitions"].map(str::to_owned)).is_err());
        assert!(
            Config::parse(["--provider-exec-scheduling-profile", "other"].map(str::to_owned))
                .is_err()
        );
        assert!(Config::parse(["--unknown"].map(str::to_owned)).is_err());
        Ok(())
    }

    #[test]
    fn fixture_shapes_and_fingerprints_match_the_frozen_recipes() -> TestResult {
        for workload in [
            Workload::FewLarger,
            Workload::ManySmall,
            Workload::ManyUnequal,
            Workload::FewLargerSparseDv,
        ] {
            let fixture =
                Fixture::create(&env::temp_dir(), workload, StorageProfile::local(), false)?;
            let temporary_path = fixture.path.clone();
            let expected = EXPECTED_FIXTURE_FINGERPRINTS
                .iter()
                .find_map(|(name, fingerprint)| (*name == workload.name()).then_some(*fingerprint))
                .ok_or_else(|| invalid("missing expected fixture fingerprint"))?;
            assert_eq!(fixture.fingerprint, expected);
            assert_eq!(fixture.file_count, workload.files().len());
            assert_eq!(
                fixture.row_count,
                workload.files().iter().map(|file| file.rows).sum::<usize>()
            );
            drop(fixture);
            assert!(!temporary_path.exists());
        }
        assert_eq!(Workload::FourThousandTinyFiles.files().len(), 4_096);
        let retained = Fixture::create(
            &env::temp_dir(),
            Workload::FewLarger,
            StorageProfile::local(),
            true,
        )?;
        let retained_path = retained.path.clone();
        drop(retained);
        assert!(retained_path.exists());
        fs::remove_dir_all(retained_path)?;
        Ok(())
    }

    #[test]
    fn unequal_pruning_and_delayed_http_use_the_public_reader() -> TestResult {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let unequal_config = config(
                Workload::ManyUnequal,
                Query::FilterTailIds,
                ParquetReaderBackend::Direct,
                StorageProfile::local(),
                Some(65_536),
                None,
            );
            let unequal = Fixture::create(
                &env::temp_dir(),
                unequal_config.workload,
                unequal_config.storage,
                false,
            )?;
            let unequal_repetition = run_repetition(&unequal_config, &unequal, 4).await?;
            let unequal_measurement = only_query(&unequal_repetition)?;
            assert_eq!(unequal_measurement.produced_rows, 68_608);
            assert_eq!(
                unequal_measurement.metrics[0].reader_metrics.files_planned,
                32
            );

            let http_config = config(
                Workload::FewLarger,
                Query::FullRows,
                ParquetReaderBackend::Direct,
                StorageProfile::throttled(),
                Some(65_536),
                None,
            );
            let http = Fixture::create(
                &env::temp_dir(),
                http_config.workload,
                http_config.storage,
                false,
            )?;
            let http_repetition = run_repetition(&http_config, &http, 4).await?;
            let http_measurement = only_query(&http_repetition)?;
            let reader = &http_measurement.metrics[0].reader_metrics;
            assert_eq!(reader.scheduler_rows_emitted, 32_768);
            assert_eq!(reader.parquet_data_file_range_get_operations, Some(8));
            Ok::<_, Box<dyn Error>>(())
        })
    }

    #[test]
    fn one_registered_table_runs_three_queries_with_on_demand_or_prepared_metadata() -> TestResult {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let mut config = config(
                Workload::FewLarger,
                Query::FullRows,
                ParquetReaderBackend::Direct,
                StorageProfile::local(),
                Some(65_536),
                None,
            );
            config.queries_per_table = 3;
            let fixture =
                Fixture::create(&env::temp_dir(), config.workload, config.storage, false)?;
            let expected_rows = expected_rows(config.workload, config.query, &fixture);

            for (scan_metadata_mode, parquet_metadata_mode) in [
                (ScanMetadataMode::Lazy, ParquetMetadataMode::OnDemand),
                (ScanMetadataMode::Eager, ParquetMetadataMode::OnDemand),
                (ScanMetadataMode::Eager, ParquetMetadataMode::Prepared),
            ] {
                config.scan_metadata_mode = scan_metadata_mode;
                config.parquet_metadata_mode = parquet_metadata_mode;
                let repetition = run_repetition(&config, &fixture, 4).await?;
                let repetitions = [repetition];
                let summary = summarize(&repetitions);
                assert_eq!(summary.repetitions, 1);
                assert_eq!(summary.read.scan_count, 1);
                assert_eq!(summary.read.files_planned, 4);
                assert_eq!(repetitions[0].table_load_count, 1);
                assert_eq!(repetitions[0].table_registration_count, 1);
                assert_eq!(repetitions[0].query_measurements.len(), 3);
                match parquet_metadata_mode {
                    ParquetMetadataMode::OnDemand => {
                        assert!(repetitions[0].parquet_metadata_preparation.is_none());
                    }
                    ParquetMetadataMode::Prepared => {
                        let preparation = repetitions[0]
                            .parquet_metadata_preparation
                            .as_ref()
                            .ok_or("prepared mode did not report metadata preparation")?;
                        assert_eq!(preparation.prepared.files_prepared, fixture.file_count);
                        assert!(preparation.prepared.estimated_retained_metadata_bytes > 0);
                        assert!(
                            preparation
                                .prepared
                                .read_metrics
                                .parquet_data_file_range_get_operations
                                .is_some_and(|operations| operations > 0)
                        );
                    }
                }
                assert!(
                    repetitions[0].session_total_micros
                        >= repetitions[0].table_initialization_micros
                );
                for measurement in &repetitions[0].query_measurements {
                    let [metrics] = measurement.metrics.as_slice() else {
                        return Err("expected one fresh Delta scan per query".into());
                    };
                    let reader = &metrics.reader_metrics;
                    assert_eq!(measurement.produced_rows, expected_rows);
                    assert!(measurement.produced_batches > 0);
                    assert_eq!(reader.scan_partitions_started, 4);
                    assert_eq!(reader.scan_partitions_completed, 4);
                    assert_eq!(reader.file_tasks_started, 4);
                    assert_eq!(reader.file_tasks_completed, 4);
                    assert_eq!(reader.scheduler_rows_emitted, expected_rows as u64);
                }
            }
            Ok::<_, Box<dyn Error>>(())
        })
    }

    #[test]
    fn summaries_keep_repetition_and_query_sample_populations_separate() {
        let query = |planning_micros| QueryMeasurement {
            planning_micros,
            first_batch_micros: planning_micros,
            total_micros: planning_micros,
            source_rows_per_second: planning_micros,
            produced_rows: 1,
            produced_batches: 1,
            process_peak_rss_bytes: None,
            process_peak_rss_delta_bytes: None,
            batch_latency_micros: vec![planning_micros],
            metrics: Vec::new(),
        };
        let repetitions = [
            RepetitionMeasurement {
                table_initialization_micros: 10,
                parquet_metadata_preparation: None,
                session_total_micros: 100,
                table_load_count: 1,
                table_registration_count: 1,
                query_measurements: [1, 2, 3].map(query).into(),
            },
            RepetitionMeasurement {
                table_initialization_micros: 30,
                parquet_metadata_preparation: None,
                session_total_micros: 300,
                table_load_count: 1,
                table_registration_count: 1,
                query_measurements: [4, 5, 100].map(query).into(),
            },
        ];

        let summary = summarize(&repetitions);

        assert_eq!(summary.repetitions, 2);
        assert_eq!(
            (
                summary.table_initialization.p50,
                summary.table_initialization.p95,
                summary.table_initialization.p99,
            ),
            (10, 30, 30)
        );
        assert_eq!(
            (
                summary.session_total.p50,
                summary.session_total.p95,
                summary.session_total.p99,
            ),
            (100, 300, 300)
        );
        assert_eq!(
            (
                summary.planning.p50,
                summary.planning.p95,
                summary.planning.p99,
            ),
            (3, 100, 100)
        );
    }

    #[test]
    fn csv_and_helpers_preserve_frozen_edge_behavior() -> TestResult {
        assert_eq!(CSV_HEADER.len(), 96);
        assert_eq!(CSV_HEADER[11], "scan_metadata_mode");
        assert_eq!(CSV_HEADER[12], "parquet_metadata_mode");
        assert_eq!(CSV_HEADER[20], "queries_per_table");
        assert_eq!(
            &CSV_HEADER[55..69],
            [
                "table_initialization_micros_p50",
                "table_initialization_micros_p95",
                "table_initialization_micros_p99",
                "parquet_metadata_preparation_micros_p50",
                "parquet_metadata_preparation_micros_p95",
                "parquet_metadata_preparation_micros_p99",
                "prepared_parquet_metadata_file_count_p50",
                "prepared_parquet_metadata_memory_bytes_p50",
                "parquet_metadata_preparation_range_get_operations_p50",
                "parquet_metadata_preparation_full_get_operations_p50",
                "parquet_metadata_preparation_bytes_received_p50",
                "session_total_micros_p50",
                "session_total_micros_p95",
                "session_total_micros_p99",
            ]
        );
        assert_eq!(percentile(&[], 50), 0);
        assert_eq!(percentile(&[10, 20, 30, 40], 50), 20);
        assert_eq!(percentile(&[10, 20, 30, 40], 95), 40);
        assert_eq!(percentile(&[10, 20, 30, 40], 100), 40);
        assert_eq!(
            optional_percentile([Some(10), Some(30), Some(20)], 50),
            Some(20)
        );
        assert_eq!(optional_percentile([Some(10), None], 50), None);
        assert_eq!(optional(None), "");
        assert_eq!(optional(Some(1_024)), "1024");
        assert_eq!(optional_usize(None), "");
        assert_eq!(optional_usize(Some(8)), "8");
        assert_eq!(parse_optional_positive("disabled")?, None);
        assert!(parse_optional_positive("0").is_err());
        assert_eq!(parse_range(Some("bytes=-4"), 10)?, Some((6, 10)));
        assert_eq!(
            process_status_memory_kib("VmPeak:\t12 kB\nVmHWM:\t34 kB\n", "VmHWM"),
            Some(34)
        );
        assert_eq!(process_status_memory_kib("VmHWM:\t34 kB\n", "VmSwap"), None);
        assert_eq!(process_status_memory_kib("VmHWM:\t34 MB\n", "VmHWM"), None);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let mut config = config(
                Workload::FewLargerSparseDv,
                Query::ProjectId,
                ParquetReaderBackend::DeltaKernel,
                StorageProfile::local(),
                Some(65_536),
                None,
            );
            config.scan_metadata_mode = ScanMetadataMode::Eager;
            config.queries_per_table = 3;
            let fixture =
                Fixture::create(&env::temp_dir(), config.workload, config.storage, false)?;
            let repetition = run_repetition(&config, &fixture, 4).await?;
            let repetitions = [repetition];
            let summary = summarize(&repetitions);
            let row = csv_row(&config, &fixture, Some(4), 4, &repetitions);
            assert_eq!(row.len(), CSV_HEADER.len());
            assert_eq!(row[0], "32");
            assert_eq!(row[1], "provider_exec");
            assert_eq!(row[2], env::consts::OS);
            assert_eq!(row[3], env::consts::ARCH);
            assert_eq!(row[4], "4");
            assert_eq!(row[5], "0");
            assert_eq!(row[6], "1");
            assert_eq!(row[7], "provider_few_larger_files_sparse_dv");
            assert_eq!(row[8], "local");
            assert_eq!(row[9], "project_id");
            assert_eq!(row[10], "delta_kernel");
            assert_eq!(row[11], "eager");
            assert_eq!(row[12], "on_demand");
            assert_eq!(row[13], "prefetch_2_ap_target_scan_3x");
            assert_eq!(&row[14..19], ["4", "12", "3", "1", "2"]);
            assert_eq!(row[19], "1");
            assert_eq!(row[20], "3");
            assert_eq!(row[21], "4");
            assert_eq!(row[22], "32768");
            assert_eq!(row[23], "819772");
            assert_eq!(row[24], "4");
            assert_eq!(row[25], "12");
            assert_eq!(row[26], "3");
            assert_eq!(row[27], "1");
            assert_eq!(&row[28..32], ["4", "4", "32768", "819772"]);
            assert_eq!(&row[32..36], ["4", "4", "4", "4"]);
            assert!(row[36..44].iter().all(|value| value == "0"));
            assert_eq!(row[44], "36");
            assert_eq!(row[45], "32756");
            assert_eq!(&row[46..51], ["4", "4", "12", "0", "0"]);
            assert_eq!(row[51], "32756");
            assert_eq!(row[52], "36");
            assert_eq!(row[53], optional(summary.peak_rss));
            assert_eq!(row[54], optional(summary.peak_rss_delta));
            assert_eq!(row[55], summary.table_initialization.p50.to_string());
            assert_eq!(row[56], summary.table_initialization.p95.to_string());
            assert_eq!(row[57], summary.table_initialization.p99.to_string());
            assert!(row[58..66].iter().all(String::is_empty));
            assert_eq!(row[66], summary.session_total.p50.to_string());
            assert_eq!(row[67], summary.session_total.p95.to_string());
            assert_eq!(row[68], summary.session_total.p99.to_string());
            assert_eq!(row[69], summary.planning.p50.to_string());
            assert_eq!(row[70], summary.planning.p95.to_string());
            assert_eq!(row[71], summary.planning.p99.to_string());
            assert_eq!(row[72], summary.first_batch.p50.to_string());
            assert_eq!(row[73], summary.first_batch.p95.to_string());
            assert_eq!(row[74], summary.first_batch.p99.to_string());
            assert_eq!(row[75], summary.total.p50.to_string());
            assert_eq!(row[76], summary.total.p95.to_string());
            assert_eq!(row[77], summary.total.p99.to_string());
            assert_eq!(row[78], summary.rows_per_second.p50.to_string());
            assert_eq!(row[79], summary.rows_per_second.p95.to_string());
            assert_eq!(row[80], summary.rows_per_second.p99.to_string());
            assert_eq!(row[81], summary.batch_latency.p50.to_string());
            assert_eq!(row[82], summary.batch_latency.p95.to_string());
            assert_eq!(row[83], summary.batch_latency.p99.to_string());
            assert_eq!(row[84], summary.min_total.to_string());
            assert_eq!(row[85], summary.max_total.to_string());
            assert_eq!(row[86], "0");
            assert_eq!(row[87], "0");
            assert_eq!(row[88], "disabled");
            assert_eq!(row[89], "65536");
            assert_eq!(row[90], "");
            assert_eq!(&row[91..95], ["", "", "", ""]);
            assert_eq!(row[95], "fnv1a64:e1509da31486f25a");
            Ok::<_, Box<dyn Error>>(())
        })?;
        Ok(())
    }
}
