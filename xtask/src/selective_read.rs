mod campaign;
mod experiment;
mod invoke;
mod network;
mod observe;
mod pairs;
mod report;
mod storage;
mod supervise;
mod validation;
mod workload;
mod zstd_probe;

use clap::Subcommand;
use serde_json::Value;
use std::{ffi::OsString, path::PathBuf};

use crate::Result;

#[derive(Subcommand)]
pub(crate) enum Task {
    /// Supervise a reader process. Read the launch request as JSON from stdin.
    Supervise,
    /// Invoke a reader and retain its process evidence. Read JSON from stdin.
    Invoke,
    /// Schedule and summarize a campaign. Read the operation as JSON from stdin.
    Campaign,
    /// Execute a frozen campaign, using Python only for independent data validation.
    RunCampaign(Box<campaign::controller::Args>),
    /// Upload a verified fixture snapshot and save its immutable receipt.
    UploadStorage(storage::upload::Args),
    /// Operate the dedicated benchmark storage. Read the operation as JSON from stdin.
    Storage,
    /// Read or reconcile storage observations. Read the operation as JSON from stdin.
    Observe,
    /// Control the optional benchmark proxy. Read the operation as JSON from stdin.
    Network,
    /// Combine audited formal batches. Read the report inputs as JSON from stdin.
    ProductionReport,
    /// Export audited matrix, file-organization or workload results as JSON/CSV.
    CampaignReport,
    /// Validate workload definitions and fixture metadata.
    Workload,
    /// Validate public query matrix metadata. Read the operation as JSON from stdin.
    Matrix,
    /// Prepare all 30 public cases with independent Python references.
    PrepareMatrix(workload::matrix::Prepare),
    /// Export a validated public query matrix campaign.
    ReportMatrix(workload::matrix::Report),
    /// Verify repacked rows and prepare independent file-organization references.
    PrepareFiles(workload::files::Prepare),
    /// Export a verified file-organization campaign.
    ReportFiles(workload::files::Report),
    /// Freeze independently checked production fixtures and pinned Polars translations.
    FreezeWorkload(Box<workload::production::Define>),
    /// Probe or generate production layouts using the pinned Arrow streaming producer.
    ProductionFixtures(Box<workload::fixtures::Args>),
    /// Plan production layouts from independent Arrow predicates and DuckDB geometry.
    PlanProduction(workload::planner::Args),
    /// Pair independently validated production fixtures. Read JSON from stdin.
    ProductionPairs,
    /// Run a numeric-page or Delta-length diagnostic process. Read JSON from stdin.
    Experiment,
    /// Compare partial-page policies or reader builds. Read one invocation as JSON.
    IntraPage,
    /// Run a complete partial-page or two-build experiment with independent validation.
    RunIntraPage(Box<experiment::intra_page::batch::Args>),
    /// Compare numeric readers against exact independent Arrow values.
    RunNumericPages(Box<experiment::batch::Numeric>),
    /// Compare Delta-length policies and preserve all validation and timing samples.
    RunDeltaLength(Box<experiment::batch::Length>),
    /// Measure raw Zstd block discovery against complete column reads.
    ZstdProbe(zstd_probe::Args),
    /// Calibrate the 200 +/- 20 ms, 150 Mbit/s proxy against signed MinIO reads.
    CheckNetwork {
        #[arg(long)]
        state: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Build and attach the optional network proxy.
    StartNetwork(network::Start),
    /// Stop the attached proxy after proving that its process exited.
    StopNetwork {
        #[arg(long)]
        state: PathBuf,
    },
    /// Stop the dedicated benchmark server and any attached proxy.
    StopStorage {
        #[arg(long)]
        state: PathBuf,
    },
    /// Capture and reconcile MinIO requests, using a deadline sent on stdin.
    Capture {
        #[arg(long)]
        state: PathBuf,
        #[arg(long)]
        request: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Build the pinned MinIO server in a new directory.
    PrepareStorage {
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        source: Option<PathBuf>,
    },
    /// Start the pinned server with dedicated CPUs and enforced memory limits.
    StartStorage {
        #[arg(long)]
        build: PathBuf,
        #[arg(long)]
        state: PathBuf,
        #[arg(long, default_value_t = 19000)]
        port: u16,
    },
    /// Verify the effective reader limits, then replace this process with the reader.
    ExecReader {
        #[arg(long)]
        state: PathBuf,
        #[arg(last = true, required = true)]
        command: Vec<OsString>,
    },
}

impl Task {
    pub(crate) fn run(self) -> Result<()> {
        match self {
            Self::RunCampaign(args) => return campaign::controller::run(*args),
            Self::ProductionFixtures(args) => return workload::fixtures::run(*args),
            Self::PlanProduction(args) => return workload::planner::run(args),
            Self::PrepareMatrix(args) => {
                let result = workload::matrix::prepare(args)?;
                require(
                    result["status"] == "complete",
                    "matrix preparation incomplete",
                )?;
                return Ok(());
            }
            Self::ReportMatrix(args) => {
                println!("{}", workload::matrix::report(args)?);
                return Ok(());
            }
            Self::PrepareFiles(args) => {
                let result = workload::files::prepare(args)?;
                println!(
                    "{}",
                    serde_json::json!({"status":result["status"],"cases":array(&result["cases"])?.len()})
                );
                return Ok(());
            }
            Self::ReportFiles(args) => {
                println!("{}", workload::files::report(args)?);
                return Ok(());
            }
            Self::RunIntraPage(args) => return experiment::intra_page::batch::run(*args),
            Self::FreezeWorkload(args) => {
                let value = workload::production::define(*args)?;
                println!(
                    "{}",
                    serde_json::json!({"comparison_revision":value["comparison_revision"], "scope":value["scope"],
                    "prepared_cases":array(&value["cases"])?.len(),"core_cases":8})
                );
                return Ok(());
            }
            Self::RunNumericPages(args) => return experiment::batch::numeric(*args),
            Self::RunDeltaLength(args) => return experiment::batch::length(*args),
            Self::UploadStorage(args) => {
                let receipt = storage::upload::run(&args, storage::upload::hashes)?;
                println!(
                    "{}",
                    serde_json::json!({"status":receipt["status"], "objects":array(&receipt["objects"])?.len()})
                );
                return Ok(());
            }
            Self::ZstdProbe(args) => return zstd_probe::run(args),
            Self::CheckNetwork { state, output } => return network::calibrate(&state, &output),
            Self::StartNetwork(arguments) => return network::start(arguments),
            Self::StopNetwork { state } => {
                let _lock = storage::exclusive(&state)?;
                return network::stop(&state);
            }
            Self::StopStorage { state } => {
                let _lock = storage::exclusive(&state)?;
                network::stop(&state)?;
                let config = storage::state(&state)?;
                require(
                    text(&config["unit"])?.starts_with("selective-read-minio-"),
                    "unexpected MinIO unit",
                )?;
                crate::command::run_command(std::process::Command::new("systemctl").args([
                    "--user",
                    "stop",
                    text(&config["unit"])?,
                ]))?;
                return Ok(());
            }
            Self::ExecReader { state, command } => return storage::exec_reader(&state, &command),
            Self::PrepareStorage { output, source } => {
                return storage::service::prepare(&output, source.as_deref());
            }
            Self::StartStorage { build, state, port } => {
                return storage::service::start(&build, &state, port);
            }
            Self::Capture {
                state,
                request,
                output,
            } => return observe::capture(&state, &request, &output),
            _ => {}
        }
        let input: Value = serde_json::from_reader(std::io::stdin().lock())?;
        let result = match self {
            Self::Supervise => supervise::launch(serde_json::from_value(input)?)?,
            Self::Invoke => invoke::run(&serde_json::from_value(input)?)?,
            Self::Campaign => campaign::run(serde_json::from_value(input)?)?,
            Self::Storage => storage::run(serde_json::from_value(input)?)?,
            Self::Observe => observe::run(serde_json::from_value(input)?)?,
            Self::Network => network::run(serde_json::from_value(input)?)?,
            Self::ProductionReport => report::run(serde_json::from_value(input)?)?,
            Self::CampaignReport => report::batch::run(serde_json::from_value(input)?)?,
            Self::Workload => workload::run(serde_json::from_value(input)?)?,
            Self::Matrix => workload::matrix::run(serde_json::from_value(input)?)?,
            Self::ProductionPairs => pairs::run(serde_json::from_value(input)?)?,
            Self::Experiment => experiment::run(serde_json::from_value(input)?)?,
            Self::IntraPage => experiment::intra_page::run(serde_json::from_value(input)?)?,
            Self::ExecReader { .. }
            | Self::RunCampaign(_)
            | Self::RunIntraPage(_)
            | Self::FreezeWorkload(_)
            | Self::ProductionFixtures(_)
            | Self::PlanProduction(_)
            | Self::PrepareMatrix(_)
            | Self::ReportMatrix(_)
            | Self::PrepareFiles(_)
            | Self::ReportFiles(_)
            | Self::RunNumericPages(_)
            | Self::RunDeltaLength(_)
            | Self::UploadStorage(_)
            | Self::ZstdProbe(_)
            | Self::CheckNetwork { .. }
            | Self::PrepareStorage { .. }
            | Self::StartStorage { .. }
            | Self::Capture { .. }
            | Self::StartNetwork(_)
            | Self::StopNetwork { .. }
            | Self::StopStorage { .. } => {
                unreachable!()
            }
        };
        println!("{}", serde_json::to_string(&result)?);
        Ok(())
    }
}

fn require(condition: bool, reason: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(reason.into())
    }
}

fn object(value: &Value) -> Result<&serde_json::Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| "expected a JSON object".into())
}

fn array(value: &Value) -> Result<&Vec<Value>> {
    value
        .as_array()
        .ok_or_else(|| "expected a JSON array".into())
}

fn field<'a>(value: &'a Value, key: &str) -> Result<&'a Value> {
    value
        .get(key)
        .ok_or_else(|| format!("missing field: {key}").into())
}

fn text(value: &Value) -> Result<&str> {
    value
        .as_str()
        .ok_or_else(|| "expected a JSON string".into())
}

fn budget() -> Value {
    serde_json::json!({"worker_threads":8, "max_blocking_threads":64, "target_partitions":8,
        "batch_rows":8192, "datafusion_pool_bytes":4 * 1024u64.pow(3),
        "process_memory_bytes":8 * 1024u64.pow(3), "logical_cpus":8})
}

fn fixture_tables(manifest: &Value) -> Result<&Vec<Value>> {
    require(
        manifest["status"] == "complete",
        "incomplete fixture manifest",
    )?;
    match manifest["protocol"].as_str() {
        Some("selective-read-v1") => array(&manifest["tables"]),
        Some("selective-read-production-fixtures-v1" | "selective-read-production-pairs-v1") => {
            require(
                manifest["writer"]["status"] == "complete",
                "incomplete production writer",
            )?;
            array(&manifest["writer"]["tables"])
        }
        _ => Err("unknown fixture manifest".into()),
    }
}
