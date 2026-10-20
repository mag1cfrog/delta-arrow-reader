mod campaign;
mod invoke;
mod network;
mod observe;
mod pairs;
mod report;
mod storage;
mod supervise;
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
    /// Operate the dedicated benchmark storage. Read the operation as JSON from stdin.
    Storage,
    /// Read or reconcile storage observations. Read the operation as JSON from stdin.
    Observe,
    /// Control the optional benchmark proxy. Read the operation as JSON from stdin.
    Network,
    /// Combine audited formal batches. Read the report inputs as JSON from stdin.
    ProductionReport,
    /// Pair independently validated production fixtures. Read JSON from stdin.
    ProductionPairs,
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
            Self::ProductionPairs => pairs::run(serde_json::from_value(input)?)?,
            Self::ExecReader { .. }
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
