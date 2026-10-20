mod campaign;
mod storage;
mod supervise;

use clap::Subcommand;
use serde_json::Value;
use std::{ffi::OsString, path::PathBuf};

use crate::Result;

#[derive(Subcommand)]
pub(crate) enum Task {
    /// Supervise a reader process. Read the launch request as JSON from stdin.
    Supervise,
    /// Schedule and summarize a campaign. Read the operation as JSON from stdin.
    Campaign,
    /// Operate the dedicated benchmark storage. Read the operation as JSON from stdin.
    Storage,
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
            Self::ExecReader { state, command } => return storage::exec_reader(&state, &command),
            Self::PrepareStorage { output, source } => {
                return storage::service::prepare(&output, source.as_deref());
            }
            Self::StartStorage { build, state, port } => {
                return storage::service::start(&build, &state, port);
            }
            _ => {}
        }
        let input: Value = serde_json::from_reader(std::io::stdin().lock())?;
        let result = match self {
            Self::Supervise => supervise::launch(serde_json::from_value(input)?)?,
            Self::Campaign => campaign::run(serde_json::from_value(input)?)?,
            Self::Storage => storage::run(serde_json::from_value(input)?)?,
            Self::ExecReader { .. } | Self::PrepareStorage { .. } | Self::StartStorage { .. } => {
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
