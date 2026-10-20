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
        if let Self::ExecReader { state, command } = self {
            return storage::exec_reader(&state, &command);
        }
        let input: Value = serde_json::from_reader(std::io::stdin().lock())?;
        let result = match self {
            Self::Supervise => supervise::launch(serde_json::from_value(input)?)?,
            Self::Campaign => campaign::run(serde_json::from_value(input)?)?,
            Self::Storage => storage::run(serde_json::from_value(input)?)?,
            Self::ExecReader { .. } => unreachable!(),
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
