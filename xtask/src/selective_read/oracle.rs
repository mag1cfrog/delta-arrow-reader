//! Independent value rules and comparisons; never invoke a candidate reader.

use std::path::PathBuf;

use super::require;
use crate::Result;

mod capabilities;
pub(super) mod check;
mod experiments;
mod extras;
mod files;
mod geometry;
mod io;
mod limits;
mod prepare;
mod production;
mod rows;
mod rules;
mod sort;
mod spec;
mod table;

const MEMORY: usize = 512 * 1024 * 1024;

struct State {
    python: PathBuf,
    harness: PathBuf,
    python_version: String,
    decoder: io::Decoder,
    digests: files::Digests,
    cache: production::Cache,
    extras: std::collections::HashMap<Vec<files::Identity>, std::collections::BTreeSet<[i64; 2]>>,
    spark: bool,
    experiments: std::collections::HashMap<String, arrow_array::RecordBatch>,
}
impl State {
    fn new(python: &std::path::Path, harness: &std::path::Path) -> Result<Self> {
        let version = std::process::Command::new(python)
            .arg("--version")
            .output()?;
        require(
            version.status.success(),
            "oracle Python runtime is unavailable",
        )?;
        let version = String::from_utf8(version.stdout)?;
        let python_version = version
            .trim()
            .strip_prefix("Python ")
            .ok_or("unexpected Python runtime version")?
            .to_owned();
        let harness = harness.canonicalize()?;
        Ok(Self {
            python: python.into(),
            python_version,
            decoder: io::Decoder::new(python, &harness),
            harness,
            digests: Default::default(),
            cache: Default::default(),
            extras: Default::default(),
            spark: false,
            experiments: Default::default(),
        })
    }
    fn call(&mut self, value: &serde_json::Value) -> Result<serde_json::Value> {
        use super::{array, text};
        use serde_json::{Value, json};
        use std::path::Path;
        match text(&value["action"])? {
            "hashes" => Ok(json!(
                array(&value["paths"])?
                    .iter()
                    .map(|p| self.digests.digest(Path::new(text(p)?)))
                    .collect::<Result<Vec<_>>>()?
            )),
            "prepare-reference" => self.prepare(value),
            "check-export" => self.check(value),
            "check-intra" => self.intra(value),
            "spark-pilot" => {
                self.spark = true;
                Ok(Value::Null)
            }
            "repacked-values" => Ok(json!(self.same_tables(
                Path::new(text(&value["parent"])?),
                &value["original"],
                Path::new(text(&value["fixtures"])?),
                &value["table"]
            )?)),
            "source-counts" => self.source_counts(value),
            "capability-export" => self.capability_export(value),
            "capability-stream" => self.capability_stream(value),
            "capability-corrupt" => capabilities::corrupt(value),
            "experiment-reference" => self.experiment_reference(value),
            "experiment-check" => self.experiment_check(value),
            _ => Err("unknown oracle operation".into()),
        }
    }
}

pub(crate) fn run(value: serde_json::Value) -> Result<serde_json::Value> {
    let mut state = State::new(
        std::path::Path::new(super::text(&value["python"])?),
        std::path::Path::new(super::text(&value["harness"])?),
    )?;
    state.spark = value["spark_pilot"] == true;
    state.call(&value)
}
#[derive(clap::Args)]
pub(crate) struct Worker {
    #[arg(long)]
    python: PathBuf,
    #[arg(long)]
    harness: PathBuf,
}
pub(crate) fn worker(args: Worker) -> Result<()> {
    use std::io::{BufRead, Write};
    let mut state = State::new(&args.python, &args.harness)?;
    let mut output = std::io::stdout().lock();
    for line in std::io::stdin().lock().lines() {
        let line = line?;
        let result = serde_json::from_str(&line)
            .map_err(Into::into)
            .and_then(|value| state.call(&value));
        let reply = match result {
            Ok(result) => serde_json::json!({"result":result}),
            Err(error) => serde_json::json!({"error":error.to_string()}),
        };
        serde_json::to_writer(&mut output, &reply)?;
        output.write_all(b"\n")?;
        output.flush()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
