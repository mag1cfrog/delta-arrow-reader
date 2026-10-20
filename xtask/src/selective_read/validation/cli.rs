use std::{collections::BTreeSet, fs, path::PathBuf, process::Stdio};

use super::Worker;
use crate::{Result, selective_read::require};
use serde_json::{Value, json};

#[derive(clap::Args)]
struct Oracle {
    #[arg(long)]
    python: PathBuf,
    #[arg(long, default_value = "benches/selective_read")]
    harness: PathBuf,
}

#[derive(clap::Args)]
pub(crate) struct Prepare {
    #[command(flatten)]
    oracle: Oracle,
    #[arg(long)]
    fixtures: PathBuf,
    #[arg(long = "case", required = true)]
    cases: Vec<String>,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    workload: Option<PathBuf>,
    #[arg(long)]
    duplicate_in_literal: bool,
}

#[derive(clap::Args)]
pub(crate) struct Check {
    #[command(flatten)]
    oracle: Oracle,
    #[arg(long)]
    fixtures: PathBuf,
    #[arg(long)]
    reference: PathBuf,
    #[arg(long)]
    result: PathBuf,
    #[arg(long)]
    identity: PathBuf,
}

fn cases(values: &[String]) -> Result<()> {
    require(!values.is_empty(), "missing preparation case")?;
    require(
        values.iter().collect::<BTreeSet<_>>().len() == values.len(),
        "duplicate preparation case",
    )?;
    if values.len() > 1 {
        require(
            values.iter().all(|case| {
                !case.is_empty()
                    && ![".", ".."].contains(&case.as_str())
                    && case
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
            }),
            "invalid batch preparation case",
        )?;
    }
    Ok(())
}

fn finish(operation: &str, result: Result<Value>) -> Result<()> {
    match result {
        Ok(value) => {
            println!("{value}");
            Ok(())
        }
        Err(error) => {
            println!(
                "{}",
                json!({"status":"validation_failed","error":error.to_string(),"operation":operation})
            );
            Err(error)
        }
    }
}

pub(crate) fn prepare(args: Prepare) -> Result<()> {
    finish(
        "prepare",
        (|| {
            cases(&args.cases)?;
            require(!args.output.exists(), "reference output already exists")?;
            let batch = args.cases.len() > 1;
            if batch {
                fs::create_dir(&args.output)?;
            }
            let mut worker =
                Worker::oracle(&args.oracle.python, &args.oracle.harness, Stdio::inherit())?;
            let values = args.cases.iter().map(|case| {
            worker.call(&json!({"action":"prepare-reference","fixtures":args.fixtures,"case_id":case,
                "reference":if batch { args.output.join(case) } else { args.output.clone() },"reuse":false,
                "duplicate_in_literal":args.duplicate_in_literal,"workload":args.workload}))
        }).collect::<Result<Vec<_>>>()?;
            Ok(if batch {
                json!(values)
            } else {
                values.into_iter().next().ok_or("missing reference")?
            })
        })(),
    )
}

pub(crate) fn check(args: Check) -> Result<()> {
    finish(
        "check",
        (|| {
            Worker::oracle(&args.oracle.python, &args.oracle.harness, Stdio::inherit())?.call(
                &json!({"action":"check-export","fixtures":args.fixtures,"reference":args.reference,
                "result":args.result,"identity":args.identity}),
            )
        })(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn batch_cases_cannot_duplicate_or_escape_output() -> Result<()> {
        for invalid in [
            vec![],
            vec!["case", "case"],
            vec!["case", "../other"],
            vec!["case", "."],
            vec!["case", ".."],
            vec!["case", ""],
            vec!["case", "/absolute"],
        ] {
            assert!(cases(&invalid.into_iter().map(String::from).collect::<Vec<_>>()).is_err());
        }
        cases(&[
            "li.clustered.eq2-in20".into(),
            "wide.shuffled.eq2-in20".into(),
        ])
    }
}
