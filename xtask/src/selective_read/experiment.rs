use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    os::unix::process::{CommandExt, ExitStatusExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use rustix::process::{Pid, Signal, kill_process_group};
use serde::Deserialize;
use serde_json::{Value, json};

use super::{array, network, require, storage, text};
use crate::{Result, command::wait_with_timeout};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Request {
    directory: PathBuf,
    output: PathBuf,
    binary: PathBuf,
    table: String,
    selection: String,
    reader: Reader,
}

#[derive(Deserialize)]
#[serde(tag = "driver", rename_all = "kebab-case", deny_unknown_fields)]
enum Reader {
    NumericPages {
        column_type: String,
        validation: bool,
    },
    DeltaLength {
        mode: String,
        arrays: String,
        phase: String,
        cost_profile: Option<PathBuf>,
    },
}

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        // The launcher may exit before its timed reader or descendants.
        let _ = kill_process_group(Pid::from_child(&self.0), Signal::KILL);
        let _ = self.0.wait();
    }
}

fn execute(
    command: &mut Command,
    output: &Path,
    initialization: bool,
    timeout: Duration,
    ready: impl FnOnce() -> Result<()>,
) -> Result<()> {
    let mut process = Process(command.process_group(0).stdin(Stdio::piped()).spawn()?);
    let result = (|| {
        if initialization {
            let deadline = Instant::now() + timeout;
            while !output.join("reader/initialized.json").try_exists()? {
                require(process.0.try_wait()?.is_none(), "initialization failed")?;
                require(Instant::now() < deadline, "initialization deadline")?;
                thread::sleep(Duration::from_millis(10));
            }
            ready()?;
            writeln!(process.0.stdin.take().ok_or("missing reader input")?)?;
        }
        require(
            wait_with_timeout(&mut process.0, timeout)?.success(),
            "query failed",
        )
    })();
    if let Err(error) = &result {
        let code = process
            .0
            .try_wait()?
            .map(|status| status.code().unwrap_or(-status.signal().unwrap_or(0)));
        storage::save(
            &output.join("failure.json"),
            &json!({"error":error.to_string(),"returncode":code}),
        )?;
    }
    result
}

fn capture(directory: &Path, output: &Path) -> Result<Value> {
    network::run(network::Request::Finish {
        directory: directory.to_owned(),
        output: output.to_owned(),
    })
}

pub(super) fn run(input: Request) -> Result<Value> {
    fs::create_dir(&input.output)?;
    let output = fs::canonicalize(&input.output)?;
    let mut environment: BTreeMap<String, String> =
        serde_json::from_value(storage::reader_environment(&input.directory)?)?;
    let mut arguments = vec![input.binary.to_string_lossy().into_owned(), input.table];
    let (initialization, traced, stdout) = match input.reader {
        Reader::NumericPages {
            column_type,
            validation,
        } => {
            environment.remove("RUST_LOG");
            environment.remove("NUMERIC_PAGE_VALIDATE");
            if validation {
                environment.insert("NUMERIC_PAGE_VALIDATE".into(), "1".into());
                environment.insert(
                    "RUST_LOG".into(),
                    "delta_arrow_reader::diagnostics::intra_page=debug".into(),
                );
            }
            arguments.extend([column_type, input.selection]);
            (false, validation, "stdout.json")
        }
        Reader::DeltaLength {
            mode,
            arrays,
            phase,
            cost_profile,
        } => {
            arguments.extend([
                mode,
                input.selection,
                arrays,
                phase.clone(),
                cost_profile
                    .map_or_else(|| "auto".into(), |path| path.to_string_lossy().into_owned()),
            ]);
            (true, phase == "validate", "stdout.log")
        }
    };
    arguments.push(output.join("reader").to_string_lossy().into_owned());
    let prefix = storage::reader_prefix(&input.directory, None)?;
    let prefix: Vec<_> = array(&prefix)?.iter().map(text).collect::<Result<_>>()?;
    let mut command = Command::new(prefix.first().ok_or("empty reader prefix")?);
    command
        .args(&prefix[1..])
        .args(["/usr/bin/time", "-f", "%U %S %M", "-o"])
        .arg(output.join("resources.txt"))
        .args(arguments)
        .env_clear()
        .envs(environment)
        .stdout(fs::File::create_new(output.join(stdout))?)
        .stderr(fs::File::create_new(output.join("stderr.log"))?);
    network::control(&input.directory, true, traced, 10.0)?;
    execute(
        &mut command,
        &output,
        initialization,
        Duration::from_secs(180),
        || {
            let init = output.join("initialization");
            fs::create_dir(&init)?;
            capture(&input.directory, &init)?;
            network::control(&input.directory, true, traced, 10.0)?;
            Ok(())
        },
    )?;
    let capture = capture(&input.directory, &output)?;
    Ok(
        json!({"record":storage::read_json(&output.join("reader/record.json"))?,
        "network":capture,
        "process_resources":fs::read_to_string(output.join("resources.txt"))?.trim()}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descendants_stop_after_timeout_failure_and_success() -> Result<()> {
        for result in ["timeout", "7", "0"] {
            let root = tempfile::tempdir()?;
            let pid_file = root.path().join("child.pid");
            let mut command = Command::new("sh");
            command.args(["-c",r#"sleep 60 & echo $! > "$1"; if [ "$2" = timeout ]; then sleep 60; else exit "$2"; fi"#,
                "probe"]).arg(&pid_file).arg(result);
            let value = execute(
                &mut command,
                root.path(),
                false,
                Duration::from_secs(1),
                || Ok(()),
            );
            assert_eq!(value.is_ok(), result == "0");
            assert_eq!(root.path().join("failure.json").exists(), result != "0");
            let pid = fs::read_to_string(pid_file)?;
            let deadline = Instant::now() + Duration::from_secs(2);
            loop {
                let stat = fs::read_to_string(format!("/proc/{}/stat", pid.trim()));
                if stat.is_err() || stat?.split_whitespace().nth(2) == Some("Z") {
                    break;
                }
                assert!(Instant::now() < deadline, "reader survived its launcher");
                thread::sleep(Duration::from_millis(10));
            }
        }
        Ok(())
    }

    #[test]
    fn initialization_gate_waits_for_capture_and_propagates_early_exit_and_capture_failure()
    -> Result<()> {
        for failure in ["none", "exit", "capture", "timeout"] {
            let root = tempfile::tempdir()?;
            let output = root.path();
            let mut command = Command::new("sh");
            command
                .args([
                    "-c",
                    r#"
if [ "$2" = exit ]; then exit 7; fi
mkdir "$1/reader"
if [ "$2" = timeout ]; then sleep 60; fi
printf '{}' > "$1/reader/initialized.json"
read -r gate
test -f "$1/captured"
printf '{"status":"complete"}' > "$1/reader/record.json"
"#,
                    "probe",
                ])
                .arg(output)
                .arg(failure);
            let result = execute(&mut command, output, true, Duration::from_secs(1), || {
                if failure == "capture" {
                    return Err("capture failed".into());
                }
                fs::write(output.join("captured"), b"")?;
                Ok(())
            });
            assert_eq!(result.is_ok(), failure == "none");
            assert_eq!(
                output.join("reader/record.json").exists(),
                failure == "none"
            );
            assert_eq!(output.join("failure.json").exists(), failure != "none");
        }
        Ok(())
    }
}
