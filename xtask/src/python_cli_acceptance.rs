//! Process checks against the executable installed by a wheel, on every wheel platform.

use std::{
    env, fs,
    io::{Read, Seek},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

use arrow_array::{RecordBatch, StringArray};
use clap::Args;
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use serde_json::{Value, json};

use crate::{
    Result,
    command::{KillOnDrop, output_with_redirected_stdout, output_with_timeout, wait_with_timeout},
    process::check_diagnostic,
};

const TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Args)]
pub(crate) struct Arguments {
    binary: PathBuf,
    #[arg(long)]
    expected_version: String,
    /// Partitioned external-writer fixture, including table/ and expected.arrow.
    #[arg(long)]
    fixture: PathBuf,
    /// CPython's base install provides the supported Windows MSVC runtime.
    #[arg(long)]
    python_base_prefix: PathBuf,
}

impl Arguments {
    fn command(&self, directory: &Path) -> Result<Command> {
        let mut command = Command::new(&self.binary);
        command.current_dir(directory).stdin(Stdio::null());
        for (key, _) in env::vars_os() {
            let name = key.to_string_lossy();
            if name.starts_with("LD_") || name.starts_with("DYLD_") || name == "RUST_LOG" {
                command.env_remove(key);
            }
        }
        #[cfg(windows)]
        {
            let windows = PathBuf::from(env::var_os("SystemRoot").ok_or("missing SystemRoot")?);
            command.env(
                "PATH",
                env::join_paths([
                    windows.join("System32"),
                    windows,
                    self.python_base_prefix.clone(),
                ])?,
            );
        }
        #[cfg(not(windows))]
        command.env("PATH", "/usr/bin:/bin");
        Ok(command)
    }

    pub(crate) fn run(mut self) -> Result<()> {
        self.binary = self.binary.canonicalize()?;
        self.fixture = self.fixture.canonicalize()?;
        let directory = tempfile::Builder::new()
            .prefix("secret installed CLI ")
            .tempdir()?;
        let root = directory.path();
        let version = output_with_timeout(self.command(root)?.arg("--version"), TIMEOUT)?;
        assert!(version.status.success(), "{version:?}");
        assert!(version.stderr.is_empty(), "{version:?}");
        assert_eq!(
            String::from_utf8(version.stdout)?,
            format!("delta-arrow-reader {}\n", self.expected_version)
        );
        for subcommand in [None, Some("inspect"), Some("scan")] {
            let mut command = self.command(root)?;
            if let Some(name) = subcommand {
                command.arg(name);
            }
            let output = output_with_timeout(command.arg("--help"), TIMEOUT)?;
            assert!(
                output.status.success() && output.stderr.is_empty(),
                "{output:?}"
            );
            let text = String::from_utf8(output.stdout)?;
            let prefix = subcommand.map_or(String::new(), |name| format!(" {name}"));
            assert!(
                text.contains(&format!("Usage: delta-arrow-reader{prefix} ")),
                "{text}"
            );
            assert!(!text.contains("secret"), "{text}");
        }
        let table = self.fixture.join("table");
        let malformed = root.join("secret-options.json");
        fs::write(&malformed, b"{\"token\": \"secret-value\",")?;
        for action in ["inspect", "scan"] {
            for (path, options, status, phase, code) in [
                (
                    table.clone(),
                    vec![
                        "--storage-options-file".into(),
                        malformed.clone().into_os_string(),
                    ],
                    2,
                    "configuration",
                    "invalid_input_json",
                ),
                (
                    root.join("secret-missing-table"),
                    vec![],
                    1,
                    "table_location",
                    "invalid_table_location",
                ),
                (
                    table.clone(),
                    vec!["--table-version".into(), "secret-version".into()],
                    2,
                    "configuration",
                    "invalid_cli_argument",
                ),
            ] {
                let output = output_with_timeout(
                    self.command(root)?.arg(action).arg(path).args(options),
                    TIMEOUT,
                )?;
                assert_eq!(output.status.code(), Some(status), "{output:?}");
                assert!(output.stdout.is_empty(), "{output:?}");
                check_diagnostic(&output.stderr, phase, code)?;
                assert!(!String::from_utf8_lossy(&output.stderr).contains("Traceback"));
            }
        }
        for action in ["--help", "inspect", "scan"] {
            let (reader, writer) = std::io::pipe()?;
            drop(reader);
            let mut command = self.command(root)?;
            command.arg(action);
            if action != "--help" {
                command.arg(&table);
            }
            let output = output_with_redirected_stdout(&mut command, writer.into(), TIMEOUT)?;
            assert_eq!(output.status.code(), Some(3), "{output:?}");
            check_diagnostic(&output.stderr, "execution", "output_write")?;
        }
        let large = root.join("table");
        write_large_fixture(&table, &large)?;
        #[cfg(unix)]
        let signals = [Some(libc::SIGINT), Some(libc::SIGTERM)];
        #[cfg(not(unix))]
        let signals = [None];
        for signal in signals {
            self.check_termination(root, &large, signal)?;
        }
        println!("Installed Python CLI process acceptance passed");
        Ok(())
    }

    fn check_termination(&self, directory: &Path, table: &Path, signal: Option<i32>) -> Result<()> {
        let mut stderr = tempfile::tempfile()?;
        let mut process = KillOnDrop {
            child: self
                .command(directory)?
                .arg("scan")
                .arg(table)
                .stdout(Stdio::piped())
                .stderr(stderr.try_clone()?)
                .spawn()?,
        };
        let mut stdout = process.child.stdout.take().ok_or("missing child stdout")?;
        let (send, receive) = mpsc::channel();
        let reader = thread::spawn(move || {
            let mut byte = [0];
            let result = stdout.read_exact(&mut byte);
            let _ = send.send((result, byte, stdout));
        });
        let (result, byte, _stdout) = receive.recv_timeout(TIMEOUT)?;
        result?;
        assert_eq!(byte, [0xff]);
        assert!(
            process.child.try_wait()?.is_none(),
            "scan exited before termination"
        );
        #[cfg(unix)]
        if let Some(signal) = signal {
            let pid = i32::try_from(process.child.id())?;
            // The owned child is still running; send only the signal under test.
            if unsafe { libc::kill(pid, signal) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
        }
        if signal.is_none() {
            process.child.kill()?;
        }
        let status = wait_with_timeout(&mut process.child, TIMEOUT)?;
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), signal);
        }
        assert!(!status.success());
        stderr.rewind()?;
        let mut text = String::new();
        stderr.read_to_string(&mut text)?;
        assert!(text.is_empty(), "{text}");
        reader.join().map_err(|_| "stdout reader panicked")?;
        Ok(())
    }
}

fn write_large_fixture(source: &Path, destination: &Path) -> Result<()> {
    const LOG: &str = "_delta_log/00000000000000000000.json";
    let actions: Vec<Value> = fs::read_to_string(source.join(LOG))?
        .lines()
        .map(serde_json::from_str)
        .collect::<std::result::Result<_, _>>()?;
    let mut add = actions
        .iter()
        .find_map(|action| action.get("add"))
        .ok_or("fixture has no file")?
        .clone();
    let file = fs::File::open(source.join(add["path"].as_str().ok_or("missing file path")?))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    fs::create_dir_all(destination.join("_delta_log"))?;
    let path = destination.join("part.parquet");
    let mut writer = None;
    let mut bytes = 0;
    for batch in reader {
        let batch = batch?;
        let mut columns = batch.columns().to_vec();
        columns[batch.schema().index_of("label")?] =
            Arc::new(StringArray::from(vec!["x".repeat(32768); batch.num_rows()]));
        let batch = RecordBatch::try_new(batch.schema(), columns)?;
        bytes += batch.get_array_memory_size();
        if writer.is_none() {
            writer = Some(ArrowWriter::try_new(
                fs::File::create(&path)?,
                batch.schema(),
                None,
            )?);
        }
        writer.as_mut().ok_or("missing writer")?.write(&batch)?;
    }
    writer.ok_or("fixture has no rows")?.close()?;
    assert!(
        bytes > 1024 * 1024,
        "fixture must fill the pipe before termination"
    );
    add["path"] = json!("part.parquet");
    add["size"] = json!(fs::metadata(path)?.len());
    add.as_object_mut()
        .ok_or("invalid add action")?
        .remove("stats");
    let mut kept: Vec<_> = actions
        .into_iter()
        .filter(|action| action.get("protocol").is_some() || action.get("metaData").is_some())
        .collect();
    kept.push(json!({"add":add}));
    fs::write(
        destination.join(LOG),
        kept.iter()
            .map(serde_json::to_string)
            .collect::<std::result::Result<Vec<_>, _>>()?
            .join("\n")
            + "\n",
    )?;
    Ok(())
}
