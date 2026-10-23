use std::{
    fs::File,
    io::{self, Write},
    process::Stdio,
    time::Duration,
};

use serde_json::json;
use snafu::{ResultExt, ensure};

use super::{ProcessFixture, check_error, run_cli};
use crate::{
    command::{output_with_redirected_stdout, status_with_timeout},
    error::{FileSnafu, InvalidInputSnafu, IoSnafu, Result},
};

impl ProcessFixture<'_> {
    pub(super) fn check_failure_reporting_without_threads(&self) -> Result<()> {
        for machine in [false, true] {
            for filter in ["warn", "off"] {
                for args in [
                    vec!["--secret-invalid-argument"],
                    vec!["scan", "secret-table", "--limit", "secret-invalid-limit"],
                ] {
                    let mut command = self.command();
                    command.args(args).env("RUST_LOG", filter);
                    if machine {
                        command.arg("--diagnostics=json");
                    }
                    let expected = run_cli(&mut command)?;
                    let actual = run_cli(
                        command
                            .env("LD_PRELOAD", self.fault_library)
                            .env("DAR_TEST_REJECT_THREADS", "1"),
                    )?;
                    ensure!(
                        expected.status.code() == Some(2)
                            && actual.status == expected.status
                            && actual.stdout.is_empty()
                            && actual.stderr == expected.stderr
                            && (filter == "off" && !machine || !actual.stderr.is_empty()),
                        InvalidInputSnafu {
                            message: "Thread exhaustion changed the failure status or diagnostic"
                        }
                    );
                }
            }

            for (argument, status) in [("--secret-invalid-argument", 2), ("capabilities", 1)] {
                let (_reader, mut writer) = io::pipe().context(IoSnafu {
                    operation: "create a blocked stderr pipe",
                })?;
                let capacity = rustix::pipe::fcntl_getpipe_size(&writer)
                    .map_err(io::Error::from)
                    .context(IoSnafu {
                        operation: "read the stderr pipe capacity",
                    })?;
                writer.write_all(&vec![0; capacity]).context(IoSnafu {
                    operation: "fill the stderr pipe",
                })?;
                let mut command = self.command();
                command
                    .arg(argument)
                    .env("LD_PRELOAD", self.fault_library)
                    .env("DAR_TEST_REJECT_THREADS", "1")
                    .stdout(Stdio::null())
                    .stderr(writer);
                if machine {
                    command.arg("--diagnostics=json");
                }
                ensure!(
                    status_with_timeout(&mut command, Duration::from_secs(3))?.code()
                        == Some(status),
                    InvalidInputSnafu {
                        message: "Blocked stderr changed the failure status under thread exhaustion"
                    }
                );
            }
        }
        Ok(())
    }

    pub(super) fn check_stdout_failures(&self) -> Result<()> {
        let table = self.create_metadata_table(
            json!([
                {"name": "id", "type": "integer", "nullable": true, "metadata": {}},
            ]),
            1,
        )?;
        for action in ["--help", "--version", "inspect", "scan"] {
            let mut command = self.command();
            command.arg(action);
            if matches!(action, "inspect" | "scan") {
                command.arg(&table);
            }
            let sink = File::options()
                .write(true)
                .open("/dev/full")
                .context(FileSnafu { path: "/dev/full" })?;
            let output = output_with_redirected_stdout(
                &mut command,
                Stdio::from(sink),
                Duration::from_secs(15),
            )?;
            check_error(&output, 3, "execution", "output_write")?;
        }
        for action in ["inspect", "scan"] {
            let (reader, writer) = io::pipe().context(IoSnafu {
                operation: "create the broken-pipe fixture",
            })?;
            drop(reader);
            let output = output_with_redirected_stdout(
                self.command().arg(action).arg(&table),
                Stdio::from(writer),
                Duration::from_secs(15),
            )?;
            check_error(&output, 3, "execution", "output_write")?;
        }
        let sink = File::options()
            .write(true)
            .open("/dev/full")
            .context(FileSnafu { path: "/dev/full" })?;
        let status = status_with_timeout(
            self.command()
                .arg("--version")
                .stdout(sink.try_clone().context(FileSnafu { path: "/dev/full" })?)
                .stderr(sink),
            Duration::from_secs(15),
        )?;
        ensure!(
            status.code() == Some(3),
            InvalidInputSnafu {
                message: format!("expected exit 3 when both output streams fail: {status}")
            }
        );
        Ok(())
    }
}
