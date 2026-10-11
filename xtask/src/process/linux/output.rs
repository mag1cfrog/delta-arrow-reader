use std::{fs::File, io, process::Stdio, time::Duration};

use serde_json::json;
use snafu::{ResultExt, ensure};

use super::{ProcessFixture, check_error};
use crate::{
    command::{output_with_redirected_stdout, status_with_timeout},
    error::{FileSnafu, InvalidInputSnafu, IoSnafu, Result},
};

impl ProcessFixture<'_> {
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
