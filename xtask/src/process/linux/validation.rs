use std::{
    ffi::OsStr,
    fs::File,
    io,
    net::TcpListener,
    os::unix::ffi::OsStrExt,
    path::PathBuf,
    process::{Command, Output, Stdio},
    time::Duration,
};

use rustix::{
    event::{PollFd, PollFlags, Timespec, poll},
    pty::{OpenptFlags, ioctl_tiocgptpeer, openpt, unlockpt},
};
use serde_json::json;
use snafu::{ResultExt, ensure};

use super::{
    ProcessFixture, check_error, check_inspection, describe_output, run_cli, write_fixture,
};
use crate::{
    command::output_with_redirected_stdout,
    error::{InvalidInputSnafu, IoSnafu, Result},
};

const JSON_SIZE_LIMIT: usize = 1024 * 1024;
const TRUE_PREDICATE: &str = r#"{"op":"constant","value":true}"#;
const INVALID_UNSIGNED_NUMBERS: [&str; 12] = [
    "",
    "-1",
    "+1",
    " 1",
    "1 ",
    "1\n",
    "1.0",
    "1e2",
    "0x1",
    "18446744073709551616",
    "secret",
    "\u{0661}",
];

impl ProcessFixture<'_> {
    fn validation_command(&self) -> Command {
        let mut command = self.command();
        command
            .env("LD_PRELOAD", self.fault_library)
            .env("DAR_TEST_REJECT_THREADS", "1");
        command
    }

    pub(super) fn check_inspect_input_validation(&self) -> Result<()> {
        let probe = ConnectionProbe::new()?;
        let table = probe.table_url.as_str();
        for args in [
            vec![],
            vec!["secret-command"],
            vec!["inspect"],
            vec!["inspect", table, "secret-extra"],
            vec!["inspect", "--secret-option", table],
            vec!["inspect", "--table-version"],
            vec!["inspect", "--storage-options-file"],
            vec!["inspect", "--predicate-file", "secret-missing", table],
            vec![
                "inspect",
                "--execution-options-file",
                "secret-missing",
                table,
            ],
            vec!["inspect", "--target-partitions", "1", table],
            vec![
                "inspect",
                "--table-version",
                "0",
                "--table-version",
                "1",
                table,
            ],
            vec![
                "inspect",
                "--storage-options-file",
                "secret-missing",
                "--storage-options-file",
                "secret-missing",
                table,
            ],
            vec!["inspect", "--", table, "extra"],
        ] {
            check_error(
                &run_cli(self.validation_command().args(args))?,
                2,
                "configuration",
                "invalid_cli_argument",
            )?;
        }
        check_error(
            &run_cli(
                self.validation_command()
                    .arg("inspect")
                    .arg(OsStr::from_bytes(b"secret-\xff")),
            )?,
            2,
            "configuration",
            "invalid_cli_argument",
        )?;
        for version in INVALID_UNSIGNED_NUMBERS {
            check_error(
                &run_cli(self.validation_command().args([
                    "inspect",
                    "--table-version",
                    version,
                    table,
                ]))?,
                2,
                "configuration",
                "invalid_cli_argument",
            )?;
        }
        let oversized = padded_json(b"{}", JSON_SIZE_LIMIT + 1);
        for content in [
            b"".as_slice(),
            b"secret",
            b"{",
            b"{}secret",
            b"{}{}",
            b"\xff",
            br#""secret""#,
            b"[]",
            b"null",
            br#"{"secret":1}"#,
            br#"{"secret":true}"#,
            br#"{"secret":null}"#,
            br#"{"secret":[]}"#,
            br#"{"secret":{}}"#,
            br#"{"secret":"a","secret":"b"}"#,
            br#"{"a":"a","\u0061":"b"}"#,
            b"{\"secret\":\"\xff\"}",
            &oversized,
        ] {
            let path = self.write_input_file(content)?;
            check_error(
                &run_cli(
                    self.validation_command()
                        .args(["inspect", "--storage-options-file"])
                        .arg(path)
                        .arg(table),
                )?,
                2,
                "configuration",
                "invalid_input_json",
            )?;
        }
        for path in [
            self.directory.path().join("secret-missing"),
            self.directory.path().to_owned(),
            PathBuf::from("-"),
        ] {
            check_error(
                &run_cli(
                    self.validation_command()
                        .args(["inspect", "--storage-options-file"])
                        .arg(path)
                        .arg(table),
                )?,
                2,
                "configuration",
                "input_file_io",
            )?;
        }
        probe.check_no_connections()
    }

    pub(super) fn check_scan_input_validation(&self) -> Result<()> {
        let probe = ConnectionProbe::new()?;
        let table = probe.table_url.as_str();
        for args in [
            vec![],
            vec![table, "secret-extra"],
            vec!["--column"],
            vec!["--limit"],
            vec!["--target-partitions"],
            vec![
                "--target-partitions",
                "1",
                "--target-partitions",
                "2",
                table,
            ],
            vec!["--predicate-file"],
            vec![
                "--predicate-file",
                "secret-missing",
                "--predicate-file",
                "secret-missing",
                table,
            ],
            vec!["--no-columns", "--column", "secret", table],
            vec!["--no-columns", "--no-columns", table],
            vec!["--limit", "0", "--limit", "1", table],
            vec!["--column", "id", "value", table],
            vec!["--table-version", "0", "--table-version", "1", table],
        ] {
            check_error(
                &run_cli(self.validation_command().arg("scan").args(args))?,
                2,
                "configuration",
                "invalid_cli_argument",
            )?;
        }
        let invalid_utf8 = OsStr::from_bytes(b"secret-\xff");
        check_error(
            &run_cli(self.validation_command().arg("scan").arg(invalid_utf8))?,
            2,
            "configuration",
            "invalid_cli_argument",
        )?;
        for option in ["--target-partitions", "--predicate-file", "--column"] {
            check_error(
                &run_cli(
                    self.validation_command()
                        .args(["scan", option])
                        .arg(invalid_utf8)
                        .arg(table),
                )?,
                2,
                "configuration",
                "invalid_cli_argument",
            )?;
        }
        for limit in INVALID_UNSIGNED_NUMBERS {
            for option in ["--limit", "--target-partitions"] {
                check_error(
                    &run_cli(
                        self.validation_command()
                            .args(["scan", option, limit, table]),
                    )?,
                    2,
                    "configuration",
                    "invalid_cli_argument",
                )?;
            }
        }
        for count in ["0", "000", "-0", "+0"] {
            check_error(
                &run_cli(self.validation_command().args([
                    "scan",
                    "--target-partitions",
                    count,
                    table,
                ]))?,
                2,
                "configuration",
                "invalid_cli_argument",
            )?;
        }
        let path = self.write_input_file(b"secret")?;
        check_error(
            &run_cli(
                self.validation_command()
                    .args(["scan", "--storage-options-file"])
                    .arg(path)
                    .arg(table),
            )?,
            2,
            "configuration",
            "invalid_input_json",
        )?;

        let (master, terminal) = open_terminal()?;
        let output = output_with_redirected_stdout(
            self.validation_command().args(["scan", table]),
            Stdio::from(terminal.try_clone().context(IoSnafu {
                operation: "duplicate the test terminal",
            })?),
            Duration::from_secs(15),
        )?;
        check_no_terminal_output(&master)?;
        check_error(&output, 2, "configuration", "invalid_cli_argument")?;
        probe.check_no_connections()
    }

    pub(super) fn check_execution_options_validation(&self) -> Result<()> {
        let probe = ConnectionProbe::new()?;
        let table = probe.table_url.as_str();
        for args in [
            vec!["--execution-options-file"],
            vec![
                "--execution-options-file",
                "secret-missing",
                "--execution-options-file",
                "secret-missing",
                table,
            ],
        ] {
            check_error(
                &run_cli(self.validation_command().arg("scan").args(args))?,
                2,
                "configuration",
                "invalid_cli_argument",
            )?;
        }
        check_error(
            &run_cli(
                self.validation_command()
                    .args(["scan", "--execution-options-file"])
                    .arg(OsStr::from_bytes(b"secret-\xff"))
                    .arg(table),
            )?,
            2,
            "configuration",
            "invalid_cli_argument",
        )?;
        let oversized = padded_json(b"{}", JSON_SIZE_LIMIT + 1);
        for content in [
            b"".as_slice(),
            b"\xff",
            b"null",
            b"[]",
            b"{}{}",
            &oversized,
            br#"{"secret-extra":1}"#,
            br#"{"parquet_backend":"secret-backend"}"#,
            br#"{"parquet_backend":{"direct":null}}"#,
            br#"{"prefetch_files_per_partition":null}"#,
            br#"{"prefetch_files_per_partition":1e0}"#,
            br#"{"prefetch_files_per_partition":18446744073709551616}"#,
            br#"{"parquet_metadata_size_hint_bytes":null,"parquet_metadata_size_hint_bytes":null}"#,
        ] {
            let path = self.write_input_file(content)?;
            check_json_error(&run_cli(
                self.validation_command()
                    .args(["scan", "--execution-options-file"])
                    .arg(path)
                    .arg(table),
            )?)?;
        }
        for (field, values) in [
            (
                "max_concurrent_file_reads_per_scan",
                [0, isize::MAX as usize].as_slice(),
            ),
            (
                "max_concurrent_file_reads_per_partition",
                &[0, isize::MAX as usize],
            ),
            (
                "output_buffer_batches_per_partition",
                &[0, isize::MAX as usize],
            ),
            ("parquet_metadata_size_hint_bytes", &[0]),
            ("parquet_full_file_read_threshold_bytes", &[0]),
        ] {
            for value in values {
                let content = json!({field: value}).to_string();
                let path = self.write_input_file(content.as_bytes())?;
                check_error(
                    &run_cli(
                        self.validation_command()
                            .args(["scan", "--execution-options-file"])
                            .arg(path)
                            .arg(table),
                    )?,
                    2,
                    "configuration",
                    "invalid_configuration",
                )?;
            }
        }
        self.check_invalid_input_files("--execution-options-file", table)?;
        probe.check_no_connections()
    }

    pub(super) fn check_execution_options_size_and_literal_dash(&self) -> Result<()> {
        let table = self
            .repository
            .join("tests/reader/fixtures/external_writer/corpus/partitioned/table");
        let boundary = padded_json(b"{}", JSON_SIZE_LIMIT);
        for content in [b"{}".as_slice(), &boundary] {
            let path = self.write_input_file(content)?;
            check_scan_completion(&run_cli(
                self.command()
                    .args(["scan", "--execution-options-file"])
                    .arg(path)
                    .args(["--limit", "0"])
                    .arg(&table),
            )?)?;
        }
        write_fixture(
            &self.directory.path().join("-"),
            br#"{"parquet_metadata_size_hint_bytes":null}"#,
        )?;
        check_scan_completion(&run_cli(
            self.command()
                .args(["scan", "--execution-options-file", "-", "--limit", "0"])
                .arg(&table),
        )?)
    }

    pub(super) fn check_predicate_json_validation(&self) -> Result<()> {
        let probe = ConnectionProbe::new()?;
        let table = probe.table_url.as_str();
        let too_deep = format!(
            "{}{TRUE_PREDICATE}{}",
            r#"{"op":"not","arg":"#.repeat(32),
            "}".repeat(32)
        );
        let too_wide = format!(
            r#"{{"op":"and","args":[{}]}}"#,
            [TRUE_PREDICATE; 1024].join(",")
        );
        let extra_json = format!("{TRUE_PREDICATE}{{}}");
        let oversized = padded_json(TRUE_PREDICATE.as_bytes(), JSON_SIZE_LIMIT + 1);
        for content in [
            b"".as_slice(),
            b"\xff",
            b"{}",
            b"null",
            b"[]",
            extra_json.as_bytes(),
            br#"{"op":"constant","value":true,"value":false}"#,
            br#"{"op":"is_null","column":"secret-column","secret-extra":0}"#,
            br#"{"op":"eq","column":"secret-column","value":{"type":"int64","value":"secret-value"}}"#,
            br#"{"op":"eq","column":"secret-column","value":["utf8","secret-value"]}"#,
            too_deep.as_bytes(),
            too_wide.as_bytes(),
            &oversized,
        ] {
            let path = self.write_input_file(content)?;
            check_json_error(&run_cli(
                self.validation_command()
                    .args(["scan", "--predicate-file"])
                    .arg(path)
                    .arg(table),
            )?)?;
        }
        self.check_invalid_input_files("--predicate-file", table)?;
        probe.check_no_connections()
    }

    pub(super) fn check_predicate_size_and_literal_dash(&self) -> Result<()> {
        let table = self
            .repository
            .join("tests/reader/fixtures/external_writer/corpus/partitioned/table");
        let boundary = padded_json(TRUE_PREDICATE.as_bytes(), JSON_SIZE_LIMIT);
        for content in [TRUE_PREDICATE.as_bytes(), &boundary] {
            let path = self.write_input_file(content)?;
            check_scan_completion(&run_cli(
                self.command()
                    .args(["scan", "--predicate-file"])
                    .arg(path)
                    .args(["--limit", "0"])
                    .arg(&table),
            )?)?;
        }
        write_fixture(&self.directory.path().join("-"), TRUE_PREDICATE)?;
        check_scan_completion(&run_cli(
            self.command()
                .args(["scan", "--predicate-file", "-", "--limit", "0"])
                .arg(&table),
        )?)
    }

    pub(super) fn check_storage_options_size_limit(&self) -> Result<()> {
        let table = self.create_metadata_table(json!([]), 1)?;
        let boundary = padded_json(b"{}", JSON_SIZE_LIMIT);
        for content in [b"{}".as_slice(), &boundary] {
            let path = self.write_input_file(content)?;
            check_inspection(&run_cli(
                self.command()
                    .args(["inspect", "--storage-options-file"])
                    .arg(path)
                    .arg(&table),
            )?)?;
        }
        check_error(
            &run_cli(
                self.command()
                    .args(["inspect", "--storage-options-file", "/dev/zero"])
                    .arg(&table),
            )?,
            2,
            "configuration",
            "invalid_input_json",
        )?;
        let content = json!({"secret": "\u{00e9}".repeat(JSON_SIZE_LIMIT / 2)}).to_string();
        ensure!(
            content.chars().count() < JSON_SIZE_LIMIT && content.len() > JSON_SIZE_LIMIT,
            InvalidInputSnafu {
                message: "multibyte fixture must cross the byte limit but not the character limit"
            }
        );
        let path = self.write_input_file(content.as_bytes())?;
        check_error(
            &run_cli(
                self.command()
                    .args(["inspect", "--storage-options-file"])
                    .arg(path)
                    .arg(&table),
            )?,
            2,
            "configuration",
            "invalid_input_json",
        )?;
        Ok(())
    }

    fn check_invalid_input_files(&self, option: &str, table: &str) -> Result<()> {
        for (path, code) in [
            (
                self.directory.path().join("secret-missing"),
                "input_file_io",
            ),
            (self.directory.path().to_owned(), "input_file_io"),
            (PathBuf::from("-"), "input_file_io"),
            (PathBuf::from("/dev/zero"), "invalid_input_json"),
        ] {
            check_error(
                &run_cli(
                    self.validation_command()
                        .args(["scan", option])
                        .arg(path)
                        .arg(table),
                )?,
                2,
                "configuration",
                code,
            )?;
        }
        Ok(())
    }
}

fn padded_json(content: &[u8], size: usize) -> Vec<u8> {
    let mut bytes = content.to_vec();
    bytes.resize(size, b' ');
    bytes
}

fn check_json_error(output: &Output) -> Result<()> {
    let diagnostic = check_error(output, 2, "configuration", "invalid_input_json")?;
    ensure!(
        diagnostic["message"] == "Invalid JSON input.",
        InvalidInputSnafu {
            message: format!("unexpected JSON input message: {diagnostic}")
        }
    );
    Ok(())
}

fn check_scan_completion(output: &Output) -> Result<()> {
    ensure!(
        output.status.success()
            && output.stderr.is_empty()
            && output.stdout.ends_with(&[255, 255, 255, 255, 0, 0, 0, 0]),
        InvalidInputSnafu {
            message: format!(
                "expected a successful scan with its IPC end marker: {}",
                describe_output(output)
            )
        }
    );
    Ok(())
}

struct ConnectionProbe {
    listener: TcpListener,
    table_url: String,
}

impl ConnectionProbe {
    fn new() -> Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).context(IoSnafu {
            operation: "bind the table access probe",
        })?;
        listener.set_nonblocking(true).context(IoSnafu {
            operation: "make the table access probe nonblocking",
        })?;
        let address = listener.local_addr().context(IoSnafu {
            operation: "read the table access probe address",
        })?;
        Ok(Self {
            listener,
            table_url: format!("http://{address}/secret-table"),
        })
    }

    fn check_no_connections(&self) -> Result<()> {
        match self.listener.accept() {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(()),
            Err(error) => Err(error).context(IoSnafu {
                operation: "check for table access during input validation",
            }),
            Ok(_) => InvalidInputSnafu {
                message: "invalid input caused a connection to the table endpoint",
            }
            .fail(),
        }
    }
}

fn open_terminal() -> Result<(File, File)> {
    let flags = OpenptFlags::RDWR | OpenptFlags::NOCTTY | OpenptFlags::CLOEXEC;
    let master = openpt(flags).map_err(io::Error::from).context(IoSnafu {
        operation: "open the test pseudoterminal",
    })?;
    unlockpt(&master)
        .map_err(io::Error::from)
        .context(IoSnafu {
            operation: "unlock the test pseudoterminal",
        })?;
    let terminal = ioctl_tiocgptpeer(&master, flags)
        .map_err(io::Error::from)
        .context(IoSnafu {
            operation: "open the test terminal",
        })?;
    Ok((File::from(master), File::from(terminal)))
}

fn check_no_terminal_output(master: &File) -> Result<()> {
    let mut descriptors = [PollFd::new(master, PollFlags::IN)];
    let ready = poll(&mut descriptors, Some(&Timespec::default()))
        .map_err(io::Error::from)
        .context(IoSnafu {
            operation: "check for binary output on the test terminal",
        })?;
    ensure!(
        ready == 0,
        InvalidInputSnafu {
            message: "binary output reached a terminal"
        }
    );
    Ok(())
}

#[cfg(test)]
mod tests;
