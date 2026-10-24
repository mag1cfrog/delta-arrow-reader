use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::{Command, Output, Stdio},
    time::Duration,
};

use snafu::{OptionExt, ResultExt, ensure};

use super::TestImageArgs;
use crate::{
    command::output_with_timeout,
    error::{FileSnafu, InvalidInputSnafu, Result},
    http_fixture::{
        HttpTestServer,
        table::HttpTable,
        tls::{create_certificates, load_server_config},
    },
    platform::Architecture,
    process::{check_error, check_inspection},
    scan::{
        corpus::check_corpus,
        ipc::{IpcTable, check_arrow_output, check_completed_count_query},
    },
};

struct ImageFixture {
    image: String,
    architecture: Architecture,
    name: String,
    corpus: PathBuf,
    directory: tempfile::TempDir,
}

pub(super) fn run(args: TestImageArgs) -> Result<()> {
    let inspection = successful_output(
        Command::new("docker")
            .args([
                "image",
                "inspect",
                "--format",
                "{{.Id}} {{.Os}} {{.Architecture}}",
                "--",
            ])
            .arg(&args.image),
    )?;
    let image = image_id(&inspection.stdout, args.architecture)?;
    let corpus = args
        .repository
        .join("crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus");
    let corpus = fs::canonicalize(&corpus).context(FileSnafu { path: &corpus })?;
    let directory = tempfile::Builder::new()
        .prefix("dar-image-check-")
        .tempdir()
        .context(FileSnafu {
            path: std::env::temp_dir(),
        })?;
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o755)).context(
        FileSnafu {
            path: directory.path(),
        },
    )?;
    for path in [&corpus, directory.path()] {
        ensure!(
            path.to_str().is_some_and(|path| !path.contains(',')),
            InvalidInputSnafu {
                message: "image fixture mount paths must be UTF-8 and cannot contain commas",
            }
        );
    }
    let fixture = ImageFixture {
        image,
        architecture: args.architecture,
        name: directory
            .path()
            .file_name()
            .context(InvalidInputSnafu {
                message: "image fixture directory must have a name",
            })?
            .to_string_lossy()
            .into_owned(),
        corpus,
        directory,
    };
    eprintln!("Image check: defaults, help, version, inspection, and redacted errors");
    fixture.check_basic_commands(&args.expected_version)?;
    eprintln!("Image check: pinned SQL aggregate and complete result");
    fixture.check_query()?;
    eprintln!("Image check: offline agent discovery, pinned reads, repair, and saved results");
    fixture.check_agent_workflow(&args.expected_version)?;
    eprintln!("Image check: Spark corpus, projection, limits, predicates, and execution options");
    check_corpus(&fixture.corpus, fixture.directory.path(), &|| {
        let mut command = fixture.dar_command();
        command.arg("scan");
        command
    })?;
    eprintln!("Image check: HTTPS trust and runtime custom CA");
    fixture.check_https()?;
    eprintln!("Image check: closing the CLI's output pipe fails the scan");
    fixture.check_closed_stdout()?;
    eprintln!("Image check: SIGINT and SIGTERM reach dar as PID 1");
    fixture.check_signals()?;
    println!("CLI image runtime acceptance passed for {}", fixture.image);
    Ok(())
}

impl ImageFixture {
    fn check_agent_workflow(&self, version: &str) -> Result<()> {
        let directory = self.directory.path().join("agent-workflow");
        fs::create_dir(&directory).context(FileSnafu { path: &directory })?;
        // Match the mount owner so the host can verify private (0600) result artifacts.
        let user = format!("{}:{}", unsafe { libc::geteuid() }, unsafe {
            libc::getegid()
        });
        let command = |network: &str| {
            let mut command = Command::new("docker");
            command
                .args([
                    "run",
                    "--rm",
                    "--read-only",
                    "--cap-drop=ALL",
                    "--security-opt=no-new-privileges",
                    "--security-opt=label=disable",
                ])
                .args([
                    "--name",
                    &self.name,
                    "--network",
                    network,
                    "--user",
                    &user,
                    "--mount",
                ])
                .arg(format!(
                    "type=bind,src={},dst={}",
                    directory.display(),
                    directory.display()
                ))
                .arg("--workdir")
                .arg(&directory)
                .args([
                    "--entrypoint=/usr/bin/env",
                    &self.image,
                    "-i",
                    "/usr/local/bin/dar",
                ]);
            Ok(command)
        };
        crate::packaged_cli::check_agent_workflow(
            &|| command("none"),
            &|| command("host"),
            &directory,
            "dar",
            version,
        )
        .map_err(|error| {
            InvalidInputSnafu {
                message: format!("Packaged image acceptance failed: {error}"),
            }
            .build()
        })
    }

    fn container_command(&self, action: &str) -> Command {
        let mut command = Command::new("docker");
        command
            .args([action, "--name", &self.name, "--platform"])
            .arg(format!("linux/{}", self.architecture.name()))
            .args([
                "--read-only",
                "--network=host",
                "--cap-drop=ALL",
                "--security-opt=no-new-privileges",
                "--security-opt=label=disable",
                "--env=NO_PROXY=*",
                "--env=no_proxy=*",
            ])
            .stdin(Stdio::null());
        for path in [&self.corpus, self.directory.path()] {
            command.arg("--mount").arg(format!(
                "type=bind,src={},dst={},readonly",
                path.display(),
                path.display()
            ));
        }
        command
    }

    fn dar_command(&self) -> Command {
        let mut command = self.container_command("run");
        command.args(["--rm", &self.image]);
        command
    }

    fn check_basic_commands(&self, version: &str) -> Result<()> {
        let default = successful_output(&mut self.dar_command())?;
        let help = successful_output(self.dar_command().arg("--help"))?;
        ensure!(
            default.stdout == help.stdout && help.stdout.windows(6).any(|part| part == b"Usage:"),
            InvalidInputSnafu {
                message: "the default image command did not print CLI help",
            }
        );
        let output = successful_output(self.dar_command().arg("--version"))?;
        ensure!(
            output.stdout == format!("dar {version}\n").as_bytes(),
            InvalidInputSnafu {
                message: "image version differs from the expected CLI version",
            }
        );
        let output = output_with_timeout(
            self.dar_command()
                .arg("inspect")
                .arg(self.corpus.join("partitioned/table")),
            Duration::from_secs(30),
        )?;
        let inspection = check_inspection(&output)?;
        ensure!(
            inspection["table_version"] == "0",
            InvalidInputSnafu {
                message: "image inspected the wrong snapshot"
            }
        );
        for action in ["inspect", "scan"] {
            check_error(
                &output_with_timeout(
                    self.dar_command().args([action, "/secret-missing-table"]),
                    Duration::from_secs(30),
                )?,
                1,
                "table_location",
                "invalid_table_location",
            )?;
        }
        successful_output(self.container_command("run").args([
            "--rm",
            "--entrypoint=/bin/sh",
            &self.image,
            "-ec",
            r#"
test "$(id -u)" = 65532
test "$(id -g)" = 65532
test "$PWD" = /work
test -r /etc/ssl/certs/ca-certificates.crt
test -s /etc/ssl/certs/ca-certificates.crt
test -z "${SSL_CERT_FILE-}"
test -z "${SSL_CERT_DIR-}"
test -r /usr/share/doc/dar/LICENSE
test -r /usr/share/doc/dar/NOTICE
"#,
        ]))?;
        Ok(())
    }

    fn check_query(&self) -> Result<()> {
        successful_output(self.dar_command().args(["query", "--help"]))?;
        let sql_file = self.directory.path().join("query.sql");
        fs::write(&sql_file, "SELECT COUNT(*) AS n FROM t")
            .context(FileSnafu { path: &sql_file })?;
        let expected = IpcTable::read_file(&self.corpus.join("partitioned/expected.arrow"))?;
        let output = output_with_timeout(
            self.dar_command()
                .arg("query")
                .arg(self.corpus.join("partitioned/table"))
                .args(["--table-version", "0", "--diagnostics=json", "--sql-file"])
                .arg(&sql_file),
            Duration::from_secs(30),
        )?;
        check_completed_count_query(&output, expected.row_count())
    }

    fn check_https(&self) -> Result<()> {
        let directory = self.directory.path();
        create_certificates(directory)?;
        let empty_certs = directory.join("empty-certs");
        fs::create_dir(&empty_certs).context(FileSnafu { path: &empty_certs })?;
        fs::set_permissions(&empty_certs, fs::Permissions::from_mode(0o755))
            .context(FileSnafu { path: &empty_certs })?;
        let trusted_ca = directory.join("trusted-ca.pem");
        fs::set_permissions(&trusted_ca, fs::Permissions::from_mode(0o644))
            .context(FileSnafu { path: &trusted_ca })?;
        let expected_path = self.corpus.join("partitioned/expected.arrow");
        let expected = IpcTable::read_file(&expected_path)?;
        let id = expected
            .schema
            .index_of("id")
            .context(crate::error::ArrowSnafu {
                path: &expected_path,
            })?;
        let expected = expected.project(&[id], &expected_path)?;
        for (certificate, custom_ca) in [("trusted", false), ("trusted", true), ("untrusted", true)]
        {
            let table = HttpTable::from_directory(&self.corpus.join("partitioned/table"))?;
            let server = HttpTestServer::start_tls(
                load_server_config(directory, certificate)?,
                move |request| table.respond(request).map(Some),
            )?;
            for action in ["inspect", "scan"] {
                let mut command = self.container_command("run");
                command.arg("--rm");
                if custom_ca {
                    command
                        .arg("--env")
                        .arg(format!(
                            "SSL_CERT_FILE={}",
                            directory.join("trusted-ca.pem").display()
                        ))
                        .arg("--env")
                        .arg(format!("SSL_CERT_DIR={}", empty_certs.display()));
                }
                command.args([&self.image, action]);
                if action == "scan" {
                    command.args(["--column", "id"]);
                }
                let output = output_with_timeout(
                    command.arg(format!("{}/", server.origin)),
                    Duration::from_secs(30),
                )?;
                let requests = server.take_requests()?;
                if certificate == "untrusted" || !custom_ca {
                    check_error(&output, 1, "snapshot", "snapshot_load")?;
                    ensure!(
                        requests.is_empty(),
                        InvalidInputSnafu {
                            message: "an untrusted TLS server received HTTP requests"
                        }
                    );
                } else {
                    ensure!(
                        !requests.is_empty(),
                        InvalidInputSnafu {
                            message: "the trusted TLS server received no HTTP requests"
                        }
                    );
                    if action == "scan" {
                        check_arrow_output(&output)?.check_matches(&expected, None)?;
                    } else {
                        check_inspection(&output)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn check_closed_stdout(&self) -> Result<()> {
        // Close the reader inside the container so Docker's output buffering cannot hide EPIPE.
        let output = output_with_timeout(
            self.container_command("run")
                .args([
                    "--rm",
                    "--tmpfs=/tmp:rw,noexec,nosuid,size=1m",
                    "--entrypoint=/bin/sh",
                    &self.image,
                    "-ec",
                    r#"
mkfifo /tmp/output
(: </tmp/output) & reader=$!
exec 3>/tmp/output
wait "$reader"
exec /usr/local/bin/dar scan "$@" >&3 3>&-
"#,
                    "closed-stdout",
                ])
                .arg(self.corpus.join("partitioned/table")),
            Duration::from_secs(30),
        )?;
        check_error(&output, 3, "execution", "output_write")?;
        Ok(())
    }

    fn check_signals(&self) -> Result<()> {
        let options = self.directory.path().join("storage.json");
        fs::write(&options, br#"{"allow_http":"true"}"#).context(FileSnafu { path: &options })?;
        fs::set_permissions(&options, fs::Permissions::from_mode(0o644))
            .context(FileSnafu { path: &options })?;
        for action in ["inspect", "scan"] {
            for (signal, exit) in [("INT", "130\n"), ("TERM", "143\n")] {
                let server = HttpTestServer::start(|_| Ok(None))?;
                successful_output(
                    self.container_command("create")
                        .args([&self.image, action, "--storage-options-file"])
                        .arg(&options)
                        .arg(format!("{}/", server.origin)),
                )?;
                successful_output(Command::new("docker").args(["start", &self.name]))?;
                let request = server.wait_for_request(Duration::from_secs(15))?;
                successful_output(Command::new("docker").args([
                    "exec",
                    &self.name,
                    "/bin/sh",
                    "-ec",
                    "test \"$(cat /proc/1/comm)\" = dar",
                ]))?;
                successful_output(
                    Command::new("docker").args(["kill", "--signal", signal, &self.name]),
                )?;
                let output = successful_output(Command::new("docker").args(["wait", &self.name]))?;
                ensure!(
                    output.stdout == exit.as_bytes(),
                    InvalidInputSnafu {
                        message: format!(
                            "container {action} did not exit from SIG{signal}: {:?}",
                            String::from_utf8_lossy(&output.stdout)
                        ),
                    }
                );
                let logs = successful_output(Command::new("docker").args(["logs", &self.name]))?;
                ensure!(
                    logs.stdout.is_empty(),
                    InvalidInputSnafu {
                        message: "terminated CLI wrote unexpected stdout"
                    }
                );
                server.wait_for_disconnect(request.connection_id, Duration::from_secs(5))?;
                successful_output(Command::new("docker").args(["rm", &self.name]))?;
            }
        }
        Ok(())
    }
}

impl Drop for ImageFixture {
    fn drop(&mut self) {
        // Killing a timed-out Docker client alone can leave its container running.
        for args in [
            vec!["kill", "--signal=KILL", &self.name],
            vec!["rm", "--force", &self.name],
        ] {
            let _ = output_with_timeout(Command::new("docker").args(args), Duration::from_secs(10));
        }
    }
}

fn successful_output(command: &mut Command) -> Result<Output> {
    let output = output_with_timeout(command, Duration::from_secs(30))?;
    ensure!(
        output.status.success() && output.stderr.is_empty(),
        InvalidInputSnafu {
            message: format!(
                "image command failed: {}; stderr={}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            ),
        }
    );
    Ok(output)
}

fn image_id(output: &[u8], architecture: Architecture) -> Result<String> {
    let text = String::from_utf8_lossy(output);
    let fields: Vec<_> = text.split_whitespace().collect();
    ensure!(
        fields.len() == 3 && fields[1] == "linux" && fields[2] == architecture.name(),
        InvalidInputSnafu {
            message: "local image platform differs from the requested Linux architecture",
        }
    );
    let digest = fields[0].strip_prefix("sha256:").unwrap_or(fields[0]);
    ensure!(
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        InvalidInputSnafu {
            message: "local image inspection did not return a SHA-256 image ID",
        }
    );
    Ok(format!("sha256:{digest}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_resolution_requires_one_immutable_id_on_the_expected_platform() {
        let digest = "a".repeat(64);
        for prefix in ["", "sha256:"] {
            assert_eq!(
                image_id(
                    format!("{prefix}{digest} linux amd64\n").as_bytes(),
                    Architecture::Amd64
                )
                .unwrap(),
                format!("sha256:{digest}")
            );
        }
        for output in [
            "".into(),
            format!("{digest} linux arm64"),
            format!("{digest} windows amd64"),
            "dar:latest linux amd64".into(),
            format!("{digest} linux amd64\n{digest} linux amd64"),
        ] {
            assert!(image_id(output.as_bytes(), Architecture::Amd64).is_err());
        }
    }
}
