use std::{
    fs,
    process::{Command, Stdio},
    time::Duration,
};

use snafu::{ResultExt, ensure};

use super::{IpcTable, ScanFixture};
use crate::{
    command::output_with_timeout,
    error::{FileSnafu, InvalidInputSnafu, Result},
    http_fixture::{
        HttpTestServer,
        table::HttpTable,
        tls::{create_certificates, load_server_config},
    },
    process::{check_error, check_inspection},
};

impl ScanFixture<'_> {
    pub(super) fn check_https_trust(&self) -> Result<()> {
        let directory = self.directory.path();
        create_certificates(directory)?;
        let empty_certs = directory.join("empty-certs");
        fs::create_dir(&empty_certs).context(FileSnafu { path: &empty_certs })?;
        let fixture = self.corpus.join("partitioned");
        let expected_path = fixture.join("expected.arrow");
        let expected = IpcTable::read_file(&expected_path)?;
        let id = expected
            .schema
            .index_of("id")
            .context(crate::error::ArrowSnafu {
                path: &expected_path,
            })?;
        let expected = expected.project(&[id], &expected_path)?;
        for certificate in ["trusted", "untrusted"] {
            let table = HttpTable::from_directory(&fixture.join("table"))?;
            let server = HttpTestServer::start_tls(
                load_server_config(directory, certificate)?,
                move |request| table.respond(request).map(Some),
            )?;
            for action in ["inspect", "scan"] {
                eprintln!("  certificate={certificate}, command={action}");
                let mut command = Command::new(self.binary);
                command
                    .arg(action)
                    .current_dir(directory)
                    .env("NO_PROXY", "*")
                    .env("no_proxy", "*")
                    .env("RUST_LOG", "trace")
                    .env("SSL_CERT_FILE", directory.join("trusted-ca.pem"))
                    .env("SSL_CERT_DIR", &empty_certs)
                    .stdin(Stdio::null());
                if action == "scan" {
                    command.args(["--column", "id"]);
                }
                let output = output_with_timeout(
                    command.arg(format!("{}/", server.origin)),
                    Duration::from_secs(15),
                )?;
                let requests = server.take_requests()?;
                if certificate == "untrusted" {
                    check_error(&output, 1, "snapshot", "snapshot_load")?;
                    ensure!(
                        requests.is_empty(),
                        InvalidInputSnafu {
                            message: format!(
                                "untrusted TLS server received HTTP requests: {requests:?}"
                            )
                        }
                    );
                } else {
                    ensure!(
                        !requests.is_empty(),
                        InvalidInputSnafu {
                            message: "trusted TLS server received no HTTP requests"
                        }
                    );
                    if action == "inspect" {
                        let inspection = check_inspection(&output)?;
                        ensure!(
                            inspection["table_version"] == "0",
                            InvalidInputSnafu {
                                message: format!("unexpected HTTPS inspection: {inspection}")
                            }
                        );
                    } else {
                        self.check_successful_scan(&output)?
                            .check_matches(&expected, None)?;
                    }
                }
            }
        }
        Ok(())
    }
}
