use std::{
    fs,
    path::Path,
    process::{Command, Stdio},
    sync::Arc,
    time::Duration,
};

use snafu::{ResultExt, ensure};
use tokio_rustls::rustls::{
    ServerConfig,
    crypto::ring,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};

use super::{IpcTable, ScanFixture};
use crate::{
    command::output_with_timeout,
    error::{FileSnafu, InvalidInputSnafu, PemSnafu, Result, TlsSnafu},
    http_fixture::{HttpTestServer, table::HttpTable},
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

fn create_certificates(directory: &Path) -> Result<()> {
    let extensions = directory.join("extensions");
    fs::write(
        &extensions,
        "basicConstraints=critical,CA:FALSE\n\
         keyUsage=critical,digitalSignature,keyEncipherment\n\
         extendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost\n",
    )
    .context(FileSnafu { path: &extensions })?;
    for name in ["trusted", "untrusted"] {
        run_openssl(
            directory,
            &[
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "2",
                "-subj",
                &format!("/CN=Delta reader {name} test CA"),
                "-addext",
                "basicConstraints=critical,CA:TRUE",
                "-addext",
                "keyUsage=critical,keyCertSign,cRLSign",
                "-keyout",
                &format!("{name}-ca.key"),
                "-out",
                &format!("{name}-ca.pem"),
            ],
        )?;
        run_openssl(
            directory,
            &[
                "req",
                "-new",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-subj",
                "/CN=localhost",
                "-keyout",
                &format!("{name}.key"),
                "-out",
                &format!("{name}.csr"),
            ],
        )?;
        run_openssl(
            directory,
            &[
                "x509",
                "-req",
                "-in",
                &format!("{name}.csr"),
                "-CA",
                &format!("{name}-ca.pem"),
                "-CAkey",
                &format!("{name}-ca.key"),
                "-CAcreateserial",
                "-days",
                "2",
                "-extfile",
                "extensions",
                "-out",
                &format!("{name}.pem"),
            ],
        )?;
    }
    Ok(())
}

fn run_openssl(directory: &Path, args: &[&str]) -> Result<()> {
    let output = output_with_timeout(
        Command::new("openssl")
            .current_dir(directory)
            .args(args)
            .stdin(Stdio::null()),
        Duration::from_secs(30),
    )?;
    ensure!(
        output.status.success(),
        InvalidInputSnafu {
            message: format!(
                "certificate generation failed: {}; stderr={}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            )
        }
    );
    Ok(())
}

fn load_server_config(directory: &Path, name: &str) -> Result<ServerConfig> {
    let certificate_path = directory.join(format!("{name}.pem"));
    let key_path = directory.join(format!("{name}.key"));
    let certificate = CertificateDer::from_pem_file(&certificate_path).context(PemSnafu {
        path: &certificate_path,
    })?;
    let key = PrivateKeyDer::from_pem_file(&key_path).context(PemSnafu { path: &key_path })?;
    ServerConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_safe_default_protocol_versions()
        .context(TlsSnafu)?
        .with_no_client_auth()
        .with_single_cert(vec![certificate], key)
        .context(TlsSnafu)
}
