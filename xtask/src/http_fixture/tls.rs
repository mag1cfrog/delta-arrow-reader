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

use crate::{
    command::output_with_timeout,
    error::{FileSnafu, InvalidInputSnafu, PemSnafu, Result, TlsSnafu},
};

pub(crate) fn create_certificates(directory: &Path) -> Result<()> {
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

pub(crate) fn load_server_config(directory: &Path, name: &str) -> Result<ServerConfig> {
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
