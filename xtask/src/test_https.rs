use std::{
    collections::BTreeSet,
    env, fs,
    path::Path,
    process::{Command, Stdio},
    time::Duration,
};

use hyper::Method;
use serde::Deserialize;

use crate::{
    Result,
    command::{command_output, remove_cargo_package_environment, run_with_timeout},
    http_fixture::{
        HttpTestServer,
        table::HttpTable,
        tls::{create_certificates, load_server_config},
    },
    repository::cargo_workspace_root,
};

mod proxy;
use proxy::ConnectProxy;

#[derive(clap::Args)]
pub(crate) struct Arguments {
    /// Cargo features for the core tests, for example datafusion.
    #[arg(long)]
    features: Option<String>,
}

impl Arguments {
    pub fn run(self) -> Result<()> {
        let core =
            cargo_workspace_root(&env::current_dir()?)?.join("crates/delta-arrow-reader-core");
        check_tls_dependencies(&command_output(
            self.cargo(&core, "metadata")
                .args(["--format-version", "1"]),
        )?)?;
        let temporary = tempfile::Builder::new()
            .prefix("delta-reader-https-")
            .tempdir()?;
        let root = temporary.path();
        create_certificates(root)?;
        let data = root.join("data");
        fs::create_dir(&data)?;
        fs::write(data.join("pool-probe"), b"ok")?;
        fs::create_dir(root.join("empty-certs"))?;
        fs::write(root.join("empty-ca.pem"), b"")?;
        let server = |name| {
            let data = data.clone();
            HttpTestServer::start_tls(load_server_config(root, name)?, move |request| {
                // The test writes its tables after the servers have started.
                HttpTable::from_directory(&data)?.respond(request).map(Some)
            })
        };
        let trusted = server("trusted")?;
        let untrusted = server("untrusted")?;
        let port = |server: &HttpTestServer| -> Result<u16> {
            server
                .origin
                .parse::<hyper::Uri>()?
                .port_u16()
                .ok_or_else(|| "HTTPS fixture has no port".into())
        };
        let ports = [port(&trusted)?, port(&untrusted)?];
        let proxy = ConnectProxy::start(ports)?;
        let test = |args: &[&str]| {
            let mut command = self.cargo(&core, "test");
            command
                .args(args)
                .args(["--", "--exact", "--ignored", "--nocapture"]);
            isolate_environment(&mut command);
            command
                .env("SSL_CERT_FILE", root.join("trusted-ca.pem"))
                .env("SSL_CERT_DIR", root.join("empty-certs"))
                .env("DAR_TLS_DIRECTORY", &data)
                .env("DAR_TLS_ENDPOINT", &trusted.origin)
                .env("DAR_TLS_UNTRUSTED_ENDPOINT", &untrusted.origin);
            command
        };
        for empty_roots in [false, true] {
            eprintln!("Partial-read TLS checks, empty_roots={empty_roots}");
            let mut command = test(&[
                "--lib",
                "delta::kernel::s3::http_pool::tests::http_without_roots_preserves_https_verification",
            ]);
            command
                .env("DAR_TLS_EMPTY_ROOTS", empty_roots.to_string())
                .env(
                    "SSL_CERT_FILE",
                    root.join(if empty_roots {
                        "empty-ca.pem"
                    } else {
                        "trusted-ca.pem"
                    }),
                );
            run_with_timeout(&mut command, Duration::from_secs(600))?;
        }
        for proxied in [false, true] {
            eprintln!("HTTPS certificate checks, proxy={proxied}");
            trusted.take_requests()?;
            let mut command = test(&["--test", "reader", "https::verified_storage_paths"]);
            if proxied {
                command
                    .env("HTTPS_PROXY", &proxy.server.origin)
                    .env("DAR_TLS_PROXY", &proxy.server.origin);
            }
            run_with_timeout(&mut command, Duration::from_secs(600))?;
            let requests = trusted.take_requests()?;
            if !requests.iter().any(|request| {
                request.method == Method::GET
                    && request
                        .uri
                        .query()
                        .is_some_and(|query| query.contains("X-Amz-Signature="))
                    && request.range.is_none()
            }) {
                return Err("HTTPS checks did not exercise a full presigned GET".into());
            }
            if !requests.iter().any(|request| request.range.is_some()) {
                return Err("HTTPS checks did not exercise range reads".into());
            }
            if !untrusted.take_requests()?.is_empty() {
                return Err("untrusted TLS server received an HTTP request".into());
            }
            if proxied {
                if proxy.server.take_requests()?.is_empty() {
                    return Err("HTTPS checks bypassed the proxy".into());
                }
                let ports = proxy
                    .upstream_ports
                    .lock()
                    .map_err(|_| "HTTPS proxy port log is poisoned")?;
                if requests
                    .iter()
                    .any(|request| !ports.contains(&request.peer_port))
                {
                    return Err("an HTTPS request bypassed the proxy".into());
                }
            }
        }
        Ok(())
    }

    fn cargo(&self, core: &Path, action: &str) -> Command {
        let mut command = Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
        command
            .current_dir(core)
            .args([action, "--locked"])
            .stdin(Stdio::null());
        if let Some(features) = &self.features {
            command.args(["--features", features]);
        }
        remove_cargo_package_environment(&mut command);
        command
    }
}

fn isolate_environment(command: &mut Command) {
    for (key, _) in env::vars_os() {
        let name = key.to_string_lossy();
        if ["http_proxy", "https_proxy", "all_proxy", "no_proxy"]
            .contains(&name.to_ascii_lowercase().as_str())
            || name.starts_with("DAR_TLS_")
        {
            command.env_remove(key);
        }
    }
}

fn check_tls_dependencies(metadata: &str) -> Result<()> {
    #[derive(Deserialize)]
    struct Metadata {
        packages: Vec<Package>,
        resolve: Resolve,
    }
    #[derive(Deserialize)]
    struct Package {
        id: String,
        name: String,
    }
    #[derive(Deserialize)]
    struct Resolve {
        nodes: Vec<Node>,
    }
    #[derive(Deserialize)]
    struct Node {
        id: String,
    }
    let metadata: Metadata = serde_json::from_str(metadata)?;
    let resolved: BTreeSet<_> = metadata.resolve.nodes.iter().map(|node| &node.id).collect();
    for package in metadata.packages {
        if resolved.contains(&package.id)
            && ["native-tls", "openssl", "openssl-sys"].contains(&package.name.as_str())
        {
            return Err(format!(
                "native TLS re-entered the resolved dependency graph: {}",
                package.name
            )
            .into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
