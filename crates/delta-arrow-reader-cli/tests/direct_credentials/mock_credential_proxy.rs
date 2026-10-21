//! Mock Google OAuth and AWS credential endpoints exposed through a local proxy.

use std::{
    fs,
    path::PathBuf,
    process::{Child, Command},
    thread,
    time::{Duration, Instant},
};

use serde_json::Value;

/// Selects the request protocol and credential response used by the local service.
pub(super) enum CredentialService {
    GoogleOAuth,
    AwsSts,
    AwsContainer,
}

/// Owns the mock credential proxy, its request log, and process cleanup.
pub(super) struct MockCredentialProxy {
    directory: tempfile::TempDir,
    child: Child,
    pub(super) endpoint: String,
}

impl MockCredentialProxy {
    pub(super) fn new(
        service: CredentialService,
        mode: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let child = Command::new("python3")
            .arg(
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/direct_credentials/mock_credential_proxy.py"),
            )
            .arg(directory.path())
            .arg(match service {
                CredentialService::GoogleOAuth => "gcs",
                CredentialService::AwsSts => "sts",
                CredentialService::AwsContainer => "container",
            })
            .arg(mode)
            .spawn()?;
        let mut fixture = Self {
            directory,
            child,
            endpoint: String::new(),
        };
        let started = Instant::now();
        loop {
            if let Ok(endpoint) = fs::read_to_string(fixture.directory.path().join("endpoint"))
                && endpoint.starts_with("http://127.0.0.1:")
            {
                fixture.endpoint = endpoint;
                return Ok(fixture);
            }
            if fixture.child.try_wait()?.is_some() || started.elapsed() > Duration::from_secs(15) {
                return Err("mock credential proxy did not start".into());
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    pub(super) fn requests(&self) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
        Ok(
            fs::read_to_string(self.directory.path().join("requests.jsonl"))?
                .lines()
                .map(serde_json::from_str)
                .collect::<Result<_, _>>()?,
        )
    }
}

impl Drop for MockCredentialProxy {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
