//! Runs the local TLS endpoint used by native authorized-user ADC refreshes.

use std::{
    fs,
    path::PathBuf,
    process::{Child, Command},
    thread,
    time::{Duration, Instant},
};

use serde_json::Value;

/// Owns the Python OAuth fixture, its request log, and process cleanup.
pub(super) struct OAuthProxyFixture {
    directory: tempfile::TempDir,
    child: Child,
    pub(super) endpoint: String,
}

impl OAuthProxyFixture {
    pub(super) fn new(mode: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let child = Command::new("python3")
            .arg(
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/direct_credentials/gcs/oauth_proxy.py"),
            )
            .arg(directory.path())
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
                return Err("local OAuth fixture did not start".into());
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

impl Drop for OAuthProxyFixture {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
