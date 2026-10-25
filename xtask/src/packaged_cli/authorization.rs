//! Selected file credentials are checked against a local S3 endpoint that can revoke access.

use std::{
    collections::BTreeMap,
    fs,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use hyper::{StatusCode, body::Bytes};
use serde_json::json;

use super::PackagedCli;
use crate::{
    Result,
    http_fixture::{
        HttpTestServer,
        table::{file_response, response, xml_response},
    },
};

impl PackagedCli<'_> {
    pub(super) fn check_authorization(&mut self) -> Result<()> {
        let files = ["_delta_log/00000000000000000000.json", "part.parquet"]
            .into_iter()
            .map(|name| {
                Ok((
                    format!("/fixture/table/{name}"),
                    Bytes::from(fs::read(self.directory.join("table").join(name))?),
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let revoked = Arc::new(AtomicBool::new(false));
        let denied = Arc::clone(&revoked);
        let stalled = Arc::new(AtomicBool::new(false));
        let hold_response = Arc::clone(&stalled);
        let requests = Arc::new(AtomicUsize::new(0));
        let received = Arc::clone(&requests);
        let server = HttpTestServer::start(move |request| {
            received.fetch_add(1, Ordering::Relaxed);
            if hold_response.load(Ordering::Relaxed) {
                return Ok(None);
            }
            let selected_key = request
                .headers()
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|header| header.contains("Credential=credential-canary-key/"));
            let selected_token = request
                .headers()
                .get("x-amz-security-token")
                .is_some_and(|token| token == "credential-canary-token");
            if denied.load(Ordering::Relaxed) || !selected_key || !selected_token {
                return Ok(Some(xml_response(
                    StatusCode::FORBIDDEN,
                    Bytes::from_static(b"<Error><Code>AccessDenied</Code><Message>credential-canary-denied</Message></Error>"),
                )));
            }
            if request
                .uri()
                .query()
                .is_some_and(|query| query.contains("list-type=2"))
            {
                return Ok(Some(xml_response(StatusCode::OK, Bytes::from_static(b"<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>table/_delta_log/00000000000000000000.json</Key><Size>1</Size><LastModified>2026-01-01T00:00:00Z</LastModified></Contents></ListBucketResult>"))));
            }
            Ok(Some(match files.get(request.uri().path()) {
                Some(bytes) => file_response(request, bytes)?,
                None => response(StatusCode::NOT_FOUND, Bytes::new()),
            }))
        })?;
        let mut configuration =
            json!({"aws_endpoint": server.origin, "aws_region": "us-east-1", "allow_http": "true"});
        fs::write(
            self.directory.join("endpoint.json"),
            serde_json::to_vec(&configuration)?,
        )?;
        configuration["aws_access_key_id"] = json!("credential-canary-key");
        configuration["aws_secret_access_key"] = json!("credential-canary-secret");
        configuration["aws_session_token"] = json!("credential-canary-token");
        fs::write(
            self.directory.join("s3.json"),
            serde_json::to_vec(&configuration)?,
        )?;
        fs::write(
            self.directory.join("s3.env"),
            "AWS_ACCESS_KEY_ID=credential-canary-key\nAWS_SECRET_ACCESS_KEY=credential-canary-secret\nAWS_SESSION_TOKEN=credential-canary-token\n",
        )?;
        let table = "s3://fixture/table";
        for authorization in [
            vec!["--auth-source", "json", "--storage-options-file", "s3.json"],
            vec![
                "--env-file",
                "s3.env",
                "--storage-options-file",
                "endpoint.json",
            ],
        ] {
            let inspection = [
                vec![
                    "inspect",
                    table,
                    "--view",
                    "summary",
                    "--table-version",
                    "0",
                ],
                authorization.clone(),
            ]
            .concat();
            assert_eq!(self.read_json(&inspection, 4096)?["table_version"], "0");
            let query = [
                vec![
                    "query",
                    table,
                    "--table-version",
                    "0",
                    "--sql-file",
                    "query.sql",
                    "--format",
                    "json",
                ],
                authorization.clone(),
            ]
            .concat();
            assert_eq!(self.read_json(&query, 8192)?["row_count"], "2");
            let destination = if authorization.contains(&"--env-file") {
                "remote-env.json"
            } else {
                "remote-json.json"
            };
            let receipt = self.read_json(
                &[query.as_slice(), &["--output", destination]].concat(),
                8192,
            )?;
            assert_eq!(self.read_verified_manifest(&receipt)?["rows"], "2");
            let saved: serde_json::Value =
                serde_json::from_slice(&fs::read(self.directory.join(destination))?)?;
            assert_eq!(
                saved["rows"],
                json!([
                    ["9007199254740993", "12345", "0"],
                    ["9007199254740994", null, "1"]
                ])
            );
            revoked.store(true, Ordering::Relaxed);
            let output = self.invoke(&inspection, 1, 0)?;
            let error: serde_json::Value = serde_json::from_slice(&output.stderr)?;
            // object_store 0.13.2 hides listing status in a private error type.
            assert_eq!(error["source_kind"], "unknown");
            assert_eq!(error["recovery"], "unknown");
            assert_eq!(error["retryable"], serde_json::Value::Null);
            let latest = [
                vec!["inspect", table, "--view", "summary"],
                authorization.clone(),
            ]
            .concat();
            let output = self.invoke(&latest, 1, 0)?;
            let error: serde_json::Value = serde_json::from_slice(&output.stderr)?;
            assert_eq!(error["source_kind"], "permission_denied");
            assert_eq!(error["recovery"], "request_access");
            revoked.store(false, Ordering::Relaxed);
        }
        assert!(
            requests.load(Ordering::Relaxed) > 0,
            "authorization fixture was never reached"
        );
        let before = requests.load(Ordering::Relaxed);
        self.invoke(
            &[
                "inspect",
                table,
                "--table-version",
                "0",
                "--env-file",
                "missing.env",
                "--storage-options-file",
                "endpoint.json",
            ],
            2,
            0,
        )?;
        assert_eq!(
            requests.load(Ordering::Relaxed),
            before,
            "a failed selected file reached storage"
        );
        server.take_requests()?;
        stalled.store(true, Ordering::Relaxed);
        self.check_failure(
            &[
                "query",
                table,
                "--table-version",
                "0",
                "--auth-source",
                "json",
                "--storage-options-file",
                "s3.json",
                "--sql-file",
                "query.sql",
                "--format",
                "json",
                "--output",
                "interrupted.json",
                "--deadline-ms",
                "1000",
            ],
            1,
            "deadline_exceeded",
        )?;
        let held = server.wait_for_request(std::time::Duration::from_secs(2))?;
        server.wait_for_disconnect(held.connection_id, std::time::Duration::from_secs(2))?;
        assert!(!self.directory.join("interrupted.json").exists());
        assert!(
            !self
                .directory
                .join("interrupted.json.manifest.json")
                .exists()
        );
        Ok(())
    }
}
