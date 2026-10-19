use std::{collections::BTreeMap, fs};

use hyper::{Method, StatusCode, body::Bytes};
use serde_json::json;
use snafu::{ResultExt, ensure};

use super::{ProcessFixture, check_error, check_inspection, run_cli};
use crate::{
    error::{FileSnafu, InvalidInputSnafu, Result},
    http_fixture::{
        HttpTestServer,
        table::{HttpTable, file_response, response, xml_response},
    },
};

const COMMIT_FILE_NAME: &str = "00000000000000000000.json";

impl ProcessFixture<'_> {
    pub(super) fn check_paginated_listing(&self) -> Result<()> {
        let table = self.create_metadata_table(json!([]), 1)?;
        let path = table.join("_delta_log").join(COMMIT_FILE_NAME);
        let commit = Bytes::from(fs::read(&path).context(FileSnafu { path })?);
        for (table_name, encoded_name) in [("table", "table"), ("secret%name", "secret%25name")] {
            for last_entry in [
                "_last_checkpoint",
                "_sidecars/part.parquet",
                "_staged_commits/commit.json",
            ] {
                let listing = s3_listing(table_name, last_entry, commit.len());
                let object_path = format!("/bucket/{encoded_name}/_delta_log/{COMMIT_FILE_NAME}");
                let commit = commit.clone();
                let server = HttpTestServer::start(move |request| {
                    let query: Vec<_> = url::form_urlencoded::parse(
                        request.uri().query().unwrap_or_default().as_bytes(),
                    )
                    .collect();
                    let response = if request.method() == Method::GET
                        && request.uri().path().trim_end_matches('/') == "/bucket"
                        && query
                            .iter()
                            .any(|(key, value)| key == "list-type" && value == "2")
                    {
                        if query.iter().any(|(key, _)| key == "continuation-token") {
                            response(StatusCode::FORBIDDEN, Bytes::new())
                        } else {
                            xml_response(StatusCode::OK, listing.clone())
                        }
                    } else if matches!(*request.method(), Method::GET | Method::HEAD)
                        && request.uri().path() == object_path
                    {
                        file_response(request, &commit)?
                    } else {
                        response(StatusCode::NOT_FOUND, Bytes::new())
                    };
                    Ok(Some(response))
                })?;
                let options = self.write_input_file(json!({
                    "aws_endpoint_url_s3": server.origin,
                    "aws_region": "us-east-1", "aws_skip_signature": "true", "allow_http": "true",
                }).to_string().as_bytes())?;
                for version in ["0", "999"] {
                    let output = run_cli(
                        self.command()
                            .args([
                                "inspect",
                                "--table-version",
                                version,
                                "--storage-options-file",
                            ])
                            .arg(&options)
                            .arg(format!("s3://bucket/{encoded_name}")),
                    )?;
                    if version == "0" {
                        let inspection = check_inspection(&output)?;
                        ensure!(
                            inspection["table_version"] == "0",
                            InvalidInputSnafu {
                                message: format!("unexpected S3 inspection: {inspection}")
                            }
                        );
                    } else {
                        check_error(&output, 1, "snapshot", "snapshot_load")?;
                    }
                    let requests = server.take_requests()?;
                    let listings = requests
                        .iter()
                        .filter(|request| {
                            url::form_urlencoded::parse(
                                request.uri.query().unwrap_or_default().as_bytes(),
                            )
                            .any(|(key, _)| key == "list-type")
                        })
                        .count();
                    ensure!(
                        listings == 1,
                        InvalidInputSnafu {
                            message: format!(
                                "expected one listing page for {table_name}, version {version}, last entry {last_entry}: {requests:?}"
                            )
                        }
                    );
                }
            }
        }
        Ok(())
    }

    pub(super) fn check_http_metadata_only(&self) -> Result<()> {
        let table = self
            .repository
            .join("crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/deletion_vectors/table");
        let mut files = BTreeMap::new();
        for name in [COMMIT_FILE_NAME, "00000000000000000001.json"] {
            let path = table.join("_delta_log").join(name);
            let data = Bytes::from(fs::read(&path).context(FileSnafu { path })?);
            files.insert(format!("/_delta_log/{name}"), data);
        }
        let table = HttpTable::from_files(files);
        let server = HttpTestServer::start(move |request| table.respond(request).map(Some))?;
        let options = self.write_input_file(br#"{"allow_http":"true"}"#)?;
        let output = run_cli(
            self.command()
                .args(["inspect", "--storage-options-file"])
                .arg(&options)
                .arg(format!("{}/", server.origin)),
        )?;
        let inspection = check_inspection(&output)?;
        ensure!(
            inspection["table_version"] == "1",
            InvalidInputSnafu {
                message: format!("unexpected HTTP inspection: {inspection}")
            }
        );
        let location = server
            .origin
            .replace("http://", "http://secret-user:secret-password@");
        for filter in [
            "info",
            "debug",
            "trace,reqwest=trace,hyper=trace,object_store=trace",
        ] {
            let verbose = run_cli(
                self.command()
                    .env("RUST_LOG", filter)
                    .args(["inspect", "--storage-options-file"])
                    .arg(&options)
                    .arg(&location),
            )?;
            let text = String::from_utf8_lossy(&verbose.stderr);
            ensure!(
                verbose.status.success()
                    && verbose.stdout == output.stdout
                    && text.contains("table_load.completed")
                    && !text.contains("secret"),
                InvalidInputSnafu {
                    message: format!("HTTP tracing changed output or exposed credentials: {text}")
                }
            );
        }
        let requests = server.take_requests()?;
        ensure!(
            !requests.is_empty()
                && requests.iter().all(|request| {
                    let path = request.uri.path();
                    path == "/_delta_log" || path.starts_with("/_delta_log/")
                }),
            InvalidInputSnafu {
                message: format!("inspection must request only Delta metadata: {requests:?}")
            }
        );
        Ok(())
    }
}

fn s3_listing(table: &str, last_entry: &str, commit_size: usize) -> Bytes {
    let mut xml = String::from(
        "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <IsTruncated>true</IsTruncated><NextContinuationToken>auxiliary-page</NextContinuationToken>",
    );
    // The nested entry must be skipped without ending the listing at its leaf name.
    for (name, size) in [
        ("00000000000000000000.aaa/zzz", 1),
        (COMMIT_FILE_NAME, commit_size),
        (last_entry, 1),
    ] {
        xml.push_str(&format!(
            "<Contents><Key>{table}/_delta_log/{name}</Key>\
             <LastModified>2026-01-01T00:00:00Z</LastModified><Size>{size}</Size></Contents>",
        ));
    }
    xml.push_str("</ListBucketResult>");
    Bytes::from(xml)
}
