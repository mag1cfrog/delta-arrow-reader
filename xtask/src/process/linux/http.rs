use std::{collections::BTreeMap, fs, ops::Range};

use http_body_util::Full;
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Bytes, Incoming},
    header::{CONTENT_RANGE, CONTENT_TYPE, HeaderValue, RANGE},
};
use serde_json::json;
use snafu::{ResultExt, ensure};

use super::{
    ProcessFixture, check_error, check_inspection,
    http_server::{HttpResponse, HttpTestServer},
    run_cli,
};
use crate::error::{FileSnafu, HttpResponseSnafu, InvalidInputSnafu, Result};

const COMMIT_FILE_NAME: &str = "00000000000000000000.json";
const LAST_MODIFIED: &str = "Thu, 01 Jan 2026 00:00:00 GMT";

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
            .join("tests/reader/fixtures/external_writer/corpus/deletion_vectors/table");
        let mut files = BTreeMap::new();
        let mut listing = String::from("<multistatus xmlns=\"DAV:\">");
        for name in [COMMIT_FILE_NAME, "00000000000000000001.json"] {
            let path = table.join("_delta_log").join(name);
            let data = Bytes::from(fs::read(&path).context(FileSnafu { path })?);
            let href = format!("/_delta_log/{name}");
            listing.push_str(&format!(
                "<response><href>{href}</href><propstat><prop><getcontentlength>{}</getcontentlength>\
                 <getlastmodified>{LAST_MODIFIED}</getlastmodified><resourcetype/></prop>\
                 <status>HTTP/1.1 200 OK</status></propstat></response>", data.len(),
            ));
            files.insert(href, data);
        }
        listing.push_str("</multistatus>");
        let listing = Bytes::from(listing);
        let server = HttpTestServer::start(move |request| {
            let path = request.uri().path();
            let response =
                if request.method() == "PROPFIND" && path.trim_end_matches('/') == "/_delta_log" {
                    xml_response(StatusCode::MULTI_STATUS, listing.clone())
                } else if matches!(*request.method(), Method::GET | Method::HEAD) {
                    match files.get(path) {
                        Some(data) => file_response(request, data)?,
                        None => response(StatusCode::NOT_FOUND, Bytes::new()),
                    }
                } else {
                    response(StatusCode::NOT_FOUND, Bytes::new())
                };
            Ok(Some(response))
        })?;
        let options = self.write_input_file(br#"{"allow_http":"true"}"#)?;
        let output = run_cli(
            self.command()
                .args(["inspect", "--storage-options-file"])
                .arg(options)
                .arg(format!("{}/", server.origin)),
        )?;
        let inspection = check_inspection(&output)?;
        ensure!(
            inspection["table_version"] == "1",
            InvalidInputSnafu {
                message: format!("unexpected HTTP inspection: {inspection}")
            }
        );
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

fn response(status: StatusCode, body: Bytes) -> HttpResponse {
    let mut response = Response::new(Full::new(body));
    *response.status_mut() = status;
    response
}

fn xml_response(status: StatusCode, body: Bytes) -> HttpResponse {
    let mut response = response(status, body);
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    response
}

fn file_response(request: &Request<Incoming>, data: &Bytes) -> Result<HttpResponse> {
    let mut builder = Response::builder().header("Last-Modified", LAST_MODIFIED);
    let body = if let Some(range) = request.headers().get(RANGE) {
        let Some(range) = range
            .to_str()
            .ok()
            .and_then(|range| byte_range(range, data.len()))
        else {
            return Ok(response(StatusCode::RANGE_NOT_SATISFIABLE, Bytes::new()));
        };
        builder = builder.status(StatusCode::PARTIAL_CONTENT).header(
            CONTENT_RANGE,
            format!("bytes {}-{}/{}", range.start, range.end - 1, data.len()),
        );
        data.slice(range)
    } else {
        data.clone()
    };
    builder
        .header("Content-Length", body.len())
        .body(Full::new(if request.method() == Method::HEAD {
            Bytes::new()
        } else {
            body
        }))
        .context(HttpResponseSnafu)
}

fn byte_range(header: &str, size: usize) -> Option<Range<usize>> {
    let (start, end) = header.strip_prefix("bytes=")?.split_once('-')?;
    let range = if start.is_empty() {
        size.saturating_sub(end.parse::<usize>().ok()?)..size
    } else {
        let end = if end.is_empty() {
            size
        } else {
            end.parse::<usize>().ok()?.saturating_add(1).min(size)
        };
        start.parse().ok()?..end
    };
    (range.start < range.end).then_some(range)
}

#[cfg(test)]
mod tests;
