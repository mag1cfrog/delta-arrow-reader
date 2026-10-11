use std::{collections::BTreeMap, fs, ops::Range, path::Path};

use http_body_util::Full;
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Bytes, Incoming},
    header::{CONTENT_RANGE, CONTENT_TYPE, HeaderValue, RANGE},
};
use snafu::{ResultExt, ensure};

use super::HttpResponse;
use crate::error::{FileSnafu, HttpResponseSnafu, InvalidInputSnafu, Result};

const LAST_MODIFIED: &str = "Thu, 01 Jan 2026 00:00:00 GMT";

pub(crate) struct HttpTable {
    files: BTreeMap<String, Bytes>,
    listing: Bytes,
}

impl HttpTable {
    pub fn from_files(files: BTreeMap<String, Bytes>) -> Self {
        let mut listing = String::from("<multistatus xmlns=\"DAV:\">");
        for (path, data) in &files {
            if path.starts_with("/_delta_log/") {
                let href = path
                    .replace('&', "&amp;")
                    .replace('<', "&lt;")
                    .replace('>', "&gt;");
                listing.push_str(&format!(
                    "<response><href>{href}</href><propstat><prop><getcontentlength>{}</getcontentlength>\
                     <getlastmodified>{LAST_MODIFIED}</getlastmodified><resourcetype/></prop>\
                     <status>HTTP/1.1 200 OK</status></propstat></response>", data.len(),
                ));
            }
        }
        listing.push_str("</multistatus>");
        Self {
            files,
            listing: Bytes::from(listing),
        }
    }

    pub fn from_directory(directory: &Path) -> Result<Self> {
        let mut files = BTreeMap::new();
        let mut pending = vec![directory.to_path_buf()];
        while let Some(parent) = pending.pop() {
            for entry in fs::read_dir(&parent).context(FileSnafu { path: &parent })? {
                let entry = entry.context(FileSnafu { path: &parent })?;
                let path = entry.path();
                let kind = entry.file_type().context(FileSnafu { path: &path })?;
                if kind.is_dir() {
                    pending.push(path);
                    continue;
                }
                ensure!(
                    kind.is_file(),
                    InvalidInputSnafu {
                        message: format!(
                            "HTTP fixture must contain only files and directories: {}",
                            path.display()
                        )
                    }
                );
                let relative = path
                    .strip_prefix(directory)
                    .expect("entry is inside the fixture directory");
                let url =
                    url::Url::from_file_path(Path::new("/").join(relative)).map_err(|()| {
                        InvalidInputSnafu {
                            message: format!("cannot encode HTTP fixture path: {}", path.display()),
                        }
                        .build()
                    })?;
                files.insert(
                    url.path().to_owned(),
                    Bytes::from(fs::read(&path).context(FileSnafu { path })?),
                );
            }
        }
        Ok(Self::from_files(files))
    }

    pub fn respond(&self, request: &Request<Incoming>) -> Result<HttpResponse> {
        let path = request.uri().path();
        if request.method() == "PROPFIND" && path.trim_end_matches('/') == "/_delta_log" {
            Ok(xml_response(StatusCode::MULTI_STATUS, self.listing.clone()))
        } else if matches!(*request.method(), Method::GET | Method::HEAD) {
            match self.files.get(path) {
                Some(data) => file_response(request, data),
                None => Ok(response(StatusCode::NOT_FOUND, Bytes::new())),
            }
        } else {
            Ok(response(StatusCode::NOT_FOUND, Bytes::new()))
        }
    }
}

pub(crate) fn response(status: StatusCode, body: Bytes) -> HttpResponse {
    let mut response = Response::new(Full::new(body));
    *response.status_mut() = status;
    response
}

pub(crate) fn xml_response(status: StatusCode, body: Bytes) -> HttpResponse {
    let mut response = response(status, body);
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    response
}

pub(crate) fn file_response(request: &Request<Incoming>, data: &Bytes) -> Result<HttpResponse> {
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
