//! Local HTTP read responses shared by cloud location and credential tests.

use std::{
    error::Error,
    sync::{Arc, Mutex},
};

use futures_util::TryStreamExt;
use hyper::{
    HeaderMap, Request, Response, Uri, body::Incoming, server::conn::http1, service::service_fn,
};
use hyper_util::rt::TokioIo;
use object_store::{
    GetRange, ObjectStore, ObjectStoreExt, client::HttpResponseBody, memory::InMemory,
    path::Path as StorePath,
};
use tokio::{net::TcpListener, task::JoinSet};
use url::Url;

/// An outgoing storage request, captured before the fixture responds.
#[derive(Debug)]
pub(crate) struct CapturedRequest {
    pub(crate) uri: Uri,
    pub(crate) headers: HeaderMap,
}

impl CapturedRequest {
    #[expect(
        clippy::expect_used,
        reason = "requests come from native storage clients"
    )]
    pub(crate) fn decoded_path(&self) -> StorePath {
        StorePath::from_url_path(self.uri.path()).expect("valid storage object path")
    }
}

pub(crate) type CapturedRequests = Arc<Mutex<Vec<CapturedRequest>>>;

pub(crate) async fn serve(
    listener: TcpListener,
    store: Arc<InMemory>,
    requests: Option<CapturedRequests>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (socket, _) = accepted?;
                let store = Arc::clone(&store);
                let requests = requests.clone();
                connections.spawn(http1::Builder::new().serve_connection(
                    TokioIo::new(socket),
                    service_fn(move |request: Request<Incoming>| {
                        if let Some(requests) = &requests {
                            requests.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                                .push(CapturedRequest { uri: request.uri().clone(), headers: request.headers().clone() });
                        }
                        cloud_response(request, Arc::clone(&store))
                    }),
                ));
            }
            Some(result) = connections.join_next() => { result??; }
        }
    }
}

// Only the list and read operations exercised by these fixtures are implemented.
async fn cloud_response(
    request: Request<Incoming>,
    store: Arc<InMemory>,
) -> Result<Response<HttpResponseBody>, Box<dyn Error + Send + Sync>> {
    let url = Url::parse(&format!("http://localhost{}", request.uri()))?;
    let azure = url
        .query_pairs()
        .any(|(key, value)| key == "comp" && value == "list");
    if azure
        || url
            .query_pairs()
            .any(|(key, value)| key == "list-type" && value == "2")
    {
        assert_eq!(url.path().trim_end_matches('/'), "/bucket");
        let query = url
            .query_pairs()
            .collect::<std::collections::HashMap<_, _>>();
        let prefix = query.get("prefix").map_or("", |s| s.as_ref());
        let offset = query
            .get(if azure { "startFrom" } else { "start-after" })
            .map_or("", |s| s.as_ref());
        let objects = store.list(None).try_collect::<Vec<_>>().await?;
        let mut xml = String::from(if azure {
            "<EnumerationResults><Blobs>"
        } else {
            "<ListBucketResult><IsTruncated>false</IsTruncated>"
        });
        for object in objects {
            let key = object.location.as_ref();
            if key.starts_with(prefix) && key > offset {
                let key = key
                    .replace('&', "&amp;")
                    .replace('<', "&lt;")
                    .replace('>', "&gt;");
                if azure {
                    xml.push_str(&format!(
                        "<Blob><Name>{key}</Name><Properties><Last-Modified>{}</Last-Modified><Content-Length>{}</Content-Length><Content-Type>application/octet-stream</Content-Type></Properties></Blob>",
                        object.last_modified.format("%a, %d %b %Y %H:%M:%S GMT"), object.size,
                    ));
                } else {
                    xml.push_str(&format!(
                        "<Contents><Key>{key}</Key><LastModified>{}</LastModified><Size>{}</Size></Contents>",
                        object.last_modified.to_rfc3339(), object.size,
                    ));
                }
            }
        }
        xml.push_str(if azure {
            "</Blobs></EnumerationResults>"
        } else {
            "</ListBucketResult>"
        });
        return Ok(Response::builder()
            .header("Content-Type", "application/xml")
            .body(xml.into())?);
    }
    let key = StorePath::from_url_path(url.path().strip_prefix("/bucket/").ok_or("wrong bucket")?)?;
    let object = match store.get(&key).await {
        Ok(object) => object,
        Err(object_store::Error::NotFound { .. }) => {
            return Ok(Response::builder()
                .status(404)
                .body(bytes::Bytes::new().into())?);
        }
        Err(error) => return Err(error.into()),
    };
    let mut response = Response::builder()
        .header("ETag", object.meta.e_tag.as_deref().unwrap_or_default())
        .header(
            "Last-Modified",
            object
                .meta
                .last_modified
                .format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string(),
        );
    let mut body = object.bytes().await?;
    if let Some(range) = request.headers().get("Range") {
        let (start, end) = range
            .to_str()?
            .strip_prefix("bytes=")
            .and_then(|s| s.split_once('-'))
            .ok_or("invalid range")?;
        let range = match (start, end) {
            ("", end) => GetRange::Suffix(end.parse()?),
            (start, "") => GetRange::Offset(start.parse()?),
            (start, end) => GetRange::Bounded(
                start.parse()?..end.parse::<u64>()?.checked_add(1).ok_or("range overflow")?,
            ),
        }
        .as_range(body.len() as u64)?;
        response = response.status(206).header(
            "Content-Range",
            format!("bytes {}-{}/{}", range.start, range.end - 1, body.len()),
        );
        body = body.slice(range.start as usize..range.end as usize);
    }
    response = response.header("Content-Length", body.len());
    if request.method() == "HEAD" {
        body = bytes::Bytes::new();
    } else {
        assert_eq!(request.method(), "GET");
    }
    Ok(response.body(body.into())?)
}
