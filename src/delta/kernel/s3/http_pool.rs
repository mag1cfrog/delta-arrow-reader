//! A small HTTP/1 pool for S3 partial reads. Setup permits are acquired before
//! starting a connection, and remain held until a started connection completes.

use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, LazyLock, OnceLock},
    task::{Context, Poll},
    time::Duration,
};

use futures_util::future::BoxFuture;
use hyper::{
    Method, Uri,
    body::{Body, Bytes, Frame, Incoming, SizeHint},
    client::conn::http1::{self, SendRequest},
    header::{HOST, USER_AGENT},
};
use hyper_util::{
    client::{
        legacy::connect::{HttpConnector, dns::GaiResolver},
        pool::cache,
        proxy::matcher::Matcher,
    },
    rt::TokioExecutor,
};
use object_store::client::{
    HttpClient, HttpError, HttpErrorKind, HttpRequest, HttpRequestBody, HttpResponse,
    HttpResponseBody, HttpService,
};
use rand::seq::SliceRandom;
use tokio::{
    sync::Semaphore,
    time::{Instant, Sleep, timeout, timeout_at},
};
use tower::{
    Layer, Service, ServiceExt, limit::concurrency::GlobalConcurrencyLimitLayer, service_fn,
    util::BoxCloneSyncService,
};

type Error = Box<dyn std::error::Error + Send + Sync>;

// Shared across tables. Active reads already share the reader's request budget.
static CONNECTING: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(64)));

pub(super) fn client(allow_http: bool) -> Result<HttpClient, Error> {
    build_client(
        allow_http,
        Arc::clone(&CONNECTING),
        Duration::from_secs(30),
        Duration::from_secs(5),
        Matcher::from_system(),
    )
}

fn build_client(
    allow_http: bool,
    connections: Arc<Semaphore>,
    request_timeout: Duration,
    connect_timeout: Duration,
    proxies: Matcher,
) -> Result<HttpClient, Error> {
    // Match the SDK's default address shuffling, including native DNS resolution.
    let dns = service_fn(|name| async move {
        let mut addresses: Vec<_> = GaiResolver::new().oneshot(name).await?.collect();
        addresses.shuffle(&mut rand::rng());
        Ok::<_, io::Error>(addresses.into_iter())
    });
    let mut tcp = HttpConnector::new_with_resolver(dns);
    tcp.enforce_http(false);
    tcp.set_nodelay(true);
    // Preserve the ordinary SDK client's TCP liveness settings.
    tcp.set_keepalive(Some(Duration::from_secs(15)));
    tcp.set_keepalive_interval(Some(Duration::from_secs(15)));
    tcp.set_keepalive_retries(Some(3));
    #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
    tcp.set_tcp_user_timeout(Some(Duration::from_secs(30)));
    let tls = match hyper_rustls::HttpsConnectorBuilder::new()
        // Reader dependencies also enable aws-lc. Avoid an ambiguous process default.
        .with_provider_and_native_roots(rustls::crypto::ring::default_provider())
    {
        Ok(tls) => tls,
        // Plain HTTP needs no CA bundle. An empty trust store still rejects
        // HTTPS certificates; allowing HTTP must never disable TLS validation.
        Err(_) if allow_http => hyper_rustls::HttpsConnectorBuilder::new().with_tls_config(
            rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()?
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth(),
        ),
        Err(error) => return Err(error.into()),
    }
    .https_or_http()
    .enable_http1()
    .wrap_connector(tcp);
    let connector = service_fn(
        move |uri: Uri| -> BoxFuture<'static, Result<Connection, HttpError>> {
            let tls = tls.clone();
            Box::pin(async move {
                timeout(connect_timeout, async move {
                    let stream = tls
                        .oneshot(uri)
                        .await
                        .map_err(|e| HttpError::new(HttpErrorKind::Connect, io::Error::other(e)))?;
                    let (sender, driver) = http1::handshake(stream)
                        .await
                        .map_err(|e| HttpError::new(HttpErrorKind::Connect, e))?;
                    tokio::spawn(async move {
                        if let Err(error) = driver.await {
                            tracing::debug!(%error, "HTTP/1 connection closed with an error");
                        }
                    });
                    Ok(Connection {
                        sender,
                        idle_since: Instant::now(),
                    })
                })
                .await
                .map_err(|e| HttpError::new(HttpErrorKind::Timeout, e))?
            })
        },
    );
    let connector = BoxCloneSyncService::new(connector);
    let pool = Arc::new(
        cache::builder()
            .executor(TokioExecutor::new())
            .build(GlobalConcurrencyLimitLayer::with_semaphore(connections).layer(connector)),
    );
    let weak = Arc::downgrade(&pool);
    let cleanup = move || {
        let Some(pool) = weak.upgrade() else {
            return false;
        };
        let mut retained = 0;
        let mut pool = pool.as_ref().clone();
        pool.retain(|connection| {
            let keep = !connection.sender.is_closed()
                && connection.idle_since.elapsed() < Duration::from_secs(90)
                && retained < 64;
            retained += usize::from(keep);
            keep
        });
        true
    };
    let idle_cleanup = cleanup.clone();
    tokio::spawn(async move {
        // Background connects can add idle entries without a response being returned.
        // ponytail: idle retention is a soft cap; the cache API has no insertion limit.
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if !idle_cleanup() {
                break;
            }
        }
    });
    // This store shares its credential provider with the ordinary SDK store.
    // Its data requests have one origin, resolved by the SDK, not reconstructed here.
    let origin = Arc::new(OnceLock::<Uri>::new());
    let proxies = Arc::new(proxies);
    let service = service_fn(
        move |mut request: HttpRequest| -> BoxFuture<'static, Result<HttpResponse, HttpError>> {
            let pool = pool.clone();
            let origin = origin.clone();
            let cleanup = cleanup.clone();
            let proxies = proxies.clone();
            Box::pin(async move {
                if !matches!(*request.method(), Method::GET | Method::HEAD)
                    || !request.body().is_empty()
                    || request.uri().authority().is_none()
                    || !(request.uri().scheme_str() == Some("https")
                        || (allow_http && request.uri().scheme_str() == Some("http")))
                    || proxies.intercept(request.uri()).is_some()
                {
                    return Err(unsupported("unsupported partial-read transport settings"));
                }
                let origin = origin.get_or_init(|| request.uri().clone()).clone();
                if request.uri().scheme() != origin.scheme()
                    || request.uri().authority() != origin.authority()
                {
                    return Err(unsupported("partial-read origin changed"));
                }
                let deadline = Instant::now() + request_timeout;
                let work = async move {
                    let mut connection = loop {
                        let cache = pool.as_ref().clone();
                        let mut connection = cache.oneshot(origin.clone()).await?;
                        // A server may have closed an idle connection. Cache's Service
                        // implementation discards it after poll_ready reports the error.
                        if connection.ready().await.is_ok() {
                            break connection;
                        }
                    };
                    if !request.headers().contains_key(HOST) {
                        request.headers_mut().insert(
                            HOST,
                            origin
                                .authority()
                                .ok_or_else(|| unsupported("missing HTTP authority"))?
                                .as_str()
                                .parse()
                                .map_err(|_| unsupported("invalid Host header"))?,
                        );
                    }
                    request.headers_mut().entry(USER_AGENT).or_insert(
                        hyper::header::HeaderValue::from_static(concat!(
                            "delta-arrow-reader/",
                            env!("CARGO_PKG_VERSION")
                        )),
                    );
                    *request.uri_mut() = request
                        .uri()
                        .path_and_query()
                        .map_or("/", |path| path.as_str())
                        .parse()
                        .map_err(|_| unsupported("invalid request path"))?;
                    let (parts, body) = connection.call(request).await?.into_parts();
                    let release = Box::new(move || {
                        connection.inner_mut().idle_since = Instant::now();
                        drop(connection);
                        cleanup();
                    });
                    let body = LeasedBody {
                        body: Some(body),
                        deadline: Box::pin(tokio::time::sleep_until(deadline)),
                        release: Some(release),
                    };
                    Ok(HttpResponse::from_parts(parts, HttpResponseBody::new(body)))
                };
                timeout_at(deadline, work)
                    .await
                    .map_err(|e| HttpError::new(HttpErrorKind::Timeout, e))?
            })
        },
    );
    Ok(HttpClient::new(PoolClient(BoxCloneSyncService::new(
        service,
    ))))
}

#[derive(Debug)]
struct Connection {
    sender: SendRequest<HttpRequestBody>,
    idle_since: Instant,
}

impl Service<HttpRequest> for Connection {
    type Response = hyper::Response<Incoming>;
    type Error = HttpError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.sender
            .poll_ready(cx)
            .map_err(|e| HttpError::new(HttpErrorKind::Connect, e))
    }
    fn call(&mut self, request: HttpRequest) -> Self::Future {
        let response = self.sender.send_request(request);
        Box::pin(async move {
            response
                .await
                .map_err(|e| HttpError::new(HttpErrorKind::Request, e))
        })
    }
}

#[derive(Debug)]
struct PoolClient(BoxCloneSyncService<HttpRequest, HttpResponse, HttpError>);

#[async_trait::async_trait]
impl HttpService for PoolClient {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        self.0.clone().oneshot(request).await
    }
}

struct LeasedBody {
    body: Option<Incoming>,
    deadline: Pin<Box<Sleep>>,
    release: Option<Box<dyn FnOnce() + Send + Sync>>,
}

impl LeasedBody {
    fn finish(&mut self) {
        // Drop/cancel the response before making its sender available for reuse.
        drop(self.body.take());
        if let Some(release) = self.release.take() {
            release()
        }
    }
}

impl Drop for LeasedBody {
    fn drop(&mut self) {
        self.finish()
    }
}

impl Body for LeasedBody {
    type Data = Bytes;
    type Error = HttpError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, HttpError>>> {
        if self.body.is_none() {
            return Poll::Ready(None);
        }
        if self.deadline.as_mut().poll(cx).is_ready() {
            self.finish();
            return Poll::Ready(Some(Err(HttpError::new(
                HttpErrorKind::Timeout,
                io::Error::new(io::ErrorKind::TimedOut, "response deadline exceeded"),
            ))));
        }
        let Some(body) = self.body.as_mut() else {
            return Poll::Ready(None);
        };
        let result = Pin::new(body).poll_frame(cx);
        match result {
            Poll::Ready(Some(Err(error))) => {
                self.finish();
                Poll::Ready(Some(Err(HttpError::new(HttpErrorKind::Interrupted, error))))
            }
            Poll::Ready(None) => {
                self.finish();
                Poll::Ready(None)
            }
            Poll::Ready(Some(Ok(frame))) => Poll::Ready(Some(Ok(frame))),
            Poll::Pending => Poll::Pending,
        }
    }
    fn is_end_stream(&self) -> bool {
        self.body.as_ref().is_none_or(Body::is_end_stream)
    }
    fn size_hint(&self) -> SizeHint {
        self.body
            .as_ref()
            .map_or_else(SizeHint::default, Body::size_hint)
    }
}

fn unsupported(reason: &'static str) -> HttpError {
    HttpError::new(HttpErrorKind::Unknown, io::Error::other(reason))
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    reason = "local HTTP contract tests fail on any unexpected error"
)]
mod tests {
    use super::{Matcher, build_client};
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use futures_util::StreamExt;
    use hyper::{Request, Uri};
    use object_store::client::{HttpClient, HttpErrorKind, HttpRequestBody};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::{Semaphore, mpsc},
        task::{JoinHandle, JoinSet},
    };

    struct Server {
        origin: Uri,
        seen: mpsc::UnboundedReceiver<String>,
        active: Arc<AtomicUsize>,
        task: JoinHandle<()>,
    }
    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort()
        }
    }
    struct ActiveConnection(Arc<AtomicUsize>);
    impl Drop for ActiveConnection {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    async fn server() -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let (send, seen) = mpsc::unbounded_channel();
        let active = Arc::new(AtomicUsize::new(0));
        let counter = active.clone();
        let task = tokio::spawn(async move {
            let mut tasks = JoinSet::<()>::new();
            let mut next_id = 0;
            loop {
                let (mut socket, _) = tokio::select! {
                    biased;
                    Some(completed) = tasks.join_next() => { completed.unwrap(); continue; }
                    accepted = listener.accept() => accepted.unwrap(),
                };
                let id = next_id;
                next_id += 1;
                counter.fetch_add(1, Ordering::SeqCst);
                let guard = ActiveConnection(counter.clone());
                let send = send.clone();
                tasks.spawn(async move {
                let _guard = guard;
                loop {
                    let mut head = Vec::new();
                    while !head.ends_with(b"\r\n\r\n") {
                        let Ok(byte) = socket.read_u8().await else { return };
                        head.push(byte);
                        assert!(head.len() < 8192);
                    }
                    let head = String::from_utf8(head).unwrap();
                    let path = head.split_whitespace().nth(1).unwrap();
                    send.send(head.clone()).unwrap();
                    if path == "/headers-wait" {
                        let mut byte = [0];
                        assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
                        return;
                    }
                    let status = if path == "/missing" { "404 Not Found" } else { "200 OK" };
                    let length = match path { "/body-wait" => 16, "/truncated" => 8, _ => 2 };
                    let close = if path == "/close" { "Connection: close\r\n" } else { "" };
                    let payload = if head.starts_with("HEAD ") { "" } else { "ok" };
                    let response = format!("HTTP/1.1 {status}\r\nContent-Length: {length}\r\nX-Connection: {id}\r\n{close}\r\n{payload}");
                    if socket.write_all(response.as_bytes()).await.is_err() { return }
                    match path {
                        "/body-wait" => {
                            let mut byte = [0];
                            // An unread response must never receive the next request.
                            assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
                            return;
                        }
                        "/truncated" | "/close" => return,
                        _ => (),
                    }
                }
            });
            }
        });
        Server {
            origin,
            seen,
            active,
            task,
        }
    }

    async fn request(
        client: &HttpClient,
        origin: &Uri,
        path: &str,
    ) -> Result<object_store::client::HttpResponse, object_store::client::HttpError> {
        client
            .execute(
                Request::builder()
                    .uri(format!(
                        "{}://{}{path}",
                        origin.scheme_str().unwrap(),
                        origin.authority().unwrap()
                    ))
                    .header("Range", "bytes=0-1")
                    .header("If-Match", "\"etag\"")
                    .body(HttpRequestBody::empty())
                    .unwrap(),
            )
            .await
    }

    async fn healthy(client: &HttpClient, origin: &Uri) -> String {
        let response = request(client, origin, "/data?encoded=%2F").await.unwrap();
        let id = response.headers()["x-connection"]
            .to_str()
            .unwrap()
            .to_owned();
        assert_eq!(&response.into_body().bytes().await.unwrap()[..], b"ok");
        id
    }

    #[tokio::test]
    async fn streaming_reuse_cancellation_timeouts_and_server_errors() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut server = server().await;
            let budget = Arc::new(Semaphore::new(1));
            let client = build_client(
                true,
                budget.clone(),
                Duration::from_secs(2),
                Duration::from_millis(100),
                Matcher::builder().build(),
            )
            .unwrap();

            let first = healthy(&client, &server.origin).await;
            for _ in 0..10 {
                assert_eq!(healthy(&client, &server.origin).await, first)
            }
            // Idle checkout must work even while every setup permit is occupied.
            let permit = budget.acquire().await.unwrap();
            let concurrent = (0..32).map(|_| healthy(&client, &server.origin));
            for id in futures_util::future::join_all(concurrent).await {
                assert_eq!(id, first);
            }
            drop(permit);
            let head = server.seen.recv().await.unwrap().to_ascii_lowercase();
            assert!(head.starts_with("get /data?encoded=%2f http/1.1\r\n"));
            assert!(head.contains("range: bytes=0-1\r\n"));
            assert!(head.contains("if-match: \"etag\"\r\n"));
            assert!(head.contains(concat!(
                "user-agent: delta-arrow-reader/",
                env!("CARGO_PKG_VERSION"),
                "\r\n"
            )));
            assert!(head.contains(&format!("host: {}\r\n", server.origin.authority().unwrap())));
            let head = client
                .execute(
                    Request::builder()
                        .method("HEAD")
                        .uri(format!(
                            "http://{}/data",
                            server.origin.authority().unwrap()
                        ))
                        .body(HttpRequestBody::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert!(head.into_body().bytes().await.unwrap().is_empty());
            assert_eq!(healthy(&client, &server.origin).await, first);

            let slow = request(&client, &server.origin, "/body-wait")
                .await
                .unwrap();
            let slow_id = slow.headers()["x-connection"].to_str().unwrap().to_owned();
            let mut body = slow.into_body().bytes_stream();
            assert_eq!(&body.next().await.unwrap().unwrap()[..], b"ok");
            assert_ne!(healthy(&client, &server.origin).await, slow_id);
            drop(body);
            healthy(&client, &server.origin).await;

            let pending = tokio::spawn({
                let client = client.clone();
                let origin = server.origin.clone();
                async move { request(&client, &origin, "/headers-wait").await }
            });
            while !server
                .seen
                .recv()
                .await
                .unwrap()
                .starts_with("GET /headers-wait ")
            {}
            pending.abort();
            assert!(pending.await.unwrap_err().is_cancelled());
            healthy(&client, &server.origin).await;

            let truncated = request(&client, &server.origin, "/truncated")
                .await
                .unwrap();
            assert_eq!(
                truncated.into_body().bytes().await.unwrap_err().kind(),
                HttpErrorKind::Interrupted
            );
            healthy(&client, &server.origin).await;

            let missing = request(&client, &server.origin, "/missing").await.unwrap();
            assert_eq!(missing.status(), 404);
            missing.into_body().bytes().await.unwrap();
            let closing = request(&client, &server.origin, "/close").await.unwrap();
            let old_id = closing.headers()["x-connection"]
                .to_str()
                .unwrap()
                .to_owned();
            closing.into_body().bytes().await.unwrap();
            assert_ne!(healthy(&client, &server.origin).await, old_id);

            let short = build_client(
                true,
                budget.clone(),
                Duration::from_millis(100),
                Duration::from_millis(100),
                Matcher::builder().build(),
            )
            .unwrap();
            let delayed = request(&short, &server.origin, "/body-wait").await.unwrap();
            assert_eq!(
                delayed.into_body().bytes().await.unwrap_err().kind(),
                HttpErrorKind::Timeout
            );
            healthy(&short, &server.origin).await;
            assert_eq!(
                request(&short, &server.origin, "/headers-wait")
                    .await
                    .unwrap_err()
                    .kind(),
                HttpErrorKind::Timeout
            );
            healthy(&short, &server.origin).await;

            // Capacity waiting is included in the total request deadline.
            let waiting = build_client(
                true,
                budget.clone(),
                Duration::from_millis(100),
                Duration::from_millis(100),
                Matcher::builder().build(),
            )
            .unwrap();
            let permit = budget.acquire().await.unwrap();
            assert_eq!(
                request(&waiting, &server.origin, "/data")
                    .await
                    .unwrap_err()
                    .kind(),
                HttpErrorKind::Timeout
            );
            drop(permit);
            healthy(&waiting, &server.origin).await;

            // The HTTP test server never sends a TLS handshake response.
            let tls_origin: Uri = server
                .origin
                .to_string()
                .replacen("http:", "https:", 1)
                .parse()
                .unwrap();
            let tls = build_client(
                true,
                budget.clone(),
                Duration::from_secs(1),
                Duration::from_millis(100),
                Matcher::builder().build(),
            )
            .unwrap();
            assert_eq!(
                request(&tls, &tls_origin, "/data")
                    .await
                    .unwrap_err()
                    .kind(),
                HttpErrorKind::Timeout
            );
            assert_eq!(budget.available_permits(), 1);

            let wrong: Uri = "https://example.invalid".parse().unwrap();
            assert_eq!(
                request(&client, &wrong, "/data").await.unwrap_err().kind(),
                HttpErrorKind::Unknown
            );
            drop((client, short, waiting, tls));
            tokio::time::timeout(Duration::from_secs(1), async {
                while server.active.load(Ordering::SeqCst) != 0 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            tokio::task::yield_now().await;
            assert!(!server.task.is_finished(), "local HTTP server failed");
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn http_option_spellings_match_the_sdk() {
        use crate::delta::kernel::s3::PartialReadConnector;
        use object_store::client::{
            ClientConfigKey, ClientOptions, HttpConnector, ReqwestConnector,
        };

        let server = server().await;
        for value in [
            "true", "TRUE", "1", "yes", "ON", "Y", "false", "FALSE", "0", "no", "OFF", "N",
        ] {
            let options = ClientOptions::new().with_config(ClientConfigKey::AllowHttp, value);
            let ordinary = ReqwestConnector {}.connect(&options).unwrap();
            let partial = PartialReadConnector.connect(&options).unwrap();
            let expected = request(&ordinary, &server.origin, "/data").await;
            let actual = request(&partial, &server.origin, "/data").await;
            assert_eq!(actual.is_ok(), expected.is_ok(), "allow_http={value}");
            for response in [expected, actual].into_iter().flatten() {
                assert_eq!(&response.into_body().bytes().await.unwrap()[..], b"ok");
            }
        }
    }

    #[tokio::test]
    #[ignore = "run with python3 tests/reader/https.py; requires isolated trust settings and local TLS servers"]
    async fn http_without_roots_preserves_https_verification() {
        let empty_roots = std::env::var("DAR_TLS_EMPTY_ROOTS").unwrap() == "true";
        let server = server().await;
        let http = super::client(true).unwrap();
        healthy(&http, &server.origin).await;

        if empty_roots {
            assert!(super::client(false).is_err());
        }
        let trusted = std::env::var("DAR_TLS_ENDPOINT").unwrap();
        for (endpoint, should_trust) in [
            (trusted.clone(), !empty_roots),
            (std::env::var("DAR_TLS_UNTRUSTED_ENDPOINT").unwrap(), false),
            (trusted.replace("localhost", "127.0.0.1"), false),
        ] {
            // Each store accepts one origin. Use a fresh client for each server.
            let client = super::client(true).unwrap();
            let response = request(&client, &endpoint.parse().unwrap(), "/pool-probe").await;
            if should_trust {
                assert_eq!(
                    &response.unwrap().into_body().bytes().await.unwrap()[..],
                    b"ok"
                );
            } else {
                let error = response.unwrap_err();
                assert!(
                    format!("{error:?}")
                        .to_ascii_lowercase()
                        .contains("certificate"),
                    "expected certificate validation failure: {error:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn reject_proxy_and_disallowed_http_without_connecting() {
        let mut server = server().await;
        let budget = Arc::new(Semaphore::new(1));
        for (allow_http, proxies) in [
            (false, Matcher::builder().build()),
            (
                true,
                Matcher::builder().all("http://127.0.0.1:1234").build(),
            ),
        ] {
            let client = build_client(
                allow_http,
                budget.clone(),
                Duration::from_secs(1),
                Duration::from_secs(1),
                proxies,
            )
            .unwrap();
            assert_eq!(
                request(&client, &server.origin, "/data")
                    .await
                    .unwrap_err()
                    .kind(),
                HttpErrorKind::Unknown
            );
        }
        assert!(server.seen.try_recv().is_err());
        assert_eq!(server.active.load(Ordering::SeqCst), 0);
    }
}
