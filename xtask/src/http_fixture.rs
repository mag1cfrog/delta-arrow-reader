use std::{
    future::{Future, ready},
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};

use http_body_util::Full;
use hyper::{
    Method, Request, Response, Uri,
    body::{Body, Bytes, Incoming},
    header::{HeaderValue, RANGE},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use snafu::ResultExt;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
    runtime::Runtime,
    task::JoinSet,
};
use tokio_rustls::{TlsAcceptor, rustls::ServerConfig};

use crate::error::{InvalidInputSnafu, IoSnafu, Result};

pub(crate) type HttpResponse = Response<Full<Bytes>>;

pub(crate) mod table;
pub(crate) mod tls;

#[derive(Debug)]
pub(crate) struct RecordedRequest {
    pub connection_id: u64,
    pub peer_port: u16,
    pub method: Method,
    pub uri: Uri,
    pub range: Option<HeaderValue>,
}

enum Event {
    Request(RecordedRequest),
    Disconnected {
        connection_id: u64,
        response_completed: bool,
    },
    Failed(String),
}

pub(crate) struct HttpTestServer {
    pub origin: String,
    events: mpsc::Receiver<Event>,
    // Dropping the runtime cancels the listener and every connection task.
    _runtime: Runtime,
}

impl HttpTestServer {
    /// Returning None holds the response until the client disconnects.
    pub fn start(
        respond: impl Fn(&Request<Incoming>) -> Result<Option<HttpResponse>> + Send + Sync + 'static,
    ) -> Result<Self> {
        Self::start_async(move |request| ready(respond(&request)))
    }

    pub fn start_tls(
        config: ServerConfig,
        respond: impl Fn(&Request<Incoming>) -> Result<Option<HttpResponse>> + Send + Sync + 'static,
    ) -> Result<Self> {
        Self::start_listener(Some(TlsAcceptor::from(Arc::new(config))), move |request| {
            ready(respond(&request))
        })
    }

    pub fn start_async<F, B>(
        respond: impl Fn(Request<Incoming>) -> F + Send + Sync + 'static,
    ) -> Result<Self>
    where
        F: Future<Output = Result<Option<Response<B>>>> + Send + 'static,
        B: Body<Data = Bytes> + Send + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Self::start_listener(None, respond)
    }

    fn start_listener<F, B>(
        tls: Option<TlsAcceptor>,
        respond: impl Fn(Request<Incoming>) -> F + Send + Sync + 'static,
    ) -> Result<Self>
    where
        F: Future<Output = Result<Option<Response<B>>>> + Send + 'static,
        B: Body<Data = Bytes> + Send + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_io()
            .enable_time()
            .build()
            .context(IoSnafu {
                operation: "start the HTTP fixture runtime",
            })?;
        let listener = runtime
            .block_on(TcpListener::bind(("127.0.0.1", 0)))
            .context(IoSnafu {
                operation: "bind the HTTP fixture listener",
            })?;
        let address = listener.local_addr().context(IoSnafu {
            operation: "read the HTTP fixture address",
        })?;
        let origin = if tls.is_some() {
            format!("https://localhost:{}", address.port())
        } else {
            format!("http://{address}")
        };
        let (send, events) = mpsc::channel();
        let respond = Arc::new(respond);
        runtime.spawn(async move {
            let mut connections = JoinSet::new();
            let mut next_connection = 0;
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (socket, peer) = match accepted {
                            Ok(connection) => connection,
                            Err(error) => {
                                let _ = send.send(Event::Failed(format!("accept failed: {error}")));
                                return;
                            }
                        };
                        let connection_id = next_connection;
                        next_connection += 1;
                        let send = send.clone();
                        let respond = Arc::clone(&respond);
                        let tls = tls.clone();
                        connections.spawn(async move {
                            let response_completed = if let Some(tls) = tls {
                                // Rejected certificates must never reach the HTTP handler.
                                if let Ok(socket) = tls.accept(socket).await {
                                    serve_connection(socket, connection_id, peer.port(), send.clone(), respond).await
                                } else {
                                    false
                                }
                            } else {
                                serve_connection(socket, connection_id, peer.port(), send.clone(), respond).await
                            };
                            let _ = send.send(Event::Disconnected { connection_id, response_completed });
                        });
                    }
                    Some(result) = connections.join_next() => {
                        if let Err(error) = result {
                            let _ = send.send(Event::Failed(format!("connection task failed: {error}")));
                        }
                    }
                }
            }
        });
        Ok(Self {
            origin,
            events,
            _runtime: runtime,
        })
    }

    pub fn take_requests(&self) -> Result<Vec<RecordedRequest>> {
        let mut requests = Vec::new();
        for event in self.events.try_iter() {
            match event {
                Event::Request(request) => requests.push(request),
                Event::Failed(message) => return InvalidInputSnafu { message }.fail(),
                Event::Disconnected { .. } => {}
            }
        }
        Ok(requests)
    }

    pub fn wait_for_request(&self, timeout: Duration) -> Result<RecordedRequest> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Event::Request(request) = self.next_event(deadline, "a table request")? {
                return Ok(request);
            }
        }
    }

    pub fn wait_for_disconnect(&self, connection_id: u64, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if matches!(self.next_event(deadline, "the table connection to close")?, Event::Disconnected { connection_id: id, .. } if id == connection_id)
            {
                return Ok(());
            }
        }
    }

    pub fn wait_for_response_complete(&self, connection_id: u64, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Event::Disconnected {
                connection_id: id,
                response_completed,
            } = self.next_event(deadline, "the table response to finish")?
                && id == connection_id
            {
                return if response_completed {
                    Ok(())
                } else {
                    InvalidInputSnafu {
                        message: "table connection closed before its response completed",
                    }
                    .fail()
                };
            }
        }
    }

    fn next_event(&self, deadline: Instant, expected: &str) -> Result<Event> {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| {
                InvalidInputSnafu {
                    message: format!("timed out waiting for {expected}"),
                }
                .build()
            })?;
        let event = self.events.recv_timeout(remaining).map_err(|error| {
            InvalidInputSnafu {
                message: format!("waiting for {expected}: {error}"),
            }
            .build()
        })?;
        match event {
            Event::Failed(message) => InvalidInputSnafu { message }.fail(),
            event => Ok(event),
        }
    }
}

async fn serve_connection<F, B>(
    socket: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
    connection_id: u64,
    peer_port: u16,
    send: mpsc::Sender<Event>,
    respond: Arc<impl Fn(Request<Incoming>) -> F>,
) -> bool
where
    F: Future<Output = Result<Option<Response<B>>>>,
    B: Body<Data = Bytes> + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let service = service_fn(move |request: Request<Incoming>| {
        let _ = send.send(Event::Request(RecordedRequest {
            connection_id,
            peer_port,
            method: request.method().clone(),
            uri: request.uri().clone(),
            range: request.headers().get(RANGE).cloned(),
        }));
        let result = respond(request);
        let send = send.clone();
        async move {
            let result = result.await;
            if let Err(error) = &result {
                let _ = send.send(Event::Failed(error.to_string()));
            }
            match result? {
                Some(response) => Ok::<_, crate::error::Error>(response),
                None => std::future::pending().await,
            }
        }
    });
    // Distinguish a flushed response from client cancellation when a check needs completion.
    http1::Builder::new()
        .keep_alive(false)
        .serve_connection(TokioIo::new(socket), service)
        .with_upgrades()
        .await
        .is_ok()
}

#[cfg(test)]
mod tests;
