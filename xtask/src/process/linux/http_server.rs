use std::{
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};

use http_body_util::Full;
use hyper::{
    Method, Request, Response, Uri,
    body::{Bytes, Incoming},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use snafu::ResultExt;
use tokio::{net::TcpListener, runtime::Runtime, task::JoinSet};

use crate::error::{InvalidInputSnafu, IoSnafu, Result};

pub(super) type HttpResponse = Response<Full<Bytes>>;

#[derive(Debug)]
pub(super) struct RecordedRequest {
    pub connection_id: u64,
    pub method: Method,
    pub uri: Uri,
}

enum Event {
    Request(RecordedRequest),
    Disconnected(u64),
    Failed(String),
}

pub(super) struct HttpTestServer {
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
        let (send, events) = mpsc::channel();
        let respond = Arc::new(respond);
        runtime.spawn(async move {
            let mut connections = JoinSet::new();
            let mut next_connection = 0;
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let socket = match accepted {
                            Ok((socket, _)) => socket,
                            Err(error) => {
                                let _ = send.send(Event::Failed(format!("accept failed: {error}")));
                                return;
                            }
                        };
                        let connection_id = next_connection;
                        next_connection += 1;
                        let send = send.clone();
                        let respond = Arc::clone(&respond);
                        connections.spawn(async move {
                            let notifications = send.clone();
                            let service = service_fn(move |request: Request<Incoming>| {
                                let _ = notifications.send(Event::Request(RecordedRequest {
                                    connection_id, method: request.method().clone(), uri: request.uri().clone(),
                                }));
                                let result = respond(&request);
                                if let Err(error) = &result {
                                    let _ = notifications.send(Event::Failed(error.to_string()));
                                }
                                async move {
                                    match result? {
                                        Some(response) => Ok::<_, crate::error::Error>(response),
                                        None => std::future::pending().await,
                                    }
                                }
                            });
                            // Client cancellation is expected in the signal and prefetch checks.
                            let _ = http1::Builder::new().keep_alive(false)
                                .serve_connection(TokioIo::new(socket), service).await;
                            let _ = send.send(Event::Disconnected(connection_id));
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
            origin: format!("http://{address}"),
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
                Event::Disconnected(_) => {}
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
            if matches!(self.next_event(deadline, "the table connection to close")?, Event::Disconnected(id) if id == connection_id)
            {
                return Ok(());
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

#[cfg(test)]
mod tests;
