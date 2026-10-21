use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use hyper::{Method, StatusCode, body::Bytes};
use hyper_util::rt::TokioIo;
use snafu::ResultExt;
use tokio::{io::copy_bidirectional, net::TcpStream, time::timeout};

use crate::{
    error::{InvalidInputSnafu, IoSnafu, Result},
    http_fixture::{HttpTestServer, table::response},
};

pub(super) struct ConnectProxy {
    pub server: HttpTestServer,
    pub upstream_ports: Arc<Mutex<BTreeSet<u16>>>,
}

impl ConnectProxy {
    pub fn start(ports: [u16; 2]) -> Result<Self> {
        let upstream_ports = Arc::new(Mutex::new(BTreeSet::new()));
        let recorded_ports = Arc::clone(&upstream_ports);
        let server = HttpTestServer::start_async(move |mut request| {
            let recorded_ports = Arc::clone(&recorded_ports);
            async move {
                let Some(port) = request.uri().port_u16().filter(|port| ports.contains(port))
                else {
                    return Ok(Some(response(StatusCode::FORBIDDEN, Bytes::new())));
                };
                if request.method() != Method::CONNECT
                    || !matches!(request.uri().host(), Some("localhost" | "127.0.0.1"))
                {
                    return Ok(Some(response(StatusCode::FORBIDDEN, Bytes::new())));
                }
                let mut upstream = timeout(
                    Duration::from_secs(10),
                    TcpStream::connect(("127.0.0.1", port)),
                )
                .await
                .unwrap_or_else(|error| {
                    Err(std::io::Error::new(std::io::ErrorKind::TimedOut, error))
                })
                .context(IoSnafu {
                    operation: "connect the HTTPS test proxy",
                })?;
                let local = upstream.local_addr().context(IoSnafu {
                    operation: "read the HTTPS test proxy source port",
                })?;
                recorded_ports
                    .lock()
                    .map_err(|_| {
                        InvalidInputSnafu {
                            message: "HTTPS proxy port log is poisoned",
                        }
                        .build()
                    })?
                    .insert(local.port());
                let upgraded = hyper::upgrade::on(&mut request);
                tokio::spawn(async move {
                    // Certificate rejection can close either end before the tunnel finishes.
                    let _ = timeout(Duration::from_secs(10), async move {
                        let mut downstream = TokioIo::new(upgraded.await?);
                        copy_bidirectional(&mut downstream, &mut upstream).await?;
                        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
                    })
                    .await;
                });
                Ok(Some(response(StatusCode::OK, Bytes::new())))
            }
        })?;
        Ok(Self {
            server,
            upstream_ports,
        })
    }
}
