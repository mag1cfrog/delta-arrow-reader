use std::{
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use http_body_util::{BodyExt, Either, channel::Channel};
use hyper::{Method, Response, body::Bytes};
use snafu::ensure;
use tokio::sync::watch;

use super::{
    ScanFixture,
    child::ScanProcess,
    fixtures::{create_repeated_table, expand_repeated_table},
    http::DataRequests,
};
use crate::{
    error::{InvalidInputSnafu, Result},
    http_fixture::{HttpTestServer, table::HttpTable},
};

impl ScanFixture<'_> {
    pub(super) fn check_slow_consumer(&self) -> Result<()> {
        let table_path = self.directory.path().join("repeated-table");
        create_repeated_table(&self.corpus.join("partitioned/table"), &table_path, 8)?;
        expand_repeated_table(&table_path)?;
        let table = Arc::new(HttpTable::from_directory(&table_path)?);
        let requests = Arc::new(DataRequests::default());
        let recorded = Arc::clone(&requests);
        let (response_ready, response_started) = watch::channel(false);
        let (release_body, body_released) = watch::channel(false);
        let server = HttpTestServer::start_async(move |request| {
            let table = Arc::clone(&table);
            let recorded = Arc::clone(&recorded);
            let response_ready = response_ready.clone();
            let mut response_started = response_started.clone();
            let mut body_released = body_released.clone();
            async move {
                let path = request.uri().path();
                recorded.record(path)?;
                let response = table.respond(&request)?;
                if path == "/part-003.parquet" && request.method() == Method::GET {
                    let (parts, body) = response.into_parts();
                    let bytes = body
                        .collect()
                        .await
                        .expect("a full fixture body cannot fail")
                        .to_bytes();
                    let (mut sender, body) = Channel::<Bytes>::new(1);
                    response_ready.send_replace(true);
                    tokio::spawn(async move {
                        if body_released.wait_for(|released| *released).await.is_ok() {
                            // Client cancellation is handled by the scan's exit and diagnostic checks.
                            let _ = sender.send_data(bytes).await;
                        }
                    });
                    return Ok(Some(Response::from_parts(parts, Either::Right(body))));
                }
                if path == "/part-002.parquet"
                    && request
                        .headers()
                        .get("Range")
                        .is_some_and(|range| range.as_bytes().starts_with(b"bytes=4-"))
                {
                    response_started
                        .wait_for(|started| *started)
                        .await
                        .map_err(|error| {
                            InvalidInputSnafu {
                                message: format!("prefetch response channel closed: {error}"),
                            }
                            .build()
                        })?;
                    // Let the client start reading the pending response before this file fills stdout.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Ok(Some(response.map(Either::Left)))
            }
        })?;
        let mut scan = ScanProcess::start(
            self.http_command(&server.origin, r#"{"allow_http":"true","timeout":"5s"}"#)?,
        )?;
        scan.wait_for_blocked_output(Duration::from_secs(15))?;
        let deadline = Instant::now() + Duration::from_secs(3);
        let pending = loop {
            let request =
                server.wait_for_request(deadline.saturating_duration_since(Instant::now()))?;
            if request.uri.path() == "/part-003.parquet" && request.method == Method::GET {
                break request;
            }
        };
        thread::sleep(Duration::from_millis(500));
        release_body.send_replace(true);
        server.wait_for_response_complete(pending.connection_id, Duration::from_secs(1))?;
        thread::sleep(Duration::from_secs(6));
        ensure!(
            requests.count()? < 8,
            InvalidInputSnafu {
                message: "all files were read while stdout was blocked"
            }
        );
        self.check_completed_scan(&mut scan, 8 * 120)?;
        server.take_requests()?;
        Ok(())
    }
}
