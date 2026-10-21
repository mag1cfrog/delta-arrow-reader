use serde_json::Value;
use std::{io, time::Duration};

pub(super) fn require(condition: bool, reason: &str) -> io::Result<()> {
    if condition {
        Ok(())
    } else {
        Err(io::Error::new(io::ErrorKind::InvalidData, reason))
    }
}

fn fields(value: &Value, expected: &[&str]) -> bool {
    value.as_object().is_some_and(|object| {
        object.len() == expected.len() && expected.iter().all(|key| object.contains_key(*key))
    })
}

pub(in crate::selective_read) fn count(payload: &Value) -> io::Result<usize> {
    let reuse = payload["execution_mode"] == "reuse";
    if !payload["concurrent_queries"].is_null() {
        return payload["concurrent_queries"]
            .as_u64()
            .filter(|count| matches!(count, 2 | 4) && reuse && payload["comparison_revision"] == 6)
            .map(|count| count as usize)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "concurrent queries require revision 6, reuse mode and a count of 2 or 4",
                )
            });
    }
    Ok(if !reuse {
        1
    } else if payload["comparison_revision"].as_u64().unwrap_or(2) >= 4 {
        2
    } else {
        10
    })
}

pub(super) struct Progress {
    count: usize,
    concurrent: bool,
    timed: bool,
    query_ns: u64,
    pub(super) cleanup_ns: u64,
    pub(super) phase: &'static str,
    pub(super) queries: Vec<Value>,
    pub(super) initialization_ns: Value,
    pub(super) cleanup_started_ns: Option<u64>,
    pub(super) deadline: u64,
}

impl Progress {
    pub(super) fn new(
        payload: &Value,
        query_seconds: f64,
        cleanup_seconds: f64,
        now: u64,
    ) -> io::Result<Self> {
        let nanos = |seconds| -> io::Result<u64> {
            let duration = Duration::try_from_secs_f64(seconds).map_err(io::Error::other)?;
            let nanos = u64::try_from(duration.as_nanos()).map_err(io::Error::other)?;
            require(
                nanos > 0 && now.checked_add(nanos).is_some(),
                "invalid watchdog duration",
            )?;
            Ok(nanos)
        };
        let query_ns = nanos(query_seconds)?;
        Ok(Self {
            count: count(payload)?,
            concurrent: !payload["concurrent_queries"].is_null(),
            timed: payload["purpose"] == "timing",
            query_ns,
            cleanup_ns: nanos(cleanup_seconds)?,
            phase: "setup",
            queries: Vec::new(),
            initialization_ns: Value::Null,
            cleanup_started_ns: None,
            deadline: now + query_ns,
        })
    }

    pub(super) fn receive(&mut self, message: &Value, now: u64) -> io::Result<()> {
        require(
            fields(
                message,
                &["phase", "query_index", "query", "initialization_ns"],
            ),
            "malformed watchdog message",
        )?;
        let index = &message["query_index"];
        let query = &message["query"];
        let phase = match message["phase"].as_str() {
            Some("cleanup") => {
                require(
                    index.is_null() && query.is_null(),
                    "malformed cleanup message",
                )?;
                if self.phase == "cleanup" {
                    return Ok(());
                }
                "cleanup"
            }
            Some(phase @ ("open" | "initialization")) => {
                require(
                    self.phase == "setup"
                        && phase
                            == if self.count == 1 {
                                "open"
                            } else {
                                "initialization"
                            }
                        && index.is_null()
                        && query.is_null(),
                    "invalid snapshot phase",
                )?;
                if phase == "open" {
                    "open"
                } else {
                    "initialization"
                }
            }
            Some(phase @ ("query" | "concurrent_queries")) => {
                if self.concurrent {
                    require(
                        phase == "concurrent_queries"
                            && self.phase == "initialization"
                            && index.is_null()
                            && query.is_null(),
                        "invalid concurrent query start",
                    )?;
                } else {
                    require(
                        phase == "query"
                            && self.count > 1
                            && matches!(self.phase, "initialization" | "between")
                            && index.as_u64() == Some(self.queries.len() as u64)
                            && self.queries.len() < self.count
                            && query.is_null(),
                        "invalid query sequence",
                    )?;
                }
                self.initialization_ns = message["initialization_ns"].clone();
                if self.timed {
                    let time = self.initialization_ns.as_u64().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "missing initialization time")
                    })?;
                    if time > self.query_ns {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "initialization exceeded deadline",
                        ));
                    }
                }
                if self.concurrent {
                    "concurrent_queries"
                } else {
                    "query"
                }
            }
            Some("query_end") => {
                let valid_index = index.as_u64().is_some_and(|index| {
                    if self.concurrent {
                        index < self.count as u64
                            && !self
                                .queries
                                .iter()
                                .any(|query| query["query_index"] == index)
                    } else {
                        index == self.queries.len() as u64
                    }
                });
                require(
                    (if self.concurrent {
                        self.phase == "concurrent_queries"
                    } else {
                        matches!(self.phase, "open" | "query")
                    }) && valid_index
                        && fields(
                            query,
                            &[
                                "query_index",
                                "output_rows",
                                "output_batches",
                                "completion_ns",
                                "first_batch_ns",
                            ],
                        ),
                    "invalid completed query",
                )?;
                require(
                    query["query_index"].as_u64() == index.as_u64()
                        && query["output_rows"].as_u64().is_some()
                        && query["output_batches"].as_u64().is_some(),
                    "invalid query counts",
                )?;
                if self.timed {
                    let time = query["completion_ns"].as_u64().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "invalid query clocks")
                    })?;
                    require(
                        query["first_batch_ns"].is_null()
                            || query["first_batch_ns"]
                                .as_u64()
                                .is_some_and(|first| first <= time),
                        "invalid query clocks",
                    )?;
                    if time > self.query_ns {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "query exceeded deadline",
                        ));
                    }
                } else {
                    require(
                        query["completion_ns"].is_null() && query["first_batch_ns"].is_null(),
                        "untimed query has timing values",
                    )?;
                }
                self.queries.push(query.clone());
                if self.concurrent {
                    self.queries
                        .sort_by_key(|query| query["query_index"].as_u64());
                    if self.queries.len() < self.count {
                        return Ok(());
                    }
                }
                if self.queries.len() == self.count {
                    "cleanup"
                } else {
                    "between"
                }
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unknown watchdog phase",
                ));
            }
        };
        self.phase = phase;
        if phase == "cleanup" {
            self.cleanup_started_ns = Some(now);
        }
        self.deadline = now
            + if matches!(phase, "cleanup" | "between") {
                self.cleanup_ns
            } else {
                self.query_ns
            };
        Ok(())
    }
}

#[cfg(test)]
mod tests;
