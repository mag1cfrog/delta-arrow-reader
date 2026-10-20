use super::{array, field, object, require, storage, text};
use crate::Result;
use regex::Regex;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    sync::LazyLock,
};

mod session;
mod trace;
pub(super) use trace::capture;

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case", deny_unknown_fields)]
pub(super) enum Request {
    Invoke(session::Request),
    Metrics {
        directory: PathBuf,
        timeout: f64,
    },
    Reconcile {
        before: Value,
        after: Value,
        counts: BTreeMap<String, u64>,
        size: u64,
    },
    Summarize {
        output: PathBuf,
        record: Value,
    },
}

pub(super) fn run(request: Request) -> Result<Value> {
    match request {
        Request::Invoke(request) => session::run(request),
        Request::Metrics { directory, timeout } => metrics(&directory, timeout),
        Request::Reconcile {
            before,
            after,
            counts,
            size,
        } => reconcile(&before, &after, &counts, size),
        Request::Summarize { output, record } => summarize(&output, &record),
    }
}

fn integer(value: &Value) -> Result<u64> {
    value
        .as_u64()
        .ok_or_else(|| "expected a nonnegative native counter".into())
}

fn increment(value: &mut Value, by: u64) -> Result<()> {
    *value = json!(
        integer(value)?
            .checked_add(by)
            .ok_or("native counter overflow")?
    );
    Ok(())
}

fn counter(value: &str) -> Result<u64> {
    let (mantissa, exponent) = match value.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i64>()?),
        None => (value, 0),
    };
    let negative = mantissa.starts_with('-');
    let mantissa = mantissa.strip_prefix(['+', '-']).unwrap_or(mantissa);
    let (whole, fractional) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    require(
        !whole.is_empty() || !fractional.is_empty(),
        "invalid native counter",
    )?;
    require(
        whole
            .bytes()
            .chain(fractional.bytes())
            .all(|b| b.is_ascii_digit()),
        "invalid native counter",
    )?;
    let digits = format!("{whole}{fractional}");
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Ok(0);
    }
    require(!negative, "negative native counter")?;
    let scale = exponent
        .checked_sub(fractional.len().try_into()?)
        .ok_or("invalid counter exponent")?;
    let result = if scale < 0 {
        let fractional: usize = scale.unsigned_abs().try_into()?;
        require(
            fractional < digits.len()
                && digits[digits.len() - fractional..]
                    .bytes()
                    .all(|b| b == b'0'),
            "counter cannot be represented exactly",
        )?;
        digits[..digits.len() - fractional].parse()?
    } else {
        require(scale <= 15, "counter cannot be represented exactly")?;
        digits
            .parse::<u64>()?
            .checked_mul(10u64.pow(scale as u32))
            .ok_or("native counter overflow")?
    };
    require(result < 1u64 << 53, "counter cannot be represented exactly")?;
    Ok(result)
}

fn parse_metrics(source: &str) -> Result<Value> {
    static METRIC: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^(\w+)\{(.*)\} ([^ ]+)$").unwrap());
    static NAME: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"name="([^"]+)""#).unwrap());
    let mut result =
        json!({"requests":{}, "response_bytes":0, "active":0, "rejected":0, "canceled":0});
    let mut found = false;
    for line in source
        .lines()
        .filter(|line| line.starts_with("minio_api_requests_"))
    {
        let values = METRIC.captures(line).ok_or("unexpected MinIO metric")?;
        let key = match &values[1] {
            "minio_api_requests_total" => "requests",
            "minio_api_requests_traffic_sent_bytes" => "response_bytes",
            "minio_api_requests_inflight_total" | "minio_api_requests_waiting_total" => "active",
            "minio_api_requests_canceled_total" => "canceled",
            name if name.starts_with("minio_api_requests_rejected_") => "rejected",
            _ => continue,
        };
        let count = counter(&values[3])?;
        match key {
            "requests" => {
                let name = NAME
                    .captures(&values[2])
                    .ok_or("missing request API label")?;
                result["requests"][&name[1]] = json!(count);
            }
            "active" | "rejected" => increment(&mut result[key], count)?,
            _ => {
                result[key] = json!(count);
                found |= key == "response_bytes";
            }
        }
    }
    require(found, "MinIO traffic counter missing")?;
    Ok(result)
}

fn metrics(directory: &Path, timeout: f64) -> Result<Value> {
    let config = storage::state(directory)?;
    let source = storage::http(
        &format!(
            "{}/minio/metrics/v3/api/requests",
            storage::endpoint(&config)?
        ),
        "GET",
        timeout,
    )?;
    parse_metrics(std::str::from_utf8(&source)?)
}

fn reconcile(
    before: &Value,
    after: &Value,
    counts: &BTreeMap<String, u64>,
    size: u64,
) -> Result<Value> {
    let previous = object(&before["requests"])?;
    let next = object(&after["requests"])?;
    let keys: BTreeSet<_> = previous.keys().chain(next.keys()).collect();
    let mut difference = BTreeMap::new();
    for key in keys {
        let value = integer(next.get(key).unwrap_or(&json!(0)))?
            .checked_sub(integer(previous.get(key).unwrap_or(&json!(0)))?)
            .ok_or("server counters reset")?;
        if value > 0 {
            difference.insert(key.clone(), value);
        }
    }
    let mut expected = json!({"requests": difference});
    for key in ["response_bytes", "rejected", "canceled"] {
        expected[key] = json!(
            integer(&after[key])?
                .checked_sub(integer(&before[key])?)
                .ok_or("server counters reset")?
        );
    }
    let positive: BTreeMap<_, _> = counts
        .iter()
        .filter(|(_, v)| **v > 0)
        .map(|(k, v)| (k.clone(), *v))
        .collect();
    require(
        after["active"] == 0
            && expected["rejected"] == 0
            && positive == difference
            && expected["response_bytes"] == size,
        "trace differs from native request/byte counters or server is not idle",
    )?;
    Ok(expected)
}

fn unquote(value: &str) -> String {
    let mut bytes = value.as_bytes().iter().copied();
    let mut output = Vec::new();
    while let Some(byte) = bytes.next() {
        if byte == b'%' {
            let mut remaining = bytes.clone();
            if let (Some(a), Some(b)) = (remaining.next(), remaining.next())
                && let (Some(a), Some(b)) = ((a as char).to_digit(16), (b as char).to_digit(16))
            {
                output.push((a * 16 + b) as u8);
                bytes = remaining;
                continue;
            }
        }
        output.push(byte);
    }
    String::from_utf8_lossy(&output).into_owned()
}

fn timestamp(value: &str) -> Result<i64> {
    static STAMP: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(?:\.([0-9]{1,9}))?Z$").unwrap()
    });
    let parts = STAMP.captures(value).ok_or("unexpected native timestamp")?;
    let numbers: Vec<i32> = (1..=6)
        .map(|i| parts[i].parse())
        .collect::<std::result::Result<_, _>>()?;
    let fraction = parts.get(7).map(|v| v.as_str()).unwrap_or("");
    let nanos: i64 = format!("{fraction:0<9}").parse()?;
    let mut date = libc::tm {
        tm_year: numbers[0] - 1900,
        tm_mon: numbers[1] - 1,
        tm_mday: numbers[2],
        tm_hour: numbers[3],
        tm_min: numbers[4],
        tm_sec: numbers[5],
        tm_isdst: 0,
        tm_wday: 0,
        tm_yday: 0,
        tm_gmtoff: 0,
        tm_zone: std::ptr::null(),
    };
    // SAFETY: timegm takes a valid tm pointer and normalizes it in place.
    let seconds = unsafe { libc::timegm(&mut date) };
    require(
        numbers
            == [
                date.tm_year + 1900,
                date.tm_mon + 1,
                date.tm_mday,
                date.tm_hour,
                date.tm_min,
                date.tm_sec,
            ],
        "invalid native calendar timestamp",
    )?;
    Ok(seconds
        .checked_mul(1_000_000_000)
        .and_then(|v| v.checked_add(nanos))
        .ok_or("native timestamp outside nanosecond range")?)
}

fn header(headers: &Value, name: &str) -> Result<Value> {
    if headers.is_null() {
        return Ok(Value::Null);
    }
    let mut result = None;
    for (key, values) in object(headers)? {
        if key.eq_ignore_ascii_case(name) {
            require(result.is_none(), "ambiguous native trace header")?;
            result = Some(array(values)?.first().cloned().unwrap_or(Value::Null));
        }
    }
    Ok(result.unwrap_or(Value::Null))
}

fn sanitize(raw: &Value, scope: &str, run_id: &str, case_id: &str) -> Result<Value> {
    static ATTEMPT: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(?:^|[; ,])attempt=([0-9]+)").unwrap());
    let http = &raw["http"];
    let (request, response) = (&http["request"], &http["response"]);
    let path = unquote(text(&request["path"])?);
    let root = format!("/{}", storage::BUCKET);
    let key = path.strip_prefix(&format!("{root}/")).unwrap_or("");
    let query = request.get("rawquery").map(text).transpose()?.unwrap_or("");
    let prefix = url::form_urlencoded::parse(query.as_bytes())
        .find(|(k, _)| k == "prefix")
        .map(|(_, v)| v.into_owned())
        .unwrap_or_default();
    require(
        path == root || path == format!("{root}/") || key.starts_with(scope),
        "unrelated object request",
    )?;
    require(
        prefix.is_empty() || prefix.starts_with(scope),
        "unrelated listing request",
    )?;
    let function = text(&raw["funcname"])?;
    require(
        !function.starts_with("s3.List") || !prefix.is_empty(),
        "unscoped listing request",
    )?;
    let api = function.strip_prefix("s3.").unwrap_or(function);
    let size = integer(&http["stats"]["outputbytes"])?;
    let length = header(&response["headers"], "content-length")?;
    let length = if length.is_null() {
        None
    } else {
        Some(text(&length)?.parse::<u64>()?)
    };
    let method = text(&request["method"])?;
    let category = if api.starts_with("List") {
        "listing"
    } else if key.is_empty() {
        "bucket"
    } else if format!("/{key}").contains("/_delta_log/") {
        "delta_log"
    } else if key.ends_with(".parquet") {
        "parquet"
    } else if key
        .rsplit('/')
        .next()
        .unwrap_or("")
        .starts_with("deletion_vector_")
    {
        "deletion_vector"
    } else {
        "other"
    };
    let sdk = header(&request["headers"], "amz-sdk-request")?;
    let attempt = ATTEMPT
        .captures(sdk.as_str().unwrap_or(""))
        .map(|v| v[1].parse::<u64>())
        .transpose()?;
    Ok(
        json!({"run_id":run_id, "case_id":case_id, "api":api, "method":method, "object":key,
        "object_class":category, "list_prefix":if prefix.is_empty() { None } else { Some(prefix) },
        "range":header(&request["headers"],"range")?, "content_range":header(&response["headers"],"content-range")?,
        "status":field(response,"statuscode")?, "response_bytes":size, "advertised_content_length":length,
        "incomplete_body":method != "HEAD" && length.is_some_and(|length| size < length),
        "request_id":header(&response["headers"],"x-amz-request-id")?,
        "sdk_invocation_id":header(&request["headers"],"amz-sdk-invocation-id")?, "sdk_attempt":attempt,
        "started_ns":timestamp(text(&request["time"])?)?, "ended_ns":timestamp(text(&response["time"])?)?}),
    )
}

fn summarize(output: &Path, record: &Value) -> Result<Value> {
    let last = record
        .get("diagnostic_events")
        .map(array)
        .transpose()?
        .into_iter()
        .flatten()
        .filter(|event| event["event"] == "stream_complete")
        .map(|event| integer(&event["time_ns"]))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .max();
    let mut summary = json!({"requests":0, "response_bytes":0, "by_class":{}, "by_api_status":{},
        "incomplete_responses":0, "explicit_retry_attempts":0, "retry_note":"all attempts count; retry identity is unknown without SDK metadata",
        "started_after_final_stream":0, "ended_after_final_stream":0, "response_bytes_after_stream":null,
        "last_stream_complete_ns":last, "post_stream_byte_reason":"request totals cannot split a response that crosses the stream boundary"});
    let (mut touched, mut gets, mut heads) = (BTreeSet::new(), BTreeSet::new(), BTreeSet::new());
    for line in BufReader::new(File::open(output.join("requests.jsonl"))?).lines() {
        let item: Value = serde_json::from_str(&line?)?;
        let size = integer(&item["response_bytes"])?;
        increment(&mut summary["requests"], 1)?;
        increment(&mut summary["response_bytes"], size)?;
        let category = text(&item["object_class"])?;
        if summary["by_class"][category].is_null() {
            summary["by_class"][category] = json!({"requests":0,"response_bytes":0});
        }
        increment(&mut summary["by_class"][category]["requests"], 1)?;
        increment(&mut summary["by_class"][category]["response_bytes"], size)?;
        let key = format!("{}/{}", text(&item["api"])?, integer(&item["status"])?);
        if summary["by_api_status"][&key].is_null() {
            summary["by_api_status"][&key] = json!(0);
        }
        increment(&mut summary["by_api_status"][&key], 1)?;
        if category == "parquet" {
            let key = text(&item["object"])?.to_owned();
            touched.insert(key.clone());
            if item["method"] == "GET" {
                gets.insert(key.clone());
            }
            if item["method"] == "HEAD" {
                heads.insert(key);
            }
        }
        increment(
            &mut summary["incomplete_responses"],
            u64::from(
                item["incomplete_body"]
                    .as_bool()
                    .ok_or("invalid incomplete body flag")?,
            ),
        )?;
        increment(
            &mut summary["explicit_retry_attempts"],
            u64::from(item["sdk_attempt"].as_u64().unwrap_or(1) > 1),
        )?;
        increment(
            &mut summary["started_after_final_stream"],
            u64::from(
                last.is_some_and(|last| item["started_ns"].as_u64().is_some_and(|v| v >= last)),
            ),
        )?;
        increment(
            &mut summary["ended_after_final_stream"],
            u64::from(last.is_some_and(|last| item["ended_ns"].as_u64().is_some_and(|v| v > last))),
        )?;
    }
    summary["touched_parquet_objects"] = json!(touched);
    summary["parquet_get_objects"] = json!(gets);
    summary["parquet_head_objects"] = json!(heads);
    Ok(summary)
}

#[cfg(test)]
mod tests;
