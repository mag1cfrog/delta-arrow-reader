use serde_json::{Value, json};

use crate::Result;

fn endpoint(value: &str) -> Result<(&str, bool)> {
    let invalid = "invalid AWS_ENDPOINT_URL";
    let parsed = url::Url::parse(value).map_err(|_| invalid)?;
    let authority = value.split_once("://").ok_or(invalid)?.1;
    let authority = authority.strip_suffix('/').unwrap_or(authority);
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || authority.contains(['/', '@', '?', '#', '\\'])
        || value.chars().any(|c| c.is_whitespace() || c.is_control())
    {
        return Err(invalid.into());
    }
    Ok((authority, parsed.scheme() == "https"))
}

pub(super) fn options(
    reader: &str,
    uri: &str,
    env: impl Fn(&str) -> Option<String>,
) -> Result<Value> {
    if !matches!(reader, "duckdb" | "polars" | "spark") {
        return Err("unknown Python storage adapter".into());
    }
    let mut result = json!({"options":{},"sql":null,"uri":uri,"settings":{"credentials":"none"}});
    if url::Url::parse(uri)?.scheme() != "s3" {
        return Ok(result);
    }
    let region = env("AWS_REGION")
        .or_else(|| env("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|| "us-east-1".into());
    let endpoint_value = env("AWS_ENDPOINT_URL");
    let endpoint = endpoint_value
        .as_deref()
        .filter(|value| !value.is_empty())
        .map(endpoint)
        .transpose()?;
    result["settings"] = json!({"credentials":"AWS environment","region":region,
        "endpoint":endpoint_value,"url_style":if endpoint.is_some() {"path"}
            else if reader == "polars" {"native default"} else {"vhost"}});
    let options = &mut result["options"];
    options[match reader {
        "duckdb" => "REGION",
        "polars" => "AWS_REGION",
        _ => "fs.s3a.endpoint.region",
    }] = json!(region);
    for (name, duckdb, spark) in [
        ("AWS_ACCESS_KEY_ID", "KEY_ID", "access.key"),
        ("AWS_SECRET_ACCESS_KEY", "SECRET", "secret.key"),
        ("AWS_SESSION_TOKEN", "SESSION_TOKEN", "session.token"),
    ] {
        let value = env(name);
        if reader == "spark" && value.as_ref().is_none_or(String::is_empty) {
            if name != "AWS_SESSION_TOKEN" {
                return Err("missing AWS credentials".into());
            }
            continue;
        }
        if let Some(value) = value {
            let key = match reader {
                "duckdb" => duckdb.to_owned(),
                "polars" => name.to_owned(),
                _ => format!("fs.s3a.{spark}"),
            };
            options[key] = json!(value);
        }
    }
    if let Some((authority, ssl)) = endpoint {
        match reader {
            "duckdb" => {
                options["ENDPOINT"] = json!(authority);
                options["URL_STYLE"] = json!("path");
                options["USE_SSL"] = json!(ssl);
            }
            "polars" => {
                options["AWS_ENDPOINT_URL"] = json!(endpoint_value);
                options["AWS_ALLOW_HTTP"] = json!((!ssl).to_string());
                options["AWS_VIRTUAL_HOSTED_STYLE_REQUEST"] = json!("false");
            }
            _ => {
                options["fs.s3a.endpoint"] = json!(endpoint_value);
                options["fs.s3a.connection.ssl.enabled"] = json!(ssl.to_string());
                options["fs.s3a.path.style.access"] = json!("true");
            }
        }
    }
    if reader == "duckdb" {
        let fields = options
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| {
                let value = value.as_str().map_or_else(
                    || value.to_string(),
                    |value| format!("'{}'", value.replace('\'', "''")),
                );
                format!("{key} {value}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        result["sql"] = json!(format!("CREATE SECRET bench_s3 (TYPE s3, {fields})"));
    } else if reader == "spark" {
        let provider = if options["fs.s3a.session.token"].is_string() {
            "TemporaryAWSCredentialsProvider"
        } else {
            "SimpleAWSCredentialsProvider"
        };
        options["fs.s3a.aws.credentials.provider"] =
            json!(format!("org.apache.hadoop.fs.s3a.{provider}"));
        result["uri"] = json!(format!("s3a:{}", uri.split_once(':').unwrap().1));
    }
    Ok(result)
}

#[cfg(test)]
mod tests;
