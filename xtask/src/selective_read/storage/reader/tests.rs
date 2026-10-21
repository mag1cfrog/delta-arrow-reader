use super::*;
use std::collections::BTreeMap;

fn configure(reader: &str, values: &[(&str, &str)]) -> Result<Value> {
    let env: BTreeMap<_, _> = values.iter().copied().collect();
    options(reader, "s3://bucket/table", |key| {
        env.get(key).map(|s| (*s).to_owned())
    })
}

#[test]
fn readers_keep_native_storage_options_and_omit_secrets_from_settings() -> Result<()> {
    for (endpoint, authority, ssl) in [
        ("http://127.0.0.1:9000/", "127.0.0.1:9000", false),
        ("https://[::1]:443", "[::1]:443", true),
    ] {
        for reader in ["duckdb", "polars", "spark"] {
            let value = configure(
                reader,
                &[
                    ("AWS_REGION", "region-primary"),
                    ("AWS_DEFAULT_REGION", "region-fallback"),
                    ("AWS_ENDPOINT_URL", endpoint),
                    ("AWS_ACCESS_KEY_ID", "test-key"),
                    ("AWS_SECRET_ACCESS_KEY", "test-'secret"),
                    ("AWS_SESSION_TOKEN", "test-token"),
                ],
            )?;
            assert_eq!(
                value["settings"],
                json!({"credentials":"AWS environment",
                "region":"region-primary","endpoint":endpoint,"url_style":"path"})
            );
            let opts = &value["options"];
            match reader {
                "duckdb" => {
                    assert_eq!(opts["ENDPOINT"], authority);
                    assert_eq!(opts["USE_SSL"], ssl);
                    assert_eq!(opts["SESSION_TOKEN"], "test-token");
                    let sql = value["sql"].as_str().unwrap();
                    assert!(sql.contains("SECRET 'test-''secret'"));
                    assert!(sql.contains(&format!("USE_SSL {ssl}")));
                }
                "polars" => {
                    assert_eq!(opts["AWS_ENDPOINT_URL"], endpoint);
                    assert_eq!(opts["AWS_ALLOW_HTTP"], (!ssl).to_string());
                    assert_eq!(opts["AWS_VIRTUAL_HOSTED_STYLE_REQUEST"], "false");
                    assert_eq!(opts["AWS_SECRET_ACCESS_KEY"], "test-'secret");
                }
                _ => {
                    assert_eq!(opts["fs.s3a.endpoint"], endpoint);
                    assert_eq!(opts["fs.s3a.connection.ssl.enabled"], ssl.to_string());
                    assert_eq!(opts["fs.s3a.path.style.access"], "true");
                    assert_eq!(opts["fs.s3a.session.token"], "test-token");
                    assert_eq!(
                        opts["fs.s3a.aws.credentials.provider"],
                        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
                    );
                    assert_eq!(value["uri"], "s3a://bucket/table");
                }
            }
        }
    }
    for reader in ["duckdb", "polars", "spark"] {
        let local = options(reader, "file:///tmp/table", |_| {
            panic!("local reads need no AWS environment")
        })?;
        assert_eq!(
            local,
            json!({"options":{},"sql":null,"uri":"file:///tmp/table","settings":{"credentials":"none"}})
        );
        let value = configure(
            reader,
            &[
                ("AWS_ACCESS_KEY_ID", "test-key"),
                ("AWS_SECRET_ACCESS_KEY", "test-secret"),
                ("AWS_SESSION_TOKEN", ""),
                ("AWS_DEFAULT_REGION", "fallback"),
            ],
        )?;
        assert_eq!(value["settings"]["region"], "fallback");
        assert!(value["settings"]["endpoint"].is_null());
        if reader == "spark" {
            assert_eq!(
                value["options"]["fs.s3a.aws.credentials.provider"],
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            );
            assert!(value["options"]["fs.s3a.session.token"].is_null());
        } else {
            assert_eq!(configure(reader, &[])?["settings"]["region"], "us-east-1");
        }
    }
    Ok(())
}

#[test]
fn invalid_endpoints_and_missing_spark_credentials_fail_without_echoing_values() -> Result<()> {
    for reader in ["duckdb", "polars", "spark"] {
        for endpoint in [
            "http:/host",
            "ftp://host",
            "http://",
            "http://host:invalid",
            "http://key:secret@host",
            "http://host/path",
            "http://host/a/..",
            "http://host//",
            "http://host?token=secret",
            "http://host#secret",
            "http://host\\path",
            "http:// host",
            "http://host\n",
        ] {
            let error = configure(reader, &[("AWS_ENDPOINT_URL", endpoint)]).unwrap_err();
            assert_eq!(
                error.to_string(),
                "invalid AWS_ENDPOINT_URL",
                "{reader}: {endpoint:?}"
            );
        }
    }
    for credentials in [
        &[][..],
        &[("AWS_ACCESS_KEY_ID", "key")],
        &[
            ("AWS_ACCESS_KEY_ID", ""),
            ("AWS_SECRET_ACCESS_KEY", "secret"),
        ],
    ] {
        assert_eq!(
            configure("spark", credentials).unwrap_err().to_string(),
            "missing AWS credentials"
        );
    }
    assert!(configure("unknown", &[]).is_err());
    Ok(())
}
