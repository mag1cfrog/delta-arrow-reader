use super::*;
use serde_json::{Value, json};

fn aws_credentials_response() -> Value {
    let expiration_time = (SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
        + Duration::from_secs(300))
    .as_millis() as u64;
    json!({
        "url": "s3://bucket/table/",
        "expiration_time": expiration_time,
        "aws_temp_credentials": {
            "access_key_id": "test-key",
            "secret_access_key": "test-secret",
            "session_token": "test-token"
        }
    })
}

#[test]
fn pipeline_grants_must_cover_the_published_location_and_renew_at_the_same_root() {
    let response = aws_credentials_response();
    let read_location =
        TableStorageLocation::parse("s3a://bucket/table/_external_metadata").unwrap();
    let bytes = serde_json::to_vec(&response).unwrap();
    let (_, scope) = parse_pipeline_credentials(&bytes, &read_location).unwrap();
    assert_eq!(scope.as_url().as_str(), "s3://bucket/table/");
    assert!(parse_temporary_credentials(&bytes, &scope).is_ok());
    assert!(parse_temporary_credentials(&bytes, &read_location).is_err());
    for wrong_scope in [
        "s3://other/table",
        "s3://bucket/tabl",
        "s3://bucket/table2",
        "s3://bucket/table/_external_metadata/subdir",
        "gs://bucket/table",
        "s3://bucket/table?token=secret",
    ] {
        let mut wrong = response.clone();
        wrong["url"] = json!(wrong_scope);
        let bytes = serde_json::to_vec(&wrong).unwrap();
        assert!(
            parse_pipeline_credentials(&bytes, &read_location).is_err(),
            "{wrong_scope}"
        );
    }
    let mut wider = response;
    wider["url"] = json!("s3://bucket/");
    assert!(parse_temporary_credentials(&serde_json::to_vec(&wider).unwrap(), &scope).is_err());
}

#[test]
fn r2_credentials_require_the_same_root_and_exactly_one_credential_kind() {
    let mut response = aws_credentials_response();
    let credentials = response
        .as_object_mut()
        .unwrap()
        .remove("aws_temp_credentials")
        .unwrap();
    response["r2_temp_credentials"] = credentials;
    response["url"] = json!("https://account.r2.cloudflarestorage.com/bucket/table/");
    let parse_grant = |value: &Value| {
        parse_temporary_credentials(
            &serde_json::to_vec(value).unwrap(),
            &TableStorageLocation::parse("r2://bucket@account.r2.cloudflarestorage.com/table")
                .unwrap(),
        )
    };
    let grant = parse_grant(&response).unwrap();
    let CloudCredentials::Aws(credentials) = grant.credentials else {
        panic!("R2 must use AWS credentials");
    };
    assert_eq!(credentials.key_id, "test-key");
    assert_eq!(credentials.secret_key, "test-secret");
    assert_eq!(credentials.token.as_deref(), Some("test-token"));
    for (field, invalid) in [
        (
            "url",
            json!("https://other.r2.cloudflarestorage.com/bucket/table/"),
        ),
        (
            "url",
            json!("https://account.r2.cloudflarestorage.com/other/table/"),
        ),
        (
            "url",
            json!("https://account.r2.cloudflarestorage.com/bucket/other/"),
        ),
        (
            "aws_temp_credentials",
            response["r2_temp_credentials"].clone(),
        ),
        (
            "azure_user_delegation_sas",
            json!({"sas_token":"sig=secret"}),
        ),
        ("azure_aad", json!({"aad_token":"secret"})),
        ("gcp_oauth_token", json!({"oauth_token":"secret"})),
        ("r2_temp_credentials", json!([])),
        ("r2_temp_credentials", json!({"access_key_id":"key"})),
        (
            "r2_temp_credentials",
            json!({
                "access_key_id": "key",
                "secret_access_key": "",
                "session_token": "token"
            }),
        ),
    ] {
        let mut value = response.clone();
        value[field] = invalid;
        assert!(parse_grant(&value).is_err(), "{field}");
    }
    let mut wrong_kind = response.clone();
    wrong_kind["aws_temp_credentials"] = wrong_kind
        .as_object_mut()
        .unwrap()
        .remove("r2_temp_credentials")
        .unwrap();
    assert!(matches!(
        parse_grant(&wrong_kind),
        Err(Error::UnsupportedDirectRead)
    ));
    response["url"] = json!("s3://bucket/table/");
    assert!(matches!(
        parse_temporary_credentials(
            &serde_json::to_vec(&response).unwrap(),
            &TableStorageLocation::parse("s3://bucket/table/").unwrap(),
        ),
        Err(Error::UnsupportedDirectRead)
    ));
}

#[test]
fn credentials_validate_expiry_location_and_cloud_before_mapping() {
    let value = aws_credentials_response();
    let parse_grant = |value: &Value| {
        parse_temporary_credentials(
            &serde_json::to_vec(value).unwrap(),
            &TableStorageLocation::parse("s3a://bucket/table").unwrap(),
        )
    };
    let CloudCredentials::Aws(credentials) = parse_grant(&value).unwrap().credentials else {
        panic!("S3 must use AWS credentials");
    };
    assert_eq!(credentials.token.as_deref(), Some("test-token"));
    for (field, invalid) in [
        ("expiration_time", json!(0)),
        ("expiration_time", json!("tomorrow")),
        ("url", json!("s3://other/table")),
        ("url", json!("s3://bucket/table2")),
        ("url", json!("s3://bucket/table?secret-sentinel")),
        (
            "aws_temp_credentials",
            json!({
                "access_key_id": "test-key",
                "secret_access_key": "",
                "session_token": "test-token"
            }),
        ),
        ("aws_temp_credentials", json!({"access_key_id":"test-key"})),
        ("r2_temp_credentials", value["aws_temp_credentials"].clone()),
        (
            "azure_user_delegation_sas",
            json!({"sas_token":"sig=secret"}),
        ),
        ("azure_aad", json!({"aad_token":"secret"})),
        ("gcp_oauth_token", json!({"oauth_token":"secret"})),
    ] {
        let mut invalid_value = value.clone();
        invalid_value[field] = invalid;
        assert!(parse_grant(&invalid_value).is_err(), "{field}");
    }
    let mut azure_blob_response = value;
    azure_blob_response
        .as_object_mut()
        .unwrap()
        .remove("aws_temp_credentials");
    azure_blob_response["url"] =
        json!("https://account.blob.core.windows.net/container/a%20b/%252F");
    azure_blob_response["azure_user_delegation_sas"] =
        json!({"sas_token": "?sv=test&sig=test%2Bsignature&sp=rl"});
    let result = parse_temporary_credentials(
        &serde_json::to_vec(&azure_blob_response).unwrap(),
        &TableStorageLocation::parse("abfss://container@account.dfs.core.windows.net/a%20b/%252F/")
            .unwrap(),
    )
    .unwrap();
    let CloudCredentials::Azure(credentials) = result.credentials else {
        panic!("Azure Blob must use Azure credentials");
    };
    let AzureCredential::SASToken(query_pairs) = credentials.as_ref() else {
        panic!("expected SAS credentials");
    };
    assert_eq!(
        query_pairs,
        &vec![
            ("sv".into(), "test".into()),
            ("sig".into(), "test+signature".into()),
            ("sp".into(), "rl".into()),
        ]
    );
    for sas in [
        "",
        "sig=",
        "sig=secret&missing-value",
        "sig=secret#fragment",
        "sig=secret\nvalue",
        "sig=%FF",
    ] {
        let mut response = azure_blob_response.clone();
        response["azure_user_delegation_sas"] = json!({"sas_token": sas});
        assert!(matches!(
            parse_temporary_credentials(
                &serde_json::to_vec(&response).unwrap(),
                &TableStorageLocation::parse(response["url"].as_str().unwrap()).unwrap(),
            ),
            Err(Error::InvalidDatabricksResponse)
        ));
    }
    for (field, conflicting) in [
        (
            "aws_temp_credentials",
            aws_credentials_response()["aws_temp_credentials"].clone(),
        ),
        (
            "r2_temp_credentials",
            aws_credentials_response()["aws_temp_credentials"].clone(),
        ),
        ("azure_aad", json!({"aad_token": "secret-bearer-token"})),
    ] {
        let mut response = azure_blob_response.clone();
        response[field] = conflicting;
        assert!(matches!(
            parse_temporary_credentials(
                &serde_json::to_vec(&response).unwrap(),
                &TableStorageLocation::parse(response["url"].as_str().unwrap()).unwrap(),
            ),
            Err(Error::UnsupportedDirectRead)
        ));
    }
    for location in [
        "file:///tmp/table",
        "https://unknown/table",
        "abfs://container@account.dfs.core.windows.net/table",
        "abfss://account.dfs.core.windows.net/table",
        "s3://user:secret@bucket/table",
        "s3://bucket//table",
        "s3://bucket/table#fragment",
    ] {
        assert!(TableStorageLocation::parse(location).is_err(), "{location}");
    }
}

#[test]
fn gcs_credentials_require_one_token_for_the_same_bucket_and_root() {
    let mut response = aws_credentials_response();
    response
        .as_object_mut()
        .unwrap()
        .remove("aws_temp_credentials");
    response["url"] = json!("gs://bucket/table/");
    response["gcp_oauth_token"] = json!({"oauth_token": "secret-gcp-token"});
    let parse_grant = |value: &Value| {
        parse_temporary_credentials(
            &serde_json::to_vec(value).unwrap(),
            &TableStorageLocation::parse("gs://bucket/table").unwrap(),
        )
    };
    let grant = parse_grant(&response).unwrap();
    let CloudCredentials::Gcp(credentials) = grant.credentials else {
        panic!("GCS must use GCP credentials");
    };
    assert_eq!(credentials.bearer, "secret-gcp-token");
    for invalid in [
        json!("secret-gcp-token"),
        json!([]),
        json!({}),
        json!({"oauth_token": null}),
        json!({"oauth_token": ""}),
        json!({"oauth_token": " "}),
        json!({"oauth_token": "secret\ngcp-token"}),
        json!({"oauth_token": "secret gcp-token"}),
    ] {
        let mut value = response.clone();
        value["gcp_oauth_token"] = invalid;
        assert!(matches!(
            parse_grant(&value),
            Err(Error::InvalidDatabricksResponse)
        ));
    }
    for (field, credentials) in [
        (
            "aws_temp_credentials",
            aws_credentials_response()["aws_temp_credentials"].clone(),
        ),
        (
            "r2_temp_credentials",
            aws_credentials_response()["aws_temp_credentials"].clone(),
        ),
        (
            "azure_user_delegation_sas",
            json!({"sas_token": "sig=secret"}),
        ),
        ("azure_aad", json!({"aad_token": "secret-bearer-token"})),
    ] {
        let mut value = response.clone();
        value[field] = credentials;
        assert!(matches!(
            parse_grant(&value),
            Err(Error::UnsupportedDirectRead)
        ));
    }
    for url in [
        "gs://other/table/",
        "gs://bucket/other/",
        "gs://bucket/table?secret",
        "gs://user@bucket/table",
        "gs://bucket:1234/table",
        "gs://bucket//table",
        "gs://bucket/table#fragment",
        "s3://bucket/table/",
    ] {
        let mut value = response.clone();
        value["url"] = json!(url);
        assert!(parse_grant(&value).is_err(), "{url}");
    }
    response["expiration_time"] = json!(0);
    assert!(matches!(
        parse_grant(&response),
        Err(Error::CredentialsExpired)
    ));
}

#[test]
fn azure_bearer_credentials_require_one_valid_token_for_azure_blob_storage() {
    let mut response = aws_credentials_response();
    response
        .as_object_mut()
        .unwrap()
        .remove("aws_temp_credentials");
    response["url"] = json!("abfss://container@account.dfs.core.windows.net/table/");
    response["azure_aad"] = json!({"aad_token":"secret-bearer-token"});
    let parse_grant = |value: &Value| {
        parse_temporary_credentials(
            &serde_json::to_vec(value).unwrap(),
            &TableStorageLocation::parse(value["url"].as_str().unwrap()).unwrap(),
        )
    };
    let grant = parse_grant(&response).unwrap();
    let CloudCredentials::Azure(credentials) = grant.credentials else {
        panic!("Azure Blob must use Azure credentials");
    };
    assert!(matches!(
        credentials.as_ref(),
        AzureCredential::BearerToken(token) if token == "secret-bearer-token"
    ));
    for invalid in [
        json!("secret-bearer-token"),
        json!([]),
        json!({}),
        json!({"aad_token":null}),
        json!({"aad_token":""}),
        json!({"aad_token":" "}),
        json!({"aad_token":"secret\nbearer-token"}),
        json!({"aad_token":"secret bearer-token"}),
    ] {
        let mut value = response.clone();
        value["azure_aad"] = invalid;
        assert!(matches!(
            parse_grant(&value),
            Err(Error::InvalidDatabricksResponse)
        ));
    }
    for (field, conflicting) in [
        (
            "azure_user_delegation_sas",
            json!({"sas_token":"sig=secret"}),
        ),
        (
            "aws_temp_credentials",
            aws_credentials_response()["aws_temp_credentials"].clone(),
        ),
        ("gcp_oauth_token", json!({"oauth_token":"secret"})),
        (
            "r2_temp_credentials",
            json!({
                "access_key_id": "key",
                "secret_access_key": "secret",
                "session_token": "token"
            }),
        ),
        ("url", json!("s3://bucket/table/")),
    ] {
        let mut value = response.clone();
        value[field] = conflicting;
        assert!(matches!(
            parse_grant(&value),
            Err(Error::UnsupportedDirectRead)
        ));
    }
}
