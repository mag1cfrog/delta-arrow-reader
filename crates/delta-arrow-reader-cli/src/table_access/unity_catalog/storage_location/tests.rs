use super::*;

#[test]
fn options_keep_transport_and_region_but_never_override_identity() {
    for key in [
        "AWS_ACCESS_KEY_ID",
        "access_key_id",
        "AWS_TOKEN",
        "token",
        "AWS_BUCKET_NAME",
        "bucket",
        "azure_storage_master_key",
        "access_key",
        "AZURE_STORAGE_SAS_TOKEN",
        "sas_key",
        "container_name",
        "account_name",
        "azure_use_azure_cli",
        "skip_signature",
        "aws_role_arn",
        "federated_token_file",
        "aws_metadata_endpoint",
        "GOOGLE_SERVICE_ACCOUNT",
        "service_account_key",
        "google_application_credentials",
        "google_bucket",
        "google_skip_signature",
        "unknown_option",
    ] {
        let options = DeltaStorageOptions::from([(key.into(), "secret-sentinel".into())]);
        assert_eq!(
            validate_storage_options(&options, None),
            Err(Error::InvalidStorageOptions),
            "{key}"
        );
    }
    let mut options = DeltaStorageOptions::from([
        ("AWS_REGION".into(), "eu-west-1".into()),
        ("aws_allow_http".into(), "true".into()),
        ("aws_endpoint".into(), "http://127.0.0.1:1234".into()),
    ]);
    validate_storage_options(&options, Some(StorageBackend::S3)).unwrap();
    options.insert("default_region".into(), "us-east-1".into());
    assert_eq!(
        validate_storage_options(&options, Some(StorageBackend::S3)),
        Err(Error::InvalidStorageOptions)
    );
    assert_eq!(
        validate_storage_options(&DeltaStorageOptions::new(), Some(StorageBackend::S3)),
        Err(Error::MissingS3Region)
    );
    validate_storage_options(&DeltaStorageOptions::new(), Some(StorageBackend::AzureBlob)).unwrap();
    let gcs_options = DeltaStorageOptions::from([
        ("GOOGLE_BASE_URL".into(), "http://127.0.0.1:1234".into()),
        ("google_allow_http".into(), "true".into()),
    ]);
    validate_storage_options(&gcs_options, None).unwrap();
    validate_storage_options(&gcs_options, Some(StorageBackend::Gcs)).unwrap();
    assert_eq!(
        validate_storage_options(&gcs_options, Some(StorageBackend::S3)),
        Err(Error::InvalidStorageOptions)
    );
    assert_eq!(
        validate_storage_options(
            &DeltaStorageOptions::from([("aws_region".into(), "eu-west-1".into())]),
            Some(StorageBackend::Gcs),
        ),
        Err(Error::InvalidStorageOptions)
    );
}

#[test]
fn azure_blob_container_root_locations_are_equivalent() {
    let expected = "abfss://container@account.dfs.core.windows.net/";
    for location in [
        "https://account.blob.core.windows.net/container",
        "https://account.blob.core.windows.net/container/",
        "https://account.dfs.core.windows.net/container",
        "https://account.dfs.core.windows.net/container/",
        expected,
    ] {
        assert_eq!(
            TableStorageLocation::parse(location).unwrap().url.as_str(),
            expected
        );
    }
    for location in [
        "https://account.blob.core.windows.net",
        "https://account.dfs.core.windows.net/",
    ] {
        assert!(TableStorageLocation::parse(location).is_err(), "{location}");
    }
}

#[test]
fn r2_locations_preserve_account_bucket_and_encoded_path() {
    for (location, expected) in [
        (
            "r2://bucket@account.r2.cloudflarestorage.com",
            "https://account.r2.cloudflarestorage.com/bucket/",
        ),
        (
            "r2://bucket@account.r2.cloudflarestorage.com/",
            "https://account.r2.cloudflarestorage.com/bucket/",
        ),
        (
            "https://account.r2.cloudflarestorage.com/bucket",
            "https://account.r2.cloudflarestorage.com/bucket/",
        ),
        (
            "r2://bucket@account.r2.cloudflarestorage.com/a%20b/%252F",
            "https://account.r2.cloudflarestorage.com/bucket/a%20b/%252F/",
        ),
        (
            "https://account.r2.cloudflarestorage.com/bucket/a%20b/%252F/",
            "https://account.r2.cloudflarestorage.com/bucket/a%20b/%252F/",
        ),
    ] {
        let parsed = TableStorageLocation::parse(location).unwrap();
        assert_eq!(parsed.url.as_str(), expected);
        assert!(parsed.backend == StorageBackend::R2);
    }
    for location in [
        "r2://account.r2.cloudflarestorage.com/bucket/table",
        "r2://bucket@unrecognized.example/table",
        "r2://bucket@account.r2.cloudflarestorage.com.example/table",
        "r2://bucket@sub.account.r2.cloudflarestorage.com/table",
        "r2://bucket@account.r2.cloudflarestorage.com:1234/table",
        "r2://bucket:secret@account.r2.cloudflarestorage.com/table",
        "r2://buck%65t@account.r2.cloudflarestorage.com/table",
        "r2://..@account.r2.cloudflarestorage.com/table",
        "https://account.r2.cloudflarestorage.com/",
        "https://account.r2.cloudflarestorage.com/buck%2Fet/table",
        "https://account.r2.cloudflarestorage.com/bucket/table?secret",
        "https://account.r2.cloudflarestorage.com/bucket/table#fragment",
        "http://account.r2.cloudflarestorage.com/bucket/table",
    ] {
        assert!(TableStorageLocation::parse(location).is_err(), "{location}");
    }
}

#[test]
fn r2_options_use_auto_region_and_the_service_endpoint_override() {
    validate_storage_options(&DeltaStorageOptions::new(), Some(StorageBackend::R2)).unwrap();
    let options = DeltaStorageOptions::from([
        ("AWS_DEFAULT_REGION".into(), "auto".into()),
        ("aws_endpoint_url_s3".into(), "http://127.0.0.1:1234".into()),
        ("aws_allow_http".into(), "true".into()),
    ]);
    validate_storage_options(&options, Some(StorageBackend::R2)).unwrap();
    for (key, value) in [
        ("region", "eu-west-1"),
        ("default_region", "us-east-1"),
        ("aws_endpoint", "http://127.0.0.1:1234"),
        ("endpoint_url", "http://127.0.0.1:1234"),
        ("azure_endpoint", "http://127.0.0.1:1234"),
        ("aws_bucket", "other"),
        ("aws_session_token", "secret"),
    ] {
        let options = DeltaStorageOptions::from([(key.into(), value.into())]);
        assert_eq!(
            validate_storage_options(&options, Some(StorageBackend::R2)),
            Err(Error::InvalidStorageOptions),
            "{key}"
        );
    }
}
