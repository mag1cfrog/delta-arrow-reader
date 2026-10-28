# Renewable storage credentials in Rust

Use `DeltaTableBuilder::with_credential_provider` when your application already
has an `object_store` provider that can renew temporary cloud credentials.
This API accepts a provider supplied by your application; it does not resolve
Unity Catalog table names. CLI users can instead follow the
[Unity Catalog guide](../unity-catalog.md), where DAR manages renewal.

## Supply a provider

The provider types use `object_store` 0.13.2. Add the matching dependency to your
application:

```sh
cargo add delta-arrow-reader@0.8
cargo add object_store@0.13.2
```

This function accepts an AWS provider from your application:

```no_run
use delta_arrow_reader::{
    DeltaReaderError, DeltaStorageCredentialProvider, DeltaStorageOptions,
    DeltaTable, DeltaTableBuilder,
};
use object_store::aws::AwsCredentialProvider;

async fn open_with_credentials(provider: AwsCredentialProvider) -> Result<DeltaTable, DeltaReaderError> {
    DeltaTableBuilder::new("s3://bucket/table")
        .with_storage_options(DeltaStorageOptions::from([
            ("aws_region".into(), "us-east-1".into()),
        ]))
        .with_credential_provider(DeltaStorageCredentialProvider::Aws(provider))
        .load_table()
        .await
}
```

## Supported providers

Use `Aws` for S3 or R2, `Azure` for an Azure provider returning SAS or bearer
credentials, and `Gcp` for a GCS provider returning OAuth tokens. R2 uses a native
`https://account.r2.cloudflarestorage.com/bucket/table` URL; GCS uses `gs://bucket/table`.

## Lifetime and renewal

The provider owns credential caching and renewal. It can be called concurrently,
including from Delta Kernel's runtime, and must return usable credentials or an
error. The reader retains it for scans, table clones, and snapshot refreshes.
Updating credentials does not change the table's snapshot version.

## Storage options and precedence

The provider takes precedence over ordinary authentication settings in
`DeltaStorageOptions`, following the native client's `with_credentials` rules.
For example, an AWS provider overrides static keys, and a GCP provider skips
Application Default Credentials.

Omit `skip_signature`, Azure's `use_emulator`, and GCP's `service_account` and
`service_account_key` options, including their aliases, when supplying a provider.
These options can bypass the provider or load service-account data separately.
Their presence is rejected before storage reads, even when a boolean value is
`false`. Providers for the wrong cloud also fail before reads.

Transport and region options still follow the native storage client's rules:
values derived from the URL override options. R2 selects the `auto` region and
accepts `aws_endpoint_url_s3` for an endpoint override.

## Provider failures

If the provider fails, the operation returns an error without trying environment,
instance, or credential-file authentication. Error `Display` and `Debug` stay
redacted; the raw dependency error remains available through `source()`.
