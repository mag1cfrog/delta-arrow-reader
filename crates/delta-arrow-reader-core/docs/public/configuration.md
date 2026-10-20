---
title: Storage and configuration
description: Connect to cloud storage, then adjust table opening and resource use when needed.
---

# Storage and configuration

<span id="configuration"></span>

Use this page to connect to [S3](#read-from-s3) or [Azure](#read-from-azure),
[reduce work before the first query](#want-less-work-before-the-first-query),
or [limit parallel reads](#adjust-queries-when-needed).

The examples use Python. See [Rust configuration](#configure-rust) for Rust; CLI
users can pass the same storage keys in a
[storage options file](cli.md#storage-options-file).

## Start with the defaults

After [installing the Python package](https://mag1cfrog.github.io/delta-arrow-reader/installation/#python),
open a table using its path:

```python
from delta_arrow_reader import DeltaTable

table = DeltaTable("/path/to/table")
```

Keep using the same `table` for later queries. Each new `DeltaTable` opens the
table again and repeats its preparation. A loaded table stays on one version.
[Refresh](python.md#read-again-or-use-newer-data) opens the latest version as a
new table.

## Read from S3

Use an `s3://bucket/table` URL in place of a local path and supply your storage
settings. For example, if you use AWS credentials from environment variables:

Set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in your environment. When
using temporary credentials, also set `AWS_SESSION_TOKEN`. Replace the bucket,
table prefix, and region in this example:

```python
import os

from delta_arrow_reader import DeltaTable

options = {
    "aws_region": "<region>",
    "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
    "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
}
if token := os.environ.get("AWS_SESSION_TOKEN"):
    options["aws_session_token"] = token

table = DeltaTable("s3://<bucket>/<table-prefix>", storage_options=options)
```

Keep credentials out of source code. `storage_options` takes string keys and
values; the available settings depend on the storage backend.

The URL must point to the directory containing `_delta_log`. The reader
accesses storage directly and needs credentials that can list and read the
table's log and data files. A Databricks table name or workspace PAT is not a
storage location or storage credential.

Use `table.to_reader()` as in the [Python quickstart](python.md#read-your-first-rows)
to read the rows.

## Read from Azure

For Azure Blob Storage or ADLS Gen2, use the table's storage URL. This example
uses a shared access signature (SAS) token with permission to list and read
the table's files. Set `AZURE_STORAGE_SAS_TOKEN` in your environment, then
replace the container, account, and table prefix:

```python
import os

from delta_arrow_reader import DeltaTable

table = DeltaTable(
    "abfss://<container>@<account>.dfs.core.windows.net/<table-prefix>",
    storage_options={
        "azure_storage_sas_key": os.environ["AZURE_STORAGE_SAS_TOKEN"],
    },
)
```

The equivalent Blob Storage URL is
`https://<account>.blob.core.windows.net/<container>/<table-prefix>`.
Both URLs point to the directory containing `_delta_log`. Keep the SAS token
in `storage_options`, outside the URL and source code. This is one authentication
option; other credentials use the storage backend's
[Azure configuration keys](https://docs.rs/object_store/0.13.2/object_store/azure/enum.AzureConfigKey.html).

Once the table opens, use the same
[Python reading methods](python.md#read-your-first-rows) as for a local table.

## Want less work before the first query?

With the default `automatic` mode on supported S3 connections, opening a table
prepares information for later queries and checks the connection speed.
**The connection check can take up to
five seconds**, plus the time needed to load the file information. It may finish
sooner. Local tables and Azure skip this extra preparation.

<details markdown="1">
<summary>What the S3 preparation does</summary>

Delta tables store their rows in data files. Opening a supported S3 table
prepares two things that later queries can reuse:

1. It loads information about the table's files and keeps it in memory. Queries
   use this to decide which files they need.
2. It reads parts of data files to check the connection speed. This helps it
   decide when downloading less data will make a query faster.

The saved information stays in memory; it does not cache query results or
persist on disk.

An incomplete check does not by itself fail table loading. The first query that
can use the measurements retries once, with the same five-second budget.
Concurrent queries share the retry. If it also fails, queries use normal reads
without another attempt.

The check makes real requests and downloads data. It requires the default
`direct` backend and a [supported S3 connection](https://mag1cfrog.github.io/delta-arrow-reader/partial-page-reads/).

</details>

Set `warmup` when opening the table. Keep the same path and any `storage_options`
you normally use; the examples below show just the setting being changed.

### Skip the connection check

If you want to keep the useful file information but avoid test reads of data
files, use `query_planning`:

```python
table = DeltaTable("s3://bucket/table", warmup="query_planning")
```

You still wait for the file information to load. It stays in memory for later
queries, which is useful when querying the same table repeatedly. This option
also works for local tables.

### Leave the preparation to each query

If you will query only once, or want to avoid keeping the file information in
memory, use `none`:

```python
table = DeltaTable("s3://bucket/table", warmup="none")
```

Opening still reads the table version and its column names and types. Each
query then builds the file information it needs. Repeated queries may spend
more time doing that work.

Both options skip the connection check, including any later retry. Queries can
still skip unwanted data, but [partial-page reads](https://mag1cfrog.github.io/delta-arrow-reader/partial-page-reads/)
stay off. That optimization needs the connection measurements to decide when
to download selected values within a block of data.

Compare the time to open the table **and** run your queries when choosing a mode.

## Adjust queries when needed

Start with the default read settings. Change one setting at a time if a query
uses more resources than your application can spare.

<details markdown="1">
<summary id="limit-parallel-reads">Limit how many files a query reads at once</summary>

If a query uses too many resources, try a limit on concurrent file reads. This
example allows at most four files to be read at once in one scan:

```python
from delta_arrow_reader import ScanExecutionOptions

options = ScanExecutionOptions(max_concurrent_file_reads_per_scan=4)
with table.to_reader(execution_options=options, limit=10) as reader:
    print(reader.read_all())
```

Lowering this limit can reduce resource use, but may slow the query. It applies
to each scan separately, does not cap network requests or total memory, and does
not change the connection check during table loading.

Pass `execution_options` to `DeltaTable(...)` to set defaults for its direct
scans. Passing it to `to_reader()` or `scan()` replaces the full options object
for that one scan.

For controls over buffering and reading ahead, see
[ScanExecutionOptions](python-api/options.md). For the number of groups of files
processed in parallel, see `target_partitions` in
[to_reader()](python-api/table.md#delta_arrow_reader.DeltaTable.to_reader).

</details>

<details markdown="1">
<summary id="configure-python-sql">Use the loaded table with Python SQL</summary>

Register the table you already opened. SQL reuses its file information and any
connection measurements. Registration does not repeat the preparation.

SQL registrations have their own default read settings. To give SQL a custom
file-read limit, pass it when registering the table:

```python
from delta_arrow_reader import ScanExecutionOptions, Session

options = ScanExecutionOptions(max_concurrent_file_reads_per_scan=4)
session = Session()
session.register_table("events", table, execution_options=options)
```

Omit `execution_options` to use SQL's defaults. Custom read settings passed to
`DeltaTable` do not automatically carry over to SQL. The
[SQL quickstart](python.md#query-with-sql) shows how to run the query afterward.

</details>

<details markdown="1">
<summary id="configure-rust">Configure a table in Rust</summary>

Choose the same behavior with `with_warmup`. This example skips the connection
check and keeps the file information for later queries:

```no_run
use delta_arrow_reader::{DeltaReaderError, DeltaTable, DeltaTableBuilder, WarmupMode};

async fn open_table() -> Result<DeltaTable, DeltaReaderError> {
    DeltaTableBuilder::new("s3://bucket/table")
        .with_warmup(WarmupMode::QueryPlanning)
        .load_table()
        .await
}
```

Call this function inside your application's Tokio runtime. Use
`WarmupMode::Automatic` for the default or `WarmupMode::None` to leave preparation
to queries.

Rust also lets you shorten or extend the connection check. To give it two
seconds, replace `with_warmup` above with:

```ignore
.with_warmup(WarmupMode::Network {
    max_duration: std::time::Duration::from_secs(2),
})
```

That limit applies to the check and its possible retry, after file information
has loaded. Zero skips the check but still prepares file information. Python
currently supports the three named modes without a custom time limit.

For Rust read limits, use `DeltaScanExecutionOptions` on the table builder or
one scan. SQL uses `datafusion::ScanOptions.execution_options` when
[registering the table](https://mag1cfrog.github.io/delta-arrow-reader/datafusion/).
Changing read settings later does not undo preparation already performed.

### Renew temporary storage credentials

Pass an `object_store` credential provider to the table builder when credentials
may expire while a table is open. This example accepts an AWS provider from your
application:

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

Use `Aws` for S3 or R2, `Azure` for an Azure provider returning SAS or bearer
credentials, and `Gcp` for a GCS provider returning OAuth tokens. R2 uses a native
`https://account.r2.cloudflarestorage.com/bucket/table` URL; GCS uses `gs://bucket/table`.

The provider owns credential caching and renewal. It can be called concurrently,
including from Delta Kernel's runtime, and must return usable credentials or an
error. The reader retains it for scans, table clones, and snapshot refreshes.
Updating credentials does not change the table's snapshot version.

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

If the provider fails, the operation returns an error without trying environment,
instance, or credential-file authentication. Error `Display` and `Debug` stay
redacted; the raw dependency error remains available through `source()`.
The provider API is available to Rust callers.

</details>

For the complete Python settings and defaults, see
[ScanExecutionOptions](python-api/options.md). Rust callers can use the
[execution options reference](https://mag1cfrog.github.io/delta-arrow-reader/reference/execution-options/).
