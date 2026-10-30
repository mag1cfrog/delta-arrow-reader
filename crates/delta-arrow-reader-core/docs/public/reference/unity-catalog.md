# Unity Catalog access reference

Use this page to check table eligibility, storage options, and credential
behavior. For a first read, follow [Read Unity Catalog tables](../unity-catalog.md).

## Table names and profiles

`inspect`, `preview`, `scan`, `query`, and `explain` accept `uc://catalog.schema.table` with an explicit
`--databricks-profile PROFILE`. Use exactly three unquoted name components.
Each component contains ASCII letters, digits, underscores, or hyphens and
cannot start with a hyphen. Quoted identifiers, URL escapes, query strings,
and fragments are unsupported.

Profiles must be nonblank and contain no control characters. Quote profile
names containing spaces. `--databricks-profile` is invalid for a direct path
or storage URL; `--auth-source` is invalid for a UC table.

In a [tables manifest](cli-inputs.md#join-table-snapshots), each UC entry requires
`authorization.source: "databricks"` and `authorization.databricks_profile`.
Entries can use different profiles, including for the same table name in
different workspaces. Each binding retains its own identity and snapshot.

## Table access

The metastore must allow external data access. The requesting principal needs
`USE CATALOG`, `USE SCHEMA`, `SELECT`, and explicit `EXTERNAL USE SCHEMA`.
Selecting a profile does not grant additional permissions. Administrators can
follow [Databricks external access setup](https://docs.databricks.com/aws/en/external-access/admin).

DAR accepts managed or external Delta tables and the published Delta snapshots
of [materialized views and streaming tables](#materialized-views-and-streaming-tables).
Each must permit direct external-engine reads and use a supported Delta read protocol.
It rejects:

- Ordinary SQL views and shallow clones.
- Row filters and column masks.
- Coordinated commits using `delta.coordinatedCommits.*`.

Managed Delta tables with catalog commits (`catalogManaged`) need no additional
DAR flag. The workspace must support Delta API 1.0 table reads. DAR gets the
confirmed version and commit list from UC before reading storage. An explicit
`--table-version` must not exceed the version confirmed by the catalog.

Tables in [Databricks default storage](https://docs.databricks.com/aws/en/storage/default-storage)
cannot vend the credentials needed for this path. Delta Sharing/OpenSharing
also uses a different authorization flow and is unsupported by this integration.

Reader-protocol validation accepts versions 1, 2, and 3. Supported reader features
are `timestampNtz`, `deletionVectors`, `columnMapping`, `v2Checkpoint`,
`vacuumProtocolCheck`, `typeWidening`, `typeWidening-preview`, and `catalogManaged`. The table and
policy restrictions above still apply.

Run the [inspection step](../unity-catalog.md#inspect-and-pin-the-table) to check
your table with the installed DAR version. UC inspection fails if it rejects
the table or its read protocol; a successful summary reports
`checks.scan_protocol: "passed"`. This checks snapshot metadata, not access to
every data file. An unsupported result means that DAR cannot read the selected
snapshot; changing login credentials cannot add protocol support.

DAR gets the storage location and READ credentials from Unity Catalog. It reads
the Delta log for the snapshot version and Arrow schema; UC column metadata
does not define the output schema. It submits no SQL or compute workload to
Databricks. UC scans and previews accept the same projection, predicate, row
limit, and execution options as direct-path reads. Snapshot selection is available
except for the latest-only objects below.

### Materialized views and streaming tables

Both standalone and pipeline-defined objects require external metadata publishing:

- Set `pipelines.externalMetadata.enabled` to `true` on the object, then refresh it.
- Use a supported pipeline runtime (Databricks Runtime 17.3 or newer) and default
  publishing mode.
- Meet the permissions above. Row filters, column masks, and catalog-managed
  commits are unsupported for this external-access path.

See [Databricks external access for pipelines](https://docs.databricks.com/aws/en/external-access/external-for-pipelines)
for setup and platform restrictions. DAR does not change these settings or run
the refresh.

DAR checks the original object's identity and policies, then requests its published
managed Delta representation through the UC API. That response selects the log
location; DAR does not derive it from private pipeline properties or read the
internal backing table. The credential grant can cover a parent of the published
log directory, including the data files referenced by its log. Renewal must keep
that original grant scope.

Only the latest published snapshot is selectable. `inspect` reports
`time_travel: false`; JSON result, plan, and completion bindings carry the same
marker. `table_version` records the observed snapshot, not a version that can be
requested again. Explicit `--table-version` and `--tables-file` entry versions
are rejected for these objects. The optional `--manifest` is also rejected because
it promises pinned replay. Ordinary file output works.

A command keeps its resolved snapshot for the duration of the read. Separate
commands, including retries, can observe different published snapshots. A streaming
table read returns a finite snapshot; it does not subscribe to future rows.

## Storage backends and credentials

R2 can be configured as a UC [external location](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/external-locations-r2).

| Backend | Temporary credentials | Accepted storage locations returned by UC |
| --- | --- | --- |
| S3 | Access key, secret key, session token | `s3://bucket/path` or `s3a://bucket/path`; S3 access points are unsupported |
| Azure Blob / ADLS | User-delegation SAS or Azure AD bearer token | `abfss://` or Azure Blob/DFS HTTPS URLs identifying the account and container |
| GCS | OAuth bearer token | `gs://bucket/path` |
| R2 | Access key, secret key, session token | `r2://bucket@account.r2.cloudflarestorage.com/path` or `https://account.r2.cloudflarestorage.com/bucket/path` |

Other credential forms and other Azure Blob/R2 endpoint forms are unsupported.
The location comes from UC; users supply the catalog name, not these storage URLs.

### S3 region

S3 needs one nonempty region setting. The credential response does not include
the region, and DAR does not assume a default. Ask the table or storage owner
for the bucket's region and save it as `storage.json`, for example:

```json
{"aws_region":"eu-west-1"}
```

Pass the file to each positional-table command:

```sh
dar inspect uc://main.sales.orders --view summary --databricks-profile work \
  --storage-options-file storage.json
```

For a manifest entry, use `authorization.storage_options_file` instead. That
path is relative to the manifest directory.

### Other storage options

`--storage-options-file` may supply supported region and transport settings,
including endpoints, timeouts, and proxies. It cannot replace the credentials,
bucket, account, container, authentication source, or signature requirements
returned by UC. Those overrides and their aliases are rejected. Unknown
options also fail.

R2 selects region `auto` and needs no region option; an explicit region must be
`auto`. To override its transport endpoint, use `aws_endpoint_url_s3`. The
generic endpoint option is rejected because the R2 URL would override it.

## Credential renewal and failures

Credentials stay in memory for one command. Storage requests reuse the grant
until it approaches expiry, then request new READ credentials for the original
table ID and profile. Concurrent requests share one refresh. A replacement must
keep the same storage root and credential type and have a future expiry.

Renewal preserves the selected Delta snapshot. It does not restart a scan or
repeat rows already written. If renewal fails, the command fails without using
the old grant or falling back to environment or other cloud credentials.
Renewal failures leave preview and JSON query stdout empty. Arrow scans or
queries may leave partial output; discard it and retry the whole command after
correcting the cause.

Each Databricks subprocess has a 30-second timeout, no interactive stdin, and a
1 MiB limit on each output stream. This is a subprocess limit, not a total read
deadline. On failure, DAR includes a filtered error explanation of at most 1024
UTF-8 bytes. It removes SDK request logs and configuration summaries, redacts
known credential fields and bearer values, and removes URL userinfo, queries,
and fragments. If a credential value has no clear boundary, DAR omits the remaining
text. The explanation can still name tables, workspaces, or principals;
review it before sharing. Unlabelled secret values cannot be reliably identified.
Successful child diagnostics are discarded. See
[troubleshooting](../unity-catalog.md#if-a-command-fails) for next actions and
[CLI diagnostics](cli-diagnostics.md) for exits, signals, and completion records.

## Databricks CLI compatibility

The Databricks CLI handles the selected profile's PAT, user OAuth, or
service-principal OAuth authentication. DAR does not start a login flow or read
the CLI token cache. The executable must support:

- `tables get --include-manifest-capabilities`
- `temporary-table-credentials generate-temporary-table-credentials --operation READ`
- `api get` for catalog-managed tables, materialized views, and streaming tables

The table and credential commands were checked with CLI 1.6.0; live Azure and
catalog-managed validation used 1.15.0. This does not establish a minimum
supported version.

<details markdown="1">
<summary id="validation-coverage">Live validation limits</summary>

Live validation covers an Azure managed Delta table using user OAuth and
user-delegation SAS on macOS and Linux ARM64. Coverage includes inspection,
Arrow scans, SQL aggregation, and UC/local joins.

Catalog-managed S3 reads were also checked using Databricks CLI 1.15.0:
inspection, preview, and complete JSON query results for current and pinned
snapshots matched Databricks SQL reference results. Reads where confirmed
commits exist only in staged files are covered by local tests, not live validation.

Materialized view and streaming table reads were checked on S3 with CLI 1.6.0.
Complete results before and after manual refresh matched Databricks SQL references.
Their other storage backends and credential renewal are covered by local tests.

GCS, R2, Azure bearer credentials,
other authentication modes, live renewal, and the full release platform matrix
remain unverified online. Build and environment details are in the
[Azure validation record](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/crates/delta-arrow-reader-cli/release/uc-azure-validation-2026-10-22.md).

</details>
