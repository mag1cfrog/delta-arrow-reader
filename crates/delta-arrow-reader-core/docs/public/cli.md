# Command-line reference

Look up command arguments, input formats, and process behavior here. For a first
read, use the [CLI quickstart](cli-quickstart.md). The shell examples use Bash.

| Task | Command or section |
| --- | --- |
| Discover commands and argument rules offline | [`dar capabilities`](#discover-installed-commands) |
| See the table's columns and version | [`dar inspect`](#inspect-a-snapshot) |
| Save selected rows as Arrow | [`dar scan`](#scan-rows) |
| Aggregate, group, or sort rows with SQL | [`dar query`](#query-a-snapshot) |
| Join named table snapshots | [Tables manifest](#join-table-snapshots) |
| Select rows by a condition | [Filter rows](#filter-rows) |
| Connect to remote storage | [Storage options file](#storage-options-file) |
| Select S3 credentials from a file or environment | [S3 credential sources](#s3-credential-sources) |
| Select Azure credentials from a file or environment | [Azure credential sources](#azure-credential-sources) |
| Select a GCS credentials file | [GCS credential sources](#gcs-credential-sources) |
| Read a Unity Catalog table by name | [Databricks profile](#read-a-unity-catalog-table) |
| Change parallel reads or buffering | [Execution options](#execution-options) |
| Handle a command's result in a script | [Inspection JSON](#inspection-json) and [exit statuses](#exit-statuses-for-scripts) |

The standalone command is `dar`. A Python wheel installs `delta-arrow-reader`
instead; substitute that name in the examples. Both accept the same arguments.
For packages and runtime requirements, see [Installation](installation.md).

## Discover installed commands

```sh
dar capabilities
dar capabilities --command query
dar capabilities --schema predicate
dar capabilities --schema predicate --detail timestamp_us
dar capabilities --schema completion
dar capabilities --authorization s3
dar capabilities --authorization s3 --full
dar capabilities --schema completion --full > completion-schema.json
dar capabilities --all --diagnostics=json > capabilities.json
```

Start with `dar capabilities` to choose a command. It returns a JSON directory
of at most 2 KiB, with command names, short descriptions, and schema names.
Each command's `details` field gives the invocation for
its usage guide. `schema_command` gives the invocation for a format guide;
replace its `NAME` argument with one of the listed names. `all_command` gives
the invocation for the complete inventory. These invocations are arrays of
command-line arguments, starting with the installed executable's name.

| Invocation | JSON `kind` | Contents |
| --- | --- | --- |
| `dar capabilities` | `capability_index` | Short directory and commands for loading details. |
| `dar capabilities --command query` | `command_guide` | A usable example, essential options and limits, output format, and related lookups. |
| `dar capabilities --authorization s3` | `authorization_guide` | Canonical credential keys and necessary access rules for one service. |
| `dar capabilities --schema NAME` | `schema_guide` | One example, format notes, file size limit, and available fields or types. |
| `dar capabilities --schema NAME --detail FIELD_OR_TYPE` | `schema_detail` | One field, predicate operator, or scalar type from that guide. |
| `dar capabilities --command query --full` | `command_capabilities` | Complete query argument metadata, authorization, formats, schemas, and limits. |
| `dar capabilities --authorization s3 --full` | `authorization_capabilities` | Credential aliases and complete access rules for the selected service. |
| `dar capabilities --all` | `capabilities` | Complete command tree and support information. |
| `dar capabilities --schema NAME --full` | `input_schema` or `output_schema` | One complete JSON Schema validation document. |

All responses have `format_version: 1`. The directory and full definitions also
include `cli_version`. `--command`, `--schema`, `--authorization`, and `--all`
are mutually exclusive. `--full` requires `--command`, `--schema`, or
`--authorization`. `--detail` requires `--schema` and conflicts with
`--full`. Command names must match an installed top-level command. Unsupported
names or conflicting selectors fail with status 2.
For text help, use `dar query --help` or the corresponding command's help.

For output and execution-option schemas, `--detail` accepts the example's
top-level field names and any names in `other_fields`. The latter lists only
fields absent from the example; it is omitted when the example shows them all.

For example, after finding `query` in the directory, run
`dar capabilities --command query`. Its `example` is an argument array with
placeholder paths. The `sql` field gives the table alias `t`, the `--sql-file`
argument, and a count query to put in that file. `stdout` identifies Arrow IPC;
redirect it to a file or pipe. Redirection is a shell operation, not part of the
argument array. `options` describes commonly needed parameters, and
`resource_defaults` gives the query's resource ceilings. Use `help_command`
for all options or `full_command` for their machine-readable definitions. Fetch
`--schema storage-options` if a storage-options file is needed, then follow
`authorization_command` for the selected service. Local paths need no
credentials. To recover these details later, repeat the same discovery commands;
discovery does not depend on previous invocations.

For a local JOIN, the query guide's `join.example` uses `--tables-file` instead
of a positional table, and `join.sql` names the aliases to declare. Fetch
`--schema tables` for the manifest example and relative-path rules. This takes
three lookups from the directory to a runnable JOIN. The manifest guide's
`detail_syntax` explains how to select nested fields; `other_fields` lists the
optional authorization reference omitted from the example. Select
`--detail 'tables[].authorization'` for credential references and service guides.

Authorization guide names are `s3`, `azure-blob`, `gcs`, and
`unity-catalog`. The first three describe direct storage access; Unity Catalog
describes access through a Databricks profile. Command guides list the applicable
names in `authorization.guides`; the storage-options guide lists
`authorization_guides`. Each authorization response identifies its guide in
`name`. For example, use `dar capabilities --authorization azure-blob`
for Azure Blob Storage, including its Data Lake Storage Gen2 capabilities.
The default guide lists canonical credential keys. Its `full_command` supplies
alternate spellings and complete service metadata when needed. Neither response
contains credential values or checks the caller's access.

An input schema guide's `example` can be used as the file contents, subject to the
listed constraints. For predicates, the example's column and literal type must
match the table. Predicate guides list available `operators` and `scalar_types`.
Replace the `NAME` argument in `detail_command` with the field, operator, or
scalar type needed. For example,
`--schema execution-options --detail prefetch_files_per_partition` gives one
setting's default and bounds. `--schema predicate --detail timestamp_us` gives
the timestamp unit, integer encoding, and required timezone field. Selected
details include only applicable conditional rules and integer-notation rules.

These guides describe how to use a format. Recursive predicate references lead
to another operator or scalar choice through `detail_command`. Use a guide's
`full_command` when a JSON Schema validator or a metadata consumer needs the
complete definition.

For result verification, `--schema completion` shows an exhausted scan and
explains which counts, snapshot binding, and completion fields to check against
the request and saved output. It also distinguishes a row-limited scan and a SQL
result from a complete table read. Use `--detail FIELD` for a field's exact
type or bounds; `--full` exports the schema for a JSON Schema validator.

In full command metadata and the full inventory, `command` contains the selected
command or complete command tree, and each argument's required status, value
count, defaults, allowed choices, repeatability, and conflicts. For example,
`scan --column` is repeatable and conflicts with `--no-columns`.
`stdout_formats` identifies result formats; `diagnostics` describes stderr
formats. `input_schemas` lists the applicable schema names and their file
arguments. `output_schemas` lists result schema names and their streams. The
`inspect` entry in `stdout_formats` and the JSON entry in `diagnostics` identify which
output schemas apply.

In full metadata, `authorization` describes local access, direct-storage sources
and configuration files, and Databricks profiles, including the commands that
accept each. Direct storage supports explicit source selection per command or
manifest table. Its authorization guide lists supported sources, credential inputs,
and the backend lookup used when source selection is omitted. Discovery
does not inspect the caller's credentials or check provider availability.
The authorization guide's `full_command` returns complete metadata for the
selected service, including credential option aliases.

`read_support` describes each command's scope and limits. For example, `query`
registers positional `TABLE` as `t`, or up to 64 named snapshots from a manifest.
UC SQL accepts a positional table with an explicit Databricks profile, or
manifest entries with per-table profile references. A manifest can mix local,
direct-storage, and UC tables. The memory ceiling
applies to DataFusion's execution pool, and disk spill is disabled. The SQL examples that use `id`
assume the selected table has that column. A null limit or deadline means that
the command has no ceiling for that resource; it does not mean zero.

An empty `possible_values` list means the argument has no enumerated choices;
it does not mean every value is valid. A null `max_values` means there is no
fixed upper count for that argument. Descriptions contain additional input rules.

Pass a name to `--schema` with `--full` to return an object with
`kind: "input_schema"` or `kind: "output_schema"`, `format_version: 1`,
`cli_version`, `name`, and `schema`.
The `schema` field contains a [JSON Schema draft 2020-12](https://json-schema.org/draft/2020-12/json-schema-validation)
document with examples.
Extract that field when passing it to a JSON Schema validator.
Input schema responses also include `argument` and `parser_constraints`:

| Schema name | File argument | Contents |
| --- | --- | --- |
| `storage-options` | `--storage-options-file` | A map of option names to string values. The selected backend checks their meanings. |
| `execution-options` | `--execution-options-file` | Optional scan settings with their defaults, accepted nulls, and integer bounds for this build. |
| `predicate` | `--predicate-file` | Row-filter operators and typed literals, including nested boolean groups. |
| `tables` | `--tables-file` | Named snapshots, requested versions, and per-table authorization references. |

Schema validation checks the JSON structure. `parser_constraints` records the
additional file and value rules: files may contain at most 1 MiB, object keys
must be unique, and integer fields reject fractions, exponents, and negative
zero.
Tables also require unique, non-reserved aliases, versions in the u64 range,
valid locations, and compatible authorization references. Those checks run
before table IO; JSON Schema validation alone does not replace them.

Predicate trees allow 1,024 nodes and 32 levels, counting the root.
Integer strings must fit their declared width, floats must remain finite after
conversion, and fixed-size binary lengths must match. Predicate column and type
checks still require a table. These rules also apply to the published examples;
for instance, the example using `id` as `int32` requires an `Int32` column.

Output schema responses include `stream`, identifying where the described
result appears. The schema response itself always goes to stdout.

| Schema name | Result stream | Contents |
| --- | --- | --- |
| `inspection` | stdout | The full `inspect` JSON, including the snapshot version and Arrow schema. |
| `inspection-summary` | stdout | Read checks, matching column names, or selected column types. |
| `diagnostic` | stderr | A terminal failure record with `--diagnostics=json`, including optional correction fields. |
| `completion` | stderr | A terminal success record with `--diagnostics=json`, including snapshot bindings and output counts. |

Output schemas allow additional object fields. Version and count strings use
unsigned decimal notation; consumers must check the u64 range when converting
them. The inspection schema checks the Arrow schema's outer fields, metadata,
and type-description shapes. Use Arrow's Rust Serde decoder to validate the nested
`data_type` encoding. `scan` and `query` return Arrow IPC streams, whose schema
is carried in the stream itself.

For example, save `dar capabilities --schema completion --full` and validate the
command's stderr record against its `schema` field. Also check exit status 0
and decode stdout: a matching completion record alone does not establish that
the result was received intact.

Discovery needs no table, input file, credentials, Databricks executable, or
network access. The directory, schema guides, schema details, and authorization
guides are limited to 2 KiB each; command guides are limited to 4 KiB. Explicit
full command and authorization metadata, full schemas, and the full inventory
are limited to 1 MiB.
Limits count UTF-8 bytes, including JSON framing and the final newline. An oversized
response fails before writing stdout; discovery never truncates JSON.
The command has a five-second deadline, including stdout, log setup, and
completion reporting on stderr.
A closed stdout pipe fails with status 3; an expired deadline fails with
status 1. Failure reporting has at most one additional second before exit,
including when stderr is blocked. Full command and authorization metadata and
the inventory report that allowance in `limits.failure_diagnostic_timeout_ms`.
Runtime initialization and argument errors use the same
reporting allowance. With `--diagnostics=json`, success produces a completion
record on stderr, and failure produces a diagnostic when stderr can accept it.
In this mode, a closed stderr pipe also fails with status 3.
Use a separate stderr pipe for each command. If the pipe is full, discovery
waits for capacity before publishing completion; cancellation or timeout during
that wait produces a failure instead.
Cancellation keeps status 130 for SIGINT and 143 for Unix SIGTERM.

Consumers must check `format_version` and ignore additional object fields.
Changes to existing field types or meanings require a new format version.
Schema names describe inputs and results of that installed CLI version. Unsupported
names are argument errors. Input files retain their existing formats: do not add
the discovery envelope or `format_version` to them.

## Inspect a snapshot

`inspect --view summary` returns the table version and scoped read checks as JSON. It reads Delta
metadata without reading row data or deletion-vector files. For a direct path or
storage URL, a table whose scan protocol is unsupported can still be inspected
if its snapshot and schema load. Unity Catalog targets require a supported read
protocol.

```sh
dar inspect /data/orders --view summary
dar inspect /data/orders --view summary --table-version 0 --column-prefix ord
dar inspect /data/orders --view summary --table-version 0 --column order_id
dar inspect --view summary --auth-source json --storage-options-file ./storage.json s3://example-bucket/orders
dar inspect --view summary -- './-table with spaces, and commas'
```

Use the reported `table_version` for follow-up lookups. `--column-prefix` returns
only matching top-level names, in schema order. Matching is case-sensitive; an
empty prefix matches every name. `--column` returns the exact name, type, and
nullability of each selected top-level column, in request order. Repeat it for
multiple columns. Name and type lookups omit checks. Add `--include-metadata`
to include nonempty metadata on selected columns and their nested fields.

Summary stdout defaults to 4 KiB, including its final newline. If a selection
cannot fit, the command exits 1 before writing stdout. Narrow the prefix, select
fewer columns, omit metadata, or explicitly increase `--max-output-bytes N`.
Names and types are never truncated. There is no total command deadline.

Omitting `--view`, or choosing `--view full`, preserves the complete Arrow schema
for existing consumers. Full inspection has no output ceiling; redirect it to a
file. Selection, metadata, and budget options require the summary view.

The command accepts exactly one UTF-8 local path or URL. Relative paths use
the process working directory. Quote paths and option values that contain
spaces; each must arrive as one command-line argument. `--` ends option
parsing and allows a table name beginning with `-`.

```text
dar inspect [--table-version N] [--storage-options-file PATH]
            [--auth-source SOURCE] [--databricks-profile PROFILE] [--view full|summary]
            [--column NAME ... | --column-prefix PREFIX]
            [--include-metadata] [--max-output-bytes N] [--] TABLE
```

<details markdown="1">
<summary>Version bounds and argument parsing</summary>

Each option except `--column` may appear once. Omit `--table-version` to select the latest
snapshot, or pass unsigned decimal digits from `0` through
`18446744073709551615`. Signs, whitespace, fractions, exponents, and overflow
are rejected. Missing values, unknown options, and extra arguments are errors.
Running `dar` without a subcommand exits with status 2.

The parser accepts the full u64 range. The current Kernel cannot load version
`18446744073709551615`; requesting it returns a snapshot error (status 1).

Explicit versions use a forward listing of retained log entries up to the
requested version. Kernel validates the commits and checkpoints in that listing.
Discovery does not search numeric version gaps.

</details>

## Read a Unity Catalog table

Use an existing Databricks CLI workspace profile to resolve a table name and
obtain temporary cloud credentials. DAR reads the Delta log and data files from
S3, Azure Blob, GCS, or R2 using local compute. `inspect` reports snapshot metadata
and version; `scan` streams its rows as Arrow IPC, and `query` runs local SQL.
Unity Catalog column metadata does not determine the output schema. These commands
do not submit SQL to Databricks or start a cluster or SQL warehouse.

```sh
dar inspect uc://main.sales.orders --view summary --databricks-profile work
dar inspect uc://main.sales.orders --view summary --databricks-profile work --table-version 0
dar scan uc://main.sales.orders --databricks-profile work \
  --column customer_id --limit 100 > orders.arrow
```

Install the Databricks CLI in the same environment as DAR and make it available
on `PATH`. DAR binaries, wheels, and images do not bundle it. The required
commands and flags were checked with Databricks CLI 1.6.0; live Azure validation
used 1.15.0. These checks do not establish a minimum supported version. The CLI
must support
`tables get --include-manifest-capabilities` and
`temporary-table-credentials generate-temporary-table-credentials --operation READ`.
It handles the selected profile's PAT, user OAuth, or service-principal OAuth
authentication. DAR does not start a login flow or read the CLI's token cache.

For a user OAuth profile, log in once from the environment where DAR will run:

```sh
databricks auth login --host https://YOUR_WORKSPACE_HOST --profile work
```

Use your workspace URL, not the account console URL. An existing working profile
needs no new login. For service-principal OAuth or PAT profiles, follow
[Databricks CLI authentication](https://docs.databricks.com/aws/en/dev-tools/cli/authentication).

An administrator must enable external data access on the metastore. The
requesting principal needs `USE CATALOG`, `USE SCHEMA`, `SELECT`, and explicit
`EXTERNAL USE SCHEMA`. Both the workspace and storage endpoint must be reachable.
See [Databricks external access setup](https://docs.databricks.com/aws/en/external-access/admin).
Selecting a profile does not grant additional permissions.

The storage paths and credential forms below have offline test coverage. Live
inspection, scans, and local SQL passed with Azure Databricks, user OAuth, Azure
SAS, and a managed Delta table on macOS and Linux ARM64. The macOS source build
and installed dev-profile wheel, Linux source build, and extracted release archive
passed; this does not establish the full release matrix. Other storage backends,
Azure bearer credentials, and other authentication modes remain unverified online.
A public bucket or a table shared through Delta Sharing/OpenSharing does not
exercise this authorization flow. Tables in
[Databricks default storage](https://docs.databricks.com/aws/en/storage/default-storage)
also cannot vend the credentials needed for direct reads.

For S3, supply the bucket's region through the existing storage options file.
The credential response does not include it, and DAR does not assume a default:

```json
{"aws_region": "eu-west-1"}
```

```sh
dar inspect uc://main.sales.orders --view summary --databricks-profile work \
  --storage-options-file ./storage.json
```

These rules apply to Unity Catalog targets:

- Use exactly three unquoted name components, separated by dots. Each component
  contains ASCII letters, digits, underscores, or hyphens and cannot start with
  a hyphen. Quoted identifiers, URL escapes, query strings, and fragments are
  unsupported. The profile must be explicit and nonempty. Quote profile names
  containing spaces. `--databricks-profile` is invalid for direct paths or URLs.
- Only ordinary managed or external Delta tables with a supported read
  protocol are admitted. Views, pipeline tables, shallow clones, row filters,
  column masks, catalog commits (`catalogManaged`), and coordinated commits
  are unsupported. Authorization failures never fall back to cloud credentials
  from the environment.
- Supported credentials are S3/R2 temporary key/secret/session-token credentials,
  Azure user-delegation SAS or Azure AD bearer tokens, and GCS OAuth bearer tokens.
  GCS locations use `gs://bucket/path`. Azure Blob locations must
  identify the account and container using `abfss://` or an Azure Blob/DFS HTTPS
  URL. R2 locations use `r2://bucket@account.r2.cloudflarestorage.com/path`
  or the equivalent `https://account.r2.cloudflarestorage.com/bucket/path`.
  Other credential types, S3 access points, and other Azure Blob/R2 endpoints are
  unsupported.
- Storage options may supply supported region and transport settings, including
  endpoints, timeouts, and proxies. Credential, bucket, account, container,
  authentication-source, and unsigned-access overrides are rejected, including
  their aliases. S3 needs one nonempty region setting. R2 uses `auto` and needs
  no region option; an explicit region must be `auto`. To override the R2
  transport endpoint, use `aws_endpoint_url_s3`; the generic endpoint option
  is rejected because the R2 URL would override it. Unknown options fail.
- Credentials stay in memory for one command. Storage requests reuse the grant
  until it approaches expiry, then request new READ credentials for the original
  table ID and profile. Concurrent requests share one refresh. Every replacement
  must keep the same storage root and credential type and have a future expiry.
  If renewal fails, the command fails without using the old grant or falling
  back to another identity. Renewal keeps the selected Delta snapshot and does
  not restart a scan or repeat rows already written.

UC scans accept the same projection, predicate, limit, snapshot-version, and
execution options as direct-path scans. If renewal fails after output starts,
the process exits unsuccessfully and leaves an incomplete Arrow stream.

Each Databricks subprocess has a 30-second timeout, no interactive stdin, and
a 1 MiB limit on each output stream. DAR captures and discards child diagnostics
instead of forwarding them. Errors use tracing diagnostics on stderr with fixed
messages that omit profile names, table names, storage URLs, and credentials.
See [Errors and process status](#errors-and-process-status) for output and
filtering rules. Check the selected profile's login and permissions separately
when a metadata or credential request fails.

If user OAuth login has expired or been revoked, repeat `databricks auth login`
with the same workspace and profile, then rerun the command. A permissions error
requires the administrator to check the grants and external-access setting above;
logging in again cannot grant access. If a scan or query fails after writing rows,
discard its partial output and rerun the whole command at the same snapshot.
See [Scan rows](#scan-rows) and [Errors and process status](#errors-and-process-status)
for the checks needed before accepting IPC output.

## Scan rows

```sh
dar scan /data/orders > orders.arrow
dar scan --column customer_id --column total --limit 100 /data/orders > sample.arrow
dar scan --no-columns /data/orders > row-counts.arrow
dar scan --table-version 0 /data/orders > original.arrow
```

The output uses Arrow IPC **stream** framing. Open it with a streaming IPC
reader. Stdout must be a file or pipe; a terminal is rejected with status 2
before table loading, including for `--limit 0`.

```text
dar scan [--table-version N] [--storage-options-file PATH]
         [--auth-source SOURCE] [--databricks-profile PROFILE]
         [--predicate-file PATH] [--column NAME ... | --no-columns]
         [--execution-options-file PATH] [--target-partitions N] [--limit N] [--] TABLE
```

Table paths, snapshot selection, and storage options follow the inspection
rules. Each invocation reads one immutable snapshot.

CLI scans skip table warmup and S3 connection sampling, so partial-page reads
stay off. File, row-group, and whole-page skipping remain available. For repeated
queries with automatic connection sampling, use the [Python](python.md) or
[Rust](streaming-reader.md) API.

<details markdown="1">
<summary>Column selection and row-limit rules</summary>

Omit projection flags to read all columns. Repeat `--column` once per logical
column, in the desired output order. A name containing commas or dots remains
one name. The reader validates names and rejects duplicates. `--no-columns`
selects zero columns and preserves the number of live rows in each batch. It
conflicts with `--column` and may appear once.

`--limit` accepts unsigned decimal digits in the platform's `usize` range and
may appear once. Zero is valid; omitting it reads all rows. Limits count live
rows after deletion vectors and predicate filtering. The command does not
promise a global row order.
Empty results, including `--limit 0`, contain the selected schema in a valid
IPC stream.

</details>

<details markdown="1">
<summary>Streaming, schema preservation, and memory use</summary>

The CLI completes scan planning before writing the schema, then writes and
flushes each batch before requesting another. It preserves logical types,
nullability, nested values, and schema and field metadata. It does not collect
the whole result. Memory use includes the core's read buffers, the current
batch, and IPC encoding state; variable-sized batches have no fixed byte bound.

</details>

Require both successful Arrow decoding and a successful child exit status.
IPC readers can accept EOF at a message boundary, so readable output can still
be incomplete. Scan or encoding errors exit 1 without writing the IPC end
marker. Stdout failures, including an early pipe close, exit 3. Status 0 means
the end marker was written and stdout was flushed. Shell redirects can leave
partial files after failure; discard those files.

## Query a snapshot

`query` runs SQL locally through the Core DataFusion table provider. It opens
one Delta snapshot as `t` when given `TABLE`, or registers the aliases in a
[tables manifest](#join-table-snapshots). Positional `TABLE` accepts a local path,
direct storage URL, or `uc://catalog.schema.table` with `--databricks-profile`.
UC queries use the same [authorization prerequisites](#read-a-unity-catalog-table)
and credential renewal as scans. Databricks supplies metadata and READ credentials;
SQL executes locally without a Databricks cluster or warehouse.

```sh
cat > query.sql <<'SQL'
SELECT customer_id, count(*) AS orders, sum(total) AS total
FROM t
GROUP BY customer_id
ORDER BY total DESC
LIMIT 100
SQL
dar query /data/orders --sql-file query.sql > totals.arrow
dar query /data/orders --table-version 0 --sql-file query.sql > original.arrow
dar query uc://main.sales.orders --databricks-profile work \
  --storage-options-file storage.json --sql-file query.sql > totals.arrow
```

```text
dar query [--table-version N] [--storage-options-file PATH]
          [--auth-source SOURCE] [--databricks-profile PROFILE]
          --sql-file PATH [--max-rows N] [--max-output-bytes BYTES]
          [--memory-limit-bytes BYTES] [--] TABLE
```

The SQL file must contain one read-only query in UTF-8, at most 64 KiB, with
a parser and expression-depth limit of 128, including chains of set operations
such as `UNION`. Paths use the working directory; `-` names a
literal file. SQL is not read from stdin. Malformed input is rejected before
opening any table. Projections, predicates, joins, aggregations, grouping, ordering,
subqueries, and nonrecursive CTEs are supported. DDL, DML, COPY, session changes,
EXPLAIN, recursive CTEs, and table functions are rejected. SQL cannot discover
additional tables from file paths or URLs.

Results use Arrow IPC stream framing on stdout, which must be a file or pipe.
The command streams the complete SQL result, including empty results and
`COUNT(*)`. Use SQL `LIMIT` to select a smaller result. Positional table queries
use the same snapshot selection and storage options as `inspect`. All queries
skip table warmup.

| Option | Default | Behavior |
| --- | --- | --- |
| `--max-rows` | `1000000` | Fail if the SQL result exceeds this many rows; zero is valid |
| `--max-output-bytes` | `1073741824` (1 GiB) | Fail if Arrow output, including schema and framing, exceeds this many bytes |
| `--memory-limit-bytes` | `268435456` (256 MiB) | Limit DataFusion's tracked execution memory; disk spill is disabled |

These options accept unsigned decimal digits in the platform's `usize` range.
Byte and memory limits must be positive. The memory pool does not cap total
process RSS: Core read buffers, SQL planning, and IPC encoding allocate outside
it. Sorting and grouping can require memory before producing output. There is
currently no total-command deadline.

Exceeding a ceiling fails with status 1 and leaves incomplete output. The CLI
does not turn a query into a successful sample or write an IPC end marker after
failure. As with `scan`, require valid Arrow output and status 0, and discard
partial files after failure. With `--diagnostics=json`, query completion has
`scope: "query"`, each alias and its actual version in `bindings`, and row,
batch, and output-byte counts.
This option controls stderr diagnostics; query results remain Arrow.

## Join table snapshots

Declare the tables in a UTF-8 JSON file, then use their aliases in SQL. For a
`tables.json` beside the `orders/` and `customers/` Delta directories:

```json
{"format_version":1,"tables":[
  {"alias":"orders","location":"./orders","table_version":"42"},
  {"alias":"customers","location":"./customers"}
]}
```

```sql
SELECT o.order_id, c.name
FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id
ORDER BY o.order_id, c.customer_id
```

Save the query as `join.sql` and run:

```sh
dar query --tables-file tables.json --sql-file join.sql --diagnostics=json > joined.arrow
```

`--tables-file` conflicts with positional `TABLE`, `--table-version`,
`--storage-options-file`, `--auth-source`, and `--databricks-profile`.
Manifests accept local paths, direct storage URLs, and Unity Catalog table names.
Command-line file paths use the working directory. Locations inside the manifest
use the manifest's directory.
The complete manifest is validated before any table is opened.

<details markdown="1">
<summary>Tables manifest fields and limits</summary>

| Field | Accepted value |
| --- | --- |
| `format_version` | Required JSON integer `1` |
| `tables` | Required array of 1-64 table objects |
| `tables[].alias` | Unique name matching `[a-z][a-z0-9_]{0,62}` |
| `tables[].location` | Nonempty local path, direct storage URL, or `uc://catalog.schema.table`, without control characters or embedded credentials |
| `tables[].table_version` | Optional decimal string from `"0"` to `"18446744073709551615"`; omit to select latest |
| `tables[].authorization` | Optional object selecting one explicit credential source |
| `tables[].authorization.source` | Required when `authorization` is present; an installed `--auth-source` name for direct storage, or `databricks` for UC |
| `tables[].authorization.storage_options_file` | Local JSON storage-options file reference; required for source `json`, optional for other sources |
| `tables[].authorization.databricks_profile` | Existing Databricks CLI profile name; required for source `databricks`, invalid for other sources |

The file is limited to 1 MiB. Unknown and repeated fields are errors. Versions
must use canonical digits without signs or leading zeros; JSON numbers and
`null` are rejected. The Core version-loading limits described under
[snapshot selection](#inspect-a-snapshot) still apply.

Aliases cannot be `t`, `datafusion`, `public`, `information_schema`, `pg_catalog`,
`system`, or a SQL keyword recognized by the parser. URLs cannot contain a
password, query string, or fragment. Azure Blob's `container@account` URL form
is accepted. Omitting `authorization` uses default authentication for direct
storage; UC entries require source `databricks` and a profile. An explicit
authorization must be an object; `null` is invalid.

</details>

<details markdown="1">
<summary>Per-table authorization and credential file paths</summary>

Each table can select a separate source and storage-options file. For example,
this entry uses S3 credentials from a referenced JSON file:

```json
{"alias":"orders","location":"s3://example-bucket/orders",
 "authorization":{"source":"json","storage_options_file":"./credentials/orders.json"}}
```

The direct-storage sources and their provider restrictions are the same as
[`--auth-source`](#explicit-credential-sources): `json`, `env`, `aws-web-identity`,
`aws-container`, `azure-workload-identity`, and `azure-managed-identity`.
The manifest accepts references only; put credential values in the selected
storage-options file or inherited environment. `env_file` references are not
yet supported.

`storage_options_file` is relative to the manifest directory. Credential file
paths inside that JSON, such as `google_application_credentials` or
`aws_web_identity_token_file`, are relative to the options file's directory.
Absolute paths stay absolute. Paths inherited from environment variables use
the process working directory, as they do for positional table commands.

The CLI reads every referenced options file before loading any table. Each
binding retains its selected provider; changing an options file during the
query cannot switch its identity. Native providers can still renew credentials
and reread rotating identity-token files under their existing rules.

</details>

<details markdown="1">
<summary>Join Unity Catalog, local, and direct-storage tables</summary>

Each UC entry uses an existing Databricks CLI profile. The same
[Unity Catalog permissions and storage options](#read-a-unity-catalog-table)
apply as for positional UC reads. For example:

```json
{"format_version":1,"tables":[
  {"alias":"orders","location":"uc://main.sales.orders",
   "authorization":{"source":"databricks","databricks_profile":"work",
                    "storage_options_file":"./uc-region.json"}},
  {"alias":"customers","location":"./customers"},
  {"alias":"returned_orders","location":"s3://example-bucket/returns",
   "authorization":{"source":"env"}}
]}
```

Save this as `tables.json` and query the declared aliases with `--tables-file`.
UC storage options may contain transport and region settings, such as
`{"aws_region":"eu-west-1"}` for S3. They cannot override the vended credentials.
Profile names must be nonblank and contain no control characters. Different UC
entries may select different profiles, including for the same table name in
different workspaces. Each retains its own table ID, storage root, and provider
during credential renewal. SQL runs locally; it is not sent to Databricks compute.

</details>

Each table loads once and retains that snapshot for the query. Completion
reports the actual version even when latest was requested. These are independent
snapshots and do not form a transaction across tables. For repeatable row
ordering, use `ORDER BY` with columns that resolve ties. Volatile functions and
floating-point aggregation can still produce different values between runs.

## Filter rows

`dar scan --predicate-file PATH` reads a typed JSON predicate from a local
UTF-8 file. The option may appear once; omitting it applies no predicate.
Paths resolve against the working directory, and `-` names a literal file.
The CLI never reads a predicate from stdin.

For a table with an `Int64` column named `id`, this selects IDs greater than
or equal to 10 and returns only `value`:

```sh
cat > predicate.json <<'JSON'
{"op":"ge","column":"id","value":{"type":"int64","value":"10"}}
JSON
dar scan --predicate-file predicate.json --column value /data/orders > filtered.arrow
```

Filter columns need not appear in the output projection. Filtering uses the
native reader and happens before `--limit` counts output rows. For example,
the following file combines a comparison with a null check:

```sh
cat > predicate.json <<'JSON'
{
  "op": "and",
  "args": [
    {"op": "ge", "column": "id", "value": {"type": "int64", "value": "10"}},
    {"op": "is_not_null", "column": "region"}
  ]
}
JSON
dar scan --predicate-file predicate.json --limit 100 /data/orders > sample.arrow
```

<details markdown="1">
<summary id="predicate-objects">Predicate operators and logical combinations</summary>

The file must contain exactly one predicate object. Only the fields shown
for each operator are accepted:

| Operator | Object |
| --- | --- |
| Constant | `{"op":"constant","value":true}` (or `false`) |
| Comparison | `{"op":"eq","column":"id","value":{"type":"int64","value":"10"}}` |
| Null check | `{"op":"is_null","column":"region"}` |
| Non-null check | `{"op":"is_not_null","column":"region"}` |
| AND | `{"op":"and","args":[PREDICATE, ...]}` |
| OR | `{"op":"or","args":[PREDICATE, ...]}` |
| NOT | `{"op":"not","arg":PREDICATE}` |

Comparison operators are `eq` (equal), `ne` (not equal), `lt` (less than),
`le` (less than or equal), `gt` (greater than), and `ge` (greater than or
equal). `PREDICATE` in the table means another predicate object. Empty AND
arguments evaluate to true; empty OR arguments evaluate to false.

Only rows where the predicate evaluates to true are returned. Comparisons
with null values produce unknown results, including under NOT. Use the null
check operators to test for nulls explicitly.

Column names are passed unchanged. They must identify an unambiguous top-level
logical field; dots are not interpreted as nested access. There is no SQL
parser or schema-based scalar type inference.

</details>

<details markdown="1">
<summary id="scalar-objects">Scalar types, validation, and input limits</summary>

Comparison values must be non-null objects with a `type` tag and the fields
listed below. Types must exactly match the column's Arrow type, including
timestamp timezone and decimal precision and scale. No widening or string
coercion is performed.

| `type` | Required fields besides `type` | Native scalar |
| --- | --- | --- |
| `boolean` | `value`: JSON boolean | Boolean |
| `int8`, `int16`, `int32`, `int64` | `value`: canonical integer string fitting the signed width | Int8, Int16, Int32, Int64 |
| `float32` | `value`: finite JSON number, finite after conversion to f32 | Float32 |
| `float64` | `value`: finite JSON number fitting f64 | Float64 |
| `utf8`, `large_utf8` | `value`: JSON string | Utf8, LargeUtf8 |
| `binary`, `large_binary` | `value`: array of integer bytes from 0 through 255; may be empty | Binary, LargeBinary |
| `fixed_size_binary` | `value`: byte array; `size`: positive i32 JSON integer equal to the array length | FixedSizeBinary |
| `date32` | `value`: canonical i32 integer string, days since the Unix epoch | Date32 |
| `timestamp_us` | `value`: canonical i64 integer string, microseconds since the Unix epoch; `timezone`: null or a nonempty string | TimestampMicrosecond |
| `decimal128` | `value`: canonical i128 integer string of the unscaled value; `precision`: u8 JSON integer; `scale`: i8 JSON integer | Decimal128 |

Canonical integer strings are `"0"` or an optional minus sign followed by a
nonzero digit and further digits. `"-0"`, leading zeros, plus signs, whitespace,
fractions, exponents, and overflow are rejected. Integers are parsed directly
at their declared width without passing through floating point.

Bytes, `size`, `precision`, and `scale` use integer JSON tokens, so `1.0` and
`1e0` are invalid for those fields. Float32 conversion may round or underflow
to zero. Dates and timestamps accept negative epoch offsets without calendar
parsing or timezone conversion. `timezone` is required even when null.
Decimals are not rescaled or rounded; negative scales are allowed.

The file limit is 1 MiB (1,048,576 bytes). The tree limit is 1024 predicate
nodes and 32 levels, counting the root as one node at level 1. Duplicate keys,
unknown fields or tags, incorrect shapes or types, unexpected nulls, invalid
UTF-8, and trailing non-whitespace content are rejected before table loading.
These failures produce status 2 with `configuration/invalid_input_json`;
file open or read failures use `configuration/input_file_io`.

Column resolution, scalar/schema compatibility, and decimal precision, scale,
and value compatibility are validated by the core during scan planning.
Failures there produce status 1 with `scan_planning/unsupported_predicate`
and leave stdout empty.

</details>

## Storage options file

`--storage-options-file` accepts a local UTF-8 JSON file containing an object
of string keys and string values. For example, save this as `storage.json`
to select an S3 region while using explicitly selected environment credentials:

```json
{"aws_region":"us-west-2"}
```

Then pass the file when opening the table:

```sh
dar inspect --view summary --auth-source env --storage-options-file storage.json s3://example-bucket/orders
```

Use the same option with `dar scan` or `dar query`. The
[storage guide](configuration.md#read-from-s3) explains table URLs and credentials,
including [Azure Blob](configuration.md#read-from-azure). Use its storage keys as JSON
keys here. Keep files containing credentials out of source control.

Without `--auth-source`, the CLI forwards keys and values unchanged, including
case, spaces, and commas. The core reader interprets provider options. Omitting
the file supplies an empty map; an empty object is also valid. This path does
not automatically read AWS access keys from the environment. Use
`--auth-source env` to select them.

For a local HTTP test endpoint, `{"allow_http":"true"}` enables plain HTTP.

<details markdown="1">
<summary>Storage file parsing and size limits</summary>

The file limit is 1 MiB (1,048,576 bytes), measured before JSON parsing. Reading
stops when that limit is exceeded. Invalid UTF-8, malformed JSON, trailing
non-whitespace content, duplicate keys (including equivalent escaped keys),
and non-string values are rejected before table loading. File paths resolve
against the working directory. A path of `-` names a local file; it does not
read standard input.

</details>

## Explicit credential sources

`--auth-source` selects authorization for direct cloud reads. The same sources
are available to `inspect`, `scan`, and `query`. Help lists the installed
selectors without reading input files or resolving credentials.

| Backend | Source | Credentials | Renewal |
| --- | --- | --- | --- |
| S3 | [`json` or `env`](#s3-credential-sources) | Access-key pair, with an optional session token | Supply new credentials for another command |
| S3 | [`aws-web-identity`](#s3-web-identity) | Role ARN and web identity token file | Refreshes temporary credentials; rereads the token file |
| S3 | [`aws-container`](#s3-container-credentials) | ECS relative URI, or EKS full URI and token file | Refreshes at the selected endpoint; rereads the EKS token file |
| Azure Blob | [`json` or `env`](#azure-credential-sources) | Account key, SAS token, or bearer token | Supply new credentials for another command |
| Azure Blob | [`json` or `env`](#azure-credential-sources) | Client ID, client secret, and tenant ID | Refreshes access tokens for the loaded principal |
| Azure Blob | [`azure-workload-identity`](#azure-workload-identity) | Client ID, tenant ID, and federated token file | Refreshes access tokens; rereads the token file |
| Azure Blob | [`azure-managed-identity`](#azure-managed-identity) | One explicit client, object, or resource ID | Refreshes access tokens for that identity |
| GCS | [`json` or `env`](#gcs-credential-sources) | Explicit service-account or authorized-user ADC file | Renews using the identity loaded from the file |

Local paths do not accept `--auth-source`. Unity Catalog uses
`--databricks-profile` and rejects it. Omitting `--auth-source` preserves the
existing storage-options behavior, including any native provider defaults.
The explicit sources above do not fall back to other credential sources after
configuration, acquisition, renewal, or permission failures.

### Provider limits

The CLI exposes the mechanisms it can select through the installed
`object_store` provider. It does not implement a full cloud SDK credential
chain, AWS profiles or SSO, automatic home-directory ADC discovery, or an
anonymous source selector.

These candidate selectors are not accepted by `--auth-source`:

| Candidate | Why it is not exposed |
| --- | --- |
| `aws-imds` | The native provider queries the instance's current role on every refresh, so it cannot retain an explicitly selected role. |
| `azure-cli` | The native provider follows the current CLI account and invokes a blocking subprocess without a timeout or bounded output. |
| `gcp-metadata` | Native construction checks home-directory ADC first. Its metadata provider selects the default service account and retries a second address after failure. |

Explicit GCS ADC files already use `json` or `env`; there is no separate
`gcp-adc` selector. The sections below list accepted keys, aliases, environment
variables, precedence, and limits for each supported source.

## S3 credential sources

`inspect`, `scan`, and `query` accept `--auth-source json|env` for explicit S3
access-key credentials. The key ID and secret key must both be present; a
session token is optional. The source is loaded once per command. Expired or
denied credentials fail the read without selecting another identity. Supply
updated credentials when starting another command.

`--auth-source json` requires `--storage-options-file`. That file supplies the
credentials and storage configuration; AWS environment credentials are ignored.
For example, provision a protected `storage.json` file with this shape:

```json
{
  "aws_access_key_id": "<access-key-id>",
  "aws_secret_access_key": "<secret-access-key>",
  "aws_region": "eu-west-1"
}
```

```sh
dar inspect s3://example-bucket/orders --view summary --auth-source json --storage-options-file storage.json
```

`--auth-source env` reads only `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_SESSION_TOKEN`, `AWS_REGION`, and `AWS_DEFAULT_REGION`. Provision these in
the command's environment. An optional storage-options file may supply
noncredential settings, but credential fields in that file are rejected.

```sh
dar query s3://example-bucket/orders --auth-source env --sql-file count.sql > count.arrow
```

JSON region settings take precedence over both environment region settings.
Within either source, `aws_region`/`AWS_REGION` takes precedence over
`aws_default_region`/`AWS_DEFAULT_REGION`. Without a region, the native S3
builder uses `us-east-1`; set the bucket's region explicitly for other regions.

JSON keys use the native S3 aliases and are case-insensitive. For example,
`access_key_id` and `AWS_ACCESS_KEY_ID` refer to the same setting, as do
`token`, `session_token`, `aws_token`, and `aws_session_token`. Supplying two
aliases for the same setting is an error, even if their values match.
Credentials must be nonempty and contain no control characters.

The `json` and `env` sources accept region, endpoint, HTTP client, virtual-hosted
request, unsigned-payload, checksum, and requester-pays settings. They reject
unknown keys, bucket overrides, unsigned requests, and provider-selection
settings. They do not select AWS profiles, SSO, web identity, container
credentials, or instance metadata. Unity Catalog uses `--databricks-profile`
and rejects `--auth-source`.

Invalid source configuration exits with status 2 and diagnostic code
`invalid_storage_authorization`. Storage failures retain the reader's normal
diagnostics. Credential values and file contents are omitted from both text
and JSON diagnostics. Keep credential files out of source control and restrict
access, for example with `chmod 600 storage.json` on Unix. A file reference does
not isolate credentials from an agent or process that can read that file.

### S3 web identity

Use `--auth-source aws-web-identity` with `inspect`, `scan`, or `query` to assume
an AWS role using a web identity token file. The role ARN and token file path
are required. Each JSON field takes precedence over its environment variable;
fields absent from JSON are read from the environment.

| Setting | Canonical JSON key | Environment variable |
| --- | --- | --- |
| Role ARN | `aws_role_arn` | `AWS_ROLE_ARN` |
| Token file path | `aws_web_identity_token_file` | `AWS_WEB_IDENTITY_TOKEN_FILE` |
| Session name | `aws_role_session_name` | `AWS_ROLE_SESSION_NAME` |
| STS endpoint | `aws_endpoint_url_sts` | `AWS_ENDPOINT_URL_STS` |

For example, with `AWS_ROLE_ARN`, `AWS_WEB_IDENTITY_TOKEN_FILE`, and `AWS_REGION`
set by the workload environment:

```sh
dar query s3://example-bucket/orders --auth-source aws-web-identity \
  --sql-file count.sql > count.arrow
```

An optional `--storage-options-file` can supply the fields above and the S3
storage settings described earlier. The same region precedence applies: JSON
over environment, then `aws_region` over `aws_default_region`. The default
region is `us-east-1`, the default session name is `WebIdentitySession`, and
the default STS endpoint is `https://sts.<region>.amazonaws.com`. STS requires
HTTPS even when `allow_http` enables HTTP for storage requests.

The command fixes the role, session name, token file path, and endpoint when
it starts. The native provider obtains temporary credentials and rereads that
token file when credentials need refreshing. Your workload must provision and
rotate the token file. Changing the options file during a command does not
change the selected identity.

Missing references, duplicate aliases, empty values, and control characters
are rejected. JSON access keys and alternative provider settings are also
rejected. Ambient access keys, profiles, SSO, container credentials, and
instance metadata are not selected. An unreadable token file, failed STS
request, or storage permission denial fails the read without switching
identities. This source supports S3 tables only.

### S3 container credentials

Use `--auth-source aws-container` with `inspect`, `scan`, or `query` for
[ECS task roles or EKS Pod Identity](https://docs.aws.amazon.com/sdkref/latest/guide/feature-container-credentials.html).
Configure exactly one of these references:

- ECS: a relative credentials URI. It is appended to `http://169.254.170.2`.
- EKS: a full credentials URI and an authorization token file path.

| Setting | Canonical JSON key | Environment variable |
| --- | --- | --- |
| Relative URI | `aws_container_credentials_relative_uri` | `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` |
| Full URI | `aws_container_credentials_full_uri` | `AWS_CONTAINER_CREDENTIALS_FULL_URI` |
| Token file path | `aws_container_authorization_token_file` | `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` |

For example, when the container runtime supplies the environment reference:

```sh
dar query s3://example-bucket/orders --auth-source aws-container \
  --sql-file count.sql > count.arrow
```

If `--storage-options-file` contains any container reference field, it must
contain the complete reference. All container environment references are then
ignored. Otherwise, the command reads the three environment variables above.
It rejects mixed ECS/EKS references and incomplete EKS references, including
a full URI without a token file. Region and other S3 storage settings follow
the rules described earlier.

Relative URIs must begin with a single `/` and contain no backslashes. Full
URIs must use HTTP or HTTPS. Both forms may include a query string; embedded
user credentials and URL fragments are rejected. The native provider allows
HTTP for container credential endpoints independently of storage `allow_http`.

The endpoint and token file path are fixed when the command starts. The native
provider refreshes temporary credentials from that endpoint. For EKS, it
rereads the token file and sends its contents in the `Authorization` header;
the container runtime must provision and rotate this file. The CLI does not
use `AWS_CONTAINER_AUTHORIZATION_TOKEN` or support a full URI without a token
file. These limits reflect the installed provider's supported mechanisms.

Unreadable token files, failed credential requests, malformed responses, and
storage permission denials fail the read. The command does not switch to
ambient access keys, web identity, profiles, or instance metadata. JSON fields
selecting those other sources are rejected. This source supports S3 only.

## Azure credential sources

`inspect`, `scan`, and `query` accept `--auth-source json|env` for Azure account
keys, SAS tokens, bearer tokens, and service principals using a client secret.
Select exactly one credential family. A service principal requires all three
fields: client ID, client secret, and tenant ID. Partial or mixed families are
rejected before reading the table.

The identity is loaded once per command. Client-secret authorization uses the
native Azure provider to obtain and refresh access tokens for that identity.
An authorization or refresh failure fails the read without switching identities.
Account keys, SAS tokens, and supplied bearer tokens are not refreshed.

| Credential | Canonical JSON key | Environment variable |
| --- | --- | --- |
| Account key | `azure_storage_account_key` | `AZURE_STORAGE_ACCOUNT_KEY` |
| SAS token | `azure_storage_sas_key` | `AZURE_STORAGE_SAS_KEY` |
| Bearer token | `azure_storage_token` | `AZURE_STORAGE_TOKEN` |
| Client ID | `azure_storage_client_id` | `AZURE_CLIENT_ID` |
| Client secret | `azure_storage_client_secret` | `AZURE_CLIENT_SECRET` |
| Tenant ID | `azure_storage_tenant_id` | `AZURE_TENANT_ID` |

`--auth-source json` requires `--storage-options-file` and ignores environment
credentials. For example, provision a protected `storage.json` file:

```json
{
  "azure_storage_account_name": "exampleaccount",
  "azure_storage_sas_key": "<sas-token>"
}
```

```sh
dar inspect az://orders/table --view summary --auth-source json --storage-options-file storage.json
```

For a service principal, replace the SAS field with the complete client-secret
family:

```json
{
  "azure_storage_account_name": "exampleaccount",
  "azure_storage_client_id": "<application-id>",
  "azure_storage_client_secret": "<client-secret>",
  "azure_storage_tenant_id": "<tenant-id>"
}
```

`--auth-source env` reads only the six credential variables above and
`AZURE_STORAGE_ACCOUNT_NAME`. A storage-options file may supply noncredential
settings, but credential fields in that file are rejected.

```sh
dar query abfss://orders@exampleaccount.dfs.core.windows.net/table \
  --auth-source env --sql-file count.sql > count.arrow
```

An account embedded in an `abfss://container@account...` or native HTTPS storage
URL takes precedence, as in the native Azure builder. Otherwise, supply
`azure_storage_account_name` in JSON or, with `--auth-source env`,
`AZURE_STORAGE_ACCOUNT_NAME`. JSON takes precedence over the environment.

JSON accepts the native Azure aliases, case-insensitively. Examples include
`access_key`/`account_key`, `sas_key`/`sas_token`, `bearer_token`/`token`,
`client_id`, `client_secret`, and `tenant_id`/`authority_id`.
Duplicate aliases are rejected, even with identical values. Account keys must
be base64-encoded. SAS tokens may start with `?` and must include a nonempty
`sig`; literal and percent-encoded `+` characters are preserved. Bearer tokens
must contain only visible ASCII characters.

Explicit selection accepts account, endpoint, HTTP client, and Fabric endpoint
settings. Client-secret authorization uses the native provider's public Azure
authority by default; set `azure_storage_authority_host` in JSON to override it.
The `authority_host` alias is also accepted. The `json` and `env` sources do not
read `AZURE_AUTHORITY_HOST`.

Unknown keys, container overrides, emulator mode, unsigned requests, and
provider-selection options are rejected. Workload and managed identities require
their own source selectors, described below. Azure CLI is not offered by
`--auth-source`. Invalid source configuration exits with status 2 and code
`invalid_storage_authorization`.

Keep credential files out of source control and restrict access, for example
with `chmod 600 storage.json` on Unix. File references do not isolate credentials
from a process that can read the file. Credential values and file contents are
omitted from text and JSON diagnostics.

### Azure workload identity

Use `--auth-source azure-workload-identity` with an Azure service principal
configured to trust the issuer of a federated token file. It is available for
tables in Azure Blob Storage with `inspect`, `scan`, and `query`.

```sh
dar query abfss://orders@exampleaccount.dfs.core.windows.net/table \
  --auth-source azure-workload-identity --sql-file count.sql > count.arrow
```

Provision all three required references in the environment or a protected
storage-options file:

| Reference | Canonical JSON key | Environment variable |
| --- | --- | --- |
| Client ID | `azure_storage_client_id` | `AZURE_CLIENT_ID` |
| Tenant ID | `azure_storage_tenant_id` | `AZURE_TENANT_ID` |
| Token file path | `azure_federated_token_file` | `AZURE_FEDERATED_TOKEN_FILE` |

JSON accepts native aliases such as `client_id`, `tenant_id`, and
`federated_token_file`. Each JSON field takes precedence over its environment
counterpart. Missing, empty, and duplicate references are rejected before
reading the table. Account names follow the same URL, JSON, and environment
precedence as the other Azure sources.

The optional `azure_storage_authority_host` JSON setting takes precedence over
`AZURE_AUTHORITY_HOST`. If both are absent, the native provider uses the public
Azure authority. Endpoint and HTTP client settings remain in the storage-options
file.

The native provider fixes the client ID, tenant ID, authority, and token file
path for the command. When an access token needs renewal, it rereads the same
file, so the workload issuer can rotate its contents. Unreadable files, rejected
assertions, and refresh failures fail the read without selecting another
identity. The CLI does not create the file or manage its rotation.

Other credential families in JSON are rejected. Ambient account keys, SAS
tokens, bearer tokens, and client secrets are ignored by this source. Token
contents are not passed in command arguments or included in diagnostics.

### Azure managed identity

Use `--auth-source azure-managed-identity` on a host that provides Azure managed
identity tokens. Select exactly one identity by client ID, object ID, or managed
identity resource ID. This source requires an explicit ID; it does not select
the host's default identity when the ID is missing or rejected.

| Identity selector | Canonical JSON key | Environment variable |
| --- | --- | --- |
| Client ID | `azure_storage_client_id` | `AZURE_CLIENT_ID` |
| Object ID | `azure_object_id` | `AZURE_OBJECT_ID` |
| Resource ID | `azure_msi_resource_id` | `AZURE_MSI_RESOURCE_ID` |

JSON accepts native aliases such as `client_id`, `object_id`, and
`msi_resource_id`. One selector in JSON takes precedence over all three
environment selectors. Without a JSON selector, set exactly one of the listed
environment variables. Missing IDs, empty values, duplicate aliases, and
multiple selectors fail before table access.

For example, provision a `storage.json` file containing the selected client ID:

```json
{
  "azure_storage_client_id": "<managed-identity-client-id>"
}
```

```sh
dar inspect abfss://orders@exampleaccount.dfs.core.windows.net/table --view summary \
  --auth-source azure-managed-identity --storage-options-file storage.json
```

The source also works with `scan` and `query`. Account names follow the same
URL, JSON, and environment precedence as the other Azure sources.

The token endpoint comes from `azure_msi_endpoint` in JSON, then
`IDENTITY_ENDPOINT`, then the native Azure IMDS endpoint. JSON aliases include
`msi_endpoint` and `identity_endpoint`. If the host supplies `IDENTITY_HEADER`,
the native provider sends it as `x-identity-header` on token requests only.
That secret stays in the environment; it is not a JSON field or command argument.
`AZURE_MSI_ENDPOINT` is not read by this source.

The native provider obtains and refreshes tokens with the same identity
selector and endpoint throughout the command. Token acquisition, refresh, and
storage permission failures stop the read without selecting another identity.
Other credential families in JSON are rejected; ambient storage credentials,
client secrets, workload tokens, and Azure CLI sessions are ignored.

## GCS credential sources

`inspect`, `scan`, and `query` accept `--auth-source json|env` to select an
explicit Application Default Credentials (ADC) file for GCS. The installed
native provider supports these file types:

| ADC `type` | Authorization |
| --- | --- |
| `service_account` | Signs JWT bearer tokens with the file's RSA private key |
| `authorized_user` | Exchanges the file's refresh token for OAuth access tokens |

`--auth-source json` requires `--storage-options-file`. Use that file to point
to the separately provisioned credentials file:

```json
{"google_application_credentials":"/run/secrets/gcs-credentials.json"}
```

```sh
dar inspect gs://example-bucket/orders --view summary --auth-source json --storage-options-file storage.json
```

`--auth-source env` reads only `GOOGLE_APPLICATION_CREDENTIALS`, which must
contain a nonempty file path. An optional storage-options file may contain
noncredential settings, but an ADC reference in that file is rejected.

```sh
dar query gs://example-bucket/orders --auth-source env --sql-file count.sql > count.arrow
```

Relative credential paths use the command's working directory. The provider
loads the selected file once and retains that identity for token renewal.
Changing the file during a command does not change the identity. Missing,
invalid, expired, or denied credentials never trigger home-directory ADC
lookup, a metadata request, or selection of another source. Authorized-user
token renewal requires access to Google's OAuth endpoint.

JSON keys are case-insensitive. `application_credentials` is an alias for
`google_application_credentials`; supplying both is an error. Storage endpoint
(`google_base_url`/`base_url`) and HTTP client settings are accepted. Bucket
overrides, unsigned requests, and unknown keys are rejected. Explicit source
selection uses ADC files; it rejects the legacy `google_service_account` and
`google_service_account_key` settings. Standard service-account files with
`"type":"service_account"` use the ADC reference shown above. Other ADC types,
including external-account and impersonated-service-account files, are not
supported by the installed provider.

Invalid source configuration or an unreadable or malformed ADC file exits with
status 2 and code `invalid_storage_authorization`. Token-fetch, renewal, and
storage failures retain the reader's normal diagnostics. Credential values and
file contents are omitted from text and JSON diagnostics. Protect both files
with appropriate filesystem permissions and keep credentials out of source
control. A file reference does not prevent a process with read access from
reading the credentials.

## Execution options

Omit these options to use the reader's defaults. `--target-partitions` changes
how many groups of files can work in parallel. `--execution-options-file` supplies
a JSON object for file-read limits, buffering, and backend selection.

See [Configuration](configuration.md) to understand the tradeoffs. The CLI's
accepted fields and validation rules are listed below.

<details markdown="1">
<summary>Execution flags, complete JSON example, and accepted fields</summary>

`--target-partitions N` sets the execution partition target for one scan. It
may appear once and accepts positive unsigned decimal digits fitting the
executable's `usize` range. Zero, signs, whitespace, fractions, exponents, and
overflow are rejected before table loading with status 2 and
`configuration/invalid_cli_argument`. Leading zeros are accepted.

Omit the flag to keep the core's
[automatic partition selection](scan-planning.md#choose-a-partition-target).
The built scan may have fewer partitions than requested, for example when
there are fewer files. This flag does not change the table's partition columns.
`inspect` rejects it.

`dar scan --execution-options-file PATH` reads execution settings from a local
UTF-8 JSON object. The option may appear once and is accepted only by `scan`.
Omitting the file or supplying `{}` uses the native reader's defaults. Each
invocation starts with fresh settings.

```sh
cat > execution.json <<'JSON'
{
  "parquet_backend": "direct",
  "max_concurrent_file_reads_per_scan": 1,
  "max_concurrent_file_reads_per_partition": 1,
  "output_buffer_batches_per_partition": 1,
  "prefetch_files_per_partition": 0,
  "parquet_metadata_size_hint_bytes": null,
  "parquet_full_file_read_threshold_bytes": 1048576
}
JSON
dar scan --target-partitions 2 --execution-options-file execution.json \
  --limit 100 /data/orders > sample.arrow
```

Only these fields are accepted. Omitted fields retain their current native
defaults:

| Field | Accepted value | Current default |
| --- | --- | --- |
| `parquet_backend` | `"direct"` or `"delta_kernel"` | `"direct"` |
| `max_concurrent_file_reads_per_scan` | Positive capacity or null | null, deriving capacity from the partition target and per-partition limit |
| `max_concurrent_file_reads_per_partition` | Positive capacity | `3` |
| `output_buffer_batches_per_partition` | Positive capacity | `1` |
| `prefetch_files_per_partition` | Nonnegative integer | `2` |
| `parquet_metadata_size_hint_bytes` | Positive integer or null | `65536` |
| `parquet_full_file_read_threshold_bytes` | Positive integer or null | null, disabling full-file buffering |

Numeric values must be JSON integer tokens fitting the executable's `usize`
range. Booleans, strings, negative values, fractions, and exponent forms such
as `1e0` are rejected. The three capacity fields are also capped at
`tokio::sync::Semaphore::MAX_PERMITS` (`2305843009213693951` on 64-bit builds).
This capacity bound does not apply to byte sizes or prefetch counts.

Explicit null is accepted only for the three nullable fields in the table.
For `parquet_metadata_size_hint_bytes`, omission keeps the default hint while
null disables it. Null on `max_concurrent_file_reads_per_scan` selects derived
capacity; null on `parquet_full_file_read_threshold_bytes` disables full-file
buffering. Zero is accepted only for `prefetch_files_per_partition`, where it
disables preparation of future files.

Both backends use the concurrency and output-buffer limits. Prefetch, metadata
hints, and full-file buffering apply to the direct backend. See the
[execution options reference](reference/execution-options.md) for their effects.
Execution settings compose with predicates, projection, and row limits.

File paths resolve against the working directory; `-` names a literal file.
The limit is 1 MiB (1,048,576 bytes). Invalid UTF-8, duplicate or unknown keys,
non-object roots, wrong types, and trailing non-whitespace content are rejected
before table loading. These failures use status 2 with
`configuration/invalid_input_json`; file access failures use
`configuration/input_file_io`.

The native setters validate positive values and capacity limits before table
loading. Their failures also exit 2, preserving the core's redacted
`configuration/invalid_configuration` diagnostic and leaving stdout empty.

</details>

## Inspection JSON

A successful inspection writes one UTF-8 JSON object followed by a newline to
stdout. Diagnostic logging uses stderr and does not change this JSON.
With `--view summary`, the default result is:

```json
{"format_version":1,"kind":"inspection_summary","table_version":"0","checks":{"scan_protocol":"passed","data_files":"not_checked"}}
```

`scan_protocol` is `passed` or `failed` according to Core's reader-protocol
validation for the loaded snapshot.
`data_files` is always `not_checked`: neither Parquet nor deletion-vector access
was tested. These checks do not establish authorization for a subsequent scan.

With `--column id`, `fields` replaces `checks`:

```json
{"format_version":1,"kind":"inspection_summary","table_version":"0","fields":[{"name":"id","data_type":"Int32","nullable":true}]}
```

Types use Arrow's Serde encoding. Nested fields use the same reduced fields;
`dict_is_ordered` appears only for dictionary fields. Dictionary IDs are omitted.
Metadata is absent unless explicitly requested, and empty metadata is always
omitted. This selected representation is not a serialized `arrow_schema::Schema`.
With `--column-prefix i`, `column_names: ["id"]` replaces `checks` instead.
Names, timezone text, and metadata are untrusted table content.

The legacy full view represents an empty schema as:

```json
{"format_version":1,"table_version":"0","schema":{"fields":[],"metadata":{}}}
```

`table_version` is a decimal string so JSON consumers preserve the full u64
range. `schema` uses the upstream `arrow_schema::Schema` Serde representation
at the locked workspace Arrow version (currently 58.4.0). It retains field
order, nested types, nullability, field metadata, and schema metadata.

<details markdown="1">
<summary>Field encodings and format compatibility</summary>

For example, a nullable string field is encoded as:

```json
{
  "name": "city",
  "data_type": "Utf8",
  "nullable": true,
  "dict_id": 0,
  "dict_is_ordered": false,
  "metadata": {}
}
```

Parameterized types use Serde's enum encoding, such as
`{"Decimal128":[20,4]}` or `{"Struct":[...]}`. Rust consumers using the matching
Arrow version with its `serde` feature can deserialize the `schema` value
directly into `arrow_schema::Schema`. JSON object key order is not significant.
Incompatible changes to the envelope or schema encoding require a new
`format_version`.

</details>

The envelope does not include table locations or storage options. Requested
schema metadata is returned as table data, including any sensitive strings
the table author put there.

## Errors and process status

The CLI normally shows warnings and errors in the terminal. Data goes to
standard output (`stdout`), while log messages go to standard error (`stderr`),
so you can save them separately.

A failed command can leave an incomplete result file. Check the command's exit
status before using the result: zero means success, and a nonzero status means
failure. Use `--help` if the command reports an invalid argument.

For scripts and agents, add the global option `--diagnostics=json` before or
after the command:

```sh
dar inspect /path/to/table --view summary --diagnostics=json > snapshot.json 2> diagnostics.jsonl
```

Place the option before `--`, which marks the end of options. Stderr contains
one versioned JSON terminal record, including argument errors
or successful completion. It remains enabled with `RUST_LOG=off`; stdout and
exit statuses are unchanged. See the
[machine diagnostics reference](reference/cli-diagnostics.md#machine-diagnostics)
for fields and completion checks.

### Enable more detail

In the default human mode, set `RUST_LOG=debug` before your command to show more detail:

```sh
RUST_LOG=debug dar inspect /path/to/table --view summary
```

To save that detail in `dar.log`, use `2>`:

```sh
RUST_LOG=debug dar inspect /path/to/table --view summary > snapshot.json 2> dar.log
RUST_LOG=debug dar scan --limit 10 /path/to/table > rows.arrow 2> dar.log
```

`snapshot.json` or `rows.arrow` contains the result; `dar.log` contains the log.
Use `info` for progress messages, `warn` for the default verbosity, `error` for
failures only, or `trace` for the most detail. `off` suppresses logs while
preserving the exit status.

Changing the log level does not change the result format. Only reader and CLI
diagnostics are enabled, even at `trace`. Use shell redirection to save logs;
the CLI has no separate file sink. Machine mode suppresses these logs.

### Exit statuses for scripts

Use the exact status when a script needs to distinguish failures:

| Status | Meaning |
| --- | --- |
| 0 | Complete success, including help and version |
| 2 | Invalid CLI arguments or local input configuration before loading |
| 1 | Runtime, reader, SQL, resource-limit, or Arrow IPC encoding failure |
| 3 | stdout write or flush failure, including a broken pipe |

For target-specific filters, diagnostic fields, or process lifecycle rules,
see the [CLI diagnostics reference](reference/cli-diagnostics.md).

## Installation alternatives

[Installation](installation.md#cli-binaries-and-container-images) lists the
available packages. Use these details when choosing a runtime or embedding the
command in another application.

<details markdown="1">
<summary id="use-the-cli-from-a-python-wheel">Use the CLI from a Python wheel</summary>

The `delta-arrow-reader` Python package includes a native command with the same
name. Install it from PyPI with pip to get the Python API and CLI in one
environment, or use uv tool/uvx for a separate tool environment. Follow the
[Python installation instructions](https://mag1cfrog.github.io/delta-arrow-reader/installation/#python)
for installation commands.

The wheels support Linux x86_64 with glibc 2.28 or newer, Windows x86_64, and
macOS arm64 with macOS 12.0 or newer. They require CPython 3.10 or newer and
install PyArrow even when you only need the CLI. The installation guide lists
the tested Python versions and runtime requirements. No Rust toolchain is needed
to install a wheel.

Use the full command name for wheel installations:

```sh
delta-arrow-reader --version
delta-arrow-reader inspect /data/orders --view summary
delta-arrow-reader scan --column id --limit 100 /data/orders > sample.arrow
```

The reference examples use the standalone name `dar`. Substitute
`delta-arrow-reader` when using a wheel. Both commands accept the same arguments
and have the same output and exit statuses; wheels do not install a `dar` alias.
The Linux archives and containers below have their own platform and runtime
requirements.

</details>

<details markdown="1">
<summary id="use-a-linux-binary">Use a Linux binary</summary>

CLI releases provide validated archives for these platforms:

| Platform | Rust target | Minimum runtime |
| --- | --- | --- |
| Linux amd64 | `x86_64-unknown-linux-gnu` | glibc 2.36, baseline x86-64 CPU |
| Linux arm64 | `aarch64-unknown-linux-gnu` | glibc 2.36, baseline ARMv8-A CPU |

Both targets are built on Debian 12. The same binaries are tested on Debian 12,
Ubuntu 24.04, and Fedora 44. They use glibc and libgcc shared libraries. On Debian
or Ubuntu, install `libc6`, `libgcc-s1`, and `ca-certificates`; on Fedora, install
`glibc`, `libgcc`, and `ca-certificates`. The certificate bundle is needed for
HTTPS storage. The archives do not support Alpine Linux's musl runtime or a
`scratch` container. Other platforms can build from source.

Follow the [version-pinned installation and checksum steps](installation.md#cli-binaries-and-container-images)
to download the two archives, `cli-artifacts.json`, and `SHA256SUMS` from a
completed GitHub Release. Extract the archive for your CPU after verification.
Keep `LICENSE` and `NOTICE` alongside `dar`, including when distributing it with
a service. The executable works outside a source checkout and does not require
Python or Rust.

</details>

<details markdown="1">
<summary id="use-a-container-or-a-go-service">Use a container or a Go service</summary>

The CLI container packages the same standalone binary with its runtime libraries
and CA certificates. Copy the index digest from the completed Release's CLI
section and replace `RELEASE_INDEX_HASH` below. Docker selects the matching
architecture from the index:

```sh
dar_image='ghcr.io/mag1cfrog/delta-arrow-reader@sha256:RELEASE_INDEX_HASH'
docker run --rm --read-only "$dar_image" --version
docker run --rm --read-only \
  --mount type=bind,src=/absolute/path/to/table,dst=/table,readonly \
  "$dar_image" inspect /table --view summary
docker run --rm --read-only \
  --mount type=bind,src=/absolute/path/to/table,dst=/table,readonly \
  "$dar_image" scan --column id --limit 100 /table > sample.arrow
```

The image runs `dar` directly as UID/GID `65532:65532`. Omitting arguments prints
help. Each invocation exits when the command finishes; the image does not run a
Reader server. Mounted files and their parent directories must be readable by that user.
Supply storage credentials and configuration at runtime. Keep HTTPS certificate
verification enabled and provide a readable custom trust bundle when required
by your storage endpoint. See [HTTPS configuration](installation.md#https-and-build-prerequisites).

When `dar` is the container's PID 1, SIGINT and SIGTERM stop it with exit status
130 and 143, respectively. The CLI does not flush buffered output or finish an
IPC stream after either signal. Treat that result as an interrupted scan.

A Go service can invoke the binary with `exec.CommandContext` and decode stdout
with the official Arrow Go streaming IPC reader. Pass each CLI argument
separately, drain stderr separately, and require both successful IPC decoding and
a zero child exit status. An early stop or decoding failure must cancel, close,
and reap the child before returning.

The runnable [Go example](https://github.com/mag1cfrog/delta-arrow-reader/tree/main/examples/go-cli)
includes bounded stderr capture, cancellation tests, and a consumer Dockerfile.
Its `COPY --from` stages copy `/usr/local/bin/dar` and `/usr/share/doc/dar/` from
an explicit CLI image into a Debian 12 service image. The service still needs
glibc 2.36 or newer, the binary manifest's shared libraries, and CA certificates.
Copying the executable alone does not support Alpine or `scratch`. The Go
application needs no cgo or Rust toolchain at runtime.

</details>

<details markdown="1">
<summary id="build-from-source">Build from source</summary>

Install the [Rust build prerequisites](installation.md#https-and-build-prerequisites),
then run these commands from a checkout containing the CLI crate:

```sh
cargo build --locked -p delta-arrow-reader-cli
./target/debug/dar --version
./target/debug/dar --help
```

To install the binary into Cargo's binary directory, usually `~/.cargo/bin`:

```sh
cargo install --locked --path crates/delta-arrow-reader-cli
dar inspect --help
```

The CLI package is built from source and is not published to crates.io. It
shares the workspace version, and `dar --version` prints `dar <version>` plus
a newline. Help and version work outside the checkout without opening files,
loading a table, starting a runtime, or contacting storage.

If the executable is named `delta-arrow-reader` (`delta-arrow-reader.exe` on
Windows), help and version display `delta-arrow-reader`. Both names accept the
same arguments and produce the same inspection JSON, Arrow streams, diagnostics,
and exit statuses.

`-h` is short for `--help`, and `-V` is short for `--version`. For inspection
help, use `dar inspect --help` or `dar help inspect`. Scan help is available
through `dar scan --help` or `dar help scan`. For SQL, use `dar query --help`
or `dar help query`.

</details>
