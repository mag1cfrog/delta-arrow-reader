# Command-line reference

For a first read, start with the [CLI quickstart](cli-quickstart.md). For a task
example, use [Filter, group, and join](cli-workflows.md),
[Save and read results](cli-results.md), or [Connect to storage](cli-storage.md).

The examples use `dar`. If you installed the Python wheel, substitute
`delta-arrow-reader`; both names accept the same arguments. See
[Installation](installation.md) for packages and runtime requirements.

## Commands

| Task | Command |
| --- | --- |
| Find a version or column | [inspect](#inspect-a-snapshot) |
| Look at a few rows | [preview](#preview-rows) |
| Filter, group, sort, or join with SQL | [query](#query-a-snapshot) |
| Read selected rows as an Arrow stream | [scan](#scan-rows) |
| Examine a query plan | [explain](#plan-a-query) |
| Handle a failed command | [Errors and exit statuses](#errors-and-process-status) |

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
Names and types are never truncated. Summary inspection has a 30000 ms total
deadline, adjustable with `--deadline-ms N`.

Omitting `--view`, or choosing `--view full`, preserves the complete Arrow schema
for existing consumers. Full inspection has no output ceiling; redirect it to a
file and set `--deadline-ms N` for unattended use. Full inspection has no default
deadline. Selection, metadata, and byte-budget options require the summary view.

The command accepts exactly one UTF-8 local path or URL. Relative paths use
the process working directory. Quote paths and option values that contain
spaces; each must arrive as one command-line argument. `--` ends option
parsing and allows a table name beginning with `-`.

```text
dar inspect [--table-version N] [--storage-options-file PATH]
            [--auth-source SOURCE | --env-file PATH | --databricks-profile PROFILE]
            [--view full|summary]
            [--column NAME ... | --column-prefix PREFIX]
            [--include-metadata] [--max-output-bytes N] [--deadline-ms N] [--] TABLE
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

## Preview rows

Choose the columns and sample size needed for the task:

```sh
dar --diagnostics=json preview /data/orders --table-version 42 --column id --limit 3
```

Keep your chosen version and known column name. Use
[inspection](#inspect-a-snapshot) only to discover either value when it is missing.
Preview uses scan's projection, predicate, snapshot, execution options, and
storage authorization, including Databricks profiles. Row order is unspecified.
It returns [typed JSON](reference/cli-output.md#read-a-small-result-as-typed-json) and can write to a terminal.

| Limit | Default | Override |
| --- | --- | --- |
| Rows | 20 | `--limit N`; zero returns only the selected schema. |
| Whole response | 8 KiB | `--max-output-bytes N`; includes schema, JSON framing, and newline. |
| Command time | 30 seconds | `--deadline-ms N`; includes inputs, authorization, reads, stdout, and completion. |

The byte and time overrides require positive unsigned decimal integers.
Select fewer columns or rows before increasing a budget. A preview always
reports `completion: "sample"`, including an empty or exhausted result:

| `stop_reason` | `has_more` | Meaning |
| --- | --- | --- |
| `row_limit` | null | Reached the requested row count without looking ahead. |
| `byte_limit` | true | An observed row was omitted to fit the response budget. |
| `end_of_input` | false | Observed exhaustion before reaching either limit. |

The schema or first row must fit; otherwise stdout stays empty and the command
fails with `preview_schema_limit_exceeded` or `preview_row_too_large`. A later
byte overflow returns only whole rows. If the required first row cannot fit,
explicitly increase `--max-output-bytes` while keeping the projection, predicate,
row limit, authorization, snapshot, and deadline. To save an Arrow sample
instead, use `scan` with redirected stdout, the same read inputs and an explicit
`--deadline-ms N`; scan has no byte ceiling or default deadline.

After a deadline or cancellation, cleanup has at most one additional second,
and failure reporting has at most one more. A blocked consumer can leave partial
stdout; use the result only after exit status 0 and, with `--diagnostics=json`,
a matching completion record. A deadline exits 1; closed output exits 3.

## Query a snapshot

`query` runs SQL locally through the Core DataFusion table provider. It opens
one Delta snapshot as `t` when given `TABLE`, or registers the aliases in a
[tables manifest](reference/cli-inputs.md#join-table-snapshots). Positional `TABLE` accepts a local path,
direct storage URL, or `uc://catalog.schema.table` with `--databricks-profile`.
UC queries use the same [authorization prerequisites](unity-catalog.md#before-you-start)
and credential renewal as scans.

```sh
cat > query.sql <<'SQL'
SELECT customer_id, count(*) AS orders, sum(total) AS total
FROM t
GROUP BY customer_id
ORDER BY total DESC
LIMIT 100
SQL
dar query /data/orders --table-version 0 --sql-file query.sql --format json
```

```text
dar query [--table-version N] [--storage-options-file PATH]
          [--auth-source SOURCE | --env-file PATH | --databricks-profile PROFILE]
          [--format arrow|json] [--output PATH [--manifest]]
          --sql-file PATH [--max-rows N] [--max-output-bytes BYTES]
          [--memory-limit-bytes BYTES] [--spill-limit-bytes BYTES --spill-dir PATH]
          [--deadline-ms N] [--] TABLE
```

The SQL file must contain one read-only query in UTF-8, at most 64 KiB, with
a parser and expression-depth limit of 128, including chains of set operations
such as `UNION`. Paths use the working directory; `-` names a
literal file. SQL is not read from stdin. Malformed input is rejected before
opening any table. Projections, predicates, joins, aggregations, grouping, ordering,
subqueries, and nonrecursive CTEs are supported. DDL, DML, COPY, session changes,
EXPLAIN, recursive CTEs, and table functions are rejected. SQL cannot discover
additional tables from file paths or URLs.

By default, results use Arrow IPC stream framing on stdout, which must be a
file or pipe. `--format json` returns bounded typed JSON and permits terminal
output. Both formats return the complete SQL result, including empty results and
`COUNT(*)`. Use SQL `LIMIT` to select a smaller result. Positional table queries use the same
snapshot selection and storage options as `inspect`. All queries skip table warmup.

| Option | Default | Behavior |
| --- | --- | --- |
| `--max-rows` | `1000000` | Fail if the SQL result exceeds this many rows; zero is valid |
| `--max-output-bytes` | Arrow: `1073741824` (1 GiB); JSON stdout: `8192` (8 KiB); JSON file: `16777216` (16 MiB) | Fail if the complete result, including schema, framing, and final newline for JSON, exceeds this many bytes |
| `--memory-limit-bytes` | `268435456` (256 MiB) | Limit DataFusion's tracked execution memory |
| `--spill-limit-bytes` | `0` | DataFusion spill budget; a positive value requires `--spill-dir` |
| `--spill-dir` | Unset | Existing writable directory for query temporary files; requires a positive spill budget |
| `--deadline-ms` | `300000` (5 minutes) | Limit total time, including input files, authorization, planning, execution, stdout, and completion |

Row, output-byte, memory, and deadline options accept unsigned decimal digits
in the platform's `usize` range. The spill budget accepts a `u64` decimal value.
Output-byte, memory, and time limits must be positive. The memory pool does not cap total
process RSS: Core read buffers, SQL planning, and output encoding allocate outside
it. Sorting and grouping can require memory before producing output.

<details markdown="1">
<summary id="sql-spill">Enable bounded disk spill</summary>

Spill is disabled unless both a positive budget and an existing directory are
provided. To allow intermediate query data in a private directory:

```sh
dar query /data/orders --table-version 0 --sql-file query.sql --format json \
  --spill-limit-bytes 67108864 --spill-dir /private/query-scratch
```

The CLI creates a child directory for this invocation and attempts to remove it
after completion or handled failure/cancellation. A cleanup error fails the
command and can leave temporary files behind. The parent directory and its
existing contents remain. Files can contain table data; on Windows, use a
directory with a private inherited ACL. Crashes or uninterruptible IO can also
leave temporary files behind.

DataFusion checks disk usage after writes. Usage can temporarily exceed the
budget before the command fails; this is not an operating-system disk quota.
Spilling does not cap process memory or remove output and deadline limits.

</details>

Exceeding a ceiling fails with status 1. JSON stdout stays buffered until the full
result fits; failures before publication leave it empty. A later cleanup or
completion-reporting failure can leave complete JSON with a nonzero exit status;
discard that stdout result. File output streams
both formats into temporary storage. Arrow stdout may leave partial
output without an IPC end marker. Discard incomplete stdout after failure; for
file output, follow the [publication checks](reference/cli-output.md#save-a-result-with-a-manifest).
For stdout results with `--diagnostics=json`, query completion identifies
the command, each alias and its actual snapshot version, and row and output-byte counts.
This option controls stderr diagnostics independently of the result format.
Arrow keeps completion v1. JSON uses v2, whose completion kind marks terminal
success for the requested query. Check `completion: "complete"` and `scope: "query"`
in stdout, then read the values needed by the task.

## Scan rows

```sh
dar scan /data/orders --deadline-ms 30000 > orders.arrow
dar scan --column customer_id --column total --limit 100 /data/orders > sample.arrow
dar scan --no-columns /data/orders > row-counts.arrow
dar scan --table-version 0 /data/orders > original.arrow
```

The output uses Arrow IPC **stream** framing. Open it with a streaming IPC
reader. Without `--output`, stdout must be a file or pipe; a terminal is rejected with status 2
before table loading, including for `--limit 0`.

```text
dar scan [--table-version N] [--storage-options-file PATH]
         [--auth-source SOURCE | --env-file PATH | --databricks-profile PROFILE]
         [--predicate-file PATH] [--column NAME ... | --no-columns]
         [--execution-options-file PATH] [--target-partitions N] [--limit N]
         [--output PATH [--manifest] [--max-rows N] [--max-output-bytes BYTES]] [--deadline-ms N] [--] TABLE
```

Table paths, snapshot selection, and storage options follow the inspection
rules. Each invocation reads one immutable snapshot.
Stdout scans have no default deadline. File scans default to 300000 ms,
1000000 rows and 1 GiB. `--max-rows` and `--max-output-bytes` require `--output`;
exceeding either fails the command. `--limit` instead selects a prefix.
Use a positive `--deadline-ms N` to change the whole-command budget, including
input loading and output.

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

## Plan a query

Use `explain` with the same SQL file and table inputs as `query`. It loads and
authorizes snapshots, then builds the optimized plan without executing result
rows. Metadata IO is required. Success does not establish access to Parquet or
deletion-vector payloads, or guarantee that execution will fit its resource limits.

```sh
dar --diagnostics=json explain /data/orders --sql-file query.sql
dar --diagnostics=json explain /data/orders --sql-file query.sql --table-version 42 \
  --node 0 --field 0 --statistics --scan 0
```

The first command returns actual `bindings`, `node_count`, `scan_count`, and the
root's `column_count`. Replace `42` with its reported snapshot version. For
multiple tables, use `--tables-file` and pin each entry's `table_version`.
Keep SQL and options unchanged for subsequent selections.

| Selection | Returned detail |
| --- | --- |
| `--node N` | Physical operator, child IDs and output column count. Root is 0. |
| `--field N` | Exact output type at that column position; repeat for multiple fields. Uses the selected node, or root by default. |
| `--statistics` | Planner rows and relation bytes for the selected node, or root. |
| `--scan N` | Logical scan's binding, source projection positions and pushdown counts. Can combine with node selections. |

Node IDs and scan IDs are separate zero-based preorder lists. A self-join can
have several scans of one binding. Projection `null` means all source columns;
`[]` means zero columns. Pushdown counts distinguish exact filters from inexact
filters requiring a residual check. Other filters remain in the physical plan.

Statistics mark values as `exact`, `estimated`, or `unknown`. These describe
planner knowledge, not observed query costs. `relation_bytes` is not storage IO.
On Delta scan nodes, `selected_files` reports metadata-derived planned files and
input row/byte estimates before row filters or deletion vectors. Missing estimates
are null, not zero. No execution time or IO cost is invented.

Names, SQL expressions and metadata are omitted from plan output. Selected types
retain their Arrow encoding. A type containing arbitrary nested names or timezone
text fails before stdout because that text could come from a SQL literal. Only
fixed structural child names and UTC or absent timezones are disclosed. Select
another field or omit `--field`; a larger budget cannot resolve this restriction.
Use `capabilities --schema query-plan --detail node.fields` for the exact rule.

The complete response, including its newline, defaults to 4096 bytes. Oversized
requests fail with empty stdout: select fewer fields or explicitly increase
`--max-output-bytes`. Even the summary must include every binding. The deadline
defaults to 30000 ms and covers input, authorization, planning and output.
`--memory-limit-bytes` defaults to 256 MiB for the DataFusion pool; metadata,
planning buffers and process RSS are outside that limit. No spill is enabled.
SQL admission and credential-file rules are the same as for `query`.

With `--diagnostics=json`, require exit 0 and a v3 completion whose `command` is
`explain`, then read the plan and bindings from stdout.
Focused format lookups accept paths such as
`capabilities --schema query-plan --detail node.statistics.selected_files`.

## Read a Unity Catalog table

Use `uc://catalog.schema.table` and an explicit `--databricks-profile PROFILE`
with `inspect`, `preview`, `scan`, or `query`. DAR obtains temporary authorization through
that profile and reads the data locally.

Follow [Read Unity Catalog tables](unity-catalog.md) for setup, a pinned sample,
SQL, and a UC/local join. Look up accepted table types, storage settings, and
renewal behavior in the [Unity Catalog access reference](reference/unity-catalog.md).

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
| 3 | Result or machine-completion output failure, including a broken pipe |
| 130 / 143 | Handled cancellation by Ctrl-C / Unix SIGTERM |

All commands handle cancellation, including during input loading and blocked output.
For command-specific signal behavior, target filters, or diagnostic fields,
see the [CLI diagnostics reference](reference/cli-diagnostics.md).

## Related references

<span id="join-table-snapshots"></span>
<span id="filter-rows"></span>
<span id="execution-options"></span>
<span id="predicate-objects"></span>
<span id="scalar-objects"></span>

[Inputs and execution settings](reference/cli-inputs.md).

<span id="read-a-small-result-as-typed-json"></span>
<span id="save-a-result"></span>
<span id="record-provenance-with---manifest"></span>
<span id="inspection-json"></span>
<span id="save-a-result-with-a-manifest"></span>
<span id="verify-a-json-result"></span>

[Output formats and result files](reference/cli-output.md).

<span id="storage-options-file"></span>
<span id="explicit-credential-sources"></span>
<span id="provider-limits"></span>
<span id="credential-env-files"></span>

[Credential selection and env files](reference/cli-authorization.md).

<span id="s3-credential-sources"></span>
<span id="s3-web-identity"></span>
<span id="s3-container-credentials"></span>

[S3 credentials](reference/cli-s3.md).

<span id="azure-credential-sources"></span>
<span id="azure-workload-identity"></span>
<span id="azure-managed-identity"></span>

[Azure credentials](reference/cli-azure.md).

<span id="gcs-credential-sources"></span>

[GCS credentials](reference/cli-gcs.md).

<span id="discover-installed-commands"></span>

[Offline capability discovery](reference/cli-discovery.md).

<span id="installation-alternatives"></span>
<span id="use-the-cli-from-a-python-wheel"></span>
<span id="use-a-linux-binary"></span>
<span id="build-from-source"></span>

[Installation and runtime requirements](installation.md).

<span id="use-a-container-or-a-go-service"></span>

[Container and Go integration](cli-integration.md).
