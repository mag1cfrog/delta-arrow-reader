# CLI capability discovery

Use these offline lookups to discover the installed command surface or a specific
input or output field. For a first read, start with the [quickstart](../cli-quickstart.md).

## Discover installed commands

```sh
dar capabilities
dar capabilities --command query
```

Use the first command if you do not know which operation you need; use the
second if you already chose `query` and need its argument rules. These are
lookup alternatives, not a required sequence. A known command needs no prior
discovery call. For one input type or credential source, select its guide
directly, such as `--schema predicate --detail timestamp_us` or
`--authorization s3 --detail env-file`.

`dar capabilities` returns a JSON directory
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
| `dar capabilities --authorization s3` | `authorization_guide` | Credential source choices and access rules for one service. |
| `dar capabilities --authorization s3 --detail env-file` | `authorization_detail` | Accepted env-file variables for one storage service. |
| `dar capabilities --authorization s3 --detail env-file-grammar` | `authorization_detail` | Shared env-file syntax and limits. |
| `dar capabilities --authorization s3 --detail aws-web-identity` | `authorization_detail` | Configuration for the selected credential source. |
| `dar capabilities --authorization s3 --detail connection` | `authorization_detail` | Common noncredential connection settings. |
| `dar capabilities --schema NAME` | `schema_guide` | One example, format notes, file size limit, and available fields or types. |
| `dar capabilities --schema NAME --detail FIELD_OR_TYPE` | `schema_detail` | One field, predicate operator, or scalar type from that guide. |

All responses have `format_version: 1`. The directory and full definitions also
include `cli_version`. `--command`, `--schema`, `--authorization`, and `--all`
are mutually exclusive. `--full` requires `--command`, `--schema`, or
`--authorization`. `--detail` requires `--schema`, `--authorization`, or `--command` and
conflicts with `--full`. Command names must match an installed top-level command.
Unsupported names or conflicting selectors fail with status 2.
For text help, use `dar query --help` or the corresponding command's help.

### Field and type selectors

For output and execution-option schemas, `--detail` accepts the example's
top-level field names and any names in `other_fields`. The latter lists only
fields absent from the example; it is omitted when the example shows them all.
The completion guide instead uses `version_1_fields` for the v1 fields absent
from its v2 example.
The `result-artifact` and `result-manifest` guides list fields without expanding
their contents. Nested fields use dots, such as `--detail result.format` or
`--detail replay.options`. A selected object lists its child fields.

### Command and join guides

For example, after finding `query` in the directory, run
`dar capabilities --command query`. Its `example` is an argument array with
placeholder paths. The `sql` field gives the table alias `t`, the `--sql-file`
argument, and a count query to put in that file. The example uses `--format json`
for a small complete result. `options` describes commonly needed parameters,
and `resource_defaults` gives the ceilings for the format named by
`resource_defaults_format`. Use `help_command`
for all options or `full_command` for their machine-readable definitions. Fetch
`--schema storage-options` if a storage-options file is needed, then follow
`authorization_command` for the selected service. Local paths need no
credentials. To recover these details later, repeat the same discovery commands;
discovery does not depend on previous invocations.

For a local JOIN, follow the query guide's `tables` input schema to
`--schema tables`. Its `query_example` uses `--tables-file` instead of a
positional table, and `sql_example` uses the aliases in the manifest `example`.
The same guide explains relative-path rules. The manifest guide's
`detail_syntax` explains how to select nested fields; `other_fields` lists the
optional authorization reference omitted from the example. Select
`--detail 'tables[].authorization'` for a credential-file example and service
guides. Select `tables[].authorization.source` for identity choices or
`tables[].authorization.databricks_profile` for profile requirements.

### Authorization guides

Authorization guide names are `s3`, `azure-blob`, `gcs`, and
`unity-catalog`. The first three describe direct storage access; Unity Catalog
describes access through a Databricks profile. Command guides list the applicable
names in `authorization.guides`; the storage-options guide lists
`authorization_guides`. Each authorization response identifies its guide in
`name`. For example, use `dar capabilities --authorization azure-blob`
for Azure Blob Storage, including its Data Lake Storage Gen2 capabilities.
The default guide lists sources and the commands for their details. Select a
source with `--detail`, such as `json`, `env`, or `aws-web-identity`, for its
configuration. Provider details include JSON reference examples, environment
names, precedence, and renewal rules. Use `--detail connection` for common
connection settings. `full_command` supplies alternate spellings and complete
service metadata when needed.
These responses contain no credential values and do not check the caller's access.

For a provisioned credential env file, pass `--env-file PATH` directly;
reading or expanding its values is unnecessary. For its arguments, conflicts,
or contents, follow the service guide's `env_file_details_command`.
This selects `--detail env-file`, with the service's accepted variable names
and credential combinations. Its `grammar_command` selects
`--detail env-file-grammar` for syntax and limits. These two details apply to
`s3`, `azure-blob`, and `gcs`; Unity Catalog uses a Databricks profile.

### Input and output guides

An input schema guide's `example` can be used as the file contents, subject to the
listed constraints. For predicates, the example's column and literal type must
match the table. Predicate guides list available `operators` and `scalar_types`.
Replace the `NAME` argument in `detail_command` with the field, operator, or
scalar type needed. For example,
`--schema execution-options --detail prefetch_files_per_partition` gives one
setting's default and bounds. `--schema predicate --detail timestamp_us` gives
the timestamp unit, integer encoding, and required timezone field. Selected
details retain the units and constraints needed for that field or type.

These guides describe how to use a format. Output field details retain types,
units, ranges, and null meanings; full schemas include validation expressions.
Recursive predicate references lead
to another operator or scalar choice through `detail_command`. Use a guide's
`full_command` when a JSON Schema validator or a metadata consumer needs the
complete definition.

`--schema completion` explains how to verify a terminal success record.
Its v2 example applies to JSON query stdout: compare snapshots and counts with
stdout, which carries sample/completion state. File output uses v3 and reports
the receipt's byte count. Read JSON result counts and snapshots from the saved file.
Use `--detail FIELD` for a field's type and meaning, including fields used in
v1 scan and Arrow completion records. `--schema rows --detail has_more` explains the three
preview stopping states without repeating conditional validation blocks;
`--full` retains those rules for JSON Schema validators.

## Complete metadata for integrations

Use a full definition when implementing a validator or inventory consumer:

| Invocation | JSON `kind` | Contents |
| --- | --- | --- |
| `dar capabilities --command query --full` | `command_capabilities` | Complete query argument metadata, authorization, formats, schemas, and limits. |
| `dar capabilities --authorization s3 --full` | `authorization_capabilities` | Credential aliases and complete access rules for the selected service. |
| `dar capabilities --all` | `capabilities` | Complete command tree and support information. |
| `dar capabilities --schema NAME --full` | `input_schema` or `output_schema` | One complete JSON Schema validation document. |

In full command metadata and the full inventory, `command` contains the selected
command or complete command tree, and each argument's required status, value
count, defaults, allowed choices, repeatability, and conflicts. For example,
`scan --column` is repeatable and conflicts with `--no-columns`.
`stdout_formats` identifies result formats; `diagnostics` describes stderr
formats. `input_schemas` lists the applicable schema names and their file
arguments. `output_schemas` lists result schema names and their streams. The
`inspect` entry in `stdout_formats` and the JSON entry in `diagnostics` identify which
output schemas apply.

`diagnostics.json.format_versions` gives the default record versions for the
selected command. A `completion_format_version` in `stdout_formats` overrides
that default for its command or format choice. For example, preview uses version
2; query's Arrow stdout uses version 1 and its JSON stdout uses version 2.
Scan/query with `--output` use version 3, advertised in `stdout_formats.NAME.output_file`.
Explain also uses version 3; its bindings are in stdout and it has no row counts.
`completion_format_versions` lists all versions available in the selected scope.
In the full inventory, the default is version 1; use each command's format entry
to select its completion version.

In full metadata, `authorization` describes local access, direct-storage sources
and configuration files, and Databricks profiles, including the commands that
accept each. Direct storage supports explicit source selection per command or
manifest table. Its authorization guide lists supported sources, credential inputs,
and the backend lookup used when source selection is omitted. Discovery
does not inspect the caller's credentials or check provider availability.
The authorization guide's `full_command` returns complete metadata for the
selected service, including credential option aliases.

Each command guide links to `capabilities --command NAME --detail runtime` for
deadline scope, cleanup allowances, platform signal behavior, and retry rules.
Its `shutdown` summary gives the maximum cleanup and failure-reporting allowances
beyond the selected deadline.
For example, `dar capabilities --command query --detail runtime` describes query
cancellation without returning SQL or schema metadata.

`read_support` describes each command's scope and limits. For example, `query`
registers positional `TABLE` as `t`, or up to 64 named snapshots from a manifest.
UC SQL accepts a positional table with an explicit Databricks profile, or
manifest entries with per-table profile references. A manifest can mix local,
direct-storage, and UC tables. The memory ceiling
applies to DataFusion's execution pool, and disk spill is disabled by default. The SQL examples that use `id`
assume the selected table has that column. A null limit or deadline means that
the command has no ceiling for that resource; it does not mean zero.

An empty `possible_values` list means the argument has no enumerated choices;
it does not mean every value is valid. A null `max_values` means there is no
fixed upper count for that argument. Descriptions contain additional input rules.

### JSON Schema validation

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
| `rows` | stdout | Typed preview or query JSON with ordered fields and positional rows. |
| `result-artifact` | stdout | A saved-result receipt: v2 names the result; opt-in v1 also names its manifest. |
| `result-manifest` | file | Sources, completion, counts and replay settings saved with `--manifest`. |
| `query-plan` | stdout | Summary or selected plan node, output types, scan pushdown, or planner statistics. |
| `diagnostic` | stderr | A terminal failure record with `--diagnostics=json`, including optional correction fields. |
| `completion` | stderr | A terminal success record with `--diagnostics=json`. Stdout mode includes snapshots and counts; file mode counts receipt bytes; `--manifest` optionally saves result metadata. |

Output schemas allow additional object fields. Version and count strings use
unsigned decimal notation; consumers must check the u64 range when converting
them. The inspection schema checks the Arrow schema's outer fields, metadata,
and type-description shapes. Use Arrow's Rust Serde decoder to validate the nested
`data_type` encoding. Arrow IPC from `scan` or `query --format arrow` carries its
schema in the stream itself. The `rows` schema validates the JSON envelope;
interpret row values using the returned field types and the selected encoding guide.

For example, save `dar capabilities --schema completion --full` and validate the
command's stderr record against its `schema` field. Also check exit status 0
and decode stdout: a matching completion record alone does not establish that
the result was received intact.

For a validator or inventory consumer that explicitly needs complete definitions,
save the chosen expansion to a file:

```sh
dar capabilities --schema completion --full > completion-schema.json
dar capabilities --all --diagnostics=json > capabilities.json
```

## Output limits and process behavior

Discovery needs no table, input file, credentials, Databricks executable, or
network access. The directory, runtime details, schema guides, schema details, authorization
guides, and authorization details are limited to 2 KiB each; command guides are
limited to 4 KiB. Explicit full command and authorization metadata, full schemas,
and the full inventory are limited to 1 MiB.
Limits count UTF-8 bytes, including JSON framing and the final newline. An oversized
response fails before writing stdout; discovery never truncates JSON.
Every discovery view, help, and version has a five-second deadline, including
stdout, log setup, and completion reporting on stderr.
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

## Help and version

`dar --version` prints the executable name and version, such as `dar 0.8.1`.

Use `dar --help` for commands or `dar query --help` for one command's flags.
`dar help query` is equivalent. `-h` and `-V` are short for `--help` and
`--version`. These lookups do not open input files or contact storage, and their
output is subject to the five-second discovery deadline.
