# CLI agent workflow contract

For contributors implementing and reviewing the CLI's agent workflow. This
contract fixes the behavior shared by individual features. The interfaces below
are targets; the [CLI reference](../../crates/delta-arrow-reader-core/docs/public/cli.md)
documents the implemented commands.

## Workflow and interface

An agent discovers the installed CLI, selects authorization by reference,
inspects and pins a snapshot, previews rows, runs a read, and verifies completion.
Local tables and existing direct-storage backends are in scope. SQL runs locally
through DataFusion. Catalog integrations and writes are separate work.

| Step | Target interface |
| --- | --- |
| Discover | `dar capabilities` returns a short directory; `--command NAME` returns command details, `--schema NAME` a named schema, and `--all` the full inventory. All are offline. |
| Inspect | `dar inspect TABLE --view summary`; `--column NAME` selects fields, `--include-metadata` opts into metadata. |
| Preview | `dar preview TABLE`; reuse scan projection, predicate, version, and execution options. |
| Query | `dar query TABLE --sql-file PATH`; single-table alias is `t`. `--format arrow` is the default; `--format json` returns typed rows. |
| Join | `dar query --tables-file PATH --sql-file PATH`; bind each table explicitly. |
| Plan | `dar explain` with the same query inputs; report bounded structured plan information without executing rows. |
| Save | `scan` or `query` with `--output PATH`; publish a result and completion manifest. |

The wheel executable is `delta-arrow-reader`, with the same arguments.
Capabilities reports only installed commands, options, schemas, auth mechanisms,
formats, defaults, and limitations. Help/discovery requires neither credentials
nor network access. JSON Schemas use draft 2020-12 and match the actual parsers.

The default directory fits in 2 KiB and gives explicit invocations for loading
command details and schemas. A caller can select a command without downloading
the entire inventory or writing a filtering script. Command details include all
of that command's argument rules and applicable authorization, format, and
resource constraints. Full schemas and the full inventory are explicit requests.
Discovery is stateless, so an agent can retrieve the same details after context
compaction. Byte limits count the complete UTF-8 response, including framing;
oversized responses fail before stdout is written rather than being truncated.

Summary inspection reports the actual version and scoped checks: `passed`,
`failed`, `unknown`, or `not_checked`. Metadata access does not establish data-file
access. Inspection reads no Parquet/DV payloads and can describe a snapshot whose
scan protocol is unsupported. Names and metadata are untrusted result data.

```sh
dar capabilities
dar --diagnostics=json inspect /data/orders --view summary --column id
dar --diagnostics=json preview /data/orders --table-version 42 --column id --limit 1
dar --diagnostics=json query /data/orders --table-version 42 --sql-file count.sql --format json
```

## Output and recovery

Keep existing full `inspect` JSON v1, Arrow-stream `scan`, and input-file formats
compatible. New inspection behavior is opt-in. New JSON envelopes have `kind`
and `format_version: 1`; incompatible meanings/types require a new version.
Consumers ignore additional output fields; parsers reject unknown input fields
and unsupported versions. Stdout contains only the result.

Typed JSON uses the existing Arrow schema encoding and positional row arrays,
so duplicate names and zero-column rows survive. Empty results retain schema.

| Type | JSON representation |
| --- | --- |
| Integer, decimal | Decimal string; decimals store the unscaled integer with precision/scale in the schema. |
| Date, time, timestamp, duration | Integer string in the Arrow unit; retain timezone, including null. |
| Binary | Padded base64 string; fixed-size values retain their declared size. |
| List, struct, map | Arrays; structs follow field order, maps are ordered `[key, value]` pairs. |
| Dictionary | Decoded logical value; retain key/value types in the schema. Physical indexes are not preserved. |
| Boolean, string, null | Native JSON values; null differs from an empty collection/string. |
| Float | Round-tripping finite number; strings for `-0`, `NaN`, `Infinity`, and `-Infinity`. NaN payload bits require Arrow. |

Reject unsupported types, including unions, intervals, and run-end encoding,
with `unsupported_json_type`. Never silently stringify a value. Predicate input
encoding remains unchanged, including its binary byte arrays.

A one-row preview response:

```json
{
  "format_version": 1, "kind": "rows",
  "bindings": [{"alias": "t", "table_version": "42"}],
  "schema": {"fields": [
    {"name": "id", "data_type": "Int64", "nullable": false,
     "dict_id": 0, "dict_is_ordered": false, "metadata": {}}
  ], "metadata": {}},
  "rows": [["9007199254740993"]], "row_count": "1",
  "completion": "sample", "stop_reason": "row_limit", "has_more": null
}
```

Preview always reports `sample`. `has_more` is true only when an extra row was
observed, false at observed exhaustion, and otherwise null. Stop reasons are
`row_limit`, `byte_limit`, or `end_of_input`. A first row/schema that cannot fit
fails; later byte overflow returns the preceding sample. Never slice JSON bytes.

A query succeeds only after exhausting its requested result, with
`completion: "complete"` and `scope: "query"`. SQL LIMIT changes that result;
`--max-rows` is a resource ceiling and excess rows cause failure. Scan with
`--limit` has `scope: "limited_scan"`; it is not a complete table export.
Reaching an exact ceiling alone does not establish exhaustion.

Use the global `--diagnostics=json` option before or after the command, and
before `--`, to select versioned JSON Lines on stderr, including parse errors.
Plain stderr stays the default.
Reuse typed phase/code/reason classifications.
Do not echo SQL, paths, unknown keys, credential contents, or provider messages.
Known argument/field names and line/column locations provide safe error context:

```json
{
  "format_version": 1, "kind": "diagnostic", "terminal": true,
  "phase": "configuration", "code": "invalid_input_json",
  "reason": "input_json_decode_failed", "argument": "--predicate-file",
  "line": 3, "column": 12, "retryable": false, "recovery": "fix_input"
}
```

Recovery distinguishes `fix_input`, `authorize`, `request_access`, `retry`,
`change_request`, and `adjust_budget`. Unclassified failures remain `unknown`
with `retryable: null`; do not guess from error text. Preserve exits: 0 success,
2 argument/local-input failure, 1 runtime/planning/auth/resource failure,
3 stdout failure. Handled interrupt/termination uses 130/143 where supported.
A predicate that parses but fails table planning remains a runtime error.

Machine mode attempts one terminal failure or completion record, independent of
`RUST_LOG`. Completion includes scope, stop reason, actual bindings, and row,
batch, and result-byte counts. Data-sized quantities are decimal strings.
Consumers require exit 0, valid framing, and the terminal record in machine
mode. IPC decoding or a JSON prefix alone cannot prove success. Failed streams
must not write a success marker. Results themselves are not secret-filtered.

## Budgets and cancellation

All byte units below are binary. Numeric overrides are finite decimal integers;
reject overflow and unsupported platform sizes. Zero means zero, not unlimited.
Rows may be zero; byte/time/memory overrides must be positive. Spill zero
disables it.

| Resource | Default/bound | Override |
| --- | --- | --- |
| JSON/env files | 1 MiB each; tables: 64 bindings/depth 32; env: 1,024 records | None |
| SQL file | 64 KiB, one statement, nesting at most 128 | None |
| Predicate structure | Existing 1,024 nodes/depth 32 | None |
| Preview | 20 rows, 64 KiB total serialized response, 30 seconds | `--limit`, `--max-output-bytes`, `--deadline-ms` |
| Summary/explain | 64 KiB, 30 seconds | `--max-output-bytes`, `--deadline-ms` |
| Query/file-mode scan | 1,000,000 rows, 300 seconds; JSON 16 MiB, Arrow 1 GiB | `--max-rows`, `--deadline-ms`, `--max-output-bytes` |
| Query memory/spill | 256 MiB pool, spill disabled | `--memory-limit-bytes`; positive `--spill-limit-bytes` requires `--spill-dir` |
| Discovery | Directory: 2 KiB; command details, named schema, or full inventory: 1 MiB; 5 seconds | Select details with `--command`, `--schema`, or `--all` |
| Provider capture/manifest | 1 MiB per child stream or manifest | None |
| Diagnostics | 8 KiB per record; 64 KiB optional records plus terminal reserve | None |

Limits include schema/framing, not just row values. Stream full queries; buffer
only bounded previews. Query-pool memory is not total process RSS. Spill uses
private per-invocation files, a total disk budget, and cleanup on handled exits.

Legacy full inspect/stdout scan keeps existing unbounded defaults; agent callers
must opt into finite deadline/output limits, and a scan row ceiling. Existing
auth selection also stays unchanged unless the caller opts into explicit sources.
This avoids silently changing current consumers.

Commands never prompt. Total deadlines cover input, auth, planning, execution,
blocked output, and publication. Cancel work, terminate/reap controlled children,
and clean up owned temporary files within 1 second after cancellation/deadline.
Broken pipes fail. SIGKILL, OS-uninterruptible IO, crashes, or closed stderr cannot
guarantee cleanup/diagnostic delivery; missing completion is not success.

## Authorization by reference

Users provision files, inherited environment, or provider sessions. Agents pass
references, without reading secrets or expanding them into tool-call arguments.
A credential file is not isolation from an agent that can read it; filesystem
permissions and the runner boundary remain separate controls.

New commands, summary inspection, and file-mode scan use these selection rules:

- Local tables need no source. For remote storage, a storage-options file alone
  selects JSON, `--env-file PATH` selects that file, otherwise require
  `--auth-source SOURCE`. Existing commands opt in with either selector.
- `--auth-source json` requires `--storage-options-file PATH`; `env` reads only
  advertised inherited variables. `--env-file` conflicts with `--auth-source`.
- Select one complete credential family. Reject partial/conflicting families
  and duplicate aliases. JSON may supply noncredential configuration alongside
  another source, taking precedence over its environment configuration.
- A selected source's load, expiry, refresh, or permission failure never falls
  through to another identity. Refresh may renew the same identity only.

| Backend | JSON/environment inputs | Provider mechanisms to expose only when verified |
| --- | --- | --- |
| S3 | Access-key pair plus optional session token; `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_REGION` | `aws-web-identity`, `aws-container`, `aws-imds`; no implied AWS profile/SSO chain. |
| Azure | One account key, SAS, bearer token, or client-secret family; `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY`, `AZURE_STORAGE_SAS_KEY`, `AZURE_STORAGE_TOKEN`, or `AZURE_CLIENT_ID`/`AZURE_CLIENT_SECRET`/`AZURE_TENANT_ID` | `azure-cli`, `azure-workload-identity`, `azure-managed-identity`; no fallback between them. |
| GCS | Service-account/ADC file reference; `GOOGLE_APPLICATION_CREDENTIALS` | `gcp-adc` with explicit file, or `gcp-metadata` without home-directory ADC lookup. ADC supports service-account/authorized-user content. |

Use existing storage-builder keys. Discovery lists accepted aliases, required
provider reference fields, and installed mechanisms. The current S3 builder does
not automatically consume AWS environment credentials. Do not advertise full
SDK chains. Anonymous access, when supported, requires explicit selection.

Env files are bounded UTF-8 `NAME=VALUE` data: LF/CRLF, blank lines, full-line `#`
comments, outer whitespace, and matching single/double quotes are accepted.
Reject unknown/duplicate names, empty values, NUL, malformed quotes, and extra
text after quoted values. No escapes, multiline values, inline comments,
`export`, interpolation, execution, auto-discovery, or process/PATH mutation.
Dollar signs/backticks in values stay literal. Never forward secrets into child
arguments or unrelated child environments. Static inputs load once per command.

```sh
dar --diagnostics=json preview s3://example-bucket/orders --env-file /run/secrets/orders.env
```

## SQL, saved results, and replay

Admit one read-only query with nonrecursive CTEs, aggregation, grouping, ordering,
and joins over registered snapshots. Validate parsed/planned operations; reject
writes, COPY, session changes, extension loading, EXPLAIN ANALYZE, and arbitrary
file/URL sources. Explain exposes structured operators/types/estimates, with
unknowns explicit, without raw SQL/literals or row execution. It may read needed
metadata and must disclose that scope.

A tables file contains aliases, locations, optional requested versions, and
optional authorization references:

```json
{"format_version":1,"tables":[
  {"alias":"orders","location":"./orders","table_version":"42"},
  {"alias":"customers","location":"./customers","table_version":"7"}
]}
```

Reject unknown fields, duplicate/reserved aliases, and mixed single/multi-table
forms before storage IO. Aliases match `[a-z][a-z0-9_]{0,62}`; reserve `t`, system
catalog names, and SQL keywords. Authorization contains `source` and
`storage_options_file`/`env_file` references, never values. Command-line paths
are cwd-relative; manifest references are relative to its directory, and nested
credential-file references to their containing file. Reject credential-bearing
URLs. Each binding resolves once; report the actual version even when latest
was requested. Refresh cannot retarget it. Pinned tables are independent
snapshots, not a cross-table transaction. Repeatable ordering needs ORDER BY
with ties resolved; volatile functions/float aggregation can still vary.

`--output PATH` stages on the destination filesystem, then atomically publishes
without clobbering. V1 has no overwrite mode. Publish `PATH.manifest.json` only
after the complete result. Two-file publication is not atomic: a result lacking
its completion manifest is uncommitted. Receipt failure/cancellation after both
publications can leave a complete pair despite nonzero process status.

The versioned manifest records completion/scope, actual bindings, format, result
path, row/batch/byte counts, result SHA-256, build versions, limits, and execution
overrides. Keep replay input references/hashes and safe locations; never embed
SQL literals, tokens, provider responses, or credential contents/hashes. Stdout
returns an artifact receipt bounded to 8 KiB. Verify manifest, hash, framing, and counts
before acceptance. Replay uses the recorded inputs and explicit actual versions;
input hashes must still match and snapshot/data/DV files must remain available.

## Acceptance evidence

Every criterion needs success, boundary, and failure evidence through installed
artifacts. Untested or blocked cases do not pass. Use local tables and controlled
storage/provider fixtures; a live cloud account is not required.

| Criterion | Success | Boundary | Failure | Evidence |
| --- | --- | --- | --- | --- |
| Discovery | Construct requests from directory and selected details offline | Directory byte budget, complete scoped details, disabled features/platform limits | Unknown command/schema, conflicting selectors, oversized response | Follow advertised invocations; parser/schema checks; no network/child calls |
| Inspection | Selected schema and actual version | Wide/nested schema, unsupported protocol | Metadata accessible/data denied | Exact fields/check scope; IO trace |
| Read coverage | Scan, COUNT, GROUP BY, ORDER BY, CTE, join | Null/empty/zero-column/DV/pinned updates | Write/external-source attempts | Independent exact-result oracle; no write side effects |
| Small results | Interpret typed JSON | Large ints, decimals, timezones, binary, nesting, duplicates, dictionary partitions | Oversized value/unsupported type | Exact values and total-byte bounds |
| Resources/completion | Complete query, labeled preview | Exact limit versus one extra row | Memory/disk/deadline/output exhaustion | Bounded resources; no false completion |
| Recovery | Correct input and retry | Parse/planning distinction; unknown causes | Expiry/denial/transient failures | Stable exits/diagnostics and correct recovery branch |
| Confidentiality | Reference reaches intended identity | Malicious data/provider errors | Credential loading/refresh failure | Secret canaries absent from calls, diagnostics, manifests, child args |
| Auth predictability | Each advertised source authenticates | Aliases/partial families/two identities | Selected-source failure | Signed-request/provider assertions; zero fallback |
| Automation | Non-TTY/piped/file use | Blocked output, PID 1, platform signals | Broken pipe/hung child/cancellation | Deadline-plus-grace, reaping, temp cleanup |
| Integrity/replay | Verify and replay pinned result | Concurrent updates/order ties/destination races | Midstream/disk/publication/hash failure | Versions, decoded rows, digest/counts, no-clobber checks |

Run eight agent tasks: first read from discovery/auth reference; repair predicate
and SQL input; multi-table analysis; expiry/denial recovery; large/long-cell
output; cancellation/midstream failure; adversarial result text; pin/save/replay.
Declare the harness version/configuration, exact model/revision/settings,
artifact/fixture hashes, and prompts before running. Use fresh context/workspace
for five attempts per task. Do not choose a model after seeing scores.

Pass requires 5/5 for every task, with zero credential leaks, instruction
execution from data, broader-identity fallback, or partial/sample false success.
Each task gets 600 seconds, 20 shell calls, 256 KiB captured output, 65,536 total
model tokens, and at most two repair retries per failed request. Acceptance
requires token accounting; exploratory runs without it cannot establish a pass.
Record all outcomes, calls, bytes, tokens, CLI/provider/corrective retries,
latency, and artifacts. Separate deterministic guarantees from the observed
agent results; do not claim all models/deployments behave alike.

For discovery, compare the short directory and selected details with the full
inventory using the same task prompts, model, and fixtures. Record task success,
shell calls, input tokens, and latency. Include a continuation after context
compaction, with the table and task goal retained but command details omitted;
the agent must rediscover the inputs it needs. Byte-budget and parser tests do
not establish a pass for these agent trials.

For this documentation change, check local links and examples, then run:

```sh
uv run --locked --project docs zensical build --strict -f crates/delta-arrow-reader-core/docs/mkdocs.yml
```
