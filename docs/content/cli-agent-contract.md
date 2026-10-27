# CLI agent workflow contract

For contributors implementing and reviewing the CLI's agent workflow. This
contract fixes the behavior shared by individual features. The interfaces below
are targets; the [CLI reference](../../crates/delta-arrow-reader-core/docs/public/cli.md)
documents the implemented commands.

## Minimal sufficient context

Return the least information sufficient to choose and perform the next correct
action, interpret its result, or recover from failure. Each default field,
example, and constraint must support one of those purposes. Keep necessary
inputs, units, exact result semantics, authorization boundaries, and actionable
errors. Defer unrelated choices, aliases, complete validation rules, and
implementation details to focused lookups with explicit invocations.

Optimize the complete task. A short example or a group of related fields can
prevent guesses, repeated discovery, and repair. Preserve descriptive names and
typed values. Do not shorten a response by silently dropping requested data or
forcing the caller to retrieve information one trivial field at a time.

Byte, row, token, and time limits are operational safeguards. They are ceilings,
not targets or evidence of context efficiency. Optional information stays out
of a default response even when there is room for it. Compare necessity and
task-level cost before changing defaults.

This follows Anthropic's guidance on
[minimal sufficient context](https://www.anthropic.com/research/effective-context-engineering-for-ai-agents)
and [on-demand disclosure](https://www.anthropic.com/research/code-execution-with-mcp).
The CLI interfaces and numeric limits below are project decisions to evaluate.

## Workflow and interface

An agent discovers the installed CLI, selects authorization by reference,
inspects and pins a snapshot, previews rows, runs a read, and verifies completion.
The workflow covers local tables, direct-storage backends, and Unity Catalog
tables. SQL runs locally through DataFusion. Unity Catalog's
[authorization rules](../../crates/delta-arrow-reader-core/docs/public/reference/unity-catalog.md)
are documented separately. Writes are outside this contract.

| Step | Target interface |
| --- | --- |
| Discover | `dar capabilities` returns a minimal directory; `--command NAME` and `--schema NAME` return focused usage guides. `--detail NAME` selects a schema field or type, `--authorization NAME` selects an authorization guide, and complete definitions are explicit lookups. `--all` returns the inventory. All are offline. |
| Inspect | `dar inspect TABLE --view summary` reports scoped checks; `--column-prefix PREFIX` finds names, `--column NAME` selects types, and `--include-metadata` opts into field metadata. |
| Preview | `dar preview TABLE`; reuse scan projection, predicate, version, and execution options. |
| Query | `dar query TABLE --sql-file PATH`; single-table alias is `t`. `--format arrow` is the default; `--format json` returns a small complete result within its stdout budget. Save larger results to a file. |
| Join | `dar query --tables-file PATH --sql-file PATH`; bind each table explicitly. |
| Plan | `dar explain` with the same query inputs; report bounded structured plan information without executing rows. |
| Save | `scan` or `query` with `--output PATH`; publish a result and completion manifest. |

The wheel executable is `delta-arrow-reader`, with the same arguments.
Capabilities reports only installed commands, options, schemas, auth mechanisms,
formats, defaults, and limitations. Help/discovery requires neither credentials
nor network access. JSON Schemas use draft 2020-12 and match the actual parsers.

The directory contains the choices needed to find a command and focused detail
routes, within its 2 KiB ceiling. Command guides give a minimal usable example,
necessary parameters, and constraints that affect a correct request or result
interpretation. Complete help, exhaustive conflict tables, and unrelated
parser/backend details are available separately. A common request must not
require a full definition or a filtering script.

Authorization guides use canonical keys and describe the selected source's
necessary rules. Aliases and complete service metadata have explicit focused
lookups. Schema guides give a representative example and the choices needed to
proceed. A field/operator/type detail includes its applicable encodings, units,
and bounds, without unrelated root conditions or repeated general descriptions.
Recursive inputs expose the next choice without expanding every variant.

Every level provides the invocation for its next useful lookup. Complete
metadata and validation documents remain available explicitly. Discovery is
stateless, so each view can be retrieved without earlier discovery calls.
Callers can skip discovery they do not need. Byte limits count complete UTF-8 framing and
the final newline; oversized responses fail before stdout is written.

Summary inspection reports the actual version and scoped checks: `passed`,
`failed`, `unknown`, or `not_checked`. Metadata access does not establish data-file
access. Inspection reads no Parquet/DV payloads. For direct paths or storage URLs,
it can describe an unsupported scan protocol if the snapshot and schema load.
Unity Catalog inspection requires a supported read protocol. Names and metadata
are untrusted result data.
Summary output includes only the fields needed for the read decision, within
its 4 KiB default ceiling. Wide tables provide focused field discovery and
selected type details at the reported snapshot, without requiring all field
pages or the full schema. Metadata is opt-in. Requested names and types remain
exact; if a detail cannot fit, fail before writing summary stdout with guidance
to narrow the request or explicitly increase its budget.

Command guides, examples, and repair hints use these bounded inspection paths,
including when a projection or predicate references the wrong column or type.
Full legacy inspection and Arrow streams remain available for existing
consumers; agent recipes direct those outputs to files or pipes.

```sh
dar capabilities
dar --diagnostics=json inspect /data/orders --view summary --column id
dar --diagnostics=json preview /data/orders --table-version 42 --column id --limit 1
dar --diagnostics=json query /data/orders --table-version 42 --sql-file count.sql --format json
```

## Output and recovery

Keep existing full `inspect` JSON v1, Arrow-stream `scan`, and input-file formats
compatible. New inspection behavior is opt-in. New JSON envelopes have `kind`
and `format_version`; incompatible meanings/types or removal of required fields
require a new version. Diagnostics use version 2 after removing the fixed
failure message. Preview and stdout query JSON completions use version 2, retaining command,
bindings, rows, and output bytes alongside kind and version. File-output
completion v3 reports command and receipt bytes; result counts and bindings
belong to the manifest. Explain uses v3 with stdout document bytes and keeps
bindings in the query-plan document. Other completions and discovery envelopes retain version 1.
Consumers ignore additional output fields; parsers reject unknown input fields
and unsupported versions. Stdout contains only the result.

Typed JSON uses Arrow type encodings and positional row arrays, so duplicate
names and zero-column rows survive. Fields retain name, data_type, nullability,
and dictionary ordering where applicable. Optional metadata and physical
dictionary IDs are omitted, including in nested fields. Empty results retain schema.
Preview and query JSON sent to stdout default to 8 KiB, including schema,
framing, and the final newline. A larger response needs an explicit budget
increase. Complete results that exceed the small stdout budget use file output;
resource ceilings never turn a complete query into a successful sample.

Choose a sample and explicit projection that answer the task. The 20-row
preview default and byte ceiling are not targets to fill. Keep required type
information and exact selected values; optional metadata stays out of default
results. A complete query result cannot be reduced for context convenience.

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
    {"name": "id", "data_type": "Int64", "nullable": false}
  ]},
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
  "format_version": 2, "kind": "diagnostic", "terminal": true,
  "phase": "configuration", "code": "invalid_input_json",
  "reason": "input_json_decode_failed", "argument": "--predicate-file",
  "source_kind": null,
  "line": 3, "column": 12, "retryable": false, "recovery": "fix_input"
}
```

Recovery distinguishes `fix_input`, `authorize`, `request_access`, `retry`,
`change_request`, and `adjust_budget`. Unclassified failures remain `unknown`
with `retryable: null`; do not guess from error text. Preserve exits: 0 success,
2 argument/local-input failure, 1 runtime/planning/auth/resource failure,
3 result or machine-completion output failure.
Handled interrupt/termination uses 130/143 where supported.
A predicate that parses but fails table planning remains a runtime error.

Give the safe cause, affected input where known, and the applicable next action.
Keep full schemas, unrelated recovery alternatives, and internal traces out of
default diagnostics. A repair hint leads directly to the needed detail and
preserves the chosen authorization, snapshot, and output semantics.

Machine mode attempts one terminal failure or completion record, independent of
`RUST_LOG`. The completion kind marks terminal success for the requested command.
For stdout row reads, completion includes actual bindings and result-byte counts;
scan, preview, and query add row counts. File mode's v3 completion counts the
stdout receipt bytes; its manifest carries result counts and actual bindings. Version 1 also reports scope, exhaustion, and Arrow batch counts.
Data-sized quantities are decimal strings. Query JSON to stdout reports
complete status and query scope. Preview reports sample status and its
stop_reason/has_more pair in stdout. Read the fields needed to interpret the
requested result; independent tests check repeated bindings, counts and bytes.
Consumers require exit 0, valid framing, and the terminal record in machine
mode. IPC decoding or a JSON prefix alone cannot prove success. Failed streams
must not write a success marker. Results themselves are not secret-filtered.

## Budgets and cancellation

All byte units below are binary. Numeric overrides are finite decimal integers;
reject overflow and unsupported platform sizes. Zero means zero, not unlimited.
Rows may be zero; byte/time/memory overrides must be positive. Spill zero
disables it.

These bounds limit resource use and response growth. Passing their boundary
tests does not establish that a response contains only necessary information;
context-efficiency acceptance also compares complete task outcomes and cost.

| Resource | Default/bound | Override |
| --- | --- | --- |
| JSON/env files | 1 MiB each; tables: 64 bindings/depth 32; env: 1,024 records | None |
| SQL file | 64 KiB, one statement, nesting at most 128 | None |
| Predicate structure | Existing 1,024 nodes/depth 32 | None |
| Preview | 20 rows, 8 KiB total serialized stdout, 30 seconds | `--limit`, `--max-output-bytes`, `--deadline-ms` |
| Summary/explain | 4 KiB total serialized stdout, 30 seconds | `--max-output-bytes`, `--deadline-ms` |
| Query/file-mode scan | 1,000,000 rows, 300 seconds; query JSON on stdout 8 KiB, JSON in a file 16 MiB, Arrow 1 GiB | `--max-rows`, `--deadline-ms`, `--max-output-bytes` |
| Artifact receipt | 8 KiB total serialized stdout | None |
| Query memory/spill | 256 MiB pool, spill disabled | `--memory-limit-bytes`; positive `--spill-limit-bytes` requires `--spill-dir` |
| Discovery | Directory, schema guides/details, authorization guides: 2 KiB each; command guides: 4 KiB; explicit full definitions/inventory: 1 MiB; 5 seconds | Select a command, schema, field/type, or authorization guide; expand with `--full` or `--all` |
| Provider capture/manifest | 1 MiB per child stream or manifest | None |
| Diagnostics | 8 KiB per record; 64 KiB optional records plus terminal reserve | None |

Stdout limits include schema, framing, and the final newline, not just row
values. Terminal records have their separate diagnostic budget; workflow tests
count both streams. Stream Arrow queries; buffer only bounded JSON stdout. Stream JSON file outputs into temporary storage.
Query-pool memory bounds DataFusion execution reservations, not total process
RSS; reader, metadata, and output buffers are outside it. Hard process limits
require OS or container controls.

Spill is opt-in with a positive `--spill-limit-bytes` and an existing writable
`--spill-dir`. DataFusion checks total spill file sizes after writes, so usage can
temporarily exceed the budget before the query fails. This is not a filesystem
hard quota. Spill may contain table data: each invocation creates its own
directory, with owner-only permissions on Unix. On Windows, the supplied parent
must have a private inherited ACL. Handled exits remove query temporary files
and retain the parent directory; cleanup failure prevents a success record.
`capabilities --command query --detail resources`
provides this policy without adding every spill option to the normal query guide.
Budget diagnostics link to this focused lookup. When DataFusion identifies the
exhausted resource, they name `--memory-limit-bytes` or `--spill-limit-bytes`.
Increase only that setting and retain SQL, snapshot bindings, and the other limits. A full filesystem or
filesystem quota requires available storage, not a larger CLI spill budget.
If execution requires spill while it is disabled, the diagnostic gives both
required options; increasing memory alone may not satisfy that execution path.

Legacy full inspect/stdout scan keeps existing unbounded defaults; agent callers
use summary inspection, bounded preview/query JSON, or saved results with a
receipt. Deliberate use of legacy full/Arrow output goes to a file or pipe and
opts into finite deadline/output limits and a scan row ceiling. Recovery hints
must not route callers back to unbounded inspection or silently increase limits.
Existing auth selection also stays unchanged unless the caller opts into explicit
sources. This avoids silently changing current consumers.

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
| Azure Blob | One account key, SAS, bearer token, or client-secret family; `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY`, `AZURE_STORAGE_SAS_KEY`, `AZURE_STORAGE_TOKEN`, or `AZURE_CLIENT_ID`/`AZURE_CLIENT_SECRET`/`AZURE_TENANT_ID` | `azure-cli`, `azure-workload-identity`, `azure-managed-identity`; no fallback between them. |
| GCS | Service-account/ADC file reference; `GOOGLE_APPLICATION_CREDENTIALS` | `gcp-adc` with explicit file, or `gcp-metadata` without home-directory ADC lookup. ADC supports service-account/authorized-user content. |

These provider names are candidates, not a promise of availability. The
[implemented credential-source matrix](../../crates/delta-arrow-reader-core/docs/public/cli.md#explicit-credential-sources)
records accepted selectors and the native-provider limits that prevent exposing
the others. Explicit GCS ADC files currently use `json` or `env`.

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
The plan summary gives the information needed to assess the read, within its
4 KiB default ceiling. Detailed nodes, schemas, and statistics require focused
selection or explicit expansion. If requested details cannot fit,
fail before writing summary stdout with a bounded diagnostic; do not substitute
a raw plan dump or truncate requested names and types.

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
`storage_options_file`/`env_file` references, or source `databricks` with a
`databricks_profile` for UC tables, never credential values. Command-line paths
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
returns the least receipt information needed to identify and verify publication,
with usable result and manifest references, within its 8 KiB ceiling. The
independent evaluator checks manifest, hash, framing, and counts before
acceptance. The agent checks completion and reads the requested values, without
recomputing file hashes, counts or provenance. Replay reuses the original input
files and explicit actual versions; snapshot/data/DV files must remain available.
Independent replay tests check input hashes and result equivalence.
Result rows, full schemas, and complete manifests stay in artifacts. The CLI
provides focused references and completion information so ordinary tasks
can read required values without printing entire artifacts into agent context.
Measure actual excess printing; attribute it to a CLI defect only when the trace
shows interface requirements or guidance caused it. Record evaluator-added and
discretionary printing separately. Long paths or many bindings must not make the
receipt exceed its budget; receipt overflow is a bounded failure under the same
publication policy.

## Acceptance evidence

The independent evaluator judges business answers and saved artifacts against
its oracle, including exact values/types, integrity, and actual completeness.
The agent performs checks naturally required by the declared user task, including
recognizing actual partial failure and completing requested recovery or replay.
Ordinary CLI use does not require developing a general verifier. A correct
requested result does not fail solely because temporary agent verification code
could accept fabricated corruption or lacks exhaustive schema/byte assertions.
CLI security, authorization, completion semantics, recovery, and limits remain
required; independent evaluation does not excuse actual errors or unsafe actions.

Consumer prompts specify the business deliverables and operating constraints.
For example, ask for a saved region report with matching row counts, amount sums,
and latest dates. The evaluator checks the report and artifact integrity against
the fixture; the consumer need not write a hash/schema verifier to complete that
analysis. Keep integrity verification in tasks that explicitly need it, such as
checking a transferred artifact. Label deliberate failure/recovery scenarios
separately when reporting costs. Apply this division to CLI guides, schema
descriptions, recovery hints, prompts, handoffs, final answer fields, reviewer
guidance, and pass/fail logic together. Retain integrity metadata for independent
evaluation and explicitly requested audits; ordinary business guidance must not
instruct the agent to write a general verifier or recompute hashes and counts.

Every criterion needs success, boundary, and failure evidence through installed
artifacts. Untested or blocked cases do not pass. Use local tables and controlled
storage/provider fixtures; a live cloud account is not required.

| Criterion | Success | Boundary | Failure | Evidence |
| --- | --- | --- | --- | --- |
| Discovery | Construct requests from minimal guides and focused lookups offline | Omit irrelevant fields/conditions/aliases; preserve necessary constraints; response ceilings and full definitions | Unknown selector/detail, conflicting selectors, oversized response | Field necessity; task tokens/calls; parser/schema checks; no network/child calls |
| Inspection | Find needed fields/types and actual version with minimal context | 1,000 columns, nesting, long UTF-8 names, large metadata, exact 4 KiB boundary | Oversized requested detail; metadata accessible/data denied | Exact fields/check scope; no unrelated schema; IO trace; complete recovery cost |
| Read coverage | Scan, COUNT, GROUP BY, ORDER BY, CTE, join | Null/empty/zero-column/DV/pinned updates | Write/external-source attempts | Independent exact-result oracle; no write side effects |
| Small results | Interpret the requested sample or complete result from necessary typed JSON | Large ints, decimals, timezones, binary, nesting, duplicates, dictionary partitions, exact 8 KiB boundary | Oversized schema/value; unsupported type | Exact values/types; no optional metadata; total task cost; sample versus complete result |
| Resources/completion | Complete query, labeled preview | Exact limit versus one extra row | Memory/disk/deadline/output exhaustion | Bounded resources; no false completion |
| Recovery | Correct input from relevant details and an actionable next step | Parse/planning distinction; wrong column/type in a wide table; unknown causes | Expiry/denial/transient failures | Follow hints without full dumps; stable exits; tokens/calls/retries and response ceilings |
| Confidentiality | Reference reaches intended identity | Malicious data/provider errors | Credential loading/refresh failure | Secret canaries absent from calls, diagnostics, manifests, child args |
| Auth predictability | Each advertised source authenticates | Aliases/partial families/two identities | Selected-source failure | Signed-request/provider assertions; zero fallback |
| Automation | Non-TTY/piped/file use | Blocked output, PID 1, platform signals | Broken pipe/hung child/cancellation | Deadline-plus-grace, reaping, temp cleanup |
| Integrity/replay | Save, reopen and replay a pinned result from a minimal receipt and focused artifact reads | Concurrent updates/order ties/destination races | Midstream/disk/publication/hash failure | Independent checks of exact versions/rows/digest/counts; no-clobber; no full manifest/result in context |

Run eight agent tasks: first read from discovery/auth reference; repair predicate
and SQL input; multi-table analysis; expiry/denial recovery; large/long-cell
output; cancellation/midstream failure; adversarial result text; pin/save/replay.
Declare the harness version/configuration, exact model/revision/settings,
artifact/fixture hashes, and prompts before running. Use fresh context/workspace
for five attempts per task. Do not choose a model after seeing scores.

Give the operator the active shell, available readers, required evidence paths,
and the declared safeguards, including cumulative model tokens across calls. State
which outputs the harness saves automatically, so the operator need not create
duplicate reports. Shell and artifact-handling instructions may explain the
environment; they must not supply complete CLI commands, corrected inputs, or
expected results. Apply the same instructions to both compared versions. If the harness
or prompts change, freeze and rerun both versions; retain earlier failures and
report the changed conditions.

Pass requires 5/5 for every task, with zero credential leaks, instruction
execution from data, broader-identity fallback, or partial/sample false success.
Calibrate elapsed-time, model-token and capture-storage safeguards against the
complete baseline workflow, with headroom for ordinary variation. Freeze them
before comparing versions. Count shell calls and repair retries as costs, not as
universal task cutoffs. A runner safeguard protects evaluation resources; an
interrupted attempt remains incomplete with all observed costs retained. It does
not establish a CLI defect or the cost of finishing the task. CLI operation limits
and task-specific authorization and resource constraints still apply. Acceptance
requires token accounting; exploratory runs without it cannot establish a pass.
Record all outcomes, calls, bytes, tokens, CLI/provider/corrective retries,
latency, and artifacts. Separate deterministic guarantees from the observed
agent results; do not claim all models/deployments behave alike.
The wide-data task includes at least 1,000 columns, deep nesting, large metadata,
a long UTF-8 value, and repair of a wrong column or literal type. Record stdout
and stderr bytes for every invocation through discovery, execution, recovery,
saved-result use, and applicable continuation. Identify each response
kind, its applicable limit, and any explicit full expansion or budget increase.
Check both individual limits and the declared cumulative safeguards, including
failed attempts and retries. Count bytes before harness truncation; clipping a
tool response cannot establish a pass. Ordinary tasks must be achievable without
full-schema retrieval or printing complete results/manifests into context.
Discovery-only traversal and small-table success do not satisfy these checks.

Compare proposed responses with the current bounded interface on tasks both
support. Fix both artifact versions, the model/settings, prompts, fixtures, and
safeguards before evaluation. The full inventory is a secondary comparison;
beating it alone does not establish an improvement. Let the agent choose its
calls without supplied finished commands or manual repair. Include saving,
reopening and reusing results when the business task needs them. Ordinary CLI
acceptance does not require a forced context reset or a restricted handoff that
discards otherwise available task information. Test actual compaction separately
only for an explicit cross-context requirement, declaring what is retained and
how that experiment answers the requirement. A compaction experiment does not
gate unrelated CLI business or disclosure acceptance.

This follows Anthropic's
[task-level tool evaluation guidance](https://www.anthropic.com/research/writing-tools-for-agents).
Byte-ceiling, parser, and scripted workflow tests alone cannot establish a pass
for these agent trials.

## Agent trajectory review protocol

Apply this protocol to the feature's affected success, interpretation, and
recovery paths. Broader business workflows need their own runs. A separately
scoped compaction experiment is required only for a cross-context claim.
An earlier issue's closure or a passing code review does not
establish these results for a new artifact.

1. **Freeze the comparison before running.** Save a run manifest with baseline
   and candidate executable hashes, source revisions, fixture hashes, exact
   prompts, model/settings, harness version, allowed tools, safeguards, task
   checks, run order, and stopping rule. Keep the original baseline when repairing
   a regression; beating only the regressed version is insufficient. Use five
   attempts per variant and scenario for acceptance. Keep all attempts; a changed
   prompt or executable starts a new comparison, preserving the old records.
   Calibrate resource safeguards on the complete baseline scenario before
   freezing, including both parts of A+B. Allow ordinary variation; count calls
   and retries without imposing a shared fixed call ceiling on different tasks.
   Any revised safeguard applies equally to new runs of both variants; it cannot
   regrade an earlier interrupted attempt.
2. **Cover isolated and continued work.** For a disclosure change, define a
   focused task A and a task B that needs the deferred information. Run A alone,
   B with fresh context, and A followed by B with context retained. Report each
   scenario separately; do not count A and its continuation as independent runs.
   These are complete tasks, not prescribed command sequences. Add applicable
   failure repair and boundary cases. Use independent expected-result checks,
   including exact values, authorization, and completion state. Compare each
   scenario under the same declared safeguards for both variants. A+B needs
   calibration for the whole workflow; it does not inherit A's single-task
   allowance. Keep its complete cost and cumulative counters across both parts.
   Do not add forced compaction to ordinary business tasks.
3. **Let fresh consumers choose the route.** Give each variant the same task,
   executable and fixture references, tools, and safeguards. Exclude source,
   implementation discussion, other trials, expected answers, and suggested
   repairs. For a recipe evaluation, include only the published recipe and record
   it. Balance variant order and keep evaluative labels out of consumer prompts.
   Human rescue is a recorded failure of unaided completion, not a successful
   retry. A continuation keeps its own context; a new B trial does not.
   Only for explicitly selected batching and file-readback recipe trials,
   publish and freeze this task guidance: "Batch independent discovery calls in
   one tool request. Read task-required values from the saved file; this may
   be in the same script that saves it." Include an uncoached condition and report
   this guidance separately from CLI changes; ordinary CLI tasks do not inherit
   recipe instructions.
4. **Capture what was returned and what was seen.** Save chronological requests,
   arguments or scripts, exits, raw stdout/stderr, tool errors, artifact reads,
   projections, generated code, retries, and the exact content delivered to the
   model. Reference immutable raw records from the review ledger below. Record
   truncation and filtering; they cannot establish that the CLI response itself
   is concise. Do not ask consumers to duplicate logs already captured by the
   harness. Their statements about delivery supplement, but do not replace, a
   harness transcript. Protect credential contents; use controlled canaries and
   references rather than recording real secrets.
5. **Review decisions, then test removals.** A separate reviewer follows every
   step through to the checked result. For each field or coherent explanation,
   identify the decision or verification it supports now and any later reuse.
   Mark unused, repeated, or unclear content as a candidate, not a proven deletion.
   Compare one coherent omission, deferral, or grouping change on the same A, B,
   and A+B tasks with the same safeguards. Include new discovery, request syntax,
   repeated framing, repair, and artifact work in its cost. Keep short related
   rules together when that reduces total task cost without losing meaning.
   A hypothetical future use or a byte-ceiling pass is not evidence of necessity.
6. **Make separate judgments.** Mark each verdict below `pass`, `fail`, or
   `insufficient evidence`, naming the artifacts and scenarios. Include raw-record
   references, per-run results, median and range, and unresolved cases. Do not
   hide a mixed-task regression in an average of cheaper focused tasks. A measured
   tradeoff is not an unqualified efficiency pass. Fix the regression or state
   which task costs more and why the chosen default is justified. Five runs
   describe these trials; they do not prove a universal minimum.

Use one ledger row per invocation, with linked notes for individual explanations:

| Step | Request, exit, raw and delivered records | Information used now or reused at a later step | Unused, repeated, or unclear information | Incremental and cumulative cost |
| --- | --- | --- | --- | --- |
| Run/step ID | Exact arguments or script; record references | Field/clause, decision or check, consuming step ID | Candidate and counter-task that could need it | Requests, delivered text, retries; model usage when available |

For example, a date rule unused in A but used in B is later-used information in
A+B. Removing it from A is a deferral whose added lookup must be measured. If the
agent still solves B, that proves recoverability, not that the deferral is cheaper.

Keep three cost measurements separate: raw CLI stdout/stderr; all model-visible
tool request/result text, including scripts and artifact reads; and provider
model input/output usage across the entire task. Record the tokenizer for text
estimates. Count cached input within input usage without adding it twice; report
reasoning/cache details separately when the provider exposes them. Include
failed attempts, repeated context, final answers, and compaction calls in model
usage. Report peak context separately from cumulative tokens. Record latency and
CLI/provider retries. Count CLI process launches, shell invocations, outer tool
requests, and model responses separately: one outer request can contain several
shell invocations, and one shell invocation can launch several CLI processes.
Count executed calls from invocation/result records, including loop and batch
expansion; occurrences of a tool name in a script are not a call count.
Never label CLI calls as model turns or output-token estimates as total usage.
Collect per-response usage from the harness, deduplicate response IDs, and
reconcile their sum with cumulative thread usage. Do not sum cumulative snapshots.
Missing telemetry is unknown, not zero; missing delivery records prevent a full
context review. A simulated handoff is not an actual compaction trial.

Retain whole-task provider totals and distinguish direct CLI output,
interface-induced work supported by request/result traces, evaluator-added
demands, discretionary agent work, and mixed or unattributable work. Work outside
the CLI process, including scripts and artifact reads, can be interface-induced;
process identity alone does not establish attribution. Record the supporting
trace and uncertainty. One trajectory, conjecture about model reasoning, or a
comparison of model-effort settings cannot establish interface cost causality.
Use comparisons with the same model/settings and task conditions to test an
interface's effect. Token categories describe the observed run; subtracting them
does not produce a counterfactual total for a prompt without extra demands.

Validate the review method itself against retained regression and control traces.
It must flag a known increase in cumulative CLI or delivered-tool text even when
model totals decrease, and must not report that increase as a model-token
regression. Keep any budget, repetition, or delivery failures visible. Replaying
these records checks the detector; it does not replace fresh acceptance runs.

| Verdict | Required evidence |
| --- | --- |
| Task correctness | All five attempts satisfy independent checks with no credential leak, instruction execution from data, broader-identity fallback, or false completion. |
| Information sufficiency and necessity | Decisions and later reuse are traceable; tested omissions/groupings retain required semantics and do not force avoidable guesses, repair, or fragmented retrieval. Untested candidates remain unresolved. |
| Whole-task efficiency | Comparable complete runs include all work and provider token usage. Report each scenario's cost and spread; fewer tokens in one response cannot pass this verdict when the full task regresses. Missing telemetry leaves this verdict unestablished, while narrower CLI-text measurements remain reportable. |

Publish the run manifest, raw-record references, ledger, counterfactual results,
and verdict table in the evaluation comment or linked artifacts. Keep the PR
description focused on behavior and examples. Historical runs retain their
original conditions; replay or a newly completed ledger is not a fresh trial.

A retrospective re-audit is supplementary evidence. Preserve the original
prompts, raw evidence, conditions, and ratings alongside separately labeled
revised judgments with supporting record references. Complete retained evidence
can satisfy a revised observable-result criterion; applicable repetition and
safety requirements still hold, and untested cases remain untested. Do not
silently overwrite original ratings or relabel historical runs as fresh trials.
A workflow or cost claim for tasks without extra verification instructions needs
fresh comparisons with revised prompts frozen in advance only when the original
prompt demands confound that inference. Rerun claims the retained evidence cannot
support.

When editing this contract, check local links and examples, then run the
[strict documentation build](../README.md#site-content).
