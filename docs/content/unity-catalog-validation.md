# Validate Unity Catalog reads

For maintainers checking source or packaged CLI behavior. To read your own table,
use the [Unity Catalog user guide](../../crates/delta-arrow-reader-core/docs/public/unity-catalog.md).

| Check | What it establishes | Needs a cloud account? |
| --- | --- | --- |
| [Offline harness tests](#check-the-harness-offline) | Reference validation and report behavior | No |
| [Packaged executable tests](#check-a-packaged-executable) | UC query, join, and renewal behavior against simulated services | No |
| [Prepared live fixture](#prepare-a-live-fixture) | Real authorization and exact results for one fixture and executable | Yes |

Run the Bash commands below from a source checkout matching the executable under
test, with Rust and Cargo available. A test against simulated services does not establish
workspace permissions or cloud compatibility. The
[Azure validation record](../../crates/delta-arrow-reader-cli/release/uc-azure-validation-2026-10-22.md)
records the completed live runs and their coverage gaps.

## Check the harness offline

The opt-in harness checks inspection, a filtered projection,
a local SQL aggregate, and a join between UC and local Delta tables against
independent Arrow reference files. CI runs the same offline validator and harness
tests on Linux, macOS, and Windows. Run both checks without a Databricks
installation or account:

```sh
cargo test --locked -p xtask --bin xtask scan::ipc::
cargo test --locked -p xtask --test unity_catalog_live
```

To check the same validators against a built CLI and the local Spark corpus:

```sh
DAR_LOCAL_BINARY=/absolute/path/to/dar \
  cargo test --locked -p xtask --test unity_catalog_live \
  harness_checks_a_real_cli_against_a_local_delta_fixture -- --ignored
```

This self-check redirects UC reads to a local Delta fixture, joins a second
local fixture at a different snapshot version, and uses a fake Databricks
version command. It does not validate cloud authorization. Repeat
with `DAR_LOCAL_BINARY` pointing to the wheel's installed `dar`.

## Check a packaged executable

On Unix, run the UC query fixtures against an extracted archive or installed
wheel. From the source checkout that matches the package, set an absolute path
to the executable:

```sh
DAR_TEST_UC_BINARY=/absolute/path/to/dar \
  cargo test --locked -p delta-arrow-reader-cli --test cli -- \
  unity_catalog::queries unity_catalog::table_manifests
```

Repeat with `DAR_TEST_UC_BINARY` pointing to the wheel's installed
`dar` executable. These tests use temporary Databricks CLI and
storage fixtures; they need no login or cloud account. They check exact SQL
results, mixed joins, profile isolation, renewal, and redacted authorization
failures for S3, R2, GCS, Azure SAS, and Azure bearer credentials. Linux also
checks the production 30-second child timeout and process cleanup.

`DAR_TEST_UC_BINARY` affects only the UC test fixtures. Omitting it tests the
Cargo-built executable. These checks supplement the archive and wheel runtime
acceptance tests in the [release tools](../../crates/delta-arrow-reader-cli/release/README.md).
They do not establish access to a real UC table or replace platform compatibility checks.

## Prepare a live fixture

For an authorized live run, have the fixture administrator prepare a synthetic
UC table and a local Delta table, each at a fixed snapshot. Prepare four Arrow
IPC **files** (not streams): the full UC table, the filtered projection, the
aggregate result, and the join result. Produce the references independently of
DAR, for example with Spark. Keep both tables and each SQL result at 1,000 rows
or fewer. The scan must return at least one row, fewer rows than the full UC
table, and fewer columns than its full schema. Both SQL results must contain
at least one row and one column. Each input file must be at most 16 MiB; SQL
files must be UTF-8 and at most 64 KiB.

Reference field names, Arrow types, and nullability must match the command's
output. Use the full table's fields for the filtered scan reference: filtering
may narrow Spark's nullability while DAR preserves the table schema. SQL engines
can also produce different Arrow string types for equal values, such as `string`
and `string_view`. Choose compatible output types and document any reference
schema adjustment without changing row values. The harness ignores field metadata
for top-level fields and nested structs, but does not coerce types or nullability.

Save a private configuration alongside those files:

```json
{
  "databricks_profile": "uc-fixture",
  "table": "uc://test_catalog.test_schema.reader_fixture",
  "table_version": 0,
  "expected_table": "table.arrow",
  "scan": {
    "columns": ["value"],
    "predicate_file": "predicate.json",
    "expected_result": "filtered.arrow"
  },
  "sql": {
    "aggregate": {
      "sql_file": "aggregate.sql",
      "expected_result": "aggregate.arrow"
    },
    "join": {
      "sql_file": "join.sql",
      "expected_result": "join.arrow"
    },
    "local_table": "local-delta-table",
    "local_table_version": 0
  },
  "context": {
    "credential_form": "azure_blob_sas",
    "auth_mode": "user_oauth",
    "table_kind": "managed",
    "protocol": {
      "min_reader_version": 1,
      "min_writer_version": 2,
      "reader_features": []
    }
  }
}
```

Write the aggregate over `uc_rows`. Write the join over `uc_rows` and
`local_rows`, and include values from both tables in the expected result. For
tables with `id` and `value` columns, the SQL files could contain:

```sql
-- aggregate.sql
SELECT COUNT(*) AS row_count, SUM(value) AS total FROM uc_rows;

-- join.sql
SELECT u.id, u.value AS uc_value, l.value AS local_value
FROM uc_rows u JOIN local_rows l ON u.id = l.id;
```

Each file contains one read-only query. Choose queries and references that
exercise the prepared fixture; the harness checks their output but does not
classify the SQL as an aggregate or join. It creates table manifests using the
configured profile, storage options, and snapshot versions. All SQL execution
uses DAR's local compute. Reference preparation is a separate administrator step.

Paths inside the configuration are relative to that file. `predicate_file` uses
the [CLI predicate format](../../crates/delta-arrow-reader-core/docs/public/reference/cli-inputs.md#filter-rows);
`storage_options_file` uses the [storage options format](../../crates/delta-arrow-reader-core/docs/public/reference/cli-authorization.md#storage-options-file).
For an S3 fixture, add
`"storage_options_file":"storage.json"` to the configuration,
and put `{"aws_region":"us-west-2"}` in that file. Omit the option when no
overrides are needed. Use the
[UC prerequisites and read options](../../crates/delta-arrow-reader-core/docs/public/unity-catalog.md#before-you-start)
to prepare the profile and permissions. The harness does not provision data,
change grants, or log in. Databricks CLI must already be available on `PATH` in
the same environment as the executable being tested.

The credential labels are `s3_session`, `azure_blob_sas`, `azure_blob_bearer`,
`gcs_oauth`, and `r2_session`. Authentication labels are `user_oauth`,
`service_principal_oauth`, and `pat`; table kinds are `managed` and `external`.
Copy the protocol version numbers and reader features from the prepared snapshot.
The report labels these properties as `declared_fixture_context`: the harness
does not independently identify the credential form or authentication mode.

## Run a live check

Run one prepared fixture with the commit recorded in the binary's build provenance:

```sh
cargo xtask cli test-unity-catalog \
  --fixture /private/fixtures/uc-fixture.json \
  --binary /absolute/path/to/dar \
  --dar-revision FULL_40_CHARACTER_BUILD_COMMIT > uc-validation.json
```

Omitting all options emits a `skipped` report without invoking either CLI.
Configured runs record the declared build commit, executable SHA256, Databricks
CLI version, fixture context, both snapshot versions, and check results. Failure
returns a nonzero exit status and identifies the failed step. Reports omit profile names,
table identifiers, schemas, row values, tokens, and subprocess output. Keep the
private configuration and reference files out of published evidence.

The report scope is `inspect_scan_and_sql`. A passing report requires the expected
schemas and exact result rows, including duplicate and null values. Row and batch
order may differ. Each command must exit successfully with one matching completion
record; scan and query output must be complete Arrow streams with matching row,
batch, byte, and snapshot fields. The scan stops at one more row than expected,
and passing requires exhaustion before that limit. Each SQL query is capped at
its expected row count and 16 MiB of IPC output; excess output fails the check.
The first failure stops the run and leaves later checks as `not_run`. A passing
report covers only that fixture and executable, not other clouds or auth modes.

## Compare environments and retain evidence

Use the same prepared snapshots, SQL, and independent reference files when
comparing source builds, installed wheels, and extracted archives. Build each
executable for its target OS and CPU. Select artifacts from the
[supported distributions](../../crates/delta-arrow-reader-core/docs/public/installation.md);
standalone release archives are Linux-only.

The Databricks CLI and selected profile must work in each execution environment.
A host login is not automatically available inside a container. Keep private
fixture inputs together; their relative paths allow the directory to be mounted
read-only in another environment. Do not publish login files or raw grants.

Save a separate report and exit status for every executable. Alongside it, record:

- Source revision, executable/package hashes, and build profile.
- OS, CPU, toolchain, Databricks CLI version, and any container image or CPU emulation.
- Fixture snapshot versions, reference provenance, and observed authorization type.
- Untested platforms, artifacts, storage backends, auth modes, and renewal paths,
  with a reason for each `not_run` result.

The report's `cloud` describes storage, not the workspace's deployment cloud.
A passing report covers only its declared fixture and executable. In particular,
a short run may not exercise credential renewal. Keep observed authorization
separate from operator-supplied labels, and keep earlier failures separate from
passing reports. The [Azure record](../../crates/delta-arrow-reader-cli/release/uc-azure-validation-2026-10-22.md)
shows how to retain those distinctions without publishing private identifiers.
