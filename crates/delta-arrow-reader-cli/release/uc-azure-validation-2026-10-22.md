# Azure Unity Catalog live validation, 2026-10-22

The fixture operator reported passing inspection, filtered scans, local SQL
aggregation, and UC/local Delta joins on all four executables below. Each run used
real Unity Catalog authorization and Azure Blob / ADLS access. DAR read Delta data
and executed SQL locally; Databricks SQL prepared the fixture and independent
reference results.

This record preserves the operator's redacted report. The original JSON reports,
exit-status files, packages, and private fixture remain on the work computer and
are not included here. The reported hashes identify those artifacts; they were not
independently rehashed when this record was added.

## Tested revision and results

Tested source: [`353624b51becab479f55e2ceb98fef1bf1a90783`](https://github.com/mag1cfrog/delta-arrow-reader/commit/353624b51becab479f55e2ceb98fef1bf1a90783).
All runs used user OAuth, Azure user-delegation SAS (`azure_blob_sas`), managed
Delta snapshot 1, and local Delta snapshot 0. The UC protocol was reader version 1,
writer version 2, with no reader features.

| Executable | Build profile | Inspect | Scan | Aggregate | Join | Exit |
| --- | --- | --- | --- | --- | --- | --- |
| macOS ARM64 source | dev | passed | passed | passed | passed | 0 |
| macOS ARM64 installed wheel | dev | passed | passed | passed | passed | 0 |
| Linux ARM64 source | dev | passed | passed | passed | passed | 0 |
| Linux ARM64 extracted archive | release | passed | passed | passed | passed | 0 |

The starting revision, `c4f7d0a8499dc5210b6261c5dd189e867c468ff2`, failed live
inspection on both platforms because the table manifest omitted optional kind
fields. The tested fix accepts a manifest without those fields when it explicitly
advertises `HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT`. Explicit unsupported kinds,
row filters, column masks, pipeline tables, browse-only access, and unsupported
commit coordination remain rejected. Baseline failure reports were retained
separately from the passing runs.

## Environment and artifact identity

- macOS 26.6.2, ARM64.
- Debian GNU/Linux 12.15, aarch64, kernel 6.8.0-117-generic, in Colima 0.10.3 on
  the Mac. No CPU emulation was used.
- Rust 1.94.1 (`e408947bf`, 2026-03-25), Cargo 1.94.1, and Databricks CLI 1.15.0
  in both environments.
- Fixture tools: Python 3.12.14, PyArrow 26.0.0, delta-rs Python 1.6.6, and
  Databricks SDK 0.156.0. The macOS wheel was installed in a fresh Python 3.12
  environment using the documented build backend's dev profile.

The initial Linux source builder used
`rust:1.94.1-slim-bookworm@sha256:cf9dd0ec73e75f827fe59123fff9dc65af1a1c8363c3c31ee8d7f8ad0b6a5fb2`.
Fixed-revision builds, packaging, and live execution used the repository's pinned
base, `debian@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587`.
The resulting local image ID was
`sha256:791449877a69f628dadfdfcd7ab9fc4c86c29e90b86a8803e61666bff2c53c14`.
The operator verified the Databricks download against the official GitHub asset
digest and recorded the Dockerfile and Databricks binary hashes in
`container-image.json`.

| Executable | SHA256 |
| --- | --- |
| macOS ARM64 source | `e8e38fd2762d339161020d56b4d2fce82938d9d20e7630f80cd4d64f8ad0e4e7` |
| macOS ARM64 wheel executable | `e9ac36bf8d1b9ba8029efec8597cac0145509f889ced1bd286e0dde8cb317160` |
| Linux ARM64 source | `d7ff9eaf8e9b4141c675ad3ddf59b804c197da5c817f457d6cf3690368e0ee14` |
| Linux ARM64 archive executable | `bb31a72edfac9cff3e0f2e9e4e19542b38ef8665d1b172981c3dd2f109629884` |

| Package | SHA256 |
| --- | --- |
| `dar-v0.7.0-linux-arm64-gnu.tar.gz` | `bfc37c143f126b85bfe56219b1b5926382f99f3acdd59f66fe471e02098ce817` |
| `delta_arrow_reader-0.7.0-cp310-abi3-macosx_12_0_arm64.whl` | `81e34eb2cb738e2fa18615cc2760735824b6b1cc719001385adbc81091a1b5af` |

## Fixture and authorization

The fixture contained 8 UC rows, 4 filtered scan rows across 3 columns, 7 aggregate
rows, and 6 join rows, including nulls and duplicate values. Four Arrow IPC files
came from independent Databricks SQL results at the pinned UC version. The join
reference used independently read values from the pinned local Delta snapshot.
Each input was below 16 MiB and each SQL file below 64 KiB. Linux mounted the same
fixture directory read-only; the operator recorded matching content hashes in
`fixture-mount-verification.json` and `environment-and-fixture.json`.

Spark narrowed a filtered column's nullability. The scan reference retained that
field from Spark's full-table schema to match the scan's table-schema contract,
without changing values. An earlier aggregate matched values but used different
Arrow string representations (`string` versus DataFusion's `string_view`). The
final SQL references used numeric outputs; the scan covered strings and nulls.
Intermediate reference-layout failures were retained separately.

The operator identified SAS from the returned grant and independently read the
Azure Delta log with HTTP 200. Identity checks confirmed the same user on both
platforms. Linux used a separate read-only cache containing an exported short-lived
OAuth access token, without the host refresh token. These observations are
separate from the harness's operator-supplied `declared_fixture_context`.

Setup created a dedicated schema and managed table in an authorized, user-owned
development catalog, with the required catalog/schema use, schema external use,
and table select grants. External data access was already enabled. Existing Azure
storage configuration was preserved. The fixture and grants remain available for
reproduction; their identifiers and detailed change log are private.

## Checks and retained reports

The operator ran 13 IPC validator tests, 11 harness tests, and the real-CLI local
fixture self-check on each platform. The 16 parser/credential unit tests and the
20-test macOS UC integration/closed-connection suite passed, along with formatting
and CLI Clippy with warnings denied. Packaged executables ran both the local
self-check and live harness. Details remain in `fix-validation.json` and platform
offline reports.

The four final reports are `macos-arm64-source.json`, `macos-arm64-wheel.json`,
`linux-arm64-source.json`, and `linux-arm64-archive.json`, each with a matching
`.exit` file. `results-matrix.json` records results and gaps. They omit workspace
URLs, private table identifiers, authentication profile names, credentials, and
raw credential responses.

The operator removed three temporary containers, two build volumes, the exported
OAuth token, container authentication files, and raw temporary storage credential
responses. Tested binaries, packages, the local validation image, and the private
fixture were retained. `artifacts.json` records retained paths and `cleanup.json`
records cleanup. The existing Colima VM and host authentication were preserved.

## Unverified combinations

These combinations remain `not_run`; the passing runs do not establish the full
release matrix.

| Combination | Reason |
| --- | --- |
| Linux ARM64 wheel | Release wheels support Linux x86_64 only at this revision. |
| Linux x86_64 source, wheel, and archive | No x86_64 execution environment or artifact was prepared. |
| macOS release-profile wheel | The installed wheel used the backend's dev profile. |
| macOS standalone release archive | Standalone release archives are Linux-only. |
| Service-principal OAuth and PAT | No appropriate profile was available. |
| Azure bearer credentials | The workspace returned SAS credentials. |
| Storage credential renewal during a live command | The fixture completed before credential expiry; offline tests cover renewal separately. |
| OAuth renewal inside Linux | The container had an access token without a refresh token. |
| Encoded Azure storage paths | The managed table used generated paths. A local path with spaces and a plus sign does not cover Azure URL encoding. |
| External UC tables | The fixture was a managed Delta table. |
| String-valued SQL output | Final aggregate and join outputs were numeric; the scan included strings and nulls. |
| S3, GCS, and R2 live access | Deferred under the Azure-only acceptance scope. |
| Other OS, CPU, and Python versions; full release acceptance | Evidence covers the stated ARM64 environments and Python 3.12 wheel installation. |

Prerequisites were checked against the Databricks documentation for
[external data access](https://learn.microsoft.com/en-us/azure/databricks/external-access/admin),
[temporary table credentials](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/reference/temporary-table-credentials-commands),
and the [table API schema](https://docs.databricks.com/api/uc-tables/v1/get-table).
