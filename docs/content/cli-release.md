# Publish and recover a CLI release

The CLI publisher copies the binary archives and OCI images already tested on
the release PR. It cannot rebuild them. The CLI's `vX.Y.Z` tag and executable
version match the Rust workspace version; the CLI crate stays unpublished on
crates.io. See [installation](../../crates/delta-arrow-reader/docs/public/installation.md#cli-binaries-and-container-images)
for the supported Linux runtimes and user commands.

## Approve the release PR

1. Open the trusted release-plz PR's current CI run. Review its version and
   changes, then approve the pending `release-checks` environment deployment.
2. Wait for both native binary builds, the Debian 12/Ubuntu 24.04/Fedora 44
   runtime matrix, both native image/Go checks, and OCI export/import checks.
3. Confirm **Record validated CLI artifacts** uploaded its record and the
   required **CI** check passed. Merge only that validated PR revision.

New PR commits cancel the previous CI run and require fresh approval and
validation. Ordinary feature PRs run source/tooling checks as needed, without
the heavy release matrix. The shared environment also gates Python validation;
the CLI publisher does not manage Python or Rust publication.

The complete binary artifact, complete image artifact, and validation record
are retained for 90 days. Intermediate architecture artifacts are not a complete
release set. The separate record contains:

| Identity | Purpose |
| --- | --- |
| Repository, release PR number, PR head SHA | Bind approval and results to the merged PR revision |
| Tested source SHA and Git tree, workspace version | Match the final release tree without requiring the temporary PR merge SHA to survive |
| Workflow run ID and attempt | Identify the successful approved run and job results |
| Binary/image artifact IDs and manifest hashes | Select the original complete files by immutable ID |

The publisher rechecks these claims against live GitHub metadata, including
approval and required job results. It rejects missing, expired, foreign,
ambiguous, or mismatched artifacts. A JSON file alone does not authorize a
release. The final merge SHA may differ from the tested merge candidate; its
tree, version, and merged PR revision must still match. Published manifests and
OCI labels retain the actual tested source SHA.

## Automatic publication

The [CLI release workflow](../../.github/workflows/cli-release.yml) accepts
`release: published` for a stable `vX.Y.Z` release in
`mag1cfrog/delta-arrow-reader`. It rejects draft/prerelease and unrelated tags,
versions without CLI code, and commits not reachable from `main`.

Release-plz creates the release using its GitHub App token. This allows the
release event to start a new workflow; releases created with the default
`GITHUB_TOKEN` would not trigger this downstream workflow. See GitHub's
[event triggering rules](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow).
The first actual automatic trigger must be recorded in the parent checklist
below. Do not edit or retag an older release to test publication.

| Job | Work | Token permissions beyond `contents: read` |
| --- | --- | --- |
| Prepare | Resolve the event, download exact original artifacts, verify inventories/hashes/OCI contents, rehearse uploads | `actions: read`, `pull-requests: read`, `packages: read` |
| Assets | Mark the CLI section pending and upload missing identical-plan release files | `contents: write`, `actions: read`, `pull-requests: read`, `packages: read` |
| Images | Import both original OCI archives, then publish the index | `packages: write`, `actions: read`, `pull-requests: read` |
| Delivery | Download all four GitHub files and anonymously fetch both images, checking their digests and executable bytes | `actions: read`, `pull-requests: read` |
| Notes | Mark the CLI section complete after delivery succeeds | `contents: write`, `actions: read`, `pull-requests: read` |
| Result | Require every publication stage, or confirm a read-only rehearsal | None |

Preparation compiles the `xtask` maintenance executable from the verified
release commit so recovery uses that release's embedded image baseline and
license data. It does not compile `dar`, build images, or run acceptance tests.
Write jobs receive this executable and the verified plan in an immutable
same-run artifact; they check hashes supplied separately by the preparation job
before execution. That temporary transfer artifact is retained for 7 days.
Recovery resolves the original 90-day validation artifacts again.

Only the assets and notes jobs can change the GitHub release. Only the images
job can write packages. Downloaded CLI executables and container contents are
inspected without execution in these jobs. Every writer rechecks the stable
tag and release identity. A per-version concurrency group queues retries
without canceling an upload already in progress.

## Published files and images

The GitHub release receives these exact original files:

```text
dar-vX.Y.Z-linux-amd64-gnu.tar.gz
dar-vX.Y.Z-linux-arm64-gnu.tar.gz
cli-artifacts.json
SHA256SUMS
```

The SHA256 checks cover both archives and their manifest. Delivery also compares
the checksum file itself with the verified original. GHCR receives
`ghcr.io/mag1cfrog/delta-arrow-reader:vX.Y.Z`, an OCI index for `linux/amd64` and
`linux/arm64`. The intermediate tags are `vX.Y.Z-linux-amd64` and
`vX.Y.Z-linux-arm64`. There are no mutable `latest`, major, or minor aliases.

The index is published only after both original platform manifests are present
at their expected digests. Delivery downloads each image anonymously by digest,
checks its configuration, and compares `dar`, `LICENSE`, and `NOTICE` with the
binary archive, including file modes. It does not repeat native, distro, or Go
acceptance tests.

The release title and text outside `<!-- dar-cli:start -->` and
`<!-- dar-cli:end -->` are preserved. The section records version, tested and
final SHAs, validation run/record identities, file hashes, support baseline,
and image digests. Its state comment pins the validation selection before the
first upload. Avoid editing release notes while publication runs: the updater
checks for concurrent edits before writing, but GitHub's body update is not an
atomic compare-and-swap operation.

## GHCR permissions and first public release

Publication uses the repository's job-scoped `GITHUB_TOKEN`, with
`packages: write` only in the image job. No new long-lived package credential is
needed. The GHCR package must permit this repository's Actions workflows to
write it. An existing package with the same name but different ownership or
access must be resolved before publication; authentication failures never mean
an image is absent.

New packages can start private even when the source repository is public.
If authenticated uploads succeed but the anonymous check fails, open the
`delta-arrow-reader` container package's **Package settings**, confirm the
repository connection/Actions access, and change its visibility to **Public**.
See GitHub's [container registry guide](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry).
Retry the same validation selection afterward. The workflow cannot report
completion while anonymous access fails.

After the first real release, record these items in
[parent issue #428](https://github.com/mag1cfrog/delta-arrow-reader/issues/428):

- The release-plz run that created the release and the automatically started
  CLI release run, showing the `release` event and stable tag.
- The validation run/attempt, record artifact ID, both complete artifact IDs,
  tested source SHA/tree, final release SHA, and workspace version.
- Links to all four GitHub release files and the successful delivery job.
- Public GHCR version reference, index digest, and both platform digests, with
  anonymous download verification and the completed CLI release section.

The implementation issue closes on the merged code and its tests. The parent's
first-release checklist remains open until this public-run evidence exists.

## Rehearse or recover publication

Use the original run and record artifact IDs from the workflow summary or
Release CLI section. Replace all three example values below. Dispatch from
`main`; feature-branch and foreign-repository dispatches are rejected.

```sh
gh workflow run cli-release.yml --repo mag1cfrog/delta-arrow-reader --ref main \
  -f mode=rehearse -f tag=vX.Y.Z \
  -f validation-run-id=VALIDATION_RUN_ID -f record-artifact-id=RECORD_ARTIFACT_ID
```

The rehearsal performs selection, file/OCI verification, and existing-content
checks with read-only credentials. It leaves GitHub assets, GHCR tags, and the
release body unchanged. Open that run in Actions, or use `gh run watch RUN_ID
--repo mag1cfrog/delta-arrow-reader --exit-status`. To publish or resume, dispatch
the same command with `-f mode=publish` and the same identities.

Automatic selection fails when several eligible records remain. It prints the
candidate IDs for explicit recovery selection; it never chooses the newest
artifact globally or falls back to current `main`. Recovery uses the selected
release commit's publisher, preserving its runtime image baseline.

| Existing state | Recovery |
| --- | --- |
| Missing asset or tag | Upload its already-validated bytes |
| Identical asset or manifest digest | Verify and reuse it |
| Some files uploaded, later stage failed | Retry the exact original run/record; leave existing content in place |
| Conflicting bytes/digest or incomplete `starter` asset | Stop; no overwrite, deletion, or retagging. Corrected binaries need a new version |
| Package private or access denied | Fix package visibility/access, then retry the same identities |
| Missing or expired original artifacts | Stop; publication cannot recreate the set |

Before any CLI bytes have been published, an operator may separately rerun the
original release-PR validation, where GitHub still allows it, with fresh
approval and every check for that same PR revision/tree/version. Explicitly
select the new record afterward. A pending section can change selection only
while all CLI release assets and all three image tags are absent. Once any CLI
content exists, retain the original selection. If its complete validated set
cannot be recovered, use a new release version. Never rebuild against a newer
base image or move a released tag.

GitHub and GHCR publication is not atomic. A failed run may leave valid partial
uploads and a pending CLI section. The workflow succeeds only after both public
delivery checks and the final notes update pass.

## Run the publisher commands locally

Prefer the workflow for its scoped credentials and concurrency control. For
diagnosis, use the verified release checkout, GitHub CLI read access, and the
tool versions listed in the [release tools reference](../../crates/delta-arrow-reader-cli/release/README.md).
Build the maintenance tool once, then prepare a new directory with explicit IDs:

```sh
cargo build --locked --package xtask
./target/debug/xtask cli plan-release target/cli-release-plan --tag vX.Y.Z \
  --run-id VALIDATION_RUN_ID --record-artifact-id RECORD_ARTIFACT_ID
```

Keep the plan SHA256 printed by this trusted preparation step. Do not derive a
new trusted hash from a directory received from an untrusted source. Set
`plan_sha256` to that printed value, then check existing content without writes:

```sh
plan_sha256=SHA256_FROM_PREPARATION
./target/debug/xtask cli update-release-notes target/cli-release-plan \
  --plan-sha256 "$plan_sha256" --status pending --dry-run
./target/debug/xtask cli upload-release-assets target/cli-release-plan \
  --plan-sha256 "$plan_sha256" --dry-run
./target/debug/xtask cli upload-release-images target/cli-release-plan \
  --plan-sha256 "$plan_sha256" --dry-run
```

For an authorized local recovery with appropriate credentials, run the same
three commands without `--dry-run`, in that order. Use only one publisher and
do not edit the body concurrently. Finish with the same delivery checks as CI:

```sh
set -e
./target/debug/xtask cli verify-release-assets target/cli-release-plan \
  --plan-sha256 "$plan_sha256"
./target/debug/xtask cli verify-release-images target/cli-release-plan \
  --plan-sha256 "$plan_sha256"
./target/debug/xtask cli update-release-notes target/cli-release-plan \
  --plan-sha256 "$plan_sha256" --status complete
```

The final command records the successful verification; invoke it only after
both preceding commands pass. It does not replace those checks. Image downloads
use isolated anonymous registry settings even if the local publisher is logged
in. See the tools reference for registry login and the separate upload/retry
contracts.
