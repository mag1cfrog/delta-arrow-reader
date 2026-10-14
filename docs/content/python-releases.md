# Publish Python wheels

This guide is for maintainers releasing `delta-arrow-reader` wheels containing
both the Python API and native CLI. Complete the
[first-release checklist in #101](https://github.com/mag1cfrog/delta-arrow-reader/issues/101)
before the first PyPI publication, including TestPyPI rehearsal and maintainer
authorization. Track the first public combined-package installation checks in
#101 and the [CLI parent issue #428](https://github.com/mag1cfrog/delta-arrow-reader/issues/428).
Merging the publishing workflow does not complete that checklist.

The commands use a Linux or macOS shell and an authenticated GitHub CLI. Replace
`X.Y.Z` with the exact stable version in the selected commit's root `Cargo.toml`.
For supported installation targets, see the
[Python installation guide](../../crates/delta-arrow-reader-core/docs/public/installation.md#python).

## Configure the publishers

Confirm that the maintainer can use the `delta-arrow-reader` project name on both
indexes. TestPyPI needs its own account; it has a
[separate database from PyPI](https://packaging.python.org/en/latest/guides/using-testpypi/).
Resolve a name conflict with the maintainer before changing the package name.

In the repository's Settings > Environments, create `testpypi-release` and
`pypi-release`. Set required reviewers on `pypi-release` before the first
publication and allow `v*` tags in its deployment rules. Allow the intended
rehearsal branch or tag on `testpypi-release`.

Create a `release-checks` environment shared by the Python and CLI release PR
checks. Add the maintainer as a required reviewer, allow the branch pattern
`refs/pull/*/merge`, and disable administrator bypass. Allow self-review so the
maintainer can approve a run they restart. This environment needs no secrets
or PyPI publisher registration; it approves building and testing both artifact
sets.

Register a separate GitHub Actions Trusted Publisher on each index. Use these
values, including the environment field:

| Publisher field | TestPyPI | PyPI |
| --- | --- | --- |
| Project name | `delta-arrow-reader` | `delta-arrow-reader` |
| Repository owner | `mag1cfrog` | `mag1cfrog` |
| Repository name | `delta-arrow-reader` | `delta-arrow-reader` |
| Workflow filename | `pypi.yml` | `pypi.yml` |
| Environment name | `testpypi-release` | `pypi-release` |

For an existing project, use its
[Publishing settings](https://docs.pypi.org/trusted-publishers/adding-a-publisher/).
For a new project, add a
[pending publisher](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/)
under the account's Publishing settings. A pending publisher does not reserve
the project name; the first successful publication creates the project.

The workflow uses OIDC through `pypa/gh-action-pypi-publish`, with `id-token: write`
only on its two upload jobs. It needs no PyPI password or long-lived API token.
Keep the existing release-plz GitHub App credentials, `APP_ID` and
`APP_PRIVATE_KEY`: the App's GitHub release event starts Python publishing.

## Build without uploading

After `pypi.yml` is available on the default branch, dispatch the `Python release`
workflow. Set `PYTHON_RELEASE_REF` to a branch containing the workflow or to the
matching `vX.Y.Z` tag. Every mode checks the selected commit and Cargo version;
a selected tag must also have the matching name and point to that commit.

```console
PYTHON_RELEASE_VERSION=X.Y.Z
PYTHON_RELEASE_REF=main
gh workflow run pypi.yml --repo mag1cfrog/delta-arrow-reader \
  --ref "$PYTHON_RELEASE_REF" \
  -f publish-to=none -f expected-version="$PYTHON_RELEASE_VERSION"
gh run list --repo mag1cfrog/delta-arrow-reader --workflow pypi.yml --limit 5
gh run watch RUN_ID --repo mag1cfrog/delta-arrow-reader --exit-status
```

Replace `RUN_ID` with the dispatched run's ID. The default mode, `none`, builds
and validates all three wheels without running either upload job. Require
`Require validated wheels` to pass; individual platform artifacts can exist even
when the complete build failed. The
[wheel build guide](contributing.md#build-python-wheels-in-ci) describes the
platform artifacts and installed checks. Save the passing run URL and source
commit in #101's checklist. For each platform, record the wheel filename and
SHA256, artifact ID, Python and PyArrow versions, and results for pip, uv tool,
and uvx. The checks must use the final repaired wheel, including its native CLI.
This build-only rehearsal does not authorize PyPI publication.

## Rehearse on TestPyPI

After configuring the TestPyPI publisher, dispatch the same workflow with
`publish-to=testpypi` and the selected ref's exact Cargo version:

```console
gh workflow run pypi.yml --repo mag1cfrog/delta-arrow-reader \
  --ref "$PYTHON_RELEASE_REF" \
  -f publish-to=testpypi -f expected-version="$PYTHON_RELEASE_VERSION"
```

This run builds and tests its own complete artifact set, then uploads through
`testpypi-release`. Wait for `Publish to TestPyPI` to pass before checking the
installed package.

Use a checkout of the commit that the run built. From its repository root,
create a fresh environment outside the checkout. Install dependencies from
PyPI, then install only the exact reader version from TestPyPI with `--no-deps`:

```console
PYTHON_RELEASE_REPO="$(git rev-parse --show-toplevel)"
PYTHON_RELEASE_CHECK="$(mktemp -d)"
python3 -m venv "$PYTHON_RELEASE_CHECK/venv"
. "$PYTHON_RELEASE_CHECK/venv/bin/activate"
cd "$PYTHON_RELEASE_CHECK"
python -m pip --isolated install --only-binary=:all: \
  --index-url https://pypi.org/simple/ 'pyarrow>=18.0.0'
python -m pip --isolated install --no-cache-dir --only-binary=:all: --no-deps \
  --index-url https://test.pypi.org/simple/ "delta-arrow-reader==$PYTHON_RELEASE_VERSION"
python -m pip check
python -I - "$PYTHON_RELEASE_VERSION" <<'PY'
from importlib.metadata import version
import sys
import delta_arrow_reader

assert version("delta-arrow-reader") == delta_arrow_reader.__version__ == sys.argv[1]
print(delta_arrow_reader.__version__)
PY
test "$(delta-arrow-reader --version)" = "delta-arrow-reader $PYTHON_RELEASE_VERSION"
python -I -m unittest discover \
  -s "$PYTHON_RELEASE_REPO/crates/delta-arrow-reader-python/tests" -k installed
```

The installed checks cover package metadata, Python Arrow streams, and the
native CLI's version, scan results, diagnostics, closed stdout, and process
termination. They use the executable installed in this environment. Record the
exact version, source commit, workflow URL, TestPyPI version URL, wheel hashes,
and installation results in #101.

## Publish a stable release

Release-plz owns the shared workspace version, the core `CHANGELOG.md`, and the
`vX.Y.Z` tag and GitHub release. The Python crate is a git-only release-plz member;
it has no crates.io upload, separate changelog, tag, or GitHub release.

Before updating a release PR, the workflow checks the highest stable `vX.Y.Z`
tag. If that tag predates the Python crate, it prepares a temporary configuration
that treats Python as an initial release. Once a shared release tag contains the
Python crate, the workflow uses the normal configuration automatically.

Automatic release PRs require the repository variable
`RELEASE_PLZ_PR_ENABLED=true`. Review the version, lockfile, and changelog in that
PR. When ready to validate the release, open its CI run, choose
`Review deployments`, select `release-checks`, and click `Approve and deploy`.
This starts the three combined Python/CLI wheel builds and the separate
standalone CLI checks in parallel, without uploading to a package index.
The required `CI` check waits for successful approval and complete validation of
both artifact sets. Each wheel must pass the Python API and bundled CLI checks,
including pip, uv tool, and uvx acceptance. A failed, skipped, or cancelled
platform cannot produce a validated set. New commits to the PR require a new
approval and build.
The complete wheel bundle is retained for 90 days. Confirm it is still available
before merging; if it has expired, update the open release PR and approve a new
CI run first.

Merge the PR after its checks pass and publication is authorized. The
existing release-plz workflow publishes the core crate and its GitHub release.
A stable published release starts `pypi.yml`, which verifies the tag and commit
and retrieves the merged release PR's validated wheel bundle. It requires that
PR's latest CI run to have succeeded, matches the Git tree to the release tag,
and verifies the bundle's SHA256 and all three wheels. It then requests the
`pypi-release` environment. This path uploads the same final wheel bytes; it does
not build, repair, repackage, or repeat the installed acceptance suites. Missing,
expired, or mismatched artifacts stop publication without a fallback build.
Drafts, prereleases, and unrelated tags cannot upload.

If the automatic Python run did not start, a maintainer can dispatch it manually
for the existing stable tag. Set the version to that tag's Cargo version:

```console
PYTHON_RELEASE_VERSION=X.Y.Z
gh workflow run pypi.yml --repo mag1cfrog/delta-arrow-reader \
  --ref "v$PYTHON_RELEASE_VERSION" \
  -f publish-to=pypi -f expected-version="$PYTHON_RELEASE_VERSION"
```

Manual PyPI publication requires the matching version tag; a branch is rejected.
It uses the same release PR artifact checks as automatic publication.
If an upload already started, use the recovery steps below instead of starting
a new run. After publication, repeat the installation and acceptance checks in a
fresh environment, changing the reader's index URL to `https://pypi.org/simple/`.
Record the PyPI version URL and results in #101 before closing its checklist.
Also verify the exact public version with uv tool and uvx in separate tool
environments. Pin the package as `delta-arrow-reader==X.Y.Z` for uv tool, and
use `uvx --no-build --isolated --from delta-arrow-reader==X.Y.Z delta-arrow-reader --version`
for uvx. Both must expose `delta-arrow-reader`; neither installs the Python API
into a consumer's project environment. Use the
[installation guide](../../crates/delta-arrow-reader-core/docs/public/installation.md#python)
for the commands and record the tested platform and Python version.

### Check artifact selection without publishing

For an existing release tag whose commit belongs to a merged release-plz PR,
the selection command can download and validate the retained bundle without
uploading. Run it from a clean checkout of that tag with authenticated `gh`
access to the repository's Actions and pull requests. Set
`PYTHON_RELEASE_VERSION` to the tag's exact version:

```console
PYTHON_ARTIFACT_CHECK="$(mktemp -d)"
GITHUB_REPOSITORY=mag1cfrog/delta-arrow-reader \
GITHUB_OUTPUT="$PYTHON_ARTIFACT_CHECK/identity" \
  cargo xtask reuse-python-wheels "$PYTHON_ARTIFACT_CHECK/wheel-artifacts" \
    --expected-version "$PYTHON_RELEASE_VERSION"
cat "$PYTHON_ARTIFACT_CHECK/identity"
```

Record the returned `artifact-id` and `artifact-sha256`, the CI run URL, and
individual wheel hashes. A build-only run does not create a merged release PR
or its retained bundle, so it cannot satisfy this selection check. Do not merge
a release PR or publish a package solely to make the rehearsal pass.

## Recover an interrupted upload

Open the original `Python release` run. Confirm that preparation and
`Require validated wheels` succeeded and that only the selected upload job
failed. For PyPI, keep the release PR bundle identified in the preparation log;
for TestPyPI, keep the publishing run's three platform artifacts. Correct any
publisher/environment mismatch or wait for an index outage to end, then use
**Re-run failed jobs** on that run:

```console
gh run rerun RUN_ID --repo mag1cfrog/delta-arrow-reader --failed
gh run watch RUN_ID --repo mag1cfrog/delta-arrow-reader --exit-status
```

This reruns the failed upload job with the original artifact selection.
The PyPI job uses the artifact ID and SHA256 recorded during preparation, even
if someone has since rerun the release PR's CI.
Do not select **Re-run all jobs** or dispatch a new workflow to recover a partial
upload. GitHub keeps the original commit and ref for a rerun and
[allows reruns for up to 30 days](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/re-run-workflows-and-jobs).
The artifacts must still exist and be unexpired.

The upload job validates all three wheels again and compares the index's
published filenames and SHA256 hashes with those artifacts. It stages only
missing files; if every file already matches, the job succeeds without another
upload. A wheel without its CLI or dependency notices is rejected before the
index is contacted. It rejects unexpected or yanked files, hash mismatches,
malformed index responses, and network failures. Uploads to the same index and
version run one at a time, and `skip-existing` stays disabled.

Stop on a hash mismatch or if the original artifacts are unavailable. Corrected
binaries need a new shared version and tag. This includes adding a CLI to a
version already published with library-only wheels. Do not overwrite released
files, move the release tag, or bypass the hash checks.
