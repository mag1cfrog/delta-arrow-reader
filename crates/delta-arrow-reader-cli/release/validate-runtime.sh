#!/usr/bin/env bash
# Run inside a fresh pinned Debian container with read-only /checks, /artifacts, /package.
set -euo pipefail
: "${CLI_ARCHITECTURE:?}" "${CLI_VERSION:?}" "${CLI_SOURCE_COMMIT:?}" "${CLI_RUST_TOOLCHAIN:?}"

apt-get update
apt-get install -y --no-install-recommends ca-certificates libc6 libgcc-s1
dpkg-query -W -f '${Package}=${Version}\n' ca-certificates libc6 libgcc-s1

mkdir /tmp/cli-runtime-checks
cd /tmp/cli-runtime-checks
test "$(/package/dar --version)" = "dar $CLI_VERSION"
cli_fixture=/checks/tests/reader/fixtures/external_writer/corpus/partitioned/table
/package/dar inspect "$cli_fixture" >inspect.json 2>inspect.stderr
test ! -s inspect.stderr
/package/dar scan --column id --limit 2 "$cli_fixture" >scan.arrow 2>scan.stderr
test ! -s scan.stderr

apt-get install -y --no-install-recommends \
  binutils build-essential openssl python3 python3-venv
python3 -m venv /tmp/cli-runtime-venv
/tmp/cli-runtime-venv/bin/python -m pip install --only-binary=:all: 'pyarrow==25.0.1'
/tmp/cli-runtime-venv/bin/python /checks/crates/delta-arrow-reader-cli/release/run_acceptance.py \
  /artifacts --extracted /package --smoke-output /tmp/cli-runtime-checks \
  --architecture "$CLI_ARCHITECTURE" --version "$CLI_VERSION" \
  --source-commit "$CLI_SOURCE_COMMIT" --rust-toolchain "$CLI_RUST_TOOLCHAIN"
