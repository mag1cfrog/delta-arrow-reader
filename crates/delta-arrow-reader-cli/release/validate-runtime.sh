#!/usr/bin/env bash
# Mount /checks, /artifacts, /package, and the Debian-built /tools/xtask read-only.
set -euo pipefail
: "${CLI_ARCHITECTURE:?}" "${CLI_VERSION:?}" "${CLI_SOURCE_COMMIT:?}" "${CLI_RUST_TOOLCHAIN:?}"

. /etc/os-release
case "$ID" in
  debian|ubuntu)
    apt-get update
    apt-get install -y --no-install-recommends ca-certificates libc6 libgcc-s1
    dpkg-query -W -f '${Package}=${Version}\n' ca-certificates libc6 libgcc-s1
    ;;
  fedora)
    dnf install -y --setopt=install_weak_deps=False ca-certificates glibc libgcc
    rpm -q ca-certificates glibc libgcc
    ;;
  *) echo "Unsupported acceptance image: $ID" >&2; exit 1 ;;
esac

mkdir /tmp/cli-runtime-checks
cd /tmp/cli-runtime-checks
test "$(/package/dar --version)" = "dar $CLI_VERSION"
cli_fixture=/checks/tests/reader/fixtures/external_writer/corpus/partitioned/table
/package/dar inspect "$cli_fixture" >inspect.json 2>inspect.stderr
test ! -s inspect.stderr
/package/dar scan --column id --limit 2 "$cli_fixture" >scan.arrow 2>scan.stderr
test ! -s scan.stderr

case "$ID" in
  debian|ubuntu) apt-get install -y --no-install-recommends binutils build-essential openssl ;;
  fedora) dnf install -y --setopt=install_weak_deps=False binutils gcc openssl ;;
esac
/tools/xtask cli test-archive /artifacts \
  --extracted /package --smoke-output /tmp/cli-runtime-checks --repository /checks \
  --architecture "$CLI_ARCHITECTURE" --version "$CLI_VERSION" \
  --source-commit "$CLI_SOURCE_COMMIT" --rust-toolchain "$CLI_RUST_TOOLCHAIN"
