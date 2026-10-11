# Read a Delta table from Go through the CLI

This example launches a standalone `dar` executable and decodes its Arrow IPC
stdout with the official [Arrow Go library](https://github.com/apache/arrow-go).
It needs Go 1.26 or newer to build and runs without cgo or a Rust toolchain.

The query selects the nullable `Int32` column `id` and reads up to 1,000 rows.
The example reports the row count and ID sum, and rejects null IDs. It keeps
only these two counters between batches. Change the projection, expected schema,
and batch callback in [main.go](main.go) for your application's query.

## Run against the fixture table

Set `DAR_BINARY` to the absolute path of an extracted, validated CLI binary for
your host architecture. From the repository root:

```sh
export DAR_BINARY=/absolute/path/to/dar
cd examples/go-cli
CGO_ENABLED=0 go run . \
  -dar "$DAR_BINARY" \
  -table ../../tests/reader/fixtures/external_writer/corpus/partitioned/table
```

The expected result is:

```json
{"rows":360,"id_sum":64980}
```

The timeout defaults to 30 seconds; set `-timeout 5s` to change it. `-help` lists
the available arguments. The table path or URL is passed as one argument after
`--`, so spaces and shell metacharacters remain part of the location.

## Copy the CLI into a service image

The [Dockerfile](Dockerfile) builds the Go application in a separate stage and
copies `dar`, its `LICENSE`, and its `NOTICE` from an explicit CLI image. The
final image contains the two executables, Debian runtime libraries, and CA
certificates. It contains no Go or Rust toolchain.

From the repository root, replace `RELEASE_INDEX_HASH` below with the index hash
from a completed GitHub Release's CLI section. The public index contains amd64
and arm64. Use `linux/arm64` instead of `linux/amd64` when building for arm64.

```sh
dar_image='ghcr.io/mag1cfrog/delta-arrow-reader@sha256:RELEASE_INDEX_HASH'
docker build --platform linux/amd64 \
  --build-arg DAR_IMAGE="$dar_image" \
  --tag dar-go-example examples/go-cli
docker run --rm --read-only --network none --cap-drop ALL \
  --mount type=bind,src="$PWD/tests/reader/fixtures/external_writer/corpus/partitioned/table",dst=/table,readonly \
  dar-go-example -dar /usr/local/bin/dar -table /table
```

For local development, `DAR_IMAGE` can be the matching local tag produced by
[`cargo xtask cli build-image`](../../crates/delta-arrow-reader-cli/release/README.md#build-a-local-container-image).
Use `linux/arm64` and an arm64 CLI image for the other architecture. The Go build
stage uses the host platform and cross-compiles the application when needed.
Run release validation on native runners for each target architecture.

The final image uses UID/GID `65532:65532`. Mounted tables and configuration
files must be readable by that user, including their parent directories. The
local fixture needs no network or writable root filesystem. Remote storage
needs network access and its usual runtime credentials. The child inherits the
service's environment; supply credentials at runtime and keep them out of build
arguments and layers. To pass a `dar` options file, add its flag and path as
separate elements of the argument slice in `summarizeTable`.

The CLI requires glibc 2.36 or newer, `libc6`, `libgcc-s1`, and CA certificates
on Debian. Its binary manifest records the exact shared-library requirements.
The consumer Dockerfile installs these packages on the pinned Debian 12 base.
Copying the executable into Alpine, `scratch`, or an older glibc image does not
provide those dependencies. Preserve `/usr/share/doc/dar/` when redistributing
the CLI with your service.

## Handle output and process completion

[`runScan`](scan.go) uses `exec.CommandContext` with separate arguments. It
checks the incoming schema before consuming batches, including an empty result.
The IPC reader owns each batch; the callback borrows it until returning. Copy
the values you need before the next batch, or pair an explicit `Retain` with
`Release` if your application needs to keep a batch.

A successful call requires both successful decoding and a zero child exit
status. IPC EOF alone can follow a partial scan. The example discards its
summary on any failure and prints it only after the child has been reaped.
Applications that perform external work inside the callback must account for
later scan failures before committing their results.

Returning an error from the callback stops consumption. Decoder errors, an
early stop, or context cancellation close stdout, cancel the child, and call
`Wait`. The reader is released on every path. Callbacks run synchronously and
should honor their context when performing blocking work. SIGINT and SIGTERM
cancel the example's scan context.

Stderr is drained separately from IPC stdout. At most 32 KiB is retained on
failure; further bytes are discarded without blocking the child. The example's
error text omits command arguments, stderr, and decoder details. Use `errors.Is`
for cancellation and `errors.As` for `*exec.ExitError` when classifying failures.
The private `scanError` retains the cause and bounded stderr for inspection;
these can contain sensitive data and are not printed by the example.

## Verify the example

From `examples/go-cli`:

```sh
CGO_ENABLED=0 go test -count=1 -timeout 60s ./...
DAR_BINARY=/absolute/path/to/dar CGO_ENABLED=0 go test -count=1 -timeout 60s ./...
go vet ./...
```

The Linux process tests start the test executable as a child that writes real
Arrow IPC. They cover successful and empty streams, partial results followed
by failure, malformed IPC, signaled exits, bounded stderr, timeouts, and early
consumer stops. They also check that the child was reaped. The optional
`DAR_BINARY` test compares a real scan with the existing Spark oracle and
checks that reader errors remain redacted. The Docker build runs the process
tests without a Rust build or fixture download.

The approved release PR image workflow runs these Docker build tests on native
amd64 and arm64 runners. It then runs each final consumer image against the
local fixture, requires `{"rows":360,"id_sum":64980}`, and checks that the copied
`dar`, `LICENSE`, and `NOTICE` match the validated CLI image. Feature PRs skip
these container checks.
