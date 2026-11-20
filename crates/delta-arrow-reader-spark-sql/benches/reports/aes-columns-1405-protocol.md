# AES column argument timings

[#1405](https://github.com/mag1cfrog/delta-arrow-reader/issues/1405) restores
valid binary results for the four AES cases in #1391. The original strict
calls failed on multi-row input, while the tolerant calls returned NULL.
The measurements below use the original statements and one-million-row
table in a separate [manifest](../aes_columns_1405/manifest.json).

Build the unmodified `functions` benchmark driver from the candidate with
Rust 1.96.0 and `CARGO_PROFILE_RELEASE_DEBUG=1`. Keep the release binary and
build log fingerprints. Run the AES Spark 4.2.0 fixture test before timing.

The original table repeats one nonnull plaintext and its AES-128 ECB
ciphertext. Before timing, compare all rows and executed types for each
statement and ANSI mode against a native projection of the matching
Spark-captured constant. Preserve NULLs from the source column. The four
preflight calls use the existing driver with a 180-second bound each.

Run three replicas on CPUs 2 through 5. Use one fresh process per ANSI mode
in each replica and reverse mode order for the middle replica. Rotate the
four statements by one position per replica. Keep three samples per cell
after one warmup and retain the plain projection control. Count a cell
only if it keeps the binary output type and returns one million rows.
Report execution and projection-adjusted ns per input row as medians of
process medians. Each timing process has a 600-second bound and the
complete run has a 30-minute bound.

From the Spark workspace:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_RELEASE_DEBUG=1 \
  cargo +1.96.0 build --locked --release --bench functions
timeout 30m python3 benches/aes_columns_1405/run.py FUNCTIONS_BINARY OUTPUT
```
