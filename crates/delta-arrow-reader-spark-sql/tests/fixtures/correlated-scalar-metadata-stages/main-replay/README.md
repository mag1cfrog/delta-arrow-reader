# Complete original main scalar replay: schema stages

This immutable source package captures all 152 observations selected by
`single_row_projections_match_spark` at
`3dda18219085af27fdfe796f805594bf2de95fa1`. It makes no product changes and has
not run an engine. Root owns the serial Spark capture and candidate validation.

The complete original 202-observation raw file is preserved under `sources/`,
SHA256 `0d3696b51f73f63ca68fa7cc5fb17143d137920f343e78056fa7b3d30a4718fc`.
The original Python capture and Rust test are also byte-exact source snapshots.
The planned 152 records use the unchanged Rust selection predicate and original
order. The other 50 identities are enumerated in the manifest and remain in the
complete source raw file with their separate execution contracts. Selection
does not depend on candidate failures.

All four original providers, SQL text, ANSI modes and original expected objects
are unchanged. The selected set contains 130 successful observations and 22
required failures: 12 unresolved outer references, two division failures, two
CAST failures and six scalar cardinality failures. Eight records retain their
explicit old volatile evaluation counts. The original main Rust guard requires
zero unused volatile evaluations for every selected case.

The capture reuses the existing full-stage observer and its NULL/string/integer
decoder. It uses the original main capture's leaf encoder, including lowercase
Boolean values and bit-exact doubles. Its nondeterministic `count_row` UDF body
and registration match the original source. The observer resets the accumulator
for each query, matching the Rust replay.

Every observation records all completed public, analyzed, optimized and
executed stages, before and after collection, plus plans and complete sorted
rows without removing duplicates. Errors keep completed stages and their
actual condition, message and phase. The final accumulator is recorded after
success or failure. A planning count exists only if physical planning completed;
the capture does not invent an unavailable stage. Each original expected object
remains under `original_observation`, with explicit status, public, row,
condition and old-counter comparisons where applicable.

Run from this directory with the existing pinned reference environment and a
fresh destination:

```sh
/home/hanbo/.local/share/delta-arrow-reader-review/spark-array-null-2026-10-31/reference-venv/bin/python capture_main_scalar_stages.py main-scalar-stages-spark.jsonl
```

The script validates source hashes, the complete 152/50 partition and the exact
Spark 4.2.0 revision `32f7299601108917fb01920a54e084595b7b3bf8`. It refuses an
existing output path. `manifest.json` freezes the script, planned cases and all
source snapshots. Author static checks also compare the original counter AST
and exercise the original NULL, Boolean, string and double encoding.

After capture, root can inspect every recorded stage before authorizing any
test assertion change. In particular, an execution-time error must still be
reached after validating a recorded pre-collection physical schema; a generic
schema error must not substitute for DIVIDE_BY_ZERO or another required cause.
Retain the old raw observations and all current field, row, batch, error and
counter guards. The whole original main set has 304 DAR comparisons at batch
sizes 1 and 32.

The unchanged candidate test recipe, from its standalone Spark SQL workspace,
is:

```sh
cargo +1.96.0 test --locked --workspace --test single_row_correlated -- --nocapture --test-threads=1
```
