# Accepted-runtime replay

`accepted-runtime.jsonl.gz` preserves the frozen implementation's observations,
including its unresolved Spark differences. `import_accepted_runtime.py`
reconstructs it from the source archive named in `accepted-runtime.json`.
Importing the fixture does not run Spark or certify compatibility.

## Parallel errors

A single capture cannot specify which unordered partition finishes first.
`parallel-error-outcomes.json` lists one additional complete error outcome for
each of 20 exact observation IDs. The original captures remain unchanged.

- Eight ANSI `only_*_union_exists` / `only_*_union_count` queries race a failing
  grouped branch against a valid row under `LIMIT 1`. The frozen physical plans
  already contain parallel `UnionExec` children under a coalescer with `fetch=1`.
  Either the captured successful result or that branch's exact execution error
  can arrive first. The four expressions are integer division and casts to INT,
  DOUBLE and DECIMAL. Successful values and all recorded schema fields must match.
- Twelve ANSI `*_column_*` ROUND queries repartition size-two batches before
  rounding. Partition zero first overflows on the type's minimum value;
  partition one first overflows on its maximum. The table permits only those
  two complete errors, retaining the exact operand, scale, stage and output name.
  It does not permit success or an arbitrary overflow message.

These alternatives were checked against the frozen plans and by executing each
ROUND partition and each failing UNION branch separately. Some repeated complete
queries also reproduced both outcomes. The matcher tests reject wrong values,
types, nullability, names, errors, stages and alternatives applied to another ID.
All other replay observations retain their original comparisons, including the
nearby forced-evaluation, ORDER BY and OFFSET error cases.

Independent Spark 4.2.0 and the frozen Spark reference both fail the eight UNION
queries. The retained runtime's possible success is an unresolved compatibility
gap tracked in the [restart plan](../../../../plans/spark-sql-restart.md) under
S07, not a new supported Spark behavior. Fixing that gap must replace these
alternatives with strict Spark expectations. Neither scheduler retries nor
disabling parallelism is a fix.

## Array membership

`array-contains-spark.jsonl` starts with input rows and Spark settings, followed by
166 independent query observations. Regenerate it with
`capture_array_contains_spark.py` in a Python 3.12 environment containing
`pyspark==4.2.0`, with Java 21 available. Rust tests use the committed captures.

The tests compare values, output names and types for 151 supported-input cases in
both one-row and larger batches. Returned NULLs must be representable in both
output schemas. Logical nullability can remain conservative, and constant folding
can narrow physical nullability; exact Spark nullability parity is separate S07
work. The volatile-lookup and direct-function tests check evaluation count and
all-NULL batch cardinality separately.

Fifteen observations have explicit `retained_difference` labels. Their assertions
preserve the relevant boundary without counting them as Spark-compatible:

- Mixed Decimal precisions still fail coercion. Equal Decimal types are covered.
- Two projected NULL/cast cases still evaluate the bad cast in ANSI mode.
- Untyped NULL columns remain admitted, unlike Spark. Their membership results
  now follow the NULL rule across the complete batch; the admission gap remains.
- `SIZE(NULL)` remains NULL in non-ANSI mode, while the captured Spark setting
  `spark.sql.legacy.sizeOfNull=true` returns -1.

For local lookup-cost comparisons, run
`cargo bench --locked --bench array_contains --profile dev` on each revision with
the same profile. A baseline predating the benchmark needs the same benchmark
source and Cargo target declaration copied into its checkout first. The benchmark
reports planning and execution samples for 24 Int32 scenarios, including nullable
and nonnullable hits and misses and long constant arrays. Nullable misses
intentionally change result validity; their timing measures the cost of corrected
behavior, not equivalent-output speed.

## Floating NULLIF

`nullif-float-spark.jsonl` contains input bits/settings and 98 independent Spark
4.2.0 observations. Regenerate it with `capture_nullif_float_spark.py` using the
same Python/Java setup as the array reference. Rust tests need neither runtime.
Float results use hexadecimal IEEE bits to retain width and zero signs; NaNs
share one display token. Separate Rust assertions check unmatched NaN payloads.

The public-API tests compare values, names and types with batches of one, three
and 32 rows. They cover FLOAT/DOUBLE columns and scalars, zero signs, NaNs,
infinities, NULLs, empty batches, live casts and nonfloating controls. Returned
NULLs must be representable in both schemas; exact Spark schema-nullability
parity is not claimed. Guarded and live multirow subqueries check integration
with existing expression-error handling. Direct calls check sliced arrays,
scalar/array combinations, empty results and unmatched value bits. A volatile
input test checks one evaluation of each operand per row.

`reviewed-corrections.json` replaces four exact historical outcomes in the replay
with the independently verified NULLIF results. It does not add alternatives:
the old negative-zero output now fails. The original compressed captures stay
unchanged, and unused correction IDs fail the complete replay.

Mixed-input return types, unused-operand errors and nested-type admission remain
separate conditional findings. These tests do not establish full NULLIF or Spark
compatibility.

Run `cargo bench --locked --bench nullif --profile dev` on both revisions with the
same profile and benchmark source/target declaration. It records planning and
execution samples for nullable/nonnullable DOUBLE columns and constant operands,
using 262,144 rows, two warm-ups and eleven samples per case. These inputs have
equivalent outputs before and after the fix. Timing has no pass/fail threshold;
local development-profile results do not establish release throughput.
