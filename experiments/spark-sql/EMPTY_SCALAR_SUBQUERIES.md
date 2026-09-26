# Empty scalar-subquery fields

[Issue 234](https://github.com/mag1cfrog/delta-arrow-reader/issues/234) owns
NULL results from empty scalar subqueries. The accepted baseline is PR 254,
integration `7dc5f3871782de09dcf514054cb1b85c78d3ea34`.
The [optional Rust patch](datafusion-empty-scalar.patch) changes two DataFusion
files. [Issue 165](https://github.com/mag1cfrog/delta-arrow-reader/issues/165)
owns default-build adoption.

## Field contract

`SELECT (SELECT 7 FROM range(3) LIMIT 0) AS r` must return one INT NULL.
The scalar executor already produces that typed NULL, but the logical and
physical planners inherit the inner field's nonnullable flag. Arrow rejects
the result. The same flag can make the optimizer turn `IS NULL` into false,
drop COALESCE fallbacks or select the wrong CASE branch.

The logical scalar expression now reports nullable=true and clones the inner
field with that flag, preserving its type and metadata. Physical lowering uses
the logical expression's nullable contract. The scalar executor, cardinality
checks, numerical kernels and dependency versions are unchanged.

## Validation

All 11 assigned observations improve from 0/11 to 11/11. With opposite-mode
controls, agreement improves 5/22 -> 22/22. Fresh Spark captures agree with
all 22 frozen references. Repeated manifestations remain individually recorded.

The [148-query matrix](empty-scalar.jsonl), run in both ANSI modes, improves
146/296 -> 294/296 with no lost passing observation. It covers INT, BIGINT,
DOUBLE, two Decimal precisions, STRING and BOOLEAN; LIMIT 0, empty range and
VALUES filters, nullable inner expressions, OFFSET, nesting, outer projections,
IS NULL, COALESCE and CASE. All 140 empty-result observations return typed NULL
with the expected logical and physical nullable fields. Assigned targets assert
both fields too. One-row values, one NULL row, COUNT(empty)=0, multiple-row
errors and column-count validation retain their checks.

The remaining two value differences are projected IN with NULL, where the
accepted policy preserves correct SQL NULL semantics instead of Spark's false.
Schema differences on successful main-matrix observations change from
20 logical / 20 physical to 2 logical / 4 physical. The remaining logical
fields belong to that IN policy; physical fields are BOOLEAN COALESCE and IN
controls. Values are correct for those COALESCE and empty-IN controls, and
[issue 149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) retains
the conservative physical-field differences. Every required empty scalar field
matches. Live correlated execution remains with its existing owner.

The comparison reuses the numeric, exact floating/Decimal and error-cause
checks, adding only Arrow UTF8 type-name normalization for STRING. It does not
infer an error from the SQL text. Schema checks remain a separate dimension;
the 140 empty-result observations and all 22 assigned/control observations
explicitly require matching logical and physical nullability.

Historical replay retains 6,599/6,784
coarse agreements across 37 groups, also 6,599
before. Four repaired LIMIT 0 observations offset four lost coarse matches.
Those lost matches were false positives: Spark raised DIVIDE_BY_ZERO while the
baseline raised the unrelated Arrow nonnullable-field error. The candidate
returns NULL, exposing the existing missing early error. Fresh Spark captures
confirm DIVIDE_BY_ZERO during optimization for all four WHERE false subqueries;
non-ANSI and LIMIT 0 controls return NULL. The error-aware comparison for that
350-observation group improves 342/350 -> 346/350 with no lost genuine agreement.
Both the four coarse losses and full error payloads remain in
`masked-error-audit.json`; issue 149 owns their review and routing. They are
not counted as repaired Spark behavior. Dedicated numeric, CAST, arithmetic
and NULL-subquery passing observations are retained. Integer overflow stays
616/616.
The exact-error audit retains 907 paired failures and
records 1 changed error texts separately. The
existing mixed-overflow case selects the other failing Decimal value (positive
instead of negative); both complete errors are retained. This capture does
not establish a new deterministic error-order rule.
Rust function/planner/runner suites pass, together with the two logical/physical
field regressions embedded in the patch. Those two tests also run with the
native Rust test harness linked to the private candidate libraries, since Cargo
cannot run dependency dev-tests through this experiment's workspace membership.
Source extraction, command, linkage and output are archived. Real-Delta checks
retain all 116 outcomes and 18 adapter checks. These suites establish bounded
acceptance, not full Spark conformance.

## Bounded performance check

Both binaries compile the same benchmark source against private artifact
snapshots with identical package versions/features. The 14 queries cover
empty, populated and nullable scalar inputs, COUNT(empty), COALESCE and
column/native SQL controls. They run with nonnullable and 10%-NULL input
patterns: 20 configurations have equal output, while 8 baseline
configurations fail and have candidate-only timings. No ratio compares a
working query with a failing one.

Protocol: CPU 2, 262,144 outer rows, batches of 8,192, one partition, four
warmups and 21 planning plus 21 execution samples per process/configuration.
Fixed process order: before/after/after/before/after/before/before/after.
Every successful run checks every output against the same Spark 1,000-row
period, including exact type, value, NULL pattern and digest. No sample or
completed run is discarded. Times below are medians of four process medians.
Largest observed increases among equal-output configurations:

- Planning, `count_empty_nullstrue`: 0.4777320 -> 0.5031395 ms, +5.3184% (+0.0254075 ms).
- Execution, `native_column_nullstrue`: 0.0214650 -> 0.0219160 ms, +2.1011% (+0.0004510 ms).

Nullability can change optimization and physical plans. The field clone also
changes planning work. This bounded run does not isolate those costs or remove
measurement noise. No same-binary calibration, allocation/memory measurement,
hardware counters or reserved host was used. Full raw samples, all process
medians, plans, SQL/settings, input construction and source/library/binary
hashes remain in the archive and with
[issue 159](https://github.com/mag1cfrog/delta-arrow-reader/issues/159).
This does not establish zero regression or complete performance acceptance.

| Configuration | Planning before / after ms | Change | Execution before / after ms | Change |
| --- | ---: | ---: | ---: | ---: |
| column_nullsfalse | 0.225399 / 0.226936 | +0.68% | 0.022462 / 0.022356 | -0.47% |
| nullable_column_nullsfalse | 0.244029 / 0.242871 | -0.47% | 0.022597 / 0.022812 | +0.95% |
| native_column_nullsfalse | 0.138682 / 0.137390 | -0.93% | 0.022467 / 0.022417 | -0.22% |
| one_bigint_nullsfalse | 0.392163 / 0.389919 | -0.57% | 0.052868 / 0.053534 | +1.26% |
| one_decimal_nullsfalse | 0.408654 / 0.409786 | +0.28% | 0.081872 / 0.082278 | +0.50% |
| nullable_one_nullsfalse | 0.541345 / 0.537072 | -0.79% | 0.052553 / 0.053455 | +1.72% |
| one_null_nullsfalse | 0.396432 / 0.394969 | -0.37% | 0.044588 / 0.044453 | -0.30% |
| empty_bigint_nullsfalse | error / 0.390425 | no ratio | error / 0.044618 | no ratio |
| empty_decimal_nullsfalse | error / 0.410417 | no ratio | error / 0.059135 | no ratio |
| empty_nullable_nullsfalse | 0.541451 / 0.539637 | -0.33% | 0.044492 / 0.044507 | +0.03% |
| count_empty_nullsfalse | 0.503170 / 0.504412 | +0.25% | 0.039839 / 0.039844 | +0.01% |
| coalesce_empty_nullsfalse | error / 0.546220 | no ratio | error / 0.087468 | no ratio |
| native_one_nullsfalse | 0.235853 / 0.235077 | -0.33% | 0.052803 / 0.053009 | +0.39% |
| native_empty_nullsfalse | error / 0.218827 | no ratio | error / 0.044001 | no ratio |
| column_nullstrue | 0.224838 / 0.225113 | +0.12% | 0.022562 / 0.022527 | -0.16% |
| nullable_column_nullstrue | 0.225459 / 0.223741 | -0.76% | 0.022943 / 0.023189 | +1.07% |
| native_column_nullstrue | 0.136103 / 0.137370 | +0.93% | 0.021465 / 0.021916 | +2.10% |
| one_bigint_nullstrue | 0.370267 / 0.370368 | +0.03% | 0.053565 / 0.054170 | +1.13% |
| one_decimal_nullstrue | 0.396055 / 0.395675 | -0.10% | 0.081937 / 0.082353 | +0.51% |
| nullable_one_nullstrue | 0.517872 / 0.520382 | +0.48% | 0.053064 / 0.053228 | +0.31% |
| one_null_nullstrue | 0.368890 / 0.373363 | +1.21% | 0.049818 / 0.050704 | +1.78% |
| empty_bigint_nullstrue | error / 0.368876 | no ratio | error / 0.044983 | no ratio |
| empty_decimal_nullstrue | error / 0.386723 | no ratio | error / 0.059360 | no ratio |
| empty_nullable_nullstrue | 0.515958 / 0.534949 | +3.68% | 0.045079 / 0.044623 | -1.01% |
| count_empty_nullstrue | 0.477732 / 0.503140 | +5.32% | 0.040400 / 0.040090 | -0.77% |
| coalesce_empty_nullstrue | error / 0.548213 | no ratio | error / 0.087713 | no ratio |
| native_one_nullstrue | 0.236349 / 0.234846 | -0.64% | 0.053384 / 0.053174 | -0.39% |
| native_empty_nullstrue | error / 0.219162 | no ratio | error / 0.044372 | no ratio |

Candidate probe SHA-256: `2bcac8e54b2b953d3ff9de1fd940ae42e4d0284dcbce692d4b258ceaf3387375`.
Candidate runner SHA-256: `280aaf2361e4b169f933cddbd7dddfe75d1896fb2eefbaaecfed3d8af4c3ff80`.

## Reproduction

The result map is `empty-scalar-results.json`; the evidence archive is
`empty-scalar-runs.json.gz`, a gzip-compressed JSON object with `files` and
per-file `sha256` maps. Its `check-archive.py` verifies source/patch identity,
frozen comparisons, nullable assertions, retained checks, raw timing medians
and test/restoration records without rebuilding or running Spark. Point its
`root` at this checkout's `experiments/spark-sql` directory before running it.

`prepare.py` reconstructs the accepted 95-entry source manifest and adds four
unchanged DataFusion source snapshots, checked against the earlier diagnostic
archive. `before-built-original.json` preserves the original accepted build
record; `before-built.json` records those additional unmodified source captures.
`build.py` installs the two candidate files, records build identities and
restores shared sources and executable slots on exit. `validate-all.py` runs
the focused and retained checks. `link-bench.py`, `run-nullable-tests.py` and
`measure.py` use the private candidate artifacts. The archived absolute paths
describe this research environment; clean-checkout/default CI adoption remains
with issue 165.
