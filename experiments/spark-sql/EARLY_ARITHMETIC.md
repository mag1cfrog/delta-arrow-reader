# Early arithmetic evaluation

[Issue 232](https://github.com/mag1cfrog/delta-arrow-reader/issues/232) owns
early arithmetic errors outside discarded scalar subqueries. For example,
`SELECT 7 DIV 0 AS r FROM range(3) WHERE false LIMIT 0` now raises
divide-by-zero, as Spark does during optimization. A live CASE or NULLIF
around a local column CAST also preserves its required error. Dead conditional
parents still discard unused derived division columns.

The baseline is PR 252 at integration
`860ddc0d2b1ac0fc2f38579f35817a21b7512a70`. The
[Sail patch](sail-early-arithmetic.patch) changes the existing analyzer and
integer DIV planning metadata. The [DataFusion patch](datafusion-early-arithmetic.patch)
exports the already selected NULL-pruning helper without changing its body.
There is no new dependency, row scanner or numeric kernel. Query execution is
Rust; Python records the independent Spark reference and verifies evidence.
These remain optional experimental patches. Default-build adoption belongs to
[issue 165](https://github.com/mag1cfrog/delta-arrow-reader/issues/165).

## Evaluation order

The analyzer now recognizes arithmetic inside whole parent expressions and
in filter, sort and aggregate expressions. Existing local VALUES evaluation
runs before LIMIT and projection pruning. COALESCE uses DataFusion's existing
CASE lowering. A literal NULL divisor suppresses discarded operands, while a
live divisor CAST must still be evaluated. Empty local inputs and dead CASE
branches do not acquire errors.

Nonlocal checks preserve NULL propagation, conditional-parent pruning and
unused-column removal before checking constants. They avoid the full Boolean
and filter simplification that previously erased required arithmetic errors.
A LIMIT 0 check distinguishes a genuinely truncating limit from a redundant
limit over an already known empty input. Table scans, range execution, joins
and aggregates are not executed by the local evaluator.

Integer DIV retains its ANSI setting in the UDF instance so the broader
precheck does not introduce legacy numeric-CAST errors. This metadata is read
by the analyzer; division kernels are unchanged. The discarded scalar-subquery
selector and final rewrite are unchanged. Remaining scalar-subquery acceptance
belongs to [issue 233](https://github.com/mag1cfrog/delta-arrow-reader/issues/233).

The reference is Spark 4.2.0's
[optimizer rule order and local relation conversion](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala)
and [constant folding](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/expressions.scala).
Pinned source snapshots are archived. Fresh phase traces place all 167 errors
in the expanded Spark matrix in optimization. All 167 corresponding native
errors carry the analyzer's context prefix. Harness call-boundary labels are
retained separately.

## Correctness and retained boundaries

All 58 assigned ANSI observations and their 58 opposite-mode controls pass,
improving from 58/116 to 116/116. Of the ANSI targets, 53 require errors and
five require successful NULL results. Fresh Spark captures agree with all
116 frozen reference outcomes.

The [422-query matrix](early-arithmetic.jsonl) improves from 693/844 to 842/844
across both modes. It covers four signed integer widths, floating and Decimal
arithmetic, CASE/COALESCE/NULLIF, Boolean operand order, local and range inputs,
filters, sorting, aggregates, LIMIT 0, NULL positions, casts, unused columns,
and batches of 1, 2 and 64. Generated cases and retained failures are bounded
regression evidence, not an exhaustive Spark conformance suite.

Both remaining observations reject zero remainder operands as required but
report DIVIDE_BY_ZERO instead of REMAINDER_BY_ZERO: the BIGINT and Decimal
column forms under LIMIT 0. Existing
[issue 149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) owns
these diagnostics. The comparator never infers an error condition from SQL
text. It recognizes Arrow's numeric narrowing diagnostic separately from
string parsing, preserves generic divide/remainder distinctions, and retains
every complete error payload.

All prior passing historical observations remain passing. The 37 historical
groups improve from 6,542/6,784 to 6,587/6,784. Dedicated numeric suites retain
their passing observations; the existing floating remainder NULL/CAST control
now agrees too. Integer-overflow remains 616/616. Of 131 retained integer error
texts, six gain the analyzer context as errors move earlier. Historical replay
retains 886 paired errors and records 111 changed texts, with full payloads in
the archive.

Earlier CAST results retain Decimal 445/454, scalar reachability 340/346,
strict integer 496/496, floating grammar 488/488 and legacy conversion 562/562.
The STRING-modulo matrix improves from 636/646 to 638/646, resolving its two
LIMIT 0 handoffs. Selected earlier CAST controls improve from 122/126 to
126/126; the reduced diagnostic improves from 107/128 to 116/128 using its
unchanged comparator.

The competing BIGINT overflow/zero query now fails in the analyzer. A fixed
before/after/after/before repeat records overflow 25/32 times
in the baseline, with zero reported in the other 7 runs;
the candidate reports overflow 32/32 times. Separate single-error
controls agree. Fresh Spark traces confirm optimization-time overflow for the
original query. This is a local-VALUES ordering improvement, not general
error-order acceptance. All original captures remain available under issue 149.

The main matrix retains 71 logical and 79 physical nullability differences.
The 668 observations successful on both builds keep the same metadata
differences. Real-Delta validation preserves all 116 outcomes and 18 adapter
checks; its frozen Spark split remains 47 matches, 58 differences and 11
pending adapters. Rust tests pass: 314 function, 49 planner and 28 runner.
Existing SQL tests now include the added early-error and dead-parent controls.
The comparator regression check passes.

The first prototype incorrectly rejected legacy NaN casts. The next exposed
four historical dead-parent regressions. Both are corrected and retained in
the archive with their source snapshots and failed checks.

## Bounded cost check

Both benchmark binaries use the same Rust source, package versions and feature
sets, each linked against 505 private artifacts and 414 named libraries.
The four changed source entries and all transitive library hashes are recorded.
Changed library hashes: `datafusion_optimizer`, `datafusion`, `sail_common_datafusion`, `datafusion_spark`, `sail_function`, `sail_logical_plan`, `delta_arrow_reader`, `sail_plan`.

The fixed process order is before/after/after/before/after/before/before/after.
Each process uses CPU 2, 262,144 rows, batches of 8,192, one partition, four
warmups and 21 samples per phase. Sixteen queries use nonnullable and 10%-NULL
divisor patterns, giving 32 equal-output configurations. Every successful run
checks every row against Spark's 1,000-row input period, including exact types,
integers, floating bits, Decimal coefficients, NULLs and output digests. A time
is the median of four process medians. No sample or completed run is discarded.

Execution changes range from -14.07% to +2.61%.
The largest planning ratios in this run are:

- `local_coalesce_nullstrue`: 0.611260 -> 0.635976 ms, +4.04% (+0.024716 ms).
- `local_coalesce_nullsfalse`: 0.611090 -> 0.630781 ms, +3.22% (+0.019691 ms).
- `modulo_nullsfalse`: 0.296360 -> 0.302628 ms, +2.11% (+0.006268 ms).
- `coalesce_nullsfalse`: 0.360319 -> 0.367468 ms, +1.98% (+0.007149 ms).

The added expression traversal and local prechecks remain candidates for cost
attribution. Native SQL controls and the unchanged arithmetic kernels are
retained, but this run has no same-binary calibration, hardware counters,
allocation/memory measurements or reserved host. These observations do not
establish zero regression. Existing
[issue 159](https://github.com/mag1cfrog/delta-arrow-reader/issues/159) retains
this planning evidence alongside earlier CAST costs. Compatibility scope is
not expanded into a performance optimization pass.

| Query / divisor NULLs | Planning before / after ms | Change | Execution before / after ms | Change |
| --- | ---: | ---: | ---: | ---: |
| column_nullsfalse | 0.233127 / 0.232177 | -0.41% | 0.022717 / 0.022748 | +0.13% |
| addition_nullsfalse | 0.318878 / 0.323155 | +1.34% | 0.158900 / 0.160147 | +0.79% |
| div_nullsfalse | 0.276343 / 0.278903 | +0.93% | 0.389218 / 0.389950 | +0.19% |
| divide_nullsfalse | 0.284148 / 0.286232 | +0.73% | 0.364126 / 0.359699 | -1.22% |
| modulo_nullsfalse | 0.296360 / 0.302628 | +2.11% | 0.387861 / 0.387154 | -0.18% |
| case_nullsfalse | 0.416644 / 0.422850 | +1.49% | 1.481186 / 1.482254 | +0.07% |
| coalesce_nullsfalse | 0.360319 / 0.367468 | +1.98% | 0.388572 / 0.388331 | -0.06% |
| null_divisor_nullsfalse | 0.372912 / 0.379445 | +1.75% | 0.043481 / 0.043085 | -0.91% |
| null_dividend_nullsfalse | 0.359252 / 0.358761 | -0.14% | 0.043415 / 0.043291 | -0.29% |
| nullif_nullsfalse | 0.347390 / 0.353066 | +1.63% | 0.498491 / 0.497629 | -0.17% |
| decimal_nullsfalse | 0.326877 / 0.328630 | +0.54% | 4.912830 / 4.929221 | +0.33% |
| string_div_nullsfalse | 0.346618 / 0.349284 | +0.77% | 4.543855 / 4.662536 | +2.61% |
| local_coalesce_nullsfalse | 0.611090 / 0.630781 | +3.22% | 0.061995 / 0.062060 | +0.10% |
| legacy_div_nullsfalse | 0.333114 / 0.330935 | -0.65% | 0.498672 / 0.498085 | -0.12% |
| native_addition_nullsfalse | 0.177940 / 0.180165 | +1.25% | 0.070636 / 0.070055 | -0.82% |
| native_divide_nullsfalse | 0.194055 / 0.193194 | -0.44% | 0.286667 / 0.286197 | -0.16% |
| column_nullstrue | 0.225519 / 0.223876 | -0.73% | 0.023107 / 0.022892 | -0.93% |
| addition_nullstrue | 0.299992 / 0.303894 | +1.30% | 0.160904 / 0.160779 | -0.08% |
| div_nullstrue | 0.257854 / 0.261551 | +1.43% | 0.374631 / 0.375402 | +0.21% |
| divide_nullstrue | 0.372868 / 0.377005 | +1.11% | 1.804867 / 1.812622 | +0.43% |
| modulo_nullstrue | 0.276334 / 0.279139 | +1.02% | 0.372161 / 0.371800 | -0.10% |
| case_nullstrue | 0.401586 / 0.408023 | +1.60% | 1.913269 / 1.946671 | +1.75% |
| coalesce_nullstrue | 0.418582 / 0.422235 | +0.87% | 1.016809 / 1.022550 | +0.56% |
| null_divisor_nullstrue | 0.346774 / 0.353431 | +1.92% | 0.044277 / 0.043946 | -0.75% |
| null_dividend_nullstrue | 0.352635 / 0.353998 | +0.39% | 0.043511 / 0.043716 | +0.47% |
| nullif_nullstrue | 0.350771 / 0.353367 | +0.74% | 0.487976 / 0.484335 | -0.75% |
| decimal_nullstrue | 0.441464 / 0.448093 | +1.50% | 5.997906 / 5.994805 | -0.05% |
| string_div_nullstrue | 0.369436 / 0.372897 | +0.94% | 6.025341 / 6.100181 | +1.24% |
| local_coalesce_nullstrue | 0.611260 / 0.635976 | +4.04% | 0.062461 / 0.061755 | -1.13% |
| legacy_div_nullstrue | 0.333534 / 0.332407 | -0.34% | 0.567930 / 0.488046 | -14.07% |
| native_addition_nullstrue | 0.177870 / 0.176283 | -0.89% | 0.069575 / 0.069609 | +0.05% |
| native_divide_nullstrue | 0.196296 / 0.193570 | -1.39% | 0.359747 / 0.359884 | +0.04% |

The archive includes every phase sample, all four process medians, exact SQL,
NULL patterns, plans, input sizes, binary/source/link hashes and commands.
Candidate probe SHA-256: `ecfd8a0427b4221374235912b844276301b101db75a64426fa9376e1ff9cd292`.
Candidate runner SHA-256: `b4720441101d867a23e1a610fa4e3eef3e0cdcb089e23534e20d7c45e746a86c`.

## Reproduction

The [result map](early-arithmetic-results.json) pins
[the evidence archive](early-arithmetic-runs.json.gz). Its `check-archive.py`
verifies source and patch identity in both directions, frozen comparisons,
schema/error records, test logs and every timing sample without Spark or Cargo.
Run from the repository root:

```sh
python -m unittest discover -s experiments/spark-sql -p test_early_arithmetic.py
python - <<'PY'
import gzip, json, subprocess, tempfile
from pathlib import Path
root = Path('experiments/spark-sql')
archive = json.loads(gzip.decompress((root / 'early-arithmetic-runs.json.gz').read_bytes()))
with tempfile.TemporaryDirectory() as directory:
    check = Path(directory) / 'check-archive.py'
    check.write_text(archive['files']['check-archive.py'].replace(
        '/home/hanbo/repo/delta-arrow-reader/experiments/spark-sql', str(root.resolve())))
    subprocess.run(['python', str(check)], check=True)
PY
```

For a runtime replay, use `prepare.py` and `build.py` from the archive to
reconstruct the 95-source accepted baseline and apply the four candidate
entries. Scripts preserve the actual build/cache paths and restore shared
sources and executable slots. They are research reproduction records, not a
clean-checkout installer; issue 165 owns that integration. The default vendor
and Python package are not changed by this slice.
