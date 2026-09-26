# STRING-peer modulo coercion

[Issue 247](https://github.com/mag1cfrog/delta-arrow-reader/issues/247) owns
implicit STRING-peer conversion for `%` and `MOD`. Before this change,
`SELECT '7' % 2L` fails during type planning. The optional
[Rust patch](sail-string-modulo-coercion.patch) returns BIGINT 1 in ANSI mode
and DOUBLE 1.0 in legacy mode, using the existing conversion and remainder
kernels.

The baseline is the accepted Decimal CAST runtime from PR 251, integration
6f29c0a05a614a843f12bb995cbf4831a4b4ed7b. Only Sail's arithmetic planning file
changes. Default vendored sources remain unchanged; default-build adoption
belongs to [issue 165](https://github.com/mag1cfrog/delta-arrow-reader/issues/165).
SQL planning/execution remains Rust. Python captures the independent Spark
reference and packages the evidence.

## Implementation and contract

`%` and `MOD` already share a resolver. That resolver now calls the same
STRING conversion helper as `/` and `DIV`, before resolving the result type.
The helper is renamed to include remainder; its conversion rules are unchanged.
The existing column-CAST recognizer accepts the DOUBLE trim character set as
well as the integer set. This lets the existing nullable-divisor CASE guard
skip malformed text on unreachable rows. No new kernel, parser or dependency
is introduced.

With ANSI enabled, a signed-integer peer promotes both inputs to BIGINT.
FLOAT, DOUBLE and Decimal peers select DOUBLE. STRING/STRING and
STRING/untyped-NULL remain type errors. Legacy mode promotes supported STRING
arithmetic to DOUBLE and converts malformed strings to NULL. These rules are
checked against pinned Spark 4.2.0 captures and its
[ANSI promotion](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/AnsiStringPromotionTypeCoercion.scala),
[legacy promotion](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/StringPromotionTypeCoercion.scala)
and [arithmetic expressions](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala).
Source URLs and hashes are retained in the archive.

## Correctness and existing owners

All 24 assigned observations pass, improving from 0/24. The
[323-query matrix](string-modulo-coercion.jsonl) improves from 16/646 to
636/646 across both ANSI modes. It covers signed integer widths, floating
and Decimal peers, STRING/STRING, both operand positions, both aliases,
valid/malformed/padded strings, numeric bounds, NaN/infinity, zeros, NULLs,
literals/columns, empty/dead paths and batches of 1, 2 and 64. No prior
agreement is lost in this matrix. These are bounded observations, including
repeated shapes, not complete Spark compatibility.

Ten differences remain with existing owners:

- Eight integer remainder-zero diagnostics report Arrow's DIVIDE_BY_ZERO
  instead of Spark's REMAINDER_BY_ZERO. Both reject the query. Full error
  identity belongs to [issue 149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149).
- Two local VALUES queries under LIMIT 0 return no rows instead of raising
  CAST_INVALID_INPUT: `SELECT a % b AS r FROM VALUES ('bad',2L) t(a,b) LIMIT 0`
  and the MOD alias. The existing early arithmetic analyzer does not precheck
  this column-CAST remainder shape. They belong to
  [issue 232](https://github.com/mag1cfrog/delta-arrow-reader/issues/232).

The comparator reuses the existing value/type/schema comparison and recognizes
the explicit REMAINDER_BY_ZERO diagnostic. It does not relabel Arrow's generic
DIVIDE_BY_ZERO from the query text or equate arbitrary errors. A runnable
comparator check preserves these distinctions.

Prior CAST reachability improves from 316/346 to 340/346. Decimal CAST retains
445/454, strict integer CAST 496/496, floating grammar 488/488 and explicit
numeric CAST 562/562. Selected earlier CAST controls retain 122/126 and the
reduced diagnostic retains 107/128.

The 37 historical groups retain 6,542/6,784 agreements with no changed
value/type/status outcomes. Eleven dedicated numeric suites retain their
case-level agreements. The dedicated BIGINT DIV capture records 233/234,
versus the prior 234/234: one query contains both an overflowing divisor -1
and a zero divisor in different batches. This capture reports divide-by-zero
first instead of overflow. Its original failure is retained.

A fixed before/after/after/before reproduction runs that exact query 16 times
per process. Of 32 ANSI observations per build, the baseline reports overflow
28 times and zero four times; the candidate reports overflow 27 times and
zero five times. Their logical/physical plans are identical, including
RoundRobinBatch(2). Separate overflow-only and zero-only controls report the
expected cause on both builds. This demonstrates an existing variable error
order; it does not establish exact Spark error-order compatibility or turn
the original failure into a pass. Issue 149 owns this evidence. No runtime
change is made to numeric DIV.

Integer-overflow checks retain 616/616 and all 131 complete error payloads.
Rust tests pass: 314 function, 49 planner and 28 runner. The new planner test
checks result types, guarded invalid text and live CAST errors through both
aliases and three peer families with ANSI on/off/on in one session. The two
Python comparator checks pass. Real-Delta validation retains all 116 prior
outcomes and 18 adapter checks; its frozen Spark split remains 47 matches,
58 differences and 11 pending adapters.

The new matrix retains one logical-nullability difference and 76 physical
nullability differences. More queries now reach physical execution: the 13
queries successful on both builds keep the same one logical and ten physical
differences. Harness phase-label differences decrease from 614 to two; labels
are call boundaries, not Spark optimizer phases. All 111 changed retained
error payloads/texts are archived. Historical replay retains 890 errors;
Ten texts change with the shared helper's division/remainder diagnostic wording;
two multi-error Decimal queries report a different first invalid value.
Schema and full error review remain with issue 149.

## Bounded cost evidence

Both benchmark binaries use the same Rust source and private link sets with
505 artifacts and 414 named libraries. Package versions/features agree; only
sail_plan's library hash changes. All 95 selected source entries, binaries,
commands, plans and raw samples are pinned.

The fixed process order is before/after/after/before/after/before/before/after.
Each process uses CPU 2, 262,144 rows, batches of 8,192, one partition, four
warmups and 21 samples per phase. Each reported time is the median of four
process medians. Sixteen queries use nonnullable and 10%-NULL divisors, giving
32 configurations: 18 equal-output before/after pairs and 14 candidate-only
cases whose baseline fails planning. Every successful run verifies every row
against a Spark reference, including exact types, integers, floating bits,
NULLs and output digests. No runs or samples are discarded.

Equal-output execution changes range from -0.79% to +0.62%. Planning increases
include nullable numeric modulo 0.269176 -> 0.295454 ms (+9.76%, +0.026279 ms).
The unchanged addition control also rises 0.291897 -> 0.320380 ms (+9.76%);
the native SQL control rises 0.174965 -> 0.178948 ms (+2.28%). This check does
not isolate the added type lookup from other planning/build effects.

The new implicit forms have no successful baseline timing. Within the
candidate, equal-output implicit versus explicit DOUBLE conversion takes
6.796098 / 5.781814 ms without NULLs (+17.54%) and 8.332157 / 7.202809 ms
with NULLs (+15.68%). Legacy and reverse implicit forms are about 12%-13%
slower than their explicit equivalents; integer implicit/explicit forms
are close in this sample. The plans retain the existing btrim plus native
CAST/TRY_CAST path for implicit floating conversion, whereas explicit DOUBLE
CAST uses the selected Spark converter. These are query-form comparisons,
not before/after regressions or allocation measurements.

[Issue 159](https://github.com/mag1cfrog/delta-arrow-reader/issues/159) retains
these planning and conversion costs with all 64 phase/process-median rows,
SQL/settings and raw samples. Earlier CAST/selection costs remain open.
The machine was not reserved; no same-binary calibration, allocation counters,
hardware counters or memory measurements were collected. This is a bounded
check, not performance equivalence or final performance acceptance.

| Query / divisor NULLs | Planning before / after ms | Change | Execution before / after ms | Change |
| --- | ---: | ---: | ---: | ---: |
| implicit_long_nullsfalse | error / 0.324503 | candidate only | error / 4.523752 | candidate only |
| explicit_long_nullsfalse | 0.340603 / 0.350406 | +2.88% | 4.530260 / 4.535519 | +0.12% |
| implicit_long_legacy_nullsfalse | error / 0.361050 | candidate only | error / 5.911615 | candidate only |
| explicit_long_legacy_nullsfalse | 0.360405 / 0.362078 | +0.46% | 5.278059 / 5.278033 | -0.00% |
| implicit_double_nullsfalse | error / 0.376715 | candidate only | error / 6.796098 | candidate only |
| explicit_double_nullsfalse | 0.348548 / 0.351242 | +0.77% | 5.766350 / 5.781814 | +0.27% |
| implicit_decimal_nullsfalse | error / 0.384734 | candidate only | error / 8.163383 | candidate only |
| implicit_reverse_nullsfalse | error / 0.379034 | candidate only | error / 5.288113 | candidate only |
| explicit_reverse_nullsfalse | 0.361591 / 0.369026 | +2.06% | 4.663858 / 4.692937 | +0.62% |
| implicit_alias_nullsfalse | error / 0.347129 | candidate only | error / 4.521358 | candidate only |
| implicit_guarded_nullsfalse | error / 0.333921 | candidate only | error / 4.512646 | candidate only |
| explicit_guarded_nullsfalse | 0.348918 / 0.353096 | +1.20% | 4.526794 / 4.507637 | -0.42% |
| division_control_nullsfalse | 0.320861 / 0.327798 | +2.16% | 4.557350 / 4.553002 | -0.10% |
| numeric_modulo_nullsfalse | 0.292678 / 0.296671 | +1.36% | 0.385371 / 0.386829 | +0.38% |
| numeric_nullsfalse | 0.317680 / 0.318406 | +0.23% | 0.159531 / 0.159211 | -0.20% |
| native_numeric_nullsfalse | 0.176548 / 0.179103 | +1.45% | 0.070250 / 0.069735 | -0.73% |
| implicit_long_nullstrue | error / 0.334371 | candidate only | error / 6.034889 | candidate only |
| explicit_long_nullstrue | 0.350066 / 0.354363 | +1.23% | 6.007448 / 6.023383 | +0.27% |
| implicit_long_legacy_nullstrue | error / 0.338659 | candidate only | error / 5.931587 | candidate only |
| explicit_long_legacy_nullstrue | 0.335804 / 0.336711 | +0.27% | 5.325316 / 5.284846 | -0.76% |
| implicit_double_nullstrue | error / 0.480828 | candidate only | error / 8.332156 | candidate only |
| explicit_double_nullstrue | 0.447411 / 0.456528 | +2.04% | 7.206415 / 7.202808 | -0.05% |
| implicit_decimal_nullstrue | error / 0.500745 | candidate only | error / 9.546226 | candidate only |
| implicit_reverse_nullstrue | error / 0.362232 | candidate only | error / 5.386364 | candidate only |
| explicit_reverse_nullstrue | 0.342546 / 0.370598 | +8.19% | 4.743561 / 4.756315 | +0.27% |
| implicit_alias_nullstrue | error / 0.380552 | candidate only | error / 5.995296 | candidate only |
| implicit_guarded_nullstrue | error / 0.362508 | candidate only | error / 5.988563 | candidate only |
| explicit_guarded_nullstrue | 0.355731 / 0.376930 | +5.96% | 5.980323 / 5.982692 | +0.04% |
| division_control_nullstrue | 0.418727 / 0.444030 | +6.04% | 6.069733 / 6.047622 | -0.36% |
| numeric_modulo_nullstrue | 0.269176 / 0.295454 | +9.76% | 0.369346 / 0.369501 | +0.04% |
| numeric_nullstrue | 0.291897 / 0.320380 | +9.76% | 0.159902 / 0.159035 | -0.54% |
| native_numeric_nullstrue | 0.174965 / 0.178948 | +2.28% | 0.070371 / 0.069815 | -0.79% |

## Reproduction

[Results](string-modulo-coercion-results.json) pin the
[compressed evidence](string-modulo-coercion-runs.json.gz), corpus, generator,
patch, source/reference hashes and prior accepted archives. The archive
includes the original BIGINT error difference, all fixed reproduction runs
and all performance samples.

From the repository root:

```sh
python - <<'PYTHON'
import gzip, json
from pathlib import Path
root = Path('experiments/spark-sql')
a = json.loads(gzip.decompress((root/'string-modulo-coercion-runs.json.gz').read_bytes()))
Path('/tmp/check-string-modulo.py').write_text(a['files']['check-archive.py'])
PYTHON
python /tmp/check-string-modulo.py
PYTHONPATH=experiments/spark-sql python -m unittest test_division_cast_classification test_string_modulo_coercion
```

The verifier recomputes the comparisons, error-order counts, source patches
and all timing medians offline. The live build recipe records its original
scratch paths and is not a clean-checkout installer. Install the final
candidate-math.rs snapshot over the archived 95-source baseline. The earlier
initial snapshot predates the added Rust regression test and is not the
selected build. Shared sources, executable slots and Delta inputs were
restored and checked.
