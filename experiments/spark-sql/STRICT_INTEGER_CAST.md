# Strict integer CAST whitespace

[Issue 237](https://github.com/mag1cfrog/delta-arrow-reader/issues/237) makes
explicit strict CAST and TRY_CAST accept Spark's surrounding ASCII whitespace
for TINYINT, SMALLINT, INT and BIGINT. For example, `CAST(' 7 ' AS BIGINT)`
in ANSI mode now returns 7, and TRY_CAST returns 7 in either mode. Invalid
grammar and width overflow still raise strict errors or produce TRY_CAST NULLs.

The [Rust patch](sail-strict-integer-cast.patch) applies native BTRIM before
the existing conversions. Its character set is bytes 0-32 and DEL, matching
the byte iteration in Spark's [UTF8String exact integer parser](https://github.com/apache/spark/blob/v4.2.0/common/unsafe/src/main/java/org/apache/spark/unsafe/types/UTF8String.java).
NBSP, Unicode spaces, non-ASCII digits and internal whitespace remain invalid.
Legacy fractional truncation retains its existing parser; floating grammar,
Decimal conversion and implicit arithmetic coercion retain their own paths.

The second changed Rust file preserves recognition of this exact native
wrapper in the existing local-column analyzer precheck. The first candidate
passed the bare CAST corpus but failed an existing planner test and changed
117 historical required errors into NULL results. The old predicate only
recognized direct column CASTs. The final predicate recognizes the native
BTRIM implementation with the exact integer trim character set, then applies
the same column/cast boundary. It does not broaden the precheck to arbitrary
scalar functions or change the remaining evaluation-order policy.

This is an optional patch on integration `1b3b0f5a`, following the merged
[legacy CAST-mode repair](LEGACY_NUMERIC_CAST.md). Default-build adoption
remains separate. Runtime planning/execution stays in Rust; Python captures
independent Spark outputs and verifies the archived evidence.

## Correctness evidence

All 36 observations assigned to this issue now agree with Spark. The new
[248-query corpus](strict-integer-cast.jsonl) improves from 448/496 to 496/496
across both ANSI modes. It covers all 34 allowed edge bytes, signs, NULL and
empty inputs, integer-width boundaries, overflow, fractional/exponent text,
Unicode exclusions, literal and runtime-column expressions, and batch sizes
1, 2 and 64. The Rust matrix also checks Utf8, LargeUtf8 and Utf8View and
switches ANSI mode on/off/on within the same session.

The prior CAST corpus improves from 492/562 to 528/562, losing no earlier
agreements. Its remaining 34 floating-string observations stay with the
[floating grammar leaf](https://github.com/mag1cfrog/delta-arrow-reader/issues/238).
All 63 previously repaired legacy-mode observations remain correct; their
63 opposite-mode controls keep 26 agreements and 37 separately owned
evaluation differences. The combined 89/126 count is unchanged.

The 37 historical groups retain 6,508/6,784 agreements with no changed
value/type/status outcomes and no lost earlier agreements. Integer-overflow
controls retain 616/616 and all 131 complete error payloads. The 12 dedicated
comparators return the same case results as the accepted baseline:
arithmetic 575/578, string division 472/480, BIGINT DIV 232/234,
modulo 427/442, extreme Decimal ROUND 1,194/1,322, Float ROUND 398/398 targets
and 16/18 controls, ROUND arguments 388/388, integer ROUND 1,254/1,254,
legacy Decimal ROUND 234/234, Float BROUND boundaries 246/246 plus 8/8 controls,
Float BROUND 80/80 plus 8/8 controls and 10/10 boundaries, and Decimal BROUND
506/506. Their remaining mismatches are retained.

Rust tests pass: 313 function, 46 planner and 28 runner tests. The comparator
self-check passes. The real-Delta capture preserves all 116 prior outcomes
and passes 18 adapter checks. Against its frozen Spark oracle it still has
47 matches, 58 differences and 11 pending adapter observations; matching the
accepted runtime is not the same as full Delta/Spark acceptance.

Nullable schema differences remain separately tracked:

| Corpus | Logical before / after | Physical before / after |
| --- | --- | --- |
| focused | 0 / 0 | 12 / 12 |
| legacy | 64 / 40 | 176 / 188 |
| owned | 0 / 0 | 3 / 3 |

Successful repairs change which observations have a result schema, so these
counts are not a value-regression count. Harness phase-label differences are
16 to 0 in the new corpus, 36 to 16 in the prior CAST corpus and 37 to 37 in
the original owned/control set. Twelve retained strict-error texts in the new
corpus now quote the trimmed invalid input; the prior CAST and owned sets
have no changed error text. Complete error objects, including changed plans,
remain archived.

In the historical replay, all 908 error outcomes are retained. Of 157 changed
complete payloads, 154 change only recorded plans and three report a different
first invalid row in existing multi-error Decimal queries. These three texts
remain visible in `shared-controls.json`; no normalization makes them equal.
The rejected candidate also recorded the already owned mixed-error BIGINT DIV
variation (231/234 instead of 232/234); the corrected runtime's final capture
is 232/234. All intermediate captures remain in the archive. Schema and exact
error/phase acceptance stay with the existing
[validation leaf](https://github.com/mag1cfrog/delta-arrow-reader/issues/149).

Values, complete types, NULLs and classified error causes are compared
separately from schemas, first-error text and harness phase labels. A
`collect()` error label does not identify Spark's internal optimizer phase.
The [result record](strict-integer-cast-results.json) and archive retain
these dimensions and every remaining mismatch; the finite corpus is not a
claim of complete Spark SQL compatibility.

## Bounded cost check

The fixed schedule is before, after, after, before, after, before, before,
after. Each process uses 1,048,576 rows, batches of 8,192, one partition,
CPU 2, eight execution warmups and 41 samples per planning/execution phase.
Planning includes parsing through physical-plan creation. Execution consumes
an existing plan; input creation and output verification occur outside timing.
Each table reports the median of four process medians, with all individual
samples and process medians retained.

There are 50 configurations: all four signed widths for ANSI and TRY_CAST,
canonical and padded integer strings, 19-digit BIGINT strings, legacy
integer/float conversions, Decimal/numeric/native controls, and a NULL-DIV
control for the analyzer path. Each runs with and without NULLs. The initial
48-configuration preparation was extended with the NULL-DIV control before
timing; all 48 previous Spark output vectors remain identical.

Input pattern `i = row % 10000` gives `a = i % 199 - 99`, `s = str(a)`,
`w = str((9007199254740993000 + a) * (1 if i % 2 == 0 else -1))`, and
`f = str(a) + '.25'`. Padded input `ip` adds SPACE/TAB on the left and DEL
on the right. The NULL configuration masks all columns when `i % 10 == 9`.
The exact schema, SQL and construction remain in the benchmark source.

Every successful timed query is checked row by row against the independent
Spark reference, including exact floating bits, Decimal coefficients,
integer widths and NULLs. All 34 successful before/after pairs have equal
types and complete output digests. The remaining 16 baseline configurations
are not throughput baselines: eight strict padded cases raise errors and
eight TRY_CAST padded cases return incorrect NULL values. They retain errors
or output mismatch counts and plans, with candidate-only times and no ratio.

Execution time:

| Configuration | Before (ms) | After (ms) | After / before |
| --- | --- | --- | --- |
| ansi_tiny_nullsfalse | 4.868196 | 17.068290 | 3.5061x (+250.61%) |
| try_tiny_nullsfalse | 3.846439 | 16.195023 | 4.2104x (+321.04%) |
| space_ansi_tiny_nullsfalse | error | 23.087113 | n/a |
| space_try_tiny_nullsfalse | incorrect NULLs | 22.024364 | n/a |
| ansi_small_nullsfalse | 4.810484 | 17.050302 | 3.5444x (+254.44%) |
| try_small_nullsfalse | 3.822715 | 16.186502 | 4.2343x (+323.43%) |
| space_ansi_small_nullsfalse | error | 23.006354 | n/a |
| space_try_small_nullsfalse | incorrect NULLs | 22.143476 | n/a |
| ansi_int_nullsfalse | 4.680288 | 17.000153 | 3.6323x (+263.23%) |
| try_int_nullsfalse | 3.652694 | 16.032962 | 4.3894x (+338.94%) |
| space_ansi_int_nullsfalse | error | 22.944022 | n/a |
| space_try_int_nullsfalse | incorrect NULLs | 21.995041 | n/a |
| ansi_long_nullsfalse | 4.681995 | 16.991698 | 3.6292x (+262.92%) |
| try_long_nullsfalse | 4.254091 | 16.729796 | 3.9326x (+293.26%) |
| space_ansi_long_nullsfalse | error | 22.837445 | n/a |
| space_try_long_nullsfalse | incorrect NULLs | 22.514555 | n/a |
| ansi_wide_long_nullsfalse | 14.597351 | 26.848885 | 1.8393x (+83.93%) |
| try_wide_long_nullsfalse | 13.514365 | 25.864852 | 1.9139x (+91.39%) |
| legacy_long_nullsfalse | 6.089064 | 6.098171 | 1.0015x (+0.15%) |
| legacy_int_nullsfalse | 6.152131 | 6.400803 | 1.0404x (+4.04%) |
| legacy_float_nullsfalse | 19.596556 | 19.404504 | 0.9902x (-0.98%) |
| decimal_nullsfalse | 73.213756 | 73.511243 | 1.0041x (+0.41%) |
| numeric_nullsfalse | 0.449129 | 0.444125 | 0.9889x (-1.11%) |
| native_long_nullsfalse | 4.717987 | 4.695756 | 0.9953x (-0.47%) |
| null_div_nullsfalse | 0.146502 | 0.143687 | 0.9808x (-1.92%) |
| ansi_tiny_nullstrue | 4.914332 | 16.826331 | 3.4239x (+242.39%) |
| try_tiny_nullstrue | 3.962444 | 15.914211 | 4.0163x (+301.63%) |
| space_ansi_tiny_nullstrue | error | 22.154922 | n/a |
| space_try_tiny_nullstrue | incorrect NULLs | 21.185857 | n/a |
| ansi_small_nullstrue | 4.881170 | 16.816743 | 3.4452x (+244.52%) |
| try_small_nullstrue | 3.967723 | 15.854756 | 3.9959x (+299.59%) |
| space_ansi_small_nullstrue | error | 22.170275 | n/a |
| space_try_small_nullstrue | incorrect NULLs | 21.230610 | n/a |
| ansi_int_nullstrue | 4.957337 | 16.849508 | 3.3989x (+239.89%) |
| try_int_nullstrue | 3.832172 | 15.795997 | 4.1219x (+312.19%) |
| space_ansi_int_nullstrue | error | 22.314974 | n/a |
| space_try_int_nullstrue | incorrect NULLs | 21.229103 | n/a |
| ansi_long_nullstrue | 4.773050 | 16.743071 | 3.5078x (+250.78%) |
| try_long_nullstrue | 4.335332 | 16.303895 | 3.7607x (+276.07%) |
| space_ansi_long_nullstrue | error | 22.058984 | n/a |
| space_try_long_nullstrue | incorrect NULLs | 21.575761 | n/a |
| ansi_wide_long_nullstrue | 13.408959 | 25.141173 | 1.8750x (+87.50%) |
| try_wide_long_nullstrue | 12.458619 | 24.279267 | 1.9488x (+94.88%) |
| legacy_long_nullstrue | 7.910902 | 7.915441 | 1.0006x (+0.06%) |
| legacy_int_nullstrue | 7.730588 | 7.836309 | 1.0137x (+1.37%) |
| legacy_float_nullstrue | 18.744037 | 18.521915 | 0.9881x (-1.19%) |
| decimal_nullstrue | 67.445322 | 67.470203 | 1.0004x (+0.04%) |
| numeric_nullstrue | 0.810180 | 0.806563 | 0.9955x (-0.45%) |
| native_long_nullstrue | 4.799835 | 4.811701 | 1.0025x (+0.25%) |
| null_div_nullstrue | 0.144298 | 0.143933 | 0.9975x (-0.25%) |

Planning time:

| Configuration | Before (ms) | After (ms) | After / before |
| --- | --- | --- | --- |
| ansi_tiny_nullsfalse | 0.296196 | 0.346137 | 1.1686x (+16.86%) |
| try_tiny_nullsfalse | 0.294667 | 0.344039 | 1.1675x (+16.75%) |
| space_ansi_tiny_nullsfalse | error | 0.338924 | n/a |
| space_try_tiny_nullsfalse | incorrect NULLs | 0.337321 | n/a |
| ansi_small_nullsfalse | 0.294938 | 0.343137 | 1.1634x (+16.34%) |
| try_small_nullsfalse | 0.293786 | 0.342271 | 1.1650x (+16.50%) |
| space_ansi_small_nullsfalse | error | 0.340878 | n/a |
| space_try_small_nullsfalse | incorrect NULLs | 0.338925 | n/a |
| ansi_int_nullsfalse | 0.295749 | 0.345913 | 1.1696x (+16.96%) |
| try_int_nullsfalse | 0.295027 | 0.345186 | 1.1700x (+17.00%) |
| space_ansi_int_nullsfalse | error | 0.344750 | n/a |
| space_try_int_nullsfalse | incorrect NULLs | 0.342000 | n/a |
| ansi_long_nullsfalse | 0.296702 | 0.348642 | 1.1751x (+17.51%) |
| try_long_nullsfalse | 0.295504 | 0.344460 | 1.1657x (+16.57%) |
| space_ansi_long_nullsfalse | error | 0.341434 | n/a |
| space_try_long_nullsfalse | incorrect NULLs | 0.341004 | n/a |
| ansi_wide_long_nullsfalse | 0.296616 | 0.345777 | 1.1657x (+16.57%) |
| try_wide_long_nullsfalse | 0.295739 | 0.342616 | 1.1585x (+15.85%) |
| legacy_long_nullsfalse | 0.309275 | 0.307611 | 0.9946x (-0.54%) |
| legacy_int_nullsfalse | 0.309976 | 0.307551 | 0.9922x (-0.78%) |
| legacy_float_nullsfalse | 0.348653 | 0.347961 | 0.9980x (-0.20%) |
| decimal_nullsfalse | 0.316137 | 0.313512 | 0.9917x (-0.83%) |
| numeric_nullsfalse | 0.349780 | 0.348382 | 0.9960x (-0.40%) |
| native_long_nullsfalse | 0.166620 | 0.166855 | 1.0014x (+0.14%) |
| null_div_nullsfalse | 0.331616 | 0.354789 | 1.0699x (+6.99%) |
| ansi_tiny_nullstrue | 0.280441 | 0.327999 | 1.1696x (+16.96%) |
| try_tiny_nullstrue | 0.278682 | 0.326081 | 1.1701x (+17.01%) |
| space_ansi_tiny_nullstrue | error | 0.331180 | n/a |
| space_try_tiny_nullstrue | incorrect NULLs | 0.339310 | n/a |
| ansi_small_nullstrue | 0.278692 | 0.346784 | 1.2443x (+24.43%) |
| try_small_nullstrue | 0.296266 | 0.346855 | 1.1708x (+17.08%) |
| space_ansi_small_nullstrue | error | 0.340803 | n/a |
| space_try_small_nullstrue | incorrect NULLs | 0.342943 | n/a |
| ansi_int_nullstrue | 0.298088 | 0.348097 | 1.1678x (+16.78%) |
| try_int_nullstrue | 0.297372 | 0.346664 | 1.1658x (+16.58%) |
| space_ansi_int_nullstrue | error | 0.344690 | n/a |
| space_try_int_nullstrue | incorrect NULLs | 0.344480 | n/a |
| ansi_long_nullstrue | 0.298149 | 0.349013 | 1.1706x (+17.06%) |
| try_long_nullstrue | 0.301119 | 0.346398 | 1.1504x (+15.04%) |
| space_ansi_long_nullstrue | error | 0.341359 | n/a |
| space_try_long_nullstrue | incorrect NULLs | 0.339234 | n/a |
| ansi_wide_long_nullstrue | 0.298484 | 0.347555 | 1.1644x (+16.44%) |
| try_wide_long_nullstrue | 0.296635 | 0.346523 | 1.1682x (+16.82%) |
| legacy_long_nullstrue | 0.312981 | 0.309725 | 0.9896x (-1.04%) |
| legacy_int_nullstrue | 0.312350 | 0.307381 | 0.9841x (-1.59%) |
| legacy_float_nullstrue | 0.353312 | 0.348117 | 0.9853x (-1.47%) |
| decimal_nullstrue | 0.317855 | 0.314479 | 0.9894x (-1.06%) |
| numeric_nullstrue | 0.355610 | 0.349459 | 0.9827x (-1.73%) |
| native_long_nullstrue | 0.171403 | 0.170662 | 0.9957x (-0.43%) |
| null_div_nullstrue | 0.328545 | 0.353647 | 1.0764x (+7.64%) |

Canonical short integer strings take 3.3989-4.3894 times the previous
execution time, an additional 11.887033-12.475706 ms per 1,048,576 rows.
For example, ANSI BIGINT without NULLs rises from 4.681995 to 16.991698 ms
(3.6292x); TRY_CAST INT rises from 3.652694 to 16.032962 ms (4.3894x).
The 19-digit BIGINT configurations take 1.8393-1.9488 times the previous time.
These costs are present even when the input has no surrounding whitespace.

The changed plans add native BTRIM before the existing conversion. A later
optimization should test whether trimming and conversion can share one pass
or avoid materializing an intermediate string array, while retaining all 34
edge bytes and strict/TRY errors. This run identifies the changed path; it
does not measure or isolate allocations, copies or instructions.

Planning for the short integer configurations rises 15.04%-24.43%; the
19-digit configurations rise 15.85%-16.82%. NULL-DIV retains the same physical
NULL projection but planning rises from 0.331616 to 0.354790 ms without NULL
inputs (+6.99%) and from 0.328545 to 0.353647 ms with NULL inputs (+7.64%).
That analyzer/planning cost remains open alongside the conversion cost.
NULL-DIV execution varies -1.92% to -0.25%.

Legacy CAST, Decimal, numeric and native-cast controls vary -1.19% to +4.04%
in execution and -1.73% to +0.14% in planning. The largest control difference
is unchanged legacy INT without NULLs, 6.152131 to 6.400803 ms (+4.04%).
All four candidate process medians are higher in that control, so it remains
an attribution question rather than being discarded as noise. These controls
do not show a slowdown comparable to the changed strict/TRY paths; they do
not establish the absence of a global regression.

Padded strings take 21.185857-23.087113 ms in the candidate. Their baseline
errors or incorrect TRY_CAST NULLs prevent a throughput ratio. All samples,
including slower individual processes, are retained. No measurement was
rerun or removed to improve the result. These costs are recorded for the
performance owner, not accepted as final performance.

Host: AMD Ryzen 7 8845HS w/ Radeon 780M Graphics; `Linux-6.19.14-200.fc43.x86_64-x86_64-with-glibc2.42`; `rustc 1.98.1 (48a229cea 2026-09-01)`. The guard recorded 0 waits for other compiler/JVM/benchmark processes between runs. The host was not isolated or reserved; the guard does not prove that
unrelated activity could not start during a run. No timing process was
discarded or repeated. Same-binary calibration, instructions, allocations
and memory were not measured in this compatibility slice, so small differences
remain unattributed.

The existing [CAST performance leaf](https://github.com/mag1cfrog/delta-arrow-reader/issues/159)
owns the unresolved costs and their final attribution/acceptance. This slice
records compatibility and a bounded comparison; it does not close historical
CAST costs or combine measurements from different baselines into a global
regression percentage.

## Reproduction

The two variants use the same 92 selected source paths. Only the CAST
resolver and local-column analyzer file change. All 414 linked library
hashes were checked; only `sail_plan` differs. Shared sources, executable
slots and Delta inputs were restored after testing.

| Executable | SHA-256 |
| --- | --- |
| before probe | `5b377a0803ba365f5610cfa31cee3b0123e21c7768c675fca4154e5982fc30e4` |
| before runner | `f1b1d49dac5af3624cf53e52b37ee4ba38af05dc854475b2b4bf45cbdef6894d` |
| after probe | `607ef3f7636b391615d743166ddf588a1b9a0b553a7c141dadd13b2ae532261d` |
| after runner | `a8d9ce69bf41804c8263f858919ee71beebc6aee2fd6fc5769874925f43ad376` |

| Benchmark | SHA-256 |
| --- | --- |
| before | `527ab0f3db8406d21bb1b8aad95f58bb4712232baec5c7c1c8b632c5c210ff42` |
| after | `7053a31c9181ba2c3db2cd1f389d37fc85debe591af1638b5ac37149d6441bfa` |

The [raw archive](strict-integer-cast-runs.json.gz) contains the fixed source
snapshots, build and test logs, Spark/native captures, failed first candidate,
source manifests, timing protocol and every timing sample. The initial
baseline-link attempt failed while Cargo replaced a shared transitive
artifact; it produced no executable or measurement. Final linking and timing
follow completed compilation/regression work. The failure is retained with
its diagnosis, alongside the rejected analyzer candidate.

From the repository root, verify without Rust or Spark:

```sh
python - <<'PY'
import gzip, hashlib, json
from pathlib import Path
root = Path('experiments/spark-sql')
record = json.loads((root / 'strict-integer-cast-results.json').read_text())
packed = (root / record['archive']['path']).read_bytes()
assert hashlib.sha256(packed).hexdigest() == record['archive']['sha256']
archive = json.loads(gzip.decompress(packed))
exec(compile(archive['files']['check-archive.py'], 'check-archive.py', 'exec'))
PY
python -m unittest discover -s experiments/spark-sql -p test_division_cast_classification.py
```

To repeat the focused capture, use the pinned Spark environment with
`strict_integer_cast.py spark OUT`, the selected probe with
`strict-integer-cast.jsonl OUT --physical-plans`, then
`strict_integer_cast.py compare SPARK NATIVE REPORT`. Archived `prepare.py`,
`build.py`, `link-bench.py`, `bench-reference.py` and `measure.py` record the
accepted source reconstruction, dependency overrides, commands and schedule.
Their cache paths describe the measured environment. The runtime-build leaf
owns the portable clean-checkout/CI handoff.
