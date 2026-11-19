# SUBSTRING prefix row cost (#1388, PR 2)

The isolated SUBSTRING ANSI-on median is within this run's native noise floor;
ANSI off remains 16.214383 ms above native, exceeding 3.847254 ms noise. Both
combined-query modes also exceed noise. The target is not fully met, and the
single bounded SUBSTRING implementation round is complete.

## Before and after

Completion includes planning through stream drain. Each value is the median of
three process medians, each from three retained iterations after one warmup.
Min/max and IQR describe the process medians. ns/row divides by 6,001,215;
decisions use unrounded nanoseconds. Before is the separate
[PR 1 campaign](strings-1388.md); after is the fixed affected-shape run below.

| Shape | Campaign | Mode | Completion ms | ns/row | Min-max ms | IQR ms |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Combined | Before | Native | 350.202303 | 58.355234 | 345.659365-369.263736 | 11.8021855 |
| Combined | Before | Spark ANSI off | 420.596906 | 70.085292 | 420.386486-426.388724 | 3.0011190 |
| Combined | Before | Spark ANSI on | 418.043935 | 69.659883 | 411.170809-425.022967 | 6.9260790 |
| Combined | After | Native | 342.956079 | 57.147774 | 335.423629-343.136163 | 3.8562670 |
| Combined | After | Spark ANSI off | 370.216248 | 61.690216 | 364.547539-370.914273 | 3.1833670 |
| Combined | After | Spark ANSI on | 377.845356 | 62.961476 | 375.676409-378.734568 | 1.5290795 |
| SUBSTRING | Before | Native | 193.595235 | 32.259340 | 175.662970-194.837072 | 9.5870510 |
| SUBSTRING | Before | Spark ANSI off | 240.511022 | 40.077055 | 218.560714-245.716952 | 13.5781190 |
| SUBSTRING | Before | Spark ANSI on | 235.056081 | 39.168082 | 223.432983-242.942398 | 9.7547075 |
| SUBSTRING | After | Native | 177.065963 | 29.505019 | 176.394816-183.118724 | 3.3619540 |
| SUBSTRING | After | Spark ANSI off | 193.280346 | 32.206869 | 191.149669-201.636355 | 5.2433430 |
| SUBSTRING | After | Spark ANSI on | 180.695463 | 30.109813 | 171.330270-200.469198 | 14.5694640 |

The completion flag is strictly `Spark completion - native completion > noise`,
where noise is the absolute difference between native A/B arm medians under the
same aggregation. Equality is false; there is no 1.25x/50 ms cutoff.

| Shape | Campaign | Noise ms | ANSI off gap ms | Off flag | ANSI on gap ms | On flag |
| --- | --- | ---: | ---: | --- | ---: | --- |
| Combined | Before | 0.437082 | 70.394603 | true | 67.841632 | true |
| Combined | After | 0.719106 | 27.260169 | true | 34.889277 | true |
| SUBSTRING | Before | 18.656630 | 46.915787 | true | 41.460846 | true |
| SUBSTRING | After | 3.847254 | 16.214383 | true | 3.629500 | false |

Native completion also fell between campaigns: 7.246224 ms for combined and
16.529272 ms for isolated SUBSTRING. These separate collections are not paired
observations, so the before/after difference is not an exact causal allocation
to the patch. ANSI-on SUBSTRING spans 171.330270-200.469198 ms with a 14.569464 ms
IQR. Its false flag describes this collection, not a general parity guarantee.
Spark off/on preflight plan texts are identical for both shapes; the different
medians do not establish an ANSI-setting cost. CONCAT and UPPER retain their
labeled PR 1 evidence and were not retimed here.

[Raw samples](strings-1388-substring-samples.jsonl.gz), the unchanged
[summary output](strings-1388-substring-summary.json) and [all 170 metric statistics](strings-1388-substring-metrics.json.gz)
retain decision inputs, phases, first-batch results, pool/RSS and source counters.
The inherited summary's `profile_cases` field reports eligibility only: no PR 2
profiles ran.

## Change and remaining gap

The [Sail guard](https://github.com/mag1cfrog/delta-arrow-reader/blob/49176fd72984a19fef9cd10c432458c34ddf8c2b/crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-function/src/scalar/string/spark_substring.rs#L165)
delegates only Utf8View input with non-NULL scalar Int64 position 1 and
nonnegative scalar Int64 length to the installed `unicode::substr()` singleton.
The measured `(1,12)` prefix therefore uses the same row kernel as native SQL,
removing the old registry kernel's unconditional positional-length calculation,
including its full character count on this short-prefix path. Reuse of
DataFusion's bounds/view implementation avoids a new Unicode algorithm or fork.
[PR 1's source attribution and original profiles](strings-1388.md#source-attribution)
remain the evidence for that cost; no profile archive is duplicated.

Utf8View preserves the old executed storage. Position 1 keeps byte_start zero:
the native builder's [u32 offset addition](https://github.com/mag1cfrog/delta-arrow-reader/blob/49176fd72984a19fef9cd10c432458c34ddf8c2b/crates/delta-arrow-reader-spark-sql/vendor/dependencies/datafusion-functions/src/strings.rs#L1141)
can overflow for other positions in valid views into buffers larger than 4 GiB.
Review narrowed the guard to prefixes before measurement. Other positions,
storage, NULL/column bounds and negative lengths retain the old implementation;
BINARY and lazy/async evaluation remain unchanged. Omitted length still expands
to INT_MAX. [UPSTREAM.md](../../UPSTREAM.md#string-and-binary-substring) records
the registry `datafusion-spark` 54.1.0 provenance and unchanged lockfile checksum.

The isolated Spark plan still has an extra ProjectionExec and preserves the
shared NULL-selection/evaluation path. Existing counters put the summed
two-partition prefix-expression elapsed medians at 96.210025/96.589585/96.968385 ms
for native/off/on. The extra outer projection's summed elapsed medians are
0.314068/0.318355 ms for off/on; planning medians are
1.441327/2.311463/2.301224 ms. These elapsed counters can overlap and include
preemption; they are not CPU time or an additive wall-time breakdown. The prefix
expression counters are near native, but the remaining 16.214383 ms completion
gap is not fully attributed. Neither the projection nor observed variation proves
its cause. No second SUBSTRING strategy or additional experiment was run.

## Equality, provenance and artifacts

All six full exports, four decoded native/Spark comparisons and six old/new
comparisons passed for 6,001,215 rows each. Values, NULLs, multiplicity, SQL types
and actual Arrow storage match; Int64/Int32 keys and Utf8View results are retained.
Each configuration reads the same six source paths with two output partitions,
complete counters and equal full range multisets. Combined delivers 81,807,546
bytes and SUBSTRING 80,224,680 bytes per configuration. All 90 retained samples
match the untimed rows, partitions and exact source I/O. No shape was excluded.

The [full preflight](strings-1388-substring-preflight.json.gz) retains ranges and comparison
proofs; the [bound receipt](../strings/substring/preflight-receipt.json) retains
compact expectations. The [plans archive](strings-1388-substring-plans.tar.gz) contains six
pre-export-sort plans, four native/Spark diffs and six before/after plan diffs,
with a hash manifest. IPC files remain outside Git.

The [protocol](strings-1388-substring-protocol.md) was committed in `e944900d`; `2dd66fd4`
corrected only the new dictionary fixture's explicit logical-NULL field
declaration, retaining NULL dictionary values and the original expectations.
Local Linux fmt, strict root-package all-target Clippy and workspace tests passed:
3,169 passed, zero failed, three ignored. Every original 3,167 named outcome is
unchanged; the five added tests cover the direct old oracle, dispatch, resolved
storage and effect/error boundaries. Independent Spark captures are unchanged.

The measured freeze is `49176fd72984a19fef9cd10c432458c34ddf8c2b`, after the untimed receipt was committed and
its exact source rebuilt. The executable and independent read-only seal both hash
to `fa06fd38820390e60de15e25dc9c446bbccb5ada0c3d851b20b109701ff5d308`.
Product baseline is staging `7cbb9dbf`; emitted `source_commit=8260c58b` identifies
the unchanged query manifest's origin, not the PR 2 product commit. The
[metadata](strings-1388-substring-metadata.json) binds full commits, sources, commands,
toolchain and raw/compressed hashes.

One local Linux release schedule ran with 30 processes, 30 warmups and 90 retained
samples, without retries, concurrency or profiles. Rust 1.96.0 opt3/debug1,
affinity 2,3,4,5, two workers/partitions, batch 8192 and the common view/session
remain fixed. The ten frozen files, all 16 PR 1 paths and 86 protected historical
assets remain unchanged. [Validation](strings-1388-substring-validation.json) and
[compressed details](strings-1388-substring-validation-details.json.gz) retain named outcomes,
build/preflight and audit receipts, and the existing operator-counter analysis.

CONCAT remains the next cost from PR 1's attribution. This report
leaves the SUBSTRING residual open without choosing another optimization.
