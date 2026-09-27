# Three-valued IN and NOT IN filters

[Issue 276](https://github.com/mag1cfrog/delta-arrow-reader/issues/276) owns
this optional Rust repair. The accepted baseline is PR 275 integration
`2fa12742c717b5a6194ff1dba034a11fa8d34e38`.

## Behavior and implementation

The accepted runtime incorrectly returns a row for:

```sql
SELECT 1 AS r WHERE 7 NOT IN (SELECT CAST(NULL AS BIGINT) AS v);
```

NOT IN evaluates to UNKNOWN here, so WHERE must discard the row. Its left
literal cannot become a hash equijoin key. The resulting nested-loop anti join
does not use the null-aware flag, and filter pushdown can discard the NULL row.
The [optimizer patch](datafusion-not-in-filter.patch) makes the IN equality
`IS NOT FALSE` for a column-free anti predicate. TRUE or UNKNOWN excludes the
outer row; an empty subquery still keeps it. Correlation predicates remain
outside that wrapper and must evaluate to TRUE. Top-level filtering retains
one native anti join.

The first candidate fixed that example but broke a previously correct
`WHERE COALESCE(NOT IN(...), true)` result. A strict Spark comparison missed the
regression because Spark also gives the wrong result. Two shared prerequisites
complete the repair:

- The [expression patch](datafusion-in-nullable.patch) derives IN/NOT IN
  nullability from both the left expression and the subquery output. Otherwise
  simplification can discard COALESCE or IS NULL before decorrelation.
- Embedded filter expressions reuse the existing value-producing IN helper.
  Its match, NULL-presence and nonempty mark joins preserve UNKNOWN through
  COALESCE, IS NULL, CASE and negation. This adds two mark joins to an embedded
  IN expression compared with the old boolean-only preparation.

The [physical projection patch](datafusion-in-projection.patch) also fixes the
shared mapper used by hash and nested-loop joins. Its child-index calculation
assumes concatenated output columns, so semi/anti/mark joins use the existing
embedded-projection fallback. The original mapper already fails an independent
native test; the newly correct COALESCE plan makes that bug reachable.

Five files change in the 101-entry selected source set. The two newly selected
physical-plan files match unmodified DataFusion 54.1.0; their baseline hashes
are recorded separately from the preceding 99-source archive. The repair reuses
existing DataFusion expressions and native joins, with no new execution node,
dependency or Python UDF. Top-level column-key decorrelation retains its path;
the shared physical projection check also applies when its join type requires
it. Production execution remains Rust.

The rejected anti-only source, builds, captures and regression evidence remain
under `rejected-anti-only/` in the archive. The second candidate and its
physical projection failure remain under
`rejected-projection-error/`. No performance schedule was run for either
rejected candidate.

## Validation and remaining work

The [1,952-query corpus](not-in-null-filter.jsonl) retains all 1,796 preceding
queries and adds 156. Both ANSI modes cover literals/foldable/NULL left values,
empty/matching/unmatched/duplicate/NULL RHS, aliases, computed CAST/division,
GROUP BY, ordering, LIMIT/OFFSET, correlation and embedded boolean controls.
Default native batches contain two rows, with batch 1/64 controls. All 3,592
retained Spark observations preserve status, rows, types and error condition
in the fresh reference.

Strict Spark agreement changes **3,630/3,904 -> 3,721/3,904**. All four assigned WHERE NOT IN failures return the required empty result. The 320 independent legacy SQL checks and eight independent boolean boundary observations pass. SQL expectations preserve the approved NULL policy where Spark differs.

Twelve newly added observations lose strict Spark agreement because the candidate now returns the independently expected SQL result. No previously retained strict agreement is lost. The preceding matrix improves 3,445/3,592 -> 3,449/3,592. The two new ANSI IS NULL error-pruning observations are unchanged native behavior and stay with issue 142; two wrapped Spark stage failures stay with issue 149.

| Remaining observations | Finding | Owner |
| ---: | --- | --- |
| 13 | Correlated EXISTS/IN and UNION error ordering | [141](https://github.com/mag1cfrog/delta-arrow-reader/issues/141) |
| 14 | Grouping, HAVING, grouping sets/ROLLUP and NULL-test error pruning | [142](https://github.com/mag1cfrog/delta-arrow-reader/issues/142) |
| 13 | Modulo error condition | [149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) |
| 2 | Wrapped Spark stage error condition | [149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) |
| 141 | Independently checked three-valued SQL | Approved SQL NULL policy |

Full SQL, rows, errors, schemas and plans stay in `classification.json`. Strict agreement is not a claim of complete Spark compatibility. Broader relational, grouping and diagnostic acceptance retain their existing owners.

Logical nullable differences change 319 -> 122; physical differences change 220 -> 220. There are 249 changed nullable records among successful pairs and 19 changed complete paired-error payloads. The archive preserves each one.

Rust suites pass 314 function, 58 planner and 28 runner tests. The exact embedded
optimizer regression covers 16 shapes; the expression regression covers eight
IN/NOT IN nullable combinations. A third native regression covers 20 projection
shapes across all ten join types and renamed outputs. All three fail against baseline libraries and pass
against candidate libraries. Retained numeric/subquery suites and historical
replay are recorded with their original comparators. Strict DIV remains
350/350, integer overflow 616/616, and all 131 integer errors are unchanged.
Real-Delta retains 116 outcomes and 18 adapters; its Spark comparison remains
47 matches, 58 differences and 11 pending adapters.

Historical replay retains 911 errors; 0 complete error payloads change. Complete payloads and represented outcome changes are preserved separately. Both builds keep the same package/features, 505 artifact records and 414 named libraries; 28 library hashes change through the Rust dependency chain.

## Bounded performance comparison

The Rust harness checks all output rows before timing. It now supports either
zero rows or the complete input, and records failed baseline output checks
without assigning them a speedup ratio. COALESCE and IS NULL references use an
independent SQL truth table, with Spark's original wrong outputs also retained.

Nineteen queries use both nonnullable and 10%-NULL fixtures, 262,144 input rows,
batch 8,192, one partition, four warmups and 21 samples per phase/process.
The fixed order is before/after/after/before/after/before/before/after on CPU 2.
Each reported value is the median of four process medians. Plan-state resets
stay outside execution timing.

There are 29 equivalent configurations and 9 candidate-only configurations. Comparable printed plans are equal for 19 and change for 10.

The largest equivalent-output costs are local to embedded NOT IN, nullable matching anti joins and COALESCE. COALESCE planning rises 0.7085860 -> 2.9194525 ms (+312.011%); execution rises 0.1031165 -> 0.7933485 ms (+669.371%). Its baseline happens to return the correct rows for this fixture despite the incomplete NULL preparation. The candidate uses the independently correct SQL result across the bounded truth-table checks.

| Query/input pattern | Plan before ms | Plan after ms | Change | Execute before ms | Execute after ms | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| native_numeric_nullsfalse | 0.1739980 | 0.1759265 | +1.108% | 0.0853535 | 0.0869965 | +1.925% |
| embedded_anti_nullsfalse | 0.8939455 | 1.3355010 | +49.394% | 0.2207105 | 0.4596040 | +108.238% |
| ordinary_numeric_nullstrue | 0.3089490 | 0.3098605 | +0.295% | 0.1786020 | 0.1835210 | +2.754% |
| anti_nullable_match_nullstrue | 0.6609575 | 0.6219205 | -5.906% | 1.5945470 | 2.6266935 | +64.730% |
| coalesce_anti_nullstrue | 0.7085860 | 2.9194525 | +312.011% | 0.1031165 | 0.7933485 | +669.371% |

There are 7 increases where every candidate process median exceeds every baseline median: `embedded_anti_nullsfalse planning`, `embedded_anti_nullsfalse execution`, `anti_nullable_match_nullstrue execution`, `anti_empty_nullstrue execution`, `anti_exists_nullstrue planning`, `coalesce_anti_nullstrue planning`, `coalesce_anti_nullstrue execution`. All other increases also remain evidence; overlapping ranges are not grounds to dismiss them. Ordinary numeric nullable execution rises 2.754% and native nonnullable numeric execution rises 1.925%, so no claim of zero control cost is made.

[Issue 161](https://github.com/mag1cfrog/delta-arrow-reader/issues/161) retains
all absolute/relative costs, SQL/settings, raw samples, process medians, output
checks, plans and source/binary/library identities. No same-binary calibration
or instruction/allocation/memory counters were collected. Earlier costs,
attribution and final performance acceptance remain open.

## Reproduce

`not-in-null-filter-results.json` pins `not-in-null-filter-runs.json.gz`.
Extract the archive's `files` map into a scratch directory and run
`check-archive.py` from the matching repository checkout. It verifies all three Rust
patches, retained captures, independent SQL checks, rejected-candidate evidence,
native regressions, Delta outcomes and every raw timing median offline.

`prepare.py` reconstructs the accepted source set from the pinned preceding
archive plus the two recorded unmodified DataFusion 54.1.0 files. Apply the optimizer patch at the selected datafusion-optimizer crate
root, the nullable patch at the selected datafusion-expr crate root, and the
projection patch at the selected datafusion-physical-plan crate root.
`build.py` installs the candidate temporarily and restores all shared sources
and executable slots. Private libraries preserve each build's linkage.
Default-build and clean-checkout adoption remain with
[issue 165](https://github.com/mag1cfrog/delta-arrow-reader/issues/165).
