# Local Decimal CAST preparation

[Local Decimal narrowing preparation](https://github.com/mag1cfrog/delta-arrow-reader/issues/289) corrects the remaining runtime blocker identified by the [first-error reference](ARITHMETIC_FIRST_ERRORS.md). Strict Decimal-to-Decimal CASTs over local VALUES now use the existing analyzer preparation before physical repartitioning. The source row order determines the first failure for the tested projections, including when it differs from the outer ORDER BY.

This is an optional Rust patch over the selected PR 287 runtime, evaluated on integration `4da6f57ecad73f95bdbb8d6c019ec48e7ba58905`. Default-build adoption remains with [the build owner](https://github.com/mag1cfrog/delta-arrow-reader/issues/165). General diagnostic and schema acceptance remains with [arithmetic validation](https://github.com/mag1cfrog/delta-arrow-reader/issues/149).

## Change and observations

[sail-local-decimal-cast.patch](sail-local-decimal-cast.patch) extends the existing `string_cast` predicate, renamed `early_cast`, to include Decimal128-to-Decimal128 CASTs. It reuses local input discovery, conditional simplification and physical expression evaluation. TRY_CAST, other numeric conversions and the ordinary physical execution path keep their existing behavior. No dependency or execution node changes.

The shared predicate includes widening Decimal casts as well as narrowing ones. This keeps the rule simple; successful local casts have a small additional planning cost measured below. Nonlocal inputs do not enter local evaluation.

Fresh Spark 4.2.0/JRE 21 references cover 46 SQL queries in both ANSI modes. Both engines use UTC, `allowPrecisionLoss=true` and two partitions. Native batch sizes 1, 2 and 4 cover original and reversed source order. [The corpus](local-decimal-cast.jsonl) also includes positive/negative failures, ordinary rounding, rounding carry overflow, zero/NULL, CAST/TRY_CAST, LIMIT 0, WHERE false, CASE, COALESCE, a NULL parent expression and an unused projection.

- All 78 successful observations preserve Spark values and types. Their complete native logical and physical schemas are unchanged from the paired baseline.
- All 14 errors have a matching known range-error category and select the expected first invalid value. Native now fails in its analyzer, while Spark reports failure in optimization. These are recorded separately from the public error-stage comparison.
- The baseline has two success/error disagreements, for LIMIT 0 and an unused projection. Spark evaluates the local projection before those expressions are discarded. The same preparation change fixes both. WHERE false and dead conditional branches remain successful.
- The native regression covers both source orders, all three batch sizes, both ANSI modes and CAST/TRY_CAST. It fails against the paired baseline because the strict CAST reaches physical execution, and passes after the change.

The first-value check compares the reported numeric value, precision and the target rounding required by the saved Spark parameters. For example, Spark names source `999.99`, while Arrow reports rounded `1000` for DECIMAL(3,0). This establishes the selected failure in these controls, not a native SQLSTATE/parameter API.

Schema limitations remain explicit: two logical and eight physical nullability observations differ from Spark, all unchanged on shared successful paths. Fourteen early errors no longer have a physical schema because planning stops before constructing one; their logical schemas remain captured. These are not counted as physical-schema matches.

## Retained checks

The candidate replays 67 existing groups, totaling 31,890 observations including overlapping corpora. Existing comparator matches remain 30,513/31,890, with no lost agreement. The original 37 groups remain 6,603/6,784. These historical comparators have different coverage and diagnostic limitations; their totals do not establish full Spark compatibility or classify every error.

All shared successful results preserve the recorded logical and physical nullability. Of 179 changed raw observations, 174 only reorder rows in SQL without ORDER BY; exact row multisets, duplicates and NULLs are preserved. Five existing narrowing errors move into analyzer preparation. Complete old/new errors and results remain archived.

A separate 48-observation replay preserves the prior four first-error queries and their controls. It has 24 matching successful outputs, 12 known error-category matches and 12 ambiguous string CAST errors. The other three original error questions preserve their raw cause payloads. The narrowing query now selects positive `1` before repartitioning. Generic string-to-Decimal messages receive no new cause-match credit.

## Bounded cost record

The unchanged benchmark harness runs nine queries under two registered-input NULL configurations. Local queries use their own two or four VALUES rows; their SQL is identical across those configurations. Nonlocal controls read 262,144 rows in batches of 8,192. Timing uses one partition on CPU 2, four warmups and 21 samples per planning/execution phase. The eight-process order is before, after, after, before, after, before, before, after. Each figure is the median of four process medians.

| Query/configuration | Planning before -> after, ms | Change | Execution before -> after, ms |
| --- | --- | --- | --- |
| Local zero/NULL, first configuration | 0.613159 -> 0.627730 | +2.376% | 0.017046 -> 0.017141 |
| Local widening, first configuration | 0.790954 -> 0.809068 | +2.290% | 0.017242 -> 0.017192 |
| Local rounding, first configuration | 0.795713 -> 0.812764 | +2.143% | 0.017572 -> 0.017583 |
| Local rounding, second configuration | 0.766148 -> 0.786480 | +2.654% | 0.017302 -> 0.017497 |
| Nonlocal Decimal, no NULLs | 0.277516 -> 0.279890 | +0.855% | 2.353001 -> 2.348202 |
| Nonlocal Decimal, NULLs | 0.259206 -> 0.261732 | +0.974% | 2.291330 -> 2.286963 |

Local strict CAST planning increases by 0.013566-0.020333 ms, or 2.14%-2.65%. TRY/legacy planning ranges from -0.64% to +0.03%. The ordinary string CAST control's execution increases from 3.793986 to 3.843984 ms without NULLs, +1.318%, and from 3.810868 to 3.843434 ms with NULLs, +0.855%. Native numeric controls also vary. Every configuration verifies all output rows and types before timing.

These are bounded observations, not calibrated attribution or final performance acceptance. There is no same-binary timing calibration in this slice. All 18 configurations, absolute/relative times, four process medians, raw samples, physical plans and exact source/library identities are in `local-decimal-cast-results.json` and the archive. Follow-up stays with [the existing CAST cost owner](https://github.com/mag1cfrog/delta-arrow-reader/issues/159).

## Build identity and verification

Only `sail-plan` is rebuilt, in two private directories, over 504 frozen dependency artifacts from the accepted runtime. All 505 original artifacts are hash-checked again after validation. The private baseline reconstructs the accepted selected sources; unpatched crate files match the repository snapshot. Both variants use the same compiler flags. The unchanged schema probe source is `b16afa371401d107470ca21645b88a2deb5db19f2e0215473ed697b7d06bf42b`.

The accepted artifact set contains multiple rand versions and host/target builds of either. The direct build selects rand 0.10.2 and target either using the original dependency identities; it does not select an arbitrary last artifact with the same name. `selected-source-proof.json`, `accepted-sail-plan-metadata.txt` and the build manifests retain this evidence. Mutable cached Arrow source is not rebuilt or used to identify these frozen libraries.

Extract `local-decimal-cast-runs.json.gz`'s `files` map into a fresh directory. With PyArrow 25.0.1 available, run:

```sh
python /path/to/extracted/check-archive.py "$PWD/experiments/spark-sql"
```

The checker verifies the patch against the accepted source, embedded/native-test identity, focused references and first values, every retained comparison, schema preservation and all cost medians. The archive also contains private build/link commands and manifests, the Spark observer, complete captures and all raw logs. This slice does not rerun Delta lifecycle tests or full dependency suites, and does not close the parent validation or C04 area.
