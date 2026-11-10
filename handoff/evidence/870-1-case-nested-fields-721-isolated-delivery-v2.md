# Partial #721 CASE follow-up source handoff

Candidate `335a743fd46153f21f848f0054d45a1aead42584` is clean on `fix/spark-case-nested-result-fields`, still based on actual staging d72. Root's formatted prior candidate `3ad7061b022d61b170c90d28fa160e00c1cbcc22` is preserved at `test/spark-case-nested-fields-before-phase-repair`. This source candidate has not run. Rebase onto current staging remains pending while the fixes stabilize.

Root's original RED232 run produced 192 observations, 86 matches and one passed/three failed tests. Formatted candidate3ad produced 192 observations, 190 matches and three passed/one failed tests. Both retained the same two ANSI required-condition phase failures. The v1 handoff records the initial nested-field source change; this follow-up addresses the inherited phase boundary and a reviewer-identified test gap. Neither artifact claims broad issue #721 is complete.

## Exact follow-up

| Commit | Scope | Evidence and remaining check |
| --- | --- | --- |
| `94826aaac69d646c4b45594d16565c62ed52779d` | Add the verbatim independent pruned numeric fixture, compare it through the existing strict observer, and add a native invalid Boolean-condition control before/after Spark. | Two ANSI references require BIGINT after the dead BIGINT arm is removed. Total98 references/196 batch comparisons. Root runtime and mutation proof are pending. |
| `335a743fd46153f21f848f0054d45a1aead42584` | Reconstruct one zero-column row for a query-marked, empty-schema, produce_one_row EmptyRelation in existing `Prepare::local_batch`; update the provenance note. | Root explicitly authorized this narrow source addition after the inherited phase failure was traced. Direct zero-row, field-bearing, unmarked native and unsupported nonlocal inputs keep the existing path. Runtime remains pending. |

The existing nested `get_type`, analyzer cast guard, typed-NULL synthesis and physical CASE normalization sources are byte-identical to formatted3ad. The observer and original two CASE fixtures are also unchanged. `expr.rs`, common-type helper, host lib, Sail CASE/IF resolver and nested result converter still match the original d72 source. Exact hunk headers, hashes and caller coordinates are in the v2 JSON companion.

## Required-condition phase

The independent Spark query is `SELECT CASE WHEN CAST('bad' AS BOOLEAN) THEN ARRAY(TRUE) ELSE ARRAY(FALSE) END AS r`. In ANSI mode Spark records `CAST_INVALID_INPUT` at `optimized_schema`, which the unchanged observer maps to DAR `create_physical_plan`. Both root RED and candidate runs instead reported the same error at `collect`, with every available field matching. This is an inherited boundary, not a nested-field regression.

`has_cast` and `early_cast` already recognize the Boolean cast UDF. `cast_reachability` performs its existing full-parent projection evaluation only when `local_batch(input)` returns a batch. The old helper does not reconstruct a one-row EmptyRelation, so the literal query never reaches that evaluation. DataFusion's constant evaluator preserves failed conditional evaluation for runtime short-circuiting, which explains the observed collect phase.

The added match arm uses the same empty-schema/produce_one_row eligibility already present in `one_row_exprs`, plus the query-local `SparkSqlExpressionSemantics` marker. It builds a validated one-row RecordBatch with no columns. Existing `cast_reachability` then evaluates the whole parent, preserving CASE short-circuiting and propagating a required WHEN failure during physical-plan creation. No individual dead arm is evaluated separately, no fixture phase was changed, and no global folding rule was introduced. Direct zero-row and nonlocal behavior is unchanged by the match arm; derived LIMIT/empty and broader preparation consumers still require root regression gates.

## Numeric control and fault model

Root's independent fixture `case-nested-fields-721-capture-v1/numeric-pruning-spark.jsonl` is copied verbatim as `tests/fixtures/case-nested-numeric-pruning-spark.jsonl`, SHA256 `71e6c66aa31123954e0638bf6d09746178db9ccd8cdc0230220dcce2e55eb745`. It contains this SQL in both ANSI modes:

```sql
SELECT CASE WHEN FALSE THEN ARRAY(CAST(2 AS BIGINT)) ELSE ARRAY(id) END AS r
FROM case_nested_mixed
```

The original numeric test kept both result arms live. Even a faulty analyzer that skipped every marked result cast could pass it because physical CASE normalization still recovered the common BIGINT type. This control removes that repair opportunity: the FALSE/BIGINT arm disappears, so an unconditional analyzer bypass leaves a plain INT array and no CASE for the physical planner to normalize. Correct code retains the value-changing cast on ARRAY(id); `DataType::contains` rejects Long versus Int. Captured public and physical fields both require `long`, although encoded row strings alone would look the same.

`case-nested-fields-721-unconditional-cast-bypass-fault-v2.patch` contains only the one-line faulty unconditional-bypass mutation. The author did not apply or execute it. Root should test it in an isolated engine checkout, require the new control to fail on physical fields, then restore the candidate before normal gates. The source trace establishes the fault model; it is not an executed mutation result.

## Review criteria and acceptance

Correctness asks whether the required literal WHEN cast fails at the independently captured phase while dead branches and native queries retain their behavior. The old root observations confirm the gap; the new source path is traced, and runtime is pending. Test quality asks whether dropping analyzer value casts is observable after pruning; the independent numeric fixture now requires exact BIGINT fields, and root mutation execution is pending. The existing native observer comparison includes an invalid literal Boolean condition before and after a Spark query to protect the query-marker boundary.

The change adds no public declaration, dependency, option, constructor or recursive merger. `local_batch` keeps its existing input reconstruction responsibility. The one-row source uses validated RecordBatch construction, and every caller keeps its prior error handling. Names and caller roles are recorded before the implementation trace in the JSON inventory. Documentation Writer structure and Humanizer final prose pass were applied to these maintainer notes.

The original six #828 raw references for12 CASE comparisons and the four separate IF references remain unchanged. This source slice still makes no IF, COALESCE, #828 integration or broad #721 completion claim.

Author checks passed: exact capture and old-fixture hashes, source byte-preservation checks, clean immutable head, ASCII handoff and `git diff --check`. Root still owns196 focused comparisons, strict phase acceptance, isolated fault proof, inherited/native/depth/resource and empty/LIMIT regression checks, current-staging rebase, formatting, full gates and fresh review.

Skipped by author: Cargo/Rust/rustfmt, Spark/JVM, builds, installs, targets and GitHub writes.
Risk:335a743f is unexecuted; one-row preparation has several existing consumers, so root regression gates are required before acceptance.
