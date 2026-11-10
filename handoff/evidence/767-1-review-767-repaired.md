# Bulletproof Review #767: repaired USING key metadata

Assessment: required test work and runtime verification remain. I found no new confirmed production correctness defect in the frozen repair. The new shared schema and outer-reference deduplication paths are not exercised by the supplied tests. The exact CAST references retain a source-confirmed, pre-existing #710 metadata gate. No claim of runtime acceptance is made.

Reviewed checkout: `/home/hanbo/repo/delta-arrow-reader-spark-using-key-metadata`, branch `fix/spark-using-key-metadata`, immutable HEAD `3a1173414d6acd4a8a5b90569ee931a3ab204ffd`, base `588cfc30d574c9eec9dabc98fc70227d5a480096`. The review covers all 11 changed files, including four resolver source files, the complete integration test, three capture scripts, and all three reference files. I read the current Bulletproof Review skill and Ponytail full instructions. No applicable on-disk AGENTS.md was found in the checkout or its ancestors; the supplied global instructions apply.

This is a static review. The root agent owns all Rust/Cargo, Spark/JVM, native execution, and integration checks. I ran only Git and static Python checks. The checkout was clean at the beginning and after inspection. I created only this report and the separately authorized capture script in the external evidence directory. No source edits, dependencies, environments, worktrees, subagents, commits, pushes, or GitHub writes were made.

## Required finding

### P2: Add assertions for the two newly changed lookup paths

Locations: `tests/using_key_metadata.rs:25-150`; changed implementations at `vendor/sail/crates/sail-plan/src/resolver/schema.rs:42-51` and `resolver/expression/attribute.rs:249-268`, relative to the Spark crate.

The 48 duplicate/sharp-chain references exercise repeated keys in a single join and unique keys in a later join. None performs another USING join after `USING(id,id)`. All three ordinary ambiguity controls also use single-key inputs. As a result, removing the new deduplication from `resolve_column_candidates` would leave these assertions satisfied while a later join incorrectly rejects the repeated visible aliases as ambiguous.

A concrete missing case is:

```sql
SELECT id AS merged_id,l.id AS left_id,r.id AS right_id,t.id AS third_id
FROM left_mixed l
LEFT JOIN (SELECT id,rv FROM right_mixed) r USING(id,id)
LEFT JOIN (SELECT id,tv FROM third_mixed) t USING(id)
```

The second USING resolution must treat repeated aliases of the same first-join key as one candidate, retain genuinely different source fields as ambiguous, and preserve the output key values, metadata, names, and qualified-star repetitions required by the independent reference.

The same suite contains no correlated expression that can reach the changed `resolve_outer_field` branch. Removing that branch's new deduplication would also leave this suite satisfied while repeated outer keys remain ambiguous. For example:

```sql
SELECT l.id AS left_id
FROM left_mixed l LEFT JOIN right_mixed r USING(id,id)
WHERE EXISTS (SELECT 1 WHERE l.id=1)
```

Also check the hidden outer key `r.id`, because qualified hidden fields enter the same outer lookup. A local qualified-key assertion cannot establish the outer-reference result.

Smallest useful fix: capture these cases independently and add focused assertions at the public planning/execution boundary. Check both ANSI modes and the existing batch sizes. Keep the three ordinary ambiguity controls, add a CTE containing USING output under distinct relation aliases, and retain a FULL merged-versus-qualified key control. Exact Spark success/error conditions for those additional cases are currently pending; do not replace their expectations with the Rust implementation's output. This is a confirmed coverage defect, not a claim that the current source fails the scenarios.

The external capture prepared at the root agent's request is described below. It was not run by this reviewer and does not yet close this finding.

## Explicit gates

1. Frozen candidate execution is unverified. Run all four `using_key_metadata` tests on this exact HEAD. The reference replay makes 472 comparisons: 236 references at batch sizes 1 and 32, comprising 420 successful-result comparisons and 52 planning-error comparisons. The native before/after control and Arrow extension test must run as well.
2. The pre-existing #710 explicit CAST gate remains visible. Four raw references, `coerced_keys` and `coerced_full_keys` in both ANSI modes, expect the right explicit `CAST(id AS BIGINT)` result to lose inherited `source` metadata and acquire only the hidden marker. The unchanged cast resolver falls through to native `cast` at `resolver/expression/cast.rs:298`. Vendored `datafusion-expr/src/expr_schema.rs:75-90` clones source metadata in `cast_output_field`, called by the CAST arm at `:606-610`; aliases merge it at `:488-505`. The new join-local `with_metadata` correctly preserves that input map and therefore cannot remove this pre-existing extra `source: right`. These four references represent eight batch-expanded comparisons. Do not skip them, remove metadata keys from comparisons, rewrite raw expectations, or broaden #767 into generic CAST metadata work. A fully green combined replay requires the separate owner or an explicitly recorded external gate.
3. The new identity cases and three ordinary ambiguity expectations still need an independent Spark capture and production replay. The ordinary test's expected ambiguity strings currently come from the intended contract and Rust assertions, not fresh captured conditions. A contradictory independent result must be reconciled openly.
4. #762 integration remains unverified. Its frozen `9a8e05d7ce80ce552ac99fbed7e655277bef8c61` changes the same local attribute resolver to choose qualified-name tiers before nested access. Preserve that tier selection and deduplicate USING source identities inside the selected tier before the ambiguity check. Keep nested-field traversal deferred until the tier's candidate is selected. Neither branch alone proves the combined behavior.
5. The parent-provided previous production baseline and archived logs describe 244/472 mismatches, including 192 physical hidden-marker, 32 source-spelling, and 20 duplicate-qualified-lookup comparisons; the native before/after control passed, and the extension control failed on physical marker preservation. Those are baseline evidence only. The archived baseline result identifies staging HEAD `b5571c201c792ced3428bfd6db157e2d77bb3807`; do not relabel those results as execution of this frozen candidate or the earlier `d81f08a6` source snapshot. The static source model's 206/210 schema matches are not runtime evidence.

## Scope and connected behavior

The observable requirements are preserved source spelling, complete public and physical metadata, qualified/unqualified key lookup, qualified wildcard ordering and repeated occurrences, normal duplicate-source ambiguity, CTE/scope separation, distinct FULL coalesced keys, and unchanged native SQL.

The public entry path was traced from `src/lib.rs::plan_query` through SQL parsing and analyzer join construction to `PlanResolver::resolve_named_plan`. `resolve_named_plan` creates a fresh `PlanResolverState`, resolves the plan, lowers set operations, and obtains public names from registered fields. The analyzer retains the supplied USING identifiers and dispatches the supported join types. The shared join resolver resolves both sides with hidden fields available, resolves each USING name through `resolve_one_column`, builds existing Spark equality expressions, creates the DataFusion join, and then constructs visible and hidden output projections.

The consumer paths were followed beyond the changed builder:

- `resolver/query/project.rs::resolve_query_project` resolves names before wildcard expansion, rewriters, and fresh output aliases. `rewrite_wildcard` filters hidden fields only for plain stars and retains qualified occurrences; its stable hidden-first ordering is part of chained ordering behavior. `rewrite_named_expressions` creates distinct ordinary output IDs, so the new join-alias equivalence is not automatically applied to arbitrary duplicate projections.
- `resolver/expression/attribute.rs` first respects lambda and aggregate scopes, then local fields, hidden fallback, and outer fields. Local nested lookup deduplicates by canonical qualified source plus nested path. Outer lookup has its own candidate set. Local/outer sets are separate, so the state map alone does not combine candidates across scopes.
- `resolver/schema.rs` returns visible candidate columns, rejects more than one genuinely different candidate, and reports a missing key before a plan is returned. Its callers include USING/NATURAL joins, deduplication, and unpivot column selection. The new equivalence rule is therefore shared behavior, not only a SELECT expression change.
- `resolver/query/mod.rs` strips hidden fields at boundaries requiring ordinary output, verifies registered field IDs, and routes filter/sort/project/join paths that keep hidden fields while they are needed. `query/filter.rs` and `query/sort.rs` retain those fields while resolving predicates/order expressions.
- `resolver/query/alias.rs` removes hidden fields before applying a table alias. Explicit column renaming registers new ordinary IDs; `SubqueryAlias` changes current relation qualifiers. `query/cte.rs` stores resolved CTE plans in an RAII-restored CTE scope; `query/read.rs` clones CTE plans and registers fresh provider fields for normal reads. These are the important reuse paths for canonical source identity.
- `resolver/state.rs` field IDs are monotonically allocated within the top-level query. The new map stores generated join-alias ID to original field ID. Registration resolves the source transitively before inserting the new alias. `join_key_source` combines that original field ID with the column's current qualifier. This preserves alias separation rather than retaining a stale source relation. FULL coalesce projections use `register_field_name`, not `register_join_key_alias`.
- `resolver/expression/subquery.rs` enters a query scope with the current outer schema for scalar, EXISTS, and IN resolution. Scope guards restore the outer schema on success and error. The identity map remains query-local, with unique IDs preventing a later nested registration from overwriting an older identity. `PlanResolverState` is discarded after top-level failure or completion.
- Public physical planning clones session state and enables existing Spark-specific markers on that clone. `with_metadata` returns the input value while cloning its metadata and overriding the requested key. DataFusion scalar-function return-field derivation and physical projection schema construction preserve the resulting map; `rename_physical_plan` renames via a positional physical column projection. There is no public-name-based metadata overlay.
- Native SQL continues to use `SessionContext::sql` and DataFusion's own USING representation. The changed code is in the Spark resolver; no provider schema, session registry, or global optimizer state is mutated by this diff.

I also inspected existing join unit tests for floating-key equality, qualified key scope, natural chaining, plain/qualified stars, and struct expansion; the Spark crate's automatic integration-test discovery and dependencies; `tests/support/spark_inputs.rs`, `spark_results.rs`, and the capture helper; fixture README and UPSTREAM context. No public signature, dependency, export, flag, default, catalog format, or execution kernel changes in this range.

## Interfaces, names, and distinct concepts

The intended reader is a Rust/DataFusion maintainer changing join exposure or diagnosing schema parity. The naming check used an interface/call-site self-check after the initial diff was visible; it is not presented as an independent blind-reader experiment. I established the following interface readings and compared them with the implementations:

| Declaration or family | Interface reading and supporting cue | Body/caller comparison and verdict |
| --- | --- | --- |
| `resolve_query_join_using_columns(left, right, using, state) -> Vec<(String, (Column, Column))>` | Resolve parsed USING names against both input plans; Columns distinguish resolved inputs from identifier text. | Calls shared visible-column validation. The carried name now comes from the left field's registered source spelling, used for FULL merged output. Satisfied. |
| `resolve_query_join_using(... join_type, join_columns, state) -> LogicalPlan` | Build the shared USING/NATURAL join result from already resolved pairs. Mutable state signals registration. | Equality construction and result exposure remain together in one join orchestration. Satisfied. |
| `join_key_sources: HashMap<String, String>` and its field comment | Track source identity of uniquely projected USING aliases. The comment explicitly distinguishes identity from projection IDs. | This is a canonical identity map, not user metadata or a second field registry. The current qualifier is added by the getter. Satisfied; do not treat the returned token as a schema-resolvable replacement expression. |
| `join_key_source(&Column) -> Column` | Obtain the canonical source identity for comparing join aliases; receiver and adjacent map contract supply that context. | Used for equality/deduplication only. It returns the current relation plus original ID, preserving CTE alias meaning. Satisfied with the documented state context. |
| `register_join_key_alias(&Column, hidden: bool) -> PlanResult<String>` | Register another key alias, controlling visibility and returning an opaque field ID. It parallels existing field-registration methods. | Preserves registered display spelling and records canonical ancestry; errors precede insertion if the input field is unknown. Satisfied. |
| `left_key_sources`, `right_key_sources`, `preserved_keys`, `columns` | Canonical key identities versus actual builder output Columns, with a selected retained side. | Source comparison avoids dropping/re-marking a prior join alias by generated ID alone. The distinction is meaningful and maintained. Satisfied. |
| `sources` in local/outer/schema lookup | A local set of already considered canonical candidates. Immediate `HashSet` insertion and iteration expose deduplication. | Local nested lookup includes its path; schema lookup uses only top-level identity; outer candidates use their top-level names. The contracts differ, so this syntax should not be merged blindly. Satisfied. |
| `hidden_columns` | Existing local collection containing hidden key aliases plus untouched remaining columns. | The name is inherited; the explicit `extend(...map(Expr::Column))` exposes the passthrough remainder in the same local algorithm. No new external interface depends on a false hidden-only promise. No actionable new naming finding. |
| Tests and capture files | Spark parity, source distinction, native compatibility, and Arrow/user metadata are separately named responsibilities. | The names match their assertions, subject to the two missing paths reported above. Satisfied for naming. |

Three concepts remain distinct in the design: public display spelling in `FieldInfo`, physical/projection uniqueness in generated field IDs, and lookup equivalence in the join-source map. Arrow metadata carries the Spark hidden marker and provider metadata; it does not carry internal provenance. That separation avoids exposing internal identity in public schemas or using user-controlled metadata as an authority for ambiguity resolution.

## Per-criterion correctness record

Each row states the question and a concrete observation that would distinguish a satisfactory result from failure. Satisfied means the indicated source trace or static check supports that scope; it does not mean a runtime test passed.

| Question | Required observation or scenario | Evidence and verdict |
| --- | --- | --- |
| Does each join preserve the correct key side? | INNER/LEFT/semi/anti retain left; RIGHT retains right; FULL produces a merged value and exposes both source keys only through qualified lookup. | `join.rs:196-205`, `:238-291`, join schemas, and all six recorded join kinds inspected. Right semi/anti extensions share the right-preserving branch. Mark variants are not externally constructed by the exposed spec. Satisfied by trace; runtime unverified. |
| Are hidden markers restricted to suppressed keys? | LEFT `l.id` retains `{source:left}` and `r.id` gains `{source:right,__metadata_col:id}`; ON has no new marker. | Loop skips canonical preserved sources and columns absent from one-sided schemas, then applies `with_metadata` only to suppressed key expressions. Non-key columns pass through. Exact references assert all maps. Satisfied by trace. |
| Is provider metadata preserved in both public stages? | Marker addition keeps `purpose`, Arrow extension name/metadata, types, nullability, and rows. | Existing `with_metadata` clones the input map and passes its value through; physical return-field/projection/rename paths were traced. Extension test checks exact logical/physical maps in both ANSI modes with USING and ON. Runtime gate remains. |
| Is source spelling preserved? | Source `id` with `USING(ID)` remains `id`; separately named `Id`/`ID` keep their own spelling and marker values. | Visible alias registration reads source `FieldInfo`; hidden marker reads its own source name; FULL merged display uses the left source. The 32 follow-up references cover source casing and chains. Previous source-name finding is addressed by trace, pending runtime. |
| Do duplicate USING entries preserve wildcard occurrences? | `USING(id,id)` permits source lookup while plain/qualified stars retain repeated positions and complete rows. | Every visible and suppressed occurrence receives a unique projected ID, while alias ancestry supports lookup equivalence. Wildcards deliberately do not deduplicate. All 36 duplicate references were inspected and retain repeats. Satisfied by trace; runtime unverified. |
| Are ordinary duplicate fields still ambiguous? | Distinct output IDs or distinct relation aliases remain multiple candidates even with the same display name. | Ordinary project registration does not populate the join-source map. The three new test SQLs assert the intended ambiguity causes. CTE qualifiers are part of canonical identities. Satisfied by trace for the stated contract; fresh Spark conditions and execution pending. |
| Can a repeated join key feed another join? | Repeated aliases of one source become one candidate in a subsequent USING/NATURAL resolution. | `schema.rs` canonical deduplication supports this. No supplied assertion reaches it with duplicate aliases. Finding above. |
| Do correlated references use the same identity rule? | An inner scalar/EXISTS query can select the outer repeated visible or qualified hidden key, without merging different outer fields. | `resolve_outer_field` has its own canonical set, and outer-schema scope entry/restoration was traced. No supplied assertion exercises the repeated-key case. Finding above. |
| Are FULL merged keys distinct? | On right-only row `id=3`, merged id is 3 while `l.id` is NULL and `r.id` is 3; a later join must preserve those meanings. | FULL merged coalesce has a fresh unmapped ID and no source qualifier; source keys use separate mapped hidden IDs. Ordinary/chained FULL references include right-only rows. Additional full-chain and duplicate-FULL lookup oracle work is pending. Satisfied by trace for existing cases. |
| Are chained order and repeated sources retained? | LEFT-then-LEFT different keys and RIGHT/FULL-then-LEFT retain hidden-before-visible qualified ordering and the actual key list order. | New hidden occurrences are emitted in USING-list order, untouched earlier hidden fields remain, and qualified star ordering is stable. All sharp-chain reference schemas contain the required source order. No source mismatch found; runtime unverified. |
| Are values, multiplicity, and coercion unchanged? | Duplicate source rows stay duplicated; NULL equality remains unchanged; coerced original source fields retain their own types. | Equality construction and execution kernels are unchanged. The projection uses original columns, while FULL coalesce uses normal coercion. Exact row multisets and schemas protect these outputs. Runtime unverified; explicit CAST metadata gate separated. |
| Do missing or unavailable keys fail at the right boundary? | Missing USING keys reject planning; semi/anti absent-side attributes/stars are unresolved rather than silently exposed. | Shared resolver validates before join creation. Recorded errors classify 16 unavailable attributes, eight stars, and two missing USING keys. Test accepts only the mapped planning cause and rejects unknown reference conditions. Satisfied by trace/static reference validation. |
| Can error or nested scope state leak? | Failed planning leaves no persistent catalog/session change; nested query/CTE state restores its caller's schema/CTEs. | State is allocated per top-level resolution; IDs never reuse entries; QueryScope/CteScope drop guards restore owned scope values. Join map growth is private to that query. No source leak found. Runtime state-control execution remains open. |
| Is native SQL unaffected before and after Spark use? | Plain and configured native sessions agree, and the configured session still agrees after executing a Spark FULL USING query. | Native test captures full logical schema, physical schema, and batches for six joins over mixed/NULL/empty data, executes Spark in that same session, then repeats all native comparisons. Spark failure is recorded while native after-checks still run. The prior missing-post-Spark finding is addressed in assertions. Runtime gate remains. |
| Is public compatibility bounded to the requested change? | Output names/order/types stay stable except intended hidden metadata repair; no option/signature/config change. | Public entry/rename paths and complete diff inspected. Unique internal field IDs are retained for DataFusion validation. No new public compatibility defect found; runtime and #710 gates remain. |

## Test quality, maintenance, and simplicity record

| Criterion and question | Satisfactory observation | Evidence and verdict |
| --- | --- | --- |
| Coverage: do tests reach every changed rule? | Both local and outer lookup and subsequent join resolution must encounter repeated aliases, with controls for true ambiguity. | Local duplicate-key lookup is covered; outer and subsequent-join repeated-key paths are absent. P2 coverage finding. Broader native and extension controls are present but unexecuted here. |
| Assertion strength: would plausible faults fail? | Wrong marker side, loss of metadata, overwritten extension keys, casing changes, missing wildcard repetitions, row deduplication, and unavailable-side exposure should be rejected. | `capture_spark_query_result` compares complete schema maps and rows and separately enforces logical/physical field equality; direct errors must have a recognized cause during planning. Native compares schemas and batches against both baselines. These faults are protected in the recorded cases. Removing schema/outer deduplication survives this suite, establishing the coverage finding. |
| Expected-value independence | Expectations must come from raw Spark capture, not production helpers or a model copied from the implementation. | All 236 raw bytes match the external archived files. Capture definitions match all SQLs. No metadata filtering or expected-record rewriting exists in the replay. Ordinary ambiguity strings still need fresh external capture; pending. |
| Failure assertions and state guarantees | Reject the correct error without using an arbitrary failure as success; inspect the relevant persistent state boundary. | Tests map exactly three recorded error classes to specific planner causes; new ordinary ambiguity controls assert intended error text. No write transaction or persistent query mutation exists, so rollback/retry checks are not applicable to this diff. Native before/after remains the relevant state guarantee. |
| Names: can a maintainer identify roles from interfaces? | Source identity, display name, visible alias, and hidden marker must be distinguishable before tracing internals. | Interface inventory above compared with actual callers and bodies. No actionable new naming defect found. |
| Distinct concepts versus duplicate policy | A visibility rule change should have a clear owner; similar syntax must not collapse independently different contracts. | All USING/NATURAL construction shares one helper. Canonical ancestry has one state implementation. Local nested, outer top-level, and schema name lookup intentionally differ in candidate shape. Native USING remains separately owned. No duplicate-policy finding. |
| Focused responsibilities | Changing a hidden-key marker should not require unrelated parser/kernel/state changes; adding a join type should have predictable locations. | Join projection orchestration stays in the existing join resolver. State records only identity; metadata annotation remains in the result builder. Parsing, execution, and general CAST metadata are unchanged. No useful smaller split identified. |
| Comments/docs: do explanations help the next change? | Explain why aliases need ancestry/current relation and why wildcard occurrences are preserved; avoid false parity claims. | New comments state projection-identity and reused-CTE reasons and per-occurrence ordering. API behavior is still governed by existing public docs. README/UPSTREAM do not yet describe this work; completion documentation should record the final replay command, raw counts, and explicit #710 gate if still open. No new false public claim found. |
| Simplicity: is new machinery necessary? | Use existing metadata handling and retain unique field IDs; avoid schema overlays or provenance in user metadata. | Installed `with_metadata` supplies the physical guarantee without a new UDF. The small query-local identity map prevents duplicate generated IDs while allowing repeated lookup. Inlining it into each caller would duplicate policy; reusing one projected ID would conflict with DataFusion unique-name validation. No unnecessary new layer found. |
| Resources, concurrency, and security | No new I/O, persistent mutation, user-controlled provenance authority, or material algorithmic growth should enter planning. | Map/set storage is bounded by the current query's generated fields/candidates. Existing per-column/per-key scans remain; no global cache, execution scan, network, credential, or storage path is added. Source trace is satisfactory for this scope. No benchmark or load claim made. |

## Static checks and immutable evidence

Completed successfully:

- Verified frozen HEAD and clean `git status --short`; reviewed the full base-to-HEAD diff and ran `git diff --check`.
- Parsed all three capture scripts without importing PySpark or executing them. Extracted only their `queries` definitions with static AST selection: 78, 16, and 24 unique SQL definitions, exactly matching every corresponding captured `(id, ansi, sql)` record.
- Parsed all 236 raw records and verified 236 unique `(id, ansi)` pairs, identical provider headers, matching ANSI-pair SQL and expectations, all successful row widths, NULLs compatible with the declared fields, sorted complete row multisets, and expected schema equal to the retained analyzed stage.
- Confirmed 210 successful references and 26 error references: 16 `UNRESOLVED_COLUMN.WITH_SUGGESTION`, eight `CANNOT_RESOLVE_STAR_EXPAND`, and two `UNRESOLVED_USING_COLUMN_FOR_JOIN`. Optimized/executed stages differ from analyzed in 24 original and eight follow-up records; those original stages remain intact rather than being flattened into one schema claim.
- Confirmed byte identity with the original external evidence, using these SHA-256 values:

| Raw file | References | SHA-256 |
| --- | --- | --- |
| `using-key-metadata-spark.jsonl` | 156 | `df01c1c34e5240e6ab15aad271cf7c49c738935943733175c3344544474fea03` |
| `using-key-metadata-followup-spark.jsonl` | 32 | `9f7f8c9ba6309fc3f22f4b0a1b44b4816500732e58ea6553c747793c57d24bfa` |
| `using-key-metadata-boundaries-spark.jsonl` | 48 | `450557e8b461e619f12685b2a1cbb779b00dea4dd26351d621c1d4acfd20d9b5` |

Prior review and repair reports were consulted only after my own source/interface, caller/lifecycle, fixture, and assertion assessment. Their model conclusions were not treated as executed evidence. The earlier source-spelling and absent-native-after-control findings were rechecked against the current source and assertions before being considered addressed by trace.

## Pending independent identity capture

Prepared file:

`/home/hanbo/.local/share/delta-arrow-reader-review/spark-window-completion-2026-11-02/capture_using_key_identity_review_spark.py`

SHA-256: `b8dfff05e85fa289d4b43f0782254556e9d90c593c32967bff2ae0124d96e0ef`.

The script takes the existing provider/header reference path and a fresh output path as positional arguments. It refuses to replace output, checks Spark 4.2.0 and UTC, sets case-insensitive resolution, runs both ANSI modes, and retains the original header. Each success records full public, analyzed, optimized, and executed schemas plus complete sorted rows; errors retain exact condition, Python error type, message, and planning/execution phase. It includes 15 SQL definitions, for 30 intended observations:

- Direct unqualified repeated-key lookup and a repeated USING join followed by another USING join, with key and wildcard projections.
- Scalar and EXISTS correlated references to both visible `l.id` and hidden `r.id` repeated outer keys.
- A CTE containing USING output referenced under distinct aliases, both qualified and unqualified, plus a repeated-key CTE case.
- FULL merged-versus-qualified keys followed by another join and duplicate-FULL unqualified lookup.
- The exact three ordinary ambiguity SQLs in the new Rust test: duplicate projected fields with USING, duplicate projected fields with ON, and unqualified lookup across two aliases of an ordinary CTE.

Static AST/definition checks passed: 15 unique names, correct intended count, and exact equality of the three ordinary SQL strings. PySpark was not imported and Spark was not run. Root must capture these references, inspect actual conditions/results, add the smallest useful replay assertions, and rerun the frozen/new candidate as appropriate before this work can be accepted.

Skipped all heavy runtime checks and unrelated generic metadata/qualified-name implementations. Remaining risks are the unexercised identity paths, the explicit #710 CAST gate, and unverified #762 combined behavior; static source/model agreement does not close them.
