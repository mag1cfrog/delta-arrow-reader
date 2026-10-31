# Spark SQL restart: work plan and progress

This is a temporary engineering plan for resuming the frozen Spark SQL work. It
is not a supported-feature reference and is not part of the documentation site.
Keep implementation evidence and unfinished work here until the relevant slice
has been reviewed. Publish the final support contract separately.

## Current slice

S01 is complete and merged into `feat/spark-sql-staging` by PR #599, commit
`3f4245fc258b59c835fc683d4c34a03bbe322231`. Its final CI run passed 408 tests on
each of Linux, macOS and Windows. Issue #165 is closed.

S02's first slice, #116 ARRAY_CONTAINS NULL semantics, merged into staging through
PR #600 as `c4e75faf`, after all three platform jobs passed. The following #150
conditional review has resumed. Its floating NULLIF equality fix, #601, merged
through PR #602 as `5b4b92f5` after all three platforms passed. IFNULL/NVL temporal
coercion, #603, merged through PR #607 as `4238a6c4` after all three platforms
passed. Searched CASE condition types, #604, merged through PR #609 as
`0f2b595d` after all three platforms passed. Collection-size NULL rules, #606,
merged through PR #611 as `929810ff` after all three platforms passed.
Mixed-temporal promotion has a separate implementation issue, #605, in S03.
Shared COALESCE/IFNULL/NVL evaluation, #608, merged through PR #614 as
`16df0951` after all three platform jobs passed. NULLIF evaluation, #610, is
reviewed and pushed as `f43bcf5b` in PR #617, with CI running. Known NULL array projections,
#612, are reviewed and pushed as `80d79acf`; their PR follows #610. Array
predicates at join boundaries, #613, are reviewed and pushed as `7dcfda7a`.
S03's #615, scalar VALUES Decimal common types, is reviewed and pushed as
`04b191a6`. VALUES row-width validation, #616, is reviewed and pushed as
`28d93682`. Mixed-temporal conditional types, #605, are reviewed and pushed as
`22151773`. The current slice is #618, NULLIF first-argument result preservation.
Its separately confirmed comparison-policy gap is assigned to #619. The
relational coverage tracker #170 and reference review #141 have resumed.

The active Spark frontend is developed and integrated on
`feat/spark-sql-staging`. Each implementation PR targets it and contains one
issue's changes. The original `feat/spark-sql-extraction` branch remains frozen.
Adoption into `main` is a separate decision after the staged work is evaluated.

### Consolidated S01 baseline

S01 consolidates the accepted extracted Sail frontend and its accepted runtime patches
into an independent crate in this repository. A clean checkout must build the
same selected implementation without old private caches or manual patch stacks.
The crate must accept the host's DataFusion context and registered providers;
Reader storage, authorization and scan optimization stay with the existing core.
This slice does not switch the CLI to Spark SQL or claim full Spark compatibility.

The frozen branch is `feat/spark-sql-extraction`, commit
`c426ef86e4da43eb57f270f1a4945c9dcc6559fe`. The last accepted runtime change is
PR #291, merge `2a82385d1d420cbd88461d43ad6015801e3647ea`. PR #292, merge
`bde063e527547f20e687548cd8cda098c1b0fb88`, adds the final evidence index without
changing that runtime. The default vendor checkpoint does not include all
accepted repairs. Selected dependency changes include DataFusion and Arrow;
their effects on other workspace consumers must be checked.

### S01 acceptance

- [x] Inventory the accepted source set, dependency versions, patches and license
  notices. Exclude rejected prototypes and the unreviewed #293 patch.
- [x] Consolidate the selected implementation behind one DAR Spark SQL crate,
  retaining upstream module boundaries where they help maintenance.
- [x] Build from checked-in source with Cargo. No Python UDF, Spark server,
  additional storage reader or private build artifact is required at runtime.
- [x] Exercise accepted repairs through the consolidated public entry point.
  Preserve recorded unresolved cases as unresolved; do not redefine them as passes.
- [x] Check core-only and existing native DataFusion behavior when dependency
  patches are shared. Preserve validation and error handling while moving code.
- [x] Run a clean-context Sol/high review using the current Bulletproof Review
  skill. Record its scope, each criterion's evidence, findings and verification
  gaps. Fix findings and obtain a review of the fixes.
- [x] Complete local validation and independent review before committing this
  slice and progressing to the next issue.

## Ordered queue

Each row is a work area, not permission to combine its independent fixes into one
PR. Restore one implementation or validation issue at a time. Reuse existing
native parent/sub-issue relationships; give newly confirmed independent causes
their own small implementation issues. Assign resumed work to `mag1cfrog`.

| Order | Work | Existing issues | Completion evidence |
| --- | --- | --- | --- |
| S01 | Reproducible independent Spark SQL crate | #113, #165 | Selected-source reconstruction, builds, repair regressions and independent review |
| S02 | NULL and array semantics | #116, #150, #606, #608, #610, #612, #613 | ARRAY_CONTAINS truth table; NULLIF equality; conditional admission/evaluation; collection-size modes |
| S03 | Decimal and conditional input coercion | #141, #150, #605, #615, #616, #618, #619 | VALUES Decimal/integer values and types; NULLIF result/comparison types; mixed CASE Decimal execution; retained non-temporal IFNULL/NVL/COALESCE coercion; mixed-temporal promotion |
| S04 | Conditional aggregation and window contracts | #142 | A separate COUNT_IF FILTER fix, then aggregation/window coverage |
| S05 | Dates, timestamps and timezones | #152, #153, #154 | DATE_DIFF width, tolerant parsing, TIMESTAMPDIFF units/elapsed time and DST boundaries |
| S06 | JSON extraction and hashing | New bounded children of #167 | Requested get_json_object and sha2 behavior, nulls, invalid inputs and dynamic arguments |
| S07 | Arithmetic schema and diagnostic contracts | #149, #293 | Classify remaining schema/error differences; separately fix multi-output CAST error order and the retained BROUND gaps below |
| S08 | Reader and CLI integration | #189, #190, #191, #192 | Select the same dependency patches at the consuming workspace root; verify Delta snapshots/rows, stream lifecycle, read-only admission, cancellation and resource limits |
| S09 | Names, projections and subqueries | #137, #138, #139, #140 | Identifiers, SELECT * EXCEPT, case-sensitive resolution and retained nested correlated queries |
| S10 | Remaining string and nested-value coverage | #151, #155, #156 | Declared string/binary behavior, ELEMENT_AT modes, maps, arrays and explode contracts |
| S11 | Release comparison and support statement | #157, #164, #166, #620 | Equivalent-output size/planning/execution comparisons under the same release settings; NULLIF selection cost; user-facing support documentation |

The other eleven historical performance leaves remain deferred unless these
comparisons identify a relevant regression or bottleneck. Preserve their evidence
when deciding whether to resume them. Do not restart micro-optimizations merely
because the old issue exists.

## Working rules

- Implement, verify and independently review one slice at a time. The user's
  2026-10-31 instruction authorizes ordered progress without waiting for approval
  between slices. Keep separate issues and commits, and fix review findings before
  proceeding.
- Merge accepted slices into `feat/spark-sql-staging`, not `main`. Keep the
  staging branch separate from ordinary releases until the user requests adoption.
- Give each implementation issue its own PR targeting staging. If a newly
  confirmed gap falls outside existing issue scope, create and assign a bounded
  issue before implementing it. Link the issue family through native sub-issues;
  keep review and validation detail in comments. Do not fold unrelated fixes
  into a passing slice merely because they touch the same module.
- Review completed work with a clean-context Sol/high sub-agent and the latest
  `/home/hanbo/.agents/skills/bulletproof-review/SKILL.md`. Review correctness,
  tests, names, responsibilities, documentation, integration and simplicity.
- Keep the forty completed historical issues closed. Consolidate their accepted
  patches in S01 and fix demonstrated regressions with explicit evidence.
- A recorded mismatch is not automatically a distinct bug. Check the latest
  selected runtime and completed fixes before reopening historical findings.
  For example, #262 resolved older subquery failures still described in #141.
- Preserve the explicitly accepted projected-IN NULL policy. Distinguish values,
  result types, logical/physical nullability, failure causes and execution stages.
- Keep detailed review and evaluation records out of PR descriptions. Use concise
  PR summaries and separate evidence when publication is authorized.
- Keep original source revisions, corpora and reference captures available.
  Do not turn the historical observation count into a compatibility percentage.

## Progress log

### 2026-10-31

- Audited the live #113 issue family: 98 issues, including 40 completed and 58
  closed as not planned. The latter contain 23 parent trackers and 35 leaves.
- Created the ongoing goal and this plan after the user's authorization.
- Created a clean worktree from current main for S01. Existing documentation
  edits and feasibility experiments remain in their original worktrees.
- Reconstructed 97 selected runtime source files from verified frozen archives
  and patches. Eleven dependency forks came from their pinned crates.io archives.
  Two original overrides were byte-identical to official packages and now use
  those registry packages directly. The inventory records archive checksums,
  accepted file checksums,
  consolidation changes and license notices.
- Independent source review caught six accepted dependency files absent from the
  final changed-source manifest. Those files were restored from the earlier
  accepted patches and independently verified. Rejected prototypes and #293's
  unreviewed candidate remain excluded.
- Consolidated the eight extracted Sail crates behind the new public crate in
  its own workspace. Its 11 dependency overrides do not enter the root release
  graph. The CLI and Python packages still use their existing implementation.
- Added the retained Sail crates to the independent workspace so ordinary
  `cargo test --workspace` runs their tests with the selected dependency forks.
  Their development optimization matches the repository's dependency profile;
  debug assertions and overflow checks remain enabled.
- Clean-context Sol/high reviewers inspected source selection and the public
  boundary, retained parser/planning changes, and runtime/dependency changes.
  Fixes include missing source files and notices, the expression-error analyzer's
  name, planning-time volatile function evaluation, dead precision branches,
  stale comments, fixture import pairing and meaningful result assertions.
- Replayed all 25,180 distinct SQL/ANSI/batch combinations from 32,030 captures,
  including the last accepted PR291 cases. Comparisons cover values, types,
  names, nullability, error messages and failure stages. Row order is checked
  separately through the public API.
- The replay initially exposed six debug-only failures: simplification removed
  correlation predicates but left stale subquery metadata. The original captures
  used release builds. Refreshed that metadata without disabling validation;
  the full replay now passes and unsafe live grouping remains rejected.
- The complete workspace test run passed, including retained Sail units, real
  Delta deletion vectors/snapshot refresh/stream failures, public API contracts
  and the crate example. Two pre-existing ignored aggregate-macro documentation
  fragments remain ignored; neither is an executable query acceptance test.
- Added a dedicated CI matrix for the independent workspace on Linux, macOS and
  Windows, using the same ordinary Cargo commands. CI filter and result-gate
  tests cover the new job. Remote CI has not run yet.
- The final 11-fork dependency graph passed the complete workspace test run on
  Rust 1.96.0: 407 passed, zero failed, two upstream documentation fragments
  ignored. The one preservation test includes all 25,180 observations; that
  observation count is not a separate Spark-compatibility pass count.
- Independent follow-up review verified the source inventory, registry mappings,
  workspace/CI configuration and final fixes, with no remaining actionable S01
  finding.
- Rust 1.96 formatting and warning-denying Clippy checks passed. All 46 relevant
  CI/workflow regression tests passed, including the new path filter and required
  result gate. Locked Cargo metadata and Clippy resolved the final graph without
  updating the lockfile.
- Local slice review is complete. The user authorized committing and continuing
  in order without per-slice approval. Remote macOS/Windows CI, release
  size/performance comparisons and complete Spark compatibility remain unverified.
- Committed S01 as `2847b369` and opened PR #599. The user selected a separate
  staging branch for the complete frontend; #599 and subsequent slices target
  `feat/spark-sql-staging`.
- Added the reproducible source inventory retained from the frozen experiment,
  extended for its standalone test functions. It reports 103,985 Sail Rust lines:
  88,140 production, 15,765 test and 80 build-script lines, plus 269 generated
  parser lines. The 11 dependency forks retain another 403,139 Rust lines. Their
  34 modified Rust files contain 4,048 added and 386 removed lines relative to
  the checksum-verified published sources; the retained source is reported in
  full, not only the patch. The Linux normal/build graph has 380 packages and no
  PyO3 or Sail Python runtime packages. A clean-context Sol/high review reproduced
  the source and generated counts with no actionable finding; Cargo graph counts
  were executed by the parent, not independently by that reviewer.
- All three platform jobs passed the complete workspace tests on `a39cdcaa`.
  The subsequent inventory/docs-only commit `e9d942a2` exposed three replay
  differences on macOS. Frozen physical plans already admitted competing
  partition outcomes; the importer had preserved only one observation of each.
- Checked 26 related queries with 40 and 500 local repeats each, then executed
  the twelve ROUND queries' two partitions and eight UNION failing branches
  separately. Twenty exact observation IDs have evidenced alternative errors.
  The source archive and imported captures stay unchanged; the replay now checks
  these alternatives without accepting unrelated errors or altered success
  metadata. Independent Spark 4.2.0 repeats also confirmed the inherited UNION
  compatibility gap described below.
- Reopened #167, #173 and #116 with their existing native hierarchy, assigned to
  `mag1cfrog`. S02's ARRAY_CONTAINS prototype and independent reference captures
  are retained separately while the S01 CI repair is completed. S02 has not been
  accepted, committed or merged.
- A fresh clean-context Sol/high Bulletproof Review verified all twenty
  alternatives against the frozen plans and isolated execution evidence, checked
  the unchanged import and comparison boundaries, and found no correctness
  issue. Clarified that complete-query repeats observed only some alternative
  pairs. The full local replay, matcher rejection tests, formatting and the
  CI-equivalent Clippy check passed; the revised remote CI remains to run.
- Pushed the replay repair as `1263c894` to PR #599. While its final platform CI
  runs, S02 proceeds on `fix/spark-array-contains-null`; it will merge only after
  S01 has entered the staging branch.
- S02 reuses the pinned DataFusion Spark ARRAY_CONTAINS implementation, retaining
  native constant-array simplification and adapting NULL scalar/NullType results
  to complete batches. Independent Spark captures contain 166 observations:
  151 supported-input comparisons and 15 explicitly retained differences, each
  exercised with input batches of one and 32 rows. Volatile lookups are checked
  for one evaluation per row, and direct function calls cover zero/one/multiple
  rows. Local focused tests and the complete workspace test run passed before
  final review; S02 remains uncommitted and unaccepted.
- The local lookup benchmark uses 16,384 Int32 rows, eleven measured repeats
  after two warm-ups, and the same development profile on both revisions.
  Column lookups without NULL elements took 0.74-0.91 times the baseline execution
  time; nullable hits took 0.84-0.98 times, and nullable misses 1.06-1.11 times.
  Constant-array cases took 0.50-1.13 times. These are local observations, not
  release throughput guarantees. Nullable misses now return NULL, so their
  additional cost must be reported separately from equivalent-output comparisons.
- The CI-equivalent root-package Clippy command passed for S02. An additional
  `--all-targets` run selecting `sail-function` and `sail-plan` as primary lint
  targets reported inherited test annotations, unwraps and style findings. Every
  reported statement exists in the baseline; no diagnostic names the new adapter,
  integration test or benchmark. This broader lint run did not pass. Include
  vendored lint cleanup in S11 before claiming those packages are lint-clean.
- S01's final run, `41222603054` at `1263c894`, passed 408 tests on each platform.
  PR #599 merged into staging as `3f4245fc`; #165 is complete. Main and the release
  graph remain unchanged.
- The independent S02 review inspected every changed file, connected planner and
  execution paths, all 302 supported Spark comparisons, 30 inherited-difference
  checks, and 24 cost scenarios. It found no actionable issue. Clarified how to
  carry the benchmark harness onto the baseline. Final local validation passed
  412 tests, with only the two existing ignored documentation fragments; format
  and the CI-equivalent Clippy check passed. Remote CI is the remaining delivery
  check before this slice can merge into staging.
- Committed #116 as `8fb846ad` and opened PR #600 against staging. Its evidence
  comment distinguishes the passing comparisons from inherited differences and
  records the independent review and local cost measurements.
- Reopened #150 and reproduced 34 conditional queries in both ANSI modes against
  fresh Spark 4.2.0 references. NULLIF floating equality and first-argument types,
  dead operand evaluation, untyped CASE conditions and IFNULL temporal coercion
  need separate repairs. Completed typed-NULL CASE/scalar-subquery behavior still
  passes its value/type controls. Mixed Decimal and nested-admission work retain
  their existing owners.
- Created #601 under the existing NULL/conditional parent for floating NULLIF
  equality. The candidate reuses SparkComparisonFloat and native equality/validity
  kernels, preserving unmatched input bits and one evaluation per operand. Its
  98 Spark observations run at three batch sizes; direct calls cover sliced and
  empty arrays plus NaN payloads. Four exact replay outcomes now require the
  independently confirmed NULL result, while the source archive remains intact.
  The full local workspace passed 415 tests with the two existing ignored
  documentation fragments. Independent review found a concrete analyzer
  integration regression: a NULLIF-guarded multirow subquery executed instead of
  being discarded. Added recognition of the new function type and independent
  guarded/live INT/DOUBLE subquery controls. The full workspace and all 294
  focused reference comparisons pass after the repair. The clean-context review
  follow-up confirmed the fix and found no additional actionable issue; formatting
  and the CI-equivalent Clippy check passed. Remote CI remains pending.
- Local NULLIF cost probes use equivalent-output DOUBLE inputs and alternating
  baseline/candidate executables. Normalization adds measurable work to column
  comparisons. A nonzero, non-NaN scalar bypasses normalization; this restores
  column/nonzero timings near baseline. Nonnullable scalar/column execution still
  costs about 2.1-2.4 times the baseline in these development-profile runs, while
  the nullable case is near baseline. Instrumentation confirms the same scalar
  and array argument shapes reach the native fast path; the cause remains open.
  S11 must investigate this case in the release comparison before any general
  performance acceptance. The current slice reports this cost, not a performance
  pass or a justified cause.
- PR #600 passed CI on Linux, macOS and Windows and merged into staging as
  `c4e75faf`; #116 is complete. Opened PR #602 for #601 against staging with the
  independent review and measured costs in a separate evidence comment.
- Created #603 for IFNULL/NVL temporal coercion. Both currently bypass the
  existing COALESCE temporal rule. Fresh Spark references cover DATE, TIMESTAMP
  and TIMESTAMP_NTZ in UTC and America/Phoenix, both ANSI modes, selected/unused
  invalid fallbacks, column inputs, empty results and the aliases' exact arity.
  Reference collection converts local TIMESTAMP values to UTC before encoding
  epoch microseconds; TIMESTAMP_NTZ keeps wall time.
- The first #603 candidate delegated all inputs to COALESCE. Its 380-reference
  suite and 416 workspace tests passed, but independent review identified an
  uncovered numeric/string regression. Fresh Spark/baseline/candidate probes
  confirmed that non-ANSI aliases changed STRING '9' to INT 9 and errored on
  STRING 'bad'. The repair uses COALESCE only when an operand is temporal;
  non-temporal operands retain native NVL. All 60 mixed-input observations now
  match the baseline, including types, values, errors and plans. The final
  reference suite adds these non-ANSI mixed-string cases. Existing alias Decimal
  and ANSI mixed-string differences remain S03 work, not compatibility passes.
  Post-review validation passed 416 workspace tests, the 380-reference suite at
  two batch sizes, formatting and CI-equivalent Clippy. The clean-context
  follow-up found no outstanding actionable finding in this slice. Remote CI
  and the staging merge remain pending. Existing timestamp parser diagnostics
  lack Spark's CAST_INVALID_INPUT classification; keep that gap in S07.
- #604 validates searched CASE condition types before branch simplification.
  The shared conditional reference suite now contains 440 observations, including
  60 CASE/IF cases. Sixteen historical untyped-NULL CASE successes are independently
  confirmed Spark analysis failures; their exact replay outcomes are corrected,
  preserving the original source captures. BOOLEAN NULL, IF(NULL, ...), simple
  CASE and dead/live result controls remain. All 416 workspace tests passed,
  including the complete replay and 880 conditional reference comparisons.
  Formatting and CI-equivalent Clippy passed. The clean-context Sol/high review
  found no actionable finding; remote CI and staging integration remain pending.
- #602 passed Linux, macOS and Windows CI and merged into staging as `5b4b92f5`.
  Opened the separate #607 PR for #603. The reviewed CASE slice is committed as
  `da304f5e`; its PR follows the preceding staging merge.
- #606 adds the existing Spark legacy size setting to the public options and
  keeps the native length calculations and checked INT cast. Independent Spark
  references contain 288 observations across all four mode combinations. Untyped
  NULLs now reach the INT NULL result for SIZE, CARDINALITY and ARRAY_SIZE; the
  latter never applies the legacy -1 rule. The former retained SIZE difference
  in the array suite is now a strict reference comparison.
- A one-line COALESCE fallback passed the value references but evaluated a
  three-row volatile input five times. The size fix instead replaces NULL in
  the already computed length. Focused tests pass across seven Arrow input
  representations, slices, empty batches and the INT overflow boundary. The
  public COALESCE/IFNULL/NVL probe independently confirms the shared evaluation
  gap in both ANSI modes; #608 owns that repair. All 419 workspace tests pass,
  including the complete 25,180-observation historical replay and 576 new Spark
  comparisons. The same two upstream documentation fragments remain ignored.
  Formatting, diff checks, capture-script syntax checks and CI-equivalent Clippy
  pass. The clean-context Sol/high Bulletproof Review independently checked the
  input matrix, public API paths, native kernels, assertions and naming/docs,
  with no actionable finding. It did not rerun Cargo or Spark. Remote platform
  CI and staging integration remain pending; release cost and exact Spark schema
  nullability are separate S11/S07 work.
- #607 passed all three platforms in CI run `41233226325` and merged into staging
  as `4238a6c4`; #603 is closed. The next independent PR, #609, contains #604.
- #608 keeps COALESCE as a conditional physical expression. Each input is
  evaluated once on rows still needing a value, and guaranteed unused arguments
  remain pruned without changing the result type. The size fallback now uses
  this shared implementation. New counter assertions cover both SQL frontends,
  aliases, scalar/column inputs and empty results. Independent Spark captures
  add 72 fallback observations; all previous 440 observations remain identical.
  The 25,180-observation replay passes with six independently confirmed physical
  nullability corrections. The first workspace run exposed a dead subquery under
  ROUND with a NULL scale. The analyzer now retains successful constant proofs
  and changed fallback arguments. The repaired workspace passes all 420 tests,
  with the same two upstream doctests ignored. Clean-context Sol/high review
  found no remaining actionable issue after inspecting the analyzer repair and
  added nonnullable-column, native error and direct empty-batch controls. Formatting
  and CI-equivalent Clippy pass. Remote CI and staging integration remain pending.
- #608 is committed and pushed as `0425d50b`. Created #610 for NULLIF's unused
  right operand and captured 40 independent Spark observations. The baseline
  evaluates four right rows where only two are needed, and has twelve value/error
  differences across the two batch sizes. Reusing the existing conditional
  metadata and NULL-first runtime path resolves those focused differences. A
  direct empty-batch test also exposed operand evaluation before the empty check;
  the shared conditional path now returns an empty result first. Independent
  review identified a dictionary-value NULL gap in the shared mask. A counted
  dictionary input reproduced three right-side rows instead of one; selection
  now uses Arrow logical NULLs. Expanded references contain 50 observations,
  including subquery preparation/admission. The full workspace passes 421 tests
  with two inherited ignored doctests and no replay expectation changes.
  Formatting and CI-equivalent Clippy pass. The clean-context Sol/high reviewer
  rechecked the dictionary fix, subquery boundaries and shared empty-batch controls
  and found no remaining actionable issue. Remote CI and staging integration
  remain pending.

- #609 passed CI run `41236059809` on Linux, macOS and Windows and merged as
  `0f2b595d`. Issue #604 is closed. Collection-size PR #611 now targets staging
  with one issue's changes; its CI is running.
- #610 is committed and pushed as `f43bcf5b`. #612 owns the remaining projected
  NULL array-membership failures. A public-API probe on that baseline fails both
  examples in `simplify_expressions`, before rows execute. The candidate follows
  only known NULLs through projection/alias chains, reuses NULL simplification
  and native column pruning, and leaves other expressions in place. Independent
  Spark references add 90 observations; the original values/types/errors remain
  unchanged. The two old retained failures now have strict assertions. Expanded
  volatile counters cover unused and shared required inputs in both ANSI modes.
  Review identified missing Filter/window/grouping sites, required aggregate/sort
  error boundaries and a new empty-group regression. The candidate now shares
  the rewrite only at supported sites and reuses the existing foldable-group
  preparation. Grouping-set keys use native per-key name preservation. Inner-join
  pruning remains an unchanged, independently reproduced gap with its own issue
  #613; outer-join failure controls remain strict. All 421 workspace tests,
  formatting and Clippy pass. The clean-context Sol/high reviewer verified the
  repaired paths and final logs with no remaining actionable finding in this
  slice. Remote platform CI and staging integration remain pending.

- #612 is committed and pushed as `80d79acf`. #613 reuses native join predicate
  movement before the existing single-input NULL proof. The 104 new Spark
  observations cover six join types, both sides, compound/cross-input predicates,
  aliases, empty inputs, local VALUES and subquery preparation. The baseline
  differs in 30 of the initial 160 focused status/value comparisons across two
  batch sizes. The candidate passes the expanded 360-observation array matrix,
  including the two formerly retained inner-join failures. Required outer/anti,
  cross-input and selected-output errors remain strict. Left/right outer-join
  ID outputs retain exact rows and representable NULLs. Shared-live-value and
  join-cardinality counters pass in both ANSI modes. The final complete workspace
  run passes 421 tests with the same two inherited ignored doctests; the full
  historical replay needs no new corrections. Formatting and Clippy pass. The
  clean-context Sol/high reviewer checked the final references and logs with no
  remaining actionable finding. Remote CI and staging integration remain pending.
- #611 passed all three platform jobs and merged into staging as `929810ff`.
  Issue #606 is closed. The separate conditional-evaluation PR #614 now targets
  staging with one implementation commit; its detailed validation is a comment.
- #613 is committed and pushed as `7dcfda7a`. S03 starts with #615, the bounded
  scalar VALUES Decimal repair under the resumed #170 relational tracker. The
  #141 coverage review keeps its wider relational scope; its historical body
  remains in a comment. Fresh Spark 4.2.0 captures confirm the Decimal(38,38)/INT
  result must be Decimal(38,28), including fractional rounding. BIGINT and
  floating inputs expose related type differences. The first 130-observation
  reference corpus catches 164 value/type/admission differences across two batch
  sizes after excluding error-wording and test-decoder discrepancies. The final
  142-observation corpus also covers exact historical SQL and MAP rejection in
  both operand orders. Decimal typing now precedes MAP normalization so invalid
  original types remain visible. Five historical expectations have independent
  Spark replacements; the frozen corpus and prior corrections remain unchanged.
  The sole unordered historical query compares duplicate-preserving multisets.
  Final workspace validation passes 422 tests, zero failures and the same two
  inherited ignored doctests, including the complete historical replay.
  Formatting and Clippy pass. The clean-context Sol/high reviewer audited the
  final eight files, connected paths and validation with no remaining actionable
  finding. Remote platform CI and staging integration remain pending.
- #615 is committed and pushed as `04b191a6`. Its review exposed an older MAP
  preparation boundary: `VALUES (0, MAP('k',1)), (1)` panics while indexing the
  absent column, before the native builder can report malformed dimensions.
  A fresh public-API regression reproduces the panic on `04b191a6`. NaN FLOAT
  SQL controls already reject the row width correctly. Separate issue #616
  moves dimension validation before all column coercion and removes the
  Decimal-only deferred guard. Focused controls cover MAP/scalar row orders and
  valid map/NULL values. The full workspace passes 423 tests with zero failures
  and the same two inherited ignored doctests; all historical expectations are
  unchanged. Formatting and Clippy pass. The clean-context Sol/high review
  found no actionable issue after tracing SQL grammar, direct AST boundaries,
  error precedence, caller paths and assertion faults. Remote platform CI and
  staging integration remain pending.
- #616 is committed and pushed as `28d93682`. S03 continues with the already
  assigned mixed-temporal issue #605. The 700 added Spark observations preserve
  all 562 prior conditional records and cover five functions, both ANSI modes,
  DATE/LTZ/NTZ operand orders, selected/all-NULL/column/empty cases, New York DST
  transitions and a fixed offset. The pre-fix public test reports 920 differences
  across two batch sizes; the candidate passes all 1,262 references. Existing
  local-time conversion is shared with CAST and SEQUENCE, rather than copied.
  The first full workspace run passes without historical corrections. A further
  120 references exercise unused constant/column and required invalid temporal
  fallbacks; the final corpus has 1,382 observations. Temporal logical/physical
  type equality and the physical output name are now checked explicitly. The
  full workspace passes 423 tests, zero failures and the same two inherited
  ignored doctests, including the complete historical replay without further
  corrections. Formatting and CI-equivalent Clippy pass. The clean-context
  Sol/high reviewer independently checked all added reference rows, types and
  errors and found no remaining actionable finding. Remote CI and staging
  integration remain pending.
- #605 review traced an inherited `ConvertTz` limitation: conversions between
  different zones rely on Chrono and do not cover Spark's full i64-microsecond
  timestamp range. The new conditional path reuses this existing converter.
  The current corpus verifies ordinary dates and DST/fixed-offset boundaries,
  not the full range. S05 must reproduce and route that limitation before making
  a broader support claim; it is not silently accepted as complete parity.

- #605 is committed and pushed as `22151773`. #614 passed the three platform
  jobs and merged into staging as `16df0951`; issue #608 is closed. The separate
  NULLIF evaluation PR #617 now targets staging with one issue's changes.
- #618 owns NULLIF first-argument result types and values. A 228-observation
  Spark capture exposes 308 baseline result-preservation differences across two
  batch sizes, including
  loss of BIGINT precision and string leading zeros. Two observations independently
  confirm an existing Decimal/DOUBLE comparison-policy gap, now owned by #619.
  The result-preservation candidate keeps original values and performs native
  comparison casts on temporary values. New mixed-type volatile counters retain
  once-per-required-row evaluation. The initial full workspace passes 424 tests.
  Review found that direct columns/literals could bypass NULL row selection before
  the new internal casts. Eight new Spark observations reproduce that boundary;
  four more check comparability rejection. The expanded capture has 240 records.
  Initial baseline totals also included four #619 comparison mismatches and two
  error-wording assertion differences; neither is counted as a result-type repair.
  Both physical constructors now honor the declared NULL-first contract without
  inferring safety from a Column/Literal node. The complete final workspace passes
  424 tests with zero failures and the two inherited ignored doctests; the full
  replay needs no new corrections. Mixed-width NaN/zero-bit checks and an explicit
  native DataFusion type/validity control pass. The clean-context Sol/high review
  resolved its implicit-cast, naming, stale-documentation and test-validity findings.
  Final formatting, the explicit dependency-file format check and warning-denying
  Clippy pass. Remote CI and staging integration remain pending.
- The required NULL selection in #618 has a measured development-profile cost:
  five alternating pairs on 262,144-row DOUBLE inputs show nullable-left column
  cases at 3.20-3.82x (roughly 0.89-1.37 ms); nonnullable/scalar-left controls are
  near unchanged. Dedicated S11 issue #620 under the resumed performance tracker
  #168 owns release cost, checked-arithmetic controls and a safe optimization
  before adoption; analyzer-only #157 keeps its original scope. Restoring the
  unsafe node-kind gate is not an acceptable optimization. This is a staged
  correctness repair, not a performance pass.
- PR #617 CI run `41245115427` failed on macOS when the sccache client lost its
  server response while compiling `unicode-ident`; there was no Clippy source
  diagnostic. Linux/Windows were still running, so GitHub rejected an immediate
  job rerun. Retry the failed job after the run completes; do not merge on this
  attempt or change the source/workflow to disguise the infrastructure failure.

### Retained gaps found during S01 review

These are follow-up compatibility items, not consolidation regressions. Confirm
each with a focused reference comparison before reopening or creating its issue.

- BIGINT BROUND converts through floating point: the retained implementation can
  change `9007199254740993` to `9007199254740992` at scale zero.
- DOUBLE BROUND needs boundary cases for extreme scales, where its power-of-ten
  calculation can produce NaN. Keep these with the S07 arithmetic work.
- Eight ANSI group-only UNION/LIMIT probes can return success before another
  branch's division/cast failure, while both the frozen Spark reference and fresh
  Spark 4.2.0 runs fail. S07 must settle evaluation/error preservation and replace
  the replay's explicit scheduling alternatives with strict Spark expectations.

### Retained gaps confirmed during S02

- Mixed Decimal array/lookup precision coercion remains rejected. Carry the
  focused reference cases into S03 rather than widening #116's operand types.
- #612 addresses the two projected NULL/unused CAST cases. Its independent
  references preserve local VALUES preparation and required-input errors.
- Untyped NullType operands remain admitted although Spark rejects them. Track
  their admission with #156; batch NULL handling is covered by #116.
- Legacy collection-size behavior is being resolved in #606. The Spark result in
  the original #116 companion capture is unchanged; its assertion is now strict.
- IFNULL/NVL's native path promotes equal DECIMAL inputs to DOUBLE, retains
  STRING for ANSI integer/string pairs, and admits Boolean/string pairs that
  Spark rejects in non-ANSI mode. COALESCE's numeric/string union instead selects
  the numeric type regardless of mode. Fresh captures and the unchanged paths
  are retained for S03; #603 changes only temporal aliases.
- Mixed DATE/TIMESTAMP coercion needs a shared conditional repair in S03. Fresh
  Spark references cover 144 combinations of DATE/LTZ/NTZ, argument order, ANSI
  mode and UTC/Phoenix. The baseline aliases returned STRING; the narrowed #603
  candidate uses COALESCE's existing nanosecond/no-timezone DATE promotion, which
  still differs from Spark. All 96 COALESCE observations at two batch sizes are
  unchanged. These are diagnosed gaps, not passing compatibility observations.
  Its dedicated implementation owner is #605 under the native conditional parent.
