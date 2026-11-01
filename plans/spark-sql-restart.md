# Spark SQL restart: work plan and progress

This is a temporary engineering plan for resuming the frozen Spark SQL work. It
is not a supported-feature reference and is not part of the documentation site.
Keep implementation evidence and unfinished work here until the relevant slice
has been reviewed. Publish the final support contract separately.

## Current slice

### S04 aggregate FILTER behavior

#142 is reopened on the current staging baseline. #696 owns COUNT_IF aggregate
FILTER results and evaluation/admission boundaries. #697 separately owns FILTER
on aggregate window expressions. Both are native children of #171 and blockers
of #142. #656 remains a separate floating PIVOT repair.

Independent Spark captures reproduce the ignored COUNT_IF FILTER and show why
COUNT(CASE WHEN predicate THEN 0 END) is insufficient: Spark extracts repeated
complex predicates before applying FILTER. Reuse native expressions and common
subexpression elimination, while retaining constant-NULL counts and admission
checks. All 164 independent Spark observations pass at batches 1 and 32, with
328 exact result/schema/error comparisons. Full Linux workspace checks pass
501 tests with zero failures and two inherited ignored doctests; root Clippy,
formatting and diff checks pass. Independent Sol/high review identified a
constant-pruning admission bug, reproduced and repaired before integration.
Native expression searches now reject illegal nested aggregates/windows before
pruning, while valid scalar aggregate subqueries and windows over aggregates
remain covered.

### Completed synchronization and platform checkpoint

The #690 -> #689 -> #687 -> #141 chain is complete. PR #695 merged at
`e420aa36556dac1bb0867b2ca16fdc46fb249927`; checkpoint `41344985125` passed on that
exact commit. Each macOS, Linux and Windows job passed 500 tests with zero
failures and two inherited ignored doctests, plus formatting and root Clippy.
Their logs include the historical, relational, UNION and enclosing-context suites.

GitHub scope and acceptance are synchronized across #141/#113/#167/#173/#174.
#632 now belongs to Dates/timestamps. The parents of #656/#661/#666 are open,
and #170 records the remaining relational work. S01 is complete; Reader/CLI
integration and production adoption remain pending. Reopen #189-#192 at S08
and #166 at S11. The remaining S04-S11 mapping and provisional 30-59 engineering
hour estimate are recorded in #167 before implementation resumed.

### Relational coverage acceptance

#690 and #689 closed after PR #693 merged at `a56f2509`. #687 closed after PR #694
merged at `47fe3946`; its complete tree matches reviewed source `824f8ce0`.
The UNION repair passed all 664 new comparisons, the existing result/raw-bit/
metadata/native-SQL controls, 499 Linux workspace tests, root Clippy and a fresh
clean-context Sol/high Bulletproof Review. The final focused replay also passed.

PR #695 restores #141's 128 original
Spark records. All 256 batch-size comparisons pass on the repaired staging
runtime. Four projected IN shapes retain their previously approved SQL NULL
policy; their raw Spark results are unchanged. The matcher now verifies their
exact ID-specific policy rows and rejects overrides on unrelated cases.

Fresh probes recheck #140's nested qualified scalar queries and live correlated
ROUND/BROUND cardinality cases. These remain a separate S09 execution gap, not
supported relational behavior. The eight retained UNION/LIMIT early-error cases
remain S07/#149 work. Full Linux validation passes 500 tests, zero failures and two inherited ignored
doctests, including the complete 25,180-observation historical replay. Root
all-target Clippy, formatting and diff checks pass. Clean-context Sol/high
Bulletproof Review found no actionable issue after correcting an obsolete
progress paragraph. The coverage PR and three-platform checkpoint passed, and
#141 closed with every scoped acceptance item checked.

INTERSECT/EXCEPT stay private temporary nodes until query resolution finishes.
A memoized column-lineage callback shares native NULL-rejection rules without
expanding projection expressions. Only analysis copies reduce outer joins;
executable joins and required expressions remain intact. Native simplification
handles constant values with query-time functions masked, and native projection
pruning prevents folding unused growing payloads. The TABLE-argument boundary
lowers its query before an arbitrary registered table function can hide it.

The three reference corpora contain 4,096, 592 and 220 independent Spark records.
The original 184 parent-context records are unchanged; 36 additional records
cover the diagnosed LIMIT shapes and identity/filter controls in both floating
widths and ANSI modes. Across configured batches and partitions the replay has
11,440 comparisons: 11,344 require exact Spark results, and 96 exercise the
approved zero-sign boundary. The latter represent 24 reference records across
six explicit shapes. Their original Spark results remain recorded; either
opposite-sign zero pair is accepted, with one row, names/types and NULL behavior
still enforced. Separate guards reject extra/missing rows, NULLs, equal signs,
nonzero values and applying the exception to another query shape.

No product query-shape workaround was added for the retained differences.
Earlier red results, full Spark plans and native-rule investigation are retained
under `spark-set-bounded-analysis-2026-11-01` outside the repository. The final
Linux workspace run passes 497 tests, with zero failures and two inherited
ignored doctests. Root-crate all-targets Clippy passes with warnings denied.
The fresh clean-context Sol/high Bulletproof Review completed every applicable
criterion and rechecked the final source manifest without actionable findings.
Both issues were closed after staging integration.

GitHub synchronization and the three-platform checkpoint completed before S04
resumed; the results are recorded above.

#691 merged through PR #692 at `69ee9323`, whose entire tree and eight reviewed
file hashes match source `7dc60068`. Shared physical join properties now drop
invalid constant claims only on NULL-extended sides, preserving column equality,
known NULLs and offset metadata. Independent review's CrossJoinExec caller fix
has a red/green regression. Final Linux checks passed 483 tests, zero failures
and two inherited ignored doctests, including 4,600 SQL comparisons; root Clippy,
formatting and clean-context Sol/high Bulletproof Review passed. The original
128 captures also passed 256 zero-sign-independent bag comparisons on that final
runtime, with 64 exact sign differences left visible. The issue is closed and
its implementation branches removed. Cross-platform and large-input performance
checks remain for later checkpoints.

#683 merged through PR #688 at `40f03f4e`, whose complete tree matches reviewed
source `fc8cb9d2`. The floating comparison keys pass 660 floating-key, 512
coercion and 366 duplicate-count Spark references at batches 1 and 32. Final
Linux checks passed 480 tests, zero failures and two inherited ignored doctests,
plus root Clippy, formatting and clean-context Sol/high Bulletproof Review.
The issue is closed and its implementation branches removed. General post-set
zero rendering remains with #687; macOS/Windows await the next checkpoint.

#682 merged through PR #686 at `5a1b5ff9`, whose complete tree and seven reviewed
file hashes match source `c441fd54`. The duplicate-row numbering repair passes
all 366 independent Spark references at batches 1 and 32, plus 512 coercion
references and metadata/native controls. Final Linux checks passed 476 tests,
zero failures and two inherited ignored doctests; root Clippy, formatting and
diff checks passed. Clean-context Sol/high Bulletproof Review completed without
unresolved findings. Its issue is closed and owned implementation branches are
removed. No automatic PR workflow ran; macOS/Windows remain for the integration
checkpoint.

#684 merged through PR #685 at `e50edfae`, whose complete tree and all ten
reviewed file hashes match source `ab00eda4`. All 512 independent Spark
references, including 74 required errors, pass at batches 1 and 32. Separate
checks preserve output names/metadata and native DataFusion SQL; BY NAME
reordering and missing-column NULLs match two independent DataFrame observations.
Final Linux checks pass 475 tests with zero failures and two inherited ignored
doctests; root Clippy, formatting and diff checks pass. Clean-context Sol/high
Bulletproof Review completed without unresolved findings. The issue is closed,
and its local/remote implementation branches are removed. No automatic cloud
PR workflow ran. Exact Spark error-stage/nullability parity, #682/#683 and
performance checks remain separate work.

#678 merged through PR #681 at `06dc3ad4`, whose full tree matches reviewed
source `d46117dc`. All 124 Spark references, including 45 required errors,
pass at batches 1 and 32. Final Linux checks pass 471 tests with zero failures
and two inherited ignored doctests; root Clippy and formatting pass.
Clean-context Sol/high Bulletproof Review resolved the native SQL boundary and
indistinguishable local/scalar error tokens. No review finding remains. The issue
is closed, and its temporary branch and restoration stash are removed.

#679 merged through PR #680 at `6d2c93cc`, whose entire tree matches reviewed
source `311e7a08`. ABS validates its single argument before every affected trait
callback reads it. Four focused tests and all 469 Linux workspace tests pass,
with zero failures and two inherited ignored doctests; root Clippy and formatting
pass. The connected replay fixes four arity panics without changing its other
180 observations. Clean-context Sol/high Bulletproof Review resolved a test
representation assumption and a stale progress claim, with no remaining finding.
The issue is closed and the temporary branch is removed.

At the initial #141 coverage snapshot (`06dc3ad4`), the 128 references produced
36 mismatches across nine query shapes. The saved files were restored onto
`47fe3946` after the independent count/coercion, floating equality, outer-join
and post-set representation repairs merged. Their replay now passes all 256
comparisons, preserving the original Spark captures and the approved IN NULL
policy. Current validation and checkpoint status is recorded above.

The original 164 historical observations have fresh Spark 4.2 and pre-#678 native
captures. #678 repairs their two literal-false filter observations, with exact
replacement expectations verified by the full replay. The fresh 24-case correlated probe confirms #140 remains an S09 execution gap:
Spark returns rows for four nested-qualified cases and cardinality errors for
twenty live ROUND/BROUND cases; DAR rejects physical planning in all 24.
UNION/LIMIT early-error differences remain S07/#149 work. Final coverage review
and the three-platform checkpoint passed before #141 closed.

#150 merged through PR #677 at `d32ae7d6`, whose entire tree matches reviewed
source `4ba2e4b3`. All 70 independent Spark observations, including six required
ANSI cast errors, pass at batch sizes 1 and 32. Full local Linux checks pass
465 tests with zero failures and two inherited ignored doctests; root Clippy
and formatting pass. Clean-context Sol/high Bulletproof Review completed with
no unresolved findings. The issue is closed and its temporary branch and owned
stash are removed. The slice adds coverage and fixture documentation only.

The original 34 conditional queries were replayed in both modes and batch sizes.
Only the known MAP NULLIF admission difference remains in status/values after
typed display normalization. ARRAY NULLIF returns the expected NULL array. The
old temporary collector required logical-null handling for NullArray; that was
an evidence-collection defect. MAP admission stays queued under #156/S10, and
exact schema/error contracts stay in #149/S07. The new schema controls record
Spark analyzed/optimized and native resolved/physical fields separately. Fixture
documentation now counts 356 strict original array cases, four remaining
untyped-NULL admission cases, and the separate 324-case Decimal corpus.

#671 merged through PR #676 at `c726c9c9`, whose complete tree matches reviewed
source `761db6f2`. Dynamic list operands run only on unresolved, non-NULL lookup
rows. All 28 independent Spark observations, including six required ANSI errors,
pass at batch sizes 1 and 32. Native checks cover exact visits, logical NULLs,
scalar inputs, empty batches, slices and invalid comparison bits. Final focused
checks pass eight native tests plus the Spark replay; combined Linux workspace
checks pass 464 tests with zero failures and two inherited ignored doctests.
All 55 retained native InList units, root Clippy and formatting pass. The extra
scalar-zero test case postdates the full run and passes the final focused suite
and Clippy. Clean-context Sol/high review has no unresolved findings. The issue
is closed; its temporary branch and restoration stash are removed. Filtering
cost remains an S11 check, and cross-platform validation remains for a checkpoint.

#674 merged through PR #675 at `280689b7`, whose complete tree matches reviewed
source `fb209761`. Native dynamic NullArray list items now return unknown and can
be resolved by a later match, without bypassing type validation or required
operand errors. Final Linux workspace checks pass 459 tests with zero failures
and two inherited ignored doctests; 55 retained native InList units, root Clippy
and formatting pass. Clean-context Sol/high Bulletproof Review found no unresolved
issue. The issue is closed and the temporary branch removed. Broader nested
Spark admission remains #156/S10; no cross-platform/performance claim is made.

#672 merged through PR #673 at `ca5dd5b8`, whose entire tree matches reviewed
source `be743b39`. A recursive volatility precheck prevents planning calls and
cached volatile membership values. Literal and deterministic constant filters
retain their existing path. Linux workspace checks pass 457 tests with zero
failures and two inherited ignored doctests, plus 55 retained InList unit tests.
Final focused tests, root Clippy and formatting pass. Clean-context Sol/high
Bulletproof Review completed after two test-only fixes, with no unresolved
findings. The issue is closed and temporary branches removed. No automatic
cloud PR checks ran, consistent with the staging policy; this is not a new
cross-platform checkpoint or performance claim.

#669 merged through PR #670 at `686f8126`, whose full tree matches reviewed
source `1818f397`. The shared Boolean evaluation path uses existing row
selection/scatter and Kleene kernels instead of the old density-only AND helper.
All 48 independent Spark cases pass at both batch sizes; native checks cover
operand visits, NULLs, scalar/array inputs, slices and empty batches. The final
Linux workspace passes 455 tests with zero failures and two inherited ignored
doctests; all 80 retained BinaryExpr unit tests, root Clippy and formatting checks
pass. Clean-context Sol/high Bulletproof Review found no unresolved actionable
issue. The issue is closed and its temporary branch is removed. Mixed Boolean
filtering cost remains a required dense/wide/nested comparison in S11. This is
correctness evidence, not a performance or cross-platform acceptance.

#659 merged through PR #668 at `716dd884`, whose full tree matches reviewed
source `73f426a4`. All 324 independent Spark observations and the original eight
Decimal results pass unchanged at both batch sizes. Final local checks pass
453 workspace tests, zero failures and two inherited ignored doctests, plus
Clippy and formatting. Clean-context Sol/high Bulletproof Review completed after
rechecking both prerequisite repairs. Its issue is closed, temporary branch and
three owned backups removed. Untyped NULL/nested admission remains with #156.

#665 merged through PR #667 at `a5321cd3`, whose full tree matches reviewed
source `5df768c1`. The shared cast boundary restricts ordinary row spans and
unencoded scalar view ranges before conversion. Connected repairs preserve
field metadata/nullability and zero-width row counts, and return invalid target
errors instead of panicking. Nested sharing, encoded storage and contiguous
buffer reuse are protected; inherited gapped nested/encoded bounds stay in #666.
Final local checks pass 451 workspace tests, zero failures and two inherited
ignored doctests, plus 41 retained Arrow unit checks, Clippy and formatting.
Clean-context Sol/high Bulletproof Review completed with no unresolved findings.
The required #659 strict replay passes. Its issue is closed and temporary branch
removed. macOS/Windows remain for the next checkpoint.

#663 merged through PR #664 at `780f9700`, whose full tree matches reviewed
source `7d9fa725`. Final Linux checks passed 441 tests, Clippy and formatting,
with zero failures and two inherited ignored doctests. Clean-context Sol/high
Bulletproof Review completed after fixing the new helper name and full-backing
float normalization. The required #659 strict rerun also passes. Its issue is
closed and temporary implementation branch removed.

#660 is complete through PR #662 at `13cd8e0b`, whose full merge tree matches
reviewed source `b3f44d99`. Local Linux validation passed 438 workspace tests,
zero failures and two inherited ignored doctests, plus 17 optimizer tests,
Clippy and formatting. Clean-context Sol/high Bulletproof Review completed
without unresolved findings. Its issue is closed and temporary branch removed.
The separate legacy Decimal-to-small-integer overflow gap stays with #661 in S07.

#650 and #651 are complete through PRs #658 and #657. Their complete merge trees
match the reviewed sources, and their issues are closed. Final local checks pass
437 tests, zero failures and two inherited ignored doctests, plus root-crate
Clippy, formatting and clean-context Sol/high Bulletproof Review. Their temporary
branches and owned backups are cleaned up.

The three-platform checkpoint on `30a284f2a89fcb9222129098b0c1f8b9075ea098` is
[run 41276527898](https://github.com/mag1cfrog/delta-arrow-reader/actions/runs/41276527898).
Linux, macOS and Windows each passed 437 tests, zero failures and two inherited
ignored doctests, plus formatting and root-crate Clippy. That checkpoint covers
its recorded baseline, not the later #660/#659/#663/#665 changes. #656's separate signed-zero
PIVOT matching gap stays in S04.

The ten reviewed slices #618, #619, #624, #621, #622, #623, #627, #631, #629 and
#633 are merged through separate PRs #637 and #640-#648. Their issues are closed.
Each actual merge tree matches its locally verified integration tree. After the
#637 integration rerun passed 424 tests, later merges reused their completed
Linux checks only after proving the entire independent Spark workspace matched
the reviewed source commit exactly. The final source passed 434 tests, zero
failures and two inherited ignored doctests, plus root-crate Clippy and formatting.
Clean-context Sol/high Bulletproof Reviews found no unresolved actionable issues
within each slice's scope.

PR #639, merged as `8a600047`, disables automatic cloud CI only for staging PRs.
The combined three-platform checkpoint passed at
[run 41268882510](https://github.com/mag1cfrog/delta-arrow-reader/actions/runs/41268882510)
on `3f952d1914b7487ae1b44c3907d94491c5f352d3`. Its Linux, macOS and Windows jobs
each passed 434 tests, zero failures and two inherited ignored doctests, plus
formatting and root-crate Clippy. Existing performance, vendor-lint and
remaining compatibility work stays in the ordered queue below.

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
| S02 | NULL and array semantics | #116, #150, #606, #608, #610, #612, #613, #669 | ARRAY_CONTAINS truth table; NULLIF equality; conditional admission/evaluation; collection-size modes |
| S03 | Decimal and conditional input coercion | #141, #150, #605, #615, #616, #618, #619, #624, #621, #622, #623, #627, #629, #633, #635, #650, #659, #660, #663, #665 | VALUES Decimal/integer values and types; NULLIF result/comparison types; mixed CASE Decimal execution; retained non-temporal IFNULL/NVL/COALESCE coercion; mixed-temporal promotion, nanosecond precision, STRING temporal comparisons and local-NULL preparation |
| S04 | Conditional aggregation and window contracts | #142, #656, #696, #697 | COUNT_IF aggregate FILTER, aggregate window FILTER, floating PIVOT signed-zero equality, then aggregation/window coverage |
| S05 | Dates, timestamps and timezones | #152, #153, #154, #632 | DATE_DIFF width, tolerant parsing, TIMESTAMPDIFF units/elapsed time, date-string boundaries, timestamp-literal separators and DST boundaries |
| S06 | JSON extraction and hashing | New bounded children of #167 | Requested get_json_object and sha2 behavior, nulls, invalid inputs and dynamic arguments |
| S07 | Arithmetic schema and diagnostic contracts | #149, #293, #661 | Classify remaining schema/error differences; separately fix multi-output CAST error order, legacy Decimal-to-integer overflow and the retained BROUND gaps below |
| S08 | Reader and CLI integration | #189, #190, #191, #192 | Select the same dependency patches at the consuming workspace root; verify Delta snapshots/rows, stream lifecycle, read-only admission, cancellation and resource limits |
| S09 | Names, projections and subqueries | #137, #138, #139, #140 | Identifiers, SELECT * EXCEPT, case-sensitive resolution and retained nested correlated queries |
| S10 | Remaining string and nested-value coverage | #151, #155, #156, #666 | Declared string/binary behavior, ELEMENT_AT modes, maps, arrays and explode contracts |
| S11 | Release comparison and support statement | #157, #164, #166, #620, #626 | Equivalent-output size/planning/execution comparisons under the same release settings; NULLIF, Boolean and IN-list row-selection cost; user-facing support documentation |

The other eleven historical performance leaves remain deferred unless these
comparisons identify a relevant regression or bottleneck. Preserve their evidence
when deciding whether to resume them. Do not restart micro-optimizations merely
because the old issue exists.

PR #636's Linux Spark job missed the target cache and had no Rust sccache hits.
Matching target caches existed only on previous PR merge refs, which sibling
PRs cannot restore. The user then selected local Linux validation for individual
staging slices. PR #639 removes that repeated cloud-build loop and retains
explicit remote checkpoints. No cache warmer was changed and no cache speedup
is claimed.

## Working rules

- Implement, verify and independently review one slice at a time. The user's
  2026-10-31 instruction authorizes ordered progress without waiting for approval
  between slices. Keep separate issues and commits, and fix review findings before
  proceeding.
- Merge accepted slices into `feat/spark-sql-staging`, not `main`. Keep the
  staging branch separate from ordinary releases until the user requests adoption.
- Each staging PR requires the full local Linux Spark workspace tests, root-crate
  Clippy, formatting and clean-context review. Automatic cloud CI excludes this
  base branch. Run the existing three-platform Spark checks at integration
  checkpoints with `gh workflow run ci.yml --ref feat/spark-sql-staging`, including
  before frontend adoption. PRs into main or other bases retain normal CI.
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
  job rerun. Linux and Windows then passed; the macOS job was rerun on the same
  head after the run completed. Staging merge waits for that result. Source and
  workflow files are unchanged for the rerun. The retry passed and PR #617 merged
  as `1e75c8bd`; issue #610 is closed. PR #625 now contains only #612.


- #619 separates the existing numeric comparison promotion rule from its binary
  expression caller and reuses it inside NULLIF. The initial 492-reference capture
  exposes 44 baseline differences across two batch sizes; targeted candidate and
  counted-evaluation tests pass. The expanded capture has 624 observations,
  including both narrow-integer operand orders and 96 NaN/infinity cases. The
  original 240 reference outcomes remain unchanged; two former Decimal comparison
  exceptions become strict assertions. The full workspace passes 424 tests with
  the 528-observation fixture, zero failures and two inherited ignored doctests.
  The final 624-observation fixture and counted-evaluation tests pass a focused
  rerun, with production code unchanged. The nonfinite cases trigger 96 unexpected
  Decimal cast errors in 192 baseline executions. Warning-denying Clippy,
  formatting and diff checks pass. Independent review found stale documentation
  claims, now corrected and rechecked. The clean-context Sol/high review has no
  unresolved actionable findings. The slice is ready to commit; its separate PR,
  remote CI and staging merge follow the existing queue.
- A separate 288-observation Spark comparison audit uses literals and VALUES,
  both ANSI modes, and UTC/Phoenix. It confirms shared string coercion (#621),
  mixed temporal comparison timezones (#622), and known-NULL-right simplification
  (#623). All are assigned to `mag1cfrog` as native children of #173. They remain
  required S03 work and are not counted as supported by #619. Dynamic NULL right
  columns can still require a conversion that a literal NULL skips; #623 must
  preserve that distinction.


- Preparing #621 exposed a separate string-to-BOOLEAN CAST gap: native Arrow
  accepts tokens such as `on` and `tr`, trims Unicode whitespace instead of
  Spark's ASCII controls/DEL, and throws for invalid legacy CAST values. Issue
  #624 owns this conversion repair before #621 reuses it. Both are native
  children of #173 and assigned to `mag1cfrog`.
- #624 captures 344 independent Spark cases. Across two batch sizes the baseline
  has 70 success/error-kind mismatches, 136 value mismatches and 14 error-class
  mismatches. The candidate's public-API and counted-evaluation tests pass.
  Native DataFusion and non-string conversions retain separate controls. The final
  workspace passes 427 tests, with zero failures and two inherited ignored
  doctests, including the layout/slice/empty units and unchanged historical replay.
  Root warning-denying Clippy, formatting and diff checks pass. The clean-context
  Sol/high Bulletproof Review reports no actionable findings. The slice is ready
  to commit; its PR and staging CI follow the existing queue.
- The additional workspace-wide Clippy diagnostic run for #624 stops on inherited
  vendor lint denials: 22 errors and 20 warnings at 38 unique locations. No
  reported line was added or changed by #624. This wider check is not green.
  Dedicated issue #626, assigned under #113, owns the S11 cleanup and complete
  all-target warning-denying check before main adoption.

- #621 captures 532 independent Spark string-comparison observations. Replaying
  them on #624 at batch sizes 1 and 32 exposes 426 success/error differences and
  32 value differences; these counts exclude error-message and schema differences.
  The candidate shares comparison type selection and existing Spark conversion
  functions. Explicit integer CAST and implicit comparisons now reuse the same
  strict Arrow parser or legacy truncation, including ASCII control trimming.
  The final reference expands to 684 cases with reversed boundary comparisons
  and WHERE/JOIN consumers. All 683 in-scope cases pass at both batch sizes; one
  ANSI known-NULL-right case remains assigned to #623. The first candidate's
  explicit integer UDF changed guarded arithmetic and nullable-field error
  preservation. Historical replay caught these regressions. Keeping native
  Cast/TryCast nodes while sharing conversion policy restores all historical
  outcomes without changing expectations. The workspace passes 428 tests with
  the 532-reference fixture, zero failures and two inherited ignored doctests;
  the final 684-case fixture and evaluation counters pass a focused rerun.
  Clean-context Sol/high Bulletproof Review findings are fixed and rechecked,
  with no actionable findings remaining. Root warning-denying Clippy, formatting
  and diff checks pass. The slice is ready to commit; its separate PR and staging
  CI follow the existing queue.

- #622 captures 1,350 independent temporal comparison observations initially;
  the baseline has 500 status/value differences across two batch sizes. The
  expanded 1,414-case capture adds registered-table rows whose LTZ data is UTC
  while the session uses UTC, Phoenix, New York or +05:45. Ordinary comparisons,
  NULLIF, BETWEEN and ON/USING joins reuse the existing session-zone conversion.
  The common temporal type and expression/runtime adapters are grouped together;
  the existing conditional, CAST and SEQUENCE expression trees remain unchanged.
  The focused public-API tests pass. Clean review found that the new microsecond
  promotion would lose precision for external nanosecond columns, making 1ns=2ns
  true. A public-API regression reproduces it. The comparison selector now leaves
  any nanosecond input on its original DataFusion path; unit/zone combinations
  check equality, original NULLIF types and values. Dedicated #627, assigned as a
  native child of #173, owns mixed nanosecond precision and session-zone coercion.
  Re-review caught a NULLIF runtime conversion that still bypassed this fallback.
  A second public regression reproduces the lost DST-gap error. The runtime now
  requires the Spark temporal policy to match; separate binary/NULLIF executions
  preserve nanosecond gap/overlap errors in both orders and ANSI modes.
  Full i64-microsecond timestamp range remains S05 work because the reused
  named-zone kernel depends on Chrono. The final workspace run passes 431 tests,
  with no failures and the same two inherited ignored macro doctests. Root
  warning-denying Clippy, formatting and diff checks pass. Clean-context Sol/high
  Bulletproof Review rechecked both repairs and the full scope with no remaining
  actionable findings. The slice is ready to commit; its dedicated staging PR
  follows the existing queue.

- #623 adds 96 independent known-NULL-right references without changing the
  previous 624 NULLIF observations. Spark distinguishes constants, dynamic NULL
  columns and early local VALUES conversion. The initial generic simplification
  erased local errors, so local prechecks preserve NULLIF comparisons while the
  existing projected-NULL machinery simplifies ordinary plans. NULLIF still
  validates comparability and returns
  its original left expression once. Three pre-existing non-ANSI STRING/temporal
  cast failures have dedicated #629, assigned as a native child of #173. Their
  Spark expected values stay intact and the current errors remain explicit.
  Clean-context Sol/high review found a native conversion-UDF lowering regression
  in the first precheck repair. A public host-session test reproduced it. The
  repair restores the full native ExprSimplifier and marks only temporary local
  NULLIF copies to preserve their comparisons; those copies never enter the
  returned plan. Four native conversion functions, the comparison corpus and
  evaluation counters pass the recheck. Independent review has no remaining
  actionable findings. The final workspace run passes 432 tests with no failures
  and the same two inherited ignored doctests; the 25,180 historical observations
  require no new corrections. Root warning-denying Clippy, formatting and diff
  checks pass. The slice is ready to commit and enter the dedicated staging PR
  queue.

- #627 replaces the retained mixed-nanosecond fallback with exact comparison
  values. Binary operators and NULLIF share the conversion; tagged instants and
  timezone-free values scale in Decimal128 without i64-nanosecond overflow.
  Local inputs reuse the existing timezone kernel after Euclidean splitting,
  preserving fractions across pre-epoch DST boundaries. Unsupported converter
  ranges produce an explicit error. A Python integer/zoneinfo corpus contains
  58 cases, with its microsecond timezone rules independently checked by 18 fresh
  Spark 4.2.0 conversions. Public tests cover both orders and ANSI modes,
  dates and all timestamp units, original NULLIF results, sliced/NULL/empty
  inputs, eight comparisons, BETWEEN and ON/USING joins. Conditional counters
  and a native DataFusion control preserve execution boundaries. The full
  workspace passes 433 tests, zero failures and two inherited ignored doctests;
  the frozen historical replay corpus remains unchanged. The old nanosecond
  rounding and DST-error assertions are replaced by independent expectations.
  Focused checks also cover explicit range
  errors and known-NULL-right pruning. Root warning-denying Clippy passes after
  replacing a test-helper panic with a reported fixture error. Clean-context
  Sol/high Bulletproof Review inspected every changed file and connected
  consumer. The lint and documentation findings are fixed and re-reviewed;
  no actionable finding remains. The slice is ready to commit and enter the
  dedicated staging PR queue. Performance and predicate pushdown were not
  measured; full-range named-zone conversion remains S05 work.

- #629's independent STRING/DATE/LTZ/NTZ capture has 786 observations. Reusing
  the existing converters repairs the ANSI-mode gap but exposed a prerequisite:
  SparkDate rejects an uppercase-T date suffix that the old Arrow comparison
  accepted. Its separate parser repair is #631. The #629 prototype is saved
  while that dependency is completed; its remaining local-preparation and error
  assertions are still unresolved, so the comparison slice is not accepted.

- #631 reuses the date-component grammar while restricting ignored suffixes to
  complete dates followed by space or uppercase T. The public parse entry trims
  surrounding ASCII controls and spaces. Timestamp parsing supplies an empty
  day suffix, preserving its previous grammar. Fresh Spark references contain
  516 observations: 514 match strictly at both batch sizes, while two expose the
  pre-existing lowercase timestamp separator gap now assigned to #632 for S05.
  The full workspace passes 434 tests, zero failures and two inherited ignored
  doctests, with the frozen historical corpus unchanged. Independent review
  caught an overly broad error assertion: malformed CAST input must retain its
  CAST_INVALID_INPUT class, while typed-literal errors use their own existing
  mapping. The repaired assertions, root warning-denying Clippy and formatting
  checks pass. Clean-context Sol/high Bulletproof Review completed the whole
  scope and rechecked the assertion repair with no remaining findings. The slice
  is ready to commit and enter the dedicated staging PR queue.

- #629 reuses SparkDate and SparkTimestamp for implicit STRING comparisons,
  sharing ANSI mode and session-timezone selection between binary operators and
  NULLIF. NULLIF keeps the original left value/type, and counted expressions
  preserve one evaluation per required operand. The reference capture now has
  956 Spark observations: 953 match strictly, while three local-NULL preparation
  cases remain explicit under #633. Original reference SQL and expected values
  are unchanged. Clean-context Sol/high Bulletproof Review found a missing
  constant-NULLIF error before WHERE false; follow-up probes also caught nested
  LIMIT 0 ordering. Both use the existing whole-expression error check and native
  empty-relation propagation after local preparation. Numeric/BOOLEAN, typed
  DATE literals, registered CASE and nested empty-input controls verify those
  boundaries. The generic DataFusion simplifier is unchanged. The final workspace
  passes 434 tests, zero failures and two inherited ignored doctests, including
  the full historical replay. Independent review rechecked every changed file,
  connected consumers and the repairs, with no actionable findings remaining.
  Remote platform CI and staging integration remain pending. Exact error-stage,
  error-class and nullability parity, other Arrow timestamp units and release
  performance remain separate work.

- #633 captures 452 independent Spark comparisons, including BOOLEAN, explicit
  casts, operand order, registered/literal controls and volatile siblings. All
  use strict result/error assertions at batch sizes 1 and 32; the original 956
  STRING/temporal expectations are unchanged and now have no retained cases.
  Local preparation reuses the full native simplifier with left-before-NULL
  conversion ordering. A query-local marker keeps native SQL on the same host
  unchanged. Independent Sol/high Bulletproof Review found and closed three
  gaps: native-query activation, BOOLEAN-specific NULL folding and a volatile
  sibling hiding deterministic conversion errors. Counted functions stay unused
  during planning; native SQL still works after a failed Spark query on the same
  host. Final workspace validation passes 434 tests, zero failures and two
  inherited ignored doctests. Root-crate all-targets Clippy, formatting and diff
  checks pass. No actionable review findings remain. Platform CI and the
  separate staging PR remain pending; exact Spark error-stage parity and CLI
  integration are outside this slice.

- #635 reuses one numeric result-type rule across CASE, IF, COALESCE, IFNULL,
  NVL and Decimal VALUES columns. It preserves integral digits at precision 38
  and selects DOUBLE for Decimal/floating inputs and ANSI integer/FLOAT results.
  Existing casts and conditional evaluation perform the conversions. The new
  capture contains 1,230 independent observations, including both selected
  IF/CASE branches. Before the repair, 1,600 assertions differ across batch sizes
  1 and 32; the first focused candidate passes the complete conditional corpus,
  mixed-type counters and VALUES Decimal references. A native SQL control now
  checks Decimal NVL and FLOAT/integer COALESCE on the configured host. Full
  workspace validation and independent review are running; neither is claimed
  complete yet. STRING/non-numeric coercion remains separate S03 work.

- #635's first full run reached all 25,180 historical observations and found two
  changed outcomes for one Decimal division query. Its corrected CASE result
  removes an old function-signature mismatch. Fresh Spark 4.2.0 execution confirms
  the candidate's ANSI bad-cast error and legacy NULL DECIMAL(25,15) result.
  The exact SQL is added to the reproducible numeric capture, now 1,232 records,
  and two explicit replay corrections replace those obsolete failures. The raw
  frozen corpus and the replay matcher are unchanged. Full validation is rerunning.

- #635's final full run passes 434 tests, zero failures and two inherited ignored
  doctests, including the 25,180 historical observations and native SQL controls.
  Root-crate warning-denying Clippy, formatting and diff checks pass. Independent
  Sol/high Bulletproof Review inspected all twelve changed files and connected
  consumers, checked the expanded capture and both replay corrections against
  fresh Spark evidence, and rechecked the repaired stale status text. No
  actionable finding remains. The separate platform checkpoint covers staging
  before this slice; this candidate's acceptance uses the local Linux policy.

- #652 changes only CSV record finalization: an empty encoded record becomes NULL.
  The 100 Spark 4.2 references cover both ANSI modes and run at batch sizes 1 and
  32. Baseline comparison found 104 result differences; the candidate has none.
  Four existing unit tests now assert NULL validity instead of reading NULL-slot
  bytes. Separators, quoted empty strings, replacement values and option errors
  retain their reference results. Spark reference capture uses NO_CODEGEN because
  its generated column path throws on a NULL CSV result; scalar and interpreted
  results agree. Full Linux validation passes 435 tests, zero failures and the
  same two inherited ignored doctests, plus root-crate Clippy and formatting.

- #654 uses 48 fresh Spark references for nested NULL markers, SQL NULL validity,
  literal NULL/null text, CSV controls and pivot names. The same supported PIVOT
  syntax is used by both engines. DAR currently rejects Spark's bare `FOR v`
  pivot syntax and PIVOT directly after inline VALUES; retain these parser cases
  for S09 relational-query coverage rather than changing the parser in this slice.
  The later float-name check also found that aliases before PIVOT are rejected;
  its verified reference omits the unnecessary subquery alias.
  Final candidate validation passes 436 tests, zero failures and the two inherited
  ignored doctests, including the unchanged historical replay. Root-crate Clippy
  and formatting pass. The 52 baseline result differences are resolved; the
  unchanged CSV and PIVOT controls pass.

- #651's connected PIVOT check exposed #656: matching negative zero counts only
  one sign while Spark combines both. The original independent reference and
  failed public result are preserved. #656 is queued in S04; formatting does not
  modify pivot predicates. Name controls use a separate four-value input with
  one zero sign, keeping their expected counts independently captured.

- #651 shares FLOAT/DOUBLE rendering through the installed ryu formatter, retaining
  Java 21 notation, signed zero and the closest decimal for subnormal values.
  The unchanged formatter source matches 30,024 independent Java bit patterns.
  All 446 original Spark input rows pass across CAST/TRY_CAST, CSV, XML and nested
  display in both ANSI modes and at batch sizes 1 and 32. Four focused pivot
  references verify generated names, types and counts; original mixed-zero
  counting remains assigned to #656. Volatile inputs execute once per required
  row, native DataFusion casts retain their results, and the historical replay
  needs no correction. The final Linux workspace passes 437 tests, zero failures
  and two inherited ignored doctests, plus root-crate Clippy.

- #650's saved conditional STRING candidate is restored on `e6f16d90` after
  #651 merged through PR #657. It reuses existing STRING-to-integer, floating
  and Boolean kernels, and SparkToUtf8 for temporal/floating STRING results.
  The 962 independent Spark observations have 811 successes and 151 required
  errors across both ANSI modes, 18 registered input definitions, scalar/local
  queries, both branch selections and variadic permutations. All are strict;
  the thirty former text differences disappear without changing expectations.
  Existing numeric/temporal references, lazy evaluation counters and native SQL
  controls remain passing. The complete Linux workspace passes 437 tests, zero
  failures and two inherited ignored doctests; root-crate Clippy and formatting
  pass. Independent review completed with no unresolved actionable finding.

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
