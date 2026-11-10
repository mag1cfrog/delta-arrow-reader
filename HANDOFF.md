# Issue-by-issue code handoff

The unfinished code is available as ordinary remote Git branches. Start with an issue row below; downloading the backup bundle is optional.

This publication preserved existing source and did not resume implementation, tests or merging. Staging observed during inventory: `714c949ade6d16a49e0d0a7a8f47fb9571e68f30`.

## Active issue starting points

| Issue | Remote branch | Scope and remaining work |
| --- | --- | --- |
| [#736](handoff/issues/736.md) | [fix/736-nested-constructor-declarations-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/736-nested-constructor-declarations-wip) | Historical optimizer/declaration candidate; current-staging validation pending. |
| [#748](handoff/issues/748.md) | [fix/748-nested-expression-names-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/748-nested-expression-names-wip) | Naming candidate; related replay corrections also preserved as validation branches. |
| [#757](handoff/issues/757.md) | [fix/757-struct-expression-names-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/757-struct-expression-names-wip) | STRUCT naming candidate; composed metadata integration preserved separately. |
| [#762](handoff/issues/762.md) | [fix/762-qualified-column-precedence-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/762-qualified-column-precedence-wip) | Qualified-column tier candidate; original native and field-access controls retained. |
| [#766](handoff/issues/766.md) | [fix/766-named-struct-fields-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/766-named-struct-fields-wip) | Partial source; the narrower #810 repair was merged, full #766 is not claimed complete. |
| [#767](handoff/issues/767.md) | [fix/767-using-key-metadata-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/767-using-key-metadata-wip) | USING identity/metadata candidate and recorded replay boundaries. |
| [#786](handoff/issues/786.md) | [fix/786-repeated-projection-identity-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/786-repeated-projection-identity-wip) | Projection identity candidate; complete acceptance remains pending. |
| [#790](handoff/issues/790.md) | [fix/790-scalar-subquery-names-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/790-scalar-subquery-names-wip) | Scalar-subquery display-name candidate; preserve full error/schema stages. |
| [#794](handoff/issues/794.md) | [fix/794-union-output-metadata-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/794-union-output-metadata-wip) | Shared UNION prototype with #832/#833, not an isolated merge-ready patch. |
| [#826](handoff/issues/826.md) | [fix/826-duplicate-struct-casts-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/826-duplicate-struct-casts-wip) | Position-based duplicate-field cast candidate and failure controls. |
| [#827](handoff/issues/827.md) | [fix/827-array-struct-declarations-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/827-array-struct-declarations-wip) | Constructor argument/declaration alignment candidate; shared historical ancestry retained. |
| [#832](handoff/issues/832.md) | [fix/832-union-left-metadata-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/832-union-left-metadata-wip) | Same shared prototype as #794/#833; issue-specific separation remains pending. |
| [#833](handoff/issues/833.md) | [fix/833-set-operation-cast-metadata-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/833-set-operation-cast-metadata-wip) | Shared metadata source/reference only; no claim the implicit-cast gap is fixed. |
| [#870](handoff/issues/870.md) | [fix/870-required-case-conditions-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/870-required-case-conditions-wip) | Includes the saved uncommitted CASE preparation attempt; unvalidated prototype. |
| [#893](handoff/issues/893.md) | [fix/893-udf-folding-opt-outs-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/893-udf-folding-opt-outs-wip) | Workspace2223/0/3, focused33/0, nativeCASE38/0,940 observations; unresolved eager-order finding and new vendor lint. No final acceptance. |
| [#1155](handoff/issues/1155.md) | [test/1155-literal-folding-baseline-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/1155-literal-folding-baseline-wip) | Existing literal-only route baseline source; not an implementation or successful final acceptance. |

The #794/#832/#833 branches share the existing UNION prototype. Their changes still need issue-specific isolation. An issue without an implementation is not represented by an empty branch: #711 was only the next planned task. Broader #1007 and the remaining parent-plan work are not claimed complete.

## Stash and uncommitted code

These normal branches contain the saved files directly. The original stashes and working directories remain unchanged.

| Issue | Remote branch | Origin |
| --- | --- | --- |
| #828 (CLOSED) | [fix/828-recursive-coercion-stash-latest-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/828-recursive-coercion-stash-latest-wip) | materialized_stash; `72c65717cbccdee2f95456e0a81f118b2429cd86` |
| #828 (CLOSED) | [fix/828-recursive-coercion-stash-before-rebase-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/828-recursive-coercion-stash-before-rebase-wip) | materialized_stash; `6567126aa0467896b3e0cbc3d7fe417e944b2f87` |
| #726 (CLOSED) | [test/726-floating-window-coverage-stash-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/726-floating-window-coverage-stash-wip) | materialized_stash; `51a2ca0bf8d87fd5160d10cb4adcb28c9dc65959` |
| #171 (OPEN) | [test/171-aggregate-window-coverage-stash-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/171-aggregate-window-coverage-stash-wip) | materialized_stash; `52e68dc9a88445e7298cedb9abf766231816cb09` |
| #528 (CLOSED) | [docs/528-schema-error-header-stash-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/docs/528-schema-error-header-stash-wip) | materialized_stash; `e4d37f86e5eebc485af702f702f3ff13b43cf8c3` |
| #525 (OPEN) | [docs/525-cli-entry-examples-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/docs/525-cli-entry-examples-wip) | dirty_worktree_snapshot; `refs/archive/stopped-2026-11-09/worktree-00` |
| #113 (OPEN) | [experiment/113-sail-feasibility-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/experiment/113-sail-feasibility-wip) | dirty_worktree_snapshot; `refs/archive/stopped-2026-11-09/worktree-01` |
| #721 (CLOSED) | [fix/721-nested-case-uncommitted-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/721-nested-case-uncommitted-wip) | dirty_worktree_snapshot; `refs/archive/stopped-2026-11-09/worktree-04` |

Stash recovery includes the third-parent untracked files, including the nested result-coercion module and frozen Spark fixtures. Historical code for a closed issue is reference material; use its accepted staging repair instead of redoing it.

## Validation and original implementation references

The inventory below preserves additional code and replay sources. Names such as `final`, `ready`, `validated` or `red` are historical labels, not a current test result. The private #893 native CASE branch changes a validation workspace and must not be merged as product configuration.

<details>
<summary>Published reference branches</summary>

| Related issues | Branch | Role |
| --- | --- | --- |
| #165 (CLOSED) | [feat/spark-sql-crate](https://github.com/mag1cfrog/delta-arrow-reader/tree/feat/spark-sql-crate) | original_implementation_reference |
| #834 (CLOSED), #719 (CLOSED), #841 (CLOSED) | [fix/spark-abs-result-fields](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-abs-result-fields) | original_implementation_reference |
| #710 (CLOSED) | [fix/spark-alias-contract-followup](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-alias-contract-followup) | original_implementation_reference |
| #835 (CLOSED), #836 (CLOSED), #837 (CLOSED), #840 (CLOSED), #843 (CLOSED) | [fix/spark-array-result-fields](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-array-result-fields) | original_implementation_reference |
| #827 (OPEN) | [fix/spark-array-struct-declarations](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-array-struct-declarations) | original_implementation_reference |
| #721 (CLOSED) | [fix/spark-case-alias-metadata](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-case-alias-metadata) | original_implementation_reference |
| #721 (CLOSED) | [fix/spark-case-alias-test-boundary](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-case-alias-test-boundary) | original_implementation_reference |
| #721 (CLOSED), #869 (CLOSED) | [fix/spark-case-mixed-declarations](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-case-mixed-declarations) | original_implementation_reference |
| #721 (CLOSED), #870 (OPEN) | [fix/spark-case-nested-result-fields](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-case-nested-result-fields) | original_implementation_reference |
| #721 (CLOSED) | [fix/spark-case-nullability](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-case-nullability) | original_implementation_reference |
| #818 (CLOSED) | [fix/spark-correlated-metadata-pre815](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-correlated-metadata-pre815) | original_implementation_reference |
| #818 (CLOSED) | [fix/spark-correlated-scalar-metadata](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-correlated-scalar-metadata) | original_implementation_reference |
| #826 (OPEN) | [fix/spark-duplicate-struct-casts](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-duplicate-struct-casts) | original_implementation_reference |
| #816 (CLOSED) | [fix/spark-folded-map-lambda-nullability](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-folded-map-lambda-nullability) | original_implementation_reference |
| #710 (CLOSED) | [fix/spark-generated-alias-before-div-integration](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-generated-alias-before-div-integration) | original_implementation_reference |
| #710 (CLOSED) | [fix/spark-generated-alias-integration](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-generated-alias-integration) | original_implementation_reference |
| #710 (CLOSED) | [fix/spark-generated-alias-metadata](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-generated-alias-metadata) | original_implementation_reference |
| #749 (CLOSED), #782 (CLOSED) | [fix/spark-interval-qualifiers](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-interval-qualifiers) | original_implementation_reference |
| #824 (CLOSED) | [fix/spark-map-constructor-nullability](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-map-constructor-nullability) | original_implementation_reference |
| #766 (OPEN), #810 (CLOSED) | [fix/spark-named-struct-fields](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-named-struct-fields) | original_implementation_reference |
| #828 (CLOSED) | [fix/spark-nested-conditional-coercion](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-nested-conditional-coercion) | original_implementation_reference |
| #828 (CLOSED) | [fix/spark-nested-conditional-results](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-nested-conditional-results) | original_implementation_reference |
| #821 (CLOSED), #829 (OPEN) | [fix/spark-nested-lambda-captures](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-nested-lambda-captures) | original_implementation_reference |
| #744 (CLOSED) | [fix/spark-pivot-source-alias](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-pivot-source-alias) | original_implementation_reference |
| #762 (OPEN) | [fix/spark-qualified-column-precedence](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-qualified-column-precedence) | original_implementation_reference |
| #786 (OPEN) | [fix/spark-repeated-projection-identity](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-repeated-projection-identity) | original_implementation_reference |
| #790 (OPEN) | [fix/spark-scalar-subquery-names](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-scalar-subquery-names) | original_implementation_reference |
| #822 (CLOSED) | [fix/spark-size-result-nullability](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-size-result-nullability) | original_implementation_reference |
| #757 (OPEN) | [fix/spark-struct-expression-names](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-struct-expression-names) | original_implementation_reference |
| #757 (OPEN), #710 (CLOSED) | [fix/spark-struct-metadata-integration](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-struct-metadata-integration) | original_implementation_reference |
| #757 (OPEN), #710 (CLOSED) | [fix/spark-struct-metadata-integration-pre803](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-struct-metadata-integration-pre803) | original_implementation_reference |
| #893 (OPEN) | [fix/spark-udf-folding-opt-outs](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-udf-folding-opt-outs) | original_implementation_reference |
| #794 (OPEN), #832 (OPEN), #833 (OPEN) | [fix/spark-union-generated-metadata](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-union-generated-metadata) | original_implementation_reference |
| #767 (OPEN) | [fix/spark-using-key-metadata](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-using-key-metadata) | original_implementation_reference |
| #781 (CLOSED) | [fix/spark-year-month-scaling](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/spark-year-month-scaling) | original_implementation_reference |
| #893 (OPEN) | [test/893-case-source-target-observations](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/893-case-source-target-observations) | all_16_stage_observation_source |
| #893 (OPEN) | [test/893-handoff-observations](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/893-handoff-observations) | frozen_940_observation_source |
| #893 (OPEN) | [test/893-native-case-factory](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/893-native-case-factory) | private_native_CASE_factory |
| #721 (CLOSED) | [test/spark-case-nested-fields-pre-staging-rebase](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-case-nested-fields-pre-staging-rebase) | historical_validation_branch |
| #721 (CLOSED), #828 (CLOSED), #836 (CLOSED), #840 (CLOSED), #843 (CLOSED) | [test/spark-conditional-integration](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-conditional-integration) | historical_validation_branch |
| #756 (CLOSED) | [test/spark-date-day-final](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-date-day-final) | historical_validation_branch |
| #756 (CLOSED) | [test/spark-date-day-repaired](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-date-day-repaired) | historical_validation_branch |
| #747 (CLOSED) | [test/spark-date-range-consumer-ready](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-date-range-consumer-ready) | historical_validation_branch |
| #747 (CLOSED) | [test/spark-date-range-validation](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-date-range-validation) | historical_validation_branch |
| #745 (CLOSED) | [test/spark-equivalent-window-validation](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-equivalent-window-validation) | historical_validation_branch |
| #730 (CLOSED) | [test/spark-extrema-final-validation](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-extrema-final-validation) | historical_validation_branch |
| #730 (CLOSED) | [test/spark-extrema-integrated-final](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-extrema-integrated-final) | historical_validation_branch |
| #730 (CLOSED) | [test/spark-extrema-merge-check](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-extrema-merge-check) | historical_validation_branch |
| #710 (CLOSED) | [test/spark-generated-alias-baseline-repaired](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-generated-alias-baseline-repaired) | historical_validation_branch |
| #731 (CLOSED), #861 (OPEN) | [test/spark-native-constant-validation](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-native-constant-validation) | historical_validation_branch |
| #828 (CLOSED) | [test/spark-nested-coalesce-control-red](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-nested-coalesce-control-red) | historical_validation_branch |
| #828 (CLOSED) | [test/spark-nested-conditional-observer-red](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-nested-conditional-observer-red) | historical_validation_branch |
| #828 (CLOSED) | [test/spark-nested-conditional-pre-staging-rebase](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-nested-conditional-pre-staging-rebase) | historical_validation_branch |
| #736 (OPEN), #748 (OPEN) | [test/spark-nested-label-candidate](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-nested-label-candidate) | historical_validation_branch |
| #748 (OPEN) | [test/spark-nested-label-final](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-nested-label-final) | historical_validation_branch |
| #748 (OPEN) | [test/spark-nested-label-validated](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-nested-label-validated) | historical_validation_branch |
| #744 (CLOSED) | [test/spark-pivot-alias-final](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-pivot-alias-final) | historical_validation_branch |
| #762 (OPEN) | [test/spark-qualified-precedence-candidate](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-qualified-precedence-candidate) | historical_validation_branch |
| #828 (CLOSED) | [test/spark-recursive-admission-integration](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-recursive-admission-integration) | historical_validation_branch |
| #752 (CLOSED) | [test/spark-scalar-float-final](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-scalar-float-final) | historical_validation_branch |
| #752 (CLOSED) | [test/spark-scalar-float-validation](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-scalar-float-validation) | historical_validation_branch |
| #763 (CLOSED) | [test/spark-single-row-candidate](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-single-row-candidate) | historical_validation_branch |
| #736 (OPEN) | [test/spark-statistics-schema-validation](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-statistics-schema-validation) | historical_validation_branch |
| #174 (CLOSED), #788 (CLOSED), #1022 (CLOSED) | [test/spark-string-array-c06-observation](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-string-array-c06-observation) | historical_validation_branch |
| #727 (CLOSED) | [test/spark-window-completion-validation](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-window-completion-validation) | historical_validation_branch |
| #745 (CLOSED) | [test/spark-window-cse-validation](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/spark-window-cse-validation) | historical_validation_branch |

</details>

## Evidence and exact revisions

- [Machine-readable branch inventory](handoff/branches.json) records every published HEAD and its issue mapping.
- [Publication verification](handoff/VERIFIED.json) records remote ref equality and unchanged local working state.
- [Original archive instructions](README.md) describe the complete source and evidence backup.
- Source and historical receipts remain available in the unpublished archive draft. The backup predates the new handoff aliases; those aliases resolve directly on GitHub.

No issue was closed or marked accepted by this publication.
