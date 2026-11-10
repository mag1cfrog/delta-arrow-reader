# Recommended issue continuations

Use only the implementation table below when assigning saved implementation work. It has 11 selected branches, one for each listed issue. All other published branches are classified in [HISTORY.md](HISTORY.md).

The selected branches contain unfinished work on historical bases. They are not merge approvals or fresh validation on current staging. No source branch was moved, rewritten or deleted to make this list.

## Implementation branches

| Issue | Select this branch | State | First work |
| --- | --- | --- | --- |
| [#748: Nested expression names](handoff/issues/748.md) | [fix/748-nested-expression-names-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/748-nested-expression-names-wip) | implementation WIP | Continue the field-path naming implementation and its ordinary projection/window controls on current staging. |
| [#757: STRUCT child and expression names](handoff/issues/757.md) | [fix/757-struct-expression-names-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/757-struct-expression-names-wip) | implementation WIP | Continue the STRUCT naming implementation. Compose it with the current generated-alias metadata rules; the older composition branch is reference material. |
| [#762: Qualified-column precedence](handoff/issues/762.md) | [fix/762-qualified-column-precedence-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/762-qualified-column-precedence-wip) | implementation WIP | Continue the qualifier-tier implementation and retain its extraction/wildcard controls. |
| [#767: Hidden USING key metadata](handoff/issues/767.md) | [fix/767-using-key-metadata-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/767-using-key-metadata-wip) | implementation WIP | Continue the hidden USING identity/metadata implementation and its replay boundaries. |
| [#786: Repeated projection identity](handoff/issues/786.md) | [fix/786-repeated-projection-identity-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/786-repeated-projection-identity-wip) | implementation WIP | Continue the projection identity implementation; keep separate UNION metadata acceptance under its own issues. |
| [#790: Scalar-subquery expression names](handoff/issues/790.md) | [fix/790-scalar-subquery-names-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/790-scalar-subquery-names-wip) | implementation WIP | Continue scalar-subquery naming after lowering and rebasing; retain hidden-sort and error-stage controls. |
| [#826: Duplicate STRUCT cast positions](handoff/issues/826.md) | [fix/826-duplicate-struct-casts-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/826-duplicate-struct-casts-wip) | implementation WIP | Continue position-preserving duplicate-field widening and its failure controls. |
| [#827: ARRAY/STRUCT declaration alignment](handoff/issues/827.md) | [fix/827-array-struct-declarations-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/827-array-struct-declarations-wip) | implementation WIP | Continue constructor argument/declaration alignment and reconcile shared coercion prerequisites with current staging. |
| [#832: UNION left metadata](handoff/issues/832.md) | [fix/832-union-left-metadata-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/832-union-left-metadata-wip) | shared prototype, not isolated | This is the shared UNION prototype. First isolate the left-metadata change for #832. #794/#833 have no independent implementation in the saved work. |
| [#870: Required CASE/IF condition errors](handoff/issues/870.md) | [fix/870-required-case-conditions-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/870-required-case-conditions-wip) | unvalidated implementation attempt | This is an unvalidated saved CASE preparation attempt. First reproduce the original CASE and IF timing cases and assess the parked change. |
| [#893: UDF folding opt-outs](handoff/issues/893.md) | [fix/893-udf-folding-opt-outs-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/893-udf-folding-opt-outs-wip) | reviewed WIP, findings unresolved | Resolve the eager-argument ordering finding and the new strict vendor lint, then integrate and validate against current staging. |

## Shared source without an independent implementation

| Issue | Source reference | Disposition |
| --- | --- | --- |
| [#794](handoff/issues/794.md) | [fix/832-union-left-metadata-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/832-union-left-metadata-wip) | Shared reference under #832. No independent #794 implementation is selected. |
| [#833](handoff/issues/833.md) | [fix/832-union-left-metadata-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/fix/832-union-left-metadata-wip) | Shared reference under #832. No independent #833 implementation is selected. |

The previous `fix/794-...-wip` and `fix/833-...-wip` aliases point to the same prototype as #832. They are historical aliases, not additional workstreams. Assign the shared source once before separating the issue-specific changes.

## Reproducer only

| Issue | Branch | State |
| --- | --- | --- |
| [#1155](handoff/issues/1155.md) | [test/1155-literal-folding-baseline-wip](https://github.com/mag1cfrog/delta-arrow-reader/tree/test/1155-literal-folding-baseline-wip) | Baseline/reproducer source only. No implementation is included. |

## No dedicated implementation to resume

| Issue | Saved-work assessment |
| --- | --- |
| [#736](handoff/issues/736.md) | The previous WIP alias is an old aggregate-statistics experiment. It is not the constructor repair. |
| [#766](handoff/issues/766.md) | The previous WIP alias preserves the narrower accepted #810/#812 source. It does not establish a new candidate for the remaining scope. |
| [#711](handoff/issues/711.md) | Planned next, no implementation written. |
| [#1007](handoff/issues/1007.md) | No independent branch covers the full scope. #893 contains only the related NoFold work. |

Start newly assigned work for these issues from current staging. Do not select a historical branch solely because its name contains the issue number.

## Reference material

- [HISTORY.md](HISTORY.md): every non-selected branch, labeled as historical implementation, historical validation, validation plumbing, recovery snapshot, or withdrawn alias.
- [Machine-readable routing](handoff/routing.json): the same issue choices, including explicit nulls for absent independent implementations.
- [Exact branch inventory](handoff/branches.json): all preserved refs and their SHA values.
- [Verification](handoff/ROUTING-VERIFIED.json): selection and remote-ref checks.
- [Archive recovery](README.md): complete source and evidence backups.

The original issue comments and individual notes are updated to use this selection. Historical branches are retained for recovery. This organization does not resume implementation or merging.
