# Wide physical projection properties on macOS (#1390)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).
This reuses the #1571 phase and stack samples for staging `cd8351ee`.

**Apple Silicon, native macOS.** The initial physical projection
constructor added 0.998 / 1.003 ms over native configured with ANSI
off / on. Its combined A/A floors were 0.176 / 0.211 ms.

| Call under initial projection construction | Stack samples, ANSI off | Stack samples, ANSI on |
| --- | ---: | ---: |
| `ProjectionExec::try_new` | 8 | 13 |
| `ProjectionExec::try_from_projector` | 7 | 11 |
| `EquivalenceProperties::project` | 5 | 8 |
| `EquivalenceGroup::project` | 3 | 3 |

These are inclusive stack counts under
`create_project_physical_exec_with_props`. They locate work in the
projection constructor, separate from the earlier alias-to-column
conversion.

The wide memory scan supplies no ordering, equivalence classes, or
constraints. Its inputs are projected in order, each exactly once.
The constructor still builds a projection mapping, normalizes every
source expression, and builds classes that it discards as trivial.
For that input shape, the projected properties are empty and
partitioning is preserved. The next trial targets this repeated
property construction in the existing physical-plan fork.

[Phase report](planning-1390-macos-attribution.md) |
[Original stack samples](planning-1390-macos-wide-stacks.jsonl.gz) |
[Projection constructor](../../vendor/dependencies/datafusion-physical-plan/src/projection.rs#L148) |
[Equivalence projection](../../vendor/dependencies/datafusion-physical-expr/src/equivalence/class.rs#L710).
