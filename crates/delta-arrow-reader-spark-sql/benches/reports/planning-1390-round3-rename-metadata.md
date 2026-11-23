# Wide scan rename schema alignment (#1390)

Keep the metadata alignment change. Both Spark modes improved in all
five pairs and every native and Spark control passed the frozen gate.

| Configuration | Alignment before, ms | Alignment after, ms | Physical before, ms | Physical after, ms |
| --- | ---: | ---: | ---: | ---: |
| Native plain | n/a | n/a | 2.147 | 2.162 |
| Native configured | 0.000030 | 0.000041 | 2.314 | 2.311 |
| Spark ANSI off | 0.954 | 0.048 | 10.471 | 9.633 |
| Spark ANSI on | 0.951 | 0.048 | 10.509 | 9.596 |

Values are medians of five process medians. The 0.002485 ms native
A/B floor is below the median paired wide physical gain in each
Spark mode. The physical gap to native-configured remains
7.322 / 7.285 ms, ANSI off / on.

The aligned root builds qualified output fields from their already
bound scan positions. Both ordinary metadata walk and schema-recompute
spans appeared twice per staging Spark sample and disappeared in the
candidate. The remaining alignment span takes 47.929 / 48.430 us for
checking the scan shape and rebuilding and validating its schema. The
normal path covers filters, functional dependencies, alias metadata
and other shapes. Default planner expression lowering and optimizer
work remain visible in the observed component table.

## Validation

All 678 canonical traces per arm and eight wide observer traces
matched before timing. This compares exact Arrow schemas, logical and
physical plans, partitioning, ordering, inputs and Delta scan counters.
Each retained observer plan recorded the correct 22 physical rules
and schema checks. Measurement executed no SQL and read no Delta data.
Formatting, Spark-crate Clippy and the full Linux workspace passed
3,298 tests with three ignored. The focused test compares schema and
field metadata, nullability and mixed output qualifiers with the
standard projection builder, including the fallback cases.

The [protocol](planning-1390-round3-rename-metadata-protocol.md) was
committed at `3bf3009cbb7d12f68a39073abff78b3d77bf22ea` before timing.
The candidate release binaries both use product `7790b47d`.

- [All 360 canonical comparisons](../planning_round3_1390/rename-metadata-summary.csv) and [observed planner components](../planning_round3_1390/rename-metadata-planner-components.csv).
- [50 canonical processes and 13,500 timing pairs](../planning_round3_1390/rename-metadata-paired-samples.jsonl.gz).
- [40 wide observer processes and 360 retained plans](../planning_round3_1390/rename-metadata-stage-samples.jsonl.gz).
- [Build receipts, trace digests and process medians](../planning_round3_1390/rename-metadata-proof.json.gz).
