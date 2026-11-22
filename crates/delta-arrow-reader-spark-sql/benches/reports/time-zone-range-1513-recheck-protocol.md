# Rebased time zone range comparison (#1513)

Repeat the [original protocol](time-zone-range-1513-protocol.md) from
the start against staging `d39ce212`. The candidate carries the exact
product changes from `cee97162`. The 48 archived partial samples
belong to the earlier session.

Use the frozen release builder, statements, preflight, four blocks
and 36 processes. Start the session after the rebased workspace
tests and exact preflight pass. Record all new retained samples. Bind
both release build receipts to their checked-out source commits and
confirm that the repaired conversion routines are present in the
candidate binary.

The original paired comparison flags a cell when its median paired
increase exceeds the maximum staging self-pair difference. For every
flagged cell, trace its executed routine and compare normalized
instructions and relevant state layout. Address relocation alone does
not change the instructions. Check the contents of any differing
referenced constants before treating the routines as equivalent.

A flagged cell passes if the executed instructions and relevant state
layout match. If any flagged cell has changed instructions or layout,
the correction stays unmerged and the report names the change in work.
A missing instruction or layout comparison, missing timing block,
process failure, or result or type mismatch leaves acceptance incomplete.
If all cells pass, merge the candidate and its references into staging.
Keep the report and the complete compressed sample archive together.

```sh
python3 benches/time_zone_range_1513_recheck/run.py STAGING_BUILD CANDIDATE_BUILD OUTPUT
```
