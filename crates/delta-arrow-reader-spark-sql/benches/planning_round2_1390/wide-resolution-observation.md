# Wide logical resolution cost (#1390)

The retained build in the [logical phase comparison](../reports/planning-1390-round2-logical.md)
still takes 1.368 / 1.367 ms more than native-configured for the
400-column logical phase, ANSI off / on. Observe that statement
on the [current staging source](wide-resolution-observation.json)
before choosing one change.

Split registered-table resolution into scan setup, field
registration, the scan's rename projection and timezone annotation.
Measure SELECT projection work after its input has resolved and
separately record the final SELECT schema construction within it.
Observe these scopes inside the full resolution interval. The
intervals exclude each other's work except for the explicitly
nested SELECT schema span.

Use the [same assembly and timeout commands](logical-phase-observation.md)
with `wide_resolution_observer.py`, this source pin and separate
evidence destinations. Use five fresh processes per ANSI mode,
two warmups and nine retained samples. Accept all 90 samples only
if the spans are complete and their nesting is valid. Check the
frozen fixture and the absence of Delta reads through the existing
planning driver. Planning does not execute a query.
