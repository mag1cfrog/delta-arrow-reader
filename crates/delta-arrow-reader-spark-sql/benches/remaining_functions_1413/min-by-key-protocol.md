# MIN_BY key filter comparison (#1413)

Use the frozen MIN_BY statement and native FIRST_VALUE control on one
million rows. In each ANSI mode, the same ordered reducer must be present,
with only the synthesized true filter absent in the candidate. Check this
in both untimed physical plans. Match each full result and its executed
type to native before timing.

Use the [shared matched schedule](protocol.md) and the
[pinned comparison configuration](min-by-key-comparison.json). Retain the
change only if both ANSI modes are faster in at least two of their three
process pairs and their paired median reductions exceed the candidate
native A/A floor. The target is the candidate native median plus its floor
in both modes. No retries or replacement samples.
