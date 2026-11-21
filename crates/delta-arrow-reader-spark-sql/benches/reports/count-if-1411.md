# Plain Boolean COUNT_IF filter results (#1411)

COUNT_IF over a plain Boolean column with no modifiers can use
a native filter on the count. The existing checks and lowering
remain for other shapes.

All measured shapes and ANSI modes meet the native noise target.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| COUNT_IF | Native | 1.317 [1.315, 1.345] | 1.403 [1.391, 1.412] | 0.009 |
| COUNT_IF | ANSI off | 8.843 [8.806, 8.949] | 1.372 [1.353, 1.406] | 0.009 |
| COUNT_IF | ANSI on | 8.834 [8.812, 8.911] | 1.380 [1.360, 1.399] | 0.009 |

The aggregate result row and its executed Arrow type matched across
both builds, both ANSI modes and native.
The boundary differential compares grouped results, names and types,
true, false and NULL arguments, DISTINCT and explicit FILTER, the
zero for empty input, and the exact outcome of a derived failing
Boolean cast in both ANSI modes.
All 680 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../generators_text_conditionals_1411/count-if-protocol.md) was
committed at `be049c70146cf11b7ee33e4556a5b99fa5442448` before clocks.
The Linux release session ran 2026-11-21T08:47:12Z to 2026-11-21T08:47:40Z.
The 18 comparison processes and 6 native controls retained 72 pairs.
Local evidence is under `costs-168-2026-11-20/1411/count-if`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `7e84d3dfaa866627985fa5b2f6eef73483b02e65` | `495dfc2a6c976a34cdd0f97392691c51729b21c19765e32886b1d45b684d6247` |
| candidate | `ba88564270b088cfe99948b4c33f6a130d7c1010` | `05213424cba2024cde5c53d41e5299e586f93f843a28ac561b7684c17cfff457` |
