# XPath batch reuse (#1407)

Across the nine XPath functions and both ANSI modes, staging medians ranged
from 44.33 to 67.26 microseconds per input row above projection. Candidate
medians ranged from 4.12 to 4.90 microseconds on the same input.
The speedup for each matched function and mode was 10.3x to 13.8x.
All 54 candidate case runs on the original million rows completed within the
original 120-second case bound.

## Paired and full-input results

Times are ns per input row, after subtracting that execution's plain
projection. Each cell shows the median and range of three process medians.
Each process retained three executions after a warmup. The two 16,384-row
builds ran in turn in one session. The million-row column measures the
candidate against the original bound. #1391 recorded timeouts on that input.

| Function | ANSI | Staging, 16,384 | Candidate, 16,384 | Candidate, 1,000,000 |
| --- | --- | ---: | ---: | ---: |
| `xpath` | off | 66,956 [66,343-67,086] | 4,903 [4,849-4,913] | 4,940 [4,918-4,972] |
| `xpath` | on | 67,263 [66,548-68,620] | 4,889 [4,867-4,929] | 4,945 [4,923-4,949] |
| `xpath_boolean` | off | 44,464 [44,358-44,506] | 4,130 [4,093-4,132] | 4,150 [4,146-4,199] |
| `xpath_boolean` | on | 44,331 [44,283-44,557] | 4,119 [4,112-4,197] | 4,189 [4,146-4,210] |
| `xpath_double` | off | 45,377 [45,048-46,360] | 4,254 [4,239-4,271] | 4,350 [4,272-4,412] |
| `xpath_double` | on | 45,011 [44,965-45,042] | 4,263 [4,261-4,269] | 4,337 [4,288-4,337] |
| `xpath_float` | off | 44,762 [44,311-45,569] | 4,291 [4,288-4,292] | 4,279 [4,276-4,356] |
| `xpath_float` | on | 45,156 [44,825-45,511] | 4,285 [4,264-4,313] | 4,275 [4,263-4,314] |
| `xpath_int` | off | 45,497 [44,995-45,533] | 4,417 [4,255-4,938] | 4,267 [4,251-4,310] |
| `xpath_int` | on | 44,821 [44,575-45,487] | 4,302 [4,288-4,310] | 4,272 [4,262-4,275] |
| `xpath_long` | off | 44,762 [44,681-45,289] | 4,248 [4,235-4,271] | 4,312 [4,278-4,315] |
| `xpath_long` | on | 45,256 [44,790-45,780] | 4,251 [4,238-4,295] | 4,311 [4,296-4,338] |
| `xpath_number` | off | 45,276 [45,146-45,803] | 4,266 [4,259-4,293] | 4,309 [4,278-4,333] |
| `xpath_number` | on | 45,045 [44,869-45,232] | 4,309 [4,279-4,396] | 4,361 [4,289-4,382] |
| `xpath_short` | off | 45,363 [44,413-46,139] | 4,306 [4,305-4,309] | 4,302 [4,293-4,311] |
| `xpath_short` | on | 45,081 [45,026-45,098] | 4,286 [4,259-4,469] | 4,283 [4,280-4,319] |
| `xpath_string` | off | 44,579 [44,308-45,175] | 4,246 [4,239-4,254] | 4,265 [4,248-4,390] |
| `xpath_string` | on | 44,461 [44,375-44,982] | 4,236 [4,234-4,251] | 4,283 [4,256-4,314] |

On a million rows, the median entire execution lasted 4.15 to 4.95
seconds across the 18 function and mode combinations. The case bound also
includes warmup, projection and planning.

## Remaining row cost

A batch compiles each distinct usable path once and releases the program
after its last use. Each row still parses its XML, runs the XPath program
against a fresh execution context, and builds the result. The
[attribution pass](xpath-1407-attribution.md) separately measured these
phases before the change. It found no second row-independent cost outside
query preparation. These release timings measure the remaining work together.
Several repeated paths with overlapping uses can keep several programs
live. The timing statements all have literal paths.

## Validation and evidence

Both ANSI modes matched the executed Arrow types and all rows of the paired
prefix. The staging prefix covers all 31 seeded XML payloads and NULL.
Each of the 18 full-input result sets matched its staging reference by input
ID, value and type. The connected tests on both release sources also matched
684 boundary-case values and 54 complete first errors. The compile-count
test recorded one compile each for 300 interleaved paths. Workspace rustfmt,
root-crate Clippy with warnings denied, and the manifest checks passed.

The [comparison protocol](../xpath_1407/reuse-protocol.md) was frozen at
`1a645ca087174258aa99d1a456251dd492df2758` before timing.
The Linux session ran from 2026-11-20T23:11:54Z to 2026-11-20T23:33:55Z.
It used the same Ryzen 7 8845HS, CPU affinity and Rust 1.96.0 release profile
as the attribution pass. All 162 processes terminated successfully and
recorded 486 retained executions. Local receipts, exact preflight rows,
statements, plans and observations are in `costs-168-2026-11-20/1407`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `6c774893052e5eea5da25a02ac1b079223e5f1df` | `e3d966d6bc4d10cf5bae72313c45031a8e540351197504d246cce27c61c24255` |
| candidate | `100991ce5394e8fbe8e3bc116e9f55cb50e14136` | `b597f07cbf49f0fb221098dfd688ba8f3f20270ecac75796bf98e121fc635394` |
| full | `100991ce5394e8fbe8e3bc116e9f55cb50e14136` | `6ba48a5087ca72ec6b896e2d92658b18acd37fe0e479ba7839ffc276a0af9af5` |
