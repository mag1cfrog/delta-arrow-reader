# XPath baseline and phase costs (#1407)

Per-row XPath compilation accounts for 86% to 90% of the sum
of phase medians including the residual. The list function spends about 61
microseconds per input row compiling its path, and the typed functions
spend about 39. Each original
statement supplies one literal path. The next change can reuse its compiled
query while still parsing and evaluating each row independently.

## Execution time

All nine frozen #1391 statements ran on the first 256 seeded rows in both
ANSI modes. These uninstrumented numbers are in ns per input row, with the
median and range of three process medians. Each process retained three
executions after one warmup. Projection-subtracted time removes the
paired `SELECT id, xml_s FROM bench` from the same Spark configuration.

| Function | ANSI | Entire execution | Above projection |
| --- | --- | ---: | ---: |
| `xpath` | off | 67,644 [67,172-67,672] | 67,570 [67,084-67,583] |
| `xpath` | on | 66,932 [66,796-67,520] | 66,858 [66,707-67,435] |
| `xpath_boolean` | off | 44,781 [44,659-45,107] | 44,707 [44,573-45,034] |
| `xpath_boolean` | on | 44,901 [43,877-45,042] | 44,811 [43,788-44,962] |
| `xpath_double` | off | 45,262 [44,708-46,131] | 45,172 [44,619-46,042] |
| `xpath_double` | on | 45,662 [44,913-47,461] | 45,571 [44,839-47,372] |
| `xpath_float` | off | 45,281 [44,895-45,600] | 45,209 [44,783-45,525] |
| `xpath_float` | on | 45,985 [44,563-49,098] | 45,895 [44,474-49,010] |
| `xpath_int` | off | 45,563 [45,170-47,106] | 45,488 [45,083-47,007] |
| `xpath_int` | on | 46,705 [45,333-48,523] | 46,616 [45,258-48,433] |
| `xpath_long` | off | 45,731 [44,920-54,592] | 45,655 [44,830-54,390] |
| `xpath_long` | on | 45,518 [45,128-47,829] | 45,430 [45,034-47,741] |
| `xpath_number` | off | 46,402 [46,127-46,417] | 46,312 [46,039-46,340] |
| `xpath_number` | on | 45,182 [45,036-45,845] | 45,095 [44,949-45,758] |
| `xpath_short` | off | 46,096 [44,728-46,744] | 46,005 [44,656-46,638] |
| `xpath_short` | on | 44,734 [44,407-45,040] | 44,651 [44,320-44,952] |
| `xpath_string` | off | 46,181 [45,033-46,305] | 46,105 [44,959-46,206] |
| `xpath_string` | on | 44,984 [44,435-45,984] | 44,895 [44,347-45,892] |

## Phase attribution

Phase medians use the same units in the instrumented build. Every retained
iteration processed 256 rows and made 240 document and compile calls.
Evaluation includes xee's typed conversion. Output includes list-node
materialization and Arrow builders. The residual includes destructors, loop
overhead and the phase clocks.

| Function | ANSI | XML parse/setup | Queries setup/drop | Wrap/compile | Evaluate | Output | Residual | Instrumented batch wall |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `xpath` | off | 1,667 | 56 | 61,481 | 4,224 | 161 | 753 | 68,221 [67,614-70,090] |
| `xpath` | on | 1,627 | 58 | 61,285 | 4,339 | 152 | 758 | 67,989 [67,984-68,497] |
| `xpath_boolean` | off | 1,630 | 56 | 38,931 | 3,437 | 30 | 685 | 44,738 [44,415-48,556] |
| `xpath_boolean` | on | 1,621 | 56 | 38,985 | 3,442 | 29 | 669 | 44,798 [44,744-45,272] |
| `xpath_double` | off | 1,740 | 55 | 38,990 | 3,668 | 30 | 709 | 45,164 [45,137-45,631] |
| `xpath_double` | on | 1,718 | 49 | 38,874 | 3,714 | 27 | 726 | 45,349 [44,636-45,569] |
| `xpath_float` | off | 1,668 | 51 | 39,810 | 3,724 | 40 | 759 | 45,917 [45,054-46,332] |
| `xpath_float` | on | 1,738 | 48 | 38,943 | 3,808 | 38 | 737 | 45,302 [44,938-46,331] |
| `xpath_int` | off | 1,689 | 51 | 39,149 | 3,727 | 30 | 711 | 45,462 [45,407-45,557] |
| `xpath_int` | on | 1,688 | 47 | 38,980 | 3,745 | 29 | 730 | 45,222 [45,029-46,321] |
| `xpath_long` | off | 1,700 | 49 | 39,153 | 3,725 | 31 | 734 | 45,319 [45,172-45,748] |
| `xpath_long` | on | 1,674 | 49 | 38,751 | 3,656 | 29 | 724 | 44,949 [44,816-45,555] |
| `xpath_number` | off | 1,686 | 49 | 38,962 | 3,763 | 28 | 729 | 45,255 [44,575-46,645] |
| `xpath_number` | on | 1,646 | 49 | 38,929 | 3,819 | 30 | 715 | 45,023 [44,957-45,427] |
| `xpath_short` | off | 1,675 | 47 | 38,918 | 3,729 | 29 | 719 | 45,127 [44,733-46,416] |
| `xpath_short` | on | 1,685 | 49 | 39,831 | 3,873 | 34 | 768 | 46,219 [44,628-47,855] |
| `xpath_string` | off | 1,690 | 57 | 39,933 | 3,713 | 50 | 747 | 46,237 [45,047-46,276] |
| `xpath_string` | on | 1,643 | 56 | 38,989 | 3,553 | 43 | 708 | 44,981 [44,927-45,176] |

Batch wall is also normalized per input row. Batch medians and ranges
come from the same phase records and discard the same warmup.

Creating and dropping the `Queries` builder takes 46 to 58 ns per row and
occurs only when compiling. Building the static context is included in
wrap/compile. Reusing the compiled query addresses both costs. The remaining measured phases operate on the current row or its
output. This measurement identifies one optimization for the scope of #1407.

## Evidence

Both binaries agreed on all 9,216 preflight rows and their executed Arrow
types. The physical function and source-row checks passed for all cases.
The bounded session ran 108 timing processes, with 162 ordinary samples
and 162 instrumented samples. The original million-row case bound remains
the target of the next slice.

The [protocol](../xpath_1407/protocol.md) was committed before timing at
`c52ac280cee45359bb9e616a88b09ce076bb2a6c`. Both builds used staging
`eaabba0ba3c43c2bd9f011378f1e0040997c7c2d`,
Rust 1.96.0, opt level 3 and debug level 1 on x86-64 Linux 7.2.14
(AMD Ryzen 7 8845HS, CPUs 2-5). The session ran
2026-11-20 22:38:39 to 22:38:53 UTC.

| Build | Executable SHA256 |
| --- | --- |
| plain | `5b6c0fb30281fcf7d9660882506aecb03cabfe9625e764945b2edfc9ee27130a` |
| phases | `b71662849f24b82efdcc5544f8b64c4467e6d3607a8357a854824c42979b3754` |

Local evidence is under `costs-168-2026-11-20/1407/attribution-run`.
It includes the complete rows before timing, phase counters,
process spreads and both build receipts. The original session receipt
retained some unused #1391 schedule fields. The local
`effective-attribution-schedule.json` records the actual input, order and
overall deadline alongside the unchanged original receipt.
