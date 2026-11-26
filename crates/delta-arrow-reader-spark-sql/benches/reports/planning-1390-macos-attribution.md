# Planning phase attribution on macOS (#1390)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).
The observed source was `cd8351ee`, equivalent to staging
`1ac767eb` for all release benchmark inputs. All 678 canonical trace
comparisons passed. The two A/A arms captured 2,160 process records
and 19,440 retained plans across 30 statements and four configurations.
[Protocol](../macos_rounds/1390-attribution.md) |
[Preflight and build receipts](planning-1390-macos-attribution-preflight.json).

## 400-column physical planning

**Apple Silicon, native macOS.** Spark minus native-configured in ms per
statement. Each pair is ANSI off / on. Shares use the observed phase
gap. Nested spans overlap and their shares cannot be added.

| Span | Paired excess | Combined A/A floor | Share of phase gap |
| --- | ---: | ---: | ---: |
| Physical planning | 22.822 / 22.879 | 6.096 / 7.481 | 100% |
| Projection rule | 6.435 / 6.418 | 1.641 / 2.400 | 28.2% / 28.1% |
| Initial physical plan | 2.918 / 2.906 | 0.502 / 0.757 | 12.8% / 12.7% |
| Alias conversion within initial planning | 1.720 / 1.708 | 0.329 / 0.475 | 7.5% / 7.5% |
| Physical optimizer | 2.756 / 2.684 | 0.658 / 0.549 | 12.1% / 11.7% |
| Analyzer | 3.614 / 3.633 | 1.757 / 2.103 | 15.8% / 15.9% |

The projection rule reuses scan ordinals after #1568, then calls
`Projection::try_new` to rebuild the projection schema. In an additional
36-process stack capture, the rewrite routine was visible in 17 of 18
captures per ANSI mode. Its call-graph nodes accounted for 80 samples
with ANSI off and 73 with ANSI on. Of those, 72 and 69 respectively
included `Projection::try_new` followed by `projection_schema`. The
expression field calls accounted for 70 and 68. The scan reconstruction
branch accounted for seven and three. These are inclusive stack counts
under the rewrite routine. The first remaining cause in that rule is
the per-column field resolution during projection schema reconstruction.
[Optimizer call site](../../vendor/dependencies/datafusion-optimizer/src/optimize_projections/mod.rs#L885) |
[Schema constructor](../../vendor/dependencies/datafusion-expr/src/logical_plan/plan.rs#L2471) |
[Stack capture protocol](../macos_rounds/1390-wide-stacks.md).

Initial physical planning includes alias expression conversion and
`ProjectionExec` construction. The constructor's excess was
0.998 / 1.003 ms, with 0.176 / 0.211 ms combined floors. Physical
optimizer rules run after that initial plan. The earlier ordinal
physical lowering trial stayed on its held branch because neither
ANSI mode cleared its A/A floor. The latest uninstrumented staging
gap remains 22.026 / 21.887 ms.
[Phase hooks](../planning_round3_1390/observe_default_planner.py) |
[Held trial](planning-1390-macos-physical-renames.md).

## Logical planning

**Apple Silicon, native macOS.** Values are Spark minus
native-configured in ms per statement unless marked as Spark-only.

| Statement | Observed phase gap, ANSI off / on | Combined A/A floor | Largest attributable spans |
| --- | ---: | ---: | --- |
| CASE with 200 branches | 5.213 / 5.262 | 2.524 / 2.717 | Resolution 3.056 / 3.043 ms (59% / 58%), parsing 2.044 / 2.040 ms (39% / 39%) |
| IN with 1,000 literals | 2.833 / 3.062 | 1.032 / 1.388 | Parsing 2.706 / 2.781 ms (96% / 91%) |
| Median of 25 ordinary statements | 0.224 / 0.221 | 0.089 / 0.091 | Parsing 0.172 / 0.170 ms (77% / 77%), resolution 0.095 / 0.097 ms (42% / 44%) |

Within Spark parsing, the grammar span spent 2.340 / 2.333 ms for
the CASE statement and 3.146 / 3.183 ms for the IN statement. These
are Spark's grammar costs. Native parsing is measured as a whole.
The IN grammar passes all 1,000 comma-separated elements through the
full recursive expression parser. The CASE grammar parses each
condition and result separately, then the Spark resolver builds its
typed expression. Ordinary statement parser construction accounts for
0.084 / 0.084 ms in the Spark path. Median component deltas are
computed independently.
[Fast statement parser](../../vendor/sail/crates/sail-sql-analyzer/src/parser.rs#L81) |
[IN and CASE grammar](../../vendor/sail/crates/sail-sql-parser/src/ast/expression.rs) |
[Query resolution entry](../../src/statement_planning.rs#L31).

## Statement coverage

**Apple Silicon, native macOS.** Each baseline priority is the largest
above-floor #1560 difference for the statement in either mode or
phase. An observation appears here when its phase was slower in the
baseline, the observed phase clears its combined floor, and the span
clears its own floor. The full table retains both modes, every span,
and the measured values for cells within the floor.

| Statement | Baseline priority, us | Largest observed span, us and share of phase gap |
| --- | ---: | --- |
| `stress/wide_400` | 27321.9 | logical optimizer 10727.2, 47% (on) |
| `stress/case_200` | 10364.9 | resolution 3055.5, 59% (off) |
| `stress/in_1000` | 5641.5 | statement parsing 2781.4, 91% (on) |
| `stress/join_six` | 2628.8 | logical optimizer 897.1, 36% (off) |
| `paired/correlated_scalar` | 2432.2 | statement parsing 213.6, 64% (on) |
| `stress/cte_eight` | 2348.7 | logical optimizer 1115.4, 41% (on) |
| `paired/cte_chain` | 2039.0 | logical optimizer 608.2, 36% (on) |
| `paired/three_way_join` | 1993.0 | analyzer 385.5, 27% (on) |
| `paired/inner_join` | 1519.4 | physical optimizer 270.2, 27% (on) |
| `paired/left_join` | 1501.1 | statement parsing 162.6, 93% (off) |
| `paired/exists` | 1334.1 | Within the observed floor |
| `paired/having` | 1290.0 | Within the observed floor |
| `paired/intersect` | 1185.0 | resolution 435.9, 75% (on) |
| `paired/except` | 1170.9 | resolution 390.7, 74% (off) |
| `paired/union_all` | 1081.6 | logical optimizer 298.4, 40% (on) |
| `paired/window_sum` | 1077.8 | analyzer 258.5, 35% (on) |
| `paired/row_number` | 1057.0 | analyzer 249.0, 35% (off) |
| `paired/delta_filter` | 1003.3 | logical optimizer 390.0, 48% (off) |
| `paired/global_aggregates` | 936.5 | analyzer 350.1, 38% (on) |
| `paired/delta_aggregate` | 825.1 | Within the observed floor |
| `paired/delta_projection` | 801.6 | logical optimizer 330.5, 49% (off) |
| `paired/filter` | 791.0 | physical optimizer 115.4, 23% (on) |
| `paired/ordered_limit` | 759.4 | statement parsing 192.1, 90% (on) |
| `paired/grouped_aggregates` | 708.5 | Within the observed floor |
| `paired/struct_access` | 665.1 | Within the observed floor |
| `paired/map_access` | 622.2 | analyzer 300.8, 56% (on) |
| `paired/null_filter` | 613.3 | physical optimizer 138.6, 22% (on) |
| `paired/array_projection` | 597.3 | analyzer 122.8, 25% (off) |
| `paired/projection` | 446.8 | Within the observed floor |
| `paired/distinct` | 392.0 | Within the observed floor |

The [full phase table](planning-1390-macos-attribution-phases.json)
records per-mode medians and floors for all 30 statements and the
ordinary logical aggregate. The timings include the observation
hooks and locate costs for review. Only uninstrumented paired
candidate sessions decide a performance merge.

- [Compressed samples for 2,160 observed processes](planning-1390-macos-attribution-samples.jsonl.gz)
- [Compressed archives for all 36 stack captures](planning-1390-macos-wide-stacks.jsonl.gz)
