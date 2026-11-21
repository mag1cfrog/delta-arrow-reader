# Sliding aggregate baseline and phase attribution (#1408)

The string windows spend most of their measured extra time creating and
materializing each frame's list. MAX_BY and MIN_BY recreate the accumulator
and replay the frame. Numeric PERCENTILE already retracts rows, with most of
the measured difference in the update and retract paths.

## Release execution and native noise

Times are ns per input row above the paired projection. Each table cell
gives the median and range of nine retained samples from three processes.
The last column is the same-session native A/A noise floor. Spark-only
cases have no native function that supplies the required reference.

| Function | Native | Spark ANSI off | Spark ANSI on | Native noise |
| --- | ---: | ---: | ---: | ---: |
| `listagg` (aggregate) | 14.6 [14.4-15.0] | 21.0 [18.7-21.9] | 21.2 [18.7-21.4] | 0.3 |
| `string_agg` (aggregate) | 14.7 [14.5-15.2] | 21.0 [18.8-21.5] | 21.0 [18.6-21.5] | 0.4 |
| `array_join` (window) | 419.6 [408.4-432.9] | 1,063.7 [1,045.6-1,069.0] | 1,060.9 [1,046.8-1,077.1] | 11.1 |
| `count_min_sketch` (window) | n/a | 877.3 [866.5-888.0] | 876.3 [826.0-907.3] | n/a |
| `histogram_numeric` (window) | n/a | 1,229.7 [1,215.5-1,238.5] | 1,226.7 [1,217.0-1,240.0] | n/a |
| `hll_sketch_agg` (window) | n/a | 428.4 [422.9-444.6] | 426.2 [422.7-441.6] | n/a |
| `hll_union_agg` (window) | n/a | 947.0 [935.7-995.4] | 950.4 [927.4-1,043.2] | n/a |
| `kurtosis` (window) | n/a | 254.0 [252.4-255.3] | 254.2 [252.6-256.8] | n/a |
| `listagg` (window) | 414.8 [408.0-426.3] | 1,062.4 [1,057.1-1,080.8] | 1,061.1 [1,050.9-1,071.4] | 17.5 |
| `max_by` (window) | 147.2 [145.8-147.7] | 478.5 [464.7-491.8] | 485.5 [470.2-495.4] | 10.5 |
| `min_by` (window) | 146.8 [145.9-148.6] | 415.1 [408.3-421.4] | 420.4 [410.3-427.5] | 3.3 |
| `mode` (window) | n/a | 385.8 [382.5-389.0] | 385.2 [382.6-385.6] | n/a |
| `percentile` (window) | 486.5 [480.8-490.1] | 636.4 [626.9-646.2] | 632.0 [624.3-638.7] | 4.4 |
| `product` (window) | n/a | 238.0 [237.1-248.5] | 237.1 [236.3-240.9] | n/a |
| `skewness` (window) | n/a | 252.0 [250.1-255.7] | 251.8 [250.6-252.9] | n/a |
| `string_agg` (window) | 415.8 [412.3-426.8] | 1,059.0 [1,046.9-1,069.0] | 1,071.6 [1,056.7-1,082.7] | 7.5 |
| `theta_sketch_agg` (window) | n/a | 699.8 [696.1-718.5] | 700.8 [697.9-706.7] | n/a |
| `theta_union_agg` (window) | n/a | 987.0 [968.0-997.4] | 987.0 [974.6-1,006.7] | n/a |

## Phase profiles

The instrumented build measured one profile process per path in this table.
Cells are medians of three retained samples in ns per input row. Update
includes new rows added without a reset. Replay includes slicing and
feeding the full frame after a reset. Materialize converts scalar results
into Arrow arrays. Convert covers list-to-string or the native Utf8 cast.
The remaining wall time includes sorting, frame bounds, output collection,
the surrounding CASE and projection, and probe overhead. It also includes
costs outside the selected clock boundaries. None of these runs used a
partial-state merge. The ordinary timing table is the performance baseline.

| Function | Path | Create | Discard | Replay | Update | Retract | Evaluate | Materialize | Convert | Remainder |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `listagg` (aggregate) | native | 0.0 | 0.0 | 0.0 | 8.4 | 0.0 | 3.3 | 3.0 | 0.0 | 0.4 |
| `listagg` (aggregate) | spark-off | 0.0 | 0.0 | 0.0 | 5.7 | 0.0 | 4.3 | 0.0 | 9.8 | 1.4 |
| `listagg` (aggregate) | spark-on | 0.0 | 0.0 | 0.0 | 5.7 | 0.0 | 4.3 | 0.0 | 9.8 | 1.4 |
| `string_agg` (aggregate) | native | 0.0 | 0.0 | 0.0 | 8.5 | 0.0 | 3.3 | 3.0 | 0.0 | 0.4 |
| `string_agg` (aggregate) | spark-off | 0.0 | 0.0 | 0.0 | 5.7 | 0.0 | 4.3 | 0.0 | 9.8 | 1.5 |
| `string_agg` (aggregate) | spark-on | 0.0 | 0.0 | 0.0 | 5.7 | 0.0 | 4.3 | 0.0 | 10.0 | 1.5 |
| `array_join` (window) | native | 52.6 | 23.7 | 172.9 | 3.6 | 0.0 | 22.1 | 23.4 | 1.2 | 279.0 |
| `array_join` (window) | spark-off | 0.4 | 0.0 | 0.0 | 182.2 | 96.5 | 398.6 | 124.9 | 74.6 | 294.9 |
| `array_join` (window) | spark-on | 0.5 | 0.0 | 0.0 | 179.3 | 96.4 | 398.7 | 126.1 | 78.2 | 295.9 |
| `count_min_sketch` (window) | spark-off | 201.6 | 22.1 | 188.6 | 3.9 | 0.0 | 71.4 | 60.8 | 0.0 | 489.5 |
| `histogram_numeric` (window) | spark-off | 85.0 | 23.6 | 210.6 | 3.5 | 0.0 | 385.0 | 248.2 | 0.0 | 427.7 |
| `hll_sketch_agg` (window) | spark-off | 84.1 | 20.6 | 165.5 | 3.0 | 0.0 | 33.0 | 22.4 | 0.0 | 265.3 |
| `hll_union_agg` (window) | spark-off | 72.9 | 20.2 | 670.4 | 4.9 | 0.0 | 23.2 | 22.3 | 0.0 | 268.6 |
| `kurtosis` (window) | spark-off | 24.9 | 18.8 | 109.5 | 2.3 | 0.0 | 18.8 | 10.4 | 0.0 | 245.5 |
| `listagg` (window) | native | 51.5 | 23.6 | 174.5 | 3.6 | 0.0 | 22.1 | 23.3 | 1.2 | 277.9 |
| `listagg` (window) | spark-off | 0.4 | 0.0 | 0.0 | 182.4 | 98.6 | 396.4 | 125.1 | 73.2 | 295.3 |
| `listagg` (window) | spark-on | 0.4 | 0.0 | 0.0 | 182.6 | 96.6 | 399.6 | 125.2 | 73.6 | 295.6 |
| `max_by` (window) | native | 0.1 | 0.0 | 0.0 | 0.0 | 0.0 | 25.3 | 8.9 | 0.0 | 146.6 |
| `max_by` (window) | spark-off | 64.6 | 20.3 | 285.8 | 3.4 | 0.0 | 19.3 | 8.6 | 0.0 | 258.3 |
| `max_by` (window) | spark-on | 63.2 | 20.8 | 284.8 | 3.4 | 0.0 | 19.3 | 8.7 | 0.0 | 257.8 |
| `min_by` (window) | native | 0.1 | 0.0 | 0.0 | 0.0 | 0.0 | 25.4 | 8.9 | 0.0 | 147.0 |
| `min_by` (window) | spark-off | 62.8 | 20.4 | 215.3 | 3.0 | 0.0 | 19.2 | 8.6 | 0.0 | 256.9 |
| `min_by` (window) | spark-on | 63.0 | 20.3 | 215.6 | 3.0 | 0.0 | 19.2 | 8.5 | 0.0 | 256.6 |
| `mode` (window) | spark-off | 45.2 | 20.4 | 174.6 | 2.7 | 0.0 | 59.0 | 9.2 | 0.0 | 242.9 |
| `percentile` (window) | native | 0.6 | 0.0 | 0.0 | 118.4 | 186.5 | 68.3 | 9.8 | 0.0 | 230.2 |
| `percentile` (window) | spark-off | 1.6 | 0.0 | 0.0 | 239.0 | 233.9 | 54.2 | 9.7 | 0.0 | 234.6 |
| `percentile` (window) | spark-on | 1.6 | 0.0 | 0.0 | 245.1 | 239.9 | 54.2 | 9.5 | 0.0 | 236.1 |
| `product` (window) | spark-off | 23.4 | 18.8 | 89.0 | 2.2 | 0.0 | 18.6 | 9.5 | 0.0 | 249.4 |
| `skewness` (window) | spark-off | 25.0 | 18.8 | 111.9 | 2.4 | 0.0 | 20.1 | 10.5 | 0.0 | 249.3 |
| `string_agg` (window) | native | 51.8 | 23.6 | 174.1 | 3.5 | 0.0 | 22.1 | 24.1 | 1.2 | 277.4 |
| `string_agg` (window) | spark-off | 0.4 | 0.0 | 0.0 | 180.0 | 96.7 | 397.3 | 125.0 | 73.4 | 296.6 |
| `string_agg` (window) | spark-on | 0.5 | 0.0 | 0.0 | 180.5 | 96.4 | 406.7 | 124.8 | 79.1 | 295.8 |
| `theta_sketch_agg` (window) | spark-off | 113.9 | 20.4 | 189.7 | 3.1 | 0.0 | 283.7 | 24.9 | 0.0 | 341.2 |
| `theta_union_agg` (window) | spark-off | 63.7 | 20.1 | 713.3 | 5.4 | 0.0 | 30.4 | 29.9 | 0.0 | 275.6 |

## Paired paths

- **ARRAY_JOIN, LISTAGG and STRING_AGG.** The Spark window variants all
  lower through `window::listagg` to `ArrayAggAccumulator` and a single
  `array_to_string` projection over the materialized list. The optimizer
  hoists the common expression out of the surrounding CASE. The accumulator
  already supports FIFO retraction. Per execution it adds 1,000,000 rows,
  retracts 979,520 and replays none. Its `evaluate` clones or slices the
  frame batches, concatenates them and builds a one-row list. The executor
  assembles those lists into Arrow before conversion. The native
  `SimpleStringAggAccumulator` replays 4,897,600 input rows in 979,520
  frames, with direct concatenation and a final Utf8 cast. In the two
  plain Spark aggregates, the same list-to-string conversion runs over
  the final list of input values. Their native counterparts concatenate
  during updates. The measured string cause is the intermediate list
  and its conversion. Keep the existing path for forms whose types,
  inputs or delimiter cannot be shown equivalent to a direct string result.

- **MAX_BY and MIN_BY.** Spark's `MaxMinByAccumulator` compares keys as
  `ScalarValue` and remembers the current winning pair. Dropping rows from
  that single pair cannot restore the previous winner. The executor creates
  983,616 accumulators and replays the same 979,520 frames for each function.
  A rolling winner for supported ordered key types can retain candidates and
  preserve the first winning tie. Native `NthValue` reads the first or last
  value directly with 4,096 evaluator creations, no state updates and no
  replay. This reference works on the fixture because its ordering key is
  unique and also orders the frame. General MAX_BY and MIN_BY need their
  own key comparison and tie handling.

- **PERCENTILE.** Spark's `NumericPercentileAccumulator::apply_batch` casts
  the value array to Float64, checks the optional frequency input and applies
  counts in a hash map on update and retraction. The native
  `PercentileContAccumulator` reads the typed input directly and retains
  raw values in a vector. Both paths add 1,000,000 rows and retract
  979,520. Spark's evaluation phase is already faster on this shape.
  The largest difference is in update and retract. A Float64 path without
  an explicit frequency can reuse the typed batch and update the existing
  counts without the generic conversion and frequency checks. Keep the
  original percentile interpolation and its operation order.

## Spark-only paths

Each profile below comes from the ANSI-off execution shown above. The
measured replaying paths each replayed 979,520 five-row frames.

| Function | Work independent of frame contents | Exact row removal |
| --- | --- | --- |
| COUNT_MIN_SKETCH | Literal epsilon, confidence and seed determine the same width, depth and hash coefficients at each creation. Rebuilding zero-filled state and hashing replayed rows drive the measured create and replay cost. | The given outgoing value determines every bucket to decrement, along with the integer total. This can preserve the serialized sketch and existing order-independent counts. |
| HISTOGRAM_NUMERIC | The bin count, initial PRNG seed and output fields are fixed. The output field layout can be reused in `evaluate`. Bins and PRNG progression belong to the current frame. | Trimming merges adjacent bins with rounded weighted averages and discards their original values. The merged state cannot reproduce the frame after removing an input. |
| HLL_SKETCH_AGG | The capacity setting is fixed. Each frame starts fresh sketch state. Hashing and serializing the frame depend on its inputs. | The HLL summary discards insertion history, so there is no exact inverse for an outgoing item in the current state. |
| HLL_UNION_AGG | The compatibility option is fixed. Each call to `merge_one` processes the current compact input and the accumulated union. Most measured time is in replay. | Registers and compact union results do not store each input's contribution. Removing a sketch requires recomputing from the retained inputs. |
| THETA_SKETCH_AGG | The capacity is fixed. `evaluate_bytes` compacts and serializes the current sketch, which depends on the frame. | A compact sample and its threshold drop information needed to undo insertion and preserve sketch bytes. |
| THETA_UNION_AGG | The capacity is fixed. Replay spends time decoding, unioning and compacting the current sketches in `merge_one`. | The union drops hashes beyond its threshold and loses input contributions. An exact inverse from the union alone is unavailable. |
| MODE | The primitive input type and tie rule are fixed. The measured path resets and repopulates a frequency map. | Primitive frequencies can be decremented exactly. Selection can keep the existing lowest/highest tie rule. Floating keys with incomparable values need their original path. |
| PRODUCT | Only the initial empty product is independent of the frame. | Dividing out an element changes the multiplication order, can round differently and cannot undo zero, overflow or NaN. |
| SKEWNESS | Only the initial empty moments are independent of the frame. | Inverting floating-point moment updates may change the result bits. Replay retains the existing update order. |
| KURTOSIS | Only the initial empty moments are independent of the frame. | Removing a row from the rounded fourth-moment state cannot ensure the original result bits or evaluation order. |

The sketch profiles time the whole factory and do not assign a separate
number to literal settings. Their replay and output phases depend on the
current frame. Replacing those algorithms would require proving agreement
for the exact sketch bytes.

## Reproduction

Both builds matched release output values and executed types over all 18
statements. The paired cases matched native in both ANSI modes. The
Spark-only cases agreed across builds and ANSI modes. The observer kept
exact floating bits and recorded physical plans. All 214 timing and
profile processes completed, retaining 642 executions.

The [protocol](../window_aggregates_1408/protocol.md) was committed before
the session at `56cf7b35b01936a22a0ce84f199517ddf047be4b`.
Both scratch builds use staging `7f66c78cb8a1e03f1f300fe0403f0636e37b122a`.
They used Rust 1.96.0 on Linux (Ryzen 7 8845HS, CPUs 2-5), with opt level 3
and debug level 1. The session ran 2026-11-21 00:04:04 to 00:17:55 UTC.
Local evidence is under `costs-168-2026-11-20/1408/attribution`.

| Build | Executable SHA256 |
| --- | --- |
| plain | `5e3c98a926124be3f9370725d79c6e99cae7ca6a6d63105ea315a6aa88e86925` |
| phases | `0a4f4c0e9a97ac0dd7c0267bd05e930c087de7c36f7ac9ced1c3bec89e35906b` |
