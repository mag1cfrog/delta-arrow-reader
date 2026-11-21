# Array function costs (#1410)

ARRAY_MIN and ARRAY_MAX have the largest measured gap. Their Spark
implementation builds an aggregate accumulator for each list row. Native
DataFusion scans the primitive value buffer. The other six slices concern
separate work in membership, lookup, flattening, vector access, slicing and
sequence output.

All nine million-row outputs matched native by ID in both ANSI modes.
Executed column types and input guards also matched. The physical plans,
profile records and leaf symbols are retained under
`costs-168-2026-11-20/1410/attribution`.

## Causes in implementation order

The gaps below are the larger ANSI median differences from
[#1391](functions-1391.md), in ns per input row above projection. The new
profiles sampled the ANSI-off Spark plan. One process per function ran three
sets of one warmup and three retained pairs. Counts are sampled leaf instructions
over the whole process, including setup and the projection control. Use
them with the plan and source traces to identify the extra work.

| Slice | Largest #1391 gap | Sampled Spark work |
| --- | ---: | --- |
| ARRAY_MIN and ARRAY_MAX | 102.13 | The row accumulator loop had 198 and 203 samples. Its min and max update routines had 165 and 144 samples. |
| ARRAYS_OVERLAP | 90.61 | Membership symbols had 64 samples and Spark compaction had 115. |
| TRY_ELEMENT_AT and ELEMENT_AT | 49.42 | The length check had 20 and 17 samples. Array assembly also appeared in each profile. |
| FLATTEN | 45.16 | List row encoding had 117 samples and membership had 39. |
| VECTOR_INNER_PRODUCT | 27.30 | The dot routine had 10 samples. Primitive-array slices were the largest sampled leaf family. |
| SLICE | 15.24 | The Spark slice wrapper had 40 samples, including 21 in coordinate preparation. |
| SEQUENCE | 13.21 | Spark sequence implementations had 47 samples. Lazy child evaluation and result scattering had 37 and six. |

### ARRAY_MIN and ARRAY_MAX

Both plans display the same function names, with different physical UDFs.
[The Spark row loop](../../vendor/sail/crates/sail-function/src/scalar/array/spark_array_min_max.rs#L172)
slices the child array, creates a MinAccumulator or MaxAccumulator, updates
and evaluates it, then converts the accumulated Scalars into an array.
The native `min_max.rs` primitive path walks the list offsets and writes
the result directly to a typed array. The first slice can reuse the typed
BIGINT buffer route while retaining the current parent NULL and child
value semantics and the other types' accumulator path.

### ARRAYS_OVERLAP

The Spark plan prepares the operand type, checks parent NULLs and lengths,
probes for NULL elements, then compacts and tests membership in the affected
rows. This lowering in
[arrays_overlap](../../vendor/sail/crates/sail-plan/src/function/scalar/array.rs#L672)
handles Spark's three-valued result. The native plan evaluates one
`array_has_any` call. A direct typed membership path can combine the
observed length, NULL and membership work. It needs the original lazy
operand behavior on shapes where evaluation can fail.

### TRY_ELEMENT_AT and ELEMENT_AT

The Spark plans evaluate an `array_length` bounds test and CASE before
calling native `array_element`. The ANSI-on ELEMENT_AT plan retains its
error branch. The native plan calls `array_element` directly. The
[element_at lowering](../../vendor/sail/crates/sail-plan/src/function/scalar/collection.rs#L99)
can combine lookup with validation for the affected array shapes. Zero,
negative and out-of-bounds positions, NULL arrays and map lookup retain
their existing contracts.

### FLATTEN

Spark first looks for NULL inner arrays through `array_has_any` and a
CASE, then delegates to native FLATTEN for selected rows. The
[flatten lowering](../../vendor/sail/crates/sail-plan/src/function/scalar/array.rs#L898)
uses generic nested-row encoding for this membership probe. Native
`flatten.rs` can map outer offsets to child offsets directly. Checking
inner-list validity directly can preserve Spark's NULL-child behavior
with less work than generic membership.

### VECTOR_INNER_PRODUCT

The Spark plan casts to a Float32 list and calls its own vector kernel.
The native plan casts the list to Float64, calls its inner product, then
casts the result to Float32. In
[the Spark kernel](../../vendor/sail/crates/sail-function/src/scalar/vector/inner_product.rs#L112)
each vector pair creates child array slices before downcasting and
checking validity. Native `inner_product.rs` downcasts both child buffers
once per batch, then takes typed slices by row offset. The sampled slice
symbols are shared with other code, so the
profile cannot assign their full count to this kernel. Direct child
buffer access is the opportunity. The Spark eight-product accumulation
order, scalar tail, dimensions and NULL behavior define its results.

### SLICE

The Spark plan invokes `spark_slice(arr, 1, 2)`. Its
[coordinate loop](../../vendor/sail/crates/sail-plan/src/function/scalar/array.rs#L234)
checks start and length, reads each list's length, validates and appends
two coordinates before delegating to native ARRAY_SLICE. The native
statement already has its two scalar coordinates. The literal shape can
reuse validated coordinates where the array results match. Dynamic
arguments and row-level errors need the existing path.

### SEQUENCE

Spark's physical `spark_sequence_lazy` evaluates the start and stop in
order on active rows, including a checked addition with ANSI enabled.
The [integer generator](../../vendor/sail/crates/sail-function/src/scalar/array/spark_sequence.rs#L648)
then collects the per-row range lengths before the output pass. The
native plan evaluates arithmetic, generates a half-open range into the
child buffer and casts to the same list type. The sampled Spark paths
include both lazy preparation and integer generation. The opportunity is
to reduce sequence-specific range preparation and materialization while
preserving bounds and lazy evaluation. The profile cannot assign the
entire difference to either part alone.

## Reproduction

The [protocol](../array_functions_1410/protocol.md) was committed at
`bdf13a73e3681b21b466f87342a0de5eb9d733ef` before sampling.
The Linux session ran 2026-11-21T04:59:40Z to
2026-11-21T05:02:55Z. The release executable is the last #1409 build at
`42de836662a847b1059fafc29c8be4ab71d8a3f8`, with staging pinned at
`087925f6a3e56cbde5eec52cfe6009d9aea3377e`. The executable SHA256 is
`15e79c04e2fe31503dcc31fb74246270c44f14260eecce9730b545dc5d92e81a`.

The nine captures contain 2,029, 2,027, 2,086, 1,478, 1,486, 1,256,
1,492, 1,183 and 1,099 samples for ARRAY_MIN, ARRAY_MAX, ARRAYS_OVERLAP,
TRY_ELEMENT_AT, ELEMENT_AT, FLATTEN, VECTOR_INNER_PRODUCT, SLICE and
SEQUENCE, respectively. Many DWARF chains did not unwind to callers.
The counts above use leaf symbols from
`perf script -G -F comm,pid,time,event,ip,sym,dso,period` on the same
saved captures. They combine with the physical plans and exact source
paths to identify the work to remove.
