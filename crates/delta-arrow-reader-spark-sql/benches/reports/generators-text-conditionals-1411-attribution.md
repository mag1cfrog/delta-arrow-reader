# Generator, UTF-8 and conditional cost attribution (#1411)

All nine frozen shapes matched the native reference values and executed
types in both ANSI modes. The checks compared one million source and
projection rows per shape. INLINE returned 937,500 rows, STACK returned
two million and COUNT_IF returned one row.

Follow-up slices are ordered by the largest #1391 difference in either
ANSI mode. INLINE_OUTER, DECODE and ENCODE have controls that do different
work, so their reports give absolute cost and describe the remaining work.

| Function | Reported gap in ns per input row | Additional Spark work |
| --- | ---: | --- |
| INLINE_OUTER | 98.93 | Per-row list slicing for the emptiness test before outer UNNEST. |
| DECODE | 29.17 | Charset normalization and UTF-8 conversion and buffering for each value. |
| ENCODE | 27.41 | Charset normalization and copying bytes into a new output buffer. |
| WHEN | 12.16 | General expression CASE selection and assembly for a column and NULL. |
| INLINE | 8.81 | Validity-aware STRUCT field projection and list expansion. |
| COUNT_IF | 7.46 | Rewriting the Boolean predicate to a nullable aggregate argument. |
| NANVL | 6.44 | Eager replacement evaluation and construction of the result buffer. |
| STACK | 4.75 | Zipping a constructed scalar list into structs and projecting it back to a list. |
| CASE | 3.81 | General expression CASE selection and assembly with an ELSE branch. |

## INLINE_OUTER and INLINE

The [generator resolver](../../vendor/sail/crates/sail-plan/src/resolver/tree/explode.rs)
lowers INLINE_OUTER to `array_struct_field(CASE WHEN empty(records) IS NOT DISTINCT FROM false THEN records END, x)`
followed by UNNEST with null preservation. The native outer reference projects
the known fixture field directly. DataFusion's `general_array_empty` in
`datafusion-functions-nested/src/empty.rs` iterates through list values
and slices each nonnull child to test its length. An offset comparison
can supply the same predicate for canonical list arrays while retaining
the existing CASE, validity and expansion. The outer profile contains
182 primitive slices and 84 inherent STRUCT slices. Those symbols also
occur outside the emptiness probe, so the counts cannot measure it in
isolation.

INLINE skips the emptiness probe. Its
[ArrayStructField](../../vendor/sail/crates/sail-function/src/scalar/array_struct_field.rs)
unions STRUCT and child validity and rewraps a field with the list offsets.
UNNEST expands the result and copies selected input rows. The native fixture
reference filters the known one-item list's parent and projects `i`. The
profile has 47 unnest samples and two field projection samples. The
projection can avoid reconstructing child data when the parent has no
NULLs. The row expansion still handles multiple elements and empty or NULL
parents.

## DECODE and ENCODE

The Spark plan calls `spark_decode` and `spark_encode` with a scalar
UTF-8 charset. In the
[conversion kernels](../../vendor/sail/crates/sail-function/src/scalar/string/spark_encode_decode.rs),
the scalar-charset array branch normalizes the same charset for every
nonnull row. UTF-8 decode copies bytes into a String, validates them and
appends them to the array. Encode copies the string to a byte vector and
then appends it. The native controls cast contiguous arrays without that
row preparation. The respective profiles show 45 and 56 samples in the
row converters and 39 and 42 in uppercase conversion. A batch path for
scalar UTF-8 and contiguous inputs can share buffers when Arrow's cast
succeeds. The current branch remains the reference for invalid UTF-8,
NULLs, storage variants and other admitted charsets.

## WHEN and CASE

The
[conditional resolver](../../vendor/sail/crates/sail-plan/src/function/scalar/conditional.rs)
marks Spark searched CASE with a true base to retain Spark's declared
result rules. The
[physical planner](../../vendor/dependencies/datafusion-physical-expr/src/planner.rs)
casts the arms to that declared type and keeps the base. The
[CASE evaluator](../../vendor/dependencies/datafusion-physical-expr/src/expressions/case.rs)
then takes the general expression path for `CASE true WHEN flag ...`.
Native searched CASE can use a validity mask for a selected column or
a specialized two-arm evaluation. Each Spark profile has 29 samples in
`ResultBuilder::finish`. The ELSE benchmark also shows a Spark metadata
wrapper around the cast. A physical shortcut needs to preserve declared
types, NULL conditions and evaluation of only the selected branch. The
missing-ELSE and explicit-ELSE shapes have separate comparisons.

## COUNT_IF

The
[aggregate resolver](../../vendor/sail/crates/sail-plan/src/function/aggregate.rs)
lowers COUNT_IF to COUNT over a CASE that replaces false with NULL.
DataFusion simplifies the CASE to Boolean mask expressions in the
aggregate argument. The native plan uses a constant COUNT argument
filtered by the Boolean column. The profile has one named Kleene AND
leaf, with fixture work dominating the sample. The physical plans show
the extra predicate stages. A simple Boolean column without a separate
FILTER can feed the native filter path if the existing aggregate result
and error behavior are preserved.

## NANVL

The
[conditional resolver](../../vendor/sail/crates/sail-plan/src/function/scalar/conditional.rs)
lowers NANVL to the DataFusion scalar UDF. The physical input includes
`d + 1.0` for every row. The
[NANVL kernel](../../vendor/dependencies/datafusion-functions/src/math/nanvl.rs)
then zips the two arrays and builds the output even when the left buffer
has no NaNs. The native CASE evaluates its replacement only on selected
rows. In the fixture `d` contains finite values and NULLs. Iterator and
fixture frames dominate the profile, with no named NANVL leaf. The
physical plan and kernel give the work to check in a bounded comparison.
A shortcut must preserve errors in the eagerly evaluated right argument.

## STACK

The
[STACK resolver](../../vendor/sail/crates/sail-plan/src/function/generator.rs)
builds one array per output column, zips them into structs and lowers
that list through INLINE. For this single-column shape, the native
plan makes and unnests the scalar list directly. The profile has 91
ArraysZip samples, 49 in Spark array construction and 87 in unnesting.
The one-column construction can avoid the ZIP and field projection if
the output name, type, NULL padding and cardinality are preserved.

## Reproduction

Both captures used release executable source `44827ae5e95edec98ea54554df5a23a6bb478985`
and pinned staging `3f287bfcea3b2402e3611c9fd5a2046afc81ec61`.
Its SHA256 was `9f13e80664c34f0da3f7c9bb99488d5550653fb96ae8e48235746d2033a666bc`.
The [protocol](../generators_text_conditionals_1411/protocol.md)
was committed at `962f992850cedc6e161981d1410be62996fa0a3b` before the
first capture (07:01:42Z to 07:04:56Z on 2026-11-21).
Its shared observer assumed one result per source row for generators.
INLINE and STACK passed value and type checks and were left unsampled
by that row guard. Commit `2f7e386be1f958846ec5eae0ef978c3ca1d1a36b`
pinned their expected output counts before the supplemental capture
(07:05:25Z to 07:06:31Z). Each function has one profile.

Leaf counts come from the original instruction pointers via
`perf script -G -F comm,pid,time,event,ip,sym,dso,period`. Many DWARF
chains do not resolve to source frames. The counts include fixture,
projection and query work and support the plan and source trace.
Local evidence is under `costs-168-2026-11-20/1411`.
