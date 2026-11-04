# SLICE shared projection errors

The saved Spark records cover a repeated computed ARRAY, its named-subquery
variant, and an original input sort below SLICE. Six observations run in both
ANSI modes; the Rust replay checks each at batch sizes 1 and 32.

Spark 4.2.0 revision 32f7299601108917fb01920a54e084595b7b3bf8 supplied the
reference in UTC with one input partition. The manifest, capture, verifier,
and raw reference retain their original bytes and preparation commit
d2f9e5eb50d50f9eae0c285b51bc52ab4ecf5cdf. Historical absolute paths in the
manifest record provenance; replay reads this directory's files.

Run the verifier without Spark:

    python3 -B verify_capture.py

It checks the exact observation inventory, ordered provider and selected input,
all available complete schema stages, structured causes and parameters, and
capture source hashes. Its mutation checks reject incomplete records. The raw
reference SHA256 is
bef5d407f51ae133ba0ba7537bf0cec326a058201c4a7782e46ec3fe198423c5.

`slice_shared_projection_errors_920.rs` requires eight strict repeated/named
comparisons. Three sorted controls also match Spark. The sorted ANSI-true,
batch-1 control records the separate #919 difference: START instead of Spark's
CAST. That comparison remains explicitly non-strict. All 12 comparisons retain
complete fields, error causes, schema stages, and input observations.

The all-valid callback control asserts one actual shared computed producer,
four one-key invocations at batch 1, one four-key invocation at batch 32, exact
rows and fields, and no planning calls. Physical observations bind consumers to
the producer's real output index and assert prefix-transport admission. Generated
column names do not decide admission.
