# SLICE row-error priority

These 24 Spark observations compare failures on different input rows and
different SLICE operands. The Rust replay runs all 12 scenarios with ANSI
enabled and disabled at batch sizes 1 and 32, for 48 strict comparisons.
The original eight comparisons run in the strict slice_null_evaluation.rs suite.

The capture uses Spark 4.2.0 at revision
32f7299601108917fb01920a54e084595b7b3bf8, one input partition, and UTC.
The manifest and raw header preserve the original DAR preparation commit
d2f9e5eb50d50f9eae0c285b51bc52ab4ecf5cdf. The saved source and reference bytes
were copied without recapturing them.

Run the independent verifier from this directory:

    python3 verify_capture.py

It checks input order, complete fields and schema stages, exact observation
identities, structured errors, and source hashes without loading Spark.
The Rust replay uses the existing Spark result observer and also checks the
exact failing CAST input. A matching error category alone is insufficient.

SHA256SUMS includes the raw reference and its capture, verifier, and manifest
sources. The raw reference SHA256 is
4d1f61567e1f460561a931b55e805318dde7b09b91c55735b9a9fe97abb5ebc6.
Shipped tests load these files from the crate and need no external evidence
directory.
