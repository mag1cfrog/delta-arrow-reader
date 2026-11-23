# SLICE profile of the remaining difference (#1410)

One stack profile identified the shared list slicing kernel but did
not isolate the cost around it. The existing SLICE implementation
stays in place. The last paired comparison measured 25.443 and
25.817 ns per row with ANSI off and on, against 22.632 ns native.
The differences of 2.811 and 3.185 ns per row remain open.
This profile does not replace that paired comparison.

On current staging, native plans `array_slice(arr, 1, 2)`. Spark
plans `spark_slice(arr, 1, 2)` in an extra projection. Its positive
scalar branch passes the same coordinates to native with the Spark
result field. Native and both Spark modes matched all one million
rows and the executed Int64 and List(Int64) types.

The ANSI-off profile retained 70 stack snapshots. All six stacks that
include the Spark wrapper also include the shared native slicing
kernel. They reach allocation, list extension and buffer operations
inside the kernel. The profile did not capture separate work spending
time adapting the fields or processing row errors. The physical plan
shows where those differences can arise, but the stack sample does
not assign the residual to a particular operation.

The [protocol](../array_functions_1410_round3/slice-protocol.md) was
committed at `b9345087` before the profile. The release binary was
built from `f5ea78cb8485523738775056d28a2232e9c32762`, with the same
runtime source as staging `81c480e47e4ac5de53cb8f06913a9303f286b6a5`.
Its SHA256 is
`95af9da86355f99940a1c3834422a3f878b2e203410caabc55f124d8a666f444`.
The first debugger launch omitted the benchmark subcommand and exited
before starting SQL or recording a timing. The corrected launch ran the
statement once. LLDB lost the exit connection after the benchmark emitted
the successful case summary and completion marker. The archive keeps all
[three profiled samples](../array_functions_1410_round3/slice-profile-samples.jsonl.gz)
and the [stack snapshots](../array_functions_1410_round3/slice-profile-stacks.jsonl.gz).
