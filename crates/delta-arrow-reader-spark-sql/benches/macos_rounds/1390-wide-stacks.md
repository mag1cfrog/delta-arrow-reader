# #1390 call stacks for the wide planning statement

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

Use the released and verified staging binary from the scan ordinal
session, built at `1d74a82d`. Its frozen `planning profile` command
plans `stress/wide_400` once, then builds the physical plan 100 times.
It uses the same mains power, High Power, sleep prevention, and
user-interactive QoS controls as the other #1390 sessions.

Capture one second of call stacks at a 1 ms interval with the macOS
`sample` tool in each process. Run two arms of the same release in
both ANSI modes across nine blocks. Alternate mode and arm order.
Bound the capture session at 15 minutes and each process at 30
seconds. Archive each result and any partial captures.

For each completed capture, use stacks under
`rewrite_projection_given_requirements` to identify the code still
doing work inside the projection rule. Record the stack counts and
source locations. These are stack sample counts. The phase observer
supplies the wall-time attribution. A repeated stack in both ANSI
modes supports a named cause. An incomplete or unsymbolized capture
provides no evidence. This diagnostic does not decide material speed
or product merges.
