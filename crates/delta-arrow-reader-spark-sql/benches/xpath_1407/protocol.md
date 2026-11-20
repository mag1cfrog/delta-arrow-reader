# XPath row cost (#1407)

Measure the nine original #1391 statements on the first 256 seeded rows of
its table, including the same 16 NULLs. The initial staging source is pinned
in the manifest. Build on Linux with Rust 1.96.0, locked dependencies,
`--release`, and `CARGO_PROFILE_RELEASE_DEBUG=1`. Use the original 8,192-row
batch size, one partition and one Tokio worker, CPUs 2-5, and the 12 GiB
address-space cap. No GitHub runner timings.

From an isolated checkout of the pinned source, prepare two disposable copies
of the #1391 driver with the reduced row count. The driver saves every
preflight row with its input ID, and records the physical and executed Arrow
types using the original guard. The second build also records phase counters
from the original vendored implementations. Ordinary wall times come from
the first build. No product source changes belong in this PR.

Before taking timings, require both builds to return the same 256 rows,
including NULLs, and the same executed types in both ANSI modes. Require the
original physical function, source row guard, and projection to be present.
Exclude a function if its values or types do not match.

Run the original projection and query pair, with one warmup and three
retained samples per process. Alternate the two builds in the same session.
Run three replicas, reverse form order on the second replica, and rotate the
nine IDs left by three places each replica. Each process has 120 seconds.
Report median, min and max of the three process medians, in ns per input row,
including the paired projection-subtracted time. Do not replace a completed
observation with a retry.

The scratch phase clocks separate XML document construction and parsing,
`Queries` builder creation and destruction, path wrapping and compilation
(including construction of xee's static context), query execution (with xee's typed conversion), and output
building. Output includes list node conversion, Arrow appends, and builder
finalization. Report separate batch wall and the residual, which includes
destructors and instrumentation. The phase runner prints one record per
batch and checks that each full iteration has 256 rows and 240 document and
compile calls. Discard its first iteration. Phase totals describe the
instrumented build. Use them to identify costs, not as an alternate wall
benchmark. A second optimization is eligible only for a separately
measured cost with work that is independent of each row and remains after
reusing a compiled query.

Save the source and binary digests, preflight output and raw timing records
locally. Commit this protocol, the manifest and the helpers before the run.
