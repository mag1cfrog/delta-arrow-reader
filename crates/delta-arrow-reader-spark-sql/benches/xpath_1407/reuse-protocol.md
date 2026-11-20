# XPath batch reuse comparison (#1407)

Compare the release sources pinned in [reuse.json](reuse.json) by running each
pair in turn in one session on Linux. Use the unchanged nine #1391 statements,
8,192-row batches, one partition, one worker, CPUs 2-5, and the 12 GiB
address-space cap. Use locked Rust 1.96.0 release builds with debug level 1.
The larger run uses the full million-row fixture.

## Result gate

The paired prefix has 16,384 rows and includes all 31 non-NULL seeded XML
payloads and NULL. Execute each query in both ANSI modes on both paired builds
before timing. Require exact rows by ID, executed Arrow types and the original
physical function. For each candidate full-input row, use the corresponding
result from the staging prefix with the same XML payload and fixed path.
Require all 1,000,000 IDs exactly once and the same executed types. The fixture
uses non-NaN floating results, so comparison includes their exact bits.
Exclude any failing function. Review differences without timing it.

Before timing, run the focused SQL differential tests against both sources.
They compare 684 values with exact Arrow types and 54 complete first errors
to the [staging capture](../../tests/fixtures/xpath-batch-reuse/staging.json).
Inputs cover interleaved and literal paths, empty and NULL arguments,
conversion boundaries, malformed XML and XPath, and evaluation errors.
The shared batch-loop test checks compile counts with 300 alternating paths.
The runner reads the local edge-differential receipt with both source
commits, test and fixture hashes, and successful test counts before starting.

## Schedule and pass rule

Run staging and candidate in turn for each function and ANSI mode on the
16,384-row prefix. After those pairs, run the candidate on the full fixture.
The full run is an absolute bound check. The original million-row timeouts are
historical context. Use each size's original query and projection, with one
warmup and three retained executions per process. Run three fresh-process
replicas, rotate the sorted case IDs left by three each replica, and reverse
build and ANSI order on replica two. Each case has the original 120-second
bound and each process has 130 seconds including startup. Preflight has up
to 300 seconds per process, and the session has a 4-hour cap. Stop repeating
a timed case and build after a failure. Preserve raw samples and failures.

Pass requires exact paired and full results and executed types, no repeated
compilation of the same path within a batch, and every full candidate case
and replica completing within the original bound in both ANSI modes.
Report the median and range of process medians in ns per input row, with
paired projection-subtracted time. Keep the reduced and full sizes separate.
