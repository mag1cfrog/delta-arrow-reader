# Use exact validation as storage warmup

For a new revision 6 campaign covering one snapshot and both open/reuse profiles,
`--gate-warmup` uses the existing exact-value gates to warm storage before timing.
Each supported reader consumes the complete query results during its gates.
Every gate still exports and validates every projected value, and the controller
must prove process cleanup before proceeding.

Each independent timing invocation starts a new process, native client and, for
Spark, JVM. A separate warmup process therefore cannot retain a query plan, result,
connection pool or JIT state for the next process. It mainly warms MinIO and OS
caches. The gates already read the same snapshot's data. This mode removes the
additional storage-warmup invocations; it does not claim identical cache residency
or remove initialization from query clocks.

## Declare a new campaign

Add `--gate-warmup` to the existing revision 6 campaign command. Select exactly one
case and retain both profiles. The flag can accompany `--combined-diagnostics`.
The controller freezes `gate_warmup: true` and this document's SHA-256 as
`warmup_amendment_sha256` in the campaign configuration. Every inventory entry
also declares the method before timing. Earlier campaigns retain their standalone
warmup rules and identities.

Keep all five independent timed samples, reader order, newly planned reuse
queries, native builds, query definitions, resource limits and transport. Do not
reuse gate clocks as timing samples or reuse a gate's process for timing. A
successful matching gate, exact-value proof and completed cleanup are required
before a reader/profile can admit any timing samples.

With five supported readers, both profiles and combined diagnostics, a case has
62 post-gate invocations instead of 72. Ten exact gates still execute first. All
diagnostics remain after every timing slot. No reader rebuild or replacement
reference is needed.

## Compare and reproduce results

The overview records each campaign's warming method and amendment hash. Gate
warmup and standalone warmup are distinct preparation methods. Compare readers
within the same case and declared method; never pool samples across methods or
retag completed observations. All method changes must precede the new campaign.

The controller/report process also reuses a validated production binding while
the manifest's device, inode, size, mtime and ctime and the declared workload row
are unchanged. It still checks those identities on every reuse. A changed file or
row goes through the original validation again. The cache is scoped to one
synchronous controller/report operation; native runners and independent reference
preparation retain their existing source identities and validation paths.
