---
title: Selective-read sampling amendment
description: Revision 4 fixes the main data scale at SF10 and reduces repeated work.
---

# Selective-read sampling amendment

Revision 4 inherits the queries, exact validation, resource limits and storage
policy of the [large-workload contract](selective-read-large-workloads.md).
It replaces that contract's data-scale selection and repetition counts with
the owner's decisions recorded in [#345](https://github.com/mag1cfrog/delta-arrow-reader/issues/345).
The original protocol files and historical observations keep their identities.

## Data scale

Use SF10 for the main large-data family and SF1 for its scale controls. The
owner selected SF10 after the two-scale screen: the shuffled wide table has
59,986,052 rows and 35.95 GiB of Parquet, with reader median times from 22.46 to
106.94 seconds. This was an owner decision after calibration; the historical
60-second selection rule was not met. No runtime floor applies to a reader or
a successfully pruned query. Keep first/open, initialization and reused query
times separate. Do not extend the scale ladder to prolong a fast result.

The full publication inventory and the separate many-file geometry remain
pending. Selecting SF10 does not establish the 4,096-file/64 MiB requirement
or increase the 192 GiB data allowance.

## Queries and samples

An open invocation executes one query. A reuse invocation initializes one
native source and executes exactly two newly planned queries, consuming both
results completely. Record initialization, query 1, query 2, session totals
and cleanup separately. Validate both exports before timing that configuration.

For each case or reuse profile, the revision 4 campaign runs the following
invocations per runnable reader:

| Stage | Invocations | Included in headline timing |
| --- | ---: | --- |
| Exact validation | 1 | No |
| Warmup | 1 | No |
| Independent timed samples | 5 | Yes |
| Plan capture | 1 | No |
| Traced I/O diagnostic | 1 | No |

Persist all slots before timing. Use the first five orders from the existing
counterbalanced sequence, cycling it when fewer orders exist. With all five
readers, each reader occupies each position once. Smaller reader subsets keep
five samples; do not add rounds to complete a counterbalancing cycle. This
schedule does not claim full adjacency balance. Warmup, plan capture and I/O
use the fixed reader order; both diagnostic stages follow all timed slots.

Tracing is disabled for warmup and headline samples. This reduced schedule
does not estimate tracing overhead; diagnostic latency cannot replace native
timing. Retain all five capability entries, missing slots and failures. Do not
retry, discard samples or extend sampling after seeing favorable speedups.
Screening uses two independent single-query trials and cannot fill formal slots.

## Identity and compatibility

Every revision 4 workload, request, result, correctness certificate, campaign
slot and report carries this file's SHA-256 as `sampling_sha256`, alongside
the existing protocol and workload identities. Reject missing or changed
sampling identities, incomplete query sequences and altered schedules.

The request helper, native readers, watchdog, export limits and reports use
the same revision's query count. Revisions 2 and 3 retain their ten-query reuse
and historical campaign schedules for replay. Rebuild all readers and prepare
new workload/reference identities before executing revision 4. Existing
Parquet fixtures can be reused; old correctness certificates cannot.
