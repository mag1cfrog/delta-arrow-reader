# Residual interval parser cost (#1389)

The owned Chumsky token stream costs about 143 ns per day-time
conversion and 198 ns per calendar conversion. Both exceed a tenth
of the end-to-end difference left after the lexer change. The next
candidate will feed the existing numeric interval grammar from the
already lexed slice. The full grammar still receives the original
owned stream when the numeric grammar declines an input.

## CPU samples

The direct row parser processed 2,097,152 inputs for each shape,
following the [profile protocol](interval-1389-residual-protocol.md).
The table assigns sampled CPU periods to the reported leaf routine
and divides by calls. Stream and mapped input are separate leaf
categories. The mapped input row can include several input kinds.

| Input | Samples | With call frames | CPU ns/call | Owned stream next ns/call | Mapped input next ns/call | 10% of residual ns/row |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Year-month | 2,032 | 403 | 972 | 68 | 113 | 79 |
| Day-time | 3,573 | 525 | 1,709 | 143 | 366 | 85 |
| Calendar | 5,378 | 760 | 2,572 | 198 | 520 | 167 |
| Quoted year-month | 4,242 | 775 | 2,029 | 61 | 40 | 124 |

All sample leaf instructions resolved. The call-chain view was
truncated or empty for many samples, so the retained capture has
both the call-chain and leaf decodes from the same data. No timing
was repeated. The two evidence views retain all 15,225 sample
periods.

The timed conversion's `interval_parser` passes the lexed tokens
through `Stream::from_iter`. In Chumsky 0.12,
`Stream::next` first fills another vector from the iterator and
clones the token it returns. The one-shot numeric interval parser
already uses a mapped slice of the same token and span pairs.
Use that input for the numeric batch path to avoid the extra cache
and the successful branch's vector clone. Continue to evaluate the
same grammar and retain the original rich-parser fallback.

The [compressed profile](../interval_1389_round3/residual-profile.tar.gz)
records every sample in both views. The probe executable SHA-256 is
`b9d7b457d0d23338a7a1f65d4722104363016e1d31ccc3ce0c1bd69a9c74a506`.
It links the analyzer object with SHA-256
`c91271971aedc85312664863c10bad227d07e958684850bf1f92ec15a10a005b`.
Its two interval entry point sizes match the timed lexer candidate.
