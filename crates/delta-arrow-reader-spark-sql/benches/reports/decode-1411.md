# Validated UTF-8 DECODE results (#1411)

DECODE shares contiguous binary buffers for scalar UTF-8 after
checking NULL byte ranges and validating the bytes and offsets. Invalid
visible values and nonempty NULL ranges use the original decoder.

## Remaining work

The reusable path checks the charset and NULL byte ranges, then validates
UTF-8 and its boundaries. Arrow can rebuild valid visible rows when
surrounding bytes prevent sharing. The native cast is a cost control
on valid bytes with its own handling of malformed values and NULL storage.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| DECODE | Native | 0.809 [0.775, 0.849] | 0.817 [0.776, 0.976] | 0.082 |
| DECODE | ANSI off | 30.818 [30.364, 44.466] | 1.217 [1.194, 1.230] | 0.082 |
| DECODE | ANSI on | 30.778 [30.436, 31.519] | 1.204 [1.173, 1.222] | 0.082 |

Each timed statement matched all 1,000,000 result rows
and their executed Arrow types across both builds, both ANSI modes and native.
The differential compares all six admitted charsets, scalar UTF-8
spelling with mixed case and whitespace, NULLs, empty and sliced
batches, invalid byte sequences and split codepoints at both widths.
The initial candidate retained one byte under a NULL, where the original
decoder emitted an empty span. The guarded candidate passes that same
differential for valid and invalid bytes under NULL.
The SQL boundary checks matched all 24 rows and executed types for
DECODE, TO_CHAR and TO_VARCHAR, including varying charsets. Malformed
UTF-8 retained the exact execution error in both ANSI modes.
All 646 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../generators_text_conditionals_1411/decode-protocol.md) was
committed at `89dd563fe33dd35c53a994bb0221aebc5aee09bb` before clocks.
The Linux release session ran 2026-11-21T07:48:57Z to 2026-11-21T07:49:29Z.
The 18 comparison processes and 6 native controls retained 72 pairs.
Local evidence is under `costs-168-2026-11-20/1411/decode`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `edfdafd1bc7afa705fe8825a33d5e7ce7c4e1f1f` | `41b087b967a935f33870d326fbd1b48405e02728c1665afdd97e5d2f9edfa46d` |
| candidate | `fa4e9d46fa0a52a0ebb4277b4f16c68d577d89dc` | `e0b885a3915c3de94cff8834d8af9172ed977c4bfa315b29c1c00ec6501456c7` |
