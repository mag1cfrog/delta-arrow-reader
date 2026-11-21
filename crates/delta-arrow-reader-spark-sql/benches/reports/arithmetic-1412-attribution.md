# Arithmetic and integer function attribution (#1412)

All 15 frozen #1391 shapes qualify for timing. On staging, both ANSI
modes and native matched values and executed Arrow data types. The
aggregate emits one result row. Each other shape emits one million.

The table lists selected instruction-pointer samples from one profile
per function. Most profiles also include fixture construction,
allocation and output work. The named operation counts and matching
plan and source paths identify work to test. They do not divide the
elapsed time among those operations.

| Shape | ANSI | Total samples | Evidence |
| --- | --- | ---: | --- |
| SHIFTRIGHTUNSIGNED | On | 1,008 | 19 decimal-to-integer casts, 19 unsigned 128-bit division helper samples |
| >>> | Off | 1,021 | 15 decimal-to-integer casts, 23 unsigned 128-bit division helper samples |
| PMOD | On | 923 | 62 fallible Arrow binary kernel samples |
| DECIMAL division and ROUND | On | 1,339 | 178 decimal precision checks, 83 fused divide samples |
| TRY_ADD | Off | 859 | 13 Spark per-row TRY kernel samples |
| TRY_MULTIPLY | Off | 867 | 17 Spark per-row TRY kernel samples |
| TRY_SUBTRACT | Off | 837 | 12 Spark per-row TRY kernel samples |
| / | On | 841 | Five Arrow binary kernel samples. Divisor guard visible in the plan |
| HEX | Off | 914 | 51 Spark integer hex encoder samples |
| + | On | 832 | Four Arrow checked binary kernel samples |
| - | Off | 842 | Three Arrow binary kernel samples |
| * | Off | 825 | Two Arrow binary kernel samples |
| BITMAP_BUCKET_NUMBER | On | 920 | One CASE evaluator and one integer operator sample. Same arithmetic plans |
| TRY_SUM | Off | 794 | 11 Spark accumulator update samples |
| NEGATIVE | On | 803 | One Spark negation invocation sample |

## Unsigned right shift

`SHIFTRIGHTUNSIGNED` and `>>>` share the Sail
[bitwise resolver](../../vendor/sail/crates/sail-plan/src/function/scalar/bitwise.rs).
For a negative BIGINT it computes an absolute value and subtracts it
from the unsigned maximum. The physical CASE uses DECIMAL intermediates,
casts to UInt64 before shifting, and casts the result to Int64. The
native references use a signed bit mask and put the high bit back.
A direct UInt64 view could avoid the decimal conversion while keeping
the same checks for the signed minimum and shift counts.

## Positive modulo

[SparkPmod](../../vendor/sail/crates/sail-function/src/scalar/math/spark_pmod.rs)
calls `datafusion-spark` 54.1.0's `function::math::modulus::spark_pmod`.
It takes a remainder, builds a sign mask and a conditional correction,
adds, then takes a second remainder. The native reference uses two
remainder passes without the sign selection. On positive integer
divisors the first remainder and one conditional addition suffice.
Zero and negative divisors need the existing behavior.

## Decimal division and rounding

The DECIMAL projection uses two input
[casts](../../vendor/sail/crates/sail-function/src/scalar/spark_decimal_cast.rs),
[FusedDecimalDivide](../../vendor/sail/crates/sail-plan/src/function/scalar/math.rs),
ROUND and a final numeric-to-decimal cast. The cast declarations
carry Spark result annotations. With ANSI on, `guard_cast_arithmetic`
adds a divisor-NULL branch. Common-subexpression extraction projects
the repeated divisor. The native plan uses Arrow
casts and the native divide operator. The fused kernel computes the
Spark precision, scale and half-up quotient in one loop. The physical
difference includes the conversion stages and the ANSI branch. It does
not isolate how much time each conversion contributes.

## Integer arithmetic, division and guards

The [arithmetic resolver](../../vendor/sail/crates/sail-plan/src/function/scalar/math.rs)
uses a native arithmetic operator for ANSI-off addition, subtraction
and multiplication. `guard_cast_arithmetic` adds a left-NULL CASE around the operator
and explicit widening cast. ANSI-on versions use the
[checked kernel](../../vendor/sail/crates/sail-function/src/scalar/math/spark_checked_arithmetic.rs),
which delegates checked arithmetic to Arrow and translates overflow.
Both modes carry a result annotation on the widening cast.

The three TRY shapes use the same checked arithmetic UDF with
NULL-on-overflow. Its [primitive builder loop](../../vendor/sail/crates/sail-function/src/scalar/math/utils/try_op.rs)
checks both validity masks before each operation and appends each
result or NULL. The native reference has no overflow-to-NULL work.
A shared change can target the per-row TRY path while preserving the
arithmetic width and each operator's overflow.

Plain floating division after integer widening retains an explicit
zero-divisor policy. ANSI off replaces zero with NULL before dividing.
ANSI on evaluates `spark_checked_divide` under a divisor-NULL CASE.
Its shared validity-aware zero check scans the divisor before invoking
Arrow division. The native plan contains just widening and division.
The fixture's divisor column is positive. New paths must still honor
zero and NULL behavior for general inputs.

## Integer HEX and negation

[HEX dispatch](../../vendor/sail/crates/sail-plan/src/function/scalar/math.rs)
uses the numeric `SparkHex` function from locked `datafusion-spark`
54.1.0. Its integer loop formats the significant hex digits into a
per-row byte buffer and appends the string. Native `upper(to_hex(i))`
uses a different integer formatting loop and uppercases its output.
Compare exact width, signed endpoints, NULL and zero before changing
the numeric dispatch. Other HEX input classes use their existing paths.

[Negation](../../vendor/sail/crates/sail-function/src/scalar/math/spark_negative.rs)
for signed integers delegates through a mode-specific UDF. The wrapper
reuses a pinned ANSI configuration. Its kernel selects checked or
wrapping negation. Native unary minus uses a physical operator. The
candidate must preserve the minimum-integer overflow or wrap in each
mode.

## TRY_SUM and bitmap buckets

[TRY_SUM](../../vendor/sail/crates/sail-plan/src/function/aggregate.rs)
uses the locked Spark accumulator's nullable running sum. Its integer
update checks each addition and retains a failed flag. Native SUM
reduces batches without those ordered overflow semantics. The test
fixture does not overflow. Any faster update must retain the first
overflow even if later values would bring the sum back into range.

[BITMAP_BUCKET_NUMBER](../../vendor/sail/crates/sail-plan/src/function/scalar/misc.rs)
and its native reference project the same signed integer CASE and
division. Both ANSI modes have the same physical arithmetic. The
plans provide no separate Spark operation to remove for this case.
An unchanged control in a matched comparison can check whether the
earlier difference reproduces.

## Reproduction

The [protocol](../arithmetic_1412/protocol.md) and
[manifest](../arithmetic_1412/manifest.json) were committed at
`eaf19b35cb05a0e6cff68d24b653e1f8073d1397` before sampling.
Staging was `052b9f8cec0204091d1e0645f8ffdfa1baf8ff00`.
The Linux release executable was built from `b906c65d0e8c3b5c38a2c40f8c13cfe31e575754`
with SHA256 `bc200c25c5984ce872d59fb934b87dd4e62f0b2b5e5f8cf64d2c296f30625339`.

The session ran 2026-11-21T09:38:45Z to
2026-11-21T09:44:49Z. It captured one
profile for each of the 15 qualified cases. Count instruction-pointer
samples with `perf script -G -F comm,pid,time,event,ip,sym,dso,period`.
The raw plans and profiles are under `costs-168-2026-11-20/1412`.
