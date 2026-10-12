# Decimal division without temporary big integers

DECIMAL division retains Flink's `BigDecimal.divide(MathContext(38, HALF_UP))`
followed by HALF_UP rescaling to the resolved result type. Rounding directly to
the final scale would change boundary cases where the first step creates a tie.

When the unscaled divisor fits an unsigned 64-bit integer and the dividend is a
valid Decimal128 value, the kernel builds the 38-digit quotient by long division
in chunks of at most 18 digits. The divisor bound keeps each scaled remainder
inside u128, and the quotient fits i128 even if rounding carries into `10^38`.
Input scales adjust the quotient's decimal position without multiplying either
operand by a large power of ten. Final precision/scale enforcement retains the
second HALF_UP step and overflow-to-NULL. Wider divisors use the original exact
BigInt implementation; modulo is unchanged.

This removes heap-allocated powers, division intermediates and decimal strings
used only to count digits. Sampling the shared COUNT/AVG/SUM immediate pipeline
identified these costs in the outer Calc. The accumulator itself was already
batched; enabling aggregation without addressing the outer per-row division
made the initial native DECIMAL job slower than stock.

The compact path is checked against the original big-integer implementation over
signs, extreme input scales, NULL/zero/precision boundaries and 64-bit divisor
limits. Released-Flink SQL regressions also cover the context/rescale boundaries.
Criterion's `calc_expressions` suite calls the production Calc pipeline at 256
and 4,096 rows with nullable decimals and both compact and wide divisors.
Complete-job results and all samples are on the
[basic type coverage page](../benchmarks/basic-type-coverage.md).

With release/system-allocator builds and real Arrow IPC shuffle, 2,000,000 rows
and five measured repetitions, shared DECIMAL immediate aggregation takes
**0.756 s versus stock Flink 1.213 s** at 64 groups, and **0.758 s versus
1.324 s** at 1,024 groups. The initial compact-path-free prototype took 3.390 s
against its own 1.211 s stock control even with cheaper same-JVM shuffles. These
are whole-job comparisons, not claims about wider-divisor kernel throughput.
