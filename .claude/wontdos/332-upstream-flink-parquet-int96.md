# Preserve released Flink INT96 timestamp behavior

[StreamFusion #332](https://github.com/datafusion-contrib/StreamFusion/issues/332) stays closed.
Do not add a StreamFusion-specific correction for Flink's near-epoch INT96 arithmetic.

Released Flink 1.18.1 and 2.2.1 can encode invalid local nanoseconds of day or silently change
UTC -1ms to +999ms. The actual StreamFusion native encoder matches released Flink 2.2.1:
24 configurations and 240 values produced identical timestamp bytes and reader outcomes,
including 24 complete stock-reader file comparisons. This is inherited host behavior.

Preserve host parity, the portable modern-date fixtures, and the explicit timestamp regressions.
This closure does not claim that upstream Flink has fixed the defect. Reassess after a verified
canonical release changes the host contract, or if a distinct native-only mismatch is demonstrated.

See [the compatibility documentation](../../docs/flink-compatibility.md) for the validation matrix.
