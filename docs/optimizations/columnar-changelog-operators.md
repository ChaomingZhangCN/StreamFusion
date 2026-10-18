# All native operators are columnar; changelog operators converted

**Applies to:** GROUP BY aggregate, updating join, Top-N

A row-fed native operator forces a transpose on every batch even inside an all-native chain, so
Arrow-in/Arrow-out is the standing rule and the row-fed GROUP BY / updating join / Top-N variants
were deleted once columnar ones existed.

The whole-query all-or-nothing gate then guarantees no interior row↔Arrow round-trip ever survives
planning: a query accelerates as one columnar island or runs as stock Flink.

## Stateless changelog filtering

DropUpdateBefore remains inside the Arrow chain. A borrowed row-kind scan forwards an unchanged
batch without spending a shared consumer's ownership, entering JNI or allocating Arrow buffers.
A fully discarded batch closes its owned input directly. Mixed batches use one Arrow mask across
all columns and sidecars, retaining stream timestamps and selecting row lineage consistently.

A row source feeding only this filter and a row sink stays on Flink by default. CPU profiling of
the initial implementation showed column conversion, field access and Arrow filtering/copy work
around a predicate Flink evaluates directly on RowData. Avoiding that whole round trip is cheaper
than optimizing the mask alone; the explicit standalone opt-in remains available for evaluation.

Six complete updating UNNEST jobs improve 1.08–1.70× over stock and 1.08–1.95× over the preceding
revision. The original blocked DATE-array workload moves from 2.196 s prior StreamFusion to
1.466 s native (paired stock 2.311 s). The initial lone-filter result loses, 0.713 s native versus
0.476 s stock, so that path is not the default. Full configuration, ranges and all 150 trials live
on the [operator page](../operators/drop-update-before.md#validation-and-measurement) and in its
linked raw artifact; the table includes both Arrow boundaries and all startup costs.
