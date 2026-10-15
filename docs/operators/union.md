# Union

**Status:** native, with one gap.

`UNION ALL` substitutes to a native `Union` operator that simply concatenates its inputs' Arrow
batches — no state, no key. It admits any row type the columnar boundary can carry.

Append-only and updating branches can share a union. Each input retains its physical RowKind
and Java string-cache sidecars. Before a downstream keyed operator coalesces these batches,
native code aligns logical columns by position and sidecars by identity: missing RowKind tags
become INSERT, while missing string caches remain absent values. UPDATE_BEFORE, UPDATE_AFTER
and DELETE tags are preserved. Logical arity/type mismatches fail before Arrow concatenation.
No row conversion is added inside the island.

## Fallback

- **An inconvertible row type** — a column (or nested leaf) outside the Arrow boundary's supported
  types falls the operator back, dragging the whole query with it per the
  [all-or-nothing island rule](index.md#the-all-or-nothing-island).

## Not a fallback: `UNION` (distinct)

Plain `UNION` isn't planned as this operator at all — Flink's own optimizer rewrites duplicate
elimination into a `GROUP BY` before the plan reaches us, so a distinct union accelerates through
the [GROUP BY](group-by.md) path instead. It never reaches the `Union` matcher, so there's nothing
here for it to decline.
