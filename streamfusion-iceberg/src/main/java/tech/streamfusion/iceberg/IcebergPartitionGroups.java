package tech.streamfusion.iceberg;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.sink.PartitionKeySelector;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.StructLikeMap;

/**
 * Memoizes host routing for immutable integer partition tuples, including bucket/time transforms.
 */
final class IcebergPartitionGroups implements KeySelector<RowData, Integer> {
  private static final int MAX_ENTRIES = 1024;
  private final PartitionKey key;
  private final int maxParallelism;
  private final KeySelector<RowData, Integer> delegate;
  private final transient RowDataWrapper rows;
  private final transient StructLikeMap<Integer> groups;

  private IcebergPartitionGroups(
      PartitionKey key,
      Schema schema,
      RowType type,
      PartitionSpec spec,
      int maxParallelism,
      KeySelector<RowData, Integer> delegate) {
    this.key = key.copy();
    this.maxParallelism = maxParallelism;
    this.delegate = delegate;
    rows = new RowDataWrapper(type, schema.asStruct());
    groups = StructLikeMap.create(spec.partitionType());
  }

  static KeySelector<RowData, Integer> create(KeySelector<RowData, ?> stock, int maxParallelism) {
    return create(stock, maxParallelism, null);
  }

  static KeySelector<RowData, Integer> memoize(
      PartitionKeySelector partition, KeySelector<RowData, Integer> delegate) {
    return create(partition, 0, delegate);
  }

  private static KeySelector<RowData, Integer> create(
      KeySelector<RowData, ?> stock, int maxParallelism, KeySelector<RowData, Integer> delegate) {
    if (stock.getClass() == PartitionKeySelector.class) {
      try {
        var key =
            (PartitionKey)
                IcebergSinkAdapter.field(PartitionKeySelector.class, "partitionKey").get(stock);
        var spec = (PartitionSpec) IcebergSinkAdapter.field(PartitionKey.class, "spec").get(key);
        if (delegate != null && !boundedBuckets(spec)) return delegate;
        if (spec.isPartitioned()
            && spec.partitionType().fields().stream()
                .allMatch(field -> field.type().typeId() == Type.TypeID.INTEGER)) {
          var schema =
              (Schema) IcebergSinkAdapter.field(PartitionKeySelector.class, "schema").get(stock);
          var type =
              (RowType)
                  IcebergSinkAdapter.field(PartitionKeySelector.class, "flinkSchema").get(stock);
          return new IcebergPartitionGroups(key, schema, type, spec, maxParallelism, delegate);
        }
      } catch (IllegalAccessException failure) {
        throw new IllegalStateException(
            "Cannot inspect released Iceberg partition selector", failure);
      }
    }
    return delegate != null
        ? delegate
        : row -> KeyGroupRangeAssignment.assignToKeyGroup(stock.getKey(row), maxParallelism);
  }

  private static boolean boundedBuckets(PartitionSpec spec) {
    if (spec.isUnpartitioned()) return false;
    long cardinality = 1;
    for (var field : spec.fields()) {
      // Bucket is package-private; its persisted transform syntax exposes the bucket count.
      String transform = field.transform().toString();
      if (!transform.startsWith("bucket[") || !transform.endsWith("]")) return false;
      int buckets;
      try {
        buckets = Integer.parseInt(transform.substring(7, transform.length() - 1));
      } catch (NumberFormatException unsupported) {
        return false;
      }
      long values = (long) buckets + 1; // Include the null partition for each bucket field.
      if (buckets < 1 || cardinality > MAX_ENTRIES / values) return false;
      cardinality *= values;
    }
    return true;
  }

  @Override
  public Integer getKey(RowData row) throws Exception {
    key.partition(rows.wrap(row));
    Integer group = groups.get(key);
    if (group == null) {
      group =
          delegate == null
              ? KeyGroupRangeAssignment.assignToKeyGroup(key.toPath(), maxParallelism)
              : delegate.getKey(row);
      if (groups.size() == MAX_ENTRIES) groups.clear();
      groups.put(key.copy(), group);
    }
    return group;
  }
}
