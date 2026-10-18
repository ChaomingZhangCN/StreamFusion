package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.PartitionKeySelector;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class IcebergDynamicRoutingTest {
  @SuppressWarnings("unchecked")
  static KeySelector<RowData, Integer> selector(
      String table, Schema schema, PartitionSpec spec, DistributionMode mode, Set<String> equality)
      throws Exception {
    Class<?> type = Class.forName("org.apache.iceberg.flink.sink.dynamic.HashKeyGenerator");
    var constructor = type.getDeclaredConstructor(int.class, int.class);
    constructor.setAccessible(true);
    var method =
        type.getDeclaredMethod(
            "getKeySelector",
            String.class,
            Schema.class,
            PartitionSpec.class,
            DistributionMode.class,
            Set.class,
            int.class);
    method.setAccessible(true);
    return (KeySelector<RowData, Integer>)
        method.invoke(constructor.newInstance(1, 128), table, schema, spec, mode, equality, 4);
  }

  @Test
  void preservesReleasedWriterKeysAcrossNullsNegativeValuesAndEviction() throws Exception {
    var schema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    for (boolean bucket : new boolean[] {false, true}) {
      var spec =
          bucket
              ? PartitionSpec.builderFor(schema).bucket("id", 4).build()
              : PartitionSpec.builderFor(schema).identity("id").build();
      for (String table : new String[] {"bench.a", "bench.b"}) {
        var stock = selector(table, schema, spec, DistributionMode.HASH, Set.of("id"));
        var cached = IcebergDynamicRouting.memoize(stock);
        if (bucket) assertNotSame(stock, cached);
        else assertSame(stock, cached);
        for (int i = 0; i < 6000; i++) {
          RowData row = GenericRowData.of(i % 17 == 0 ? null : i % 4000 - 2000);
          assertEquals(stock.getKey(row), cached.getKey(row));
        }
      }
    }
  }

  @Test
  void callsOriginalSelectorOnMissAndCopiesBorrowedPartitionTuples() throws Exception {
    var schema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    var spec = PartitionSpec.builderFor(schema).bucket("id", 4).build();
    var stock = selector("bench.a", schema, spec, DistributionMode.HASH, Set.of());
    var calls = new AtomicInteger();
    var cached =
        IcebergPartitionGroups.memoize(
            new PartitionKeySelector(spec, schema, FlinkSchemaUtil.convert(schema)),
            row -> {
              calls.incrementAndGet();
              return stock.getKey(row);
            });
    var row = new GenericRowData(1);
    for (int i = 0; i < 10000; i++) {
      row.setField(0, i % 17 == 0 ? null : i);
      assertEquals(stock.getKey(row), cached.getKey(row));
    }
    assertTrue(calls.get() > 0 && calls.get() <= 5);
  }

  @Test
  void boundsCompoundBucketsIncludingNullTuplesWithoutOverflow() throws Exception {
    var schema =
        new Schema(
            Types.NestedField.optional(1, "a", Types.IntegerType.get()),
            Types.NestedField.optional(2, "b", Types.IntegerType.get()));
    for (int buckets : new int[] {16, 32, Integer.MAX_VALUE}) {
      var spec = PartitionSpec.builderFor(schema).bucket("a", 32).bucket("b", buckets).build();
      var stock = selector("bench.t", schema, spec, DistributionMode.HASH, Set.of());
      var cached = IcebergDynamicRouting.memoize(stock);
      if (buckets == 16) assertNotSame(stock, cached);
      else assertSame(stock, cached);
      for (int i = 0; i < 2000; i++) {
        var row = GenericRowData.of(i % 2 == 0 ? null : i, i % 3 == 0 ? null : -i);
        assertEquals(stock.getKey(row), cached.getKey(row));
      }
    }
  }

  @Test
  void retainsOriginalSelectorsOutsideVerifiedIntegerPartitions() throws Exception {
    var schema = new Schema(Types.NestedField.optional(1, "id", Types.StringType.get()));
    var stringSpec = PartitionSpec.builderFor(schema).identity("id").build();
    var partition = selector("bench.t", schema, stringSpec, DistributionMode.HASH, Set.of());
    var largeBucket =
        selector(
            "bench.t",
            schema,
            PartitionSpec.builderFor(schema).bucket("id", 16384).build(),
            DistributionMode.HASH,
            Set.of());
    assertSame(largeBucket, IcebergDynamicRouting.memoize(largeBucket));
    assertSame(partition, IcebergDynamicRouting.memoize(partition));
    var equality =
        selector(
            "bench.t", schema, PartitionSpec.unpartitioned(), DistributionMode.NONE, Set.of("id"));
    assertSame(equality, IcebergDynamicRouting.memoize(equality));
    var roundRobin =
        selector("bench.t", schema, PartitionSpec.unpartitioned(), DistributionMode.NONE, Set.of());
    assertSame(roundRobin, IcebergDynamicRouting.memoize(roundRobin));
    assertNotNull(partition.getKey(GenericRowData.of(StringData.fromString("é"))));
  }
}
