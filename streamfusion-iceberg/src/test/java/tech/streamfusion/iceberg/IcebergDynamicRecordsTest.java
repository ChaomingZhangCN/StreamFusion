package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.table.data.GenericRowData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class IcebergDynamicRecordsTest {
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("id", 4).build();

  private static IcebergDynamicBatch.Target target(
      String table,
      String branch,
      Schema schema,
      PartitionSpec spec,
      int key,
      boolean upsert,
      Set<Integer> equality) {
    return new IcebergDynamicBatch.Target(
        table,
        branch,
        SchemaParser.toJson(schema),
        PartitionSpecParser.toJson(spec),
        key,
        upsert,
        equality);
  }

  @Test
  void reusesOnlyImmutableMetadataAcrossFreshPayloadViews() {
    var cache = new IcebergDynamicRecords.Targets();
    var target = target("bench.t", "main", SCHEMA, SPEC, 3, true, Set.of(1));
    var first =
        cache.resolve(IcebergDynamicRecords.record(target, SCHEMA, SPEC, GenericRowData.of(1)));
    var second =
        cache.resolve(IcebergDynamicRecords.record(target, SCHEMA, SPEC, GenericRowData.of(2)));
    assertSame(first, second);
    assertEquals(target, second.target());
    assertEquals(org.apache.iceberg.flink.FlinkSchemaUtil.convert(SCHEMA), second.rowType());
  }

  @Test
  void observesEveryControlFieldAndSchemaSpecRefresh() {
    var schema = new Schema(Types.NestedField.required(7, "id", Types.IntegerType.get()));
    var spec = PartitionSpec.builderFor(SCHEMA).withSpecId(9).bucket("id", 4).build();
    var base = target("bench.t", "main", SCHEMA, SPEC, 3, true, Set.of(1));
    var variants =
        List.of(
            target("bench.other", "main", SCHEMA, SPEC, 3, true, Set.of(1)),
            target("bench.t", "audit", SCHEMA, SPEC, 3, true, Set.of(1)),
            target("bench.t", "main", schema, PartitionSpec.unpartitioned(), 3, true, Set.of(7)),
            target("bench.t", "main", SCHEMA, spec, 3, true, Set.of(1)),
            target("bench.t", "main", SCHEMA, SPEC, 4, true, Set.of(1)),
            target("bench.t", "main", SCHEMA, SPEC, 3, false, Set.of(1)),
            target("bench.t", "main", SCHEMA, SPEC, 3, true, Set.of()));
    var cache = new IcebergDynamicRecords.Targets();
    var first =
        cache.resolve(IcebergDynamicRecords.record(base, SCHEMA, SPEC, GenericRowData.of(1)));
    for (var variant : variants) {
      var currentSchema = variant.schema();
      var resolved =
          cache.resolve(
              IcebergDynamicRecords.record(
                  variant, currentSchema, variant.spec(currentSchema), GenericRowData.of(2)));
      assertEquals(variant, resolved.target());
      assertNotSame(first, resolved);
    }
    assertSame(
        first,
        cache.resolve(IcebergDynamicRecords.record(base, SCHEMA, SPEC, GenericRowData.of(3))));
  }

  @Test
  void copiesMutableEqualitySetsBeforeCachingAndBoundsRetention() throws Exception {
    var target = target("bench.t", "main", SCHEMA, SPEC, 3, true, Set.of(1));
    Object record = IcebergDynamicRecords.record(target, SCHEMA, SPEC, GenericRowData.of(1));
    var fields = new HashSet<>(Set.of(1));
    var setter = record.getClass().getDeclaredMethod("setEqualityFieldIds", Set.class);
    setter.setAccessible(true);
    setter.invoke(record, fields);
    var cache = new IcebergDynamicRecords.Targets(1);
    var first = cache.resolve(record);
    fields.clear();
    var second = cache.resolve(record);
    assertEquals(Set.of(1), first.target().equalityFields());
    assertEquals(Set.of(), second.target().equalityFields());
    setter.invoke(record, (Object) null);
    assertSame(second, cache.resolve(record));
    fields.add(1);
    setter.invoke(record, fields);
    var afterEviction = cache.resolve(record);
    assertEquals(first.target(), afterEviction.target());
    assertNotSame(first, afterEviction);
    assertSame(afterEviction, cache.resolve(record));
  }
}
