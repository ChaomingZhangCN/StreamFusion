package tech.streamfusion.iceberg;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.flink.FlinkSchemaUtil;

/** Exact released control-record contract; payloads are never serialized as Java rows. */
final class IcebergDynamicRecords {
  private static final MethodHandle CONSTRUCTOR;
  private static final MethodHandle TABLE, BRANCH, SCHEMA, SPEC, KEY, UPSERT, EQUALITY;

  static {
    try {
      var type = Class.forName("org.apache.iceberg.flink.sink.dynamic.DynamicRecordInternal");
      var lookup = MethodHandles.privateLookupIn(type, MethodHandles.lookup());
      var signature =
          MethodType.methodType(
              void.class,
              String.class,
              String.class,
              Schema.class,
              RowData.class,
              PartitionSpec.class,
              int.class,
              boolean.class,
              Set.class);
      CONSTRUCTOR =
          lookup.findConstructor(type, signature).asType(signature.changeReturnType(Object.class));
      TABLE = getter(lookup, type, "tableName", String.class);
      BRANCH = getter(lookup, type, "branch", String.class);
      SCHEMA = getter(lookup, type, "schema", Schema.class);
      SPEC = getter(lookup, type, "spec", PartitionSpec.class);
      KEY = getter(lookup, type, "writerKey", int.class);
      UPSERT = getter(lookup, type, "upsertMode", boolean.class);
      EQUALITY = getter(lookup, type, "equalityFields", Set.class);
    } catch (ReflectiveOperationException failure) {
      throw new ExceptionInInitializerError(failure);
    }
  }

  private static MethodHandle getter(
      MethodHandles.Lookup lookup, Class<?> type, String name, Class<?> result)
      throws ReflectiveOperationException {
    return lookup
        .findVirtual(type, name, MethodType.methodType(result))
        .asType(MethodType.methodType(result, Object.class));
  }

  record Resolved(IcebergDynamicBatch.Target target, RowType rowType) {}

  /** Derived immutable metadata only; never caches a host control record or its borrowed row. */
  static final class Targets {
    private final int capacity;
    private final Map<Control, Resolved> targets = new HashMap<>();
    private final Map<Schema, String> schemas = new IdentityHashMap<>();
    private final Map<PartitionSpec, String> specs = new IdentityHashMap<>();

    Targets() {
      this(1024);
    }

    Targets(int capacity) {
      if (capacity < 1)
        throw new IllegalArgumentException("Positive metadata cache capacity required");
      this.capacity = capacity;
    }

    @SuppressWarnings("unchecked")
    Resolved resolve(Object record) {
      try {
        var control =
            new Control(
                (String) TABLE.invokeExact(record),
                (String) BRANCH.invokeExact(record),
                (Schema) SCHEMA.invokeExact(record),
                (PartitionSpec) SPEC.invokeExact(record),
                (int) KEY.invokeExact(record),
                (boolean) UPSERT.invokeExact(record),
                (Set<Integer>) EQUALITY.invokeExact(record));
        var cached = targets.get(control);
        if (cached != null) return cached;
        if (targets.size() >= capacity) {
          targets.clear();
          schemas.clear();
          specs.clear();
        }
        var target =
            new IcebergDynamicBatch.Target(
                control.table(),
                control.branch(),
                schemas.computeIfAbsent(control.schema(), SchemaParser::toJson),
                specs.computeIfAbsent(control.spec(), PartitionSpecParser::toJson),
                control.writerKey(),
                control.upsert(),
                control.equalityFields());
        var resolved = new Resolved(target, FlinkSchemaUtil.convert(control.schema()));
        // Host equality sets may be mutable. Only the transient lookup borrows them.
        targets.put(
            new Control(
                control.table(),
                control.branch(),
                control.schema(),
                control.spec(),
                control.writerKey(),
                control.upsert(),
                target.equalityFields()),
            resolved);
        return resolved;
      } catch (Throwable failure) {
        throw new IllegalStateException("Cannot inspect released dynamic control record", failure);
      }
    }
  }

  private record Control(
      String table,
      String branch,
      Schema schema,
      PartitionSpec spec,
      int writerKey,
      boolean upsert,
      Set<Integer> equalityFields) {
    Control {
      if (equalityFields == null) equalityFields = Set.of();
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Control that
          && schema == that.schema
          && spec == that.spec
          && writerKey == that.writerKey
          && upsert == that.upsert
          && Objects.equals(table, that.table)
          && Objects.equals(branch, that.branch)
          && equalityFields.equals(that.equalityFields);
    }

    @Override
    public int hashCode() {
      int result = Objects.hashCode(table);
      result = 31 * result + Objects.hashCode(branch);
      result = 31 * result + System.identityHashCode(schema);
      result = 31 * result + System.identityHashCode(spec);
      result = 31 * result + writerKey;
      result = 31 * result + Boolean.hashCode(upsert);
      return 31 * result + equalityFields.hashCode();
    }
  }

  static Object record(
      IcebergDynamicBatch.Target target, Schema schema, PartitionSpec spec, RowData row) {
    try {
      return (Object)
          CONSTRUCTOR.invokeExact(
              target.table(),
              target.branch(),
              schema,
              row,
              spec,
              target.writerKey(),
              target.upsert(),
              target.equalityFields());
    } catch (Throwable failure) {
      throw new IllegalStateException("Cannot create released dynamic control record", failure);
    }
  }
}
