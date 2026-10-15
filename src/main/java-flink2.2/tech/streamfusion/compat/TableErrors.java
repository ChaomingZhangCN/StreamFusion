package tech.streamfusion.compat;

/** Preserves the host's generated SINGLE_VALUE failure type across Flink lines. */
public final class TableErrors {
  private TableErrors() {}

  public static Class<? extends RuntimeException> exceptionType() {
    return org.apache.flink.table.api.TableRuntimeException.class;
  }

  public static void fail(String message) {
    throw new org.apache.flink.table.api.TableRuntimeException(message);
  }

  public static void jsonAggregate(String message) {
    throw new org.apache.flink.table.api.TableException(message);
  }

  public static void jsonNullKey(String message) {
    throw new org.apache.flink.table.api.TableRuntimeException(message);
  }

  public static void jsonSerialization(String message) {
    throw new org.apache.flink.table.api.TableException(message);
  }

  public static void jsonDuplicateNullKey(String message) throws Exception {
    throw new org.apache.flink.table.api.TableRuntimeException(message);
  }
}
