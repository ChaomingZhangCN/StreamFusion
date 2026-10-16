package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.planner.NativePlanner;

class NativeIcebergSqlTest {
  @TempDir Path temp;

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void updatingSqlKeepsJavaDeleteEncoding(boolean upsert) throws Exception {
    String previous = System.getProperty("streamfusion.experimental.iceberg.enabled");
    System.setProperty("streamfusion.experimental.iceberg.enabled", "true");
    try {
      var env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.configure(Configuration.fromMap(java.util.Map.of("restart-strategy.type", "none")));
      env.setParallelism(2);
      env.setMaxParallelism(16);
      env.enableCheckpointing(1000);
      var sql = StreamTableEnvironment.create(env);
      var scan = NativePlanner.install(sql);
      sql.executeSql(
          "CREATE CATALOG ice WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='"
              + temp.toUri()
              + "')");
      sql.executeSql("CREATE DATABASE ice.db");
      sql.executeSql(
          "CREATE TABLE ice.db.t (id INT NOT NULL, n BIGINT, PRIMARY KEY (id) NOT ENFORCED) "
              + "PARTITIONED BY (id) WITH ('format-version'='2', 'write.distribution-mode'='hash', "
              + "'write.upsert.enabled'='"
              + upsert
              + "')");
      sql.executeSql(
              "INSERT INTO ice.db.t SELECT id, COUNT(*) FROM (VALUES (1), (2), (1), (1), (2)) AS"
                  + " v(id) GROUP BY id")
          .await();
      try (var catalog =
          new HadoopCatalog(new org.apache.hadoop.conf.Configuration(), temp.toUri().toString())) {
        var table = catalog.loadTable(TableIdentifier.of("db", "t"));
        var values = new java.util.TreeMap<Integer, Long>();
        try (var rows = IcebergGenerics.read(table).build()) {
          for (var row : rows) {
            assertNull(values.put((Integer) row.getField("id"), (Long) row.getField("n")));
          }
        }
        assertEquals(java.util.Map.of(1, 3L, 2, 2L), values);
        assertNativeDataFiles(table, scan.fallbackReasons().toString());
      }
      assertEquals("", NativeIceberg.liveNativeHandles());
    } finally {
      if (previous == null) System.clearProperty("streamfusion.experimental.iceberg.enabled");
      else System.setProperty("streamfusion.experimental.iceberg.enabled", previous);
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"none", "hash"})
  void sqlRetainsTheHostCommitterAndUsesTheNativeDataAppender(String distribution)
      throws Exception {
    String previous = System.getProperty("streamfusion.experimental.iceberg.enabled");
    System.setProperty("streamfusion.experimental.iceberg.enabled", "true");
    try {
      var env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.configure(Configuration.fromMap(java.util.Map.of("restart-strategy.type", "none")));
      env.setParallelism(distribution.equals("hash") ? 2 : 1);
      env.setMaxParallelism(2);
      env.enableCheckpointing(1000);
      var sql = StreamTableEnvironment.create(env);
      var scan = NativePlanner.install(sql);
      sql.executeSql(
          "CREATE CATALOG ice WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='"
              + temp.toUri()
              + "')");
      sql.executeSql("CREATE DATABASE ice.db");
      sql.executeSql(
          "CREATE TABLE ice.db.t (id INT, payload STRING) "
              + (distribution.equals("hash") ? "PARTITIONED BY (payload) " : "")
              + "WITH ('write.distribution-mode'='"
              + distribution
              + "')");
      sql.executeSql("INSERT INTO ice.db.t VALUES (1, 'first'), (2, CAST(NULL AS STRING))").await();
      try (var catalog =
          new HadoopCatalog(new org.apache.hadoop.conf.Configuration(), temp.toUri().toString())) {
        var table = catalog.loadTable(TableIdentifier.of("db", "t"));
        var values = new java.util.TreeMap<Integer, Object>();
        try (var rows = IcebergGenerics.read(table).build()) {
          for (var row : rows) values.put((Integer) row.getField("id"), row.getField("payload"));
        }
        assertEquals(2, values.size());
        assertEquals("first", values.get(1));
        assertNull(values.get(2));
        assertNativeDataFiles(table, scan.fallbackReasons().toString());
      }
      assertEquals("", NativeIceberg.liveNativeHandles());
    } finally {
      if (previous == null) System.clearProperty("streamfusion.experimental.iceberg.enabled");
      else System.setProperty("streamfusion.experimental.iceberg.enabled", previous);
    }
  }

  private static void assertNativeDataFiles(org.apache.iceberg.Table table, String fallbacks)
      throws Exception {
    int files = 0;
    try (var tasks = table.newScan().planFiles()) {
      for (var task : tasks) {
        try (var parquet =
            ParquetFileReader.open(
                org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                    new org.apache.hadoop.fs.Path(task.file().location()),
                    new org.apache.hadoop.conf.Configuration()))) {
          assertTrue(
              parquet.getFooter().getFileMetaData().getCreatedBy().contains("parquet-rs"),
              () -> "Expected native data files; planner fallbacks: " + fallbacks);
        }
        files++;
      }
    }
    assertTrue(files > 0);
  }
}
