/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.spark.table;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

/**
 * Lakehouse packaging / classpath spike. Connectors are default runtime dependencies of {@code
 * hop-engines-spark}; assumptions skip only if the classpath is incomplete.
 *
 * <p>Success criteria:
 *
 * <ol>
 *   <li>{@code Class.forName(DeltaSparkSessionExtension)} under the test / engine CL
 *   <li>Delta PATH write/read with extension + {@code DeltaCatalog}
 *   <li>Record whether PATH I/O works without {@code DeltaCatalog} (feeds tiered probe defaults)
 *   <li>Iceberg classpath + Hadoop-warehouse / path smoke when the Iceberg runtime is present
 * </ol>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SparkLakehouseConnectorTest {

  @TempDir Path tempDir;

  private SparkSession spark;

  @BeforeAll
  static void requireLakehouseProfileHint() {
    // Soft documentation for developers when the suite is skipped.
    if (!SparkLakeConnectorProbe.isDeltaPresent(SparkLakeConnectorProbe.class.getClassLoader())) {
      System.err.println(
          "SparkLakehouseConnectorTest: Delta not on classpath — "
              + "rebuild hop-engines-spark with default runtime deps.");
    }
  }

  @AfterEach
  void stopSpark() {
    if (spark != null) {
      try {
        spark.stop();
      } catch (Exception ignored) {
        // best effort
      }
      spark = null;
    }
    // Clear active/default session so the next test can rebuild with different conf.
    try {
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
    } catch (Exception ignored) {
      // Spark API availability may vary; ignore
    }
  }

  @Test
  @Order(1)
  void deltaExtensionIsLoadableWhenConnectorPresent() {
    assumeDelta();
    ClassLoader cl = SparkLakeConnectorProbe.class.getClassLoader();
    assertTrue(SparkLakeConnectorProbe.isClassPresent(SparkLakeFormats.DELTA_EXTENSION, cl));
    assertTrue(SparkLakeConnectorProbe.isClassPresent(SparkLakeFormats.DELTA_CATALOG, cl));
    assertDoesNotThrow(
        () -> SparkLakeConnectorProbe.verifyClasspath(Set.of(SparkLakeFormats.FORMAT_DELTA), cl));
  }

  /**
   * Primary spike gate: PATH round-trip with modern Delta 4.x session conf (extension +
   * DeltaCatalog).
   */
  @Test
  @Order(2)
  void deltaPathRoundTripWithExtensionAndDeltaCatalog() {
    assumeDelta();
    Path tablePath = tempDir.resolve("delta_with_catalog");
    spark =
        newSessionBuilder("hop-delta-with-catalog")
            .config(SparkLakeFormats.SPARK_CONF_EXTENSIONS, SparkLakeFormats.DELTA_EXTENSION)
            .config(SparkLakeFormats.SPARK_CONF_SPARK_CATALOG, SparkLakeFormats.DELTA_CATALOG)
            .getOrCreate();

    Dataset<Row> written = spark.range(0, 25).toDF("id");
    written
        .write()
        .format(SparkLakeFormats.FORMAT_DELTA)
        .mode("overwrite")
        .save(tablePath.toString());

    long count =
        spark.read().format(SparkLakeFormats.FORMAT_DELTA).load(tablePath.toString()).count();
    assertEquals(25L, count);
  }

  /**
   * Records whether pure PATH {@code format("delta")} works without registering DeltaCatalog.
   *
   * <p>Spike finding (Delta 4.3.1 / Spark 4.1.2): PATH write/read <strong>requires</strong>
   * DeltaCatalog — Delta fails with {@code
   * DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG} if only the extension is set. Tiered
   * probe (KD-15) must hard-fail missing DeltaCatalog for any Delta use, including PATH-only.
   */
  @Test
  @Order(3)
  void deltaPathIoWithoutDeltaCatalog_recordsBehavior() {
    assumeDelta();
    Path tablePath = tempDir.resolve("delta_no_catalog");

    // Session with extension only — no spark.sql.catalog.spark_catalog=DeltaCatalog
    spark =
        newSessionBuilder("hop-delta-no-catalog")
            .config(SparkLakeFormats.SPARK_CONF_EXTENSIONS, SparkLakeFormats.DELTA_EXTENSION)
            .getOrCreate();

    boolean pathIoSucceeded;
    String detail;
    try {
      spark
          .range(0, 10)
          .toDF("id")
          .write()
          .format(SparkLakeFormats.FORMAT_DELTA)
          .mode("overwrite")
          .save(tablePath.toString());
      long count =
          spark.read().format(SparkLakeFormats.FORMAT_DELTA).load(tablePath.toString()).count();
      pathIoSucceeded = count == 10L;
      detail = "PATH write/read succeeded without DeltaCatalog (count=" + count + ")";
    } catch (Exception e) {
      pathIoSucceeded = false;
      detail =
          "PATH write/read failed without DeltaCatalog: "
              + e.getClass().getName()
              + ": "
              + e.getMessage();
    }

    System.out.println("SPIKE_FINDING deltaPathWithoutDeltaCatalog: " + detail);
    assertFalse(
        pathIoSucceeded,
        "Unexpected: Delta PATH I/O succeeded without DeltaCatalog. "
            + "Update README spike findings and KD-15 defaults. Detail: "
            + detail);
    assertTrue(
        detail.contains("DELTA_CONFIGURE_SPARK_SESSION")
            || detail.toLowerCase().contains("deltacatalog")
            || detail.toLowerCase().contains("delta catalog")
            || detail.contains("DeltaCatalog"),
        "Expected Delta session/catalog configuration error, got: " + detail);
  }

  /**
   * Iceberg classpath + local I/O smoke using a Hadoop warehouse catalog (no Hive Metastore). Bare
   * {@code format("iceberg").save(path)} without catalog conf defaults toward HiveCatalog and fails
   * without HMS client jars — document that PATH-style local use still needs catalog/warehouse
   * conf.
   */
  @Test
  @Order(4)
  void icebergExtensionIsLoadableAndHadoopWarehouseSmoke() {
    assumeIceberg();
    ClassLoader cl = SparkLakeConnectorProbe.class.getClassLoader();
    assertDoesNotThrow(
        () -> SparkLakeConnectorProbe.verifyClasspath(Set.of(SparkLakeFormats.FORMAT_ICEBERG), cl));

    Path warehouse = tempDir.resolve("iceberg_warehouse");
    // Hadoop catalog warehouse must be a URI (file://…); bare paths can fall through to
    // HiveCatalog.
    String warehouseUri = warehouse.toUri().toString();
    spark =
        newSessionBuilder("hop-iceberg-hadoop")
            .config(SparkLakeFormats.SPARK_CONF_EXTENSIONS, SparkLakeFormats.ICEBERG_EXTENSIONS)
            .config("spark.sql.catalog.local", SparkLakeFormats.ICEBERG_CATALOG)
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouseUri)
            .getOrCreate();

    // Prefer writeTo for Iceberg TABLE-style create (KD-24 may still refine vs saveAsTable).
    spark.range(0, 15).toDF("id").writeTo("local.db.sample").using("iceberg").createOrReplace();

    long countViaTable = spark.table("local.db.sample").count();
    assertEquals(15L, countViaTable);

    // Physical table dir under Hadoop warehouse (metadata written on disk).
    Path tableDir = warehouse.resolve("db").resolve("sample");
    assertTrue(tableDir.toFile().isDirectory(), "Expected Iceberg table directory at " + tableDir);

    // Note: bare spark.read.format("iceberg").load(path) still routes through catalog
    // resolution and can default to HiveCatalog without HMS jars. Prefer catalog
    // identifiers (spark.table / TABLE mode) for v1; path-mode Iceberg needs explicit
    // SparkSessionCatalog/Hadoop conf design in PR 3/5.
    System.out.println(
        "SPIKE_NOTE iceberg: writeTo+createOrReplace + spark.table OK with Hadoop warehouse "
            + "(warehouse URI="
            + warehouseUri
            + "). saveAsTable not compared — KD-24 / PR 5. "
            + "Bare format().save/load(path) without catalog conf defaults toward HiveCatalog.");
  }

  @Test
  @Order(5)
  void bothFormatsProbeWhenPresent() {
    assumeDelta();
    assumeIceberg();
    assertDoesNotThrow(
        () ->
            SparkLakeConnectorProbe.verifyClasspath(
                List.of(SparkLakeFormats.FORMAT_DELTA, SparkLakeFormats.FORMAT_ICEBERG)));
  }

  private static void assumeDelta() {
    assumeTrue(
        SparkLakeConnectorProbe.isDeltaPresent(SparkLakeConnectorProbe.class.getClassLoader()),
        "Delta connector not on classpath; connectors missing from test classpath");
  }

  private static void assumeIceberg() {
    assumeTrue(
        SparkLakeConnectorProbe.isIcebergPresent(SparkLakeConnectorProbe.class.getClassLoader()),
        "Iceberg connector not on classpath; connectors missing from test classpath");
  }

  private static SparkSession.Builder newSessionBuilder(String appName) {
    return SparkSession.builder()
        .appName(appName)
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.metrics.staticSources.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.session.timeZone", "UTC");
  }
}
