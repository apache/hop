/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.spark.pipeline.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Issue #8138: writing with {@code file_format=jdbc} must not hand Spark a path.
 *
 * <p>{@code DataFrameWriter.save(String)} records its argument as the {@code path} option, and the
 * JDBC provider passes every option it does not recognise to the driver as a connection property.
 * Teradata validates connection properties and rejects {@code path}; Postgres, MySQL and H2 all
 * ignore properties they do not know, which is why the bug is invisible against them.
 *
 * <p>Rather than depend on a strict driver being available, these tests assert the thing that is
 * actually wrong: what Spark hands the driver. {@link CapturingJdbcDriver} records the {@link
 * Properties} of every connection and delegates to H2, so the absence of {@code path} is checked
 * directly instead of inferred from whether some driver happened to complain.
 */
class SparkFileIoJdbcWriteTest {

  private static SparkSession spark;
  private static final String H2_URL = "jdbc:h2:mem:hop8138;DB_CLOSE_DELAY=-1";
  private static final String URL = CapturingJdbcDriver.PREFIX + H2_URL;

  @BeforeAll
  static void start() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
    spark =
        SparkSession.builder()
            .appName("hop-spark-jdbc-write-test")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.metrics.staticSources.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.host", "localhost")
            .getOrCreate();
    DriverManager.registerDriver(new CapturingJdbcDriver());
    // Keep the in-memory database alive for the duration of the test class.
    DriverManager.getConnection(H2_URL).close();
  }

  @AfterAll
  static void stop() {
    if (spark != null) {
      spark.stop();
    }
  }

  private Dataset<Row> sampleRows() {
    StructType schema =
        DataTypes.createStructType(
            new org.apache.spark.sql.types.StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("name", DataTypes.StringType, true)
            });
    return spark.createDataFrame(
        java.util.List.of(RowFactory.create(1, "alpha"), RowFactory.create(2, "beta")), schema);
  }

  private Map<String, String> jdbcOptions(String table) {
    Map<String, String> options = new LinkedHashMap<>();
    options.put("url", URL);
    options.put("dbtable", table);
    options.put("driver", CapturingJdbcDriver.class.getName());
    return options;
  }

  /**
   * The reported defect: the meaningless file path the transform forces the user to invent must not
   * reach the driver as a connection property. Fails before the fix with {@code path} present.
   */
  @Test
  void jdbcWriteDoesNotSendThePathAsAConnectionProperty() throws Exception {
    CapturingJdbcDriver.CAPTURED.clear();
    SparkFileIoSupport.writeDataset(
        sampleRows(),
        "jdbc",
        "/tmp/hop-bug-repro/category_sales_summary",
        SaveMode.Overwrite,
        jdbcOptions("SALES_SUMMARY"),
        null,
        null);

    assertFalse(
        CapturingJdbcDriver.CAPTURED.isEmpty(), "the driver was never asked for a connection");
    for (Properties p : CapturingJdbcDriver.CAPTURED) {
      assertFalse(
          p.containsKey("path"),
          "Spark sent 'path' as a connection property: " + p.stringPropertyNames());
    }

    try (Connection c = DriverManager.getConnection(H2_URL);
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery("select count(*) from SALES_SUMMARY")) {
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
    }
  }

  /** The same write with no path at all must behave identically — nothing depends on the path. */
  @Test
  void jdbcWriteWorksWithNoPathConfiguredAtAll() throws Exception {
    SparkFileIoSupport.writeDataset(
        sampleRows(), "jdbc", "", SaveMode.Overwrite, jdbcOptions("NO_PATH"), null, null);

    try (Connection c = DriverManager.getConnection(H2_URL);
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery("select count(*) from NO_PATH")) {
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
    }
  }

  /** Path-based formats keep receiving their path; the change must not reach them. */
  @Test
  void fileFormatsAreStillPathBased() {
    assertTrue(SparkFileIoSupport.isPathless("jdbc"));
    assertTrue(SparkFileIoSupport.isPathless("JDBC"));
    assertTrue(SparkFileIoSupport.isPathless("  jdbc  "));
    assertFalse(SparkFileIoSupport.isPathless("csv"));
    assertFalse(SparkFileIoSupport.isPathless("parquet"));
    assertFalse(SparkFileIoSupport.isPathless("orc"));
    assertFalse(SparkFileIoSupport.isPathless("json"));
    assertFalse(SparkFileIoSupport.isPathless("text"));
    assertFalse(SparkFileIoSupport.isPathless("delta"));
    assertFalse(SparkFileIoSupport.isPathless(null));
  }
}
