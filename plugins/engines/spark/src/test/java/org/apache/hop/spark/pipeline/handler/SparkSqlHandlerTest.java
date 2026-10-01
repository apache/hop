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

package org.apache.hop.spark.pipeline.handler;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.apache.hop.spark.transforms.io.SparkField;
import org.apache.hop.spark.transforms.sql.SparkSqlMeta;
import org.apache.hop.spark.transforms.sql.SparkSqlView;
import org.apache.hop.spark.util.SparkConst;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration-style unit tests for the native Spark SQL handler on a local Spark session. These
 * prove multi-input view registration, whole-query planning and the declared-schema contract.
 */
class SparkSqlHandlerTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
    spark =
        SparkSession.builder()
            .appName("hop-spark-sql-handler-test")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.metrics.staticSources.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.host", "localhost")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void joinsTwoInputsRegisteredAsTempViews() throws Exception {
    Dataset<Row> orders = ordersDataset();
    Dataset<Row> customers = customersDataset();

    SparkSqlMeta meta = new SparkSqlMeta();
    meta.setSql(
        "SELECT c.name AS name, SUM(o.amount) AS total "
            + "FROM orders o JOIN customers c ON o.customer_id = c.id "
            + "GROUP BY c.name");
    meta.setFields(List.of(new SparkField("name", "String"), new SparkField("total", "Integer")));

    Map<String, Dataset<Row>> map = new HashMap<>();
    map.put("orders", orders);
    map.put("customers", customers);

    Dataset<Row> output = run(meta, "sql", map, List.of("orders", "customers"), new Variables());

    assertArrayEquals(new String[] {"name", "total"}, output.columns());
    List<Row> rows = output.orderBy("name").collectAsList();
    assertEquals(2, rows.size());
    assertEquals("alice", rows.get(0).getString(0));
    assertEquals(30L, rows.get(0).getLong(1));
    assertEquals("bob", rows.get(1).getString(0));
    assertEquals(7L, rows.get(1).getLong(1));
  }

  @Test
  void unionsAcrossPartitionsUnderOnePlan() throws Exception {
    Dataset<Row> orders = ordersDataset().repartition(4);

    SparkSqlMeta meta = new SparkSqlMeta();
    meta.setSql(
        "SELECT COUNT(*) AS row_count FROM (SELECT * FROM orders UNION ALL SELECT * FROM orders)");
    meta.setFields(List.of(new SparkField("row_count", "Integer")));

    Map<String, Dataset<Row>> map = new HashMap<>();
    map.put("orders", orders);

    Dataset<Row> output = run(meta, "sql", map, List.of("orders"), new Variables());
    assertEquals(8L, output.collectAsList().get(0).getLong(0));
  }

  @Test
  void viewNameOverrideIsUsed() throws Exception {
    Dataset<Row> orders = ordersDataset();

    SparkSqlMeta meta = new SparkSqlMeta();
    meta.setSql("SELECT SUM(amount) AS total FROM raw_orders");
    meta.setViews(List.of(new SparkSqlView("Read orders (raw)", "raw_orders")));
    meta.setFields(List.of(new SparkField("total", "Integer")));

    Map<String, Dataset<Row>> map = new HashMap<>();
    map.put("Read orders (raw)", orders);

    Dataset<Row> output = run(meta, "sql", map, List.of("Read orders (raw)"), new Variables());
    assertEquals(37L, output.collectAsList().get(0).getLong(0));
  }

  @Test
  void transformNameWithoutOverrideIsSanitisedIntoAViewName() throws Exception {
    Dataset<Row> orders = ordersDataset();

    SparkSqlMeta meta = new SparkSqlMeta();
    // "Read orders (raw)" sanitises to "Read_orders__raw_"
    meta.setSql("SELECT SUM(amount) AS total FROM Read_orders__raw_");
    meta.setFields(List.of(new SparkField("total", "Integer")));

    Map<String, Dataset<Row>> map = new HashMap<>();
    map.put("Read orders (raw)", orders);

    Dataset<Row> output = run(meta, "sql", map, List.of("Read orders (raw)"), new Variables());
    assertEquals(37L, output.collectAsList().get(0).getLong(0));
  }

  @Test
  void zeroInputStatementIsAllowed() throws Exception {
    SparkSqlMeta meta = new SparkSqlMeta();
    meta.setSql("SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, label)");
    meta.setFields(List.of(new SparkField("id", "Integer"), new SparkField("label", "String")));

    Dataset<Row> output = run(meta, "sql", new HashMap<>(), List.of(), new Variables());

    assertArrayEquals(new String[] {"id", "label"}, output.columns());
    assertEquals(2, output.count());
  }

  @Test
  void variablesAreResolvedOnTheDriverBeforeExecution() throws Exception {
    Dataset<Row> orders = ordersDataset();

    IVariables variables = new Variables();
    variables.setVariable("MIN_AMOUNT", "10");

    SparkSqlMeta meta = new SparkSqlMeta();
    meta.setSql("SELECT COUNT(*) AS row_count FROM orders WHERE amount >= ${MIN_AMOUNT}");
    meta.setFields(List.of(new SparkField("row_count", "Integer")));

    Map<String, Dataset<Row>> map = new HashMap<>();
    map.put("orders", orders);

    Dataset<Row> output = run(meta, "sql", map, List.of("orders"), variables);
    assertEquals(2L, output.collectAsList().get(0).getLong(0));
  }

  @Test
  void declaredFieldsDriveOutputOrderAndType() throws Exception {
    Dataset<Row> orders = ordersDataset();

    SparkSqlMeta meta = new SparkSqlMeta();
    meta.setSql("SELECT customer_id, amount FROM orders");
    // Declared in the opposite order, and amount declared as String
    meta.setFields(
        List.of(new SparkField("amount", "String"), new SparkField("customer_id", "String")));

    Map<String, Dataset<Row>> map = new HashMap<>();
    map.put("orders", orders);

    Dataset<Row> output = run(meta, "sql", map, List.of("orders"), new Variables());

    assertArrayEquals(new String[] {"amount", "customer_id"}, output.columns());
    assertEquals(
        DataTypes.StringType, output.schema().apply("amount").dataType(), "declared cast applied");
    assertEquals(DataTypes.StringType, output.schema().apply("customer_id").dataType());
  }

  @Test
  void missingDeclaredFieldFailsWithTheColumnsThatWereReturned() {
    Dataset<Row> orders = ordersDataset();

    SparkSqlMeta meta = new SparkSqlMeta();
    meta.setSql("SELECT customer_id FROM orders");
    meta.setFields(List.of(new SparkField("nope", "String")));

    Map<String, Dataset<Row>> map = new HashMap<>();
    map.put("orders", orders);

    HopException e =
        assertThrows(
            HopException.class, () -> run(meta, "sql", map, List.of("orders"), new Variables()));
    assertTrue(e.getMessage().contains("nope"), e.getMessage());
    assertTrue(e.getMessage().contains("customer_id"), e.getMessage());
  }

  @Test
  void emptyFieldListIsRejected() {
    Dataset<Row> orders = ordersDataset();

    SparkSqlMeta meta = new SparkSqlMeta();
    meta.setSql("SELECT customer_id FROM orders");
    meta.setFields(new ArrayList<>());

    Map<String, Dataset<Row>> map = new HashMap<>();
    map.put("orders", orders);

    HopException e =
        assertThrows(
            HopException.class, () -> run(meta, "sql", map, List.of("orders"), new Variables()));
    assertTrue(e.getMessage().contains("output field list is required"), e.getMessage());
  }

  @Test
  void emptySqlIsRejected() {
    SparkSqlMeta meta = new SparkSqlMeta();
    meta.setSql("   ");
    meta.setFields(List.of(new SparkField("a", "String")));

    HopException e =
        assertThrows(
            HopException.class,
            () -> run(meta, "sql", new HashMap<>(), List.of(), new Variables()));
    assertTrue(e.getMessage().contains("SQL statement is empty"), e.getMessage());
  }

  @Test
  void collidingViewNamesAreReportedRatherThanShadowed() {
    SparkSqlMeta meta = new SparkSqlMeta();
    meta.setSql("SELECT 1 AS one");
    meta.setFields(List.of(new SparkField("one", "Integer")));

    Map<String, Dataset<Row>> map = new HashMap<>();
    // Both sanitise to "orders_a"
    map.put("orders a", ordersDataset());
    map.put("orders_a", ordersDataset());

    HopException e =
        assertThrows(
            HopException.class,
            () -> run(meta, "sql", map, List.of("orders a", "orders_a"), new Variables()));
    assertTrue(e.getMessage().contains("both map to view"), e.getMessage());
  }

  @Test
  void invalidSqlReportsTheRegisteredViews() {
    Dataset<Row> orders = ordersDataset();

    SparkSqlMeta meta = new SparkSqlMeta();
    meta.setSql("SELECT * FROM does_not_exist");
    meta.setFields(List.of(new SparkField("a", "String")));

    Map<String, Dataset<Row>> map = new HashMap<>();
    map.put("orders", orders);

    HopException e =
        assertThrows(
            HopException.class, () -> run(meta, "sql", map, List.of("orders"), new Variables()));
    assertTrue(e.getMessage().contains("Registered views"), e.getMessage());
    assertTrue(e.getMessage().contains("orders"), e.getMessage());
  }

  /** Builds the TransformMeta/PipelineMeta wiring and runs the handler, returning its Dataset. */
  private static Dataset<Row> run(
      SparkSqlMeta meta,
      String transformName,
      Map<String, Dataset<Row>> transformDatasetMap,
      List<String> previousTransformNames,
      IVariables variables)
      throws HopException {

    TransformMeta sqlTransform = new TransformMeta(transformName, meta);
    sqlTransform.setTransformPluginId(SparkConst.SPARK_SQL_PLUGIN_ID);

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(sqlTransform);

    List<TransformMeta> previousTransforms = new ArrayList<>();
    for (String name : previousTransformNames) {
      TransformMeta previous = new TransformMeta(name, meta);
      pipelineMeta.addTransform(previous);
      previousTransforms.add(previous);
    }

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("customer_id"));
    rowMeta.addValueMeta(new ValueMetaInteger("amount"));

    new SparkSqlHandler()
        .handleTransform(
            LogChannel.GENERAL,
            variables,
            "spark",
            new SparkPipelineRunConfiguration(),
            new MemoryMetadataProvider(),
            "{}",
            pipelineMeta,
            sqlTransform,
            transformDatasetMap,
            spark,
            rowMeta,
            previousTransforms,
            null);

    return transformDatasetMap.get(transformName);
  }

  private static Dataset<Row> ordersDataset() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("customer_id", DataTypes.StringType, false),
              DataTypes.createStructField("amount", DataTypes.LongType, false)
            });
    List<Row> rows =
        Arrays.asList(
            RowFactory.create("c1", 10L),
            RowFactory.create("c1", 20L),
            RowFactory.create("c2", 7L),
            RowFactory.create("c1", 0L));
    return spark.createDataFrame(rows, schema);
  }

  private static Dataset<Row> customersDataset() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.StringType, false),
              DataTypes.createStructField("name", DataTypes.StringType, false)
            });
    List<Row> rows =
        Arrays.asList(RowFactory.create("c1", "alice"), RowFactory.create("c2", "bob"));
    return spark.createDataFrame(rows, schema);
  }
}
