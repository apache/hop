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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.spark.core.SparkTransformMetricSlice;
import org.apache.hop.spark.core.SparkTransformMetricsAccumulator;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.apache.hop.spark.transforms.io.SparkField;
import org.apache.hop.spark.transforms.io.SparkFileInputMeta;
import org.apache.hop.spark.transforms.io.SparkFileOutputMeta;
import org.apache.hop.spark.util.SparkConst;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SparkFileIoHandlersTest {

  private static SparkSession spark;

  @TempDir Path tempDir;

  @BeforeAll
  static void start() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
    spark =
        SparkSession.builder()
            .appName("hop-spark-file-io-test")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.metrics.staticSources.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.host", "localhost")
            .getOrCreate();
  }

  @AfterAll
  static void stop() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void csvRoundTripWithExplicitSchema() throws Exception {
    Path inputFile = tempDir.resolve("people.csv");
    Files.writeString(inputFile, "name,age\nAlice,30\nBob,25\nCarol,40\n", StandardCharsets.UTF_8);

    SparkFileInputMeta inMeta = new SparkFileInputMeta();
    inMeta.setFilePath(inputFile.toString());
    inMeta.setFileFormat(SparkFileInputMeta.FORMAT_CSV);
    inMeta.setHeader(true);
    inMeta.setSeparator(",");
    inMeta.getFields().add(new SparkField("name", "String"));
    inMeta.getFields().add(new SparkField("age", "Integer"));

    TransformMeta inTm = new TransformMeta("read", inMeta);
    inTm.setTransformPluginId(SparkConst.SPARK_FILE_INPUT_PLUGIN_ID);
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(inTm);

    SparkTransformMetricsAccumulator metrics = new SparkTransformMetricsAccumulator();
    spark.sparkContext().register(metrics, "file-io-metrics");

    Map<String, Dataset<Row>> map = new HashMap<>();
    SparkFileInputHandler inputHandler = new SparkFileInputHandler();
    inputHandler.setMetricsAccumulator(metrics);
    inputHandler.handleTransform(
        LogChannel.GENERAL,
        new Variables(),
        "spark",
        new SparkPipelineRunConfiguration(),
        new MemoryMetadataProvider(),
        "{}",
        pipelineMeta,
        inTm,
        map,
        spark,
        new RowMeta(),
        List.of(),
        null);

    Dataset<Row> read = map.get("read");
    assertEquals(3, read.count());
    assertEquals(2, read.columns().length);

    long inputRows =
        metrics.value().values().stream()
            .filter(s -> "read".equals(s.getTransformName()))
            .mapToLong(SparkTransformMetricSlice::getLinesInput)
            .sum();
    assertEquals(3L, inputRows, "Spark File Input should report linesInput via native metrics");

    Path outDir = tempDir.resolve("out-csv");
    SparkFileOutputMeta outMeta = new SparkFileOutputMeta();
    outMeta.setFilePath(outDir.toString());
    outMeta.setFileFormat(SparkFileInputMeta.FORMAT_CSV);
    outMeta.setSaveMode(SparkFileOutputMeta.MODE_OVERWRITE);
    outMeta.setHeader(true);
    outMeta.setCoalescePartitions("1");

    TransformMeta outTm = new TransformMeta("write", outMeta);
    outTm.setTransformPluginId(SparkConst.SPARK_FILE_OUTPUT_PLUGIN_ID);
    pipelineMeta.addTransform(outTm);

    SparkFileOutputHandler outputHandler = new SparkFileOutputHandler();
    outputHandler.setMetricsAccumulator(metrics);
    outputHandler.handleTransform(
        LogChannel.GENERAL,
        new Variables(),
        "spark",
        new SparkPipelineRunConfiguration(),
        new MemoryMetadataProvider(),
        "{}",
        pipelineMeta,
        outTm,
        map,
        spark,
        new RowMeta(),
        List.of(inTm),
        read);

    // Leaf is empty marker; data is on disk
    assertEquals(0, map.get("write").count());
    assertTrue(Files.exists(outDir));

    long outputRows =
        metrics.value().values().stream()
            .filter(s -> "write".equals(s.getTransformName()))
            .mapToLong(SparkTransformMetricSlice::getLinesOutput)
            .sum();
    assertEquals(3L, outputRows, "Spark File Output should report linesOutput via native metrics");

    // Re-read written CSV
    long rewritten =
        spark
            .read()
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(outDir.toString())
            .count();
    assertEquals(3, rewritten);
  }

  @Test
  void parquetRoundTrip() throws Exception {
    Path parquetDir = tempDir.resolve("data.parquet");
    spark.range(0, 10).toDF("id").write().mode("overwrite").parquet(parquetDir.toString());

    SparkFileInputMeta inMeta = new SparkFileInputMeta();
    inMeta.setFilePath(parquetDir.toString());
    inMeta.setFileFormat(SparkFileInputMeta.FORMAT_PARQUET);

    TransformMeta inTm = new TransformMeta("pq_in", inMeta);
    inTm.setTransformPluginId(SparkConst.SPARK_FILE_INPUT_PLUGIN_ID);
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(inTm);

    Map<String, Dataset<Row>> map = new HashMap<>();
    new SparkFileInputHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "spark",
            new SparkPipelineRunConfiguration(),
            new MemoryMetadataProvider(),
            "{}",
            pipelineMeta,
            inTm,
            map,
            spark,
            new RowMeta(),
            List.of(),
            null);

    assertEquals(10, map.get("pq_in").count());

    Path outParquet = tempDir.resolve("out.parquet");
    SparkFileOutputMeta outMeta = new SparkFileOutputMeta();
    outMeta.setFilePath(outParquet.toString());
    outMeta.setFileFormat(SparkFileInputMeta.FORMAT_PARQUET);
    outMeta.setSaveMode(SparkFileOutputMeta.MODE_OVERWRITE);

    TransformMeta outTm = new TransformMeta("pq_out", outMeta);
    outTm.setTransformPluginId(SparkConst.SPARK_FILE_OUTPUT_PLUGIN_ID);
    pipelineMeta.addTransform(outTm);

    new SparkFileOutputHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "spark",
            new SparkPipelineRunConfiguration(),
            new MemoryMetadataProvider(),
            "{}",
            pipelineMeta,
            outTm,
            map,
            spark,
            new RowMeta(),
            List.of(inTm),
            map.get("pq_in"));

    assertEquals(10, spark.read().parquet(outParquet.toString()).count());
  }

  /**
   * Reproduces customers-1k style issues: semicolon separator, leading spaces on integers, date as
   * yyyy/MM/dd, and a field list that omits a middle column (firstname) — values must not shift.
   */
  @Test
  void csvNameBasedProjectionDoesNotShiftWhenFieldOmitted() throws Exception {
    Path inputFile = tempDir.resolve("customers-sample.txt");
    Files.writeString(
        inputFile,
        "id;name;firstname;zip;city;birthdate;street;housenr;stateCode;state\n"
            + " 1;jwcdf-name;fsj-firstname; 13520;oem-city;1954/02/07;amrb-street; 145;AK;ALASKA\n"
            + " 2;flhxu-name;tum-firstname; 17520;buo-city;1966/04/24;wfyz-street; 96;GA;GEORGIA\n",
        StandardCharsets.UTF_8);

    SparkFileInputMeta inMeta = new SparkFileInputMeta();
    inMeta.setFilePath(inputFile.toString());
    inMeta.setFileFormat(SparkFileInputMeta.FORMAT_CSV);
    inMeta.setHeader(true);
    inMeta.setSeparator(";");
    // Intentionally omit firstname — must drop that column, not shift the rest
    inMeta.getFields().add(new SparkField("id", "Integer"));
    inMeta.getFields().add(new SparkField("name", "String"));
    inMeta.getFields().add(new SparkField("zip", "String"));
    inMeta.getFields().add(new SparkField("city", "String"));
    SparkField birth = new SparkField("birthdate", "Date");
    birth.setFormatMask("yyyy/MM/dd");
    inMeta.getFields().add(birth);
    inMeta.getFields().add(new SparkField("street", "String"));
    inMeta.getFields().add(new SparkField("housenr", "String"));
    inMeta.getFields().add(new SparkField("stateCode", "String"));
    inMeta.getFields().add(new SparkField("state", "String"));

    TransformMeta inTm = new TransformMeta("customers", inMeta);
    inTm.setTransformPluginId(SparkConst.SPARK_FILE_INPUT_PLUGIN_ID);
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(inTm);

    Map<String, Dataset<Row>> map = new HashMap<>();
    new SparkFileInputHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "spark",
            new SparkPipelineRunConfiguration(),
            new MemoryMetadataProvider(),
            "{}",
            pipelineMeta,
            inTm,
            map,
            spark,
            new RowMeta(),
            List.of(),
            null);

    Dataset<Row> ds = map.get("customers");
    assertEquals(2, ds.count());
    String[] cols = ds.columns();
    assertEquals(9, cols.length);
    assertEquals("id", cols[0]);
    assertEquals("name", cols[1]);
    assertEquals("zip", cols[2]);
    assertEquals("city", cols[3]);
    assertEquals("birthdate", cols[4]);
    assertEquals("state", cols[8]);

    Row first = ds.orderBy("id").collectAsList().get(0);
    assertEquals(1L, first.getLong(0));
    assertEquals("jwcdf-name", first.getString(1));
    assertEquals("13520", first.getString(2)); // zip, not firstname
    assertEquals("oem-city", first.getString(3));
    assertTrue(first.get(4) != null, "birthdate should parse");
    assertEquals("amrb-street", first.getString(5));
    assertEquals("145", first.getString(6));
    assertEquals("AK", first.getString(7));
    assertEquals("ALASKA", first.getString(8));
  }

  @Test
  void csvInferSchemaWithoutFields() throws Exception {
    Path inputFile = tempDir.resolve("nums.csv");
    Files.writeString(inputFile, "a,b\n1,2\n3,4\n", StandardCharsets.UTF_8);

    SparkFileInputMeta inMeta = new SparkFileInputMeta();
    inMeta.setFilePath(inputFile.toString());
    inMeta.setFileFormat(SparkFileInputMeta.FORMAT_CSV);
    inMeta.setHeader(true);
    inMeta.setInferSchema(true);

    TransformMeta inTm = new TransformMeta("infer", inMeta);
    inTm.setTransformPluginId(SparkConst.SPARK_FILE_INPUT_PLUGIN_ID);
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(inTm);

    Map<String, Dataset<Row>> map = new HashMap<>();
    new SparkFileInputHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "spark",
            new SparkPipelineRunConfiguration(),
            new MemoryMetadataProvider(),
            "{}",
            pipelineMeta,
            inTm,
            map,
            spark,
            new RowMeta(),
            List.of(),
            null);

    Dataset<Row> ds = map.get("infer");
    assertEquals(2, ds.count());
    assertEquals(2, ds.columns().length);
  }
}
