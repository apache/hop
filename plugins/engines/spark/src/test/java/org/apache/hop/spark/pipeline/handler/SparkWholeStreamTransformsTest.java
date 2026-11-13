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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.analyticquery.AnalyticQueryMeta;
import org.apache.hop.pipeline.transforms.analyticquery.GroupField;
import org.apache.hop.pipeline.transforms.analyticquery.QueryField;
import org.apache.hop.pipeline.transforms.detectlastrow.DetectLastRowMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.fieldschangesequence.FieldsChangeSequenceField;
import org.apache.hop.pipeline.transforms.fieldschangesequence.FieldsChangeSequenceMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorField;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.apache.hop.pipeline.transforms.reservoirsampling.ReservoirSamplingMeta;
import org.apache.hop.pipeline.transforms.samplerows.SampleRowsMeta;
import org.apache.hop.spark.core.HopSparkRowConverter;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.apache.hop.spark.pipeline.ISparkPipelineTransformHandler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The transforms that number, sample or compare the rows of the whole stream give the local
 * engine's rows, in the local engine's order, on 4 partitions.
 */
class SparkWholeStreamTransformsTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
    spark =
        SparkSession.builder()
            .appName("hop-spark-whole-stream-test")
            .master("local[4]")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.driver.host", "localhost")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  private static IRowMeta inputRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("n"));
    rowMeta.addValueMeta(new ValueMetaString("grp"));
    return rowMeta;
  }

  /** 103 rows; the group changes every 7 rows, so groups cross the partition boundaries. */
  private static List<Object[]> inputRows() {
    List<Object[]> rows = new ArrayList<>();
    for (long n = 1; n <= 103; n++) {
      rows.add(new Object[] {n, "g" + (n / 7)});
    }
    return rows;
  }

  private static PipelineMeta pipeline(ITransformMeta meta, String pluginId) {
    InjectorMeta injector = new InjectorMeta();
    injector.getInjectorFields().add(new InjectorField("n", "Integer", "-1", "-1"));
    injector.getInjectorFields().add(new InjectorField("grp", "String", "-1", "-1"));
    TransformMeta src = new TransformMeta("src", injector);
    src.setTransformPluginId("Injector");
    TransformMeta transform = new TransformMeta("transform", meta);
    transform.setTransformPluginId(pluginId);
    TransformMeta out = new TransformMeta("out", new DummyMeta());
    out.setTransformPluginId("Dummy");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(src);
    pipelineMeta.addTransform(transform);
    pipelineMeta.addTransform(out);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(src, transform));
    pipelineMeta.addPipelineHop(new PipelineHopMeta(transform, out));
    return pipelineMeta;
  }

  private static List<String> onTheLocalEngine(PipelineMeta pipelineMeta) throws Exception {
    LocalPipelineEngine pipeline = new LocalPipelineEngine(pipelineMeta, new Variables(), null);
    pipeline.setMetadataProvider(new MemoryMetadataProvider());
    pipeline.prepareExecution();
    List<Object[]> written = new ArrayList<>();
    pipeline
        .getTransform("transform", 0)
        .addRowListener(
            new RowAdapter() {
              @Override
              public void rowWrittenEvent(IRowMeta rowMeta, Object[] row)
                  throws HopTransformException {
                written.add(row.clone());
              }
            });
    RowProducer producer = pipeline.addRowProducer("src", 0);
    pipeline.startThreads();
    for (Object[] row : inputRows()) {
      producer.putRow(inputRowMeta(), row.clone());
    }
    producer.finished();
    pipeline.waitUntilFinished();
    assertEquals(0, pipeline.getErrors(), "errors on the local engine");
    HopSparkRowConverter.RowCodec codec =
        HopSparkRowConverter.RowCodec.of(
            pipelineMeta.getTransformFields(new Variables(), "transform"));
    List<String> result = new ArrayList<>();
    for (Object[] row : written) {
      result.add(codec.toSpark(row).toString());
    }
    return result;
  }

  private static List<String> onSpark(
      PipelineMeta pipelineMeta, ISparkPipelineTransformHandler handler) throws Exception {
    IRowMeta rowMeta = inputRowMeta();
    HopSparkRowConverter.RowCodec codec = HopSparkRowConverter.RowCodec.of(rowMeta);
    List<Row> sparkRows = new ArrayList<>();
    for (Object[] row : inputRows()) {
      sparkRows.add(codec.toSpark(row));
    }
    Dataset<Row> input =
        SparkGenericTransformHandler.spreadInOrder(
            spark.createDataFrame(sparkRows, HopSparkRowConverter.toStructType(rowMeta)), 4);
    Map<String, Dataset<Row>> map = new HashMap<>();
    handler.handleTransform(
        LogChannel.GENERAL,
        new Variables(),
        "spark",
        new SparkPipelineRunConfiguration(),
        new MemoryMetadataProvider(),
        "{}",
        pipelineMeta,
        pipelineMeta.findTransform("transform"),
        map,
        spark,
        rowMeta,
        List.of(pipelineMeta.findTransform("src")),
        input);
    List<String> result = new ArrayList<>();
    for (Row row : map.get("transform").collectAsList()) {
      result.add(row.toString());
    }
    return result;
  }

  private static void assertSameRowsInOrder(
      ITransformMeta meta, String pluginId, ISparkPipelineTransformHandler handler)
      throws Exception {
    List<String> local = onTheLocalEngine(pipeline(meta, pluginId));
    List<String> onSpark = onSpark(pipeline(meta, pluginId), handler);
    assertFalse(local.isEmpty());
    assertEquals(local, onSpark);
  }

  @Test
  void sampleRowsCountsTheLinesOfAllPartitions() throws Exception {
    SampleRowsMeta meta = new SampleRowsMeta();
    meta.setDefault();
    meta.setLinesRange("1..3,7,26..28,52,100..200");
    meta.setLineNumberField("line");
    assertSameRowsInOrder(meta, "SampleRows", new SparkSampleRowsHandler());

    SampleRowsMeta withoutLineNumber = new SampleRowsMeta();
    withoutLineNumber.setDefault();
    withoutLineNumber.setLinesRange("5,60..61");
    withoutLineNumber.setLineNumberField("");
    assertSameRowsInOrder(withoutLineNumber, "SampleRows", new SparkSampleRowsHandler());
  }

  @Test
  void onlyTheLastRowOfAllPartitionsIsTheLastRow() throws Exception {
    DetectLastRowMeta meta = new DetectLastRowMeta();
    meta.setDefault();
    meta.setResultFieldName("last");
    assertSameRowsInOrder(meta, "DetectLastRow", new SparkDetectLastRowHandler());
  }

  @Test
  void analyticQueryLooksAcrossThePartitionBoundaries() throws Exception {
    AnalyticQueryMeta meta = new AnalyticQueryMeta();
    meta.setDefault();
    meta.setGroupFields(new ArrayList<>(List.of(new GroupField("grp"))));
    meta.setQueryFields(
        new ArrayList<>(
            List.of(
                new QueryField("previous", "n", QueryField.AggregateType.LAG, 1),
                new QueryField("next", "n", QueryField.AggregateType.LEAD, 2))));
    assertSameRowsInOrder(meta, "AnalyticQuery", new SparkGenericTransformHandler());
  }

  @Test
  void theSequenceOfChangingFieldsContinuesOverThePartitions() throws Exception {
    FieldsChangeSequenceMeta meta = new FieldsChangeSequenceMeta();
    meta.setDefault();
    meta.setFields(new ArrayList<>(List.of(new FieldsChangeSequenceField("grp"))));
    meta.setResultFieldName("seq");
    meta.setStart("1");
    meta.setIncrement("1");
    assertSameRowsInOrder(meta, "FieldsChangeSequence", new SparkGenericTransformHandler());
  }

  @Test
  void reservoirSamplingSamplesAllRows() throws Exception {
    ReservoirSamplingMeta meta = new ReservoirSamplingMeta();
    meta.setDefault();
    meta.setSampleSize("10");
    meta.setSeed("7");
    assertSameRowsInOrder(meta, "ReservoirSampling", new SparkGenericTransformHandler());
  }

  @Test
  void transformsThatAddToATableRunOnOneStream() throws Exception {
    assertTrue(
        SparkGenericTransformHandler.needsOneOrderedStream(withXml("CombinationLookup", "")));
    assertTrue(
        SparkGenericTransformHandler.needsOneOrderedStream(
            withXml("DimensionLookup", "<update>Y</update>")));
    assertFalse(
        SparkGenericTransformHandler.needsOneOrderedStream(
            withXml("DimensionLookup", "<update>N</update>")),
        "a lookup only reads");
    assertFalse(SparkGenericTransformHandler.needsOneOrderedStream(withXml("Calculator", "")));
  }

  private static TransformMeta withXml(String pluginId, String xml) {
    TransformMeta transformMeta =
        new TransformMeta(
            pluginId,
            new DummyMeta() {
              @Override
              public String getXml() {
                return xml;
              }
            });
    transformMeta.setTransformPluginId(pluginId);
    return transformMeta;
  }
}
