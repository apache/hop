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

package org.apache.hop.spark.core;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Condition;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.ValueMetaAndData;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.filterrows.FilterRowsMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorField;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.apache.hop.spark.pipeline.handler.SparkGenericTransformHandler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The Catalyst row path gives exactly the values of the external Row path. */
class SparkInternalRowsTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
    spark =
        SparkSession.builder()
            .appName("hop-spark-internal-rows-test")
            .master("local[2]")
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

  private static IRowMeta allTypes() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("text"));
    rowMeta.addValueMeta(new ValueMetaNumber("number"));
    rowMeta.addValueMeta(new ValueMetaBigNumber("big"));
    rowMeta.addValueMeta(new ValueMetaBoolean("flag"));
    rowMeta.addValueMeta(new ValueMetaDate("day"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("moment"));
    rowMeta.addValueMeta(new ValueMetaBinary("bytes"));
    return rowMeta;
  }

  private static List<Object[]> edgeRows() {
    Timestamp withNanos = new Timestamp(1_700_000_000_123L);
    withNanos.setNanos(123_456_789);
    List<Object[]> rows = new ArrayList<>();
    rows.add(
        new Object[] {
          1L,
          "plain",
          1.5,
          new BigDecimal("12345.678900000000000000"),
          true,
          new Date(1_700_000_000_000L),
          new Timestamp(1_700_000_000_000L),
          "abc".getBytes(StandardCharsets.UTF_8)
        });
    rows.add(new Object[] {2L, null, null, null, null, null, null, null});
    rows.add(
        new Object[] {
          3L,
          "unicodé ✓ 日本 😀",
          -0.0,
          new BigDecimal("-1.5"),
          false,
          // 1850: before 1900, where the fast path stops
          new Date(-3_786_825_600_000L),
          withNanos,
          new byte[0]
        });
    rows.add(
        new Object[] {
          4L,
          "",
          Double.MAX_VALUE,
          BigDecimal.ZERO,
          true,
          // 1500: before the Gregorian cutover, where Spark rebases
          new Date(-14_831_769_600_000L),
          new Timestamp(-1L),
          new byte[] {(byte) 0xff, 0}
        });
    return rows;
  }

  private static Dataset<Row> dataset(IRowMeta rowMeta) throws Exception {
    HopSparkRowConverter.RowCodec codec = HopSparkRowConverter.RowCodec.of(rowMeta);
    List<Row> rows = new ArrayList<>();
    for (Object[] row : edgeRows()) {
      rows.add(codec.toSpark(row));
    }
    return spark.createDataFrame(rows, HopSparkRowConverter.toStructType(rowMeta)).repartition(2);
  }

  @Test
  void aCatalystRowGivesTheHopRowOfItsExternalRow() throws Exception {
    IRowMeta rowMeta = allTypes();
    Dataset<Row> dataset = dataset(rowMeta);
    HopSparkRowConverter.RowCodec rowCodec = HopSparkRowConverter.RowCodec.of(rowMeta);
    SparkInternalRows.Codec internalCodec = SparkInternalRows.Codec.of(rowMeta, dataset.schema());

    Map<Long, Object[]> viaRows = new HashMap<>();
    for (Row row : dataset.collectAsList()) {
      Object[] hop = rowCodec.toHop(row);
      viaRows.put((Long) hop[0], hop);
    }
    Map<Long, Object[]> viaInternal = new HashMap<>();
    for (InternalRow row :
        SparkInternalRows.toRdd(dataset).toJavaRDD().map(InternalRow::copy).collect()) {
      Object[] hop = internalCodec.toHop(row);
      viaInternal.put((Long) hop[0], hop);
    }
    assertEquals(viaRows.keySet(), viaInternal.keySet());
    for (Long id : viaRows.keySet()) {
      assertValuesEqual(rowMeta, viaRows.get(id), viaInternal.get(id));
    }
  }

  @Test
  void aGenericTransformWritesTheSameRowsOnBothPaths() throws Exception {
    IRowMeta rowMeta = allTypes();
    List<Row> internal = runFilter(rowMeta, false);
    List<Row> external = runFilter(rowMeta, true);
    assertEquals(edgeRows().size(), internal.size());
    assertEquals(sorted(external), sorted(internal));
  }

  private static List<Row> runFilter(IRowMeta rowMeta, boolean externalRows) throws Exception {
    InjectorMeta injector = new InjectorMeta();
    for (IValueMeta v : rowMeta.getValueMetaList()) {
      injector.getInjectorFields().add(new InjectorField(v.getName(), v.getTypeDesc(), "-1", "-1"));
    }
    TransformMeta src = new TransformMeta("src", injector);
    src.setTransformPluginId("Injector");
    FilterRowsMeta filterMeta = new FilterRowsMeta();
    filterMeta.setCondition(
        new Condition(
            "id",
            Condition.Function.LARGER,
            null,
            new ValueMetaAndData(new ValueMetaInteger("constant"), 0L)));
    TransformMeta filter = new TransformMeta("filter", filterMeta);
    filter.setTransformPluginId("FilterRows");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(src);
    pipelineMeta.addTransform(filter);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(src, filter));

    SparkPipelineRunConfiguration runConfiguration = new SparkPipelineRunConfiguration();
    runConfiguration.setInternalRowConversion(!externalRows);
    Map<String, Dataset<Row>> map = new HashMap<>();
    new SparkGenericTransformHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "spark",
            runConfiguration,
            new MemoryMetadataProvider(),
            "{}",
            pipelineMeta,
            filter,
            map,
            spark,
            rowMeta,
            List.of(src),
            dataset(rowMeta));
    return map.get("filter").collectAsList();
  }

  private static List<String> sorted(List<Row> rows) {
    List<String> out = new ArrayList<>();
    for (Row row : rows) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < row.length(); i++) {
        Object v = row.get(i);
        sb.append(v instanceof byte[] b ? java.util.Arrays.toString(b) : String.valueOf(v))
            .append('|');
      }
      out.add(sb.toString());
    }
    out.sort(String::compareTo);
    return out;
  }

  private static void assertValuesEqual(IRowMeta rowMeta, Object[] expected, Object[] actual) {
    for (int i = 0; i < rowMeta.size(); i++) {
      Object e = expected[i];
      Object a = actual[i];
      String field = rowMeta.getValueMeta(i).getName() + " of row " + expected[0];
      if (e instanceof byte[] eb) {
        assertArrayEquals(eb, (byte[]) a, field);
      } else if (e instanceof Date ed) {
        assertTrue(a instanceof Date, field);
        assertEquals(ed.getTime(), ((Date) a).getTime(), field);
        assertEquals(e.getClass(), a.getClass(), field);
      } else {
        assertEquals(e, a, field);
      }
    }
  }
}
