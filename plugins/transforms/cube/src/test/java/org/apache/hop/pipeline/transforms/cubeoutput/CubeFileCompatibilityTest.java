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

package org.apache.hop.pipeline.transforms.cubeoutput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopEofException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.cubeinput.CubeInputMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Cube files written without a buffer between the data stream and the GZIP stream (as before) and
 * files written by Serialize to file read in both directions, with the same layout and rows. 20,000
 * rows is more than one buffer.
 */
class CubeFileCompatibilityTest {

  private static final int NR_ROWS = 20_000;

  @TempDir Path tempDir;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    for (ITransformMeta meta :
        List.of(new CubeInputMeta(), new CubeOutputMeta(), new InjectorMeta(), new DummyMeta())) {
      if (registry.getPluginId(TransformPluginType.class, meta) == null) {
        registry.registerPluginClass(
            meta.getClass().getName(), TransformPluginType.class, Transform.class);
      }
    }
  }

  @Test
  void fileWrittenUnbufferedIsReadByDeSerializeFromFile() throws Exception {
    Path file = tempDir.resolve("old.cube");
    IRowMeta layout = layout();
    List<Object[]> rows = rows();
    try (OutputStream os = Files.newOutputStream(file);
        DataOutputStream dos = new DataOutputStream(new GZIPOutputStream(os))) {
      layout.writeMeta(dos);
      for (Object[] row : rows) {
        layout.writeData(dos, row);
      }
    }

    CubeInputMeta inputMeta = new CubeInputMeta();
    inputMeta.setDefault();
    inputMeta.getFile().setName(file.toString());
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("read cube");
    TransformMeta input = addTransform(pipelineMeta, "cube input", inputMeta);
    TransformMeta output = addTransform(pipelineMeta, "output", new DummyMeta());
    pipelineMeta.addPipelineHop(new PipelineHopMeta(input, output));

    LocalPipelineEngine pipeline =
        new LocalPipelineEngine(pipelineMeta, new Variables(), new LoggingObject("test"));
    pipeline.prepareExecution();
    List<Object[]> read = new ArrayList<>();
    List<IRowMeta> readLayouts = new ArrayList<>();
    pipeline
        .getTransform("cube input", 0)
        .addRowListener(
            new RowAdapter() {
              @Override
              public void rowWrittenEvent(IRowMeta rowMeta, Object[] row)
                  throws HopTransformException {
                if (readLayouts.isEmpty()) {
                  readLayouts.add(rowMeta);
                }
                read.add(Arrays.copyOf(row, rowMeta.size()));
              }
            });
    pipeline.startThreads();
    pipeline.waitUntilFinished();

    assertEquals(0, pipeline.getErrors());
    assertEquals(List.of(layout.getFieldNames()), List.of(readLayouts.get(0).getFieldNames()));
    assertRows(rows, read);
  }

  @Test
  void fileWrittenBySerializeToFileReadsUnbuffered() throws Exception {
    Path file = tempDir.resolve("new.cube");
    IRowMeta layout = layout();
    List<Object[]> rows = rows();

    CubeOutputMeta outputMeta = new CubeOutputMeta();
    outputMeta.setDefault();
    outputMeta.setFilename(file.toString());
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("write cube");
    TransformMeta input = addTransform(pipelineMeta, "input", new InjectorMeta());
    TransformMeta output = addTransform(pipelineMeta, "cube output", outputMeta);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(input, output));

    LocalPipelineEngine pipeline =
        new LocalPipelineEngine(pipelineMeta, new Variables(), new LoggingObject("test"));
    pipeline.setVariable(Const.HOP_METRIC_DATA_VOLUME, "Y");
    pipeline.prepareExecution();
    RowProducer producer = pipeline.addRowProducer("input", 0);
    pipeline.startThreads();
    for (Object[] row : rows) {
      producer.putRow(layout, row);
    }
    producer.finished();
    pipeline.waitUntilFinished();
    assertEquals(0, pipeline.getErrors());

    List<Object[]> read = new ArrayList<>();
    IRowMeta readLayout;
    try (InputStream is = Files.newInputStream(file);
        DataInputStream dis = new DataInputStream(new GZIPInputStream(is))) {
      readLayout = new RowMeta(dis);
      while (true) {
        try {
          read.add(readLayout.readData(dis));
        } catch (HopEofException eof) {
          break;
        }
      }
    }
    assertEquals(List.of(layout.getFieldNames()), List.of(readLayout.getFieldNames()));
    assertRows(rows, read);

    // The counter below the GZIP stream still sees every compressed byte.
    assertEquals(
        Files.size(file), (long) pipeline.getTransform("cube output", 0).getDataVolumeOut());
  }

  private static IRowMeta layout() {
    IRowMeta layout = new RowMeta();
    layout.addValueMeta(new ValueMetaString("name"));
    layout.addValueMeta(new ValueMetaInteger("id"));
    layout.addValueMeta(new ValueMetaNumber("price"));
    layout.addValueMeta(new ValueMetaBigNumber("amount"));
    layout.addValueMeta(new ValueMetaDate("ordered"));
    layout.addValueMeta(new ValueMetaBoolean("paid"));
    layout.addValueMeta(new ValueMetaBinary("payload"));
    return layout;
  }

  /** Every fifth row has only nulls apart from its id. */
  private static List<Object[]> rows() {
    List<Object[]> rows = new ArrayList<>();
    for (long i = 0; i < NR_ROWS; i++) {
      if (i % 5 == 0) {
        rows.add(new Object[] {null, i, null, null, null, null, null});
      } else {
        rows.add(
            new Object[] {
              "name-" + i,
              i,
              i * 1.25,
              BigDecimal.valueOf(i, 3),
              new Date(1_700_000_000_000L + i),
              i % 2 == 0,
              new byte[] {(byte) i, (byte) (i >> 8)}
            });
      }
    }
    return rows;
  }

  private static void assertRows(List<Object[]> expected, List<Object[]> actual) {
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      assertTrue(
          Arrays.deepEquals(expected.get(i), actual.get(i)),
          "row " + i + ": " + Arrays.deepToString(actual.get(i)));
    }
  }

  private static TransformMeta addTransform(
      PipelineMeta pipelineMeta, String name, ITransformMeta meta) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    TransformMeta transformMeta = new TransformMeta(pluginId, name, meta);
    pipelineMeta.addTransform(transformMeta);
    return transformMeta;
  }
}
