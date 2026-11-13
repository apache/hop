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
package org.apache.hop.pipeline.transforms.cubeoutput;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.compress.gzip.GzipCompressionLevel;
import org.apache.hop.core.exception.HopEofException;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
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
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * The compression level Serialize to file writes with. The rows repeat enough for every level to
 * give another file, so the bytes of a file tell which level wrote it.
 */
class CubeOutputCompressionTest {

  private static final int NR_ROWS = 20_000;

  @TempDir Path tempDir;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    for (ITransformMeta meta : List.of(new CubeOutputMeta(), new InjectorMeta())) {
      if (registry.getPluginId(TransformPluginType.class, meta) == null) {
        registry.registerPluginClass(
            meta.getClass().getName(), TransformPluginType.class, Transform.class);
      }
    }
  }

  /** A pipeline saved before the level could be chosen writes the same bytes as it always did. */
  @Test
  void transformSavedWithoutALevelWritesAtLevelSix() throws Exception {
    CubeOutputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/serialize-transform.xml", CubeOutputMeta.class);

    byte[] file = write(meta, "saved.cube");

    assertEquals(GzipCompressionLevel.STANDARD, meta.getCompressionLevel());
    assertArrayEquals(gzip(gunzip(file), 6), file);
    assertRows(file);
  }

  /** A null level is what code that builds the transform itself leaves behind. */
  @Test
  void transformWithoutALevelWritesAtLevelSix() throws Exception {
    CubeOutputMeta meta = new CubeOutputMeta();
    meta.setCompressionLevel(null);

    byte[] file = write(meta, "null.cube");

    assertArrayEquals(gzip(gunzip(file), 6), file);
  }

  @Test
  void newTransformWritesAtLevelSix() throws Exception {
    CubeOutputMeta meta = new CubeOutputMeta();
    meta.setDefault();

    byte[] file = write(meta, "new.cube");

    assertEquals(GzipCompressionLevel.STANDARD, meta.getCompressionLevel());
    assertArrayEquals(gzip(gunzip(file), 6), file);
  }

  @Test
  void theLevelIsSavedAndLoaded() throws Exception {
    CubeOutputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/serialize-with-compression-level.xml", CubeOutputMeta.class);

    assertEquals(GzipCompressionLevel.FASTEST, meta.getCompressionLevel());
    assertTrue(meta.getXml().contains("<compression_level>FASTEST</compression_level>"));
  }

  /** Every level writes the bytes of that level, and a file that holds the same rows. */
  @ParameterizedTest
  @EnumSource(GzipCompressionLevel.class)
  void fileIsWrittenAtTheChosenLevel(GzipCompressionLevel level) throws Exception {
    CubeOutputMeta meta = new CubeOutputMeta();
    meta.setDefault();
    meta.setCompressionLevel(level);

    byte[] file = write(meta, level + ".cube");

    assertArrayEquals(gzip(gunzip(file), level.getLevel()), file);
    assertRows(file);
  }

  @Test
  void lowerLevelGivesALargerFileWithTheSameContent() throws Exception {
    CubeOutputMeta fastest = new CubeOutputMeta();
    fastest.setCompressionLevel(GzipCompressionLevel.FASTEST);
    CubeOutputMeta standard = new CubeOutputMeta();
    standard.setCompressionLevel(GzipCompressionLevel.STANDARD);
    CubeOutputMeta smallest = new CubeOutputMeta();
    smallest.setCompressionLevel(GzipCompressionLevel.SMALLEST);

    byte[] fastestFile = write(fastest, "fastest.cube");
    byte[] standardFile = write(standard, "standard.cube");
    byte[] smallestFile = write(smallest, "smallest.cube");

    assertTrue(fastestFile.length > standardFile.length);
    assertTrue(standardFile.length > smallestFile.length);
    assertArrayEquals(gunzip(standardFile), gunzip(fastestFile));
    assertArrayEquals(gunzip(standardFile), gunzip(smallestFile));
  }

  private byte[] write(CubeOutputMeta meta, String name) throws Exception {
    Path file = tempDir.resolve(name);
    meta.setFilename(file.toString());
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("write cube");
    TransformMeta input = addTransform(pipelineMeta, "input", new InjectorMeta());
    TransformMeta output = addTransform(pipelineMeta, "cube output", meta);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(input, output));

    LocalPipelineEngine pipeline =
        new LocalPipelineEngine(pipelineMeta, new Variables(), new LoggingObject("test"));
    pipeline.prepareExecution();
    RowProducer producer = pipeline.addRowProducer("input", 0);
    pipeline.startThreads();
    IRowMeta layout = layout();
    for (Object[] row : rows()) {
      producer.putRow(layout, row);
    }
    producer.finished();
    pipeline.waitUntilFinished();
    assertEquals(0, pipeline.getErrors());
    return Files.readAllBytes(file);
  }

  /** The file holds the layout and every row, whatever the level. */
  private static void assertRows(byte[] file) throws Exception {
    List<Object[]> read = new ArrayList<>();
    try (DataInputStream dis =
        new DataInputStream(new GZIPInputStream(new ByteArrayInputStream(file)))) {
      IRowMeta readLayout = new RowMeta(dis);
      assertEquals(List.of(layout().getFieldNames()), List.of(readLayout.getFieldNames()));
      while (true) {
        try {
          read.add(readLayout.readData(dis));
        } catch (HopEofException eof) {
          break;
        }
      }
    }
    List<Object[]> expected = rows();
    assertEquals(expected.size(), read.size());
    for (int i = 0; i < expected.size(); i++) {
      assertTrue(
          Arrays.deepEquals(expected.get(i), read.get(i)),
          "row " + i + ": " + Arrays.deepToString(read.get(i)));
    }
  }

  private static byte[] gunzip(byte[] compressed) throws Exception {
    try (InputStream in = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
      return in.readAllBytes();
    }
  }

  /** What java.util.zip gives for this content at a level. */
  private static byte[] gzip(byte[] content, int level) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (GZIPOutputStream out =
        new GZIPOutputStream(bytes) {
          {
            def.setLevel(level);
          }
        }) {
      out.write(content);
    }
    return bytes.toByteArray();
  }

  private static IRowMeta layout() {
    IRowMeta layout = new RowMeta();
    layout.addValueMeta(new ValueMetaString("name"));
    layout.addValueMeta(new ValueMetaInteger("id"));
    layout.addValueMeta(new ValueMetaNumber("price"));
    layout.addValueMeta(new ValueMetaDate("ordered"));
    return layout;
  }

  private static List<Object[]> rows() {
    List<Object[]> rows = new ArrayList<>();
    for (long i = 0; i < NR_ROWS; i++) {
      rows.add(
          new Object[] {
            "customer " + (i % 97) + " in city " + (i * 31 % 113),
            i,
            (i % 1000) * 1.25,
            new Date(1_700_000_000_000L + i * 86_400_000L)
          });
    }
    return rows;
  }

  private static TransformMeta addTransform(
      PipelineMeta pipelineMeta, String name, ITransformMeta meta) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    TransformMeta transformMeta = new TransformMeta(pluginId, name, meta);
    pipelineMeta.addTransform(transformMeta);
    return transformMeta;
  }
}
