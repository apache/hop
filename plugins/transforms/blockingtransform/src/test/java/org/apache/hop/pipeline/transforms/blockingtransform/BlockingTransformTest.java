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

package org.apache.hop.pipeline.transforms.blockingtransform;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.compress.gzip.GzipCompressionLevel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class BlockingTransformTest {

  @TempDir Path tempDir;

  private TransformMockHelper<BlockingTransformMeta, BlockingTransformData> helper;

  private BlockingTransformData lastData;

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>(
            "Blocking transform", BlockingTransformMeta.class, BlockingTransformData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  @Test
  void passesAllRowsInOrderFromMemory() throws Exception {
    List<Object[]> output = run(true, 10_000, true, 2_500);

    assertRowsInOrder(2_500, output);
    assertNoTempFilesLeft();
  }

  @Test
  void passesAllRowsInOrderFromCompressedTempFiles() throws Exception {
    // 10 full caches and a partial one: the last 50 rows are written to a file of their own
    List<Object[]> output = run(true, 100, true, 1_050);

    assertRowsInOrder(1_050, output);
    assertNoTempFilesLeft();
  }

  @Test
  void passesAllRowsInOrderFromUncompressedTempFiles() throws Exception {
    List<Object[]> output = run(true, 100, false, 1_050);

    assertRowsInOrder(1_050, output);
    assertNoTempFilesLeft();
  }

  @Test
  void passesNothingWithoutInput() throws Exception {
    assertTrue(run(true, 100, true, 0).isEmpty());
  }

  @Test
  void passesOnlyTheLastRow() throws Exception {
    List<Object[]> output = run(false, 100, true, 1_050);

    assertEquals(1, output.size());
    assertArrayEquals(row(1_049), output.get(0));
    assertNoTempFilesLeft();
  }

  @ParameterizedTest
  @EnumSource(GzipCompressionLevel.class)
  void passesAllRowsInOrderAtEveryCompressionLevel(GzipCompressionLevel level) throws Exception {
    List<Object[]> output =
        run(true, 100, true, 1_050, meta -> meta.setCompressionLevel(level), transform -> {});

    assertRowsInOrder(1_050, output);
    assertNoTempFilesLeft();
  }

  @Test
  void aVariableSwitchesCompressionOn() throws Exception {
    List<Object[]> output =
        run(
            true,
            100,
            false,
            1_050,
            meta -> meta.setCompressFilesVariable("COMPRESS_SPOOL_FILES"),
            transform -> transform.setVariable("COMPRESS_SPOOL_FILES", "Y"));

    assertTrue(lastData.compressFiles);
    assertRowsInOrder(1_050, output);
    assertNoTempFilesLeft();
  }

  private List<Object[]> run(boolean passAllRows, int cacheSize, boolean compress, int rowCount)
      throws Exception {
    return run(passAllRows, cacheSize, compress, rowCount, meta -> {}, transform -> {});
  }

  private List<Object[]> run(
      boolean passAllRows,
      int cacheSize,
      boolean compress,
      int rowCount,
      Consumer<BlockingTransformMeta> metaSetup,
      Consumer<BlockingTransform> beforeInit)
      throws Exception {
    BlockingTransformMeta meta = new BlockingTransformMeta();
    meta.setPassAllRows(passAllRows);
    meta.setCacheSize(cacheSize);
    meta.setCompressFiles(compress);
    meta.setDirectory(tempDir.toString());
    meta.setPrefix("block");
    metaSetup.accept(meta);
    lastData = new BlockingTransformData();

    BlockingTransform transform =
        new BlockingTransform(
            helper.transformMeta, meta, lastData, 0, helper.pipelineMeta, helper.pipeline);
    beforeInit.accept(transform);
    transform.init();

    List<Object[]> input = new ArrayList<>();
    for (int i = 0; i < rowCount; i++) {
      input.add(row(i));
    }
    IRowSet rowSet = helper.getMockInputRowSet(input);
    // BaseTransform also reads with getRowImmediate() (every 500 rows): same rows
    when(rowSet.getRowImmediate()).thenAnswer(invocation -> rowSet.getRow());
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    when(rowSet.getRowMeta()).thenReturn(rowMeta);
    transform.setInputRowSets(new ArrayList<>(List.of(rowSet)));

    List<Object[]> output = new ArrayList<>();
    transform.addRowListener(
        new RowAdapter() {
          @Override
          public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
            output.add(row);
          }
        });

    while (transform.processRow()) {
      // keep going until the transform is done
    }
    transform.dispose();
    return output;
  }

  private static Object[] row(int i) {
    return new Object[] {(long) i, "row " + i};
  }

  private static void assertRowsInOrder(int rowCount, List<Object[]> output) {
    assertEquals(rowCount, output.size());
    for (int i = 0; i < rowCount; i++) {
      Object[] expected = row(i);
      Object[] actual = output.get(i);
      assertEquals(expected[0], actual[0], "row " + i);
      assertEquals(expected[1], actual[1], "row " + i);
    }
  }

  private void assertNoTempFilesLeft() throws Exception {
    try (Stream<Path> left = Files.list(tempDir)) {
      assertEquals(0, left.count(), "temp files left behind");
    }
  }
}
