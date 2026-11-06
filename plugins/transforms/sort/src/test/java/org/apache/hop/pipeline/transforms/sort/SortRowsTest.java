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

package org.apache.hop.pipeline.transforms.sort;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.IRowSet;
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

class SortRowsTest {

  private static final int ROWS = 1_050;

  @TempDir Path tempDir;

  private TransformMockHelper<SortRowsMeta, SortRowsData> helper;

  @BeforeEach
  void setUp() {
    helper = new TransformMockHelper<>("Sort rows", SortRowsMeta.class, SortRowsData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  @Test
  void sortsInMemory() throws Exception {
    assertSorted(run("10000", true));
    assertNoTempFilesLeft();
  }

  @Test
  void sortsThroughCompressedTempFiles() throws Exception {
    // 10 full sort buffers and a partial one: 11 temp files to merge
    assertSorted(run("100", true));
    assertNoTempFilesLeft();
  }

  @Test
  void sortsThroughUncompressedTempFiles() throws Exception {
    assertSorted(run("100", false));
    assertNoTempFilesLeft();
  }

  private List<Object[]> run(String sortSize, boolean compress) throws Exception {
    SortRowsMeta meta = new SortRowsMeta();
    meta.setDefault();
    meta.setSortSize(sortSize);
    meta.setFreeMemoryLimit(null);
    meta.setCompressFiles(compress);
    meta.setDirectory(tempDir.toString());
    meta.setPrefix("sort");
    meta.getSortFields().clear();
    meta.getSortFields().add(new SortRowsField("id", true, true, false, 0, false));

    SortRows transform =
        new SortRows(
            helper.transformMeta,
            meta,
            new SortRowsData(),
            0,
            helper.pipelineMeta,
            helper.pipeline);
    transform.init();

    // Every id from 0 to ROWS - 1 once, in a scrambled order
    List<Object[]> input = new ArrayList<>();
    for (int i = 0; i < ROWS; i++) {
      long id = (i * 7919L) % ROWS;
      input.add(new Object[] {id, "row " + id});
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

  private static void assertSorted(List<Object[]> output) {
    assertEquals(ROWS, output.size());
    for (int i = 0; i < ROWS; i++) {
      assertEquals((long) i, output.get(i)[0], "row " + i);
      assertEquals("row " + i, output.get(i)[1], "row " + i);
    }
  }

  private void assertNoTempFilesLeft() throws Exception {
    try (Stream<Path> left = Files.list(tempDir)) {
      assertEquals(0, left.count(), "temp files left behind");
    }
  }
}
