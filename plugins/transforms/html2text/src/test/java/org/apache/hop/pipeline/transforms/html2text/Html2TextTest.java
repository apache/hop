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

package org.apache.hop.pipeline.transforms.html2text;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class Html2TextTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<Html2TextMeta, Html2TextData> mockHelper;

  @BeforeEach
  void setUp() {
    mockHelper =
        new TransformMockHelper<>("HTML to text", Html2TextMeta.class, Html2TextData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
    when(mockHelper.transformMeta.getCopies(any())).thenReturn(1);
    when(mockHelper.transformMeta.isPartitioned()).thenReturn(false);
    when(mockHelper.transformMeta.isDoingErrorHandling()).thenReturn(false);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void theTextOfTheHtmlGoesIntoTheOutputField() throws Exception {
    Html2Text transform = createTransform(meta(false), false);
    assertTrue(transform.init());

    BlockingRowSet output =
        run(transform, row(1L, "<p>Hello <b>world</b> &amp; all</p>"), row(2L, "no tags"));

    Object[] first = output.getRowWait(1, TimeUnit.SECONDS);
    assertEquals(1L, first[0]);
    assertEquals("<p>Hello <b>world</b> &amp; all</p>", first[1]);
    assertEquals("Hello world & all", first[2]);
    assertEquals("no tags", output.getRowWait(1, TimeUnit.SECONDS)[2]);
    assertNull(output.getRowWait(100, TimeUnit.MILLISECONDS));
    transform.dispose();
  }

  /** A row with a null or blank HTML field used to disappear from the stream. */
  @Test
  void aRowWithEmptyHtmlPassesOnWithAnEmptyText() throws Exception {
    Html2Text transform = createTransform(meta(false), false);
    assertTrue(transform.init());

    BlockingRowSet output =
        run(transform, row(1L, null), row(2L, "   "), row(3L, "<p>text</p>"), row(4L, ""));

    for (long id : new long[] {1L, 2L}) {
      Object[] row = output.getRowWait(1, TimeUnit.SECONDS);
      assertNotNull(row, "row " + id + " was dropped");
      assertEquals(id, row[0]);
      assertEquals("", row[2]);
    }
    assertEquals("text", output.getRowWait(1, TimeUnit.SECONDS)[2]);
    Object[] last = output.getRowWait(1, TimeUnit.SECONDS);
    assertEquals(4L, last[0]);
    assertEquals("", last[2]);
    assertNull(output.getRowWait(100, TimeUnit.MILLISECONDS));
    transform.dispose();
  }

  @Test
  void parallelModeWritesEveryRow() throws Exception {
    Html2Text transform = createTransform(meta(true), false);
    assertTrue(transform.init());

    Object[][] rows = new Object[200][];
    for (int i = 0; i < rows.length; i++) {
      rows[i] = row((long) i, i % 10 == 0 ? null : "<p>row <i>" + i + "</i></p>");
    }
    BlockingRowSet output = run(transform, rows);

    Map<Object, Object> textById = new HashMap<>();
    for (int i = 0; i < rows.length; i++) {
      Object[] row = output.getRowWait(5, TimeUnit.SECONDS);
      assertNotNull(row);
      textById.put(row[0], row[2]);
    }
    assertEquals(rows.length, textById.size());
    assertEquals("", textById.get(0L));
    assertEquals("row 7", textById.get(7L));
    assertEquals("row 199", textById.get(199L));
    transform.dispose();
  }

  /** A failing parallel job used to leave the transform waiting forever at the end. */
  @Test
  void aFailingParallelJobStopsTheTransform() {
    Html2Text transform = createTransform(meta(true), true);
    assertTrue(transform.init());

    HopException e =
        assertTimeoutPreemptively(
            Duration.ofSeconds(30),
            () -> assertThrows(HopException.class, () -> run(transform, row(1L, "<p>x</p>"))));
    assertTrue(e.getMessage().contains("no room for this row"));
    transform.dispose();
  }

  @Test
  void aFailingRowStopsTheTransformWithoutParallelismToo() {
    Html2Text transform = createTransform(meta(false), true);
    assertTrue(transform.init());

    HopException e = assertThrows(HopException.class, () -> run(transform, row(1L, "<p>x</p>")));
    assertTrue(e.getMessage().contains("no room for this row"));
    transform.dispose();
  }

  /** Several copies at once: every copy converts its own rows. */
  @Test
  void copiesRunningAtTheSameTimeDoNotMixTheirRows() throws Exception {
    int copies = 6;
    int rowsPerCopy = 300;
    Html2TextMeta meta = meta(false);
    when(mockHelper.transformMeta.getTransform()).thenReturn(meta);
    ExecutorService pool = Executors.newFixedThreadPool(copies);
    try {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<BlockingRowSet>> outputs = new ArrayList<>();
      for (int copy = 0; copy < copies; copy++) {
        int copyNr = copy;
        outputs.add(
            pool.submit(
                () -> {
                  Object[][] rows = new Object[rowsPerCopy][];
                  for (int r = 0; r < rowsPerCopy; r++) {
                    rows[r] = row(copyNr * 10_000L + r, "<b>copy " + copyNr + "</b> row " + r);
                  }
                  Html2Text transform = newTransform(meta, false);
                  assertTrue(transform.init());
                  start.await();
                  return run(transform, rows);
                }));
      }
      start.countDown();
      for (int copy = 0; copy < copies; copy++) {
        BlockingRowSet output = outputs.get(copy).get(2, TimeUnit.MINUTES);
        for (int r = 0; r < rowsPerCopy; r++) {
          Object[] row = output.getRowWait(1, TimeUnit.SECONDS);
          assertEquals(copy * 10_000L + r, row[0]);
          assertEquals("copy " + copy + " row " + r, row[2]);
        }
      }
    } finally {
      pool.shutdownNow();
    }
  }

  private static Html2TextMeta meta(boolean parallelism) {
    Html2TextMeta meta = new Html2TextMeta();
    meta.setHtmlField("html");
    meta.setOutputField("text");
    meta.setCleanOnly(false);
    meta.setNormalisedText(true);
    meta.setParallelism(parallelism);
    return meta;
  }

  private static Object[] row(Long id, String html) {
    return new Object[] {id, html};
  }

  /** Feeds the rows through the transform until it reports the end of its input. */
  private static BlockingRowSet run(Html2Text transform, Object[]... rows) throws HopException {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("html"));

    BlockingRowSet input = new BlockingRowSet(rows.length + 10);
    input.setThreadNameFromToCopy("previous", 0, "HTML to text", 0);
    input.setRowMeta(rowMeta);
    for (Object[] row : rows) {
      input.putRow(rowMeta, row);
    }
    input.setDone();

    BlockingRowSet output = new BlockingRowSet(rows.length + 10);
    output.setThreadNameFromToCopy("HTML to text", 0, "next", 0);

    transform.addRowSetToInputRowSets(input);
    transform.addRowSetToOutputRowSets(output);

    int calls = 0;
    boolean more = true;
    while (more) {
      more = transform.processRow();
      calls++;
    }
    assertEquals(rows.length + 1, calls);
    assertFalse(more);
    return output;
  }

  private Html2Text createTransform(Html2TextMeta meta, boolean failOnWrite) {
    when(mockHelper.transformMeta.getTransform()).thenReturn(meta);
    return newTransform(meta, failOnWrite);
  }

  /** Without stubbing: safe to call from several threads. */
  private Html2Text newTransform(Html2TextMeta meta, boolean failOnWrite) {
    Html2TextData data = new Html2TextData();
    if (!failOnWrite) {
      return new Html2Text(
          mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
    }
    return new Html2Text(
        mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline) {
      @Override
      public void putRow(IRowMeta rowMeta, Object[] row) throws HopTransformException {
        throw new HopTransformException("no room for this row");
      }
    };
  }
}
