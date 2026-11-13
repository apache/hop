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

package org.apache.hop.pipeline.transforms.language;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.github.pemistahl.lingua.api.Language;
import com.github.pemistahl.lingua.api.LanguageDetectorBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
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

class DetectLanguageTest {

  private static final String ENGLISH_TEXT =
      "The weather is lovely today and we are going for a long walk in the park";
  private static final String FRENCH_TEXT =
      "Il fait très beau aujourd'hui et nous allons nous promener dans le parc";
  private static final String GERMAN_TEXT =
      "Das Wetter ist heute sehr schön und wir gehen im Park spazieren";

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<DetectLanguageMeta, DetectLanguageData> mockHelper;

  /** Disposed after every test, also a failed one: the Lingua models live in static maps. */
  private final List<TestDetectLanguage> transforms = new ArrayList<>();

  @BeforeEach
  void setUp() {
    mockHelper =
        new TransformMockHelper<>(
            "Detect language", DetectLanguageMeta.class, DetectLanguageData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
    when(mockHelper.transformMeta.getCopies(any())).thenReturn(1);
    when(mockHelper.transformMeta.isPartitioned()).thenReturn(false);
    when(mockHelper.transformMeta.isDoingErrorHandling()).thenReturn(false);
  }

  @AfterEach
  void tearDown() {
    for (TestDetectLanguage transform : transforms) {
      transform.dispose();
    }
    mockHelper.cleanUp();
  }

  /** Without options the detector is what the transform always built. */
  @Test
  void theDefaultDetectorCoversEverySpokenLanguageWithHighAccuracy() {
    LanguageDetectorBuilder builder = DetectLanguage.createBuilder(List.of(), false);

    assertEquals(Language.allSpokenOnes(), builder.getLanguages$lingua());
    assertFalse(builder.isLowAccuracyModeEnabled$lingua());
    assertTrue(builder.isEveryLanguageModelPreloaded$lingua());
  }

  @Test
  void theConfiguredLanguagesAndAccuracyModeReachTheDetector() {
    LanguageDetectorBuilder builder =
        DetectLanguage.createBuilder(List.of(Language.ENGLISH, Language.FRENCH), true);

    assertEquals(List.of(Language.ENGLISH, Language.FRENCH), builder.getLanguages$lingua());
    assertTrue(builder.isLowAccuracyModeEnabled$lingua());
  }

  @Test
  void rowsGetTheirLanguageAndBlankTextsPassAsUnknown() throws Exception {
    DetectLanguage transform = createTransform(meta(false, false));
    assertTrue(transform.init());

    BlockingRowSet output =
        run(transform, row(1L, ENGLISH_TEXT), row(2L, null), row(3L, FRENCH_TEXT), row(4L, "   "));

    Object[] english = output.getRowWait(1, TimeUnit.SECONDS);
    assertEquals(1L, english[0]);
    assertEquals("ENGLISH", english[2]);
    assertNotNull(english[3]);

    // a null or blank text used to drop the row
    Object[] empty = output.getRowWait(1, TimeUnit.SECONDS);
    assertEquals(2L, empty[0]);
    assertEquals("UNKNOWN", empty[2]);
    assertNull(empty[3]);

    assertEquals("FRENCH", output.getRowWait(1, TimeUnit.SECONDS)[2]);

    Object[] blank = output.getRowWait(1, TimeUnit.SECONDS);
    assertEquals(4L, blank[0]);
    assertEquals("UNKNOWN", blank[2]);
    assertNull(blank[3]);

    assertNull(output.getRowWait(100, TimeUnit.MILLISECONDS));
  }

  @Test
  void theCorpusFieldCanComeFromAVariable() throws Exception {
    DetectLanguageMeta meta = meta(false, false);
    meta.setCorpusField("${TEXT_FIELD}");
    DetectLanguage transform = createTransform(meta);
    assertTrue(transform.init());
    transform.setVariable("TEXT_FIELD", "comment");

    BlockingRowSet output = run(transform, row(1L, GERMAN_TEXT));

    assertEquals("GERMAN", output.getRowWait(1, TimeUnit.SECONDS)[2]);
  }

  @Test
  void lowAccuracyModeFillsBothFields() throws Exception {
    DetectLanguage transform = createTransform(meta(false, true));
    assertTrue(transform.init());

    BlockingRowSet output = run(transform, row(1L, GERMAN_TEXT));

    Object[] german = output.getRowWait(1, TimeUnit.SECONDS);
    assertEquals("GERMAN", german[2]);
    assertNotNull(german[3]);
  }

  @Test
  void parallelModeWritesEveryRow() throws Exception {
    DetectLanguage transform = createTransform(meta(true, false));
    assertTrue(transform.init());

    BlockingRowSet output =
        run(transform, row(1L, ENGLISH_TEXT), row(2L, FRENCH_TEXT), row(3L, GERMAN_TEXT));

    Map<Object, Object> languageById = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      Object[] row = output.getRowWait(5, TimeUnit.SECONDS);
      assertNotNull(row);
      languageById.put(row[0], row[2]);
    }
    assertEquals(Map.of(1L, "ENGLISH", 2L, "FRENCH", 3L, "GERMAN"), languageById);
  }

  /** A failing parallel job used to leave the transform waiting forever at the end. */
  @Test
  void aFailingParallelJobStopsTheTransform() {
    DetectLanguage transform = createTransform(meta(true, false), DetectLanguageTest::noRoom);
    assertTrue(transform.init());

    HopException e =
        assertTimeoutPreemptively(
            Duration.ofSeconds(30),
            () -> assertThrows(HopException.class, () -> run(transform, row(1L, ENGLISH_TEXT))));
    assertTrue(e.getMessage().contains("no room for this row"));
  }

  @Test
  void aFailingParallelRowGoesToTheErrorHop() throws Exception {
    when(mockHelper.transformMeta.isDoingErrorHandling()).thenReturn(true);
    TestDetectLanguage transform = createTransform(meta(true, false), DetectLanguageTest::noRoom);
    assertTrue(transform.init());

    assertTimeoutPreemptively(
        Duration.ofSeconds(30), () -> run(transform, row(1L, ENGLISH_TEXT), row(2L, FRENCH_TEXT)));

    List<ErrorRow> errorRows = new ArrayList<>(transform.errorRows);
    assertEquals(2, errorRows.size());
    for (ErrorRow errorRow : errorRows) {
      assertEquals("comment", errorRow.fieldNames());
      assertEquals("DetectLanguage001", errorRow.errorCodes());
    }
  }

  /** An Error says nothing about the row: it stops the transform, also with error handling on. */
  @Test
  void anErrorInAParallelJobStopsTheTransform() {
    when(mockHelper.transformMeta.isDoingErrorHandling()).thenReturn(true);
    TestDetectLanguage transform =
        createTransform(
            meta(true, false),
            (rowMeta, row) -> {
              throw new NoClassDefFoundError("simulated failure");
            });
    assertTrue(transform.init());

    HopException e =
        assertTimeoutPreemptively(
            Duration.ofSeconds(30),
            () -> assertThrows(HopException.class, () -> run(transform, row(1L, ENGLISH_TEXT))));
    assertTrue(e.getMessage().contains("simulated failure"));
    assertTrue(transform.errorRows.isEmpty());
  }

  /**
   * After an error or a stop the transform is disposed without reaching the end of its input.
   * Parallel jobs that still run use the detector, so dispose waits for them.
   */
  @Test
  void disposeWaitsForTheParallelJobsStillRunning() throws Exception {
    CountDownLatch writing = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    TestDetectLanguage transform =
        createTransform(
            meta(true, false),
            (rowMeta, row) -> {
              writing.countDown();
              awaitQuietly(release);
            });
    assertTrue(transform.init());

    connect(transform, row(1L, ENGLISH_TEXT), row(2L, FRENCH_TEXT), row(3L, GERMAN_TEXT));
    for (int i = 0; i < 3; i++) {
      assertTrue(transform.processRow());
    }
    assertTrue(writing.await(30, TimeUnit.SECONDS));

    // What RunThread does when processRow fails or the pipeline is stopped
    transform.stopAll();
    Thread disposer = new Thread(transform::dispose, "dispose");
    disposer.start();
    disposer.join(300);
    assertTrue(disposer.isAlive(), "dispose returned while a job was still running");
    assertNotNull(transform.getData().detector);

    release.countDown();
    disposer.join(30_000);
    assertFalse(disposer.isAlive());
    assertNull(transform.getData().detector);
    assertTrue(transform.errorRows.isEmpty());
  }

  @Test
  void anUnknownLanguageFailsAtStart() {
    DetectLanguageMeta meta = meta(false, false);
    meta.getLanguages().add(new DetectLanguageChoice("KLINGON"));

    assertFalse(createTransform(meta).init());
  }

  private static DetectLanguageMeta meta(boolean parallelism, boolean lowAccuracy) {
    DetectLanguageMeta meta = new DetectLanguageMeta();
    meta.setCorpusField("comment");
    meta.setParallelism(parallelism);
    meta.setLowAccuracy(lowAccuracy);
    // three languages keep the models small; the default is covered by the builder test
    meta.getLanguages().add(new DetectLanguageChoice("ENGLISH"));
    meta.getLanguages().add(new DetectLanguageChoice("FRENCH"));
    meta.getLanguages().add(new DetectLanguageChoice("GERMAN"));
    return meta;
  }

  private static Object[] row(Long id, String comment) {
    return new Object[] {id, comment};
  }

  private static void noRoom(IRowMeta rowMeta, Object[] row) throws HopTransformException {
    throw new HopTransformException("no room for this row");
  }

  private static void awaitQuietly(CountDownLatch latch) {
    try {
      latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Connects an input holding the rows, and an output, to the transform. */
  private static BlockingRowSet connect(DetectLanguage transform, Object[]... rows) {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("comment"));

    BlockingRowSet input = new BlockingRowSet(100);
    input.setThreadNameFromToCopy("previous", 0, "Detect language", 0);
    input.setRowMeta(rowMeta);
    for (Object[] row : rows) {
      input.putRow(rowMeta, row);
    }
    input.setDone();

    BlockingRowSet output = new BlockingRowSet(100);
    output.setThreadNameFromToCopy("Detect language", 0, "next", 0);

    transform.addRowSetToInputRowSets(input);
    transform.addRowSetToOutputRowSets(output);
    return output;
  }

  /** Feeds the rows through the transform until it reports the end of its input. */
  private static BlockingRowSet run(DetectLanguage transform, Object[]... rows)
      throws HopException {
    BlockingRowSet output = connect(transform, rows);
    List<Boolean> results = new ArrayList<>();
    boolean more = true;
    while (more) {
      more = transform.processRow();
      results.add(more);
    }
    assertEquals(rows.length + 1, results.size());
    return output;
  }

  private TestDetectLanguage createTransform(DetectLanguageMeta meta) {
    return createTransform(meta, null);
  }

  private TestDetectLanguage createTransform(DetectLanguageMeta meta, RowWrite write) {
    when(mockHelper.transformMeta.getTransform()).thenReturn(meta);
    TestDetectLanguage transform = new TestDetectLanguage(mockHelper, meta, write);
    transforms.add(transform);
    return transform;
  }

  /** What a test's transform does with an output row instead of writing it. */
  @FunctionalInterface
  private interface RowWrite {
    void write(IRowMeta rowMeta, Object[] row) throws HopTransformException;
  }

  private record ErrorRow(Object id, String fieldNames, String errorCodes) {}

  /** The transform, with the writes a test controls and the error rows it sends. */
  private static class TestDetectLanguage extends DetectLanguage {
    private final RowWrite write;
    final List<ErrorRow> errorRows = Collections.synchronizedList(new ArrayList<>());

    TestDetectLanguage(
        TransformMockHelper<DetectLanguageMeta, DetectLanguageData> helper,
        DetectLanguageMeta meta,
        RowWrite write) {
      super(
          helper.transformMeta,
          meta,
          new DetectLanguageData(),
          0,
          helper.pipelineMeta,
          helper.pipeline);
      this.write = write;
    }

    @Override
    public void putRow(IRowMeta rowMeta, Object[] row) throws HopTransformException {
      if (write == null) {
        super.putRow(rowMeta, row);
      } else {
        write.write(rowMeta, row);
      }
    }

    @Override
    public void putError(
        IRowMeta rowMeta,
        Object[] row,
        long nrErrors,
        String errorDescriptions,
        String fieldNames,
        String errorCodes) {
      errorRows.add(new ErrorRow(row[0], fieldNames, errorCodes));
    }
  }
}
