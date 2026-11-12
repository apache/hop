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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    DetectLanguage transform = createTransform(meta(false, false), false);
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
    transform.dispose();
  }

  @Test
  void lowAccuracyModeFillsBothFields() throws Exception {
    DetectLanguage transform = createTransform(meta(false, true), false);
    assertTrue(transform.init());

    BlockingRowSet output = run(transform, row(1L, GERMAN_TEXT));

    Object[] german = output.getRowWait(1, TimeUnit.SECONDS);
    assertEquals("GERMAN", german[2]);
    assertNotNull(german[3]);
    transform.dispose();
  }

  @Test
  void parallelModeWritesEveryRow() throws Exception {
    DetectLanguage transform = createTransform(meta(true, false), false);
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
    transform.dispose();
  }

  /** A failing parallel job used to leave the transform waiting forever at the end. */
  @Test
  void aFailingParallelJobStopsTheTransform() {
    DetectLanguage transform = createTransform(meta(true, false), true);
    assertTrue(transform.init());

    HopException e =
        assertTimeoutPreemptively(
            Duration.ofSeconds(30),
            () -> assertThrows(HopException.class, () -> run(transform, row(1L, ENGLISH_TEXT))));
    assertTrue(e.getMessage().contains("no room for this row"));
    transform.dispose();
  }

  @Test
  void anUnknownLanguageFailsAtStart() {
    DetectLanguageMeta meta = meta(false, false);
    meta.getLanguages().add(new DetectLanguageChoice("KLINGON"));

    assertFalse(createTransform(meta, false).init());
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

  /** Feeds the rows through the transform until it reports the end of its input. */
  private BlockingRowSet run(DetectLanguage transform, Object[]... rows) throws HopException {
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

    List<Boolean> results = new ArrayList<>();
    boolean more = true;
    while (more) {
      more = transform.processRow();
      results.add(more);
    }
    assertEquals(rows.length + 1, results.size());
    return output;
  }

  private DetectLanguage createTransform(DetectLanguageMeta meta, boolean failOnWrite) {
    when(mockHelper.transformMeta.getTransform()).thenReturn(meta);
    DetectLanguageData data = new DetectLanguageData();
    if (!failOnWrite) {
      return new DetectLanguage(
          mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
    }
    return new DetectLanguage(
        mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline) {
      @Override
      public void putRow(IRowMeta rowMeta, Object[] row) throws HopTransformException {
        throw new HopTransformException("no room for this row");
      }
    };
  }
}
