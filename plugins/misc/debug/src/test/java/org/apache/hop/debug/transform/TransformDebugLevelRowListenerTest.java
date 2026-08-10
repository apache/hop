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

package org.apache.hop.debug.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.Condition;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.ValueMetaAndData;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The custom log level has to be switched on for exactly the selected rows, and it has to still be
 * switched on by the time the transform logs the row, which transforms do after they called {@code
 * putRow()}.
 */
class TransformDebugLevelRowListenerTest {

  private static final LogLevel BASE = LogLevel.NOTHING;
  private static final LogLevel CUSTOM = LogLevel.ROWLEVEL;

  private final AtomicReference<LogLevel> currentLevel = new AtomicReference<>(BASE);

  @BeforeAll
  static void beforeAll() throws HopException {
    HopClientEnvironment.init();
  }

  // ------------------------------------------------------------------ helpers

  private IEngineComponent component() {
    currentLevel.set(BASE);
    ILogChannel logChannel = mock(ILogChannel.class);
    doAnswer(
            invocation -> {
              currentLevel.set(invocation.getArgument(0));
              return null;
            })
        .when(logChannel)
        .setLogLevel(any(LogLevel.class));
    when(logChannel.getLogLevel()).thenAnswer(invocation -> currentLevel.get());

    IEngineComponent component = mock(IEngineComponent.class);
    when(component.getLogChannel()).thenReturn(logChannel);
    return component;
  }

  private static TransformDebugLevel debugLevel(int startRow, int endRow, Condition condition) {
    TransformDebugLevel debugLevel = new TransformDebugLevel(CUSTOM);
    debugLevel.setStartRow(startRow);
    debugLevel.setEndRow(endRow);
    if (condition != null) {
      debugLevel.setCondition(condition);
    }
    return debugLevel;
  }

  private static IRowMeta rowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("seq"));
    return rowMeta;
  }

  /**
   * Push a number of rows through the listener the way a transform does and collect, per row, the
   * log level that is active at the moment the transform logs its row: right after {@code putRow()}
   * returned.
   */
  private List<LogLevel> levelsWhenLogging(
      TransformDebugLevelRowListener listener, boolean readingRows, int nrRows)
      throws HopException {
    IRowMeta rowMeta = rowMeta();
    List<LogLevel> levels = new ArrayList<>();
    for (int i = 1; i <= nrRows; i++) {
      Object[] row = new Object[] {(long) i};
      if (readingRows) {
        listener.rowReadEvent(rowMeta, row);
      }
      listener.rowWrittenEvent(rowMeta, row);
      levels.add(currentLevel.get());
    }
    return levels;
  }

  // -------------------------------------------------------------------- tests

  @Test
  void rangeIsAppliedWhileTheTransformLogsTheRow() throws Exception {
    TransformDebugLevelRowListener listener =
        new TransformDebugLevelRowListener(component(), debugLevel(2, 4, null), CUSTOM, BASE, true);

    assertEquals(List.of(BASE, CUSTOM, CUSTOM, CUSTOM, BASE), levelsWhenLogging(listener, true, 5));
  }

  @Test
  void rangeIsAppliedForTransformsThatOnlyGenerateRows() throws Exception {
    // No read events at all: the row counter has to run on the written rows instead.
    //
    TransformDebugLevelRowListener listener =
        new TransformDebugLevelRowListener(
            component(), debugLevel(2, 4, null), CUSTOM, BASE, false);

    assertEquals(
        List.of(BASE, CUSTOM, CUSTOM, CUSTOM, BASE), levelsWhenLogging(listener, false, 5));
  }

  @Test
  void startRowWithoutEndRowRunsUntilTheEnd() throws Exception {
    TransformDebugLevelRowListener listener =
        new TransformDebugLevelRowListener(
            component(), debugLevel(3, -1, null), CUSTOM, BASE, true);

    assertEquals(List.of(BASE, BASE, CUSTOM, CUSTOM, CUSTOM), levelsWhenLogging(listener, true, 5));
  }

  @Test
  void endRowWithoutStartRowStartsAtTheFirstRow() throws Exception {
    TransformDebugLevelRowListener listener =
        new TransformDebugLevelRowListener(
            component(), debugLevel(-1, 2, null), CUSTOM, BASE, true);

    assertEquals(List.of(CUSTOM, CUSTOM, BASE, BASE, BASE), levelsWhenLogging(listener, true, 5));
  }

  @Test
  void conditionSelectsTheMatchingRows() throws Exception {
    Condition condition =
        new Condition(
            "seq",
            Condition.Function.EQUAL,
            null,
            new ValueMetaAndData(new ValueMetaInteger("constant"), 3L));

    TransformDebugLevelRowListener listener =
        new TransformDebugLevelRowListener(
            component(), debugLevel(-1, -1, condition), CUSTOM, BASE, true);

    assertEquals(List.of(BASE, BASE, CUSTOM, BASE, BASE), levelsWhenLogging(listener, true, 5));
  }

  @Test
  void conditionIsCombinedWithTheRange() throws Exception {
    Condition condition =
        new Condition(
            "seq",
            Condition.Function.LARGER,
            null,
            new ValueMetaAndData(new ValueMetaInteger("constant"), 2L));

    TransformDebugLevelRowListener listener =
        new TransformDebugLevelRowListener(
            component(), debugLevel(-1, 4, condition), CUSTOM, BASE, true);

    // Row 1 and 2 fail the condition, row 5 falls outside the range.
    //
    assertEquals(List.of(BASE, BASE, CUSTOM, CUSTOM, BASE), levelsWhenLogging(listener, true, 5));
  }
}
