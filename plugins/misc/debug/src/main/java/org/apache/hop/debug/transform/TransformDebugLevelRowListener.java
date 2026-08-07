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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.hop.core.Condition;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.transform.IRowListener;

/**
 * Switches the configured custom log level on for the rows selected by a {@link
 * TransformDebugLevel} start/end row range and/or condition, and back off for all other rows.
 *
 * <p>The level is evaluated exactly once per row and then left alone for the rest of that row's
 * processing. That matters because transforms log their row <em>after</em> they called {@code
 * putRow()}: switching the level back off on the write event (as this used to do) killed every
 * single row-level line the transform was supposed to produce.
 *
 * <p>Which event drives the evaluation depends on the transform: transforms that read rows are
 * evaluated on the read event (the row is then available for the condition before the transform
 * does any work with it), transforms that only generate rows are evaluated on the write event,
 * since they never fire a read event at all.
 */
public class TransformDebugLevelRowListener implements IRowListener {

  private final IEngineComponent component;
  private final TransformDebugLevel debugLevel;
  private final LogLevel customLogLevel;
  private final LogLevel baseLogLevel;
  private final boolean readingRows;

  private final AtomicLong rowCounter = new AtomicLong(0L);

  /**
   * @param component the transform copy to change the log level on
   * @param debugLevel the custom logging configuration to apply
   * @param customLogLevel the resolved log level to switch on for the selected rows
   * @param baseLogLevel the log level to fall back to for all other rows
   * @param readingRows true if the transform reads rows from other transforms, false if it only
   *     generates them
   */
  public TransformDebugLevelRowListener(
      IEngineComponent component,
      TransformDebugLevel debugLevel,
      LogLevel customLogLevel,
      LogLevel baseLogLevel,
      boolean readingRows) {
    this.component = component;
    this.debugLevel = debugLevel;
    this.customLogLevel = customLogLevel;
    this.baseLogLevel = baseLogLevel;
    this.readingRows = readingRows;
  }

  @Override
  public void rowReadEvent(IRowMeta rowMeta, Object[] row) {
    if (readingRows) {
      applyLogLevel(rowMeta, row);
    }
  }

  @Override
  public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
    if (!readingRows) {
      applyLogLevel(rowMeta, row);
    }
  }

  @Override
  public void errorRowWrittenEvent(IRowMeta rowMeta, Object[] row) {
    // Error rows are handled by the error handling of the transform, leave the level alone.
  }

  private void applyLogLevel(IRowMeta rowMeta, Object[] row) {
    long rowNumber = rowCounter.incrementAndGet();
    component
        .getLogChannel()
        .setLogLevel(isSelected(rowNumber, rowMeta, row) ? customLogLevel : baseLogLevel);
  }

  /**
   * Is the given row one of the rows we want the custom log level for?
   *
   * @param rowNumber the 1-based number of the row in this transform copy
   * @param rowMeta the metadata of the row
   * @param row the row itself
   * @return true if the custom log level applies to this row
   */
  boolean isSelected(long rowNumber, IRowMeta rowMeta, Object[] row) {
    int startRow = debugLevel.getStartRow();
    int endRow = debugLevel.getEndRow();

    // A start or end row of 0 or less simply means: no boundary on that side.
    //
    if (startRow > 0 && rowNumber < startRow) {
      return false;
    }
    if (endRow > 0 && rowNumber > endRow) {
      return false;
    }

    Condition condition = debugLevel.getCondition();
    if (condition == null || condition.isEmpty()) {
      return true;
    }
    return condition.evaluate(rowMeta, row);
  }
}
