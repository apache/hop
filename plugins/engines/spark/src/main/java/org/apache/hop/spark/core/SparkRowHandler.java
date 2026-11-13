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

import java.util.List;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.IRowHandler;
import org.apache.hop.pipeline.transform.IRowListener;
import org.apache.hop.pipeline.transform.IRowToListener;

/**
 * Non-blocking row I/O for single-threaded mini-pipelines on Spark executors (same idea as Beam's
 * {@code BeamRowHandler}).
 *
 * <p>The default {@link BaseTransform} row handler busy-waits when an input hop is empty and not
 * yet done. That deadlocks Stream Lookup: after {@code readLookupValues()} it calls {@code
 * getRow()} for the main stream while the single-threaded executor has not yet (or cannot) produce
 * the next main row on another thread.
 */
public class SparkRowHandler implements IRowHandler {

  private final BaseTransform transform;

  public SparkRowHandler(BaseTransform transform) {
    this.transform = transform;
  }

  /**
   * Prefer live rowsets from the transform (not a snapshot at construction): hop wiring and {@code
   * addRowProducer} can still change after the handler is installed.
   */
  private IRowSet firstOutput() {
    List<IRowSet> outputRowSets = transform.getOutputRowSets();
    return outputRowSets == null || outputRowSets.isEmpty() ? null : outputRowSets.get(0);
  }

  /**
   * The next row of the current input, without waiting. A finished input is dropped and reading
   * goes on with the next one, as {@link BaseTransform} does: a transform that reads several inputs
   * through {@code getRow()} (Stream schema merge, ...) gets all of them, and {@code
   * getCurrentInputRowSetNr()} says which one the row came from. An input that is empty but not
   * finished gives null: more rows can still arrive.
   */
  @Override
  public Object[] getRow() throws HopException {
    List<IRowSet> inputRowSets = transform.getInputRowSets();
    if (inputRowSets == null || inputRowSets.isEmpty()) {
      return null;
    }
    int current =
        Math.max(0, Math.min(transform.getCurrentInputRowSetNr(), inputRowSets.size() - 1));
    boolean dropped = false;
    Object[] row = null;
    IRowSet inputRowSet = null;
    while (!inputRowSets.isEmpty()) {
      inputRowSet = inputRowSets.get(current);
      row = inputRowSet.getRow();
      if (row != null || !inputRowSet.isDone()) {
        break;
      }
      inputRowSets.remove(current);
      dropped = true;
      if (current >= inputRowSets.size()) {
        current = 0;
      }
    }
    if (dropped) {
      transform.setInputRowSets(inputRowSets);
    }
    transform.setCurrentInputRowSetNr(inputRowSets.isEmpty() ? 0 : current);
    if (row != null) {
      transform.incrementLinesRead();
      transform.setInputRowMeta(inputRowSet.getRowMeta());
      List<IRowListener> rowListeners = transform.getRowListeners();
      for (IRowListener rowListener : rowListeners) {
        rowListener.rowReadEvent(inputRowSet.getRowMeta(), row);
      }
    }
    return row;
  }

  @Override
  public void putRow(IRowMeta rowMeta, Object[] row) throws HopTransformException {
    List<IRowListener> rowListeners = transform.getRowListeners();
    for (IRowListener rowListener : rowListeners) {
      rowListener.rowWrittenEvent(rowMeta, row);
    }
    IRowSet outputRowSet = firstOutput();
    if (outputRowSet != null) {
      outputRowSet.putRow(rowMeta, row);
    }
    // Always count written (including leaf transforms with no output hop)
    transform.incrementLinesWritten();
  }

  @Override
  public void putError(
      IRowMeta rowMeta,
      Object[] row,
      long nrErrors,
      String errorDescriptions,
      String fieldNames,
      String errorCodes)
      throws HopTransformException {
    transform.handlePutError(
        transform, rowMeta, row, nrErrors, errorDescriptions, fieldNames, errorCodes);
  }

  @Override
  public void putRowTo(IRowMeta rowMeta, Object[] row, IRowSet rowSet)
      throws HopTransformException {
    // Destination-aware listeners (not IRowListener — mixed layouts need the target rowset)
    List<IRowToListener> rowToListeners = transform.getRowToListeners();
    for (IRowToListener listener : rowToListeners) {
      listener.rowWrittenTo(rowMeta, row, rowSet);
    }
    rowSet.putRow(rowMeta, row);
    transform.incrementLinesWritten();
  }

  @Override
  public Object[] getRowFrom(IRowSet rowSet) throws HopTransformException {
    Object[] row = rowSet.getRow();
    if (row != null) {
      transform.incrementLinesRead();
      List<IRowListener> rowListeners = transform.getRowListeners();
      for (IRowListener listener : rowListeners) {
        listener.rowReadEvent(rowSet.getRowMeta(), row);
      }
    }
    return row;
  }
}
