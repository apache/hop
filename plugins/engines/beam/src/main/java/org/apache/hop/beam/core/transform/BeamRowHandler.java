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
 *
 */

package org.apache.hop.beam.core.transform;

import java.util.List;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.IRowHandler;
import org.apache.hop.pipeline.transform.IRowListener;

/**
 * Reading and writing rows from/to row sets is simpler in a Beam context since we're using a single
 * thread. No locking is required and so on.
 */
public class BeamRowHandler implements IRowHandler {

  private final BaseTransform transform;

  private final IRowSet outputRowSet;
  private final IRowSet inputRowSet;

  public BeamRowHandler(BaseTransform transform) {
    this.transform = transform;

    List<IRowSet> inputRowSets = transform.getInputRowSets();
    if (inputRowSets.isEmpty()) {
      inputRowSet = null;
    } else {
      inputRowSet = inputRowSets.get(0);
    }
    List<IRowSet> outputRowSets = transform.getOutputRowSets();
    if (outputRowSets.isEmpty()) {
      outputRowSet = null;
    } else {
      outputRowSet = outputRowSets.get(0);
    }
  }

  @Override
  public Object[] getRow() throws HopException {
    if (inputRowSet == null) {
      return null;
    } else {
      Object[] row = inputRowSet.getRow();
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
  }

  @Override
  public void putRow(IRowMeta rowMeta, Object[] row) throws HopTransformException {
    // call all row listeners...
    //
    List<IRowListener> rowListeners = transform.getRowListeners();
    for (IRowListener rowListener : rowListeners) {
      rowListener.rowWrittenEvent(rowMeta, row);
    }

    if (outputRowSet != null) {
      outputRowSet.putRow(rowMeta, row);
      transform.incrementLinesWritten();
    }
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
    List<IRowListener> rowListeners = transform.getRowListeners();
    for (IRowListener listener : rowListeners) {
      listener.rowWrittenEvent(rowMeta, row);
    }

    rowSet.putRow(rowMeta, row);
    transform.incrementLinesWritten();
  }

  @Override
  public Object[] getRowFrom(IRowSet rowSet) throws HopTransformException {
    Object[] row = rowSet.getRow();
    transform.incrementLinesRead();

    // call all row listeners...
    //
    List<IRowListener> rowListeners = transform.getRowListeners();
    for (IRowListener listener : rowListeners) {
      listener.rowReadEvent(rowSet.getRowMeta(), row);
    }

    return row;
  }
}
