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

package org.apache.hop.pipeline.transforms.sortedschemamerge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Merges multiple pre-sorted input streams with different schemas into one globally sorted stream
 * with a unified sparse row layout.
 */
public class SortedSchemaMerge extends BaseTransform<SortedSchemaMergeMeta, SortedSchemaMergeData> {

  private static final Class<?> PKG = SortedSchemaMergeMeta.class;

  public SortedSchemaMerge(
      TransformMeta transformMeta,
      SortedSchemaMergeMeta meta,
      SortedSchemaMergeData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRowSorted();
    if (row == null) {
      setOutputDone();
      return false;
    }

    putRow(data.outputRowMeta, row);
    if (checkFeedback(getLinesRead()) && isBasic()) {
      logBasic(BaseMessages.getString(PKG, "SortedSchemaMerge.Log.LineNumber", getLinesRead()));
    }
    return true;
  }

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }
    if (meta.getSortKeys() == null || meta.getSortKeys().isEmpty()) {
      logError(BaseMessages.getString(PKG, "SortedSchemaMerge.Error.NoSortKeys"));
      return false;
    }
    return true;
  }

  private synchronized Object[] getRowSorted() throws HopException {
    if (first) {
      first = false;
      initializeSortedMerge();
    }

    if (data.sortedBuffer == null || data.sortedBuffer.isEmpty()) {
      return null;
    }

    SortedSchemaMergeRow smallestRow = data.sortedBuffer.remove(0);
    Object[] outputRow =
        data.schemaMapping.mapRow(smallestRow.getStreamIndex(), smallestRow.getRowData());

    Object[] extraRow = getRowFrom(smallestRow.getRowSet());
    if (extraRow != null) {
      SortedSchemaMergeRow add =
          new SortedSchemaMergeRow(
              smallestRow.getStreamIndex(),
              smallestRow.getRowSet(),
              smallestRow.getRowSet().getRowMeta(),
              extraRow);
      int index = Collections.binarySearch(data.sortedBuffer, add, data.comparator);
      if (index < 0) {
        data.sortedBuffer.add(-index - 1, add);
      } else {
        data.sortedBuffer.add(index, add);
      }
    }

    if (getPipeline().isSafeModeEnabled()) {
      safeModeChecking(smallestRow.getRowMeta());
    }

    return outputRow;
  }

  private void initializeSortedMerge() throws HopException {
    List<IRowSet> inputRowSets = getInputRowSets();
    if (inputRowSets == null || inputRowSets.size() < 2) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "SortedSchemaMerge.Error.TooFewInputs"));
    }

    data.sortedBuffer = new ArrayList<>();
    List<IRowMeta> inputLayouts = new ArrayList<>();
    for (int i = inputRowSets.size() - 1; i >= 0 && !isStopped(); i--) {
      IRowSet rowSet = inputRowSets.get(i);
      Object[] row = getRowFrom(rowSet);
      if (row != null) {
        int streamIndex = inputLayouts.size();
        inputLayouts.add(rowSet.getRowMeta());
        data.sortedBuffer.add(
            new SortedSchemaMergeRow(streamIndex, rowSet, rowSet.getRowMeta(), row));
      }
    }

    if (data.sortedBuffer.isEmpty()) {
      return;
    }

    IRowMeta[] inputRowMetas = inputLayouts.toArray(new IRowMeta[0]);
    data.schemaMapping = SortedSchemaMergeLogic.buildSchemaMapping(inputRowMetas);
    data.outputRowMeta = data.schemaMapping.getOutputRowMeta().clone();
    for (int i = 0; i < data.outputRowMeta.size(); i++) {
      data.outputRowMeta.getValueMeta(i).setOrigin(getTransformName());
    }

    data.sortKeyIndices =
        SortedSchemaMergeLogic.resolveSortKeyIndices(inputRowMetas, meta.getSortKeys());
    data.comparator =
        (left, right) -> {
          try {
            return SortedSchemaMergeLogic.compareRows(
                left, right, data.sortKeyIndices, meta.getSortKeys());
          } catch (HopValueException e) {
            throw new IllegalStateException("Error comparing sorted rows", e);
          }
        };

    data.sortedBuffer.sort(data.comparator);
  }
}
