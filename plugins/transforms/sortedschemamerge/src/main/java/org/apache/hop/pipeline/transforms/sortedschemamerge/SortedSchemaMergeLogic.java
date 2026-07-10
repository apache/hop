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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;

/** Schema union and row mapping helpers for {@link SortedSchemaMerge}. */
public final class SortedSchemaMergeLogic {

  private SortedSchemaMergeLogic() {}

  /** Union of all input row layouts plus per-stream field index mappings. */
  public static final class SchemaMapping {
    private final IRowMeta outputRowMeta;
    private final int[][] streamMappings;

    private SchemaMapping(IRowMeta outputRowMeta, int[][] streamMappings) {
      this.outputRowMeta = outputRowMeta;
      this.streamMappings = streamMappings;
    }

    public IRowMeta getOutputRowMeta() {
      return outputRowMeta;
    }

    public int[][] getStreamMappings() {
      return streamMappings;
    }

    public Object[] mapRow(int streamIndex, Object[] sourceRow) {
      Object[] outputRow = new Object[outputRowMeta.size()];
      if (sourceRow == null || streamIndex < 0 || streamIndex >= streamMappings.length) {
        return outputRow;
      }
      int[] mapping = streamMappings[streamIndex];
      for (int sourceIndex = 0; sourceIndex < mapping.length; sourceIndex++) {
        int outputIndex = mapping[sourceIndex];
        if (outputIndex >= 0 && outputIndex < outputRow.length) {
          outputRow[outputIndex] = sourceRow[sourceIndex];
        }
      }
      return outputRow;
    }
  }

  public static SchemaMapping buildSchemaMapping(IRowMeta[] inputRowMetas)
      throws HopPluginException {
    if (inputRowMetas == null || inputRowMetas.length == 0) {
      throw new HopPluginException("At least one input row layout is required");
    }

    int[][] mappings = new int[inputRowMetas.length][];
    IRowMeta outputRowMeta = inputRowMetas[0].clone();
    HashSet<String> fieldNames = new HashSet<>();
    Collections.addAll(fieldNames, outputRowMeta.getFieldNames());

    for (int streamIndex = 0; streamIndex < inputRowMetas.length; streamIndex++) {
      IRowMeta inputRowMeta = inputRowMetas[streamIndex];
      int[] streamMapping = new int[inputRowMeta.size()];
      for (int fieldIndex = 0; fieldIndex < inputRowMeta.size(); fieldIndex++) {
        IValueMeta field = inputRowMeta.getValueMeta(fieldIndex);
        String name = field.getName();
        if (!fieldNames.contains(name)) {
          outputRowMeta.addValueMeta(field);
          fieldNames.add(name);
        }
        int outputIndex = outputRowMeta.indexOfValue(name);
        streamMapping[fieldIndex] = outputIndex;
        IValueMeta outputField = outputRowMeta.getValueMeta(outputIndex);
        if (outputField.getType() != field.getType()) {
          IValueMeta updatedField =
              ValueMetaFactory.cloneValueMeta(outputField, IValueMeta.TYPE_STRING);
          outputRowMeta.setValueMeta(outputIndex, updatedField);
        }
      }
      mappings[streamIndex] = streamMapping;
    }

    return new SchemaMapping(outputRowMeta, mappings);
  }

  public static int[][] resolveSortKeyIndices(
      IRowMeta[] inputRowMetas, List<SortedSchemaMergeSortKey> sortKeys)
      throws HopTransformException {
    if (sortKeys == null || sortKeys.isEmpty()) {
      throw new HopTransformException("At least one sort key is required");
    }

    int[][] indices = new int[inputRowMetas.length][];
    for (int streamIndex = 0; streamIndex < inputRowMetas.length; streamIndex++) {
      IRowMeta rowMeta = inputRowMetas[streamIndex];
      int[] streamIndices = new int[sortKeys.size()];
      for (int keyIndex = 0; keyIndex < sortKeys.size(); keyIndex++) {
        SortedSchemaMergeSortKey sortKey = sortKeys.get(keyIndex);
        if (sortKey == null || Utils.isEmpty(sortKey.getFieldName())) {
          throw new HopTransformException("Sort key name is required for all streams");
        }
        int fieldIndex = rowMeta.indexOfValue(sortKey.getFieldName());
        if (fieldIndex < 0) {
          throw new HopTransformException(
              "Sort key [" + sortKey.getFieldName() + "] not found in input stream " + streamIndex);
        }
        streamIndices[keyIndex] = fieldIndex;
      }
      indices[streamIndex] = streamIndices;
    }
    return indices;
  }

  public static int compareRows(
      SortedSchemaMergeRow left,
      SortedSchemaMergeRow right,
      int[][] sortKeyIndices,
      List<SortedSchemaMergeSortKey> sortKeys)
      throws HopValueException {
    for (int keyIndex = 0; keyIndex < sortKeys.size(); keyIndex++) {
      SortedSchemaMergeSortKey sortKey = sortKeys.get(keyIndex);
      int leftFieldIndex = sortKeyIndices[left.getStreamIndex()][keyIndex];
      int rightFieldIndex = sortKeyIndices[right.getStreamIndex()][keyIndex];
      IValueMeta leftMeta = left.getRowMeta().getValueMeta(leftFieldIndex);
      IValueMeta rightMeta = right.getRowMeta().getValueMeta(rightFieldIndex);
      Object leftValue = left.getRowData()[leftFieldIndex];
      Object rightValue = right.getRowData()[rightFieldIndex];
      int compare = leftMeta.compare(leftValue, rightMeta, rightValue);
      if (!sortKey.isAscending()) {
        compare = -compare;
      }
      if (compare != 0) {
        return compare;
      }
    }
    return Integer.compare(left.getStreamIndex(), right.getStreamIndex());
  }
}
