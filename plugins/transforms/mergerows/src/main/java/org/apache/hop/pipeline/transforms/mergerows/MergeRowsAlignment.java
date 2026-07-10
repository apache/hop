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

package org.apache.hop.pipeline.transforms.mergerows;

import java.util.HashSet;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.i18n.BaseMessages;

/** Reference-authoritative schema union and row mapping for {@link MergeRows}. */
public final class MergeRowsAlignment {

  private static final Class<?> PKG = MergeRowsMeta.class;

  private MergeRowsAlignment() {}

  /** Union of reference and compare layouts plus per-stream field index mappings. */
  public static final class SchemaMapping {
    private final IRowMeta outputRowMeta;
    private final IRowMeta referenceRowMeta;
    private final IRowMeta compareRowMeta;
    private final int[] referenceMapping;
    private final int[] compareMapping;

    private SchemaMapping(
        IRowMeta outputRowMeta,
        IRowMeta referenceRowMeta,
        IRowMeta compareRowMeta,
        int[] referenceMapping,
        int[] compareMapping) {
      this.outputRowMeta = outputRowMeta;
      this.referenceRowMeta = referenceRowMeta;
      this.compareRowMeta = compareRowMeta;
      this.referenceMapping = referenceMapping;
      this.compareMapping = compareMapping;
    }

    public IRowMeta getOutputRowMeta() {
      return outputRowMeta;
    }

    public Object[] mapRow(int streamIndex, Object[] sourceRow) throws HopTransformException {
      Object[] outputRow = new Object[outputRowMeta.size()];
      if (sourceRow == null) {
        return outputRow;
      }
      if (streamIndex == 0) {
        mapReferenceRow(sourceRow, outputRow);
      } else if (streamIndex == 1) {
        mapCompareRow(sourceRow, outputRow);
      }
      return outputRow;
    }

    private void mapReferenceRow(Object[] sourceRow, Object[] outputRow) {
      for (int sourceIndex = 0; sourceIndex < referenceMapping.length; sourceIndex++) {
        int outputIndex = referenceMapping[sourceIndex];
        if (outputIndex >= 0 && outputIndex < outputRow.length) {
          outputRow[outputIndex] = sourceRow[sourceIndex];
        }
      }
    }

    private void mapCompareRow(Object[] sourceRow, Object[] outputRow)
        throws HopTransformException {
      for (int sourceIndex = 0; sourceIndex < compareMapping.length; sourceIndex++) {
        int outputIndex = compareMapping[sourceIndex];
        if (outputIndex < 0 || outputIndex >= outputRow.length) {
          continue;
        }
        IValueMeta compareField = compareRowMeta.getValueMeta(sourceIndex);
        IValueMeta outputField = outputRowMeta.getValueMeta(outputIndex);
        Object value = sourceRow[sourceIndex];
        if (compareField.getType() != outputField.getType()) {
          value = convertCompareValue(compareField, outputField, value);
        }
        outputRow[outputIndex] = value;
      }
    }

    private Object convertCompareValue(
        IValueMeta compareField, IValueMeta referenceField, Object value)
        throws HopTransformException {
      try {
        return referenceField.convertData(compareField, value);
      } catch (HopValueException e) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG,
                "MergeRowsAlignment.Exception.UnableToConvertCompareField",
                compareField.getName(),
                ValueMetaFactory.getValueMetaName(compareField.getType()),
                ValueMetaFactory.getValueMetaName(referenceField.getType())),
            e);
      }
    }
  }

  public static SchemaMapping buildSchemaMapping(
      IRowMeta referenceRowMeta, IRowMeta compareRowMeta) {
    IRowMeta outputRowMeta = referenceRowMeta.clone();
    HashSet<String> fieldNames = new HashSet<>();
    for (String fieldName : outputRowMeta.getFieldNames()) {
      fieldNames.add(fieldName);
    }

    int[] referenceMapping = new int[referenceRowMeta.size()];
    for (int fieldIndex = 0; fieldIndex < referenceRowMeta.size(); fieldIndex++) {
      String name = referenceRowMeta.getValueMeta(fieldIndex).getName();
      referenceMapping[fieldIndex] = outputRowMeta.indexOfValue(name);
    }

    int[] compareMapping = new int[compareRowMeta.size()];
    for (int fieldIndex = 0; fieldIndex < compareRowMeta.size(); fieldIndex++) {
      IValueMeta compareField = compareRowMeta.getValueMeta(fieldIndex);
      String name = compareField.getName();
      if (!fieldNames.contains(name)) {
        outputRowMeta.addValueMeta(compareField.clone());
        fieldNames.add(name);
      }
      compareMapping[fieldIndex] = outputRowMeta.indexOfValue(name);
    }

    return new SchemaMapping(
        outputRowMeta, referenceRowMeta, compareRowMeta, referenceMapping, compareMapping);
  }
}
