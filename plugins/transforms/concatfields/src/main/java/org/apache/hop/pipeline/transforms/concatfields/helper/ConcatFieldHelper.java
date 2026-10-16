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

package org.apache.hop.pipeline.transforms.concatfields.helper;

import lombok.experimental.UtilityClass;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.util.Utils;
import org.apache.hop.pipeline.transforms.concatfields.ConcatField;
import org.apache.hop.pipeline.transforms.concatfields.ConcatFieldsData;
import org.apache.hop.pipeline.transforms.concatfields.ConcatFieldsMeta;

/**
 * Utility class used by the Concat Fields transform to concatenate multiple input field values into
 * a single string field.
 *
 * <p>The concatenation behavior supports:
 *
 * <ul>
 *   <li>Custom field separators
 *   <li>Null value replacement
 *   <li>Configurable trim types per field
 *   <li>Optional value enclosure
 *   <li>Skipping null and/or empty values
 * </ul>
 *
 * <p>This helper is stateless and thread-safe.
 */
@UtilityClass
public class ConcatFieldHelper {

  /**
   * Concatenates the values of the selected input fields into a single string field.
   *
   * @param inputRowData the input row data
   * @param data the runtime data for the Concat Fields transform
   * @param meta the metadata configuration for the Concat Fields transform
   * @param rowMeta the metadata describing the input row structure
   * @return the output row data with the concatenated field appended
   * @throws HopException if a conversion error occurs while processing field values
   */
  public static Object[] concat(
      Object[] inputRowData, ConcatFieldsData data, ConcatFieldsMeta meta, IRowMeta rowMeta)
      throws HopException {
    Object[] outputRowData = createOutputRow(inputRowData, data, meta);
    if (data.fieldTrimTypes == null) {
      prepareFields(data, meta);
    }

    // This runs for every row: build the target in one pass, no intermediate list or stream
    String separator = Const.NVL(data.stringSeparator, "");
    boolean skipEmpty = meta.isSkipValueEmpty();
    boolean forceEnclosure = meta.isForceEnclosure();
    String enclosure = meta.getEnclosure();
    StringBuilder target = new StringBuilder(Math.max(data.targetFieldLength, 16));
    boolean firstValue = true;

    for (int i = 0; i < data.inputFieldIndexes.size(); i++) {
      int inputRowIndex = data.inputFieldIndexes.get(i);
      IValueMeta valueMeta = rowMeta.getValueMeta(inputRowIndex);
      Object valueData = inputRowData[inputRowIndex];

      String value;
      if (valueMeta.isNull(valueData)) {
        value = data.fieldNullStrings[i];
      } else {
        value = fieldString(valueMeta, valueData, data.fieldTrimTypes[i]);
      }

      if (skipEmpty && Utils.isEmpty(value)) {
        continue;
      }
      if (!firstValue) {
        target.append(separator);
      }
      firstValue = false;
      if (forceEnclosure) {
        target.append(enclosure).append(value).append(enclosure);
      } else {
        target.append(value);
      }
    }

    outputRowData[data.outputRowMeta.size() - 1] = target.toString();
    return outputRowData;
  }

  /**
   * Work out the null string and trim type of every concatenated field once, rather than for every
   * row. Without output fields every input field is used, with no null string and no trimming.
   */
  private static void prepareFields(ConcatFieldsData data, ConcatFieldsMeta meta) {
    int nrFields = data.inputFieldIndexes.size();
    String[] nullStrings = new String[nrFields];
    int[] trimTypes = new int[nrFields];
    for (int i = 0; i < nrFields; i++) {
      if (meta.getOutputFields().isEmpty()) {
        nullStrings[i] = "";
        trimTypes[i] = IValueMeta.TRIM_TYPE_NONE;
      } else {
        ConcatField field = meta.getOutputFields().get(i);
        nullStrings[i] = Const.NVL(field.getNullString(), "");
        String trimType = data.trimType == null ? null : data.trimType[i];
        // Manage missing trim type. Leave the incoming value as it is
        trimTypes[i] =
            trimType == null
                ? IValueMeta.TRIM_TYPE_NONE
                : ValueMetaBase.getTrimTypeByCode(trimType);
      }
    }
    data.fieldNullStrings = nullStrings;
    data.fieldTrimTypes = trimTypes;
  }

  /**
   * Converts a non-null field value to its string representation with the trim type of this
   * transform rather than the one of the incoming field.
   *
   * @param valueMeta the value metadata describing the field type
   * @param valueData the actual field value
   * @param trimType the trim type to apply
   * @return the string value
   * @throws HopValueException if the value cannot be converted to a string
   */
  private static String fieldString(IValueMeta valueMeta, Object valueData, int trimType)
      throws HopValueException {
    // Get the raw string value without applying the incoming field's trim type
    // by temporarily setting trim type to NONE
    int originalTrimType = valueMeta.getTrimType();
    String stringValue;
    if (originalTrimType == IValueMeta.TRIM_TYPE_NONE) {
      stringValue = valueMeta.getString(valueData);
    } else {
      valueMeta.setTrimType(IValueMeta.TRIM_TYPE_NONE);
      try {
        stringValue = valueMeta.getString(valueData);
      } finally {
        // Restore the original trim type
        valueMeta.setTrimType(originalTrimType);
      }
    }

    // Apply only the configured trim type from the transform
    return Const.trimToType(stringValue, trimType);
  }

  /**
   * Creates the output row based on the transform configuration.
   *
   * @param inputRowData the input row data
   * @param data the runtime data for the transform
   * @param meta the transform metadata
   * @return a new output row array with the appropriate size
   */
  private static Object[] createOutputRow(
      Object[] inputRowData, ConcatFieldsData data, ConcatFieldsMeta meta) {
    // Allocate a new empty row
    Object[] outputRowData;

    if (meta.getExtraFields().isRemoveSelectedFields()) {
      outputRowData = RowDataUtil.allocateRowData(data.outputRowMeta.size());
      int outputRowIndex = 0;

      // Copy over only the fields we want from the input.
      for (int inputRowIndex : data.outputFieldIndexes) {
        outputRowData[outputRowIndex++] = inputRowData[inputRowIndex];
      }
    } else {
      // Keep the current row and add a field
      outputRowData = RowDataUtil.createResizedCopy(inputRowData, data.outputRowMeta.size());
    }
    return outputRowData;
  }
}
