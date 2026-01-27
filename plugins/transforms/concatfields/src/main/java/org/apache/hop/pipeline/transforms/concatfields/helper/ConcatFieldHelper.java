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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
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
    List<String> fields = new ArrayList<>(Math.max(data.targetFieldLength, 8));

    for (int i = 0; i < data.inputFieldIndexes.size(); i++) {
      int inputRowIndex = data.inputFieldIndexes.get(i);
      IValueMeta valueMeta = rowMeta.getValueMeta(inputRowIndex);
      Object valueData = inputRowData[inputRowIndex];

      String nullString;
      String trimType;
      if (meta.getOutputFields().isEmpty()) {
        // No specific null value defined
        nullString = "";
        trimType = ValueMetaBase.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE);
      } else {
        ConcatField field = meta.getOutputFields().get(i);
        nullString = Const.NVL(field.getNullString(), "");
        trimType = data.trimType[i];
        // Manage missing trim type. Leave the incoming value as it is
        if (trimType == null) {
          trimType = ValueMetaBase.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE);
        }
      }

      handlerField(fields, valueMeta, valueData, nullString, trimType);
    }

    // list -> string(support separator)
    String separator = Const.NVL(data.stringSeparator, "");
    String collect =
        fields.stream()
            .filter(c -> !(meta.isSkipValueEmpty() && Utils.isEmpty(c)))
            .map(c -> meta.isForceEnclosure() ? meta.getEnclosure() + c + meta.getEnclosure() : c)
            .collect(Collectors.joining(separator));

    outputRowData[data.outputRowMeta.size() - 1] = collect;
    return outputRowData;
  }

  /**
   * Converts a single field value to its string representation and adds it to the provided list of
   * field values.
   *
   * @param fields the list collecting converted field values
   * @param valueMeta the value metadata describing the field type
   * @param valueData the actual field value
   * @param nullString the replacement value used when the field value is null
   * @param trimType the trim type code to apply
   * @throws HopValueException if the value cannot be converted to a string
   */
  private static void handlerField(
      List<String> fields,
      IValueMeta valueMeta,
      Object valueData,
      String nullString,
      String trimType)
      throws HopValueException {
    if (valueMeta.isNull(valueData)) {
      fields.add(nullString);
      return;
    }

    if (trimType != null) {
      // Get the raw string value without applying the incoming field's trim type
      // by temporarily setting trim type to NONE
      int originalTrimType = valueMeta.getTrimType();
      valueMeta.setTrimType(IValueMeta.TRIM_TYPE_NONE);
      String stringValue = valueMeta.getString(valueData);
      // Restore the original trim type
      valueMeta.setTrimType(originalTrimType);

      // Apply only the configured trim type from the transform
      fields.add(Const.trimToType(stringValue, ValueMetaBase.getTrimTypeByCode(trimType)));
    }
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
