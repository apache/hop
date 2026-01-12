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

package org.apache.hop.pipeline.transforms.concatfields;

import java.util.ArrayList;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class ConcatFields extends BaseTransform<ConcatFieldsMeta, ConcatFieldsData> {

  private static final Class<?> PKG = ConcatFields.class;

  public ConcatFields(
      TransformMeta transformMeta,
      ConcatFieldsMeta meta,
      ConcatFieldsData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline); // allocate TextFileOutput
  }

  @Override
  public synchronized boolean processRow() throws HopException {

    Object[] row = getRow(); // This also waits for a row to be finished.
    if (row == null) {
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      data.posTargetField =
          data.outputRowMeta.indexOfValue(meta.getExtraFields().getTargetFieldName());
      if (data.posTargetField < 0) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG,
                "ConcatFields.Error.TargetFieldNotFoundOutputStream",
                "" + meta.getExtraFields().getTargetFieldName()));
      }

      // Assume we want to keep all input fields in the output
      //
      data.outputFieldIndexes = new ArrayList<>();
      for (int i = 0; i < getInputRowMeta().size(); i++) {
        data.outputFieldIndexes.add(i);
      }

      data.inputFieldIndexes = new ArrayList<>();
      if (meta.getOutputFields().isEmpty()) {
        for (int i = 0; i < getInputRowMeta().size(); i++) {
          data.inputFieldIndexes.add(i);
          if (meta.getExtraFields().isRemoveSelectedFields()) {
            data.outputFieldIndexes.remove((Integer) i);
          }
        }
      } else {
        for (int i = 0; i < meta.getOutputFields().size(); i++) {
          ConcatField field = meta.getOutputFields().get(i);
          int fieldIndex = getInputRowMeta().indexOfValue(field.getName());
          if (fieldIndex < 0) {
            throw new HopTransformException(
                BaseMessages.getString(
                    PKG, "ConcatFields.Error.FieldNotFoundInputStream", "" + field.getName()));
          }
          data.inputFieldIndexes.add(fieldIndex);
          if (meta.getExtraFields().isRemoveSelectedFields()) {
            data.outputFieldIndexes.remove((Integer) fieldIndex);
          }
        }
      }

      // What's the target field length?
      // If the user didn't specify anything, take a guess.
      //
      data.targetFieldLength = meta.getExtraFields().getTargetFieldLength();
      if (data.targetFieldLength <= 0) { // try it as a guess: 50 * size
        if (meta.getOutputFields().isEmpty()) {
          data.targetFieldLength = 50 * getInputRowMeta().size();
        } else {
          data.targetFieldLength = 50 * meta.getOutputFields().size();
        }
      }
    }

    Object[] outputRowData = concatFields(row);
    putRow(data.outputRowMeta, outputRowData);

    if (isRowLevel()) {
      logRowlevel(
          BaseMessages.getString(PKG, "ConcatFields.Log.WriteRow")
              + getLinesWritten()
              + " : "
              + data.outputRowMeta.getString(row));
    }
    if (checkFeedback(getLinesRead()) && isBasic()) {
      logBasic(BaseMessages.getString(PKG, "ConcatFields.Log.LineNumber") + getLinesRead());
    }

    return true;
  }

  /** Concat the field values and call putRow() */
  private Object[] concatFields(Object[] inputRowData) throws HopException {
    Object[] outputRowData = createOutputRow(inputRowData);

    StringBuilder targetString = new StringBuilder(data.targetFieldLength); // use a good capacity

    for (int i = 0; i < data.inputFieldIndexes.size(); i++) {
      if (i > 0) {
        targetString.append(data.stringSeparator);
      }

      int inputRowIndex = data.inputFieldIndexes.get(i);
      IValueMeta valueMeta = getInputRowMeta().getValueMeta(inputRowIndex);
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

      concatField(targetString, valueMeta, valueData, nullString, trimType);
    }

    outputRowData[data.outputRowMeta.size() - 1] = targetString.toString();

    return outputRowData;
  }

  private void concatField(
      StringBuilder targetField,
      IValueMeta valueMeta,
      Object valueData,
      String nullString,
      String trimType)
      throws HopValueException {

    if (meta.isForceEnclosure()) {
      targetField.append(meta.getEnclosure());
    }
    if (valueMeta.isNull(valueData)) {
      targetField.append(nullString);
    } else {
      if (trimType != null) {
        // Get the raw string value without applying the incoming field's trim type
        // by temporarily setting trim type to NONE
        int originalTrimType = valueMeta.getTrimType();
        valueMeta.setTrimType(IValueMeta.TRIM_TYPE_NONE);
        String stringValue = valueMeta.getString(valueData);
        // Restore the original trim type
        valueMeta.setTrimType(originalTrimType);

        // Apply only the configured trim type from the transform
        targetField.append(
            Const.trimToType(stringValue, ValueMetaBase.getTrimTypeByCode(trimType)));
      }
    }
    if (meta.isForceEnclosure()) {
      targetField.append(meta.getEnclosure());
    }
  }

  // reserve room for the target field and eventually re-map the fields
  private Object[] createOutputRow(Object[] inputRowData) {
    // Allocate a new empty row
    //
    Object[] outputRowData;

    if (meta.getExtraFields().isRemoveSelectedFields()) {
      outputRowData = RowDataUtil.allocateRowData(data.outputRowMeta.size());
      int outputRowIndex = 0;

      // Copy over only the fields we want from the input.
      //
      for (int inputRowIndex : data.outputFieldIndexes) {
        outputRowData[outputRowIndex++] = inputRowData[inputRowIndex];
      }
    } else {
      // Keep the current row and add a field
      //
      outputRowData = RowDataUtil.createResizedCopy(inputRowData, data.outputRowMeta.size());
    }

    return outputRowData;
  }

  @Override
  public boolean init() {

    initStringDataFields();
    return super.init();
  }

  // init separator,enclosure, null values for fast data dump
  private void initStringDataFields() {
    data.stringSeparator = "";
    data.stringEnclosure = "";

    if (!StringUtil.isEmpty(meta.getSeparator())) {
      data.stringSeparator = resolve(meta.getSeparator());
    }
    if (!StringUtil.isEmpty(meta.getEnclosure())) {
      data.stringEnclosure = resolve(meta.getEnclosure());
    }

    data.stringNullValue = new String[meta.getOutputFields().size()];
    data.trimType = new String[meta.getOutputFields().size()];

    for (int i = 0; i < meta.getOutputFields().size(); i++) {
      data.stringNullValue[i] = "";
      String nullString = meta.getOutputFields().get(i).getNullString();
      if (!StringUtil.isEmpty(nullString)) {
        data.stringNullValue[i] = nullString;
      }
      String trimType = meta.getOutputFields().get(i).getTrimType();
      if (!StringUtil.isEmpty(trimType)) {
        data.trimType[i] = trimType;
      }
    }
  }

  @Override
  public void dispose() {
    super.dispose();
  }
}
