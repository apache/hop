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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class ConcatFields extends BaseTransform<ConcatFieldsMeta, ConcatFieldsData>
    implements ITransform<ConcatFieldsMeta, ConcatFieldsData> {

  private static final Class<?> PKG = ConcatFields.class; // For Translator

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

    boolean result = true;
    boolean bEndedLineWrote = false;

    Object[] r = getRow(); // This also waits for a row to be finished.

    if (r != null && first) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      // the field precisions and lengths are altered! see TextFileOutputMeta.getFields().
      // otherwise trim(), padding etc. will not work
      data.inputRowMetaModified = getInputRowMeta().clone();
      meta.getFieldsModifyInput(
          data.inputRowMetaModified, getTransformName(), null, null, this, metadataProvider);

      data.posTargetField = data.outputRowMeta.indexOfValue(meta.getTargetFieldName());
      if (data.posTargetField < 0) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG,
                "ConcatFields.Error.TargetFieldNotFoundOutputStream",
                "" + meta.getTargetFieldName()));
      }

      data.fieldnrs = new int[meta.getOutputFields().length];
      for (int i = 0; i < meta.getOutputFields().length; i++) {
        data.fieldnrs[i] =
            data.inputRowMetaModified.indexOfValue(meta.getOutputFields()[i].getName());
        if (data.fieldnrs[i] < 0) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG,
                  "ConcatFields.Error.FieldNotFoundInputStream",
                  "" + meta.getOutputFields()[i].getName()));
        }
      }

      // prepare for fast data dump (StringBuilder size)
      data.targetFieldLengthFastDataDump = meta.getTargetFieldLength();
      if (data.targetFieldLengthFastDataDump <= 0) { // try it as a guess: 50 * size
        if (meta.getOutputFields().length == 0) {
          data.targetFieldLengthFastDataDump = 50 * getInputRowMeta().size();
        } else {
          data.targetFieldLengthFastDataDump = 50 * meta.getOutputFields().length;
        }
      }
      prepareForReMap();
    }

    if (r == null) {
      setOutputDone();
      return false;
    }

    r = putRowFastDataDump(r);

    if (log.isRowLevel()) {
      logRowlevel(
          BaseMessages.getString(PKG, "ConcatFields.Log.WriteRow")
              + getLinesWritten()
              + " : "
              + data.outputRowMeta.getString(r));
    }
    if (checkFeedback(getLinesRead())) {
      if (log.isBasic()) {
        logBasic(BaseMessages.getString(PKG, "ConcatFields.Log.LineNumber") + getLinesRead());
      }
    }

    return result;
  }

  void prepareForReMap() throws HopTransformException {
    // prepare for re-map when removeSelectedFields
    if (meta.isRemoveSelectedFields()) {
      data.remainingFieldsInputOutputMapping =
          new int[data.outputRowMeta.size() - 1]; // -1: don't need the new
      // target field
      String[] fieldNames = data.outputRowMeta.getFieldNames();
      for (int i = 0; i < fieldNames.length - 1; i++) { // -1: don't search the new target field
        data.remainingFieldsInputOutputMapping[i] =
            data.inputRowMetaModified.indexOfValue(fieldNames[i]);
        if (data.remainingFieldsInputOutputMapping[i] < 0) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, "ConcatFields.Error.RemainingFieldNotFoundInputStream", "" + fieldNames[i]));
        }
      }
    }
  }

  // reads the row from the stream, flushs, add target field and call putRow()
  Object[] putRowFromStream(Object[] r) throws HopTransformException {

    Object[] outputRowData = prepareOutputRow(r);

    // add target field
    outputRowData[data.posTargetField] = null;

    putRow(data.outputRowMeta, outputRowData);
    return outputRowData;
  }

  // concat as a fast data dump (no formatting) and call putRow()
  // this method is only called from a normal line, never from header/footer/split stuff
  Object[] putRowFastDataDump(Object[] r) throws HopException {

    Object[] outputRowData = prepareOutputRow(r);

    StringBuilder targetString =
        new StringBuilder(data.targetFieldLengthFastDataDump); // use a good capacity

    if (meta.getOutputFields() == null || meta.getOutputFields().length == 0) {
      // all values in stream
      for (int i = 0; i < getInputRowMeta().size(); i++) {
        if (i > 0) {
          targetString.append(data.stringSeparator);
        }
        IValueMeta valueMeta = getInputRowMeta().getValueMeta(i);
        concatFieldFastDataDump(
            targetString, valueMeta, r[i], ""); // "": no specific null value defined
      }
    } else {
      for (int i = 0; i < data.fieldnrs.length; i++) {
        if (i > 0) {
          targetString.append(data.stringSeparator);
        }
        IValueMeta valueMeta = getInputRowMeta().getValueMeta(i);
        concatFieldFastDataDump(
            targetString, valueMeta, r[data.fieldnrs[i]], data.stringNullValue[i]);
      }
    }

    outputRowData[data.posTargetField] = new String(targetString);

    putRow(data.outputRowMeta, outputRowData);
    return outputRowData;
  }

  private void concatFieldFastDataDump(
      StringBuilder targetField, IValueMeta valueMeta, Object valueData, String nullString)
      throws HopValueException {

    if (valueMeta.isNull(valueData)) {
      targetField.append(nullString);
    } else {
      targetField.append(valueMeta.getString(valueData));
    }
  }

  // reserve room for the target field and eventually re-map the fields
  private Object[] prepareOutputRow(Object[] r) {
    Object[] outputRowData = null;
    if (!meta.isRemoveSelectedFields()) {
      // reserve room for the target field
      outputRowData = RowDataUtil.resizeArray(r, data.outputRowMeta.size());
    } else {
      // reserve room for the target field and re-map the fields
      outputRowData = new Object[data.outputRowMeta.size() + RowDataUtil.OVER_ALLOCATE_SIZE];
      if (r != null) {
        // re-map the fields
        for (int i = 0;
            i < data.remainingFieldsInputOutputMapping.length;
            i++) { // BTW: the new target field is not
          // here
          outputRowData[i] = r[data.remainingFieldsInputOutputMapping[i]];
        }
      }
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

    data.stringNullValue = new String[meta.getOutputFields().length];
    for (int i = 0; i < meta.getOutputFields().length; i++) {
      data.stringNullValue[i] = "";
      String nullString = meta.getOutputFields()[i].getNullString();
      if (!StringUtil.isEmpty(nullString)) {
        data.stringNullValue[i] = nullString;
      }
    }
  }

  @Override
  public void dispose() {
    super.dispose();
  }
}
