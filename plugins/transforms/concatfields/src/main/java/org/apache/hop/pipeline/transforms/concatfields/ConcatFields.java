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
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.concatfields.helper.ConcatFieldHelper;

public class ConcatFields extends BaseTransform<ConcatFieldsMeta, ConcatFieldsData> {

  private static final Class<?> PKG = ConcatFields.class;

  public ConcatFields(
      TransformMeta transformMeta,
      ConcatFieldsMeta meta,
      ConcatFieldsData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    // allocate TextFileOutput
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public synchronized boolean processRow() throws HopException {
    // This also waits for a row to be finished.
    Object[] row = getRow();
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

    Object[] outputRowData = ConcatFieldHelper.concat(row, data, meta, getInputRowMeta());
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
}
