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

package org.apache.hop.pipeline.transforms.fieldsplitter;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Split a single String fields into multiple parts based on certain conditions.
 *
 * @author Matt
 * @author Daniel Einspanjer
 * @since 31-Okt-2003
 * @since 15-01-2008
 */
public class FieldSplitter extends BaseTransform<FieldSplitterMeta, FieldSplitterData>
    implements ITransform<FieldSplitterMeta, FieldSplitterData> {
  private static final Class<?> PKG = FieldSplitterMeta.class; // For Translator

  public FieldSplitter(
      TransformMeta transformMeta,
      FieldSplitterMeta meta,
      FieldSplitterData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  private Object[] splitField(Object[] r) throws HopException {
    if (first) {
      first = false;
      // get the RowMeta
      data.previousMeta = getInputRowMeta().clone();

      // search field
      data.fieldnr = data.previousMeta.indexOfValue(meta.getSplitField());
      if (data.fieldnr < 0) {
        throw new HopValueException(
            BaseMessages.getString(
                PKG, "FieldSplitter.Log.CouldNotFindFieldToSplit", meta.getSplitField()));
      }

      // only String type allowed
      if (!data.previousMeta.getValueMeta(data.fieldnr).isString()) {
        throw new HopValueException(
            (BaseMessages.getString(
                PKG, "FieldSplitter.Log.SplitFieldNotValid", meta.getSplitField())));
      }

      // prepare the outputMeta
      //
      data.outputMeta = getInputRowMeta().clone();
      meta.getFields(data.outputMeta, getTransformName(), null, null, this, metadataProvider);

      // Now create objects to do string to data type conversion...
      //
      data.conversionMeta = data.outputMeta.cloneToType(IValueMeta.TYPE_STRING);

      data.delimiter = resolve(meta.getDelimiter());
      data.enclosure = resolve(meta.getEnclosure());
      data.escapeString = resolve(meta.getEscapeString());
    }

    // reserve room
    Object[] outputRow = RowDataUtil.allocateRowData(data.outputMeta.size());

    int nrExtraFields = meta.getFieldID().length - 1;

    System.arraycopy(r, 0, outputRow, 0, data.fieldnr);
    System.arraycopy(
        r,
        data.fieldnr + 1,
        outputRow,
        data.fieldnr + 1 + nrExtraFields,
        data.previousMeta.size() - (data.fieldnr + 1));

    // OK, now we have room in the middle to place the fields...
    //

    // Named values info.id[0] not filled in!
    final boolean selectFieldById =
        (meta.getFieldID().length > 0)
            && (meta.getFieldID()[0] != null)
            && (meta.getFieldID()[0].length() > 0);

    if (log.isDebug()) {
      if (selectFieldById) {
        logDebug(BaseMessages.getString(PKG, "FieldSplitter.Log.UsingIds"));
      } else {
        logDebug(BaseMessages.getString(PKG, "FieldSplitter.Log.UsingPositionOfValue"));
      }
    }

    String valueToSplit = data.previousMeta.getString(r, data.fieldnr);
    boolean removeEnclosure = getVariableBoolean(Const.HOP_SPLIT_FIELDS_REMOVE_ENCLOSURE, false);
    String[] valueParts =
        Const.splitString(
            valueToSplit, data.delimiter, data.enclosure, removeEnclosure, data.escapeString);
    int prev = 0;
    for (int i = 0; i < meta.getFieldsCount(); i++) {
      String rawValue = null;
      if (selectFieldById) {
        for (String part : valueParts) {
          if (part.startsWith(meta.getFieldID()[i])) {
            // Optionally remove the id
            if (meta.getFieldRemoveID()[i]) {
              rawValue = part.substring(meta.getFieldID()[i].length());
            } else {
              rawValue = part;
            }

            break;
          }
        }

        if (log.isDebug()) {
          logDebug(BaseMessages.getString(PKG, "FieldSplitter.Log.SplitInfo") + rawValue);
        }
      } else {
        rawValue = (valueParts == null || i >= valueParts.length) ? null : valueParts[i];
        prev += (rawValue == null ? 0 : rawValue.length()) + data.delimiter.length();

        if (log.isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG, "FieldSplitter.Log.SplitFieldsInfo", rawValue, String.valueOf(prev)));
        }
      }

      Object value;
      try {
        IValueMeta valueMeta = data.outputMeta.getValueMeta(data.fieldnr + i);
        IValueMeta conversionValueMeta = data.conversionMeta.getValueMeta(data.fieldnr + i);

        if (rawValue != null && valueMeta.isNull(rawValue)) {
          rawValue = null;
        }
        value =
            valueMeta.convertDataFromString(
                rawValue,
                conversionValueMeta,
                meta.getFieldNullIf()[i],
                meta.getFieldIfNull()[i],
                meta.getFieldTrimType()[i]);
      } catch (Exception e) {
        throw new HopValueException(
            BaseMessages.getString(
                PKG,
                "FieldSplitter.Log.ErrorConvertingSplitValue",
                rawValue,
                meta.getSplitField() + "]!"),
            e);
      }
      outputRow[data.fieldnr + i] = value;
    }

    return outputRow;
  }

  @Override
  public synchronized boolean processRow() throws HopException {

    Object[] r = getRow(); // get row from rowset, wait for our turn, indicate busy!
    if (r == null) {
      // no more input to be expected...
      setOutputDone();

      return false;
    }

    Object[] outputRowData = splitField(r);
    putRow(data.outputMeta, outputRowData);

    if (checkFeedback(getLinesRead())) {
      if (log.isBasic()) {
        logBasic(BaseMessages.getString(PKG, "FieldSplitter.Log.LineNumber") + getLinesRead());
      }
    }

    return true;
  }
}
