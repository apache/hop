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

package org.apache.hop.pipeline.transforms.stringcut;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Cut strings. */
public class StringCut extends BaseTransform<StringCutMeta, StringCutData> {
  private static final Class<?> PKG = StringCutMeta.class; // For Translator

  public StringCut(
      TransformMeta transformMeta,
      StringCutMeta meta,
      StringCutData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  private String cutString(String string, int cutFrom, int cutTo) {
    String rcode = string;

    if (!Utils.isEmpty(rcode)) {
      int lenCode = rcode.length();

      if ((cutFrom >= 0 && cutTo >= 0) && cutFrom > lenCode) {
        rcode = null;
      } else if ((cutFrom >= 0 && cutTo >= 0) && (cutTo < cutFrom)) {
        rcode = null;
      } else if ((cutFrom < 0 && cutTo < 0) && cutFrom < -lenCode) {
        rcode = null;
      } else if ((cutFrom < 0 && cutTo < 0) && (cutFrom < cutTo)) {
        rcode = null;
      } else {
        if (cutTo > lenCode) {
          cutTo = lenCode;
        }
        if (cutTo < 0 && cutFrom == 0 && (-cutTo) > lenCode) {
          cutTo = -(lenCode);
        }
        if (cutTo < 0 && cutFrom < 0 && (-cutTo) > lenCode) {
          cutTo = -(lenCode);
        }

        if (cutFrom >= 0 && cutTo > 0) {
          rcode = rcode.substring(cutFrom, cutTo);
        } else if (cutFrom < 0 && cutTo < 0) {
          rcode = rcode.substring(rcode.length() + cutTo, lenCode + cutFrom);
        } else if (cutFrom == 0 && cutTo < 0) {
          int intFrom = rcode.length() + cutTo;
          rcode = rcode.substring(intFrom, lenCode);
        }
      }
    }

    return rcode;
  }

  private Object[] getOneRow(IRowMeta rowMeta, Object[] row) throws HopException {
    Object[] rowData = new Object[data.outputRowMeta.size()];

    // Copy the input fields.
    System.arraycopy(row, 0, rowData, 0, rowMeta.size());
    int length = meta.getFields().size();

    int j = 0; // Index into "new fields" area, past the first {data.inputFieldsNr} records
    for (int i = 0; i < length; i++) {
      String valueIn = getInputRowMeta().getString(row, data.inStreamNrs[i]);
      String value = cutString(valueIn, data.cutFrom[i], data.cutTo[i]);
      if (Utils.isEmpty(data.outStreamNrs[i])) {
        rowData[data.inStreamNrs[i]] = value;
      } else {
        rowData[data.inputFieldsNr + j] = value;
        j++;
      }
    }
    return rowData;
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if (r == null) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      // What's the format of the output row?
      data.outputRowMeta = getInputRowMeta().clone();
      data.inputFieldsNr = data.outputRowMeta.size();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      data.inStreamNrs = new int[meta.getFields().size()];
      for (int i = 0; i < meta.getFields().size(); i++) {
        StringCutField scf = meta.getFields().get(i);
        data.inStreamNrs[i] = getInputRowMeta().indexOfValue(scf.getFieldInStream());
        if (data.inStreamNrs[i] < 0) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, "StringCut.Exception.FieldRequired", scf.getFieldInStream()));
        }

        // check field type
        if (getInputRowMeta().getValueMeta(data.inStreamNrs[i]).getType()
            != IValueMeta.TYPE_STRING) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, "StringCut.Exception.FieldTypeNotString", scf.getFieldInStream()));
        }
      }

      data.outStreamNrs = new String[meta.getFields().size()];
      for (int i = 0; i < meta.getFields().size(); i++) {
        StringCutField scf = meta.getFields().get(i);
        if (!StringUtils.isEmpty(scf.getFieldOutStream())) {
          data.outStreamNrs[i] = resolve(scf.getFieldOutStream());
        }
      }

      data.cutFrom = new int[meta.getFields().size()];
      data.cutTo = new int[meta.getFields().size()];

      for (int i = 0; i < meta.getFields().size(); i++) {
        StringCutField scf = meta.getFields().get(i);
        if (Utils.isEmpty(resolve(scf.getCutFrom()))) {
          data.cutFrom[i] = 0;
        } else {
          data.cutFrom[i] = Const.toInt(resolve(scf.getCutFrom()), 0);
        }

        if (Utils.isEmpty(resolve(scf.getCutTo()))) {
          data.cutTo[i] = 0;
        } else {
          data.cutTo[i] = Const.toInt(resolve(scf.getCutTo()), 0);
        }
      } // end for
    } // end if first

    try {
      Object[] output = getOneRow(getInputRowMeta(), r);
      putRow(data.outputRowMeta, output);

      if (checkFeedback(getLinesRead()) && log.isDetailed()) {

        logDetailed(BaseMessages.getString(PKG, "StringCut.Log.LineNumber") + getLinesRead());
      }
    } catch (HopException e) {
      boolean sendToErrorRow = false;
      String errorMessage = null;

      if (getTransformMeta().isDoingErrorHandling()) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError(BaseMessages.getString(PKG, "StringCut.Log.ErrorInTransform", e.getMessage()));
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if (sendToErrorRow) {
        // Simply add this row to the error row
        putError(getInputRowMeta(), r, 1, errorMessage, null, "StringCut001");
      }
    }
    return true;
  }

  @Override
  public boolean init() {
    boolean rCode = true;

    if (super.init()) {

      return rCode;
    }
    return false;
  }
}
