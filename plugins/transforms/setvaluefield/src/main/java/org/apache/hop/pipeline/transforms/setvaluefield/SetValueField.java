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

package org.apache.hop.pipeline.transforms.setvaluefield;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;

/** Set value field with another value field. */
public class SetValueField extends BaseTransform<SetValueFieldMeta, SetValueFieldData>
    implements ITransform<SetValueFieldMeta, SetValueFieldData> {
  private static final Class<?> PKG = SetValueFieldMeta.class; // For Translator

  public SetValueField(
      TransformMeta transformMeta,
      SetValueFieldMeta meta,
      SetValueFieldData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    // Get one row from one of the rowsets...
    Object[] r = getRow();

    if (r == null) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      // What's the format of the output row?
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      List<SetField> fields = meta.getFields();

      data.indexOfField = new int[fields.size()];
      data.indexOfReplaceByValue = new int[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        SetField field = fields.get(i);
        // Check if this field was specified only one time
        for (int j = 0; j < fields.size(); j++) {
          if (j != i && field.equals(fields.get(j))) {
            throw new HopException(
                BaseMessages.getString(
                    PKG,
                    "SetValueField.Log.FieldSpecifiedMoreThatOne",
                    field.getFieldName(),
                    "" + i,
                    "" + j));
          }
        }

        data.indexOfField[i] = data.outputRowMeta.indexOfValue(resolve(field.getFieldName()));
        if (data.indexOfField[i] < 0) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, "SetValueField.Log.CouldNotFindFieldInRow", field.getFieldName()));
        }
        String sourceField = resolve(field.getReplaceByField());
        if (Utils.isEmpty(sourceField)) {
          throw new HopTransformException(
              BaseMessages.getString(PKG, "SetValueField.Log.ReplaceByValueFieldMissing", "" + i));
        }
        data.indexOfReplaceByValue[i] = data.outputRowMeta.indexOfValue(sourceField);
        if (data.indexOfReplaceByValue[i] < 0) {
          throw new HopTransformException(
              BaseMessages.getString(PKG, "SetValueField.Log.CouldNotFindFieldInRow", sourceField));
        }
        // Compare fields type
        IValueMeta valueMeta = getInputRowMeta().getValueMeta(data.indexOfField[i]);
        IValueMeta replaceByValueMeta =
            getInputRowMeta().getValueMeta(data.indexOfReplaceByValue[i]);

        if (valueMeta.getType() != replaceByValueMeta.getType()) {
          String err =
              BaseMessages.getString(
                  PKG,
                  "SetValueField.Log.FieldsTypeDifferent",
                  valueMeta.getName() + " (" + valueMeta.getTypeDesc() + ")",
                  replaceByValueMeta.getName() + " (" + replaceByValueMeta.getTypeDesc() + ")");
          throw new HopTransformException(err);
        }
      }
    }
    try {
      for (int i = 0; i < data.indexOfField.length; i++) {
        r[data.indexOfField[i]] = r[data.indexOfReplaceByValue[i]];
      }
      putRow(data.outputRowMeta, r); // copy row to output rowset(s)
    } catch (HopException e) {
      boolean sendToErrorRow = false;
      String errorMessage = null;

      if (getTransformMeta().isDoingErrorHandling()) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError(BaseMessages.getString(PKG, "SetValueField.Log.ErrorInTransform", e.getMessage()));
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if (sendToErrorRow) {
        // Simply add this row to the error row
        putError(data.outputRowMeta, r, 1, errorMessage, null, "SetValueField001");
      }
    }
    return true;
  }

  @Override
  public void dispose() {

    super.dispose();
  }

  @Override
  public boolean init() {

    if (super.init()) {
      // Add init code here.
      return true;
    }
    return false;
  }
}
