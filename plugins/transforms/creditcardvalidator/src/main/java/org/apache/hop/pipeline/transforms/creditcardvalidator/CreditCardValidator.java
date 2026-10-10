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

package org.apache.hop.pipeline.transforms.creditcardvalidator;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Check if a Credit Card is valid * */
public class CreditCardValidator
    extends BaseTransform<CreditCardValidatorMeta, CreditCardValidatorData> {

  private static final Class<?> PKG = CreditCardValidatorMeta.class;

  public CreditCardValidator(
      TransformMeta transformMeta,
      CreditCardValidatorMeta meta,
      CreditCardValidatorData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    boolean sendToErrorRow;
    String errorMessage;

    Object[] row = getRow();
    if (row == null) {
      setOutputDone();
      return false;
    }

    boolean isValid;
    String cardType = null;
    String unValid = null;

    if (first) {
      first = false;

      // get the RowMeta
      data.previousRowMeta = getInputRowMeta().clone();
      data.NrPrevFields = data.previousRowMeta.size();
      data.outputRowMeta = data.previousRowMeta;
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      // Check if field is provided
      if (Utils.isEmpty(meta.getFieldName())) {
        logError(BaseMessages.getString(PKG, "CreditCardValidator.Error.CardFieldMissing"));
        throw new HopException(
            BaseMessages.getString(PKG, "CreditCardValidator.Error.CardFieldMissing"));
      }

      // cache the position of the field
      if (data.indexOfField < 0) {
        data.indexOfField = getInputRowMeta().indexOfValue(meta.getFieldName());
        if (data.indexOfField < 0) {
          // The field is unreachable !
          throw new HopException(
              BaseMessages.getString(
                  PKG, "CreditCardValidator.Exception.CouldnotFindField", meta.getFieldName()));
        }
      }
      data.realResultFieldname = resolve(meta.getResultFieldName());
      if (Utils.isEmpty(data.realResultFieldname)) {
        throw new HopException(
            BaseMessages.getString(PKG, "CreditCardValidator.Exception.ResultFieldMissing"));
      }
      data.realCardTypeFieldname = resolve(meta.getCardType());
      data.realNotValidMsgFieldname = resolve(meta.getNotValidMessage());
      data.outputFieldMetas.clear();
      if (meta.isUseBinDatabase()) {
        for (BinOutputField outputField : meta.getOutputFields()) {
          String realName = resolve(outputField.getName());
          if (!Utils.isEmpty(realName)) {
            try {
              data.outputFieldMetas.add(outputField.createValueMeta(realName));
            } catch (Exception e) {
              throw new HopException(e);
            }
          }
        }
      }
    } // End If first

    Object[] outputRow = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
    try {
      // get field
      String fieldValue = getInputRowMeta().getString(row, data.indexOfField);
      if (meta.isOnlyDigits()) {
        fieldValue = Const.getDigitsOnly(fieldValue);
      }

      ReturnIndicator rt;
      if (data.binDatabase != null) {
        rt = CreditCardVerifier.checkCC(fieldValue, data.binDatabase);
      } else {
        rt = CreditCardVerifier.checkCC(fieldValue);
      }

      // Check if Card is Valid?
      isValid = rt.CardValid;
      // include Card Type?
      if (!Utils.isEmpty(data.realCardTypeFieldname)) {
        cardType = rt.CardType;
      }
      // include Not valid message?
      if (!Utils.isEmpty(data.realNotValidMsgFieldname)) {
        unValid = rt.UnValidMsg;
      }

      // add card is Valid
      outputRow[data.NrPrevFields] = isValid;
      int rowIndex = data.NrPrevFields;
      rowIndex++;

      // add card type?
      if (!Utils.isEmpty(data.realCardTypeFieldname)) {
        outputRow[rowIndex++] = cardType;
      }
      // add not valid message?
      if (!Utils.isEmpty(data.realNotValidMsgFieldname)) {
        outputRow[rowIndex++] = unValid;
      }
      // add extra output fields?
      if (meta.isUseBinDatabase() && rt.extraValues != null) {
        for (int i = 0; i < meta.getOutputFields().size(); i++) {
          BinOutputField outputField = meta.getOutputFields().get(i);
          String realName = resolve(outputField.getName());
          if (!Utils.isEmpty(realName)) {
            String value = rt.extraValues.get(realName);
            IValueMeta valueMeta = data.outputFieldMetas.get(i);
            outputRow[rowIndex++] = convertValue(valueMeta, value);
          }
        }
      }

      // add new values to the row.
      putRow(data.outputRowMeta, outputRow); // copy row to output rowset(s)
      if (isRowLevel()) {
        logRowlevel(
            BaseMessages.getString(
                PKG,
                "CreditCardValidator.LineNumber",
                getLinesRead() + " : " + data.outputRowMeta.getString(outputRow)));
      }

    } catch (Exception e) {
      if (getTransformMeta().isDoingErrorHandling()) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError(
            BaseMessages.getString(PKG, "CreditCardValidator.ErrorInTransformRunning")
                + e.getMessage());
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if (sendToErrorRow) {
        // Simply add this row to the error row
        putError(
            getInputRowMeta(),
            row,
            1,
            errorMessage,
            meta.getResultFieldName(),
            "CreditCardValidator001");
      }
    }

    return true;
  }

  private Object convertValue(IValueMeta valueMeta, String value) throws Exception {
    if (value == null) {
      return null;
    }
    return valueMeta.convertDataFromString(
        value, new ValueMetaString(valueMeta.getName()), null, null, valueMeta.getTrimType());
  }

  @Override
  public boolean init() {
    if (super.init()) {
      if (Utils.isEmpty(meta.getResultFieldName())) {
        logError(BaseMessages.getString(PKG, "CreditCardValidator.Error.ResultFieldMissing"));
        return false;
      }
      if (meta.isUseBinDatabase()) {
        String fileName = resolve(meta.getBinFileName());
        if (Utils.isEmpty(fileName)) {
          logError(BaseMessages.getString(PKG, "CreditCardValidator.Error.BinFileNameMissing"));
          return false;
        }
        try {
          data.binDatabase = new BinDatabase();
          data.binDatabase.load(
              this,
              fileName,
              resolve(meta.getBinCsvColumn()),
              meta.getOutputFields(),
              resolve(meta.getBinDelimiter()),
              resolve(meta.getBinEnclosure()),
              resolve(meta.getBinEncoding()),
              meta.isBinHeaderPresent());
          if (data.binDatabase.getSkippedRows() > 0) {
            logBasic(
                BaseMessages.getString(
                    PKG,
                    "CreditCardValidator.Log.SkippedBinRows",
                    data.binDatabase.getSkippedRows()));
          }
        } catch (HopException e) {
          logError(
              BaseMessages.getString(PKG, "CreditCardValidator.Error.BinDatabaseLoad")
                  + e.getMessage());
          return false;
        }
      }
      return true;
    }
    return false;
  }
}
