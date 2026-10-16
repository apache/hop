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

package org.apache.hop.pipeline.transforms.stringoperations;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.ValueDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.jetbrains.annotations.Nullable;

/** Apply certain operations to string. */
public class StringOperations extends BaseTransform<StringOperationsMeta, StringOperationsData> {
  private static final Class<?> PKG = StringOperationsMeta.class;

  public StringOperations(
      TransformMeta transformMeta,
      StringOperationsMeta meta,
      StringOperationsData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  private String processString(
      String string, StringOperationsMeta.StringOperation operation, int operationNr) {
    String processed = processStringTrim(operation.getTrimType(), string);
    processed = processStringLowerUpper(operation.getLowerUpper(), processed);
    processed =
        processStringPadding(
            operation.getPaddingType(),
            data.padChars[operationNr],
            data.padLens[operationNr],
            processed);
    processed = processStringInitCap(operation.getInitCap(), processed);
    processed = processStringMaskXml(operation.getMaskXml(), processed);
    processed = processStringDigits(operation.getDigits(), processed);
    processed = processStringRemoveSpecialCharacters(operation.getRemoveSpecialChars(), processed);

    return processed;
  }

  private static @Nullable String processStringRemoveSpecialCharacters(
      StringOperationsMeta.RemoveSpecialChars removeSpecialCharacters, String string) {

    if (!Utils.isEmpty(string)) {
      return switch (removeSpecialCharacters) {
        case NONE -> string;
        case CR -> Const.removeCR(string);
        case LF -> Const.removeLF(string);
        case CRLF -> Const.removeCRLF(string);
        case TAB -> Const.removeTAB(string);
        case SPACE -> string.replace(" ", "");
      };
    }
    return string;
  }

  private static @Nullable String processStringDigits(
      StringOperationsMeta.Digits digits, String string) {
    if (!Utils.isEmpty(string)) {
      return switch (digits) {
        case NONE -> string;
        case DIGITS_ONLY -> Const.getDigitsOnly(string);
        case DIGITS_REMOVE -> Const.removeDigits(string);
      };
    }
    return string;
  }

  private static @Nullable String processStringMaskXml(
      StringOperationsMeta.MaskXml maskXml, String string) {
    if (!Utils.isEmpty(string)) {
      return switch (maskXml) {
        case NONE -> string;
        case ESCAPE_XML -> Const.escapeXml(string);
        case CDATA -> Const.protectXmlCdata(string);
        case UNESCAPE_XML -> Const.unEscapeXml(string);
        case ESCAPE_HTML -> Const.escapeHtml(string);
        case UNESCAPE_HTML -> Const.unEscapeHtml(string);
        case ESCAPE_SQL -> Const.escapeSql(string);
      };
    }
    return string;
  }

  private static @Nullable String processStringInitCap(
      StringOperationsMeta.InitCap iniCap, String string) {
    if (!Utils.isEmpty(string)) {
      return switch (iniCap) {
        case NO -> string;
        case YES -> ValueDataUtil.initCap(string);
      };
    }
    return string;
  }

  private static @Nullable String processStringPadding(
      StringOperationsMeta.Padding padType, String padChar, int padLen, String string) {
    if (!Utils.isEmpty(string)) {
      return switch (padType) {
        case LEFT -> Const.lpad(string, padChar, padLen);
        case RIGHT -> Const.rpad(string, padChar, padLen);
        case NONE -> string;
      };
    }
    return string;
  }

  private static @Nullable String processStringLowerUpper(
      StringOperationsMeta.LowerUpper lowerUpper, String string) {
    if (!Utils.isEmpty(string)) {
      return switch (lowerUpper) {
        case NONE -> string;
        case LOWER -> string.toLowerCase();
        case UPPER -> string.toUpperCase();
      };
    }
    return string;
  }

  private static @Nullable String processStringTrim(
      StringOperationsMeta.TrimType trimType, String string) {
    if (!Utils.isEmpty(string)) {
      return switch (trimType) {
        case RIGHT -> Const.rtrim(string);
        case LEFT -> Const.ltrim(string);
        case BOTH -> Const.trim(string);
        case NONE -> string;
      };
    }
    return string;
  }

  private Object[] performStringOperations(Object[] row) throws HopException {
    Object[] rowData = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());

    int j = 0; // Index into "new fields" area, past the first {data.inputFieldsNr} records
    for (int i = 0; i < meta.getOperations().size(); i++) {
      StringOperationsMeta.StringOperation operation = meta.getOperations().get(i);
      if (data.inStreamNrs[i] >= 0) {
        // Get source value
        String value = getInputRowMeta().getString(row, data.inStreamNrs[i]);
        // Apply String operations and return result value
        value = processString(value, operation, i);
        if (Utils.isEmpty(operation.getFieldOutStream())) {
          // Update field
          rowData[data.inStreamNrs[i]] = value;
          data.outputRowMeta
              .getValueMeta(data.inStreamNrs[i])
              .setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
        } else {
          // create a new Field
          rowData[getInputRowMeta().size() + j] = value;
          j++;
        }
      }
    }
    return rowData;
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if (r == null) {
      // no more input to be expected...
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      firstProcessRow();
    }

    try {
      Object[] output = performStringOperations(r);
      putRow(data.outputRowMeta, output);

      if (checkFeedback(getLinesRead()) && isDetailed()) {
        logDetailed(
            BaseMessages.getString(PKG, "StringOperations.Log.LineNumber") + getLinesRead());
      }
    } catch (HopException e) {
      String errorMessage;

      if (getTransformMeta().isDoingErrorHandling()) {
        errorMessage = e.toString();
      } else {
        logError(
            BaseMessages.getString(PKG, "StringOperations.Log.ErrorInTransform", e.getMessage()));
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      // Simply add this row to the error row
      putError(getInputRowMeta(), r, 1, errorMessage, null, "StringOperations001");
    }
    return true;
  }

  private void firstProcessRow() throws HopTransformException {
    // What's the format of the output row?
    data.outputRowMeta = getInputRowMeta().clone();
    meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
    data.inStreamNrs = new int[meta.getOperations().size()];
    data.padChars = new String[meta.getOperations().size()];
    data.padLens = new int[meta.getOperations().size()];
    for (int i = 0; i < meta.getOperations().size(); i++) {
      StringOperationsMeta.StringOperation operation = meta.getOperations().get(i);
      // Resolve and parse the padding once, not for every row
      data.padChars[i] = resolve(operation.getPadChar());
      data.padLens[i] = Const.toInt(resolve(operation.getPadLen()), -1);
      data.inStreamNrs[i] = getInputRowMeta().indexOfValue(operation.getFieldInStream());
      if (data.inStreamNrs[i] < 0) { // couldn't find field!
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "StringOperations.Exception.FieldRequired", operation.getFieldInStream()));
      }
      // check field type
      if (!getInputRowMeta().getValueMeta(data.inStreamNrs[i]).isString()) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG,
                "StringOperations.Exception.FieldTypeNotString",
                operation.getFieldInStream()));
      }
    }
  }
}
