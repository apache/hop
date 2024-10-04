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

package org.apache.hop.pipeline.transforms.replacestring;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Search and replace in string. */
public class ReplaceString extends BaseTransform<ReplaceStringMeta, ReplaceStringData> {

  private static final Class<?> PKG = ReplaceStringMeta.class;

  public ReplaceString(
      TransformMeta transformMeta,
      ReplaceStringMeta meta,
      ReplaceStringData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  public static String replaceString(
      String originalString, Pattern pattern, String replaceByString) {
    if (originalString == null) {
      return null;
    }
    final Matcher matcher = pattern.matcher(originalString);

    if (replaceByString == null) {
      if (matcher.matches()) {
        return null;
      } else {
        return originalString;
      }
    } else {
      return matcher.replaceAll(replaceByString);
    }
  }

  @VisibleForTesting
  static Pattern buildPattern(
      boolean literalParsing,
      boolean caseSensitive,
      boolean wholeWord,
      String patternString,
      boolean isUnicode) {
    int flags = 0;
    if (literalParsing && !wholeWord) {
      flags |= Pattern.LITERAL;
    }
    if (!caseSensitive) {
      flags |= Pattern.CASE_INSENSITIVE;
    }
    if (isUnicode) {
      flags |= Pattern.UNICODE_CHARACTER_CLASS;
    }

    /*
     * XXX: I don't like this parameter. I think it would almost always be better for the user to define either word
     * boundaries or ^/$ anchors explicitly in their pattern.
     */
    if (wholeWord) {
      if (literalParsing) {
        patternString = "\\Q" + patternString + "\\E";
      }
      patternString = "\\b" + patternString + "\\b";
    }

    return Pattern.compile(patternString, flags);
  }

  private String getResolvedReplaceByString(int index, Object[] row) throws HopException {

    if (data.setEmptyString[index]) {
      // return empty string rather than null value
      return StringUtil.EMPTY_STRING;
    }

    // if there is something in the original replaceByString, then use it.
    if (data.replaceFieldIndex[index] == -1) {
      return data.replaceByString[index];
    }

    String str = getInputRowMeta().getString(row, data.replaceFieldIndex[index]);

    // Escape the regex pattern backslash
    if (str != null) {
      str = str.replace("\\", "\\\\");
    }
    return str;
  }

  synchronized Object[] handleOneRow(IRowMeta rowMeta, Object[] row) throws HopException {
    Object[] rowData = RowDataUtil.resizeArray(row, data.outputRowMeta.size());
    int index = 0;
    Set<Integer> numFieldsAlreadyBeenTransformed = new HashSet<>();
    for (int i = 0; i < data.numFields; i++) {

      IRowMeta currentRowMeta =
          (numFieldsAlreadyBeenTransformed.contains(data.inStreamNrs[i]))
              ? data.outputRowMeta
              : getInputRowMeta();
      String value =
          replaceString(
              currentRowMeta.getString(rowData, data.inStreamNrs[i]),
              data.patterns[i],
              getResolvedReplaceByString(i, row));

      if (Utils.isEmpty(data.outStreamNrs[i])) {
        // update field value
        rowData[data.inStreamNrs[i]] = value;
        numFieldsAlreadyBeenTransformed.add(data.inStreamNrs[i]);
      } else {
        // add new field value
        rowData[data.inputFieldsNr + index++] = value;
      }
    }
    return rowData;
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] r = getRow();
    if (r == null) {
      // There is no more input to be expected.
      //
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;

      // What's the format of the output row?
      data.outputRowMeta = getInputRowMeta().clone();
      data.inputFieldsNr = data.outputRowMeta.size();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      data.numFields = meta.getFields().size();
      data.inStreamNrs = new int[data.numFields];
      data.outStreamNrs = new String[data.numFields];
      data.patterns = new Pattern[data.numFields];
      data.replaceByString = new String[data.numFields];
      data.setEmptyString = new boolean[data.numFields];
      data.replaceFieldIndex = new int[data.numFields];

      for (int i = 0; i < data.numFields; i++) {
        ReplaceStringMeta.RSField field = meta.getFields().get(i);
        data.inStreamNrs[i] = getInputRowMeta().indexOfValue(field.getFieldInStream());
        if (data.inStreamNrs[i] < 0) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, "ReplaceString.Exception.FieldRequired", field.getFieldInStream()));
        }

        // check field type
        if (getInputRowMeta().getValueMeta(data.inStreamNrs[i]).getType()
            != IValueMeta.TYPE_STRING) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, "ReplaceString.Exception.FieldTypeNotString", field.getFieldInStream()));
        }

        data.outStreamNrs[i] = resolve(field.getFieldOutStream());

        data.patterns[i] =
            buildPattern(
                !field.isUsingRegEx(),
                field.isCaseSensitive(),
                field.isReplacingWholeWord(),
                resolve(field.getReplaceString()),
                field.isUnicode());

        String replaceField = field.getReplaceFieldByString();
        if (StringUtils.isNotEmpty(replaceField)) {
          data.replaceFieldIndex[i] = getInputRowMeta().indexOfValue(replaceField);
          if (data.replaceFieldIndex[i] < 0) {
            throw new HopTransformException(
                BaseMessages.getString(
                    PKG, "ReplaceString.Exception.FieldRequired", field.getFieldInStream()));
          }
        } else {
          data.replaceFieldIndex[i] = -1;
          // Escape the regex pattern backslash
          data.replaceByString[i] =
              Const.NVL(resolve(field.getReplaceByString()), "").replace("\\", "\\\\");
        }
        data.setEmptyString[i] = field.isSettingEmptyString();
      }
    } // end if first

    try {
      Object[] output = handleOneRow(getInputRowMeta(), r);
      putRow(data.outputRowMeta, output);

      if (checkFeedback(getLinesRead()) && isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "ReplaceString.Log.LineNumber") + getLinesRead());
      }
    } catch (HopException e) {
      if (getTransformMeta().isDoingErrorHandling()) {
        putError(getInputRowMeta(), r, 1, e.toString(), null, "ReplaceString001");
      } else {
        logError(BaseMessages.getString(PKG, "ReplaceString.Log.ErrorInTransform", e.getMessage()));
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
    }
    return true;
  }

  @Override
  public void dispose() {
    data.outStreamNrs = null;
    data.patterns = null;
    data.replaceByString = null;
    data.replaceString = null;
    data.valueChange = null;
    super.dispose();
  }
}
