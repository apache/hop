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

package org.apache.hop.pipeline.transforms.splitfieldtorows;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class SplitFieldToRows extends BaseTransform<SplitFieldToRowsMeta, SplitFieldToRowsData> {
  private static final Class<?> PKG = SplitFieldToRowsMeta.class;

  public SplitFieldToRows(
      TransformMeta transformMeta,
      SplitFieldToRowsMeta meta,
      SplitFieldToRowsData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  private boolean splitField(IRowMeta rowMeta, Object[] rowData) throws HopException {
    if (first) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      String realSplitFieldName = resolve(meta.getSplitField());
      data.fieldnr = rowMeta.indexOfValue(realSplitFieldName);

      int numErrors = 0;
      if (Utils.isEmpty(meta.getNewFieldname())) {
        logError(BaseMessages.getString(PKG, "SplitFieldToRows.Log.NewFieldNameIsNull"));
        numErrors++;
      }

      if (data.fieldnr < 0) {
        logError(
            BaseMessages.getString(
                PKG, "SplitFieldToRows.Log.CouldNotFindFieldToSplit", realSplitFieldName));
        numErrors++;
      }

      if (!rowMeta.getValueMeta(data.fieldnr).isString()) {
        logError(
            BaseMessages.getString(
                PKG, "SplitFieldToRows.Log.SplitFieldNotValid", realSplitFieldName));
        numErrors++;
      }

      if (meta.isIncludeRowNumber()) {
        String realRowNumberField = resolve(meta.getRowNumberField());
        if (Utils.isEmpty(realRowNumberField)) {
          logError(BaseMessages.getString(PKG, "SplitFieldToRows.Exception.RownrFieldMissing"));
          numErrors++;
        }
      }

      if (numErrors > 0) {
        setErrors(numErrors);
        stopAll();
        return false;
      }

      data.splitMeta = rowMeta.getValueMeta(data.fieldnr);
    }

    String originalString = data.splitMeta.getString(rowData[data.fieldnr]);
    if (originalString == null) {
      originalString = "";
    }

    if (meta.isIncludeRowNumber() && meta.isResetRowNumber()) {
      data.rownr = 1L;
    }

    String[] splitStrings = splitSource(originalString);
    for (String string : splitStrings) {
      Object[] outputRow = RowDataUtil.createResizedCopy(rowData, data.outputRowMeta.size());
      outputRow[rowMeta.size()] = string;
      // Include row number in output?
      if (meta.isIncludeRowNumber()) {
        outputRow[rowMeta.size() + 1] = data.rownr;
      }
      putRow(data.outputRowMeta, outputRow);
      data.rownr++;
    }

    return true;
  }

  /**
   * Split using enclosure-aware parsing when an enclosure is set and the delimiter is not a regular
   * expression. Otherwise keep the historical Pattern.split behavior, including trailing empty
   * values.
   */
  private String[] splitSource(String originalString) {
    if (Utils.isEmpty(data.enclosure) || meta.isIsDelimiterRegex()) {
      // use -1 to include trailing empty strings
      return data.delimiterPattern.split(originalString, -1);
    }
    return splitWithEnclosure(originalString);
  }

  /**
   * Split on the delimiter, ignoring delimiters inside enclosures. Doubled enclosures inside an
   * enclosed value are kept as one literal enclosure. Trailing empty values are preserved, matching
   * the non-enclosure behaviour.
   */
  private String[] splitWithEnclosure(String source) {
    String delimiter = data.delimiter;
    String enclosure = data.enclosure;
    List<String> values = new ArrayList<>();
    StringBuilder value = new StringBuilder();
    boolean inEnclosure = false;
    int index = 0;
    while (index < source.length()) {
      if (source.startsWith(enclosure, index)) {
        if (inEnclosure && source.startsWith(enclosure, index + enclosure.length())) {
          value.append(enclosure);
          index += 2 * enclosure.length();
        } else {
          inEnclosure = !inEnclosure;
          index += enclosure.length();
        }
      } else if (!inEnclosure && !delimiter.isEmpty() && source.startsWith(delimiter, index)) {
        values.add(value.toString());
        value.setLength(0);
        index += delimiter.length();
      } else {
        value.append(source.charAt(index));
        index++;
      }
    }
    if (inEnclosure) {
      logError(BaseMessages.getString(PKG, "SplitFieldToRows.Log.UnterminatedEnclosure", source));
    }
    values.add(value.toString());
    return values.toArray(new String[0]);
  }

  @Override
  public synchronized boolean processRow() throws HopException {

    Object[] r = getRow(); // get row from rowset, wait for our turn, indicate busy!
    if (r == null) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    boolean ok = splitField(getInputRowMeta(), r);
    if (!ok) {
      setOutputDone();
      return false;
    }

    if (checkFeedback(getLinesRead()) && isDetailed()) {
      logBasic(BaseMessages.getString(PKG, "SplitFieldToRows.Log.LineNumber") + getLinesRead());
    }

    return true;
  }

  @Override
  public boolean init() {

    if (super.init()) {
      data.rownr = 1L;

      try {
        data.delimiter = resolve(Const.nullToEmpty(meta.getDelimiter()));
        data.enclosure = resolve(Const.NVL(meta.getEnclosure(), ""));
        if (meta.isIsDelimiterRegex()) {
          data.delimiterPattern = Pattern.compile(data.delimiter);
        } else {
          data.delimiterPattern = Pattern.compile(Pattern.quote(data.delimiter));
        }
      } catch (PatternSyntaxException pse) {
        logError(pse.getMessage());
        throw pse;
      }

      return true;
    }
    return false;
  }
}
