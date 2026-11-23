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

package org.apache.hop.pipeline.transforms.html2text;

import static java.lang.System.arraycopy;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.trim;
import static org.apache.hop.core.util.Utils.isEmpty;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Turns the HTML of a field into text, or into cleaned HTML. Like every transform, a copy reads,
 * converts and writes its rows on its own thread, in the order they come in: to convert on more
 * threads, run more copies.
 */
public class Html2Text extends BaseTransform<Html2TextMeta, Html2TextData> {
  public static final int OVER_ALLOCATE_SIZE = 10;
  private static final Class<?> PKG = Html2Text.class; // For Translator

  private static final String ERROR_CODE = "Html2Text001";

  public Html2Text(
      TransformMeta transformMeta,
      Html2TextMeta meta,
      Html2TextData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
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

      // get the RowMeta
      data.previousRowMeta = getInputRowMeta().clone();
      data.NrPrevFields = data.previousRowMeta.size();
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      if (isEmpty(meta.getHtmlField())) {
        throw new HopException(BaseMessages.getString(PKG, "Html2Text.Error.HtmlFieldMissing"));
      }
      if (isEmpty(meta.getOutputField())) {
        throw new HopException(BaseMessages.getString(PKG, "Html2Text.Error.OutputFieldMissing"));
      }
      // cache the position of the field
      cacheIndexPositions();

      // The parser, and the safelist and cleaner when cleaning, are made once
      data.converter =
          new HtmlConverter(meta.isCleanOnly(), meta.getSafelistType(), meta.isNormalisedText());
    } // End If first

    if (isRowLevel()) {
      logRowlevel(
          BaseMessages.getString(
              PKG,
              "Html2Text.LineNumber",
              getLinesRead() + " : " + getInputRowMeta().getString(r)));
    }

    try {
      putRow(data.outputRowMeta, outputRow(r, convert(r)));
    } catch (Exception e) {
      if (!getTransformMeta().isDoingErrorHandling()) {
        throw new HopException(
            BaseMessages.getString(PKG, "Html2Text.ErrorInTransformRunning") + e.getMessage(), e);
      }
      // Add this row to the error output of this transform
      putError(getInputRowMeta(), r, 1, e.toString(), meta.getHtmlField(), ERROR_CODE);
    }
    return true;
  }

  /** The text of the row's HTML; an empty text when the HTML is empty or null. */
  private String convert(Object[] inputRow) throws HopException {
    String html = trim(getInputRowMeta().getString(inputRow, data.indexOfHtmlField));
    return isBlank(html) ? "" : data.converter.convert(html);
  }

  private void cacheIndexPositions() throws HopException {
    if (data.indexOfHtmlField < 0) {
      data.indexOfHtmlField = data.previousRowMeta.indexOfValue(meta.getHtmlField());
      if (data.indexOfHtmlField < 0) {
        // The field is unreachable !
        throw new HopException(
            BaseMessages.getString(
                PKG, "Html2Text.Exception.CouldnotFindField", meta.getHtmlField()));
      }
    }
    if (data.indexOfOutputField < 0) {
      data.indexOfOutputField = data.outputRowMeta.indexOfValue(meta.getOutputField());
      if (data.indexOfOutputField < 0) {
        // The field is unreachable !
        throw new HopException(
            BaseMessages.getString(
                PKG, "Html2Text.Exception.CouldnotFindField", meta.getOutputField()));
      }
    }
  }

  private Object[] outputRow(Object[] inputRow, String output) {
    Object[] outputRow = new Object[inputRow.length + 1 + OVER_ALLOCATE_SIZE];
    arraycopy(inputRow, 0, outputRow, 0, inputRow.length);
    outputRow[data.indexOfOutputField] = output;
    return outputRow;
  }
}
