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
import static java.util.concurrent.ForkJoinPool.commonPool;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.trim;
import static org.apache.hop.core.util.Utils.isEmpty;
import static org.jsoup.Jsoup.clean;
import static org.jsoup.Jsoup.parseBodyFragment;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.html2text.Html2TextMeta.SafelistType;
import org.jsoup.nodes.Document;
import org.jsoup.safety.Safelist;

public class Html2Text extends BaseTransform<Html2TextMeta, Html2TextData> {
  public static final int OVER_ALLOCATE_SIZE = 10;
  private static final Class<?> PKG = Html2Text.class; // For Translator
  private SafelistType safelistType;

  private final int cores = Runtime.getRuntime().availableProcessors();
  private final int maxJobs = cores * 100;
  private final ExecutorService executor = commonPool();
  private final AtomicInteger jobs = new AtomicInteger(0);

  private final Object lock = new Object();

  public Html2Text(
      TransformMeta transformMeta,
      Html2TextMeta meta,
      Html2TextData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  private void finish() {
    // Wait until all jobs are finished.
    synchronized (lock) {
      while (jobs.get() > 0) {
        try {
          lock.wait();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
    setOutputDone();
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if (r == null) { // no more input to be expected...
      finish();
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

      this.safelistType = SafelistType.valueOf(meta.getSafelistType());
    } // End If first

    boolean sendToErrorRow = false;
    String errorMessage = null;

    process(r);

    try {
      if (isRowLevel()) {
        logRowlevel(
            BaseMessages.getString(
                PKG,
                "Html2Text.LineNumber",
                getLinesRead() + " : " + getInputRowMeta().getString(r)));
      }
    } catch (Exception e) {
      if (getTransformMeta().isDoingErrorHandling()) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError(BaseMessages.getString(PKG, "Html2Text.ErrorInTransformRunning") + e.getMessage());
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if (sendToErrorRow) {
        // Simply add this row to the error row
        putError(getInputRowMeta(), r, 1, errorMessage, meta.getHtmlField(), "Html2Text001");
      }
    }

    return true;
  }

  private void process(Object[] inputRow) throws HopTransformException {
    String html = trim((String) inputRow[data.indexOfHtmlField]);
    if (isBlank(html)) {
      return;
    }

    if (meta.isParallelism()) {
      processAsync(html, inputRow);
    } else {
      processSync(html, inputRow);
    }
  }

  private void processAsync(String html, Object[] inputRow) {
    synchronized (lock) {
      while (jobs.get() > maxJobs) {
        try {
          lock.wait();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    executor.submit(
        () -> {
          try {
            processSync(html, inputRow);
            jobs.decrementAndGet();

            synchronized (lock) {
              lock.notifyAll();
            }
          } catch (Exception e) {
            logError(e.getMessage(), e);
          }
        });

    jobs.incrementAndGet();
  }

  private void processSync(String html, Object[] inputRow) throws HopTransformException {

    Document document = parseBodyFragment(html);

    if (meta.isCleanOnly()) {
      Safelist safelist;
      switch (safelistType) {
        case basic -> safelist = Safelist.basic();
        case simpleText -> safelist = Safelist.simpleText();
        case basicWithImages -> safelist = Safelist.basicWithImages();
        case none -> safelist = Safelist.none();
        case relaxed -> safelist = Safelist.relaxed();
        default -> safelist = Safelist.basic();
      }

      processOutput(clean(document.html(), safelist), inputRow);
    } else {
      var text = meta.isNormalisedText() ? document.text() : document.wholeText();
      processOutput(text, inputRow);
    }
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

  private void processOutput(String output, Object[] inputRow) throws HopTransformException {
    Object[] outputRow = new Object[inputRow.length + 1 + OVER_ALLOCATE_SIZE];
    arraycopy(inputRow, 0, outputRow, 0, inputRow.length);

    outputRow[data.indexOfOutputField] = output;
    putRow(data.outputRowMeta, outputRow);
  }

  @Override
  public void putRow(IRowMeta rowMeta, Object[] row) throws HopTransformException {
    synchronized (this) {
      super.putRow(rowMeta, row);
    }
  }
}
