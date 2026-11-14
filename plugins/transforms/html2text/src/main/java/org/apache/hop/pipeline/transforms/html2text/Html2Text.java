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
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.jsoup.nodes.Document;
import org.jsoup.safety.Safelist;

public class Html2Text extends BaseTransform<Html2TextMeta, Html2TextData> {
  public static final int OVER_ALLOCATE_SIZE = 10;
  private static final Class<?> PKG = Html2Text.class; // For Translator

  private final int cores = Runtime.getRuntime().availableProcessors();
  private final int maxJobs = cores * 100;
  private final ExecutorService executor = commonPool();

  /** How long draining waits for accepted asynchronous work before abandoning it. */
  private static final long DRAIN_TIMEOUT_MILLIS = 30_000L;

  // Guarded by lock, including reservation before submission.
  private int jobs;
  private Throwable failure;
  // Guards against surfacing the same asynchronous failure twice (finish/checkFailure and dispose).
  private boolean failureReported;

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

  private void awaitJobs() {
    awaitJobs(DRAIN_TIMEOUT_MILLIS);
  }

  private void awaitJobs(final long timeoutMillis) {
    boolean interrupted = false;
    final long deadline = System.currentTimeMillis() + timeoutMillis;
    synchronized (lock) {
      long remaining = timeoutMillis;
      while (jobs > 0 && remaining > 0) {
        try {
          lock.wait(remaining);
        } catch (InterruptedException e) {
          // Accepted work should finish before output is closed or resources are disposed, but an
          // interrupted waiter must not hold up the transform or the pipeline.
          interrupted = true;
        }
        remaining = deadline - System.currentTimeMillis();
      }
      if (jobs > 0) {
        // A worker is stuck and no provider timeout applies. Do not block disposal forever: log and
        // abandon the remaining jobs.
        logBasic(
            "Abandoning "
                + jobs
                + " asynchronous job(s) that did not finish within "
                + timeoutMillis
                + " ms");
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Wait for every accepted job to complete on the normal end-of-input path. Unlike {@link
   * #awaitJobs(long)} there is no timeout: rows that are merely slow must not be lost because the
   * output is closed while workers still run. A stop (see {@link #stopRunning()}) breaks the wait.
   */
  private void awaitJobsUncapped() {
    boolean interrupted = false;
    synchronized (lock) {
      while (jobs > 0 && !isStopped()) {
        try {
          lock.wait(1_000L);
        } catch (InterruptedException e) {
          // Accepted work should finish before output is closed, but an interrupted waiter must
          // not hold up the transform or the pipeline.
          interrupted = true;
        }
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private void checkFailure() throws HopException {
    synchronized (lock) {
      if (failure != null) {
        failureReported = true;
        throw new HopException("Asynchronous processing failed", failure);
      }
    }
  }

  private void finish() throws HopException {
    awaitJobsUncapped();
    checkFailure();
    setOutputDone();
  }

  @Override
  public void dispose() {
    // The common pool is shared. Drain our work, but never shut the pool down.
    awaitJobs();
    final boolean shouldLog;
    synchronized (lock) {
      // Only log a failure we have not already surfaced through the run loop, so it is not logged
      // twice; the error accounting below still applies in either case.
      shouldLog = failure != null && !failureReported;
      if (shouldLog) {
        failureReported = true;
      }
    }
    if (failure != null) {
      if (shouldLog) {
        logError("Asynchronous processing failed", failure);
      }
      setErrors(1);
      stopAll();
    }
    super.dispose();
  }

  @Override
  public boolean processRow() throws HopException {
    checkFailure();
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
    } // End If first

    try {
      process(r);
      if (isRowLevel()) {
        logRowlevel(
            BaseMessages.getString(
                PKG,
                "Html2Text.LineNumber",
                getLinesRead() + " : " + getInputRowMeta().getString(r)));
      }
    } catch (Exception e) {
      // An infrastructure/worker failure must not be attributed to the current input row.
      checkFailure();
      handleRowError(r, e);
    }

    return true;
  }

  private void process(Object[] inputRow) throws HopException {
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

  private void handleRowError(Object[] row, Exception e) throws HopException {
    if (!getTransformMeta().isDoingErrorHandling()) {
      throw new HopException("Error processing row", e);
    }
    // Error output has the same serialization requirement as normal output.
    synchronized (this) {
      putError(getInputRowMeta(), row, 1, e.toString(), meta.getHtmlField(), "Html2Text001");
    }
  }

  private void processAsync(String text, Object[] inputRow) throws HopException {
    synchronized (lock) {
      while (jobs >= maxJobs && failure == null) {
        try {
          lock.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new HopException("Interrupted waiting for async processing", e);
        }
      }
      checkFailure();
      jobs++;
    }

    try {
      executor.submit(
          () -> {
            try {
              try {
                processSync(text, inputRow);
              } catch (Exception e) {
                handleRowError(inputRow, e);
              }
            } catch (Throwable e) {
              synchronized (lock) {
                if (failure == null) {
                  failure = e;
                }
              }
            } finally {
              synchronized (lock) {
                jobs--;
                lock.notifyAll();
              }
            }
          });
    } catch (RuntimeException | Error e) {
      synchronized (lock) {
        jobs--;
        if (failure == null) {
          failure = e;
        }
        lock.notifyAll();
      }
      checkFailure();
    }
  }

  private void processSync(String html, Object[] inputRow) throws HopTransformException {

    Document document = parseBodyFragment(html);

    if (meta.isCleanOnly()) {
      Safelist safelist;
      switch (meta.getSafelistType()) {
        case SIMPLE_TEXT -> safelist = Safelist.simpleText();
        case BASIC_WITH_IMAGES -> safelist = Safelist.basicWithImages();
        case NONE -> safelist = Safelist.none();
        case RELAXED -> safelist = Safelist.relaxed();
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
