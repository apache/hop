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

package org.apache.hop.pipeline.transforms.language;

import static com.github.pemistahl.lingua.api.Language.UNKNOWN;
import static java.lang.System.arraycopy;
import static java.util.concurrent.ForkJoinPool.commonPool;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.trim;
import static org.apache.hop.core.util.Utils.isEmpty;

import com.github.pemistahl.lingua.api.Language;
import com.github.pemistahl.lingua.api.LanguageDetector;
import com.github.pemistahl.lingua.api.LanguageDetectorBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class DetectLanguage extends BaseTransform<DetectLanguageMeta, DetectLanguageData> {
  private static final Class<?> PKG = DetectLanguage.class;
  private static final String CONST_DETECT_LANGUAGE_EXCEPTION_COULDNOT_FIND_FIELD =
      "DetectLanguage.Exception.CouldnotFindField";
  private static final String ERROR_CODE = "DetectLanguage001";

  public DetectLanguage(
      TransformMeta transformMeta,
      DetectLanguageMeta meta,
      DetectLanguageData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  public static final int OVER_ALLOCATE_SIZE = 10;
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

  /** The outcome of one detection: the language name and its confidence, null when unknown. */
  record Detection(String language, Double confidence) {}

  /**
   * The Lingua builder for the configured options.
   *
   * @param languages the languages to choose from; empty means all spoken languages
   * @param lowAccuracy use only the trigram models
   */
  static LanguageDetectorBuilder createBuilder(List<Language> languages, boolean lowAccuracy) {
    LanguageDetectorBuilder builder =
        languages.isEmpty()
            ? LanguageDetectorBuilder.fromAllSpokenLanguages()
            : LanguageDetectorBuilder.fromLanguages(languages.toArray(new Language[0]));
    if (lowAccuracy) {
      builder = builder.withLowAccuracyMode();
    }
    return builder.withPreloadedLanguageModels();
  }

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }
    try {
      data.detector = createBuilder(meta.resolveLanguages(this), meta.isLowAccuracy()).build();
      return true;
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "DetectLanguage.Error.DetectorNotBuilt"), e);
      return false;
    }
  }

  /**
   * Waits for accepted asynchronous work, at most {@link #DRAIN_TIMEOUT_MILLIS}.
   *
   * @return true when every job finished, false when some were abandoned
   */
  private boolean awaitJobs() {
    return awaitJobs(DRAIN_TIMEOUT_MILLIS);
  }

  private boolean awaitJobs(final long timeoutMillis) {
    boolean interrupted = false;
    boolean drained;
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
      drained = jobs == 0;
      if (!drained) {
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
    return drained;
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
    // After an error or a stop the transform ends without finish(), and parallel jobs can still be
    // queued. They use the detector: unloading under them would make them fail on a missing
    // detector, or load the models again with nothing left to unload them.
    boolean drained = awaitJobs();
    if (drained && data.detector != null) {
      // Lingua keeps its models in static maps: release what this transform asked for. Another
      // Detect language transform that is still running loads them again on demand.
      data.detector.unloadLanguageModels();
      data.detector = null;
    }
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

      // The field combo accepts variables
      data.corpusField = resolve(meta.getCorpusField());
      if (isEmpty(data.corpusField)) {
        throw new HopException(
            BaseMessages.getString(PKG, "DetectLanguage.Error.CorpusFieldMissing"));
      }
      // cache the position of the field
      cacheIndexPositions();
    } // End If first

    try {
      if (isRowLevel()) {
        logRowlevel(
            BaseMessages.getString(
                PKG,
                "DetectLanguage.LineNumber",
                getLinesRead() + " : " + getInputRowMeta().getString(r)));
      }
      process(r);
    } catch (Exception e) {
      // An infrastructure/worker failure must not be attributed to the current input row.
      checkFailure();
      handleRowError(r, e);
    }

    return true;
  }

  private void process(Object[] inputRow) throws HopException {
    String corpus = trim(getInputRowMeta().getString(inputRow, data.indexOfCorpusField));
    if (isBlank(corpus)) {
      // Nothing to detect: the row passes with an unknown language and no confidence.
      putDetection(inputRow, new Detection(UNKNOWN.name(), null));
      return;
    }

    if (meta.isParallelism()) {
      processAsync(corpus, inputRow);
    } else {
      processSync(corpus, inputRow);
    }
  }

  private void handleRowError(Object[] row, Exception e) throws HopException {
    if (!getTransformMeta().isDoingErrorHandling()) {
      throw new HopException(
          BaseMessages.getString(PKG, "DetectLanguage.ErrorInTransformRunning") + e.getMessage(),
          e);
    }
    // Error output has the same serialization requirement as normal output.
    synchronized (this) {
      putError(getInputRowMeta(), row, 1, e.toString(), data.corpusField, ERROR_CODE);
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
                // Stopped: nobody wants the row anymore, and dispose() waits for the queue to
                // drain.
                if (!isStopped() || isSafeStopped()) {
                  processSync(text, inputRow);
                }
              } catch (Exception e) {
                handleRowError(inputRow, e);
              }
            } catch (Throwable e) {
              // Throwable, not Exception: nobody reads the pool's Future, so an Error caught
              // nowhere else would make the row disappear without a trace. An Error says nothing
              // about the row: it always stops the transform.
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

  private void cacheIndexPositions() throws HopException {
    if (data.indexOfCorpusField < 0) {
      data.indexOfCorpusField = data.previousRowMeta.indexOfValue(data.corpusField);
      if (data.indexOfCorpusField < 0) {
        // The field is unreachable!
        throw new HopException(
            BaseMessages.getString(
                PKG, CONST_DETECT_LANGUAGE_EXCEPTION_COULDNOT_FIND_FIELD, data.corpusField));
      }
    }
    if (data.indexOfDetectedLanguage < 0) {
      data.indexOfDetectedLanguage = data.outputRowMeta.indexOfValue("detected_language");
      if (data.indexOfDetectedLanguage < 0) {
        // The field is unreachable!
        throw new HopException(
            BaseMessages.getString(
                PKG, CONST_DETECT_LANGUAGE_EXCEPTION_COULDNOT_FIND_FIELD, "detected_language"));
      }
    }
    if (data.indexOfDetectedLanguageConfidence < 0) {
      data.indexOfDetectedLanguageConfidence =
          data.outputRowMeta.indexOfValue("detected_language_confidence");
      if (data.indexOfDetectedLanguageConfidence < 0) {
        // The field is unreachable!
        throw new HopException(
            BaseMessages.getString(
                PKG,
                CONST_DETECT_LANGUAGE_EXCEPTION_COULDNOT_FIND_FIELD,
                "detected_language_confidence"));
      }
    }
  }

  /**
   * Detects the language of a text: the most likely language, unless the second one is as likely or
   * closer than Lingua's minimum relative distance, in which case the language is unknown.
   */
  @SuppressWarnings(
      "javabugs:S2259") // the probabilities come from the map's own keys and are never null
  static Detection detect(LanguageDetector detector, String corpus) {
    SortedMap<Language, Double> confidenceValues = detector.computeLanguageConfidenceValues(corpus);
    Language detectedLanguage = UNKNOWN;
    Double confidence = null;

    if (!confidenceValues.isEmpty()) {
      Language mostLikelyLanguage = confidenceValues.firstKey();
      Double mostLikelyLanguageProbability = confidenceValues.get(mostLikelyLanguage);
      if (confidenceValues.size() == 1) {
        detectedLanguage = mostLikelyLanguage;
        confidence = mostLikelyLanguageProbability;
      } else {
        Double secondMostLikelyLanguageProbability =
            new ArrayList<>(confidenceValues.values()).get(1);
        boolean eq =
            Objects.equals(mostLikelyLanguageProbability, secondMostLikelyLanguageProbability);
        boolean unk =
            (mostLikelyLanguageProbability - secondMostLikelyLanguageProbability)
                < detector.getMinimumRelativeDistance$lingua();
        if (!eq && !unk) {
          detectedLanguage = mostLikelyLanguage;
          confidence = mostLikelyLanguageProbability;
        }
      }
    }
    return new Detection(detectedLanguage.name(), confidence);
  }

  private void processSync(String corpus, Object[] inputRow) throws HopTransformException {
    putDetection(inputRow, detect(data.detector, corpus));
  }

  private void putDetection(Object[] inputRow, Detection detection) throws HopTransformException {
    Object[] outputRow = new Object[inputRow.length + 2 + OVER_ALLOCATE_SIZE];
    arraycopy(inputRow, 0, outputRow, 0, inputRow.length);
    outputRow[data.indexOfDetectedLanguage] = detection.language();
    outputRow[data.indexOfDetectedLanguageConfidence] = detection.confidence();
    putRow(data.outputRowMeta, outputRow);
  }

  @Override
  public void putRow(IRowMeta rowMeta, Object[] row) throws HopTransformException {
    synchronized (this) {
      super.putRow(rowMeta, row);
    }
  }
}
