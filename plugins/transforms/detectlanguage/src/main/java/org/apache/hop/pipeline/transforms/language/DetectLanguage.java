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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class DetectLanguage extends BaseTransform<DetectLanguageMeta, DetectLanguageData> {
  private static final Class<?> PKG = DetectLanguage.class;

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
  private static final String ERROR_CODE = "DetectLanguage001";

  private final int cores = Runtime.getRuntime().availableProcessors();
  private final int maxJobs = cores * 100;
  private final ExecutorService executor = commonPool();
  private final AtomicInteger jobs = new AtomicInteger(0);

  /** The first failure of a parallel job when there is no error handling to send the row to. */
  private final AtomicReference<Throwable> asyncFailure = new AtomicReference<>();

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

  @Override
  public void dispose() {
    try {
      // After an error or a stop the transform ends without finish(), and parallel jobs can still
      // be queued. They use the detector: unloading under them would make them fail on a missing
      // detector, or load the models again with nothing left to unload them.
      awaitJobs();
      if (data.detector != null) {
        // Lingua keeps its models in static maps: release what this transform asked for. Another
        // Detect language transform that is still running loads them again on demand.
        data.detector.unloadLanguageModels();
        data.detector = null;
      }
    } catch (InterruptedException e) {
      // Jobs can still be using the models: leave them loaded rather than pull them away.
      Thread.currentThread().interrupt();
    }
    super.dispose();
  }

  /** Waits until every parallel job is done. */
  private void awaitJobs() throws InterruptedException {
    synchronized (lock) {
      while (jobs.get() > 0) {
        lock.wait();
      }
    }
  }

  private void finish() throws HopException {
    try {
      awaitJobs();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HopRuntimeException(e);
    }
    failOnAsyncFailure();
    setOutputDone();
  }

  private void failOnAsyncFailure() throws HopException {
    Throwable failure = asyncFailure.get();
    if (failure != null) {
      throw new HopException(
          BaseMessages.getString(PKG, "DetectLanguage.ErrorInTransformRunning")
              + failure.getMessage(),
          failure);
    }
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

      // The field combo accepts variables
      data.corpusField = resolve(meta.getCorpusField());
      if (isEmpty(data.corpusField)) {
        throw new HopException(
            BaseMessages.getString(PKG, "DetectLanguage.Error.CorpusFieldMissing"));
      }
      // cache the position of the field
      cacheIndexPositions();
    } // End If first

    // A parallel job failed and there is no error handling: stop instead of reading on.
    failOnAsyncFailure();

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
      if (!getTransformMeta().isDoingErrorHandling()) {
        throw new HopException(
            BaseMessages.getString(PKG, "DetectLanguage.ErrorInTransformRunning") + e.getMessage(),
            e);
      }
      putErrorRow(r, e);
    }

    return true;
  }

  private void putErrorRow(Object[] row, Exception e) throws HopTransformException {
    synchronized (this) {
      putError(getInputRowMeta(), row, 1, e.toString(), data.corpusField, ERROR_CODE);
    }
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

  private void processAsync(String corpus, Object[] inputRow) {
    synchronized (lock) {
      while (jobs.get() > maxJobs) {
        try {
          lock.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new HopRuntimeException(e);
        }
      }
    }

    jobs.incrementAndGet();
    executor.submit(
        () -> {
          try {
            // Stopped: nobody wants the row anymore, and dispose() waits for the queue to drain.
            if (!isStopped() || isSafeStopped()) {
              processSync(corpus, inputRow);
            }
          } catch (Throwable t) {
            // Throwable, not Exception: nobody reads the pool's Future, so an Error caught
            // nowhere else would make the row disappear without a trace.
            handleAsyncFailure(inputRow, t);
          } finally {
            // Always count the job down: finish() waits for zero.
            jobs.decrementAndGet();
            synchronized (lock) {
              lock.notifyAll();
            }
          }
        });
  }

  /**
   * A failed row goes to the error hop when there is one. An Error, such as running out of memory,
   * says nothing about the row: it always stops the transform.
   */
  private void handleAsyncFailure(Object[] inputRow, Throwable failure) {
    if (failure instanceof Exception e && getTransformMeta().isDoingErrorHandling()) {
      try {
        putErrorRow(inputRow, e);
        return;
      } catch (Exception errorRowFailure) {
        failure.addSuppressed(errorRowFailure);
      }
    }
    asyncFailure.compareAndSet(null, failure);
  }

  private void cacheIndexPositions() throws HopException {
    if (data.indexOfCorpusField < 0) {
      data.indexOfCorpusField = data.previousRowMeta.indexOfValue(data.corpusField);
      if (data.indexOfCorpusField < 0) {
        // The field is unreachable!
        throw new HopException(
            BaseMessages.getString(
                PKG, "DetectLanguage.Exception.CouldnotFindField", data.corpusField));
      }
    }
    if (data.indexOfDetectedLanguage < 0) {
      data.indexOfDetectedLanguage = data.outputRowMeta.indexOfValue("detected_language");
      if (data.indexOfDetectedLanguage < 0) {
        // The field is unreachable!
        throw new HopException(
            BaseMessages.getString(
                PKG, "DetectLanguage.Exception.CouldnotFindField", "detected_language"));
      }
    }
    if (data.indexOfDetectedLanguageConfidence < 0) {
      data.indexOfDetectedLanguageConfidence =
          data.outputRowMeta.indexOfValue("detected_language_confidence");
      if (data.indexOfDetectedLanguageConfidence < 0) {
        // The field is unreachable!
        throw new HopException(
            BaseMessages.getString(
                PKG, "DetectLanguage.Exception.CouldnotFindField", "detected_language_confidence"));
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
