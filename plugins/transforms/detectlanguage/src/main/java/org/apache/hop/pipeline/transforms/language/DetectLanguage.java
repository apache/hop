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
import static com.github.pemistahl.lingua.api.LanguageDetectorBuilder.fromAllSpokenLanguages;
import static java.lang.System.arraycopy;
import static java.util.concurrent.ForkJoinPool.commonPool;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.trim;
import static org.apache.hop.core.util.Utils.isEmpty;

import com.github.pemistahl.lingua.api.Language;
import com.github.pemistahl.lingua.api.LanguageDetector;
import java.util.ArrayList;
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
  // Guarded by lock, including reservation before submission.
  private int jobs;
  private Throwable failure;

  private final Object lock = new Object();

  private final LanguageDetector detector =
      fromAllSpokenLanguages().withPreloadedLanguageModels().build();

  private void awaitJobs() {
    boolean interrupted = false;
    synchronized (lock) {
      while (jobs > 0) {
        try {
          lock.wait();
        } catch (InterruptedException e) {
          // Accepted work must finish before output is closed or resources are disposed.
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
        throw new HopException("Asynchronous processing failed", failure);
      }
    }
  }

  private void finish() throws HopException {
    awaitJobs();
    detector.unloadLanguageModels();
    checkFailure();
    setOutputDone();
  }

  @Override
  public void dispose() {
    // The common pool is shared. Drain our work, but never shut the pool down.
    awaitJobs();
    detector.unloadLanguageModels();
    if (failure != null) {
      logError("Asynchronous processing failed", failure);
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

      if (isEmpty(meta.getCorpusField())) {
        throw new HopException(
            BaseMessages.getString(PKG, "DetectLanguage.Error.CorpusFieldMissing"));
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
                "DetectLanguage.LineNumber",
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
    String corpus = trim((String) inputRow[data.indexOfCorpusField]);
    if (isBlank(corpus)) {
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
      throw new HopException("Error processing row", e);
    }
    // Error output has the same serialization requirement as normal output.
    synchronized (this) {
      putError(getInputRowMeta(), row, 1, e.toString(), meta.getCorpusField(), "DetectLanguage001");
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

  @SuppressWarnings(
      "javabugs:S2259") // the probabilities come from the map's own keys and are never null
  private void cacheIndexPositions() throws HopException {
    if (data.indexOfCorpusField < 0) {
      data.indexOfCorpusField = data.previousRowMeta.indexOfValue(meta.getCorpusField());
      if (data.indexOfCorpusField < 0) {
        // The field is unreachable!
        throw new HopException(
            BaseMessages.getString(
                PKG, "DetectLanguage.Exception.CouldnotFindField", meta.getCorpusField()));
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

  @SuppressWarnings(
      "javabugs:S2259") // the probabilities come from the map's own keys and are never null
  private void processSync(String corpus, Object[] inputRow) throws HopTransformException {
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

    Object[] outputRow = new Object[inputRow.length + 2 + OVER_ALLOCATE_SIZE];
    arraycopy(inputRow, 0, outputRow, 0, inputRow.length);
    outputRow[data.indexOfDetectedLanguage] = detectedLanguage.name();
    outputRow[data.indexOfDetectedLanguageConfidence] = confidence;
    putRow(data.outputRowMeta, outputRow);
  }

  @Override
  public void putRow(IRowMeta rowMeta, Object[] row) throws HopTransformException {
    synchronized (this) {
      super.putRow(rowMeta, row);
    }
  }
}
