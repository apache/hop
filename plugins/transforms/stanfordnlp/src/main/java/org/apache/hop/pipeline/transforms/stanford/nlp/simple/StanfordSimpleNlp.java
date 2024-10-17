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

package org.apache.hop.pipeline.transforms.stanford.nlp.simple;

import static java.lang.System.arraycopy;
import static java.util.concurrent.ForkJoinPool.commonPool;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.join;
import static org.apache.commons.lang3.StringUtils.trim;
import static org.apache.hop.core.util.Utils.isEmpty;
import static org.apache.hop.pipeline.transforms.stanford.nlp.simple.ParsedSentence.Builder.parsedSentenceBuilder;

import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class StanfordSimpleNlp extends BaseTransform<StanfordSimpleNlpMeta, StanfordSimpleNlpData> {
  public static final int OVER_ALLOCATE_SIZE = 10;
  private static final Class<?> PKG = StanfordSimpleNlp.class; // For Translator
  private final int cores = Runtime.getRuntime().availableProcessors();
  private final ForkJoinPool executor = commonPool();
  private final AtomicInteger jobs = new AtomicInteger(0);
  private final int maxJobs = cores * 100;
  private final Object lock = new Object();
  private ParsedSentence.Builder sentenceBuilder;

  public StanfordSimpleNlp(
      TransformMeta transformMeta,
      StanfordSimpleNlpMeta meta,
      StanfordSimpleNlpData data,
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

      if (isEmpty(meta.getCorpusField())) {
        throw new HopException(
            BaseMessages.getString(PKG, "StanfordSimpleNlp.Error.CorpusFieldMissing"));
      }
      // cache the position of the field
      cacheIndexPositions();

      sentenceBuilder = parsedSentenceBuilder().includePartOfSpeech(meta.isIncludePartOfSpeech());
    } // End If first

    boolean sendToErrorRow = false;
    String errorMessage = null;

    process(r);

    try {
      if (isRowLevel()) {
        logRowlevel(
            BaseMessages.getString(
                PKG,
                "StanfordSimpleNlp.LineNumber",
                getLinesRead() + " : " + getInputRowMeta().getString(r)));
      }
    } catch (Exception e) {
      if (getTransformMeta().isDoingErrorHandling()) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError(
            BaseMessages.getString(PKG, "StanfordSimpleNlp.ErrorInTransformRunning")
                + e.getMessage());
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if (sendToErrorRow) {
        // Simply add this row to the error row
        putError(
            getInputRowMeta(), r, 1, errorMessage, meta.getCorpusField(), "StanfordSimpleNlp001");
      }
    }

    return true;
  }

  private void process(Object[] inputRow) {
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

  private void processAsync(String corpus, Object[] inputRow) {
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
            processSync(corpus, inputRow);
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

  private void processSync(String corpus, Object[] inputRow) {
    List<ParsedSentence> sentences = sentenceBuilder.buildSentences(inputRow, corpus);
    for (ParsedSentence sentence : sentences) {
      try {
        processSentence(sentence);
      } catch (HopTransformException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void cacheIndexPositions() throws HopException {
    if (data.indexOfCorpusField < 0) {
      data.indexOfCorpusField = data.previousRowMeta.indexOfValue(meta.getCorpusField());
      if (data.indexOfCorpusField < 0) {
        // The field is unreachable !
        throw new HopException(
            BaseMessages.getString(
                PKG, "StanfordSimpleNlp.Exception.CouldnotFindField", meta.getCorpusField()));
      }
    }

    for (String field : data.sentenceFields) {
      field = meta.getOutputFieldNamePrefix() + field;
      if (data.indexOfMap.getOrDefault(field, -1) < 0) {
        data.indexOfMap.put(field, data.outputRowMeta.indexOfValue(field));
        if (data.indexOfMap.getOrDefault(field, -1) < 0) {
          // The field is unreachable!
          throw new HopException(
              BaseMessages.getString(PKG, "DetectLanguage.Exception.CouldnotFindField", field));
        }
      }
    }

    if (meta.isIncludePartOfSpeech()) {
      for (String field : data.posFields) {
        field = meta.getOutputFieldNamePrefix() + field;
        if (data.indexOfMap.getOrDefault(field, -1) < 0) {
          data.indexOfMap.put(field, data.outputRowMeta.indexOfValue(field));
          if (data.indexOfMap.getOrDefault(field, -1) < 0) {
            // The field is unreachable!
            throw new HopException(
                BaseMessages.getString(PKG, "DetectLanguage.Exception.CouldnotFindField", field));
          }
        }
      }

      for (PennTreebankPartOfSpeech e : PennTreebankPartOfSpeech.values()) {
        String field = meta.getOutputFieldNamePrefix() + "penn_treebank_pos_" + e.name();
        if (data.indexOfMap.getOrDefault(field, -1) < 0) {
          data.indexOfMap.put(field, data.outputRowMeta.indexOfValue(field));
          if (data.indexOfMap.getOrDefault(field, -1) < 0) {
            // The field is unreachable!
            throw new HopException(
                BaseMessages.getString(PKG, "DetectLanguage.Exception.CouldnotFindField", field));
          }
        }
      }
    }
  }

  private void processSentence(ParsedSentence sentence) throws HopTransformException {

    Object[] outputRow =
        new Object[sentence.getInputRow().length + data.indexOfMap.size() + OVER_ALLOCATE_SIZE];
    arraycopy(sentence.getInputRow(), 0, outputRow, 0, sentence.getInputRow().length);

    outputRow[data.indexOfMap.get(meta.getOutputFieldNamePrefix() + "text")] =
        sentence.getSentenceText();
    outputRow[data.indexOfMap.get(meta.getOutputFieldNamePrefix() + "index")] = sentence.getIndex();
    outputRow[data.indexOfMap.get(meta.getOutputFieldNamePrefix() + "index_start")] =
        sentence.getIndexBegin();
    outputRow[data.indexOfMap.get(meta.getOutputFieldNamePrefix() + "index_end")] =
        sentence.getIndexEnd();
    outputRow[data.indexOfMap.get(meta.getOutputFieldNamePrefix() + "character_count")] =
        sentence.getCharacterCount();
    outputRow[data.indexOfMap.get(meta.getOutputFieldNamePrefix() + "word_count")] =
        sentence.getWordCount();

    if (meta.isIncludePartOfSpeech()) {
      outputRow[data.indexOfMap.get(meta.getOutputFieldNamePrefix() + "pos_tagged")] =
          join(sentence.getSentencePosTags(), ",");
      outputRow[data.indexOfMap.get(meta.getOutputFieldNamePrefix() + "pos_tags")] =
          sentence.getSentenceTextPosTagged();
      // Count occurrences of the annotations.
      for (PennTreebankPartOfSpeech tag : PennTreebankPartOfSpeech.values()) {
        String field = meta.getOutputFieldNamePrefix() + "penn_treebank_pos_" + tag.name();
        outputRow[data.indexOfMap.get(field)] =
            (long) sentence.getSentencePosBag().getCount(tag.name());
      }
    }

    putRow(data.outputRowMeta, outputRow);
  }

  @Override
  public void putRow(IRowMeta rowMeta, Object[] row) throws HopTransformException {
    synchronized (this) {
      super.putRow(rowMeta, row);
    }
  }
}
