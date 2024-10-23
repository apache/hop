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

package org.apache.hop.pipeline.transforms.languagemodelchat;

import static java.lang.Runtime.getRuntime;
import static java.lang.System.arraycopy;
import static java.time.Duration.between;
import static java.time.Instant.now;
import static java.util.concurrent.Executors.newWorkStealingPool;
import static java.util.concurrent.ForkJoinPool.commonPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.trim;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCauseMessage;
import static org.apache.hop.core.util.Utils.isEmpty;
import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.i18nUtil.i18n;

import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.model.output.Response;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.LanguageModel;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.LanguageModelFacade;

public class LanguageModelChat extends BaseTransform<LanguageModelChatMeta, LanguageModelChatData> {
  public static final int OVER_ALLOCATE_SIZE = 10;
  private static final Class<?> PKG = LanguageModelChat.class; // For Translator
  public static final String CONST_LANGUAGE_MODEL_CHAT_EXCEPTION_COULDNOT_FIND_FIELD =
      "LanguageModelChat.Exception.CouldnotFindField";
  public static final String CONST_MODEL_TYPE = "model_type";

  private Map<LanguageModel, LanguageModelFacade> facadeMap = new ConcurrentHashMap<>();

  private int parallelism = 1;
  private ForkJoinPool executor;
  private final int cores = Runtime.getRuntime().availableProcessors();
  private final AtomicInteger jobs = new AtomicInteger(0);
  private final int maxJobs = cores * 100;
  private final Object lock = new Object();

  public LanguageModelChat(
      TransformMeta transformMeta,
      LanguageModelChatMeta meta,
      LanguageModelChatData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  private void finish() {

    synchronized (lock) {
      while (jobs.get() > 0) {
        try {
          lock.wait();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    if (executor != null) {
      try {
        while (executor.hasQueuedSubmissions() || executor.getActiveThreadCount() > 0) {
          SECONDS.sleep(5);
        }
      } catch (InterruptedException e) {
        logError(e.getMessage(), e);
      }
    }

    facadeMap.clear();
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
      data.nrPrevFields = data.previousRowMeta.size();
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      facadeMap.put(new LanguageModel(meta), new LanguageModelFacade(variables, meta));

      int parallelism =
          meta.getParallelism() <= 0 ? getRuntime().availableProcessors() : meta.getParallelism();
      if (parallelism != 0 && this.parallelism != parallelism || executor == null) {
        if (executor != null) {
          executor.shutdownNow();
        }
        executor = (ForkJoinPool) newWorkStealingPool(parallelism);
      }

      if (isEmpty(meta.getInputField())) {
        throw new HopException(
            BaseMessages.getString(PKG, "LanguageModelChat.Error.InputFieldMissing"));
      }
      // cache the position of the field
      cacheIndexPositions();
    } // End If first

    try {
      process(r);

      if (isRowLevel()) {
        logRowlevel(
            i18n(
                "LanguageModelChat.LineNumber",
                getLinesRead() + " : " + getInputRowMeta().getString(r)));
      }
    } catch (Exception e) {

      String errorMessage;
      if (getTransformMeta().isDoingErrorHandling()) {
        errorMessage = e.toString();
      } else {
        logError(i18n("LanguageModelChat.ErrorInTransformRunning") + e.getMessage());
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      // Simply add this row to the error row
      putError(getInputRowMeta(), r, 1, errorMessage, meta.getInputField(), "LanguageModelChat001");
    }

    return true;
  }

  private void process(Object[] inputRow) {
    String message = trim((String) inputRow[data.indexOfInputField]);
    if (isBlank(message)) {
      return;
    }

    synchronized (lock) {
      while (jobs.get() > maxJobs) {
        try {
          lock.wait();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    ExecutorService exec = meta.getParallelism() == 0 ? commonPool() : executor;

    exec.submit(
        () -> {
          try {
            processSync(inputRow, message);
            jobs.decrementAndGet();
            synchronized (lock) {
              lock.notifyAll();
            }
          } catch (Exception e) {
            try {
              processError(inputRow, e);
            } catch (HopTransformException ex) {
              logError(e.getMessage(), e);
              logError(ex.getMessage(), ex);
            }
          }
        });
    jobs.incrementAndGet();
  }

  private void processError(Object[] inputRow, Exception e) throws HopTransformException {
    logError(e.getMessage(), e);
    int newFields = 8;
    Object[] outputRow = new Object[inputRow.length + newFields + OVER_ALLOCATE_SIZE];
    arraycopy(inputRow, 0, outputRow, 0, inputRow.length);

    LanguageModel model = new LanguageModel(meta);

    outputRow[data.indexOfModelType] = model.getType().code();
    outputRow[data.indexOfModelName] = model.getName();
    outputRow[data.indexOfFinishReason] = "error";
    outputRow[data.indexOfInputTokenCount] = null;
    outputRow[data.indexOfOutputTokenCount] = null;
    outputRow[data.indexOfTotalTokenCount] = null;
    outputRow[data.indexOfInferenceTime] = null;
    outputRow[data.indexOfOutput] = getRootCauseMessage(e);

    putRow(data.outputRowMeta, outputRow);
  }

  private void processSync(Object[] inputRow, String message)
      throws HopTransformException, HopValueException {

    Long inputTokenCount = null;
    Long outputTokenCount = null;
    Long totalTokenCount = null;
    Long inferenceTime = null;
    String finishReason = null;
    String output = null;

    LanguageModel model = new LanguageModel(meta);
    LanguageModelFacade facade = facadeMap.get(model);

    List<ChatMessage> messageList = facade.inputToChatMessages(message);

    if (meta.isMock()) {
      inputTokenCount = -1L;
      outputTokenCount = -1L;
      totalTokenCount = -1L;
      inferenceTime = -1L;
      finishReason = "MOCK";

      if (meta.isOutputChatJson()) {
        output = facade.messagesToOutput(messageList, meta.getMockOutputValue());
      } else {
        output = meta.getMockOutputValue();
      }
    } else {
      Instant inferenceStart = now();
      try {
        Response<AiMessage> ai = facade.generate(messageList);
        inferenceTime = between(inferenceStart, now()).toMillis();
        inputTokenCount =
            ai.tokenUsage() == null || ai.tokenUsage().inputTokenCount() == null
                ? null
                : ai.tokenUsage().inputTokenCount().longValue();
        outputTokenCount =
            ai.tokenUsage() == null || ai.tokenUsage().outputTokenCount() == null
                ? null
                : ai.tokenUsage().outputTokenCount().longValue();
        totalTokenCount =
            ai.tokenUsage() == null || ai.tokenUsage().totalTokenCount() == null
                ? null
                : ai.tokenUsage().totalTokenCount().longValue();
        finishReason = ai.finishReason() == null ? null : ai.finishReason().name();
        if (meta.isOutputChatJson()) {
          output = facade.messagesToOutput(messageList, ai.content().text());
        } else {
          output = ai.content().text();
        }
      } catch (Exception e) {
        inferenceTime = between(inferenceStart, now()).toMillis();
        finishReason = "ERROR";
        output = e.getMessage();
      }
    }

    int newFields = 8;
    Object[] outputRow = new Object[inputRow.length + newFields + OVER_ALLOCATE_SIZE];
    arraycopy(inputRow, 0, outputRow, 0, inputRow.length);

    outputRow[data.indexOfIdentifier] =
        isNotBlank(meta.getIdentifierValue()) ? meta.getIdentifierValue() : model.getName();
    outputRow[data.indexOfModelType] = model.getType().code();
    outputRow[data.indexOfModelName] = model.getName();
    outputRow[data.indexOfFinishReason] = finishReason;
    outputRow[data.indexOfInputTokenCount] = inputTokenCount;
    outputRow[data.indexOfOutputTokenCount] = outputTokenCount;
    outputRow[data.indexOfTotalTokenCount] = totalTokenCount;
    outputRow[data.indexOfInferenceTime] = inferenceTime;
    outputRow[data.indexOfOutput] = output;

    putRow(data.outputRowMeta, outputRow);
  }

  private void cacheIndexPositions() throws HopException {
    if (data.indexOfInputField < 0) {
      data.indexOfInputField = data.previousRowMeta.indexOfValue(meta.getInputField());
      if (data.indexOfInputField < 0) {
        throw new HopException(
            i18n(CONST_LANGUAGE_MODEL_CHAT_EXCEPTION_COULDNOT_FIND_FIELD, meta.getInputField()));
      }
    }
    String outputPrefix = meta.getOutputFieldNamePrefix();

    if (data.indexOfIdentifier < 0) {
      data.indexOfIdentifier = data.outputRowMeta.indexOfValue(outputPrefix + "identifier");
      if (data.indexOfIdentifier < 0) {
        throw new HopException(
            i18n(CONST_LANGUAGE_MODEL_CHAT_EXCEPTION_COULDNOT_FIND_FIELD, CONST_MODEL_TYPE));
      }
    }
    if (data.indexOfModelType < 0) {
      data.indexOfModelType = data.outputRowMeta.indexOfValue(outputPrefix + CONST_MODEL_TYPE);
      if (data.indexOfModelType < 0) {
        throw new HopException(
            i18n(CONST_LANGUAGE_MODEL_CHAT_EXCEPTION_COULDNOT_FIND_FIELD, CONST_MODEL_TYPE));
      }
    }
    if (data.indexOfModelName < 0) {
      data.indexOfModelName = data.outputRowMeta.indexOfValue(outputPrefix + "model_name");
      if (data.indexOfModelName < 0) {
        throw new HopException(
            i18n(CONST_LANGUAGE_MODEL_CHAT_EXCEPTION_COULDNOT_FIND_FIELD, "model_name"));
      }
    }
    if (data.indexOfFinishReason < 0) {
      data.indexOfFinishReason = data.outputRowMeta.indexOfValue(outputPrefix + "finish_reason");
      if (data.indexOfFinishReason < 0) {
        throw new HopException(
            i18n(CONST_LANGUAGE_MODEL_CHAT_EXCEPTION_COULDNOT_FIND_FIELD, "finish_reason"));
      }
    }
    if (data.indexOfInputTokenCount < 0) {
      data.indexOfInputTokenCount =
          data.outputRowMeta.indexOfValue(outputPrefix + "input_token_count");
      if (data.indexOfInputTokenCount < 0) {
        throw new HopException(
            i18n(CONST_LANGUAGE_MODEL_CHAT_EXCEPTION_COULDNOT_FIND_FIELD, "input_token_count"));
      }
    }
    if (data.indexOfOutputTokenCount < 0) {
      data.indexOfOutputTokenCount =
          data.outputRowMeta.indexOfValue(outputPrefix + "output_token_count");
      if (data.indexOfOutputTokenCount < 0) {
        throw new HopException(
            i18n(CONST_LANGUAGE_MODEL_CHAT_EXCEPTION_COULDNOT_FIND_FIELD, "output_token_count"));
      }
    }
    if (data.indexOfTotalTokenCount < 0) {
      data.indexOfTotalTokenCount =
          data.outputRowMeta.indexOfValue(outputPrefix + "total_token_count");
      if (data.indexOfTotalTokenCount < 0) {
        throw new HopException(
            i18n(CONST_LANGUAGE_MODEL_CHAT_EXCEPTION_COULDNOT_FIND_FIELD, "total_token_count"));
      }
    }
    if (data.indexOfInferenceTime < 0) {
      data.indexOfInferenceTime = data.outputRowMeta.indexOfValue(outputPrefix + "inference_time");
      if (data.indexOfInferenceTime < 0) {
        throw new HopException(
            i18n(CONST_LANGUAGE_MODEL_CHAT_EXCEPTION_COULDNOT_FIND_FIELD, "inference_time"));
      }
    }
    if (data.indexOfOutput < 0) {
      data.indexOfOutput = data.outputRowMeta.indexOfValue(outputPrefix + "output");
      if (data.indexOfOutput < 0) {
        throw new HopException(
            i18n(CONST_LANGUAGE_MODEL_CHAT_EXCEPTION_COULDNOT_FIND_FIELD, "output"));
      }
    }
  }

  @Override
  public void putRow(IRowMeta rowMeta, Object[] row) throws HopTransformException {
    synchronized (this) {
      super.putRow(rowMeta, row);
    }
  }
}
