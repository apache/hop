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
package org.apache.hop.ai.transforms.embedtext;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.exception.NonRetriableException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.ai.engine.AiEmbeddingFactory;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Turns a text field into an embedding vector using the model of an AI provider. */
public class EmbedText extends BaseTransform<EmbedTextMeta, EmbedTextData> {

  private static final Class<?> PKG = EmbedTextMeta.class;

  /** Used when the batch size is left empty. */
  static final int DEFAULT_BATCH_SIZE = 16;

  /**
   * Retries of a failed call, after the first attempt. The same as langchain4j's default, which
   * this transform switches off so a retry is one log line rather than a stack trace.
   */
  static final int MAX_RETRIES = 2;

  public EmbedText(
      TransformMeta transformMeta,
      EmbedTextMeta meta,
      EmbedTextData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    if (Utils.isEmpty(meta.getAiProvider())) {
      logError(BaseMessages.getString(PKG, "EmbedText.Validation.ProviderRequired"));
      return false;
    }
    String batchSize = resolve(meta.getBatchSize());
    if (Utils.isEmpty(batchSize)) {
      data.batchSize = DEFAULT_BATCH_SIZE;
    } else {
      try {
        data.batchSize = Integer.parseInt(batchSize.trim());
      } catch (NumberFormatException e) {
        // An unset variable stays as ${...}. Running with some other batch size would hide that.
        logError(
            BaseMessages.getString(PKG, "EmbedText.Validation.BatchSizeNotANumber", batchSize));
        return false;
      }
    }
    if (data.batchSize < 1) {
      logError(BaseMessages.getString(PKG, "EmbedText.Validation.BatchSizePositive"));
      return false;
    }
    data.emitVector = meta.getOutputFormat() == EmbedTextOutputFormat.VECTOR;
    return super.init();
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();
    if (row == null) {
      flushBatch();
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      data.inputRowMeta = getInputRowMeta();
      data.outputRowMeta = data.inputRowMeta.clone();
      meta.getFields(
          data.outputRowMeta, getTransformName(), null, null, this, getMetadataProvider());
      resolveInputField();
      resolveOutputFieldNames();
      openModel();
    }

    String text = data.inputRowMeta.getString(row, data.inputFieldIndex);
    data.pendingRows.add(row);
    // Null marks a row that needs no embedding. It still queues, so the output keeps input order.
    data.pendingTexts.add(hasNothingToEmbed(text) ? null : text);
    if (data.pendingRows.size() >= data.batchSize) {
      flushBatch();
    }
    return true;
  }

  /**
   * Embeds the pending rows that have text, in one call, and releases every pending row in input
   * order.
   *
   * <p>A provider failure diverts the whole batch when an error hop is attached, because the
   * provider answers per batch and cannot say which text it choked on.
   */
  private void flushBatch() throws HopException {
    if (data.pendingRows.isEmpty()) {
      return;
    }
    List<Object[]> rows = new ArrayList<>(data.pendingRows);
    List<String> texts = new ArrayList<>(data.pendingTexts);
    data.pendingRows.clear();
    data.pendingTexts.clear();

    List<TextSegment> segments = new ArrayList<>();
    for (String text : texts) {
      if (text != null) {
        segments.add(TextSegment.from(text));
      }
    }

    List<Embedding> embeddings = List.of();
    if (!segments.isEmpty()) {
      try {
        embeddings = embedWithRetries(segments);
      } catch (Exception e) {
        String failure = describeFailure(e);
        if (!getTransformMeta().isDoingErrorHandling()) {
          throw new HopException(failure, e);
        }
        // Only the rows that were actually in the request failed. The others carry no text, were
        // never sent, and belong in the output stream.
        for (int i = 0; i < rows.size(); i++) {
          if (texts.get(i) == null) {
            putRow(data.outputRowMeta, resize(rows.get(i)));
          } else {
            putError(
                data.inputRowMeta, rows.get(i), 1, failure, meta.getInputField(), "EMBEDTEXT001");
          }
        }
        return;
      }
      if (embeddings == null || embeddings.size() != segments.size()) {
        throw new HopException(
            BaseMessages.getString(
                PKG,
                "EmbedText.Error.BatchSizeMismatch",
                String.valueOf(embeddings == null ? 0 : embeddings.size()),
                String.valueOf(segments.size())));
      }
    }

    int embedded = 0;
    for (int i = 0; i < rows.size(); i++) {
      Object[] output = resize(rows.get(i));
      if (texts.get(i) != null) {
        withEmbedding(output, embeddings.get(embedded++).vector());
      }
      putRow(data.outputRowMeta, output);
    }
  }

  /**
   * Calls the provider, retrying a failure that may pass (no connection, a timeout, a rate limit, a
   * server error). A refusal that will not change, such as a bad API key or an unknown model, fails
   * at once.
   */
  private List<Embedding> embedWithRetries(List<TextSegment> segments) {
    long delay = data.retryDelayMillis;
    for (int attempt = 1; ; attempt++) {
      try {
        return data.model.embedAll(segments).content();
      } catch (RuntimeException e) {
        if (attempt > MAX_RETRIES || e instanceof NonRetriableException || isStopped()) {
          throw e;
        }
        logBasic(
            BaseMessages.getString(
                PKG,
                "EmbedText.Log.Retrying",
                describeFailure(e),
                String.valueOf(attempt),
                String.valueOf(MAX_RETRIES)));
        if (isDebug()) {
          logDebug(Const.getStackTracker(e));
        }
        if (!pause(delay)) {
          throw e;
        }
        delay = delay * 3 / 2;
      }
    }
  }

  /** Waits, but gives up as soon as the pipeline is stopped. False when the wait was cut short. */
  private boolean pause(long millis) {
    long until = System.currentTimeMillis() + millis;
    try {
      while (!isStopped()) {
        long left = until - System.currentTimeMillis();
        if (left <= 0) {
          return true;
        }
        Thread.sleep(Math.min(left, 100L));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return false;
  }

  /**
   * One line saying which model, provider and endpoint failed, and why. The exception of a failed
   * connection carries no message of its own, so on its own it says nothing about where it went.
   */
  String describeFailure(Throwable e) {
    String endpoint = endpointForMessages(data.baseUrl);
    return BaseMessages.getString(
        PKG,
        "EmbedText.Error.Embedding",
        data.modelName,
        data.providerName,
        Utils.isEmpty(endpoint) ? "?" : endpoint,
        rootCause(e));
  }

  /** The deepest cause, as its class name and its message when it has one. */
  static String rootCause(Throwable e) {
    Throwable root = e;
    while (root.getCause() != null && root.getCause() != root) {
      root = root.getCause();
    }
    String message = root.getMessage();
    String name = root.getClass().getSimpleName();
    return Utils.isEmpty(message) ? name : name + ": " + message;
  }

  /**
   * The base URL without user info, query or fragment, which is where a key would sit if someone
   * put one in the URL. Error rows can end up in a table, so they must not carry it.
   */
  static String endpointForMessages(String baseUrl) {
    if (Utils.isEmpty(baseUrl)) {
      return "";
    }
    try {
      URI uri = new URI(baseUrl.trim());
      if (uri.getHost() == null) {
        return uri.getRawUserInfo() == null && uri.getRawQuery() == null ? baseUrl.trim() : "";
      }
      return new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), uri.getPath(), null, null)
          .toString();
    } catch (URISyntaxException e) {
      return "";
    }
  }

  /**
   * True for text that has no content to embed: only whitespace, Unicode spaces (no-break, narrow
   * no-break, ideographic, ...), control characters and invisible format characters such as the
   * zero-width space. langchain4j refuses a segment that trims to nothing, which would stop the
   * transform, and the others would be sent and come back as an embedding that means nothing.
   */
  static boolean hasNothingToEmbed(String text) {
    return text == null || text.codePoints().allMatch(EmbedText::isInvisible);
  }

  private static boolean isInvisible(int codePoint) {
    return Character.isWhitespace(codePoint)
        || Character.isSpaceChar(codePoint)
        || Character.isISOControl(codePoint)
        || Character.getType(codePoint) == Character.FORMAT;
  }

  private void withEmbedding(Object[] output, float[] vector) {
    int index = data.inputRowMeta.size();
    output[index++] = data.emitVector ? vector : toJsonArray(vector);
    // These mirror the columns getFields added, which it decided on the same resolved names.
    if (meta.isIncludeModelMetadata()) {
      if (!Utils.isEmpty(data.modelFieldName)) {
        output[index++] = data.modelName;
      }
      if (!Utils.isEmpty(data.dimensionsFieldName)) {
        output[index] = (long) vector.length;
      }
    }
  }

  private Object[] resize(Object[] row) {
    return RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
  }

  /** The canonical text form, which pgvector accepts as a literal and any JSON reader can parse. */
  static String toJsonArray(float[] vector) {
    StringBuilder builder = new StringBuilder(vector.length * 8 + 2);
    builder.append('[');
    for (int i = 0; i < vector.length; i++) {
      if (i > 0) {
        builder.append(',');
      }
      builder.append(vector[i]);
    }
    return builder.append(']').toString();
  }

  /** Visible for testing: the first-row resolution step, without needing a live model. */
  void resolveOutputFieldNamesForTesting() throws HopException {
    resolveOutputFieldNames();
  }

  private void resolveOutputFieldNames() throws HopException {
    data.outputFieldName = resolve(meta.getOutputField());
    data.modelFieldName = resolve(meta.getModelField());
    data.dimensionsFieldName = resolve(meta.getDimensionsField());
    // getFields adds no embedding column for an empty name, so writing one anyway would run off
    // the end of the row. A variable that resolves to nothing is a mistake worth reporting, not
    // a reason to drop the embedding quietly.
    if (Utils.isEmpty(data.outputFieldName)) {
      throw new HopException(
          BaseMessages.getString(PKG, "EmbedText.Validation.OutputFieldRequired"));
    }
  }

  private void resolveInputField() throws HopException {
    data.inputFieldIndex = data.inputRowMeta.indexOfValue(meta.getInputField());
    if (data.inputFieldIndex < 0) {
      throw new HopException(
          BaseMessages.getString(
              PKG,
              "EmbedText.Validation.InputFieldNotFound",
              String.valueOf(meta.getInputField())));
    }
  }

  private void openModel() throws HopException {
    AiEmbeddingFactory.ResolvedEmbeddingModel resolved =
        AiEmbeddingFactory.resolveEmbeddingModel(
            resolve(meta.getAiProvider()), meta.getModelName(), this, getMetadataProvider());
    data.model = resolved.model();
    data.modelName = resolved.modelName();
    data.providerName = resolved.providerName();
    data.baseUrl = resolved.baseUrl();
  }

  @Override
  public void dispose() {
    data.pendingRows.clear();
    data.pendingTexts.clear();
    data.model = null;
    super.dispose();
  }
}
