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
    data.batchSize = Const.toInt(resolve(meta.getBatchSize()), 16);
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
    data.pendingTexts.add(Utils.isEmpty(text) ? null : text);
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
        embeddings = data.model.embedAll(segments).content();
      } catch (Exception e) {
        if (!getTransformMeta().isDoingErrorHandling()) {
          throw new HopException(
              BaseMessages.getString(PKG, "EmbedText.Error.Embedding", data.modelName), e);
        }
        // Only the rows that were actually in the request failed. The others carry no text, were
        // never sent, and belong in the output stream.
        for (int i = 0; i < rows.size(); i++) {
          if (texts.get(i) == null) {
            putRow(data.outputRowMeta, resize(rows.get(i)));
          } else {
            putError(
                data.inputRowMeta,
                rows.get(i),
                1,
                e.getMessage(),
                meta.getInputField(),
                "EMBEDTEXT001");
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
  }

  @Override
  public void dispose() {
    data.pendingRows.clear();
    data.pendingTexts.clear();
    data.model = null;
    super.dispose();
  }
}
