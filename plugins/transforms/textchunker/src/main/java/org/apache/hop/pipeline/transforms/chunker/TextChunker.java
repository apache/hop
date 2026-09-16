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
package org.apache.hop.pipeline.transforms.chunker;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.chunker.chunking.ChunkingStrategy;
import org.apache.hop.pipeline.transforms.chunker.chunking.ChunkingStrategyFactory;
import org.apache.hop.pipeline.transforms.chunker.chunking.ChunkingStrategyType;
import org.apache.hop.pipeline.transforms.chunker.chunking.StructureChunkingStrategy;
import org.apache.hop.pipeline.transforms.chunker.document.ContentType;
import org.apache.hop.pipeline.transforms.chunker.document.ContentTypeResolver;

/**
 * Text Chunker transform - splits text into chunks for AI/ML processing. Supports multiple chunking
 * strategies (character-based with word boundary respect, paragraph-based, structure-aware) with
 * configurable overlap.
 */
public class TextChunker extends BaseTransform<TextChunkerMeta, TextChunkerData> {

  private static final Class<?> PKG = TextChunkerMeta.class;

  /** The chunking strategy instance. */
  private ChunkingStrategy strategy;

  /**
   * Counter used to synthesise a document ID when no source field is configured. Prefixed with the
   * transform copy number so parallel copies cannot produce colliding identifiers.
   */
  private long documentIdCounter = 0;

  public TextChunker(
      TransformMeta transformMeta,
      TextChunkerMeta meta,
      TextChunkerData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }
    strategy = ChunkingStrategyFactory.createStrategy(meta.getChunkingStrategy());
    logBasic(
        BaseMessages.getString(
            PKG, "TextChunker.Log.Initialized", String.valueOf(meta.getChunkingStrategy())));
    return true;
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();

    if (row == null) {
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      data.inputRowMeta = getInputRowMeta();
      data.outputRowMeta = data.inputRowMeta.clone();
      meta.getFields(
          data.outputRowMeta, getTransformName(), null, null, this, getMetadataProvider());
      resolveFieldIndices();
    }

    try {
      chunkRow(row);
    } catch (HopException e) {
      if (getTransformMeta().isDoingErrorHandling()) {
        putError(data.inputRowMeta, row, 1, e.getMessage(), meta.getInputField(), "TEXTCHUNKER001");
      } else {
        throw e;
      }
    }

    return true;
  }

  private void chunkRow(Object[] row) throws HopException {
    String text = data.inputRowMeta.getString(row, data.inputFieldIndex);
    String documentIdValue = resolveDocumentId(row);

    if (Utils.isEmpty(text)) {
      if (isRowLevel()) {
        logRowlevel(BaseMessages.getString(PKG, "TextChunker.Log.EmptyText"));
      }
      putRow(data.outputRowMeta, createOutputRow(row, "", 0, 0, documentIdValue, 0));
      return;
    }

    List<Chunk> chunks = chunk(row, text);
    int totalChunks = chunks.size();

    // Output each chunk as a new row (must copy row data per chunk; the buffer is reused).
    for (Chunk chunk : chunks) {
      putRow(
          data.outputRowMeta,
          createOutputRow(
              row,
              chunk.getContent(),
              chunk.getIndex(),
              chunk.getStartPosition(),
              documentIdValue,
              totalChunks));
    }
  }

  private List<Chunk> chunk(Object[] row, String text) throws HopException {
    if (meta.getChunkingStrategy() == ChunkingStrategyType.STRUCTURE
        && strategy instanceof StructureChunkingStrategy structureStrategy) {
      ContentType contentType = resolveContentType(row);
      structureStrategy.setContentType(contentType);
      return structureStrategy.chunk(
          text, meta.getChunkSize(), meta.getChunkOverlap(), contentType);
    }
    return strategy.chunk(text, meta.getChunkSize(), meta.getChunkOverlap());
  }

  /**
   * Resolves every input and output field index once, so the row loop never scans the row metadata.
   */
  private void resolveFieldIndices() throws HopException {
    data.inputFieldIndex = data.inputRowMeta.indexOfValue(meta.getInputField());
    if (data.inputFieldIndex < 0) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "TextChunker.Validation.InputFieldNotFound", meta.getInputField()));
    }

    data.sourceDocumentIdFieldIndex =
        requireOptionalInputField(
            meta.getSourceDocumentIdField(),
            "TextChunker.Validation.SourceDocumentIdFieldNotFound");
    data.contentTypeFieldIndex =
        requireOptionalInputField(
            meta.getContentTypeField(), "TextChunker.Validation.ContentTypeFieldNotFound");

    data.outputChunkFieldIndex = data.outputRowMeta.indexOfValue(meta.getOutputChunkField());
    if (meta.isIncludeMetadata()) {
      data.chunkIndexFieldIndex = data.outputRowMeta.indexOfValue(meta.getChunkIndexField());
      data.chunkStartPosFieldIndex = data.outputRowMeta.indexOfValue(meta.getChunkStartPosField());
      data.documentIdFieldIndex = data.outputRowMeta.indexOfValue(meta.getDocumentIdField());
      data.chunkCountFieldIndex = data.outputRowMeta.indexOfValue(meta.getChunkCountField());
    }
  }

  private int requireOptionalInputField(String fieldName, String messageKey) throws HopException {
    if (Utils.isEmpty(fieldName)) {
      return -1;
    }
    int index = data.inputRowMeta.indexOfValue(fieldName);
    if (index < 0) {
      throw new HopException(BaseMessages.getString(PKG, messageKey, fieldName));
    }
    return index;
  }

  private String resolveDocumentId(Object[] row) throws HopException {
    if (data.sourceDocumentIdFieldIndex >= 0) {
      return data.inputRowMeta.getString(row, data.sourceDocumentIdFieldIndex);
    }
    return getCopy() + "_" + documentIdCounter++;
  }

  private ContentType resolveContentType(Object[] row) throws HopException {
    if (data.contentTypeFieldIndex >= 0) {
      String fieldValue = data.inputRowMeta.getString(row, data.contentTypeFieldIndex);
      ContentType fromSource = ContentTypeResolver.fromSourceType(fieldValue);
      if (fromSource != ContentType.AUTO) {
        return fromSource;
      }
      ContentType parsed = ContentType.fromString(fieldValue);
      if (parsed != ContentType.AUTO) {
        return parsed;
      }
    }
    ContentType configured = meta.getContentType();
    return configured != null ? configured : ContentType.AUTO;
  }

  /**
   * Creates an output row with the chunk data and metadata.
   *
   * @param inputRow The original input row
   * @param chunkContent The chunk text content
   * @param chunkIndex The 0-based index of this chunk
   * @param chunkStartPos The start position of this chunk in the original text
   * @param documentId The document ID for this row
   * @param totalChunks The total number of chunks for this document
   * @return The output row with chunk data
   */
  private Object[] createOutputRow(
      Object[] inputRow,
      String chunkContent,
      int chunkIndex,
      int chunkStartPos,
      String documentId,
      int totalChunks) {

    Object[] outputRow = RowDataUtil.createResizedCopy(inputRow, data.outputRowMeta.size());

    setField(outputRow, data.outputChunkFieldIndex, chunkContent);

    if (meta.isIncludeMetadata()) {
      setField(outputRow, data.chunkIndexFieldIndex, (long) chunkIndex);
      setField(outputRow, data.chunkStartPosFieldIndex, (long) chunkStartPos);
      setField(outputRow, data.documentIdFieldIndex, documentId);
      setField(outputRow, data.chunkCountFieldIndex, (long) totalChunks);
    }

    return outputRow;
  }

  private static void setField(Object[] row, int index, Object value) {
    if (index >= 0 && index < row.length) {
      row[index] = value;
    }
  }

  @Override
  public void dispose() {
    strategy = null;
    super.dispose();
  }
}
