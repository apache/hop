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

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.chunker.chunking.ChunkingStrategyType;
import org.apache.hop.pipeline.transforms.chunker.document.ContentType;

@Getter
@Setter
@Transform(
    id = "TextChunker",
    image = "chunker.svg",
    name = "i18n::TextChunker.Name",
    description = "i18n::TextChunker.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "/pipeline/transforms/textchunker.html",
    keywords = "i18n::TextChunker.Keywords")
public class TextChunkerMeta extends BaseTransformMeta<TextChunker, TextChunkerData> {

  private static final Class<?> PKG = TextChunkerMeta.class;

  /** The field containing text to chunk. */
  @HopMetadataProperty(key = "inputField", injectionKey = "INPUT_FIELD")
  private String inputField;

  /** The name of the field to output chunks to. */
  @HopMetadataProperty(key = "outputChunkField", injectionKey = "OUTPUT_CHUNK_FIELD")
  private String outputChunkField = "chunk_text";

  /** The chunking strategy to use. */
  @HopMetadataProperty(key = "chunkingStrategy", injectionKey = "CHUNKING_STRATEGY")
  private ChunkingStrategyType chunkingStrategy = ChunkingStrategyType.CHARACTER;

  /**
   * The maximum size for each chunk (characters for CHARACTER strategy, approximate for PARAGRAPH).
   */
  @HopMetadataProperty(key = "chunkSize", injectionKey = "CHUNK_SIZE")
  private int chunkSize = 1000;

  /** The number of characters to overlap between chunks (for CHARACTER strategy). */
  @HopMetadataProperty(key = "chunkOverlap", injectionKey = "CHUNK_OVERLAP")
  private int chunkOverlap = 200;

  /** Whether to include metadata fields in the output. */
  @HopMetadataProperty(key = "includeMetadata", injectionKey = "INCLUDE_METADATA")
  private boolean includeMetadata = true;

  /** Field name for the chunk index (if metadata is enabled). */
  @HopMetadataProperty(key = "chunkIndexField", injectionKey = "CHUNK_INDEX_FIELD")
  private String chunkIndexField = "chunk_index";

  /** Field name for the chunk start position (if metadata is enabled). */
  @HopMetadataProperty(key = "chunkStartPosField", injectionKey = "CHUNK_START_POS_FIELD")
  private String chunkStartPosField = "chunk_start_position";

  /**
   * Optional input field containing the business document identifier. When empty, a row counter is
   * used.
   */
  @HopMetadataProperty(key = "sourceDocumentIdField", injectionKey = "SOURCE_DOCUMENT_ID_FIELD")
  private String sourceDocumentIdField;

  /** Field name for the original document ID (if metadata is enabled). */
  @HopMetadataProperty(key = "documentIdField", injectionKey = "DOCUMENT_ID_FIELD")
  private String documentIdField = "chunk_doc_id";

  /** Field name for the total chunk count (if metadata is enabled). */
  @HopMetadataProperty(key = "chunkCountField", injectionKey = "CHUNK_COUNT_FIELD")
  private String chunkCountField = "total_chunks";

  /** Document format for STRUCTURE strategy (Auto infers from source_type field or text). */
  @HopMetadataProperty(key = "contentType", injectionKey = "CONTENT_TYPE")
  private ContentType contentType = ContentType.AUTO;

  /**
   * Optional input field (e.g. {@code source_type}) used to pick a document parser for STRUCTURE
   * strategy.
   */
  @HopMetadataProperty(key = "contentTypeField", injectionKey = "CONTENT_TYPE_FIELD")
  private String contentTypeField;

  /** Default constructor. */
  public TextChunkerMeta() {
    super();
  }

  @Override
  public Object clone() {
    TextChunkerMeta retval = (TextChunkerMeta) super.clone();
    return retval;
  }

  @Override
  public void setDefault() {
    inputField = "";
    outputChunkField = "chunk_text";
    chunkingStrategy = ChunkingStrategyType.CHARACTER;
    chunkSize = 1000;
    chunkOverlap = 200;
    includeMetadata = true;
    chunkIndexField = "chunk_index";
    chunkStartPosField = "chunk_start_position";
    documentIdField = "chunk_doc_id";
    chunkCountField = "total_chunks";
    contentType = ContentType.AUTO;
    contentTypeField = "";
  }

  @Override
  public void getFields(
      IRowMeta row,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    try {
      addField(row, new ValueMetaString(outputChunkField), origin);

      if (includeMetadata) {
        addField(row, new ValueMetaInteger(chunkIndexField), origin);
        addField(row, new ValueMetaInteger(chunkStartPosField), origin);
        addField(row, new ValueMetaString(documentIdField), origin);
        addField(row, new ValueMetaInteger(chunkCountField), origin);
      }
    } catch (Exception e) {
      throw new HopTransformException("Error creating output fields", e);
    }
  }

  private static void addField(IRowMeta row, IValueMeta field, String origin)
      throws HopPluginException {
    String fieldName = field.getName();
    if (fieldName == null || fieldName.isEmpty() || row.indexOfValue(fieldName) >= 0) {
      return;
    }
    field.setOrigin(origin);
    row.addValueMeta(field);
  }

  /**
   * Gets the output field names that will be added by this transform.
   *
   * @return List of output field names
   */
  public List<String> getOutputFieldNames() {
    List<String> fields = new ArrayList<>();
    fields.add(outputChunkField);
    if (includeMetadata) {
      fields.add(chunkIndexField);
      fields.add(chunkStartPosField);
      fields.add(documentIdField);
      fields.add(chunkCountField);
    }
    return fields;
  }

  /**
   * Validates the configuration.
   *
   * @param remarks List to add validation messages to
   * @param pipelineMeta The pipeline metadata
   * @param transformMeta The transform metadata
   * @param prev The previous transform's row metadata
   * @param input The input field names
   * @param output The output field names
   * @param info The info row metadata
   * @param variables The variables
   * @param metadataProvider The metadata provider
   */
  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    if (Utils.isEmpty(inputField)) {
      error(remarks, transformMeta, "TextChunker.Validation.InputFieldRequired");
    } else if (prev != null && prev.indexOfValue(inputField) < 0) {
      error(remarks, transformMeta, "TextChunker.Validation.InputFieldNotFound", inputField);
    }

    if (!Utils.isEmpty(sourceDocumentIdField)
        && prev != null
        && prev.indexOfValue(sourceDocumentIdField) < 0) {
      error(
          remarks,
          transformMeta,
          "TextChunker.Validation.SourceDocumentIdFieldNotFound",
          sourceDocumentIdField);
    }

    if (!Utils.isEmpty(contentTypeField)
        && prev != null
        && prev.indexOfValue(contentTypeField) < 0) {
      error(
          remarks,
          transformMeta,
          "TextChunker.Validation.ContentTypeFieldNotFound",
          contentTypeField);
    }

    if (Utils.isEmpty(outputChunkField)) {
      error(remarks, transformMeta, "TextChunker.Validation.OutputChunkFieldRequired");
    }

    if (chunkSize <= 0) {
      error(remarks, transformMeta, "TextChunker.Validation.ChunkSizePositive");
    }

    if (chunkOverlap < 0) {
      error(remarks, transformMeta, "TextChunker.Validation.OverlapNonNegative");
    } else if (chunkOverlap >= chunkSize) {
      warning(remarks, transformMeta, "TextChunker.Validation.OverlapWarning");
    }

    if (chunkingStrategy == ChunkingStrategyType.PARAGRAPH && chunkOverlap > 0) {
      warning(remarks, transformMeta, "TextChunker.Validation.ParagraphOverlapIgnored");
    }
  }

  private static void error(
      List<ICheckResult> remarks, TransformMeta transformMeta, String key, String... parameters) {
    remarks.add(
        new CheckResult(
            ICheckResult.TYPE_RESULT_ERROR,
            BaseMessages.getString(PKG, key, parameters),
            transformMeta));
  }

  private static void warning(
      List<ICheckResult> remarks, TransformMeta transformMeta, String key, String... parameters) {
    remarks.add(
        new CheckResult(
            ICheckResult.TYPE_RESULT_WARNING,
            BaseMessages.getString(PKG, key, parameters),
            transformMeta));
  }
}
