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
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
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
@GuiPlugin
public class TextChunkerMeta extends BaseTransformMeta<TextChunker, TextChunkerData> {

  private static final Class<?> PKG = TextChunkerMeta.class;

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "TEXT_CHUNKER_DIALOG_OPTIONS";
  public static final String WIDGET_INPUT_FIELD = "TEXT_CHUNKER_INPUT_FIELD";
  public static final String WIDGET_SOURCE_DOCUMENT_ID_FIELD = "TEXT_CHUNKER_SOURCE_DOC_ID_FIELD";
  public static final String WIDGET_CONTENT_TYPE = "TEXT_CHUNKER_CONTENT_TYPE";
  public static final String WIDGET_CONTENT_TYPE_FIELD = "TEXT_CHUNKER_CONTENT_TYPE_FIELD";
  public static final String WIDGET_CHUNKING_STRATEGY = "TEXT_CHUNKER_CHUNKING_STRATEGY";
  public static final String WIDGET_INCLUDE_METADATA = "TEXT_CHUNKER_INCLUDE_METADATA";
  public static final String WIDGET_CHUNK_INDEX_FIELD = "TEXT_CHUNKER_CHUNK_INDEX_FIELD";
  public static final String WIDGET_CHUNK_START_POS_FIELD = "TEXT_CHUNKER_CHUNK_START_POS_FIELD";
  public static final String WIDGET_DOCUMENT_ID_FIELD = "TEXT_CHUNKER_DOCUMENT_ID_FIELD";
  public static final String WIDGET_CHUNK_COUNT_FIELD = "TEXT_CHUNKER_CHUNK_COUNT_FIELD";

  private static final String GROUP_INPUT = "Input";
  private static final String GROUP_CHUNKING = "Chunking";
  private static final String GROUP_METADATA = "Chunk metadata";

  /** The field containing text to chunk. */
  @GuiWidgetElement(
      id = WIDGET_INPUT_FIELD,
      order = "0100",
      type = GuiElementType.COMBO,
      label = "i18n::TextChunker.inputField.Label",
      toolTip = "i18n::TextChunker.inputField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_INPUT)
  @HopMetadataProperty(key = "inputField", injectionKey = "INPUT_FIELD")
  private String inputField;

  /** The name of the field to output chunks to. */
  @GuiWidgetElement(
      order = "0300",
      type = GuiElementType.TEXT,
      label = "i18n::TextChunker.outputChunkField.Label",
      toolTip = "i18n::TextChunker.outputChunkField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_INPUT)
  @HopMetadataProperty(key = "outputChunkField", injectionKey = "OUTPUT_CHUNK_FIELD")
  private String outputChunkField = "chunk_text";

  /** The chunking strategy to use. */
  @GuiWidgetElement(
      id = WIDGET_CHUNKING_STRATEGY,
      order = "0400",
      type = GuiElementType.COMBO,
      label = "i18n::TextChunker.chunkingStrategy.Label",
      toolTip = "i18n::TextChunker.chunkingStrategy.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_CHUNKING)
  @HopMetadataProperty(key = "chunkingStrategy", injectionKey = "CHUNKING_STRATEGY")
  private ChunkingStrategyType chunkingStrategy = ChunkingStrategyType.CHARACTER;

  /**
   * The maximum size for each chunk (characters for CHARACTER strategy, approximate for PARAGRAPH).
   */
  @GuiWidgetElement(
      order = "0700",
      type = GuiElementType.TEXT,
      label = "i18n::TextChunker.chunkSize.Label",
      toolTip = "i18n::TextChunker.chunkSize.Tooltip",
      variables = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_CHUNKING)
  @HopMetadataProperty(key = "chunkSize", injectionKey = "CHUNK_SIZE")
  private String chunkSize = "1000";

  /** The number of characters to overlap between chunks (for CHARACTER strategy). */
  @GuiWidgetElement(
      order = "0800",
      type = GuiElementType.TEXT,
      label = "i18n::TextChunker.chunkOverlap.Label",
      toolTip = "i18n::TextChunker.chunkOverlap.Tooltip",
      variables = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_CHUNKING)
  @HopMetadataProperty(key = "chunkOverlap", injectionKey = "CHUNK_OVERLAP")
  private String chunkOverlap = "200";

  /** Whether to include metadata fields in the output. */
  @GuiWidgetElement(
      id = WIDGET_INCLUDE_METADATA,
      order = "0900",
      type = GuiElementType.CHECKBOX,
      label = "i18n::TextChunker.includeMetadata.Label",
      toolTip = "i18n::TextChunker.includeMetadata.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_METADATA)
  @HopMetadataProperty(key = "includeMetadata", injectionKey = "INCLUDE_METADATA")
  private boolean includeMetadata = true;

  /** Field name for the chunk index (if metadata is enabled). */
  @GuiWidgetElement(
      id = WIDGET_CHUNK_INDEX_FIELD,
      order = "1000",
      type = GuiElementType.TEXT,
      label = "i18n::TextChunker.chunkIndexField.Label",
      toolTip = "i18n::TextChunker.chunkIndexField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_METADATA)
  @HopMetadataProperty(key = "chunkIndexField", injectionKey = "CHUNK_INDEX_FIELD")
  private String chunkIndexField = "chunk_index";

  /** Field name for the chunk start position (if metadata is enabled). */
  @GuiWidgetElement(
      id = WIDGET_CHUNK_START_POS_FIELD,
      order = "1100",
      type = GuiElementType.TEXT,
      label = "i18n::TextChunker.chunkStartPosField.Label",
      toolTip = "i18n::TextChunker.chunkStartPosField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_METADATA)
  @HopMetadataProperty(key = "chunkStartPosField", injectionKey = "CHUNK_START_POS_FIELD")
  private String chunkStartPosField = "chunk_start_position";

  /**
   * Optional input field containing the business document identifier. When empty, a row counter is
   * used.
   */
  @GuiWidgetElement(
      id = WIDGET_SOURCE_DOCUMENT_ID_FIELD,
      order = "0200",
      type = GuiElementType.COMBO,
      label = "i18n::TextChunker.sourceDocumentIdField.Label",
      toolTip = "i18n::TextChunker.sourceDocumentIdField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_INPUT)
  @HopMetadataProperty(key = "sourceDocumentIdField", injectionKey = "SOURCE_DOCUMENT_ID_FIELD")
  private String sourceDocumentIdField;

  /** Field name for the original document ID (if metadata is enabled). */
  @GuiWidgetElement(
      id = WIDGET_DOCUMENT_ID_FIELD,
      order = "1200",
      type = GuiElementType.TEXT,
      label = "i18n::TextChunker.documentIdField.Label",
      toolTip = "i18n::TextChunker.documentIdField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_METADATA)
  @HopMetadataProperty(key = "documentIdField", injectionKey = "DOCUMENT_ID_FIELD")
  private String documentIdField = "chunk_doc_id";

  /** Field name for the total chunk count (if metadata is enabled). */
  @GuiWidgetElement(
      id = WIDGET_CHUNK_COUNT_FIELD,
      order = "1300",
      type = GuiElementType.TEXT,
      label = "i18n::TextChunker.chunkCountField.Label",
      toolTip = "i18n::TextChunker.chunkCountField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_METADATA)
  @HopMetadataProperty(key = "chunkCountField", injectionKey = "CHUNK_COUNT_FIELD")
  private String chunkCountField = "total_chunks";

  /** Document format for STRUCTURE strategy (Auto infers from source_type field or text). */
  @GuiWidgetElement(
      id = WIDGET_CONTENT_TYPE,
      order = "0500",
      type = GuiElementType.COMBO,
      label = "i18n::TextChunker.contentType.Label",
      toolTip = "i18n::TextChunker.contentType.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_CHUNKING)
  @HopMetadataProperty(key = "contentType", injectionKey = "CONTENT_TYPE")
  private ContentType contentType = ContentType.AUTO;

  /**
   * Optional input field (e.g. {@code source_type}) used to pick a document parser for STRUCTURE
   * strategy.
   */
  @GuiWidgetElement(
      id = WIDGET_CONTENT_TYPE_FIELD,
      order = "0600",
      type = GuiElementType.COMBO,
      label = "i18n::TextChunker.contentTypeField.Label",
      toolTip = "i18n::TextChunker.contentTypeField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_CHUNKING)
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
    chunkSize = "1000";
    chunkOverlap = "200";
    includeMetadata = true;
    chunkIndexField = "chunk_index";
    chunkStartPosField = "chunk_start_position";
    documentIdField = "chunk_doc_id";
    chunkCountField = "total_chunks";
    contentType = ContentType.AUTO;
    contentTypeField = "";
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
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

    int resolvedSize = Const.toInt(variables.resolve(chunkSize), -1);
    int resolvedOverlap = Const.toInt(variables.resolve(chunkOverlap), -1);

    if (resolvedSize <= 0) {
      error(remarks, transformMeta, "TextChunker.Validation.ChunkSizePositive");
    }

    if (resolvedOverlap < 0) {
      error(remarks, transformMeta, "TextChunker.Validation.OverlapNonNegative");
    } else if (resolvedSize > 0 && resolvedOverlap >= resolvedSize) {
      warning(remarks, transformMeta, "TextChunker.Validation.OverlapWarning");
    }

    if (chunkingStrategy == ChunkingStrategyType.PARAGRAPH && resolvedOverlap > 0) {
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
