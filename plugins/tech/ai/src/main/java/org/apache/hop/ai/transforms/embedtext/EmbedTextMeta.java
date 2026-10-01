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

import java.util.List;
import java.util.function.IntPredicate;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Getter
@Setter
@Transform(
    id = "EmbedText",
    image = "embedtext.svg",
    name = "i18n::EmbedText.Name",
    description = "i18n::EmbedText.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.AI",
    keywords = "embedding,vector,ai,rag,retrieval,semantic",
    documentationUrl = "/pipeline/transforms/embedtext.html",
    // Shares a child-first classloader with hop-tech-ai, which supplies AiEmbeddingFactory
    // and langchain4j. Those jars are therefore excluded from this plugin's own lib.
    classLoaderGroup = "hop-ai")
@GuiPlugin(classLoaderGroup = "hop-ai")
public class EmbedTextMeta extends BaseTransformMeta<EmbedText, EmbedTextData> {

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "EMBED_TEXT_DIALOG_OPTIONS";
  public static final String WIDGET_INPUT_FIELD = "EMBED_TEXT_INPUT_FIELD";
  public static final String WIDGET_MODEL_NAME = "EMBED_TEXT_MODEL_NAME";

  private static final String TAB_MAIN = "i18n::EmbedText.Tab.Main";
  private static final String TAB_MAIN_ORDER = "0100";

  /** The dimension of an OpenAI text-embedding-3-small vector, and the Vector value type's id. */
  private static final int TYPE_VECTOR = 1536;

  private static final Class<?> PKG = EmbedTextMeta.class;

  @GuiWidgetElement(
      order = "0100",
      type = GuiElementType.METADATA,
      metadata = AiProvider.class,
      label = "i18n::EmbedText.aiProvider.Label",
      toolTip = "i18n::EmbedText.aiProvider.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(
      key = "ai_provider",
      injectionKey = "AI_PROVIDER",
      injectionKeyDescription = "EmbedTextMeta.Injection.AI_PROVIDER")
  private String aiProvider;

  @GuiWidgetElement(
      id = WIDGET_MODEL_NAME,
      order = "0200",
      type = GuiElementType.TEXT,
      label = "i18n::EmbedText.modelName.Label",
      toolTip = "i18n::EmbedText.modelName.Tooltip",
      variables = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(
      key = "model_name",
      injectionKey = "MODEL_NAME",
      injectionKeyDescription = "EmbedTextMeta.Injection.MODEL_NAME")
  private String modelName = "";

  @GuiWidgetElement(
      id = WIDGET_INPUT_FIELD,
      order = "0300",
      type = GuiElementType.COMBO,
      label = "i18n::EmbedText.inputField.Label",
      toolTip = "i18n::EmbedText.inputField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(
      key = "input_field",
      injectionKey = "INPUT_FIELD",
      injectionKeyDescription = "EmbedTextMeta.Injection.INPUT_FIELD")
  private String inputField = "chunk_text";

  @GuiWidgetElement(
      order = "0400",
      type = GuiElementType.TEXT,
      label = "i18n::EmbedText.outputField.Label",
      toolTip = "i18n::EmbedText.outputField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(
      key = "output_field",
      injectionKey = "OUTPUT_FIELD",
      injectionKeyDescription = "EmbedTextMeta.Injection.OUTPUT_FIELD")
  private String outputField = "embedding";

  @GuiWidgetElement(
      order = "0500",
      type = GuiElementType.COMBO,
      label = "i18n::EmbedText.outputFormat.Label",
      toolTip = "i18n::EmbedText.outputFormat.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(
      key = "output_format",
      injectionKey = "OUTPUT_FORMAT",
      injectionKeyDescription = "EmbedTextMeta.Injection.OUTPUT_FORMAT")
  private EmbedTextOutputFormat outputFormat = EmbedTextOutputFormat.STRING;

  @GuiWidgetElement(
      order = "0600",
      type = GuiElementType.TEXT,
      label = "i18n::EmbedText.batchSize.Label",
      toolTip = "i18n::EmbedText.batchSize.Tooltip",
      variables = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(
      key = "batch_size",
      injectionKey = "BATCH_SIZE",
      injectionKeyDescription = "EmbedTextMeta.Injection.BATCH_SIZE")
  private String batchSize = "16";

  @GuiWidgetElement(
      order = "0700",
      type = GuiElementType.CHECKBOX,
      label = "i18n::EmbedText.includeModelMetadata.Label",
      toolTip = "i18n::EmbedText.includeModelMetadata.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(
      key = "include_model_metadata",
      injectionKey = "INCLUDE_MODEL_METADATA",
      injectionKeyDescription = "EmbedTextMeta.Injection.INCLUDE_MODEL_METADATA")
  private boolean includeModelMetadata = true;

  @GuiWidgetElement(
      order = "0800",
      type = GuiElementType.TEXT,
      label = "i18n::EmbedText.modelField.Label",
      toolTip = "i18n::EmbedText.modelField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(
      key = "model_field",
      injectionKey = "MODEL_FIELD",
      injectionKeyDescription = "EmbedTextMeta.Injection.MODEL_FIELD")
  private String modelField = "embedding_model";

  @GuiWidgetElement(
      order = "0900",
      type = GuiElementType.TEXT,
      label = "i18n::EmbedText.dimensionsField.Label",
      toolTip = "i18n::EmbedText.dimensionsField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(
      key = "dimensions_field",
      injectionKey = "DIMENSIONS_FIELD",
      injectionKeyDescription = "EmbedTextMeta.Injection.DIMENSIONS_FIELD")
  private String dimensionsField = "embedding_dimensions";

  @Override
  public void setDefault() {
    aiProvider = "";
    modelName = "";
    inputField = "chunk_text";
    outputField = "embedding";
    outputFormat = EmbedTextOutputFormat.STRING;
    batchSize = "16";
    includeModelMetadata = true;
    modelField = "embedding_model";
    dimensionsField = "embedding_dimensions";
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
    // EmbedText resolves the same three names once per run and writes the slots in this order.
    // Both sides have to judge emptiness on the resolved value, or a variable that resolves to
    // nothing adds no column here while the transform still writes one.
    addField(row, origin, variables.resolve(outputField), embeddingType());
    if (includeModelMetadata) {
      addField(row, origin, variables.resolve(modelField), IValueMeta.TYPE_STRING);
      addField(row, origin, variables.resolve(dimensionsField), IValueMeta.TYPE_INTEGER);
    }
  }

  /**
   * The Vector value type is an optional plugin. When it is not installed the embedding falls back
   * to a String holding a JSON array, which every downstream transform can still read.
   */
  private int embeddingType() {
    if (outputFormat != EmbedTextOutputFormat.VECTOR) {
      return IValueMeta.TYPE_STRING;
    }
    try {
      ValueMetaFactory.createValueMeta("probe", TYPE_VECTOR);
      return TYPE_VECTOR;
    } catch (Exception e) {
      return IValueMeta.TYPE_STRING;
    }
  }

  private static void addField(IRowMeta row, String origin, String name, int type)
      throws HopTransformException {
    if (Utils.isEmpty(name)) {
      return;
    }
    try {
      IValueMeta value = ValueMetaFactory.createValueMeta(name, type);
      value.setOrigin(origin);
      row.addValueMeta(value);
    } catch (Exception e) {
      throw new HopTransformException("Unable to add output field '" + name + "'", e);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

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

    if (Utils.isEmpty(aiProvider)) {
      error(remarks, transformMeta, "EmbedText.Validation.ProviderRequired");
    }
    if (Utils.isEmpty(inputField)) {
      error(remarks, transformMeta, "EmbedText.Validation.InputFieldRequired");
    } else if (prev != null && prev.indexOfValue(inputField) < 0) {
      error(remarks, transformMeta, "EmbedText.Validation.InputFieldNotFound", inputField);
    }
    if (Utils.isEmpty(outputField)) {
      error(remarks, transformMeta, "EmbedText.Validation.OutputFieldRequired");
    }
    // Batch size accepts variables, which a design-time check cannot resolve. Only a value that is
    // genuinely fixed can be judged here.
    if (isResolvedNumberBad(variables, batchSize, size -> size < 1)) {
      error(remarks, transformMeta, "EmbedText.Validation.BatchSizePositive");
    }
    if (includeModelMetadata && Utils.isEmpty(modelField) && Utils.isEmpty(dimensionsField)) {
      warning(remarks, transformMeta, "EmbedText.Validation.NoMetadataFields");
    }
  }

  private static boolean isResolvedNumberBad(
      IVariables variables, String value, IntPredicate invalid) {
    String resolved = variables.resolve(value);
    if (StringUtil.containsVariableToken(resolved)) {
      return false;
    }
    return invalid.test(Const.toInt(resolved, -1));
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
