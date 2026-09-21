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
package org.apache.hop.ai.transforms.structuredextract;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
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
    id = "StructuredExtract",
    image = "structuredextract.svg",
    name = "i18n::StructuredExtract.Name",
    description = "i18n::StructuredExtract.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.AI",
    keywords = "ai,extract,structured,json,schema,llm,classify,entities",
    documentationUrl = "/pipeline/transforms/structuredextract.html",
    classLoaderGroup = "hop-ai")
@GuiPlugin(classLoaderGroup = "hop-ai")
public class StructuredExtractMeta
    extends BaseTransformMeta<StructuredExtract, StructuredExtractData> {

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "STRUCTURED_EXTRACT_DIALOG_OPTIONS";
  public static final String WIDGET_INPUT_FIELD = "STRUCTURED_EXTRACT_INPUT_FIELD";

  private static final String TAB_MAIN = "i18n::StructuredExtract.Tab.Main";
  private static final String TAB_MAIN_ORDER = "0100";

  private static final Class<?> PKG = StructuredExtractMeta.class;

  @GuiWidgetElement(
      order = "0100",
      type = GuiElementType.METADATA,
      metadata = AiProvider.class,
      label = "i18n::StructuredExtract.aiProvider.Label",
      toolTip = "i18n::StructuredExtract.aiProvider.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(
      key = "ai_provider",
      injectionKey = "AI_PROVIDER",
      injectionKeyDescription = "StructuredExtractMeta.Injection.AI_PROVIDER")
  private String aiProvider;

  @GuiWidgetElement(
      order = "0200",
      type = GuiElementType.TEXT,
      label = "i18n::StructuredExtract.modelName.Label",
      toolTip = "i18n::StructuredExtract.modelName.Tooltip",
      variables = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(
      key = "model_name",
      injectionKey = "MODEL_NAME",
      injectionKeyDescription = "StructuredExtractMeta.Injection.MODEL_NAME")
  private String modelName = "";

  @GuiWidgetElement(
      id = WIDGET_INPUT_FIELD,
      order = "0300",
      type = GuiElementType.COMBO,
      label = "i18n::StructuredExtract.inputField.Label",
      toolTip = "i18n::StructuredExtract.inputField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(
      key = "input_field",
      injectionKey = "INPUT_FIELD",
      injectionKeyDescription = "StructuredExtractMeta.Injection.INPUT_FIELD")
  private String inputField;

  @GuiWidgetElement(
      order = "0400",
      type = GuiElementType.MULTI_LINE_TEXT,
      multiLineTextHeight = 4,
      label = "i18n::StructuredExtract.instructions.Label",
      toolTip = "i18n::StructuredExtract.instructions.Tooltip",
      variables = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(
      key = "instructions",
      injectionKey = "INSTRUCTIONS",
      injectionKeyDescription = "StructuredExtractMeta.Injection.INSTRUCTIONS")
  private String instructions = "";

  /**
   * The fields to pull out. This is the heart of the transform: it becomes the schema the model is
   * constrained by, the columns added to the stream, and the types the answer is read into.
   */
  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionGroupKey = "FIELDS",
      injectionGroupDescription = "StructuredExtractMeta.Injection.FIELDS")
  private List<StructuredExtractField> fields = new ArrayList<>();

  @Override
  public void setDefault() {
    aiProvider = "";
    modelName = "";
    inputField = "";
    instructions = "";
    fields = new ArrayList<>();
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
    for (StructuredExtractField field : fields) {
      if (field == null || field.trimmedName().isEmpty()) {
        continue;
      }
      try {
        // StructuredExtract writes one value per field in this same order, so the two have to
        // agree on which fields exist and what type each one is.
        IValueMeta value =
            ValueMetaFactory.createValueMeta(field.trimmedName(), ExtractionSchema.typeOf(field));
        value.setOrigin(origin);
        row.addValueMeta(value);
      } catch (HopException e) {
        throw new HopTransformException("Unable to add output field '" + field.getName() + "'", e);
      }
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
      error(remarks, transformMeta, "StructuredExtract.Validation.ProviderRequired");
    }
    if (Utils.isEmpty(inputField)) {
      error(remarks, transformMeta, "StructuredExtract.Validation.InputFieldRequired");
    } else if (prev != null && prev.indexOfValue(inputField) < 0) {
      error(remarks, transformMeta, "StructuredExtract.Validation.InputFieldNotFound", inputField);
    }

    List<StructuredExtractField> named =
        fields.stream().filter(f -> f != null && !f.trimmedName().isEmpty()).toList();
    if (named.isEmpty()) {
      error(remarks, transformMeta, "StructuredExtract.Validation.NoFields");
      return;
    }

    Set<String> seen = new HashSet<>();
    for (StructuredExtractField field : named) {
      String name = field.trimmedName();
      if (!seen.add(name)) {
        error(remarks, transformMeta, "StructuredExtract.Validation.DuplicateField", name);
      }
      if (prev != null && prev.indexOfValue(name) >= 0) {
        error(remarks, transformMeta, "StructuredExtract.Validation.FieldAlreadyInStream", name);
      }
      try {
        ExtractionSchema.typeOf(field);
      } catch (HopException e) {
        error(remarks, transformMeta, "StructuredExtract.Validation.BadType", name, e.getMessage());
      }
      if (Utils.isEmpty(field.getDescription())) {
        warning(remarks, transformMeta, "StructuredExtract.Validation.NoDescription", name);
      }
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
