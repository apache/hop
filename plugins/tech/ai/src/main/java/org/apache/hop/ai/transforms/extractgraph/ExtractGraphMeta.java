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

package org.apache.hop.ai.transforms.extractgraph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Reads the entities in a text and the relationships between them, for a knowledge graph.
 *
 * <p>Every input row becomes one output row per entity and per relationship the model finds, with
 * the input fields kept, so the document and chunk the element came from stay attached to it.
 */
@Getter
@Setter
@Transform(
    id = "ExtractGraph",
    image = "extractgraph.svg",
    name = "i18n::ExtractGraph.Name",
    description = "i18n::ExtractGraph.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.AI",
    keywords = "ai,graph,knowledge graph,graphrag,entities,relationships,extract,llm",
    documentationUrl = "/pipeline/transforms/extractgraph.html",
    classLoaderGroup = "hop-ai")
@GuiPlugin(classLoaderGroup = "hop-ai")
public class ExtractGraphMeta extends BaseTransformMeta<ExtractGraph, ExtractGraphData> {

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "EXTRACT_GRAPH_DIALOG_OPTIONS";
  public static final String WIDGET_INPUT_FIELD = "EXTRACT_GRAPH_INPUT_FIELD";

  /** The kind field holds one of these two values. */
  public static final String KIND_ENTITY = "ENTITY";

  public static final String KIND_RELATIONSHIP = "RELATIONSHIP";

  private static final String TAB_MAIN = "i18n::ExtractGraph.Tab.Main";
  private static final String TAB_MAIN_ORDER = "0100";
  private static final String TAB_OUTPUT = "i18n::ExtractGraph.Tab.Output";
  private static final String TAB_OUTPUT_ORDER = "0200";

  private static final Class<?> PKG = ExtractGraphMeta.class;

  @GuiWidgetElement(
      order = "0100",
      type = GuiElementType.METADATA,
      metadata = AiProvider.class,
      label = "i18n::ExtractGraph.aiProvider.Label",
      toolTip = "i18n::ExtractGraph.aiProvider.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(
      key = "ai_provider",
      injectionKey = "AI_PROVIDER",
      injectionKeyDescription = "ExtractGraphMeta.Injection.AI_PROVIDER")
  private String aiProvider;

  @GuiWidgetElement(
      order = "0200",
      type = GuiElementType.TEXT,
      label = "i18n::ExtractGraph.modelName.Label",
      toolTip = "i18n::ExtractGraph.modelName.Tooltip",
      variables = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(
      key = "model_name",
      injectionKey = "MODEL_NAME",
      injectionKeyDescription = "ExtractGraphMeta.Injection.MODEL_NAME")
  private String modelName = "";

  @GuiWidgetElement(
      id = WIDGET_INPUT_FIELD,
      order = "0300",
      type = GuiElementType.COMBO,
      label = "i18n::ExtractGraph.inputField.Label",
      toolTip = "i18n::ExtractGraph.inputField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(
      key = "input_field",
      injectionKey = "INPUT_FIELD",
      injectionKeyDescription = "ExtractGraphMeta.Injection.INPUT_FIELD")
  private String inputField;

  @GuiWidgetElement(
      order = "0400",
      type = GuiElementType.TEXT,
      label = "i18n::ExtractGraph.entityTypes.Label",
      toolTip = "i18n::ExtractGraph.entityTypes.Tooltip",
      variables = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(
      key = "entity_types",
      injectionKey = "ENTITY_TYPES",
      injectionKeyDescription = "ExtractGraphMeta.Injection.ENTITY_TYPES")
  private String entityTypes = "";

  @GuiWidgetElement(
      order = "0500",
      type = GuiElementType.TEXT,
      label = "i18n::ExtractGraph.relationshipTypes.Label",
      toolTip = "i18n::ExtractGraph.relationshipTypes.Tooltip",
      variables = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(
      key = "relationship_types",
      injectionKey = "RELATIONSHIP_TYPES",
      injectionKeyDescription = "ExtractGraphMeta.Injection.RELATIONSHIP_TYPES")
  private String relationshipTypes = "";

  @GuiWidgetElement(
      order = "0600",
      type = GuiElementType.MULTI_LINE_TEXT,
      multiLineTextHeight = 4,
      label = "i18n::ExtractGraph.instructions.Label",
      toolTip = "i18n::ExtractGraph.instructions.Tooltip",
      variables = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(
      key = "instructions",
      injectionKey = "INSTRUCTIONS",
      injectionKeyDescription = "ExtractGraphMeta.Injection.INSTRUCTIONS")
  private String instructions = "";

  @GuiWidgetElement(
      order = "0700",
      type = GuiElementType.CHECKBOX,
      label = "i18n::ExtractGraph.passRowsWithoutResults.Label",
      toolTip = "i18n::ExtractGraph.passRowsWithoutResults.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_MAIN,
      groupOrder = TAB_MAIN_ORDER)
  @HopMetadataProperty(
      key = "pass_rows_without_results",
      injectionKey = "PASS_ROWS_WITHOUT_RESULTS",
      injectionKeyDescription = "ExtractGraphMeta.Injection.PASS_ROWS_WITHOUT_RESULTS")
  private boolean passRowsWithoutResults;

  @GuiWidgetElement(
      order = "0100",
      type = GuiElementType.TEXT,
      label = "i18n::ExtractGraph.kindField.Label",
      toolTip = "i18n::ExtractGraph.kindField.Tooltip",
      variables = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_OUTPUT,
      groupOrder = TAB_OUTPUT_ORDER)
  @HopMetadataProperty(
      key = "kind_field",
      injectionKey = "KIND_FIELD",
      injectionKeyDescription = "ExtractGraphMeta.Injection.KIND_FIELD")
  private String kindField = "graph_element";

  @GuiWidgetElement(
      order = "0200",
      type = GuiElementType.TEXT,
      label = "i18n::ExtractGraph.nameField.Label",
      toolTip = "i18n::ExtractGraph.nameField.Tooltip",
      variables = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_OUTPUT,
      groupOrder = TAB_OUTPUT_ORDER)
  @HopMetadataProperty(
      key = "name_field",
      injectionKey = "NAME_FIELD",
      injectionKeyDescription = "ExtractGraphMeta.Injection.NAME_FIELD")
  private String nameField = "name";

  @GuiWidgetElement(
      order = "0300",
      type = GuiElementType.TEXT,
      label = "i18n::ExtractGraph.typeField.Label",
      toolTip = "i18n::ExtractGraph.typeField.Tooltip",
      variables = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_OUTPUT,
      groupOrder = TAB_OUTPUT_ORDER)
  @HopMetadataProperty(
      key = "type_field",
      injectionKey = "TYPE_FIELD",
      injectionKeyDescription = "ExtractGraphMeta.Injection.TYPE_FIELD")
  private String typeField = "type";

  @GuiWidgetElement(
      order = "0400",
      type = GuiElementType.TEXT,
      label = "i18n::ExtractGraph.descriptionField.Label",
      toolTip = "i18n::ExtractGraph.descriptionField.Tooltip",
      variables = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_OUTPUT,
      groupOrder = TAB_OUTPUT_ORDER)
  @HopMetadataProperty(
      key = "description_field",
      injectionKey = "DESCRIPTION_FIELD",
      injectionKeyDescription = "ExtractGraphMeta.Injection.DESCRIPTION_FIELD")
  private String descriptionField = "description";

  @GuiWidgetElement(
      order = "0500",
      type = GuiElementType.TEXT,
      label = "i18n::ExtractGraph.sourceField.Label",
      toolTip = "i18n::ExtractGraph.sourceField.Tooltip",
      variables = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_OUTPUT,
      groupOrder = TAB_OUTPUT_ORDER)
  @HopMetadataProperty(
      key = "source_field",
      injectionKey = "SOURCE_FIELD",
      injectionKeyDescription = "ExtractGraphMeta.Injection.SOURCE_FIELD")
  private String sourceField = "source";

  @GuiWidgetElement(
      order = "0600",
      type = GuiElementType.TEXT,
      label = "i18n::ExtractGraph.targetField.Label",
      toolTip = "i18n::ExtractGraph.targetField.Tooltip",
      variables = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_OUTPUT,
      groupOrder = TAB_OUTPUT_ORDER)
  @HopMetadataProperty(
      key = "target_field",
      injectionKey = "TARGET_FIELD",
      injectionKeyDescription = "ExtractGraphMeta.Injection.TARGET_FIELD")
  private String targetField = "target";

  @Override
  public void setDefault() {
    aiProvider = "";
    modelName = "";
    inputField = "";
    entityTypes = "";
    relationshipTypes = "";
    instructions = "";
    passRowsWithoutResults = false;
    kindField = "graph_element";
    nameField = "name";
    typeField = "type";
    descriptionField = "description";
    sourceField = "source";
    targetField = "target";
  }

  /** The output field names, in the order the transform writes them. */
  public List<String> outputFieldNames() {
    return Arrays.asList(
        kindField, nameField, typeField, descriptionField, sourceField, targetField);
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
    for (String name : outputFieldNames()) {
      IValueMeta value = new ValueMetaString(variables.resolve(name));
      value.setOrigin(origin);
      row.addValueMeta(value);
    }
  }

  /** The comma separated types as a list, empty when any type is allowed. */
  public static List<String> splitTypes(String types) {
    List<String> list = new ArrayList<>();
    if (types != null) {
      for (String type : types.split(",")) {
        String trimmed = type.trim();
        if (!trimmed.isEmpty() && !list.contains(trimmed)) {
          list.add(trimmed);
        }
      }
    }
    return list;
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
      error(remarks, transformMeta, "ExtractGraph.Validation.ProviderRequired");
    }
    if (Utils.isEmpty(inputField)) {
      error(remarks, transformMeta, "ExtractGraph.Validation.InputFieldRequired");
    } else if (prev != null && prev.indexOfValue(inputField) < 0) {
      error(remarks, transformMeta, "ExtractGraph.Validation.InputFieldNotFound", inputField);
    }
    for (String name : outputFieldNames()) {
      if (Utils.isEmpty(name)) {
        error(remarks, transformMeta, "ExtractGraph.Validation.OutputFieldRequired");
        break;
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
}
