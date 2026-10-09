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

package org.apache.hop.neo4j.transforms.schema;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.graph.GraphDatabaseMeta;
import org.apache.hop.core.graph.GraphSchema;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.neo4j.shared.GraphConnectionSelectionLine;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Reads the labels, relationship types and properties of a graph database, one row each. */
@Transform(
    id = "GetGraphSchema",
    name = "i18n::GetGraphSchema.Name",
    description = "i18n::GetGraphSchema.Description",
    image = "graph_schema.svg",
    categoryDescription = "Graph",
    keywords = "i18n::GetGraphSchema.Keywords",
    documentationUrl = "/pipeline/transforms/get-graph-schema.html")
@GuiPlugin
@Getter
@Setter
public class GetGraphSchemaMeta extends BaseTransformMeta<GetGraphSchema, GetGraphSchemaData> {

  private static final Class<?> PKG = GetGraphSchemaMeta.class;

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "GET_GRAPH_SCHEMA_DIALOG_OPTIONS";

  private static final String TAB_SCHEMA = "i18n::GetGraphSchema.Tab.Schema";
  private static final String TAB_SCHEMA_ORDER = "0100";
  private static final String TAB_FIELDS = "i18n::GetGraphSchema.Tab.Fields";
  private static final String TAB_FIELDS_ORDER = "0200";

  @GuiWidgetElement(
      id = "connection",
      order = "0100",
      type = GuiElementType.METADATA,
      metadata = GraphDatabaseMeta.class,
      metadataSelectionLine = GraphConnectionSelectionLine.class,
      label = "i18n::GetGraphSchema.Connection.Label",
      toolTip = "i18n::GetGraphSchema.Connection.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_SCHEMA,
      groupOrder = TAB_SCHEMA_ORDER)
  @HopMetadataProperty(
      key = "connection",
      injectionKey = "CONNECTION",
      injectionKeyDescription = "GetGraphSchema.Injection.CONNECTION")
  private String connection;

  @GuiWidgetElement(
      id = "sampleSize",
      order = "0200",
      type = GuiElementType.TEXT,
      label = "i18n::GetGraphSchema.SampleSize.Label",
      toolTip = "i18n::GetGraphSchema.SampleSize.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_SCHEMA,
      groupOrder = TAB_SCHEMA_ORDER)
  @HopMetadataProperty(
      key = "sample_size",
      injectionKey = "SAMPLE_SIZE",
      injectionKeyDescription = "GetGraphSchema.Injection.SAMPLE_SIZE")
  private String sampleSize;

  @GuiWidgetElement(
      id = "elementTypeField",
      order = "0100",
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::GetGraphSchema.ElementTypeField.Label",
      toolTip = "i18n::GetGraphSchema.ElementTypeField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_FIELDS,
      groupOrder = TAB_FIELDS_ORDER)
  @HopMetadataProperty(key = "element_type_field")
  private String elementTypeField;

  @GuiWidgetElement(
      id = "nameField",
      order = "0200",
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::GetGraphSchema.NameField.Label",
      toolTip = "i18n::GetGraphSchema.NameField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_FIELDS,
      groupOrder = TAB_FIELDS_ORDER)
  @HopMetadataProperty(key = "name_field")
  private String nameField;

  @GuiWidgetElement(
      id = "propertyField",
      order = "0300",
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::GetGraphSchema.PropertyField.Label",
      toolTip = "i18n::GetGraphSchema.PropertyField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_FIELDS,
      groupOrder = TAB_FIELDS_ORDER)
  @HopMetadataProperty(key = "property_field")
  private String propertyField;

  @GuiWidgetElement(
      id = "propertyTypesField",
      order = "0400",
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::GetGraphSchema.PropertyTypesField.Label",
      toolTip = "i18n::GetGraphSchema.PropertyTypesField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_FIELDS,
      groupOrder = TAB_FIELDS_ORDER)
  @HopMetadataProperty(key = "property_types_field")
  private String propertyTypesField;

  @GuiWidgetElement(
      id = "mandatoryField",
      order = "0500",
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::GetGraphSchema.MandatoryField.Label",
      toolTip = "i18n::GetGraphSchema.MandatoryField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_FIELDS,
      groupOrder = TAB_FIELDS_ORDER)
  @HopMetadataProperty(key = "mandatory_field")
  private String mandatoryField;

  @GuiWidgetElement(
      id = "indexedField",
      order = "0600",
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::GetGraphSchema.IndexedField.Label",
      toolTip = "i18n::GetGraphSchema.IndexedField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_FIELDS,
      groupOrder = TAB_FIELDS_ORDER)
  @HopMetadataProperty(key = "indexed_field")
  private String indexedField;

  @GuiWidgetElement(
      id = "uniqueField",
      order = "0700",
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::GetGraphSchema.UniqueField.Label",
      toolTip = "i18n::GetGraphSchema.UniqueField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_FIELDS,
      groupOrder = TAB_FIELDS_ORDER)
  @HopMetadataProperty(key = "unique_field")
  private String uniqueField;

  @GuiWidgetElement(
      id = "startLabelsField",
      order = "0800",
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::GetGraphSchema.StartLabelsField.Label",
      toolTip = "i18n::GetGraphSchema.StartLabelsField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_FIELDS,
      groupOrder = TAB_FIELDS_ORDER)
  @HopMetadataProperty(key = "start_labels_field")
  private String startLabelsField;

  @GuiWidgetElement(
      id = "endLabelsField",
      order = "0900",
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::GetGraphSchema.EndLabelsField.Label",
      toolTip = "i18n::GetGraphSchema.EndLabelsField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = TAB_FIELDS,
      groupOrder = TAB_FIELDS_ORDER)
  @HopMetadataProperty(key = "end_labels_field")
  private String endLabelsField;

  public GetGraphSchemaMeta() {
    setDefault();
  }

  @Override
  public void setDefault() {
    connection = "";
    sampleSize = Integer.toString(GraphSchema.DEFAULT_SAMPLE_SIZE);
    elementTypeField = "element_type";
    nameField = "name";
    propertyField = "property";
    propertyTypesField = "property_types";
    mandatoryField = "mandatory";
    indexedField = "indexed";
    uniqueField = "unique";
    startLabelsField = "start_labels";
    endLabelsField = "end_labels";
  }

  /** The output: one row per schema entry, whatever comes in. */
  @Override
  public void getFields(
      IRowMeta row,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    row.clear();
    addField(row, new ValueMetaString(elementTypeField), origin);
    addField(row, new ValueMetaString(nameField), origin);
    addField(row, new ValueMetaString(propertyField), origin);
    addField(row, new ValueMetaString(propertyTypesField), origin);
    addField(row, new ValueMetaBoolean(mandatoryField), origin);
    addField(row, new ValueMetaBoolean(indexedField), origin);
    addField(row, new ValueMetaBoolean(uniqueField), origin);
    addField(row, new ValueMetaString(startLabelsField), origin);
    addField(row, new ValueMetaString(endLabelsField), origin);
  }

  /** Fields without a name are left out. */
  private static void addField(IRowMeta row, IValueMeta field, String origin) {
    if (Utils.isEmpty(field.getName())) {
      return;
    }
    field.setOrigin(origin);
    row.addValueMeta(field);
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
    if (Utils.isEmpty(connection)) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GetGraphSchema.Error.NoConnection"),
              transformMeta));
    }
    String resolved = variables.resolve(sampleSize);
    if (!Utils.isEmpty(resolved)
        && !StringUtil.containsVariableToken(resolved)
        && Const.toInt(resolved, -1) < 1) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GetGraphSchema.Error.SampleSize", resolved),
              transformMeta));
    }
    if (input != null && input.length > 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "GetGraphSchema.Warning.InputIgnored"),
              transformMeta));
    }
  }
}
