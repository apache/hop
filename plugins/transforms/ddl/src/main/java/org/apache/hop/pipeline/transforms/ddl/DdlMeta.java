/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.ddl;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Meta data for the DDL transform. */
@Setter
@Getter
@Transform(
    id = "DDL",
    name = "i18n::Ddl.Name",
    description = "i18n::Ddl.Description",
    image = "ddl.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
    keywords = "i18n::Ddl.keyword",
    documentationUrl = "/pipeline/transforms/ddl.html")
@GuiPlugin
public class DdlMeta extends BaseTransformMeta<Ddl, DdlData> {
  private static final Class<?> PKG = DdlMeta.class;
  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "DDL_DIALOG_OPTIONS";
  public static final String WIDGET_COLUMN_NAME_FIELD = "COLUMN_NAME_FIELD";
  public static final String WIDGET_COLUMN_TYPE_FIELD = "WIDGET_COLUMN_TYPE_FIELD";
  public static final String WIDGET_COLUMN_LENGTH_FIELD = "WIDGET_COLUMN_LENGTH_FIELD";
  public static final String WIDGET_COLUMN_PRECISION_FIELD = "WIDGET_COLUMN_PRECISION_FIELD";

  @GuiWidgetElement(
      order = "0100",
      type = GuiElementType.METADATA,
      metadata = DatabaseMeta.class,
      toolTip = "i18n::DdlMeta.ConnectionName.Tooltip",
      label = "i18n::DdlMeta.ConnectionName.Label",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty(
      key = "connection",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION,
      storeWithCode = true)
  private String connectionName;

  @GuiWidgetElement(
      order = "0200",
      type = GuiElementType.TEXT,
      toolTip = "i18n::DdlMeta.SchemaName.Tooltip",
      label = "i18n::DdlMeta.SchemaName.Label",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty(
      key = "schemaName",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SCHEMA)
  private String schemaName;

  @GuiWidgetElement(
      order = "0300",
      type = GuiElementType.TEXT,
      toolTip = "i18n::DdlMeta.TableName.Tooltip",
      label = "i18n::DdlMeta.TableName.Label",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty(
      key = "tableName",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
  private String tableName;

  @GuiWidgetElement(
      order = "0500",
      type = GuiElementType.COMBO,
      toolTip = "i18n::DdlMeta.FieldNameField.Tooltip",
      label = "i18n::DdlMeta.FieldNameField.Label",
      id = WIDGET_COLUMN_NAME_FIELD,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty
  private String fieldNameField;

  @GuiWidgetElement(
      order = "0600",
      type = GuiElementType.COMBO,
      toolTip = "i18n::DdlMeta.FieldTypeField.Tooltip",
      label = "i18n::DdlMeta.FieldTypeField.Label",
      id = WIDGET_COLUMN_TYPE_FIELD,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty
  private String fieldTypeField;

  @GuiWidgetElement(
      order = "0700",
      type = GuiElementType.COMBO,
      toolTip = "i18n::DdlMeta.FieldLengthField.Tooltip",
      label = "i18n::DdlMeta.FieldLengthField.Label",
      id = WIDGET_COLUMN_LENGTH_FIELD,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty
  private String fieldLengthField;

  @GuiWidgetElement(
      order = "0800",
      type = GuiElementType.COMBO,
      toolTip = "i18n::DdlMeta.FieldPrecisionField.Tooltip",
      label = "i18n::DdlMeta.FieldPrecisionField.Label",
      id = WIDGET_COLUMN_PRECISION_FIELD,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty
  private String fieldPrecisionField;

  @GuiWidgetElement(
      order = "0900",
      type = GuiElementType.CHECKBOX,
      toolTip = "i18n::DdlMeta.ExecutingDdl.Tooltip",
      label = "i18n::DdlMeta.ExecutingDdl.Label",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty
  private boolean executingDdl;

  @GuiWidgetElement(
      order = "1000",
      type = GuiElementType.TEXT,
      toolTip = "i18n::DdlMeta.DdlOutputField.Tooltip",
      label = "i18n::DdlMeta.DdlOutputField.Label",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty
  private String ddlOutputField;

  @GuiWidgetElement(
      order = "1100",
      type = GuiElementType.CHECKBOX,
      toolTip = "i18n::DdlMeta.DropTable.Tooltip",
      label = "i18n::DdlMeta.DropTable.Label",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty
  private boolean droppingTable;

  public DdlMeta() {
    fieldNameField = "FieldName";
    fieldTypeField = "Type";
    fieldLengthField = "Length";
    fieldPrecisionField = "Precision";
    executingDdl = true;
    ddlOutputField = "ddl";
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    inputRowMeta.clear();
    String outputFieldName = variables.resolve(ddlOutputField);
    if (StringUtils.isNotEmpty(outputFieldName)) {
      // Just one String containing the DDL in the output.
      //
      inputRowMeta.addValueMeta(new ValueMetaString(outputFieldName, 65535, -1));
    }
  }
}
