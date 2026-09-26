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

package org.apache.hop.ui.hopgui.dialog;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.schema.HopXmlSchemaExportOptions;

/** Model backing the XML schema export dialog using annotated GuiWidgetElement fields. */
@GuiPlugin(id = "xml-schema-export-dialog-model", description = "XML schema export options")
@Getter
@Setter
public class XmlSchemaExportDialogModel {

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "XmlSchemaExportDialogModel-Widgets";

  public static final String WIDGET_TARGET_FOLDER = "xml-schema-target-folder";
  public static final String WIDGET_EXPORT_PIPELINE = "xml-schema-export-pipeline";
  public static final String WIDGET_EXPORT_WORKFLOW = "xml-schema-export-workflow";
  public static final String WIDGET_EXPORT_TRANSFORMS = "xml-schema-export-transforms";
  public static final String WIDGET_EXPORT_ACTIONS = "xml-schema-export-actions";
  public static final String WIDGET_FILTER = "xml-schema-filter";
  public static final String WIDGET_STRICT_ORDER = "xml-schema-strict-order";

  @GuiWidgetElement(
      id = WIDGET_TARGET_FOLDER,
      order = "0100",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FOLDER,
      label = "i18n::XmlSchemaExportDialog.TargetFolder.Label",
      toolTip = "i18n::XmlSchemaExportDialog.TargetFolder.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::XmlSchemaExportDialog.Group.ExportSettings")
  @HopMetadataProperty
  private String targetFolder;

  @GuiWidgetElement(
      id = WIDGET_EXPORT_PIPELINE,
      order = "0200",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::XmlSchemaExportDialog.ExportPipeline.Label",
      toolTip = "i18n::XmlSchemaExportDialog.ExportPipeline.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::XmlSchemaExportDialog.Group.ExportSettings")
  @HopMetadataProperty
  private boolean exportPipeline = true;

  @GuiWidgetElement(
      id = WIDGET_EXPORT_WORKFLOW,
      order = "0300",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::XmlSchemaExportDialog.ExportWorkflow.Label",
      toolTip = "i18n::XmlSchemaExportDialog.ExportWorkflow.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::XmlSchemaExportDialog.Group.ExportSettings")
  @HopMetadataProperty
  private boolean exportWorkflow = true;

  @GuiWidgetElement(
      id = WIDGET_EXPORT_TRANSFORMS,
      order = "0400",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::XmlSchemaExportDialog.ExportTransforms.Label",
      toolTip = "i18n::XmlSchemaExportDialog.ExportTransforms.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::XmlSchemaExportDialog.Group.ExportSettings")
  @HopMetadataProperty
  private boolean exportTransforms = true;

  @GuiWidgetElement(
      id = WIDGET_EXPORT_ACTIONS,
      order = "0500",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::XmlSchemaExportDialog.ExportActions.Label",
      toolTip = "i18n::XmlSchemaExportDialog.ExportActions.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::XmlSchemaExportDialog.Group.ExportSettings")
  @HopMetadataProperty
  private boolean exportActions = true;

  @GuiWidgetElement(
      id = WIDGET_FILTER,
      order = "0600",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::XmlSchemaExportDialog.Filter.Label",
      toolTip = "i18n::XmlSchemaExportDialog.Filter.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::XmlSchemaExportDialog.Group.ExportSettings")
  @HopMetadataProperty
  private String filter;

  @GuiWidgetElement(
      id = WIDGET_STRICT_ORDER,
      order = "0700",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::XmlSchemaExportDialog.StrictOrder.Label",
      toolTip = "i18n::XmlSchemaExportDialog.StrictOrder.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::XmlSchemaExportDialog.Group.ExportSettings")
  @HopMetadataProperty
  private boolean strictOrder;

  public HopXmlSchemaExportOptions toExportOptions() {
    HopXmlSchemaExportOptions options = new HopXmlSchemaExportOptions();
    options.setExportPipeline(exportPipeline);
    options.setExportWorkflow(exportWorkflow);
    options.setExportTransforms(exportTransforms);
    options.setExportActions(exportActions);
    options.setFlexibleElementOrder(!strictOrder);
    options.setPluginFilterPattern(filter);
    return options;
  }
}
