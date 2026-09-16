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
package org.apache.hop.pipeline.transforms.plugincatalog;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Plugin Catalog transform metadata.
 *
 * <p>Emits one row per plugin (or per plugin property) by reflecting over the live Hop {@link
 * org.apache.hop.core.plugins.PluginRegistry}. Labels are resolved to human-readable text; output
 * is never stale against the running Hop version or any installed third-party plugins.
 */
@Getter
@Setter
@Transform(
    id = "PluginCatalog",
    image = "plugincatalog.svg",
    name = "i18n::PluginCatalog.Name",
    description = "i18n::PluginCatalog.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    documentationUrl = "/pipeline/transforms/plugincatalog.html",
    keywords = "i18n::PluginCatalog.Keywords")
@GuiPlugin
public class PluginCatalogMeta extends BaseTransformMeta<PluginCatalog, PluginCatalogData> {
  private static final Class<?> PKG = PluginCatalogMeta.class;
  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "PLUGIN_CATALOG_DIALOG_OPTIONS";
  private static final String GROUP_OPTIONS = "Options";

  // Output field names shared by both detail levels.
  public static final String FIELD_PLUGIN_ID = "plugin_id";
  public static final String FIELD_PLUGIN_TYPE = "plugin_type";
  public static final String FIELD_NAME = "name";
  public static final String FIELD_DESCRIPTION = "description";
  public static final String FIELD_CATEGORY = "category";
  public static final String FIELD_KEYWORDS = "keywords";
  public static final String FIELD_CLASS_NAME = "class_name";
  public static final String FIELD_ENGLISH_ALIASES = "english_aliases";
  public static final String FIELD_LOCALE = "locale";

  /** Number of plugin-level columns emitted before the detail-level columns. */
  public static final int BASE_FIELD_COUNT = 9;

  // PER_PLUGIN extra.
  public static final String FIELD_METADATA_FIELDS = "metadata_fields";
  // PER_PROPERTY extras.
  public static final String FIELD_PROPERTY_FIELD = "property_field";
  public static final String FIELD_PROPERTY_XML_KEY = "property_xml_key";
  public static final String FIELD_PROPERTY_JAVA_TYPE = "property_java_type";
  public static final String FIELD_PROPERTY_PASSWORD = "property_password";
  public static final String FIELD_PROPERTY_GROUP = "property_group";
  public static final String FIELD_PROPERTY_GROUP_KEY = "property_group_key";

  @GuiWidgetElement(
      order = "0100",
      type = GuiElementType.CHECKBOX,
      label = "i18n::PluginCatalog.includeTransforms.Label",
      toolTip = "i18n::PluginCatalog.includeTransforms.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_OPTIONS)
  @HopMetadataProperty(key = "includeTransforms", injectionKey = "INCLUDE_TRANSFORMS")
  private boolean includeTransforms = true;

  @GuiWidgetElement(
      order = "0200",
      type = GuiElementType.CHECKBOX,
      label = "i18n::PluginCatalog.includeActions.Label",
      toolTip = "i18n::PluginCatalog.includeActions.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_OPTIONS)
  @HopMetadataProperty(key = "includeActions", injectionKey = "INCLUDE_ACTIONS")
  private boolean includeActions = true;

  @GuiWidgetElement(
      order = "0300",
      type = GuiElementType.CHECKBOX,
      label = "i18n::PluginCatalog.includeMetadataTypes.Label",
      toolTip = "i18n::PluginCatalog.includeMetadataTypes.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_OPTIONS)
  @HopMetadataProperty(key = "includeMetadataTypes", injectionKey = "INCLUDE_METADATA_TYPES")
  private boolean includeMetadataTypes = true;

  @GuiWidgetElement(
      order = "0400",
      type = GuiElementType.COMBO,
      label = "i18n::PluginCatalog.detailLevel.Label",
      toolTip = "i18n::PluginCatalog.detailLevel.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_OPTIONS)
  @HopMetadataProperty(key = "detailLevel", injectionKey = "DETAIL_LEVEL")
  private DetailLevel detailLevel = DetailLevel.PER_PLUGIN;

  public PluginCatalogMeta() {
    super();
  }

  @Override
  public void setDefault() {
    includeTransforms = true;
    includeActions = true;
    includeMetadataTypes = true;
    detailLevel = DetailLevel.PER_PLUGIN;
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
      row.clear();
      addString(row, FIELD_PLUGIN_ID, origin);
      addString(row, FIELD_PLUGIN_TYPE, origin);
      addString(row, FIELD_NAME, origin);
      addString(row, FIELD_DESCRIPTION, origin);
      addString(row, FIELD_CATEGORY, origin);
      addString(row, FIELD_KEYWORDS, origin);
      addString(row, FIELD_CLASS_NAME, origin);
      addString(row, FIELD_ENGLISH_ALIASES, origin);
      addString(row, FIELD_LOCALE, origin);

      if (detailLevel == DetailLevel.PER_PROPERTY) {
        addString(row, FIELD_PROPERTY_FIELD, origin);
        addString(row, FIELD_PROPERTY_XML_KEY, origin);
        addString(row, FIELD_PROPERTY_JAVA_TYPE, origin);
        addBoolean(row, FIELD_PROPERTY_PASSWORD, origin);
        addString(row, FIELD_PROPERTY_GROUP, origin);
        addString(row, FIELD_PROPERTY_GROUP_KEY, origin);
      } else {
        addString(row, FIELD_METADATA_FIELDS, origin);
      }
    } catch (Exception e) {
      throw new HopTransformException("Error creating Plugin Catalog output fields", e);
    }
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
    if (prev != null && !prev.isEmpty()) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "PluginCatalogMeta.CheckResult.NoInputStreamsError"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "PluginCatalogMeta.CheckResult.NoInputStreamOk"),
              transformMeta));
    }
  }

  @Override
  public ITransformIOMeta getTransformIOMeta() {
    return new TransformIOMeta(false, true, false, false, false, false);
  }

  @Override
  public boolean consumesMainInput() {
    return false;
  }

  @Override
  public boolean canStartWithoutInput() {
    return true;
  }

  private static void addString(IRowMeta row, String name, String origin) {
    ValueMetaString field = new ValueMetaString(name);
    field.setOrigin(origin);
    row.addValueMeta(field);
  }

  private static void addBoolean(IRowMeta row, String name, String origin) {
    ValueMetaBoolean field = new ValueMetaBoolean(name);
    field.setOrigin(origin);
    row.addValueMeta(field);
  }
}
