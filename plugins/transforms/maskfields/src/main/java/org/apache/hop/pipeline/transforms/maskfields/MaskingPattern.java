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

package org.apache.hop.pipeline.transforms.maskfields;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/** Reusable rule for replacing the values of a field. */
@Getter
@Setter
@GuiPlugin
@SuppressWarnings("java:S2160")
@HopMetadata(
    key = "masking-pattern",
    name = "i18n::MaskingPattern.Name",
    description = "i18n::MaskingPattern.Description",
    image = "masking-pattern.svg",
    category = HopMetadataCategory.DATA_DEFINITION,
    documentationUrl = "/metadata-types/masking-pattern.html",
    hopMetadataPropertyType = HopMetadataPropertyType.MASKING_PATTERN)
public class MaskingPattern extends HopMetadataBase implements IHopMetadata {

  public static final String GUI_WIDGETS_PARENT_ID = "MaskingPattern.Widgets";
  public static final String WIDGET_DESCRIPTION = "description";
  public static final String WIDGET_CLASSIFICATION = "piiClassification";
  public static final String WIDGET_VALUE_SOURCE = "valueSource";
  public static final String WIDGET_TOKEN = "token";
  public static final String WIDGET_PREFIX = "prefix";
  public static final String WIDGET_SUFFIX = "suffix";
  public static final String WIDGET_SEQUENCE_START = "sequenceStart";
  public static final String WIDGET_LIST_FIELD = "listField";
  public static final String WIDGET_STORAGE = "storage";
  public static final String WIDGET_CONNECTION = "connection";
  public static final String WIDGET_SCHEMA = "schemaName";
  public static final String WIDGET_TABLE = "tableName";

  public static final String GROUP_DESCRIPTION = "i18n::MaskingPattern.Group.Description";
  public static final String GROUP_RULE = "i18n::MaskingPattern.Group.Rule";
  public static final String GROUP_STORAGE = "i18n::MaskingPattern.Group.Storage";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_DESCRIPTION,
      order = "0100",
      type = GuiElementType.MULTI_LINE_TEXT,
      multiLineTextHeight = 3,
      parentId = GUI_WIDGETS_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_DESCRIPTION,
      groupOrder = "10",
      label = "i18n::MaskingPattern.DescriptionField.Label",
      toolTip = "i18n::MaskingPattern.DescriptionField.Tooltip")
  private String description = "";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_CLASSIFICATION,
      order = "0200",
      type = GuiElementType.COMBO,
      comboValuesMethod = "classificationSuggestions",
      variables = false,
      parentId = GUI_WIDGETS_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_DESCRIPTION,
      groupOrder = "10",
      label = "i18n::MaskingPattern.Classification.Label",
      toolTip = "i18n::MaskingPattern.Classification.Tooltip")
  private String piiClassification = "";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_VALUE_SOURCE,
      order = "0300",
      type = GuiElementType.COMBO,
      parentId = GUI_WIDGETS_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_RULE,
      groupOrder = "20",
      label = "i18n::MaskingPattern.ValueSource.Label",
      toolTip = "i18n::MaskingPattern.ValueSource.Tooltip")
  private MaskingValueSource valueSource = MaskingValueSource.SYNTHETIC;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_TOKEN,
      order = "0400",
      type = GuiElementType.COMBO,
      parentId = GUI_WIDGETS_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_RULE,
      groupOrder = "20",
      label = "i18n::MaskingPattern.Token.Label",
      toolTip = "i18n::MaskingPattern.Token.Tooltip")
  private MaskingToken token = MaskingToken.SEQUENCE;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_PREFIX,
      order = "0500",
      type = GuiElementType.TEXT,
      parentId = GUI_WIDGETS_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_RULE,
      groupOrder = "20",
      label = "i18n::MaskingPattern.Prefix.Label",
      toolTip = "i18n::MaskingPattern.Prefix.Tooltip")
  private String prefix = "";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_SUFFIX,
      order = "0600",
      type = GuiElementType.TEXT,
      parentId = GUI_WIDGETS_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_RULE,
      groupOrder = "20",
      label = "i18n::MaskingPattern.Suffix.Label",
      toolTip = "i18n::MaskingPattern.Suffix.Tooltip")
  private String suffix = "";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_SEQUENCE_START,
      order = "0700",
      type = GuiElementType.TEXT,
      parentId = GUI_WIDGETS_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_RULE,
      groupOrder = "20",
      label = "i18n::MaskingPattern.SequenceStart.Label",
      toolTip = "i18n::MaskingPattern.SequenceStart.Tooltip")
  private String sequenceStart = "1";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_LIST_FIELD,
      order = "0800",
      type = GuiElementType.TEXT,
      parentId = GUI_WIDGETS_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_RULE,
      groupOrder = "20",
      label = "i18n::MaskingPattern.ListField.Label",
      toolTip = "i18n::MaskingPattern.ListField.Tooltip")
  private String listField = "";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_STORAGE,
      order = "0900",
      type = GuiElementType.COMBO,
      parentId = GUI_WIDGETS_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_STORAGE,
      groupOrder = "30",
      label = "i18n::MaskingPattern.Storage.Label",
      toolTip = "i18n::MaskingPattern.Storage.Tooltip")
  private MaskingStorage storage = MaskingStorage.NONE;

  @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  @GuiWidgetElement(
      id = WIDGET_CONNECTION,
      order = "1000",
      type = GuiElementType.METADATA,
      metadata = org.apache.hop.core.database.DatabaseMeta.class,
      parentId = GUI_WIDGETS_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_STORAGE,
      groupOrder = "30",
      label = "i18n::MaskingPattern.Connection.Label",
      toolTip = "i18n::MaskingPattern.Connection.Tooltip")
  private String connection = "";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_SCHEMA,
      order = "1100",
      type = GuiElementType.TEXT,
      parentId = GUI_WIDGETS_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_STORAGE,
      groupOrder = "30",
      label = "i18n::MaskingPattern.Schema.Label",
      toolTip = "i18n::MaskingPattern.Schema.Tooltip")
  private String schemaName = "";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_TABLE,
      order = "1200",
      type = GuiElementType.TEXT,
      parentId = GUI_WIDGETS_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_STORAGE,
      groupOrder = "30",
      label = "i18n::MaskingPattern.Table.Label",
      toolTip = "i18n::MaskingPattern.Table.Tooltip")
  private String tableName = "";

  public MaskingPattern() {}

  /** Suggestions for the classification combo. A project can type its own term. */
  public List<String> classificationSuggestions(
      ILogChannel log, IHopMetadataProvider metadataProvider) {
    return List.of("Direct identifier", "Quasi-identifier", "Sensitive", "Special category");
  }

  public boolean remembers() {
    return storage == MaskingStorage.MEMORY || storage == MaskingStorage.DATABASE;
  }
}
