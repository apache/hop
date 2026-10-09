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

package org.apache.hop.ui.hopgui.markdown;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.markdown.MarkdownTableAlignment;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Fields for {@link MarkdownTableDialog}. Not a stored metadata type. */
@GuiPlugin(description = "Markdown table dialog")
@Getter
@Setter
public class MarkdownTableDialogModel {

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "MarkdownTableDialog-Widgets";

  public static final String WIDGET_COLUMNS = "markdown-table-columns";
  public static final String WIDGET_ROWS = "markdown-table-rows";
  public static final String WIDGET_ALIGNMENT = "markdown-table-alignment";

  private static final String GROUP = "i18n::MarkdownTableDialog.Group";

  @GuiWidgetElement(
      id = WIDGET_COLUMNS,
      order = "0100",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::MarkdownTableDialog.Columns.Label",
      toolTip = "i18n::MarkdownTableDialog.Columns.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP)
  @HopMetadataProperty
  private String columns = "2";

  @GuiWidgetElement(
      id = WIDGET_ROWS,
      order = "0200",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::MarkdownTableDialog.Rows.Label",
      toolTip = "i18n::MarkdownTableDialog.Rows.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP)
  @HopMetadataProperty
  private String rows = "3";

  @GuiWidgetElement(
      id = WIDGET_ALIGNMENT,
      order = "0300",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::MarkdownTableDialog.Alignment.Label",
      toolTip = "i18n::MarkdownTableDialog.Alignment.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP)
  @HopMetadataProperty
  private MarkdownTableAlignment alignment = MarkdownTableAlignment.DEFAULT;
}
