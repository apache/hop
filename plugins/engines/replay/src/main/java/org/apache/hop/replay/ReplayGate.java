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

package org.apache.hop.replay;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.metadata.api.HopMetadataProperty;

@GuiPlugin(description = "Replay Gate Configuration")
@Getter
@Setter
public class ReplayGate {

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "ReplayGateDialog-Parent";

  public static final String WIDGET_ENABLED = "0100-enabled";
  public static final String WIDGET_SPOOL_DIR = "0200-spool-dir";
  public static final String WIDGET_DESCRIPTION = "0300-description";

  @GuiWidgetElement(
      id = WIDGET_ENABLED,
      order = "0100",
      type = GuiElementType.CHECKBOX,
      label = "i18n::ReplayGate.Enabled.Label",
      toolTip = "i18n::ReplayGate.Enabled.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::ReplayGate.Group")
  @HopMetadataProperty
  private boolean enabled;

  @GuiWidgetElement(
      id = WIDGET_SPOOL_DIR,
      order = "0200",
      type = GuiElementType.FOLDER,
      label = "i18n::ReplayGate.SpoolDirectory.Label",
      toolTip = "i18n::ReplayGate.SpoolDirectory.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::ReplayGate.Group")
  @HopMetadataProperty
  private String spoolDirectory;

  @GuiWidgetElement(
      id = WIDGET_DESCRIPTION,
      order = "0300",
      type = GuiElementType.TEXT,
      label = "i18n::ReplayGate.Description.Label",
      toolTip = "i18n::ReplayGate.Description.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::ReplayGate.Group")
  @HopMetadataProperty
  private String description;

  public ReplayGate() {
    this.enabled = true;
    this.spoolDirectory = ReplayDefaults.DEFAULT_SPOOL_DIR;
    this.description = "";
  }
}
