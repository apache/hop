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

package org.apache.hop.annotation;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Plain-text callout anchored to one transform or one workflow action. */
@GuiPlugin
@Getter
@Setter
public class Annotation {
  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "AnnotationDialog";

  @GuiWidgetElement(
      id = "annotation-text",
      order = "0100",
      type = GuiElementType.MULTI_LINE_TEXT,
      multiLineTextHeight = 6,
      variables = false,
      label = "i18n::Annotation.Text.Label",
      toolTip = "i18n::Annotation.Text.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::Annotation.Group")
  @HopMetadataProperty
  private String text;

  @GuiWidgetElement(
      id = "annotation-placement",
      order = "0200",
      type = GuiElementType.COMBO,
      label = "i18n::Annotation.Placement.Label",
      toolTip = "i18n::Annotation.Placement.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::Annotation.Group")
  @HopMetadataProperty
  private AnnotationPlacement placement = AnnotationPlacement.TOP;

  @GuiWidgetElement(
      id = "annotation-dashed-line",
      order = "0300",
      type = GuiElementType.CHECKBOX,
      label = "i18n::Annotation.DashedLine.Label",
      toolTip = "i18n::Annotation.DashedLine.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::Annotation.Group")
  @HopMetadataProperty
  private boolean dashedLine = true;
}
