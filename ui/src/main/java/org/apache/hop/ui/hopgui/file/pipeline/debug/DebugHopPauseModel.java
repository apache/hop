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

package org.apache.hop.ui.hopgui.file.pipeline.debug;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.pipeline.debug.DebugPauseScope;

/** Scope combo for {@link DebugHopPauseDialog}. The condition editor is an extra group. */
@GuiPlugin(description = "Debug hop pause")
@Getter
@Setter
public class DebugHopPauseModel {

  public static final String PARENT_ID = "DebugHopPauseDialog.Parent";

  public static final String WIDGET_SCOPE = "debug-hop-pause-scope";

  @GuiWidgetElement(
      id = WIDGET_SCOPE,
      order = "10",
      parentId = PARENT_ID,
      type = GuiElementType.COMBO,
      label = "i18n::DebugHopPauseDialog.Scope.Label",
      toolTip = "i18n::DebugHopPauseDialog.Scope.Tooltip",
      group = "i18n::DebugHopPauseDialog.Group.Pause",
      groupOrder = "10",
      groupType = GuiWidgetGroupType.BOXES)
  private DebugPauseScope scope = DebugPauseScope.TRANSFORM;
}
