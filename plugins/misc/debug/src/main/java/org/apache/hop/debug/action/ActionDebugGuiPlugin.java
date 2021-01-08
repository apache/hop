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

package org.apache.hop.debug.action;

import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.debug.util.DebugLevelUtil;
import org.apache.hop.debug.util.Defaults;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.workflow.context.HopGuiWorkflowActionContext;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

import java.util.HashMap;
import java.util.Map;

@GuiPlugin
public class ActionDebugGuiPlugin {

  @GuiContextAction(
    id = "workflow-graph-action-11001-clear-logging",
    parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
    type = GuiActionType.Delete,
    name = "Clear Custom Logging",
    tooltip = "Clear custom log settings ",
    image = "ui/images/debug.svg",
    category = "Logging",
    categoryOrder = "7"
  )
  public void clearCustomActionLogging( HopGuiWorkflowActionContext context ) {
    WorkflowMeta workflowMeta = context.getWorkflowMeta();
    ActionMeta action = context.getActionMeta();

    Map<String, Map<String, String>> attributesMap = workflowMeta.getAttributesMap();
    Map<String, String> debugGroupAttributesMap = attributesMap.get( Defaults.DEBUG_GROUP );

    DebugLevelUtil.clearDebugLevel( debugGroupAttributesMap, action.toString() );
    workflowMeta.setChanged();
  }

  @GuiContextAction(
    id = "workflow-graph-action-11000-clear-logging",
    parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Edit Custom Logging",
    tooltip = "Edit the custom log settings for this action",
    image = "ui/images/debug.svg",
    category = "Logging",
    categoryOrder = "7"
  )
  public void applyCustomActionLogging( HopGuiWorkflowActionContext context ) {
    HopGui hopGui = HopGui.getInstance();
    try {
      WorkflowMeta workflowMeta = context.getWorkflowMeta();
      ActionMeta action = context.getActionMeta();

      Map<String, Map<String, String>> attributesMap = workflowMeta.getAttributesMap();
      Map<String, String> debugGroupAttributesMap = attributesMap.get( Defaults.DEBUG_GROUP );

      if ( debugGroupAttributesMap == null ) {
        debugGroupAttributesMap = new HashMap<>();
        attributesMap.put( Defaults.DEBUG_GROUP, debugGroupAttributesMap );
      }

      ActionDebugLevel debugLevel = DebugLevelUtil.getActionDebugLevel( debugGroupAttributesMap, action.toString() );
      if ( debugLevel == null ) {
        debugLevel = new ActionDebugLevel();
      }

      ActionDebugLevelDialog dialog = new ActionDebugLevelDialog( hopGui.getShell(), debugLevel );
      if ( dialog.open() ) {
        DebugLevelUtil.storeActionDebugLevel( debugGroupAttributesMap, action.toString(), debugLevel );
      }

      workflowMeta.setChanged();
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error changing action log settings", e );
    }

  }


}

