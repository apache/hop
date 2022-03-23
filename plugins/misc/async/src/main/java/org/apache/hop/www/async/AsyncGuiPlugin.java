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

package org.apache.hop.www.async;

import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.action.GuiContextActionFilter;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.workflow.context.HopGuiWorkflowActionContext;
import org.apache.hop.workflow.action.ActionMeta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@GuiPlugin
public class AsyncGuiPlugin {

  public static final String ACTION_ID_WORKFLOW_GRAPH_ENABLE_ASYNC_LOGGING =
      "workflow-graph-action-30000-enable-async-logging";
  public static final String ACTION_ID_WORKFLOW_GRAPH_DISABLE_ASYNC_LOGGING =
      "workflow-graph-action-30100-disable-async-logging";

  private static AsyncGuiPlugin instance = null;

  public AsyncGuiPlugin() {}

  /**
   * Gets instance
   *
   * @return value of instance
   */
  public static AsyncGuiPlugin getInstance() {
    if (instance == null) {
      instance = new AsyncGuiPlugin();
    }
    return instance;
  }

  @GuiContextAction(
      id = ACTION_ID_WORKFLOW_GRAPH_ENABLE_ASYNC_LOGGING,
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Custom,
      name = "i18n::AsyncGuiPlugin.EnableAsyncLogging.Name",
      tooltip = "i18n::AsyncGuiPlugin.EnableAsyncLogging.ToolTip",
      image = "ui/images/log.svg",
      category = "i18n::AsyncGuiPlugin.EnableAsyncLogging.Category",
      categoryOrder = "9")
  public void enableAsyncLogging(HopGuiWorkflowActionContext context) {
    HopGui hopGui = context.getWorkflowGraph().getHopGui();

    try {
      ActionMeta actionMeta = context.getActionMeta();
      if (!actionMeta.isPipeline()) {
        return;
      }

      Map<String, String> asyncMap =
          actionMeta
              .getAttributesMap()
              .computeIfAbsent(Defaults.ASYNC_STATUS_GROUP, k -> new HashMap<>());

      // Ask for the name of the async service...
      //
      IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();
      IHopMetadataSerializer<AsyncWebService> serializer =
          metadataProvider.getSerializer(AsyncWebService.class);
      List<String> serviceNames = serializer.listObjectNames();
      EnterSelectionDialog dialog =
          new EnterSelectionDialog(
              hopGui.getShell(),
              serviceNames.toArray(new String[0]),
              "Select async service",
              "Select the asynchronous service to report to");
      String serviceName = dialog.open();
      if (serviceName == null) {
        return;
      }

      // Put the name of the async service in the map
      //
      asyncMap.put(Defaults.ASYNC_ACTION_PIPELINE_SERVICE_NAME, serviceName);

      // Flag the change
      actionMeta.setChanged();

    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          "Error",
          "Error enabling asynchronous status reporting of a pipeline",
          e);
    }
    // Refresh the graph
    context.getWorkflowGraph().redraw();
  }

  @GuiContextAction(
      id = ACTION_ID_WORKFLOW_GRAPH_DISABLE_ASYNC_LOGGING,
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Custom,
      name = "i18n::AsyncGuiPlugin.DisableAsyncLogging.Name",
      tooltip = "i18n::AsyncGuiPlugin.DisableAsyncLogging.ToolTip",
      image = "ui/images/log.svg",
      category = "i18n::AsyncGuiPlugin.DisableAsyncLogging.Category",
      categoryOrder = "9")
  public void disableAsyncLogging(HopGuiWorkflowActionContext context) {
    ActionMeta actionMeta = context.getActionMeta();
    if (!actionMeta.isPipeline()) {
      return;
    }

    Map<String, String> asyncMap = actionMeta.getAttributesMap().get(Defaults.ASYNC_STATUS_GROUP);
    if (asyncMap == null) {
      // Nothing to disable
      return;
    }
    asyncMap.remove(Defaults.ASYNC_ACTION_PIPELINE_SERVICE_NAME);

    // Flag the change
    actionMeta.setChanged();

    // Refresh the graph
    context.getWorkflowGraph().redraw();
  }

  /**
   * We're filtering out the actions which are not a Pipeline action and which are already
   * enabled/disabled.
   *
   * @param contextActionId
   * @param context
   * @return True if the action should be shown and false otherwise.
   */
  @GuiContextActionFilter(parentId = HopGuiWorkflowActionContext.CONTEXT_ID)
  public boolean filterAsyncActions(String contextActionId, HopGuiWorkflowActionContext context) {

    ActionMeta actionMeta = context.getActionMeta();
    boolean isPipeline = actionMeta.isPipeline();

    Map<String, String> asyncMap = actionMeta.getAttributesMap().get(Defaults.ASYNC_STATUS_GROUP);
    boolean enabled = false;
    if (asyncMap != null) {
      enabled = asyncMap.get(Defaults.ASYNC_ACTION_PIPELINE_SERVICE_NAME) != null;
    }

    // Enable / disable
    //
    if (contextActionId.equals(ACTION_ID_WORKFLOW_GRAPH_ENABLE_ASYNC_LOGGING)) {
      return isPipeline && !enabled;
    }
    if (contextActionId.equals(ACTION_ID_WORKFLOW_GRAPH_DISABLE_ASYNC_LOGGING)) {
      return isPipeline && enabled;
    }
    return true;
  }
}
