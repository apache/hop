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

package org.apache.hop.workflow.actions.workflow;

import org.apache.hop.core.Const;
import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiExtensionPoint;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileOpenedExtension;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.pipeline.context.HopGuiPipelineContext;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.file.workflow.context.HopGuiWorkflowContext;
import org.apache.hop.ui.hopgui.file.workflow.delegates.HopGuiWorkflowClipboardDelegate;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

/**
 * This is a convenient way to add a workflow you just designed to another workflow in the form of
 * an action.
 */
@GuiPlugin
public class ActionWorkflowGuiPlugin {

  @GuiContextAction(
      id = "workflow-graph-workflow-copy-action",
      parentId = HopGuiWorkflowContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "Copy as workflow action",
      tooltip = "Copy this workflow as an action so you can paste it in another workflow",
      image = "ui/images/copy.svg",
      category = "Basic",
      categoryOrder = "1")
  public void copyAsActionToClipboard(HopGuiWorkflowContext context) {

    WorkflowMeta workflowMeta = context.getWorkflowMeta();
    HopGuiWorkflowGraph workflowGraph = context.getWorkflowGraph();
    HopGui hopGui = workflowGraph.getHopGui();
    IVariables variables = workflowGraph.getVariables();

    ActionWorkflow actionWorkflow = new ActionWorkflow(workflowMeta.getName());

    HopGuiFileOpenedExtension ext =
        new HopGuiFileOpenedExtension(null, variables, workflowMeta.getFilename());

    // See if there are any plugins interested in manipulating the filename...
    //
    try {
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.UI, variables, HopGuiExtensionPoint.HopGuiFileOpenedDialog.id, ext);
    } catch (Exception xe) {
      LogChannel.UI.logError("Error handling extension point 'HopGuiFileOpenDialog'", xe);
    }

    actionWorkflow.setFileName(ext.filename);
    ActionMeta actionMeta = new ActionMeta(actionWorkflow);

    StringBuilder xml = new StringBuilder(5000).append(XmlHandler.getXmlHeader());
    xml.append(XmlHandler.openTag(HopGuiWorkflowClipboardDelegate.XML_TAG_WORKFLOW_ACTIONS))
        .append(Const.CR);
    xml.append(XmlHandler.openTag(HopGuiWorkflowClipboardDelegate.XML_TAG_ACTIONS))
        .append(Const.CR);
    xml.append(actionMeta.getXml());
    xml.append(XmlHandler.closeTag(HopGuiWorkflowClipboardDelegate.XML_TAG_ACTIONS))
        .append(Const.CR);
    xml.append(XmlHandler.closeTag(HopGuiWorkflowClipboardDelegate.XML_TAG_WORKFLOW_ACTIONS))
        .append(Const.CR);

    workflowGraph.workflowClipboardDelegate.toClipboard(xml.toString());
  }
}
