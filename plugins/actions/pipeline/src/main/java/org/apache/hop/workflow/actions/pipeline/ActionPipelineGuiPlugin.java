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

package org.apache.hop.workflow.actions.pipeline;

import org.apache.hop.core.Const;
import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGuiExtensionPoint;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileOpenedExtension;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.pipeline.context.HopGuiPipelineContext;
import org.apache.hop.ui.hopgui.file.workflow.delegates.HopGuiWorkflowClipboardDelegate;
import org.apache.hop.workflow.action.ActionMeta;

import java.util.List;

/**
 * This is a convenient way to add a pipeline you just designed to a workflow in the form of an
 * action.
 */
@GuiPlugin
public class ActionPipelineGuiPlugin {

  @GuiContextAction(
      id = "pipeline-graph-transform-10300-copy-pipeline-action",
      parentId = HopGuiPipelineContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "Copy as pipeline action",
      tooltip = "Copy this pipeline as an action so you can paste it in a workflow",
      image = "ui/images/copy.svg",
      category = "Basic",
      categoryOrder = "1")
  public void copyAsActionToClipboard(HopGuiPipelineContext context) {

    PipelineMeta pipelineMeta = context.getPipelineMeta();
    HopGuiPipelineGraph pipelineGraph = context.getPipelineGraph();
    IVariables variables = pipelineGraph.getVariables();

    ActionPipeline actionPipeline = new ActionPipeline(pipelineMeta.getName());

    HopGuiFileOpenedExtension ext =
        new HopGuiFileOpenedExtension(null, variables, pipelineMeta.getFilename());

    // See if there are any plugins interested in manipulating the filename...
    //
    try {
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.UI, variables, HopGuiExtensionPoint.HopGuiFileOpenedDialog.id, ext);
    } catch (Exception xe) {
      LogChannel.UI.logError("Error handling extension point 'HopGuiFileOpenDialog'", xe);
    }

    actionPipeline.setFileName(ext.filename);

    // The pipeline run configuration to use: pick the only one or ask
    //
    try {
      IHopMetadataProvider metadataProvider = pipelineGraph.getHopGui().getMetadataProvider();
      IHopMetadataSerializer<PipelineRunConfiguration> serializer =
          metadataProvider.getSerializer(PipelineRunConfiguration.class);
      List<String> configNames = serializer.listObjectNames();
      if (!configNames.isEmpty()) {
        if (configNames.size() == 1) {
          actionPipeline.setRunConfiguration(configNames.get(0));
        } else {
          EnterSelectionDialog dialog =
              new EnterSelectionDialog(
                  pipelineGraph.getShell(),
                  configNames.toArray(new String[0]),
                  "Select run configuration",
                  "Select the pipeline run configuration to use in the action:");
          String configName = dialog.open();
          if (configName != null) {
            actionPipeline.setRunConfiguration(configName);
          }
        }
      }
    } catch (Exception e) {
      new ErrorDialog(
          pipelineGraph.getShell(), "Error", "Error selecting pipeline run configurations", e);
    }

    ActionMeta actionMeta = new ActionMeta(actionPipeline);

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

    pipelineGraph.pipelineClipboardDelegate.toClipboard(xml.toString());
  }
}
