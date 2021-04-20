package org.apache.hop.workflow.actions.pipeline;

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
import org.apache.hop.ui.hopgui.file.workflow.delegates.HopGuiWorkflowClipboardDelegate;
import org.apache.hop.workflow.action.ActionMeta;

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
    HopGui hopGui = pipelineGraph.getHopGui();
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
