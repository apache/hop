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
import org.apache.hop.workflow.action.ActionCopy;

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
    image = "ui/images/debug.svg"
  )
  public void clearCustomActionLogging( HopGuiWorkflowActionContext context ) {
    WorkflowMeta workflowMeta = context.getWorkflowMeta();
    ActionCopy action = context.getActionCopy();

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
    image = "ui/images/debug.svg"
  )
  public void applyCustomActionLogging( HopGuiWorkflowActionContext context ) {
    HopGui hopGui = HopGui.getInstance();
    try {
      WorkflowMeta workflowMeta = context.getWorkflowMeta();
      ActionCopy action = context.getActionCopy();

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

