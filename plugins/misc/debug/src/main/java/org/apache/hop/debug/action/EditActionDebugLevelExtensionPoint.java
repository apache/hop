package org.apache.hop.debug.action;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.debug.util.DebugLevelUtil;
import org.apache.hop.debug.util.Defaults;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.workflow.extension.HopGuiWorkflowGraphExtension;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionCopy;

@ExtensionPoint(
  id = "EditActionDebugLevelExtensionPoint",
  extensionPointId = "WorkflowGraphMouseUp",
  description = "Edit the custom action debug level with a single click"
)
public class EditActionDebugLevelExtensionPoint implements IExtensionPoint<HopGuiWorkflowGraphExtension> {
  @Override public void callExtensionPoint( ILogChannel log, HopGuiWorkflowGraphExtension ext ) throws HopException {
    try {
      if ( ext.getAreaOwner() == null || ext.getAreaOwner().getOwner() == null ) {
        return;
      }
      if ( !( ext.getAreaOwner().getOwner() instanceof ActionDebugLevel ) ) {
        return;
      }
      ActionDebugLevel debugLevel = (ActionDebugLevel) ext.getAreaOwner().getOwner();
      ActionDebugLevelDialog dialog = new ActionDebugLevelDialog( HopGui.getInstance().getShell(), debugLevel );
      if ( dialog.open() ) {
        WorkflowMeta workflowMeta = ext.getWorkflowGraph().getWorkflowMeta();
        ActionCopy actionCopy = (ActionCopy) ext.getAreaOwner().getParent();

        DebugLevelUtil.storeActionDebugLevel(
          workflowMeta.getAttributesMap().get( Defaults.DEBUG_GROUP ),
          actionCopy.getName(),
          debugLevel
        );
        ext.getWorkflowGraph().redraw();
      }
    } catch(Exception e) {
      new ErrorDialog( HopGui.getInstance().getShell(), "Error", "Error editing action debug level", e );
    }
  }
}
