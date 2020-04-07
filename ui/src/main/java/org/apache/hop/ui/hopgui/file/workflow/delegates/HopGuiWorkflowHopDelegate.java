package org.apache.hop.ui.hopgui.file.workflow.delegates;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionCopy;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;

public class HopGuiWorkflowHopDelegate {

  private static Class<?> PKG = HopGui.class; // for i18n purposes, needed by Translator!!


  private HopGui hopUi;
  private HopGuiWorkflowGraph jobGraph;
  private PropsUI props;

  public HopGuiWorkflowHopDelegate( HopGui hopGui, HopGuiWorkflowGraph jobGraph ) {
    this.hopUi = hopGui;
    this.jobGraph = jobGraph;
    this.props = PropsUI.getInstance();
  }

  public void newHop( WorkflowMeta workflowMeta, ActionCopy fr, ActionCopy to ) {
    WorkflowHopMeta hi = new WorkflowHopMeta( fr, to );
    newHop( workflowMeta, hi );
  }

  public void newHop( WorkflowMeta workflowMeta, WorkflowHopMeta hopMeta ) {
    if ( checkIfHopAlreadyExists( workflowMeta, hopMeta ) ) {
      workflowMeta.addWorkflowHop( hopMeta );
      int idx = workflowMeta.indexOfWorkflowHop( hopMeta );

      if ( !performNewWorkflowHopChecks( workflowMeta, hopMeta ) ) {
        // Some error occurred: loops, existing hop, etc.
        // Remove it again...
        //
        workflowMeta.removeWorkflowHop( idx );
      } else {
        // TODO: Create a new Undo/Redo system
        hopUi.undoDelegate.addUndoNew( workflowMeta, new WorkflowHopMeta[] { hopMeta }, new int[] { workflowMeta.indexOfWorkflowHop( hopMeta ) } );
      }

      jobGraph.updateGui();
    }
  }

  /**
   * @param workflowMeta workflow metadata
   * @param newHop  hop to be checked
   * @return true when the hop was added, false if there was an error
   */
  public boolean checkIfHopAlreadyExists( WorkflowMeta workflowMeta, WorkflowHopMeta newHop ) {
    boolean ok = true;
    if ( workflowMeta.findWorkflowHop( newHop.getFromEntry(), newHop.getToEntry() ) != null ) {
      MessageBox mb = new MessageBox( hopUi.getShell(), SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "HopGui.Dialog.HopExists.Message" ) ); // "This hop already exists!"
      mb.setText( BaseMessages.getString( PKG, "HopGui.Dialog.HopExists.Title" ) ); // Error!
      mb.open();
      ok = false;
    }

    return ok;
  }

  /**
   * @param workflowMeta workflow meta
   * @param newHop  hop to be checked
   * @return true when the hop was added, false if there was an error
   */
  public boolean performNewWorkflowHopChecks( WorkflowMeta workflowMeta, WorkflowHopMeta newHop ) {
    boolean ok = true;

    if ( workflowMeta.hasLoop( newHop.getToEntry() ) ) {
      MessageBox mb = new MessageBox( hopUi.getShell(), SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "JobGraph.Dialog.HopCausesLoop.Message" ) );
      mb.setText( BaseMessages.getString( PKG, "JobGraph.Dialog.HopCausesLoop.Title" ) );
      mb.open();
      ok = false;
    }

    return ok;
  }

  public void delHop( WorkflowMeta workflowMeta, WorkflowHopMeta hopMeta ) {
    int index = workflowMeta.indexOfWorkflowHop( hopMeta );

    // TODO: Create new Undo/Redo system
    hopUi.undoDelegate.addUndoDelete( workflowMeta, new Object[] { hopMeta.clone() }, new int[] { index } );
    workflowMeta.removeWorkflowHop( index );

    jobGraph.redraw();
  }
}
