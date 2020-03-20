package org.apache.hop.ui.hopgui.file.job.delegates;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobHopMeta;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryCopy;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.job.HopGuiJobGraph;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;

public class HopGuiJobHopDelegate {

  private static Class<?> PKG = HopGui.class; // for i18n purposes, needed by Translator2!!


  private HopGui hopUi;
  private HopGuiJobGraph jobGraph;
  private PropsUI props;

  public HopGuiJobHopDelegate( HopGui hopGui, HopGuiJobGraph jobGraph ) {
    this.hopUi = hopGui;
    this.jobGraph = jobGraph;
    this.props = PropsUI.getInstance();
  }

  public void newHop( JobMeta jobMeta, JobEntryCopy fr, JobEntryCopy to ) {
    JobHopMeta hi = new JobHopMeta( fr, to );
    newHop( jobMeta, hi );
  }

  public void newHop( JobMeta jobMeta, JobHopMeta hopMeta ) {
    if ( checkIfHopAlreadyExists( jobMeta, hopMeta ) ) {
      jobMeta.addJobHop( hopMeta );
      int idx = jobMeta.indexOfJobHop( hopMeta );

      if ( !performNewJobHopChecks( jobMeta, hopMeta ) ) {
        // Some error occurred: loops, existing hop, etc.
        // Remove it again...
        //
        jobMeta.removeJobHop( idx );
      } else {
        // TODO: Create a new Undo/Redo system
        hopUi.undoDelegate.addUndoNew( jobMeta, new JobHopMeta[] { hopMeta }, new int[] { jobMeta.indexOfJobHop( hopMeta ) } );
      }

      jobGraph.updateGui();
    }
  }

  /**
   * @param jobMeta job metadata
   * @param newHop  hop to be checked
   * @return true when the hop was added, false if there was an error
   */
  public boolean checkIfHopAlreadyExists( JobMeta jobMeta, JobHopMeta newHop ) {
    boolean ok = true;
    if ( jobMeta.findJobHop( newHop.getFromEntry(), newHop.getToEntry() ) != null ) {
      MessageBox mb = new MessageBox( hopUi.getShell(), SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "Spoon.Dialog.HopExists.Message" ) ); // "This hop already exists!"
      mb.setText( BaseMessages.getString( PKG, "Spoon.Dialog.HopExists.Title" ) ); // Error!
      mb.open();
      ok = false;
    }

    return ok;
  }

  /**
   * @param jobMeta job meta
   * @param newHop  hop to be checked
   * @return true when the hop was added, false if there was an error
   */
  public boolean performNewJobHopChecks( JobMeta jobMeta, JobHopMeta newHop ) {
    boolean ok = true;

    if ( jobMeta.hasLoop( newHop.getToEntry() ) ) {
      MessageBox mb = new MessageBox( hopUi.getShell(), SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "JobGraph.Dialog.HopCausesLoop.Message" ) );
      mb.setText( BaseMessages.getString( PKG, "JobGraph.Dialog.HopCausesLoop.Title" ) );
      mb.open();
      ok = false;
    }

    return ok;
  }

  public void delHop( JobMeta jobMeta, JobHopMeta hopMeta ) {
    int index = jobMeta.indexOfJobHop( hopMeta );

    // TODO: Create new Undo/Redo system
    hopUi.undoDelegate.addUndoDelete( jobMeta, new Object[] { hopMeta.clone() }, new int[] { index } );
    jobMeta.removeJobHop( index );

    jobGraph.redraw();
  }
}
