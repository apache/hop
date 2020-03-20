package org.apache.hop.ui.hopgui.file.trans.delegates;

import org.apache.hop.core.exception.HopRowException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.TransHopMeta;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.RowDistributionInterface;
import org.apache.hop.trans.step.RowDistributionPluginType;
import org.apache.hop.trans.step.StepErrorMeta;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.trans.HopGuiTransGraph;
import org.apache.hop.ui.trans.dialog.TransHopDialog;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.dialogs.MessageDialogWithToggle;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;

import java.util.ArrayList;
import java.util.List;

public class HopGuiTransHopDelegate {

  // TODO: move i18n package to HopGui
  private static Class<?> PKG = HopGui.class; // for i18n purposes, needed by Translator2!!

  public static final int MESSAGE_DIALOG_WITH_TOGGLE_YES_BUTTON_ID = 256;

  public static final int MESSAGE_DIALOG_WITH_TOGGLE_NO_BUTTON_ID = 257;

  public static final int MESSAGE_DIALOG_WITH_TOGGLE_CUSTOM_DISTRIBUTION_BUTTON_ID = 258;


  private HopGui hopUi;
  private HopGuiTransGraph transGraph;
  private PropsUI props;

  public HopGuiTransHopDelegate( HopGui hopGui, HopGuiTransGraph transGraph ) {
    this.hopUi = hopGui;
    this.transGraph = transGraph;
    this.props = PropsUI.getInstance();
  }

  public void newHop( TransMeta transMeta, StepMeta fr, StepMeta to ) {
    TransHopMeta hi = new TransHopMeta( fr, to );

    TransHopDialog hd = new TransHopDialog( hopUi.getShell(), SWT.NONE, hi, transMeta );
    if ( hd.open() != null ) {
      newHop( transMeta, hi );
    }
  }

  public void newHop( TransMeta transMeta, TransHopMeta transHopMeta ) {
    if ( checkIfHopAlreadyExists( transMeta, transHopMeta ) ) {
      transMeta.addTransHop( transHopMeta );
      int idx = transMeta.indexOfTransHop( transHopMeta );

      if ( !performNewTransHopChecks( transMeta, transHopMeta ) ) {
        // Some error occurred: loops, existing hop, etc.
        // Remove it again...
        //
        transMeta.removeTransHop( idx );
      } else {
        // TODO: Create a new Undo/Redo system
        // addUndoNew( transMeta, new TransHopMeta[] { transHopMeta }, new int[] { transMeta.indexOfTransHop( transHopMeta ) } );
      }

      transGraph.redraw();
    }
  }

  /**
   * @param transMeta transformation's meta
   * @param newHop    hop to be checked
   * @return true when the hop was added, false if there was an error
   */
  public boolean checkIfHopAlreadyExists( TransMeta transMeta, TransHopMeta newHop ) {
    boolean ok = true;
    if ( transMeta.findTransHop( newHop.getFromStep(), newHop.getToStep() ) != null ) {
      MessageBox mb = new MessageBox( hopUi.getShell(), SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "HopGui.Dialog.HopExists.Message" ) ); // "This hop already exists!"
      mb.setText( BaseMessages.getString( PKG, "HopGui.Dialog.HopExists.Title" ) ); // Error!
      mb.open();
      ok = false;
    }

    return ok;
  }

  /**
   * @param transMeta transformation's meta
   * @param newHop    hop to be checked
   * @return true when the hop was added, false if there was an error
   */
  public boolean performNewTransHopChecks( TransMeta transMeta, TransHopMeta newHop ) {
    boolean ok = true;

    if ( transMeta.hasLoop( newHop.getToStep() ) ) {
      MessageBox mb = new MessageBox( hopUi.getShell(), SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "TransGraph.Dialog.HopCausesLoop.Message" ) );
      mb.setText( BaseMessages.getString( PKG, "TransGraph.Dialog.HopCausesLoop.Title" ) );
      mb.open();
      ok = false;
    }

    if ( ok ) { // only do the following checks, e.g. checkRowMixingStatically
      // when not looping, otherwise we get a loop with
      // StackOverflow there ;-)
      try {
        if ( !newHop.getToStep().getStepMetaInterface().excludeFromRowLayoutVerification() ) {
          transMeta.checkRowMixingStatically( newHop.getToStep(), null );
        }
      } catch ( HopRowException re ) {
        // Show warning about mixing rows with conflicting layouts...
        new ErrorDialog(
          hopUi.getShell(), BaseMessages.getString( PKG, "TransGraph.Dialog.HopCausesRowMixing.Title" ), BaseMessages
          .getString( PKG, "TransGraph.Dialog.HopCausesRowMixing.Message" ), re );
      }

      verifyCopyDistribute( transMeta, newHop.getFromStep() );
    }

    return ok;
  }

  public void verifyCopyDistribute( TransMeta transMeta, StepMeta fr ) {
    List<StepMeta> nextSteps = transMeta.findNextSteps( fr );
    int nrNextSteps = nextSteps.size();

    // don't show it for 3 or more hops, by then you should have had the
    // message
    if ( nrNextSteps == 2 ) {
      boolean distributes = fr.getStepMetaInterface().excludeFromCopyDistributeVerification();
      boolean customDistribution = false;

      if ( props.showCopyOrDistributeWarning()
        && !fr.getStepMetaInterface().excludeFromCopyDistributeVerification() ) {
        MessageDialogWithToggle md =
          new MessageDialogWithToggle(
            hopUi.getShell(), BaseMessages.getString( PKG, "System.Warning" ), null, BaseMessages.getString(
            PKG, "HopGui.Dialog.CopyOrDistribute.Message", fr.getName(), Integer.toString( nrNextSteps ) ),
            MessageDialog.WARNING, getRowDistributionLabels(), 0, BaseMessages.getString(
            PKG, "HopGui.Message.Warning.NotShowWarning" ), !props.showCopyOrDistributeWarning() );
        MessageDialogWithToggle.setDefaultImage( GUIResource.getInstance().getImageHopUi() );
        int idx = md.open();
        props.setShowCopyOrDistributeWarning( !md.getToggleState() );
        props.saveProps();

        distributes = idx == MESSAGE_DIALOG_WITH_TOGGLE_YES_BUTTON_ID;
        customDistribution = idx == MESSAGE_DIALOG_WITH_TOGGLE_CUSTOM_DISTRIBUTION_BUTTON_ID;
      }

      if ( distributes ) {
        fr.setDistributes( true );
        fr.setRowDistribution( null );
      } else if ( customDistribution ) {

        RowDistributionInterface rowDistribution = transGraph.askUserForCustomDistributionMethod();

        fr.setDistributes( true );
        fr.setRowDistribution( rowDistribution );
      } else {
        fr.setDistributes( false );
        fr.setDistributes( false );
      }

      transGraph.redraw();
    }
  }

  private String[] getRowDistributionLabels() {
    ArrayList<String> labels = new ArrayList<>();
    labels.add( BaseMessages.getString( PKG, "HopGui.Dialog.CopyOrDistribute.Distribute" ) );
    labels.add( BaseMessages.getString( PKG, "HopGui.Dialog.CopyOrDistribute.Copy" ) );
    if ( PluginRegistry.getInstance().getPlugins( RowDistributionPluginType.class ).size() > 0 ) {
      labels.add( BaseMessages.getString( PKG, "HopGui.Dialog.CopyOrDistribute.CustomRowDistribution" ) );
    }
    return labels.toArray( new String[ labels.size() ] );
  }

  public void delHop( TransMeta transMeta, TransHopMeta transHopMeta ) {
    int index = transMeta.indexOfTransHop( transHopMeta );

    hopUi.undoDelegate.addUndoDelete( transMeta, new Object[] { (TransHopMeta) transHopMeta.clone() }, new int[] { index } );
    transMeta.removeTransHop( index );

    StepMeta fromStepMeta = transHopMeta.getFromStep();
    StepMeta beforeFrom = (StepMeta) fromStepMeta.clone();
    int indexFrom = transMeta.indexOfStep( fromStepMeta );

    StepMeta toStepMeta = transHopMeta.getToStep();
    StepMeta beforeTo = (StepMeta) toStepMeta.clone();
    int indexTo = transMeta.indexOfStep( toStepMeta );

    boolean stepFromNeedAddUndoChange = fromStepMeta.getStepMetaInterface()
      .cleanAfterHopFromRemove( transHopMeta.getToStep() );
    boolean stepToNeedAddUndoChange = toStepMeta.getStepMetaInterface().cleanAfterHopToRemove( fromStepMeta );

    // If this is an error handling hop, disable it
    //
    if ( transHopMeta.getFromStep().isDoingErrorHandling() ) {
      StepErrorMeta stepErrorMeta = fromStepMeta.getStepErrorMeta();

      // We can only disable error handling if the target of the hop is the same as the target of the error handling.
      //
      if ( stepErrorMeta.getTargetStep() != null
        && stepErrorMeta.getTargetStep().equals( transHopMeta.getToStep() ) ) {

        // Only if the target step is where the error handling is going to...
        //
        stepErrorMeta.setEnabled( false );
        stepFromNeedAddUndoChange = true;
      }
    }

    if ( stepFromNeedAddUndoChange ) {
      hopUi.undoDelegate.addUndoChange( transMeta, new Object[] { beforeFrom }, new Object[] { fromStepMeta }, new int[] { indexFrom } );
    }

    if ( stepToNeedAddUndoChange ) {
      hopUi.undoDelegate.addUndoChange( transMeta, new Object[] { beforeTo }, new Object[] { toStepMeta }, new int[] { indexTo } );
    }

    transGraph.redraw();
  }

  public void editHop( TransMeta transMeta, TransHopMeta transHopMeta ) {
    // Backup situation BEFORE edit:
    String name = transHopMeta.toString();
    TransHopMeta before = (TransHopMeta) transHopMeta.clone();

    TransHopDialog hd = new TransHopDialog( hopUi.getShell(), SWT.NONE, transHopMeta, transMeta );
    if ( hd.open() != null ) {
      // Backup situation for redo/undo:
      TransHopMeta after = (TransHopMeta) transHopMeta.clone();
      /* TODO: Create new Undo/Redo system

      addUndoChange( transMeta, new TransHopMeta[] { before }, new TransHopMeta[] { after }, new int[] { transMeta
        .indexOfTransHop( transHopMeta ) } );
 */

      String newName = transHopMeta.toString();
      if ( !name.equalsIgnoreCase( newName ) ) {
        transGraph.redraw(); // color, nr of copies...
      }
    }
    transGraph.updateGui();
  }
}
