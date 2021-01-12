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

package org.apache.hop.ui.hopgui.file.pipeline.delegates;

import org.apache.hop.core.exception.HopRowException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.IRowDistribution;
import org.apache.hop.pipeline.transform.RowDistributionPluginType;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageDialogWithToggle;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.pipeline.dialog.PipelineHopDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;

import java.util.ArrayList;
import java.util.List;

public class HopGuiPipelineHopDelegate {

  // TODO: move i18n package to HopGui
  private static final Class<?> PKG = HopGui.class; // For Translator

  public static final int MESSAGE_DIALOG_WITH_TOGGLE_YES_BUTTON_ID = 256;

  public static final int MESSAGE_DIALOG_WITH_TOGGLE_NO_BUTTON_ID = 257;

  public static final int MESSAGE_DIALOG_WITH_TOGGLE_CUSTOM_DISTRIBUTION_BUTTON_ID = 258;


  private HopGui hopGui;
  private HopGuiPipelineGraph pipelineGraph;
  private PropsUi props;

  public HopGuiPipelineHopDelegate( HopGui hopGui, HopGuiPipelineGraph pipelineGraph ) {
    this.hopGui = hopGui;
    this.pipelineGraph = pipelineGraph;
    this.props = PropsUi.getInstance();
  }

  public void newHop( PipelineMeta pipelineMeta, TransformMeta fr, TransformMeta to ) {
    PipelineHopMeta hi = new PipelineHopMeta( fr, to );

    PipelineHopDialog hd = new PipelineHopDialog( hopGui.getShell(), SWT.NONE, hi, pipelineMeta );
    if ( hd.open() != null ) {
      newHop( pipelineMeta, hi );
    }
  }

  public void newHop( PipelineMeta pipelineMeta, PipelineHopMeta pipelineHopMeta ) {
    if ( checkIfHopAlreadyExists( pipelineMeta, pipelineHopMeta ) ) {
      pipelineMeta.addPipelineHop( pipelineHopMeta );
      int idx = pipelineMeta.indexOfPipelineHop( pipelineHopMeta );

      if ( !performNewPipelineHopChecks( pipelineMeta, pipelineHopMeta ) ) {
        // Some error occurred: loops, existing hop, etc.
        // Remove it again...
        //
        pipelineMeta.removePipelineHop( idx );
      } else {
        hopGui.undoDelegate.addUndoNew( pipelineMeta, new PipelineHopMeta[] { pipelineHopMeta }, new int[] { pipelineMeta.indexOfPipelineHop( pipelineHopMeta ) } );
      }

      pipelineGraph.redraw();
    }
  }

  /**
   * @param pipelineMeta pipeline's meta
   * @param newHop    hop to be checked
   * @return true when the hop was added, false if there was an error
   */
  public boolean checkIfHopAlreadyExists( PipelineMeta pipelineMeta, PipelineHopMeta newHop ) {
    boolean ok = true;
    if ( pipelineMeta.findPipelineHop( newHop.getFromTransform(), newHop.getToTransform() ) != null ) {
      MessageBox mb = new MessageBox( hopGui.getShell(), SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "HopGui.Dialog.HopExists.Message" ) ); // "This hop already exists!"
      mb.setText( BaseMessages.getString( PKG, "HopGui.Dialog.HopExists.Title" ) ); // Error!
      mb.open();
      ok = false;
    }

    return ok;
  }

  /**
   * @param pipelineMeta pipeline's meta
   * @param newHop    hop to be checked
   * @return true when the hop was added, false if there was an error
   */
  public boolean performNewPipelineHopChecks( PipelineMeta pipelineMeta, PipelineHopMeta newHop ) {
    boolean ok = true;

    if ( pipelineMeta.hasLoop( newHop.getToTransform() ) ) {
      MessageBox mb = new MessageBox( hopGui.getShell(), SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "PipelineGraph.Dialog.HopCausesLoop.Message" ) );
      mb.setText( BaseMessages.getString( PKG, "PipelineGraph.Dialog.HopCausesLoop.Title" ) );
      mb.open();
      ok = false;
    }

    if ( ok ) { // only do the following checks, e.g. checkRowMixingStatically
      // when not looping, otherwise we get a loop with
      // StackOverflow there ;-)
      try {
        if ( !newHop.getToTransform().getTransform().excludeFromRowLayoutVerification() ) {
          pipelineMeta.checkRowMixingStatically( pipelineGraph.getVariables(), newHop.getToTransform(), null );
        }
      } catch ( HopRowException re ) {
        // Show warning about mixing rows with conflicting layouts...
        new ErrorDialog(
          hopGui.getShell(), BaseMessages.getString( PKG, "PipelineGraph.Dialog.HopCausesRowMixing.Title" ), BaseMessages
          .getString( PKG, "PipelineGraph.Dialog.HopCausesRowMixing.Message" ), re );
      }

      verifyCopyDistribute( pipelineMeta, newHop.getFromTransform() );
    }

    return ok;
  }

  public void verifyCopyDistribute( PipelineMeta pipelineMeta, TransformMeta fr ) {
    List<TransformMeta> nextTransforms = pipelineMeta.findNextTransforms( fr );
    int nrNextTransforms = nextTransforms.size();

    // don't show it for 3 or more hops, by then you should have had the
    // message
    if ( nrNextTransforms == 2 ) {
      boolean distributes = fr.getTransform().excludeFromCopyDistributeVerification();
      boolean customDistribution = false;

      if ( props.showCopyOrDistributeWarning()
        && !fr.getTransform().excludeFromCopyDistributeVerification() ) {
        MessageDialogWithToggle md =
          new MessageDialogWithToggle(
            hopGui.getShell(), BaseMessages.getString( PKG, "System.Warning" ), BaseMessages.getString(
            PKG, "HopGui.Dialog.CopyOrDistribute.Message", fr.getName(), Integer.toString( nrNextTransforms ) ),
            SWT.ICON_WARNING, getRowDistributionLabels(), BaseMessages.getString(
            PKG, "HopGui.Message.Warning.NotShowWarning" ), !props.showCopyOrDistributeWarning() );
        int idx = md.open();
        props.setShowCopyOrDistributeWarning( !md.getToggleState() );

        distributes = idx == MESSAGE_DIALOG_WITH_TOGGLE_YES_BUTTON_ID;
        customDistribution = idx == MESSAGE_DIALOG_WITH_TOGGLE_CUSTOM_DISTRIBUTION_BUTTON_ID;
      }

      if ( distributes ) {
        fr.setDistributes( true );
        fr.setRowDistribution( null );
      } else if ( customDistribution ) {

        IRowDistribution rowDistribution = pipelineGraph.askUserForCustomDistributionMethod();

        fr.setDistributes( true );
        fr.setRowDistribution( rowDistribution );
      } else {
        fr.setDistributes( false );
        fr.setDistributes( false );
      }

      pipelineGraph.redraw();
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

  public void delHop( PipelineMeta pipelineMeta, PipelineHopMeta pipelineHopMeta ) {
    int index = pipelineMeta.indexOfPipelineHop( pipelineHopMeta );

    hopGui.undoDelegate.addUndoDelete( pipelineMeta, new Object[] { (PipelineHopMeta) pipelineHopMeta.clone() }, new int[] { index } );
    pipelineMeta.removePipelineHop( index );

    TransformMeta fromTransformMeta = pipelineHopMeta.getFromTransform();
    TransformMeta beforeFrom = (TransformMeta) fromTransformMeta.clone();
    int indexFrom = pipelineMeta.indexOfTransform( fromTransformMeta );

    TransformMeta toTransformMeta = pipelineHopMeta.getToTransform();
    TransformMeta beforeTo = (TransformMeta) toTransformMeta.clone();
    int indexTo = pipelineMeta.indexOfTransform( toTransformMeta );

    boolean transformFromNeedAddUndoChange = fromTransformMeta.getTransform()
      .cleanAfterHopFromRemove( pipelineHopMeta.getToTransform() );
    boolean transformToNeedAddUndoChange = toTransformMeta.getTransform().cleanAfterHopToRemove( fromTransformMeta );

    // If this is an error handling hop, disable it
    //
    if ( pipelineHopMeta.getFromTransform().isDoingErrorHandling() ) {
      TransformErrorMeta transformErrorMeta = fromTransformMeta.getTransformErrorMeta();

      // We can only disable error handling if the target of the hop is the same as the target of the error handling.
      //
      if ( transformErrorMeta.getTargetTransform() != null
        && transformErrorMeta.getTargetTransform().equals( pipelineHopMeta.getToTransform() ) ) {

        // Only if the target transform is where the error handling is going to...
        //
        transformErrorMeta.setEnabled( false );
        transformFromNeedAddUndoChange = true;
      }
    }

    if ( transformFromNeedAddUndoChange ) {
      hopGui.undoDelegate.addUndoChange( pipelineMeta, new Object[] { beforeFrom }, new Object[] { fromTransformMeta }, new int[] { indexFrom } );
    }

    if ( transformToNeedAddUndoChange ) {
      hopGui.undoDelegate.addUndoChange( pipelineMeta, new Object[] { beforeTo }, new Object[] { toTransformMeta }, new int[] { indexTo } );
    }

    pipelineGraph.redraw();
  }

  public void editHop( PipelineMeta pipelineMeta, PipelineHopMeta pipelineHopMeta ) {
    // Backup situation BEFORE edit:
    String name = pipelineHopMeta.toString();
    PipelineHopMeta before = (PipelineHopMeta) pipelineHopMeta.clone();

    PipelineHopDialog hd = new PipelineHopDialog( hopGui.getShell(), SWT.NONE, pipelineHopMeta, pipelineMeta );
    if ( hd.open() != null ) {
      // Backup situation for redo/undo:
      PipelineHopMeta after = (PipelineHopMeta) pipelineHopMeta.clone();
      /* TODO: Create new Undo/Redo system

      addUndoChange( pipelineMeta, new PipelineHopMeta[] { before }, new PipelineHopMeta[] { after }, new int[] { pipelineMeta
        .indexOfPipelineHop( pipelineHopMeta ) } );
 */

      String newName = pipelineHopMeta.toString();
      if ( !name.equalsIgnoreCase( newName ) ) {
        pipelineGraph.redraw(); // color, nr of copies...
      }
    }
    pipelineGraph.updateGui();
  }
}
