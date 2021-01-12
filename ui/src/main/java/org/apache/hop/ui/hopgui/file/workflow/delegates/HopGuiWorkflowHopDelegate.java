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

package org.apache.hop.ui.hopgui.file.workflow.delegates;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;

public class HopGuiWorkflowHopDelegate {

  private static final Class<?> PKG = HopGui.class; // For Translator


  private HopGui hopGui;
  private HopGuiWorkflowGraph workflowGraph;
  private PropsUi props;

  public HopGuiWorkflowHopDelegate( HopGui hopGui, HopGuiWorkflowGraph workflowGraph ) {
    this.hopGui = hopGui;
    this.workflowGraph = workflowGraph;
    this.props = PropsUi.getInstance();
  }

  public void newHop( WorkflowMeta workflowMeta, ActionMeta fr, ActionMeta to ) {
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
        hopGui.undoDelegate.addUndoNew( workflowMeta, new WorkflowHopMeta[] { hopMeta }, new int[] { workflowMeta.indexOfWorkflowHop( hopMeta ) } );
      }

      workflowGraph.updateGui();
    }
  }

  /**
   * @param workflowMeta workflow metadata
   * @param newHop  hop to be checked
   * @return true when the hop was added, false if there was an error
   */
  public boolean checkIfHopAlreadyExists( WorkflowMeta workflowMeta, WorkflowHopMeta newHop ) {
    boolean ok = true;
    if ( workflowMeta.findWorkflowHop( newHop.getFromAction(), newHop.getToAction() ) != null ) {
      MessageBox mb = new MessageBox( hopGui.getShell(), SWT.OK | SWT.ICON_ERROR );
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

    if ( workflowMeta.hasLoop( newHop.getToAction() ) ) {
      MessageBox mb = new MessageBox( hopGui.getShell(), SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "WorkflowGraph.Dialog.HopCausesLoop.Message" ) );
      mb.setText( BaseMessages.getString( PKG, "WorkflowGraph.Dialog.HopCausesLoop.Title" ) );
      mb.open();
      ok = false;
    }

    return ok;
  }

  public void delHop( WorkflowMeta workflowMeta, WorkflowHopMeta hopMeta ) {
    int index = workflowMeta.indexOfWorkflowHop( hopMeta );

    hopGui.undoDelegate.addUndoDelete( workflowMeta, new Object[] { hopMeta.clone() }, new int[] { index } );
    workflowMeta.removeWorkflowHop( index );
  }
}
