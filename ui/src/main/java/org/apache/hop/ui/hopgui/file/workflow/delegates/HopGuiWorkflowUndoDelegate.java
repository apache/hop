/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.ui.hopgui.file.workflow.delegates;

import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.undo.ChangeAction;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionCopy;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;

public class HopGuiWorkflowUndoDelegate {
  private static Class<?> PKG = HopGui.class; // for i18n purposes, needed by Translator!!

  private HopGuiWorkflowGraph jobGraph;
  private HopGui hopUi;

  /**
   * @param hopUi
   */
  public HopGuiWorkflowUndoDelegate( HopGui hopUi, HopGuiWorkflowGraph jobGraph ) {
    this.hopUi = hopUi;
    this.jobGraph = jobGraph;
  }

  public void undoJobAction( IHopFileTypeHandler handler, WorkflowMeta workflowMeta ) {
    ChangeAction changeAction = workflowMeta.previousUndo();
    if ( changeAction == null ) {
      return;
    }
    undoJobAction( handler, workflowMeta, changeAction );
    handler.updateGui();
  }


  public void undoJobAction( IHopFileTypeHandler handler, WorkflowMeta workflowMeta, ChangeAction changeAction ) {
    switch ( changeAction.getType() ) {
      // We created a new transform : undo this...
      case NewAction:
        // Delete the transform at correct location:
        for ( int i = changeAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = changeAction.getCurrentIndex()[ i ];
          workflowMeta.removeAction( idx );
        }
        break;

      // We created a new note : undo this...
      case NewNote:
        // Delete the note at correct location:
        for ( int i = changeAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = changeAction.getCurrentIndex()[ i ];
          workflowMeta.removeNote( idx );
        }
        break;

      // We created a new hop : undo this...
      case NewHop:
        // Delete the hop at correct location:
        for ( int i = changeAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = changeAction.getCurrentIndex()[ i ];
          workflowMeta.removeWorkflowHop( idx );
        }
        break;

      //
      // DELETE
      //

      // We delete a transform : undo this...
      case DeleteAction:
        // un-Delete the transform at correct location: re-insert
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          ActionCopy action = (ActionCopy) changeAction.getCurrent()[ i ];
          int idx = changeAction.getCurrentIndex()[ i ];
          workflowMeta.addAction( idx, action );
        }
        break;

      // We delete new note : undo this...
      case DeleteNote:
        // re-insert the note at correct location:
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          NotePadMeta ni = (NotePadMeta) changeAction.getCurrent()[ i ];
          int idx = changeAction.getCurrentIndex()[ i ];
          workflowMeta.addNote( idx, ni );
        }
        break;

      // We deleted a hop : undo this...
      case DeleteHop:
        // re-insert the hop at correct location:
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          WorkflowHopMeta hopMeta = (WorkflowHopMeta) changeAction.getCurrent()[ i ];
          int idx = changeAction.getCurrentIndex()[ i ];
          // Build a new hop:
          ActionCopy from = workflowMeta.findAction( hopMeta.getFromEntry().getName() );
          ActionCopy to = workflowMeta.findAction( hopMeta.getToEntry().getName() );
          WorkflowHopMeta newHopMeta = new WorkflowHopMeta( from, to );
          workflowMeta.addWorkflowHop( idx, newHopMeta );
        }
        break;

      //
      // CHANGE
      //

      // We changed a transform : undo this...
      case ChangeAction:
        // Delete the current transform, insert previous version.
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          ActionCopy prev = ( (ActionCopy) changeAction.getPrevious()[ i ] ).clone();
          int idx = changeAction.getCurrentIndex()[ i ];

          workflowMeta.getAction( idx ).replaceMeta( prev );
        }
        break;

      // We changed a note : undo this...
      case ChangeNote:
        // Delete & re-insert
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          int idx = changeAction.getCurrentIndex()[ i ];
          workflowMeta.removeNote( idx );
          NotePadMeta prev = (NotePadMeta) changeAction.getPrevious()[ i ];
          workflowMeta.addNote( idx, (NotePadMeta) prev.clone() );
        }
        break;

      // We changed a hop : undo this...
      case ChangeHop:
        // Delete & re-insert
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          WorkflowHopMeta prev = (WorkflowHopMeta) changeAction.getPrevious()[ i ];
          int idx = changeAction.getCurrentIndex()[ i ];

          workflowMeta.removeWorkflowHop( idx );
          workflowMeta.addWorkflowHop( idx, (WorkflowHopMeta) prev.clone() );
        }
        break;

      //
      // POSITION
      //

      // The position of a transform has changed: undo this...
      case PositionAction:
        // Find the location of the transform:
        for ( int i = 0; i < changeAction.getCurrentIndex().length; i++ ) {
          ActionCopy action = workflowMeta.getAction( changeAction.getCurrentIndex()[ i ] );
          action.setLocation( changeAction.getPreviousLocation()[ i ] );
        }
        break;

      // The position of a note has changed: undo this...
      case PositionNote:
        for ( int i = 0; i < changeAction.getCurrentIndex().length; i++ ) {
          int idx = changeAction.getCurrentIndex()[ i ];
          NotePadMeta npi = workflowMeta.getNote( idx );
          Point prev = changeAction.getPreviousLocation()[ i ];
          npi.setLocation( prev );
        }
        break;
      default:
        break;
    }

    // OK, now check if we need to do this again...
    if ( workflowMeta.viewNextUndo() != null ) {
      if ( workflowMeta.viewNextUndo().getNextAlso() ) {
        undoJobAction( handler, workflowMeta );
      }
    }
  }

  public void redoJobAction( IHopFileTypeHandler handler, WorkflowMeta workflowMeta ) {
    ChangeAction changeAction = workflowMeta.nextUndo();
    if ( changeAction == null ) {
      return;
    }
    redoJobAction( handler, workflowMeta, changeAction );
    handler.updateGui();
  }

  public void redoJobAction( IHopFileTypeHandler handler, WorkflowMeta workflowMeta, ChangeAction changeAction ) {
    switch ( changeAction.getType() ) {
      case NewAction:
        // re-delete the transform at correct location:
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          ActionCopy entryCopy = (ActionCopy) changeAction.getCurrent()[ i ];
          int idx = changeAction.getCurrentIndex()[ i ];
          workflowMeta.addAction( idx, entryCopy );
        }
        break;

      case NewNote:
        // re-insert the note at correct location:
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          NotePadMeta ni = (NotePadMeta) changeAction.getCurrent()[ i ];
          int idx = changeAction.getCurrentIndex()[ i ];
          workflowMeta.addNote( idx, ni );
        }
        break;

      case NewHop:
        // re-insert the hop at correct location:
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          WorkflowHopMeta hopMeta = (WorkflowHopMeta) changeAction.getCurrent()[ i ];
          int idx = changeAction.getCurrentIndex()[ i ];
          workflowMeta.addWorkflowHop( idx, hopMeta );
        }
        break;

      //
      // DELETE
      //
      case DeleteAction:
        // re-remove the transform at correct location:
        for ( int i = changeAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = changeAction.getCurrentIndex()[ i ];
          workflowMeta.removeAction( idx );
        }
        break;

      case DeleteNote:
        // re-remove the note at correct location:
        for ( int i = changeAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = changeAction.getCurrentIndex()[ i ];
          workflowMeta.removeNote( idx );
        }
        break;

      case DeleteHop:
        // re-remove the hop at correct location:
        for ( int i = changeAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = changeAction.getCurrentIndex()[ i ];
          workflowMeta.removeWorkflowHop( idx );
        }
        break;

      //
      // CHANGE
      //

      // We changed a transform : undo this...
      case ChangeTransform:
        // Delete the current transform, insert previous version.
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          ActionCopy clonedEntry = ( (ActionCopy) changeAction.getCurrent()[ i ] ).clone();
          workflowMeta.getAction( changeAction.getCurrentIndex()[ i ] ).replaceMeta( clonedEntry );
        }
        break;

      // We changed a note : undo this...
      case ChangeNote:
        // Delete & re-insert
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          NotePadMeta ni = (NotePadMeta) changeAction.getCurrent()[ i ];
          int idx = changeAction.getCurrentIndex()[ i ];

          workflowMeta.removeNote( idx );
          workflowMeta.addNote( idx, ni.clone() );
        }
        break;

      // We changed a hop : undo this...
      case ChangeHop:
        // Delete & re-insert
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          WorkflowHopMeta hi = (WorkflowHopMeta) changeAction.getCurrent()[ i ];
          int idx = changeAction.getCurrentIndex()[ i ];

          workflowMeta.removeWorkflowHop( idx );
          workflowMeta.addWorkflowHop( idx, (WorkflowHopMeta) hi.clone() );
        }
        break;

      //
      // CHANGE POSITION
      //
      case PositionTransform:
        for ( int i = 0; i < changeAction.getCurrentIndex().length; i++ ) {
          // Find & change the location of the transform:
          ActionCopy action = workflowMeta.getAction( changeAction.getCurrentIndex()[ i ] );
          action.setLocation( changeAction.getCurrentLocation()[ i ] );
        }
        break;
      case PositionNote:
        for ( int i = 0; i < changeAction.getCurrentIndex().length; i++ ) {
          int idx = changeAction.getCurrentIndex()[ i ];
          NotePadMeta npi = workflowMeta.getNote( idx );
          Point curr = changeAction.getCurrentLocation()[ i ];
          npi.setLocation( curr );
        }
        break;
      default:
        break;
    }

    // OK, now check if we need to do this again...
    if ( workflowMeta.viewNextUndo() != null ) {
      if ( workflowMeta.viewNextUndo().getNextAlso() ) {
        redoJobAction( handler, workflowMeta );
      }
    }
  }

  /**
   * Gets jobGraph
   *
   * @return value of jobGraph
   */
  public HopGuiWorkflowGraph getJobGraph() {
    return jobGraph;
  }

  /**
   * @param jobGraph The jobGraph to set
   */
  public void setJobGraph( HopGuiWorkflowGraph jobGraph ) {
    this.jobGraph = jobGraph;
  }

  /**
   * Gets hopUi
   *
   * @return value of hopUi
   */
  public HopGui getHopUi() {
    return hopUi;
  }

  /**
   * @param hopUi The hopUi to set
   */
  public void setHopUi( HopGui hopUi ) {
    this.hopUi = hopUi;
  }
}
