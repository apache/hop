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

import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.undo.ChangeAction;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;

public class HopGuiWorkflowUndoDelegate {
  private static final Class<?> PKG = HopGui.class; // For Translator

  private HopGuiWorkflowGraph workflowGraph;
  private HopGui hopGui;

  /**
   * @param hopGui
   */
  public HopGuiWorkflowUndoDelegate( HopGui hopGui, HopGuiWorkflowGraph workflowGraph ) {
    this.hopGui = hopGui;
    this.workflowGraph = workflowGraph;
  }

  public void undoWorkflowAction( IHopFileTypeHandler handler, WorkflowMeta workflowMeta ) {
    ChangeAction changeAction = workflowMeta.previousUndo();
    if ( changeAction == null ) {
      return;
    }
    undoWorkflowAction( handler, workflowMeta, changeAction );
    handler.updateGui();
  }


  public void undoWorkflowAction( IHopFileTypeHandler handler, WorkflowMeta workflowMeta, ChangeAction changeAction ) {
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
          ActionMeta action = (ActionMeta) changeAction.getCurrent()[ i ];
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
          ActionMeta from = workflowMeta.findAction( hopMeta.getFromAction().getName() );
          ActionMeta to = workflowMeta.findAction( hopMeta.getToAction().getName() );
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
          ActionMeta prev = ( (ActionMeta) changeAction.getPrevious()[ i ] ).clone();
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
          ActionMeta action = workflowMeta.getAction( changeAction.getCurrentIndex()[ i ] );
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
        undoWorkflowAction( handler, workflowMeta );
      }
    }
  }

  public void redoWorkflowAction( IHopFileTypeHandler handler, WorkflowMeta workflowMeta ) {
    ChangeAction changeAction = workflowMeta.nextUndo();
    if ( changeAction == null ) {
      return;
    }
    redoWorkflowAction( handler, workflowMeta, changeAction );
    handler.updateGui();
  }

  public void redoWorkflowAction( IHopFileTypeHandler handler, WorkflowMeta workflowMeta, ChangeAction changeAction ) {
    switch ( changeAction.getType() ) {
      case NewAction:
        // re-delete the transform at correct location:
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          ActionMeta entryCopy = (ActionMeta) changeAction.getCurrent()[ i ];
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
          ActionMeta clonedEntry = ( (ActionMeta) changeAction.getCurrent()[ i ] ).clone();
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
          ActionMeta action = workflowMeta.getAction( changeAction.getCurrentIndex()[ i ] );
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
        redoWorkflowAction( handler, workflowMeta );
      }
    }
  }

  /**
   * Gets workflowGraph
   *
   * @return value of workflowGraph
   */
  public HopGuiWorkflowGraph getWorkflowGraph() {
    return workflowGraph;
  }

  /**
   * @param workflowGraph The workflowGraph to set
   */
  public void setWorkflowGraph( HopGuiWorkflowGraph workflowGraph ) {
    this.workflowGraph = workflowGraph;
  }

  /**
   * Gets hopGui
   *
   * @return value of hopGui
   */
  public HopGui getHopGui() {
    return hopGui;
  }

  /**
   * @param hopGui The hopGui to set
   */
  public void setHopGui( HopGui hopGui ) {
    this.hopGui = hopGui;
  }
}
