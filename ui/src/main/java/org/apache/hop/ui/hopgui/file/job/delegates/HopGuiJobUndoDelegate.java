/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.hopgui.file.job.delegates;

import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.undo.TransAction;
import org.apache.hop.job.JobHopMeta;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryCopy;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.HopFileTypeHandlerInterface;
import org.apache.hop.ui.hopgui.file.job.HopGuiJobGraph;

public class HopGuiJobUndoDelegate {
  private static Class<?> PKG = HopGui.class; // for i18n purposes, needed by Translator!!

  private HopGuiJobGraph jobGraph;
  private HopGui hopUi;

  /**
   * @param hopUi
   */
  public HopGuiJobUndoDelegate( HopGui hopUi, HopGuiJobGraph jobGraph ) {
    this.hopUi = hopUi;
    this.jobGraph = jobGraph;
  }

  public void undoJobAction( HopFileTypeHandlerInterface handler, JobMeta jobMeta ) {
    TransAction transAction = jobMeta.previousUndo();
    if ( transAction == null ) {
      return;
    }
    undoJobAction( handler, jobMeta, transAction );
    handler.updateGui();
  }


  public void undoJobAction( HopFileTypeHandlerInterface handler, JobMeta jobMeta, TransAction transAction ) {
    switch ( transAction.getType() ) {
      // We created a new step : undo this...
      case NewJobEntry:
        // Delete the step at correct location:
        for ( int i = transAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = transAction.getCurrentIndex()[ i ];
          jobMeta.removeJobEntry( idx );
        }
        break;

      // We created a new note : undo this...
      case NewNote:
        // Delete the note at correct location:
        for ( int i = transAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = transAction.getCurrentIndex()[ i ];
          jobMeta.removeNote( idx );
        }
        break;

      // We created a new hop : undo this...
      case NewHop:
        // Delete the hop at correct location:
        for ( int i = transAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = transAction.getCurrentIndex()[ i ];
          jobMeta.removeJobHop( idx );
        }
        break;

      //
      // DELETE
      //

      // We delete a step : undo this...
      case DeleteJobEntry:
        // un-Delete the step at correct location: re-insert
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          JobEntryCopy entry = (JobEntryCopy) transAction.getCurrent()[ i ];
          int idx = transAction.getCurrentIndex()[ i ];
          jobMeta.addJobEntry( idx, entry );
        }
        break;

      // We delete new note : undo this...
      case DeleteNote:
        // re-insert the note at correct location:
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          NotePadMeta ni = (NotePadMeta) transAction.getCurrent()[ i ];
          int idx = transAction.getCurrentIndex()[ i ];
          jobMeta.addNote( idx, ni );
        }
        break;

      // We deleted a hop : undo this...
      case DeleteHop:
        // re-insert the hop at correct location:
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          JobHopMeta hopMeta = (JobHopMeta) transAction.getCurrent()[ i ];
          int idx = transAction.getCurrentIndex()[ i ];
          // Build a new hop:
          JobEntryCopy from = jobMeta.findJobEntry( hopMeta.getFromEntry().getName() );
          JobEntryCopy to = jobMeta.findJobEntry( hopMeta.getToEntry().getName() );
          JobHopMeta newHopMeta = new JobHopMeta( from, to );
          jobMeta.addJobHop( idx, newHopMeta );
        }
        break;

      //
      // CHANGE
      //

      // We changed a step : undo this...
      case ChangeJobEntry:
        // Delete the current step, insert previous version.
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          JobEntryCopy prev = ( (JobEntryCopy) transAction.getPrevious()[ i ] ).clone();
          int idx = transAction.getCurrentIndex()[ i ];

          jobMeta.getJobEntry( idx ).replaceMeta( prev );
        }
        break;

      // We changed a note : undo this...
      case ChangeNote:
        // Delete & re-insert
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          int idx = transAction.getCurrentIndex()[ i ];
          jobMeta.removeNote( idx );
          NotePadMeta prev = (NotePadMeta) transAction.getPrevious()[ i ];
          jobMeta.addNote( idx, (NotePadMeta) prev.clone() );
        }
        break;

      // We changed a hop : undo this...
      case ChangeHop:
        // Delete & re-insert
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          JobHopMeta prev = (JobHopMeta) transAction.getPrevious()[ i ];
          int idx = transAction.getCurrentIndex()[ i ];

          jobMeta.removeJobHop( idx );
          jobMeta.addJobHop( idx, (JobHopMeta) prev.clone() );
        }
        break;

      //
      // POSITION
      //

      // The position of a step has changed: undo this...
      case PositionJobEntry:
        // Find the location of the step:
        for ( int i = 0; i < transAction.getCurrentIndex().length; i++ ) {
          JobEntryCopy jobEntry = jobMeta.getJobEntry( transAction.getCurrentIndex()[ i ] );
          jobEntry.setLocation( transAction.getPreviousLocation()[ i ] );
        }
        break;

      // The position of a note has changed: undo this...
      case PositionNote:
        for ( int i = 0; i < transAction.getCurrentIndex().length; i++ ) {
          int idx = transAction.getCurrentIndex()[ i ];
          NotePadMeta npi = jobMeta.getNote( idx );
          Point prev = transAction.getPreviousLocation()[ i ];
          npi.setLocation( prev );
        }
        break;
      default:
        break;
    }

    // OK, now check if we need to do this again...
    if ( jobMeta.viewNextUndo() != null ) {
      if ( jobMeta.viewNextUndo().getNextAlso() ) {
        undoJobAction( handler, jobMeta );
      }
    }
  }

  public void redoJobAction( HopFileTypeHandlerInterface handler, JobMeta jobMeta ) {
    TransAction transAction = jobMeta.nextUndo();
    if ( transAction == null ) {
      return;
    }
    redoJobAction( handler, jobMeta, transAction );
    handler.updateGui();
  }

  public void redoJobAction( HopFileTypeHandlerInterface handler, JobMeta jobMeta, TransAction transAction ) {
    switch ( transAction.getType() ) {
      case NewJobEntry:
        // re-delete the step at correct location:
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          JobEntryCopy entryCopy = (JobEntryCopy) transAction.getCurrent()[ i ];
          int idx = transAction.getCurrentIndex()[ i ];
          jobMeta.addJobEntry( idx, entryCopy );
        }
        break;

      case NewNote:
        // re-insert the note at correct location:
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          NotePadMeta ni = (NotePadMeta) transAction.getCurrent()[ i ];
          int idx = transAction.getCurrentIndex()[ i ];
          jobMeta.addNote( idx, ni );
        }
        break;

      case NewHop:
        // re-insert the hop at correct location:
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          JobHopMeta hopMeta = (JobHopMeta) transAction.getCurrent()[ i ];
          int idx = transAction.getCurrentIndex()[ i ];
          jobMeta.addJobHop( idx, hopMeta );
        }
        break;

      //
      // DELETE
      //
      case DeleteJobEntry:
        // re-remove the step at correct location:
        for ( int i = transAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = transAction.getCurrentIndex()[ i ];
          jobMeta.removeJobEntry( idx );
        }
        break;

      case DeleteNote:
        // re-remove the note at correct location:
        for ( int i = transAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = transAction.getCurrentIndex()[ i ];
          jobMeta.removeNote( idx );
        }
        break;

      case DeleteHop:
        // re-remove the hop at correct location:
        for ( int i = transAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = transAction.getCurrentIndex()[ i ];
          jobMeta.removeJobHop( idx );
        }
        break;

      //
      // CHANGE
      //

      // We changed a step : undo this...
      case ChangeStep:
        // Delete the current step, insert previous version.
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          JobEntryCopy clonedEntry = ( (JobEntryCopy) transAction.getCurrent()[ i ] ).clone();
          jobMeta.getJobEntry( transAction.getCurrentIndex()[ i ] ).replaceMeta( clonedEntry );
        }
        break;

      // We changed a note : undo this...
      case ChangeNote:
        // Delete & re-insert
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          NotePadMeta ni = (NotePadMeta) transAction.getCurrent()[ i ];
          int idx = transAction.getCurrentIndex()[ i ];

          jobMeta.removeNote( idx );
          jobMeta.addNote( idx, ni.clone() );
        }
        break;

      // We changed a hop : undo this...
      case ChangeHop:
        // Delete & re-insert
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          JobHopMeta hi = (JobHopMeta) transAction.getCurrent()[ i ];
          int idx = transAction.getCurrentIndex()[ i ];

          jobMeta.removeJobHop( idx );
          jobMeta.addJobHop( idx, (JobHopMeta) hi.clone() );
        }
        break;

      //
      // CHANGE POSITION
      //
      case PositionStep:
        for ( int i = 0; i < transAction.getCurrentIndex().length; i++ ) {
          // Find & change the location of the step:
          JobEntryCopy jobEntry = jobMeta.getJobEntry( transAction.getCurrentIndex()[ i ] );
          jobEntry.setLocation( transAction.getCurrentLocation()[ i ] );
        }
        break;
      case PositionNote:
        for ( int i = 0; i < transAction.getCurrentIndex().length; i++ ) {
          int idx = transAction.getCurrentIndex()[ i ];
          NotePadMeta npi = jobMeta.getNote( idx );
          Point curr = transAction.getCurrentLocation()[ i ];
          npi.setLocation( curr );
        }
        break;
      default:
        break;
    }

    // OK, now check if we need to do this again...
    if ( jobMeta.viewNextUndo() != null ) {
      if ( jobMeta.viewNextUndo().getNextAlso() ) {
        redoJobAction( handler, jobMeta );
      }
    }
  }

  /**
   * Gets jobGraph
   *
   * @return value of jobGraph
   */
  public HopGuiJobGraph getJobGraph() {
    return jobGraph;
  }

  /**
   * @param jobGraph The jobGraph to set
   */
  public void setJobGraph( HopGuiJobGraph jobGraph ) {
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
