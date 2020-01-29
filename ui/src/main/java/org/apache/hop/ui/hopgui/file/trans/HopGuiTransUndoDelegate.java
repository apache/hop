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

package org.apache.hop.ui.hopgui.file.trans;

import org.apache.hop.core.Const;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.Props;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.undo.TransAction;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.TransHopMeta;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.HopFileTypeHandlerInterface;
import org.apache.hop.ui.hopui.HopUi;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.events.DisposeEvent;
import org.eclipse.swt.events.DisposeListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.ToolBar;

import java.util.ArrayList;
import java.util.Map;

public class HopGuiTransUndoDelegate {
  private static Class<?> PKG = HopUi.class; // for i18n purposes, needed by Translator2!!

  private HopGuiTransGraph transGraph;
  private HopGui hopUi;

  /**
   * @param hopUi
   */
  public HopGuiTransUndoDelegate( HopGui hopUi, HopGuiTransGraph transGraph ) {
    this.hopUi = hopUi;
    this.transGraph = transGraph;
  }

  public void undoTransformationAction( HopFileTypeHandlerInterface handler, TransMeta transMeta ) {
    TransAction transAction = transMeta.previousUndo();
    if ( transAction == null ) {
      return;
    }
    undoTransformationAction( handler, transMeta, transAction );
    handler.updateGui();
  }


  public void undoTransformationAction( HopFileTypeHandlerInterface handler, TransMeta transMeta, TransAction transAction ) {
    switch ( transAction.getType() ) {
      // We created a new step : undo this...
      case NewStep:
        // Delete the step at correct location:
        for ( int i = transAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = transAction.getCurrentIndex()[ i ];
          transMeta.removeStep( idx );
        }
        break;

      // We created a new note : undo this...
      case NewNote:
        // Delete the note at correct location:
        for ( int i = transAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = transAction.getCurrentIndex()[ i ];
          transMeta.removeNote( idx );
        }
        break;

      // We created a new hop : undo this...
      case NewHop:
        // Delete the hop at correct location:
        for ( int i = transAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = transAction.getCurrentIndex()[ i ];
          transMeta.removeTransHop( idx );
        }
        break;

      //
      // DELETE
      //

      // We delete a step : undo this...
      case DeleteStep:
        // un-Delete the step at correct location: re-insert
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          StepMeta stepMeta = (StepMeta) transAction.getCurrent()[ i ];
          int idx = transAction.getCurrentIndex()[ i ];
          transMeta.addStep( idx, stepMeta );
        }
        break;

      // We delete new note : undo this...
      case DeleteNote:
        // re-insert the note at correct location:
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          NotePadMeta ni = (NotePadMeta) transAction.getCurrent()[ i ];
          int idx = transAction.getCurrentIndex()[ i ];
          transMeta.addNote( idx, ni );
        }
        break;

      // We deleted a hop : undo this...
      case DeleteHop:
        // re-insert the hop at correct location:
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          TransHopMeta hi = (TransHopMeta) transAction.getCurrent()[ i ];
          int idx = transAction.getCurrentIndex()[ i ];
          // Build a new hop:
          StepMeta from = transMeta.findStep( hi.getFromStep().getName() );
          StepMeta to = transMeta.findStep( hi.getToStep().getName() );
          TransHopMeta hinew = new TransHopMeta( from, to );
          transMeta.addTransHop( idx, hinew );
        }
        break;

      //
      // CHANGE
      //

      // We changed a step : undo this...
      case ChangeStep:
        // Delete the current step, insert previous version.
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          StepMeta prev = (StepMeta) ( (StepMeta) transAction.getPrevious()[ i ] ).clone();
          int idx = transAction.getCurrentIndex()[ i ];

          transMeta.getStep( idx ).replaceMeta( prev );
        }
        break;

      // We changed a note : undo this...
      case ChangeNote:
        // Delete & re-insert
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          int idx = transAction.getCurrentIndex()[ i ];
          transMeta.removeNote( idx );
          NotePadMeta prev = (NotePadMeta) transAction.getPrevious()[ i ];
          transMeta.addNote( idx, (NotePadMeta) prev.clone() );
        }
        break;

      // We changed a hop : undo this...
      case ChangeHop:
        // Delete & re-insert
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          TransHopMeta prev = (TransHopMeta) transAction.getPrevious()[ i ];
          int idx = transAction.getCurrentIndex()[ i ];

          transMeta.removeTransHop( idx );
          transMeta.addTransHop( idx, (TransHopMeta) prev.clone() );
        }
        break;

      //
      // POSITION
      //

      // The position of a step has changed: undo this...
      case PositionStep:
        // Find the location of the step:
        for ( int i = 0; i < transAction.getCurrentIndex().length; i++ ) {
          StepMeta stepMeta = transMeta.getStep( transAction.getCurrentIndex()[ i ] );
          stepMeta.setLocation( transAction.getPreviousLocation()[ i ] );
        }
        break;

      // The position of a note has changed: undo this...
      case PositionNote:
        for ( int i = 0; i < transAction.getCurrentIndex().length; i++ ) {
          int idx = transAction.getCurrentIndex()[ i ];
          NotePadMeta npi = transMeta.getNote( idx );
          Point prev = transAction.getPreviousLocation()[ i ];
          npi.setLocation( prev );
        }
        break;
      default:
        break;
    }

    // OK, now check if we need to do this again...
    if ( transMeta.viewNextUndo() != null ) {
      if ( transMeta.viewNextUndo().getNextAlso() ) {
        undoTransformationAction( handler, transMeta );
      }
    }
  }

  public void redoTransformationAction( HopFileTypeHandlerInterface handler, TransMeta transMeta) {
    TransAction transAction = transMeta.nextUndo();
    if (transAction==null) {
      return;
    }
    redoTransformationAction( handler, transMeta, transAction );
    handler.updateGui();
  }

  public void redoTransformationAction( HopFileTypeHandlerInterface handler, TransMeta transMeta, TransAction transAction ) {
    switch ( transAction.getType() ) {
      case NewStep:
        // re-delete the step at correct location:
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          StepMeta stepMeta = (StepMeta) transAction.getCurrent()[ i ];
          int idx = transAction.getCurrentIndex()[ i ];
          transMeta.addStep( idx, stepMeta );
        }
        break;

      case NewNote:
        // re-insert the note at correct location:
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          NotePadMeta ni = (NotePadMeta) transAction.getCurrent()[ i ];
          int idx = transAction.getCurrentIndex()[ i ];
          transMeta.addNote( idx, ni );
        }
        break;

      case NewHop:
        // re-insert the hop at correct location:
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          TransHopMeta hi = (TransHopMeta) transAction.getCurrent()[ i ];
          int idx = transAction.getCurrentIndex()[ i ];
          transMeta.addTransHop( idx, hi );
        }
        break;

      //
      // DELETE
      //
      case DeleteStep:
        // re-remove the step at correct location:
        for ( int i = transAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = transAction.getCurrentIndex()[ i ];
          transMeta.removeStep( idx );
        }
        break;

      case DeleteNote:
        // re-remove the note at correct location:
        for ( int i = transAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = transAction.getCurrentIndex()[ i ];
          transMeta.removeNote( idx );
        }
        break;

      case DeleteHop:
        // re-remove the hop at correct location:
        for ( int i = transAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = transAction.getCurrentIndex()[ i ];
          transMeta.removeTransHop( idx );
        }
        break;

      //
      // CHANGE
      //

      // We changed a step : undo this...
      case ChangeStep:
        // Delete the current step, insert previous version.
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          StepMeta stepMeta = (StepMeta) ( (StepMeta) transAction.getCurrent()[ i ] ).clone();
          transMeta.getStep( transAction.getCurrentIndex()[ i ] ).replaceMeta( stepMeta );
        }
        break;

      // We changed a note : undo this...
      case ChangeNote:
        // Delete & re-insert
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          NotePadMeta ni = (NotePadMeta) transAction.getCurrent()[ i ];
          int idx = transAction.getCurrentIndex()[ i ];

          transMeta.removeNote( idx );
          transMeta.addNote( idx, (NotePadMeta) ni.clone() );
        }
        break;

      // We changed a hop : undo this...
      case ChangeHop:
        // Delete & re-insert
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          TransHopMeta hi = (TransHopMeta) transAction.getCurrent()[ i ];
          int idx = transAction.getCurrentIndex()[ i ];

          transMeta.removeTransHop( idx );
          transMeta.addTransHop( idx, (TransHopMeta) hi.clone() );
        }
        break;

      //
      // CHANGE POSITION
      //
      case PositionStep:
        for ( int i = 0; i < transAction.getCurrentIndex().length; i++ ) {
          // Find & change the location of the step:
          StepMeta stepMeta = transMeta.getStep( transAction.getCurrentIndex()[ i ] );
          stepMeta.setLocation( transAction.getCurrentLocation()[ i ] );
        }
        break;
      case PositionNote:
        for ( int i = 0; i < transAction.getCurrentIndex().length; i++ ) {
          int idx = transAction.getCurrentIndex()[ i ];
          NotePadMeta npi = transMeta.getNote( idx );
          Point curr = transAction.getCurrentLocation()[ i ];
          npi.setLocation( curr );
        }
        break;
      default:
        break;
    }

    // OK, now check if we need to do this again...
    if ( transMeta.viewNextUndo() != null ) {
      if ( transMeta.viewNextUndo().getNextAlso() ) {
        redoTransformationAction( handler, transMeta );
      }
    }
  }

  /**
   * Gets transGraph
   *
   * @return value of transGraph
   */
  public HopGuiTransGraph getTransGraph() {
    return transGraph;
  }

  /**
   * @param transGraph The transGraph to set
   */
  public void setTransGraph( HopGuiTransGraph transGraph ) {
    this.transGraph = transGraph;
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
