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

import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.undo.ChangeAction;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;

public class HopGuiPipelineUndoDelegate {
  private static final Class<?> PKG = HopGui.class; // For Translator

  private HopGuiPipelineGraph pipelineGraph;
  private HopGui hopGui;

  /**
   * @param hopGui
   */
  public HopGuiPipelineUndoDelegate( HopGui hopGui, HopGuiPipelineGraph pipelineGraph ) {
    this.hopGui = hopGui;
    this.pipelineGraph = pipelineGraph;
  }

  public void undoPipelineAction( IHopFileTypeHandler handler, PipelineMeta pipelineMeta ) {
    ChangeAction changeAction = pipelineMeta.previousUndo();
    if ( changeAction == null ) {
      return;
    }
    undoPipelineAction( handler, pipelineMeta, changeAction );
    handler.updateGui();
  }


  public void undoPipelineAction( IHopFileTypeHandler handler, PipelineMeta pipelineMeta, ChangeAction changeAction ) {
    switch ( changeAction.getType() ) {
      // We created a new transform : undo this...
      case NewTransform:
        // Delete the transform at correct location:
        for ( int i = changeAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = changeAction.getCurrentIndex()[ i ];
          pipelineMeta.removeTransform( idx );
        }
        break;

      // We created a new note : undo this...
      case NewNote:
        // Delete the note at correct location:
        for ( int i = changeAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = changeAction.getCurrentIndex()[ i ];
          pipelineMeta.removeNote( idx );
        }
        break;

      // We created a new hop : undo this...
      case NewHop:
        // Delete the hop at correct location:
        for ( int i = changeAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = changeAction.getCurrentIndex()[ i ];
          pipelineMeta.removePipelineHop( idx );
        }
        break;

      //
      // DELETE
      //

      // We delete a transform : undo this...
      case DeleteTransform:
        // un-Delete the transform at correct location: re-insert
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          TransformMeta transformMeta = (TransformMeta) changeAction.getCurrent()[ i ];
          int idx = changeAction.getCurrentIndex()[ i ];
          pipelineMeta.addTransform( idx, transformMeta );
        }
        break;

      // We delete new note : undo this...
      case DeleteNote:
        // re-insert the note at correct location:
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          NotePadMeta ni = (NotePadMeta) changeAction.getCurrent()[ i ];
          int idx = changeAction.getCurrentIndex()[ i ];
          pipelineMeta.addNote( idx, ni );
        }
        break;

      // We deleted a hop : undo this...
      case DeleteHop:
        // re-insert the hop at correct location:
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          PipelineHopMeta hi = (PipelineHopMeta) changeAction.getCurrent()[ i ];
          int idx = changeAction.getCurrentIndex()[ i ];
          // Build a new hop:
          TransformMeta from = pipelineMeta.findTransform( hi.getFromTransform().getName() );
          TransformMeta to = pipelineMeta.findTransform( hi.getToTransform().getName() );
          PipelineHopMeta hinew = new PipelineHopMeta( from, to );
          pipelineMeta.addPipelineHop( idx, hinew );
        }
        break;

      //
      // CHANGE
      //

      // We changed a transform : undo this...
      case ChangeTransform:
        // Delete the current transform, insert previous version.
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          TransformMeta prev = (TransformMeta) ( (TransformMeta) changeAction.getPrevious()[ i ] ).clone();
          int idx = changeAction.getCurrentIndex()[ i ];

          pipelineMeta.getTransform( idx ).replaceMeta( prev );
        }
        break;

      // We changed a note : undo this...
      case ChangeNote:
        // Delete & re-insert
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          int idx = changeAction.getCurrentIndex()[ i ];
          pipelineMeta.removeNote( idx );
          NotePadMeta prev = (NotePadMeta) changeAction.getPrevious()[ i ];
          pipelineMeta.addNote( idx, (NotePadMeta) prev.clone() );
        }
        break;

      // We changed a hop : undo this...
      case ChangeHop:
        // Delete & re-insert
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          PipelineHopMeta prev = (PipelineHopMeta) changeAction.getPrevious()[ i ];
          int idx = changeAction.getCurrentIndex()[ i ];

          pipelineMeta.removePipelineHop( idx );
          pipelineMeta.addPipelineHop( idx, (PipelineHopMeta) prev.clone() );
        }
        break;

      //
      // POSITION
      //

      // The position of a transform has changed: undo this...
      case PositionTransform:
        // Find the location of the transform:
        for ( int i = 0; i < changeAction.getCurrentIndex().length; i++ ) {
          TransformMeta transformMeta = pipelineMeta.getTransform( changeAction.getCurrentIndex()[ i ] );
          transformMeta.setLocation( changeAction.getPreviousLocation()[ i ] );
        }
        break;

      // The position of a note has changed: undo this...
      case PositionNote:
        for ( int i = 0; i < changeAction.getCurrentIndex().length; i++ ) {
          int idx = changeAction.getCurrentIndex()[ i ];
          NotePadMeta npi = pipelineMeta.getNote( idx );
          Point prev = changeAction.getPreviousLocation()[ i ];
          npi.setLocation( prev );
        }
        break;
      default:
        break;
    }

    // OK, now check if we need to do this again...
    if ( pipelineMeta.viewNextUndo() != null ) {
      if ( pipelineMeta.viewNextUndo().getNextAlso() ) {
        undoPipelineAction( handler, pipelineMeta );
      }
    }
  }

  public void redoPipelineAction( IHopFileTypeHandler handler, PipelineMeta pipelineMeta ) {
    ChangeAction changeAction = pipelineMeta.nextUndo();
    if ( changeAction == null ) {
      return;
    }
    redoPipelineAction( handler, pipelineMeta, changeAction );
    handler.updateGui();
  }

  public void redoPipelineAction( IHopFileTypeHandler handler, PipelineMeta pipelineMeta, ChangeAction changeAction ) {
    switch ( changeAction.getType() ) {
      case NewTransform:
        // re-delete the transform at correct location:
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          TransformMeta transformMeta = (TransformMeta) changeAction.getCurrent()[ i ];
          int idx = changeAction.getCurrentIndex()[ i ];
          pipelineMeta.addTransform( idx, transformMeta );
        }
        break;

      case NewNote:
        // re-insert the note at correct location:
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          NotePadMeta ni = (NotePadMeta) changeAction.getCurrent()[ i ];
          int idx = changeAction.getCurrentIndex()[ i ];
          pipelineMeta.addNote( idx, ni );
        }
        break;

      case NewHop:
        // re-insert the hop at correct location:
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          PipelineHopMeta hi = (PipelineHopMeta) changeAction.getCurrent()[ i ];
          int idx = changeAction.getCurrentIndex()[ i ];
          pipelineMeta.addPipelineHop( idx, hi );
        }
        break;

      //
      // DELETE
      //
      case DeleteTransform:
        // re-remove the transform at correct location:
        for ( int i = changeAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = changeAction.getCurrentIndex()[ i ];
          pipelineMeta.removeTransform( idx );
        }
        break;

      case DeleteNote:
        // re-remove the note at correct location:
        for ( int i = changeAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = changeAction.getCurrentIndex()[ i ];
          pipelineMeta.removeNote( idx );
        }
        break;

      case DeleteHop:
        // re-remove the hop at correct location:
        for ( int i = changeAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = changeAction.getCurrentIndex()[ i ];
          pipelineMeta.removePipelineHop( idx );
        }
        break;

      //
      // CHANGE
      //

      // We changed a transform : undo this...
      case ChangeTransform:
        // Delete the current transform, insert previous version.
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          TransformMeta transformMeta = (TransformMeta) ( (TransformMeta) changeAction.getCurrent()[ i ] ).clone();
          pipelineMeta.getTransform( changeAction.getCurrentIndex()[ i ] ).replaceMeta( transformMeta );
        }
        break;

      // We changed a note : undo this...
      case ChangeNote:
        // Delete & re-insert
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          NotePadMeta ni = (NotePadMeta) changeAction.getCurrent()[ i ];
          int idx = changeAction.getCurrentIndex()[ i ];

          pipelineMeta.removeNote( idx );
          pipelineMeta.addNote( idx, (NotePadMeta) ni.clone() );
        }
        break;

      // We changed a hop : undo this...
      case ChangeHop:
        // Delete & re-insert
        for ( int i = 0; i < changeAction.getCurrent().length; i++ ) {
          PipelineHopMeta hi = (PipelineHopMeta) changeAction.getCurrent()[ i ];
          int idx = changeAction.getCurrentIndex()[ i ];

          pipelineMeta.removePipelineHop( idx );
          pipelineMeta.addPipelineHop( idx, (PipelineHopMeta) hi.clone() );
        }
        break;

      //
      // CHANGE POSITION
      //
      case PositionTransform:
        for ( int i = 0; i < changeAction.getCurrentIndex().length; i++ ) {
          // Find & change the location of the transform:
          TransformMeta transformMeta = pipelineMeta.getTransform( changeAction.getCurrentIndex()[ i ] );
          transformMeta.setLocation( changeAction.getCurrentLocation()[ i ] );
        }
        break;
      case PositionNote:
        for ( int i = 0; i < changeAction.getCurrentIndex().length; i++ ) {
          int idx = changeAction.getCurrentIndex()[ i ];
          NotePadMeta npi = pipelineMeta.getNote( idx );
          Point curr = changeAction.getCurrentLocation()[ i ];
          npi.setLocation( curr );
        }
        break;
      default:
        break;
    }

    // OK, now check if we need to do this again...
    if ( pipelineMeta.viewNextUndo() != null ) {
      if ( pipelineMeta.viewNextUndo().getNextAlso() ) {
        redoPipelineAction( handler, pipelineMeta );
      }
    }
  }

  /**
   * Gets pipelineGraph
   *
   * @return value of pipelineGraph
   */
  public HopGuiPipelineGraph getPipelineGraph() {
    return pipelineGraph;
  }

  /**
   * @param pipelineGraph The pipelineGraph to set
   */
  public void setPipelineGraph( HopGuiPipelineGraph pipelineGraph ) {
    this.pipelineGraph = pipelineGraph;
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
