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

package org.apache.hop.ui.hopgui.delegates;

import org.apache.hop.core.IAddUndoPosition;
import org.apache.hop.core.gui.IUndo;
import org.apache.hop.core.gui.Point;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.hopgui.HopGui;

public class HopGuiUndoDelegate implements IAddUndoPosition {
  private HopGui hopUi;

  public HopGuiUndoDelegate( HopGui hopGui ) {
    this.hopUi = hopGui;
  }

  public void addUndoNew( IUndo undoInterface, Object[] obj, int[] position ) {
    addUndoNew( undoInterface, obj, position, false );
  }

  public void addUndoNew( IUndo undoInterface, Object[] obj, int[] position, boolean nextAlso ) {
    undoInterface.addUndo( obj, null, position, null, null, PipelineMeta.TYPE_UNDO_NEW, nextAlso );
    hopUi.setUndoMenu( undoInterface );
  }

  // Undo delete object
  public void addUndoDelete( IUndo undoInterface, Object[] obj, int[] position ) {
    addUndoDelete( undoInterface, obj, position, false );
  }

  // Undo delete object
  public void addUndoDelete( IUndo undoInterface, Object[] obj, int[] position, boolean nextAlso ) {
    undoInterface.addUndo( obj, null, position, null, null, PipelineMeta.TYPE_UNDO_DELETE, nextAlso );
    hopUi.setUndoMenu( undoInterface );
  }

  // Change of transform, connection, hop or note...
  public void addUndoPosition( IUndo undoInterface, Object[] obj, int[] pos, Point[] prev, Point[] curr ) {
    addUndoPosition( undoInterface, obj, pos, prev, curr, false );
  }

  // Change of transform, connection, hop or note...
  public void addUndoPosition( IUndo undoInterface, Object[] obj, int[] pos, Point[] prev, Point[] curr, boolean nextAlso ) {
    // It's better to store the indexes of the objects, not the objects
    // itself!
    undoInterface.addUndo( obj, null, pos, prev, curr, WorkflowMeta.TYPE_UNDO_POSITION, false );
    hopUi.setUndoMenu( undoInterface );
  }

  // Change of transform, connection, hop or note...
  public void addUndoChange( IUndo undoInterface, Object[] from, Object[] to, int[] pos ) {
    addUndoChange( undoInterface, from, to, pos, false );
  }

  // Change of transform, connection, hop or note...
  public void addUndoChange( IUndo undoInterface, Object[] from, Object[] to, int[] pos, boolean nextAlso ) {
    undoInterface.addUndo( from, to, pos, null, null, WorkflowMeta.TYPE_UNDO_CHANGE, nextAlso );
    hopUi.setUndoMenu( undoInterface );
  }

  /**
   * Gets hopGui
   *
   * @return value of hopGui
   */
  public HopGui getHopUi() {
    return hopUi;
  }

  /**
   * @param hopUi The hopGui to set
   */
  public void setHopUi( HopGui hopUi ) {
    this.hopUi = hopUi;
  }
}
