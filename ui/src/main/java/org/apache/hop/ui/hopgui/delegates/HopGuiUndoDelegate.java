package org.apache.hop.ui.hopgui.delegates;

import org.apache.hop.core.AddUndoPositionInterface;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.UndoInterface;
import org.apache.hop.job.JobMeta;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.ui.hopgui.HopGui;

public class HopGuiUndoDelegate implements AddUndoPositionInterface {
  private HopGui hopGui;

  public HopGuiUndoDelegate( HopGui hopGui ) {
    this.hopGui = hopGui;
  }

  public void addUndoNew( UndoInterface undoInterface, Object[] obj, int[] position ) {
    addUndoNew( undoInterface, obj, position, false );
  }

  public void addUndoNew( UndoInterface undoInterface, Object[] obj, int[] position, boolean nextAlso ) {
    undoInterface.addUndo( obj, null, position, null, null, TransMeta.TYPE_UNDO_NEW, nextAlso );
    hopGui.setUndoMenu( undoInterface );
  }

  // Undo delete object
  public void addUndoDelete( UndoInterface undoInterface, Object[] obj, int[] position ) {
    addUndoDelete( undoInterface, obj, position, false );
  }

  // Undo delete object
  public void addUndoDelete( UndoInterface undoInterface, Object[] obj, int[] position, boolean nextAlso ) {
    undoInterface.addUndo( obj, null, position, null, null, TransMeta.TYPE_UNDO_DELETE, nextAlso );
    hopGui.setUndoMenu( undoInterface );
  }

  // Change of step, connection, hop or note...
  public void addUndoPosition( UndoInterface undoInterface, Object[] obj, int[] pos, Point[] prev, Point[] curr ) {
    addUndoPosition( undoInterface, obj, pos, prev, curr, false );
  }

  // Change of step, connection, hop or note...
  public void addUndoPosition( UndoInterface undoInterface, Object[] obj, int[] pos, Point[] prev, Point[] curr, boolean nextAlso ) {
    // It's better to store the indexes of the objects, not the objects
    // itself!
    undoInterface.addUndo( obj, null, pos, prev, curr, JobMeta.TYPE_UNDO_POSITION, false );
    hopGui.setUndoMenu( undoInterface );
  }

  // Change of step, connection, hop or note...
  public void addUndoChange( UndoInterface undoInterface, Object[] from, Object[] to, int[] pos ) {
    addUndoChange( undoInterface, from, to, pos, false );
  }

  // Change of step, connection, hop or note...
  public void addUndoChange( UndoInterface undoInterface, Object[] from, Object[] to, int[] pos, boolean nextAlso ) {
    undoInterface.addUndo( from, to, pos, null, null, JobMeta.TYPE_UNDO_CHANGE, nextAlso );
    hopGui.setUndoMenu( undoInterface );
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
