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

package org.apache.hop.core.undo;

import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.gui.Point;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobHopMeta;
import org.apache.hop.job.entry.JobEntryCopy;
import org.apache.hop.trans.TransHopMeta;
import org.apache.hop.trans.step.StepMeta;

/**
 * This class store undo and redo information...
 * <p>
 * Possible changes to a transformation:
 * <p>
 * step
 * <p>
 * hop
 * <p>
 * note
 * <p>
 * connection
 * <p>
 * <p>
 * Build an Undo/Redo class containing:
 * <p>
 * Type of change
 * <p>
 * Description of action
 * <p>
 * Link to previous infomation
 * <p>
 *
 * @author Matt
 * @since 19-12-2003
 */
public class TransAction {
  private static Class<?> PKG = TransAction.class; // for i18n purposes, needed by Translator2!!

  public enum ActionType {

    None( "" ),

    ChangeStep( BaseMessages.getString( PKG, "TransAction.label.ChangeStep" ) ),
    ChangeHop( BaseMessages.getString( PKG, "TransAction.label.ChangeHop" ) ),
    ChangeNote( BaseMessages.getString( PKG, "TransAction.label.ChangeNote" ) ),

    NewStep( BaseMessages.getString( PKG, "TransAction.label.NewStep" ) ),
    NewHop( BaseMessages.getString( PKG, "TransAction.label.NewHop" ) ),
    NewNote( BaseMessages.getString( PKG, "TransAction.label.NewNote" ) ),
    DeleteStep( BaseMessages.getString( PKG, "TransAction.label.DeleteStep" ) ),

    DeleteHop( BaseMessages.getString( PKG, "TransAction.label.DeleteHop" ) ),
    DeleteNote( BaseMessages.getString( PKG, "TransAction.label.DeleteNote" ) ),

    PositionStep( BaseMessages.getString( PKG, "TransAction.label.PositionStep" ) ),
    PositionNote( BaseMessages.getString( PKG, "TransAction.label.PositionNote" ) ),

    ChangeJobEntry( BaseMessages.getString( PKG, "TransAction.label.ChangeJobEntry" ) ),
    ChangeJobHop( BaseMessages.getString( PKG, "TransAction.label.ChangeJobHop" ) ),

    NewJobEntry( BaseMessages.getString( PKG, "TransAction.label.NewJobEntry" ) ),
    NewJobHop( BaseMessages.getString( PKG, "TransAction.label.NewJobHop" ) ),

    DeleteJobEntry( BaseMessages.getString( PKG, "TransAction.label.DeleteJobEntry" ) ),
    DeleteJobHop( BaseMessages.getString( PKG, "TransAction.label.DeleteJobHop" ) ),

    PositionJobEntry( BaseMessages.getString( PKG, "TransAction.label.PositionJobEntry" ) ),

    ChangeTableRow( BaseMessages.getString( PKG, "TransAction.label.ChangeTableRow" ) ),
    NewTableRow( BaseMessages.getString( PKG, "TransAction.label.NewTableRow" ) ),
    DeleteTableRow( BaseMessages.getString( PKG, "TransAction.label.DeleteTableRow" ) ),
    PositionTableRow( BaseMessages.getString( PKG, "TransAction.label.PositionTableRow" ) ),
    ;

    private String description;

    private ActionType( String description ) {
      this.description = description;
    }

    /**
     * Gets description
     *
     * @return value of description
     */
    public String getDescription() {
      return description;
    }
  }


  private ActionType type;
  private Object[] previous;
  private Point[] previous_location;
  private int[] previous_index;

  private Object[] current;
  private Point[] current_location;
  private int[] current_index;

  private boolean nextAlso;

  public TransAction() {
    type = ActionType.None;
  }

  public void setDelete( Object[] prev, int[] idx ) {
    current = prev;
    current_index = idx;

    if ( prev[ 0 ] instanceof StepMeta ) {
      type = ActionType.DeleteStep;
    }
    if ( prev[ 0 ] instanceof TransHopMeta ) {
      type = ActionType.DeleteHop;
    }
    if ( prev[ 0 ] instanceof NotePadMeta ) {
      type = ActionType.DeleteNote;
    }
    if ( prev[ 0 ] instanceof JobEntryCopy ) {
      type = ActionType.DeleteJobEntry;
    }
    if ( prev[ 0 ] instanceof JobHopMeta ) {
      type = ActionType.DeleteJobHop;
    }
    if ( prev[ 0 ] instanceof String[] ) {
      type = ActionType.DeleteTableRow;
    }
  }

  public void setChanged( Object[] prev, Object[] curr, int[] idx ) {
    previous = prev;
    current = curr;
    current_index = idx;
    previous_index = idx;

    if ( prev[ 0 ] instanceof StepMeta ) {
      type = ActionType.ChangeStep;
    }
    if ( prev[ 0 ] instanceof TransHopMeta ) {
      type = ActionType.ChangeHop;
    }
    if ( prev[ 0 ] instanceof NotePadMeta ) {
      type = ActionType.ChangeNote;
    }
    if ( prev[ 0 ] instanceof JobEntryCopy ) {
      type = ActionType.ChangeJobEntry;
    }
    if ( prev[ 0 ] instanceof JobHopMeta ) {
      type = ActionType.ChangeJobHop;
    }
    if ( prev[ 0 ] instanceof String[] ) {
      type = ActionType.ChangeTableRow;
    }
  }

  public void setNew( Object[] prev, int[] position ) {
    if ( prev.length == 0 ) {
      return;
    }

    current = prev;
    current_index = position;
    previous = null;

    if ( prev[ 0 ] instanceof StepMeta ) {
      type = ActionType.NewStep;
    }
    if ( prev[ 0 ] instanceof TransHopMeta ) {
      type = ActionType.NewHop;
    }
    if ( prev[ 0 ] instanceof NotePadMeta ) {
      type = ActionType.NewNote;
    }
    if ( prev[ 0 ] instanceof JobEntryCopy ) {
      type = ActionType.NewJobEntry;
    }
    if ( prev[ 0 ] instanceof JobHopMeta ) {
      type = ActionType.NewJobHop;
    }
    if ( prev[ 0 ] instanceof String[] ) {
      type = ActionType.NewTableRow;
    }
  }

  public void setPosition( Object[] obj, int[] idx, Point[] prev, Point[] curr ) {
    if ( prev.length != curr.length ) {
      return;
    }

    previous_location = new Point[ prev.length ];
    current_location = new Point[ curr.length ];
    current = obj;
    current_index = idx;

    for ( int i = 0; i < prev.length; i++ ) {
      previous_location[ i ] = new Point( prev[ i ].x, prev[ i ].y );
      current_location[ i ] = new Point( curr[ i ].x, curr[ i ].y );
    }

    Object fobj = obj[ 0 ];
    if ( fobj instanceof StepMeta ) {
      type = ActionType.PositionStep;
    }
    if ( fobj instanceof NotePadMeta ) {
      type = ActionType.PositionNote;
    }
    if ( fobj instanceof JobEntryCopy ) {
      type = ActionType.PositionJobEntry;
    }
  }

  public void setItemMove( int[] prev, int[] curr ) {
    previous_location = null;
    current_location = null;
    current = null;
    current_index = curr;
    previous = null;
    previous_index = prev;

    type = ActionType.PositionTableRow;
  }

  public ActionType getType() {
    return type;
  }

  public Object[] getPrevious() {
    return previous;
  }

  public Object[] getCurrent() {
    return current;
  }

  public Point[] getPreviousLocation() {
    return previous_location;
  }

  public Point[] getCurrentLocation() {
    return current_location;
  }

  public int[] getPreviousIndex() {
    return previous_index;
  }

  public int[] getCurrentIndex() {
    return current_index;
  }

  /**
   * Indicate that the next operations needs to be undone too.
   *
   * @param nextAlso The nextAlso to set.
   */
  public void setNextAlso( boolean nextAlso ) {
    this.nextAlso = nextAlso;
  }

  /**
   * Get the status of the nextAlso flag.
   *
   * @return true if the next operation needs to be done too.
   */
  public boolean getNextAlso() {
    return nextAlso;
  }

  public String toString() {
    String retval = "";
    if ( type == null ) {
      return TransAction.class.getName();
    }

    retval = type.getDescription();

    if ( current != null && current.length > 1 ) {
      retval += " (x" + current.length + ")";
    }

    return retval;
  }
}
