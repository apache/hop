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

package org.apache.hop.core.undo;

import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.gui.Point;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * This class store undo and redo information...
 * <p>
 * Possible changes to a pipeline:
 * <p>
 * transform
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
public class ChangeAction {
  private static final Class<?> PKG = ChangeAction.class; // For Translator

  public enum ActionType {

    None( "" ),

    ChangeTransform( BaseMessages.getString( PKG, "TransAction.label.ChangeTransform" ) ),
    ChangeHop( BaseMessages.getString( PKG, "TransAction.label.ChangeHop" ) ),
    ChangeNote( BaseMessages.getString( PKG, "TransAction.label.ChangeNote" ) ),

    NewTransform( BaseMessages.getString( PKG, "TransAction.label.NewTransform" ) ),
    NewHop( BaseMessages.getString( PKG, "TransAction.label.NewHop" ) ),
    NewNote( BaseMessages.getString( PKG, "TransAction.label.NewNote" ) ),
    DeleteTransform( BaseMessages.getString( PKG, "TransAction.label.DeleteTransform" ) ),

    DeleteHop( BaseMessages.getString( PKG, "TransAction.label.DeleteHop" ) ),
    DeleteNote( BaseMessages.getString( PKG, "TransAction.label.DeleteNote" ) ),

    PositionTransform( BaseMessages.getString( PKG, "TransAction.label.PositionTransform" ) ),
    PositionNote( BaseMessages.getString( PKG, "TransAction.label.PositionNote" ) ),

    ChangeAction( BaseMessages.getString( PKG, "TransAction.label.ChangeAction" ) ),
    ChangeWorkflowHop( BaseMessages.getString( PKG, "TransAction.label.ChangeWorkflowHop" ) ),

    NewAction( BaseMessages.getString( PKG, "TransAction.label.NewAction" ) ),
    NewWorkflowHop( BaseMessages.getString( PKG, "TransAction.label.NewWorkflowHop" ) ),

    DeleteAction( BaseMessages.getString( PKG, "TransAction.label.DeleteAction" ) ),
    DeleteWorkflowHop( BaseMessages.getString( PKG, "TransAction.label.DeleteWorkflowHop" ) ),

    PositionAction( BaseMessages.getString( PKG, "TransAction.label.PositionAction" ) ),

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
  private int[] previousIndex;

  private Object[] current;
  private Point[] current_location;
  private int[] currentIndex;

  private boolean nextAlso;

  public ChangeAction() {
    type = ActionType.None;
  }

  public void setDelete( Object[] prev, int[] idx ) {
    current = prev;
    currentIndex = idx;

    if ( prev[ 0 ] instanceof TransformMeta ) {
      type = ActionType.DeleteTransform;
    }
    if ( prev[ 0 ] instanceof PipelineHopMeta ) {
      type = ActionType.DeleteHop;
    }
    if ( prev[ 0 ] instanceof NotePadMeta ) {
      type = ActionType.DeleteNote;
    }
    if ( prev[ 0 ] instanceof ActionMeta ) {
      type = ActionType.DeleteAction;
    }
    if ( prev[ 0 ] instanceof WorkflowHopMeta ) {
      type = ActionType.DeleteWorkflowHop;
    }
    if ( prev[ 0 ] instanceof String[] ) {
      type = ActionType.DeleteTableRow;
    }
  }

  public void setChanged( Object[] prev, Object[] curr, int[] idx ) {
    previous = prev;
    current = curr;
    currentIndex = idx;
    previousIndex = idx;

    if ( prev[ 0 ] instanceof TransformMeta ) {
      type = ActionType.ChangeTransform;
    }
    if ( prev[ 0 ] instanceof PipelineHopMeta ) {
      type = ActionType.ChangeHop;
    }
    if ( prev[ 0 ] instanceof NotePadMeta ) {
      type = ActionType.ChangeNote;
    }
    if ( prev[ 0 ] instanceof ActionMeta ) {
      type = ActionType.ChangeAction;
    }
    if ( prev[ 0 ] instanceof WorkflowHopMeta ) {
      type = ActionType.ChangeWorkflowHop;
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
    currentIndex = position;
    previous = null;

    if ( prev[ 0 ] instanceof TransformMeta ) {
      type = ActionType.NewTransform;
    }
    if ( prev[ 0 ] instanceof PipelineHopMeta ) {
      type = ActionType.NewHop;
    }
    if ( prev[ 0 ] instanceof NotePadMeta ) {
      type = ActionType.NewNote;
    }
    if ( prev[ 0 ] instanceof ActionMeta ) {
      type = ActionType.NewAction;
    }
    if ( prev[ 0 ] instanceof WorkflowHopMeta ) {
      type = ActionType.NewWorkflowHop;
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
    currentIndex = idx;

    for ( int i = 0; i < prev.length; i++ ) {
      previous_location[ i ] = new Point( prev[ i ].x, prev[ i ].y );
      current_location[ i ] = new Point( curr[ i ].x, curr[ i ].y );
    }

    Object fobj = obj[ 0 ];
    if ( fobj instanceof TransformMeta ) {
      type = ActionType.PositionTransform;
    }
    if ( fobj instanceof NotePadMeta ) {
      type = ActionType.PositionNote;
    }
    if ( fobj instanceof ActionMeta ) {
      type = ActionType.PositionAction;
    }
  }

  public void setItemMove( int[] prev, int[] curr ) {
    previous_location = null;
    current_location = null;
    current = null;
    currentIndex = curr;
    previous = null;
    previousIndex = prev;

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
    return previousIndex;
  }

  public int[] getCurrentIndex() {
    return currentIndex;
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
      return ChangeAction.class.getName();
    }

    retval = type.getDescription();

    if ( current != null && current.length > 1 ) {
      retval += " (x" + current.length + ")";
    }

    return retval;
  }
}
