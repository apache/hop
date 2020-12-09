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

package org.apache.hop.workflow;

import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.AreaOwner.AreaType;
import org.apache.hop.core.gui.BasePainter;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.IGc.EColor;
import org.apache.hop.core.gui.IGc.EFont;
import org.apache.hop.core.gui.IGc.EImage;
import org.apache.hop.core.gui.IGc.ELineStyle;
import org.apache.hop.core.gui.IScrollBar;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.Rectangle;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.workflow.action.ActionMeta;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class WorkflowPainter extends BasePainter<WorkflowHopMeta, ActionMeta> {

  private WorkflowMeta workflowMeta;

  private ActionMeta startHopAction;
  private Point endHopLocation;
  private ActionMeta endHopAction;
  private ActionMeta noInputAction;
  private List<ActionMeta> activeActions;
  private List<ActionResult> actionResults;

  public WorkflowPainter( IGc gc, IVariables variables, WorkflowMeta workflowMeta, Point area, IScrollBar hori,
                          IScrollBar vert, WorkflowHopMeta candidate, Point drop_candidate, Rectangle selrect,
                          List<AreaOwner> areaOwners, int iconSize, int lineWidth, int gridSize,
                          String noteFontName, int noteFontHeight, double zoomFactor ) {
    super( gc, variables, workflowMeta, area, hori, vert, drop_candidate, selrect, areaOwners, iconSize, lineWidth, gridSize,
      noteFontName, noteFontHeight, zoomFactor );
    this.workflowMeta = workflowMeta;

    this.candidate = candidate;

  }

  public void drawWorkflow() throws HopException {

    Point max = workflowMeta.getMaximum();
    Point thumb = getThumb( area, max );
    if (offset==null) {
      offset = getOffset( thumb, area );
    }

    // Make sure the canvas is scaled 100%
    gc.setTransform( 0.0f, 0.0f, 1.0f );
    // First clear the image in the background color
    gc.setBackground( EColor.BACKGROUND );
    gc.fillRectangle( 0, 0, area.x, area.y );

    if ( hori != null ) {
      hori.setThumb( thumb.x );
    }
    if ( vert != null ) {
      vert.setThumb( thumb.y );
    }

    // Draw the pipeline onto the image
    //
    gc.setAlpha( 255 );
    gc.setTransform( translationX, translationY, magnification );

    drawActions();

    gc.dispose();

  }

  private void drawActions() throws HopException {
    if ( gridSize > 1 ) {
      drawGrid();
    }

    try {
      ExtensionPointHandler.callExtensionPoint( LogChannel.GENERAL, variables, HopExtensionPoint.WorkflowPainterStart.id, this );
    } catch ( HopException e ) {
      LogChannel.GENERAL.logError( "Error in JobPainterStart extension point", e );
    }

    // First draw the notes...
    gc.setFont( EFont.NOTE );

    for ( int i = 0; i < workflowMeta.nrNotes(); i++ ) {
      NotePadMeta ni = workflowMeta.getNote( i );
      drawNote( ni );
    }

    gc.setFont( EFont.GRAPH );

    // ... and then the rest on top of it...
    for ( int i = 0; i < workflowMeta.nrWorkflowHops(); i++ ) {
      WorkflowHopMeta hi = workflowMeta.getWorkflowHop( i );
      drawWorkflowHop( hi, false );
    }

    EImage arrow;
    if ( candidate != null ) {
      drawWorkflowHop( candidate, true );
    } else {
      if ( startHopAction != null && endHopLocation != null ) {
        Point fr = startHopAction.getLocation();
        Point to = endHopLocation;
        if ( endHopAction == null ) {
          gc.setForeground( EColor.GRAY );
          arrow = EImage.ARROW_DISABLED;
        } else {
          gc.setForeground( EColor.BLUE );
          arrow = EImage.ARROW_DEFAULT;
        }
        Point start = real2screen( fr.x + iconSize / 2, fr.y + iconSize / 2 );
        Point end = real2screen( to.x, to.y );
        drawArrow( arrow, start.x, start.y, end.x, end.y, theta, calcArrowLength(), 1.2, null, startHopAction,
        	endHopAction == null ? endHopLocation : endHopAction );
      } else if ( endHopAction != null && endHopLocation != null ) {
        Point fr = endHopLocation;
        Point to = endHopAction.getLocation();
        if ( startHopAction == null ) {
          gc.setForeground( EColor.GRAY );
          arrow = EImage.ARROW_DISABLED;
        } else {
          gc.setForeground( EColor.BLUE );
          arrow = EImage.ARROW_DEFAULT;
        }
        Point start = real2screen( fr.x, fr.y );
        Point end = real2screen( to.x + iconSize / 2, to.y + iconSize / 2 );
        drawArrow( arrow, start.x, start.y, end.x, end.y + iconSize / 2, theta, calcArrowLength(), 1.2, null,
          startHopAction == null ? endHopLocation : startHopAction, endHopAction );
      }
    }

    for ( int j = 0; j < workflowMeta.nrActions(); j++ ) {
      ActionMeta je = workflowMeta.getAction( j );
      drawAction( je );
    }

    // Display an icon on the indicated location signaling to the user that the transform in question does not accept input
    //
    if ( noInputAction != null ) {
      gc.setLineWidth( 2 );
      gc.setForeground( EColor.RED );
      Point n = noInputAction.getLocation();
      gc.drawLine( offset.x + n.x - 5, offset.y + n.y - 5, offset.x + n.x + iconSize + 5, offset.y
        + n.y + iconSize + 5 );
      gc.drawLine( offset.x + n.x - 5, offset.y + n.y + iconSize + 5, offset.x + n.x + iconSize + 5, offset.y
        + n.y - 5 );
    }

    if ( dropCandidate != null ) {
      gc.setLineStyle( ELineStyle.SOLID );
      gc.setForeground( EColor.BLACK );
      Point screen = real2screen( dropCandidate.x, dropCandidate.y );
      gc.drawRectangle( screen.x, screen.y, iconSize, iconSize );
    }

    try {
      ExtensionPointHandler.callExtensionPoint( LogChannel.GENERAL, variables, HopExtensionPoint.WorkflowPainterEnd.id, this );
    } catch ( HopException e ) {
      LogChannel.GENERAL.logError( "Error in JobPainterEnd extension point", e );
    }

    drawRect( selectionRectangle );
  }

  protected void drawAction( ActionMeta actionMeta ) throws HopException {
    int alpha = gc.getAlpha();

    Point pt = actionMeta.getLocation();
    if ( pt == null ) {
      pt = new Point( 50, 50 );
    }

    Point screen = real2screen( pt.x, pt.y );
    int x = screen.x;
    int y = screen.y;

    String name = actionMeta.getName();
    if ( actionMeta.isSelected() ) {
      gc.setLineWidth( 3 );
    } else {
      gc.setLineWidth( 1 );
    }

    gc.setBackground( EColor.BACKGROUND );
    gc.fillRoundRectangle( x - 1, y - 1, iconSize + 1, iconSize + 1, 7, 7 );
    gc.drawActionIcon( x, y, actionMeta, magnification );

    areaOwners.add( new AreaOwner( AreaType.ACTION_ICON, x, y, iconSize, iconSize, offset, subject, actionMeta ) );

    if ( actionMeta.isMissing() ) {
      gc.setForeground( EColor.RED );
    } else if ( actionMeta.isDeprecated() ) {
      gc.setForeground( EColor.DEPRECATED );
    } else {
      gc.setForeground( EColor.CRYSTAL );
    }
    gc.drawRoundRectangle( x - 1, y - 1, iconSize + 1, iconSize + 1, 7, 7 );
    gc.setForeground( EColor.CRYSTAL );
    Point textsize = new Point( gc.textExtent( "" + name ).x, gc.textExtent( "" + name ).y );

    gc.setBackground( EColor.BACKGROUND );
    gc.setLineWidth( 1 );

    int xpos = x + ( iconSize / 2 ) - ( textsize.x / 2 );
    int ypos = y + iconSize + 5;

    gc.setForeground( EColor.BLACK );
    gc.drawText( name, xpos, ypos, true );

    if ( activeActions != null && activeActions.contains( actionMeta ) ) {
      gc.setForeground( EColor.BLUE );
      int iconX = ( x + iconSize ) - ( miniIconSize / 2 );
      int iconY = y - ( miniIconSize / 2 );
      gc.drawImage( EImage.BUSY, iconX, iconY, magnification );
      areaOwners.add( new AreaOwner( AreaType.ACTION_BUSY, iconX, iconY, miniIconSize, miniIconSize, offset, subject, actionMeta ) );
    } else {
      gc.setForeground( EColor.BLACK );
    }

    ActionResult actionResult = findJobEntryResult( actionMeta );
    if ( actionResult != null ) {
      Result result = actionResult.getResult();
      int iconX = ( x + iconSize ) - ( miniIconSize / 2 );
      int iconY = y - ( miniIconSize / 2 );

      // Draw an execution result on the top right corner...
      //
      if ( actionResult.isCheckpoint() ) {
        gc.drawImage( EImage.CHECKPOINT, iconX, iconY, magnification );
        areaOwners.add( new AreaOwner( AreaType.ACTION_RESULT_CHECKPOINT, iconX, iconY, miniIconSize,
          miniIconSize, offset, actionMeta, actionResult ) );
      } else {
        if ( result.getResult() ) {
          gc.drawImage( EImage.TRUE, iconX, iconY, magnification );
          areaOwners.add( new AreaOwner( AreaType.ACTION_RESULT_SUCCESS, iconX, iconY, miniIconSize,
            miniIconSize, offset, actionMeta, actionResult ) );
        } else {
          gc.drawImage( EImage.FALSE, iconX, iconY, magnification );
          areaOwners.add( new AreaOwner( AreaType.ACTION_RESULT_FAILURE, iconX, iconY, miniIconSize,
            miniIconSize, offset, actionMeta, actionResult ) );
        }
      }
    }

    WorkflowPainterExtension extension = new WorkflowPainterExtension( gc, areaOwners, workflowMeta, null, actionMeta, x, y, 0, 0, 0, 0, offset, iconSize );
    try {
      ExtensionPointHandler.callExtensionPoint( LogChannel.GENERAL, variables, HopExtensionPoint.WorkflowPainterAction.id, extension );
    } catch ( Exception e ) {
      LogChannel.GENERAL.logError( "Error calling extension point(s) for the workflow painter action", e );
    }

    // Restore the previous alpha value
    //
    gc.setAlpha( alpha );
  }

  private ActionResult findJobEntryResult( ActionMeta actionCopy ) {
    if ( actionResults == null ) {
      return null;
    }

    Iterator<ActionResult> iterator = actionResults.iterator();
    while ( iterator.hasNext() ) {
      ActionResult actionResult = iterator.next();

      if ( actionResult.getActionName().equals( actionCopy.getName() )
        && actionResult.getActionNr() == actionCopy.getNr() ) {
        return actionResult;
      }
    }

    return null;
  }

  protected void drawWorkflowHop( WorkflowHopMeta hop, boolean candidate ) throws HopException {
    if ( hop == null || hop.getFromAction() == null || hop.getToAction() == null ) {
      return;
    }

    drawLine( hop, candidate );
  }

  /**
   * Calculates line coordinates from center to center.
   */
  protected void drawLine( WorkflowHopMeta jobHop, boolean is_candidate ) throws HopException {
    int[] line = getLine( jobHop.getFromAction(), jobHop.getToAction() );

    gc.setLineWidth( lineWidth );
    EColor col;

    if ( jobHop.getFromAction().isLaunchingInParallel() ) {
      gc.setLineStyle( ELineStyle.PARALLEL );
    } else {
      gc.setLineStyle( ELineStyle.SOLID );
    }

    EImage arrow;
    if ( is_candidate ) {
      col = EColor.BLUE;
      arrow = EImage.ARROW_CANDIDATE;
    } else if ( jobHop.isEnabled() ) {
      if ( jobHop.isUnconditional() ) {
        col = EColor.HOP_DEFAULT;
        arrow = EImage.ARROW_DEFAULT;
      } else {
        if ( jobHop.getEvaluation() ) {
          col = EColor.HOP_OK;
          arrow = EImage.ARROW_OK;
        } else {
          col = EColor.RED;
          arrow = EImage.ARROW_ERROR;
          gc.setLineStyle( ELineStyle.DASH );
        }
      }
    } else {
      col = EColor.GRAY;
      arrow = EImage.ARROW_DISABLED;
    }

    gc.setForeground( col );

    if ( jobHop.isSplit() ) {
      gc.setLineWidth( lineWidth + 2 );
    }
    drawArrow( arrow, line, jobHop );
    if ( jobHop.isSplit() ) {
      gc.setLineWidth( lineWidth );
    }

    gc.setForeground( EColor.BLACK );
    gc.setBackground( EColor.BACKGROUND );
    gc.setLineStyle( ELineStyle.SOLID );
  }

  private void drawArrow( EImage arrow, int[] line, WorkflowHopMeta jobHop ) throws HopException {
    drawArrow( arrow, line, jobHop, jobHop.getFromAction(), jobHop.getToAction() );
  }

  @Override
  protected void drawArrow( EImage arrow, int x1, int y1, int x2, int y2, double theta, int size, double factor,
                            WorkflowHopMeta jobHop, Object startObject, Object endObject ) throws HopException {
    int mx, my;
    int a, b, dist;
    double angle;

    // gc.setLineWidth(1);
    // WuLine(gc, black, x1, y1, x2, y2);

    gc.drawLine( x1, y1, x2, y2 );

    // What's the distance between the 2 points?
    a = Math.abs( x2 - x1 );
    b = Math.abs( y2 - y1 );
    dist = (int) Math.sqrt( a * a + b * b );

    // determine factor (position of arrow to left side or right side
    // 0-->100%)
    if ( factor < 0 ) {
      if ( dist >= 2 * iconSize ) {
        factor = 1.3;
      } else {
        factor = 1.2;
      }
    }

    // in between 2 points
    mx = (int) ( x1 + factor * ( x2 - x1 ) / 2 );
    my = (int) ( y1 + factor * ( y2 - y1 ) / 2 );

    // calculate points for arrowhead
    angle = Math.atan2( y2 - y1, x2 - x1 ) + ( Math.PI / 2 );

    boolean q1 = Math.toDegrees( angle ) >= 0 && Math.toDegrees( angle ) <= 90;
    boolean q2 = Math.toDegrees( angle ) > 90 && Math.toDegrees( angle ) <= 180;
    boolean q3 = Math.toDegrees( angle ) > 180 && Math.toDegrees( angle ) <= 270;
    boolean q4 = Math.toDegrees( angle ) > 270 || Math.toDegrees( angle ) < 0;

    if ( q1 || q3 ) {
      gc.drawImage( arrow, mx + 1, my, magnification, angle );
    } else if ( q2 || q4 ) {
      gc.drawImage( arrow, mx, my, magnification, angle );
    }
    // Display an icon above the hop...
    //
    factor = 0.8;

    // in between 2 points
    mx = (int) ( x1 + factor * ( x2 - x1 ) / 2 ) - 8;
    my = (int) ( y1 + factor * ( y2 - y1 ) / 2 ) - 8;

    if ( jobHop != null ) {
      EImage hopsIcon;
      if ( jobHop.isUnconditional() ) {
        hopsIcon = EImage.UNCONDITIONAL;
      } else {
        if ( jobHop.getEvaluation() ) {
          hopsIcon = EImage.TRUE;
        } else {
          hopsIcon = EImage.FALSE;
        }
      }

      gc.drawImage( hopsIcon, mx, my, magnification );
      areaOwners.add( new AreaOwner( AreaType.WORKFLOW_HOP_ICON, mx, my, miniIconSize, miniIconSize, offset, subject, jobHop ) );

      if ( jobHop.getFromAction().isLaunchingInParallel() ) {

        factor = 1;

        // in between 2 points
        mx = (int) ( x1 + factor * ( x2 - x1 ) / 2 ) - 8;
        my = (int) ( y1 + factor * ( y2 - y1 ) / 2 ) - 8;

        hopsIcon = EImage.PARALLEL;
        gc.drawImage( hopsIcon, mx, my, magnification );
        areaOwners.add( new AreaOwner( AreaType.WORKFLOW_HOP_PARALLEL_ICON, mx, my, miniIconSize, miniIconSize, offset, subject, jobHop ) );
      }

      WorkflowPainterExtension extension = new WorkflowPainterExtension( gc, areaOwners, workflowMeta, jobHop, null, x1, y1, x2, y2, mx, my, offset, iconSize );
      try {
        ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL, variables, HopExtensionPoint.WorkflowPainterArrow.id, extension );
      } catch ( Exception e ) {
        LogChannel.GENERAL.logError( "Error calling extension point(s) for the workflow painter arrow", e );
      }
    }
  }

  public void setStartHopAction( ActionMeta action ) {
    this.startHopAction = action;
  }

  public void setEndHopLocation( Point location ) {
    this.endHopLocation = location;
  }

  public void setEndHopAction( ActionMeta action ) {
    this.endHopAction = action;
  }

  public void setNoInputAction( ActionMeta action ) {
    this.noInputAction = action;
  }

  public void setActiveActions( List<ActionMeta> activeActions ) {
    this.activeActions = activeActions;
  }

  /**
   * @return the actionResults
   */
  public List<ActionResult> getActionResults() {
    return actionResults;
  }

  /**
   * @param actionResults Sets AND sorts the action results by name and number
   */
  public void setActionResults( List<ActionResult> actionResults ) {
    this.actionResults = actionResults;
    Collections.sort( this.actionResults );
  }

  public WorkflowMeta getWorkflowMeta() {
    return workflowMeta;
  }

  public void setWorkflowMeta( WorkflowMeta workflowMeta ) {
    this.workflowMeta = workflowMeta;
  }

  public ActionMeta getStartHopAction() {
    return startHopAction;
  }

  public Point getEndHopLocation() {
    return endHopLocation;
  }

  public ActionMeta getEndHopAction() {
    return endHopAction;
  }

  public ActionMeta getNoInputAction() {
    return noInputAction;
  }

  public List<ActionMeta> getActiveActions() {
    return activeActions;
  }

}
