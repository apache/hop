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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.AreaOwner.AreaType;
import org.apache.hop.core.gui.BasePainter;
import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.IGc.EColor;
import org.apache.hop.core.gui.IGc.EFont;
import org.apache.hop.core.gui.IGc.EImage;
import org.apache.hop.core.gui.IGc.ELineStyle;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.Rectangle;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.workflow.action.ActionMeta;

public class WorkflowPainter extends BasePainter<WorkflowHopMeta, ActionMeta> {

  private WorkflowMeta workflowMeta;

  private ActionMeta startHopAction;
  private Point endHopLocation;
  private ActionMeta endHopAction;
  private ActionMeta noInputAction;
  private List<ActionMeta> activeActions;
  private List<ActionResult> actionResults;

  public WorkflowPainter(
      IGc gc,
      IVariables variables,
      WorkflowMeta workflowMeta,
      Point area,
      DPoint offset,
      WorkflowHopMeta candidate,
      Rectangle selrect,
      List<AreaOwner> areaOwners,
      int iconSize,
      int lineWidth,
      int gridSize,
      String noteFontName,
      int noteFontHeight,
      double zoomFactor,
      boolean drawingBorderAroundName,
      String mouseOverName) {
    super(
        gc,
        variables,
        workflowMeta,
        area,
        offset,
        selrect,
        areaOwners,
        iconSize,
        lineWidth,
        gridSize,
        noteFontName,
        noteFontHeight,
        zoomFactor,
        drawingBorderAroundName,
        mouseOverName);
    this.workflowMeta = workflowMeta;

    this.candidate = candidate;
  }

  public void drawWorkflow() throws HopException {
    // Make sure the canvas is scaled 100%
    gc.setTransform(0.0f, 0.0f, 1.0f);
    // First clear the image in the background color
    gc.setBackground(EColor.BACKGROUND);
    gc.fillRectangle(0, 0, area.x, area.y);

    // Draw the pipeline onto the image
    //
    gc.setAlpha(255);
    gc.setTransform((float) offset.x, (float) offset.y, magnification);
    drawActions();

    // Draw the navigation view in native pixels to make calculation a bit easier.
    //
    gc.setTransform(0.0f, 0.0f, 1.0f);
    drawNavigationView();

    gc.dispose();
  }

  private void drawActions() throws HopException {
    if (gridSize > 1) {
      drawGrid();
    }

    try {
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL, variables, HopExtensionPoint.WorkflowPainterStart.id, this);
    } catch (HopException e) {
      LogChannel.GENERAL.logError("Error in WorkflowPainterStart extension point", e);
    }

    // First draw the notes...
    gc.setFont(EFont.NOTE);
    for (NotePadMeta notePad : workflowMeta.getNotes()) {
      drawNote(notePad);
    }

    // Second draw the hops on top of it...
    gc.setFont(EFont.GRAPH);
    for (WorkflowHopMeta hop : workflowMeta.getWorkflowHops()) {
      drawWorkflowHop(hop, false);
    }

    EImage arrow;
    if (candidate != null) {
      drawWorkflowHop(candidate, true);
    } else {
      if (startHopAction != null && endHopLocation != null) {
        Point fr = startHopAction.getLocation();
        Point to = endHopLocation;
        if (endHopAction == null) {
          gc.setForeground(EColor.GRAY);
          arrow = EImage.ARROW_DISABLED;
        } else {
          gc.setForeground(EColor.BLUE);
          arrow = EImage.ARROW_DEFAULT;
        }
        Point start = real2screen(fr.x + iconSize / 2, fr.y + iconSize / 2);
        Point end = real2screen(to.x, to.y);
        drawArrow(
            arrow,
            start.x,
            start.y,
            end.x,
            end.y,
            theta,
            calcArrowLength(),
            1.2,
            null,
            startHopAction,
            endHopAction == null ? endHopLocation : endHopAction);
      } else if (endHopAction != null && endHopLocation != null) {
        Point fr = endHopLocation;
        Point to = endHopAction.getLocation();
        if (startHopAction == null) {
          gc.setForeground(EColor.GRAY);
          arrow = EImage.ARROW_DISABLED;
        } else {
          gc.setForeground(EColor.BLUE);
          arrow = EImage.ARROW_DEFAULT;
        }
        Point start = real2screen(fr.x, fr.y);
        Point end = real2screen(to.x + iconSize / 2, to.y + iconSize / 2);
        drawArrow(
            arrow,
            start.x,
            start.y,
            end.x,
            end.y + iconSize / 2,
            theta,
            calcArrowLength(),
            1.2,
            null,
            startHopAction == null ? endHopLocation : startHopAction,
            endHopAction);
      }
    }

    for (ActionMeta actionMeta : workflowMeta.getActions()) {
      drawAction(actionMeta);
    }

    // Display an icon on the indicated location signaling to the user that the action in
    // question does not accept input
    //
    if (noInputAction != null) {
      gc.setLineWidth(2);
      gc.setForeground(EColor.RED);
      Point n = noInputAction.getLocation();
      gc.drawLine(
          round(offset.x + n.x - 5),
          round(offset.y + n.y - 5),
          round(offset.x + n.x + iconSize + 5),
          round(offset.y + n.y + iconSize + 5));
      gc.drawLine(
          round(offset.x + n.x - 5),
          round(offset.y + n.y + iconSize + 5),
          round(offset.x + n.x + iconSize + 5),
          round(offset.y + n.y - 5));
    }

    try {
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL, variables, HopExtensionPoint.WorkflowPainterEnd.id, this);
    } catch (HopException e) {
      LogChannel.GENERAL.logError("Error in WorkflowPainterEnd extension point", e);
    }

    drawRect(selectionRectangle);
  }

  protected void drawAction(ActionMeta actionMeta) throws HopException {
    int alpha = gc.getAlpha();

    Point pt = actionMeta.getLocation();
    if (pt == null) {
      pt = new Point(50, 50);
    }

    Point screen = real2screen(pt.x, pt.y);
    int x = screen.x;
    int y = screen.y;

    String name = actionMeta.getName();
    if (actionMeta.isSelected()) {
      gc.setLineWidth(3);
    } else {
      gc.setLineWidth(1);
    }

    gc.setBackground(EColor.BACKGROUND);
    gc.fillRoundRectangle(x - 1, y - 1, iconSize + 1, iconSize + 1, 7, 7);
    gc.drawActionIcon(x, y, actionMeta, magnification);

    areaOwners.add(
        new AreaOwner(AreaType.ACTION_ICON, x, y, iconSize, iconSize, offset, subject, actionMeta));

    boolean actionError = false;
    ActionResult actionResult = findActionResult(actionMeta);
    if (actionResult != null && !actionResult.isCheckpoint()) {
      actionError = !actionResult.getResult().getResult();
    }

    if (actionError || actionMeta.isMissing()) {
      gc.setForeground(EColor.RED);
    } else if (actionMeta.isDeprecated()) {
      gc.setForeground(EColor.DEPRECATED);
    } else {
      gc.setForeground(EColor.CRYSTAL);
    }
    gc.drawRoundRectangle(x - 1, y - 1, iconSize + 1, iconSize + 1, 7, 7);
    gc.setForeground(EColor.CRYSTAL);
    Point textsize = new Point(gc.textExtent("" + name).x, gc.textExtent("" + name).y);

    gc.setBackground(EColor.BACKGROUND);
    gc.setLineWidth(1);

    int xPos = x + (iconSize / 2) - (textsize.x / 2);
    int yPos = y + iconSize + 5;

    gc.setForeground(EColor.BLACK);

    // Help out the user working in single-click mode by allowing the name to be clicked to edit
    //
    Point nameExtent = gc.textExtent(name);
    if (isDrawingBorderAroundName()) {
      int tmpAlpha = gc.getAlpha();
      gc.setAlpha(230);
      gc.setBackground(EColor.LIGHTGRAY);
      gc.fillRoundRectangle(
          xPos - 8,
          yPos - 2,
          nameExtent.x + 15,
          nameExtent.y + 8,
          BasePainter.CORNER_RADIUS_5 + 15,
          BasePainter.CORNER_RADIUS_5 + 15);
      gc.setAlpha(tmpAlpha);
    }

    // Add the area owner for the action name
    //
    areaOwners.add(
        new AreaOwner(
            AreaType.ACTION_NAME,
            xPos - 8,
            yPos - 2,
            nameExtent.x + 15,
            nameExtent.y + 4,
            offset,
            actionMeta,
            name));

    gc.setForeground(EColor.BLACK);
    gc.setFont(EFont.GRAPH);
    gc.drawText(name, xPos, yPos, true);

    // See if we need to draw a line under the name to make the name look like a hyperlink.
    //
    if (name.equals(mouseOverName)) {
      gc.drawLine(xPos, yPos + nameExtent.y, xPos + nameExtent.x, yPos + nameExtent.y);
    }

    if (activeActions != null && activeActions.contains(actionMeta)) {
      gc.setForeground(EColor.BLUE);
      int iconX = (x + iconSize) - (miniIconSize / 2) + 1;
      int iconY = y - (miniIconSize / 2) - 1;

      gc.drawImage(actionMeta.isJoin() ? EImage.WAITING : EImage.BUSY, iconX, iconY, magnification);
      areaOwners.add(
          new AreaOwner(
              AreaType.ACTION_BUSY,
              iconX,
              iconY,
              miniIconSize,
              miniIconSize,
              offset,
              subject,
              actionMeta));
    } else {
      gc.setForeground(EColor.BLACK);
    }

    // Show an information icon in the upper left corner of the action...
    //
    if (!Utils.isEmpty(actionMeta.getDescription())) {
      int xInfo = x - (miniIconSize / 2) - 1;
      int yInfo = y - (miniIconSize / 2) - 1;
      gc.drawImage(EImage.INFO_DISABLED, xInfo, yInfo, magnification);
      areaOwners.add(
          new AreaOwner(
              AreaType.ACTION_INFO_ICON,
              xInfo,
              yInfo,
              miniIconSize,
              miniIconSize,
              offset,
              workflowMeta,
              actionMeta));
    }

    if (actionResult != null) {
      Result result = actionResult.getResult();
      int iconX = (x + iconSize) - (miniIconSize / 2) + 1;
      int iconY = y - (miniIconSize / 2) - 1;

      // Draw an execution result on the top right corner...
      //
      if (actionResult.isCheckpoint()) {
        gc.drawImage(EImage.CHECKPOINT, iconX, iconY, magnification);
        areaOwners.add(
            new AreaOwner(
                AreaType.ACTION_RESULT_CHECKPOINT,
                iconX,
                iconY,
                miniIconSize,
                miniIconSize,
                offset,
                actionMeta,
                actionResult));
      } else {
        if (result.getResult()) {
          gc.drawImage(EImage.SUCCESS, iconX, iconY, magnification);
          areaOwners.add(
              new AreaOwner(
                  AreaType.ACTION_RESULT_SUCCESS,
                  iconX,
                  iconY,
                  miniIconSize,
                  miniIconSize,
                  offset,
                  actionMeta,
                  actionResult));
        } else {
          gc.drawImage(EImage.FAILURE, iconX, iconY, magnification);
          areaOwners.add(
              new AreaOwner(
                  AreaType.ACTION_RESULT_FAILURE,
                  iconX,
                  iconY,
                  miniIconSize,
                  miniIconSize,
                  offset,
                  actionMeta,
                  actionResult));
        }
      }
    }

    WorkflowPainterExtension extension =
        new WorkflowPainterExtension(
            gc, areaOwners, workflowMeta, null, actionMeta, x, y, 0, 0, 0, 0, offset, iconSize);
    try {
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL, variables, HopExtensionPoint.WorkflowPainterAction.id, extension);
    } catch (Exception e) {
      LogChannel.GENERAL.logError(
          "Error calling extension point(s) for the workflow painter action", e);
    }

    // Restore the previous alpha value
    //
    gc.setAlpha(alpha);
  }

  private ActionResult findActionResult(ActionMeta actionMeta) {
    if (actionResults == null) {
      return null;
    }

    Iterator<ActionResult> iterator = actionResults.iterator();
    while (iterator.hasNext()) {
      ActionResult actionResult = iterator.next();

      if (actionResult.getActionName().equals(actionMeta.getName())) {
        return actionResult;
      }
    }

    return null;
  }

  protected void drawWorkflowHop(WorkflowHopMeta hop, boolean candidate) throws HopException {
    if (hop == null || hop.getFromAction() == null || hop.getToAction() == null) {
      return;
    }

    drawLine(hop, candidate);
  }

  /** Calculates line coordinates from center to center. */
  protected void drawLine(WorkflowHopMeta workflowHop, boolean isCandidate) throws HopException {
    int[] line = getLine(workflowHop.getFromAction(), workflowHop.getToAction());

    gc.setLineWidth(lineWidth);
    EColor color;

    if (workflowHop.getFromAction().isLaunchingInParallel()) {
      gc.setLineStyle(ELineStyle.PARALLEL);
    } else {
      gc.setLineStyle(ELineStyle.SOLID);
    }

    EImage arrow;
    if (isCandidate) {
      color = EColor.BLUE;
      arrow = EImage.ARROW_CANDIDATE;
    } else if (workflowHop.isEnabled()) {
      if (workflowHop.isUnconditional()) {
        color = EColor.HOP_DEFAULT;
        arrow = EImage.ARROW_DEFAULT;
      } else {
        if (workflowHop.isEvaluation()) {
          color = EColor.HOP_TRUE;
          arrow = EImage.ARROW_TRUE;
        } else {
          color = EColor.HOP_FALSE;
          arrow = EImage.ARROW_FALSE;
        }
      }
    } else {
      color = EColor.GRAY;
      arrow = EImage.ARROW_DISABLED;
    }

    gc.setForeground(color);

    if (workflowHop.isSplit()) {
      gc.setLineWidth(lineWidth + 2);
    }
    drawArrow(arrow, line, workflowHop);
    if (workflowHop.isSplit()) {
      gc.setLineWidth(lineWidth);
    }

    gc.setForeground(EColor.BLACK);
    gc.setBackground(EColor.BACKGROUND);
    gc.setLineStyle(ELineStyle.SOLID);
  }

  private void drawArrow(EImage arrow, int[] line, WorkflowHopMeta workflowHop)
      throws HopException {
    drawArrow(arrow, line, workflowHop, workflowHop.getFromAction(), workflowHop.getToAction());
  }

  @Override
  protected void drawArrow(
      EImage arrow,
      int x1,
      int y1,
      int x2,
      int y2,
      double theta,
      int size,
      double factor,
      WorkflowHopMeta workflowHop,
      Object startObject,
      Object endObject)
      throws HopException {
    int mx;
    int my;
    int a;
    int b;
    int dist;
    double angle;

    gc.drawLine(x1, y1, x2, y2);

    // What's the distance between the 2 points?
    a = Math.abs(x2 - x1);
    b = Math.abs(y2 - y1);
    dist = (int) Math.sqrt(a * a + b * b);

    // determine factor (position of arrow to left side or right side
    // 0-->100%)
    if (factor < 0) {
      if (dist >= 2 * iconSize) {
        factor = 1.3;
      } else {
        factor = 1.2;
      }
    }

    // in between 2 points
    mx = (int) (x1 + factor * (x2 - x1) / 2);
    my = (int) (y1 + factor * (y2 - y1) / 2);

    // calculate points for arrowhead
    angle = Math.atan2(y2 - y1, x2 - x1) + (Math.PI / 2);

    boolean q1 = Math.toDegrees(angle) >= 0 && Math.toDegrees(angle) < 90;
    boolean q2 = Math.toDegrees(angle) >= 90 && Math.toDegrees(angle) < 180;
    boolean q3 = Math.toDegrees(angle) >= 180 && Math.toDegrees(angle) < 270;
    boolean q4 = Math.toDegrees(angle) >= 270 || Math.toDegrees(angle) < 0;

    if (q1 || q3) {
      gc.drawImage(arrow, mx, my + 1, magnification, angle);
    } else if (q2 || q4) {
      gc.drawImage(arrow, mx, my, magnification, angle);
    }

    // Display an icon above the hop...
    //
    factor = 0.8;

    // in between 2 points
    mx = (int) (x1 + factor * (x2 - x1) / 2) - miniIconSize / 2;
    my = (int) (y1 + factor * (y2 - y1) / 2) - miniIconSize / 2;

    if (workflowHop != null) {

      if (workflowHop.getFromAction().isLaunchingInParallel()) {

        // in between 2 points
        mx = (int) (x1 + factor * (x2 - x1) / 2);
        my = (int) (y1 + factor * (y2 - y1) / 2);
        EImage image = (workflowHop.isEnabled()) ? EImage.PARALLEL : EImage.PARALLEL_DISABLED;
        gc.drawImage(image, mx, my, magnification, angle);
        areaOwners.add(
            new AreaOwner(
                AreaType.WORKFLOW_HOP_PARALLEL_ICON,
                mx,
                my,
                miniIconSize,
                miniIconSize,
                offset,
                subject,
                workflowHop));
      } else {
        EImage image;
        if (workflowHop.isUnconditional()) {
          image = (workflowHop.isEnabled()) ? EImage.UNCONDITIONAL : EImage.UNCONDITIONAL_DISABLED;
        } else {
          if (workflowHop.isEvaluation()) {
            image = (workflowHop.isEnabled()) ? EImage.TRUE : EImage.TRUE_DISABLED;
          } else {
            image = (workflowHop.isEnabled()) ? EImage.FALSE : EImage.FALSE_DISABLED;
          }
        }

        gc.drawImage(image, mx, my, magnification);
        areaOwners.add(
            new AreaOwner(
                AreaType.WORKFLOW_HOP_ICON,
                mx,
                my,
                miniIconSize,
                miniIconSize,
                offset,
                subject,
                workflowHop));
      }

      WorkflowPainterExtension extension =
          new WorkflowPainterExtension(
              gc,
              areaOwners,
              workflowMeta,
              workflowHop,
              null,
              x1,
              y1,
              x2,
              y2,
              mx,
              my,
              offset,
              iconSize);
      try {
        ExtensionPointHandler.callExtensionPoint(
            LogChannel.GENERAL, variables, HopExtensionPoint.WorkflowPainterArrow.id, extension);
      } catch (Exception e) {
        LogChannel.GENERAL.logError(
            "Error calling extension point(s) for the workflow painter arrow", e);
      }
    }
  }

  public void setStartHopAction(ActionMeta action) {
    this.startHopAction = action;
  }

  public void setEndHopLocation(Point location) {
    this.endHopLocation = location;
  }

  public void setEndHopAction(ActionMeta action) {
    this.endHopAction = action;
  }

  public void setNoInputAction(ActionMeta action) {
    this.noInputAction = action;
  }

  public void setActiveActions(List<ActionMeta> activeActions) {
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
  public void setActionResults(List<ActionResult> actionResults) {
    this.actionResults = actionResults;
    Collections.sort(this.actionResults);
  }

  public WorkflowMeta getWorkflowMeta() {
    return workflowMeta;
  }

  public void setWorkflowMeta(WorkflowMeta workflowMeta) {
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
