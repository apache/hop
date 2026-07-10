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

package org.apache.hop.workflow.canvas;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.CanvasSvgRenderResult;
import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.Rectangle;
import org.apache.hop.core.gui.SvgGc;
import org.apache.hop.core.svg.HopSvgGraphics2D;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.WorkflowPainter;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/**
 * Renders a workflow graph to SVG using the same {@link WorkflowPainter} logic as the Hop GUI,
 * while collecting {@link AreaOwner} click regions.
 */
public final class WorkflowCanvasSvgRenderer {

  private WorkflowCanvasSvgRenderer() {}

  /** Mutable bag of painter inputs; populated by HopGuiWorkflowGraph before rendering. */
  public static final class Context {
    public IVariables variables;
    public WorkflowMeta workflowMeta;
    public Point canvasSize;
    public DPoint offset;
    public WorkflowHopMeta candidate;
    public Rectangle selectionRegion;
    public int iconSize;
    public int lineWidth;
    public int gridSize;
    public String noteFontName;
    public int noteFontHeight;
    public double zoomFactor;
    public boolean drawingBorderAroundName;
    public String mouseOverName;
    public float magnification;
    public float screenMagnification;
    public ActionMeta startHopAction;
    public Point endHopLocation;
    public ActionMeta endHopAction;
    public ActionMeta forbiddenAction;
    public IWorkflowEngine<WorkflowMeta> workflow;
    public Point maximum;
    public boolean showingNavigationView;
    public boolean showOriginBoundary;
    public String emptyWorkflowImagePath;
    public ClassLoader emptyWorkflowImageClassLoader;
    public String emptyWorkflowMessage;
    public boolean darkMode;
    public Map<String, String> contrastingColorStrings;
  }

  public static CanvasSvgRenderResult render(Context ctx) throws HopException {
    try {
      List<AreaOwner> areaOwners = new ArrayList<>();
      HopSvgGraphics2D graphics2D = HopSvgGraphics2D.newDocument();
      SvgGc gc =
          new SvgGc(
              graphics2D,
              ctx.canvasSize,
              ctx.iconSize,
              0,
              0,
              ctx.darkMode,
              ctx.contrastingColorStrings);

      WorkflowPainter workflowPainter =
          new WorkflowPainter(
              gc,
              ctx.variables,
              ctx.workflowMeta,
              ctx.canvasSize,
              ctx.offset,
              ctx.candidate,
              ctx.selectionRegion,
              areaOwners,
              ctx.iconSize,
              ctx.lineWidth,
              ctx.gridSize,
              ctx.noteFontName,
              ctx.noteFontHeight,
              ctx.zoomFactor,
              ctx.drawingBorderAroundName,
              ctx.mouseOverName);

      workflowPainter.setMagnification(ctx.magnification);
      workflowPainter.setStartHopAction(ctx.startHopAction);
      workflowPainter.setEndHopLocation(ctx.endHopLocation);
      workflowPainter.setEndHopAction(ctx.endHopAction);
      workflowPainter.setNoInputAction(ctx.forbiddenAction);

      if (ctx.workflow != null) {
        workflowPainter.setActionResults(ctx.workflow.getActionResults());
        workflowPainter.setActiveActions(new ArrayList<>(ctx.workflow.getActiveActions()));
      } else {
        workflowPainter.setActionResults(new ArrayList<>());
        workflowPainter.setActiveActions(new ArrayList<>());
      }

      workflowPainter.setMaximum(ctx.maximum);
      workflowPainter.setShowingNavigationView(ctx.showingNavigationView);
      workflowPainter.setScreenMagnification(ctx.screenMagnification);
      workflowPainter.setShowOriginBoundary(ctx.showOriginBoundary);

      workflowPainter.drawWorkflow();

      boolean showEmpty =
          ctx.workflowMeta.isEmpty()
              || (ctx.workflowMeta.nrNotes() == 0
                  && ctx.workflowMeta.nrActions() == 1
                  && ctx.workflowMeta.getAction(0).isStart());
      if (showEmpty
          && ctx.emptyWorkflowImagePath != null
          && ctx.emptyWorkflowImageClassLoader != null) {
        SvgFile svgFile =
            new SvgFile(ctx.emptyWorkflowImagePath, ctx.emptyWorkflowImageClassLoader);
        gc.setTransform(0.0f, 0.0f, ctx.magnification);
        int iconX = 150 + (int) Math.round(ctx.offset.x);
        int iconY = 150 + (int) Math.round(ctx.offset.y);
        gc.drawImage(svgFile, iconX, iconY, 32, 40, gc.getMagnification(), 0);
        if (ctx.emptyWorkflowMessage != null) {
          gc.drawText(
              ctx.emptyWorkflowMessage,
              155 + (int) Math.round(ctx.offset.x),
              125 + (int) Math.round(ctx.offset.y),
              true);
        }
      }

      return new CanvasSvgRenderResult(
          graphics2D.toXml(),
          areaOwners,
          workflowPainter.getViewPort(),
          workflowPainter.getGraphPort());
    } catch (Exception e) {
      throw new HopException(
          "Unable to generate SVG for workflow " + ctx.workflowMeta.getName(), e);
    }
  }
}
