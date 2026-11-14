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

import java.awt.Dimension;
import java.util.ArrayList;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.SvgExportCanvas;
import org.apache.hop.core.gui.SvgGc;
import org.apache.hop.core.svg.HopSvgGraphics2D;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.workflow.action.ActionMeta;

public class WorkflowSvgPainter {

  public static String generateWorkflowSvg(
      WorkflowMeta workflowMeta, float magnification, IVariables variables) throws HopException {
    try {
      float mag = magnification > 0f ? magnification : 1f;
      int margin = SvgExportCanvas.MARGIN;

      HopSvgGraphics2D graphics2D = HopSvgGraphics2D.newDocument();
      Point area = new Point(1, 1);
      SvgGc gc = new SvgGc(graphics2D, area, SvgExportCanvas.ICON_SIZE, 0, 0);
      WorkflowPainter workflowPainter =
          new WorkflowPainter(
              gc,
              variables,
              workflowMeta,
              area,
              new DPoint(0, 0),
              null,
              null,
              new ArrayList<>(),
              SvgExportCanvas.ICON_SIZE,
              1,
              0,
              "Arial",
              10,
              1.0d,
              false,
              null);
      workflowPainter.setMagnification(mag);

      SvgExportCanvas.Bounds bounds = new SvgExportCanvas.Bounds();
      for (ActionMeta action : workflowMeta.getActions()) {
        SvgExportCanvas.includeNode(
            bounds,
            gc,
            action.getLocation(),
            action.getName(),
            action.getAction() != null && !Utils.isEmpty(action.getDescription()),
            SvgExportCanvas.ICON_SIZE);
      }
      for (NotePadMeta note : workflowMeta.getNotes()) {
        SvgExportCanvas.includeNote(
            bounds, note.getLocation(), workflowPainter.getNoteDrawnSize(note));
      }

      Point canvas = SvgExportCanvas.canvasSize(bounds, mag, margin);
      area.x = canvas.x;
      area.y = canvas.y;
      graphics2D.setSVGCanvasSize(new Dimension(canvas.x, canvas.y));
      workflowPainter.setOffset(SvgExportCanvas.offsetFor(bounds, mag, margin));
      workflowPainter.drawWorkflow();

      return graphics2D.toXml();
    } catch (Exception e) {
      throw new HopException("Unable to generate SVG for workflow " + workflowMeta.getName(), e);
    }
  }
}
