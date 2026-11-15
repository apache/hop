/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline;

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hop.core.Const;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.SvgExportCanvas;
import org.apache.hop.core.gui.SvgGc;
import org.apache.hop.core.svg.HopSvgGraphics2D;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.pipeline.transform.TransformMeta;

public class PipelineSvgPainter {

  public static String generatePipelineSvg(
      PipelineMeta pipelineMeta, float magnification, IVariables variables) throws HopException {
    return generatePipelineSvg(pipelineMeta, magnification, variables, new DPoint(0, 0), 1.0f);
  }

  public static String generatePipelineSvg(
      PipelineMeta pipelineMeta,
      float magnification,
      IVariables variables,
      DPoint offset,
      float zoomFactor)
      throws HopException {
    return generatePipelineSvg(
        pipelineMeta, magnification, variables, offset, zoomFactor, SvgExportCanvas.MARGIN);
  }

  public static String generatePipelineSvg(
      PipelineMeta pipelineMeta,
      float magnification,
      IVariables variables,
      DPoint offset,
      float zoomFactor,
      int extraSize)
      throws HopException {
    try {
      float mag = magnification > 0f ? magnification : 1f;
      int margin = Math.max(0, extraSize);

      HopSvgGraphics2D graphics2D = HopSvgGraphics2D.newDocument();
      Point area = new Point(1, 1);
      SvgGc gc = new SvgGc(graphics2D, area, SvgExportCanvas.ICON_SIZE, 0, 0);
      PipelinePainter pipelinePainter =
          new PipelinePainter(
              gc,
              variables,
              pipelineMeta,
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
              zoomFactor,
              false,
              null,
              new HashMap<>());
      pipelinePainter.setMagnification(mag);

      SvgExportCanvas.Bounds bounds = new SvgExportCanvas.Bounds();
      for (TransformMeta transform : pipelineMeta.getTransforms()) {
        SvgExportCanvas.includeNode(
            bounds,
            gc,
            transform.getLocation(),
            transform.getName(),
            !Utils.isEmpty(transform.getDescription()),
            SvgExportCanvas.ICON_SIZE);
        includeTransformDecorations(bounds, gc, transform);
      }
      for (NotePadMeta note : pipelineMeta.getNotes()) {
        SvgExportCanvas.includeNote(
            bounds, note.getLocation(), pipelinePainter.getNoteDrawnSize(note));
      }

      Point canvas = SvgExportCanvas.canvasSize(bounds, mag, margin);
      area.x = canvas.x;
      area.y = canvas.y;
      graphics2D.setSVGCanvasSize(new Dimension(canvas.x, canvas.y));

      DPoint fit = SvgExportCanvas.offsetFor(bounds, mag, margin);
      if (offset != null) {
        fit.x += offset.x;
        fit.y += offset.y;
      }
      pipelinePainter.setOffset(fit);
      pipelinePainter.drawPipelineImage();

      return graphics2D.toXml();
    } catch (Exception e) {
      throw new HopException("Unable to generate SVG for pipeline " + pipelineMeta.getName(), e);
    }
  }

  /**
   * Partition schema box and the {@code xN} copies label are painted outside the icon. {@link
   * PipelinePainter} draws the box 1.5 icons left and 2 icons above, and the copies label just
   * above the icon's top-left.
   */
  private static void includeTransformDecorations(
      SvgExportCanvas.Bounds bounds, IGc gc, TransformMeta transform) {
    Point loc = transform.getLocation() == null ? new Point(50, 50) : transform.getLocation();
    int icon = SvgExportCanvas.ICON_SIZE;
    gc.setFont(IGc.EFont.GRAPH);
    PartitionSchema schema =
        transform.isPartitioned()
            ? transform.getTransformPartitioningMeta().getPartitionSchema()
            : null;
    if (schema != null) {
      String count =
          schema.isDynamicallyDefined()
              ? Const.NVL(schema.getNumberOfPartitions(), "?")
              : String.valueOf(schema.getPartitionIDs().size());
      Point box = gc.textExtent("Px" + count);
      Point name = gc.textExtent(Const.NVL(schema.getName(), "<no partition name>"));
      bounds.include(
          loc.x - icon - icon / 2,
          loc.y - 2 * icon,
          Math.max(box.x + 2, name.x),
          box.y + 5 + name.y);
    } else if (!"1".equals(transform.getCopiesString())) {
      Point copies = gc.textExtent("x" + transform.getCopiesString());
      bounds.include(loc.x - copies.x + 1, loc.y - copies.y - 4, copies.x, copies.y);
    }
  }
}
