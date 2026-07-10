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

package org.apache.hop.pipeline.canvas;

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
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.svg.HopSvgGraphics2D;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePainter;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream.StreamType;

/**
 * Renders a pipeline graph to SVG using the same {@link PipelinePainter} logic as the Hop GUI,
 * while collecting {@link AreaOwner} click regions.
 */
public final class PipelineCanvasSvgRenderer {

  private PipelineCanvasSvgRenderer() {}

  /** Mutable bag of painter inputs; populated by HopGuiPipelineGraph before rendering. */
  public static final class Context {
    public IVariables variables;
    public PipelineMeta pipelineMeta;
    public Point canvasSize;
    public DPoint offset;
    public PipelineHopMeta candidate;
    public Rectangle selectionRegion;
    public int iconSize;
    public int lineWidth;
    public int gridSize;
    public String noteFontName;
    public int noteFontHeight;
    public IPipelineEngine<PipelineMeta> pipeline;
    public boolean slowTransformIndicatorEnabled;
    public double zoomFactor;
    public Map<String, RowBuffer> outputRowsMap;
    public boolean drawingBorderAroundName;
    public String mouseOverName;
    public Map<String, Object> stateMap;
    public float magnification;
    public float screenMagnification;
    public Map<String, String> transformLogMap;
    public TransformMeta startHopTransform;
    public Point endHopLocation;
    public TransformMeta forbiddenTransform;
    public TransformMeta endHopTransform;
    public StreamType candidateHopType;
    public boolean startErrorHopTransform;
    public Point maximum;
    public boolean showingNavigationView;
    public boolean showOriginBoundary;
    public boolean showingSelectedTransformMetrics;
    public String emptyPipelineImagePath;
    public ClassLoader emptyPipelineImageClassLoader;
    public String emptyPipelineMessage;
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

      PipelinePainter pipelinePainter =
          new PipelinePainter(
              gc,
              ctx.variables,
              ctx.pipelineMeta,
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
              ctx.pipeline,
              ctx.slowTransformIndicatorEnabled,
              ctx.zoomFactor,
              ctx.outputRowsMap,
              ctx.drawingBorderAroundName,
              ctx.mouseOverName,
              ctx.stateMap);

      pipelinePainter.setMagnification(ctx.magnification);
      pipelinePainter.setTransformLogMap(ctx.transformLogMap);
      pipelinePainter.setStartHopTransform(ctx.startHopTransform);
      pipelinePainter.setEndHopLocation(ctx.endHopLocation);
      pipelinePainter.setNoInputTransform(ctx.forbiddenTransform);
      pipelinePainter.setEndHopTransform(ctx.endHopTransform);
      pipelinePainter.setCandidateHopType(ctx.candidateHopType);
      pipelinePainter.setStartErrorHopTransform(ctx.startErrorHopTransform);
      pipelinePainter.setMaximum(ctx.maximum);
      pipelinePainter.setShowingNavigationView(ctx.showingNavigationView);
      pipelinePainter.setScreenMagnification(ctx.screenMagnification);
      pipelinePainter.setShowOriginBoundary(ctx.showOriginBoundary);
      pipelinePainter.setShowingSelectedTransformMetrics(ctx.showingSelectedTransformMetrics);

      pipelinePainter.drawPipelineImage();

      if (ctx.pipelineMeta.isEmpty()
          && ctx.emptyPipelineImagePath != null
          && ctx.emptyPipelineImageClassLoader != null) {
        SvgFile svgFile =
            new SvgFile(ctx.emptyPipelineImagePath, ctx.emptyPipelineImageClassLoader);
        gc.setTransform(0.0f, 0.0f, ctx.magnification);
        int iconX = 150 + (int) Math.round(ctx.offset.x);
        int iconY = 150 + (int) Math.round(ctx.offset.y);
        gc.drawImage(svgFile, iconX, iconY, 32, 40, gc.getMagnification(), 0);
        if (ctx.emptyPipelineMessage != null) {
          gc.drawText(
              ctx.emptyPipelineMessage,
              155 + (int) Math.round(ctx.offset.x),
              125 + (int) Math.round(ctx.offset.y),
              true);
        }
      }

      return new CanvasSvgRenderResult(
          graphics2D.toXml(),
          areaOwners,
          pipelinePainter.getViewPort(),
          pipelinePainter.getGraphPort());
    } catch (Exception e) {
      throw new HopException(
          "Unable to generate SVG for pipeline " + ctx.pipelineMeta.getName(), e);
    }
  }
}
