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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.gui.CanvasSvgRenderResult;
import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;

class PipelineCanvasSvgRendererTest {

  @Test
  void rendersPipelineSvgWithClickRegions() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("test-pipeline");

    TransformMeta transform = new TransformMeta();
    transform.setName("Generator");
    transform.setLocation(100, 100);
    pipelineMeta.addTransform(transform);

    PipelineCanvasSvgRenderer.Context context = new PipelineCanvasSvgRenderer.Context();
    context.variables = new Variables();
    context.pipelineMeta = pipelineMeta;
    context.canvasSize = new Point(800, 600);
    context.offset = new DPoint(0, 0);
    context.iconSize = 32;
    context.lineWidth = 1;
    context.gridSize = 16;
    context.noteFontName = "Arial";
    context.noteFontHeight = 10;
    context.zoomFactor = 1.0;
    context.magnification = 1.0f;
    context.screenMagnification = 1.0f;
    context.maximum = pipelineMeta.getMaximum();
    context.showingNavigationView = false;
    context.showOriginBoundary = false;
    context.showingSelectedTransformMetrics = false;
    context.drawingBorderAroundName = false;

    CanvasSvgRenderResult result = PipelineCanvasSvgRenderer.render(context);

    assertNotNull(result);
    assertNotNull(result.getSvg());
    assertTrue(result.getSvg().contains("<svg"));
    assertFalse(result.getAreaOwners().isEmpty());
  }

  @Test
  void rendersDarkThemeBackground() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("dark-pipeline");

    PipelineCanvasSvgRenderer.Context context = new PipelineCanvasSvgRenderer.Context();
    context.variables = new Variables();
    context.pipelineMeta = pipelineMeta;
    context.canvasSize = new Point(400, 300);
    context.offset = new DPoint(0, 0);
    context.iconSize = 32;
    context.lineWidth = 1;
    context.gridSize = 16;
    context.noteFontName = "Arial";
    context.noteFontHeight = 10;
    context.zoomFactor = 1.0;
    context.magnification = 1.0f;
    context.screenMagnification = 1.0f;
    context.maximum = pipelineMeta.getMaximum();
    context.showingNavigationView = false;
    context.showOriginBoundary = false;
    context.showingSelectedTransformMetrics = false;
    context.drawingBorderAroundName = false;
    context.darkMode = true;

    CanvasSvgRenderResult result = PipelineCanvasSvgRenderer.render(context);

    assertNotNull(result.getSvg());
    assertTrue(
        result.getSvg().contains("rgb(50,50,50)") || result.getSvg().contains("rgb(50, 50, 50)"),
        "Dark canvas background should use graph color rgb(50,50,50)");
  }
}
