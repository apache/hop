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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.batik.anim.dom.SAXSVGDocumentFactory;
import org.apache.batik.util.XMLResourceDescriptor;
import org.apache.hop.core.gui.CanvasSvgRenderResult;
import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.svg.SvgCache;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.EngineComponent;
import org.apache.hop.pipeline.engine.EngineComponent.ComponentExecutionStatus;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.w3c.dom.svg.SVGDocument;

class PipelineCanvasSvgRendererTest {

  /**
   * Status icons live in the UI module. The engine test classpath does not contain them, so seed
   * the SVG cache from the UI sources before a badge is drawn.
   */
  @BeforeAll
  static void cacheStatusIcons() throws Exception {
    cacheCanvasIcon("ui/images/success.svg");
    cacheCanvasIcon("ui/images/failure.svg");
    cacheCanvasIcon("ui/images/waiting.svg");
  }

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

  @Test
  void partialCopiesDrawTheFinishedCountAndNotTheSuccessCheck() throws Exception {
    String svg =
        renderWithCopies(
                List.of(
                    copy(0, ComponentExecutionStatus.STATUS_FINISHED),
                    copy(1, ComponentExecutionStatus.STATUS_RUNNING),
                    copy(2, ComponentExecutionStatus.STATUS_RUNNING),
                    copy(3, ComponentExecutionStatus.STATUS_RUNNING)),
                null)
            .getSvg();

    assertFalse(svg.contains("#5cc0c4"), svg);
    assertTrue(svg.contains(">1<") || svg.contains(">1</text>"), svg);
    assertTrue(svg.contains("rgb(92,192,196)") || svg.contains("rgb(92, 192, 196)"), svg);
  }

  @Test
  void allCopiesFinishedDrawTheSuccessCheckWithoutACountDisc() throws Exception {
    String svg =
        renderWithCopies(
                List.of(
                    copy(0, ComponentExecutionStatus.STATUS_FINISHED),
                    copy(1, ComponentExecutionStatus.STATUS_FINISHED)),
                null)
            .getSvg();

    assertTrue(svg.contains("#5cc0c4"), svg);
    assertEquals(1, countOf(svg, "#5cc0c4"));
    assertFalse(svg.contains("rgb(92,192,196)") || svg.contains("rgb(92, 192, 196)"), svg);
  }

  @Test
  void failureIconIsNotCoveredByTheSuccessCheck() throws Exception {
    Map<String, String> transformLogMap = new HashMap<>();
    transformLogMap.put("Load", "transform failed");
    String svg =
        renderWithCopies(
                List.of(
                    copy(0, ComponentExecutionStatus.STATUS_FINISHED),
                    copy(1, ComponentExecutionStatus.STATUS_FINISHED)),
                transformLogMap)
            .getSvg();

    assertTrue(svg.contains("#5cc0c4"), svg);
    assertTrue(svg.contains("#ea102a"), svg);
  }

  @Test
  void failureIconWithPartialCopiesDrawsBothBadges() throws Exception {
    Map<String, String> transformLogMap = new HashMap<>();
    transformLogMap.put("Load", "transform failed");
    String svg =
        renderWithCopies(
                List.of(
                    copy(0, ComponentExecutionStatus.STATUS_FINISHED),
                    copy(1, ComponentExecutionStatus.STATUS_RUNNING)),
                transformLogMap)
            .getSvg();

    assertTrue(svg.contains("#ea102a"), svg);
    assertTrue(svg.contains("rgb(92,192,196)") || svg.contains("rgb(92, 192, 196)"), svg);
    assertTrue(svg.contains(">1<") || svg.contains(">1</text>"), svg);
  }

  @Test
  void failureIconDrawnFromComponentErrorsWithoutTransformLogMap() throws Exception {
    EngineComponent failedCopy = copy(0, ComponentExecutionStatus.STATUS_STOPPED);
    failedCopy.setErrors(1);
    EngineComponent runningCopy = copy(1, ComponentExecutionStatus.STATUS_RUNNING);

    String svg = renderWithCopies(List.of(failedCopy, runningCopy), null).getSvg();

    assertTrue(svg.contains("#ea102a"), "Failure icon should be drawn from copy errors");
  }

  @Test
  void finishedWithErrorsDrawsFailureAndSuccessWhenTransformLogMapIsNull() throws Exception {
    EngineComponent copy0 = copy(0, ComponentExecutionStatus.STATUS_FINISHED);
    copy0.setErrors(1);
    EngineComponent copy1 = copy(1, ComponentExecutionStatus.STATUS_FINISHED);

    String svg = renderWithCopies(List.of(copy0, copy1), null).getSvg();

    assertTrue(svg.contains("#ea102a"), "Failure icon should be drawn from copy errors");
    assertTrue(svg.contains("#5cc0c4"), "Success check should be drawn alongside failure icon");
  }

  @Test
  void pausedCopiesDrawTheWaitingIconOnce() throws Exception {
    String svg =
        renderWithCopies(
                List.of(
                    copy(0, ComponentExecutionStatus.STATUS_PAUSED),
                    copy(1, ComponentExecutionStatus.STATUS_PAUSED)),
                null)
            .getSvg();

    assertTrue(svg.contains("#800080"), svg);
    assertEquals(1, countOf(svg, "#800080"));
    assertFalse(svg.contains("#5cc0c4"), svg);
  }

  private static CanvasSvgRenderResult renderWithCopies(
      List<IEngineComponent> copies, Map<String, String> transformLogMap) throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("copies");

    TransformMeta transform = new TransformMeta();
    transform.setName("Load");
    transform.setLocation(100, 100);
    pipelineMeta.addTransform(transform);

    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    when(pipeline.getComponentCopies("Load")).thenReturn(copies);

    PipelineCanvasSvgRenderer.Context context = baseContext(pipelineMeta);
    context.pipeline = pipeline;
    context.transformLogMap = transformLogMap;
    return PipelineCanvasSvgRenderer.render(context);
  }

  private static PipelineCanvasSvgRenderer.Context baseContext(PipelineMeta pipelineMeta) {
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
    return context;
  }

  private static EngineComponent copy(int copyNr, ComponentExecutionStatus status) {
    EngineComponent component = new EngineComponent("Load", copyNr);
    component.setStatus(status);
    return component;
  }

  private static void cacheCanvasIcon(String filename) throws Exception {
    if (SvgCache.findSvg(filename) != null) {
      return;
    }
    Path fromModule = Path.of("..", "ui", "src", "main", "resources").resolve(filename);
    Path fromRoot = Path.of("ui", "src", "main", "resources").resolve(filename);
    Path file = Files.exists(fromModule) ? fromModule : fromRoot;
    SAXSVGDocumentFactory factory =
        new SAXSVGDocumentFactory(XMLResourceDescriptor.getXMLParserClassName());
    SVGDocument document;
    try (InputStream in = Files.newInputStream(file)) {
      document = factory.createSVGDocument(filename, in);
    }
    SvgCache.addSvg(filename, document, 24, 24, 0, 0);
  }

  private static int countOf(String text, String token) {
    int count = 0;
    int from = 0;
    while (true) {
      int found = text.indexOf(token, from);
      if (found < 0) {
        return count;
      }
      count++;
      from = found + token.length();
    }
  }
}
