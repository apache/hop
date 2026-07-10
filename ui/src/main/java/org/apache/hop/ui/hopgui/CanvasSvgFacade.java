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

package org.apache.hop.ui.hopgui;

import org.apache.hop.core.gui.CanvasSvgRenderResult;
import org.apache.hop.core.gui.DPoint;
import org.apache.hop.pipeline.canvas.PipelineCanvasSvgRenderer;
import org.apache.hop.workflow.canvas.WorkflowCanvasSvgRenderer;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;

/**
 * Facade for Hop Web server-side SVG canvas rendering. Desktop (RCP) uses a no-op implementation.
 */
public abstract class CanvasSvgFacade {

  private static final CanvasSvgFacade IMPL;

  static {
    IMPL = (CanvasSvgFacade) ImplementationLoader.newInstance(CanvasSvgFacade.class);
  }

  public static void registerCanvas(Canvas canvas, Object graph) {
    IMPL.registerCanvasInternal(canvas, graph);
  }

  public static void unregisterCanvas(Canvas canvas) {
    IMPL.unregisterCanvasInternal(canvas);
  }

  public static CanvasSvgRenderResult renderPipeline(
      Canvas canvas,
      PipelineCanvasSvgRenderer.Context context,
      float magnification,
      DPoint offset) {
    return IMPL.renderPipelineInternal(canvas, context, magnification, offset);
  }

  public static CanvasSvgRenderResult renderWorkflow(
      Canvas canvas,
      WorkflowCanvasSvgRenderer.Context context,
      float magnification,
      DPoint offset) {
    return IMPL.renderWorkflowInternal(canvas, context, magnification, offset);
  }

  public static String getSessionUuid() {
    return IMPL.getSessionUuidInternal();
  }

  public static long getRevision(String canvasId) {
    return IMPL.getRevisionInternal(canvasId);
  }

  public static void setCanvasWidgetData(Canvas canvas, long revision) {
    IMPL.setCanvasWidgetDataInternal(canvas, revision);
  }

  public static void ensureInteractionHandler(Composite parent, Canvas canvas) {
    IMPL.ensureInteractionHandlerInternal(parent, canvas);
  }

  abstract void registerCanvasInternal(Canvas canvas, Object graph);

  abstract void unregisterCanvasInternal(Canvas canvas);

  abstract CanvasSvgRenderResult renderPipelineInternal(
      Canvas canvas, PipelineCanvasSvgRenderer.Context context, float magnification, DPoint offset);

  abstract CanvasSvgRenderResult renderWorkflowInternal(
      Canvas canvas, WorkflowCanvasSvgRenderer.Context context, float magnification, DPoint offset);

  abstract String getSessionUuidInternal();

  abstract long getRevisionInternal(String canvasId);

  abstract void setCanvasWidgetDataInternal(Canvas canvas, long revision);

  abstract void ensureInteractionHandlerInternal(Composite parent, Canvas canvas);
}
