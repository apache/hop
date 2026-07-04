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

/** No-op implementation for the desktop RCP client. */
public class CanvasSvgFacadeImpl extends CanvasSvgFacade {

  @Override
  void registerCanvasInternal(Canvas canvas, Object graph) {
    // Desktop paints on SWT canvas directly
  }

  @Override
  void unregisterCanvasInternal(Canvas canvas) {
    // No-op
  }

  @Override
  CanvasSvgRenderResult renderPipelineInternal(
      Canvas canvas,
      PipelineCanvasSvgRenderer.Context context,
      float magnification,
      DPoint offset) {
    return null;
  }

  @Override
  CanvasSvgRenderResult renderWorkflowInternal(
      Canvas canvas,
      WorkflowCanvasSvgRenderer.Context context,
      float magnification,
      DPoint offset) {
    return null;
  }

  @Override
  String getSessionUuidInternal() {
    return null;
  }

  @Override
  long getRevisionInternal(String canvasId) {
    return 0;
  }

  @Override
  void setCanvasWidgetDataInternal(Canvas canvas, long revision) {
    // No-op
  }

  @Override
  void ensureInteractionHandlerInternal(Composite parent, Canvas canvas) {
    // No-op
  }
}
