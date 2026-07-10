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

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.CanvasSvgRenderResult;
import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.Rectangle;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.pipeline.canvas.PipelineCanvasSvgRenderer;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.hopgui.canvas.AreaOwnerJsonSerializer;
import org.apache.hop.ui.hopgui.canvas.CanvasGraphRegistry;
import org.apache.hop.ui.hopgui.canvas.CanvasInteractionHandler;
import org.apache.hop.ui.hopgui.canvas.CanvasRenderSnapshot;
import org.apache.hop.ui.hopgui.canvas.CanvasSvgRendererHandler;
import org.apache.hop.workflow.canvas.WorkflowCanvasSvgRenderer;
import org.eclipse.rap.json.JsonObject;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.widgets.WidgetUtil;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;

public class CanvasSvgFacadeImpl extends CanvasSvgFacade {

  @Override
  void registerCanvasInternal(Canvas canvas, Object graph) {
    String canvasId = WidgetUtil.getId(canvas);
    CanvasGraphRegistry.getInstance().register(canvasId, canvas, graph);
    canvas.setData("canvasId", canvasId);
    canvas.setData("sessionUuid", getSessionUuidInternal());
    new CanvasSvgRendererHandler((Composite) canvas.getParent());
  }

  @Override
  void unregisterCanvasInternal(Canvas canvas) {
    String canvasId = WidgetUtil.getId(canvas);
    CanvasGraphRegistry.getInstance().unregister(canvasId);
  }

  @Override
  CanvasSvgRenderResult renderPipelineInternal(
      Canvas canvas,
      PipelineCanvasSvgRenderer.Context context,
      float magnification,
      DPoint offset) {
    try {
      CanvasSvgRenderResult result = PipelineCanvasSvgRenderer.render(context);
      storeSnapshot(canvas, result, magnification, offset, context.canvasSize);
      return result;
    } catch (HopException e) {
      LogChannel.UI.logError("Failed to render pipeline SVG for web canvas", e);
      return null;
    }
  }

  @Override
  CanvasSvgRenderResult renderWorkflowInternal(
      Canvas canvas,
      WorkflowCanvasSvgRenderer.Context context,
      float magnification,
      DPoint offset) {
    try {
      CanvasSvgRenderResult result = WorkflowCanvasSvgRenderer.render(context);
      storeSnapshot(canvas, result, magnification, offset, context.canvasSize);
      return result;
    } catch (HopException e) {
      LogChannel.UI.logError("Failed to render workflow SVG for web canvas", e);
      return null;
    }
  }

  private void storeSnapshot(
      Canvas canvas,
      CanvasSvgRenderResult result,
      float magnification,
      DPoint offset,
      org.apache.hop.core.gui.Point canvasSize) {
    if (result == null) {
      return;
    }
    String canvasId = WidgetUtil.getId(canvas);
    long revision = CanvasGraphRegistry.getInstance().nextRevision();

    JsonObject props = new JsonObject();
    props.add("themeId", PropsUi.getInstance().isDarkMode() ? "dark" : "light");
    props.add("magnification", (float) (magnification * PropsUi.getNativeZoomFactor()));
    props.add("offsetX", offset.x);
    props.add("offsetY", offset.y);
    props.add("iconSize", PropsUi.getInstance().getIconSize());
    props.add("gridSize", PropsUi.getInstance().getCanvasGridSize());
    props.add("showGrid", PropsUi.getInstance().isShowCanvasGridEnabled());
    props.add("useDoubleClick", PropsUi.getInstance().useDoubleClick());
    if (canvasSize != null) {
      props.add("width", canvasSize.x);
      props.add("height", canvasSize.y);
    }
    addRectangle(props, "viewPort", result.getViewPort());
    addRectangle(props, "graphPort", result.getGraphPort());

    CanvasRenderSnapshot snapshot =
        new CanvasRenderSnapshot(revision, result.getSvg(), result.getAreaOwners(), props);
    CanvasGraphRegistry.getInstance().updateSnapshot(canvasId, snapshot);
    Object graph = CanvasGraphRegistry.getInstance().getGraph(canvasId);
    syncAreaOwnersToGraph(graph, result.getAreaOwners());
    setCanvasWidgetDataInternal(canvas, revision);
    CanvasSvgRendererHandler.notifyCanvasReady(canvas, revision);
  }

  @Override
  String getSessionUuidInternal() {
    Object uuid = RWT.getUISession().getAttribute(CanvasGraphRegistry.SESSION_UUID_ATTR);
    return uuid == null ? null : uuid.toString();
  }

  @Override
  long getRevisionInternal(String canvasId) {
    return CanvasGraphRegistry.getInstance().getCurrentRevision(canvasId);
  }

  @Override
  void setCanvasWidgetDataInternal(Canvas canvas, long revision) {
    canvas.setData("renderRevision", revision);
    canvas.setData("sessionUuid", getSessionUuidInternal());
    String canvasId = WidgetUtil.getId(canvas);
    canvas.setData("canvasId", canvasId);

    CanvasRenderSnapshot snapshot = CanvasGraphRegistry.getInstance().getSnapshot(canvasId);
    if (snapshot != null) {
      // Keep area owners on the graph object via cache; notify client to refetch
      canvas.setData("areas", AreaOwnerJsonSerializer.toJsonArray(snapshot.getAreaOwners()));
    }
  }

  @Override
  void ensureInteractionHandlerInternal(Composite parent, Canvas canvas) {
    new CanvasInteractionHandler(parent);
    CanvasInteractionHandler.ensureRemoteObject(canvas);
  }

  private static void addRectangle(JsonObject parent, String key, Rectangle rect) {
    if (rect == null) {
      return;
    }
    JsonObject jsonRect = new JsonObject();
    jsonRect.add("x", rect.x);
    jsonRect.add("y", rect.y);
    jsonRect.add("width", rect.width);
    jsonRect.add("height", rect.height);
    parent.add(key, jsonRect);
  }

  /** Expose area list update for graph classes that populate areaOwners after render. */
  static void syncAreaOwnersToGraph(Object graph, List<AreaOwner> areaOwners) {
    if (graph instanceof org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph pipelineGraph) {
      pipelineGraph.replaceAreaOwners(areaOwners);
    } else if (graph
        instanceof org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph workflowGraph) {
      workflowGraph.replaceAreaOwners(areaOwners);
    }
  }
}
