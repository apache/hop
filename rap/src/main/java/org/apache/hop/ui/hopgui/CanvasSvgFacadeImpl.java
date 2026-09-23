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
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.Rectangle;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.pipeline.canvas.PipelineCanvasSvgRenderer;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.hopgui.canvas.AreaOwnerJsonSerializer;
import org.apache.hop.ui.hopgui.canvas.CanvasGraphRegistry;
import org.apache.hop.ui.hopgui.canvas.CanvasInteractionHandler;
import org.apache.hop.ui.hopgui.canvas.CanvasRenderSnapshot;
import org.apache.hop.ui.hopgui.canvas.CanvasSvgRendererHandler;
import org.apache.hop.ui.hopgui.shared.IWebCanvasGraph;
import org.apache.hop.workflow.canvas.WorkflowCanvasSvgRenderer;
import org.eclipse.rap.json.JsonObject;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.widgets.WidgetUtil;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;

/**
 * Hop Web implementation: the graph is rendered to SVG on the server and fetched by the browser.
 *
 * <p>RAP paints synchronously on every {@code redraw()} and every resize, and the graph classes
 * call {@code redraw()} liberally (mouse down, mouse up, updateGui, resize, ...). On top of that,
 * RAP's text-size measurement enlarges and restores every shell by 1000px after each new dialog,
 * which resizes every open graph tab twice. A single click on a transform in a 120-transform
 * pipeline rendered the SVG a dozen times per request (issue #8435). So a paint does not render: it
 * schedules one repaint that runs once the request's event queue has drained, and only that repaint
 * renders. Canvases that are not on screen (background tabs) are not rendered at all; the tab's
 * Show/Resize listeners repaint them when they come to the front.
 */
public class CanvasSvgFacadeImpl extends CanvasSvgFacade {

  private static final String DATA_KEY_RENDER_STATE =
      CanvasSvgFacadeImpl.class.getName() + ".renderState";

  /** Paint coalescing state, kept on the canvas widget (RAP widget ids are per session). */
  private static final class RenderState {
    private boolean repaintScheduled;
    private boolean repainting;
    private CanvasSvgRenderResult lastResult;
  }

  @Override
  void registerCanvasInternal(Canvas canvas, Object graph) {
    String canvasId = WidgetUtil.getId(canvas);
    CanvasGraphRegistry.getInstance().register(canvasId, canvas, graph);
    canvas.setData("canvasId", canvasId);
    canvas.setData("sessionUuid", getSessionUuidInternal());
    CanvasSvgRendererHandler.ensureRemote(canvas);
  }

  @Override
  void unregisterCanvasInternal(Canvas canvas) {
    CanvasSvgRendererHandler.unregister(canvas);
  }

  @Override
  CanvasSvgRenderResult renderPipelineInternal(
      Canvas canvas,
      PipelineCanvasSvgRenderer.Context context,
      float magnification,
      DPoint offset) {
    RenderState state = renderState(canvas);
    if (!shouldRenderNow(canvas, state)) {
      return state.lastResult;
    }
    try {
      CanvasSvgRenderResult result = PipelineCanvasSvgRenderer.render(context);
      publishSnapshotInternal(canvas, result, magnification, offset, context.canvasSize);
      state.lastResult = result;
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
    RenderState state = renderState(canvas);
    if (!shouldRenderNow(canvas, state)) {
      return state.lastResult;
    }
    try {
      CanvasSvgRenderResult result = WorkflowCanvasSvgRenderer.render(context);
      publishSnapshotInternal(canvas, result, magnification, offset, context.canvasSize);
      state.lastResult = result;
      return result;
    } catch (HopException e) {
      LogChannel.UI.logError("Failed to render workflow SVG for web canvas", e);
      return null;
    }
  }

  /**
   * Decides whether the paint that called us renders. Only the coalesced repaint scheduled below
   * does, and only for a canvas that is on screen; any other paint just makes sure that repaint is
   * scheduled and gets the previous result back, which is what the graph already holds.
   */
  private static boolean shouldRenderNow(Canvas canvas, RenderState state) {
    if (state.repainting) {
      return canvas.isVisible();
    }
    if (!state.repaintScheduled) {
      state.repaintScheduled = true;
      // Runs after the events and RAP-internal actions of this request, before the response is
      // written: RAP's Display.readAndDispatch drains async runnables inside the same request.
      canvas.getDisplay().asyncExec(new CoalescedRepaint(canvas, state));
    }
    return false;
  }

  /** The one repaint of a request that actually renders. */
  private static final class CoalescedRepaint implements Runnable {
    private final Canvas canvas;
    private final RenderState state;
    private boolean yielded;

    private CoalescedRepaint(Canvas canvas, RenderState state) {
      this.canvas = canvas;
      this.state = state;
    }

    @Override
    public void run() {
      if (canvas.isDisposed() || canvas.getData(DATA_KEY_RENDER_STATE) != state) {
        state.repaintScheduled = false;
        return;
      }
      // The graph classes redraw from asyncExec runnables of their own (updateGui, the Show
      // listener, tab activation). Those were queued behind us, so step back once and let them
      // land first: they then find the repaint still scheduled and add nothing.
      if (!yielded) {
        yielded = true;
        canvas.getDisplay().asyncExec(this);
        return;
      }
      state.repaintScheduled = false;
      state.repainting = true;
      try {
        canvas.redraw();
      } finally {
        state.repainting = false;
      }
    }
  }

  private static RenderState renderState(Canvas canvas) {
    RenderState state = (RenderState) canvas.getData(DATA_KEY_RENDER_STATE);
    if (state == null) {
      state = new RenderState();
      canvas.setData(DATA_KEY_RENDER_STATE, state);
    }
    return state;
  }

  @Override
  void publishSnapshotInternal(
      Canvas canvas,
      CanvasSvgRenderResult result,
      float magnification,
      DPoint offset,
      Point canvasSize) {
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
    CanvasGraphRegistry registry = CanvasGraphRegistry.getInstance();
    registry.updateSnapshot(canvasId, snapshot);
    Object graph = registry.getGraph(canvasId);
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
    if (graph instanceof IWebCanvasGraph webCanvasGraph) {
      webCanvasGraph.replaceAreaOwners(areaOwners);
    }
  }
}
