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

package org.apache.hop.ui.hopgui.canvas;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.rap.rwt.SingletonUtil;
import org.eclipse.rap.rwt.remote.RemoteObject;
import org.eclipse.swt.widgets.Canvas;

/**
 * Per UISession registry of pipeline/workflow canvas renders. Stores the latest SVG snapshot and
 * click-map for each canvas widget.
 */
public class CanvasGraphRegistry {

  public static final String SESSION_UUID_ATTR = "hop.web.sessionUuid";

  private final Map<String, CanvasRenderSnapshot> snapshots = new ConcurrentHashMap<>();
  private final Map<String, Object> graphsByCanvasId = new ConcurrentHashMap<>();
  private final Map<String, Canvas> canvasById = new ConcurrentHashMap<>();
  private final AtomicLong revisionCounter = new AtomicLong(1);

  /** Per-session remote object for the SVG canvas client (must not be static across sessions). */
  private RemoteObject svgRendererRemote;

  /** Per-session remote object for canvas hover notifications. */
  private RemoteObject interactionRemote;

  /** Active canvas for hover routing within this UI session. */
  private Canvas activeCanvas;

  /** Per-session remote object for mouse-wheel zoom. */
  private RemoteObject zoomRemote;

  /** Active zoom target (HopGuiPipelineGraph / HopGuiWorkflowGraph). */
  private Object activeZoomable;

  public static CanvasGraphRegistry getInstance() {
    return SingletonUtil.getSessionInstance(CanvasGraphRegistry.class);
  }

  public void register(String canvasId, Canvas canvas, Object graph) {
    canvasById.put(canvasId, canvas);
    graphsByCanvasId.put(canvasId, graph);
  }

  public void unregister(String canvasId) {
    canvasById.remove(canvasId);
    graphsByCanvasId.remove(canvasId);
    snapshots.remove(canvasId);
  }

  public void updateSnapshot(String canvasId, CanvasRenderSnapshot snapshot) {
    snapshots.put(canvasId, snapshot);
  }

  public CanvasRenderSnapshot getSnapshot(String canvasId) {
    return snapshots.get(canvasId);
  }

  public Object getGraph(String canvasId) {
    return graphsByCanvasId.get(canvasId);
  }

  public Canvas getCanvas(String canvasId) {
    return canvasById.get(canvasId);
  }

  public long nextRevision() {
    return revisionCounter.incrementAndGet();
  }

  public long getCurrentRevision(String canvasId) {
    CanvasRenderSnapshot snapshot = snapshots.get(canvasId);
    return snapshot == null ? 0 : snapshot.getRevision();
  }

  public RemoteObject getSvgRendererRemote() {
    return svgRendererRemote;
  }

  public void setSvgRendererRemote(RemoteObject svgRendererRemote) {
    this.svgRendererRemote = svgRendererRemote;
  }

  public RemoteObject getInteractionRemote() {
    return interactionRemote;
  }

  public void setInteractionRemote(RemoteObject interactionRemote) {
    this.interactionRemote = interactionRemote;
  }

  public Canvas getActiveCanvas() {
    return activeCanvas;
  }

  public void setActiveCanvas(Canvas activeCanvas) {
    this.activeCanvas = activeCanvas;
  }

  public RemoteObject getZoomRemote() {
    return zoomRemote;
  }

  public void setZoomRemote(RemoteObject zoomRemote) {
    this.zoomRemote = zoomRemote;
  }

  public Object getActiveZoomable() {
    return activeZoomable;
  }

  public void setActiveZoomable(Object activeZoomable) {
    this.activeZoomable = activeZoomable;
  }
}
