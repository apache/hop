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
import org.apache.hop.core.logging.LogChannel;
import org.eclipse.rap.rwt.SingletonUtil;
import org.eclipse.rap.rwt.remote.RemoteObject;
import org.eclipse.swt.widgets.Canvas;

/**
 * Per UISession registry of pipeline/workflow canvas renders. Stores the latest SVG snapshot and
 * click-map for each canvas widget, plus one RAP remote object per canvas for the SVG overlay,
 * hover bridge, and wheel zoom.
 */
public class CanvasGraphRegistry {

  public static final String SESSION_UUID_ATTR = "hop.web.sessionUuid";

  private final Map<String, CanvasRenderSnapshot> snapshots = new ConcurrentHashMap<>();
  private final Map<String, Object> graphsByCanvasId = new ConcurrentHashMap<>();
  private final Map<String, Canvas> canvasById = new ConcurrentHashMap<>();
  private final Map<String, RemoteObject> svgRendererRemotes = new ConcurrentHashMap<>();
  private final Map<String, RemoteObject> interactionRemotes = new ConcurrentHashMap<>();
  private final Map<String, RemoteObject> zoomRemotes = new ConcurrentHashMap<>();
  private final AtomicLong revisionCounter = new AtomicLong(1);

  /** Active canvas for hover/zoom routing within this UI session. */
  private Canvas activeCanvas;

  /** Active zoom target (HopGuiPipelineGraph / HopGuiWorkflowGraph). */
  private Object activeZoomable;

  public static CanvasGraphRegistry getInstance() {
    return SingletonUtil.getSessionInstance(CanvasGraphRegistry.class);
  }

  public void register(String canvasId, Canvas canvas, Object graph) {
    if (canvas != null) {
      canvasById.put(canvasId, canvas);
    }
    graphsByCanvasId.put(canvasId, graph);
  }

  public void unregister(String canvasId) {
    Canvas canvas = canvasById.remove(canvasId);
    graphsByCanvasId.remove(canvasId);
    snapshots.remove(canvasId);
    destroyRemote(svgRendererRemotes.remove(canvasId));
    destroyRemote(interactionRemotes.remove(canvasId));
    destroyRemote(zoomRemotes.remove(canvasId));
    if (activeCanvas != null && (activeCanvas == canvas || isDisposed(activeCanvas))) {
      activeCanvas = null;
      activeZoomable = null;
    }
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

  public RemoteObject getSvgRendererRemote(String canvasId) {
    return canvasId == null ? null : svgRendererRemotes.get(canvasId);
  }

  public void putSvgRendererRemote(String canvasId, RemoteObject remoteObject) {
    svgRendererRemotes.put(canvasId, remoteObject);
  }

  public RemoteObject getInteractionRemote(String canvasId) {
    return canvasId == null ? null : interactionRemotes.get(canvasId);
  }

  public void putInteractionRemote(String canvasId, RemoteObject remoteObject) {
    interactionRemotes.put(canvasId, remoteObject);
  }

  public RemoteObject getZoomRemote(String canvasId) {
    return canvasId == null ? null : zoomRemotes.get(canvasId);
  }

  public void putZoomRemote(String canvasId, RemoteObject remoteObject) {
    zoomRemotes.put(canvasId, remoteObject);
  }

  public RemoteObject removeZoomRemote(String canvasId) {
    return canvasId == null ? null : zoomRemotes.remove(canvasId);
  }

  public Canvas getActiveCanvas() {
    return activeCanvas;
  }

  public void setActiveCanvas(Canvas activeCanvas) {
    this.activeCanvas = activeCanvas;
  }

  public Object getActiveZoomable() {
    return activeZoomable;
  }

  public void setActiveZoomable(Object activeZoomable) {
    this.activeZoomable = activeZoomable;
  }

  private static boolean isDisposed(Canvas canvas) {
    try {
      return canvas.isDisposed();
    } catch (Exception e) {
      return true;
    }
  }

  private static void destroyRemote(RemoteObject remoteObject) {
    if (remoteObject == null) {
      return;
    }
    try {
      remoteObject.destroy();
    } catch (Exception e) {
      LogChannel.UI.logDebug("Failed to destroy canvas remote object: " + e.getMessage());
    }
  }
}
