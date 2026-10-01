/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.hopgui.CanvasSvgFacade;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.remote.Connection;
import org.eclipse.rap.rwt.remote.RemoteObject;
import org.eclipse.rap.rwt.widgets.WidgetUtil;
import org.eclipse.swt.widgets.Canvas;

/**
 * Pushes SVG render metadata to the Hop Web canvas client. One {@code hop.CanvasSvgRenderer} remote
 * object is created per canvas so dialogs and editor tabs can keep independent overlays.
 */
public final class CanvasSvgRendererHandler {

  private CanvasSvgRendererHandler() {}

  public static void ensureRemote(Canvas canvas) {
    if (canvas == null || canvas.isDisposed()) {
      return;
    }
    String canvasId = WidgetUtil.getId(canvas);
    CanvasGraphRegistry registry = CanvasGraphRegistry.getInstance();
    if (registry.getSvgRendererRemote(canvasId) == null) {
      createRemoteObject(registry, canvasId);
    }
  }

  public static void notifyCanvasReady(Canvas canvas, long revision) {
    if (canvas == null || canvas.isDisposed()) {
      return;
    }
    CanvasGraphRegistry registry = CanvasGraphRegistry.getInstance();
    String canvasId = WidgetUtil.getId(canvas);
    RemoteObject remoteObject = registry.getSvgRendererRemote(canvasId);
    if (remoteObject == null) {
      remoteObject = createRemoteObject(registry, canvasId);
    }
    if (remoteObject != null) {
      updateRemoteObject(remoteObject, revision);
    }
  }

  public static void unregister(Canvas canvas) {
    if (canvas == null) {
      return;
    }
    try {
      CanvasGraphRegistry.getInstance().unregister(WidgetUtil.getId(canvas));
    } catch (Exception e) {
      LogChannel.UI.logDebug(
          "Failed to unregister Canvas SVG renderer RemoteObject: " + e.getMessage());
    }
  }

  private static RemoteObject createRemoteObject(CanvasGraphRegistry registry, String canvasId) {
    try {
      Connection connection = RWT.getUISession().getConnection();
      RemoteObject remoteObject = connection.createRemoteObject("hop.CanvasSvgRenderer");
      remoteObject.set("self", remoteObject.getId());
      remoteObject.set("sessionUuid", CanvasSvgFacade.getSessionUuid());
      remoteObject.set("canvasId", canvasId);
      remoteObject.set("renderRevision", 0L);
      remoteObject.set(
          "serviceHandlerUrl",
          RWT.getServiceManager().getServiceHandlerUrl(CanvasRenderServiceHandler.SERVICE_ID));
      registry.putSvgRendererRemote(canvasId, remoteObject);
      remoteObject.call("attachListener", null);
      return remoteObject;
    } catch (Exception e) {
      LogChannel.UI.logError("Failed to create CanvasSvgRendererHandler remote object", e);
      return null;
    }
  }

  private static void updateRemoteObject(RemoteObject remoteObject, long revision) {
    remoteObject.set("sessionUuid", CanvasSvgFacade.getSessionUuid());
    remoteObject.set("renderRevision", revision);
    remoteObject.set(
        "serviceHandlerUrl",
        RWT.getServiceManager().getServiceHandlerUrl(CanvasRenderServiceHandler.SERVICE_ID));
    remoteObject.call("attachListener", null);
  }
}
