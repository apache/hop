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

import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.hopgui.CanvasSvgFacade;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.remote.Connection;
import org.eclipse.rap.rwt.remote.RemoteObject;
import org.eclipse.rap.rwt.widgets.WidgetUtil;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Widget;

/** Pushes SVG render metadata to the Hop Web canvas client. */
public class CanvasSvgRendererHandler extends Widget {

  public CanvasSvgRendererHandler(Composite parent) {
    super(parent, 0);
  }

  public static void notifyCanvasReady(Canvas canvas, long revision) {
    if (canvas == null || canvas.isDisposed()) {
      return;
    }
    CanvasGraphRegistry registry = CanvasGraphRegistry.getInstance();
    registry.setActiveCanvas(canvas);
    RemoteObject remoteObject = registry.getSvgRendererRemote();
    if (remoteObject == null) {
      remoteObject = createRemoteObject(registry, canvas);
    }
    if (remoteObject != null) {
      updateRemoteObject(remoteObject, canvas, revision);
    }
  }

  private static RemoteObject createRemoteObject(CanvasGraphRegistry registry, Canvas canvas) {
    try {
      Connection connection = RWT.getUISession().getConnection();
      RemoteObject remoteObject = connection.createRemoteObject("hop.CanvasSvgRenderer");
      remoteObject.set("self", remoteObject.getId());
      registry.setSvgRendererRemote(remoteObject);
      updateRemoteObject(remoteObject, canvas, 0);
      remoteObject.call("attachListener", null);
      return remoteObject;
    } catch (Exception e) {
      LogChannel.UI.logError("Failed to create CanvasSvgRendererHandler remote object", e);
      return null;
    }
  }

  private static void updateRemoteObject(RemoteObject remoteObject, Canvas canvas, long revision) {
    remoteObject.set("sessionUuid", CanvasSvgFacade.getSessionUuid());
    remoteObject.set("canvasId", WidgetUtil.getId(canvas));
    remoteObject.set("renderRevision", revision);
    remoteObject.set(
        "serviceHandlerUrl",
        RWT.getServiceManager().getServiceHandlerUrl(CanvasRenderServiceHandler.SERVICE_ID));
    remoteObject.call("attachListener", null);
  }
}
