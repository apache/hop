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

import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.hopgui.canvas.CanvasGraphRegistry;
import org.eclipse.rap.json.JsonObject;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.remote.AbstractOperationHandler;
import org.eclipse.rap.rwt.remote.Connection;
import org.eclipse.rap.rwt.remote.RemoteObject;
import org.eclipse.rap.rwt.widgets.WidgetUtil;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Widget;

public class CanvasZoomHandler extends Widget {

  private final IZoomable zoomable;
  private final Canvas canvas;

  public interface IZoomable {
    void zoomIn(MouseEvent mouseEvent);

    void zoomOut(MouseEvent mouseEvent);
  }

  public CanvasZoomHandler(Composite parent, Canvas canvas, IZoomable zoomable) {
    super(parent, 0);
    this.zoomable = zoomable;
    this.canvas = canvas;
  }

  public void notifyCanvasReady() {
    if (isDisposed() || canvas == null || canvas.isDisposed()) {
      return;
    }

    CanvasGraphRegistry registry = CanvasGraphRegistry.getInstance();
    registry.setActiveCanvas(canvas);
    registry.setActiveZoomable(zoomable);

    RemoteObject remoteObject = registry.getZoomRemote();
    if (remoteObject == null) {
      createRemoteObject(registry);
    } else {
      updateCanvas(remoteObject);
    }
  }

  private void createRemoteObject(CanvasGraphRegistry registry) {
    try {
      Connection connection = RWT.getUISession().getConnection();
      Canvas currentCanvas = registry.getActiveCanvas();

      RemoteObject remoteObject = connection.createRemoteObject("hop.CanvasZoom");
      remoteObject.set("self", remoteObject.getId());
      remoteObject.set("canvas", WidgetUtil.getId(currentCanvas));

      remoteObject.setHandler(
          new AbstractOperationHandler() {
            @Override
            public void handleNotify(String event, JsonObject properties) {
              if ("zoom".equals(event)) {
                handleZoom(registry, properties);
              }
            }
          });

      remoteObject.listen("zoom", true);
      registry.setZoomRemote(remoteObject);

      getDisplay()
          .timerExec(
              50,
              () -> {
                RemoteObject ro = registry.getZoomRemote();
                if (ro != null) {
                  ro.call("attachListener", null);
                }
              });

    } catch (Exception e) {
      LogChannel.UI.logError("Failed to create CanvasZoomHandler remote object: ", e);
    }
  }

  private static void handleZoom(CanvasGraphRegistry registry, JsonObject properties) {
    Canvas currentCanvas = registry.getActiveCanvas();
    Object zoomable = registry.getActiveZoomable();
    if (currentCanvas == null || zoomable == null) {
      return;
    }
    if (!(zoomable instanceof IZoomable activeZoomable)) {
      return;
    }

    int count = properties.get("count").asInt();
    int x = properties.get("x").asInt();
    int y = properties.get("y").asInt();

    org.eclipse.swt.widgets.Event swtEvent = new org.eclipse.swt.widgets.Event();
    swtEvent.widget = currentCanvas;
    swtEvent.x = x;
    swtEvent.y = y;
    swtEvent.count = count;
    MouseEvent mouseEvent = new MouseEvent(swtEvent);

    if (count > 0) {
      activeZoomable.zoomIn(mouseEvent);
    } else {
      activeZoomable.zoomOut(mouseEvent);
    }
  }

  private void updateCanvas(RemoteObject remoteObject) {
    remoteObject.set("canvas", WidgetUtil.getId(canvas));
    remoteObject.call("attachListener", null);
  }

  @Override
  public void dispose() {
    CanvasGraphRegistry registry = CanvasGraphRegistry.getInstance();
    if (registry.getActiveCanvas() == this.canvas) {
      registry.setActiveCanvas(null);
      registry.setActiveZoomable(null);
    }
    super.dispose();
  }
}
