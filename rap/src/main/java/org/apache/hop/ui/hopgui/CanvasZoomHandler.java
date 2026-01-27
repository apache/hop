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

  // Singleton: ONE remote object per UI session, reused for all canvases
  private static RemoteObject globalRemoteObject;
  private static IZoomable currentZoomable;
  private static Canvas currentCanvas;
  private static final Object sessionLock = new Object();

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
    if (isDisposed()) {
      return;
    }

    synchronized (sessionLock) {
      // Update the current canvas and zoomable
      currentCanvas = this.canvas;
      currentZoomable = this.zoomable;

      // Create remote object only once per session
      if (globalRemoteObject == null) {
        createGlobalRemoteObject();
      } else {
        updateCanvas();
      }
    }
  }

  private void createGlobalRemoteObject() {
    try {
      Connection connection = RWT.getUISession().getConnection();

      // Create ONE remote object for the entire session
      globalRemoteObject = connection.createRemoteObject("hop.CanvasZoom");
      globalRemoteObject.set("self", globalRemoteObject.getId());

      String canvasId = org.eclipse.rap.rwt.widgets.WidgetUtil.getId(currentCanvas);
      globalRemoteObject.set("canvas", canvasId);

      globalRemoteObject.setHandler(
          new AbstractOperationHandler() {
            @Override
            public void handleNotify(String event, JsonObject properties) {
              if ("zoom".equals(event)) {
                synchronized (sessionLock) {
                  // Use the current canvas and zoomable (may have changed since creation)
                  if (currentCanvas == null || currentZoomable == null) {
                    return;
                  }

                  int count = properties.get("count").asInt();
                  int x = properties.get("x").asInt();
                  int y = properties.get("y").asInt();

                  // Create a mouse event with Event object
                  org.eclipse.swt.widgets.Event swtEvent = new org.eclipse.swt.widgets.Event();
                  swtEvent.widget = currentCanvas;
                  swtEvent.x = x;
                  swtEvent.y = y;
                  swtEvent.count = count;
                  MouseEvent mouseEvent = new MouseEvent(swtEvent);

                  // Call the appropriate zoom method on the current zoomable
                  if (count > 0) {
                    currentZoomable.zoomIn(mouseEvent);
                  } else {
                    currentZoomable.zoomOut(mouseEvent);
                  }
                }
              }
            }
          });

      globalRemoteObject.listen("zoom", true);

      // Signal the client to attach the wheel listener
      getDisplay()
          .timerExec(
              50,
              () -> {
                if (globalRemoteObject != null) {
                  globalRemoteObject.call("attachListener", null);
                }
              });

    } catch (Exception e) {
      LogChannel.UI.logError("Failed to create CanvasZoomHandler remote object: ", e);
    }
  }

  private void updateCanvas() {
    // Update which canvas the remote object is attached to
    String canvasId = WidgetUtil.getId(currentCanvas);

    // Update the canvas property - this will trigger property change on client
    globalRemoteObject.set("canvas", canvasId);

    // Then call attachListener to reattach to the new canvas
    globalRemoteObject.call("attachListener", null);
  }

  @Override
  public void dispose() {
    synchronized (sessionLock) {
      // If this handler was the current one, clear the current references
      if (currentCanvas == this.canvas) {
        currentCanvas = null;
        currentZoomable = null;
      }
    }
    // Note: Don't destroy globalRemoteObject - it's shared across all canvases in the session
    super.dispose();
  }
}
