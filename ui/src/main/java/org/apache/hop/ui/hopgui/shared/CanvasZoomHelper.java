/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui.shared;

import java.lang.reflect.Constructor;
import java.lang.reflect.Proxy;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.hopgui.perspective.execution.DragViewZoomBase;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;

/**
 * Helper class to create CanvasZoomHandler for web environments using reflection to avoid direct
 * module dependencies.
 */
public class CanvasZoomHelper {

  private CanvasZoomHelper() {}

  /**
   * Creates a zoom handler for the given canvas in web/RAP environments. Uses reflection to load
   * the CanvasZoomHandler from the RAP module to avoid compile-time dependencies.
   *
   * @param parent The parent composite (typically the graph instance)
   * @param canvas The canvas to attach zoom handling to
   * @param zoomable The object that implements zoom methods (zoomIn/zoomOut)
   * @return The created zoom handler instance (as Object to avoid direct dependency)
   */
  public static Object createZoomHandler(
      Composite parent, Canvas canvas, DragViewZoomBase zoomable) {
    try {
      Class<?> zoomHandlerClass = Class.forName("org.apache.hop.ui.hopgui.CanvasZoomHandler");
      Class<?> zoomableInterface =
          Class.forName("org.apache.hop.ui.hopgui.CanvasZoomHandler$IZoomable");

      Object zoomableProxy =
          Proxy.newProxyInstance(
              CanvasZoomHelper.class.getClassLoader(),
              new Class<?>[] {zoomableInterface},
              (proxy, method, args) ->
                  switch (method.getName()) {
                    case "zoomIn" -> {
                      zoomable.zoomIn((MouseEvent) args[0]);
                      yield null;
                    }
                    case "zoomOut" -> {
                      zoomable.zoomOut((MouseEvent) args[0]);
                      yield null;
                    }
                    default -> null;
                  });

      Constructor<?> constructor =
          zoomHandlerClass.getConstructor(
              org.eclipse.swt.widgets.Composite.class,
              org.eclipse.swt.widgets.Canvas.class,
              zoomableInterface);
      return constructor.newInstance(parent, canvas, zoomableProxy);
    } catch (Exception e) {
      LogChannel.UI.logError("Failed to create CanvasZoomHandler", e);
      return null;
    }
  }

  /**
   * Notifies the zoom handler that the canvas is ready for listener attachment. This should be
   * called after the canvas is visible and ready to receive events.
   *
   * @param zoomHandler The zoom handler instance (created by createZoomHandler)
   */
  public static void notifyCanvasReady(Object zoomHandler) {
    if (zoomHandler == null) {
      return;
    }
    try {
      zoomHandler.getClass().getMethod("notifyCanvasReady").invoke(zoomHandler);
    } catch (Exception e) {
      LogChannel.UI.logError("Failed to notify canvas ready", e);
    }
  }
}
