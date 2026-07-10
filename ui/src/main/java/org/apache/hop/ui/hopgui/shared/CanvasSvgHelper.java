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

package org.apache.hop.ui.hopgui.shared;

import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.hopgui.CanvasSvgFacade;
import org.eclipse.swt.widgets.Canvas;

/** Helper for notifying the Hop Web SVG canvas client when the active tab changes. */
public final class CanvasSvgHelper {

  private CanvasSvgHelper() {}

  public static void notifyCanvasReady(Canvas canvas) {
    if (canvas == null || canvas.isDisposed()) {
      return;
    }
    try {
      String canvasId =
          (String)
              Class.forName("org.eclipse.rap.rwt.widgets.WidgetUtil")
                  .getMethod("getId", org.eclipse.swt.widgets.Widget.class)
                  .invoke(null, canvas);
      long revision = CanvasSvgFacade.getRevision(canvasId);
      if (revision == 0) {
        canvas.getDisplay().asyncExec(canvas::redraw);
      }
      Class<?> handlerClass =
          Class.forName("org.apache.hop.ui.hopgui.canvas.CanvasSvgRendererHandler");
      handlerClass
          .getMethod("notifyCanvasReady", Canvas.class, long.class)
          .invoke(null, canvas, revision);
    } catch (ClassNotFoundException e) {
      // Desktop build without RAP fragment
    } catch (Exception e) {
      LogChannel.UI.logError("Failed to notify SVG canvas ready", e);
    }
  }
}
