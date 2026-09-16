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

package org.apache.hop.ui.core.dialog;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.hopgui.canvas.AreaOwnerJsonSerializer;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.remote.Connection;
import org.eclipse.rap.rwt.remote.RemoteObject;
import org.eclipse.rap.rwt.widgets.WidgetUtil;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Canvas;

/** Manages the client-side SVG renderer remote object for Hop Web ContextDialog. */
public final class ContextDialogSvgRendererHandler {

  private static final Map<String, RemoteObject> remoteObjects = new ConcurrentHashMap<>();
  private static final Set<String> renderingCanvases = ConcurrentHashMap.newKeySet();

  private ContextDialogSvgRendererHandler() {}

  public static void register(Canvas canvas, ContextDialog dialog) {
    if (canvas == null || canvas.isDisposed()) {
      return;
    }
    try {
      String canvasId = WidgetUtil.getId(canvas);
      Connection connection = RWT.getUISession().getConnection();
      RemoteObject remoteObject = connection.createRemoteObject("hop.ContextDialogSvgRenderer");
      remoteObject.set("canvasId", canvasId);
      if (dialog.getTooltipLabel() != null && !dialog.getTooltipLabel().isDisposed()) {
        remoteObject.set("tooltipId", WidgetUtil.getId(dialog.getTooltipLabel()));
      }
      remoteObjects.put(canvasId, remoteObject);
    } catch (Exception e) {
      LogChannel.UI.logError("Failed to register ContextDialog SVG renderer RemoteObject", e);
    }
  }

  public static void unregister(Canvas canvas) {
    if (canvas == null) {
      return;
    }
    try {
      String canvasId = WidgetUtil.getId(canvas);
      renderingCanvases.remove(canvasId);
      RemoteObject remoteObject = remoteObjects.remove(canvasId);
      if (remoteObject != null) {
        remoteObject.destroy();
      }
    } catch (Exception e) {
      LogChannel.UI.logDebug(
          "Failed to unregister ContextDialog SVG renderer RemoteObject: " + e.getMessage());
    }
  }

  public static void renderAndPublish(Canvas canvas, ContextDialog dialog) {
    if (canvas == null || canvas.isDisposed() || dialog == null) {
      return;
    }
    String canvasId = WidgetUtil.getId(canvas);
    if (!renderingCanvases.add(canvasId)) {
      return;
    }
    try {
      RemoteObject remoteObject = remoteObjects.get(canvasId);
      if (remoteObject == null) {
        register(canvas, dialog);
        remoteObject = remoteObjects.get(canvasId);
      }
      if (remoteObject == null) {
        return;
      }

      int areaWidth = 800;
      int areaHeight = 600;
      if (dialog.getScrolledComposite() != null && !dialog.getScrolledComposite().isDisposed()) {
        Rectangle area = dialog.getScrolledComposite().getClientArea();
        if (area.width > 0) {
          areaWidth = area.width;
        }
        if (area.height > 0) {
          areaHeight = area.height;
        }
      }

      ContextDialogSvgRenderResult result =
          ContextDialogSvgRenderer.render(dialog, areaWidth, areaHeight);

      dialog.setAreaOwners(result.areaOwners());
      dialog.updateContentHeight(result.totalContentHeight());

      if (dialog.getScrolledComposite() != null && !dialog.getScrolledComposite().isDisposed()) {
        Rectangle newArea = dialog.getScrolledComposite().getClientArea();
        if (newArea.width > 0 && newArea.width != areaWidth) {
          areaWidth = newArea.width;
          result = ContextDialogSvgRenderer.render(dialog, areaWidth, areaHeight);
          dialog.setAreaOwners(result.areaOwners());
          dialog.updateContentHeight(result.totalContentHeight());
        }
      }

      remoteObject.set("contentHeight", result.totalContentHeight());
      remoteObject.set("svg", result.svg());
      remoteObject.set("areas", AreaOwnerJsonSerializer.toJsonArray(result.areaOwners()));
      if (dialog.getSelectedItem() != null && dialog.getSelectedItem().getAction() != null) {
        remoteObject.set(
            "selectedTooltip", Const.NVL(dialog.getSelectedItem().getAction().getTooltip(), ""));
      } else {
        remoteObject.set("selectedTooltip", "");
      }
    } catch (Exception e) {
      LogChannel.UI.logError("Failed to render and publish ContextDialog SVG", e);
    } finally {
      renderingCanvases.remove(canvasId);
    }
  }
}
