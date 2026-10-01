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

import java.util.Objects;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.hopgui.canvas.AreaOwnerJsonSerializer;
import org.eclipse.rap.json.JsonArray;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.remote.Connection;
import org.eclipse.rap.rwt.remote.RemoteObject;
import org.eclipse.rap.rwt.widgets.WidgetUtil;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Canvas;

/**
 * Publishes the {@link ContextDialog} item list to the browser as one SVG document (see
 * context-dialog-svg.js).
 *
 * <p>RAP has no paint coalescing: every {@code Canvas.redraw()} and every resize of the canvas
 * fires {@code SWT.Paint} synchronously. Opening the dialog paints a dozen times in a single
 * request ({@code filter()} + {@code selectItem()}, then RAP's text-size measurement enlarging and
 * restoring every shell by 1000px), and rendering a few hundred icons to SVG is expensive enough
 * that on a slow host each of those requests exceeded RAP's 1s wait-hint timeout and greyed out the
 * whole UI (issue #8435). So a paint only <em>schedules</em> a render: one {@code asyncExec} per
 * request, executed once the event queue has drained and before the response is written. What it
 * produces is only sent to the client when it differs from what the client already has, so the
 * measurement round trips that follow the open carry no SVG at all.
 *
 * <p>All state lives on the canvas widget itself: RAP widget ids are per session, so a static map
 * keyed by id would let two users' dialogs overwrite each other.
 */
public final class ContextDialogSvgRendererHandler {

  private static final String DATA_KEY_STATE = ContextDialogSvgRendererHandler.class.getName();

  /** What this canvas has on the client, and whether a render is pending or running. */
  private static final class State {
    private RemoteObject remoteObject;
    private boolean renderScheduled;
    private boolean rendering;
    private int publishedContentHeight = -1;
    private String publishedSvg;
    private String publishedAreas;
    private String publishedTooltip;
  }

  private ContextDialogSvgRendererHandler() {}

  public static void register(Canvas canvas, ContextDialog dialog) {
    if (canvas == null || canvas.isDisposed()) {
      return;
    }
    try {
      State state = getOrCreateState(canvas);
      if (state.remoteObject != null) {
        return;
      }
      Connection connection = RWT.getUISession().getConnection();
      RemoteObject remoteObject = connection.createRemoteObject("hop.ContextDialogSvgRenderer");
      remoteObject.set("canvasId", WidgetUtil.getId(canvas));
      if (dialog.getTooltipLabel() != null && !dialog.getTooltipLabel().isDisposed()) {
        remoteObject.set("tooltipId", WidgetUtil.getId(dialog.getTooltipLabel()));
      }
      state.remoteObject = remoteObject;
    } catch (Exception e) {
      LogChannel.UI.logError("Failed to register ContextDialog SVG renderer RemoteObject", e);
    }
  }

  public static void unregister(Canvas canvas) {
    if (canvas == null || canvas.isDisposed()) {
      return;
    }
    try {
      State state = (State) canvas.getData(DATA_KEY_STATE);
      canvas.setData(DATA_KEY_STATE, null);
      if (state != null && state.remoteObject != null) {
        state.remoteObject.destroy();
        state.remoteObject = null;
      }
    } catch (Exception e) {
      LogChannel.UI.logDebug(
          "Failed to unregister ContextDialog SVG renderer RemoteObject: " + e.getMessage());
    }
  }

  /**
   * Called from every {@code SWT.Paint} of the dialog canvas. Schedules a single render for the
   * current request instead of rendering on the spot.
   */
  public static void renderAndPublish(Canvas canvas, ContextDialog dialog) {
    if (canvas == null || canvas.isDisposed() || dialog == null) {
      return;
    }
    State state = getOrCreateState(canvas);
    // A paint caused by our own render (the canvas is resized to the content height) or one that
    // is already covered by a scheduled render: nothing to do.
    if (state.rendering || state.renderScheduled) {
      return;
    }
    state.renderScheduled = true;
    canvas
        .getDisplay()
        .asyncExec(
            () -> {
              state.renderScheduled = false;
              // Unregistered (dialog closing) in the meantime: the state is gone from the canvas.
              if (canvas.isDisposed()
                  || dialog.isDisposed()
                  || canvas.getData(DATA_KEY_STATE) != state) {
                return;
              }
              render(canvas, dialog, state);
            });
  }

  private static void render(Canvas canvas, ContextDialog dialog, State state) {
    state.rendering = true;
    try {
      if (state.remoteObject == null) {
        register(canvas, dialog);
      }
      if (state.remoteObject == null) {
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

      // Applying the content height can show or hide the vertical scrollbar, which changes the
      // width the icons flow into: render once more at the final width.
      if (dialog.getScrolledComposite() != null && !dialog.getScrolledComposite().isDisposed()) {
        Rectangle newArea = dialog.getScrolledComposite().getClientArea();
        if (newArea.width > 0 && newArea.width != areaWidth) {
          areaWidth = newArea.width;
          result = ContextDialogSvgRenderer.render(dialog, areaWidth, areaHeight);
          dialog.setAreaOwners(result.areaOwners());
          dialog.updateContentHeight(result.totalContentHeight());
        }
      }

      publish(state, result, selectedTooltip(dialog));
    } catch (Exception e) {
      LogChannel.UI.logError("Failed to render and publish ContextDialog SVG", e);
    } finally {
      state.rendering = false;
    }
  }

  /** Sends only the properties that changed since the client last received them. */
  private static void publish(State state, ContextDialogSvgRenderResult result, String tooltip) {
    RemoteObject remoteObject = state.remoteObject;
    if (result.totalContentHeight() != state.publishedContentHeight) {
      remoteObject.set("contentHeight", result.totalContentHeight());
      state.publishedContentHeight = result.totalContentHeight();
    }
    if (!Objects.equals(result.svg(), state.publishedSvg)) {
      remoteObject.set("svg", result.svg());
      state.publishedSvg = result.svg();
    }
    JsonArray areas = AreaOwnerJsonSerializer.toJsonArray(result.areaOwners());
    String areasJson = areas.toString();
    if (!areasJson.equals(state.publishedAreas)) {
      remoteObject.set("areas", areas);
      state.publishedAreas = areasJson;
    }
    if (!tooltip.equals(state.publishedTooltip)) {
      remoteObject.set("selectedTooltip", tooltip);
      state.publishedTooltip = tooltip;
    }
  }

  private static String selectedTooltip(ContextDialog dialog) {
    if (dialog.getSelectedItem() != null && dialog.getSelectedItem().getAction() != null) {
      return Const.NVL(dialog.getSelectedItem().getAction().getTooltip(), "");
    }
    return "";
  }

  private static State getOrCreateState(Canvas canvas) {
    State state = (State) canvas.getData(DATA_KEY_STATE);
    if (state == null) {
      state = new State();
      canvas.setData(DATA_KEY_STATE, state);
    }
    return state;
  }
}
