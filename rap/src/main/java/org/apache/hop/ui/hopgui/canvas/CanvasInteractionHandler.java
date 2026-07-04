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

import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.eclipse.rap.json.JsonObject;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.remote.AbstractOperationHandler;
import org.eclipse.rap.rwt.remote.Connection;
import org.eclipse.rap.rwt.remote.RemoteObject;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Widget;

/**
 * Remote object bridge for client-side canvas hover notifications. Mouse clicks continue to use the
 * standard RAP SWT listener path with server-side {@link AreaOwner} hit testing.
 */
public class CanvasInteractionHandler extends Widget {

  public CanvasInteractionHandler(Composite parent) {
    super(parent, 0);
  }

  public static void ensureRemoteObject(Canvas canvas) {
    if (canvas == null || canvas.isDisposed()) {
      return;
    }
    CanvasGraphRegistry registry = CanvasGraphRegistry.getInstance();
    registry.setActiveCanvas(canvas);
    RemoteObject remoteObject = registry.getInteractionRemote();
    if (remoteObject == null) {
      createRemoteObject(registry, canvas);
    } else {
      updateCanvas(remoteObject, canvas);
    }
  }

  private static void createRemoteObject(CanvasGraphRegistry registry, Canvas canvas) {
    try {
      Connection connection = RWT.getUISession().getConnection();
      RemoteObject remoteObject = connection.createRemoteObject("hop.CanvasInteraction");
      remoteObject.set("self", remoteObject.getId());
      remoteObject.set("canvas", org.eclipse.rap.rwt.widgets.WidgetUtil.getId(canvas));
      remoteObject.setHandler(
          new AbstractOperationHandler() {
            @Override
            public void handleNotify(String event, JsonObject properties) {
              if ("hover".equals(event)) {
                handleHover(properties);
              }
            }
          });
      remoteObject.listen("hover", true);
      registry.setInteractionRemote(remoteObject);
      remoteObject.call("attachListener", null);
    } catch (Exception e) {
      LogChannel.UI.logError("Failed to create CanvasInteractionHandler remote object", e);
    }
  }

  private static void updateCanvas(RemoteObject remoteObject, Canvas canvas) {
    remoteObject.set("canvas", org.eclipse.rap.rwt.widgets.WidgetUtil.getId(canvas));
    remoteObject.call("attachListener", null);
  }

  private static void handleHover(JsonObject properties) {
    CanvasGraphRegistry registry = CanvasGraphRegistry.getInstance();
    if (registry.getActiveCanvas() == null) {
      return;
    }
    String canvasId = properties.get("canvasId").asString();
    Object graph = registry.getGraph(canvasId);
    if (graph == null) {
      return;
    }
    int graphX = properties.get("graphX").asInt();
    int graphY = properties.get("graphY").asInt();
    int screenX = properties.get("screenX") != null ? properties.get("screenX").asInt() : graphX;
    int screenY = properties.get("screenY") != null ? properties.get("screenY").asInt() : graphY;
    if (graph instanceof HopGuiPipelineGraph pipelineGraph) {
      pipelineGraph.handleWebCanvasHover(graphX, graphY, screenX, screenY);
    } else if (graph instanceof HopGuiWorkflowGraph workflowGraph) {
      workflowGraph.handleWebCanvasHover(graphX, graphY, screenX, screenY);
    }
  }
}
