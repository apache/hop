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

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.hop.core.logging.LogChannel;
import org.eclipse.rap.json.JsonObject;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.service.ServiceHandler;
import org.eclipse.rap.rwt.service.UISession;

/**
 * RAP ServiceHandler that serves cached SVG renders and click-maps to the Hop Web canvas client.
 *
 * <p>URL: {@code
 * {entryPoint}?servicehandler=canvasRender&cid=...&session={uuid}&canvas={id}&rev={clientRev}}
 */
public class CanvasRenderServiceHandler implements ServiceHandler {

  public static final String SERVICE_ID = "canvasRender";

  @Override
  public void service(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    String sessionUuid = request.getParameter("session");
    String canvasId = request.getParameter("canvas");
    String revParam = request.getParameter("rev");
    long clientRev = 0;
    if (revParam != null && !revParam.isBlank()) {
      try {
        clientRev = Long.parseLong(revParam);
      } catch (NumberFormatException e) {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid rev parameter");
        return;
      }
    }

    if (!isAuthorized(sessionUuid, canvasId)) {
      response.sendError(HttpServletResponse.SC_FORBIDDEN, "Invalid session or canvas");
      return;
    }

    CanvasRenderSnapshot snapshot = CanvasGraphRegistry.getInstance().getSnapshot(canvasId);
    if (snapshot == null) {
      response.setStatus(HttpServletResponse.SC_NO_CONTENT);
      return;
    }

    if (clientRev > 0 && clientRev == snapshot.getRevision()) {
      response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
      return;
    }

    JsonObject json = snapshot.toJson();
    byte[] payload = json.toString().getBytes(StandardCharsets.UTF_8);
    response.setStatus(HttpServletResponse.SC_OK);
    response.setContentType("application/json; charset=UTF-8");
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    response.setContentLength(payload.length);
    response.getOutputStream().write(payload);
  }

  private boolean isAuthorized(String sessionUuid, String canvasId) {
    if (sessionUuid == null || sessionUuid.isBlank() || canvasId == null || canvasId.isBlank()) {
      return false;
    }
    UISession uiSession = RWT.getUISession();
    if (uiSession == null) {
      LogChannel.UI.logDebug("Canvas render rejected: no UI session");
      return false;
    }
    Object expectedUuid = uiSession.getAttribute(CanvasGraphRegistry.SESSION_UUID_ATTR);
    if (expectedUuid == null || !sessionUuid.equals(expectedUuid.toString())) {
      LogChannel.UI.logDebug("Canvas render rejected: session UUID mismatch");
      return false;
    }
    return CanvasGraphRegistry.getInstance().getCanvas(canvasId) != null;
  }
}
