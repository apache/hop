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

package org.apache.hop.ui.hopgui.explorer;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.explorer.config.ExplorerPerspectiveConfigSingleton;
import org.apache.hop.ui.hopgui.perspective.explorer.web.ExplorerFileServing;
import org.eclipse.rap.rwt.service.UISession;

/**
 * Serves allow-listed files from the current explorer root for the RAP Browser iframe. Mapped at
 * {@code /explorer-file/*} so the document URL has a directory base for relative {@code
 * href}/{@code src}.
 */
public class ExplorerFileServlet extends HttpServlet {

  static final long DEFAULT_MAX_BYTES = 16L * 1024L * 1024L;

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    Optional<ExplorerFileServing.PathInfo> pathInfo =
        ExplorerFileServing.parsePathInfo(request.getPathInfo());
    if (pathInfo.isEmpty()) {
      notFound(response);
      return;
    }
    ExplorerFileLease lease = ExplorerFileRegistry.find(pathInfo.get().token());
    if (lease == null || !sessionMatches(request, lease)) {
      notFound(response);
      return;
    }
    UISession uiSession = lease.getUiSession();
    if (uiSession == null) {
      notFound(response);
      return;
    }

    ServeResult result = new ServeResult();
    try {
      uiSession.exec(
          () -> {
            try {
              serve(lease, pathInfo.get().relativePath(), result);
            } catch (Exception e) {
              result.error = e;
            }
          });
    } catch (Exception e) {
      LogChannel.UI.logDebug("Explorer file servlet: UI session is gone", e);
      notFound(response);
      return;
    }
    if (result.error != null) {
      LogChannel.UI.logDebug("Explorer file servlet failed to read file", result.error);
      notFound(response);
      return;
    }
    if (result.notFound) {
      notFound(response);
      return;
    }
    if (result.tooLarge) {
      response.sendError(HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE);
      return;
    }

    response.setStatus(HttpServletResponse.SC_OK);
    response.setContentType(result.contentType);
    response.setHeader("X-Content-Type-Options", "nosniff");
    response.setHeader("Cache-Control", "private, no-store");
    if (result.contentType != null && result.contentType.startsWith("application/pdf")) {
      response.setHeader("Content-Disposition", "inline");
    }
    if (result.body != null) {
      response.setContentLength(result.body.length);
      response.getOutputStream().write(result.body);
    }
  }

  private static void serve(ExplorerFileLease lease, String relativePath, ServeResult result)
      throws Exception {
    IVariables variables = variables();
    FileObject root = HopVfs.getFileObject(lease.getRootVfsUri(), variables);
    Optional<FileObject> file = ExplorerFileServing.resolveUnderRoot(root, relativePath);
    if (file.isEmpty()) {
      result.notFound = true;
      return;
    }
    Optional<String> contentType = ExplorerFileServing.contentType(relativePath);
    if (contentType.isEmpty()) {
      result.notFound = true;
      return;
    }
    long size = file.get().getContent().getSize();
    long maxBytes = maxBytes();
    if (size > maxBytes) {
      result.tooLarge = true;
      return;
    }
    try (InputStream in = HopVfs.getInputStream(file.get())) {
      result.body = in.readAllBytes();
    }
    result.contentType = contentType.get();
  }

  private static boolean sessionMatches(HttpServletRequest request, ExplorerFileLease lease) {
    HttpSession session = request.getSession(false);
    return session != null && lease.getHttpSessionId().equals(session.getId());
  }

  private static IVariables variables() {
    HopGui hopGui = HopGui.peekInstance();
    return hopGui != null ? hopGui.getVariables() : new Variables();
  }

  static long maxBytes() {
    try {
      String option = ExplorerPerspectiveConfigSingleton.getConfig().getFileLoadingMaxSize();
      HopGui hopGui = HopGui.peekInstance();
      String resolved = hopGui != null ? hopGui.getVariables().resolve(option) : option;
      return Const.toLong(resolved, 16) * 1024L * 1024L;
    } catch (Exception e) {
      return DEFAULT_MAX_BYTES;
    }
  }

  private static void notFound(HttpServletResponse response) throws IOException {
    response.sendError(HttpServletResponse.SC_NOT_FOUND);
  }

  private static final class ServeResult {
    byte[] body;
    String contentType;
    boolean notFound;
    boolean tooLarge;
    Exception error;
  }
}
