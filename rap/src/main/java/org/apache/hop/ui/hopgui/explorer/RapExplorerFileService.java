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

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import java.util.Optional;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.web.ExplorerFileServing;
import org.apache.hop.ui.hopgui.perspective.explorer.web.IHopWebExplorerFileService;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.service.UISession;

/**
 * Builds origin-relative {@code /explorer-file/{token}/...} URLs for the RAP Browser widget. The
 * path shape is what gives HTML a directory base for relative CSS, images, and links.
 */
public final class RapExplorerFileService implements IHopWebExplorerFileService {

  @Override
  public String urlFor(String vfsFilename, IVariables variables) {
    if (Utils.isEmpty(vfsFilename)) {
      return null;
    }
    try {
      UISession uiSession = RWT.getUISession();
      HttpServletRequest request = RWT.getRequest();
      if (uiSession == null || request == null) {
        return null;
      }
      HttpSession httpSession = uiSession.getHttpSession();
      if (httpSession == null) {
        return null;
      }
      ExplorerPerspective perspective = ExplorerPerspective.getInstance();
      if (perspective == null || Utils.isEmpty(perspective.getRootFolder())) {
        return null;
      }
      FileObject root = HopVfs.getFileObject(perspective.getRootFolder(), variables);
      FileObject file = HopVfs.getFileObject(vfsFilename, variables);
      Optional<String> relative = ExplorerFileServing.relativePath(root, file);
      if (relative.isEmpty() || !ExplorerFileServing.isAllowedExtension(relative.get())) {
        return null;
      }
      ExplorerFileLease lease =
          ExplorerFileRegistry.getOrCreate(uiSession, httpSession.getId(), root.getName().getURI());
      return ExplorerFileServing.buildPublicPath(
          request.getContextPath(), lease.getToken(), relative.get());
    } catch (Exception e) {
      LogChannel.UI.logDebug("Could not build explorer file URL for '" + vfsFilename + "'", e);
      return null;
    }
  }
}
