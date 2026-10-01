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

package org.apache.hop.ui.hopgui.file.delegates;

import java.util.Objects;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.AreaOwner.AreaType;
import org.apache.hop.core.gui.markdown.NoteLinkHit;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.HopFileTypeRegistry;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.widgets.Shell;

/**
 * Shared navigation for Markdown note hyperlinks on pipeline and workflow canvases. Handles HTTP(S)
 * URLs and Hop file paths (absolute or relative to the host pipeline/workflow file).
 */
public final class HopGuiNoteLinkSupport {

  private static final Class<?> PKG = HopGuiNoteLinkSupport.class;

  private HopGuiNoteLinkSupport() {}

  public static NoteLinkHit linkHitFrom(AreaOwner areaOwner) {
    if (areaOwner == null || areaOwner.getAreaType() != AreaType.NOTE_LINK) {
      return null;
    }
    if (areaOwner.getOwner() instanceof NoteLinkHit hit) {
      return hit;
    }
    return null;
  }

  public static boolean noteLinksEqual(NoteLinkHit a, NoteLinkHit b) {
    if (a == b) {
      return true;
    }
    if (a == null || b == null) {
      return false;
    }
    return a.note() == b.note()
        && Objects.equals(a.target(), b.target())
        && Objects.equals(a.label(), b.label());
  }

  public static boolean isUrlTarget(String target) {
    if (Utils.isEmpty(target)) {
      return false;
    }
    String trimmed = target.trim();
    return trimmed.regionMatches(true, 0, "http://", 0, 7)
        || trimmed.regionMatches(true, 0, "https://", 0, 8);
  }

  /**
   * Resolve a note link target to an absolute/path or URL string after variable substitution.
   * Relative paths are resolved against the directory of {@code baseFilename} when present.
   */
  public static String resolveTarget(IVariables variables, String baseFilename, String target)
      throws HopException {
    if (Utils.isEmpty(target)) {
      return target;
    }
    String resolved = variables != null ? variables.resolve(target.trim()) : target.trim();
    if (Utils.isEmpty(resolved) || isUrlTarget(resolved)) {
      return resolved;
    }
    // file:// scheme → use path for Hop open when possible
    if (resolved.regionMatches(true, 0, "file:", 0, 5)) {
      try {
        FileObject file = HopVfs.getFileObject(resolved, variables);
        return file.getName().getURI();
      } catch (Exception e) {
        return resolved;
      }
    }
    // Absolute VFS/local paths: leave as-is (Unix /, Windows drive, scheme:...)
    if (resolved.startsWith("/")
        || resolved.startsWith("\\")
        || resolved.matches("^[A-Za-z]:[\\\\/].*")
        || resolved.contains("://")) {
      return resolved;
    }
    if (Utils.isEmpty(baseFilename)) {
      return resolved;
    }
    try {
      String base = variables != null ? variables.resolve(baseFilename) : baseFilename;
      FileObject baseFile = HopVfs.getFileObject(base, variables);
      FileObject parent = baseFile.getParent();
      if (parent == null) {
        return resolved;
      }
      FileObject targetFile = parent.resolveFile(resolved);
      return targetFile.getName().getURI();
    } catch (Exception e) {
      throw new HopException(
          "Unable to resolve note link '" + target + "' relative to '" + baseFilename + "'", e);
    }
  }

  /**
   * Follow a note hyperlink: open HTTP(S) in the browser, otherwise open a Hop file when a file
   * type handles it.
   *
   * @return true if the link was handled (including showing an error dialog)
   */
  public static boolean followLink(
      HopGui hopGui, IVariables variables, String baseFilename, NoteLinkHit linkHit) {
    if (hopGui == null || linkHit == null || Utils.isEmpty(linkHit.target())) {
      return false;
    }
    Shell shell = hopGui.getShell();
    String rawTarget = linkHit.target().trim();
    try {
      if (isUrlTarget(rawTarget) || isUrlTarget(variables.resolve(rawTarget))) {
        String url = variables.resolve(rawTarget);
        EnvironmentUtils.getInstance().openUrl(url);
        return true;
      }

      String path = resolveTarget(variables, baseFilename, rawTarget);
      HopFileTypeRegistry.getInstance().ensureLoaded();
      IHopFileType fileType = HopFileTypeRegistry.getInstance().findHopFileType(path);
      if (fileType == null) {
        // Fallback: try browser for unknown absolute URLs we might have missed
        if (path != null && path.contains("://") && !path.regionMatches(true, 0, "file:", 0, 5)) {
          EnvironmentUtils.getInstance().openUrl(path);
          return true;
        }
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "HopGuiNoteLinkSupport.Error.Title"),
            BaseMessages.getString(PKG, "HopGuiNoteLinkSupport.Error.UnknownType", path),
            new HopException("No Hop file type for: " + path));
        return true;
      }
      hopGui.fileDelegate.fileOpen(path);
      return true;
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "HopGuiNoteLinkSupport.Error.Title"),
          BaseMessages.getString(PKG, "HopGuiNoteLinkSupport.Error.Message", rawTarget),
          e);
      return true;
    }
  }

  public static String tooltipFor(NoteLinkHit hit) {
    if (hit == null || Utils.isEmpty(hit.target())) {
      return null;
    }
    return hit.target().trim();
  }
}
