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
package org.apache.hop.lint;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;

/** Resolves which Hop file to lint: Explorer selection first, then the active editor tab. */
public final class LintFileSelection {

  private LintFileSelection() {}

  public static String resolveLintFilePath() {
    try {
      // When the user is working in the Metadata perspective, lint the metadata item they have
      // open rather than falling through to the last-active pipeline/workflow editor.
      if (LintMetadataSelection.isMetadataPerspectiveActive()) {
        String fromMetadata = LintMetadataSelection.resolveActiveMetadataPath();
        if (!Utils.isEmpty(fromMetadata)) {
          return fromMetadata;
        }
      }
      String fromExplorer = resolveFromExplorerSelection();
      if (!Utils.isEmpty(fromExplorer)) {
        return fromExplorer;
      }
      return resolveFromActiveEditor();
    } catch (Exception | Error e) {
      // Reaching the HopGui singleton fails hard when the UI is not up (headless runs, tests):
      // SWT throws SWTError, which extends Error directly rather than LinkageError, so catching
      // LinkageError alone let it through. "Which file is selected" has no answer without a UI,
      // so degrade to null instead of letting it escape into the extension point that called us.
      LogChannel.GENERAL.logDetailed("Could not resolve the selected Hop file: " + e);
      return null;
    }
  }

  private static String resolveFromExplorerSelection() {
    try {
      // peekInstance, not getInstance: the latter builds a HopGui, and building one creates an SWT
      // Shell, which on a headless machine fails outright instead of simply saying "no UI here".
      if (HopGui.peekInstance() == null) {
        return null;
      }
      ExplorerPerspective perspective = HopGui.getExplorerPerspective();
      if (perspective == null) {
        return null;
      }
      ExplorerFile selectedFile = perspective.getSelectedFile();
      if (selectedFile == null || Utils.isEmpty(selectedFile.getFilename())) {
        return null;
      }
      return resolveLintablePath(selectedFile.getFilename());
    } catch (Exception e) {
      return null;
    }
  }

  private static String resolveFromActiveEditor() {
    return LintEditorGraphHelper.getActiveEditorFilename();
  }

  private static String resolveLintablePath(String filename) {
    try {
      FileObject fileObject = HopVfs.getFileObject(filename);
      String actualPath = LintPathUtils.normalizePath(fileObject.getName().getPath());
      if (LintEditorGraphHelper.isLintableFilename(actualPath)) {
        return actualPath;
      }
    } catch (Exception ignored) {
      String normalized = LintPathUtils.normalizePath(filename);
      if (LintEditorGraphHelper.isLintableFilename(normalized)) {
        return normalized;
      }
    }
    return null;
  }
}
