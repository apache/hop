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

import java.io.File;
import org.apache.hop.core.util.Utils;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;

/**
 * Resolves the metadata item currently open in the Metadata perspective to the on-disk JSON file
 * the linter can read: {@code <project>/metadata/<key>/<name>.json}.
 */
final class LintMetadataSelection {

  private LintMetadataSelection() {}

  /** True when the Metadata perspective is the active perspective. */
  static boolean isMetadataPerspectiveActive() {
    try {
      MetadataPerspective perspective = MetadataPerspective.getInstance();
      return perspective != null && perspective.isActive();
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * The JSON file path for the metadata item in the active Metadata perspective editor, or {@code
   * null} when no metadata editor is active or the path cannot be resolved.
   */
  static String resolveActiveMetadataPath() {
    try {
      MetadataPerspective perspective = MetadataPerspective.getInstance();
      if (perspective == null) {
        return null;
      }
      // Resolve from the metadata item open in the active editor tab. The perspective tree
      // selection has no public accessor, so the active editor is the reliable source.
      // TODO(apache/hop#7330): once merged, MetadataPerspective exposes the tree selection
      // (getSelectedMetadataKeyAndName()); prefer the right-click tree target over the active
      // editor by restoring resolveFromTreeSelection(perspective) ahead of this call.
      return resolveFromActiveEditor(perspective);
    } catch (Exception e) {
      return null;
    }
  }

  private static String resolveFromActiveEditor(MetadataPerspective perspective) {
    MetadataEditor<?> editor = perspective.getActiveEditor();
    if (editor == null) {
      return null;
    }
    IHopMetadata metadata = editor.getMetadata();
    if (metadata == null || Utils.isEmpty(metadata.getName())) {
      return null;
    }
    HopMetadata annotation = HopMetadataUtil.getHopMetadataAnnotation(metadata.getClass());
    if (annotation == null || Utils.isEmpty(annotation.key())) {
      return null;
    }
    return resolveMetadataPath(annotation.key(), metadata.getName());
  }

  private static String resolveMetadataPath(String key, String name) {
    String projectPath = LinterConfigPlugin.getInstance().getProjectPath();
    if (Utils.isEmpty(projectPath)) {
      return null;
    }
    File file =
        new File(projectPath + File.separator + "metadata" + File.separator + key, name + ".json");
    if (!file.isFile()) {
      return null;
    }
    return LintPathUtils.normalizePath(file.getAbsolutePath());
  }
}
