/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui.vfs.explorer;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.HopFileTypeRegistry;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.FolderFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.GenericFileType;

/**
 * Hop file type for an explorer row. Listing uses the extension only. The registry's second pass
 * asks types to read file content, which would touch every unmatched file in a remote folder.
 */
public final class VfsHopFileTypes {

  private VfsHopFileTypes() {}

  public static IHopFileType find(String path, boolean folder) {
    try {
      HopGui hopGui = HopGui.peekInstance();
      if (hopGui != null && hopGui.getPerspectiveManager() != null) {
        ExplorerPerspective perspective =
            hopGui.getPerspectiveManager().findPerspective(ExplorerPerspective.class);
        if (perspective != null) {
          return perspective.getFileType(path, folder);
        }
      }
    } catch (Exception ignored) {
      // Unit tests and startup have no Hop GUI. Fall through to the extension match.
    }
    return findByExtension(HopFileTypeRegistry.getInstance().getFileTypes(), path, folder);
  }

  static IHopFileType findByExtension(List<IHopFileType> types, String path, boolean folder) {
    if (types == null) {
      return null;
    }
    if (folder) {
      for (IHopFileType type : types) {
        if (type instanceof FolderFileType) {
          return type;
        }
      }
      return null;
    }
    try {
      for (IHopFileType type : types) {
        if (type instanceof FolderFileType || type instanceof GenericFileType) {
          continue;
        }
        if (type.isHandledBy(path, false)) {
          return type;
        }
      }
    } catch (HopException e) {
      return null;
    }
    return null;
  }
}
