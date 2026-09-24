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

import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.history.AuditManager;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.hopgui.HopGui;

/**
 * Bookmarks shared with the VFS file dialog. The map is {@code vfs-bookmarks}; the dialog's
 * navigation history is a separate list file with the same type name and is left alone.
 */
public final class VfsBookmarks {

  public static final String AUDIT_TYPE = "vfs-bookmarks";

  private VfsBookmarks() {}

  public static String namespace() {
    try {
      if (PropsUi.getInstance().useGlobalFileBookmarks()) {
        return HopGui.DEFAULT_HOP_GUI_NAMESPACE;
      }
    } catch (RuntimeException ignored) {
      return HopGui.DEFAULT_HOP_GUI_NAMESPACE;
    }
    String namespace = HopNamespace.getNamespace();
    if (namespace == null || namespace.isEmpty()) {
      return HopGui.DEFAULT_HOP_GUI_NAMESPACE;
    }
    return namespace;
  }

  public static Map<String, String> load() {
    try {
      Map<String, String> stored = AuditManager.getActive().loadMap(namespace(), AUDIT_TYPE);
      return stored == null ? new HashMap<>() : new HashMap<>(stored);
    } catch (Exception e) {
      LogChannel.GENERAL.logError("Error loading VFS bookmarks", e);
      return new HashMap<>();
    }
  }

  public static void save(Map<String, String> bookmarks) throws HopException {
    AuditManager.getActive()
        .saveMap(namespace(), AUDIT_TYPE, bookmarks == null ? new HashMap<>() : bookmarks);
  }
}
