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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.history.AuditManager;
import org.apache.hop.history.AuditState;
import org.apache.hop.ui.hopgui.HopGui;

/** Column visibility and sort for the file explorer. Separate from the file dialog's sort. */
@Getter
public final class VfsExplorerViewState {

  public static final String STATE_TYPE = "vfs-explorer-state";
  public static final String STATE_NAME = "vfs-explorer-state";

  /**
   * Bumped when the default column set changes. Older saved states listed every column because the
   * column menu could not be changed, so those files are not treated as a user choice.
   */
  static final int COLUMNS_VERSION = 2;

  @Setter private VfsFileColumn sortColumn = VfsFileColumn.NAME;
  @Setter private boolean ascending = true;
  @Setter private boolean showHidden;
  private final EnumSet<VfsFileColumn> visible = defaultVisible();

  public boolean isVisible(VfsFileColumn column) {
    return column == VfsFileColumn.NAME || visible.contains(column);
  }

  public void setVisible(VfsFileColumn column, boolean shown) {
    if (column == null || column == VfsFileColumn.NAME) {
      return;
    }
    if (shown) {
      visible.add(column);
    } else {
      visible.remove(column);
    }
  }

  public static VfsExplorerViewState load() {
    VfsExplorerViewState state = new VfsExplorerViewState();
    try {
      AuditState stored =
          AuditManager.retrieveState(
              LogChannel.UI, HopGui.DEFAULT_HOP_GUI_NAMESPACE, STATE_TYPE, STATE_NAME);
      if (stored == null || stored.getStateMap() == null) {
        return state;
      }
      String sortName = stored.extractString("sortColumn", VfsFileColumn.NAME.name());
      try {
        state.sortColumn = VfsFileColumn.valueOf(sortName);
      } catch (IllegalArgumentException ignored) {
        state.sortColumn = VfsFileColumn.NAME;
      }
      state.ascending = stored.extractBoolean("ascending", true);
      state.showHidden = stored.extractBoolean("showHidden", false);
      applyStoredColumns(state, stored);
    } catch (Exception e) {
      LogChannel.GENERAL.logError("Error loading VFS explorer view state", e);
    }
    return state;
  }

  public void save() {
    Map<String, Object> values = new HashMap<>();
    values.put("sortColumn", sortColumn == null ? VfsFileColumn.NAME.name() : sortColumn.name());
    values.put("ascending", ascending);
    values.put("showHidden", showHidden);
    values.put("columnsVersion", COLUMNS_VERSION);
    for (VfsFileColumn column : VfsFileColumn.values()) {
      if (column != VfsFileColumn.NAME) {
        values.put(column.name(), visible.contains(column));
      }
    }
    AuditManager.storeState(
        LogChannel.UI, HopGui.DEFAULT_HOP_GUI_NAMESPACE, STATE_TYPE, STATE_NAME, values);
  }

  /** Name is always shown. Size and modified are the other columns shown until the user chooses. */
  static boolean shownByDefault(VfsFileColumn column) {
    return column == VfsFileColumn.SIZE || column == VfsFileColumn.MODIFIED;
  }

  static EnumSet<VfsFileColumn> defaultVisible() {
    EnumSet<VfsFileColumn> columns = EnumSet.noneOf(VfsFileColumn.class);
    columns.add(VfsFileColumn.SIZE);
    columns.add(VfsFileColumn.MODIFIED);
    return columns;
  }

  /**
   * States saved before {@link #COLUMNS_VERSION} stored every column as visible. Ignore those flags
   * and use {@link #shownByDefault}.
   */
  static void applyStoredColumns(VfsExplorerViewState state, AuditState stored) {
    if (state == null || stored == null || stored.getStateMap() == null) {
      return;
    }
    boolean chosen = stored.extractInteger("columnsVersion", 1) >= COLUMNS_VERSION;
    for (VfsFileColumn column : VfsFileColumn.values()) {
      if (column == VfsFileColumn.NAME) {
        continue;
      }
      boolean shown =
          chosen
              ? stored.extractBoolean(column.name(), shownByDefault(column))
              : shownByDefault(column);
      state.setVisible(column, shown);
    }
  }
}
