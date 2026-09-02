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

package org.apache.hop.ui.hopgui.perspective.database;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.history.AuditList;
import org.apache.hop.history.AuditManager;
import org.apache.hop.history.AuditState;
import org.apache.hop.history.AuditStateMap;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.eclipse.swt.widgets.Display;

/**
 * Remembers SQL editor tabs across the Database perspective, floating window and bottom dock, and
 * across Hop Gui restarts. Stored per project namespace in the audit trail (same mechanism as
 * terminal tabs).
 *
 * <p>Query results are not persisted. Untitled or dirty buffers are stored as text (capped). Saved
 * clean files are reopened from VFS.
 */
final class DatabaseSqlTabMemory {

  static final String AUDIT_TYPE = "database-sql-tabs";
  static final String SELECTION_KEY = "selection";
  static final String PROP_CONNECTION = "connection";
  static final String PROP_FILENAME = "filename";
  static final String PROP_SQL = "sql";
  static final String PROP_DIRTY = "dirty";
  static final String PROP_NAME = "name";
  static final String PROP_INDEX = "index";

  static final int MAX_SQL_CHARS = 512_000;
  private static final int SAVE_DEBOUNCE_MS = 400;

  private DatabaseSqlTabMemory() {}

  static void restore(DatabaseWorkbench workbench) {
    if (workbench == null || workbench.isDisposed() || workbench.hasSqlEditorTabs()) {
      return;
    }
    try {
      String group = HopNamespace.getNamespace();
      AuditList list = AuditManager.getActive().retrieveList(group, AUDIT_TYPE);
      AuditStateMap stateMap = AuditManager.getActive().loadAuditStateMap(group, AUDIT_TYPE);
      List<Snapshot> snapshots = snapshotsFromAudit(list, stateMap);
      int selected = 0;
      AuditState selection = stateMap.get(SELECTION_KEY);
      if (selection != null) {
        selected = number(selection.getStateMap().get(PROP_INDEX), 0);
      }
      workbench.restoringSqlTabs = true;
      try {
        int opened = 0;
        int selectIndex = 0;
        for (int i = 0; i < snapshots.size(); i++) {
          Snapshot snapshot = snapshots.get(i);
          if (workbench.restoreSqlTab(snapshot)) {
            if (i == selected) {
              selectIndex = opened;
            }
            opened++;
          }
        }
        workbench.selectSqlTabIndex(selectIndex);
      } finally {
        workbench.restoringSqlTabs = false;
      }
    } catch (Exception e) {
      LogChannel.UI.logError("Unable to restore Database SQL editor tabs", e);
    }
  }

  static void scheduleSave(DatabaseWorkbench workbench) {
    if (workbench == null || workbench.isDisposed() || workbench.restoringSqlTabs) {
      return;
    }
    Display display = workbench.getDisplay();
    if (display == null || display.isDisposed()) {
      return;
    }
    display.timerExec(-1, workbench.persistSqlTabsRunnable);
    display.timerExec(SAVE_DEBOUNCE_MS, workbench.persistSqlTabsRunnable);
  }

  static void saveNow(DatabaseWorkbench workbench) {
    if (workbench == null || workbench.isDisposed() || workbench.restoringSqlTabs) {
      return;
    }
    Display display = workbench.getDisplay();
    if (display != null && !display.isDisposed()) {
      display.timerExec(-1, workbench.persistSqlTabsRunnable);
    }
    save(workbench);
  }

  static void save(DatabaseWorkbench workbench) {
    if (workbench == null || workbench.isDisposed()) {
      return;
    }
    try {
      List<Snapshot> snapshots = workbench.snapshotSqlTabs();
      int selected = workbench.selectedSqlTabIndex();
      AuditStateMap stateMap = new AuditStateMap();
      List<String> ids = new ArrayList<>();
      for (int i = 0; i < snapshots.size(); i++) {
        String id = "sql-" + i;
        ids.add(id);
        stateMap.add(new AuditState(id, snapshots.get(i).toStateMap()));
      }
      AuditList list = new AuditList(ids);
      Map<String, Object> selection = new LinkedHashMap<>();
      selection.put(PROP_INDEX, selected);
      stateMap.add(new AuditState(SELECTION_KEY, selection));
      String group = HopNamespace.getNamespace();
      AuditManager.getActive().storeList(group, AUDIT_TYPE, list);
      AuditManager.getActive().saveAuditStateMap(group, AUDIT_TYPE, stateMap);
    } catch (Exception e) {
      LogChannel.UI.logError("Unable to save Database SQL editor tabs", e);
    }
  }

  static List<Snapshot> snapshotsFromAudit(AuditList list, AuditStateMap stateMap) {
    List<Snapshot> snapshots = new ArrayList<>();
    if (list == null || Utils.isEmpty(list.getNames()) || stateMap == null) {
      return snapshots;
    }
    for (String id : list.getNames()) {
      AuditState state = stateMap.get(id);
      if (state == null) {
        continue;
      }
      Snapshot snapshot = Snapshot.fromStateMap(state.getStateMap());
      if (snapshot != null) {
        snapshots.add(snapshot);
      }
    }
    return snapshots;
  }

  static int number(Object value, int defaultValue) {
    if (value instanceof Number n) {
      return n.intValue();
    }
    if (value != null) {
      try {
        return Integer.parseInt(value.toString());
      } catch (NumberFormatException ignored) {
        return defaultValue;
      }
    }
    return defaultValue;
  }

  static final class Snapshot {
    final String connection;
    final String filename;
    final String sql;
    final boolean dirty;
    final String name;

    Snapshot(String connection, String filename, String sql, boolean dirty, String name) {
      this.connection = Const.NVL(connection, "");
      this.filename = Const.NVL(filename, "");
      this.sql = sql;
      this.dirty = dirty;
      this.name = Const.NVL(name, "");
    }

    Map<String, Object> toStateMap() {
      Map<String, Object> map = new LinkedHashMap<>();
      map.put(PROP_CONNECTION, connection);
      map.put(PROP_FILENAME, filename);
      map.put(PROP_DIRTY, dirty);
      map.put(PROP_NAME, name);
      if (sql != null) {
        map.put(PROP_SQL, sql.length() > MAX_SQL_CHARS ? sql.substring(0, MAX_SQL_CHARS) : sql);
      }
      return map;
    }

    static Snapshot fromStateMap(Map<String, Object> map) {
      if (map == null) {
        return null;
      }
      String connection = string(map.get(PROP_CONNECTION));
      if (Utils.isEmpty(connection)) {
        return null;
      }
      Object dirtyValue = map.get(PROP_DIRTY);
      boolean dirty =
          dirtyValue instanceof Boolean b ? b : Boolean.parseBoolean(String.valueOf(dirtyValue));
      return new Snapshot(
          connection,
          string(map.get(PROP_FILENAME)),
          string(map.get(PROP_SQL)),
          dirty,
          string(map.get(PROP_NAME)));
    }

    private static String string(Object value) {
      return value == null ? "" : value.toString();
    }
  }

  static Snapshot snapshotOf(DatabaseSqlEditorTab tab) {
    boolean persistBuffer = Utils.isEmpty(tab.getFilename()) || tab.hasChanged();
    String sql = persistBuffer ? tab.getSqlText() : null;
    return new Snapshot(
        tab.getDatabaseMeta().getName(), tab.getFilename(), sql, tab.hasChanged(), tab.getName());
  }
}
