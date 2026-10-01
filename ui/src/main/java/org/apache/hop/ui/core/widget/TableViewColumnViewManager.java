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
package org.apache.hop.ui.core.widget;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.history.AuditManager;
import org.apache.hop.history.AuditState;
import org.apache.hop.history.AuditStateMap;

/**
 * Load and store named {@link TableView} column views via the active {@link AuditManager}.
 *
 * <p>Each view is one {@link AuditState} under type {@link #AUDIT_TYPE}. The group is the current
 * project namespace.
 */
public final class TableViewColumnViewManager {
  public static final String AUDIT_TYPE = "table-view";
  public static final String STATE_COLUMNS = "columns";

  private TableViewColumnViewManager() {}

  public static List<TableViewColumnView> list(String group) {
    List<TableViewColumnView> views = new ArrayList<>();
    if (StringUtils.isEmpty(group)) {
      return views;
    }
    try {
      AuditStateMap stateMap = AuditManager.getActive().loadAuditStateMap(group, AUDIT_TYPE);
      if (stateMap == null || stateMap.getNameStateMap() == null) {
        return views;
      }
      for (AuditState state : stateMap.getNameStateMap().values()) {
        TableViewColumnView view = fromState(state);
        if (view != null && StringUtils.isNotEmpty(view.getName())) {
          views.add(view);
        }
      }
      views.sort(
          Comparator.comparing(
              view -> view.getName() == null ? "" : view.getName(), String.CASE_INSENSITIVE_ORDER));
    } catch (Exception e) {
      LogChannel.UI.logError(
          "Unable to list table views from audit manager type " + AUDIT_TYPE + " in group " + group,
          e);
    }
    return views;
  }

  public static TableViewColumnView load(String group, String name) {
    if (StringUtils.isEmpty(group) || StringUtils.isEmpty(name)) {
      return null;
    }
    try {
      AuditState state = AuditManager.getActive().retrieveState(group, AUDIT_TYPE, name);
      return fromState(state);
    } catch (Exception e) {
      LogChannel.UI.logError(
          "Unable to load table view '"
              + name
              + "' from audit manager type "
              + AUDIT_TYPE
              + " in group "
              + group,
          e);
      return null;
    }
  }

  public static void save(String group, TableViewColumnView view) {
    if (StringUtils.isEmpty(group) || view == null || StringUtils.isEmpty(view.getName())) {
      return;
    }
    Map<String, Object> stateMap = new HashMap<>();
    List<String> columns =
        view.getColumnNames() != null ? new ArrayList<>(view.getColumnNames()) : new ArrayList<>();
    stateMap.put(STATE_COLUMNS, columns);
    AuditManager.storeState(LogChannel.UI, group, AUDIT_TYPE, view.getName(), stateMap);
  }

  public static void delete(String group, String name) {
    if (StringUtils.isEmpty(group) || StringUtils.isEmpty(name)) {
      return;
    }
    try {
      AuditStateMap stateMap = AuditManager.getActive().loadAuditStateMap(group, AUDIT_TYPE);
      if (stateMap == null) {
        return;
      }
      stateMap.remove(name);
      AuditManager.getActive().saveAuditStateMap(group, AUDIT_TYPE, stateMap);
    } catch (Exception e) {
      LogChannel.UI.logError(
          "Unable to delete table view '"
              + name
              + "' from audit manager type "
              + AUDIT_TYPE
              + " in group "
              + group,
          e);
    }
  }

  static TableViewColumnView fromState(AuditState state) {
    if (state == null || StringUtils.isEmpty(state.getName())) {
      return null;
    }
    TableViewColumnView view = new TableViewColumnView();
    view.setName(state.getName());
    view.setColumnNames(extractStringList(state, STATE_COLUMNS));
    return view;
  }

  static List<String> extractStringList(AuditState state, String key) {
    List<String> columns = new ArrayList<>();
    if (state == null || state.getStateMap() == null) {
      return columns;
    }
    Object raw = state.getStateMap().get(key);
    if (raw instanceof Collection<?> collection) {
      for (Object item : collection) {
        if (item != null) {
          columns.add(String.valueOf(item));
        }
      }
    }
    return columns;
  }
}
