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

package org.apache.hop.ui.hopgui.vfs.explorer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.hop.history.AuditState;
import org.junit.jupiter.api.Test;

class VfsExplorerViewStateTest {

  @Test
  void extensionOwnerAndPermissionsAreHiddenByDefault() {
    VfsExplorerViewState state = new VfsExplorerViewState();
    assertTrue(state.isVisible(VfsFileColumn.NAME));
    assertTrue(state.isVisible(VfsFileColumn.SIZE));
    assertTrue(state.isVisible(VfsFileColumn.MODIFIED));
    assertFalse(state.isVisible(VfsFileColumn.EXTENSION));
    assertFalse(state.isVisible(VfsFileColumn.OWNER));
    assertFalse(state.isVisible(VfsFileColumn.PERMISSIONS));
  }

  @Test
  void legacySavedStateDoesNotKeepEveryColumnVisible() {
    Map<String, Object> values = new HashMap<>();
    for (VfsFileColumn column : VfsFileColumn.values()) {
      values.put(column.name(), true);
    }
    VfsExplorerViewState state = new VfsExplorerViewState();
    VfsExplorerViewState.applyStoredColumns(
        state, new AuditState(VfsExplorerViewState.STATE_NAME, values));

    assertFalse(state.isVisible(VfsFileColumn.EXTENSION));
    assertFalse(state.isVisible(VfsFileColumn.OWNER));
    assertFalse(state.isVisible(VfsFileColumn.PERMISSIONS));
    assertTrue(state.isVisible(VfsFileColumn.SIZE));
  }

  @Test
  void savedChoiceAfterColumnVersionIsKept() {
    Map<String, Object> values = new HashMap<>();
    values.put("columnsVersion", VfsExplorerViewState.COLUMNS_VERSION);
    values.put(VfsFileColumn.EXTENSION.name(), true);
    values.put(VfsFileColumn.OWNER.name(), false);
    values.put(VfsFileColumn.PERMISSIONS.name(), false);
    values.put(VfsFileColumn.SIZE.name(), true);
    values.put(VfsFileColumn.MODIFIED.name(), false);

    VfsExplorerViewState state = new VfsExplorerViewState();
    VfsExplorerViewState.applyStoredColumns(
        state, new AuditState(VfsExplorerViewState.STATE_NAME, values));

    assertTrue(state.isVisible(VfsFileColumn.EXTENSION));
    assertFalse(state.isVisible(VfsFileColumn.OWNER));
    assertFalse(state.isVisible(VfsFileColumn.PERMISSIONS));
    assertFalse(state.isVisible(VfsFileColumn.MODIFIED));
  }
}
