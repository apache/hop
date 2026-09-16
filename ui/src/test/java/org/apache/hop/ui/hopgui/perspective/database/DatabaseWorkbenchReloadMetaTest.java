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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DatabaseWorkbenchReloadMetaTest {

  private IDatabaseWorkbenchHost host;
  private IHopMetadataProvider metadataProvider;
  private IHopMetadataSerializer<DatabaseMeta> serializer;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() throws Exception {
    metadataProvider = mock(IHopMetadataProvider.class);
    serializer = (IHopMetadataSerializer<DatabaseMeta>) mock(IHopMetadataSerializer.class);
    when(metadataProvider.getSerializer(DatabaseMeta.class)).thenReturn(serializer);

    host = mock(IDatabaseWorkbenchHost.class);
    when(host.getMetadataProvider()).thenReturn(metadataProvider);
  }

  @Test
  void reloadConnectionMetaReturnsNullForEmpty() {
    Map<String, DatabaseConnectionState> connections = new LinkedHashMap<>();
    assertNull(DatabaseWorkbench.reloadConnectionMeta(null, host, connections, List.of(), null));
    assertNull(DatabaseWorkbench.reloadConnectionMeta("", host, connections, List.of(), null));
  }

  @Test
  void reloadConnectionMetaRemovesDeletedConnection() throws Exception {
    Map<String, DatabaseConnectionState> connections = new LinkedHashMap<>();
    DatabaseMeta meta = mock(DatabaseMeta.class);
    when(meta.getName()).thenReturn("deleted-conn");
    connections.put("deleted-conn", new DatabaseConnectionState(meta));

    when(serializer.exists("deleted-conn")).thenReturn(false);

    AtomicBoolean treeChanged = new AtomicBoolean(false);
    DatabaseMeta result =
        DatabaseWorkbench.reloadConnectionMeta(
            "deleted-conn", host, connections, List.of(), () -> treeChanged.set(true));

    assertNull(result);
    assertFalse(connections.containsKey("deleted-conn"));
    assertTrue(treeChanged.get());
  }

  @Test
  void reloadConnectionMetaUpdatesExistingStateAndOpenTabs() throws Exception {
    Map<String, DatabaseConnectionState> connections = new LinkedHashMap<>();
    DatabaseMeta staleMeta = mock(DatabaseMeta.class);
    when(staleMeta.getName()).thenReturn("my-conn");
    when(staleMeta.getHostname()).thenReturn("old-host");
    connections.put("my-conn", new DatabaseConnectionState(staleMeta));

    DatabaseMeta freshMeta = mock(DatabaseMeta.class);
    when(freshMeta.getName()).thenReturn("my-conn");
    when(freshMeta.getHostname()).thenReturn("new-host");

    when(serializer.exists("my-conn")).thenReturn(true);
    when(serializer.load("my-conn")).thenReturn(freshMeta);

    DatabaseSqlEditorTab sqlTab = mock(DatabaseSqlEditorTab.class);
    when(sqlTab.getDatabaseMeta()).thenReturn(staleMeta);
    TabItemHandler sqlHandler = mock(TabItemHandler.class);
    when(sqlHandler.getTypeHandler()).thenReturn(sqlTab);

    DatabaseTableInfoTab tableTab = mock(DatabaseTableInfoTab.class);
    when(tableTab.getDatabaseMeta()).thenReturn(staleMeta);
    TabItemHandler tableHandler = mock(TabItemHandler.class);
    when(tableHandler.getTypeHandler()).thenReturn(tableTab);

    DatabaseMeta result =
        DatabaseWorkbench.reloadConnectionMeta(
            "my-conn", host, connections, List.of(sqlHandler, tableHandler), null);

    assertSame(freshMeta, result);
    assertSame(freshMeta, connections.get("my-conn").getDatabaseMeta());
    verify(sqlTab).setDatabaseMeta(freshMeta);
    verify(tableTab).setDatabaseMeta(freshMeta);
  }

  @Test
  void ensureConnectionFallsBackToSuppliedMetaWhenNotInSerializer() throws Exception {
    Map<String, DatabaseConnectionState> connections = new LinkedHashMap<>();
    DatabaseMeta meta = mock(DatabaseMeta.class);
    when(meta.getName()).thenReturn("unsaved-conn");

    when(serializer.exists("unsaved-conn")).thenReturn(false);

    DatabaseConnectionState state =
        DatabaseWorkbench.ensureConnection(meta, host, connections, List.of(), null);

    assertNotNull(state);
    assertSame(meta, state.getDatabaseMeta());
    assertTrue(connections.containsKey("unsaved-conn"));
  }

  @Test
  void ensureConnectionReturnsNullForBlankName() {
    Map<String, DatabaseConnectionState> connections = new LinkedHashMap<>();
    DatabaseMeta emptyName = mock(DatabaseMeta.class);
    when(emptyName.getName()).thenReturn("");
    assertNull(DatabaseWorkbench.ensureConnection(emptyName, host, connections, List.of(), null));

    DatabaseMeta unnamed = mock(DatabaseMeta.class);
    when(unnamed.getName()).thenReturn(null);
    assertNull(DatabaseWorkbench.ensureConnection(unnamed, host, connections, List.of(), null));
  }

  @Test
  void ensureConnectionReturnsFreshStateWhenExists() throws Exception {
    Map<String, DatabaseConnectionState> connections = new LinkedHashMap<>();
    DatabaseMeta staleMeta = mock(DatabaseMeta.class);
    when(staleMeta.getName()).thenReturn("conn1");
    when(staleMeta.getHostname()).thenReturn("old-host");
    connections.put("conn1", new DatabaseConnectionState(staleMeta));

    DatabaseMeta freshMeta = mock(DatabaseMeta.class);
    when(freshMeta.getName()).thenReturn("conn1");
    when(freshMeta.getHostname()).thenReturn("new-host");

    when(serializer.exists("conn1")).thenReturn(true);
    when(serializer.load("conn1")).thenReturn(freshMeta);

    DatabaseConnectionState state =
        DatabaseWorkbench.ensureConnection(staleMeta, host, connections, List.of(), null);

    assertNotNull(state);
    assertSame(freshMeta, state.getDatabaseMeta());
    assertEquals("new-host", state.getDatabaseMeta().getHostname());
  }

  @Test
  void updateTabsDatabaseMetaUpdatesMatchingTabsOnly() {
    DatabaseMeta freshMeta = mock(DatabaseMeta.class);
    when(freshMeta.getName()).thenReturn("match");

    DatabaseMeta matchMeta = mock(DatabaseMeta.class);
    when(matchMeta.getName()).thenReturn("match");

    DatabaseMeta otherMeta = mock(DatabaseMeta.class);
    when(otherMeta.getName()).thenReturn("other");

    DatabaseSqlEditorTab matchTab = mock(DatabaseSqlEditorTab.class);
    when(matchTab.getDatabaseMeta()).thenReturn(matchMeta);
    TabItemHandler matchHandler = mock(TabItemHandler.class);
    when(matchHandler.getTypeHandler()).thenReturn(matchTab);

    DatabaseSqlEditorTab otherTab = mock(DatabaseSqlEditorTab.class);
    when(otherTab.getDatabaseMeta()).thenReturn(otherMeta);
    TabItemHandler otherHandler = mock(TabItemHandler.class);
    when(otherHandler.getTypeHandler()).thenReturn(otherTab);

    DatabaseWorkbench.updateTabsDatabaseMeta(freshMeta, List.of(matchHandler, otherHandler));

    verify(matchTab).setDatabaseMeta(freshMeta);
    verify(otherTab, never()).setDatabaseMeta(freshMeta);
  }
}
