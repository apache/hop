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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.Method;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.junit.jupiter.api.Test;

class DatabaseWorkbenchViewsTest {

  @Test
  void toolsMenuIsInertWithoutHopGui() {
    assertDoesNotThrow(() -> new DatabaseWorkbenchViews().menuToolsDatabaseWindow());
  }

  @Test
  void dockIsNotOpenWithoutHopGui() {
    assertFalse(DatabaseWorkbenchViews.isDockOpen(null));
  }

  @Test
  void dialogIsNotOpenWithoutHopGui() {
    assertFalse(DatabaseWorkbenchViews.isDialogOpen(null));
  }

  @Test
  void openDialogAndDockTolerateNullHopGui() {
    assertDoesNotThrow(() -> DatabaseWorkbenchViews.openDialog(null));
    assertDoesNotThrow(() -> DatabaseWorkbenchViews.openDock(null));
  }

  @Test
  void openSqlIsInertWithoutHopGui() {
    assertDoesNotThrow(() -> DatabaseWorkbenchDialog.openSql(null, "SELECT 1"));
  }

  @Test
  void openInDatabaseIsInertWithoutHopGui() {
    assertDoesNotThrow(() -> DatabaseWorkbenchViews.openInDatabase(null, null, "SELECT 1"));
    assertDoesNotThrow(() -> DatabaseWorkbenchViews.openInDatabase(null, "SELECT 1"));
  }

  @Test
  void editMetadataToolbarIsRegisteredOnTheWorkbench() throws Exception {
    Method method = DatabaseWorkbench.class.getMethod("editSelectedConnection");
    GuiToolbarElement element = method.getAnnotation(GuiToolbarElement.class);
    assertNotNull(element);
    assertEquals(DatabaseWorkbench.GUI_PLUGIN_TOOLBAR_PARENT_ID, element.root());
    assertEquals(DatabaseWorkbench.TOOLBAR_ITEM_EDIT_METADATA, element.id());
    assertEquals("ui/images/metadata.svg", element.image());
  }

  @Test
  void openPerspectiveToolbarIsRegisteredOnTheExplorer() throws Exception {
    Method method = DatabaseExplorerDialog.class.getMethod("openInDatabasePerspective");
    GuiToolbarElement element = method.getAnnotation(GuiToolbarElement.class);
    assertNotNull(element);
    assertEquals(DatabaseExplorerDialog.GUI_PLUGIN_TOOLBAR_PARENT_ID, element.root());
    assertEquals(DatabaseExplorerDialog.TOOLBAR_ITEM_OPEN_PERSPECTIVE, element.id());
    assertEquals("ui/images/database-perspective.svg", element.image());
  }
}
