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
package org.apache.hop.ui.hopgui.context.menu;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElementType;
import org.apache.hop.core.gui.plugin.menu.GuiMenuItem;
import org.apache.hop.ui.core.gui.GuiMenuWidgets;
import org.junit.jupiter.api.Test;

/**
 * The menu as actions, in the searchable actions view.
 *
 * <p>A submenu was listed as an action of its own that did nothing when clicked, next to the
 * actions it contains.
 *
 * @see <a href="https://github.com/apache/hop/issues/8736">#8736</a>
 */
class MenuContextHandlerTest {

  private static final String ROOT = "MenuContextHandlerTest-Menu";

  private static void addItem(String id, String parentId, String label) {
    GuiMenuItem item = new GuiMenuItem();
    item.setRoot(ROOT);
    item.setId(id);
    item.setParentId(parentId);
    item.setLabel(label);
    item.setType(GuiMenuElementType.MENU_ITEM);
    item.setClassLoader(MenuContextHandlerTest.class.getClassLoader());
    GuiRegistry.getInstance().addGuiMenuItem(ROOT, item);
  }

  @Test
  void aSubmenuIsNotAnAction() {
    addItem("10000-tools", ROOT, "Tools");
    addItem("10100-tools-export", "10000-tools", "Export");
    addItem("10110-tools-export-csv", "10100-tools-export", "Export to CSV");
    addItem("10200-tools-search", "10000-tools", "Search");

    List<GuiAction> actions =
        new MenuContextHandler(ROOT, new GuiMenuWidgets()).getSupportedActions();

    assertEquals(
        List.of("10110-tools-export-csv", "10200-tools-search"),
        actions.stream().map(GuiAction::getId).toList());
    assertEquals("Export", actions.get(0).getCategory(), "the submenu names its children's group");
  }
}
