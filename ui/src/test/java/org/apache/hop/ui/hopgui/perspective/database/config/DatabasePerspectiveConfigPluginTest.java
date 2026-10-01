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

package org.apache.hop.ui.hopgui.perspective.database.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.ui.hopgui.perspective.configuration.tabs.ConfigPluginOptionsTab;
import org.junit.jupiter.api.Test;

class DatabasePerspectiveConfigPluginTest {

  @Test
  void isAConfigGuiPluginForThePluginsTab() throws Exception {
    ConfigPlugin config = DatabasePerspectiveConfigPlugin.class.getAnnotation(ConfigPlugin.class);
    GuiPlugin gui = DatabasePerspectiveConfigPlugin.class.getAnnotation(GuiPlugin.class);
    assertNotNull(config);
    assertEquals(ConfigPlugin.CATEGORY_CONFIG, config.category());
    assertNotNull(gui);
    assertEquals("i18n::DatabasePerspectiveConfigPlugin.Name", gui.description());
    assertNotNull(
        DatabasePerspectiveConfigPlugin.class.getMethod("getInstance"),
        "ConfigPluginOptionsTab loads plugins via getInstance()");
  }

  @Test
  void sqlOptionsAreOnThePluginsParent() throws Exception {
    assertWidget("autoConnectWhenExecutingSql", GuiElementType.CHECKBOX);
    assertWidget("selectExecutedSql", GuiElementType.CHECKBOX);
    assertWidget("queryRowLimit", GuiElementType.TEXT);
  }

  @Test
  void defaults() {
    DatabasePerspectiveConfig config = new DatabasePerspectiveConfig();
    assertFalse(config.isAutoConnectWhenExecutingSql());
    assertTrue(config.isSelectExecutedSql());
    assertEquals(DatabasePerspectiveConfig.DEFAULT_QUERY_ROW_LIMIT, config.resolvedQueryRowLimit());
  }

  @Test
  void resolvedQueryRowLimitFallsBackWhenMissingOrInvalid() {
    DatabasePerspectiveConfig config = new DatabasePerspectiveConfig();
    config.setQueryRowLimit(null);
    assertEquals(1000, config.resolvedQueryRowLimit());
    config.setQueryRowLimit(0);
    assertEquals(1000, config.resolvedQueryRowLimit());
    config.setQueryRowLimit(-5);
    assertEquals(1000, config.resolvedQueryRowLimit());
    config.setQueryRowLimit(2000);
    assertEquals(2000, config.resolvedQueryRowLimit());
  }

  @Test
  void parseQueryRowLimit() {
    assertEquals(2000, DatabasePerspectiveConfigPlugin.parseQueryRowLimit("2000"));
    assertEquals(1000, DatabasePerspectiveConfigPlugin.parseQueryRowLimit(""));
    assertEquals(1000, DatabasePerspectiveConfigPlugin.parseQueryRowLimit("abc"));
    assertEquals(1000, DatabasePerspectiveConfigPlugin.parseQueryRowLimit("0"));
    assertEquals(1000, DatabasePerspectiveConfigPlugin.parseQueryRowLimit(null));
  }

  private static void assertWidget(String fieldName, GuiElementType type) throws Exception {
    Field field = DatabasePerspectiveConfigPlugin.class.getDeclaredField(fieldName);
    GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
    assertNotNull(element, fieldName);
    assertEquals(ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID, element.parentId());
    assertEquals(type, element.type());
    assertEquals(GuiWidgetGroupType.BOXES, element.groupType());
  }
}
