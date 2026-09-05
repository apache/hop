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

package org.apache.hop.calcite.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

class CalciteSqlFormatConfigPluginTest {

  @Test
  void isAConfigGuiPluginForThePluginsTab() throws Exception {
    ConfigPlugin config = CalciteSqlFormatConfigPlugin.class.getAnnotation(ConfigPlugin.class);
    GuiPlugin gui = CalciteSqlFormatConfigPlugin.class.getAnnotation(GuiPlugin.class);
    assertNotNull(config);
    assertEquals(ConfigPlugin.CATEGORY_CONFIG, config.category());
    assertNotNull(gui);
    assertEquals("i18n::CalciteSqlFormatConfigPlugin.Name", gui.description());
    assertNotNull(
        CalciteSqlFormatConfigPlugin.class.getMethod("getInstance"),
        "ConfigPluginOptionsTab loads plugins via getInstance()");
  }

  @Test
  void everySqlFormatOptionIsAWidgetOnThePluginsParent() throws Exception {
    int widgets = 0;
    for (Field field : CalciteSqlFormatConfigPlugin.class.getDeclaredFields()) {
      GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
      if (element == null) {
        continue;
      }
      widgets++;
      assertEquals(ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID, element.parentId());
      assertEquals(GuiWidgetGroupType.TABS, element.groupType());
      assertTrue(
          element.type() == GuiElementType.CHECKBOX || element.type() == GuiElementType.TEXT);
    }
    assertEquals(11, widgets, "SqlFormatOptions has 11 fields");
  }
}
