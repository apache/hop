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

package org.apache.hop.ui.core.gui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("uitest")
class GuiCompositeWidgetsGroupTest extends SwtBotTestBase {

  private static final String FLAT_PARENT = "GuiCompositeWidgetsGroupTest-flat";
  private static final String GROUPED_PARENT = "GuiCompositeWidgetsGroupTest-grouped";

  @BeforeAll
  static void registerSampleWidgets() {
    register(FlatSample.class);
    register(GroupedSample.class);
  }

  @Test
  void ungroupedWidgetsStayOnAFlatForm() {
    Shell shell = new Shell(display);
    shell.setLayout(new FormLayout());
    try {
      FlatSample source = new FlatSample();
      source.setName("alpha");
      GuiCompositeWidgets widgets = new GuiCompositeWidgets(new Variables());
      widgets.createCompositeWidgets(source, null, shell, FLAT_PARENT, null);
      widgets.setWidgetsContents(source, shell, FLAT_PARENT);

      assertEquals(0, countTabFolders(shell));
      assertNotNull(widgets.getWidgetsMap().get("name"));

      source.setName("beta");
      widgets.setWidgetsContents(source, shell, FLAT_PARENT);
      widgets.getWidgetsContents(source, FLAT_PARENT);
      assertEquals("beta", source.getName());
    } finally {
      shell.dispose();
    }
  }

  @Test
  void groupedWidgetsOpenOnTabsAndRoundTripValues() {
    Shell shell = new Shell(display);
    shell.setLayout(new FormLayout());
    try {
      GroupedSample source = new GroupedSample();
      source.setFirst("one");
      source.setSecond("two");
      GuiCompositeWidgets widgets = new GuiCompositeWidgets(new Variables());
      widgets.createCompositeWidgets(source, null, shell, GROUPED_PARENT, null);
      widgets.setWidgetsContents(source, shell, GROUPED_PARENT);

      CTabFolder folder = findTabFolder(shell);
      assertNotNull(folder);
      assertEquals(2, folder.getItemCount());
      assertEquals("First tab", folder.getItem(0).getText());
      assertEquals("Second tab", folder.getItem(1).getText());
      assertFalse(folder.getItem(0).getControl() instanceof Label);

      source.setFirst("uno");
      source.setSecond("dos");
      widgets.setWidgetsContents(source, shell, GROUPED_PARENT);
      widgets.getWidgetsContents(source, GROUPED_PARENT);
      assertEquals("uno", source.getFirst());
      assertEquals("dos", source.getSecond());
    } finally {
      shell.dispose();
    }
  }

  @Test
  void extraGroupBecomesAnotherTab() {
    Shell shell = new Shell(display);
    shell.setLayout(new FormLayout());
    try {
      GroupedSample source = new GroupedSample();
      GuiCompositeWidgets widgets = new GuiCompositeWidgets(new Variables());
      widgets.registerExtraGroup("Extra", "30", null, parent -> new Label(parent, 0).setText("x"));
      widgets.createCompositeWidgets(source, null, shell, GROUPED_PARENT, null);

      CTabFolder folder = findTabFolder(shell);
      assertNotNull(folder);
      assertEquals(3, folder.getItemCount());
      assertEquals("Extra", folder.getItem(2).getText());
    } finally {
      shell.dispose();
    }
  }

  @Test
  void extraGroupOnUngroupedFieldsKeepsThemOnAGeneralTab() {
    Shell shell = new Shell(display);
    shell.setLayout(new FormLayout());
    try {
      FlatSample source = new FlatSample();
      source.setName("kept");
      GuiCompositeWidgets widgets = new GuiCompositeWidgets(new Variables());
      widgets.registerExtraGroup("Extra", "30", null, parent -> new Label(parent, 0).setText("x"));
      widgets.createCompositeWidgets(source, null, shell, FLAT_PARENT, null);
      widgets.setWidgetsContents(source, shell, FLAT_PARENT);

      CTabFolder folder = findTabFolder(shell);
      assertNotNull(folder);
      assertEquals(2, folder.getItemCount());
      assertEquals("General", folder.getItem(0).getText());
      assertEquals("Extra", folder.getItem(1).getText());
      assertNotNull(widgets.getWidgetsMap().get("name"));

      source.setName("still-there");
      widgets.setWidgetsContents(source, shell, FLAT_PARENT);
      widgets.getWidgetsContents(source, FLAT_PARENT);
      assertEquals("still-there", source.getName());
    } finally {
      shell.dispose();
    }
  }

  @Test
  void hidingAFieldOnOneTabDoesNotCollapseAnotherTab() {
    Shell shell = new Shell(display);
    shell.setLayout(new FormLayout());
    try {
      GroupedSample source = new GroupedSample();
      source.setFirst("one");
      source.setSecond("two");
      GuiCompositeWidgets widgets = new GuiCompositeWidgets(new Variables());
      widgets.createCompositeWidgets(source, null, shell, GROUPED_PARENT, null);
      widgets.setWidgetsContents(source, shell, GROUPED_PARENT);

      Control first = widgets.getWidgetsMap().get("first");
      Control second = widgets.getWidgetsMap().get("second");
      assertNotNull(first);
      assertNotNull(second);
      assertNotEquals(first.getParent(), second.getParent());

      widgets.setWidgetsHidden(source, Set.of("first"));

      assertFalse(first.getVisible());
      assertTrue(second.getVisible());
      assertTrue(second.getLayoutData() instanceof FormData);
      FormData secondData = (FormData) second.getLayoutData();
      assertTrue(secondData.height == -1 || secondData.height > 0);

      source.setSecond("dos");
      widgets.setWidgetsContents(source, shell, GROUPED_PARENT);
      widgets.getWidgetsContents(source, GROUPED_PARENT);
      assertEquals("dos", source.getSecond());
    } finally {
      shell.dispose();
    }
  }

  private static void register(Class<?> type) {
    GuiRegistry registry = GuiRegistry.getInstance();
    String parentId = null;
    for (Field field : type.getDeclaredFields()) {
      GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
      if (element != null) {
        parentId = element.parentId();
        break;
      }
    }
    if (parentId == null || registry.findGuiElements(type.getName(), parentId) != null) {
      return;
    }
    for (Field field : type.getDeclaredFields()) {
      GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
      if (element != null) {
        registry.addGuiWidgetElement(type.getName(), element, field);
      }
    }
  }

  private static CTabFolder findTabFolder(Composite parent) {
    for (Control child : parent.getChildren()) {
      if (child instanceof CTabFolder folder) {
        return folder;
      }
    }
    return null;
  }

  private static int countTabFolders(Composite parent) {
    int count = 0;
    for (Control child : parent.getChildren()) {
      if (child instanceof CTabFolder) {
        count++;
      }
    }
    return count;
  }

  @GuiPlugin
  @Getter
  @Setter
  public static class FlatSample {
    @GuiWidgetElement(
        id = "name",
        parentId = FLAT_PARENT,
        type = GuiElementType.TEXT,
        label = "Name")
    private String name;
  }

  @GuiPlugin
  @Getter
  @Setter
  public static class GroupedSample {
    @GuiWidgetElement(
        id = "first",
        parentId = GROUPED_PARENT,
        type = GuiElementType.TEXT,
        label = "First",
        group = "First tab",
        groupOrder = "10",
        groupType = GuiWidgetGroupType.TABS)
    private String first;

    @GuiWidgetElement(
        id = "second",
        parentId = GROUPED_PARENT,
        type = GuiElementType.TEXT,
        label = "Second",
        group = "Second tab",
        groupOrder = "20",
        groupType = GuiWidgetGroupType.TABS)
    private String second;
  }
}
