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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("uitest")
class GuiCompositeWidgetsGroupTest extends SwtBotTestBase {

  private static final String FLAT_PARENT = "GuiCompositeWidgetsGroupTest-flat";
  private static final String GROUPED_PARENT = "GuiCompositeWidgetsGroupTest-grouped";
  private static final String BOXES_PARENT = "GuiCompositeWidgetsGroupTest-boxes";
  private static final String SINGLE_BOX_PARENT = "GuiCompositeWidgetsGroupTest-single-box";

  @BeforeAll
  static void registerSampleWidgets() {
    register(FlatSample.class);
    register(GroupedSample.class);
    register(BoxesSample.class);
    register(SingleBoxSample.class);
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
      assertNull(source.getNote());
    } finally {
      shell.dispose();
    }
  }

  @Test
  void emptyTextWidgetLeavesNullFieldNull() {
    Shell shell = new Shell(display);
    shell.setLayout(new FormLayout());
    try {
      FlatSample source = new FlatSample();
      source.setName("alpha");
      assertNull(source.getNote());

      GuiCompositeWidgets widgets = new GuiCompositeWidgets(new Variables());
      widgets.createCompositeWidgets(source, null, shell, FLAT_PARENT, null);
      widgets.setWidgetsContents(source, shell, FLAT_PARENT);
      widgets.getWidgetsContents(source, FLAT_PARENT);
      assertNull(source.getNote());

      TextVar note = (TextVar) widgets.getWidgetsMap().get("note");
      assertNotNull(note);
      note.setText("typed");
      widgets.getWidgetsContents(source, FLAT_PARENT);
      assertEquals("typed", source.getNote());

      note.setText("");
      widgets.getWidgetsContents(source, FLAT_PARENT);
      assertEquals("", source.getNote());
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
      assertInstanceOf(FormData.class, second.getLayoutData());
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

  @Test
  void boxedWidgetsAreGroupsThatScrollAndRoundTripValues() {
    Shell shell = new Shell(display);
    shell.setLayout(new FormLayout());
    try {
      BoxesSample source = new BoxesSample();
      source.setFirst("one");
      source.setSecond("two");
      GuiCompositeWidgets widgets = new GuiCompositeWidgets(new Variables());
      widgets.createCompositeWidgets(source, null, shell, BOXES_PARENT, null);
      widgets.setWidgetsContents(source, shell, BOXES_PARENT);

      assertNull(findTabFolderDeep(shell));
      List<Group> groups = findGroups(shell);
      assertEquals(2, groups.size());
      assertEquals("First box", groups.get(0).getText());
      assertEquals("Second box", groups.get(1).getText());

      Control first = widgets.getWidgetsMap().get("first");
      Control second = widgets.getWidgetsMap().get("second");
      assertNotNull(first);
      assertNotNull(second);
      assertInstanceOf(ScrolledComposite.class, first.getParent().getParent());
      assertInstanceOf(ScrolledComposite.class, second.getParent().getParent());
      assertEquals(groups.get(0), first.getParent().getParent().getParent());
      assertEquals(groups.get(1), second.getParent().getParent().getParent());

      source.setFirst("uno");
      source.setSecond("dos");
      widgets.setWidgetsContents(source, shell, BOXES_PARENT);
      widgets.getWidgetsContents(source, BOXES_PARENT);
      assertEquals("uno", source.getFirst());
      assertEquals("dos", source.getSecond());
    } finally {
      shell.dispose();
    }
  }

  @Test
  void extraGroupBecomesAnotherBox() {
    Shell shell = new Shell(display);
    shell.setLayout(new FormLayout());
    try {
      BoxesSample source = new BoxesSample();
      Label[] created = new Label[1];
      GuiCompositeWidgets widgets = new GuiCompositeWidgets(new Variables());
      widgets.registerExtraGroup(
          "Extra", "30", null, parent -> created[0] = new Label(parent, SWT.NONE));
      widgets.createCompositeWidgets(source, null, shell, BOXES_PARENT, null);

      assertNull(findTabFolderDeep(shell));
      List<Group> groups = findGroups(shell);
      assertEquals(3, groups.size());
      assertEquals("Extra", groups.get(2).getText());
      assertNotNull(created[0]);
      assertInstanceOf(ScrolledComposite.class, created[0].getParent().getParent());
      assertEquals(groups.get(2), created[0].getParent().getParent().getParent());
    } finally {
      shell.dispose();
    }
  }

  @Test
  void boxesSplitTheParentIntoBands() {
    Shell shell = new Shell(display);
    shell.setLayout(new FormLayout());
    shell.setSize(500, 400);
    try {
      BoxesSample source = new BoxesSample();
      GuiCompositeWidgets widgets = new GuiCompositeWidgets(new Variables());
      widgets.createCompositeWidgets(source, null, shell, BOXES_PARENT, null);
      shell.layout(true, true);

      List<Group> groups = findGroups(shell);
      assertEquals(2, groups.size());
      int margin = PropsUi.getMargin();
      FormData firstData = (FormData) groups.get(0).getLayoutData();
      FormData secondData = (FormData) groups.get(1).getLayoutData();
      assertEquals(0, firstData.top.numerator);
      assertEquals(50, firstData.bottom.numerator);
      assertEquals(100, firstData.bottom.denominator);
      assertEquals(-margin, firstData.bottom.offset);
      assertEquals(50, secondData.top.numerator);
      assertEquals(margin, secondData.top.offset);
      assertEquals(100, secondData.bottom.numerator);
      assertEquals(0, secondData.bottom.offset);

      Rectangle first = groups.get(0).getBounds();
      Rectangle second = groups.get(1).getBounds();
      assertTrue(first.y + first.height <= second.y);
      Composite filler = groups.get(0).getParent();
      assertEquals(filler.getClientArea().height, second.y + second.height);
    } finally {
      shell.dispose();
    }
  }

  @Test
  void singleBoxFillsTheSpaceBetweenHeaderAndButtons() {
    Shell shell = new Shell(display);
    shell.setLayout(new FormLayout());
    shell.setSize(500, 400);
    try {
      Label header = new Label(shell, SWT.LEFT);
      header.setText("Header");
      FormData fdHeader = new FormData();
      fdHeader.left = new FormAttachment(0, 0);
      fdHeader.top = new FormAttachment(0, 0);
      fdHeader.right = new FormAttachment(100, 0);
      header.setLayoutData(fdHeader);

      Button ok = new Button(shell, SWT.PUSH);
      ok.setText("OK");
      FormData fdOk = new FormData();
      fdOk.right = new FormAttachment(100, 0);
      fdOk.bottom = new FormAttachment(100, 0);
      ok.setLayoutData(fdOk);

      SingleBoxSample source = new SingleBoxSample();
      source.setName("alpha");
      GuiCompositeWidgets.addScrolledComposite(
          shell, new Variables(), header, ok, SINGLE_BOX_PARENT, source);
      shell.layout(true, true);

      List<Group> groups = findGroups(shell);
      assertEquals(1, groups.size());
      assertEquals("Only", groups.get(0).getText());
      assertNull(findTabFolderDeep(shell));

      Rectangle box = absoluteBounds(groups.get(0));
      Rectangle headerBounds = absoluteBounds(header);
      Rectangle okBounds = absoluteBounds(ok);
      assertTrue(box.y >= headerBounds.y + headerBounds.height);
      assertTrue(box.y + box.height <= okBounds.y);
      int band = okBounds.y - (headerBounds.y + headerBounds.height);
      assertTrue(band > 0);
      assertTrue(box.height * 100 / band >= 60);
    } finally {
      shell.dispose();
    }
  }

  @Test
  void hidingAFieldInOneBoxShrinksThatBoxOnly() {
    Shell shell = new Shell(display);
    shell.setLayout(new FormLayout());
    try {
      BoxesSample source = new BoxesSample();
      source.setFirst("one");
      source.setSecond("two");
      GuiCompositeWidgets widgets = new GuiCompositeWidgets(new Variables());
      widgets.createCompositeWidgets(source, null, shell, BOXES_PARENT, null);
      widgets.setWidgetsContents(source, shell, BOXES_PARENT);

      Control first = widgets.getWidgetsMap().get("first");
      Control second = widgets.getWidgetsMap().get("second");
      assertNotNull(first);
      assertNotNull(second);
      ScrolledComposite firstScroll = (ScrolledComposite) first.getParent().getParent();
      ScrolledComposite secondScroll = (ScrolledComposite) second.getParent().getParent();
      int firstMinBefore = firstScroll.getMinHeight();
      int secondMinBefore = secondScroll.getMinHeight();

      widgets.setWidgetsHidden(source, Set.of("first"));

      assertFalse(first.getVisible());
      assertTrue(second.getVisible());
      assertInstanceOf(FormData.class, second.getLayoutData());
      FormData secondData = (FormData) second.getLayoutData();
      assertTrue(secondData.height == -1 || secondData.height > 0);
      assertTrue(firstScroll.getMinHeight() < firstMinBefore);
      assertEquals(secondMinBefore, secondScroll.getMinHeight());

      source.setSecond("dos");
      widgets.setWidgetsContents(source, shell, BOXES_PARENT);
      widgets.getWidgetsContents(source, BOXES_PARENT);
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

  private static CTabFolder findTabFolderDeep(Composite parent) {
    for (Control child : parent.getChildren()) {
      if (child instanceof CTabFolder folder) {
        return folder;
      }
      if (child instanceof Composite composite) {
        CTabFolder nested = findTabFolderDeep(composite);
        if (nested != null) {
          return nested;
        }
      }
    }
    return null;
  }

  private static List<Group> findGroups(Composite parent) {
    List<Group> groups = new ArrayList<>();
    collectGroups(parent, groups);
    return groups;
  }

  private static void collectGroups(Composite parent, List<Group> groups) {
    for (Control child : parent.getChildren()) {
      if (child instanceof Group group) {
        groups.add(group);
      }
      if (child instanceof Composite composite) {
        collectGroups(composite, groups);
      }
    }
  }

  private static Rectangle absoluteBounds(Control control) {
    Rectangle bounds = control.getBounds();
    Point origin = control.getParent().toDisplay(bounds.x, bounds.y);
    return new Rectangle(origin.x, origin.y, bounds.width, bounds.height);
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

    @GuiWidgetElement(
        id = "note",
        parentId = FLAT_PARENT,
        type = GuiElementType.TEXT,
        label = "Note")
    private String note;
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

  @GuiPlugin
  @Getter
  @Setter
  public static class BoxesSample {
    @GuiWidgetElement(
        id = "first",
        parentId = BOXES_PARENT,
        type = GuiElementType.TEXT,
        label = "First",
        group = "First box",
        groupOrder = "10",
        groupType = GuiWidgetGroupType.BOXES)
    private String first;

    @GuiWidgetElement(
        id = "second",
        parentId = BOXES_PARENT,
        type = GuiElementType.TEXT,
        label = "Second",
        group = "Second box",
        groupOrder = "20",
        groupType = GuiWidgetGroupType.BOXES)
    private String second;
  }

  @GuiPlugin
  @Getter
  @Setter
  public static class SingleBoxSample {
    @GuiWidgetElement(
        id = "name",
        parentId = SINGLE_BOX_PARENT,
        type = GuiElementType.TEXT,
        label = "Name",
        group = "Only",
        groupType = GuiWidgetGroupType.BOXES)
    private String name;
  }
}
