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

package org.apache.hop.ui.core.gui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiTableColumn;
import org.apache.hop.core.gui.plugin.GuiTableColumnType;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("uitest")
class GuiCompositeWidgetsTableTest extends SwtBotTestBase {

  private static final String TABBED_PARENT = "GuiCompositeWidgetsTableTest-tabbed";
  private static final String FLAT_PARENT = "GuiCompositeWidgetsTableTest-flat";

  @BeforeAll
  static void registerSampleWidgets() throws Exception {
    register(TableSample.class);
    register(FlatTable.class);
  }

  @Test
  void gridRoundTripsRowsAndDropsTheBlankRow() {
    Shell shell = new Shell(display);
    shell.setLayout(new FormLayout());
    try {
      TableSample source = new TableSample();
      List<SampleRow> original = source.getRows();
      original.add(new SampleRow("alpha", true, SampleKind.LEFT));
      original.add(new SampleRow("beta", false, SampleKind.RIGHT));

      GuiCompositeWidgets widgets = new GuiCompositeWidgets(new Variables());
      widgets.createCompositeWidgets(source, null, shell, TABBED_PARENT, null);
      widgets.setWidgetsContents(source, shell, TABBED_PARENT);

      TableView table = (TableView) widgets.getWidgetsMap().get("rows");
      assertEquals("alpha", table.getTable().getItem(0).getText(1));
      assertEquals("Y", table.getTable().getItem(0).getText(2));
      assertEquals("LEFT", table.getTable().getItem(0).getText(3));
      assertEquals("beta", table.getTable().getItem(1).getText(1));
      assertEquals("N", table.getTable().getItem(1).getText(2));
      assertEquals("RIGHT", table.getTable().getItem(1).getText(3));

      table.getTable().getItem(0).setText(1, "gamma");
      table.getTable().getItem(0).setText(2, "N");
      table.getTable().getItem(0).setText(3, "RIGHT");
      new TableItem(table.getTable(), SWT.NONE);

      widgets.getWidgetsContents(source, TABBED_PARENT);

      assertSame(original, source.getRows());
      assertEquals(2, source.getRows().size());
      assertEquals("gamma", source.getRows().get(0).getName());
      assertFalse(source.getRows().get(0).isActive());
      assertEquals(SampleKind.RIGHT, source.getRows().get(0).getKind());
      assertEquals("beta", source.getRows().get(1).getName());

      table.getTable().getItem(0).setText(3, "");
      widgets.getWidgetsContents(source, TABBED_PARENT);
      assertNull(source.getRows().get(0).getKind());

      table.getTable().getItem(0).setText(2, "x");
      table.getTable().getItem(0).setText(3, "NOPE");
      widgets.getWidgetsContents(source, TABBED_PARENT);
      assertFalse(source.getRows().get(0).isActive());
      assertNull(source.getRows().get(0).getKind());

      source.setRows(null);
      widgets.setWidgetsContents(source, shell, TABBED_PARENT);
      widgets.getWidgetsContents(source, TABBED_PARENT);
      assertNotNull(source.getRows());
      assertTrue(source.getRows().isEmpty());
    } finally {
      shell.dispose();
    }
  }

  @Test
  void lastGridFillsTheParentAndHidingRestoresThatAttachment() {
    Shell shell = new Shell(display);
    shell.setLayout(new FormLayout());
    shell.setSize(800, 600);
    try {
      TableSample source = new TableSample();
      GuiCompositeWidgets widgets = new GuiCompositeWidgets(new Variables());
      widgets.createCompositeWidgets(source, null, shell, TABBED_PARENT, null);

      CTabFolder folder = findTabFolder(shell);
      assertNotNull(folder);
      assertEquals(1, folder.getItemCount());
      assertEquals("Rows", folder.getItem(0).getText());

      TableView rows = (TableView) widgets.getWidgetsMap().get("rows");
      TableView more = (TableView) widgets.getWidgetsMap().get("more");
      FormData rowsData = (FormData) rows.getLayoutData();
      FormData moreData = (FormData) more.getLayoutData();
      assertEquals(0, rowsData.left.numerator);
      assertEquals(100, rowsData.right.numerator);
      assertTrue(rowsData.height > 0);
      assertNull(rowsData.bottom);
      assertEquals(100, moreData.bottom.numerator);
      assertTrue(moreData.height > 0);

      Label label = (Label) widgets.getLabelsMap().get("rows");
      FormData labelData = (FormData) label.getLayoutData();
      assertEquals(100, labelData.right.numerator);
      assertEquals(0, label.getStyle() & SWT.RIGHT);

      widgets.setWidgetsHidden(source, Set.of("more"));
      assertFalse(more.getVisible());
      FormData hidden = (FormData) more.getLayoutData();
      assertNull(hidden.bottom);
      assertEquals(0, hidden.height);
      assertNull(((FormData) rows.getLayoutData()).bottom);

      widgets.setWidgetsHidden(source, Set.of());
      assertTrue(more.getVisible());
      FormData restored = (FormData) more.getLayoutData();
      assertNotNull(restored.bottom);
      assertEquals(100, restored.bottom.numerator);
      assertTrue(restored.height > 0);
    } finally {
      shell.dispose();
    }
  }

  @Test
  void aFlatGridAlsoFillsItsParent() {
    Shell shell = new Shell(display);
    shell.setLayout(new FormLayout());
    try {
      FlatTable source = new FlatTable();
      GuiCompositeWidgets widgets = new GuiCompositeWidgets(new Variables());
      widgets.createCompositeWidgets(source, null, shell, FLAT_PARENT, null);

      Control table = widgets.getWidgetsMap().get("only");
      assertInstanceOf(TableView.class, table);
      FormData data = (FormData) table.getLayoutData();
      assertEquals(0, data.left.numerator);
      assertEquals(100, data.right.numerator);
      assertNotNull(data.bottom);
      assertEquals(100, data.bottom.numerator);
      assertTrue(data.height > 0);

      Label label = (Label) widgets.getLabelsMap().get("only");
      assertEquals(100, ((FormData) label.getLayoutData()).right.numerator);
      assertEquals(0, label.getStyle() & SWT.RIGHT);
    } finally {
      shell.dispose();
    }
  }

  @Test
  void buttonAppendsARowAndRefreshesTheGrid() {
    Shell shell = new Shell(display);
    shell.setLayout(new FormLayout());
    try {
      TableSample source = new TableSample();
      GuiCompositeWidgets widgets = new GuiCompositeWidgets(new Variables());
      widgets.createCompositeWidgets(source, null, shell, TABBED_PARENT, null);
      widgets.setWidgetsContents(source, shell, TABBED_PARENT);

      Button add = (Button) widgets.getWidgetsMap().get("add");
      Event event = new Event();
      event.widget = add;
      add.notifyListeners(SWT.Selection, event);

      TableView table = (TableView) widgets.getWidgetsMap().get("rows");
      assertEquals(1, source.getRows().size());
      assertEquals("added", source.getRows().get(0).getName());
      assertEquals("added", table.getTable().getItem(0).getText(1));
      assertEquals("Y", table.getTable().getItem(0).getText(2));
      assertEquals("LEFT", table.getTable().getItem(0).getText(3));
    } finally {
      shell.dispose();
    }
  }

  private static CTabFolder findTabFolder(Composite parent) {
    for (Control child : parent.getChildren()) {
      if (child instanceof CTabFolder folder) {
        return folder;
      }
      if (child instanceof Composite composite) {
        CTabFolder nested = findTabFolder(composite);
        if (nested != null) {
          return nested;
        }
      }
    }
    return null;
  }

  private static void register(Class<?> type) throws Exception {
    GuiRegistry registry = GuiRegistry.getInstance();
    String parentId = parentId(type);
    if (parentId == null || registry.findGuiElements(type.getName(), parentId) != null) {
      return;
    }
    for (Field field : type.getDeclaredFields()) {
      GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
      if (element != null) {
        registry.addGuiWidgetElement(type.getName(), element, field);
      }
    }
    for (Method method : type.getDeclaredMethods()) {
      GuiWidgetElement element = method.getAnnotation(GuiWidgetElement.class);
      if (element != null) {
        registry.addGuiWidgetElement(element, method, type.getName(), type.getClassLoader());
      }
    }
  }

  private static String parentId(Class<?> type) {
    for (Field field : type.getDeclaredFields()) {
      GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
      if (element != null) {
        return element.parentId();
      }
    }
    return null;
  }

  @GuiPlugin
  @Getter
  @Setter
  public static class TableSample {
    @GuiWidgetElement(
        id = "add",
        order = "10",
        type = GuiElementType.BUTTON,
        label = "Add",
        parentId = TABBED_PARENT,
        group = "Rows",
        groupOrder = "10",
        groupType = GuiWidgetGroupType.TABS)
    public void addRow(TableSample sample) {
      if (sample.getRows() == null) {
        sample.setRows(new ArrayList<>());
      }
      sample.getRows().add(new SampleRow("added", true, SampleKind.LEFT));
    }

    @GuiWidgetElement(
        id = "rows",
        order = "20",
        type = GuiElementType.TABLE,
        label = "Rows",
        parentId = TABBED_PARENT,
        group = "Rows",
        groupOrder = "10",
        groupType = GuiWidgetGroupType.TABS,
        tableRows = 5)
    private List<SampleRow> rows = new ArrayList<>();

    @GuiWidgetElement(
        id = "more",
        order = "30",
        type = GuiElementType.TABLE,
        label = "More",
        parentId = TABBED_PARENT,
        group = "Rows",
        groupOrder = "10",
        groupType = GuiWidgetGroupType.TABS,
        tableRows = 3)
    private List<NoteRow> more = new ArrayList<>();
  }

  @GuiPlugin
  @Getter
  @Setter
  public static class FlatTable {
    @GuiWidgetElement(
        id = "only",
        type = GuiElementType.TABLE,
        label = "Only",
        parentId = FLAT_PARENT,
        tableRows = 4)
    private List<NoteRow> only = new ArrayList<>();
  }

  /** Custom {@code toString} so a grid that used it would fail the round trip. */
  public enum SampleKind {
    LEFT,
    RIGHT;

    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }

  @Getter
  @Setter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class SampleRow {
    @GuiTableColumn(order = "10", type = GuiTableColumnType.TEXT, label = "Name", variables = false)
    private String name;

    @GuiTableColumn(order = "20", type = GuiTableColumnType.CHECKBOX, label = "Active")
    private boolean active;

    @GuiTableColumn(order = "30", type = GuiTableColumnType.COMBO, label = "Kind")
    private SampleKind kind;
  }

  @Getter
  @Setter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class NoteRow {
    @GuiTableColumn(order = "10", type = GuiTableColumnType.TEXT, label = "Note")
    private String note;
  }
}
