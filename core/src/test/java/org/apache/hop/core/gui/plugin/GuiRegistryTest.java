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

package org.apache.hop.core.gui.plugin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GuiRegistryTest {
  private GuiRegistry registry;

  @BeforeEach
  void before() {
    registry = GuiRegistry.getInstance();
  }

  @Test
  void retrieveClassInstance() {
    Object object1 = new Object();
    registry.registerGuiPluginObject("hop-gui-id1", "class1", "instance1", object1);
    Object verifyObject1111 = registry.findGuiPluginObject("hop-gui-id1", "class1", "instance1");
    assertEquals(object1, verifyObject1111);
    // class2 is not found
    Object verifyObject1211 = registry.findGuiPluginObject("hop-gui-id1", "class2", "instance1");
    assertNull(verifyObject1211);
  }

  @Test
  void retrieveSameObjectMultipleInstances() {
    Object object1 = new Object();
    registry.registerGuiPluginObject("hop-gui-id1", "class1", "instance1", object1);
    registry.registerGuiPluginObject("hop-gui-id1", "class1", "instance2", object1);

    Object verifyObject1121 = registry.findGuiPluginObject("hop-gui-id1", "class1", "instance2");
    assertEquals(object1, verifyObject1121);
    Object verifyObject1111 = registry.findGuiPluginObject("hop-gui-id1", "class1", "instance1");
    assertEquals(object1, verifyObject1111);
  }

  @Test
  void registeringAFieldWidgetTwiceKeepsOneElement() throws Exception {
    String dataClassName = getClass().getName() + "#fieldTwice";
    Field field = WidgetSample.class.getDeclaredField("name");
    GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);

    registry.addGuiWidgetElement(dataClassName, element, field);
    registry.addGuiWidgetElement(dataClassName, element, field);

    GuiElements elements = registry.findGuiElements(dataClassName, WidgetSample.PARENT_ID);
    assertEquals(1, elements.getChildren().size());
  }

  @Test
  void registeringAWidgetWithoutAnExplicitIdTwiceKeepsOneElement() throws Exception {
    // Without an id in the annotation the element takes the field name as its id.
    String dataClassName = getClass().getName() + "#noIdTwice";
    Field field = WidgetSample.class.getDeclaredField("sampleSize");
    GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);

    registry.addGuiWidgetElement(dataClassName, element, field);
    registry.addGuiWidgetElement(dataClassName, element, field);

    GuiElements elements = registry.findGuiElements(dataClassName, WidgetSample.PARENT_ID);
    assertEquals(1, elements.getChildren().size());
    assertEquals("sampleSize", elements.getChildren().get(0).getId());
  }

  @Test
  void registeringAMethodWidgetTwiceKeepsOneElement() throws Exception {
    String dataClassName = getClass().getName() + "#methodTwice";
    Method method = WidgetSample.class.getDeclaredMethod("browse");
    GuiWidgetElement element = method.getAnnotation(GuiWidgetElement.class);
    ClassLoader classLoader = getClass().getClassLoader();

    registry.addGuiWidgetElement(element, method, dataClassName, classLoader);
    registry.addGuiWidgetElement(element, method, dataClassName, classLoader);

    GuiElements elements = registry.findGuiElements(dataClassName, WidgetSample.PARENT_ID);
    assertEquals(1, elements.getChildren().size());
  }

  @Test
  void anIgnoredDeclarationStillHidesARegisteredWidget() throws Exception {
    String dataClassName = getClass().getName() + "#ignored";
    Field field = WidgetSample.class.getDeclaredField("name");
    Field ignoredField = WidgetSample.class.getDeclaredField("hiddenName");

    registry.addGuiWidgetElement(dataClassName, field.getAnnotation(GuiWidgetElement.class), field);
    registry.addGuiWidgetElement(
        dataClassName, ignoredField.getAnnotation(GuiWidgetElement.class), ignoredField);

    GuiElements elements = registry.findGuiElements(dataClassName, WidgetSample.PARENT_ID);
    assertEquals(1, elements.getChildren().size());
    assertTrue(elements.getChildren().get(0).isIgnored());
  }

  @Test
  void tableFieldRegistersTheRowColumns() throws Exception {
    String dataClassName = getClass().getName() + "#tables";
    registerTableFields(dataClassName);
    registerTableFields(dataClassName);

    GuiElements elements = registry.findGuiElements(dataClassName, TableHost.PARENT_ID);
    assertEquals(6, elements.getChildren().size());

    GuiElements rows = elements.findChild("rows");
    assertEquals(SampleRow.class, rows.getTableRowClass());
    assertEquals(4, rows.getTableRows());
    assertEquals(List.of("kind", "name", "active", "choice"), columnIds(rows));

    GuiTableColumnElement name = column(rows, "name");
    assertEquals(GuiTableColumnType.TEXT, name.getType());
    assertEquals(String.class, name.getFieldClass());
    assertEquals("readName", name.getGetterMethod());
    assertEquals("writeName", name.getSetterMethod());
    assertFalse(name.isVariables());
    assertTrue(name.isPassword());
    assertEquals(80, name.getWidth());

    GuiTableColumnElement kind = column(rows, "kind");
    assertEquals(GuiTableColumnType.COMBO, kind.getType());
    assertEquals(SampleKind.class, kind.getFieldClass());
    assertTrue(kind.isVariables());

    GuiTableColumnElement active = column(rows, "active");
    assertEquals(GuiTableColumnType.CHECKBOX, active.getType());
    assertEquals(boolean.class, active.getFieldClass());
    assertEquals("isActive", active.getGetterMethod());
    assertEquals("setActive", active.getSetterMethod());

    GuiTableColumnElement choice = column(rows, "choice");
    assertEquals(String.class, choice.getFieldClass());
    assertEquals("choices", choice.getComboValuesMethod());

    GuiElements children = elements.findChild("children");
    assertEquals(ChildRow.class, children.getTableRowClass());
    assertEquals(List.of("name", "extra", "note"), columnIds(children));
    assertEquals("Child name", column(children, "name").getLabel());

    GuiElements wild = elements.findChild("wild");
    assertEquals(SampleRow.class, wild.getTableRowClass());
    assertEquals(4, wild.getTableColumns().size());

    GuiElements strings = elements.findChild("strings");
    assertEquals(String.class, strings.getTableRowClass());
    assertTrue(strings.getTableColumns().isEmpty());

    GuiElements raw = elements.findChild("raw");
    assertNull(raw.getTableRowClass());
    assertTrue(raw.getTableColumns().isEmpty());

    GuiElements bad = elements.findChild("bad");
    assertNull(bad.getTableRowClass());
    assertTrue(bad.getTableColumns().isEmpty());
    assertEquals(5, bad.getTableRows());
  }

  @Test
  void tableWidgetOnAMethodIsIgnored() throws Exception {
    String dataClassName = getClass().getName() + "#tableMethod";
    Method method = TableHost.class.getDeclaredMethod("notAGrid");
    registry.addGuiWidgetElement(
        method.getAnnotation(GuiWidgetElement.class),
        method,
        dataClassName,
        getClass().getClassLoader());

    assertNull(registry.findGuiElements(dataClassName, TableHost.PARENT_ID));
  }

  private void registerTableFields(String dataClassName) throws Exception {
    for (Field field : TableHost.class.getDeclaredFields()) {
      GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
      if (element != null) {
        registry.addGuiWidgetElement(dataClassName, element, field);
      }
    }
  }

  private static List<String> columnIds(GuiElements element) {
    List<String> ids = new ArrayList<>();
    for (GuiTableColumnElement column : element.getTableColumns()) {
      ids.add(column.getId());
    }
    return ids;
  }

  private static GuiTableColumnElement column(GuiElements element, String id) {
    for (GuiTableColumnElement column : element.getTableColumns()) {
      if (id.equals(column.getId())) {
        return column;
      }
    }
    throw new AssertionError("Missing column " + id);
  }

  private static class WidgetSample {
    static final String PARENT_ID = "GuiRegistryTest-parent";

    @GuiWidgetElement(id = "name", type = GuiElementType.TEXT, parentId = PARENT_ID)
    private String name;

    @GuiWidgetElement(id = "name", type = GuiElementType.TEXT, parentId = PARENT_ID, ignored = true)
    private String hiddenName;

    @GuiWidgetElement(type = GuiElementType.TEXT, parentId = PARENT_ID)
    private String sampleSize;

    @GuiWidgetElement(id = "browse", type = GuiElementType.BUTTON, parentId = PARENT_ID)
    void browse() {
      // Only the annotation matters here.
    }
  }

  public static class TableHost {
    static final String PARENT_ID = "GuiRegistryTest-table";

    @GuiWidgetElement(id = "rows", type = GuiElementType.TABLE, parentId = PARENT_ID, tableRows = 4)
    private List<SampleRow> rows;

    @GuiWidgetElement(id = "children", type = GuiElementType.TABLE, parentId = PARENT_ID)
    private List<ChildRow> children;

    @GuiWidgetElement(id = "wild", type = GuiElementType.TABLE, parentId = PARENT_ID)
    private List<? extends SampleRow> wild;

    @GuiWidgetElement(id = "strings", type = GuiElementType.TABLE, parentId = PARENT_ID)
    private List<String> strings;

    @SuppressWarnings("rawtypes")
    @GuiWidgetElement(id = "raw", type = GuiElementType.TABLE, parentId = PARENT_ID)
    private List rawRows;

    @GuiWidgetElement(id = "bad", type = GuiElementType.TABLE, parentId = PARENT_ID, tableRows = 0)
    private String bad;

    @GuiWidgetElement(id = "not-a-grid", type = GuiElementType.TABLE, parentId = PARENT_ID)
    public void notAGrid() {
      // TABLE on a method is rejected by the registry.
    }
  }

  public enum SampleKind {
    LEFT,
    RIGHT
  }

  public static class SampleRow {
    @GuiTableColumn(order = "10", type = GuiTableColumnType.COMBO, label = "Kind")
    private SampleKind kind;

    @GuiTableColumn(
        order = "20",
        type = GuiTableColumnType.TEXT,
        label = "Name",
        getterMethod = "readName",
        setterMethod = "writeName",
        variables = false,
        password = true,
        width = 80)
    private String name;

    @GuiTableColumn(order = "30", type = GuiTableColumnType.CHECKBOX, label = "Active")
    private boolean active;

    @GuiTableColumn(
        order = "40",
        type = GuiTableColumnType.COMBO,
        label = "Choice",
        comboValuesMethod = "choices")
    private String choice;

    @GuiTableColumn(order = "50", type = GuiTableColumnType.TEXT, label = "Count")
    private int count;

    private String note;

    public SampleKind getKind() {
      return kind;
    }

    public void setKind(SampleKind kind) {
      this.kind = kind;
    }

    public String readName() {
      return name;
    }

    public void writeName(String name) {
      this.name = name;
    }

    public boolean isActive() {
      return active;
    }

    public void setActive(boolean active) {
      this.active = active;
    }

    public String getChoice() {
      return choice;
    }

    public void setChoice(String choice) {
      this.choice = choice;
    }

    public int getCount() {
      return count;
    }

    public void setCount(int count) {
      this.count = count;
    }
  }

  public static class ParentRow {
    @GuiTableColumn(order = "10", type = GuiTableColumnType.TEXT, label = "Parent name")
    private String name;

    @GuiTableColumn(order = "30", type = GuiTableColumnType.TEXT, label = "Note")
    private String note;

    @GuiTableColumn(order = "40", type = GuiTableColumnType.TEXT, label = "Hidden")
    private String hidden;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getNote() {
      return note;
    }

    public void setNote(String note) {
      this.note = note;
    }

    public String getHidden() {
      return hidden;
    }

    public void setHidden(String hidden) {
      this.hidden = hidden;
    }
  }

  public static class ChildRow extends ParentRow {
    @GuiTableColumn(order = "10", type = GuiTableColumnType.TEXT, label = "Child name")
    private String name;

    @GuiTableColumn(order = "20", type = GuiTableColumnType.TEXT, label = "Extra")
    private String extra;

    private String hidden;

    @Override
    public String getName() {
      return name;
    }

    @Override
    public void setName(String name) {
      this.name = name;
    }

    public String getExtra() {
      return extra;
    }

    public void setExtra(String extra) {
      this.extra = extra;
    }
  }
}
