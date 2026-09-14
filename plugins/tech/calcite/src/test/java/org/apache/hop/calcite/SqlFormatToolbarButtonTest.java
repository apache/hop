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

package org.apache.hop.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.ui.core.widget.IFindReplaceTarget;
import org.apache.hop.ui.core.widget.TextComposite;
import org.apache.hop.ui.core.widget.editor.IContentEditorWidget;
import org.junit.jupiter.api.Test;

class SqlFormatToolbarButtonTest {

  @Test
  void contentEditorButtonIsOnTheSqlToolbar() throws Exception {
    Method method = SqlFormatToolbarButton.class.getMethod("formatSql", IContentEditorWidget.class);
    GuiToolbarElement element = method.getAnnotation(GuiToolbarElement.class);
    assertNotNull(element);
    assertEquals(IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID, element.root());
    assertEquals(SqlFormatToolbarButton.ID_CONTENT_EDITOR_FORMAT_SQL, element.id());
    assertEquals("format-sql.svg", element.image());
    assertTrue(element.separator());
  }

  @Test
  void textCompositeButtonIsOnTheSqlToolbar() throws Exception {
    Method method = SqlFormatToolbarButton.class.getMethod("formatSql", TextComposite.class);
    GuiToolbarElement element = method.getAnnotation(GuiToolbarElement.class);
    assertNotNull(element);
    assertEquals(TextComposite.ID_TOOLBAR, element.root());
    assertEquals(SqlFormatToolbarButton.ID_TEXTCOMPOSITE_FORMAT_SQL, element.id());
  }

  @Test
  void filtersLeaveOtherToolbarItemsAlone() {
    assertTrue(SqlFormatToolbarButton.showForSqlEditor("ContentEditor-Toolbar-10000-undo", this));
    assertTrue(
        SqlFormatToolbarButton.showForSqlTextComposite("textcomposite-toolbar-10000-undo", this));
  }

  @Test
  void filtersHideFormatButtonForNonSqlHosts() {
    assertFalse(
        SqlFormatToolbarButton.showForSqlEditor(
            SqlFormatToolbarButton.ID_CONTENT_EDITOR_FORMAT_SQL, this));
    assertFalse(
        SqlFormatToolbarButton.showForSqlTextComposite(
            SqlFormatToolbarButton.ID_TEXTCOMPOSITE_FORMAT_SQL, this));
  }

  @Test
  void applyFormattedReplacesWholeBufferThroughInsert() {
    FakeTarget target = new FakeTarget("select 1");
    SqlFormatToolbarButton.applyFormatted(target, "select 1", "SELECT\n  1", false);
    assertEquals("SELECT\n  1", target.getText());
    assertEquals(1, target.insertCount);
    assertEquals(0, target.setTextCount);
    assertTrue(target.toolbarUpdated);
  }

  @Test
  void applyFormattedReplacesSelectionThroughInsert() {
    FakeTarget target = new FakeTarget("keep select 1 tail");
    target.setSelection(5, 13);
    SqlFormatToolbarButton.applyFormatted(target, "select 1", "SELECT\n  1", true);
    assertEquals("keep SELECT\n  1 tail", target.getText());
    assertEquals(1, target.insertCount);
    assertEquals(0, target.setTextCount);
  }

  @Test
  void applyFormattedIsNoOpWhenUnchanged() {
    FakeTarget target = new FakeTarget("select 1");
    SqlFormatToolbarButton.applyFormatted(target, "select 1", "select 1", false);
    assertEquals("select 1", target.getText());
    assertEquals(0, target.insertCount);
    assertFalse(target.toolbarUpdated);
  }

  private static final class FakeTarget implements IFindReplaceTarget {
    private String text;
    private int selStart;
    private int selEnd;
    private int insertCount;
    private int setTextCount;
    private boolean toolbarUpdated;

    private FakeTarget(String text) {
      this.text = text;
    }

    @Override
    public String getText() {
      return text;
    }

    @Override
    public void setText(String text) {
      setTextCount++;
      this.text = text != null ? text : "";
    }

    @Override
    public String getSelectionText() {
      if (selEnd <= selStart || selStart >= text.length()) {
        return "";
      }
      return text.substring(selStart, Math.min(selEnd, text.length()));
    }

    @Override
    public int getSelectionCount() {
      return Math.max(0, selEnd - selStart);
    }

    @Override
    public void setSelection(int start, int end) {
      selStart = Math.max(0, start);
      selEnd = Math.max(selStart, end);
    }

    @Override
    public int getCaretPosition() {
      return selEnd;
    }

    @Override
    public void setCaretPosition(int position) {
      selStart = position;
      selEnd = position;
    }

    @Override
    public void insert(String replacement) {
      insertCount++;
      String safe = replacement != null ? replacement : "";
      text = text.substring(0, selStart) + safe + text.substring(selEnd);
      selStart = selStart + safe.length();
      selEnd = selStart;
    }

    @Override
    public boolean isEditable() {
      return true;
    }

    @Override
    public boolean isDisposed() {
      return false;
    }

    @Override
    public boolean setFocus() {
      return true;
    }

    @Override
    public void updateToolbar() {
      toolbarUpdated = true;
    }
  }
}
