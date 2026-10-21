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

package org.apache.hop.ui.hopgui.markdown;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.ui.core.widget.TextComposite;
import org.apache.hop.ui.core.widget.editor.IContentEditorWidget;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.widgets.Control;
import org.junit.jupiter.api.Test;

class MarkdownEditActionsTest {

  @Test
  void toolbarButtonsAreRegisteredOnBothEditors() throws Exception {
    assertToolbar(
        MarkdownEditActions.class.getMethod("link", IContentEditorWidget.class),
        IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
        MarkdownEditActions.ID_CONTENT_LINK,
        true);
    assertToolbar(
        MarkdownEditActions.class.getMethod("image", IContentEditorWidget.class),
        IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
        MarkdownEditActions.ID_CONTENT_IMAGE,
        false);
    assertToolbar(
        MarkdownEditActions.class.getMethod("bold", IContentEditorWidget.class),
        IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
        MarkdownEditActions.ID_CONTENT_BOLD,
        false);
    assertToolbar(
        MarkdownEditActions.class.getMethod("italic", IContentEditorWidget.class),
        IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
        MarkdownEditActions.ID_CONTENT_ITALIC,
        false);
    assertToolbar(
        MarkdownEditActions.class.getMethod("table", IContentEditorWidget.class),
        IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
        MarkdownEditActions.ID_CONTENT_TABLE,
        false);
    assertToolbar(
        MarkdownEditActions.class.getMethod("code", IContentEditorWidget.class),
        IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
        MarkdownEditActions.ID_CONTENT_CODE,
        false);
    assertToolbar(
        MarkdownEditActions.class.getMethod("header", IContentEditorWidget.class),
        IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
        MarkdownEditActions.ID_CONTENT_HEADER,
        false);

    assertToolbar(
        MarkdownEditActions.class.getMethod("link", TextComposite.class),
        TextComposite.ID_TOOLBAR,
        MarkdownEditActions.ID_TEXT_LINK,
        true);
    assertToolbar(
        MarkdownEditActions.class.getMethod("header", TextComposite.class),
        TextComposite.ID_TOOLBAR,
        MarkdownEditActions.ID_TEXT_HEADER,
        false);
  }

  @Test
  void filtersFollowLanguageAndStyle() {
    assertTrue(
        MarkdownEditActions.showForContentEditor(
            MarkdownEditActions.ID_CONTENT_BOLD, editor("markdown")));
    assertFalse(
        MarkdownEditActions.showForContentEditor(
            MarkdownEditActions.ID_CONTENT_BOLD, editor("json")));
    assertTrue(
        MarkdownEditActions.showForContentEditor(
            "ContentEditor-Toolbar-10000-undo", editor("json")));
    assertFalse(
        MarkdownEditActions.showForContentEditor(MarkdownEditActions.ID_CONTENT_LINK, "markdown"));

    assertTrue(
        MarkdownEditActions.showForStyle(
            MarkdownEditActions.ID_TEXT_TABLE, TextComposite.STYLE_TYPE_MARKDOWN));
    assertFalse(
        MarkdownEditActions.showForStyle(
            MarkdownEditActions.ID_TEXT_TABLE, TextComposite.STYLE_TYPE_TEXT));
    assertTrue(
        MarkdownEditActions.showForStyle(
            TextComposite.ID_TOOLBAR_UNDO, TextComposite.STYLE_TYPE_TEXT));
    assertFalse(
        MarkdownEditActions.showForTextComposite(MarkdownEditActions.ID_TEXT_BOLD, new Object()));
    assertTrue(
        MarkdownEditActions.showForTextComposite(TextComposite.ID_TOOLBAR_COPY, new Object()));
  }

  private static void assertToolbar(Method method, String root, String id, boolean separator) {
    GuiToolbarElement element = method.getAnnotation(GuiToolbarElement.class);
    assertNotNull(element, method.getName());
    assertEquals(root, element.root());
    assertEquals(id, element.id());
    assertEquals(separator, element.separator());
    assertTrue(element.image().startsWith("ui/images/"));
  }

  private static IContentEditorWidget editor(String language) {
    return new IContentEditorWidget() {
      @Override
      public Control getControl() {
        return null;
      }

      @Override
      public String getText() {
        return "";
      }

      @Override
      public void setText(String text) {}

      @Override
      public void setTextSuppressModify(String text) {}

      @Override
      public String getLanguage() {
        return language;
      }

      @Override
      public void setLanguage(String languageId) {}

      @Override
      public void setReadOnly(boolean readOnly) {}

      @Override
      public void addModifyListener(ModifyListener listener) {}

      @Override
      public void removeModifyListener(ModifyListener listener) {}

      @Override
      public void selectAll() {}

      @Override
      public void unselectAll() {}

      @Override
      public void copy() {}

      @Override
      public void cut() {}

      @Override
      public void paste() {}

      @Override
      public void undo() {}

      @Override
      public void redo() {}

      @Override
      public String getSelectionText() {
        return "";
      }

      @Override
      public int getSelectionCount() {
        return 0;
      }

      @Override
      public void setSelection(int start, int end) {}

      @Override
      public int getCaretPosition() {
        return 0;
      }

      @Override
      public void setCaretPosition(int position) {}

      @Override
      public void insert(String text) {}

      @Override
      public boolean isEditable() {
        return true;
      }
    };
  }
}
