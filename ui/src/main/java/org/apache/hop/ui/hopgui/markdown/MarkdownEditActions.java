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

import org.apache.hop.core.gui.markdown.MarkdownEditing;
import org.apache.hop.core.gui.markdown.MarkdownEditing.Edit;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementFilter;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterStringDialog;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.widget.IFindReplaceTarget;
import org.apache.hop.ui.core.widget.TextComposite;
import org.apache.hop.ui.core.widget.editor.IContentEditorWidget;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Shell;

/**
 * Markdown toolbar actions shared by canvas notes ({@link TextComposite}) and explorer {@code .md}
 * files ({@link IContentEditorWidget}). Each action is registered on both toolbars. Filters hide
 * the buttons everywhere else.
 */
@GuiPlugin(name = "Markdown editing")
public class MarkdownEditActions {

  private static final Class<?> PKG = MarkdownEditActions.class;

  public static final String LANGUAGE = "markdown";

  public static final String ID_CONTENT_LINK = "ContentEditor-Toolbar-50000-markdown-link";
  public static final String ID_CONTENT_IMAGE = "ContentEditor-Toolbar-50010-markdown-image";
  public static final String ID_CONTENT_BOLD = "ContentEditor-Toolbar-50020-markdown-bold";
  public static final String ID_CONTENT_ITALIC = "ContentEditor-Toolbar-50030-markdown-italic";
  public static final String ID_CONTENT_TABLE = "ContentEditor-Toolbar-50040-markdown-table";
  public static final String ID_CONTENT_CODE = "ContentEditor-Toolbar-50050-markdown-code";
  public static final String ID_CONTENT_HEADER = "ContentEditor-Toolbar-50060-markdown-header";

  public static final String ID_TEXT_LINK = "textcomposite-toolbar-10300-markdown-link";
  public static final String ID_TEXT_IMAGE = "textcomposite-toolbar-10310-markdown-image";
  public static final String ID_TEXT_BOLD = "textcomposite-toolbar-10320-markdown-bold";
  public static final String ID_TEXT_ITALIC = "textcomposite-toolbar-10330-markdown-italic";
  public static final String ID_TEXT_TABLE = "textcomposite-toolbar-10340-markdown-table";
  public static final String ID_TEXT_CODE = "textcomposite-toolbar-10350-markdown-code";
  public static final String ID_TEXT_HEADER = "textcomposite-toolbar-10360-markdown-header";

  private static final String[] SELECTION_IDS = {
    ID_CONTENT_BOLD, ID_CONTENT_ITALIC, ID_TEXT_BOLD, ID_TEXT_ITALIC
  };

  private static final String[] ALWAYS_IDS = {
    ID_CONTENT_LINK,
    ID_CONTENT_IMAGE,
    ID_CONTENT_TABLE,
    ID_CONTENT_CODE,
    ID_CONTENT_HEADER,
    ID_TEXT_LINK,
    ID_TEXT_IMAGE,
    ID_TEXT_TABLE,
    ID_TEXT_CODE,
    ID_TEXT_HEADER
  };

  private static final String[] IMAGE_EXTENSIONS = {
    "*.png;*.jpg;*.jpeg;*.gif;*.svg", "*.png", "*.jpg;*.jpeg", "*.gif", "*.svg"
  };

  private MarkdownEditActions() {}

  /** Show the content-editor buttons only while the editor language is markdown. */
  @GuiToolbarElementFilter(parentId = IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID)
  public static boolean showForContentEditor(String itemId, Object guiPluginInstance) {
    if (!isContentEditorItem(itemId)) {
      return true;
    }
    if (!(guiPluginInstance instanceof IContentEditorWidget editor)) {
      return false;
    }
    return LANGUAGE.equalsIgnoreCase(editor.getLanguage());
  }

  /** Show the note-editor buttons only on a markdown-styled {@link TextComposite}. */
  @GuiToolbarElementFilter(parentId = TextComposite.ID_TOOLBAR)
  public static boolean showForTextComposite(String itemId, Object guiPluginInstance) {
    if (!(guiPluginInstance instanceof TextComposite text)) {
      return showForStyle(itemId, null);
    }
    return showForStyle(itemId, text.getStyleType());
  }

  /** Filter predicate used by {@link #showForTextComposite} and unit tests. */
  public static boolean showForStyle(String itemId, String styleType) {
    if (!isTextCompositeItem(itemId)) {
      return true;
    }
    return TextComposite.STYLE_TYPE_MARKDOWN.equals(styleType);
  }

  public static void updateContentEditor(IContentEditorWidget editor) {
    if (editor == null) {
      return;
    }
    boolean markdown = LANGUAGE.equalsIgnoreCase(editor.getLanguage()) && editor.isEditable();
    enable(editor.getToolbarWidgets(), markdown, editor.getSelectionCount());
  }

  public static void updateTextComposite(TextComposite text) {
    if (text == null) {
      return;
    }
    MarkdownEditContext context = MarkdownEditContext.from(text);
    boolean markdown =
        TextComposite.STYLE_TYPE_MARKDOWN.equals(text.getStyleType())
            && (context == null || context.active())
            && text.isEditable();
    enable(text.getToolbarWidgets(), markdown, text.getSelectionCount());
  }

  @GuiToolbarElement(
      root = IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = ID_CONTENT_LINK,
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/link.svg",
      toolTip = "i18n::MarkdownEditActions.Link.Tooltip",
      separator = true)
  public static void link(IContentEditorWidget editor) {
    linkTarget(editor);
  }

  @GuiToolbarElement(
      root = TextComposite.ID_TOOLBAR,
      id = ID_TEXT_LINK,
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/link.svg",
      toolTip = "i18n::MarkdownEditActions.Link.Tooltip",
      separator = true)
  public static void link(TextComposite text) {
    linkTarget(text);
  }

  @GuiToolbarElement(
      root = IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = ID_CONTENT_IMAGE,
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/image.svg",
      toolTip = "i18n::MarkdownEditActions.Image.Tooltip")
  public static void image(IContentEditorWidget editor) {
    insertImage(editor);
  }

  @GuiToolbarElement(
      root = TextComposite.ID_TOOLBAR,
      id = ID_TEXT_IMAGE,
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/image.svg",
      toolTip = "i18n::MarkdownEditActions.Image.Tooltip")
  public static void image(TextComposite text) {
    insertImage(text);
  }

  @GuiToolbarElement(
      root = IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = ID_CONTENT_BOLD,
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/bold.svg",
      toolTip = "i18n::MarkdownEditActions.Bold.Tooltip")
  public static void bold(IContentEditorWidget editor) {
    toggleBold(editor);
  }

  @GuiToolbarElement(
      root = TextComposite.ID_TOOLBAR,
      id = ID_TEXT_BOLD,
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/bold.svg",
      toolTip = "i18n::MarkdownEditActions.Bold.Tooltip")
  public static void bold(TextComposite text) {
    toggleBold(text);
  }

  @GuiToolbarElement(
      root = IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = ID_CONTENT_ITALIC,
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/italic.svg",
      toolTip = "i18n::MarkdownEditActions.Italic.Tooltip")
  public static void italic(IContentEditorWidget editor) {
    toggleItalic(editor);
  }

  @GuiToolbarElement(
      root = TextComposite.ID_TOOLBAR,
      id = ID_TEXT_ITALIC,
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/italic.svg",
      toolTip = "i18n::MarkdownEditActions.Italic.Tooltip")
  public static void italic(TextComposite text) {
    toggleItalic(text);
  }

  @GuiToolbarElement(
      root = IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = ID_CONTENT_TABLE,
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/table.svg",
      toolTip = "i18n::MarkdownEditActions.Table.Tooltip")
  public static void table(IContentEditorWidget editor) {
    insertTable(editor);
  }

  @GuiToolbarElement(
      root = TextComposite.ID_TOOLBAR,
      id = ID_TEXT_TABLE,
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/table.svg",
      toolTip = "i18n::MarkdownEditActions.Table.Tooltip")
  public static void table(TextComposite text) {
    insertTable(text);
  }

  @GuiToolbarElement(
      root = IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = ID_CONTENT_CODE,
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/code.svg",
      toolTip = "i18n::MarkdownEditActions.Code.Tooltip")
  public static void code(IContentEditorWidget editor) {
    insertCode(editor);
  }

  @GuiToolbarElement(
      root = TextComposite.ID_TOOLBAR,
      id = ID_TEXT_CODE,
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/code.svg",
      toolTip = "i18n::MarkdownEditActions.Code.Tooltip")
  public static void code(TextComposite text) {
    insertCode(text);
  }

  @GuiToolbarElement(
      root = IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = ID_CONTENT_HEADER,
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/header.svg",
      toolTip = "i18n::MarkdownEditActions.Header.Tooltip")
  public static void header(IContentEditorWidget editor) {
    showHeaderMenu(editor);
  }

  @GuiToolbarElement(
      root = TextComposite.ID_TOOLBAR,
      id = ID_TEXT_HEADER,
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/header.svg",
      toolTip = "i18n::MarkdownEditActions.Header.Tooltip")
  public static void header(TextComposite text) {
    showHeaderMenu(text);
  }

  private static void linkTarget(IFindReplaceTarget editor) {
    Shell shell = shellOf(editor);
    if (shell == null) {
      return;
    }
    Menu menu = new Menu(shell, SWT.POP_UP);
    MenuItem urlItem = new MenuItem(menu, SWT.PUSH);
    urlItem.setText(BaseMessages.getString(PKG, "MarkdownEditActions.Link.Url"));
    urlItem.addListener(SWT.Selection, e -> askUrl(editor));
    MenuItem fileItem = new MenuItem(menu, SWT.PUSH);
    fileItem.setText(BaseMessages.getString(PKG, "MarkdownEditActions.Link.File"));
    fileItem.addListener(SWT.Selection, e -> askFile(editor));
    showPopup(anchor(editor, ID_TEXT_LINK, ID_CONTENT_LINK), menu);
  }

  private static void askUrl(IFindReplaceTarget editor) {
    Shell shell = shellOf(editor);
    if (shell == null) {
      return;
    }
    String url =
        new EnterStringDialog(
                shell,
                "",
                BaseMessages.getString(PKG, "MarkdownEditActions.Url.Title"),
                BaseMessages.getString(PKG, "MarkdownEditActions.Url.Message"))
            .open();
    if (Utils.isEmpty(url)) {
      return;
    }
    String label = editor.getSelectionText();
    insertSnippet(editor, MarkdownEditing.link(Utils.isEmpty(label) ? url : label, url.trim()));
  }

  private static void askFile(IFindReplaceTarget editor) {
    Shell shell = shellOf(editor);
    if (shell == null) {
      return;
    }
    String picked =
        BaseDialog.presentFileDialog(
            shell,
            null,
            variablesOf(editor),
            new String[] {"*.*"},
            new String[] {BaseMessages.getString(PKG, "MarkdownEditActions.File.All")},
            false);
    if (Utils.isEmpty(picked)) {
      return;
    }
    String path = markdownPath(editor, picked);
    String label = editor.getSelectionText();
    if (Utils.isEmpty(label)) {
      label = MarkdownEditing.fileName(path);
    }
    insertSnippet(editor, MarkdownEditing.link(label, path));
  }

  private static void insertImage(IFindReplaceTarget editor) {
    Shell shell = shellOf(editor);
    if (shell == null) {
      return;
    }
    String picked =
        BaseDialog.presentFileDialog(
            shell,
            null,
            variablesOf(editor),
            IMAGE_EXTENSIONS,
            new String[] {
              BaseMessages.getString(PKG, "MarkdownEditActions.Image.All"),
              BaseMessages.getString(PKG, "MarkdownEditActions.Image.Png"),
              BaseMessages.getString(PKG, "MarkdownEditActions.Image.Jpeg"),
              BaseMessages.getString(PKG, "MarkdownEditActions.Image.Gif"),
              BaseMessages.getString(PKG, "MarkdownEditActions.Image.Svg")
            },
            false);
    if (Utils.isEmpty(picked)) {
      return;
    }
    String path = markdownPath(editor, picked);
    String alt = editor.getSelectionText();
    insertSnippet(editor, MarkdownEditing.image(alt, path));
  }

  private static void toggleBold(IFindReplaceTarget editor) {
    if (editor == null || editor.getSelectionCount() <= 0) {
      return;
    }
    int[] range = range(editor);
    apply(editor, MarkdownEditing.toggleBold(editor.getText(), range[0], range[1]));
  }

  private static void toggleItalic(IFindReplaceTarget editor) {
    if (editor == null || editor.getSelectionCount() <= 0) {
      return;
    }
    int[] range = range(editor);
    apply(editor, MarkdownEditing.toggleItalic(editor.getText(), range[0], range[1]));
  }

  private static void insertCode(IFindReplaceTarget editor) {
    if (editor == null) {
      return;
    }
    int[] range = range(editor);
    apply(editor, MarkdownEditing.codeBlock(editor.getText(), range[0], range[1]));
  }

  private static void insertTable(IFindReplaceTarget editor) {
    Shell shell = shellOf(editor);
    if (shell == null) {
      return;
    }
    String table = MarkdownTableDialog.open(shell);
    if (Utils.isEmpty(table)) {
      return;
    }
    insertSnippet(editor, table);
  }

  private static void showHeaderMenu(IFindReplaceTarget editor) {
    Shell shell = shellOf(editor);
    if (shell == null) {
      return;
    }
    Menu menu = new Menu(shell, SWT.POP_UP);
    addHeaderItem(menu, editor, 1, "MarkdownEditActions.Header.1");
    addHeaderItem(menu, editor, 2, "MarkdownEditActions.Header.2");
    addHeaderItem(menu, editor, 3, "MarkdownEditActions.Header.3");
    addHeaderItem(menu, editor, 4, "MarkdownEditActions.Header.4");
    new MenuItem(menu, SWT.SEPARATOR);
    addHeaderItem(menu, editor, 0, "MarkdownEditActions.Header.Regular");
    showPopup(anchor(editor, ID_TEXT_HEADER, ID_CONTENT_HEADER), menu);
  }

  private static void addHeaderItem(
      Menu menu, IFindReplaceTarget editor, int level, String labelKey) {
    MenuItem item = new MenuItem(menu, SWT.PUSH);
    item.setText(BaseMessages.getString(PKG, labelKey));
    item.addListener(
        SWT.Selection,
        e -> {
          int[] range = range(editor);
          apply(editor, MarkdownEditing.applyHeader(editor.getText(), range[0], range[1], level));
        });
  }

  private static void insertSnippet(IFindReplaceTarget editor, String snippet) {
    if (editor == null || snippet == null) {
      return;
    }
    int[] range = range(editor);
    apply(editor, new Edit(range[0], range[1], snippet, range[0] + snippet.length()));
  }

  private static void apply(IFindReplaceTarget editor, Edit edit) {
    if (editor == null || edit == null || editor.isDisposed() || !editor.isEditable()) {
      return;
    }
    editor.setSelection(edit.start(), edit.end());
    editor.insert(edit.replacement());
    editor.setCaretPosition(edit.caret());
    editor.setFocus();
    editor.updateToolbar();
  }

  private static int[] range(IFindReplaceTarget editor) {
    int start = Math.max(0, editor.getSelectionStart());
    int count = Math.max(0, editor.getSelectionCount());
    return new int[] {start, start + count};
  }

  private static String markdownPath(IFindReplaceTarget editor, String picked) {
    MarkdownEditContext context = contextOf(editor);
    String base = context == null ? null : context.baseFilename();
    return MarkdownEditing.toMarkdownPath(variablesOf(editor), base, picked);
  }

  private static IVariables variablesOf(IFindReplaceTarget editor) {
    MarkdownEditContext context = contextOf(editor);
    if (context != null && context.variables() != null) {
      return context.variables();
    }
    HopGui hopGui = HopGui.getInstance();
    return hopGui != null ? hopGui.getVariables() : null;
  }

  private static MarkdownEditContext contextOf(IFindReplaceTarget editor) {
    if (editor instanceof TextComposite text) {
      return MarkdownEditContext.from(text);
    }
    if (editor instanceof IContentEditorWidget widget) {
      return MarkdownEditContext.from(widget.getControl());
    }
    return null;
  }

  private static void enable(GuiToolbarWidgets widgets, boolean markdown, int selectionCount) {
    if (widgets == null) {
      return;
    }
    boolean selected = markdown && selectionCount > 0;
    for (String id : ALWAYS_IDS) {
      widgets.enableToolbarItem(id, markdown);
    }
    for (String id : SELECTION_IDS) {
      widgets.enableToolbarItem(id, selected);
    }
  }

  static boolean isContentEditorItem(String itemId) {
    return ID_CONTENT_LINK.equals(itemId)
        || ID_CONTENT_IMAGE.equals(itemId)
        || ID_CONTENT_BOLD.equals(itemId)
        || ID_CONTENT_ITALIC.equals(itemId)
        || ID_CONTENT_TABLE.equals(itemId)
        || ID_CONTENT_CODE.equals(itemId)
        || ID_CONTENT_HEADER.equals(itemId);
  }

  static boolean isTextCompositeItem(String itemId) {
    return ID_TEXT_LINK.equals(itemId)
        || ID_TEXT_IMAGE.equals(itemId)
        || ID_TEXT_BOLD.equals(itemId)
        || ID_TEXT_ITALIC.equals(itemId)
        || ID_TEXT_TABLE.equals(itemId)
        || ID_TEXT_CODE.equals(itemId)
        || ID_TEXT_HEADER.equals(itemId);
  }

  private static Shell shellOf(IFindReplaceTarget editor) {
    if (editor == null || editor.isDisposed()) {
      return null;
    }
    if (editor instanceof TextComposite text) {
      return text.isDisposed() ? null : text.getShell();
    }
    if (editor instanceof IContentEditorWidget widget) {
      Control control = widget.getControl();
      return control == null || control.isDisposed() ? null : control.getShell();
    }
    return null;
  }

  private static Control anchor(IFindReplaceTarget editor, String textId, String contentId) {
    if (editor instanceof TextComposite text && text.getToolbarWidgets() != null) {
      Control control = text.getToolbarWidgets().getControlForMenu(textId);
      if (control != null) {
        return control;
      }
    }
    if (editor instanceof IContentEditorWidget widget && widget.getToolbarWidgets() != null) {
      return widget.getToolbarWidgets().getControlForMenu(contentId);
    }
    return null;
  }

  private static void showPopup(Control anchor, Menu menu) {
    if (anchor != null && !anchor.isDisposed() && anchor.getParent() != null) {
      Rectangle rect = anchor.getBounds();
      Point location = anchor.getParent().toDisplay(new Point(rect.x, rect.y + rect.height + 6));
      menu.setLocation(location);
    }
    menu.addListener(
        SWT.Hide,
        e -> {
          Display display = menu.getDisplay();
          if (display != null && !display.isDisposed()) {
            display.asyncExec(
                () -> {
                  if (!menu.isDisposed()) {
                    menu.dispose();
                  }
                });
          }
        });
    menu.setVisible(true);
  }
}
