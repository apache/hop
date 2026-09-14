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

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementFilter;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.IFindReplaceTarget;
import org.apache.hop.ui.core.widget.TextComposite;
import org.apache.hop.ui.core.widget.editor.IContentEditorWidget;
import org.apache.hop.ui.hopgui.perspective.database.DatabaseSqlEditorTab;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;

/**
 * Pretty-print SQL for:
 *
 * <ul>
 *   <li>Database perspective / SQL editor tabs ({@link IContentEditorWidget} language {@code sql})
 *   <li>{@link TextComposite} toolbars when {@link TextComposite#getStyleType()} is {@link
 *       TextComposite#STYLE_TYPE_SQL}
 * </ul>
 */
@GuiPlugin(
    id = "GUI-CalciteSqlFormatter",
    name = "Apache Calcite SQL formatter",
    description = "Pretty-print SQL in editors using Apache Calcite")
public class SqlFormatToolbarButton {
  private static final Class<?> PKG = SqlFormatToolbarButton.class;

  public static final String ID_CONTENT_EDITOR_FORMAT_SQL =
      "ContentEditor-Toolbar-35000-format-sql";
  public static final String ID_TEXTCOMPOSITE_FORMAT_SQL = "textcomposite-toolbar-20020-format-sql";

  private static final String LANGUAGE_SQL = "sql";

  private SqlFormatToolbarButton() {}

  /**
   * Only show this toolbar button when the content editor is SQL. Other toolbar item IDs always
   * return {@code true} so built-in buttons are not hidden.
   */
  @GuiToolbarElementFilter(parentId = IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID)
  public static boolean showForSqlEditor(String itemId, Object guiPluginInstance) {
    if (!ID_CONTENT_EDITOR_FORMAT_SQL.equals(itemId)) {
      return true;
    }
    if (!(guiPluginInstance instanceof IContentEditorWidget editor)) {
      return false;
    }
    return LANGUAGE_SQL.equalsIgnoreCase(editor.getLanguage());
  }

  /**
   * Only show this toolbar button when the text composite is SQL. Other toolbar item IDs always
   * return {@code true} so built-in buttons are not hidden.
   *
   * <p>Do not call {@link TextComposite#isEditable()} here. Filters run while the constructor is
   * still building the toolbar.
   */
  @GuiToolbarElementFilter(parentId = TextComposite.ID_TOOLBAR)
  public static boolean showForSqlTextComposite(String itemId, Object guiPluginInstance) {
    if (!ID_TEXTCOMPOSITE_FORMAT_SQL.equals(itemId)) {
      return true;
    }
    if (!(guiPluginInstance instanceof TextComposite textComposite)) {
      return false;
    }
    return TextComposite.STYLE_TYPE_SQL.equalsIgnoreCase(textComposite.getStyleType());
  }

  @GuiToolbarElement(
      root = IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = ID_CONTENT_EDITOR_FORMAT_SQL,
      toolTip = "i18n::SqlFormatToolbarButton.FormatSql.ToolTip",
      separator = true,
      image = "format-sql.svg")
  public static void formatSql(IContentEditorWidget editor) {
    if (editor == null) {
      return;
    }
    Control control = editor.getControl();
    if (control == null || control.isDisposed() || !editor.isEditable()) {
      return;
    }
    String selection = editor.getSelectionText();
    boolean selected = StringUtils.isNotEmpty(selection);
    String sql = selected ? selection : editor.getText();
    if (StringUtils.isBlank(sql)) {
      return;
    }
    try {
      applyFormatted(
          editor, sql, CalciteSqlFormatter.format(sql, databasePluginId(editor)), selected);
    } catch (Exception e) {
      showFormatError(control.getShell(), e);
    }
  }

  @GuiToolbarElement(
      root = TextComposite.ID_TOOLBAR,
      id = ID_TEXTCOMPOSITE_FORMAT_SQL,
      toolTip = "i18n::SqlFormatToolbarButton.FormatSql.ToolTip",
      separator = true,
      image = "format-sql.svg")
  public static void formatSql(TextComposite textComposite) {
    if (textComposite == null || textComposite.isDisposed() || !textComposite.isEditable()) {
      return;
    }
    String selection = textComposite.getSelectionText();
    boolean selected = StringUtils.isNotEmpty(selection);
    String sql = selected ? selection : textComposite.getText();
    if (StringUtils.isBlank(sql)) {
      return;
    }
    try {
      applyFormatted(textComposite, sql, CalciteSqlFormatter.format(sql), selected);
    } catch (Exception e) {
      showFormatError(textComposite.getShell(), e);
    }
  }

  /**
   * Replace the formatted range through {@link IFindReplaceTarget#insert(String)} so the editor can
   * undo the change. {@link IFindReplaceTarget#setText(String)} is used to load buffers and does
   * not record undo.
   */
  static void applyFormatted(
      IFindReplaceTarget target, String original, String formatted, boolean selected) {
    if (target == null || formatted == null || formatted.equals(original)) {
      return;
    }
    if (!selected) {
      target.setSelection(0, original == null ? 0 : original.length());
    }
    target.insert(formatted);
    target.updateToolbar();
  }

  private static String databasePluginId(IContentEditorWidget editor) {
    DatabaseSqlEditorTab tab = DatabaseSqlEditorTab.fromEditor(editor);
    if (tab == null || tab.getDatabaseMeta() == null) {
      return null;
    }
    return tab.getDatabaseMeta().getPluginId();
  }

  private static void showFormatError(Shell shell, Exception e) {
    new ErrorDialog(
        shell,
        BaseMessages.getString(PKG, "SqlFormatToolbarButton.Error.Title"),
        BaseMessages.getString(PKG, "SqlFormatToolbarButton.Error.Message"),
        e);
  }
}
