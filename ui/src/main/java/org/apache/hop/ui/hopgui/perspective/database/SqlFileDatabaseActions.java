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

package org.apache.hop.ui.hopgui.perspective.database;

import java.util.List;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementFilter;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.widget.editor.IContentEditorWidget;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;

/**
 * Content-editor toolbar plugin: open the current SQL buffer in the database perspective after
 * picking a connection.
 */
@GuiPlugin
public class SqlFileDatabaseActions {

  public static final Class<?> PKG = DatabasePerspective.class;

  public static final String TOOLBAR_ITEM_OPEN_IN_DATABASE =
      "ContentEditor-Toolbar-50000-open-in-database";

  private SqlFileDatabaseActions() {}

  @GuiToolbarElement(
      root = IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_OPEN_IN_DATABASE,
      toolTip = "i18n::DatabasePerspective.OpenInDatabase.Tooltip",
      image = "ui/images/database.svg",
      separator = true)
  public static void openInDatabasePerspective(IContentEditorWidget editor) {
    if (editor == null || editor.isDisposed()) {
      return;
    }
    if (!"sql".equalsIgnoreCase(editor.getLanguage())) {
      return;
    }
    HopGui hopGui = HopGui.getInstance();
    if (hopGui == null || hopGui.getPerspectiveManager() == null) {
      return;
    }
    DatabasePerspective perspective =
        hopGui.getPerspectiveManager().findPerspective(DatabasePerspective.class);
    if (perspective == null) {
      return;
    }
    perspective.activate();
    List<String> names = perspective.connectionNames();
    if (names.isEmpty()) {
      return;
    }
    Shell shell = editor.getControl() != null ? editor.getControl().getShell() : hopGui.getShell();
    EnterSelectionDialog dialog =
        new EnterSelectionDialog(
            shell,
            names.toArray(new String[0]),
            BaseMessages.getString(PKG, "DatabasePerspective.OpenInDatabase.Title"),
            BaseMessages.getString(PKG, "DatabasePerspective.OpenInDatabase.Message"));
    String selected = dialog.open();
    if (selected == null) {
      return;
    }
    DatabaseMeta meta = perspective.findConnection(selected);
    if (meta == null) {
      return;
    }
    FileContext context = findFileContext(editor);
    boolean dirty = context.handler != null && context.handler.hasChanged();
    perspective.openSqlFile(context.filename, meta, editor.getText(), dirty);
  }

  @GuiToolbarElementFilter(parentId = IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID)
  public static boolean showForSql(String itemId, Object guiPluginInstance) {
    if (!TOOLBAR_ITEM_OPEN_IN_DATABASE.equals(itemId)) {
      return true;
    }
    if (guiPluginInstance instanceof IContentEditorWidget editor) {
      return "sql".equalsIgnoreCase(editor.getLanguage());
    }
    return false;
  }

  private static FileContext findFileContext(IContentEditorWidget editor) {
    FileContext context = new FileContext();
    Control control = editor.getControl();
    ExplorerPerspective explorer = ExplorerPerspective.getInstance();
    if (explorer != null) {
      for (TabItemHandler item : explorer.getItems()) {
        if (item == null || item.getTypeHandler() == null) {
          continue;
        }
        CTabItem tab = item.getTabItem();
        if (tab == null || tab.isDisposed()) {
          continue;
        }
        if (isUnder(tab.getControl(), control)) {
          context.handler = item.getTypeHandler();
          context.filename = item.getTypeHandler().getFilename();
          return context;
        }
      }
    }
    Control current = control;
    while (current != null) {
      if (current.getParent() instanceof CTabFolder folder) {
        for (CTabItem item : folder.getItems()) {
          if (isUnder(item.getControl(), control)
              && item.getData() instanceof IHopFileTypeHandler handler) {
            context.handler = handler;
            context.filename = handler.getFilename();
            return context;
          }
        }
      }
      current = current.getParent();
    }
    return context;
  }

  private static boolean isUnder(Control root, Control child) {
    if (root == null || child == null) {
      return false;
    }
    Control current = child;
    while (current != null) {
      if (current == root) {
        return true;
      }
      current = current.getParent();
    }
    return false;
  }

  private static final class FileContext {
    private String filename;
    private IHopFileTypeHandler handler;
  }
}
