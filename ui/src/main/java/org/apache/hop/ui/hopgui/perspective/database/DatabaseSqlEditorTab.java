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

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.SqlScriptStatement;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementFilter;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.editor.IContentEditorWidget;
import org.apache.hop.ui.hopgui.ContentEditorFacade;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.shared.SashFormMemory;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;

/** One SQL editor tab: editor, Run, hideable results sash. */
@GuiPlugin
public class DatabaseSqlEditorTab implements IHopFileTypeHandler {

  public static final Class<?> PKG = DatabasePerspective.class;

  public static final String TOOLBAR_ITEM_RUN = "ContentEditor-Toolbar-05000-run";

  static final String DATA_SQL_TAB = DatabaseSqlEditorTab.class.getName();

  public static final int DEFAULT_ROW_LIMIT = 1000;

  private static final DatabaseSqlFileType FILE_TYPE = new DatabaseSqlFileType();

  private final IDatabaseWorkbenchHost host;
  private final DatabaseWorkbench workbench;
  @Getter private final DatabaseMeta databaseMeta;
  @Getter private final Composite control;
  @Getter private CTabItem tabItem;

  private final IContentEditorWidget editor;
  private final SashForm sash;
  private final DatabaseResultsPanel resultsPanel;

  private String filename;
  private String name;
  private boolean changed;

  /** True while Ctrl/Cmd+Enter is being handled so Traverse and KeyDown cannot both run SQL. */
  private boolean executeShortcutArmed;

  public DatabaseSqlEditorTab(
      Composite parent,
      IDatabaseWorkbenchHost host,
      DatabaseWorkbench workbench,
      DatabaseMeta databaseMeta) {
    this.host = host;
    this.workbench = workbench;
    this.databaseMeta = databaseMeta;
    this.name = untitledName();

    control = new Composite(parent, SWT.NONE);
    control.setLayout(new FormLayout());
    PropsUi.setLook(control);

    sash = new SashForm(control, SWT.VERTICAL);
    sash.setLayoutData(new FormDataBuilder().fullSize().result());

    Composite editorArea = new Composite(sash, SWT.NONE);
    editorArea.setLayout(new FormLayout());
    PropsUi.setLook(editorArea);

    editor = ContentEditorFacade.createContentEditor(editorArea, "sql");
    editor.addModifyListener(
        e -> {
          setChanged();
          host.updateGui(this);
        });
    editor.getControl().setData(DATA_SQL_TAB, this);
    editor
        .getControl()
        .setData(IContentEditorWidget.DATA_EXECUTE_ACTION, (Runnable) this::executeSql);
    control.setData(DATA_SQL_TAB, this);
    control.setData(IContentEditorWidget.DATA_EXECUTE_ACTION, (Runnable) this::executeSql);
    installExecuteShortcut();

    resultsPanel = new DatabaseResultsPanel(sash, host.getVariables(), this::hideResults);
    sash.setWeights(70, 30);
    SashFormMemory.persist(sash, "database-sql-editor-sash", 70, 30);
    hideResults();
  }

  public void setTabItem(CTabItem tabItem) {
    this.tabItem = tabItem;
    refreshTabPresentation();
  }

  public void setInitialText(String sql) {
    editor.setTextSuppressModify(Const.NVL(sql, ""));
  }

  public String getSqlText() {
    return editor.getText();
  }

  private String untitledName() {
    return BaseMessages.getString(
        PKG, "DatabasePerspective.SqlTab.Untitled", Const.NVL(databaseMeta.getName(), ""));
  }

  @GuiToolbarElement(
      root = IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_RUN,
      toolTip = "i18n::DatabasePerspective.SqlTab.Run.Tooltip",
      image = "ui/images/run.svg",
      separator = true)
  public static void runFromEditor(IContentEditorWidget editor) {
    DatabaseSqlEditorTab tab = fromEditor(editor);
    if (tab != null) {
      tab.executeSql();
    }
  }

  @GuiToolbarElementFilter(parentId = IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID)
  public static boolean showRunOnDatabaseSqlEditor(String itemId, Object guiPluginInstance) {
    if (!TOOLBAR_ITEM_RUN.equals(itemId)) {
      return true;
    }
    if (!(guiPluginInstance instanceof IContentEditorWidget editor)) {
      return false;
    }
    return fromEditor(editor) != null || isInDatabaseWorkbench(editor);
  }

  /**
   * Listen for Ctrl+Enter / Cmd+Enter on the editor widget tree and run {@link #executeSql()}.
   *
   * <p>SWT does not bubble keys, so the listener has to sit on the focused control (desktop {@code
   * StyledText}, RAP fallback {@code Text}), not on this tab composite. On GTK, {@code StyledText}
   * marks Ctrl+Return as {@link SWT#TRAVERSE_RETURN} with {@code doit=true}; {@code
   * Control.translateTraversal} then consumes the key as the shell default button and never sends
   * {@link SWT#KeyDown}. Cancelling that traverse is what lets the shortcut fire.
   *
   * <p>Hop Web's Monaco editor does not go through SWT keys; it notifies {@code executeRequested}
   * which looks up {@link IContentEditorWidget#DATA_EXECUTE_ACTION}.
   */
  private void installExecuteShortcut() {
    Listener listener = this::handleExecuteShortcut;
    Deque<Control> stack = new ArrayDeque<>();
    stack.push(editor.getControl());
    while (!stack.isEmpty()) {
      Control current = stack.pop();
      if (current.isDisposed()) {
        continue;
      }
      current.addListener(SWT.Traverse, listener);
      current.addListener(SWT.KeyDown, listener);
      current.addListener(SWT.Verify, listener);
      if (current instanceof Composite composite) {
        for (Control child : composite.getChildren()) {
          stack.push(child);
        }
      }
    }

    // Display filters run before StyledText.handleKeyDown, so we can mark the newline to eat
    // before it is inserted. Widget KeyDown listeners are too late.
    Display display = control.getDisplay();
    Listener eatFilter =
        event -> {
          if (!(event.widget instanceof Control focused) || !isInThisEditor(focused)) {
            return;
          }
          if (IContentEditorWidget.isExecuteKey(event.stateMask, event.keyCode, event.character)) {
            armEatExecuteNewline();
          }
        };
    display.addFilter(SWT.KeyDown, eatFilter);
    control.addDisposeListener(
        e -> {
          if (!display.isDisposed()) {
            display.removeFilter(SWT.KeyDown, eatFilter);
          }
        });
  }

  /**
   * SWT listener for Ctrl/Cmd+Enter. Called from {@link SWT#Traverse} (GTK), {@link SWT#KeyDown}
   * (Windows/macOS, and GTK after traverse is cancelled), and {@link SWT#Verify} (reject the
   * newline StyledText always inserts for Return).
   */
  private void handleExecuteShortcut(Event event) {
    if (event.type == SWT.Verify) {
      if (IContentEditorWidget.isLineDelimiterText(event.text)
          && (executeShortcutArmed
              || IContentEditorWidget.eatExecuteNewlineArmed(editor.getControl()))) {
        event.doit = false;
      }
      return;
    }
    boolean match =
        event.type == SWT.Traverse
            ? IContentEditorWidget.isExecuteTraverse(event.detail, event.stateMask)
            : IContentEditorWidget.isExecuteKey(event.stateMask, event.keyCode, event.character);
    if (!match) {
      return;
    }
    event.doit = false;
    if (event.type == SWT.Traverse) {
      event.detail = SWT.TRAVERSE_NONE;
    }
    armEatExecuteNewline();
    if (executeShortcutArmed) {
      return;
    }
    executeShortcutArmed = true;
    try {
      executeSql();
    } finally {
      Display display = host.getDisplay();
      if (display != null && !display.isDisposed()) {
        display.asyncExec(this::disarmExecuteShortcut);
      } else {
        disarmExecuteShortcut();
      }
    }
  }

  private void armEatExecuteNewline() {
    Control editorControl = editor.getControl();
    if (editorControl != null && !editorControl.isDisposed()) {
      editorControl.setData(IContentEditorWidget.DATA_EAT_EXECUTE_NEWLINE, Boolean.TRUE);
    }
  }

  private void disarmExecuteShortcut() {
    executeShortcutArmed = false;
    Control editorControl = editor.getControl();
    if (editorControl != null && !editorControl.isDisposed()) {
      editorControl.setData(IContentEditorWidget.DATA_EAT_EXECUTE_NEWLINE, null);
    }
  }

  private boolean isInThisEditor(Control focused) {
    Control editorControl = editor.getControl();
    Control current = focused;
    while (current != null && !current.isDisposed()) {
      if (current == editorControl || current == control) {
        return true;
      }
      current = current.getParent();
    }
    return false;
  }

  public static DatabaseSqlEditorTab fromEditor(IContentEditorWidget editor) {
    if (editor == null || editor.isDisposed()) {
      return null;
    }
    Control current = editor.getControl();
    while (current != null && !current.isDisposed()) {
      Object data = current.getData(DATA_SQL_TAB);
      if (data instanceof DatabaseSqlEditorTab tab) {
        return tab;
      }
      current = current.getParent();
    }
    return null;
  }

  private static boolean isInDatabaseWorkbench(IContentEditorWidget editor) {
    Control current = editor.getControl();
    while (current != null && !current.isDisposed()) {
      if (current instanceof DatabaseWorkbench) {
        return true;
      }
      current = current.getParent();
    }
    return false;
  }

  public void executeSql() {
    String script =
        SqlExecuteRange.scriptToExecute(
            editor.getText(), editor.getSelectionText(), editor.getCaretPosition());
    if (Utils.isEmpty(script)) {
      return;
    }
    List<SqlScriptStatement> statements =
        databaseMeta.getIDatabase().getSqlScriptStatements(script + Const.CR);
    if (statements.isEmpty()) {
      return;
    }

    String description =
        BaseMessages.getString(
            PKG, "DatabasePerspective.Operation.ExecuteSql", databaseMeta.getName());
    workbench.runOperation(
        description,
        databaseMeta.getName(),
        operation -> {
          int timeout = queryTimeoutSeconds();
          StringBuilder messages = new StringBuilder();
          List<DatabaseResultsPanel.QueryResult> queryResults = new ArrayList<>();
          try (Database db =
              new Database(host.getLoggingObject(), host.getVariables(), databaseMeta)) {
            operation.attachDatabase(db);
            if (timeout > 0) {
              db.setStatementQueryTimeoutSeconds(timeout);
            }
            db.setQueryLimit(DEFAULT_ROW_LIMIT);
            db.connect();
            int nr = 0;
            for (SqlScriptStatement sql : statements) {
              if (operation.isCancelled()) {
                messages
                    .append(BaseMessages.getString(PKG, "DatabasePerspective.SqlTab.Cancelled"))
                    .append(Const.CR);
                break;
              }
              nr++;
              if (sql.isQuery()) {
                List<Object[]> rows = db.getRows(sql.getStatement(), DEFAULT_ROW_LIMIT);
                IRowMeta rowMeta = db.getReturnRowMeta();
                queryResults.add(new DatabaseResultsPanel.QueryResult(nr, rowMeta, rows));
                messages
                    .append(
                        BaseMessages.getString(
                            PKG,
                            "DatabasePerspective.SqlTab.QueryRows",
                            Integer.toString(nr),
                            Integer.toString(rows.size())))
                    .append(Const.CR);
              } else {
                db.execStatement(sql.getStatement());
                messages
                    .append(
                        BaseMessages.getString(
                            PKG, "DatabasePerspective.SqlTab.Executed", sql.getStatement().trim()))
                    .append(Const.CR);
              }
            }
          }
          String messageText = messages.toString();
          host.asyncExec(
              () -> {
                resultsPanel.show(queryResults, messageText);
                showResults();
              });
        });
  }

  private int queryTimeoutSeconds() {
    IVariables variables = host.getVariables();
    String raw =
        variables.getVariable(
            Const.HOP_QUERY_PREVIEW_TIMEOUT,
            EnvUtil.getSystemProperty(Const.HOP_QUERY_PREVIEW_TIMEOUT, "0"));
    return Math.max(0, Const.toInt(variables.resolve(raw), 0));
  }

  private void showResults() {
    sash.setMaximizedControl(null);
    SashFormMemory.restore(sash, "database-sql-editor-sash", 70, 30);
  }

  private void hideResults() {
    sash.setMaximizedControl(sash.getChildren()[0]);
  }

  private void refreshTabPresentation() {
    if (tabItem == null || tabItem.isDisposed()) {
      return;
    }
    tabItem.setText(Const.NVL(getName(), ""));
    tabItem.setImage(GuiResource.getInstance().getImageFile());
    tabItem.setFont(
        changed ? GuiResource.getInstance().getFontBold() : tabItem.getParent().getFont());
  }

  public void setChanged() {
    if (!changed) {
      changed = true;
      refreshTabPresentation();
    }
  }

  public void clearChanged() {
    if (changed) {
      changed = false;
      refreshTabPresentation();
    }
  }

  @Override
  public Object getSubject() {
    return this;
  }

  @Override
  public String getName() {
    if (!Utils.isEmpty(filename)) {
      try {
        return HopVfs.getFileObject(filename, getVariables()).getName().getBaseName();
      } catch (Exception e) {
        return name;
      }
    }
    return name;
  }

  @Override
  public void setName(String name) {
    this.name = name;
    refreshTabPresentation();
  }

  @Override
  public IHopFileType getFileType() {
    return FILE_TYPE;
  }

  @Override
  public String getFilename() {
    return filename;
  }

  @Override
  public void setFilename(String filename) {
    this.filename = filename;
    refreshTabPresentation();
  }

  @Override
  public void save() throws HopException {
    if (Utils.isEmpty(filename)) {
      throw new HopException("No filename set");
    }
    try {
      try (OutputStream outputStream = HopVfs.getOutputStream(filename, false, getVariables())) {
        outputStream.write(editor.getText().getBytes(StandardCharsets.UTF_8));
        outputStream.flush();
      }
      clearChanged();
      host.updateGui(this);
      workbench.refreshTab(this);
    } catch (Exception e) {
      throw new HopException("Unable to save file '" + filename + "'", e);
    }
  }

  @Override
  public void saveAs(String newFilename) throws HopException {
    try {
      if (!newFilename.toLowerCase().endsWith(FILE_TYPE.getDefaultFileExtension())) {
        newFilename = newFilename + FILE_TYPE.getDefaultFileExtension();
      }
      FileObject fileObject = HopVfs.getFileObject(newFilename, getVariables());
      if (!HopVfs.startsWithScheme(newFilename, getVariables()) && !newFilename.contains("://")) {
        newFilename = HopVfs.normalize(newFilename);
        fileObject = HopVfs.getFileObject(newFilename, getVariables());
      } else {
        newFilename = fileObject.getName().getURI();
      }
      if (fileObject.exists()) {
        MessageBox box = new MessageBox(host.getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
        box.setText(BaseMessages.getString(PKG, "DatabasePerspective.Save.Overwrite.Title"));
        box.setMessage(
            BaseMessages.getString(PKG, "DatabasePerspective.Save.Overwrite.Message", newFilename));
        if ((box.open() & SWT.YES) == 0) {
          return;
        }
      }
      setFilename(newFilename);
      save();
      host.getHopGui().fileRefreshDelegate.register(newFilename, this);
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Error validating file existence for '" + newFilename + "'", e);
    }
  }

  public void loadFromVfs() throws HopException {
    if (Utils.isEmpty(filename)) {
      return;
    }
    try {
      FileObject file = HopVfs.getFileObject(filename, getVariables());
      if (!file.exists()) {
        throw new HopException("File '" + filename + "' doesn't exist");
      }
      try (InputStream inputStream = HopVfs.getInputStream(file)) {
        String contents = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        editor.setTextSuppressModify(Const.NVL(contents, ""));
      }
      clearChanged();
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("I/O exception while reading '" + filename + "'", e);
    }
  }

  public void applyBuffer(String text, boolean markDirty) {
    editor.setTextSuppressModify(Const.NVL(text, ""));
    if (markDirty) {
      setChanged();
    } else {
      clearChanged();
    }
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public void pause() {}

  @Override
  public void resume() {}

  @Override
  public void preview() {}

  @Override
  public void debug() {}

  @Override
  public void redraw() {}

  @Override
  public void updateGui() {
    host.getDisplay().asyncExec(() -> host.updateGui(this));
  }

  @Override
  public void selectAll() {
    editor.selectAll();
  }

  @Override
  public void unselectAll() {
    editor.unselectAll();
  }

  @Override
  public void copySelectedToClipboard() {
    editor.copy();
  }

  @Override
  public void cutSelectedToClipboard() {
    editor.cut();
  }

  @Override
  public void deleteSelected() {}

  @Override
  public void pasteFromClipboard() {
    editor.paste();
  }

  @Override
  public boolean isCloseable() {
    try {
      if (!changed) {
        return true;
      }
      MessageBox messageDialog =
          new MessageBox(host.getShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO | SWT.CANCEL);
      messageDialog.setText(BaseMessages.getString(PKG, "DatabasePerspective.Save.Ask.Title"));
      messageDialog.setMessage(
          BaseMessages.getString(PKG, "DatabasePerspective.Save.Ask.Message", getName()));
      int answer = messageDialog.open();
      if ((answer & SWT.YES) != 0) {
        if (Utils.isEmpty(filename)) {
          host.getHopGui().fileDelegate.fileSaveAs();
          return !changed;
        }
        save();
        return true;
      }
      return (answer & SWT.NO) != 0;
    } catch (Exception e) {
      new ErrorDialog(
          host.getShell(),
          BaseMessages.getString(PKG, "DatabasePerspective.Error.Title"),
          BaseMessages.getString(PKG, "DatabasePerspective.Save.Error.Message", getName()),
          e);
      return false;
    }
  }

  @Override
  public void close() {
    workbench.remove(this);
  }

  @Override
  public boolean hasChanged() {
    return changed;
  }

  @Override
  public void undo() {
    editor.undo();
  }

  @Override
  public void redo() {
    editor.redo();
  }

  @Override
  public Map<String, Object> getStateProperties() {
    return Collections.emptyMap();
  }

  @Override
  public void applyStateProperties(Map<String, Object> stateProperties) {}

  @Override
  public List<IGuiContextHandler> getContextHandlers() {
    return List.of();
  }

  @Override
  public IVariables getVariables() {
    return host.getVariables();
  }

  public boolean isSameFile(String otherFilename) {
    return !Utils.isEmpty(filename) && Objects.equals(filename, otherFilename);
  }
}
