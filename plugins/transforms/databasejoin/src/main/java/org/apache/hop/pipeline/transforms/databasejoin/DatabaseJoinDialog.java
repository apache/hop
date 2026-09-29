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

package org.apache.hop.pipeline.transforms.databasejoin;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeButtonsListener;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.SQLStyledTextComp;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextComposite;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.BackgroundThreadFacade;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class DatabaseJoinDialog extends BaseTransformDialog {
  private static final Class<?> PKG = DatabaseJoinMeta.class;

  private TextComposite wSql;

  private Label wlPosition;

  private TableView wParam;
  private TableView wResolvedParam;

  private final DatabaseJoinMeta input;

  private ColumnInfo[] ciKey;
  private ColumnInfo[] ciResolvedParam;

  private final List<String> inputFields = new ArrayList<>();
  private IRowMeta sourceFieldsMeta;

  private GuiCompositeWidgets widgets;

  public DatabaseJoinDialog(
      Shell parent,
      IVariables variables,
      DatabaseJoinMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "DatabaseJoinDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).get(e -> get()).cancel(e -> cancel()).build();

    backupChanged = input.hasChanged();

    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            variables,
            wSpacer,
            wOk,
            DatabaseJoinMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
            input,
            w -> {
              // Extra-group builders run during createCompositeWidgets, before
              // addScrolledComposite returns. Keep the field assigned so they can look up the
              // widgets already placed on the same tab.
              widgets = w;
              w.registerExtraGroup(
                  BaseMessages.getString(PKG, "DatabaseJoin.Tab.Sql"), "0200", null, this::addSql);
              w.registerExtraGroup(
                  BaseMessages.getString(PKG, "DatabaseJoin.Tab.Parameters"),
                  "0300",
                  null,
                  this::addParameters);
            });

    widgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            if (!loading) {
              input.setChanged();
            }
            if (DatabaseJoinMeta.WIDGET_CACHED.equals(widgetId)) {
              enableFields();
            } else if (DatabaseJoinMeta.WIDGET_CONNECTION.equals(widgetId)) {
              onConnectionChanged();
            } else if (DatabaseJoinMeta.WIDGET_SQL_FROM_FILE.equals(widgetId)) {
              onSqlFromFileChanged();
            }
          }

          @Override
          public void persistContents(GuiCompositeWidgets compositeWidgets) {
            persistSqlEditor();
            persistParameters();
          }
        });

    // The connection drives SQL syntax highlighting. The Connection tab is laid out before the
    // SQL tab, so the selection line already exists by the time the editor is built.
    MetaSelectionLine<?> connectionLine = connectionLine();
    if (connectionLine != null) {
      connectionLine.addListener(SWT.Selection, e -> onConnectionChanged());
    }

    widgets.setCompositeButtonsListener(
        new IGuiPluginCompositeButtonsListener() {
          @Override
          public void buttonPressed(Object sourceObject) {
            // Flush the widgets before the no-op Meta method runs: browseSqlFromFile() reads the
            // path from the widget, and the setWidgetsContents that follows a button press would
            // otherwise re-read editor state that is about to change.
            widgets.getWidgetsContents(input, DatabaseJoinMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
          }

          @Override
          public void afterButtonPressed(Object sourceObject) {
            browseSqlFromFile();
          }
        });

    populateSqlEditor();
    populateParameters();
    enableFields();
    searchPrevTransformFields();

    input.setChanged(backupChanged);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /**
   * The SQL tab: the styled SQL editor with its line/column readout, plus the read-only table of
   * parameters resolved from the SQL. The editor takes all remaining vertical space, which the old
   * flat form could not do without squeezing the parameter grid below it.
   */
  private void addSql(Composite parent) {
    Label wlSql = new Label(parent, SWT.NONE);
    wlSql.setText(BaseMessages.getString(PKG, "DatabaseJoinDialog.SQL.Label"));
    PropsUi.setLook(wlSql);
    FormData fdlSql = new FormData();
    fdlSql.left = new FormAttachment(0, 0);
    fdlSql.right = new FormAttachment(100, 0);
    Control lastOnTab = widgets.getWidgetsMap().get(DatabaseJoinMeta.WIDGET_REPLACE_VARIABLES);
    fdlSql.top =
        lastOnTab == null ? new FormAttachment(0, 0) : new FormAttachment(lastOnTab, margin);
    wlSql.setLayoutData(fdlSql);

    wSql =
        EnvironmentUtils.getInstance().isWeb()
            ? new StyledTextComp(
                variables,
                parent,
                SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL,
                TextComposite.STYLE_TYPE_SQL)
            : new SQLStyledTextComp(
                variables, parent, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    PropsUi.setLook(wSql, Props.WIDGET_STYLE_FIXED);
    wSql.addLineStyleListener(getSqlReservedWords());
    wSql.addModifyListener(e -> refreshResolvedParametersPanel());
    wSql.addModifyListener(e -> setPosition());
    wSql.addKeyListener(
        new KeyAdapter() {
          @Override
          public void keyPressed(KeyEvent e) {
            setPosition();
          }

          @Override
          public void keyReleased(KeyEvent e) {
            setPosition();
          }
        });
    wSql.addFocusListener(
        new FocusAdapter() {
          @Override
          public void focusGained(FocusEvent e) {
            setPosition();
          }

          @Override
          public void focusLost(FocusEvent e) {
            setPosition();
          }
        });
    wSql.addMouseListener(
        new MouseAdapter() {
          @Override
          public void mouseDoubleClick(MouseEvent e) {
            setPosition();
          }

          @Override
          public void mouseDown(MouseEvent e) {
            setPosition();
          }

          @Override
          public void mouseUp(MouseEvent e) {
            setPosition();
          }
        });
    FormData fdSql = new FormData();
    fdSql.left = new FormAttachment(0, 0);
    fdSql.top = new FormAttachment(wlSql, margin);
    fdSql.right = new FormAttachment(100, 0);
    fdSql.bottom = new FormAttachment(100, 0);
    wSql.setLayoutData(fdSql);

    wlPosition = new Label(parent, SWT.NONE);
    PropsUi.setLook(wlPosition);
    FormData fdlPosition = new FormData();
    fdlPosition.left = new FormAttachment(0, 0);
    fdlPosition.top = new FormAttachment(wSql, margin);
    fdlPosition.right = new FormAttachment(100, 0);
    wlPosition.setLayoutData(fdlPosition);

    Label wlResolvedParam = new Label(parent, SWT.NONE);
    wlResolvedParam.setText(
        BaseMessages.getString(PKG, "DatabaseJoinDialog.ResolvedParameters.Label"));
    PropsUi.setLook(wlResolvedParam);
    FormData fdlResolvedParam = new FormData();
    fdlResolvedParam.left = new FormAttachment(0, 0);
    fdlResolvedParam.right = new FormAttachment(100, 0);
    fdlResolvedParam.top = new FormAttachment(wlPosition, margin);
    wlResolvedParam.setLayoutData(fdlResolvedParam);

    ciResolvedParam = new ColumnInfo[3];
    ciResolvedParam[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseJoinDialog.ColumnInfo.ResolvedPlaceholder"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    ciResolvedParam[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseJoinDialog.ColumnInfo.ResolvedInputField"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    ciResolvedParam[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseJoinDialog.ColumnInfo.ResolvedType"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);

    wResolvedParam =
        new TableView(
            variables,
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciResolvedParam,
            1,
            true,
            e -> {},
            props,
            true,
            null,
            false,
            false);
    FormData fdResolvedParam = new FormData();
    fdResolvedParam.left = new FormAttachment(0, 0);
    fdResolvedParam.top = new FormAttachment(wlResolvedParam, margin);
    fdResolvedParam.right = new FormAttachment(100, 0);
    fdResolvedParam.height = (int) (90 * props.getZoomFactor());
    wResolvedParam.setLayoutData(fdResolvedParam);
  }

  /** The Parameters tab: the grid that maps positional {@code ?} markers to input fields. */
  private void addParameters(Composite parent) {
    int nrKeyRows = (input.getParameters() != null ? input.getParameters().size() : 1);

    ciKey = new ColumnInfo[2];
    ciKey[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseJoinDialog.ColumnInfo.ParameterFieldname"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciKey[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseJoinDialog.ColumnInfo.ParameterType"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            ValueMetaFactory.getValueMetaNames());

    Label wlParam = new Label(parent, SWT.NONE);
    wlParam.setText(BaseMessages.getString(PKG, "DatabaseJoinDialog.Param.Label"));
    PropsUi.setLook(wlParam);
    FormData fdlParam = new FormData();
    fdlParam.left = new FormAttachment(0, 0);
    fdlParam.right = new FormAttachment(100, 0);
    fdlParam.top = new FormAttachment(0, 0);
    wlParam.setLayoutData(fdlParam);

    wParam =
        new TableView(
            variables,
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciKey,
            nrKeyRows,
            e -> {
              if (!loading) {
                input.setChanged();
              }
              refreshResolvedParametersPanel();
            },
            props);

    FormData fdParam = new FormData();
    fdParam.left = new FormAttachment(0, 0);
    fdParam.top = new FormAttachment(wlParam, margin);
    fdParam.right = new FormAttachment(100, 0);
    fdParam.bottom = new FormAttachment(100, 0);
    wParam.setLayoutData(fdParam);
  }

  private MetaSelectionLine<?> connectionLine() {
    if (widgets == null) {
      return null;
    }
    Control control = widgets.getWidgetsMap().get(DatabaseJoinMeta.WIDGET_CONNECTION);
    return control instanceof MetaSelectionLine<?> line ? line : null;
  }

  private void onConnectionChanged() {
    // Only the resolved-parameter table depends on the connection here. The SQL highlighter is
    // deliberately not re-seeded: TextComposite has no removeLineStyleListener, so every call to
    // addLineStyleListener stacks another listener and the stale keyword sets would keep firing.
    // Highlighting therefore uses the keywords of the connection stored on the transform, which is
    // what the dialog did before it was split into tabs.
    refreshResolvedParametersPanel();
  }

  private void onSqlFromFileChanged() {
    if (Utils.isEmpty(readWidgetText(DatabaseJoinMeta.WIDGET_SQL_FROM_FILE))) {
      wSql.setEditable(true);
      refreshResolvedParametersPanel();
    } else {
      loadSqlFromFileAndSetReadOnly();
    }
  }

  private List<String> getSqlReservedWords() {
    String connectionName = readWidgetText(DatabaseJoinMeta.WIDGET_CONNECTION);
    // Do not search keywords when connection is empty
    if (Utils.isEmpty(connectionName)) {
      return List.of();
    }

    // If connection is a variable that can't be resolved
    if (variables.resolve(connectionName).startsWith("${")) {
      return List.of();
    }

    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connectionName, variables);
    if (databaseMeta == null) {
      return List.of();
    }
    return Arrays.stream(databaseMeta.getReservedWords()).toList();
  }

  private void enableFields() {
    setEnabled(DatabaseJoinMeta.WIDGET_CACHE_SIZE, isChecked(DatabaseJoinMeta.WIDGET_CACHED));
  }

  private boolean isChecked(String widgetId) {
    if (widgets == null) {
      return false;
    }
    Control control = widgets.getWidgetsMap().get(widgetId);
    return control instanceof Button button && button.getSelection();
  }

  private void setEnabled(String widgetId, boolean enabled) {
    if (widgets == null) {
      return;
    }
    Control label = widgets.getLabelsMap().get(widgetId);
    if (label != null && !label.isDisposed()) {
      label.setEnabled(enabled);
    }
    Control widget = widgets.getWidgetsMap().get(widgetId);
    if (widget != null && !widget.isDisposed()) {
      widget.setEnabled(enabled);
    }
  }

  private void setComboBoxes() {
    // Something was changed in the row.
    //
    if (ciKey == null) {
      return;
    }
    ciKey[0].setComboValues(ConstUi.sortFieldNames(inputFields));
  }

  public void setPosition() {
    if (wSql == null || wSql.isDisposed() || wlPosition == null || wlPosition.isDisposed()) {
      return;
    }
    int lineNumber = wSql.getLineNumber();
    int columnNumber = wSql.getColumnNumber();
    wlPosition.setText(
        BaseMessages.getString(
            PKG, "DatabaseJoinDialog.Position.Label", "" + lineNumber, "" + columnNumber));
  }

  private DatabaseJoinMeta.SqlParameterSpec parseCurrentSqlParameterSpec() {
    String sourceSql = wSql == null || wSql.isDisposed() ? null : wSql.getText();
    return DatabaseJoinMeta.parseSqlParameterSpec(
        sourceSql,
        DatabaseJoinMeta.supportsBracketQuotedIdentifiers(
            pipelineMeta.findDatabase(
                readWidgetText(DatabaseJoinMeta.WIDGET_CONNECTION), variables)));
  }

  private List<ParameterField> getDeclaredParametersFromGrid() {
    List<ParameterField> parameters = new ArrayList<>();
    if (wParam == null || wParam.isDisposed()) {
      return parameters;
    }

    int nrparam = wParam.nrNonEmpty();
    for (int i = 0; i < nrparam; i++) {
      TableItem item = wParam.getNonEmpty(i);
      ParameterField field = new ParameterField();
      field.setName(item.getText(1));
      field.setType(ValueMetaFactory.getIdForValueMeta(item.getText(2)));
      parameters.add(field);
    }
    return parameters;
  }

  private void refreshResolvedParametersPanel() {
    if (wResolvedParam == null || wResolvedParam.isDisposed()) {
      return;
    }

    DatabaseJoinMeta.SqlParameterSpec parameterSpec = parseCurrentSqlParameterSpec();
    List<ParameterField> declaredParameters = getDeclaredParametersFromGrid();
    Map<String, String> declaredTypesByName = new LinkedHashMap<>();
    for (ParameterField parameter : declaredParameters) {
      if (!Utils.isEmpty(parameter.getName())) {
        declaredTypesByName.put(parameter.getName(), parameter.getType());
      }
    }

    wResolvedParam.table.removeAll();
    int positionalIndex = 0;
    for (String parameterReference : parameterSpec.getParameterReferences()) {
      boolean positional = parameterReference == null;
      String placeholder = positional ? "?" : "?{" + parameterReference + "}";

      String inputFieldName = parameterReference;
      String declaredType = positional ? null : declaredTypesByName.get(parameterReference);
      if (positional) {
        if (positionalIndex < declaredParameters.size()) {
          ParameterField declared = declaredParameters.get(positionalIndex);
          inputFieldName = declared.getName();
          declaredType = declared.getType();
        } else {
          inputFieldName = null;
        }
        positionalIndex++;
      }

      boolean foundInInput =
          !Utils.isEmpty(inputFieldName)
              && ((sourceFieldsMeta != null && sourceFieldsMeta.indexOfValue(inputFieldName) >= 0)
                  || inputFields.contains(inputFieldName));

      String inputFieldDisplay;
      if (Utils.isEmpty(inputFieldName)) {
        inputFieldDisplay =
            BaseMessages.getString(PKG, "DatabaseJoinDialog.ResolvedParameters.Unmapped");
      } else if (foundInInput) {
        inputFieldDisplay = inputFieldName;
      } else {
        inputFieldDisplay =
            inputFieldName
                + " ("
                + BaseMessages.getString(PKG, "DatabaseJoinDialog.ResolvedParameters.NotFound")
                + ")";
      }

      String typeDisplay = declaredType;
      if (foundInInput && sourceFieldsMeta != null) {
        int sourceIndex = sourceFieldsMeta.indexOfValue(inputFieldName);
        if (sourceIndex >= 0) {
          typeDisplay =
              ValueMetaFactory.getValueMetaName(
                  sourceFieldsMeta.getValueMeta(sourceIndex).getType());
        }
      }
      if (typeDisplay == null) {
        typeDisplay = "";
      }

      wResolvedParam.add(placeholder, inputFieldDisplay, typeDisplay);
    }

    if (wResolvedParam.table.getItemCount() == 0) {
      new TableItem(wResolvedParam.table, SWT.NONE);
    }
    wResolvedParam.setRowNums();
    wResolvedParam.optWidth(true);

    // The declared grid only maps positional "?" markers. With named "?{field}" placeholders
    // there is nothing left to fill in, so the grid is disabled rather than silently ignored.
    boolean hasPositionalParameters = parameterSpec.getPositionalParameterCount() > 0;
    if (wParam != null && !wParam.isDisposed()) {
      wParam.setEnabled(hasPositionalParameters);
    }
  }

  private void loadSqlFromFileAndSetReadOnly() {
    String path = variables.resolve(readWidgetText(DatabaseJoinMeta.WIDGET_SQL_FROM_FILE));
    if (Utils.isEmpty(path)) {
      wSql.setEditable(true);
      return;
    }
    try {
      String content = HopVfs.getTextFileContent(path, StandardCharsets.UTF_8);
      wSql.setText(content);
      wSql.setEditable(false);
      refreshResolvedParametersPanel();
    } catch (HopFileException e) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_WARNING);
      mb.setText(BaseMessages.getString(PKG, "DatabaseJoinDialog.CouldNotLoadSqlFromFile.Title"));
      mb.setMessage(
          BaseMessages.getString(PKG, "DatabaseJoinDialog.CouldNotLoadSqlFromFile", path)
              + Const.CR
              + e.getMessage());
      mb.open();
      wSql.setEditable(true);
      refreshResolvedParametersPanel();
    }
  }

  /**
   * Open a file dialog on the transform dialog shell, load the chosen SQL into the editor and make
   * the editor read-only. Runs from the composite button listener rather than the annotated Meta
   * method: {@code setWidgetsContents} after a Meta mutation re-reads widgets that are still empty
   * and would wipe the selection.
   */
  private void browseSqlFromFile() {
    String path =
        BaseDialog.presentFileDialog(
            shell,
            null,
            variables,
            new String[] {"*.sql", "*"},
            new String[] {
              BaseMessages.getString(PKG, "DatabaseJoinDialog.SqlFiles"),
              BaseMessages.getString(PKG, "System.FileType.AllFiles")
            },
            false);
    if (path == null) {
      return;
    }
    writeWidgetText(DatabaseJoinMeta.WIDGET_SQL_FROM_FILE, path);
    input.setSqlFromFile(path);
    loadSqlFromFileAndSetReadOnly();
    input.setChanged();
  }

  private void populateSqlEditor() {
    wSql.setText(Const.NVL(input.getSql(), ""));
    if (!Utils.isEmpty(input.getSqlFromFile())) {
      loadSqlFromFileAndSetReadOnly();
    } else {
      wSql.setEditable(true);
    }
    setPosition();
  }

  private void populateParameters() {
    if (wParam == null || wParam.isDisposed()) {
      return;
    }
    wParam.clearAll();
    if (input.getParameters() != null) {
      for (ParameterField field : input.getParameters()) {
        TableItem item = new TableItem(wParam.table, SWT.NONE);
        if (field != null) {
          item.setText(1, Const.NVL(field.getName(), ""));
          item.setText(2, Const.NVL(field.getType(), ""));
        }
      }
    }
    if (wParam.table.getItemCount() == 0) {
      new TableItem(wParam.table, SWT.NONE);
    }
    wParam.setRowNums();
    wParam.optWidth(true);
    refreshResolvedParametersPanel();
  }

  private void persistSqlEditor() {
    if (wSql != null && !wSql.isDisposed()) {
      input.setSql(wSql.getText());
    }
  }

  private void persistParameters() {
    List<ParameterField> parameters = getDeclaredParametersFromGrid();
    logDebug(
        BaseMessages.getString(PKG, "DatabaseJoinDialog.Log.ParametersFound")
            + parameters.size()
            + " parameters");
    input.setParameters(parameters);
  }

  private String readWidgetText(String widgetId) {
    if (widgets == null) {
      return "";
    }
    Control control = widgets.getWidgetsMap().get(widgetId);
    if (control instanceof TextVar textVar) {
      return Const.NVL(textVar.getText(), "");
    }
    if (control instanceof Text text) {
      return Const.NVL(text.getText(), "");
    }
    if (control instanceof MetaSelectionLine<?> line) {
      return Const.NVL(line.getText(), "");
    }
    return "";
  }

  private void writeWidgetText(String widgetId, String value) {
    if (widgets == null) {
      return;
    }
    Control control = widgets.getWidgetsMap().get(widgetId);
    String text = Const.NVL(value, "");
    if (control instanceof TextVar textVar) {
      textVar.setText(text);
    } else if (control instanceof Text widget) {
      widget.setText(text);
    } else if (control instanceof MetaSelectionLine<?> line) {
      line.setText(text);
    }
  }

  private void searchPrevTransformFields() {
    //
    // Search the fields in the background
    //
    BackgroundThreadFacade.start(
        () -> {
          TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
          if (transformMeta == null) {
            return;
          }
          try {
            IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);

            // Remember these fields...
            sourceFieldsMeta = row;
            for (int i = 0; i < row.size(); i++) {
              inputFields.add(row.getValueMeta(i).getName());
            }
            setComboBoxes();
            if (!shell.isDisposed()) {
              shell.getDisplay().asyncExec(this::refreshResolvedParametersPanel);
            }
          } catch (HopException e) {
            logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
          }
        });
  }

  private void cancel() {
    transformName = null;
    input.setChanged(backupChanged);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    widgets.getWidgetsContents(input, DatabaseJoinMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    persistSqlEditor();
    persistParameters();

    transformName = wTransformName.getText(); // return value

    if (pipelineMeta.findDatabase(readWidgetText(DatabaseJoinMeta.WIDGET_CONNECTION), variables)
        == null) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "DatabaseJoinDialog.InvalidConnection.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "DatabaseJoinDialog.InvalidConnection.DialogTitle"));
      mb.open();
      // Keep the dialog open: disposing here would commit the unusable connection that was just
      // reported as invalid, leaving the transform broken with no chance to fix it.
      return;
    }

    dispose();
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null && !r.isEmpty()) {
        BaseTransformDialog.getFieldsFromPrevious(
            r, wParam, 1, new int[] {1}, new int[] {2}, -1, -1, null);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "DatabaseJoinDialog.GetFieldsFailed.DialogTitle"),
          BaseMessages.getString(PKG, "DatabaseJoinDialog.GetFieldsFailed.DialogMessage"),
          ke);
    }
  }
}
