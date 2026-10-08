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
import org.apache.hop.core.util.StringUtil;
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
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class DatabaseJoinDialog extends BaseTransformDialog {
  private static final Class<?> PKG = DatabaseJoinMeta.class;

  /** Caret movement in the SQL editor. Each of these updates the line/column readout. */
  private static final int[] EDITOR_EVENTS = {
    SWT.Modify,
    SWT.KeyDown,
    SWT.KeyUp,
    SWT.FocusIn,
    SWT.FocusOut,
    SWT.MouseDown,
    SWT.MouseUp,
    SWT.MouseDoubleClick
  };

  private TextComposite wSql;
  private Label wlPosition;
  private TableView wParam;
  private TableView wResolvedParam;

  private final DatabaseJoinMeta input;

  private ColumnInfo[] ciKey;

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
              // Extra-group builders run inside this call, before the field is assigned.
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
              // Bracket quoting follows the database. The highlighter stays as built in addSql.
              refreshResolvedParametersPanel();
            } else if (DatabaseJoinMeta.WIDGET_SQL_FROM_FILE.equals(widgetId)) {
              onSqlFromFileChanged();
            }
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
   * SQL tab. The resolved-parameter table is pinned to the bottom and the line/column readout sits
   * above it, so the editor ends at the readout. Ending the editor at the bottom of the tab lays
   * the readout and the table outside the client area.
   */
  private void addSql(Composite parent) {
    Control lastOnTab = widgets.getWidgetsMap().get(DatabaseJoinMeta.WIDGET_REPLACE_VARIABLES);
    Label wlSql =
        fullWidthLabel(parent, BaseMessages.getString(PKG, "DatabaseJoinDialog.SQL.Label"));
    attachTop(wlSql, lastOnTab);

    wSql = newSqlEditor(parent);
    // Keywords of the connection selected while the editor is built. TextComposite cannot remove a
    // line-style listener, so a later connection change must not add a second one.
    wSql.addLineStyleListener(getSqlReservedWords());
    trackEditorCaret();

    wlPosition = fullWidthLabel(parent, "");

    Label wlResolvedParam =
        fullWidthLabel(
            parent, BaseMessages.getString(PKG, "DatabaseJoinDialog.ResolvedParameters.Label"));

    ColumnInfo[] resolvedColumns =
        new ColumnInfo[] {
          textColumn("DatabaseJoinDialog.ColumnInfo.ResolvedPlaceholder"),
          textColumn("DatabaseJoinDialog.ColumnInfo.ResolvedInputField"),
          textColumn("DatabaseJoinDialog.ColumnInfo.ResolvedType")
        };
    wResolvedParam =
        new TableView(
            variables,
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            resolvedColumns,
            1,
            true,
            null,
            props,
            true,
            null,
            false,
            false);

    attachBottom(wResolvedParam, (int) (90 * props.getZoomFactor()));
    attachAbove(wlResolvedParam, wResolvedParam);
    attachAbove(wlPosition, wlResolvedParam);
    attachBetween(wSql, wlSql, wlPosition);
  }

  /** Parameters tab: the grid that maps positional {@code ?} markers to input fields. */
  private void addParameters(Composite parent) {
    int nrKeyRows = input.getParameters() != null ? input.getParameters().size() : 1;

    ciKey =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "DatabaseJoinDialog.ColumnInfo.ParameterFieldname"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "DatabaseJoinDialog.ColumnInfo.ParameterType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames())
        };

    Label wlParam =
        fullWidthLabel(parent, BaseMessages.getString(PKG, "DatabaseJoinDialog.Param.Label"));
    attachTop(wlParam, null);

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
    attachFill(wParam, wlParam);
  }

  private TextComposite newSqlEditor(Composite parent) {
    int style = SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL;
    TextComposite editor =
        EnvironmentUtils.getInstance().isWeb()
            ? new StyledTextComp(variables, parent, style, TextComposite.STYLE_TYPE_SQL)
            : new SQLStyledTextComp(variables, parent, style);
    PropsUi.setLook(editor, Props.WIDGET_STYLE_FIXED);
    return editor;
  }

  private void trackEditorCaret() {
    Listener listener =
        e -> {
          setPosition();
          if (e.type == SWT.Modify) {
            refreshResolvedParametersPanel();
          }
        };
    for (int event : EDITOR_EVENTS) {
      wSql.addListener(event, listener);
    }
  }

  private ColumnInfo textColumn(String key) {
    return new ColumnInfo(BaseMessages.getString(PKG, key), ColumnInfo.COLUMN_TYPE_TEXT, false);
  }

  private Label fullWidthLabel(Composite parent, String text) {
    Label label = new Label(parent, SWT.NONE);
    label.setText(text);
    PropsUi.setLook(label);
    return label;
  }

  private void attachTop(Control control, Control above) {
    FormData data = fullWidth();
    data.top = above == null ? new FormAttachment(0, 0) : new FormAttachment(above, margin);
    control.setLayoutData(data);
  }

  private void attachBottom(Control control, int height) {
    FormData data = fullWidth();
    data.bottom = new FormAttachment(100, 0);
    data.height = height;
    control.setLayoutData(data);
  }

  private void attachAbove(Control control, Control below) {
    FormData data = fullWidth();
    data.bottom = new FormAttachment(below, -margin);
    control.setLayoutData(data);
  }

  private void attachBetween(Control control, Control above, Control below) {
    FormData data = fullWidth();
    data.top = new FormAttachment(above, margin);
    data.bottom = new FormAttachment(below, -margin);
    control.setLayoutData(data);
  }

  private void attachFill(Control control, Control above) {
    FormData data = fullWidth();
    data.top = new FormAttachment(above, margin);
    data.bottom = new FormAttachment(100, 0);
    control.setLayoutData(data);
  }

  private static FormData fullWidth() {
    FormData data = new FormData();
    data.left = new FormAttachment(0, 0);
    data.right = new FormAttachment(100, 0);
    return data;
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
    if (Utils.isEmpty(connectionName)) {
      return List.of();
    }
    // A variable that cannot be resolved here has no keyword list yet.
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
    if (ciKey == null) {
      return;
    }
    ciKey[0].setComboValues(ConstUi.sortFieldNames(inputFields));
  }

  private void setPosition() {
    if (wSql == null || wSql.isDisposed() || wlPosition == null || wlPosition.isDisposed()) {
      return;
    }
    wlPosition.setText(
        BaseMessages.getString(
            PKG,
            "DatabaseJoinDialog.Position.Label",
            Integer.toString(wSql.getLineNumber()),
            Integer.toString(wSql.getColumnNumber())));
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
    for (TableItem item : wParam.getNonEmptyItems()) {
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

    DatabaseJoinMeta.SqlParameterSpec spec = parseCurrentSqlParameterSpec();
    List<ParameterField> declared = getDeclaredParametersFromGrid();
    Map<String, String> typesByName = typesByName(declared);

    wResolvedParam.table.removeAll();
    int positionalIndex = 0;
    for (String reference : spec.getParameterReferences()) {
      addResolvedRow(reference, positionalIndex, declared, typesByName);
      if (reference == null) {
        positionalIndex++;
      }
    }

    if (wResolvedParam.table.getItemCount() == 0) {
      new TableItem(wResolvedParam.table, SWT.NONE);
    }
    wResolvedParam.setRowNums();
    wResolvedParam.optWidth(true);

    // The declared grid only maps positional "?" markers. Named "?{field}" placeholders leave
    // nothing to fill in, so the grid is disabled rather than silently ignored.
    if (wParam != null && !wParam.isDisposed()) {
      wParam.setEnabled(spec.getPositionalParameterCount() > 0);
    }
  }

  private void addResolvedRow(
      String reference,
      int positionalIndex,
      List<ParameterField> declared,
      Map<String, String> typesByName) {
    boolean positional = reference == null;
    String fieldName = reference;
    String declaredType = positional ? null : typesByName.get(reference);
    if (positional) {
      if (positionalIndex < declared.size()) {
        ParameterField field = declared.get(positionalIndex);
        fieldName = field.getName();
        declaredType = field.getType();
      } else {
        fieldName = null;
      }
    }

    boolean found = fieldIsInInput(fieldName);
    wResolvedParam.add(
        positional ? "?" : "?{" + reference + "}",
        fieldDisplay(fieldName, found),
        typeDisplay(fieldName, found, declaredType));
  }

  private static Map<String, String> typesByName(List<ParameterField> declared) {
    Map<String, String> typesByName = new LinkedHashMap<>();
    for (ParameterField parameter : declared) {
      if (!Utils.isEmpty(parameter.getName())) {
        typesByName.put(parameter.getName(), parameter.getType());
      }
    }
    return typesByName;
  }

  private boolean fieldIsInInput(String fieldName) {
    if (Utils.isEmpty(fieldName)) {
      return false;
    }
    if (sourceFieldsMeta != null && sourceFieldsMeta.indexOfValue(fieldName) >= 0) {
      return true;
    }
    return inputFields.contains(fieldName);
  }

  private String fieldDisplay(String fieldName, boolean found) {
    if (Utils.isEmpty(fieldName)) {
      return BaseMessages.getString(PKG, "DatabaseJoinDialog.ResolvedParameters.Unmapped");
    }
    if (found) {
      return fieldName;
    }
    return fieldName
        + " ("
        + BaseMessages.getString(PKG, "DatabaseJoinDialog.ResolvedParameters.NotFound")
        + ")";
  }

  private String typeDisplay(String fieldName, boolean found, String declaredType) {
    if (found && sourceFieldsMeta != null) {
      int sourceIndex = sourceFieldsMeta.indexOfValue(fieldName);
      if (sourceIndex >= 0) {
        return ValueMetaFactory.getValueMetaName(
            sourceFieldsMeta.getValueMeta(sourceIndex).getType());
      }
    }
    return declaredType == null ? "" : declaredType;
  }

  private void loadSqlFromFileAndSetReadOnly() {
    String path = variables.resolve(readWidgetText(DatabaseJoinMeta.WIDGET_SQL_FROM_FILE));
    if (Utils.isEmpty(path)) {
      wSql.setEditable(true);
      return;
    }
    try {
      wSql.setText(HopVfs.getTextFileContent(path, StandardCharsets.UTF_8));
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
    input.setParameters(getDeclaredParametersFromGrid());
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

  private void searchPrevTransformFields() {
    BackgroundThreadFacade.start(
        () -> {
          TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
          if (transformMeta == null) {
            return;
          }
          try {
            IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);
            sourceFieldsMeta = row;
            inputFields.addAll(Arrays.asList(row.getFieldNames()));
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

    // Check the connection before copying widgets into the meta. A name that still holds a
    // variable cannot be resolved here: warn, then save. Anything else stays open and unsaved.
    String connectionName = readWidgetText(DatabaseJoinMeta.WIDGET_CONNECTION);
    if (pipelineMeta.findDatabase(connectionName, variables) == null) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "DatabaseJoinDialog.InvalidConnection.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "DatabaseJoinDialog.InvalidConnection.DialogTitle"));
      mb.open();
      if (!StringUtil.containsVariableToken(connectionName)) {
        return;
      }
    }

    widgets.getWidgetsContents(input, DatabaseJoinMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    persistSqlEditor();
    persistParameters();

    transformName = wTransformName.getText();
    dispose();
  }

  private void get() {
    try {
      IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (row != null && !row.isEmpty()) {
        BaseTransformDialog.getFieldsFromPrevious(
            row, wParam, 1, new int[] {1}, new int[] {2}, -1, -1, null);
      }
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "DatabaseJoinDialog.GetFieldsFailed.DialogTitle"),
          BaseMessages.getString(PKG, "DatabaseJoinDialog.GetFieldsFailed.DialogMessage"),
          e);
    }
  }
}
