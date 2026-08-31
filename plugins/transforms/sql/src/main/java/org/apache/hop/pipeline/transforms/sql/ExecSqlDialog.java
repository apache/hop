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

package org.apache.hop.pipeline.transforms.sql;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.row.IRowMeta;
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
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class ExecSqlDialog extends BaseTransformDialog {
  private static final Class<?> PKG = ExecSqlMeta.class;

  private final ExecSqlMeta input;
  private GuiCompositeWidgets widgets;

  private TextComposite wSql;
  private Label wlPosition;
  private Button wEachRow;
  private Button wSingleStatement;
  private Button wVariables;
  private Button wQuoteString;
  private Label wlFields;
  private TableView wFields;
  private ColumnInfo[] colinf;
  private final List<String> inputFields = new ArrayList<>();

  public ExecSqlDialog(
      Shell parent, IVariables variables, ExecSqlMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "ExecSqlDialog.Shell.Label"));

    changed = input.hasChanged();

    buildButtonBar().ok(e -> ok()).get(e -> get()).cancel(e -> cancel()).build();

    Composite area = new Composite(shell, SWT.NONE);
    PropsUi.setLook(area);
    area.setLayout(new FormLayout());
    FormData fdArea = new FormData();
    fdArea.left = new FormAttachment(0, 0);
    fdArea.top = new FormAttachment(wSpacer, margin);
    fdArea.right = new FormAttachment(100, 0);
    fdArea.bottom = new FormAttachment(wOk, -2 * margin);
    area.setLayoutData(fdArea);

    widgets = new GuiCompositeWidgets(variables);
    widgets.registerExtraGroup(
        BaseMessages.getString(PKG, "ExecSqlMeta.Group.SQL"),
        "20",
        null,
        this::addSqlEditorAndOptions);
    widgets.registerExtraGroup(
        BaseMessages.getString(PKG, "ExecSqlMeta.Group.Parameters"),
        "30",
        null,
        this::addParametersTable);
    widgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            if (!loading) {
              input.setChanged();
            }
            if (ExecSqlMeta.WIDGET_SQL_FROM_FILE.equals(widgetId)) {
              loadSqlFromFileAndSetReadOnly(false);
            }
            if (ExecSqlMeta.WIDGET_BIND_PARAMETERS.equals(widgetId)) {
              setExecutedSetParams();
            }
          }
        });
    widgets.createCompositeWidgets(
        input, null, area, ExecSqlMeta.GUI_PLUGIN_ELEMENT_PARENT_ID, null);

    final Runnable runnable =
        () -> {
          TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
          if (transformMeta != null) {
            try {
              IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);
              for (int i = 0; i < row.size(); i++) {
                inputFields.add(row.getValueMeta(i).getName());
              }
              setComboBoxes();
            } catch (HopException e) {
              logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
            }
          }
        };
    BackgroundThreadFacade.start(runnable);

    getData();
    setExecutedEachInputRow();
    setExecutedSetParams();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void addSqlEditorAndOptions(Composite parent) {
    Control last = widgets.getWidgetsMap().get(ExecSqlMeta.WIDGET_SQL_FROM_FILE);

    Label wlSql = new Label(parent, SWT.LEFT);
    wlSql.setText(BaseMessages.getString(PKG, "ExecSqlDialog.SQL.Label"));
    PropsUi.setLook(wlSql);
    FormData fdlSql = new FormData();
    fdlSql.left = new FormAttachment(0, 0);
    fdlSql.top = last == null ? new FormAttachment(0, margin) : new FormAttachment(last, margin);
    wlSql.setLayoutData(fdlSql);

    wQuoteString = new Button(parent, SWT.CHECK);
    wQuoteString.setText(BaseMessages.getString(PKG, "ExecSqlDialog.QuoteString.Label"));
    wQuoteString.setToolTipText(BaseMessages.getString(PKG, "ExecSqlDialog.QuoteString.Tooltip"));
    PropsUi.setLook(wQuoteString);
    wQuoteString.addListener(SWT.Selection, e -> input.setChanged());
    FormData fdQuoteString = new FormData();
    fdQuoteString.left = new FormAttachment(0, 0);
    fdQuoteString.bottom = new FormAttachment(100, 0);
    wQuoteString.setLayoutData(fdQuoteString);

    wVariables = new Button(parent, SWT.CHECK);
    wVariables.setText(BaseMessages.getString(PKG, "ExecSqlDialog.ReplaceVariables"));
    PropsUi.setLook(wVariables);
    wVariables.addListener(SWT.Selection, e -> input.setChanged());
    FormData fdVariables = new FormData();
    fdVariables.left = new FormAttachment(0, 0);
    fdVariables.bottom = new FormAttachment(wQuoteString, -margin);
    wVariables.setLayoutData(fdVariables);

    wSingleStatement = new Button(parent, SWT.CHECK);
    wSingleStatement.setText(BaseMessages.getString(PKG, "ExecSqlDialog.SingleStatement.Label"));
    PropsUi.setLook(wSingleStatement);
    wSingleStatement.addListener(SWT.Selection, e -> input.setChanged());
    FormData fdSingleStatement = new FormData();
    fdSingleStatement.left = new FormAttachment(0, 0);
    fdSingleStatement.bottom = new FormAttachment(wVariables, -margin);
    wSingleStatement.setLayoutData(fdSingleStatement);

    wEachRow = new Button(parent, SWT.CHECK);
    wEachRow.setText(BaseMessages.getString(PKG, "ExecSqlDialog.EachRow.Label"));
    PropsUi.setLook(wEachRow);
    wEachRow.addListener(
        SWT.Selection,
        e -> {
          setExecutedEachInputRow();
          input.setChanged();
        });
    FormData fdEachRow = new FormData();
    fdEachRow.left = new FormAttachment(0, 0);
    fdEachRow.bottom = new FormAttachment(wSingleStatement, -margin);
    wEachRow.setLayoutData(fdEachRow);

    wlPosition = new Label(parent, SWT.NONE);
    PropsUi.setLook(wlPosition);
    FormData fdlPosition = new FormData();
    fdlPosition.left = new FormAttachment(0, 0);
    fdlPosition.right = new FormAttachment(100, 0);
    fdlPosition.bottom = new FormAttachment(wEachRow, -margin);
    wlPosition.setLayoutData(fdlPosition);

    wSql =
        EnvironmentUtils.getInstance().isWeb()
            ? new StyledTextComp(
                variables,
                parent,
                SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL,
                TextComposite.STYLE_TYPE_SQL)
            : new SQLStyledTextComp(
                variables, parent, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    wSql.addLineStyleListener(getSqlReservedWords());
    PropsUi.setLook(wSql, Props.WIDGET_STYLE_FIXED);
    wSql.addModifyListener(lsMod);
    wSql.addModifyListener(arg0 -> setPosition());
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
    fdSql.bottom = new FormAttachment(wlPosition, -margin);
    fdSql.height = 200;
    wSql.setLayoutData(fdSql);

    setPosition();
  }

  private void addParametersTable(Composite parent) {
    Control last = widgets.getWidgetsMap().get(ExecSqlMeta.WIDGET_BIND_PARAMETERS);

    wlFields = new Label(parent, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "ExecSqlDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = last == null ? new FormAttachment(0, margin) : new FormAttachment(last, margin);
    wlFields.setLayoutData(fdlFields);

    colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "ExecSqlDialog.ColumnInfo.ArgumentFieldname"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
        };

    wFields =
        new TableView(
            variables,
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            input.getArguments().size(),
            lsMod,
            props);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(100, 0);
    fdFields.height = 150;
    wFields.setLayoutData(fdFields);
  }

  private List<String> getSqlReservedWords() {
    String connectionName = getConnectionName();
    if (Utils.isEmpty(connectionName)) {
      return List.of();
    }
    if (variables.resolve(connectionName).startsWith("${")) {
      return List.of();
    }
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connectionName, variables);
    if (databaseMeta == null) {
      return List.of();
    }
    return Arrays.stream(databaseMeta.getReservedWords()).toList();
  }

  private String getConnectionName() {
    Control control = widgets.getWidgetsMap().get(ExecSqlMeta.WIDGET_CONNECTION);
    if (control instanceof MetaSelectionLine<?> line && !Utils.isEmpty(line.getText())) {
      return line.getText();
    }
    return input.getConnection();
  }

  private String getSqlFromFilePath() {
    Control control = widgets.getWidgetsMap().get(ExecSqlMeta.WIDGET_SQL_FROM_FILE);
    if (control instanceof TextVar textVar) {
      return textVar.getText();
    }
    return Const.NVL(input.getSqlFromFile(), "");
  }

  private Button getBindParametersButton() {
    Control control = widgets.getWidgetsMap().get(ExecSqlMeta.WIDGET_BIND_PARAMETERS);
    if (control instanceof Button button) {
      return button;
    }
    return null;
  }

  private void setExecutedEachInputRow() {
    if (wEachRow == null || wFields == null) {
      return;
    }
    boolean eachRow = wEachRow.getSelection();
    wlFields.setEnabled(eachRow);
    wFields.setEnabled(eachRow);
    Button wSetParams = getBindParametersButton();
    Control wlSetParams = widgets.getLabelsMap().get(ExecSqlMeta.WIDGET_BIND_PARAMETERS);
    if (wSetParams != null) {
      wSetParams.setEnabled(eachRow);
    }
    if (wlSetParams != null) {
      wlSetParams.setEnabled(eachRow);
    }
    if (!eachRow) {
      if (wSetParams != null) {
        wSetParams.setSelection(false);
      }
      wQuoteString.setSelection(false);
    }
  }

  private void setExecutedSetParams() {
    if (wQuoteString == null) {
      return;
    }
    Button wSetParams = getBindParametersButton();
    boolean bind = wSetParams != null && wSetParams.getSelection();
    wQuoteString.setEnabled(!bind);
    if (bind) {
      wQuoteString.setSelection(false);
    }
  }

  public void setPosition() {
    int lineNumber = wSql.getLineNumber();
    int columnNumber = wSql.getColumnNumber();
    wlPosition.setText(
        BaseMessages.getString(
            PKG, "ExecSqlDialog.Position.Label", "" + lineNumber, "" + columnNumber));
  }

  protected void setComboBoxes() {
    String[] fieldNames = ConstUi.sortFieldNames(inputFields);
    colinf[0].setComboValues(fieldNames);
  }

  private void loadSqlFromFileAndSetReadOnly(boolean warnOnError) {
    String path = variables.resolve(getSqlFromFilePath());
    if (Utils.isEmpty(path)) {
      wSql.setEditable(true);
      return;
    }
    try {
      String content = HopVfs.getTextFileContent(path, StandardCharsets.UTF_8);
      wSql.setText(content);
      wSql.setEditable(false);
    } catch (HopFileException e) {
      wSql.setEditable(true);
      if (warnOnError) {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_WARNING);
        mb.setText(BaseMessages.getString(PKG, "ExecSqlDialog.DialogCaptionError"));
        mb.setMessage(
            BaseMessages.getString(PKG, "ExecSqlDialog.CouldNotLoadSqlFromFile", path)
                + Const.CR
                + e.getMessage());
        mb.open();
      }
    }
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    widgets.setWidgetsContents(input, shell, ExecSqlMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    wSql.setText(Const.NVL(input.getSql(), ""));
    wEachRow.setSelection(input.isExecutedEachInputRow());
    wSingleStatement.setSelection(input.isSingleStatement());
    wVariables.setSelection(input.isReplaceVariables());
    wQuoteString.setSelection(input.isQuoteString());

    for (int i = 0; i < input.getArguments().size(); i++) {
      TableItem item = wFields.table.getItem(i);
      ExecSqlArgumentItem arg = input.getArguments().get(i);
      if (arg != null) {
        item.setText(1, Const.NVL(arg.getName(), ""));
      }
    }
    wFields.setRowNums();
    wFields.optWidth(true);

    if (!Utils.isEmpty(getSqlFromFilePath())) {
      loadSqlFromFileAndSetReadOnly(true);
    } else {
      wSql.setEditable(true);
    }
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText();
    widgets.getWidgetsContents(input, ExecSqlMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    input.setSql(wSql.getText());
    input.setExecutedEachInputRow(wEachRow.getSelection());
    input.setSingleStatement(wSingleStatement.getSelection());
    input.setReplaceVariables(wVariables.getSelection());
    input.setQuoteString(wQuoteString.getSelection());

    int nrargs = wFields.nrNonEmpty();
    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "ExecSqlDialog.Log.FoundArguments", nrargs + ""));
    }

    input.getArguments().clear();
    for (int i = 0; i < nrargs; i++) {
      TableItem item = wFields.getNonEmpty(i);
      input.getArguments().add(new ExecSqlArgumentItem(item.getText(1)));
    }

    if (Utils.isEmpty(input.getConnection())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG, "ExecSqlDialog.InvalidConnection.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "ExecSqlDialog.InvalidConnection.DialogTitle"));
      mb.open();
    }

    dispose();
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        BaseTransformDialog.getFieldsFromPrevious(
            r, wFields, 1, new int[] {1}, new int[] {}, -1, -1, null);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "ExecSqlDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "ExecSqlDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }
}
