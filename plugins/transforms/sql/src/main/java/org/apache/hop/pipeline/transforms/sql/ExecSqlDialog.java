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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.SQLStyledTextComp;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextComposite;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class ExecSqlDialog extends BaseTransformDialog {
  private static final Class<?> PKG = ExecSqlMeta.class;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TextComposite wSql;

  private Button wEachRow;

  private Label wlSetParams;
  private Button wSetParams;

  private Button wSingleStatement;

  private Text wInsertField;

  private Text wUpdateField;

  private Text wDeleteField;

  private Text wReadField;

  private Label wlFields;

  private TableView wFields;

  private Button wVariables;

  private Label wlQuoteString;
  private Button wQuoteString;

  private final ExecSqlMeta input;
  private boolean changedInDialog;

  private Label wlPosition;

  private final List<String> inputFields = new ArrayList<>();

  private ColumnInfo[] colinf;

  public ExecSqlDialog(
      Shell parent, IVariables variables, ExecSqlMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "ExecSqlDialog.Shell.Label"));

    buildButtonBar().ok(e -> ok()).get(e -> get()).cancel(e -> cancel()).build();

    ModifyListener lsMod =
        e -> {
          changedInDialog = true;
          input.setChanged();
        };

    SelectionAdapter lsSel =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        };

    changed = input.hasChanged();

    SelectionListener lsSelection =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        };

    // Connection line
    wConnection = addConnectionLine(shell, wSpacer, input.getConnection(), lsMod);
    wConnection.addSelectionListener(lsSelection);
    wConnection.addListener(SWT.Selection, e -> getSqlReservedWords());

    // Table line...
    Label wlSql = new Label(shell, SWT.LEFT);
    wlSql.setText(BaseMessages.getString(PKG, "ExecSqlDialog.SQL.Label"));
    PropsUi.setLook(wlSql);
    FormData fdlSql = new FormData();
    fdlSql.left = new FormAttachment(0, 0);
    fdlSql.top = new FormAttachment(wConnection, margin);
    wlSql.setLayoutData(fdlSql);

    wSql =
        EnvironmentUtils.getInstance().isWeb()
            ? new StyledTextComp(
                variables, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL)
            : new SQLStyledTextComp(
                variables, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    final List<String> sqlKeywords = getSqlReservedWords();

    wSql.addLineStyleListener(sqlKeywords);

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

    // Build it up from the bottom up...
    // Read field
    //
    Label wlReadField = new Label(shell, SWT.RIGHT);
    wlReadField.setText(BaseMessages.getString(PKG, "ExecSqlDialog.ReadField.Label"));
    PropsUi.setLook(wlReadField);
    FormData fdlReadField = new FormData();
    fdlReadField.left = new FormAttachment(middle, margin);
    fdlReadField.right = new FormAttachment(middle * 2, -margin);
    fdlReadField.bottom = new FormAttachment(wOk, -margin);
    wlReadField.setLayoutData(fdlReadField);
    wReadField = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wReadField);
    wReadField.addModifyListener(lsMod);
    FormData fdReadField = new FormData();
    fdReadField.left = new FormAttachment(middle * 2, 0);
    fdReadField.bottom = new FormAttachment(wOk, -3 * margin);
    fdReadField.right = new FormAttachment(100, 0);
    wReadField.setLayoutData(fdReadField);

    // Delete field
    //
    Label wlDeleteField = new Label(shell, SWT.RIGHT);
    wlDeleteField.setText(BaseMessages.getString(PKG, "ExecSqlDialog.DeleteField.Label"));
    PropsUi.setLook(wlDeleteField);
    FormData fdlDeleteField = new FormData();
    fdlDeleteField.left = new FormAttachment(middle, margin);
    fdlDeleteField.right = new FormAttachment(middle * 2, -margin);
    fdlDeleteField.bottom = new FormAttachment(wReadField, -margin);
    wlDeleteField.setLayoutData(fdlDeleteField);
    wDeleteField = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDeleteField);
    wDeleteField.addModifyListener(lsMod);
    FormData fdDeleteField = new FormData();
    fdDeleteField.left = new FormAttachment(middle * 2, 0);
    fdDeleteField.bottom = new FormAttachment(wReadField, -margin);
    fdDeleteField.right = new FormAttachment(100, 0);
    wDeleteField.setLayoutData(fdDeleteField);

    // Update field
    //
    Label wlUpdateField = new Label(shell, SWT.RIGHT);
    wlUpdateField.setText(BaseMessages.getString(PKG, "ExecSqlDialog.UpdateField.Label"));
    PropsUi.setLook(wlUpdateField);
    FormData fdlUpdateField = new FormData();
    fdlUpdateField.left = new FormAttachment(middle, margin);
    fdlUpdateField.right = new FormAttachment(middle * 2, -margin);
    fdlUpdateField.bottom = new FormAttachment(wDeleteField, -margin);
    wlUpdateField.setLayoutData(fdlUpdateField);
    wUpdateField = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wUpdateField);
    wUpdateField.addModifyListener(lsMod);
    FormData fdUpdateField = new FormData();
    fdUpdateField.left = new FormAttachment(middle * 2, 0);
    fdUpdateField.bottom = new FormAttachment(wDeleteField, -margin);
    fdUpdateField.right = new FormAttachment(100, 0);
    wUpdateField.setLayoutData(fdUpdateField);

    // insert field
    //
    Label wlInsertField = new Label(shell, SWT.RIGHT);
    wlInsertField.setText(BaseMessages.getString(PKG, "ExecSqlDialog.InsertField.Label"));
    PropsUi.setLook(wlInsertField);
    FormData fdlInsertField = new FormData();
    fdlInsertField.left = new FormAttachment(middle, margin);
    fdlInsertField.right = new FormAttachment(middle * 2, -margin);
    fdlInsertField.bottom = new FormAttachment(wUpdateField, -margin);
    wlInsertField.setLayoutData(fdlInsertField);
    wInsertField = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wInsertField);
    wInsertField.addModifyListener(lsMod);
    FormData fdInsertField = new FormData();
    fdInsertField.left = new FormAttachment(middle * 2, 0);
    fdInsertField.bottom = new FormAttachment(wUpdateField, -margin);
    fdInsertField.right = new FormAttachment(100, 0);
    wInsertField.setLayoutData(fdInsertField);

    // Setup the "Parameters" label
    //
    wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "ExecSqlDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.right = new FormAttachment(middle, 0);
    fdlFields.bottom = new FormAttachment(wInsertField, -25);
    wlFields.setLayoutData(fdlFields);

    // Parameter fields...
    //
    final int FieldsRows = input.getArguments().size();

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
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
            lsMod,
            props);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(middle, 0);
    fdFields.bottom = new FormAttachment(wOk, -3 * margin);
    wFields.setLayoutData(fdFields);

    // For the "execute for each row" and "variable substitution" labels,
    // find their maximum width
    // and use that in the alignment
    //
    Label wlEachRow = new Label(shell, SWT.RIGHT);
    wlEachRow.setText(BaseMessages.getString(PKG, "ExecSqlDialog.EachRow.Label"));
    wlEachRow.pack();
    Label wlSingleStatement = new Label(shell, SWT.RIGHT);
    wlSingleStatement.setText(BaseMessages.getString(PKG, "ExecSqlDialog.SingleStatement.Label"));
    wlSingleStatement.pack();
    Label wlVariables = new Label(shell, SWT.RIGHT);
    wlVariables.setText(BaseMessages.getString(PKG, "ExecSqlDialog.ReplaceVariables"));
    wlVariables.pack();
    wlQuoteString = new Label(shell, SWT.RIGHT);
    wlQuoteString.setText(BaseMessages.getString(PKG, "ExecSqlDialog.QuoteString.Label"));
    wlQuoteString.pack();
    Rectangle rEachRow = wlEachRow.getBounds();
    Rectangle rSingleStatement = wlSingleStatement.getBounds();
    Rectangle rVariables = wlVariables.getBounds();
    Rectangle rQuoteString = wlQuoteString.getBounds();
    int width =
        Math.max(
                Math.max(Math.max(rEachRow.width, rSingleStatement.width), rVariables.width),
                rQuoteString.width)
            + 30;

    // Setup the "Quote String" label and checkbox
    //
    PropsUi.setLook(wlQuoteString);
    FormData fdlQuoteString = new FormData();
    fdlQuoteString.left = new FormAttachment(0, margin);
    fdlQuoteString.right = new FormAttachment(0, width);
    fdlQuoteString.bottom = new FormAttachment(wlFields, -margin);
    wlQuoteString.setLayoutData(fdlQuoteString);
    wQuoteString = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wQuoteString);
    wQuoteString.setToolTipText(BaseMessages.getString(PKG, "ExecSqlDialog.QuoteString.Tooltip"));
    FormData fdQuoteString = new FormData();
    fdQuoteString.left = new FormAttachment(wlQuoteString, margin);
    fdQuoteString.top = new FormAttachment(wlQuoteString, 0, SWT.CENTER);
    fdQuoteString.right = new FormAttachment(middle, 0);
    wQuoteString.setLayoutData(fdQuoteString);
    wQuoteString.addSelectionListener(lsSel);

    // Setup the "Bind parameters" label and checkbox
    //
    wlSetParams = new Label(this.shell, SWT.RIGHT);
    wlSetParams.setText(BaseMessages.getString(PKG, "ExecSqlDialog.SetParams.Label"));
    PropsUi.setLook(this.wlSetParams);
    FormData fdlSetParams = new FormData();
    fdlSetParams.left = new FormAttachment(0, margin);
    fdlSetParams.bottom = new FormAttachment(wQuoteString, -margin);
    fdlSetParams.right = new FormAttachment(0, width);
    wlSetParams.setLayoutData(fdlSetParams);
    wSetParams = new Button(shell, SWT.CHECK);
    PropsUi.setLook(this.wSetParams);
    wSetParams.setToolTipText(BaseMessages.getString(PKG, "ExecSqlDialog.SetParams.Tooltip"));
    FormData fdSetParams = new FormData();
    fdSetParams.left = new FormAttachment(wlSetParams, margin);
    fdSetParams.top = new FormAttachment(wlSetParams, 0, SWT.CENTER);
    fdSetParams.right = new FormAttachment(middle, 0);
    wSetParams.setLayoutData(fdSetParams);
    wSetParams.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            setExecutedSetParams();
            input.setChanged();
          }
        });

    // Setup the "variable substitution" label and checkbox
    //
    PropsUi.setLook(wlVariables);
    FormData fdlVariables = new FormData();
    fdlVariables.left = new FormAttachment(0, margin);
    fdlVariables.right = new FormAttachment(0, width);
    fdlVariables.bottom = new FormAttachment(wSetParams, -margin);
    wlVariables.setLayoutData(fdlVariables);
    wVariables = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wVariables);
    FormData fdVariables = new FormData();
    fdVariables.left = new FormAttachment(wlVariables, margin);
    fdVariables.top = new FormAttachment(wlVariables, 0, SWT.CENTER);
    fdVariables.right = new FormAttachment(middle, 0);
    wVariables.setLayoutData(fdVariables);
    wVariables.addSelectionListener(lsSel);

    // Setup the "Single statement" label and checkbox
    //
    PropsUi.setLook(wlSingleStatement);
    FormData fdlSingleStatement = new FormData();
    fdlSingleStatement.left = new FormAttachment(0, margin);
    fdlSingleStatement.right = new FormAttachment(0, width);
    fdlSingleStatement.bottom = new FormAttachment(wVariables, -margin);
    wlSingleStatement.setLayoutData(fdlSingleStatement);
    wSingleStatement = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wSingleStatement);
    FormData fdSingleStatement = new FormData();
    fdSingleStatement.left = new FormAttachment(wlEachRow, margin);
    fdSingleStatement.top = new FormAttachment(wlSingleStatement, 0, SWT.CENTER);
    fdSingleStatement.right = new FormAttachment(middle, 0);
    wSingleStatement.setLayoutData(fdSingleStatement);
    wSingleStatement.addSelectionListener(lsSel);

    // Setup the "execute for each row" label and checkbox
    //
    PropsUi.setLook(wlEachRow);
    FormData fdlEachRow = new FormData();
    fdlEachRow.left = new FormAttachment(0, margin);
    fdlEachRow.right = new FormAttachment(0, width);
    fdlEachRow.bottom = new FormAttachment(wSingleStatement, -margin);
    wlEachRow.setLayoutData(fdlEachRow);
    wEachRow = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wEachRow);
    FormData fdEachRow = new FormData();
    fdEachRow.left = new FormAttachment(wlEachRow, margin);
    fdEachRow.top = new FormAttachment(wlEachRow, 0, SWT.CENTER);
    fdEachRow.right = new FormAttachment(middle, 0);
    wEachRow.setLayoutData(fdEachRow);
    wEachRow.addSelectionListener(lsSel);

    // Position label under the SQL editor
    //
    wlPosition = new Label(shell, SWT.NONE);
    PropsUi.setLook(wlPosition);
    FormData fdlPosition = new FormData();
    fdlPosition.left = new FormAttachment(0, 0);
    fdlPosition.right = new FormAttachment(100, 0);
    fdlPosition.bottom =
        new FormAttachment(wEachRow, -margin); // 2 times since we deal with bottom instead of
    // top
    wlPosition.setLayoutData(fdlPosition);

    // Finally, the SQL editor takes up all other variables between the position and the SQL label
    //
    FormData fdSql = new FormData();
    fdSql.left = new FormAttachment(0, 0);
    fdSql.top = new FormAttachment(wlSql, margin);
    fdSql.right = new FormAttachment(100, -margin);
    fdSql.bottom = new FormAttachment(wlPosition, -margin);
    fdSql.height = 200;
    wSql.setLayoutData(fdSql);

    // Search the fields in the background
    //
    final Runnable runnable =
        () -> {
          TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
          if (transformMeta != null) {
            try {
              IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);

              // Remember these fields...
              for (int i = 0; i < row.size(); i++) {
                inputFields.add(row.getValueMeta(i).getName());
              }
              setComboBoxes();
            } catch (HopException e) {
              logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
            }
          }
        };
    new Thread(runnable).start();

    // Add listeners
    //
    wEachRow.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            ExecSqlDialog.this.setExecutedEachInputRow();
            ExecSqlDialog.this.input.setChanged();
          }
        });

    getData();
    setExecutedEachInputRow();
    setExecutedSetParams();
    changedInDialog = false; // for prompting if dialog is simply closed
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private List<String> getSqlReservedWords() {
    // Do not search keywords when connection is empty
    if (Utils.isEmpty(input.getConnection())) {
      return List.of();
    }

    // If connection is a variable that can't be resolved
    if (variables.resolve(input.getConnection()).startsWith("${")) {
      return List.of();
    }

    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(input.getConnection(), variables);
    return Arrays.stream(databaseMeta.getReservedWords()).toList();
  }

  private void setExecutedEachInputRow() {

    wlFields.setEnabled(wEachRow.getSelection());
    wFields.setEnabled(wEachRow.getSelection());
    wlSetParams.setEnabled(wEachRow.getSelection());
    wSetParams.setEnabled(wEachRow.getSelection());
    if (!wEachRow.getSelection()) {
      wSetParams.setSelection(wEachRow.getSelection());
    }

    if (!wEachRow.getSelection()) {
      wQuoteString.setSelection(wEachRow.getSelection());
    }
  }

  private void setExecutedSetParams() {
    wlQuoteString.setEnabled(!wSetParams.getSelection());
    wQuoteString.setEnabled(!wSetParams.getSelection());
    if (wSetParams.getSelection()) {
      wQuoteString.setSelection(!wSetParams.getSelection());
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
    // Something was changed in the row.
    //
    String[] fieldNames = ConstUi.sortFieldNames(inputFields);
    colinf[0].setComboValues(fieldNames);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (input.getSql() != null) {
      wSql.setText(input.getSql());
    }
    if (input.getConnection() != null) {
      wConnection.setText(input.getConnection());
    }
    wEachRow.setSelection(input.isExecutedEachInputRow());
    wSingleStatement.setSelection(input.isSingleStatement());
    wVariables.setSelection(input.isReplaceVariables());
    wQuoteString.setSelection(input.isQuoteString());

    if (input.getUpdateField() != null) {
      wUpdateField.setText(input.getUpdateField());
    }
    if (input.getInsertField() != null) {
      wInsertField.setText(input.getInsertField());
    }
    if (input.getDeleteField() != null) {
      wDeleteField.setText(input.getDeleteField());
    }
    if (input.getReadField() != null) {
      wReadField.setText(input.getReadField());
    }

    for (int i = 0; i < input.getArguments().size(); i++) {
      TableItem item = wFields.table.getItem(i);
      ExecSqlArgumentItem arg = input.getArguments().get(i);
      if (arg != null) {
        item.setText(1, arg.getName());
      }
    }
    wSetParams.setSelection(input.isParams());
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

    transformName = wTransformName.getText(); // return value
    // copy info to TextFileInputMeta class (input)
    input.setSql(wSql.getText());
    input.setConnection(wConnection.getText());
    input.setExecutedEachInputRow(wEachRow.getSelection());
    input.setSingleStatement(wSingleStatement.getSelection());
    input.setReplaceVariables(wVariables.getSelection());
    input.setQuoteString(wQuoteString.getSelection());
    input.setParams(wSetParams.getSelection());
    input.setInsertField(wInsertField.getText());
    input.setUpdateField(wUpdateField.getText());
    input.setDeleteField(wDeleteField.getText());
    input.setReadField(wReadField.getText());

    int nrargs = wFields.nrNonEmpty();
    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "ExecSqlDialog.Log.FoundArguments", +nrargs + ""));
    }

    input.getArguments().clear();
    for (int i = 0; i < nrargs; i++) {
      TableItem item = wFields.getNonEmpty(i);
      input.getArguments().add(new ExecSqlArgumentItem(item.getText(1)));
    }

    if (input.getConnection() == null) {
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
