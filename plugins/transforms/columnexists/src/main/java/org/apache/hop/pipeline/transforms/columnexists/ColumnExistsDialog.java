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

package org.apache.hop.pipeline.transforms.columnexists;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class ColumnExistsDialog extends BaseTransformDialog {
  private static final Class<?> PKG = ColumnExistsDialog.class;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private Label wlTableName;
  private CCombo wTableName;

  private Text wResult;

  private Label wlTablenameText;
  private TextVar wTablenameText;

  private CCombo wColumnName;

  private Button wTablenameInField;

  private TextVar wSchemaname;

  private final ColumnExistsMeta input;

  public ColumnExistsDialog(
      Shell parent,
      IVariables variables,
      ColumnExistsMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "ColumnExistsDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ScrolledComposite scrolledComposite = new ScrolledComposite(shell, SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(scrolledComposite);
    FormData fdScrolledComposite = new FormData();
    fdScrolledComposite.left = new FormAttachment(0, 0);
    fdScrolledComposite.top = new FormAttachment(wSpacer, 0);
    fdScrolledComposite.right = new FormAttachment(100, 0);
    fdScrolledComposite.bottom = new FormAttachment(wOk, -margin);
    scrolledComposite.setLayoutData(fdScrolledComposite);
    scrolledComposite.setLayout(new FillLayout());

    Composite wContent = new Composite(scrolledComposite, SWT.NONE);
    PropsUi.setLook(wContent);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();
    wContent.setLayout(contentLayout);

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    // Connection line
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(input.getDatabaseName(), variables);
    wConnection = addConnectionLine(wContent, null, databaseMeta, lsMod);

    // Schema name line
    // Schema name
    Label wlSchemaname = new Label(wContent, SWT.RIGHT);
    wlSchemaname.setText(BaseMessages.getString(PKG, "ColumnExistsDialog.Schemaname.Label"));
    PropsUi.setLook(wlSchemaname);
    FormData fdlSchemaname = new FormData();
    fdlSchemaname.left = new FormAttachment(0, 0);
    fdlSchemaname.right = new FormAttachment(middle, -margin);
    fdlSchemaname.top = new FormAttachment(wConnection, margin);
    wlSchemaname.setLayoutData(fdlSchemaname);

    Button wbSchema = new Button(wContent, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSchema);
    wbSchema.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbSchema = new FormData();
    fdbSchema.top = new FormAttachment(wConnection, margin);
    fdbSchema.right = new FormAttachment(100, 0);
    wbSchema.setLayoutData(fdbSchema);
    wbSchema.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            getSchemaNames();
          }
        });

    wSchemaname = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSchemaname);
    wSchemaname.setToolTipText(
        BaseMessages.getString(PKG, "ColumnExistsDialog.Schemaname.Tooltip"));
    wSchemaname.addModifyListener(lsMod);
    FormData fdSchemaname = new FormData();
    fdSchemaname.left = new FormAttachment(middle, 0);
    fdSchemaname.top = new FormAttachment(wConnection, margin);
    fdSchemaname.right = new FormAttachment(wbSchema, -margin);
    wSchemaname.setLayoutData(fdSchemaname);

    // TablenameText fieldname ...
    wlTablenameText = new Label(wContent, SWT.RIGHT);
    wlTablenameText.setText(
        BaseMessages.getString(PKG, "ColumnExistsDialog.TablenameTextField.Label"));
    PropsUi.setLook(wlTablenameText);
    FormData fdlTablenameText = new FormData();
    fdlTablenameText.left = new FormAttachment(0, 0);
    fdlTablenameText.right = new FormAttachment(middle, -margin);
    fdlTablenameText.top = new FormAttachment(wbSchema, margin);
    wlTablenameText.setLayoutData(fdlTablenameText);

    Button wbTable = new Button(wContent, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTable);
    wbTable.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment(100, 0);
    fdbTable.top = new FormAttachment(wbSchema, margin);
    wbTable.setLayoutData(fdbTable);
    wbTable.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            getTableName();
          }
        });

    wTablenameText = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTablenameText.setToolTipText(
        BaseMessages.getString(PKG, "ColumnExistsDialog.TablenameTextField.Tooltip"));
    PropsUi.setLook(wTablenameText);
    wTablenameText.addModifyListener(lsMod);
    FormData fdTablenameText = new FormData();
    fdTablenameText.left = new FormAttachment(middle, 0);
    fdTablenameText.top = new FormAttachment(wbSchema, margin);
    fdTablenameText.right = new FormAttachment(wbTable, -margin);
    wTablenameText.setLayoutData(fdTablenameText);

    // Is tablename is field?
    Label wlTablenameInField = new Label(wContent, SWT.RIGHT);
    wlTablenameInField.setText(
        BaseMessages.getString(PKG, "ColumnExistsDialog.TablenameInfield.Label"));
    PropsUi.setLook(wlTablenameInField);
    FormData fdlTablenameInField = new FormData();
    fdlTablenameInField.left = new FormAttachment(0, 0);
    fdlTablenameInField.top = new FormAttachment(wTablenameText, margin);
    fdlTablenameInField.right = new FormAttachment(middle, -margin);
    wlTablenameInField.setLayoutData(fdlTablenameInField);
    wTablenameInField = new Button(wContent, SWT.CHECK);
    wTablenameInField.setToolTipText(
        BaseMessages.getString(PKG, "ColumnExistsDialog.TablenameInfield.Tooltip"));
    PropsUi.setLook(wTablenameInField);
    FormData fdTablenameInField = new FormData();
    fdTablenameInField.left = new FormAttachment(middle, 0);
    fdTablenameInField.top = new FormAttachment(wlTablenameInField, 0, SWT.CENTER);
    fdTablenameInField.right = new FormAttachment(100, 0);
    wTablenameInField.setLayoutData(fdTablenameInField);
    SelectionAdapter lsSelR =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
            activeTablenameInField();
          }
        };
    wTablenameInField.addSelectionListener(lsSelR);

    // Dynamic tablename
    wlTableName = new Label(wContent, SWT.RIGHT);
    wlTableName.setText(BaseMessages.getString(PKG, "ColumnExistsDialog.TableName.Label"));
    PropsUi.setLook(wlTableName);
    FormData fdlTableName = new FormData();
    fdlTableName.left = new FormAttachment(0, 0);
    fdlTableName.right = new FormAttachment(middle, -margin);
    fdlTableName.top = new FormAttachment(wTablenameInField, margin);
    wlTableName.setLayoutData(fdlTableName);

    wTableName = new CCombo(wContent, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wTableName);
    wTableName.addModifyListener(lsMod);
    FormData fdTableName = new FormData();
    fdTableName.left = new FormAttachment(middle, 0);
    fdTableName.top = new FormAttachment(wTablenameInField, margin);
    fdTableName.right = new FormAttachment(100, -margin);
    wTableName.setLayoutData(fdTableName);
    wTableName.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Disable Focuslost event
          }

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);
            get();
            shell.setCursor(null);
            busy.dispose();
          }
        });

    // Dynamic column name field
    Label wlColumnName = new Label(wContent, SWT.RIGHT);
    wlColumnName.setText(BaseMessages.getString(PKG, "ColumnExistsDialog.ColumnName.Label"));
    PropsUi.setLook(wlColumnName);
    FormData fdlColumnName = new FormData();
    fdlColumnName.left = new FormAttachment(0, 0);
    fdlColumnName.right = new FormAttachment(middle, -margin);
    fdlColumnName.top = new FormAttachment(wTableName, margin);
    wlColumnName.setLayoutData(fdlColumnName);

    wColumnName = new CCombo(wContent, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wColumnName);
    wColumnName.addModifyListener(lsMod);
    FormData fdColumnName = new FormData();
    fdColumnName.left = new FormAttachment(middle, 0);
    fdColumnName.top = new FormAttachment(wTableName, margin);
    fdColumnName.right = new FormAttachment(100, -margin);
    wColumnName.setLayoutData(fdColumnName);
    wColumnName.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Disable Focuslost event
          }

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);
            get();
            shell.setCursor(null);
            busy.dispose();
          }
        });

    // Result fieldname ...
    Label wlResult = new Label(wContent, SWT.RIGHT);
    wlResult.setText(BaseMessages.getString(PKG, "ColumnExistsDialog.ResultField.Label"));
    PropsUi.setLook(wlResult);
    FormData fdlResult = new FormData();
    fdlResult.left = new FormAttachment(0, 0);
    fdlResult.right = new FormAttachment(middle, -margin);
    fdlResult.top = new FormAttachment(wColumnName, margin);
    wlResult.setLayoutData(fdlResult);
    wResult = new Text(wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wResult.setToolTipText(BaseMessages.getString(PKG, "ColumnExistsDialog.ResultField.Tooltip"));
    PropsUi.setLook(wResult);
    wResult.addModifyListener(lsMod);
    FormData fdResult = new FormData();
    fdResult.left = new FormAttachment(middle, 0);
    fdResult.top = new FormAttachment(wColumnName, margin);
    fdResult.right = new FormAttachment(100, 0);
    wResult.setLayoutData(fdResult);

    wContent.pack();
    Rectangle bounds = wContent.getBounds();
    scrolledComposite.setContent(wContent);
    scrolledComposite.setExpandHorizontal(true);
    scrolledComposite.setExpandVertical(true);
    scrolledComposite.setMinWidth(bounds.width);
    scrolledComposite.setMinHeight(bounds.height);

    getData();
    activeTablenameInField();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void activeTablenameInField() {
    wlTableName.setEnabled(wTablenameInField.getSelection());
    wTableName.setEnabled(wTablenameInField.getSelection());
    wTablenameText.setEnabled(!wTablenameInField.getSelection());
    wlTablenameText.setEnabled(!wTablenameInField.getSelection());
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "ColumnExistsDialog.Log.GettingKeyInfo"));
    }

    if (input.getDatabaseName() != null) {
      wConnection.setText(input.getDatabaseName());
    }
    if (input.getSchemaname() != null) {
      wSchemaname.setText(input.getSchemaname());
    }
    if (input.getTableName() != null) {
      wTablenameText.setText(input.getTableName());
    }
    wTablenameInField.setSelection(input.isTablenameInfield());
    if (input.getTablenamefield() != null) {
      wTableName.setText(input.getTablenamefield());
    }
    if (input.getColumnnamefield() != null) {
      wColumnName.setText(input.getColumnnamefield());
    }
    if (input.getResultfieldname() != null) {
      wResult.setText(input.getResultfieldname());
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

    input.setDatabaseName(wConnection.getText());
    input.setSchemaname(wSchemaname.getText());
    input.setTableName(wTablenameText.getText());
    input.setTablenameInfield(wTablenameInField.getSelection());
    input.setTablenamefield(wTableName.getText());
    input.setColumnnamefield(wColumnName.getText());
    input.setResultfieldname(wResult.getText());

    transformName = wTransformName.getText(); // return value

    if (input.getDatabaseName() == null) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "ColumnExistsDialog.InvalidConnection.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "ColumnExistsDialog.InvalidConnection.DialogTitle"));
      mb.open();
    }

    dispose();
  }

  private void get() {
    try {
      String columnName = wColumnName.getText();
      String tableName = wTableName.getText();

      wColumnName.removeAll();
      wTableName.removeAll();
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        r.getFieldNames();

        for (int i = 0; i < r.getFieldNames().length; i++) {
          wTableName.add(r.getFieldNames()[i]);
          wColumnName.add(r.getFieldNames()[i]);
        }
      }
      wColumnName.setText(columnName);
      wTableName.setText(tableName);
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "ColumnExistsDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "ColumnExistsDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  private void getTableName() {
    String connectionName = wConnection.getText();
    if (StringUtils.isEmpty(connectionName)) {
      return;
    }
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connectionName, variables);
    if (databaseMeta != null) {
      DatabaseExplorerDialog std =
          new DatabaseExplorerDialog(
              shell, SWT.NONE, variables, databaseMeta, pipelineMeta.getDatabases());
      std.setSelectedSchemaAndTable(wSchemaname.getText(), wTablenameText.getText());
      if (std.open()) {
        wSchemaname.setText(Const.NVL(std.getSchemaName(), ""));
        wTablenameText.setText(Const.NVL(std.getTableName(), ""));
      }
    } else {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "ColumnExistsDialog.ConnectionError2.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
      mb.open();
    }
  }

  private void getSchemaNames() {
    if (wSchemaname.isDisposed()) {
      return;
    }
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(wConnection.getText(), variables);
    if (databaseMeta != null) {
      try (Database database = new Database(loggingObject, variables, databaseMeta)) {
        database.connect();
        String[] schemas = database.getSchemas();

        if (null != schemas && schemas.length > 0) {
          schemas = Const.sortStrings(schemas);
          EnterSelectionDialog dialog =
              new EnterSelectionDialog(
                  shell,
                  schemas,
                  BaseMessages.getString(
                      PKG, "System.Dialog.AvailableSchemas.Title", wConnection.getText()),
                  BaseMessages.getString(PKG, "System.Dialog.AvailableSchemas.Message"));
          String d = dialog.open();
          if (d != null) {
            wSchemaname.setText(Const.NVL(d, ""));
          }

        } else {
          MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
          mb.setMessage(
              BaseMessages.getString(PKG, "System.Dialog.AvailableSchemas.Empty.Message"));
          mb.setText(BaseMessages.getString(PKG, "System.Dialog.AvailableSchemas.Empty.Title"));
          mb.open();
        }
      } catch (Exception e) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
            BaseMessages.getString(PKG, "System.Dialog.AvailableSchemas.ConnectionError"),
            e);
      }
    }
  }
}
