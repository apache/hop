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

package org.apache.hop.pipeline.transforms.delete;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class DeleteDialog extends BaseTransformDialog {
  private static final Class<?> PKG = DeleteMeta.class;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TableView wKey;

  private TextVar wSchema;

  private TextVar wTable;

  private TextVar wCommit;

  private Button wBatch;

  private final DeleteMeta input;

  private final List<String> inputFields = new ArrayList<>();

  private ColumnInfo[] ciKey;

  /** List of ColumnInfo that should have the field names of the selected database table */
  private final List<ColumnInfo> tableFieldColumns = new ArrayList<>();

  public DeleteDialog(
      Shell parent, IVariables variables, DeleteMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "DeleteDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    ModifyListener lsTableMod =
        arg0 -> {
          input.setChanged();
          setTableFieldCombo();
        };
    SelectionListener lsSelection =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            setTableFieldCombo();
          }
        };
    changed = input.hasChanged();

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    addGeneralTab(wTabFolder, lsMod, lsTableMod, lsSelection);
    addKeysTab(wTabFolder, lsMod);

    wTabFolder.setLayoutData(
        FormDataBuilder.builder()
            .left()
            .top(wSpacer, margin)
            .right()
            .bottom(wOk, -margin)
            .result());
    wTabFolder.setSelection(0);

    //
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

    getData();
    setTableFieldCombo();
    setFlags();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void addGeneralTab(
      CTabFolder wTabFolder,
      ModifyListener lsMod,
      ModifyListener lsTableMod,
      SelectionListener lsSelection) {
    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setFont(GuiResource.getInstance().getFontDefault());
    wGeneralTab.setText(BaseMessages.getString(PKG, "DeleteDialog.GeneralTab.Title"));

    Composite composite = new Composite(wTabFolder, SWT.NONE);
    composite.setLayout(props.createFormLayout());
    PropsUi.setLook(composite);

    // Connection line
    wConnection = addConnectionLine(composite, null, input.getConnection(), lsMod);
    wConnection.addSelectionListener(lsSelection);

    // Schema line...
    Label wlSchema = new Label(composite, SWT.RIGHT);
    wlSchema.setText(BaseMessages.getString(PKG, "DeleteDialog.TargetSchema.Label"));
    PropsUi.setLook(wlSchema);
    FormData fdlSchema = new FormData();
    fdlSchema.left = new FormAttachment(0, 0);
    fdlSchema.right = new FormAttachment(middle, -margin);
    fdlSchema.top = new FormAttachment(wConnection, margin);
    wlSchema.setLayoutData(fdlSchema);

    Button wbSchema = new Button(composite, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSchema);
    wbSchema.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbSchema = new FormData();
    fdbSchema.top = new FormAttachment(wConnection, margin);
    fdbSchema.right = new FormAttachment(100, 0);
    wbSchema.setLayoutData(fdbSchema);
    wbSchema.addListener(SWT.Selection, e -> getSchemaNames());

    wSchema = new TextVar(variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSchema);
    wSchema.addModifyListener(lsTableMod);
    FormData fdSchema = new FormData();
    fdSchema.left = new FormAttachment(middle, 0);
    fdSchema.top = new FormAttachment(wConnection, margin);
    fdSchema.right = new FormAttachment(wbSchema, -margin);
    wSchema.setLayoutData(fdSchema);

    // Table line...
    Label wlTable = new Label(composite, SWT.RIGHT);
    wlTable.setText(BaseMessages.getString(PKG, "DeleteDialog.TargetTable.Label"));
    PropsUi.setLook(wlTable);
    FormData fdlTable = new FormData();
    fdlTable.left = new FormAttachment(0, 0);
    fdlTable.right = new FormAttachment(middle, -margin);
    fdlTable.top = new FormAttachment(wbSchema, margin);
    wlTable.setLayoutData(fdlTable);

    Button wbTable = new Button(composite, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTable);
    wbTable.setText(BaseMessages.getString(PKG, "DeleteDialog.Browse.Button"));
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment(100, 0);
    fdbTable.top = new FormAttachment(wbSchema, margin);
    wbTable.setLayoutData(fdbTable);
    wbTable.addListener(SWT.Selection, e -> getTableName());

    wTable = new TextVar(variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTable);
    wTable.addModifyListener(lsTableMod);
    FormData fdTable = new FormData();
    fdTable.left = new FormAttachment(middle, 0);
    fdTable.top = new FormAttachment(wbSchema, margin);
    fdTable.right = new FormAttachment(wbTable, -margin);
    wTable.setLayoutData(fdTable);

    // Commit line
    Label wlCommit = new Label(composite, SWT.RIGHT);
    wlCommit.setText(BaseMessages.getString(PKG, "DeleteDialog.Commit.Label"));
    PropsUi.setLook(wlCommit);
    FormData fdlCommit = new FormData();
    fdlCommit.left = new FormAttachment(0, 0);
    fdlCommit.top = new FormAttachment(wTable, margin);
    fdlCommit.right = new FormAttachment(middle, -margin);
    wlCommit.setLayoutData(fdlCommit);
    wCommit = new TextVar(variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCommit);
    wCommit.addModifyListener(lsMod);
    FormData fdCommit = new FormData();
    fdCommit.left = new FormAttachment(middle, 0);
    fdCommit.top = new FormAttachment(wTable, margin);
    fdCommit.right = new FormAttachment(100, 0);
    wCommit.setLayoutData(fdCommit);

    // Batch delete
    Label wlBatch = new Label(composite, SWT.RIGHT);
    wlBatch.setText(BaseMessages.getString(PKG, "DeleteDialog.Batch.Label"));
    PropsUi.setLook(wlBatch);
    FormData fdlBatch = new FormData();
    fdlBatch.left = new FormAttachment(0, 0);
    fdlBatch.top = new FormAttachment(wCommit, margin);
    fdlBatch.right = new FormAttachment(middle, -margin);
    wlBatch.setLayoutData(fdlBatch);
    wBatch = new Button(composite, SWT.CHECK);
    PropsUi.setLook(wBatch);
    FormData fdBatch = new FormData();
    fdBatch.left = new FormAttachment(middle, 0);
    fdBatch.top = new FormAttachment(wlBatch, 0, SWT.CENTER);
    fdBatch.right = new FormAttachment(100, 0);
    wBatch.setLayoutData(fdBatch);
    wBatch.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            setFlags();
            input.setChanged();
          }
        });

    wGeneralTab.setControl(composite);
  }

  private void addKeysTab(CTabFolder wTabFolder, ModifyListener lsMod) {
    Composite composite = new Composite(wTabFolder, SWT.NONE);
    composite.setLayout(props.createFormLayout());
    PropsUi.setLook(composite);

    CTabItem tabItem = new CTabItem(wTabFolder, SWT.NONE);
    tabItem.setFont(GuiResource.getInstance().getFontDefault());
    tabItem.setText(BaseMessages.getString(PKG, "DeleteDialog.KeysTab.Title"));
    tabItem.setControl(composite);

    Label wlKey = new Label(composite, SWT.NONE);
    wlKey.setText(BaseMessages.getString(PKG, "DeleteDialog.Key.Label"));
    PropsUi.setLook(wlKey);
    FormData fdlKey = new FormData();
    fdlKey.left = new FormAttachment(0, 0);
    fdlKey.top = new FormAttachment(0, 0);
    wlKey.setLayoutData(fdlKey);

    int nrKeyCols = 4;
    List<DeleteKeyField> keyFields = input.getLookup().getFields();
    int nrKeyRows =
        (keyFields != null && !keyFields.equals(Collections.emptyList()) ? keyFields.size() : 1);

    ciKey = new ColumnInfo[nrKeyCols];
    ciKey[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DeleteDialog.ColumnInfo.TableField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciKey[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DeleteDialog.ColumnInfo.Comparator"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {
              "=", "<>", "<", "<=", ">", ">=", "LIKE", "BETWEEN", "IS NULL", "IS NOT NULL"
            });
    ciKey[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DeleteDialog.ColumnInfo.StreamField1"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciKey[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DeleteDialog.ColumnInfo.StreamField2"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    tableFieldColumns.add(ciKey[0]);
    wKey =
        new TableView(
            variables,
            composite,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciKey,
            nrKeyRows,
            lsMod,
            props);

    wGet = new Button(composite, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "DeleteDialog.GetFields.Button"));
    wGet.addListener(SWT.Selection, e -> get());
    PropsUi.setLook(wGet);

    setButtonPositions(new Button[] {wGet}, margin, null);

    FormData fdKey = new FormData();
    fdKey.left = new FormAttachment(0, 0);
    fdKey.top = new FormAttachment(wlKey, margin);
    fdKey.right = new FormAttachment(100, 0);
    fdKey.bottom = new FormAttachment(wGet, -margin);
    wKey.setLayoutData(fdKey);
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    String[] fieldNames = ConstUi.sortFieldNames(inputFields);
    ciKey[2].setComboValues(fieldNames);
    ciKey[3].setComboValues(fieldNames);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "DeleteDialog.Log.GettingKeyInfo"));
    }

    wCommit.setText(input.getCommitSizeVar());
    wBatch.setSelection(input.isUseBatchUpdate());

    List<DeleteKeyField> keyFields = input.getLookup().getFields();

    if (keyFields != null && !keyFields.equals(Collections.emptyList())) {
      for (int i = 0; i < keyFields.size(); i++) {
        TableItem item = wKey.table.getItem(i);
        DeleteKeyField field = keyFields.get(i);
        if (field.getKeyLookup() != null) {
          item.setText(1, field.getKeyLookup());
        }
        if (field.getKeyCondition() != null) {
          item.setText(2, field.getKeyCondition());
        }
        if (field.getKeyStream() != null) {
          item.setText(3, field.getKeyStream());
        }
        if (field.getKeyStream2() != null) {
          item.setText(4, field.getKeyStream2());
        }
      }
    }

    if (input.getLookup().getSchemaName() != null) {
      wSchema.setText(input.getLookup().getSchemaName());
    }
    if (input.getLookup().getTableName() != null) {
      wTable.setText(input.getLookup().getTableName());
    }

    if (input.getConnection() != null) {
      wConnection.setText(input.getConnection());
    }

    wKey.setRowNums();
    wKey.optWidth(true);
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  /**
   * Updates dialog control states based on the current configuration.
   *
   * <p>Batch deletes are disabled when this transform uses error handling and the selected database
   * does not support batch updates together with error handling (for example mysql and look-likes).
   */
  public void setFlags() {
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(wConnection.getText(), variables);
    boolean hasErrorHandling = pipelineMeta.findTransform(transformName).isDoingErrorHandling();

    boolean enableBatch = wBatch.getSelection();
    enableBatch =
        enableBatch
            && !(databaseMeta != null
                && databaseMeta.supportsErrorHandlingOnBatchUpdates()
                && hasErrorHandling);
    wBatch.setSelection(enableBatch);
  }

  private void setTableFieldCombo() {
    Runnable fieldLoader =
        () -> {
          if (!wTable.isDisposed() && !wConnection.isDisposed() && !wSchema.isDisposed()) {
            final String tableName = wTable.getText();
            final String connectionName = wConnection.getText();
            final String schemaName = wSchema.getText();

            // clear
            for (ColumnInfo colInfo : tableFieldColumns) {
              colInfo.setComboValues(new String[] {});
            }
            if (!Utils.isEmpty(tableName)) {
              DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connectionName, variables);
              if (databaseMeta != null) {
                Database database = new Database(loggingObject, variables, databaseMeta);
                try {
                  database.connect();

                  IRowMeta r =
                      database.getTableFieldsMeta(
                          variables.resolve(schemaName), variables.resolve(tableName));
                  if (null != r) {
                    String[] fieldNames = r.getFieldNames();
                    if (null != fieldNames) {
                      for (ColumnInfo colInfo : tableFieldColumns) {
                        colInfo.setComboValues(fieldNames);
                      }
                    }
                  }
                } catch (Exception e) {
                  for (ColumnInfo colInfo : tableFieldColumns) {
                    colInfo.setComboValues(new String[] {});
                  }
                  // ignore any errors here. drop downs will not be
                  // filled, but no problem for the user
                } finally {
                  try {
                    if (database != null) {
                      database.disconnect();
                    }
                  } catch (Exception ignored) {
                    // ignore any errors here.
                    database = null;
                  }
                }
              }
            }
          }
        };
    shell.getDisplay().asyncExec(fieldLoader);
  }

  private void getInfo(DeleteMeta inf) {
    int nrkeys = wKey.nrNonEmpty();

    inf.setCommitSize(wCommit.getText());
    inf.setUseBatchUpdate(wBatch.getSelection());

    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "DeleteDialog.Log.FoundKeys", String.valueOf(nrkeys)));
    }
    List<DeleteKeyField> keyFields = inf.getLookup().getFields();
    keyFields.clear();

    for (int i = 0; i < nrkeys; i++) {
      TableItem item = wKey.getNonEmpty(i);
      DeleteKeyField f =
          new DeleteKeyField(item.getText(1), item.getText(2), item.getText(3), item.getText(4));
      keyFields.add(f);
    }

    inf.getLookup().setSchemaName(wSchema.getText());
    inf.getLookup().setTableName(wTable.getText());
    inf.setConnection(wConnection.getText());

    transformName = wTransformName.getText(); // return value
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    if (wConnection.getText() == null) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG, "DeleteDialog.InvalidConnection.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "DeleteDialog.InvalidConnection.DialogTitle"));
      mb.open();
      return;
    }
    // Get the information for the dialog into the input structure.
    getInfo(input);

    dispose();
  }

  private void getSchemaNames() {
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(wConnection.getText(), variables);
    if (databaseMeta != null) {
      Database database = new Database(loggingObject, variables, databaseMeta);
      try {
        database.connect();
        String[] schemas = database.getSchemas();

        if (null != schemas && schemas.length > 0) {
          schemas = Const.sortStrings(schemas);
          EnterSelectionDialog dialog =
              new EnterSelectionDialog(
                  shell,
                  schemas,
                  BaseMessages.getString(
                      PKG, "DeleteDialog.AvailableSchemas.Title", wConnection.getText()),
                  BaseMessages.getString(
                      PKG, "DeleteDialog.AvailableSchemas.Message", wConnection.getText()));
          String d = dialog.open();
          if (d != null) {
            wSchema.setText(Const.NVL(d, ""));
            setTableFieldCombo();
          }

        } else {
          MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
          mb.setMessage(BaseMessages.getString(PKG, "DeleteDialog.NoSchema.Error"));
          mb.setText(BaseMessages.getString(PKG, "DeleteDialog.GetSchemas.Error"));
          mb.open();
        }
      } catch (Exception e) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
            BaseMessages.getString(PKG, "DeleteDialog.ErrorGettingSchemas"),
            e);
      } finally {
        database.disconnect();
      }
    }
  }

  private void getTableName() {
    String connectionName = wConnection.getText();
    if (StringUtils.isEmpty(connectionName)) {
      return;
    }
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connectionName, variables);
    if (databaseMeta != null) {
      logDebug(
          BaseMessages.getString(PKG, "DeleteDialog.Log.LookingAtConnection")
              + databaseMeta.toString());

      DatabaseExplorerDialog std =
          new DatabaseExplorerDialog(
              shell, SWT.NONE, variables, databaseMeta, pipelineMeta.getDatabases());
      std.setSelectedSchemaAndTable(wSchema.getText(), wTable.getText());
      if (std.open()) {
        wSchema.setText(Const.NVL(std.getSchemaName(), ""));
        wTable.setText(Const.NVL(std.getTableName(), ""));
        setTableFieldCombo();
      }
    } else {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG, "DeleteDialog.InvalidConnection.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "DeleteDialog.InvalidConnection.DialogTitle"));
      mb.open();
    }
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null && !r.isEmpty()) {
        ITableItemInsertListener listener =
            (tableItem, v) -> {
              tableItem.setText(2, "=");
              return true;
            };
        BaseTransformDialog.getFieldsFromPrevious(
            r, wKey, 1, new int[] {1, 3}, new int[] {}, -1, -1, listener);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "DeleteDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "DeleteDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }
}
