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

package org.apache.hop.pipeline.transforms.insertupdate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.Props;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.database.dialog.SqlEditor;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ComponentSelectionListener;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class InsertUpdateDialog extends BaseTransformDialog {
  private static final Class<?> PKG = InsertUpdateMeta.class;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TableView wKey;

  private TextVar wSchema;

  private TextVar wTable;

  private TableView wReturn;

  private TextVar wCommit;

  private Button wUpdateBypassed;

  private final InsertUpdateMeta input;

  /** List of ColumnInfo that should have the field names of the selected database table */
  private final List<ColumnInfo> tableFieldColumns = new ArrayList<>();

  /** List of ColumnInfo that should have the field names of the input fields */
  private final List<ColumnInfo> inputFieldColumns = new ArrayList<>();

  public InsertUpdateDialog(
      Shell parent,
      IVariables variables,
      InsertUpdateMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "InsertUpdateDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).sql(e -> create()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();

    changed = input.hasChanged();

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    wTabFolder.setLayoutData(
        new FormDataBuilder().left().top(wSpacer, margin).right().bottom(wOk, -margin).result());

    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    addGeneralTab(wTabFolder, lsMod);
    addKeysTab(wTabFolder, lsMod);
    addFieldsTab(wTabFolder, lsMod);
    wTabFolder.setSelection(0);

    getData();
    setInputFieldCombo();
    setTableFieldCombo();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void addGeneralTab(CTabFolder wTabFolder, ModifyListener lsMod) {

    Composite composite = new Composite(wTabFolder, SWT.NONE);
    composite.setLayout(props.createFormLayout());
    PropsUi.setLook(composite);

    CTabItem tabItem = new CTabItem(wTabFolder, SWT.NONE);
    tabItem.setFont(GuiResource.getInstance().getFontDefault());
    tabItem.setText(BaseMessages.getString(PKG, "InsertUpdateDialog.GeneralTab.Title"));
    tabItem.setControl(composite);

    ModifyListener lsTableMod =
        event -> {
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

    // Connection line
    wConnection = addConnectionLine(composite, null, input.getConnection(), lsMod);
    wConnection.addSelectionListener(lsSelection);

    // Schema line...
    Label wlSchema = new Label(composite, SWT.RIGHT);
    wlSchema.setText(BaseMessages.getString(PKG, "InsertUpdateDialog.TargetSchema.Label"));
    wlSchema.setLayoutData(
        new FormDataBuilder().left().right(middle, -margin).top(wConnection, margin).result());
    PropsUi.setLook(wlSchema);

    Button wbSchema = new Button(composite, SWT.PUSH | SWT.CENTER);
    wbSchema.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    wbSchema.setLayoutData(new FormDataBuilder().top(wConnection, margin).right().result());
    wbSchema.addListener(SWT.Selection, e -> getSchemaName());
    PropsUi.setLook(wbSchema);

    wSchema = new TextVar(variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wSchema.addModifyListener(lsTableMod);
    wSchema.setLayoutData(
        new FormDataBuilder()
            .left(middle, 0)
            .top(wConnection, margin)
            .right(wbSchema, -margin)
            .result());
    PropsUi.setLook(wSchema);

    // Table line...
    Label wlTable = new Label(composite, SWT.RIGHT);
    wlTable.setText(BaseMessages.getString(PKG, "InsertUpdateDialog.TargetTable.Label"));
    PropsUi.setLook(wlTable);
    wlTable.setLayoutData(
        new FormDataBuilder().left().right(middle, -margin).top(wbSchema, margin).result());

    Button wbTable = new Button(composite, SWT.PUSH | SWT.CENTER);
    wbTable.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    wbTable.setLayoutData(new FormDataBuilder().right().top(wbSchema, margin).result());
    wbTable.addListener(SWT.Selection, e -> getTableName());
    PropsUi.setLook(wbTable);

    wTable = new TextVar(variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTable.addModifyListener(lsTableMod);
    wTable.setLayoutData(
        new FormDataBuilder()
            .left(middle, 0)
            .top(wbSchema, margin)
            .right(wbTable, -margin)
            .result());
    PropsUi.setLook(wTable);

    // Commit line
    Label wlCommit = new Label(composite, SWT.RIGHT);
    wlCommit.setText(BaseMessages.getString(PKG, "InsertUpdateDialog.CommitSize.Label"));
    wlCommit.setLayoutData(
        new FormDataBuilder().left().top(wTable, margin).right(middle, -margin).result());
    PropsUi.setLook(wlCommit);

    wCommit = new TextVar(variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wCommit.addModifyListener(lsMod);
    wCommit.setLayoutData(
        new FormDataBuilder().left(middle, 0).top(wTable, margin).right().result());
    PropsUi.setLook(wCommit);

    // UpdateBypassed line
    Label wlUpdateBypassed = new Label(composite, SWT.RIGHT);
    wlUpdateBypassed.setText(
        BaseMessages.getString(PKG, "InsertUpdateDialog.UpdateBypassed.Label"));
    wlUpdateBypassed.setLayoutData(
        new FormDataBuilder().left().top(wCommit, margin).right(middle, -margin).result());
    PropsUi.setLook(wlUpdateBypassed);

    wUpdateBypassed = new Button(composite, SWT.CHECK);
    wUpdateBypassed.setLayoutData(
        new FormDataBuilder()
            .left(middle, 0)
            .top(wlUpdateBypassed, 0, SWT.CENTER)
            .right()
            .result());
    wUpdateBypassed.addSelectionListener(new ComponentSelectionListener(input));
    PropsUi.setLook(wUpdateBypassed);
  }

  private void addKeysTab(CTabFolder wTabFolder, ModifyListener lsMod) {

    Composite composite = new Composite(wTabFolder, SWT.NONE);
    composite.setLayout(props.createFormLayout());
    PropsUi.setLook(composite);

    CTabItem tabItem = new CTabItem(wTabFolder, SWT.NONE);
    tabItem.setFont(GuiResource.getInstance().getFontDefault());
    tabItem.setText(BaseMessages.getString(PKG, "InsertUpdateDialog.KeysTab.Title"));
    tabItem.setControl(composite);

    Label wlKey = new Label(composite, SWT.NONE);
    wlKey.setText(BaseMessages.getString(PKG, "InsertUpdateDialog.Keys.Label"));
    wlKey.setLayoutData(new FormDataBuilder().left().top().result());
    PropsUi.setLook(wlKey);

    int nrKeyCols = 4;
    int nrKeyRows =
        (input.getInsertUpdateLookupField().getLookupKeys() != null
            ? input.getInsertUpdateLookupField().getLookupKeys().size()
            : 1);

    ColumnInfo[] ciKey = new ColumnInfo[nrKeyCols];
    ciKey[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "InsertUpdateDialog.ColumnInfo.TableField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciKey[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "InsertUpdateDialog.ColumnInfo.Comparator"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {
              "=",
              "= ~NULL",
              "<>",
              "<",
              "<=",
              ">",
              ">=",
              "LIKE",
              "BETWEEN",
              "IS NULL",
              "IS NOT NULL"
            });
    ciKey[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "InsertUpdateDialog.ColumnInfo.StreamField1"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);

    ciKey[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "InsertUpdateDialog.ColumnInfo.StreamField2"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);

    tableFieldColumns.add(ciKey[0]);
    inputFieldColumns.add(ciKey[2]);
    inputFieldColumns.add(ciKey[3]);

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
    wGet.setText(BaseMessages.getString(PKG, "InsertUpdateDialog.GetFields.Button"));
    wGet.setLayoutData(new FormDataBuilder().right().top(wlKey, margin).result());
    wGet.addListener(SWT.Selection, e -> get());
    PropsUi.setLook(wGet);
    setButtonPositions(new Button[] {wGet}, margin, null);

    wKey.setLayoutData(
        FormDataBuilder.builder().top(wlKey, margin).bottom(wGet, -margin).fullWidth().result());
  }

  private void addFieldsTab(CTabFolder wTabFolder, ModifyListener lsMod) {

    Composite composite = new Composite(wTabFolder, SWT.NONE);
    composite.setLayout(props.createFormLayout());
    PropsUi.setLook(composite);

    CTabItem tabItem = new CTabItem(wTabFolder, SWT.NONE);
    tabItem.setFont(GuiResource.getInstance().getFontDefault());
    tabItem.setText(BaseMessages.getString(PKG, "InsertUpdateDialog.FieldsTab.Title"));
    tabItem.setControl(composite);

    Label wlReturn = new Label(composite, SWT.NONE);
    wlReturn.setText(BaseMessages.getString(PKG, "InsertUpdateDialog.UpdateFields.Label"));
    wlReturn.setLayoutData(new FormDataBuilder().left().top().result());
    PropsUi.setLook(wlReturn);

    int upInsCols = 3;
    int upInsRows =
        (input.getInsertUpdateLookupField().getValueFields() != null
            ? input.getInsertUpdateLookupField().getValueFields().size()
            : 1);

    ColumnInfo[] ciReturn = new ColumnInfo[upInsCols];
    ciReturn[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "InsertUpdateDialog.ColumnInfo.TableField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciReturn[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "InsertUpdateDialog.ColumnInfo.StreamField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciReturn[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "InsertUpdateDialog.ColumnInfo.Update"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {"Y", "N"});

    tableFieldColumns.add(ciReturn[0]);
    inputFieldColumns.add(ciReturn[1]);

    wReturn =
        new TableView(
            variables,
            composite,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciReturn,
            upInsRows,
            lsMod,
            props);

    Button wGetFields = new Button(composite, SWT.PUSH);
    wGetFields.setText(BaseMessages.getString(PKG, "InsertUpdateDialog.GetAndUpdateFields.Label"));
    wGetFields.addListener(SWT.Selection, event -> getUpdate());
    PropsUi.setLook(wGetFields);

    Button wDoMapping = new Button(composite, SWT.PUSH);
    wDoMapping.setText(BaseMessages.getString(PKG, "InsertUpdateDialog.EditMapping.Label"));
    wDoMapping.addListener(SWT.Selection, event -> generateMappings());
    PropsUi.setLook(wDoMapping);

    setButtonPositions(new Button[] {wGetFields, wDoMapping}, margin, null);

    wReturn.setLayoutData(
        FormDataBuilder.builder()
            .top(wlReturn, margin)
            .bottom(wGetFields, -margin)
            .fullWidth()
            .result());
  }

  /** Search the fields in the background */
  protected void setInputFieldCombo() {
    shell
        .getDisplay()
        .asyncExec(
            () -> {
              TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
              if (transformMeta != null) {
                try {
                  IRowMeta rowMeta = pipelineMeta.getPrevTransformFields(variables, transformMeta);
                  String[] fieldNames = Const.sortStrings(rowMeta.getFieldNames());
                  for (ColumnInfo colInfo : inputFieldColumns) {
                    colInfo.setComboValues(fieldNames);
                  }
                } catch (HopException e) {
                  logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
                }
              }
            });
  }

  /**
   * Reads in the fields from the previous transforms and from the ONE next transform and opens an
   * EnterMappingDialog with this information. After the user did the mapping, those information is
   * put into the Select/Rename table.
   */
  private void generateMappings() {

    // Determine the source and target fields...
    //
    IRowMeta sourceFields;
    IRowMeta targetFields;

    try {
      sourceFields = pipelineMeta.getPrevTransformFields(variables, transformMeta);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(
              PKG, "InsertUpdateDialog.DoMapping.UnableToFindSourceFields.Title"),
          BaseMessages.getString(
              PKG, "InsertUpdateDialog.DoMapping.UnableToFindSourceFields.Message"),
          e);
      return;
    }
    // refresh data
    input.setConnection(wConnection.getText());
    input.getInsertUpdateLookupField().setTableName(variables.resolve(wTable.getText()));
    ITransformMeta transformMetaInterface = transformMeta.getTransform();
    try {
      targetFields = transformMetaInterface.getRequiredFields(variables);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(
              PKG, "InsertUpdateDialog.DoMapping.UnableToFindTargetFields.Title"),
          BaseMessages.getString(
              PKG, "InsertUpdateDialog.DoMapping.UnableToFindTargetFields.Message"),
          e);
      return;
    }

    String[] inputNames = new String[sourceFields.size()];
    for (int i = 0; i < sourceFields.size(); i++) {
      IValueMeta value = sourceFields.getValueMeta(i);
      inputNames[i] = value.getName();
    }

    // Create the existing mapping list...
    // Also copy the update status of targets in to a hashmap
    //
    List<SourceToTargetMapping> mappings = new ArrayList<>();
    Map<String, String> targetUpdateStatus = new HashMap<>();
    StringBuilder missingSourceFields = new StringBuilder();
    StringBuilder missingTargetFields = new StringBuilder();

    int nrFields = wReturn.nrNonEmpty();
    for (int i = 0; i < nrFields; i++) {
      TableItem item = wReturn.getNonEmpty(i);
      String source = item.getText(2);
      String target = item.getText(1);
      targetUpdateStatus.put(item.getText(1), item.getText(3));
      int sourceIndex = sourceFields.indexOfValue(source);
      if (sourceIndex < 0) {
        missingSourceFields
            .append(Const.CR)
            .append("   ")
            .append(source)
            .append(" --> ")
            .append(target);
      }
      int targetIndex = targetFields.indexOfValue(target);
      if (targetIndex < 0) {
        missingTargetFields
            .append(Const.CR)
            .append("   ")
            .append(source)
            .append(" --> ")
            .append(target);
      }
      if (sourceIndex < 0 || targetIndex < 0) {
        continue;
      }

      SourceToTargetMapping mapping = new SourceToTargetMapping(sourceIndex, targetIndex);
      mappings.add(mapping);
    }

    // show a confirm dialog if some missing field was found
    //
    if (!missingSourceFields.isEmpty() || !missingTargetFields.isEmpty()) {

      String message = "";
      if (!missingSourceFields.isEmpty()) {
        message +=
            BaseMessages.getString(
                    PKG,
                    "InsertUpdateDialog.DoMapping.SomeSourceFieldsNotFound",
                    missingSourceFields.toString())
                + Const.CR;
      }
      if (!missingTargetFields.isEmpty()) {
        message +=
            BaseMessages.getString(
                    PKG,
                    "InsertUpdateDialog.DoMapping.SomeTargetFieldsNotFound",
                    missingSourceFields.toString())
                + Const.CR;
      }
      message += Const.CR;
      message +=
          BaseMessages.getString(PKG, "InsertUpdateDialog.DoMapping.SomeFieldsNotFoundContinue")
              + Const.CR;
      int answer =
          BaseDialog.openMessageBox(
              shell,
              BaseMessages.getString(PKG, "InsertUpdateDialog.DoMapping.SomeFieldsNotFoundTitle"),
              message,
              SWT.ICON_QUESTION | SWT.OK | SWT.CANCEL);
      boolean goOn = (answer & SWT.OK) != 0;
      if (!goOn) {
        return;
      }
    }
    EnterMappingDialog d =
        new EnterMappingDialog(
            InsertUpdateDialog.this.shell,
            sourceFields.getFieldNames(),
            targetFields.getFieldNames(),
            mappings);
    mappings = d.open();

    // mappings == null if the user pressed cancel
    //
    if (mappings != null) {
      // Clear and re-populate!
      //
      wReturn.table.removeAll();
      wReturn.table.setItemCount(mappings.size());
      for (int i = 0; i < mappings.size(); i++) {
        SourceToTargetMapping mapping = mappings.get(i);
        TableItem item = wReturn.table.getItem(i);
        item.setText(2, sourceFields.getValueMeta(mapping.getSourcePosition()).getName());
        item.setText(1, targetFields.getValueMeta(mapping.getTargetPosition()).getName());
        if (targetUpdateStatus.get(item.getText(1)) == null) {
          item.setText(3, "Y");
        } else {
          item.setText(3, targetUpdateStatus.get(item.getText(1)));
        }
      }
      wReturn.setRowNums();
      wReturn.optWidth(true);
    }
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "InsertUpdateDialog.Log.GettingKeyInfo"));
    }

    wCommit.setText(input.getCommitSize());
    wUpdateBypassed.setSelection(input.isUpdateBypassed());

    if (input.getInsertUpdateLookupField().getLookupKeys() != null) {
      for (int i = 0; i < input.getInsertUpdateLookupField().getLookupKeys().size(); i++) {
        InsertUpdateKeyField keyField = input.getInsertUpdateLookupField().getLookupKeys().get(i);

        TableItem item = wKey.table.getItem(i);
        if (keyField.getKeyLookup() != null) {
          item.setText(1, keyField.getKeyLookup());
        }
        if (keyField.getKeyCondition() != null) {
          item.setText(2, keyField.getKeyCondition());
        }
        if (keyField.getKeyStream() != null) {
          item.setText(3, keyField.getKeyStream());
        }
        if (keyField.getKeyStream2() != null) {
          item.setText(4, keyField.getKeyStream2());
        }
      }
    }

    if (input.getInsertUpdateLookupField().getValueFields() != null) {
      for (int i = 0; i < input.getInsertUpdateLookupField().getValueFields().size(); i++) {
        InsertUpdateValue valueField = input.getInsertUpdateLookupField().getValueFields().get(i);

        TableItem item = wReturn.table.getItem(i);
        if (valueField.getUpdateLookup() != null) {
          item.setText(1, valueField.getUpdateLookup());
        }
        if (valueField.getUpdateStream() != null) {
          item.setText(2, valueField.getUpdateStream());
        }
        item.setText(3, valueField.isUpdate() ? "Y" : "N");
      }
    }

    if (input.getSchemaName() != null) {
      wSchema.setText(input.getSchemaName());
    }
    if (input.getTableName() != null) {
      wTable.setText(input.getTableName());
    }
    if (input.getConnection() != null) {
      wConnection.setText(input.getConnection());
    }

    wKey.setRowNums();
    wKey.optWidth(true);
    wReturn.setRowNums();
    wReturn.optWidth(true);
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void getInfo(InsertUpdateMeta inf) {
    int nrkeys = wKey.nrNonEmpty();
    int nrFields = wReturn.nrNonEmpty();

    inf.setCommitSize(wCommit.getText());
    inf.setUpdateBypassed(wUpdateBypassed.getSelection());

    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "InsertUpdateDialog.Log.FoundKeys", nrkeys + ""));
    }

    inf.getInsertUpdateLookupField().getLookupKeys().clear();

    for (int i = 0; i < nrkeys; i++) {
      TableItem item = wKey.getNonEmpty(i);
      InsertUpdateKeyField keyField =
          new InsertUpdateKeyField(
              item.getText(3) // KeyStream
              ,
              item.getText(1) // KeyLookup
              ,
              item.getText(2) // KeyCondition
              ,
              item.getText(4)); // KeyStream2
      inf.getInsertUpdateLookupField().getLookupKeys().add(keyField);
    }

    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "InsertUpdateDialog.Log.FoundFields", nrFields + ""));
    }

    inf.getInsertUpdateLookupField().getValueFields().clear();
    for (int i = 0; i < nrFields; i++) {
      TableItem item = wReturn.getNonEmpty(i);
      InsertUpdateValue valueField =
          new InsertUpdateValue(
              item.getText(1) // UpdateLookup
              ,
              item.getText(2) // UpdateStream
              ,
              !"N".equals(item.getText(3))); // DoUpdate

      inf.getInsertUpdateLookupField().getValueFields().add(valueField);
    }

    inf.getInsertUpdateLookupField().setSchemaName(wSchema.getText());
    inf.getInsertUpdateLookupField().setTableName(wTable.getText());
    inf.setConnection(wConnection.getText());

    transformName = wTransformName.getText(); // return value
  }

  private void setTableFieldCombo() {
    shell
        .getDisplay()
        .asyncExec(
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
                    try (Database database = new Database(loggingObject, variables, databaseMeta)) {
                      database.connect();

                      IRowMeta rowMeta =
                          database.getTableFieldsMeta(
                              variables.resolve(schemaName), variables.resolve(tableName));
                      if (null != rowMeta) {
                        String[] fieldNames = Const.sortStrings(rowMeta.getFieldNames());
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
                    }
                  }
                }
              }
            });
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    if (Utils.isEmpty(wConnection.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "InsertUpdateDialog.InvalidConnection.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "InsertUpdateDialog.InvalidConnection.DialogTitle"));
      mb.open();
    }

    // Get the information for the dialog into the input structure.
    getInfo(input);
    dispose();
  }

  private void getSchemaName() {
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
                      PKG, "InsertUpDateDialog.AvailableSchemas.Title", wConnection.getText()),
                  BaseMessages.getString(
                      PKG, "InsertUpDateDialog.AvailableSchemas.Message", wConnection.getText()));
          String d = dialog.open();
          if (d != null) {
            wSchema.setText(Const.NVL(d, ""));
            setTableFieldCombo();
          }

        } else {
          MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
          mb.setMessage(BaseMessages.getString(PKG, "InsertUpDateDialog.NoSchema.Error"));
          mb.setText(BaseMessages.getString(PKG, "InsertUpDateDialog.GetSchemas.Error"));
          mb.open();
        }
      } catch (Exception e) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
            BaseMessages.getString(PKG, "InsertUpDateDialog.ErrorGettingSchemas"),
            e);
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
      if (log.isDebug()) {
        logDebug(
            BaseMessages.getString(PKG, "InsertUpdateDialog.Log.LookingAtConnection")
                + databaseMeta.toString());
      }

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
      mb.setMessage(
          BaseMessages.getString(PKG, "InsertUpdateDialog.InvalidConnection.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "InsertUpdateDialog.InvalidConnection.DialogTitle"));
      mb.open();
    }
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
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
          BaseMessages.getString(PKG, "InsertUpdateDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "InsertUpdateDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  private void getUpdate() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        ITableItemInsertListener listener =
            (tableItem, v) -> {
              tableItem.setText(3, "Y");
              return true;
            };
        BaseTransformDialog.getFieldsFromPrevious(
            r, wReturn, 1, new int[] {1, 2}, new int[] {}, -1, -1, listener);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "InsertUpdateDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "InsertUpdateDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  // Generate code for create table...
  // Conversions done by Database
  private void create() {
    try {
      InsertUpdateMeta info = new InsertUpdateMeta();
      getInfo(info);
      DatabaseMeta databaseMeta = pipelineMeta.findDatabase(info.getConnection(), variables);
      String name = transformName; // new name might not yet be linked to other transforms!
      TransformMeta transformMeta =
          new TransformMeta(
              BaseMessages.getString(PKG, "InsertUpdateDialog.TransformMeta.Title"), name, info);
      IRowMeta prev = pipelineMeta.getPrevTransformFields(variables, transformName);

      SqlStatement sql =
          info.getSqlStatements(variables, pipelineMeta, transformMeta, prev, metadataProvider);
      if (!sql.hasError()) {
        if (sql.hasSql()) {
          SqlEditor sqledit =
              new SqlEditor(
                  shell, SWT.NONE, variables, databaseMeta, DbCache.getInstance(), sql.getSql());
          sqledit.open();
        } else {
          MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
          mb.setMessage(BaseMessages.getString(PKG, "InsertUpdateDialog.NoSQLNeeds.DialogMessage"));
          mb.setText(BaseMessages.getString(PKG, "InsertUpdateDialog.NoSQLNeeds.DialogTitle"));
          mb.open();
        }
      } else {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(sql.getError());
        mb.setText(BaseMessages.getString(PKG, "InsertUpdateDialog.SQLError.DialogTitle"));
        mb.open();
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "InsertUpdateDialog.CouldNotBuildSQL.DialogTitle"),
          BaseMessages.getString(PKG, "InsertUpdateDialog.CouldNotBuildSQL.DialogMessage"),
          ke);
    }
  }
}
