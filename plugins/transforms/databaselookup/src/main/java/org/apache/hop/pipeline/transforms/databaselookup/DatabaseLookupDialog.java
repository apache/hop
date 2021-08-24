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

package org.apache.hop.pipeline.transforms.databaselookup;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DatabaseLookupDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = DatabaseLookupMeta.class; // For Translator

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private Button wCache;

  private Label wlCacheLoadAll;
  private Button wCacheLoadAll;

  private Label wlCachesize;
  private Text wCachesize;

  private TableView wKey;

  private TextVar wSchema;

  private TextVar wTable;

  private TableView wReturn;

  private Label wlOrderBy;
  private Text wOrderBy;

  private Label wlFailMultiple;
  private Button wFailMultiple;

  private Button wEatRows;

  private final DatabaseLookupMeta input;

  /** List of ColumnInfo that should have the field names of the selected database table */
  private final List<ColumnInfo> tableFieldColumns = new ArrayList<>();

  /** List of ColumnInfo that should have the previous fields combo box */
  private final List<ColumnInfo> fieldColumns = new ArrayList<>();

  /** all fields from the previous transforms */
  private IRowMeta prevFields = null;

  public DatabaseLookupDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (DatabaseLookupMeta) in;
  }

  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();

    ModifyListener lsTableMod =
        arg0 -> {
          input.setChanged();
          setTableFieldCombo();
        };
    SelectionListener lsSelection =
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            setTableFieldCombo();
          }
        };
    backupChanged = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.shell.Title"));

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(
        BaseMessages.getString(PKG, "DatabaseLookupDialog.TransformName.Label"));
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    // Connection line
    wConnection = addConnectionLine(shell, wTransformName, input.getDatabaseMeta(), lsMod);
    wConnection.addSelectionListener(lsSelection);

    // Schema line...
    Label wlSchema = new Label(shell, SWT.RIGHT);
    wlSchema.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.TargetSchema.Label"));
    props.setLook(wlSchema);
    FormData fdlSchema = new FormData();
    fdlSchema.left = new FormAttachment(0, 0);
    fdlSchema.right = new FormAttachment(middle, -margin);
    fdlSchema.top = new FormAttachment(wConnection, margin * 2);
    wlSchema.setLayoutData(fdlSchema);

    Button wbSchema = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbSchema);
    wbSchema.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbSchema = new FormData();
    fdbSchema.top = new FormAttachment(wConnection, 2 * margin);
    fdbSchema.right = new FormAttachment(100, 0);
    wbSchema.setLayoutData(fdbSchema);

    wSchema = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wSchema);
    wSchema.addModifyListener(lsTableMod);
    FormData fdSchema = new FormData();
    fdSchema.left = new FormAttachment(middle, 0);
    fdSchema.top = new FormAttachment(wConnection, margin * 2);
    fdSchema.right = new FormAttachment(wbSchema, -margin);
    wSchema.setLayoutData(fdSchema);

    // Table line...
    Label wlTable = new Label(shell, SWT.RIGHT);
    wlTable.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.Lookuptable.Label"));
    props.setLook(wlTable);
    FormData fdlTable = new FormData();
    fdlTable.left = new FormAttachment(0, 0);
    fdlTable.right = new FormAttachment(middle, -margin);
    fdlTable.top = new FormAttachment(wbSchema, margin);
    wlTable.setLayoutData(fdlTable);

    Button wbTable = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbTable);
    wbTable.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.Browse.Button"));
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment(100, 0);
    fdbTable.top = new FormAttachment(wbSchema, margin);
    wbTable.setLayoutData(fdbTable);

    wTable = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTable);
    wTable.addModifyListener(lsTableMod);
    FormData fdTable = new FormData();
    fdTable.left = new FormAttachment(middle, 0);
    fdTable.top = new FormAttachment(wbSchema, margin);
    fdTable.right = new FormAttachment(wbTable, -margin);
    wTable.setLayoutData(fdTable);

    // ICache?
    Label wlCache = new Label(shell, SWT.RIGHT);
    wlCache.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.Cache.Label"));
    props.setLook(wlCache);
    FormData fdlCache = new FormData();
    fdlCache.left = new FormAttachment(0, 0);
    fdlCache.right = new FormAttachment(middle, -margin);
    fdlCache.top = new FormAttachment(wTable, margin);
    wlCache.setLayoutData(fdlCache);
    wCache = new Button(shell, SWT.CHECK);
    props.setLook(wCache);
    FormData fdCache = new FormData();
    fdCache.left = new FormAttachment(middle, 0);
    fdCache.top = new FormAttachment(wlCache, 0, SWT.CENTER);
    wCache.setLayoutData(fdCache);
    wCache.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            enableFields();
          }
        });

    // ICache size line
    wlCachesize = new Label(shell, SWT.RIGHT);
    wlCachesize.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.Cachesize.Label"));
    props.setLook(wlCachesize);
    wlCachesize.setEnabled(input.isCached());
    FormData fdlCachesize = new FormData();
    fdlCachesize.left = new FormAttachment(0, 0);
    fdlCachesize.right = new FormAttachment(middle, -margin);
    fdlCachesize.top = new FormAttachment(wCache, margin);
    wlCachesize.setLayoutData(fdlCachesize);
    wCachesize = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wCachesize);
    wCachesize.setEnabled(input.isCached());
    wCachesize.addModifyListener(lsMod);
    FormData fdCachesize = new FormData();
    fdCachesize.left = new FormAttachment(middle, 0);
    fdCachesize.right = new FormAttachment(100, 0);
    fdCachesize.top = new FormAttachment(wCache, margin);
    wCachesize.setLayoutData(fdCachesize);

    // ICache : Load all?
    wlCacheLoadAll = new Label(shell, SWT.RIGHT);
    wlCacheLoadAll.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.CacheLoadAll.Label"));
    props.setLook(wlCacheLoadAll);
    FormData fdlCacheLoadAll = new FormData();
    fdlCacheLoadAll.left = new FormAttachment(0, 0);
    fdlCacheLoadAll.right = new FormAttachment(middle, -margin);
    fdlCacheLoadAll.top = new FormAttachment(wCachesize, margin);
    wlCacheLoadAll.setLayoutData(fdlCacheLoadAll);
    wCacheLoadAll = new Button(shell, SWT.CHECK);
    props.setLook(wCacheLoadAll);
    FormData fdCacheLoadAll = new FormData();
    fdCacheLoadAll.left = new FormAttachment(middle, 0);
    fdCacheLoadAll.top = new FormAttachment(wlCacheLoadAll, 0, SWT.CENTER);
    wCacheLoadAll.setLayoutData(fdCacheLoadAll);
    wCacheLoadAll.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            enableFields();
          }
        });

    Label wlKey = new Label(shell, SWT.NONE);
    wlKey.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.Keys.Label"));
    props.setLook(wlKey);
    FormData fdlKey = new FormData();
    fdlKey.left = new FormAttachment(0, 0);
    fdlKey.top = new FormAttachment(wCacheLoadAll, margin);
    wlKey.setLayoutData(fdlKey);

    int nrKeyCols = 4;
    int nrKeyRows =
        input.getLookup().getKeyFields().isEmpty() ? 1 : input.getLookup().getKeyFields().size();

    ColumnInfo[] ciKey = new ColumnInfo[nrKeyCols];
    ciKey[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseLookupDialog.ColumnInfo.Tablefield"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciKey[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseLookupDialog.ColumnInfo.Comparator"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            DatabaseLookupMeta.conditionStrings);
    ciKey[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseLookupDialog.ColumnInfo.Field1"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciKey[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseLookupDialog.ColumnInfo.Field2"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    tableFieldColumns.add(ciKey[0]);
    fieldColumns.add(ciKey[2]);
    fieldColumns.add(ciKey[3]);
    wKey =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciKey,
            nrKeyRows,
            lsMod,
            props);

    FormData fdKey = new FormData();
    fdKey.left = new FormAttachment(0, 0);
    fdKey.top = new FormAttachment(wlKey, margin);
    fdKey.right = new FormAttachment(100, 0);
    fdKey.bottom = new FormAttachment(wlKey, (int) (200 * props.getZoomFactor()));
    wKey.setLayoutData(fdKey);

    // THE UPDATE/INSERT TABLE
    Label wlReturn = new Label(shell, SWT.NONE);
    wlReturn.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.Return.Label"));
    props.setLook(wlReturn);
    FormData fdlReturn = new FormData();
    fdlReturn.left = new FormAttachment(0, 0);
    fdlReturn.top = new FormAttachment(wKey, margin);
    wlReturn.setLayoutData(fdlReturn);

    int UpInsCols = 5;
    int UpInsRows =
        input.getLookup().getReturnValues().isEmpty()
            ? 1
            : input.getLookup().getReturnValues().size();

    ColumnInfo[] ciReturn = new ColumnInfo[UpInsCols];

    ciReturn[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseLookupDialog.ColumnInfo.Field"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {},
            false);

    ciReturn[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseLookupDialog.ColumnInfo.Newname"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);

    ciReturn[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseLookupDialog.ColumnInfo.Default"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);

    ciReturn[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseLookupDialog.ColumnInfo.Type"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            ValueMetaFactory.getValueMetaNames());

    ciReturn[4] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseLookupDialog.TrimTypeColumn.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            ValueMetaString.trimTypeDesc,
            true);

    tableFieldColumns.add(ciReturn[0]);

    wReturn =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciReturn,
            UpInsRows,
            lsMod,
            props);

    FormData fdReturn = new FormData();
    fdReturn.left = new FormAttachment(0, 0);
    fdReturn.top = new FormAttachment(wlReturn, margin);
    fdReturn.right = new FormAttachment(100, 0);
    fdReturn.bottom = new FormAttachment(wlReturn, (int) (200 * props.getZoomFactor()));
    wReturn.setLayoutData(fdReturn);

    // EatRows?
    Label wlEatRows = new Label(shell, SWT.RIGHT);
    wlEatRows.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.EatRows.Label"));
    props.setLook(wlEatRows);
    FormData fdlEatRows = new FormData();
    fdlEatRows.left = new FormAttachment(0, 0);
    fdlEatRows.top = new FormAttachment(wReturn, margin);
    fdlEatRows.right = new FormAttachment(middle, -margin);
    wlEatRows.setLayoutData(fdlEatRows);
    wEatRows = new Button(shell, SWT.CHECK);
    props.setLook(wEatRows);
    FormData fdEatRows = new FormData();
    fdEatRows.left = new FormAttachment(middle, 0);
    fdEatRows.top = new FormAttachment(wlEatRows, 0, SWT.CENTER);
    wEatRows.setLayoutData(fdEatRows);
    wEatRows.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            enableFields();
          }
        });

    // FailMultiple?
    wlFailMultiple = new Label(shell, SWT.RIGHT);
    wlFailMultiple.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.FailMultiple.Label"));
    props.setLook(wlFailMultiple);
    FormData fdlFailMultiple = new FormData();
    fdlFailMultiple.left = new FormAttachment(0, 0);
    fdlFailMultiple.top = new FormAttachment(wEatRows, margin);
    fdlFailMultiple.right = new FormAttachment(middle, -margin);
    wlFailMultiple.setLayoutData(fdlFailMultiple);
    wFailMultiple = new Button(shell, SWT.CHECK);
    props.setLook(wFailMultiple);
    FormData fdFailMultiple = new FormData();
    fdFailMultiple.left = new FormAttachment(middle, 0);
    fdFailMultiple.top = new FormAttachment(wlFailMultiple, 0, SWT.CENTER);
    wFailMultiple.setLayoutData(fdFailMultiple);
    wFailMultiple.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            enableFields();
          }
        });

    // OderBy line
    wlOrderBy = new Label(shell, SWT.RIGHT);
    wlOrderBy.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.Orderby.Label"));
    props.setLook(wlOrderBy);
    FormData fdlOrderBy = new FormData();
    fdlOrderBy.left = new FormAttachment(0, 0);
    fdlOrderBy.top = new FormAttachment(wFailMultiple, margin);
    fdlOrderBy.right = new FormAttachment(middle, -margin);
    wlOrderBy.setLayoutData(fdlOrderBy);
    wOrderBy = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wOrderBy);
    FormData fdOrderBy = new FormData();
    fdOrderBy.left = new FormAttachment(middle, 0);
    fdOrderBy.top = new FormAttachment(wFailMultiple, margin);
    fdOrderBy.right = new FormAttachment(100, 0);
    wOrderBy.setLayoutData(fdOrderBy);
    wOrderBy.addModifyListener(lsMod);

    // THE BUTTONS
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    Button wGet = new Button(shell, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.GetFields.Button"));
    Button wGetLU = new Button(shell, SWT.PUSH);
    wGetLU.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.GetLookupFields.Button"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    setButtonPositions(new Button[] {wOk, wGet, wGetLU, wCancel}, margin, wOrderBy);

    // Add listeners
    wOk.addListener(SWT.Selection, e -> ok());
    wGet.addListener(SWT.Selection, e -> get());
    wGetLU.addListener(SWT.Selection, e -> getlookup());
    wCancel.addListener(SWT.Selection, e -> cancel());

    wbSchema.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            getSchemaNames();
          }
        });
    wbTable.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            getTableName();
          }
        });

    getData();

    setComboValues();
    setTableFieldCombo();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void setComboValues() {
    Runnable fieldLoader =
        () -> {
          try {
            prevFields = pipelineMeta.getPrevTransformFields(variables, transformName);
          } catch (HopException e) {
            prevFields = new RowMeta();
            String msg =
                BaseMessages.getString(PKG, "DatabaseLookupDialog.DoMapping.UnableToFindInput");
            logError(msg);
          }
          String[] prevTransformFieldNames = prevFields.getFieldNames();
          Arrays.sort(prevTransformFieldNames);
          for (ColumnInfo colInfo : fieldColumns) {
            colInfo.setComboValues(prevTransformFieldNames);
          }
        };
    new Thread(fieldLoader).start();
  }

  private void setTableFieldCombo() {
    Runnable fieldLoader =
        () -> {
          if (!wTable.isDisposed() && !wConnection.isDisposed() && !wSchema.isDisposed()) {
            final String tableName = wTable.getText(),
                connectionName = wConnection.getText(),
                schemaName = wSchema.getText();
            if (!Utils.isEmpty(tableName)) {
              DatabaseMeta ci = pipelineMeta.findDatabase(connectionName);
              if (ci != null) {
                Database db = new Database(loggingObject, variables, ci);
                try {
                  db.connect();

                  // IRowMeta r = db.getTableFieldsMeta( schemaName, tableName );
                  String schemaTable =
                      ci.getQuotedSchemaTableCombination(variables, schemaName, tableName);
                  IRowMeta r = db.getTableFields(schemaTable);

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
                    if (db != null) {
                      db.disconnect();
                    }
                  } catch (Exception ignored) {
                    // ignore any errors here.
                    db = null;
                  }
                }
              }
            }
          }
        };
    shell.getDisplay().asyncExec(fieldLoader);
  }

  private void enableFields() {
    wlOrderBy.setEnabled(!wFailMultiple.getSelection());
    wOrderBy.setEnabled(!wFailMultiple.getSelection());

    wCachesize.setEnabled(wCache.getSelection() && !wCacheLoadAll.getSelection());
    wlCachesize.setEnabled(wCache.getSelection() && !wCacheLoadAll.getSelection());
    wCacheLoadAll.setEnabled(wCache.getSelection());
    wlCacheLoadAll.setEnabled(wCache.getSelection());
    wFailMultiple.setEnabled(!wCache.getSelection());
    wlFailMultiple.setEnabled(!wCache.getSelection());
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    logDebug(BaseMessages.getString(PKG, "DatabaseLookupDialog.Log.GettingKeyInfo"));

    wCache.setSelection(input.isCached());
    wCachesize.setText("" + input.getCacheSize());
    wCacheLoadAll.setSelection(input.isLoadingAllDataInCache());

    Lookup lookup = input.getLookup();

    for (int i = 0; i < lookup.getKeyFields().size(); i++) {
      KeyField keyField = lookup.getKeyFields().get(i);

      TableItem item = wKey.table.getItem(i);
      item.setText(1, Const.NVL(keyField.getTableField(), ""));
      item.setText(2, Const.NVL(keyField.getCondition(), ""));
      item.setText(3, Const.NVL(keyField.getStreamField1(), ""));
      item.setText(4, Const.NVL(keyField.getStreamField2(), ""));
    }
    for (int i = 0; i < lookup.getReturnValues().size(); i++) {
      ReturnValue returnValue = lookup.getReturnValues().get(i);

      TableItem item = wReturn.table.getItem(i);
      item.setText(1, Const.NVL(returnValue.getTableField(), ""));
      item.setText(2, Const.NVL(returnValue.getNewName(), ""));
      item.setText(3, Const.NVL(returnValue.getDefaultValue(), ""));
      item.setText(4, Const.NVL(returnValue.getDefaultType(), ""));
      item.setText(
          5,
          Const.NVL(
              ValueMetaString.getTrimTypeDesc(
                  ValueMetaString.getTrimTypeByCode(returnValue.getTrimType())),
              ValueMetaBase.trimTypeCode[0]));
    }
    wSchema.setText(Const.NVL(input.getSchemaName(), ""));
    wTable.setText(Const.NVL(input.getTableName(), ""));
    if (input.getDatabaseMeta() != null) {
      wConnection.setText(input.getDatabaseMeta().getName());
    }
    wOrderBy.setText(Const.NVL(lookup.getOrderByClause(), ""));
    wFailMultiple.setSelection(lookup.isFailingOnMultipleResults());
    wEatRows.setSelection(lookup.isEatingRowOnLookupFailure());

    wKey.optimizeTableView();
    wReturn.optimizeTableView();

    enableFields();

    wTransformName.selectAll();
    wTransformName.setFocus();
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

    Lookup lookup = input.getLookup();
    lookup.getKeyFields().clear();
    lookup.getReturnValues().clear();

    input.setCached(wCache.getSelection());
    input.setCacheSize(Const.toInt(wCachesize.getText(), 0));
    input.setLoadingAllDataInCache(wCacheLoadAll.getSelection());

    for (TableItem item : wKey.getNonEmptyItems()) {
      KeyField keyField = new KeyField();
      keyField.setTableField(item.getText(1));
      keyField.setCondition(item.getText(2));
      keyField.setStreamField1(item.getText(3));
      keyField.setStreamField2(item.getText(4));
      lookup.getKeyFields().add(keyField);
    }

    for (TableItem item : wReturn.getNonEmptyItems()) {
      ReturnValue returnValue = new ReturnValue();
      returnValue.setTableField(item.getText(1));
      returnValue.setNewName(item.getText(2));
      returnValue.setDefaultValue(item.getText(3));
      returnValue.setDefaultType(item.getText(4));
      returnValue.setTrimType(
          ValueMetaString.getTrimTypeCode(ValueMetaString.getTrimTypeByDesc(item.getText(5))));
      lookup.getReturnValues().add(returnValue);
    }

    lookup.setSchemaName(wSchema.getText());
    lookup.setTableName(wTable.getText());
    input.setDatabaseMeta(pipelineMeta.findDatabase(wConnection.getText()));
    lookup.setOrderByClause(wOrderBy.getText());
    lookup.setFailingOnMultipleResults(wFailMultiple.getSelection());
    lookup.setEatingRowOnLookupFailure(wEatRows.getSelection());

    transformName = wTransformName.getText(); // return value

    if (pipelineMeta.findDatabase(wConnection.getText()) == null) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "DatabaseLookupDialog.InvalidConnection.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.InvalidConnection.DialogTitle"));
      mb.open();
    }

    dispose();
  }

  private void getTableName() {
    String connectionName = wConnection.getText();
    if (StringUtils.isEmpty(connectionName)) {
      return;
    }
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connectionName);
    if (databaseMeta != null) {
      if (log.isDebug()) {
        logDebug(
            BaseMessages.getString(PKG, "DatabaseLookupDialog.Log.LookingAtConnection")
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
          BaseMessages.getString(PKG, "DatabaseLookupDialog.InvalidConnection.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.InvalidConnection.DialogTitle"));
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
          BaseMessages.getString(PKG, "DatabaseLookupDialog.GetFieldsFailed.DialogTitle"),
          BaseMessages.getString(PKG, "DatabaseLookupDialog.GetFieldsFailed.DialogMessage"),
          ke);
    }
  }

  private void getlookup() {
    DatabaseMeta ci = pipelineMeta.findDatabase(wConnection.getText());
    if (ci != null) {
      Database db = new Database(loggingObject, variables, ci);
      try {
        db.connect();

        if (!Utils.isEmpty(wTable.getText())) {
          String schemaTable =
              ci.getQuotedSchemaTableCombination(variables, wSchema.getText(), wTable.getText());
          IRowMeta r = db.getTableFields(schemaTable);

          if (r != null && !r.isEmpty()) {
            logDebug(
                BaseMessages.getString(PKG, "DatabaseLookupDialog.Log.FoundTableFields")
                    + schemaTable
                    + " --> "
                    + r.toStringMeta());
            BaseTransformDialog.getFieldsFromPrevious(
                r, wReturn, 1, new int[] {1, 2}, new int[] {4}, -1, -1, null);
          } else {
            MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
            mb.setMessage(
                BaseMessages.getString(
                    PKG, "DatabaseLookupDialog.CouldNotReadTableInfo.DialogMessage"));
            mb.setText(
                BaseMessages.getString(
                    PKG, "DatabaseLookupDialog.CouldNotReadTableInfo.DialogTitle"));
            mb.open();
          }
        }
      } catch (HopException e) {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(
            BaseMessages.getString(PKG, "DatabaseLookupDialog.ErrorOccurred.DialogMessage")
                + Const.CR
                + e.getMessage());
        mb.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.ErrorOccurred.DialogTitle"));
        mb.open();
      }
    } else {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "DatabaseLookupDialog.InvalidConnectionName.DialogMessage"));
      mb.setText(
          BaseMessages.getString(PKG, "DatabaseLookupDialog.InvalidConnectionName.DialogTitle"));
      mb.open();
    }
  }

  private void getSchemaNames() {
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(wConnection.getText());
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
                      PKG, "DatabaseLookupDialog.AvailableSchemas.Title", wConnection.getText()),
                  BaseMessages.getString(
                      PKG, "DatabaseLookupDialog.AvailableSchemas.Message", wConnection.getText()));
          String d = dialog.open();
          if (d != null) {
            wSchema.setText(Const.NVL(d, ""));
            setTableFieldCombo();
          }

        } else {
          MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
          mb.setMessage(BaseMessages.getString(PKG, "DatabaseLookupDialog.NoSchema.Error"));
          mb.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.GetSchemas.Error"));
          mb.open();
        }
      } catch (Exception e) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
            BaseMessages.getString(PKG, "DatabaseLookupDialog.ErrorGettingSchemas"),
            e);
      } finally {
        database.disconnect();
      }
    }
  }
}
