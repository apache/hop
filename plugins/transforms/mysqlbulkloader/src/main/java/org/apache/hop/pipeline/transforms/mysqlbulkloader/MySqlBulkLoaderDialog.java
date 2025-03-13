/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.mysqlbulkloader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.Const;
import org.apache.hop.core.DbCache;
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
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.database.dialog.SqlEditor;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.ShowMessageDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class MySqlBulkLoaderDialog extends BaseTransformDialog {
  private static final Class<?> PKG =
      MySqlBulkLoaderDialog.class; // for i18n purposes, needed by Translator2!!

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TextVar wSchema;
  private TextVar wTable;
  private TextVar wFifoFile;
  private Button wReplace;
  private Button wIgnore;
  private Button wLocal;
  private TextVar wDelimiter;
  private TextVar wEnclosure;
  private TextVar wEscapeChar;
  private TextVar wLoadCharSet;
  private TextVar wCharSet;
  private TextVar wBulkSize;
  private TableView wReturn;
  private final MySqlBulkLoaderMeta input;
  private ColumnInfo[] ciReturn;
  private final Map<String, Integer> inputFields;

  /** List of ColumnInfo that should have the field names of the selected database table */
  private final List<ColumnInfo> tableFieldColumns = new ArrayList<>();

  public MySqlBulkLoaderDialog(
      Shell parent,
      IVariables variables,
      MySqlBulkLoaderMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
    inputFields = new HashMap<>();
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();
    FocusListener lsFocusLost =
        new FocusAdapter() {
          @Override
          public void focusLost(FocusEvent arg0) {
            setTableFieldCombo();
          }
        };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(
        BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.TransformName.Label"));
    PropsUi.setLook(wlTransformName);
    FormData fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    FormData fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    // Connection line
    wConnection = addConnectionLine(shell, wTransformName, input.getConnection(), lsMod);
    if (input.getConnection() == null) {
      wConnection.select(0);
    }
    wConnection.addModifyListener(lsMod);

    // Schema line...
    Label wlSchema = new Label(shell, SWT.RIGHT);
    wlSchema.setText(BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.TargetSchema.Label"));
    PropsUi.setLook(wlSchema);
    FormData fdlSchema = new FormData();
    fdlSchema.left = new FormAttachment(0, 0);
    fdlSchema.right = new FormAttachment(middle, -margin);
    fdlSchema.top = new FormAttachment(wConnection, margin * 2);
    wlSchema.setLayoutData(fdlSchema);

    wSchema = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSchema);
    wSchema.addModifyListener(lsMod);
    wSchema.addFocusListener(lsFocusLost);
    FormData fdSchema = new FormData();
    fdSchema.left = new FormAttachment(middle, 0);
    fdSchema.top = new FormAttachment(wConnection, margin * 2);
    fdSchema.right = new FormAttachment(100, 0);
    wSchema.setLayoutData(fdSchema);

    // Table line...
    Label wlTable = new Label(shell, SWT.RIGHT);
    wlTable.setText(BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.TargetTable.Label"));
    PropsUi.setLook(wlTable);
    FormData fdlTable = new FormData();
    fdlTable.left = new FormAttachment(0, 0);
    fdlTable.right = new FormAttachment(middle, -margin);
    fdlTable.top = new FormAttachment(wSchema, margin);
    wlTable.setLayoutData(fdlTable);

    Button wbTable = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTable);
    wbTable.setText(BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.Browse.Button"));
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment(100, 0);
    fdbTable.top = new FormAttachment(wSchema, margin);
    wbTable.setLayoutData(fdbTable);
    wTable = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTable);
    wTable.addModifyListener(lsMod);
    wTable.addFocusListener(lsFocusLost);
    FormData fdTable = new FormData();
    fdTable.left = new FormAttachment(middle, 0);
    fdTable.top = new FormAttachment(wSchema, margin);
    fdTable.right = new FormAttachment(wbTable, -margin);
    wTable.setLayoutData(fdTable);

    // FifoFile line...
    Label wlFifoFile = new Label(shell, SWT.RIGHT);
    wlFifoFile.setText(BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.FifoFile.Label"));
    PropsUi.setLook(wlFifoFile);
    FormData fdlFifoFile = new FormData();
    fdlFifoFile.left = new FormAttachment(0, 0);
    fdlFifoFile.right = new FormAttachment(middle, -margin);
    fdlFifoFile.top = new FormAttachment(wTable, margin);
    wlFifoFile.setLayoutData(fdlFifoFile);
    wFifoFile = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFifoFile);
    wFifoFile.addModifyListener(lsMod);
    FormData fdFifoFile = new FormData();
    fdFifoFile.left = new FormAttachment(middle, 0);
    fdFifoFile.top = new FormAttachment(wTable, margin);
    fdFifoFile.right = new FormAttachment(100, 0);
    wFifoFile.setLayoutData(fdFifoFile);

    // Delimiter line...
    Label wlDelimiter = new Label(shell, SWT.RIGHT);
    wlDelimiter.setText(BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.Delimiter.Label"));
    PropsUi.setLook(wlDelimiter);
    FormData fdlDelimiter = new FormData();
    fdlDelimiter.left = new FormAttachment(0, 0);
    fdlDelimiter.right = new FormAttachment(middle, -margin);
    fdlDelimiter.top = new FormAttachment(wFifoFile, margin);
    wlDelimiter.setLayoutData(fdlDelimiter);
    Button wbDelimiter = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbDelimiter);
    wbDelimiter.setText(BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.Delimiter.Button"));
    FormData fdbDelimiter = new FormData();
    fdbDelimiter.top = new FormAttachment(wFifoFile, margin);
    fdbDelimiter.right = new FormAttachment(100, 0);
    wbDelimiter.setLayoutData(fdbDelimiter);
    wDelimiter = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDelimiter);
    wDelimiter.addModifyListener(lsMod);
    FormData fdDelimiter = new FormData();
    fdDelimiter.left = new FormAttachment(middle, 0);
    fdDelimiter.top = new FormAttachment(wFifoFile, margin);
    fdDelimiter.right = new FormAttachment(wbDelimiter, -margin);
    wDelimiter.setLayoutData(fdDelimiter);
    // Allow the insertion of tabs as separator...
    wbDelimiter.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent se) {
            Text t = wDelimiter.getTextWidget();
            if (t != null) {
              t.insert("\t");
            }
          }
        });

    // Enclosure line...
    Label wlEnclosure = new Label(shell, SWT.RIGHT);
    wlEnclosure.setText(BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.Enclosure.Label"));
    PropsUi.setLook(wlEnclosure);
    FormData fdlEnclosure = new FormData();
    fdlEnclosure.left = new FormAttachment(0, 0);
    fdlEnclosure.right = new FormAttachment(middle, -margin);
    fdlEnclosure.top = new FormAttachment(wDelimiter, margin);
    wlEnclosure.setLayoutData(fdlEnclosure);
    wEnclosure = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wEnclosure);
    wEnclosure.addModifyListener(lsMod);
    FormData fdEnclosure = new FormData();
    fdEnclosure.left = new FormAttachment(middle, 0);
    fdEnclosure.top = new FormAttachment(wDelimiter, margin);
    fdEnclosure.right = new FormAttachment(100, 0);
    wEnclosure.setLayoutData(fdEnclosure);

    // EscapeChar line...
    Label wlEscapeChar = new Label(shell, SWT.RIGHT);
    wlEscapeChar.setText(BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.EscapeChar.Label"));
    PropsUi.setLook(wlEscapeChar);
    FormData fdlEscapeChar = new FormData();
    fdlEscapeChar.left = new FormAttachment(0, 0);
    fdlEscapeChar.right = new FormAttachment(middle, -margin);
    fdlEscapeChar.top = new FormAttachment(wEnclosure, margin);
    wlEscapeChar.setLayoutData(fdlEscapeChar);
    wEscapeChar = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wEscapeChar);
    wEscapeChar.addModifyListener(lsMod);
    FormData fdEscapeChar = new FormData();
    fdEscapeChar.left = new FormAttachment(middle, 0);
    fdEscapeChar.top = new FormAttachment(wEnclosure, margin);
    fdEscapeChar.right = new FormAttachment(100, 0);
    wEscapeChar.setLayoutData(fdEscapeChar);

    // Load Charset line...
    Label wlLoadCharSet = new Label(shell, SWT.RIGHT);
    wlLoadCharSet.setText(BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.LoadCharSet.Label"));
    PropsUi.setLook(wlLoadCharSet);
    FormData fdlLoadCharSet = new FormData();
    fdlLoadCharSet.left = new FormAttachment(0, 0);
    fdlLoadCharSet.right = new FormAttachment(middle, -margin);
    fdlLoadCharSet.top = new FormAttachment(wEscapeChar, margin);
    wlLoadCharSet.setLayoutData(fdlLoadCharSet);

    wLoadCharSet = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wLoadCharSet);
    wLoadCharSet.addModifyListener(lsMod);
    FormData fdLoadCharSet = new FormData();
    fdLoadCharSet.left = new FormAttachment(middle, 0);
    fdLoadCharSet.top = new FormAttachment(wEscapeChar, margin);
    fdLoadCharSet.right = new FormAttachment(100, 0);
    wLoadCharSet.setLayoutData(fdLoadCharSet);

    // CharSet line...
    Label wlCharSet = new Label(shell, SWT.RIGHT);
    wlCharSet.setText(BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.CharSet.Label"));
    PropsUi.setLook(wlCharSet);
    FormData fdlCharSet = new FormData();
    fdlCharSet.left = new FormAttachment(0, 0);
    fdlCharSet.right = new FormAttachment(middle, -margin);
    fdlCharSet.top = new FormAttachment(wLoadCharSet, margin);
    wlCharSet.setLayoutData(fdlCharSet);

    wCharSet = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCharSet);
    wCharSet.addModifyListener(lsMod);
    FormData fdCharSet = new FormData();
    fdCharSet.left = new FormAttachment(middle, 0);
    fdCharSet.top = new FormAttachment(wLoadCharSet, margin);
    fdCharSet.right = new FormAttachment(100, 0);
    wCharSet.setLayoutData(fdCharSet);

    // BulkSize line...
    Label wlBulkSize = new Label(shell, SWT.RIGHT);
    wlBulkSize.setText(BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.BulkSize.Label"));
    PropsUi.setLook(wlBulkSize);
    FormData fdlBulkSize = new FormData();
    fdlBulkSize.left = new FormAttachment(0, 0);
    fdlBulkSize.right = new FormAttachment(middle, -margin);
    fdlBulkSize.top = new FormAttachment(wCharSet, margin);
    wlBulkSize.setLayoutData(fdlBulkSize);
    wBulkSize = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wBulkSize);
    wBulkSize.addModifyListener(lsMod);
    FormData fdBulkSize = new FormData();
    fdBulkSize.left = new FormAttachment(middle, 0);
    fdBulkSize.top = new FormAttachment(wCharSet, margin);
    fdBulkSize.right = new FormAttachment(100, 0);
    wBulkSize.setLayoutData(fdBulkSize);

    // Replace line...
    Label wlReplace = new Label(shell, SWT.RIGHT);
    wlReplace.setText(BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.Replace.Label"));
    PropsUi.setLook(wlReplace);
    FormData fdlReplace = new FormData();
    fdlReplace.left = new FormAttachment(0, 0);
    fdlReplace.right = new FormAttachment(middle, -margin);
    fdlReplace.top = new FormAttachment(wBulkSize, margin * 2);
    wlReplace.setLayoutData(fdlReplace);

    wReplace = new Button(shell, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wReplace);
    FormData fdReplace = new FormData();
    fdReplace.left = new FormAttachment(middle, 0);
    fdReplace.top = new FormAttachment(wBulkSize, margin * 2);
    fdReplace.right = new FormAttachment(100, 0);
    wReplace.setLayoutData(fdReplace);
    wReplace.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            if (wReplace.getSelection()) {
              wIgnore.setSelection(false);
            }
            input.setChanged();
          }
        });

    // Ignore line...
    Label wlIgnore = new Label(shell, SWT.RIGHT);
    wlIgnore.setText(BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.Ignore.Label"));
    PropsUi.setLook(wlIgnore);
    FormData fdlIgnore = new FormData();
    fdlIgnore.left = new FormAttachment(0, 0);
    fdlIgnore.right = new FormAttachment(middle, -margin);
    fdlIgnore.top = new FormAttachment(wReplace, margin * 2);
    wlIgnore.setLayoutData(fdlIgnore);

    wIgnore = new Button(shell, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wIgnore);
    FormData fdIgnore = new FormData();
    fdIgnore.left = new FormAttachment(middle, 0);
    fdIgnore.top = new FormAttachment(wReplace, margin * 2);
    fdIgnore.right = new FormAttachment(100, 0);
    wIgnore.setLayoutData(fdIgnore);
    wIgnore.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            if (wIgnore.getSelection()) {
              wReplace.setSelection(false);
            }
            input.setChanged();
          }
        });

    // Local line...
    Label wlLocal = new Label(shell, SWT.RIGHT);
    wlLocal.setText(BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.Local.Label"));
    PropsUi.setLook(wlLocal);
    FormData fdlLocal = new FormData();
    fdlLocal.left = new FormAttachment(0, 0);
    fdlLocal.right = new FormAttachment(middle, -margin);
    fdlLocal.top = new FormAttachment(wIgnore, margin * 2);
    wlLocal.setLayoutData(fdlLocal);

    wLocal = new Button(shell, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wLocal);
    FormData fdLocal = new FormData();
    fdLocal.left = new FormAttachment(middle, 0);
    fdLocal.top = new FormAttachment(wIgnore, margin * 2);
    fdLocal.right = new FormAttachment(100, 0);
    wLocal.setLayoutData(fdLocal);
    wLocal.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        });

    // THE BUTTONS
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wSql = new Button(shell, SWT.PUSH);
    wSql.setText(BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.SQL.Button"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    setButtonPositions(new Button[] {wOk, wCancel, wSql}, margin, null);

    // The field Table
    Label wlReturn = new Label(shell, SWT.NONE);
    wlReturn.setText(BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.Fields.Label"));
    PropsUi.setLook(wlReturn);
    FormData fdlReturn = new FormData();
    fdlReturn.left = new FormAttachment(0, 0);
    fdlReturn.top = new FormAttachment(wLocal, margin);
    wlReturn.setLayoutData(fdlReturn);

    int upInsCols = 3;
    int upInsRows = (input.getFields() != null ? input.getFields().size() : 1);

    ciReturn = new ColumnInfo[upInsCols];
    ciReturn[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.ColumnInfo.TableField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciReturn[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.ColumnInfo.StreamField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciReturn[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.ColumnInfo.FormatOK"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            MySqlBulkLoaderMeta.getFieldFormatTypeDescriptions(),
            true);

    tableFieldColumns.add(ciReturn[0]);
    wReturn =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciReturn,
            upInsRows,
            lsMod,
            props);

    Button wGetLU = new Button(shell, SWT.PUSH);
    wGetLU.setText(BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.GetFields.Label"));
    FormData fdGetLU = new FormData();
    fdGetLU.top = new FormAttachment(wlReturn, margin);
    fdGetLU.right = new FormAttachment(100, 0);
    wGetLU.setLayoutData(fdGetLU);

    Button wDoMapping = new Button(shell, SWT.PUSH);
    wDoMapping.setText(BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.EditMapping.Label"));
    FormData fdDoMapping = new FormData();
    fdDoMapping.top = new FormAttachment(wGetLU, margin);
    fdDoMapping.right = new FormAttachment(100, 0);
    wDoMapping.setLayoutData(fdDoMapping);

    wDoMapping.addListener(SWT.Selection, arg0 -> generateMappings());

    FormData fdReturn = new FormData();
    fdReturn.left = new FormAttachment(0, 0);
    fdReturn.top = new FormAttachment(wlReturn, margin);
    fdReturn.right = new FormAttachment(wDoMapping, -margin);
    fdReturn.bottom = new FormAttachment(wOk, -2 * margin);
    wReturn.setLayoutData(fdReturn);

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
                inputFields.put(row.getValueMeta(i).getName(), i);
              }

              setComboBoxes();
            } catch (HopException e) {
              logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
            }
          }
        };
    new Thread(runnable).start();

    wOk.addListener(SWT.Selection, e -> ok());
    wGetLU.addListener(SWT.Selection, e -> getUpdate());
    wSql.addListener(SWT.Selection, e -> create());
    wCancel.addListener(SWT.Selection, e -> cancel());

    wbTable.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            getTableName();
          }
        });

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    setTableFieldCombo();
    input.setChanged(changed);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /**
   * Reads in the fields from the previous Transforms and from the ONE next Transform and opens an
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
              PKG, "MySqlBulkLoaderDialog.DoMapping.UnableToFindSourceFields.Title"),
          BaseMessages.getString(
              PKG, "MySqlBulkLoaderDialog.DoMapping.UnableToFindSourceFields.Message"),
          e);
      return;
    }
    // refresh data
    input.setConnection(wConnection.getText());
    input.setTableName(variables.resolve(wTable.getText()));
    ITransformMeta transformMetaInterface = transformMeta.getTransform();
    try {
      targetFields = transformMetaInterface.getRequiredFields(variables);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(
              PKG, "MySqlBulkLoaderDialog.DoMapping.UnableToFindTargetFields.Title"),
          BaseMessages.getString(
              PKG, "MySqlBulkLoaderDialog.DoMapping.UnableToFindTargetFields.Message"),
          e);
      return;
    }

    String[] inputNames = new String[sourceFields.size()];
    for (int i = 0; i < sourceFields.size(); i++) {
      IValueMeta value = sourceFields.getValueMeta(i);
      inputNames[i] = value.getName() + "-" + value.getOrigin() + ")";
    }

    // Create the existing mapping list...
    //
    List<SourceToTargetMapping> mappings = new ArrayList<>();
    StringBuilder missingSourceFields = new StringBuilder();
    StringBuilder missingTargetFields = new StringBuilder();

    int nrFields = wReturn.nrNonEmpty();
    for (int i = 0; i < nrFields; i++) {
      TableItem item = wReturn.getNonEmpty(i);
      String source = item.getText(2);
      String target = item.getText(1);

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
                    "MySqlBulkLoaderDialog.DoMapping.SomeSourceFieldsNotFound",
                    missingSourceFields.toString())
                + Const.CR;
      }
      if (!missingTargetFields.isEmpty()) {
        message +=
            BaseMessages.getString(
                    PKG,
                    "MySqlBulkLoaderDialog.DoMapping.SomeTargetFieldsNotFound",
                    missingSourceFields.toString())
                + Const.CR;
      }
      message += Const.CR;
      message +=
          BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.DoMapping.SomeFieldsNotFoundContinue")
              + Const.CR;

      ShowMessageDialog msgDialog =
          new ShowMessageDialog(
              shell,
              SWT.ICON_INFORMATION | SWT.OK,
              BaseMessages.getString(
                  PKG, "MySqlBulkLoaderDialog.DoMapping.SomeFieldsNotFoundTitle"),
              message,
              false);
      msgDialog.open();
    }
    EnterMappingDialog d =
        new EnterMappingDialog(
            MySqlBulkLoaderDialog.this.shell,
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
        item.setText(
            3,
            MySqlBulkLoaderMeta.getFieldFormatTypeDescription(
                MySqlBulkLoaderMeta.getFieldFormatType(
                    input.getFields().get(i).getFieldFormatType())));
      }
      wReturn.setRowNums();
      wReturn.optWidth(true);
    }
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.Log.GettingKeyInfo"));
    }

    wEnclosure.setText(Const.NVL(input.getEnclosure(), ""));
    wDelimiter.setText(Const.NVL(input.getDelimiter(), ""));
    wEscapeChar.setText(Const.NVL(input.getEscapeChar(), ""));
    wLoadCharSet.setText(Const.NVL(input.getLoadCharSet(), ""));
    wCharSet.setText(Const.NVL(input.getEncoding(), ""));
    wReplace.setSelection(input.isReplacingData());
    wIgnore.setSelection(input.isIgnoringErrors());
    wLocal.setSelection(input.isLocalFile());
    wBulkSize.setText(Const.NVL(input.getBulkSize(), ""));

    if (input.getFields() != null) {
      for (int i = 0; i < input.getFields().size(); i++) {
        TableItem item = wReturn.table.getItem(i);
        if (input.getFields().get(i).getFieldTable() != null) {
          item.setText(1, input.getFields().get(i).getFieldTable());
        }
        if (input.getFields().get(i).getFieldStream() != null) {
          item.setText(2, input.getFields().get(i).getFieldStream());
        }
        item.setText(
            3,
            MySqlBulkLoaderMeta.getFieldFormatTypeDescription(
                MySqlBulkLoaderMeta.getFieldFormatType(
                    input.getFields().get(i).getFieldFormatType())));
      }
    }

    if (input.getConnection() != null) {
      wConnection.setText(input.getConnection());
    }
    if (input.getSchemaName() != null) {
      wSchema.setText(input.getSchemaName());
    }
    if (input.getTableName() != null) {
      wTable.setText(input.getTableName());
    }
    if (input.getFifoFileName() != null) {
      wFifoFile.setText(input.getFifoFileName());
    }

    wReturn.setRowNums();
    wReturn.optWidth(true);

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<>();

    // Add the currentMeta fields...
    fields.putAll(inputFields);

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>(keySet);

    String[] fieldNames = entries.toArray(new String[entries.size()]);
    Const.sortStrings(fieldNames);
    // return fields
    ciReturn[1].setComboValues(fieldNames);
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void getInfo(MySqlBulkLoaderMeta inf) {
    int nrfields = wReturn.nrNonEmpty();

    inf.setEnclosure(wEnclosure.getText());
    inf.setDelimiter(wDelimiter.getText());
    inf.setEscapeChar(wEscapeChar.getText());
    inf.setLoadCharSet(wLoadCharSet.getText());
    inf.setEncoding(wCharSet.getText());
    inf.setReplacingData(wReplace.getSelection());
    inf.setIgnoringErrors(wIgnore.getSelection());
    inf.setLocalFile(wLocal.getSelection());
    inf.setBulkSize(wBulkSize.getText());

    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.Log.FoundFields", "" + nrfields));
    }
    // CHECKSTYLE:Indentation:OFF
    List<MySqlBulkLoaderMeta.Field> fields = new ArrayList<>();
    for (int i = 0; i < nrfields; i++) {
      TableItem item = wReturn.getNonEmpty(i);
      MySqlBulkLoaderMeta.Field field = new MySqlBulkLoaderMeta.Field();
      field.setFieldTable(item.getText(1));
      field.setFieldStream(item.getText(2));
      field.setFieldFormatType(
          MySqlBulkLoaderMeta.getFieldFormatTypeCode(
              MySqlBulkLoaderMeta.getFieldFormatType(item.getText(3))));
      fields.add(field);
    }
    inf.setFields(fields);
    inf.setSchemaName(wSchema.getText());
    inf.setTableName(wTable.getText());
    inf.setConnection(wConnection.getText());
    inf.setFifoFileName(wFifoFile.getText());

    transformName = wTransformName.getText(); // return value
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    // Get the information for the dialog into the input structure.
    getInfo(input);

    if (input.getConnection() == null) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.InvalidConnection.DialogMessage"));
      mb.setText(
          BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.InvalidConnection.DialogTitle"));
      mb.open();
    }

    dispose();
  }

  private void getTableName() {
    DatabaseMeta inf = null;

    if (!wConnection.getText().isEmpty()) {
      inf = pipelineMeta.findDatabase(wConnection.getText(), variables);
    }

    if (inf != null) {
      if (log.isDebug()) {
        logDebug(
            BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.Log.LookingAtConnection")
                + inf.toString());
      }

      DatabaseExplorerDialog std =
          new DatabaseExplorerDialog(
              shell, SWT.NONE, variables, inf, pipelineMeta.getDatabases(), false, true);
      std.setSelectedSchemaAndTable(wSchema.getText(), wTable.getText());
      if (std.open()) {
        wSchema.setText(Const.NVL(std.getSchemaName(), ""));
        wTable.setText(Const.NVL(std.getTableName(), ""));
      }
    } else {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.InvalidConnection.DialogMessage"));
      mb.setText(
          BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.InvalidConnection.DialogTitle"));
      mb.open();
    }
  }

  private void getUpdate() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        ITableItemInsertListener listener =
            (tableItem, v) -> {
              if (v.getType() == IValueMeta.TYPE_DATE) {
                // The default is : format is OK for dates, see if this sticks later on...
                //
                tableItem.setText(
                    3,
                    BaseMessages.getString(
                        PKG, "MySqlBulkLoaderMeta.FieldFormatType.Date.Description"));
              } else {
                tableItem.setText(
                    3,
                    BaseMessages.getString(
                        PKG,
                        "MySqlBulkLoaderMeta.FieldFormatType.OK.Description")); // default is OK
                // too...
              }
              return true;
            };

        BaseTransformDialog.getFieldsFromPrevious(
            r, wReturn, 1, new int[] {1, 2}, new int[] {}, -1, -1, listener);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  // Generate code for create table...
  // Conversions done by Database
  private void create() {
    try {
      MySqlBulkLoaderMeta info = new MySqlBulkLoaderMeta();
      getInfo(info);

      String name = transformName;
      TransformMeta transformMeta =
          new TransformMeta(
              BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.TransformMeta.Title"), name, info);
      IRowMeta prev = pipelineMeta.getPrevTransformFields(variables, transformName);
      DatabaseMeta databaseMeta = pipelineMeta.findDatabase(wConnection.getText(), variables);
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
          mb.setMessage(
              BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.NoSQLNeeds.DialogMessage"));
          mb.setText(BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.NoSQLNeeds.DialogTitle"));
          mb.open();
        }
      } else {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(sql.getError());
        mb.setText(BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.SQLError.DialogTitle"));
        mb.open();
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.CouldNotBuildSQL.DialogTitle"),
          BaseMessages.getString(PKG, "MySqlBulkLoaderDialog.CouldNotBuildSQL.DialogMessage"),
          ke);
    }
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
              DatabaseMeta ci = pipelineMeta.findDatabase(connectionName, variables);
              if (ci != null) {
                try (Database db = new Database(loggingObject, variables, ci)) {
                  db.connect();

                  String schemaTable =
                      ci.getQuotedSchemaTableCombination(
                          variables, variables.resolve(schemaName), variables.resolve(tableName));
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
                }
              }
            }
          }
        };
    shell.getDisplay().asyncExec(fieldLoader);
  }
}
