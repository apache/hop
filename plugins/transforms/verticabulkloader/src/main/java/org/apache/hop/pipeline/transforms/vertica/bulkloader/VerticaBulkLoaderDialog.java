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

package org.apache.hop.pipeline.transforms.vertica.bulkloader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.Const;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.Props;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.StringUtil;
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
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
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
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class VerticaBulkLoaderDialog extends BaseTransformDialog {

  private static final Class<?> PKG =
      VerticaBulkLoaderMeta.class; // for i18n purposes, needed by Translator2!!
  public static final String CONST_SYSTEM_DIALOG_ERROR_TITLE = "System.Dialog.Error.Title";

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private Button wTruncate;

  private Button wOnlyWhenHaveRows;

  private TextVar wSchema;

  private TextVar wTable;

  private TextVar wExceptionsLogFile;

  private TextVar wRejectedDataLogFile;

  private TextVar wStreamName;

  private Button wAbortOnError;

  private Button wDirect;

  private Button wSpecifyFields;

  private TableView wFields;

  private Button wGetFields;

  private Button wDoMapping;

  private VerticaBulkLoaderMeta input;

  private Map<String, Integer> inputFields;

  private ColumnInfo[] ciFields;

  /** List of ColumnInfo that should have the field names of the selected database table */
  private List<ColumnInfo> tableFieldColumns = new ArrayList<>();

  /** Constructor. */
  public VerticaBulkLoaderDialog(
      Shell parent,
      IVariables variables,
      VerticaBulkLoaderMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
    inputFields = new HashMap<>();
  }

  /** Open the dialog. */
  public String open() {
    createShell(BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.DialogTitle"));
    buildButtonBar().ok(e -> ok()).sql(e -> sql()).cancel(e -> cancel()).build();

    FormData fdDoMapping;
    FormData fdGetFields;
    Label wlFields;
    FormData fdSpecifyFields;
    Label wlSpecifyFields;
    FormData fdDirect;
    FormData fdAbortOnError;
    FormData fdlAbortOnError;
    Label wlAbortOnError;
    FormData fdStreamName;
    Label wlStreamName;
    FormData fdRejectedDataLogFile;
    FormData fdlRejectedDataLogFile;
    Label wlRejectedDataLogFile;
    FormData fdExceptionsLogFile;
    FormData fdlExceptionsLogFile;
    Label wlExceptionsLogFile;
    FormData fdbTable;
    FormData fdlTable;
    Button wbTable;
    FormData fdSchema;
    FormData fdlSchema;
    Label wlSchema;
    FormData fdMainComp;
    CTabItem wFieldsTab;
    CTabItem wMainTab;
    FormData fdTabFolder;
    CTabFolder wTabFolder;
    Label wlTruncate;

    ModifyListener lsMod = e -> input.setChanged();
    FocusListener lsFocusLost =
        new FocusAdapter() {
          @Override
          public void focusLost(FocusEvent arg0) {
            setTableFieldCombo();
          }
        };
    backupChanged = input.hasChanged();

    // Connection line
    DatabaseMeta dbm = pipelineMeta.findDatabase(input.getConnection(), variables);
    wConnection = addConnectionLine(shell, wSpacer, input.getDatabaseMeta(), null);
    if (input.getDatabaseMeta() == null && pipelineMeta.nrDatabases() == 1) {
      wConnection.select(0);
    }
    wConnection.addModifyListener(lsMod);
    wConnection.addModifyListener(event -> setFlags());

    // Schema line...
    wlSchema = new Label(shell, SWT.RIGHT);
    wlSchema.setText(
        BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.TargetSchema.Label")); // $NON-NLS-1$
    PropsUi.setLook(wlSchema);
    fdlSchema = new FormData();
    fdlSchema.left = new FormAttachment(0, 0);
    fdlSchema.right = new FormAttachment(middle, -margin);
    fdlSchema.top = new FormAttachment(wConnection, margin);
    wlSchema.setLayoutData(fdlSchema);

    wSchema = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSchema);
    wSchema.addModifyListener(lsMod);
    wSchema.addFocusListener(lsFocusLost);
    fdSchema = new FormData();
    fdSchema.left = new FormAttachment(middle, 0);
    fdSchema.top = new FormAttachment(wConnection, margin);
    fdSchema.right = new FormAttachment(100, 0);
    wSchema.setLayoutData(fdSchema);

    // Table line...
    Label wlTable = new Label(shell, SWT.RIGHT);
    wlTable.setText(BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.TargetTable.Label"));
    PropsUi.setLook(wlTable);
    fdlTable = new FormData();
    fdlTable.left = new FormAttachment(0, 0);
    fdlTable.right = new FormAttachment(middle, -margin);
    fdlTable.top = new FormAttachment(wSchema, margin);
    wlTable.setLayoutData(fdlTable);

    wbTable = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTable);
    wbTable.setText(BaseMessages.getString("System.Button.Browse"));
    fdbTable = new FormData();
    fdbTable.right = new FormAttachment(100, 0);
    fdbTable.top = new FormAttachment(wSchema, margin);
    wbTable.setLayoutData(fdbTable);

    wTable = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTable);
    wTable.addModifyListener(lsMod);
    wTable.addFocusListener(lsFocusLost);
    FormData fdTable = new FormData();
    fdTable.top = new FormAttachment(wSchema, margin);
    fdTable.left = new FormAttachment(middle, 0);
    fdTable.right = new FormAttachment(wbTable, -margin);
    wTable.setLayoutData(fdTable);

    SelectionAdapter lsSelMod =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        };

    // Truncate table
    wlTruncate = new Label(shell, SWT.RIGHT);
    wlTruncate.setText(BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.TruncateTable.Label"));
    PropsUi.setLook(wlTruncate);
    FormData fdlTruncate = new FormData();
    fdlTruncate.left = new FormAttachment(0, 0);
    fdlTruncate.top = new FormAttachment(wTable, margin);
    fdlTruncate.right = new FormAttachment(middle, -margin);
    wlTruncate.setLayoutData(fdlTruncate);
    wTruncate = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wTruncate);
    FormData fdTruncate = new FormData();
    fdTruncate.left = new FormAttachment(middle, 0);
    fdTruncate.top = new FormAttachment(wlTruncate, 0, SWT.CENTER);
    fdTruncate.right = new FormAttachment(100, 0);
    wTruncate.setLayoutData(fdTruncate);
    SelectionAdapter lsTruncMod =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        };
    wTruncate.addSelectionListener(lsTruncMod);
    wTruncate.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            setFlags();
          }
        });

    // Truncate only when have rows
    Label wlOnlyWhenHaveRows = new Label(shell, SWT.RIGHT);
    wlOnlyWhenHaveRows.setText(
        BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.OnlyWhenHaveRows.Label"));
    PropsUi.setLook(wlOnlyWhenHaveRows);
    FormData fdlOnlyWhenHaveRows = new FormData();
    fdlOnlyWhenHaveRows.left = new FormAttachment(0, 0);
    fdlOnlyWhenHaveRows.top = new FormAttachment(wTruncate, margin);
    fdlOnlyWhenHaveRows.right = new FormAttachment(middle, -margin);
    wlOnlyWhenHaveRows.setLayoutData(fdlOnlyWhenHaveRows);
    wOnlyWhenHaveRows = new Button(shell, SWT.CHECK);
    wOnlyWhenHaveRows.setToolTipText(
        BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.OnlyWhenHaveRows.Tooltip"));
    PropsUi.setLook(wOnlyWhenHaveRows);
    FormData fdTruncateWhenHaveRows = new FormData();
    fdTruncateWhenHaveRows.left = new FormAttachment(middle, 0);
    fdTruncateWhenHaveRows.top = new FormAttachment(wlOnlyWhenHaveRows, 0, SWT.CENTER);
    fdTruncateWhenHaveRows.right = new FormAttachment(100, 0);
    wOnlyWhenHaveRows.setLayoutData(fdTruncateWhenHaveRows);
    wOnlyWhenHaveRows.addSelectionListener(lsSelMod);

    // Specify fields
    wlSpecifyFields = new Label(shell, SWT.RIGHT);
    wlSpecifyFields.setText(
        BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.SpecifyFields.Label"));
    PropsUi.setLook(wlSpecifyFields);
    FormData fdlSpecifyFields = new FormData();
    fdlSpecifyFields.left = new FormAttachment(0, 0);
    fdlSpecifyFields.top = new FormAttachment(wOnlyWhenHaveRows, margin);
    fdlSpecifyFields.right = new FormAttachment(middle, -margin);
    wlSpecifyFields.setLayoutData(fdlSpecifyFields);
    wSpecifyFields = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wSpecifyFields);
    fdSpecifyFields = new FormData();
    fdSpecifyFields.left = new FormAttachment(middle, 0);
    fdSpecifyFields.top = new FormAttachment(wlSpecifyFields, 0, SWT.CENTER);
    fdSpecifyFields.right = new FormAttachment(100, 0);
    wSpecifyFields.setLayoutData(fdSpecifyFields);
    wSpecifyFields.addSelectionListener(lsSelMod);

    // If the flag is off, gray out the fields tab e.g.
    wSpecifyFields.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            setFlags();
          }
        });

    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF KEY TAB ///
    // /
    wMainTab = new CTabItem(wTabFolder, SWT.NONE);
    wMainTab.setText(
        BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.MainTab.CTabItem")); // $NON-NLS-1$

    FormLayout mainLayout = new FormLayout();
    mainLayout.marginWidth = PropsUi.getFormMargin();
    mainLayout.marginHeight = PropsUi.getFormMargin();

    Composite wMainComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wMainComp);
    wMainComp.setLayout(mainLayout);

    fdMainComp = new FormData();
    fdMainComp.left = new FormAttachment(0, 0);
    fdMainComp.top = new FormAttachment(0, 0);
    fdMainComp.right = new FormAttachment(100, 0);
    fdMainComp.bottom = new FormAttachment(100, 0);
    wMainComp.setLayoutData(fdMainComp);

    // Insert directly to ROS
    Label wlDirect = new Label(wMainComp, SWT.RIGHT);
    wlDirect.setText(BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.InsertDirect.Label"));
    wlDirect.setToolTipText(
        BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.InsertDirect.Tooltip"));
    PropsUi.setLook(wlDirect);
    FormData fdlDirect = new FormData();
    fdlDirect.left = new FormAttachment(0, 0);
    fdlDirect.top = new FormAttachment(0, margin);
    fdlDirect.right = new FormAttachment(middle, -margin);
    wlDirect.setLayoutData(fdlDirect);
    wDirect = new Button(wMainComp, SWT.CHECK);
    wDirect.setToolTipText(
        BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.InsertDirect.Tooltip"));
    PropsUi.setLook(wDirect);
    fdDirect = new FormData();
    fdDirect.left = new FormAttachment(middle, 0);
    fdDirect.top = new FormAttachment(0, margin);
    fdDirect.right = new FormAttachment(100, 0);
    wDirect.setLayoutData(fdDirect);
    wDirect.addSelectionListener(lsSelMod);
    wDirect.setSelection(true);

    // Abort on error
    wlAbortOnError = new Label(wMainComp, SWT.RIGHT);
    wlAbortOnError.setText(
        BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.AbortOnError.Label"));
    wlAbortOnError.setToolTipText(
        BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.AbortOnError.Tooltip"));
    PropsUi.setLook(wlAbortOnError);
    fdlAbortOnError = new FormData();
    fdlAbortOnError.left = new FormAttachment(0, 0);
    fdlAbortOnError.top = new FormAttachment(wlDirect, margin);
    fdlAbortOnError.right = new FormAttachment(middle, -margin);
    wlAbortOnError.setLayoutData(fdlAbortOnError);
    wAbortOnError = new Button(wMainComp, SWT.CHECK);
    wAbortOnError.setToolTipText(
        BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.AbortOnError.Tooltip"));
    PropsUi.setLook(wAbortOnError);
    fdAbortOnError = new FormData();
    fdAbortOnError.left = new FormAttachment(middle, 0);
    fdAbortOnError.top = new FormAttachment(wlDirect, margin);
    fdAbortOnError.right = new FormAttachment(100, 0);
    wAbortOnError.setLayoutData(fdAbortOnError);
    wAbortOnError.addSelectionListener(lsSelMod);
    wAbortOnError.setSelection(true);

    // ExceptionsLogFile line...
    wlExceptionsLogFile = new Label(wMainComp, SWT.RIGHT);
    wlExceptionsLogFile.setText(
        BaseMessages.getString(
            PKG, "VerticaBulkLoaderDialog.ExceptionsLogFile.Label")); // $NON-NLS-1$
    wlExceptionsLogFile.setToolTipText(
        BaseMessages.getString(
            PKG, "VerticaBulkLoaderDialog.ExceptionsLogFile.Tooltip")); // $NON-NLS-1$
    PropsUi.setLook(wlExceptionsLogFile);
    fdlExceptionsLogFile = new FormData();
    fdlExceptionsLogFile.left = new FormAttachment(0, 0);
    fdlExceptionsLogFile.right = new FormAttachment(middle, -margin);
    fdlExceptionsLogFile.top = new FormAttachment(wlAbortOnError, margin);
    wlExceptionsLogFile.setLayoutData(fdlExceptionsLogFile);

    wExceptionsLogFile = new TextVar(variables, wMainComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wExceptionsLogFile.setToolTipText(
        BaseMessages.getString(
            PKG, "VerticaBulkLoaderDialog.ExceptionsLogFile.Tooltip")); // $NON-NLS-1$
    PropsUi.setLook(wExceptionsLogFile);
    wExceptionsLogFile.addModifyListener(lsMod);
    wExceptionsLogFile.addFocusListener(lsFocusLost);
    fdExceptionsLogFile = new FormData();
    fdExceptionsLogFile.left = new FormAttachment(middle, 0);
    fdExceptionsLogFile.top = new FormAttachment(wlAbortOnError, margin);
    fdExceptionsLogFile.right = new FormAttachment(100, 0);
    wExceptionsLogFile.setLayoutData(fdExceptionsLogFile);

    // RejectedDataLogFile line...
    wlRejectedDataLogFile = new Label(wMainComp, SWT.RIGHT);
    wlRejectedDataLogFile.setText(
        BaseMessages.getString(
            PKG, "VerticaBulkLoaderDialog.RejectedDataLogFile.Label")); // $NON-NLS-1$
    wlRejectedDataLogFile.setToolTipText(
        BaseMessages.getString(
            PKG, "VerticaBulkLoaderDialog.RejectedDataLogFile.Tooltip")); // $NON-NLS-1$
    PropsUi.setLook(wlRejectedDataLogFile);
    fdlRejectedDataLogFile = new FormData();
    fdlRejectedDataLogFile.left = new FormAttachment(0, 0);
    fdlRejectedDataLogFile.right = new FormAttachment(middle, -margin);
    fdlRejectedDataLogFile.top = new FormAttachment(wlExceptionsLogFile, margin);
    wlRejectedDataLogFile.setLayoutData(fdlRejectedDataLogFile);

    wRejectedDataLogFile = new TextVar(variables, wMainComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wRejectedDataLogFile.setToolTipText(
        BaseMessages.getString(
            PKG, "VerticaBulkLoaderDialog.RejectedDataLogFile.Tooltip")); // $NON-NLS-1$
    PropsUi.setLook(wRejectedDataLogFile);
    wRejectedDataLogFile.addModifyListener(lsMod);
    wRejectedDataLogFile.addFocusListener(lsFocusLost);
    fdRejectedDataLogFile = new FormData();
    fdRejectedDataLogFile.left = new FormAttachment(middle, 0);
    fdRejectedDataLogFile.top = new FormAttachment(wlExceptionsLogFile, margin);
    fdRejectedDataLogFile.right = new FormAttachment(100, 0);
    wRejectedDataLogFile.setLayoutData(fdRejectedDataLogFile);

    // StreamName line...
    wlStreamName = new Label(wMainComp, SWT.RIGHT);
    wlStreamName.setText(
        BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.StreamName.Label")); // $NON-NLS-1$
    wlStreamName.setToolTipText(
        BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.StreamName.Tooltip")); // $NON-NLS-1$
    PropsUi.setLook(wlStreamName);
    FormData fdlStreamName = new FormData();
    fdlStreamName.left = new FormAttachment(0, 0);
    fdlStreamName.right = new FormAttachment(middle, -margin);
    fdlStreamName.top = new FormAttachment(wlRejectedDataLogFile, margin);
    wlStreamName.setLayoutData(fdlStreamName);

    wStreamName = new TextVar(variables, wMainComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wStreamName.setToolTipText(
        BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.StreamName.Tooltip")); // $NON-NLS-1$
    PropsUi.setLook(wStreamName);
    wStreamName.addModifyListener(lsMod);
    wStreamName.addFocusListener(lsFocusLost);
    fdStreamName = new FormData();
    fdStreamName.left = new FormAttachment(middle, 0);
    fdStreamName.top = new FormAttachment(wlRejectedDataLogFile, margin);
    fdStreamName.right = new FormAttachment(100, 0);
    wStreamName.setLayoutData(fdStreamName);

    wMainComp.layout();
    wMainTab.setControl(wMainComp);

    //
    // Fields tab...
    //
    wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setText(
        BaseMessages.getString(
            PKG, "VerticaBulkLoaderDialog.FieldsTab.CTabItem.Title")); // $NON-NLS-1$

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFieldsComp);

    FormLayout fieldsCompLayout = new FormLayout();
    fieldsCompLayout.marginWidth = Const.FORM_MARGIN;
    fieldsCompLayout.marginHeight = Const.FORM_MARGIN;
    wFieldsComp.setLayout(fieldsCompLayout);

    // The fields table
    wlFields = new Label(wFieldsComp, SWT.NONE);
    wlFields.setText(
        BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.InsertFields.Label")); // $NON-NLS-1$
    PropsUi.setLook(wlFields);
    FormData fdlUpIns = new FormData();
    fdlUpIns.left = new FormAttachment(0, 0);
    fdlUpIns.top = new FormAttachment(0, margin);
    wlFields.setLayoutData(fdlUpIns);

    int tableCols = 2;
    int upInsRows =
        (input.getFields() != null && !input.getFields().equals(Collections.emptyList())
            ? input.getFields().size()
            : 1);

    ciFields = new ColumnInfo[tableCols];
    ciFields[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.ColumnInfo.TableField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false); //$NON-NLS-1$
    ciFields[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.ColumnInfo.StreamField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false); //$NON-NLS-1$
    tableFieldColumns.add(ciFields[0]);
    wFields =
        new TableView(
            variables,
            wFieldsComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciFields,
            upInsRows,
            lsMod,
            props);

    wGetFields = new Button(wFieldsComp, SWT.PUSH);
    wGetFields.setText(
        BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.GetFields.Button")); // $NON-NLS-1$
    fdGetFields = new FormData();
    fdGetFields.top = new FormAttachment(wlFields, margin);
    fdGetFields.right = new FormAttachment(100, 0);
    wGetFields.setLayoutData(fdGetFields);

    wDoMapping = new Button(wFieldsComp, SWT.PUSH);
    wDoMapping.setText(
        BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.DoMapping.Button")); // $NON-NLS-1$
    fdDoMapping = new FormData();
    fdDoMapping.top = new FormAttachment(wGetFields, margin);
    fdDoMapping.right = new FormAttachment(100, 0);
    wDoMapping.setLayoutData(fdDoMapping);

    wDoMapping.addListener(SWT.Selection, arg0 -> generateMappings());

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(wDoMapping, -margin);
    fdFields.bottom = new FormAttachment(100, -margin);
    wFields.setLayoutData(fdFields);

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment(0, 0);
    fdFieldsComp.top = new FormAttachment(0, 0);
    fdFieldsComp.right = new FormAttachment(100, 0);
    fdFieldsComp.bottom = new FormAttachment(100, 0);
    wFieldsComp.setLayoutData(fdFieldsComp);

    wFieldsComp.layout();
    wFieldsTab.setControl(wFieldsComp);

    //
    // Search the fields in the background
    //

    final Runnable runnable =
        new Runnable() {
          public void run() {
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
                log.logError(
                    toString(), BaseMessages.getString("System.Dialog.GetFieldsFailed.Message"));
              }
            }
          }
        };
    new Thread(runnable).start();

    fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wlSpecifyFields, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -margin);
    wTabFolder.setLayoutData(fdTabFolder);
    wTabFolder.setSelection(0);
    wGetFields.addListener(SWT.Selection, c -> get());

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    setTableFieldCombo();
    input.setChanged(backupChanged);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
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
              PKG, "VerticaBulkLoaderDialog.DoMapping.UnableToFindSourceFields.Title"),
          BaseMessages.getString(
              PKG, "VerticaBulkLoaderDialog.DoMapping.UnableToFindSourceFields.Message"),
          e);
      return;
    }

    // refresh data
    input.setTablename(variables.resolve(wTable.getText()));
    ITransformMeta transformMetaInterface = transformMeta.getTransform();
    try {
      targetFields = transformMetaInterface.getRequiredFields(variables);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(
              PKG, "VerticaBulkLoaderDialog.DoMapping.UnableToFindTargetFields.Title"),
          BaseMessages.getString(
              PKG, "VerticaBulkLoaderDialog.DoMapping.UnableToFindTargetFields.Message"),
          e);
      return;
    }

    String[] inputNames = new String[sourceFields.size()];
    for (int i = 0; i < sourceFields.size(); i++) {
      IValueMeta value = sourceFields.getValueMeta(i);
      inputNames[i] = value.getName();
    }

    // Create the existing mapping list...
    //
    List<SourceToTargetMapping> mappings = new ArrayList<>();
    StringBuffer missingSourceFields = new StringBuffer();
    StringBuffer missingTargetFields = new StringBuffer();

    int nrFields = wFields.nrNonEmpty();
    for (int i = 0; i < nrFields; i++) {
      TableItem item = wFields.getNonEmpty(i);
      String source = item.getText(2);
      String target = item.getText(1);

      int sourceIndex = sourceFields.indexOfValue(source);
      if (sourceIndex < 0) {
        missingSourceFields.append(Const.CR + "   " + source + " --> " + target);
      }
      int targetIndex = targetFields.indexOfValue(target);
      if (targetIndex < 0) {
        missingTargetFields.append(Const.CR + "   " + source + " --> " + target);
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
                    "VerticaBulkLoaderDialog.DoMapping.SomeSourceFieldsNotFound",
                    missingSourceFields.toString())
                + Const.CR;
      }
      if (!missingTargetFields.isEmpty()) {
        message +=
            BaseMessages.getString(
                    PKG,
                    "VerticaBulkLoaderDialog.DoMapping.SomeTargetFieldsNotFound",
                    missingSourceFields.toString())
                + Const.CR;
      }
      message += Const.CR;
      message +=
          BaseMessages.getString(
                  PKG, "VerticaBulkLoaderDialog.DoMapping.SomeFieldsNotFoundContinue")
              + Const.CR;
      int answer =
          BaseDialog.openMessageBox(
              shell,
              BaseMessages.getString(
                  PKG, "VerticaBulkLoaderDialog.DoMapping.SomeFieldsNotFoundTitle"),
              message,
              SWT.ICON_QUESTION | SWT.YES | SWT.NO);
      boolean goOn = (answer & SWT.YES) != 0;
      if (!goOn) {
        return;
      }
    }
    EnterMappingDialog d =
        new EnterMappingDialog(
            VerticaBulkLoaderDialog.this.shell,
            sourceFields.getFieldNames(),
            targetFields.getFieldNames(),
            mappings);
    mappings = d.open();

    // mappings == null if the user pressed cancel
    //
    if (mappings != null) {
      // Clear and re-populate!
      //
      wFields.table.removeAll();
      wFields.table.setItemCount(mappings.size());
      for (int i = 0; i < mappings.size(); i++) {
        SourceToTargetMapping mapping = (SourceToTargetMapping) mappings.get(i);
        TableItem item = wFields.table.getItem(i);
        item.setText(2, sourceFields.getValueMeta(mapping.getSourcePosition()).getName());
        item.setText(1, targetFields.getValueMeta(mapping.getTargetPosition()).getName());
      }
      wFields.setRowNums();
      wFields.optWidth(true);
    }
  }

  private void setTableFieldCombo() {
    Runnable fieldLoader =
        () -> {
          // clear
          for (ColumnInfo fieldColumn : tableFieldColumns) {
            ColumnInfo colInfo = (ColumnInfo) fieldColumn;
            colInfo.setComboValues(new String[] {});
          }
          if (!StringUtil.isEmpty(wTable.getText())) {
            DatabaseMeta databaseMeta = pipelineMeta.findDatabase(wConnection.getText(), variables);
            if (databaseMeta != null) {
              try (Database db = new Database(loggingObject, variables, databaseMeta)) {
                db.connect();

                String schemaTable =
                    databaseMeta.getQuotedSchemaTableCombination(
                        variables,
                        variables.resolve(wSchema.getText()),
                        variables.resolve(wTable.getText()));
                IRowMeta r = db.getTableFields(schemaTable);
                if (null != r) {
                  String[] fieldNames = r.getFieldNames();
                  if (null != fieldNames) {
                    for (ColumnInfo tableFieldColumn : tableFieldColumns) {
                      ColumnInfo colInfo = (ColumnInfo) tableFieldColumn;
                      colInfo.setComboValues(fieldNames);
                    }
                  }
                }
              } catch (Exception e) {
                for (ColumnInfo tableFieldColumn : tableFieldColumns) {
                  ColumnInfo colInfo = (ColumnInfo) tableFieldColumn;
                  colInfo.setComboValues(new String[] {});
                }
                // ignore any errors here. drop downs will not be
                // filled, but no problem for the user
              }
            }
          }
        };
    shell.getDisplay().asyncExec(fieldLoader);
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<>();

    // Add the currentMeta fields...
    fields.putAll(inputFields);

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>(keySet);

    String[] fieldNames = (String[]) entries.toArray(new String[entries.size()]);

    if (PropsUi.getInstance().isSortFieldByName()) {
      Const.sortStrings(fieldNames);
    }
    ciFields[1].setComboValues(fieldNames);
  }

  public void setFlags() {
    boolean specifyFields = wSpecifyFields.getSelection();
    wFields.setEnabled(specifyFields);
    wGetFields.setEnabled(specifyFields);
    wDoMapping.setEnabled(specifyFields);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (input.getSchemaName() != null) {
      wSchema.setText(input.getSchemaName());
    }
    if (input.getTableName() != null) {
      wTable.setText(input.getTableName());
    }
    if (input.getConnection() != null) {
      wConnection.setText(input.getConnection());
    }

    if (input.getExceptionsFileName() != null) {
      wExceptionsLogFile.setText(input.getExceptionsFileName());
    }
    if (input.getRejectedDataFileName() != null) {
      wRejectedDataLogFile.setText(input.getRejectedDataFileName());
    }
    if (input.getStreamName() != null) {
      wStreamName.setText(input.getStreamName());
    }

    wTruncate.setSelection(input.isTruncateTable());
    wOnlyWhenHaveRows.setSelection(input.isOnlyWhenHaveRows());

    wDirect.setSelection(input.isDirect());
    wAbortOnError.setSelection(input.isAbortOnError());

    wSpecifyFields.setSelection(input.specifyFields());

    for (int i = 0; i < input.getFields().size(); i++) {
      VerticaBulkLoaderField vbf = input.getFields().get(i);
      TableItem item = wFields.table.getItem(i);
      if (vbf.getFieldDatabase() != null) {
        item.setText(1, vbf.getFieldDatabase());
      }
      if (vbf.getFieldStream() != null) {
        item.setText(2, vbf.getFieldStream());
      }
    }

    setFlags();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(backupChanged);
    dispose();
  }

  private void getInfo(VerticaBulkLoaderMeta info) {
    info.setSchemaName(wSchema.getText());
    info.setTablename(wTable.getText());
    info.setConnection(wConnection.getText());

    info.setTruncateTable(wTruncate.getSelection());
    info.setOnlyWhenHaveRows(wOnlyWhenHaveRows.getSelection());

    info.setExceptionsFileName(wExceptionsLogFile.getText());
    info.setRejectedDataFileName(wRejectedDataLogFile.getText());
    info.setStreamName(wStreamName.getText());

    info.setDirect(wDirect.getSelection());
    info.setAbortOnError(wAbortOnError.getSelection());

    info.setSpecifyFields(wSpecifyFields.getSelection());

    int nrRows = wFields.nrNonEmpty();
    info.getFields().clear();

    for (int i = 0; i < nrRows; i++) {
      TableItem item = wFields.getNonEmpty(i);
      VerticaBulkLoaderField vbf =
          new VerticaBulkLoaderField(
              Const.NVL(item.getText(1), ""), Const.NVL(item.getText(2), ""));
      info.getFields().add(vbf);
    }
  }

  private void ok() {
    if (StringUtil.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    getInfo(input);

    if (Utils.isEmpty(input.getConnection())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.ConnectionError.DialogMessage"));
      mb.setText(BaseMessages.getString(CONST_SYSTEM_DIALOG_ERROR_TITLE));
      mb.open();
      return;
    }

    dispose();
  }

  private void getTableName() {

    String connectionName = wConnection.getText();
    if (StringUtil.isEmpty(connectionName)) {
      return;
    }
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connectionName, variables);

    if (databaseMeta != null) {
      log.logDebug(
          toString(),
          BaseMessages.getString(
              PKG, "VerticaBulkLoaderDialog.Log.LookingAtConnection", databaseMeta.toString()));

      DatabaseExplorerDialog std =
          new DatabaseExplorerDialog(
              shell, SWT.NONE, variables, databaseMeta, pipelineMeta.getDatabases());
      std.setSelectedSchemaAndTable(wSchema.getText(), wTable.getText());
      if (std.open()) {
        wSchema.setText(Const.NVL(std.getSchemaName(), ""));
        wTable.setText(Const.NVL(std.getTableName(), ""));
      }
    } else {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.ConnectionError2.DialogMessage"));
      mb.setText(BaseMessages.getString(CONST_SYSTEM_DIALOG_ERROR_TITLE));
      mb.open();
    }
  }

  /** Fill up the fields table with the incoming fields. */
  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null && !r.isEmpty()) {
        BaseTransformDialog.getFieldsFromPrevious(
            r, wFields, 1, new int[] {1, 2}, new int[] {}, -1, -1, null);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.FailedToGetFields.DialogMessage"),
          ke); //$NON-NLS-1$ //$NON-NLS-2$
    }
  }

  // Generate code for create table...
  // Conversions done by Database
  //
  private void sql() {
    try {
      VerticaBulkLoaderMeta info = new VerticaBulkLoaderMeta();
      DatabaseMeta databaseMeta = pipelineMeta.findDatabase(wConnection.getText(), variables);

      getInfo(info);
      IRowMeta prev = pipelineMeta.getPrevTransformFields(variables, transformName);
      TransformMeta transformMeta = pipelineMeta.findTransform(transformName);

      if (info.specifyFields()) {
        // Only use the fields that were specified.
        IRowMeta prevNew = new RowMeta();

        for (int i = 0; i < info.getFields().size(); i++) {
          VerticaBulkLoaderField vbf = info.getFields().get(i);
          IValueMeta insValue = prev.searchValueMeta(vbf.getFieldStream());
          if (insValue != null) {
            IValueMeta insertValue = insValue.clone();
            insertValue.setName(vbf.getFieldDatabase());
            prevNew.addValueMeta(insertValue);
          } else {
            throw new HopTransformException(
                BaseMessages.getString(
                    PKG,
                    "VerticaBulkLoaderDialog.FailedToFindField.Message",
                    vbf.getFieldStream())); // $NON-NLS-1$
          }
        }
        prev = prevNew;
      }

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
          mb.setMessage(BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.NoSQL.DialogMessage"));
          mb.setText(BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.NoSQL.DialogTitle"));
          mb.open();
        }
      } else {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(sql.getError());
        mb.setText(BaseMessages.getString(CONST_SYSTEM_DIALOG_ERROR_TITLE));
        mb.open();
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.BuildSQLError.DialogTitle"),
          BaseMessages.getString(PKG, "VerticaBulkLoaderDialog.BuildSQLError.DialogMessage"),
          ke);
    }
  }

  @Override
  public String toString() {
    return this.getClass().getName();
  }
}
