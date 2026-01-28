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

package org.apache.hop.pipeline.transforms.cratedbbulkloader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
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
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.PasswordTextVar;
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
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class CrateDBBulkLoaderDialog extends BaseTransformDialog {

  private static final Class<?> PKG =
      CrateDBBulkLoaderMeta.class; // for i18n purposes, needed by Translator2!!
  public static final String SYSTEM_DIALOG_ERROR_TITLE = "System.Dialog.Error.Title";

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TextVar wSchema;

  private Label wlHttpEndpoint;
  private TextVar wHttpEndpoint;

  private Label wlBatchSize;
  private TextVar wBatchSize;

  private TextVar wTable;

  private Button wStreamToRemoteCsv;

  private TextVar wLocalRemoteVolumeMapping;

  // private ComboVar wLoadFromExistingFileFormat;

  private TextVar wReadFromFilename;

  private Button wUseHTTPEndpoint;

  private TableView wFields;

  private Button wGetFields;

  private Button wDoMapping;

  private CrateDBBulkLoaderMeta input;

  private Map<String, Integer> inputFields;

  private ColumnInfo[] ciFields;

  private Label wlUseSystemVars;
  private Button wUseSystemVars;
  private Label wlAccessKeyId;
  private TextVar wAccessKeyId;
  private Label wlSecretAccessKey;
  private TextVar wSecretAccessKey;

  private Button wSpecifyFields;

  private TextVar wHttpLogin;

  private TextVar wHttpPassword;

  /** List of ColumnInfo that should have the field names of the selected database table */
  private List<ColumnInfo> tableFieldColumns = new ArrayList<>();

  private Label wlHttpLogin;
  private Label wlHttpPassword;
  private Label wlReadFromFile;
  private Label wlLocalRemoteVolumeMapping;
  private Label wlStreamToRemoteCsv;

  /** Constructor. */
  public CrateDBBulkLoaderDialog(
      Shell parent,
      IVariables variables,
      CrateDBBulkLoaderMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
    inputFields = new HashMap<>();
  }

  /** Open the dialog. */
  public String open() {
    createShell(BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.DialogTitle"));

    buildButtonBar().ok(e -> ok()).sql(e -> sql()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    FocusListener lsFocusLost =
        new FocusAdapter() {
          @Override
          public void focusLost(FocusEvent arg0) {
            setTableFieldCombo();
          }
        };
    backupChanged = input.hasChanged();

    Control lastControl = wSpacer;

    // Schema line...
    Label wlSchema = new Label(shell, SWT.RIGHT);
    wlSchema.setText(
        BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.TargetSchema.Label")); // $NON-NLS-1$
    PropsUi.setLook(wlSchema);
    FormData fdlSchema = new FormData();
    fdlSchema.left = new FormAttachment(0, 0);
    fdlSchema.right = new FormAttachment(middle, -margin);
    fdlSchema.top = new FormAttachment(lastControl, margin);
    wlSchema.setLayoutData(fdlSchema);

    Button wbSchema = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSchema);
    wbSchema.setText(BaseMessages.getString("System.Button.Browse"));
    FormData fdbSchema = new FormData();
    fdbSchema.right = new FormAttachment(100, 0);
    fdbSchema.top = new FormAttachment(lastControl, margin);
    wbSchema.setLayoutData(fdbSchema);

    wSchema = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSchema);
    wSchema.addModifyListener(lsMod);
    wSchema.addFocusListener(lsFocusLost);
    FormData fdSchema = new FormData();
    fdSchema.left = new FormAttachment(middle, 0);
    fdSchema.top = new FormAttachment(lastControl, margin);
    fdSchema.right = new FormAttachment(wbSchema, -margin);
    wSchema.setLayoutData(fdSchema);

    lastControl = wSchema;

    // Table line...
    Label wlTable = new Label(shell, SWT.RIGHT);
    wlTable.setText(BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.TargetTable.Label"));
    PropsUi.setLook(wlTable);
    FormData fdlTable = new FormData();
    fdlTable.left = new FormAttachment(0, 0);
    fdlTable.right = new FormAttachment(middle, -margin);
    fdlTable.top = new FormAttachment(lastControl, margin);
    wlTable.setLayoutData(fdlTable);

    Button wbTable = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTable);
    wbTable.setText(BaseMessages.getString("System.Button.Browse"));
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment(100, 0);
    fdbTable.top = new FormAttachment(lastControl, margin);
    wbTable.setLayoutData(fdbTable);

    wTable = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTable);
    wTable.addModifyListener(lsMod);
    wTable.addFocusListener(lsFocusLost);
    FormData fdTable = new FormData();
    fdTable.top = new FormAttachment(lastControl, margin);
    fdTable.left = new FormAttachment(middle, 0);
    fdTable.right = new FormAttachment(wbTable, -margin);
    wTable.setLayoutData(fdTable);

    lastControl = wlTable;

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    addGeneralTab(wTabFolder, margin, middle, lsMod, lsFocusLost);
    addAwsAuthenticationTab(wTabFolder, margin, middle, lsMod, lsFocusLost);
    addHttpAuthenticationTab(wTabFolder, margin, middle, lsMod, lsFocusLost);
    addFieldsTab(wTabFolder, margin, lsMod);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(lastControl, margin * 4);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(100, -50);
    wTabFolder.setLayoutData(fdTabFolder);
    wTabFolder.setSelection(0);

    wGetFields.addListener(SWT.Selection, c -> get());
    wbTable.addListener(SWT.Selection, c -> getTableName());
    wbSchema.addListener(SWT.Selection, c -> getSchemaName());

    // Set the shell size, based upon previous time...
    setSize();
    getData();
    setTableFieldCombo();

    toggleHttpEndpointFlags();
    toggleSpecifyFieldsFlags();
    toggleKeysSelection();

    input.setChanged(backupChanged);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private void addFieldsTab(CTabFolder wTabFolder, int margin, ModifyListener lsMod) {

    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setText(
        BaseMessages.getString(
            PKG, "CrateDBBulkLoaderDialog.FieldsTab.TabItem.Label")); // $NON-NLS-1$

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFieldsComp);

    FormLayout fieldsCompLayout = new FormLayout();
    fieldsCompLayout.marginWidth = Const.FORM_MARGIN;
    fieldsCompLayout.marginHeight = Const.FORM_MARGIN;
    wFieldsComp.setLayout(fieldsCompLayout);

    // The fields table
    Label wlFields = new Label(wFieldsComp, SWT.NONE);
    wlFields.setText(
        BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.InsertFields.Label")); // $NON-NLS-1$
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
            BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.ColumnInfo.TableField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false); //$NON-NLS-1$
    ciFields[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.ColumnInfo.StreamField"),
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
        BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.GetFields.Button")); // $NON-NLS-1$
    FormData fdGetFields = new FormData();
    fdGetFields.top = new FormAttachment(wlFields, margin);
    fdGetFields.right = new FormAttachment(100, 0);
    wGetFields.setLayoutData(fdGetFields);

    wDoMapping = new Button(wFieldsComp, SWT.PUSH);
    wDoMapping.setText(
        BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.DoMapping.Button")); // $NON-NLS-1$
    FormData fdDoMapping = new FormData();
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

    getFieldsFromPrevious();
  }

  private void getFieldsFromPrevious() {
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
  }

  private void addAwsAuthenticationTab(
      CTabFolder wTabFolder,
      int margin,
      int middle,
      ModifyListener lsMod,
      FocusListener lsFocusLost) {

    CTabItem wOptionsTab = new CTabItem(wTabFolder, SWT.NONE);
    wOptionsTab.setText(
        BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.AWSTab.TabItem.Label")); // $NON-NLS-1$

    FormLayout optionsLayout = new FormLayout();
    optionsLayout.marginWidth = 3;
    optionsLayout.marginHeight = 3;

    Composite wOptionsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wOptionsComp);
    wOptionsComp.setLayout(optionsLayout);

    wlUseSystemVars = new Label(wOptionsComp, SWT.RIGHT);
    wlUseSystemVars.setText(
        BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.Authenticate.UseSystemVars.Label"));
    wlUseSystemVars.setToolTipText(
        BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.Authenticate.UseSystemVars.Tooltip"));
    PropsUi.setLook(wlUseSystemVars);
    FormData fdlUseSystemVars = new FormData();
    fdlUseSystemVars.top = new FormAttachment(0, margin);
    fdlUseSystemVars.left = new FormAttachment(0, 0);
    fdlUseSystemVars.right = new FormAttachment(middle, -margin);
    wlUseSystemVars.setLayoutData(fdlUseSystemVars);

    wUseSystemVars = new Button(wOptionsComp, SWT.CHECK);
    wUseSystemVars.setSelection(true);
    PropsUi.setLook(wUseSystemVars);
    FormData fdUseSystemVars = new FormData();
    fdUseSystemVars.top = new FormAttachment(0, margin);
    fdUseSystemVars.left = new FormAttachment(middle, 0);
    fdUseSystemVars.right = new FormAttachment(100, 0);
    wUseSystemVars.setLayoutData(fdUseSystemVars);

    Control lastControl = wlUseSystemVars;

    wUseSystemVars.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            toggleKeysSelection();
          }
        });

    wlAccessKeyId = new Label(wOptionsComp, SWT.RIGHT);
    wlAccessKeyId.setText("AWS_ACCESS_KEY_ID");
    PropsUi.setLook(wlAccessKeyId);
    FormData fdlAccessKeyId = new FormData();
    fdlAccessKeyId.top = new FormAttachment(lastControl, margin);
    fdlAccessKeyId.left = new FormAttachment(0, 0);
    fdlAccessKeyId.right = new FormAttachment(middle, -margin);
    wlAccessKeyId.setLayoutData(fdlAccessKeyId);
    wAccessKeyId = new TextVar(variables, wOptionsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wAccessKeyId);
    FormData fdUseAccessKeyId = new FormData();
    fdUseAccessKeyId.top = new FormAttachment(lastControl, margin);
    fdUseAccessKeyId.left = new FormAttachment(middle, 0);
    fdUseAccessKeyId.right = new FormAttachment(100, 0);
    wAccessKeyId.setLayoutData(fdUseAccessKeyId);
    wAccessKeyId.addModifyListener(lsMod);

    lastControl = wAccessKeyId;

    wlSecretAccessKey = new Label(wOptionsComp, SWT.RIGHT);
    wlSecretAccessKey.setText("AWS_SECRET_ACCESS_KEY");
    PropsUi.setLook(wlSecretAccessKey);
    FormData fdlSecretAccessKey = new FormData();
    fdlSecretAccessKey.top = new FormAttachment(lastControl, margin);
    fdlSecretAccessKey.left = new FormAttachment(0, 0);
    fdlSecretAccessKey.right = new FormAttachment(middle, -margin);
    wlSecretAccessKey.setLayoutData(fdlSecretAccessKey);
    wSecretAccessKey =
        new TextVar(variables, wOptionsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.PASSWORD);
    PropsUi.setLook(wSecretAccessKey);
    FormData fdSecretAccessKey = new FormData();
    fdSecretAccessKey.top = new FormAttachment(lastControl, margin);
    fdSecretAccessKey.left = new FormAttachment(middle, 0);
    fdSecretAccessKey.right = new FormAttachment(100, 0);
    wSecretAccessKey.setLayoutData(fdSecretAccessKey);
    wSecretAccessKey.addModifyListener(lsMod);

    FormData fdOptionsComp = new FormData();
    fdOptionsComp.left = new FormAttachment(0, 0);
    fdOptionsComp.top = new FormAttachment(0, 0);
    fdOptionsComp.right = new FormAttachment(100, 0);
    fdOptionsComp.bottom = new FormAttachment(100, 0);
    wOptionsComp.setLayoutData(fdOptionsComp);

    wOptionsComp.layout();
    wOptionsTab.setControl(wOptionsComp);
  }

  private void addHttpAuthenticationTab(
      CTabFolder wTabFolder,
      int margin,
      int middle,
      ModifyListener lsMod,
      FocusListener lsFocusLost) {

    CTabItem wHttpTab = new CTabItem(wTabFolder, SWT.NONE);
    wHttpTab.setText(
        BaseMessages.getString(
            PKG, "CrateDBBulkLoaderDialog.HttpTab.TabItem.Label")); // $NON-NLS-1$

    FormLayout httpLayout = new FormLayout();
    httpLayout.marginWidth = 3;
    httpLayout.marginHeight = 3;

    Composite wHttpComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wHttpComp);
    wHttpComp.setLayout(httpLayout);

    wlHttpLogin = new Label(wHttpComp, SWT.RIGHT);
    wlHttpLogin.setText(BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.HttpLogin.Label"));
    PropsUi.setLook(wlHttpLogin);
    FormData fdlHttpLogin = new FormData();
    fdlHttpLogin.top = new FormAttachment(0, margin);
    fdlHttpLogin.left = new FormAttachment(0, 0);
    fdlHttpLogin.right = new FormAttachment(middle, -margin);
    wlHttpLogin.setLayoutData(fdlHttpLogin);
    wHttpLogin = new TextVar(variables, wHttpComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wHttpLogin.addModifyListener(lsMod);
    wHttpLogin.setToolTipText(
        BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.HttpLogin.Tooltip"));
    PropsUi.setLook(wHttpLogin);
    FormData fdHttpLogin = new FormData();
    fdHttpLogin.top = new FormAttachment(0, margin);
    fdHttpLogin.left = new FormAttachment(middle, 0);
    fdHttpLogin.right = new FormAttachment(100, 0);
    wHttpLogin.setLayoutData(fdHttpLogin);

    Control lastControl = wlHttpLogin;

    wlHttpPassword = new Label(wHttpComp, SWT.RIGHT);
    wlHttpPassword.setText(
        BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.HttpPassword.Label"));
    PropsUi.setLook(wlHttpPassword);
    FormData fdlHttpPassword = new FormData();
    fdlHttpPassword.top = new FormAttachment(lastControl, margin);
    fdlHttpPassword.left = new FormAttachment(0, 0);
    fdlHttpPassword.right = new FormAttachment(middle, -margin);
    wlHttpPassword.setLayoutData(fdlHttpPassword);
    wHttpPassword = new PasswordTextVar(variables, wHttpComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wHttpPassword.addModifyListener(lsMod);
    wHttpPassword.setToolTipText(
        BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.HttpPassword.Tooltip"));
    PropsUi.setLook(wHttpPassword);
    FormData fdHttpPassword = new FormData();
    fdHttpPassword.top = new FormAttachment(lastControl, margin);
    fdHttpPassword.left = new FormAttachment(middle, 0);
    fdHttpPassword.right = new FormAttachment(100, 0);
    wHttpPassword.setLayoutData(fdHttpPassword);

    FormData fdHttpComp = new FormData();
    fdHttpComp.left = new FormAttachment(0, 0);
    fdHttpComp.top = new FormAttachment(0, 0);
    fdHttpComp.right = new FormAttachment(100, 0);
    fdHttpComp.bottom = new FormAttachment(100, 0);
    wHttpComp.setLayoutData(fdHttpComp);

    wHttpComp.layout();
    wHttpTab.setControl(wHttpComp);
  }

  private void addGeneralTab(
      CTabFolder wTabFolder,
      int margin,
      int middle,
      ModifyListener lsMod,
      FocusListener lsFocusLost) {

    CTabItem wMainTab = new CTabItem(wTabFolder, SWT.NONE);
    wMainTab.setText(
        BaseMessages.getString(
            PKG, "CrateDBBulkLoaderDialog.MainTab.TabItem.Label")); // $NON-NLS-1$

    FormLayout mainLayout = new FormLayout();
    mainLayout.marginWidth = 3;
    mainLayout.marginHeight = 3;

    Composite wMainComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wMainComp);
    wMainComp.setLayout(mainLayout);

    FormData fdMainComp = new FormData();
    fdMainComp.left = new FormAttachment(0, 0);
    fdMainComp.top = new FormAttachment(0, 0);
    fdMainComp.right = new FormAttachment(100, 0);
    fdMainComp.bottom = new FormAttachment(100, 0);
    wMainComp.setLayoutData(fdMainComp);

    // Connection line
    DatabaseMeta dbm = pipelineMeta.findDatabase(input.getConnection(), variables);
    wConnection = addConnectionLine(wMainComp, wSpacer, input.getDatabaseMeta(), null);
    wConnection.addModifyListener(lsMod);

    Control lastControl = wConnection;

    SelectionAdapter lsSelMod =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        };

    // Use HTTP Endpoint
    Label wlUseHTTPEndpoint = new Label(wMainComp, SWT.RIGHT);
    wlUseHTTPEndpoint.setText(
        BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.UseHTTPEndpoint.Label"));
    PropsUi.setLook(wlUseHTTPEndpoint);
    FormData fdlUseHttpEndpoint = new FormData();
    fdlUseHttpEndpoint.top = new FormAttachment(lastControl, margin);
    fdlUseHttpEndpoint.left = new FormAttachment(0, 0);
    fdlUseHttpEndpoint.right = new FormAttachment(middle, -margin);
    wlUseHTTPEndpoint.setLayoutData(fdlUseHttpEndpoint);

    wUseHTTPEndpoint = new Button(wMainComp, SWT.CHECK);
    PropsUi.setLook(wUseHTTPEndpoint);
    FormData fdUseHTTPEndpoint = new FormData();
    fdUseHTTPEndpoint.top = new FormAttachment(lastControl, margin);
    fdUseHTTPEndpoint.left = new FormAttachment(middle, 0);
    fdUseHTTPEndpoint.right = new FormAttachment(100, 0);
    wUseHTTPEndpoint.setLayoutData(fdUseHTTPEndpoint);
    wUseHTTPEndpoint.addSelectionListener(lsSelMod);
    wUseHTTPEndpoint.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            toggleHttpEndpointFlags();
          }
        });

    lastControl = wlUseHTTPEndpoint;

    // HttpEndpoint line...
    wlHttpEndpoint = new Label(wMainComp, SWT.RIGHT);
    wlHttpEndpoint.setText(
        BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.HTTPEndpoint.Label")); // $NON-NLS-1$
    PropsUi.setLook(wlHttpEndpoint);
    FormData fdlHttpEndpoint = new FormData();
    fdlHttpEndpoint.left = new FormAttachment(0, 0);
    fdlHttpEndpoint.right = new FormAttachment(middle, -margin);
    fdlHttpEndpoint.top = new FormAttachment(lastControl, margin);
    wlHttpEndpoint.setLayoutData(fdlHttpEndpoint);

    wHttpEndpoint = new TextVar(variables, wMainComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wHttpEndpoint);
    FormData fdHttpEndpoint = new FormData();
    fdHttpEndpoint.left = new FormAttachment(middle, 0);
    fdHttpEndpoint.top = new FormAttachment(lastControl, margin);
    fdHttpEndpoint.right = new FormAttachment(100, 0);
    wHttpEndpoint.addModifyListener(lsMod);
    wHttpEndpoint.addFocusListener(lsFocusLost);
    wHttpEndpoint.setLayoutData(fdHttpEndpoint);

    lastControl = wHttpEndpoint;

    // Batch Size
    wlBatchSize = new Label(wMainComp, SWT.RIGHT);
    wlBatchSize.setText(
        BaseMessages.getString(
            PKG, "CrateDBBulkLoaderDialog.HTTPEndpointBatchSize.Label")); // $NON-NLS-1$
    PropsUi.setLook(wlBatchSize);
    FormData fdlBatchSize = new FormData();
    fdlBatchSize.left = new FormAttachment(0, 0);
    fdlBatchSize.right = new FormAttachment(middle, -margin);
    fdlBatchSize.top = new FormAttachment(lastControl, margin);
    wlBatchSize.setLayoutData(fdlBatchSize);

    wBatchSize = new TextVar(variables, wMainComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wBatchSize);
    wBatchSize.addModifyListener(lsMod);
    wBatchSize.addFocusListener(lsFocusLost);
    FormData fdBatchSize = new FormData();
    fdBatchSize.left = new FormAttachment(middle, 0);
    fdBatchSize.top = new FormAttachment(lastControl, margin);
    fdBatchSize.right = new FormAttachment(100, 0);
    wBatchSize.setLayoutData(fdBatchSize);

    lastControl = wBatchSize;

    // Specify fields
    Label wlSpecifyFields = new Label(wMainComp, SWT.RIGHT);
    wlSpecifyFields.setText(
        BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.SpecifyFields.Label"));
    PropsUi.setLook(wlSpecifyFields);
    FormData fdlSpecifyFields = new FormData();
    fdlSpecifyFields.top = new FormAttachment(lastControl, margin);
    fdlSpecifyFields.left = new FormAttachment(0, 0);
    fdlSpecifyFields.right = new FormAttachment(middle, -margin);
    wlSpecifyFields.setLayoutData(fdlSpecifyFields);
    wSpecifyFields = new Button(wMainComp, SWT.CHECK);
    PropsUi.setLook(wSpecifyFields);
    FormData fdSpecifyFields = new FormData();
    fdSpecifyFields.top = new FormAttachment(lastControl, margin);
    fdSpecifyFields.left = new FormAttachment(middle, 0);
    fdSpecifyFields.right = new FormAttachment(100, 0);
    wSpecifyFields.setLayoutData(fdSpecifyFields);
    wSpecifyFields.addSelectionListener(lsSelMod);
    wSpecifyFields.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            toggleSpecifyFieldsFlags();
          }
        });

    lastControl = wSpecifyFields;

    wlStreamToRemoteCsv = new Label(wMainComp, SWT.RIGHT);
    wlStreamToRemoteCsv.setText(
        BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.StreamCsvToRemote.Label"));
    wlStreamToRemoteCsv.setToolTipText(
        BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.StreamCsvToRemote.Tooltip"));
    PropsUi.setLook(wlStreamToRemoteCsv);
    FormData fdlStreamCsvToRemote = new FormData();
    fdlStreamCsvToRemote.top = new FormAttachment(lastControl, margin);
    fdlStreamCsvToRemote.left = new FormAttachment(0, 0);
    fdlStreamCsvToRemote.right = new FormAttachment(middle, -margin);
    wlStreamToRemoteCsv.setLayoutData(fdlStreamCsvToRemote);

    wStreamToRemoteCsv = new Button(wMainComp, SWT.CHECK);
    wStreamToRemoteCsv.setToolTipText(
        BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.StreamCsvToRemote.ToolTip"));
    PropsUi.setLook(wStreamToRemoteCsv);
    FormData fdStreamCsvToRemote = new FormData();
    fdStreamCsvToRemote.top = new FormAttachment(lastControl, margin);
    fdStreamCsvToRemote.left = new FormAttachment(middle, 0);
    fdStreamCsvToRemote.right = new FormAttachment(100, 0);
    wStreamToRemoteCsv.setLayoutData(fdStreamCsvToRemote);
    wStreamToRemoteCsv.setSelection(true);

    lastControl = wStreamToRemoteCsv;

    wlLocalRemoteVolumeMapping = new Label(wMainComp, SWT.RIGHT);
    wlLocalRemoteVolumeMapping.setText(
        BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.LocalRemoteVolumeMapping.Label"));
    wlLocalRemoteVolumeMapping.setToolTipText(
        BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.LocalRemoteVolumeMapping.Tooltip"));
    PropsUi.setLook(wlLocalRemoteVolumeMapping);
    FormData fdlLocalRemoteVolumeMapping = new FormData();
    fdlLocalRemoteVolumeMapping.top = new FormAttachment(lastControl, margin);
    fdlLocalRemoteVolumeMapping.left = new FormAttachment(0, 0);
    fdlLocalRemoteVolumeMapping.right = new FormAttachment(middle, -margin);
    wlLocalRemoteVolumeMapping.setLayoutData(fdlLocalRemoteVolumeMapping);

    wLocalRemoteVolumeMapping =
        new TextVar(variables, wMainComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wLocalRemoteVolumeMapping);
    wLocalRemoteVolumeMapping
        .getTextWidget()
        .setMessage(
            BaseMessages.getString(
                PKG, "CrateDBBulkLoaderDialog.LocalRemoteVolumeMapping.Message"));

    FormData fdLocalRemoteVolumeMapping = new FormData();
    fdLocalRemoteVolumeMapping.top = new FormAttachment(lastControl, margin);
    fdLocalRemoteVolumeMapping.left = new FormAttachment(middle, 0);
    fdLocalRemoteVolumeMapping.right = new FormAttachment(100, -margin);
    wLocalRemoteVolumeMapping.setLayoutData(fdLocalRemoteVolumeMapping);

    lastControl = wLocalRemoteVolumeMapping;

    wlReadFromFile = new Label(wMainComp, SWT.RIGHT);
    wlReadFromFile.setText(
        BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.ReadFromFile.Label"));
    wlReadFromFile.setToolTipText(
        BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.ReadFromFile.Tooltip"));
    PropsUi.setLook(wlReadFromFile);
    FormData fdlReadFromFile = new FormData();
    fdlReadFromFile.top = new FormAttachment(lastControl, margin);
    fdlReadFromFile.left = new FormAttachment(0, 0);
    fdlReadFromFile.right = new FormAttachment(middle, -margin);
    wlReadFromFile.setLayoutData(fdlReadFromFile);

    wReadFromFilename = new TextVar(variables, wMainComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wReadFromFilename);
    wReadFromFilename.addModifyListener(lsMod);
    wReadFromFilename.addFocusListener(lsFocusLost);
    wReadFromFilename
        .getTextWidget()
        .setMessage(BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.ReadFromFile.Message"));

    FormData fdReadFromFile = new FormData();
    fdReadFromFile.top = new FormAttachment(lastControl, margin);
    fdReadFromFile.left = new FormAttachment(middle, 0);
    fdReadFromFile.right = new FormAttachment(100, -margin);
    wReadFromFilename.setLayoutData(fdReadFromFile);

    wMainComp.layout();
    wMainTab.setControl(wMainComp);
  }

  public void toggleHttpEndpointFlags() {
    final boolean useHTTPEndpoint = wUseHTTPEndpoint.getSelection();
    wlHttpEndpoint.setEnabled(useHTTPEndpoint);
    wHttpEndpoint.setEnabled(useHTTPEndpoint);
    wlBatchSize.setEnabled(useHTTPEndpoint);
    wBatchSize.setEnabled(useHTTPEndpoint);
    wlHttpLogin.setEnabled(useHTTPEndpoint);
    wHttpLogin.setEnabled(useHTTPEndpoint);
    wlHttpPassword.setEnabled(useHTTPEndpoint);
    wHttpPassword.setEnabled(useHTTPEndpoint);

    wlStreamToRemoteCsv.setVisible(!useHTTPEndpoint);
    wStreamToRemoteCsv.setVisible(!useHTTPEndpoint);

    wlLocalRemoteVolumeMapping.setVisible(!useHTTPEndpoint);
    wLocalRemoteVolumeMapping.setVisible(!useHTTPEndpoint);

    wlReadFromFile.setVisible(!useHTTPEndpoint);
    wReadFromFilename.setVisible(!useHTTPEndpoint);
  }

  public void toggleSpecifyFieldsFlags() {
    boolean specifyFields = wSpecifyFields.getSelection();
    wFields.setEnabled(specifyFields);
    wGetFields.setEnabled(specifyFields);
    wDoMapping.setEnabled(specifyFields);
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
              PKG, "CrateDBBulkLoaderDialog.DoMapping.UnableToFindSourceFields.Title"),
          BaseMessages.getString(
              PKG, "CrateDBBulkLoaderDialog.DoMapping.UnableToFindSourceFields.Message"),
          e);
      return;
    }

    // refresh data
    input.setTablename(variables.resolve(wTable.getText()));
    ITransformMeta transformMeta = this.transformMeta.getTransform();
    if (StringUtils.isEmpty(input.getConnection())) {
      input.setConnection(wConnection.getText());
    }
    try {
      targetFields = transformMeta.getRequiredFields(variables);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(
              PKG, "CrateDBBulkLoaderDialog.DoMapping.UnableToFindTargetFields.Title"),
          BaseMessages.getString(
              PKG, "CrateDBBulkLoaderDialog.DoMapping.UnableToFindTargetFields.Message"),
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
                    "CrateDBBulkLoaderDialog.DoMapping.SomeSourceFieldsNotFound",
                    missingSourceFields.toString())
                + Const.CR;
      }
      if (!missingTargetFields.isEmpty()) {
        message +=
            BaseMessages.getString(
                    PKG,
                    "CrateDBBulkLoaderDialog.DoMapping.SomeTargetFieldsNotFound",
                    missingSourceFields.toString())
                + Const.CR;
      }
      message += Const.CR;
      message +=
          BaseMessages.getString(
                  PKG, "CrateDBBulkLoaderDialog.DoMapping.SomeFieldsNotFoundContinue")
              + Const.CR;
      int answer =
          BaseDialog.openMessageBox(
              shell,
              BaseMessages.getString(
                  PKG, "CrateDBBulkLoaderDialog.DoMapping.SomeFieldsNotFoundTitle"),
              message,
              SWT.ICON_QUESTION | SWT.YES | SWT.NO);
      boolean goOn = (answer & SWT.YES) != 0;
      if (!goOn) {
        return;
      }
    }
    EnterMappingDialog d =
        new EnterMappingDialog(
            CrateDBBulkLoaderDialog.this.shell,
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

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (!StringUtils.isEmpty(input.getConnection())) {
      wConnection.setText(input.getConnection());
    }
    if (!StringUtils.isEmpty(input.getSchemaName())) {
      wSchema.setText(input.getSchemaName());
    }
    if (!StringUtils.isEmpty(input.getTableName())) {
      wTable.setText(input.getTableName());
    }

    wUseHTTPEndpoint.setSelection(input.isUseHttpEndpoint());

    if (input.isUseHttpEndpoint()) {
      wHttpEndpoint.setText(input.getHttpEndpoint());
      wBatchSize.setText(Const.NVL(input.getBatchSize(), ""));
      wHttpLogin.setText(Const.NVL(input.getHttpLogin(), ""));
      wHttpPassword.setText(Const.NVL(input.getHttpPassword(), ""));
    }

    wUseSystemVars.setSelection(input.isUseSystemEnvVars());
    if (!input.isUseSystemEnvVars()) {
      if (!StringUtil.isEmpty(input.getAwsAccessKeyId())) {
        wAccessKeyId.setText(input.getAwsAccessKeyId());
      }
      if (!StringUtils.isEmpty(input.getAwsSecretAccessKey())) {
        wSecretAccessKey.setText(input.getAwsSecretAccessKey());
      }
    }

    wStreamToRemoteCsv.setSelection(input.isStreamToS3Csv());
    if (!StringUtils.isEmpty(input.getReadFromFilename())) {
      wReadFromFilename.setText(input.getReadFromFilename());
    }

    if (!StringUtils.isEmpty(input.getVolumeMapping())) {
      wLocalRemoteVolumeMapping.setText(input.getVolumeMapping());
    }

    // wTruncate.setSelection(input.isTruncateTable());
    // wOnlyWhenHaveRows.setSelection(input.isOnlyWhenHaveRows());

    wSpecifyFields.setSelection(input.specifyFields());

    for (int i = 0; i < input.getFields().size(); i++) {
      CrateDBBulkLoaderField vbf = input.getFields().get(i);
      TableItem item = wFields.table.getItem(i);
      if (vbf.getDatabaseField() != null) {
        item.setText(1, vbf.getDatabaseField());
      }
      if (vbf.getStreamField() != null) {
        item.setText(2, vbf.getStreamField());
      }
    }
  }

  private void cancel() {
    transformName = null;
    input.setChanged(backupChanged);
    dispose();
  }

  private void getInfo(CrateDBBulkLoaderMeta info) {

    if (!StringUtils.isEmpty(wConnection.getText())) {
      info.setConnection(wConnection.getText());
    }

    if (!StringUtils.isEmpty(wSchema.getText())) {
      info.setSchemaName(wSchema.getText());
    }

    if (!StringUtils.isEmpty(wTable.getText())) {
      info.setTablename(wTable.getText());
    }

    info.setUseHttpEndpoint(wUseHTTPEndpoint.getSelection());
    if (wUseHTTPEndpoint.getSelection()) {
      info.setHttpEndpoint(wHttpEndpoint.getText());
      info.setBatchSize(wBatchSize.getText());
      info.setHttpLogin(wHttpLogin.getText());
      info.setHttpPassword(wHttpPassword.getText());
    }

    if (wUseSystemVars.getSelection()) {
      info.setUseSystemEnvVars(wUseSystemVars.getSelection());
    } else {
      info.setUseSystemEnvVars(wUseSystemVars.getSelection());
      if (!StringUtils.isEmpty(wAccessKeyId.getText())) {
        info.setAwsAccessKeyId(wAccessKeyId.getText());
      }
      if (!StringUtil.isEmpty(wSecretAccessKey.getText())) {
        info.setAwsSecretAccessKey(wSecretAccessKey.getText());
      }
    }
    // info.setTruncateTable(wTruncate.getSelection());
    // info.setOnlyWhenHaveRows(wOnlyWhenHaveRows.getSelection());
    info.setStreamToS3Csv(wStreamToRemoteCsv.getSelection());

    if (!StringUtils.isEmpty(wReadFromFilename.getText())) {
      info.setReadFromFilename(wReadFromFilename.getText());
    }

    info.setSpecifyFields(wSpecifyFields.getSelection());

    info.setVolumeMapping(wLocalRemoteVolumeMapping.getText());

    int nrRows = wFields.nrNonEmpty();
    info.getFields().clear();

    for (int i = 0; i < nrRows; i++) {
      TableItem item = wFields.getNonEmpty(i);
      CrateDBBulkLoaderField vbf =
          new CrateDBBulkLoaderField(
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
          BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.ConnectionError.DialogMessage"));
      mb.setText(BaseMessages.getString(SYSTEM_DIALOG_ERROR_TITLE));
      mb.open();
      return;
    }

    dispose();
  }

  private void getSchemaName() {
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(wConnection.getText(), variables);
    if (databaseMeta != null) {
      try (Database database = new Database(loggingObject, variables, databaseMeta)) {
        database.connect();
        String[] schemas = database.getSchemas();

        if (null != schemas && schemas.length > 0) {
          EnterSelectionDialog dialog =
              new EnterSelectionDialog(
                  shell,
                  schemas,
                  BaseMessages.getString(
                      PKG, "CrateDBBulkLoaderDialog.AvailableSchemas.Title", wConnection.getText()),
                  BaseMessages.getString(
                      PKG,
                      "CrateDBBulkLoaderDialog.AvailableSchemas.Message",
                      wConnection.getText()));
          String d = dialog.open();
          if (d != null) {
            wSchema.setText(Const.NVL(d, ""));
            setTableFieldCombo();
          }

        } else {
          org.apache.hop.ui.core.dialog.MessageBox mb =
              new org.apache.hop.ui.core.dialog.MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
          mb.setMessage(BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.NoSchema.Error"));
          mb.setText(BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.GetSchemas.Error"));
          mb.open();
        }
      } catch (Exception e) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, SYSTEM_DIALOG_ERROR_TITLE),
            BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.ErrorGettingSchemas"),
            e);
      }
    }
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
              PKG, "CrateDBBulkLoaderDialog.Log.LookingAtConnection", databaseMeta.toString()));

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
          BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.ConnectionError2.DialogMessage"));
      mb.setText(BaseMessages.getString(SYSTEM_DIALOG_ERROR_TITLE));
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
          BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.FailedToGetFields.DialogMessage"),
          ke); //$NON-NLS-1$ //$NON-NLS-2$
    }
  }

  // Generate code for create table...
  // Conversions done by Database
  //
  private void sql() {
    try {
      CrateDBBulkLoaderMeta info = new CrateDBBulkLoaderMeta();
      DatabaseMeta databaseMeta = pipelineMeta.findDatabase(wConnection.getText(), variables);

      getInfo(info);
      IRowMeta prev = pipelineMeta.getPrevTransformFields(variables, transformName);
      TransformMeta transformMeta = pipelineMeta.findTransform(transformName);

      if (info.specifyFields()) {
        // Only use the fields that were specified.
        IRowMeta prevNew = new RowMeta();

        for (int i = 0; i < info.getFields().size(); i++) {
          CrateDBBulkLoaderField vbf = info.getFields().get(i);
          IValueMeta insValue = prev.searchValueMeta(vbf.getStreamField());
          if (insValue != null) {
            IValueMeta insertValue = insValue.clone();
            insertValue.setName(vbf.getDatabaseField());
            prevNew.addValueMeta(insertValue);
          } else {
            throw new HopTransformException(
                BaseMessages.getString(
                    PKG,
                    "CrateDBBulkLoaderDialog.FailedToFindField.Message",
                    vbf.getStreamField())); // $NON-NLS-1$
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
          mb.setMessage(BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.NoSQL.DialogMessage"));
          mb.setText(BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.NoSQL.DialogTitle"));
          mb.open();
        }
      } else {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(sql.getError());
        mb.setText(BaseMessages.getString(SYSTEM_DIALOG_ERROR_TITLE));
        mb.open();
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.BuildSQLError.DialogTitle"),
          BaseMessages.getString(PKG, "CrateDBBulkLoaderDialog.BuildSQLError.DialogMessage"),
          ke);
    }
  }

  @Override
  public String toString() {
    return this.getClass().getName();
  }

  public void toggleKeysSelection() {
    wlAccessKeyId.setEnabled(!wUseSystemVars.getSelection());
    wAccessKeyId.setEnabled(!wUseSystemVars.getSelection());
    wlSecretAccessKey.setEnabled(!wUseSystemVars.getSelection());
    wSecretAccessKey.setEnabled(!wUseSystemVars.getSelection());
  }
}
