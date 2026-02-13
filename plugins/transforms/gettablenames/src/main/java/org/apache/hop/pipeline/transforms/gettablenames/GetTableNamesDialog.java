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

package org.apache.hop.pipeline.transforms.gettablenames;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class GetTableNamesDialog extends BaseTransformDialog {
  private static final Class<?> PKG = GetTableNamesMeta.class;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private Text wTableNameField;
  private Text wSqlCreationField;
  private Button wIncludeTable;

  private Button wIncludeSchema;

  private Button wIncludeCatalog;
  private Label wlIncludeCatalog;

  private Button wIncludeProcedure;

  private Button wIncludeSynonym;

  private Button wAddSchemaInOutput;

  private Button wIncludeView;

  private Text wObjectTypeField;

  private Text wIsSystemObjectField;

  private Label wlSchemaName;
  private TextVar wSchemaName;

  private Button wDynamicSchema;

  private Label wlSchemaField;
  private CCombo wSchemaField;

  private final GetTableNamesMeta input;

  private boolean gotPreviousFields = false;

  public GetTableNamesDialog(
      Shell parent,
      IVariables variables,
      GetTableNamesMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "GetTableNamesDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).preview(e -> preview()).cancel(e -> cancel()).build();

    Control lastControl = wSpacer;

    // Connection line
    wConnection =
        addConnectionLine(shell, lastControl, input.getConnection(), e -> input.setChanged());

    // schemaname fieldname ...
    wlSchemaName = new Label(shell, SWT.RIGHT);
    wlSchemaName.setText(BaseMessages.getString(PKG, "GetTableNamesDialog.SchemaNameName.Label"));
    PropsUi.setLook(wlSchemaName);
    FormData fdlschemaname = new FormData();
    fdlschemaname.left = new FormAttachment(0, 0);
    fdlschemaname.right = new FormAttachment(middle, -margin);
    fdlschemaname.top = new FormAttachment(wConnection, margin);
    wlSchemaName.setLayoutData(fdlschemaname);
    wSchemaName = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wSchemaName.setToolTipText(
        BaseMessages.getString(PKG, "GetTableNamesDialog.SchemaNameName.Tooltip"));
    PropsUi.setLook(wSchemaName);
    FormData fdschemaname = new FormData();
    fdschemaname.left = new FormAttachment(middle, 0);
    fdschemaname.top = new FormAttachment(wConnection, margin);
    fdschemaname.right = new FormAttachment(100, 0);
    wSchemaName.setLayoutData(fdschemaname);
    ModifyListener lsModSchema = e -> refreshIncludeCatalog();
    wSchemaName.addModifyListener(lsModSchema);

    // Is schema name defined in a Field
    Label wlDynamicSchema = new Label(shell, SWT.RIGHT);
    wlDynamicSchema.setText(BaseMessages.getString(PKG, "GetTableNamesDialog.DynamicSchema.Label"));
    PropsUi.setLook(wlDynamicSchema);
    FormData fdldynamicSchema = new FormData();
    fdldynamicSchema.left = new FormAttachment(0, -margin);
    fdldynamicSchema.top = new FormAttachment(wSchemaName, margin);
    fdldynamicSchema.right = new FormAttachment(middle, -margin);
    wlDynamicSchema.setLayoutData(fdldynamicSchema);
    wDynamicSchema = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wDynamicSchema);
    wDynamicSchema.setToolTipText(
        BaseMessages.getString(PKG, "GetTableNamesDialog.DynamicSchema.Tooltip"));
    FormData fddynamicSchema = new FormData();
    fddynamicSchema.left = new FormAttachment(middle, 0);
    fddynamicSchema.top = new FormAttachment(wlDynamicSchema, 0, SWT.CENTER);
    wDynamicSchema.setLayoutData(fddynamicSchema);
    wDynamicSchema.addListener(SWT.Selection, e -> activateDynamicSchema());

    // If schema string defined in a Field
    wlSchemaField = new Label(shell, SWT.RIGHT);
    wlSchemaField.setText(BaseMessages.getString(PKG, "GetTableNamesDialog.SchemaField.Label"));
    PropsUi.setLook(wlSchemaField);
    FormData fdlSchemaField = new FormData();
    fdlSchemaField.left = new FormAttachment(0, -margin);
    fdlSchemaField.top = new FormAttachment(wDynamicSchema, margin);
    fdlSchemaField.right = new FormAttachment(middle, -margin);
    wlSchemaField.setLayoutData(fdlSchemaField);

    wSchemaField = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
    wSchemaField.setEditable(true);
    PropsUi.setLook(wSchemaField);
    FormData fdSchemaField = new FormData();
    fdSchemaField.left = new FormAttachment(middle, 0);
    fdSchemaField.top = new FormAttachment(wDynamicSchema, margin);
    fdSchemaField.right = new FormAttachment(100, -margin);
    wSchemaField.setLayoutData(fdSchemaField);
    wSchemaField.addListener(
        SWT.FocusIn,
        e -> {
          setSchemaField();
          shell.setCursor(null);
        });

    // ///////////////////////////////
    // START OF SETTINGS GROUP //
    // ///////////////////////////////

    Group wSettings = new Group(shell, SWT.SHADOW_NONE);
    PropsUi.setLook(wSettings);
    wSettings.setText(BaseMessages.getString(PKG, "GetTableNamesDialog.wSettings.Label"));

    FormLayout settingsgroupLayout = new FormLayout();
    settingsgroupLayout.marginWidth = 10;
    settingsgroupLayout.marginHeight = 10;
    wSettings.setLayout(settingsgroupLayout);

    // Include Catalogs
    wlIncludeCatalog = new Label(wSettings, SWT.RIGHT);
    wlIncludeCatalog.setText(
        BaseMessages.getString(PKG, "GetTableNamesDialog.IncludeCatalog.Label"));
    PropsUi.setLook(wlIncludeCatalog);
    FormData fdlIncludeCatalog = new FormData();
    fdlIncludeCatalog.left = new FormAttachment(0, -margin);
    fdlIncludeCatalog.top = new FormAttachment(wSchemaField, margin);
    fdlIncludeCatalog.right = new FormAttachment(middle, -margin);
    wlIncludeCatalog.setLayoutData(fdlIncludeCatalog);
    wIncludeCatalog = new Button(wSettings, SWT.CHECK);
    PropsUi.setLook(wIncludeCatalog);
    wIncludeCatalog.setToolTipText(
        BaseMessages.getString(PKG, "GetTableNamesDialog.IncludeCatalog.Tooltip"));
    FormData fdIncludeCatalog = new FormData();
    fdIncludeCatalog.left = new FormAttachment(middle, -margin);
    fdIncludeCatalog.top = new FormAttachment(wlIncludeCatalog, 0, SWT.CENTER);
    wIncludeCatalog.setLayoutData(fdIncludeCatalog);

    // Include Schemas
    Label wlIncludeSchema = new Label(wSettings, SWT.RIGHT);
    wlIncludeSchema.setText(BaseMessages.getString(PKG, "GetTableNamesDialog.IncludeSchema.Label"));
    PropsUi.setLook(wlIncludeSchema);
    FormData fdlincludeSchema = new FormData();
    fdlincludeSchema.left = new FormAttachment(0, -margin);
    fdlincludeSchema.top = new FormAttachment(wIncludeCatalog, margin);
    fdlincludeSchema.right = new FormAttachment(middle, -margin);
    wlIncludeSchema.setLayoutData(fdlincludeSchema);
    wIncludeSchema = new Button(wSettings, SWT.CHECK);
    PropsUi.setLook(wIncludeSchema);
    wIncludeSchema.setToolTipText(
        BaseMessages.getString(PKG, "GetTableNamesDialog.IncludeSchema.Tooltip"));
    FormData fdincludeSchema = new FormData();
    fdincludeSchema.left = new FormAttachment(middle, -margin);
    fdincludeSchema.top = new FormAttachment(wlIncludeSchema, 0, SWT.CENTER);
    wIncludeSchema.setLayoutData(fdincludeSchema);
    SelectionAdapter lincludeSchema =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        };
    wIncludeSchema.addSelectionListener(lincludeSchema);

    // Include tables
    Label wlIncludeTable = new Label(wSettings, SWT.RIGHT);
    wlIncludeTable.setText(BaseMessages.getString(PKG, "GetTableNamesDialog.IncludeTable.Label"));
    PropsUi.setLook(wlIncludeTable);
    FormData fdlincludeTable = new FormData();
    fdlincludeTable.left = new FormAttachment(0, -margin);
    fdlincludeTable.top = new FormAttachment(wIncludeSchema, margin);
    fdlincludeTable.right = new FormAttachment(middle, -margin);
    wlIncludeTable.setLayoutData(fdlincludeTable);
    wIncludeTable = new Button(wSettings, SWT.CHECK);
    PropsUi.setLook(wIncludeTable);
    wIncludeTable.setToolTipText(
        BaseMessages.getString(PKG, "GetTableNamesDialog.IncludeTable.Tooltip"));
    FormData fdincludeTable = new FormData();
    fdincludeTable.left = new FormAttachment(middle, -margin);
    fdincludeTable.top = new FormAttachment(wlIncludeTable, 0, SWT.CENTER);
    wIncludeTable.setLayoutData(fdincludeTable);
    SelectionAdapter lincludeTable =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        };
    wIncludeTable.addSelectionListener(lincludeTable);

    // Include views
    Label wlIncludeView = new Label(wSettings, SWT.RIGHT);
    wlIncludeView.setText(BaseMessages.getString(PKG, "GetTableNamesDialog.IncludeView.Label"));
    PropsUi.setLook(wlIncludeView);
    FormData fdlincludeView = new FormData();
    fdlincludeView.left = new FormAttachment(0, -margin);
    fdlincludeView.top = new FormAttachment(wIncludeTable, margin);
    fdlincludeView.right = new FormAttachment(middle, -margin);
    wlIncludeView.setLayoutData(fdlincludeView);
    wIncludeView = new Button(wSettings, SWT.CHECK);
    PropsUi.setLook(wIncludeView);
    wIncludeView.setToolTipText(
        BaseMessages.getString(PKG, "GetTableNamesDialog.IncludeView.Tooltip"));
    FormData fdincludeView = new FormData();
    fdincludeView.left = new FormAttachment(middle, -margin);
    fdincludeView.top = new FormAttachment(wlIncludeView, 0, SWT.CENTER);
    wIncludeView.setLayoutData(fdincludeView);
    SelectionAdapter lincludeView =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        };
    wIncludeView.addSelectionListener(lincludeView);

    // Include procedures
    Label wlIncludeProcedure = new Label(wSettings, SWT.RIGHT);
    wlIncludeProcedure.setText(
        BaseMessages.getString(PKG, "GetTableNamesDialog.IncludeProcedure.Label"));
    PropsUi.setLook(wlIncludeProcedure);
    FormData fdlincludeProcedure = new FormData();
    fdlincludeProcedure.left = new FormAttachment(0, -margin);
    fdlincludeProcedure.top = new FormAttachment(wIncludeView, margin);
    fdlincludeProcedure.right = new FormAttachment(middle, -margin);
    wlIncludeProcedure.setLayoutData(fdlincludeProcedure);
    wIncludeProcedure = new Button(wSettings, SWT.CHECK);
    PropsUi.setLook(wIncludeProcedure);
    wIncludeProcedure.setToolTipText(
        BaseMessages.getString(PKG, "GetTableNamesDialog.IncludeProcedure.Tooltip"));
    FormData fdincludeProcedure = new FormData();
    fdincludeProcedure.left = new FormAttachment(middle, -margin);
    fdincludeProcedure.top = new FormAttachment(wlIncludeProcedure, 0, SWT.CENTER);
    wIncludeProcedure.setLayoutData(fdincludeProcedure);
    SelectionAdapter lincludeProcedure =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        };
    wIncludeProcedure.addSelectionListener(lincludeProcedure);

    // Include Synonyms
    Label wlIncludeSynonym = new Label(wSettings, SWT.RIGHT);
    wlIncludeSynonym.setText(
        BaseMessages.getString(PKG, "GetTableNamesDialog.includeSynonym.Label"));
    PropsUi.setLook(wlIncludeSynonym);
    FormData fdlincludeSynonym = new FormData();
    fdlincludeSynonym.left = new FormAttachment(0, -margin);
    fdlincludeSynonym.top = new FormAttachment(wIncludeProcedure, margin);
    fdlincludeSynonym.right = new FormAttachment(middle, -margin);
    wlIncludeSynonym.setLayoutData(fdlincludeSynonym);
    wIncludeSynonym = new Button(wSettings, SWT.CHECK);
    PropsUi.setLook(wIncludeSynonym);
    wIncludeSynonym.setToolTipText(
        BaseMessages.getString(PKG, "GetTableNamesDialog.IncludeSynonym.Tooltip"));
    FormData fdincludeSynonym = new FormData();
    fdincludeSynonym.left = new FormAttachment(middle, -margin);
    fdincludeSynonym.top = new FormAttachment(wlIncludeSynonym, 0, SWT.CENTER);
    wIncludeSynonym.setLayoutData(fdincludeSynonym);
    SelectionAdapter lincludeSynonym =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        };
    wIncludeSynonym.addSelectionListener(lincludeSynonym);

    // Add schema in output
    Label wlAddSchemaInOutput = new Label(wSettings, SWT.RIGHT);
    wlAddSchemaInOutput.setText(
        BaseMessages.getString(PKG, "GetTableNamesDialog.AddSchemaInOutput.Label"));
    PropsUi.setLook(wlAddSchemaInOutput);
    FormData fdladdSchemaInOutput = new FormData();
    fdladdSchemaInOutput.left = new FormAttachment(0, -margin);
    fdladdSchemaInOutput.top = new FormAttachment(wIncludeSynonym, margin);
    fdladdSchemaInOutput.right = new FormAttachment(middle, -margin);
    wlAddSchemaInOutput.setLayoutData(fdladdSchemaInOutput);
    wAddSchemaInOutput = new Button(wSettings, SWT.CHECK);
    PropsUi.setLook(wAddSchemaInOutput);
    wAddSchemaInOutput.setToolTipText(
        BaseMessages.getString(PKG, "GetTableNamesDialog.addSchemaInOutput.Tooltip"));
    FormData fdaddSchemaInOutput = new FormData();
    fdaddSchemaInOutput.left = new FormAttachment(middle, -margin);
    fdaddSchemaInOutput.top = new FormAttachment(wlAddSchemaInOutput, 0, SWT.CENTER);
    wAddSchemaInOutput.setLayoutData(fdaddSchemaInOutput);
    SelectionAdapter laddSchemaInOutput =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        };
    wAddSchemaInOutput.addSelectionListener(laddSchemaInOutput);

    FormData fdSettings = new FormData();
    fdSettings.left = new FormAttachment(0, margin);
    fdSettings.top = new FormAttachment(wSchemaField, margin);
    fdSettings.right = new FormAttachment(100, -margin);
    wSettings.setLayoutData(fdSettings);

    // ///////////////////////////////////////////////////////////
    // / END OF SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    // ///////////////////////////////
    // START OF OutputFields GROUP //
    // ///////////////////////////////

    Group wOutputFields = new Group(shell, SWT.SHADOW_NONE);
    PropsUi.setLook(wOutputFields);
    wOutputFields.setText(BaseMessages.getString(PKG, "GetTableNamesDialog.wOutputFields.Label"));

    FormLayout outputFieldsgroupLayout = new FormLayout();
    outputFieldsgroupLayout.marginWidth = 10;
    outputFieldsgroupLayout.marginHeight = 10;
    wOutputFields.setLayout(outputFieldsgroupLayout);

    // TableNameField field name ...
    Label wlTableNameField = new Label(wOutputFields, SWT.RIGHT);
    wlTableNameField.setText(
        BaseMessages.getString(PKG, "GetTableNamesDialog.TableNameFieldName.Label"));
    PropsUi.setLook(wlTableNameField);
    FormData fdlTableNameField = new FormData();
    fdlTableNameField.left = new FormAttachment(0, 0);
    fdlTableNameField.right = new FormAttachment(middle, -margin);
    fdlTableNameField.top = new FormAttachment(wSettings, margin);
    wlTableNameField.setLayoutData(fdlTableNameField);
    wTableNameField = new Text(wOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTableNameField.setToolTipText(
        BaseMessages.getString(PKG, "GetTableNamesDialog.TableNameFieldName.Tooltip"));
    PropsUi.setLook(wTableNameField);
    FormData fdTableNameField = new FormData();
    fdTableNameField.left = new FormAttachment(middle, 0);
    fdTableNameField.top = new FormAttachment(wSettings, margin);
    fdTableNameField.right = new FormAttachment(100, 0);
    wTableNameField.setLayoutData(fdTableNameField);

    // ObjectTypeField field name ...
    Label wlObjectTypeField = new Label(wOutputFields, SWT.RIGHT);
    wlObjectTypeField.setText(
        BaseMessages.getString(PKG, "GetTableNamesDialog.ObjectTypeFieldName.Label"));
    PropsUi.setLook(wlObjectTypeField);
    FormData fdlObjectTypeField = new FormData();
    fdlObjectTypeField.left = new FormAttachment(0, 0);
    fdlObjectTypeField.right = new FormAttachment(middle, -margin);
    fdlObjectTypeField.top = new FormAttachment(wTableNameField, margin);
    wlObjectTypeField.setLayoutData(fdlObjectTypeField);
    wObjectTypeField = new Text(wOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wObjectTypeField.setToolTipText(
        BaseMessages.getString(PKG, "GetTableNamesDialog.ObjectTypeFieldName.Tooltip"));
    PropsUi.setLook(wObjectTypeField);
    FormData fdObjectTypeField = new FormData();
    fdObjectTypeField.left = new FormAttachment(middle, 0);
    fdObjectTypeField.top = new FormAttachment(wTableNameField, margin);
    fdObjectTypeField.right = new FormAttachment(100, 0);
    wObjectTypeField.setLayoutData(fdObjectTypeField);

    // isSystemObjectField field name ...
    Label wlisSystemObjectField = new Label(wOutputFields, SWT.RIGHT);
    wlisSystemObjectField.setText(
        BaseMessages.getString(PKG, "GetTableNamesDialog.IsSystemObjectFieldName.Label"));
    PropsUi.setLook(wlisSystemObjectField);
    FormData fdlisSystemObjectField = new FormData();
    fdlisSystemObjectField.left = new FormAttachment(0, 0);
    fdlisSystemObjectField.right = new FormAttachment(middle, -margin);
    fdlisSystemObjectField.top = new FormAttachment(wObjectTypeField, margin);
    wlisSystemObjectField.setLayoutData(fdlisSystemObjectField);
    wIsSystemObjectField = new Text(wOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wIsSystemObjectField.setToolTipText(
        BaseMessages.getString(PKG, "GetTableNamesDialog.IsSystemObjectFieldName.Tooltip"));
    PropsUi.setLook(wIsSystemObjectField);
    FormData fdisSystemObjectField = new FormData();
    fdisSystemObjectField.left = new FormAttachment(middle, 0);
    fdisSystemObjectField.top = new FormAttachment(wObjectTypeField, margin);
    fdisSystemObjectField.right = new FormAttachment(100, 0);
    wIsSystemObjectField.setLayoutData(fdisSystemObjectField);

    // CreationSQL field name ...
    Label wlSqlCreationField = new Label(wOutputFields, SWT.RIGHT);
    wlSqlCreationField.setText(
        BaseMessages.getString(PKG, "GetTableNamesDialog.CreationSQLName.Label"));
    PropsUi.setLook(wlSqlCreationField);
    FormData fdlSqlCreationField = new FormData();
    fdlSqlCreationField.left = new FormAttachment(0, 0);
    fdlSqlCreationField.right = new FormAttachment(middle, -margin);
    fdlSqlCreationField.top = new FormAttachment(wIsSystemObjectField, margin);
    wlSqlCreationField.setLayoutData(fdlSqlCreationField);
    wSqlCreationField = new Text(wOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wSqlCreationField.setToolTipText(
        BaseMessages.getString(PKG, "GetTableNamesDialog.CreationSQLName.Tooltip"));
    PropsUi.setLook(wSqlCreationField);
    FormData fdSqlCreationField = new FormData();
    fdSqlCreationField.left = new FormAttachment(middle, 0);
    fdSqlCreationField.top = new FormAttachment(wIsSystemObjectField, margin);
    fdSqlCreationField.right = new FormAttachment(100, 0);
    wSqlCreationField.setLayoutData(fdSqlCreationField);

    FormData fdOutputFields = new FormData();
    fdOutputFields.left = new FormAttachment(0, margin);
    fdOutputFields.top = new FormAttachment(wSettings, margin);
    fdOutputFields.right = new FormAttachment(100, -margin);
    fdOutputFields.bottom = new FormAttachment(100, -50);
    wOutputFields.setLayoutData(fdOutputFields);

    // ///////////////////////////////////////////////////////////
    // / END OF OutputFields GROUP
    // ///////////////////////////////////////////////////////////

    getData();
    activateDynamicSchema();
    refreshIncludeCatalog();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void refreshIncludeCatalog() {
    if (!Utils.isEmpty(wSchemaName.getText())) {
      wIncludeCatalog.setSelection(false);
      wlIncludeCatalog.setEnabled(false);
      wIncludeCatalog.setEnabled(false);
    } else {
      wlIncludeCatalog.setEnabled(true);
      wIncludeCatalog.setEnabled(true);
    }
  }

  private void activateDynamicSchema() {
    wlSchemaField.setEnabled(wDynamicSchema.getSelection());
    wSchemaField.setEnabled(wDynamicSchema.getSelection());
    wPreview.setEnabled(!wDynamicSchema.getSelection());
    wlSchemaName.setEnabled(!wDynamicSchema.getSelection());
    wSchemaName.setEnabled(!wDynamicSchema.getSelection());
    if (wDynamicSchema.getSelection()) {
      wIncludeCatalog.setSelection(false);
    }
    wlIncludeCatalog.setEnabled(!wDynamicSchema.getSelection());
    wIncludeCatalog.setEnabled(!wDynamicSchema.getSelection());
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (isDebug()) {
      logDebug(toString(), BaseMessages.getString(PKG, "GetTableNamesDialog.Log.GettingKeyInfo"));
    }

    if (input.getConnection() != null) {
      wConnection.setText(input.getConnection());
    }
    if (input.getSchemaName() != null) {
      wSchemaName.setText(input.getSchemaName());
    }
    if (input.getTableNameFieldName() != null) {
      wTableNameField.setText(input.getTableNameFieldName());
    }
    if (input.getObjectTypeFieldName() != null) {
      wObjectTypeField.setText(input.getObjectTypeFieldName());
    }
    if (input.isSystemObjectFieldName() != null) {
      wIsSystemObjectField.setText(input.isSystemObjectFieldName());
    }
    if (input.getSqlCreationFieldName() != null) {
      wSqlCreationField.setText(input.getSqlCreationFieldName());
    }
    wIncludeCatalog.setSelection(input.isIncludeCatalog());
    wIncludeSchema.setSelection(input.isIncludeSchema());
    wIncludeTable.setSelection(input.isIncludeTable());
    wIncludeView.setSelection(input.isIncludeView());
    wIncludeProcedure.setSelection(input.isIncludeProcedure());
    wIncludeSynonym.setSelection(input.isIncludeSynonym());
    wAddSchemaInOutput.setSelection(input.isAddSchemaInOutput());

    wDynamicSchema.setSelection(input.isDynamicSchema());
    if (input.getSchemaNameField() != null) {
      wSchemaField.setText(input.getSchemaNameField());
    }
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void setSchemaField() {
    if (!gotPreviousFields) {
      try {
        String value = wSchemaField.getText();
        wSchemaField.removeAll();

        IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
        if (r != null) {
          wSchemaField.setItems(r.getFieldNames());
        }
        if (value != null) {
          wSchemaField.setText(value);
        }
      } catch (HopException ke) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "GetTableNamesDialog.FailedToGetFields.DialogTitle"),
            BaseMessages.getString(PKG, "GetTableNamesDialog.FailedToGetFields.DialogMessage"),
            ke);
      }
      gotPreviousFields = true;
    }
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }
    transformName = wTransformName.getText(); // return value

    if (Utils.isEmpty(wConnection.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "GetTableNamesDialog.InvalidConnection.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "GetTableNamesDialog.InvalidConnection.DialogTitle"));
      mb.open();
      return;
    }

    input.setChanged();
    getInfo(input);
    dispose();
  }

  private void getInfo(GetTableNamesMeta info) {
    info.setConnection(wConnection.getText());
    info.setSchemaName(wSchemaName.getText());
    info.setTableNameFieldName(wTableNameField.getText());
    info.setSqlCreationFieldName(wSqlCreationField.getText());
    info.setObjectTypeFieldName(wObjectTypeField.getText());
    info.setIsSystemObjectFieldName(wIsSystemObjectField.getText());
    info.setIncludeCatalog(wIncludeCatalog.getSelection());
    info.setIncludeSchema(wIncludeSchema.getSelection());
    info.setIncludeTable(wIncludeTable.getSelection());
    info.setIncludeView(wIncludeView.getSelection());
    info.setIncludeProcedure(wIncludeProcedure.getSelection());
    info.setIncludeSynonym(wIncludeSynonym.getSelection());
    info.setAddSchemaInOutput(wAddSchemaInOutput.getSelection());
    info.setDynamicSchema(wDynamicSchema.getSelection());
    info.setSchemaNameField(wSchemaField.getText());
  }

  private boolean checkUserInput(GetTableNamesMeta meta) {

    if (Utils.isEmpty(meta.getConnection())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "GetTableNamesDialog.InvalidConnection.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "GetTableNamesDialog.InvalidConnection.DialogTitle"));
      mb.open();
      return false;
    }

    if (Utils.isEmpty(meta.getTableNameFieldName())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(
              PKG, "GetTableNamesDialog.Error.TableNameFieldNameMissingMessage"));
      mb.setText(
          BaseMessages.getString(PKG, "GetTableNamesDialog.Error.TableNameFieldNameMissingTitle"));
      mb.open();

      return false;
    }
    return true;
  }

  // Preview the data
  private void preview() {
    GetTableNamesMeta oneMeta = new GetTableNamesMeta();

    getInfo(oneMeta);

    if (!checkUserInput(oneMeta)) {
      return;
    }

    PipelineMeta previewMeta =
        PipelinePreviewFactory.generatePreviewPipeline(
            pipelineMeta.getMetadataProvider(), oneMeta, wTransformName.getText());

    EnterNumberDialog numberDialog =
        new EnterNumberDialog(
            shell,
            props.getDefaultPreviewSize(),
            BaseMessages.getString(PKG, "GetTableNamesDialog.PreviewSize.DialogTitle"),
            BaseMessages.getString(PKG, "GetTableNamesDialog.PreviewSize.DialogMessage"));
    int previewSize = numberDialog.open();
    if (previewSize > 0) {
      PipelinePreviewProgressDialog progressDialog =
          new PipelinePreviewProgressDialog(
              shell,
              variables,
              previewMeta,
              new String[] {wTransformName.getText()},
              new int[] {previewSize});
      progressDialog.open();

      if (!progressDialog.isCancelled()) {
        Pipeline pipeline = progressDialog.getPipeline();
        String loggingText = progressDialog.getLoggingText();

        if (pipeline.getResult() != null && pipeline.getResult().getNrErrors() > 0) {
          EnterTextDialog etd =
              new EnterTextDialog(
                  shell,
                  BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
                  BaseMessages.getString(PKG, "GetTableNamesDialog.ErrorInPreview.DialogMessage"),
                  loggingText,
                  true);
          etd.setReadOnly();
          etd.open();
        }

        PreviewRowsDialog prd =
            new PreviewRowsDialog(
                shell,
                variables,
                SWT.NONE,
                wTransformName.getText(),
                progressDialog.getPreviewRowsMeta(wTransformName.getText()),
                progressDialog.getPreviewRows(wTransformName.getText()),
                loggingText);
        prd.open();
      }
    }
  }
}
