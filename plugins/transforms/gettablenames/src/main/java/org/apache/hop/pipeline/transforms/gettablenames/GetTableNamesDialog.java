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

package org.apache.hop.pipeline.transforms.gettablenames;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class GetTableNamesDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = GetTableNamesMeta.class; // For Translator

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private Text wTablenameField;
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

  public GetTableNamesDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (GetTableNamesMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "GetTableNamesDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // THE BUTTONS
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wPreview = new Button( shell, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( PKG, "GetTableNamesDialog.Preview.Button" ) );
    wPreview.addListener( SWT.Selection, e -> preview() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    setButtonPositions( new Button[] { wOk, wPreview, wCancel }, margin, null );

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "GetTableNamesDialog.TransformName.Label" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.right = new FormAttachment( middle, -margin );
    fdlTransformName.top = new FormAttachment( 0, margin );
    wlTransformName.setLayoutData( fdlTransformName );
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setText( transformName );
    props.setLook( wTransformName );
    wTransformName.addModifyListener( lsMod );
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment( middle, 0 );
    fdTransformName.top = new FormAttachment( 0, margin );
    fdTransformName.right = new FormAttachment( 100, 0 );
    wTransformName.setLayoutData( fdTransformName );

    // Connection line
    wConnection = addConnectionLine( shell, wTransformName, input.getDatabase(), lsMod );

    // schemaname fieldname ...
    wlSchemaName = new Label( shell, SWT.RIGHT );
    wlSchemaName.setText( BaseMessages.getString( PKG, "GetTableNamesDialog.schemanameName.Label" ) );
    props.setLook( wlSchemaName );
    FormData fdlschemaname = new FormData();
    fdlschemaname.left = new FormAttachment( 0, 0 );
    fdlschemaname.right = new FormAttachment( middle, -margin );
    fdlschemaname.top = new FormAttachment( wConnection, 2 * margin );
    wlSchemaName.setLayoutData( fdlschemaname );
    wSchemaName = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wSchemaName.setToolTipText( BaseMessages.getString( PKG, "GetTableNamesDialog.schemanameName.Tooltip" ) );
    props.setLook( wSchemaName );
    FormData fdschemaname = new FormData();
    fdschemaname.left = new FormAttachment( middle, 0 );
    fdschemaname.top = new FormAttachment( wConnection, 2 * margin );
    fdschemaname.right = new FormAttachment( 100, 0 );
    wSchemaName.setLayoutData( fdschemaname );
    ModifyListener lsModSchema = e -> {
      input.setChanged();
      refreshIncludeCatalog();
    };
    wSchemaName.addModifyListener( lsModSchema );

    // Is schema name defined in a Field
    Label wlDynamicSchema = new Label( shell, SWT.RIGHT );
    wlDynamicSchema.setText( BaseMessages.getString( PKG, "GetTableNamesDialog.wldynamicSchema.Label" ) );
    props.setLook( wlDynamicSchema );
    FormData fdldynamicSchema = new FormData();
    fdldynamicSchema.left = new FormAttachment( 0, -margin );
    fdldynamicSchema.top = new FormAttachment( wSchemaName, margin );
    fdldynamicSchema.right = new FormAttachment( middle, -margin );
    wlDynamicSchema.setLayoutData( fdldynamicSchema );
    wDynamicSchema = new Button( shell, SWT.CHECK );
    props.setLook( wDynamicSchema );
    wDynamicSchema.setToolTipText( BaseMessages.getString( PKG, "GetTableNamesDialog.wdynamicSchema.Tooltip" ) );
    FormData fddynamicSchema = new FormData();
    fddynamicSchema.left = new FormAttachment( middle, 0 );
    fddynamicSchema.top = new FormAttachment( wlDynamicSchema, 0, SWT.CENTER );
    wDynamicSchema.setLayoutData( fddynamicSchema );
    SelectionAdapter lsxmlstream = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        ActivedynamicSchema();
        input.setChanged();
      }
    };
    wDynamicSchema.addSelectionListener( lsxmlstream );

    // If schema string defined in a Field
    wlSchemaField = new Label( shell, SWT.RIGHT );
    wlSchemaField.setText( BaseMessages.getString( PKG, "GetTableNamesDialog.wlSchemaField.Label" ) );
    props.setLook( wlSchemaField );
    FormData fdlSchemaField = new FormData();
    fdlSchemaField.left = new FormAttachment( 0, -margin );
    fdlSchemaField.top = new FormAttachment( wDynamicSchema, margin );
    fdlSchemaField.right = new FormAttachment( middle, -margin );
    wlSchemaField.setLayoutData( fdlSchemaField );

    wSchemaField = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    wSchemaField.setEditable( true );
    props.setLook( wSchemaField );
    wSchemaField.addModifyListener( lsMod );
    FormData fdSchemaField = new FormData();
    fdSchemaField.left = new FormAttachment( middle, 0 );
    fdSchemaField.top = new FormAttachment( wDynamicSchema, margin );
    fdSchemaField.right = new FormAttachment( 100, -margin );
    wSchemaField.setLayoutData( fdSchemaField );
    wSchemaField.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        setSchemaField();
        shell.setCursor( null );
      }
    } );

    // ///////////////////////////////
    // START OF SETTINGS GROUP //
    // ///////////////////////////////

    Group wSettings = new Group( shell, SWT.SHADOW_NONE );
    props.setLook( wSettings );
    wSettings.setText( BaseMessages.getString( PKG, "GetTableNamesDialog.wSettings.Label" ) );

    FormLayout SettingsgroupLayout = new FormLayout();
    SettingsgroupLayout.marginWidth = 10;
    SettingsgroupLayout.marginHeight = 10;
    wSettings.setLayout( SettingsgroupLayout );

    // Include Catalogs
    wlIncludeCatalog = new Label( wSettings, SWT.RIGHT );
    wlIncludeCatalog.setText( BaseMessages.getString( PKG, "GetCatalogNamesDialog.includeCatalog.Label" ) );
    props.setLook( wlIncludeCatalog );
    FormData fdlincludeCatalog = new FormData();
    fdlincludeCatalog.left = new FormAttachment( 0, -margin );
    fdlincludeCatalog.top = new FormAttachment( wSchemaField, margin );
    fdlincludeCatalog.right = new FormAttachment( middle, -2 * margin );
    wlIncludeCatalog.setLayoutData( fdlincludeCatalog );
    wIncludeCatalog = new Button( wSettings, SWT.CHECK );
    props.setLook( wIncludeCatalog );
    wIncludeCatalog.setToolTipText( BaseMessages.getString( PKG, "GetCatalogNamesDialog.includeCatalog.Tooltip" ) );
    FormData fdincludeCatalog = new FormData();
    fdincludeCatalog.left = new FormAttachment( middle, -margin );
    fdincludeCatalog.top = new FormAttachment( wlIncludeCatalog, 0, SWT.CENTER );
    wIncludeCatalog.setLayoutData( fdincludeCatalog );
    SelectionAdapter lincludeCatalog = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };
    wIncludeCatalog.addSelectionListener( lincludeCatalog );

    // Include Schemas
    Label wlIncludeSchema = new Label( wSettings, SWT.RIGHT );
    wlIncludeSchema.setText( BaseMessages.getString( PKG, "GetSchemaNamesDialog.includeSchema.Label" ) );
    props.setLook( wlIncludeSchema );
    FormData fdlincludeSchema = new FormData();
    fdlincludeSchema.left = new FormAttachment( 0, -margin );
    fdlincludeSchema.top = new FormAttachment( wIncludeCatalog, margin );
    fdlincludeSchema.right = new FormAttachment( middle, -2 * margin );
    wlIncludeSchema.setLayoutData( fdlincludeSchema );
    wIncludeSchema = new Button( wSettings, SWT.CHECK );
    props.setLook( wIncludeSchema );
    wIncludeSchema.setToolTipText( BaseMessages.getString( PKG, "GetSchemaNamesDialog.includeSchema.Tooltip" ) );
    FormData fdincludeSchema = new FormData();
    fdincludeSchema.left = new FormAttachment( middle, -margin );
    fdincludeSchema.top = new FormAttachment( wlIncludeSchema, 0, SWT.CENTER );
    wIncludeSchema.setLayoutData( fdincludeSchema );
    SelectionAdapter lincludeSchema = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };
    wIncludeSchema.addSelectionListener( lincludeSchema );

    // Include tables
    Label wlIncludeTable = new Label( wSettings, SWT.RIGHT );
    wlIncludeTable.setText( BaseMessages.getString( PKG, "GetTableNamesDialog.includeTable.Label" ) );
    props.setLook( wlIncludeTable );
    FormData fdlincludeTable = new FormData();
    fdlincludeTable.left = new FormAttachment( 0, -margin );
    fdlincludeTable.top = new FormAttachment( wIncludeSchema, margin );
    fdlincludeTable.right = new FormAttachment( middle, -2 * margin );
    wlIncludeTable.setLayoutData( fdlincludeTable );
    wIncludeTable = new Button( wSettings, SWT.CHECK );
    props.setLook( wIncludeTable );
    wIncludeTable.setToolTipText( BaseMessages.getString( PKG, "GetTableNamesDialog.includeTable.Tooltip" ) );
    FormData fdincludeTable = new FormData();
    fdincludeTable.left = new FormAttachment( middle, -margin );
    fdincludeTable.top = new FormAttachment( wlIncludeTable, 0, SWT.CENTER );
    wIncludeTable.setLayoutData( fdincludeTable );
    SelectionAdapter lincludeTable = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };
    wIncludeTable.addSelectionListener( lincludeTable );

    // Include views
    Label wlIncludeView = new Label( wSettings, SWT.RIGHT );
    wlIncludeView.setText( BaseMessages.getString( PKG, "GetTableNamesDialog.includeView.Label" ) );
    props.setLook( wlIncludeView );
    FormData fdlincludeView = new FormData();
    fdlincludeView.left = new FormAttachment( 0, -margin );
    fdlincludeView.top = new FormAttachment( wIncludeTable, margin );
    fdlincludeView.right = new FormAttachment( middle, -2 * margin );
    wlIncludeView.setLayoutData( fdlincludeView );
    wIncludeView = new Button( wSettings, SWT.CHECK );
    props.setLook( wIncludeView );
    wIncludeView.setToolTipText( BaseMessages.getString( PKG, "GetTableNamesDialog.includeView.Tooltip" ) );
    FormData fdincludeView = new FormData();
    fdincludeView.left = new FormAttachment( middle, -margin );
    fdincludeView.top = new FormAttachment( wlIncludeView, 0, SWT.CENTER );
    wIncludeView.setLayoutData( fdincludeView );
    SelectionAdapter lincludeView = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };
    wIncludeView.addSelectionListener( lincludeView );

    // Include procedures
    Label wlIncludeProcedure = new Label( wSettings, SWT.RIGHT );
    wlIncludeProcedure.setText( BaseMessages.getString( PKG, "GetTableNamesDialog.includeProcedure.Label" ) );
    props.setLook( wlIncludeProcedure );
    FormData fdlincludeProcedure = new FormData();
    fdlincludeProcedure.left = new FormAttachment( 0, -margin );
    fdlincludeProcedure.top = new FormAttachment( wIncludeView, margin );
    fdlincludeProcedure.right = new FormAttachment( middle, -2 * margin );
    wlIncludeProcedure.setLayoutData( fdlincludeProcedure );
    wIncludeProcedure = new Button( wSettings, SWT.CHECK );
    props.setLook( wIncludeProcedure );
    wIncludeProcedure
      .setToolTipText( BaseMessages.getString( PKG, "GetTableNamesDialog.includeProcedure.Tooltip" ) );
    FormData fdincludeProcedure = new FormData();
    fdincludeProcedure.left = new FormAttachment( middle, -margin );
    fdincludeProcedure.top = new FormAttachment( wlIncludeProcedure, 0, SWT.CENTER );
    wIncludeProcedure.setLayoutData( fdincludeProcedure );
    SelectionAdapter lincludeProcedure = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };
    wIncludeProcedure.addSelectionListener( lincludeProcedure );

    // Include Synonyms
    Label wlIncludeSynonym = new Label( wSettings, SWT.RIGHT );
    wlIncludeSynonym.setText( BaseMessages.getString( PKG, "GetTableNamesDialog.includeSynonym.Label" ) );
    props.setLook( wlIncludeSynonym );
    FormData fdlincludeSynonym = new FormData();
    fdlincludeSynonym.left = new FormAttachment( 0, -margin );
    fdlincludeSynonym.top = new FormAttachment( wIncludeProcedure, margin );
    fdlincludeSynonym.right = new FormAttachment( middle, -2 * margin );
    wlIncludeSynonym.setLayoutData( fdlincludeSynonym );
    wIncludeSynonym = new Button( wSettings, SWT.CHECK );
    props.setLook( wIncludeSynonym );
    wIncludeSynonym.setToolTipText( BaseMessages.getString( PKG, "GetTableNamesDialog.includeSynonym.Tooltip" ) );
    FormData fdincludeSynonym = new FormData();
    fdincludeSynonym.left = new FormAttachment( middle, -margin );
    fdincludeSynonym.top = new FormAttachment( wlIncludeSynonym, 0, SWT.CENTER );
    wIncludeSynonym.setLayoutData( fdincludeSynonym );
    SelectionAdapter lincludeSynonym = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };
    wIncludeSynonym.addSelectionListener( lincludeSynonym );

    // Add schema in output
    Label wlAddSchemaInOutput = new Label( wSettings, SWT.RIGHT );
    wlAddSchemaInOutput.setText( BaseMessages.getString( PKG, "GetTableNamesDialog.addSchemaInOutput.Label" ) );
    props.setLook( wlAddSchemaInOutput );
    FormData fdladdSchemaInOutput = new FormData();
    fdladdSchemaInOutput.left = new FormAttachment( 0, -margin );
    fdladdSchemaInOutput.top = new FormAttachment( wIncludeSynonym, 2 * margin );
    fdladdSchemaInOutput.right = new FormAttachment( middle, -2 * margin );
    wlAddSchemaInOutput.setLayoutData( fdladdSchemaInOutput );
    wAddSchemaInOutput = new Button( wSettings, SWT.CHECK );
    props.setLook( wAddSchemaInOutput );
    wAddSchemaInOutput.setToolTipText( BaseMessages.getString(
      PKG, "GetTableNamesDialog.addSchemaInOutput.Tooltip" ) );
    FormData fdaddSchemaInOutput = new FormData();
    fdaddSchemaInOutput.left = new FormAttachment( middle, -margin );
    fdaddSchemaInOutput.top = new FormAttachment( wlAddSchemaInOutput, 0, SWT.CENTER );
    wAddSchemaInOutput.setLayoutData( fdaddSchemaInOutput );
    SelectionAdapter laddSchemaInOutput = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };
    wAddSchemaInOutput.addSelectionListener( laddSchemaInOutput );

    FormData fdSettings = new FormData();
    fdSettings.left = new FormAttachment( 0, margin );
    fdSettings.top = new FormAttachment( wSchemaField, 2 * margin );
    fdSettings.right = new FormAttachment( 100, -margin );
    wSettings.setLayoutData( fdSettings );

    // ///////////////////////////////////////////////////////////
    // / END OF SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    // ///////////////////////////////
    // START OF OutputFields GROUP //
    // ///////////////////////////////

    Group wOutputFields = new Group( shell, SWT.SHADOW_NONE );
    props.setLook( wOutputFields );
    wOutputFields.setText( BaseMessages.getString( PKG, "GetTableNamesDialog.wOutputFields.Label" ) );

    FormLayout OutputFieldsgroupLayout = new FormLayout();
    OutputFieldsgroupLayout.marginWidth = 10;
    OutputFieldsgroupLayout.marginHeight = 10;
    wOutputFields.setLayout( OutputFieldsgroupLayout );

    // TablenameField fieldname ...
    Label wlTablenameField = new Label( wOutputFields, SWT.RIGHT );
    wlTablenameField.setText( BaseMessages.getString( PKG, "GetTableNamesDialog.TablenameFieldName.Label" ) );
    props.setLook( wlTablenameField );
    FormData fdlTablenameField = new FormData();
    fdlTablenameField.left = new FormAttachment( 0, 0 );
    fdlTablenameField.right = new FormAttachment( middle, -margin );
    fdlTablenameField.top = new FormAttachment( wSettings, margin * 2 );
    wlTablenameField.setLayoutData( fdlTablenameField );
    wTablenameField = new Text( wOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTablenameField
      .setToolTipText( BaseMessages.getString( PKG, "GetTableNamesDialog.TablenameFieldName.Tooltip" ) );
    props.setLook( wTablenameField );
    wTablenameField.addModifyListener( lsMod );
    FormData fdTablenameField = new FormData();
    fdTablenameField.left = new FormAttachment( middle, 0 );
    fdTablenameField.top = new FormAttachment( wSettings, margin * 2 );
    fdTablenameField.right = new FormAttachment( 100, 0 );
    wTablenameField.setLayoutData( fdTablenameField );

    // ObjectTypeField fieldname ...
    Label wlObjectTypeField = new Label( wOutputFields, SWT.RIGHT );
    wlObjectTypeField.setText( BaseMessages.getString( PKG, "GetTableNamesDialog.ObjectTypeFieldName.Label" ) );
    props.setLook( wlObjectTypeField );
    FormData fdlObjectTypeField = new FormData();
    fdlObjectTypeField.left = new FormAttachment( 0, 0 );
    fdlObjectTypeField.right = new FormAttachment( middle, -margin );
    fdlObjectTypeField.top = new FormAttachment( wTablenameField, margin );
    wlObjectTypeField.setLayoutData( fdlObjectTypeField );
    wObjectTypeField = new Text( wOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wObjectTypeField.setToolTipText( BaseMessages.getString(
      PKG, "GetTableNamesDialog.ObjectTypeFieldName.Tooltip" ) );
    props.setLook( wObjectTypeField );
    wObjectTypeField.addModifyListener( lsMod );
    FormData fdObjectTypeField = new FormData();
    fdObjectTypeField.left = new FormAttachment( middle, 0 );
    fdObjectTypeField.top = new FormAttachment( wTablenameField, margin );
    fdObjectTypeField.right = new FormAttachment( 100, 0 );
    wObjectTypeField.setLayoutData( fdObjectTypeField );

    // isSystemObjectField fieldname ...
    Label wlisSystemObjectField = new Label( wOutputFields, SWT.RIGHT );
    wlisSystemObjectField.setText( BaseMessages.getString(
      PKG, "GetTableNamesDialog.isSystemObjectFieldName.Label" ) );
    props.setLook( wlisSystemObjectField );
    FormData fdlisSystemObjectField = new FormData();
    fdlisSystemObjectField.left = new FormAttachment( 0, 0 );
    fdlisSystemObjectField.right = new FormAttachment( middle, -margin );
    fdlisSystemObjectField.top = new FormAttachment( wObjectTypeField, margin );
    wlisSystemObjectField.setLayoutData( fdlisSystemObjectField );
    wIsSystemObjectField = new Text( wOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wIsSystemObjectField.setToolTipText( BaseMessages.getString(
      PKG, "GetTableNamesDialog.isSystemObjectFieldName.Tooltip" ) );
    props.setLook( wIsSystemObjectField );
    wIsSystemObjectField.addModifyListener( lsMod );
    FormData fdisSystemObjectField = new FormData();
    fdisSystemObjectField.left = new FormAttachment( middle, 0 );
    fdisSystemObjectField.top = new FormAttachment( wObjectTypeField, margin );
    fdisSystemObjectField.right = new FormAttachment( 100, 0 );
    wIsSystemObjectField.setLayoutData( fdisSystemObjectField );

    // CreationSQL fieldname ...
    Label wlSqlCreationField = new Label( wOutputFields, SWT.RIGHT );
    wlSqlCreationField.setText( BaseMessages.getString( PKG, "GetTableNamesDialog.CreationSQLName.Label" ) );
    props.setLook( wlSqlCreationField );
    FormData fdlSqlCreationField = new FormData();
    fdlSqlCreationField.left = new FormAttachment( 0, 0 );
    fdlSqlCreationField.right = new FormAttachment( middle, -margin );
    fdlSqlCreationField.top = new FormAttachment( wIsSystemObjectField, margin );
    wlSqlCreationField.setLayoutData( fdlSqlCreationField );
    wSqlCreationField = new Text( wOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wSqlCreationField
      .setToolTipText( BaseMessages.getString( PKG, "GetTableNamesDialog.CreationSQLName.Tooltip" ) );
    props.setLook( wSqlCreationField );
    wSqlCreationField.addModifyListener( lsMod );
    FormData fdSqlCreationField = new FormData();
    fdSqlCreationField.left = new FormAttachment( middle, 0 );
    fdSqlCreationField.top = new FormAttachment( wIsSystemObjectField, margin );
    fdSqlCreationField.right = new FormAttachment( 100, 0 );
    wSqlCreationField.setLayoutData( fdSqlCreationField );

    FormData fdOutputFields = new FormData();
    fdOutputFields.left = new FormAttachment( 0, margin );
    fdOutputFields.top = new FormAttachment( wSettings, 2 * margin );
    fdOutputFields.right = new FormAttachment( 100, -margin );
    fdOutputFields.bottom = new FormAttachment( wOk, -2 * margin );
    wOutputFields.setLayoutData( fdOutputFields );

    // ///////////////////////////////////////////////////////////
    // / END OF OutputFields GROUP
    // ///////////////////////////////////////////////////////////


    // Add listeners

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    ActivedynamicSchema();
    refreshIncludeCatalog();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void refreshIncludeCatalog() {
    if ( !Utils.isEmpty( wSchemaName.getText() ) ) {
      wIncludeCatalog.setSelection( false );
      wlIncludeCatalog.setEnabled( false );
      wIncludeCatalog.setEnabled( false );
    } else {
      wlIncludeCatalog.setEnabled( true );
      wIncludeCatalog.setEnabled( true );
    }
  }

  private void ActivedynamicSchema() {
    wlSchemaField.setEnabled( wDynamicSchema.getSelection() );
    wSchemaField.setEnabled( wDynamicSchema.getSelection() );
    wPreview.setEnabled( !wDynamicSchema.getSelection() );
    wlSchemaName.setEnabled( !wDynamicSchema.getSelection() );
    wSchemaName.setEnabled( !wDynamicSchema.getSelection() );
    if ( wDynamicSchema.getSelection() ) {
      wIncludeCatalog.setSelection( false );
    }
    wlIncludeCatalog.setEnabled( !wDynamicSchema.getSelection() );
    wIncludeCatalog.setEnabled( !wDynamicSchema.getSelection() );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( isDebug() ) {
      logDebug( toString(), BaseMessages.getString( PKG, "GetTableNamesDialog.Log.GettingKeyInfo" ) );
    }

    if ( input.getDatabase() != null ) {
      wConnection.setText( input.getDatabase().getName() );
    }
    if ( input.getSchemaName() != null ) {
      wSchemaName.setText( input.getSchemaName() );
    }
    if ( input.getTablenameFieldName() != null ) {
      wTablenameField.setText( input.getTablenameFieldName() );
    }
    if ( input.getObjectTypeFieldName() != null ) {
      wObjectTypeField.setText( input.getObjectTypeFieldName() );
    }
    if ( input.isSystemObjectFieldName() != null ) {
      wIsSystemObjectField.setText( input.isSystemObjectFieldName() );
    }
    if ( input.getSqlCreationFieldName() != null ) {
      wSqlCreationField.setText( input.getSqlCreationFieldName() );
    }
    wIncludeCatalog.setSelection( input.isIncludeCatalog() );
    wIncludeSchema.setSelection( input.isIncludeSchema() );
    wIncludeTable.setSelection( input.isIncludeTable() );
    wIncludeView.setSelection( input.isIncludeView() );
    wIncludeProcedure.setSelection( input.isIncludeProcedure() );
    wIncludeSynonym.setSelection( input.isIncludeSynonym() );
    wAddSchemaInOutput.setSelection( input.isAddSchemaInOut() );

    wDynamicSchema.setSelection( input.isDynamicSchema() );
    if ( input.getSchemaFieldName() != null ) {
      wSchemaField.setText( input.getSchemaFieldName() );
    }

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( changed );
    dispose();
  }

  private void setSchemaField() {
    if ( !gotPreviousFields ) {
      try {
        String value = wSchemaField.getText();
        wSchemaField.removeAll();

        IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
        if ( r != null ) {
          wSchemaField.setItems( r.getFieldNames() );
        }
        if ( value != null ) {
          wSchemaField.setText( value );
        }
      } catch ( HopException ke ) {
        new ErrorDialog(
          shell, BaseMessages.getString( PKG, "GetTableNamesDialog.FailedToGetFields.DialogTitle" ),
          BaseMessages.getString( PKG, "GetTableNamesDialog.FailedToGetFields.DialogMessage" ), ke );
      }
      gotPreviousFields = true;
    }
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }
    transformName = wTransformName.getText(); // return value
    getInfo( input );
    if ( input.getDatabase() == null ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "GetTableNamesDialog.InvalidConnection.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "GetTableNamesDialog.InvalidConnection.DialogTitle" ) );
      mb.open();
      return;
    }
    dispose();
  }

  private void getInfo( GetTableNamesMeta info ) {
    info.setDatabase( pipelineMeta.findDatabase( wConnection.getText() ) );
    info.setSchemaName( wSchemaName.getText() );
    info.setTablenameFieldName( wTablenameField.getText() );
    info.setSqlCreationFieldName( wSqlCreationField.getText() );
    info.setObjectTypeFieldName( wObjectTypeField.getText() );
    info.setIsSystemObjectFieldName( wIsSystemObjectField.getText() );
    info.setIncludeCatalog( wIncludeCatalog.getSelection() );
    info.setIncludeSchema( wIncludeSchema.getSelection() );
    info.setIncludeTable( wIncludeTable.getSelection() );
    info.setIncludeView( wIncludeView.getSelection() );
    info.setIncludeProcedure( wIncludeProcedure.getSelection() );
    info.setIncludeSynonym( wIncludeSynonym.getSelection() );
    info.setAddSchemaInOut( wAddSchemaInOutput.getSelection() );

    info.setDynamicSchema( wDynamicSchema.getSelection() );
    info.setSchemaFieldName( wSchemaField.getText() );

  }

  private boolean checkUserInput( GetTableNamesMeta meta ) {

    if ( Utils.isEmpty( meta.getTablenameFieldName() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "GetTableNamesDialog.Error.TablenameFieldNameMissingMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "GetTableNamesDialog.Error.TablenameFieldNameMissingTitle" ) );
      mb.open();

      return false;
    }
    return true;
  }

  // Preview the data
  private void preview() {
    GetTableNamesMeta oneMeta = new GetTableNamesMeta();

    getInfo( oneMeta );
    if ( oneMeta.getDatabase() == null ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "GetTableNamesDialog.InvalidConnection.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "GetTableNamesDialog.InvalidConnection.DialogTitle" ) );
      mb.open();
      return;
    }
    if ( !checkUserInput( oneMeta ) ) {
      return;
    }

    PipelineMeta previewMeta = PipelinePreviewFactory.generatePreviewPipeline( variables, pipelineMeta.getMetadataProvider(),
      oneMeta, wTransformName.getText() );

    EnterNumberDialog numberDialog = new EnterNumberDialog( shell, props.getDefaultPreviewSize(),
      BaseMessages.getString( PKG, "GetTableNamesDialog.PreviewSize.DialogTitle" ),
      BaseMessages.getString( PKG, "GetTableNamesDialog.PreviewSize.DialogMessage" ) );
    int previewSize = numberDialog.open();
    if ( previewSize > 0 ) {
      PipelinePreviewProgressDialog progressDialog =
        new PipelinePreviewProgressDialog(
          shell, variables, previewMeta, new String[] { wTransformName.getText() }, new int[] { previewSize } );
      progressDialog.open();

      if ( !progressDialog.isCancelled() ) {
        Pipeline pipeline = progressDialog.getPipeline();
        String loggingText = progressDialog.getLoggingText();

        if ( pipeline.getResult() != null && pipeline.getResult().getNrErrors() > 0 ) {
          EnterTextDialog etd =
            new EnterTextDialog( shell, BaseMessages.getString( PKG, "System.Dialog.Error.Title" ), BaseMessages
              .getString( PKG, "GetTableNamesDialog.ErrorInPreview.DialogMessage" ), loggingText, true );
          etd.setReadOnly();
          etd.open();
        }

        PreviewRowsDialog prd =
          new PreviewRowsDialog(
            shell, variables, SWT.NONE, wTransformName.getText(), progressDialog.getPreviewRowsMeta( wTransformName
            .getText() ), progressDialog.getPreviewRows( wTransformName.getText() ), loggingText );
        prd.open();
      }
    }
  }
}
