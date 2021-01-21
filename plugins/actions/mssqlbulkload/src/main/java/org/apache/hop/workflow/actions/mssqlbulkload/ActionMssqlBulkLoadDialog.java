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

package org.apache.hop.workflow.actions.mssqlbulkload;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

/**
 * Dialog class for the MSSqlBulkLoader.
 *
 * @author Samatar Hassan
 * @since Jan-2007
 */
public class ActionMssqlBulkLoadDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionMssqlBulkLoad.class; // For Translator

  private static final String[] FILETYPES = new String[] {
    BaseMessages.getString( PKG, "JobMssqlBulkLoad.Filetype.Text" ),
    BaseMessages.getString( PKG, "JobMssqlBulkLoad.Filetype.Csv" ),
    BaseMessages.getString( PKG, "JobMssqlBulkLoad.Filetype.All" ) };

  private Text wName;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TextVar wSchemaname;

  private TextVar wTablename;

  private ActionMssqlBulkLoad action;
  private Shell shell;
  private boolean changed;

  private TextVar wFilename;

  // Field Terminator
  private Label wlFieldTerminator;
  private TextVar wFieldTerminator;

  private TextVar wLineterminated;

  private TextVar wOrderBy;

  private TextVar wStartFile;

  private TextVar wEndFile;

  // Specific Codepage
  private Label wlSpecificCodePage;
  private TextVar wSpecificCodePage;

  private TextVar wMaxErrors;

  private Button wAddFileToResult;

  private Button wTruncate;

  private Button wFireTriggers;

  private Button wCheckConstraints;

  private Button wAddDateTime;

  private Button wKeepNulls;

  private Button wKeepIdentity;

  private Button wTablock;

  private CCombo wDataFiletype;

  // Format file

  private TextVar wFormatFilename;

  private CCombo wOrderDirection;

  private CCombo wCodePage;

  private TextVar wErrorFilename;

  private TextVar wBatchSize;

  private TextVar wRowsPerBatch;

  public ActionMssqlBulkLoadDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = (ActionMssqlBulkLoad) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Name.Default" ) );
    }
  }

  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, action );
    
    WorkflowMeta workflowMeta = getWorkflowMeta();
    
    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////
    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Tab.General.Label" ) );

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout( generalLayout );

    // Filename line
    Label wlName = new Label(wGeneralComp, SWT.RIGHT);
    wlName.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Name.Label" ) );
    props.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, 0 );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData(fdlName);
    wName = new Text(wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    FormData fdName = new FormData();
    fdName.left = new FormAttachment( middle, 0 );
    fdName.top = new FormAttachment( 0, margin );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData(fdName);

    // ///////////////////////////////
    // START OF ConnectionGroup GROUP///
    // /////////////////////////////////
    Group wConnectionGroup = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wConnectionGroup);
    wConnectionGroup.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.ConnectionGroup.Group.Label" ) );

    FormLayout ConnectionGroupLayout = new FormLayout();
    ConnectionGroupLayout.marginWidth = 10;
    ConnectionGroupLayout.marginHeight = 10;

    wConnectionGroup.setLayout( ConnectionGroupLayout );

    // Connection line
    wConnection = addConnectionLine(wConnectionGroup, wName, action.getDatabase(), lsMod );

    // Schema name line
    // Schema name
    Label wlSchemaname = new Label(wConnectionGroup, SWT.RIGHT);
    wlSchemaname.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Schemaname.Label" ) );
    props.setLook(wlSchemaname);
    FormData fdlSchemaname = new FormData();
    fdlSchemaname.left = new FormAttachment( 0, 0 );
    fdlSchemaname.right = new FormAttachment( middle, 0 );
    fdlSchemaname.top = new FormAttachment( wConnection, margin );
    wlSchemaname.setLayoutData(fdlSchemaname);

    wSchemaname = new TextVar( variables, wConnectionGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSchemaname );
    wSchemaname.setToolTipText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Schemaname.Tooltip" ) );
    wSchemaname.addModifyListener( lsMod );
    FormData fdSchemaname = new FormData();
    fdSchemaname.left = new FormAttachment( middle, 0 );
    fdSchemaname.top = new FormAttachment( wConnection, margin );
    fdSchemaname.right = new FormAttachment( 100, 0 );
    wSchemaname.setLayoutData(fdSchemaname);

    // Table name line
    Label wlTablename = new Label(wConnectionGroup, SWT.RIGHT);
    wlTablename.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Tablename.Label" ) );
    props.setLook(wlTablename);
    FormData fdlTablename = new FormData();
    fdlTablename.left = new FormAttachment( 0, 0 );
    fdlTablename.right = new FormAttachment( middle, 0 );
    fdlTablename.top = new FormAttachment( wSchemaname, margin );
    wlTablename.setLayoutData(fdlTablename);

    Button wbTable = new Button(wConnectionGroup, SWT.PUSH | SWT.CENTER);
    props.setLook(wbTable);
    wbTable.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment( 100, 0 );
    fdbTable.top = new FormAttachment( wSchemaname, margin / 2 );
    wbTable.setLayoutData( fdbTable );
    wbTable.addSelectionListener(new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        getTableName();
      }
    } );

    wTablename = new TextVar( variables, wConnectionGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTablename );
    wTablename.setToolTipText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Tablename.Tooltip" ) );
    wTablename.addModifyListener( lsMod );
    FormData fdTablename = new FormData();
    fdTablename.left = new FormAttachment( middle, 0 );
    fdTablename.top = new FormAttachment( wSchemaname, margin );
    fdTablename.right = new FormAttachment(wbTable, -margin );
    wTablename.setLayoutData(fdTablename);

    // Truncate table
    // Truncate table
    Label wlTruncate = new Label(wConnectionGroup, SWT.RIGHT);
    wlTruncate.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Truncate.Label" ) );
    props.setLook(wlTruncate);
    FormData fdlTruncate = new FormData();
    fdlTruncate.left = new FormAttachment( 0, 0 );
    fdlTruncate.top = new FormAttachment( wTablename, margin );
    fdlTruncate.right = new FormAttachment( middle, -margin );
    wlTruncate.setLayoutData(fdlTruncate);
    wTruncate = new Button(wConnectionGroup, SWT.CHECK );
    props.setLook( wTruncate );
    wTruncate.setToolTipText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Truncate.Tooltip" ) );
    FormData fdTruncate = new FormData();
    fdTruncate.left = new FormAttachment( middle, 0 );
    fdTruncate.top = new FormAttachment( wTablename, margin );
    fdTruncate.right = new FormAttachment( 100, 0 );
    wTruncate.setLayoutData(fdTruncate);
    wTruncate.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    FormData fdConnectionGroup = new FormData();
    fdConnectionGroup.left = new FormAttachment( 0, margin );
    fdConnectionGroup.top = new FormAttachment( wName, margin );
    fdConnectionGroup.right = new FormAttachment( 100, -margin );
    wConnectionGroup.setLayoutData(fdConnectionGroup);
    // ///////////////////////////////////////////////////////////
    // / END OF ConnectionGroup GROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF DataFileGroup GROUP///
    // ///////////////////////////////
    Group wDataFileGroup = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wDataFileGroup);
    wDataFileGroup.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.DataFileGroup.Group.Label" ) );

    FormLayout DataFileGroupLayout = new FormLayout();
    DataFileGroupLayout.marginWidth = 10;
    DataFileGroupLayout.marginHeight = 10;
    wDataFileGroup.setLayout( DataFileGroupLayout );

    // Filename line
    // File
    Label wlFilename = new Label(wDataFileGroup, SWT.RIGHT);
    wlFilename.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Filename.Label" ) );
    props.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment(wConnectionGroup, margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData(fdlFilename);

    Button wbFilename = new Button(wDataFileGroup, SWT.PUSH | SWT.CENTER);
    props.setLook(wbFilename);
    wbFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment(wConnectionGroup, 0 );
    wbFilename.setLayoutData(fdbFilename);

    wFilename = new TextVar( variables, wDataFileGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.setToolTipText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Filename.Tooltip" ) );
    wFilename.addModifyListener( lsMod );
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.top = new FormAttachment(wConnectionGroup, margin );
    fdFilename.right = new FormAttachment(wbFilename, -margin );
    wFilename.setLayoutData(fdFilename);

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename.addModifyListener( e -> wFilename.setToolTipText( variables.resolve( wFilename.getText() ) ) );

    wbFilename.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wFilename, variables,
      new String[] { "*.txt", "*.csv", "*" }, FILETYPES, true )
    );

    // Data file type
    // Data file type
    Label wlDataFiletype = new Label(wDataFileGroup, SWT.RIGHT);
    wlDataFiletype.setText( BaseMessages.getString( PKG, "JobMysqlBulkLoad.DataFiletype.Label" ) );
    props.setLook(wlDataFiletype);
    FormData fdlDataFiletype = new FormData();
    fdlDataFiletype.left = new FormAttachment( 0, 0 );
    fdlDataFiletype.right = new FormAttachment( middle, 0 );
    fdlDataFiletype.top = new FormAttachment( wFilename, margin );
    wlDataFiletype.setLayoutData(fdlDataFiletype);
    wDataFiletype = new CCombo(wDataFileGroup, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wDataFiletype.add( "char" );
    wDataFiletype.add( "native" );
    wDataFiletype.add( "widechar" );
    wDataFiletype.add( "widenative" );
    wDataFiletype.select( 0 ); // +1: starts at -1

    props.setLook( wDataFiletype );
    FormData fdDataFiletype = new FormData();
    fdDataFiletype.left = new FormAttachment( middle, 0 );
    fdDataFiletype.top = new FormAttachment( wFilename, margin );
    fdDataFiletype.right = new FormAttachment( 100, 0 );
    wDataFiletype.setLayoutData(fdDataFiletype);
    wDataFiletype.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        setDataType();

      }
    } );

    FormData fdDataFileGroup = new FormData();
    fdDataFileGroup.left = new FormAttachment( 0, margin );
    fdDataFileGroup.top = new FormAttachment(wConnectionGroup, margin );
    fdDataFileGroup.right = new FormAttachment( 100, -margin );
    wDataFileGroup.setLayoutData(fdDataFileGroup);
    // ///////////////////////////////////////////////////////////
    // / END OF DataFileGroup GROUP
    // ///////////////////////////////////////////////////////////

    // FieldTerminator
    wlFieldTerminator = new Label(wGeneralComp, SWT.RIGHT );
    wlFieldTerminator.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.FieldTerminator.Label" ) );
    props.setLook( wlFieldTerminator );
    FormData fdlFieldTerminator = new FormData();
    fdlFieldTerminator.left = new FormAttachment( 0, 0 );
    fdlFieldTerminator.right = new FormAttachment( middle, 0 );
    fdlFieldTerminator.top = new FormAttachment(wDataFileGroup, 3 * margin );
    wlFieldTerminator.setLayoutData(fdlFieldTerminator);

    wFieldTerminator = new TextVar( variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFieldTerminator );
    wFieldTerminator.setToolTipText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.FieldTerminator.Tooltip" ) );
    wFieldTerminator.addModifyListener( lsMod );
    FormData fdFieldTerminator = new FormData();
    fdFieldTerminator.left = new FormAttachment( middle, 0 );
    fdFieldTerminator.top = new FormAttachment(wDataFileGroup, 3 * margin );
    fdFieldTerminator.right = new FormAttachment( 100, 0 );
    wFieldTerminator.setLayoutData(fdFieldTerminator);

    // Line terminated
    // Line terminated
    Label wlLineterminated = new Label(wGeneralComp, SWT.RIGHT);
    wlLineterminated.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Lineterminated.Label" ) );
    props.setLook(wlLineterminated);
    FormData fdlLineterminated = new FormData();
    fdlLineterminated.left = new FormAttachment( 0, 0 );
    fdlLineterminated.right = new FormAttachment( middle, 0 );
    fdlLineterminated.top = new FormAttachment( wFieldTerminator, margin );
    wlLineterminated.setLayoutData(fdlLineterminated);

    wLineterminated = new TextVar( variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLineterminated );
    wLineterminated.setToolTipText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Lineterminated.Tooltip" ) );
    wLineterminated.addModifyListener( lsMod );
    FormData fdLineterminated = new FormData();
    fdLineterminated.left = new FormAttachment( middle, 0 );
    fdLineterminated.top = new FormAttachment( wFieldTerminator, margin );
    fdLineterminated.right = new FormAttachment( 100, 0 );
    wLineterminated.setLayoutData(fdLineterminated);

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( 0, 0 );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 500, -margin );
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);
    props.setLook(wGeneralComp);

    // ////////////////////////
    // END OF GENERAL TAB ///
    // ////////////////////////

    // ////////////////////////////////////
    // START OF Advanced TAB ///
    // ///////////////////////////////////

    CTabItem wAdvancedTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdvancedTab.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Tab.Advanced.Label" ) );

    FormLayout AdvancedLayout = new FormLayout();
    AdvancedLayout.marginWidth = 3;
    AdvancedLayout.marginHeight = 3;

    Composite wAdvancedComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wAdvancedComp);
    wAdvancedComp.setLayout( AdvancedLayout );

    // CodePage
    // CodePage
    Label wlCodePage = new Label(wAdvancedComp, SWT.RIGHT);
    wlCodePage.setText( BaseMessages.getString( PKG, "JobMysqlBulkLoad.CodePage.Label" ) );
    props.setLook(wlCodePage);
    FormData fdlCodePage = new FormData();
    fdlCodePage.left = new FormAttachment( 0, 0 );
    fdlCodePage.right = new FormAttachment( middle, 0 );
    fdlCodePage.top = new FormAttachment( 0, margin );
    wlCodePage.setLayoutData(fdlCodePage);
    wCodePage = new CCombo(wAdvancedComp, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wCodePage.add( "ACP" );
    wCodePage.add( "OEM" );
    wCodePage.add( "RAW" );
    wCodePage.add( BaseMessages.getString( PKG, "JobMssqlBulkLoad.CodePage.Specific" ) );
    wCodePage.select( 0 ); // +1: starts at -1

    props.setLook( wCodePage );
    FormData fdCodePage = new FormData();
    fdCodePage.left = new FormAttachment( middle, 0 );
    fdCodePage.top = new FormAttachment( 0, margin );
    fdCodePage.right = new FormAttachment( 100, 0 );
    wCodePage.setLayoutData(fdCodePage);
    wCodePage.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        setCodeType();

      }
    } );

    // Specific CodePage
    wlSpecificCodePage = new Label(wAdvancedComp, SWT.RIGHT );
    wlSpecificCodePage.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.SpecificCodePage.Label" ) );
    props.setLook( wlSpecificCodePage );
    FormData fdlSpecificCodePage = new FormData();
    fdlSpecificCodePage.left = new FormAttachment( 0, 0 );
    fdlSpecificCodePage.right = new FormAttachment( middle, 0 );
    fdlSpecificCodePage.top = new FormAttachment( wCodePage, margin );
    wlSpecificCodePage.setLayoutData(fdlSpecificCodePage);

    wSpecificCodePage = new TextVar( variables, wAdvancedComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSpecificCodePage );
    wSpecificCodePage.addModifyListener( lsMod );
    FormData fdSpecificCodePage = new FormData();
    fdSpecificCodePage.left = new FormAttachment( middle, 0 );
    fdSpecificCodePage.top = new FormAttachment( wCodePage, margin );
    fdSpecificCodePage.right = new FormAttachment( 100, 0 );
    wSpecificCodePage.setLayoutData(fdSpecificCodePage);

    // FormatFilename line
    Label wlFormatFilename = new Label(wAdvancedComp, SWT.RIGHT);
    wlFormatFilename.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.FormatFilename.Label" ) );
    props.setLook(wlFormatFilename);
    FormData fdlFormatFilename = new FormData();
    fdlFormatFilename.left = new FormAttachment( 0, 0 );
    fdlFormatFilename.top = new FormAttachment( wSpecificCodePage, margin );
    fdlFormatFilename.right = new FormAttachment( middle, -margin );
    wlFormatFilename.setLayoutData(fdlFormatFilename);

    Button wbFormatFilename = new Button(wAdvancedComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbFormatFilename);
    wbFormatFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbFormatFilename = new FormData();
    fdbFormatFilename.right = new FormAttachment( 100, 0 );
    fdbFormatFilename.top = new FormAttachment( wSpecificCodePage, 0 );
    wbFormatFilename.setLayoutData(fdbFormatFilename);

    wFormatFilename = new TextVar( variables, wAdvancedComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFormatFilename );
    wFormatFilename.setToolTipText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.FormatFilename.Tooltip" ) );
    wFormatFilename.addModifyListener( lsMod );
    FormData fdFormatFilename = new FormData();
    fdFormatFilename.left = new FormAttachment( middle, 0 );
    fdFormatFilename.top = new FormAttachment( wSpecificCodePage, margin );
    fdFormatFilename.right = new FormAttachment(wbFormatFilename, -margin );
    wFormatFilename.setLayoutData(fdFormatFilename);

    // Whenever something changes, set the tooltip to the expanded version:
    wFormatFilename.addModifyListener( e -> wFormatFilename.setToolTipText( variables.resolve( wFormatFilename.getText() ) ) );

    wbFormatFilename.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wFormatFilename, variables,
      new String[] { "*.txt", "*.csv", "*" }, FILETYPES, true )
    );

    // Fire Triggers?
    // Fire Triggers
    Label wlFireTriggers = new Label(wAdvancedComp, SWT.RIGHT);
    wlFireTriggers.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.FireTriggers.Label" ) );
    props.setLook(wlFireTriggers);
    FormData fdlFireTriggers = new FormData();
    fdlFireTriggers.left = new FormAttachment( 0, 0 );
    fdlFireTriggers.top = new FormAttachment( wFormatFilename, margin );
    fdlFireTriggers.right = new FormAttachment( middle, -margin );
    wlFireTriggers.setLayoutData(fdlFireTriggers);
    wFireTriggers = new Button(wAdvancedComp, SWT.CHECK );
    props.setLook( wFireTriggers );
    wFireTriggers.setToolTipText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.FireTriggers.Tooltip" ) );
    FormData fdFireTriggers = new FormData();
    fdFireTriggers.left = new FormAttachment( middle, 0 );
    fdFireTriggers.top = new FormAttachment( wFormatFilename, margin );
    fdFireTriggers.right = new FormAttachment( 100, 0 );
    wFireTriggers.setLayoutData(fdFireTriggers);
    wFireTriggers.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // CHECK CONSTRAINTS
    // Check Constaints
    Label wlCheckConstraints = new Label(wAdvancedComp, SWT.RIGHT);
    wlCheckConstraints.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.CheckConstraints.Label" ) );
    props.setLook(wlCheckConstraints);
    FormData fdlCheckConstraints = new FormData();
    fdlCheckConstraints.left = new FormAttachment( 0, 0 );
    fdlCheckConstraints.top = new FormAttachment( wFireTriggers, margin );
    fdlCheckConstraints.right = new FormAttachment( middle, -margin );
    wlCheckConstraints.setLayoutData(fdlCheckConstraints);
    wCheckConstraints = new Button(wAdvancedComp, SWT.CHECK );
    props.setLook( wCheckConstraints );
    wCheckConstraints.setToolTipText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.CheckConstraints.Tooltip" ) );
    FormData fdCheckConstraints = new FormData();
    fdCheckConstraints.left = new FormAttachment( middle, 0 );
    fdCheckConstraints.top = new FormAttachment( wFireTriggers, margin );
    fdCheckConstraints.right = new FormAttachment( 100, 0 );
    wCheckConstraints.setLayoutData(fdCheckConstraints);
    wCheckConstraints.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Keep Nulls
    // Keep nulls
    Label wlKeepNulls = new Label(wAdvancedComp, SWT.RIGHT);
    wlKeepNulls.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.KeepNulls.Label" ) );
    props.setLook(wlKeepNulls);
    FormData fdlKeepNulls = new FormData();
    fdlKeepNulls.left = new FormAttachment( 0, 0 );
    fdlKeepNulls.top = new FormAttachment( wCheckConstraints, margin );
    fdlKeepNulls.right = new FormAttachment( middle, -margin );
    wlKeepNulls.setLayoutData(fdlKeepNulls);
    wKeepNulls = new Button(wAdvancedComp, SWT.CHECK );
    props.setLook( wKeepNulls );
    wKeepNulls.setToolTipText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.KeepNulls.Tooltip" ) );
    FormData fdKeepNulls = new FormData();
    fdKeepNulls.left = new FormAttachment( middle, 0 );
    fdKeepNulls.top = new FormAttachment( wCheckConstraints, margin );
    fdKeepNulls.right = new FormAttachment( 100, 0 );
    wKeepNulls.setLayoutData(fdKeepNulls);
    wKeepNulls.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Keep Identity
    // Keep Identity?
    Label wlKeepIdentity = new Label(wAdvancedComp, SWT.RIGHT);
    wlKeepIdentity.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.KeepIdentity.Label" ) );
    props.setLook(wlKeepIdentity);
    FormData fdlKeepIdentity = new FormData();
    fdlKeepIdentity.left = new FormAttachment( 0, 0 );
    fdlKeepIdentity.top = new FormAttachment( wKeepNulls, margin );
    fdlKeepIdentity.right = new FormAttachment( middle, -margin );
    wlKeepIdentity.setLayoutData(fdlKeepIdentity);
    wKeepIdentity = new Button(wAdvancedComp, SWT.CHECK );
    props.setLook( wKeepIdentity );
    wKeepIdentity.setToolTipText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.KeepIdentity.Tooltip" ) );
    FormData fdKeepIdentity = new FormData();
    fdKeepIdentity.left = new FormAttachment( middle, 0 );
    fdKeepIdentity.top = new FormAttachment( wKeepNulls, margin );
    fdKeepIdentity.right = new FormAttachment( 100, 0 );
    wKeepIdentity.setLayoutData(fdKeepIdentity);
    wKeepIdentity.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // TABBLOCK
    // Tablock
    Label wlTablock = new Label(wAdvancedComp, SWT.RIGHT);
    wlTablock.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Tablock.Label" ) );
    props.setLook(wlTablock);
    FormData fdlTablock = new FormData();
    fdlTablock.left = new FormAttachment( 0, 0 );
    fdlTablock.top = new FormAttachment( wKeepIdentity, margin );
    fdlTablock.right = new FormAttachment( middle, -margin );
    wlTablock.setLayoutData(fdlTablock);
    wTablock = new Button(wAdvancedComp, SWT.CHECK );
    props.setLook( wTablock );
    wTablock.setToolTipText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Tablock.Tooltip" ) );
    FormData fdTablock = new FormData();
    fdTablock.left = new FormAttachment( middle, 0 );
    fdTablock.top = new FormAttachment( wKeepIdentity, margin );
    fdTablock.right = new FormAttachment( 100, 0 );
    wTablock.setLayoutData(fdTablock);
    wTablock.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Start file
    // start file at line
    Label wlStartFile = new Label(wAdvancedComp, SWT.RIGHT);
    wlStartFile.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.StartFile.Label" ) );
    props.setLook(wlStartFile);
    FormData fdlStartFile = new FormData();
    fdlStartFile.left = new FormAttachment( 0, 0 );
    fdlStartFile.right = new FormAttachment( middle, 0 );
    fdlStartFile.top = new FormAttachment( wTablock, margin );
    wlStartFile.setLayoutData(fdlStartFile);

    wStartFile = new TextVar( variables, wAdvancedComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wStartFile );
    wStartFile.setToolTipText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.StartFile.Tooltip" ) );
    wStartFile.addModifyListener( lsMod );
    FormData fdStartFile = new FormData();
    fdStartFile.left = new FormAttachment( middle, 0 );
    fdStartFile.top = new FormAttachment( wTablock, margin );
    fdStartFile.right = new FormAttachment( 100, 0 );
    wStartFile.setLayoutData(fdStartFile);

    // End file
    // End file line
    Label wlEndFile = new Label(wAdvancedComp, SWT.RIGHT);
    wlEndFile.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.EndFile.Label" ) );
    props.setLook(wlEndFile);
    FormData fdlEndFile = new FormData();
    fdlEndFile.left = new FormAttachment( 0, 0 );
    fdlEndFile.right = new FormAttachment( middle, 0 );
    fdlEndFile.top = new FormAttachment( wStartFile, margin );
    wlEndFile.setLayoutData(fdlEndFile);

    wEndFile = new TextVar( variables, wAdvancedComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wEndFile );
    wEndFile.setToolTipText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.EndFile.Tooltip" ) );
    wEndFile.addModifyListener( lsMod );
    FormData fdEndFile = new FormData();
    fdEndFile.left = new FormAttachment( middle, 0 );
    fdEndFile.top = new FormAttachment( wStartFile, margin );
    fdEndFile.right = new FormAttachment( 100, 0 );
    wEndFile.setLayoutData(fdEndFile);

    // Specifies how the data in the data file is sorted
    // List Columns
    Label wlOrderBy = new Label(wAdvancedComp, SWT.RIGHT);
    wlOrderBy.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.OrderBy.Label" ) );
    props.setLook(wlOrderBy);
    FormData fdlOrderBy = new FormData();
    fdlOrderBy.left = new FormAttachment( 0, 0 );
    fdlOrderBy.right = new FormAttachment( middle, 0 );
    fdlOrderBy.top = new FormAttachment( wEndFile, margin );
    wlOrderBy.setLayoutData(fdlOrderBy);

    Button wbOrderBy = new Button(wAdvancedComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbOrderBy);
    wbOrderBy.setText( BaseMessages.getString( PKG, "System.Button.Edit" ) );
    FormData fdbListattribut = new FormData();
    fdbListattribut.right = new FormAttachment( 100, 0 );
    fdbListattribut.top = new FormAttachment( wEndFile, margin );
    wbOrderBy.setLayoutData( fdbListattribut );
    wbOrderBy.addSelectionListener(new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        getListColumns();
      }
    } );

    wOrderBy = new TextVar( variables, wAdvancedComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wOrderBy );
    wOrderBy.setToolTipText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.OrderBy.Tooltip" ) );
    wOrderBy.addModifyListener( lsMod );
    FormData fdOrderBy = new FormData();
    fdOrderBy.left = new FormAttachment( middle, 0 );
    fdOrderBy.top = new FormAttachment( wEndFile, margin );
    fdOrderBy.right = new FormAttachment(wbOrderBy, -margin );
    wOrderBy.setLayoutData(fdOrderBy);

    // Order Direction
    // Order Direction
    Label wlOrderDirection = new Label(wAdvancedComp, SWT.RIGHT);
    wlOrderDirection.setText( BaseMessages.getString( PKG, "JobMysqlBulkLoad.OrderDirection.Label" ) );
    props.setLook(wlOrderDirection);
    FormData fdlOrderDirection = new FormData();
    fdlOrderDirection.left = new FormAttachment( 0, 0 );
    fdlOrderDirection.right = new FormAttachment( middle, 0 );
    fdlOrderDirection.top = new FormAttachment( wOrderBy, margin );
    wlOrderDirection.setLayoutData(fdlOrderDirection);
    wOrderDirection = new CCombo(wAdvancedComp, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wOrderDirection.add( BaseMessages.getString( PKG, "JobMysqlBulkLoad.OrderDirectionAsc.Label" ) );
    wOrderDirection.add( BaseMessages.getString( PKG, "JobMysqlBulkLoad.OrderDirectionDesc.Label" ) );
    wOrderDirection.select( 0 ); // +1: starts at -1

    props.setLook( wOrderDirection );
    FormData fdOrderDirection = new FormData();
    fdOrderDirection.left = new FormAttachment( middle, 0 );
    fdOrderDirection.top = new FormAttachment( wOrderBy, margin );
    fdOrderDirection.right = new FormAttachment( 100, 0 );
    wOrderDirection.setLayoutData(fdOrderDirection);

    // ErrorFilename line
    Label wlErrorFilename = new Label(wAdvancedComp, SWT.RIGHT);
    wlErrorFilename.setText( BaseMessages.getString( PKG, "JobMysqlBulkLoad.ErrorFilename.Label" ) );
    props.setLook(wlErrorFilename);
    FormData fdlErrorFilename = new FormData();
    fdlErrorFilename.left = new FormAttachment( 0, 0 );
    fdlErrorFilename.top = new FormAttachment( wOrderDirection, margin );
    fdlErrorFilename.right = new FormAttachment( middle, -margin );
    wlErrorFilename.setLayoutData(fdlErrorFilename);

    Button wbErrorFilename = new Button(wAdvancedComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbErrorFilename);
    wbErrorFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbErrorFilename = new FormData();
    fdbErrorFilename.right = new FormAttachment( 100, 0 );
    fdbErrorFilename.top = new FormAttachment( wOrderDirection, 0 );
    wbErrorFilename.setLayoutData(fdbErrorFilename);

    wErrorFilename = new TextVar( variables, wAdvancedComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wErrorFilename );
    wErrorFilename.addModifyListener( lsMod );
    wErrorFilename.setToolTipText( BaseMessages.getString( PKG, "JobMysqlBulkLoad.ErrorFilename.Tooltip" ) );
    FormData fdErrorFilename = new FormData();
    fdErrorFilename.left = new FormAttachment( middle, 0 );
    fdErrorFilename.top = new FormAttachment( wOrderDirection, margin );
    fdErrorFilename.right = new FormAttachment(wbErrorFilename, -margin );
    wErrorFilename.setLayoutData(fdErrorFilename);

    // Whenever something changes, set the tooltip to the expanded version:
    wErrorFilename.addModifyListener( e -> wErrorFilename.setToolTipText( variables.resolve( wErrorFilename.getText() ) ) );
    wbErrorFilename.addListener( SWT.Selection, e-> BaseDialog.presentDirectoryDialog( shell, wErrorFilename, variables ) );

    // Add Date time
    // Add Datetime
    Label wlAddDateTime = new Label(wAdvancedComp, SWT.RIGHT);
    wlAddDateTime.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.AddDateTime.Label" ) );
    props.setLook(wlAddDateTime);
    FormData fdlAddDateTime = new FormData();
    fdlAddDateTime.left = new FormAttachment( 0, 0 );
    fdlAddDateTime.top = new FormAttachment( wErrorFilename, margin );
    fdlAddDateTime.right = new FormAttachment( middle, -margin );
    wlAddDateTime.setLayoutData(fdlAddDateTime);
    wAddDateTime = new Button(wAdvancedComp, SWT.CHECK );
    props.setLook( wAddDateTime );
    wAddDateTime.setToolTipText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.AddDateTime.Tooltip" ) );
    FormData fdAddDateTime = new FormData();
    fdAddDateTime.left = new FormAttachment( middle, 0 );
    fdAddDateTime.top = new FormAttachment( wErrorFilename, margin );
    fdAddDateTime.right = new FormAttachment( 100, 0 );
    wAddDateTime.setLayoutData(fdAddDateTime);
    wAddDateTime.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Maximum errors allowed
    // Maximum Errors allowed
    Label wlMaxErrors = new Label(wAdvancedComp, SWT.RIGHT);
    wlMaxErrors.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.MaxErrors.Label" ) );
    props.setLook(wlMaxErrors);
    FormData fdlMaxErrors = new FormData();
    fdlMaxErrors.left = new FormAttachment( 0, 0 );
    fdlMaxErrors.right = new FormAttachment( middle, 0 );
    fdlMaxErrors.top = new FormAttachment( wAddDateTime, margin );
    wlMaxErrors.setLayoutData(fdlMaxErrors);

    wMaxErrors = new TextVar( variables, wAdvancedComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMaxErrors );
    wlMaxErrors.setToolTipText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.MaxErrors.Tooltip" ) );
    wMaxErrors.addModifyListener( lsMod );
    FormData fdMaxErrors = new FormData();
    fdMaxErrors.left = new FormAttachment( middle, 0 );
    fdMaxErrors.top = new FormAttachment( wAddDateTime, margin );
    fdMaxErrors.right = new FormAttachment( 100, 0 );
    wMaxErrors.setLayoutData(fdMaxErrors);

    // Batch Size
    // Batch Size
    Label wlBatchSize = new Label(wAdvancedComp, SWT.RIGHT);
    wlBatchSize.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.BatchSize.Label" ) );
    props.setLook(wlBatchSize);
    FormData fdlBatchSize = new FormData();
    fdlBatchSize.left = new FormAttachment( 0, 0 );
    fdlBatchSize.right = new FormAttachment( middle, 0 );
    fdlBatchSize.top = new FormAttachment( wMaxErrors, margin );
    wlBatchSize.setLayoutData(fdlBatchSize);

    wBatchSize = new TextVar( variables, wAdvancedComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wBatchSize );
    wBatchSize.setToolTipText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.BatchSize.Tooltip" ) );
    wBatchSize.addModifyListener( lsMod );
    FormData fdBatchSize = new FormData();
    fdBatchSize.left = new FormAttachment( middle, 0 );
    fdBatchSize.top = new FormAttachment( wMaxErrors, margin );
    fdBatchSize.right = new FormAttachment( 100, 0 );
    wBatchSize.setLayoutData(fdBatchSize);

    // Rows per Batch
    // Kilobytes per Batch
    Label wlRowsPerBatch = new Label(wAdvancedComp, SWT.RIGHT);
    wlRowsPerBatch.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.RowsPerBatch.Label" ) );
    props.setLook(wlRowsPerBatch);
    FormData fdlRowsPerBatch = new FormData();
    fdlRowsPerBatch.left = new FormAttachment( 0, 0 );
    fdlRowsPerBatch.right = new FormAttachment( middle, 0 );
    fdlRowsPerBatch.top = new FormAttachment( wBatchSize, margin );
    wlRowsPerBatch.setLayoutData(fdlRowsPerBatch);

    wRowsPerBatch = new TextVar( variables, wAdvancedComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wRowsPerBatch );
    wRowsPerBatch.setToolTipText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.RowsPerBatch.Label" ) );
    wRowsPerBatch.addModifyListener( lsMod );
    FormData fdRowsPerBatch = new FormData();
    fdRowsPerBatch.left = new FormAttachment( middle, 0 );
    fdRowsPerBatch.top = new FormAttachment( wBatchSize, margin );
    fdRowsPerBatch.right = new FormAttachment( 100, 0 );
    wRowsPerBatch.setLayoutData(fdRowsPerBatch);

    // fileresult grouping?
    // ////////////////////////
    // START OF FILE RESULT GROUP///
    // /
    // Add File to result
    Group wFileResult = new Group(wAdvancedComp, SWT.SHADOW_NONE);
    props.setLook(wFileResult);
    wFileResult.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.FileResult.Group.Label" ) );

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;

    wFileResult.setLayout( groupLayout );

    // Add file to result
    Label wlAddFileToResult = new Label(wFileResult, SWT.RIGHT);
    wlAddFileToResult.setText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.AddFileToResult.Label" ) );
    props.setLook(wlAddFileToResult);
    FormData fdlAddFileToResult = new FormData();
    fdlAddFileToResult.left = new FormAttachment( 0, 0 );
    fdlAddFileToResult.top = new FormAttachment( wRowsPerBatch, margin );
    fdlAddFileToResult.right = new FormAttachment( middle, -margin );
    wlAddFileToResult.setLayoutData(fdlAddFileToResult);
    wAddFileToResult = new Button(wFileResult, SWT.CHECK );
    props.setLook( wAddFileToResult );
    wAddFileToResult.setToolTipText( BaseMessages.getString( PKG, "JobMssqlBulkLoad.AddFileToResult.Tooltip" ) );
    FormData fdAddFileToResult = new FormData();
    fdAddFileToResult.left = new FormAttachment( middle, 0 );
    fdAddFileToResult.top = new FormAttachment( wRowsPerBatch, margin );
    fdAddFileToResult.right = new FormAttachment( 100, 0 );
    wAddFileToResult.setLayoutData(fdAddFileToResult);
    wAddFileToResult.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    FormData fdFileResult = new FormData();
    fdFileResult.left = new FormAttachment( 0, margin );
    fdFileResult.top = new FormAttachment( wRowsPerBatch, margin );
    fdFileResult.right = new FormAttachment( 100, -margin );
    wFileResult.setLayoutData(fdFileResult);
    // ///////////////////////////////////////////////////////////
    // / END OF FilesResult GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdAdvancedComp = new FormData();
    fdAdvancedComp.left = new FormAttachment( 0, 0 );
    fdAdvancedComp.top = new FormAttachment( 0, 0 );
    fdAdvancedComp.right = new FormAttachment( 100, 0 );
    fdAdvancedComp.bottom = new FormAttachment( 500, -margin );
    wAdvancedComp.setLayoutData(fdAdvancedComp);

    wAdvancedComp.layout();
    wAdvancedTab.setControl(wAdvancedComp);
    props.setLook(wAdvancedComp);

    // ////////////////////////
    // END OF Advanced TAB ///
    // ////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( 100, -50 );
    wTabFolder.setLayoutData(fdTabFolder);

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] {wOk, wCancel}, margin, wTabFolder);
    // Add listeners
    Listener lsCancel = e -> cancel();
    Listener lsOk = e -> ok();

    wCancel.addListener( SWT.Selection, lsCancel);
    wOk.addListener( SWT.Selection, lsOk);

    SelectionAdapter lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected(SelectionEvent e) {
        ok();
      }
    };

    wName.addSelectionListener(lsDef);
    wTablename.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    setDataType();
    setCodeType();
    BaseTransformDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "JobMssqlBulkLoadDialogSize" );
    wTabFolder.setSelection( 0 );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  private void setDataType() {
    if ( wDataFiletype.getSelectionIndex() == 0 || wDataFiletype.getSelectionIndex() == 2 ) {
      wFieldTerminator.setEnabled( true );
      wlFieldTerminator.setEnabled( true );
    } else {
      wFieldTerminator.setEnabled( false );
      wlFieldTerminator.setEnabled( false );
    }
  }

  private void setCodeType() {
    if ( wCodePage.getSelectionIndex() == 3 ) {
      wSpecificCodePage.setEnabled( true );
      wlSpecificCodePage.setEnabled( true );
    } else {
      wSpecificCodePage.setEnabled( false );
      wlSpecificCodePage.setEnabled( false );
    }
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wName.setText( Const.NVL( action.getName(), "" ) );
    if ( action.getDatabase() != null ) {
      wConnection.setText( action.getDatabase().getName() );
    }
    if ( action.getSchemaname() != null ) {
      wSchemaname.setText( action.getSchemaname() );
    }
    if ( action.getTablename() != null ) {
      wTablename.setText( action.getTablename() );
    }
    if ( action.getFilename() != null ) {
      wFilename.setText( action.getFilename() );
    }
    if ( action.getDataFileType() != null ) {
      wDataFiletype.setText( action.getDataFileType() );
    }
    if ( action.getFieldTerminator() != null ) {
      wFieldTerminator.setText( action.getFieldTerminator() );
    }
    if ( action.getLineterminated() != null ) {
      wLineterminated.setText( action.getLineterminated() );
    }
    if ( action.getCodePage() != null ) {
      wCodePage.setText( action.getCodePage() );
    } else {
      wCodePage.setText( "RAW" );
    }
    if ( action.getSpecificCodePage() != null ) {
      wSpecificCodePage.setText( action.getSpecificCodePage() );
    }
    if ( action.getFormatFilename() != null ) {
      wFormatFilename.setText( action.getFormatFilename() );
    }

    wFireTriggers.setSelection( action.isFireTriggers() );
    wCheckConstraints.setSelection( action.isCheckConstraints() );
    wKeepNulls.setSelection( action.isKeepNulls() );
    wKeepIdentity.setSelection( action.isKeepIdentity() );

    wTablock.setSelection( action.isTablock() );

    wStartFile.setText( "" + action.getStartFile() );
    wEndFile.setText( "" + action.getEndFile() );

    if ( action.getOrderBy() != null ) {
      wOrderBy.setText( action.getOrderBy() );
    }
    if ( action.getOrderDirection() != null ) {
      if ( action.getOrderDirection().equals( "Asc" ) ) {
        wOrderDirection.select( 0 );
      } else {
        wOrderDirection.select( 1 );
      }
    } else {
      wOrderDirection.select( 0 );
    }

    if ( action.getErrorFilename() != null ) {
      wErrorFilename.setText( action.getErrorFilename() );
    }

    wMaxErrors.setText( "" + action.getMaxErrors() );
    wBatchSize.setText( "" + action.getBatchSize() );
    wRowsPerBatch.setText( "" + action.getRowsPerBatch() );

    wAddDateTime.setSelection( action.isAddDatetime() );

    wAddFileToResult.setSelection( action.isAddFileToResult() );
    wTruncate.setSelection( action.isTruncate() );

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    action.setChanged( changed );
    action = null;
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "System.TransformActionNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.ActionNameMissing.Msg" ) );
      mb.open();
      return;
    }
    action.setName( wName.getText() );
    action.setDatabase( getWorkflowMeta().findDatabase( wConnection.getText() ) );
    action.setSchemaname( wSchemaname.getText() );
    action.setTablename( wTablename.getText() );
    action.setFilename( wFilename.getText() );
    action.setDataFileType( wDataFiletype.getText() );
    action.setFieldTerminator( wFieldTerminator.getText() );
    action.setLineterminated( wLineterminated.getText() );
    action.setCodePage( wCodePage.getText() );
    action.setSpecificCodePage( wSpecificCodePage.getText() );
    action.setFormatFilename( wFormatFilename.getText() );
    action.setFireTriggers( wFireTriggers.getSelection() );
    action.setCheckConstraints( wCheckConstraints.getSelection() );
    action.setKeepNulls( wKeepNulls.getSelection() );
    action.setKeepIdentity( wKeepIdentity.getSelection() );

    action.setTablock( wTablock.getSelection() );

    action.setStartFile( Const.toInt( wStartFile.getText(), 0 ) );
    action.setEndFile( Const.toInt( wEndFile.getText(), 0 ) );
    action.setOrderBy( wOrderBy.getText() );
    if ( wOrderDirection.getSelectionIndex() == 0 ) {
      action.setOrderDirection( "Asc" );
    } else {
      action.setOrderDirection( "Desc" );
    }

    action.setErrorFilename( wErrorFilename.getText() );
    action.setMaxErrors( Const.toInt( wMaxErrors.getText(), 0 ) );
    action.setBatchSize( Const.toInt( wBatchSize.getText(), 0 ) );
    action.setRowsPerBatch( Const.toInt( wRowsPerBatch.getText(), 0 ) );

    action.setAddDatetime( wAddDateTime.getSelection() );

    action.setAddFileToResult( wAddFileToResult.getSelection() );
    action.setTruncate( wTruncate.getSelection() );

    dispose();
  }

  private void getTableName() {
    String connectionName = wConnection.getText();
    if ( StringUtils.isEmpty( connectionName ) ) {
      return;
    }
        
    DatabaseMeta databaseMeta = getWorkflowMeta().findDatabase( connectionName );
    if ( databaseMeta != null ) {
      DatabaseExplorerDialog std = new DatabaseExplorerDialog( shell, SWT.NONE, variables, databaseMeta, getWorkflowMeta().getDatabases() );
      std.setSelectedSchemaAndTable( wSchemaname.getText(), wTablename.getText() );
      if ( std.open() ) {
        wTablename.setText( Const.NVL( std.getTableName(), "" ) );
      }
    } else {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "JobMssqlBulkLoad.ConnectionError2.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
      mb.open();
    }
  }

  /**
   * Get a list of columns, comma separated, allow the user to select from it.
   */
  private void getListColumns() {
    if ( !Utils.isEmpty( wTablename.getText() ) ) {
      DatabaseMeta databaseMeta = getWorkflowMeta().findDatabase( wConnection.getText() );
      if ( databaseMeta != null ) {
        Database database = new Database( loggingObject, variables, databaseMeta );
        try {
          database.connect();
          IRowMeta row =
            database.getTableFieldsMeta( wSchemaname.getText(), wTablename.getText() );
          String[] available = row.getFieldNames();

          String[] source = wOrderBy.getText().split( "," );
          for ( int i = 0; i < source.length; i++ ) {
            source[ i ] = Const.trim( source[ i ] );
          }
          int[] idxSource = Const.indexsOfStrings( source, available );
          EnterSelectionDialog dialog = new EnterSelectionDialog( shell, available,
            BaseMessages.getString( PKG, "JobMssqlBulkLoad.SelectColumns.Title" ),
            BaseMessages.getString( PKG, "JobMssqlBulkLoad.SelectColumns.Message" ) );
          dialog.setMulti( true );
          dialog.setAvoidQuickSearch();
          dialog.setSelectedNrs( idxSource );
          if ( dialog.open() != null ) {
            String columns = "";
            int[] idx = dialog.getSelectionIndeces();
            for ( int i = 0; i < idx.length; i++ ) {
              if ( i > 0 ) {
                columns += ", ";
              }
              columns += available[ idx[ i ] ];
            }
            wOrderBy.setText( columns );
          }
        } catch ( HopDatabaseException e ) {
          new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.Error.Title" ), BaseMessages
            .getString( PKG, "JobMssqlBulkLoad.ConnectionError2.DialogMessage" ), e );
        } finally {
          database.disconnect();
        }
      }
    }
  }
}
