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

package org.apache.hop.pipeline.transforms.sqlfileoutput;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.Props;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.database.dialog.SqlEditor;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.variables.IVariables;

public class SQLFileOutputDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = SQLFileOutputMeta.class; // For Translator

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TextVar wSchema;

  private TextVar wTable;

  private Label wlTruncate;
  private Button wTruncate;

  private Button wStartNewLine;

  private Button wAddToResult;

  private Button wAddCreate;

  private TextVar wFilename;

  private TextVar wExtension;

  private Button wAddTransformNr;

  private Button wAddDate;

  private Button wAddTime;

  private Button wAppend;

  private Text wSplitEvery;

  private CCombo wEncoding;

  private CCombo wFormat;

  private boolean gotEncodings = false;

  private Button wCreateParentFolder;

  private Button wDoNotOpenNewFileInit;

  private final SQLFileOutputMeta input;

  public SQLFileOutputDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (SQLFileOutputMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();
    backupChanged = input.hasChanged();

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "SQLFileOutputDialog.DialogTitle" ) );

    // Some buttons
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wCreate = new Button( shell, SWT.PUSH );
    wCreate.setText( BaseMessages.getString( PKG, "System.Button.SQL" ) );
    wCreate.addListener( SWT.Selection, e -> sql() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    setButtonPositions( new Button[] { wOk, wCreate, wCancel }, margin, null );

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "System.Label.TransformName" ) );
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

    CTabFolder wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    CTabItem wGeneralTab = new CTabItem( wTabFolder, SWT.NONE );
    wGeneralTab.setText( BaseMessages.getString( PKG, "SQLFileOutputDialog.GeneralTab.TabTitle" ) );

    Composite wGeneralComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wGeneralComp );

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout( generalLayout );

    // Connection grouping?
    // ////////////////////////
    // START OF Connection GROUP
    //

    Group wGConnection = new Group( wGeneralComp, SWT.SHADOW_NONE );
    props.setLook( wGConnection );
    wGConnection.setText( BaseMessages.getString( PKG, "SQLFileOutputDialog.Group.ConnectionInfos.Label" ) );

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;
    wGConnection.setLayout( groupLayout );

    // Connection line
    wConnection = addConnectionLine( wGConnection, wTransformName, input.getDatabaseMeta(), lsMod );

    // Schema line...
    Label wlSchema = new Label( wGConnection, SWT.RIGHT );
    wlSchema.setText( BaseMessages.getString( PKG, "SQLFileOutputDialog.TargetSchema.Label" ) );
    props.setLook( wlSchema );
    FormData fdlSchema = new FormData();
    fdlSchema.left = new FormAttachment( 0, 0 );
    fdlSchema.right = new FormAttachment( middle, -margin );
    fdlSchema.top = new FormAttachment( wConnection, margin );
    wlSchema.setLayoutData( fdlSchema );

    wSchema = new TextVar( variables, wGConnection, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSchema );
    wSchema.addModifyListener( lsMod );
    wSchema.setToolTipText( BaseMessages.getString( PKG, "SQLFileOutputDialog.TargetSchema.Tooltip" ) );
    FormData fdSchema = new FormData();
    fdSchema.left = new FormAttachment( middle, 0 );
    fdSchema.top = new FormAttachment( wConnection, margin );
    fdSchema.right = new FormAttachment( 100, 0 );
    wSchema.setLayoutData( fdSchema );

    // Table line...
    Label wlTable = new Label( wGConnection, SWT.RIGHT );
    wlTable.setText( BaseMessages.getString( PKG, "SQLFileOutputDialog.TargetTable.Label" ) );
    props.setLook( wlTable );
    FormData fdlTable = new FormData();
    fdlTable.left = new FormAttachment( 0, 0 );
    fdlTable.right = new FormAttachment( middle, -margin );
    fdlTable.top = new FormAttachment( wSchema, margin );
    wlTable.setLayoutData( fdlTable );

    Button wbTable = new Button( wGConnection, SWT.PUSH | SWT.CENTER );
    props.setLook( wbTable );
    wbTable.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment( 100, 0 );
    fdbTable.top = new FormAttachment( wSchema, margin );
    wbTable.setLayoutData( fdbTable );

    wTable = new TextVar( variables, wGConnection, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTable );
    wTable.setToolTipText( BaseMessages.getString( PKG, "SQLFileOutputDialog.TargetTable.Tooltip" ) );
    wTable.addModifyListener( lsMod );
    FormData fdTable = new FormData();
    fdTable.top = new FormAttachment( wSchema, margin );
    fdTable.left = new FormAttachment( middle, 0 );
    fdTable.right = new FormAttachment( wbTable, -margin );
    wTable.setLayoutData( fdTable );

    FormData fdGConnection = new FormData();
    fdGConnection.left = new FormAttachment( 0, margin );
    fdGConnection.top = new FormAttachment( wTransformName, margin );
    fdGConnection.right = new FormAttachment( 100, -margin );
    wGConnection.setLayoutData( fdGConnection );

    // ///////////////////////////////////////////////////////////
    // / END OF Connection GROUP
    // ///////////////////////////////////////////////////////////

    // Connection grouping?
    // ////////////////////////
    // START OF FileName GROUP
    //

    Group wFileName = new Group( wGeneralComp, SWT.SHADOW_NONE );
    props.setLook( wFileName );
    wFileName.setText( BaseMessages.getString( PKG, "SQLFileOutputDialog.Group.File.Label" ) );

    FormLayout groupFileLayout = new FormLayout();
    groupFileLayout.marginWidth = 10;
    groupFileLayout.marginHeight = 10;
    wFileName.setLayout( groupFileLayout );

    // Add Create table
    Label wlAddCreate = new Label( wFileName, SWT.RIGHT );
    wlAddCreate.setText( BaseMessages.getString( PKG, "SQLFileOutputDialog.CreateTable.Label" ) );
    props.setLook( wlAddCreate );
    FormData fdlAddCreate = new FormData();
    fdlAddCreate.left = new FormAttachment( 0, 0 );
    fdlAddCreate.top = new FormAttachment( wGConnection, margin );
    fdlAddCreate.right = new FormAttachment( middle, -margin );
    wlAddCreate.setLayoutData( fdlAddCreate );
    wAddCreate = new Button( wFileName, SWT.CHECK );
    wAddCreate.setToolTipText( BaseMessages.getString( PKG, "SQLFileOutputDialog.CreateTable.Tooltip" ) );
    props.setLook( wAddCreate );
    FormData fdAddCreate = new FormData();
    fdAddCreate.left = new FormAttachment( middle, 0 );
    fdAddCreate.top = new FormAttachment( wlAddCreate, 0, SWT.CENTER );
    fdAddCreate.right = new FormAttachment( 100, 0 );
    wAddCreate.setLayoutData( fdAddCreate );
    SelectionAdapter lsSelMod = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        ActiveTruncate();
        input.setChanged();
      }
    };
    wAddCreate.addSelectionListener( lsSelMod );

    // Truncate table
    wlTruncate = new Label( wFileName, SWT.RIGHT );
    wlTruncate.setText( BaseMessages.getString( PKG, "SQLFileOutputDialog.TruncateTable.Label" ) );
    props.setLook( wlTruncate );
    FormData fdlTruncate = new FormData();
    fdlTruncate.left = new FormAttachment( 0, 0 );
    fdlTruncate.top = new FormAttachment( wAddCreate, margin );
    fdlTruncate.right = new FormAttachment( middle, -margin );
    wlTruncate.setLayoutData( fdlTruncate );
    wTruncate = new Button( wFileName, SWT.CHECK );
    wTruncate.setToolTipText( BaseMessages.getString( PKG, "SQLFileOutputDialog.TruncateTable.Tooltip" ) );
    props.setLook( wTruncate );
    FormData fdTruncate = new FormData();
    fdTruncate.left = new FormAttachment( middle, 0 );
    fdTruncate.top = new FormAttachment( wlTruncate, 0, SWT.CENTER );
    fdTruncate.right = new FormAttachment( 100, 0 );
    wTruncate.setLayoutData( fdTruncate );
    SelectionAdapter lsSelTMod = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };
    wTruncate.addSelectionListener( lsSelTMod );

    // Start New Line For each statement
    Label wlStartNewLine = new Label( wFileName, SWT.RIGHT );
    wlStartNewLine.setText( BaseMessages.getString( PKG, "SQLFileOutputDialog.StartNewLine.Label" ) );
    props.setLook( wlStartNewLine );
    FormData fdlStartNewLine = new FormData();
    fdlStartNewLine.left = new FormAttachment( 0, 0 );
    fdlStartNewLine.top = new FormAttachment( wTruncate, margin );
    fdlStartNewLine.right = new FormAttachment( middle, -margin );
    wlStartNewLine.setLayoutData( fdlStartNewLine );
    wStartNewLine = new Button( wFileName, SWT.CHECK );
    wStartNewLine.setToolTipText( BaseMessages.getString( PKG, "SQLFileOutputDialog.StartNewLine.Label" ) );
    props.setLook( wStartNewLine );
    FormData fdStartNewLine = new FormData();
    fdStartNewLine.left = new FormAttachment( middle, 0 );
    fdStartNewLine.top = new FormAttachment( wlStartNewLine, 0, SWT.CENTER );
    fdStartNewLine.right = new FormAttachment( 100, 0 );
    wStartNewLine.setLayoutData( fdStartNewLine );
    SelectionAdapter lsSelSMod = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };
    wStartNewLine.addSelectionListener( lsSelSMod );

    // Filename line
    Label wlFilename = new Label( wFileName, SWT.RIGHT );
    wlFilename.setText( BaseMessages.getString( PKG, "SQLFileOutputDialog.Filename.Label" ) );
    props.setLook( wlFilename );
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment( wStartNewLine, margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData( fdlFilename );

    Button wbFilename = new Button( wFileName, SWT.PUSH | SWT.CENTER );
    props.setLook( wbFilename );
    wbFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment( wStartNewLine, 0 );
    wbFilename.setLayoutData( fdbFilename );

    wFilename = new TextVar( variables, wFileName, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.top = new FormAttachment( wStartNewLine, margin );
    fdFilename.right = new FormAttachment( wbFilename, -margin );
    wFilename.setLayoutData( fdFilename );

    // Create Parent Folder
    Label wlCreateParentFolder = new Label( wFileName, SWT.RIGHT );
    wlCreateParentFolder.setText( BaseMessages.getString( PKG, "SQLFileOutputDialog.CreateParentFolder.Label" ) );
    props.setLook( wlCreateParentFolder );
    FormData fdlCreateParentFolder = new FormData();
    fdlCreateParentFolder.left = new FormAttachment( 0, 0 );
    fdlCreateParentFolder.top = new FormAttachment( wFilename, margin );
    fdlCreateParentFolder.right = new FormAttachment( middle, -margin );
    wlCreateParentFolder.setLayoutData( fdlCreateParentFolder );
    wCreateParentFolder = new Button( wFileName, SWT.CHECK );
    wCreateParentFolder.setToolTipText( BaseMessages.getString(
      PKG, "SQLFileOutputDialog.CreateParentFolder.Tooltip" ) );
    props.setLook( wCreateParentFolder );
    FormData fdCreateParentFolder = new FormData();
    fdCreateParentFolder.left = new FormAttachment( middle, 0 );
    fdCreateParentFolder.top = new FormAttachment( wlCreateParentFolder, 0, SWT.CENTER );
    fdCreateParentFolder.right = new FormAttachment( 100, 0 );
    wCreateParentFolder.setLayoutData( fdCreateParentFolder );
    wCreateParentFolder.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Open new File at Init
    Label wlDoNotOpenNewFileInit = new Label( wFileName, SWT.RIGHT );
    wlDoNotOpenNewFileInit.setText( BaseMessages.getString( PKG, "SQLFileOutputDialog.DoNotOpenNewFileInit.Label" ) );
    props.setLook( wlDoNotOpenNewFileInit );
    FormData fdlDoNotOpenNewFileInit = new FormData();
    fdlDoNotOpenNewFileInit.left = new FormAttachment( 0, 0 );
    fdlDoNotOpenNewFileInit.top = new FormAttachment( wCreateParentFolder, margin );
    fdlDoNotOpenNewFileInit.right = new FormAttachment( middle, -margin );
    wlDoNotOpenNewFileInit.setLayoutData( fdlDoNotOpenNewFileInit );
    wDoNotOpenNewFileInit = new Button( wFileName, SWT.CHECK );
    wDoNotOpenNewFileInit.setToolTipText( BaseMessages.getString( PKG, "SQLFileOutputDialog.DoNotOpenNewFileInit.Tooltip" ) );
    props.setLook( wDoNotOpenNewFileInit );
    FormData fdDoNotOpenNewFileInit = new FormData();
    fdDoNotOpenNewFileInit.left = new FormAttachment( middle, 0 );
    fdDoNotOpenNewFileInit.top = new FormAttachment( wlDoNotOpenNewFileInit, 0, SWT.CENTER );
    fdDoNotOpenNewFileInit.right = new FormAttachment( 100, 0 );
    wDoNotOpenNewFileInit.setLayoutData( fdDoNotOpenNewFileInit );
    wDoNotOpenNewFileInit.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Extension line
    Label wlExtension = new Label( wFileName, SWT.RIGHT );
    wlExtension.setText( BaseMessages.getString( PKG, "System.Label.Extension" ) );
    props.setLook( wlExtension );
    FormData fdlExtension = new FormData();
    fdlExtension.left = new FormAttachment( 0, 0 );
    fdlExtension.top = new FormAttachment( wDoNotOpenNewFileInit, margin );
    fdlExtension.right = new FormAttachment( middle, -margin );
    wlExtension.setLayoutData( fdlExtension );

    wExtension = new TextVar( variables, wFileName, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wExtension );
    wExtension.addModifyListener( lsMod );
    FormData fdExtension = new FormData();
    fdExtension.left = new FormAttachment( middle, 0 );
    fdExtension.top = new FormAttachment( wDoNotOpenNewFileInit, margin );
    fdExtension.right = new FormAttachment( 100, -margin );
    wExtension.setLayoutData( fdExtension );

    // Create multi-part file?
    Label wlAddTransformNr = new Label( wFileName, SWT.RIGHT );
    wlAddTransformNr.setText( BaseMessages.getString( PKG, "SQLFileOutputDialog.AddTransformnr.Label" ) );
    props.setLook( wlAddTransformNr );
    FormData fdlAddTransformNr = new FormData();
    fdlAddTransformNr.left = new FormAttachment( 0, 0 );
    fdlAddTransformNr.top = new FormAttachment( wExtension, 2 * margin );
    fdlAddTransformNr.right = new FormAttachment( middle, -margin );
    wlAddTransformNr.setLayoutData( fdlAddTransformNr );
    wAddTransformNr = new Button( wFileName, SWT.CHECK );
    props.setLook( wAddTransformNr );
    FormData fdAddTransformNr = new FormData();
    fdAddTransformNr.left = new FormAttachment( middle, 0 );
    fdAddTransformNr.top = new FormAttachment( wlAddTransformNr, 0, SWT.CENTER );
    fdAddTransformNr.right = new FormAttachment( 100, 0 );
    wAddTransformNr.setLayoutData( fdAddTransformNr );
    wAddTransformNr.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Create multi-part file?
    Label wlAddDate = new Label( wFileName, SWT.RIGHT );
    wlAddDate.setText( BaseMessages.getString( PKG, "SQLFileOutputDialog.AddDate.Label" ) );
    props.setLook( wlAddDate );
    FormData fdlAddDate = new FormData();
    fdlAddDate.left = new FormAttachment( 0, 0 );
    fdlAddDate.top = new FormAttachment( wAddTransformNr, margin );
    fdlAddDate.right = new FormAttachment( middle, -margin );
    wlAddDate.setLayoutData( fdlAddDate );
    wAddDate = new Button( wFileName, SWT.CHECK );
    props.setLook( wAddDate );
    FormData fdAddDate = new FormData();
    fdAddDate.left = new FormAttachment( middle, 0 );
    fdAddDate.top = new FormAttachment( wlAddDate, 0, SWT.CENTER );
    fdAddDate.right = new FormAttachment( 100, 0 );
    wAddDate.setLayoutData( fdAddDate );
    wAddDate.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );
    // Create multi-part file?
    Label wlAddTime = new Label( wFileName, SWT.RIGHT );
    wlAddTime.setText( BaseMessages.getString( PKG, "SQLFileOutputDialog.AddTime.Label" ) );
    props.setLook( wlAddTime );
    FormData fdlAddTime = new FormData();
    fdlAddTime.left = new FormAttachment( 0, 0 );
    fdlAddTime.top = new FormAttachment( wAddDate, margin );
    fdlAddTime.right = new FormAttachment( middle, -margin );
    wlAddTime.setLayoutData( fdlAddTime );
    wAddTime = new Button( wFileName, SWT.CHECK );
    props.setLook( wAddTime );
    FormData fdAddTime = new FormData();
    fdAddTime.left = new FormAttachment( middle, 0 );
    fdAddTime.top = new FormAttachment( wlAddTime, 0, SWT.CENTER );
    fdAddTime.right = new FormAttachment( 100, 0 );
    wAddTime.setLayoutData( fdAddTime );
    wAddTime.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Append to end of file?
    Label wlAppend = new Label( wFileName, SWT.RIGHT );
    wlAppend.setText( BaseMessages.getString( PKG, "SQLFileOutputDialog.Append.Label" ) );
    props.setLook( wlAppend );
    FormData fdlAppend = new FormData();
    fdlAppend.left = new FormAttachment( 0, 0 );
    fdlAppend.top = new FormAttachment( wAddTime, margin );
    fdlAppend.right = new FormAttachment( middle, -margin );
    wlAppend.setLayoutData( fdlAppend );
    wAppend = new Button( wFileName, SWT.CHECK );
    wAppend.setToolTipText( BaseMessages.getString( PKG, "SQLFileOutputDialog.Append.Tooltip" ) );
    props.setLook( wAppend );
    FormData fdAppend = new FormData();
    fdAppend.left = new FormAttachment( middle, 0 );
    fdAppend.top = new FormAttachment( wlAppend, 0, SWT.CENTER );
    fdAppend.right = new FormAttachment( 100, 0 );
    wAppend.setLayoutData( fdAppend );
    wAppend.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    Label wlSplitEvery = new Label( wFileName, SWT.RIGHT );
    wlSplitEvery.setText( BaseMessages.getString( PKG, "SQLFileOutputDialog.SplitEvery.Label" ) );
    props.setLook( wlSplitEvery );
    FormData fdlSplitEvery = new FormData();
    fdlSplitEvery.left = new FormAttachment( 0, 0 );
    fdlSplitEvery.top = new FormAttachment( wAppend, margin );
    fdlSplitEvery.right = new FormAttachment( middle, -margin );
    wlSplitEvery.setLayoutData( fdlSplitEvery );

    wSplitEvery = new Text( wFileName, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSplitEvery );
    wSplitEvery.addModifyListener( lsMod );
    FormData fdSplitEvery = new FormData();
    fdSplitEvery.left = new FormAttachment( middle, 0 );
    fdSplitEvery.top = new FormAttachment( wAppend, margin );
    fdSplitEvery.right = new FormAttachment( 100, 0 );
    wSplitEvery.setLayoutData( fdSplitEvery );

    Button wbShowFiles = new Button( wFileName, SWT.PUSH | SWT.CENTER );
    props.setLook( wbShowFiles );
    wbShowFiles.setText( BaseMessages.getString( PKG, "SQLFileOutputDialog.ShowFiles.Button" ) );
    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment( middle, 0 );
    fdbShowFiles.top = new FormAttachment( wSplitEvery, margin * 2 );
    wbShowFiles.setLayoutData( fdbShowFiles );
    wbShowFiles.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        SQLFileOutputMeta tfoi = new SQLFileOutputMeta();
        getInfo( tfoi );
        String[] files = tfoi.getFiles( variables, variables.resolve( wFilename.getText() ) );
        if ( files != null && files.length > 0 ) {
          EnterSelectionDialog esd = new EnterSelectionDialog( shell, files,
            BaseMessages.getString( PKG, "SQLFileOutputDialog.SelectOutputFiles.DialogTitle" ),
            BaseMessages.getString( PKG, "SQLFileOutputDialog.SelectOutputFiles.DialogMessage" ) );
          esd.setViewOnly();
          esd.open();
        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
          mb.setMessage( BaseMessages.getString( PKG, "SQLFileOutputDialog.NoFilesFound.DialogMessage" ) );
          mb.setText( BaseMessages.getString( PKG, "System.DialogTitle.Error" ) );
          mb.open();
        }
      }
    } );

    // Add File to the result files name
    Label wlAddToResult = new Label( wFileName, SWT.RIGHT );
    wlAddToResult.setText( BaseMessages.getString( PKG, "SQLFileOutputDialog.AddFileToResult.Label" ) );
    props.setLook( wlAddToResult );
    FormData fdlAddToResult = new FormData();
    fdlAddToResult.left = new FormAttachment( 0, 0 );
    fdlAddToResult.top = new FormAttachment( wbShowFiles, margin );
    fdlAddToResult.right = new FormAttachment( middle, -margin );
    wlAddToResult.setLayoutData( fdlAddToResult );
    wAddToResult = new Button( wFileName, SWT.CHECK );
    wAddToResult.setToolTipText( BaseMessages.getString( PKG, "SQLFileOutputDialog.AddFileToResult.Tooltip" ) );
    props.setLook( wAddToResult );
    FormData fdAddToResult = new FormData();
    fdAddToResult.left = new FormAttachment( middle, 0 );
    fdAddToResult.top = new FormAttachment( wlAddToResult, 0, SWT.CENTER );
    fdAddToResult.right = new FormAttachment( 100, 0 );
    wAddToResult.setLayoutData( fdAddToResult );
    SelectionAdapter lsSelR = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };
    wAddToResult.addSelectionListener( lsSelR );

    FormData fdFileName = new FormData();
    fdFileName.left = new FormAttachment( 0, margin );
    fdFileName.top = new FormAttachment( wGConnection, 2 * margin );
    fdFileName.right = new FormAttachment( 100, -margin );
    wFileName.setLayoutData( fdFileName );

    // ///////////////////////////////////////////////////////////
    // / END OF FileName GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( 0, 0 );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 100, 0 );
    wGeneralComp.setLayoutData( fdGeneralComp );

    wGeneralComp.layout();
    wGeneralTab.setControl( wGeneralComp );
    props.setLook( wGeneralComp );

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF CONTENT TAB///
    // /
    CTabItem wContentTab = new CTabItem( wTabFolder, SWT.NONE );
    wContentTab.setText( BaseMessages.getString( PKG, "SQLFileOutputDialog.ContentTab.TabTitle" ) );

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    Composite wContentComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wContentComp );
    wContentComp.setLayout( contentLayout );

    // Prepare a list of possible formats...
    String[] dats = Const.getDateFormats();

    // format
    Label wlFormat = new Label( wContentComp, SWT.RIGHT );
    wlFormat.setText( BaseMessages.getString( PKG, "SQLFileOutputDialog.DateFormat.Label" ) );
    props.setLook( wlFormat );
    FormData fdlFormat = new FormData();
    fdlFormat.left = new FormAttachment( 0, 0 );
    fdlFormat.top = new FormAttachment( 0, margin );
    fdlFormat.right = new FormAttachment( middle, -margin );
    wlFormat.setLayoutData( fdlFormat );
    wFormat = new CCombo( wContentComp, SWT.BORDER | SWT.READ_ONLY );
    wFormat.setEditable( true );
    props.setLook( wFormat );
    wFormat.addModifyListener( lsMod );
    FormData fdFormat = new FormData();
    fdFormat.left = new FormAttachment( middle, 0 );
    fdFormat.top = new FormAttachment( 0, margin );
    fdFormat.right = new FormAttachment( 100, 0 );
    wFormat.setLayoutData( fdFormat );

    for ( String dat : dats ) {
      wFormat.add( dat );
    }

    // Encoding
    Label wlEncoding = new Label( wContentComp, SWT.RIGHT );
    wlEncoding.setText( BaseMessages.getString( PKG, "SQLFileOutputDialog.Encoding.Label" ) );
    props.setLook( wlEncoding );
    FormData fdlEncoding = new FormData();
    fdlEncoding.left = new FormAttachment( 0, 0 );
    fdlEncoding.top = new FormAttachment( wFormat, margin );
    fdlEncoding.right = new FormAttachment( middle, -margin );
    wlEncoding.setLayoutData( fdlEncoding );
    wEncoding = new CCombo( wContentComp, SWT.BORDER | SWT.READ_ONLY );
    wEncoding.setEditable( true );
    props.setLook( wEncoding );
    wEncoding.addModifyListener( lsMod );
    FormData fdEncoding = new FormData();
    fdEncoding.left = new FormAttachment( middle, 0 );
    fdEncoding.top = new FormAttachment( wFormat, margin );
    fdEncoding.right = new FormAttachment( 100, 0 );
    wEncoding.setLayoutData( fdEncoding );
    wEncoding.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        setEncodings();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    FormData fdContentComp = new FormData();
    fdContentComp.left = new FormAttachment( 0, 0 );
    fdContentComp.top = new FormAttachment( 0, 0 );
    fdContentComp.right = new FormAttachment( 100, 0 );
    fdContentComp.bottom = new FormAttachment( 100, 0 );
    wContentComp.setLayoutData( wContentComp );

    wContentComp.layout();
    wContentTab.setControl( wContentComp );

    // ///////////////////////////////////////////////////////////
    // / END OF CONTENT TAB
    // ///////////////////////////////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wTransformName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOk, -2 * margin );
    wTabFolder.setLayoutData( fdTabFolder );


    // Add listeners

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );
    wSchema.addSelectionListener( lsDef );
    wTable.addSelectionListener( lsDef );

    wbTable.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        getTableName();
      }
    } );


    wbFilename.addListener( SWT.Selection, e -> BaseDialog.presentFileDialog( true, shell, wFilename, variables,
      new String[] { "*.sql", "*" },
      new String[] {
        "SQL File",
        BaseMessages.getString( PKG, "System.FileType.AllFiles" ) },
      true )
    );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    lsResize = event -> {
      // Point size = shell.getSize();

    };
    shell.addListener( SWT.Resize, lsResize );

    wTabFolder.setSelection( 0 );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    ActiveTruncate();
    input.setChanged( changed ); // backupChanged);

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void ActiveTruncate() {

    wlTruncate.setEnabled( !wAddCreate.getSelection() );
    wTruncate.setEnabled( !wAddCreate.getSelection() );
    if ( wAddCreate.getSelection() ) {
      wTruncate.setSelection( false );
    }

  }

  private void setEncodings() {
    // Encoding of the text file:
    if ( !gotEncodings ) {
      gotEncodings = true;

      wEncoding.removeAll();
      List<Charset> values = new ArrayList<>( Charset.availableCharsets().values() );
      for ( Charset charSet : values ) {
        wEncoding.add( charSet.displayName() );
      }

      // Now select the default!
      String defEncoding = Const.getEnvironmentVariable( "file.encoding", "UTF-8" );
      int idx = Const.indexOfString( defEncoding, wEncoding.getItems() );
      if ( idx >= 0 ) {
        wEncoding.select( idx );
      }
    }
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( input.getSchemaName() != null ) {
      wSchema.setText( input.getSchemaName() );
    }
    if ( input.getTablename() != null ) {
      wTable.setText( input.getTablename() );
    }
    if ( input.getDatabaseMeta() != null ) {
      wConnection.setText( input.getDatabaseMeta().getName() );
    }

    if ( input.getFileName() != null ) {
      wFilename.setText( input.getFileName() );
    }
    wCreateParentFolder.setSelection( input.isCreateParentFolder() );
    if ( input.getExtension() != null ) {
      wExtension.setText( input.getExtension() );
    } else {
      wExtension.setText( "sql" );
    }

    if ( input.getDateFormat() != null ) {
      wFormat.setText( input.getDateFormat() );
    }

    wSplitEvery.setText( "" + input.getSplitEvery() );
    wAddDate.setSelection( input.isDateInFilename() );
    wAddTime.setSelection( input.isTimeInFilename() );
    wAppend.setSelection( input.isFileAppended() );
    wAddTransformNr.setSelection( input.isTransformNrInFilename() );

    wTruncate.setSelection( input.truncateTable() );
    wAddCreate.setSelection( input.createTable() );

    if ( input.getEncoding() != null ) {
      wEncoding.setText( input.getEncoding() );
    }
    wAddToResult.setSelection( input.AddToResult() );
    wStartNewLine.setSelection( input.StartNewLine() );
    wDoNotOpenNewFileInit.setSelection( input.isDoNotOpenNewFileInit() );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( backupChanged );
    dispose();
  }

  private void getInfo( SQLFileOutputMeta info ) {
    info.setSchemaName( wSchema.getText() );
    info.setTablename( wTable.getText() );
    info.setDatabaseMeta( pipelineMeta.findDatabase( wConnection.getText() ) );
    info.setTruncateTable( wTruncate.getSelection() );
    info.setCreateParentFolder( wCreateParentFolder.getSelection() );

    info.setCreateTable( wAddCreate.getSelection() );

    info.setFileName( wFilename.getText() );
    info.setExtension( wExtension.getText() );
    info.setDateFormat( wFormat.getText() );
    info.setSplitEvery( Const.toInt( wSplitEvery.getText(), 0 ) );
    info.setFileAppended( wAppend.getSelection() );
    info.setTransformNrInFilename( wAddTransformNr.getSelection() );
    info.setDateInFilename( wAddDate.getSelection() );
    info.setTimeInFilename( wAddTime.getSelection() );

    info.setEncoding( wEncoding.getText() );
    info.setAddToResult( wAddToResult.getSelection() );
    info.setStartNewLine( wStartNewLine.getSelection() );
    info.setDoNotOpenNewFileInit( wDoNotOpenNewFileInit.getSelection() );

  }

  private void ok() {
    transformName = wTransformName.getText(); // return value

    getInfo( input );

    if ( input.getDatabaseMeta() == null ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "SQLFileOutputDialog.ConnectionError.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
      mb.open();
      return;
    }

    dispose();
  }

  private void getTableName() {
    String connectionName = wConnection.getText();
    if ( StringUtils.isEmpty( connectionName ) ) {
      return;
    }
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase( connectionName );
    if ( databaseMeta != null ) {

      logDebug( BaseMessages.getString( PKG, "SQLFileOutputDialog.Log.LookingAtConnection", databaseMeta.toString() ) );

      DatabaseExplorerDialog std = new DatabaseExplorerDialog( shell, SWT.NONE, variables, databaseMeta, pipelineMeta.getDatabases() );
      std.setSelectedSchemaAndTable( wSchema.getText(), wTable.getText() );
      if ( std.open() ) {
        wSchema.setText( Const.NVL( std.getSchemaName(), "" ) );
        wTable.setText( Const.NVL( std.getTableName(), "" ) );
      }
    } else {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "SQLFileOutputDialog.ConnectionError2.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
      mb.open();
    }

  }

  // Generate code for create table...
  // Conversions done by Database
  //
  private void sql() {
    try {
      SQLFileOutputMeta info = new SQLFileOutputMeta();
      getInfo( info );
      IRowMeta prev = pipelineMeta.getPrevTransformFields( variables, transformName );

      TransformMeta transformMeta = pipelineMeta.findTransform( transformName );

      SqlStatement sql = info.getSqlStatements( variables, pipelineMeta, transformMeta, prev, metadataProvider );
      if ( !sql.hasError() ) {
        if ( sql.hasSql() ) {
          SqlEditor sqledit =
            new SqlEditor( shell, SWT.NONE, variables,  info.getDatabaseMeta(), DbCache.getInstance(), sql
              .getSql() );
          sqledit.open();
        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
          mb.setMessage( BaseMessages.getString( PKG, "SQLFileOutputDialog.NoSQL.DialogMessage" ) );
          mb.setText( BaseMessages.getString( PKG, "SQLFileOutputDialog.NoSQL.DialogTitle" ) );
          mb.open();
        }
      } else {
        MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
        mb.setMessage( sql.getError() );
        mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
        mb.open();
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "SQLFileOutputDialog.BuildSQLError.DialogTitle" ), BaseMessages
        .getString( PKG, "SQLFileOutputDialog.BuildSQLError.DialogMessage" ), ke );
    }
  }
}
