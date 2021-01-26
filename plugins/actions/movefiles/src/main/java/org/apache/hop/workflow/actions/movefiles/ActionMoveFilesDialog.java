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

package org.apache.hop.workflow.actions.movefiles;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
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
 * This dialog allows you to edit the Move Files action settings.
 *
 * @author Samatar Hassan
 * @since 20-02-2008
 */
public class ActionMoveFilesDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionMoveFiles.class; // For Translator

  private static final String[] FILETYPES = new String[] { BaseMessages.getString(
    PKG, "JobMoveFiles.Filetype.All" ) };

  private Text wName;

  private Label wlSourceFileFolder;
  private Button wbSourceFileFolder, wbDestinationFileFolder, wbSourceDirectory, wbDestinationDirectory;

  private TextVar wSourceFileFolder;

  private Label wlMoveEmptyFolders;
  private Button wMoveEmptyFolders;

  private Button wIncludeSubfolders;

  private ActionMoveFiles action;
  private Shell shell;

  private boolean changed;

  private Button wPrevious;

  private Label wlFields;

  private TableView wFields;

  private Label wlDestinationFileFolder;
  private TextVar wDestinationFileFolder;

  private Label wlWildcard;
  private TextVar wWildcard;

  private Button wbdSourceFileFolder; // Delete
  private Button wbeSourceFileFolder; // Edit
  private Button wbaSourceFileFolder; // Add or change

  private Label wlCreateMoveToFolder;
  private Button wCreateMoveToFolder;

  // Add File to result

  private Button wAddFileToResult;

  private Button wCreateDestinationFolder;

  private Button wDestinationIsAFile;

  private CCombo wSuccessCondition;

  private Label wlNrErrorsLessThan;
  private TextVar wNrErrorsLessThan;

  private Label wlAddDate;
  private Button wAddDate;

  private Label wlAddTime;
  private Button wAddTime;

  private Button wSpecifyFormat;

  private Label wlDateTimeFormat;
  private CCombo wDateTimeFormat;

  private Label wlMovedDateTimeFormat;
  private CCombo wMovedDateTimeFormat;

  private Label wlAddDateBeforeExtension;
  private Button wAddDateBeforeExtension;

  private Label wlAddMovedDateBeforeExtension;
  private Button wAddMovedDateBeforeExtension;

  private Label wlDoNotKeepFolderStructure;
  private Button wDoNotKeepFolderStructure;

  private CCombo wIfFileExists;

  private Label wlIfMovedFileExists;
  private CCombo wIfMovedFileExists;

  private Button wbDestinationFolder;
  private Label wlDestinationFolder;
  private TextVar wDestinationFolder;

  private Label wlAddMovedDate;
  private Button wAddMovedDate;

  private Label wlAddMovedTime;
  private Button wAddMovedTime;

  private Label wlSpecifyMoveFormat;
  private Button wSpecifyMoveFormat;

  private Button wSimulate;

  public ActionMoveFilesDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = (ActionMoveFiles) action;

    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "JobMoveFiles.Name.Default" ) );
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
    shell.setText( BaseMessages.getString( PKG, "JobMoveFiles.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText( BaseMessages.getString( PKG, "JobMoveFiles.Name.Label" ) );
    props.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, -margin );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData(fdlName);
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    FormData fdName = new FormData();
    fdName.left = new FormAttachment( middle, 0 );
    fdName.top = new FormAttachment( 0, margin );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData(fdName);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText( BaseMessages.getString( PKG, "JobMoveFiles.Tab.General.Label" ) );

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout( generalLayout );

    // SETTINGS grouping?
    // ////////////////////////
    // START OF SETTINGS GROUP
    //

    Group wSettings = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wSettings);
    wSettings.setText( BaseMessages.getString( PKG, "JobMoveFiles.Settings.Label" ) );

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;
    wSettings.setLayout( groupLayout );

    Label wlIncludeSubfolders = new Label(wSettings, SWT.RIGHT);
    wlIncludeSubfolders.setText( BaseMessages.getString( PKG, "JobMoveFiles.IncludeSubfolders.Label" ) );
    props.setLook(wlIncludeSubfolders);
    FormData fdlIncludeSubfolders = new FormData();
    fdlIncludeSubfolders.left = new FormAttachment( 0, 0 );
    fdlIncludeSubfolders.top = new FormAttachment( wName, margin );
    fdlIncludeSubfolders.right = new FormAttachment( middle, -margin );
    wlIncludeSubfolders.setLayoutData(fdlIncludeSubfolders);
    wIncludeSubfolders = new Button(wSettings, SWT.CHECK );
    props.setLook( wIncludeSubfolders );
    wIncludeSubfolders.setToolTipText( BaseMessages.getString( PKG, "JobMoveFiles.IncludeSubfolders.Tooltip" ) );
    FormData fdIncludeSubfolders = new FormData();
    fdIncludeSubfolders.left = new FormAttachment( middle, 0 );
    fdIncludeSubfolders.top = new FormAttachment( wName, margin );
    fdIncludeSubfolders.right = new FormAttachment( 100, 0 );
    wIncludeSubfolders.setLayoutData(fdIncludeSubfolders);
    wIncludeSubfolders.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        CheckIncludeSubFolders();
      }
    } );

    // Copy empty folders
    wlMoveEmptyFolders = new Label(wSettings, SWT.RIGHT );
    wlMoveEmptyFolders.setText( BaseMessages.getString( PKG, "JobMoveFiles.MoveEmptyFolders.Label" ) );
    props.setLook( wlMoveEmptyFolders );
    FormData fdlMoveEmptyFolders = new FormData();
    fdlMoveEmptyFolders.left = new FormAttachment( 0, 0 );
    fdlMoveEmptyFolders.top = new FormAttachment( wIncludeSubfolders, margin );
    fdlMoveEmptyFolders.right = new FormAttachment( middle, -margin );
    wlMoveEmptyFolders.setLayoutData(fdlMoveEmptyFolders);
    wMoveEmptyFolders = new Button(wSettings, SWT.CHECK );
    props.setLook( wMoveEmptyFolders );
    wMoveEmptyFolders.setToolTipText( BaseMessages.getString( PKG, "JobMoveFiles.MoveEmptyFolders.Tooltip" ) );
    FormData fdMoveEmptyFolders = new FormData();
    fdMoveEmptyFolders.left = new FormAttachment( middle, 0 );
    fdMoveEmptyFolders.top = new FormAttachment( wIncludeSubfolders, margin );
    fdMoveEmptyFolders.right = new FormAttachment( 100, 0 );
    wMoveEmptyFolders.setLayoutData(fdMoveEmptyFolders);
    wMoveEmptyFolders.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Simulate?
    Label wlSimulate = new Label(wSettings, SWT.RIGHT);
    wlSimulate.setText( BaseMessages.getString( PKG, "JobMoveFiles.Simulate.Label" ) );
    props.setLook(wlSimulate);
    FormData fdlSimulate = new FormData();
    fdlSimulate.left = new FormAttachment( 0, 0 );
    fdlSimulate.top = new FormAttachment( wMoveEmptyFolders, margin );
    fdlSimulate.right = new FormAttachment( middle, -margin );
    wlSimulate.setLayoutData(fdlSimulate);
    wSimulate = new Button(wSettings, SWT.CHECK );
    props.setLook( wSimulate );
    wSimulate.setToolTipText( BaseMessages.getString( PKG, "JobMoveFiles.Simulate.Tooltip" ) );
    FormData fdSimulate = new FormData();
    fdSimulate.left = new FormAttachment( middle, 0 );
    fdSimulate.top = new FormAttachment( wMoveEmptyFolders, margin );
    fdSimulate.right = new FormAttachment( 100, 0 );
    wSimulate.setLayoutData(fdSimulate);
    wSimulate.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // previous
    Label wlPrevious = new Label(wSettings, SWT.RIGHT);
    wlPrevious.setText( BaseMessages.getString( PKG, "JobMoveFiles.Previous.Label" ) );
    props.setLook(wlPrevious);
    FormData fdlPrevious = new FormData();
    fdlPrevious.left = new FormAttachment( 0, 0 );
    fdlPrevious.top = new FormAttachment( wSimulate, margin );
    fdlPrevious.right = new FormAttachment( middle, -margin );
    wlPrevious.setLayoutData(fdlPrevious);
    wPrevious = new Button(wSettings, SWT.CHECK );
    props.setLook( wPrevious );
    wPrevious.setSelection( action.argFromPrevious );
    wPrevious.setToolTipText( BaseMessages.getString( PKG, "JobMoveFiles.Previous.Tooltip" ) );
    FormData fdPrevious = new FormData();
    fdPrevious.left = new FormAttachment( middle, 0 );
    fdPrevious.top = new FormAttachment( wSimulate, margin );
    fdPrevious.right = new FormAttachment( 100, 0 );
    wPrevious.setLayoutData(fdPrevious);
    wPrevious.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {

        RefreshArgFromPrevious();

      }
    } );
    FormData fdSettings = new FormData();
    fdSettings.left = new FormAttachment( 0, margin );
    fdSettings.top = new FormAttachment( wName, margin );
    fdSettings.right = new FormAttachment( 100, -margin );
    wSettings.setLayoutData(fdSettings);

    // ///////////////////////////////////////////////////////////
    // / END OF SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    // SourceFileFolder line
    wlSourceFileFolder = new Label(wGeneralComp, SWT.RIGHT );
    wlSourceFileFolder.setText( BaseMessages.getString( PKG, "JobMoveFiles.SourceFileFolder.Label" ) );
    props.setLook( wlSourceFileFolder );
    FormData fdlSourceFileFolder = new FormData();
    fdlSourceFileFolder.left = new FormAttachment( 0, 0 );
    fdlSourceFileFolder.top = new FormAttachment(wSettings, 2 * margin );
    fdlSourceFileFolder.right = new FormAttachment( middle, -margin );
    wlSourceFileFolder.setLayoutData( fdlSourceFileFolder );

    // Browse Source folders button ...
    wbSourceDirectory = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbSourceDirectory );
    wbSourceDirectory.setText( BaseMessages.getString( PKG, "JobMoveFiles.BrowseFolders.Label" ) );
    FormData fdbSourceDirectory = new FormData();
    fdbSourceDirectory.right = new FormAttachment( 100, 0 );
    fdbSourceDirectory.top = new FormAttachment(wSettings, margin );
    wbSourceDirectory.setLayoutData( fdbSourceDirectory );
    wbSourceDirectory.addListener( SWT.Selection, e-> BaseDialog.presentDirectoryDialog( shell, wSourceFileFolder, variables ) );

    // Browse Source files button ...
    wbSourceFileFolder = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbSourceFileFolder );
    wbSourceFileFolder.setText( BaseMessages.getString( PKG, "JobMoveFiles.BrowseFiles.Label" ) );
    FormData fdbSourceFileFolder = new FormData();
    fdbSourceFileFolder.right = new FormAttachment( wbSourceDirectory, -margin );
    fdbSourceFileFolder.top = new FormAttachment(wSettings, margin );
    wbSourceFileFolder.setLayoutData( fdbSourceFileFolder );

    // Browse Destination file add button ...
    wbaSourceFileFolder = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbaSourceFileFolder );
    wbaSourceFileFolder.setText( BaseMessages.getString( PKG, "JobMoveFiles.FilenameAdd.Button" ) );
    FormData fdbaSourceFileFolder = new FormData();
    fdbaSourceFileFolder.right = new FormAttachment( wbSourceFileFolder, -margin );
    fdbaSourceFileFolder.top = new FormAttachment(wSettings, margin );
    wbaSourceFileFolder.setLayoutData(fdbaSourceFileFolder);

    wSourceFileFolder = new TextVar( variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wSourceFileFolder.setToolTipText( BaseMessages.getString( PKG, "JobMoveFiles.SourceFileFolder.Tooltip" ) );

    props.setLook( wSourceFileFolder );
    wSourceFileFolder.addModifyListener( lsMod );
    FormData fdSourceFileFolder = new FormData();
    fdSourceFileFolder.left = new FormAttachment( middle, 0 );
    fdSourceFileFolder.top = new FormAttachment(wSettings, 2 * margin );
    fdSourceFileFolder.right = new FormAttachment( wbSourceFileFolder, -55 );
    wSourceFileFolder.setLayoutData( fdSourceFileFolder );

    // Whenever something changes, set the tooltip to the expanded version:
    wSourceFileFolder.addModifyListener( e -> wSourceFileFolder.setToolTipText( variables.resolve( wSourceFileFolder.getText() ) ) );

    wbSourceFileFolder.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wSourceFileFolder, variables,
      new String[] { "*" }, FILETYPES, true )
    );

    // Destination
    wlDestinationFileFolder = new Label(wGeneralComp, SWT.RIGHT );
    wlDestinationFileFolder.setText( BaseMessages.getString( PKG, "JobMoveFiles.DestinationFileFolder.Label" ) );
    props.setLook( wlDestinationFileFolder );
    FormData fdlDestinationFileFolder = new FormData();
    fdlDestinationFileFolder.left = new FormAttachment( 0, 0 );
    fdlDestinationFileFolder.top = new FormAttachment( wSourceFileFolder, margin );
    fdlDestinationFileFolder.right = new FormAttachment( middle, -margin );
    wlDestinationFileFolder.setLayoutData(fdlDestinationFileFolder);

    // Browse Destination folders button ...
    wbDestinationDirectory = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbDestinationDirectory );
    wbDestinationDirectory.setText( BaseMessages.getString( PKG, "JobMoveFiles.BrowseFolders.Label" ) );
    FormData fdbDestinationDirectory = new FormData();
    fdbDestinationDirectory.right = new FormAttachment( 100, 0 );
    fdbDestinationDirectory.top = new FormAttachment( wSourceFileFolder, margin );
    wbDestinationDirectory.setLayoutData( fdbDestinationDirectory );
    wbDestinationDirectory.addListener( SWT.Selection, e-> BaseDialog.presentDirectoryDialog( shell, wDestinationFileFolder, variables ) );

    // Browse Destination file browse button ...
    wbDestinationFileFolder = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbDestinationFileFolder );
    wbDestinationFileFolder.setText( BaseMessages.getString( PKG, "JobMoveFiles.BrowseFiles.Label" ) );
    FormData fdbDestinationFileFolder = new FormData();
    fdbDestinationFileFolder.right = new FormAttachment( wbDestinationDirectory, -margin );
    fdbDestinationFileFolder.top = new FormAttachment( wSourceFileFolder, margin );
    wbDestinationFileFolder.setLayoutData( fdbDestinationFileFolder );

    wDestinationFileFolder = new TextVar( variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wDestinationFileFolder.setToolTipText( BaseMessages.getString(
      PKG, "JobMoveFiles.DestinationFileFolder.Tooltip" ) );
    props.setLook( wDestinationFileFolder );
    wDestinationFileFolder.addModifyListener( lsMod );
    FormData fdDestinationFileFolder = new FormData();
    fdDestinationFileFolder.left = new FormAttachment( middle, 0 );
    fdDestinationFileFolder.top = new FormAttachment( wSourceFileFolder, margin );
    fdDestinationFileFolder.right = new FormAttachment( wbSourceFileFolder, -55 );
    wDestinationFileFolder.setLayoutData(fdDestinationFileFolder);

    wbDestinationFileFolder.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wDestinationFileFolder, variables,
      new String[] { "*" }, FILETYPES, true )
    );

    // Buttons to the right of the screen...
    wbdSourceFileFolder = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbdSourceFileFolder );
    wbdSourceFileFolder.setText( BaseMessages.getString( PKG, "JobMoveFiles.FilenameDelete.Button" ) );
    wbdSourceFileFolder.setToolTipText( BaseMessages.getString( PKG, "JobMoveFiles.FilenameDelete.Tooltip" ) );
    FormData fdbdSourceFileFolder = new FormData();
    fdbdSourceFileFolder.right = new FormAttachment( 100, 0 );
    fdbdSourceFileFolder.top = new FormAttachment( wDestinationFileFolder, 40 );
    wbdSourceFileFolder.setLayoutData(fdbdSourceFileFolder);

    wbeSourceFileFolder = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbeSourceFileFolder );
    wbeSourceFileFolder.setText( BaseMessages.getString( PKG, "JobMoveFiles.FilenameEdit.Button" ) );
    wbeSourceFileFolder.setToolTipText( BaseMessages.getString( PKG, "JobMoveFiles.FilenameEdit.Tooltip" ) );
    FormData fdbeSourceFileFolder = new FormData();
    fdbeSourceFileFolder.right = new FormAttachment( 100, 0 );
    fdbeSourceFileFolder.left = new FormAttachment( wbdSourceFileFolder, 0, SWT.LEFT );
    fdbeSourceFileFolder.top = new FormAttachment( wbdSourceFileFolder, margin );
    wbeSourceFileFolder.setLayoutData(fdbeSourceFileFolder);

    // Wildcard
    wlWildcard = new Label(wGeneralComp, SWT.RIGHT );
    wlWildcard.setText( BaseMessages.getString( PKG, "JobMoveFiles.Wildcard.Label" ) );
    props.setLook( wlWildcard );
    FormData fdlWildcard = new FormData();
    fdlWildcard.left = new FormAttachment( 0, 0 );
    fdlWildcard.top = new FormAttachment( wDestinationFileFolder, margin );
    fdlWildcard.right = new FormAttachment( middle, -margin );
    wlWildcard.setLayoutData(fdlWildcard);

    wWildcard = new TextVar( variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wWildcard.setToolTipText( BaseMessages.getString( PKG, "JobMoveFiles.Wildcard.Tooltip" ) );
    props.setLook( wWildcard );
    wWildcard.addModifyListener( lsMod );
    FormData fdWildcard = new FormData();
    fdWildcard.left = new FormAttachment( middle, 0 );
    fdWildcard.top = new FormAttachment( wDestinationFileFolder, margin );
    fdWildcard.right = new FormAttachment( wbSourceFileFolder, -55 );
    wWildcard.setLayoutData(fdWildcard);

    wlFields = new Label(wGeneralComp, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "JobMoveFiles.Fields.Label" ) );
    props.setLook( wlFields );
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.right = new FormAttachment( middle, -margin );
    fdlFields.top = new FormAttachment( wWildcard, margin );
    wlFields.setLayoutData(fdlFields);

    int rows =
      action.sourceFileFolder == null ? 1 : ( action.sourceFileFolder.length == 0
        ? 0 : action.sourceFileFolder.length );
    final int FieldsRows = rows;

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "JobMoveFiles.Fields.SourceFileFolder.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "JobMoveFiles.Fields.DestinationFileFolder.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "JobMoveFiles.Fields.Wildcard.Label" ), ColumnInfo.COLUMN_TYPE_TEXT,
          false ), };

    colinf[ 0 ].setUsingVariables( true );
    colinf[ 0 ].setToolTip( BaseMessages.getString( PKG, "JobMoveFiles.Fields.SourceFileFolder.Tooltip" ) );
    colinf[ 1 ].setUsingVariables( true );
    colinf[ 1 ].setToolTip( BaseMessages.getString( PKG, "JobMoveFiles.Fields.DestinationFileFolder.Tooltip" ) );
    colinf[ 2 ].setUsingVariables( true );
    colinf[ 2 ].setToolTip( BaseMessages.getString( PKG, "JobMoveFiles.Fields.Wildcard.Tooltip" ) );

    wFields =
      new TableView(
        variables, wGeneralComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( 100, -75 );
    fdFields.bottom = new FormAttachment( 100, -margin );
    wFields.setLayoutData(fdFields);

    RefreshArgFromPrevious();

    // Add the file to the list of files...
    SelectionAdapter selA = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        wFields.add( new String[] {
          wSourceFileFolder.getText(), wDestinationFileFolder.getText(), wWildcard.getText() } );
        wSourceFileFolder.setText( "" );
        wDestinationFileFolder.setText( "" );
        wWildcard.setText( "" );
        wFields.removeEmptyRows();
        wFields.setRowNums();
        wFields.optWidth( true );
      }
    };
    wbaSourceFileFolder.addSelectionListener( selA );
    wSourceFileFolder.addSelectionListener( selA );

    // Delete files from the list of files...
    wbdSourceFileFolder.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        int[] idx = wFields.getSelectionIndices();
        wFields.remove( idx );
        wFields.removeEmptyRows();
        wFields.setRowNums();
      }
    } );

    // Edit the selected file & remove from the list...
    wbeSourceFileFolder.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        int idx = wFields.getSelectionIndex();
        if ( idx >= 0 ) {
          String[] string = wFields.getItem( idx );
          wSourceFileFolder.setText( string[ 0 ] );
          wDestinationFileFolder.setText( string[ 1 ] );
          wWildcard.setText( string[ 2 ] );
          wFields.remove( idx );
        }
        wFields.removeEmptyRows();
        wFields.setRowNums();
      }
    } );

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( 0, 0 );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 100, 0 );
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);
    props.setLook(wGeneralComp);

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////////////////
    // START OF DESTINATION FILE TAB ///
    // ///////////////////////////////////

    CTabItem wDestinationFileTab = new CTabItem(wTabFolder, SWT.NONE);
    wDestinationFileTab.setText( BaseMessages.getString( PKG, "JobMoveFiles.DestinationFileTab.Label" ) );

    FormLayout DestcontentLayout = new FormLayout();
    DestcontentLayout.marginWidth = 3;
    DestcontentLayout.marginHeight = 3;

    Composite wDestinationFileComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wDestinationFileComp);
    wDestinationFileComp.setLayout( DestcontentLayout );

    // DestinationFile grouping?
    // ////////////////////////
    // START OF DestinationFile GROUP
    //

    Group wDestinationFile = new Group(wDestinationFileComp, SWT.SHADOW_NONE);
    props.setLook(wDestinationFile);
    wDestinationFile.setText( BaseMessages.getString( PKG, "JobMoveFiles.GroupDestinationFile.Label" ) );

    FormLayout groupLayoutFile = new FormLayout();
    groupLayoutFile.marginWidth = 10;
    groupLayoutFile.marginHeight = 10;
    wDestinationFile.setLayout( groupLayoutFile );

    // Create destination folder/parent folder
    Label wlCreateDestinationFolder = new Label(wDestinationFile, SWT.RIGHT);
    wlCreateDestinationFolder
      .setText( BaseMessages.getString( PKG, "JobMoveFiles.CreateDestinationFolder.Label" ) );
    props.setLook(wlCreateDestinationFolder);
    FormData fdlCreateDestinationFolder = new FormData();
    fdlCreateDestinationFolder.left = new FormAttachment( 0, 0 );
    fdlCreateDestinationFolder.top = new FormAttachment( 0, margin );
    fdlCreateDestinationFolder.right = new FormAttachment( middle, -margin );
    wlCreateDestinationFolder.setLayoutData(fdlCreateDestinationFolder);
    wCreateDestinationFolder = new Button(wDestinationFile, SWT.CHECK );
    props.setLook( wCreateDestinationFolder );
    wCreateDestinationFolder.setToolTipText( BaseMessages.getString(
      PKG, "JobMoveFiles.CreateDestinationFolder.Tooltip" ) );
    FormData fdCreateDestinationFolder = new FormData();
    fdCreateDestinationFolder.left = new FormAttachment( middle, 0 );
    fdCreateDestinationFolder.top = new FormAttachment( 0, margin );
    fdCreateDestinationFolder.right = new FormAttachment( 100, 0 );
    wCreateDestinationFolder.setLayoutData(fdCreateDestinationFolder);
    wCreateDestinationFolder.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Destination is a file?
    Label wlDestinationIsAFile = new Label(wDestinationFile, SWT.RIGHT);
    wlDestinationIsAFile.setText( BaseMessages.getString( PKG, "JobMoveFiles.DestinationIsAFile.Label" ) );
    props.setLook(wlDestinationIsAFile);
    FormData fdlDestinationIsAFile = new FormData();
    fdlDestinationIsAFile.left = new FormAttachment( 0, 0 );
    fdlDestinationIsAFile.top = new FormAttachment( wCreateDestinationFolder, margin );
    fdlDestinationIsAFile.right = new FormAttachment( middle, -margin );
    wlDestinationIsAFile.setLayoutData(fdlDestinationIsAFile);
    wDestinationIsAFile = new Button(wDestinationFile, SWT.CHECK );
    props.setLook( wDestinationIsAFile );
    wDestinationIsAFile.setToolTipText( BaseMessages.getString( PKG, "JobMoveFiles.DestinationIsAFile.Tooltip" ) );
    FormData fdDestinationIsAFile = new FormData();
    fdDestinationIsAFile.left = new FormAttachment( middle, 0 );
    fdDestinationIsAFile.top = new FormAttachment( wCreateDestinationFolder, margin );
    fdDestinationIsAFile.right = new FormAttachment( 100, 0 );
    wDestinationIsAFile.setLayoutData(fdDestinationIsAFile);
    wDestinationIsAFile.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {

        action.setChanged();
      }
    } );

    // Do not keep folder structure?
    wlDoNotKeepFolderStructure = new Label(wDestinationFile, SWT.RIGHT );
    wlDoNotKeepFolderStructure.setText( BaseMessages
      .getString( PKG, "JobMoveFiles.DoNotKeepFolderStructure.Label" ) );
    props.setLook( wlDoNotKeepFolderStructure );
    FormData fdlDoNotKeepFolderStructure = new FormData();
    fdlDoNotKeepFolderStructure.left = new FormAttachment( 0, 0 );
    fdlDoNotKeepFolderStructure.top = new FormAttachment( wDestinationIsAFile, margin );
    fdlDoNotKeepFolderStructure.right = new FormAttachment( middle, -margin );
    wlDoNotKeepFolderStructure.setLayoutData(fdlDoNotKeepFolderStructure);
    wDoNotKeepFolderStructure = new Button(wDestinationFile, SWT.CHECK );
    props.setLook( wDoNotKeepFolderStructure );
    wDoNotKeepFolderStructure.setToolTipText( BaseMessages.getString(
      PKG, "JobMoveFiles.DoNotKeepFolderStructure.Tooltip" ) );
    FormData fdDoNotKeepFolderStructure = new FormData();
    fdDoNotKeepFolderStructure.left = new FormAttachment( middle, 0 );
    fdDoNotKeepFolderStructure.top = new FormAttachment( wDestinationIsAFile, margin );
    fdDoNotKeepFolderStructure.right = new FormAttachment( 100, 0 );
    wDoNotKeepFolderStructure.setLayoutData(fdDoNotKeepFolderStructure);
    wDoNotKeepFolderStructure.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Create multi-part file?
    wlAddDate = new Label(wDestinationFile, SWT.RIGHT );
    wlAddDate.setText( BaseMessages.getString( PKG, "JobMoveFiles.AddDate.Label" ) );
    props.setLook( wlAddDate );
    FormData fdlAddDate = new FormData();
    fdlAddDate.left = new FormAttachment( 0, 0 );
    fdlAddDate.top = new FormAttachment( wDoNotKeepFolderStructure, margin );
    fdlAddDate.right = new FormAttachment( middle, -margin );
    wlAddDate.setLayoutData(fdlAddDate);
    wAddDate = new Button(wDestinationFile, SWT.CHECK );
    props.setLook( wAddDate );
    wAddDate.setToolTipText( BaseMessages.getString( PKG, "JobMoveFiles.AddDate.Tooltip" ) );
    FormData fdAddDate = new FormData();
    fdAddDate.left = new FormAttachment( middle, 0 );
    fdAddDate.top = new FormAttachment( wlAddDate, 0, SWT.CENTER );
    fdAddDate.right = new FormAttachment( 100, 0 );
    wAddDate.setLayoutData(fdAddDate);
    wAddDate.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        setAddDateBeforeExtension();
      }
    } );
    // Create multi-part file?
    wlAddTime = new Label(wDestinationFile, SWT.RIGHT );
    wlAddTime.setText( BaseMessages.getString( PKG, "JobMoveFiles.AddTime.Label" ) );
    props.setLook( wlAddTime );
    FormData fdlAddTime = new FormData();
    fdlAddTime.left = new FormAttachment( 0, 0 );
    fdlAddTime.top = new FormAttachment( wAddDate, margin );
    fdlAddTime.right = new FormAttachment( middle, -margin );
    wlAddTime.setLayoutData(fdlAddTime);
    wAddTime = new Button(wDestinationFile, SWT.CHECK );
    props.setLook( wAddTime );
    wAddTime.setToolTipText( BaseMessages.getString( PKG, "JobMoveFiles.AddTime.Tooltip" ) );
    FormData fdAddTime = new FormData();
    fdAddTime.left = new FormAttachment( middle, 0 );
    fdAddTime.top = new FormAttachment( wlAddTime, 0, SWT.CENTER );
    fdAddTime.right = new FormAttachment( 100, 0 );
    wAddTime.setLayoutData(fdAddTime);
    wAddTime.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        setAddDateBeforeExtension();
      }
    } );

    // Specify date time format?
    Label wlSpecifyFormat = new Label(wDestinationFile, SWT.RIGHT);
    wlSpecifyFormat.setText( BaseMessages.getString( PKG, "JobMoveFiles.SpecifyFormat.Label" ) );
    props.setLook(wlSpecifyFormat);
    FormData fdlSpecifyFormat = new FormData();
    fdlSpecifyFormat.left = new FormAttachment( 0, 0 );
    fdlSpecifyFormat.top = new FormAttachment( wAddTime, margin );
    fdlSpecifyFormat.right = new FormAttachment( middle, -margin );
    wlSpecifyFormat.setLayoutData(fdlSpecifyFormat);
    wSpecifyFormat = new Button(wDestinationFile, SWT.CHECK );
    props.setLook( wSpecifyFormat );
    wSpecifyFormat.setToolTipText( BaseMessages.getString( PKG, "JobMoveFiles.SpecifyFormat.Tooltip" ) );
    FormData fdSpecifyFormat = new FormData();
    fdSpecifyFormat.left = new FormAttachment( middle, 0 );
    fdSpecifyFormat.top = new FormAttachment( wAddTime, margin );
    fdSpecifyFormat.right = new FormAttachment( 100, 0 );
    wSpecifyFormat.setLayoutData(fdSpecifyFormat);
    wSpecifyFormat.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        setDateTimeFormat();
        setAddDateBeforeExtension();
      }
    } );

    // DateTimeFormat
    wlDateTimeFormat = new Label(wDestinationFile, SWT.RIGHT );
    wlDateTimeFormat.setText( BaseMessages.getString( PKG, "JobMoveFiles.DateTimeFormat.Label" ) );
    props.setLook( wlDateTimeFormat );
    FormData fdlDateTimeFormat = new FormData();
    fdlDateTimeFormat.left = new FormAttachment( 0, 0 );
    fdlDateTimeFormat.top = new FormAttachment( wSpecifyFormat, margin );
    fdlDateTimeFormat.right = new FormAttachment( middle, -margin );
    wlDateTimeFormat.setLayoutData(fdlDateTimeFormat);
    wDateTimeFormat = new CCombo(wDestinationFile, SWT.BORDER | SWT.READ_ONLY );
    wDateTimeFormat.setEditable( true );
    props.setLook( wDateTimeFormat );
    wDateTimeFormat.addModifyListener( lsMod );
    FormData fdDateTimeFormat = new FormData();
    fdDateTimeFormat.left = new FormAttachment( middle, 0 );
    fdDateTimeFormat.top = new FormAttachment( wSpecifyFormat, margin );
    fdDateTimeFormat.right = new FormAttachment( 100, 0 );
    wDateTimeFormat.setLayoutData(fdDateTimeFormat);
    // Prepare a list of possible DateTimeFormats...
    String[] dats = Const.getDateFormats();
    for (String s : dats) {
      wDateTimeFormat.add(s);
    }

    // Add Date before extension?
    wlAddDateBeforeExtension = new Label(wDestinationFile, SWT.RIGHT );
    wlAddDateBeforeExtension.setText( BaseMessages.getString( PKG, "JobMoveFiles.AddDateBeforeExtension.Label" ) );
    props.setLook( wlAddDateBeforeExtension );
    FormData fdlAddDateBeforeExtension = new FormData();
    fdlAddDateBeforeExtension.left = new FormAttachment( 0, 0 );
    fdlAddDateBeforeExtension.top = new FormAttachment( wDateTimeFormat, margin );
    fdlAddDateBeforeExtension.right = new FormAttachment( middle, -margin );
    wlAddDateBeforeExtension.setLayoutData(fdlAddDateBeforeExtension);
    wAddDateBeforeExtension = new Button(wDestinationFile, SWT.CHECK );
    props.setLook( wAddDateBeforeExtension );
    wAddDateBeforeExtension.setToolTipText( BaseMessages.getString(
      PKG, "JobMoveFiles.AddDateBeforeExtension.Tooltip" ) );
    FormData fdAddDateBeforeExtension = new FormData();
    fdAddDateBeforeExtension.left = new FormAttachment( middle, 0 );
    fdAddDateBeforeExtension.top = new FormAttachment( wDateTimeFormat, margin );
    fdAddDateBeforeExtension.right = new FormAttachment( 100, 0 );
    wAddDateBeforeExtension.setLayoutData(fdAddDateBeforeExtension);
    wAddDateBeforeExtension.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // If File Exists
    Label wlIfFileExists = new Label(wDestinationFile, SWT.RIGHT);
    wlIfFileExists.setText( BaseMessages.getString( PKG, "JobMoveFiles.IfFileExists.Label" ) );
    props.setLook(wlIfFileExists);
    FormData fdlIfFileExists = new FormData();
    fdlIfFileExists.left = new FormAttachment( 0, 0 );
    fdlIfFileExists.right = new FormAttachment( middle, 0 );
    fdlIfFileExists.top = new FormAttachment( wAddDateBeforeExtension, margin );
    wlIfFileExists.setLayoutData(fdlIfFileExists);
    wIfFileExists = new CCombo(wDestinationFile, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wIfFileExists.add( BaseMessages.getString( PKG, "JobMoveFiles.Do_Nothing_IfFileExists.Label" ) );
    wIfFileExists.add( BaseMessages.getString( PKG, "JobMoveFiles.Overwrite_File_IfFileExists.Label" ) );
    wIfFileExists.add( BaseMessages.getString( PKG, "JobMoveFiles.Unique_Name_IfFileExists.Label" ) );
    wIfFileExists.add( BaseMessages.getString( PKG, "JobMoveFiles.Delete_Source_File_IfFileExists.Label" ) );
    wIfFileExists.add( BaseMessages.getString( PKG, "JobMoveFiles.Move_To_Folder_IfFileExists.Label" ) );
    wIfFileExists.add( BaseMessages.getString( PKG, "JobMoveFiles.Fail_IfFileExists.Label" ) );
    wIfFileExists.select( 0 ); // +1: starts at -1

    props.setLook( wIfFileExists );
    FormData fdIfFileExists = new FormData();
    fdIfFileExists.left = new FormAttachment( middle, 0 );
    fdIfFileExists.top = new FormAttachment( wAddDateBeforeExtension, margin );
    fdIfFileExists.right = new FormAttachment( 100, 0 );
    wIfFileExists.setLayoutData(fdIfFileExists);

    fdIfFileExists = new FormData();
    fdIfFileExists.left = new FormAttachment( middle, 0 );
    fdIfFileExists.top = new FormAttachment( wAddDateBeforeExtension, margin );
    fdIfFileExists.right = new FormAttachment( 100, 0 );
    wIfFileExists.setLayoutData(fdIfFileExists);

    wIfFileExists.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {

        activeDestinationFolder();
        setMovedDateTimeFormat();
        // setAddDateBeforeExtension();
        setAddMovedDateBeforeExtension();

      }
    } );

    FormData fdDestinationFile = new FormData();
    fdDestinationFile.left = new FormAttachment( 0, margin );
    fdDestinationFile.top = new FormAttachment( wName, margin );
    fdDestinationFile.right = new FormAttachment( 100, -margin );
    wDestinationFile.setLayoutData(fdDestinationFile);

    // ///////////////////////////////////////////////////////////
    // / END OF DestinationFile GROUP
    // ///////////////////////////////////////////////////////////

    // MoveTo grouping?
    // ////////////////////////
    // START OF MoveTo GROUP
    //

    Group wMoveToGroup = new Group(wDestinationFileComp, SWT.SHADOW_NONE);
    props.setLook(wMoveToGroup);
    wMoveToGroup.setText( BaseMessages.getString( PKG, "JobMoveFiles.GroupMoveToGroup.Label" ) );

    FormLayout MovetoLayoutFile = new FormLayout();
    MovetoLayoutFile.marginWidth = 10;
    MovetoLayoutFile.marginHeight = 10;
    wMoveToGroup.setLayout( MovetoLayoutFile );

    // DestinationFolder line
    wlDestinationFolder = new Label(wMoveToGroup, SWT.RIGHT );
    wlDestinationFolder.setText( BaseMessages.getString( PKG, "JobMoveFiles.DestinationFolder.Label" ) );
    props.setLook( wlDestinationFolder );
    FormData fdlDestinationFolder = new FormData();
    fdlDestinationFolder.left = new FormAttachment( 0, 0 );
    fdlDestinationFolder.top = new FormAttachment(wDestinationFile, margin );
    fdlDestinationFolder.right = new FormAttachment( middle, -margin );
    wlDestinationFolder.setLayoutData(fdlDestinationFolder);

    wbDestinationFolder = new Button(wMoveToGroup, SWT.PUSH | SWT.CENTER );
    props.setLook( wbDestinationFolder );
    wbDestinationFolder.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbDestinationFolder = new FormData();
    fdbDestinationFolder.right = new FormAttachment( 100, 0 );
    fdbDestinationFolder.top = new FormAttachment(wDestinationFile, 0 );
    wbDestinationFolder.setLayoutData(fdbDestinationFolder);

    wDestinationFolder = new TextVar( variables, wMoveToGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDestinationFolder );
    wDestinationFolder.addModifyListener( lsMod );
    FormData fdDestinationFolder = new FormData();
    fdDestinationFolder.left = new FormAttachment( middle, 0 );
    fdDestinationFolder.top = new FormAttachment(wDestinationFile, margin );
    fdDestinationFolder.right = new FormAttachment( wbDestinationFolder, -margin );
    wDestinationFolder.setLayoutData(fdDestinationFolder);

    // Whenever something changes, set the tooltip to the expanded version:
    wDestinationFolder.addModifyListener( e -> wDestinationFolder.setToolTipText( variables.resolve( wDestinationFolder.getText() ) ) );
    wbDestinationFolder.addListener( SWT.Selection, e-> BaseDialog.presentDirectoryDialog( shell, wDestinationFolder, variables ) );

    // Create destination folder/parent folder
    wlCreateMoveToFolder = new Label(wMoveToGroup, SWT.RIGHT );
    wlCreateMoveToFolder.setText( BaseMessages.getString( PKG, "JobMoveFiles.CreateMoveToFolder.Label" ) );
    props.setLook( wlCreateMoveToFolder );
    FormData fdlCreateMoveToFolder = new FormData();
    fdlCreateMoveToFolder.left = new FormAttachment( 0, 0 );
    fdlCreateMoveToFolder.top = new FormAttachment( wDestinationFolder, margin );
    fdlCreateMoveToFolder.right = new FormAttachment( middle, -margin );
    wlCreateMoveToFolder.setLayoutData(fdlCreateMoveToFolder);
    wCreateMoveToFolder = new Button(wMoveToGroup, SWT.CHECK );
    props.setLook( wCreateMoveToFolder );
    wCreateMoveToFolder.setToolTipText( BaseMessages.getString( PKG, "JobMoveFiles.CreateMoveToFolder.Tooltip" ) );
    FormData fdCreateMoveToFolder = new FormData();
    fdCreateMoveToFolder.left = new FormAttachment( middle, 0 );
    fdCreateMoveToFolder.top = new FormAttachment( wDestinationFolder, margin );
    fdCreateMoveToFolder.right = new FormAttachment( 100, 0 );
    wCreateMoveToFolder.setLayoutData(fdCreateMoveToFolder);
    wCreateMoveToFolder.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Create multi-part file?
    wlAddMovedDate = new Label(wMoveToGroup, SWT.RIGHT );
    wlAddMovedDate.setText( BaseMessages.getString( PKG, "JobMoveFiles.AddMovedDate.Label" ) );
    props.setLook( wlAddMovedDate );
    FormData fdlAddMovedDate = new FormData();
    fdlAddMovedDate.left = new FormAttachment( 0, 0 );
    fdlAddMovedDate.top = new FormAttachment( wCreateMoveToFolder, margin );
    fdlAddMovedDate.right = new FormAttachment( middle, -margin );
    wlAddMovedDate.setLayoutData(fdlAddMovedDate);
    wAddMovedDate = new Button(wMoveToGroup, SWT.CHECK );
    props.setLook( wAddMovedDate );
    wAddMovedDate.setToolTipText( BaseMessages.getString( PKG, "JobMoveFiles.AddMovedDate.Tooltip" ) );
    FormData fdAddMovedDate = new FormData();
    fdAddMovedDate.left = new FormAttachment( middle, 0 );
    fdAddMovedDate.top = new FormAttachment( wCreateMoveToFolder, margin );
    fdAddMovedDate.right = new FormAttachment( 100, 0 );
    wAddMovedDate.setLayoutData(fdAddMovedDate);
    wAddMovedDate.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        setAddMovedDateBeforeExtension();
      }
    } );
    // Create multi-part file?
    wlAddMovedTime = new Label(wMoveToGroup, SWT.RIGHT );
    wlAddMovedTime.setText( BaseMessages.getString( PKG, "JobMoveFiles.AddMovedTime.Label" ) );
    props.setLook( wlAddMovedTime );
    FormData fdlAddMovedTime = new FormData();
    fdlAddMovedTime.left = new FormAttachment( 0, 0 );
    fdlAddMovedTime.top = new FormAttachment( wAddMovedDate, margin );
    fdlAddMovedTime.right = new FormAttachment( middle, -margin );
    wlAddMovedTime.setLayoutData(fdlAddMovedTime);
    wAddMovedTime = new Button(wMoveToGroup, SWT.CHECK );
    props.setLook( wAddMovedTime );
    wAddMovedTime.setToolTipText( BaseMessages.getString( PKG, "JobMoveFiles.AddMovedTime.Tooltip" ) );
    FormData fdAddMovedTime = new FormData();
    fdAddMovedTime.left = new FormAttachment( middle, 0 );
    fdAddMovedTime.top = new FormAttachment( wAddMovedDate, margin );
    fdAddMovedTime.right = new FormAttachment( 100, 0 );
    wAddMovedTime.setLayoutData(fdAddMovedTime);
    wAddMovedTime.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        setAddMovedDateBeforeExtension();
      }
    } );

    // Specify date time format?
    wlSpecifyMoveFormat = new Label(wMoveToGroup, SWT.RIGHT );
    wlSpecifyMoveFormat.setText( BaseMessages.getString( PKG, "JobMoveFiles.SpecifyMoveFormat.Label" ) );
    props.setLook( wlSpecifyMoveFormat );
    FormData fdlSpecifyMoveFormat = new FormData();
    fdlSpecifyMoveFormat.left = new FormAttachment( 0, 0 );
    fdlSpecifyMoveFormat.top = new FormAttachment( wAddMovedTime, margin );
    fdlSpecifyMoveFormat.right = new FormAttachment( middle, -margin );
    wlSpecifyMoveFormat.setLayoutData(fdlSpecifyMoveFormat);
    wSpecifyMoveFormat = new Button(wMoveToGroup, SWT.CHECK );
    props.setLook( wSpecifyMoveFormat );
    wSpecifyMoveFormat.setToolTipText( BaseMessages.getString( PKG, "JobMoveFiles.SpecifyMoveFormat.Tooltip" ) );
    FormData fdSpecifyMoveFormat = new FormData();
    fdSpecifyMoveFormat.left = new FormAttachment( middle, 0 );
    fdSpecifyMoveFormat.top = new FormAttachment( wAddMovedTime, margin );
    fdSpecifyMoveFormat.right = new FormAttachment( 100, 0 );
    wSpecifyMoveFormat.setLayoutData(fdSpecifyMoveFormat);
    wSpecifyMoveFormat.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        setMovedDateTimeFormat();
        setAddMovedDateBeforeExtension();
      }
    } );

    // Moved DateTimeFormat
    wlMovedDateTimeFormat = new Label(wMoveToGroup, SWT.RIGHT );
    wlMovedDateTimeFormat.setText( BaseMessages.getString( PKG, "JobMoveFiles.MovedDateTimeFormat.Label" ) );
    props.setLook( wlMovedDateTimeFormat );
    FormData fdlMovedDateTimeFormat = new FormData();
    fdlMovedDateTimeFormat.left = new FormAttachment( 0, 0 );
    fdlMovedDateTimeFormat.top = new FormAttachment( wSpecifyMoveFormat, margin );
    fdlMovedDateTimeFormat.right = new FormAttachment( middle, -margin );
    wlMovedDateTimeFormat.setLayoutData(fdlMovedDateTimeFormat);
    wMovedDateTimeFormat = new CCombo(wMoveToGroup, SWT.BORDER | SWT.READ_ONLY );
    wMovedDateTimeFormat.setEditable( true );
    props.setLook( wMovedDateTimeFormat );
    wMovedDateTimeFormat.addModifyListener( lsMod );
    FormData fdMovedDateTimeFormat = new FormData();
    fdMovedDateTimeFormat.left = new FormAttachment( middle, 0 );
    fdMovedDateTimeFormat.top = new FormAttachment( wSpecifyMoveFormat, margin );
    fdMovedDateTimeFormat.right = new FormAttachment( 100, 0 );
    wMovedDateTimeFormat.setLayoutData(fdMovedDateTimeFormat);

    for (String dat : dats) {
      wMovedDateTimeFormat.add(dat);
    }

    // Add Date before extension?
    wlAddMovedDateBeforeExtension = new Label(wMoveToGroup, SWT.RIGHT );
    wlAddMovedDateBeforeExtension.setText( BaseMessages.getString(
      PKG, "JobMoveFiles.AddMovedDateBeforeExtension.Label" ) );
    props.setLook( wlAddMovedDateBeforeExtension );
    FormData fdlAddMovedDateBeforeExtension = new FormData();
    fdlAddMovedDateBeforeExtension.left = new FormAttachment( 0, 0 );
    fdlAddMovedDateBeforeExtension.top = new FormAttachment( wMovedDateTimeFormat, margin );
    fdlAddMovedDateBeforeExtension.right = new FormAttachment( middle, -margin );
    wlAddMovedDateBeforeExtension.setLayoutData(fdlAddMovedDateBeforeExtension);
    wAddMovedDateBeforeExtension = new Button(wMoveToGroup, SWT.CHECK );
    props.setLook( wAddMovedDateBeforeExtension );
    wAddMovedDateBeforeExtension.setToolTipText( BaseMessages.getString(
      PKG, "JobMoveFiles.AddMovedDateBeforeExtension.Tooltip" ) );
    FormData fdAddMovedDateBeforeExtension = new FormData();
    fdAddMovedDateBeforeExtension.left = new FormAttachment( middle, 0 );
    fdAddMovedDateBeforeExtension.top = new FormAttachment( wMovedDateTimeFormat, margin );
    fdAddMovedDateBeforeExtension.right = new FormAttachment( 100, 0 );
    wAddMovedDateBeforeExtension.setLayoutData(fdAddMovedDateBeforeExtension);
    wAddMovedDateBeforeExtension.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // If moved File Exists
    wlIfMovedFileExists = new Label(wMoveToGroup, SWT.RIGHT );
    wlIfMovedFileExists.setText( BaseMessages.getString( PKG, "JobMoveFiles.IfMovedFileExists.Label" ) );
    props.setLook( wlIfMovedFileExists );
    FormData fdlIfMovedFileExists = new FormData();
    fdlIfMovedFileExists.left = new FormAttachment( 0, 0 );
    fdlIfMovedFileExists.right = new FormAttachment( middle, 0 );
    fdlIfMovedFileExists.top = new FormAttachment( wAddMovedDateBeforeExtension, margin );
    wlIfMovedFileExists.setLayoutData(fdlIfMovedFileExists);
    wIfMovedFileExists = new CCombo(wMoveToGroup, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wIfMovedFileExists.add( BaseMessages.getString( PKG, "JobMoveFiles.Do_Nothing_IfMovedFileExists.Label" ) );
    wIfMovedFileExists.add( BaseMessages.getString(
      PKG, "JobMoveFiles.Overwrite_Filename_IffMovedFileExists.Label" ) );
    wIfMovedFileExists.add( BaseMessages.getString( PKG, "JobMoveFiles.UniqueName_IfMovedFileExists.Label" ) );
    wIfMovedFileExists.add( BaseMessages.getString( PKG, "JobMoveFiles.Fail_IfMovedFileExists.Label" ) );
    wIfMovedFileExists.select( 0 ); // +1: starts at -1

    props.setLook( wIfMovedFileExists );
    FormData fdIfMovedFileExists = new FormData();
    fdIfMovedFileExists.left = new FormAttachment( middle, 0 );
    fdIfMovedFileExists.top = new FormAttachment( wAddMovedDateBeforeExtension, margin );
    fdIfMovedFileExists.right = new FormAttachment( 100, 0 );
    wIfMovedFileExists.setLayoutData(fdIfMovedFileExists);

    fdIfMovedFileExists = new FormData();
    fdIfMovedFileExists.left = new FormAttachment( middle, 0 );
    fdIfMovedFileExists.top = new FormAttachment( wAddMovedDateBeforeExtension, margin );
    fdIfMovedFileExists.right = new FormAttachment( 100, 0 );
    wIfMovedFileExists.setLayoutData(fdIfMovedFileExists);

    FormData fdMoveToGroup = new FormData();
    fdMoveToGroup.left = new FormAttachment( 0, margin );
    fdMoveToGroup.top = new FormAttachment(wDestinationFile, margin );
    fdMoveToGroup.right = new FormAttachment( 100, -margin );
    wMoveToGroup.setLayoutData(fdMoveToGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF MoveToGroup GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdDestinationFileComp = new FormData();
    fdDestinationFileComp.left = new FormAttachment( 0, 0 );
    fdDestinationFileComp.top = new FormAttachment( 0, 0 );
    fdDestinationFileComp.right = new FormAttachment( 100, 0 );
    fdDestinationFileComp.bottom = new FormAttachment( 100, 0 );
    wDestinationFileComp.setLayoutData(wDestinationFileComp);

    wDestinationFileComp.layout();
    wDestinationFileTab.setControl(wDestinationFileComp);

    // ///////////////////////////////////////////////////////////
    // / END OF DESTINATION FILETAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////////////////
    // START OF ADVANCED TAB ///
    // ///////////////////////////////////

    CTabItem wAdvancedTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdvancedTab.setText( BaseMessages.getString( PKG, "JobMoveFiles.Tab.Advanced.Label" ) );

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    Composite wAdvancedComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wAdvancedComp);
    wAdvancedComp.setLayout( contentLayout );

    // SuccessOngrouping?
    // ////////////////////////
    // START OF SUCCESS ON GROUP///
    // /
    Group wSuccessOn = new Group(wAdvancedComp, SWT.SHADOW_NONE);
    props.setLook(wSuccessOn);
    wSuccessOn.setText( BaseMessages.getString( PKG, "JobMoveFiles.SuccessOn.Group.Label" ) );

    FormLayout successongroupLayout = new FormLayout();
    successongroupLayout.marginWidth = 10;
    successongroupLayout.marginHeight = 10;

    wSuccessOn.setLayout( successongroupLayout );

    // Success Condition
    Label wlSuccessCondition = new Label(wSuccessOn, SWT.RIGHT);
    wlSuccessCondition.setText( BaseMessages.getString( PKG, "JobMoveFiles.SuccessCondition.Label" ) );
    props.setLook(wlSuccessCondition);
    FormData fdlSuccessCondition = new FormData();
    fdlSuccessCondition.left = new FormAttachment( 0, 0 );
    fdlSuccessCondition.right = new FormAttachment( middle, 0 );
    fdlSuccessCondition.top = new FormAttachment( 0, margin );
    wlSuccessCondition.setLayoutData(fdlSuccessCondition);
    wSuccessCondition = new CCombo(wSuccessOn, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wSuccessCondition.add( BaseMessages.getString( PKG, "JobMoveFiles.SuccessWhenAllWorksFine.Label" ) );
    wSuccessCondition.add( BaseMessages.getString( PKG, "JobMoveFiles.SuccessWhenAtLeat.Label" ) );
    wSuccessCondition.add( BaseMessages.getString( PKG, "JobMoveFiles.SuccessWhenErrorsLessThan.Label" ) );

    wSuccessCondition.select( 0 ); // +1: starts at -1

    props.setLook( wSuccessCondition );
    FormData fdSuccessCondition = new FormData();
    fdSuccessCondition.left = new FormAttachment( middle, 0 );
    fdSuccessCondition.top = new FormAttachment( 0, margin );
    fdSuccessCondition.right = new FormAttachment( 100, 0 );
    wSuccessCondition.setLayoutData(fdSuccessCondition);
    wSuccessCondition.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        activeSuccessCondition();

      }
    } );

    // Success when number of errors less than
    wlNrErrorsLessThan = new Label(wSuccessOn, SWT.RIGHT );
    wlNrErrorsLessThan.setText( BaseMessages.getString( PKG, "JobMoveFiles.NrErrorsLessThan.Label" ) );
    props.setLook( wlNrErrorsLessThan );
    FormData fdlNrErrorsLessThan = new FormData();
    fdlNrErrorsLessThan.left = new FormAttachment( 0, 0 );
    fdlNrErrorsLessThan.top = new FormAttachment( wSuccessCondition, margin );
    fdlNrErrorsLessThan.right = new FormAttachment( middle, -margin );
    wlNrErrorsLessThan.setLayoutData(fdlNrErrorsLessThan);

    wNrErrorsLessThan =
      new TextVar( variables, wSuccessOn, SWT.SINGLE | SWT.LEFT | SWT.BORDER, BaseMessages.getString(
        PKG, "JobMoveFiles.NrErrorsLessThan.Tooltip" ) );
    props.setLook( wNrErrorsLessThan );
    wNrErrorsLessThan.addModifyListener( lsMod );
    FormData fdNrErrorsLessThan = new FormData();
    fdNrErrorsLessThan.left = new FormAttachment( middle, 0 );
    fdNrErrorsLessThan.top = new FormAttachment( wSuccessCondition, margin );
    fdNrErrorsLessThan.right = new FormAttachment( 100, -margin );
    wNrErrorsLessThan.setLayoutData(fdNrErrorsLessThan);

    FormData fdSuccessOn = new FormData();
    fdSuccessOn.left = new FormAttachment( 0, margin );
    fdSuccessOn.top = new FormAttachment(wDestinationFile, margin );
    fdSuccessOn.right = new FormAttachment( 100, -margin );
    wSuccessOn.setLayoutData(fdSuccessOn);
    // ///////////////////////////////////////////////////////////
    // / END OF Success ON GROUP
    // ///////////////////////////////////////////////////////////

    // fileresult grouping?
    // ////////////////////////
    // START OF LOGGING GROUP///
    // /
    Group wFileResult = new Group(wAdvancedComp, SWT.SHADOW_NONE);
    props.setLook(wFileResult);
    wFileResult.setText( BaseMessages.getString( PKG, "JobMoveFiles.FileResult.Group.Label" ) );

    FormLayout fileresultgroupLayout = new FormLayout();
    fileresultgroupLayout.marginWidth = 10;
    fileresultgroupLayout.marginHeight = 10;

    wFileResult.setLayout( fileresultgroupLayout );

    // Add file to result
    Label wlAddFileToResult = new Label(wFileResult, SWT.RIGHT);
    wlAddFileToResult.setText( BaseMessages.getString( PKG, "JobMoveFiles.AddFileToResult.Label" ) );
    props.setLook(wlAddFileToResult);
    FormData fdlAddFileToResult = new FormData();
    fdlAddFileToResult.left = new FormAttachment( 0, 0 );
    fdlAddFileToResult.top = new FormAttachment(wSuccessOn, margin );
    fdlAddFileToResult.right = new FormAttachment( middle, -margin );
    wlAddFileToResult.setLayoutData(fdlAddFileToResult);
    wAddFileToResult = new Button(wFileResult, SWT.CHECK );
    props.setLook( wAddFileToResult );
    wAddFileToResult.setToolTipText( BaseMessages.getString( PKG, "JobMoveFiles.AddFileToResult.Tooltip" ) );
    FormData fdAddFileToResult = new FormData();
    fdAddFileToResult.left = new FormAttachment( middle, 0 );
    fdAddFileToResult.top = new FormAttachment(wSuccessOn, margin );
    fdAddFileToResult.right = new FormAttachment( 100, 0 );
    wAddFileToResult.setLayoutData(fdAddFileToResult);
    wAddFileToResult.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    FormData fdFileResult = new FormData();
    fdFileResult.left = new FormAttachment( 0, margin );
    fdFileResult.top = new FormAttachment(wSuccessOn, margin );
    fdFileResult.right = new FormAttachment( 100, -margin );
    wFileResult.setLayoutData(fdFileResult);
    // ///////////////////////////////////////////////////////////
    // / END OF FilesResult GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdAdvancedComp = new FormData();
    fdAdvancedComp.left = new FormAttachment( 0, 0 );
    fdAdvancedComp.top = new FormAttachment( 0, 0 );
    fdAdvancedComp.right = new FormAttachment( 100, 0 );
    fdAdvancedComp.bottom = new FormAttachment( 100, 0 );
    wAdvancedComp.setLayoutData(wAdvancedComp);

    wAdvancedComp.layout();
    wAdvancedTab.setControl(wAdvancedComp);

    // ///////////////////////////////////////////////////////////
    // / END OF ADVANCED TAB
    // ///////////////////////////////////////////////////////////

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
    wSourceFileFolder.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    CheckIncludeSubFolders();
    activeSuccessCondition();
    setDateTimeFormat();
    activeSuccessCondition();

    activeDestinationFolder();
    setMovedDateTimeFormat();
    setAddDateBeforeExtension();
    setAddMovedDateBeforeExtension();
    wTabFolder.setSelection( 0 );
    BaseTransformDialog.setSize( shell );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }

  private void activeDestinationFolder() {

    wbDestinationFolder.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wlDestinationFolder.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wDestinationFolder.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wlMovedDateTimeFormat.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wMovedDateTimeFormat.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wIfMovedFileExists.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wlIfMovedFileExists.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wlAddMovedDateBeforeExtension.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wAddMovedDateBeforeExtension.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wlAddMovedDate.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wAddMovedDate.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wlAddMovedTime.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wAddMovedTime.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wlSpecifyMoveFormat.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wSpecifyMoveFormat.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wlCreateMoveToFolder.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wCreateMoveToFolder.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
  }

  private void activeSuccessCondition() {
    wlNrErrorsLessThan.setEnabled( wSuccessCondition.getSelectionIndex() != 0 );
    wNrErrorsLessThan.setEnabled( wSuccessCondition.getSelectionIndex() != 0 );
  }

  private void setAddDateBeforeExtension() {
    wlAddDateBeforeExtension.setEnabled( wAddDate.getSelection()
      || wAddTime.getSelection() || wSpecifyFormat.getSelection() );
    wAddDateBeforeExtension.setEnabled( wAddDate.getSelection()
      || wAddTime.getSelection() || wSpecifyFormat.getSelection() );
    if ( !wAddDate.getSelection() && !wAddTime.getSelection() && !wSpecifyFormat.getSelection() ) {
      wAddDateBeforeExtension.setSelection( false );
    }
  }

  private void setAddMovedDateBeforeExtension() {
    wlAddMovedDateBeforeExtension.setEnabled( wAddMovedDate.getSelection()
      || wAddMovedTime.getSelection() || wSpecifyMoveFormat.getSelection() );
    wAddMovedDateBeforeExtension.setEnabled( wAddMovedDate.getSelection()
      || wAddMovedTime.getSelection() || wSpecifyMoveFormat.getSelection() );
    if ( !wAddMovedDate.getSelection() && !wAddMovedTime.getSelection() && !wSpecifyMoveFormat.getSelection() ) {
      wAddMovedDateBeforeExtension.setSelection( false );
    }
  }

  private void setDateTimeFormat() {
    if ( wSpecifyFormat.getSelection() ) {
      wAddDate.setSelection( false );
      wAddTime.setSelection( false );
    }

    wDateTimeFormat.setEnabled( wSpecifyFormat.getSelection() );
    wlDateTimeFormat.setEnabled( wSpecifyFormat.getSelection() );
    wAddDate.setEnabled( !wSpecifyFormat.getSelection() );
    wlAddDate.setEnabled( !wSpecifyFormat.getSelection() );
    wAddTime.setEnabled( !wSpecifyFormat.getSelection() );
    wlAddTime.setEnabled( !wSpecifyFormat.getSelection() );

  }

  private void setMovedDateTimeFormat() {
    if ( wSpecifyMoveFormat.getSelection() ) {
      wAddMovedDate.setSelection( false );
      wAddMovedTime.setSelection( false );
    }

    wlMovedDateTimeFormat.setEnabled( wSpecifyMoveFormat.getSelection() );
    wMovedDateTimeFormat.setEnabled( wSpecifyMoveFormat.getSelection() );
    // wAddMovedDate.setEnabled(!wSpecifyMoveFormat.getSelection());
    // wlAddMovedDate.setEnabled(!wSpecifyMoveFormat.getSelection());
    // wAddMovedTime.setEnabled(!wSpecifyMoveFormat.getSelection());
    // wlAddMovedTime.setEnabled(!wSpecifyMoveFormat.getSelection());

  }

  private void RefreshArgFromPrevious() {

    wlFields.setEnabled( !wPrevious.getSelection() );
    wFields.setEnabled( !wPrevious.getSelection() );
    wbdSourceFileFolder.setEnabled( !wPrevious.getSelection() );
    wbeSourceFileFolder.setEnabled( !wPrevious.getSelection() );
    wbSourceFileFolder.setEnabled( !wPrevious.getSelection() );
    wbaSourceFileFolder.setEnabled( !wPrevious.getSelection() );
    wbDestinationFileFolder.setEnabled( !wPrevious.getSelection() );
    wlDestinationFileFolder.setEnabled( !wPrevious.getSelection() );
    wDestinationFileFolder.setEnabled( !wPrevious.getSelection() );
    wlSourceFileFolder.setEnabled( !wPrevious.getSelection() );
    wSourceFileFolder.setEnabled( !wPrevious.getSelection() );

    wlWildcard.setEnabled( !wPrevious.getSelection() );
    wWildcard.setEnabled( !wPrevious.getSelection() );
    wbSourceDirectory.setEnabled( !wPrevious.getSelection() );
    wbDestinationDirectory.setEnabled( !wPrevious.getSelection() );
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  private void CheckIncludeSubFolders() {
    wlMoveEmptyFolders.setEnabled( wIncludeSubfolders.getSelection() );
    wMoveEmptyFolders.setEnabled( wIncludeSubfolders.getSelection() );
    wlDoNotKeepFolderStructure.setEnabled( wIncludeSubfolders.getSelection() );
    wDoNotKeepFolderStructure.setEnabled( wIncludeSubfolders.getSelection() );
    if ( !wIncludeSubfolders.getSelection() ) {
      wDoNotKeepFolderStructure.setSelection( false );
      wMoveEmptyFolders.setSelection( false );
    }
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wName.setText( Const.NVL( action.getName(), "" ) );
    wMoveEmptyFolders.setSelection( action.moveEmptyFolders );

    if ( action.sourceFileFolder != null ) {
      for ( int i = 0; i < action.sourceFileFolder.length; i++ ) {
        TableItem ti = wFields.table.getItem( i );
        if ( action.sourceFileFolder[ i ] != null ) {
          ti.setText( 1, action.sourceFileFolder[ i ] );
        }
        if ( action.destinationFileFolder[ i ] != null ) {
          ti.setText( 2, action.destinationFileFolder[ i ] );
        }
        if ( action.wildcard[ i ] != null ) {
          ti.setText( 3, action.wildcard[ i ] );
        }
      }
      wFields.setRowNums();
      wFields.optWidth( true );
    }
    wPrevious.setSelection( action.argFromPrevious );
    wIncludeSubfolders.setSelection( action.includeSubfolders );
    wDestinationIsAFile.setSelection( action.destinationIsAFile );
    wCreateDestinationFolder.setSelection( action.createDestinationFolder );

    wAddFileToResult.setSelection( action.addResultFilenames );

    wCreateMoveToFolder.setSelection( action.createMoveToFolder );

    if ( action.getNrErrorsLessThan() != null ) {
      wNrErrorsLessThan.setText( action.getNrErrorsLessThan() );
    } else {
      wNrErrorsLessThan.setText( "10" );
    }

    if ( action.getSuccessCondition() != null ) {
      if ( action.getSuccessCondition().equals( action.SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED ) ) {
        wSuccessCondition.select( 1 );
      } else if ( action.getSuccessCondition().equals( action.SUCCESS_IF_ERRORS_LESS ) ) {
        wSuccessCondition.select( 2 );
      } else {
        wSuccessCondition.select( 0 );
      }
    } else {
      wSuccessCondition.select( 0 );
    }

    if ( action.getIfFileExists() != null ) {
      if ( action.getIfFileExists().equals( "overwrite_file" ) ) {
        wIfFileExists.select( 1 );
      } else if ( action.getIfFileExists().equals( "unique_name" ) ) {
        wIfFileExists.select( 2 );
      } else if ( action.getIfFileExists().equals( "delete_file" ) ) {
        wIfFileExists.select( 3 );
      } else if ( action.getIfFileExists().equals( "move_file" ) ) {
        wIfFileExists.select( 4 );
      } else if ( action.getIfFileExists().equals( "fail" ) ) {
        wIfFileExists.select( 5 );
      } else {
        wIfFileExists.select( 0 );
      }

    } else {
      wIfFileExists.select( 0 );
    }

    if ( action.getDestinationFolder() != null ) {
      wDestinationFolder.setText( action.getDestinationFolder() );
    }

    if ( action.getIfMovedFileExists() != null ) {
      if ( action.getIfMovedFileExists().equals( "overwrite_file" ) ) {
        wIfMovedFileExists.select( 1 );
      } else if ( action.getIfMovedFileExists().equals( "unique_name" ) ) {
        wIfMovedFileExists.select( 2 );
      } else if ( action.getIfMovedFileExists().equals( "fail" ) ) {
        wIfMovedFileExists.select( 3 );
      } else {
        wIfMovedFileExists.select( 0 );
      }

    } else {
      wIfMovedFileExists.select( 0 );
    }
    wDoNotKeepFolderStructure.setSelection( action.isDoNotKeepFolderStructure() );
    wAddDateBeforeExtension.setSelection( action.isAddDateBeforeExtension() );
    wSimulate.setSelection( action.simulate );

    wAddDate.setSelection( action.isAddDate() );
    wAddTime.setSelection( action.isAddTime() );
    wSpecifyFormat.setSelection( action.isSpecifyFormat() );
    if ( action.getDateTimeFormat() != null ) {
      wDateTimeFormat.setText( action.getDateTimeFormat() );
    }

    wAddMovedDate.setSelection( action.isAddMovedDate() );
    wAddMovedTime.setSelection( action.isAddMovedTime() );
    wSpecifyMoveFormat.setSelection( action.isSpecifyMoveFormat() );
    if ( action.getMovedDateTimeFormat() != null ) {
      wMovedDateTimeFormat.setText( action.getMovedDateTimeFormat() );
    }
    wAddMovedDateBeforeExtension.setSelection( action.isAddMovedDateBeforeExtension() );

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
    action.setMoveEmptyFolders( wMoveEmptyFolders.getSelection() );
    action.setIncludeSubfolders( wIncludeSubfolders.getSelection() );
    action.setArgFromPrevious( wPrevious.getSelection() );
    action.setAddresultfilesname( wAddFileToResult.getSelection() );
    action.setDestinationIsAFile( wDestinationIsAFile.getSelection() );
    action.setCreateDestinationFolder( wCreateDestinationFolder.getSelection() );
    action.setNrErrorsLessThan( wNrErrorsLessThan.getText() );

    action.setCreateMoveToFolder( wCreateMoveToFolder.getSelection() );

    if ( wSuccessCondition.getSelectionIndex() == 1 ) {
      action.setSuccessCondition( action.SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED );
    } else if ( wSuccessCondition.getSelectionIndex() == 2 ) {
      action.setSuccessCondition( action.SUCCESS_IF_ERRORS_LESS );
    } else {
      action.setSuccessCondition( action.SUCCESS_IF_NO_ERRORS );
    }

    if ( wIfFileExists.getSelectionIndex() == 1 ) {
      action.setIfFileExists( "overwrite_file" );
    } else if ( wIfFileExists.getSelectionIndex() == 2 ) {
      action.setIfFileExists( "unique_name" );
    } else if ( wIfFileExists.getSelectionIndex() == 3 ) {
      action.setIfFileExists( "delete_file" );
    } else if ( wIfFileExists.getSelectionIndex() == 4 ) {
      action.setIfFileExists( "move_file" );
    } else if ( wIfFileExists.getSelectionIndex() == 5 ) {
      action.setIfFileExists( "fail" );
    } else {
      action.setIfFileExists( "do_nothing" );
    }

    action.setDestinationFolder( wDestinationFolder.getText() );

    if ( wIfMovedFileExists.getSelectionIndex() == 1 ) {
      action.setIfMovedFileExists( "overwrite_file" );
    } else if ( wIfMovedFileExists.getSelectionIndex() == 2 ) {
      action.setIfMovedFileExists( "unique_name" );
    } else if ( wIfMovedFileExists.getSelectionIndex() == 3 ) {
      action.setIfMovedFileExists( "fail" );
    } else {
      action.setIfMovedFileExists( "do_nothing" );
    }

    action.setDoNotKeepFolderStructure( wDoNotKeepFolderStructure.getSelection() );
    action.setSimulate( wSimulate.getSelection() );

    action.setAddDate( wAddDate.getSelection() );
    action.setAddTime( wAddTime.getSelection() );
    action.setSpecifyFormat( wSpecifyFormat.getSelection() );
    action.setDateTimeFormat( wDateTimeFormat.getText() );
    action.setAddDateBeforeExtension( wAddDateBeforeExtension.getSelection() );

    action.setAddMovedDate( wAddMovedDate.getSelection() );
    action.setAddMovedTime( wAddMovedTime.getSelection() );
    action.setSpecifyMoveFormat( wSpecifyMoveFormat.getSelection() );
    action.setMovedDateTimeFormat( wMovedDateTimeFormat.getText() );
    action.setAddMovedDateBeforeExtension( wAddMovedDateBeforeExtension.getSelection() );

    int nrItems = wFields.nrNonEmpty();
    int nr = 0;
    for ( int i = 0; i < nrItems; i++ ) {
      String arg = wFields.getNonEmpty( i ).getText( 1 );
      if ( arg != null && arg.length() != 0 ) {
        nr++;
      }
    }
    action.sourceFileFolder = new String[ nr ];
    action.destinationFileFolder = new String[ nr ];
    action.wildcard = new String[ nr ];
    nr = 0;
    for ( int i = 0; i < nrItems; i++ ) {
      String source = wFields.getNonEmpty( i ).getText( 1 );
      String dest = wFields.getNonEmpty( i ).getText( 2 );
      String wild = wFields.getNonEmpty( i ).getText( 3 );
      if ( source != null && source.length() != 0 ) {
        action.sourceFileFolder[ nr ] = source;
        action.destinationFileFolder[ nr ] = dest;
        action.wildcard[ nr ] = wild;
        nr++;
      }
    }
    dispose();
  }
}
