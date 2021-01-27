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

package org.apache.hop.workflow.actions.zipfile;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ComboVar;
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
 * This dialog allows you to edit the Zip File action settings.
 *
 * @author Samatar Hassan
 * @since 27-02-2007
 */
public class ActionZipFileDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionZipFile.class; // For Translator

  private static final String[] FILETYPES = new String[] {
    BaseMessages.getString( PKG, "JobZipFiles.Filetype.Zip" ),
    BaseMessages.getString( PKG, "JobZipFiles.Filetype.All" ) };

  private Text wName;

  private Button wCreateParentFolder;

  private Label wlZipFilename;

  private Button wbZipFilename;

  private TextVar wZipFilename;

  private ActionZipFile action;

  private Shell shell;

  private Label wlSourceDirectory;

  private TextVar wSourceDirectory;

  private Label wlMovetoDirectory;

  private TextVar wMovetoDirectory;

  private Label wlWildcard;

  private TextVar wWildcard;

  private Label wlWildcardExclude;

  private TextVar wWildcardExclude;

  private Button wIncludeSubfolders;

  private CCombo wCompressionRate;

  private CCombo wIfFileExists;

  private CCombo wAfterZip;

  private Button wAddFileToResult;

  private Button wbSourceDirectory, wbSourceFile;

  private Button wbMovetoDirectory;

  private Button wgetFromPrevious;

  private Label wlAddDate;

  private Button wAddDate;

  private Label wlAddTime;

  private Button wAddTime;

  private Button wbShowFiles;

  private Button wSpecifyFormat;

  private Label wlDateTimeFormat;

  private CCombo wDateTimeFormat;

  private Label wlCreateMoveToDirectory;

  private Button wCreateMoveToDirectory;

  private ComboVar wStoredSourcePathDepth;

  private boolean changed;

  public ActionZipFileDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = (ActionZipFile) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "JobZipFiles.Name.Default" ) );
    }

  }

  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, action );

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobZipFiles.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // ZipFilename line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText( BaseMessages.getString( PKG, "JobZipFiles.Name.Label" ) );
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
    wGeneralTab.setText( BaseMessages.getString( PKG, "JobZipFiles.Tab.General.Label" ) );

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout( generalLayout );

    // SourceFile grouping?
    // ////////////////////////
    // START OF SourceFile GROUP///
    // /
    Group wSourceFiles = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wSourceFiles);
    wSourceFiles.setText( BaseMessages.getString( PKG, "JobZipFiles.SourceFiles.Group.Label" ) );

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;

    wSourceFiles.setLayout( groupLayout );

    // Get Result from previous?
    // Result from previous?
    Label wlgetFromPrevious = new Label(wSourceFiles, SWT.RIGHT);
    wlgetFromPrevious.setText( BaseMessages.getString( PKG, "JobZipFiles.getFromPrevious.Label" ) );
    props.setLook(wlgetFromPrevious);
    FormData fdlgetFromPrevious = new FormData();
    fdlgetFromPrevious.left = new FormAttachment( 0, 0 );
    fdlgetFromPrevious.top = new FormAttachment( wName, margin );
    fdlgetFromPrevious.right = new FormAttachment( middle, -margin );
    wlgetFromPrevious.setLayoutData(fdlgetFromPrevious);
    wgetFromPrevious = new Button(wSourceFiles, SWT.CHECK );
    props.setLook( wgetFromPrevious );
    wgetFromPrevious.setToolTipText( BaseMessages.getString( PKG, "JobZipFiles.getFromPrevious.Tooltip" ) );
    FormData fdgetFromPrevious = new FormData();
    fdgetFromPrevious.left = new FormAttachment( middle, 0 );
    fdgetFromPrevious.top = new FormAttachment( wName, margin );
    fdgetFromPrevious.right = new FormAttachment( 100, 0 );
    wgetFromPrevious.setLayoutData(fdgetFromPrevious);
    wgetFromPrevious.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        setGetFromPrevious();
      }
    } );

    // TargetDirectory line
    wlSourceDirectory = new Label(wSourceFiles, SWT.RIGHT );
    wlSourceDirectory.setText( BaseMessages.getString( PKG, "JobZipFiles.SourceDir.Label" ) );
    props.setLook( wlSourceDirectory );
    FormData fdlSourceDirectory = new FormData();
    fdlSourceDirectory.left = new FormAttachment( 0, 0 );
    fdlSourceDirectory.top = new FormAttachment( wgetFromPrevious, margin );
    fdlSourceDirectory.right = new FormAttachment( middle, -margin );
    wlSourceDirectory.setLayoutData(fdlSourceDirectory);

    // Browse folders button ...
    wbSourceDirectory = new Button(wSourceFiles, SWT.PUSH | SWT.CENTER );
    props.setLook( wbSourceDirectory );
    wbSourceDirectory.setText( BaseMessages.getString( PKG, "JobZipFiles.BrowseFolders.Label" ) );
    FormData fdbSourceDirectory = new FormData();
    fdbSourceDirectory.right = new FormAttachment( 100, 0 );
    fdbSourceDirectory.top = new FormAttachment( wgetFromPrevious, margin );
    wbSourceDirectory.setLayoutData(fdbSourceDirectory);

    // Browse Destination file browse button ...
    wbSourceFile = new Button(wSourceFiles, SWT.PUSH | SWT.CENTER );
    props.setLook( wbSourceFile );
    wbSourceFile.setText( BaseMessages.getString( PKG, "JobZipFiles.BrowseFiles.Label" ) );
    FormData fdbSourceFile = new FormData();
    fdbSourceFile.right = new FormAttachment( wbSourceDirectory, -margin );
    fdbSourceFile.top = new FormAttachment( wgetFromPrevious, margin );
    wbSourceFile.setLayoutData(fdbSourceFile);

    wSourceDirectory =
      new TextVar( variables, wSourceFiles, SWT.SINGLE | SWT.LEFT | SWT.BORDER, BaseMessages.getString(
        PKG, "JobZipFiles.SourceDir.Tooltip" ) );
    props.setLook( wSourceDirectory );
    wSourceDirectory.addModifyListener( lsMod );
    FormData fdSourceDirectory = new FormData();
    fdSourceDirectory.left = new FormAttachment( middle, 0 );
    fdSourceDirectory.top = new FormAttachment( wgetFromPrevious, margin );
    fdSourceDirectory.right = new FormAttachment( wbSourceFile, -margin );
    wSourceDirectory.setLayoutData(fdSourceDirectory);

    // Wildcard line
    wlWildcard = new Label(wSourceFiles, SWT.RIGHT );
    wlWildcard.setText( BaseMessages.getString( PKG, "JobZipFiles.Wildcard.Label" ) );
    props.setLook( wlWildcard );
    FormData fdlWildcard = new FormData();
    fdlWildcard.left = new FormAttachment( 0, 0 );
    fdlWildcard.top = new FormAttachment( wSourceDirectory, margin );
    fdlWildcard.right = new FormAttachment( middle, -margin );
    wlWildcard.setLayoutData(fdlWildcard);
    wWildcard =
      new TextVar( variables, wSourceFiles, SWT.SINGLE | SWT.LEFT | SWT.BORDER, BaseMessages.getString(
        PKG, "JobZipFiles.Wildcard.Tooltip" ) );
    props.setLook( wWildcard );
    wWildcard.addModifyListener( lsMod );
    FormData fdWildcard = new FormData();
    fdWildcard.left = new FormAttachment( middle, 0 );
    fdWildcard.top = new FormAttachment( wSourceDirectory, margin );
    fdWildcard.right = new FormAttachment( 100, 0 );
    wWildcard.setLayoutData(fdWildcard);

    // Wildcard to exclude
    wlWildcardExclude = new Label(wSourceFiles, SWT.RIGHT );
    wlWildcardExclude.setText( BaseMessages.getString( PKG, "JobZipFiles.WildcardExclude.Label" ) );
    props.setLook( wlWildcardExclude );
    FormData fdlWildcardExclude = new FormData();
    fdlWildcardExclude.left = new FormAttachment( 0, 0 );
    fdlWildcardExclude.top = new FormAttachment( wWildcard, margin );
    fdlWildcardExclude.right = new FormAttachment( middle, -margin );
    wlWildcardExclude.setLayoutData(fdlWildcardExclude);
    wWildcardExclude =
      new TextVar( variables, wSourceFiles, SWT.SINGLE | SWT.LEFT | SWT.BORDER, BaseMessages.getString(
        PKG, "JobZipFiles.WildcardExclude.Tooltip" ) );
    props.setLook( wWildcardExclude );
    wWildcardExclude.addModifyListener( lsMod );
    FormData fdWildcardExclude = new FormData();
    fdWildcardExclude.left = new FormAttachment( middle, 0 );
    fdWildcardExclude.top = new FormAttachment( wWildcard, margin );
    fdWildcardExclude.right = new FormAttachment( 100, 0 );
    wWildcardExclude.setLayoutData(fdWildcardExclude);

    // Include sub-folders?
    //
    Label wlIncludeSubfolders = new Label(wSourceFiles, SWT.RIGHT);
    wlIncludeSubfolders.setText( BaseMessages.getString( PKG, "JobZipFiles.IncludeSubfolders.Label" ) );
    props.setLook(wlIncludeSubfolders);
    FormData fdlIncludeSubfolders = new FormData();
    fdlIncludeSubfolders.left = new FormAttachment( 0, 0 );
    fdlIncludeSubfolders.top = new FormAttachment( wWildcardExclude, margin );
    fdlIncludeSubfolders.right = new FormAttachment( middle, -margin );
    wlIncludeSubfolders.setLayoutData(fdlIncludeSubfolders);
    wIncludeSubfolders = new Button(wSourceFiles, SWT.CHECK );
    props.setLook( wIncludeSubfolders );
    wIncludeSubfolders.setToolTipText( BaseMessages.getString( PKG, "JobZipFiles.IncludeSubfolders.Tooltip" ) );
    FormData fdIncludeSubfolders = new FormData();
    fdIncludeSubfolders.left = new FormAttachment( middle, 0 );
    fdIncludeSubfolders.top = new FormAttachment( wWildcardExclude, margin );
    fdIncludeSubfolders.right = new FormAttachment( 100, 0 );
    wIncludeSubfolders.setLayoutData(fdIncludeSubfolders);
    wIncludeSubfolders.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    FormData fdSourceFiles = new FormData();
    fdSourceFiles.left = new FormAttachment( 0, margin );
    fdSourceFiles.top = new FormAttachment( wName, margin );
    fdSourceFiles.right = new FormAttachment( 100, -margin );
    wSourceFiles.setLayoutData(fdSourceFiles);
    // ///////////////////////////////////////////////////////////
    // / END OF SourceFile GROUP
    // ///////////////////////////////////////////////////////////

    // ZipFile grouping?
    // ////////////////////////
    // START OF ZipFile GROUP///
    // /
    Group wZipFile = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wZipFile);
    wZipFile.setText( BaseMessages.getString( PKG, "JobZipFiles.ZipFile.Group.Label" ) );

    FormLayout groupLayoutzipfile = new FormLayout();
    groupLayoutzipfile.marginWidth = 10;
    groupLayoutzipfile.marginHeight = 10;

    wZipFile.setLayout( groupLayoutzipfile );

    // ZipFilename line
    wlZipFilename = new Label(wZipFile, SWT.RIGHT );
    wlZipFilename.setText( BaseMessages.getString( PKG, "JobZipFiles.ZipFilename.Label" ) );
    props.setLook( wlZipFilename );
    FormData fdlZipFilename = new FormData();
    fdlZipFilename.left = new FormAttachment( 0, 0 );
    fdlZipFilename.top = new FormAttachment(wSourceFiles, margin );
    fdlZipFilename.right = new FormAttachment( middle, -margin );
    wlZipFilename.setLayoutData(fdlZipFilename);
    wbZipFilename = new Button(wZipFile, SWT.PUSH | SWT.CENTER );
    props.setLook( wbZipFilename );
    wbZipFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbZipFilename = new FormData();
    fdbZipFilename.right = new FormAttachment( 100, 0 );
    fdbZipFilename.top = new FormAttachment(wSourceFiles, 0 );
    wbZipFilename.setLayoutData(fdbZipFilename);
    wZipFilename = new TextVar( variables, wZipFile, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wZipFilename );
    wZipFilename.addModifyListener( lsMod );
    FormData fdZipFilename = new FormData();
    fdZipFilename.left = new FormAttachment( middle, 0 );
    fdZipFilename.top = new FormAttachment(wSourceFiles, margin );
    fdZipFilename.right = new FormAttachment( wbZipFilename, -margin );
    wZipFilename.setLayoutData(fdZipFilename);

    // Whenever something changes, set the tooltip to the expanded version:
    wZipFilename.addModifyListener( e -> wZipFilename.setToolTipText( variables.resolve( wZipFilename.getText() ) ) );

    wbZipFilename.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wZipFilename, variables,
      new String[] { "*.zip;*.ZIP", "*" }, FILETYPES, true )
    );

    // Create Parent Folder
    Label wlCreateParentFolder = new Label(wZipFile, SWT.RIGHT);
    wlCreateParentFolder.setText( BaseMessages.getString( PKG, "JobZipFiles.CreateParentFolder.Label" ) );
    props.setLook(wlCreateParentFolder);
    FormData fdlCreateParentFolder = new FormData();
    fdlCreateParentFolder.left = new FormAttachment( 0, 0 );
    fdlCreateParentFolder.top = new FormAttachment( wZipFilename, margin );
    fdlCreateParentFolder.right = new FormAttachment( middle, -margin );
    wlCreateParentFolder.setLayoutData(fdlCreateParentFolder);
    wCreateParentFolder = new Button(wZipFile, SWT.CHECK );
    wCreateParentFolder.setToolTipText( BaseMessages.getString( PKG, "JobZipFiles.CreateParentFolder.Tooltip" ) );
    props.setLook( wCreateParentFolder );
    FormData fdCreateParentFolder = new FormData();
    fdCreateParentFolder.left = new FormAttachment( middle, 0 );
    fdCreateParentFolder.top = new FormAttachment( wlCreateParentFolder, 0, SWT.CENTER );
    fdCreateParentFolder.right = new FormAttachment( 100, 0 );
    wCreateParentFolder.setLayoutData(fdCreateParentFolder);
    wCreateParentFolder.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Create multi-part file?
    wlAddDate = new Label(wZipFile, SWT.RIGHT );
    wlAddDate.setText( BaseMessages.getString( PKG, "JobZipFiles.AddDate.Label" ) );
    props.setLook( wlAddDate );
    FormData fdlAddDate = new FormData();
    fdlAddDate.left = new FormAttachment( 0, 0 );
    fdlAddDate.top = new FormAttachment( wCreateParentFolder, margin );
    fdlAddDate.right = new FormAttachment( middle, -margin );
    wlAddDate.setLayoutData(fdlAddDate);
    wAddDate = new Button(wZipFile, SWT.CHECK );
    props.setLook( wAddDate );
    wAddDate.setToolTipText( BaseMessages.getString( PKG, "JobZipFiles.AddDate.Tooltip" ) );
    FormData fdAddDate = new FormData();
    fdAddDate.left = new FormAttachment( middle, 0 );
    fdAddDate.top = new FormAttachment( wlAddDate, 0, SWT.CENTER );
    fdAddDate.right = new FormAttachment( 100, 0 );
    wAddDate.setLayoutData(fdAddDate);
    wAddDate.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );
    // Create multi-part file?
    wlAddTime = new Label(wZipFile, SWT.RIGHT );
    wlAddTime.setText( BaseMessages.getString( PKG, "JobZipFiles.AddTime.Label" ) );
    props.setLook( wlAddTime );
    FormData fdlAddTime = new FormData();
    fdlAddTime.left = new FormAttachment( 0, 0 );
    fdlAddTime.top = new FormAttachment( wAddDate, margin );
    fdlAddTime.right = new FormAttachment( middle, -margin );
    wlAddTime.setLayoutData(fdlAddTime);
    wAddTime = new Button(wZipFile, SWT.CHECK );
    props.setLook( wAddTime );
    wAddTime.setToolTipText( BaseMessages.getString( PKG, "JobZipFiles.AddTime.Tooltip" ) );
    FormData fdAddTime = new FormData();
    fdAddTime.left = new FormAttachment( middle, 0 );
    fdAddTime.top = new FormAttachment( wlAddTime, 0, SWT.CENTER );
    fdAddTime.right = new FormAttachment( 100, 0 );
    wAddTime.setLayoutData(fdAddTime);
    wAddTime.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Specify date time format?
    Label wlSpecifyFormat = new Label(wZipFile, SWT.RIGHT);
    wlSpecifyFormat.setText( BaseMessages.getString( PKG, "JobZipFiles.SpecifyFormat.Label" ) );
    props.setLook(wlSpecifyFormat);
    FormData fdlSpecifyFormat = new FormData();
    fdlSpecifyFormat.left = new FormAttachment( 0, 0 );
    fdlSpecifyFormat.top = new FormAttachment( wAddTime, margin );
    fdlSpecifyFormat.right = new FormAttachment( middle, -margin );
    wlSpecifyFormat.setLayoutData(fdlSpecifyFormat);
    wSpecifyFormat = new Button(wZipFile, SWT.CHECK );
    props.setLook( wSpecifyFormat );
    wSpecifyFormat.setToolTipText( BaseMessages.getString( PKG, "JobZipFiles.SpecifyFormat.Tooltip" ) );
    FormData fdSpecifyFormat = new FormData();
    fdSpecifyFormat.left = new FormAttachment( middle, 0 );
    fdSpecifyFormat.top = new FormAttachment( wAddTime, margin );
    fdSpecifyFormat.right = new FormAttachment( 100, 0 );
    wSpecifyFormat.setLayoutData(fdSpecifyFormat);
    wSpecifyFormat.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        setDateTimeFormat();
      }
    } );

    // Prepare a list of possible DateTimeFormats...
    String[] dats = Const.getDateFormats();

    // DateTimeFormat
    wlDateTimeFormat = new Label(wZipFile, SWT.RIGHT );
    wlDateTimeFormat.setText( BaseMessages.getString( PKG, "JobZipFiles.DateTimeFormat.Label" ) );
    props.setLook( wlDateTimeFormat );
    FormData fdlDateTimeFormat = new FormData();
    fdlDateTimeFormat.left = new FormAttachment( 0, 0 );
    fdlDateTimeFormat.top = new FormAttachment( wSpecifyFormat, margin );
    fdlDateTimeFormat.right = new FormAttachment( middle, -margin );
    wlDateTimeFormat.setLayoutData(fdlDateTimeFormat);
    wDateTimeFormat = new CCombo(wZipFile, SWT.BORDER | SWT.READ_ONLY );
    wDateTimeFormat.setEditable( true );
    props.setLook( wDateTimeFormat );
    wDateTimeFormat.addModifyListener( lsMod );
    FormData fdDateTimeFormat = new FormData();
    fdDateTimeFormat.left = new FormAttachment( middle, 0 );
    fdDateTimeFormat.top = new FormAttachment( wSpecifyFormat, margin );
    fdDateTimeFormat.right = new FormAttachment( 100, 0 );
    wDateTimeFormat.setLayoutData(fdDateTimeFormat);
    for (String dat : dats) {
      wDateTimeFormat.add(dat);
    }

    wbShowFiles = new Button(wZipFile, SWT.PUSH | SWT.CENTER );
    props.setLook( wbShowFiles );
    wbShowFiles.setText( BaseMessages.getString( PKG, "JobZipFiles.ShowFile.Button" ) );
    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment( middle, 0 );
    fdbShowFiles.top = new FormAttachment( wDateTimeFormat, margin * 2 );
    wbShowFiles.setLayoutData(fdbShowFiles);
    wbShowFiles.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        if ( !Utils.isEmpty( wZipFilename.getText() ) ) {
          ActionZipFile actionZipFile = new ActionZipFile();
          String[] filename = new String[ 1 ];
          filename[ 0 ] = ActionZipFileDialog.this.action.getFullFilename(
              wZipFilename.getText(), wAddDate.getSelection(), wAddTime.getSelection(), wSpecifyFormat.getSelection(), wDateTimeFormat.getText() );
          if ( filename != null && filename.length > 0 ) {
            EnterSelectionDialog esd = new EnterSelectionDialog( shell, filename,
              BaseMessages.getString( PKG, "JobZipFiles.SelectOutputFiles.DialogTitle" ),
              BaseMessages.getString( PKG, "JobZipFiles.SelectOutputFiles.DialogMessage" ) );
            esd.setViewOnly();
            esd.open();
          } else {
            MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
            mb.setMessage( BaseMessages.getString( PKG, "JobZipFiles.NoFilesFound.DialogMessage" ) );
            mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
            mb.open();
          }
        }
      }
    } );

    FormData fdZipFile = new FormData();
    fdZipFile.left = new FormAttachment( 0, margin );
    fdZipFile.top = new FormAttachment(wSourceFiles, margin );
    fdZipFile.right = new FormAttachment( 100, -margin );
    wZipFile.setLayoutData(fdZipFile);

    // ///////////////////////////////////////////////////////////
    // END OF ZipFile GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( 0, 0 );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 500, -margin );
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);
    props.setLook(wGeneralComp);

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF ADVANCED TAB ///
    // ////////////////////////

    CTabItem wAdvancedTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdvancedTab.setText( BaseMessages.getString( PKG, "JobZipFiles.Tab.Advanced.Label" ) );

    Composite wAdvancedComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wAdvancedComp);

    FormLayout advancedLayout = new FormLayout();
    advancedLayout.marginWidth = 3;
    advancedLayout.marginHeight = 3;
    wAdvancedComp.setLayout( advancedLayout );

    // ////////////////////////////
    // START OF Settings GROUP
    //
    Group wSettings = new Group(wAdvancedComp, SWT.SHADOW_NONE);
    props.setLook(wSettings);
    wSettings.setText( BaseMessages.getString( PKG, "JobZipFiles.Advanced.Group.Label" ) );
    FormLayout groupLayoutSettings = new FormLayout();
    groupLayoutSettings.marginWidth = 10;
    groupLayoutSettings.marginHeight = 10;
    wSettings.setLayout( groupLayoutSettings );

    // Compression Rate
    Label wlCompressionRate = new Label(wSettings, SWT.RIGHT);
    wlCompressionRate.setText( BaseMessages.getString( PKG, "JobZipFiles.CompressionRate.Label" ) );
    props.setLook(wlCompressionRate);
    FormData fdlCompressionRate = new FormData();
    fdlCompressionRate.left = new FormAttachment( 0, -margin );
    fdlCompressionRate.right = new FormAttachment( middle, -margin );
    fdlCompressionRate.top = new FormAttachment(wZipFile, margin );
    wlCompressionRate.setLayoutData(fdlCompressionRate);
    wCompressionRate = new CCombo(wSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wCompressionRate.add( BaseMessages.getString( PKG, "JobZipFiles.NO_COMP_CompressionRate.Label" ) );
    wCompressionRate.add( BaseMessages.getString( PKG, "JobZipFiles.DEF_COMP_CompressionRate.Label" ) );
    wCompressionRate.add( BaseMessages.getString( PKG, "JobZipFiles.BEST_COMP_CompressionRate.Label" ) );
    wCompressionRate.add( BaseMessages.getString( PKG, "JobZipFiles.BEST_SPEED_CompressionRate.Label" ) );
    wCompressionRate.select( 1 ); // +1: starts at -1

    props.setLook( wCompressionRate );
    FormData fdCompressionRate = new FormData();
    fdCompressionRate.left = new FormAttachment( middle, 0 );
    fdCompressionRate.top = new FormAttachment(wZipFile, margin );
    fdCompressionRate.right = new FormAttachment( 100, 0 );
    wCompressionRate.setLayoutData(fdCompressionRate);

    // If File Exists
    Label wlIfFileExists = new Label(wSettings, SWT.RIGHT);
    wlIfFileExists.setText( BaseMessages.getString( PKG, "JobZipFiles.IfZipFileExists.Label" ) );
    props.setLook(wlIfFileExists);
    FormData fdlIfFileExists = new FormData();
    fdlIfFileExists.left = new FormAttachment( 0, -margin );
    fdlIfFileExists.right = new FormAttachment( middle, -margin );
    fdlIfFileExists.top = new FormAttachment( wCompressionRate, margin );
    wlIfFileExists.setLayoutData(fdlIfFileExists);
    wIfFileExists = new CCombo(wSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wIfFileExists.add( BaseMessages.getString( PKG, "JobZipFiles.Create_NewFile_IfFileExists.Label" ) );
    wIfFileExists.add( BaseMessages.getString( PKG, "JobZipFiles.Append_File_IfFileExists.Label" ) );
    wIfFileExists.add( BaseMessages.getString( PKG, "JobZipFiles.Do_Nothing_IfFileExists.Label" ) );

    wIfFileExists.add( BaseMessages.getString( PKG, "JobZipFiles.Fail_IfFileExists.Label" ) );
    wIfFileExists.select( 3 ); // +1: starts at -1

    props.setLook( wIfFileExists );
    FormData fdIfFileExists = new FormData();
    fdIfFileExists.left = new FormAttachment( middle, 0 );
    fdIfFileExists.top = new FormAttachment( wCompressionRate, margin );
    fdIfFileExists.right = new FormAttachment( 100, 0 );
    wIfFileExists.setLayoutData(fdIfFileExists);

    // After Zipping
    Label wlAfterZip = new Label(wSettings, SWT.RIGHT);
    wlAfterZip.setText( BaseMessages.getString( PKG, "JobZipFiles.AfterZip.Label" ) );
    props.setLook(wlAfterZip);
    FormData fdlAfterZip = new FormData();
    fdlAfterZip.left = new FormAttachment( 0, -margin );
    fdlAfterZip.right = new FormAttachment( middle, -margin );
    fdlAfterZip.top = new FormAttachment( wIfFileExists, margin );
    wlAfterZip.setLayoutData(fdlAfterZip);
    wAfterZip = new CCombo(wSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wAfterZip.add( BaseMessages.getString( PKG, "JobZipFiles.Do_Nothing_AfterZip.Label" ) );
    wAfterZip.add( BaseMessages.getString( PKG, "JobZipFiles.Delete_Files_AfterZip.Label" ) );
    wAfterZip.add( BaseMessages.getString( PKG, "JobZipFiles.Move_Files_AfterZip.Label" ) );

    wAfterZip.select( 0 ); // +1: starts at -1

    props.setLook( wAfterZip );
    FormData fdAfterZip = new FormData();
    fdAfterZip.left = new FormAttachment( middle, 0 );
    fdAfterZip.top = new FormAttachment( wIfFileExists, margin );
    fdAfterZip.right = new FormAttachment( 100, 0 );
    wAfterZip.setLayoutData(fdAfterZip);

    wAfterZip.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        AfterZipActivate();

      }
    } );

    // moveTo Directory
    wlMovetoDirectory = new Label(wSettings, SWT.RIGHT );
    wlMovetoDirectory.setText( BaseMessages.getString( PKG, "JobZipFiles.MovetoDirectory.Label" ) );
    props.setLook( wlMovetoDirectory );
    FormData fdlMovetoDirectory = new FormData();
    fdlMovetoDirectory.left = new FormAttachment( 0, 0 );
    fdlMovetoDirectory.top = new FormAttachment( wAfterZip, margin );
    fdlMovetoDirectory.right = new FormAttachment( middle, -margin );
    wlMovetoDirectory.setLayoutData(fdlMovetoDirectory);

    // Browse folders button ...
    wbMovetoDirectory = new Button(wSettings, SWT.PUSH | SWT.CENTER );
    props.setLook( wbMovetoDirectory );
    wbMovetoDirectory.setText( BaseMessages.getString( PKG, "JobZipFiles.BrowseFolders.Label" ) );
    FormData fdbMovetoDirectory = new FormData();
    fdbMovetoDirectory.right = new FormAttachment( 100, 0 );
    fdbMovetoDirectory.top = new FormAttachment( wAfterZip, margin );
    wbMovetoDirectory.setLayoutData(fdbMovetoDirectory);

    wMovetoDirectory =
      new TextVar( variables, wSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER, BaseMessages.getString(
        PKG, "JobZipFiles.MovetoDirectory.Tooltip" ) );
    props.setLook( wMovetoDirectory );
    wMovetoDirectory.addModifyListener( lsMod );
    FormData fdMovetoDirectory = new FormData();
    fdMovetoDirectory.left = new FormAttachment( middle, 0 );
    fdMovetoDirectory.top = new FormAttachment( wAfterZip, margin );
    fdMovetoDirectory.right = new FormAttachment( wbMovetoDirectory, -margin );
    wMovetoDirectory.setLayoutData(fdMovetoDirectory);

    // create moveto folder
    wlCreateMoveToDirectory = new Label(wSettings, SWT.RIGHT );
    wlCreateMoveToDirectory.setText( BaseMessages.getString( PKG, "JobZipFiles.createMoveToDirectory.Label" ) );
    props.setLook( wlCreateMoveToDirectory );
    FormData fdlCreateMoveToDirectory = new FormData();
    fdlCreateMoveToDirectory.left = new FormAttachment( 0, 0 );
    fdlCreateMoveToDirectory.top = new FormAttachment( wMovetoDirectory, margin );
    fdlCreateMoveToDirectory.right = new FormAttachment( middle, -margin );
    wlCreateMoveToDirectory.setLayoutData(fdlCreateMoveToDirectory);
    wCreateMoveToDirectory = new Button(wSettings, SWT.CHECK );
    props.setLook( wCreateMoveToDirectory );
    wCreateMoveToDirectory.setToolTipText( BaseMessages.getString(
      PKG, "JobZipFiles.createMoveToDirectory.Tooltip" ) );
    FormData fdCreateMoveToDirectory = new FormData();
    fdCreateMoveToDirectory.left = new FormAttachment( middle, 0 );
    fdCreateMoveToDirectory.top = new FormAttachment( wMovetoDirectory, margin );
    fdCreateMoveToDirectory.right = new FormAttachment( 100, 0 );
    wCreateMoveToDirectory.setLayoutData(fdCreateMoveToDirectory);
    wCreateMoveToDirectory.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    Label wlStoredSourcePathDepth = new Label(wSettings, SWT.RIGHT);
    wlStoredSourcePathDepth.setText( BaseMessages.getString( PKG, "JobZipFiles.StoredSourcePathDepth.Label" ) );
    props.setLook(wlStoredSourcePathDepth);
    FormData fdlStoredSourcePathDepth = new FormData();
    fdlStoredSourcePathDepth.left = new FormAttachment( 0, 0 );
    fdlStoredSourcePathDepth.top = new FormAttachment( wCreateMoveToDirectory, margin );
    fdlStoredSourcePathDepth.right = new FormAttachment( middle, -margin );
    wlStoredSourcePathDepth.setLayoutData(fdlStoredSourcePathDepth);
    wStoredSourcePathDepth = new ComboVar( variables, wSettings, SWT.SINGLE | SWT.BORDER );
    props.setLook( wStoredSourcePathDepth );
    wStoredSourcePathDepth.setToolTipText( BaseMessages.getString(
      PKG, "JobZipFiles.StoredSourcePathDepth.Tooltip" ) );
    FormData fdStoredSourcePathDepth = new FormData();
    fdStoredSourcePathDepth.left = new FormAttachment( middle, 0 );
    fdStoredSourcePathDepth.top = new FormAttachment( wCreateMoveToDirectory, margin );
    fdStoredSourcePathDepth.right = new FormAttachment( 100, 0 );
    wStoredSourcePathDepth.setLayoutData(fdStoredSourcePathDepth);
    wStoredSourcePathDepth.setItems( new String[] {
      "0 : /project-hop/work/transfer/input/project/file.txt", "1 : file.txt", "2 : project/file.txt",
      "3 : input/project/file.txt", "4 : transfer/input/project/file.txt",
      "5 : work/transfer/input/project/file.txt", "6 : project-hop/work/transfer/input/project/file.txt",
      "7 : project-hop/work/transfer/input/project/file.txt", "8 : project-hop/work/transfer/input/project/file.txt", } );

    FormData fdSettings = new FormData();
    fdSettings.left = new FormAttachment( 0, margin );
    fdSettings.top = new FormAttachment(wZipFile, margin );
    fdSettings.right = new FormAttachment( 100, -margin );
    wSettings.setLayoutData(fdSettings);
    // ///////////////////////////////////////////////////////////
    // / END OF Settings GROUP
    // ///////////////////////////////////////////////////////////

    // fileresult grouping?
    // ////////////////////////
    // START OF LOGGING GROUP///
    // /
    Group wFileResult = new Group(wAdvancedComp, SWT.SHADOW_NONE);
    props.setLook(wFileResult);
    wFileResult.setText( BaseMessages.getString( PKG, "JobZipFiles.FileResult.Group.Label" ) );

    FormLayout groupLayoutresult = new FormLayout();
    groupLayoutresult.marginWidth = 10;
    groupLayoutresult.marginHeight = 10;

    wFileResult.setLayout( groupLayoutresult );

    // Add file to result
    // Add File to result
    Label wlAddFileToResult = new Label(wFileResult, SWT.RIGHT);
    wlAddFileToResult.setText( BaseMessages.getString( PKG, "JobZipFiles.AddFileToResult.Label" ) );
    props.setLook(wlAddFileToResult);
    FormData fdlAddFileToResult = new FormData();
    fdlAddFileToResult.left = new FormAttachment( 0, 0 );
    fdlAddFileToResult.top = new FormAttachment(wSettings, margin );
    fdlAddFileToResult.right = new FormAttachment( middle, -margin );
    wlAddFileToResult.setLayoutData(fdlAddFileToResult);
    wAddFileToResult = new Button(wFileResult, SWT.CHECK );
    props.setLook( wAddFileToResult );
    wAddFileToResult.setToolTipText( BaseMessages.getString( PKG, "JobZipFiles.AddFileToResult.Tooltip" ) );
    FormData fdAddFileToResult = new FormData();
    fdAddFileToResult.left = new FormAttachment( middle, 0 );
    fdAddFileToResult.top = new FormAttachment(wSettings, margin );
    fdAddFileToResult.right = new FormAttachment( 100, 0 );
    wAddFileToResult.setLayoutData(fdAddFileToResult);
    wAddFileToResult.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    FormData fdFileResult = new FormData();
    fdFileResult.left = new FormAttachment( 0, margin );
    fdFileResult.top = new FormAttachment(wSettings, margin );
    fdFileResult.right = new FormAttachment( 100, -margin );
    wFileResult.setLayoutData(fdFileResult);
    // ///////////////////////////////////////////////////////////
    // / END OF FILE RESULT GROUP
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

    // ///////////////////////////////////////////////////////////
    // / END OF Advanced TAB
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

    wbSourceDirectory.addListener( SWT.Selection, e-> BaseDialog.presentDirectoryDialog( shell, wSourceDirectory, variables ) );
    wbMovetoDirectory.addListener( SWT.Selection, e-> BaseDialog.presentDirectoryDialog( shell, wMovetoDirectory, variables ) );
    wbSourceFile.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wSourceDirectory, variables,
      new String[] { "*" }, FILETYPES, true )
    );

    wName.addSelectionListener(lsDef);
    wZipFilename.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    setGetFromPrevious();
    AfterZipActivate();
    setDateTimeFormat();

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

  public void setGetFromPrevious() {
    wlSourceDirectory.setEnabled( !wgetFromPrevious.getSelection() );
    wSourceDirectory.setEnabled( !wgetFromPrevious.getSelection() );
    wWildcard.setEnabled( !wgetFromPrevious.getSelection() );
    wlWildcard.setEnabled( !wgetFromPrevious.getSelection() );
    wWildcardExclude.setEnabled( !wgetFromPrevious.getSelection() );
    wlWildcardExclude.setEnabled( !wgetFromPrevious.getSelection() );
    wbSourceDirectory.setEnabled( !wgetFromPrevious.getSelection() );
    wlZipFilename.setEnabled( !wgetFromPrevious.getSelection() );
    wZipFilename.setEnabled( !wgetFromPrevious.getSelection() );
    wbZipFilename.setEnabled( !wgetFromPrevious.getSelection() );
    wbSourceFile.setEnabled( !wgetFromPrevious.getSelection() );
    wlAddDate.setEnabled( !wgetFromPrevious.getSelection() );
    wAddDate.setEnabled( !wgetFromPrevious.getSelection() );
    wlAddTime.setEnabled( !wgetFromPrevious.getSelection() );
    wAddTime.setEnabled( !wgetFromPrevious.getSelection() );
    wbShowFiles.setEnabled( !wgetFromPrevious.getSelection() );

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

  public void AfterZipActivate() {

    action.setChanged();
    if ( wAfterZip.getSelectionIndex() == 2 ) {
      wMovetoDirectory.setEnabled( true );
      wlMovetoDirectory.setEnabled( true );
      wbMovetoDirectory.setEnabled( true );
      wlCreateMoveToDirectory.setEnabled( true );
      wCreateMoveToDirectory.setEnabled( true );
    } else {
      wMovetoDirectory.setEnabled( false );
      wlMovetoDirectory.setEnabled( false );
      wbMovetoDirectory.setEnabled( false );
      wlCreateMoveToDirectory.setEnabled( false );
      wCreateMoveToDirectory.setEnabled( true );
    }
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wName.setText( Const.nullToEmpty( action.getName() ) );
    wZipFilename.setText( Const.nullToEmpty( action.getZipFilename() ) );

    if ( action.compressionRate >= 0 ) {
      wCompressionRate.select( action.compressionRate );
    } else {
      wCompressionRate.select( 1 ); // DEFAULT
    }

    if ( action.ifZipFileExists >= 0 ) {
      wIfFileExists.select( action.ifZipFileExists );
    } else {
      wIfFileExists.select( 2 ); // NOTHING
    }

    wWildcard.setText( Const.NVL( action.getWildcard(), "" ) );
    wWildcardExclude.setText( Const.NVL( action.getWildcardExclude(), "" ) );
    wSourceDirectory.setText( Const.NVL( action.getSourceDirectory(), "" ) );
    wMovetoDirectory.setText( Const.NVL( action.getMoveToDirectory(), "" ) );
    if ( action.afterZip >= 0 ) {
      wAfterZip.select( action.afterZip );
    } else {
      wAfterZip.select( 0 ); // NOTHING
    }

    wAddFileToResult.setSelection( action.isAddFileToResult() );
    wgetFromPrevious.setSelection( action.getDatafromprevious() );
    wCreateParentFolder.setSelection( action.getcreateparentfolder() );
    wAddDate.setSelection( action.isDateInFilename() );
    wAddTime.setSelection( action.isTimeInFilename() );

    wDateTimeFormat.setText( Const.NVL( action.getDateTimeFormat(), "" ) );
    wSpecifyFormat.setSelection( action.isSpecifyFormat() );
    wCreateMoveToDirectory.setSelection( action.isCreateMoveToDirectory() );
    wIncludeSubfolders.setSelection( action.isIncludingSubFolders() );

    wStoredSourcePathDepth.setText( Const.NVL( action.getStoredSourcePathDepth(), "" ) );

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
    action.setZipFilename( wZipFilename.getText() );

    action.compressionRate = wCompressionRate.getSelectionIndex();
    action.ifZipFileExists = wIfFileExists.getSelectionIndex();

    action.setWildcard( wWildcard.getText() );
    action.setWildcardExclude( wWildcardExclude.getText() );
    action.setSourceDirectory( wSourceDirectory.getText() );

    action.setMoveToDirectory( wMovetoDirectory.getText() );

    action.afterZip = wAfterZip.getSelectionIndex();

    action.setAddFileToResult( wAddFileToResult.getSelection() );
    action.setDatafromprevious( wgetFromPrevious.getSelection() );
    action.setcreateparentfolder( wCreateParentFolder.getSelection() );
    action.setDateInFilename( wAddDate.getSelection() );
    action.setTimeInFilename( wAddTime.getSelection() );
    action.setSpecifyFormat( wSpecifyFormat.getSelection() );
    action.setDateTimeFormat( wDateTimeFormat.getText() );
    action.setCreateMoveToDirectory( wCreateMoveToDirectory.getSelection() );
    action.setIncludingSubFolders( wIncludeSubfolders.getSelection() );

    action.setStoredSourcePathDepth( wStoredSourcePathDepth.getText() );

    dispose();
  }
}
