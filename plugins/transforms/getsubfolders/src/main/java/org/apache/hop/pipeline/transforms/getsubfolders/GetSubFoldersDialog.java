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

package org.apache.hop.pipeline.transforms.getsubfolders;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class GetSubFoldersDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = GetSubFoldersMeta.class; // For Translator

  private Label wlFoldername;

  private Button wbbFoldername; // Browse: add directory

  private Button wbdFoldername; // Delete

  private Button wbeFoldername; // Edit

  private Button wbaFoldername; // Add or change

  private TextVar wFoldername;

  private Label wlFoldernameList;

  private TableView wFoldernameList;

  private final GetSubFoldersMeta input;

  private Button wFolderField;

  private Label wlFilenameField;
  private ComboVar wFoldernameField;

  private Label wlLimit;
  private Text wLimit;

  private Button wInclRownum;

  private Label wlInclRownumField;
  private TextVar wInclRownumField;

  public GetSubFoldersDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (GetSubFoldersMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "GetSubFoldersDialog.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Buttons at the bottom
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wPreview = new Button( shell, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( PKG, "GetSubFoldersDialog.Preview.Button" ) );
    wPreview.addListener( SWT.Selection, e -> preview() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    setButtonPositions( new Button[] { wOk, wPreview, wCancel }, margin, null);

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "System.Label.TransformName" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.top = new FormAttachment( 0, margin);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    wlTransformName.setLayoutData( fdlTransformName );
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setText( transformName );
    props.setLook( wTransformName );
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0 );
    fdTransformName.top = new FormAttachment( 0, margin);
    fdTransformName.right = new FormAttachment( 100, 0 );
    wTransformName.setLayoutData( fdTransformName );

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF FILE TAB ///
    // ////////////////////////
    CTabItem wFolderTab = new CTabItem(wTabFolder, SWT.NONE);
    wFolderTab.setText( BaseMessages.getString( PKG, "GetSubFoldersDialog.FolderTab.TabTitle" ) );

    Composite wFolderComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wFolderComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wFolderComp.setLayout( fileLayout );

    // ///////////////////////////////
    // START OF Origin files GROUP //
    // ///////////////////////////////

    Group wOriginFolders = new Group(wFolderComp, SWT.SHADOW_NONE);
    props.setLook(wOriginFolders);
    wOriginFolders.setText( BaseMessages.getString( PKG, "GetSubFoldersDialog.wOriginFiles.Label" ) );

    FormLayout originFilesgroupLayout = new FormLayout();
    originFilesgroupLayout.marginWidth = 10;
    originFilesgroupLayout.marginHeight = 10;
    wOriginFolders.setLayout( originFilesgroupLayout );

    // Is Filename defined in a Field
    Label wlFileField = new Label(wOriginFolders, SWT.RIGHT);
    wlFileField.setText( BaseMessages.getString( PKG, "GetSubFoldersDialog.FolderField.Label" ) );
    props.setLook(wlFileField);
    FormData fdlFileField = new FormData();
    fdlFileField.left = new FormAttachment( 0, -margin);
    fdlFileField.top = new FormAttachment( 0, margin);
    fdlFileField.right = new FormAttachment(middle, -2 * margin);
    wlFileField.setLayoutData(fdlFileField);

    wFolderField = new Button(wOriginFolders, SWT.CHECK );
    props.setLook( wFolderField );
    wFolderField.setToolTipText( BaseMessages.getString( PKG, "GetSubFoldersDialog.FileField.Tooltip" ) );
    FormData fdFileField = new FormData();
    fdFileField.left = new FormAttachment(middle, -margin);
    fdFileField.top = new FormAttachment( wlFileField, 0, SWT.CENTER);
    wFolderField.setLayoutData(fdFileField);
    SelectionAdapter lsFileField = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        activateFileField();
        input.setChanged();
      }
    };
    wFolderField.addSelectionListener( lsFileField );

    // Filename field
    wlFilenameField = new Label(wOriginFolders, SWT.RIGHT );
    wlFilenameField.setText( BaseMessages.getString( PKG, "GetSubFoldersDialog.wlFilenameField.Label" ) );
    props.setLook( wlFilenameField );
    FormData fdlFoldernameField = new FormData();
    fdlFoldernameField.left = new FormAttachment( 0, -margin);
    fdlFoldernameField.top = new FormAttachment( wFolderField, margin);
    fdlFoldernameField.right = new FormAttachment(middle, -2 * margin);
    wlFilenameField.setLayoutData(fdlFoldernameField);

    wFoldernameField = new ComboVar( variables, wOriginFolders, SWT.BORDER | SWT.READ_ONLY );
    wFoldernameField.setEditable( true );
    props.setLook( wFoldernameField );
    wFoldernameField.addModifyListener(lsMod);
    FormData fdFoldernameField = new FormData();
    fdFoldernameField.left = new FormAttachment(middle, -margin);
    fdFoldernameField.top = new FormAttachment( wFolderField, margin);
    fdFoldernameField.right = new FormAttachment( 100, -margin);
    wFoldernameField.setLayoutData(fdFoldernameField);
    wFoldernameField.setEnabled( false );
    wFoldernameField.addFocusListener( new FocusListener() {
      public void focusLost( FocusEvent e ) {
      }

      public void focusGained( FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        BaseTransformDialog.getFieldsFromPrevious( wFoldernameField, pipelineMeta, transformMeta );
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    FormData fdOriginFolders = new FormData();
    fdOriginFolders.left = new FormAttachment( 0, margin);
    fdOriginFolders.top = new FormAttachment( wFoldernameList, margin);
    fdOriginFolders.right = new FormAttachment( 100, -margin);
    wOriginFolders.setLayoutData(fdOriginFolders);

    // ///////////////////////////////////////////////////////////
    // / END OF Origin files GROUP
    // ///////////////////////////////////////////////////////////

    // Foldername line
    wlFoldername = new Label(wFolderComp, SWT.RIGHT );
    wlFoldername.setText( BaseMessages.getString( PKG, "GetSubFoldersDialog.Filename.Label" ) );
    props.setLook( wlFoldername );
    FormData fdlFoldername = new FormData();
    fdlFoldername.left = new FormAttachment( 0, 0 );
    fdlFoldername.top = new FormAttachment(wOriginFolders, margin);
    fdlFoldername.right = new FormAttachment(middle, -margin);
    wlFoldername.setLayoutData(fdlFoldername);

    wbbFoldername = new Button(wFolderComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbbFoldername );
    wbbFoldername.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    wbbFoldername.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.BrowseForFileOrDirAndAdd" ) );
    FormData fdbFoldername = new FormData();
    fdbFoldername.right = new FormAttachment( 100, 0 );
    fdbFoldername.top = new FormAttachment(wOriginFolders, margin);
    wbbFoldername.setLayoutData(fdbFoldername);

    wbaFoldername = new Button(wFolderComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbaFoldername );
    wbaFoldername.setText( BaseMessages.getString( PKG, "GetSubFoldersDialog.FoldernameAdd.Button" ) );
    wbaFoldername.setToolTipText( BaseMessages.getString( PKG, "GetSubFoldersDialog.FoldernameAdd.Tooltip" ) );
    FormData fdbaFoldername = new FormData();
    fdbaFoldername.right = new FormAttachment( wbbFoldername, -margin);
    fdbaFoldername.top = new FormAttachment(wOriginFolders, margin);
    wbaFoldername.setLayoutData(fdbaFoldername);

    wFoldername = new TextVar( variables, wFolderComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFoldername );
    wFoldername.addModifyListener(lsMod);
    FormData fdFoldername = new FormData();
    fdFoldername.left = new FormAttachment(middle, 0 );
    fdFoldername.right = new FormAttachment( wbaFoldername, -margin);
    fdFoldername.top = new FormAttachment(wOriginFolders, margin);
    wFoldername.setLayoutData(fdFoldername);

    // Filename list line
    wlFoldernameList = new Label(wFolderComp, SWT.RIGHT );
    wlFoldernameList.setText( BaseMessages.getString( PKG, "GetSubFoldersDialog.FoldernameList.Label" ) );
    props.setLook( wlFoldernameList );
    FormData fdlFoldernameList = new FormData();
    fdlFoldernameList.left = new FormAttachment( 0, 0 );
    fdlFoldernameList.top = new FormAttachment( wFoldername, margin);
    fdlFoldernameList.right = new FormAttachment(middle, -margin);
    wlFoldernameList.setLayoutData(fdlFoldernameList);

    // Buttons to the right of the screen...
    wbdFoldername = new Button(wFolderComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbdFoldername );
    wbdFoldername.setText( BaseMessages.getString( PKG, "GetSubFoldersDialog.FoldernameDelete.Button" ) );
    wbdFoldername.setToolTipText( BaseMessages.getString( PKG, "GetSubFoldersDialog.FoldernameDelete.Tooltip" ) );
    FormData fdbdFoldername = new FormData();
    fdbdFoldername.right = new FormAttachment( 100, 0 );
    fdbdFoldername.top = new FormAttachment( wFoldername, 40 );
    wbdFoldername.setLayoutData(fdbdFoldername);

    wbeFoldername = new Button(wFolderComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbeFoldername );
    wbeFoldername.setText( BaseMessages.getString( PKG, "GetSubFoldersDialog.FilenameEdit.Button" ) );
    wbeFoldername.setToolTipText( BaseMessages.getString( PKG, "GetSubFoldersDialog.FilenameEdit.Tooltip" ) );
    FormData fdbeFoldername = new FormData();
    fdbeFoldername.right = new FormAttachment( 100, 0 );
    fdbeFoldername.left = new FormAttachment( wbdFoldername, 0, SWT.LEFT );
    fdbeFoldername.top = new FormAttachment( wbdFoldername, margin);
    wbeFoldername.setLayoutData(fdbeFoldername);

    ColumnInfo[] colinfo = new ColumnInfo[ 2 ];
    colinfo[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "GetSubFoldersDialog.FileDirColumn.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinfo[ 0 ].setUsingVariables( true );
    colinfo[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "GetSubFoldersDialog.Required.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        GetSubFoldersMeta.RequiredFoldersDesc );
    colinfo[ 1 ].setToolTip( BaseMessages.getString( PKG, "GetSubFoldersDialog.Required.Tooltip" ) );

    wFoldernameList =
      new TableView(
        variables, wFolderComp, SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER, colinfo, colinfo.length, lsMod,
        props );
    props.setLook( wFoldernameList );
    FormData fdFoldernameList = new FormData();
    fdFoldernameList.left = new FormAttachment(middle, 0 );
    fdFoldernameList.right = new FormAttachment( wbdFoldername, -margin);
    fdFoldernameList.top = new FormAttachment( wFoldername, margin);
    fdFoldernameList.bottom = new FormAttachment( 100, -margin);
    wFoldernameList.setLayoutData(fdFoldernameList);

    FormData fdFolderComp = new FormData();
    fdFolderComp.left = new FormAttachment( 0, 0 );
    fdFolderComp.top = new FormAttachment( 0, 0 );
    fdFolderComp.right = new FormAttachment( 100, 0 );
    fdFolderComp.bottom = new FormAttachment( 100, 0 );
    wFolderComp.setLayoutData(fdFolderComp);

    wFolderComp.layout();
    wFolderTab.setControl(wFolderComp);

    // ///////////////////////////////////////////////////////////
    // / END OF FILE TAB
    // ///////////////////////////////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wTransformName, margin);
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOk, -2*margin );
    wTabFolder.setLayoutData(fdTabFolder);

    // ////////////////////////
    // START OF Filter TAB ///
    // ////////////////////////
    CTabItem wSettingsTab = new CTabItem(wTabFolder, SWT.NONE);
    wSettingsTab.setText( BaseMessages.getString( PKG, "GetSubFoldersDialog.SettingsTab.TabTitle" ) );

    Composite wSettingsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wSettingsComp);

    FormLayout filesettingLayout = new FormLayout();
    filesettingLayout.marginWidth = 3;
    filesettingLayout.marginHeight = 3;
    wSettingsComp.setLayout( fileLayout );

    // /////////////////////////////////
    // START OF Additional Fields GROUP
    // /////////////////////////////////

    Group wAdditionalGroup = new Group(wSettingsComp, SWT.SHADOW_NONE);
    props.setLook(wAdditionalGroup);
    wAdditionalGroup.setText( BaseMessages.getString( PKG, "GetSubFoldersDialog.Group.AdditionalGroup.Label" ) );

    FormLayout additionalgroupLayout = new FormLayout();
    additionalgroupLayout.marginWidth = 10;
    additionalgroupLayout.marginHeight = 10;
    wAdditionalGroup.setLayout( additionalgroupLayout );

    Label wlInclRownum = new Label(wAdditionalGroup, SWT.RIGHT);
    wlInclRownum.setText( BaseMessages.getString( PKG, "GetSubFoldersDialog.InclRownum.Label" ) );
    props.setLook(wlInclRownum);
    FormData fdlInclRownum = new FormData();
    fdlInclRownum.left = new FormAttachment( 0, 0 );
    fdlInclRownum.top = new FormAttachment( 0, 2 * margin);
    fdlInclRownum.right = new FormAttachment(middle, -margin);
    wlInclRownum.setLayoutData(fdlInclRownum);
    wInclRownum = new Button(wAdditionalGroup, SWT.CHECK );
    props.setLook( wInclRownum );
    wInclRownum.setToolTipText( BaseMessages.getString( PKG, "GetSubFoldersDialog.InclRownum.Tooltip" ) );
    FormData fdRownum = new FormData();
    fdRownum.left = new FormAttachment(middle, 0 );
    fdRownum.top = new FormAttachment( wlInclRownum, 0, SWT.CENTER);
    wInclRownum.setLayoutData(fdRownum);
    SelectionAdapter linclRownum = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        ActiveIncludeRowNum();
        input.setChanged();
      }
    };
    wInclRownum.addSelectionListener( linclRownum );

    wlInclRownumField = new Label(wAdditionalGroup, SWT.RIGHT );
    wlInclRownumField.setText( BaseMessages.getString( PKG, "GetSubFoldersDialog.InclRownumField.Label" ) );
    props.setLook( wlInclRownumField );
    FormData fdlInclRownumField = new FormData();
    fdlInclRownumField.left = new FormAttachment( wInclRownum, margin);
    fdlInclRownumField.top = new FormAttachment( 0, 2 * margin);
    wlInclRownumField.setLayoutData(fdlInclRownumField);
    wInclRownumField = new TextVar( variables, wAdditionalGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclRownumField );
    wInclRownumField.addModifyListener(lsMod);
    FormData fdInclRownumField = new FormData();
    fdInclRownumField.left = new FormAttachment( wlInclRownumField, margin);
    fdInclRownumField.top = new FormAttachment( 0, 2 * margin);
    fdInclRownumField.right = new FormAttachment( 100, 0 );
    wInclRownumField.setLayoutData(fdInclRownumField);

    FormData fdAdditionalGroup = new FormData();
    fdAdditionalGroup.left = new FormAttachment( 0, margin);
    fdAdditionalGroup.top = new FormAttachment( 0, margin);
    fdAdditionalGroup.right = new FormAttachment( 100, -margin);
    wAdditionalGroup.setLayoutData(fdAdditionalGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF DESTINATION ADDRESS GROUP
    // ///////////////////////////////////////////////////////////

    wlLimit = new Label(wSettingsComp, SWT.RIGHT );
    wlLimit.setText( BaseMessages.getString( PKG, "GetSubFoldersDialog.Limit.Label" ) );
    props.setLook( wlLimit );
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment( 0, 0 );
    fdlLimit.top = new FormAttachment(wAdditionalGroup, 2 * margin);
    fdlLimit.right = new FormAttachment(middle, -margin);
    wlLimit.setLayoutData(fdlLimit);
    wLimit = new Text(wSettingsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLimit );
    wLimit.addModifyListener(lsMod);
    FormData fdLimit = new FormData();
    fdLimit.left = new FormAttachment(middle, 0 );
    fdLimit.top = new FormAttachment(wAdditionalGroup, 2 * margin);
    fdLimit.right = new FormAttachment( 100, 0 );
    wLimit.setLayoutData(fdLimit);

    FormData fdSettingsComp = new FormData();
    fdSettingsComp.left = new FormAttachment( 0, 0 );
    fdSettingsComp.top = new FormAttachment( 0, 0 );
    fdSettingsComp.right = new FormAttachment( 100, 0 );
    fdSettingsComp.bottom = new FormAttachment( 100, 0 );
    wSettingsComp.setLayoutData(fdSettingsComp);

    wSettingsComp.layout();
    wSettingsTab.setControl(wSettingsComp);

    // ///////////////////////////////////////////////////////////
    // / END OF FILE Filter TAB
    // ///////////////////////////////////////////////////////////



    // Add listeners

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );

    // Add the file to the list of files...
    SelectionAdapter selA = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        wFoldernameList.add( new String[] { wFoldername.getText() } );
        wFoldername.setText( "" );
        wFoldernameList.removeEmptyRows();
        wFoldernameList.setRowNums();
        wFoldernameList.optWidth( true );
      }
    };
    wbaFoldername.addSelectionListener( selA );
    wFoldername.addSelectionListener( selA );

    // Delete files from the list of files...
    wbdFoldername.addListener( SWT.Selection, e-> {
        int[] idx = wFoldernameList.getSelectionIndices();
        wFoldernameList.remove( idx );
        wFoldernameList.removeEmptyRows();
        wFoldernameList.setRowNums();
      } );

    // Edit the selected file & remove from the list...
    wbeFoldername.addListener(SWT.Selection, e-> {
        int idx = wFoldernameList.getSelectionIndex();
        if ( idx >= 0 ) {
          String[] string = wFoldernameList.getItem( idx );
          wFoldername.setText( string[ 0 ] );
          wFoldernameList.remove( idx );
        }
        wFoldernameList.removeEmptyRows();
        wFoldernameList.setRowNums();
      } );

    wbbFoldername.addListener( SWT.Selection, e-> BaseDialog.presentDirectoryDialog( shell, wFoldername, variables ) );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    wTabFolder.setSelection( 0 );

    // Set the shell size, based upon previous time...
    getData( input );
    activateFileField();
    ActiveIncludeRowNum();
    setSize();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void ActiveIncludeRowNum() {
    wlInclRownumField.setEnabled( wInclRownum.getSelection() );
    wInclRownumField.setEnabled( wInclRownum.getSelection() );
  }

  private void activateFileField() {
    if ( wFolderField.getSelection() ) {
      wLimit.setText( "0" );
    }
    wlFilenameField.setEnabled( wFolderField.getSelection() );
    wFoldernameField.setEnabled( wFolderField.getSelection() );

    wlFoldername.setEnabled( !wFolderField.getSelection() );
    wbbFoldername.setEnabled( !wFolderField.getSelection() );
    wbaFoldername.setEnabled( !wFolderField.getSelection() );
    wFoldername.setEnabled( !wFolderField.getSelection() );
    wlFoldernameList.setEnabled( !wFolderField.getSelection() );
    wbdFoldername.setEnabled( !wFolderField.getSelection() );
    wbeFoldername.setEnabled( !wFolderField.getSelection() );
    wlFoldernameList.setEnabled( !wFolderField.getSelection() );
    wFoldernameList.setEnabled( !wFolderField.getSelection() );
    wPreview.setEnabled( !wFolderField.getSelection() );
    wlLimit.setEnabled( !wFolderField.getSelection() );
    wLimit.setEnabled( !wFolderField.getSelection() );

  }

  /**
   * Read the data from the TextFileInputMeta object and show it in this dialog.
   *
   * @param meta The TextFileInputMeta object to obtain the data from.
   */
  public void getData( GetSubFoldersMeta meta ) {
    final GetSubFoldersMeta in = meta;

    if ( in.getFolderName() != null ) {
      wFoldernameList.removeAll();
      for ( int i = 0; i < in.getFolderName().length; i++ ) {
        wFoldernameList.add( new String[] {
          in.getFolderName()[ i ], in.getRequiredFilesDesc( in.getFolderRequired()[ i ] ) } );

      }
      wFoldernameList.removeEmptyRows();
      wFoldernameList.setRowNums();
      wFoldernameList.optWidth( true );

      wInclRownum.setSelection( in.includeRowNumber() );
      wFolderField.setSelection( in.isFoldernameDynamic() );
      if ( in.getRowNumberField() != null ) {
        wInclRownumField.setText( in.getRowNumberField() );
      }
      if ( in.getDynamicFoldernameField() != null ) {
        wFoldernameField.setText( in.getDynamicFoldernameField() );
      }
      wLimit.setText( "" + in.getRowLimit() );

    }

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }
    getInfo( input );
    dispose();
  }

  private void getInfo( GetSubFoldersMeta in ) {
    transformName = wTransformName.getText(); // return value

    int nrfiles = wFoldernameList.getItemCount();
    in.allocate( nrfiles );

    in.setFolderName( wFoldernameList.getItems( 0 ) );
    in.setFolderRequired( wFoldernameList.getItems( 1 ) );

    in.setIncludeRowNumber( wInclRownum.getSelection() );
    in.setDynamicFoldernameField( wFoldernameField.getText() );
    in.setFolderField( wFolderField.getSelection() );
    in.setRowNumberField( wInclRownumField.getText() );
    in.setRowLimit( Const.toLong( wLimit.getText(), 0L ) );

  }

  // Preview the data
  private void preview() {
    // Create the XML input transform
    GetSubFoldersMeta oneMeta = new GetSubFoldersMeta();
    getInfo( oneMeta );

    PipelineMeta previewMeta = PipelinePreviewFactory.generatePreviewPipeline( variables, pipelineMeta.getMetadataProvider(),
      oneMeta, wTransformName.getText() );

    EnterNumberDialog numberDialog = new EnterNumberDialog( shell, props.getDefaultPreviewSize(),
      BaseMessages.getString( PKG, "GetSubFoldersDialog.PreviewSize.DialogTitle" ),
      BaseMessages.getString( PKG, "GetSubFoldersDialog.PreviewSize.DialogMessage" ) );
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
              .getString( PKG, "GetSubFoldersDialog.ErrorInPreview.DialogMessage" ), loggingText, true );
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
