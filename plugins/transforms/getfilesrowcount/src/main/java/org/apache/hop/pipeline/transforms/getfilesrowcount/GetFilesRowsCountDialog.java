/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.transforms.getfilesrowcount;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.*;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class GetFilesRowsCountDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = GetFilesRowsCountMeta.class; // Needed by Translator

  private CTabFolder wTabFolder;
  private FormData fdTabFolder;

  private CTabItem wFileTab, wContentTab;

  private Composite wFileComp, wContentComp;
  private FormData fdFileComp, fdContentComp;

  private Label wlExcludeFilemask;
  private TextVar wExcludeFilemask;
  private FormData fdlExcludeFilemask, fdExcludeFilemask;

  private Label wlFilename;
  private Button wbbFilename; // Browse: add file or directory

  private Button wbdFilename; // Delete
  private Button wbeFilename; // Edit
  private Button wbaFilename; // Add or change
  private TextVar wFilename;
  private FormData fdlFilename, fdbFilename, fdbdFilename, fdbeFilename, fdbaFilename, fdFilename;

  private Label wlFilenameList;
  private TableView wFilenameList;
  private FormData fdlFilenameList, fdFilenameList;

  private Label wlFilemask;
  private TextVar wFilemask;
  private FormData fdlFilemask, fdFilemask;

  private Button wbShowFiles;
  private FormData fdbShowFiles;

  private Label wlInclFilesCount;
  private Button wInclFilesCount;
  private FormData fdlInclFilesCount, fdFilesCount;

  private Label wlInclFilesCountField;
  private TextVar wInclFilesCountField;
  private FormData fdlInclFilesCountField, fdInclFilesCountField;

  private Label wlRowsCountField;
  private TextVar wRowsCountField;
  private FormData fdlRowsCountField, fdRowsCountField;

  private Label wlRowSeparator;
  private TextVar wRowSeparator;
  private FormData fdlRowSeparator, fdRowSeparator;

  private Label wlRowSeparatorFormat;
  private CCombo wRowSeparatorFormat;
  private FormData fdlRowSeparatorFormat, fdRowSeparatorFormat, fdlAddResult;

  private GetFilesRowsCountMeta input;

  private Group wAdditionalGroup, wFilesCountFieldGroup, wRowSeparatorGroup, wOriginFiles, wAddFileResult;
  private FormData fdAdditionalGroup, fdFilesCountFieldGroup, fdRowSeparatorGroup, fdAddFileResult;

  private FormData fdOriginFiles, fdFilenameField, fdlFilenameField, fdAddResult, fdSmartCount;
  private Button wFileField, wAddResult, wSmartCount;

  private Label wlFileField, wlFilenameField, wlAddResult, wlSmartCount;
  private CCombo wFilenameField;
  private FormData fdlFileField, fdFileField, fdlSmartCount;

  public GetFilesRowsCountDialog( Shell parent, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (GetFilesRowsCountMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Buttons go at the bottom
    //
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wPreview = new Button( shell, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.Button.PreviewRows" ) );
    wPreview.addListener( SWT.Selection, e -> preview() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    setButtonPositions( new Button[] { wOk, wPreview, wCancel }, margin, null );

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "System.Label.TransformName" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.top = new FormAttachment( 0, margin );
    fdlTransformName.right = new FormAttachment( middle, -margin );
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

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF FILE TAB ///
    // ////////////////////////
    wFileTab = new CTabItem( wTabFolder, SWT.NONE );
    wFileTab.setText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.File.Tab" ) );

    wFileComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wFileComp );

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wFileComp.setLayout( fileLayout );

    // ///////////////////////////////
    // START OF Origin files GROUP //
    // ///////////////////////////////

    wOriginFiles = new Group( wFileComp, SWT.SHADOW_NONE );
    props.setLook( wOriginFiles );
    wOriginFiles.setText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.wOriginFiles.Label" ) );

    FormLayout OriginFilesgroupLayout = new FormLayout();
    OriginFilesgroupLayout.marginWidth = 10;
    OriginFilesgroupLayout.marginHeight = 10;
    wOriginFiles.setLayout( OriginFilesgroupLayout );

    // Is Filename defined in a Field
    wlFileField = new Label( wOriginFiles, SWT.RIGHT );
    wlFileField.setText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.FileField.Label" ) );
    props.setLook( wlFileField );
    fdlFileField = new FormData();
    fdlFileField.left = new FormAttachment( 0, -margin );
    fdlFileField.top = new FormAttachment( 0, margin );
    fdlFileField.right = new FormAttachment( middle, -2 * margin );
    wlFileField.setLayoutData( fdlFileField );

    wFileField = new Button( wOriginFiles, SWT.CHECK );
    props.setLook( wFileField );
    wFileField.setToolTipText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.FileField.Tooltip" ) );
    fdFileField = new FormData();
    fdFileField.left = new FormAttachment( middle, -margin );
    fdFileField.top = new FormAttachment( wlFileField, 0, SWT.CENTER );
    wFileField.setLayoutData( fdFileField );
    SelectionAdapter lsFileField = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        activateFileField();
        input.setChanged();
      }
    };
    wFileField.addSelectionListener( lsFileField );

    // Filename field
    wlFilenameField = new Label( wOriginFiles, SWT.RIGHT );
    wlFilenameField.setText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.FilenameField.Label" ) );
    props.setLook( wlFilenameField );
    fdlFilenameField = new FormData();
    fdlFilenameField.left = new FormAttachment( 0, -margin );
    fdlFilenameField.top = new FormAttachment( wFileField, margin );
    fdlFilenameField.right = new FormAttachment( middle, -2 * margin );
    wlFilenameField.setLayoutData( fdlFilenameField );

    wFilenameField = new CCombo( wOriginFiles, SWT.BORDER | SWT.READ_ONLY );
    wFilenameField.setEditable( true );
    props.setLook( wFilenameField );
    wFilenameField.addModifyListener( lsMod );
    fdFilenameField = new FormData();
    fdFilenameField.left = new FormAttachment( middle, -margin );
    fdFilenameField.top = new FormAttachment( wFileField, margin );
    fdFilenameField.right = new FormAttachment( 100, -margin );
    wFilenameField.setLayoutData( fdFilenameField );
    wFilenameField.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        setFileField();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    fdOriginFiles = new FormData();
    fdOriginFiles.left = new FormAttachment( 0, margin );
    fdOriginFiles.top = new FormAttachment( wFilenameList, margin );
    fdOriginFiles.right = new FormAttachment( 100, -margin );
    wOriginFiles.setLayoutData( fdOriginFiles );

    // ///////////////////////////////////////////////////////////
    // / END OF Origin files GROUP
    // ///////////////////////////////////////////////////////////

    // Filename line
    wlFilename = new Label( wFileComp, SWT.RIGHT );
    wlFilename.setText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.Filename.Label" ) );
    props.setLook( wlFilename );
    fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment( wOriginFiles, margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData( fdlFilename );

    wbbFilename = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbbFilename );
    wbbFilename.setText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.FilenameBrowse.Button" ) );
    wbbFilename.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.BrowseForFileOrDirAndAdd" ) );
    fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment( wOriginFiles, margin );
    wbbFilename.setLayoutData( fdbFilename );

    wbaFilename = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbaFilename );
    wbaFilename.setText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.FilenameAdd.Button" ) );
    wbaFilename.setToolTipText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.FilenameAdd.Tooltip" ) );
    fdbaFilename = new FormData();
    fdbaFilename.right = new FormAttachment( wbbFilename, -margin );
    fdbaFilename.top = new FormAttachment( wOriginFiles, margin );
    wbaFilename.setLayoutData( fdbaFilename );

    wFilename = new TextVar( pipelineMeta, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.right = new FormAttachment( wbaFilename, -margin );
    fdFilename.top = new FormAttachment( wOriginFiles, margin );
    wFilename.setLayoutData( fdFilename );

    wlFilemask = new Label( wFileComp, SWT.RIGHT );
    wlFilemask.setText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.RegExp.Label" ) );
    props.setLook( wlFilemask );
    fdlFilemask = new FormData();
    fdlFilemask.left = new FormAttachment( 0, 0 );
    fdlFilemask.top = new FormAttachment( wFilename, margin );
    fdlFilemask.right = new FormAttachment( middle, -margin );
    wlFilemask.setLayoutData( fdlFilemask );
    wFilemask = new TextVar( pipelineMeta, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilemask );
    wFilemask.addModifyListener( lsMod );
    fdFilemask = new FormData();
    fdFilemask.left = new FormAttachment( middle, 0 );
    fdFilemask.top = new FormAttachment( wFilename, margin );
    fdFilemask.right = new FormAttachment( 100, 0 );
    wFilemask.setLayoutData( fdFilemask );

    wlExcludeFilemask = new Label( wFileComp, SWT.RIGHT );
    wlExcludeFilemask.setText( BaseMessages.getString( PKG, "GetFilesRowsDialog.ExcludeFilemask.Label" ) );
    props.setLook( wlExcludeFilemask );
    fdlExcludeFilemask = new FormData();
    fdlExcludeFilemask.left = new FormAttachment( 0, 0 );
    fdlExcludeFilemask.top = new FormAttachment( wFilemask, margin );
    fdlExcludeFilemask.right = new FormAttachment( middle, -margin );
    wlExcludeFilemask.setLayoutData( fdlExcludeFilemask );
    wExcludeFilemask = new TextVar( pipelineMeta, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wExcludeFilemask );
    wExcludeFilemask.addModifyListener( lsMod );
    fdExcludeFilemask = new FormData();
    fdExcludeFilemask.left = new FormAttachment( middle, 0 );
    fdExcludeFilemask.top = new FormAttachment( wFilemask, margin );
    fdExcludeFilemask.right = new FormAttachment( wFilename, 0, SWT.RIGHT );
    wExcludeFilemask.setLayoutData( fdExcludeFilemask );

    // Filename list line
    wlFilenameList = new Label( wFileComp, SWT.RIGHT );
    wlFilenameList.setText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.FilenameList.Label" ) );
    props.setLook( wlFilenameList );
    fdlFilenameList = new FormData();
    fdlFilenameList.left = new FormAttachment( 0, 0 );
    fdlFilenameList.top = new FormAttachment( wExcludeFilemask, margin );
    fdlFilenameList.right = new FormAttachment( middle, -margin );
    wlFilenameList.setLayoutData( fdlFilenameList );

    // Buttons to the right of the screen...
    wbdFilename = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbdFilename );
    wbdFilename.setText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.FilenameRemove.Button" ) );
    wbdFilename.setToolTipText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.FilenameRemove.Tooltip" ) );
    fdbdFilename = new FormData();
    fdbdFilename.right = new FormAttachment( 100, 0 );
    fdbdFilename.top = new FormAttachment( wExcludeFilemask, 40 );
    wbdFilename.setLayoutData( fdbdFilename );

    wbeFilename = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbeFilename );
    wbeFilename.setText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.FilenameEdit.Button" ) );
    wbeFilename.setToolTipText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.FilenameEdit.Tooltip" ) );
    fdbeFilename = new FormData();
    fdbeFilename.right = new FormAttachment( 100, 0 );
    fdbeFilename.top = new FormAttachment( wbdFilename, margin );
    wbeFilename.setLayoutData( fdbeFilename );

    wbShowFiles = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbShowFiles );
    wbShowFiles.setText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.ShowFiles.Button" ) );
    fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment( middle, 0 );
    fdbShowFiles.bottom = new FormAttachment( 100, 0 );
    wbShowFiles.setLayoutData( fdbShowFiles );

    ColumnInfo[] colinfo = new ColumnInfo[ 5 ];
    colinfo[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "GetFilesRowsCountDialog.Files.Filename.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinfo[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "GetFilesRowsCountDialog.Files.Wildcard.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinfo[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "GetFilesRowsDialog.Files.ExcludeWildcard.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );

    colinfo[ 3 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "GetFilesRowsCountDialog.Required.Column" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, GetFilesRowsCountMeta.RequiredFilesDesc );
    colinfo[ 4 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "GetFilesRowsCountDialog.IncludeSubDirs.Column" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, GetFilesRowsCountMeta.RequiredFilesDesc );

    colinfo[ 0 ].setUsingVariables( true );
    colinfo[ 1 ].setUsingVariables( true );
    colinfo[ 1 ].setToolTip( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.Files.Wildcard.Tooltip" ) );
    colinfo[ 2 ].setUsingVariables( true );
    colinfo[ 2 ].setToolTip( BaseMessages.getString( PKG, "GetFilesRowsDialog.Files.ExcludeWildcard.Tooltip" ) );

    wFilenameList =
      new TableView(
        pipelineMeta, wFileComp, SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER, colinfo, 2, lsMod, props );
    props.setLook( wFilenameList );

    fdFilenameList = new FormData();
    fdFilenameList.left = new FormAttachment( middle, 0 );
    fdFilenameList.right = new FormAttachment( wbdFilename, -margin );
    fdFilenameList.top = new FormAttachment( wExcludeFilemask, margin );
    fdFilenameList.bottom = new FormAttachment( wbShowFiles, -margin );
    wFilenameList.setLayoutData( fdFilenameList );

    fdFileComp = new FormData();
    fdFileComp.left = new FormAttachment( 0, 0 );
    fdFileComp.top = new FormAttachment( 0, 0 );
    fdFileComp.right = new FormAttachment( 100, 0 );
    fdFileComp.bottom = new FormAttachment( 100, 0 );
    wFileComp.setLayoutData( fdFileComp );

    wFileComp.layout();
    wFileTab.setControl( wFileComp );

    // ///////////////////////////////////////////////////////////
    // / END OF FILE TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF CONTENT TAB///
    // /
    wContentTab = new CTabItem( wTabFolder, SWT.NONE );
    wContentTab.setText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.Content.Tab" ) );

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    wContentComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wContentComp );
    wContentComp.setLayout( contentLayout );

    // /////////////////////////////////
    // START OF Files Count Field GROUP
    // /////////////////////////////////

    wFilesCountFieldGroup = new Group( wContentComp, SWT.SHADOW_NONE );
    props.setLook( wFilesCountFieldGroup );
    wFilesCountFieldGroup.setText( BaseMessages.getString(
      PKG, "GetFilesRowsCountDialog.Group.CountFilesFieldGroup.Label" ) );

    FormLayout countfilesfieldgroupLayout = new FormLayout();
    countfilesfieldgroupLayout.marginWidth = 10;
    countfilesfieldgroupLayout.marginHeight = 10;
    wFilesCountFieldGroup.setLayout( countfilesfieldgroupLayout );

    wlRowsCountField = new Label( wFilesCountFieldGroup, SWT.RIGHT );
    wlRowsCountField.setText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.RowsCountField.Label" ) );
    props.setLook( wlRowsCountField );
    fdlRowsCountField = new FormData();
    fdlRowsCountField.left = new FormAttachment( wInclFilesCount, margin );
    fdlRowsCountField.top = new FormAttachment( 0, margin );
    wlRowsCountField.setLayoutData( fdlRowsCountField );
    wRowsCountField = new TextVar( pipelineMeta, wFilesCountFieldGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wRowsCountField );
    wRowsCountField
      .setToolTipText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.RowsCountField.Tooltip" ) );
    wRowsCountField.addModifyListener( lsMod );
    fdRowsCountField = new FormData();
    fdRowsCountField.left = new FormAttachment( wlRowsCountField, margin );
    fdRowsCountField.top = new FormAttachment( 0, margin );
    fdRowsCountField.right = new FormAttachment( 100, 0 );
    wRowsCountField.setLayoutData( fdRowsCountField );

    fdFilesCountFieldGroup = new FormData();
    fdFilesCountFieldGroup.left = new FormAttachment( 0, margin );
    fdFilesCountFieldGroup.top = new FormAttachment( 0, margin );
    fdFilesCountFieldGroup.right = new FormAttachment( 100, -margin );
    wFilesCountFieldGroup.setLayoutData( fdFilesCountFieldGroup );

    // ///////////////////////////////////////////////////////////
    // / END OF ADDITIONNAL FIELDS GROUP
    // ///////////////////////////////////////////////////////////

    // /////////////////////////////////
    // START OF Row separator GROUP
    // /////////////////////////////////

    wRowSeparatorGroup = new Group( wContentComp, SWT.SHADOW_NONE );
    props.setLook( wRowSeparatorGroup );
    wRowSeparatorGroup.setText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.Group.RowSeparator.Label" ) );

    FormLayout rowseparatorgroupLayout = new FormLayout();
    rowseparatorgroupLayout.marginWidth = 10;
    rowseparatorgroupLayout.marginHeight = 10;
    wRowSeparatorGroup.setLayout( rowseparatorgroupLayout );

    wlRowSeparatorFormat = new Label( wRowSeparatorGroup, SWT.RIGHT );
    wlRowSeparatorFormat
      .setText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.RowSeparatorFormat.Label" ) );
    props.setLook( wlRowSeparatorFormat );
    fdlRowSeparatorFormat = new FormData();
    fdlRowSeparatorFormat.left = new FormAttachment( 0, 0 );
    fdlRowSeparatorFormat.top = new FormAttachment( wFilesCountFieldGroup, margin );
    fdlRowSeparatorFormat.right = new FormAttachment( middle, -margin );
    wlRowSeparatorFormat.setLayoutData( fdlRowSeparatorFormat );
    wRowSeparatorFormat = new CCombo( wRowSeparatorGroup, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wRowSeparatorFormat );
    wRowSeparatorFormat.add( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.RowSeparatorFormat.CR.Label" ) );
    wRowSeparatorFormat.add( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.RowSeparatorFormat.LF.Label" ) );
    wRowSeparatorFormat
      .add( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.RowSeparatorFormat.CRLF.Label" ) );
    wRowSeparatorFormat
      .add( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.RowSeparatorFormat.TAB.Label" ) );
    wRowSeparatorFormat.add( BaseMessages.getString(
      PKG, "GetFilesRowsCountDialog.RowSeparatorFormat.CUSTOM.Label" ) );
    wRowSeparatorFormat.select( 0 );
    wRowSeparatorFormat.addModifyListener( lsMod );
    fdRowSeparatorFormat = new FormData();
    fdRowSeparatorFormat.left = new FormAttachment( middle, 0 );
    fdRowSeparatorFormat.top = new FormAttachment( wFilesCountFieldGroup, margin );
    fdRowSeparatorFormat.right = new FormAttachment( 100, 0 );
    wRowSeparatorFormat.setLayoutData( fdRowSeparatorFormat );

    wRowSeparatorFormat.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        ActiveRowSeparator();

      }
    } );

    wlRowSeparator = new Label( wRowSeparatorGroup, SWT.RIGHT );
    wlRowSeparator.setText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.RowSeparator.Label" ) );
    props.setLook( wlRowSeparator );
    fdlRowSeparator = new FormData();
    fdlRowSeparator.left = new FormAttachment( wInclFilesCount, margin );
    fdlRowSeparator.top = new FormAttachment( wRowSeparatorFormat, margin );
    wlRowSeparator.setLayoutData( fdlRowSeparator );
    wRowSeparator = new TextVar( pipelineMeta, wRowSeparatorGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wRowSeparator );
    wRowSeparator.setToolTipText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.RowSeparator.Tooltip" ) );
    wRowSeparator.addModifyListener( lsMod );
    fdRowSeparator = new FormData();
    fdRowSeparator.left = new FormAttachment( wlRowSeparator, margin );
    fdRowSeparator.top = new FormAttachment( wRowSeparatorFormat, margin );
    fdRowSeparator.right = new FormAttachment( 100, 0 );
    wRowSeparator.setLayoutData( fdRowSeparator );

    wlSmartCount = new Label( wRowSeparatorGroup, SWT.RIGHT );
    wlSmartCount.setText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.SmartCount.Label" ) );
    props.setLook( wlSmartCount );
    fdlSmartCount = new FormData();
    fdlSmartCount.left = new FormAttachment( 0, 0 );
    fdlSmartCount.top = new FormAttachment( wRowSeparator, margin );
    fdlSmartCount.right = new FormAttachment( middle, -margin );
    wlSmartCount.setLayoutData( fdlSmartCount );
    wSmartCount = new Button( wRowSeparatorGroup, SWT.CHECK );
    props.setLook( wSmartCount );
    wSmartCount.setToolTipText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.SmartCount.Tooltip" ) );
    fdSmartCount = new FormData();
    fdSmartCount.left = new FormAttachment( middle, 0 );
    fdSmartCount.top = new FormAttachment( wlSmartCount, 0, SWT.CENTER );
    wSmartCount.setLayoutData( fdSmartCount );
    wSmartCount.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        input.setChanged();
      }
    } );

    fdRowSeparatorGroup = new FormData();
    fdRowSeparatorGroup.left = new FormAttachment( 0, margin );
    fdRowSeparatorGroup.top = new FormAttachment( wFilesCountFieldGroup, margin );
    fdRowSeparatorGroup.right = new FormAttachment( 100, -margin );
    wRowSeparatorGroup.setLayoutData( fdRowSeparatorGroup );

    // ///////////////////////////////////////////////////////////
    // / END OF ROW SEPARATOR GROUP
    // ///////////////////////////////////////////////////////////

    // /////////////////////////////////
    // START OF Additional Fields GROUP
    // /////////////////////////////////

    wAdditionalGroup = new Group( wContentComp, SWT.SHADOW_NONE );
    props.setLook( wAdditionalGroup );
    wAdditionalGroup
      .setText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.Group.AdditionalGroup.Label" ) );

    FormLayout additionalgroupLayout = new FormLayout();
    additionalgroupLayout.marginWidth = 10;
    additionalgroupLayout.marginHeight = 10;
    wAdditionalGroup.setLayout( additionalgroupLayout );

    wlInclFilesCount = new Label( wAdditionalGroup, SWT.RIGHT );
    wlInclFilesCount.setText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.InclCountFiles.Label" ) );
    props.setLook( wlInclFilesCount );
    fdlInclFilesCount = new FormData();
    fdlInclFilesCount.left = new FormAttachment( 0, 0 );
    fdlInclFilesCount.top = new FormAttachment( wRowSeparatorGroup, margin );
    fdlInclFilesCount.right = new FormAttachment( middle, -margin );
    wlInclFilesCount.setLayoutData( fdlInclFilesCount );
    wInclFilesCount = new Button( wAdditionalGroup, SWT.CHECK );
    props.setLook( wInclFilesCount );
    wInclFilesCount.setToolTipText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.InclCountFiles.Tooltip" ) );
    fdFilesCount = new FormData();
    fdFilesCount.left = new FormAttachment( middle, 0 );
    fdFilesCount.top = new FormAttachment( wRowSeparatorGroup, margin );
    wInclFilesCount.setLayoutData( fdFilesCount );

    wlInclFilesCountField = new Label( wAdditionalGroup, SWT.RIGHT );
    wlInclFilesCountField.setText( BaseMessages.getString(
      PKG, "GetFilesRowsCountDialog.InclCountFilesField.Label" ) );
    props.setLook( wlInclFilesCountField );
    fdlInclFilesCountField = new FormData();
    fdlInclFilesCountField.left = new FormAttachment( wInclFilesCount, margin );
    fdlInclFilesCountField.top = new FormAttachment( wRowSeparatorGroup, margin );
    wlInclFilesCountField.setLayoutData( fdlInclFilesCountField );
    wInclFilesCountField = new TextVar( pipelineMeta, wAdditionalGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclFilesCountField );
    wInclFilesCountField.addModifyListener( lsMod );
    fdInclFilesCountField = new FormData();
    fdInclFilesCountField.left = new FormAttachment( wlInclFilesCountField, margin );
    fdInclFilesCountField.top = new FormAttachment( wlInclFilesCountField, 0, SWT.CENTER );
    fdInclFilesCountField.right = new FormAttachment( 100, 0 );
    wInclFilesCountField.setLayoutData( fdInclFilesCountField );

    fdAdditionalGroup = new FormData();
    fdAdditionalGroup.left = new FormAttachment( 0, margin );
    fdAdditionalGroup.top = new FormAttachment( wRowSeparatorGroup, margin );
    fdAdditionalGroup.right = new FormAttachment( 100, -margin );
    wAdditionalGroup.setLayoutData( fdAdditionalGroup );

    // ///////////////////////////////////////////////////////////
    // / END OF ADDITIONNAL FIELDS GROUP
    // ///////////////////////////////////////////////////////////

    // ///////////////////////////////
    // START OF AddFileResult GROUP //
    // ///////////////////////////////

    wAddFileResult = new Group( wContentComp, SWT.SHADOW_NONE );
    props.setLook( wAddFileResult );
    wAddFileResult.setText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.wAddFileResult.Label" ) );

    FormLayout AddFileResultgroupLayout = new FormLayout();
    AddFileResultgroupLayout.marginWidth = 10;
    AddFileResultgroupLayout.marginHeight = 10;
    wAddFileResult.setLayout( AddFileResultgroupLayout );

    wlAddResult = new Label( wAddFileResult, SWT.RIGHT );
    wlAddResult.setText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.AddResult.Label" ) );
    props.setLook( wlAddResult );
    fdlAddResult = new FormData();
    fdlAddResult.left = new FormAttachment( 0, 0 );
    fdlAddResult.top = new FormAttachment( wAdditionalGroup, margin );
    fdlAddResult.right = new FormAttachment( middle, -margin );
    wlAddResult.setLayoutData( fdlAddResult );
    wAddResult = new Button( wAddFileResult, SWT.CHECK );
    props.setLook( wAddResult );
    wAddResult.setToolTipText( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.AddResult.Tooltip" ) );
    fdAddResult = new FormData();
    fdAddResult.left = new FormAttachment( middle, 0 );
    fdAddResult.top = new FormAttachment( wlAddResult, 0, SWT.CENTER );
    wAddResult.setLayoutData( fdAddResult );
    wAddResult.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        input.setChanged();
      }
    } );

    fdAddFileResult = new FormData();
    fdAddFileResult.left = new FormAttachment( 0, margin );
    fdAddFileResult.top = new FormAttachment( wAdditionalGroup, margin );
    fdAddFileResult.right = new FormAttachment( 100, -margin );
    wAddFileResult.setLayoutData( fdAddFileResult );

    // ///////////////////////////////////////////////////////////
    // / END OF AddFileResult GROUP
    // ////////////////////////////////////

    fdContentComp = new FormData();
    fdContentComp.left = new FormAttachment( 0, 0 );
    fdContentComp.top = new FormAttachment( 0, 0 );
    fdContentComp.right = new FormAttachment( 100, 0 );
    fdContentComp.bottom = new FormAttachment( 100, 0 );
    wContentComp.setLayoutData( fdContentComp );

    wContentComp.layout();
    wContentTab.setControl( wContentComp );

    // ///////////////////////////////////////////////////////////
    // / END OF CONTENT TAB
    // ///////////////////////////////////////////////////////////

    fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wTransformName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOk, -2*margin );
    wTabFolder.setLayoutData( fdTabFolder );


    // Add listeners
    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    wTransformName.addSelectionListener( lsDef );
    wInclFilesCountField.addSelectionListener( lsDef );

    // Add the file to the list of files...
    SelectionAdapter selA = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        wFilenameList.add( new String[] {
          wFilename.getText(), wFilemask.getText(), wExcludeFilemask.getText(),
          GetFilesRowsCountMeta.RequiredFilesCode[ 0 ], GetFilesRowsCountMeta.RequiredFilesCode[ 0 ] } );
        wFilename.setText( "" );
        wFilemask.setText( "" );
        wExcludeFilemask.setText( "" );
        wFilenameList.removeEmptyRows();
        wFilenameList.setRowNums();
        wFilenameList.optWidth( true );
      }
    };
    wbaFilename.addSelectionListener( selA );
    wFilename.addSelectionListener( selA );

    // Delete files from the list of files...
    wbdFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        int[] idx = wFilenameList.getSelectionIndices();
        wFilenameList.remove( idx );
        wFilenameList.removeEmptyRows();
        wFilenameList.setRowNums();
        input.setChanged();
      }
    } );

    // Edit the selected file & remove from the list...
    wbeFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        int idx = wFilenameList.getSelectionIndex();
        if ( idx >= 0 ) {
          String[] string = wFilenameList.getItem( idx );
          wFilename.setText( string[ 0 ] );
          wFilemask.setText( string[ 1 ] );
          wExcludeFilemask.setText( string[ 2 ] );
          wFilenameList.remove( idx );
        }
        wFilenameList.removeEmptyRows();
        wFilenameList.setRowNums();
        input.setChanged();
      }
    } );

    // Show the files that are selected at this time...
    wbShowFiles.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        try {
          GetFilesRowsCountMeta tfii = new GetFilesRowsCountMeta();
          getInfo( tfii );
          FileInputList fileInputList = tfii.getFiles( pipelineMeta );
          String[] files = fileInputList.getFileStrings();

          if ( files.length > 0 ) {
            EnterSelectionDialog esd = new EnterSelectionDialog( shell, files,
              BaseMessages.getString( PKG, "GetFilesRowsCountDialog.FilesReadSelection.DialogTitle" ),
              BaseMessages.getString( PKG, "GetFilesRowsCountDialog.FilesReadSelection.DialogMessage" ) );
            esd.setViewOnly();
            esd.open();
          } else {
            MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
            mb.setMessage( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.NoFileFound.DialogMessage" ) );
            mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
            mb.open();
          }
        } catch ( HopException ex ) {
          new ErrorDialog( shell, BaseMessages.getString( PKG, "GetFilesRowsCountDialog.ErrorParsingData.DialogTitle" ),
            BaseMessages.getString( PKG, "GetFilesRowsCountDialog.ErrorParsingData.DialogMessage" ), ex );
        }
      }
    } );

    // Enable/disable the right fields to allow a row number to be added to each row...
    wInclFilesCount.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        setIncludeRownum();
        input.setChanged();
      }
    } );

    // Whenever something changes, set the tooltip to the expanded version of the filename:
    wFilename.addModifyListener( e -> {
      wFilename.setToolTipText( "" ); // StringUtil.environmentSubstitute( wFilename.getText() ) );
    } );

    // Listen to the Browse... button
    wbbFilename.addListener( SWT.Selection, e-> {
        if ( !Utils.isEmpty( wFilemask.getText() ) || !Utils.isEmpty( wExcludeFilemask.getText() ) ) { // A mask: a directory!
          BaseDialog.presentDirectoryDialog( shell, wFilemask, pipelineMeta );
        } else {
          BaseDialog.presentFileDialog( shell, wFilename, pipelineMeta,
            new String[] { "*" },
            new String[] { BaseMessages.getString( PKG, "System.FileType.AllFiles" ) },
            true
          );
        }
      } );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    wTabFolder.setSelection( 0 );

    // Set the shell size, based upon previous time...
    setSize();
    getData( input );
    activateFileField();
    ActiveRowSeparator();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void ActiveRowSeparator() {
    wRowSeparator.setEnabled( wRowSeparatorFormat.getSelectionIndex() == 3 );
    wlRowSeparator.setEnabled( wRowSeparatorFormat.getSelectionIndex() == 3 );
  }

  public void setIncludeRownum() {
    wlInclFilesCountField.setEnabled( wInclFilesCount.getSelection() );
    wInclFilesCountField.setEnabled( wInclFilesCount.getSelection() );
  }

  private void activateFileField() {
    wlFilenameField.setEnabled( wFileField.getSelection() );
    wFilenameField.setEnabled( wFileField.getSelection() );

    wlFilename.setEnabled( !wFileField.getSelection() );
    wbbFilename.setEnabled( !wFileField.getSelection() );
    wbaFilename.setEnabled( !wFileField.getSelection() );
    wFilename.setEnabled( !wFileField.getSelection() );
    wlFilemask.setEnabled( !wFileField.getSelection() );
    wFilemask.setEnabled( !wFileField.getSelection() );
    wlExcludeFilemask.setEnabled( !wFileField.getSelection() );
    wExcludeFilemask.setEnabled( !wFileField.getSelection() );
    wlFilenameList.setEnabled( !wFileField.getSelection() );
    wbdFilename.setEnabled( !wFileField.getSelection() );
    wbeFilename.setEnabled( !wFileField.getSelection() );
    wbShowFiles.setEnabled( !wFileField.getSelection() );
    wlFilenameList.setEnabled( !wFileField.getSelection() );
    wFilenameList.setEnabled( !wFileField.getSelection() );
    wPreview.setEnabled( !wFileField.getSelection() );
  }

  private void setFileField() {
    try {

      wFilenameField.removeAll();

      IRowMeta r = pipelineMeta.getPrevTransformFields( transformName );
      if ( r != null ) {
        r.getFieldNames();

        for ( int i = 0; i < r.getFieldNames().length; i++ ) {
          wFilenameField.add( r.getFieldNames()[ i ] );

        }
      }

    } catch ( HopException ke ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "GetFilesRowsCountDialog.FailedToGetFields.DialogTitle" ),
        BaseMessages.getString( PKG, "GetFilesRowsCountDialog.FailedToGetFields.DialogMessage" ), ke );
    }
  }

  /**
   * Read the data from the GetFilesRowsCountMeta object and show it in this dialog.
   *
   * @param in The GetFilesRowsCountMeta object to obtain the data from.
   */
  public void getData( GetFilesRowsCountMeta in ) {
    if ( in.getFileName() != null ) {
      wFilenameList.removeAll();
      for ( int i = 0; i < in.getFileName().length; i++ ) {
        wFilenameList.add( new String[] {
          in.getFileName()[ i ], in.getFileMask()[ i ], in.getExludeFileMask()[ i ],
          in.getRequiredFilesDesc( in.getFileRequired()[ i ] ),
          in.getRequiredFilesDesc( in.getIncludeSubFolders()[ i ] ) } );
      }
      wFilenameList.removeEmptyRows();
      wFilenameList.setRowNums();
      wFilenameList.optWidth( true );
    }
    wInclFilesCount.setSelection( in.includeCountFiles() );
    wSmartCount.setSelection( in.isSmartCount() );

    if ( in.getFilesCountFieldName() != null ) {
      wInclFilesCountField.setText( in.getFilesCountFieldName() );
    } else {
      wInclFilesCountField.setText( "filescount" );
    }

    if ( in.getRowsCountFieldName() != null ) {
      wRowsCountField.setText( in.getRowsCountFieldName() );
    } else {
      wRowsCountField.setText( GetFilesRowsCountMeta.DEFAULT_ROWSCOUNT_FIELDNAME );
    }

    if ( in.getRowSeparatorFormat() != null ) {
      // Checking for 'CR' for backwards compatibility
      if ( in.getRowSeparatorFormat().equals( "CARRIAGERETURN" ) || in.getRowSeparatorFormat().equals( "CR" ) ) {
        wRowSeparatorFormat.select( 0 );
      } else if ( in.getRowSeparatorFormat().equals( "LINEFEED" ) || in.getRowSeparatorFormat().equals( "LF" ) ) {
        // Checking for 'LF' for backwards compatibility
        wRowSeparatorFormat.select( 1 );
      } else if ( in.getRowSeparatorFormat().equals( "CRLF" ) ) {
        wRowSeparatorFormat.select( 2 );
      } else if ( in.getRowSeparatorFormat().equals( "TAB" ) ) {
        wRowSeparatorFormat.select( 3 );
      } else {
        wRowSeparatorFormat.select( 4 );
      }
    } else {
      wRowSeparatorFormat.select( 0 );
    }

    if ( in.getRowSeparator() != null ) {
      wRowSeparator.setText( in.getRowSeparator() );
    }

    wAddResult.setSelection( in.isAddResultFile() );
    wFileField.setSelection( in.isFileField() );
    if ( in.setOutputFilenameField() != null ) {
      wFilenameField.setText( in.setOutputFilenameField() );
    }

    logDebug( BaseMessages.getString( PKG, "GetFilesRowsCountDialog.Log.GettingFieldsInfo" ) );

    setIncludeRownum();

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

    try {
      getInfo( input );
    } catch ( HopException e ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "GetFilesRowsCountDialog.ErrorParsingData.DialogTitle" ),
        BaseMessages.getString( PKG, "GetFilesRowsCountDialog.ErrorParsingData.DialogMessage" ), e );
    }
    dispose();
  }

  private void getInfo( GetFilesRowsCountMeta in ) throws HopException {
    transformName = wTransformName.getText(); // return value

    in.setIncludeCountFiles( wInclFilesCount.getSelection() );
    in.setFilesCountFieldName( wInclFilesCountField.getText() );
    in.setRowsCountFieldName( wRowsCountField.getText() );

    if ( wRowSeparatorFormat.getSelectionIndex() == 0 ) {
      in.setRowSeparatorFormat( "CARRIAGERETURN" );
    } else if ( wRowSeparatorFormat.getSelectionIndex() == 1 ) {
      in.setRowSeparatorFormat( "LINEFEED" );
    } else if ( wRowSeparatorFormat.getSelectionIndex() == 2 ) {
      in.setRowSeparatorFormat( "CRLF" );
    } else if ( wRowSeparatorFormat.getSelectionIndex() == 3 ) {
      in.setRowSeparatorFormat( "TAB" );
    } else {
      in.setRowSeparatorFormat( "CUSTOM" );
    }

    int nrFiles = wFilenameList.getItemCount();

    in.allocate( nrFiles );

    in.setFileName( wFilenameList.getItems( 0 ) );
    in.setFileMask( wFilenameList.getItems( 1 ) );
    in.setExcludeFileMask( wFilenameList.getItems( 2 ) );
    in.setFileRequired( wFilenameList.getItems( 3 ) );
    in.setIncludeSubFolders( wFilenameList.getItems( 4 ) );

    if ( wRowSeparator.getText().length() > 1 ) {
      if ( wRowSeparator.getText().substring( 0, 1 ).equals( "\\" ) ) {
        // Take the 2 first
        wRowSeparator.setText( wRowSeparator.getText().substring( 0, 2 ) );
      } else {
        wRowSeparator.setText( wRowSeparator.getText().substring( 0, 1 ) );
      }
    }
    in.setRowSeparator( wRowSeparator.getText() );
    in.setSmartCount( wSmartCount.getSelection() );
    in.setAddResultFile( wAddResult.getSelection() );
    in.setFileField( wFileField.getSelection() );
    in.setOutputFilenameField( wFilenameField.getText() );

  }

  // Preview the data
  private void preview() {
    try {

      GetFilesRowsCountMeta oneMeta = new GetFilesRowsCountMeta();
      getInfo( oneMeta );

      PipelineMeta previewMeta = PipelinePreviewFactory.generatePreviewPipeline( pipelineMeta, pipelineMeta.getMetadataProvider(),
        oneMeta, wTransformName.getText() );

      EnterNumberDialog numberDialog =
        new EnterNumberDialog( shell, props.getDefaultPreviewSize(),
          BaseMessages.getString( PKG, "GetFilesRowsCountDialog.NumberRows.DialogTitle" ),
          BaseMessages.getString( PKG, "GetFilesRowsCountDialog.NumberRows.DialogMessage" ) );
      int previewSize = numberDialog.open();
      if ( previewSize > 0 ) {
        PipelinePreviewProgressDialog progressDialog =
          new PipelinePreviewProgressDialog(
            shell, previewMeta, new String[] { wTransformName.getText() }, new int[] { previewSize } );
        progressDialog.open();

        if ( !progressDialog.isCancelled() ) {
          Pipeline pipeline = progressDialog.getPipeline();
          String loggingText = progressDialog.getLoggingText();

          if ( pipeline.getResult() != null && pipeline.getResult().getNrErrors() > 0 ) {
            EnterTextDialog etd =
              new EnterTextDialog(
                shell, BaseMessages.getString( PKG, "System.Dialog.PreviewError.Title" ), BaseMessages
                .getString( PKG, "System.Dialog.PreviewError.Message" ), loggingText, true );
            etd.setReadOnly();
            etd.open();
          }

          PreviewRowsDialog prd =
            new PreviewRowsDialog(
              shell, pipelineMeta, SWT.NONE, wTransformName.getText(), progressDialog.getPreviewRowsMeta( wTransformName
              .getText() ), progressDialog.getPreviewRows( wTransformName.getText() ), loggingText );
          prd.open();

        }
      }
    } catch ( HopException e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "GetFilesRowsCountDialog.ErrorPreviewingData.DialogTitle" ),
        BaseMessages.getString( PKG, "GetFilesRowsCountDialog.ErrorPreviewingData.DialogMessage" ), e );
    }
  }

}
