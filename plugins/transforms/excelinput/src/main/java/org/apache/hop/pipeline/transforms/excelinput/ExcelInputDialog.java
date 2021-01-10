//CHECKSTYLE:FileLength:OFF
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

package org.apache.hop.pipeline.transforms.excelinput;


import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.spreadsheet.IKCell;
import org.apache.hop.core.spreadsheet.IKSheet;
import org.apache.hop.core.spreadsheet.IKWorkbook;
import org.apache.hop.core.spreadsheet.KCellType;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.fileinput.text.DirectoryDialogButtonListenerFactory;
import org.apache.hop.ui.core.dialog.*;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.core.widget.VariableButtonListenerFactory;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ComponentSelectionListener;
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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class ExcelInputDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = ExcelInputMeta.class; // For Translator

  /**
   * Marker put on tab to indicate attention required
   */
  private static final String TAB_FLAG = "!";

  private static final String[] YES_NO_COMBO = new String[] {
    BaseMessages.getString( PKG, "System.Combo.No" ), BaseMessages.getString( PKG, "System.Combo.Yes" ) };

  private CTabFolder wTabFolder;

  private CTabItem wFileTab;
  private CTabItem wSheetTab;
  private CTabItem wFieldsTab;

  private Label wlStatusMessage;

  private Label wlFilename;
  private Button wbbFilename; // Browse: add file or directory
  private Button wbdFilename; // Delete
  private Button wbeFilename; // Edit
  private Button wbaFilename; // Add or change
  private TextVar wFilename;

  private Label wlFilenameList;
  private TableView wFilenameList;

  private Label wlFilemask;
  private Text wFilemask;

  private Label wlExcludeFilemask;
  private TextVar wExcludeFilemask;

  private Button wAccFilenames;

  private Label wlAccField;
  private CCombo wAccField;

  private Label wlAccTransform;
  private CCombo wAccTransform;

  private Button wbShowFiles;

  private TableView wSheetnameList;

  private Button wHeader;

  private Button wNoEmpty;

  private Button wStopOnEmpty;

  private Text wInclFilenameField;

  private Text wInclSheetnameField;

  private Text wInclRownumField;

  private Text wInclSheetRownumField;

  private Text wLimit;

  private CCombo wSpreadSheetType;

  private CCombo wEncoding;

  private Button wbGetFields;

  private TableView wFields;

  private Button wStrictTypes;

  private Button wErrorIgnored;

  private Label wlSkipErrorLines;
  private Button wSkipErrorLines;

  // New entries for intelligent error handling AKA replay functionality
  // Bad files destination directory
  private Label wlWarningDestDir;
  private Button wbbWarningDestDir; // Browse: add file or directory
  private Button wbvWarningDestDir; // Variable
  private TextVar wWarningDestDir;
  private Label wlWarningExt;
  private Text wWarningExt;

  // Error messages files destination directory
  private Label wlErrorDestDir;
  private Button wbbErrorDestDir; // Browse: add file or directory
  private Button wbvErrorDestDir; // Variable
  private TextVar wErrorDestDir;
  private Label wlErrorExt;
  private Text wErrorExt;

  // Line numbers files destination directory
  private Label wlLineNrDestDir;
  private Button wbbLineNrDestDir; // Browse: add file or directory
  private Button wbvLineNrDestDir; // Variable
  private TextVar wLineNrDestDir;
  private Label wlLineNrExt;
  private Text wLineNrExt;

  private final ExcelInputMeta input;
  private int middle;
  private int margin;
  private boolean gotEncodings = false;

  private Button wAddResult;

  private ModifyListener lsMod;

  private Text wShortFileFieldName;
  private Text wPathFieldName;

  private Text wIsHiddenName;
  private Text wLastModificationTimeName;
  private Text wUriName;
  private Text wRootUriName;
  private Text wExtensionFieldName;
  private Text wSizeFieldName;

  public ExcelInputDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (ExcelInputMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    lsMod = e -> {
      input.setChanged();
      checkAlerts();
    };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "ExcelInputDialog.DialogTitle" ) );

    middle = props.getMiddlePct();
    margin = props.getMargin();

    // Buttons at the bottom
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wPreview = new Button( shell, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( PKG, "ExcelInputDialog.PreviewRows.Button" ) );
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

    // Status Message
    wlStatusMessage = new Label( shell, SWT.RIGHT );
    wlStatusMessage.setText( "(This Space To Let)" );
    wlStatusMessage.setForeground( display.getSystemColor( SWT.COLOR_RED ) );
    props.setLook( wlStatusMessage );
    FormData fdlStatusMessage = new FormData();
    fdlStatusMessage.left = new FormAttachment( 0, 0 );
    fdlStatusMessage.top = new FormAttachment( wlTransformName, margin );
    fdlStatusMessage.right = new FormAttachment( middle, -margin );
    wlStatusMessage.setLayoutData(fdlStatusMessage);

    // Tabs
    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

    //
    // START OF FILE TAB /
    //
    wFileTab = new CTabItem( wTabFolder, SWT.NONE );
    wFileTab.setText( BaseMessages.getString( PKG, "ExcelInputDialog.FileTab.TabTitle" ) );

    Composite wFileComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wFileComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wFileComp.setLayout( fileLayout );

    // spreadsheet engine type
    Label wlSpreadSheetType = new Label(wFileComp, SWT.RIGHT);
    wlSpreadSheetType.setText( BaseMessages.getString( PKG, "ExcelInputDialog.SpreadSheetType.Label" ) );
    props.setLook(wlSpreadSheetType);
    FormData fdlSpreadSheetType = new FormData();
    fdlSpreadSheetType.left = new FormAttachment( 0, 0 );
    fdlSpreadSheetType.right = new FormAttachment( middle, -margin );
    fdlSpreadSheetType.top = new FormAttachment( 0, 0 );

    wlSpreadSheetType.setLayoutData(fdlSpreadSheetType);
    wSpreadSheetType = new CCombo(wFileComp, SWT.BORDER | SWT.READ_ONLY );
    wSpreadSheetType.setEditable( true );
    props.setLook( wSpreadSheetType );
    wSpreadSheetType.addModifyListener( lsMod );
    FormData fdSpreadSheetType = new FormData();
    fdSpreadSheetType.left = new FormAttachment( middle, 0 );
    fdSpreadSheetType.right = new FormAttachment( 100, 0 );
    fdSpreadSheetType.top = new FormAttachment( 0, 0 );
    wSpreadSheetType.setLayoutData(fdSpreadSheetType);
    for ( SpreadSheetType type : SpreadSheetType.values() ) {
      wSpreadSheetType.add( type.getDescription() );
    }

    // Filename line
    wlFilename = new Label(wFileComp, SWT.RIGHT );
    wlFilename.setText( BaseMessages.getString( PKG, "ExcelInputDialog.Filename.Label" ) );
    props.setLook( wlFilename );
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment( wSpreadSheetType, margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData(fdlFilename);

    wbbFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbbFilename );
    wbbFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    wbbFilename.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.BrowseForFileOrDirAndAdd" ) );
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment( wSpreadSheetType, margin );
    wbbFilename.setLayoutData(fdbFilename);

    wbaFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbaFilename );
    wbaFilename.setText( BaseMessages.getString( PKG, "ExcelInputDialog.FilenameAdd.Button" ) );
    wbaFilename.setToolTipText( BaseMessages.getString( PKG, "ExcelInputDialog.FilenameAdd.Tooltip" ) );
    FormData fdbaFilename = new FormData();
    fdbaFilename.right = new FormAttachment( wbbFilename, -margin );
    fdbaFilename.top = new FormAttachment( wSpreadSheetType, margin );
    wbaFilename.setLayoutData(fdbaFilename);

    wFilename = new TextVar( variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.right = new FormAttachment( wbaFilename, -margin );
    fdFilename.top = new FormAttachment( wSpreadSheetType, margin );
    wFilename.setLayoutData(fdFilename);

    wlFilemask = new Label(wFileComp, SWT.RIGHT );
    wlFilemask.setText( BaseMessages.getString( PKG, "ExcelInputDialog.Filemask.Label" ) );
    props.setLook( wlFilemask );
    FormData fdlFilemask = new FormData();
    fdlFilemask.left = new FormAttachment( 0, 0 );
    fdlFilemask.top = new FormAttachment( wFilename, margin );
    fdlFilemask.right = new FormAttachment( middle, -margin );
    wlFilemask.setLayoutData(fdlFilemask);
    wFilemask = new Text(wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilemask );
    wFilemask.addModifyListener( lsMod );
    FormData fdFilemask = new FormData();
    fdFilemask.left = new FormAttachment( middle, 0 );
    fdFilemask.top = new FormAttachment( wFilename, margin );
    fdFilemask.right = new FormAttachment( wbaFilename, -margin );
    wFilemask.setLayoutData(fdFilemask);

    wlExcludeFilemask = new Label(wFileComp, SWT.RIGHT );
    wlExcludeFilemask.setText( BaseMessages.getString( PKG, "ExcelInputDialog.ExcludeFilemask.Label" ) );
    props.setLook( wlExcludeFilemask );
    FormData fdlExcludeFilemask = new FormData();
    fdlExcludeFilemask.left = new FormAttachment( 0, 0 );
    fdlExcludeFilemask.top = new FormAttachment( wFilemask, margin );
    fdlExcludeFilemask.right = new FormAttachment( middle, -margin );
    wlExcludeFilemask.setLayoutData(fdlExcludeFilemask);
    wExcludeFilemask = new TextVar( variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wExcludeFilemask );
    wExcludeFilemask.addModifyListener( lsMod );
    FormData fdExcludeFilemask = new FormData();
    fdExcludeFilemask.left = new FormAttachment( middle, 0 );
    fdExcludeFilemask.top = new FormAttachment( wFilemask, margin );
    fdExcludeFilemask.right = new FormAttachment( wFilename, 0, SWT.RIGHT );
    wExcludeFilemask.setLayoutData(fdExcludeFilemask);

    // Filename list line
    wlFilenameList = new Label(wFileComp, SWT.RIGHT );
    wlFilenameList.setText( BaseMessages.getString( PKG, "ExcelInputDialog.FilenameList.Label" ) );
    props.setLook( wlFilenameList );
    FormData fdlFilenameList = new FormData();
    fdlFilenameList.left = new FormAttachment( 0, 0 );
    fdlFilenameList.top = new FormAttachment( wExcludeFilemask, margin );
    fdlFilenameList.right = new FormAttachment( middle, -margin );
    wlFilenameList.setLayoutData(fdlFilenameList);

    // Buttons to the right of the screen...
    wbdFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbdFilename );
    wbdFilename.setText( BaseMessages.getString( PKG, "ExcelInputDialog.FilenameDelete.Button" ) );
    wbdFilename.setToolTipText( BaseMessages.getString( PKG, "ExcelInputDialog.FilenameDelete.Tooltip" ) );
    FormData fdbdFilename = new FormData();
    fdbdFilename.right = new FormAttachment( 100, 0 );
    fdbdFilename.top = new FormAttachment( wExcludeFilemask, 40 );
    wbdFilename.setLayoutData(fdbdFilename);

    wbeFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbeFilename );
    wbeFilename.setText( BaseMessages.getString( PKG, "ExcelInputDialog.FilenameEdit.Button" ) );
    wbeFilename.setToolTipText( BaseMessages.getString( PKG, "ExcelInputDialog.FilenameEdit.Tooltip" ) );
    FormData fdbeFilename = new FormData();
    fdbeFilename.right = new FormAttachment( 100, 0 );
    fdbeFilename.left = new FormAttachment( wbdFilename, 0, SWT.LEFT );
    fdbeFilename.top = new FormAttachment( wbdFilename, margin );
    wbeFilename.setLayoutData(fdbeFilename);

    wbShowFiles = new Button(wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbShowFiles );
    wbShowFiles.setText( BaseMessages.getString( PKG, "ExcelInputDialog.ShowFiles.Button" ) );
    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment( middle, 0 );
    fdbShowFiles.bottom = new FormAttachment( 100, -margin );
    wbShowFiles.setLayoutData(fdbShowFiles);

    // Accepting filenames group
    //
    Group gAccepting = new Group(wFileComp, SWT.SHADOW_ETCHED_IN);
    gAccepting.setText( BaseMessages.getString( PKG, "ExcelInputDialog.AcceptingGroup.Label" ) );
    FormLayout acceptingLayout = new FormLayout();
    acceptingLayout.marginWidth = 3;
    acceptingLayout.marginHeight = 3;
    gAccepting.setLayout( acceptingLayout );
    props.setLook(gAccepting);

    // Accept filenames from previous transforms?
    //
    Label wlAccFilenames = new Label(gAccepting, SWT.RIGHT);
    wlAccFilenames.setText( BaseMessages.getString( PKG, "ExcelInputDialog.AcceptFilenames.Label" ) );
    props.setLook(wlAccFilenames);
    FormData fdlAccFilenames = new FormData();
    fdlAccFilenames.top = new FormAttachment( 0, margin );
    fdlAccFilenames.left = new FormAttachment( 0, 0 );
    fdlAccFilenames.right = new FormAttachment( middle, -margin );
    wlAccFilenames.setLayoutData(fdlAccFilenames);
    wAccFilenames = new Button(gAccepting, SWT.CHECK );
    wAccFilenames.setToolTipText( BaseMessages.getString( PKG, "ExcelInputDialog.AcceptFilenames.Tooltip" ) );
    props.setLook( wAccFilenames );
    FormData fdAccFilenames = new FormData();
    fdAccFilenames.top = new FormAttachment( wlAccFilenames, 0, SWT.CENTER );
    fdAccFilenames.left = new FormAttachment( middle, 0 );
    fdAccFilenames.right = new FormAttachment( 100, 0 );
    wAccFilenames.setLayoutData(fdAccFilenames);
    wAccFilenames.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        setFlags();
        input.setChanged();
      }
    } );

    // Which transform to read from?
    wlAccTransform = new Label(gAccepting, SWT.RIGHT );
    wlAccTransform.setText( BaseMessages.getString( PKG, "ExcelInputDialog.AcceptTransform.Label" ) );
    props.setLook( wlAccTransform );
    FormData fdlAccTransform = new FormData();
    fdlAccTransform.top = new FormAttachment( wAccFilenames, margin );
    fdlAccTransform.left = new FormAttachment( 0, 0 );
    fdlAccTransform.right = new FormAttachment( middle, -margin );
    wlAccTransform.setLayoutData(fdlAccTransform);
    wAccTransform = new CCombo(gAccepting, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wAccTransform.setToolTipText( BaseMessages.getString( PKG, "ExcelInputDialog.AcceptTransform.Tooltip" ) );
    props.setLook( wAccTransform );
    FormData fdAccTransform = new FormData();
    fdAccTransform.top = new FormAttachment( wAccFilenames, margin );
    fdAccTransform.left = new FormAttachment( middle, 0 );
    fdAccTransform.right = new FormAttachment( 100, 0 );
    wAccTransform.setLayoutData(fdAccTransform);

    // Which field?
    //
    wlAccField = new Label(gAccepting, SWT.RIGHT );
    wlAccField.setText( BaseMessages.getString( PKG, "ExcelInputDialog.AcceptField.Label" ) );
    props.setLook( wlAccField );
    FormData fdlAccField = new FormData();
    fdlAccField.top = new FormAttachment( wAccTransform, margin );
    fdlAccField.left = new FormAttachment( 0, 0 );
    fdlAccField.right = new FormAttachment( middle, -margin );
    wlAccField.setLayoutData(fdlAccField);

    wAccField = new CCombo(gAccepting, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    IRowMeta previousFields;
    try {
      previousFields = pipelineMeta.getPrevTransformFields( variables, transformMeta );
    } catch ( HopTransformException e ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "ExcelInputDialog.ErrorDialog.UnableToGetInputFields.Title" ),
        BaseMessages.getString( PKG, "ExcelInputDialog.ErrorDialog.UnableToGetInputFields.Message" ), e );
      previousFields = new RowMeta();
    }
    wAccField.setItems( previousFields.getFieldNames() );
    wAccField.setToolTipText( BaseMessages.getString( PKG, "ExcelInputDialog.AcceptField.Tooltip" ) );

    props.setLook( wAccField );
    FormData fdAccField = new FormData();
    fdAccField.top = new FormAttachment( wAccTransform, margin );
    fdAccField.left = new FormAttachment( middle, 0 );
    fdAccField.right = new FormAttachment( 100, 0 );
    wAccField.setLayoutData(fdAccField);

    // Fill in the source transforms...
    List<TransformMeta> prevTransforms = pipelineMeta.findPreviousTransforms( pipelineMeta.findTransform( transformName ) );
    for ( TransformMeta prevTransform : prevTransforms ) {
      wAccTransform.add( prevTransform.getName() );
    }

    FormData fdAccepting = new FormData();
    fdAccepting.left = new FormAttachment( middle, 0 );
    fdAccepting.right = new FormAttachment( 100, 0 );
    fdAccepting.bottom = new FormAttachment( wbShowFiles, -margin * 2 );
    // fdAccepting.bottom = new FormAttachment(wAccTransform, margin);
    gAccepting.setLayoutData(fdAccepting);

    ColumnInfo[] colinfo = new ColumnInfo[ 5 ];
    colinfo[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ExcelInputDialog.FileDir.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinfo[ 0 ].setUsingVariables( true );
    colinfo[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ExcelInputDialog.Wildcard.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinfo[ 1 ].setToolTip( BaseMessages.getString( PKG, "ExcelInputDialog.Wildcard.Tooltip" ) );
    colinfo[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ExcelInputDialog.Files.ExcludeWildcard.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinfo[ 2 ].setUsingVariables( true );
    colinfo[ 3 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ExcelInputDialog.Required.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        YES_NO_COMBO );
    colinfo[ 3 ].setToolTip( BaseMessages.getString( PKG, "ExcelInputDialog.Required.Tooltip" ) );
    colinfo[ 4 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ExcelInputDialog.IncludeSubDirs.Column" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, YES_NO_COMBO );
    colinfo[ 4 ].setToolTip( BaseMessages.getString( PKG, "ExcelInputDialog.IncludeSubDirs.Tooltip" ) );

    wFilenameList =
      new TableView( variables, wFileComp, SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER, colinfo, input
        .getFileName().length, lsMod, props );
    props.setLook( wFilenameList );
    FormData fdFilenameList = new FormData();
    fdFilenameList.left = new FormAttachment( middle, 0 );
    fdFilenameList.right = new FormAttachment( wbdFilename, -margin );
    fdFilenameList.top = new FormAttachment( wExcludeFilemask, margin );
    fdFilenameList.bottom = new FormAttachment(gAccepting, -margin );
    wFilenameList.setLayoutData(fdFilenameList);

    FormData fdFileComp = new FormData();
    fdFileComp.left = new FormAttachment( 0, 0 );
    fdFileComp.top = new FormAttachment( 0, 0 );
    fdFileComp.right = new FormAttachment( 100, 0 );
    fdFileComp.bottom = new FormAttachment( 100, 0 );
    wFileComp.setLayoutData(fdFileComp);

    wFileComp.layout();
    wFileTab.setControl(wFileComp);

    //
    // / END OF FILE TAB
    //
    //
    // START OF SHEET TAB /
    //
    wSheetTab = new CTabItem( wTabFolder, SWT.NONE );
    wSheetTab.setText( BaseMessages.getString( PKG, "ExcelInputDialog.SheetsTab.TabTitle" ) );

    Composite wSheetComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wSheetComp);

    FormLayout sheetLayout = new FormLayout();
    sheetLayout.marginWidth = 3;
    sheetLayout.marginHeight = 3;
    wSheetComp.setLayout( sheetLayout );

    Button wbGetSheets = new Button(wSheetComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbGetSheets);
    wbGetSheets.setText( BaseMessages.getString( PKG, "ExcelInputDialog.GetSheets.Button" ) );
    FormData fdbGetSheets = new FormData();
    fdbGetSheets.left = new FormAttachment( middle, 0 );
    fdbGetSheets.bottom = new FormAttachment( 100, -margin );
    wbGetSheets.setLayoutData(fdbGetSheets);

    Label wlSheetnameList = new Label(wSheetComp, SWT.RIGHT);
    wlSheetnameList.setText( BaseMessages.getString( PKG, "ExcelInputDialog.SheetNameList.Label" ) );
    props.setLook(wlSheetnameList);
    FormData fdlSheetnameList = new FormData();
    fdlSheetnameList.left = new FormAttachment( 0, 0 );
    fdlSheetnameList.top = new FormAttachment( wFilename, margin );
    fdlSheetnameList.right = new FormAttachment( middle, -margin );
    wlSheetnameList.setLayoutData(fdlSheetnameList);

    ColumnInfo[] shinfo = new ColumnInfo[ 3 ];
    shinfo[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ExcelInputDialog.SheetName.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false );
    shinfo[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ExcelInputDialog.StartRow.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false );
    shinfo[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ExcelInputDialog.StartColumn.Column" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );

    wSheetnameList =
      new TableView( variables, wSheetComp, SWT.FULL_SELECTION | SWT.MULTI | SWT.BORDER, shinfo, input
        .getSheetName().length, lsMod, props );
    props.setLook( wSheetnameList );
    fdFilenameList = new FormData();
    fdFilenameList.left = new FormAttachment( middle, 0 );
    fdFilenameList.right = new FormAttachment( 100, 0 );
    fdFilenameList.top = new FormAttachment( 0, 0 );
    fdFilenameList.bottom = new FormAttachment(wbGetSheets, -margin );
    wSheetnameList.setLayoutData(fdFilenameList);

    wSheetnameList.addModifyListener( arg0 -> checkAlerts() );

    FormData fdSheetComp = new FormData();
    fdSheetComp.left = new FormAttachment( 0, 0 );
    fdSheetComp.top = new FormAttachment( 0, 0 );
    fdSheetComp.right = new FormAttachment( 100, 0 );
    fdSheetComp.bottom = new FormAttachment( 100, 0 );
    wSheetComp.setLayoutData(fdSheetComp);

    wSheetComp.layout();
    wSheetTab.setControl(wSheetComp);

    //
    // / END OF SHEET TAB
    //
    //
    // START OF CONTENT TAB/
    // /
    CTabItem wContentTab = new CTabItem(wTabFolder, SWT.NONE);
    wContentTab.setText( BaseMessages.getString( PKG, "ExcelInputDialog.ContentTab.TabTitle" ) );

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    Composite wContentComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wContentComp);
    wContentComp.setLayout( contentLayout );

    // Header checkbox
    Label wlHeader = new Label(wContentComp, SWT.RIGHT);
    wlHeader.setText( BaseMessages.getString( PKG, "ExcelInputDialog.Header.Label" ) );
    props.setLook(wlHeader);
    FormData fdlHeader = new FormData();
    fdlHeader.left = new FormAttachment( 0, 0 );
    fdlHeader.top = new FormAttachment( 0, 0 );
    fdlHeader.right = new FormAttachment( middle, -margin );
    wlHeader.setLayoutData(fdlHeader);
    wHeader = new Button(wContentComp, SWT.CHECK );
    props.setLook( wHeader );
    FormData fdHeader = new FormData();
    fdHeader.left = new FormAttachment( middle, 0 );
    fdHeader.top = new FormAttachment( wlHeader, 0, SWT.CENTER );
    fdHeader.right = new FormAttachment( 100, 0 );
    wHeader.setLayoutData(fdHeader);
    wHeader.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        setFlags();
        input.setChanged();
      }
    } );

    Label wlNoEmpty = new Label(wContentComp, SWT.RIGHT);
    wlNoEmpty.setText( BaseMessages.getString( PKG, "ExcelInputDialog.NoEmpty.Label" ) );
    props.setLook(wlNoEmpty);
    FormData fdlNoEmpty = new FormData();
    fdlNoEmpty.left = new FormAttachment( 0, 0 );
    fdlNoEmpty.top = new FormAttachment( wHeader, margin );
    fdlNoEmpty.right = new FormAttachment( middle, -margin );
    wlNoEmpty.setLayoutData(fdlNoEmpty);
    wNoEmpty = new Button(wContentComp, SWT.CHECK );
    props.setLook( wNoEmpty );
    wNoEmpty.setToolTipText( BaseMessages.getString( PKG, "ExcelInputDialog.NoEmpty.Tooltip" ) );
    FormData fdNoEmpty = new FormData();
    fdNoEmpty.left = new FormAttachment( middle, 0 );
    fdNoEmpty.top = new FormAttachment( wlNoEmpty, 0, SWT.CENTER );
    fdNoEmpty.right = new FormAttachment( 100, 0 );
    wNoEmpty.setLayoutData(fdNoEmpty);
    wNoEmpty.addSelectionListener( new ComponentSelectionListener( input ) );

    Label wlStopOnEmpty = new Label(wContentComp, SWT.RIGHT);
    wlStopOnEmpty.setText( BaseMessages.getString( PKG, "ExcelInputDialog.StopOnEmpty.Label" ) );
    props.setLook(wlStopOnEmpty);
    FormData fdlStopOnEmpty = new FormData();
    fdlStopOnEmpty.left = new FormAttachment( 0, 0 );
    fdlStopOnEmpty.top = new FormAttachment( wNoEmpty, margin );
    fdlStopOnEmpty.right = new FormAttachment( middle, -margin );
    wlStopOnEmpty.setLayoutData(fdlStopOnEmpty);
    wStopOnEmpty = new Button(wContentComp, SWT.CHECK );
    props.setLook( wStopOnEmpty );
    wStopOnEmpty.setToolTipText( BaseMessages.getString( PKG, "ExcelInputDialog.StopOnEmpty.Tooltip" ) );
    FormData fdStopOnEmpty = new FormData();
    fdStopOnEmpty.left = new FormAttachment( middle, 0 );
    fdStopOnEmpty.top = new FormAttachment( wlStopOnEmpty, 0, SWT.CENTER );
    fdStopOnEmpty.right = new FormAttachment( 100, 0 );
    wStopOnEmpty.setLayoutData(fdStopOnEmpty);
    wStopOnEmpty.addSelectionListener( new ComponentSelectionListener( input ) );

    Label wlLimit = new Label(wContentComp, SWT.RIGHT);
    wlLimit.setText( BaseMessages.getString( PKG, "ExcelInputDialog.Limit.Label" ) );
    props.setLook(wlLimit);
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment( 0, 0 );
    fdlLimit.top = new FormAttachment( wStopOnEmpty, margin );
    fdlLimit.right = new FormAttachment( middle, -margin );
    wlLimit.setLayoutData(fdlLimit);
    wLimit = new Text(wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLimit );
    wLimit.addModifyListener( lsMod );
    FormData fdLimit = new FormData();
    fdLimit.left = new FormAttachment( middle, 0 );
    fdLimit.top = new FormAttachment( wStopOnEmpty, margin );
    fdLimit.right = new FormAttachment( 100, 0 );
    wLimit.setLayoutData(fdLimit);

    Label wlEncoding = new Label(wContentComp, SWT.RIGHT);
    wlEncoding.setText( BaseMessages.getString( PKG, "ExcelInputDialog.Encoding.Label" ) );
    props.setLook(wlEncoding);
    FormData fdlEncoding = new FormData();
    fdlEncoding.left = new FormAttachment( 0, 0 );
    fdlEncoding.top = new FormAttachment( wLimit, margin );
    fdlEncoding.right = new FormAttachment( middle, -margin );
    wlEncoding.setLayoutData(fdlEncoding);
    wEncoding = new CCombo(wContentComp, SWT.BORDER | SWT.READ_ONLY );
    wEncoding.setEditable( true );
    props.setLook( wEncoding );
    wEncoding.addModifyListener( lsMod );
    FormData fdEncoding = new FormData();
    fdEncoding.left = new FormAttachment( middle, 0 );
    fdEncoding.top = new FormAttachment( wLimit, margin );
    fdEncoding.right = new FormAttachment( 100, 0 );
    wEncoding.setLayoutData(fdEncoding);
    wEncoding.addFocusListener( new FocusListener() {
      @Override
      public void focusLost( FocusEvent e ) {
      }

      @Override
      public void focusGained( FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        setEncodings();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    //
    // START OF AddFileResult GROUP
    //
    Group wAddFileResult = new Group(wContentComp, SWT.SHADOW_NONE);
    props.setLook(wAddFileResult);
    wAddFileResult.setText( BaseMessages.getString( PKG, "ExcelInputDialog.AddFileResult.Label" ) );

    FormLayout AddFileResultgroupLayout = new FormLayout();
    AddFileResultgroupLayout.marginWidth = 10;
    AddFileResultgroupLayout.marginHeight = 10;
    wAddFileResult.setLayout( AddFileResultgroupLayout );

    Label wlAddResult = new Label(wAddFileResult, SWT.RIGHT);
    wlAddResult.setText( BaseMessages.getString( PKG, "ExcelInputDialog.AddResult.Label" ) );
    props.setLook(wlAddResult);
    FormData fdlAddResult = new FormData();
    fdlAddResult.left = new FormAttachment( 0, 0 );
    fdlAddResult.top = new FormAttachment( wEncoding, margin );
    fdlAddResult.right = new FormAttachment( middle, -margin );
    wlAddResult.setLayoutData(fdlAddResult);
    wAddResult = new Button(wAddFileResult, SWT.CHECK );
    props.setLook( wAddResult );
    wAddResult.setToolTipText( BaseMessages.getString( PKG, "ExcelInputDialog.AddResult.Tooltip" ) );
    FormData fdAddResult = new FormData();
    fdAddResult.left = new FormAttachment( middle, 0 );
    fdAddResult.top = new FormAttachment( wlAddResult, 0, SWT.CENTER );
    wAddResult.setLayoutData(fdAddResult);
    wAddResult.addSelectionListener( new ComponentSelectionListener( input ) );

    FormData fdAddFileResult = new FormData();
    fdAddFileResult.left = new FormAttachment( 0, margin );
    fdAddFileResult.top = new FormAttachment( wEncoding, margin );
    fdAddFileResult.right = new FormAttachment( 100, -margin );
    wAddFileResult.setLayoutData(fdAddFileResult);

    //
    // / END OF AddFileResult GROUP
    //
    FormData fdContentComp = new FormData();
    fdContentComp.left = new FormAttachment( 0, 0 );
    fdContentComp.top = new FormAttachment( 0, 0 );
    fdContentComp.right = new FormAttachment( 100, 0 );
    fdContentComp.bottom = new FormAttachment( 100, 0 );
    wContentComp.setLayoutData(fdContentComp);

    wContentComp.layout();
    wContentTab.setControl(wContentComp);

    //
    // / END OF CONTENT TAB
    //
    //
    // / START OF CONTENT TAB
    //
    addErrorTab();

    // Fields tab...
    //
    wFieldsTab = new CTabItem( wTabFolder, SWT.NONE );
    wFieldsTab.setText( BaseMessages.getString( PKG, "ExcelInputDialog.FieldsTab.TabTitle" ) );

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    wFieldsComp.setLayout( fieldsLayout );

    wbGetFields = new Button(wFieldsComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbGetFields );
    wbGetFields.setText( BaseMessages.getString( PKG, "ExcelInputDialog.GetFields.Button" ) );

    setButtonPositions( new Button[] { wbGetFields }, margin, null );

    final int FieldsRows = input.getField().length;
    int FieldsWidth = 600;
    int FieldsHeight = 150;

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "ExcelInputDialog.Name.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "ExcelInputDialog.Type.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
          ValueMetaFactory.getValueMetaNames() ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "ExcelInputDialog.Length.Column" ), ColumnInfo.COLUMN_TYPE_TEXT,
          false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "ExcelInputDialog.Precision.Column" ), ColumnInfo.COLUMN_TYPE_TEXT,
          false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "ExcelInputDialog.TrimType.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
          ValueMetaString.trimTypeDesc ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "ExcelInputDialog.Repeat.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
          new String[] {
            BaseMessages.getString( PKG, "System.Combo.Yes" ),
            BaseMessages.getString( PKG, "System.Combo.No" ) } ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "ExcelInputDialog.Format.Column" ), ColumnInfo.COLUMN_TYPE_FORMAT, 2 ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "ExcelInputDialog.Currency.Column" ), ColumnInfo.COLUMN_TYPE_TEXT ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "ExcelInputDialog.Decimal.Column" ), ColumnInfo.COLUMN_TYPE_TEXT ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "ExcelInputDialog.Grouping.Column" ), ColumnInfo.COLUMN_TYPE_TEXT ) };

    colinf[ 5 ].setToolTip( BaseMessages.getString( PKG, "ExcelInputDialog.Repeat.Tooltip" ) );

    wFields =
      new TableView( variables, wFieldsComp, SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );
    wFields.setSize( FieldsWidth, FieldsHeight );
    wFields.addModifyListener( arg0 -> checkAlerts() );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( 0, 0 );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wbGetFields, -margin );
    wFields.setLayoutData(fdFields);

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment( 0, 0 );
    fdFieldsComp.top = new FormAttachment( 0, 0 );
    fdFieldsComp.right = new FormAttachment( 100, 0 );
    fdFieldsComp.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData(fdFieldsComp);

    wFieldsComp.layout();
    wFieldsTab.setControl(wFieldsComp);
    props.setLook(wFieldsComp);

    addAdditionalFieldsTab();

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wlStatusMessage, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOk, -2*margin );
    wTabFolder.setLayoutData(fdTabFolder);



    // Add listeners

    lsDef = new SelectionAdapter() {
      @Override
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );
    wFilename.addSelectionListener( lsDef );
    wLimit.addSelectionListener( lsDef );
    wInclRownumField.addSelectionListener( lsDef );
    wInclFilenameField.addSelectionListener( lsDef );
    wInclSheetnameField.addSelectionListener( lsDef );
    wAccField.addSelectionListener( lsDef );

    // Add the file to the list of files...
    SelectionAdapter selA = new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        wFilenameList.add( new String[] {
          wFilename.getText(), wFilemask.getText(), wExcludeFilemask.getText(),
          ExcelInputMeta.RequiredFilesCode[ 0 ], ExcelInputMeta.RequiredFilesCode[ 0 ] } );
        wFilename.setText( "" );
        wFilemask.setText( "" );
        wExcludeFilemask.setText( "" );
        wFilenameList.removeEmptyRows();
        wFilenameList.setRowNums();
        wFilenameList.optWidth( true );
        checkAlerts();
      }
    };
    wbaFilename.addSelectionListener( selA );
    wFilename.addSelectionListener( selA );

    // Delete files from the list of files...
    wbdFilename.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        int[] idx = wFilenameList.getSelectionIndices();
        wFilenameList.remove( idx );
        wFilenameList.removeEmptyRows();
        wFilenameList.setRowNums();
        checkAlerts();
      }
    } );

    // Edit the selected file & remove from the list...
    wbeFilename.addSelectionListener( new SelectionAdapter() {
      @Override
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
      }
    } );

    // Show the files that are selected at this time...
    wbShowFiles.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        showFiles();
      }
    } );

    // Whenever something changes, set the tooltip to the expanded version of the filename:
    wFilename.addModifyListener( e -> wFilename.setToolTipText( variables.resolve( wFilename.getText() ) ) );

    // Listen to the Browse... button
    wbbFilename.addListener( SWT.Selection, e -> {
      if ( !Utils.isEmpty( wFilemask.getText() ) || !Utils.isEmpty( wExcludeFilemask.getText() ) ) { // A mask: a directory!
        BaseDialog.presentDirectoryDialog( shell, wFilename, variables );
      } else {
        String[] extentions;
        SpreadSheetType type = SpreadSheetType.getStpreadSheetTypeByDescription( wSpreadSheetType.getText() );
        switch ( type ) {
          case SAX_POI:
            extentions = new String[] { "*.xlsx;*.XLSX", "*" };
            break;
          case ODS:
            extentions = new String[] { "*.ods;*.ODS;", "*" };
            break;
          case POI:
          default:
            extentions = new String[] { "*.xls;*.XLS;*.xlsx;*.XLSX", "*" };
            break;
        }

        BaseDialog.presentFileDialog( shell, wFilename, variables,
          extentions,
          new String[] {
            BaseMessages.getString( PKG, "ExcelInputDialog.FilterNames.ExcelFiles" ),
            BaseMessages.getString( PKG, "System.FileType.AllFiles" ) },
          true
        );
      }
    } );

    // Get a list of the sheetnames.
    wbGetSheets.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        getSheets();
      }
    } );

    wbGetFields.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        getFields();
      }
    } );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      @Override
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    wTabFolder.setSelection( 0 );

    // Set the shell size, based upon previous time...
    setSize();
    getData( input );
    input.setChanged( changed );
    wFields.optWidth( true );
    checkAlerts(); // resyncing after setup

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  public void setFlags() {
    wbGetFields.setEnabled( wHeader.getSelection() );

    boolean accept = wAccFilenames.getSelection();
    wlAccField.setEnabled( accept );
    wAccField.setEnabled( accept );
    wlAccTransform.setEnabled( accept );
    wAccTransform.setEnabled( accept );

    wlFilename.setEnabled( !accept );
    wbbFilename.setEnabled( !accept ); // Browse: add file or directory
    wbdFilename.setEnabled( !accept ); // Delete
    wbeFilename.setEnabled( !accept ); // Edit
    wbaFilename.setEnabled( !accept ); // Add or change
    wFilename.setEnabled( !accept );
    wlFilenameList.setEnabled( !accept );
    wFilenameList.setEnabled( !accept );
    wlFilemask.setEnabled( !accept );
    wlExcludeFilemask.setEnabled( !accept );
    wExcludeFilemask.setEnabled( !accept );
    wFilemask.setEnabled( !accept );
    wbShowFiles.setEnabled( !accept );

    // wPreview.setEnabled(!accept); // Keep this one: you can do preview on defined files in the files section.

    // Error handling tab...
    wlSkipErrorLines.setEnabled( wErrorIgnored.getSelection() );
    wSkipErrorLines.setEnabled( wErrorIgnored.getSelection() );

    wlErrorDestDir.setEnabled( wErrorIgnored.getSelection() );
    wErrorDestDir.setEnabled( wErrorIgnored.getSelection() );
    wlErrorExt.setEnabled( wErrorIgnored.getSelection() );
    wErrorExt.setEnabled( wErrorIgnored.getSelection() );
    wbbErrorDestDir.setEnabled( wErrorIgnored.getSelection() );
    wbvErrorDestDir.setEnabled( wErrorIgnored.getSelection() );

    wlWarningDestDir.setEnabled( wErrorIgnored.getSelection() );
    wWarningDestDir.setEnabled( wErrorIgnored.getSelection() );
    wlWarningExt.setEnabled( wErrorIgnored.getSelection() );
    wWarningExt.setEnabled( wErrorIgnored.getSelection() );
    wbbWarningDestDir.setEnabled( wErrorIgnored.getSelection() );
    wbvWarningDestDir.setEnabled( wErrorIgnored.getSelection() );

    wlLineNrDestDir.setEnabled( wErrorIgnored.getSelection() );
    wLineNrDestDir.setEnabled( wErrorIgnored.getSelection() );
    wlLineNrExt.setEnabled( wErrorIgnored.getSelection() );
    wLineNrExt.setEnabled( wErrorIgnored.getSelection() );
    wbbLineNrDestDir.setEnabled( wErrorIgnored.getSelection() );
    wbvLineNrDestDir.setEnabled( wErrorIgnored.getSelection() );
  }

  /**
   * Read the data from the ExcelInputMeta object and show it in this dialog.
   *
   * @param meta The ExcelInputMeta object to obtain the data from.
   */
  public void getData( ExcelInputMeta meta ) {
    if ( meta.getFileName() != null ) {
      wFilenameList.removeAll();

      for ( int i = 0; i < meta.getFileName().length; i++ ) {
        wFilenameList.add( new String[] {
          meta.getFileName()[ i ], meta.getFileMask()[ i ], meta.getExcludeFileMask()[ i ],
          meta.getRequiredFilesDesc( meta.getFileRequired()[ i ] ),
          meta.getRequiredFilesDesc( meta.getIncludeSubFolders()[ i ] ) } );
      }
      wFilenameList.removeEmptyRows();
      wFilenameList.setRowNums();
      wFilenameList.optWidth( true );
    }

    wAccFilenames.setSelection( meta.isAcceptingFilenames() );

    if ( meta.getAcceptingField() != null && !meta.getAcceptingField().equals( "" ) ) {
      wAccField.select( wAccField.indexOf( meta.getAcceptingField() ) );
    }
    if ( meta.getAcceptingTransformName() != null && !meta.getAcceptingTransformName().equals( "" ) ) {
      wAccTransform.select( wAccTransform.indexOf( meta.getAcceptingTransformName() ) );
    }

    wHeader.setSelection( meta.startsWithHeader() );
    wNoEmpty.setSelection( meta.ignoreEmptyRows() );
    wStopOnEmpty.setSelection( meta.stopOnEmpty() );
    if ( meta.getFileField() != null ) {
      wInclFilenameField.setText( meta.getFileField() );
    }
    if ( meta.getSheetField() != null ) {
      wInclSheetnameField.setText( meta.getSheetField() );
    }
    if ( meta.getSheetRowNumberField() != null ) {
      wInclSheetRownumField.setText( meta.getSheetRowNumberField() );
    }
    if ( meta.getRowNumberField() != null ) {
      wInclRownumField.setText( meta.getRowNumberField() );
    }
    wLimit.setText( "" + meta.getRowLimit() );
    wEncoding.setText( Const.NVL( meta.getEncoding(), "" ) );
    wSpreadSheetType.setText( meta.getSpreadSheetType().getDescription() );
    wAddResult.setSelection( meta.isAddResultFile() );

    if ( isDebug() ) {
      logDebug( "getting fields info..." );
    }
    for ( int i = 0; i < meta.getField().length; i++ ) {
      TableItem item = wFields.table.getItem( i );
      String field = meta.getField()[ i ].getName();
      String type = meta.getField()[ i ].getTypeDesc();
      String length = "" + meta.getField()[ i ].getLength();
      String prec = "" + meta.getField()[ i ].getPrecision();
      String trim = meta.getField()[ i ].getTrimTypeDesc();
      String rep =
        meta.getField()[ i ].isRepeated() ? BaseMessages.getString( PKG, "System.Combo.Yes" ) : BaseMessages
          .getString( PKG, "System.Combo.No" );
      String format = meta.getField()[ i ].getFormat();
      String currency = meta.getField()[ i ].getCurrencySymbol();
      String decimal = meta.getField()[ i ].getDecimalSymbol();
      String grouping = meta.getField()[ i ].getGroupSymbol();

      if ( field != null ) {
        item.setText( 1, field );
      }
      if ( type != null ) {
        item.setText( 2, type );
      }
      if ( length != null ) {
        item.setText( 3, length );
      }
      if ( prec != null ) {
        item.setText( 4, prec );
      }
      if ( trim != null ) {
        item.setText( 5, trim );
      }
      if ( rep != null ) {
        item.setText( 6, rep );
      }
      if ( format != null ) {
        item.setText( 7, format );
      }
      if ( currency != null ) {
        item.setText( 8, currency );
      }
      if ( decimal != null ) {
        item.setText( 9, decimal );
      }
      if ( grouping != null ) {
        item.setText( 10, grouping );
      }
    }

    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth( true );

    logDebug( "getting sheets info..." );
    for ( int i = 0; i < meta.getSheetName().length; i++ ) {
      TableItem item = wSheetnameList.table.getItem( i );
      String sheetname = meta.getSheetName()[ i ];
      String startrow = "" + meta.getStartRow()[ i ];
      String startcol = "" + meta.getStartColumn()[ i ];

      if ( sheetname != null ) {
        item.setText( 1, sheetname );
      }
      if ( startrow != null ) {
        item.setText( 2, startrow );
      }
      if ( startcol != null ) {
        item.setText( 3, startcol );
      }
    }
    wSheetnameList.removeEmptyRows();
    wSheetnameList.setRowNums();
    wSheetnameList.optWidth( true );

    // Error handling fields...
    wErrorIgnored.setSelection( meta.isErrorIgnored() );
    wStrictTypes.setSelection( meta.isStrictTypes() );
    wSkipErrorLines.setSelection( meta.isErrorLineSkipped() );

    if ( meta.getWarningFilesDestinationDirectory() != null ) {
      wWarningDestDir.setText( meta.getWarningFilesDestinationDirectory() );
    }
    if ( meta.getBadLineFilesExtension() != null ) {
      wWarningExt.setText( meta.getBadLineFilesExtension() );
    }

    if ( meta.getErrorFilesDestinationDirectory() != null ) {
      wErrorDestDir.setText( meta.getErrorFilesDestinationDirectory() );
    }
    if ( meta.getErrorFilesExtension() != null ) {
      wErrorExt.setText( meta.getErrorFilesExtension() );
    }

    if ( meta.getLineNumberFilesDestinationDirectory() != null ) {
      wLineNrDestDir.setText( meta.getLineNumberFilesDestinationDirectory() );
    }
    if ( meta.getLineNumberFilesExtension() != null ) {
      wLineNrExt.setText( meta.getLineNumberFilesExtension() );
    }
    if ( meta.getPathField() != null ) {
      wPathFieldName.setText( meta.getPathField() );
    }
    if ( meta.getShortFileNameField() != null ) {
      wShortFileFieldName.setText( meta.getShortFileNameField() );
    }

    if ( meta.getPathField() != null ) {
      wPathFieldName.setText( meta.getPathField() );
    }
    if ( meta.isHiddenField() != null ) {
      wIsHiddenName.setText( meta.isHiddenField() );
    }
    if ( meta.getLastModificationDateField() != null ) {
      wLastModificationTimeName.setText( meta.getLastModificationDateField() );
    }
    if ( meta.getUriField() != null ) {
      wUriName.setText( meta.getUriField() );
    }
    if ( meta.getRootUriField() != null ) {
      wRootUriName.setText( meta.getRootUriField() );
    }
    if ( meta.getExtensionField() != null ) {
      wExtensionFieldName.setText( meta.getExtensionField() );
    }
    if ( meta.getSizeField() != null ) {
      wSizeFieldName.setText( meta.getSizeField() );
    }

    setFlags();

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

  private void getInfo( ExcelInputMeta meta ) {
    TransformMeta currentTransformMeta = pipelineMeta.findTransform( transformName );
    transformName = wTransformName.getText(); // return value

    // copy info to Meta class (input)
    meta.setRowLimit( Const.toLong( wLimit.getText(), 0 ) );
    meta.setEncoding( wEncoding.getText() );
    meta.setSpreadSheetType( SpreadSheetType.values()[ wSpreadSheetType.getSelectionIndex() ] );
    meta.setFileField( wInclFilenameField.getText() );
    meta.setSheetField( wInclSheetnameField.getText() );
    meta.setSheetRowNumberField( wInclSheetRownumField.getText() );
    meta.setRowNumberField( wInclRownumField.getText() );

    meta.setAddResultFile( wAddResult.getSelection() );

    meta.setStartsWithHeader( wHeader.getSelection() );
    meta.setIgnoreEmptyRows( wNoEmpty.getSelection() );
    meta.setStopOnEmpty( wStopOnEmpty.getSelection() );

    meta.setAcceptingFilenames( wAccFilenames.getSelection() );
    meta.setAcceptingField( wAccField.getText() );
    meta.setAcceptingTransformName( wAccTransform.getText() );
    meta.searchInfoAndTargetTransforms( pipelineMeta.findPreviousTransforms( currentTransformMeta ) );

    int nrfiles = wFilenameList.nrNonEmpty();
    int nrsheets = wSheetnameList.nrNonEmpty();
    int nrFields = wFields.nrNonEmpty();

    meta.allocate( nrfiles, nrsheets, nrFields );

    meta.setFileName( wFilenameList.getItems( 0 ) );
    meta.setFileMask( wFilenameList.getItems( 1 ) );
    meta.setExcludeFileMask( wFilenameList.getItems( 2 ) );
    meta.setFileRequired( wFilenameList.getItems( 3 ) );
    meta.setIncludeSubFolders( wFilenameList.getItems( 4 ) );

    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrsheets; i++ ) {
      TableItem item = wSheetnameList.getNonEmpty( i );
      meta.getSheetName()[ i ] = item.getText( 1 );
      meta.getStartRow()[ i ] = Const.toInt( item.getText( 2 ), 0 );
      meta.getStartColumn()[ i ] = Const.toInt( item.getText( 3 ), 0 );
    }

    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wFields.getNonEmpty( i );
      meta.getField()[ i ] = new ExcelInputField();

      meta.getField()[ i ].setName( item.getText( 1 ) );
      meta.getField()[ i ].setType( ValueMetaFactory.getIdForValueMeta( item.getText( 2 ) ) );
      String slength = item.getText( 3 );
      String sprec = item.getText( 4 );
      meta.getField()[ i ].setTrimType( ExcelInputMeta.getTrimTypeByDesc( item.getText( 5 ) ) );
      meta.getField()[ i ].setRepeated( BaseMessages.getString( PKG, "System.Combo.Yes" ).equalsIgnoreCase(
        item.getText( 6 ) ) );

      meta.getField()[ i ].setLength( Const.toInt( slength, -1 ) );
      meta.getField()[ i ].setPrecision( Const.toInt( sprec, -1 ) );

      meta.getField()[ i ].setFormat( item.getText( 7 ) );
      meta.getField()[ i ].setCurrencySymbol( item.getText( 8 ) );
      meta.getField()[ i ].setDecimalSymbol( item.getText( 9 ) );
      meta.getField()[ i ].setGroupSymbol( item.getText( 10 ) );
    }

    // Error handling fields...
    meta.setStrictTypes( wStrictTypes.getSelection() );
    meta.setErrorIgnored( wErrorIgnored.getSelection() );
    meta.setErrorLineSkipped( wSkipErrorLines.getSelection() );

    meta.setWarningFilesDestinationDirectory( wWarningDestDir.getText() );
    meta.setBadLineFilesExtension( wWarningExt.getText() );
    meta.setErrorFilesDestinationDirectory( wErrorDestDir.getText() );
    meta.setErrorFilesExtension( wErrorExt.getText() );
    meta.setLineNumberFilesDestinationDirectory( wLineNrDestDir.getText() );
    meta.setLineNumberFilesExtension( wLineNrExt.getText() );
    meta.setShortFileNameField( wShortFileFieldName.getText() );
    meta.setPathField( wPathFieldName.getText() );
    meta.setIsHiddenField( wIsHiddenName.getText() );
    meta.setLastModificationDateField( wLastModificationTimeName.getText() );
    meta.setUriField( wUriName.getText() );
    meta.setRootUriField( wRootUriName.getText() );
    meta.setExtensionField( wExtensionFieldName.getText() );
    meta.setSizeField( wSizeFieldName.getText() );
  }

  private void addErrorTab() {
    //
    // START OF ERROR TAB /
    // /
    CTabItem wErrorTab = new CTabItem(wTabFolder, SWT.NONE);
    wErrorTab.setText( BaseMessages.getString( PKG, "ExcelInputDialog.ErrorTab.TabTitle" ) );

    FormLayout errorLayout = new FormLayout();
    errorLayout.marginWidth = 3;
    errorLayout.marginHeight = 3;

    Composite wErrorComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wErrorComp);
    wErrorComp.setLayout( errorLayout );

    // ERROR HANDLING...
    // ErrorIgnored?
    // ERROR HANDLING...
    Label wlStrictTypes = new Label(wErrorComp, SWT.RIGHT);
    wlStrictTypes.setText( BaseMessages.getString( PKG, "ExcelInputDialog.StrictTypes.Label" ) );
    props.setLook(wlStrictTypes);
    FormData fdlStrictTypes = new FormData();
    fdlStrictTypes.left = new FormAttachment( 0, 0 );
    fdlStrictTypes.top = new FormAttachment( 0, margin );
    fdlStrictTypes.right = new FormAttachment( middle, -margin );
    wlStrictTypes.setLayoutData(fdlStrictTypes);
    wStrictTypes = new Button(wErrorComp, SWT.CHECK );
    props.setLook( wStrictTypes );
    wStrictTypes.setToolTipText( BaseMessages.getString( PKG, "ExcelInputDialog.StrictTypes.Tooltip" ) );
    FormData fdStrictTypes = new FormData();
    fdStrictTypes.left = new FormAttachment( middle, 0 );
    fdStrictTypes.top = new FormAttachment( wlStrictTypes, 0, SWT.CENTER );
    wStrictTypes.setLayoutData(fdStrictTypes);
    Control previous = wStrictTypes;
    wStrictTypes.addSelectionListener( new ComponentSelectionListener( input ) );

    // ErrorIgnored?
    Label wlErrorIgnored = new Label(wErrorComp, SWT.RIGHT);
    wlErrorIgnored.setText( BaseMessages.getString( PKG, "ExcelInputDialog.ErrorIgnored.Label" ) );
    props.setLook(wlErrorIgnored);
    FormData fdlErrorIgnored = new FormData();
    fdlErrorIgnored.left = new FormAttachment( 0, 0 );
    fdlErrorIgnored.top = new FormAttachment( previous, margin );
    fdlErrorIgnored.right = new FormAttachment( middle, -margin );
    wlErrorIgnored.setLayoutData(fdlErrorIgnored);
    wErrorIgnored = new Button(wErrorComp, SWT.CHECK );
    props.setLook( wErrorIgnored );
    wErrorIgnored.setToolTipText( BaseMessages.getString( PKG, "ExcelInputDialog.ErrorIgnored.Tooltip" ) );
    FormData fdErrorIgnored = new FormData();
    fdErrorIgnored.left = new FormAttachment( middle, 0 );
    fdErrorIgnored.top = new FormAttachment( wlErrorIgnored, 0, SWT.CENTER );
    wErrorIgnored.setLayoutData(fdErrorIgnored);
    previous = wErrorIgnored;
    wErrorIgnored.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        setFlags();
        input.setChanged();
      }
    } );

    // Skip error lines?
    wlSkipErrorLines = new Label(wErrorComp, SWT.RIGHT );
    wlSkipErrorLines.setText( BaseMessages.getString( PKG, "ExcelInputDialog.SkipErrorLines.Label" ) );
    props.setLook( wlSkipErrorLines );
    FormData fdlSkipErrorLines = new FormData();
    fdlSkipErrorLines.left = new FormAttachment( 0, 0 );
    fdlSkipErrorLines.top = new FormAttachment( previous, margin );
    fdlSkipErrorLines.right = new FormAttachment( middle, -margin );
    wlSkipErrorLines.setLayoutData(fdlSkipErrorLines);
    wSkipErrorLines = new Button(wErrorComp, SWT.CHECK );
    props.setLook( wSkipErrorLines );
    wSkipErrorLines.setToolTipText( BaseMessages.getString( PKG, "ExcelInputDialog.SkipErrorLines.Tooltip" ) );
    FormData fdSkipErrorLines = new FormData();
    fdSkipErrorLines.left = new FormAttachment( middle, 0 );
    fdSkipErrorLines.top = new FormAttachment( wlSkipErrorLines, 0, SWT.CENTER );
    wSkipErrorLines.setLayoutData(fdSkipErrorLines);
    wSkipErrorLines.addSelectionListener( new ComponentSelectionListener( input ) );

    previous = wSkipErrorLines;

    // Bad lines files directory + extention

    // WarningDestDir line
    wlWarningDestDir = new Label(wErrorComp, SWT.RIGHT );
    wlWarningDestDir.setText( BaseMessages.getString( PKG, "ExcelInputDialog.WarningDestDir.Label" ) );
    props.setLook( wlWarningDestDir );
    FormData fdlWarningDestDir = new FormData();
    fdlWarningDestDir.left = new FormAttachment( 0, 0 );
    fdlWarningDestDir.top = new FormAttachment( previous, margin * 4 );
    fdlWarningDestDir.right = new FormAttachment( middle, -margin );
    wlWarningDestDir.setLayoutData(fdlWarningDestDir);

    wbbWarningDestDir = new Button(wErrorComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbbWarningDestDir );
    wbbWarningDestDir.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    wbbWarningDestDir.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.BrowseForDir" ) );
    FormData fdbWarningDestDir = new FormData();
    fdbWarningDestDir.right = new FormAttachment( 100, 0 );
    fdbWarningDestDir.top = new FormAttachment( previous, margin * 4 );
    wbbWarningDestDir.setLayoutData(fdbWarningDestDir);

    wbvWarningDestDir = new Button(wErrorComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbvWarningDestDir );
    wbvWarningDestDir.setText( BaseMessages.getString( PKG, "System.Button.Variable" ) );
    wbvWarningDestDir.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.VariableToDir" ) );
    FormData fdbvWarningDestDir = new FormData();
    fdbvWarningDestDir.right = new FormAttachment( wbbWarningDestDir, -margin );
    fdbvWarningDestDir.top = new FormAttachment( previous, margin * 4 );
    wbvWarningDestDir.setLayoutData(fdbvWarningDestDir);

    wWarningExt = new Text(wErrorComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wWarningExt );
    wWarningExt.addModifyListener( lsMod );
    FormData fdWarningDestExt = new FormData();
    fdWarningDestExt.left = new FormAttachment( wbvWarningDestDir, -150 );
    fdWarningDestExt.right = new FormAttachment( wbvWarningDestDir, -margin );
    fdWarningDestExt.top = new FormAttachment( previous, margin * 4 );
    wWarningExt.setLayoutData(fdWarningDestExt);

    wlWarningExt = new Label(wErrorComp, SWT.RIGHT );
    wlWarningExt.setText( BaseMessages.getString( PKG, "System.Label.Extension" ) );
    props.setLook( wlWarningExt );
    FormData fdlWarningDestExt = new FormData();
    fdlWarningDestExt.top = new FormAttachment( previous, margin * 4 );
    fdlWarningDestExt.right = new FormAttachment( wWarningExt, -margin );
    wlWarningExt.setLayoutData(fdlWarningDestExt);

    wWarningDestDir = new TextVar( variables, wErrorComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wWarningDestDir );
    wWarningDestDir.addModifyListener( lsMod );
    FormData fdWarningDestDir = new FormData();
    fdWarningDestDir.left = new FormAttachment( middle, 0 );
    fdWarningDestDir.right = new FormAttachment( wlWarningExt, -margin );
    fdWarningDestDir.top = new FormAttachment( previous, margin * 4 );
    wWarningDestDir.setLayoutData(fdWarningDestDir);

    // Listen to the Browse... button
    wbbWarningDestDir.addListener( SWT.Selection, e->BaseDialog.presentDirectoryDialog( shell, wWarningDestDir, variables) );

    // Listen to the Variable... button
    wbvWarningDestDir.addSelectionListener( VariableButtonListenerFactory.getSelectionAdapter(
      shell, wWarningDestDir, variables ) );

    // Whenever something changes, set the tooltip to the expanded version of the directory:
    wWarningDestDir.addModifyListener( getModifyListenerTooltipText( wWarningDestDir ) );

    // Error lines files directory + extention
    previous = wWarningDestDir;

    // ErrorDestDir line
    wlErrorDestDir = new Label(wErrorComp, SWT.RIGHT );
    wlErrorDestDir.setText( BaseMessages.getString( PKG, "ExcelInputDialog.ErrorDestDir.Label" ) );
    props.setLook( wlErrorDestDir );
    FormData fdlErrorDestDir = new FormData();
    fdlErrorDestDir.left = new FormAttachment( 0, 0 );
    fdlErrorDestDir.top = new FormAttachment( previous, margin );
    fdlErrorDestDir.right = new FormAttachment( middle, -margin );
    wlErrorDestDir.setLayoutData(fdlErrorDestDir);

    wbbErrorDestDir = new Button(wErrorComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbbErrorDestDir );
    wbbErrorDestDir.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    wbbErrorDestDir.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.BrowseForDir" ) );
    FormData fdbErrorDestDir = new FormData();
    fdbErrorDestDir.right = new FormAttachment( 100, 0 );
    fdbErrorDestDir.top = new FormAttachment( previous, margin );
    wbbErrorDestDir.setLayoutData(fdbErrorDestDir);

    wbvErrorDestDir = new Button(wErrorComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbvErrorDestDir );
    wbvErrorDestDir.setText( BaseMessages.getString( PKG, "System.Button.Variable" ) );
    wbvErrorDestDir.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.VariableToDir" ) );
    FormData fdbvErrorDestDir = new FormData();
    fdbvErrorDestDir.right = new FormAttachment( wbbErrorDestDir, -margin );
    fdbvErrorDestDir.top = new FormAttachment( previous, margin );
    wbvErrorDestDir.setLayoutData(fdbvErrorDestDir);

    wErrorExt = new Text(wErrorComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wErrorExt );
    wErrorExt.addModifyListener( lsMod );
    FormData fdErrorDestExt = new FormData();
    fdErrorDestExt.left = new FormAttachment( wbvErrorDestDir, -150 );
    fdErrorDestExt.right = new FormAttachment( wbvErrorDestDir, -margin );
    fdErrorDestExt.top = new FormAttachment( previous, margin );
    wErrorExt.setLayoutData(fdErrorDestExt);

    wlErrorExt = new Label(wErrorComp, SWT.RIGHT );
    wlErrorExt.setText( BaseMessages.getString( PKG, "System.Label.Extension" ) );
    props.setLook( wlErrorExt );
    FormData fdlErrorDestExt = new FormData();
    fdlErrorDestExt.top = new FormAttachment( previous, margin );
    fdlErrorDestExt.right = new FormAttachment( wErrorExt, -margin );
    wlErrorExt.setLayoutData(fdlErrorDestExt);

    wErrorDestDir = new TextVar( variables, wErrorComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wErrorDestDir );
    wErrorDestDir.addModifyListener( lsMod );
    FormData fdErrorDestDir = new FormData();
    fdErrorDestDir.left = new FormAttachment( middle, 0 );
    fdErrorDestDir.right = new FormAttachment( wlErrorExt, -margin );
    fdErrorDestDir.top = new FormAttachment( previous, margin );
    wErrorDestDir.setLayoutData(fdErrorDestDir);

    // Listen to the Browse... button
    wbbErrorDestDir.addSelectionListener( DirectoryDialogButtonListenerFactory.getSelectionAdapter(
      shell, wErrorDestDir ) );

    // Listen to the Variable... button
    wbvErrorDestDir.addSelectionListener( VariableButtonListenerFactory.getSelectionAdapter(
      shell, wErrorDestDir, variables ) );

    // Whenever something changes, set the tooltip to the expanded version of the directory:
    wErrorDestDir.addModifyListener( getModifyListenerTooltipText( wErrorDestDir ) );

    // Line numbers files directory + extention
    previous = wErrorDestDir;

    // LineNrDestDir line
    wlLineNrDestDir = new Label(wErrorComp, SWT.RIGHT );
    wlLineNrDestDir.setText( BaseMessages.getString( PKG, "ExcelInputDialog.LineNrDestDir.Label" ) );
    props.setLook( wlLineNrDestDir );
    FormData fdlLineNrDestDir = new FormData();
    fdlLineNrDestDir.left = new FormAttachment( 0, 0 );
    fdlLineNrDestDir.top = new FormAttachment( previous, margin );
    fdlLineNrDestDir.right = new FormAttachment( middle, -margin );
    wlLineNrDestDir.setLayoutData(fdlLineNrDestDir);

    wbbLineNrDestDir = new Button(wErrorComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbbLineNrDestDir );
    wbbLineNrDestDir.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    wbbLineNrDestDir.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.BrowseForDir" ) );
    FormData fdbLineNrDestDir = new FormData();
    fdbLineNrDestDir.right = new FormAttachment( 100, 0 );
    fdbLineNrDestDir.top = new FormAttachment( previous, margin );
    wbbLineNrDestDir.setLayoutData(fdbLineNrDestDir);

    wbvLineNrDestDir = new Button(wErrorComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbvLineNrDestDir );
    wbvLineNrDestDir.setText( BaseMessages.getString( PKG, "System.Button.Variable" ) );
    wbvLineNrDestDir.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.VariableToDir" ) );
    FormData fdbvLineNrDestDir = new FormData();
    fdbvLineNrDestDir.right = new FormAttachment( wbbLineNrDestDir, -margin );
    fdbvLineNrDestDir.top = new FormAttachment( previous, margin );
    wbvLineNrDestDir.setLayoutData(fdbvLineNrDestDir);

    wLineNrExt = new Text(wErrorComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLineNrExt );
    wLineNrExt.addModifyListener( lsMod );
    FormData fdLineNrDestExt = new FormData();
    fdLineNrDestExt.left = new FormAttachment( wbvLineNrDestDir, -150 );
    fdLineNrDestExt.right = new FormAttachment( wbvLineNrDestDir, -margin );
    fdLineNrDestExt.top = new FormAttachment( previous, margin );
    wLineNrExt.setLayoutData(fdLineNrDestExt);

    wlLineNrExt = new Label(wErrorComp, SWT.RIGHT );
    wlLineNrExt.setText( BaseMessages.getString( PKG, "System.Label.Extension" ) );
    props.setLook( wlLineNrExt );
    FormData fdlLineNrDestExt = new FormData();
    fdlLineNrDestExt.top = new FormAttachment( previous, margin );
    fdlLineNrDestExt.right = new FormAttachment( wLineNrExt, -margin );
    wlLineNrExt.setLayoutData(fdlLineNrDestExt);

    wLineNrDestDir = new TextVar( variables, wErrorComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLineNrDestDir );
    wLineNrDestDir.addModifyListener( lsMod );
    FormData fdLineNrDestDir = new FormData();
    fdLineNrDestDir.left = new FormAttachment( middle, 0 );
    fdLineNrDestDir.right = new FormAttachment( wlLineNrExt, -margin );
    fdLineNrDestDir.top = new FormAttachment( previous, margin );
    wLineNrDestDir.setLayoutData(fdLineNrDestDir);

    // Listen to the Browse... button
    wbbLineNrDestDir.addSelectionListener( DirectoryDialogButtonListenerFactory.getSelectionAdapter(
      shell, wLineNrDestDir ) );

    // Listen to the Variable... button
    wbvLineNrDestDir.addSelectionListener( VariableButtonListenerFactory.getSelectionAdapter(
      shell, wLineNrDestDir, variables ) );

    // Whenever something changes, set the tooltip to the expanded version of the directory:
    wLineNrDestDir.addModifyListener( getModifyListenerTooltipText( wLineNrDestDir ) );

    wErrorComp.layout();
    wErrorTab.setControl(wErrorComp);

    //
    // / END OF CONTENT TAB
    //
  }

  /**
   * Preview the data generated by this transform. This generates a pipeline using this transform & a dummy and previews it.
   */
  private void preview() {
    // Create the excel reader transform...
    ExcelInputMeta oneMeta = new ExcelInputMeta();
    getInfo( oneMeta );

    if ( oneMeta.isAcceptingFilenames() ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
      mb.setMessage( BaseMessages.getString( PKG, "ExcelInputDialog.Dialog.SpecifyASampleFile.Message" ) ); // Nothing
      // found
      // that
      // matches
      // your
      // criteria
      mb.setText( BaseMessages.getString( PKG, "ExcelInputDialog.Dialog.SpecifyASampleFile.Title" ) ); // Sorry!
      mb.open();
      return;
    }

    PipelineMeta previewMeta = PipelinePreviewFactory.generatePreviewPipeline( variables, pipelineMeta.getMetadataProvider(),
      oneMeta, wTransformName.getText() );

    EnterNumberDialog numberDialog =
      new EnterNumberDialog( shell, props.getDefaultPreviewSize(), BaseMessages.getString(
        PKG, "ExcelInputDialog.PreviewSize.DialogTitle" ), BaseMessages.getString(
        PKG, "ExcelInputDialog.PreviewSize.DialogMessage" ) );
    int previewSize = numberDialog.open();
    if ( previewSize > 0 ) {
      PipelinePreviewProgressDialog progressDialog =
        new PipelinePreviewProgressDialog(
          shell, variables, previewMeta, new String[] { wTransformName.getText() }, new int[] { previewSize } );
      progressDialog.open();

      Pipeline pipeline = progressDialog.getPipeline();
      String loggingText = progressDialog.getLoggingText();

      if ( !progressDialog.isCancelled() ) {
        if ( pipeline.getResult() != null && pipeline.getResult().getNrErrors() > 0 ) {
          EnterTextDialog etd =
            new EnterTextDialog(
              shell, BaseMessages.getString( PKG, "System.Dialog.PreviewError.Title" ), BaseMessages
              .getString( PKG, "System.Dialog.PreviewError.Message" ), loggingText, true );
          etd.setReadOnly();
          etd.open();
        }
      }

      PreviewRowsDialog prd =
        new PreviewRowsDialog(
          shell, variables, SWT.NONE, wTransformName.getText(), progressDialog.getPreviewRowsMeta( wTransformName
          .getText() ), progressDialog.getPreviewRows( wTransformName.getText() ), loggingText );
      prd.open();
    }
  }

  /**
   * Get the names of the sheets from the Excel workbooks and let the user select some or all of them.
   */
  public void getSheets() {
    List<String> sheetnames = new ArrayList<>();

    ExcelInputMeta info = new ExcelInputMeta();
    getInfo( info );

    FileInputList fileList = info.getFileList( variables );
    for ( FileObject fileObject : fileList.getFiles() ) {
      try {
        IKWorkbook workbook =
          WorkbookFactory.getWorkbook( info.getSpreadSheetType(), HopVfs.getFilename( fileObject ), info
            .getEncoding() );

        int nrSheets = workbook.getNumberOfSheets();
        for ( int j = 0; j < nrSheets; j++ ) {
          IKSheet sheet = workbook.getSheet( j );
          String sheetname = sheet.getName();

          if ( Const.indexOfString( sheetname, sheetnames ) < 0 ) {
            sheetnames.add( sheetname );
          }
        }

        workbook.close();
      } catch ( Exception e ) {
        new ErrorDialog(
          shell, BaseMessages.getString( PKG, "System.Dialog.Error.Title" ), BaseMessages.getString(
          PKG, "ExcelInputDialog.ErrorReadingFile.DialogMessage", HopVfs.getFilename( fileObject ) ), e );
      }
    }

    // Put it in an array:
    String[] lst = sheetnames.toArray( new String[ sheetnames.size() ] );

    // Let the user select the sheet-names...
    EnterListDialog esd = new EnterListDialog( shell, SWT.NONE, lst );
    String[] selection = esd.open();
    if ( selection != null ) {
      for (String s : selection) {
        wSheetnameList.add(new String[]{s, ""});
      }
      wSheetnameList.removeEmptyRows();
      wSheetnameList.setRowNums();
      wSheetnameList.optWidth( true );
      checkAlerts();
    }
  }

  /**
   * Get the list of fields in the Excel workbook and put the result in the fields table view.
   */
  public void getFields() {
    IRowMeta fields = new RowMeta();

    ExcelInputMeta info = new ExcelInputMeta();
    getInfo( info );

    int clearFields = SWT.YES;
    if ( wFields.nrNonEmpty() > 0 ) {
      MessageBox messageBox = new MessageBox( shell, SWT.YES | SWT.NO | SWT.CANCEL | SWT.ICON_QUESTION );
      messageBox.setMessage( BaseMessages.getString( PKG, "ExcelInputDialog.ClearFieldList.DialogMessage" ) );
      messageBox.setText( BaseMessages.getString( PKG, "ExcelInputDialog.ClearFieldList.DialogTitle" ) );
      clearFields = messageBox.open();
      if ( clearFields == SWT.CANCEL ) {
        return;
      }
    }

    FileInputList fileList = info.getFileList( variables );
    for ( FileObject file : fileList.getFiles() ) {
      try {
        IKWorkbook workbook =
          WorkbookFactory.getWorkbook( info.getSpreadSheetType(), HopVfs.getFilename( file ), info
            .getEncoding() );
        processingWorkbook( fields, info, workbook );
        workbook.close();
      } catch ( Exception e ) {
        new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.Error.Title" ), BaseMessages
          .getString( PKG, "ExcelInputDialog.ErrorReadingFile2.DialogMessage", HopVfs.getFilename( file ), e
            .toString() ), e );
      }
    }

    if ( fields.size() > 0 ) {
      if ( clearFields == SWT.YES ) {
        wFields.clearAll( false );
      }
      for ( int j = 0; j < fields.size(); j++ ) {
        IValueMeta field = fields.getValueMeta( j );
        wFields.add( new String[] { field.getName(), field.getTypeDesc(), "", "", "none", "N" } );
      }
      wFields.removeEmptyRows();
      wFields.setRowNums();
      wFields.optWidth( true );
    } else {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_WARNING );
      mb.setMessage( BaseMessages.getString( PKG, "ExcelInputDialog.UnableToFindFields.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "ExcelInputDialog.UnableToFindFields.DialogTitle" ) );
      mb.open();
    }
    checkAlerts();
  }

  /**
   * Processing excel workbook, filling fields
   *
   * @param fields   IRowMeta for filling fields
   * @param info     ExcelInputMeta
   * @param workbook excel workbook for processing
   * @throws HopPluginException
   */
  private void processingWorkbook( IRowMeta fields, ExcelInputMeta info, IKWorkbook workbook ) throws HopPluginException {
    int nrSheets = workbook.getNumberOfSheets();
    for ( int j = 0; j < nrSheets; j++ ) {
      IKSheet sheet = workbook.getSheet( j );

      // See if it's a selected sheet:
      int sheetIndex;
      if ( info.readAllSheets() ) {
        sheetIndex = 0;
      } else {
        sheetIndex = Const.indexOfString( sheet.getName(), info.getSheetName() );
      }
      if ( sheetIndex >= 0 ) {
        // We suppose it's the complete range we're looking for...
        //
        int rownr = 0;
        int startcol = 0;

        if ( info.readAllSheets() ) {
          if ( info.getStartColumn().length == 1 ) {
            startcol = info.getStartColumn()[ 0 ];
          }
          if ( info.getStartRow().length == 1 ) {
            rownr = info.getStartRow()[ 0 ];
          }
        } else {
          rownr = info.getStartRow()[ sheetIndex ];
          startcol = info.getStartColumn()[ sheetIndex ];
        }

        boolean stop = false;
        for ( int colnr = startcol; !stop; colnr++ ) {
          try {
            String fieldname = null;
            int fieldtype = IValueMeta.TYPE_NONE;

            IKCell cell = sheet.getCell( colnr, rownr );
            if ( cell == null ) {
              stop = true;
            } else {
              if ( cell.getType() != KCellType.EMPTY ) {
                // We found a field.
                fieldname = cell.getContents();
              }

              IKCell below = sheet.getCell( colnr, rownr + 1 );

              if ( below != null ) {
                if ( below.getType() == KCellType.BOOLEAN ) {
                  fieldtype = IValueMeta.TYPE_BOOLEAN;
                } else if ( below.getType() == KCellType.DATE ) {
                  fieldtype = IValueMeta.TYPE_DATE;
                } else if ( below.getType() == KCellType.LABEL ) {
                  fieldtype = IValueMeta.TYPE_STRING;
                } else if ( below.getType() == KCellType.NUMBER ) {
                  fieldtype = IValueMeta.TYPE_NUMBER;
                } else {
                  fieldtype = IValueMeta.TYPE_STRING;
                }
              } else {
                fieldtype = IValueMeta.TYPE_STRING;
              }

              if ( Utils.isEmpty( fieldname ) ) {
                stop = true;
              } else {
                if ( fieldtype != IValueMeta.TYPE_NONE ) {
                  IValueMeta field = ValueMetaFactory.createValueMeta( fieldname, fieldtype );
                  fields.addValueMeta( field );
                }
              }
            }
          } catch ( ArrayIndexOutOfBoundsException aioobe ) {
            stop = true;
          }
        }
      }
    }
  }

  private void showFiles() {
    ExcelInputMeta eii = new ExcelInputMeta();
    getInfo( eii );
    String[] files = eii.getFilePaths(HopGui.getInstance().getVariables());
    if ( files.length > 0 ) {
      EnterSelectionDialog esd =
        new EnterSelectionDialog( shell, files,
          BaseMessages.getString( PKG, "ExcelInputDialog.FilesRead.DialogTitle" ),
          BaseMessages.getString( PKG, "ExcelInputDialog.FilesRead.DialogMessage" ) );
      esd.setViewOnly();
      esd.open();
    } else {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "ExcelInputDialog.NoFilesFound.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
      mb.open();
    }
  }

  private void setEncodings() {
    // Encoding of the text file:
    if ( !gotEncodings ) {
      gotEncodings = true;

      wEncoding.removeAll();

      List<Charset> values = new ArrayList<>(Charset.availableCharsets().values());
      for (Charset charSet : values) {
        wEncoding.add(charSet.displayName());
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
   * It is perfectly permissible to put away an incomplete transform definition. However, to assist the user in setting up
   * the full kit, this method is invoked whenever data changes in the dialog. It scans the dialog's model looking for
   * missing and/or inconsistent data. Tabs needing attention are visually flagged and attention messages are displayed
   * in the statusMessage line (a la Eclipse).
   * <p>
   * Since there's only one statusMessage line, messages are prioritized. As each higher-level item is corrected, the
   * next lower level message is displayed.
   *
   * @author Tim Holloway <timh@mousetech.com>
   * @since 15-FEB-2008
   */
  private void checkAlerts() {
    logDebug( "checkAlerts" );
    // # Check the fields tab. At least one field is required.
    // # Check the Sheets tab. At least one sheet is required.
    // # Check the Files tab.

    final boolean fieldsOk = wFields.nrNonEmpty() != 0;
    final boolean sheetsOk = wSheetnameList.nrNonEmpty() != 0;
    final boolean filesOk =
      wFilenameList.nrNonEmpty() != 0
        || ( wAccFilenames.getSelection() && !Utils.isEmpty( wAccField.getText() ) );
    String msgText = ""; // Will clear status if no actions.

    // Assign the highest-priority action message.
    if ( !fieldsOk ) {
      // TODO: NLS
      msgText = ( BaseMessages.getString( PKG, "ExcelInputDialog.AddFields" ) );
    } else if ( !sheetsOk ) {
      // TODO: NLS
      msgText = ( BaseMessages.getString( PKG, "ExcelInputDialog.AddSheets" ) );
    } else if ( !filesOk ) {
      // TODO: NLS
      msgText = ( BaseMessages.getString( PKG, "ExcelInputDialog.AddFilenames" ) );
    }
    tagTab( !fieldsOk, wFieldsTab, BaseMessages.getString( PKG, "ExcelInputDialog.FieldsTab.TabTitle" ) );
    tagTab( !sheetsOk, wSheetTab, BaseMessages.getString( PKG, "ExcelInputDialog.SheetsTab.TabTitle" ) );
    tagTab( !filesOk, wFileTab, BaseMessages.getString( PKG, "ExcelInputDialog.FileTab.TabTitle" ) );

    wPreview.setEnabled( fieldsOk && sheetsOk && filesOk );

    wlStatusMessage.setText( msgText );
  }

  /**
   * Hilight (or not) tab to indicate if action is required.
   *
   * @param hilightMe  <code>true</code> to highlight, <code>false</code> if not.
   * @param tabItem    Tab to highlight
   * @param tabCaption Tab text (normally fetched from resource).
   */
  private void tagTab( boolean hilightMe, CTabItem tabItem, String tabCaption ) {
    if ( hilightMe ) {
      tabItem.setText( TAB_FLAG + tabCaption );
    } else {
      tabItem.setText( tabCaption );
    }
  }

  private void addAdditionalFieldsTab() {
    //
    // START OF ADDITIONAL FIELDS TAB /
    //
    CTabItem wAdditionalFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdditionalFieldsTab.setText( BaseMessages.getString( PKG, "ExcelInputDialog.AdditionalFieldsTab.TabTitle" ) );

    Composite wAdditionalFieldsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wAdditionalFieldsComp);

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 3;
    fieldsLayout.marginHeight = 3;
    wAdditionalFieldsComp.setLayout( fieldsLayout );

    Label wlInclFilenameField = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlInclFilenameField.setText( BaseMessages.getString( PKG, "ExcelInputDialog.InclFilenameField.Label" ) );
    props.setLook(wlInclFilenameField);
    FormData fdlInclFilenameField = new FormData();
    fdlInclFilenameField.left = new FormAttachment( 0, 0 );
    fdlInclFilenameField.top = new FormAttachment( wTransformName, margin );
    fdlInclFilenameField.right = new FormAttachment( middle, -margin );
    wlInclFilenameField.setLayoutData(fdlInclFilenameField);
    wInclFilenameField = new Text(wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclFilenameField );
    wInclFilenameField.addModifyListener( lsMod );
    FormData fdInclFilenameField = new FormData();
    fdInclFilenameField.left = new FormAttachment( middle, 0 );
    fdInclFilenameField.top = new FormAttachment( wTransformName, margin );
    fdInclFilenameField.right = new FormAttachment( 100, 0 );
    wInclFilenameField.setLayoutData(fdInclFilenameField);

    Label wlInclSheetnameField = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlInclSheetnameField.setText( BaseMessages.getString( PKG, "ExcelInputDialog.InclSheetnameField.Label" ) );
    props.setLook(wlInclSheetnameField);
    FormData fdlInclSheetnameField = new FormData();
    fdlInclSheetnameField.left = new FormAttachment( 0, 0 );
    fdlInclSheetnameField.top = new FormAttachment( wInclFilenameField, margin );
    fdlInclSheetnameField.right = new FormAttachment( middle, -margin );
    wlInclSheetnameField.setLayoutData(fdlInclSheetnameField);
    wInclSheetnameField = new Text(wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclSheetnameField );
    wInclSheetnameField.addModifyListener( lsMod );
    FormData fdInclSheetnameField = new FormData();
    fdInclSheetnameField.left = new FormAttachment( middle, 0 );
    fdInclSheetnameField.top = new FormAttachment( wInclFilenameField, margin );
    fdInclSheetnameField.right = new FormAttachment( 100, 0 );
    wInclSheetnameField.setLayoutData(fdInclSheetnameField);

    Label wlInclSheetRownumField = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlInclSheetRownumField.setText( BaseMessages.getString( PKG, "ExcelInputDialog.InclSheetRownumField.Label" ) );
    props.setLook(wlInclSheetRownumField);
    FormData fdlInclSheetRownumField = new FormData();
    fdlInclSheetRownumField.left = new FormAttachment( 0, 0 );
    fdlInclSheetRownumField.top = new FormAttachment( wInclSheetnameField, margin );
    fdlInclSheetRownumField.right = new FormAttachment( middle, -margin );
    wlInclSheetRownumField.setLayoutData(fdlInclSheetRownumField);
    wInclSheetRownumField = new Text(wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclSheetRownumField );
    wInclSheetRownumField.addModifyListener( lsMod );
    FormData fdInclSheetRownumField = new FormData();
    fdInclSheetRownumField.left = new FormAttachment( middle, 0 );
    fdInclSheetRownumField.top = new FormAttachment( wInclSheetnameField, margin );
    fdInclSheetRownumField.right = new FormAttachment( 100, 0 );
    wInclSheetRownumField.setLayoutData(fdInclSheetRownumField);

    Label wlInclRownumField = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlInclRownumField.setText( BaseMessages.getString( PKG, "ExcelInputDialog.InclRownumField.Label" ) );
    props.setLook(wlInclRownumField);
    FormData fdlInclRownumField = new FormData();
    fdlInclRownumField.left = new FormAttachment( 0, 0 );
    fdlInclRownumField.top = new FormAttachment( wInclSheetRownumField, margin );
    fdlInclRownumField.right = new FormAttachment( middle, -margin );
    wlInclRownumField.setLayoutData(fdlInclRownumField);
    wInclRownumField = new Text(wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclRownumField );
    wInclRownumField.addModifyListener( lsMod );
    FormData fdInclRownumField = new FormData();
    fdInclRownumField.left = new FormAttachment( middle, 0 );
    fdInclRownumField.top = new FormAttachment( wInclSheetRownumField, margin );
    fdInclRownumField.right = new FormAttachment( 100, 0 );
    wInclRownumField.setLayoutData(fdInclRownumField);

    // ShortFileFieldName line
    Label wlShortFileFieldName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlShortFileFieldName.setText( BaseMessages.getString( PKG, "ExcelInputDialog.ShortFileFieldName.Label" ) );
    props.setLook(wlShortFileFieldName);
    FormData fdlShortFileFieldName = new FormData();
    fdlShortFileFieldName.left = new FormAttachment( 0, 0 );
    fdlShortFileFieldName.top = new FormAttachment( wInclRownumField, margin );
    fdlShortFileFieldName.right = new FormAttachment( middle, -margin );
    wlShortFileFieldName.setLayoutData(fdlShortFileFieldName);

    wShortFileFieldName = new Text(wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wShortFileFieldName );
    wShortFileFieldName.addModifyListener( lsMod );
    FormData fdShortFileFieldName = new FormData();
    fdShortFileFieldName.left = new FormAttachment( middle, 0 );
    fdShortFileFieldName.right = new FormAttachment( 100, -margin );
    fdShortFileFieldName.top = new FormAttachment( wInclRownumField, margin );
    wShortFileFieldName.setLayoutData(fdShortFileFieldName);

    // ExtensionFieldName line
    Label wlExtensionFieldName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlExtensionFieldName.setText( BaseMessages.getString( PKG, "ExcelInputDialog.ExtensionFieldName.Label" ) );
    props.setLook(wlExtensionFieldName);
    FormData fdlExtensionFieldName = new FormData();
    fdlExtensionFieldName.left = new FormAttachment( 0, 0 );
    fdlExtensionFieldName.top = new FormAttachment( wShortFileFieldName, margin );
    fdlExtensionFieldName.right = new FormAttachment( middle, -margin );
    wlExtensionFieldName.setLayoutData(fdlExtensionFieldName);

    wExtensionFieldName = new Text(wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wExtensionFieldName );
    wExtensionFieldName.addModifyListener( lsMod );
    FormData fdExtensionFieldName = new FormData();
    fdExtensionFieldName.left = new FormAttachment( middle, 0 );
    fdExtensionFieldName.right = new FormAttachment( 100, -margin );
    fdExtensionFieldName.top = new FormAttachment( wShortFileFieldName, margin );
    wExtensionFieldName.setLayoutData(fdExtensionFieldName);

    // PathFieldName line
    Label wlPathFieldName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlPathFieldName.setText( BaseMessages.getString( PKG, "ExcelInputDialog.PathFieldName.Label" ) );
    props.setLook(wlPathFieldName);
    FormData fdlPathFieldName = new FormData();
    fdlPathFieldName.left = new FormAttachment( 0, 0 );
    fdlPathFieldName.top = new FormAttachment( wExtensionFieldName, margin );
    fdlPathFieldName.right = new FormAttachment( middle, -margin );
    wlPathFieldName.setLayoutData(fdlPathFieldName);

    wPathFieldName = new Text(wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPathFieldName );
    wPathFieldName.addModifyListener( lsMod );
    FormData fdPathFieldName = new FormData();
    fdPathFieldName.left = new FormAttachment( middle, 0 );
    fdPathFieldName.right = new FormAttachment( 100, -margin );
    fdPathFieldName.top = new FormAttachment( wExtensionFieldName, margin );
    wPathFieldName.setLayoutData(fdPathFieldName);

    // SizeFieldName line
    Label wlSizeFieldName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlSizeFieldName.setText( BaseMessages.getString( PKG, "ExcelInputDialog.SizeFieldName.Label" ) );
    props.setLook(wlSizeFieldName);
    FormData fdlSizeFieldName = new FormData();
    fdlSizeFieldName.left = new FormAttachment( 0, 0 );
    fdlSizeFieldName.top = new FormAttachment( wPathFieldName, margin );
    fdlSizeFieldName.right = new FormAttachment( middle, -margin );
    wlSizeFieldName.setLayoutData(fdlSizeFieldName);

    wSizeFieldName = new Text(wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSizeFieldName );
    wSizeFieldName.addModifyListener( lsMod );
    FormData fdSizeFieldName = new FormData();
    fdSizeFieldName.left = new FormAttachment( middle, 0 );
    fdSizeFieldName.right = new FormAttachment( 100, -margin );
    fdSizeFieldName.top = new FormAttachment( wPathFieldName, margin );
    wSizeFieldName.setLayoutData(fdSizeFieldName);

    // IsHiddenName line
    Label wlIsHiddenName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlIsHiddenName.setText( BaseMessages.getString( PKG, "ExcelInputDialog.IsHiddenName.Label" ) );
    props.setLook(wlIsHiddenName);
    FormData fdlIsHiddenName = new FormData();
    fdlIsHiddenName.left = new FormAttachment( 0, 0 );
    fdlIsHiddenName.top = new FormAttachment( wSizeFieldName, margin );
    fdlIsHiddenName.right = new FormAttachment( middle, -margin );
    wlIsHiddenName.setLayoutData(fdlIsHiddenName);

    wIsHiddenName = new Text(wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wIsHiddenName );
    wIsHiddenName.addModifyListener( lsMod );
    FormData fdIsHiddenName = new FormData();
    fdIsHiddenName.left = new FormAttachment( middle, 0 );
    fdIsHiddenName.right = new FormAttachment( 100, -margin );
    fdIsHiddenName.top = new FormAttachment( wSizeFieldName, margin );
    wIsHiddenName.setLayoutData(fdIsHiddenName);

    // LastModificationTimeName line
    Label wlLastModificationTimeName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlLastModificationTimeName.setText( BaseMessages.getString(
      PKG, "ExcelInputDialog.LastModificationTimeName.Label" ) );
    props.setLook(wlLastModificationTimeName);
    FormData fdlLastModificationTimeName = new FormData();
    fdlLastModificationTimeName.left = new FormAttachment( 0, 0 );
    fdlLastModificationTimeName.top = new FormAttachment( wIsHiddenName, margin );
    fdlLastModificationTimeName.right = new FormAttachment( middle, -margin );
    wlLastModificationTimeName.setLayoutData(fdlLastModificationTimeName);

    wLastModificationTimeName = new Text(wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLastModificationTimeName );
    wLastModificationTimeName.addModifyListener( lsMod );
    FormData fdLastModificationTimeName = new FormData();
    fdLastModificationTimeName.left = new FormAttachment( middle, 0 );
    fdLastModificationTimeName.right = new FormAttachment( 100, -margin );
    fdLastModificationTimeName.top = new FormAttachment( wIsHiddenName, margin );
    wLastModificationTimeName.setLayoutData(fdLastModificationTimeName);

    // UriName line
    Label wlUriName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlUriName.setText( BaseMessages.getString( PKG, "ExcelInputDialog.UriName.Label" ) );
    props.setLook(wlUriName);
    FormData fdlUriName = new FormData();
    fdlUriName.left = new FormAttachment( 0, 0 );
    fdlUriName.top = new FormAttachment( wLastModificationTimeName, margin );
    fdlUriName.right = new FormAttachment( middle, -margin );
    wlUriName.setLayoutData(fdlUriName);

    wUriName = new Text(wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUriName );
    wUriName.addModifyListener( lsMod );
    FormData fdUriName = new FormData();
    fdUriName.left = new FormAttachment( middle, 0 );
    fdUriName.right = new FormAttachment( 100, -margin );
    fdUriName.top = new FormAttachment( wLastModificationTimeName, margin );
    wUriName.setLayoutData(fdUriName);

    // RootUriName line
    Label wlRootUriName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlRootUriName.setText( BaseMessages.getString( PKG, "ExcelInputDialog.RootUriName.Label" ) );
    props.setLook(wlRootUriName);
    FormData fdlRootUriName = new FormData();
    fdlRootUriName.left = new FormAttachment( 0, 0 );
    fdlRootUriName.top = new FormAttachment( wUriName, margin );
    fdlRootUriName.right = new FormAttachment( middle, -margin );
    wlRootUriName.setLayoutData(fdlRootUriName);

    wRootUriName = new Text(wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wRootUriName );
    wRootUriName.addModifyListener( lsMod );
    FormData fdRootUriName = new FormData();
    fdRootUriName.left = new FormAttachment( middle, 0 );
    fdRootUriName.right = new FormAttachment( 100, -margin );
    fdRootUriName.top = new FormAttachment( wUriName, margin );
    wRootUriName.setLayoutData(fdRootUriName);

    FormData fdAdditionalFieldsComp = new FormData();
    fdAdditionalFieldsComp.left = new FormAttachment( 0, 0 );
    fdAdditionalFieldsComp.top = new FormAttachment( wTransformName, margin );
    fdAdditionalFieldsComp.right = new FormAttachment( 100, 0 );
    fdAdditionalFieldsComp.bottom = new FormAttachment( 100, 0 );
    wAdditionalFieldsComp.setLayoutData(fdAdditionalFieldsComp);

    wAdditionalFieldsComp.layout();
    wAdditionalFieldsTab.setControl(wAdditionalFieldsComp);

    //
    // / END OF ADDITIONAL FIELDS TAB
    //
  }
}
