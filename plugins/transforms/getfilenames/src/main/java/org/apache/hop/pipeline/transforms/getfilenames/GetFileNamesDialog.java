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

package org.apache.hop.pipeline.transforms.getfilenames;


import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
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
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
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

public class GetFileNamesDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = GetFileNamesMeta.class; // For Translator

  private static final String[] YES_NO_COMBO = new String[] {
    BaseMessages.getString( PKG, "System.Combo.No" ), BaseMessages.getString( PKG, "System.Combo.Yes" ) };

  private Button wDoNotFailIfNoFile;

  private Label wlFilename;

  private Button wbbFilename; // Browse: add file or directory

  private Button wbdFilename; // Delete

  private Button wbeFilename; // Edit

  private Button wbaFilename; // Add or change

  private TextVar wFilename;

  private Label wlFilenameList;

  private TableView wFilenameList;

  private Label wlExcludeFilemask;

  private TextVar wExcludeFilemask;

  private Label wlFilemask;

  private TextVar wFilemask;

  private Button wbShowFiles;

  private CCombo wFilterFileType;

  private final GetFileNamesMeta input;

  private Button wFileField;

  private Label wlFilenameField;
  private CCombo wFilenameField;

  private Label wlWildcardField;
  private CCombo wWildcardField;

  private Label wlExcludeWildcardField;
  private CCombo wExcludeWildcardField;

  private Label wlIncludeSubFolder;
  private Button wIncludeSubFolder;

  private Button wAddResult;

  private Label wlLimit;
  private Text wLimit;

  private Button wInclRownum;

  private TextVar wInclRownumField;

  private boolean getPreviousFields = false;

  public GetFileNamesDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (GetFileNamesMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Buttons at the bottom
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wPreview = new Button( shell, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.Preview.Button" ) );
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

    CTabFolder wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF FILE TAB ///
    // ////////////////////////
    CTabItem wFileTab = new CTabItem( wTabFolder, SWT.NONE );
    wFileTab.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.FileTab.TabTitle" ) );

    Composite wFileComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wFileComp );

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wFileComp.setLayout( fileLayout );

    // ///////////////////////////////
    // START OF Origin files GROUP //
    // ///////////////////////////////

    Group wOriginFiles = new Group( wFileComp, SWT.SHADOW_NONE );
    props.setLook( wOriginFiles );
    wOriginFiles.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.wOriginFiles.Label" ) );

    FormLayout OriginFilesgroupLayout = new FormLayout();
    OriginFilesgroupLayout.marginWidth = 10;
    OriginFilesgroupLayout.marginHeight = 10;
    wOriginFiles.setLayout( OriginFilesgroupLayout );

    // Is Filename defined in a Field
    Label wlFileField = new Label( wOriginFiles, SWT.RIGHT );
    wlFileField.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.FileField.Label" ) );
    props.setLook( wlFileField );
    FormData fdlFileField = new FormData();
    fdlFileField.left = new FormAttachment( 0, -margin );
    fdlFileField.top = new FormAttachment( 0, margin );
    fdlFileField.right = new FormAttachment( middle, -2 * margin );
    wlFileField.setLayoutData( fdlFileField );

    wFileField = new Button( wOriginFiles, SWT.CHECK );
    props.setLook( wFileField );
    wFileField.setToolTipText( BaseMessages.getString( PKG, "GetFileNamesDialog.FileField.Tooltip" ) );
    FormData fdFileField = new FormData();
    fdFileField.left = new FormAttachment( middle, -margin );
    fdFileField.top = new FormAttachment( wlFileField, 0, SWT.CENTER );
    wFileField.setLayoutData( fdFileField );
    SelectionAdapter lfilefield = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        activateFileField();
        setFileField();
        input.setChanged();
      }
    };
    wFileField.addListener( SWT.Selection, e -> {
      activateFileField();
      setFileField();
      input.setChanged();
    } );

    // Filename field
    wlFilenameField = new Label( wOriginFiles, SWT.RIGHT );
    wlFilenameField.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.wlFilenameField.Label" ) );
    props.setLook( wlFilenameField );
    FormData fdlFilenameField = new FormData();
    fdlFilenameField.left = new FormAttachment( 0, -margin );
    fdlFilenameField.top = new FormAttachment( wFileField, margin );
    fdlFilenameField.right = new FormAttachment( middle, -2 * margin );
    wlFilenameField.setLayoutData( fdlFilenameField );

    wFilenameField = new CCombo( wOriginFiles, SWT.BORDER | SWT.READ_ONLY );
    wFilenameField.setEditable( true );
    props.setLook( wFilenameField );
    wFilenameField.addModifyListener( lsMod );
    FormData fdFilenameField = new FormData();
    fdFilenameField.left = new FormAttachment( middle, -margin );
    fdFilenameField.top = new FormAttachment( wFileField, margin );
    fdFilenameField.right = new FormAttachment( 100, -margin );
    wFilenameField.setLayoutData( fdFilenameField );

    // Wildcard field
    wlWildcardField = new Label( wOriginFiles, SWT.RIGHT );
    wlWildcardField.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.wlWildcardField.Label" ) );
    props.setLook( wlWildcardField );
    FormData fdlWildcardField = new FormData();
    fdlWildcardField.left = new FormAttachment( 0, -margin );
    fdlWildcardField.top = new FormAttachment( wFilenameField, margin );
    fdlWildcardField.right = new FormAttachment( middle, -2 * margin );
    wlWildcardField.setLayoutData( fdlWildcardField );

    wWildcardField = new CCombo( wOriginFiles, SWT.BORDER | SWT.READ_ONLY );
    wWildcardField.setEditable( true );
    props.setLook( wWildcardField );
    wWildcardField.addModifyListener( lsMod );
    FormData fdWildcardField = new FormData();
    fdWildcardField.left = new FormAttachment( middle, -margin );
    fdWildcardField.top = new FormAttachment( wFilenameField, margin );
    fdWildcardField.right = new FormAttachment( 100, -margin );
    wWildcardField.setLayoutData( fdWildcardField );

    // ExcludeWildcard field
    wlExcludeWildcardField = new Label( wOriginFiles, SWT.RIGHT );
    wlExcludeWildcardField.setText( BaseMessages
      .getString( PKG, "GetFileNamesDialog.wlExcludeWildcardField.Label" ) );
    props.setLook( wlExcludeWildcardField );
    FormData fdlExcludeWildcardField = new FormData();
    fdlExcludeWildcardField.left = new FormAttachment( 0, -margin );
    fdlExcludeWildcardField.top = new FormAttachment( wWildcardField, margin );
    fdlExcludeWildcardField.right = new FormAttachment( middle, -2 * margin );
    wlExcludeWildcardField.setLayoutData( fdlExcludeWildcardField );

    wExcludeWildcardField = new CCombo( wOriginFiles, SWT.BORDER | SWT.READ_ONLY );
    wExcludeWildcardField.setEditable( true );
    props.setLook( wExcludeWildcardField );
    wExcludeWildcardField.addModifyListener( lsMod );
    FormData fdExcludeWildcardField = new FormData();
    fdExcludeWildcardField.left = new FormAttachment( middle, -margin );
    fdExcludeWildcardField.top = new FormAttachment( wWildcardField, margin );
    fdExcludeWildcardField.right = new FormAttachment( 100, -margin );
    wExcludeWildcardField.setLayoutData( fdExcludeWildcardField );

    // Is includeSubFoldername defined in a Field
    wlIncludeSubFolder = new Label( wOriginFiles, SWT.RIGHT );
    wlIncludeSubFolder.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.includeSubFolder.Label" ) );
    props.setLook( wlIncludeSubFolder );
    FormData fdlIncludeSubFolder = new FormData();
    fdlIncludeSubFolder.left = new FormAttachment( 0, -margin );
    fdlIncludeSubFolder.top = new FormAttachment( wExcludeWildcardField, margin );
    fdlIncludeSubFolder.right = new FormAttachment( middle, -2 * margin );
    wlIncludeSubFolder.setLayoutData( fdlIncludeSubFolder );

    wIncludeSubFolder = new Button( wOriginFiles, SWT.CHECK );
    props.setLook( wIncludeSubFolder );
    wIncludeSubFolder
      .setToolTipText( BaseMessages.getString( PKG, "GetFileNamesDialog.includeSubFolder.Tooltip" ) );
    FormData fdIncludeSubFolder = new FormData();
    fdIncludeSubFolder.left = new FormAttachment( middle, -margin );
    fdIncludeSubFolder.top = new FormAttachment( wlIncludeSubFolder, 0, SWT.CENTER );
    wIncludeSubFolder.setLayoutData( fdIncludeSubFolder );
    wIncludeSubFolder.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        input.setChanged();
      }
    } );

    FormData fdOriginFiles = new FormData();
    fdOriginFiles.left = new FormAttachment( 0, margin );
    fdOriginFiles.top = new FormAttachment( wFilenameList, margin );
    fdOriginFiles.right = new FormAttachment( 100, -margin );
    wOriginFiles.setLayoutData( fdOriginFiles );

    // ///////////////////////////////////////////////////////////
    // / END OF Origin files GROUP
    // ///////////////////////////////////////////////////////////

    // Filename line
    wlFilename = new Label( wFileComp, SWT.RIGHT );
    wlFilename.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.Filename.Label" ) );
    props.setLook( wlFilename );
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment( wOriginFiles, margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData( fdlFilename );

    wbbFilename = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbbFilename );
    wbbFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    wbbFilename.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.BrowseForFileOrDirAndAdd" ) );
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment( wOriginFiles, margin );
    wbbFilename.setLayoutData( fdbFilename );

    wbaFilename = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbaFilename );
    wbaFilename.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.FilenameAdd.Button" ) );
    wbaFilename.setToolTipText( BaseMessages.getString( PKG, "GetFileNamesDialog.FilenameAdd.Tooltip" ) );
    FormData fdbaFilename = new FormData();
    fdbaFilename.right = new FormAttachment( wbbFilename, -margin );
    fdbaFilename.top = new FormAttachment( wOriginFiles, margin );
    wbaFilename.setLayoutData( fdbaFilename );

    wFilename = new TextVar( variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.right = new FormAttachment( wbaFilename, -margin );
    fdFilename.top = new FormAttachment( wOriginFiles, margin );
    wFilename.setLayoutData( fdFilename );

    wlFilemask = new Label( wFileComp, SWT.RIGHT );
    wlFilemask.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.Filemask.Label" ) );
    props.setLook( wlFilemask );
    FormData fdlFilemask = new FormData();
    fdlFilemask.left = new FormAttachment( 0, 0 );
    fdlFilemask.top = new FormAttachment( wFilename, margin );
    fdlFilemask.right = new FormAttachment( middle, -margin );
    wlFilemask.setLayoutData( fdlFilemask );
    wFilemask = new TextVar( variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilemask );
    wFilemask.addModifyListener( lsMod );
    FormData fdFilemask = new FormData();
    fdFilemask.left = new FormAttachment( middle, 0 );
    fdFilemask.top = new FormAttachment( wFilename, margin );
    fdFilemask.right = new FormAttachment( wFilename, 0, SWT.RIGHT );
    wFilemask.setLayoutData( fdFilemask );

    wlExcludeFilemask = new Label( wFileComp, SWT.RIGHT );
    wlExcludeFilemask.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.ExcludeFilemask.Label" ) );
    props.setLook( wlExcludeFilemask );
    FormData fdlExcludeFilemask = new FormData();
    fdlExcludeFilemask.left = new FormAttachment( 0, 0 );
    fdlExcludeFilemask.top = new FormAttachment( wFilemask, margin );
    fdlExcludeFilemask.right = new FormAttachment( middle, -margin );
    wlExcludeFilemask.setLayoutData( fdlExcludeFilemask );
    wExcludeFilemask = new TextVar( variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wExcludeFilemask );
    wExcludeFilemask.addModifyListener( lsMod );
    FormData fdExcludeFilemask = new FormData();
    fdExcludeFilemask.left = new FormAttachment( middle, 0 );
    fdExcludeFilemask.top = new FormAttachment( wFilemask, margin );
    fdExcludeFilemask.right = new FormAttachment( wFilename, 0, SWT.RIGHT );
    wExcludeFilemask.setLayoutData( fdExcludeFilemask );

    // Filename list line
    wlFilenameList = new Label( wFileComp, SWT.RIGHT );
    wlFilenameList.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.FilenameList.Label" ) );
    props.setLook( wlFilenameList );
    FormData fdlFilenameList = new FormData();
    fdlFilenameList.left = new FormAttachment( 0, 0 );
    fdlFilenameList.top = new FormAttachment( wExcludeFilemask, margin );
    fdlFilenameList.right = new FormAttachment( middle, -margin );
    wlFilenameList.setLayoutData( fdlFilenameList );

    // Buttons to the right of the screen...
    wbdFilename = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbdFilename );
    wbdFilename.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.FilenameDelete.Button" ) );
    wbdFilename.setToolTipText( BaseMessages.getString( PKG, "GetFileNamesDialog.FilenameDelete.Tooltip" ) );
    FormData fdbdFilename = new FormData();
    fdbdFilename.right = new FormAttachment( 100, 0 );
    fdbdFilename.top = new FormAttachment( wExcludeFilemask, 40 );
    wbdFilename.setLayoutData( fdbdFilename );

    wbeFilename = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbeFilename );
    wbeFilename.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.FilenameEdit.Button" ) );
    wbeFilename.setToolTipText( BaseMessages.getString( PKG, "GetFileNamesDialog.FilenameEdit.Tooltip" ) );
    FormData fdbeFilename = new FormData();
    fdbeFilename.right = new FormAttachment( 100, 0 );
    fdbeFilename.left = new FormAttachment( wbdFilename, 0, SWT.LEFT );
    fdbeFilename.top = new FormAttachment( wbdFilename, margin );
    wbeFilename.setLayoutData( fdbeFilename );

    wbShowFiles = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbShowFiles );
    wbShowFiles.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.ShowFiles.Button" ) );
    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment( middle, 0 );
    fdbShowFiles.bottom = new FormAttachment( 100, 0 );
    wbShowFiles.setLayoutData( fdbShowFiles );

    ColumnInfo[] colinfo =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "GetFileNamesDialog.FileDirColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "GetFileNamesDialog.WildcardColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "GetFileNamesDialog.ExcludeWildcardColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "GetFileNamesDialog.Required.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, YES_NO_COMBO ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "GetFileNamesDialog.IncludeSubDirs.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, YES_NO_COMBO ) };

    colinfo[ 0 ].setUsingVariables( true );
    colinfo[ 1 ].setUsingVariables( true );
    colinfo[ 1 ].setToolTip( BaseMessages.getString( PKG, "GetFileNamesDialog.RegExpColumn.Column" ) );
    colinfo[ 2 ].setUsingVariables( true );
    colinfo[ 2 ].setToolTip( BaseMessages.getString( PKG, "GetFileNamesDialog.ExcludeRegExpColumn.Column" ) );
    colinfo[ 3 ].setToolTip( BaseMessages.getString( PKG, "GetFileNamesDialog.Required.Tooltip" ) );
    colinfo[ 4 ].setToolTip( BaseMessages.getString( PKG, "GetFileNamesDialog.IncludeSubDirs.ToolTip" ) );

    wFilenameList =
      new TableView(
        variables, wFileComp, SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER, colinfo, colinfo.length, lsMod,
        props );
    props.setLook( wFilenameList );
    FormData fdFilenameList = new FormData();
    fdFilenameList.left = new FormAttachment( middle, 0 );
    fdFilenameList.right = new FormAttachment( wbdFilename, -margin );
    fdFilenameList.top = new FormAttachment( wExcludeFilemask, margin );
    fdFilenameList.bottom = new FormAttachment( wbShowFiles, -margin );
    wFilenameList.setLayoutData( fdFilenameList );

    FormData fdFileComp = new FormData();
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

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wTransformName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOk, -2 * margin );
    wTabFolder.setLayoutData( fdTabFolder );

    // ////////////////////////
    // START OF Filter TAB ///
    // ////////////////////////
    CTabItem wFilterTab = new CTabItem( wTabFolder, SWT.NONE );
    wFilterTab.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.FilterTab.TabTitle" ) );

    Composite wFilterComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wFilterComp );

    FormLayout filesettingLayout = new FormLayout();
    filesettingLayout.marginWidth = 3;
    filesettingLayout.marginHeight = 3;
    wFilterComp.setLayout( fileLayout );

    // Filter File Type
    Label wlFilterFileType = new Label( wFilterComp, SWT.RIGHT );
    wlFilterFileType.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.FilterTab.FileType.Label" ) );
    props.setLook( wlFilterFileType );
    FormData fdlFilterFileType = new FormData();
    fdlFilterFileType.left = new FormAttachment( 0, 0 );
    fdlFilterFileType.right = new FormAttachment( middle, 0 );
    fdlFilterFileType.top = new FormAttachment( 0, 3 * margin );
    wlFilterFileType.setLayoutData( fdlFilterFileType );
    wFilterFileType = new CCombo( wFilterComp, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wFilterFileType.add( BaseMessages.getString( PKG, "GetFileNamesDialog.FilterTab.FileType.All.Label" ) );
    wFilterFileType.add( BaseMessages.getString( PKG, "GetFileNamesDialog.FilterTab.FileType.OnlyFile.Label" ) );
    wFilterFileType.add( BaseMessages.getString( PKG, "GetFileNamesDialog.FilterTab.FileType.OnlyFolder.Label" ) );
    // wFilterFileType.select(0); // +1: starts at -1
    props.setLook( wFilterFileType );
    FormData fdFilterFileType = new FormData();
    fdFilterFileType.left = new FormAttachment( middle, 0 );
    fdFilterFileType.top = new FormAttachment( 0, 3 * margin );
    fdFilterFileType.right = new FormAttachment( 100, 0 );
    wFilterFileType.setLayoutData( fdFilterFileType );
    wFilterFileType.addModifyListener( lsMod );

    // /////////////////////////////////
    // START OF Additional Fields GROUP
    // /////////////////////////////////

    Group wAdditionalGroup = new Group( wFilterComp, SWT.SHADOW_NONE );
    props.setLook( wAdditionalGroup );
    wAdditionalGroup.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.Group.AdditionalGroup.Label" ) );

    FormLayout additionalgroupLayout = new FormLayout();
    additionalgroupLayout.marginWidth = 10;
    additionalgroupLayout.marginHeight = 10;
    wAdditionalGroup.setLayout( additionalgroupLayout );

    Label wlInclRownum = new Label( wAdditionalGroup, SWT.RIGHT );
    wlInclRownum.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.InclRownum.Label" ) );
    props.setLook( wlInclRownum );
    FormData fdlInclRownum = new FormData();
    fdlInclRownum.left = new FormAttachment( 0, 0 );
    fdlInclRownum.top = new FormAttachment( wFilterFileType, 2 * margin );
    fdlInclRownum.right = new FormAttachment( middle, -margin );
    wlInclRownum.setLayoutData( fdlInclRownum );
    wInclRownum = new Button( wAdditionalGroup, SWT.CHECK );
    props.setLook( wInclRownum );
    wInclRownum.setToolTipText( BaseMessages.getString( PKG, "GetFileNamesDialog.InclRownum.Tooltip" ) );
    FormData fdRownum = new FormData();
    fdRownum.left = new FormAttachment( middle, 0 );
    fdRownum.top = new FormAttachment( wlInclRownum, 0, SWT.CENTER );
    wInclRownum.setLayoutData( fdRownum );
    wInclRownum.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        input.setChanged();
      }
    } );

    Label wlInclRownumField = new Label( wAdditionalGroup, SWT.RIGHT );
    wlInclRownumField.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.InclRownumField.Label" ) );
    props.setLook( wlInclRownumField );
    FormData fdlInclRownumField = new FormData();
    fdlInclRownumField.left = new FormAttachment( wInclRownum, margin );
    fdlInclRownumField.top = new FormAttachment( wFilterFileType, 2 * margin );
    wlInclRownumField.setLayoutData( fdlInclRownumField );
    wInclRownumField = new TextVar( variables, wAdditionalGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclRownumField );
    wInclRownumField.addModifyListener( lsMod );
    FormData fdInclRownumField = new FormData();
    fdInclRownumField.left = new FormAttachment( wlInclRownumField, margin );
    fdInclRownumField.top = new FormAttachment( wFilterFileType, 2 * margin );
    fdInclRownumField.right = new FormAttachment( 100, 0 );
    wInclRownumField.setLayoutData( fdInclRownumField );

    FormData fdAdditionalGroup = new FormData();
    fdAdditionalGroup.left = new FormAttachment( 0, margin );
    fdAdditionalGroup.top = new FormAttachment( wFilterFileType, margin );
    fdAdditionalGroup.right = new FormAttachment( 100, -margin );
    wAdditionalGroup.setLayoutData( fdAdditionalGroup );

    // ///////////////////////////////////////////////////////////
    // / END OF DESTINATION ADDRESS GROUP
    // ///////////////////////////////////////////////////////////

    // do not fail if no files?
    // do not fail if no files?
    Label wlDoNotFailIfNoFile = new Label( wFilterComp, SWT.RIGHT );
    wlDoNotFailIfNoFile.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.doNotFailIfNoFile.Label" ) );
    props.setLook( wlDoNotFailIfNoFile );
    FormData fdldoNotFailIfNoFile = new FormData();
    fdldoNotFailIfNoFile.left = new FormAttachment( 0, 0 );
    fdldoNotFailIfNoFile.top = new FormAttachment( wAdditionalGroup, 2 * margin );
    fdldoNotFailIfNoFile.right = new FormAttachment( middle, -margin );
    wlDoNotFailIfNoFile.setLayoutData( fdldoNotFailIfNoFile );
    wDoNotFailIfNoFile = new Button( wFilterComp, SWT.CHECK );
    props.setLook( wDoNotFailIfNoFile );
    wDoNotFailIfNoFile.setToolTipText( BaseMessages.getString( PKG, "GetFileNamesDialog.doNotFailIfNoFile.Tooltip" ) );
    FormData fddoNotFailIfNoFile = new FormData();
    fddoNotFailIfNoFile.left = new FormAttachment( middle, 0 );
    fddoNotFailIfNoFile.top = new FormAttachment( wlDoNotFailIfNoFile, 0, SWT.CENTER );
    wDoNotFailIfNoFile.setLayoutData( fddoNotFailIfNoFile );
    wDoNotFailIfNoFile.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        input.setChanged();
      }
    } );

    wlLimit = new Label( wFilterComp, SWT.RIGHT );
    wlLimit.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.Limit.Label" ) );
    props.setLook( wlLimit );
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment( 0, 0 );
    fdlLimit.top = new FormAttachment( wDoNotFailIfNoFile, margin );
    fdlLimit.right = new FormAttachment( middle, -margin );
    wlLimit.setLayoutData( fdlLimit );
    wLimit = new Text( wFilterComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLimit );
    wLimit.addModifyListener( lsMod );
    FormData fdLimit = new FormData();
    fdLimit.left = new FormAttachment( middle, 0 );
    fdLimit.top = new FormAttachment( wDoNotFailIfNoFile, margin );
    fdLimit.right = new FormAttachment( 100, 0 );
    wLimit.setLayoutData( fdLimit );

    // ///////////////////////////////
    // START OF AddFileResult GROUP //
    // ///////////////////////////////

    Group wAddFileResult = new Group( wFilterComp, SWT.SHADOW_NONE );
    props.setLook( wAddFileResult );
    wAddFileResult.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.wAddFileResult.Label" ) );

    FormLayout AddFileResultgroupLayout = new FormLayout();
    AddFileResultgroupLayout.marginWidth = 10;
    AddFileResultgroupLayout.marginHeight = 10;
    wAddFileResult.setLayout( AddFileResultgroupLayout );

    Label wlAddResult = new Label( wAddFileResult, SWT.RIGHT );
    wlAddResult.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.AddResult.Label" ) );
    props.setLook( wlAddResult );
    FormData fdlAddResult = new FormData();
    fdlAddResult.left = new FormAttachment( 0, 0 );
    fdlAddResult.top = new FormAttachment( wLimit, margin );
    fdlAddResult.right = new FormAttachment( middle, -margin );
    wlAddResult.setLayoutData( fdlAddResult );
    wAddResult = new Button( wAddFileResult, SWT.CHECK );
    props.setLook( wAddResult );
    wAddResult.setToolTipText( BaseMessages.getString( PKG, "GetFileNamesDialog.AddResult.Tooltip" ) );
    FormData fdAddResult = new FormData();
    fdAddResult.left = new FormAttachment( middle, 0 );
    fdAddResult.top = new FormAttachment( wlAddResult, 0, SWT.CENTER );
    wAddResult.setLayoutData( fdAddResult );
    wAddResult.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        input.setChanged();
      }
    } );

    FormData fdAddFileResult = new FormData();
    fdAddFileResult.left = new FormAttachment( 0, margin );
    fdAddFileResult.top = new FormAttachment( wLimit, margin );
    fdAddFileResult.right = new FormAttachment( 100, -margin );
    wAddFileResult.setLayoutData( fdAddFileResult );

    // ///////////////////////////////////////////////////////////
    // / END OF AddFileResult GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdFilterComp = new FormData();
    fdFilterComp.left = new FormAttachment( 0, 0 );
    fdFilterComp.top = new FormAttachment( 0, 0 );
    fdFilterComp.right = new FormAttachment( 100, 0 );
    fdFilterComp.bottom = new FormAttachment( 100, 0 );
    wFilterComp.setLayoutData( fdFilterComp );

    wFilterComp.layout();
    wFilterTab.setControl( wFilterComp );

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
        wFilenameList.add( new String[] {
          wFilename.getText(), wFilemask.getText(), wExcludeFilemask.getText(),
          GetFileNamesMeta.RequiredFilesCode[ 0 ], GetFileNamesMeta.RequiredFilesCode[ 0 ] } );
        wFilename.setText( "" );
        wFilemask.setText( "" );
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
        GetFileNamesMeta tfii = new GetFileNamesMeta();
        getInfo( tfii );
        String[] files = tfii.getFilePaths( variables );
        if ( files != null && files.length > 0 ) {
          EnterSelectionDialog esd = new EnterSelectionDialog( shell, files, "Files read", "Files read:" );
          esd.setViewOnly();
          esd.open();
        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
          mb.setMessage( BaseMessages.getString( PKG, "GetFileNamesDialog.NoFilesFound.DialogMessage" ) );
          mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
          mb.open();
        }
      }
    } );

    // Listen to the Browse... button
    wbbFilename.addListener( SWT.Selection, e -> {
      if ( !Utils.isEmpty( wFilemask.getText() ) || !Utils.isEmpty( wExcludeFilemask.getText() ) ) {
        BaseDialog.presentDirectoryDialog( shell, wFilename, variables );
      } else {
        BaseDialog.presentFileDialog( shell, wFilename, variables,
          new String[] { "*.txt;*.csv", "*.csv", "*.txt", "*" },
          new String[] {
            BaseMessages.getString( PKG, "GetFileNamesDialog.FileType.TextAndCSVFiles" ),
            BaseMessages.getString( PKG, "System.FileType.CSVFiles" ),
            BaseMessages.getString( PKG, "System.FileType.TextFiles" ),
            BaseMessages.getString( PKG, "System.FileType.AllFiles" ) },
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
    setFileField();
    getData( input );
    activateFileField();
    setSize();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void setFileField() {
    try {
      if ( !getPreviousFields ) {
        getPreviousFields = true;
        String filename = wFilenameField.getText();
        String wildcard = wWildcardField.getText();
        String excludewildcard = wExcludeWildcardField.getText();

        wFilenameField.removeAll();
        wWildcardField.removeAll();
        wExcludeWildcardField.removeAll();

        IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
        if ( r != null ) {
          wFilenameField.setItems( r.getFieldNames() );
          wWildcardField.setItems( r.getFieldNames() );
          wExcludeWildcardField.setItems( r.getFieldNames() );
        }
        if ( filename != null ) {
          wFilenameField.setText( filename );
        }
        if ( wildcard != null ) {
          wWildcardField.setText( wildcard );
        }
        if ( excludewildcard != null ) {
          wExcludeWildcardField.setText( excludewildcard );
        }
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "GetFileNamesDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "GetFileNamesDialog.FailedToGetFields.DialogMessage" ), ke );
    }
  }

  private void activateFileField() {
    if ( wFileField.getSelection() ) {
      wLimit.setText( "0" );
    }
    wlFilenameField.setEnabled( wFileField.getSelection() );
    wFilenameField.setEnabled( wFileField.getSelection() );
    wlWildcardField.setEnabled( wFileField.getSelection() );
    wWildcardField.setEnabled( wFileField.getSelection() );
    wlExcludeWildcardField.setEnabled( wFileField.getSelection() );
    wExcludeWildcardField.setEnabled( wFileField.getSelection() );
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
    wlLimit.setEnabled( !wFileField.getSelection() );
    wLimit.setEnabled( !wFileField.getSelection() );
    wlIncludeSubFolder.setEnabled( wFileField.getSelection() );
    wIncludeSubFolder.setEnabled( wFileField.getSelection() );

  }

  /**
   * Read the data from the GetFileNamesMeta object and show it in this dialog.
   *
   * @param meta The TextFileInputMeta object to obtain the data from.
   */
  public void getData( GetFileNamesMeta meta ) {
    final GetFileNamesMeta in = meta;

    if ( in.getFileName() != null ) {
      wFilenameList.removeAll();

      for ( int i = 0; i < meta.getFileName().length; i++ ) {
        wFilenameList.add( new String[] {
          in.getFileName()[ i ], in.getFileMask()[ i ], in.getExludeFileMask()[ i ],
          in.getRequiredFilesDesc( in.getFileRequired()[ i ] ),
          in.getRequiredFilesDesc( in.getIncludeSubFolders()[ i ] ) } );
      }

      wDoNotFailIfNoFile.setSelection( in.isdoNotFailIfNoFile() );
      wFilenameList.removeEmptyRows();
      wFilenameList.setRowNums();
      wFilenameList.optWidth( true );

      if ( in.getFileTypeFilter() != null ) {
        wFilterFileType.select( in.getFileTypeFilter().ordinal() );
      } else {
        wFilterFileType.select( 0 );

      }

      wInclRownum.setSelection( in.includeRowNumber() );
      wAddResult.setSelection( in.isAddResultFile() );
      wFileField.setSelection( in.isFileField() );
      if ( in.getRowNumberField() != null ) {
        wInclRownumField.setText( in.getRowNumberField() );
      }
      if ( in.getDynamicFilenameField() != null ) {
        wFilenameField.setText( in.getDynamicFilenameField() );
      }
      if ( in.getDynamicWildcardField() != null ) {
        wWildcardField.setText( in.getDynamicWildcardField() );
      }
      if ( in.getDynamicExcludeWildcardField() != null ) {
        wExcludeWildcardField.setText( in.getDynamicExcludeWildcardField() );
      }
      wLimit.setText( "" + in.getRowLimit() );
      wIncludeSubFolder.setSelection( in.isDynamicIncludeSubFolders() );
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

  private void getInfo( GetFileNamesMeta in ) {
    transformName = wTransformName.getText(); // return value

    int nrfiles = wFilenameList.getItemCount();
    in.allocate( nrfiles );

    in.setFileName( wFilenameList.getItems( 0 ) );
    in.setFileMask( wFilenameList.getItems( 1 ) );
    in.setExcludeFileMask( wFilenameList.getItems( 2 ) );
    in.setFileRequired( wFilenameList.getItems( 3 ) );
    in.setIncludeSubFolders( wFilenameList.getItems( 4 ) );

    in.setFilterFileType( FileInputList.FileTypeFilter.getByOrdinal( wFilterFileType.getSelectionIndex() ) );
    in.setIncludeRowNumber( wInclRownum.getSelection() );
    in.setAddResultFile( wAddResult.getSelection() );
    in.setDynamicFilenameField( wFilenameField.getText() );
    in.setDynamicWildcardField( wWildcardField.getText() );
    in.setDynamicExcludeWildcardField( wExcludeWildcardField.getText() );
    in.setFileField( wFileField.getSelection() );
    in.setRowNumberField( wInclRownumField.getText() );
    in.setRowLimit( Const.toLong( wLimit.getText(), 0L ) );
    in.setDynamicIncludeSubFolders( wIncludeSubFolder.getSelection() );
    in.setdoNotFailIfNoFile( wDoNotFailIfNoFile.getSelection() );
  }

  // Preview the data
  private void preview() {
    // Create the XML input transform
    GetFileNamesMeta oneMeta = new GetFileNamesMeta();
    getInfo( oneMeta );

    PipelineMeta previewMeta = PipelinePreviewFactory.generatePreviewPipeline( variables, pipelineMeta.getMetadataProvider(),
      oneMeta, wTransformName.getText() );

    EnterNumberDialog numberDialog =
      new EnterNumberDialog( shell, props.getDefaultPreviewSize(),
        BaseMessages.getString( PKG, "GetFileNamesDialog.PreviewSize.DialogTitle" ),
        BaseMessages.getString( PKG, "GetFileNamesDialog.PreviewSize.DialogMessage" ) );
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
              .getString( PKG, "GetFileNamesDialog.ErrorInPreview.DialogMessage" ), loggingText, true );
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
