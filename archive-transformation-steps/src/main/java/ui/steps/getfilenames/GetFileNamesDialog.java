/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.apache.hop.ui.trans.steps.getfilenames;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.TransPreviewFactory;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDialogInterface;
import org.apache.hop.trans.steps.getfilenames.GetFileNamesMeta;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.trans.dialog.TransPreviewProgressDialog;
import org.apache.hop.ui.trans.step.BaseStepDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
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
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class GetFileNamesDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = GetFileNamesMeta.class; // for i18n purposes, needed by Translator2!!

  private static final String[] YES_NO_COMBO = new String[] {
    BaseMessages.getString( PKG, "System.Combo.No" ), BaseMessages.getString( PKG, "System.Combo.Yes" ) };

  // do not fail if no files?
  private Label wldoNotFailIfNoFile;
  private Button wdoNotFailIfNoFile;
  private FormData fdldoNotFailIfNoFile, fddoNotFailIfNoFile;

  private CTabFolder wTabFolder;

  private FormData fdTabFolder;

  private CTabItem wFileTab, wFilterTab;

  private Composite wFileComp, wFilterComp;

  private FormData fdFileComp, fdFilterComp;

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

  private Label wlExcludeFilemask;

  private TextVar wExcludeFilemask;

  private FormData fdlExcludeFilemask, fdExcludeFilemask;

  private Label wlFilemask;

  private TextVar wFilemask;

  private FormData fdlFilemask, fdFilemask;

  private Button wbShowFiles;

  private FormData fdbShowFiles;

  private Label wlFilterFileType;

  private CCombo wFilterFileType;

  private FormData fdlFilterFileType, fdFilterFileType;

  private GetFileNamesMeta input;

  private int middle, margin;

  private ModifyListener lsMod;

  private Group wOriginFiles;

  private FormData fdOriginFiles, fdFilenameField, fdlFilenameField;
  private Button wFileField;

  private Label wlFileField, wlFilenameField;
  private CCombo wFilenameField;
  private FormData fdlFileField, fdFileField;

  private Label wlWildcardField;
  private CCombo wWildcardField;
  private FormData fdlWildcardField, fdWildcardField;

  private Label wlExcludeWildcardField;
  private CCombo wExcludeWildcardField;
  private FormData fdlExcludeWildcardField, fdExcludeWildcardField;

  private Label wlIncludeSubFolder;
  private FormData fdlIncludeSubFolder;
  private Button wIncludeSubFolder;
  private FormData fdIncludeSubFolder;

  private Group wAdditionalGroup;
  private FormData fdAdditionalGroup, fdlAddResult;
  private Group wAddFileResult;

  private FormData fdAddResult, fdAddFileResult;
  private Button wAddResult;

  private Label wlLimit;
  private Text wLimit;
  private FormData fdlLimit, fdLimit;

  private Label wlInclRownum;
  private Button wInclRownum;
  private FormData fdlInclRownum, fdRownum;

  private Label wlInclRownumField;
  private TextVar wInclRownumField;
  private FormData fdlInclRownumField, fdInclRownumField;

  private boolean getpreviousFields = false;

  private Label wlAddResult;

  public GetFileNamesDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (GetFileNamesMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        input.setChanged();
      }
    };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.DialogTitle" ) );

    middle = props.getMiddlePct();
    margin = props.getMargin();

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "System.Label.StepName" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.top = new FormAttachment( 0, margin );
    fdlStepname.right = new FormAttachment( middle, -margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF FILE TAB ///
    // ////////////////////////
    wFileTab = new CTabItem( wTabFolder, SWT.NONE );
    wFileTab.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.FileTab.TabTitle" ) );

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
    wOriginFiles.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.wOriginFiles.Label" ) );

    FormLayout OriginFilesgroupLayout = new FormLayout();
    OriginFilesgroupLayout.marginWidth = 10;
    OriginFilesgroupLayout.marginHeight = 10;
    wOriginFiles.setLayout( OriginFilesgroupLayout );

    // Is Filename defined in a Field
    wlFileField = new Label( wOriginFiles, SWT.RIGHT );
    wlFileField.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.FileField.Label" ) );
    props.setLook( wlFileField );
    fdlFileField = new FormData();
    fdlFileField.left = new FormAttachment( 0, -margin );
    fdlFileField.top = new FormAttachment( 0, margin );
    fdlFileField.right = new FormAttachment( middle, -2 * margin );
    wlFileField.setLayoutData( fdlFileField );

    wFileField = new Button( wOriginFiles, SWT.CHECK );
    props.setLook( wFileField );
    wFileField.setToolTipText( BaseMessages.getString( PKG, "GetFileNamesDialog.FileField.Tooltip" ) );
    fdFileField = new FormData();
    fdFileField.left = new FormAttachment( middle, -margin );
    fdFileField.top = new FormAttachment( 0, margin );
    wFileField.setLayoutData( fdFileField );
    SelectionAdapter lfilefield = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        ActiveFileField();
        setFileField();
        input.setChanged();
      }
    };
    wFileField.addSelectionListener( lfilefield );

    // Filename field
    wlFilenameField = new Label( wOriginFiles, SWT.RIGHT );
    wlFilenameField.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.wlFilenameField.Label" ) );
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

    // Wildcard field
    wlWildcardField = new Label( wOriginFiles, SWT.RIGHT );
    wlWildcardField.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.wlWildcardField.Label" ) );
    props.setLook( wlWildcardField );
    fdlWildcardField = new FormData();
    fdlWildcardField.left = new FormAttachment( 0, -margin );
    fdlWildcardField.top = new FormAttachment( wFilenameField, margin );
    fdlWildcardField.right = new FormAttachment( middle, -2 * margin );
    wlWildcardField.setLayoutData( fdlWildcardField );

    wWildcardField = new CCombo( wOriginFiles, SWT.BORDER | SWT.READ_ONLY );
    wWildcardField.setEditable( true );
    props.setLook( wWildcardField );
    wWildcardField.addModifyListener( lsMod );
    fdWildcardField = new FormData();
    fdWildcardField.left = new FormAttachment( middle, -margin );
    fdWildcardField.top = new FormAttachment( wFilenameField, margin );
    fdWildcardField.right = new FormAttachment( 100, -margin );
    wWildcardField.setLayoutData( fdWildcardField );

    // ExcludeWildcard field
    wlExcludeWildcardField = new Label( wOriginFiles, SWT.RIGHT );
    wlExcludeWildcardField.setText( BaseMessages
      .getString( PKG, "GetFileNamesDialog.wlExcludeWildcardField.Label" ) );
    props.setLook( wlExcludeWildcardField );
    fdlExcludeWildcardField = new FormData();
    fdlExcludeWildcardField.left = new FormAttachment( 0, -margin );
    fdlExcludeWildcardField.top = new FormAttachment( wWildcardField, margin );
    fdlExcludeWildcardField.right = new FormAttachment( middle, -2 * margin );
    wlExcludeWildcardField.setLayoutData( fdlExcludeWildcardField );

    wExcludeWildcardField = new CCombo( wOriginFiles, SWT.BORDER | SWT.READ_ONLY );
    wExcludeWildcardField.setEditable( true );
    props.setLook( wExcludeWildcardField );
    wExcludeWildcardField.addModifyListener( lsMod );
    fdExcludeWildcardField = new FormData();
    fdExcludeWildcardField.left = new FormAttachment( middle, -margin );
    fdExcludeWildcardField.top = new FormAttachment( wWildcardField, margin );
    fdExcludeWildcardField.right = new FormAttachment( 100, -margin );
    wExcludeWildcardField.setLayoutData( fdExcludeWildcardField );

    // Is includeSubFoldername defined in a Field
    wlIncludeSubFolder = new Label( wOriginFiles, SWT.RIGHT );
    wlIncludeSubFolder.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.includeSubFolder.Label" ) );
    props.setLook( wlIncludeSubFolder );
    fdlIncludeSubFolder = new FormData();
    fdlIncludeSubFolder.left = new FormAttachment( 0, -margin );
    fdlIncludeSubFolder.top = new FormAttachment( wExcludeWildcardField, margin );
    fdlIncludeSubFolder.right = new FormAttachment( middle, -2 * margin );
    wlIncludeSubFolder.setLayoutData( fdlIncludeSubFolder );

    wIncludeSubFolder = new Button( wOriginFiles, SWT.CHECK );
    props.setLook( wIncludeSubFolder );
    wIncludeSubFolder
      .setToolTipText( BaseMessages.getString( PKG, "GetFileNamesDialog.includeSubFolder.Tooltip" ) );
    fdIncludeSubFolder = new FormData();
    fdIncludeSubFolder.left = new FormAttachment( middle, -margin );
    fdIncludeSubFolder.top = new FormAttachment( wExcludeWildcardField, margin );
    wIncludeSubFolder.setLayoutData( fdIncludeSubFolder );
    wIncludeSubFolder.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        input.setChanged();
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
    wlFilename.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.Filename.Label" ) );
    props.setLook( wlFilename );
    fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment( wOriginFiles, margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData( fdlFilename );

    wbbFilename = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbbFilename );
    wbbFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    wbbFilename.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.BrowseForFileOrDirAndAdd" ) );
    fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment( wOriginFiles, margin );
    wbbFilename.setLayoutData( fdbFilename );

    wbaFilename = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbaFilename );
    wbaFilename.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.FilenameAdd.Button" ) );
    wbaFilename.setToolTipText( BaseMessages.getString( PKG, "GetFileNamesDialog.FilenameAdd.Tooltip" ) );
    fdbaFilename = new FormData();
    fdbaFilename.right = new FormAttachment( wbbFilename, -margin );
    fdbaFilename.top = new FormAttachment( wOriginFiles, margin );
    wbaFilename.setLayoutData( fdbaFilename );

    wFilename = new TextVar( transMeta, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.right = new FormAttachment( wbaFilename, -margin );
    fdFilename.top = new FormAttachment( wOriginFiles, margin );
    wFilename.setLayoutData( fdFilename );

    wlFilemask = new Label( wFileComp, SWT.RIGHT );
    wlFilemask.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.Filemask.Label" ) );
    props.setLook( wlFilemask );
    fdlFilemask = new FormData();
    fdlFilemask.left = new FormAttachment( 0, 0 );
    fdlFilemask.top = new FormAttachment( wFilename, margin );
    fdlFilemask.right = new FormAttachment( middle, -margin );
    wlFilemask.setLayoutData( fdlFilemask );
    wFilemask = new TextVar( transMeta, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilemask );
    wFilemask.addModifyListener( lsMod );
    fdFilemask = new FormData();
    fdFilemask.left = new FormAttachment( middle, 0 );
    fdFilemask.top = new FormAttachment( wFilename, margin );
    fdFilemask.right = new FormAttachment( wFilename, 0, SWT.RIGHT );
    wFilemask.setLayoutData( fdFilemask );

    wlExcludeFilemask = new Label( wFileComp, SWT.RIGHT );
    wlExcludeFilemask.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.ExcludeFilemask.Label" ) );
    props.setLook( wlExcludeFilemask );
    fdlExcludeFilemask = new FormData();
    fdlExcludeFilemask.left = new FormAttachment( 0, 0 );
    fdlExcludeFilemask.top = new FormAttachment( wFilemask, margin );
    fdlExcludeFilemask.right = new FormAttachment( middle, -margin );
    wlExcludeFilemask.setLayoutData( fdlExcludeFilemask );
    wExcludeFilemask = new TextVar( transMeta, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wExcludeFilemask );
    wExcludeFilemask.addModifyListener( lsMod );
    fdExcludeFilemask = new FormData();
    fdExcludeFilemask.left = new FormAttachment( middle, 0 );
    fdExcludeFilemask.top = new FormAttachment( wFilemask, margin );
    fdExcludeFilemask.right = new FormAttachment( wFilename, 0, SWT.RIGHT );
    wExcludeFilemask.setLayoutData( fdExcludeFilemask );

    // Filename list line
    wlFilenameList = new Label( wFileComp, SWT.RIGHT );
    wlFilenameList.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.FilenameList.Label" ) );
    props.setLook( wlFilenameList );
    fdlFilenameList = new FormData();
    fdlFilenameList.left = new FormAttachment( 0, 0 );
    fdlFilenameList.top = new FormAttachment( wExcludeFilemask, margin );
    fdlFilenameList.right = new FormAttachment( middle, -margin );
    wlFilenameList.setLayoutData( fdlFilenameList );

    // Buttons to the right of the screen...
    wbdFilename = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbdFilename );
    wbdFilename.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.FilenameDelete.Button" ) );
    wbdFilename.setToolTipText( BaseMessages.getString( PKG, "GetFileNamesDialog.FilenameDelete.Tooltip" ) );
    fdbdFilename = new FormData();
    fdbdFilename.right = new FormAttachment( 100, 0 );
    fdbdFilename.top = new FormAttachment( wExcludeFilemask, 40 );
    wbdFilename.setLayoutData( fdbdFilename );

    wbeFilename = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbeFilename );
    wbeFilename.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.FilenameEdit.Button" ) );
    wbeFilename.setToolTipText( BaseMessages.getString( PKG, "GetFileNamesDialog.FilenameEdit.Tooltip" ) );
    fdbeFilename = new FormData();
    fdbeFilename.right = new FormAttachment( 100, 0 );
    fdbeFilename.left = new FormAttachment( wbdFilename, 0, SWT.LEFT );
    fdbeFilename.top = new FormAttachment( wbdFilename, margin );
    wbeFilename.setLayoutData( fdbeFilename );

    wbShowFiles = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbShowFiles );
    wbShowFiles.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.ShowFiles.Button" ) );
    fdbShowFiles = new FormData();
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
        transMeta, wFileComp, SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER, colinfo, colinfo.length, lsMod,
        props );
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

    fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wStepname, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( 100, -50 );
    wTabFolder.setLayoutData( fdTabFolder );

    // ////////////////////////
    // START OF Filter TAB ///
    // ////////////////////////
    wFilterTab = new CTabItem( wTabFolder, SWT.NONE );
    wFilterTab.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.FilterTab.TabTitle" ) );

    wFilterComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wFilterComp );

    FormLayout filesettingLayout = new FormLayout();
    filesettingLayout.marginWidth = 3;
    filesettingLayout.marginHeight = 3;
    wFilterComp.setLayout( fileLayout );

    // Filter File Type
    wlFilterFileType = new Label( wFilterComp, SWT.RIGHT );
    wlFilterFileType.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.FilterTab.FileType.Label" ) );
    props.setLook( wlFilterFileType );
    fdlFilterFileType = new FormData();
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
    fdFilterFileType = new FormData();
    fdFilterFileType.left = new FormAttachment( middle, 0 );
    fdFilterFileType.top = new FormAttachment( 0, 3 * margin );
    fdFilterFileType.right = new FormAttachment( 100, 0 );
    wFilterFileType.setLayoutData( fdFilterFileType );
    wFilterFileType.addModifyListener( lsMod );

    // /////////////////////////////////
    // START OF Additional Fields GROUP
    // /////////////////////////////////

    wAdditionalGroup = new Group( wFilterComp, SWT.SHADOW_NONE );
    props.setLook( wAdditionalGroup );
    wAdditionalGroup.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.Group.AdditionalGroup.Label" ) );

    FormLayout additionalgroupLayout = new FormLayout();
    additionalgroupLayout.marginWidth = 10;
    additionalgroupLayout.marginHeight = 10;
    wAdditionalGroup.setLayout( additionalgroupLayout );

    wlInclRownum = new Label( wAdditionalGroup, SWT.RIGHT );
    wlInclRownum.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.InclRownum.Label" ) );
    props.setLook( wlInclRownum );
    fdlInclRownum = new FormData();
    fdlInclRownum.left = new FormAttachment( 0, 0 );
    fdlInclRownum.top = new FormAttachment( wFilterFileType, 2 * margin );
    fdlInclRownum.right = new FormAttachment( middle, -margin );
    wlInclRownum.setLayoutData( fdlInclRownum );
    wInclRownum = new Button( wAdditionalGroup, SWT.CHECK );
    props.setLook( wInclRownum );
    wInclRownum.setToolTipText( BaseMessages.getString( PKG, "GetFileNamesDialog.InclRownum.Tooltip" ) );
    fdRownum = new FormData();
    fdRownum.left = new FormAttachment( middle, 0 );
    fdRownum.top = new FormAttachment( wFilterFileType, 2 * margin );
    wInclRownum.setLayoutData( fdRownum );
    wInclRownum.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        input.setChanged();
      }
    } );

    wlInclRownumField = new Label( wAdditionalGroup, SWT.RIGHT );
    wlInclRownumField.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.InclRownumField.Label" ) );
    props.setLook( wlInclRownumField );
    fdlInclRownumField = new FormData();
    fdlInclRownumField.left = new FormAttachment( wInclRownum, margin );
    fdlInclRownumField.top = new FormAttachment( wFilterFileType, 2 * margin );
    wlInclRownumField.setLayoutData( fdlInclRownumField );
    wInclRownumField = new TextVar( transMeta, wAdditionalGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclRownumField );
    wInclRownumField.addModifyListener( lsMod );
    fdInclRownumField = new FormData();
    fdInclRownumField.left = new FormAttachment( wlInclRownumField, margin );
    fdInclRownumField.top = new FormAttachment( wFilterFileType, 2 * margin );
    fdInclRownumField.right = new FormAttachment( 100, 0 );
    wInclRownumField.setLayoutData( fdInclRownumField );

    fdAdditionalGroup = new FormData();
    fdAdditionalGroup.left = new FormAttachment( 0, margin );
    fdAdditionalGroup.top = new FormAttachment( wFilterFileType, margin );
    fdAdditionalGroup.right = new FormAttachment( 100, -margin );
    wAdditionalGroup.setLayoutData( fdAdditionalGroup );

    // ///////////////////////////////////////////////////////////
    // / END OF DESTINATION ADDRESS GROUP
    // ///////////////////////////////////////////////////////////

    // do not fail if no files?
    wldoNotFailIfNoFile = new Label( wFilterComp, SWT.RIGHT );
    wldoNotFailIfNoFile.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.doNotFailIfNoFile.Label" ) );
    props.setLook( wldoNotFailIfNoFile );
    fdldoNotFailIfNoFile = new FormData();
    fdldoNotFailIfNoFile.left = new FormAttachment( 0, 0 );
    fdldoNotFailIfNoFile.top = new FormAttachment( wAdditionalGroup, 2 * margin );
    fdldoNotFailIfNoFile.right = new FormAttachment( middle, -margin );
    wldoNotFailIfNoFile.setLayoutData( fdldoNotFailIfNoFile );
    wdoNotFailIfNoFile = new Button( wFilterComp, SWT.CHECK );
    props.setLook( wdoNotFailIfNoFile );
    wdoNotFailIfNoFile.setToolTipText( BaseMessages
      .getString( PKG, "GetFileNamesDialog.doNotFailIfNoFile.Tooltip" ) );
    fddoNotFailIfNoFile = new FormData();
    fddoNotFailIfNoFile.left = new FormAttachment( middle, 0 );
    fddoNotFailIfNoFile.top = new FormAttachment( wAdditionalGroup, 2 * margin );
    wdoNotFailIfNoFile.setLayoutData( fddoNotFailIfNoFile );
    wdoNotFailIfNoFile.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        input.setChanged();
      }
    } );

    wlLimit = new Label( wFilterComp, SWT.RIGHT );
    wlLimit.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.Limit.Label" ) );
    props.setLook( wlLimit );
    fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment( 0, 0 );
    fdlLimit.top = new FormAttachment( wdoNotFailIfNoFile, margin );
    fdlLimit.right = new FormAttachment( middle, -margin );
    wlLimit.setLayoutData( fdlLimit );
    wLimit = new Text( wFilterComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLimit );
    wLimit.addModifyListener( lsMod );
    fdLimit = new FormData();
    fdLimit.left = new FormAttachment( middle, 0 );
    fdLimit.top = new FormAttachment( wdoNotFailIfNoFile, margin );
    fdLimit.right = new FormAttachment( 100, 0 );
    wLimit.setLayoutData( fdLimit );

    // ///////////////////////////////
    // START OF AddFileResult GROUP //
    // ///////////////////////////////

    wAddFileResult = new Group( wFilterComp, SWT.SHADOW_NONE );
    props.setLook( wAddFileResult );
    wAddFileResult.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.wAddFileResult.Label" ) );

    FormLayout AddFileResultgroupLayout = new FormLayout();
    AddFileResultgroupLayout.marginWidth = 10;
    AddFileResultgroupLayout.marginHeight = 10;
    wAddFileResult.setLayout( AddFileResultgroupLayout );

    wlAddResult = new Label( wAddFileResult, SWT.RIGHT );
    wlAddResult.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.AddResult.Label" ) );
    props.setLook( wlAddResult );
    fdlAddResult = new FormData();
    fdlAddResult.left = new FormAttachment( 0, 0 );
    fdlAddResult.top = new FormAttachment( wLimit, margin );
    fdlAddResult.right = new FormAttachment( middle, -margin );
    wlAddResult.setLayoutData( fdlAddResult );
    wAddResult = new Button( wAddFileResult, SWT.CHECK );
    props.setLook( wAddResult );
    wAddResult.setToolTipText( BaseMessages.getString( PKG, "GetFileNamesDialog.AddResult.Tooltip" ) );
    fdAddResult = new FormData();
    fdAddResult.left = new FormAttachment( middle, 0 );
    fdAddResult.top = new FormAttachment( wLimit, margin );
    wAddResult.setLayoutData( fdAddResult );
    wAddResult.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        input.setChanged();
      }
    } );

    fdAddFileResult = new FormData();
    fdAddFileResult.left = new FormAttachment( 0, margin );
    fdAddFileResult.top = new FormAttachment( wLimit, margin );
    fdAddFileResult.right = new FormAttachment( 100, -margin );
    wAddFileResult.setLayoutData( fdAddFileResult );

    // ///////////////////////////////////////////////////////////
    // / END OF AddFileResult GROUP
    // ///////////////////////////////////////////////////////////

    fdFilterComp = new FormData();
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

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wPreview = new Button( shell, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( PKG, "GetFileNamesDialog.Preview.Button" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wPreview, wCancel }, margin, wTabFolder );

    // Add listeners
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsPreview = new Listener() {
      public void handleEvent( Event e ) {
        preview();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wPreview.addListener( SWT.Selection, lsPreview );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );

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
        String[] files = tfii.getFilePaths( transMeta );
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
    wbbFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        if ( !Utils.isEmpty( wFilemask.getText() ) || !Utils.isEmpty( wExcludeFilemask.getText() ) ) {
          DirectoryDialog dialog = new DirectoryDialog( shell, SWT.OPEN );
          if ( wFilename.getText() != null ) {
            String fpath = transMeta.environmentSubstitute( wFilename.getText() );
            dialog.setFilterPath( fpath );
          }

          if ( dialog.open() != null ) {
            String str = dialog.getFilterPath();
            wFilename.setText( str );
          }
        } else {
          FileDialog dialog = new FileDialog( shell, SWT.OPEN );
          dialog.setFilterExtensions( new String[] { "*.txt;*.csv", "*.csv", "*.txt", "*" } );
          if ( wFilename.getText() != null ) {
            String fname = transMeta.environmentSubstitute( wFilename.getText() );
            dialog.setFileName( fname );
          }

          dialog.setFilterNames( new String[] {
            BaseMessages.getString( PKG, "GetFileNamesDialog.FileType.TextAndCSVFiles" ),
            BaseMessages.getString( PKG, "System.FileType.CSVFiles" ),
            BaseMessages.getString( PKG, "System.FileType.TextFiles" ),
            BaseMessages.getString( PKG, "System.FileType.AllFiles" ) } );

          if ( dialog.open() != null ) {
            String str = dialog.getFilterPath() + System.getProperty( "file.separator" ) + dialog.getFileName();
            wFilename.setText( str );
          }
        }
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
    ActiveFileField();
    setSize();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  private void setFileField() {
    try {
      if ( !getpreviousFields ) {
        getpreviousFields = true;
        String filename = wFilenameField.getText();
        String wildcard = wWildcardField.getText();
        String excludewildcard = wExcludeWildcardField.getText();

        wFilenameField.removeAll();
        wWildcardField.removeAll();
        wExcludeWildcardField.removeAll();

        RowMetaInterface r = transMeta.getPrevStepFields( stepname );
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

  private void ActiveFileField() {
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

      wdoNotFailIfNoFile.setSelection( in.isdoNotFailIfNoFile() );
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

    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }

    getInfo( input );
    dispose();
  }

  private void getInfo( GetFileNamesMeta in ) {
    stepname = wStepname.getText(); // return value

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
    in.setdoNotFailIfNoFile( wdoNotFailIfNoFile.getSelection() );
  }

  // Preview the data
  private void preview() {
    // Create the XML input step
    GetFileNamesMeta oneMeta = new GetFileNamesMeta();
    getInfo( oneMeta );

    TransMeta previewMeta =
      TransPreviewFactory.generatePreviewTransformation( transMeta, oneMeta, wStepname.getText() );

    EnterNumberDialog numberDialog =
      new EnterNumberDialog( shell, props.getDefaultPreviewSize(),
        BaseMessages.getString( PKG, "GetFileNamesDialog.PreviewSize.DialogTitle" ),
        BaseMessages.getString( PKG, "GetFileNamesDialog.PreviewSize.DialogMessage" ) );
    int previewSize = numberDialog.open();
    if ( previewSize > 0 ) {
      TransPreviewProgressDialog progressDialog =
        new TransPreviewProgressDialog(
          shell, previewMeta, new String[] { wStepname.getText() }, new int[] { previewSize } );
      progressDialog.open();

      if ( !progressDialog.isCancelled() ) {
        Trans trans = progressDialog.getTrans();
        String loggingText = progressDialog.getLoggingText();

        if ( trans.getResult() != null && trans.getResult().getNrErrors() > 0 ) {
          EnterTextDialog etd =
            new EnterTextDialog( shell, BaseMessages.getString( PKG, "System.Dialog.Error.Title" ), BaseMessages
              .getString( PKG, "GetFileNamesDialog.ErrorInPreview.DialogMessage" ), loggingText, true );
          etd.setReadOnly();
          etd.open();
        }

        PreviewRowsDialog prd =
          new PreviewRowsDialog(
            shell, transMeta, SWT.NONE, wStepname.getText(), progressDialog.getPreviewRowsMeta( wStepname
            .getText() ), progressDialog.getPreviewRows( wStepname.getText() ), loggingText );
        prd.open();
      }
    }
  }
}
