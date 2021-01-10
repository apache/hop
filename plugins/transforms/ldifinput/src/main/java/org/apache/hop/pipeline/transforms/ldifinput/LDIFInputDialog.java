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

package org.apache.hop.pipeline.transforms.ldifinput;


import netscape.ldap.LDAPAttribute;
import netscape.ldap.util.LDIF;
import netscape.ldap.util.LDIFAttributeContent;
import netscape.ldap.util.LDIFContent;
import netscape.ldap.util.LDIFRecord;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
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
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.text.SimpleDateFormat;
import java.util.Enumeration;
import java.util.HashSet;

public class LDIFInputDialog extends BaseTransformDialog implements ITransformDialog {

  private static final Class<?> PKG = LDIFInputMeta.class; // For Translator

  private CTabFolder wTabFolder;

  private Label wlFilename;

  private Button wbbFilename; // Browse: add file or directory

  private Button wbdFilename; // Delete

  private Button wbeFilename; // Edit

  private Button wbaFilename; // Add or change

  private TextVar wFilename;

  private Label wlFilenameList;

  private TableView wFilenameList;

  private Label wlFilemask;

  private TextVar wFilemask;

  private TextVar wExcludeFilemask;

  private Button wbShowFiles;

  private Label wlInclFilename, wlInclDNField;

  private Button wInclFilename, wInclContentType, wInclDN;

  private Label wlInclFilenameField;

  private TextVar wInclFilenameField, wInclContentTypeField, wInclDNField;

  private Button wInclRownum;

  private Label wlInclRownumField;

  private TextVar wInclRownumField;

  private Label wlLimit;

  private Text wLimit;

  private TableView wFields;

  private final LDIFInputMeta input;

  private Button wAddResult;

  private Label wlInclContentTypeField;

  private TextVar wMultiValuedSeparator;

  private Label wlFilenameField;
  private CCombo wFilenameField;

  private Button wFileField;

  private boolean gotPreviousField = false;

  private TextVar wShortFileFieldName;
  private TextVar wPathFieldName;

  private TextVar wIsHiddenName;
  private TextVar wLastModificationTimeName;
  private TextVar wUriName;
  private TextVar wRootUriName;
  private TextVar wExtensionFieldName;
  private TextVar wSizeFieldName;

  private int middle;
  private int margin;
  private ModifyListener lsMod;

  public static final int[] dateLengths = new int[] { 23, 19, 14, 10, 10, 10, 10, 8, 8, 8, 8, 6, 6 };

  public LDIFInputDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (LDIFInputMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "LDIFInputDialog.DialogTitle" ) );

    middle = props.getMiddlePct();
    margin = props.getMargin();

    // Buttons at the bottom
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wPreview = new Button( shell, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( PKG, "LDIFInputDialog.Button.PreviewRows" ) );
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
    CTabItem wFileTab = new CTabItem(wTabFolder, SWT.NONE);
    wFileTab.setText( BaseMessages.getString( PKG, "LDIFInputDialog.File.Tab" ) );

    Composite wFileComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wFileComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wFileComp.setLayout( fileLayout );

    // ///////////////////////////////
    // START OF Origin files GROUP //
    // ///////////////////////////////

    Group wOriginFiles = new Group(wFileComp, SWT.SHADOW_NONE);
    props.setLook(wOriginFiles);
    wOriginFiles.setText( BaseMessages.getString( PKG, "LDIFInputDialog.wOriginFiles.Label" ) );

    FormLayout OriginFilesgroupLayout = new FormLayout();
    OriginFilesgroupLayout.marginWidth = 10;
    OriginFilesgroupLayout.marginHeight = 10;
    wOriginFiles.setLayout( OriginFilesgroupLayout );

    // Is Filename defined in a Field
    Label wlFileField = new Label(wOriginFiles, SWT.RIGHT);
    wlFileField.setText( BaseMessages.getString( PKG, "LDIFInputDialog.FileField.Label" ) );
    props.setLook(wlFileField);
    FormData fdlFileField = new FormData();
    fdlFileField.left = new FormAttachment( 0, -margin );
    fdlFileField.top = new FormAttachment( 0, margin );
    fdlFileField.right = new FormAttachment( middle, -2 * margin );
    wlFileField.setLayoutData(fdlFileField);

    wFileField = new Button(wOriginFiles, SWT.CHECK );
    props.setLook( wFileField );
    wFileField.setToolTipText( BaseMessages.getString( PKG, "LDIFInputDialog.FileField.Tooltip" ) );
    FormData fdFileField = new FormData();
    fdFileField.left = new FormAttachment( middle, -margin );
    fdFileField.top = new FormAttachment( wlFileField, 0, SWT.CENTER );
    wFileField.setLayoutData(fdFileField);
    SelectionAdapter lfilefield = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        activateFileField();
        input.setChanged();
      }
    };
    wFileField.addSelectionListener( lfilefield );

    // Filename field
    wlFilenameField = new Label(wOriginFiles, SWT.RIGHT );
    wlFilenameField.setText( BaseMessages.getString( PKG, "LDIFInputDialog.wlFilenameField.Label" ) );
    props.setLook( wlFilenameField );
    FormData fdlFilenameField = new FormData();
    fdlFilenameField.left = new FormAttachment( 0, -margin );
    fdlFilenameField.top = new FormAttachment( wFileField, margin );
    fdlFilenameField.right = new FormAttachment( middle, -2 * margin );
    wlFilenameField.setLayoutData(fdlFilenameField);

    wFilenameField = new CCombo(wOriginFiles, SWT.BORDER | SWT.READ_ONLY );
    wFilenameField.setEditable( true );
    props.setLook( wFilenameField );
    wFilenameField.addModifyListener( lsMod );
    FormData fdFilenameField = new FormData();
    fdFilenameField.left = new FormAttachment( middle, -margin );
    fdFilenameField.top = new FormAttachment( wFileField, margin );
    fdFilenameField.right = new FormAttachment( 100, -margin );
    wFilenameField.setLayoutData(fdFilenameField);
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

    FormData fdOriginFiles = new FormData();
    fdOriginFiles.left = new FormAttachment( 0, margin );
    fdOriginFiles.top = new FormAttachment( wFilenameList, margin );
    fdOriginFiles.right = new FormAttachment( 100, -margin );
    wOriginFiles.setLayoutData(fdOriginFiles);

    // ///////////////////////////////////////////////////////////
    // / END OF Origin files GROUP
    // ///////////////////////////////////////////////////////////

    // Filename line
    wlFilename = new Label(wFileComp, SWT.RIGHT );
    wlFilename.setText( BaseMessages.getString( PKG, "LDIFInputDialog.Filename.Label" ) );
    props.setLook( wlFilename );
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment(wOriginFiles, margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData(fdlFilename);

    wbbFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbbFilename );
    wbbFilename.setText( BaseMessages.getString( PKG, "LDIFInputDialog.FilenameBrowse.Button" ) );
    wbbFilename.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.BrowseForFileOrDirAndAdd" ) );
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment(wOriginFiles, margin );
    wbbFilename.setLayoutData(fdbFilename);

    wbaFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbaFilename );
    wbaFilename.setText( BaseMessages.getString( PKG, "LDIFInputDialog.FilenameAdd.Button" ) );
    wbaFilename.setToolTipText( BaseMessages.getString( PKG, "LDIFInputDialog.FilenameAdd.Tooltip" ) );
    FormData fdbaFilename = new FormData();
    fdbaFilename.right = new FormAttachment( wbbFilename, -margin );
    fdbaFilename.top = new FormAttachment(wOriginFiles, margin );
    wbaFilename.setLayoutData(fdbaFilename);

    wFilename = new TextVar( variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.right = new FormAttachment( wbaFilename, -margin );
    fdFilename.top = new FormAttachment(wOriginFiles, margin );
    wFilename.setLayoutData(fdFilename);

    wlFilemask = new Label(wFileComp, SWT.RIGHT );
    wlFilemask.setText( BaseMessages.getString( PKG, "LDIFInputDialog.RegExp.Label" ) );
    props.setLook( wlFilemask );
    FormData fdlFilemask = new FormData();
    fdlFilemask.left = new FormAttachment( 0, 0 );
    fdlFilemask.top = new FormAttachment( wFilename, margin );
    fdlFilemask.right = new FormAttachment( middle, -margin );
    wlFilemask.setLayoutData(fdlFilemask);
    wFilemask = new TextVar( variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilemask );
    wFilemask.setToolTipText( BaseMessages.getString( PKG, "LDIFInputDialog.RegExp.Tooltip" ) );
    wFilemask.addModifyListener( lsMod );
    FormData fdFilemask = new FormData();
    fdFilemask.left = new FormAttachment( middle, 0 );
    fdFilemask.top = new FormAttachment( wFilename, margin );
    fdFilemask.right = new FormAttachment( 100, 0 );
    wFilemask.setLayoutData(fdFilemask);

    Label wlExcludeFilemask = new Label(wFileComp, SWT.RIGHT);
    wlExcludeFilemask.setText( BaseMessages.getString( PKG, "LDIFInputDialog.ExcludeFilemask.Label" ) );
    props.setLook(wlExcludeFilemask);
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
    wlFilenameList.setText( BaseMessages.getString( PKG, "LDIFInputDialog.FilenameList.Label" ) );
    props.setLook( wlFilenameList );
    FormData fdlFilenameList = new FormData();
    fdlFilenameList.left = new FormAttachment( 0, 0 );
    fdlFilenameList.top = new FormAttachment( wExcludeFilemask, margin );
    fdlFilenameList.right = new FormAttachment( middle, -margin );
    wlFilenameList.setLayoutData(fdlFilenameList);

    // Buttons to the right of the screen...
    wbdFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbdFilename );
    wbdFilename.setText( BaseMessages.getString( PKG, "LDIFInputDialog.FilenameRemove.Button" ) );
    wbdFilename.setToolTipText( BaseMessages.getString( PKG, "LDIFInputDialog.FilenameRemove.Tooltip" ) );
    FormData fdbdFilename = new FormData();
    fdbdFilename.right = new FormAttachment( 100, 0 );
    fdbdFilename.top = new FormAttachment( wExcludeFilemask, 40 );
    wbdFilename.setLayoutData(fdbdFilename);

    wbeFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbeFilename );
    wbeFilename.setText( BaseMessages.getString( PKG, "LDIFInputDialog.FilenameEdit.Button" ) );
    wbeFilename.setToolTipText( BaseMessages.getString( PKG, "LDIFInputDialog.FilenameEdit.Tooltip" ) );
    FormData fdbeFilename = new FormData();
    fdbeFilename.right = new FormAttachment( 100, 0 );
    fdbeFilename.top = new FormAttachment( wbdFilename, margin );
    wbeFilename.setLayoutData(fdbeFilename);

    wbShowFiles = new Button(wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbShowFiles );
    wbShowFiles.setText( BaseMessages.getString( PKG, "LDIFInputDialog.ShowFiles.Button" ) );
    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment( middle, 0 );
    fdbShowFiles.bottom = new FormAttachment( 100, 0 );
    wbShowFiles.setLayoutData(fdbShowFiles);

    ColumnInfo[] colinfo = new ColumnInfo[ 5 ];
    colinfo[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "LDIFInputDialog.Files.Filename.Column" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );
    colinfo[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "LDIFInputDialog.Files.Wildcard.Column" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );
    colinfo[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "LDIFInputDialog.Files.ExcludeWildcard.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinfo[ 3 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "LDIFInputDialog.Required.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        LDIFInputMeta.RequiredFilesDesc );
    colinfo[ 4 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "LDIFInputDialog.IncludeSubDirs.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        LDIFInputMeta.RequiredFilesDesc );

    colinfo[ 0 ].setUsingVariables( true );
    colinfo[ 1 ].setUsingVariables( true );
    colinfo[ 1 ].setToolTip( BaseMessages.getString( PKG, "LDIFInputDialog.Files.Wildcard.Tooltip" ) );
    colinfo[ 2 ].setToolTip( BaseMessages.getString( PKG, "LDIFInputDialog.Required.Tooltip" ) );
    colinfo[ 2 ].setUsingVariables( true );
    colinfo[ 2 ].setToolTip( BaseMessages.getString( PKG, "LDIFInputDialog.Files.ExcludeWildcard.Tooltip" ) );
    colinfo[ 4 ].setToolTip( BaseMessages.getString( PKG, "LDIFInputDialog.IncludeSubDirs.Tooltip" ) );

    wFilenameList =
      new TableView(
        variables, wFileComp, SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER, colinfo, 2, lsMod, props );
    props.setLook( wFilenameList );
    FormData fdFilenameList = new FormData();
    fdFilenameList.left = new FormAttachment( middle, 0 );
    fdFilenameList.right = new FormAttachment( wbdFilename, -margin );
    fdFilenameList.top = new FormAttachment( wExcludeFilemask, margin );
    fdFilenameList.bottom = new FormAttachment( wbShowFiles, -margin );
    wFilenameList.setLayoutData(fdFilenameList);

    FormData fdFileComp = new FormData();
    fdFileComp.left = new FormAttachment( 0, 0 );
    fdFileComp.top = new FormAttachment( 0, 0 );
    fdFileComp.right = new FormAttachment( 100, 0 );
    fdFileComp.bottom = new FormAttachment( 100, 0 );
    wFileComp.setLayoutData(fdFileComp);

    wFileComp.layout();
    wFileTab.setControl(wFileComp);

    // ///////////////////////////////////////////////////////////
    // / END OF FILE TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF CONTENT TAB///
    // /
    CTabItem wContentTab = new CTabItem(wTabFolder, SWT.NONE);
    wContentTab.setText( BaseMessages.getString( PKG, "LDIFInputDialog.Content.Tab" ) );

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    Composite wContentComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wContentComp);
    wContentComp.setLayout( contentLayout );

    wlInclFilename = new Label(wContentComp, SWT.RIGHT );
    wlInclFilename.setText( BaseMessages.getString( PKG, "LDIFInputDialog.InclFilename.Label" ) );
    props.setLook( wlInclFilename );
    FormData fdlInclFilename = new FormData();
    fdlInclFilename.left = new FormAttachment( 0, 0 );
    fdlInclFilename.top = new FormAttachment( 0, 2 * margin );
    fdlInclFilename.right = new FormAttachment( middle, -margin );
    wlInclFilename.setLayoutData(fdlInclFilename);
    wInclFilename = new Button(wContentComp, SWT.CHECK );
    props.setLook( wInclFilename );
    wInclFilename.setToolTipText( BaseMessages.getString( PKG, "LDIFInputDialog.InclFilename.Tooltip" ) );
    FormData fdInclFilename = new FormData();
    fdInclFilename.left = new FormAttachment( middle, 0 );
    fdInclFilename.top = new FormAttachment(wlInclFilename, 0, SWT.CENTER );
    wInclFilename.setLayoutData(fdInclFilename);

    wlInclFilenameField = new Label(wContentComp, SWT.LEFT );
    wlInclFilenameField.setText( BaseMessages.getString( PKG, "LDIFInputDialog.InclFilenameField.Label" ) );
    props.setLook( wlInclFilenameField );
    FormData fdlInclFilenameField = new FormData();
    fdlInclFilenameField.left = new FormAttachment( wInclFilename, margin );
    fdlInclFilenameField.top = new FormAttachment( 0, 2 * margin );
    wlInclFilenameField.setLayoutData(fdlInclFilenameField);
    wInclFilenameField = new TextVar( variables, wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclFilenameField );
    wInclFilenameField.addModifyListener( lsMod );
    FormData fdInclFilenameField = new FormData();
    fdInclFilenameField.left = new FormAttachment( wlInclFilenameField, margin );
    fdInclFilenameField.top = new FormAttachment( 0, 2 * margin );
    fdInclFilenameField.right = new FormAttachment( 100, 0 );
    wInclFilenameField.setLayoutData(fdInclFilenameField);

    Label wlInclRownum = new Label(wContentComp, SWT.RIGHT);
    wlInclRownum.setText( BaseMessages.getString( PKG, "LDIFInputDialog.InclRownum.Label" ) );
    props.setLook(wlInclRownum);
    FormData fdlInclRownum = new FormData();
    fdlInclRownum.left = new FormAttachment( 0, 0 );
    fdlInclRownum.top = new FormAttachment( wInclFilenameField, margin );
    fdlInclRownum.right = new FormAttachment( middle, -margin );
    wlInclRownum.setLayoutData(fdlInclRownum);
    wInclRownum = new Button(wContentComp, SWT.CHECK );
    props.setLook( wInclRownum );
    wInclRownum.setToolTipText( BaseMessages.getString( PKG, "LDIFInputDialog.InclRownum.Tooltip" ) );
    FormData fdRownum = new FormData();
    fdRownum.left = new FormAttachment( middle, 0 );
    fdRownum.top = new FormAttachment( wInclFilenameField, margin );
    wInclRownum.setLayoutData(fdRownum);

    wlInclRownumField = new Label(wContentComp, SWT.RIGHT );
    wlInclRownumField.setText( BaseMessages.getString( PKG, ( "LDIFInputDialog.InclRownumField.Label" ) ) );
    props.setLook( wlInclRownumField );
    FormData fdlInclRownumField = new FormData();
    fdlInclRownumField.left = new FormAttachment( wInclRownum, margin );
    fdlInclRownumField.top = new FormAttachment( wInclFilenameField, margin );
    wlInclRownumField.setLayoutData(fdlInclRownumField);
    wInclRownumField = new TextVar( variables, wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclRownumField );
    wInclRownumField.addModifyListener( lsMod );
    FormData fdInclRownumField = new FormData();
    fdInclRownumField.left = new FormAttachment( wlInclRownumField, margin );
    fdInclRownumField.top = new FormAttachment( wInclFilenameField, margin );
    fdInclRownumField.right = new FormAttachment( 100, 0 );
    wInclRownumField.setLayoutData(fdInclRownumField);

    // Add content type field?
    Label wlInclContentType = new Label(wContentComp, SWT.RIGHT);
    wlInclContentType.setText( BaseMessages.getString( PKG, "LDIFInputDialog.InclContentType.Label" ) );
    props.setLook(wlInclContentType);
    FormData fdlInclContentType = new FormData();
    fdlInclContentType.left = new FormAttachment( 0, 0 );
    fdlInclContentType.top = new FormAttachment( wInclRownumField, margin );
    fdlInclContentType.right = new FormAttachment( middle, -margin );
    wlInclContentType.setLayoutData(fdlInclContentType);
    wInclContentType = new Button(wContentComp, SWT.CHECK );
    props.setLook( wInclContentType );
    wInclContentType.setToolTipText( BaseMessages.getString( PKG, "LDIFInputDialog.InclContentType.Tooltip" ) );
    FormData fdInclContentType = new FormData();
    fdInclContentType.left = new FormAttachment( middle, 0 );
    fdInclContentType.top = new FormAttachment( wlInclContentType, 0, SWT.CENTER );
    wInclContentType.setLayoutData(fdInclContentType);

    // Content type field name
    wlInclContentTypeField = new Label(wContentComp, SWT.LEFT );
    wlInclContentTypeField.setText( BaseMessages.getString( PKG, "LDIFInputDialog.InclContentTypeField.Label" ) );
    props.setLook( wlInclContentTypeField );
    FormData fdlInclContentTypeField = new FormData();
    fdlInclContentTypeField.left = new FormAttachment( wInclContentType, margin );
    fdlInclContentTypeField.top = new FormAttachment( wInclRownumField, margin );
    wlInclContentTypeField.setLayoutData(fdlInclContentTypeField);
    wInclContentTypeField = new TextVar( variables, wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclContentTypeField );
    wInclContentTypeField.addModifyListener( lsMod );
    FormData fdInclContentTypeField = new FormData();
    fdInclContentTypeField.left = new FormAttachment( wlInclContentTypeField, margin );
    fdInclContentTypeField.top = new FormAttachment( wInclRownumField, margin );
    fdInclContentTypeField.right = new FormAttachment( 100, 0 );
    wInclContentTypeField.setLayoutData(fdInclContentTypeField);

    // Add content type field?
    Label wlInclDN = new Label(wContentComp, SWT.RIGHT);
    wlInclDN.setText( BaseMessages.getString( PKG, "LDIFInputDialog.InclDN.Label" ) );
    props.setLook(wlInclDN);
    FormData fdlInclDN = new FormData();
    fdlInclDN.left = new FormAttachment( 0, 0 );
    fdlInclDN.top = new FormAttachment( wInclContentTypeField, margin );
    fdlInclDN.right = new FormAttachment( middle, -margin );
    wlInclDN.setLayoutData(fdlInclDN);
    wInclDN = new Button(wContentComp, SWT.CHECK );
    props.setLook( wInclDN );
    wInclDN.setToolTipText( BaseMessages.getString( PKG, "LDIFInputDialog.InclDN.Tooltip" ) );
    FormData fdInclDN = new FormData();
    fdInclDN.left = new FormAttachment( middle, 0 );
    fdInclDN.top = new FormAttachment( wlInclDN, 0, SWT.CENTER );
    wInclDN.setLayoutData(fdInclDN);

    // Content type field name
    wlInclDNField = new Label(wContentComp, SWT.LEFT );
    wlInclDNField.setText( BaseMessages.getString( PKG, "LDIFInputDialog.InclDNField.Label" ) );
    props.setLook( wlInclDNField );
    FormData fdlInclDNField = new FormData();
    fdlInclDNField.left = new FormAttachment( wInclDN, margin );
    fdlInclDNField.top = new FormAttachment( wInclContentTypeField, margin );
    wlInclDNField.setLayoutData(fdlInclDNField);
    wInclDNField = new TextVar( variables, wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclDNField );
    wInclDNField.addModifyListener( lsMod );
    FormData fdInclDNField = new FormData();
    fdInclDNField.left = new FormAttachment( wlInclDNField, margin );
    fdInclDNField.top = new FormAttachment( wInclContentTypeField, margin );
    fdInclDNField.right = new FormAttachment( 100, 0 );
    wInclDNField.setLayoutData(fdInclDNField);

    // Limit to preview
    wlLimit = new Label(wContentComp, SWT.RIGHT );
    wlLimit.setText( BaseMessages.getString( PKG, "LDIFInputDialog.Limit.Label" ) );
    props.setLook( wlLimit );
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment( 0, 0 );
    fdlLimit.top = new FormAttachment( wInclDNField, margin );
    fdlLimit.right = new FormAttachment( middle, -margin );
    wlLimit.setLayoutData(fdlLimit);
    wLimit = new Text(wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLimit );
    wLimit.addModifyListener( lsMod );
    FormData fdLimit = new FormData();
    fdLimit.left = new FormAttachment( middle, 0 );
    fdLimit.top = new FormAttachment( wInclDNField, margin );
    fdLimit.right = new FormAttachment( 100, 0 );
    wLimit.setLayoutData(fdLimit);

    // Multi valued field separator
    Label wlMultiValuedSeparator = new Label(wContentComp, SWT.RIGHT);
    wlMultiValuedSeparator.setText( BaseMessages.getString( PKG, "LDIFInputDialog.MultiValuedSeparator.Label" ) );
    props.setLook(wlMultiValuedSeparator);
    FormData fdlMultiValuedSeparator = new FormData();
    fdlMultiValuedSeparator.left = new FormAttachment( 0, 0 );
    fdlMultiValuedSeparator.top = new FormAttachment( wLimit, margin );
    fdlMultiValuedSeparator.right = new FormAttachment( middle, -margin );
    wlMultiValuedSeparator.setLayoutData(fdlMultiValuedSeparator);
    wMultiValuedSeparator = new TextVar( variables, wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMultiValuedSeparator );
    wMultiValuedSeparator.setToolTipText( BaseMessages.getString(
      PKG, "LDIFInputDialog.MultiValuedSeparator.Tooltip" ) );
    wMultiValuedSeparator.addModifyListener( lsMod );
    FormData fdMultiValuedSeparator = new FormData();
    fdMultiValuedSeparator.left = new FormAttachment( middle, 0 );
    fdMultiValuedSeparator.top = new FormAttachment( wLimit, margin );
    fdMultiValuedSeparator.right = new FormAttachment( 100, 0 );
    wMultiValuedSeparator.setLayoutData(fdMultiValuedSeparator);

    // ///////////////////////////////
    // START OF AddFileResult GROUP //
    // ///////////////////////////////

    Group wAddFileResult = new Group(wContentComp, SWT.SHADOW_NONE);
    props.setLook(wAddFileResult);
    wAddFileResult.setText( BaseMessages.getString( PKG, "LDIFInputDialog.wAddFileResult.Label" ) );

    FormLayout AddFileResultgroupLayout = new FormLayout();
    AddFileResultgroupLayout.marginWidth = 10;
    AddFileResultgroupLayout.marginHeight = 10;
    wAddFileResult.setLayout( AddFileResultgroupLayout );

    Label wlAddResult = new Label(wAddFileResult, SWT.RIGHT);
    wlAddResult.setText( BaseMessages.getString( PKG, "LDIFInputDialog.AddResult.Label" ) );
    props.setLook(wlAddResult);
    FormData fdlAddResult = new FormData();
    fdlAddResult.left = new FormAttachment( 0, 0 );
    fdlAddResult.top = new FormAttachment( wMultiValuedSeparator, margin );
    fdlAddResult.right = new FormAttachment( middle, -margin );
    wlAddResult.setLayoutData(fdlAddResult);
    wAddResult = new Button(wAddFileResult, SWT.CHECK );
    props.setLook( wAddResult );
    wAddResult.setToolTipText( BaseMessages.getString( PKG, "LDIFInputDialog.AddResult.Tooltip" ) );
    FormData fdAddResult = new FormData();
    fdAddResult.left = new FormAttachment( middle, 0 );
    fdAddResult.top = new FormAttachment( wlAddResult, 0, SWT.CENTER );
    wAddResult.setLayoutData(fdAddResult);

    FormData fdAddFileResult = new FormData();
    fdAddFileResult.left = new FormAttachment( 0, margin );
    fdAddFileResult.top = new FormAttachment( wMultiValuedSeparator, margin );
    fdAddFileResult.right = new FormAttachment( 100, -margin );
    wAddFileResult.setLayoutData(fdAddFileResult);

    // ///////////////////////////////////////////////////////////
    // / END OF AddFileResult GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdContentComp = new FormData();
    fdContentComp.left = new FormAttachment( 0, 0 );
    fdContentComp.top = new FormAttachment( 0, 0 );
    fdContentComp.right = new FormAttachment( 100, 0 );
    fdContentComp.bottom = new FormAttachment( 100, 0 );
    wContentComp.setLayoutData(fdContentComp);

    wContentComp.layout();
    wContentTab.setControl(wContentComp);

    // ///////////////////////////////////////////////////////////
    // / END OF CONTENT TAB
    // ///////////////////////////////////////////////////////////

    // Fields tab...
    //
    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setText( BaseMessages.getString( PKG, "LDIFInputDialog.Fields.Tab" ) );

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    wFieldsComp.setLayout( fieldsLayout );
    props.setLook(wFieldsComp);

    wGet = new Button(wFieldsComp, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "LDIFInputDialog.GetFields.Button" ) );
    fdGet = new FormData();
    fdGet.left = new FormAttachment( 50, 0 );
    fdGet.bottom = new FormAttachment( 100, 0 );
    wGet.setLayoutData( fdGet );

    final int FieldsRows = input.getInputFields().length;

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "LDIFInputDialog.FieldsTable.Name.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "LDIFInputDialog.FieldsTable.Attribut.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "LDIFInputDialog.FieldsTable.Type.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getValueMetaNames(), true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "LDIFInputDialog.FieldsTable.Format.Column" ),
          ColumnInfo.COLUMN_TYPE_FORMAT, 3 ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "LDIFInputDialog.FieldsTable.Length.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "LDIFInputDialog.FieldsTable.Precision.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "LDIFInputDialog.FieldsTable.Currency.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "LDIFInputDialog.FieldsTable.Decimal.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "LDIFInputDialog.FieldsTable.Group.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "LDIFInputDialog.FieldsTable.TrimType.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, LDIFInputField.trimTypeDesc, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "LDIFInputDialog.FieldsTable.Repeat.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {
          BaseMessages.getString( PKG, "System.Combo.Yes" ),
          BaseMessages.getString( PKG, "System.Combo.No" ) }, true ),

      };

    colinf[ 0 ].setUsingVariables( true );
    colinf[ 0 ].setToolTip( BaseMessages.getString( PKG, "LDIFInputDialog.FieldsTable.Name.Column.Tooltip" ) );
    colinf[ 1 ].setUsingVariables( true );
    colinf[ 1 ].setToolTip( BaseMessages.getString( PKG, "LDIFInputDialog.FieldsTable.Attribut.Column.Tooltip" ) );

    wFields =
      new TableView( variables, wFieldsComp, SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( 0, 0 );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wGet, -margin );
    wFields.setLayoutData(fdFields);

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment( 0, 0 );
    fdFieldsComp.top = new FormAttachment( 0, 0 );
    fdFieldsComp.right = new FormAttachment( 100, 0 );
    fdFieldsComp.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData(fdFieldsComp);

    wFieldsComp.layout();
    wFieldsTab.setControl(wFieldsComp);

    addAdditionalFieldsTab();

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wTransformName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOk, -2*margin );
    wTabFolder.setLayoutData(fdTabFolder);

    // Add listeners

    wGet.addListener( SWT.Selection, e -> get() );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );
    wLimit.addSelectionListener( lsDef );
    wInclRownumField.addSelectionListener( lsDef );
    wInclFilenameField.addSelectionListener( lsDef );

    // Add the file to the list of files...
    SelectionAdapter selA = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        wFilenameList.add( new String[] {
          wFilename.getText(), wFilemask.getText(), wExcludeFilemask.getText(),
          LDIFInputMeta.RequiredFilesCode[ 0 ], LDIFInputMeta.RequiredFilesCode[ 0 ] } );
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
      }
    } );

    // Show the files that are selected at this time...
    wbShowFiles.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        try {
          LDIFInputMeta tfii = new LDIFInputMeta();
          getInfo( tfii );
          FileInputList fileInputList = tfii.getFiles( variables );
          String[] files = fileInputList.getFileStrings();
          if ( files != null && files.length > 0 ) {
            EnterSelectionDialog esd =
              new EnterSelectionDialog( shell, files,
                BaseMessages.getString( PKG, "LDIFInputDialog.FilesReadSelection.DialogTitle" ),
                BaseMessages.getString( PKG, "LDIFInputDialog.FilesReadSelection.DialogMessage" ) );
            esd.setViewOnly();
            esd.open();
          } else {
            MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
            mb.setMessage( BaseMessages.getString( PKG, "LDIFInputDialog.NoFileFound.DialogMessage" ) );
            mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
            mb.open();
          }
        } catch ( HopException ex ) {
          new ErrorDialog( shell, BaseMessages.getString( PKG, "LDIFInputDialog.ErrorParsingData.DialogTitle" ),
            BaseMessages.getString( PKG, "LDIFInputDialog.ErrorParsingData.DialogMessage" ), ex );
        }
      }
    } );
    // Enable/disable the right fields to allow a filename to be added to
    // each row...
    wInclFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        setIncludeFilename();
      }
    } );

    // Enable/disable the right fields to allow a row number to be added to
    // each row...
    wInclRownum.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        setIncludeRownum();
      }
    } );

    // Enable/disable the right fields to allow a content type to be added to
    // each row...
    wInclContentType.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        setContenType();
      }
    } );

    // Enable/disable the right fields to allow a content type to be added to
    // each row...
    wInclDN.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        setDN();
      }
    } );

    // Whenever something changes, set the tooltip to the expanded version
    // of the filename:
    wFilename.addModifyListener( e -> wFilename.setToolTipText( variables.resolve( wFilename.getText() ) ) );

    // Listen to the Browse... button
    wbbFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        if ( !Utils.isEmpty( wFilemask.getText() ) || !Utils.isEmpty( wExcludeFilemask.getText() ) ) {
          DirectoryDialog dialog = new DirectoryDialog( shell, SWT.OPEN );
          if ( wFilename.getText() != null ) {
            String fpath = variables.resolve( wFilename.getText() );
            dialog.setFilterPath( fpath );
          }

          if ( dialog.open() != null ) {
            String str = dialog.getFilterPath();
            wFilename.setText( str );
          }
        } else {
          FileDialog dialog = new FileDialog( shell, SWT.OPEN );
          dialog.setFilterExtensions( new String[] { "*ldif;*.LDIF", "*" } );
          if ( wFilename.getText() != null ) {
            String fname = variables.resolve( wFilename.getText() );
            dialog.setFileName( fname );
          }

          dialog.setFilterNames( new String[] {
            BaseMessages.getString( PKG, "LDIFInputDialog.FileType" ),
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
    setSize();
    getData( input );
    activateFileField();
    setContenType();
    setDN();
    input.setChanged( changed );
    wFields.optWidth( true );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
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
    wlFilenameList.setEnabled( !wFileField.getSelection() );
    wbdFilename.setEnabled( !wFileField.getSelection() );
    wbeFilename.setEnabled( !wFileField.getSelection() );
    wbShowFiles.setEnabled( !wFileField.getSelection() );
    wlFilenameList.setEnabled( !wFileField.getSelection() );
    wFilenameList.setEnabled( !wFileField.getSelection() );
    if ( wFileField.getSelection() ) {
      wInclFilename.setSelection( false );
    }
    wInclFilename.setEnabled( !wFileField.getSelection() );
    wlInclFilename.setEnabled( !wFileField.getSelection() );
    wLimit.setEnabled( !wFileField.getSelection() );
    wPreview.setEnabled( !wFileField.getSelection() );
    wGet.setEnabled( !wFileField.getSelection() );
    wLimit.setEnabled( !wFileField.getSelection() );
    wlLimit.setEnabled( !wFileField.getSelection() );
  }

  private void setFileField() {
    if ( !gotPreviousField ) {
      try {
        String value = wFilenameField.getText();
        wFilenameField.removeAll();

        IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
        if ( r != null ) {
          r.getFieldNames();

          for ( int i = 0; i < r.getFieldNames().length; i++ ) {
            wFilenameField.add( r.getFieldNames()[ i ] );

          }
        }
        gotPreviousField = true;
        if ( value != null ) {
          wFilenameField.setText( value );
        }
      } catch ( HopException ke ) {
        new ErrorDialog(
          shell, BaseMessages.getString( PKG, "LDIFInputDialog.FailedToGetFields.DialogTitle" ), BaseMessages
          .getString( PKG, "LDIFInputDialog.FailedToGetFields.DialogMessage" ), ke );
      }
    }
  }

  private void get() {

    try {
      LDIFInputMeta meta = new LDIFInputMeta();
      getInfo( meta );

      FileInputList inputList = meta.getFiles( variables );
      // Clear Fields Grid
      wFields.removeAll();

      if ( inputList.getFiles().size() > 0 ) {
        // Open the file (only first file)...

        LDIF InputLDIF = new LDIF( HopVfs.getFilename( inputList.getFile( 0 ) ) );

        HashSet<String> attributeSet = new HashSet<>();

        //CHECKSTYLE:LineLength:OFF
        for ( LDIFRecord recordLDIF = InputLDIF.nextRecord(); recordLDIF != null; recordLDIF = InputLDIF.nextRecord() ) {
          // Get LDIF Content
          LDIFContent contentLDIF = recordLDIF.getContent();

          if ( contentLDIF.getType() == LDIFContent.ATTRIBUTE_CONTENT ) {
            // Get only ATTRIBUTE_CONTENT

            LDIFAttributeContent attrContentLDIF = (LDIFAttributeContent) contentLDIF;
            LDAPAttribute[] attributes_LDIF = attrContentLDIF.getAttributes();

            for (LDAPAttribute attribute_DIF : attributes_LDIF) {

              String attributeName = attribute_DIF.getName();
              if (!attributeSet.contains(attributeName)) {
                // Get attribut Name
                TableItem item = new TableItem(wFields.table, SWT.NONE);
                item.setText(1, attributeName);
                item.setText(2, attributeName);

                String attributeValue = GetValue(attributes_LDIF, attributeName);
                // Try to get the Type

                if (IsDate(attributeValue)) {
                  item.setText(3, "Date");
                } else if (IsInteger(attributeValue)) {
                  item.setText(3, "Integer");
                } else if (IsNumber(attributeValue)) {
                  item.setText(3, "Number");
                } else {
                  item.setText(3, "String");
                }
                attributeSet.add(attributeName);
              }
            }
          }
        }
      }

      wFields.removeEmptyRows();
      wFields.setRowNums();
      wFields.optWidth( true );
    } catch ( HopException e ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "LDIFInputMeta.ErrorRetrieveData.DialogTitle" ), BaseMessages
        .getString( PKG, "LDIFInputMeta.ErrorRetrieveData.DialogMessage" ), e );
    } catch ( Exception e ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "LDIFInputMeta.ErrorRetrieveData.DialogTitle" ), BaseMessages
        .getString( PKG, "LDIFInputMeta.ErrorRetrieveData.DialogMessage" ), e );

    }
  }

  private boolean IsInteger( String str ) {
    try {
      Integer.parseInt( str );
    } catch ( NumberFormatException e ) {
      return false;
    }
    return true;
  }

  private boolean IsNumber( String str ) {
    try {
      Float.parseFloat( str );
    } catch ( Exception e ) {
      return false;
    }
    return true;
  }

  private boolean IsDate( String str ) {
    // TODO: What about other dates? Maybe something for a CRQ
    try {
      SimpleDateFormat fdate = new SimpleDateFormat( "yy-mm-dd" );
      fdate.parse( str );
    } catch ( Exception e ) {
      return false;
    }
    return true;
  }

  @SuppressWarnings( "unchecked" )
  private String GetValue( LDAPAttribute[] attributes_LDIF, String AttributValue ) {
    String Stringvalue = null;

    for (LDAPAttribute attribute_DIF : attributes_LDIF) {
      if (attribute_DIF.getName().equalsIgnoreCase(AttributValue)) {
        Enumeration<String> valuesLDIF = attribute_DIF.getStringValues();
        // Get the first occurence
        Stringvalue = valuesLDIF.nextElement();
      }
    }

    return Stringvalue;
  }

  public void setIncludeFilename() {
    wlInclFilenameField.setEnabled( wInclFilename.getSelection() );
    wInclFilenameField.setEnabled( wInclFilename.getSelection() );
  }

  public void setIncludeRownum() {
    wlInclRownumField.setEnabled( wInclRownum.getSelection() );
    wInclRownumField.setEnabled( wInclRownum.getSelection() );
  }

  /**
   * Read the data from the TextFileInputMeta object and show it in this dialog.
   *
   * @param in The TextFileInputMeta object to obtain the data from.
   */
  public void getData( LDIFInputMeta in ) {

    wFileField.setSelection( in.isFileField() );
    if ( in.getDynamicFilenameField() != null ) {
      wFilenameField.setText( in.getDynamicFilenameField() );
    }

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
    wInclFilename.setSelection( in.includeFilename() );
    wInclRownum.setSelection( in.includeRowNumber() );
    wInclContentType.setSelection( in.includeContentType() );
    wInclDN.setSelection( in.IncludeDN() );

    if ( in.getMultiValuedSeparator() != null ) {
      wMultiValuedSeparator.setText( in.getMultiValuedSeparator() );
    }

    if ( in.getFilenameField() != null ) {
      wInclFilenameField.setText( in.getFilenameField() );
    }
    if ( in.getRowNumberField() != null ) {
      wInclRownumField.setText( in.getRowNumberField() );
    }
    if ( in.getContentTypeField() != null ) {
      wInclContentTypeField.setText( in.getContentTypeField() );
    }
    if ( in.getDNField() != null ) {
      wInclDNField.setText( in.getDNField() );
    }

    wLimit.setText( "" + in.getRowLimit() );
    wAddResult.setSelection( in.AddToResultFilename() );
    logDebug( BaseMessages.getString( PKG, "LDIFInputDialog.Log.GettingFieldsInfo" ) );
    for ( int i = 0; i < in.getInputFields().length; i++ ) {
      LDIFInputField field = in.getInputFields()[ i ];

      if ( field != null ) {
        TableItem item = wFields.table.getItem( i );
        String name = field.getName();
        String xpath = field.getAttribut();
        String type = field.getTypeDesc();
        String format = field.getFormat();
        String length = "" + field.getLength();
        String prec = "" + field.getPrecision();
        String curr = field.getCurrencySymbol();
        String group = field.getGroupSymbol();
        String decim = field.getDecimalSymbol();
        String trim = field.getTrimTypeDesc();
        String rep =
          field.isRepeated() ? BaseMessages.getString( PKG, "System.Combo.Yes" ) : BaseMessages.getString(
            PKG, "System.Combo.No" );

        if ( name != null ) {
          item.setText( 1, name );
        }
        if ( xpath != null ) {
          item.setText( 2, xpath );
        }
        if ( type != null ) {
          item.setText( 3, type );
        }
        if ( format != null ) {
          item.setText( 4, format );
        }
        if ( length != null && !"-1".equals( length ) ) {
          item.setText( 5, length );
        }
        if ( prec != null && !"-1".equals( prec ) ) {
          item.setText( 6, prec );
        }
        if ( curr != null ) {
          item.setText( 7, curr );
        }
        if ( decim != null ) {
          item.setText( 8, decim );
        }
        if ( group != null ) {
          item.setText( 9, group );
        }
        if ( trim != null ) {
          item.setText( 10, trim );
        }
        if ( rep != null ) {
          item.setText( 11, rep );
        }
      }
    }

    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth( true );
    if ( in.getShortFileNameField() != null ) {
      wShortFileFieldName.setText( in.getShortFileNameField() );
    }
    if ( in.getPathField() != null ) {
      wPathFieldName.setText( in.getPathField() );
    }
    if ( in.isHiddenField() != null ) {
      wIsHiddenName.setText( in.isHiddenField() );
    }
    if ( in.getLastModificationDateField() != null ) {
      wLastModificationTimeName.setText( in.getLastModificationDateField() );
    }
    if ( in.getUriField() != null ) {
      wUriName.setText( in.getUriField() );
    }
    if ( in.getRootUriField() != null ) {
      wRootUriName.setText( in.getRootUriField() );
    }
    if ( in.getExtensionField() != null ) {
      wExtensionFieldName.setText( in.getExtensionField() );
    }
    if ( in.getSizeField() != null ) {
      wSizeFieldName.setText( in.getSizeField() );
    }

    setIncludeFilename();
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
    try {
      getInfo( input );
    } catch ( HopException e ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "LDIFInputDialog.ErrorParsingData.DialogTitle" ), BaseMessages
        .getString( PKG, "LDIFInputDialog.ErrorParsingData.DialogMessage" ), e );
    }
    dispose();
  }

  private void setContenType() {
    wlInclContentTypeField.setEnabled( wInclContentType.getSelection() );
    wInclContentTypeField.setEnabled( wInclContentType.getSelection() );
  }

  private void setDN() {
    wlInclDNField.setEnabled( wInclDN.getSelection() );
    wInclDNField.setEnabled( wInclDN.getSelection() );
  }

  private void getInfo( LDIFInputMeta in ) throws HopException {
    transformName = wTransformName.getText(); // return value

    // copy info to TextFileInputMeta class (input)
    in.setDynamicFilenameField( wFilenameField.getText() );
    in.setFileField( wFileField.getSelection() );

    in.setRowLimit( Const.toLong( wLimit.getText(), 0L ) );
    in.setFilenameField( wInclFilenameField.getText() );
    in.setRowNumberField( wInclRownumField.getText() );
    in.setContentTypeField( wInclContentTypeField.getText() );
    in.setDNField( wInclDNField.getText() );

    in.setIncludeFilename( wInclFilename.getSelection() );
    in.setIncludeRowNumber( wInclRownum.getSelection() );
    in.setIncludeContentType( wInclContentType.getSelection() );
    in.setIncludeDN( wInclDN.getSelection() );

    in.setAddToResultFilename( wAddResult.getSelection() );
    in.setMultiValuedSeparator( wMultiValuedSeparator.getText() );

    int nrFiles = wFilenameList.getItemCount();
    int nrFields = wFields.nrNonEmpty();

    in.allocate( nrFiles, nrFields );

    in.setFileName( wFilenameList.getItems( 0 ) );
    in.setFileMask( wFilenameList.getItems( 1 ) );
    in.setExcludeFileMask( wFilenameList.getItems( 2 ) );
    in.setFileRequired( wFilenameList.getItems( 3 ) );
    in.setIncludeSubFolders( wFilenameList.getItems( 4 ) );

    for ( int i = 0; i < nrFields; i++ ) {
      LDIFInputField field = new LDIFInputField();

      TableItem item = wFields.getNonEmpty( i );

      field.setName( item.getText( 1 ) );
      field.setAttribut( item.getText( 2 ) );
      field.setType( ValueMetaFactory.getIdForValueMeta( item.getText( 3 ) ) );
      field.setFormat( item.getText( 4 ) );
      field.setLength( Const.toInt( item.getText( 5 ), -1 ) );
      field.setPrecision( Const.toInt( item.getText( 6 ), -1 ) );
      field.setCurrencySymbol( item.getText( 7 ) );
      field.setDecimalSymbol( item.getText( 8 ) );
      field.setGroupSymbol( item.getText( 9 ) );
      field.setTrimType( LDIFInputField.getTrimTypeByDesc( item.getText( 10 ) ) );
      field.setRepeated( BaseMessages.getString( PKG, "System.Combo.Yes" ).equalsIgnoreCase( item.getText( 11 ) ) );

      //CHECKSTYLE:Indentation:OFF
      in.getInputFields()[ i ] = field;
    }
    in.setShortFileNameField( wShortFileFieldName.getText() );
    in.setPathField( wPathFieldName.getText() );
    in.setIsHiddenField( wIsHiddenName.getText() );
    in.setLastModificationDateField( wLastModificationTimeName.getText() );
    in.setUriField( wUriName.getText() );
    in.setRootUriField( wRootUriName.getText() );
    in.setExtensionField( wExtensionFieldName.getText() );
    in.setSizeField( wSizeFieldName.getText() );
  }

  // Preview the data
  private void preview() {
    try {
      // Create the LDIF input transform
      LDIFInputMeta oneMeta = new LDIFInputMeta();
      getInfo( oneMeta );

      PipelineMeta previewMeta =
        PipelinePreviewFactory.generatePreviewPipeline( variables, pipelineMeta.getMetadataProvider(), oneMeta, wTransformName.getText() );

      EnterNumberDialog numberDialog = new EnterNumberDialog( shell, props.getDefaultPreviewSize(),
        BaseMessages.getString( PKG, "LDIFInputDialog.NumberRows.DialogTitle" ),
        BaseMessages.getString( PKG, "LDIFInputDialog.NumberRows.DialogMessage" ) );
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
              new EnterTextDialog(
                shell, BaseMessages.getString( PKG, "System.Dialog.PreviewError.Title" ), BaseMessages
                .getString( PKG, "System.Dialog.PreviewError.Message" ), loggingText, true );
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
    } catch ( HopException e ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "LDIFInputDialog.ErrorPreviewingData.DialogTitle" ), BaseMessages
        .getString( PKG, "LDIFInputDialog.ErrorPreviewingData.DialogMessage" ), e );
    }
  }

  private void addAdditionalFieldsTab() {
    // ////////////////////////
    // START OF ADDITIONAL FIELDS TAB ///
    // ////////////////////////
    CTabItem wAdditionalFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdditionalFieldsTab.setText( BaseMessages.getString( PKG, "LDIFInputDialog.AdditionalFieldsTab.TabTitle" ) );

    Composite wAdditionalFieldsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wAdditionalFieldsComp);

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 3;
    fieldsLayout.marginHeight = 3;
    wAdditionalFieldsComp.setLayout( fieldsLayout );

    // ShortFileFieldName line
    Label wlShortFileFieldName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlShortFileFieldName.setText( BaseMessages.getString( PKG, "LDIFInputDialog.ShortFileFieldName.Label" ) );
    props.setLook(wlShortFileFieldName);
    FormData fdlShortFileFieldName = new FormData();
    fdlShortFileFieldName.left = new FormAttachment( 0, 0 );
    fdlShortFileFieldName.top = new FormAttachment( wInclRownumField, margin );
    fdlShortFileFieldName.right = new FormAttachment( middle, -margin );
    wlShortFileFieldName.setLayoutData(fdlShortFileFieldName);

    wShortFileFieldName = new TextVar( variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wShortFileFieldName );
    wShortFileFieldName.addModifyListener( lsMod );
    FormData fdShortFileFieldName = new FormData();
    fdShortFileFieldName.left = new FormAttachment( middle, 0 );
    fdShortFileFieldName.right = new FormAttachment( 100, -margin );
    fdShortFileFieldName.top = new FormAttachment( wInclRownumField, margin );
    wShortFileFieldName.setLayoutData(fdShortFileFieldName);

    // ExtensionFieldName line
    Label wlExtensionFieldName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlExtensionFieldName.setText( BaseMessages.getString( PKG, "LDIFInputDialog.ExtensionFieldName.Label" ) );
    props.setLook(wlExtensionFieldName);
    FormData fdlExtensionFieldName = new FormData();
    fdlExtensionFieldName.left = new FormAttachment( 0, 0 );
    fdlExtensionFieldName.top = new FormAttachment( wShortFileFieldName, margin );
    fdlExtensionFieldName.right = new FormAttachment( middle, -margin );
    wlExtensionFieldName.setLayoutData(fdlExtensionFieldName);

    wExtensionFieldName = new TextVar( variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wExtensionFieldName );
    wExtensionFieldName.addModifyListener( lsMod );
    FormData fdExtensionFieldName = new FormData();
    fdExtensionFieldName.left = new FormAttachment( middle, 0 );
    fdExtensionFieldName.right = new FormAttachment( 100, -margin );
    fdExtensionFieldName.top = new FormAttachment( wShortFileFieldName, margin );
    wExtensionFieldName.setLayoutData(fdExtensionFieldName);

    // PathFieldName line
    Label wlPathFieldName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlPathFieldName.setText( BaseMessages.getString( PKG, "LDIFInputDialog.PathFieldName.Label" ) );
    props.setLook(wlPathFieldName);
    FormData fdlPathFieldName = new FormData();
    fdlPathFieldName.left = new FormAttachment( 0, 0 );
    fdlPathFieldName.top = new FormAttachment( wExtensionFieldName, margin );
    fdlPathFieldName.right = new FormAttachment( middle, -margin );
    wlPathFieldName.setLayoutData(fdlPathFieldName);

    wPathFieldName = new TextVar( variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPathFieldName );
    wPathFieldName.addModifyListener( lsMod );
    FormData fdPathFieldName = new FormData();
    fdPathFieldName.left = new FormAttachment( middle, 0 );
    fdPathFieldName.right = new FormAttachment( 100, -margin );
    fdPathFieldName.top = new FormAttachment( wExtensionFieldName, margin );
    wPathFieldName.setLayoutData(fdPathFieldName);

    // SizeFieldName line
    Label wlSizeFieldName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlSizeFieldName.setText( BaseMessages.getString( PKG, "LDIFInputDialog.SizeFieldName.Label" ) );
    props.setLook(wlSizeFieldName);
    FormData fdlSizeFieldName = new FormData();
    fdlSizeFieldName.left = new FormAttachment( 0, 0 );
    fdlSizeFieldName.top = new FormAttachment( wPathFieldName, margin );
    fdlSizeFieldName.right = new FormAttachment( middle, -margin );
    wlSizeFieldName.setLayoutData(fdlSizeFieldName);

    wSizeFieldName = new TextVar( variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSizeFieldName );
    wSizeFieldName.addModifyListener( lsMod );
    FormData fdSizeFieldName = new FormData();
    fdSizeFieldName.left = new FormAttachment( middle, 0 );
    fdSizeFieldName.right = new FormAttachment( 100, -margin );
    fdSizeFieldName.top = new FormAttachment( wPathFieldName, margin );
    wSizeFieldName.setLayoutData(fdSizeFieldName);

    // IsHiddenName line
    Label wlIsHiddenName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlIsHiddenName.setText( BaseMessages.getString( PKG, "LDIFInputDialog.IsHiddenName.Label" ) );
    props.setLook(wlIsHiddenName);
    FormData fdlIsHiddenName = new FormData();
    fdlIsHiddenName.left = new FormAttachment( 0, 0 );
    fdlIsHiddenName.top = new FormAttachment( wSizeFieldName, margin );
    fdlIsHiddenName.right = new FormAttachment( middle, -margin );
    wlIsHiddenName.setLayoutData(fdlIsHiddenName);

    wIsHiddenName = new TextVar( variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
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
      PKG, "LDIFInputDialog.LastModificationTimeName.Label" ) );
    props.setLook(wlLastModificationTimeName);
    FormData fdlLastModificationTimeName = new FormData();
    fdlLastModificationTimeName.left = new FormAttachment( 0, 0 );
    fdlLastModificationTimeName.top = new FormAttachment( wIsHiddenName, margin );
    fdlLastModificationTimeName.right = new FormAttachment( middle, -margin );
    wlLastModificationTimeName.setLayoutData(fdlLastModificationTimeName);

    wLastModificationTimeName = new TextVar( variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLastModificationTimeName );
    wLastModificationTimeName.addModifyListener( lsMod );
    FormData fdLastModificationTimeName = new FormData();
    fdLastModificationTimeName.left = new FormAttachment( middle, 0 );
    fdLastModificationTimeName.right = new FormAttachment( 100, -margin );
    fdLastModificationTimeName.top = new FormAttachment( wIsHiddenName, margin );
    wLastModificationTimeName.setLayoutData(fdLastModificationTimeName);

    // UriName line
    Label wlUriName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlUriName.setText( BaseMessages.getString( PKG, "LDIFInputDialog.UriName.Label" ) );
    props.setLook(wlUriName);
    FormData fdlUriName = new FormData();
    fdlUriName.left = new FormAttachment( 0, 0 );
    fdlUriName.top = new FormAttachment( wLastModificationTimeName, margin );
    fdlUriName.right = new FormAttachment( middle, -margin );
    wlUriName.setLayoutData(fdlUriName);

    wUriName = new TextVar( variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUriName );
    wUriName.addModifyListener( lsMod );
    FormData fdUriName = new FormData();
    fdUriName.left = new FormAttachment( middle, 0 );
    fdUriName.right = new FormAttachment( 100, -margin );
    fdUriName.top = new FormAttachment( wLastModificationTimeName, margin );
    wUriName.setLayoutData(fdUriName);

    // RootUriName line
    Label wlRootUriName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlRootUriName.setText( BaseMessages.getString( PKG, "LDIFInputDialog.RootUriName.Label" ) );
    props.setLook(wlRootUriName);
    FormData fdlRootUriName = new FormData();
    fdlRootUriName.left = new FormAttachment( 0, 0 );
    fdlRootUriName.top = new FormAttachment( wUriName, margin );
    fdlRootUriName.right = new FormAttachment( middle, -margin );
    wlRootUriName.setLayoutData(fdlRootUriName);

    wRootUriName = new TextVar( variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
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

    // ///////////////////////////////////////////////////////////
    // / END OF ADDITIONAL FIELDS TAB
    // ///////////////////////////////////////////////////////////
  }
}
