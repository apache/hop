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

package org.apache.hop.pipeline.transforms.xml.getxmldata;


import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
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

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;

public class GetXmlDataDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = GetXmlDataMeta.class; // For Translator

  private String XMLSource = null;

  private CTabFolder wTabFolder;

  private Label wlFilename, wlXMLIsAFile;
  private Button wbbFilename; // Browse: add file or directory
  private Button wbdFilename; // Delete
  private Button wbeFilename; // Edit
  private Button wbaFilename; // Add or change
  private TextVar wFilename;

  private Label wlFilenameList;
  private TableView wFilenameList;

  private Label wlFilemask;
  private TextVar wFilemask;

  private Button wbShowFiles;

  private Button wUseToken;

  private Label wlXMLField;
  private CCombo wXMLField;
  private Button wXMLStreamField, wXMLIsAFile;

  private Label wlInclFilename;
  private Button wInclFilename, wAddResult;

  private Button wNameSpaceAware;

  private Label wlReadUrl;
  private Button wReadUrl;

  private Button wIgnoreComment;

  private Button wValidating;

  private Label wlInclFilenameField;
  private TextVar wInclFilenameField;

  private Label wlAddResult;
  private Button wInclRownum;

  private Label wlInclRownumField;
  private TextVar wInclRownumField;

  private Label wlLimit;
  private Text wLimit;

  private TextVar wLoopXPath;

  private Label wlPrunePath;
  private TextVar wPrunePath;

  private Label wlEncoding;
  private CCombo wEncoding;

  private TableView wFields;

  private Label wlExcludeFilemask;
  private TextVar wExcludeFilemask;

  private Button wIgnoreEmptyFile;

  private Button wDoNotFailIfNoFile;

  private Label wlShortFileFieldName;
  private TextVar wShortFileFieldName;
  private Label wlPathFieldName;
  private TextVar wPathFieldName;

  private Label wlIsHiddenName;
  private TextVar wIsHiddenName;
  private Label wlLastModificationTimeName;
  private TextVar wLastModificationTimeName;
  private Label wlUriName;
  private TextVar wUriName;
  private Label wlRootUriName;
  private TextVar wRootUriName;
  private Label wlExtensionFieldName;
  private TextVar wExtensionFieldName;
  private Label wlSizeFieldName;
  private TextVar wSizeFieldName;

  private final GetXmlDataMeta input;

  private int middle;
  private int margin;
  private ModifyListener lsMod;

  private boolean gotEncodings = false;

  public static final int[] dateLengths = new int[] { 23, 19, 14, 10, 10, 10, 10, 8, 8, 8, 8, 6, 6 };

  String precNodeName = null;

  public GetXmlDataDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (GetXmlDataMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.DialogTitle" ) );

    middle = props.getMiddlePct();
    margin = Const.MARGIN;

    // Buttons go at the bottom
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wPreview = new Button( shell, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.Button.PreviewRows" ) );
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
    CTabItem wFileTab = new CTabItem( wTabFolder, SWT.NONE );
    wFileTab.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.File.Tab" ) );

    Composite wFileComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wFileComp );

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wFileComp.setLayout( fileLayout );

    // ///////////////////////////////
    // START OF Output Field GROUP //
    // ///////////////////////////////

    Group wOutputField = new Group( wFileComp, SWT.SHADOW_NONE );
    props.setLook( wOutputField );
    wOutputField.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.wOutputField.Label" ) );

    FormLayout outputfieldgroupLayout = new FormLayout();
    outputfieldgroupLayout.marginWidth = 10;
    outputfieldgroupLayout.marginHeight = 10;
    wOutputField.setLayout( outputfieldgroupLayout );

    // Is XML string defined in a Field
    Label wlXmlStreamField = new Label( wOutputField, SWT.RIGHT );
    wlXmlStreamField.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.wlXmlStreamField.Label" ) );
    props.setLook( wlXmlStreamField );
    FormData fdlXMLStreamField = new FormData();
    fdlXMLStreamField.left = new FormAttachment( 0, -margin );
    fdlXMLStreamField.top = new FormAttachment( 0, margin );
    fdlXMLStreamField.right = new FormAttachment( middle, -2 * margin );
    wlXmlStreamField.setLayoutData( fdlXMLStreamField );

    wXMLStreamField = new Button( wOutputField, SWT.CHECK );
    props.setLook( wXMLStreamField );
    wXMLStreamField.setToolTipText( BaseMessages.getString( PKG, "GetXMLDataDialog.wXmlStreamField.Tooltip" ) );
    FormData fdXSDFileField = new FormData();
    fdXSDFileField.left = new FormAttachment( middle, -margin );
    fdXSDFileField.top = new FormAttachment( wlXmlStreamField, 0, SWT.CENTER );
    wXMLStreamField.setLayoutData( fdXSDFileField );
    wXMLStreamField.addListener( SWT.Selection, e -> {
      XMLSource = null;
      ActiveXmlStreamField();
      input.setChanged();
    } );

    // Is XML source is a file?
    wlXMLIsAFile = new Label( wOutputField, SWT.RIGHT );
    wlXMLIsAFile.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.XMLIsAFile.Label" ) );
    props.setLook( wlXMLIsAFile );
    FormData fdlXMLIsAFile = new FormData();
    fdlXMLIsAFile.left = new FormAttachment( 0, -margin );
    fdlXMLIsAFile.top = new FormAttachment( wXMLStreamField, margin );
    fdlXMLIsAFile.right = new FormAttachment( middle, -2 * margin );
    wlXMLIsAFile.setLayoutData( fdlXMLIsAFile );

    wXMLIsAFile = new Button( wOutputField, SWT.CHECK );
    props.setLook( wXMLIsAFile );
    wXMLIsAFile.setToolTipText( BaseMessages.getString( PKG, "GetXMLDataDialog.XMLIsAFile.Tooltip" ) );
    FormData fdXMLIsAFile = new FormData();
    fdXMLIsAFile.left = new FormAttachment( middle, -margin );
    fdXMLIsAFile.top = new FormAttachment( wlXMLIsAFile, 0, SWT.CENTER );
    wXMLIsAFile.setLayoutData( fdXMLIsAFile );
    wXMLIsAFile.addListener( SWT.Selection, e -> {
      XMLSource = null;
      if ( wXMLIsAFile.getSelection() ) {
        wReadUrl.setSelection( false );
      }
      input.setChanged();
    } );

    // read url as source ?
    wlReadUrl = new Label( wOutputField, SWT.RIGHT );
    wlReadUrl.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.readUrl.Label" ) );
    props.setLook( wlReadUrl );
    FormData fdlreadUrl = new FormData();
    fdlreadUrl.left = new FormAttachment( 0, -margin );
    fdlreadUrl.top = new FormAttachment( wXMLIsAFile, margin );
    fdlreadUrl.right = new FormAttachment( middle, -2 * margin );
    wlReadUrl.setLayoutData( fdlreadUrl );
    wReadUrl = new Button( wOutputField, SWT.CHECK );
    props.setLook( wReadUrl );
    wReadUrl.setToolTipText( BaseMessages.getString( PKG, "GetXMLDataDialog.readUrl.Tooltip" ) );
    FormData fdreadUrl = new FormData();
    fdreadUrl.left = new FormAttachment( middle, -margin );
    fdreadUrl.top = new FormAttachment( wlReadUrl, 0, SWT.CENTER );
    wReadUrl.setLayoutData( fdreadUrl );
    SelectionAdapter lsreadurl = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        XMLSource = null;
        if ( wReadUrl.getSelection() ) {
          wXMLIsAFile.setSelection( false );
        }
        input.setChanged();
      }
    };
    wReadUrl.addSelectionListener( lsreadurl );

    // If XML string defined in a Field
    wlXMLField = new Label( wOutputField, SWT.RIGHT );
    wlXMLField.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.wlXMLField.Label" ) );
    props.setLook( wlXMLField );
    FormData fdlXMLField = new FormData();
    fdlXMLField.left = new FormAttachment( 0, -margin );
    fdlXMLField.top = new FormAttachment( wReadUrl, margin );
    fdlXMLField.right = new FormAttachment( middle, -2 * margin );
    wlXMLField.setLayoutData( fdlXMLField );

    wXMLField = new CCombo( wOutputField, SWT.BORDER | SWT.READ_ONLY );
    wXMLField.setEditable( true );
    props.setLook( wXMLField );
    wXMLField.addModifyListener( lsMod );
    FormData fdXMLField = new FormData();
    fdXMLField.left = new FormAttachment( middle, -margin );
    fdXMLField.top = new FormAttachment( wReadUrl, margin );
    fdXMLField.right = new FormAttachment( 100, -margin );
    wXMLField.setLayoutData( fdXMLField );
    wXMLField.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        setXMLStreamField();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    FormData fdOutputField = new FormData();
    fdOutputField.left = new FormAttachment( 0, margin );
    fdOutputField.top = new FormAttachment( wFilenameList, margin );
    fdOutputField.right = new FormAttachment( 100, -margin );
    wOutputField.setLayoutData( fdOutputField );

    // ///////////////////////////////////////////////////////////
    // / END OF Output Field GROUP
    // ///////////////////////////////////////////////////////////

    // Filename line
    wlFilename = new Label( wFileComp, SWT.RIGHT );
    wlFilename.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.Filename.Label" ) );
    props.setLook( wlFilename );
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment( wOutputField, margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData( fdlFilename );

    wbbFilename = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbbFilename );
    wbbFilename.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.FilenameBrowse.Button" ) );
    wbbFilename.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.BrowseForFileOrDirAndAdd" ) );
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment( wOutputField, margin );
    wbbFilename.setLayoutData( fdbFilename );

    wbaFilename = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbaFilename );
    wbaFilename.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.FilenameAdd.Button" ) );
    wbaFilename.setToolTipText( BaseMessages.getString( PKG, "GetXMLDataDialog.FilenameAdd.Tooltip" ) );
    FormData fdbaFilename = new FormData();
    fdbaFilename.right = new FormAttachment( wbbFilename, -margin );
    fdbaFilename.top = new FormAttachment( wOutputField, margin );
    wbaFilename.setLayoutData( fdbaFilename );

    wFilename = new TextVar( variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.right = new FormAttachment( wbaFilename, -margin );
    fdFilename.top = new FormAttachment( wOutputField, margin );
    wFilename.setLayoutData( fdFilename );

    wlFilemask = new Label( wFileComp, SWT.RIGHT );
    wlFilemask.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.RegExp.Label" ) );
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
    fdFilemask.right = new FormAttachment( 100, 0 );
    wFilemask.setLayoutData( fdFilemask );

    wlExcludeFilemask = new Label( wFileComp, SWT.RIGHT );
    wlExcludeFilemask.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.ExcludeFilemask.Label" ) );
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
    wlFilenameList.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.FilenameList.Label" ) );
    props.setLook( wlFilenameList );
    FormData fdlFilenameList = new FormData();
    fdlFilenameList.left = new FormAttachment( 0, 0 );
    fdlFilenameList.top = new FormAttachment( wExcludeFilemask, margin );
    fdlFilenameList.right = new FormAttachment( middle, -margin );
    wlFilenameList.setLayoutData( fdlFilenameList );

    // Buttons to the right of the screen...
    wbdFilename = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbdFilename );
    wbdFilename.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.FilenameRemove.Button" ) );
    wbdFilename.setToolTipText( BaseMessages.getString( PKG, "GetXMLDataDialog.FilenameRemove.Tooltip" ) );
    FormData fdbdFilename = new FormData();
    fdbdFilename.right = new FormAttachment( 100, 0 );
    fdbdFilename.top = new FormAttachment( wExcludeFilemask, 40 );
    wbdFilename.setLayoutData( fdbdFilename );

    wbeFilename = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbeFilename );
    wbeFilename.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.FilenameEdit.Button" ) );
    wbeFilename.setToolTipText( BaseMessages.getString( PKG, "GetXMLDataDialog.FilenameEdit.Tooltip" ) );
    FormData fdbeFilename = new FormData();
    fdbeFilename.right = new FormAttachment( 100, 0 );
    fdbeFilename.left = new FormAttachment( wbdFilename, 0, SWT.LEFT );
    fdbeFilename.top = new FormAttachment( wbdFilename, margin );
    wbeFilename.setLayoutData( fdbeFilename );

    wbShowFiles = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbShowFiles );
    wbShowFiles.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.ShowFiles.Button" ) );
    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment( middle, 0 );
    fdbShowFiles.bottom = new FormAttachment( 100, 0 );
    wbShowFiles.setLayoutData( fdbShowFiles );

    ColumnInfo[] colinfo = new ColumnInfo[ 5 ];
    colinfo[ 0 ] =
      new ColumnInfo( BaseMessages.getString( PKG, "GetXMLDataDialog.Files.Filename.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinfo[ 1 ] =
      new ColumnInfo( BaseMessages.getString( PKG, "GetXMLDataDialog.Files.Wildcard.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinfo[ 2 ] =
      new ColumnInfo( BaseMessages.getString( PKG, "GetXMLDataDialog.Files.ExcludeWildcard.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );

    colinfo[ 0 ].setUsingVariables( true );
    colinfo[ 1 ].setUsingVariables( true );
    colinfo[ 1 ].setToolTip( BaseMessages.getString( PKG, "GetXMLDataDialog.Files.Wildcard.Tooltip" ) );
    colinfo[ 2 ].setUsingVariables( true );
    colinfo[ 2 ].setToolTip( BaseMessages.getString( PKG, "GetXMLDataDialog.Files.ExcludeWildcard.Tooltip" ) );
    colinfo[ 3 ] =
      new ColumnInfo( BaseMessages.getString( PKG, "GetXMLDataDialog.Required.Column" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, GetXmlDataMeta.RequiredFilesDesc );
    colinfo[ 3 ].setToolTip( BaseMessages.getString( PKG, "GetXMLDataDialog.Required.Tooltip" ) );
    colinfo[ 4 ] =
      new ColumnInfo( BaseMessages.getString( PKG, "GetXMLDataDialog.IncludeSubDirs.Column" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, GetXmlDataMeta.RequiredFilesDesc );
    colinfo[ 4 ].setToolTip( BaseMessages.getString( PKG, "GetXMLDataDialog.IncludeSubDirs.Tooltip" ) );

    wFilenameList =
      new TableView( variables, wFileComp, SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER, colinfo, 2, lsMod, props );
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

    // ////////////////////////
    // START OF CONTENT TAB///
    // /
    CTabItem wContentTab = new CTabItem( wTabFolder, SWT.NONE );
    wContentTab.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.Content.Tab" ) );

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    Composite wContentComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wContentComp );
    wContentComp.setLayout( contentLayout );

    // ///////////////////////////////
    // START OF XmlConf Field GROUP //
    // ///////////////////////////////

    Group wXmlConf = new Group( wContentComp, SWT.SHADOW_NONE );
    props.setLook( wXmlConf );
    wXmlConf.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.wXmlConf.Label" ) );

    FormLayout XmlConfgroupLayout = new FormLayout();
    XmlConfgroupLayout.marginWidth = 10;
    XmlConfgroupLayout.marginHeight = 10;
    wXmlConf.setLayout( XmlConfgroupLayout );

    Button wbbLoopPathList = new Button( wXmlConf, SWT.PUSH | SWT.CENTER );
    props.setLook( wbbLoopPathList );
    wbbLoopPathList.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.LoopPathList.Button" ) );
    wbbLoopPathList.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.BrowseForFileOrDirAndAdd" ) );
    FormData fdbLoopPathList = new FormData();
    fdbLoopPathList.right = new FormAttachment( 100, 0 );
    fdbLoopPathList.top = new FormAttachment( 0, 0 );
    wbbLoopPathList.setLayoutData( fdbLoopPathList );

    wbbLoopPathList.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        getLoopPathList();
      }
    } );

    Label wlLoopXPath = new Label( wXmlConf, SWT.RIGHT );
    wlLoopXPath.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.LoopXPath.Label" ) );
    props.setLook( wlLoopXPath );
    FormData fdlLoopXPath = new FormData();
    fdlLoopXPath.left = new FormAttachment( 0, 0 );
    fdlLoopXPath.top = new FormAttachment( 0, margin );
    fdlLoopXPath.right = new FormAttachment( middle, -margin );
    wlLoopXPath.setLayoutData( fdlLoopXPath );
    wLoopXPath = new TextVar( variables, wXmlConf, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wLoopXPath.setToolTipText( BaseMessages.getString( PKG, "GetXMLDataDialog.LoopXPath.Tooltip" ) );
    props.setLook( wLoopXPath );
    wLoopXPath.addModifyListener( lsMod );
    FormData fdLoopXPath = new FormData();
    fdLoopXPath.left = new FormAttachment( middle, 0 );
    fdLoopXPath.top = new FormAttachment( 0, margin );
    fdLoopXPath.right = new FormAttachment( wbbLoopPathList, -margin );
    wLoopXPath.setLayoutData( fdLoopXPath );

    wlEncoding = new Label( wXmlConf, SWT.RIGHT );
    wlEncoding.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.Encoding.Label" ) );
    props.setLook( wlEncoding );
    FormData fdlEncoding = new FormData();
    fdlEncoding.left = new FormAttachment( 0, 0 );
    fdlEncoding.top = new FormAttachment( wLoopXPath, margin );
    fdlEncoding.right = new FormAttachment( middle, -margin );
    wlEncoding.setLayoutData( fdlEncoding );
    wEncoding = new CCombo( wXmlConf, SWT.BORDER | SWT.READ_ONLY );
    wEncoding.setEditable( true );
    props.setLook( wEncoding );
    wEncoding.addModifyListener( lsMod );
    FormData fdEncoding = new FormData();
    fdEncoding.left = new FormAttachment( middle, 0 );
    fdEncoding.top = new FormAttachment( wLoopXPath, margin );
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

    // Set Namespace aware ?
    Label wlNameSpaceAware = new Label( wXmlConf, SWT.RIGHT );
    wlNameSpaceAware.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.NameSpaceAware.Label" ) );
    props.setLook( wlNameSpaceAware );
    FormData fdlNameSpaceAware = new FormData();
    fdlNameSpaceAware.left = new FormAttachment( 0, 0 );
    fdlNameSpaceAware.top = new FormAttachment( wEncoding, margin );
    fdlNameSpaceAware.right = new FormAttachment( middle, -margin );
    wlNameSpaceAware.setLayoutData( fdlNameSpaceAware );
    wNameSpaceAware = new Button( wXmlConf, SWT.CHECK );
    props.setLook( wNameSpaceAware );
    wNameSpaceAware.setToolTipText( BaseMessages.getString( PKG, "GetXMLDataDialog.NameSpaceAware.Tooltip" ) );
    FormData fdNameSpaceAware = new FormData();
    fdNameSpaceAware.left = new FormAttachment( middle, 0 );
    fdNameSpaceAware.top = new FormAttachment( wlNameSpaceAware, 0, SWT.CENTER );
    wNameSpaceAware.setLayoutData( fdNameSpaceAware );

    // Ignore comments ?
    Label wlIgnoreComment = new Label( wXmlConf, SWT.RIGHT );
    wlIgnoreComment.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.IgnoreComment.Label" ) );
    props.setLook( wlIgnoreComment );
    FormData fdlIgnoreComment = new FormData();
    fdlIgnoreComment.left = new FormAttachment( 0, 0 );
    fdlIgnoreComment.top = new FormAttachment( wNameSpaceAware, margin );
    fdlIgnoreComment.right = new FormAttachment( middle, -margin );
    wlIgnoreComment.setLayoutData( fdlIgnoreComment );
    wIgnoreComment = new Button( wXmlConf, SWT.CHECK );
    props.setLook( wIgnoreComment );
    wIgnoreComment.setToolTipText( BaseMessages.getString( PKG, "GetXMLDataDialog.IgnoreComment.Tooltip" ) );
    FormData fdIgnoreComment = new FormData();
    fdIgnoreComment.left = new FormAttachment( middle, 0 );
    fdIgnoreComment.top = new FormAttachment( wlIgnoreComment, 0, SWT.CENTER );
    wIgnoreComment.setLayoutData( fdIgnoreComment );

    // Validate XML?
    Label wlValidating = new Label( wXmlConf, SWT.RIGHT );
    wlValidating.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.Validating.Label" ) );
    props.setLook( wlValidating );
    FormData fdlValidating = new FormData();
    fdlValidating.left = new FormAttachment( 0, 0 );
    fdlValidating.top = new FormAttachment( wIgnoreComment, margin );
    fdlValidating.right = new FormAttachment( middle, -margin );
    wlValidating.setLayoutData( fdlValidating );
    wValidating = new Button( wXmlConf, SWT.CHECK );
    props.setLook( wValidating );
    wValidating.setToolTipText( BaseMessages.getString( PKG, "GetXMLDataDialog.Validating.Tooltip" ) );
    FormData fdValidating = new FormData();
    fdValidating.left = new FormAttachment( middle, 0 );
    fdValidating.top = new FormAttachment( wlValidating, 0, SWT.CENTER );
    wValidating.setLayoutData( fdValidating );

    // use Token ?
    Label wlUseToken = new Label( wXmlConf, SWT.RIGHT );
    wlUseToken.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.useToken.Label" ) );
    props.setLook( wlUseToken );
    FormData fdluseToken = new FormData();
    fdluseToken.left = new FormAttachment( 0, 0 );
    fdluseToken.top = new FormAttachment( wValidating, margin );
    fdluseToken.right = new FormAttachment( middle, -margin );
    wlUseToken.setLayoutData( fdluseToken );
    wUseToken = new Button( wXmlConf, SWT.CHECK );
    props.setLook( wUseToken );
    wUseToken.setToolTipText( BaseMessages.getString( PKG, "GetXMLDataDialog.useToken.Tooltip" ) );
    FormData fduseToken = new FormData();
    fduseToken.left = new FormAttachment( middle, 0 );
    fduseToken.top = new FormAttachment( wlUseToken, 0, SWT.CENTER );
    wUseToken.setLayoutData( fduseToken );

    // Ignore Empty File
    // ignore empty files flag
    Label wlIgnoreEmptyFile = new Label( wXmlConf, SWT.RIGHT );
    wlIgnoreEmptyFile.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.IgnoreEmptyFile.Label" ) );
    props.setLook( wlIgnoreEmptyFile );
    FormData fdlIgnoreEmptyFile = new FormData();
    fdlIgnoreEmptyFile.left = new FormAttachment( 0, 0 );
    fdlIgnoreEmptyFile.top = new FormAttachment( wUseToken, margin );
    fdlIgnoreEmptyFile.right = new FormAttachment( middle, -margin );
    wlIgnoreEmptyFile.setLayoutData( fdlIgnoreEmptyFile );
    wIgnoreEmptyFile = new Button( wXmlConf, SWT.CHECK );
    props.setLook( wIgnoreEmptyFile );
    wIgnoreEmptyFile.setToolTipText( BaseMessages.getString( PKG, "GetXMLDataDialog.IgnoreEmptyFile.Tooltip" ) );
    FormData fdIgnoreEmptyFile = new FormData();
    fdIgnoreEmptyFile.left = new FormAttachment( middle, 0 );
    fdIgnoreEmptyFile.top = new FormAttachment( wlIgnoreEmptyFile, 0, SWT.CENTER );
    wIgnoreEmptyFile.setLayoutData( fdIgnoreEmptyFile );

    // do not fail if no files?
    // do not fail if no files?
    Label wlDoNotFailIfNoFile = new Label( wXmlConf, SWT.RIGHT );
    wlDoNotFailIfNoFile.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.doNotFailIfNoFile.Label" ) );
    props.setLook( wlDoNotFailIfNoFile );
    FormData fdldoNotFailIfNoFile = new FormData();
    fdldoNotFailIfNoFile.left = new FormAttachment( 0, 0 );
    fdldoNotFailIfNoFile.top = new FormAttachment( wIgnoreEmptyFile, margin );
    fdldoNotFailIfNoFile.right = new FormAttachment( middle, -margin );
    wlDoNotFailIfNoFile.setLayoutData( fdldoNotFailIfNoFile );
    wDoNotFailIfNoFile = new Button( wXmlConf, SWT.CHECK );
    props.setLook( wDoNotFailIfNoFile );
    wDoNotFailIfNoFile.setToolTipText( BaseMessages.getString( PKG, "GetXMLDataDialog.doNotFailIfNoFile.Tooltip" ) );
    FormData fddoNotFailIfNoFile = new FormData();
    fddoNotFailIfNoFile.left = new FormAttachment( middle, 0 );
    fddoNotFailIfNoFile.top = new FormAttachment( wlDoNotFailIfNoFile, 0, SWT.CENTER );
    wDoNotFailIfNoFile.setLayoutData( fddoNotFailIfNoFile );

    wlLimit = new Label( wXmlConf, SWT.RIGHT );
    wlLimit.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.Limit.Label" ) );
    props.setLook( wlLimit );
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment( 0, 0 );
    fdlLimit.top = new FormAttachment( wDoNotFailIfNoFile, margin );
    fdlLimit.right = new FormAttachment( middle, -margin );
    wlLimit.setLayoutData( fdlLimit );
    wLimit = new Text( wXmlConf, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLimit );
    wLimit.addModifyListener( lsMod );
    FormData fdLimit = new FormData();
    fdLimit.left = new FormAttachment( middle, 0 );
    fdLimit.top = new FormAttachment( wDoNotFailIfNoFile, margin );
    fdLimit.right = new FormAttachment( 100, 0 );
    wLimit.setLayoutData( fdLimit );

    // Prune path to handle large files (streaming mode)
    wlPrunePath = new Label( wXmlConf, SWT.RIGHT );
    wlPrunePath.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.StreamingMode.Label" ) );
    props.setLook( wlPrunePath );
    FormData fdlPrunePath = new FormData();
    fdlPrunePath.left = new FormAttachment( 0, 0 );
    fdlPrunePath.top = new FormAttachment( wLimit, margin );
    fdlPrunePath.right = new FormAttachment( middle, -margin );
    wlPrunePath.setLayoutData( fdlPrunePath );
    wPrunePath = new TextVar( variables, wXmlConf, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wPrunePath.setToolTipText( BaseMessages.getString( PKG, "GetXMLDataDialog.StreamingMode.Tooltip" ) );
    props.setLook( wPrunePath );
    wPrunePath.addModifyListener( lsMod );
    FormData fdPrunePath = new FormData();
    fdPrunePath.left = new FormAttachment( middle, 0 );
    fdPrunePath.top = new FormAttachment( wLimit, margin );
    fdPrunePath.right = new FormAttachment( 100, 0 );
    wPrunePath.setLayoutData( fdPrunePath );

    FormData fdXmlConf = new FormData();
    fdXmlConf.left = new FormAttachment( 0, margin );
    fdXmlConf.top = new FormAttachment( 0, margin );
    fdXmlConf.right = new FormAttachment( 100, -margin );
    wXmlConf.setLayoutData( fdXmlConf );

    // ///////////////////////////////////////////////////////////
    // / END OF XmlConf Field GROUP
    // ///////////////////////////////////////////////////////////

    // ///////////////////////////////
    // START OF Additional Fields GROUP //
    // ///////////////////////////////

    Group wAdditionalFields = new Group( wContentComp, SWT.SHADOW_NONE );
    props.setLook( wAdditionalFields );
    wAdditionalFields.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.wAdditionalFields.Label" ) );

    FormLayout additionalFieldsgroupLayout = new FormLayout();
    additionalFieldsgroupLayout.marginWidth = 10;
    additionalFieldsgroupLayout.marginHeight = 10;
    wAdditionalFields.setLayout( additionalFieldsgroupLayout );

    wlInclFilename = new Label( wAdditionalFields, SWT.RIGHT );
    wlInclFilename.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.InclFilename.Label" ) );
    props.setLook( wlInclFilename );
    FormData fdlInclFilename = new FormData();
    fdlInclFilename.left = new FormAttachment( 0, 0 );
    fdlInclFilename.top = new FormAttachment( wXmlConf, 4 * margin );
    fdlInclFilename.right = new FormAttachment( middle, -margin );
    wlInclFilename.setLayoutData( fdlInclFilename );
    wInclFilename = new Button( wAdditionalFields, SWT.CHECK );
    props.setLook( wInclFilename );
    wInclFilename.setToolTipText( BaseMessages.getString( PKG, "GetXMLDataDialog.InclFilename.Tooltip" ) );
    FormData fdInclFilename = new FormData();
    fdInclFilename.left = new FormAttachment( middle, 0 );
    fdInclFilename.top = new FormAttachment( wlInclFilename, 0, SWT.CENTER );
    wInclFilename.setLayoutData( fdInclFilename );

    wlInclFilenameField = new Label( wAdditionalFields, SWT.LEFT );
    wlInclFilenameField.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.InclFilenameField.Label" ) );
    props.setLook( wlInclFilenameField );
    FormData fdlInclFilenameField = new FormData();
    fdlInclFilenameField.left = new FormAttachment( wInclFilename, margin );
    fdlInclFilenameField.top = new FormAttachment( wLimit, 4 * margin );
    wlInclFilenameField.setLayoutData( fdlInclFilenameField );
    wInclFilenameField = new TextVar( variables, wAdditionalFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclFilenameField );
    wInclFilenameField.addModifyListener( lsMod );
    FormData fdInclFilenameField = new FormData();
    fdInclFilenameField.left = new FormAttachment( wlInclFilenameField, margin );
    fdInclFilenameField.top = new FormAttachment( wLimit, 4 * margin );
    fdInclFilenameField.right = new FormAttachment( 100, 0 );
    wInclFilenameField.setLayoutData( fdInclFilenameField );

    Label wlInclRownum = new Label( wAdditionalFields, SWT.RIGHT );
    wlInclRownum.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.InclRownum.Label" ) );
    props.setLook( wlInclRownum );
    FormData fdlInclRownum = new FormData();
    fdlInclRownum.left = new FormAttachment( 0, 0 );
    fdlInclRownum.top = new FormAttachment( wInclFilenameField, margin );
    fdlInclRownum.right = new FormAttachment( middle, -margin );
    wlInclRownum.setLayoutData( fdlInclRownum );
    wInclRownum = new Button( wAdditionalFields, SWT.CHECK );
    props.setLook( wInclRownum );
    wInclRownum.setToolTipText( BaseMessages.getString( PKG, "GetXMLDataDialog.InclRownum.Tooltip" ) );
    FormData fdRownum = new FormData();
    fdRownum.left = new FormAttachment( middle, 0 );
    fdRownum.top = new FormAttachment( wlInclRownum, 0, SWT.CENTER );
    wInclRownum.setLayoutData( fdRownum );

    wlInclRownumField = new Label( wAdditionalFields, SWT.RIGHT );
    wlInclRownumField.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.InclRownumField.Label" ) );
    props.setLook( wlInclRownumField );
    FormData fdlInclRownumField = new FormData();
    fdlInclRownumField.left = new FormAttachment( wInclRownum, margin );
    fdlInclRownumField.top = new FormAttachment( wInclFilenameField, margin );
    wlInclRownumField.setLayoutData( fdlInclRownumField );
    wInclRownumField = new TextVar( variables, wAdditionalFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclRownumField );
    wInclRownumField.addModifyListener( lsMod );
    FormData fdInclRownumField = new FormData();
    fdInclRownumField.left = new FormAttachment( wlInclRownumField, margin );
    fdInclRownumField.top = new FormAttachment( wInclFilenameField, margin );
    fdInclRownumField.right = new FormAttachment( 100, 0 );
    wInclRownumField.setLayoutData( fdInclRownumField );

    FormData fdAdditionalFields = new FormData();
    fdAdditionalFields.left = new FormAttachment( 0, margin );
    fdAdditionalFields.top = new FormAttachment( wXmlConf, margin );
    fdAdditionalFields.right = new FormAttachment( 100, -margin );
    wAdditionalFields.setLayoutData( fdAdditionalFields );

    // ///////////////////////////////////////////////////////////
    // / END OF Additional Fields GROUP
    // ///////////////////////////////////////////////////////////

    // ///////////////////////////////
    // START OF AddFileResult GROUP //
    // ///////////////////////////////

    Group wAddFileResult = new Group( wContentComp, SWT.SHADOW_NONE );
    props.setLook( wAddFileResult );
    wAddFileResult.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.wAddFileResult.Label" ) );

    FormLayout AddFileResultgroupLayout = new FormLayout();
    AddFileResultgroupLayout.marginWidth = 10;
    AddFileResultgroupLayout.marginHeight = 10;
    wAddFileResult.setLayout( AddFileResultgroupLayout );

    wlAddResult = new Label( wAddFileResult, SWT.RIGHT );
    wlAddResult.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.AddResult.Label" ) );
    props.setLook( wlAddResult );
    FormData fdlAddResult = new FormData();
    fdlAddResult.left = new FormAttachment( 0, 0 );
    fdlAddResult.top = new FormAttachment( wAdditionalFields, margin );
    fdlAddResult.right = new FormAttachment( middle, -margin );
    wlAddResult.setLayoutData( fdlAddResult );
    wAddResult = new Button( wAddFileResult, SWT.CHECK );
    props.setLook( wAddResult );
    wAddResult.setToolTipText( BaseMessages.getString( PKG, "GetXMLDataDialog.AddResult.Tooltip" ) );
    FormData fdAddResult = new FormData();
    fdAddResult.left = new FormAttachment( middle, 0 );
    fdAddResult.top = new FormAttachment( wlAddResult, 0, SWT.CENTER );
    wAddResult.setLayoutData( fdAddResult );

    FormData fdAddFileResult = new FormData();
    fdAddFileResult.left = new FormAttachment( 0, margin );
    fdAddFileResult.top = new FormAttachment( wAdditionalFields, margin );
    fdAddFileResult.right = new FormAttachment( 100, -margin );
    wAddFileResult.setLayoutData( fdAddFileResult );

    // ///////////////////////////////////////////////////////////
    // / END OF AddFileResult GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdContentComp = new FormData();
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

    // Fields tab...
    //
    CTabItem wFieldsTab = new CTabItem( wTabFolder, SWT.NONE );
    wFieldsTab.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.Fields.Tab" ) );

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;

    Composite wFieldsComp = new Composite( wTabFolder, SWT.NONE );
    wFieldsComp.setLayout( fieldsLayout );
    props.setLook( wFieldsComp );

    wGet = new Button( wFieldsComp, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.GetFields.Button" ) );
    fdGet = new FormData();
    fdGet.left = new FormAttachment( 50, 0 );
    fdGet.bottom = new FormAttachment( 100, 0 );
    wGet.setLayoutData( fdGet );

    final int FieldsRows = input.getInputFields().length;

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo( BaseMessages.getString( PKG, "GetXMLDataDialog.FieldsTable.Name.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( PKG, "GetXMLDataDialog.FieldsTable.XPath.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( PKG, "GetXMLDataDialog.FieldsTable.Element.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, GetXmlDataField.ElementTypeDesc, true ),
        new ColumnInfo( BaseMessages.getString( PKG, "GetXMLDataDialog.FieldsTable.ResultType.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, GetXmlDataField.ResultTypeDesc, true ),
        new ColumnInfo( BaseMessages.getString( PKG, "GetXMLDataDialog.FieldsTable.Type.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaBase.getTypes(), true ),
        new ColumnInfo( BaseMessages.getString( PKG, "GetXMLDataDialog.FieldsTable.Format.Column" ),
          ColumnInfo.COLUMN_TYPE_FORMAT, 4 ),
        new ColumnInfo( BaseMessages.getString( PKG, "GetXMLDataDialog.FieldsTable.Length.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( PKG, "GetXMLDataDialog.FieldsTable.Precision.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( PKG, "GetXMLDataDialog.FieldsTable.Currency.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( PKG, "GetXMLDataDialog.FieldsTable.Decimal.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( PKG, "GetXMLDataDialog.FieldsTable.Group.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( PKG, "GetXMLDataDialog.FieldsTable.TrimType.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, GetXmlDataField.trimTypeDesc, true ),
        new ColumnInfo( BaseMessages.getString( PKG, "GetXMLDataDialog.FieldsTable.Repeat.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { BaseMessages.getString( PKG, "System.Combo.Yes" ),
          BaseMessages.getString( PKG, "System.Combo.No" ) }, true ),

      };

    colinf[ 0 ].setUsingVariables( true );
    colinf[ 0 ].setToolTip( BaseMessages.getString( PKG, "GetXMLDataDialog.FieldsTable.Name.Column.Tooltip" ) );
    colinf[ 1 ].setUsingVariables( true );
    colinf[ 1 ].setToolTip( BaseMessages.getString( PKG, "GetXMLDataDialog.FieldsTable.XPath.Column.Tooltip" ) );

    wFields = new TableView( variables, wFieldsComp, SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( 0, 0 );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wGet, -margin );
    wFields.setLayoutData( fdFields );

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment( 0, 0 );
    fdFieldsComp.top = new FormAttachment( 0, 0 );
    fdFieldsComp.right = new FormAttachment( 100, 0 );
    fdFieldsComp.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData( fdFieldsComp );

    wFieldsComp.layout();
    wFieldsTab.setControl( wFieldsComp );

    addAdditionalFieldsTab();

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wTransformName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOk, -2 * margin );
    wTabFolder.setLayoutData( fdTabFolder );


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
        wFilenameList.add( new String[] { wFilename.getText(), wFilemask.getText(), wExcludeFilemask.getText(),
          GetXmlDataMeta.RequiredFilesCode[ 0 ], GetXmlDataMeta.RequiredFilesCode[ 0 ] } );
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
          GetXmlDataMeta tfii = new GetXmlDataMeta();
          getInfo( tfii );
          FileInputList fileInputList = tfii.getFiles( variables );
          String[] files = fileInputList.getFileStrings();
          if ( files != null && files.length > 0 ) {
            EnterSelectionDialog esd =
              new EnterSelectionDialog( shell, files, BaseMessages.getString( PKG,
                "GetXMLDataDialog.FilesReadSelection.DialogTitle" ), BaseMessages.getString( PKG,
                "GetXMLDataDialog.FilesReadSelection.DialogMessage" ) );
            esd.setViewOnly();
            esd.open();
          } else {
            MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
            mb.setMessage( BaseMessages.getString( PKG, "GetXMLDataDialog.NoFileFound.DialogMessage" ) );
            mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
            mb.open();
          }
        } catch ( HopException ex ) {
          new ErrorDialog( shell, BaseMessages.getString( PKG, "GetXMLDataDialog.ErrorParsingData.DialogTitle" ),
            BaseMessages.getString( PKG, "GetXMLDataDialog.ErrorParsingData.DialogMessage" ), ex );
        }
      }
    } );
    // Enable/disable the right fields to allow a filename to be added to each row...
    wInclFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        setIncludeFilename();
      }
    } );

    // Enable/disable the right fields to allow a row number to be added to each row...
    wInclRownum.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        setIncludeRownum();
      }
    } );

    // Whenever something changes, set the tooltip to the expanded version of the filename:
    wFilename.addModifyListener( e -> wFilename.setToolTipText( wFilename.getText() ) );


    wbbFilename.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wFilename, variables,
            new String[] { "*.xml", "*.XML", "*" },
            new String[] { BaseMessages.getString( PKG, "System.FileType.XMLFiles" ),
                    BaseMessages.getString( PKG, "System.FileType.AllFiles" ) },
            true )
    );

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
    ActiveXmlStreamField();
    setIncludeFilename();
    setIncludeRownum();
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

  private void setXMLStreamField() {
    try {

      wXMLField.removeAll();

      IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
      if ( r != null ) {
        String[] fieldNames = r.getFieldNames();
        if ( fieldNames != null ) {

          for ( String fieldName : fieldNames ) {
            wXMLField.add( fieldName );
          }
        }
      }
    } catch ( HopException ke ) {
      if ( !Const.isOSX() ) { // see PDI-8871 for details
        shell.setFocus();
      }
      String EMPTY_FIELDS = "<EMPTY>";
      wXMLField.add( EMPTY_FIELDS );
      wXMLField.setText( EMPTY_FIELDS );
      new ErrorDialog( shell, BaseMessages.getString( PKG, "GetXMLDataDialog.FailedToGetFields.DialogTitle" ),
        BaseMessages.getString( PKG, "GetXMLDataDialog.FailedToGetFields.DialogMessage" ), ke );
    }
  }

  private void ActiveXmlStreamField() {
    wlXMLField.setEnabled( wXMLStreamField.getSelection() );
    wXMLField.setEnabled( wXMLStreamField.getSelection() );
    wlXMLIsAFile.setEnabled( wXMLStreamField.getSelection() );
    wXMLIsAFile.setEnabled( wXMLStreamField.getSelection() );
    wlReadUrl.setEnabled( wXMLStreamField.getSelection() );
    wReadUrl.setEnabled( wXMLStreamField.getSelection() );

    wlFilename.setEnabled( !wXMLStreamField.getSelection() );
    wbbFilename.setEnabled( !wXMLStreamField.getSelection() );
    wbaFilename.setEnabled( !wXMLStreamField.getSelection() );
    wFilename.setEnabled( !wXMLStreamField.getSelection() );
    wlExcludeFilemask.setEnabled( !wXMLStreamField.getSelection() );
    wExcludeFilemask.setEnabled( !wXMLStreamField.getSelection() );
    wlFilemask.setEnabled( !wXMLStreamField.getSelection() );
    wFilemask.setEnabled( !wXMLStreamField.getSelection() );
    wlFilenameList.setEnabled( !wXMLStreamField.getSelection() );
    wbdFilename.setEnabled( !wXMLStreamField.getSelection() );
    wbeFilename.setEnabled( !wXMLStreamField.getSelection() );
    wbShowFiles.setEnabled( !wXMLStreamField.getSelection() );
    wlFilenameList.setEnabled( !wXMLStreamField.getSelection() );
    wFilenameList.setEnabled( !wXMLStreamField.getSelection() );
    wInclFilename.setEnabled( !wXMLStreamField.getSelection() );
    wlInclFilename.setEnabled( !wXMLStreamField.getSelection() );

    if ( wXMLStreamField.getSelection() ) {
      wInclFilename.setSelection( false );
      wlInclFilenameField.setEnabled( false );
      wInclFilenameField.setEnabled( false );
    } else {
      wlInclFilenameField.setEnabled( wInclFilename.getSelection() );
      wInclFilenameField.setEnabled( wInclFilename.getSelection() );
    }

    if ( wXMLStreamField.getSelection() && !wXMLIsAFile.getSelection() ) {
      wEncoding.setEnabled( false );
      wlEncoding.setEnabled( false );
    } else {
      wEncoding.setEnabled( true );
      wlEncoding.setEnabled( true );
    }
    wAddResult.setEnabled( !wXMLStreamField.getSelection() );
    wlAddResult.setEnabled( !wXMLStreamField.getSelection() );
    wLimit.setEnabled( !wXMLStreamField.getSelection() );
    wlLimit.setEnabled( !wXMLStreamField.getSelection() );
    wPreview.setEnabled( !wXMLStreamField.getSelection() );
    wPrunePath.setEnabled( !wXMLStreamField.getSelection() );
    wlPrunePath.setEnabled( !wXMLStreamField.getSelection() );
    wlShortFileFieldName.setEnabled( !wXMLStreamField.getSelection() );
    wShortFileFieldName.setEnabled( !wXMLStreamField.getSelection() );
    wlPathFieldName.setEnabled( !wXMLStreamField.getSelection() );
    wPathFieldName.setEnabled( !wXMLStreamField.getSelection() );
    wlIsHiddenName.setEnabled( !wXMLStreamField.getSelection() );
    wIsHiddenName.setEnabled( !wXMLStreamField.getSelection() );
    wlLastModificationTimeName.setEnabled( !wXMLStreamField.getSelection() );
    wLastModificationTimeName.setEnabled( !wXMLStreamField.getSelection() );
    wlUriName.setEnabled( !wXMLStreamField.getSelection() );
    wUriName.setEnabled( !wXMLStreamField.getSelection() );
    wlRootUriName.setEnabled( !wXMLStreamField.getSelection() );
    wRootUriName.setEnabled( !wXMLStreamField.getSelection() );
    wlExtensionFieldName.setEnabled( !wXMLStreamField.getSelection() );
    wExtensionFieldName.setEnabled( !wXMLStreamField.getSelection() );
    wlSizeFieldName.setEnabled( !wXMLStreamField.getSelection() );
    wSizeFieldName.setEnabled( !wXMLStreamField.getSelection() );
    if ( wXMLStreamField.getSelection() ) {
      wShortFileFieldName.setText( "" );
      wPathFieldName.setText( "" );
      wIsHiddenName.setText( "" );
      wLastModificationTimeName.setText( "" );
      wUriName.setText( "" );
      wRootUriName.setText( "" );
      wExtensionFieldName.setText( "" );
      wSizeFieldName.setText( "" );

    }

  }

  private void getLoopPathList() {
    try {
      GetXmlDataMeta meta = new GetXmlDataMeta();
      getInfo( meta );
      if ( meta.isInFields() ) {
        if ( meta.isReadUrl() ) {
          // Read URL
          String url = XMLSource;
          if ( url == null ) {
            EnterStringDialog d =
              new EnterStringDialog( shell, "", BaseMessages.getString( PKG, "GetXMLDataDialog.AskURL.Title" ),
                BaseMessages.getString( PKG, "GetXMLDataDialog.AskURL.Message" ) );
            url = d.open();
          }
          populateLoopPaths( meta, url, true, true );

        } else if ( meta.getIsAFile() ) {
          // Read file
          String str = XMLSource;
          if ( str == null ) {
            FileDialog dialog = new FileDialog( shell, SWT.OPEN );
            dialog.setFilterExtensions( new String[] { "*.xml;*.XML", "*" } );
            dialog.setFilterNames( new String[] { BaseMessages.getString( PKG, "System.FileType.XMLFiles" ),
              BaseMessages.getString( PKG, "System.FileType.AllFiles" ) } );

            if ( dialog.open() != null ) {
              str = dialog.getFilterPath() + System.getProperty( "file.separator" ) + dialog.getFileName();
            }
            populateLoopPaths( meta, str, false, false );
          }
        } else {
          // Read xml
          String xml = XMLSource;
          if ( xml == null ) {
            EnterTextDialog d =
              new EnterTextDialog( shell, BaseMessages.getString( PKG, "GetXMLDataDialog.AskXML.Title" ),
                BaseMessages.getString( PKG, "GetXMLDataDialog.AskXML.Message" ), null );
            xml = d.open();
          }
          populateLoopPaths( meta, xml, true, false );
        }
      } else {

        FileInputList fileinputList = meta.getFiles( variables );

        if ( fileinputList.nrOfFiles() > 0 ) {
          // Check the first file

          if ( fileinputList.getFile( 0 ).exists() ) {
            populateLoopPaths( meta, HopVfs.getFilename( fileinputList.getFile( 0 ) ), false, false );
          } else {
            // The file not exists !
            throw new HopException( BaseMessages.getString( PKG, "GetXMLDataDialog.Exception.FileDoesNotExist",
              HopVfs.getFilename( fileinputList.getFile( 0 ) ) ) );
          }
        } else {
          // No file specified
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
          mb.setMessage( BaseMessages.getString( PKG, "GetXMLDataDialog.FilesMissing.DialogMessage" ) );
          mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
          mb.open();
        }
      }
    } catch ( Exception e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "GetXMLDataDialog.UnableToGetListOfPaths.Title" ),
        BaseMessages.getString( PKG, "GetXMLDataDialog.UnableToGetListOfPaths.Message" ), e );
    }
  }

  private void get() {
    InputStream is = null;
    try {
      GetXmlDataMeta meta = new GetXmlDataMeta();
      getInfo( meta );

      // check if the path is given
      if ( !checkLoopXPath( meta ) ) {
        return;
      }
      int clearFields = SWT.YES;
      if ( wFields.nrNonEmpty() > 0 ) {
        MessageBox messageBox = new MessageBox( shell, SWT.YES | SWT.NO | SWT.CANCEL | SWT.ICON_QUESTION );
        messageBox.setMessage( BaseMessages.getString( PKG, "GetXMLDataDialog.ClearFieldList.DialogMessage" ) );
        messageBox.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.ClearFieldList.DialogTitle" ) );
        clearFields = messageBox.open();
        if ( clearFields == SWT.CANCEL ) {
          return;
        }
      }

      if ( meta.isInFields() ) {
        if ( meta.isReadUrl() ) {
          // Read URL
          String url = XMLSource;
          if ( url == null ) {
            EnterStringDialog enterStringDialog =
              new EnterStringDialog( shell, "", BaseMessages.getString( PKG, "GetXMLDataDialog.AskURL.Title" ),
                BaseMessages.getString( PKG, "GetXMLDataDialog.AskURL.Title" ) );
            url = enterStringDialog.open();
          }
          populateFields( meta, url, true, true, clearFields );

        } else if ( meta.getIsAFile() ) {
          // Read file
          String str = XMLSource;
          if ( str == null ) {
            FileDialog dialog = new FileDialog( shell, SWT.OPEN );
            dialog.setFilterExtensions( new String[] { "*.xml;*.XML", "*" } );
            dialog.setFilterNames( new String[] { BaseMessages.getString( PKG, "System.FileType.XMLFiles" ),
              BaseMessages.getString( PKG, "System.FileType.AllFiles" ) } );

            if ( dialog.open() != null ) {
              str = dialog.getFilterPath() + System.getProperty( "file.separator" ) + dialog.getFileName();
            }
          }
          populateFields( meta, str, false, false, clearFields );
        } else {
          // Read xml
          String xml = XMLSource;
          if ( xml == null ) {
            EnterTextDialog d =
              new EnterTextDialog( shell, BaseMessages.getString( PKG, "GetXMLDataDialog.AskXML.Title" ),
                BaseMessages.getString( PKG, "GetXMLDataDialog.AskXML.Message" ), null );
            xml = d.open();
          }
          populateFields( meta, xml, true, false, clearFields );
        }
      } else {

        FileInputList inputList = meta.getFiles( variables );

        if ( inputList.getFiles().size() > 0 ) {
          populateFields( meta, HopVfs.getFilename( inputList.getFile( 0 ) ), false, false, clearFields );
        }
      }
    } catch ( Exception e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "GetXMLDataDialog.ErrorParsingData.DialogTitle" ),
        BaseMessages.getString( PKG, "GetXMLDataDialog.ErrorParsingData.DialogMessage" ), e );
    } finally {
      try {
        if ( is != null ) {
          is.close();
        }
      } catch ( Exception e ) { /* Ignore */
      }
    }
  }

  private void setEncodings() {
    // Encoding of the text file:
    if ( !gotEncodings ) {
      gotEncodings = true;

      wEncoding.removeAll();
      ArrayList<Charset> values = new ArrayList<>( Charset.availableCharsets().values() );
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
  public void getData( GetXmlDataMeta in ) {
    if ( in.getFileName() != null ) {
      wFilenameList.removeAll();

      for ( int i = 0; i < in.getFileName().length; i++ ) {
        wFilenameList
          .add( new String[] { in.getFileName()[ i ], in.getFileMask()[ i ], in.getExludeFileMask()[ i ],
            in.getRequiredFilesDesc( in.getFileRequired()[ i ] ),
            in.getRequiredFilesDesc( in.getIncludeSubFolders()[ i ] ) } );
      }

      wFilenameList.removeEmptyRows();
      wFilenameList.setRowNums();
      wFilenameList.optWidth( true );
    }
    wInclFilename.setSelection( in.includeFilename() );
    wInclRownum.setSelection( in.includeRowNumber() );
    wAddResult.setSelection( in.addResultFile() );
    wNameSpaceAware.setSelection( in.isNamespaceAware() );
    wReadUrl.setSelection( in.isReadUrl() );
    wIgnoreComment.setSelection( in.isIgnoreComments() );
    wValidating.setSelection( in.isValidating() );
    wUseToken.setSelection( in.isuseToken() );
    wIgnoreEmptyFile.setSelection( in.isIgnoreEmptyFile() );
    wDoNotFailIfNoFile.setSelection( in.isdoNotFailIfNoFile() );
    wXMLStreamField.setSelection( in.isInFields() );
    wXMLIsAFile.setSelection( in.getIsAFile() );

    if ( in.getXMLField() != null ) {
      wXMLField.setText( in.getXMLField() );
    }

    if ( in.getFilenameField() != null ) {
      wInclFilenameField.setText( in.getFilenameField() );
    }
    if ( in.getRowNumberField() != null ) {
      wInclRownumField.setText( in.getRowNumberField() );
    }
    wLimit.setText( "" + in.getRowLimit() );
    if ( in.getPrunePath() != null ) {
      wPrunePath.setText( in.getPrunePath() );
    }
    if ( in.getLoopXPath() != null ) {
      wLoopXPath.setText( in.getLoopXPath() );
    }
    if ( in.getEncoding() != null ) {
      wEncoding.setText( "" + in.getEncoding() );
    } else {
      wEncoding.setText( "UTF-8" );
    }

    logDebug( BaseMessages.getString( PKG, "GetXMLDataDialog.Log.GettingFieldsInfo" ) );
    for ( int i = 0; i < in.getInputFields().length; i++ ) {
      GetXmlDataField field = in.getInputFields()[ i ];

      if ( field != null ) {
        TableItem item = wFields.table.getItem( i );
        String name = field.getName();
        String xpath = field.getXPath();
        String element = field.getElementTypeDesc();
        String resulttype = field.getResultTypeDesc();
        String type = field.getTypeDesc();
        String format = field.getFormat();
        String length = "" + field.getLength();
        String prec = "" + field.getPrecision();
        String curr = field.getCurrencySymbol();
        String group = field.getGroupSymbol();
        String decim = field.getDecimalSymbol();
        String trim = field.getTrimTypeDesc();
        String rep =
          field.isRepeated() ? BaseMessages.getString( PKG, "System.Combo.Yes" ) : BaseMessages.getString( PKG,
            "System.Combo.No" );

        if ( name != null ) {
          item.setText( 1, name );
        }
        if ( xpath != null ) {
          item.setText( 2, xpath );
        }
        if ( element != null ) {
          item.setText( 3, element );
        }
        if ( resulttype != null ) {
          item.setText( 4, resulttype );
        }
        if ( type != null ) {
          item.setText( 5, type );
        }
        if ( format != null ) {
          item.setText( 6, format );
        }
        if ( length != null && !"-1".equals( length ) ) {
          item.setText( 7, length );
        }
        if ( prec != null && !"-1".equals( prec ) ) {
          item.setText( 8, prec );
        }
        if ( curr != null ) {
          item.setText( 9, curr );
        }
        if ( decim != null ) {
          item.setText( 10, decim );
        }
        if ( group != null ) {
          item.setText( 11, group );
        }
        if ( trim != null ) {
          item.setText( 12, trim );
        }
        if ( rep != null ) {
          item.setText( 13, rep );
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

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( changed );
    dispose();
  }

  public void dispose() {
    XMLSource = null;
    super.dispose();
  }

  private void ok() {
    try {
      getInfo( input );
    } catch ( HopException e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "GetXMLDataDialog.ErrorParsingData.DialogTitle" ),
        BaseMessages.getString( PKG, "GetXMLDataDialog.ErrorParsingData.DialogMessage" ), e );
    }
    dispose();
  }

  private void getInfo( GetXmlDataMeta in ) throws HopException {
    transformName = wTransformName.getText(); // return value

    // copy info to TextFileInputMeta class (input)
    in.setRowLimit( Const.toLong( wLimit.getText(), 0L ) );
    in.setPrunePath( wPrunePath.getText() );
    in.setLoopXPath( wLoopXPath.getText() );
    in.setEncoding( wEncoding.getText() );
    in.setFilenameField( wInclFilenameField.getText() );
    in.setRowNumberField( wInclRownumField.getText() );
    in.setAddResultFile( wAddResult.getSelection() );
    in.setIncludeFilename( wInclFilename.getSelection() );
    in.setIncludeRowNumber( wInclRownum.getSelection() );
    in.setNamespaceAware( wNameSpaceAware.getSelection() );
    in.setReadUrl( wReadUrl.getSelection() );
    in.setIgnoreComments( wIgnoreComment.getSelection() );
    in.setValidating( wValidating.getSelection() );
    in.setuseToken( wUseToken.getSelection() );
    in.setIgnoreEmptyFile( wIgnoreEmptyFile.getSelection() );
    in.setdoNotFailIfNoFile( wDoNotFailIfNoFile.getSelection() );

    in.setInFields( wXMLStreamField.getSelection() );
    in.setIsAFile( wXMLIsAFile.getSelection() );
    in.setXMLField( wXMLField.getText() );

    int nrFiles = wFilenameList.getItemCount();
    int nrFields = wFields.nrNonEmpty();

    in.allocate( nrFiles, nrFields );
    in.setFileName( wFilenameList.getItems( 0 ) );
    in.setFileMask( wFilenameList.getItems( 1 ) );
    in.setExcludeFileMask( wFilenameList.getItems( 2 ) );
    in.setFileRequired( wFilenameList.getItems( 3 ) );
    in.setIncludeSubFolders( wFilenameList.getItems( 4 ) );

    for ( int i = 0; i < nrFields; i++ ) {
      GetXmlDataField field = new GetXmlDataField();

      TableItem item = wFields.getNonEmpty( i );

      field.setName( item.getText( 1 ) );
      field.setXPath( item.getText( 2 ) );
      field.setElementType( GetXmlDataField.getElementTypeByDesc( item.getText( 3 ) ) );
      field.setResultType( GetXmlDataField.getResultTypeByDesc( item.getText( 4 ) ) );
      field.setType( ValueMetaBase.getType( item.getText( 5 ) ) );
      field.setFormat( item.getText( 6 ) );
      field.setLength( Const.toInt( item.getText( 7 ), -1 ) );
      field.setPrecision( Const.toInt( item.getText( 8 ), -1 ) );
      field.setCurrencySymbol( item.getText( 9 ) );
      field.setDecimalSymbol( item.getText( 10 ) );
      field.setGroupSymbol( item.getText( 11 ) );
      field.setTrimType( GetXmlDataField.getTrimTypeByDesc( item.getText( 12 ) ) );
      field.setRepeated( BaseMessages.getString( PKG, "System.Combo.Yes" ).equalsIgnoreCase( item.getText( 13 ) ) );

      // CHECKSTYLE:Indentation:OFF
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

  // check if the loop xpath is given
  private boolean checkLoopXPath( GetXmlDataMeta meta ) {
    if ( meta.getLoopXPath() == null || meta.getLoopXPath().length() < 1 ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "GetXMLDataDialog.SpecifyRepeatingElement.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
      mb.open();
      return false;
    } else {
      return true;
    }
  }

  // Preview the data
  private void preview() {
    try {
      // Create the XML input transform
      GetXmlDataMeta oneMeta = new GetXmlDataMeta();
      getInfo( oneMeta );

      // check if the path is given
      if ( !checkLoopXPath( oneMeta ) ) {
        return;
      }
      PipelineMeta previewMeta =
        PipelinePreviewFactory.generatePreviewPipeline( variables, metadataProvider, oneMeta, wTransformName.getText() );

      EnterNumberDialog numberDialog =
        new EnterNumberDialog( shell, props.getDefaultPreviewSize(), BaseMessages.getString( PKG,
          "GetXMLDataDialog.NumberRows.DialogTitle" ), BaseMessages.getString( PKG,
          "GetXMLDataDialog.NumberRows.DialogMessage" ) );

      int previewSize = numberDialog.open();
      if ( previewSize > 0 ) {
        PipelinePreviewProgressDialog progressDialog =
          new PipelinePreviewProgressDialog( shell, variables, previewMeta, new String[] { wTransformName.getText() },
            new int[] { previewSize } );
        progressDialog.open();

        if ( !progressDialog.isCancelled() ) {
          Pipeline trans = progressDialog.getPipeline();
          String loggingText = progressDialog.getLoggingText();

          if ( trans.getResult() != null && trans.getResult().getNrErrors() > 0 ) {
            EnterTextDialog etd =
              new EnterTextDialog( shell, BaseMessages.getString( PKG, "System.Dialog.PreviewError.Title" ),
                BaseMessages.getString( PKG, "System.Dialog.PreviewError.Message" ), loggingText, true );
            etd.setReadOnly();
            etd.open();
          }
          PreviewRowsDialog prd =
            new PreviewRowsDialog( shell, variables, SWT.NONE, wTransformName.getText(), progressDialog
              .getPreviewRowsMeta( wTransformName.getText() ), progressDialog.getPreviewRows( wTransformName.getText() ),
              loggingText );
          prd.open();
        }
      }
    } catch ( HopException e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "GetXMLDataDialog.ErrorPreviewingData.DialogTitle" ),
        BaseMessages.getString( PKG, "GetXMLDataDialog.ErrorPreviewingData.DialogMessage" ), e );
    }
  }

  private void addAdditionalFieldsTab() {
    // ////////////////////////
    // START OF ADDITIONAL FIELDS TAB ///
    // ////////////////////////
    CTabItem wAdditionalFieldsTab = new CTabItem( wTabFolder, SWT.NONE );
    wAdditionalFieldsTab.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.AdditionalFieldsTab.TabTitle" ) );

    Composite wAdditionalFieldsComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wAdditionalFieldsComp );

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 3;
    fieldsLayout.marginHeight = 3;
    wAdditionalFieldsComp.setLayout( fieldsLayout );
    // ShortFileFieldName line
    wlShortFileFieldName = new Label( wAdditionalFieldsComp, SWT.RIGHT );
    wlShortFileFieldName.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.ShortFileFieldName.Label" ) );
    props.setLook( wlShortFileFieldName );
    FormData fdlShortFileFieldName = new FormData();
    fdlShortFileFieldName.left = new FormAttachment( 0, 0 );
    fdlShortFileFieldName.top = new FormAttachment( wInclRownumField, margin );
    fdlShortFileFieldName.right = new FormAttachment( middle, -margin );
    wlShortFileFieldName.setLayoutData( fdlShortFileFieldName );

    wShortFileFieldName = new TextVar( variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wShortFileFieldName );
    wShortFileFieldName.addModifyListener( lsMod );
    FormData fdShortFileFieldName = new FormData();
    fdShortFileFieldName.left = new FormAttachment( middle, 0 );
    fdShortFileFieldName.right = new FormAttachment( 100, -margin );
    fdShortFileFieldName.top = new FormAttachment( wInclRownumField, margin );
    wShortFileFieldName.setLayoutData( fdShortFileFieldName );

    // ExtensionFieldName line
    wlExtensionFieldName = new Label( wAdditionalFieldsComp, SWT.RIGHT );
    wlExtensionFieldName.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.ExtensionFieldName.Label" ) );
    props.setLook( wlExtensionFieldName );
    FormData fdlExtensionFieldName = new FormData();
    fdlExtensionFieldName.left = new FormAttachment( 0, 0 );
    fdlExtensionFieldName.top = new FormAttachment( wShortFileFieldName, margin );
    fdlExtensionFieldName.right = new FormAttachment( middle, -margin );
    wlExtensionFieldName.setLayoutData( fdlExtensionFieldName );

    wExtensionFieldName = new TextVar( variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wExtensionFieldName );
    wExtensionFieldName.addModifyListener( lsMod );
    FormData fdExtensionFieldName = new FormData();
    fdExtensionFieldName.left = new FormAttachment( middle, 0 );
    fdExtensionFieldName.right = new FormAttachment( 100, -margin );
    fdExtensionFieldName.top = new FormAttachment( wShortFileFieldName, margin );
    wExtensionFieldName.setLayoutData( fdExtensionFieldName );

    // PathFieldName line
    wlPathFieldName = new Label( wAdditionalFieldsComp, SWT.RIGHT );
    wlPathFieldName.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.PathFieldName.Label" ) );
    props.setLook( wlPathFieldName );
    FormData fdlPathFieldName = new FormData();
    fdlPathFieldName.left = new FormAttachment( 0, 0 );
    fdlPathFieldName.top = new FormAttachment( wExtensionFieldName, margin );
    fdlPathFieldName.right = new FormAttachment( middle, -margin );
    wlPathFieldName.setLayoutData( fdlPathFieldName );

    wPathFieldName = new TextVar( variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPathFieldName );
    wPathFieldName.addModifyListener( lsMod );
    FormData fdPathFieldName = new FormData();
    fdPathFieldName.left = new FormAttachment( middle, 0 );
    fdPathFieldName.right = new FormAttachment( 100, -margin );
    fdPathFieldName.top = new FormAttachment( wExtensionFieldName, margin );
    wPathFieldName.setLayoutData( fdPathFieldName );

    // SizeFieldName line
    wlSizeFieldName = new Label( wAdditionalFieldsComp, SWT.RIGHT );
    wlSizeFieldName.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.SizeFieldName.Label" ) );
    props.setLook( wlSizeFieldName );
    FormData fdlSizeFieldName = new FormData();
    fdlSizeFieldName.left = new FormAttachment( 0, 0 );
    fdlSizeFieldName.top = new FormAttachment( wPathFieldName, margin );
    fdlSizeFieldName.right = new FormAttachment( middle, -margin );
    wlSizeFieldName.setLayoutData( fdlSizeFieldName );

    wSizeFieldName = new TextVar( variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSizeFieldName );
    wSizeFieldName.addModifyListener( lsMod );
    FormData fdSizeFieldName = new FormData();
    fdSizeFieldName.left = new FormAttachment( middle, 0 );
    fdSizeFieldName.right = new FormAttachment( 100, -margin );
    fdSizeFieldName.top = new FormAttachment( wPathFieldName, margin );
    wSizeFieldName.setLayoutData( fdSizeFieldName );

    // IsHiddenName line
    wlIsHiddenName = new Label( wAdditionalFieldsComp, SWT.RIGHT );
    wlIsHiddenName.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.IsHiddenName.Label" ) );
    props.setLook( wlIsHiddenName );
    FormData fdlIsHiddenName = new FormData();
    fdlIsHiddenName.left = new FormAttachment( 0, 0 );
    fdlIsHiddenName.top = new FormAttachment( wSizeFieldName, margin );
    fdlIsHiddenName.right = new FormAttachment( middle, -margin );
    wlIsHiddenName.setLayoutData( fdlIsHiddenName );

    wIsHiddenName = new TextVar( variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wIsHiddenName );
    wIsHiddenName.addModifyListener( lsMod );
    FormData fdIsHiddenName = new FormData();
    fdIsHiddenName.left = new FormAttachment( middle, 0 );
    fdIsHiddenName.right = new FormAttachment( 100, -margin );
    fdIsHiddenName.top = new FormAttachment( wSizeFieldName, margin );
    wIsHiddenName.setLayoutData( fdIsHiddenName );

    // LastModificationTimeName line
    wlLastModificationTimeName = new Label( wAdditionalFieldsComp, SWT.RIGHT );
    wlLastModificationTimeName
      .setText( BaseMessages.getString( PKG, "GetXMLDataDialog.LastModificationTimeName.Label" ) );
    props.setLook( wlLastModificationTimeName );
    FormData fdlLastModificationTimeName = new FormData();
    fdlLastModificationTimeName.left = new FormAttachment( 0, 0 );
    fdlLastModificationTimeName.top = new FormAttachment( wIsHiddenName, margin );
    fdlLastModificationTimeName.right = new FormAttachment( middle, -margin );
    wlLastModificationTimeName.setLayoutData( fdlLastModificationTimeName );

    wLastModificationTimeName = new TextVar( variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLastModificationTimeName );
    wLastModificationTimeName.addModifyListener( lsMod );
    FormData fdLastModificationTimeName = new FormData();
    fdLastModificationTimeName.left = new FormAttachment( middle, 0 );
    fdLastModificationTimeName.right = new FormAttachment( 100, -margin );
    fdLastModificationTimeName.top = new FormAttachment( wIsHiddenName, margin );
    wLastModificationTimeName.setLayoutData( fdLastModificationTimeName );

    // UriName line
    wlUriName = new Label( wAdditionalFieldsComp, SWT.RIGHT );
    wlUriName.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.UriName.Label" ) );
    props.setLook( wlUriName );
    FormData fdlUriName = new FormData();
    fdlUriName.left = new FormAttachment( 0, 0 );
    fdlUriName.top = new FormAttachment( wLastModificationTimeName, margin );
    fdlUriName.right = new FormAttachment( middle, -margin );
    wlUriName.setLayoutData( fdlUriName );

    wUriName = new TextVar( variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUriName );
    wUriName.addModifyListener( lsMod );
    FormData fdUriName = new FormData();
    fdUriName.left = new FormAttachment( middle, 0 );
    fdUriName.right = new FormAttachment( 100, -margin );
    fdUriName.top = new FormAttachment( wLastModificationTimeName, margin );
    wUriName.setLayoutData( fdUriName );

    // RootUriName line
    wlRootUriName = new Label( wAdditionalFieldsComp, SWT.RIGHT );
    wlRootUriName.setText( BaseMessages.getString( PKG, "GetXMLDataDialog.RootUriName.Label" ) );
    props.setLook( wlRootUriName );
    FormData fdlRootUriName = new FormData();
    fdlRootUriName.left = new FormAttachment( 0, 0 );
    fdlRootUriName.top = new FormAttachment( wUriName, margin );
    fdlRootUriName.right = new FormAttachment( middle, -margin );
    wlRootUriName.setLayoutData( fdlRootUriName );

    wRootUriName = new TextVar( variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wRootUriName );
    wRootUriName.addModifyListener( lsMod );
    FormData fdRootUriName = new FormData();
    fdRootUriName.left = new FormAttachment( middle, 0 );
    fdRootUriName.right = new FormAttachment( 100, -margin );
    fdRootUriName.top = new FormAttachment( wUriName, margin );
    wRootUriName.setLayoutData( fdRootUriName );

    FormData fdAdditionalFieldsComp = new FormData();
    fdAdditionalFieldsComp.left = new FormAttachment( 0, 0 );
    fdAdditionalFieldsComp.top = new FormAttachment( wTransformName, margin );
    fdAdditionalFieldsComp.right = new FormAttachment( 100, 0 );
    fdAdditionalFieldsComp.bottom = new FormAttachment( 100, 0 );
    wAdditionalFieldsComp.setLayoutData( fdAdditionalFieldsComp );

    wAdditionalFieldsComp.layout();
    wAdditionalFieldsTab.setControl( wAdditionalFieldsComp );

    // ///////////////////////////////////////////////////////////
    // / END OF ADDITIONAL FIELDS TAB
    // ///////////////////////////////////////////////////////////

  }

  private void populateLoopPaths( GetXmlDataMeta meta, String XMLSource, boolean dynamicXMLSource, boolean useURL ) {
    if ( Utils.isEmpty( XMLSource ) ) {
      return;
    }
    String[] list_xpath = null;
    LoopNodesImportProgressDialog pd = null;
    if ( dynamicXMLSource ) {
      pd = new LoopNodesImportProgressDialog( shell, meta, XMLSource, useURL );
    } else {
      pd =
        new LoopNodesImportProgressDialog( shell, meta, XMLSource, meta.getEncoding() == null ? "UTF-8" : meta
          .getEncoding() );
    }
    if ( pd != null ) {
      list_xpath = pd.open();
      if ( list_xpath != null ) {
        EnterSelectionDialog s =
          new EnterSelectionDialog( shell, list_xpath, BaseMessages.getString( PKG,
            "GetXMLDataDialog.Dialog.SelectALoopPath.Title" ), BaseMessages.getString( PKG,
            "GetXMLDataDialog.Dialog.SelectALoopPath.Message" ) );
        String listxpaths = s.open();
        if ( listxpaths != null ) {
          wLoopXPath.setText( listxpaths );
        }
      }
    }
    this.XMLSource = XMLSource;
  }

  private void populateFields( GetXmlDataMeta meta, String XMLSource, boolean dynamicXMLSource, boolean useURL,
                               int clearFields ) throws HopException {
    if ( Utils.isEmpty( XMLSource ) ) {
      return;
    }

    XmlInputFieldsImportProgressDialog prd = null;
    RowMetaAndData[] fields = null;

    if ( dynamicXMLSource ) {
      prd =
        new XmlInputFieldsImportProgressDialog( shell, meta, XMLSource, useURL, variables.resolve( meta
          .getLoopXPath() ) );
    } else {
      prd =
        new XmlInputFieldsImportProgressDialog( shell, meta, XMLSource, meta.getEncoding() == null ? "UTF-8" : meta
          .getEncoding(), variables.resolve( meta.getLoopXPath() ) );
    }
    if ( prd != null ) {
      fields = prd.open();
      if ( fields != null ) {
        if ( clearFields == SWT.YES ) {
          wFields.clearAll( false );
        }
        int nr = fields.length;
        for ( RowMetaAndData row : fields ) {
          TableItem item = new TableItem( wFields.table, SWT.NONE );
          item.setText( 1, row.getString( 0, "" ) );
          item.setText( 2, row.getString( 1, GetXmlDataField.ElementTypeDesc[ 0 ] ) );
          item.setText( 3, row.getString( 2, "" ) );
          item.setText( 4, row.getString( 3, "" ) );
          item.setText( 5, row.getString( 4, "" ) );
        }
        wFields.removeEmptyRows();
        wFields.setRowNums();
        wFields.optWidth( true );
      }
    }
  }
}
