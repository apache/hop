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

package org.apache.hop.pipeline.transforms.propertyinput;


import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
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
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ComponentSelectionListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.ini4j.Wini;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;

public class PropertyInputDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = PropertyInputMeta.class; // For Translator
  private static final String[] YES_NO_COMBO = new String[] {
    BaseMessages.getString( PKG, "System.Combo.No" ), BaseMessages.getString( PKG, "System.Combo.Yes" ) };

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

  private Label wlExcludeFilemask;
  private TextVar wExcludeFilemask;

  private Button wbShowFiles;

  private Label wlInclFilename;
  private Button wInclFilename;

  private Label wlInclFilenameField;
  private TextVar wInclFilenameField;

  private Button wInclRownum;

  private boolean gotEncodings = false;

  private Label wlInclRownumField;
  private TextVar wInclRownumField;

  private Label wlInclINIsection;
  private Button wInclINIsection;

  private Label wlInclINIsectionField;
  private TextVar wInclINIsectionField;

  private Label wlResetRownum;
  private Button wResetRownum;

  private Button wResolveValueVariable;

  private Label wlLimit;
  private Text wLimit;

  private Label wlSection;
  private TextVar wSection;
  private Button wbbSection;

  private TableView wFields;

  private final PropertyInputMeta input;

  private Button wFileField;
  private Button wAddResult;

  private Label wlEncoding;
  private ComboVar wEncoding;

  private CCombo wFileType;

  private Label wlFilenameField;
  private CCombo wFilenameField;

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

  private boolean gotPreviousfields = false;

  public static final int[] dateLengths = new int[] { 23, 19, 14, 10, 10, 10, 10, 8, 8, 8, 8, 6, 6 };

  public PropertyInputDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (PropertyInputMeta) in;
  }

  @Override
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
    shell.setText( BaseMessages.getString( PKG, "PropertyInputDialog.DialogTitle" ) );

    middle = props.getMiddlePct();
    margin = props.getMargin();

    // Buttons at the bottom
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wPreview = new Button( shell, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( PKG, "PropertyInputDialog.Button.PreviewRows" ) );
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
    wFileTab.setText( BaseMessages.getString( PKG, "PropertyInputDialog.File.Tab" ) );

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
    wOriginFiles.setText( BaseMessages.getString( PKG, "PropertyInputDialog.wOriginFiles.Label" ) );

    FormLayout OriginFilesgroupLayout = new FormLayout();
    OriginFilesgroupLayout.marginWidth = 10;
    OriginFilesgroupLayout.marginHeight = 10;
    wOriginFiles.setLayout( OriginFilesgroupLayout );

    // Is Filename defined in a Field
    Label wlFileField = new Label(wOriginFiles, SWT.RIGHT);
    wlFileField.setText( BaseMessages.getString( PKG, "PropertyInputDialog.FileField.Label" ) );
    props.setLook(wlFileField);
    FormData fdlFileField = new FormData();
    fdlFileField.left = new FormAttachment( 0, 0 );
    fdlFileField.top = new FormAttachment( 0, margin );
    fdlFileField.right = new FormAttachment( middle, -margin );
    wlFileField.setLayoutData(fdlFileField);
    wFileField = new Button(wOriginFiles, SWT.CHECK );
    props.setLook( wFileField );
    wFileField.setToolTipText( BaseMessages.getString( PKG, "PropertyInputDialog.FileField.Tooltip" ) );
    FormData fdFileField = new FormData();
    fdFileField.left = new FormAttachment( middle, margin );
    fdFileField.top = new FormAttachment( wlFileField, 0, SWT.CENTER );
    wFileField.setLayoutData(fdFileField);
    SelectionAdapter lfilefield = new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        ActiveFileField();
        input.setChanged();
      }
    };
    wFileField.addSelectionListener( lfilefield );

    // Filename field
    wlFilenameField = new Label(wOriginFiles, SWT.RIGHT );
    wlFilenameField.setText( BaseMessages.getString( PKG, "PropertyInputDialog.wlFilenameField.Label" ) );
    props.setLook( wlFilenameField );
    FormData fdlFilenameField = new FormData();
    fdlFilenameField.left = new FormAttachment( 0, 0 );
    fdlFilenameField.top = new FormAttachment( wFileField, margin );
    fdlFilenameField.right = new FormAttachment( middle, -margin );
    wlFilenameField.setLayoutData(fdlFilenameField);

    wFilenameField = new CCombo(wOriginFiles, SWT.BORDER | SWT.READ_ONLY );
    wFilenameField.setEditable( true );
    props.setLook( wFilenameField );
    wFilenameField.addModifyListener( lsMod );
    FormData fdFilenameField = new FormData();
    fdFilenameField.left = new FormAttachment( middle, margin );
    fdFilenameField.top = new FormAttachment( wFileField, margin );
    fdFilenameField.right = new FormAttachment( 100, -margin );
    wFilenameField.setLayoutData(fdFilenameField);
    wFilenameField.addFocusListener( new FocusListener() {
      @Override
      public void focusLost( FocusEvent e ) {
      }

      @Override
      public void focusGained( FocusEvent e ) {
        setFileField();
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

    middle = middle / 2;

    // Filename line
    wlFilename = new Label(wFileComp, SWT.RIGHT );
    wlFilename.setText( BaseMessages.getString( PKG, "PropertyInputDialog.Filename.Label" ) );
    props.setLook( wlFilename );
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment(wOriginFiles, margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData(fdlFilename);

    wbbFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbbFilename );
    wbbFilename.setText( BaseMessages.getString( PKG, "PropertyInputDialog.FilenameBrowse.Button" ) );
    wbbFilename.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.BrowseForFileOrDirAndAdd" ) );
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment(wOriginFiles, margin );
    wbbFilename.setLayoutData(fdbFilename);

    wbaFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbaFilename );
    wbaFilename.setText( BaseMessages.getString( PKG, "PropertyInputDialog.FilenameAdd.Button" ) );
    wbaFilename.setToolTipText( BaseMessages.getString( PKG, "PropertyInputDialog.FilenameAdd.Tooltip" ) );
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
    wlFilemask.setText( BaseMessages.getString( PKG, "PropertyInputDialog.RegExp.Label" ) );
    props.setLook( wlFilemask );
    FormData fdlFilemask = new FormData();
    fdlFilemask.left = new FormAttachment( 0, 0 );
    fdlFilemask.top = new FormAttachment( wFilename, 2 * margin );
    fdlFilemask.right = new FormAttachment( middle, -margin );
    wlFilemask.setLayoutData(fdlFilemask);
    wFilemask = new TextVar( variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilemask );
    wFilemask.addModifyListener( lsMod );
    FormData fdFilemask = new FormData();
    fdFilemask.left = new FormAttachment( middle, 0 );
    fdFilemask.top = new FormAttachment( wFilename, 2 * margin );
    fdFilemask.right = new FormAttachment( 100, 0 );
    wFilemask.setLayoutData(fdFilemask);

    wlExcludeFilemask = new Label(wFileComp, SWT.RIGHT );
    wlExcludeFilemask.setText( BaseMessages.getString( PKG, "PropertyInputDialog.ExcludeFilemask.Label" ) );
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
    wlFilenameList.setText( BaseMessages.getString( PKG, "PropertyInputDialog.FilenameList.Label" ) );
    props.setLook( wlFilenameList );
    FormData fdlFilenameList = new FormData();
    fdlFilenameList.left = new FormAttachment( 0, 0 );
    fdlFilenameList.top = new FormAttachment( wExcludeFilemask, margin );
    fdlFilenameList.right = new FormAttachment( middle, -margin );
    wlFilenameList.setLayoutData(fdlFilenameList);

    // Buttons to the right of the screen...
    wbdFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbdFilename );
    wbdFilename.setText( BaseMessages.getString( PKG, "PropertyInputDialog.FilenameRemove.Button" ) );
    wbdFilename.setToolTipText( BaseMessages.getString( PKG, "PropertyInputDialog.FilenameRemove.Tooltip" ) );
    FormData fdbdFilename = new FormData();
    fdbdFilename.right = new FormAttachment( 100, 0 );
    fdbdFilename.top = new FormAttachment( wExcludeFilemask, 40 );
    wbdFilename.setLayoutData(fdbdFilename);

    wbeFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbeFilename );
    wbeFilename.setText( BaseMessages.getString( PKG, "PropertyInputDialog.FilenameEdit.Button" ) );
    wbeFilename.setToolTipText( BaseMessages.getString( PKG, "PropertyInputDialog.FilenameEdit.Tooltip" ) );
    FormData fdbeFilename = new FormData();
    fdbeFilename.right = new FormAttachment( 100, 0 );
    fdbeFilename.top = new FormAttachment( wbdFilename, margin );
    wbeFilename.setLayoutData(fdbeFilename);

    wbShowFiles = new Button(wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbShowFiles );
    wbShowFiles.setText( BaseMessages.getString( PKG, "PropertyInputDialog.ShowFiles.Button" ) );
    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment( middle, 0 );
    fdbShowFiles.bottom = new FormAttachment( 100, 0 );
    wbShowFiles.setLayoutData(fdbShowFiles);

    ColumnInfo[] colinfo = new ColumnInfo[ 5 ];
    colinfo[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "PropertyInputDialog.Files.Filename.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinfo[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "PropertyInputDialog.Files.Wildcard.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinfo[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "PropertyInputDialog.Files.ExcludeWildcard.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinfo[ 3 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "PropertyInputDialog.Required.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        YES_NO_COMBO );
    colinfo[ 4 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "PropertyInputDialog.IncludeSubDirs.Column" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, YES_NO_COMBO );

    colinfo[ 0 ].setUsingVariables( true );
    colinfo[ 1 ].setUsingVariables( true );
    colinfo[ 1 ].setToolTip( BaseMessages.getString( PKG, "PropertyInputDialog.Files.Wildcard.Tooltip" ) );
    colinfo[ 2 ].setUsingVariables( true );
    colinfo[ 2 ].setToolTip( BaseMessages.getString( PKG, "PropertyInputDialog.Files.ExcludeWildcard.Tooltip" ) );
    colinfo[ 3 ].setToolTip( BaseMessages.getString( PKG, "PropertyInputDialog.Required.Tooltip" ) );
    colinfo[ 4 ].setToolTip( BaseMessages.getString( PKG, "PropertyInputDialog.IncludeSubDirs.Tooltip" ) );

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
    middle = props.getMiddlePct();
    // ////////////////////////
    // START OF CONTENT TAB///
    // /
    CTabItem wContentTab = new CTabItem(wTabFolder, SWT.NONE);
    wContentTab.setText( BaseMessages.getString( PKG, "PropertyInputDialog.Content.Tab" ) );

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    Composite wContentComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wContentComp);
    wContentComp.setLayout( contentLayout );

    // ///////////////////////////////
    // START OF SettingsGroup GROUP //
    // ///////////////////////////////

    Group wSettingsGroup = new Group(wContentComp, SWT.SHADOW_NONE);
    props.setLook(wSettingsGroup);
    wSettingsGroup.setText( BaseMessages.getString( PKG, "PropertyInputDialog.SettingsGroup.Label" ) );

    FormLayout settingsGroupLayout = new FormLayout();
    settingsGroupLayout.marginWidth = 10;
    settingsGroupLayout.marginHeight = 10;
    wSettingsGroup.setLayout( settingsGroupLayout );

    Label wlFileType = new Label(wSettingsGroup, SWT.RIGHT);
    wlFileType.setText( BaseMessages.getString( PKG, "PropertyInputDialog.FileType.Label" ) );
    props.setLook(wlFileType);
    FormData fdlFileType = new FormData();
    fdlFileType.left = new FormAttachment( 0, 0 );
    fdlFileType.top = new FormAttachment( 0, margin );
    fdlFileType.right = new FormAttachment( middle, -margin );
    wlFileType.setLayoutData(fdlFileType);
    wFileType = new CCombo(wSettingsGroup, SWT.BORDER | SWT.READ_ONLY );
    wFileType.setEditable( true );
    wFileType.setItems( PropertyInputMeta.fileTypeDesc );
    props.setLook( wFileType );
    wFileType.addModifyListener( lsMod );
    FormData fdFileType = new FormData();
    fdFileType.left = new FormAttachment( middle, 0 );
    fdFileType.top = new FormAttachment( 0, margin );
    fdFileType.right = new FormAttachment( 100, 0 );
    wFileType.setLayoutData(fdFileType);
    wFileType.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        setFileType();
      }
    } );

    wlEncoding = new Label(wSettingsGroup, SWT.RIGHT );
    wlEncoding.setText( BaseMessages.getString( PKG, "PropertyInputDialog.Encoding.Label" ) );
    props.setLook( wlEncoding );
    FormData fdlEncoding = new FormData();
    fdlEncoding.left = new FormAttachment( 0, 0 );
    fdlEncoding.top = new FormAttachment( wFileType, margin );
    fdlEncoding.right = new FormAttachment( middle, -margin );
    wlEncoding.setLayoutData(fdlEncoding);
    wEncoding = new ComboVar( variables, wSettingsGroup, SWT.BORDER | SWT.READ_ONLY );
    wEncoding.setEditable( true );
    props.setLook( wEncoding );
    wEncoding.addModifyListener( lsMod );
    FormData fdEncoding = new FormData();
    fdEncoding.left = new FormAttachment( middle, 0 );
    fdEncoding.top = new FormAttachment( wFileType, margin );
    fdEncoding.right = new FormAttachment( 100, 0 );
    wEncoding.setLayoutData(fdEncoding);
    wEncoding.addFocusListener( new FocusListener() {
      @Override
      public void focusLost( FocusEvent e ) {
      }

      @Override
      public void focusGained( FocusEvent e ) {
        setEncodings();
      }
    } );

    wlSection = new Label(wSettingsGroup, SWT.RIGHT );
    wlSection.setText( BaseMessages.getString( PKG, "PropertyInputDialog.Section.Label" ) );
    props.setLook( wlSection );
    FormData fdlSection = new FormData();
    fdlSection.left = new FormAttachment( 0, 0 );
    fdlSection.top = new FormAttachment( wEncoding, margin );
    fdlSection.right = new FormAttachment( middle, -margin );
    wlSection.setLayoutData(fdlSection);

    wbbSection = new Button(wSettingsGroup, SWT.PUSH | SWT.CENTER );
    props.setLook( wbbSection );
    wbbSection.setText( BaseMessages.getString( PKG, "PropertyInputDialog.SectionBrowse.Button" ) );
    FormData fdbSection = new FormData();
    fdbSection.right = new FormAttachment( 100, 0 );
    fdbSection.top = new FormAttachment( wEncoding, margin );
    wbbSection.setLayoutData(fdbSection);
    wbbSection.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        getSections();
      }
    } );

    wSection = new TextVar( variables, wSettingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wSection.setToolTipText( BaseMessages.getString( PKG, "PropertyInputDialog.Section.Tooltip" ) );
    props.setLook( wSection );
    wSection.addModifyListener( lsMod );
    FormData fdSection = new FormData();
    fdSection.left = new FormAttachment( middle, 0 );
    fdSection.top = new FormAttachment( wEncoding, margin );
    fdSection.right = new FormAttachment( wbbSection, -margin );
    wSection.setLayoutData(fdSection);

    wlLimit = new Label(wSettingsGroup, SWT.RIGHT );
    wlLimit.setText( BaseMessages.getString( PKG, "PropertyInputDialog.Limit.Label" ) );
    props.setLook( wlLimit );
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment( 0, 0 );
    fdlLimit.top = new FormAttachment( wbbSection, margin );
    fdlLimit.right = new FormAttachment( middle, -margin );
    wlLimit.setLayoutData(fdlLimit);
    wLimit = new Text(wSettingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLimit );
    wLimit.addModifyListener( lsMod );
    FormData fdLimit = new FormData();
    fdLimit.left = new FormAttachment( middle, 0 );
    fdLimit.top = new FormAttachment( wbbSection, margin );
    fdLimit.right = new FormAttachment( 100, 0 );
    wLimit.setLayoutData(fdLimit);

    Label wlResolveValueVariable = new Label(wSettingsGroup, SWT.RIGHT);
    wlResolveValueVariable.setText( BaseMessages.getString( PKG, "PropertyInputDialog.resolveValueVariable.Label" ) );
    props.setLook(wlResolveValueVariable);
    FormData fdlResolveValueVariable = new FormData();
    fdlResolveValueVariable.left = new FormAttachment( 0, 0 );
    fdlResolveValueVariable.top = new FormAttachment( wLimit, margin );
    fdlResolveValueVariable.right = new FormAttachment( middle, -margin );
    wlResolveValueVariable.setLayoutData(fdlResolveValueVariable);
    wResolveValueVariable = new Button(wSettingsGroup, SWT.CHECK );
    props.setLook( wResolveValueVariable );
    wResolveValueVariable.setToolTipText( BaseMessages.getString( PKG, "PropertyInputDialog.resolveValueVariable.Tooltip" ) );
    FormData fdResolveValueVariable = new FormData();
    fdResolveValueVariable.left = new FormAttachment( middle, 0 );
    fdResolveValueVariable.top = new FormAttachment( wlResolveValueVariable, 0, SWT.CENTER );
    wResolveValueVariable.setLayoutData(fdResolveValueVariable);
    wResolveValueVariable.addSelectionListener( new ComponentSelectionListener( input ) );

    FormData fdSettingsGroup = new FormData();
    fdSettingsGroup.left = new FormAttachment( 0, margin );
    fdSettingsGroup.top = new FormAttachment( wResolveValueVariable, margin );
    fdSettingsGroup.right = new FormAttachment( 100, -margin );
    wSettingsGroup.setLayoutData(fdSettingsGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF SettingsGroup GROUP
    // ///////////////////////////////////////////////////////////

    // /////////////////////////////////
    // START OF Additional Fields GROUP
    // /////////////////////////////////

    Group wAdditionalGroup = new Group(wContentComp, SWT.SHADOW_NONE);
    props.setLook(wAdditionalGroup);
    wAdditionalGroup.setText( BaseMessages.getString( PKG, "PropertyInputDialog.Group.AdditionalGroup.Label" ) );

    FormLayout additionalgroupLayout = new FormLayout();
    additionalgroupLayout.marginWidth = 10;
    additionalgroupLayout.marginHeight = 10;
    wAdditionalGroup.setLayout( additionalgroupLayout );

    wlInclFilename = new Label(wAdditionalGroup, SWT.RIGHT );
    wlInclFilename.setText( BaseMessages.getString( PKG, "PropertyInputDialog.InclFilename.Label" ) );
    props.setLook( wlInclFilename );
    FormData fdlInclFilename = new FormData();
    fdlInclFilename.left = new FormAttachment( 0, 0 );
    fdlInclFilename.top = new FormAttachment(wSettingsGroup, margin );
    fdlInclFilename.right = new FormAttachment( middle, -margin );
    wlInclFilename.setLayoutData(fdlInclFilename);
    wInclFilename = new Button(wAdditionalGroup, SWT.CHECK );
    props.setLook( wInclFilename );
    wInclFilename.setToolTipText( BaseMessages.getString( PKG, "PropertyInputDialog.InclFilename.Tooltip" ) );
    FormData fdInclFilename = new FormData();
    fdInclFilename.left = new FormAttachment( middle, 0 );
    fdInclFilename.top = new FormAttachment( wlInclFilename, 0, SWT.CENTER );
    wInclFilename.setLayoutData(fdInclFilename);
    wInclFilename.addSelectionListener( new ComponentSelectionListener( input ) );

    wlInclFilenameField = new Label(wAdditionalGroup, SWT.LEFT );
    wlInclFilenameField.setText( BaseMessages.getString( PKG, "PropertyInputDialog.InclFilenameField.Label" ) );
    props.setLook( wlInclFilenameField );
    FormData fdlInclFilenameField = new FormData();
    fdlInclFilenameField.left = new FormAttachment( wInclFilename, margin );
    fdlInclFilenameField.top = new FormAttachment(wSettingsGroup, margin );
    wlInclFilenameField.setLayoutData(fdlInclFilenameField);
    wInclFilenameField = new TextVar( variables, wAdditionalGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclFilenameField );
    wInclFilenameField.addModifyListener( lsMod );
    FormData fdInclFilenameField = new FormData();
    fdInclFilenameField.left = new FormAttachment( wlInclFilenameField, margin );
    fdInclFilenameField.top = new FormAttachment(wSettingsGroup, margin );
    fdInclFilenameField.right = new FormAttachment( 100, 0 );
    wInclFilenameField.setLayoutData(fdInclFilenameField);

    Label wlInclRownum = new Label(wAdditionalGroup, SWT.RIGHT);
    wlInclRownum.setText( BaseMessages.getString( PKG, "PropertyInputDialog.InclRownum.Label" ) );
    props.setLook(wlInclRownum);
    FormData fdlInclRownum = new FormData();
    fdlInclRownum.left = new FormAttachment( 0, 0 );
    fdlInclRownum.top = new FormAttachment( wInclFilenameField, margin );
    fdlInclRownum.right = new FormAttachment( middle, -margin );
    wlInclRownum.setLayoutData(fdlInclRownum);
    wInclRownum = new Button(wAdditionalGroup, SWT.CHECK );
    props.setLook( wInclRownum );
    wInclRownum.setToolTipText( BaseMessages.getString( PKG, "PropertyInputDialog.InclRownum.Tooltip" ) );
    FormData fdRownum = new FormData();
    fdRownum.left = new FormAttachment( middle, 0 );
    fdRownum.top = new FormAttachment( wlInclRownum, 0, SWT.CENTER );
    wInclRownum.setLayoutData(fdRownum);
    wInclRownum.addSelectionListener( new ComponentSelectionListener( input ) );

    wlInclRownumField = new Label(wAdditionalGroup, SWT.RIGHT );
    wlInclRownumField.setText( BaseMessages.getString( PKG, "PropertyInputDialog.InclRownumField.Label" ) );
    props.setLook( wlInclRownumField );
    FormData fdlInclRownumField = new FormData();
    fdlInclRownumField.left = new FormAttachment( wInclRownum, margin );
    fdlInclRownumField.top = new FormAttachment( wInclFilenameField, margin );
    wlInclRownumField.setLayoutData(fdlInclRownumField);
    wInclRownumField = new TextVar( variables, wAdditionalGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclRownumField );
    wInclRownumField.addModifyListener( lsMod );
    FormData fdInclRownumField = new FormData();
    fdInclRownumField.left = new FormAttachment( wlInclRownumField, margin );
    fdInclRownumField.top = new FormAttachment( wInclFilenameField, margin );
    fdInclRownumField.right = new FormAttachment( 100, 0 );
    wInclRownumField.setLayoutData(fdInclRownumField);

    wlResetRownum = new Label(wAdditionalGroup, SWT.RIGHT );
    wlResetRownum.setText( BaseMessages.getString( PKG, "PropertyInputDialog.ResetRownum.Label" ) );
    props.setLook( wlResetRownum );
    FormData fdlResetRownum = new FormData();
    fdlResetRownum.left = new FormAttachment( wInclRownum, margin );
    fdlResetRownum.top = new FormAttachment( wInclRownumField, margin );
    wlResetRownum.setLayoutData(fdlResetRownum);
    wResetRownum = new Button(wAdditionalGroup, SWT.CHECK );
    props.setLook( wResetRownum );
    wResetRownum.setToolTipText( BaseMessages.getString( PKG, "PropertyInputDialog.ResetRownum.Tooltip" ) );
    fdRownum = new FormData();
    fdRownum.left = new FormAttachment( wlResetRownum, margin );
    fdRownum.top = new FormAttachment( wlResetRownum, 0, SWT.CENTER );
    wResetRownum.setLayoutData(fdRownum);
    wResetRownum.addSelectionListener( new ComponentSelectionListener( input ) );

    wlInclINIsection = new Label(wAdditionalGroup, SWT.RIGHT );
    wlInclINIsection.setText( BaseMessages.getString( PKG, "PropertyInputDialog.InclINIsection.Label" ) );
    props.setLook( wlInclINIsection );
    FormData fdlInclINIsection = new FormData();
    fdlInclINIsection.left = new FormAttachment( 0, 0 );
    fdlInclINIsection.top = new FormAttachment( wResetRownum, margin );
    fdlInclINIsection.right = new FormAttachment( middle, -margin );
    wlInclINIsection.setLayoutData(fdlInclINIsection);
    wInclINIsection = new Button(wAdditionalGroup, SWT.CHECK );
    props.setLook( wInclINIsection );
    wInclINIsection.setToolTipText( BaseMessages.getString( PKG, "PropertyInputDialog.InclINIsection.Tooltip" ) );
    fdRownum = new FormData();
    fdRownum.left = new FormAttachment( middle, 0 );
    fdRownum.top = new FormAttachment( wlInclINIsection, 0, SWT.CENTER );
    wInclINIsection.setLayoutData(fdRownum);
    wInclINIsection.addSelectionListener( new ComponentSelectionListener( input ) );

    wlInclINIsectionField = new Label(wAdditionalGroup, SWT.RIGHT );
    wlInclINIsectionField.setText( BaseMessages.getString( PKG, "PropertyInputDialog.InclINIsectionField.Label" ) );
    props.setLook( wlInclINIsectionField );
    FormData fdlInclINIsectionField = new FormData();
    fdlInclINIsectionField.left = new FormAttachment( wInclINIsection, margin );
    fdlInclINIsectionField.top = new FormAttachment( wResetRownum, margin );
    wlInclINIsectionField.setLayoutData(fdlInclINIsectionField);
    wInclINIsectionField = new TextVar( variables, wAdditionalGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclINIsectionField );
    wInclINIsectionField.addModifyListener( lsMod );
    FormData fdInclINIsectionField = new FormData();
    fdInclINIsectionField.left = new FormAttachment( wlInclINIsectionField, margin );
    fdInclINIsectionField.top = new FormAttachment( wResetRownum, margin );
    fdInclINIsectionField.right = new FormAttachment( 100, 0 );
    wInclINIsectionField.setLayoutData(fdInclINIsectionField);

    FormData fdAdditionalGroup = new FormData();
    fdAdditionalGroup.left = new FormAttachment( 0, margin );
    fdAdditionalGroup.top = new FormAttachment(wSettingsGroup, margin );
    fdAdditionalGroup.right = new FormAttachment( 100, -margin );
    wAdditionalGroup.setLayoutData(fdAdditionalGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF DESTINATION ADDRESS GROUP
    // ///////////////////////////////////////////////////////////

    // ///////////////////////////////
    // START OF AddFileResult GROUP //
    // ///////////////////////////////

    Group wAddFileResult = new Group(wContentComp, SWT.SHADOW_NONE);
    props.setLook(wAddFileResult);
    wAddFileResult.setText( BaseMessages.getString( PKG, "PropertyInputDialog.wAddFileResult.Label" ) );

    FormLayout AddFileResultgroupLayout = new FormLayout();
    AddFileResultgroupLayout.marginWidth = 10;
    AddFileResultgroupLayout.marginHeight = 10;
    wAddFileResult.setLayout( AddFileResultgroupLayout );

    Label wlAddResult = new Label(wAddFileResult, SWT.RIGHT);
    wlAddResult.setText( BaseMessages.getString( PKG, "PropertyInputDialog.AddResult.Label" ) );
    props.setLook(wlAddResult);
    FormData fdlAddResult = new FormData();
    fdlAddResult.left = new FormAttachment( 0, 0 );
    fdlAddResult.top = new FormAttachment(wAdditionalGroup, margin );
    fdlAddResult.right = new FormAttachment( middle, -margin );
    wlAddResult.setLayoutData(fdlAddResult);
    wAddResult = new Button(wAddFileResult, SWT.CHECK );
    props.setLook( wAddResult );
    wAddResult.setToolTipText( BaseMessages.getString( PKG, "PropertyInputDialog.AddResult.Tooltip" ) );
    FormData fdAddResult = new FormData();
    fdAddResult.left = new FormAttachment( middle, 0 );
    fdAddResult.top = new FormAttachment(wlAddResult, 0, SWT.CENTER );
    wAddResult.setLayoutData(fdAddResult);
    wAddResult.addSelectionListener( new ComponentSelectionListener( input ) );

    FormData fdAddFileResult = new FormData();
    fdAddFileResult.left = new FormAttachment( 0, margin );
    fdAddFileResult.top = new FormAttachment(wAdditionalGroup, margin );
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
    wFieldsTab.setText( BaseMessages.getString( PKG, "PropertyInputDialog.Fields.Tab" ) );

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    wFieldsComp.setLayout( fieldsLayout );
    props.setLook(wFieldsComp);

    wGet = new Button(wFieldsComp, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "PropertyInputDialog.GetFields.Button" ) );
    fdGet = new FormData();
    fdGet.left = new FormAttachment( 50, 0 );
    fdGet.bottom = new FormAttachment( 100, 0 );
    wGet.setLayoutData( fdGet );

    final int FieldsRows = input.getInputFields().length;

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "PropertyInputDialog.FieldsTable.Name.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "PropertyInputDialog.FieldsTable.Attribut.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, PropertyInputField.ColumnDesc, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "PropertyInputDialog.FieldsTable.Type.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getValueMetaNames(), true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "PropertyInputDialog.FieldsTable.Format.Column" ),
          ColumnInfo.COLUMN_TYPE_FORMAT, 3 ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "PropertyInputDialog.FieldsTable.Length.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "PropertyInputDialog.FieldsTable.Precision.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "PropertyInputDialog.FieldsTable.Currency.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "PropertyInputDialog.FieldsTable.Decimal.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "PropertyInputDialog.FieldsTable.Group.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "PropertyInputDialog.FieldsTable.TrimType.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, PropertyInputField.trimTypeDesc, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "PropertyInputDialog.FieldsTable.Repeat.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {
          BaseMessages.getString( PKG, "System.Combo.Yes" ),
          BaseMessages.getString( PKG, "System.Combo.No" ) }, true ),

      };

    colinf[ 0 ].setUsingVariables( true );
    colinf[ 0 ].setToolTip( BaseMessages.getString( PKG, "PropertyInputDialog.FieldsTable.Name.Column.Tooltip" ) );
    colinf[ 1 ]
      .setToolTip( BaseMessages.getString( PKG, "PropertyInputDialog.FieldsTable.Attribut.Column.Tooltip" ) );
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
      @Override
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
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        wFilenameList.add( new String[] {
          wFilename.getText(), wFilemask.getText(), wExcludeFilemask.getText(),
          PropertyInputMeta.RequiredFilesCode[ 0 ], PropertyInputMeta.RequiredFilesCode[ 0 ] } );
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
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        int[] idx = wFilenameList.getSelectionIndices();
        wFilenameList.remove( idx );
        wFilenameList.removeEmptyRows();
        wFilenameList.setRowNums();
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
        try {
          PropertyInputMeta tfii = new PropertyInputMeta();
          getInfo( tfii );
          FileInputList fileInputList = tfii.getFiles( variables );
          String[] files = fileInputList.getFileStrings();

          if ( files.length > 0 ) {
            EnterSelectionDialog esd = new EnterSelectionDialog( shell, files,
              BaseMessages.getString( PKG, "PropertyInputDialog.FilesReadSelection.DialogTitle" ),
              BaseMessages.getString( PKG, "PropertyInputDialog.FilesReadSelection.DialogMessage" ) );
            esd.setViewOnly();
            esd.open();
          } else {
            MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
            mb.setMessage( BaseMessages.getString( PKG, "PropertyInputDialog.NoFileFound.DialogMessage" ) );
            mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
            mb.open();
          }
        } catch ( Exception ex ) {
          new ErrorDialog(
            shell, BaseMessages.getString( PKG, "PropertyInputDialog.ErrorParsingData.DialogTitle" ),
            BaseMessages.getString( PKG, "PropertyInputDialog.ErrorParsingData.DialogMessage" ), ex );
        }
      }
    } );

    // Enable/disable the right fields to allow a filename to be added to each row...
    wInclFilename.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        setIncludeFilename();
      }
    } );
    // Enable/disable the right fields to allow a filename to be added to each row...
    wInclINIsection.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        setIncludeSection();
      }
    } );

    // Enable/disable the right fields to allow a row number to be added to each row...
    wInclRownum.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        setIncludeRownum();
      }
    } );

    // Whenever something changes, set the tooltip to the expanded version of the filename:
    wFilename.addModifyListener( e -> wFilename.setToolTipText( "" ) );

    // Listen to the Browse... button
    wbbFilename.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        if ( !Utils.isEmpty( wFilemask.getText() ) || !Utils.isEmpty( wExcludeFilemask.getText() ) ) { // A mask: a directory!
          DirectoryDialog dialog = new DirectoryDialog( shell, SWT.OPEN );
          if ( wFilename.getText() != null ) {
            String fpath = "";
            dialog.setFilterPath( fpath );
          }

          if ( dialog.open() != null ) {
            String str = dialog.getFilterPath();
            wFilename.setText( str );
          }
        } else {
          FileDialog dialog = new FileDialog( shell, SWT.OPEN );

          if ( PropertyInputMeta.getFileTypeByDesc( wFileType.getText() ) == PropertyInputMeta.FILE_TYPE_PROPERTY ) {
            dialog.setFilterExtensions( new String[] { "*.properties;*.PROPERTIES", "*" } );
            dialog.setFilterNames( new String[] {
              BaseMessages.getString( PKG, "PropertyInputDialog.FileType.PropertiesFiles" ),
              BaseMessages.getString( PKG, "System.FileType.AllFiles" ) } );
          } else {
            dialog.setFilterExtensions( new String[] { "*.ini;*.INI", "*" } );
            dialog.setFilterNames( new String[] {
              BaseMessages.getString( PKG, "PropertyInputDialog.FileType.INIFiles" ),
              BaseMessages.getString( PKG, "System.FileType.AllFiles" ) } );
          }

          if ( wFilename.getText() != null ) {
            String fname = "";
            dialog.setFileName( fname );
          }

          if ( dialog.open() != null ) {
            String str = dialog.getFilterPath() + System.getProperty( "file.separator" ) + dialog.getFileName();
            wFilename.setText( str );
          }
        }
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
    setFileType();
    input.setChanged( changed );
    ActiveFileField();
    wFields.optWidth( true );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void setFileType() {
    boolean active =
      ( PropertyInputMeta.getFileTypeByDesc( wFileType.getText() ) == PropertyInputMeta.FILE_TYPE_INI );
    wlSection.setEnabled( active );
    wSection.setEnabled( active );
    wbbSection.setEnabled( active );
    wlEncoding.setEnabled( active );
    wEncoding.setEnabled( active );
    if ( !active && wInclINIsection.getSelection() ) {
      wInclINIsection.setSelection( false );
    }
    wlInclINIsection.setEnabled( active );
    wInclINIsection.setEnabled( active );
    setIncludeSection();
  }

  private void setEncodings() {
    // Encoding of the text file:
    if ( !gotEncodings ) {
      gotEncodings = true;

      wEncoding.removeAll();
      ArrayList<Charset> values = new ArrayList<>(Charset.availableCharsets().values());
      for (Charset charSet : values) {
        wEncoding.add(charSet.displayName());
      }

      // Now select the default!
      String defEncoding = Const.getEnvironmentVariable( "file.encoding", PropertyInputMeta.DEFAULT_ENCODING );
      int idx = Const.indexOfString( defEncoding, wEncoding.getItems() );
      if ( idx >= 0 ) {
        wEncoding.select( idx );
      }
    }
  }

  private void setFileField() {
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
      if ( value != null ) {
        wFilenameField.setText( value );
      }

    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "PropertyInputDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "PropertyInputDialog.FailedToGetFields.DialogMessage" ), ke );
    }
  }

  private void get() {
    if ( !gotPreviousfields ) {
      gotPreviousfields = true;
      IRowMeta fields = new RowMeta();

      try {
        PropertyInputMeta meta = new PropertyInputMeta();
        getInfo( meta );

        FileInputList inputList = meta.getFiles( variables );

        if ( inputList.getFiles().size() > 0 ) {

          IValueMeta field =
            new ValueMetaString( PropertyInputField.getColumnDesc( 0 ) );
          fields.addValueMeta( field );
          field = new ValueMetaString( PropertyInputField.getColumnDesc( 1 ) );
          fields.addValueMeta( field );

        }

      } catch ( Exception e ) {
        new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.Error.Title" ), BaseMessages
          .getString( PKG, "PropertyInputDialog.ErrorReadingFile.DialogMessage", e.toString() ), e );
      }

      if ( fields.size() > 0 ) {

        // Clear Fields Grid
        wFields.removeAll();

        for ( int j = 0; j < fields.size(); j++ ) {
          IValueMeta field = fields.getValueMeta( j );
          wFields.add( new String[] {
            field.getName(), field.getName(), field.getTypeDesc(), "", "-1", "", "", "", "", "none", "N" } );
        }
        wFields.removeEmptyRows();
        wFields.setRowNums();
        wFields.optWidth( true );
      } else {
        MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_WARNING );
        mb.setMessage( BaseMessages.getString( PKG, "PropertyInputDialog.UnableToFindFields.DialogTitle" ) );
        mb.setText( BaseMessages.getString( PKG, "PropertyInputDialog.UnableToFindFields.DialogMessage" ) );
        mb.open();
      }
    }
  }

  private void ActiveFileField() {
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
    if ( wFileField.getSelection() ) {
      wInclFilename.setSelection( false );
    }
    wInclFilename.setEnabled( !wFileField.getSelection() );
    wlInclFilename.setEnabled( !wFileField.getSelection() );
    wlLimit.setEnabled( !wFileField.getSelection() );
    wLimit.setEnabled( !wFileField.getSelection() );
    wPreview.setEnabled( !wFileField.getSelection() );
    wGet.setEnabled( !wFileField.getSelection() );
  }

  public void setIncludeFilename() {
    wlInclFilenameField.setEnabled( wInclFilename.getSelection() );
    wInclFilenameField.setEnabled( wInclFilename.getSelection() );
  }

  public void setIncludeSection() {
    boolean active =
      ( PropertyInputMeta.getFileTypeByDesc( wFileType.getText() ) == PropertyInputMeta.FILE_TYPE_INI );
    wlInclINIsectionField.setEnabled( active && wInclINIsection.getSelection() );
    wInclINIsectionField.setEnabled( active && wInclINIsection.getSelection() );
  }

  public void setIncludeRownum() {
    wlInclRownumField.setEnabled( wInclRownum.getSelection() );
    wInclRownumField.setEnabled( wInclRownum.getSelection() );
    wlResetRownum.setEnabled( wInclRownum.getSelection() );
    wResetRownum.setEnabled( wInclRownum.getSelection() );
  }

  /**
   * Read the data from the TextFileInputMeta object and show it in this dialog.
   *
   * @param in The TextFileInputMeta object to obtain the data from.
   */
  public void getData( PropertyInputMeta in ) {
    if ( in.getFileName() != null ) {
      wFilenameList.removeAll();

      for ( int i = 0; i < in.getFileName().length; i++ ) {
        wFilenameList.add( new String[] {
          in.getFileName()[ i ], in.getFileMask()[ i ], in.getExcludeFileMask()[ i ],
          in.getRequiredFilesDesc( in.getFileRequired()[ i ] ),
          in.getRequiredFilesDesc( in.getIncludeSubFolders()[ i ] ) } );
      }
      wFilenameList.removeEmptyRows();
      wFilenameList.setRowNums();
      wFilenameList.optWidth( true );
    }
    wFileType
      .setText( PropertyInputMeta.getFileTypeDesc( PropertyInputMeta.getFileTypeByCode( in.getFileType() ) ) );
    wInclFilename.setSelection( in.includeFilename() );
    wInclRownum.setSelection( in.includeRowNumber() );
    wInclINIsection.setSelection( in.includeIniSection() );
    wAddResult.setSelection( in.isAddResultFile() );
    wResolveValueVariable.setSelection( in.isResolveValueVariable() );
    wFileField.setSelection( in.isFileField() );
    if ( in.getEncoding() != null ) {
      wEncoding.setText( in.getEncoding() );
    }
    if ( in.getFilenameField() != null ) {
      wInclFilenameField.setText( in.getFilenameField() );
    }
    if ( in.getDynamicFilenameField() != null ) {
      wFilenameField.setText( in.getDynamicFilenameField() );
    }
    if ( in.getINISectionField() != null ) {
      wInclINIsectionField.setText( in.getINISectionField() );
    }
    if ( in.getSection() != null ) {
      wSection.setText( in.getSection() );
    }
    if ( in.getRowNumberField() != null ) {
      wInclRownumField.setText( in.getRowNumberField() );
    }
    wResetRownum.setSelection( in.resetRowNumber() );
    wLimit.setText( "" + in.getRowLimit() );

    if ( log.isDebug() ) {
      log.logDebug( BaseMessages.getString( PKG, "PropertyInputDialog.Log.GettingFieldsInfo" ) );
    }
    for ( int i = 0; i < in.getInputFields().length; i++ ) {
      PropertyInputField field = in.getInputFields()[ i ];

      if ( field != null ) {
        TableItem item = wFields.table.getItem( i );
        String name = field.getName();
        String column = field.getColumnDesc();
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
        if ( column != null ) {
          item.setText( 2, column );
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
    setIncludeSection();

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
        shell, BaseMessages.getString( PKG, "PropertyInputDialog.ErrorParsingData.DialogTitle" ), BaseMessages
        .getString( PKG, "PropertyInputDialog.ErrorParsingData.DialogMessage" ), e );
    }
    dispose();
  }

  private void getInfo( PropertyInputMeta in ) throws HopException {
    transformName = wTransformName.getText(); // return value

    // copy info to PropertyInputMeta class (input)
    in.setRowLimit( Const.toLong( wLimit.getText(), 0L ) );
    in.setIncludeFilename( wInclFilename.getSelection() );
    in.setFilenameField( wInclFilenameField.getText() );
    in.setIncludeRowNumber( wInclRownum.getSelection() );
    in.setIncludeIniSection( wInclINIsection.getSelection() );
    in.setAddResultFile( wAddResult.getSelection() );
    in.setEncoding( wEncoding.getText() );
    in.setDynamicFilenameField( wFilenameField.getText() );
    in.setFileField( wFileField.getSelection() );
    in.setRowNumberField( wInclRownumField.getText() );
    in.setINISectionField( wInclINIsectionField.getText() );
    in.setResetRowNumber( wResetRownum.getSelection() );
    in.setResolveValueVariable( wResolveValueVariable.getSelection() );
    int nrFiles = wFilenameList.getItemCount();
    int nrFields = wFields.nrNonEmpty();
    in.allocate( nrFiles, nrFields );
    in.setSection( wSection.getText() );
    in.setFileName( wFilenameList.getItems( 0 ) );
    in.setFileMask( wFilenameList.getItems( 1 ) );
    in.setExcludeFileMask( wFilenameList.getItems( 2 ) );
    in.setFileRequired( wFilenameList.getItems( 3 ) );
    in.setIncludeSubFolders( wFilenameList.getItems( 4 ) );
    in
      .setFileType( PropertyInputMeta
        .getFileTypeCode( PropertyInputMeta.getFileTypeByDesc( wFileType.getText() ) ) );
    for ( int i = 0; i < nrFields; i++ ) {
      PropertyInputField field = new PropertyInputField();

      TableItem item = wFields.getNonEmpty( i );

      field.setName( item.getText( 1 ) );
      field.setColumn( PropertyInputField.getColumnByDesc( item.getText( 2 ) ) );
      field.setType( ValueMetaFactory.getIdForValueMeta( item.getText( 3 ) ) );
      field.setFormat( item.getText( 4 ) );
      field.setLength( Const.toInt( item.getText( 5 ), -1 ) );
      field.setPrecision( Const.toInt( item.getText( 6 ), -1 ) );
      field.setCurrencySymbol( item.getText( 7 ) );
      field.setDecimalSymbol( item.getText( 8 ) );
      field.setGroupSymbol( item.getText( 9 ) );
      field.setTrimType( PropertyInputField.getTrimTypeByDesc( item.getText( 10 ) ) );
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

      PropertyInputMeta oneMeta = new PropertyInputMeta();
      getInfo( oneMeta );

      PipelineMeta previewMeta =
        PipelinePreviewFactory.generatePreviewPipeline( variables, pipelineMeta.getMetadataProvider(), oneMeta, wTransformName.getText() );

      EnterNumberDialog numberDialog = new EnterNumberDialog( shell, props.getDefaultPreviewSize(),
        BaseMessages.getString( PKG, "PropertyInputDialog.NumberRows.DialogTitle" ),
        BaseMessages.getString( PKG, "PropertyInputDialog.NumberRows.DialogMessage" ) );

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
        shell, BaseMessages.getString( PKG, "PropertyInputDialog.ErrorPreviewingData.DialogTitle" ),
        BaseMessages.getString( PKG, "PropertyInputDialog.ErrorPreviewingData.DialogMessage" ), e );
    }
  }

  private void getSections() {
    Wini wini = new Wini();
    PropertyInputMeta meta = new PropertyInputMeta();
    try {
      getInfo( meta );

      FileInputList fileInputList = meta.getFiles( variables );

      if ( fileInputList.nrOfFiles() > 0 ) {
        // Check the first file
        if ( fileInputList.getFile( 0 ).exists() ) {
          // Open the file (only first file) in readOnly ...
          //
          wini = new Wini( HopVfs.getInputStream( fileInputList.getFile( 0 ) ) );
          Iterator<String> itSection = wini.keySet().iterator();
          String[] sectionsList = new String[ wini.keySet().size() ];
          int i = 0;
          while ( itSection.hasNext() ) {
            sectionsList[ i ] = itSection.next().toString();
            i++;
          }
          Const.sortStrings( sectionsList );
          EnterSelectionDialog dialog = new EnterSelectionDialog( shell, sectionsList,
            BaseMessages.getString( PKG, "PropertyInputDialog.Dialog.SelectASection.Title" ),
            BaseMessages.getString( PKG, "PropertyInputDialog.Dialog.SelectASection.Message" ) );
          String sectionname = dialog.open();
          if ( sectionname != null ) {
            wSection.setText( sectionname );
          }
        } else {
          // The file not exists !
          throw new HopException( BaseMessages.getString(
            PKG, "PropertyInputDialog.Exception.FileDoesNotExist", HopVfs.getFilename( fileInputList
              .getFile( 0 ) ) ) );
        }
      } else {
        // No file specified
        MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
        mb.setMessage( BaseMessages.getString( PKG, "PropertyInputDialog.FilesMissing.DialogMessage" ) );
        mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
        mb.open();
      }
    } catch ( Throwable e ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "PropertyInputDialog.UnableToGetListOfSections.Title" ),
        BaseMessages.getString( PKG, "PropertyInputDialog.UnableToGetListOfSections.Message" ), e );
    } finally {
      if ( wini != null ) {
        wini.clear();
      }
      wini = null;
      meta = null;
    }
  }

  private void addAdditionalFieldsTab() {
    // ////////////////////////
    // START OF ADDITIONAL FIELDS TAB ///
    // ////////////////////////
    CTabItem wAdditionalFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdditionalFieldsTab
      .setText( BaseMessages.getString( PKG, "PropertyInputDialog.AdditionalFieldsTab.TabTitle" ) );

    Composite wAdditionalFieldsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wAdditionalFieldsComp);

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 3;
    fieldsLayout.marginHeight = 3;
    wAdditionalFieldsComp.setLayout( fieldsLayout );

    // ShortFileFieldName line
    Label wlShortFileFieldName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlShortFileFieldName.setText( BaseMessages.getString( PKG, "PropertyInputDialog.ShortFileFieldName.Label" ) );
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
    wlExtensionFieldName.setText( BaseMessages.getString( PKG, "PropertyInputDialog.ExtensionFieldName.Label" ) );
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
    wlPathFieldName.setText( BaseMessages.getString( PKG, "PropertyInputDialog.PathFieldName.Label" ) );
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
    wlSizeFieldName.setText( BaseMessages.getString( PKG, "PropertyInputDialog.SizeFieldName.Label" ) );
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
    wlIsHiddenName.setText( BaseMessages.getString( PKG, "PropertyInputDialog.IsHiddenName.Label" ) );
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
      PKG, "PropertyInputDialog.LastModificationTimeName.Label" ) );
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
    wlUriName.setText( BaseMessages.getString( PKG, "PropertyInputDialog.UriName.Label" ) );
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
    wlRootUriName.setText( BaseMessages.getString( PKG, "PropertyInputDialog.RootUriName.Label" ) );
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
