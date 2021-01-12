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

package org.apache.hop.pipeline.transforms.yamlinput;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
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

import java.util.ArrayList;

public class YamlInputDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = YamlInputMeta.class; // For Translator

  private CTabFolder wTabFolder;
  private FormData fdTabFolder;

  private CTabItem wFileTab, wContentTab, wFieldsTab;

  private Composite wFileComp, wContentComp, wFieldsComp;
  private FormData fdFileComp, fdContentComp, fdFieldsComp;

  private Label wlFilename, wlYamlIsAFile;
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

  private FormData fdlXMLField, fdlXMLStreamField, fdlXMLIsAFile;
  private FormData fdXMLField, fdYAMLStreamField;
  private FormData fdOutputField, fdYAMLIsAFile, fdAdditionalFields, fdAddFileResult, fdXmlConf;
  private Label wlYamlField, wlXmlStreamField;
  private CCombo wYAMLLField;
  private Button wYAMLStreamField, wYAMLIsAFile;

  private Label wlInclFilename;
  private Button wInclFilename, wAddResult;
  private FormData fdlInclFilename, fdInclFilename, fdAddResult, fdlAddResult;

  private Label wlInclFilenameField;
  private TextVar wInclFilenameField;
  private FormData fdlInclFilenameField, fdInclFilenameField;

  private Label wlInclRownum, wlAddResult;
  private Button wInclRownum;
  private FormData fdlInclRownum, fdRownum;

  private Label wlInclRownumField;
  private TextVar wInclRownumField;
  private FormData fdlInclRownumField, fdInclRownumField;

  private Label wlLimit;
  private Text wLimit;
  private FormData fdlLimit, fdLimit;

  private TableView wFields;
  private FormData fdFields;

  private Group wOutputField;
  private Group wAdditionalFields;
  private Group wAddFileResult;
  private Group wXmlConf;

  // ignore empty files flag
  private Label wlIgnoreEmptyFile;
  private Button wIgnoreEmptyFile;
  private FormData fdlIgnoreEmptyFile, fdIgnoreEmptyFile;

  // do not fail if no files?
  private Label wlDoNotFailIfNoFile;
  private Button wDoNotFailIfNoFile;
  private FormData fdlDoNotFailIfNoFile, fdDoNotFailIfNoFile;

  private YamlInputMeta input;

  public static final int[] dateLengths = new int[] { 23, 19, 14, 10, 10, 10, 10, 8, 8, 8, 8, 6, 6 };

  ArrayList<String> listpath = new ArrayList<>();
  String precNodeName = null;

  public YamlInputDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (YamlInputMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "YamlInputDialog.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Buttons at the bottom
    //
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wPreview = new Button( shell, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( PKG, "YamlInputDialog.Button.PreviewRows" ) );
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
    wFileTab.setText( BaseMessages.getString( PKG, "YamlInputDialog.File.Tab" ) );

    wFileComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wFileComp );

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wFileComp.setLayout( fileLayout );

    // ///////////////////////////////
    // START OF Output Field GROUP //
    // ///////////////////////////////

    wOutputField = new Group( wFileComp, SWT.SHADOW_NONE );
    props.setLook( wOutputField );
    wOutputField.setText( BaseMessages.getString( PKG, "YamlInputDialog.wOutputField.Label" ) );

    FormLayout outputfieldgroupLayout = new FormLayout();
    outputfieldgroupLayout.marginWidth = 10;
    outputfieldgroupLayout.marginHeight = 10;
    wOutputField.setLayout( outputfieldgroupLayout );

    // Is XML string defined in a Field
    wlXmlStreamField = new Label( wOutputField, SWT.RIGHT );
    wlXmlStreamField.setText( BaseMessages.getString( PKG, "YamlInputDialog.wlXmlStreamField.Label" ) );
    props.setLook( wlXmlStreamField );
    fdlXMLStreamField = new FormData();
    fdlXMLStreamField.left = new FormAttachment( 0, -margin );
    fdlXMLStreamField.top = new FormAttachment( 0, margin );
    fdlXMLStreamField.right = new FormAttachment( middle, -2 * margin );
    wlXmlStreamField.setLayoutData( fdlXMLStreamField );
    wYAMLStreamField = new Button( wOutputField, SWT.CHECK );
    props.setLook( wYAMLStreamField );
    wYAMLStreamField.setToolTipText( BaseMessages.getString( PKG, "YamlInputDialog.wYAMLStreamField.Tooltip" ) );
    fdYAMLStreamField = new FormData();
    fdYAMLStreamField.left = new FormAttachment( middle, -margin );
    fdYAMLStreamField.top = new FormAttachment( wlXmlStreamField, 0, SWT.CENTER );
    wYAMLStreamField.setLayoutData( fdYAMLStreamField );
    SelectionAdapter lsyamlstream = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        ActiveStreamField();
        input.setChanged();
      }
    };
    wYAMLStreamField.addSelectionListener( lsyamlstream );

    // Is XML source is a file?
    wlYamlIsAFile = new Label( wOutputField, SWT.RIGHT );
    wlYamlIsAFile.setText( BaseMessages.getString( PKG, "YamlInputDialog.XMLIsAFile.Label" ) );
    props.setLook( wlYamlIsAFile );
    fdlXMLIsAFile = new FormData();
    fdlXMLIsAFile.left = new FormAttachment( 0, -margin );
    fdlXMLIsAFile.top = new FormAttachment( wYAMLStreamField, margin );
    fdlXMLIsAFile.right = new FormAttachment( middle, -2 * margin );
    wlYamlIsAFile.setLayoutData( fdlXMLIsAFile );
    wYAMLIsAFile = new Button( wOutputField, SWT.CHECK );
    props.setLook( wYAMLIsAFile );
    wYAMLIsAFile.setToolTipText( BaseMessages.getString( PKG, "YamlInputDialog.XMLIsAFile.Tooltip" ) );
    fdYAMLIsAFile = new FormData();
    fdYAMLIsAFile.left = new FormAttachment( middle, -margin );
    fdYAMLIsAFile.top = new FormAttachment( wlYamlIsAFile, 0, SWT.CENTER );
    wYAMLIsAFile.setLayoutData( fdYAMLIsAFile );
    SelectionAdapter lsyamlisafile = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
        enableFileSettings();
      }
    };
    wYAMLIsAFile.addSelectionListener( lsyamlisafile );

    // If XML string defined in a Field
    wlYamlField = new Label( wOutputField, SWT.RIGHT );
    wlYamlField.setText( BaseMessages.getString( PKG, "YamlInputDialog.wlYamlField.Label" ) );
    props.setLook( wlYamlField );
    fdlXMLField = new FormData();
    fdlXMLField.left = new FormAttachment( 0, -margin );
    fdlXMLField.top = new FormAttachment( wYAMLIsAFile, margin );
    fdlXMLField.right = new FormAttachment( middle, -2 * margin );
    wlYamlField.setLayoutData( fdlXMLField );

    wYAMLLField = new CCombo( wOutputField, SWT.BORDER | SWT.READ_ONLY );
    wYAMLLField.setEditable( true );
    props.setLook( wYAMLLField );
    wYAMLLField.addModifyListener( lsMod );
    fdXMLField = new FormData();
    fdXMLField.left = new FormAttachment( middle, -margin );
    fdXMLField.top = new FormAttachment( wYAMLIsAFile, margin );
    fdXMLField.right = new FormAttachment( 100, -margin );
    wYAMLLField.setLayoutData( fdXMLField );
    wYAMLLField.addFocusListener( new FocusListener() {
      public void focusLost( FocusEvent e ) {
      }

      public void focusGained( FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        setXMLStreamField();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    fdOutputField = new FormData();
    fdOutputField.left = new FormAttachment( 0, margin );
    fdOutputField.top = new FormAttachment( wFilenameList, margin );
    fdOutputField.right = new FormAttachment( 100, -margin );
    wOutputField.setLayoutData( fdOutputField );

    // ///////////////////////////////////////////////////////////
    // / END OF Output Field GROUP
    // ///////////////////////////////////////////////////////////

    // Filename line
    wlFilename = new Label( wFileComp, SWT.RIGHT );
    wlFilename.setText( BaseMessages.getString( PKG, "YamlInputDialog.Filename.Label" ) );
    props.setLook( wlFilename );
    fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment( wOutputField, margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData( fdlFilename );

    wbbFilename = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbbFilename );
    wbbFilename.setText( BaseMessages.getString( PKG, "YamlInputDialog.FilenameBrowse.Button" ) );
    wbbFilename.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.BrowseForFileOrDirAndAdd" ) );
    fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment( wOutputField, margin );
    wbbFilename.setLayoutData( fdbFilename );

    wbaFilename = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbaFilename );
    wbaFilename.setText( BaseMessages.getString( PKG, "YamlInputDialog.FilenameAdd.Button" ) );
    wbaFilename.setToolTipText( BaseMessages.getString( PKG, "YamlInputDialog.FilenameAdd.Tooltip" ) );
    fdbaFilename = new FormData();
    fdbaFilename.right = new FormAttachment( wbbFilename, -margin );
    fdbaFilename.top = new FormAttachment( wOutputField, margin );
    wbaFilename.setLayoutData( fdbaFilename );

    wFilename = new TextVar( variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.right = new FormAttachment( wbaFilename, -margin );
    fdFilename.top = new FormAttachment( wOutputField, margin );
    wFilename.setLayoutData( fdFilename );

    wlFilemask = new Label( wFileComp, SWT.RIGHT );
    wlFilemask.setText( BaseMessages.getString( PKG, "YamlInputDialog.RegExp.Label" ) );
    props.setLook( wlFilemask );
    fdlFilemask = new FormData();
    fdlFilemask.left = new FormAttachment( 0, 0 );
    fdlFilemask.top = new FormAttachment( wFilename, margin );
    fdlFilemask.right = new FormAttachment( middle, -margin );
    wlFilemask.setLayoutData( fdlFilemask );
    wFilemask = new TextVar( variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilemask );
    wFilemask.addModifyListener( lsMod );
    fdFilemask = new FormData();
    fdFilemask.left = new FormAttachment( middle, 0 );
    fdFilemask.top = new FormAttachment( wFilename, margin );
    fdFilemask.right = new FormAttachment( 100, 0 );
    wFilemask.setLayoutData( fdFilemask );

    // Filename list line
    wlFilenameList = new Label( wFileComp, SWT.RIGHT );
    wlFilenameList.setText( BaseMessages.getString( PKG, "YamlInputDialog.FilenameList.Label" ) );
    props.setLook( wlFilenameList );
    fdlFilenameList = new FormData();
    fdlFilenameList.left = new FormAttachment( 0, 0 );
    fdlFilenameList.top = new FormAttachment( wFilemask, margin );
    fdlFilenameList.right = new FormAttachment( middle, -margin );
    wlFilenameList.setLayoutData( fdlFilenameList );

    // Buttons to the right of the screen...
    wbdFilename = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbdFilename );
    wbdFilename.setText( BaseMessages.getString( PKG, "YamlInputDialog.FilenameRemove.Button" ) );
    wbdFilename.setToolTipText( BaseMessages.getString( PKG, "YamlInputDialog.FilenameRemove.Tooltip" ) );
    fdbdFilename = new FormData();
    fdbdFilename.right = new FormAttachment( 100, 0 );
    fdbdFilename.top = new FormAttachment( wFilemask, 40 );
    wbdFilename.setLayoutData( fdbdFilename );

    wbeFilename = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbeFilename );
    wbeFilename.setText( BaseMessages.getString( PKG, "YamlInputDialog.FilenameEdit.Button" ) );
    wbeFilename.setToolTipText( BaseMessages.getString( PKG, "YamlInputDialog.FilenameEdit.Tooltip" ) );
    fdbeFilename = new FormData();
    fdbeFilename.right = new FormAttachment( 100, 0 );
    fdbeFilename.left = new FormAttachment( wbdFilename, 0, SWT.LEFT );
    fdbeFilename.top = new FormAttachment( wbdFilename, margin );
    wbeFilename.setLayoutData( fdbeFilename );

    wbShowFiles = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbShowFiles );
    wbShowFiles.setText( BaseMessages.getString( PKG, "YamlInputDialog.ShowFiles.Button" ) );
    fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment( middle, 0 );
    fdbShowFiles.bottom = new FormAttachment( 100, 0 );
    wbShowFiles.setLayoutData( fdbShowFiles );

    ColumnInfo[] colinfo = new ColumnInfo[ 4 ];
    colinfo[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "YamlInputDialog.Files.Filename.Column" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );
    colinfo[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "YamlInputDialog.Files.Wildcard.Column" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );

    colinfo[ 0 ].setUsingVariables( true );
    colinfo[ 1 ].setUsingVariables( true );
    colinfo[ 1 ].setToolTip( BaseMessages.getString( PKG, "YamlInputDialog.Files.Wildcard.Tooltip" ) );
    colinfo[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "YamlInputDialog.Required.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        YamlInputMeta.RequiredFilesDesc );
    colinfo[ 2 ].setToolTip( BaseMessages.getString( PKG, "YamlInputDialog.Required.Tooltip" ) );
    colinfo[ 3 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "YamlInputDialog.IncludeSubDirs.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        YamlInputMeta.RequiredFilesDesc );
    colinfo[ 3 ].setToolTip( BaseMessages.getString( PKG, "YamlInputDialog.IncludeSubDirs.Tooltip" ) );

    wFilenameList =
      new TableView(
        variables, wFileComp, SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER, colinfo, 2, lsMod, props );
    props.setLook( wFilenameList );
    fdFilenameList = new FormData();
    fdFilenameList.left = new FormAttachment( middle, 0 );
    fdFilenameList.right = new FormAttachment( wbdFilename, -margin );
    fdFilenameList.top = new FormAttachment( wFilemask, margin );
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
    wContentTab.setText( BaseMessages.getString( PKG, "YamlInputDialog.Content.Tab" ) );

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    wContentComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wContentComp );
    wContentComp.setLayout( contentLayout );

    // ///////////////////////////////
    // START OF XmlConf Field GROUP //
    // ///////////////////////////////

    wXmlConf = new Group( wContentComp, SWT.SHADOW_NONE );
    props.setLook( wXmlConf );
    wXmlConf.setText( BaseMessages.getString( PKG, "YamlInputDialog.wXmlConf.Label" ) );

    FormLayout XmlConfgroupLayout = new FormLayout();
    XmlConfgroupLayout.marginWidth = 10;
    XmlConfgroupLayout.marginHeight = 10;
    wXmlConf.setLayout( XmlConfgroupLayout );

    // Ignore Empty File
    wlIgnoreEmptyFile = new Label( wXmlConf, SWT.RIGHT );
    wlIgnoreEmptyFile.setText( BaseMessages.getString( PKG, "YamlInputDialog.IgnoreEmptyFile.Label" ) );
    props.setLook( wlIgnoreEmptyFile );
    fdlIgnoreEmptyFile = new FormData();
    fdlIgnoreEmptyFile.left = new FormAttachment( 0, 0 );
    fdlIgnoreEmptyFile.top = new FormAttachment( 0, margin );
    fdlIgnoreEmptyFile.right = new FormAttachment( middle, -margin );
    wlIgnoreEmptyFile.setLayoutData( fdlIgnoreEmptyFile );
    wIgnoreEmptyFile = new Button( wXmlConf, SWT.CHECK );
    props.setLook( wIgnoreEmptyFile );
    wIgnoreEmptyFile.setToolTipText( BaseMessages.getString( PKG, "YamlInputDialog.IgnoreEmptyFile.Tooltip" ) );
    fdIgnoreEmptyFile = new FormData();
    fdIgnoreEmptyFile.left = new FormAttachment( middle, 0 );
    fdIgnoreEmptyFile.top = new FormAttachment( wlIgnoreEmptyFile, 0, SWT.CENTER );
    wIgnoreEmptyFile.setLayoutData( fdIgnoreEmptyFile );
    wIgnoreEmptyFile.addSelectionListener( new ComponentSelectionListener( input ) );

    // do not fail if no files?
    wlDoNotFailIfNoFile = new Label( wXmlConf, SWT.RIGHT );
    wlDoNotFailIfNoFile.setText( BaseMessages.getString( PKG, "YamlInputDialog.doNotFailIfNoFile.Label" ) );
    props.setLook( wlDoNotFailIfNoFile );
    fdlDoNotFailIfNoFile = new FormData();
    fdlDoNotFailIfNoFile.left = new FormAttachment( 0, 0 );
    fdlDoNotFailIfNoFile.top = new FormAttachment( wIgnoreEmptyFile, margin );
    fdlDoNotFailIfNoFile.right = new FormAttachment( middle, -margin );
    wlDoNotFailIfNoFile.setLayoutData( fdlDoNotFailIfNoFile );
    wDoNotFailIfNoFile = new Button( wXmlConf, SWT.CHECK );
    props.setLook( wDoNotFailIfNoFile );
    wDoNotFailIfNoFile.setToolTipText( BaseMessages.getString( PKG, "YamlInputDialog.doNotFailIfNoFile.Tooltip" ) );
    fdDoNotFailIfNoFile = new FormData();
    fdDoNotFailIfNoFile.left = new FormAttachment( middle, 0 );
    fdDoNotFailIfNoFile.top = new FormAttachment( wlDoNotFailIfNoFile, 0, SWT.CENTER );
    wDoNotFailIfNoFile.setLayoutData( fdDoNotFailIfNoFile );
    wDoNotFailIfNoFile.addSelectionListener( new ComponentSelectionListener( input ) );

    wlLimit = new Label( wXmlConf, SWT.RIGHT );
    wlLimit.setText( BaseMessages.getString( PKG, "YamlInputDialog.Limit.Label" ) );
    props.setLook( wlLimit );
    fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment( 0, 0 );
    fdlLimit.top = new FormAttachment( wDoNotFailIfNoFile, margin );
    fdlLimit.right = new FormAttachment( middle, -margin );
    wlLimit.setLayoutData( fdlLimit );
    wLimit = new Text( wXmlConf, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLimit );
    wLimit.addModifyListener( lsMod );
    fdLimit = new FormData();
    fdLimit.left = new FormAttachment( middle, 0 );
    fdLimit.top = new FormAttachment( wDoNotFailIfNoFile, margin );
    fdLimit.right = new FormAttachment( 100, 0 );
    wLimit.setLayoutData( fdLimit );

    fdXmlConf = new FormData();
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

    wAdditionalFields = new Group( wContentComp, SWT.SHADOW_NONE );
    props.setLook( wAdditionalFields );
    wAdditionalFields.setText( BaseMessages.getString( PKG, "YamlInputDialog.wAdditionalFields.Label" ) );

    FormLayout AdditionalFieldsgroupLayout = new FormLayout();
    AdditionalFieldsgroupLayout.marginWidth = 10;
    AdditionalFieldsgroupLayout.marginHeight = 10;
    wAdditionalFields.setLayout( AdditionalFieldsgroupLayout );

    wlInclFilename = new Label( wAdditionalFields, SWT.RIGHT );
    wlInclFilename.setText( BaseMessages.getString( PKG, "YamlInputDialog.InclFilename.Label" ) );
    props.setLook( wlInclFilename );
    fdlInclFilename = new FormData();
    fdlInclFilename.left = new FormAttachment( 0, 0 );
    fdlInclFilename.top = new FormAttachment( wXmlConf, 4 * margin );
    fdlInclFilename.right = new FormAttachment( middle, -margin );
    wlInclFilename.setLayoutData( fdlInclFilename );
    wInclFilename = new Button( wAdditionalFields, SWT.CHECK );
    props.setLook( wInclFilename );
    wInclFilename.setToolTipText( BaseMessages.getString( PKG, "YamlInputDialog.InclFilename.Tooltip" ) );
    fdInclFilename = new FormData();
    fdInclFilename.left = new FormAttachment( middle, 0 );
    fdInclFilename.top = new FormAttachment( wlInclFilename, 0, SWT.CENTER );
    wInclFilename.setLayoutData( fdInclFilename );
    wInclFilename.addSelectionListener( new ComponentSelectionListener( input ) );

    wlInclFilenameField = new Label( wAdditionalFields, SWT.LEFT );
    wlInclFilenameField.setText( BaseMessages.getString( PKG, "YamlInputDialog.InclFilenameField.Label" ) );
    props.setLook( wlInclFilenameField );
    fdlInclFilenameField = new FormData();
    fdlInclFilenameField.left = new FormAttachment( wInclFilename, margin );
    fdlInclFilenameField.top = new FormAttachment( wLimit, 4 * margin );
    wlInclFilenameField.setLayoutData( fdlInclFilenameField );
    wInclFilenameField = new TextVar( variables, wAdditionalFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclFilenameField );
    wInclFilenameField.addModifyListener( lsMod );
    fdInclFilenameField = new FormData();
    fdInclFilenameField.left = new FormAttachment( wlInclFilenameField, margin );
    fdInclFilenameField.top = new FormAttachment( wLimit, 4 * margin );
    fdInclFilenameField.right = new FormAttachment( 100, 0 );
    wInclFilenameField.setLayoutData( fdInclFilenameField );

    wlInclRownum = new Label( wAdditionalFields, SWT.RIGHT );
    wlInclRownum.setText( BaseMessages.getString( PKG, "YamlInputDialog.InclRownum.Label" ) );
    props.setLook( wlInclRownum );
    fdlInclRownum = new FormData();
    fdlInclRownum.left = new FormAttachment( 0, 0 );
    fdlInclRownum.top = new FormAttachment( wInclFilenameField, margin );
    fdlInclRownum.right = new FormAttachment( middle, -margin );
    wlInclRownum.setLayoutData( fdlInclRownum );
    wInclRownum = new Button( wAdditionalFields, SWT.CHECK );
    props.setLook( wInclRownum );
    wInclRownum.setToolTipText( BaseMessages.getString( PKG, "YamlInputDialog.InclRownum.Tooltip" ) );
    fdRownum = new FormData();
    fdRownum.left = new FormAttachment( middle, 0 );
    fdRownum.top = new FormAttachment( wlInclRownum, 0, SWT.CENTER );
    wInclRownum.setLayoutData( fdRownum );
    wInclRownum.addSelectionListener( new ComponentSelectionListener( input ) );

    wlInclRownumField = new Label( wAdditionalFields, SWT.RIGHT );
    wlInclRownumField.setText( BaseMessages.getString( PKG, "YamlInputDialog.InclRownumField.Label" ) );
    props.setLook( wlInclRownumField );
    fdlInclRownumField = new FormData();
    fdlInclRownumField.left = new FormAttachment( wInclRownum, margin );
    fdlInclRownumField.top = new FormAttachment( wInclFilenameField, margin );
    wlInclRownumField.setLayoutData( fdlInclRownumField );
    wInclRownumField = new TextVar( variables, wAdditionalFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclRownumField );
    wInclRownumField.addModifyListener( lsMod );
    fdInclRownumField = new FormData();
    fdInclRownumField.left = new FormAttachment( wlInclRownumField, margin );
    fdInclRownumField.top = new FormAttachment( wInclFilenameField, margin );
    fdInclRownumField.right = new FormAttachment( 100, 0 );
    wInclRownumField.setLayoutData( fdInclRownumField );

    fdAdditionalFields = new FormData();
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

    wAddFileResult = new Group( wContentComp, SWT.SHADOW_NONE );
    props.setLook( wAddFileResult );
    wAddFileResult.setText( BaseMessages.getString( PKG, "YamlInputDialog.wAddFileResult.Label" ) );

    FormLayout AddFileResultgroupLayout = new FormLayout();
    AddFileResultgroupLayout.marginWidth = 10;
    AddFileResultgroupLayout.marginHeight = 10;
    wAddFileResult.setLayout( AddFileResultgroupLayout );

    wlAddResult = new Label( wAddFileResult, SWT.RIGHT );
    wlAddResult.setText( BaseMessages.getString( PKG, "YamlInputDialog.AddResult.Label" ) );
    props.setLook( wlAddResult );
    fdlAddResult = new FormData();
    fdlAddResult.left = new FormAttachment( 0, 0 );
    fdlAddResult.top = new FormAttachment( wAdditionalFields, margin );
    fdlAddResult.right = new FormAttachment( middle, -margin );
    wlAddResult.setLayoutData( fdlAddResult );
    wAddResult = new Button( wAddFileResult, SWT.CHECK );
    props.setLook( wAddResult );
    wAddResult.setToolTipText( BaseMessages.getString( PKG, "YamlInputDialog.AddResult.Tooltip" ) );
    fdAddResult = new FormData();
    fdAddResult.left = new FormAttachment( middle, 0 );
    fdAddResult.top = new FormAttachment( wlAddResult, 0, SWT.CENTER );
    wAddResult.setLayoutData( fdAddResult );
    wAddResult.addSelectionListener( new ComponentSelectionListener( input ) );

    fdAddFileResult = new FormData();
    fdAddFileResult.left = new FormAttachment( 0, margin );
    fdAddFileResult.top = new FormAttachment( wAdditionalFields, margin );
    fdAddFileResult.right = new FormAttachment( 100, -margin );
    wAddFileResult.setLayoutData( fdAddFileResult );

    // ///////////////////////////////////////////////////////////
    // / END OF AddFileResult GROUP
    // ///////////////////////////////////////////////////////////

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

    // Fields tab...
    //
    wFieldsTab = new CTabItem( wTabFolder, SWT.NONE );
    wFieldsTab.setText( BaseMessages.getString( PKG, "YamlInputDialog.Fields.Tab" ) );

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;

    wFieldsComp = new Composite( wTabFolder, SWT.NONE );
    wFieldsComp.setLayout( fieldsLayout );
    props.setLook( wFieldsComp );

    wGet = new Button( wFieldsComp, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "YamlInputDialog.GetFields.Button" ) );
    fdGet = new FormData();
    fdGet.left = new FormAttachment( 50, 0 );
    fdGet.bottom = new FormAttachment( 100, 0 );
    wGet.setLayoutData( fdGet );

    final int FieldsRows = input.getInputFields().length;

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "YamlInputDialog.FieldsTable.Name.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "YamlInputDialog.FieldsTable.XPath.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "YamlInputDialog.FieldsTable.Type.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getValueMetaNames(), true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "YamlInputDialog.FieldsTable.Format.Column" ),
          ColumnInfo.COLUMN_TYPE_FORMAT, 4 ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "YamlInputDialog.FieldsTable.Length.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "YamlInputDialog.FieldsTable.Precision.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "YamlInputDialog.FieldsTable.Currency.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "YamlInputDialog.FieldsTable.Decimal.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "YamlInputDialog.FieldsTable.Group.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "YamlInputDialog.FieldsTable.TrimType.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, YamlInputField.trimTypeDesc, true ) };

    colinf[ 0 ].setUsingVariables( true );
    colinf[ 0 ].setToolTip( BaseMessages.getString( PKG, "YamlInputDialog.FieldsTable.Name.Column.Tooltip" ) );
    colinf[ 1 ].setUsingVariables( true );
    colinf[ 1 ].setToolTip( BaseMessages.getString( PKG, "YamlInputDialog.FieldsTable.XPath.Column.Tooltip" ) );

    wFields =
      new TableView( variables, wFieldsComp, SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( 0, 0 );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wGet, -margin );
    wFields.setLayoutData( fdFields );

    fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment( 0, 0 );
    fdFieldsComp.top = new FormAttachment( 0, 0 );
    fdFieldsComp.right = new FormAttachment( 100, 0 );
    fdFieldsComp.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData( fdFieldsComp );

    wFieldsComp.layout();
    wFieldsTab.setControl( wFieldsComp );

    fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wTransformName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOk, -2*margin );
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
        wFilenameList.add( new String[] { wFilename.getText(), wFilemask.getText() } );
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
          YamlInputMeta tfii = new YamlInputMeta();
          getInfo( tfii );
          FileInputList fileInputList = tfii.getFiles( variables );
          String[] files = fileInputList.getFileStrings();
          if ( files != null && files.length > 0 ) {
            EnterSelectionDialog esd = new EnterSelectionDialog( shell, files,
              BaseMessages.getString( PKG, "YamlInputDialog.FilesReadSelection.DialogTitle" ),
              BaseMessages.getString( PKG, "YamlInputDialog.FilesReadSelection.DialogMessage" ) );
            esd.setViewOnly();
            esd.open();
          } else {
            MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
            mb.setMessage( BaseMessages.getString( PKG, "YamlInputDialog.NoFileFound.DialogMessage" ) );
            mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
            mb.open();
          }
        } catch ( HopException ex ) {
          new ErrorDialog(
            shell, BaseMessages.getString( PKG, "YamlInputDialog.ErrorParsingData.DialogTitle" ), BaseMessages
            .getString( PKG, "YamlInputDialog.ErrorParsingData.DialogMessage" ), ex );
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
    wFilename.addModifyListener(e -> wFilename.setToolTipText( wFilename.getText() ));

    // Listen to the Browse... button
    if ( wbbFilename != null ) {
      // Listen to the browse button next to the file name
      //
      wbbFilename.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wFilename, variables,
              new String[] { "*.yaml;*.YAML;*.yml;*.YML", "*" },
              new String[] {
                      "Yaml files",
                      BaseMessages.getString( PKG, "System.FileType.AllFiles" ) },
              true )
      );
    }


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
    ActiveStreamField();
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

      wYAMLLField.removeAll();

      IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
      if ( r != null ) {
        r.getFieldNames();

        for ( int i = 0; i < r.getFieldNames().length; i++ ) {
          wYAMLLField.add( r.getFieldNames()[ i ] );

        }
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "YamlInputDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "YamlInputDialog.FailedToGetFields.DialogMessage" ), ke );
    }
  }

  private void ActiveStreamField() {
    wlYamlField.setEnabled( wYAMLStreamField.getSelection() );
    wYAMLLField.setEnabled( wYAMLStreamField.getSelection() );
    wlYamlIsAFile.setEnabled( wYAMLStreamField.getSelection() );
    wYAMLIsAFile.setEnabled( wYAMLStreamField.getSelection() );

    wlFilename.setEnabled( !wYAMLStreamField.getSelection() );
    wbbFilename.setEnabled( !wYAMLStreamField.getSelection() );
    wbaFilename.setEnabled( !wYAMLStreamField.getSelection() );
    wFilename.setEnabled( !wYAMLStreamField.getSelection() );
    wlFilemask.setEnabled( !wYAMLStreamField.getSelection() );
    wFilemask.setEnabled( !wYAMLStreamField.getSelection() );
    wlFilenameList.setEnabled( !wYAMLStreamField.getSelection() );
    wbdFilename.setEnabled( !wYAMLStreamField.getSelection() );
    wbeFilename.setEnabled( !wYAMLStreamField.getSelection() );
    wbShowFiles.setEnabled( !wYAMLStreamField.getSelection() );
    wlFilenameList.setEnabled( !wYAMLStreamField.getSelection() );
    wFilenameList.setEnabled( !wYAMLStreamField.getSelection() );
    wInclFilename.setEnabled( !wYAMLStreamField.getSelection() );
    wlInclFilename.setEnabled( !wYAMLStreamField.getSelection() );

    if ( wYAMLStreamField.getSelection() ) {
      wInclFilename.setSelection( false );
      wlInclFilenameField.setEnabled( false );
      wInclFilenameField.setEnabled( false );
    } else {
      wlInclFilenameField.setEnabled( wInclFilename.getSelection() );
      wInclFilenameField.setEnabled( wInclFilename.getSelection() );
    }

    wAddResult.setEnabled( !wYAMLStreamField.getSelection() );
    wlAddResult.setEnabled( !wYAMLStreamField.getSelection() );
    wLimit.setEnabled( !wYAMLStreamField.getSelection() );
    wlLimit.setEnabled( !wYAMLStreamField.getSelection() );
    wPreview.setEnabled( !wYAMLStreamField.getSelection() );
    wGet.setEnabled( !wYAMLStreamField.getSelection() );
    enableFileSettings();
  }

  private void enableFileSettings() {
    boolean active =
      !wYAMLStreamField.getSelection() || ( wYAMLStreamField.getSelection() && wYAMLIsAFile.getSelection() );
    wlIgnoreEmptyFile.setEnabled( active );
    wIgnoreEmptyFile.setEnabled( active );
    wlDoNotFailIfNoFile.setEnabled( active );
    wDoNotFailIfNoFile.setEnabled( active );
  }

  private void get() {
    YamlReader yaml = null;
    try {
      YamlInputMeta meta = new YamlInputMeta();
      getInfo( meta );

      FileInputList inputList = meta.getFiles( variables );

      if ( inputList.getFiles().size() > 0 ) {
        wFields.removeAll();

        yaml = new YamlReader();
        yaml.loadFile( inputList.getFile( 0 ) );
        RowMeta row = yaml.getFields();

        for ( int i = 0; i < row.size(); i++ ) {
          IValueMeta value = row.getValueMeta( i );

          TableItem item = new TableItem( wFields.table, SWT.NONE );
          item.setText( 1, value.getName() );
          item.setText( 2, value.getName() );
          item.setText( 3, value.getTypeDesc() );
        }
        wFields.removeEmptyRows();
        wFields.setRowNums();
        wFields.optWidth( true );
      }
    } catch ( Exception e ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "YamlInputDialog.ErrorParsingData.DialogTitle" ), BaseMessages
        .getString( PKG, "YamlInputDialog.ErrorParsingData.DialogMessage" ), e );
    } finally {
      if ( yaml != null ) {
        try {
          yaml.close();
        } catch ( Exception e ) { /* Ignore */
        }
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
  public void getData( YamlInputMeta in ) {
    if ( in.getFileName() != null ) {
      wFilenameList.removeAll();

      for ( int i = 0; i < in.getFileName().length; i++ ) {
        wFilenameList.add( new String[] {
          in.getFileName()[ i ], in.getFileMask()[ i ], in.getRequiredFilesDesc( in.getFileRequired()[ i ] ),
          in.getRequiredFilesDesc( in.getIncludeSubFolders()[ i ] ) } );
      }
      wFilenameList.removeEmptyRows();
      wFilenameList.setRowNums();
      wFilenameList.optWidth( true );
    }
    wInclFilename.setSelection( in.includeFilename() );
    wInclRownum.setSelection( in.includeRowNumber() );
    wAddResult.setSelection( in.addResultFile() );
    wIgnoreEmptyFile.setSelection( in.isIgnoreEmptyFile() );
    wDoNotFailIfNoFile.setSelection( in.isdoNotFailIfNoFile() );
    wYAMLStreamField.setSelection( in.isInFields() );
    wYAMLIsAFile.setSelection( in.getIsAFile() );

    if ( in.getYamlField() != null ) {
      wYAMLLField.setText( in.getYamlField() );
    }

    if ( in.getFilenameField() != null ) {
      wInclFilenameField.setText( in.getFilenameField() );
    }
    if ( in.getRowNumberField() != null ) {
      wInclRownumField.setText( in.getRowNumberField() );
    }
    wLimit.setText( "" + in.getRowLimit() );

    if ( isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "YamlInputDialog.Log.GettingFieldsInfo" ) );
    }
    for ( int i = 0; i < in.getInputFields().length; i++ ) {
      YamlInputField field = in.getInputFields()[ i ];

      if ( field != null ) {
        TableItem item = wFields.table.getItem( i );
        String name = field.getName();
        String path = field.getPath();
        String type = field.getTypeDesc();
        String format = field.getFormat();
        String length = "" + field.getLength();
        String prec = "" + field.getPrecision();
        String curr = field.getCurrencySymbol();
        String group = field.getGroupSymbol();
        String decim = field.getDecimalSymbol();
        String trim = field.getTrimTypeDesc();

        if ( name != null ) {
          item.setText( 1, name );
        }
        if ( path != null ) {
          item.setText( 2, path );
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

      }
    }

    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth( true );

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
        shell, BaseMessages.getString( PKG, "YamlInputDialog.ErrorParsingData.DialogTitle" ), BaseMessages
        .getString( PKG, "YamlInputDialog.ErrorParsingData.DialogMessage" ), e );
    }
    dispose();
  }

  private void getInfo( YamlInputMeta in ) throws HopException {
    transformName = wTransformName.getText(); // return value

    // copy info to TextFileInputMeta class (input)
    in.setRowLimit( Const.toLong( wLimit.getText(), 0L ) );
    in.setFilenameField( wInclFilenameField.getText() );
    in.setRowNumberField( wInclRownumField.getText() );
    in.setAddResultFile( wAddResult.getSelection() );
    in.setIncludeFilename( wInclFilename.getSelection() );
    in.setIncludeRowNumber( wInclRownum.getSelection() );
    in.setIgnoreEmptyFile( wIgnoreEmptyFile.getSelection() );
    in.setdoNotFailIfNoFile( wDoNotFailIfNoFile.getSelection() );

    in.setInFields( wYAMLStreamField.getSelection() );
    in.setIsAFile( wYAMLIsAFile.getSelection() );
    in.setYamlField( wYAMLLField.getText() );

    int nrFiles = wFilenameList.getItemCount();
    int nrFields = wFields.nrNonEmpty();

    in.allocate( nrFiles, nrFields );
    in.setFileName( wFilenameList.getItems( 0 ) );
    in.setFileMask( wFilenameList.getItems( 1 ) );
    in.setFileRequired( wFilenameList.getItems( 2 ) );
    in.setIncludeSubFolders( wFilenameList.getItems( 3 ) );

    for ( int i = 0; i < nrFields; i++ ) {
      YamlInputField field = new YamlInputField();

      TableItem item = wFields.getNonEmpty( i );

      field.setName( item.getText( 1 ) );
      field.setPath( item.getText( 2 ) );
      field.setType( ValueMetaFactory.getIdForValueMeta( item.getText( 3 ) ) );
      field.setFormat( item.getText( 4 ) );
      field.setLength( Const.toInt( item.getText( 5 ), -1 ) );
      field.setPrecision( Const.toInt( item.getText( 6 ), -1 ) );
      field.setCurrencySymbol( item.getText( 7 ) );
      field.setDecimalSymbol( item.getText( 8 ) );
      field.setGroupSymbol( item.getText( 9 ) );
      field.setTrimType( YamlInputField.getTrimTypeByDesc( item.getText( 10 ) ) );

      //CHECKSTYLE:Indentation:OFF
      in.getInputFields()[ i ] = field;
    }
  }

  // Preview the data
  private void preview() {
    try {
      // Create the XML input transform
      YamlInputMeta oneMeta = new YamlInputMeta();
      getInfo( oneMeta );

      PipelineMeta previewMeta = PipelinePreviewFactory.generatePreviewPipeline( variables, pipelineMeta.getMetadataProvider(), oneMeta, wTransformName.getText() );

      EnterNumberDialog numberDialog = new EnterNumberDialog( shell, props.getDefaultPreviewSize(),
        BaseMessages.getString( PKG, "YamlInputDialog.NumberRows.DialogTitle" ),
        BaseMessages.getString( PKG, "YamlInputDialog.NumberRows.DialogMessage" ) );

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
        shell, BaseMessages.getString( PKG, "YamlInputDialog.ErrorPreviewingData.DialogTitle" ), BaseMessages
        .getString( PKG, "YamlInputDialog.ErrorPreviewingData.DialogMessage" ), e );
    }
  }
}
