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

package org.apache.hop.pipeline.transforms.propertyoutput;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class PropertyOutputDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = PropertyOutputMeta.class; // For Translator

  private Button wAddToResult;

  private Label wlFilename;
  private Button wbFilename;
  private TextVar wFilename;

  private Label wlExtension;
  private TextVar wExtension;

  private Button wFileNameInField;

  private Label wlFileNameField;
  private ComboVar wFileNameField;

  private Label wlAddTransformNr;
  private Button wAddTransformNr;

  private Label wlAddDate;
  private Button wAddDate;

  private Label wlAddTime;
  private Button wAddTime;

  private Button wbShowFiles;

  private CCombo wKeyField;

  private CCombo wValueField;

  private Button wCreateParentFolder;

  private boolean gotPreviousFields = false;
  private String[] fieldNames;

  private Text wComment;

  private Button wAppend;

  private final PropertyOutputMeta input;

  public PropertyOutputDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (PropertyOutputMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();
    backupChanged = input.hasChanged();

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "PropertyOutputDialog.DialogTitle" ) );

    // get previous fields name
    getFields();

    // Some buttons
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    setButtonPositions( new Button[] { wOk, wCancel }, margin, null);


    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "System.Label.TransformName" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.right = new FormAttachment( middle, -margin );
    fdlTransformName.top = new FormAttachment( 0, margin );
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

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText( BaseMessages.getString( PKG, "PropertyOutputDialog.GeneralTab.TabTitle" ) );

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout( generalLayout );

    // Fields grouping?
    // ////////////////////////
    // START OF Fields GROUP
    //

    Group wFields = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wFields);
    wFields.setText( BaseMessages.getString( PKG, "PropertyOutputDialog.Group.Fields.Label" ) );

    FormLayout groupFieldsLayout = new FormLayout();
    groupFieldsLayout.marginWidth = 10;
    groupFieldsLayout.marginHeight = 10;
    wFields.setLayout( groupFieldsLayout );

    // Key field
    Label wlKeyField = new Label(wFields, SWT.RIGHT);
    wlKeyField.setText( BaseMessages.getString( PKG, "PropertyOutputDialog.KeyField.Label" ) );
    props.setLook(wlKeyField);
    FormData fdlKeyField = new FormData();
    fdlKeyField.left = new FormAttachment( 0, 0 );
    fdlKeyField.top = new FormAttachment( 0, 3 * margin );
    fdlKeyField.right = new FormAttachment( middle, -margin );
    wlKeyField.setLayoutData(fdlKeyField);
    wKeyField = new CCombo(wFields, SWT.BORDER | SWT.READ_ONLY );
    wKeyField.setEditable( true );
    wKeyField.setItems( fieldNames );
    props.setLook( wKeyField );
    wKeyField.addModifyListener( lsMod );
    FormData fdKeyField = new FormData();
    fdKeyField.left = new FormAttachment( middle, 0 );
    fdKeyField.top = new FormAttachment( 0, 3 * margin );
    fdKeyField.right = new FormAttachment( 100, 0 );
    wKeyField.setLayoutData(fdKeyField);

    // Value field
    Label wlValueField = new Label(wFields, SWT.RIGHT);
    wlValueField.setText( BaseMessages.getString( PKG, "PropertyOutputDialog.ValueField.Label" ) );
    props.setLook(wlValueField);
    FormData fdlValueField = new FormData();
    fdlValueField.left = new FormAttachment( 0, 0 );
    fdlValueField.top = new FormAttachment( wKeyField, margin );
    fdlValueField.right = new FormAttachment( middle, -margin );
    wlValueField.setLayoutData(fdlValueField);
    wValueField = new CCombo(wFields, SWT.BORDER | SWT.READ_ONLY );
    wValueField.setEditable( true );
    wValueField.setItems( fieldNames );
    props.setLook( wValueField );
    wValueField.addModifyListener( lsMod );
    FormData fdValueField = new FormData();
    fdValueField.left = new FormAttachment( middle, 0 );
    fdValueField.top = new FormAttachment( wKeyField, margin );
    fdValueField.right = new FormAttachment( 100, 0 );
    wValueField.setLayoutData(fdValueField);

    // Comment
    Label wlComment = new Label(wGeneralComp, SWT.RIGHT);
    wlComment.setText( BaseMessages.getString( PKG, "PropertyOutputDialog.Comment.Label" ) );
    props.setLook(wlComment);
    FormData fdlComment = new FormData();
    fdlComment.left = new FormAttachment( 0, 0 );
    fdlComment.top = new FormAttachment(wFields, 2 * margin );
    fdlComment.right = new FormAttachment( middle, -margin );
    wlComment.setLayoutData(fdlComment);

    wComment = new Text(wGeneralComp, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL );
    wComment.setToolTipText( BaseMessages.getString( PKG, "PropertyOutputDialog.Comment.Tooltip" ) );
    props.setLook( wComment );
    wComment.addModifyListener( lsMod );
    FormData fdComment = new FormData();
    fdComment.left = new FormAttachment( middle, 0 );
    fdComment.top = new FormAttachment(wFields, 2 * margin );
    fdComment.right = new FormAttachment( 100, 0 );
    fdComment.bottom = new FormAttachment( 100, -margin );
    wComment.setLayoutData(fdComment);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, margin );
    fdFields.top = new FormAttachment( 0, margin );
    fdFields.right = new FormAttachment( 100, -margin );
    wFields.setLayoutData(fdFields);

    // ///////////////////////////////////////////////////////////
    // / END OF Fields GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( 0, 0 );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 100, 0 );
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);
    props.setLook(wGeneralComp);

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF CONTENT TAB///
    // /
    CTabItem wContentTab = new CTabItem(wTabFolder, SWT.NONE);
    wContentTab.setText( BaseMessages.getString( PKG, "PropertyOutputDialog.ContentTab.TabTitle" ) );

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    Composite wContentComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wContentComp);
    wContentComp.setLayout( contentLayout );

    // File grouping?
    // ////////////////////////
    // START OF FileName GROUP
    //

    Group wFileName = new Group(wContentComp, SWT.SHADOW_NONE);
    props.setLook(wFileName);
    wFileName.setText( BaseMessages.getString( PKG, "PropertyOutputDialog.Group.File.Label" ) );

    FormLayout groupFileLayout = new FormLayout();
    groupFileLayout.marginWidth = 10;
    groupFileLayout.marginHeight = 10;
    wFileName.setLayout( groupFileLayout );

    // Filename line
    wlFilename = new Label(wFileName, SWT.RIGHT );
    wlFilename.setText( BaseMessages.getString( PKG, "PropertyOutputDialog.Filename.Label" ) );
    props.setLook( wlFilename );
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment(wFields, margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData(fdlFilename);

    wbFilename = new Button(wFileName, SWT.PUSH | SWT.CENTER );
    props.setLook( wbFilename );
    wbFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment(wFields, 0 );
    wbFilename.setLayoutData(fdbFilename);

    wFilename = new TextVar( variables, wFileName, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.top = new FormAttachment(wFields, margin );
    fdFilename.right = new FormAttachment( wbFilename, -margin );
    wFilename.setLayoutData(fdFilename);

    // Append checkbox
    Label wlAppend = new Label(wFileName, SWT.RIGHT);
    wlAppend.setText( BaseMessages.getString( PKG, "PropertyOutputDialog.Append.Label" ) );
    props.setLook(wlAppend);
    FormData fdlAppend = new FormData();
    fdlAppend.left = new FormAttachment( 0, 0 );
    fdlAppend.top = new FormAttachment( wFilename, margin );
    fdlAppend.right = new FormAttachment( middle, -margin );
    wlAppend.setLayoutData(fdlAppend);
    wAppend = new Button(wFileName, SWT.CHECK );
    props.setLook( wAppend );
    wAppend.setToolTipText( BaseMessages.getString( PKG, "PropertyOutputDialog.Append.Tooltip" ) );
    FormData fdAppend = new FormData();
    fdAppend.left = new FormAttachment( middle, 0 );
    fdAppend.top = new FormAttachment( wlAppend, 0, SWT.CENTER );
    fdAppend.right = new FormAttachment( 100, 0 );
    wAppend.setLayoutData(fdAppend);
    wAppend.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    } );

    // Create Parent Folder
    Label wlCreateParentFolder = new Label(wFileName, SWT.RIGHT);
    wlCreateParentFolder.setText( BaseMessages.getString( PKG, "PropertyOutputDialog.CreateParentFolder.Label" ) );
    props.setLook(wlCreateParentFolder);
    FormData fdlCreateParentFolder = new FormData();
    fdlCreateParentFolder.left = new FormAttachment( 0, 0 );
    fdlCreateParentFolder.top = new FormAttachment( wAppend, margin );
    fdlCreateParentFolder.right = new FormAttachment( middle, -margin );
    wlCreateParentFolder.setLayoutData(fdlCreateParentFolder);
    wCreateParentFolder = new Button(wFileName, SWT.CHECK );
    wCreateParentFolder.setToolTipText( BaseMessages.getString( PKG, "PropertyOutputDialog.CreateParentFolder.Tooltip" ) );
    props.setLook( wCreateParentFolder );
    FormData fdCreateParentFolder = new FormData();
    fdCreateParentFolder.left = new FormAttachment( middle, 0 );
    fdCreateParentFolder.top = new FormAttachment( wlCreateParentFolder, 0, SWT.CENTER );
    fdCreateParentFolder.right = new FormAttachment( 100, 0 );
    wCreateParentFolder.setLayoutData(fdCreateParentFolder);
    wCreateParentFolder.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // FileNameInField line
    Label wlFileNameInField = new Label(wFileName, SWT.RIGHT);
    wlFileNameInField.setText( BaseMessages.getString( PKG, "PropertyOutputDialog.FileNameInField.Label" ) );
    props.setLook(wlFileNameInField);
    FormData fdlFileNameInField = new FormData();
    fdlFileNameInField.left = new FormAttachment( 0, 0 );
    fdlFileNameInField.top = new FormAttachment( wCreateParentFolder, margin );
    fdlFileNameInField.right = new FormAttachment( middle, -margin );
    wlFileNameInField.setLayoutData(fdlFileNameInField);
    wFileNameInField = new Button(wFileName, SWT.CHECK );
    wlFileNameInField.setToolTipText( BaseMessages.getString( PKG, "PropertyOutputDialog.FileNameInField.Label" ) );
    props.setLook( wFileNameInField );
    FormData fdFileNameInField = new FormData();
    fdFileNameInField.left = new FormAttachment( middle, 0 );
    fdFileNameInField.top = new FormAttachment( wlFileNameInField, 0, SWT.CENTER );
    fdFileNameInField.right = new FormAttachment( 100, 0 );
    wFileNameInField.setLayoutData(fdFileNameInField);
    wFileNameInField.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        activeFilenameInField();
      }
    } );

    // FileNameField Line
    wlFileNameField = new Label(wFileName, SWT.RIGHT );
    wlFileNameField.setText( BaseMessages.getString( PKG, "PropertyOutputDialog.FileNameField.Label" ) );
    props.setLook( wlFileNameField );
    FormData fdlFileNameField = new FormData();
    fdlFileNameField.left = new FormAttachment( 0, 0 );
    fdlFileNameField.right = new FormAttachment( middle, -margin );
    fdlFileNameField.top = new FormAttachment( wFileNameInField, margin );
    wlFileNameField.setLayoutData(fdlFileNameField);

    wFileNameField = new ComboVar( variables, wFileName, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFileNameField );
    wFileNameField.addModifyListener( lsMod );
    FormData fdFileNameField = new FormData();
    fdFileNameField.left = new FormAttachment( middle, 0 );
    fdFileNameField.top = new FormAttachment( wFileNameInField, margin );
    fdFileNameField.right = new FormAttachment( 100, 0 );
    wFileNameField.setLayoutData(fdFileNameField);
    wFileNameField.setEnabled( false );
    wFileNameField.setItems( fieldNames );

    // Extension line
    wlExtension = new Label(wFileName, SWT.RIGHT );
    wlExtension.setText( BaseMessages.getString( PKG, "System.Label.Extension" ) );
    props.setLook( wlExtension );
    FormData fdlExtension = new FormData();
    fdlExtension.left = new FormAttachment( 0, 0 );
    fdlExtension.top = new FormAttachment( wFileNameField, margin );
    fdlExtension.right = new FormAttachment( middle, -margin );
    wlExtension.setLayoutData(fdlExtension);

    wExtension = new TextVar( variables, wFileName, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wExtension );
    wExtension.addModifyListener( lsMod );
    FormData fdExtension = new FormData();
    fdExtension.left = new FormAttachment( middle, 0 );
    fdExtension.top = new FormAttachment( wFileNameField, margin );
    fdExtension.right = new FormAttachment( 100, -margin );
    wExtension.setLayoutData(fdExtension);

    // Create multi-part file?
    wlAddTransformNr = new Label(wFileName, SWT.RIGHT );
    wlAddTransformNr.setText( BaseMessages.getString( PKG, "PropertyOutputDialog.AddTransformnr.Label" ) );
    props.setLook( wlAddTransformNr );
    FormData fdlAddTransformNr = new FormData();
    fdlAddTransformNr.left = new FormAttachment( 0, 0 );
    fdlAddTransformNr.top = new FormAttachment( wExtension, 2 * margin );
    fdlAddTransformNr.right = new FormAttachment( middle, -margin );
    wlAddTransformNr.setLayoutData(fdlAddTransformNr);
    wAddTransformNr = new Button(wFileName, SWT.CHECK );
    props.setLook( wAddTransformNr );
    FormData fdAddTransformNr = new FormData();
    fdAddTransformNr.left = new FormAttachment( middle, 0 );
    fdAddTransformNr.top = new FormAttachment( wlAddTransformNr, 0, SWT.CENTER );
    fdAddTransformNr.right = new FormAttachment( 100, 0 );
    wAddTransformNr.setLayoutData(fdAddTransformNr);
    wAddTransformNr.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Create multi-part file?
    wlAddDate = new Label(wFileName, SWT.RIGHT );
    wlAddDate.setText( BaseMessages.getString( PKG, "PropertyOutputDialog.AddDate.Label" ) );
    props.setLook( wlAddDate );
    FormData fdlAddDate = new FormData();
    fdlAddDate.left = new FormAttachment( 0, 0 );
    fdlAddDate.top = new FormAttachment( wAddTransformNr, margin );
    fdlAddDate.right = new FormAttachment( middle, -margin );
    wlAddDate.setLayoutData(fdlAddDate);
    wAddDate = new Button(wFileName, SWT.CHECK );
    props.setLook( wAddDate );
    FormData fdAddDate = new FormData();
    fdAddDate.left = new FormAttachment( middle, 0 );
    fdAddDate.top = new FormAttachment( wlAddDate, 0, SWT.CENTER );
    fdAddDate.right = new FormAttachment( 100, 0 );
    wAddDate.setLayoutData(fdAddDate);
    wAddDate.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );
    // Create multi-part file?
    wlAddTime = new Label(wFileName, SWT.RIGHT );
    wlAddTime.setText( BaseMessages.getString( PKG, "PropertyOutputDialog.AddTime.Label" ) );
    props.setLook( wlAddTime );
    FormData fdlAddTime = new FormData();
    fdlAddTime.left = new FormAttachment( 0, 0 );
    fdlAddTime.top = new FormAttachment( wAddDate, margin );
    fdlAddTime.right = new FormAttachment( middle, -margin );
    wlAddTime.setLayoutData(fdlAddTime);
    wAddTime = new Button(wFileName, SWT.CHECK );
    props.setLook( wAddTime );
    FormData fdAddTime = new FormData();
    fdAddTime.left = new FormAttachment( middle, 0 );
    fdAddTime.top = new FormAttachment( wlAddTime, 0, SWT.CENTER );
    fdAddTime.right = new FormAttachment( 100, 0 );
    wAddTime.setLayoutData(fdAddTime);
    wAddTime.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    wbShowFiles = new Button(wFileName, SWT.PUSH | SWT.CENTER );
    props.setLook( wbShowFiles );
    wbShowFiles.setText( BaseMessages.getString( PKG, "PropertyOutputDialog.ShowFiles.Button" ) );
    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment( middle, 0 );
    fdbShowFiles.top = new FormAttachment( wAddTime, margin * 2 );
    wbShowFiles.setLayoutData(fdbShowFiles);
    wbShowFiles.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        PropertyOutputMeta tfoi = new PropertyOutputMeta();
        getInfo( tfoi );
        String[] files = tfoi.getFiles( variables );
        if ( files != null && files.length > 0 ) {
          EnterSelectionDialog esd = new EnterSelectionDialog( shell, files,
            BaseMessages.getString( PKG, "PropertyOutputDialog.SelectOutputFiles.DialogTitle" ),
            BaseMessages.getString( PKG, "PropertyOutputDialog.SelectOutputFiles.DialogMessage" ) );
          esd.setViewOnly();
          esd.open();
        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
          mb.setMessage( BaseMessages.getString( PKG, "PropertyOutputDialog.NoFilesFound.DialogMessage" ) );
          mb.setText( BaseMessages.getString( PKG, "System.DialogTitle.Error" ) );
          mb.open();
        }
      }
    } );

    FormData fdFileName = new FormData();
    fdFileName.left = new FormAttachment( 0, margin );
    fdFileName.top = new FormAttachment(wFields, margin );
    fdFileName.right = new FormAttachment( 100, -margin );
    wFileName.setLayoutData(fdFileName);

    // ///////////////////////////////////////////////////////////
    // / END OF FileName GROUP
    // ///////////////////////////////////////////////////////////

    // File grouping?
    // ////////////////////////
    // START OF ResultFile GROUP
    //

    Group wResultFile = new Group(wContentComp, SWT.SHADOW_NONE);
    props.setLook(wResultFile);
    wResultFile.setText( BaseMessages.getString( PKG, "PropertyOutputDialog.Group.ResultFile.Label" ) );

    FormLayout groupResultFile = new FormLayout();
    groupResultFile.marginWidth = 10;
    groupResultFile.marginHeight = 10;
    wResultFile.setLayout( groupResultFile );

    // Add File to the result files name
    Label wlAddToResult = new Label(wResultFile, SWT.RIGHT);
    wlAddToResult.setText( BaseMessages.getString( PKG, "PropertyOutputDialog.AddFileToResult.Label" ) );
    props.setLook(wlAddToResult);
    FormData fdlAddToResult = new FormData();
    fdlAddToResult.left = new FormAttachment( 0, 0 );
    fdlAddToResult.top = new FormAttachment(wFileName, margin );
    fdlAddToResult.right = new FormAttachment( middle, -margin );
    wlAddToResult.setLayoutData(fdlAddToResult);
    wAddToResult = new Button(wResultFile, SWT.CHECK );
    wAddToResult.setToolTipText( BaseMessages.getString( PKG, "PropertyOutputDialog.AddFileToResult.Tooltip" ) );
    props.setLook( wAddToResult );
    FormData fdAddToResult = new FormData();
    fdAddToResult.left = new FormAttachment( middle, 0 );
    fdAddToResult.top = new FormAttachment( wlAddToResult, 0, SWT.CENTER );
    fdAddToResult.right = new FormAttachment( 100, 0 );
    wAddToResult.setLayoutData(fdAddToResult);
    SelectionAdapter lsSelAR = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };
    wAddToResult.addSelectionListener( lsSelAR );

    FormData fdResultFile = new FormData();
    fdResultFile.left = new FormAttachment( 0, margin );
    fdResultFile.top = new FormAttachment(wFileName, margin );
    fdResultFile.right = new FormAttachment( 100, -margin );
    wResultFile.setLayoutData(fdResultFile);

    // ///////////////////////////////////////////////////////////
    // / END OF ResultFile GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdContentComp = new FormData();
    fdContentComp.left = new FormAttachment( 0, 0 );
    fdContentComp.top = new FormAttachment( 0, 0 );
    fdContentComp.right = new FormAttachment( 100, 0 );
    fdContentComp.bottom = new FormAttachment( 100, 0 );
    wContentComp.setLayoutData(wContentComp);

    wContentComp.layout();
    wContentTab.setControl(wContentComp);

    // ///////////////////////////////////////////////////////////
    // / END OF CONTENT TAB
    // ///////////////////////////////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wTransformName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOk, -2*margin );
    wTabFolder.setLayoutData(fdTabFolder);

    // Add listeners

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );

    wbFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        FileDialog dialog = new FileDialog( shell, SWT.SAVE );
        dialog.setFilterExtensions( new String[] { "*.txt", "*.TXT", "*" } );
        if ( wFilename.getText() != null ) {
          dialog.setFileName( variables.resolve( wFilename.getText() ) );
        }
        dialog.setFilterNames( new String[] {
          BaseMessages.getString( PKG, "System.FileType.TextFiles" ),
          BaseMessages.getString( PKG, "System.FileType.CSVFiles" ),
          BaseMessages.getString( PKG, "System.FileType.AllFiles" ) } );
        if ( dialog.open() != null ) {
          String extension = wExtension.getText();
          if ( extension != null
            && dialog.getFileName() != null && dialog.getFileName().endsWith( "." + extension ) ) {
            // The extension is filled in and matches the end
            // of the selected file => Strip off the extension.
            String fileName = dialog.getFileName();
            wFilename.setText( dialog.getFilterPath()
              + System.getProperty( "file.separator" )
              + fileName.substring( 0, fileName.length() - ( extension.length() + 1 ) ) );
          } else {
            wFilename.setText( dialog.getFilterPath()
              + System.getProperty( "file.separator" ) + dialog.getFileName() );
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

    lsResize = event -> {
      // Point size = shell.getSize();

    };
    shell.addListener( SWT.Resize, lsResize );

    wTabFolder.setSelection( 0 );

    // Set the shell size, based upon previous time...
    setSize();
    getData();
    activeFilenameInField();

    input.setChanged( changed ); // backupChanged);

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void activeFilenameInField() {
    wlFileNameField.setEnabled( wFileNameInField.getSelection() );
    wFileNameField.setEnabled( wFileNameInField.getSelection() );
    wlFilename.setEnabled( !wFileNameInField.getSelection() );
    wFilename.setEnabled( !wFileNameInField.getSelection() );
    wbFilename.setEnabled( !wFileNameInField.getSelection() );
    wlExtension.setEnabled( !wFileNameInField.getSelection() );
    wExtension.setEnabled( !wFileNameInField.getSelection() );
    wlAddDate.setEnabled( !wFileNameInField.getSelection() );
    wAddDate.setEnabled( !wFileNameInField.getSelection() );
    wlAddTransformNr.setEnabled( !wFileNameInField.getSelection() );
    wAddTransformNr.setEnabled( !wFileNameInField.getSelection() );
    wlAddTime.setEnabled( !wFileNameInField.getSelection() );
    wAddTime.setEnabled( !wFileNameInField.getSelection() );
    wbShowFiles.setEnabled( !wFileNameInField.getSelection() );
  }

  private void getFields() {
    if ( !gotPreviousFields ) {
      try {
        IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
        if ( r != null ) {
          fieldNames = r.getFieldNames();
        }
      } catch ( HopException ke ) {
        new ErrorDialog(
          shell, BaseMessages.getString( PKG, "PropertyOutputDialog.FailedToGetFields.DialogTitle" ),
          BaseMessages.getString( PKG, "PropertyOutputDialog.FailedToGetFields.DialogMessage" ), ke );
      }
      gotPreviousFields = true;
    }
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( input.getKeyField() != null ) {
      wKeyField.setText( input.getKeyField() );
    }
    if ( input.getValueField() != null ) {
      wValueField.setText( input.getValueField() );
    }

    if ( input.getFileName() != null ) {
      wFilename.setText( input.getFileName() );
    }
    wFileNameInField.setSelection( input.isFileNameInField() );
    if ( input.getFileNameField() != null ) {
      wFileNameField.setText( input.getFileNameField() );
    }
    wCreateParentFolder.setSelection( input.isCreateParentFolder() );
    if ( input.getExtension() != null ) {
      wExtension.setText( input.getExtension() );
    } else {
      wExtension.setText( "properties" );
    }

    wAddDate.setSelection( input.isDateInFilename() );
    wAddTime.setSelection( input.isTimeInFilename() );
    wAddTransformNr.setSelection( input.isTransformNrInFilename() );

    wAddToResult.setSelection( input.addToResult() );
    wAppend.setSelection( input.isAppend() );

    if ( input.getComment() != null ) {
      wComment.setText( input.getComment() );
    }

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( backupChanged );
    dispose();
  }

  private void getInfo( PropertyOutputMeta info ) {
    info.setKeyField( wKeyField.getText() );
    info.setValueField( wValueField.getText() );
    info.setCreateParentFolder( wCreateParentFolder.getSelection() );
    info.setAppend( wAppend.getSelection() );
    info.setFileName( wFilename.getText() );
    info.setExtension( wExtension.getText() );
    info.setTransformNrInFilename( wAddTransformNr.getSelection() );
    info.setDateInFilename( wAddDate.getSelection() );
    info.setTimeInFilename( wAddTime.getSelection() );
    info.setFileNameField( wFileNameField.getText() );
    info.setFileNameInField( wFileNameInField.getSelection() );
    info.setAddToResult( wAddToResult.getSelection() );

    info.setComment( wComment.getText() );
  }

  private void ok() {
    transformName = wTransformName.getText(); // return value

    getInfo( input );

    dispose();
  }

}
