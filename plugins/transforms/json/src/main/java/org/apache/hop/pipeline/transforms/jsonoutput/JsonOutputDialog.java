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

package org.apache.hop.pipeline.transforms.jsonoutput;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.nio.charset.Charset;
import java.util.List;
import java.util.*;

public class JsonOutputDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = JsonOutputMeta.class; // For Translator

  private Label wlEncoding;
  private ComboVar wEncoding;

  private Label wlOutputValue;
  private TextVar wOutputValue;

  private Button wCompatibilityMode;

  private TextVar wBlocName;

  private TextVar wNrRowsInBloc;

  private TableView wFields;

  private final JsonOutputMeta input;

  private boolean gotEncodings = false;

  private ColumnInfo[] colinf;

  private Label wlAddToResult;
  private Button wAddToResult;

  private Label wlFilename;
  private Button wbFilename;
  private TextVar wFilename;

  private Label wlExtension;
  private TextVar wExtension;

  private Label wlServletOutput;
  private Button wServletOutput;

  private Label wlCreateParentFolder;
  private Button wCreateParentFolder;

  private Label wlDoNotOpenNewFileInit;
  private Button wDoNotOpenNewFileInit;

  private Label wlAddDate;
  private Button wAddDate;

  private Label wlAddTime;
  private Button wAddTime;

  private Button wbShowFiles;

  private Label wlAppend;
  private Button wAppend;

  private CCombo wOperation;

  private final Map<String, Integer> inputFields;

  public JsonOutputDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (JsonOutputMeta) in;
    inputFields = new HashMap<>();
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
    shell.setText( BaseMessages.getString( PKG, "JsonOutputDialog.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

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

    // Buttons at the bottom
    //
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e->ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e->cancel() );
    setButtonPositions( new Button[] { wOk, wCancel }, margin, null);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF General TAB///
    // /
    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText( BaseMessages.getString( PKG, "JsonOutputDialog.GeneralTab.TabTitle" ) );

    FormLayout GeneralLayout = new FormLayout();
    GeneralLayout.marginWidth = 3;
    GeneralLayout.marginHeight = 3;

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE );
    props.setLook( wGeneralComp );
    wGeneralComp.setLayout( GeneralLayout );

    // Operation
    Label wlOperation = new Label(wGeneralComp, SWT.RIGHT);
    wlOperation.setText( BaseMessages.getString( PKG, "JsonOutputDialog.Operation.Label" ) );
    props.setLook(wlOperation);
    FormData fdlOperation = new FormData();
    fdlOperation.left = new FormAttachment( 0, 0 );
    fdlOperation.right = new FormAttachment( middle, -margin );
    fdlOperation.top = new FormAttachment( wNrRowsInBloc, margin );
    wlOperation.setLayoutData(fdlOperation);

    wOperation = new CCombo( wGeneralComp, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wOperation );
    wOperation.addModifyListener( lsMod );
    FormData fdOperation = new FormData();
    fdOperation.left = new FormAttachment( middle, 0 );
    fdOperation.top = new FormAttachment( wNrRowsInBloc, margin );
    fdOperation.right = new FormAttachment( 100, -margin );
    wOperation.setLayoutData(fdOperation);
    wOperation.setItems( JsonOutputMeta.operationTypeDesc );
    wOperation.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        updateOperation();

      }
    } );

    // Connection grouping?
    // ////////////////////////
    // START OF Settings GROUP
    //

    Group wSettings = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wSettings);
    wSettings.setText( BaseMessages.getString( PKG, "JsonOutputDialog.Group.Settings.Label" ) );

    FormLayout groupFileLayout = new FormLayout();
    groupFileLayout.marginWidth = 10;
    groupFileLayout.marginHeight = 10;
    wSettings.setLayout( groupFileLayout );

    Label wlBlocName = new Label(wSettings, SWT.RIGHT);
    wlBlocName.setText( BaseMessages.getString( PKG, "JsonOutputDialog.BlocName.Label" ) );
    props.setLook(wlBlocName);
    FormData fdlBlocName = new FormData();
    fdlBlocName.left = new FormAttachment( 0, 0 );
    fdlBlocName.top = new FormAttachment( wOperation, margin );
    fdlBlocName.right = new FormAttachment( middle, -margin );
    wlBlocName.setLayoutData(fdlBlocName);
    wBlocName = new TextVar( variables, wSettings, SWT.BORDER | SWT.READ_ONLY );
    wBlocName.setEditable( true );
    props.setLook( wBlocName );
    wBlocName.addModifyListener( lsMod );
    FormData fdBlocName = new FormData();
    fdBlocName.left = new FormAttachment( middle, 0 );
    fdBlocName.top = new FormAttachment( wOperation, margin );
    fdBlocName.right = new FormAttachment( 100, 0 );
    wBlocName.setLayoutData(fdBlocName);

    Label wlNrRowsInBloc = new Label(wSettings, SWT.RIGHT);
    wlNrRowsInBloc.setText( BaseMessages.getString( PKG, "JsonOutputDialog.NrRowsInBloc.Label" ) );
    props.setLook(wlNrRowsInBloc);
    FormData fdlNrRowsInBloc = new FormData();
    fdlNrRowsInBloc.left = new FormAttachment( 0, 0 );
    fdlNrRowsInBloc.top = new FormAttachment( wBlocName, margin );
    fdlNrRowsInBloc.right = new FormAttachment( middle, -margin );
    wlNrRowsInBloc.setLayoutData(fdlNrRowsInBloc);
    wNrRowsInBloc = new TextVar( variables, wSettings, SWT.BORDER | SWT.READ_ONLY );
    wNrRowsInBloc.setToolTipText( BaseMessages.getString( PKG, "JsonOutputDialog.NrRowsInBloc.ToolTip" ) );
    wNrRowsInBloc.setEditable( true );
    props.setLook( wNrRowsInBloc );
    wNrRowsInBloc.addModifyListener( lsMod );
    FormData fdNrRowsInBloc = new FormData();
    fdNrRowsInBloc.left = new FormAttachment( middle, 0 );
    fdNrRowsInBloc.top = new FormAttachment( wBlocName, margin );
    fdNrRowsInBloc.right = new FormAttachment( 100, 0 );
    wNrRowsInBloc.setLayoutData(fdNrRowsInBloc);

    wlOutputValue = new Label(wSettings, SWT.RIGHT );
    wlOutputValue.setText( BaseMessages.getString( PKG, "JsonOutputDialog.OutputValue.Label" ) );
    props.setLook( wlOutputValue );
    FormData fdlOutputValue = new FormData();
    fdlOutputValue.left = new FormAttachment( 0, 0 );
    fdlOutputValue.top = new FormAttachment( wNrRowsInBloc, margin );
    fdlOutputValue.right = new FormAttachment( middle, -margin );
    wlOutputValue.setLayoutData(fdlOutputValue);
    wOutputValue = new TextVar( variables, wSettings, SWT.BORDER | SWT.READ_ONLY );
    wOutputValue.setEditable( true );
    props.setLook( wOutputValue );
    wOutputValue.addModifyListener( lsMod );
    FormData fdOutputValue = new FormData();
    fdOutputValue.left = new FormAttachment( middle, 0 );
    fdOutputValue.top = new FormAttachment( wNrRowsInBloc, margin );
    fdOutputValue.right = new FormAttachment( 100, 0 );
    wOutputValue.setLayoutData(fdOutputValue);

    // ////////////////////////// start of compatibility mode
    Label wlCompatibilityMode = new Label(wSettings, SWT.RIGHT);
    wlCompatibilityMode.setText( BaseMessages.getString( PKG, "JsonOutputDialog.CompatibilityMode.Label" ) );
    props.setLook(wlCompatibilityMode);
    FormData fdlCompatibilityMode = new FormData();
    fdlCompatibilityMode.left = new FormAttachment( 0, 0 );
    fdlCompatibilityMode.top = new FormAttachment( wOutputValue, margin );
    fdlCompatibilityMode.right = new FormAttachment( middle, -margin );
    wlCompatibilityMode.setLayoutData(fdlCompatibilityMode);
    wCompatibilityMode = new Button(wSettings, SWT.CHECK );
    wCompatibilityMode.setToolTipText( BaseMessages.getString( PKG, "JsonOutputDialog.CompatibilityMode.Tooltip" ) );
    props.setLook( wCompatibilityMode );
    FormData fdCompatibilityMode = new FormData();
    fdCompatibilityMode.left = new FormAttachment( middle, 0 );
    fdCompatibilityMode.top = new FormAttachment( wlCompatibilityMode, 0, SWT.CENTER );
    fdCompatibilityMode.right = new FormAttachment( 100, 0 );
    wCompatibilityMode.setLayoutData(fdCompatibilityMode);
    wCompatibilityMode.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    FormData fdSettings = new FormData();
    fdSettings.left = new FormAttachment( 0, margin );
    fdSettings.top = new FormAttachment( wOperation, 2 * margin );
    fdSettings.right = new FormAttachment( 100, -margin );
    wSettings.setLayoutData(fdSettings);

    // ///////////////////////////////////////////////////////////
    // / END OF Settings GROUP
    // ///////////////////////////////////////////////////////////

    // Connection grouping?
    // ////////////////////////
    // START OF FileName GROUP
    //

    Group wFileName = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wFileName);
    wFileName.setText( BaseMessages.getString( PKG, "JsonOutputDialog.Group.File.Label" ) );

    FormLayout groupfilenameayout = new FormLayout();
    groupfilenameayout.marginWidth = 10;
    groupfilenameayout.marginHeight = 10;
    wFileName.setLayout( groupfilenameayout );

    // Filename line
    wlFilename = new Label(wFileName, SWT.RIGHT );
    wlFilename.setText( BaseMessages.getString( PKG, "JsonOutputDialog.Filename.Label" ) );
    props.setLook( wlFilename );
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment(wSettings, margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData(fdlFilename);

    wbFilename = new Button(wFileName, SWT.PUSH | SWT.CENTER );
    props.setLook( wbFilename );
    wbFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment(wSettings, 0 );
    wbFilename.setLayoutData(fdbFilename);


    wbFilename.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( true, shell, wFilename, variables,
            new String[] {"*.js", "*.json", "*"},
            new String[] {
                    BaseMessages.getString( PKG, "System.FileType.JsonFiles" ),
                    BaseMessages.getString( PKG, "System.FileType.AllFiles" ) },
            true )
    );

    wFilename = new TextVar( variables, wFileName, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.top = new FormAttachment( wOutputValue, margin );
    fdFilename.right = new FormAttachment( wbFilename, -margin );
    wFilename.setLayoutData(fdFilename);

    // Append to end of file?
    wlAppend = new Label(wFileName, SWT.RIGHT );
    wlAppend.setText( BaseMessages.getString( PKG, "JsonOutputDialog.Append.Label" ) );
    props.setLook( wlAppend );
    FormData fdlAppend = new FormData();
    fdlAppend.left = new FormAttachment( 0, 0 );
    fdlAppend.top = new FormAttachment( wFilename, margin );
    fdlAppend.right = new FormAttachment( middle, -margin );
    wlAppend.setLayoutData(fdlAppend);
    wAppend = new Button(wFileName, SWT.CHECK );
    wAppend.setToolTipText( BaseMessages.getString( PKG, "JsonOutputDialog.Append.Tooltip" ) );
    props.setLook( wAppend );
    FormData fdAppend = new FormData();
    fdAppend.left = new FormAttachment( middle, 0 );
    fdAppend.top = new FormAttachment( wlAppend, 0, SWT.CENTER );
    fdAppend.right = new FormAttachment( 100, 0 );
    wAppend.setLayoutData(fdAppend);
    wAppend.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Create Parent Folder
    wlCreateParentFolder = new Label(wFileName, SWT.RIGHT );
    wlCreateParentFolder.setText( BaseMessages.getString( PKG, "JsonOutputDialog.CreateParentFolder.Label" ) );
    props.setLook( wlCreateParentFolder );
    FormData fdlCreateParentFolder = new FormData();
    fdlCreateParentFolder.left = new FormAttachment( 0, 0 );
    fdlCreateParentFolder.top = new FormAttachment( wAppend, margin );
    fdlCreateParentFolder.right = new FormAttachment( middle, -margin );
    wlCreateParentFolder.setLayoutData(fdlCreateParentFolder);
    wCreateParentFolder = new Button(wFileName, SWT.CHECK );
    wCreateParentFolder.setToolTipText( BaseMessages.getString( PKG, "JsonOutputDialog.CreateParentFolder.Tooltip" ) );
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

    // Open new File at Init
    wlDoNotOpenNewFileInit = new Label(wFileName, SWT.RIGHT );
    wlDoNotOpenNewFileInit.setText( BaseMessages.getString( PKG, "JsonOutputDialog.DoNotOpenNewFileInit.Label" ) );
    props.setLook( wlDoNotOpenNewFileInit );
    FormData fdlDoNotOpenNewFileInit = new FormData();
    fdlDoNotOpenNewFileInit.left = new FormAttachment( 0, 0 );
    fdlDoNotOpenNewFileInit.top = new FormAttachment( wCreateParentFolder, margin );
    fdlDoNotOpenNewFileInit.right = new FormAttachment( middle, -margin );
    wlDoNotOpenNewFileInit.setLayoutData(fdlDoNotOpenNewFileInit);
    wDoNotOpenNewFileInit = new Button(wFileName, SWT.CHECK );
    wDoNotOpenNewFileInit
        .setToolTipText( BaseMessages.getString( PKG, "JsonOutputDialog.DoNotOpenNewFileInit.Tooltip" ) );
    props.setLook( wDoNotOpenNewFileInit );
    FormData fdDoNotOpenNewFileInit = new FormData();
    fdDoNotOpenNewFileInit.left = new FormAttachment( middle, 0 );
    fdDoNotOpenNewFileInit.top = new FormAttachment( wlDoNotOpenNewFileInit, 0, SWT.CENTER );
    fdDoNotOpenNewFileInit.right = new FormAttachment( 100, 0 );
    wDoNotOpenNewFileInit.setLayoutData(fdDoNotOpenNewFileInit);
    wDoNotOpenNewFileInit.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Extension line
    wlExtension = new Label(wFileName, SWT.RIGHT );
    wlExtension.setText( BaseMessages.getString( PKG, "System.Label.Extension" ) );
    props.setLook( wlExtension );
    FormData fdlExtension = new FormData();
    fdlExtension.left = new FormAttachment( 0, 0 );
    fdlExtension.top = new FormAttachment( wDoNotOpenNewFileInit, margin );
    fdlExtension.right = new FormAttachment( middle, -margin );
    wlExtension.setLayoutData(fdlExtension);

    wExtension = new TextVar( variables, wFileName, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wExtension );
    wExtension.addModifyListener( lsMod );
    FormData fdExtension = new FormData();
    fdExtension.left = new FormAttachment( middle, 0 );
    fdExtension.top = new FormAttachment( wDoNotOpenNewFileInit, margin );
    fdExtension.right = new FormAttachment( 100, -margin );
    wExtension.setLayoutData(fdExtension);

    wlEncoding = new Label(wFileName, SWT.RIGHT );
    wlEncoding.setText( BaseMessages.getString( PKG, "JsonOutputDialog.Encoding.Label" ) );
    props.setLook( wlEncoding );
    FormData fdlEncoding = new FormData();
    fdlEncoding.left = new FormAttachment( 0, 0 );
    fdlEncoding.top = new FormAttachment( wExtension, margin );
    fdlEncoding.right = new FormAttachment( middle, -margin );
    wlEncoding.setLayoutData(fdlEncoding);
    wEncoding = new ComboVar( variables, wFileName, SWT.BORDER | SWT.READ_ONLY );
    wEncoding.setEditable( true );
    props.setLook( wEncoding );
    wEncoding.addModifyListener( lsMod );
    FormData fdEncoding = new FormData();
    fdEncoding.left = new FormAttachment( middle, 0 );
    fdEncoding.top = new FormAttachment( wExtension, margin );
    fdEncoding.right = new FormAttachment( 100, 0 );
    wEncoding.setLayoutData(fdEncoding);
    wEncoding.addFocusListener( new FocusListener() {
      public void focusLost( FocusEvent e ) {
      }

      public void focusGained( FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        setEncodings();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    // Output to servlet (browser, ws)
    //
    wlServletOutput = new Label(wFileName, SWT.RIGHT );
    wlServletOutput.setText( BaseMessages.getString( PKG, "JsonOutputDialog.ServletOutput.Label" ) );
    props.setLook( wlServletOutput );
    FormData fdlServletOutput = new FormData();
    fdlServletOutput.left = new FormAttachment( 0, 0 );
    fdlServletOutput.top = new FormAttachment( wEncoding, margin );
    fdlServletOutput.right = new FormAttachment( middle, -margin );
    wlServletOutput.setLayoutData(fdlServletOutput);
    wServletOutput = new Button(wFileName, SWT.CHECK );
    wServletOutput.setToolTipText( BaseMessages.getString( PKG, "JsonOutputDialog.ServletOutput.Tooltip" ) );
    props.setLook( wServletOutput );
    FormData fdServletOutput = new FormData();
    fdServletOutput.left = new FormAttachment( middle, 0 );
    fdServletOutput.top = new FormAttachment( wlServletOutput, 0, SWT.CENTER );
    fdServletOutput.right = new FormAttachment( 100, 0 );
    wServletOutput.setLayoutData(fdServletOutput);
    wServletOutput.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        setFlagsServletOption();
      }
    } );

    // Create multi-part file?
    wlAddDate = new Label(wFileName, SWT.RIGHT );
    wlAddDate.setText( BaseMessages.getString( PKG, "JsonOutputDialog.AddDate.Label" ) );
    props.setLook( wlAddDate );
    FormData fdlAddDate = new FormData();
    fdlAddDate.left = new FormAttachment( 0, 0 );
    fdlAddDate.top = new FormAttachment( wServletOutput, margin );
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
    wlAddTime.setText( BaseMessages.getString( PKG, "JsonOutputDialog.AddTime.Label" ) );
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
    wbShowFiles.setText( BaseMessages.getString( PKG, "JsonOutputDialog.ShowFiles.Button" ) );
    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment( middle, 0 );
    fdbShowFiles.top = new FormAttachment( wAddTime, margin * 2 );
    wbShowFiles.setLayoutData(fdbShowFiles);
    wbShowFiles.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        JsonOutputMeta tfoi = new JsonOutputMeta();
        getInfo( tfoi );
        String[] files = tfoi.getFiles( variables );
        if ( files != null && files.length > 0 ) {
          EnterSelectionDialog esd =
              new EnterSelectionDialog( shell, files, BaseMessages.getString( PKG,
                  "JsonOutputDialog.SelectOutputFiles.DialogTitle" ), BaseMessages.getString( PKG,
                  "JsonOutputDialog.SelectOutputFiles.DialogMessage" ) );
          esd.setViewOnly();
          esd.open();
        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
          mb.setMessage( BaseMessages.getString( PKG, "JsonOutputDialog.NoFilesFound.DialogMessage" ) );
          mb.setText( BaseMessages.getString( PKG, "System.DialogTitle.Error" ) );
          mb.open();
        }
      }
    } );

    // Add File to the result files name
    wlAddToResult = new Label(wFileName, SWT.RIGHT );
    wlAddToResult.setText( BaseMessages.getString( PKG, "JsonOutputDialog.AddFileToResult.Label" ) );
    props.setLook( wlAddToResult );
    FormData fdlAddToResult = new FormData();
    fdlAddToResult.left = new FormAttachment( 0, 0 );
    fdlAddToResult.top = new FormAttachment( wbShowFiles, margin );
    fdlAddToResult.right = new FormAttachment( middle, -margin );
    wlAddToResult.setLayoutData(fdlAddToResult);
    wAddToResult = new Button(wFileName, SWT.CHECK );
    wAddToResult.setToolTipText( BaseMessages.getString( PKG, "JsonOutputDialog.AddFileToResult.Tooltip" ) );
    props.setLook( wAddToResult );
    FormData fdAddToResult = new FormData();
    fdAddToResult.left = new FormAttachment( middle, 0 );
    fdAddToResult.top = new FormAttachment( wlAddToResult, 0, SWT.CENTER );
    fdAddToResult.right = new FormAttachment( 100, 0 );
    wAddToResult.setLayoutData(fdAddToResult);
    SelectionAdapter lsSelR = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };
    wAddToResult.addSelectionListener( lsSelR );

    FormData fdFileName = new FormData();
    fdFileName.left = new FormAttachment( 0, margin );
    fdFileName.top = new FormAttachment(wSettings, 2 * margin );
    fdFileName.right = new FormAttachment( 100, -margin );
    wFileName.setLayoutData(fdFileName);

    // ///////////////////////////////////////////////////////////
    // / END OF FileName GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( wTransformName, margin );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 100, 0 );
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl( wGeneralComp );

    // ///////////////////////////////////////////////////////////
    // / END OF General TAB
    // ///////////////////////////////////////////////////////////

    // Fields tab...
    //
    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setText( BaseMessages.getString( PKG, "JsonOutputDialog.FieldsTab.TabTitle" ) );

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE );
    wFieldsComp.setLayout( fieldsLayout );
    props.setLook( wFieldsComp );

    wGet = new Button( wFieldsComp, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "JsonOutputDialog.Get.Button" ) );
    wGet.setToolTipText( BaseMessages.getString( PKG, "JsonOutputDialog.Get.Tooltip" ) );

    setButtonPositions( new Button[] { wGet }, margin, null );

    final int FieldsRows = input.getOutputFields().length;

    colinf =
        new ColumnInfo[] {
          new ColumnInfo( BaseMessages.getString( PKG, "JsonOutputDialog.Fieldname.Column" ),
              ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false ),
          new ColumnInfo( BaseMessages.getString( PKG, "JsonOutputDialog.ElementName.Column" ),
              ColumnInfo.COLUMN_TYPE_TEXT, false ), };
    colinf[1].setUsingVariables( true );
    wFields =
        new TableView( variables, wFieldsComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod,
            props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( 0, 0 );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wGet, -margin );
    wFields.setLayoutData(fdFields);

    //
    // Search the fields in the background

    final Runnable runnable = () -> {
      TransformMeta transformMeta = pipelineMeta.findTransform( transformName );
      if ( transformMeta != null ) {
        try {
          IRowMeta row = pipelineMeta.getPrevTransformFields( variables, transformMeta );

          // Remember these fields...
          for ( int i = 0; i < row.size(); i++ ) {
            inputFields.put( row.getValueMeta( i ).getName(), i);
          }
          setComboBoxes();
        } catch ( HopException e ) {
          logError( BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Message" ) );
        }
      }
    };
    new Thread( runnable ).start();

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment( 0, 0 );
    fdFieldsComp.top = new FormAttachment( 0, 0 );
    fdFieldsComp.right = new FormAttachment( 100, 0 );
    fdFieldsComp.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData(fdFieldsComp);

    wFieldsComp.layout();
    wFieldsTab.setControl( wFieldsComp );

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wTransformName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOk, -2*margin );
    wTabFolder.setLayoutData(fdTabFolder);

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    wGet.addListener( SWT.Selection, e -> get() );

    wTransformName.addSelectionListener( lsDef );
    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    lsResize = event -> {
      Point size = shell.getSize();
      wFields.setSize( size.x - 10, size.y - 50 );
      wFields.table.setSize( size.x - 10, size.y - 50 );
      wFields.redraw();
    };
    shell.addListener( SWT.Resize, lsResize );

    wTabFolder.setSelection( 0 );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    updateOperation();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  protected void setFlagsServletOption() {
    boolean enableFilename = !wServletOutput.getSelection();
    wlFilename.setEnabled( enableFilename );
    wFilename.setEnabled( enableFilename );
    wlDoNotOpenNewFileInit.setEnabled( enableFilename );
    wDoNotOpenNewFileInit.setEnabled( enableFilename );
    wlCreateParentFolder.setEnabled( enableFilename );
    wCreateParentFolder.setEnabled( enableFilename );
    wlExtension.setEnabled( enableFilename );
    wExtension.setEnabled( enableFilename );
    wlAddDate.setEnabled( enableFilename );
    wAddDate.setEnabled( enableFilename );
    wlAddTime.setEnabled( enableFilename );
    wAddTime.setEnabled( enableFilename );
    wlAppend.setEnabled( enableFilename );
    wAppend.setEnabled( enableFilename );
    wbShowFiles.setEnabled( enableFilename );
    wlAddToResult.setEnabled( enableFilename );
    wAddToResult.setEnabled( enableFilename );
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<>();

    // Add the currentMeta fields...
    fields.putAll( inputFields );

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>(keySet);

    String[] fieldNames = entries.toArray( new String[entries.size()] );

    Const.sortStrings( fieldNames );
    colinf[0].setComboValues( fieldNames );
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
      } else {
        wEncoding.select( Const.indexOfString( "UTF-8", wEncoding.getItems() ) );
      }
    }
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wBlocName.setText( Const.NVL( input.getJsonBloc(), "" ) );
    wNrRowsInBloc.setText( Const.NVL( input.getNrRowsInBloc(), "" ) );
    wEncoding.setText( Const.NVL( input.getEncoding(), "" ) );
    wOutputValue.setText( Const.NVL( input.getOutputValue(), "" ) );
    wCompatibilityMode.setSelection( input.isCompatibilityMode() );
    wOperation.setText( JsonOutputMeta.getOperationTypeDesc( input.getOperationType() ) );
    wFilename.setText( Const.NVL( input.getFileName(), "" ) );
    wCreateParentFolder.setSelection( input.isCreateParentFolder() );
    wExtension.setText( Const.NVL( input.getExtension(), "js" ) );
    setFlagsServletOption();

    wAddDate.setSelection( input.isDateInFilename() );
    wAddTime.setSelection( input.isTimeInFilename() );
    wAppend.setSelection( input.isFileAppended() );

    wEncoding.setText( Const.NVL( input.getEncoding(), "" ) );
    wAddToResult.setSelection( input.AddToResult() );
    wDoNotOpenNewFileInit.setSelection( input.isDoNotOpenNewFileInit() );

    if ( isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "JsonOutputDialog.Log.GettingFieldsInfo" ) );
    }

    for ( int i = 0; i < input.getOutputFields().length; i++ ) {
      JsonOutputField field = input.getOutputFields()[i];

      TableItem item = wFields.table.getItem( i );
      item.setText( 1, Const.NVL( field.getFieldName(), "" ) );
      item.setText( 2, Const.NVL( field.getElementName(), "" ) );
    }

    wFields.optWidth( true );
    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;

    input.setChanged( backupChanged );

    dispose();
  }

  private void getInfo( JsonOutputMeta jsometa ) {
    jsometa.setJsonBloc( wBlocName.getText() );
    jsometa.setNrRowsInBloc( wNrRowsInBloc.getText() );
    jsometa.setEncoding( wEncoding.getText() );
    jsometa.setOutputValue( wOutputValue.getText() );
    jsometa.setCompatibilityMode( wCompatibilityMode.getSelection() );
    jsometa.setOperationType( JsonOutputMeta.getOperationTypeByDesc( wOperation.getText() ) );
    jsometa.setCreateParentFolder( wCreateParentFolder.getSelection() );
    jsometa.setFileName( wFilename.getText() );
    jsometa.setExtension( wExtension.getText() );
    jsometa.setFileAppended( wAppend.getSelection() );

    jsometa.setDateInFilename( wAddDate.getSelection() );
    jsometa.setTimeInFilename( wAddTime.getSelection() );

    jsometa.setEncoding( wEncoding.getText() );
    jsometa.setAddToResult( wAddToResult.getSelection() );
    jsometa.setDoNotOpenNewFileInit( wDoNotOpenNewFileInit.getSelection() );

    int nrFields = wFields.nrNonEmpty();

    jsometa.allocate( nrFields );

    for ( int i = 0; i < nrFields; i++ ) {
      JsonOutputField field = new JsonOutputField();

      TableItem item = wFields.getNonEmpty( i );
      field.setFieldName( item.getText( 1 ) );
      field.setElementName( item.getText( 2 ) );
      // CHECKSTYLE:Indentation:OFF
      jsometa.getOutputFields()[i] = field;
    }
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    getInfo( input );

    dispose();
  }

  private void get() {
    boolean gotPreviousFields = false;
    if (gotPreviousFields) {
      return;
    }
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
      if ( r != null ) {
        BaseTransformDialog.getFieldsFromPrevious( r, wFields, 1, new int[] { 1, 2 }, new int[] { 3 }, 5, 6,
          ( tableItem, v ) -> {
            if ( v.isNumber() ) {
              if ( v.getLength() > 0 ) {
                int le = v.getLength();
                int pr = v.getPrecision();

                if ( v.getPrecision() <= 0 ) {
                  pr = 0;
                }

                String mask = " ";
                for ( int m = 0; m < le - pr; m++ ) {
                  mask += "0";
                }
                if ( pr > 0 ) {
                  mask += ".";
                }
                for ( int m = 0; m < pr; m++ ) {
                  mask += "0";
                }
                tableItem.setText( 4, mask );
              }
            }
            return true;
          } );
      }
    } catch ( HopException ke ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Title" ), BaseMessages
          .getString( PKG, "System.Dialog.GetFieldsFailed.Message" ), ke );
    }
  }

  private void updateOperation() {
    int opType = JsonOutputMeta.getOperationTypeByDesc( wOperation.getText() );
    boolean activeFile = opType != JsonOutputMeta.OPERATION_TYPE_OUTPUT_VALUE;

    wlFilename.setEnabled( activeFile );
    wFilename.setEnabled( activeFile );
    wbFilename.setEnabled( activeFile );
    wlExtension.setEnabled( activeFile );
    wExtension.setEnabled( activeFile );
    wlEncoding.setEnabled( activeFile );
    wEncoding.setEnabled( activeFile );
    wlAppend.setEnabled( activeFile );
    wAppend.setEnabled( activeFile );
    wlCreateParentFolder.setEnabled( activeFile );
    wCreateParentFolder.setEnabled( activeFile );
    wlDoNotOpenNewFileInit.setEnabled( activeFile );
    wDoNotOpenNewFileInit.setEnabled( activeFile );
    wlAddDate.setEnabled( activeFile );
    wAddDate.setEnabled( activeFile );
    wlAddTime.setEnabled( activeFile );
    wAddTime.setEnabled( activeFile );
    wlAddToResult.setEnabled( activeFile );
    wAddToResult.setEnabled( activeFile );
    wbShowFiles.setEnabled( activeFile );

    wlServletOutput.setEnabled( opType == JsonOutputMeta.OPERATION_TYPE_WRITE_TO_FILE
        || opType == JsonOutputMeta.OPERATION_TYPE_BOTH );
    wServletOutput.setEnabled( opType == JsonOutputMeta.OPERATION_TYPE_WRITE_TO_FILE
        || opType == JsonOutputMeta.OPERATION_TYPE_BOTH );

    boolean activeOutputValue =
        JsonOutputMeta.getOperationTypeByDesc( wOperation.getText() ) != JsonOutputMeta.OPERATION_TYPE_WRITE_TO_FILE;

    wlOutputValue.setEnabled( activeOutputValue );
    wOutputValue.setEnabled( activeOutputValue );

    setFlagsServletOption();
  }
}
