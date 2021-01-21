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

package org.apache.hop.workflow.actions.shell;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

/**
 * Dialog that allows you to enter the settings for a Shell action.
 *
 * @author Matt
 * @since 19-06-2003
 */

public class ActionShellDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionShell.class; // For Translator

  private static final String[] FILEFORMATS = new String[] {
    BaseMessages.getString( PKG, "JobShell.Fileformat.Scripts" ),
    BaseMessages.getString( PKG, "JobShell.Fileformat.All" ) };

  private Text wName;

  private Label wlFilename;

  private Button wbFilename;

  private TextVar wFilename;

  private TextVar wWorkDirectory;

  private Button wSetLogfile;

  private Label wlLogfile;

  private TextVar wLogfile;

  private Label wlLogext;

  private TextVar wLogext;

  private Label wlAddDate;

  private Button wAddDate;

  private Label wlAddTime;

  private Button wAddTime;

  private Label wlLoglevel;

  private CCombo wLoglevel;

  private Label wlPrevious;

  private Button wPrevious;

  private Label wlEveryRow;

  private Button wEveryRow;

  private Label wlFields;

  private TableView wFields;

  private Shell shell;

  private ActionShell action;

  private boolean backupChanged, backupLogfile, backupDate, backupTime;

  private Label wlAppendLogfile;

  private Button wAppendLogfile, wInsertScript;

  private Display display;

  private Label wlScript;

  private Text wScript;

  public ActionShellDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = (ActionShell) action;
  }

  public IAction open() {
    Shell parent = getParent();
    display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, action );

    ModifyListener lsMod = e -> action.setChanged();
    backupChanged = action.hasChanged();
    backupLogfile = action.setLogfile;
    backupDate = action.addDate;
    backupTime = action.addTime;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobShell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Name line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText( BaseMessages.getString( PKG, "JobShell.Name.Label" ) );
    props.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.top = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, 0 );
    wlName.setLayoutData(fdlName);

    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    FormData fdName = new FormData();
    fdName.top = new FormAttachment( 0, 0 );
    fdName.left = new FormAttachment( middle, 0 );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData(fdName);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText( BaseMessages.getString( PKG, "JobShell.Tab.General.Label" ) );

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout( generalLayout );

    // Insert Script?
    Label wlInsertScript = new Label(wGeneralComp, SWT.RIGHT);
    wlInsertScript.setText( BaseMessages.getString( PKG, "JobShell.InsertScript.Label" ) );
    props.setLook(wlInsertScript);
    FormData fdlInsertScript = new FormData();
    fdlInsertScript.left = new FormAttachment( 0, 0 );
    fdlInsertScript.top = new FormAttachment( wName, margin );
    fdlInsertScript.right = new FormAttachment( middle, -margin );
    wlInsertScript.setLayoutData(fdlInsertScript);
    wInsertScript = new Button(wGeneralComp, SWT.CHECK );
    wInsertScript.setToolTipText( BaseMessages.getString( PKG, "JobShell.InsertScript.Tooltip" ) );
    props.setLook( wInsertScript );
    FormData fdInsertScript = new FormData();
    fdInsertScript.left = new FormAttachment( middle, 0 );
    fdInsertScript.top = new FormAttachment( wName, margin );
    fdInsertScript.right = new FormAttachment( 100, 0 );
    wInsertScript.setLayoutData(fdInsertScript);
    wInsertScript.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        ActiveInsertScript();
        action.setChanged();
      }
    } );

    // /////////////////////
    // Filename line
    // /////////////////////
    wlFilename = new Label(wGeneralComp, SWT.RIGHT );
    wlFilename.setText( BaseMessages.getString( PKG, "JobShell.Filename.Label" ) );
    props.setLook( wlFilename );
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment( wInsertScript, margin );
    fdlFilename.right = new FormAttachment( middle, 0 );
    wlFilename.setLayoutData(fdlFilename);

    wbFilename = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbFilename );
    wbFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbFilename = new FormData();
    fdbFilename.top = new FormAttachment( wInsertScript, margin );
    fdbFilename.right = new FormAttachment( 100, 0 );
    wbFilename.setLayoutData(fdbFilename);

    wFilename = new TextVar( variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.right = new FormAttachment( wbFilename, -margin );
    fdFilename.top = new FormAttachment( wInsertScript, margin );
    wFilename.setLayoutData(fdFilename);

    // /////////////////////
    // Working dir line
    // /////////////////////
    Label wlWorkDirectory = new Label(wGeneralComp, SWT.RIGHT);
    wlWorkDirectory.setText( BaseMessages.getString( PKG, "JobShell.WorkingDirectory.Label" ) );
    props.setLook(wlWorkDirectory);
    FormData fdlWorkDirectory = new FormData();
    fdlWorkDirectory.left = new FormAttachment( 0, 0 );
    fdlWorkDirectory.top = new FormAttachment( wFilename, margin );
    fdlWorkDirectory.right = new FormAttachment( middle, 0 );
    wlWorkDirectory.setLayoutData(fdlWorkDirectory);

    wWorkDirectory = new TextVar( variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wWorkDirectory );
    wWorkDirectory.addModifyListener( lsMod );
    FormData fdWorkDirectory = new FormData();
    fdWorkDirectory.left = new FormAttachment( middle, 0 );
    fdWorkDirectory.right = new FormAttachment( wbFilename, -margin );
    fdWorkDirectory.top = new FormAttachment( wFilename, margin );
    wWorkDirectory.setLayoutData(fdWorkDirectory);

    // ////////////////////////
    // START OF LOGGING GROUP
    //
    Group wLogging = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wLogging);
    wLogging.setText( BaseMessages.getString( PKG, "JobShell.LogSettings.Group.Label" ) );

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;

    wLogging.setLayout( groupLayout );

    // Set the logfile?
    Label wlSetLogfile = new Label(wLogging, SWT.RIGHT);
    wlSetLogfile.setText( BaseMessages.getString( PKG, "JobShell.Specify.Logfile.Label" ) );
    props.setLook(wlSetLogfile);
    FormData fdlSetLogfile = new FormData();
    fdlSetLogfile.left = new FormAttachment( 0, 0 );
    fdlSetLogfile.top = new FormAttachment( 0, margin );
    fdlSetLogfile.right = new FormAttachment( middle, -margin );
    wlSetLogfile.setLayoutData(fdlSetLogfile);
    wSetLogfile = new Button(wLogging, SWT.CHECK );
    props.setLook( wSetLogfile );
    FormData fdSetLogfile = new FormData();
    fdSetLogfile.left = new FormAttachment( middle, 0 );
    fdSetLogfile.top = new FormAttachment( 0, margin );
    fdSetLogfile.right = new FormAttachment( 100, 0 );
    wSetLogfile.setLayoutData(fdSetLogfile);
    wSetLogfile.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setLogfile = !action.setLogfile;
        action.setChanged();
        setActive();
      }
    } );
    // Append logfile?
    wlAppendLogfile = new Label(wLogging, SWT.RIGHT );
    wlAppendLogfile.setText( BaseMessages.getString( PKG, "JobShell.Append.Logfile.Label" ) );
    props.setLook( wlAppendLogfile );
    FormData fdlAppendLogfile = new FormData();
    fdlAppendLogfile.left = new FormAttachment( 0, 0 );
    fdlAppendLogfile.top = new FormAttachment( wSetLogfile, margin );
    fdlAppendLogfile.right = new FormAttachment( middle, -margin );
    wlAppendLogfile.setLayoutData(fdlAppendLogfile);
    wAppendLogfile = new Button(wLogging, SWT.CHECK );
    wAppendLogfile.setToolTipText( BaseMessages.getString( PKG, "JobShell.Append.Logfile.Tooltip" ) );
    props.setLook( wAppendLogfile );
    FormData fdAppendLogfile = new FormData();
    fdAppendLogfile.left = new FormAttachment( middle, 0 );
    fdAppendLogfile.top = new FormAttachment( wSetLogfile, margin );
    fdAppendLogfile.right = new FormAttachment( 100, 0 );
    wAppendLogfile.setLayoutData(fdAppendLogfile);
    wAppendLogfile.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
      }
    } );

    // Set the logfile path + base-name
    wlLogfile = new Label(wLogging, SWT.RIGHT );
    wlLogfile.setText( BaseMessages.getString( PKG, "JobShell.NameOfLogfile.Label" ) );
    props.setLook( wlLogfile );
    FormData fdlLogfile = new FormData();
    fdlLogfile.left = new FormAttachment( 0, 0 );
    fdlLogfile.top = new FormAttachment( wAppendLogfile, margin );
    fdlLogfile.right = new FormAttachment( middle, 0 );
    wlLogfile.setLayoutData(fdlLogfile);
    wLogfile = new TextVar( variables, wLogging, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wLogfile.setText( "" );
    props.setLook( wLogfile );
    FormData fdLogfile = new FormData();
    fdLogfile.left = new FormAttachment( middle, 0 );
    fdLogfile.top = new FormAttachment( wAppendLogfile, margin );
    fdLogfile.right = new FormAttachment( 100, 0 );
    wLogfile.setLayoutData(fdLogfile);

    // Set the logfile filename extention
    wlLogext = new Label(wLogging, SWT.RIGHT );
    wlLogext.setText( BaseMessages.getString( PKG, "JobShell.LogfileExtension.Label" ) );
    props.setLook( wlLogext );
    FormData fdlLogext = new FormData();
    fdlLogext.left = new FormAttachment( 0, 0 );
    fdlLogext.top = new FormAttachment( wLogfile, margin );
    fdlLogext.right = new FormAttachment( middle, 0 );
    wlLogext.setLayoutData(fdlLogext);
    wLogext = new TextVar( variables, wLogging, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wLogext.setText( "" );
    props.setLook( wLogext );
    FormData fdLogext = new FormData();
    fdLogext.left = new FormAttachment( middle, 0 );
    fdLogext.top = new FormAttachment( wLogfile, margin );
    fdLogext.right = new FormAttachment( 100, 0 );
    wLogext.setLayoutData(fdLogext);

    // Add date to logfile name?
    wlAddDate = new Label(wLogging, SWT.RIGHT );
    wlAddDate.setText( BaseMessages.getString( PKG, "JobShell.Logfile.IncludeDate.Label" ) );
    props.setLook( wlAddDate );
    FormData fdlAddDate = new FormData();
    fdlAddDate.left = new FormAttachment( 0, 0 );
    fdlAddDate.top = new FormAttachment( wLogext, margin );
    fdlAddDate.right = new FormAttachment( middle, -margin );
    wlAddDate.setLayoutData(fdlAddDate);
    wAddDate = new Button(wLogging, SWT.CHECK );
    props.setLook( wAddDate );
    FormData fdAddDate = new FormData();
    fdAddDate.left = new FormAttachment( middle, 0 );
    fdAddDate.top = new FormAttachment( wlAddDate, 0, SWT.CENTER );
    fdAddDate.right = new FormAttachment( 100, 0 );
    wAddDate.setLayoutData(fdAddDate);
    wAddDate.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.addDate = !action.addDate;
        action.setChanged();
      }
    } );

    // Add time to logfile name?
    wlAddTime = new Label(wLogging, SWT.RIGHT );
    wlAddTime.setText( BaseMessages.getString( PKG, "JobShell.Logfile.IncludeTime.Label" ) );
    props.setLook( wlAddTime );
    FormData fdlAddTime = new FormData();
    fdlAddTime.left = new FormAttachment( 0, 0 );
    fdlAddTime.top = new FormAttachment( wAddDate, margin );
    fdlAddTime.right = new FormAttachment( middle, -margin );
    wlAddTime.setLayoutData(fdlAddTime);
    wAddTime = new Button(wLogging, SWT.CHECK );
    props.setLook( wAddTime );
    FormData fdAddTime = new FormData();
    fdAddTime.left = new FormAttachment( middle, 0 );
    fdAddTime.top = new FormAttachment( wlAddTime, 0, SWT.CENTER );
    fdAddTime.right = new FormAttachment( 100, 0 );
    wAddTime.setLayoutData(fdAddTime);
    wAddTime.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.addTime = !action.addTime;
        action.setChanged();
      }
    } );

    wlLoglevel = new Label(wLogging, SWT.RIGHT );
    wlLoglevel.setText( BaseMessages.getString( PKG, "JobShell.Loglevel.Label" ) );
    props.setLook( wlLoglevel );
    FormData fdlLoglevel = new FormData();
    fdlLoglevel.left = new FormAttachment( 0, 0 );
    fdlLoglevel.right = new FormAttachment( middle, -margin );
    fdlLoglevel.top = new FormAttachment( wAddTime, margin );
    wlLoglevel.setLayoutData(fdlLoglevel);
    wLoglevel = new CCombo(wLogging, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wLoglevel.setItems( LogLevel.getLogLevelDescriptions() );
    props.setLook( wLoglevel );
    FormData fdLoglevel = new FormData();
    fdLoglevel.left = new FormAttachment( middle, 0 );
    fdLoglevel.top = new FormAttachment( wAddTime, margin );
    fdLoglevel.right = new FormAttachment( 100, 0 );
    wLoglevel.setLayoutData(fdLoglevel);

    FormData fdLogging = new FormData();
    fdLogging.left = new FormAttachment( 0, margin );
    fdLogging.top = new FormAttachment( wWorkDirectory, margin );
    fdLogging.right = new FormAttachment( 100, -margin );
    wLogging.setLayoutData(fdLogging);

    // ///////////////////////////////////////////////////////////
    // / END OF LOGGING GROUP
    // ///////////////////////////////////////////////////////////

    wlPrevious = new Label(wGeneralComp, SWT.RIGHT );
    wlPrevious.setText( BaseMessages.getString( PKG, "JobShell.Previous.Label" ) );
    props.setLook( wlPrevious );
    FormData fdlPrevious = new FormData();
    fdlPrevious.left = new FormAttachment( 0, 0 );
    fdlPrevious.top = new FormAttachment(wLogging, margin * 3 );
    fdlPrevious.right = new FormAttachment( middle, -margin );
    wlPrevious.setLayoutData(fdlPrevious);
    wPrevious = new Button(wGeneralComp, SWT.CHECK );
    props.setLook( wPrevious );
    wPrevious.setSelection( action.argFromPrevious );
    wPrevious.setToolTipText( BaseMessages.getString( PKG, "JobShell.Previous.Tooltip" ) );
    FormData fdPrevious = new FormData();
    fdPrevious.left = new FormAttachment( middle, 0 );
    fdPrevious.top = new FormAttachment(wLogging, margin * 3 );
    fdPrevious.right = new FormAttachment( 100, 0 );
    wPrevious.setLayoutData(fdPrevious);
    wPrevious.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.argFromPrevious = !action.argFromPrevious;
        action.setChanged();
        wlFields.setEnabled( !action.argFromPrevious );
        wFields.setEnabled( !action.argFromPrevious );
      }
    } );

    wlEveryRow = new Label(wGeneralComp, SWT.RIGHT );
    wlEveryRow.setText( BaseMessages.getString( PKG, "JobShell.ExecForEveryInputRow.Label" ) );
    props.setLook( wlEveryRow );
    FormData fdlEveryRow = new FormData();
    fdlEveryRow.left = new FormAttachment( 0, 0 );
    fdlEveryRow.top = new FormAttachment( wPrevious, margin * 3 );
    fdlEveryRow.right = new FormAttachment( middle, -margin );
    wlEveryRow.setLayoutData(fdlEveryRow);
    wEveryRow = new Button(wGeneralComp, SWT.CHECK );
    props.setLook( wEveryRow );
    wEveryRow.setSelection( action.execPerRow );
    wEveryRow.setToolTipText( BaseMessages.getString( PKG, "JobShell.ExecForEveryInputRow.Tooltip" ) );
    FormData fdEveryRow = new FormData();
    fdEveryRow.left = new FormAttachment( middle, 0 );
    fdEveryRow.top = new FormAttachment( wPrevious, margin * 3 );
    fdEveryRow.right = new FormAttachment( 100, 0 );
    wEveryRow.setLayoutData(fdEveryRow);
    wEveryRow.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.execPerRow = !action.execPerRow;
        action.setChanged();
      }
    } );

    wlFields = new Label(wGeneralComp, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "JobShell.Fields.Label" ) );
    props.setLook( wlFields );
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( wEveryRow, margin );
    wlFields.setLayoutData(fdlFields);

    final int FieldsCols = 1;
    int rows = action.arguments == null ? 1 : ( action.arguments.length == 0 ? 0 : action.arguments.length );
    final int FieldsRows = rows;

    ColumnInfo[] colinf = new ColumnInfo[ FieldsCols ];
    colinf[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "JobShell.Fields.Argument.Label" ), ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinf[ 0 ].setUsingVariables( true );

    wFields =
      new TableView(
    		  variables, wGeneralComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( 100, -margin );
    wFields.setLayoutData(fdFields);

    wlFields.setEnabled( !action.argFromPrevious );
    wFields.setEnabled( !action.argFromPrevious );

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( 0, 0 );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 500, -margin );
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);
    props.setLook(wGeneralComp);

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////////////////
    // START OF Script TAB ///
    // ///////////////////////////////////

    CTabItem wScriptTab = new CTabItem(wTabFolder, SWT.NONE);
    wScriptTab.setText( BaseMessages.getString( PKG, "JobShell.Tab.Script.Label" ) );

    FormLayout ScriptLayout = new FormLayout();
    ScriptLayout.marginWidth = 3;
    ScriptLayout.marginHeight = 3;

    Composite wScriptComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wScriptComp);
    wScriptComp.setLayout( ScriptLayout );

    // Script line

    wScript = new Text(wScriptComp, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL );
    props.setLook( wScript );
    wScript.addModifyListener( lsMod );
    FormData fdScript = new FormData();
    fdScript.left = new FormAttachment( 0, margin );
    fdScript.top = new FormAttachment( wlScript, margin );
    fdScript.right = new FormAttachment( 100, 0 );
    fdScript.bottom = new FormAttachment( 100, -margin );
    wScript.setLayoutData(fdScript);

    FormData fdScriptComp = new FormData();
    fdScriptComp.left = new FormAttachment( 0, 0 );
    fdScriptComp.top = new FormAttachment( 0, 0 );
    fdScriptComp.right = new FormAttachment( 100, 0 );
    fdScriptComp.bottom = new FormAttachment( 100, 0 );
    wScriptComp.setLayoutData(wScriptComp);

    wScriptComp.layout();
    wScriptTab.setControl(wScriptComp);

    // ///////////////////////////////////////////////////////////
    // / END OF Script TAB
    // ///////////////////////////////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( 100, -50 );
    wTabFolder.setLayoutData(fdTabFolder);

    // Some buttons
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] {wOk, wCancel}, margin, wTabFolder);

    // Add listeners
    Listener lsCancel = e -> cancel();
    Listener lsOk = e -> ok();

    wOk.addListener( SWT.Selection, lsOk);
    wCancel.addListener( SWT.Selection, lsCancel);

    SelectionAdapter lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected(SelectionEvent e) {
        ok();
      }
    };
    wName.addSelectionListener(lsDef);
    wFilename.addSelectionListener(lsDef);

    wbFilename.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wFilename, variables,
      new String[] { "*.sh;*.bat;*.BAT", "*;*.*" }, FILEFORMATS, true ));


    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    setActive();
    ActiveInsertScript();
    wTabFolder.setSelection( 0 );

    BaseTransformDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "JobShellDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }

  private void ActiveInsertScript() {
    wFilename.setEnabled( !wInsertScript.getSelection() );
    wlFilename.setEnabled( !wInsertScript.getSelection() );
    wbFilename.setEnabled( !wInsertScript.getSelection() );
    wScript.setEnabled( wInsertScript.getSelection() );
    // We can not use arguments !!!
    if ( wInsertScript.getSelection() ) {
      wFields.clearAll( false );
      wFields.setEnabled( false );
      wlFields.setEnabled( false );
      wPrevious.setSelection( false );
      wPrevious.setEnabled( false );
      wlPrevious.setEnabled( false );
      wEveryRow.setSelection( false );
      wEveryRow.setEnabled( false );
      wlEveryRow.setEnabled( false );
    } else {
      wFields.setEnabled( true );
      wlFields.setEnabled( true );
      wPrevious.setEnabled( true );
      wlPrevious.setEnabled( true );
      wEveryRow.setEnabled( true );
      wlEveryRow.setEnabled( true );
    }

  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  public void setActive() {
    wlLogfile.setEnabled( action.setLogfile );
    wLogfile.setEnabled( action.setLogfile );

    wlLogext.setEnabled( action.setLogfile );
    wLogext.setEnabled( action.setLogfile );

    wlAddDate.setEnabled( action.setLogfile );
    wAddDate.setEnabled( action.setLogfile );

    wlAddTime.setEnabled( action.setLogfile );
    wAddTime.setEnabled( action.setLogfile );

    wlLoglevel.setEnabled( action.setLogfile );
    wLoglevel.setEnabled( action.setLogfile );

    wlAppendLogfile.setEnabled( action.setLogfile );
    wAppendLogfile.setEnabled( action.setLogfile );

    if ( action.setLogfile ) {
      wLoglevel.setForeground( display.getSystemColor( SWT.COLOR_BLACK ) );
    } else {
      wLoglevel.setForeground( display.getSystemColor( SWT.COLOR_GRAY ) );
    }
  }

  public void getData() {
    wName.setText( Const.nullToEmpty( action.getName() ) );
    wFilename.setText( Const.nullToEmpty( action.getFilename() ) );
    wWorkDirectory.setText( Const.nullToEmpty( action.getWorkDirectory() ) );

    if ( action.arguments != null ) {
      for ( int i = 0; i < action.arguments.length; i++ ) {
        TableItem ti = wFields.table.getItem( i );
        if ( action.arguments[ i ] != null ) {
          ti.setText( 1, action.arguments[ i ] );
        }
      }
      wFields.setRowNums();
      wFields.optWidth( true );
    }
    wPrevious.setSelection( action.argFromPrevious );
    wEveryRow.setSelection( action.execPerRow );
    wSetLogfile.setSelection( action.setLogfile );
    wLogfile.setText( Const.nullToEmpty( action.logfile ) );
    wLogext.setText( Const.nullToEmpty( action.logext ) );
    wAddDate.setSelection( action.addDate );
    wAddTime.setSelection( action.addTime );
    wAppendLogfile.setSelection( action.setAppendLogfile );
    if ( action.logFileLevel != null ) {
      wLoglevel.select( action.logFileLevel.getLevel() );
    }

    wInsertScript.setSelection( action.insertScript );
    wScript.setText( Const.nullToEmpty( action.getScript() ) );

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    action.setChanged( backupChanged );
    action.setLogfile = backupLogfile;
    action.addDate = backupDate;
    action.addTime = backupTime;

    action = null;
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "System.TransformActionNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.ActionNameMissing.Msg" ) );
      mb.open();
      return;
    }
    action.setFileName( wFilename.getText() );
    action.setName( wName.getText() );
    action.setWorkDirectory( wWorkDirectory.getText() );

    int nrItems = wFields.nrNonEmpty();
    int nr = 0;
    for ( int i = 0; i < nrItems; i++ ) {
      String arg = wFields.getNonEmpty( i ).getText( 1 );
      if ( arg != null && arg.length() != 0 ) {
        nr++;
      }
    }
    action.arguments = new String[ nr ];
    nr = 0;
    for ( int i = 0; i < nrItems; i++ ) {
      String arg = wFields.getNonEmpty( i ).getText( 1 );
      if ( arg != null && arg.length() != 0 ) {
        action.arguments[ nr ] = arg;
        nr++;
      }
    }

    action.logfile = wLogfile.getText();
    action.logext = wLogext.getText();
    if ( wLoglevel.getSelectionIndex() >= 0 ) {
      action.logFileLevel = LogLevel.values()[ wLoglevel.getSelectionIndex() ];
    } else {
      action.logFileLevel = LogLevel.BASIC;
    }
    action.setAppendLogfile = wAppendLogfile.getSelection();
    action.setScript( wScript.getText() );
    action.insertScript = wInsertScript.getSelection();
    dispose();
  }
}
