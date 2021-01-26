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

package org.apache.hop.workflow.actions.sendnagiospassivecheck;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.SocketUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.LabelText;
import org.apache.hop.ui.core.widget.LabelTextVar;
import org.apache.hop.ui.core.widget.StyledTextComp;
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
 * This dialog allows you to edit the SendNagiosPassiveCheck action settings.
 *
 * @author Samatar
 * @since 01-10-2011
 */
public class ActionSendNagiosPassiveCheckDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionSendNagiosPassiveCheck.class; // For Translator

  private LabelText wName;

  private LabelTextVar wServerName;

  private LabelTextVar wResponseTimeOut;

  private LabelTextVar wPassword;

  private LabelTextVar wSenderServerName;

  private LabelTextVar wSenderServiceName;

  private ActionSendNagiosPassiveCheck action;

  private Shell shell;

  private boolean changed;

  private LabelTextVar wPort;

  private LabelTextVar wConnectionTimeOut;

  private StyledTextComp wMessage;

  private CCombo wEncryptionMode;

  private CCombo wLevelMode;

  public ActionSendNagiosPassiveCheckDialog( Shell parent, IAction action,
                                             WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = (ActionSendNagiosPassiveCheck) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.Name.Default" ) );
    }
  }

  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, action );

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Action name line
    wName =
      new LabelText( shell, BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.Name.Label" ), BaseMessages
        .getString( PKG, "JobSendNagiosPassiveCheck.Name.Tooltip" ) );
    wName.addModifyListener( lsMod );
    FormData fdName = new FormData();
    fdName.top = new FormAttachment( 0, 0 );
    fdName.left = new FormAttachment( 0, 0 );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData(fdName);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, PropsUi.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText( BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.ServerSettings.General" ) );

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout( generalLayout );

    // ////////////////////////
    // START OF SERVER SETTINGS GROUP///
    // /
    Group wServerSettings = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wServerSettings);
    wServerSettings
      .setText( BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.ServerSettings.Group.Label" ) );

    FormLayout ServerSettingsgroupLayout = new FormLayout();
    ServerSettingsgroupLayout.marginWidth = 10;
    ServerSettingsgroupLayout.marginHeight = 10;

    wServerSettings.setLayout( ServerSettingsgroupLayout );

    // ServerName line
    wServerName = new LabelTextVar( variables, wServerSettings,
      BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.Server.Label" ),
      BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.Server.Tooltip" ) );
    props.setLook( wServerName );
    wServerName.addModifyListener( lsMod );
    FormData fdServerName = new FormData();
    fdServerName.left = new FormAttachment( 0, 0 );
    fdServerName.top = new FormAttachment( wName, margin );
    fdServerName.right = new FormAttachment( 100, 0 );
    wServerName.setLayoutData(fdServerName);

    // Server port line
    wPort = new LabelTextVar( variables, wServerSettings,
      BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.Port.Label" ),
      BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.Port.Tooltip" ) );
    props.setLook( wPort );
    wPort.addModifyListener( lsMod );
    FormData fdPort = new FormData();
    fdPort.left = new FormAttachment( 0, 0 );
    fdPort.top = new FormAttachment( wServerName, margin );
    fdPort.right = new FormAttachment( 100, 0 );
    wPort.setLayoutData(fdPort);

    // Password String line
    wPassword =
      new LabelTextVar( variables, wServerSettings, BaseMessages.getString(
        PKG, "JobSendNagiosPassiveCheck.Password.Label" ), BaseMessages
        .getString( "JobSendNagiosPassiveCheck.Password.Tooltip" ), true );
    props.setLook( wPassword );
    wPassword.addModifyListener( lsMod );
    FormData fdPassword = new FormData();
    fdPassword.left = new FormAttachment( 0, 0 );
    fdPassword.top = new FormAttachment( wPort, margin );
    fdPassword.right = new FormAttachment( 100, 0 );
    wPassword.setLayoutData(fdPassword);

    // Server wConnectionTimeOut line
    wConnectionTimeOut =
      new LabelTextVar( variables, wServerSettings,
        BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.ConnectionTimeOut.Label" ),
        BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.ConnectionTimeOut.Tooltip" ) );
    props.setLook( wConnectionTimeOut );
    wConnectionTimeOut.addModifyListener( lsMod );
    FormData fdwConnectionTimeOut = new FormData();
    fdwConnectionTimeOut.left = new FormAttachment( 0, 0 );
    fdwConnectionTimeOut.top = new FormAttachment( wPassword, margin );
    fdwConnectionTimeOut.right = new FormAttachment( 100, 0 );
    wConnectionTimeOut.setLayoutData(fdwConnectionTimeOut);

    // ResponseTimeOut line
    wResponseTimeOut = new LabelTextVar( variables, wServerSettings,
      BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.ResponseTimeOut.Label" ),
      BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.ResponseTimeOut.Tooltip" ) );
    props.setLook( wResponseTimeOut );
    wResponseTimeOut.addModifyListener( lsMod );
    FormData fdResponseTimeOut = new FormData();
    fdResponseTimeOut.left = new FormAttachment( 0, 0 );
    fdResponseTimeOut.top = new FormAttachment( wConnectionTimeOut, margin );
    fdResponseTimeOut.right = new FormAttachment( 100, 0 );
    wResponseTimeOut.setLayoutData(fdResponseTimeOut);

    // Test connection button
    Button wTest = new Button(wServerSettings, SWT.PUSH);
    wTest.setText( BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.TestConnection.Label" ) );
    props.setLook(wTest);
    FormData fdTest = new FormData();
    wTest.setToolTipText( BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.TestConnection.Tooltip" ) );
    fdTest.top = new FormAttachment( wResponseTimeOut, margin );
    fdTest.right = new FormAttachment( 100, 0 );
    wTest.setLayoutData(fdTest);

    FormData fdServerSettings = new FormData();
    fdServerSettings.left = new FormAttachment( 0, margin );
    fdServerSettings.top = new FormAttachment( wName, margin );
    fdServerSettings.right = new FormAttachment( 100, -margin );
    wServerSettings.setLayoutData(fdServerSettings);
    // ///////////////////////////////////////////////////////////
    // / END OF SERVER SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF Advanced SETTINGS GROUP///
    // /
    Group wSenderSettings = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wSenderSettings);
    wSenderSettings
      .setText( BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.SenderSettings.Group.Label" ) );
    FormLayout SenderSettingsgroupLayout = new FormLayout();
    SenderSettingsgroupLayout.marginWidth = 10;
    SenderSettingsgroupLayout.marginHeight = 10;
    wSenderSettings.setLayout( SenderSettingsgroupLayout );

    // SenderServerName line
    wSenderServerName = new LabelTextVar( variables, wSenderSettings,
      BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.SenderServerName.Label" ),
      BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.SenderServerName.Tooltip" ) );
    props.setLook( wSenderServerName );
    wSenderServerName.addModifyListener( lsMod );
    FormData fdSenderServerName = new FormData();
    fdSenderServerName.left = new FormAttachment( 0, 0 );
    fdSenderServerName.top = new FormAttachment(wServerSettings, margin );
    fdSenderServerName.right = new FormAttachment( 100, 0 );
    wSenderServerName.setLayoutData(fdSenderServerName);

    // SenderServiceName line
    wSenderServiceName = new LabelTextVar( variables, wSenderSettings,
      BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.SenderServiceName.Label" ),
      BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.SenderServiceName.Tooltip" ) );
    props.setLook( wSenderServiceName );
    wSenderServiceName.addModifyListener( lsMod );
    FormData fdSenderServiceName = new FormData();
    fdSenderServiceName.left = new FormAttachment( 0, 0 );
    fdSenderServiceName.top = new FormAttachment( wSenderServerName, margin );
    fdSenderServiceName.right = new FormAttachment( 100, 0 );
    wSenderServiceName.setLayoutData(fdSenderServiceName);

    // Encryption mode
    Label wlEncryptionMode = new Label(wSenderSettings, SWT.RIGHT);
    wlEncryptionMode.setText( BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.EncryptionMode.Label" ) );
    props.setLook(wlEncryptionMode);
    FormData fdlEncryptionMode = new FormData();
    fdlEncryptionMode.left = new FormAttachment( 0, margin );
    fdlEncryptionMode.right = new FormAttachment( middle, -margin );
    fdlEncryptionMode.top = new FormAttachment( wSenderServiceName, margin );
    wlEncryptionMode.setLayoutData(fdlEncryptionMode);
    wEncryptionMode = new CCombo(wSenderSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wEncryptionMode.setItems( ActionSendNagiosPassiveCheck.encryptionModeDesc );

    props.setLook( wEncryptionMode );
    FormData fdEncryptionMode = new FormData();
    fdEncryptionMode.left = new FormAttachment( middle, margin );
    fdEncryptionMode.top = new FormAttachment( wSenderServiceName, margin );
    fdEncryptionMode.right = new FormAttachment( 100, 0 );
    wEncryptionMode.setLayoutData(fdEncryptionMode);
    wEncryptionMode.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {

      }
    } );

    // Level mode
    Label wlLevelMode = new Label(wSenderSettings, SWT.RIGHT);
    wlLevelMode.setText( BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.LevelMode.Label" ) );
    props.setLook(wlLevelMode);
    FormData fdlLevelMode = new FormData();
    fdlLevelMode.left = new FormAttachment( 0, margin );
    fdlLevelMode.right = new FormAttachment( middle, -margin );
    fdlLevelMode.top = new FormAttachment( wEncryptionMode, margin );
    wlLevelMode.setLayoutData(fdlLevelMode);
    wLevelMode = new CCombo(wSenderSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wLevelMode.setItems( ActionSendNagiosPassiveCheck.levelTypeDesc );

    props.setLook( wLevelMode );
    FormData fdLevelMode = new FormData();
    fdLevelMode.left = new FormAttachment( middle, margin );
    fdLevelMode.top = new FormAttachment( wEncryptionMode, margin );
    fdLevelMode.right = new FormAttachment( 100, 0 );
    wLevelMode.setLayoutData(fdLevelMode);
    wLevelMode.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {

      }
    } );

    FormData fdSenderSettings = new FormData();
    fdSenderSettings.left = new FormAttachment( 0, margin );
    fdSenderSettings.top = new FormAttachment(wServerSettings, margin );
    fdSenderSettings.right = new FormAttachment( 100, -margin );
    wSenderSettings.setLayoutData(fdSenderSettings);
    // ///////////////////////////////////////////////////////////
    // / END OF Advanced SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF MESSAGE GROUP///
    // /
    Group wMessageGroup = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wMessageGroup);
    wMessageGroup.setText( BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.MessageGroup.Group.Label" ) );
    FormLayout MessageGroupgroupLayout = new FormLayout();
    MessageGroupgroupLayout.marginWidth = 10;
    MessageGroupgroupLayout.marginHeight = 10;
    wMessageGroup.setLayout( MessageGroupgroupLayout );

    // Message line
    Label wlMessage = new Label(wMessageGroup, SWT.RIGHT);
    wlMessage.setText( BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.Message.Label" ) );
    props.setLook(wlMessage);
    FormData fdlMessage = new FormData();
    fdlMessage.left = new FormAttachment( 0, 0 );
    fdlMessage.top = new FormAttachment(wSenderSettings, margin );
    fdlMessage.right = new FormAttachment( middle, -margin );
    wlMessage.setLayoutData(fdlMessage);

    wMessage =
      new StyledTextComp( variables, wMessageGroup, SWT.MULTI
        | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL );
    props.setLook( wMessage );
    wMessage.addModifyListener( lsMod );
    FormData fdMessage = new FormData();
    fdMessage.left = new FormAttachment( middle, 0 );
    fdMessage.top = new FormAttachment(wSenderSettings, margin );
    fdMessage.right = new FormAttachment( 100, -2 * margin );
    fdMessage.bottom = new FormAttachment( 100, -margin );
    wMessage.setLayoutData(fdMessage);

    FormData fdMessageGroup = new FormData();
    fdMessageGroup.left = new FormAttachment( 0, margin );
    fdMessageGroup.top = new FormAttachment(wSenderSettings, margin );
    fdMessageGroup.right = new FormAttachment( 100, -margin );
    fdMessageGroup.bottom = new FormAttachment( 100, -margin );
    wMessageGroup.setLayoutData(fdMessageGroup);
    // ///////////////////////////////////////////////////////////
    // / END OF MESSAGE GROUP
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

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( 100, -50 );
    wTabFolder.setLayoutData(fdTabFolder);

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] {wOk, wCancel}, margin, wTabFolder);

    // Add listeners
    Listener lsCancel = e -> cancel();
    Listener lsOk = e -> ok();
    Listener lsTest = e -> test();

    wCancel.addListener( SWT.Selection, lsCancel);
    wOk.addListener( SWT.Selection, lsOk);
    wTest.addListener( SWT.Selection, lsTest);

    SelectionAdapter lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected(SelectionEvent e) {
        ok();
      }
    };

    wName.addSelectionListener(lsDef);
    wServerName.addSelectionListener(lsDef);
    wResponseTimeOut.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    wTabFolder.setSelection( 0 );
    BaseTransformDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "JobSendNagiosPassiveCheckDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }

  private void test() {
    boolean testOK = false;
    String errMsg = null;
    String hostname = variables.resolve( wServerName.getText() );
    int nrPort =
      Const.toInt(
    		  variables.resolve( "" + wPort.getText() ), ActionSendNagiosPassiveCheck.DEFAULT_PORT );
    int realConnectionTimeOut = Const.toInt( variables.resolve( wConnectionTimeOut.getText() ), -1 );

    try {
      SocketUtil.connectToHost( hostname, nrPort, realConnectionTimeOut );
      testOK = true;
    } catch ( Exception e ) {
      errMsg = e.getMessage();
    }
    if ( testOK ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
      mb.setMessage( BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.Connected.OK", hostname ) + Const.CR );
      mb.setText( BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.Connected.Title.Ok" ) );
      mb.open();
    } else {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString(
        PKG, "JobSendNagiosPassiveCheck.Connected.NOK.ConnectionBad", hostname )
        + Const.CR + errMsg + Const.CR );
      mb.setText( BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.Connected.Title.Bad" ) );
      mb.open();
    }

  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wName.setText( Const.nullToEmpty( action.getName() ) );

    wServerName.setText( Const.NVL( action.getServerName(), "" ) );
    wPort.setText( Const.nullToEmpty( action.getPort() ) );
    wConnectionTimeOut.setText( Const.NVL( action.getConnectionTimeOut(), "" ) );
    wResponseTimeOut.setText( Const.nullToEmpty( action.getResponseTimeOut() ) );
    wPassword.setText( Const.NVL( action.getPassword(), "" ) );
    wSenderServerName.setText( Const.NVL( action.getSenderServerName(), "" ) );
    wSenderServiceName.setText( Const.NVL( action.getSenderServiceName(), "" ) );
    wMessage.setText( Const.NVL( action.getMessage(), "" ) );
    wEncryptionMode.setText( ActionSendNagiosPassiveCheck.getEncryptionModeDesc( action.getEncryptionMode() ) );
    wLevelMode.setText( ActionSendNagiosPassiveCheck.getLevelDesc( action.getLevel() ) );

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    action.setChanged( changed );
    action = null;
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "System.Error.TransformNameMissing.Message" ) );
      mb.setText( BaseMessages.getString( PKG, "System.Error.TransformNameMissing.Title" ) );
      mb.open();
      return;
    }
    action.setName( wName.getText() );
    action.setPort( wPort.getText() );
    action.setServerName( wServerName.getText() );
    action.setConnectionTimeOut( wConnectionTimeOut.getText() );
    action.setResponseTimeOut( wResponseTimeOut.getText() );
    action.setSenderServerName( wSenderServerName.getText() );
    action.setSenderServiceName( wSenderServiceName.getText() );
    action.setMessage( wMessage.getText() );
    action.setEncryptionMode( ActionSendNagiosPassiveCheck.getEncryptionModeByDesc( wEncryptionMode.getText() ) );
    action.setLevel( ActionSendNagiosPassiveCheck.getLevelByDesc( wLevelMode.getText() ) );
    action.setPassword( wPassword.getText() );

    dispose();
  }
}
