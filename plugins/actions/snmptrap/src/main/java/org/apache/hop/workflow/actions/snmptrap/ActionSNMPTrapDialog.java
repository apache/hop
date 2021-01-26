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

package org.apache.hop.workflow.actions.snmptrap;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
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
import org.snmp4j.UserTarget;
import org.snmp4j.smi.UdpAddress;

import java.net.InetAddress;

/**
 * This dialog allows you to edit the SNMPTrap action settings.
 *
 * @author Samatar
 * @since 12-09-2008
 */
public class ActionSNMPTrapDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionSNMPTrap.class; // For Translator

  private LabelText wName;

  private LabelTextVar wServerName;

  private LabelTextVar wTimeout;

  private LabelTextVar wComString;

  private LabelTextVar wUser;

  private LabelTextVar wPassphrase;

  private LabelTextVar wEngineID;

  private LabelTextVar wRetry;

  private ActionSNMPTrap action;

  private Shell shell;

  // private Props props;

  private boolean changed;

  private LabelTextVar wPort;

  private LabelTextVar wOID;

  private StyledTextComp wMessage;

  private CCombo wTargetType;

  public ActionSNMPTrapDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = (ActionSNMPTrap) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "JobSNMPTrap.Name.Default" ) );
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
    shell.setText( BaseMessages.getString( PKG, "JobSNMPTrap.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Action name line
    wName =
      new LabelText( shell, BaseMessages.getString( PKG, "JobSNMPTrap.Name.Label" ), BaseMessages.getString(
        PKG, "JobSNMPTrap.Name.Tooltip" ) );
    wName.addModifyListener( lsMod );
    FormData fdName = new FormData();
    fdName.top = new FormAttachment( 0, 0 );
    fdName.left = new FormAttachment( 0, 0 );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData(fdName);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText( BaseMessages.getString( PKG, "JobSNMPTrap.Tab.General.Label" ) );

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
    wServerSettings.setText( BaseMessages.getString( PKG, "JobSNMPTrap.ServerSettings.Group.Label" ) );

    FormLayout ServerSettingsgroupLayout = new FormLayout();
    ServerSettingsgroupLayout.marginWidth = 10;
    ServerSettingsgroupLayout.marginHeight = 10;

    wServerSettings.setLayout( ServerSettingsgroupLayout );

    // ServerName line
    wServerName =
      new LabelTextVar(
    		  variables, wServerSettings, BaseMessages.getString( PKG, "JobSNMPTrap.Server.Label" ), BaseMessages
        .getString( PKG, "JobSNMPTrap.Server.Tooltip" ) );
    props.setLook( wServerName );
    wServerName.addModifyListener( lsMod );
    FormData fdServerName = new FormData();
    fdServerName.left = new FormAttachment( 0, 0 );
    fdServerName.top = new FormAttachment( wName, margin );
    fdServerName.right = new FormAttachment( 100, 0 );
    wServerName.setLayoutData(fdServerName);

    // Server port line
    wPort =
      new LabelTextVar(
    		  variables, wServerSettings, BaseMessages.getString( PKG, "JobSNMPTrap.Port.Label" ), BaseMessages
        .getString( PKG, "JobSNMPTrap.Port.Tooltip" ) );
    props.setLook( wPort );
    wPort.addModifyListener( lsMod );
    FormData fdPort = new FormData();
    fdPort.left = new FormAttachment( 0, 0 );
    fdPort.top = new FormAttachment( wServerName, margin );
    fdPort.right = new FormAttachment( 100, 0 );
    wPort.setLayoutData(fdPort);

    // Server OID line
    wOID =
      new LabelTextVar(
    		  variables, wServerSettings, BaseMessages.getString( PKG, "JobSNMPTrap.OID.Label" ), BaseMessages
        .getString( PKG, "JobSNMPTrap.OID.Tooltip" ) );
    props.setLook( wOID );
    wOID.addModifyListener( lsMod );
    FormData fdOID = new FormData();
    fdOID.left = new FormAttachment( 0, 0 );
    fdOID.top = new FormAttachment( wPort, margin );
    fdOID.right = new FormAttachment( 100, 0 );
    wOID.setLayoutData(fdOID);

    // Test connection button
    Button wTest = new Button(wServerSettings, SWT.PUSH);
    wTest.setText( BaseMessages.getString( PKG, "JobSNMPTrap.TestConnection.Label" ) );
    props.setLook(wTest);
    FormData fdTest = new FormData();
    wTest.setToolTipText( BaseMessages.getString( PKG, "JobSNMPTrap.TestConnection.Tooltip" ) );
    fdTest.top = new FormAttachment( wOID, margin );
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
    Group wAdvancedSettings = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wAdvancedSettings);
    wAdvancedSettings.setText( BaseMessages.getString( PKG, "JobSNMPTrap.AdvancedSettings.Group.Label" ) );
    FormLayout AdvancedSettingsgroupLayout = new FormLayout();
    AdvancedSettingsgroupLayout.marginWidth = 10;
    AdvancedSettingsgroupLayout.marginHeight = 10;
    wAdvancedSettings.setLayout( AdvancedSettingsgroupLayout );

    // Target type
    Label wlTargetType = new Label(wAdvancedSettings, SWT.RIGHT);
    wlTargetType.setText( BaseMessages.getString( PKG, "JobSNMPTrap.TargetType.Label" ) );
    props.setLook(wlTargetType);
    FormData fdlTargetType = new FormData();
    fdlTargetType.left = new FormAttachment( 0, margin );
    fdlTargetType.right = new FormAttachment( middle, -margin );
    fdlTargetType.top = new FormAttachment(wServerSettings, margin );
    wlTargetType.setLayoutData(fdlTargetType);
    wTargetType = new CCombo(wAdvancedSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wTargetType.setItems( ActionSNMPTrap.targetTypeDesc );

    props.setLook( wTargetType );
    FormData fdTargetType = new FormData();
    fdTargetType.left = new FormAttachment( middle, margin );
    fdTargetType.top = new FormAttachment(wServerSettings, margin );
    fdTargetType.right = new FormAttachment( 100, 0 );
    wTargetType.setLayoutData(fdTargetType);
    wTargetType.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        CheckuseUserTarget();

      }
    } );

    // Community String line
    wComString =
      new LabelTextVar(
    		  variables, wAdvancedSettings, BaseMessages.getString( PKG, "JobSNMPTrap.ComString.Label" ), BaseMessages
        .getString( PKG, "JobSNMPTrap.ComString.Tooltip" ) );
    props.setLook( wComString );
    wComString.addModifyListener( lsMod );
    FormData fdComString = new FormData();
    fdComString.left = new FormAttachment( 0, 0 );
    fdComString.top = new FormAttachment( wTargetType, margin );
    fdComString.right = new FormAttachment( 100, 0 );
    wComString.setLayoutData(fdComString);

    // User line
    wUser =
      new LabelTextVar(
    		  variables, wAdvancedSettings, BaseMessages.getString( PKG, "JobSNMPTrap.User.Label" ), BaseMessages
        .getString( PKG, "JobSNMPTrap.User.Tooltip" ) );
    props.setLook( wUser );
    wUser.addModifyListener( lsMod );
    FormData fdUser = new FormData();
    fdUser.left = new FormAttachment( 0, 0 );
    fdUser.top = new FormAttachment( wComString, margin );
    fdUser.right = new FormAttachment( 100, 0 );
    wUser.setLayoutData(fdUser);

    // Passphrase String line
    wPassphrase =
      new LabelTextVar(
    		  variables, wAdvancedSettings, BaseMessages.getString( PKG, "JobSNMPTrap.Passphrase.Label" ),
        BaseMessages.getString( PKG, "JobSNMPTrap.Passphrase.Tooltip" ), true );
    props.setLook( wPassphrase );
    wPassphrase.addModifyListener( lsMod );
    FormData fdPassphrase = new FormData();
    fdPassphrase.left = new FormAttachment( 0, 0 );
    fdPassphrase.top = new FormAttachment( wUser, margin );
    fdPassphrase.right = new FormAttachment( 100, 0 );
    wPassphrase.setLayoutData(fdPassphrase);

    // EngineID String line
    wEngineID =
      new LabelTextVar(
    		  variables, wAdvancedSettings, BaseMessages.getString( PKG, "JobSNMPTrap.EngineID.Label" ), BaseMessages
        .getString( PKG, "JobSNMPTrap.EngineID.Tooltip" ) );
    props.setLook( wEngineID );
    wEngineID.addModifyListener( lsMod );
    FormData fdEngineID = new FormData();
    fdEngineID.left = new FormAttachment( 0, 0 );
    fdEngineID.top = new FormAttachment( wPassphrase, margin );
    fdEngineID.right = new FormAttachment( 100, 0 );
    wEngineID.setLayoutData(fdEngineID);

    // Retry line
    wRetry =
      new LabelTextVar(
    		  variables, wAdvancedSettings, BaseMessages.getString( PKG, "JobSNMPTrap.Retry.Label" ), BaseMessages
        .getString( PKG, "JobSNMPTrap.Retry.Tooltip" ) );
    props.setLook( wRetry );
    wRetry.addModifyListener( lsMod );
    FormData fdRetry = new FormData();
    fdRetry.left = new FormAttachment( 0, 0 );
    fdRetry.top = new FormAttachment( wEngineID, margin );
    fdRetry.right = new FormAttachment( 100, 0 );
    wRetry.setLayoutData(fdRetry);

    // Timeout line
    wTimeout =
      new LabelTextVar(
    		  variables, wAdvancedSettings, BaseMessages.getString( PKG, "JobSNMPTrap.Timeout.Label" ), BaseMessages
        .getString( PKG, "JobSNMPTrap.Timeout.Tooltip" ) );
    props.setLook( wTimeout );
    wTimeout.addModifyListener( lsMod );
    FormData fdTimeout = new FormData();
    fdTimeout.left = new FormAttachment( 0, 0 );
    fdTimeout.top = new FormAttachment( wRetry, margin );
    fdTimeout.right = new FormAttachment( 100, 0 );
    wTimeout.setLayoutData(fdTimeout);

    FormData fdAdvancedSettings = new FormData();
    fdAdvancedSettings.left = new FormAttachment( 0, margin );
    fdAdvancedSettings.top = new FormAttachment(wServerSettings, margin );
    fdAdvancedSettings.right = new FormAttachment( 100, -margin );
    wAdvancedSettings.setLayoutData(fdAdvancedSettings);
    // ///////////////////////////////////////////////////////////
    // / END OF Advanced SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF MESSAGE GROUP///
    // /
    Group wMessageGroup = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wMessageGroup);
    wMessageGroup.setText( BaseMessages.getString( PKG, "JobSNMPTrap.MessageGroup.Group.Label" ) );
    FormLayout MessageGroupgroupLayout = new FormLayout();
    MessageGroupgroupLayout.marginWidth = 10;
    MessageGroupgroupLayout.marginHeight = 10;
    wMessageGroup.setLayout( MessageGroupgroupLayout );

    // Message line
    Label wlMessage = new Label(wMessageGroup, SWT.RIGHT);
    wlMessage.setText( BaseMessages.getString( PKG, "JobSNMPTrap.Message.Label" ) );
    props.setLook(wlMessage);
    FormData fdlMessage = new FormData();
    fdlMessage.left = new FormAttachment( 0, 0 );
    fdlMessage.top = new FormAttachment( wComString, margin );
    fdlMessage.right = new FormAttachment( middle, -margin );
    wlMessage.setLayoutData(fdlMessage);

    wMessage = new StyledTextComp( action, wMessageGroup, SWT.MULTI
        | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL );
    props.setLook( wMessage );
    wMessage.addModifyListener( lsMod );
    FormData fdMessage = new FormData();
    fdMessage.left = new FormAttachment( middle, 0 );
    fdMessage.top = new FormAttachment( wComString, margin );
    fdMessage.right = new FormAttachment( 100, -2 * margin );
    fdMessage.bottom = new FormAttachment( 100, -margin );
    wMessage.setLayoutData(fdMessage);

    FormData fdMessageGroup = new FormData();
    fdMessageGroup.left = new FormAttachment( 0, margin );
    fdMessageGroup.top = new FormAttachment(wAdvancedSettings, margin );
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
    wTimeout.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    CheckuseUserTarget();
    wTabFolder.setSelection( 0 );
    BaseTransformDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "JobSNMPTrapDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }

  private void CheckuseUserTarget() {
    wComString.setEnabled( wTargetType.getSelectionIndex() == 0 );
    wUser.setEnabled( wTargetType.getSelectionIndex() == 1 );
    wPassphrase.setEnabled( wTargetType.getSelectionIndex() == 1 );
    wEngineID.setEnabled( wTargetType.getSelectionIndex() == 1 );
  }

  private void test() {
    boolean testOK = false;
    String errMsg = null;
    String hostname = variables.resolve( wServerName.getText() );
    int nrPort =
      Const.toInt( variables.resolve( "" + wPort.getText() ), ActionSNMPTrap.DEFAULT_PORT );

    try {
      UdpAddress udpAddress = new UdpAddress( InetAddress.getByName( hostname ), nrPort );
      UserTarget usertarget = new UserTarget();
      usertarget.setAddress( udpAddress );

      testOK = usertarget.getAddress().isValid();

      if ( !testOK ) {
        errMsg = BaseMessages.getString( PKG, "JobSNMPTrap.CanNotGetAddress", hostname );
      }

    } catch ( Exception e ) {
      errMsg = e.getMessage();
    }
    if ( testOK ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
      mb.setMessage( BaseMessages.getString( PKG, "JobSNMPTrap.Connected.OK", hostname ) + Const.CR );
      mb.setText( BaseMessages.getString( PKG, "JobSNMPTrap.Connected.Title.Ok" ) );
      mb.open();
    } else {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "JobSNMPTrap.Connected.NOK.ConnectionBad", hostname )
        + Const.CR + errMsg + Const.CR );
      mb.setText( BaseMessages.getString( PKG, "JobSNMPTrap.Connected.Title.Bad" ) );
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
    wPort.setText( action.getPort() );
    wOID.setText( Const.NVL( action.getOID(), "" ) );
    wTimeout.setText( "" + action.getTimeout() );
    wRetry.setText( "" + action.getRetry() );
    wComString.setText( Const.NVL( action.getComString(), "" ) );
    wMessage.setText( Const.NVL( action.getMessage(), "" ) );
    wTargetType.setText( action.getTargetTypeDesc( action.getTargetType() ) );
    wUser.setText( Const.NVL( action.getUser(), "" ) );
    wPassphrase.setText( Const.NVL( action.getPassPhrase(), "" ) );
    wEngineID.setText( Const.NVL( action.getEngineID(), "" ) );

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
      mb.setText( BaseMessages.getString( PKG, "System.TransformActionNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.ActionNameMissing.Msg" ) );
      mb.open();
      return;
    }
    action.setName( wName.getText() );
    action.setPort( wPort.getText() );
    action.setServerName( wServerName.getText() );
    action.setOID( wOID.getText() );
    action.setTimeout( wTimeout.getText() );
    action.setRetry( wTimeout.getText() );
    action.setComString( wComString.getText() );
    action.setMessage( wMessage.getText() );
    action.setTargetType( wTargetType.getText() );
    action.setUser( wUser.getText() );
    action.setPassPhrase( wPassphrase.getText() );
    action.setEngineID( wEngineID.getText() );
    dispose();
  }
}
