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

package org.apache.hop.workflow.actions.syslog;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ComboVar;
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
 * This dialog allows you to edit the Syslog action settings.
 *
 * @author Samatar
 * @since 19-06-2003
 */
public class ActionSyslogDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionSyslog.class; // For Translator

  private LabelText wName;

  private LabelTextVar wServerName;

  private ActionSyslog action;

  private Shell shell;

  private boolean changed;

  private LabelTextVar wPort;

  private CCombo wFacility;

  private CCombo wPriority;

  private Label wlDatePattern;
  private ComboVar wDatePattern;

  private StyledTextComp wMessage;

  private Button wAddTimestamp;

  private Button wAddHostName;

  public ActionSyslogDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = (ActionSyslog) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "ActionSyslog.Name.Default" ) );
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
    shell.setText( BaseMessages.getString( PKG, "ActionSyslog.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Action name line
    wName =
      new LabelText( shell, BaseMessages.getString( PKG, "ActionSyslog.Name.Label" ), BaseMessages.getString(
        PKG, "ActionSyslog.Name.Tooltip" ) );
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
    wGeneralTab.setText( BaseMessages.getString( PKG, "ActionSyslog.Tab.General.Label" ) );

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
    wServerSettings.setText( BaseMessages.getString( PKG, "ActionSyslog.ServerSettings.Group.Label" ) );

    FormLayout ServerSettingsgroupLayout = new FormLayout();
    ServerSettingsgroupLayout.marginWidth = 10;
    ServerSettingsgroupLayout.marginHeight = 10;

    wServerSettings.setLayout( ServerSettingsgroupLayout );

    // Server port line
    wServerName =
      new LabelTextVar(
    		  variables, wServerSettings, BaseMessages.getString( PKG, "ActionSyslog.Server.Label" ), BaseMessages
        .getString( PKG, "ActionSyslog.Server.Tooltip" ) );
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
    		  variables, wServerSettings, BaseMessages.getString( PKG, "ActionSyslog.Port.Label" ), BaseMessages
        .getString( PKG, "ActionSyslog.Port.Tooltip" ) );
    props.setLook( wPort );
    wPort.addModifyListener( lsMod );
    FormData fdPort = new FormData();
    fdPort.left = new FormAttachment( 0, 0 );
    fdPort.top = new FormAttachment( wServerName, margin );
    fdPort.right = new FormAttachment( 100, 0 );
    wPort.setLayoutData(fdPort);

    // Test connection button
    Button wTest = new Button(wServerSettings, SWT.PUSH);
    wTest.setText( BaseMessages.getString( PKG, "ActionSyslog.TestConnection.Label" ) );
    props.setLook(wTest);
    FormData fdTest = new FormData();
    wTest.setToolTipText( BaseMessages.getString( PKG, "ActionSyslog.TestConnection.Tooltip" ) );
    fdTest.top = new FormAttachment( wPort, 2 * margin );
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
    // START OF Log SETTINGS GROUP///
    // /
    Group wLogSettings = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wLogSettings);
    wLogSettings.setText( BaseMessages.getString( PKG, "ActionSyslog.LogSettings.Group.Label" ) );

    FormLayout LogSettingsgroupLayout = new FormLayout();
    LogSettingsgroupLayout.marginWidth = 10;
    LogSettingsgroupLayout.marginHeight = 10;

    wLogSettings.setLayout( LogSettingsgroupLayout );

    // Facility type
    Label wlFacility = new Label(wLogSettings, SWT.RIGHT);
    wlFacility.setText( BaseMessages.getString( PKG, "ActionSyslog.Facility.Label" ) );
    props.setLook(wlFacility);
    FormData fdlFacility = new FormData();
    fdlFacility.left = new FormAttachment( 0, margin );
    fdlFacility.right = new FormAttachment( middle, -margin );
    fdlFacility.top = new FormAttachment(wServerSettings, margin );
    wlFacility.setLayoutData(fdlFacility);
    wFacility = new CCombo(wLogSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wFacility.setItems( SyslogDefs.FACILITYS );

    props.setLook( wFacility );
    FormData fdFacility = new FormData();
    fdFacility.left = new FormAttachment( middle, margin );
    fdFacility.top = new FormAttachment(wServerSettings, margin );
    fdFacility.right = new FormAttachment( 100, 0 );
    wFacility.setLayoutData(fdFacility);
    wFacility.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {

      }
    } );

    // Priority type
    Label wlPriority = new Label(wLogSettings, SWT.RIGHT);
    wlPriority.setText( BaseMessages.getString( PKG, "ActionSyslog.Priority.Label" ) );
    props.setLook(wlPriority);
    FormData fdlPriority = new FormData();
    fdlPriority.left = new FormAttachment( 0, margin );
    fdlPriority.right = new FormAttachment( middle, -margin );
    fdlPriority.top = new FormAttachment( wFacility, margin );
    wlPriority.setLayoutData(fdlPriority);
    wPriority = new CCombo(wLogSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wPriority.setItems( SyslogDefs.PRIORITYS );

    props.setLook( wPriority );
    FormData fdPriority = new FormData();
    fdPriority.left = new FormAttachment( middle, margin );
    fdPriority.top = new FormAttachment( wFacility, margin );
    fdPriority.right = new FormAttachment( 100, 0 );
    wPriority.setLayoutData(fdPriority);
    wPriority.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {

      }
    } );

    FormData fdLogSettings = new FormData();
    fdLogSettings.left = new FormAttachment( 0, margin );
    fdLogSettings.top = new FormAttachment(wServerSettings, margin );
    fdLogSettings.right = new FormAttachment( 100, -margin );
    wLogSettings.setLayoutData(fdLogSettings);
    // ///////////////////////////////////////////////////////////
    // / END OF Log SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF MESSAGE GROUP///
    // /
    Group wMessageGroup = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wMessageGroup);
    wMessageGroup.setText( BaseMessages.getString( PKG, "ActionSyslog.MessageGroup.Group.Label" ) );
    FormLayout MessageGroupgroupLayout = new FormLayout();
    MessageGroupgroupLayout.marginWidth = 10;
    MessageGroupgroupLayout.marginHeight = 10;
    wMessageGroup.setLayout( MessageGroupgroupLayout );

    // Add HostName?
    Label wlAddHostName = new Label(wMessageGroup, SWT.RIGHT);
    wlAddHostName.setText( BaseMessages.getString( PKG, "ActionSyslog.AddHostName.Label" ) );
    props.setLook(wlAddHostName);
    FormData fdlAddHostName = new FormData();
    fdlAddHostName.left = new FormAttachment( 0, 0 );
    fdlAddHostName.top = new FormAttachment(wLogSettings, margin );
    fdlAddHostName.right = new FormAttachment( middle, -margin );
    wlAddHostName.setLayoutData(fdlAddHostName);
    wAddHostName = new Button(wMessageGroup, SWT.CHECK );
    props.setLook( wAddHostName );
    wAddHostName.setToolTipText( BaseMessages.getString( PKG, "ActionSyslog.AddHostName.Tooltip" ) );
    FormData fdAddHostName = new FormData();
    fdAddHostName.left = new FormAttachment( middle, margin );
    fdAddHostName.top = new FormAttachment(wLogSettings, margin );
    fdAddHostName.right = new FormAttachment( 100, 0 );
    wAddHostName.setLayoutData(fdAddHostName);
    wAddHostName.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Add timestamp?
    Label wlAddTimestamp = new Label(wMessageGroup, SWT.RIGHT);
    wlAddTimestamp.setText( BaseMessages.getString( PKG, "ActionSyslog.AddTimestamp.Label" ) );
    props.setLook(wlAddTimestamp);
    FormData fdlAddTimestamp = new FormData();
    fdlAddTimestamp.left = new FormAttachment( 0, 0 );
    fdlAddTimestamp.top = new FormAttachment( wAddHostName, margin );
    fdlAddTimestamp.right = new FormAttachment( middle, -margin );
    wlAddTimestamp.setLayoutData(fdlAddTimestamp);
    wAddTimestamp = new Button(wMessageGroup, SWT.CHECK );
    props.setLook( wAddTimestamp );
    wAddTimestamp.setToolTipText( BaseMessages.getString( PKG, "ActionSyslog.AddTimestamp.Tooltip" ) );
    FormData fdAddTimestamp = new FormData();
    fdAddTimestamp.left = new FormAttachment( middle, margin );
    fdAddTimestamp.top = new FormAttachment( wAddHostName, margin );
    fdAddTimestamp.right = new FormAttachment( 100, 0 );
    wAddTimestamp.setLayoutData(fdAddTimestamp);
    wAddTimestamp.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        activeAddTimestamp();
        action.setChanged();
      }
    } );

    // DatePattern type
    wlDatePattern = new Label(wMessageGroup, SWT.RIGHT );
    wlDatePattern.setText( BaseMessages.getString( PKG, "ActionSyslog.DatePattern.Label" ) );
    props.setLook( wlDatePattern );
    FormData fdlDatePattern = new FormData();
    fdlDatePattern.left = new FormAttachment( 0, margin );
    fdlDatePattern.right = new FormAttachment( middle, -margin );
    fdlDatePattern.top = new FormAttachment( wAddTimestamp, margin );
    wlDatePattern.setLayoutData(fdlDatePattern);
    wDatePattern = new ComboVar( variables, wMessageGroup, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wDatePattern.setItems( Const.getDateFormats() );
    props.setLook( wDatePattern );
    FormData fdDatePattern = new FormData();
    fdDatePattern.left = new FormAttachment( middle, margin );
    fdDatePattern.top = new FormAttachment( wAddTimestamp, margin );
    fdDatePattern.right = new FormAttachment( 100, 0 );
    wDatePattern.setLayoutData(fdDatePattern);
    wDatePattern.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {

      }
    } );

    // Message line
    Label wlMessage = new Label(wMessageGroup, SWT.RIGHT);
    wlMessage.setText( BaseMessages.getString( PKG, "ActionSyslog.Message.Label" ) );
    props.setLook(wlMessage);
    FormData fdlMessage = new FormData();
    fdlMessage.left = new FormAttachment( 0, margin );
    fdlMessage.top = new FormAttachment(wLogSettings, margin );
    fdlMessage.right = new FormAttachment( middle, -margin );
    wlMessage.setLayoutData(fdlMessage);

    wMessage = new StyledTextComp( action, wMessageGroup, SWT.MULTI
        | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL );
    props.setLook( wMessage );
    wMessage.addModifyListener( lsMod );
    FormData fdMessage = new FormData();
    fdMessage.left = new FormAttachment( middle, margin );
    fdMessage.top = new FormAttachment( wDatePattern, margin );
    fdMessage.right = new FormAttachment( 100, -2 * margin );
    fdMessage.bottom = new FormAttachment( 100, -margin );
    wMessage.setLayoutData(fdMessage);

    FormData fdMessageGroup = new FormData();
    fdMessageGroup.left = new FormAttachment( 0, margin );
    fdMessageGroup.top = new FormAttachment(wLogSettings, margin );
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

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    activeAddTimestamp();
    wTabFolder.setSelection( 0 );
    BaseTransformDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "ActionSyslogDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }

  private void activeAddTimestamp() {
    wlDatePattern.setEnabled( wAddTimestamp.getSelection() );
    wDatePattern.setEnabled( wAddTimestamp.getSelection() );
  }

  private void test() {
    boolean testOK = false;
    String errMsg = null;
    String hostname = variables.resolve( wServerName.getText() );
    int nrPort = Const.toInt( variables.resolve( "" + wPort.getText() ), SyslogDefs.DEFAULT_PORT );

    try {
      UdpAddress udpAddress = new UdpAddress( InetAddress.getByName( hostname ), nrPort );
      UserTarget usertarget = new UserTarget();
      usertarget.setAddress( udpAddress );

      testOK = usertarget.getAddress().isValid();

      if ( !testOK ) {
        errMsg = BaseMessages.getString( PKG, "ActionSyslog.CanNotGetAddress", hostname );
      }

    } catch ( Exception e ) {
      errMsg = e.getMessage();
    }
    if ( testOK ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
      mb.setMessage( BaseMessages.getString( PKG, "ActionSyslog.Connected.OK", hostname ) + Const.CR );
      mb.setText( BaseMessages.getString( PKG, "ActionSyslog.Connected.Title.Ok" ) );
      mb.open();
    } else {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "ActionSyslog.Connected.NOK.ConnectionBad", hostname )
        + Const.CR + errMsg + Const.CR );
      mb.setText( BaseMessages.getString( PKG, "ActionSyslog.Connected.Title.Bad" ) );
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
    wPort.setText( Const.NVL( action.getPort(), String.valueOf( SyslogDefs.DEFAULT_PORT ) ) );
    if ( action.getFacility() != null ) {
      wFacility.setText( action.getFacility() );
    }
    if ( action.getPriority() != null ) {
      wPriority.setText( action.getPriority() );
    }
    if ( action.getMessage() != null ) {
      wMessage.setText( action.getMessage() );
    }
    if ( action.getDatePattern() != null ) {
      wDatePattern.setText( action.getDatePattern() );
    }
    wAddTimestamp.setSelection( action.isAddTimestamp() );
    wAddHostName.setSelection( action.isAddHostName() );

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
      mb.setMessage( BaseMessages.getString( PKG, "ActionSyslog.PleaseGiveActionAName.Message" ) );
      mb.setText( BaseMessages.getString( PKG, "ActionSyslog.PleaseGiveActionAName.Title" ) );
      mb.open();
      return;
    }
    action.setName( wName.getText() );
    action.setPort( wPort.getText() );
    action.setServerName( wServerName.getText() );
    action.setFacility( wFacility.getText() );
    action.setPriority( wPriority.getText() );
    action.setMessage( wMessage.getText() );
    action.addTimestamp( wAddTimestamp.getSelection() );
    action.setDatePattern( wDatePattern.getText() );
    action.addHostName( wAddHostName.getSelection() );
    dispose();
  }
}
