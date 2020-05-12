/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.workflow.actions.sendnagiospassivecheck;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.util.SocketUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.LabelText;
import org.apache.hop.ui.core.widget.LabelTextVar;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

/**
 * This dialog allows you to edit the SendNagiosPassiveCheck action settings.
 *
 * @author Samatar
 * @since 01-10-2011
 */
@PluginDialog( 
		  id = "SEND_NAGIOS_PASSIVE_CHECK", 
		  image = "SendNagiosPassiveCheck.svg", 
		  pluginType = PluginDialog.PluginType.ACTION,
		  documentationUrl = "https://www.project-hop.org/manual/latest/plugins/actions/"
)
public class ActionSendNagiosPassiveCheckDialog extends ActionDialog implements IActionDialog {
  private static Class<?> PKG = ActionSendNagiosPassiveICheck.class; // for i18n purposes, needed by Translator!!

  private LabelText wName;

  private FormData fdName;

  private LabelTextVar wServerName;

  private FormData fdServerName;

  private LabelTextVar wResponseTimeOut;

  private FormData fdResponseTimeOut;

  private LabelTextVar wPassword;

  private FormData fdPassword;

  private LabelTextVar wSenderServerName;

  private FormData fdSenderServerName;

  private LabelTextVar wSenderServiceName;

  private FormData fdSenderServiceName;

  private Button wOk, wCancel;

  private Listener lsOk, lsCancel;

  private ActionSendNagiosPassiveICheck action;

  private Shell shell;

  private SelectionAdapter lsDef;

  private boolean changed;

  private Group wServerSettings;
  private FormData fdServerSettings;

  private CTabFolder wTabFolder;
  private Composite wGeneralComp;
  private CTabItem wGeneralTab;
  private FormData fdGeneralComp;
  private FormData fdTabFolder;

  private FormData fdPort;

  private LabelTextVar wPort;

  private FormData fdwConnectionTimeOut;

  private LabelTextVar wConnectionTimeOut;

  private Button wTest;

  private FormData fdTest;

  private Listener lsTest;

  private Group wSenderSettings;
  private FormData fdSenderSettings;

  private Group wMessageGroup;
  private FormData fdMessageGroup;

  private Label wlMessage;
  private StyledTextComp wMessage;
  private FormData fdlMessage, fdMessage;

  private Label wlEncryptionMode;
  private CCombo wEncryptionMode;
  private FormData fdlEncryptionMode, fdEncryptionMode;

  private Label wlLevelMode;
  private CCombo wLevelMode;
  private FormData fdlLevelMode, fdLevelMode;

  public ActionSendNagiosPassiveCheckDialog( Shell parent, IAction action,
                                             WorkflowMeta workflowMeta ) {
    super( parent, action, workflowMeta );
    this.action = (ActionSendNagiosPassiveICheck) action;
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

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        action.setChanged();
      }
    };
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
    fdName = new FormData();
    fdName.top = new FormAttachment( 0, 0 );
    fdName.left = new FormAttachment( 0, 0 );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData( fdName );

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, PropsUi.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    wGeneralTab = new CTabItem( wTabFolder, SWT.NONE );
    wGeneralTab.setText( BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.ServerSettings.General" ) );

    wGeneralComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wGeneralComp );

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout( generalLayout );

    // ////////////////////////
    // START OF SERVER SETTINGS GROUP///
    // /
    wServerSettings = new Group( wGeneralComp, SWT.SHADOW_NONE );
    props.setLook( wServerSettings );
    wServerSettings
      .setText( BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.ServerSettings.Group.Label" ) );

    FormLayout ServerSettingsgroupLayout = new FormLayout();
    ServerSettingsgroupLayout.marginWidth = 10;
    ServerSettingsgroupLayout.marginHeight = 10;

    wServerSettings.setLayout( ServerSettingsgroupLayout );

    // ServerName line
    wServerName = new LabelTextVar( workflowMeta, wServerSettings,
      BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.Server.Label" ),
      BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.Server.Tooltip" ) );
    props.setLook( wServerName );
    wServerName.addModifyListener( lsMod );
    fdServerName = new FormData();
    fdServerName.left = new FormAttachment( 0, 0 );
    fdServerName.top = new FormAttachment( wName, margin );
    fdServerName.right = new FormAttachment( 100, 0 );
    wServerName.setLayoutData( fdServerName );

    // Server port line
    wPort = new LabelTextVar( workflowMeta, wServerSettings,
      BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.Port.Label" ),
      BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.Port.Tooltip" ) );
    props.setLook( wPort );
    wPort.addModifyListener( lsMod );
    fdPort = new FormData();
    fdPort.left = new FormAttachment( 0, 0 );
    fdPort.top = new FormAttachment( wServerName, margin );
    fdPort.right = new FormAttachment( 100, 0 );
    wPort.setLayoutData( fdPort );

    // Password String line
    wPassword =
      new LabelTextVar( workflowMeta, wServerSettings, BaseMessages.getString(
        PKG, "JobSendNagiosPassiveCheck.Password.Label" ), BaseMessages
        .getString( "JobSendNagiosPassiveCheck.Password.Tooltip" ), true );
    props.setLook( wPassword );
    wPassword.addModifyListener( lsMod );
    fdPassword = new FormData();
    fdPassword.left = new FormAttachment( 0, 0 );
    fdPassword.top = new FormAttachment( wPort, margin );
    fdPassword.right = new FormAttachment( 100, 0 );
    wPassword.setLayoutData( fdPassword );

    // Server wConnectionTimeOut line
    wConnectionTimeOut =
      new LabelTextVar( workflowMeta, wServerSettings,
        BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.ConnectionTimeOut.Label" ),
        BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.ConnectionTimeOut.Tooltip" ) );
    props.setLook( wConnectionTimeOut );
    wConnectionTimeOut.addModifyListener( lsMod );
    fdwConnectionTimeOut = new FormData();
    fdwConnectionTimeOut.left = new FormAttachment( 0, 0 );
    fdwConnectionTimeOut.top = new FormAttachment( wPassword, margin );
    fdwConnectionTimeOut.right = new FormAttachment( 100, 0 );
    wConnectionTimeOut.setLayoutData( fdwConnectionTimeOut );

    // ResponseTimeOut line
    wResponseTimeOut = new LabelTextVar( workflowMeta, wServerSettings,
      BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.ResponseTimeOut.Label" ),
      BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.ResponseTimeOut.Tooltip" ) );
    props.setLook( wResponseTimeOut );
    wResponseTimeOut.addModifyListener( lsMod );
    fdResponseTimeOut = new FormData();
    fdResponseTimeOut.left = new FormAttachment( 0, 0 );
    fdResponseTimeOut.top = new FormAttachment( wConnectionTimeOut, margin );
    fdResponseTimeOut.right = new FormAttachment( 100, 0 );
    wResponseTimeOut.setLayoutData( fdResponseTimeOut );

    // Test connection button
    wTest = new Button( wServerSettings, SWT.PUSH );
    wTest.setText( BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.TestConnection.Label" ) );
    props.setLook( wTest );
    fdTest = new FormData();
    wTest.setToolTipText( BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.TestConnection.Tooltip" ) );
    fdTest.top = new FormAttachment( wResponseTimeOut, margin );
    fdTest.right = new FormAttachment( 100, 0 );
    wTest.setLayoutData( fdTest );

    fdServerSettings = new FormData();
    fdServerSettings.left = new FormAttachment( 0, margin );
    fdServerSettings.top = new FormAttachment( wName, margin );
    fdServerSettings.right = new FormAttachment( 100, -margin );
    wServerSettings.setLayoutData( fdServerSettings );
    // ///////////////////////////////////////////////////////////
    // / END OF SERVER SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF Advanced SETTINGS GROUP///
    // /
    wSenderSettings = new Group( wGeneralComp, SWT.SHADOW_NONE );
    props.setLook( wSenderSettings );
    wSenderSettings
      .setText( BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.SenderSettings.Group.Label" ) );
    FormLayout SenderSettingsgroupLayout = new FormLayout();
    SenderSettingsgroupLayout.marginWidth = 10;
    SenderSettingsgroupLayout.marginHeight = 10;
    wSenderSettings.setLayout( SenderSettingsgroupLayout );

    // SenderServerName line
    wSenderServerName = new LabelTextVar( workflowMeta, wSenderSettings,
      BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.SenderServerName.Label" ),
      BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.SenderServerName.Tooltip" ) );
    props.setLook( wSenderServerName );
    wSenderServerName.addModifyListener( lsMod );
    fdSenderServerName = new FormData();
    fdSenderServerName.left = new FormAttachment( 0, 0 );
    fdSenderServerName.top = new FormAttachment( wServerSettings, margin );
    fdSenderServerName.right = new FormAttachment( 100, 0 );
    wSenderServerName.setLayoutData( fdSenderServerName );

    // SenderServiceName line
    wSenderServiceName = new LabelTextVar( workflowMeta, wSenderSettings,
      BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.SenderServiceName.Label" ),
      BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.SenderServiceName.Tooltip" ) );
    props.setLook( wSenderServiceName );
    wSenderServiceName.addModifyListener( lsMod );
    fdSenderServiceName = new FormData();
    fdSenderServiceName.left = new FormAttachment( 0, 0 );
    fdSenderServiceName.top = new FormAttachment( wSenderServerName, margin );
    fdSenderServiceName.right = new FormAttachment( 100, 0 );
    wSenderServiceName.setLayoutData( fdSenderServiceName );

    // Encryption mode
    wlEncryptionMode = new Label( wSenderSettings, SWT.RIGHT );
    wlEncryptionMode.setText( BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.EncryptionMode.Label" ) );
    props.setLook( wlEncryptionMode );
    fdlEncryptionMode = new FormData();
    fdlEncryptionMode.left = new FormAttachment( 0, margin );
    fdlEncryptionMode.right = new FormAttachment( middle, -margin );
    fdlEncryptionMode.top = new FormAttachment( wSenderServiceName, margin );
    wlEncryptionMode.setLayoutData( fdlEncryptionMode );
    wEncryptionMode = new CCombo( wSenderSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wEncryptionMode.setItems( ActionSendNagiosPassiveICheck.encryption_mode_Desc );

    props.setLook( wEncryptionMode );
    fdEncryptionMode = new FormData();
    fdEncryptionMode.left = new FormAttachment( middle, margin );
    fdEncryptionMode.top = new FormAttachment( wSenderServiceName, margin );
    fdEncryptionMode.right = new FormAttachment( 100, 0 );
    wEncryptionMode.setLayoutData( fdEncryptionMode );
    wEncryptionMode.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {

      }
    } );

    // Level mode
    wlLevelMode = new Label( wSenderSettings, SWT.RIGHT );
    wlLevelMode.setText( BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.LevelMode.Label" ) );
    props.setLook( wlLevelMode );
    fdlLevelMode = new FormData();
    fdlLevelMode.left = new FormAttachment( 0, margin );
    fdlLevelMode.right = new FormAttachment( middle, -margin );
    fdlLevelMode.top = new FormAttachment( wEncryptionMode, margin );
    wlLevelMode.setLayoutData( fdlLevelMode );
    wLevelMode = new CCombo( wSenderSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wLevelMode.setItems( ActionSendNagiosPassiveICheck.level_type_Desc );

    props.setLook( wLevelMode );
    fdLevelMode = new FormData();
    fdLevelMode.left = new FormAttachment( middle, margin );
    fdLevelMode.top = new FormAttachment( wEncryptionMode, margin );
    fdLevelMode.right = new FormAttachment( 100, 0 );
    wLevelMode.setLayoutData( fdLevelMode );
    wLevelMode.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {

      }
    } );

    fdSenderSettings = new FormData();
    fdSenderSettings.left = new FormAttachment( 0, margin );
    fdSenderSettings.top = new FormAttachment( wServerSettings, margin );
    fdSenderSettings.right = new FormAttachment( 100, -margin );
    wSenderSettings.setLayoutData( fdSenderSettings );
    // ///////////////////////////////////////////////////////////
    // / END OF Advanced SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF MESSAGE GROUP///
    // /
    wMessageGroup = new Group( wGeneralComp, SWT.SHADOW_NONE );
    props.setLook( wMessageGroup );
    wMessageGroup.setText( BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.MessageGroup.Group.Label" ) );
    FormLayout MessageGroupgroupLayout = new FormLayout();
    MessageGroupgroupLayout.marginWidth = 10;
    MessageGroupgroupLayout.marginHeight = 10;
    wMessageGroup.setLayout( MessageGroupgroupLayout );

    // Message line
    wlMessage = new Label( wMessageGroup, SWT.RIGHT );
    wlMessage.setText( BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.Message.Label" ) );
    props.setLook( wlMessage );
    fdlMessage = new FormData();
    fdlMessage.left = new FormAttachment( 0, 0 );
    fdlMessage.top = new FormAttachment( wSenderSettings, margin );
    fdlMessage.right = new FormAttachment( middle, -margin );
    wlMessage.setLayoutData( fdlMessage );

    wMessage =
      new StyledTextComp( workflowMeta, wMessageGroup, SWT.MULTI
        | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL, "" );
    props.setLook( wMessage );
    wMessage.addModifyListener( lsMod );
    fdMessage = new FormData();
    fdMessage.left = new FormAttachment( middle, 0 );
    fdMessage.top = new FormAttachment( wSenderSettings, margin );
    fdMessage.right = new FormAttachment( 100, -2 * margin );
    fdMessage.bottom = new FormAttachment( 100, -margin );
    wMessage.setLayoutData( fdMessage );

    fdMessageGroup = new FormData();
    fdMessageGroup.left = new FormAttachment( 0, margin );
    fdMessageGroup.top = new FormAttachment( wSenderSettings, margin );
    fdMessageGroup.right = new FormAttachment( 100, -margin );
    fdMessageGroup.bottom = new FormAttachment( 100, -margin );
    wMessageGroup.setLayoutData( fdMessageGroup );
    // ///////////////////////////////////////////////////////////
    // / END OF MESSAGE GROUP
    // ///////////////////////////////////////////////////////////

    fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( 0, 0 );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 100, 0 );
    wGeneralComp.setLayoutData( fdGeneralComp );

    wGeneralComp.layout();
    wGeneralTab.setControl( wGeneralComp );
    props.setLook( wGeneralComp );

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( 100, -50 );
    wTabFolder.setLayoutData( fdTabFolder );

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wCancel }, margin, wTabFolder );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOk = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsTest = new Listener() {
      public void handleEvent( Event e ) {
        test();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOk.addListener( SWT.Selection, lsOk );
    wTest.addListener( SWT.Selection, lsTest );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wName.addSelectionListener( lsDef );
    wServerName.addSelectionListener( lsDef );
    wResponseTimeOut.addSelectionListener( lsDef );

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
    String hostname = workflowMeta.environmentSubstitute( wServerName.getText() );
    int nrPort =
      Const.toInt(
        workflowMeta.environmentSubstitute( "" + wPort.getText() ), ActionSendNagiosPassiveICheck.DEFAULT_PORT );
    int realConnectionTimeOut = Const.toInt( workflowMeta.environmentSubstitute( wConnectionTimeOut.getText() ), -1 );

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
    wEncryptionMode.setText( ActionSendNagiosPassiveICheck.getEncryptionModeDesc( action.getEncryptionMode() ) );
    wLevelMode.setText( ActionSendNagiosPassiveICheck.getLevelDesc( action.getLevel() ) );

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
    action.setEncryptionMode( ActionSendNagiosPassiveICheck.getEncryptionModeByDesc( wEncryptionMode.getText() ) );
    action.setLevel( ActionSendNagiosPassiveICheck.getLevelByDesc( wLevelMode.getText() ) );
    action.setPassword( wPassword.getText() );

    dispose();
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }
}
