/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.workflow.actions.ftpsput;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.LabelTextVar;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.workflow.actions.ftpsget.FtpsConnection;
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
 * This dialog allows you to edit the FTPS Put action settings
 *
 * @author Samatar
 * @since 10-03-2010
 */
public class ActionFtpsPutDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionFtpsPut.class; // for i18n purposes, needed by Translator!!

  private Text wName;

  private TextVar wServerName;

  private TextVar wServerPort;

  private TextVar wUserName;

  private TextVar wPassword;

  private TextVar wLocalDirectory;

  private TextVar wRemoteDirectory;

  private TextVar wWildcard;

  private Button wRemove;

  private ActionFtpsPut action;

  private Shell shell;

  private boolean changed;

  private Button wBinaryMode;

  private TextVar wTimeout;

  private Button wOnlyNew;

  private Button wActive;

  private LabelTextVar wProxyPort;

  private LabelTextVar wProxyUsername;

  private LabelTextVar wProxyPassword;

  private LabelTextVar wProxyHost;

  private CCombo wConnectionType;

  private FtpsConnection connection = null;

  public ActionFtpsPutDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = (ActionFtpsPut) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "JobFTPSPUT.Name.Default" ) );
    }
  }

  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, action );

    WorkflowMeta workflowMeta = getWorkflowMeta();
    
    ModifyListener lsMod = e -> {
      connection = null;
      action.setChanged();
    };
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobFTPSPUT.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText( BaseMessages.getString( PKG, "JobFTPSPUT.Name.Label" ) );
    props.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, -margin );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData(fdlName);
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    FormData fdName = new FormData();
    fdName.left = new FormAttachment( middle, 0 );
    fdName.top = new FormAttachment( 0, margin );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData(fdName);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText( BaseMessages.getString( PKG, "JobFTPSPUT.Tab.General.Label" ) );

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
    wServerSettings.setText( BaseMessages.getString( PKG, "JobFTPSPUT.ServerSettings.Group.Label" ) );

    FormLayout ServerSettingsgroupLayout = new FormLayout();
    ServerSettingsgroupLayout.marginWidth = 10;
    ServerSettingsgroupLayout.marginHeight = 10;

    wServerSettings.setLayout( ServerSettingsgroupLayout );

    // ServerName line
    Label wlServerName = new Label(wServerSettings, SWT.RIGHT);
    wlServerName.setText( BaseMessages.getString( PKG, "JobFTPSPUT.Server.Label" ) );
    props.setLook(wlServerName);
    FormData fdlServerName = new FormData();
    fdlServerName.left = new FormAttachment( 0, 0 );
    fdlServerName.top = new FormAttachment( wName, margin );
    fdlServerName.right = new FormAttachment( middle, 0 );
    wlServerName.setLayoutData(fdlServerName);
    wServerName = new TextVar( workflowMeta, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wServerName );
    wServerName.addModifyListener( lsMod );
    FormData fdServerName = new FormData();
    fdServerName.left = new FormAttachment( middle, margin );
    fdServerName.top = new FormAttachment( wName, margin );
    fdServerName.right = new FormAttachment( 100, 0 );
    wServerName.setLayoutData(fdServerName);

    // ServerPort line
    Label wlServerPort = new Label(wServerSettings, SWT.RIGHT);
    wlServerPort.setText( BaseMessages.getString( PKG, "JobFTPSPUT.Port.Label" ) );
    props.setLook(wlServerPort);
    FormData fdlServerPort = new FormData();
    fdlServerPort.left = new FormAttachment( 0, 0 );
    fdlServerPort.top = new FormAttachment( wServerName, margin );
    fdlServerPort.right = new FormAttachment( middle, 0 );
    wlServerPort.setLayoutData(fdlServerPort);
    wServerPort = new TextVar( workflowMeta, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wServerPort );
    wServerPort.setToolTipText( BaseMessages.getString( PKG, "JobFTPSPUT.Port.Tooltip" ) );
    wServerPort.addModifyListener( lsMod );
    FormData fdServerPort = new FormData();
    fdServerPort.left = new FormAttachment( middle, margin );
    fdServerPort.top = new FormAttachment( wServerName, margin );
    fdServerPort.right = new FormAttachment( 100, 0 );
    wServerPort.setLayoutData(fdServerPort);

    // UserName line
    Label wlUserName = new Label(wServerSettings, SWT.RIGHT);
    wlUserName.setText( BaseMessages.getString( PKG, "JobFTPSPUT.Username.Label" ) );
    props.setLook(wlUserName);
    FormData fdlUserName = new FormData();
    fdlUserName.left = new FormAttachment( 0, 0 );
    fdlUserName.top = new FormAttachment( wServerPort, margin );
    fdlUserName.right = new FormAttachment( middle, 0 );
    wlUserName.setLayoutData(fdlUserName);
    wUserName = new TextVar( workflowMeta, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUserName );
    wUserName.addModifyListener( lsMod );
    FormData fdUserName = new FormData();
    fdUserName.left = new FormAttachment( middle, margin );
    fdUserName.top = new FormAttachment( wServerPort, margin );
    fdUserName.right = new FormAttachment( 100, 0 );
    wUserName.setLayoutData(fdUserName);

    // Password line
    Label wlPassword = new Label(wServerSettings, SWT.RIGHT);
    wlPassword.setText( BaseMessages.getString( PKG, "JobFTPSPUT.Password.Label" ) );
    props.setLook(wlPassword);
    FormData fdlPassword = new FormData();
    fdlPassword.left = new FormAttachment( 0, 0 );
    fdlPassword.top = new FormAttachment( wUserName, margin );
    fdlPassword.right = new FormAttachment( middle, 0 );
    wlPassword.setLayoutData(fdlPassword);
    wPassword = new PasswordTextVar( workflowMeta, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPassword );
    wPassword.addModifyListener( lsMod );
    FormData fdPassword = new FormData();
    fdPassword.left = new FormAttachment( middle, margin );
    fdPassword.top = new FormAttachment( wUserName, margin );
    fdPassword.right = new FormAttachment( 100, 0 );
    wPassword.setLayoutData(fdPassword);

    // Proxy host line
    wProxyHost =
      new LabelTextVar(
        workflowMeta, wServerSettings, BaseMessages.getString( PKG, "JobFTPSPUT.ProxyHost.Label" ), BaseMessages
        .getString( PKG, "JobFTPSPUT.ProxyHost.Tooltip" ) );
    props.setLook( wProxyHost );
    wProxyHost.addModifyListener( lsMod );
    FormData fdProxyHost = new FormData();
    fdProxyHost.left = new FormAttachment( 0, 0 );
    fdProxyHost.top = new FormAttachment( wPassword, 2 * margin );
    fdProxyHost.right = new FormAttachment( 100, 0 );
    wProxyHost.setLayoutData(fdProxyHost);

    // Proxy port line
    wProxyPort =
      new LabelTextVar(
        workflowMeta, wServerSettings, BaseMessages.getString( PKG, "JobFTPSPUT.ProxyPort.Label" ), BaseMessages
        .getString( PKG, "JobFTPSPUT.ProxyPort.Tooltip" ) );
    props.setLook( wProxyPort );
    wProxyPort.addModifyListener( lsMod );
    FormData fdProxyPort = new FormData();
    fdProxyPort.left = new FormAttachment( 0, 0 );
    fdProxyPort.top = new FormAttachment( wProxyHost, margin );
    fdProxyPort.right = new FormAttachment( 100, 0 );
    wProxyPort.setLayoutData(fdProxyPort);

    // Proxy username line
    wProxyUsername =
      new LabelTextVar(
        workflowMeta, wServerSettings, BaseMessages.getString( PKG, "JobFTPSPUT.ProxyUsername.Label" ),
        BaseMessages.getString( PKG, "JobFTPSPUT.ProxyUsername.Tooltip" ) );
    props.setLook( wProxyUsername );
    wProxyUsername.addModifyListener( lsMod );
    FormData fdProxyUsername = new FormData();
    fdProxyUsername.left = new FormAttachment( 0, 0 );
    fdProxyUsername.top = new FormAttachment( wProxyPort, margin );
    fdProxyUsername.right = new FormAttachment( 100, 0 );
    wProxyUsername.setLayoutData(fdProxyUsername);

    // Proxy password line
    wProxyPassword =
      new LabelTextVar(
        workflowMeta, wServerSettings, BaseMessages.getString( PKG, "JobFTPSPUT.ProxyPassword.Label" ),
        BaseMessages.getString( PKG, "JobFTPSPUT.ProxyPassword.Tooltip" ), true );
    props.setLook( wProxyPassword );
    wProxyPassword.addModifyListener( lsMod );
    FormData fdProxyPasswd = new FormData();
    fdProxyPasswd.left = new FormAttachment( 0, 0 );
    fdProxyPasswd.top = new FormAttachment( wProxyUsername, margin );
    fdProxyPasswd.right = new FormAttachment( 100, 0 );
    wProxyPassword.setLayoutData(fdProxyPasswd);

    Label wlConnectionType = new Label(wServerSettings, SWT.RIGHT);
    wlConnectionType.setText( BaseMessages.getString( PKG, "JobFTPSPUT.ConnectionType.Label" ) );
    props.setLook(wlConnectionType);
    FormData fdlConnectionType = new FormData();
    fdlConnectionType.left = new FormAttachment( 0, 0 );
    fdlConnectionType.right = new FormAttachment( middle, 0 );
    fdlConnectionType.top = new FormAttachment( wProxyPassword, 2 * margin );
    wlConnectionType.setLayoutData(fdlConnectionType);
    wConnectionType = new CCombo(wServerSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wConnectionType.setItems( FtpsConnection.connection_type_Desc );
    props.setLook( wConnectionType );
    FormData fdConnectionType = new FormData();
    fdConnectionType.left = new FormAttachment( middle, margin );
    fdConnectionType.top = new FormAttachment( wProxyPassword, 2 * margin );
    fdConnectionType.right = new FormAttachment( 100, 0 );
    wConnectionType.setLayoutData(fdConnectionType);

    wConnectionType.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
      }
    } );

    // Test connection button
    Button wTest = new Button(wServerSettings, SWT.PUSH);
    wTest.setText( BaseMessages.getString( PKG, "JobFTPSPUT.TestConnection.Label" ) );
    props.setLook(wTest);
    FormData fdTest = new FormData();
    wTest.setToolTipText( BaseMessages.getString( PKG, "JobFTPSPUT.TestConnection.Tooltip" ) );
    fdTest.top = new FormAttachment( wConnectionType, margin );
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
    wAdvancedSettings.setText( BaseMessages.getString( PKG, "JobFTPSPUT.AdvancedSettings.Group.Label" ) );
    FormLayout AdvancedSettingsgroupLayout = new FormLayout();
    AdvancedSettingsgroupLayout.marginWidth = 10;
    AdvancedSettingsgroupLayout.marginHeight = 10;
    wAdvancedSettings.setLayout( AdvancedSettingsgroupLayout );

    // Binary mode selection...
    Label wlBinaryMode = new Label(wAdvancedSettings, SWT.RIGHT);
    wlBinaryMode.setText( BaseMessages.getString( PKG, "JobFTPSPUT.BinaryMode.Label" ) );
    props.setLook(wlBinaryMode);
    FormData fdlBinaryMode = new FormData();
    fdlBinaryMode.left = new FormAttachment( 0, 0 );
    fdlBinaryMode.top = new FormAttachment(wServerSettings, margin );
    fdlBinaryMode.right = new FormAttachment( middle, 0 );
    wlBinaryMode.setLayoutData(fdlBinaryMode);
    wBinaryMode = new Button(wAdvancedSettings, SWT.CHECK );
    props.setLook( wBinaryMode );
    wBinaryMode.setToolTipText( BaseMessages.getString( PKG, "JobFTPSPUT.BinaryMode.Tooltip" ) );
    FormData fdBinaryMode = new FormData();
    fdBinaryMode.left = new FormAttachment( middle, 0 );
    fdBinaryMode.top = new FormAttachment(wServerSettings, margin );
    fdBinaryMode.right = new FormAttachment( 100, 0 );
    wBinaryMode.setLayoutData(fdBinaryMode);

    // TimeOut...
    Label wlTimeout = new Label(wAdvancedSettings, SWT.RIGHT);
    wlTimeout.setText( BaseMessages.getString( PKG, "JobFTPSPUT.Timeout.Label" ) );
    props.setLook(wlTimeout);
    FormData fdlTimeout = new FormData();
    fdlTimeout.left = new FormAttachment( 0, 0 );
    fdlTimeout.top = new FormAttachment( wBinaryMode, margin );
    fdlTimeout.right = new FormAttachment( middle, 0 );
    wlTimeout.setLayoutData(fdlTimeout);
    wTimeout =
      new TextVar( workflowMeta, wAdvancedSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER, BaseMessages.getString(
        PKG, "JobFTPSPUT.Timeout.Tooltip" ) );
    props.setLook( wTimeout );
    wTimeout.setToolTipText( BaseMessages.getString( PKG, "JobFTPSPUT.Timeout.Tooltip" ) );
    FormData fdTimeout = new FormData();
    fdTimeout.left = new FormAttachment( middle, 0 );
    fdTimeout.top = new FormAttachment( wBinaryMode, margin );
    fdTimeout.right = new FormAttachment( 100, 0 );
    wTimeout.setLayoutData(fdTimeout);

    // active connection?
    Label wlActive = new Label(wAdvancedSettings, SWT.RIGHT);
    wlActive.setText( BaseMessages.getString( PKG, "JobFTPSPUT.ActiveConns.Label" ) );
    props.setLook(wlActive);
    FormData fdlActive = new FormData();
    fdlActive.left = new FormAttachment( 0, 0 );
    fdlActive.top = new FormAttachment( wTimeout, margin );
    fdlActive.right = new FormAttachment( middle, 0 );
    wlActive.setLayoutData(fdlActive);
    wActive = new Button(wAdvancedSettings, SWT.CHECK );
    wActive.setToolTipText( BaseMessages.getString( PKG, "JobFTPSPUT.ActiveConns.Tooltip" ) );
    props.setLook( wActive );
    FormData fdActive = new FormData();
    fdActive.left = new FormAttachment( middle, 0 );
    fdActive.top = new FormAttachment( wTimeout, margin );
    fdActive.right = new FormAttachment( 100, 0 );
    wActive.setLayoutData(fdActive);

    FormData fdAdvancedSettings = new FormData();
    fdAdvancedSettings.left = new FormAttachment( 0, margin );
    fdAdvancedSettings.top = new FormAttachment(wServerSettings, margin );
    fdAdvancedSettings.right = new FormAttachment( 100, -margin );
    wAdvancedSettings.setLayoutData(fdAdvancedSettings);
    // ///////////////////////////////////////////////////////////
    // / END OF Advanced SETTINGS GROUP
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
    // START OF Files TAB ///
    // ////////////////////////

    CTabItem wFilesTab = new CTabItem(wTabFolder, SWT.NONE);
    wFilesTab.setText( BaseMessages.getString( PKG, "JobFTPSPUT.Tab.Files.Label" ) );

    Composite wFilesComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wFilesComp);

    FormLayout FilesLayout = new FormLayout();
    FilesLayout.marginWidth = 3;
    FilesLayout.marginHeight = 3;
    wFilesComp.setLayout( FilesLayout );

    // ////////////////////////
    // START OF Source SETTINGS GROUP///
    // /
    Group wSourceSettings = new Group(wFilesComp, SWT.SHADOW_NONE);
    props.setLook(wSourceSettings);
    wSourceSettings.setText( BaseMessages.getString( PKG, "JobFTPSPUT.SourceSettings.Group.Label" ) );
    FormLayout SourceSettinsgroupLayout = new FormLayout();
    SourceSettinsgroupLayout.marginWidth = 10;
    SourceSettinsgroupLayout.marginHeight = 10;
    wSourceSettings.setLayout( SourceSettinsgroupLayout );

    // Local (source) directory line
    Label wlLocalDirectory = new Label(wSourceSettings, SWT.RIGHT);
    wlLocalDirectory.setText( BaseMessages.getString( PKG, "JobFTPSPUT.LocalDir.Label" ) );
    props.setLook(wlLocalDirectory);
    FormData fdlLocalDirectory = new FormData();
    fdlLocalDirectory.left = new FormAttachment( 0, 0 );
    fdlLocalDirectory.top = new FormAttachment( 0, margin );
    fdlLocalDirectory.right = new FormAttachment( middle, -margin );
    wlLocalDirectory.setLayoutData(fdlLocalDirectory);

    // Browse folders button ...
    Button wbLocalDirectory = new Button(wSourceSettings, SWT.PUSH | SWT.CENTER);
    props.setLook(wbLocalDirectory);
    wbLocalDirectory.setText( BaseMessages.getString( PKG, "JobFTPSPUT.BrowseFolders.Label" ) );
    FormData fdbLocalDirectory = new FormData();
    fdbLocalDirectory.right = new FormAttachment( 100, 0 );
    fdbLocalDirectory.top = new FormAttachment( 0, margin );
    wbLocalDirectory.setLayoutData(fdbLocalDirectory);

    wLocalDirectory =
      new TextVar( workflowMeta, wSourceSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER, BaseMessages.getString(
        PKG, "JobFTPSPUT.LocalDir.Tooltip" ) );
    props.setLook( wLocalDirectory );
    wLocalDirectory.addModifyListener( lsMod );
    FormData fdLocalDirectory = new FormData();
    fdLocalDirectory.left = new FormAttachment( middle, 0 );
    fdLocalDirectory.top = new FormAttachment( 0, margin );
    fdLocalDirectory.right = new FormAttachment(wbLocalDirectory, -margin );
    wLocalDirectory.setLayoutData(fdLocalDirectory);

    // Wildcard line
    Label wlWildcard = new Label(wSourceSettings, SWT.RIGHT);
    wlWildcard.setText( BaseMessages.getString( PKG, "JobFTPSPUT.Wildcard.Label" ) );
    props.setLook(wlWildcard);
    FormData fdlWildcard = new FormData();
    fdlWildcard.left = new FormAttachment( 0, 0 );
    fdlWildcard.top = new FormAttachment( wLocalDirectory, margin );
    fdlWildcard.right = new FormAttachment( middle, -margin );
    wlWildcard.setLayoutData(fdlWildcard);
    wWildcard =
      new TextVar( workflowMeta, wSourceSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER, BaseMessages.getString(
        PKG, "JobFTPSPUT.Wildcard.Tooltip" ) );
    props.setLook( wWildcard );
    wWildcard.addModifyListener( lsMod );
    FormData fdWildcard = new FormData();
    fdWildcard.left = new FormAttachment( middle, 0 );
    fdWildcard.top = new FormAttachment( wLocalDirectory, margin );
    fdWildcard.right = new FormAttachment( 100, 0 );
    wWildcard.setLayoutData(fdWildcard);

    // Remove files after retrieval...
    Label wlRemove = new Label(wSourceSettings, SWT.RIGHT);
    wlRemove.setText( BaseMessages.getString( PKG, "JobFTPSPUT.RemoveFiles.Label" ) );
    props.setLook(wlRemove);
    FormData fdlRemove = new FormData();
    fdlRemove.left = new FormAttachment( 0, 0 );
    fdlRemove.top = new FormAttachment( wWildcard, 2 * margin );
    fdlRemove.right = new FormAttachment( middle, -margin );
    wlRemove.setLayoutData(fdlRemove);
    wRemove = new Button(wSourceSettings, SWT.CHECK );
    props.setLook( wRemove );
    wRemove.setToolTipText( BaseMessages.getString( PKG, "JobFTPSPUT.RemoveFiles.Tooltip" ) );
    FormData fdRemove = new FormData();
    fdRemove.left = new FormAttachment( middle, 0 );
    fdRemove.top = new FormAttachment( wWildcard, 2 * margin );
    fdRemove.right = new FormAttachment( 100, 0 );
    wRemove.setLayoutData(fdRemove);

    // OnlyNew files after retrieval...
    Label wlOnlyNew = new Label(wSourceSettings, SWT.RIGHT);
    wlOnlyNew.setText( BaseMessages.getString( PKG, "JobFTPSPUT.DontOverwrite.Label" ) );
    props.setLook(wlOnlyNew);
    FormData fdlOnlyNew = new FormData();
    fdlOnlyNew.left = new FormAttachment( 0, 0 );
    fdlOnlyNew.top = new FormAttachment( wRemove, margin );
    fdlOnlyNew.right = new FormAttachment( middle, 0 );
    wlOnlyNew.setLayoutData(fdlOnlyNew);
    wOnlyNew = new Button(wSourceSettings, SWT.CHECK );
    wOnlyNew.setToolTipText( BaseMessages.getString( PKG, "JobFTPSPUT.DontOverwrite.Tooltip" ) );
    props.setLook( wOnlyNew );
    FormData fdOnlyNew = new FormData();
    fdOnlyNew.left = new FormAttachment( middle, 0 );
    fdOnlyNew.top = new FormAttachment( wRemove, margin );
    fdOnlyNew.right = new FormAttachment( 100, 0 );
    wOnlyNew.setLayoutData(fdOnlyNew);

    FormData fdSourceSettings = new FormData();
    fdSourceSettings.left = new FormAttachment( 0, margin );
    fdSourceSettings.top = new FormAttachment( 0, 2 * margin );
    fdSourceSettings.right = new FormAttachment( 100, -margin );
    wSourceSettings.setLayoutData(fdSourceSettings);
    // ///////////////////////////////////////////////////////////
    // / END OF Source SETTINGSGROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF Target SETTINGS GROUP///
    // /
    Group wTargetSettings = new Group(wFilesComp, SWT.SHADOW_NONE);
    props.setLook(wTargetSettings);
    wTargetSettings.setText( BaseMessages.getString( PKG, "JobFTPSPUT.TargetSettings.Group.Label" ) );
    FormLayout TargetSettinsgroupLayout = new FormLayout();
    TargetSettinsgroupLayout.marginWidth = 10;
    TargetSettinsgroupLayout.marginHeight = 10;
    wTargetSettings.setLayout( TargetSettinsgroupLayout );

    // Remote Directory line
    Label wlRemoteDirectory = new Label(wTargetSettings, SWT.RIGHT);
    wlRemoteDirectory.setText( BaseMessages.getString( PKG, "JobFTPSPUT.RemoteDir.Label" ) );
    props.setLook(wlRemoteDirectory);
    FormData fdlRemoteDirectory = new FormData();
    fdlRemoteDirectory.left = new FormAttachment( 0, 0 );
    fdlRemoteDirectory.top = new FormAttachment(wSourceSettings, margin );
    fdlRemoteDirectory.right = new FormAttachment( middle, -margin );
    wlRemoteDirectory.setLayoutData(fdlRemoteDirectory);

    // Test remote folder button ...
    Button wbTestRemoteDirectoryExists = new Button(wTargetSettings, SWT.PUSH | SWT.CENTER);
    props.setLook(wbTestRemoteDirectoryExists);
    wbTestRemoteDirectoryExists.setText( BaseMessages.getString( PKG, "JobFTPSPUT.TestFolderExists.Label" ) );
    FormData fdbTestRemoteDirectoryExists = new FormData();
    fdbTestRemoteDirectoryExists.right = new FormAttachment( 100, 0 );
    fdbTestRemoteDirectoryExists.top = new FormAttachment(wSourceSettings, margin );
    wbTestRemoteDirectoryExists.setLayoutData(fdbTestRemoteDirectoryExists);

    wRemoteDirectory =
      new TextVar( workflowMeta, wTargetSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER, BaseMessages.getString(
        PKG, "JobFTPSPUT.RemoteDir.Tooltip" ) );
    props.setLook( wRemoteDirectory );
    wRemoteDirectory.addModifyListener( lsMod );
    FormData fdRemoteDirectory = new FormData();
    fdRemoteDirectory.left = new FormAttachment( middle, 0 );
    fdRemoteDirectory.top = new FormAttachment(wSourceSettings, margin );
    fdRemoteDirectory.right = new FormAttachment(wbTestRemoteDirectoryExists, -margin );
    wRemoteDirectory.setLayoutData(fdRemoteDirectory);

    FormData fdTargetSettings = new FormData();
    fdTargetSettings.left = new FormAttachment( 0, margin );
    fdTargetSettings.top = new FormAttachment(wSourceSettings, margin );
    fdTargetSettings.right = new FormAttachment( 100, -margin );
    wTargetSettings.setLayoutData(fdTargetSettings);
    // ///////////////////////////////////////////////////////////
    // / END OF Target SETTINGSGROUP
    // ///////////////////////////////////////////////////////////

    FormData fdFilesComp = new FormData();
    fdFilesComp.left = new FormAttachment( 0, 0 );
    fdFilesComp.top = new FormAttachment( 0, 0 );
    fdFilesComp.right = new FormAttachment( 100, 0 );
    fdFilesComp.bottom = new FormAttachment( 100, 0 );
    wFilesComp.setLayoutData(fdFilesComp);

    wFilesComp.layout();
    wFilesTab.setControl(wFilesComp);
    props.setLook(wFilesComp);

    // ///////////////////////////////////////////////////////////
    // / END OF Files TAB
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
    Listener lsCheckRemoteFolder = e -> checkRemoteFolder(workflowMeta.environmentSubstitute(wRemoteDirectory.getText()));

    wbLocalDirectory.addListener( SWT.Selection, e-> BaseDialog.presentDirectoryDialog( shell, wLocalDirectory, workflowMeta ) );

    wCancel.addListener( SWT.Selection, lsCancel);
    wOk.addListener( SWT.Selection, lsOk);
    wbTestRemoteDirectoryExists.addListener( SWT.Selection, lsCheckRemoteFolder);

    wTest.addListener( SWT.Selection, lsTest);

    SelectionAdapter lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected(SelectionEvent e) {
        ok();
      }
    };

    wName.addSelectionListener(lsDef);
    wServerName.addSelectionListener(lsDef);
    wUserName.addSelectionListener(lsDef);
    wPassword.addSelectionListener(lsDef);
    wRemoteDirectory.addSelectionListener(lsDef);
    wLocalDirectory.addSelectionListener(lsDef);
    wWildcard.addSelectionListener(lsDef);

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
    props.setDialogSize( shell, "JobSFTPDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }

  private void closeFtpsConnection() {
    // Close FTP connection if necessary
    if ( connection != null ) {
      try {
        connection.disconnect();
      } catch ( Exception e ) {
        // Ignore errors
      }
    }
  }

  private void test() {
    if ( connectToFtp( false, null ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
      mb.setMessage( BaseMessages.getString( PKG, "JobFTPSPUT.Connected.OK", wServerName.getText() ) + Const.CR );
      mb.setText( BaseMessages.getString( PKG, "JobFTPSPUT.Connected.Title.Ok" ) );
      mb.open();
    }
  }

  private void checkRemoteFolder( String remoteFoldername ) {
    if ( !Utils.isEmpty( remoteFoldername ) ) {
      if ( connectToFtp( true, remoteFoldername ) ) {
        MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
        mb.setMessage( BaseMessages.getString( PKG, "JobFTPSPUT.FolderExists.OK", remoteFoldername ) + Const.CR );
        mb.setText( BaseMessages.getString( PKG, "JobFTPSPUT.FolderExists.Title.Ok" ) );
        mb.open();
      }
    }
  }

  private boolean connectToFtp(boolean checkfolder, String remoteFoldername ) {
    String realServername = null;
    try {
      WorkflowMeta workflowMeta = getWorkflowMeta();
    	
      realServername = workflowMeta.environmentSubstitute( wServerName.getText() );
      int realPort = Const.toInt( workflowMeta.environmentSubstitute( wServerPort.getText() ), 0 );
      String realUsername = workflowMeta.environmentSubstitute( wUserName.getText() );
      String realPassword = Utils.resolvePassword( workflowMeta, wPassword.getText() );

      if ( connection == null ) { // Create ftp client to host:port ...
        connection =
          new FtpsConnection(
            FtpsConnection.getConnectionTypeByDesc( wConnectionType.getText() ), realServername, realPort,
            realUsername, realPassword );

        if ( !Utils.isEmpty( wProxyHost.getText() ) ) {
          // Set proxy
          String realProxy_host = workflowMeta.environmentSubstitute( wProxyHost.getText() );
          String realProxy_user = workflowMeta.environmentSubstitute( wProxyUsername.getText() );
          String realProxy_pass = Utils.resolvePassword( workflowMeta, wProxyPassword.getText() );

          connection.setProxyHost( realProxy_host );
          int proxyport = Const.toInt( workflowMeta.environmentSubstitute( wProxyPort.getText() ), 990 );
          if ( proxyport != 0 ) {
            connection.setProxyPort( proxyport );
          }
          if ( !Utils.isEmpty( realProxy_user ) ) {
            connection.setProxyUser( realProxy_user );
          }
          if ( !Utils.isEmpty( realProxy_pass ) ) {
            connection.setProxyPassword( realProxy_pass );
          }
        }

        // login to FTPS host ...
        connection.connect();
      }

      if ( checkfolder ) {
        // move to spool dir ...
        if ( !Utils.isEmpty( remoteFoldername ) ) {
          String realFtpDirectory = workflowMeta.environmentSubstitute( remoteFoldername );
          return connection.isDirectoryExists( realFtpDirectory );
        }
      }
      return true;
    } catch ( Exception e ) {
      if ( connection != null ) {
        try {
          connection.disconnect();
        } catch ( Exception ignored ) {
          // We've tried quitting the FTPS Client exception
          // nothing else to be done if the FTPS Client was already disconnected
        }
        connection = null;
      }
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "JobFTPSPUT.ErrorConnect.NOK", realServername,
        e.getMessage() ) + Const.CR );
      mb.setText( BaseMessages.getString( PKG, "JobFTPSPUT.ErrorConnect.Title.Bad" ) );
      mb.open();
    }
    return false;
  }

  public void dispose() {
    closeFtpsConnection();
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( action.getName() != null ) {
      wName.setText( action.getName() );
    }

    wServerName.setText( Const.NVL( action.getServerName(), "" ) );
    wServerPort.setText( action.getServerPort() );
    wUserName.setText( Const.NVL( action.getUserName(), "" ) );
    wPassword.setText( Const.NVL( action.getPassword(), "" ) );
    wRemoteDirectory.setText( Const.NVL( action.getRemoteDirectory(), "" ) );
    wLocalDirectory.setText( Const.NVL( action.getLocalDirectory(), "" ) );
    wWildcard.setText( Const.NVL( action.getWildcard(), "" ) );
    wRemove.setSelection( action.getRemove() );
    wBinaryMode.setSelection( action.isBinaryMode() );
    wTimeout.setText( "" + action.getTimeout() );
    wOnlyNew.setSelection( action.isOnlyPuttingNewFiles() );
    wActive.setSelection( action.isActiveConnection() );

    wProxyHost.setText( Const.NVL( action.getProxyHost(), "" ) );
    wProxyPort.setText( Const.NVL( action.getProxyPort(), "" ) );
    wProxyUsername.setText( Const.NVL( action.getProxyUsername(), "" ) );
    wProxyPassword.setText( Const.NVL( action.getProxyPassword(), "" ) );
    wConnectionType.setText( FtpsConnection.getConnectionTypeDesc( action.getConnectionType() ) );

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
    action.setServerName( wServerName.getText() );
    action.setServerPort( wServerPort.getText() );
    action.setUserName( wUserName.getText() );
    action.setPassword( wPassword.getText() );
    action.setRemoteDirectory( wRemoteDirectory.getText() );
    action.setLocalDirectory( wLocalDirectory.getText() );
    action.setWildcard( wWildcard.getText() );
    action.setRemove( wRemove.getSelection() );
    action.setBinaryMode( wBinaryMode.getSelection() );
    action.setTimeout( Const.toInt( wTimeout.getText(), 10000 ) );
    action.setOnlyPuttingNewFiles( wOnlyNew.getSelection() );
    action.setActiveConnection( wActive.getSelection() );

    action.setProxyHost( wProxyHost.getText() );
    action.setProxyPort( wProxyPort.getText() );
    action.setProxyUsername( wProxyUsername.getText() );
    action.setProxyPassword( wProxyPassword.getText() );
    action.setConnectionType( FtpsConnection.getConnectionTypeByDesc( wConnectionType.getText() ) );

    dispose();
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }

}
