/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.workflow.actions.ftpdelete;

import java.net.InetAddress;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.LabelTextVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.actions.sftp.SftpClient;
import org.apache.hop.workflow.actions.util.FtpClientUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/** This dialog allows you to edit the FTP Delete action settings. */
public class ActionFtpDeleteDialog extends ActionDialog {
  private static final Class<?> PKG = ActionFtpDelete.class;

  private LabelTextVar wServerName;

  private LabelTextVar wUserName;

  private LabelTextVar wPassword;

  private TextVar wFtpDirectory;
  private Label wlFtpDirectory;

  private LabelTextVar wWildcard;

  private Button wUseProxy;

  private LabelTextVar wTimeout;

  private Button wActive;

  private ActionFtpDelete action;

  private Combo wProtocol;

  private Label wlUsePublicKey;

  private Button wUsePublicKey;

  private boolean changed;

  private Group wSocksProxy;
  private LabelTextVar wSocksProxyHost;
  private LabelTextVar wSocksProxyPort;
  private LabelTextVar wSocksProxyUsername;
  private LabelTextVar wSocksProxyPassword;

  private LabelTextVar wPort;

  private LabelTextVar wProxyHost;

  private LabelTextVar wProxyPort;

  private LabelTextVar wProxyUsername;

  private LabelTextVar wProxyPassword;

  private Button wbTestChangeFolderExists;

  private Label wlNrErrorsLessThan;
  private TextVar wNrErrorsLessThan;

  private CCombo wSuccessCondition;

  private LabelTextVar wKeyFilePass;

  private Label wlKeyFilename;

  private Button wbKeyFilename;

  private TextVar wKeyFilename;

  private Button wGetPrevious;

  private FTPClient ftpclient = null;
  private SftpClient sftpclient = null;
  private String pwdFolder = null;

  private static final String[] FILETYPES =
      new String[] {
        BaseMessages.getString(PKG, "ActionFtpDelete.Filetype.Pem"),
        BaseMessages.getString(PKG, "ActionFtpDelete.Filetype.All")
      };

  public ActionFtpDeleteDialog(
      Shell parent, ActionFtpDelete action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionFtpDelete.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionFtpDelete.Title"), action);
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod =
        e -> {
          pwdFolder = null;
          ftpclient = null;
          sftpclient = null;
          action.setChanged();
        };
    changed = action.hasChanged();

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setFont(GuiResource.getInstance().getFontDefault());
    wGeneralTab.setText(BaseMessages.getString(PKG, "ActionFtpDelete.Tab.General.Label"));

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout(generalLayout);

    // ////////////////////////
    // START OF SERVER SETTINGS GROUP///
    // /
    Group wServerSettings = new Group(wGeneralComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wServerSettings);
    wServerSettings.setText(
        BaseMessages.getString(PKG, "ActionFtpDelete.ServerSettings.Group.Label"));

    FormLayout serverSettingsgroupLayout = new FormLayout();
    serverSettingsgroupLayout.marginWidth = 10;
    serverSettingsgroupLayout.marginHeight = 10;

    wServerSettings.setLayout(serverSettingsgroupLayout);

    // Protocol
    Label wlProtocol = new Label(wServerSettings, SWT.RIGHT);
    wlProtocol.setText(BaseMessages.getString(PKG, "ActionFtpDelete.Protocol.Label"));
    PropsUi.setLook(wlProtocol);
    FormData fdlProtocol = new FormData();
    fdlProtocol.left = new FormAttachment(0, 0);
    fdlProtocol.top = new FormAttachment(0, margin);
    fdlProtocol.right = new FormAttachment(middle, -margin);
    wlProtocol.setLayoutData(fdlProtocol);
    wProtocol = new Combo(wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wProtocol.setToolTipText(BaseMessages.getString(PKG, "ActionFtpDelete.Protocol.Tooltip"));
    wProtocol.add(ActionFtpDelete.PROTOCOL_FTP);
    wProtocol.add(ActionFtpDelete.PROTOCOL_SFTP);
    PropsUi.setLook(wProtocol);
    FormData fdProtocol = new FormData();
    fdProtocol.left = new FormAttachment(middle, 0);
    fdProtocol.top = new FormAttachment(0, margin);
    fdProtocol.right = new FormAttachment(100, 0);
    wProtocol.setLayoutData(fdProtocol);
    wProtocol.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            activeFtpProtocol();
            action.setChanged();
          }
        });

    // ServerName line
    wServerName =
        new LabelTextVar(
            variables,
            wServerSettings,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtpDelete.Server.Label"),
            BaseMessages.getString(PKG, "ActionFtpDelete.Server.Tooltip"),
            false,
            false);
    PropsUi.setLook(wServerName);
    wServerName.addModifyListener(lsMod);
    FormData fdServerName = new FormData();
    fdServerName.left = new FormAttachment(0, 0);
    fdServerName.top = new FormAttachment(wProtocol, margin);
    fdServerName.right = new FormAttachment(100, 0);
    wServerName.setLayoutData(fdServerName);

    // Proxy port line
    wPort =
        new LabelTextVar(
            variables,
            wServerSettings,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtpDelete.Port.Label"),
            BaseMessages.getString(PKG, "ActionFtpDelete.Port.Tooltip"),
            false,
            false);
    PropsUi.setLook(wPort);
    wPort.addModifyListener(lsMod);
    FormData fdPort = new FormData();
    fdPort.left = new FormAttachment(0, 0);
    fdPort.top = new FormAttachment(wServerName, margin);
    fdPort.right = new FormAttachment(100, 0);
    wPort.setLayoutData(fdPort);

    // UserName line
    wUserName =
        new LabelTextVar(
            variables,
            wServerSettings,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtpDelete.User.Label"),
            BaseMessages.getString(PKG, "ActionFtpDelete.User.Tooltip"),
            false,
            false);
    PropsUi.setLook(wUserName);
    wUserName.addModifyListener(lsMod);
    FormData fdUserName = new FormData();
    fdUserName.left = new FormAttachment(0, 0);
    fdUserName.top = new FormAttachment(wPort, margin);
    fdUserName.right = new FormAttachment(100, 0);
    wUserName.setLayoutData(fdUserName);

    // Password line
    wPassword =
        new LabelTextVar(
            variables,
            wServerSettings,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtpDelete.Password.Label"),
            BaseMessages.getString(PKG, "ActionFtpDelete.Password.Tooltip"),
            true,
            false);
    PropsUi.setLook(wPassword);
    wPassword.addModifyListener(lsMod);
    FormData fdPassword = new FormData();
    fdPassword.left = new FormAttachment(0, 0);
    fdPassword.top = new FormAttachment(wUserName, margin);
    fdPassword.right = new FormAttachment(100, 0);
    wPassword.setLayoutData(fdPassword);

    // Use proxy...
    Label wlUseProxy = new Label(wServerSettings, SWT.RIGHT);
    wlUseProxy.setText(BaseMessages.getString(PKG, "ActionFtpDelete.useProxy.Label"));
    PropsUi.setLook(wlUseProxy);
    FormData fdlUseProxy = new FormData();
    fdlUseProxy.left = new FormAttachment(0, 0);
    fdlUseProxy.top = new FormAttachment(wPassword, margin);
    fdlUseProxy.right = new FormAttachment(middle, -margin);
    wlUseProxy.setLayoutData(fdlUseProxy);
    wUseProxy = new Button(wServerSettings, SWT.CHECK);
    PropsUi.setLook(wUseProxy);
    wUseProxy.setToolTipText(BaseMessages.getString(PKG, "ActionFtpDelete.useProxy.Tooltip"));
    FormData fdUseProxy = new FormData();
    fdUseProxy.left = new FormAttachment(middle, 0);
    fdUseProxy.top = new FormAttachment(wlUseProxy, 0, SWT.CENTER);
    fdUseProxy.right = new FormAttachment(100, 0);
    wUseProxy.setLayoutData(fdUseProxy);
    wUseProxy.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            activeProxy();
            action.setChanged();
          }
        });

    // Proxy host line
    wProxyHost =
        new LabelTextVar(
            variables,
            wServerSettings,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtpDelete.ProxyHost.Label"),
            BaseMessages.getString(PKG, "ActionFtpDelete.ProxyHost.Tooltip"),
            false,
            false);
    PropsUi.setLook(wProxyHost);
    wProxyHost.addModifyListener(lsMod);
    FormData fdProxyHost = new FormData();
    fdProxyHost.left = new FormAttachment(0, 0);
    fdProxyHost.top = new FormAttachment(wlUseProxy, margin);
    fdProxyHost.right = new FormAttachment(100, 0);
    wProxyHost.setLayoutData(fdProxyHost);

    // Proxy port line
    wProxyPort =
        new LabelTextVar(
            variables,
            wServerSettings,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtpDelete.ProxyPort.Label"),
            BaseMessages.getString(PKG, "ActionFtpDelete.ProxyPort.Tooltip"),
            false,
            false);
    PropsUi.setLook(wProxyPort);
    wProxyPort.addModifyListener(lsMod);
    FormData fdProxyPort = new FormData();
    fdProxyPort.left = new FormAttachment(0, 0);
    fdProxyPort.top = new FormAttachment(wProxyHost, margin);
    fdProxyPort.right = new FormAttachment(100, 0);
    wProxyPort.setLayoutData(fdProxyPort);

    // Proxy username line
    wProxyUsername =
        new LabelTextVar(
            variables,
            wServerSettings,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtpDelete.ProxyUsername.Label"),
            BaseMessages.getString(PKG, "ActionFtpDelete.ProxyUsername.Tooltip"),
            false,
            false);
    PropsUi.setLook(wProxyUsername);
    wProxyUsername.addModifyListener(lsMod);
    FormData fdProxyUsername = new FormData();
    fdProxyUsername.left = new FormAttachment(0, 0);
    fdProxyUsername.top = new FormAttachment(wProxyPort, margin);
    fdProxyUsername.right = new FormAttachment(100, 0);
    wProxyUsername.setLayoutData(fdProxyUsername);

    // Proxy password line
    wProxyPassword =
        new LabelTextVar(
            variables,
            wServerSettings,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtpDelete.ProxyPassword.Label"),
            BaseMessages.getString(PKG, "ActionFtpDelete.ProxyPassword.Tooltip"),
            true,
            false);
    PropsUi.setLook(wProxyPassword);
    wProxyPassword.addModifyListener(lsMod);
    FormData fdProxyPasswd = new FormData();
    fdProxyPasswd.left = new FormAttachment(0, 0);
    fdProxyPasswd.top = new FormAttachment(wProxyUsername, margin);
    fdProxyPasswd.right = new FormAttachment(100, 0);
    wProxyPassword.setLayoutData(fdProxyPasswd);

    // usePublicKey
    wlUsePublicKey = new Label(wServerSettings, SWT.RIGHT);
    wlUsePublicKey.setText(BaseMessages.getString(PKG, "ActionFtpDelete.usePublicKeyFiles.Label"));
    PropsUi.setLook(wlUsePublicKey);
    FormData fdlusePublicKey = new FormData();
    fdlusePublicKey.left = new FormAttachment(0, 0);
    fdlusePublicKey.top = new FormAttachment(wProxyPassword, margin);
    fdlusePublicKey.right = new FormAttachment(middle, -margin);
    wlUsePublicKey.setLayoutData(fdlusePublicKey);
    wUsePublicKey = new Button(wServerSettings, SWT.CHECK);
    wUsePublicKey.setToolTipText(
        BaseMessages.getString(PKG, "ActionFtpDelete.usePublicKeyFiles.Tooltip"));
    PropsUi.setLook(wUsePublicKey);
    FormData fdusePublicKey = new FormData();
    fdusePublicKey.left = new FormAttachment(middle, 0);
    fdusePublicKey.top = new FormAttachment(wlUsePublicKey, 0, SWT.CENTER);
    fdusePublicKey.right = new FormAttachment(100, 0);
    wUsePublicKey.setLayoutData(fdusePublicKey);
    wUsePublicKey.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            activeUsePublicKey();
            action.setChanged();
          }
        });

    // Key File
    wlKeyFilename = new Label(wServerSettings, SWT.RIGHT);
    wlKeyFilename.setText(BaseMessages.getString(PKG, "ActionFtpDelete.KeyFilename.Label"));
    PropsUi.setLook(wlKeyFilename);
    FormData fdlKeyFilename = new FormData();
    fdlKeyFilename.left = new FormAttachment(0, 0);
    fdlKeyFilename.top = new FormAttachment(wlUsePublicKey, margin);
    fdlKeyFilename.right = new FormAttachment(middle, -margin);
    wlKeyFilename.setLayoutData(fdlKeyFilename);

    wbKeyFilename = new Button(wServerSettings, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbKeyFilename);
    wbKeyFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbKeyFilename = new FormData();
    fdbKeyFilename.right = new FormAttachment(100, 0);
    fdbKeyFilename.top = new FormAttachment(wlKeyFilename, 0, SWT.CENTER);
    wbKeyFilename.setLayoutData(fdbKeyFilename);

    wKeyFilename = new TextVar(variables, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wKeyFilename.setToolTipText(BaseMessages.getString(PKG, "ActionFtpDelete.KeyFilename.Tooltip"));
    PropsUi.setLook(wKeyFilename);
    wKeyFilename.addModifyListener(lsMod);
    FormData fdKeyFilename = new FormData();
    fdKeyFilename.left = new FormAttachment(middle, 0);
    fdKeyFilename.top = new FormAttachment(wlKeyFilename, 0, SWT.CENTER);
    fdKeyFilename.right = new FormAttachment(wbKeyFilename, -margin);
    wKeyFilename.setLayoutData(fdKeyFilename);

    // Whenever something changes, set the tooltip to the expanded version:
    wKeyFilename.addModifyListener(
        e -> wKeyFilename.setToolTipText(variables.resolve(wKeyFilename.getText())));

    wbKeyFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell, wKeyFilename, variables, new String[] {"*.pem", "*"}, FILETYPES, true));

    // keyfilePass line
    wKeyFilePass =
        new LabelTextVar(
            variables,
            wServerSettings,
            BaseMessages.getString(PKG, "ActionFtpDelete.keyfilePass.Label"),
            BaseMessages.getString(PKG, "ActionFtpDelete.keyfilePass.Tooltip"),
            true);
    PropsUi.setLook(wKeyFilePass);
    wKeyFilePass.addModifyListener(lsMod);
    FormData fdkeyfilePass = new FormData();
    fdkeyfilePass.left = new FormAttachment(0, 0);
    fdkeyfilePass.top = new FormAttachment(wKeyFilename, margin);
    fdkeyfilePass.right = new FormAttachment(100, 0);
    wKeyFilePass.setLayoutData(fdkeyfilePass);

    // Test connection button
    Button wTest = new Button(wServerSettings, SWT.PUSH);
    wTest.setText(BaseMessages.getString(PKG, "ActionFtpDelete.TestConnection.Label"));
    PropsUi.setLook(wTest);
    FormData fdTest = new FormData();
    wTest.setToolTipText(BaseMessages.getString(PKG, "ActionFtpDelete.TestConnection.Tooltip"));
    fdTest.top = new FormAttachment(wKeyFilePass, margin);
    fdTest.right = new FormAttachment(100, 0);
    wTest.setLayoutData(fdTest);
    wTest.addListener(SWT.Selection, e -> test());

    FormData fdServerSettings = new FormData();
    fdServerSettings.left = new FormAttachment(0, margin);
    fdServerSettings.top = new FormAttachment(0, margin);
    fdServerSettings.right = new FormAttachment(100, -margin);
    wServerSettings.setLayoutData(fdServerSettings);
    // ///////////////////////////////////////////////////////////
    // / END OF SERVER SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment(0, 0);
    fdGeneralComp.top = new FormAttachment(0, 0);
    fdGeneralComp.right = new FormAttachment(100, 0);
    fdGeneralComp.bottom = new FormAttachment(100, 0);
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);
    PropsUi.setLook(wGeneralComp);

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF Advanced TAB ///
    // ////////////////////////

    CTabItem wFilesTab = new CTabItem(wTabFolder, SWT.NONE);
    wFilesTab.setFont(GuiResource.getInstance().getFontDefault());
    wFilesTab.setText(BaseMessages.getString(PKG, "ActionFtpDelete.Tab.Files.Label"));

    Composite wFilesComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFilesComp);

    FormLayout advancedLayout = new FormLayout();
    advancedLayout.marginWidth = 3;
    advancedLayout.marginHeight = 3;
    wFilesComp.setLayout(advancedLayout);

    // ////////////////////////
    // START OF Advanced SETTINGS GROUP///
    // /
    Group wAdvancedSettings = new Group(wFilesComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wAdvancedSettings);
    wAdvancedSettings.setText(
        BaseMessages.getString(PKG, "ActionFtpDelete.AdvancedSettings.Group.Label"));

    FormLayout advancedSettingsGroupLayout = new FormLayout();
    advancedSettingsGroupLayout.marginWidth = 10;
    advancedSettingsGroupLayout.marginHeight = 10;

    wAdvancedSettings.setLayout(advancedSettingsGroupLayout);

    // Timeout line
    wTimeout =
        new LabelTextVar(
            variables,
            wAdvancedSettings,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtpDelete.Timeout.Label"),
            BaseMessages.getString(PKG, "ActionFtpDelete.Timeout.Tooltip"),
            false,
            false);
    PropsUi.setLook(wTimeout);
    wTimeout.addModifyListener(lsMod);
    FormData fdTimeout = new FormData();
    fdTimeout.left = new FormAttachment(0, 0);
    fdTimeout.top = new FormAttachment(wActive, margin);
    fdTimeout.right = new FormAttachment(100, 0);
    wTimeout.setLayoutData(fdTimeout);

    // active connection?
    Label wlActive = new Label(wAdvancedSettings, SWT.RIGHT);
    wlActive.setText(BaseMessages.getString(PKG, "ActionFtpDelete.ActiveConns.Label"));
    PropsUi.setLook(wlActive);
    FormData fdlActive = new FormData();
    fdlActive.left = new FormAttachment(0, 0);
    fdlActive.top = new FormAttachment(wTimeout, margin);
    fdlActive.right = new FormAttachment(middle, -margin);
    wlActive.setLayoutData(fdlActive);
    wActive = new Button(wAdvancedSettings, SWT.CHECK);
    wActive.setToolTipText(BaseMessages.getString(PKG, "ActionFtpDelete.ActiveConns.Tooltip"));
    PropsUi.setLook(wActive);
    FormData fdActive = new FormData();
    fdActive.left = new FormAttachment(middle, 0);
    fdActive.top = new FormAttachment(wlActive, 0, SWT.CENTER);
    fdActive.right = new FormAttachment(100, 0);
    wActive.setLayoutData(fdActive);

    FormData fdAdvancedSettings = new FormData();
    fdAdvancedSettings.left = new FormAttachment(0, margin);
    fdAdvancedSettings.top = new FormAttachment(0, margin);
    fdAdvancedSettings.right = new FormAttachment(100, -margin);
    wAdvancedSettings.setLayoutData(fdAdvancedSettings);
    // ///////////////////////////////////////////////////////////
    // / END OF Advanced SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF Remote SETTINGS GROUP///
    // /
    Group wRemoteSettings = new Group(wFilesComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wRemoteSettings);
    wRemoteSettings.setText(
        BaseMessages.getString(PKG, "ActionFtpDelete.RemoteSettings.Group.Label"));

    FormLayout remoteSettinsgroupLayout = new FormLayout();
    remoteSettinsgroupLayout.marginWidth = 10;
    remoteSettinsgroupLayout.marginHeight = 10;

    wRemoteSettings.setLayout(remoteSettinsgroupLayout);

    // Get arguments from previous result...
    Label wlGetPrevious = new Label(wRemoteSettings, SWT.RIGHT);
    wlGetPrevious.setText(BaseMessages.getString(PKG, "ActionFtpDelete.getPrevious.Label"));
    PropsUi.setLook(wlGetPrevious);
    FormData fdlGetPrevious = new FormData();
    fdlGetPrevious.left = new FormAttachment(0, 0);
    fdlGetPrevious.top = new FormAttachment(wAdvancedSettings, margin);
    fdlGetPrevious.right = new FormAttachment(middle, -margin);
    wlGetPrevious.setLayoutData(fdlGetPrevious);
    wGetPrevious = new Button(wRemoteSettings, SWT.CHECK);
    PropsUi.setLook(wGetPrevious);
    wGetPrevious.setToolTipText(BaseMessages.getString(PKG, "ActionFtpDelete.getPrevious.Tooltip"));
    FormData fdGetPrevious = new FormData();
    fdGetPrevious.left = new FormAttachment(middle, 0);
    fdGetPrevious.top = new FormAttachment(wlGetPrevious, 0, SWT.CENTER);
    fdGetPrevious.right = new FormAttachment(100, 0);
    wGetPrevious.setLayoutData(fdGetPrevious);
    wGetPrevious.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            activeCopyFromPrevious();
            action.setChanged();
          }
        });

    // FTP directory
    wlFtpDirectory = new Label(wRemoteSettings, SWT.RIGHT);
    wlFtpDirectory.setText(BaseMessages.getString(PKG, "ActionFtpDelete.RemoteDir.Label"));
    PropsUi.setLook(wlFtpDirectory);
    FormData fdlFtpDirectory = new FormData();
    fdlFtpDirectory.left = new FormAttachment(0, 0);
    fdlFtpDirectory.top = new FormAttachment(wlGetPrevious, margin);
    fdlFtpDirectory.right = new FormAttachment(middle, -margin);
    wlFtpDirectory.setLayoutData(fdlFtpDirectory);

    // Test remote folder button ...
    wbTestChangeFolderExists = new Button(wRemoteSettings, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTestChangeFolderExists);
    wbTestChangeFolderExists.setText(
        BaseMessages.getString(PKG, "ActionFtpDelete.TestFolderExists.Label"));
    FormData fdbTestChangeFolderExists = new FormData();
    fdbTestChangeFolderExists.right = new FormAttachment(100, 0);
    fdbTestChangeFolderExists.top = new FormAttachment(wGetPrevious, margin);
    wbTestChangeFolderExists.setLayoutData(fdbTestChangeFolderExists);
    wbTestChangeFolderExists.addListener(SWT.Selection, e -> checkFtpFolder());

    wFtpDirectory =
        new TextVar(
            variables,
            wRemoteSettings,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionFtpDelete.RemoteDir.Tooltip"));
    PropsUi.setLook(wFtpDirectory);
    wFtpDirectory.addModifyListener(lsMod);
    FormData fdFtpDirectory = new FormData();
    fdFtpDirectory.left = new FormAttachment(middle, 0);
    fdFtpDirectory.top = new FormAttachment(wGetPrevious, margin);
    fdFtpDirectory.right = new FormAttachment(wbTestChangeFolderExists, -margin);
    wFtpDirectory.setLayoutData(fdFtpDirectory);

    // Wildcard line
    wWildcard =
        new LabelTextVar(
            variables,
            wRemoteSettings,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtpDelete.Wildcard.Label"),
            BaseMessages.getString(PKG, "ActionFtpDelete.Wildcard.Tooltip"),
            false,
            false);
    PropsUi.setLook(wWildcard);
    wWildcard.addModifyListener(lsMod);
    FormData fdWildcard = new FormData();
    fdWildcard.left = new FormAttachment(0, 0);
    fdWildcard.top = new FormAttachment(wFtpDirectory, margin);
    fdWildcard.right = new FormAttachment(100, 0);
    wWildcard.setLayoutData(fdWildcard);

    FormData fdRemoteSettings = new FormData();
    fdRemoteSettings.left = new FormAttachment(0, margin);
    fdRemoteSettings.top = new FormAttachment(wAdvancedSettings, margin);
    fdRemoteSettings.right = new FormAttachment(100, -margin);
    wRemoteSettings.setLayoutData(fdRemoteSettings);
    // ///////////////////////////////////////////////////////////
    // / END OF Remote SETTINGSGROUP
    // ///////////////////////////////////////////////////////////

    // SuccessOngrouping?
    // ////////////////////////
    // START OF SUCCESS ON GROUP///
    // /
    Group wSuccessOn = new Group(wFilesComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wSuccessOn);
    wSuccessOn.setText(BaseMessages.getString(PKG, "ActionFtpDelete.SuccessOn.Group.Label"));

    FormLayout successongroupLayout = new FormLayout();
    successongroupLayout.marginWidth = 10;
    successongroupLayout.marginHeight = 10;

    wSuccessOn.setLayout(successongroupLayout);

    // Success Condition
    Label wlSuccessCondition = new Label(wSuccessOn, SWT.RIGHT);
    wlSuccessCondition.setText(
        BaseMessages.getString(PKG, "ActionFtpDelete.SuccessCondition.Label") + " ");
    PropsUi.setLook(wlSuccessCondition);
    FormData fdlSuccessCondition = new FormData();
    fdlSuccessCondition.left = new FormAttachment(0, 0);
    fdlSuccessCondition.right = new FormAttachment(middle, 0);
    fdlSuccessCondition.top = new FormAttachment(wRemoteSettings, margin);
    wlSuccessCondition.setLayoutData(fdlSuccessCondition);
    wSuccessCondition = new CCombo(wSuccessOn, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wSuccessCondition.add(
        BaseMessages.getString(PKG, "ActionFtpDelete.SuccessWhenAllWorksFine.Label"));
    wSuccessCondition.add(BaseMessages.getString(PKG, "ActionFtpDelete.SuccessWhenAtLeat.Label"));
    wSuccessCondition.add(
        BaseMessages.getString(PKG, "ActionFtpDelete.SuccessWhenNrErrorsLessThan.Label"));
    wSuccessCondition.select(0); // +1: starts at -1

    PropsUi.setLook(wSuccessCondition);
    FormData fdSuccessCondition = new FormData();
    fdSuccessCondition.left = new FormAttachment(middle, 0);
    fdSuccessCondition.top = new FormAttachment(wRemoteSettings, margin);
    fdSuccessCondition.right = new FormAttachment(100, 0);
    wSuccessCondition.setLayoutData(fdSuccessCondition);
    wSuccessCondition.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            activeSuccessCondition();
          }
        });

    // Success when number of errors less than
    wlNrErrorsLessThan = new Label(wSuccessOn, SWT.RIGHT);
    wlNrErrorsLessThan.setText(
        BaseMessages.getString(PKG, "ActionFtpDelete.NrBadFormedLessThan.Label") + " ");
    PropsUi.setLook(wlNrErrorsLessThan);
    FormData fdlNrErrorsLessThan = new FormData();
    fdlNrErrorsLessThan.left = new FormAttachment(0, 0);
    fdlNrErrorsLessThan.top = new FormAttachment(wSuccessCondition, margin);
    fdlNrErrorsLessThan.right = new FormAttachment(middle, -margin);
    wlNrErrorsLessThan.setLayoutData(fdlNrErrorsLessThan);

    wNrErrorsLessThan =
        new TextVar(
            variables,
            wSuccessOn,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionFtpDelete.NrBadFormedLessThan.Tooltip"));
    PropsUi.setLook(wNrErrorsLessThan);
    wNrErrorsLessThan.addModifyListener(lsMod);
    FormData fdNrErrorsLessThan = new FormData();
    fdNrErrorsLessThan.left = new FormAttachment(middle, 0);
    fdNrErrorsLessThan.top = new FormAttachment(wSuccessCondition, margin);
    fdNrErrorsLessThan.right = new FormAttachment(100, 0);
    wNrErrorsLessThan.setLayoutData(fdNrErrorsLessThan);

    FormData fdSuccessOn = new FormData();
    fdSuccessOn.left = new FormAttachment(0, margin);
    fdSuccessOn.top = new FormAttachment(wRemoteSettings, margin);
    fdSuccessOn.right = new FormAttachment(100, -margin);
    wSuccessOn.setLayoutData(fdSuccessOn);
    // ///////////////////////////////////////////////////////////
    // / END OF Success ON GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdFilesComp = new FormData();
    fdFilesComp.left = new FormAttachment(0, 0);
    fdFilesComp.top = new FormAttachment(0, 0);
    fdFilesComp.right = new FormAttachment(100, 0);
    fdFilesComp.bottom = new FormAttachment(100, 0);
    wFilesComp.setLayoutData(fdFilesComp);

    wFilesComp.layout();
    wFilesTab.setControl(wFilesComp);
    PropsUi.setLook(wFilesComp);

    // ///////////////////////////////////////////////////////////
    // / END OF Advanced TAB
    // ///////////////////////////////////////////////////////////

    // ///////////////////////////////////////////////////////////
    // Start of Socks Proxy Tab
    // ///////////////////////////////////////////////////////////
    CTabItem wSocksProxyTab = new CTabItem(wTabFolder, SWT.NONE);
    wSocksProxyTab.setFont(GuiResource.getInstance().getFontDefault());
    wSocksProxyTab.setText(BaseMessages.getString(PKG, "ActionFtpDelete.Tab.Socks.Label"));

    Composite wSocksProxyComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wSocksProxyComp);

    FormLayout soxProxyLayout = new FormLayout();
    soxProxyLayout.marginWidth = 3;
    soxProxyLayout.marginHeight = 3;
    wSocksProxyComp.setLayout(soxProxyLayout);

    // ////////////////////////////////////////////////////////
    // Start of Proxy Group
    // ////////////////////////////////////////////////////////
    wSocksProxy = new Group(wSocksProxyComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wSocksProxy);
    wSocksProxy.setText(BaseMessages.getString(PKG, "ActionFtpDelete.SocksProxy.Group.Label"));

    FormLayout socksProxyGroupLayout = new FormLayout();
    socksProxyGroupLayout.marginWidth = 10;
    socksProxyGroupLayout.marginHeight = 10;
    wSocksProxy.setLayout(socksProxyGroupLayout);

    // host line
    wSocksProxyHost =
        new LabelTextVar(
            variables,
            wSocksProxy,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtpDelete.SocksProxyHost.Label"),
            BaseMessages.getString(PKG, "ActionFtpDelete.SocksProxyHost.Tooltip"),
            false,
            false);
    PropsUi.setLook(wSocksProxyHost);
    wSocksProxyHost.addModifyListener(lsMod);
    FormData fdSocksProxyHost = new FormData();
    fdSocksProxyHost.left = new FormAttachment(0, 0);
    fdSocksProxyHost.top = new FormAttachment(0, margin);
    fdSocksProxyHost.right = new FormAttachment(100, margin);
    wSocksProxyHost.setLayoutData(fdSocksProxyHost);

    // port line
    wSocksProxyPort =
        new LabelTextVar(
            variables,
            wSocksProxy,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtpDelete.SocksProxyPort.Label"),
            BaseMessages.getString(PKG, "ActionFtpDelete.SocksProxyPort.Tooltip"),
            false,
            false);
    PropsUi.setLook(wSocksProxyPort);
    wSocksProxyPort.addModifyListener(lsMod);
    FormData fdSocksProxyPort = new FormData();
    fdSocksProxyPort.left = new FormAttachment(0, 0);
    fdSocksProxyPort.top = new FormAttachment(wSocksProxyHost, margin);
    fdSocksProxyPort.right = new FormAttachment(100, margin);
    wSocksProxyPort.setLayoutData(fdSocksProxyPort);

    // username line
    wSocksProxyUsername =
        new LabelTextVar(
            variables,
            wSocksProxy,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtpDelete.SocksProxyUsername.Label"),
            BaseMessages.getString(PKG, "ActionFtpDelete.SocksProxyPassword.Tooltip"),
            false,
            false);
    PropsUi.setLook(wSocksProxyUsername);
    wSocksProxyUsername.addModifyListener(lsMod);
    FormData fdSocksProxyUsername = new FormData();
    fdSocksProxyUsername.left = new FormAttachment(0, 0);
    fdSocksProxyUsername.top = new FormAttachment(wSocksProxyPort, margin);
    fdSocksProxyUsername.right = new FormAttachment(100, margin);
    wSocksProxyUsername.setLayoutData(fdSocksProxyUsername);

    // password line
    wSocksProxyPassword =
        new LabelTextVar(
            variables,
            wSocksProxy,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtpDelete.SocksProxyPassword.Label"),
            BaseMessages.getString(PKG, "ActionFtpDelete.SocksProxyPassword.Tooltip"),
            true,
            false);
    PropsUi.setLook(wSocksProxyPort);
    wSocksProxyPassword.addModifyListener(lsMod);
    FormData fdSocksProxyPassword = new FormData();
    fdSocksProxyPassword.left = new FormAttachment(0, 0);
    fdSocksProxyPassword.top = new FormAttachment(wSocksProxyUsername, margin);
    fdSocksProxyPassword.right = new FormAttachment(100, margin);
    wSocksProxyPassword.setLayoutData(fdSocksProxyPassword);

    // ///////////////////////////////////////////////////////////////
    // End of socks proxy group
    // ///////////////////////////////////////////////////////////////

    FormData fdSocksProxyComp = new FormData();
    fdSocksProxyComp.left = new FormAttachment(0, margin);
    fdSocksProxyComp.top = new FormAttachment(0, margin);
    fdSocksProxyComp.right = new FormAttachment(100, -margin);
    wSocksProxy.setLayoutData(fdSocksProxyComp);

    wSocksProxyComp.layout();
    wSocksProxyTab.setControl(wSocksProxyComp);
    PropsUi.setLook(wSocksProxyComp);

    // ////////////////////////////////////////////////////////
    // End of Socks Proxy Tab
    // ////////////////////////////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wSpacer, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wCancel, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    getData();
    activeSuccessCondition();
    activeUsePublicKey();
    activeProxy();
    activeFtpProtocol();
    activeCopyFromPrevious();

    wTabFolder.setSelection(0);
    focusActionName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  private void activeCopyFromPrevious() {
    wFtpDirectory.setEnabled(!wGetPrevious.getSelection());
    wlFtpDirectory.setEnabled(!wGetPrevious.getSelection());
    wWildcard.setEnabled(!wGetPrevious.getSelection());
    wbTestChangeFolderExists.setEnabled(!wGetPrevious.getSelection());
  }

  private void activeUsePublicKey() {
    wlKeyFilename.setEnabled(wUsePublicKey.getSelection());
    wKeyFilename.setEnabled(wUsePublicKey.getSelection());
    wbKeyFilename.setEnabled(wUsePublicKey.getSelection());
    wKeyFilePass.setEnabled(wUsePublicKey.getSelection());
  }

  private void activeProxy() {
    wProxyHost.setEnabled(wUseProxy.getSelection());
    wProxyPassword.setEnabled(wUseProxy.getSelection());
    wProxyPort.setEnabled(wUseProxy.getSelection());
    wProxyUsername.setEnabled(wUseProxy.getSelection());
  }

  private void activeFtpProtocol() {
    if (wProtocol.getText().equals(ActionFtpDelete.PROTOCOL_FTP)) {
      wSocksProxy.setEnabled(true);
    } else {
      activeUsePublicKey();
      wSocksProxy.setEnabled(false);
    }
  }

  private void checkFtpFolder() {
    boolean folderexists = false;
    String errmsg = "";
    try {
      String realfoldername = variables.resolve(wFtpDirectory.getText());
      if (!Utils.isEmpty(realfoldername) && connect()) {
        if (wProtocol.getText().equals(ActionFtpDelete.PROTOCOL_FTP)) {
          ftpclient.changeWorkingDirectory(pwdFolder);
          ftpclient.changeWorkingDirectory(realfoldername);
          folderexists = true;
        } else if (wProtocol.getText().equals(ActionFtpDelete.PROTOCOL_SFTP)) {
          sftpclient.chdir(pwdFolder);
          sftpclient.chdir(realfoldername);
          folderexists = true;
        }
      }
    } catch (Exception e) {
      errmsg = e.getMessage();
    }
    if (folderexists) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      mb.setMessage(
          BaseMessages.getString(PKG, "ActionFtpDelete.FolderExists.OK", wFtpDirectory.getText())
              + Const.CR);
      mb.setText(BaseMessages.getString(PKG, "ActionFtpDelete.FolderExists.Title.Ok"));
      mb.open();
    } else {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "ActionFtpDelete.FolderExists.NOK", wFtpDirectory.getText())
              + Const.CR
              + errmsg);
      mb.setText(BaseMessages.getString(PKG, "ActionFtpDelete.FolderExists.Title.Bad"));
      mb.open();
    }
  }

  private boolean connect() {
    boolean connexion = false;
    if (wProtocol.getText().equals(ActionFtpDelete.PROTOCOL_FTP)) {
      connexion = connectToFtp();
    } else if (wProtocol.getText().equals(ActionFtpDelete.PROTOCOL_SFTP)) {
      connexion = connectToSftp();
    }
    return connexion;
  }

  private void test() {

    if (connect()) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      mb.setMessage(
          BaseMessages.getString(PKG, "ActionFtpDelete.Connected.OK", wServerName.getText())
              + Const.CR);
      mb.setText(BaseMessages.getString(PKG, "ActionFtpDelete.Connected.Title.Ok"));
      mb.open();
    }
    closeFtpConnections();
  }

  private boolean connectToFtp() {
    boolean success = false;
    String realServername = null;
    try {

      if (ftpclient == null || !ftpclient.isConnected()) {
        // Create ftp client to host:port ...
        ActionFtpDelete actionFtpDelete = new ActionFtpDelete();
        getInfo(actionFtpDelete);
        ftpclient =
            FtpClientUtil.connectAndLogin(
                LogChannel.UI, variables, actionFtpDelete, actionFtpDelete.getName());

        pwdFolder = ftpclient.printWorkingDirectory();
      }
      success = true;
    } catch (Exception e) {
      if (ftpclient != null) {
        try {
          ftpclient.quit();
        } catch (Exception ignored) {
          // We've tried quitting the FTP Client exception
          // nothing else to be done if the FTP Client was already disconnected
        }
        ftpclient = null;
        FtpClientUtil.clearSocksJvmSettings();
      }
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(
                  PKG, "ActionFtpDelete.ErrorConnect.NOK", realServername, e.getMessage())
              + Const.CR);
      mb.setText(BaseMessages.getString(PKG, "ActionFtpDelete.ErrorConnect.Title.Bad"));
      mb.open();
    }
    return success;
  }

  private boolean connectToSftp() {
    boolean retval = false;
    try {

      if (sftpclient == null) {
        // Create sftp client to host ...
        sftpclient =
            new SftpClient(
                InetAddress.getByName(variables.resolve(wServerName.getText())),
                Const.toInt(variables.resolve(wPort.getText()), 22),
                variables.resolve(wUserName.getText()));

        // login to ftp host ...
        sftpclient.login(Utils.resolvePassword(variables, wPassword.getText()));
        pwdFolder = sftpclient.pwd();
      }

      retval = true;
    } catch (Exception e) {
      if (sftpclient != null) {
        try {
          sftpclient.disconnect();
        } catch (Exception ignored) {
          // We've tried quitting the SFTP Client exception
          // nothing else to be done if the SFTP Client was already disconnected
        }
        sftpclient = null;
      }
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "ActionFtpDelete.ErrorConnect.NOK", e.getMessage())
              + Const.CR);
      mb.setText(BaseMessages.getString(PKG, "ActionFtpDelete.ErrorConnect.Title.Bad"));
      mb.open();
    }
    return retval;
  }

  private void activeSuccessCondition() {
    wlNrErrorsLessThan.setEnabled(wSuccessCondition.getSelectionIndex() != 0);
    wNrErrorsLessThan.setEnabled(wSuccessCondition.getSelectionIndex() != 0);
  }

  @Override
  public void dispose() {
    closeFtpConnections();
    super.dispose();
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.NVL(action.getName(), ""));

    wProtocol.setText(Const.NVL(action.getProtocol(), "FTP"));
    wPort.setText(Const.NVL(action.getServerPort(), ""));
    wServerName.setText(Const.NVL(action.getServerName(), ""));
    wUserName.setText(Const.NVL(action.getUserName(), ""));
    wPassword.setText(Const.NVL(action.getPassword(), ""));
    wFtpDirectory.setText(Const.NVL(action.getRemoteDirectory(), ""));
    wWildcard.setText(Const.NVL(action.getWildcard(), ""));
    wTimeout.setText("" + action.getTimeout());
    wActive.setSelection(action.isActiveConnection());

    wUseProxy.setSelection(action.isUseProxy());
    wProxyHost.setText(Const.NVL(action.getProxyHost(), ""));
    wProxyPort.setText(Const.NVL(action.getProxyPort(), ""));
    wProxyUsername.setText(Const.NVL(action.getProxyUsername(), ""));
    wProxyPassword.setText(Const.NVL(action.getProxyPassword(), ""));
    wSocksProxyHost.setText(Const.NVL(action.getSocksProxyHost(), ""));
    wSocksProxyPort.setText(Const.NVL(action.getSocksProxyPort(), ""));
    wSocksProxyUsername.setText(Const.NVL(action.getSocksProxyUsername(), ""));
    wSocksProxyPassword.setText(Const.NVL(action.getSocksProxyPassword(), ""));

    wNrErrorsLessThan.setText(Const.NVL(action.getLimitSuccess(), "10"));

    if (action.getSuccessCondition() != null) {
      if (action
          .getSuccessCondition()
          .equals(ActionFtpDelete.SUCCESS_IF_AT_LEAST_X_FILES_DOWNLOADED)) {
        wSuccessCondition.select(1);
      } else if (action.getSuccessCondition().equals(ActionFtpDelete.SUCCESS_IF_ERRORS_LESS)) {
        wSuccessCondition.select(2);
      } else {
        wSuccessCondition.select(0);
      }
    } else {
      wSuccessCondition.select(0);
    }

    wUsePublicKey.setSelection(action.isUsePublicKey());
    wKeyFilename.setText(Const.nullToEmpty(action.getKeyFilename()));
    wKeyFilePass.setText(Const.nullToEmpty(action.getKeyFilePass()));

    wGetPrevious.setSelection(action.isCopyPrevious());
  }

  @Override
  protected void onActionNameModified() {
    pwdFolder = null;
    ftpclient = null;
    sftpclient = null;
    action.setChanged();
  }

  private void cancel() {
    action.setChanged(changed);
    action = null;
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wName.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "System.TransformActionNameMissing.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "System.ActionNameMissing.Msg"));
      mb.open();
      return;
    }
    getInfo(action);

    dispose();
  }

  private void getInfo(ActionFtpDelete actionFtpDelete) {
    actionFtpDelete.setName(wName.getText());
    actionFtpDelete.setProtocol(wProtocol.getText());
    actionFtpDelete.setServerPort(wPort.getText());
    actionFtpDelete.setServerName(wServerName.getText());
    actionFtpDelete.setUserName(wUserName.getText());
    actionFtpDelete.setPassword(wPassword.getText());
    actionFtpDelete.setRemoteDirectory(wFtpDirectory.getText());
    actionFtpDelete.setWildcard(wWildcard.getText());
    actionFtpDelete.setTimeout(Const.toInt(wTimeout.getText(), 10000));

    actionFtpDelete.setUseProxy(wUseProxy.getSelection());
    actionFtpDelete.setProxyHost(wProxyHost.getText());
    actionFtpDelete.setProxyPort(wProxyPort.getText());
    actionFtpDelete.setProxyUsername(wProxyUsername.getText());
    actionFtpDelete.setProxyPassword(wProxyPassword.getText());
    actionFtpDelete.setSocksProxyHost(wSocksProxyHost.getText());
    actionFtpDelete.setSocksProxyPort(wSocksProxyPort.getText());
    actionFtpDelete.setSocksProxyUsername(wSocksProxyUsername.getText());
    actionFtpDelete.setSocksProxyPassword(wSocksProxyPassword.getText());

    actionFtpDelete.setLimitSuccess(wNrErrorsLessThan.getText());

    if (wSuccessCondition.getSelectionIndex() == 1) {
      actionFtpDelete.setSuccessCondition(ActionFtpDelete.SUCCESS_IF_AT_LEAST_X_FILES_DOWNLOADED);
    } else if (wSuccessCondition.getSelectionIndex() == 2) {
      actionFtpDelete.setSuccessCondition(ActionFtpDelete.SUCCESS_IF_ERRORS_LESS);
    } else {
      actionFtpDelete.setSuccessCondition(ActionFtpDelete.SUCCESS_IF_ALL_FILES_DOWNLOADED);
    }

    actionFtpDelete.setUsePublicKey(wUsePublicKey.getSelection());
    actionFtpDelete.setKeyFilename(wKeyFilename.getText());
    actionFtpDelete.setKeyFilePass(wKeyFilePass.getText());

    actionFtpDelete.setCopyPrevious(wGetPrevious.getSelection());
  }

  private void closeFtpConnections() {
    // Close FTP connection if necessary
    if (ftpclient != null && ftpclient.isConnected()) {
      try {
        ftpclient.quit();
        ftpclient = null;
      } catch (Exception e) {
        // Ignore close errors
      }
    }

    // Close SecureFTP connection if necessary
    if (sftpclient != null) {
      try {
        sftpclient.disconnect();
        sftpclient = null;
      } catch (Exception e) {
        // Ignore close errors
      }
    }
    FtpClientUtil.clearSocksJvmSettings();
  }
}
