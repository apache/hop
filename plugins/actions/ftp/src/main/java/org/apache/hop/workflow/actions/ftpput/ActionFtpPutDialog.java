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

package org.apache.hop.workflow.actions.ftpput;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.logging.LogChannel;
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
import org.apache.hop.workflow.actions.util.FtpClientUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * This dialog allows you to edit the FTP Put action settings
 *
 * @author Samatar
 * @since 15-09-2007
 */
public class ActionFtpPutDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionFtpPut.class; // For Translator

  private Text wName;

  private TextVar wServerName;

  private TextVar wServerPort;

  private TextVar wUserName;

  private TextVar wPassword;

  private TextVar wLocalDirectory;

  private TextVar wRemoteDirectory;

  private TextVar wWildcard;

  private Button wRemove;

  private ActionFtpPut action;

  private Shell shell;

  private boolean changed;

  private Button wBinaryMode;

  private TextVar wTimeout;

  private Button wOnlyNew;

  private Button wActive;

  private Combo wControlEncoding;

  private LabelTextVar wProxyPort;

  private LabelTextVar wProxyUsername;

  private LabelTextVar wProxyPassword;

  private LabelTextVar wProxyHost;

  private LabelTextVar wSocksProxyHost, wSocksProxyPort, wSocksProxyUsername, wSocksProxyPassword;

  // These should not be translated, they are required to exist on all
  // platforms according to the documentation of "Charset".
  private static final String[] encodings = {
    "US-ASCII", "ISO-8859-1", "UTF-8", "UTF-16BE", "UTF-16LE", "UTF-16"
  };

  private FTPClient ftpclient = null;
  private String pwdFolder = null;

  public ActionFtpPutDialog(Shell parent, IAction action, WorkflowMeta workflowMeta) {
    super(parent, workflowMeta);
    this.action = (ActionFtpPut) action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionFtpPut.Name.Default"));
    }
  }

  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    props.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    WorkflowMeta workflowMeta = getWorkflowMeta();

    ModifyListener lsMod =
        e -> {
          ftpclient = null;
          pwdFolder = null;
          action.setChanged();
        };
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ActionFtpPut.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "ActionFtpPut.Name.Label"));
    props.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wName);
    wName.addModifyListener(lsMod);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.top = new FormAttachment(0, margin);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    // The buttons at the bottom
    //
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(
      shell, new Button[] {wOk, wCancel}, margin, null);

    // The tab folder between the name and the buttons
    //
    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText(BaseMessages.getString(PKG, "ActionFtpPut.Tab.General.Label"));

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout(generalLayout);

    // ////////////////////////
    // START OF SERVER SETTINGS GROUP///
    // /
    Group wServerSettings = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wServerSettings);
    wServerSettings.setText(BaseMessages.getString(PKG, "ActionFtpPut.ServerSettings.Group.Label"));

    FormLayout ServerSettingsgroupLayout = new FormLayout();
    ServerSettingsgroupLayout.marginWidth = 10;
    ServerSettingsgroupLayout.marginHeight = 10;

    wServerSettings.setLayout(ServerSettingsgroupLayout);

    // ServerName line
    Label wlServerName = new Label(wServerSettings, SWT.RIGHT);
    wlServerName.setText(BaseMessages.getString(PKG, "ActionFtpPut.Server.Label"));
    props.setLook(wlServerName);
    FormData fdlServerName = new FormData();
    fdlServerName.left = new FormAttachment(0, 0);
    fdlServerName.top = new FormAttachment(wName, margin);
    fdlServerName.right = new FormAttachment(middle, 0);
    wlServerName.setLayoutData(fdlServerName);
    wServerName = new TextVar(variables, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wServerName);
    wServerName.addModifyListener(lsMod);
    FormData fdServerName = new FormData();
    fdServerName.left = new FormAttachment(middle, margin);
    fdServerName.top = new FormAttachment(wName, margin);
    fdServerName.right = new FormAttachment(100, 0);
    wServerName.setLayoutData(fdServerName);

    // ServerPort line
    Label wlServerPort = new Label(wServerSettings, SWT.RIGHT);
    wlServerPort.setText(BaseMessages.getString(PKG, "ActionFtpPut.Port.Label"));
    props.setLook(wlServerPort);
    FormData fdlServerPort = new FormData();
    fdlServerPort.left = new FormAttachment(0, 0);
    fdlServerPort.top = new FormAttachment(wServerName, margin);
    fdlServerPort.right = new FormAttachment(middle, 0);
    wlServerPort.setLayoutData(fdlServerPort);
    wServerPort = new TextVar(variables, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wServerPort);
    wServerPort.setToolTipText(BaseMessages.getString(PKG, "ActionFtpPut.Port.Tooltip"));
    wServerPort.addModifyListener(lsMod);
    FormData fdServerPort = new FormData();
    fdServerPort.left = new FormAttachment(middle, margin);
    fdServerPort.top = new FormAttachment(wServerName, margin);
    fdServerPort.right = new FormAttachment(100, 0);
    wServerPort.setLayoutData(fdServerPort);

    // UserName line
    Label wlUserName = new Label(wServerSettings, SWT.RIGHT);
    wlUserName.setText(BaseMessages.getString(PKG, "ActionFtpPut.Username.Label"));
    props.setLook(wlUserName);
    FormData fdlUserName = new FormData();
    fdlUserName.left = new FormAttachment(0, 0);
    fdlUserName.top = new FormAttachment(wServerPort, margin);
    fdlUserName.right = new FormAttachment(middle, 0);
    wlUserName.setLayoutData(fdlUserName);
    wUserName = new TextVar(variables, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wUserName);
    wUserName.addModifyListener(lsMod);
    FormData fdUserName = new FormData();
    fdUserName.left = new FormAttachment(middle, margin);
    fdUserName.top = new FormAttachment(wServerPort, margin);
    fdUserName.right = new FormAttachment(100, 0);
    wUserName.setLayoutData(fdUserName);

    // Password line
    Label wlPassword = new Label(wServerSettings, SWT.RIGHT);
    wlPassword.setText(BaseMessages.getString(PKG, "ActionFtpPut.Password.Label"));
    props.setLook(wlPassword);
    FormData fdlPassword = new FormData();
    fdlPassword.left = new FormAttachment(0, 0);
    fdlPassword.top = new FormAttachment(wUserName, margin);
    fdlPassword.right = new FormAttachment(middle, 0);
    wlPassword.setLayoutData(fdlPassword);
    wPassword = new PasswordTextVar(variables, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPassword);
    wPassword.addModifyListener(lsMod);
    FormData fdPassword = new FormData();
    fdPassword.left = new FormAttachment(middle, margin);
    fdPassword.top = new FormAttachment(wUserName, margin);
    fdPassword.right = new FormAttachment(100, 0);
    wPassword.setLayoutData(fdPassword);

    // Proxy host line
    wProxyHost =
        new LabelTextVar(
            variables,
            wServerSettings,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtpPut.ProxyHost.Label"),
            BaseMessages.getString(PKG, "ActionFtpPut.ProxyHost.Tooltip"),
            false,
            false);
    props.setLook(wProxyHost);
    wProxyHost.addModifyListener(lsMod);
    FormData fdProxyHost = new FormData();
    fdProxyHost.left = new FormAttachment(0, 0);
    fdProxyHost.top = new FormAttachment(wPassword, 2 * margin);
    fdProxyHost.right = new FormAttachment(100, 0);
    wProxyHost.setLayoutData(fdProxyHost);

    // Proxy port line
    wProxyPort =
        new LabelTextVar(
            variables,
            wServerSettings,
          SWT.NONE,
          BaseMessages.getString(PKG, "ActionFtpPut.ProxyPort.Label"),
            BaseMessages.getString(PKG, "ActionFtpPut.ProxyPort.Tooltip"), false, false);
    props.setLook(wProxyPort);
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
          BaseMessages.getString(PKG, "ActionFtpPut.ProxyUsername.Label"),
            BaseMessages.getString(PKG, "ActionFtpPut.ProxyUsername.Tooltip"), false, false);
    props.setLook(wProxyUsername);
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
          BaseMessages.getString(PKG, "ActionFtpPut.ProxyPassword.Label"),
            BaseMessages.getString(PKG, "ActionFtpPut.ProxyPassword.Tooltip"),
            true, false);
    props.setLook(wProxyPassword);
    wProxyPassword.addModifyListener(lsMod);
    FormData fdProxyPasswd = new FormData();
    fdProxyPasswd.left = new FormAttachment(0, 0);
    fdProxyPasswd.top = new FormAttachment(wProxyUsername, margin);
    fdProxyPasswd.right = new FormAttachment(100, 0);
    wProxyPassword.setLayoutData(fdProxyPasswd);

    // Test connection button
    Button wTest = new Button(wServerSettings, SWT.PUSH);
    wTest.setText(BaseMessages.getString(PKG, "ActionFtpPut.TestConnection.Label"));
    props.setLook(wTest);
    FormData fdTest = new FormData();
    wTest.setToolTipText(BaseMessages.getString(PKG, "ActionFtpPut.TestConnection.Tooltip"));
    fdTest.top = new FormAttachment(wProxyPassword, margin);
    fdTest.right = new FormAttachment(100, 0);
    wTest.setLayoutData(fdTest);

    FormData fdServerSettings = new FormData();
    fdServerSettings.left = new FormAttachment(0, margin);
    fdServerSettings.top = new FormAttachment(wName, margin);
    fdServerSettings.right = new FormAttachment(100, -margin);
    wServerSettings.setLayoutData(fdServerSettings);
    // ///////////////////////////////////////////////////////////
    // / END OF SERVER SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF Advanced SETTINGS GROUP///
    // /
    Group wAdvancedSettings = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wAdvancedSettings);
    wAdvancedSettings.setText(
        BaseMessages.getString(PKG, "ActionFtpPut.AdvancedSettings.Group.Label"));
    FormLayout AdvancedSettingsgroupLayout = new FormLayout();
    AdvancedSettingsgroupLayout.marginWidth = 10;
    AdvancedSettingsgroupLayout.marginHeight = 10;
    wAdvancedSettings.setLayout(AdvancedSettingsgroupLayout);

    // Binary mode selection...
    Label wlBinaryMode = new Label(wAdvancedSettings, SWT.RIGHT);
    wlBinaryMode.setText(BaseMessages.getString(PKG, "ActionFtpPut.BinaryMode.Label"));
    props.setLook(wlBinaryMode);
    FormData fdlBinaryMode = new FormData();
    fdlBinaryMode.left = new FormAttachment(0, 0);
    fdlBinaryMode.top = new FormAttachment(wServerSettings, margin);
    fdlBinaryMode.right = new FormAttachment(middle, 0);
    wlBinaryMode.setLayoutData(fdlBinaryMode);
    wBinaryMode = new Button(wAdvancedSettings, SWT.CHECK);
    props.setLook(wBinaryMode);
    wBinaryMode.setToolTipText(BaseMessages.getString(PKG, "ActionFtpPut.BinaryMode.Tooltip"));
    FormData fdBinaryMode = new FormData();
    fdBinaryMode.left = new FormAttachment(middle, 0);
    fdBinaryMode.top = new FormAttachment(wlBinaryMode, 0, SWT.CENTER);
    fdBinaryMode.right = new FormAttachment(100, 0);
    wBinaryMode.setLayoutData(fdBinaryMode);

    // TimeOut...
    Label wlTimeout = new Label(wAdvancedSettings, SWT.RIGHT);
    wlTimeout.setText(BaseMessages.getString(PKG, "ActionFtpPut.Timeout.Label"));
    props.setLook(wlTimeout);
    FormData fdlTimeout = new FormData();
    fdlTimeout.left = new FormAttachment(0, 0);
    fdlTimeout.top = new FormAttachment(wlBinaryMode, 2 * margin);
    fdlTimeout.right = new FormAttachment(middle, 0);
    wlTimeout.setLayoutData(fdlTimeout);
    wTimeout =
        new TextVar(
            variables,
            wAdvancedSettings,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionFtpPut.Timeout.Tooltip"));
    props.setLook(wTimeout);
    wTimeout.setToolTipText(BaseMessages.getString(PKG, "ActionFtpPut.Timeout.Tooltip"));
    FormData fdTimeout = new FormData();
    fdTimeout.left = new FormAttachment(middle, 0);
    fdTimeout.top = new FormAttachment(wBinaryMode, margin);
    fdTimeout.right = new FormAttachment(100, 0);
    wTimeout.setLayoutData(fdTimeout);

    // active connection?
    Label wlActive = new Label(wAdvancedSettings, SWT.RIGHT);
    wlActive.setText(BaseMessages.getString(PKG, "ActionFtpPut.ActiveConns.Label"));
    props.setLook(wlActive);
    FormData fdlActive = new FormData();
    fdlActive.left = new FormAttachment(0, 0);
    fdlActive.top = new FormAttachment(wTimeout, margin);
    fdlActive.right = new FormAttachment(middle, 0);
    wlActive.setLayoutData(fdlActive);
    wActive = new Button(wAdvancedSettings, SWT.CHECK);
    wActive.setToolTipText(BaseMessages.getString(PKG, "ActionFtpPut.ActiveConns.Tooltip"));
    props.setLook(wActive);
    FormData fdActive = new FormData();
    fdActive.left = new FormAttachment(middle, 0);
    fdActive.top = new FormAttachment(wlActive, 0, SWT.CENTER);
    fdActive.right = new FormAttachment(100, 0);
    wActive.setLayoutData(fdActive);

    // Control encoding line
    //
    // The drop down is editable as it may happen an encoding may not be present
    // on one machine, but you may want to use it on your execution server
    //
    Label wlControlEncoding = new Label(wAdvancedSettings, SWT.RIGHT);
    wlControlEncoding.setText(BaseMessages.getString(PKG, "ActionFtpPut.ControlEncoding.Label"));
    props.setLook(wlControlEncoding);
    FormData fdlControlEncoding = new FormData();
    fdlControlEncoding.left = new FormAttachment(0, 0);
    fdlControlEncoding.top = new FormAttachment(wlActive, 2 * margin);
    fdlControlEncoding.right = new FormAttachment(middle, 0);
    wlControlEncoding.setLayoutData(fdlControlEncoding);
    wControlEncoding = new Combo(wAdvancedSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wControlEncoding.setToolTipText(
        BaseMessages.getString(PKG, "ActionFtpPut.ControlEncoding.Tooltip"));
    wControlEncoding.setItems(encodings);
    props.setLook(wControlEncoding);
    FormData fdControlEncoding = new FormData();
    fdControlEncoding.left = new FormAttachment(middle, 0);
    fdControlEncoding.top = new FormAttachment(wlControlEncoding, 0, SWT.CENTER);
    fdControlEncoding.right = new FormAttachment(100, 0);
    wControlEncoding.setLayoutData(fdControlEncoding);

    FormData fdAdvancedSettings = new FormData();
    fdAdvancedSettings.left = new FormAttachment(0, margin);
    fdAdvancedSettings.top = new FormAttachment(wServerSettings, margin);
    fdAdvancedSettings.right = new FormAttachment(100, -margin);
    wAdvancedSettings.setLayoutData(fdAdvancedSettings);
    // ///////////////////////////////////////////////////////////
    // / END OF Advanced SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment(0, 0);
    fdGeneralComp.top = new FormAttachment(0, 0);
    fdGeneralComp.right = new FormAttachment(100, 0);
    fdGeneralComp.bottom = new FormAttachment(100, 0);
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
    wFilesTab.setText(BaseMessages.getString(PKG, "ActionFtpPut.Tab.Files.Label"));

    Composite wFilesComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wFilesComp);

    FormLayout FilesLayout = new FormLayout();
    FilesLayout.marginWidth = 3;
    FilesLayout.marginHeight = 3;
    wFilesComp.setLayout(FilesLayout);

    // ////////////////////////
    // START OF Source SETTINGS GROUP///
    // /
    Group wSourceSettings = new Group(wFilesComp, SWT.SHADOW_NONE);
    props.setLook(wSourceSettings);
    wSourceSettings.setText(BaseMessages.getString(PKG, "ActionFtpPut.SourceSettings.Group.Label"));
    FormLayout SourceSettinsgroupLayout = new FormLayout();
    SourceSettinsgroupLayout.marginWidth = 10;
    SourceSettinsgroupLayout.marginHeight = 10;
    wSourceSettings.setLayout(SourceSettinsgroupLayout);

    // Local (source) directory line
    Label wlLocalDirectory = new Label(wSourceSettings, SWT.RIGHT);
    wlLocalDirectory.setText(BaseMessages.getString(PKG, "ActionFtpPut.LocalDir.Label"));
    props.setLook(wlLocalDirectory);
    FormData fdlLocalDirectory = new FormData();
    fdlLocalDirectory.left = new FormAttachment(0, 0);
    fdlLocalDirectory.top = new FormAttachment(0, margin);
    fdlLocalDirectory.right = new FormAttachment(middle, -margin);
    wlLocalDirectory.setLayoutData(fdlLocalDirectory);

    // Browse folders button ...
    Button wbLocalDirectory = new Button(wSourceSettings, SWT.PUSH | SWT.CENTER);
    props.setLook(wbLocalDirectory);
    wbLocalDirectory.setText(BaseMessages.getString(PKG, "ActionFtpPut.BrowseFolders.Label"));
    FormData fdbLocalDirectory = new FormData();
    fdbLocalDirectory.right = new FormAttachment(100, 0);
    fdbLocalDirectory.top = new FormAttachment(0, margin);
    wbLocalDirectory.setLayoutData(fdbLocalDirectory);

    wLocalDirectory =
        new TextVar(
            variables,
            wSourceSettings,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionFtpPut.LocalDir.Tooltip"));
    props.setLook(wLocalDirectory);
    wLocalDirectory.addModifyListener(lsMod);
    FormData fdLocalDirectory = new FormData();
    fdLocalDirectory.left = new FormAttachment(middle, 0);
    fdLocalDirectory.top = new FormAttachment(0, margin);
    fdLocalDirectory.right = new FormAttachment(wbLocalDirectory, -margin);
    wLocalDirectory.setLayoutData(fdLocalDirectory);

    // Wildcard line
    Label wlWildcard = new Label(wSourceSettings, SWT.RIGHT);
    wlWildcard.setText(BaseMessages.getString(PKG, "ActionFtpPut.Wildcard.Label"));
    props.setLook(wlWildcard);
    FormData fdlWildcard = new FormData();
    fdlWildcard.left = new FormAttachment(0, 0);
    fdlWildcard.top = new FormAttachment(wLocalDirectory, margin);
    fdlWildcard.right = new FormAttachment(middle, -margin);
    wlWildcard.setLayoutData(fdlWildcard);
    wWildcard =
        new TextVar(
            variables,
            wSourceSettings,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionFtpPut.Wildcard.Tooltip"));
    props.setLook(wWildcard);
    wWildcard.addModifyListener(lsMod);
    FormData fdWildcard = new FormData();
    fdWildcard.left = new FormAttachment(middle, 0);
    fdWildcard.top = new FormAttachment(wLocalDirectory, margin);
    fdWildcard.right = new FormAttachment(100, 0);
    wWildcard.setLayoutData(fdWildcard);

    // Remove files after retrieval...
    Label wlRemove = new Label(wSourceSettings, SWT.RIGHT);
    wlRemove.setText(BaseMessages.getString(PKG, "ActionFtpPut.RemoveFiles.Label"));
    props.setLook(wlRemove);
    FormData fdlRemove = new FormData();
    fdlRemove.left = new FormAttachment(0, 0);
    fdlRemove.top = new FormAttachment(wWildcard, 2 * margin);
    fdlRemove.right = new FormAttachment(middle, -margin);
    wlRemove.setLayoutData(fdlRemove);
    wRemove = new Button(wSourceSettings, SWT.CHECK);
    props.setLook(wRemove);
    wRemove.setToolTipText(BaseMessages.getString(PKG, "ActionFtpPut.RemoveFiles.Tooltip"));
    FormData fdRemove = new FormData();
    fdRemove.left = new FormAttachment(middle, 0);
    fdRemove.top = new FormAttachment(wlRemove, 0, SWT.CENTER);
    fdRemove.right = new FormAttachment(100, 0);
    wRemove.setLayoutData(fdRemove);

    // OnlyNew files after retrieval...
    Label wlOnlyNew = new Label(wSourceSettings, SWT.RIGHT);
    wlOnlyNew.setText(BaseMessages.getString(PKG, "ActionFtpPut.DontOverwrite.Label"));
    props.setLook(wlOnlyNew);
    FormData fdlOnlyNew = new FormData();
    fdlOnlyNew.left = new FormAttachment(0, 0);
    fdlOnlyNew.top = new FormAttachment(wlRemove, 2 * margin);
    fdlOnlyNew.right = new FormAttachment(middle, 0);
    wlOnlyNew.setLayoutData(fdlOnlyNew);
    wOnlyNew = new Button(wSourceSettings, SWT.CHECK);
    wOnlyNew.setToolTipText(BaseMessages.getString(PKG, "ActionFtpPut.DontOverwrite.Tooltip"));
    props.setLook(wOnlyNew);
    FormData fdOnlyNew = new FormData();
    fdOnlyNew.left = new FormAttachment(middle, 0);
    fdOnlyNew.top = new FormAttachment(wlOnlyNew, 0, SWT.CENTER);
    fdOnlyNew.right = new FormAttachment(100, 0);
    wOnlyNew.setLayoutData(fdOnlyNew);

    FormData fdSourceSettings = new FormData();
    fdSourceSettings.left = new FormAttachment(0, margin);
    fdSourceSettings.top = new FormAttachment(0, 2 * margin);
    fdSourceSettings.right = new FormAttachment(100, -margin);
    wSourceSettings.setLayoutData(fdSourceSettings);
    // ///////////////////////////////////////////////////////////
    // / END OF Source SETTINGSGROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF Target SETTINGS GROUP///
    // /
    Group wTargetSettings = new Group(wFilesComp, SWT.SHADOW_NONE);
    props.setLook(wTargetSettings);
    wTargetSettings.setText(BaseMessages.getString(PKG, "ActionFtpPut.TargetSettings.Group.Label"));
    FormLayout TargetSettinsgroupLayout = new FormLayout();
    TargetSettinsgroupLayout.marginWidth = 10;
    TargetSettinsgroupLayout.marginHeight = 10;
    wTargetSettings.setLayout(TargetSettinsgroupLayout);

    // Remote Directory line
    Label wlRemoteDirectory = new Label(wTargetSettings, SWT.RIGHT);
    wlRemoteDirectory.setText(BaseMessages.getString(PKG, "ActionFtpPut.RemoteDir.Label"));
    props.setLook(wlRemoteDirectory);
    FormData fdlRemoteDirectory = new FormData();
    fdlRemoteDirectory.left = new FormAttachment(0, 0);
    fdlRemoteDirectory.top = new FormAttachment(wSourceSettings, margin);
    fdlRemoteDirectory.right = new FormAttachment(middle, -margin);
    wlRemoteDirectory.setLayoutData(fdlRemoteDirectory);

    // Test remote folder button ...
    Button wbTestRemoteDirectoryExists = new Button(wTargetSettings, SWT.PUSH | SWT.CENTER);
    props.setLook(wbTestRemoteDirectoryExists);
    wbTestRemoteDirectoryExists.setText(
        BaseMessages.getString(PKG, "ActionFtpPut.TestFolderExists.Label"));
    FormData fdbTestRemoteDirectoryExists = new FormData();
    fdbTestRemoteDirectoryExists.right = new FormAttachment(100, 0);
    fdbTestRemoteDirectoryExists.top = new FormAttachment(wSourceSettings, margin);
    wbTestRemoteDirectoryExists.setLayoutData(fdbTestRemoteDirectoryExists);

    wRemoteDirectory =
        new TextVar(
            variables,
            wTargetSettings,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionFtpPut.RemoteDir.Tooltip"));
    props.setLook(wRemoteDirectory);
    wRemoteDirectory.addModifyListener(lsMod);
    FormData fdRemoteDirectory = new FormData();
    fdRemoteDirectory.left = new FormAttachment(middle, 0);
    fdRemoteDirectory.top = new FormAttachment(wSourceSettings, margin);
    fdRemoteDirectory.right = new FormAttachment(wbTestRemoteDirectoryExists, -margin);
    wRemoteDirectory.setLayoutData(fdRemoteDirectory);

    FormData fdTargetSettings = new FormData();
    fdTargetSettings.left = new FormAttachment(0, margin);
    fdTargetSettings.top = new FormAttachment(wSourceSettings, margin);
    fdTargetSettings.right = new FormAttachment(100, -margin);
    wTargetSettings.setLayoutData(fdTargetSettings);
    // ///////////////////////////////////////////////////////////
    // / END OF Target SETTINGSGROUP
    // ///////////////////////////////////////////////////////////

    FormData fdFilesComp = new FormData();
    fdFilesComp.left = new FormAttachment(0, 0);
    fdFilesComp.top = new FormAttachment(0, 0);
    fdFilesComp.right = new FormAttachment(100, 0);
    fdFilesComp.bottom = new FormAttachment(100, 0);
    wFilesComp.setLayoutData(fdFilesComp);

    wFilesComp.layout();
    wFilesTab.setControl(wFilesComp);
    props.setLook(wFilesComp);

    // ///////////////////////////////////////////////////////////
    // / END OF Files TAB
    // ///////////////////////////////////////////////////////////

    // ///////////////////////////////////////////////////////////
    // Start of Socks Proxy Tab
    // ///////////////////////////////////////////////////////////
    CTabItem wSocksProxyTab = new CTabItem(wTabFolder, SWT.NONE);
    wSocksProxyTab.setText(BaseMessages.getString(PKG, "ActionFtpPut.Tab.Socks.Label"));

    Composite wSocksProxyComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wSocksProxyComp);

    FormLayout SoxProxyLayout = new FormLayout();
    SoxProxyLayout.marginWidth = 3;
    SoxProxyLayout.marginHeight = 3;
    wSocksProxyComp.setLayout(SoxProxyLayout);

    // ////////////////////////////////////////////////////////
    // Start of Proxy Group
    // ////////////////////////////////////////////////////////
    Group wSocksProxy = new Group(wSocksProxyComp, SWT.SHADOW_NONE);
    props.setLook(wSocksProxy);
    wSocksProxy.setText(BaseMessages.getString(PKG, "ActionFtpPut.SocksProxy.Group.Label"));

    FormLayout SocksProxyGroupLayout = new FormLayout();
    SocksProxyGroupLayout.marginWidth = 10;
    SocksProxyGroupLayout.marginHeight = 10;
    wSocksProxy.setLayout(SocksProxyGroupLayout);

    // host line
    wSocksProxyHost =
        new LabelTextVar(
            variables,
            wSocksProxy,
          SWT.NONE,
          BaseMessages.getString(PKG, "ActionFtpPut.SocksProxyHost.Label"),
            BaseMessages.getString(PKG, "ActionFtpPut.SocksProxyHost.Tooltip"), false, false);
    props.setLook(wSocksProxyHost);
    wSocksProxyHost.addModifyListener(lsMod);
    FormData fdSocksProxyHost = new FormData();
    fdSocksProxyHost.left = new FormAttachment(0, 0);
    fdSocksProxyHost.top = new FormAttachment(wName, margin);
    fdSocksProxyHost.right = new FormAttachment(100, margin);
    wSocksProxyHost.setLayoutData(fdSocksProxyHost);

    // port line
    wSocksProxyPort =
        new LabelTextVar(
            variables,
            wSocksProxy,
          SWT.NONE,
          BaseMessages.getString(PKG, "ActionFtpPut.SocksProxyPort.Label"),
            BaseMessages.getString(PKG, "ActionFtpPut.SocksProxyPort.Tooltip"), false, false);
    props.setLook(wSocksProxyPort);
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
          BaseMessages.getString(PKG, "ActionFtpPut.SocksProxyUsername.Label"),
            BaseMessages.getString(PKG, "ActionFtpPut.SocksProxyPassword.Tooltip"), false, false);
    props.setLook(wSocksProxyUsername);
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
          BaseMessages.getString(PKG, "ActionFtpPut.SocksProxyPassword.Label"),
            BaseMessages.getString(PKG, "ActionFtpPut.SocksProxyPassword.Tooltip"),
            true, false);
    props.setLook(wSocksProxyPort);
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
    props.setLook(wSocksProxyComp);

    // ////////////////////////////////////////////////////////
    // End of Socks Proxy Tab
    // ////////////////////////////////////////////////////////
    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wName, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -2*margin);
    wTabFolder.setLayoutData(fdTabFolder);


    // Add listeners
    Listener lsTest = e -> test();
    Listener lsCheckRemoteFolder =
        e -> checkRemoteFolder(variables.resolve(wRemoteDirectory.getText()));
    wbLocalDirectory.addListener(
        SWT.Selection, e -> BaseDialog.presentDirectoryDialog(shell, wLocalDirectory, variables));

    wbTestRemoteDirectoryExists.addListener(SWT.Selection, lsCheckRemoteFolder);

    wTest.addListener(SWT.Selection, lsTest);

    SelectionAdapter lsDef =
        new SelectionAdapter() {
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
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    getData();
    wTabFolder.setSelection(0);
    BaseTransformDialog.setSize(shell);
    shell.open();
    props.setDialogSize(shell, "JobSFTPDialogSize");
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return action;
  }

  private void closeFtpConnection() {
    // Close FTP connection if necessary
    if (ftpclient != null && ftpclient.isConnected()) {
      try {
        ftpclient.quit();
        ftpclient = null;
      } catch (Exception e) {
        // Ignore errors
      }
    }
    FtpClientUtil.clearSocksJvmSettings();
  }

  private void test() {
    if (connectToFtp(false, null)) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      mb.setMessage(
          BaseMessages.getString(PKG, "ActionFtpPut.Connected.OK", wServerName.getText())
              + Const.CR);
      mb.setText(BaseMessages.getString(PKG, "ActionFtpPut.Connected.Title.Ok"));
      mb.open();
    }
    closeFtpConnection();
  }

  private void checkRemoteFolder(String remoteFoldername) {
    if (!Utils.isEmpty(remoteFoldername)) {
      if (connectToFtp(true, remoteFoldername)) {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
        mb.setMessage(
            BaseMessages.getString(PKG, "ActionFtpPut.FolderExists.OK", remoteFoldername)
                + Const.CR);
        mb.setText(BaseMessages.getString(PKG, "ActionFtpPut.FolderExists.Title.Ok"));
        mb.open();
      }
    }
  }

  private boolean connectToFtp(boolean checkFolder, String remoteFolderName) {
    boolean retval = false;
    String realServername = null;
    try {
      WorkflowMeta workflowMeta = getWorkflowMeta();
      ActionFtpPut actionFtpPut = new ActionFtpPut();
      getInfo(actionFtpPut);

      if (ftpclient == null || !ftpclient.isConnected()) {
        // Create ftp client to host:port ...
        //
        ftpclient =
            FtpClientUtil.connectAndLogin(
                LogChannel.UI, variables, actionFtpPut, actionFtpPut.getName());

        pwdFolder = ftpclient.printWorkingDirectory();
      }

      if (checkFolder) {
        if (pwdFolder != null) {
          ftpclient.changeWorkingDirectory(pwdFolder);
        }
        // move to spool dir ...
        if (!Utils.isEmpty(remoteFolderName)) {
          String realFtpDirectory = variables.resolve(remoteFolderName);
          ftpclient.changeWorkingDirectory(realFtpDirectory);
        }
      }

      retval = true;
    } catch (Exception e) {
      if (ftpclient != null) {
        try {
          ftpclient.quit();
        } catch (Exception ignored) {
          // We've tried quitting the FTP Client exception
          // nothing else can be done if the FTP Client was already disconnected
        }
        ftpclient = null;
        FtpClientUtil.clearSocksJvmSettings();
      }
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(
                  PKG, "ActionFtpPut.ErrorConnect.NOK", realServername, e.getMessage())
              + Const.CR);
      mb.setText(BaseMessages.getString(PKG, "ActionFtpPut.ErrorConnect.Title.Bad"));
      mb.open();
    }
    return retval;
  }

  public void dispose() {
    closeFtpConnection();
    WindowProperty winprop = new WindowProperty(shell);
    props.setScreen(winprop);
    shell.dispose();
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (action.getName() != null) {
      wName.setText(action.getName());
    }

    wServerName.setText(Const.NVL(action.getServerName(), ""));
    wServerPort.setText(Const.NVL(action.getServerPort(), ""));
    wUserName.setText(Const.NVL(action.getUserName(), ""));
    wPassword.setText(Const.NVL(action.getPassword(), ""));
    wRemoteDirectory.setText(Const.NVL(action.getRemoteDirectory(), ""));
    wLocalDirectory.setText(Const.NVL(action.getLocalDirectory(), ""));
    wWildcard.setText(Const.NVL(action.getWildcard(), ""));
    wRemove.setSelection(action.getRemove());
    wBinaryMode.setSelection(action.isBinaryMode());
    wTimeout.setText("" + action.getTimeout());
    wOnlyNew.setSelection(action.isOnlyPuttingNewFiles());
    wActive.setSelection(action.isActiveConnection());
    wControlEncoding.setText(action.getControlEncoding());

    wProxyHost.setText(Const.NVL(action.getProxyHost(), ""));
    wProxyPort.setText(Const.NVL(action.getProxyPort(), ""));
    wProxyUsername.setText(Const.NVL(action.getProxyUsername(), ""));
    wProxyPassword.setText(Const.NVL(action.getProxyPassword(), ""));
    wSocksProxyHost.setText(Const.NVL(action.getSocksProxyHost(), ""));
    wSocksProxyPort.setText(Const.NVL(action.getSocksProxyPort(), "1080"));
    wSocksProxyUsername.setText(Const.NVL(action.getSocksProxyUsername(), ""));
    wSocksProxyPassword.setText(Const.NVL(action.getSocksProxyPassword(), ""));

    wName.selectAll();
    wName.setFocus();
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

  private void getInfo(ActionFtpPut actionFtpPut) {

    actionFtpPut.setName(wName.getText());
    actionFtpPut.setServerName(wServerName.getText());
    actionFtpPut.setServerPort(wServerPort.getText());
    actionFtpPut.setUserName(wUserName.getText());
    actionFtpPut.setPassword(wPassword.getText());
    actionFtpPut.setRemoteDirectory(wRemoteDirectory.getText());
    actionFtpPut.setLocalDirectory(wLocalDirectory.getText());
    actionFtpPut.setWildcard(wWildcard.getText());
    actionFtpPut.setRemove(wRemove.getSelection());
    actionFtpPut.setBinaryMode(wBinaryMode.getSelection());
    actionFtpPut.setTimeout(Const.toInt(wTimeout.getText(), 10000));
    actionFtpPut.setOnlyPuttingNewFiles(wOnlyNew.getSelection());
    actionFtpPut.setActiveConnection(wActive.getSelection());
    actionFtpPut.setControlEncoding(wControlEncoding.getText());

    actionFtpPut.setProxyHost(wProxyHost.getText());
    actionFtpPut.setProxyPort(wProxyPort.getText());
    actionFtpPut.setProxyUsername(wProxyUsername.getText());
    actionFtpPut.setProxyPassword(wProxyPassword.getText());
    actionFtpPut.setSocksProxyHost(wSocksProxyHost.getText());
    actionFtpPut.setSocksProxyPort(wSocksProxyPort.getText());
    actionFtpPut.setSocksProxyUsername(wSocksProxyUsername.getText());
    actionFtpPut.setSocksProxyPassword(wSocksProxyPassword.getText());
  }
}
