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

package org.apache.hop.workflow.actions.ftp;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.LabelText;
import org.apache.hop.ui.core.widget.LabelTextVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.workflow.actions.util.FtpClientUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
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

/**
 * This dialog allows you to edit the FTP Get action settings.
 *
 * @author Matt
 * @since 19-06-2003
 */
public class ActionFtpDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionFtp.class; // For Translator

  private LabelText wName;

  private LabelTextVar wServerName;

  private LabelTextVar wSocksProxyHost, wSocksProxyPort, wSocksProxyUsername, wSocksProxyPassword;

  private LabelTextVar wUserName;

  private LabelTextVar wPassword;

  private TextVar wFtpDirectory;

  private LabelTextVar wWildcard;

  private Button wBinaryMode;

  private LabelTextVar wTimeout;

  private Button wRemove;

  private Button wOnlyNew;

  private Button wActive;

  private ActionFtp action;

  private Shell shell;

  private Combo wControlEncoding;

  private boolean changed;

  private Button wMove;

  private Label wlMoveToDirectory;

  private TextVar wMoveToDirectory;

  private Button wSpecifyFormat;

  private Label wlDateTimeFormat;
  private CCombo wDateTimeFormat;

  private Label wlAddDate;
  private Button wAddDate;

  private Label wlAddTime;
  private Button wAddTime;

  private Label wlAddDateBeforeExtension;
  private Button wAddDateBeforeExtension;

  private LabelTextVar wProxyHost;

  private LabelTextVar wPort;

  private LabelTextVar wProxyPort;

  private LabelTextVar wProxyUsername;

  private LabelTextVar wProxyPassword;

  private Button wAddFilenameToResult;

  private TextVar wTargetDirectory;

  private Label wlIfFileExists;
  private CCombo wIfFileExists;

  private Button wbTestFolderExists;

  private Button wCreateMoveFolder;
  private Label wlCreateMoveFolder;

  private Label wlNrErrorsLessThan;
  private TextVar wNrErrorsLessThan;

  private CCombo wSuccessCondition;

  private FTPClient ftpclient = null;
  private String pwdFolder = null;

  // These should not be translated, they are required to exist on all
  // platforms according to the documentation of "Charset".
  private static final String[] encodings = {
    "US-ASCII", "ISO-8859-1", "UTF-8", "UTF-16BE", "UTF-16LE", "UTF-16"
  };

  //
  // Original code used to fill encodings, this display all possibilities but
  // takes 10 seconds on my pc to fill.
  //
  // static {
  // SortedMap charsetMap = Charset.availableCharsets();
  // Set charsetSet = charsetMap.keySet();
  // encodings = (String [])charsetSet.toArray(new String[0]);
  // }

  public ActionFtpDialog(Shell parent, IAction action, WorkflowMeta workflowMeta) {
    super(parent, workflowMeta);
    this.action = (ActionFtp) action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionFtp.Name.Default"));
    }
  }

  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    props.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    ModifyListener lsMod =
        e -> {
          pwdFolder = null;
          ftpclient = null;
          action.setChanged();
        };
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ActionFtp.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Action name line
    wName =
        new LabelText(
            shell,
            BaseMessages.getString(PKG, "ActionFtp.Name.Label"),
            BaseMessages.getString(PKG, "ActionFtp.Name.Tooltip"));
    wName.addModifyListener(lsMod);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(0, 0);
    fdName.left = new FormAttachment(0, 0);
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
    wGeneralTab.setText(BaseMessages.getString(PKG, "ActionFtp.Tab.General.Label"));

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout(generalLayout);

    // ////////////////////////
    // START OF SERVER SETTINGS GROUP///
    // /
    Group wgServerSettings = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wgServerSettings);
    wgServerSettings.setText(BaseMessages.getString(PKG, "ActionFtp.ServerSettings.Group.Label"));

    FormLayout serverSettingsGroupLayout = new FormLayout();
    serverSettingsGroupLayout.marginWidth = 10;
    serverSettingsGroupLayout.marginHeight = 10;

    wgServerSettings.setLayout(serverSettingsGroupLayout);

    // ServerName line
    wServerName =
        new LabelTextVar(
            variables,
            wgServerSettings,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtp.Server.Label"),
            BaseMessages.getString(PKG, "ActionFtp.Server.Tooltip"),
            false,
            false);
    props.setLook(wServerName);
    wServerName.addModifyListener(lsMod);
    FormData fdServerName = new FormData();
    fdServerName.left = new FormAttachment(0, 0);
    fdServerName.top = new FormAttachment(wName, margin);
    fdServerName.right = new FormAttachment(100, 0);
    wServerName.setLayoutData(fdServerName);

    // Server port line
    wPort =
        new LabelTextVar(
            variables,
            wgServerSettings,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtp.Port.Label"),
            BaseMessages.getString(PKG, "ActionFtp.Port.Tooltip"),
            false,
            false);
    props.setLook(wPort);
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
            wgServerSettings,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtp.User.Label"),
            BaseMessages.getString(PKG, "ActionFtp.User.Tooltip"),
            false,
            false);
    props.setLook(wUserName);
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
            wgServerSettings,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtp.Password.Label"),
            BaseMessages.getString(PKG, "ActionFtp.Password.Tooltip"),
            true,
            false);
    props.setLook(wPassword);
    wPassword.addModifyListener(lsMod);
    FormData fdPassword = new FormData();
    fdPassword.left = new FormAttachment(0, 0);
    fdPassword.top = new FormAttachment(wUserName, margin);
    fdPassword.right = new FormAttachment(100, 0);
    wPassword.setLayoutData(fdPassword);

    // Proxy host line
    wProxyHost =
        new LabelTextVar(
            variables,
            wgServerSettings,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtp.ProxyHost.Label"),
            BaseMessages.getString(PKG, "ActionFtp.ProxyHost.Tooltip"),
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
            wgServerSettings,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtp.ProxyPort.Label"),
            BaseMessages.getString(PKG, "ActionFtp.ProxyPort.Tooltip"),
            false,
            false);
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
            wgServerSettings,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtp.ProxyUsername.Label"),
            BaseMessages.getString(PKG, "ActionFtp.ProxyUsername.Tooltip"),
            false,
            false);
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
            wgServerSettings,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtp.ProxyPassword.Label"),
            BaseMessages.getString(PKG, "ActionFtp.ProxyPassword.Tooltip"),
            true,
            false);
    props.setLook(wProxyPassword);
    wProxyPassword.addModifyListener(lsMod);
    FormData fdProxyPasswd = new FormData();
    fdProxyPasswd.left = new FormAttachment(0, 0);
    fdProxyPasswd.top = new FormAttachment(wProxyUsername, margin);
    fdProxyPasswd.right = new FormAttachment(100, 0);
    wProxyPassword.setLayoutData(fdProxyPasswd);

    // Test connection button
    Button wTest = new Button(wgServerSettings, SWT.PUSH);
    wTest.setText(BaseMessages.getString(PKG, "ActionFtp.TestConnection.Label"));
    props.setLook(wTest);
    FormData fdTest = new FormData();
    wTest.setToolTipText(BaseMessages.getString(PKG, "ActionFtp.TestConnection.Tooltip"));
    // fdTest.left = new FormAttachment(middle, 0);
    fdTest.top = new FormAttachment(wProxyPassword, margin);
    fdTest.right = new FormAttachment(100, 0);
    wTest.setLayoutData(fdTest);

    FormData fdServerSettings = new FormData();
    fdServerSettings.left = new FormAttachment(0, margin);
    fdServerSettings.top = new FormAttachment(wName, margin);
    fdServerSettings.right = new FormAttachment(100, -margin);
    wgServerSettings.setLayoutData(fdServerSettings);
    // ///////////////////////////////////////////////////////////
    // / END OF SERVER SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF Advanced SETTINGS GROUP///
    // /
    Group wAdvancedSettings = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wAdvancedSettings);
    wAdvancedSettings.setText(
        BaseMessages.getString(PKG, "ActionFtp.AdvancedSettings.Group.Label"));
    FormLayout AdvancedSettingsgroupLayout = new FormLayout();
    AdvancedSettingsgroupLayout.marginWidth = 10;
    AdvancedSettingsgroupLayout.marginHeight = 10;
    wAdvancedSettings.setLayout(AdvancedSettingsgroupLayout);

    // Binary mode selection...
    Label wlBinaryMode = new Label(wAdvancedSettings, SWT.RIGHT);
    wlBinaryMode.setText(BaseMessages.getString(PKG, "ActionFtp.BinaryMode.Label"));
    props.setLook(wlBinaryMode);
    FormData fdlBinaryMode = new FormData();
    fdlBinaryMode.left = new FormAttachment(0, 0);
    fdlBinaryMode.top = new FormAttachment(wgServerSettings, margin);
    fdlBinaryMode.right = new FormAttachment(middle, 0);
    wlBinaryMode.setLayoutData(fdlBinaryMode);
    wBinaryMode = new Button(wAdvancedSettings, SWT.CHECK);
    props.setLook(wBinaryMode);
    wBinaryMode.setToolTipText(BaseMessages.getString(PKG, "ActionFtp.BinaryMode.Tooltip"));
    FormData fdBinaryMode = new FormData();
    fdBinaryMode.left = new FormAttachment(middle, margin);
    fdBinaryMode.top = new FormAttachment(wlBinaryMode, 0, SWT.CENTER);
    fdBinaryMode.right = new FormAttachment(100, 0);
    wBinaryMode.setLayoutData(fdBinaryMode);

    // Timeout line
    wTimeout =
        new LabelTextVar(
            variables,
            wAdvancedSettings,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtp.Timeout.Label"),
            BaseMessages.getString(PKG, "ActionFtp.Timeout.Tooltip"),
            false,
            false);
    props.setLook(wTimeout);
    wTimeout.addModifyListener(lsMod);
    FormData fdTimeout = new FormData();
    fdTimeout.left = new FormAttachment(0, 0);
    fdTimeout.top = new FormAttachment(wlBinaryMode, margin);
    fdTimeout.right = new FormAttachment(100, 0);
    wTimeout.setLayoutData(fdTimeout);

    // active connection?
    Label wlActive = new Label(wAdvancedSettings, SWT.RIGHT);
    wlActive.setText(BaseMessages.getString(PKG, "ActionFtp.ActiveConns.Label"));
    props.setLook(wlActive);
    FormData fdlActive = new FormData();
    fdlActive.left = new FormAttachment(0, 0);
    fdlActive.top = new FormAttachment(wTimeout, margin);
    fdlActive.right = new FormAttachment(middle, 0);
    wlActive.setLayoutData(fdlActive);
    wActive = new Button(wAdvancedSettings, SWT.CHECK);
    wActive.setToolTipText(BaseMessages.getString(PKG, "ActionFtp.ActiveConns.Tooltip"));
    props.setLook(wActive);
    FormData fdActive = new FormData();
    fdActive.left = new FormAttachment(middle, margin);
    fdActive.top = new FormAttachment(wlActive, 0, SWT.CENTER);
    fdActive.right = new FormAttachment(100, 0);
    wActive.setLayoutData(fdActive);

    // Control encoding line
    //
    // The drop down is editable as it may happen an encoding may not be present
    // on one machine, but you may want to use it on your execution server
    //
    Label wlControlEncoding = new Label(wAdvancedSettings, SWT.RIGHT);
    wlControlEncoding.setText(BaseMessages.getString(PKG, "ActionFtp.ControlEncoding.Label"));
    props.setLook(wlControlEncoding);
    FormData fdlControlEncoding = new FormData();
    fdlControlEncoding.left = new FormAttachment(0, 0);
    fdlControlEncoding.top = new FormAttachment(wlActive, 2 * margin);
    fdlControlEncoding.right = new FormAttachment(middle, 0);
    wlControlEncoding.setLayoutData(fdlControlEncoding);
    wControlEncoding = new Combo(wAdvancedSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wControlEncoding.setToolTipText(
        BaseMessages.getString(PKG, "ActionFtp.ControlEncoding.Tooltip"));
    wControlEncoding.setItems(encodings);
    props.setLook(wControlEncoding);
    FormData fdControlEncoding = new FormData();
    fdControlEncoding.left = new FormAttachment(middle, margin);
    fdControlEncoding.top = new FormAttachment(wlActive, 2 * margin);
    fdControlEncoding.right = new FormAttachment(100, 0);
    wControlEncoding.setLayoutData(fdControlEncoding);

    FormData fdAdvancedSettings = new FormData();
    fdAdvancedSettings.left = new FormAttachment(0, margin);
    fdAdvancedSettings.top = new FormAttachment(wgServerSettings, margin);
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
    wFilesTab.setText(BaseMessages.getString(PKG, "ActionFtp.Tab.Files.Label"));

    Composite wFilesComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wFilesComp);

    FormLayout FilesLayout = new FormLayout();
    FilesLayout.marginWidth = 3;
    FilesLayout.marginHeight = 3;
    wFilesComp.setLayout(FilesLayout);

    // ////////////////////////
    // START OF Remote SETTINGS GROUP///
    // /
    Group wgRemoteSettings = new Group(wFilesComp, SWT.SHADOW_NONE);
    props.setLook(wgRemoteSettings);
    wgRemoteSettings.setText(BaseMessages.getString(PKG, "ActionFtp.RemoteSettings.Group.Label"));

    FormLayout remoteSettinsGroupLayout = new FormLayout();
    remoteSettinsGroupLayout.marginWidth = 10;
    remoteSettinsGroupLayout.marginHeight = 10;

    wgRemoteSettings.setLayout(remoteSettinsGroupLayout);

    // Move to directory
    Label wlFtpDirectory = new Label(wgRemoteSettings, SWT.RIGHT);
    wlFtpDirectory.setText(BaseMessages.getString(PKG, "ActionFtp.RemoteDir.Label"));
    props.setLook(wlFtpDirectory);
    FormData fdlFtpDirectory = new FormData();
    fdlFtpDirectory.left = new FormAttachment(0, 0);
    fdlFtpDirectory.top = new FormAttachment(0, margin);
    fdlFtpDirectory.right = new FormAttachment(middle, 0);
    wlFtpDirectory.setLayoutData(fdlFtpDirectory);

    // Test remote folder button ...
    Button wbTestChangeFolderExists = new Button(wgRemoteSettings, SWT.PUSH | SWT.CENTER);
    props.setLook(wbTestChangeFolderExists);
    wbTestChangeFolderExists.setText(
        BaseMessages.getString(PKG, "ActionFtp.TestFolderExists.Label"));
    FormData fdbTestChangeFolderExists = new FormData();
    fdbTestChangeFolderExists.right = new FormAttachment(100, 0);
    fdbTestChangeFolderExists.top = new FormAttachment(0, margin);
    wbTestChangeFolderExists.setLayoutData(fdbTestChangeFolderExists);

    wFtpDirectory =
        new TextVar(
            variables,
            wgRemoteSettings,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionFtp.RemoteDir.Tooltip"));
    props.setLook(wFtpDirectory);
    wFtpDirectory.addModifyListener(lsMod);
    FormData fdFtpDirectory = new FormData();
    fdFtpDirectory.left = new FormAttachment(middle, margin);
    fdFtpDirectory.top = new FormAttachment(0, margin);
    fdFtpDirectory.right = new FormAttachment(wbTestChangeFolderExists, -margin);
    wFtpDirectory.setLayoutData(fdFtpDirectory);

    // Wildcard line
    wWildcard =
        new LabelTextVar(
            variables,
            wgRemoteSettings,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionFtp.Wildcard.Label"),
            BaseMessages.getString(PKG, "ActionFtp.Wildcard.Tooltip"),
            false,
            false);
    props.setLook(wWildcard);
    wWildcard.addModifyListener(lsMod);
    FormData fdWildcard = new FormData();
    fdWildcard.left = new FormAttachment(0, 0);
    fdWildcard.top = new FormAttachment(wFtpDirectory, margin);
    fdWildcard.right = new FormAttachment(100, 0);
    wWildcard.setLayoutData(fdWildcard);

    // Remove files after retrieval...
    Label wlRemove = new Label(wgRemoteSettings, SWT.RIGHT);
    wlRemove.setText(BaseMessages.getString(PKG, "ActionFtp.RemoveFiles.Label"));
    props.setLook(wlRemove);
    FormData fdlRemove = new FormData();
    fdlRemove.left = new FormAttachment(0, 0);
    fdlRemove.top = new FormAttachment(wWildcard, margin);
    fdlRemove.right = new FormAttachment(middle, 0);
    wlRemove.setLayoutData(fdlRemove);
    wRemove = new Button(wgRemoteSettings, SWT.CHECK);
    wRemove.setToolTipText(BaseMessages.getString(PKG, "ActionFtp.RemoveFiles.Tooltip"));
    props.setLook(wRemove);
    FormData fdRemove = new FormData();
    fdRemove.left = new FormAttachment(middle, margin);
    fdRemove.top = new FormAttachment(wlRemove, 0, SWT.CENTER);
    fdRemove.right = new FormAttachment(100, 0);
    wRemove.setLayoutData(fdRemove);

    wRemove.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            if (wRemove.getSelection()) {
              wMove.setSelection(false);
              activateMoveTo();
            }
          }
        });

    // Move files after the transfert?...
    Label wlMove = new Label(wgRemoteSettings, SWT.RIGHT);
    wlMove.setText(BaseMessages.getString(PKG, "ActionFtp.MoveFiles.Label"));
    props.setLook(wlMove);
    FormData fdlMove = new FormData();
    fdlMove.left = new FormAttachment(0, 0);
    fdlMove.top = new FormAttachment(wlRemove, margin);
    fdlMove.right = new FormAttachment(middle, -margin);
    wlMove.setLayoutData(fdlMove);
    wMove = new Button(wgRemoteSettings, SWT.CHECK);
    props.setLook(wMove);
    wMove.setToolTipText(BaseMessages.getString(PKG, "ActionFtp.MoveFiles.Tooltip"));
    FormData fdMove = new FormData();
    fdMove.left = new FormAttachment(middle, margin);
    fdMove.top = new FormAttachment(wlMove, 0, SWT.CENTER);
    fdMove.right = new FormAttachment(100, 0);
    wMove.setLayoutData(fdMove);
    wMove.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            activateMoveTo();
            if (wMove.getSelection()) {
              wRemove.setSelection(false);
            }
          }
        });

    // Move to directory
    wlMoveToDirectory = new Label(wgRemoteSettings, SWT.RIGHT);
    wlMoveToDirectory.setText(BaseMessages.getString(PKG, "ActionFtp.MoveFolder.Label"));
    props.setLook(wlMoveToDirectory);
    FormData fdlMoveToDirectory = new FormData();
    fdlMoveToDirectory.left = new FormAttachment(0, 0);
    fdlMoveToDirectory.top = new FormAttachment(wMove, margin);
    fdlMoveToDirectory.right = new FormAttachment(middle, 0);
    wlMoveToDirectory.setLayoutData(fdlMoveToDirectory);

    // Test remote folder button ...
    wbTestFolderExists = new Button(wgRemoteSettings, SWT.PUSH | SWT.CENTER);
    props.setLook(wbTestFolderExists);
    wbTestFolderExists.setText(BaseMessages.getString(PKG, "ActionFtp.TestFolderExists.Label"));
    FormData fdbTestFolderExists = new FormData();
    fdbTestFolderExists.right = new FormAttachment(100, 0);
    fdbTestFolderExists.top = new FormAttachment(wMove, margin);
    wbTestFolderExists.setLayoutData(fdbTestFolderExists);

    wMoveToDirectory =
        new TextVar(
            variables,
            wgRemoteSettings,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionFtp.MoveToDirectory.Tooltip"));
    wMoveToDirectory.setToolTipText(BaseMessages.getString(PKG, "ActionFtp.MoveFolder.Tooltip"));
    props.setLook(wMoveToDirectory);
    wMoveToDirectory.addModifyListener(lsMod);
    FormData fdMoveToDirectory = new FormData();
    fdMoveToDirectory.left = new FormAttachment(middle, margin);
    fdMoveToDirectory.top = new FormAttachment(wMove, margin);
    fdMoveToDirectory.right = new FormAttachment(wbTestFolderExists, -margin);
    wMoveToDirectory.setLayoutData(fdMoveToDirectory);

    // create destination folder?...
    wlCreateMoveFolder = new Label(wgRemoteSettings, SWT.RIGHT);
    wlCreateMoveFolder.setText(BaseMessages.getString(PKG, "ActionFtp.CreateMoveFolder.Label"));
    props.setLook(wlCreateMoveFolder);
    FormData fdlCreateMoveFolder = new FormData();
    fdlCreateMoveFolder.left = new FormAttachment(0, 0);
    fdlCreateMoveFolder.top = new FormAttachment(wMoveToDirectory, margin);
    fdlCreateMoveFolder.right = new FormAttachment(middle, 0);
    wlCreateMoveFolder.setLayoutData(fdlCreateMoveFolder);
    wCreateMoveFolder = new Button(wgRemoteSettings, SWT.CHECK);
    wCreateMoveFolder.setToolTipText(
        BaseMessages.getString(PKG, "ActionFtp.CreateMoveFolder.Tooltip"));
    props.setLook(wCreateMoveFolder);
    FormData fdCreateMoveFolder = new FormData();
    fdCreateMoveFolder.left = new FormAttachment(middle, margin);
    fdCreateMoveFolder.top = new FormAttachment(wlCreateMoveFolder, 0, SWT.CENTER);
    fdCreateMoveFolder.right = new FormAttachment(100, 0);
    wCreateMoveFolder.setLayoutData(fdCreateMoveFolder);

    FormData fdRemoteSettings = new FormData();
    fdRemoteSettings.left = new FormAttachment(0, margin);
    fdRemoteSettings.top = new FormAttachment(0, 2 * margin);
    fdRemoteSettings.right = new FormAttachment(100, -margin);
    wgRemoteSettings.setLayoutData(fdRemoteSettings);
    // ///////////////////////////////////////////////////////////
    // / END OF Remote SETTINGSGROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF LOCAL SETTINGS GROUP///
    // /
    Group wgLocalSettings = new Group(wFilesComp, SWT.SHADOW_NONE);
    props.setLook(wgLocalSettings);
    wgLocalSettings.setText(BaseMessages.getString(PKG, "ActionFtp.LocalSettings.Group.Label"));

    FormLayout localSettingsGroupLayout = new FormLayout();
    localSettingsGroupLayout.marginWidth = 10;
    localSettingsGroupLayout.marginHeight = 10;

    wgLocalSettings.setLayout(localSettingsGroupLayout);

    // TargetDirectory
    Label wlTargetDirectory = new Label(wgLocalSettings, SWT.RIGHT);
    wlTargetDirectory.setText(BaseMessages.getString(PKG, "ActionFtp.TargetDir.Label"));
    props.setLook(wlTargetDirectory);
    FormData fdlTargetDirectory = new FormData();
    fdlTargetDirectory.left = new FormAttachment(0, 0);
    fdlTargetDirectory.top = new FormAttachment(0, margin);
    fdlTargetDirectory.right = new FormAttachment(middle, -margin);
    wlTargetDirectory.setLayoutData(fdlTargetDirectory);

    // Browse folders button ...
    Button wbTargetDirectory = new Button(wgLocalSettings, SWT.PUSH | SWT.CENTER);
    props.setLook(wbTargetDirectory);
    wbTargetDirectory.setText(BaseMessages.getString(PKG, "ActionFtp.BrowseFolders.Label"));
    FormData fdbTargetDirectory = new FormData();
    fdbTargetDirectory.right = new FormAttachment(100, 0);
    fdbTargetDirectory.top = new FormAttachment(wlTargetDirectory, 0, SWT.CENTER);
    wbTargetDirectory.setLayoutData(fdbTargetDirectory);

    wbTargetDirectory.addListener(
        SWT.Selection, e -> BaseDialog.presentDirectoryDialog(shell, wTargetDirectory, variables));

    wTargetDirectory =
        new TextVar(
            variables,
            wgLocalSettings,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionFtp.TargetDir.Tooltip"));
    props.setLook(wTargetDirectory);
    wTargetDirectory.addModifyListener(lsMod);
    FormData fdTargetDirectory = new FormData();
    fdTargetDirectory.left = new FormAttachment(middle, margin);
    fdTargetDirectory.top = new FormAttachment(wlTargetDirectory, 0, SWT.CENTER);
    fdTargetDirectory.right = new FormAttachment(wbTargetDirectory, -margin);
    wTargetDirectory.setLayoutData(fdTargetDirectory);

    // Create multi-part file?
    wlAddDate = new Label(wgLocalSettings, SWT.RIGHT);
    wlAddDate.setText(BaseMessages.getString(PKG, "ActionFtp.AddDate.Label"));
    props.setLook(wlAddDate);
    FormData fdlAddDate = new FormData();
    fdlAddDate.left = new FormAttachment(0, 0);
    fdlAddDate.top = new FormAttachment(wTargetDirectory, margin);
    fdlAddDate.right = new FormAttachment(middle, -margin);
    wlAddDate.setLayoutData(fdlAddDate);
    wAddDate = new Button(wgLocalSettings, SWT.CHECK);
    props.setLook(wAddDate);
    wAddDate.setToolTipText(BaseMessages.getString(PKG, "ActionFtp.AddDate.Tooltip"));
    FormData fdAddDate = new FormData();
    fdAddDate.left = new FormAttachment(middle, margin);
    fdAddDate.top = new FormAttachment(wlAddDate, 0, SWT.CENTER);
    fdAddDate.right = new FormAttachment(100, 0);
    wAddDate.setLayoutData(fdAddDate);
    wAddDate.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });
    // Create multi-part file?
    wlAddTime = new Label(wgLocalSettings, SWT.RIGHT);
    wlAddTime.setText(BaseMessages.getString(PKG, "ActionFtp.AddTime.Label"));
    props.setLook(wlAddTime);
    FormData fdlAddTime = new FormData();
    fdlAddTime.left = new FormAttachment(0, 0);
    fdlAddTime.top = new FormAttachment(wlAddDate, 2 * margin);
    fdlAddTime.right = new FormAttachment(middle, -margin);
    wlAddTime.setLayoutData(fdlAddTime);
    wAddTime = new Button(wgLocalSettings, SWT.CHECK);
    props.setLook(wAddTime);
    wAddTime.setToolTipText(BaseMessages.getString(PKG, "ActionFtp.AddTime.Tooltip"));
    FormData fdAddTime = new FormData();
    fdAddTime.left = new FormAttachment(middle, margin);
    fdAddTime.top = new FormAttachment(wlAddTime, 0, SWT.CENTER);
    fdAddTime.right = new FormAttachment(100, 0);
    wAddTime.setLayoutData(fdAddTime);
    wAddTime.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    // Specify date time format?
    Label wlSpecifyFormat = new Label(wgLocalSettings, SWT.RIGHT);
    wlSpecifyFormat.setText(BaseMessages.getString(PKG, "ActionFtp.SpecifyFormat.Label"));
    props.setLook(wlSpecifyFormat);
    FormData fdlSpecifyFormat = new FormData();
    fdlSpecifyFormat.left = new FormAttachment(0, 0);
    fdlSpecifyFormat.top = new FormAttachment(wlAddTime, 2 * margin);
    fdlSpecifyFormat.right = new FormAttachment(middle, -margin);
    wlSpecifyFormat.setLayoutData(fdlSpecifyFormat);
    wSpecifyFormat = new Button(wgLocalSettings, SWT.CHECK);
    props.setLook(wSpecifyFormat);
    wSpecifyFormat.setToolTipText(BaseMessages.getString(PKG, "ActionFtp.SpecifyFormat.Tooltip"));
    FormData fdSpecifyFormat = new FormData();
    fdSpecifyFormat.left = new FormAttachment(middle, margin);
    fdSpecifyFormat.top = new FormAttachment(wlSpecifyFormat, 0, SWT.CENTER);
    fdSpecifyFormat.right = new FormAttachment(100, 0);
    wSpecifyFormat.setLayoutData(fdSpecifyFormat);
    wSpecifyFormat.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
            setDateTimeFormat();
            setAddDateBeforeExtension();
          }
        });

    // Prepare a list of possible DateTimeFormats...
    String[] dats = Const.getDateFormats();

    // DateTimeFormat
    wlDateTimeFormat = new Label(wgLocalSettings, SWT.RIGHT);
    wlDateTimeFormat.setText(BaseMessages.getString(PKG, "ActionFtp.DateTimeFormat.Label"));
    props.setLook(wlDateTimeFormat);
    FormData fdlDateTimeFormat = new FormData();
    fdlDateTimeFormat.left = new FormAttachment(0, 0);
    fdlDateTimeFormat.top = new FormAttachment(wlSpecifyFormat, 2 * margin);
    fdlDateTimeFormat.right = new FormAttachment(middle, -margin);
    wlDateTimeFormat.setLayoutData(fdlDateTimeFormat);
    wDateTimeFormat = new CCombo(wgLocalSettings, SWT.BORDER | SWT.READ_ONLY);
    wDateTimeFormat.setEditable(true);
    props.setLook(wDateTimeFormat);
    wDateTimeFormat.addModifyListener(lsMod);
    FormData fdDateTimeFormat = new FormData();
    fdDateTimeFormat.left = new FormAttachment(middle, margin);
    fdDateTimeFormat.top = new FormAttachment(wlSpecifyFormat, 2 * margin);
    fdDateTimeFormat.right = new FormAttachment(100, 0);
    wDateTimeFormat.setLayoutData(fdDateTimeFormat);
    for (String dat : dats) {
      wDateTimeFormat.add(dat);
    }

    // Add Date before extension?
    wlAddDateBeforeExtension = new Label(wgLocalSettings, SWT.RIGHT);
    wlAddDateBeforeExtension.setText(
        BaseMessages.getString(PKG, "ActionFtp.AddDateBeforeExtension.Label"));
    props.setLook(wlAddDateBeforeExtension);
    FormData fdlAddDateBeforeExtension = new FormData();
    fdlAddDateBeforeExtension.left = new FormAttachment(0, 0);
    fdlAddDateBeforeExtension.top = new FormAttachment(wDateTimeFormat, margin);
    fdlAddDateBeforeExtension.right = new FormAttachment(middle, -margin);
    wlAddDateBeforeExtension.setLayoutData(fdlAddDateBeforeExtension);
    wAddDateBeforeExtension = new Button(wgLocalSettings, SWT.CHECK);
    props.setLook(wAddDateBeforeExtension);
    wAddDateBeforeExtension.setToolTipText(
        BaseMessages.getString(PKG, "ActionFtp.AddDateBeforeExtension.Tooltip"));
    FormData fdAddDateBeforeExtension = new FormData();
    fdAddDateBeforeExtension.left = new FormAttachment(middle, margin);
    fdAddDateBeforeExtension.top = new FormAttachment(wlAddDateBeforeExtension, 0, SWT.CENTER);
    fdAddDateBeforeExtension.right = new FormAttachment(100, 0);
    wAddDateBeforeExtension.setLayoutData(fdAddDateBeforeExtension);
    wAddDateBeforeExtension.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    // OnlyNew files after retrieval...
    Label wlOnlyNew = new Label(wgLocalSettings, SWT.RIGHT);
    wlOnlyNew.setText(BaseMessages.getString(PKG, "ActionFtp.DontOverwrite.Label"));
    props.setLook(wlOnlyNew);
    FormData fdlOnlyNew = new FormData();
    fdlOnlyNew.left = new FormAttachment(0, 0);
    fdlOnlyNew.top = new FormAttachment(wlAddDateBeforeExtension, margin);
    fdlOnlyNew.right = new FormAttachment(middle, 0);
    wlOnlyNew.setLayoutData(fdlOnlyNew);
    wOnlyNew = new Button(wgLocalSettings, SWT.CHECK);
    wOnlyNew.setToolTipText(BaseMessages.getString(PKG, "ActionFtp.DontOverwrite.Tooltip"));
    props.setLook(wOnlyNew);
    FormData fdOnlyNew = new FormData();
    fdOnlyNew.left = new FormAttachment(middle, margin);
    fdOnlyNew.top = new FormAttachment(wlOnlyNew, 0, SWT.CENTER);
    fdOnlyNew.right = new FormAttachment(100, 0);
    wOnlyNew.setLayoutData(fdOnlyNew);
    wOnlyNew.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            activeIfExists();
            action.setChanged();
          }
        });

    // If File Exists
    wlIfFileExists = new Label(wgLocalSettings, SWT.RIGHT);
    wlIfFileExists.setText(BaseMessages.getString(PKG, "ActionFtp.IfFileExists.Label"));
    props.setLook(wlIfFileExists);
    FormData fdlIfFileExists = new FormData();
    fdlIfFileExists.left = new FormAttachment(0, 0);
    fdlIfFileExists.right = new FormAttachment(middle, 0);
    fdlIfFileExists.top = new FormAttachment(wlOnlyNew, 2 * margin);
    wlIfFileExists.setLayoutData(fdlIfFileExists);
    wIfFileExists = new CCombo(wgLocalSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wIfFileExists.add(BaseMessages.getString(PKG, "ActionFtp.Skip.Label"));
    wIfFileExists.add(BaseMessages.getString(PKG, "ActionFtp.Give_Unique_Name.Label"));
    wIfFileExists.add(BaseMessages.getString(PKG, "ActionFtp.Fail.Label"));
    wIfFileExists.select(0); // +1: starts at -1

    props.setLook(wIfFileExists);

    FormData fdIfFileExists = new FormData();
    fdIfFileExists.left = new FormAttachment(middle, margin);
    fdIfFileExists.top = new FormAttachment(wlIfFileExists, 0, SWT.CENTER);
    fdIfFileExists.right = new FormAttachment(100, 0);
    wIfFileExists.setLayoutData(fdIfFileExists);

    wIfFileExists.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {}
        });

    // Add filenames to result filenames...
    Label wlAddFilenameToResult = new Label(wgLocalSettings, SWT.RIGHT);
    wlAddFilenameToResult.setText(
        BaseMessages.getString(PKG, "ActionFtp.AddFilenameToResult.Label"));
    props.setLook(wlAddFilenameToResult);
    FormData fdlAddFilenameToResult = new FormData();
    fdlAddFilenameToResult.left = new FormAttachment(0, 0);
    fdlAddFilenameToResult.top = new FormAttachment(wIfFileExists, 2 * margin);
    fdlAddFilenameToResult.right = new FormAttachment(middle, 0);
    wlAddFilenameToResult.setLayoutData(fdlAddFilenameToResult);
    wAddFilenameToResult = new Button(wgLocalSettings, SWT.CHECK);
    wAddFilenameToResult.setToolTipText(
        BaseMessages.getString(PKG, "ActionFtp.AddFilenameToResult.Tooltip"));
    props.setLook(wAddFilenameToResult);
    FormData fdAddFilenameToResult = new FormData();
    fdAddFilenameToResult.left = new FormAttachment(middle, margin);
    fdAddFilenameToResult.top = new FormAttachment(wlAddFilenameToResult, 0, SWT.CENTER);
    fdAddFilenameToResult.right = new FormAttachment(100, 0);
    wAddFilenameToResult.setLayoutData(fdAddFilenameToResult);

    FormData fdLocalSettings = new FormData();
    fdLocalSettings.left = new FormAttachment(0, margin);
    fdLocalSettings.top = new FormAttachment(wgRemoteSettings, margin);
    fdLocalSettings.right = new FormAttachment(100, -margin);
    wgLocalSettings.setLayoutData(fdLocalSettings);
    // ///////////////////////////////////////////////////////////
    // / END OF LOCAL SETTINGSGROUP
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

    // ////////////////////////
    // START OF Advanced TAB ///
    // ////////////////////////

    CTabItem wAdvancedTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdvancedTab.setText(BaseMessages.getString(PKG, "ActionFtp.Tab.Advanced.Label"));

    Composite wAdvancedComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wAdvancedComp);

    FormLayout AdvancedLayout = new FormLayout();
    AdvancedLayout.marginWidth = 3;
    AdvancedLayout.marginHeight = 3;
    wAdvancedComp.setLayout(AdvancedLayout);

    // SuccessOngrouping?
    // ////////////////////////
    // START OF SUCCESS ON GROUP///
    // /
    Group wSuccessOn = new Group(wAdvancedComp, SWT.SHADOW_NONE);
    props.setLook(wSuccessOn);
    wSuccessOn.setText(BaseMessages.getString(PKG, "ActionFtp.SuccessOn.Group.Label"));

    FormLayout successongroupLayout = new FormLayout();
    successongroupLayout.marginWidth = 10;
    successongroupLayout.marginHeight = 10;

    wSuccessOn.setLayout(successongroupLayout);

    // Success Condition
    Label wlSuccessCondition = new Label(wSuccessOn, SWT.RIGHT);
    wlSuccessCondition.setText(
        BaseMessages.getString(PKG, "ActionFtp.SuccessCondition.Label") + " ");
    props.setLook(wlSuccessCondition);
    FormData fdlSuccessCondition = new FormData();
    fdlSuccessCondition.left = new FormAttachment(0, 0);
    fdlSuccessCondition.right = new FormAttachment(middle, 0);
    fdlSuccessCondition.top = new FormAttachment(0, margin);
    wlSuccessCondition.setLayoutData(fdlSuccessCondition);
    wSuccessCondition = new CCombo(wSuccessOn, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wSuccessCondition.add(BaseMessages.getString(PKG, "ActionFtp.SuccessWhenAllWorksFine.Label"));
    wSuccessCondition.add(BaseMessages.getString(PKG, "ActionFtp.SuccessWhenAtLeat.Label"));
    wSuccessCondition.add(
        BaseMessages.getString(PKG, "ActionFtp.SuccessWhenNrErrorsLessThan.Label"));
    wSuccessCondition.select(0); // +1: starts at -1

    props.setLook(wSuccessCondition);
    FormData fdSuccessCondition = new FormData();
    fdSuccessCondition.left = new FormAttachment(middle, 0);
    fdSuccessCondition.top = new FormAttachment(0, margin);
    fdSuccessCondition.right = new FormAttachment(100, 0);
    wSuccessCondition.setLayoutData(fdSuccessCondition);
    wSuccessCondition.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            activeSuccessCondition();
          }
        });

    // Success when number of errors less than
    wlNrErrorsLessThan = new Label(wSuccessOn, SWT.RIGHT);
    wlNrErrorsLessThan.setText(
        BaseMessages.getString(PKG, "ActionFtp.NrBadFormedLessThan.Label") + " ");
    props.setLook(wlNrErrorsLessThan);
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
            BaseMessages.getString(PKG, "ActionFtp.NrBadFormedLessThan.Tooltip"));
    props.setLook(wNrErrorsLessThan);
    wNrErrorsLessThan.addModifyListener(lsMod);
    FormData fdNrErrorsLessThan = new FormData();
    fdNrErrorsLessThan.left = new FormAttachment(middle, 0);
    fdNrErrorsLessThan.top = new FormAttachment(wSuccessCondition, margin);
    fdNrErrorsLessThan.right = new FormAttachment(100, -margin);
    wNrErrorsLessThan.setLayoutData(fdNrErrorsLessThan);

    FormData fdSuccessOn = new FormData();
    fdSuccessOn.left = new FormAttachment(0, margin);
    fdSuccessOn.top = new FormAttachment(0, margin);
    fdSuccessOn.right = new FormAttachment(100, -margin);
    wSuccessOn.setLayoutData(fdSuccessOn);
    // ///////////////////////////////////////////////////////////
    // / END OF Success ON GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdAdvancedComp = new FormData();
    fdAdvancedComp.left = new FormAttachment(0, 0);
    fdAdvancedComp.top = new FormAttachment(0, 0);
    fdAdvancedComp.right = new FormAttachment(100, 0);
    fdAdvancedComp.bottom = new FormAttachment(100, 0);
    wAdvancedComp.setLayoutData(fdAdvancedComp);

    wAdvancedComp.layout();
    wAdvancedTab.setControl(wAdvancedComp);
    props.setLook(wAdvancedComp);

    // ///////////////////////////////////////////////////////////
    // / END OF Advanced TAB
    // ///////////////////////////////////////////////////////////

    // ///////////////////////////////////////////////////////////
    // Start of Socks Proxy Tab
    // ///////////////////////////////////////////////////////////
    CTabItem wSocksProxyTab = new CTabItem(wTabFolder, SWT.NONE);
    wSocksProxyTab.setText(BaseMessages.getString(PKG, "ActionFtp.Tab.Socks.Label"));

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
    wSocksProxy.setText(BaseMessages.getString(PKG, "ActionFtp.SocksProxy.Group.Label"));

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
            BaseMessages.getString(PKG, "ActionFtp.SocksProxyHost.Label"),
            BaseMessages.getString(PKG, "ActionFtp.SocksProxyHost.Tooltip"),
            false,
            false);
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
            BaseMessages.getString(PKG, "ActionFtp.SocksProxyPort.Label"),
            BaseMessages.getString(PKG, "ActionFtp.SocksProxyPort.Tooltip"),
            false,
            false);
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
            BaseMessages.getString(PKG, "ActionFtp.SocksProxyUsername.Label"),
            BaseMessages.getString(PKG, "ActionFtp.SocksProxyPassword.Tooltip"),
            false,
            false);
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
            BaseMessages.getString(PKG, "ActionFtp.SocksProxyPassword.Label"),
            BaseMessages.getString(PKG, "ActionFtp.SocksProxyPassword.Tooltip"),
            true,
            false);
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
    Listener lsCheckFolder = e -> checkRemoteFolder(false, true, wMoveToDirectory.getText());
    Listener lsCheckChangeFolder = e -> checkRemoteFolder(true, false, wFtpDirectory.getText());

    wTest.addListener(SWT.Selection, lsTest);
    wbTestFolderExists.addListener(SWT.Selection, lsCheckFolder);
    wbTestChangeFolderExists.addListener(SWT.Selection, lsCheckChangeFolder);

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
    wFtpDirectory.addSelectionListener(lsDef);
    wTargetDirectory.addSelectionListener(lsDef);
    wFtpDirectory.addSelectionListener(lsDef);
    wWildcard.addSelectionListener(lsDef);
    wTimeout.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    getData();
    activateMoveTo();
    setDateTimeFormat();
    setAddDateBeforeExtension();
    activeSuccessCondition();
    activeIfExists();

    wTabFolder.setSelection(0);
    BaseTransformDialog.setSize(shell);

    shell.open();
    props.setDialogSize(shell, "JobFTPDialogSize");
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return action;
  }

  private void activeIfExists() {
    wIfFileExists.setEnabled(wOnlyNew.getSelection());
    wlIfFileExists.setEnabled(wOnlyNew.getSelection());
  }

  private void test() {
    if (connectToFtp(false, false)) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      mb.setMessage(
          BaseMessages.getString(PKG, "ActionFtp.Connected.OK", wServerName.getText()) + Const.CR);
      mb.setText(BaseMessages.getString(PKG, "ActionFtp.Connected.Title.Ok"));
      mb.open();
    }
    closeFtpConnection();
  }

  private void checkRemoteFolder(boolean FtpFolfer, boolean checkMoveFolder, String foldername) {
    if (!Utils.isEmpty(foldername)) {
      if (connectToFtp(FtpFolfer, checkMoveFolder)) {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
        mb.setMessage(
            BaseMessages.getString(PKG, "ActionFtp.FolderExists.OK", foldername) + Const.CR);
        mb.setText(BaseMessages.getString(PKG, "ActionFtp.FolderExists.Title.Ok"));
        mb.open();
      }
    }
  }

  private boolean connectToFtp(boolean checkFolder, boolean checkMoveToFolder) {
    boolean retval = false;
    String realServername = null;
    try {
      if (ftpclient == null || !ftpclient.isConnected()) {
        ActionFtp actionFtp = new ActionFtp();
        getInfo(actionFtp);

        // Create ftp client to host:port ...
        ftpclient =
            FtpClientUtil.connectAndLogin(LogChannel.UI, variables, actionFtp, wName.getText());
      }
      retval = true;
    } catch (Exception e) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "ActionFtp.ErrorConnect.NOK", realServername, e.getMessage())
              + Const.CR);
      mb.setText(BaseMessages.getString(PKG, "ActionFtp.ErrorConnect.Title.Bad"));
      mb.open();
    }
    return retval;
  }

  private void activeSuccessCondition() {
    wlNrErrorsLessThan.setEnabled(wSuccessCondition.getSelectionIndex() != 0);
    wNrErrorsLessThan.setEnabled(wSuccessCondition.getSelectionIndex() != 0);
  }

  private void setAddDateBeforeExtension() {
    wlAddDateBeforeExtension.setEnabled(
        wAddDate.getSelection() || wAddTime.getSelection() || wSpecifyFormat.getSelection());
    wAddDateBeforeExtension.setEnabled(
        wAddDate.getSelection() || wAddTime.getSelection() || wSpecifyFormat.getSelection());
    if (!wAddDate.getSelection() && !wAddTime.getSelection() && !wSpecifyFormat.getSelection()) {
      wAddDateBeforeExtension.setSelection(false);
    }
  }

  public void activateMoveTo() {
    wMoveToDirectory.setEnabled(wMove.getSelection());
    wlMoveToDirectory.setEnabled(wMove.getSelection());
    wCreateMoveFolder.setEnabled(wMove.getSelection());
    wlCreateMoveFolder.setEnabled(wMove.getSelection());
    wbTestFolderExists.setEnabled(wMove.getSelection());
  }

  private void setDateTimeFormat() {
    if (wSpecifyFormat.getSelection()) {
      wAddDate.setSelection(false);
      wAddTime.setSelection(false);
    }

    wDateTimeFormat.setEnabled(wSpecifyFormat.getSelection());
    wlDateTimeFormat.setEnabled(wSpecifyFormat.getSelection());
    wAddDate.setEnabled(!wSpecifyFormat.getSelection());
    wlAddDate.setEnabled(!wSpecifyFormat.getSelection());
    wAddTime.setEnabled(!wSpecifyFormat.getSelection());
    wlAddTime.setEnabled(!wSpecifyFormat.getSelection());
  }

  public void dispose() {
    closeFtpConnection();
    WindowProperty winprop = new WindowProperty(shell);
    props.setScreen(winprop);
    shell.dispose();
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.nullToEmpty(action.getName()));
    wServerName.setText(Const.NVL(action.getServerName(), ""));
    wPort.setText(Const.NVL(action.getServerPort(), "21"));
    wUserName.setText(Const.NVL(action.getUserName(), ""));
    wPassword.setText(Const.NVL(action.getPassword(), ""));
    wFtpDirectory.setText(Const.NVL(action.getRemoteDirectory(), ""));
    wTargetDirectory.setText(Const.NVL(action.getTargetDirectory(), ""));
    wWildcard.setText(Const.NVL(action.getWildcard(), ""));
    wBinaryMode.setSelection(action.isBinaryMode());
    wTimeout.setText("" + action.getTimeout());
    wRemove.setSelection(action.isRemove());
    wOnlyNew.setSelection(action.isOnlyGettingNewFiles());
    wActive.setSelection(action.isActiveConnection());
    wControlEncoding.setText(action.getControlEncoding());
    wMove.setSelection(action.isMoveFiles());
    wMoveToDirectory.setText(Const.NVL(action.getMoveToDirectory(), ""));

    wAddDate.setSelection(action.isAddDate());
    wAddTime.setSelection(action.isAddTime());

    wDateTimeFormat.setText(Const.nullToEmpty(action.getDateTimeFormat()));
    wSpecifyFormat.setSelection(action.isSpecifyFormat());

    wAddDateBeforeExtension.setSelection(action.isAddDateBeforeExtension());
    wAddFilenameToResult.setSelection(action.isAddResult());
    wCreateMoveFolder.setSelection(action.isCreateMoveFolder());

    wProxyHost.setText(Const.NVL(action.getProxyHost(), ""));
    wProxyPort.setText(Const.NVL(action.getProxyPort(), ""));
    wProxyUsername.setText(Const.NVL(action.getProxyUsername(), ""));
    wProxyPassword.setText(Const.NVL(action.getProxyPassword(), ""));
    wSocksProxyHost.setText(Const.NVL(action.getSocksProxyHost(), ""));
    wSocksProxyPort.setText(Const.NVL(action.getSocksProxyPort(), "1080"));
    wSocksProxyUsername.setText(Const.NVL(action.getSocksProxyUsername(), ""));
    wSocksProxyPassword.setText(Const.NVL(action.getSocksProxyPassword(), ""));
    wIfFileExists.select(action.ifFileExists);

    if (action.getNrLimit() != null) {
      wNrErrorsLessThan.setText(action.getNrLimit());
    } else {
      wNrErrorsLessThan.setText("10");
    }

    if (action.getSuccessCondition() != null) {
      if (action.getSuccessCondition().equals(action.SUCCESS_IF_AT_LEAST_X_FILES_DOWNLOADED)) {
        wSuccessCondition.select(1);
      } else if (action.getSuccessCondition().equals(action.SUCCESS_IF_ERRORS_LESS)) {
        wSuccessCondition.select(2);
      } else {
        wSuccessCondition.select(0);
      }
    } else {
      wSuccessCondition.select(0);
    }

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

  private void getInfo(ActionFtp action) {

    action.setName(wName.getText());
    action.setServerPort(wPort.getText());
    action.setServerName(wServerName.getText());
    action.setUserName(wUserName.getText());
    action.setPassword(wPassword.getText());
    action.setRemoteDirectory(wFtpDirectory.getText());
    action.setTargetDirectory(wTargetDirectory.getText());
    action.setWildcard(wWildcard.getText());
    action.setBinaryMode(wBinaryMode.getSelection());
    action.setTimeout(Const.toInt(wTimeout.getText(), 10000));
    action.setRemove(wRemove.getSelection());
    action.setOnlyGettingNewFiles(wOnlyNew.getSelection());
    action.setActiveConnection(wActive.getSelection());
    action.setControlEncoding(wControlEncoding.getText());
    action.setMoveFiles(wMove.getSelection());
    action.setMoveToDirectory(wMoveToDirectory.getText());

    action.setAddDate(wAddDate.getSelection());
    action.setAddTime(wAddTime.getSelection());
    action.setSpecifyFormat(wSpecifyFormat.getSelection());
    action.setDateTimeFormat(wDateTimeFormat.getText());

    action.setAddDateBeforeExtension(wAddDateBeforeExtension.getSelection());
    action.setAddResult(wAddFilenameToResult.getSelection());
    action.setCreateMoveFolder(wCreateMoveFolder.getSelection());

    action.setProxyHost(wProxyHost.getText());
    action.setProxyPort(wProxyPort.getText());
    action.setProxyUsername(wProxyUsername.getText());
    action.setProxyPassword(wProxyPassword.getText());

    action.setSocksProxyHost(wSocksProxyHost.getText());
    action.setSocksProxyPort(wSocksProxyPort.getText());
    action.setSocksProxyUsername(wSocksProxyUsername.getText());
    action.setSocksProxyPassword(wSocksProxyPassword.getText());

    if (wIfFileExists.getSelectionIndex() == 1) {
      action.ifFileExists = action.ifFileExistsCreateUniq;
      action.stringIfFileExists = action.STRING_IF_FILE_EXISTS_CREATE_UNIQ;
    } else if (wIfFileExists.getSelectionIndex() == 2) {
      action.ifFileExists = action.ifFileExistsFail;
      action.stringIfFileExists = action.STRING_IF_FILE_EXISTS_FAIL;
    } else {
      action.ifFileExists = action.ifFileExistsSkip;
      action.stringIfFileExists = action.STRING_IF_FILE_EXISTS_SKIP;
    }

    action.setNrLimit(wNrErrorsLessThan.getText());

    if (wSuccessCondition.getSelectionIndex() == 1) {
      action.setSuccessCondition(action.SUCCESS_IF_AT_LEAST_X_FILES_DOWNLOADED);
    } else if (wSuccessCondition.getSelectionIndex() == 2) {
      action.setSuccessCondition(action.SUCCESS_IF_ERRORS_LESS);
    } else {
      action.setSuccessCondition(action.SUCCESS_IF_NO_ERRORS);
    }
  }

  private void closeFtpConnection() {
    // Close FTP connection if necessary
    if (ftpclient != null && ftpclient.isConnected()) {
      try {
        ftpclient.quit();
        ftpclient = null;
        FtpClientUtil.clearSocksJvmSettings();
      } catch (Exception e) {
        // Ignore errors
      }
    }
  }
}
