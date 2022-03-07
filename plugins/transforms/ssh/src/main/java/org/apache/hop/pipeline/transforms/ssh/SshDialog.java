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

package org.apache.hop.pipeline.transforms.ssh;

import com.trilead.ssh2.Connection;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.*;
import org.apache.hop.ui.core.widget.LabelTextVar;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
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
import org.eclipse.swt.widgets.*;

public class SshDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = SshMeta.class; // For Translator

  private Label wlCommandField;
  private CCombo wCommandField;

  private LabelTextVar wTimeOut;
  private final SshMeta input;

  private Button wDynamicCommand;

  private LabelTextVar wPort;

  private LabelTextVar wUserName;

  private LabelTextVar wPassword;

  private Button wUseKey;

  private TextVar wPrivateKey;

  private LabelTextVar wPassphrase;

  private LabelTextVar wResultOutFieldName;
  private LabelTextVar wResultErrFieldName;

  private Label wlCommand;
  private StyledTextComp wCommand;

  private LabelTextVar wProxyHost;

  private LabelTextVar wProxyPort;

  private LabelTextVar wProxyUsername;

  private LabelTextVar wProxyPassword;

  private LabelTextVar wServerName;

  private boolean gotPreviousFields = false;

  public SshDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (SshMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);

    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "SSHDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // THE BUTTONS at the bottom
    //
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wPreview = new Button(shell, SWT.PUSH);
    wPreview.setText(BaseMessages.getString(PKG, "SSHDialog.Button.PreviewRows"));
    wPreview.addListener(SWT.Selection, e -> preview());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wPreview, wCancel}, margin, null);

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "SSHDialog.TransformName.Label"));

    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);

    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////
    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText(BaseMessages.getString(PKG, "SSHDialog.General.Tab"));

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);


    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wGeneralComp.setLayout(fileLayout);

    // ///////////////////////////////
    // START OF Settings GROUP //
    // ///////////////////////////////

    Group wSettingsGroup = new Group(wGeneralComp, SWT.SHADOW_NONE);
    wSettingsGroup.setText(BaseMessages.getString(PKG, "SSHDialog.wSettingsGroup.Label"));

    FormLayout settingGroupLayout = new FormLayout();
    settingGroupLayout.marginWidth = 10;
    settingGroupLayout.marginHeight = 10;
    wSettingsGroup.setLayout(settingGroupLayout);

    // Server port line
    wServerName =
        new LabelTextVar(
            variables,
            wSettingsGroup,
            BaseMessages.getString(PKG, "SSHDialog.Server.Label"),
            BaseMessages.getString(PKG, "SSHDialog.Server.Tooltip"));

    wServerName.addModifyListener(lsMod);
    FormData fdServerName = new FormData();
    fdServerName.left = new FormAttachment(0, 0);
    fdServerName.top = new FormAttachment(wTransformName, margin);
    fdServerName.right = new FormAttachment(100, 0);
    wServerName.setLayoutData(fdServerName);

    // Server port line
    wPort =
        new LabelTextVar(
            variables,
            wSettingsGroup,
            BaseMessages.getString(PKG, "SSHDialog.Port.Label"),
            BaseMessages.getString(PKG, "SSHDialog.Port.Tooltip"));

    wPort.addModifyListener(lsMod);
    FormData fdPort = new FormData();
    fdPort.left = new FormAttachment(0, 0);
    fdPort.top = new FormAttachment(wServerName, margin);
    fdPort.right = new FormAttachment(100, 0);
    wPort.setLayoutData(fdPort);

    // Server TimeOut line
    wTimeOut =
        new LabelTextVar(
            variables,
            wSettingsGroup,
            BaseMessages.getString(PKG, "SSHDialog.TimeOut.Label"),
            BaseMessages.getString(PKG, "SSHDialog.TimeOut.Tooltip"));
    wTimeOut.addModifyListener(lsMod);
    FormData fdTimeOut = new FormData();
    fdTimeOut.left = new FormAttachment(0, 0);
    fdTimeOut.top = new FormAttachment(wPort, margin);
    fdTimeOut.right = new FormAttachment(100, 0);
    wTimeOut.setLayoutData(fdTimeOut);

    // Usernameline
    wUserName =
        new LabelTextVar(
            variables,
            wSettingsGroup,
            BaseMessages.getString(PKG, "SSHDialog.UserName.Label"),
            BaseMessages.getString(PKG, "SSHDialog.UserName.Tooltip"));

    wUserName.addModifyListener(lsMod);
    FormData fdUserName = new FormData();
    fdUserName.left = new FormAttachment(0, 0);
    fdUserName.top = new FormAttachment(wTimeOut, margin);
    fdUserName.right = new FormAttachment(100, 0);
    wUserName.setLayoutData(fdUserName);

    // Passwordline
    wPassword =
        new LabelTextVar(
            variables,
            wSettingsGroup,
            BaseMessages.getString(PKG, "SSHDialog.Password.Label"),
            BaseMessages.getString(PKG, "SSHDialog.Password.Tooltip"),
            true);

    wPassword.addModifyListener(lsMod);
    FormData fdPassword = new FormData();
    fdPassword.left = new FormAttachment(0, 0);
    fdPassword.top = new FormAttachment(wUserName, margin);
    fdPassword.right = new FormAttachment(100, 0);
    wPassword.setLayoutData(fdPassword);

    // Use key?
    Label wlUseKey = new Label(wSettingsGroup, SWT.RIGHT);
    wlUseKey.setText(BaseMessages.getString(PKG, "SSHDialog.UseKey.Label"));
    FormData fdlUseKey = new FormData();
    fdlUseKey.left = new FormAttachment(0, 0);
    fdlUseKey.top = new FormAttachment(wPassword, margin);
    fdlUseKey.right = new FormAttachment(middle, -margin);
    wlUseKey.setLayoutData(fdlUseKey);
    wUseKey = new Button(wSettingsGroup, SWT.CHECK);
    wUseKey.setToolTipText(BaseMessages.getString(PKG, "SSHDialog.UseKey.Tooltip"));
    FormData fdUseKey = new FormData();
    fdUseKey.left = new FormAttachment(middle, margin);
    fdUseKey.top = new FormAttachment(wlUseKey, 0, SWT.CENTER);
    fdUseKey.right = new FormAttachment(100, 0);
    wUseKey.setLayoutData(fdUseKey);
    wUseKey.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            activateKey();
          }
        });

    Button wbFilename = new Button(wSettingsGroup, SWT.PUSH | SWT.CENTER);

    wbFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment(100, -margin);
    fdbFilename.top = new FormAttachment(wUseKey, 2 * margin);
    wbFilename.setLayoutData(fdbFilename);

    wbFilename.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            FileDialog dialog = new FileDialog(shell, SWT.SAVE);
            dialog.setFilterExtensions(new String[] {"*.pem", "*"});
            if (wPrivateKey.getText() != null) {
              dialog.setFileName(variables.resolve(wPrivateKey.getText()));
            }
            dialog.setFilterNames(
                new String[] {
                  BaseMessages.getString(PKG, "System.FileType.PEMFiles"),
                  BaseMessages.getString(PKG, "System.FileType.AllFiles")
                });
            if (dialog.open() != null) {
              wPrivateKey.setText(
                  dialog.getFilterPath()
                      + System.getProperty("file.separator")
                      + dialog.getFileName());
            }
          }
        });

    // Private key
    Label wlPrivateKey = new Label(wSettingsGroup, SWT.RIGHT | SWT.SINGLE);
    wlPrivateKey.setText(BaseMessages.getString(PKG, "SSHDialog.PrivateKey.Label"));
    FormData fdlPrivateKey = new FormData();
    fdlPrivateKey.left = new FormAttachment(0, 0);
    fdlPrivateKey.right = new FormAttachment(middle, 0);
    fdlPrivateKey.top = new FormAttachment(wUseKey, 2 * margin);
    wlPrivateKey.setLayoutData(fdlPrivateKey);
    wPrivateKey = new TextVar(variables, wSettingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wPrivateKey.setToolTipText(BaseMessages.getString(PKG, "SSHDialog.PrivateKey.Tooltip"));

    wPrivateKey.addModifyListener(lsMod);
    FormData fdPrivateKey = new FormData();
    fdPrivateKey.left = new FormAttachment(middle, margin);
    fdPrivateKey.top = new FormAttachment(wUseKey, 2 * margin);
    fdPrivateKey.right = new FormAttachment(wbFilename, -margin);
    wPrivateKey.setLayoutData(fdPrivateKey);

    // Passphraseline
    wPassphrase =
        new LabelTextVar(
            variables,
            wSettingsGroup,
            BaseMessages.getString(PKG, "SSHDialog.Passphrase.Label"),
            BaseMessages.getString(PKG, "SSHDialog.Passphrase.Tooltip"),
            true);
    wPassphrase.addModifyListener(lsMod);
    FormData fdPassphrase = new FormData();
    fdPassphrase.left = new FormAttachment(0, 0);
    fdPassphrase.top = new FormAttachment(wbFilename, margin);
    fdPassphrase.right = new FormAttachment(100, 0);
    wPassphrase.setLayoutData(fdPassphrase);

    // ProxyHostline
    wProxyHost =
        new LabelTextVar(
            variables,
            wSettingsGroup,
            BaseMessages.getString(PKG, "SSHDialog.ProxyHost.Label"),
            BaseMessages.getString(PKG, "SSHDialog.ProxyHost.Tooltip"));

    wProxyHost.addModifyListener(lsMod);
    FormData fdProxyHost = new FormData();
    fdProxyHost.left = new FormAttachment(0, 0);
    fdProxyHost.top = new FormAttachment(wPassphrase, 2 * margin);
    fdProxyHost.right = new FormAttachment(100, 0);
    wProxyHost.setLayoutData(fdProxyHost);

    // ProxyPortline
    wProxyPort =
        new LabelTextVar(
            variables,
            wSettingsGroup,
            BaseMessages.getString(PKG, "SSHDialog.ProxyPort.Label"),
            BaseMessages.getString(PKG, "SSHDialog.ProxyPort.Tooltip"));

    wProxyPort.addModifyListener(lsMod);
    FormData fdProxyPort = new FormData();
    fdProxyPort.left = new FormAttachment(0, 0);
    fdProxyPort.top = new FormAttachment(wProxyHost, margin);
    fdProxyPort.right = new FormAttachment(100, 0);
    wProxyPort.setLayoutData(fdProxyPort);

    // ProxyUsernameline
    wProxyUsername =
        new LabelTextVar(
            variables,
            wSettingsGroup,
            BaseMessages.getString(PKG, "SSHDialog.ProxyUsername.Label"),
            BaseMessages.getString(PKG, "SSHDialog.ProxyUsername.Tooltip"));

    wProxyUsername.addModifyListener(lsMod);
    FormData fdProxyUsername = new FormData();
    fdProxyUsername.left = new FormAttachment(0, 0);
    fdProxyUsername.top = new FormAttachment(wProxyPort, margin);
    fdProxyUsername.right = new FormAttachment(100, 0);
    wProxyUsername.setLayoutData(fdProxyUsername);

    // ProxyUsernameline
    wProxyPassword =
        new LabelTextVar(
            variables,
            wSettingsGroup,
            BaseMessages.getString(PKG, "SSHDialog.ProxyPassword.Label"),
            BaseMessages.getString(PKG, "SSHDialog.ProxyPassword.Tooltip"),
            true);

    wProxyPassword.addModifyListener(lsMod);
    FormData fdProxyPassword = new FormData();
    fdProxyPassword.left = new FormAttachment(0, 0);
    fdProxyPassword.top = new FormAttachment(wProxyUsername, margin);
    fdProxyPassword.right = new FormAttachment(100, 0);
    wProxyPassword.setLayoutData(fdProxyPassword);

    // Test connection button
    Button wTest = new Button(wSettingsGroup, SWT.PUSH);
    wTest.setText(BaseMessages.getString(PKG, "SSHDialog.TestConnection.Label"));
    FormData fdTest = new FormData();
    wTest.setToolTipText(BaseMessages.getString(PKG, "SSHDialog.TestConnection.Tooltip"));
    fdTest.top = new FormAttachment(wProxyPassword, 2 * margin);
    fdTest.right = new FormAttachment(100, 0);
    wTest.setLayoutData(fdTest);
    wTest.addListener(SWT.Selection, e -> test());

    FormData fdSettingsGroup = new FormData();
    fdSettingsGroup.left = new FormAttachment(0, margin);
    fdSettingsGroup.top = new FormAttachment(wTransformName, margin);
    fdSettingsGroup.right = new FormAttachment(100, -margin);
    wSettingsGroup.setLayoutData(fdSettingsGroup);

    // ///////////////////////////////
    // END OF Settings Fields GROUP //

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment(0, 0);
    fdGeneralComp.top = new FormAttachment(0, 0);
    fdGeneralComp.right = new FormAttachment(100, 0);
    fdGeneralComp.bottom = new FormAttachment(100, 0);
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF Settings TAB///
    // /
    CTabItem wSettingsTab = new CTabItem(wTabFolder, SWT.NONE);
    wSettingsTab.setText(BaseMessages.getString(PKG, "SSHDialog.Settings.Tab"));

    FormLayout settingsLayout = new FormLayout();
    settingsLayout.marginWidth = 3;
    settingsLayout.marginHeight = 3;

    Composite wSettingsComp = new Composite(wTabFolder, SWT.NONE);
    wSettingsComp.setLayout(settingsLayout);

    // ///////////////////////////////
    // START OF Output GROUP //
    // ///////////////////////////////

    Group wOutput = new Group(wSettingsComp, SWT.SHADOW_NONE);
    wOutput.setText(BaseMessages.getString(PKG, "SSHDialog.wOutput.Label"));

    FormLayout outputGroupLayout = new FormLayout();
    outputGroupLayout.marginWidth = 10;
    outputGroupLayout.marginHeight = 10;
    wOutput.setLayout(outputGroupLayout);

    // ResultOutFieldNameline
    wResultOutFieldName =
        new LabelTextVar(
            variables,
            wOutput,
            BaseMessages.getString(PKG, "SSHDialog.ResultOutFieldName.Label"),
            BaseMessages.getString(PKG, "SSHDialog.ResultOutFieldName.Tooltip"));
    wResultOutFieldName.addModifyListener(lsMod);
    FormData fdResultOutFieldName = new FormData();
    fdResultOutFieldName.left = new FormAttachment(0, 0);
    fdResultOutFieldName.top = new FormAttachment(wTransformName, margin);
    fdResultOutFieldName.right = new FormAttachment(100, 0);
    wResultOutFieldName.setLayoutData(fdResultOutFieldName);

    // ResultErrFieldNameline
    wResultErrFieldName =
        new LabelTextVar(
            variables,
            wOutput,
            BaseMessages.getString(PKG, "SSHDialog.ResultErrFieldName.Label"),
            BaseMessages.getString(PKG, "SSHDialog.ResultErrFieldName.Tooltip"));
    wResultErrFieldName.addModifyListener(lsMod);
    FormData fdResultErrFieldName = new FormData();
    fdResultErrFieldName.left = new FormAttachment(0, 0);
    fdResultErrFieldName.top = new FormAttachment(wResultOutFieldName, margin);
    fdResultErrFieldName.right = new FormAttachment(100, 0);
    wResultErrFieldName.setLayoutData(fdResultErrFieldName);

    FormData fdOutput = new FormData();
    fdOutput.left = new FormAttachment(0, margin);
    fdOutput.top = new FormAttachment(wTransformName, margin);
    fdOutput.right = new FormAttachment(100, -margin);
    wOutput.setLayoutData(fdOutput);

    // ///////////////////////////////
    // END OF Output Fields GROUP //

    // ////////////////////////
    // START OF Commands SETTINGS GROUP///
    // /
    Group wCommands = new Group(wSettingsComp, SWT.SHADOW_NONE);
    wCommands.setText(BaseMessages.getString(PKG, "SSHDialog.LogSettings.Group.Label"));

    FormLayout logSettingsgroupLayout = new FormLayout();
    logSettingsgroupLayout.marginWidth = 10;
    logSettingsgroupLayout.marginHeight = 10;

    wCommands.setLayout(logSettingsgroupLayout);

    // Is command defined in a Field
    Label wlDynamicCommand = new Label(wCommands, SWT.RIGHT);
    wlDynamicCommand.setText(BaseMessages.getString(PKG, "SSHDialog.dynamicCommand.Label"));
    FormData fdlDynamicBase = new FormData();
    fdlDynamicBase.left = new FormAttachment(0, margin);
    fdlDynamicBase.top = new FormAttachment(wOutput, margin);
    fdlDynamicBase.right = new FormAttachment(middle, -margin);
    wlDynamicCommand.setLayoutData(fdlDynamicBase);
    wDynamicCommand = new Button(wCommands, SWT.CHECK);
    wDynamicCommand.setToolTipText(BaseMessages.getString(PKG, "SSHDialog.dynamicCommand.Tooltip"));
    FormData fdDynamicCommand = new FormData();
    fdDynamicCommand.left = new FormAttachment(middle, margin);
    fdDynamicCommand.top = new FormAttachment(wlDynamicCommand, 0, SWT.CENTER);
    wDynamicCommand.setLayoutData(fdDynamicCommand);
    wDynamicCommand.addListener(
        SWT.Selection,
        e -> {
          activateDynamicCommand();
          input.setChanged();
        });

    // CommandField field
    wlCommandField = new Label(wCommands, SWT.RIGHT);
    wlCommandField.setText(BaseMessages.getString(PKG, "SSHDialog.MessageNameField.Label"));
    FormData fdlCommandField = new FormData();
    fdlCommandField.left = new FormAttachment(0, margin);
    fdlCommandField.right = new FormAttachment(middle, -margin);
    fdlCommandField.top = new FormAttachment(wDynamicCommand, margin);
    wlCommandField.setLayoutData(fdlCommandField);
    wCommandField = new CCombo(wCommands, SWT.BORDER | SWT.READ_ONLY);
    wCommandField.setEditable(true);
    wCommandField.addModifyListener(lsMod);
    FormData fdCommandField = new FormData();
    fdCommandField.left = new FormAttachment(middle, margin);
    fdCommandField.top = new FormAttachment(wDynamicCommand, margin);
    fdCommandField.right = new FormAttachment(100, 0);
    wCommandField.setLayoutData(fdCommandField);
    wCommandField.addListener(SWT.FocusIn, e -> get());

    // Command String
    wlCommand = new Label(wCommands, SWT.RIGHT);
    wlCommand.setText(BaseMessages.getString(PKG, "SSHDialog.Command.Label"));
    FormData fdlCommand = new FormData();
    fdlCommand.left = new FormAttachment(0, margin);
    fdlCommand.top = new FormAttachment(wCommandField, margin);
    fdlCommand.right = new FormAttachment(middle, -2 * margin);
    wlCommand.setLayoutData(fdlCommand);

    wCommand =
        new StyledTextComp(
            variables, wCommands, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    wCommand.setToolTipText(BaseMessages.getString(PKG, "SSHDialog.Command.Tooltip"));
    wCommand.addModifyListener(lsMod);
    FormData fdCommand = new FormData();
    fdCommand.left = new FormAttachment(middle, margin);
    fdCommand.top = new FormAttachment(wCommandField, margin);
    fdCommand.right = new FormAttachment(100, -2 * margin);
    fdCommand.bottom = new FormAttachment(100, -margin);
    wCommand.setLayoutData(fdCommand);

    FormData fdLogSettings = new FormData();
    fdLogSettings.left = new FormAttachment(0, margin);
    fdLogSettings.top = new FormAttachment(wOutput, margin);
    fdLogSettings.right = new FormAttachment(100, -margin);
    fdLogSettings.bottom = new FormAttachment(100, -margin);
    wCommands.setLayoutData(fdLogSettings);

    // ///////////////////////////////////////////////////////////
    // / END OF Log SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdSettingsComp = new FormData();
    fdSettingsComp.left = new FormAttachment(0, 0);
    fdSettingsComp.top = new FormAttachment(0, 0);
    fdSettingsComp.right = new FormAttachment(100, 0);
    fdSettingsComp.bottom = new FormAttachment(100, 0);
    wSettingsComp.setLayoutData(fdSettingsComp);

    wSettingsComp.layout();
    wSettingsTab.setControl(wSettingsComp);

    // ///////////////////////////////////////////////////////////
    // / END OF Settings TAB
    // ///////////////////////////////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wTransformName, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);

    wTabFolder.setSelection(0);
    getData();
    activateKey();
    activateDynamicCommand();
    input.setChanged(changed);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wDynamicCommand.setSelection(input.isDynamicCommandField());
    if (input.getCommand() != null) {
      wCommand.setText(input.getCommand());
    }
    if (input.getCommandFieldName() != null) {
      wCommandField.setText(input.getCommandFieldName());
    }
    if (input.getServerName() != null) {
      wServerName.setText(input.getServerName());
    }
    if (input.getPort() != null) {
      wPort.setText(input.getPort());
    }
    if (input.getUserName() != null) {
      wUserName.setText(input.getUserName());
    }
    if (input.getPassword() != null) {
      wPassword.setText(input.getPassword());
    }
    wUseKey.setSelection(input.isUsePrivateKey());
    if (input.getKeyFileName() != null) {
      wPrivateKey.setText(input.getKeyFileName());
    }
    if (input.getPassPhrase() != null) {
      wPassphrase.setText(input.getPassPhrase());
    }
    if (input.getStdOutFieldName() != null) {
      wResultOutFieldName.setText(input.getStdOutFieldName());
    }
    if (input.getStdErrFieldName() != null) {
      wResultErrFieldName.setText(input.getStdErrFieldName());
    }
    wTimeOut.setText(Const.NVL(input.getTimeOut(), "0"));
    if (input.getProxyHost() != null) {
      wProxyHost.setText(input.getProxyHost());
    }
    if (input.getProxyPort() != null) {
      wProxyPort.setText(input.getProxyPort());
    }
    if (input.getProxyUsername() != null) {
      wProxyUsername.setText(input.getProxyUsername());
    }
    if (input.getProxyPassword() != null) {
      wProxyPassword.setText(input.getProxyPassword());
    }

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void getInfo(SshMeta in) {
    in.setDynamicCommandField(wDynamicCommand.getSelection());
    in.setCommand(wCommand.getText());
    in.setCommandFieldName(wCommandField.getText());
    in.setServerName(wServerName.getText());
    in.setPort(wPort.getText());
    in.setUserName(wUserName.getText());
    in.setPassword(wPassword.getText());
    in.setUsePrivateKey(wUseKey.getSelection());
    in.setKeyFileName(wPrivateKey.getText());
    in.setPassPhrase(wPassphrase.getText());
    in.setStdOutFieldName(wResultOutFieldName.getText());
    in.setStdErrFieldName(wResultErrFieldName.getText());
    in.setTimeOut(wTimeOut.getText());
    in.setProxyHost(wProxyHost.getText());
    in.setProxyPort(wProxyPort.getText());
    in.setProxyUsername(wProxyUsername.getText());
    in.setProxyPassword(wProxyPassword.getText());
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    // This is the return value of the open() method
    transformName = wTransformName.getText();
    getInfo(input);

    dispose();
  }

  private void activateKey() {
    wPrivateKey.setEnabled(wUseKey.getSelection());
    wPassphrase.setEnabled(wUseKey.getSelection());
  }

  private void activateDynamicCommand() {
    wlCommand.setEnabled(!wDynamicCommand.getSelection());
    wCommand.setEnabled(!wDynamicCommand.getSelection());
    wlCommandField.setEnabled(wDynamicCommand.getSelection());
    wCommandField.setEnabled(wDynamicCommand.getSelection());
    wPreview.setEnabled(!wDynamicCommand.getSelection());
  }

  private void get() {
    if (!gotPreviousFields) {
      gotPreviousFields = true;
      try {
        String source = wCommandField.getText();

        wCommandField.removeAll();
        IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
        if (r != null) {
          wCommandField.setItems(r.getFieldNames());
          if (source != null) {
            wCommandField.setText(source);
          }
        }
      } catch (HopException ke) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "SSHDialog.FailedToGetFields.DialogTitle"),
            BaseMessages.getString(PKG, "SSHDialog.FailedToGetFields.DialogMessage"),
            ke);
      }
    }
  }

  private void test() {
    Exception exception = null;
    String errMsg = null;
    Connection connection = null;

    SshMeta meta = new SshMeta();
    getInfo(meta);

    try {
      connection = SshData.openConnection(variables, meta);
    } catch (Exception e) {
      exception = e;
      errMsg = e.getMessage();
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (Exception e) {
          /* Ignore */
        }
      }
    }
    if (exception==null) {
      MessageBox messageBox;
      messageBox = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      messageBox.setMessage(
          BaseMessages.getString(PKG, "SSHDialog.Connected.OK", meta.getServerName(), meta.getUserName()) + Const.CR);
      messageBox.setText(BaseMessages.getString(PKG, "SSHDialog.Connected.Title.Ok"));
      messageBox.open();
    } else {
      new ErrorDialog(shell, "Error",
          BaseMessages.getString(PKG, "SSHDialog.Connected.NOK.ConnectionBad", meta.getServerName(), meta.getUserName())
              + Const.CR
              + errMsg
              + Const.CR, exception);
    }
  }

  /**
   * Preview the data generated by this transform. This generates a pipeline using this transform &
   * a dummy and previews it.
   */
  private void preview() {
    // Create the Access input transform
    SshMeta oneMeta = new SshMeta();
    getInfo(oneMeta);

    PipelineMeta previewMeta =
        PipelinePreviewFactory.generatePreviewPipeline(
            pipelineMeta.getMetadataProvider(), oneMeta, wTransformName.getText());
    EnterNumberDialog numberDialog =
        new EnterNumberDialog(
            shell,
            1,
            BaseMessages.getString(PKG, "SSHDialog.NumberRows.DialogTitle"),
            BaseMessages.getString(PKG, "SSHDialog.NumberRows.DialogMessage"));

    int previewSize = numberDialog.open();
    if (previewSize > 0) {
      PipelinePreviewProgressDialog progressDialog =
          new PipelinePreviewProgressDialog(
              shell,
              variables,
              previewMeta,
              new String[] {wTransformName.getText()},
              new int[] {previewSize});
      progressDialog.open();

      if (!progressDialog.isCancelled()) {
        Pipeline pipeline = progressDialog.getPipeline();
        String loggingText = progressDialog.getLoggingText();

        if (pipeline.getResult() != null && pipeline.getResult().getNrErrors() > 0) {
          EnterTextDialog etd =
              new EnterTextDialog(
                  shell,
                  BaseMessages.getString(PKG, "System.Dialog.PreviewError.Title"),
                  BaseMessages.getString(PKG, "System.Dialog.PreviewError.Message"),
                  loggingText,
                  true);
          etd.setReadOnly();
          etd.open();
        }
        PreviewRowsDialog prd =
            new PreviewRowsDialog(
                shell,
                variables,
                SWT.NONE,
                wTransformName.getText(),
                progressDialog.getPreviewRowsMeta(wTransformName.getText()),
                progressDialog.getPreviewRows(wTransformName.getText()),
                loggingText);
        prd.open();
      }
    }
  }
}
