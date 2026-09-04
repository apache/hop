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
package org.apache.hop.vfs.sftp.metadata;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.NamingSchemeTypes;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.vfs.sftp.SftpConnections;
import org.apache.hop.vfs.sftp.client.SftpClient;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

/** Editor for the {@link SftpConnection} metadata type. */
@GuiPlugin(description = "Editor for SFTP connection metadata")
public class SftpConnectionEditor extends MetadataEditor<SftpConnection> {

  private static final Class<?> PKG = SftpConnectionEditor.class;

  private final int middle;
  private final int margin;

  private TextVar wName;
  private Text wDescription;

  private TextVar wServerName;
  private TextVar wServerPort;
  private TextVar wUsername;
  private PasswordTextVar wPassword;

  private Button wUseKeyFile;
  private TextVar wKeyFilename;
  private PasswordTextVar wKeyPassphrase;
  private ComboVar wStrictHostKeyChecking;
  private TextVar wKnownHostsFile;
  private TextVar wPreferredAuthentications;
  private TextVar wKeyExchangeAlgorithm;
  private Button wLoadOpenSshConfig;

  private ComboVar wCompression;
  private Button wUserDirIsRoot;
  private TextVar wConnectionTimeout;
  private TextVar wSessionTimeout;
  private TextVar wFileNameEncoding;
  private Button wDisableDetectExecChannel;
  private ComboVar wProxyType;
  private TextVar wProxyCommand;
  private TextVar wProxyHost;
  private TextVar wProxyPort;
  private TextVar wProxyUsername;
  private PasswordTextVar wProxyPassword;

  public SftpConnectionEditor(
      HopGui hopGui, MetadataManager<SftpConnection> manager, SftpConnection metadata) {
    super(hopGui, manager, metadata);
    middle = PropsUi.getInstance().getMiddlePct();
    margin = PropsUi.getMargin() + 2;
  }

  @Override
  public void createControl(Composite parent) {
    IVariables variables = manager.getVariables();

    wName =
        addTextVarLine(variables, parent, null, "SftpConnectionEditor.Name.Label")
            .asNameField(NamingSchemeTypes.HOP_METADATA);
    wDescription = addTextLine(parent, wName, "SftpConnectionEditor.Description.Label");

    Button wTest = new Button(parent, SWT.PUSH);
    wTest.setText(BaseMessages.getString(PKG, "SftpConnectionEditor.Test.Button"));
    PropsUi.setLook(wTest);
    FormData fdTest = new FormData();
    fdTest.left = new FormAttachment(middle, 0);
    fdTest.bottom = new FormAttachment(100, 0);
    wTest.setLayoutData(fdTest);
    wTest.addListener(SWT.Selection, e -> test());

    CTabFolder wTabFolder = new CTabFolder(parent, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);
    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wDescription, margin * 2);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wTest, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    createServerTab(variables, wTabFolder);
    createAuthenticationTab(variables, wTabFolder);
    createAdvancedTab(variables, wTabFolder);
    wTabFolder.setSelection(0);

    setWidgetsContent();

    wName.addModifyListener(e -> setChanged());
    wDescription.addModifyListener(e -> setChanged());
    for (Control control :
        new Control[] {
          wServerName,
          wServerPort,
          wUsername,
          wPassword,
          wKeyFilename,
          wKeyPassphrase,
          wStrictHostKeyChecking,
          wKnownHostsFile,
          wPreferredAuthentications,
          wKeyExchangeAlgorithm,
          wFileNameEncoding,
          wCompression,
          wConnectionTimeout,
          wSessionTimeout,
          wProxyType,
          wProxyCommand,
          wProxyHost,
          wProxyPort,
          wProxyUsername,
          wProxyPassword
        }) {
      control.addListener(SWT.Modify, e -> setChanged());
    }
    wUseKeyFile.addListener(
        SWT.Selection,
        e -> {
          setChanged();
          enableFields();
        });
    wUserDirIsRoot.addListener(SWT.Selection, e -> setChanged());
    wLoadOpenSshConfig.addListener(SWT.Selection, e -> setChanged());
    wDisableDetectExecChannel.addListener(SWT.Selection, e -> setChanged());

    enableFields();
  }

  private void createServerTab(IVariables variables, CTabFolder tabFolder) {
    Composite composite = addTab(tabFolder, "SftpConnectionEditor.Tab.Server.Label");
    wServerName =
        addTextVarLine(variables, composite, null, "SftpConnectionEditor.ServerName.Label");
    wServerPort =
        addTextVarLine(variables, composite, wServerName, "SftpConnectionEditor.ServerPort.Label");
    wUsername =
        addTextVarLine(variables, composite, wServerPort, "SftpConnectionEditor.Username.Label");
    wPassword =
        addPasswordLine(variables, composite, wUsername, "SftpConnectionEditor.Password.Label");
  }

  private void createAuthenticationTab(IVariables variables, CTabFolder tabFolder) {
    Composite composite = addTab(tabFolder, "SftpConnectionEditor.Tab.Authentication.Label");
    wUseKeyFile = addCheckBox(composite, null, "SftpConnectionEditor.UseKeyFile.Label");

    Label wlKeyFilename =
        addLabel(composite, wUseKeyFile, "SftpConnectionEditor.KeyFilename.Label");
    Button wbKeyFilename = new Button(composite, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbKeyFilename);
    wbKeyFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbKeyFilename = new FormData();
    fdbKeyFilename.right = new FormAttachment(100, 0);
    fdbKeyFilename.top = new FormAttachment(wlKeyFilename, 0, SWT.CENTER);
    wbKeyFilename.setLayoutData(fdbKeyFilename);
    wKeyFilename = new TextVar(variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wKeyFilename);
    FormData fdKeyFilename = new FormData();
    fdKeyFilename.top = new FormAttachment(wlKeyFilename, 0, SWT.CENTER);
    fdKeyFilename.left = new FormAttachment(middle, 0);
    fdKeyFilename.right = new FormAttachment(wbKeyFilename, -margin);
    wKeyFilename.setLayoutData(fdKeyFilename);
    wbKeyFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                hopGui.getShell(),
                wKeyFilename,
                variables,
                new String[] {"*"},
                new String[] {BaseMessages.getString(PKG, "System.FileType.AllFiles")},
                true));

    wKeyPassphrase =
        addPasswordLine(
            variables, composite, wKeyFilename, "SftpConnectionEditor.KeyPassphrase.Label");
    wStrictHostKeyChecking =
        addComboVarLine(
            variables,
            composite,
            wKeyPassphrase,
            "SftpConnectionEditor.StrictHostKeyChecking.Label",
            new String[] {"no", "yes", "ask"});
    wKnownHostsFile =
        addTextVarLine(
            variables,
            composite,
            wStrictHostKeyChecking,
            "SftpConnectionEditor.KnownHostsFile.Label");
    wLoadOpenSshConfig =
        addCheckBox(composite, wKnownHostsFile, "SftpConnectionEditor.LoadOpenSshConfig.Label");
    wPreferredAuthentications =
        addTextVarLine(
            variables,
            composite,
            wLoadOpenSshConfig,
            "SftpConnectionEditor.PreferredAuthentications.Label");
    wKeyExchangeAlgorithm =
        addTextVarLine(
            variables,
            composite,
            wPreferredAuthentications,
            "SftpConnectionEditor.KeyExchangeAlgorithm.Label");
  }

  private void createAdvancedTab(IVariables variables, CTabFolder tabFolder) {
    Composite composite = addTab(tabFolder, "SftpConnectionEditor.Tab.Advanced.Label");
    wCompression =
        addComboVarLine(
            variables,
            composite,
            null,
            "SftpConnectionEditor.Compression.Label",
            new String[] {"none", "zlib"});
    wUserDirIsRoot =
        addCheckBox(composite, wCompression, "SftpConnectionEditor.UserDirIsRoot.Label");
    wFileNameEncoding =
        addTextVarLine(
            variables, composite, wUserDirIsRoot, "SftpConnectionEditor.FileNameEncoding.Label");
    wDisableDetectExecChannel =
        addCheckBox(
            composite, wFileNameEncoding, "SftpConnectionEditor.DisableDetectExecChannel.Label");
    wConnectionTimeout =
        addTextVarLine(
            variables,
            composite,
            wDisableDetectExecChannel,
            "SftpConnectionEditor.ConnectionTimeout.Label");
    wSessionTimeout =
        addTextVarLine(
            variables, composite, wConnectionTimeout, "SftpConnectionEditor.SessionTimeout.Label");
    wProxyType =
        addComboVarLine(
            variables,
            composite,
            wSessionTimeout,
            "SftpConnectionEditor.ProxyType.Label",
            new String[] {"", SftpClient.PROXY_TYPE_HTTP, SftpClient.PROXY_TYPE_SOCKS5, "STREAM"});
    wProxyCommand =
        addTextVarLine(variables, composite, wProxyType, "SftpConnectionEditor.ProxyCommand.Label");
    wProxyHost =
        addTextVarLine(variables, composite, wProxyCommand, "SftpConnectionEditor.ProxyHost.Label");
    wProxyPort =
        addTextVarLine(variables, composite, wProxyHost, "SftpConnectionEditor.ProxyPort.Label");
    wProxyUsername =
        addTextVarLine(
            variables, composite, wProxyPort, "SftpConnectionEditor.ProxyUsername.Label");
    wProxyPassword =
        addPasswordLine(
            variables, composite, wProxyUsername, "SftpConnectionEditor.ProxyPassword.Label");
  }

  private Composite addTab(CTabFolder tabFolder, String messageKey) {
    CTabItem tabItem = new CTabItem(tabFolder, SWT.NONE);
    tabItem.setFont(GuiResource.getInstance().getFontDefault());
    tabItem.setText(BaseMessages.getString(PKG, messageKey));
    Composite composite = new Composite(tabFolder, SWT.NONE);
    PropsUi.setLook(composite);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    composite.setLayout(layout);
    tabItem.setControl(composite);
    return composite;
  }

  private Label addLabel(Composite parent, Control previous, String messageKey) {
    Label label = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(label);
    label.setText(BaseMessages.getString(PKG, messageKey));
    FormData fdLabel = new FormData();
    fdLabel.top =
        previous == null ? new FormAttachment(0, margin) : new FormAttachment(previous, margin);
    fdLabel.left = new FormAttachment(0, 0);
    fdLabel.right = new FormAttachment(middle, -margin);
    label.setLayoutData(fdLabel);
    return label;
  }

  private FormData fieldLayout(Label label) {
    FormData fdField = new FormData();
    fdField.top = new FormAttachment(label, 0, SWT.CENTER);
    fdField.left = new FormAttachment(middle, 0);
    fdField.right = new FormAttachment(100, 0);
    return fdField;
  }

  private Text addTextLine(Composite parent, Control previous, String messageKey) {
    Label label = addLabel(parent, previous, messageKey);
    Text text = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(text);
    FormData fdText = fieldLayout(label);
    fdText.right = new FormAttachment(95, 0);
    text.setLayoutData(fdText);
    return text;
  }

  private TextVar addTextVarLine(
      IVariables variables, Composite parent, Control previous, String messageKey) {
    Label label = addLabel(parent, previous, messageKey);
    TextVar text = new TextVar(variables, parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(text);
    text.setLayoutData(fieldLayout(label));
    return text;
  }

  private PasswordTextVar addPasswordLine(
      IVariables variables, Composite parent, Control previous, String messageKey) {
    Label label = addLabel(parent, previous, messageKey);
    PasswordTextVar text =
        new PasswordTextVar(variables, parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(text);
    text.setLayoutData(fieldLayout(label));
    return text;
  }

  private ComboVar addComboVarLine(
      IVariables variables,
      Composite parent,
      Control previous,
      String messageKey,
      String[] options) {
    Label label = addLabel(parent, previous, messageKey);
    ComboVar combo = new ComboVar(variables, parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(combo);
    combo.setItems(options);
    combo.setLayoutData(fieldLayout(label));
    return combo;
  }

  private Button addCheckBox(Composite parent, Control previous, String messageKey) {
    Label label = addLabel(parent, previous, messageKey);
    Button button = new Button(parent, SWT.CHECK);
    PropsUi.setLook(button);
    button.setLayoutData(fieldLayout(label));
    return button;
  }

  private void enableFields() {
    boolean useKeyFile = wUseKeyFile.getSelection();
    wKeyFilename.setEnabled(useKeyFile);
    wKeyPassphrase.setEnabled(useKeyFile);
  }

  /** Connect with what's on screen, without saving first. */
  private void test() {
    SftpConnection connection = new SftpConnection();
    getWidgetsContent(connection);
    SftpClient client = null;
    try {
      client = SftpConnections.createClient(manager.getVariables(), connection);
      String folder = client.pwd();
      MessageBox box = new MessageBox(hopGui.getShell(), SWT.OK | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "SftpConnectionEditor.Test.Success.Title"));
      box.setMessage(
          BaseMessages.getString(
              PKG,
              "SftpConnectionEditor.Test.Success.Message",
              Const.NVL(connection.getServerName(), ""),
              Const.NVL(folder, "")));
      box.open();
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "SftpConnectionEditor.Test.Error.Title"),
          BaseMessages.getString(PKG, "SftpConnectionEditor.Test.Error.Message"),
          e);
    } finally {
      if (client != null) {
        client.disconnect();
      }
    }
  }

  @Override
  public void setWidgetsContent() {
    SftpConnection c = getMetadata();
    wName.setText(Const.NVL(c.getName(), ""));
    wDescription.setText(Const.NVL(c.getDescription(), ""));
    wServerName.setText(Const.NVL(c.getServerName(), ""));
    wServerPort.setText(Const.NVL(c.getServerPort(), ""));
    wUsername.setText(Const.NVL(c.getUsername(), ""));
    wPassword.setText(Const.NVL(c.getPassword(), ""));
    wUseKeyFile.setSelection(c.isUseKeyFile());
    wKeyFilename.setText(Const.NVL(c.getKeyFilename(), ""));
    wKeyPassphrase.setText(Const.NVL(c.getKeyPassphrase(), ""));
    wStrictHostKeyChecking.setText(Const.NVL(c.getStrictHostKeyChecking(), "no"));
    wKnownHostsFile.setText(Const.NVL(c.getKnownHostsFile(), ""));
    wPreferredAuthentications.setText(Const.NVL(c.getPreferredAuthentications(), ""));
    wKeyExchangeAlgorithm.setText(Const.NVL(c.getKeyExchangeAlgorithm(), ""));
    wLoadOpenSshConfig.setSelection(c.isLoadOpenSshConfig());
    wCompression.setText(Const.NVL(c.getCompression(), ""));
    wUserDirIsRoot.setSelection(c.isUserDirIsRoot());
    wConnectionTimeout.setText(Const.NVL(c.getConnectionTimeout(), ""));
    wSessionTimeout.setText(Const.NVL(c.getSessionTimeout(), ""));
    wFileNameEncoding.setText(Const.NVL(c.getFileNameEncoding(), ""));
    wDisableDetectExecChannel.setSelection(c.isDisableDetectExecChannel());
    wProxyType.setText(Const.NVL(c.getProxyType(), ""));
    wProxyCommand.setText(Const.NVL(c.getProxyCommand(), ""));
    wProxyHost.setText(Const.NVL(c.getProxyHost(), ""));
    wProxyPort.setText(Const.NVL(c.getProxyPort(), ""));
    wProxyUsername.setText(Const.NVL(c.getProxyUsername(), ""));
    wProxyPassword.setText(Const.NVL(c.getProxyPassword(), ""));
    enableFields();
  }

  @Override
  public void getWidgetsContent(SftpConnection c) {
    c.setName(wName.getText());
    c.setDescription(wDescription.getText());
    c.setServerName(wServerName.getText());
    c.setServerPort(wServerPort.getText());
    c.setUsername(wUsername.getText());
    c.setPassword(wPassword.getText());
    c.setUseKeyFile(wUseKeyFile.getSelection());
    c.setKeyFilename(wKeyFilename.getText());
    c.setKeyPassphrase(wKeyPassphrase.getText());
    c.setStrictHostKeyChecking(wStrictHostKeyChecking.getText());
    c.setKnownHostsFile(wKnownHostsFile.getText());
    c.setPreferredAuthentications(wPreferredAuthentications.getText());
    c.setKeyExchangeAlgorithm(wKeyExchangeAlgorithm.getText());
    c.setLoadOpenSshConfig(wLoadOpenSshConfig.getSelection());
    c.setCompression(wCompression.getText());
    c.setUserDirIsRoot(wUserDirIsRoot.getSelection());
    c.setConnectionTimeout(wConnectionTimeout.getText());
    c.setSessionTimeout(wSessionTimeout.getText());
    c.setFileNameEncoding(wFileNameEncoding.getText());
    c.setDisableDetectExecChannel(wDisableDetectExecChannel.getSelection());
    c.setProxyType(wProxyType.getText());
    c.setProxyCommand(wProxyCommand.getText());
    c.setProxyHost(wProxyHost.getText());
    c.setProxyPort(wProxyPort.getText());
    c.setProxyUsername(wProxyUsername.getText());
    c.setProxyPassword(wProxyPassword.getText());
  }

  @Override
  public boolean setFocus() {
    if (wName == null || wName.isDisposed()) {
      return false;
    }
    return wName.setFocus();
  }

  @Override
  public void save() throws HopException {
    super.save();
    // The name of a connection is a VFS scheme: re-register the providers so the new or changed
    // connection is picked up right away.
    //
    HopVfs.refresh(hopGui.getVariables());
  }
}
