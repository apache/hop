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
package org.apache.hop.vfs.ftp.metadata;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
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
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.vfs.ftp.FtpClientFactory;
import org.apache.hop.vfs.ftp.FtpDataChannelProtection;
import org.apache.hop.vfs.ftp.FtpSecurityMode;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
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

/** Editor for the {@link FtpConnection} metadata type. */
@GuiPlugin(description = "Editor for FTP connection metadata")
public class FtpConnectionEditor extends MetadataEditor<FtpConnection> {

  private static final Class<?> PKG = FtpConnectionEditor.class;

  private final int middle;
  private final int margin;

  private Text wName;
  private Text wDescription;

  private CCombo wSecurityMode;
  private TextVar wServerName;
  private TextVar wServerPort;
  private TextVar wUserName;
  private PasswordTextVar wPassword;

  private Button wVerifyServerCertificate;
  private CCombo wDataChannelProtection;
  private TextVar wClientCertificateFile;
  private PasswordTextVar wClientCertificatePassword;
  private TextVar wClientCertificateAlias;
  private TextVar wClientCertificateType;

  private Button wBinaryMode;
  private Button wActiveConnection;
  private TextVar wActivePortRangeFrom;
  private TextVar wActivePortRangeTo;
  private Button wUserDirIsRoot;
  private Button wRemoteVerification;
  private Button wAutodetectUtf8;
  private TextVar wControlEncoding;
  private TextVar wConnectTimeout;
  private TextVar wSocketTimeout;
  private TextVar wDataTimeout;
  private TextVar wControlKeepAliveTimeout;
  private TextVar wControlKeepAliveReplyTimeout;

  private TextVar wEntryParser;
  private TextVar wServerLanguageCode;
  private TextVar wServerTimeZone;
  private TextVar wDefaultDateFormat;
  private TextVar wRecentDateFormat;
  private TextVar wShortMonthNames;
  private Button wMdtmLastModifiedTime;

  private TextVar wProxyHost;
  private TextVar wProxyPort;
  private TextVar wProxyUsername;
  private PasswordTextVar wProxyPassword;
  private TextVar wSocksProxyHost;
  private TextVar wSocksProxyPort;
  private TextVar wSocksProxyUsername;
  private PasswordTextVar wSocksProxyPassword;

  public FtpConnectionEditor(
      HopGui hopGui, MetadataManager<FtpConnection> manager, FtpConnection metadata) {
    super(hopGui, manager, metadata);
    middle = PropsUi.getInstance().getMiddlePct();
    margin = PropsUi.getMargin() + 2;
  }

  @Override
  public void createControl(Composite parent) {
    IVariables variables = manager.getVariables();

    wName = addTextLine(parent, null, "FtpConnectionEditor.Name.Label");
    wDescription = addTextLine(parent, wName, "FtpConnectionEditor.Description.Label");

    Button wTest = new Button(parent, SWT.PUSH);
    wTest.setText(BaseMessages.getString(PKG, "FtpConnectionEditor.Test.Button"));
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
    createSecurityTab(variables, wTabFolder);
    createAdvancedTab(variables, wTabFolder);
    createListingTab(variables, wTabFolder);
    createProxyTab(variables, wTabFolder);
    wTabFolder.setSelection(0);

    setWidgetsContent();

    wName.addModifyListener(e -> setChanged());
    wDescription.addModifyListener(e -> setChanged());
    for (Control control :
        new Control[] {
          wServerName,
          wServerPort,
          wUserName,
          wPassword,
          wClientCertificateFile,
          wClientCertificatePassword,
          wClientCertificateAlias,
          wClientCertificateType,
          wControlEncoding,
          wActivePortRangeFrom,
          wActivePortRangeTo,
          wConnectTimeout,
          wSocketTimeout,
          wDataTimeout,
          wControlKeepAliveTimeout,
          wControlKeepAliveReplyTimeout,
          wEntryParser,
          wServerLanguageCode,
          wServerTimeZone,
          wDefaultDateFormat,
          wRecentDateFormat,
          wShortMonthNames,
          wProxyHost,
          wProxyPort,
          wProxyUsername,
          wProxyPassword,
          wSocksProxyHost,
          wSocksProxyPort,
          wSocksProxyUsername,
          wSocksProxyPassword
        }) {
      control.addListener(SWT.Modify, e -> setChanged());
    }
    for (Button button :
        new Button[] {
          wBinaryMode,
          wActiveConnection,
          wUserDirIsRoot,
          wRemoteVerification,
          wAutodetectUtf8,
          wMdtmLastModifiedTime,
        }) {
      button.addListener(SWT.Selection, e -> setChanged());
    }
    wVerifyServerCertificate.addListener(SWT.Selection, e -> setChanged());
    wDataChannelProtection.addListener(SWT.Modify, e -> setChanged());
    wActiveConnection.addListener(SWT.Selection, e -> enableFields());
    wSecurityMode.addListener(
        SWT.Modify,
        e -> {
          setChanged();
          securityModeChanged();
        });

    enableFields();
  }

  private void createServerTab(IVariables variables, CTabFolder tabFolder) {
    Composite composite = addTab(tabFolder, "FtpConnectionEditor.Tab.Server.Label");

    Label label = addLabel(composite, null, "FtpConnectionEditor.SecurityMode.Label");
    wSecurityMode = new CCombo(composite, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wSecurityMode);
    wSecurityMode.setItems(FtpSecurityMode.getDescriptions());
    wSecurityMode.setLayoutData(fieldLayout(label));

    wServerName =
        addTextVarLine(variables, composite, wSecurityMode, "FtpConnectionEditor.ServerName.Label");
    wServerPort =
        addTextVarLine(variables, composite, wServerName, "FtpConnectionEditor.ServerPort.Label");
    wUserName =
        addTextVarLine(variables, composite, wServerPort, "FtpConnectionEditor.UserName.Label");
    wPassword =
        addPasswordLine(variables, composite, wUserName, "FtpConnectionEditor.Password.Label");
  }

  /** Everything which only means something once the connection is a TLS one. */
  private void createSecurityTab(IVariables variables, CTabFolder tabFolder) {
    Composite composite = addTab(tabFolder, "FtpConnectionEditor.Tab.Security.Label");
    wVerifyServerCertificate =
        addCheckBox(composite, null, "FtpConnectionEditor.VerifyServerCertificate.Label");
    Label protectionLabel =
        addLabel(
            composite, wVerifyServerCertificate, "FtpConnectionEditor.DataChannelProtection.Label");
    wDataChannelProtection = new CCombo(composite, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wDataChannelProtection);
    wDataChannelProtection.setItems(FtpDataChannelProtection.getDescriptions());
    wDataChannelProtection.setToolTipText(
        BaseMessages.getString(PKG, "FtpConnectionEditor.DataChannelProtection.Tooltip"));
    wDataChannelProtection.setLayoutData(fieldLayout(protectionLabel));
    wClientCertificateFile =
        addBrowsableFileLine(
            variables,
            composite,
            wDataChannelProtection,
            "FtpConnectionEditor.ClientCertificateFile.Label");
    wClientCertificatePassword =
        addPasswordLine(
            variables,
            composite,
            wClientCertificateFile,
            "FtpConnectionEditor.ClientCertificatePassword.Label");
    wClientCertificateAlias =
        addTextVarLine(
            variables,
            composite,
            wClientCertificatePassword,
            "FtpConnectionEditor.ClientCertificateAlias.Label");
    wClientCertificateType =
        addTextVarLine(
            variables,
            composite,
            wClientCertificateAlias,
            "FtpConnectionEditor.ClientCertificateType.Label");
  }

  private void createAdvancedTab(IVariables variables, CTabFolder tabFolder) {
    Composite composite = addTab(tabFolder, "FtpConnectionEditor.Tab.Advanced.Label");
    wBinaryMode = addCheckBox(composite, null, "FtpConnectionEditor.BinaryMode.Label");
    wActiveConnection =
        addCheckBox(composite, wBinaryMode, "FtpConnectionEditor.ActiveConnection.Label");
    wActivePortRangeFrom =
        addTextVarLine(
            variables,
            composite,
            wActiveConnection,
            "FtpConnectionEditor.ActivePortRangeFrom.Label");
    wActivePortRangeTo =
        addTextVarLine(
            variables,
            composite,
            wActivePortRangeFrom,
            "FtpConnectionEditor.ActivePortRangeTo.Label");
    wUserDirIsRoot =
        addCheckBox(composite, wActivePortRangeTo, "FtpConnectionEditor.UserDirIsRoot.Label");
    wRemoteVerification =
        addCheckBox(composite, wUserDirIsRoot, "FtpConnectionEditor.RemoteVerification.Label");
    wControlEncoding =
        addTextVarLine(
            variables, composite, wRemoteVerification, "FtpConnectionEditor.ControlEncoding.Label");
    wAutodetectUtf8 =
        addCheckBox(composite, wControlEncoding, "FtpConnectionEditor.AutodetectUtf8.Label");
    wConnectTimeout =
        addTextVarLine(
            variables, composite, wAutodetectUtf8, "FtpConnectionEditor.ConnectTimeout.Label");
    wSocketTimeout =
        addTextVarLine(
            variables, composite, wConnectTimeout, "FtpConnectionEditor.SocketTimeout.Label");
    wDataTimeout =
        addTextVarLine(
            variables, composite, wSocketTimeout, "FtpConnectionEditor.DataTimeout.Label");
    wControlKeepAliveTimeout =
        addTextVarLine(
            variables,
            composite,
            wDataTimeout,
            "FtpConnectionEditor.ControlKeepAliveTimeout.Label");
    wControlKeepAliveReplyTimeout =
        addTextVarLine(
            variables,
            composite,
            wControlKeepAliveTimeout,
            "FtpConnectionEditor.ControlKeepAliveReplyTimeout.Label");
  }

  /** How the directory listings of this server should be read. All of it is optional. */
  private void createListingTab(IVariables variables, CTabFolder tabFolder) {
    Composite composite = addTab(tabFolder, "FtpConnectionEditor.Tab.Listing.Label");
    wEntryParser =
        addTextVarLine(variables, composite, null, "FtpConnectionEditor.EntryParser.Label");
    wServerLanguageCode =
        addTextVarLine(
            variables, composite, wEntryParser, "FtpConnectionEditor.ServerLanguageCode.Label");
    wServerTimeZone =
        addTextVarLine(
            variables, composite, wServerLanguageCode, "FtpConnectionEditor.ServerTimeZone.Label");
    wDefaultDateFormat =
        addTextVarLine(
            variables, composite, wServerTimeZone, "FtpConnectionEditor.DefaultDateFormat.Label");
    wRecentDateFormat =
        addTextVarLine(
            variables, composite, wDefaultDateFormat, "FtpConnectionEditor.RecentDateFormat.Label");
    wShortMonthNames =
        addTextVarLine(
            variables, composite, wRecentDateFormat, "FtpConnectionEditor.ShortMonthNames.Label");
    wMdtmLastModifiedTime =
        addCheckBox(composite, wShortMonthNames, "FtpConnectionEditor.MdtmLastModifiedTime.Label");
  }

  private void createProxyTab(IVariables variables, CTabFolder tabFolder) {
    Composite composite = addTab(tabFolder, "FtpConnectionEditor.Tab.Proxy.Label");
    wProxyHost = addTextVarLine(variables, composite, null, "FtpConnectionEditor.ProxyHost.Label");
    wProxyPort =
        addTextVarLine(variables, composite, wProxyHost, "FtpConnectionEditor.ProxyPort.Label");
    wProxyUsername =
        addTextVarLine(variables, composite, wProxyPort, "FtpConnectionEditor.ProxyUsername.Label");
    wProxyPassword =
        addPasswordLine(
            variables, composite, wProxyUsername, "FtpConnectionEditor.ProxyPassword.Label");
    wSocksProxyHost =
        addTextVarLine(
            variables, composite, wProxyPassword, "FtpConnectionEditor.SocksProxyHost.Label");
    wSocksProxyPort =
        addTextVarLine(
            variables, composite, wSocksProxyHost, "FtpConnectionEditor.SocksProxyPort.Label");
    wSocksProxyUsername =
        addTextVarLine(
            variables, composite, wSocksProxyPort, "FtpConnectionEditor.SocksProxyUsername.Label");
    wSocksProxyPassword =
        addPasswordLine(
            variables,
            composite,
            wSocksProxyUsername,
            "FtpConnectionEditor.SocksProxyPassword.Label");
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

  /** A file name with a Browse button, for a file which is read through VFS. */
  private TextVar addBrowsableFileLine(
      IVariables variables, Composite parent, Control previous, String messageKey) {
    Label label = addLabel(parent, previous, messageKey);

    Button browse = new Button(parent, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(browse);
    browse.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdBrowse = new FormData();
    fdBrowse.right = new FormAttachment(100, 0);
    fdBrowse.top = new FormAttachment(label, 0, SWT.CENTER);
    browse.setLayoutData(fdBrowse);

    TextVar text = new TextVar(variables, parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(text);
    FormData fdText = fieldLayout(label);
    fdText.right = new FormAttachment(browse, -margin);
    text.setLayoutData(fdText);

    browse.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                hopGui.getShell(),
                text,
                variables,
                new String[] {"*.p12;*.pfx", "*.jks", "*"},
                new String[] {
                  BaseMessages.getString(PKG, "FtpConnectionEditor.Filetype.Pkcs12"),
                  BaseMessages.getString(PKG, "FtpConnectionEditor.Filetype.Jks"),
                  BaseMessages.getString(PKG, "System.FileType.AllFiles")
                },
                true));
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

  /** The TLS settings only mean something for the FTPS security modes. */
  private void enableFields() {
    boolean secure = selectedSecurityMode().isSecure();
    for (Control control :
        new Control[] {
          wVerifyServerCertificate,
          wDataChannelProtection,
          wClientCertificateFile,
          wClientCertificatePassword,
          wClientCertificateAlias,
          wClientCertificateType
        }) {
      control.setEnabled(secure);
    }

    // A port range is what the server is told to connect back to, so only in active mode.
    boolean active = wActiveConnection.getSelection();
    wActivePortRangeFrom.setEnabled(active);
    wActivePortRangeTo.setEnabled(active);
  }

  /** Follow the default port of the security mode, as long as the user didn't pick one. */
  private void securityModeChanged() {
    String port = wServerPort.getText();
    boolean isDefaultPort = false;
    for (FtpSecurityMode mode : FtpSecurityMode.values()) {
      isDefaultPort |= Integer.toString(mode.getDefaultPort()).equals(port);
    }
    if (Utils.isEmpty(port) || isDefaultPort) {
      wServerPort.setText(Integer.toString(selectedSecurityMode().getDefaultPort()));
    }
    enableFields();
  }

  private FtpSecurityMode selectedSecurityMode() {
    return FtpSecurityMode.lookupDescription(wSecurityMode.getText());
  }

  /** Connect with what's on screen, without saving first. */
  private void test() {
    FtpConnection connection = new FtpConnection();
    getWidgetsContent(connection);
    FTPClient client = null;
    try {
      client = FtpClientFactory.connectAndLogin(LogChannel.UI, manager.getVariables(), connection);
      String folder = client.printWorkingDirectory();
      MessageBox box = new MessageBox(hopGui.getShell(), SWT.OK | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "FtpConnectionEditor.Test.Success.Title"));
      box.setMessage(
          BaseMessages.getString(
              PKG,
              "FtpConnectionEditor.Test.Success.Message",
              Const.NVL(connection.getServerName(), ""),
              Const.NVL(folder, "")));
      box.open();
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "FtpConnectionEditor.Test.Error.Title"),
          BaseMessages.getString(PKG, "FtpConnectionEditor.Test.Error.Message"),
          e);
    } finally {
      FtpClientFactory.disconnectQuietly(LogChannel.UI, client);
    }
  }

  @Override
  public void setWidgetsContent() {
    FtpConnection c = getMetadata();
    wName.setText(Const.NVL(c.getName(), ""));
    wDescription.setText(Const.NVL(c.getDescription(), ""));
    wSecurityMode.setText(c.getSecurityMode().getDescription());
    wServerName.setText(Const.NVL(c.getServerName(), ""));
    wServerPort.setText(Const.NVL(c.getServerPort(), ""));
    wUserName.setText(Const.NVL(c.getUserName(), ""));
    wPassword.setText(Const.NVL(c.getPassword(), ""));
    wVerifyServerCertificate.setSelection(c.isVerifyServerCertificate());
    wDataChannelProtection.setText(c.getDataChannelProtection().getDescription());
    wClientCertificateFile.setText(Const.NVL(c.getClientCertificateFile(), ""));
    wClientCertificatePassword.setText(Const.NVL(c.getClientCertificatePassword(), ""));
    wClientCertificateAlias.setText(Const.NVL(c.getClientCertificateAlias(), ""));
    wClientCertificateType.setText(Const.NVL(c.getClientCertificateType(), ""));
    wBinaryMode.setSelection(c.isBinaryMode());
    wActiveConnection.setSelection(c.isActiveConnection());
    wActivePortRangeFrom.setText(Const.NVL(c.getActivePortRangeFrom(), ""));
    wActivePortRangeTo.setText(Const.NVL(c.getActivePortRangeTo(), ""));
    wUserDirIsRoot.setSelection(c.isUserDirIsRoot());
    wRemoteVerification.setSelection(c.isRemoteVerification());
    wAutodetectUtf8.setSelection(c.isAutodetectUtf8());
    wControlEncoding.setText(Const.NVL(c.getControlEncoding(), ""));
    wConnectTimeout.setText(Const.NVL(c.getConnectTimeout(), ""));
    wSocketTimeout.setText(Const.NVL(c.getSocketTimeout(), ""));
    wDataTimeout.setText(Const.NVL(c.getDataTimeout(), ""));
    wControlKeepAliveTimeout.setText(Const.NVL(c.getControlKeepAliveTimeout(), ""));
    wControlKeepAliveReplyTimeout.setText(Const.NVL(c.getControlKeepAliveReplyTimeout(), ""));
    wEntryParser.setText(Const.NVL(c.getEntryParser(), ""));
    wServerLanguageCode.setText(Const.NVL(c.getServerLanguageCode(), ""));
    wServerTimeZone.setText(Const.NVL(c.getServerTimeZone(), ""));
    wDefaultDateFormat.setText(Const.NVL(c.getDefaultDateFormat(), ""));
    wRecentDateFormat.setText(Const.NVL(c.getRecentDateFormat(), ""));
    wShortMonthNames.setText(Const.NVL(c.getShortMonthNames(), ""));
    wMdtmLastModifiedTime.setSelection(c.isMdtmLastModifiedTime());
    wProxyHost.setText(Const.NVL(c.getProxyHost(), ""));
    wProxyPort.setText(Const.NVL(c.getProxyPort(), ""));
    wProxyUsername.setText(Const.NVL(c.getProxyUsername(), ""));
    wProxyPassword.setText(Const.NVL(c.getProxyPassword(), ""));
    wSocksProxyHost.setText(Const.NVL(c.getSocksProxyHost(), ""));
    wSocksProxyPort.setText(Const.NVL(c.getSocksProxyPort(), ""));
    wSocksProxyUsername.setText(Const.NVL(c.getSocksProxyUsername(), ""));
    wSocksProxyPassword.setText(Const.NVL(c.getSocksProxyPassword(), ""));
    enableFields();
  }

  @Override
  public void getWidgetsContent(FtpConnection c) {
    c.setName(wName.getText());
    c.setDescription(wDescription.getText());
    c.setSecurityMode(selectedSecurityMode());
    c.setServerName(wServerName.getText());
    c.setServerPort(wServerPort.getText());
    c.setUserName(wUserName.getText());
    c.setPassword(wPassword.getText());
    c.setVerifyServerCertificate(wVerifyServerCertificate.getSelection());
    c.setDataChannelProtection(
        FtpDataChannelProtection.lookupDescription(wDataChannelProtection.getText()));
    c.setClientCertificateFile(wClientCertificateFile.getText());
    c.setClientCertificatePassword(wClientCertificatePassword.getText());
    c.setClientCertificateAlias(wClientCertificateAlias.getText());
    c.setClientCertificateType(wClientCertificateType.getText());
    c.setBinaryMode(wBinaryMode.getSelection());
    c.setActiveConnection(wActiveConnection.getSelection());
    c.setActivePortRangeFrom(wActivePortRangeFrom.getText());
    c.setActivePortRangeTo(wActivePortRangeTo.getText());
    c.setUserDirIsRoot(wUserDirIsRoot.getSelection());
    c.setRemoteVerification(wRemoteVerification.getSelection());
    c.setAutodetectUtf8(wAutodetectUtf8.getSelection());
    c.setControlEncoding(wControlEncoding.getText());
    c.setConnectTimeout(wConnectTimeout.getText());
    c.setSocketTimeout(wSocketTimeout.getText());
    c.setDataTimeout(wDataTimeout.getText());
    c.setControlKeepAliveTimeout(wControlKeepAliveTimeout.getText());
    c.setControlKeepAliveReplyTimeout(wControlKeepAliveReplyTimeout.getText());
    c.setEntryParser(wEntryParser.getText());
    c.setServerLanguageCode(wServerLanguageCode.getText());
    c.setServerTimeZone(wServerTimeZone.getText());
    c.setDefaultDateFormat(wDefaultDateFormat.getText());
    c.setRecentDateFormat(wRecentDateFormat.getText());
    c.setShortMonthNames(wShortMonthNames.getText());
    c.setMdtmLastModifiedTime(wMdtmLastModifiedTime.getSelection());
    c.setProxyHost(wProxyHost.getText());
    c.setProxyPort(wProxyPort.getText());
    c.setProxyUsername(wProxyUsername.getText());
    c.setProxyPassword(wProxyPassword.getText());
    c.setSocksProxyHost(wSocksProxyHost.getText());
    c.setSocksProxyPort(wSocksProxyPort.getText());
    c.setSocksProxyUsername(wSocksProxyUsername.getText());
    c.setSocksProxyPassword(wSocksProxyPassword.getText());
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
