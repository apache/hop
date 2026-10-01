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

package org.apache.hop.git.provider;

import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

public class GitConnectionEditor extends MetadataEditor<GitConnection> {

  private static final Class<?> PKG = GitConnectionEditor.class;

  private Text wName;
  private ComboVar wProvider;
  private ComboVar wAuthType;
  private TextVar wApiBaseUrl;
  private TextVar wUsername;
  private PasswordTextVar wToken;
  private PasswordTextVar wPassword;
  private Label wlUsername;
  private Label wlToken;
  private Label wlPassword;
  private GitProvider lastProvider;

  public GitConnectionEditor(
      HopGui hopGui, MetadataManager<GitConnection> manager, GitConnection connection) {
    super(hopGui, manager, connection);
  }

  @Override
  public void createControl(Composite parent) {
    PropsUi props = PropsUi.getInstance();
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin() + 2;
    IVariables variables = manager.getVariables();

    Label wlName = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "GitConnectionDialog.Name.Label"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, margin);
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.left = new FormAttachment(middle, 0);
    fdName.right = new FormAttachment(95, 0);
    wName.setLayoutData(fdName);
    Control lastControl = wName;

    Label wlProvider = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlProvider);
    wlProvider.setText(BaseMessages.getString(PKG, "GitConnectionDialog.Provider.Label"));
    FormData fdlProvider = new FormData();
    fdlProvider.top = new FormAttachment(lastControl, margin);
    fdlProvider.left = new FormAttachment(0, 0);
    fdlProvider.right = new FormAttachment(middle, -margin);
    wlProvider.setLayoutData(fdlProvider);
    wProvider = new ComboVar(variables, parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wProvider.setItems(GitProvider.displayNames());
    PropsUi.setLook(wProvider);
    FormData fdProvider = new FormData();
    fdProvider.top = new FormAttachment(wlProvider, 0, SWT.CENTER);
    fdProvider.left = new FormAttachment(middle, 0);
    fdProvider.right = new FormAttachment(95, 0);
    wProvider.setLayoutData(fdProvider);
    wProvider.addModifyListener(
        e -> {
          updateProviderFields();
          setChanged();
        });
    lastControl = wProvider;

    Label wlAuthType = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlAuthType);
    wlAuthType.setText(BaseMessages.getString(PKG, "GitConnectionDialog.AuthType.Label"));
    FormData fdlAuthType = new FormData();
    fdlAuthType.top = new FormAttachment(lastControl, margin);
    fdlAuthType.left = new FormAttachment(0, 0);
    fdlAuthType.right = new FormAttachment(middle, -margin);
    wlAuthType.setLayoutData(fdlAuthType);
    wAuthType = new ComboVar(variables, parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wAuthType.setToolTipText(BaseMessages.getString(PKG, "GitConnectionDialog.AuthType.Tooltip"));
    PropsUi.setLook(wAuthType);
    FormData fdAuthType = new FormData();
    fdAuthType.top = new FormAttachment(wlAuthType, 0, SWT.CENTER);
    fdAuthType.left = new FormAttachment(middle, 0);
    fdAuthType.right = new FormAttachment(95, 0);
    wAuthType.setLayoutData(fdAuthType);
    wAuthType.addModifyListener(
        e -> {
          updateCredentialFields();
          setChanged();
        });
    lastControl = wAuthType;

    Label wlApiBaseUrl = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlApiBaseUrl);
    wlApiBaseUrl.setText(BaseMessages.getString(PKG, "GitConnectionDialog.ApiBaseUrl.Label"));
    FormData fdlApiBaseUrl = new FormData();
    fdlApiBaseUrl.top = new FormAttachment(lastControl, margin);
    fdlApiBaseUrl.left = new FormAttachment(0, 0);
    fdlApiBaseUrl.right = new FormAttachment(middle, -margin);
    wlApiBaseUrl.setLayoutData(fdlApiBaseUrl);
    wApiBaseUrl = new TextVar(variables, parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wApiBaseUrl);
    wApiBaseUrl.setToolTipText(
        BaseMessages.getString(PKG, "GitConnectionDialog.ApiBaseUrl.Tooltip"));
    FormData fdApiBaseUrl = new FormData();
    fdApiBaseUrl.top = new FormAttachment(wlApiBaseUrl, 0, SWT.CENTER);
    fdApiBaseUrl.left = new FormAttachment(middle, 0);
    fdApiBaseUrl.right = new FormAttachment(95, 0);
    wApiBaseUrl.setLayoutData(fdApiBaseUrl);
    lastControl = wApiBaseUrl;

    wlUsername = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlUsername);
    wlUsername.setText(BaseMessages.getString(PKG, "GitConnectionDialog.Username.Label"));
    FormData fdlUsername = new FormData();
    fdlUsername.top = new FormAttachment(lastControl, margin);
    fdlUsername.left = new FormAttachment(0, 0);
    fdlUsername.right = new FormAttachment(middle, -margin);
    wlUsername.setLayoutData(fdlUsername);
    wUsername = new TextVar(variables, parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wUsername);
    FormData fdUsername = new FormData();
    fdUsername.top = new FormAttachment(wlUsername, 0, SWT.CENTER);
    fdUsername.left = new FormAttachment(middle, 0);
    fdUsername.right = new FormAttachment(95, 0);
    wUsername.setLayoutData(fdUsername);
    lastControl = wUsername;

    wlToken = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlToken);
    wlToken.setText(BaseMessages.getString(PKG, "GitConnectionDialog.Token.Label"));
    FormData fdlToken = new FormData();
    fdlToken.top = new FormAttachment(lastControl, margin);
    fdlToken.left = new FormAttachment(0, 0);
    fdlToken.right = new FormAttachment(middle, -margin);
    wlToken.setLayoutData(fdlToken);
    wToken = new PasswordTextVar(variables, parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wToken);
    wToken.setToolTipText(BaseMessages.getString(PKG, "GitConnectionDialog.Token.Tooltip"));
    FormData fdToken = new FormData();
    fdToken.top = new FormAttachment(wlToken, 0, SWT.CENTER);
    fdToken.left = new FormAttachment(middle, 0);
    fdToken.right = new FormAttachment(95, 0);
    wToken.setLayoutData(fdToken);
    lastControl = wToken;

    wlPassword = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlPassword);
    wlPassword.setText(BaseMessages.getString(PKG, "GitConnectionDialog.Password.Label"));
    FormData fdlPassword = new FormData();
    fdlPassword.top = new FormAttachment(lastControl, margin);
    fdlPassword.left = new FormAttachment(0, 0);
    fdlPassword.right = new FormAttachment(middle, -margin);
    wlPassword.setLayoutData(fdlPassword);
    wPassword = new PasswordTextVar(variables, parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPassword);
    wPassword.setToolTipText(BaseMessages.getString(PKG, "GitConnectionDialog.Password.Tooltip"));
    FormData fdPassword = new FormData();
    fdPassword.top = new FormAttachment(wlPassword, 0, SWT.CENTER);
    fdPassword.left = new FormAttachment(middle, 0);
    fdPassword.right = new FormAttachment(95, 0);
    wPassword.setLayoutData(fdPassword);

    Button wTest = new Button(parent, SWT.PUSH);
    wTest.setText(BaseMessages.getString(PKG, "GitConnectionDialog.TestConnection.Label"));
    PropsUi.setLook(wTest);
    FormData fdTest = new FormData();
    fdTest.top = new FormAttachment(wPassword, margin * 2);
    fdTest.left = new FormAttachment(middle, 0);
    wTest.setLayoutData(fdTest);
    wTest.addListener(SWT.Selection, e -> testConnection());

    setWidgetsContent();
    updateProviderFields();

    ModifyListener lsMod = e -> setChanged();
    wName.addModifyListener(lsMod);
    wApiBaseUrl.addModifyListener(lsMod);
    wUsername.addModifyListener(lsMod);
    wToken.addModifyListener(lsMod);
    wPassword.addModifyListener(lsMod);
    resetChanged();
  }

  /** Offers only the mechanisms the selected provider actually accepts. */
  private void refreshAuthTypeItems() {
    GitProvider provider = GitProvider.fromDisplayName(wProvider.getText());
    List<GitAuthType> supported = provider.getSupportedAuthTypes();
    GitAuthType current = GitAuthType.fromDisplayName(wAuthType.getText(), null);
    wAuthType.setItems(GitAuthType.displayNames(supported));
    GitAuthType selected =
        current != null && supported.contains(current) ? current : provider.getDefaultAuthType();
    wAuthType.setText(selected.getDisplayName());
  }

  /** Shows the credential fields the selected mechanism needs, and only those. */
  private void updateCredentialFields() {
    GitProvider provider = GitProvider.fromDisplayName(wProvider.getText());
    GitAuthType authType =
        GitAuthType.fromDisplayName(wAuthType.getText(), provider.getDefaultAuthType());
    boolean basic = authType == GitAuthType.BASIC;
    wlUsername.setVisible(basic);
    wUsername.setVisible(basic);
    wlPassword.setVisible(basic);
    wPassword.setVisible(basic);
    wlToken.setVisible(!basic);
    wToken.setVisible(!basic);
    wlToken.setText(
        BaseMessages.getString(
            PKG,
            authType == GitAuthType.OAUTH2
                ? "GitConnectionDialog.OauthToken.Label"
                : "GitConnectionDialog.Token.Label"));
  }

  private void updateProviderFields() {
    GitProvider provider = GitProvider.fromDisplayName(wProvider.getText());
    refreshAuthTypeItems();
    updateCredentialFields();

    String currentUrl = Const.NVL(wApiBaseUrl.getText(), "").trim();
    String newDefault = provider.getUiDefaultApiBaseUrl();
    if (!newDefault.isBlank()) {
      boolean empty = currentUrl.isEmpty();
      boolean stillPreviousDefault =
          lastProvider != null
              && (currentUrl.equals(Const.NVL(lastProvider.getUiDefaultApiBaseUrl(), "").trim())
                  || currentUrl.equals(Const.NVL(lastProvider.getDefaultApiBaseUrl(), "").trim()));
      if (empty || stillPreviousDefault) {
        wApiBaseUrl.setText(newDefault);
      }
    } else if (lastProvider != null
        && (currentUrl.equals(Const.NVL(lastProvider.getUiDefaultApiBaseUrl(), "").trim())
            || currentUrl.equals(Const.NVL(lastProvider.getDefaultApiBaseUrl(), "").trim()))) {
      wApiBaseUrl.setText("");
    }

    if (provider.getApiBaseUrlHint() != null) {
      wApiBaseUrl.setToolTipText(
          BaseMessages.getString(PKG, "GitConnectionDialog.ApiBaseUrl.Tooltip.Custom")
              + " "
              + provider.getApiBaseUrlHint());
    } else {
      wApiBaseUrl.setToolTipText(
          BaseMessages.getString(PKG, "GitConnectionDialog.ApiBaseUrl.Tooltip"));
    }

    lastProvider = provider;
  }

  private void testConnection() {
    try {
      GitConnection testConnection = buildConnectionForTest();
      testConnection.test(resolveEditorVariables());
      MessageBox box = new MessageBox(hopGui.getShell(), SWT.ICON_INFORMATION | SWT.OK);
      box.setText(BaseMessages.getString(PKG, "GitConnectionDialog.TestConnection.Success.Title"));
      box.setMessage(BaseMessages.getString(PKG, "GitConnectionDialog.TestConnection.Success"));
      box.open();
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "GitConnectionDialog.TestConnection.Error.Title"),
          BaseMessages.getString(PKG, "GitConnectionDialog.TestConnection.Error.Message"),
          e);
    }
  }

  /** Active Hop project / environment variables used to resolve ${GH_TOKEN} and similar. */
  private IVariables resolveEditorVariables() {
    return hopGui.getVariables();
  }

  private GitConnection buildConnectionForTest() {
    GitConnection testConnection = new GitConnection();
    getWidgetsContent(testConnection);
    // Password widgets are often blank when reopening a saved connection.
    if (Utils.isEmpty(testConnection.getToken()) && !Utils.isEmpty(metadata.getToken())) {
      testConnection.setToken(metadata.getToken());
    }
    if (Utils.isEmpty(testConnection.getPassword()) && !Utils.isEmpty(metadata.getPassword())) {
      testConnection.setPassword(metadata.getPassword());
    }
    return testConnection;
  }

  @Override
  public void setWidgetsContent() {
    wName.setText(Const.NVL(metadata.getName(), ""));
    wProvider.setText(metadata.getGitProvider().getDisplayName());
    refreshAuthTypeItems();
    wAuthType.setText(metadata.getAuthType().getDisplayName());
    wApiBaseUrl.setText(metadata.getDisplayApiBaseUrl());
    wUsername.setText(Const.NVL(metadata.getUsername(), ""));
    wToken.setText(Const.NVL(metadata.getToken(), ""));
    wPassword.setText(Const.NVL(metadata.getPassword(), ""));
    lastProvider = metadata.getGitProvider();
  }

  @Override
  public void getWidgetsContent(GitConnection connection) {
    GitProvider provider = GitProvider.fromDisplayName(wProvider.getText());
    connection.setName(wName.getText());
    connection.setGitProvider(provider);
    connection.setApiBaseUrl(
        GitConnection.normalizeApiBaseUrlForStorage(wApiBaseUrl.getText(), provider));
    GitAuthType authType =
        GitAuthType.fromDisplayName(wAuthType.getText(), provider.getDefaultAuthType());
    connection.setAuthType(authType);
    if (authType == GitAuthType.BASIC) {
      connection.setUsername(wUsername.getText());
      connection.setPassword(wPassword.getText());
      connection.setToken(null);
    } else {
      connection.setToken(wToken.getText());
      connection.setUsername(null);
      connection.setPassword(null);
    }
  }
}
